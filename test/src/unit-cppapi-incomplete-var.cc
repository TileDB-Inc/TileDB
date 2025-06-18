/**
 * @file   unit-cppapi-incomplete-var.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2025 TileDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * Tests the C API queries where the result buffer is not large enough
 * to hold the whole query result - variable-length attribute edition.
 */

#include <test/support/tdb_catch.h>
#include "test/support/src/array_helpers.h"
#include "test/support/src/error_helpers.h"
#include "test/support/src/helpers.h"
#include "test/support/src/vfs_helpers.h"
#include "tiledb/sm/cpp_api/tiledb"

#include <cstring>
#include <iostream>

using namespace tiledb;
using namespace tiledb::test;

/**
 * Tests cases where a read query with variable-length attributes
 * is incomplete or leads to a buffer overflow.
 */

struct Cells {
  std::vector<int32_t> rows;
  std::vector<int32_t> cols;
  std::vector<int32_t> a_vals;
  std::vector<uint64_t> a_offs;

  Cells()
      : rows{1, 2, 3, 4}
      , cols{1, 2, 3, 4}
      , a_vals{1, 2, 20, 3, 30, 300, 4, 40, 400, 4000}
      , a_offs{
            0, 1 * sizeof(int32_t), 3 * sizeof(int32_t), 6 * sizeof(int32_t)} {
  }

  Cells(uint64_t reserve)
      : rows(reserve)
      , cols(reserve)
      , a_vals(reserve)
      , a_offs(reserve) {
  }

  uint64_t num_cells() const {
    return rows.size();
  }

  void restore() {
    rows.resize(rows.capacity());
    cols.resize(cols.capacity());
    a_vals.resize(a_vals.capacity());
    a_offs.resize(a_offs.capacity());
  }

  void double_size() {
    rows.resize(rows.capacity() * 2);
    cols.resize(cols.capacity() * 2);
    a_vals.resize(a_vals.capacity() * 2);
    a_offs.resize(a_offs.capacity() * 2);
  }

  void resize(
      const std::unordered_map<std::string, std::pair<uint64_t, uint64_t>>&
          results) {
    rows.resize(results.find("rows")->second.second);
    cols.resize(results.find("cols")->second.second);
    a_vals.resize(results.find("a")->second.second);
    a_offs.resize(results.find("a")->second.first);
  }
};

struct IncompleteVarFx {
  VFSTestSetup vfs_test_setup_;
  mutable Context ctx_;
  std::string uri_;

  tiledb_ctx_t* context_c() const {
    return vfs_test_setup_.ctx_c;
  }

  Context& context() const {
    return ctx_;
  }

  void create_array();
  void write_array(Cells& cells);
  void read_array(const Cells& expect);

  IncompleteVarFx();
  ~IncompleteVarFx();
};

IncompleteVarFx::IncompleteVarFx()
    : ctx_(context_c(), false)
    , uri_(vfs_test_setup_.array_uri("incomplete_var_fx")) {
}

IncompleteVarFx::~IncompleteVarFx() {
  auto obj = tiledb::Object::object(ctx_, uri_);
  if (obj.type() == tiledb::Object::Type::Array) {
    Array::delete_array(ctx_, uri_);
  }
}

void IncompleteVarFx::create_array() {
  // Dimensions
  auto rows = Dimension::create<int32_t>(context(), "rows", {1, 10}, 4);
  auto cols = Dimension::create<int32_t>(context(), "cols", {1, 10}, 4);

  Domain domain(context());
  domain.add_dimension(rows);
  domain.add_dimension(cols);

  // Attributes
  auto att_a = Attribute::create<int32_t>(context(), "a")
                   .set_cell_val_num(TILEDB_VAR_NUM);

  ArraySchema schema(context(), TILEDB_SPARSE);
  schema.set_domain(domain);
  schema.add_attribute(att_a);

  // Create array
  Array::create(uri_, schema);
}

void IncompleteVarFx::write_array(Cells& cells) {
  Array array(context(), uri_, TILEDB_WRITE);
  Query query(context(), array);
  query.set_layout(TILEDB_UNORDERED)
      .set_data_buffer("rows", cells.rows)
      .set_data_buffer("cols", cells.cols)
      .set_data_buffer("a", cells.a_vals)
      .set_offsets_buffer("a", cells.a_offs);

  CHECK_NOTHROW(query.submit());
  query.finalize();
}

void IncompleteVarFx::read_array(const Cells& expect) {
  Cells actual(1);

  Array array(context(), uri_, TILEDB_READ);

  Query query(context(), array);
  query.set_layout(TILEDB_UNORDERED);

  auto set_buffers = [&]() {
    actual.restore();
    query.set_data_buffer("rows", actual.rows)
        .set_data_buffer("cols", actual.cols)
        .set_data_buffer("a", actual.a_vals)
        .set_offsets_buffer("a", actual.a_offs);
  };

  uint64_t cursor = 0;

  Query::Status status;
  tiledb_query_status_details_t detail;
  do {
    set_buffers();
    status = query.submit();

    using Asserter = AsserterCatch;
    TRY(context_c(),
        tiledb_query_get_status_details(
            context_c(), query.ptr().get(), &detail));

    const auto num_cells_read = query.result_buffer_elements()["rows"].second;

    if (status == Query::Status::INCOMPLETE && num_cells_read == 0 &&
        detail.incomplete_reason == TILEDB_REASON_USER_BUFFER_SIZE) {
      actual.double_size();
      set_buffers();
    } else {
      actual.resize(query.result_buffer_elements());

      for (uint64_t o = 0; o < num_cells_read; o++) {
        CHECK(actual.rows[o] == expect.rows[cursor + o]);
        CHECK(actual.cols[o] == expect.cols[cursor + o]);

        const uint64_t expect_a_end =
            (cursor + o + 1 == expect.num_cells() ?
                 expect.a_vals.size() :
                 (expect.a_offs[cursor + o + 1] / sizeof(int32_t)));
        const uint64_t actual_a_end =
            (o + 1 == actual.num_cells() ? actual.a_vals.size() :
                                           actual.a_offs[o + 1]);

        // NB: `expect` unit is bytes, `actual` unit is elements (computed by
        // the C++ API)
        const uint64_t expect_a_len =
            expect_a_end - (expect.a_offs[cursor + o] / sizeof(int32_t));
        const uint64_t actual_a_len = actual_a_end - actual.a_offs[o];

        CHECK(expect_a_len == actual_a_len);
        if (expect_a_len == actual_a_len) {
          CHECK(std::equal(
              expect.a_vals.begin() +
                  (expect.a_offs[cursor + o] / sizeof(int32_t)),
              expect.a_vals.begin() + expect_a_end,
              actual.a_vals.begin() + actual.a_offs[o],
              actual.a_vals.begin() + actual_a_end));
        }
      }
    }
    cursor += num_cells_read;
  } while (status == Query::Status::INCOMPLETE);

  CHECK(cursor == expect.num_cells());
}

TEST_CASE_METHOD(
    IncompleteVarFx,
    "CPP API: Test incomplete read queries var: sparse",
    "[capi][incomplete][sparse-global-order][rest]") {
  create_array();

  // ensure we delete the array if anything goes wrong
  DeleteArrayGuard arrayguard(context_c(), uri_.c_str());

  Cells cells;
  write_array(cells);
  read_array(cells);
}
