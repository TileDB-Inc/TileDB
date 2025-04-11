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
  void write_array();
  void read_array();

  IncompleteVarFx();
};

IncompleteVarFx::IncompleteVarFx()
    : ctx_(context_c(), false)
    , uri_(vfs_test_setup_.array_uri("incomplete_var_fx")) {
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

void IncompleteVarFx::write_array() {
  std::vector<int32_t> rows = {1, 2, 3, 4};
  std::vector<int32_t> cols = {1, 2, 3, 4};
  std::vector<int32_t> a_vals = {1, 2, 20, 3, 30, 300, 4, 40, 400, 4000};
  std::vector<uint64_t> a_offs = {0, 1, 3, 6};

  Array array(context(), uri_, TILEDB_WRITE);
  Query query(context(), array);
  query.set_layout(TILEDB_UNORDERED)
      .set_data_buffer("rows", rows)
      .set_data_buffer("cols", cols)
      .set_data_buffer("a", a_vals)
      .set_offsets_buffer("a", a_offs);

  CHECK_NOTHROW(query.submit());
  query.finalize();
}

void IncompleteVarFx::read_array() {
  std::vector<int32_t> rows(1);
  std::vector<int32_t> cols(1);
  std::vector<int32_t> a_vals(1);
  std::vector<uint64_t> a_offs(1);

  Array array(context(), uri_, TILEDB_READ);

  Query query(context(), array);
  query.set_layout(TILEDB_UNORDERED);

  auto set_buffers = [&]() {
    query.set_data_buffer("rows", rows)
        .set_data_buffer("cols", cols)
        .set_data_buffer("a", a_vals)
        .set_offsets_buffer("a", a_offs);
  };
  set_buffers();

  Query::Status status;
  tiledb_query_status_details_t detail;
  do {
    status = query.submit();

    using Asserter = AsserterCatch;
    TRY(context_c(),
        tiledb_query_get_status_details(
            context_c(), query.ptr().get(), &detail));

    const auto num_cells_read = query.result_buffer_elements()["rows"].first;

    if (status == Query::Status::INCOMPLETE && num_cells_read == 0 &&
        detail.incomplete_reason == TILEDB_REASON_USER_BUFFER_SIZE) {
      rows.resize(rows.size() * 2);
      cols.resize(cols.size() * 2);
      a_vals.resize(a_vals.size() * 2);
      a_offs.resize(a_offs.size() * 2);
      set_buffers();
    } else {
      // TODO: check results
    }
  } while (status == Query::Status::INCOMPLETE);
}

TEST_CASE_METHOD(
    IncompleteVarFx,
    "CPP API: Test incomplete read queries var: sparse",
    "[capi][incomplete][sparse-global-order][rest]") {
  create_array();

  // ensure we delete the array if anything goes wrong
  DeleteArrayGuard arrayguard(context_c(), uri_.c_str());

  write_array();
  read_array();
}
