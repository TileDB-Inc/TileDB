/**
 * @file   unit-cppapi-updates.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2021 TileDB, Inc.
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
 * Tests array updates (writes producing multiple fragments) using the C++ API.
 */

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include "test/support/src/helpers.h"
#include "test/support/src/vfs_helpers.h"
#include "tiledb/sm/cpp_api/tiledb"

using namespace tiledb;

TEST_CASE(
    "C++ API updates: test writing two identical fragments",
    "[updates][updates-identical-fragments][rest]") {
  test::VFSTestSetup vfs_test_setup;
  const std::string array_name =
      vfs_test_setup.array_uri("updates_identical_fragments");
  Context ctx{vfs_test_setup.ctx()};

  // Create
  const int rowmin = 0, rowmax = 9, row_ext = rowmax - rowmin + 1;
  const int colmin = 0, colmax = 9, col_ext = colmax - colmin + 1;

  Domain domain(ctx);
  domain
      .add_dimension(
          Dimension::create<int>(ctx, "rows", {{rowmin, rowmax}}, row_ext))
      .add_dimension(
          Dimension::create<int>(ctx, "cols", {{colmin, colmax}}, col_ext));
  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
  auto attr1 = Attribute::create<std::vector<int>>(ctx, "a1");
  schema.add_attribute(attr1);
  Array::create(array_name, schema);

  // Setup data
  std::vector<int> data_a1(row_ext * col_ext);
  std::vector<uint64_t> offsets_a1(row_ext * col_ext);

  for (int i = 0; i < (int)data_a1.size(); i++) {
    data_a1[i] = i;
    offsets_a1[i] = i * sizeof(int);
  }

  // First write
  Array array_w1(ctx, array_name, TILEDB_WRITE);
  Query query_w1(ctx, array_w1);
  query_w1
      .set_subarray(Subarray(ctx, array_w1)
                        .set_subarray({rowmin, rowmax, colmin, colmax}))
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a1", data_a1)
      .set_offsets_buffer("a1", offsets_a1);
  query_w1.submit();
  array_w1.close();

  // Second write
  Array array_w2(ctx, array_name, TILEDB_WRITE);
  Query query_w2(ctx, array_w2);
  query_w2
      .set_subarray(Subarray(ctx, array_w2)
                        .set_subarray({rowmin, rowmax, colmin, colmax}))
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a1", data_a1)
      .set_offsets_buffer("a1", offsets_a1);
  query_w2.submit();
  array_w2.close();

  // Read
  Array array(ctx, array_name, TILEDB_READ);
  Query query(ctx, array);

  std::vector<int> subarray = {rowmin, rowmax, colmin, colmax};
  std::vector<uint64_t> r_offsets_a1;
  r_offsets_a1.resize(100);
  std::vector<int> r_data_a1;
  r_data_a1.resize(300);

  Subarray sub(ctx, array);
  sub.set_subarray(subarray);
  query.set_subarray(sub)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a1", r_data_a1)
      .set_offsets_buffer("a1", r_offsets_a1);
  query.submit();
  REQUIRE(query.query_status() == Query::Status::COMPLETE);
  array.close();

  for (int i = 0; i < (int)data_a1.size(); i++)
    REQUIRE(r_data_a1[i] == i);
}

TEST_CASE(
    "C++ API updates: empty second write",
    "[updates][updates-empty-write][rest-fails][sc-45709]") {
  test::VFSTestSetup vfs_test_setup;
  const std::string array_name =
      vfs_test_setup.array_uri("updates_empty_write");
  Context ctx{vfs_test_setup.ctx()};

  // Create
  Domain domain(ctx);
  domain.add_dimension(
      Dimension::create(ctx, "d", TILEDB_STRING_ASCII, nullptr, nullptr));
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
  Array::create(array_name, schema);

  tiledb_layout_t layout = GENERATE(TILEDB_UNORDERED, TILEDB_GLOBAL_ORDER);

  // First write
  std::string data("ab");
  std::vector<uint64_t> offsets({0, 1});
  Array array_w1(ctx, array_name, TILEDB_WRITE);
  Query query_w1(ctx, array_w1);
  query_w1.set_layout(layout).set_data_buffer("d", data).set_offsets_buffer(
      "d", offsets);

  if (layout == TILEDB_GLOBAL_ORDER) {
    query_w1.submit_and_finalize();
  } else {
    query_w1.submit();
  }

  array_w1.close();

  // Second write
  offsets.clear();
  Array array_w2(ctx, array_name, TILEDB_WRITE);
  Query query_w2(ctx, array_w2);
  query_w2.set_layout(layout).set_data_buffer("d", data).set_offsets_buffer(
      "d", offsets);

  if (layout == TILEDB_GLOBAL_ORDER) {
    query_w2.submit_and_finalize();
  } else {
    query_w2.submit();
  }

  array_w2.close();
}
