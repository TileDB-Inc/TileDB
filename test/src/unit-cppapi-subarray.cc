/**
 * @file   unit-cppapi-subarray.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB Inc.
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
 * Tests the C++ API for subarray related functions.
 */

#include "catch.hpp"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/misc/utils.h"

using namespace tiledb;

TEST_CASE("C++ API: Test subarray", "[cppapi], [sparse], [subarray]") {
  const std::string array_name = "cpp_unit_array";
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  // Create
  Domain domain(ctx);
  domain.add_dimension(Dimension::create<int>(ctx, "rows", {{0, 3}}, 4))
      .add_dimension(Dimension::create<int>(ctx, "cols", {{0, 3}}, 4));
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
  schema.add_attribute(Attribute::create<int>(ctx, "a"));
  Array::create(array_name, schema);

  // Write
  std::vector<int> data_w = {1, 2, 3, 4};
  std::vector<int> coords_w = {0, 0, 1, 1, 2, 2, 3, 3};
  Array array_w(ctx, array_name, TILEDB_WRITE);
  Query query_w(ctx, array_w);
  query_w.set_coordinates(coords_w)
      .set_layout(TILEDB_UNORDERED)
      .set_buffer("a", data_w);
  query_w.submit();
  query_w.finalize();
  array_w.close();

  SECTION("- Read single cell") {
    Array array(ctx, array_name, TILEDB_READ);
    Query query(ctx, array);
    Subarray subarray(ctx, array, TILEDB_UNORDERED);
    int range[] = {0, 0};
    subarray.add_range(0, range);
    subarray.add_range(1, range);

    auto est_size = subarray.est_result_size("a");
    REQUIRE(est_size == 1);

    std::vector<int> data(est_size);
    query.set_subarray(subarray)
        .set_layout(TILEDB_ROW_MAJOR)
        .set_buffer("a", data);
    query.submit();
    REQUIRE(query.result_buffer_elements()["a"].second == 1);
    REQUIRE(data[0] == 1);
  }

  SECTION("- Read single range") {
    Array array(ctx, array_name, TILEDB_READ);
    Query query(ctx, array);
    Subarray subarray(ctx, array, TILEDB_UNORDERED);
    int range[] = {1, 2};
    subarray.add_range(0, range);
    subarray.add_range(1, range);

    auto est_size = subarray.est_result_size("a");
    REQUIRE(est_size == 4);

    std::vector<int> data(est_size);
    query.set_subarray(subarray)
        .set_layout(TILEDB_ROW_MAJOR)
        .set_buffer("a", data);
    query.submit();
    REQUIRE(query.result_buffer_elements()["a"].second == 2);
    REQUIRE(data[0] == 2);
    REQUIRE(data[1] == 3);
  }

  SECTION("- Read two cells") {
    Array array(ctx, array_name, TILEDB_READ);
    Query query(ctx, array);
    Subarray subarray(ctx, array, TILEDB_UNORDERED);
    int range0[] = {0, 0}, range1[] = {2, 2};
    subarray.add_range(0, range0);
    subarray.add_range(1, range0);
    subarray.add_range(0, range1);
    subarray.add_range(1, range1);

    auto est_size = subarray.est_result_size("a");
    REQUIRE(est_size == 4);

    std::vector<int> data(est_size);
    query.set_subarray(subarray)
        .set_layout(TILEDB_UNORDERED)
        .set_buffer("a", data);
    query.submit();
    REQUIRE(query.result_buffer_elements()["a"].second == 2);
    REQUIRE(data[0] == 1);
    REQUIRE(data[1] == 3);
  }

  SECTION("- Read two regions") {
    Array array(ctx, array_name, TILEDB_READ);
    Query query(ctx, array);
    Subarray subarray(ctx, array, TILEDB_UNORDERED);
    int range0[] = {0, 1}, range1[] = {2, 3};
    subarray.add_range(0, range0);
    subarray.add_range(1, range0);
    subarray.add_range(0, range1);
    subarray.add_range(1, range1);

    auto est_size = subarray.est_result_size("a");
    std::vector<int> data(est_size);
    query.set_subarray(subarray)
        .set_layout(TILEDB_UNORDERED)
        .set_buffer("a", data);
    query.submit();
    REQUIRE(query.result_buffer_elements()["a"].second == 4);
    REQUIRE(data[0] == 1);
    REQUIRE(data[1] == 2);
    REQUIRE(data[2] == 3);
    REQUIRE(data[3] == 4);
  }

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}