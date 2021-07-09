/**
 * @file   unit-cppapi-query.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB Inc.
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
 * Tests the C++ API for query related functions.
 */

#include "catch.hpp"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/misc/utils.h"

using namespace tiledb;

TEST_CASE("C++ API: Test get query layout", "[cppapi][query]") {
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

  Array array(ctx, array_name, TILEDB_READ);
  Query query(ctx, array);
  REQUIRE(query.query_layout() == TILEDB_ROW_MAJOR);
  query.set_layout(TILEDB_COL_MAJOR);
  REQUIRE(query.query_layout() == TILEDB_COL_MAJOR);
  query.set_layout(TILEDB_GLOBAL_ORDER);
  REQUIRE(query.query_layout() == TILEDB_GLOBAL_ORDER);
  query.set_layout(TILEDB_UNORDERED);
  REQUIRE(query.query_layout() == TILEDB_UNORDERED);

  array.close();

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

TEST_CASE("C++ API: Test get query array", "[cppapi][query]") {
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

  Array array(ctx, array_name, TILEDB_READ);
  Query query(ctx, array);

  Array rquery = query.array();
  // The two arrays are of the same query type
  REQUIRE(array.query_type() == TILEDB_READ);
  REQUIRE(rquery.query_type() == TILEDB_READ);

  // Get the information of is_open() by either sources - should match
  REQUIRE(array.is_open());
  REQUIRE(rquery.is_open());

  // The schema values are the same by either source
  REQUIRE(rquery.schema().cell_order() == array.schema().cell_order());
  array.close();

  // When one of the instances is closed the mirror should be close as well
  CHECK(rquery.is_open() == false);
  CHECK(array.is_open() == false);

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

TEST_CASE(
    "C++ API: Test get written fragments for reads",
    "[cppapi][query][written-fragments][read]") {
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

  Array array(ctx, array_name, TILEDB_READ);
  Query query(ctx, array);
  REQUIRE_THROWS(query.fragment_num());
  REQUIRE_THROWS(query.fragment_uri(0));
  REQUIRE_THROWS(query.fragment_timestamp_range(0));

  array.close();

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

TEST_CASE(
    "C++ API: Test get written fragments for writes",
    "[cppapi][query][written-fragments][write]") {
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

  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array);
  CHECK(query.fragment_num() == 0);
  REQUIRE_THROWS(query.fragment_uri(0));
  REQUIRE_THROWS(query.fragment_timestamp_range(0));

  array.close();

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

TEST_CASE(
    "C++ API: Write on open read array", "[cppapi][query][write-on-open]") {
  const std::string array_name = "cpp_unit_array";
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  // Create the array
  Domain domain(ctx);
  domain.add_dimension(Dimension::create<int>(ctx, "rows", {{0, 3}}, 4))
      .add_dimension(Dimension::create<int>(ctx, "cols", {{0, 3}}, 4));
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain).set_order({{TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR}});
  schema.add_attribute(Attribute::create<int>(ctx, "a"));
  Array::create(array_name, schema);

  // Write some initial data and close the array.
  std::vector<int> data_w = {1, 2, 3, 4};
  std::vector<int> coords_w = {0, 0, 1, 1, 2, 2, 3, 3};
  Array array_w(ctx, array_name, TILEDB_WRITE);
  Query query_w(ctx, array_w);
  query_w.set_coordinates(coords_w)
      .set_layout(TILEDB_UNORDERED)
      .set_data_buffer("a", data_w);
  query_w.submit();
  query_w.finalize();
  array_w.close();

  // Open and read an array without closing it.
  Array array1(ctx, array_name, TILEDB_READ);
  Query query1(ctx, array1);
  int range[] = {1, 2};
  query1.add_range(0, range[0], range[1]).add_range(1, range[0], range[1]);
  auto est_size = query1.est_result_size("a");
  std::vector<int> data(est_size);
  query1.set_layout(TILEDB_ROW_MAJOR).set_data_buffer("a", data);
  query1.submit();
  REQUIRE(query1.result_buffer_elements()["a"].second == 2);
  REQUIRE(data[0] == 2);
  REQUIRE(data[1] == 3);
  query1.finalize();

  // Open and write to the same array without closing it.
  Array array2(ctx, array_name, TILEDB_WRITE);
  Query query2(ctx, array2);
  query2.set_coordinates(coords_w)
      .set_layout(TILEDB_UNORDERED)
      .set_data_buffer("a", data_w);
  query2.submit();
  query2.finalize();

  // Close both arrays.
  array1.close();
  array2.close();
}

TEST_CASE(
    "C++ API: Test add and get ranges by name",
    "[cppapi][query][range][string-dims]") {
  const std::string array_name = "test_ranges";
  Context ctx;
  VFS vfs(ctx);

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  // Create array with string and fixed dimensions
  auto d1 = Dimension::create(ctx, "d1", TILEDB_STRING_ASCII, nullptr, nullptr);
  auto d2 = Dimension::create<int>(ctx, "d2", {{1, 10}}, 5);

  Domain dom(ctx);
  dom.add_dimension(d1);
  dom.add_dimension(d2);
  ArraySchema schema(ctx, TILEDB_SPARSE);
  auto a = Attribute::create<int32_t>(ctx, "a");
  schema.add_attribute(a);
  schema.set_domain(dom);
  Array::create(array_name, schema);

  // set dimensions
  Array array(ctx, array_name, TILEDB_READ);
  std::string d1_data("abbccdddd");
  uint64_t d1_off[] = {0, 1, 3, 5};
  uint64_t d1_off_size = 4;
  Query query(ctx, array, TILEDB_READ);
  CHECK_NOTHROW(
      query.set_data_buffer("d1", (void*)d1_data.c_str(), d1_data.size()));
  CHECK_NOTHROW(query.set_offsets_buffer("d1", d1_off, d1_off_size));

  // Add 1 range per dimension
  std::string s1("a", 1);
  std::string s2("cc", 2);
  CHECK_NOTHROW(query.add_range("d1", s1, s2));
  int range[] = {1, 2};
  CHECK_NOTHROW(query.add_range("d2", range[0], range[1]));

  // Check number of ranges on each dimension
  int range_num = query.range_num("d1");
  CHECK(range_num == 1);
  range_num = query.range_num("d2");
  CHECK(range_num == 1);

  // Check ranges
  std::array<std::string, 2> range1 = query.range("d1", 0);
  CHECK(range1[0] == s1);
  CHECK(range1[1] == s2);
  std::array<int, 3> range2 = query.range<int>("d2", 0);
  CHECK(range2[0] == 1);
  CHECK(range2[1] == 2);
  CHECK(range2[2] == 0);

  // Close array
  array.close();

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}
