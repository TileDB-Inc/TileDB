/**
 * @file   unit-compression-delta.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 MIT and Intel Corporation
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
 * Tests the double delta compression.
 */

#include <test/support/tdb_catch.h>
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/enums/datatype.h"

#include <iostream>

using TestTypes = std::tuple<
    std::byte,
    int8_t,
    uint8_t,
    int16_t,
    uint16_t,
    int32_t,
    uint32_t,
    int64_t,
    uint64_t,
    char>;
TEMPLATE_LIST_TEST_CASE(
    "Delta compression test accepted input datatypes",
    "[compression][delta]",
    TestTypes) {
  using namespace tiledb;
  Context ctx;
  VFS vfs(ctx);
  std::string array_name = "delta_compression_test";
  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }

  // Create schema with filter lists
  FilterList a1_filters(ctx);
  a1_filters.set_max_chunk_size(10000);
  Filter f1{ctx, TILEDB_FILTER_DELTA};
  a1_filters.add_filter(f1);

  auto a1 = Attribute::create<TestType>(ctx, "a1");
  a1.set_filter_list(a1_filters);
  Domain domain(ctx);
  auto d1 = Dimension::create<int>(ctx, "d1", {{0, 100}}, 10);
  domain.add_dimensions(d1);
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain);
  schema.add_attributes(a1);

  // Create array
  Array::create(array_name, schema);

  // Write to array.
  std::vector<TestType> a1_data = {
      TestType{1},
      TestType{2},
      TestType{3},
      TestType{4},
      TestType{5},
      TestType{6},
      TestType{7},
      TestType{8},
      TestType{9},
      TestType{10}};
  std::vector<int> coords = {0, 10, 20, 30, 31, 32, 33, 34, 40, 50};
  Array array(ctx, array_name, TILEDB_WRITE);

  Query query(ctx, array);
  query.set_data_buffer("a1", a1_data)
      .set_layout(TILEDB_UNORDERED)
      .set_data_buffer("d1", coords);
  REQUIRE(query.submit() == Query::Status::COMPLETE);
  array.close();

  // Sanity check reading
  array.open(TILEDB_READ);
  std::vector<TestType> a1_read(a1_data.size());
  Query query_r(ctx, array);
  query_r.set_layout(TILEDB_UNORDERED).set_data_buffer("a1", a1_read);
  REQUIRE(query_r.submit() == Query::Status::COMPLETE);
  array.close();
  auto ret = query_r.result_buffer_elements();
  CHECK(ret.size() == 1);
  CHECK(ret["a1"].first == 0);
  CHECK(ret["a1"].second == a1_data.size());
  CHECK(a1_data == a1_read);

  // Clean up
  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }
}

TEST_CASE(
    "Delta compression test reinterpret datatype", "[compression][delta]") {
  using namespace tiledb;
  Context ctx;
  VFS vfs(ctx);
  std::string array_name = "delta_compression_test";
  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }

  // Create schema with filter lists
  FilterList a1_filters(ctx);
  a1_filters.set_max_chunk_size(10000);
  Filter f1{ctx, TILEDB_FILTER_DELTA};
  tiledb_datatype_t reinterpret_type = TILEDB_INT32;
  REQUIRE_NOTHROW(f1.set_option<tiledb_datatype_t>(
      TILEDB_COMPRESSION_REINTERPRET_DATATYPE, reinterpret_type));
  a1_filters.add_filter(f1);

  auto a1 = Attribute::create<float>(ctx, "a1");
  a1.set_filter_list(a1_filters);
  Domain domain(ctx);
  auto d1 = Dimension::create<int>(ctx, "d1", {{0, 100}}, 10);
  domain.add_dimensions(d1);
  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain);
  schema.add_attributes(a1);

  // Create array
  Array::create(array_name, schema);

  // Write to array
  std::vector<float> a1_data = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  std::vector<int> coords = {0, 10, 20, 30, 31, 32, 33, 34, 40, 50};
  Array array(ctx, array_name, TILEDB_WRITE);

  // Check filter in schema.
  auto loaded_schema = array.schema();
  auto loaded_filters = loaded_schema.attribute("a1").filter_list();
  CHECK(loaded_filters.nfilters() == 1);
  auto loaded_delta_filter = loaded_filters.filter(0);
  CHECK(loaded_delta_filter.filter_type() == TILEDB_FILTER_DELTA);
  tiledb_datatype_t output_reinterpret_type{};
  loaded_delta_filter.get_option(
      TILEDB_COMPRESSION_REINTERPRET_DATATYPE, &output_reinterpret_type);
  CHECK(output_reinterpret_type == TILEDB_INT32);
  CHECK(
      output_reinterpret_type ==
      loaded_delta_filter.get_option<tiledb_datatype_t>(
          TILEDB_COMPRESSION_REINTERPRET_DATATYPE));

  Query query(ctx, array);
  query.set_data_buffer("a1", a1_data)
      .set_layout(TILEDB_UNORDERED)
      .set_data_buffer("d1", coords);
  REQUIRE(query.submit() == Query::Status::COMPLETE);
  array.close();

  // Sanity check reading
  array.open(TILEDB_READ);
  std::vector<int> subarray = {0, 10};
  Subarray sub(ctx, array);
  sub.set_subarray(subarray);
  std::vector<float> a1_read(2);
  Query query_r(ctx, array);
  query_r.set_subarray(sub)
      .set_layout(TILEDB_UNORDERED)
      .set_data_buffer("a1", a1_read);
  REQUIRE(query_r.submit() == Query::Status::COMPLETE);
  array.close();
  auto ret = query_r.result_buffer_elements();
  CHECK(ret.size() == 1);
  CHECK(ret["a1"].first == 0);
  CHECK(ret["a1"].second == 2);
  CHECK(a1_read[0] == 1);
  CHECK(a1_read[1] == 2);

  // Clean up
  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }
}
