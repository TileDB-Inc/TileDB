/**
 * @file   unit-cppapi-filter.cc
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
 * Tests the C++ API for filter related functions.
 */

#include <test/support/tdb_catch.h>
#include "test/src/helpers.h"
#include "tiledb/sm/cpp_api/tiledb"

static void check_filters(
    const tiledb::FilterList& answer, const tiledb::FilterList& check) {
  REQUIRE(check.nfilters() == answer.nfilters());
  REQUIRE(check.max_chunk_size() == answer.max_chunk_size());
  for (uint32_t i = 0; i < check.nfilters(); i++) {
    auto f_answer = answer.filter(i), f_check = check.filter(i);
    REQUIRE(f_check.filter_type() == f_answer.filter_type());
  }
}

TEST_CASE("C++ API: Filter options", "[cppapi][filter]") {
  using namespace tiledb;
  Context ctx;

  // Test filter creation and option setting/getting
  Filter f(ctx, TILEDB_FILTER_BZIP2);
  int32_t get_level;
  f.get_option(TILEDB_COMPRESSION_LEVEL, &get_level);
  REQUIRE(get_level == -1);

  int32_t set_level = 5;
  f.set_option(TILEDB_COMPRESSION_LEVEL, &set_level);
  f.get_option(TILEDB_COMPRESSION_LEVEL, &get_level);
  REQUIRE(get_level == 5);

  // Check templated version
  f.set_option(TILEDB_COMPRESSION_LEVEL, 4);
  f.get_option(TILEDB_COMPRESSION_LEVEL, &get_level);
  REQUIRE(get_level == 4);

  // Check templated version with wrong type throws exception
  uint32_t wrong_type_u = 1;
  REQUIRE_THROWS_AS(
      f.set_option(TILEDB_COMPRESSION_LEVEL, wrong_type_u),
      std::invalid_argument);
  REQUIRE_THROWS_AS(
      f.get_option(TILEDB_COMPRESSION_LEVEL, &wrong_type_u),
      std::invalid_argument);

  // Check that you can bypass type safety (don't do this).
  f.get_option(TILEDB_COMPRESSION_LEVEL, (void*)&wrong_type_u);
  REQUIRE(wrong_type_u == 4);

  // Unsupported option
  uint32_t window;
  REQUIRE_THROWS_AS(
      f.set_option(TILEDB_BIT_WIDTH_MAX_WINDOW, &window), TileDBError);
  REQUIRE_THROWS_AS(
      f.get_option(TILEDB_BIT_WIDTH_MAX_WINDOW, &window), TileDBError);

  Filter f2(ctx, TILEDB_FILTER_BIT_WIDTH_REDUCTION);
  int32_t wrong_type_i = 1;
  REQUIRE_THROWS_AS(f2.set_option(TILEDB_COMPRESSION_LEVEL, 1), TileDBError);
  REQUIRE_THROWS_AS(
      f2.set_option(TILEDB_BIT_WIDTH_MAX_WINDOW, -1), std::invalid_argument);
  REQUIRE_THROWS_AS(
      f2.set_option(TILEDB_BIT_WIDTH_MAX_WINDOW, wrong_type_i),
      std::invalid_argument);
}

TEST_CASE("C++ API: Filter lists", "[cppapi][filter]") {
  using namespace tiledb;
  Context ctx;

  Filter f1(ctx, TILEDB_FILTER_BIT_WIDTH_REDUCTION),
      f2(ctx, TILEDB_FILTER_BZIP2);

  const int32_t set_level = 5;
  f2.set_option(TILEDB_COMPRESSION_LEVEL, &set_level);

  FilterList list(ctx);
  REQUIRE(list.nfilters() == 0);

  REQUIRE(list.max_chunk_size() == 65536);
  list.set_max_chunk_size(10000);
  REQUIRE(list.max_chunk_size() == 10000);

  list.add_filter(f1).add_filter(f2);
  REQUIRE(list.nfilters() == 2);

  Filter f1_get = list.filter(0), f2_get(list.filter(1));
  REQUIRE_THROWS_AS(list.filter(2), TileDBError);
  REQUIRE(f1_get.filter_type() == TILEDB_FILTER_BIT_WIDTH_REDUCTION);
  REQUIRE(f2_get.filter_type() == TILEDB_FILTER_BZIP2);

  int32_t get_level;
  f2_get.get_option(TILEDB_COMPRESSION_LEVEL, &get_level);
  REQUIRE(get_level == set_level);

  list.add_filter({ctx, TILEDB_FILTER_BYTESHUFFLE});
  REQUIRE(list.nfilters() == 3);
}

TEST_CASE("C++ API: Filter lists on array", "[cppapi][filter]") {
  using namespace tiledb;
  Context ctx;
  VFS vfs(ctx);
  std::string array_name = "cpp_unit_array";

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  // Create schema with filter lists
  FilterList a1_filters(ctx);
  a1_filters.set_max_chunk_size(10000);
  a1_filters.add_filter({ctx, TILEDB_FILTER_BYTESHUFFLE})
      .add_filter({ctx, TILEDB_FILTER_BZIP2})
      .add_filter({ctx, TILEDB_FILTER_CHECKSUM_MD5})
      .add_filter({ctx, TILEDB_FILTER_CHECKSUM_SHA256});

  FilterList a2_filters(ctx);
  a2_filters.add_filter({ctx, TILEDB_FILTER_ZSTD})
      .add_filter({ctx, TILEDB_FILTER_CHECKSUM_MD5})
      .add_filter({ctx, TILEDB_FILTER_CHECKSUM_SHA256});

  auto a1 = Attribute::create<int>(ctx, "a1");
  auto a2 = Attribute::create<std::string>(ctx, "a2");
  a1.set_filter_list(a1_filters);
  a2.set_filter_list(a2_filters);

  Domain domain(ctx);
  auto d1 = Dimension::create<int>(ctx, "d1", {{0, 100}}, 10);
  auto d2 = Dimension::create<int>(ctx, "d2", {{0, 100}}, 10);
  domain.add_dimensions(d1, d2);

  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain);
  schema.add_attributes(a1, a2);

  FilterList offsets_filters(ctx);
  offsets_filters.add_filter({ctx, TILEDB_FILTER_POSITIVE_DELTA})
      .add_filter({ctx, TILEDB_FILTER_BYTESHUFFLE})
      .add_filter({ctx, TILEDB_FILTER_LZ4})
      .add_filter({ctx, TILEDB_FILTER_CHECKSUM_MD5})
      .add_filter({ctx, TILEDB_FILTER_CHECKSUM_SHA256});
  schema.set_coords_filter_list(a1_filters)
      .set_offsets_filter_list(offsets_filters);

  // Create array
  Array::create(array_name, schema);

  // Write to array
  std::vector<int> a1_data = {1, 2};
  std::vector<std::string> a2_data = {"abc", "defg"};
  auto a2buf = ungroup_var_buffer(a2_data);
  std::vector<int> coords = {0, 0, 10, 10};
  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array);
  query.set_data_buffer("a1", a1_data)
      .set_data_buffer("a2", a2buf.second)
      .set_offsets_buffer("a2", a2buf.first)
      .set_coordinates(coords)
      .set_layout(TILEDB_UNORDERED);
  REQUIRE(query.submit() == Query::Status::COMPLETE);
  array.close();

  // Sanity check reading
  array.open(TILEDB_READ);
  std::vector<int> subarray = {0, 10, 0, 10};
  std::vector<int> a1_read(2);
  std::vector<uint64_t> a2_read_off(2);
  std::string a2_read_data;
  a2_read_data.resize(7);
  Query query_r(ctx, array);
  query_r.set_subarray(subarray)
      .set_layout(TILEDB_ROW_MAJOR)
      .set_data_buffer("a1", a1_read)
      .set_data_buffer("a2", a2_read_data)
      .set_offsets_buffer("a2", a2_read_off);
  REQUIRE(query_r.submit() == Query::Status::COMPLETE);
  array.close();
  auto ret = query_r.result_buffer_elements();
  REQUIRE(ret.size() == 2);
  REQUIRE(ret["a1"].first == 0);
  REQUIRE(ret["a1"].second == 2);
  REQUIRE(ret["a2"].first == 2);
  REQUIRE(ret["a2"].second == 7);
  REQUIRE(a1_read[0] == 1);
  REQUIRE(a1_read[1] == 2);
  REQUIRE(a2_read_off[0] == 0);
  REQUIRE(a2_read_off[1] == 3);
  REQUIRE(a2_read_data.substr(0, 7) == "abcdefg");

  // Check reading filter lists.
  array.open(TILEDB_READ);
  auto schema_r = array.schema();
  check_filters(a1_filters, schema_r.coords_filter_list());
  check_filters(offsets_filters, schema_r.offsets_filter_list());
  check_filters(a1_filters, schema_r.attribute("a1").filter_list());
  check_filters(a2_filters, schema_r.attribute("a2").filter_list());
  array.close();

  // Clean up
  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

void write_sparse_array_string_attr(
    tiledb::Context ctx,
    const std::string& array_name,
    std::string& data,
    std::vector<uint64_t>& data_offsets,
    tiledb_layout_t layout) {
  using namespace tiledb;
  // Write to array
  std::vector<int64_t> d1 = {0, 10, 20, 20, 30, 30, 40};
  std::vector<int64_t> d2 = {0, 10, 20, 30, 30, 40, 40};

  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array, TILEDB_WRITE);
  query.set_layout(layout);
  query.set_data_buffer("d1", d1);
  query.set_data_buffer("d2", d2);
  query.set_data_buffer("a1", data).set_offsets_buffer("a1", data_offsets);

  bool serialized_writes = false;
  SECTION("no serialization") {
    serialized_writes = false;
  }
  SECTION("serialization enabled global order write") {
#ifdef TILEDB_SERIALIZATION
    serialized_writes = true;
#endif
  }
  if (!serialized_writes || layout != TILEDB_GLOBAL_ORDER) {
    CHECK_NOTHROW(query.submit());
    query.finalize();
  } else {
    test::submit_and_finalize_serialized_query(ctx, query);
  }

  array.close();
}

void read_and_check_sparse_array_string_attr(
    tiledb::Context ctx,
    const std::string& array_name,
    std::string& expected_data,
    std::vector<uint64_t>& expected_offsets,
    tiledb_layout_t layout) {
  using namespace tiledb;
  Array array(ctx, array_name, TILEDB_READ);
  Query query(ctx, array, TILEDB_READ);

  std::string attr_val;
  attr_val.resize(expected_data.size());
  std::vector<uint64_t> attr_off(expected_offsets.size());

  query.set_layout(layout);
  query.set_data_buffer("a1", (char*)attr_val.data(), attr_val.size());
  query.set_offsets_buffer("a1", attr_off);

  CHECK_NOTHROW(query.submit());

  // Check the element offsets are properly returned
  CHECK(attr_val == expected_data);
  CHECK(attr_off == expected_offsets);

  array.close();
}

TEST_CASE(
    "C++ API: Filter strings with RLE or Dictionary encoding, sparse array",
    "[cppapi][filter][rle-strings][dict-strings][sparse]") {
  using namespace tiledb;
  Context ctx;
  VFS vfs(ctx);
  std::string array_name = "cpp_unit_array";

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  auto f = GENERATE(TILEDB_FILTER_RLE, TILEDB_FILTER_DICTIONARY);

  // Create schema with filter lists
  FilterList a1_filters(ctx);
  a1_filters.add_filter({ctx, f});

  auto a1 = Attribute::create<std::string>(ctx, "a1");
  a1.set_cell_val_num(TILEDB_VAR_NUM);
  a1.set_filter_list(a1_filters);

  Domain domain(ctx);
  auto d1 = Dimension::create<int64_t>(ctx, "d1", {{0, 100}}, 10);
  auto d2 = Dimension::create<int64_t>(ctx, "d2", {{0, 100}}, 10);
  domain.add_dimensions(d1, d2);

  ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain);
  schema.add_attribute(a1);
  schema.set_allows_dups(true);

  // Create array
  Array::create(array_name, schema);

  std::string a1_data{
      "foo"
      "foo"
      "foobar"
      "bar"
      "bar"
      "bar"
      "bar"};
  std::vector<uint64_t> a1_offsets{0, 3, 6, 12, 15, 18, 21};

  SECTION("Unordered write") {
    write_sparse_array_string_attr(
        ctx, array_name, a1_data, a1_offsets, TILEDB_UNORDERED);
    SECTION("Row major read") {
      read_and_check_sparse_array_string_attr(
          ctx, array_name, a1_data, a1_offsets, TILEDB_ROW_MAJOR);
    }
    SECTION("Global order read") {
      read_and_check_sparse_array_string_attr(
          ctx, array_name, a1_data, a1_offsets, TILEDB_GLOBAL_ORDER);
    }
    SECTION("Unordered read") {
      read_and_check_sparse_array_string_attr(
          ctx, array_name, a1_data, a1_offsets, TILEDB_UNORDERED);
    }
  }
  SECTION("Global order write") {
    write_sparse_array_string_attr(
        ctx, array_name, a1_data, a1_offsets, TILEDB_GLOBAL_ORDER);
    SECTION("Row major read") {
      read_and_check_sparse_array_string_attr(
          ctx, array_name, a1_data, a1_offsets, TILEDB_ROW_MAJOR);
    }
    SECTION("Global order read") {
      read_and_check_sparse_array_string_attr(
          ctx, array_name, a1_data, a1_offsets, TILEDB_GLOBAL_ORDER);
    }
    SECTION("Unordered read") {
      read_and_check_sparse_array_string_attr(
          ctx, array_name, a1_data, a1_offsets, TILEDB_UNORDERED);
    }
  }

  // Clean up
  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}

void write_dense_array_string_attr(
    tiledb::Context ctx,
    const std::string& array_name,
    std::string& data,
    std::vector<uint64_t>& data_offsets,
    tiledb_layout_t layout) {
  using namespace tiledb;
  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array, TILEDB_WRITE);

  query.set_data_buffer("a1", data);
  query.set_offsets_buffer("a1", data_offsets);
  query.set_layout(layout);
  query.set_subarray<int64_t>({0, 1, 0, 2});

  bool serialized_writes = false;
  SECTION("no serialization") {
    serialized_writes = false;
  }
  SECTION("serialization enabled global order write") {
#ifdef TILEDB_SERIALIZATION
    serialized_writes = true;
#endif
  }
  if (!serialized_writes || layout != TILEDB_GLOBAL_ORDER) {
    CHECK_NOTHROW(query.submit());
    query.finalize();
  } else {
    test::submit_and_finalize_serialized_query(ctx, query);
  }

  array.close();
}

void read_and_check_dense_array_string_attr(
    tiledb::Context ctx,
    const std::string& array_name,
    std::string& expected_data,
    std::vector<uint64_t>& expected_offsets) {
  using namespace tiledb;
  Array array(ctx, array_name, TILEDB_READ);
  Query query(ctx, array, TILEDB_READ);

  std::string attr_val;
  attr_val.resize(expected_data.size());
  std::vector<uint64_t> attr_off(expected_offsets.size());

  query.set_subarray<int64_t>({0, 1, 0, 2});
  query.set_data_buffer("a1", (char*)attr_val.data(), attr_val.size());
  query.set_offsets_buffer("a1", attr_off);
  CHECK_NOTHROW(query.submit());

  // Check the element offsets are properly returned
  CHECK(attr_val == expected_data);
  CHECK(attr_off == expected_offsets);

  array.close();
}

TEST_CASE(
    "C++ API: Filter strings with RLE or Dictionary encoding, dense array",
    "[cppapi][filter][rle-strings][dict-strings][dense]") {
  using namespace tiledb;
  Context ctx;
  VFS vfs(ctx);
  std::string array_name = "cpp_unit_array";

  // Create the array
  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);

  auto f = GENERATE(TILEDB_FILTER_RLE, TILEDB_FILTER_DICTIONARY);
  // Create schema with filter lists
  FilterList a1_filters(ctx);
  a1_filters.add_filter({ctx, f});

  auto a1 = Attribute::create<std::string>(ctx, "a1");
  a1.set_cell_val_num(TILEDB_VAR_NUM);
  a1.set_filter_list(a1_filters);

  Domain domain(ctx);
  auto d1 = Dimension::create<int64_t>(ctx, "d1", {{0, 10}}, 1);
  auto d2 = Dimension::create<int64_t>(ctx, "d2", {{0, 10}}, 1);
  domain.add_dimensions(d1, d2);

  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain);
  schema.set_tile_order(TILEDB_ROW_MAJOR);
  schema.set_cell_order(TILEDB_ROW_MAJOR);
  schema.add_attribute(a1);

  // Create array
  Array::create(array_name, schema);

  std::string a1_data{
      "foo"
      "foo"
      "foobar"
      "bar"
      "bar"
      "bar"};
  std::vector<uint64_t> a1_offsets{0, 3, 6, 12, 15, 18};

  SECTION("Ordered write") {
    write_dense_array_string_attr(
        ctx, array_name, a1_data, a1_offsets, TILEDB_ROW_MAJOR);
    read_and_check_dense_array_string_attr(
        ctx, array_name, a1_data, a1_offsets);
  }
  SECTION("Global order write") {
    write_dense_array_string_attr(
        ctx, array_name, a1_data, a1_offsets, TILEDB_GLOBAL_ORDER);
    read_and_check_dense_array_string_attr(
        ctx, array_name, a1_data, a1_offsets);
  }

  // Clean up
  if (vfs.is_dir(array_name))
    vfs.remove_dir(array_name);
}
