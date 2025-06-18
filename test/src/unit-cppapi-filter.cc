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
#include "test/support/src/helpers.h"
#include "test/support/src/vfs_helpers.h"
#include "tiledb/sm/cpp_api/tiledb"

using namespace tiledb;

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
  Context ctx;

  // Test filter creation and option setting/getting
  Filter f(ctx, TILEDB_FILTER_BZIP2);
  int32_t get_level;
  f.get_option(TILEDB_COMPRESSION_LEVEL, &get_level);
  REQUIRE(get_level == -1);
  REQUIRE(get_level == f.get_option<int32_t>(TILEDB_COMPRESSION_LEVEL));

  // Check void* setter
  int32_t set_level = 5;
  f.set_option(TILEDB_COMPRESSION_LEVEL, &set_level);
  f.get_option(TILEDB_COMPRESSION_LEVEL, &get_level);
  REQUIRE(get_level == 5);
  REQUIRE(get_level == f.get_option<int32_t>(TILEDB_COMPRESSION_LEVEL));

  // Check void* getter
  int32_t get_level_void;
  f.get_option(TILEDB_COMPRESSION_LEVEL, static_cast<void*>(&get_level_void));
  REQUIRE(get_level_void == 5);

  // Check templated version
  f.set_option(TILEDB_COMPRESSION_LEVEL, 4);
  f.get_option(TILEDB_COMPRESSION_LEVEL, &get_level);
  REQUIRE(get_level == 4);
  REQUIRE(get_level == f.get_option<int32_t>(TILEDB_COMPRESSION_LEVEL));

  // Check templated version with wrong type throws exception
  uint32_t wrong_type_u = 1;
  REQUIRE_THROWS_AS(
      f.set_option(TILEDB_COMPRESSION_LEVEL, wrong_type_u), TileDBError);
  REQUIRE_THROWS_AS(
      f.get_option(TILEDB_COMPRESSION_LEVEL, &wrong_type_u), TileDBError);
  REQUIRE_THROWS_AS(
      f.get_option<uint32_t>(TILEDB_COMPRESSION_LEVEL), TileDBError);

  // Check that you can bypass type safety (don't do this).
  f.get_option(TILEDB_COMPRESSION_LEVEL, (void*)&wrong_type_u);
  REQUIRE(wrong_type_u == 4);

  // Unsupported option
  uint32_t window;
  REQUIRE_THROWS_AS(
      f.set_option(TILEDB_BIT_WIDTH_MAX_WINDOW, &window), TileDBError);
  REQUIRE_THROWS_AS(
      f.get_option(TILEDB_BIT_WIDTH_MAX_WINDOW, &window), TileDBError);
  REQUIRE_THROWS_AS(
      f.get_option<uint32_t>(TILEDB_BIT_WIDTH_MAX_WINDOW), TileDBError);

  Filter f2(ctx, TILEDB_FILTER_BIT_WIDTH_REDUCTION);
  int32_t wrong_type_i = 1;
  REQUIRE_THROWS_AS(f2.set_option(TILEDB_COMPRESSION_LEVEL, 1), TileDBError);
  REQUIRE_THROWS_AS(
      f2.set_option(TILEDB_BIT_WIDTH_MAX_WINDOW, -1), TileDBError);
  REQUIRE_THROWS_AS(
      f2.set_option(TILEDB_BIT_WIDTH_MAX_WINDOW, wrong_type_i), TileDBError);
}

TEST_CASE("C++ API: Filter lists", "[cppapi][filter]") {
  Context ctx;

  Filter f1(ctx, TILEDB_FILTER_BIT_WIDTH_REDUCTION),
      f2(ctx, TILEDB_FILTER_BZIP2);

  const int32_t set_level = 5;
  f2.set_option(TILEDB_COMPRESSION_LEVEL, set_level);

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
  REQUIRE(get_level == f2_get.get_option<int32_t>(TILEDB_COMPRESSION_LEVEL));

  list.add_filter({ctx, TILEDB_FILTER_BYTESHUFFLE});
  REQUIRE(list.nfilters() == 3);
}

TEST_CASE(
    "C++ API: Filter lists on array",
    "[cppapi][filter][rest-fails][sc-45554]") {
  test::VFSTestSetup vfs_test_setup{};
  Context ctx{vfs_test_setup.ctx()};
  auto array_name{vfs_test_setup.array_uri("cpp_unit_array")};

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
      .set_data_buffer("__coords", coords)
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
  Subarray sub(ctx, array);
  sub.set_subarray(subarray);
  query_r.set_subarray(sub)
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
}

void write_sparse_array_string_attr(
    tiledb::Context ctx,
    const std::string& array_name,
    std::string& data,
    std::vector<uint64_t>& data_offsets,
    tiledb_layout_t layout) {
  // Write to array
  std::vector<int64_t> d1 = {0, 10, 20, 20, 30, 30, 40};
  std::vector<int64_t> d2 = {0, 10, 20, 30, 30, 40, 40};

  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array, TILEDB_WRITE);
  query.set_layout(layout);
  query.set_data_buffer("d1", d1);
  query.set_data_buffer("d2", d2);
  query.set_data_buffer("a1", data).set_offsets_buffer("a1", data_offsets);

  // Submit query
  if (layout == TILEDB_GLOBAL_ORDER) {
    query.submit_and_finalize();
  } else {
    query.submit();
  }

  array.close();
}

void read_and_check_sparse_array_string_attr(
    tiledb::Context ctx,
    const std::string& array_name,
    std::string& expected_data,
    std::vector<uint64_t>& expected_offsets,
    tiledb_layout_t layout) {
  Array array(ctx, array_name, TILEDB_READ);
  Query query(ctx, array, TILEDB_READ);

  std::string attr_val;
  attr_val.resize(expected_data.size());
  std::vector<uint64_t> attr_off(expected_offsets.size());

  query.set_layout(layout);
  query.set_data_buffer("a1", (char*)attr_val.data(), attr_val.size());
  query.set_offsets_buffer("a1", attr_off);

  // Submit query
  query.submit();

  // Check the element offsets are properly returned
  CHECK(attr_val == expected_data);
  CHECK(attr_off == expected_offsets);

  array.close();
}

TEST_CASE(
    "C++ API: Filter strings with RLE or Dictionary encoding, sparse array",
    "[cppapi][filter][rle-strings][dict-strings][sparse][rest]") {
  auto f = GENERATE(TILEDB_FILTER_RLE, TILEDB_FILTER_DICTIONARY);

  test::VFSTestSetup vfs_test_setup{};
  Context ctx{vfs_test_setup.ctx()};
  auto array_name{vfs_test_setup.array_uri("cpp_unit_array")};

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
  tiledb::Array::delete_array(ctx, array_name);
}

void write_dense_array_string_attr(
    tiledb::Context ctx,
    const std::string& array_name,
    std::string& data,
    std::vector<uint64_t>& data_offsets,
    tiledb_layout_t layout) {
  Array array(ctx, array_name, TILEDB_WRITE);
  Query query(ctx, array, TILEDB_WRITE);

  query.set_data_buffer("a1", data);
  query.set_offsets_buffer("a1", data_offsets);
  query.set_layout(layout);
  query.set_subarray(Subarray(ctx, array).set_subarray<int64_t>({0, 1, 0, 2}));

  if (layout == TILEDB_GLOBAL_ORDER) {
    query.submit_and_finalize();
  } else {
    query.submit();
  }

  array.close();
}

void read_and_check_dense_array_string_attr(
    tiledb::Context ctx,
    const std::string& array_name,
    std::string& expected_data,
    std::vector<uint64_t>& expected_offsets) {
  Array array(ctx, array_name, TILEDB_READ);
  Query query(ctx, array, TILEDB_READ);

  std::string attr_val;
  attr_val.resize(expected_data.size());
  std::vector<uint64_t> attr_off(expected_offsets.size());

  query.set_subarray(Subarray(ctx, array).set_subarray<int64_t>({0, 1, 0, 2}));
  query.set_data_buffer("a1", (char*)attr_val.data(), attr_val.size());
  query.set_offsets_buffer("a1", attr_off);

  // Submit query
  query.submit();

  // Check the element offsets are properly returned
  CHECK(attr_val == expected_data);
  CHECK(attr_off == expected_offsets);

  array.close();
}

TEST_CASE(
    "C++ API: Filter strings with RLE or Dictionary encoding, dense array",
    "[cppapi][filter][rle-strings][dict-strings][dense][rest]") {
  auto f = GENERATE(TILEDB_FILTER_RLE, TILEDB_FILTER_DICTIONARY);

  test::VFSTestSetup vfs_test_setup{};
  Context ctx{vfs_test_setup.ctx()};
  auto array_name{vfs_test_setup.array_uri("cpp_unit_array")};

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
  tiledb::Array::delete_array(ctx, array_name);
}

TEST_CASE(
    "C++ API: Filter UTF-8 strings with RLE or Dictionary encoding, sparse "
    "array",
    "[cppapi][filter][rle-strings][dict-strings][sparse][utf-8][rest]") {
  auto f = GENERATE(TILEDB_FILTER_RLE, TILEDB_FILTER_DICTIONARY);

  test::VFSTestSetup vfs_test_setup{};
  Context ctx{vfs_test_setup.ctx()};
  auto array_name{vfs_test_setup.array_uri("cpp_unit_array")};

  // Create schema with filter lists
  FilterList a1_filters(ctx);
  a1_filters.add_filter({ctx, f});

  auto a1 = Attribute(ctx, "a1", TILEDB_STRING_UTF8);
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

  std::vector<std::string> a1_strings{
      "föö", "föö", "fööbär", "bär", "bär", "bär", "bär"};
  std::vector<uint64_t> a1_offsets;
  a1_offsets.reserve(a1_strings.size());

  size_t offset = 0;
  std::stringstream a1_ss;
  for (auto& s : a1_strings) {
    a1_offsets.emplace_back(offset);
    offset += s.size();
    a1_ss << s;
  }
  std::string a1_data = a1_ss.str();

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
  tiledb::Array::delete_array(ctx, array_name);
}

TEST_CASE(
    "C++ API: Filter buffer with some empty strings with RLE or Dictionary "
    "encoding",
    "[cppapi][filter][rle-strings][dict-strings][empty-strings][rest]") {
  auto f = GENERATE(TILEDB_FILTER_RLE, TILEDB_FILTER_DICTIONARY);

  test::VFSTestSetup vfs_test_setup{};
  Context ctx{vfs_test_setup.ctx()};
  auto array_name{vfs_test_setup.array_uri("cpp_unit_array")};

  // Create array with string dimension and one attribute
  ArraySchema schema(ctx, TILEDB_SPARSE);

  FilterList filters(ctx);
  filters.add_filter({ctx, f});

  auto d0 = Dimension::create(ctx, "d0", TILEDB_STRING_ASCII, nullptr, nullptr);
  d0.set_filter_list(filters);

  Domain domain(ctx);
  domain.add_dimensions(d0);
  schema.set_domain(domain);

  auto a0 = Attribute::create<int32_t>(ctx, "a0");
  schema.add_attributes(a0);
  schema.set_allows_dups(true);

  Array::create(array_name, schema);

  std::vector<char> d0_buf{10};
  std::vector<uint64_t> d0_offsets_buf{10};
  std::vector<int> a0_buf{10};

  SECTION("Only empty strings in data buffer") {
    // Write this data buffer to the array:
    // ["\0","\0","\0","\0","\0","\0","\0","\0","\0","\0"]
    // As data we use the d0 array empty, as already defined.
    d0_offsets_buf.assign(10, 0);
    a0_buf.assign(10, 42);
  }

  SECTION("Combine empty and strings of nulls in data buffer") {
    // Write this data buffer to the array:
    // ["\0","\0","\0","\0","\0","\0","\0","\0","\0","\0\0\0\0\0\0\0\0\0\0"]
    d0_buf.assign(10, 0);
    d0_offsets_buf.assign(10, 0);
    a0_buf.assign(10, 42);
  }

  SECTION("Combine empty and non-empty strings in data buffer") {
    // Write this data buffer to the array of strings: ["a", "bb", "", "c", ""]
    d0_buf.assign({'a', 'b', 'b', 'c'});
    d0_offsets_buf.assign({0, 1, 3, 3, 4});
    a0_buf.assign({42, 42, 42, 42, 42});
  }

  Array array_w(ctx, array_name, TILEDB_WRITE);
  Query query_w(ctx, array_w);
  query_w.set_layout(TILEDB_UNORDERED)
      .set_data_buffer("d0", d0_buf)
      .set_offsets_buffer("d0", d0_offsets_buf)
      .set_data_buffer("a0", a0_buf);
  query_w.submit();
  array_w.close();

  // Read all data and check no error and data correct
  std::vector<char> d0_read_buf(1 << 20);
  std::vector<uint64_t> d0_offsets_read_buf(1 << 20);
  std::vector<int32_t> a0_read_buf(1 << 20);

  Array array_r(ctx, array_name, TILEDB_READ);
  Query query_r(ctx, array_r);
  query_r.set_layout(TILEDB_UNORDERED);
  query_r.set_data_buffer("d0", d0_read_buf)
      .set_offsets_buffer("d0", d0_offsets_read_buf)
      .set_data_buffer("a0", a0_read_buf);

  auto st = query_r.submit();
  REQUIRE(st == Query::Status::COMPLETE);

  auto results = query_r.result_buffer_elements();
  auto num_offsets = results["d0"].first;
  CHECK(num_offsets == d0_offsets_buf.size());
  auto str_len = results["d0"].second;
  CHECK(str_len == d0_buf.size());

  for (uint64_t i = 0; i < num_offsets; i++) {
    CHECK(a0_read_buf[i] == 42);
  }

  array_r.close();
  tiledb::Array::delete_array(ctx, array_name);
}
