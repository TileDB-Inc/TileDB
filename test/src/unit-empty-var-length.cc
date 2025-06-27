/**
 * @file   unit-empty-var-length.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2020-2021 TileDB Inc.
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
 * Tests for TileDB's support for empty var length values at the C API level.
 */

#include <test/support/tdb_catch.h>
#include "test/support/src/helpers.h"
#include "test/support/src/vfs_helpers.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/cpp_api/tiledb"

#include <cstring>
#include <iostream>

using namespace tiledb::test;
using namespace tiledb;

float buffer_a1[4] = {0.0f, 0.1f, 0.2f, 0.3f};
int32_t buffer_a4[4] = {1, 2, 3, 4};
// Since C++20 u8"literals" use char8_t type
// https://www.open-std.org/jtc1/sc22/wg21/docs/papers/2019/p1423r3.html
#if defined(__cpp_char8_t)
const char8_t UTF8_STRINGS_VAR_FOR_EMPTY[] = u8"aαbββcγγγdδδδδ";
#else
const char UTF8_STRINGS_VAR_FOR_EMPTY[] = u8"aαbββcγγγdδδδδ";
#endif
uint64_t UTF8_NULL_SIZE_FOR_EMPTY = sizeof(u8"");
uint64_t UTF8_OFFSET_0_FOR_EMPTY = 0;
uint64_t UTF8_OFFSET_1_FOR_EMPTY = sizeof(u8"aα") - UTF8_NULL_SIZE_FOR_EMPTY;
uint64_t UTF8_OFFSET_2_FOR_EMPTY = sizeof(u8"aαbββ") - UTF8_NULL_SIZE_FOR_EMPTY;
uint64_t UTF8_OFFSET_3_FOR_EMPTY =
    sizeof(u8"aαbββcγγγ") - UTF8_NULL_SIZE_FOR_EMPTY;
uint64_t UTF8_OFFSET_4_FOR_EMPTY =
    sizeof(UTF8_STRINGS_VAR_FOR_EMPTY) - UTF8_NULL_SIZE_FOR_EMPTY;

struct StringEmptyFx {
  VFSTestSetup vfs_test_setup_;
  tiledb_ctx_t* ctx;
  StringEmptyFx();
  void create_array(const std::string& array_name);
  void read_array(const std::string& array_name);
  void write_array(const std::string& array_name);
};

StringEmptyFx::StringEmptyFx()
    : ctx(vfs_test_setup_.ctx_c) {
}

// Create a simple dense 1D array with three string attributes
void StringEmptyFx::create_array(const std::string& array_name) {
  // Create dimensions
  uint64_t dim_domain[] = {1, 8};
  uint64_t tile_extent = 2;
  tiledb_dimension_t* d1;
  int rc = tiledb_dimension_alloc(
      ctx, "d1", TILEDB_UINT64, &dim_domain[0], &tile_extent, &d1);
  REQUIRE(rc == TILEDB_OK);

  // Create domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx, &domain);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx, domain, d1);
  REQUIRE(rc == TILEDB_OK);

  // Create fixed-sized UTF-8 attribute
  tiledb_attribute_t* a1;
  rc = tiledb_attribute_alloc(ctx, "a1", TILEDB_FLOAT32, &a1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx, a1, TILEDB_VAR_NUM);
  REQUIRE(rc == TILEDB_OK);

  // Create variable-sized UTF-8 attribute
  tiledb_attribute_t* a2;
  rc = tiledb_attribute_alloc(ctx, "a2", TILEDB_STRING_UTF8, &a2);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx, a2, TILEDB_VAR_NUM);
  REQUIRE(rc == TILEDB_OK);
  rc = set_attribute_compression_filter(ctx, a2, TILEDB_FILTER_GZIP, -1);
  REQUIRE(rc == TILEDB_OK);

  // Create another variable-sized UTF-8 attribute
  tiledb_attribute_t* a3;
  rc = tiledb_attribute_alloc(ctx, "a3", TILEDB_STRING_UTF8, &a3);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx, a3, TILEDB_VAR_NUM);
  REQUIRE(rc == TILEDB_OK);
  rc = set_attribute_compression_filter(ctx, a3, TILEDB_FILTER_GZIP, -1);
  REQUIRE(rc == TILEDB_OK);

  // Create variable-sized UTF-16 attribute
  tiledb_attribute_t* a4;
  rc = tiledb_attribute_alloc(ctx, "a4", TILEDB_INT32, &a4);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx, a4, TILEDB_VAR_NUM);
  REQUIRE(rc == TILEDB_OK);
  rc = set_attribute_compression_filter(ctx, a4, TILEDB_FILTER_ZSTD, -1);
  REQUIRE(rc == TILEDB_OK);

  // Create array schema
  tiledb_array_schema_t* array_schema;
  rc = tiledb_array_schema_alloc(ctx, TILEDB_SPARSE, &array_schema);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(ctx, array_schema, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx, array_schema, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx, array_schema, domain);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx, array_schema, a1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx, array_schema, a2);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx, array_schema, a3);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx, array_schema, a4);
  REQUIRE(rc == TILEDB_OK);

  // Check array schema
  rc = tiledb_array_schema_check(ctx, array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Create array
  rc = tiledb_array_create(ctx, array_name.c_str(), array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  tiledb_attribute_free(&a1);
  tiledb_attribute_free(&a2);
  tiledb_attribute_free(&a3);
  tiledb_attribute_free(&a4);
  tiledb_dimension_free(&d1);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

void StringEmptyFx::write_array(const std::string& array_name) {
  // Prepare buffers
  uint64_t buffer_a1_size = sizeof(buffer_a1);
  uint64_t buffer_a1_offsets[] = {
      0 * sizeof(int32_t),
      1 * sizeof(int32_t),
      2 * sizeof(int32_t),
      2 * sizeof(int32_t),
      3 * sizeof(int32_t)};
  uint64_t buffer_a1_offsets_size = sizeof(buffer_a1_offsets);

  uint64_t buffer_a2_offsets[] = {
      UTF8_OFFSET_0_FOR_EMPTY,
      UTF8_OFFSET_1_FOR_EMPTY,
      UTF8_OFFSET_2_FOR_EMPTY,
      UTF8_OFFSET_3_FOR_EMPTY,
      UTF8_OFFSET_3_FOR_EMPTY};
  uint64_t buffer_a2_offsets_size = sizeof(buffer_a2_offsets);
  uint64_t buffer_a2_size =
      sizeof(UTF8_STRINGS_VAR_FOR_EMPTY) - UTF8_NULL_SIZE_FOR_EMPTY;
  void* buffer_a2 = std::malloc(buffer_a2_size);

  std::memcpy(
      buffer_a2,
      UTF8_STRINGS_VAR_FOR_EMPTY,
      sizeof(UTF8_STRINGS_VAR_FOR_EMPTY) - UTF8_NULL_SIZE_FOR_EMPTY);

  uint64_t buffer_a3_offsets[] = {
      UTF8_OFFSET_0_FOR_EMPTY,
      UTF8_OFFSET_1_FOR_EMPTY,
      UTF8_OFFSET_4_FOR_EMPTY,
      UTF8_OFFSET_4_FOR_EMPTY,
      UTF8_OFFSET_4_FOR_EMPTY};
  uint64_t buffer_a3_offsets_size = sizeof(buffer_a3_offsets);
  uint64_t buffer_a3_size =
      sizeof(UTF8_STRINGS_VAR_FOR_EMPTY) - UTF8_NULL_SIZE_FOR_EMPTY;
  void* buffer_a3 = std::malloc(buffer_a3_size);

  std::memcpy(
      buffer_a3,
      UTF8_STRINGS_VAR_FOR_EMPTY,
      sizeof(UTF8_STRINGS_VAR_FOR_EMPTY) - UTF8_NULL_SIZE_FOR_EMPTY);

  uint64_t buffer_a4_offsets[] = {
      0 * sizeof(float),
      1 * sizeof(float),
      2 * sizeof(float),
      3 * sizeof(float),
      4 * sizeof(float)};
  uint64_t buffer_a4_offsets_size = sizeof(buffer_a4_offsets);
  uint64_t buffer_a4_size = sizeof(buffer_a4);

  uint64_t buffer_d1[5] = {1, 2, 3, 4, 5};
  uint64_t buffer_size_d1 = sizeof(buffer_d1);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx, query, TILEDB_GLOBAL_ORDER);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx, query, "d1", buffer_d1, &buffer_size_d1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx, query, "a1", buffer_a1, &buffer_a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx, query, "a1", buffer_a1_offsets, &buffer_a1_offsets_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx, query, "a2", buffer_a2, &buffer_a2_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx, query, "a2", buffer_a2_offsets, &buffer_a2_offsets_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx, query, "a3", buffer_a3, &buffer_a3_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx, query, "a3", buffer_a3_offsets, &buffer_a3_offsets_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx, query, "a4", buffer_a4, &buffer_a4_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx, query, "a4", buffer_a4_offsets, &buffer_a4_offsets_size);
  REQUIRE(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit_and_finalize(ctx, query);
  REQUIRE(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  std::free(buffer_a2);
  std::free(buffer_a3);
}

void StringEmptyFx::read_array(const std::string& array_name) {
  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  tiledb_subarray_t* sub;
  rc = tiledb_query_alloc(ctx, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx, query, TILEDB_GLOBAL_ORDER);
  REQUIRE(rc == TILEDB_OK);

  // Set subarray
  uint64_t subarray[] = {1, 5};
  rc = tiledb_subarray_alloc(ctx, array, &sub);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx, sub, subarray);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx, query, sub);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_free(&sub);

  // Set buffer
  uint64_t buffer_d1_size = 1024;
  uint64_t buffer_a1_val_size = 1024;
  uint64_t buffer_a1_off_size = 1024;
  uint64_t buffer_a2_off_size = 1024;
  uint64_t buffer_a2_val_size = 1024;
  uint64_t buffer_a3_off_size = 1024;
  uint64_t buffer_a3_val_size = 1024;
  uint64_t buffer_a4_off_size = 1024;
  uint64_t buffer_a4_val_size = 1024;

  // Check est_result_sizes
  rc = tiledb_query_get_est_result_size(ctx, query, "d1", &buffer_d1_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_get_est_result_size_var(
      ctx, query, "a1", &buffer_a1_off_size, &buffer_a1_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_get_est_result_size_var(
      ctx, query, "a2", &buffer_a2_off_size, &buffer_a2_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_get_est_result_size_var(
      ctx, query, "a4", &buffer_a4_off_size, &buffer_a4_val_size);
  REQUIRE(rc == TILEDB_OK);

  CHECK(buffer_d1_size == 5 * sizeof(uint64_t));
  // est_result_size should return 4 element for the values since one is empty
  CHECK(buffer_a1_val_size == 4 * sizeof(float));
  // we will get 5 offsets
  CHECK(buffer_a1_off_size == 5 * sizeof(uint64_t));

  // Again we expect 4 element for the values since one is empty
  CHECK(
      buffer_a2_val_size ==
      sizeof(UTF8_STRINGS_VAR_FOR_EMPTY) - UTF8_NULL_SIZE_FOR_EMPTY);
  // we will get 5 offsets
  CHECK(buffer_a2_off_size == 5 * sizeof(uint64_t));

  // Again we expect 4 element for the values since one is empty
  CHECK(buffer_a4_val_size == 4 * sizeof(int32_t));
  // we will get 5 offsets
  CHECK(buffer_a4_off_size == 5 * sizeof(uint64_t));

  // Prepare cell buffers
  void* buffer_d1 = std::malloc(buffer_d1_size);
  void* buffer_a1_val = std::malloc(buffer_a1_val_size);
  auto* buffer_a1_off = (uint64_t*)std::malloc(buffer_a1_off_size);
  auto buffer_a2_off = (uint64_t*)std::malloc(buffer_a2_off_size);
  void* buffer_a2_val = std::malloc(buffer_a2_val_size);
  auto buffer_a3_off = (uint64_t*)std::malloc(buffer_a3_off_size);
  void* buffer_a3_val = std::malloc(buffer_a3_val_size);
  auto buffer_a4_off = (uint64_t*)std::malloc(buffer_a4_off_size);
  void* buffer_a4_val = std::malloc(buffer_a4_val_size);

  rc = tiledb_query_set_data_buffer(
      ctx, query, "d1", buffer_d1, &buffer_d1_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx, query, "a1", buffer_a1_val, &buffer_a1_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx, query, "a1", buffer_a1_off, &buffer_a1_off_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx, query, "a2", buffer_a2_val, &buffer_a2_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx, query, "a2", buffer_a2_off, &buffer_a2_off_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx, query, "a3", buffer_a3_val, &buffer_a3_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx, query, "a3", buffer_a3_off, &buffer_a3_off_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx, query, "a4", buffer_a4_val, &buffer_a4_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx, query, "a4", buffer_a4_off, &buffer_a4_off_size);
  REQUIRE(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx, query);
  REQUIRE(rc == TILEDB_OK);

  // Check results
  CHECK(!std::memcmp(buffer_a1_val, buffer_a1, sizeof(buffer_a1)));
  CHECK(buffer_a1_off[0] == 0 * sizeof(int32_t));
  CHECK(buffer_a1_off[1] == 1 * sizeof(int32_t));
  CHECK(buffer_a1_off[2] == 2 * sizeof(int32_t));
  CHECK(buffer_a1_off[3] == 2 * sizeof(int32_t));
  CHECK(buffer_a1_off[4] == 3 * sizeof(int32_t));

  CHECK(!std::memcmp(
      buffer_a2_val,
      UTF8_STRINGS_VAR_FOR_EMPTY,
      sizeof(UTF8_STRINGS_VAR_FOR_EMPTY) - UTF8_NULL_SIZE_FOR_EMPTY));
  CHECK(buffer_a2_off[0] == UTF8_OFFSET_0_FOR_EMPTY);
  CHECK(buffer_a2_off[1] == UTF8_OFFSET_1_FOR_EMPTY);
  CHECK(buffer_a2_off[2] == UTF8_OFFSET_2_FOR_EMPTY);
  CHECK(buffer_a2_off[3] == UTF8_OFFSET_3_FOR_EMPTY);
  CHECK(buffer_a2_off[4] == UTF8_OFFSET_3_FOR_EMPTY);

  CHECK(!std::memcmp(
      buffer_a3_val,
      UTF8_STRINGS_VAR_FOR_EMPTY,
      sizeof(UTF8_STRINGS_VAR_FOR_EMPTY) - UTF8_NULL_SIZE_FOR_EMPTY));
  CHECK(buffer_a3_off[0] == UTF8_OFFSET_0_FOR_EMPTY);
  CHECK(buffer_a3_off[1] == UTF8_OFFSET_1_FOR_EMPTY);
  CHECK(buffer_a3_off[2] == UTF8_OFFSET_4_FOR_EMPTY);
  CHECK(buffer_a3_off[3] == UTF8_OFFSET_4_FOR_EMPTY);
  CHECK(buffer_a3_off[4] == UTF8_OFFSET_4_FOR_EMPTY);

  CHECK(!std::memcmp(buffer_a4_val, buffer_a4, sizeof(buffer_a4)));
  CHECK(buffer_a4_off[0] == 0 * sizeof(float));
  CHECK(buffer_a4_off[1] == 1 * sizeof(float));
  CHECK(buffer_a4_off[2] == 2 * sizeof(float));
  CHECK(buffer_a4_off[3] == 3 * sizeof(float));
  CHECK(buffer_a4_off[4] == 4 * sizeof(float));

  // Close array
  rc = tiledb_array_close(ctx, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  std::free(buffer_d1);
  std::free(buffer_a1_off);
  std::free(buffer_a1_val);
  std::free(buffer_a2_off);
  std::free(buffer_a2_val);
  std::free(buffer_a3_off);
  std::free(buffer_a3_val);
  std::free(buffer_a4_off);
  std::free(buffer_a4_val);
}

TEST_CASE_METHOD(
    StringEmptyFx,
    "C API: Test empty support",
    "[capi][empty-var-length][rest]") {
  std::string array_name = vfs_test_setup_.array_uri("empty_string");
  create_array(array_name);
  write_array(array_name);
  read_array(array_name);
}

struct StringEmptyFx2 {
  VFSTestSetup vfs_test_setup_;
  Context ctx;
  StringEmptyFx2();
  void create_array(const std::string& array_name);
  void write_array(const std::string& array_name);
  void read_array(const std::string& array_name);

  std::vector<uint64_t> offsets = {
      0, 4, 8, 11, 13, 14, 14, 17, 21, 24, 24, 24, 27, 32, 35, 38};
  std::vector<char> data = {
      '%',    '-', '9',    '\x1e', '\x16', '[', 'q',    '\x1c', '&',    'Y',
      '@',    '>', 'z',    'a',    'P',    '&', '\x19', 'T',    '\x19', 'y',
      '\x0b', 'k', '\x03', '2',    '5',    '|', '4',    't',    '.',    'd',
      '$',    'e', '1',    '\x17', ' ',    '1', '\x14', '('};
};

StringEmptyFx2::StringEmptyFx2()
    : ctx(vfs_test_setup_.ctx()) {};

void StringEmptyFx2::create_array(const std::string& array_name) {
  Domain domain(ctx);
  domain.add_dimension(Dimension::create<uint64_t>(ctx, "__dim_0", {0, 3}, 1));
  domain.add_dimension(Dimension::create<uint64_t>(ctx, "__dim_1", {0, 3}, 1));

  ArraySchema schema(ctx, TILEDB_DENSE);
  schema.set_domain(domain).set_order({TILEDB_ROW_MAJOR});

  auto attr = Attribute(ctx, "", TILEDB_STRING_UTF8);
  attr.set_cell_val_num(TILEDB_VAR_NUM);
  schema.add_attribute(attr);

  Array::create(array_name, schema);
}

void StringEmptyFx2::write_array(const std::string& array_name) {
  auto array = tiledb::Array(ctx, array_name, TILEDB_WRITE);

  Query query(ctx, array, TILEDB_WRITE);
  query.set_data_buffer(
      "", (char*)StringEmptyFx2::data.data(), StringEmptyFx2::data.size());
  query.set_offsets_buffer(
      "", StringEmptyFx2::offsets.data(), StringEmptyFx2::offsets.size());

  query.submit();

  REQUIRE(query.query_status() == tiledb::Query::Status::COMPLETE);
}

void StringEmptyFx2::read_array(const std::string& array_name) {
  std::vector<uint64_t> r_offsets(16);
  std::vector<char> r_data(38);

  auto array = tiledb::Array(ctx, array_name, TILEDB_READ);
  Query query(ctx, array, TILEDB_READ);
  query.set_data_buffer("", r_data);
  query.set_offsets_buffer("", r_offsets);

  Subarray subarray(ctx, array);
  subarray.add_range(0, (uint64_t)0, (uint64_t)3);
  subarray.add_range(1, (uint64_t)0, (uint64_t)3);
  query.set_subarray(subarray);

  query.submit();

  REQUIRE(r_offsets == StringEmptyFx2::offsets);
  REQUIRE(r_data == StringEmptyFx2::data);
}

TEST_CASE_METHOD(
    StringEmptyFx2,
    "C++ API: Test empty support",
    "[cppapi][empty-var-length][rest]") {
  std::string array_name = vfs_test_setup_.array_uri("empty_string2");
  create_array(array_name);
  write_array(array_name);
  read_array(array_name);
}

struct StringEmptyFx3 {
  VFSTestSetup vfs_test_setup_;
  Context ctx;
  StringEmptyFx3();
  std::vector<uint64_t> offsets = {
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
  std::vector<char> data = {'\0'};
};

StringEmptyFx3::StringEmptyFx3()
    : ctx(vfs_test_setup_.ctx()) {};

TEST_CASE_METHOD(
    StringEmptyFx3,
    "C++ API: Test var-length string only empty",
    "[cppapi][empty-var-length][rest]") {
  std::string array_name = vfs_test_setup_.array_uri("empty_string3");

  // create
  {
    Domain domain(ctx);
    domain.add_dimension(
        Dimension::create<uint64_t>(ctx, "__dim_0", {0, 3}, 1));
    domain.add_dimension(
        Dimension::create<uint64_t>(ctx, "__dim_1", {0, 3}, 1));

    ArraySchema schema(ctx, TILEDB_DENSE);
    schema.set_domain(domain).set_order({TILEDB_ROW_MAJOR});

    auto attr = Attribute(ctx, "", TILEDB_STRING_UTF8);
    attr.set_cell_val_num(TILEDB_VAR_NUM);
    schema.add_attribute(attr);

    Array::create(array_name, schema);
  }

  // write
  {
    auto array = tiledb::Array(ctx, array_name, TILEDB_WRITE);

    Query query(ctx, array, TILEDB_WRITE);
    query.set_data_buffer(
        "", (char*)StringEmptyFx3::data.data(), StringEmptyFx3::data.size());
    query.set_offsets_buffer(
        "", StringEmptyFx3::offsets.data(), StringEmptyFx3::offsets.size());

    query.submit();
  }

  // read whole array
  {
    std::vector<uint64_t> r_offsets(16);
    std::vector<char> r_data(16);

    auto array = tiledb::Array(ctx, array_name, TILEDB_READ);
    Query query(ctx, array, TILEDB_READ);
    query.set_data_buffer("", r_data);
    query.set_offsets_buffer("", r_offsets);

    Subarray subarray(ctx, array);
    subarray.add_range(0, (uint64_t)0, (uint64_t)3);
    subarray.add_range(1, (uint64_t)0, (uint64_t)3);
    query.set_subarray(subarray);

    query.submit();

    auto result_els = query.result_buffer_elements();

    REQUIRE(result_els[""].first == 16);
    REQUIRE(result_els[""].second == 1);
    REQUIRE(r_offsets == StringEmptyFx3::offsets);
    REQUIRE(r_data[0] == StringEmptyFx3::data[0]);
  }

  // read subset of array: note that the offsets are sequentially
  // 0s and the data buffer is empty in this case because all of
  // the queried cells are empty.
  {
    std::vector<uint64_t> r_offsets(4);
    std::vector<char> r_data(4);

    auto array = tiledb::Array(ctx, array_name, TILEDB_READ);
    Query query(ctx, array, TILEDB_READ);
    query.set_data_buffer("", r_data);
    query.set_offsets_buffer("", r_offsets);

    Subarray subarray(ctx, array);
    subarray.add_range(0, (uint64_t)0, (uint64_t)1);
    subarray.add_range(1, (uint64_t)1, (uint64_t)2);
    query.set_subarray(subarray);

    query.submit();

    auto result_els = query.result_buffer_elements();

    REQUIRE(result_els[""].first == 4);
    REQUIRE(result_els[""].second == 0);

    std::vector<uint64_t> q2_result_offsets = {0, 0, 0, 0};
    REQUIRE(r_offsets == q2_result_offsets);
    REQUIRE(r_data[0] == StringEmptyFx3::data[0]);
  }
}
