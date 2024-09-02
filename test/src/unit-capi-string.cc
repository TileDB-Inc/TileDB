/**
 * @file   unit-capi-string.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * Tests for TileDB's string support at the C API level.
 */

#include <test/support/tdb_catch.h>
#include "test/support/src/helpers.h"
#include "test/support/src/vfs_helpers.h"
#include "tiledb/sm/c_api/tiledb.h"

#include <cstring>
#include <iostream>

using namespace tiledb::test;

// Since C++20 u8"literals" use char8_t type
// https://www.open-std.org/jtc1/sc22/wg21/docs/papers/2019/p1423r3.html
#if defined(__cpp_char8_t)
const char8_t UTF8_STRINGS[] = u8"aabbccdd";
const char8_t UTF8_STRINGS_VAR[] = u8"aαbββcγγγdδδδδ";
#else
const char UTF8_STRINGS[] = u8"aabbccdd";
const char UTF8_STRINGS_VAR[] = u8"aαbββcγγγdδδδδ";
#endif
uint64_t UTF8_NULL_SIZE = sizeof(u8"");
uint64_t UTF8_OFFSET_0 = 0;
uint64_t UTF8_OFFSET_1 = sizeof(u8"aα") - UTF8_NULL_SIZE;
uint64_t UTF8_OFFSET_2 = sizeof(u8"aαbββ") - UTF8_NULL_SIZE;
uint64_t UTF8_OFFSET_3 = sizeof(u8"aαbββcγγγ") - UTF8_NULL_SIZE;
const char16_t UTF16_STRINGS_VAR[] = u"aαbβcγdδ";
uint64_t UTF16_NULL_SIZE = sizeof(u"");
uint64_t UTF16_OFFSET_0 = 0;
uint64_t UTF16_OFFSET_1 = sizeof(u"aα") - UTF16_NULL_SIZE;
uint64_t UTF16_OFFSET_2 = sizeof(u"aαbβ") - UTF16_NULL_SIZE;
uint64_t UTF16_OFFSET_3 = sizeof(u"aαbβcγ") - UTF16_NULL_SIZE;

struct StringFx {
  StringFx();
  void create_array(const std::string& array_name);
  void read_array(const std::string& array_name);
  void write_array(const std::string& array_name);

  VFSTestSetup vfs_test_setup_;
  tiledb_ctx_t* ctx_;
};

StringFx::StringFx()
    : ctx_(vfs_test_setup_.ctx_c) {
}

// Create a simple dense 1D array with three string attributes
void StringFx::create_array(const std::string& array_name) {
  // Create dimensions
  uint64_t dim_domain[] = {1, 4};
  uint64_t tile_extent = 2;
  tiledb_dimension_t* d1;
  int rc = tiledb_dimension_alloc(
      ctx_, "d1", TILEDB_UINT64, &dim_domain[0], &tile_extent, &d1);
  REQUIRE(rc == TILEDB_OK);

  // Create domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx_, &domain);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d1);
  REQUIRE(rc == TILEDB_OK);

  // Create fixed-sized UTF-8 attribute
  tiledb_attribute_t* a1;
  rc = tiledb_attribute_alloc(ctx_, "a1", TILEDB_STRING_ASCII, &a1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a1, 2);
  REQUIRE(rc == TILEDB_OK);

  // Create variable-sized UTF-8 attribute
  tiledb_attribute_t* a2;
  rc = tiledb_attribute_alloc(ctx_, "a2", TILEDB_STRING_UTF8, &a2);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a2, TILEDB_VAR_NUM);
  REQUIRE(rc == TILEDB_OK);
  rc = set_attribute_compression_filter(ctx_, a2, TILEDB_FILTER_GZIP, -1);
  REQUIRE(rc == TILEDB_OK);

  // Create variable-sized UTF-16 attribute
  tiledb_attribute_t* a3;
  rc = tiledb_attribute_alloc(ctx_, "a3", TILEDB_STRING_UTF16, &a3);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a3, TILEDB_VAR_NUM);
  REQUIRE(rc == TILEDB_OK);
  rc = set_attribute_compression_filter(ctx_, a3, TILEDB_FILTER_ZSTD, -1);
  REQUIRE(rc == TILEDB_OK);

  // Create array schema
  tiledb_array_schema_t* array_schema;
  rc = tiledb_array_schema_alloc(ctx_, TILEDB_DENSE, &array_schema);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a2);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a3);
  REQUIRE(rc == TILEDB_OK);

  // Check array schema
  rc = tiledb_array_schema_check(ctx_, array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Create array
  rc = tiledb_array_create(ctx_, array_name.c_str(), array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  tiledb_attribute_free(&a1);
  tiledb_attribute_free(&a2);
  tiledb_attribute_free(&a3);
  tiledb_dimension_free(&d1);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

void StringFx::write_array(const std::string& array_name) {
  // Prepare buffers
  void* buffer_a1 = std::malloc(sizeof(UTF8_STRINGS) - UTF8_NULL_SIZE);
  uint64_t buffer_a2_offsets[] = {
      UTF8_OFFSET_0, UTF8_OFFSET_1, UTF8_OFFSET_2, UTF8_OFFSET_3};
  void* buffer_a2 = std::malloc(sizeof(UTF8_STRINGS_VAR) - UTF8_NULL_SIZE);
  uint64_t buffer_a3_offsets[] = {
      UTF16_OFFSET_0, UTF16_OFFSET_1, UTF16_OFFSET_2, UTF16_OFFSET_3};
  void* buffer_a3 = std::malloc(sizeof(UTF16_STRINGS_VAR) - UTF16_NULL_SIZE);
  std::memcpy(buffer_a1, UTF8_STRINGS, sizeof(UTF8_STRINGS) - UTF8_NULL_SIZE);
  std::memcpy(
      buffer_a2, UTF8_STRINGS_VAR, sizeof(UTF8_STRINGS_VAR) - UTF8_NULL_SIZE);
  std::memcpy(
      buffer_a3,
      UTF16_STRINGS_VAR,
      sizeof(UTF16_STRINGS_VAR) - UTF16_NULL_SIZE);
  void* buffers[] = {
      buffer_a1, buffer_a2_offsets, buffer_a2, buffer_a3_offsets, buffer_a3};
  uint64_t buffer_sizes[] = {
      sizeof(UTF8_STRINGS) - UTF8_NULL_SIZE,
      4 * sizeof(uint64_t),
      sizeof(UTF8_STRINGS_VAR) - UTF8_NULL_SIZE,
      4 * sizeof(uint64_t),
      sizeof(UTF16_STRINGS_VAR) - UTF16_NULL_SIZE};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  const char* attributes[] = {"a1", "a2", "a3"};
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[1], buffers[2], &buffer_sizes[2]);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, attributes[1], (uint64_t*)buffers[1], &buffer_sizes[1]);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[2], buffers[4], &buffer_sizes[4]);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, attributes[2], (uint64_t*)buffers[3], &buffer_sizes[3]);
  REQUIRE(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit_and_finalize(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  std::free(buffer_a1);
  std::free(buffer_a2);
  std::free(buffer_a3);
}

void StringFx::read_array(const std::string& array_name) {
  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Compute max buffer sizes
  uint64_t subarray[] = {1, 4};
  uint64_t buffer_a1_size = 1024;
  uint64_t buffer_a2_off_size = 1024;
  uint64_t buffer_a2_val_size = 1024;
  uint64_t buffer_a3_off_size = 1024;
  uint64_t buffer_a3_val_size = 1024;

  // Prepare cell buffers
  void* buffer_a1 = std::malloc(buffer_a1_size);
  auto buffer_a2_off = (uint64_t*)std::malloc(buffer_a2_off_size);
  void* buffer_a2_val = std::malloc(buffer_a2_val_size);
  auto buffer_a3_off = (uint64_t*)std::malloc(buffer_a3_off_size);
  void* buffer_a3_val = std::malloc(buffer_a3_val_size);

  // Create query
  tiledb_query_t* query;
  tiledb_subarray_t* sub;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a1", buffer_a1, &buffer_a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a2", buffer_a2_val, &buffer_a2_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, "a2", buffer_a2_off, &buffer_a2_off_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a3", buffer_a3_val, &buffer_a3_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, "a3", buffer_a3_off, &buffer_a3_off_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, sub);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_free(&sub);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Check results
  CHECK(!std::memcmp(
      buffer_a1, UTF8_STRINGS, sizeof(UTF8_STRINGS) - UTF8_NULL_SIZE));
  CHECK(!std::memcmp(
      buffer_a2_val, UTF8_STRINGS_VAR, sizeof(UTF8_STRINGS) - UTF8_NULL_SIZE));
  CHECK(buffer_a2_off[0] == UTF8_OFFSET_0);
  CHECK(buffer_a2_off[1] == UTF8_OFFSET_1);
  CHECK(buffer_a2_off[2] == UTF8_OFFSET_2);
  CHECK(buffer_a2_off[3] == UTF8_OFFSET_3);
  CHECK(!std::memcmp(
      buffer_a3_val,
      UTF16_STRINGS_VAR,
      sizeof(UTF16_STRINGS_VAR) - UTF16_NULL_SIZE));
  CHECK(buffer_a3_off[0] == UTF16_OFFSET_0);
  CHECK(buffer_a3_off[1] == UTF16_OFFSET_1);
  CHECK(buffer_a3_off[2] == UTF16_OFFSET_2);
  CHECK(buffer_a3_off[3] == UTF16_OFFSET_3);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  std::free(buffer_a1);
  std::free(buffer_a2_off);
  std::free(buffer_a2_val);
  std::free(buffer_a3_off);
  std::free(buffer_a3_val);
}

TEST_CASE_METHOD(
    StringFx, "C API: Test string support", "[capi][string][rest]") {
  std::string array_name = vfs_test_setup_.array_uri("foo");
  create_array(array_name);
  write_array(array_name);
  read_array(array_name);
}
