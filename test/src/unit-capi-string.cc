/**
 * @file   unit-capi-string.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB Inc.
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

#include "catch.hpp"
#include "test/src/helpers.h"
#include "tiledb/sm/c_api/tiledb.h"

#include <cstring>
#include <iostream>

const char UTF8_STRINGS[] = u8"aabbccdd";
const char UTF8_STRINGS_VAR[] = u8"aαbββcγγγdδδδδ";
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
  void create_array(const std::string& array_name);
  void delete_array(const std::string& array_name);
  void read_array(const std::string& array_name);
  void write_array(const std::string& array_name);
};

// Create a simple dense 1D array with three string attributes
void StringFx::create_array(const std::string& array_name) {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  int rc = tiledb_ctx_alloc(nullptr, &ctx);
  REQUIRE(rc == TILEDB_OK);

  // Create dimensions
  uint64_t dim_domain[] = {1, 4};
  uint64_t tile_extent = 2;
  tiledb_dimension_t* d1;
  rc = tiledb_dimension_alloc(
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
  rc = tiledb_attribute_alloc(ctx, "a1", TILEDB_STRING_ASCII, &a1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx, a1, 2);
  REQUIRE(rc == TILEDB_OK);

  // Create variable-sized UTF-8 attribute
  tiledb_attribute_t* a2;
  rc = tiledb_attribute_alloc(ctx, "a2", TILEDB_STRING_UTF8, &a2);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx, a2, TILEDB_VAR_NUM);
  REQUIRE(rc == TILEDB_OK);
  rc = set_attribute_compression_filter(ctx, a2, TILEDB_FILTER_GZIP, -1);
  REQUIRE(rc == TILEDB_OK);

  // Create variable-sized UTF-16 attribute
  tiledb_attribute_t* a3;
  rc = tiledb_attribute_alloc(ctx, "a3", TILEDB_STRING_UTF16, &a3);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx, a3, TILEDB_VAR_NUM);
  REQUIRE(rc == TILEDB_OK);
  rc = set_attribute_compression_filter(ctx, a3, TILEDB_FILTER_ZSTD, -1);
  REQUIRE(rc == TILEDB_OK);

  // Create array schema
  tiledb_array_schema_t* array_schema;
  rc = tiledb_array_schema_alloc(ctx, TILEDB_DENSE, &array_schema);
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
  tiledb_dimension_free(&d1);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
  tiledb_ctx_free(&ctx);
}

void StringFx::write_array(const std::string& array_name) {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  int rc = tiledb_ctx_alloc(nullptr, &ctx);
  REQUIRE(rc == TILEDB_OK);

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
  uint64_t buffer_sizes[] = {sizeof(UTF8_STRINGS) - UTF8_NULL_SIZE,
                             4 * sizeof(uint64_t),
                             sizeof(UTF8_STRINGS_VAR) - UTF8_NULL_SIZE,
                             4 * sizeof(uint64_t),
                             sizeof(UTF16_STRINGS_VAR) - UTF16_NULL_SIZE};

  // Open array
  tiledb_array_t* array;
  rc = tiledb_array_alloc(ctx, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  const char* attributes[] = {"a1", "a2", "a3"};
  rc = tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx, query, TILEDB_GLOBAL_ORDER);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx, query, attributes[0], buffers[0], &buffer_sizes[0]);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx,
      query,
      attributes[1],
      (uint64_t*)buffers[1],
      &buffer_sizes[1],
      buffers[2],
      &buffer_sizes[2]);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx,
      query,
      attributes[2],
      (uint64_t*)buffers[3],
      &buffer_sizes[3],
      buffers[4],
      &buffer_sizes[4]);
  REQUIRE(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx, query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx, query);
  REQUIRE(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_ctx_free(&ctx);
  std::free(buffer_a1);
  std::free(buffer_a2);
  std::free(buffer_a3);
}

void StringFx::read_array(const std::string& array_name) {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  int rc = tiledb_ctx_alloc(nullptr, &ctx);
  REQUIRE(rc == TILEDB_OK);

  // Open array
  tiledb_array_t* array;
  rc = tiledb_array_alloc(ctx, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Compute max buffer sizes
  uint64_t subarray[] = {1, 4};
  uint64_t buffer_a1_size, buffer_a2_off_size, buffer_a2_val_size,
      buffer_a3_off_size, buffer_a3_val_size;
  rc =
      tiledb_array_max_buffer_size(ctx, array, "a1", subarray, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size_var(
      ctx, array, "a2", subarray, &buffer_a2_off_size, &buffer_a2_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size_var(
      ctx, array, "a3", subarray, &buffer_a3_off_size, &buffer_a3_val_size);
  CHECK(rc == TILEDB_OK);

  // Prepare cell buffers
  void* buffer_a1 = std::malloc(buffer_a1_size);
  auto buffer_a2_off = (uint64_t*)std::malloc(buffer_a2_off_size);
  void* buffer_a2_val = std::malloc(buffer_a2_val_size);
  auto buffer_a3_off = (uint64_t*)std::malloc(buffer_a3_off_size);
  void* buffer_a3_val = std::malloc(buffer_a3_val_size);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx, query, "a1", buffer_a1, &buffer_a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx,
      query,
      "a2",
      buffer_a2_off,
      &buffer_a2_off_size,
      buffer_a2_val,
      &buffer_a2_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx,
      query,
      "a3",
      buffer_a3_off,
      &buffer_a3_off_size,
      buffer_a3_val,
      &buffer_a3_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx, query, TILEDB_GLOBAL_ORDER);
  REQUIRE(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx, query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx, query);
  REQUIRE(rc == TILEDB_OK);

  // Check results
  CHECK(buffer_a1_size == sizeof(UTF8_STRINGS) - UTF8_NULL_SIZE);
  CHECK(buffer_a2_off_size == 4 * sizeof(uint64_t));
  CHECK(buffer_a2_val_size == sizeof(UTF8_STRINGS_VAR) - UTF8_NULL_SIZE);
  CHECK(buffer_a3_off_size == 4 * sizeof(uint64_t));
  CHECK(buffer_a3_val_size == sizeof(UTF16_STRINGS_VAR) - UTF16_NULL_SIZE);
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
  rc = tiledb_array_close(ctx, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_ctx_free(&ctx);
  std::free(buffer_a1);
  std::free(buffer_a2_off);
  std::free(buffer_a2_val);
  std::free(buffer_a3_off);
  std::free(buffer_a3_val);
}

void StringFx::delete_array(const std::string& array_name) {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  int rc = tiledb_ctx_alloc(nullptr, &ctx);
  REQUIRE(rc == TILEDB_OK);

  // Remove array
  tiledb_object_t type;
  rc = tiledb_object_type(ctx, array_name.c_str(), &type);
  REQUIRE(rc == TILEDB_OK);
  if (type == TILEDB_ARRAY) {
    rc = tiledb_object_remove(ctx, array_name.c_str());
    REQUIRE(rc == TILEDB_OK);
  }

  // Clean up
  tiledb_ctx_free(&ctx);
}

TEST_CASE_METHOD(StringFx, "C API: Test string support", "[capi], [string]") {
  std::string array_name = "foo";
  delete_array(array_name);
  create_array(array_name);
  write_array(array_name);
  read_array(array_name);
  delete_array(array_name);
}
