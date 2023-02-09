/**
 * @file   unit-capi-any.cc
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
 * Tests for TileDB's ANY datatype at the C API level.
 */

#include <test/support/src/helpers.h>
#include <test/support/tdb_catch.h>
#include "tiledb/sm/c_api/tiledb.h"

#include <cstring>
#include <iostream>

struct AnyFx {
  const int C1 = 5;
  const uint64_t C2 = 100;
  const float C3 = 1.2f;
  const double C4 = 2.3;

  // Serialization parameters
  bool serialize_ = false;
  bool refactored_query_v2_ = false;
  // Buffers to allocate on server side for serialized queries
  tiledb::test::ServerQueryBuffers server_buffers_;

  void create_array(const std::string& array_name);
  void delete_array(const std::string& array_name);
  void read_array(const std::string& array_name);
  void write_array(const std::string& array_name);
};

// Create a simple dense 1D array
void AnyFx::create_array(const std::string& array_name) {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  int rc = tiledb_ctx_alloc(NULL, &ctx);
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

  // Create attribute with datatype `ANY`
  tiledb_attribute_t* a1;
  rc = tiledb_attribute_alloc(ctx, "a1", TILEDB_ANY, &a1);
  REQUIRE(rc == TILEDB_OK);

  // The following is an error - `ANY` datatype is always variable-sized
  rc = tiledb_attribute_set_cell_val_num(ctx, a1, 2);
  REQUIRE(rc == TILEDB_ERR);

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

  // Check array schema
  rc = tiledb_array_schema_check(ctx, array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Create array
  rc = tiledb_array_create(ctx, array_name.c_str(), array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  tiledb_attribute_free(&a1);
  tiledb_dimension_free(&d1);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
  tiledb_ctx_free(&ctx);
}

void AnyFx::write_array(const std::string& array_name) {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  int rc = tiledb_ctx_alloc(NULL, &ctx);
  REQUIRE(rc == TILEDB_OK);

  // Open array
  tiledb_array_t* array;
  rc = tiledb_array_alloc(ctx, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Prepare buffers
  uint64_t buffer_a1_offsets[4];
  char buffer_a1[28];
  // First cell - an int32
  buffer_a1_offsets[0] = 0;
  buffer_a1[0] = TILEDB_INT32;
  std::memcpy(&buffer_a1[1], &C1, sizeof(int));

  // Second cell - a uint64
  buffer_a1_offsets[1] = buffer_a1_offsets[0] + (sizeof(char) + sizeof(int));
  buffer_a1[buffer_a1_offsets[1]] = TILEDB_UINT64;
  std::memcpy(&buffer_a1[buffer_a1_offsets[1] + 1], &C2, sizeof(uint64_t));

  // Third cell - a float32
  buffer_a1_offsets[2] =
      buffer_a1_offsets[1] + (sizeof(char) + sizeof(uint64_t));
  buffer_a1[buffer_a1_offsets[2]] = TILEDB_FLOAT32;
  std::memcpy(&buffer_a1[buffer_a1_offsets[2] + 1], &C3, sizeof(float));

  // Fourth cell - a float64
  buffer_a1_offsets[3] = buffer_a1_offsets[2] + (sizeof(char) + sizeof(float));
  buffer_a1[buffer_a1_offsets[3]] = TILEDB_FLOAT64;
  std::memcpy(&buffer_a1[buffer_a1_offsets[3] + 1], &C4, sizeof(double));

  void* buffers[] = {buffer_a1_offsets, buffer_a1};
  uint64_t buffer_sizes[] = {sizeof(buffer_a1_offsets), sizeof(buffer_a1)};

  // Create query
  tiledb_query_t* query;
  const char* attributes[] = {"a1"};
  rc = tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx, query, TILEDB_GLOBAL_ORDER);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx, query, attributes[0], buffers[1], &buffer_sizes[1]);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx, query, attributes[0], (uint64_t*)buffers[0], &buffer_sizes[0]);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb::test::submit_query_wrapper(
      ctx,
      array_name,
      &query,
      server_buffers_,
      serialize_,
      refactored_query_v2_);
  REQUIRE(rc == TILEDB_OK);
  // Second finalize must create no problem
  rc =
      tiledb::test::finalize_query_wrapper(ctx, array_name, &query, serialize_);
  REQUIRE(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_ctx_free(&ctx);
}

void AnyFx::read_array(const std::string& array_name) {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  int rc = tiledb_ctx_alloc(NULL, &ctx);
  REQUIRE(rc == TILEDB_OK);

  // Open array
  tiledb_array_t* array;
  rc = tiledb_array_alloc(ctx, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Get maximum buffer sizes
  uint64_t size_off = 32;
  uint64_t size_val = 32;
  uint64_t subarray[] = {1, 4};

  // Prepare cell buffers
  auto buffer_a1_off = new uint64_t[size_off / sizeof(uint64_t)];
  auto buffer_a1_val = new char[size_val / sizeof(char)];

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx, query, "a1", buffer_a1_val, &size_val);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx, query, "a1", buffer_a1_off, &size_off);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx, query, TILEDB_GLOBAL_ORDER);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx, query, subarray);
  REQUIRE(rc == TILEDB_OK);

  // Submit query
  rc = tiledb::test::submit_query_wrapper(
      ctx,
      array_name,
      &query,
      server_buffers_,
      serialize_,
      refactored_query_v2_);
  REQUIRE(rc == TILEDB_OK);

  // Check results
  CHECK(size_off == 32);
  CHECK(size_val == 28);
  CHECK(buffer_a1_off[0] == 0);
  CHECK(buffer_a1_off[1] == sizeof(char) + sizeof(int));
  CHECK(buffer_a1_off[2] == buffer_a1_off[1] + sizeof(char) + sizeof(uint64_t));
  CHECK(buffer_a1_off[3] == buffer_a1_off[2] + sizeof(char) + sizeof(float));
  CHECK(buffer_a1_val[0] == TILEDB_INT32);
  CHECK(!std::memcmp(&buffer_a1_val[1], &C1, sizeof(int)));
  CHECK(buffer_a1_val[buffer_a1_off[1]] == TILEDB_UINT64);
  CHECK(!std::memcmp(
      &buffer_a1_val[buffer_a1_off[1] + 1], &C2, sizeof(uint64_t)));
  CHECK(buffer_a1_val[buffer_a1_off[2]] == TILEDB_FLOAT32);
  CHECK(!std::memcmp(&buffer_a1_val[buffer_a1_off[2] + 1], &C3, sizeof(float)));
  CHECK(buffer_a1_val[buffer_a1_off[3]] == TILEDB_FLOAT64);
  CHECK(
      !std::memcmp(&buffer_a1_val[buffer_a1_off[3] + 1], &C4, sizeof(double)));

  // Close array
  rc = tiledb_array_close(ctx, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_ctx_free(&ctx);
  delete[] buffer_a1_off;
  delete[] buffer_a1_val;
}

void AnyFx::delete_array(const std::string& array_name) {
  // Create TileDB context
  tiledb_ctx_t* ctx;
  int rc = tiledb_ctx_alloc(NULL, &ctx);
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

TEST_CASE_METHOD(AnyFx, "C API: Test `ANY` datatype", "[capi][any]") {
  SECTION("no serialization") {
    serialize_ = false;
  }
#ifdef TILEDB_SERIALIZATION
  SECTION("serialization enabled global order write") {
    serialize_ = true;
    refactored_query_v2_ = GENERATE(true, false);
  }
#endif

  std::string array_name = "foo";
  delete_array(array_name);
  create_array(array_name);
  write_array(array_name);
  read_array(array_name);
  delete_array(array_name);
}
