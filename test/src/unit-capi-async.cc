/**
 * @file   unit-capi-async.cc
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
 * Tests the C API async queries.
 */

#include "catch.hpp"
#include "tiledb/sm/c_api/tiledb.h"

#include <cstring>

/** Tests for C API async queries. */
struct AsyncFx {
  // Constants
  const char* DENSE_ARRAY_NAME = "test_async_dense";
  const char* SPARSE_ARRAY_NAME = "test_async_sparse";

  // TileDB context
  tiledb_ctx_t* ctx_;

  // Constructors/destructors
  AsyncFx();
  ~AsyncFx();

  // Functions
  void create_dense_array();
  void create_sparse_array();
  void write_dense_async();
  void write_sparse_async();
  void read_dense_async();
  void read_sparse_async();
  void remove_dense_array();
  void remove_sparse_array();
  void remove_array(const std::string& array_name);
  bool is_array(const std::string& array_name);
};

AsyncFx::AsyncFx() {
  ctx_ = nullptr;
  REQUIRE(tiledb_ctx_create(&ctx_, NULL) == TILEDB_OK);
}

AsyncFx::~AsyncFx() {
  CHECK(tiledb_ctx_free(&ctx_) == TILEDB_OK);
}

void AsyncFx::create_dense_array() {
  // Create dimensions
  uint64_t dim_domain[] = {1, 4, 1, 4};
  uint64_t tile_extents[] = {2, 2};
  tiledb_dimension_t* d1;
  int rc = tiledb_dimension_create(
      ctx_, &d1, "d1", TILEDB_UINT64, &dim_domain[0], &tile_extents[0]);
  CHECK(rc == TILEDB_OK);
  tiledb_dimension_t* d2;
  rc = tiledb_dimension_create(
      ctx_, &d2, "d2", TILEDB_UINT64, &dim_domain[2], &tile_extents[1]);
  CHECK(rc == TILEDB_OK);

  // Create domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_create(ctx_, &domain);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d2);
  CHECK(rc == TILEDB_OK);

  // Create attributes
  tiledb_attribute_t* a1;
  rc = tiledb_attribute_create(ctx_, &a1, "a1", TILEDB_INT32);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_compressor(ctx_, a1, TILEDB_BLOSC_LZ, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a1, 1);
  CHECK(rc == TILEDB_OK);
  tiledb_attribute_t* a2;
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_create(ctx_, &a2, "a2", TILEDB_CHAR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_compressor(ctx_, a2, TILEDB_GZIP, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a2, TILEDB_VAR_NUM);
  CHECK(rc == TILEDB_OK);
  tiledb_attribute_t* a3;
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_create(ctx_, &a3, "a3", TILEDB_FLOAT32);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_compressor(ctx_, a3, TILEDB_ZSTD, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a3, 2);
  CHECK(rc == TILEDB_OK);

  // Create array schema
  tiledb_array_schema_t* array_schema;
  rc = tiledb_array_schema_create(ctx_, &array_schema, TILEDB_DENSE);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a3);
  CHECK(rc == TILEDB_OK);

  // Check array schema
  rc = tiledb_array_schema_check(ctx_, array_schema);
  CHECK(rc == TILEDB_OK);

  // Create array
  rc = tiledb_array_create(ctx_, DENSE_ARRAY_NAME, array_schema);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_attribute_free(ctx_, &a1);
  tiledb_attribute_free(ctx_, &a2);
  tiledb_attribute_free(ctx_, &a3);
  tiledb_dimension_free(ctx_, &d1);
  tiledb_dimension_free(ctx_, &d2);
  tiledb_domain_free(ctx_, &domain);
  tiledb_array_schema_free(ctx_, &array_schema);
}

void AsyncFx::create_sparse_array() {
  // Create dimensions
  uint64_t dim_domain[] = {1, 4, 1, 4};
  uint64_t tile_extents[] = {2, 2};
  tiledb_dimension_t* d1;
  int rc = tiledb_dimension_create(
      ctx_, &d1, "d1", TILEDB_UINT64, &dim_domain[0], &tile_extents[0]);
  CHECK(rc == TILEDB_OK);
  tiledb_dimension_t* d2;
  rc = tiledb_dimension_create(
      ctx_, &d2, "d2", TILEDB_UINT64, &dim_domain[2], &tile_extents[1]);
  CHECK(rc == TILEDB_OK);

  // Create domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_create(ctx_, &domain);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d2);
  CHECK(rc == TILEDB_OK);

  // Create attributes
  tiledb_attribute_t* a1;
  rc = tiledb_attribute_create(ctx_, &a1, "a1", TILEDB_INT32);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_compressor(ctx_, a1, TILEDB_BLOSC_LZ, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a1, 1);
  CHECK(rc == TILEDB_OK);
  tiledb_attribute_t* a2;
  rc = tiledb_attribute_create(ctx_, &a2, "a2", TILEDB_CHAR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_compressor(ctx_, a2, TILEDB_GZIP, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a2, TILEDB_VAR_NUM);
  CHECK(rc == TILEDB_OK);
  tiledb_attribute_t* a3;
  rc = tiledb_attribute_create(ctx_, &a3, "a3", TILEDB_FLOAT32);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_compressor(ctx_, a3, TILEDB_ZSTD, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a3, 2);
  CHECK(rc == TILEDB_OK);

  // Create array schmea
  tiledb_array_schema_t* array_schema;
  rc = tiledb_array_schema_create(ctx_, &array_schema, TILEDB_SPARSE);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_capacity(ctx_, array_schema, 2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a3);
  CHECK(rc == TILEDB_OK);

  // Check array schema
  rc = tiledb_array_schema_check(ctx_, array_schema);
  CHECK(rc == TILEDB_OK);

  // Create array
  rc = tiledb_array_create(ctx_, SPARSE_ARRAY_NAME, array_schema);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_attribute_free(ctx_, &a1);
  tiledb_attribute_free(ctx_, &a2);
  tiledb_attribute_free(ctx_, &a3);
  tiledb_dimension_free(ctx_, &d1);
  tiledb_dimension_free(ctx_, &d2);
  tiledb_domain_free(ctx_, &domain);
  tiledb_array_schema_free(ctx_, &array_schema);
}

void callback(void* v) {
  *(int*)v = 1;
}

void AsyncFx::write_dense_async() {
  // Set attributes
  const char* attributes[] = {"a1", "a2", "a3"};

  // Prepare cell buffers
  // clang-format off
  int buffer_a1[] = {
      0,  1,  2,  3, 4,  5,  6,  7,
      8,  9,  10, 11, 12, 13, 14, 15
  };
  uint64_t buffer_a2[] = {
      0,  1,  3,  6, 10, 11, 13, 16,
      20, 21, 23, 26, 30, 31, 33, 36
  };
  char buffer_var_a2[] =
      "abbcccdddd"
      "effggghhhh"
      "ijjkkkllll"
      "mnnooopppp";
  float buffer_a3[] = {
      0.1f,  0.2f,  1.1f,  1.2f,  2.1f,  2.2f,  3.1f,  3.2f,
      4.1f,  4.2f,  5.1f,  5.2f,  6.1f,  6.2f,  7.1f,  7.2f,
      8.1f,  8.2f,  9.1f,  9.2f,  10.1f, 10.2f, 11.1f, 11.2f,
      12.1f, 12.2f, 13.1f, 13.2f, 14.1f, 14.2f, 15.1f, 15.2f,
  };
  void* buffers[] = { buffer_a1, buffer_a2, buffer_var_a2, buffer_a3 };
  uint64_t buffer_sizes[] =
  {
      sizeof(buffer_a1),
      sizeof(buffer_a2),
      sizeof(buffer_var_a2)-1,  // No need to store the last '\0' character
      sizeof(buffer_a3)
  };
  // clang-format on

  // Create query
  tiledb_query_t* query;
  int rc = tiledb_query_create(ctx_, &query, DENSE_ARRAY_NAME, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffers(
      ctx_, query, attributes, 3, buffers, buffer_sizes);
  CHECK(rc == TILEDB_OK);

  // Submit query asynchronously
  int v = 0;
  rc = tiledb_query_submit_async(ctx_, query, callback, &v);
  CHECK(rc == TILEDB_OK);

  // Wait for query to complete
  tiledb_query_status_t status;
  do {
    rc = tiledb_query_get_status(ctx_, query, &status);
    CHECK(rc == TILEDB_OK);
  } while (status != TILEDB_COMPLETED);

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Check correct execution of callback
  CHECK(v == 1);

  // Clean up
  tiledb_query_free(ctx_, &query);
}

void AsyncFx::write_sparse_async() {
  // Prepare cell buffers
  int buffer_a1[] = {0, 1, 2, 3, 4, 5, 6, 7};
  uint64_t buffer_a2[] = {0, 1, 3, 6, 10, 11, 13, 16};
  char buffer_var_a2[] = "abbcccddddeffggghhhh";
  float buffer_a3[] = {0.1f,
                       0.2f,
                       1.1f,
                       1.2f,
                       2.1f,
                       2.2f,
                       3.1f,
                       3.2f,
                       4.1f,
                       4.2f,
                       5.1f,
                       5.2f,
                       6.1f,
                       6.2f,
                       7.1f,
                       7.2f};
  uint64_t buffer_coords[] = {1, 1, 1, 2, 1, 4, 2, 3, 3, 1, 4, 2, 3, 3, 3, 4};
  void* buffers[] = {
      buffer_a1, buffer_a2, buffer_var_a2, buffer_a3, buffer_coords};
  uint64_t buffer_sizes[] = {
      sizeof(buffer_a1),
      sizeof(buffer_a2),
      sizeof(buffer_var_a2) - 1,  // No need to store the last '\0' character
      sizeof(buffer_a3),
      sizeof(buffer_coords)};

  // Create query
  tiledb_query_t* query;
  const char* attributes[] = {"a1", "a2", "a3", TILEDB_COORDS};
  int rc = tiledb_query_create(ctx_, &query, SPARSE_ARRAY_NAME, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffers(
      ctx_, query, attributes, 4, buffers, buffer_sizes);
  CHECK(rc == TILEDB_OK);

  // Submit query asynchronously
  int v = 0;
  rc = tiledb_query_submit_async(ctx_, query, callback, &v);
  CHECK(rc == TILEDB_OK);

  // Wait for query to complete
  tiledb_query_status_t status;
  do {
    rc = tiledb_query_get_status(ctx_, query, &status);
    CHECK(rc == TILEDB_OK);
  } while (status != TILEDB_COMPLETED);

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Check correct execution of callback
  CHECK(v == 1);

  // Clean up
  tiledb_query_free(ctx_, &query);
}

void AsyncFx::read_dense_async() {
  // Calculate maximum buffer sizes for each attribute
  const char* attributes[] = {"a1", "a2", "a3"};
  uint64_t buffer_sizes[4];
  uint64_t subarray[] = {1, 4, 1, 4};
  int rc = tiledb_array_compute_max_read_buffer_sizes(
      ctx_, DENSE_ARRAY_NAME, subarray, attributes, 3, &buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);

  // Prepare cell buffers
  int* buffer_a1 = (int*)malloc(buffer_sizes[0]);
  uint64_t* buffer_a2 = (uint64_t*)malloc(buffer_sizes[1]);
  char* buffer_var_a2 = (char*)malloc(buffer_sizes[2]);
  float* buffer_a3 = (float*)malloc(buffer_sizes[3]);
  void* buffers[] = {buffer_a1, buffer_a2, buffer_var_a2, buffer_a3};

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_create(ctx_, &query, DENSE_ARRAY_NAME, TILEDB_READ);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffers(
      ctx_, query, attributes, 3, buffers, buffer_sizes);
  CHECK(rc == TILEDB_OK);

  // Submit query with callback
  int v = 0;
  rc = tiledb_query_submit_async(ctx_, query, callback, &v);
  CHECK(rc == TILEDB_OK);

  // Wait for the query to complete
  tiledb_query_status_t status;
  do {
    tiledb_query_get_status(ctx_, query, &status);
  } while (status != TILEDB_COMPLETED);

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Check correct execution of callback
  CHECK(v == 1);

  // Correct buffers
  int c_buffer_a1[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
  uint64_t c_buffer_a2[] = {
      0, 1, 3, 6, 10, 11, 13, 16, 20, 21, 23, 26, 30, 31, 33, 36};
  char c_buffer_var_a2[] =
      "abbcccdddd"
      "effggghhhh"
      "ijjkkkllll"
      "mnnooopppp";
  float c_buffer_a3[] = {
      0.1f,  0.2f,  1.1f,  1.2f,  2.1f,  2.2f,  3.1f,  3.2f,
      4.1f,  4.2f,  5.1f,  5.2f,  6.1f,  6.2f,  7.1f,  7.2f,
      8.1f,  8.2f,  9.1f,  9.2f,  10.1f, 10.2f, 11.1f, 11.2f,
      12.1f, 12.2f, 13.1f, 13.2f, 14.1f, 14.2f, 15.1f, 15.2f,
  };

  // Check buffers
  CHECK(!memcmp(buffer_a1, c_buffer_a1, sizeof(c_buffer_a1)));
  CHECK(!memcmp(buffer_a2, c_buffer_a2, sizeof(c_buffer_a2)));
  CHECK(!memcmp(buffer_var_a2, c_buffer_var_a2, sizeof(c_buffer_var_a2) - 1));
  CHECK(!memcmp(buffer_a3, c_buffer_a3, sizeof(c_buffer_a3)));

  // Clean up
  tiledb_query_free(ctx_, &query);
}

void AsyncFx::read_sparse_async() {
  // Calculate maximum buffer sizes for each attribute
  const char* attributes[] = {"a1", "a2", "a3", TILEDB_COORDS};
  uint64_t buffer_sizes[5];
  uint64_t subarray[] = {1, 4, 1, 4};
  int rc = tiledb_array_compute_max_read_buffer_sizes(
      ctx_, SPARSE_ARRAY_NAME, subarray, attributes, 4, &buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);

  // Prepare cell buffers
  int* buffer_a1 = (int*)malloc(buffer_sizes[0]);
  uint64_t* buffer_a2 = (uint64_t*)malloc(buffer_sizes[1]);
  char* buffer_var_a2 = (char*)malloc(buffer_sizes[2]);
  float* buffer_a3 = (float*)malloc(buffer_sizes[3]);
  uint64_t* buffer_coords = (uint64_t*)malloc(buffer_sizes[4]);
  void* buffers[] = {
      buffer_a1, buffer_a2, buffer_var_a2, buffer_a3, buffer_coords};

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_create(ctx_, &query, SPARSE_ARRAY_NAME, TILEDB_READ);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffers(
      ctx_, query, attributes, 4, buffers, buffer_sizes);
  CHECK(rc == TILEDB_OK);

  // Submit query with callback
  int v = 0;
  rc = tiledb_query_submit_async(ctx_, query, callback, &v);
  CHECK(rc == TILEDB_OK);

  // Wait for the query to complete
  tiledb_query_status_t status;
  do {
    tiledb_query_get_status(ctx_, query, &status);
  } while (status != TILEDB_COMPLETED);

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Check correct execution of callback
  CHECK(v == 1);

  // Correct buffers
  int c_buffer_a1[] = {0, 1, 2, 3, 4, 5, 6, 7};
  uint64_t c_buffer_a2[] = {0, 1, 3, 6, 10, 11, 13, 16};
  char c_buffer_var_a2[] = "abbcccddddeffggghhhh";
  float c_buffer_a3[] = {0.1f,
                         0.2f,
                         1.1f,
                         1.2f,
                         2.1f,
                         2.2f,
                         3.1f,
                         3.2f,
                         4.1f,
                         4.2f,
                         5.1f,
                         5.2f,
                         6.1f,
                         6.2f,
                         7.1f,
                         7.2f};
  uint64_t c_buffer_coords[] = {1, 1, 1, 2, 1, 4, 2, 3, 3, 1, 4, 2, 3, 3, 3, 4};

  // Check buffers
  CHECK(!memcmp(buffer_a1, c_buffer_a1, sizeof(c_buffer_a1)));
  CHECK(!memcmp(buffer_a2, c_buffer_a2, sizeof(c_buffer_a2)));
  CHECK(!memcmp(buffer_var_a2, c_buffer_var_a2, sizeof(c_buffer_var_a2) - 1));
  CHECK(!memcmp(buffer_a3, c_buffer_a3, sizeof(c_buffer_a3)));
  CHECK(!memcmp(buffer_coords, c_buffer_coords, sizeof(c_buffer_coords)));

  // Clean up
  tiledb_query_free(ctx_, &query);
}

void AsyncFx::remove_array(const std::string& array_name) {
  if (!is_array(array_name))
    return;

  CHECK(tiledb_object_remove(ctx_, array_name.c_str()) == TILEDB_OK);
}

void AsyncFx::remove_dense_array() {
  remove_array(DENSE_ARRAY_NAME);
}

void AsyncFx::remove_sparse_array() {
  remove_array(SPARSE_ARRAY_NAME);
}

bool AsyncFx::is_array(const std::string& array_name) {
  tiledb_object_t type = TILEDB_INVALID;
  REQUIRE(tiledb_object_type(ctx_, array_name.c_str(), &type) == TILEDB_OK);
  return type == TILEDB_ARRAY;
}

TEST_CASE_METHOD(
    AsyncFx, "C API: Test dense async", "[capi], [async], [dense-async]") {
  remove_dense_array();
  create_dense_array();
  write_dense_async();
  read_dense_async();
  remove_dense_array();
}

TEST_CASE_METHOD(
    AsyncFx, "C API: Test sparse async", "[capi], [async], [sparse-async]") {
  remove_sparse_array();
  create_sparse_array();
  write_sparse_async();
  read_sparse_async();
  remove_sparse_array();
}
