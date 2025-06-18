/**
 * @file   unit-capi-dense_neg.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB Inc.
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
 * Tests of C API for dense arrays with negative domains.
 */

#include <test/support/tdb_catch.h>
#include "test/support/src/helpers.h"
#include "test/support/src/vfs_helpers.h"
#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif
#include "tiledb/sm/c_api/tiledb.h"

#include <iostream>
#include <sstream>
#include <thread>

using namespace tiledb::test;

struct DenseNegFx {
  // TileDB context
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;

  // Vector of supported filsystems
  const std::vector<std::unique_ptr<SupportedFs>> fs_vec_;

  // Functions
  DenseNegFx();
  ~DenseNegFx();
  void create_temp_dir(const std::string& path);
  void remove_temp_dir(const std::string& path);
  void create_dense_vector(const std::string& path);
  void create_dense_array(const std::string& path);
  void write_dense_vector(const std::string& path);
  void write_dense_array_global(const std::string& path);
  void write_dense_array_row(const std::string& path);
  void write_dense_array_col(const std::string& path);
  void read_dense_vector(const std::string& path);
  void read_dense_array_global(const std::string& path);
  void read_dense_array_row(const std::string& path);
  void read_dense_array_col(const std::string& path);
};

DenseNegFx::DenseNegFx()
    : fs_vec_(vfs_test_get_fs_vec()) {
  // Initialize vfs test
  REQUIRE(vfs_test_init(fs_vec_, &ctx_, &vfs_).ok());
}

DenseNegFx::~DenseNegFx() {
  // Close vfs test
  REQUIRE(vfs_test_close(fs_vec_, ctx_, vfs_).ok());
  tiledb_vfs_free(&vfs_);
  tiledb_ctx_free(&ctx_);
}

void DenseNegFx::create_temp_dir(const std::string& path) {
  remove_temp_dir(path);
  REQUIRE(tiledb_vfs_create_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

void DenseNegFx::remove_temp_dir(const std::string& path) {
  int is_dir = 0;
  REQUIRE(tiledb_vfs_is_dir(ctx_, vfs_, path.c_str(), &is_dir) == TILEDB_OK);
  if (is_dir)
    REQUIRE(tiledb_vfs_remove_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

void DenseNegFx::create_dense_vector(const std::string& path) {
  int rc;
  int64_t dim_domain[] = {-1, 2};
  int64_t tile_extent = 2;

  // Create domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx_, &domain);
  REQUIRE(rc == TILEDB_OK);
  tiledb_dimension_t* dim;
  rc = tiledb_dimension_alloc(
      ctx_, "d0", TILEDB_INT64, dim_domain, &tile_extent, &dim);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, dim);
  REQUIRE(rc == TILEDB_OK);

  // Create attribute
  tiledb_attribute_t* attr;
  rc = tiledb_attribute_alloc(ctx_, "a", TILEDB_INT32, &attr);
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
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, attr);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_array_schema_check(ctx_, array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Create array
  rc = tiledb_array_create(ctx_, path.c_str(), array_schema);
  REQUIRE(rc == TILEDB_OK);
  tiledb_attribute_free(&attr);
  tiledb_dimension_free(&dim);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

void DenseNegFx::create_dense_array(const std::string& path) {
  // Create dimensions
  int64_t dim_domain[] = {-2, 1, -2, 1};
  int64_t tile_extents[] = {2, 2};
  tiledb_dimension_t* d1;
  int rc = tiledb_dimension_alloc(
      ctx_, "d1", TILEDB_INT64, &dim_domain[0], &tile_extents[0], &d1);
  CHECK(rc == TILEDB_OK);
  tiledb_dimension_t* d2;
  rc = tiledb_dimension_alloc(
      ctx_, "d2", TILEDB_INT64, &dim_domain[2], &tile_extents[1], &d2);
  CHECK(rc == TILEDB_OK);

  // Create domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx_, &domain);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d2);
  CHECK(rc == TILEDB_OK);

  // Create attributes
  tiledb_attribute_t* a;
  rc = tiledb_attribute_alloc(ctx_, "a", TILEDB_INT32, &a);
  CHECK(rc == TILEDB_OK);
  tiledb_filter_t* filter;
  tiledb_filter_list_t* list;
  rc = tiledb_filter_alloc(ctx_, TILEDB_FILTER_LZ4, &filter);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_filter_list_alloc(ctx_, &list);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_filter_list_add_filter(ctx_, list, filter);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_filter_list(ctx_, a, list);
  CHECK(rc == TILEDB_OK);

  // Create array schema
  tiledb_array_schema_t* array_schema;
  rc = tiledb_array_schema_alloc(ctx_, TILEDB_DENSE, &array_schema);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a);
  CHECK(rc == TILEDB_OK);

  // Check array schema
  rc = tiledb_array_schema_check(ctx_, array_schema);
  CHECK(rc == TILEDB_OK);

  // Create array
  rc = tiledb_array_create(ctx_, path.c_str(), array_schema);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_filter_free(&filter);
  tiledb_filter_list_free(&list);
  tiledb_attribute_free(&a);
  tiledb_dimension_free(&d1);
  tiledb_dimension_free(&d2);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

void DenseNegFx::write_dense_vector(const std::string& path) {
  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, path.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  int a[] = {0, 1, 2, 3};
  uint64_t a_size = sizeof(a);
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", a, &a_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void DenseNegFx::write_dense_array_global(const std::string& path) {
  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, path.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  int64_t subarray[] = {-2, 1, -2, 1};
  int a[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
  uint64_t a_size = sizeof(a);
  tiledb_query_t* query;
  tiledb_subarray_t* sub;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", a, &a_size);
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
  rc = tiledb_query_submit_and_finalize(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void DenseNegFx::write_dense_array_row(const std::string& path) {
  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, path.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  int a[] = {100, 130, 140, 120, 150, 160};
  uint64_t a_size = sizeof(a);
  int64_t subarray[] = {0, 1, -1, 1};
  tiledb_query_t* query;
  tiledb_subarray_t* sub;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, sub);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_free(&sub);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", a, &a_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void DenseNegFx::write_dense_array_col(const std::string& path) {
  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, path.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  int a[] = {20, 40, 50, 70};
  uint64_t a_size = sizeof(a);
  int64_t subarray[] = {-2, -1, -1, 0};
  tiledb_query_t* query;
  tiledb_subarray_t* sub;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, sub);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_free(&sub);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", a, &a_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_COL_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void DenseNegFx::read_dense_vector(const std::string& path) {
  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, path.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  int a[4];
  int64_t subarray[] = {-1, 2};
  uint64_t a_size = sizeof(a);
  tiledb_query_t* query;
  tiledb_subarray_t* sub;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, sub);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_free(&sub);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", a, &a_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  int a_c[] = {0, 1, 2, 3};
  CHECK(a_size == sizeof(a_c));
  CHECK(!memcmp(a, a_c, sizeof(a_c)));

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void DenseNegFx::read_dense_array_global(const std::string& path) {
  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, path.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  int64_t subarray[] = {-2, 1, -2, 1};
  int a[16];
  uint64_t a_size = sizeof(a);
  tiledb_query_t* query;
  tiledb_subarray_t* sub;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", a, &a_size);
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

  int a_c[] = {1, 20, 3, 40, 50, 6, 70, 8, 9, 100, 11, 120, 130, 140, 150, 160};
  CHECK(a_size == sizeof(a_c));
  CHECK(!memcmp(a, a_c, sizeof(a_c)));

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void DenseNegFx::read_dense_array_row(const std::string& path) {
  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, path.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  int64_t subarray[] = {-2, 1, -2, 1};
  int a[16];
  uint64_t a_size = sizeof(a);
  tiledb_query_t* query;
  tiledb_subarray_t* sub;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", a, &a_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
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
  REQUIRE(rc == TILEDB_OK);

  int a_c[] = {1, 20, 50, 6, 3, 40, 70, 8, 9, 100, 130, 140, 11, 120, 150, 160};
  CHECK(a_size == sizeof(a_c));
  CHECK(!memcmp(a, a_c, sizeof(a_c)));

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void DenseNegFx::read_dense_array_col(const std::string& path) {
  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, path.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  int64_t subarray[] = {-2, 1, -2, 1};
  int a[16];
  uint64_t a_size = sizeof(a);
  tiledb_query_t* query;
  tiledb_subarray_t* sub;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", a, &a_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_COL_MAJOR);
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
  REQUIRE(rc == TILEDB_OK);

  int a_c[] = {1, 3, 9, 11, 20, 40, 100, 120, 50, 70, 130, 150, 6, 8, 140, 160};
  CHECK(a_size == sizeof(a_c));
  CHECK(!memcmp(a, a_c, sizeof(a_c)));

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

TEST_CASE_METHOD(
    DenseNegFx,
    "C API: Test 1d dense vector with negative domain",
    "[capi][dense-neg][dense-neg-vector][rest]") {
  std::string temp_dir = fs_vec_[0]->temp_dir();
  create_temp_dir(temp_dir);
  std::string vector_name =
      vfs_array_uri(fs_vec_[0], temp_dir + "dense_neg_vector", ctx_);

  create_dense_vector(vector_name);
  write_dense_vector(vector_name);
  read_dense_vector(vector_name);

  remove_temp_dir(temp_dir);
  tiledb::Array::delete_array(ctx_, vector_name);
}

TEST_CASE_METHOD(
    DenseNegFx,
    "C API: Test 2d dense array with negative domain",
    "[capi][dense-neg][dense-neg-array][rest]") {
  std::string temp_dir = fs_vec_[0]->temp_dir();
  create_temp_dir(temp_dir);
  std::string array_name =
      vfs_array_uri(fs_vec_[0], temp_dir + "dense_neg_array", ctx_);

  create_dense_array(array_name);
  write_dense_array_global(array_name);
  write_dense_array_row(array_name);
  write_dense_array_col(array_name);
  read_dense_array_global(array_name);
  read_dense_array_row(array_name);
  read_dense_array_col(array_name);

  remove_temp_dir(temp_dir);
  tiledb::Array::delete_array(ctx_, array_name);
}
