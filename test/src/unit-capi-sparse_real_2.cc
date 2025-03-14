/**
 * @file   unit-capi-sparse_real_2.cc
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
 * Tests of C API for sparse arrays with real domains.
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

struct SparseRealFx2 {
  // TileDB context
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;

  // Vector of supported filsystems
  const std::vector<std::unique_ptr<SupportedFs>>& fs_vec_;

  // Functions
  SparseRealFx2();
  ~SparseRealFx2();
  void create_temp_dir(const std::string& path);
  void remove_temp_dir(const std::string& path);
  void create_sparse_array(const std::string& path);
  void write_sparse_array(const std::string& path);
  void write_sparse_array_next_partition_bug(const std::string& path);
  void read_sparse_array(const std::string& path);
  void read_sparse_array_next_partition_bug(const std::string& path);
};

SparseRealFx2::SparseRealFx2()
    : fs_vec_(vfs_test_get_fs_vec()) {
  // Initialize vfs test
  REQUIRE(vfs_test_init(fs_vec_, &ctx_, &vfs_).ok());
}

SparseRealFx2::~SparseRealFx2() {
  // Close vfs test
  REQUIRE(vfs_test_close(fs_vec_, ctx_, vfs_).ok());
  tiledb_vfs_free(&vfs_);
  tiledb_ctx_free(&ctx_);
}

void SparseRealFx2::create_temp_dir(const std::string& path) {
  remove_temp_dir(path);
  REQUIRE(tiledb_vfs_create_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

void SparseRealFx2::remove_temp_dir(const std::string& path) {
  int is_dir = 0;
  REQUIRE(tiledb_vfs_is_dir(ctx_, vfs_, path.c_str(), &is_dir) == TILEDB_OK);
  if (is_dir)
    REQUIRE(tiledb_vfs_remove_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

void SparseRealFx2::create_sparse_array(const std::string& path) {
  // Create dimensions
  float dim_domain[] = {-180.0f, 180.0f, -90.0f, 90.0f};
  float tile_extents[] = {10.1f, 10.1f};
  tiledb_dimension_t* d1;
  int rc = tiledb_dimension_alloc(
      ctx_, "d1", TILEDB_FLOAT32, &dim_domain[0], &tile_extents[0], &d1);
  CHECK(rc == TILEDB_OK);
  tiledb_dimension_t* d2;
  rc = tiledb_dimension_alloc(
      ctx_, "d2", TILEDB_FLOAT32, &dim_domain[2], &tile_extents[1], &d2);
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
  rc = tiledb_array_schema_alloc(ctx_, TILEDB_SPARSE, &array_schema);
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

void SparseRealFx2::write_sparse_array(const std::string& path) {
  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, path.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  int a[] = {1, 2, 3, 4, 5};
  uint64_t a_size = sizeof(a);
  float coords_dim1[] = {-23.5f, 43.56f, 66.2f, -160.1f, 1.0f};
  float coords_dim2[] = {-20.0f, 80.0f, -0.3f, 89.1f, 1.0f};
  uint64_t coords_size = sizeof(coords_dim1);
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", a, &a_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d1", coords_dim1, &coords_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d2", coords_dim2, &coords_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void SparseRealFx2::write_sparse_array_next_partition_bug(
    const std::string& path) {
  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, path.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  int a[] = {1, 2};
  uint64_t a_size = sizeof(a);
  float coords_dim1[] = {-180.0f, -180.0f};
  float coords_dim2[] = {1.0f, 2.0f};
  uint64_t coords_size = sizeof(coords_dim1);
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", a, &a_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d1", coords_dim1, &coords_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d2", coords_dim2, &coords_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void SparseRealFx2::read_sparse_array(const std::string& path) {
  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, path.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  int a[16];
  uint64_t a_size = sizeof(a);
  float coords_dim1[16];
  float coords_dim2[16];
  uint64_t coords_size = sizeof(coords_dim1);
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);

  // Create some subarray
  float s0[] = {-180.0f, 180.0f};
  float s1[] = {-90.0f, 90.0f};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_t* subarray;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  rc = tiledb_query_set_data_buffer(ctx_, query, "a", a, &a_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d1", coords_dim1, &coords_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d2", coords_dim2, &coords_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  int a_c[] = {4, 1, 5, 2, 3};
  float coords_c_dim1[] = {-160.1f, -23.5f, 1.0f, 43.56f, 66.2f};
  float coords_c_dim2[] = {89.1f, -20.0f, 1.0f, 80.0f, -0.3f};
  CHECK(a_size == sizeof(a_c));
  CHECK(!memcmp(a, a_c, sizeof(a_c)));
  CHECK(coords_size == sizeof(coords_c_dim1));
  CHECK(!memcmp(coords_dim1, coords_c_dim1, sizeof(coords_c_dim1)));
  CHECK(!memcmp(coords_dim2, coords_c_dim2, sizeof(coords_c_dim2)));

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_subarray_free(&subarray);
}

void SparseRealFx2::read_sparse_array_next_partition_bug(
    const std::string& path) {
  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, path.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  int a[1];
  uint64_t a_size = sizeof(a);
  float coords_dim1[4];
  float coords_dim2[4];
  uint64_t coords_size = sizeof(coords_dim1);
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);

  // Set some subarray
  float s0[] = {-180.0f, 180.0f};
  float s1[] = {-90.0f, 90.0f};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_t* subarray;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  rc = tiledb_query_set_data_buffer(ctx_, query, "a", a, &a_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d1", coords_dim1, &coords_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d2", coords_dim2, &coords_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  CHECK(a_size == sizeof(int));
  CHECK(a[0] == 1);
  CHECK(coords_dim1[0] == -180.0f);
  CHECK(coords_dim2[0] == 1.0f);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_subarray_free(&subarray);
}

TEST_CASE_METHOD(
    SparseRealFx2,
    "C API: Test 2d sparse array with real domain 2",
    "[capi][sparse-real-2]") {
  SupportedFsLocal local_fs;
  std::string vector_name =
      local_fs.file_prefix() + local_fs.temp_dir() + "sparse_real";
  create_temp_dir(local_fs.file_prefix() + local_fs.temp_dir());

  create_sparse_array(vector_name);
  write_sparse_array(vector_name);
  read_sparse_array(vector_name);

  remove_temp_dir(local_fs.file_prefix() + local_fs.temp_dir());
}

TEST_CASE_METHOD(
    SparseRealFx2,
    "C API: Test 2d sparse array with real domain, next subarray partition bug "
    "2",
    "[capi][sparse-real-2][sparse-real-next-partition-bug-2]") {
  SupportedFsLocal local_fs;
  std::string array_name = local_fs.file_prefix() + local_fs.temp_dir() +
                           "sparse_real_next_partition_bug";
  create_temp_dir(local_fs.file_prefix() + local_fs.temp_dir());

  create_sparse_array(array_name);
  write_sparse_array_next_partition_bug(array_name);
  read_sparse_array_next_partition_bug(array_name);

  remove_temp_dir(local_fs.file_prefix() + local_fs.temp_dir());
}

TEST_CASE_METHOD(
    SparseRealFx2,
    "C API: Test 2d sparse array with real domain, NaN in subarray 2",
    "[capi][sparse-real-2][sparse-real-nan-subarray-2]") {
  SupportedFsLocal local_fs;
  std::string array_name =
      local_fs.file_prefix() + local_fs.temp_dir() + "sparse_real_nan_subarray";
  create_temp_dir(local_fs.file_prefix() + local_fs.temp_dir());

  create_sparse_array(array_name);
  write_sparse_array(array_name);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);

  // Set config for `sm.read_range_oob` = `error`
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_config_set(config, "sm.read_range_oob", "error", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_query_set_config(ctx_, query, config);
  REQUIRE(rc == TILEDB_OK);

  // Create some subarray
  float s0[] = {-180.0f, std::numeric_limits<float>::quiet_NaN()};
  float s1[] = {-90.0f, std::numeric_limits<float>::infinity()};
  tiledb_subarray_t* subarray;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_ERR);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_ERR);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  // Clean up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_config_free(&config);
  tiledb_query_free(&query);
  tiledb_subarray_free(&subarray);

  remove_temp_dir(local_fs.file_prefix() + local_fs.temp_dir());
}

TEST_CASE_METHOD(
    SparseRealFx2,
    "C API: Test 2d sparse array with real domain 2, unary range",
    "[capi][sparse-real-2]") {
  SupportedFsLocal local_fs;
  std::string array_name =
      local_fs.file_prefix() + local_fs.temp_dir() + "sparse_real_unary";
  remove_temp_dir(local_fs.file_prefix() + local_fs.temp_dir());
  create_temp_dir(local_fs.file_prefix() + local_fs.temp_dir());
  create_sparse_array(array_name);

  // Write twice (2 fragments)
  write_sparse_array(array_name);
  write_sparse_array(array_name);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  int a[1];
  uint64_t a_size = sizeof(a);
  float coords_dim1[1];
  float coords_dim2[1];
  uint64_t coords_size = sizeof(coords_dim1);
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);

  // Set some subarray
  float s0[] = {-23.5f, -23.5f};
  float s1[] = {-20.0f, -20.0f};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_t* subarray;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 1, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  rc = tiledb_query_set_data_buffer(ctx_, query, "a", a, &a_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d1", coords_dim1, &coords_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d2", coords_dim2, &coords_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  int a_c[] = {1};
  float coords_c_dim1[] = {-23.5f};
  float coords_c_dim2[] = {-20.0f};
  CHECK(a_size == sizeof(a_c));
  CHECK(!memcmp(a, a_c, sizeof(a_c)));
  CHECK(coords_size == sizeof(coords_c_dim1));
  CHECK(!memcmp(coords_dim1, coords_c_dim1, sizeof(coords_c_dim1)));
  CHECK(!memcmp(coords_dim2, coords_c_dim2, sizeof(coords_c_dim2)));

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_subarray_free(&subarray);

  remove_temp_dir(local_fs.file_prefix() + local_fs.temp_dir());
}
