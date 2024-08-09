/**
 * @file   unit-capi-dense_vector.cc
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
 * Tests of C API for dense vector operations.
 */

#include <catch2/catch_test_macros.hpp>
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

struct DenseVectorFx {
  std::string ATTR_NAME = "val";
  const tiledb_datatype_t ATTR_TYPE = TILEDB_INT64;
  const char* DIM0_NAME = "dim0";
  const tiledb_datatype_t DIM_TYPE = TILEDB_INT64;
  const std::string VECTOR = "vector";

  // TileDB context
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;

  // Vector of supported filsystems
  const std::vector<std::unique_ptr<SupportedFs>> fs_vec_;

  // Functions
  DenseVectorFx();
  ~DenseVectorFx();
  void create_dense_vector(
      const std::string& path,
      tiledb_layout_t cell_order,
      tiledb_layout_t tile_order);
  void check_read(const std::string& path, tiledb_layout_t layout);
  void check_update(const std::string& path);
  void create_temp_dir(const std::string& path);
  void remove_temp_dir(const std::string& path);
};

DenseVectorFx::DenseVectorFx()
    : fs_vec_(vfs_test_get_fs_vec()) {
  // Initialize vfs test
  REQUIRE(vfs_test_init(fs_vec_, &ctx_, &vfs_).ok());
}

DenseVectorFx::~DenseVectorFx() {
  // Close vfs test
  REQUIRE(vfs_test_close(fs_vec_, ctx_, vfs_).ok());
  tiledb_vfs_free(&vfs_);
  tiledb_ctx_free(&ctx_);
}

void DenseVectorFx::create_temp_dir(const std::string& path) {
  remove_temp_dir(path);
  REQUIRE(tiledb_vfs_create_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

void DenseVectorFx::remove_temp_dir(const std::string& path) {
  int is_dir = 0;
  REQUIRE(tiledb_vfs_is_dir(ctx_, vfs_, path.c_str(), &is_dir) == TILEDB_OK);
  if (is_dir)
    REQUIRE(tiledb_vfs_remove_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

void DenseVectorFx::create_dense_vector(
    const std::string& path,
    tiledb_layout_t cell_order,
    tiledb_layout_t tile_order) {
  int rc;
  int64_t dim_domain[] = {0, 9};
  int64_t tile_extent = 10;

  // Create domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx_, &domain);
  REQUIRE(rc == TILEDB_OK);
  tiledb_dimension_t* dim;
  rc = tiledb_dimension_alloc(
      ctx_, "d", TILEDB_INT64, dim_domain, &tile_extent, &dim);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, dim);
  REQUIRE(rc == TILEDB_OK);

  // Create attribute
  tiledb_attribute_t* attr;
  rc = tiledb_attribute_alloc(ctx_, ATTR_NAME.c_str(), ATTR_TYPE, &attr);
  REQUIRE(rc == TILEDB_OK);

  // Create array schema
  tiledb_array_schema_t* array_schema;
  rc = tiledb_array_schema_alloc(ctx_, TILEDB_DENSE, &array_schema);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(ctx_, array_schema, cell_order);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx_, array_schema, tile_order);
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

  const char* attributes[] = {ATTR_NAME.c_str()};

  // Open array
  tiledb_array_t* array;
  rc = tiledb_array_alloc(ctx_, path.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  int64_t buffer_val[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  // Write array
  void* write_buffers[] = {buffer_val};
  uint64_t write_buffer_sizes[] = {sizeof(buffer_val)};
  tiledb_query_t* write_query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &write_query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_,
      write_query,
      attributes[0],
      write_buffers[0],
      &write_buffer_sizes[0]);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, write_query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, write_query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx_, write_query);
  REQUIRE(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&write_query);
}

void DenseVectorFx::check_read(
    const std::string& path, tiledb_layout_t layout) {
  // Read subset of array val[0:2]
  uint64_t subarray[] = {0, 2};
  int64_t buffer[] = {0, 0, 0};
  void* read_buffers[] = {buffer};
  uint64_t read_buffer_sizes[] = {sizeof(buffer)};
  tiledb_query_t* read_query = nullptr;
  tiledb_subarray_t* sub;
  const char* attributes[] = {ATTR_NAME.c_str()};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, path.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &read_query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, read_query, attributes[0], read_buffers[0], &read_buffer_sizes[0]);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, read_query, layout);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, read_query, sub);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_free(&sub);
  rc = tiledb_query_submit(ctx_, read_query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx_, read_query);
  REQUIRE(rc == TILEDB_OK);

  CHECK(buffer[0] == 0);
  CHECK(buffer[1] == 1);
  CHECK(buffer[2] == 2);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&read_query);
}

void DenseVectorFx::check_update(const std::string& path) {
  uint64_t subarray[] = {0, 2};
  const char* attributes[] = {ATTR_NAME.c_str()};
  int64_t update_buffer[] = {9, 8, 7};
  void* update_buffers[] = {update_buffer};
  uint64_t update_buffer_sizes[] = {sizeof(update_buffer)};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, path.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Update
  tiledb_query_t* update_query;
  tiledb_subarray_t* sub;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &update_query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_,
      update_query,
      attributes[0],
      update_buffers[0],
      &update_buffer_sizes[0]);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, update_query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, update_query, sub);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_free(&sub);
  rc = tiledb_query_submit(ctx_, update_query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx_, update_query);
  REQUIRE(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_query_free(&update_query);

  // Open array
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Read
  int64_t buffer[] = {0, 0, 0};
  void* read_buffers[] = {buffer};
  uint64_t read_buffer_sizes[] = {sizeof(buffer)};
  tiledb_query_t* read_query = nullptr;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &read_query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, read_query, attributes[0], read_buffers[0], &read_buffer_sizes[0]);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, read_query, TILEDB_COL_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, read_query, sub);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_free(&sub);
  rc = tiledb_query_submit(ctx_, read_query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx_, read_query);
  REQUIRE(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&read_query);

  CHECK((buffer[0] == 9 && buffer[1] == 8 && buffer[2] == 7));
}

TEST_CASE_METHOD(
    DenseVectorFx, "C API: Test 1d dense vector", "[capi][dense-vector]") {
  std::string vector_name;

  // TODO: refactor for each supported FS.
  std::string temp_dir = fs_vec_[0]->temp_dir();
  create_temp_dir(temp_dir);
  vector_name = temp_dir + VECTOR;
  create_dense_vector(vector_name, TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
  check_read(vector_name, TILEDB_ROW_MAJOR);
  check_read(vector_name, TILEDB_COL_MAJOR);
  check_update(vector_name);
  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    DenseVectorFx,
    "C API: Test 1d dense vector with anonymous attribute",
    "[capi][dense-vector][anon-attr]") {
  ATTR_NAME = "";

  SupportedFsLocal local_fs;

  std::string vector_name =
      local_fs.file_prefix() + local_fs.temp_dir() + VECTOR;
  create_temp_dir(local_fs.file_prefix() + local_fs.temp_dir());
  create_dense_vector(vector_name, TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
  check_read(vector_name, TILEDB_ROW_MAJOR);
  check_read(vector_name, TILEDB_COL_MAJOR);
  check_update(vector_name);
  remove_temp_dir(local_fs.file_prefix() + local_fs.temp_dir());
}

TEST_CASE_METHOD(
    DenseVectorFx,
    "C API: Test 1d dense vector cell/tile layout",
    "[capi][dense-vector][dense-vector-layout]") {
  SupportedFsLocal local_fs;
  std::string vector_name =
      local_fs.file_prefix() + local_fs.temp_dir() + VECTOR;
  create_temp_dir(local_fs.file_prefix() + local_fs.temp_dir());
  create_dense_vector(vector_name, TILEDB_COL_MAJOR, TILEDB_COL_MAJOR);

  // Check layout
  tiledb_array_schema_t* array_schema;
  tiledb_layout_t cell_order, tile_order;
  int rc = tiledb_array_schema_load(ctx_, vector_name.c_str(), &array_schema);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_get_cell_order(ctx_, array_schema, &cell_order);
  CHECK(rc == TILEDB_OK);
  CHECK(cell_order == TILEDB_COL_MAJOR);
  rc = tiledb_array_schema_get_tile_order(ctx_, array_schema, &tile_order);
  CHECK(rc == TILEDB_OK);
  CHECK(tile_order == TILEDB_COL_MAJOR);
  tiledb_array_schema_free(&array_schema);

  remove_temp_dir(local_fs.file_prefix() + local_fs.temp_dir());
}

TEST_CASE_METHOD(
    DenseVectorFx,
    "C API: Test 1d dense vector, update",
    "[capi][dense-vector][dense-vector-update]") {
  SupportedFsLocal local_fs;
  std::string vector_name =
      local_fs.file_prefix() + local_fs.temp_dir() + VECTOR;
  create_temp_dir(local_fs.file_prefix() + local_fs.temp_dir());
  int rc;

  // --- Create array ----

  // Create domain
  uint64_t dim_domain[] = {0, 49};
  uint64_t tile_extent = 50;
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx_, &domain);
  REQUIRE(rc == TILEDB_OK);
  tiledb_dimension_t* dim;
  rc = tiledb_dimension_alloc(
      ctx_, DIM0_NAME, TILEDB_UINT64, dim_domain, &tile_extent, &dim);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, dim);
  REQUIRE(rc == TILEDB_OK);

  // Create attribute
  tiledb_attribute_t* attr;
  rc = tiledb_attribute_alloc(ctx_, ATTR_NAME.c_str(), TILEDB_FLOAT64, &attr);
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
  rc = tiledb_array_create(ctx_, vector_name.c_str(), array_schema);
  REQUIRE(rc == TILEDB_OK);
  tiledb_attribute_free(&attr);
  tiledb_dimension_free(&dim);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);

  // --- Zero write ----

  // Open array
  tiledb_array_t* array;
  rc = tiledb_array_alloc(ctx_, vector_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Write array
  uint64_t write_subarray_0[] = {0, 49};
  double write_buffer_0[50];
  uint64_t write_buffer_size_0 = sizeof(write_buffer_0);
  for (int i = 0; i < 20; ++i)
    write_buffer_0[i] = 0;
  tiledb_query_t* write_query_0;
  tiledb_subarray_t* sub;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &write_query_0);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_,
      write_query_0,
      ATTR_NAME.c_str(),
      write_buffer_0,
      &write_buffer_size_0);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, write_query_0, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, write_subarray_0);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, write_query_0, sub);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_free(&sub);
  rc = tiledb_query_submit(ctx_, write_query_0);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx_, write_query_0);
  REQUIRE(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&write_query_0);

  // --- First write ----

  // Open array
  rc = tiledb_array_alloc(ctx_, vector_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Write array
  uint64_t write_subarray_1[] = {5, 24};
  double write_buffer_1[20];
  uint64_t write_buffer_size_1 = sizeof(write_buffer_1);
  for (int i = 0; i < 20; ++i)
    write_buffer_1[i] = -1;
  tiledb_query_t* write_query_1;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &write_query_1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_,
      write_query_1,
      ATTR_NAME.c_str(),
      write_buffer_1,
      &write_buffer_size_1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, write_query_1, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, write_subarray_1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, write_query_1, sub);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_free(&sub);
  rc = tiledb_query_submit(ctx_, write_query_1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx_, write_query_1);
  REQUIRE(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&write_query_1);

  // --- Second write ----

  // Open array
  rc = tiledb_array_alloc(ctx_, vector_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Second array
  double write_buffer_2[50];
  uint64_t write_buffer_size_2 = sizeof(write_buffer_2);
  for (int i = 0; i < 50; ++i)
    write_buffer_2[i] = -1;
  tiledb_query_t* write_query_2;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &write_query_2);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_,
      write_query_2,
      ATTR_NAME.c_str(),
      write_buffer_2,
      &write_buffer_size_2);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, write_query_2, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, write_query_2);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx_, write_query_2);
  REQUIRE(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&write_query_2);

  // --- Third write ----

  // Open array
  rc = tiledb_array_alloc(ctx_, vector_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Write array
  uint64_t write_subarray_3[] = {5, 24};
  double write_buffer_3[20];
  uint64_t write_buffer_size_3 = sizeof(write_buffer_3);
  for (int i = 0; i < 20; ++i)
    write_buffer_3[i] = 3;
  tiledb_query_t* write_query_3;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &write_query_3);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_,
      write_query_3,
      ATTR_NAME.c_str(),
      write_buffer_3,
      &write_buffer_size_3);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, write_query_3, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, write_subarray_3);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, write_query_3, sub);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_free(&sub);
  rc = tiledb_query_submit(ctx_, write_query_3);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx_, write_query_3);
  REQUIRE(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&write_query_3);

  // --- Read ----

  // Read subset of array val[0:2]
  uint64_t subarray[] = {0, 49};
  double read_buffer[50];
  uint64_t read_buffer_size = sizeof(read_buffer);
  tiledb_query_t* read_query = nullptr;

  // Open array
  rc = tiledb_array_alloc(ctx_, vector_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &read_query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, read_query, ATTR_NAME.c_str(), read_buffer, &read_buffer_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, read_query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, read_query, sub);
  REQUIRE(rc == TILEDB_OK);
  tiledb_subarray_free(&sub);
  rc = tiledb_query_submit(ctx_, read_query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx_, read_query);
  REQUIRE(rc == TILEDB_OK);

  // Check correctness
  double c_buffer[50];
  for (int i = 0; i < 50; ++i)
    c_buffer[i] = -1;
  for (int i = 5; i < 25; ++i)
    c_buffer[i] = 3;
  CHECK(!memcmp(c_buffer, read_buffer, sizeof(c_buffer)));

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&read_query);

  remove_temp_dir(local_fs.file_prefix() + local_fs.temp_dir());
}
