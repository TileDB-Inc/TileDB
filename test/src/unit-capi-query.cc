/**
 * @file unit-capi-query.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * Tests for the C API tiledb_query_t spec.
 */

#include <tiledb/sm/c_api/tiledb_struct_def.h>
#include <cassert>
#include <cstring>
#include <iostream>
#include <sstream>
#include <thread>

#include "catch.hpp"
#include "test/src/helpers.h"
#include "test/src/vfs_helpers.h"
#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/misc/utils.h"

using namespace tiledb::test;

struct QueryFx {
  // TileDB context and vfs
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;

  // Vector of supported filsystems
  const std::vector<std::unique_ptr<SupportedFs>> fs_vec_;

  // Functions
  QueryFx();
  ~QueryFx();
  void remove_temp_dir(const std::string& path);
  void create_temp_dir(const std::string& path);
  void create_array(const std::string& path);
  void test_get_buffer_write(const std::string& path);
  void test_get_buffer_write_decoupled(const std::string& path);
  void test_get_buffer_read(const std::string& path);
  void test_get_buffer_read_decoupled(const std::string& path);
  static std::string random_name(const std::string& prefix);
};

QueryFx::QueryFx()
    : fs_vec_(vfs_test_get_fs_vec()) {
  // Initialize vfs test
  REQUIRE(vfs_test_init(fs_vec_, &ctx_, &vfs_).ok());
}

QueryFx::~QueryFx() {
  // Close vfs test
  REQUIRE(vfs_test_close(fs_vec_, ctx_, vfs_).ok());
  tiledb_vfs_free(&vfs_);
  tiledb_ctx_free(&ctx_);
}

std::string QueryFx::random_name(const std::string& prefix) {
  std::stringstream ss;
  ss << prefix << "-" << std::this_thread::get_id() << "-"
     << TILEDB_TIMESTAMP_NOW_MS;
  return ss.str();
}

void QueryFx::create_temp_dir(const std::string& path) {
  remove_temp_dir(path);
  REQUIRE(tiledb_vfs_create_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

void QueryFx::remove_temp_dir(const std::string& path) {
  int is_dir = 0;
  REQUIRE(tiledb_vfs_is_dir(ctx_, vfs_, path.c_str(), &is_dir) == TILEDB_OK);
  if (is_dir)
    REQUIRE(tiledb_vfs_remove_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

void QueryFx::create_array(const std::string& path) {
  // Create array schema
  tiledb_array_schema_t* array_schema;
  int rc = tiledb_array_schema_alloc(ctx_, TILEDB_DENSE, &array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Set schema members
  rc = tiledb_array_schema_set_capacity(ctx_, array_schema, 10000);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);

  // Create dimensions
  tiledb_dimension_t* d1;
  uint64_t dim_domain[] = {1, 10, 1, 10};
  uint64_t extents[] = {5, 5};
  rc = tiledb_dimension_alloc(
      ctx_, "dim_1", TILEDB_INT64, &dim_domain[0], &extents[0], &d1);
  REQUIRE(rc == TILEDB_OK);
  tiledb_dimension_t* d2;
  rc = tiledb_dimension_alloc(
      ctx_, "dim_2", TILEDB_INT64, &dim_domain[2], &extents[1], &d2);
  REQUIRE(rc == TILEDB_OK);

  // Set domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx_, &domain);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d1);
  REQUIRE(rc == TILEDB_OK);
  tiledb_datatype_t domain_type;
  rc = tiledb_domain_get_type(ctx_, domain, &domain_type);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(domain_type == TILEDB_INT64);
  rc = tiledb_domain_add_dimension(ctx_, domain, d2);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  REQUIRE(rc == TILEDB_OK);

  // Add attributes
  tiledb_attribute_t* a1;
  rc = tiledb_attribute_alloc(ctx_, "", TILEDB_INT32, &a1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a1);
  REQUIRE(rc == TILEDB_OK);
  tiledb_attribute_t* a2;
  rc = tiledb_attribute_alloc(ctx_, "a2", TILEDB_INT32, &a2);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a2, TILEDB_VAR_NUM);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a2);
  REQUIRE(rc == TILEDB_OK);

  // Create array
  rc = tiledb_array_create(ctx_, path.c_str(), array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  tiledb_array_schema_free(&array_schema);
  tiledb_attribute_free(&a1);
  tiledb_attribute_free(&a2);
  tiledb_dimension_free(&d1);
  tiledb_dimension_free(&d2);
  tiledb_domain_free(&domain);
}

void QueryFx::test_get_buffer_write(const std::string& path) {
  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, path.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Prepare subarray and buffers
  uint64_t subarray[] = {1, 2, 1, 2};
  int a1[] = {1, 2, 3, 4};
  uint64_t a1_size = sizeof(a1);
  uint64_t a2_off[] = {0, 4, 8, 12};
  uint64_t a2_off_size = sizeof(a2_off);
  int a2_val[] = {1, 2, 3, 4};
  uint64_t a2_val_size = sizeof(a2_val);

  // Prepare query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  // Get unset buffers
  void* a1_got;
  uint64_t* a1_got_size;
  uint64_t* a2_off_got;
  uint64_t* a2_off_got_size;
  void* a2_val_got;
  uint64_t* a2_val_got_size;
  rc = tiledb_query_get_data_buffer(ctx_, query, "", &a1_got, &a1_got_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_get_data_buffer(
      ctx_, query, "a2", &a2_val_got, &a2_val_got_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_get_offsets_buffer(
      ctx_, query, "a2", &a2_off_got, &a2_off_got_size);
  CHECK(rc == TILEDB_OK);
  CHECK(a1_got == nullptr);
  CHECK(a1_got_size == nullptr);
  CHECK(a2_off_got == nullptr);
  CHECK(a2_off_got_size == nullptr);
  CHECK(a2_val_got == nullptr);
  CHECK(a2_val_got_size == nullptr);

  // Set buffers
  rc = tiledb_query_set_data_buffer(ctx_, query, "", a1, &a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a2", a2_val, &a2_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(ctx_, query, "a2", a2_off, &a2_off_size);
  CHECK(rc == TILEDB_OK);

  // Get invalid var-sized buffer
  rc = tiledb_query_get_data_buffer(
      ctx_, query, "a1", &a2_val_got, &a2_val_got_size);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_query_get_offsets_buffer(
      ctx_, query, "a1", &a2_off_got, &a2_off_got_size);
  CHECK(rc == TILEDB_ERR);

  // Test getting the coords buffer
  rc = tiledb_query_get_data_buffer(
      ctx_, query, "dim_1", &a2_val_got, &a2_val_got_size);
  CHECK(rc == TILEDB_OK);
  CHECK(a1_got == nullptr);
  CHECK(a1_got_size == nullptr);
  rc = tiledb_query_get_offsets_buffer(
      ctx_, query, "dim_1", &a2_off_got, &a2_off_got_size);
  CHECK(rc == TILEDB_ERR);

  // Test invalid attribute
  rc = tiledb_query_get_data_buffer(ctx_, query, "foo", &a1_got, &a1_got_size);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_query_get_data_buffer(
      ctx_, query, "foo-var", &a2_val_got, &a2_val_got_size);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_query_get_offsets_buffer(
      ctx_, query, "foo-var", &a2_off_got, &a2_off_got_size);
  CHECK(rc == TILEDB_ERR);

  // Test correctly got buffers
  rc = tiledb_query_get_data_buffer(ctx_, query, "", &a1_got, &a1_got_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_get_data_buffer(
      ctx_, query, "a2", &a2_val_got, &a2_val_got_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_get_offsets_buffer(
      ctx_, query, "a2", &a2_off_got, &a2_off_got_size);
  CHECK(rc == TILEDB_OK);
  CHECK(a1_got == a1);
  CHECK(a1_got_size == &a1_size);
  CHECK(a2_off_got == a2_off);
  CHECK(a2_off_got_size == &a2_off_size);
  CHECK(a2_val_got == a2_val);
  CHECK(a2_val_got_size == &a2_val_size);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void QueryFx::test_get_buffer_write_decoupled(const std::string& path) {
  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, path.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Prepare subarray and buffers
  uint64_t subarray[] = {1, 2, 1, 2};
  int a1[] = {1, 2, 3, 4};
  uint64_t a1_size = sizeof(a1);
  uint64_t a2_off[] = {0, 4, 8, 12};
  uint64_t a2_off_size = sizeof(a2_off);
  int a2_val[] = {1, 2, 3, 4};
  uint64_t a2_val_size = sizeof(a2_val);

  // Prepare query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  // Get unset buffers
  void* a1_got;
  uint64_t* a1_got_size;
  uint64_t* a2_off_got;
  uint64_t* a2_off_got_size;
  void* a2_val_got;
  uint64_t* a2_val_got_size;
  rc = tiledb_query_get_data_buffer(ctx_, query, "", &a1_got, &a1_got_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_get_data_buffer(
      ctx_, query, "a2", &a2_val_got, &a2_val_got_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_get_offsets_buffer(
      ctx_, query, "a2", &a2_off_got, &a2_off_got_size);
  CHECK(rc == TILEDB_OK);
  CHECK(a1_got == nullptr);
  CHECK(a1_got_size == nullptr);
  CHECK(a2_off_got == nullptr);
  CHECK(a2_off_got_size == nullptr);
  CHECK(a2_val_got == nullptr);
  CHECK(a2_val_got_size == nullptr);

  // Set buffers
  rc = tiledb_query_set_data_buffer(ctx_, query, "", a1, &a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a2", a2_val, &a2_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(ctx_, query, "a2", a2_off, &a2_off_size);
  CHECK(rc == TILEDB_OK);

  // Test getting the coords buffer
  rc =
      tiledb_query_get_data_buffer(ctx_, query, "dim_1", &a1_got, &a1_got_size);
  CHECK(rc == TILEDB_OK);
  CHECK(a1_got == nullptr);
  CHECK(a1_got_size == nullptr);
  rc = tiledb_query_get_offsets_buffer(
      ctx_, query, "dim_1", &a2_off_got, &a2_off_got_size);
  CHECK(rc == TILEDB_ERR);

  // Test invalid attribute
  rc = tiledb_query_get_data_buffer(ctx_, query, "foo", &a1_got, &a1_got_size);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_query_get_data_buffer(
      ctx_, query, "foo-var", &a2_val_got, &a2_val_got_size);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_query_get_offsets_buffer(
      ctx_, query, "foo-var", &a2_off_got, &a2_off_got_size);
  CHECK(rc == TILEDB_ERR);

  // Test correctly got buffers
  rc = tiledb_query_get_data_buffer(ctx_, query, "", &a1_got, &a1_got_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_get_data_buffer(
      ctx_, query, "a2", &a2_val_got, &a2_val_got_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_get_offsets_buffer(
      ctx_, query, "a2", &a2_off_got, &a2_off_got_size);
  CHECK(rc == TILEDB_OK);
  CHECK(a1_got == a1);
  CHECK(a1_got_size == &a1_size);
  CHECK(a2_off_got == a2_off);
  CHECK(a2_off_got_size == &a2_off_size);
  CHECK(a2_val_got == a2_val);
  CHECK(a2_val_got_size == &a2_val_size);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void QueryFx::test_get_buffer_read(const std::string& path) {
  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, path.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Prepare subarray and buffers
  uint64_t subarray[] = {1, 2, 1, 2};
  int a1[4];
  uint64_t a1_size = sizeof(a1);
  uint64_t a2_off[4];
  uint64_t a2_off_size = sizeof(a2_off);
  int a2_val[4];
  uint64_t a2_val_size = sizeof(a2_val);

  // Prepare query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  // Get unset buffers
  void* a1_got;
  uint64_t* a1_got_size;
  uint64_t* a2_off_got;
  uint64_t* a2_off_got_size;
  void* a2_val_got;
  uint64_t* a2_val_got_size;
  rc = tiledb_query_get_data_buffer(ctx_, query, "", &a1_got, &a1_got_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_get_data_buffer(
      ctx_, query, "a2", &a2_val_got, &a2_val_got_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_get_offsets_buffer(
      ctx_, query, "a2", &a2_off_got, &a2_off_got_size);
  CHECK(rc == TILEDB_OK);
  CHECK(a1_got == nullptr);
  CHECK(a1_got_size == nullptr);
  CHECK(a2_off_got == nullptr);
  CHECK(a2_off_got_size == nullptr);
  CHECK(a2_val_got == nullptr);
  CHECK(a2_val_got_size == nullptr);

  // Set buffers
  rc = tiledb_query_set_data_buffer(ctx_, query, "", a1, &a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a2", a2_val, &a2_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(ctx_, query, "a2", a2_off, &a2_off_size);
  CHECK(rc == TILEDB_OK);

  // Get invalid fixed-sized / var-sized buffer
  rc = tiledb_query_get_data_buffer(ctx_, query, "a2", &a1_got, &a1_got_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_get_data_buffer(
      ctx_, query, "a1", &a2_val_got, &a2_val_got_size);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_query_get_offsets_buffer(
      ctx_, query, "a1", &a2_off_got, &a2_off_got_size);
  CHECK(rc == TILEDB_ERR);

  // Test getting the coords buffer
  rc =
      tiledb_query_get_data_buffer(ctx_, query, "dim_1", &a1_got, &a1_got_size);
  CHECK(rc == TILEDB_OK);
  CHECK(a1_got == nullptr);
  CHECK(a1_got_size == nullptr);
  rc = tiledb_query_get_data_buffer(
      ctx_, query, "dim_1", &a2_val_got, &a2_val_got_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_get_offsets_buffer(
      ctx_, query, "dim_1", &a2_off_got, &a2_off_got_size);
  CHECK(rc == TILEDB_ERR);

  // Test invalid attribute
  rc = tiledb_query_get_data_buffer(ctx_, query, "foo", &a1_got, &a1_got_size);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_query_get_data_buffer(
      ctx_, query, "foo-var", &a2_val_got, &a2_val_got_size);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_query_get_offsets_buffer(
      ctx_, query, "foo-var", &a2_off_got, &a2_off_got_size);
  CHECK(rc == TILEDB_ERR);

  // Test correctly got buffers
  rc = tiledb_query_get_data_buffer(ctx_, query, "", &a1_got, &a1_got_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_get_data_buffer(
      ctx_, query, "a2", &a2_val_got, &a2_val_got_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_get_offsets_buffer(
      ctx_, query, "a2", &a2_off_got, &a2_off_got_size);
  CHECK(rc == TILEDB_OK);
  CHECK(a1_got == a1);
  CHECK(a1_got_size == &a1_size);
  CHECK(a2_off_got == a2_off);
  CHECK(a2_off_got_size == &a2_off_size);
  CHECK(a2_val_got == a2_val);
  CHECK(a2_val_got_size == &a2_val_size);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void QueryFx::test_get_buffer_read_decoupled(const std::string& path) {
  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, path.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Prepare subarray and buffers
  uint64_t subarray[] = {1, 2, 1, 2};
  int a1[4];
  uint64_t a1_size = sizeof(a1);
  uint64_t a2_off[4];
  uint64_t a2_off_size = sizeof(a2_off);
  int a2_val[4];
  uint64_t a2_val_size = sizeof(a2_val);

  // Prepare query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  // Get unset buffers
  void* a1_got;
  uint64_t* a1_got_size;
  uint64_t* a2_off_got;
  uint64_t* a2_off_got_size;
  void* a2_val_got;
  uint64_t* a2_val_got_size;
  rc = tiledb_query_get_data_buffer(ctx_, query, "", &a1_got, &a1_got_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_get_data_buffer(
      ctx_, query, "a2", &a2_val_got, &a2_val_got_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_get_offsets_buffer(
      ctx_, query, "a2", &a2_off_got, &a2_off_got_size);
  CHECK(rc == TILEDB_OK);
  CHECK(a1_got == nullptr);
  CHECK(a1_got_size == nullptr);
  CHECK(a2_off_got == nullptr);
  CHECK(a2_off_got_size == nullptr);
  CHECK(a2_val_got == nullptr);
  CHECK(a2_val_got_size == nullptr);

  // Set buffers
  rc = tiledb_query_set_data_buffer(ctx_, query, "", a1, &a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a2", a2_val, &a2_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(ctx_, query, "a2", a2_off, &a2_off_size);
  CHECK(rc == TILEDB_OK);

  rc = tiledb_query_get_data_buffer(
      ctx_, query, "a1", &a2_val_got, &a2_val_got_size);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_query_get_offsets_buffer(
      ctx_, query, "a1", &a2_off_got, &a2_off_got_size);
  CHECK(rc == TILEDB_ERR);

  // Test getting the coords buffer
  rc =
      tiledb_query_get_data_buffer(ctx_, query, "dim_1", &a1_got, &a1_got_size);
  CHECK(rc == TILEDB_OK);
  CHECK(a1_got == nullptr);
  CHECK(a1_got_size == nullptr);

  // Test invalid attribute
  rc = tiledb_query_get_data_buffer(ctx_, query, "foo", &a1_got, &a1_got_size);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_query_get_data_buffer(
      ctx_, query, "foo-var", &a2_val_got, &a2_val_got_size);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_query_get_offsets_buffer(
      ctx_, query, "foo-var", &a2_off_got, &a2_off_got_size);
  CHECK(rc == TILEDB_ERR);

  // Test correctly got buffers
  rc = tiledb_query_get_data_buffer(ctx_, query, "", &a1_got, &a1_got_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_get_data_buffer(
      ctx_, query, "a2", &a2_val_got, &a2_val_got_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_get_offsets_buffer(
      ctx_, query, "a2", &a2_off_got, &a2_off_got_size);
  CHECK(rc == TILEDB_OK);
  CHECK(a1_got == a1);
  CHECK(a1_got_size == &a1_size);
  CHECK(a2_off_got == a2_off);
  CHECK(a2_off_got_size == &a2_off_size);
  CHECK(a2_val_got == a2_val);
  CHECK(a2_val_got_size == &a2_val_size);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

TEST_CASE_METHOD(
    QueryFx,
    "C API: Test query get buffer",
    "[capi][query][query-get-buffer]") {
  // TODO: refactor for each supported FS.
  SupportedFsLocal local_fs;
  std::string temp_dir = local_fs.file_prefix() + local_fs.temp_dir();

  std::string array_name = temp_dir + "query_get_buffer";
  create_temp_dir(temp_dir);
  create_array(array_name);
  test_get_buffer_write(array_name);
  test_get_buffer_write_decoupled(array_name);
  test_get_buffer_read(array_name);
  test_get_buffer_read_decoupled(array_name);
  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    QueryFx,
    "C API: Test query get layout",
    "[capi][query][query-get-layout]") {
  SupportedFsLocal local_fs;
  std::string temp_dir = local_fs.file_prefix() + local_fs.temp_dir();
  std::string array_name = temp_dir + "query_get_layout";
  create_temp_dir(temp_dir);
  create_array(array_name);

  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  tiledb_layout_t layout;
  rc = tiledb_query_get_layout(ctx_, query, &layout);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(layout == TILEDB_ROW_MAJOR);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_COL_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_get_layout(ctx_, query, &layout);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(layout == TILEDB_COL_MAJOR);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_get_layout(ctx_, query, &layout);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(layout == TILEDB_GLOBAL_ORDER);

  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);

  tiledb_query_free(&query);
  tiledb_array_free(&array);
  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    QueryFx, "C API: Test query get array", "[capi][query][query-get-array]") {
  SupportedFsLocal local_fs;
  std::string temp_dir = local_fs.file_prefix() + local_fs.temp_dir();
  std::string array_name = temp_dir + "query_get_array";
  create_temp_dir(temp_dir);
  create_array(array_name);

  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);

  REQUIRE(rc == TILEDB_OK);
  tiledb_array_t* rarray;
  rc = tiledb_query_get_array(ctx_, query, &rarray);
  REQUIRE(rc == TILEDB_OK);
  CHECK(rarray->array_ != array->array_);

  tiledb_array_schema_t* rschema;
  rc = tiledb_array_get_schema(ctx_, rarray, &rschema);
  REQUIRE(rc == TILEDB_OK);

  // Get schema members
  uint64_t rcapacity;
  rc = tiledb_array_schema_get_capacity(ctx_, rschema, &rcapacity);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(rcapacity == 10000);

  tiledb_layout_t layout;
  rc = tiledb_array_schema_get_cell_order(ctx_, rschema, &layout);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(layout == TILEDB_ROW_MAJOR);

  rc = tiledb_array_schema_get_tile_order(ctx_, rschema, &layout);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(layout == TILEDB_ROW_MAJOR);

  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);

  tiledb_array_schema_free(&rschema);
  tiledb_query_free(&query);
  tiledb_array_free(&array);
  tiledb_array_free(&rarray);
  remove_temp_dir(temp_dir);
}
