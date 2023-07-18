/**
 * @file   unit-capi-dense_array.cc
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
 * Tests of C API for dense array operations.
 */

#ifdef TILEDB_SERIALIZATION
#include <capnp/message.h>
#endif

#include <test/support/tdb_catch.h>
#include "test/support/src/helpers.h"
#include "test/support/src/serialization_wrappers.h"
#include "test/support/src/vfs_helpers.h"
#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif
#include "test/support/src/helpers.h"
#include "tiledb/api/c_api/buffer/buffer_api_internal.h"
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_serialization.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/misc/tdb_time.h"
#include "tiledb/sm/serialization/array.h"
#include "tiledb/sm/serialization/query.h"

#include <array>
#include <cassert>
#include <climits>
#include <cstring>
#include <ctime>
#include <iostream>
#include <map>
#include <sstream>
#include <thread>

using namespace tiledb::test;
using namespace tiledb::sm;

struct TestRange {
  TestRange(uint64_t i, uint64_t min, uint64_t max)
      : i_(i)
      , min_(min)
      , max_(max) {
  }

  uint64_t i_;
  uint64_t min_;
  uint64_t max_;
};

struct DenseArrayFx {
  // Constant parameters
  const char* ATTR_NAME = "a";
  const tiledb_datatype_t ATTR_TYPE = TILEDB_INT32;
  const char* DIM1_NAME = "x";
  const char* DIM2_NAME = "y";
  const tiledb_datatype_t DIM_TYPE = TILEDB_INT64;
  const int ITER_NUM = 10;

  tiledb_encryption_type_t encryption_type = TILEDB_NO_ENCRYPTION;
  const char* encryption_key = nullptr;

  // use separate subarray prepared 'outside' of a query
  bool use_external_subarray_ = false;

  // TileDB context and VFS
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;

  // Vector of supported filsystems
  const std::vector<std::unique_ptr<SupportedFs>> fs_vec_;

  // Serialization parameters
  bool serialize_ = false;
  bool refactored_query_v2_ = false;
  // Buffers to allocate on server side for serialized queries
  ServerQueryBuffers server_buffers_;

  // Functions
  DenseArrayFx();
  ~DenseArrayFx();
  void create_temp_dir(const std::string& path);
  void remove_temp_dir(const std::string& path);
  void check_sorted_reads(const std::string& path);
  void check_sorted_writes(const std::string& path);
  void check_invalid_cell_num_in_dense_writes(const std::string& path);
  void check_simultaneous_writes(const std::string& path);
  void check_cancel_and_retry_writes(const std::string& path);
  void check_return_coords(const std::string& path, bool split_coords);
  void check_non_empty_domain(const std::string& path);
  void create_dense_vector(const std::string& path);
  void create_dense_array(const std::string& array_name);
  void create_dense_array_1_attribute(const std::string& array_name);
  void create_dense_array_same_tile(const std::string& array_name);
  void create_large_dense_array_1_attribute(const std::string& array_name);
  void write_dense_vector_mixed(const std::string& array_name);
  void write_dense_array(const std::string& array_name);
  void write_dense_array_missing_attributes(const std::string& array_name);
  void write_partial_dense_array(const std::string& array_name);
  void write_large_dense_array(const std::string& array_name);
  void read_dense_vector_mixed(const std::string& array_name);
  void read_dense_array_with_coords_full_global(
      const std::string& array_name, bool split_coords);
  void read_dense_array_with_coords_full_row(
      const std::string& array_name, bool split_coords);
  void read_dense_array_with_coords_full_col(
      const std::string& array_name, bool split_coords);
  void read_dense_array_with_coords_subarray_global(
      const std::string& array_name, bool split_coords);
  void read_dense_array_with_coords_subarray_row(
      const std::string& array_name, bool split_coords);
  void read_dense_array_with_coords_subarray_col(
      const std::string& array_name, bool split_coords);
  void read_large_dense_array(
      const std::string& array_name,
      const tiledb_layout_t layout,
      std::vector<TestRange>& ranges,
      std::vector<uint64_t>& a1);
  static std::string random_name(const std::string& prefix);

  /**
   * Creates a 2D dense array.
   *
   * @param array_name The array name.
   * @param tile_extent_0 The tile extent along the first dimension.
   * @param tile_extent_1 The tile extent along the second dimension.
   * @param domain_0_lo The smallest value of the first dimension domain.
   * @param domain_0_hi The largest value of the first dimension domain.
   * @param domain_1_lo The smallest value of the second dimension domain.
   * @param domain_1_hi The largest value of the second dimension domain.
   * @param capacity The tile capacity.
   * @param cell_order The cell order.
   * @param tile_order The tile order.
   */
  void create_dense_array_2D(
      const std::string& array_name,
      const int64_t tile_extent_0,
      const int64_t tile_extent_1,
      const int64_t domain_0_lo,
      const int64_t domain_0_hi,
      const int64_t domain_1_lo,
      const int64_t domain_1_hi,
      const uint64_t capacity,
      const tiledb_layout_t cell_order,
      const tiledb_layout_t tile_order);

  /**
   * Generates a 2D buffer containing the cell values of a 2D array.
   * Each cell value equals (row index * total number of columns + col index).
   * @param domain_size_0 The domain size of the first dimension.
   * @param domain_size_1 The domain size of the second dimension.
   * @return The created 2D buffer. Note that the function creates the buffer
   *     with 'new'. Make sure to delete the returned buffer in the caller
   *     function.
   */
  int** generate_2D_buffer(
      const int64_t domain_size_0, const int64_t domain_size_1);

  /**
   * Reads a subarray oriented by the input boundaries and outputs the buffer
   * containing the attribute values of the corresponding cells.
   *
   * @param array_name The array name.
   * @param domain_0_lo The smallest value of the first dimension domain to
   *     be read.
   * @param domain_0_hi The largest value of the first dimension domain to
   *     be read.
   * @param domain_1_lo The smallest value of the second dimension domain to
   *     be read.
   * @param domain_1_hi The largest value of the second dimension domain to
   *     be read.
   * @param read_mode The read mode.
   * @return The buffer with the read values. Note that this function is
   *     creating the buffer with 'new'. Therefore, make sure to properly delete
   *     it in the caller. On error, it returns NULL.
   */
  int* read_dense_array_2D(
      const std::string& array_name,
      const int64_t domain_0_lo,
      const int64_t domain_0_hi,
      const int64_t domain_1_lo,
      const int64_t domain_1_hi,
      const tiledb_query_type_t query_type,
      const tiledb_layout_t query_layout);

  /**
   * Write to a 2D dense array tile by tile. The buffer is initialized
   * with row_id*domain_size_1+col_id values.
   *
   * @param array_name The array name.
   * @param domain_size_0 The domain size of the first dimension.
   * @param domain_size_1 The domain size of the second dimension.
   * @param tile_extent_0 The tile extent along the first dimension.
   * @param tile_extent_1 The tile extent along the second dimension.
   */
  void write_dense_array_by_tiles(
      const std::string& array_name,
      const int64_t domain_size_0,
      const int64_t domain_size_1,
      const int64_t tile_extent_0,
      const int64_t tile_extent_1);

  /**
   * Writes a 2D dense subarray.
   *
   * @param array_name The array name.
   * @param subarray The subarray to focus on, given as a vector of low, high
   *     values.
   * @param write_mode The write mode.
   * @param buffer The attribute buffer to be populated and written.
   * @param buffer_sizes The buffer sizes to be dispatched to the write command.
   */
  void write_dense_subarray_2D(
      const std::string& array_name,
      int64_t* subarray,
      tiledb_query_type_t query_type,
      tiledb_layout_t query_layout,
      int* buffer,
      uint64_t* buffer_sizes);

  /**
   * Writes a 2D dense subarray by cancelling and re-issuing the query several
   * times.
   */
  void write_dense_subarray_2D_with_cancel(
      const std::string& array_name,
      int64_t* subarray,
      tiledb_query_type_t query_type,
      tiledb_layout_t query_layout,
      int* buffer,
      uint64_t* buffer_sizes);
};

DenseArrayFx::DenseArrayFx()
    : fs_vec_(vfs_test_get_fs_vec()) {
  // Initialize vfs test
  REQUIRE(vfs_test_init(fs_vec_, &ctx_, &vfs_).ok());
  std::srand(0);
}

DenseArrayFx::~DenseArrayFx() {
  // Close vfs test
  REQUIRE(vfs_test_close(fs_vec_, ctx_, vfs_).ok());
  tiledb_vfs_free(&vfs_);
  CHECK(vfs_ == nullptr);
  tiledb_ctx_free(&ctx_);
  CHECK(ctx_ == nullptr);
}

void DenseArrayFx::create_temp_dir(const std::string& path) {
  remove_temp_dir(path);
  REQUIRE(tiledb_vfs_create_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

void DenseArrayFx::remove_temp_dir(const std::string& path) {
  int is_dir = 0;
  REQUIRE(tiledb_vfs_is_dir(ctx_, vfs_, path.c_str(), &is_dir) == TILEDB_OK);
  if (is_dir)
    REQUIRE(tiledb_vfs_remove_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

void DenseArrayFx::create_dense_vector(const std::string& path) {
  // Create dimensions
  uint64_t dim_domain[] = {1, 410};
  uint64_t tile_extents[] = {10};
  tiledb_dimension_t* d;
  int rc = tiledb_dimension_alloc(
      ctx_, "d", TILEDB_UINT64, &dim_domain[0], &tile_extents[0], &d);
  CHECK(rc == TILEDB_OK);

  // Create domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx_, &domain);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d);
  CHECK(rc == TILEDB_OK);

  // Create attribute
  tiledb_attribute_t* a;
  rc = tiledb_attribute_alloc(ctx_, "a", TILEDB_INT32, &a);
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
  if (encryption_type != TILEDB_NO_ENCRYPTION) {
    tiledb_ctx_free(&ctx_);
    tiledb_vfs_free(&vfs_);
    tiledb_config_t* cfg;
    tiledb_error_t* err = nullptr;
    rc = tiledb_config_alloc(&cfg, &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    std::string encryption_type_string =
        encryption_type_str((tiledb::sm::EncryptionType)encryption_type);
    rc = tiledb_config_set(
        cfg, "sm.encryption_type", encryption_type_string.c_str(), &err);
    REQUIRE(err == nullptr);
    rc = tiledb_config_set(cfg, "sm.encryption_key", encryption_key, &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    REQUIRE(vfs_test_init(fs_vec_, &ctx_, &vfs_, cfg).ok());
    tiledb_config_free(&cfg);
  }
  rc = tiledb_array_create(ctx_, path.c_str(), array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  tiledb_attribute_free(&a);
  tiledb_dimension_free(&d);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

void DenseArrayFx::create_dense_array_2D(
    const std::string& array_name,
    const int64_t tile_extent_0,
    const int64_t tile_extent_1,
    const int64_t domain_0_lo,
    const int64_t domain_0_hi,
    const int64_t domain_1_lo,
    const int64_t domain_1_hi,
    const uint64_t capacity,
    const tiledb_layout_t cell_order,
    const tiledb_layout_t tile_order) {
  // Create attribute
  tiledb_attribute_t* a;
  int rc = tiledb_attribute_alloc(ctx_, ATTR_NAME, ATTR_TYPE, &a);
  REQUIRE(rc == TILEDB_OK);

  // Create dimensions
  int64_t dim_domain[] = {domain_0_lo, domain_0_hi, domain_1_lo, domain_1_hi};
  tiledb_dimension_t* d1;
  rc = tiledb_dimension_alloc(
      ctx_, DIM1_NAME, TILEDB_INT64, &dim_domain[0], &tile_extent_0, &d1);
  REQUIRE(rc == TILEDB_OK);
  tiledb_dimension_t* d2;
  rc = tiledb_dimension_alloc(
      ctx_, DIM2_NAME, TILEDB_INT64, &dim_domain[2], &tile_extent_1, &d2);
  REQUIRE(rc == TILEDB_OK);

  // Create domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx_, &domain);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d2);
  REQUIRE(rc == TILEDB_OK);

  // Create array schema
  tiledb_array_schema_t* array_schema;
  rc = tiledb_array_schema_alloc(ctx_, TILEDB_DENSE, &array_schema);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_capacity(ctx_, array_schema, capacity);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(ctx_, array_schema, cell_order);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx_, array_schema, tile_order);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  REQUIRE(rc == TILEDB_OK);

  // Create the array
  if (encryption_type != TILEDB_NO_ENCRYPTION) {
    tiledb_ctx_free(&ctx_);
    tiledb_vfs_free(&vfs_);
    tiledb_config_t* cfg;
    tiledb_error_t* err = nullptr;
    rc = tiledb_config_alloc(&cfg, &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    std::string encryption_type_string =
        encryption_type_str((tiledb::sm::EncryptionType)encryption_type);
    rc = tiledb_config_set(
        cfg, "sm.encryption_type", encryption_type_string.c_str(), &err);
    REQUIRE(err == nullptr);
    rc = tiledb_config_set(cfg, "sm.encryption_key", encryption_key, &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    REQUIRE(vfs_test_init(fs_vec_, &ctx_, &vfs_, cfg).ok());
    tiledb_config_free(&cfg);
  }
  rc = tiledb_array_create(ctx_, array_name.c_str(), array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  tiledb_attribute_free(&a);
  tiledb_dimension_free(&d1);
  tiledb_dimension_free(&d2);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

int** DenseArrayFx::generate_2D_buffer(
    const int64_t domain_size_0, const int64_t domain_size_1) {
  // Create buffer
  auto buffer = new int*[domain_size_0];

  // Populate buffer
  for (int64_t i = 0; i < domain_size_0; ++i) {
    buffer[i] = new int[domain_size_1];
    for (int64_t j = 0; j < domain_size_1; ++j) {
      buffer[i][j] = (int)(i * domain_size_1 + j);
    }
  }

  // Return
  return buffer;
}

int* DenseArrayFx::read_dense_array_2D(
    const std::string& array_name,
    const int64_t domain_0_lo,
    const int64_t domain_0_hi,
    const int64_t domain_1_lo,
    const int64_t domain_1_hi,
    const tiledb_query_type_t query_type,
    const tiledb_layout_t query_layout) {
  // Error code
  int rc;

  // Initialize a subarray
  const int64_t subarray[] = {
      domain_0_lo, domain_0_hi, domain_1_lo, domain_1_hi};

  const char* attributes[] = {ATTR_NAME};

  // Prepare the buffers that will store the result
  int64_t domain_size_0 = domain_0_hi - domain_0_lo + 1;
  int64_t domain_size_1 = domain_1_hi - domain_1_lo + 1;
  int64_t cell_num = domain_size_0 * domain_size_1;
  auto buffer_a1 = new int[cell_num];
  REQUIRE(buffer_a1 != nullptr);
  void* buffers[] = {buffer_a1};
  uint64_t buffer_size_a1 = cell_num * sizeof(int);
  uint64_t buffer_sizes[] = {buffer_size_a1};

  // Open array
  tiledb_array_t* array;
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  if (encryption_type != TILEDB_NO_ENCRYPTION) {
    tiledb_config_t* cfg;
    tiledb_error_t* err = nullptr;
    REQUIRE(tiledb_config_alloc(&cfg, &err) == TILEDB_OK);
    REQUIRE(err == nullptr);
    std::string encryption_type_string =
        encryption_type_str((tiledb::sm::EncryptionType)encryption_type);
    REQUIRE(
        tiledb_config_set(
            cfg, "sm.encryption_type", encryption_type_string.c_str(), &err) ==
        TILEDB_OK);
    REQUIRE(err == nullptr);
    REQUIRE(
        tiledb_config_set(cfg, "sm.encryption_key", encryption_key, &err) ==
        TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_array_set_config(ctx_, array, cfg);
    REQUIRE(rc == TILEDB_OK);
    tiledb_config_free(&cfg);
  }
  rc = array_open_wrapper(
      ctx_, query_type, (serialize_ && refactored_query_v2_), &array);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, query_type, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, query_layout);
  REQUIRE(rc == TILEDB_OK);

  // Check that the query has no results yet
  int has_results;
  rc = tiledb_query_has_results(ctx_, query, &has_results);
  CHECK(rc == TILEDB_OK);
  CHECK(!has_results);

  // Read from array
  rc = submit_query_wrapper(
      ctx_,
      array_name,
      &query,
      server_buffers_,
      serialize_,
      refactored_query_v2_);
  REQUIRE(rc == TILEDB_OK);

  // Now the query must have results
  rc = tiledb_query_has_results(ctx_, query, &has_results);
  CHECK(rc == TILEDB_OK);
  CHECK(has_results);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_query_free(&query);
  CHECK(query == nullptr);

  // Success - return the created buffer
  return buffer_a1;
}

void DenseArrayFx::write_dense_vector_mixed(const std::string& array_name) {
  // Prepare cell buffers for 4 writes
  int a_1[300];
  for (int i = 0; i < 300; ++i)
    a_1[i] = i;
  uint64_t a_1_size = sizeof(a_1);
  int a_2[] = {1050, 1100, 1150};
  uint64_t a_2_size = sizeof(a_2);
  uint64_t coords_2[] = {50, 100, 150};
  uint64_t coords_2_size = sizeof(coords_2);
  int a_3[] = {1025, 1075, 1125};
  uint64_t a_3_size = sizeof(a_3);
  uint64_t coords_3[] = {25, 75, 125};
  uint64_t coords_3_size = sizeof(coords_3);
  int a_4[110];
  for (int i = 0; i < 110; ++i)
    a_4[i] = 300 + i;
  uint64_t a_4_size = sizeof(a_4);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  if (encryption_type != TILEDB_NO_ENCRYPTION) {
    tiledb_config_t* cfg;
    tiledb_error_t* err = nullptr;
    REQUIRE(tiledb_config_alloc(&cfg, &err) == TILEDB_OK);
    REQUIRE(err == nullptr);
    std::string encryption_type_string =
        encryption_type_str((tiledb::sm::EncryptionType)encryption_type);
    REQUIRE(
        tiledb_config_set(
            cfg, "sm.encryption_type", encryption_type_string.c_str(), &err) ==
        TILEDB_OK);
    REQUIRE(err == nullptr);
    REQUIRE(
        tiledb_config_set(cfg, "sm.encryption_key", encryption_key, &err) ==
        TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_array_set_config(ctx_, array, cfg);
    REQUIRE(rc == TILEDB_OK);
    tiledb_config_free(&cfg);
  }
  rc = array_open_wrapper(
      ctx_, TILEDB_WRITE, (serialize_ && refactored_query_v2_), &array);
  CHECK(rc == TILEDB_OK);

  // Submit query #1 - Dense
  tiledb_query_t* query_1;
  uint64_t subarray[] = {1, 300};
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query_1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query_1, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query_1, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query_1, "a", a_1, &a_1_size);
  CHECK(rc == TILEDB_OK);
  rc = submit_query_wrapper(
      ctx_,
      array_name,
      &query_1,
      server_buffers_,
      serialize_,
      refactored_query_v2_);
  CHECK(rc == TILEDB_OK);
  tiledb_query_free(&query_1);

  // Submit query #2
  tiledb_query_t* query_2;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query_2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query_2, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query_2, "a", a_2, &a_2_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query_2, "d", coords_2, &coords_2_size);
  CHECK(rc == TILEDB_OK);
  rc = submit_query_wrapper(
      ctx_,
      array_name,
      &query_2,
      server_buffers_,
      serialize_,
      refactored_query_v2_);
  CHECK(rc == TILEDB_OK);
  tiledb_query_free(&query_2);

  // Submit query #3
  tiledb_query_t* query_3;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query_3);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query_3, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query_3, "a", a_3, &a_3_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query_3, "d", coords_3, &coords_3_size);
  CHECK(rc == TILEDB_OK);
  rc = submit_query_wrapper(
      ctx_,
      array_name,
      &query_3,
      server_buffers_,
      serialize_,
      refactored_query_v2_);
  CHECK(rc == TILEDB_OK);
  tiledb_query_free(&query_3);

  // Submit query #4
  tiledb_query_t* query_4;
  subarray[0] = 301;
  subarray[1] = 410;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query_4);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query_4, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query_4, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query_4, "a", a_4, &a_4_size);
  CHECK(rc == TILEDB_OK);
  rc = submit_query_wrapper(
      ctx_,
      array_name,
      &query_4,
      server_buffers_,
      serialize_,
      refactored_query_v2_);
  CHECK(rc == TILEDB_OK);
  tiledb_query_free(&query_4);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
}

void DenseArrayFx::write_dense_array_by_tiles(
    const std::string& array_name,
    const int64_t domain_size_0,
    const int64_t domain_size_1,
    const int64_t tile_extent_0,
    const int64_t tile_extent_1) {
  // Other initializations
  auto buffer{generate_2D_buffer(domain_size_0, domain_size_1)};
  int64_t cell_num_in_tile{tile_extent_0 * tile_extent_1};
  auto buffer_a1{new int[cell_num_in_tile]};
  for (int64_t i = 0; i < cell_num_in_tile; ++i)
    buffer_a1[i] = 0;
  void* buffers[2]{buffer_a1, nullptr};

  uint64_t buffer_sizes[2]{0UL, 0UL};
  int64_t index{0L};
  uint64_t buffer_size{0L};

  const char* attributes[] = {ATTR_NAME};

  // Open array
  tiledb_array_t* array{nullptr};
  auto rc{tiledb_array_alloc(ctx_, array_name.c_str(), &array)};
  CHECK(rc == TILEDB_OK);
  if (encryption_type != TILEDB_NO_ENCRYPTION) {
    tiledb_config_t* cfg{nullptr};
    tiledb_error_t* err{nullptr};
    REQUIRE(tiledb_config_alloc(&cfg, &err) == TILEDB_OK);
    REQUIRE(err == nullptr);
    std::string encryption_type_string{
        encryption_type_str((tiledb::sm::EncryptionType)encryption_type)};
    REQUIRE(
        tiledb_config_set(
            cfg, "sm.encryption_type", encryption_type_string.c_str(), &err) ==
        TILEDB_OK);
    REQUIRE(err == nullptr);
    REQUIRE(
        tiledb_config_set(cfg, "sm.encryption_key", encryption_key, &err) ==
        TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_array_set_config(ctx_, array, cfg);
    REQUIRE(rc == TILEDB_OK);
    tiledb_config_free(&cfg);
  }
  rc = array_open_wrapper(
      ctx_, TILEDB_WRITE, (serialize_ && refactored_query_v2_), &array);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query{nullptr};
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  REQUIRE(rc == TILEDB_OK);

  // Populate and write tile by tile
  for (int64_t i = 0; i < domain_size_0; i += tile_extent_0) {
    for (int64_t j = 0; j < domain_size_1; j += tile_extent_1) {
      int64_t tile_rows = ((i + tile_extent_0) < domain_size_0) ?
                              tile_extent_0 :
                              (domain_size_0 - i);
      int64_t tile_cols = ((j + tile_extent_1) < domain_size_1) ?
                              tile_extent_1 :
                              (domain_size_1 - j);
      int64_t k{0L}, l{0L};
      for (k = 0; k < tile_rows; ++k) {
        for (l = 0; l < tile_cols; ++l) {
          index = uint64_t(k * tile_cols + l);
          buffer_a1[index] = buffer[uint64_t(i + k)][uint64_t(j + l)];
        }
      }
      buffer_size = k * l * sizeof(int);
      buffer_sizes[0] = buffer_size;

      auto rc{tiledb_query_submit(ctx_, query)};

      const char* msg = "unset";
      tiledb_error_t* err{nullptr};
      tiledb_ctx_get_last_error(ctx_, &err);
      if (err != nullptr) {
        tiledb_error_message(err, &msg);
      }
      INFO(msg);

      REQUIRE(rc == TILEDB_OK);
    }
  }

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  for (int64_t i = 0; i < domain_size_0; ++i)
    delete[] buffer[i];
  delete[] buffer;
  delete[] buffer_a1;
}

void DenseArrayFx::write_dense_subarray_2D(
    const std::string& array_name,
    int64_t* subarray,
    tiledb_query_type_t query_type,
    tiledb_layout_t query_layout,
    int* buffer,
    uint64_t* buffer_sizes) {
  // Attribute to focus on and buffers
  const char* attributes[] = {ATTR_NAME};
  void* buffers[] = {buffer};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK_SAFE(rc == TILEDB_OK);
  rc = array_open_wrapper(
      ctx_, TILEDB_WRITE, (serialize_ && refactored_query_v2_), &array);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, query_type, &query);
  REQUIRE_SAFE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
  REQUIRE_SAFE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  REQUIRE_SAFE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, query_layout);
  REQUIRE_SAFE(rc == TILEDB_OK);

  // Submit query
  rc = submit_query_wrapper(
      ctx_,
      array_name,
      &query,
      server_buffers_,
      serialize_,
      refactored_query_v2_);
  REQUIRE_SAFE(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK_SAFE(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void DenseArrayFx::write_dense_subarray_2D_with_cancel(
    const std::string& array_name,
    int64_t* subarray,
    tiledb_query_type_t query_type,
    tiledb_layout_t query_layout,
    int* buffer,
    uint64_t* buffer_sizes) {
  // Attribute to focus on and buffers
  const char* attributes[] = {ATTR_NAME};
  void* buffers[] = {buffer};
  const unsigned num_writes = 10;

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = array_open_wrapper(
      ctx_, TILEDB_WRITE, (serialize_ && refactored_query_v2_), &array);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, query_type, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, query_layout);
  REQUIRE(rc == TILEDB_OK);

  auto proc_query = [&](unsigned i) -> void {
    rc = tiledb_query_submit_async(ctx_, query, NULL, NULL);
    REQUIRE(rc == TILEDB_OK);
    // Cancel it immediately.
    if (i < num_writes - 1) {
      rc = tiledb_ctx_cancel_tasks(ctx_);
      REQUIRE(rc == TILEDB_OK);
    }

    tiledb_query_status_t status;
    do {
      rc = tiledb_query_get_status(ctx_, query, &status);
      CHECK(rc == TILEDB_OK);
    } while (status != TILEDB_COMPLETED && status != TILEDB_FAILED);
    CHECK((status == TILEDB_COMPLETED || status == TILEDB_FAILED));

    // If it failed, run it again.
    if (status == TILEDB_FAILED) {
      rc = tiledb_query_submit_async(ctx_, query, NULL, NULL);
      CHECK(rc == TILEDB_OK);
      do {
        rc = tiledb_query_get_status(ctx_, query, &status);
        CHECK(rc == TILEDB_OK);
      } while (status != TILEDB_COMPLETED && status != TILEDB_FAILED);
    }
    REQUIRE(status == TILEDB_COMPLETED);
  };

  if (!use_external_subarray_) {
    // Submit the same query several times, some may be duplicates, some may
    // be cancelled, it doesn't matter since it's all the same data being
    // written.
    // TODO: this doesn't trigger the cancelled path very often.
    for (unsigned i = 0; i < num_writes; i++) {
      proc_query(i);
    }

    rc = tiledb_query_finalize(ctx_, query);
    REQUIRE(rc == TILEDB_OK);
  } else {
    tiledb_subarray_t* query_subarray;
    tiledb_query_get_subarray_t(ctx_, query, &query_subarray);
    rc = tiledb_query_set_subarray_t(ctx_, query, query_subarray);

    // Submit the same query several times, some may be duplicates, some may
    // be cancelled, it doesn't matter since it's all the same data being
    // written.
    // TODO: this doesn't trigger the cancelled path very often.
    for (unsigned i = 0; i < num_writes; i++) {
      CHECK(rc == TILEDB_OK);
      proc_query(i);
    }

    rc = tiledb_query_finalize(ctx_, query);
    REQUIRE(rc == TILEDB_OK);

    tiledb_subarray_free(&query_subarray);
  }

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void DenseArrayFx::check_sorted_reads(const std::string& path) {
  // Parameters used in this test
  int64_t domain_size_0 = 5000;
  int64_t domain_size_1 = 10000;
  int64_t tile_extent_0 = 1000;
  int64_t tile_extent_1 = 1000;
  int64_t domain_0_lo = 0;
  int64_t domain_0_hi = domain_size_0 - 1;
  int64_t domain_1_lo = 0;
  int64_t domain_1_hi = domain_size_1 - 1;
  uint64_t capacity = 1000000;
  tiledb_layout_t cell_order = TILEDB_ROW_MAJOR;
  tiledb_layout_t tile_order = TILEDB_ROW_MAJOR;
  std::string array_name = path + "sorted_reads_array";

  // Create a dense integer array
  create_dense_array_2D(
      array_name,
      tile_extent_0,
      tile_extent_1,
      domain_0_lo,
      domain_0_hi,
      domain_1_lo,
      domain_1_hi,
      capacity,
      cell_order,
      tile_order);

  // Write array cells with value = row id * COLUMNS + col id
  // to disk tile by tile
  write_dense_array_by_tiles(
      array_name, domain_size_0, domain_size_1, tile_extent_0, tile_extent_1);

  // Test random subarrays and check with corresponding value set by
  // row_id*dim1+col_id. Top left corner is always 4,4.
  int64_t d0_lo = 4;
  int64_t d0_hi = 0;
  int64_t d1_lo = 4;
  int64_t d1_hi = 0;
  int64_t height = 0, width = 0;

  for (int iter = 0; iter < ITER_NUM; ++iter) {
    height = std::rand() % (domain_size_0 - d0_lo);
    width = std::rand() % (domain_size_1 - d1_lo);
    d0_hi = d0_lo + height;
    d1_hi = d1_lo + width;
    int64_t index = 0;

    // Read subarray
    int* buffer = read_dense_array_2D(
        array_name, d0_lo, d0_hi, d1_lo, d1_hi, TILEDB_READ, TILEDB_ROW_MAJOR);
    REQUIRE(buffer != nullptr);

    bool allok = true;
    // Check
    for (int64_t i = d0_lo; i <= d0_hi; ++i) {
      for (int64_t j = d1_lo; j <= d1_hi; ++j) {
        bool match = (buffer[index] == i * domain_size_1 + j);
        if (!match) {
          allok = false;
          std::cout << "mismatch: " << i << "," << j << "=" << buffer[index]
                    << "!=" << ((i * domain_size_1 + j)) << "\n";
          break;
        }
        ++index;
      }
      if (!allok)
        break;
    }
    REQUIRE(allok);

    // Clean up
    delete[] buffer;
  }

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  if (encryption_type != TILEDB_NO_ENCRYPTION) {
    tiledb_config_t* cfg;
    tiledb_error_t* err = nullptr;
    REQUIRE(tiledb_config_alloc(&cfg, &err) == TILEDB_OK);
    REQUIRE(err == nullptr);
    std::string encryption_type_string =
        encryption_type_str((tiledb::sm::EncryptionType)encryption_type);
    REQUIRE(
        tiledb_config_set(
            cfg, "sm.encryption_type", encryption_type_string.c_str(), &err) ==
        TILEDB_OK);
    REQUIRE(err == nullptr);
    REQUIRE(
        tiledb_config_set(cfg, "sm.encryption_key", encryption_key, &err) ==
        TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_array_set_config(ctx_, array, cfg);
    REQUIRE(rc == TILEDB_OK);
    tiledb_config_free(&cfg);
  }
  rc = array_open_wrapper(
      ctx_, TILEDB_READ, (serialize_ && refactored_query_v2_), &array);
  CHECK(rc == TILEDB_OK);

  // Check out of bounds subarray
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

  REQUIRE(rc == TILEDB_OK);
  int64_t subarray_1[] = {-1, 5, 10, 10};
  rc = tiledb_query_set_subarray(ctx_, query, subarray_1);
  CHECK(rc == TILEDB_ERR);
  int64_t subarray_2[] = {0, 5000000, 10, 10};
  rc = tiledb_query_set_subarray(ctx_, query, subarray_2);
  CHECK(rc == TILEDB_ERR);
  int64_t subarray_3[] = {0, 5, -1, 10};
  rc = tiledb_query_set_subarray(ctx_, query, subarray_3);
  CHECK(rc == TILEDB_ERR);
  int64_t subarray_4[] = {0, 5, 10, 100000000};
  rc = tiledb_query_set_subarray(ctx_, query, subarray_4);
  CHECK(rc == TILEDB_ERR);
  int64_t subarray_5[] = {0, 5, 10, 10};
  rc = tiledb_query_set_subarray(ctx_, query, subarray_5);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_config_free(&config);
  tiledb_query_free(&query);
}

void DenseArrayFx::check_sorted_writes(const std::string& path) {
  // Parameters used in this test
  int64_t domain_size_0 = 100;
  int64_t domain_size_1 = 100;
  int64_t tile_extent_0 = 10;
  int64_t tile_extent_1 = 10;
  int64_t domain_0_lo = 0;
  int64_t domain_0_hi = domain_size_0 - 1;
  int64_t domain_1_lo = 0;
  int64_t domain_1_hi = domain_size_1 - 1;
  uint64_t capacity = 1000;
  tiledb_layout_t cell_order = TILEDB_ROW_MAJOR;
  tiledb_layout_t tile_order = TILEDB_ROW_MAJOR;
  std::string array_name = path + "sorted_writes_array";

  // Create a dense integer array
  create_dense_array_2D(
      array_name,
      tile_extent_0,
      tile_extent_1,
      domain_0_lo,
      domain_0_hi,
      domain_1_lo,
      domain_1_hi,
      capacity,
      cell_order,
      tile_order);

  // Write random subarray, then read it back and check
  int64_t d0[2], d1[2];
  for (int i = 0; i < ITER_NUM; ++i) {
    // Create subarray
    d0[0] = std::rand() % domain_size_0;
    d1[0] = std::rand() % domain_size_1;
    d0[1] = d0[0] + std::rand() % (domain_size_0 - d0[0]);
    d1[1] = d1[0] + std::rand() % (domain_size_1 - d1[0]);
    int64_t subarray[] = {d0[0], d0[1], d1[0], d1[1]};

    // Prepare buffers
    int64_t subarray_length[2] = {d0[1] - d0[0] + 1, d1[1] - d1[0] + 1};
    int64_t cell_num_in_subarray = subarray_length[0] * subarray_length[1];
    auto buffer = new int[cell_num_in_subarray];
    int64_t index = 0;
    uint64_t buffer_size = cell_num_in_subarray * sizeof(int);
    uint64_t buffer_sizes[] = {buffer_size};
    for (int64_t r = 0; r < subarray_length[0]; ++r)
      for (int64_t c = 0; c < subarray_length[1]; ++c)
        buffer[index++] = -(std::rand() % 999999);

    // Write 2D subarray
    write_dense_subarray_2D(
        array_name,
        subarray,
        TILEDB_WRITE,
        TILEDB_ROW_MAJOR,
        buffer,
        buffer_sizes);

    // Read back the same subarray
    int* read_buffer = read_dense_array_2D(
        array_name,
        subarray[0],
        subarray[1],
        subarray[2],
        subarray[3],
        TILEDB_READ,
        TILEDB_ROW_MAJOR);
    REQUIRE(read_buffer != nullptr);

    // Check the two buffers
    bool allok = true;
    for (index = 0; index < cell_num_in_subarray; ++index) {
      if (buffer[index] != read_buffer[index]) {
        allok = false;
        break;
      }
    }
    REQUIRE(allok);

    // Clean up
    delete[] buffer;
    delete[] read_buffer;
  }
}

void DenseArrayFx::check_invalid_cell_num_in_dense_writes(
    const std::string& path) {
  // Parameters used in this test
  int64_t domain_size_0 = 100;
  int64_t domain_size_1 = 100;
  int64_t tile_extent_0 = 10;
  int64_t tile_extent_1 = 10;
  int64_t domain_0_lo = 0;
  int64_t domain_0_hi = domain_size_0 - 1;
  int64_t domain_1_lo = 0;
  int64_t domain_1_hi = domain_size_1 - 1;
  uint64_t capacity = 1000;
  tiledb_layout_t cell_order = TILEDB_ROW_MAJOR;
  tiledb_layout_t tile_order = TILEDB_ROW_MAJOR;
  std::string array_name = path + "invalid_cell_num_dense_writes_array";

  // Create a dense integer array
  create_dense_array_2D(
      array_name,
      tile_extent_0,
      tile_extent_1,
      domain_0_lo,
      domain_0_hi,
      domain_1_lo,
      domain_1_hi,
      capacity,
      cell_order,
      tile_order);

  const char* attributes[] = {ATTR_NAME};
  int buffer[3] = {1, 2, 3};
  void* buffers[] = {buffer};
  uint64_t buffer_sizes[] = {sizeof(buffer)};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = array_open_wrapper(
      ctx_, TILEDB_WRITE, (serialize_ && refactored_query_v2_), &array);
  CHECK(rc == TILEDB_OK);

  // Global order
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx_, query);
  REQUIRE(rc == TILEDB_ERR);
  tiledb_query_free(&query);

  // Ordered layout
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = submit_query_wrapper(
      ctx_,
      array_name,
      &query,
      server_buffers_,
      serialize_,
      refactored_query_v2_);
  REQUIRE(rc == TILEDB_ERR);
  tiledb_query_free(&query);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
}

void DenseArrayFx::check_simultaneous_writes(const std::string& path) {
  // Parameters used in this test
  int64_t domain_size_0 = 100;
  int64_t domain_size_1 = 100;
  int64_t tile_extent_0 = 10;
  int64_t tile_extent_1 = 10;
  int64_t domain_0_lo = 0;
  int64_t domain_0_hi = domain_size_0 - 1;
  int64_t domain_1_lo = 0;
  int64_t domain_1_hi = domain_size_1 - 1;
  uint64_t capacity = 1000;
  tiledb_layout_t cell_order = TILEDB_ROW_MAJOR;
  tiledb_layout_t tile_order = TILEDB_ROW_MAJOR;
  std::string array_name = path + "simultaneous_writes_array";

  // Create a dense integer array
  create_dense_array_2D(
      array_name,
      tile_extent_0,
      tile_extent_1,
      domain_0_lo,
      domain_0_hi,
      domain_1_lo,
      domain_1_hi,
      capacity,
      cell_order,
      tile_order);

  int nthreads = std::thread::hardware_concurrency();
  std::vector<int*> buffers;
  std::vector<std::array<uint64_t, 1>> buffer_sizes;
  std::vector<std::array<int64_t, 4>> subarrays;
  std::vector<std::thread> threads;

  // Pre-generate buffers to write
  for (int i = 0; i < nthreads; i++) {
    subarrays.push_back(
        {{domain_0_lo,
          domain_0_lo + tile_extent_0 - 1,
          domain_1_lo,
          domain_1_lo + tile_extent_1 - 1}});
    buffer_sizes.push_back({{tile_extent_0 * tile_extent_1 * sizeof(int)}});
    buffers.push_back(new int[buffer_sizes.back()[0] / sizeof(int)]);
  }

  // Write multiple subarrays in parallel with a shared context.
  for (int i = 0; i < nthreads; i++) {
    threads.emplace_back([&, i]() {
      const int writes_per_thread = 5;
      for (int j = 0; j < writes_per_thread; j++) {
        write_dense_subarray_2D(
            array_name,
            subarrays[i].data(),
            TILEDB_WRITE,
            TILEDB_GLOBAL_ORDER,
            buffers[i],
            buffer_sizes[i].data());
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  for (int* buffer : buffers) {
    delete[] buffer;
  }
}

void DenseArrayFx::check_cancel_and_retry_writes(const std::string& path) {
  // Parameters used in this test
  int64_t domain_size_0 = 100;
  int64_t domain_size_1 = 100;
  int64_t tile_extent_0 = 10;
  int64_t tile_extent_1 = 10;
  int64_t domain_0_lo = 0;
  int64_t domain_0_hi = domain_size_0 - 1;
  int64_t domain_1_lo = 0;
  int64_t domain_1_hi = domain_size_1 - 1;
  uint64_t capacity = 1000;
  tiledb_layout_t cell_order = TILEDB_ROW_MAJOR;
  tiledb_layout_t tile_order = TILEDB_ROW_MAJOR;
  std::string array_name = path + "cancel_and_retry_writes_array";

  // Create a dense integer array
  create_dense_array_2D(
      array_name,
      tile_extent_0,
      tile_extent_1,
      domain_0_lo,
      domain_0_hi,
      domain_1_lo,
      domain_1_hi,
      capacity,
      cell_order,
      tile_order);

  int64_t subarray[] = {
      domain_0_lo,
      domain_0_lo + tile_extent_0 - 1,
      domain_1_lo,
      domain_1_lo + tile_extent_1 - 1};
  uint64_t buffer_sizes[] = {tile_extent_0 * tile_extent_1 * sizeof(int)};
  auto buffer = new int[buffer_sizes[0] / sizeof(int)];

  // Prepare buffer
  int64_t subarray_length[2] = {
      subarray[1] - subarray[0] + 1, subarray[3] - subarray[2] + 1};
  int64_t cell_num_in_subarray = subarray_length[0] * subarray_length[1];
  int64_t index = 0;
  for (int64_t r = 0; r < subarray_length[0]; ++r)
    for (int64_t c = 0; c < subarray_length[1]; ++c)
      buffer[index++] = -(std::rand() % 999999);

  write_dense_subarray_2D_with_cancel(
      array_name,
      subarray,
      TILEDB_WRITE,
      TILEDB_ROW_MAJOR,
      buffer,
      buffer_sizes);

  // Read back the same subarray
  int* read_buffer = read_dense_array_2D(
      array_name,
      subarray[0],
      subarray[1],
      subarray[2],
      subarray[3],
      TILEDB_READ,
      TILEDB_ROW_MAJOR);
  REQUIRE(read_buffer != nullptr);

  // Check the two buffers
  bool allok = true;
  for (index = 0; index < cell_num_in_subarray; ++index) {
    if (buffer[index] != read_buffer[index]) {
      allok = false;
      break;
    }
  }
  REQUIRE(allok);

  // Clean up
  delete[] buffer;
  delete[] read_buffer;
}

void DenseArrayFx::create_dense_array(const std::string& array_name) {
  // Create dimensions
  uint64_t dim_domain[] = {1, 4, 1, 4};
  uint64_t tile_extents[] = {2, 2};
  tiledb_dimension_t* d1;
  int rc = tiledb_dimension_alloc(
      ctx_, "d1", TILEDB_UINT64, &dim_domain[0], &tile_extents[0], &d1);
  CHECK(rc == TILEDB_OK);
  tiledb_dimension_t* d2;
  rc = tiledb_dimension_alloc(
      ctx_, "d2", TILEDB_UINT64, &dim_domain[2], &tile_extents[1], &d2);
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
  tiledb_attribute_t* a1;
  rc = tiledb_attribute_alloc(ctx_, "a1", TILEDB_INT32, &a1);
  CHECK(rc == TILEDB_OK);
  rc = set_attribute_compression_filter(ctx_, a1, TILEDB_FILTER_LZ4, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a1, 1);
  CHECK(rc == TILEDB_OK);
  tiledb_attribute_t* a2;
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_alloc(ctx_, "a2", TILEDB_CHAR, &a2);
  CHECK(rc == TILEDB_OK);
  rc = set_attribute_compression_filter(ctx_, a2, TILEDB_FILTER_GZIP, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a2, TILEDB_VAR_NUM);
  CHECK(rc == TILEDB_OK);
  tiledb_attribute_t* a3;
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_alloc(ctx_, "a3", TILEDB_FLOAT32, &a3);
  CHECK(rc == TILEDB_OK);
  rc = set_attribute_compression_filter(ctx_, a3, TILEDB_FILTER_ZSTD, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a3, 2);
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
  rc = tiledb_array_create(ctx_, array_name.c_str(), array_schema);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_attribute_free(&a1);
  tiledb_attribute_free(&a2);
  tiledb_attribute_free(&a3);
  tiledb_dimension_free(&d1);
  tiledb_dimension_free(&d2);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

void DenseArrayFx::create_dense_array_1_attribute(
    const std::string& array_name) {
  // Create dimensions
  uint64_t dim_domain[] = {1, 4, 1, 4};
  uint64_t tile_extents[] = {2, 2};
  tiledb_dimension_t* d1;
  int rc = tiledb_dimension_alloc(
      ctx_, "d1", TILEDB_UINT64, &dim_domain[0], &tile_extents[0], &d1);
  CHECK(rc == TILEDB_OK);
  tiledb_dimension_t* d2;
  rc = tiledb_dimension_alloc(
      ctx_, "d2", TILEDB_UINT64, &dim_domain[2], &tile_extents[1], &d2);
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
  tiledb_attribute_t* a1;
  rc = tiledb_attribute_alloc(ctx_, "a1", TILEDB_INT32, &a1);
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
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a1);
  CHECK(rc == TILEDB_OK);

  // Check array schema
  rc = tiledb_array_schema_check(ctx_, array_schema);
  CHECK(rc == TILEDB_OK);

  // Create array
  rc = tiledb_array_create(ctx_, array_name.c_str(), array_schema);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_attribute_free(&a1);
  tiledb_dimension_free(&d1);
  tiledb_dimension_free(&d2);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

void DenseArrayFx::create_dense_array_same_tile(const std::string& array_name) {
  // Create dimensions
  uint64_t dim_domain[] = {1, 5, 1, 4};
  uint64_t tile_extents[] = {5, 4};
  tiledb_dimension_t* d1;
  int rc = tiledb_dimension_alloc(
      ctx_, "d1", TILEDB_UINT64, &dim_domain[0], &tile_extents[0], &d1);
  CHECK(rc == TILEDB_OK);
  tiledb_dimension_t* d2;
  rc = tiledb_dimension_alloc(
      ctx_, "d2", TILEDB_UINT64, &dim_domain[2], &tile_extents[1], &d2);
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
  tiledb_attribute_t* a1;
  rc = tiledb_attribute_alloc(ctx_, "a1", TILEDB_INT32, &a1);
  CHECK(rc == TILEDB_OK);

  tiledb_attribute_t* a2;
  rc = tiledb_attribute_alloc(ctx_, "a2", TILEDB_INT32, &a2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a2, TILEDB_VAR_NUM);
  CHECK(rc == TILEDB_OK);

  // Create array schema
  tiledb_array_schema_t* array_schema;
  rc = tiledb_array_schema_alloc(ctx_, TILEDB_DENSE, &array_schema);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(ctx_, array_schema, TILEDB_COL_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx_, array_schema, TILEDB_COL_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a2);
  CHECK(rc == TILEDB_OK);

  // Check array schema
  rc = tiledb_array_schema_check(ctx_, array_schema);
  CHECK(rc == TILEDB_OK);

  // Create array
  rc = tiledb_array_create(ctx_, array_name.c_str(), array_schema);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_attribute_free(&a1);
  tiledb_dimension_free(&d1);
  tiledb_dimension_free(&d2);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

void DenseArrayFx::create_large_dense_array_1_attribute(
    const std::string& array_name) {
  // Create dimensions
  uint64_t dim_domain[] = {1, 20, 1, 15};
  uint64_t tile_extents[] = {4, 3};
  tiledb_dimension_t* d1;
  int rc = tiledb_dimension_alloc(
      ctx_, "d1", TILEDB_UINT64, &dim_domain[0], &tile_extents[0], &d1);
  CHECK(rc == TILEDB_OK);
  tiledb_dimension_t* d2;
  rc = tiledb_dimension_alloc(
      ctx_, "d2", TILEDB_UINT64, &dim_domain[2], &tile_extents[1], &d2);
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
  tiledb_attribute_t* a1;
  rc = tiledb_attribute_alloc(ctx_, "a1", TILEDB_UINT64, &a1);
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
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, a1);
  CHECK(rc == TILEDB_OK);

  // Check array schema
  rc = tiledb_array_schema_check(ctx_, array_schema);
  CHECK(rc == TILEDB_OK);

  // Create array
  rc = tiledb_array_create(ctx_, array_name.c_str(), array_schema);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_attribute_free(&a1);
  tiledb_dimension_free(&d1);
  tiledb_dimension_free(&d2);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

void DenseArrayFx::check_return_coords(
    const std::string& path, bool split_coords) {
  std::string array_name = path + "return_coords";
  create_dense_array(array_name);
  write_dense_array(array_name);
  read_dense_array_with_coords_full_global(array_name, split_coords);
  read_dense_array_with_coords_full_row(array_name, split_coords);
  read_dense_array_with_coords_full_col(array_name, split_coords);
  read_dense_array_with_coords_subarray_global(array_name, split_coords);
  read_dense_array_with_coords_subarray_row(array_name, split_coords);
  read_dense_array_with_coords_subarray_col(array_name, split_coords);
}

void DenseArrayFx::write_dense_array(const std::string& array_name) {
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

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = array_open_wrapper(
      ctx_, TILEDB_WRITE, (serialize_ && refactored_query_v2_), &array);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[1], buffers[2], &buffer_sizes[2]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, attributes[1], (uint64_t*)buffers[1], &buffer_sizes[1]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[2], buffers[3], &buffer_sizes[3]);
  CHECK(rc == TILEDB_OK);

  rc = submit_query_wrapper(
      ctx_,
      array_name,
      &query,
      server_buffers_,
      serialize_,
      refactored_query_v2_);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void DenseArrayFx::write_dense_array_missing_attributes(
    const std::string& array_name) {
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

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = array_open_wrapper(
      ctx_, TILEDB_WRITE, (serialize_ && refactored_query_v2_), &array);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[1], buffers[2], &buffer_sizes[2]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, attributes[1], (uint64_t*)buffers[1], &buffer_sizes[1]);
  CHECK(rc == TILEDB_OK);

  // Observe we omit setting buffer for one of the attributes (a3)

  // Submit query - this should fail
  rc = submit_query_wrapper(
      ctx_,
      array_name,
      &query,
      server_buffers_,
      serialize_,
      refactored_query_v2_);
  CHECK(rc == TILEDB_ERR);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void DenseArrayFx::write_partial_dense_array(const std::string& array_name) {
  // Set attributes
  const char* attributes[] = {"a1", "a2", "a3"};

  // Prepare cell buffers
  // clang-format off
  int buffer_a1[] = { 0,  1,  2,  3 };
  uint64_t buffer_a2[] = { 0,  1,  3,  6 };
  char buffer_var_a2[] = "abbcccdddd";
  float buffer_a3[] = { 0.1f,  0.2f,  1.1f,  1.2f,  2.1f,  2.2f,  3.1f,  3.2f };
  void* buffers[] = { buffer_a1, buffer_a2, buffer_var_a2, buffer_a3 };
  uint64_t buffer_sizes[] =
  {
      sizeof(buffer_a1),
      sizeof(buffer_a2),
      sizeof(buffer_var_a2)-1,  // No need to store the last '\0' character
      sizeof(buffer_a3)
  };
  // clang-format on

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = array_open_wrapper(
      ctx_, TILEDB_WRITE, (serialize_ && refactored_query_v2_), &array);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[1], buffers[2], &buffer_sizes[2]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, attributes[1], (uint64_t*)buffers[1], &buffer_sizes[1]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[2], buffers[3], &buffer_sizes[3]);
  CHECK(rc == TILEDB_OK);
  uint64_t subarray[] = {3, 4, 3, 4};
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  rc = submit_query_wrapper(
      ctx_,
      array_name,
      &query,
      server_buffers_,
      serialize_,
      refactored_query_v2_);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void DenseArrayFx::write_large_dense_array(const std::string& array_name) {
  // Prepare cell buffers
  // clang-format off
  uint64_t buffer_a1[] = {
     101,  102,  103,  104,  105,  106,  107,  108,  109,  110,  111,  112,  113,  114,  115,
     201,  202,  203,  204,  205,  206,  207,  208,  209,  210,  211,  212,  213,  214,  215,
     301,  302,  303,  304,  305,  306,  307,  308,  309,  310,  311,  312,  313,  314,  315,
     401,  402,  403,  404,  405,  406,  407,  408,  409,  410,  411,  412,  413,  414,  415,
     501,  502,  503,  504,  505,  506,  507,  508,  509,  510,  511,  512,  513,  514,  515,
     601,  602,  603,  604,  605,  606,  607,  608,  609,  610,  611,  612,  613,  614,  615,
     701,  702,  703,  704,  705,  706,  707,  708,  709,  710,  711,  712,  713,  714,  715,
     801,  802,  803,  804,  805,  806,  807,  808,  809,  810,  811,  812,  813,  814,  815,
     901,  902,  903,  904,  905,  906,  907,  908,  909,  910,  911,  912,  913,  914,  915,
    1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010, 1011, 1012, 1013, 1014, 1015,
    1101, 1102, 1103, 1104, 1105, 1106, 1107, 1108, 1109, 1110, 1111, 1112, 1113, 1114, 1115,
    1201, 1202, 1203, 1204, 1205, 1206, 1207, 1208, 1209, 1210, 1211, 1212, 1213, 1214, 1215,
    1301, 1302, 1303, 1304, 1305, 1306, 1307, 1308, 1309, 1310, 1311, 1312, 1313, 1314, 1315,
    1401, 1402, 1403, 1404, 1405, 1406, 1407, 1408, 1409, 1410, 1411, 1412, 1413, 1414, 1415,
    1501, 1502, 1503, 1504, 1505, 1506, 1507, 1508, 1509, 1510, 1511, 1512, 1513, 1514, 1515,
    1601, 1602, 1603, 1604, 1605, 1606, 1607, 1608, 1609, 1610, 1611, 1612, 1613, 1614, 1615,
    1701, 1702, 1703, 1704, 1705, 1706, 1707, 1708, 1709, 1710, 1711, 1712, 1713, 1714, 1715,
    1801, 1802, 1803, 1804, 1805, 1806, 1807, 1808, 1809, 1810, 1811, 1812, 1813, 1814, 1815,
    1901, 1902, 1903, 1904, 1905, 1906, 1907, 1908, 1909, 1910, 1911, 1912, 1913, 1914, 1915,
    2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015,
  };

  uint64_t buffer_size = sizeof(buffer_a1);
  // clang-format on

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a1", buffer_a1, &buffer_size);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = submit_query_wrapper(
      ctx_,
      array_name,
      &query,
      server_buffers_,
      serialize_,
      refactored_query_v2_);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void DenseArrayFx::read_dense_vector_mixed(const std::string& array_name) {
  // Correct buffer
  int c_a[410];
  for (int i = 0; i < 410; ++i)
    c_a[i] = i;
  c_a[49] = 1050;
  c_a[99] = 1100;
  c_a[149] = 1150;
  c_a[24] = 1025;
  c_a[74] = 1075;
  c_a[124] = 1125;

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  if (encryption_type != TILEDB_NO_ENCRYPTION) {
    tiledb_config_t* cfg;
    tiledb_error_t* err = nullptr;
    REQUIRE(tiledb_config_alloc(&cfg, &err) == TILEDB_OK);
    REQUIRE(err == nullptr);
    std::string encryption_type_string =
        encryption_type_str((tiledb::sm::EncryptionType)encryption_type);
    REQUIRE(
        tiledb_config_set(
            cfg, "sm.encryption_type", encryption_type_string.c_str(), &err) ==
        TILEDB_OK);
    REQUIRE(err == nullptr);
    REQUIRE(
        tiledb_config_set(cfg, "sm.encryption_key", encryption_key, &err) ==
        TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_array_set_config(ctx_, array, cfg);
    REQUIRE(rc == TILEDB_OK);
    tiledb_config_free(&cfg);
  }
  rc = array_open_wrapper(
      ctx_, TILEDB_READ, (serialize_ && refactored_query_v2_), &array);
  CHECK(rc == TILEDB_OK);

  // Preparation
  uint64_t subarray[] = {1, 410};
  int a[410];
  uint64_t a_size = sizeof(a);

  // Submit query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", a, &a_size);
  CHECK(rc == TILEDB_OK);
  rc = submit_query_wrapper(
      ctx_,
      array_name,
      &query,
      server_buffers_,
      serialize_,
      refactored_query_v2_);
  CHECK(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_COMPLETED);

  // Check buffers
  CHECK(sizeof(c_a) == a_size);
  for (int i = 0; i < 410; ++i)
    CHECK(a[i] == c_a[i]);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void DenseArrayFx::read_dense_array_with_coords_full_global(
    const std::string& array_name, bool split_coords) {
  // Correct buffers
  int c_buffer_a1[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
  uint64_t c_buffer_a2_off[] = {
      0, 1, 3, 6, 10, 11, 13, 16, 20, 21, 23, 26, 30, 31, 33, 36};
  char c_buffer_a2_val[] =
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
  uint64_t c_buffer_coords_dim1[] = {
      1, 1, 2, 2, 1, 1, 2, 2, 3, 3, 4, 4, 3, 3, 4, 4};
  uint64_t c_buffer_coords_dim2[] = {
      1, 2, 1, 2, 3, 4, 3, 4, 1, 2, 1, 2, 3, 4, 3, 4};
  uint64_t c_buffer_d1[] = {1, 1, 2, 2, 1, 1, 2, 2, 3, 3, 4, 4, 3, 3, 4, 4};
  uint64_t c_buffer_d2[] = {1, 2, 1, 2, 3, 4, 3, 4, 1, 2, 1, 2, 3, 4, 3, 4};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = array_open_wrapper(
      ctx_, TILEDB_READ, (serialize_ && refactored_query_v2_), &array);
  CHECK(rc == TILEDB_OK);

  // Compute max buffer sizes
  uint64_t subarray[] = {1, 4, 1, 4};
  uint64_t buffer_a1_size = 64;
  uint64_t buffer_a2_off_size = 128;
  uint64_t buffer_a2_val_size = 56;
  uint64_t buffer_a3_size = 128;
  uint64_t buffer_coords_dim1_size = 128;
  uint64_t buffer_coords_dim2_size = 128;
  uint64_t buffer_d1_size = 128;
  uint64_t buffer_d2_size = 128;

  // Prepare cell buffers
  auto buffer_a1 = (int*)malloc(buffer_a1_size);
  auto buffer_a2_off = (uint64_t*)malloc(buffer_a2_off_size);
  auto buffer_a2_val = (char*)malloc(buffer_a2_val_size);
  auto buffer_a3 = (float*)malloc(buffer_a3_size);
  auto buffer_coords_dim1 = (uint64_t*)malloc(buffer_coords_dim1_size);
  auto buffer_coords_dim2 = (uint64_t*)malloc(buffer_coords_dim2_size);
  auto buffer_d1 = (uint64_t*)malloc(buffer_d1_size);
  auto buffer_d2 = (uint64_t*)malloc(buffer_d2_size);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a1", buffer_a1, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a2", buffer_a2_val, &buffer_a2_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, "a2", buffer_a2_off, &buffer_a2_off_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a3", buffer_a3, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);
  if (split_coords) {
    rc = tiledb_query_set_data_buffer(
        ctx_, query, "d1", buffer_d1, &buffer_d1_size);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_query_set_data_buffer(
        ctx_, query, "d2", buffer_d2, &buffer_d2_size);
    CHECK(rc == TILEDB_OK);
  } else {
    rc = tiledb_query_set_data_buffer(
        ctx_, query, "d1", buffer_coords_dim1, &buffer_coords_dim1_size);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_query_set_data_buffer(
        ctx_, query, "d2", buffer_coords_dim2, &buffer_coords_dim2_size);
    CHECK(rc == TILEDB_OK);
  }

  // Submit query
  rc = submit_query_wrapper(
      ctx_,
      array_name,
      &query,
      server_buffers_,
      serialize_,
      refactored_query_v2_);
  CHECK(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_COMPLETED);

  // Check buffers
  CHECK(sizeof(c_buffer_a1) == buffer_a1_size);
  CHECK(sizeof(c_buffer_a2_off) == buffer_a2_off_size);
  CHECK(sizeof(c_buffer_a2_val) - 1 == buffer_a2_val_size);
  CHECK(sizeof(c_buffer_a3) == buffer_a3_size);
  CHECK(!memcmp(buffer_a1, c_buffer_a1, sizeof(c_buffer_a1)));
  CHECK(!memcmp(buffer_a2_off, c_buffer_a2_off, sizeof(c_buffer_a2_off)));
  CHECK(!memcmp(buffer_a2_val, c_buffer_a2_val, sizeof(c_buffer_a2_val) - 1));
  CHECK(!memcmp(buffer_a3, c_buffer_a3, sizeof(c_buffer_a3)));

  if (split_coords) {
    CHECK(sizeof(c_buffer_d1) == buffer_d1_size);
    CHECK(!memcmp(buffer_d1, c_buffer_d1, sizeof(c_buffer_d1)));
    CHECK(sizeof(c_buffer_d2) == buffer_d2_size);
    CHECK(!memcmp(buffer_d2, c_buffer_d2, sizeof(c_buffer_d2)));
  } else {
    CHECK(sizeof(c_buffer_coords_dim1) == buffer_coords_dim1_size);
    CHECK(!memcmp(
        buffer_coords_dim1,
        c_buffer_coords_dim1,
        sizeof(c_buffer_coords_dim1)));
    CHECK(sizeof(c_buffer_coords_dim2) == buffer_coords_dim2_size);
    CHECK(!memcmp(
        buffer_coords_dim2,
        c_buffer_coords_dim2,
        sizeof(c_buffer_coords_dim2)));
  }

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  free(buffer_a1);
  free(buffer_a2_off);
  free(buffer_a2_val);
  free(buffer_a3);
  free(buffer_coords_dim1);
  free(buffer_coords_dim2);
  free(buffer_d1);
  free(buffer_d2);
}

void DenseArrayFx::read_dense_array_with_coords_full_row(
    const std::string& array_name, bool split_coords) {
  // Correct buffers
  int c_buffer_a1[] = {0, 1, 4, 5, 2, 3, 6, 7, 8, 9, 12, 13, 10, 11, 14, 15};
  uint64_t c_buffer_a2_off[] = {
      0, 1, 3, 4, 6, 9, 13, 16, 20, 21, 23, 24, 26, 29, 33, 36};
  char c_buffer_a2_val[] =
      "abbeff"
      "cccddddggghhhh"
      "ijjmnn"
      "kkkllllooopppp";
  float c_buffer_a3[] = {
      0.1f,  0.2f,  1.1f,  1.2f,  4.1f,  4.2f,  5.1f,  5.2f,
      2.1f,  2.2f,  3.1f,  3.2f,  6.1f,  6.2f,  7.1f,  7.2f,
      8.1f,  8.2f,  9.1f,  9.2f,  12.1f, 12.2f, 13.1f, 13.2f,
      10.1f, 10.2f, 11.1f, 11.2f, 14.1f, 14.2f, 15.1f, 15.2f,
  };
  uint64_t c_buffer_coords_dim1[] = {
      1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4};
  uint64_t c_buffer_coords_dim2[] = {
      1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4};

  uint64_t c_buffer_d1[] = {1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4};
  uint64_t c_buffer_d2[] = {1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = array_open_wrapper(
      ctx_, TILEDB_READ, (serialize_ && refactored_query_v2_), &array);
  CHECK(rc == TILEDB_OK);

  // Compute max buffer sizes
  uint64_t subarray[] = {1, 4, 1, 4};
  uint64_t buffer_a1_size = 64;
  uint64_t buffer_a2_off_size = 128;
  uint64_t buffer_a2_val_size = 56;
  uint64_t buffer_a3_size = 128;
  uint64_t buffer_coords_dim1_size = 128;
  uint64_t buffer_coords_dim2_size = 128;
  uint64_t buffer_d1_size = 128;
  uint64_t buffer_d2_size = 128;

  // Prepare cell buffers
  auto buffer_a1 = (int*)malloc(buffer_a1_size);
  auto buffer_a2_off = (uint64_t*)malloc(buffer_a2_off_size);
  auto buffer_a2_val = (char*)malloc(buffer_a2_val_size);
  auto buffer_a3 = (float*)malloc(buffer_a3_size);
  auto buffer_coords_dim1 = (uint64_t*)malloc(buffer_coords_dim1_size);
  auto buffer_coords_dim2 = (uint64_t*)malloc(buffer_coords_dim2_size);
  auto buffer_d1 = (uint64_t*)malloc(buffer_d1_size);
  auto buffer_d2 = (uint64_t*)malloc(buffer_d2_size);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a1", buffer_a1, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a2", buffer_a2_val, &buffer_a2_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, "a2", buffer_a2_off, &buffer_a2_off_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a3", buffer_a3, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);
  if (split_coords) {
    rc = tiledb_query_set_data_buffer(
        ctx_, query, "d1", buffer_d1, &buffer_d1_size);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_query_set_data_buffer(
        ctx_, query, "d2", buffer_d2, &buffer_d2_size);
    CHECK(rc == TILEDB_OK);
  } else {
    rc = tiledb_query_set_data_buffer(
        ctx_, query, "d1", buffer_coords_dim1, &buffer_coords_dim1_size);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_query_set_data_buffer(
        ctx_, query, "d2", buffer_coords_dim2, &buffer_coords_dim2_size);
    CHECK(rc == TILEDB_OK);
  }

  // Submit query
  rc = submit_query_wrapper(
      ctx_,
      array_name,
      &query,
      server_buffers_,
      serialize_,
      refactored_query_v2_);
  CHECK(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_COMPLETED);

  // Check buffers
  CHECK(sizeof(c_buffer_a1) == buffer_a1_size);
  CHECK(sizeof(c_buffer_a2_off) == buffer_a2_off_size);
  CHECK(sizeof(c_buffer_a2_val) - 1 == buffer_a2_val_size);
  CHECK(sizeof(c_buffer_a3) == buffer_a3_size);
  CHECK(!memcmp(buffer_a1, c_buffer_a1, sizeof(c_buffer_a1)));
  CHECK(!memcmp(buffer_a2_off, c_buffer_a2_off, sizeof(c_buffer_a2_off)));
  CHECK(!memcmp(buffer_a2_val, c_buffer_a2_val, sizeof(c_buffer_a2_val) - 1));
  CHECK(!memcmp(buffer_a3, c_buffer_a3, sizeof(c_buffer_a3)));

  if (split_coords) {
    CHECK(sizeof(c_buffer_d1) == buffer_d1_size);
    CHECK(!memcmp(buffer_d1, c_buffer_d1, sizeof(c_buffer_d1)));
    CHECK(sizeof(c_buffer_d2) == buffer_d2_size);
    CHECK(!memcmp(buffer_d2, c_buffer_d2, sizeof(c_buffer_d2)));
  } else {
    CHECK(sizeof(c_buffer_coords_dim1) == buffer_coords_dim1_size);
    CHECK(!memcmp(
        buffer_coords_dim1,
        c_buffer_coords_dim1,
        sizeof(c_buffer_coords_dim1)));
    CHECK(sizeof(c_buffer_coords_dim2) == buffer_coords_dim2_size);
    CHECK(!memcmp(
        buffer_coords_dim2,
        c_buffer_coords_dim2,
        sizeof(c_buffer_coords_dim2)));
  }

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  free(buffer_a1);
  free(buffer_a2_off);
  free(buffer_a2_val);
  free(buffer_a3);
  free(buffer_coords_dim1);
  free(buffer_coords_dim2);
  free(buffer_d1);
  free(buffer_d2);
}

void DenseArrayFx::read_dense_array_with_coords_full_col(
    const std::string& array_name, bool split_coords) {
  // Correct buffers
  int c_buffer_a1[] = {0, 2, 8, 10, 1, 3, 9, 11, 4, 6, 12, 14, 5, 7, 13, 15};
  uint64_t c_buffer_a2_off[] = {
      0, 1, 4, 5, 8, 10, 14, 16, 20, 21, 24, 25, 28, 30, 34, 36};
  char c_buffer_a2_val[] =
      "acccikkk"
      "bbddddjjllll"
      "egggmooo"
      "ffhhhhnnpppp";
  float c_buffer_a3[] = {
      0.1f,  0.2f,  2.1f, 2.2f,  8.1f,  8.2f, 10.1f, 10.2f, 1.1f,  1.2f,  3.1f,
      3.2f,  9.1f,  9.2f, 11.1f, 11.2f, 4.1f, 4.2f,  6.1f,  6.2f,  12.1f, 12.2f,
      14.1f, 14.2f, 5.1f, 5.2f,  7.1f,  7.2f, 13.1f, 13.2f, 15.1f, 15.2f,
  };
  uint64_t c_buffer_coords_dim1[] = {
      1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4};
  uint64_t c_buffer_coords_dim2[] = {
      1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4};
  uint64_t c_buffer_d1[] = {1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4};
  uint64_t c_buffer_d2[] = {1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = array_open_wrapper(
      ctx_, TILEDB_READ, (serialize_ && refactored_query_v2_), &array);
  CHECK(rc == TILEDB_OK);

  // Compute max buffer sizes
  uint64_t subarray[] = {1, 4, 1, 4};
  uint64_t buffer_a1_size = 64;
  uint64_t buffer_a2_off_size = 128;
  uint64_t buffer_a2_val_size = 56;
  uint64_t buffer_a3_size = 128;
  uint64_t buffer_coords_dim1_size = 128;
  uint64_t buffer_coords_dim2_size = 128;
  uint64_t buffer_d1_size = 128;
  uint64_t buffer_d2_size = 128;

  // Prepare cell buffers
  auto buffer_a1 = (int*)malloc(buffer_a1_size);
  auto buffer_a2_off = (uint64_t*)malloc(buffer_a2_off_size);
  auto buffer_a2_val = (char*)malloc(buffer_a2_val_size);
  auto buffer_a3 = (float*)malloc(buffer_a3_size);
  auto buffer_coords_dim1 = (uint64_t*)malloc(buffer_coords_dim1_size);
  auto buffer_coords_dim2 = (uint64_t*)malloc(buffer_coords_dim2_size);
  auto buffer_d1 = (uint64_t*)malloc(buffer_d1_size);
  auto buffer_d2 = (uint64_t*)malloc(buffer_d2_size);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_COL_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a1", buffer_a1, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a2", buffer_a2_val, &buffer_a2_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, "a2", buffer_a2_off, &buffer_a2_off_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a3", buffer_a3, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);
  if (split_coords) {
    rc = tiledb_query_set_data_buffer(
        ctx_, query, "d1", buffer_d1, &buffer_d1_size);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_query_set_data_buffer(
        ctx_, query, "d2", buffer_d2, &buffer_d2_size);
    CHECK(rc == TILEDB_OK);
  } else {
    rc = tiledb_query_set_data_buffer(
        ctx_, query, "d1", buffer_coords_dim1, &buffer_coords_dim1_size);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_query_set_data_buffer(
        ctx_, query, "d2", buffer_coords_dim2, &buffer_coords_dim2_size);
    CHECK(rc == TILEDB_OK);
  }

  // Submit query
  rc = submit_query_wrapper(
      ctx_,
      array_name,
      &query,
      server_buffers_,
      serialize_,
      refactored_query_v2_);
  CHECK(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_COMPLETED);

  // Check buffers
  CHECK(sizeof(c_buffer_a1) == buffer_a1_size);
  CHECK(sizeof(c_buffer_a2_off) == buffer_a2_off_size);
  CHECK(sizeof(c_buffer_a2_val) - 1 == buffer_a2_val_size);
  CHECK(sizeof(c_buffer_a3) == buffer_a3_size);
  CHECK(!memcmp(buffer_a1, c_buffer_a1, sizeof(c_buffer_a1)));
  CHECK(!memcmp(buffer_a2_off, c_buffer_a2_off, sizeof(c_buffer_a2_off)));
  CHECK(!memcmp(buffer_a2_val, c_buffer_a2_val, sizeof(c_buffer_a2_val) - 1));
  CHECK(!memcmp(buffer_a3, c_buffer_a3, sizeof(c_buffer_a3)));

  if (split_coords) {
    CHECK(sizeof(c_buffer_d1) == buffer_d1_size);
    CHECK(!memcmp(buffer_d1, c_buffer_d1, sizeof(c_buffer_d1)));
    CHECK(sizeof(c_buffer_d2) == buffer_d2_size);
    CHECK(!memcmp(buffer_d2, c_buffer_d2, sizeof(c_buffer_d2)));
  } else {
    CHECK(sizeof(c_buffer_coords_dim1) == buffer_coords_dim1_size);
    CHECK(!memcmp(
        buffer_coords_dim1,
        c_buffer_coords_dim1,
        sizeof(c_buffer_coords_dim1)));
    CHECK(sizeof(c_buffer_coords_dim2) == buffer_coords_dim2_size);
    CHECK(!memcmp(
        buffer_coords_dim2,
        c_buffer_coords_dim2,
        sizeof(c_buffer_coords_dim2)));
  }

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  free(buffer_a1);
  free(buffer_a2_off);
  free(buffer_a2_val);
  free(buffer_a3);
  free(buffer_coords_dim1);
  free(buffer_coords_dim2);
  free(buffer_d1);
  free(buffer_d2);
}

void DenseArrayFx::read_dense_array_with_coords_subarray_global(
    const std::string& array_name, bool split_coords) {
  // Correct buffers
  int c_buffer_a1[] = {9, 11, 12, 13, 14, 15};
  uint64_t c_buffer_a2_off[] = {0, 2, 6, 7, 9, 12};
  char c_buffer_a2_val[] = "jjllllmnnooopppp";
  float c_buffer_a3[] = {
      9.1f,
      9.2f,
      11.1f,
      11.2f,
      12.1f,
      12.2f,
      13.1f,
      13.2f,
      14.1f,
      14.2f,
      15.1f,
      15.2f,
  };
  uint64_t c_buffer_coords_dim1[] = {3, 4, 3, 3, 4, 4};
  uint64_t c_buffer_coords_dim2[] = {2, 2, 3, 4, 3, 4};
  uint64_t c_buffer_d1[] = {3, 4, 3, 3, 4, 4};
  uint64_t c_buffer_d2[] = {2, 2, 3, 4, 3, 4};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = array_open_wrapper(
      ctx_, TILEDB_READ, (serialize_ && refactored_query_v2_), &array);
  CHECK(rc == TILEDB_OK);

  // Compute max buffer sizes
  uint64_t subarray[] = {3, 4, 2, 4};
  uint64_t buffer_a1_size = 24;
  uint64_t buffer_a2_off_size = 48;
  uint64_t buffer_a2_val_size = 26;
  uint64_t buffer_a3_size = 48;
  uint64_t buffer_coords_dim1_size = 48;
  uint64_t buffer_coords_dim2_size = 48;
  uint64_t buffer_d1_size = 48;
  uint64_t buffer_d2_size = 48;

  // Prepare cell buffers
  auto buffer_a1 = (int*)malloc(buffer_a1_size);
  auto buffer_a2_off = (uint64_t*)malloc(buffer_a2_off_size);
  auto buffer_a2_val = (char*)malloc(buffer_a2_val_size);
  auto buffer_a3 = (float*)malloc(buffer_a3_size);
  auto buffer_coords_dim1 = (uint64_t*)malloc(buffer_coords_dim1_size);
  auto buffer_coords_dim2 = (uint64_t*)malloc(buffer_coords_dim2_size);
  auto buffer_d1 = (uint64_t*)malloc(buffer_d1_size);
  auto buffer_d2 = (uint64_t*)malloc(buffer_d2_size);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a1", buffer_a1, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a2", buffer_a2_val, &buffer_a2_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, "a2", buffer_a2_off, &buffer_a2_off_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a3", buffer_a3, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);
  if (split_coords) {
    rc = tiledb_query_set_data_buffer(
        ctx_, query, "d1", buffer_d1, &buffer_d1_size);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_query_set_data_buffer(
        ctx_, query, "d2", buffer_d2, &buffer_d2_size);
    CHECK(rc == TILEDB_OK);
  } else {
    rc = tiledb_query_set_data_buffer(
        ctx_, query, "d1", buffer_coords_dim1, &buffer_coords_dim1_size);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_query_set_data_buffer(
        ctx_, query, "d2", buffer_coords_dim2, &buffer_coords_dim2_size);
    CHECK(rc == TILEDB_OK);
  }

  // Submit query
  rc = submit_query_wrapper(
      ctx_,
      array_name,
      &query,
      server_buffers_,
      serialize_,
      refactored_query_v2_);
  CHECK(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_COMPLETED);

  // Check buffers
  CHECK(sizeof(c_buffer_a1) <= buffer_a1_size);
  CHECK(sizeof(c_buffer_a2_off) <= buffer_a2_off_size);
  CHECK(sizeof(c_buffer_a2_val) - 1 <= buffer_a2_val_size);
  CHECK(sizeof(c_buffer_a3) <= buffer_a3_size);
  CHECK(!memcmp(buffer_a1, c_buffer_a1, sizeof(c_buffer_a1)));
  CHECK(!memcmp(buffer_a2_off, c_buffer_a2_off, sizeof(c_buffer_a2_off)));
  CHECK(!memcmp(buffer_a2_val, c_buffer_a2_val, sizeof(c_buffer_a2_val) - 1));
  CHECK(!memcmp(buffer_a3, c_buffer_a3, sizeof(c_buffer_a3)));

  if (split_coords) {
    CHECK(sizeof(c_buffer_d1) == buffer_d1_size);
    CHECK(!memcmp(buffer_d1, c_buffer_d1, sizeof(c_buffer_d1)));
    CHECK(sizeof(c_buffer_d2) == buffer_d2_size);
    CHECK(!memcmp(buffer_d2, c_buffer_d2, sizeof(c_buffer_d2)));
  } else {
    CHECK(sizeof(c_buffer_coords_dim1) == buffer_coords_dim1_size);
    CHECK(!memcmp(
        buffer_coords_dim1,
        c_buffer_coords_dim1,
        sizeof(c_buffer_coords_dim1)));
    CHECK(sizeof(c_buffer_coords_dim2) == buffer_coords_dim2_size);
    CHECK(!memcmp(
        buffer_coords_dim2,
        c_buffer_coords_dim2,
        sizeof(c_buffer_coords_dim2)));
  }

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  free(buffer_a1);
  free(buffer_a2_off);
  free(buffer_a2_val);
  free(buffer_a3);
  free(buffer_coords_dim1);
  free(buffer_coords_dim2);
  free(buffer_d1);
  free(buffer_d2);
}

void DenseArrayFx::read_dense_array_with_coords_subarray_row(
    const std::string& array_name, bool split_coords) {
  // Correct buffers
  int c_buffer_a1[] = {9, 12, 13, 11, 14, 15};
  uint64_t c_buffer_a2_off[] = {0, 2, 3, 5, 9, 12};
  char c_buffer_a2_val[] = "jjmnnllllooopppp";
  float c_buffer_a3[] = {
      9.1f,
      9.2f,
      12.1f,
      12.2f,
      13.1f,
      13.2f,
      11.1f,
      11.2f,
      14.1f,
      14.2f,
      15.1f,
      15.2f};
  uint64_t c_buffer_coords_dim1[] = {3, 3, 3, 4, 4, 4};
  uint64_t c_buffer_coords_dim2[] = {2, 3, 4, 2, 3, 4};
  uint64_t c_buffer_d1[] = {3, 3, 3, 4, 4, 4};
  uint64_t c_buffer_d2[] = {2, 3, 4, 2, 3, 4};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = array_open_wrapper(
      ctx_, TILEDB_READ, (serialize_ && refactored_query_v2_), &array);
  CHECK(rc == TILEDB_OK);

  // Compute max buffer sizes
  uint64_t subarray[] = {3, 4, 2, 4};
  uint64_t buffer_a1_size = 24;
  uint64_t buffer_a2_off_size = 48;
  uint64_t buffer_a2_val_size = 26;
  uint64_t buffer_a3_size = 48;
  uint64_t buffer_coords_dim1_size = 48;
  uint64_t buffer_coords_dim2_size = 48;
  uint64_t buffer_d1_size = 48;
  uint64_t buffer_d2_size = 48;

  // Prepare cell buffers
  auto buffer_a1 = (int*)malloc(buffer_a1_size);
  auto buffer_a2_off = (uint64_t*)malloc(buffer_a2_off_size);
  auto buffer_a2_val = (char*)malloc(buffer_a2_val_size);
  auto buffer_a3 = (float*)malloc(buffer_a3_size);
  auto buffer_coords_dim1 = (uint64_t*)malloc(buffer_coords_dim1_size);
  auto buffer_coords_dim2 = (uint64_t*)malloc(buffer_coords_dim2_size);
  auto buffer_d1 = (uint64_t*)malloc(buffer_d1_size);
  auto buffer_d2 = (uint64_t*)malloc(buffer_d2_size);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a1", buffer_a1, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a2", buffer_a2_val, &buffer_a2_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, "a2", buffer_a2_off, &buffer_a2_off_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a3", buffer_a3, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);
  if (split_coords) {
    rc = tiledb_query_set_data_buffer(
        ctx_, query, "d1", buffer_d1, &buffer_d1_size);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_query_set_data_buffer(
        ctx_, query, "d2", buffer_d2, &buffer_d2_size);
    CHECK(rc == TILEDB_OK);
  } else {
    rc = tiledb_query_set_data_buffer(
        ctx_, query, "d1", buffer_coords_dim1, &buffer_coords_dim1_size);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_query_set_data_buffer(
        ctx_, query, "d2", buffer_coords_dim2, &buffer_coords_dim2_size);
    CHECK(rc == TILEDB_OK);
  }

  // Submit query
  rc = submit_query_wrapper(
      ctx_,
      array_name,
      &query,
      server_buffers_,
      serialize_,
      refactored_query_v2_);
  CHECK(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_COMPLETED);

  // Check buffers
  CHECK(sizeof(c_buffer_a1) == buffer_a1_size);
  CHECK(sizeof(c_buffer_a2_off) == buffer_a2_off_size);
  CHECK(sizeof(c_buffer_a2_val) - 1 == buffer_a2_val_size);
  CHECK(sizeof(c_buffer_a3) == buffer_a3_size);
  CHECK(!memcmp(buffer_a1, c_buffer_a1, sizeof(c_buffer_a1)));
  CHECK(!memcmp(buffer_a2_off, c_buffer_a2_off, sizeof(c_buffer_a2_off)));
  CHECK(!memcmp(buffer_a2_val, c_buffer_a2_val, sizeof(c_buffer_a2_val) - 1));
  CHECK(!memcmp(buffer_a3, c_buffer_a3, sizeof(c_buffer_a3)));

  if (split_coords) {
    CHECK(sizeof(c_buffer_d1) == buffer_d1_size);
    CHECK(!memcmp(buffer_d1, c_buffer_d1, sizeof(c_buffer_d1)));
    CHECK(sizeof(c_buffer_d2) == buffer_d2_size);
    CHECK(!memcmp(buffer_d2, c_buffer_d2, sizeof(c_buffer_d2)));
  } else {
    CHECK(sizeof(c_buffer_coords_dim1) == buffer_coords_dim1_size);
    CHECK(!memcmp(
        buffer_coords_dim1,
        c_buffer_coords_dim1,
        sizeof(c_buffer_coords_dim1)));
    CHECK(sizeof(c_buffer_coords_dim2) == buffer_coords_dim2_size);
    CHECK(!memcmp(
        buffer_coords_dim2,
        c_buffer_coords_dim2,
        sizeof(c_buffer_coords_dim2)));
  }

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  free(buffer_a1);
  free(buffer_a2_off);
  free(buffer_a2_val);
  free(buffer_a3);
  free(buffer_coords_dim1);
  free(buffer_coords_dim2);
  free(buffer_d1);
  free(buffer_d2);
}

void DenseArrayFx::read_dense_array_with_coords_subarray_col(
    const std::string& array_name, bool split_coords) {
  // Correct buffers
  int c_buffer_a1[] = {9, 11, 12, 14, 13, 15};
  uint64_t c_buffer_a2_off[] = {0, 2, 6, 7, 10, 12};
  char c_buffer_a2_val[] = "jjllllmooonnpppp";
  float c_buffer_a3[] = {
      9.1f,
      9.2f,
      11.1f,
      11.2f,
      12.1f,
      12.2f,
      14.1f,
      14.2f,
      13.1f,
      13.2f,
      15.1f,
      15.2f};
  uint64_t c_buffer_coords_dim1[] = {3, 4, 3, 4, 3, 4};
  uint64_t c_buffer_coords_dim2[] = {2, 2, 3, 3, 4, 4};
  uint64_t c_buffer_d1[] = {3, 4, 3, 4, 3, 4};
  uint64_t c_buffer_d2[] = {2, 2, 3, 3, 4, 4};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = array_open_wrapper(
      ctx_, TILEDB_READ, (serialize_ && refactored_query_v2_), &array);
  CHECK(rc == TILEDB_OK);

  // Compute max buffer sizes
  uint64_t subarray[] = {3, 4, 2, 4};
  uint64_t buffer_a1_size = 24;
  uint64_t buffer_a2_off_size = 48;
  uint64_t buffer_a2_val_size = 26;
  uint64_t buffer_a3_size = 48;
  uint64_t buffer_coords_dim1_size = 48;
  uint64_t buffer_coords_dim2_size = 48;
  uint64_t buffer_d1_size = 48;
  uint64_t buffer_d2_size = 48;

  // Prepare cell buffers
  auto buffer_a1 = (int*)malloc(buffer_a1_size);
  auto buffer_a2_off = (uint64_t*)malloc(buffer_a2_off_size);
  auto buffer_a2_val = (char*)malloc(buffer_a2_val_size);
  auto buffer_a3 = (float*)malloc(buffer_a3_size);
  auto buffer_coords_dim1 = (uint64_t*)malloc(buffer_coords_dim1_size);
  auto buffer_coords_dim2 = (uint64_t*)malloc(buffer_coords_dim2_size);
  auto buffer_d1 = (uint64_t*)malloc(buffer_d1_size);
  auto buffer_d2 = (uint64_t*)malloc(buffer_d2_size);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_COL_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a1", buffer_a1, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a2", buffer_a2_val, &buffer_a2_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, "a2", buffer_a2_off, &buffer_a2_off_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a3", buffer_a3, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);
  if (split_coords) {
    rc = tiledb_query_set_data_buffer(
        ctx_, query, "d1", buffer_d1, &buffer_d1_size);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_query_set_data_buffer(
        ctx_, query, "d2", buffer_d2, &buffer_d2_size);
    CHECK(rc == TILEDB_OK);
  } else {
    rc = tiledb_query_set_data_buffer(
        ctx_, query, "d1", buffer_coords_dim1, &buffer_coords_dim1_size);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_query_set_data_buffer(
        ctx_, query, "d2", buffer_coords_dim2, &buffer_coords_dim2_size);
    CHECK(rc == TILEDB_OK);
  }

  // Submit query
  rc = submit_query_wrapper(
      ctx_,
      array_name,
      &query,
      server_buffers_,
      serialize_,
      refactored_query_v2_);
  CHECK(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_COMPLETED);

  // Check buffers
  CHECK(sizeof(c_buffer_a1) == buffer_a1_size);
  CHECK(sizeof(c_buffer_a2_off) == buffer_a2_off_size);
  CHECK(sizeof(c_buffer_a2_val) - 1 == buffer_a2_val_size);
  CHECK(sizeof(c_buffer_a3) == buffer_a3_size);
  CHECK(!memcmp(buffer_a1, c_buffer_a1, sizeof(c_buffer_a1)));
  CHECK(!memcmp(buffer_a2_off, c_buffer_a2_off, sizeof(c_buffer_a2_off)));
  CHECK(!memcmp(buffer_a2_val, c_buffer_a2_val, sizeof(c_buffer_a2_val) - 1));
  CHECK(!memcmp(buffer_a3, c_buffer_a3, sizeof(c_buffer_a3)));

  if (split_coords) {
    CHECK(sizeof(c_buffer_d1) == buffer_d1_size);
    CHECK(!memcmp(buffer_d1, c_buffer_d1, sizeof(c_buffer_d1)));
    CHECK(sizeof(c_buffer_d2) == buffer_d2_size);
    CHECK(!memcmp(buffer_d2, c_buffer_d2, sizeof(c_buffer_d2)));
  } else {
    CHECK(sizeof(c_buffer_coords_dim1) == buffer_coords_dim1_size);
    CHECK(!memcmp(
        buffer_coords_dim1,
        c_buffer_coords_dim1,
        sizeof(c_buffer_coords_dim1)));
    CHECK(sizeof(c_buffer_coords_dim2) == buffer_coords_dim2_size);
    CHECK(!memcmp(
        buffer_coords_dim2,
        c_buffer_coords_dim2,
        sizeof(c_buffer_coords_dim2)));
  }

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  free(buffer_a1);
  free(buffer_a2_off);
  free(buffer_a2_val);
  free(buffer_a3);
  free(buffer_coords_dim1);
  free(buffer_coords_dim2);
  free(buffer_d1);
  free(buffer_d2);
}

void DenseArrayFx::read_large_dense_array(
    const std::string& array_name,
    const tiledb_layout_t layout,
    std::vector<TestRange>& ranges,
    std::vector<uint64_t>& a1) {
  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = array_open_wrapper(
      ctx_, TILEDB_READ, (serialize_ && refactored_query_v2_), &array);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, layout);
  CHECK(rc == TILEDB_OK);

  for (auto& range : ranges) {
    rc = tiledb_query_add_range(
        ctx_, query, range.i_, &range.min_, &range.max_, nullptr);
    REQUIRE(rc == TILEDB_OK);
  }

  uint64_t buffer_a1_size = a1.size() * sizeof(uint64_t);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a1", a1.data(), &buffer_a1_size);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = submit_query_wrapper(
      ctx_,
      array_name,
      &query,
      server_buffers_,
      serialize_,
      refactored_query_v2_);
  CHECK(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_COMPLETED);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void DenseArrayFx::check_non_empty_domain(const std::string& path) {
  std::string array_name = path + "dense_non_empty_domain";
  create_dense_array(array_name);

  // Check empty domain
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = array_open_wrapper(
      ctx_, TILEDB_READ, (serialize_ && refactored_query_v2_), &array);
  CHECK(rc == TILEDB_OK);
  int is_empty;
  uint64_t domain[4];
  rc = tiledb_array_get_non_empty_domain(ctx_, array, domain, &is_empty);
  CHECK(rc == TILEDB_OK);
  CHECK(is_empty == 1);
  rc = tiledb_array_get_non_empty_domain_from_index(
      ctx_, array, 0, domain, &is_empty);
  CHECK(rc == TILEDB_OK);
  CHECK(is_empty == 1);
  rc = tiledb_array_get_non_empty_domain_from_name(
      ctx_, array, "d1", domain, &is_empty);
  CHECK(rc == TILEDB_OK);
  CHECK(is_empty == 1);
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Write
  write_partial_dense_array(array_name);

  // Open array
  rc = array_open_wrapper(
      ctx_, TILEDB_READ, (serialize_ && refactored_query_v2_), &array);
  CHECK(rc == TILEDB_OK);

  // Check non-empty domain
  rc = tiledb_array_get_non_empty_domain(ctx_, array, domain, &is_empty);
  CHECK(rc == TILEDB_OK);
  CHECK(is_empty == 0);
  uint64_t c_domain[] = {3, 4, 3, 4};
  CHECK(!memcmp(domain, c_domain, sizeof(c_domain)));

  // Check non-empty domain from index
  rc = tiledb_array_get_non_empty_domain_from_index(
      ctx_, array, 5, domain, &is_empty);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_array_get_non_empty_domain_from_index(
      ctx_, array, 0, domain, &is_empty);
  CHECK(rc == TILEDB_OK);
  CHECK(is_empty == 0);
  uint64_t c_domain_0[] = {3, 4};
  CHECK(!memcmp(domain, c_domain_0, sizeof(c_domain_0)));
  rc = tiledb_array_get_non_empty_domain_from_index(
      ctx_, array, 1, domain, &is_empty);
  CHECK(rc == TILEDB_OK);
  CHECK(is_empty == 0);
  uint64_t c_domain_1[] = {3, 4};
  CHECK(!memcmp(domain, c_domain_1, sizeof(c_domain_1)));

  // Check non-empty domain from name
  rc = tiledb_array_get_non_empty_domain_from_name(
      ctx_, array, "foo", domain, &is_empty);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_array_get_non_empty_domain_from_name(
      ctx_, array, "d1", domain, &is_empty);
  CHECK(rc == TILEDB_OK);
  CHECK(is_empty == 0);
  CHECK(!memcmp(domain, c_domain_0, sizeof(c_domain_0)));
  rc = tiledb_array_get_non_empty_domain_from_name(
      ctx_, array, "d2", domain, &is_empty);
  CHECK(rc == TILEDB_OK);
  CHECK(is_empty == 0);
  CHECK(!memcmp(domain, c_domain_1, sizeof(c_domain_1)));

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
};

std::string DenseArrayFx::random_name(const std::string& prefix) {
  std::stringstream ss;
  ss << prefix << "-" << std::this_thread::get_id() << "-"
     << TILEDB_TIMESTAMP_NOW_MS;
  return ss.str();
}

TEST_CASE_METHOD(
    DenseArrayFx,
    "C API: Test dense array, sorted reads",
    "[capi][dense][sorted_reads][longtest]") {
  SECTION("- No serialization") {
    serialize_ = false;
  }
#ifdef TILEDB_SERIALIZATION
  SECTION("- Serialization") {
    serialize_ = true;
    refactored_query_v2_ = GENERATE(true, false);
  }
#endif

  // TODO: refactor for each supported FS.
  std::string temp_dir = fs_vec_[0]->temp_dir();
  create_temp_dir(temp_dir);
  check_sorted_reads(temp_dir);
  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    DenseArrayFx,
    "C API: Test dense array, invalid number of cells in dense writes",
    "[capi][dense][invalid_cell_num_writes]") {
  SECTION("- No serialization") {
    serialize_ = false;
  }
#ifdef TILEDB_SERIALIZATION
  SECTION("- Serialization") {
    serialize_ = true;
    refactored_query_v2_ = GENERATE(true, false);
  }
#endif
  // TODO: refactor for each supported FS.
  std::string temp_dir = fs_vec_[0]->temp_dir();
  create_temp_dir(temp_dir);
  check_invalid_cell_num_in_dense_writes(temp_dir);
  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    DenseArrayFx,
    "C API: Test dense array, sorted writes",
    "[capi][dense][sorted_writes]") {
  SECTION("- No serialization") {
    serialize_ = false;
  }
#ifdef TILEDB_SERIALIZATION
  SECTION("- Serialization") {
    serialize_ = true;
    refactored_query_v2_ = GENERATE(true, false);
  }
#endif

  // TODO: refactor for each supported FS.
  std::string temp_dir = fs_vec_[0]->temp_dir();
  create_temp_dir(temp_dir);
  check_sorted_writes(temp_dir);
  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    DenseArrayFx,
    "C API: Test dense array, simultaneous writes",
    "[capi][dense][dense-simultaneous-writes]") {
  SECTION("- No serialization") {
    serialize_ = false;
  }
#ifdef TILEDB_SERIALIZATION
  SECTION("- Serialization") {
    serialize_ = true;
    refactored_query_v2_ = GENERATE(true, false);
  }
#endif

  // TODO: refactor for each supported FS.
  std::string temp_dir = fs_vec_[0]->temp_dir();
  create_temp_dir(temp_dir);
  check_simultaneous_writes(temp_dir);
  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    DenseArrayFx,
    "C API: Test dense array, cancel and retry writes",
    "[capi][dense][async][cancel]") {
  SECTION("- No serialization nor outside subarray") {
    serialize_ = false;
    use_external_subarray_ = false;
  }
#ifdef TILEDB_SERIALIZATION
  SECTION("- Serialization, no outside subarray") {
    serialize_ = true;
    use_external_subarray_ = false;
    refactored_query_v2_ = GENERATE(true, false);
  }
#endif

  SECTION("- no serialization, with outside subarray") {
    serialize_ = false;
    use_external_subarray_ = true;
  }
#ifdef TILEDB_SERIALIZATION
  SECTION("- Serialization, with outside subarray") {
    serialize_ = true;
    use_external_subarray_ = true;
    refactored_query_v2_ = GENERATE(true, false);
  }
#endif

  // TODO: refactor for each supported FS.
  std::string temp_dir = fs_vec_[0]->temp_dir();
  create_temp_dir(temp_dir);
  check_cancel_and_retry_writes(temp_dir);
  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    DenseArrayFx,
    "C API: Test dense array, return coordinates",
    "[capi][dense][return-coords]") {
  bool split_coords = false;

  SECTION("- No serialization") {
    serialize_ = false;

    SECTION("-- zipped coordinates") {
      split_coords = false;
    }

    SECTION("-- split coordinates") {
      split_coords = true;
    }
  }
#ifdef TILEDB_SERIALIZATION
  SECTION("- Serialization") {
    serialize_ = true;
    refactored_query_v2_ = GENERATE(true, false);

    SECTION("-- zipped coordinates") {
      split_coords = false;
    }

    SECTION("-- split coordinates") {
      split_coords = true;
    }
  }
#endif

  // TODO: refactor for each supported FS.
  std::string temp_dir = fs_vec_[0]->temp_dir();
  create_temp_dir(temp_dir);
  check_return_coords(temp_dir, split_coords);
  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    DenseArrayFx,
    "C API: Test dense array, non-empty domain",
    "[capi][dense][dense-non-empty]") {
  SECTION("- No serialization") {
    serialize_ = false;
  }
#ifdef TILEDB_SERIALIZATION
  SECTION("- Serialization") {
    serialize_ = true;
    refactored_query_v2_ = GENERATE(true, false);
  }
#endif

  SupportedFsLocal local_fs;

  std::string temp_dir = local_fs.file_prefix() + local_fs.temp_dir();
  create_temp_dir(temp_dir);
  check_non_empty_domain(temp_dir);
  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    DenseArrayFx,
    "C API: Test dense array, invalid set query buffer",
    "[capi][dense][invalid_set_query_buffer]") {
  SECTION("- No serialization") {
    serialize_ = false;
  }
#ifdef TILEDB_SERIALIZATION
  SECTION("- Serialization") {
    serialize_ = true;
    refactored_query_v2_ = GENERATE(true, false);
  }
#endif

  SupportedFsLocal local_fs;
  std::string temp_dir = local_fs.file_prefix() + local_fs.temp_dir();
  create_temp_dir(temp_dir);

  // Create and write dense array
  std::string array_name = temp_dir + "dense_invalid_set_query_buffer";
  create_dense_array(array_name);
  write_dense_array(array_name);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = array_open_wrapper(
      ctx_, TILEDB_READ, (serialize_ && refactored_query_v2_), &array);
  CHECK(rc == TILEDB_OK);

  // Allocate query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  uint64_t subarray[] = {1, 4, 1, 4};
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  // Aux variables
  uint64_t off[1];
  uint64_t off_size;
  int a1[1];
  uint64_t a1_size = sizeof(a1);

  // Check invalid attribute
  rc = tiledb_query_set_data_buffer(ctx_, query, "foo", a1, &a1_size);
  CHECK(rc == TILEDB_ERR);

  // Check invalid attribute
  rc = tiledb_query_set_data_buffer(ctx_, query, "foo", a1, &a1_size);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_query_set_offsets_buffer(ctx_, query, "foo", off, &off_size);
  CHECK(rc == TILEDB_ERR);

  // Check non-var attribute
  rc = tiledb_query_set_offsets_buffer(ctx_, query, "a1", off, &off_size);
  CHECK(rc == TILEDB_ERR);

  // Check no buffers set
  rc = submit_query_wrapper(
      ctx_,
      array_name,
      &query,
      server_buffers_,
      serialize_,
      refactored_query_v2_);
  CHECK(rc == TILEDB_ERR);

  // Issue an incomplete query
  rc = tiledb_query_set_data_buffer(ctx_, query, "a1", a1, &a1_size);
  CHECK(rc == TILEDB_OK);
  // This is an incomplete query, do not finalize
  rc = submit_query_wrapper(
      ctx_,
      array_name,
      &query,
      server_buffers_,
      serialize_,
      refactored_query_v2_,
      false);
  CHECK(rc == TILEDB_OK);

  // Check that setting a new attribute for an incomplete query fails
  rc = tiledb_query_set_data_buffer(ctx_, query, "a2", a1, &a1_size);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_query_set_offsets_buffer(ctx_, query, "a2", off, &off_size);
  CHECK(rc == TILEDB_ERR);

  // But resetting an existing attribute buffer succeeds
  rc = tiledb_query_set_data_buffer(ctx_, query, "a1", a1, &a1_size);
  CHECK(rc == TILEDB_OK);

  // Close array
  if (serialize_ == false) {
    rc = tiledb_array_close(ctx_, array);
    CHECK(rc == TILEDB_OK);
  }

  // Clean up
  tiledb_query_free(&query);
  tiledb_array_free(&array);

  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    DenseArrayFx,
    "C API: Test dense array, open array checks",
    "[capi][dense][open-array-checks]") {
  SupportedFsLocal local_fs;
  std::string temp_dir = local_fs.file_prefix() + local_fs.temp_dir();
  create_temp_dir(temp_dir);

  // Create and write dense array
  std::string array_name = temp_dir + "dense_open_array";
  create_dense_array(array_name);

  // Allocate array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);

  // Query alloc should fail if the array is not open
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_ERR);

  // Open array
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Allocating a query with a different type to the input array should fail
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_ERR);

  // Get query type
  tiledb_query_type_t query_type;
  rc = tiledb_array_get_query_type(ctx_, array, &query_type);
  CHECK(rc == TILEDB_OK);
  CHECK(query_type == TILEDB_WRITE);

  // Reopening the array without closing should fail
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_ERR);

  // Getting the non-empty domain should fail for an array opened for writes
  uint64_t domain[4];
  int is_empty = false;
  rc = tiledb_array_get_non_empty_domain(ctx_, array, domain, &is_empty);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_array_get_non_empty_domain_from_index(
      ctx_, array, 0, domain, &is_empty);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_array_get_non_empty_domain_from_name(
      ctx_, array, "d1", domain, &is_empty);
  CHECK(rc == TILEDB_ERR);

  // Check query type
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  tiledb_query_get_type(ctx_, query, &query_type);
  CHECK(query_type == TILEDB_WRITE);

  // Reopening array with a different query type should make query
  // submission fail
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);
  rc = submit_query_wrapper(
      ctx_,
      array_name,
      &query,
      server_buffers_,
      serialize_,
      refactored_query_v2_);
  CHECK(rc == TILEDB_ERR);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Submitting the query after closing an array should fail
  rc = submit_query_wrapper(
      ctx_,
      array_name,
      &query,
      server_buffers_,
      serialize_,
      refactored_query_v2_);
  CHECK(rc == TILEDB_ERR);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    DenseArrayFx,
    "C API: Test dense array, reopen array checks",
    "[capi][dense][dense-reopen-array-checks]") {
  SupportedFsLocal local_fs;
  std::string temp_dir = local_fs.file_prefix() + local_fs.temp_dir();
  create_temp_dir(temp_dir);

  // Create and write dense array
  std::string array_name = temp_dir + "dense_reopen_array";
  create_dense_array(array_name);
  write_dense_array(array_name);

  // Allocate array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);

  // Re-opening an array that is not open should fail
  rc = tiledb_array_reopen(ctx_, array);
  CHECK(rc == TILEDB_ERR);

  // Get timestamp before writing then next fragment
  auto timestamp = TILEDB_TIMESTAMP_NOW_MS;
  std::this_thread::sleep_for(std::chrono::milliseconds(1));

  // Write something
  write_partial_dense_array(array_name);

  // Open array at a timestamp before the last fragment
  rc = tiledb_array_set_open_timestamp_end(ctx_, array, timestamp);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Prepare buffer
  const uint64_t subarray[] = {3, 3, 4, 4};
  int a1_buffer[1];
  uint64_t a1_buff_size = sizeof(a1_buffer);
  CHECK(rc == TILEDB_OK);

  // Submit a read query
  tiledb_query_t* query_1;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query_1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query_1, "a1", &a1_buffer, &a1_buff_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query_1, &subarray[0]);
  CHECK(rc == TILEDB_OK);
  rc = submit_query_wrapper(
      ctx_,
      array_name,
      &query_1,
      server_buffers_,
      serialize_,
      refactored_query_v2_);
  CHECK(rc == TILEDB_OK);

  // The read query should not see the second fragment
  CHECK(a1_buffer[0] == 13);

  // Reopen the array to see the new fragment
  rc = tiledb_array_set_open_timestamp_end(
      ctx_, array, tiledb::sm::utils::time::timestamp_now_ms());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_reopen(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Submit a new query
  tiledb_query_t* query_2;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query_2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query_2, "a1", &a1_buffer, &a1_buff_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query_2, &subarray[0]);
  CHECK(rc == TILEDB_OK);
  rc = submit_query_wrapper(
      ctx_,
      array_name,
      &query_2,
      server_buffers_,
      serialize_,
      refactored_query_v2_);
  CHECK(rc == TILEDB_OK);

  // The new query see the updated array
  CHECK(a1_buffer[0] == 1);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query_1);
  tiledb_query_free(&query_2);

  // Open the array for writes
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Re-opening arrays for writes should fail
  rc = tiledb_array_reopen(ctx_, array);
  CHECK(rc == TILEDB_ERR);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);

  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    DenseArrayFx,
    "C API: Test dense array, URI ending in a slash",
    "[capi][dense][uri-ending-slash]") {
  SECTION("- No serialization") {
    serialize_ = false;
  }
#ifdef TILEDB_SERIALIZATION
  SECTION("- Serialization") {
    serialize_ = true;
    refactored_query_v2_ = GENERATE(true, false);
  }
#endif

  SupportedFsLocal local_fs;
  std::string temp_dir = local_fs.file_prefix() + local_fs.temp_dir();
  std::string array_name = temp_dir + "with_ending_slash/";
  create_temp_dir(temp_dir);
  create_dense_array(array_name);
  write_dense_array(array_name);
  read_dense_array_with_coords_full_global(array_name, true);
  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    DenseArrayFx,
    "C API: Test dense array, missing attributes in writes",
    "[capi][dense][write-missing-attributes]") {
  SECTION("- No serialization") {
    serialize_ = false;
  }
#ifdef TILEDB_SERIALIZATION
  SECTION("- Serialization") {
    serialize_ = true;
    refactored_query_v2_ = GENERATE(true, false);
  }
#endif

  SupportedFsLocal local_fs;
  std::string temp_dir = local_fs.file_prefix() + local_fs.temp_dir();
  std::string array_name = temp_dir + "dense_write_missing_attributes/";
  create_temp_dir(temp_dir);
  create_dense_array(array_name);
  write_dense_array_missing_attributes(array_name);
  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    DenseArrayFx,
    "C API: Test dense array, read subarrays with empty cells",
    "[capi][dense][read_empty]") {
  SECTION("- No serialization") {
    serialize_ = false;
  }
#ifdef TILEDB_SERIALIZATION
  SECTION("- Serialization") {
    serialize_ = true;
    refactored_query_v2_ = GENERATE(true, false);
  }
#endif

  SupportedFsLocal local_fs;
  std::string temp_dir = local_fs.file_prefix() + local_fs.temp_dir();
  std::string array_name = temp_dir + "dense_read_empty/";
  create_temp_dir(temp_dir);

  create_dense_array_1_attribute(array_name);

  // Write a slice
  const char* attributes[] = {"a1"};
  int write_a1[] = {1, 2, 3, 4};
  uint64_t write_a1_size = sizeof(write_a1);
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[0], write_a1, &write_a1_size);
  CHECK(rc == TILEDB_OK);
  uint64_t subarray[] = {2, 3, 1, 2};
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
  rc = submit_query_wrapper(
      ctx_,
      array_name,
      &query,
      server_buffers_,
      serialize_,
      refactored_query_v2_);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  // Read whole array
  uint64_t subarray_read[] = {1, 4, 1, 4};
  int c_a1[] = {
      INT_MIN,
      INT_MIN,
      INT_MIN,
      INT_MIN,
      1,
      2,
      INT_MIN,
      INT_MIN,
      3,
      4,
      INT_MIN,
      INT_MIN,
      INT_MIN,
      INT_MIN,
      INT_MIN,
      INT_MIN};
  int read_a1[16];
  uint64_t read_a1_size = sizeof(read_a1);
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray_read);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[0], read_a1, &read_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = submit_query_wrapper(
      ctx_,
      array_name,
      &query,
      server_buffers_,
      serialize_,
      refactored_query_v2_);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  CHECK(!memcmp(c_a1, read_a1, sizeof(c_a1)));

  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    DenseArrayFx,
    "C API: Test dense array, read subarrays with empty areas, merging "
    "adjacent cell ranges",
    "[capi][dense][dense-read-empty][dense-read-empty-merge]") {
  SECTION("- No serialization") {
    serialize_ = false;
  }
#ifdef TILEDB_SERIALIZATION
  SECTION("- Serialization") {
    serialize_ = true;
    refactored_query_v2_ = GENERATE(true, false);
  }
#endif

  SupportedFsLocal local_fs;
  std::string temp_dir = local_fs.file_prefix() + local_fs.temp_dir();
  std::string array_name = temp_dir + "dense_read_empty_merge/";
  create_temp_dir(temp_dir);

  create_dense_array_1_attribute(array_name);

  // Write a slice
  const char* attributes[] = {"a1"};
  int write_a1[] = {1, 2, 3, 4};
  uint64_t write_a1_size = sizeof(write_a1);
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[0], write_a1, &write_a1_size);
  CHECK(rc == TILEDB_OK);
  uint64_t subarray[] = {2, 3, 2, 3};
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
  rc = submit_query_wrapper(
      ctx_,
      array_name,
      &query,
      server_buffers_,
      serialize_,
      refactored_query_v2_);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  // Read whole array
  uint64_t subarray_read[] = {1, 4, 1, 4};
  int c_a1[] = {
      INT_MIN,
      INT_MIN,
      INT_MIN,
      INT_MIN,
      INT_MIN,
      1,
      2,
      INT_MIN,
      INT_MIN,
      3,
      4,
      INT_MIN,
      INT_MIN,
      INT_MIN,
      INT_MIN,
      INT_MIN};
  int read_a1[16];
  uint64_t read_a1_size = sizeof(read_a1);
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray_read);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[0], read_a1, &read_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = submit_query_wrapper(
      ctx_,
      array_name,
      &query,
      server_buffers_,
      serialize_,
      refactored_query_v2_);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  CHECK(!memcmp(c_a1, read_a1, sizeof(c_a1)));

  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    DenseArrayFx,
    "C API: Test dense array, multi-fragment reads",
    "[capi][dense][dense-multi-fragment]") {
  SECTION("- No serialization") {
    serialize_ = false;
  }
#ifdef TILEDB_SERIALIZATION
  SECTION("- Serialization") {
    serialize_ = true;
    refactored_query_v2_ = GENERATE(true, false);
  }
#endif

  SupportedFsLocal local_fs;
  std::string temp_dir = local_fs.file_prefix() + local_fs.temp_dir();
  std::string array_name = temp_dir + "dense_multi_fragment/";
  create_temp_dir(temp_dir);

  create_dense_array_1_attribute(array_name);

  // Write slice [1,2], [3,4]
  int write_a1[] = {1, 2, 3, 4, 5, 6, 7, 8};
  uint64_t write_a1_size = sizeof(write_a1);
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc =
      tiledb_query_set_data_buffer(ctx_, query, "a1", write_a1, &write_a1_size);
  CHECK(rc == TILEDB_OK);
  uint64_t subarray_1[] = {1, 2, 1, 4};
  rc = tiledb_query_set_subarray(ctx_, query, subarray_1);
  CHECK(rc == TILEDB_OK);
  rc = submit_query_wrapper(
      ctx_,
      array_name,
      &query,
      server_buffers_,
      serialize_,
      refactored_query_v2_);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  // Write slice [2,3], [2,3]
  int write_a2[] = {101, 102, 103, 104};
  uint64_t write_a2_size = sizeof(write_a2);
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc =
      tiledb_query_set_data_buffer(ctx_, query, "a1", write_a2, &write_a2_size);
  CHECK(rc == TILEDB_OK);
  uint64_t subarray_2[] = {2, 3, 2, 3};
  rc = tiledb_query_set_subarray(ctx_, query, subarray_2);
  CHECK(rc == TILEDB_OK);
  rc = submit_query_wrapper(
      ctx_,
      array_name,
      &query,
      server_buffers_,
      serialize_,
      refactored_query_v2_);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  // Read whole array
  uint64_t subarray_read[] = {1, 4, 1, 4};
  int c_a[] = {
      1,
      2,
      3,
      4,
      5,
      101,
      102,
      8,
      INT_MIN,
      103,
      104,
      INT_MIN,
      INT_MIN,
      INT_MIN,
      INT_MIN,
      INT_MIN};
  int read_a[16];
  uint64_t read_a_size = sizeof(read_a);
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray_read);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a1", read_a, &read_a_size);
  CHECK(rc == TILEDB_OK);
  rc = submit_query_wrapper(
      ctx_,
      array_name,
      &query,
      server_buffers_,
      serialize_,
      refactored_query_v2_);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  CHECK(!memcmp(c_a, read_a, sizeof(c_a)));

  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    DenseArrayFx,
    "C API: Test dense array, check if open",
    "[capi][dense][dense-is-open]") {
  SECTION("- No serialization") {
    serialize_ = false;
  }
#ifdef TILEDB_SERIALIZATION
  SECTION("- Serialization") {
    serialize_ = true;
    refactored_query_v2_ = GENERATE(true, false);
  }
#endif

  SupportedFsLocal local_fs;
  std::string temp_dir = local_fs.file_prefix() + local_fs.temp_dir();
  std::string array_name = temp_dir + "dense_is_open/";
  create_temp_dir(temp_dir);
  create_dense_array(array_name);

  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);

  int is_open;
  rc = tiledb_array_is_open(ctx_, array, &is_open);
  CHECK(rc == TILEDB_OK);
  CHECK(is_open == 0);

  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_is_open(ctx_, array, &is_open);
  CHECK(rc == TILEDB_OK);
  CHECK(is_open == 1);

  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_is_open(ctx_, array, &is_open);
  CHECK(rc == TILEDB_OK);
  CHECK(is_open == 0);

  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);  // Array is not open, noop
  tiledb_array_free(&array);

  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    DenseArrayFx,
    "C API: Test dense array, get schema from opened array",
    "[capi][dense][dense-get-schema]") {
  SECTION("- No serialization") {
    serialize_ = false;
  }
#ifdef TILEDB_SERIALIZATION
  SECTION("- Serialization") {
    serialize_ = true;
    refactored_query_v2_ = GENERATE(true, false);
  }
#endif

  SupportedFsLocal local_fs;
  std::string temp_dir = local_fs.file_prefix() + local_fs.temp_dir();
  std::string array_name = temp_dir + "dense_get_schema/";
  create_temp_dir(temp_dir);
  create_dense_array(array_name);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Get schema
  tiledb_array_schema_t* schema;
  rc = tiledb_array_get_schema(ctx_, array, &schema);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_check(ctx_, schema);
  CHECK(rc == TILEDB_OK);

  // Clean up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_array_schema_free(&schema);

  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    DenseArrayFx,
    "C API: Test dense array, read in col-major after updates",
    "[capi][dense][dense-col-updates]") {
  SECTION("- No serialization") {
    serialize_ = false;
  }
#ifdef TILEDB_SERIALIZATION
  SECTION("- Serialization") {
    serialize_ = true;
    refactored_query_v2_ = GENERATE(true, false);
  }
#endif

  SupportedFsLocal local_fs;
  std::string array_name =
      local_fs.file_prefix() + local_fs.temp_dir() + "dense-col-updates";
  std::string temp_dir = local_fs.file_prefix() + local_fs.temp_dir();
  create_temp_dir(temp_dir);
  create_dense_array_1_attribute(array_name);

  // ------ WRITE QUERIES ------ //

  // Open array for write query 1
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  int a1[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
  uint64_t a1_size = sizeof(a1);
  tiledb_query_t* wq1;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &wq1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, wq1, "a1", a1, &a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, wq1, TILEDB_GLOBAL_ORDER);
  REQUIRE(rc == TILEDB_OK);

  // Submit query
  rc = submit_query_wrapper(
      ctx_,
      array_name,
      &wq1,
      server_buffers_,
      serialize_,
      refactored_query_v2_);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&wq1);

  // Open array for write query 2
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  int a2[] = {100, 130, 140, 120, 150, 160};
  uint64_t a2_size = sizeof(a2);
  int64_t subarray[] = {3, 4, 2, 4};
  tiledb_query_t* wq2;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &wq2);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, wq2, subarray);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, wq2, "a1", a2, &a2_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, wq2, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = submit_query_wrapper(
      ctx_,
      array_name,
      &wq2,
      server_buffers_,
      serialize_,
      refactored_query_v2_);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx_, wq2);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&wq2);

  // Open array for write query 3
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  int a3[] = {20, 40, 50, 70};
  uint64_t a3_size = sizeof(a3);
  int64_t subarray_2[] = {1, 2, 2, 3};
  tiledb_query_t* wq3;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &wq3);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, wq3, subarray_2);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, wq3, "a1", a3, &a3_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, wq3, TILEDB_COL_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = submit_query_wrapper(
      ctx_,
      array_name,
      &wq3,
      server_buffers_,
      serialize_,
      refactored_query_v2_);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx_, wq3);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&wq3);

  // ------ READ QUERY ------ //

  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  uint64_t subarray_read[] = {1, 4, 1, 4};
  int a[16];
  uint64_t a_size = sizeof(a);
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a1", a, &a_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_COL_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray_read);
  CHECK(rc == TILEDB_OK);
  rc = submit_query_wrapper(
      ctx_,
      array_name,
      &query,
      server_buffers_,
      serialize_,
      refactored_query_v2_);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  int a_c[] = {1, 3, 9, 11, 20, 40, 100, 120, 50, 70, 130, 150, 6, 8, 140, 160};
  CHECK(a_size == sizeof(a_c));
  CHECK(!memcmp(a, a_c, sizeof(a_c)));

  // Clean up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    DenseArrayFx,
    "C API: Test dense array, encrypted",
    "[capi][dense][encryption][longtest]") {
  encryption_type = TILEDB_AES_256_GCM;
  encryption_key = "0123456789abcdeF0123456789abcdeF";

  // TODO: refactor for each supported FS.
  std::string temp_dir = fs_vec_[0]->temp_dir();
  create_temp_dir(temp_dir);
  check_sorted_reads(temp_dir);
  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    DenseArrayFx,
    "C API: Test dense array, splitting to unary ranges",
    "[capi][dense][splitting][unary-range]") {
  SECTION("- No serialization") {
    serialize_ = false;
  }
#ifdef TILEDB_SERIALIZATION
  SECTION("- Serialization") {
    serialize_ = true;
    refactored_query_v2_ = GENERATE(true, false);
  }
#endif

  SupportedFsLocal local_fs;
  std::string temp_dir = local_fs.file_prefix() + local_fs.temp_dir();
  std::string array_name = temp_dir + "unary-range";
  create_temp_dir(temp_dir);

  // Create and write dense array
  create_dense_array(array_name);
  write_dense_array(array_name);

  // Forcing splitting to unary ranges.
  tiledb_config_t* config;
  tiledb_error_t* error;
  CHECK(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  CHECK(error == nullptr);
  REQUIRE(
      tiledb_config_set(config, "sm.memory_budget", "10", &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  REQUIRE(
      tiledb_config_set(
          config, "sm.skip_unary_partitioning_budget_check", "true", &error) ==
      TILEDB_OK);
  REQUIRE(error == nullptr);

  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);

  rc = tiledb_array_set_config(ctx_, array, config);
  CHECK(rc == TILEDB_OK);

  rc = array_open_wrapper(
      ctx_, TILEDB_READ, (serialize_ && refactored_query_v2_), &array);
  CHECK(rc == TILEDB_OK);

  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);

  rc = tiledb_query_set_config(ctx_, query, config);
  CHECK(rc == TILEDB_OK);

  int a1[1];
  uint64_t a1_size = sizeof(a1);

  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);

  int64_t range[] = {0, 4};
  rc = tiledb_query_add_range(ctx_, query, 0, &range[0], &range[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 1, &range[0], &range[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a1", a1, &a1_size);
  CHECK(rc == TILEDB_OK);

  // Read all the data.
  tiledb_query_status_t status;
  int c_a1[] = {0, 1, 4, 5, 2, 3, 6, 7, 8, 9, 12, 13, 10, 11, 14, 15};
  for (uint64_t i = 0; i < 16; i++) {
    rc = submit_query_wrapper(
        ctx_,
        array_name,
        &query,
        server_buffers_,
        serialize_,
        refactored_query_v2_,
        false);
    CHECK(rc == TILEDB_OK);

    rc = tiledb_query_get_status(ctx_, query, &status);
    CHECK(rc == TILEDB_OK);
    CHECK(status == (i == 15 ? TILEDB_COMPLETED : TILEDB_INCOMPLETE));

    CHECK(a1[0] == c_a1[i]);
  }

  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_query_free(&query);
  tiledb_array_free(&array);
  tiledb_config_free(&config);

  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    DenseArrayFx,
    "C API: Test dense array, subarray default dim",
    "[capi][dense][default-dim]") {
  SECTION("- No serialization") {
    serialize_ = false;
  }
#ifdef TILEDB_SERIALIZATION
  SECTION("- Serialization") {
    serialize_ = true;
    refactored_query_v2_ = GENERATE(true, false);
  }
#endif

  SupportedFsLocal local_fs;
  std::string temp_dir = local_fs.file_prefix() + local_fs.temp_dir();
  std::string array_name = temp_dir + "default-dim";
  create_temp_dir(temp_dir);

  // Create and write dense array
  create_dense_array(array_name);
  write_dense_array(array_name);

  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);

  int a1[16];
  uint64_t a1_size = sizeof(a1);

  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);

  int64_t range[] = {0, 4};
  rc = tiledb_query_add_range(ctx_, query, 0, &range[0], &range[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a1", a1, &a1_size);
  CHECK(rc == TILEDB_OK);
  rc = submit_query_wrapper(
      ctx_,
      array_name,
      &query,
      server_buffers_,
      serialize_,
      refactored_query_v2_);
  CHECK(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(rc == TILEDB_OK);
  CHECK(status == TILEDB_COMPLETED);

  int c_a1[] = {0, 1, 4, 5, 2, 3, 6, 7, 8, 9, 12, 13, 10, 11, 14, 15};
  CHECK(!memcmp(a1, c_a1, sizeof(c_a1)));

  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_query_free(&query);
  tiledb_array_free(&array);

  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    DenseArrayFx,
    "C API: Test dense array, overlapping fragments, same tile, cell order "
    "change",
    "[capi][dense][overlapping-fragments][same-tile][cell-order-change]") {
  SECTION("- No serialization") {
    serialize_ = false;
  }
#ifdef TILEDB_SERIALIZATION
  SECTION("- Serialization") {
    serialize_ = true;
    refactored_query_v2_ = GENERATE(true, false);
  }
#endif

  SupportedFsLocal local_fs;
  std::string temp_dir = local_fs.file_prefix() + local_fs.temp_dir();
  std::string array_name = temp_dir + "dense_read_same_tile";
  create_temp_dir(temp_dir);

  create_dense_array_same_tile(array_name);

  // Write a slice
  const char* attributes[] = {"a1", "a2"};
  int write_a1[] = {13, 14, 23, 24, 33, 34};
  uint64_t write_a1_size = sizeof(write_a1);
  int write_a2[] = {13, 14, 23, 24, 33, 34};
  uint64_t write_a2_size = sizeof(write_a2);
  uint64_t write_a2_offs[] = {0, 4, 8, 12, 16, 20};
  uint64_t write_a2_offs_size = sizeof(write_a2_offs);
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[0], write_a1, &write_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[1], write_a2, &write_a2_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, attributes[1], write_a2_offs, &write_a2_offs_size);
  CHECK(rc == TILEDB_OK);
  uint64_t subarray[] = {1, 3, 3, 4};
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
  rc = submit_query_wrapper(
      ctx_,
      array_name,
      &query,
      server_buffers_,
      serialize_,
      refactored_query_v2_);
  CHECK(rc == TILEDB_OK);
  tiledb_query_free(&query);

  // Write another slice
  int write_a1_2[] = {41, 42, 43, 44, 51, 52, 53, 54};
  uint64_t write_a1_size_2 = sizeof(write_a1_2);
  int write_a2_2[] = {41, 42, 43, 44, 51, 52, 53, 54};
  uint64_t write_a2_size_2 = sizeof(write_a2_2);
  uint64_t write_a2_offs_2[] = {0, 4, 8, 12, 16, 20, 24, 28};
  uint64_t write_a2_offs_size_2 = sizeof(write_a2_offs_2);
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[0], write_a1_2, &write_a1_size_2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[1], write_a2_2, &write_a2_size_2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, attributes[1], write_a2_offs_2, &write_a2_offs_size_2);
  CHECK(rc == TILEDB_OK);
  uint64_t subarray_2[] = {4, 5, 1, 4};
  rc = tiledb_query_set_subarray(ctx_, query, subarray_2);
  CHECK(rc == TILEDB_OK);
  rc = submit_query_wrapper(
      ctx_,
      array_name,
      &query,
      server_buffers_,
      serialize_,
      refactored_query_v2_);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);  // Closing twice should be a noop
  tiledb_query_free(&query);

  // Read whole tile
  uint64_t subarray_read[] = {1, 5, 1, 4};
  int c_a1[] = {INT_MIN, INT_MIN, 13,      14, INT_MIN, INT_MIN, 23,
                24,      INT_MIN, INT_MIN, 33, 34,      41,      42,
                43,      44,      51,      52, 53,      54};
  int read_a1[20];
  uint64_t read_a1_size = sizeof(read_a1);
  uint64_t read_a2_offs[20];
  uint64_t read_a2_offs_size = sizeof(read_a2_offs);
  int read_a2[100];
  uint64_t read_a2_size = sizeof(read_a2);
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray_read);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[0], read_a1, &read_a1_size);
  rc = tiledb_query_set_offsets_buffer(
      ctx_, query, attributes[1], read_a2_offs, &read_a2_offs_size);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, attributes[1], read_a2, &read_a2_size);
  CHECK(rc == TILEDB_OK);
  rc = submit_query_wrapper(
      ctx_,
      array_name,
      &query,
      server_buffers_,
      serialize_,
      refactored_query_v2_);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  CHECK(!memcmp(c_a1, read_a1, sizeof(c_a1)));
  CHECK(!memcmp(c_a1, read_a2, sizeof(c_a1)));

  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    DenseArrayFx,
    "C API: Test dense array, simple multi-index",
    "[capi][dense][multi-index][simple]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
  if (serialize_) {
    refactored_query_v2_ = GENERATE(true, false);
  }
#endif

  SupportedFsLocal local_fs;
  std::string temp_dir = local_fs.file_prefix() + local_fs.temp_dir();
  std::string array_name = temp_dir + "dense_read_multi_index_simple";
  create_temp_dir(temp_dir);

  create_large_dense_array_1_attribute(array_name);

  write_large_dense_array(array_name);

  std::vector<TestRange> ranges;
  ranges.emplace_back(0, 2, 4);
  ranges.emplace_back(0, 17, 19);
  ranges.emplace_back(1, 2, 2);
  ranges.emplace_back(1, 14, 14);

  tiledb_layout_t layout = GENERATE(TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);

  std::vector<uint64_t> a1(12);
  read_large_dense_array(array_name, layout, ranges, a1);

  // clang-format off
  uint64_t c_a1_cm[] = {
    202, 302, 402,
    1702, 1802, 1902,
    214, 314, 414,
    1714, 1814, 1914
  };

  uint64_t c_a1_rm[] = {
    202, 214,
    302, 314,
    402, 414,
    1702, 1714,
    1802, 1814,
    1902, 1914
  };
  // clang-format on
  if (layout == TILEDB_ROW_MAJOR) {
    CHECK(!memcmp(c_a1_rm, a1.data(), sizeof(c_a1_rm)));
  } else {
    CHECK(!memcmp(c_a1_cm, a1.data(), sizeof(c_a1_cm)));
  }

  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    DenseArrayFx,
    "C API: Test dense array, complex multi-index",
    "[capi][dense][multi-index][complex]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
  if (serialize_) {
    refactored_query_v2_ = GENERATE(true, false);
  }
#endif

  SupportedFsLocal local_fs;
  std::string temp_dir = local_fs.file_prefix() + local_fs.temp_dir();
  std::string array_name = temp_dir + "dense_read_multi_index_complex";
  create_temp_dir(temp_dir);

  create_large_dense_array_1_attribute(array_name);

  write_large_dense_array(array_name);

  std::vector<TestRange> ranges;
  ranges.emplace_back(0, 2, 4);
  ranges.emplace_back(0, 7, 9);
  ranges.emplace_back(0, 12, 14);
  ranges.emplace_back(0, 17, 19);
  ranges.emplace_back(1, 2, 2);
  ranges.emplace_back(1, 5, 5);
  ranges.emplace_back(1, 8, 8);
  ranges.emplace_back(1, 11, 11);
  ranges.emplace_back(1, 14, 14);

  tiledb_layout_t layout = GENERATE(TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);

  std::vector<uint64_t> a1(60);
  read_large_dense_array(array_name, layout, ranges, a1);

  // clang-format off
  uint64_t c_a1_cm[] = {
    202, 302, 402, 702, 802, 902,
    1202, 1302, 1402, 1702, 1802, 1902,
    205, 305, 405, 705, 805, 905,
    1205, 1305, 1405, 1705, 1805, 1905,
    208, 308, 408, 708, 808, 908,
    1208, 1308, 1408, 1708, 1808, 1908,
    211, 311, 411, 711, 811, 911,
    1211, 1311, 1411, 1711, 1811, 1911,
    214, 314, 414, 714, 814, 914,
    1214, 1314, 1414, 1714, 1814, 1914
  };

  uint64_t c_a1_rm[] = {
    202, 205, 208, 211, 214,
    302, 305, 308, 311, 314,
    402, 405, 408, 411, 414,
    702, 705, 708, 711, 714,
    802, 805, 808, 811, 814,
    902, 905, 908, 911, 914,
    1202, 1205, 1208, 1211, 1214,
    1302, 1305, 1308, 1311, 1314,
    1402, 1405, 1408, 1411, 1414,
    1702, 1705, 1708, 1711, 1714,
    1802, 1805, 1808, 1811, 1814,
    1902, 1905, 1908, 1911, 1914
  };
  // clang-format on
  if (layout == TILEDB_ROW_MAJOR) {
    CHECK(!memcmp(c_a1_rm, a1.data(), sizeof(c_a1_rm)));
  } else {
    CHECK(!memcmp(c_a1_cm, a1.data(), sizeof(c_a1_cm)));
  }

  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    DenseArrayFx,
    "C API: Test dense array, multi-index, cross tile boundary",
    "[capi][dense][multi-index][cross-tile-boundary]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
  if (serialize_) {
    refactored_query_v2_ = GENERATE(true, false);
  }
#endif

  SupportedFsLocal local_fs;
  std::string temp_dir = local_fs.file_prefix() + local_fs.temp_dir();
  std::string array_name = temp_dir + "dense_read_multi_index_cross_tile";
  create_temp_dir(temp_dir);

  create_large_dense_array_1_attribute(array_name);

  write_large_dense_array(array_name);

  std::vector<TestRange> ranges;
  ranges.emplace_back(0, 5, 6);
  ranges.emplace_back(0, 10, 16);
  ranges.emplace_back(1, 3, 4);

  tiledb_layout_t layout = GENERATE(TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);

  std::vector<uint64_t> a1(18);
  read_large_dense_array(array_name, layout, ranges, a1);

  // clang-format off
  uint64_t c_a1_cm[] = {
    503, 603, 1003, 1103, 1203, 1303, 1403, 1503, 1603,
    504, 604, 1004, 1104, 1204, 1304, 1404, 1504, 1604
  };

  uint64_t c_a1_rm[] = {
    503, 504,
    603, 604,
    1003, 1004,
    1103, 1104,
    1203, 1204,
    1303, 1304,
    1403, 1404,
    1503, 1504,
    1603, 1604
  };
  // clang-format on
  if (layout == TILEDB_ROW_MAJOR) {
    CHECK(!memcmp(c_a1_rm, a1.data(), sizeof(c_a1_rm)));
  } else {
    CHECK(!memcmp(c_a1_cm, a1.data(), sizeof(c_a1_cm)));
  }

  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    DenseArrayFx,
    "C API: Test dense array, multi-index, out of order ranges",
    "[capi][dense][multi-index][out-of-order-ranges]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
  if (serialize_) {
    refactored_query_v2_ = GENERATE(true, false);
  }
#endif

  SupportedFsLocal local_fs;
  std::string temp_dir = local_fs.file_prefix() + local_fs.temp_dir();
  std::string array_name = temp_dir + "dense_read_multi_out_of_order";
  create_temp_dir(temp_dir);

  create_large_dense_array_1_attribute(array_name);

  write_large_dense_array(array_name);

  std::vector<TestRange> ranges;
  ranges.emplace_back(0, 10, 16);
  ranges.emplace_back(0, 5, 6);
  ranges.emplace_back(1, 3, 4);

  tiledb_layout_t layout = GENERATE(TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);

  std::vector<uint64_t> a1(18);
  read_large_dense_array(array_name, layout, ranges, a1);

  // clang-format off
  uint64_t c_a1_cm[] = {
    1003, 1103, 1203, 1303, 1403, 1503, 1603, 503, 603,
    1004, 1104, 1204, 1304, 1404, 1504, 1604, 504, 604
  };

  uint64_t c_a1_rm[] = {
    1003, 1004,
    1103, 1104,
    1203, 1204,
    1303, 1304,
    1403, 1404,
    1503, 1504,
    1603, 1604,
    503, 504,
    603, 604
  };
  // clang-format on
  if (layout == TILEDB_ROW_MAJOR) {
    CHECK(!memcmp(c_a1_rm, a1.data(), sizeof(c_a1_rm)));
  } else {
    CHECK(!memcmp(c_a1_cm, a1.data(), sizeof(c_a1_cm)));
  }

  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    DenseArrayFx,
    "C API: Test dense array, multi-index, out of order ranges, coalesce",
    "[capi][dense][multi-index][out-of-order-ranges-2]") {
#ifdef TILEDB_SERIALIZATION
  serialize_ = GENERATE(true, false);
  if (serialize_) {
    refactored_query_v2_ = GENERATE(true, false);
  }
#endif

  SupportedFsLocal local_fs;
  std::string temp_dir = local_fs.file_prefix() + local_fs.temp_dir();
  std::string array_name = temp_dir + "dense_read_multi_index_coalesce";
  create_temp_dir(temp_dir);

  create_large_dense_array_1_attribute(array_name);

  write_large_dense_array(array_name);

  std::vector<TestRange> ranges;
  ranges.emplace_back(0, 1, 1);
  ranges.emplace_back(1, 1, 1);
  ranges.emplace_back(1, 8, 8);
  ranges.emplace_back(1, 2, 2);

  tiledb_layout_t layout = GENERATE(TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);

  std::vector<uint64_t> a1(3);
  read_large_dense_array(array_name, layout, ranges, a1);

  // clang-format off
  uint64_t c_a1[] = {
    101, 108, 102
  };
  // clang-format on

  CHECK(!memcmp(c_a1, a1.data(), sizeof(c_a1)));

  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    TemporaryDirectoryFixture,
    "C API: Test dense array write without setting layout",
    "[capi][query]") {
  // Create the array.
  uint64_t x_tile_extent{4};
  uint64_t domain[2]{0, 3};
  auto array_schema = create_array_schema(
      ctx,
      TILEDB_DENSE,
      {"x"},
      {TILEDB_UINT64},
      {&domain[0]},
      {&x_tile_extent},
      {"a"},
      {TILEDB_FLOAT64},
      {1},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      4096,
      false);
  auto array_name = create_temporary_array("dense_array_1", array_schema);
  tiledb_array_schema_free(&array_schema);

  // Open array for writing.
  tiledb_array_t* array;
  require_tiledb_ok(tiledb_array_alloc(ctx, array_name.c_str(), &array));
  require_tiledb_ok(tiledb_array_open(ctx, array, TILEDB_WRITE));

  // Create subarray.
  tiledb_subarray_t* subarray;
  require_tiledb_ok(tiledb_subarray_alloc(ctx, array, &subarray));
  require_tiledb_ok(tiledb_subarray_add_range(
      ctx, subarray, 0, &domain[0], &domain[1], nullptr));

  // Define data for writing.
  std::vector<double> input_attr_data{0.5, 1.0, 1.5, 2.0};
  uint64_t attr_data_size{input_attr_data.size() * sizeof(double)};

  // Create write query.
  tiledb_query_t* query;
  require_tiledb_ok(tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query));
  require_tiledb_ok(tiledb_query_set_subarray_t(ctx, query, subarray));
  if (attr_data_size != 0) {
    require_tiledb_ok(tiledb_query_set_data_buffer(
        ctx, query, "a", input_attr_data.data(), &attr_data_size));
  }

  // Submit write query.
  require_tiledb_ok(tiledb_query_submit(ctx, query));
  tiledb_query_status_t query_status;
  require_tiledb_ok(tiledb_query_get_status(ctx, query, &query_status));
  REQUIRE(query_status == TILEDB_COMPLETED);

  // Clean-up.
  tiledb_subarray_free(&subarray);
  tiledb_query_free(&query);
  tiledb_array_free(&array);
}
