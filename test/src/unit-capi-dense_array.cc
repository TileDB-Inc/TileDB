/**
 * @file   unit-capi-dense_array.cc
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
 * Tests of C API for dense array operations.
 */

#include "catch.hpp"
#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/misc/utils.h"

#include <array>
#include <cassert>
#include <cstring>
#include <ctime>
#include <iostream>
#include <map>
#include <sstream>
#include <thread>

struct DenseArrayFx {
  // Constant parameters
  const char* ATTR_NAME = "a";
  const tiledb_datatype_t ATTR_TYPE = TILEDB_INT32;
  const char* DIM1_NAME = "x";
  const char* DIM2_NAME = "y";
  const tiledb_datatype_t DIM_TYPE = TILEDB_INT64;
  const std::string HDFS_TEMP_DIR = "hdfs:///tiledb_test/";
  const std::string S3_PREFIX = "s3://";
  const std::string S3_BUCKET = S3_PREFIX + random_bucket_name("tiledb") + "/";
  const std::string S3_TEMP_DIR = S3_BUCKET + "tiledb_test/";
#ifdef _WIN32
  const std::string FILE_URI_PREFIX = "";
  const std::string FILE_TEMP_DIR =
      tiledb::sm::Win::current_dir() + "\\tiledb_test\\";
#else
  const std::string FILE_URI_PREFIX = "file://";
  const std::string FILE_TEMP_DIR =
      tiledb::sm::Posix::current_dir() + "/tiledb_test/";
#endif
  const int ITER_NUM = 10;

  // TileDB context and VFS
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;

  // Supported filesystems
  bool supports_s3_;
  bool supports_hdfs_;

  // Functions
  DenseArrayFx();
  ~DenseArrayFx();
  void set_supported_fs();
  void create_temp_dir(const std::string& path);
  void remove_temp_dir(const std::string& path);
  void check_sorted_reads(const std::string& path);
  void check_sorted_writes(const std::string& path);
  void check_invalid_cell_num_in_dense_writes(const std::string& path);
  void check_sparse_writes(const std::string& path);
  void check_simultaneous_writes(const std::string& path);
  void check_cancel_and_retry_writes(const std::string& path);
  void check_return_coords(const std::string& path);
  void check_subarray_partitions(const std::string& path);
  void check_subarray_partitions_2_row(const std::string& array_name);
  void check_subarray_partitions_2_col(const std::string& array_name);
  void check_subarray_partitions_0(const std::string& array_name);
  void check_non_empty_domain(const std::string& path);
  void create_dense_array(const std::string& array_name);
  void write_dense_array(const std::string& array_name);
  void write_partial_dense_array(const std::string& array_name);
  void read_dense_array_with_coords_full_global(const std::string& array_name);
  void read_dense_array_with_coords_full_row(const std::string& array_name);
  void read_dense_array_with_coords_full_col(const std::string& array_name);
  void read_dense_array_with_coords_subarray_global(
      const std::string& array_name);
  void read_dense_array_with_coords_subarray_row(const std::string& array_name);
  void read_dense_array_with_coords_subarray_col(const std::string& array_name);
  static std::string random_bucket_name(const std::string& prefix);

  /**
   * Checks two buffers, one before and one after the updates. The updates
   * are given as function inputs and facilitate the check.
   *
   * @param buffer_before The buffer before the updates.
   * @param buffer_after The buffer after the updates.
   * @param buffer_updates_a1 The updated attribute values.
   * @param buffer_updates_coords The coordinates where the updates occurred.
   * @param domain_size_0 The domain size of the first dimension.
   * @param domain_size_1 The domain size of the second dimension.
   * @param update_num The number of updates.
   */
  void check_buffer_after_updates(
      const int* buffer_before,
      const int* buffer_after,
      const int* buffer_updates_a1,
      const int64_t* buffer_updates_coords,
      const int64_t domain_size_0,
      const int64_t domain_size_1,
      const uint64_t update_num);

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
   * Updates random locations in a dense array with the input domain sizes.
   *
   * @param array_name The array name.
   * @param domain_size_0 The domain size of the first dimension.
   * @param domain_size_1 The domain size of the second dimension.
   * @param udpate_num The number of updates to be performed.
   * @param seed The seed for the random generator.
   * @param buffers The buffers to be dispatched to the write command.
   * @param buffer_sizes The buffer sizes to be dispatched to the write command.
   */
  void update_dense_array_2D(
      const std::string& array_name,
      const int64_t domain_size_0,
      const int64_t domain_size_1,
      int64_t update_num,
      int seed,
      void** buffers,
      uint64_t* buffer_sizes);

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

DenseArrayFx::DenseArrayFx() {
  // Supported filesystems
  set_supported_fs();

  // Create TileDB context
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);
  if (supports_s3_) {
#ifndef TILEDB_TESTS_AWS_S3_CONFIG
    REQUIRE(
        tiledb_config_set(
            config, "vfs.s3.endpoint_override", "localhost:9999", &error) ==
        TILEDB_OK);
    REQUIRE(
        tiledb_config_set(config, "vfs.s3.scheme", "http", &error) ==
        TILEDB_OK);
    REQUIRE(
        tiledb_config_set(
            config, "vfs.s3.use_virtual_addressing", "false", &error) ==
        TILEDB_OK);
    REQUIRE(error == nullptr);
#endif
  }
  REQUIRE(tiledb_ctx_alloc(&ctx_, config) == TILEDB_OK);
  REQUIRE(error == nullptr);
  vfs_ = nullptr;
  REQUIRE(tiledb_vfs_alloc(ctx_, &vfs_, config) == TILEDB_OK);
  tiledb_config_free(&config);

  // Connect to S3
  if (supports_s3_) {
    // Create bucket if it does not exist
    int is_bucket = 0;
    int rc = tiledb_vfs_is_bucket(ctx_, vfs_, S3_BUCKET.c_str(), &is_bucket);
    REQUIRE(rc == TILEDB_OK);
    if (!is_bucket) {
      rc = tiledb_vfs_create_bucket(ctx_, vfs_, S3_BUCKET.c_str());
      REQUIRE(rc == TILEDB_OK);
    }
  }

  std::srand(0);
}

DenseArrayFx::~DenseArrayFx() {
  if (supports_s3_) {
    int is_bucket = 0;
    int rc = tiledb_vfs_is_bucket(ctx_, vfs_, S3_BUCKET.c_str(), &is_bucket);
    CHECK(rc == TILEDB_OK);
    if (is_bucket) {
      CHECK(
          tiledb_vfs_remove_bucket(ctx_, vfs_, S3_BUCKET.c_str()) == TILEDB_OK);
    }
  }
  tiledb_vfs_free(&vfs_);
  CHECK(vfs_ == nullptr);
  tiledb_ctx_free(&ctx_);
  CHECK(ctx_ == nullptr);
}

void DenseArrayFx::set_supported_fs() {
  tiledb_ctx_t* ctx = nullptr;
  REQUIRE(tiledb_ctx_alloc(&ctx, nullptr) == TILEDB_OK);

  int is_supported = 0;
  int rc = tiledb_ctx_is_supported_fs(ctx, TILEDB_S3, &is_supported);
  REQUIRE(rc == TILEDB_OK);
  supports_s3_ = (bool)is_supported;
  rc = tiledb_ctx_is_supported_fs(ctx, TILEDB_HDFS, &is_supported);
  REQUIRE(rc == TILEDB_OK);
  supports_hdfs_ = (bool)is_supported;

  tiledb_ctx_free(&ctx);
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

void DenseArrayFx::check_buffer_after_updates(
    const int* buffer_before,
    const int* buffer_after,
    const int* buffer_updates_a1,
    const int64_t* buffer_updates_coords,
    const int64_t domain_size_0,
    const int64_t domain_size_1,
    const uint64_t update_num) {
  // Initializations
  int l, r;
  uint64_t cell_num = (uint64_t)domain_size_0 * domain_size_1;

  // Check the contents of the buffers cell by cell
  for (uint64_t i = 0; i < cell_num; ++i) {
    l = buffer_before[i];
    r = buffer_after[i];

    // If they are not the same, check if it is due to an update
    if (l != r) {
      bool found = false;
      for (uint64_t k = 0; k < update_num; ++k) {
        // The difference is due to an update
        if (r == buffer_updates_a1[k] &&
            (l / domain_size_1) == buffer_updates_coords[2 * k] &&
            (l % domain_size_1) == buffer_updates_coords[2 * k + 1]) {
          found = true;
          break;
        }
      }

      // The difference is not due to an update
      REQUIRE(found);
    }
  }
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
  int rc = tiledb_attribute_alloc(ctx_, &a, ATTR_NAME, ATTR_TYPE);
  REQUIRE(rc == TILEDB_OK);

  // Create dimensions
  int64_t dim_domain[] = {domain_0_lo, domain_0_hi, domain_1_lo, domain_1_hi};
  tiledb_dimension_t* d1;
  rc = tiledb_dimension_alloc(
      ctx_, &d1, DIM1_NAME, TILEDB_INT64, &dim_domain[0], &tile_extent_0);
  REQUIRE(rc == TILEDB_OK);
  tiledb_dimension_t* d2;
  rc = tiledb_dimension_alloc(
      ctx_, &d2, DIM2_NAME, TILEDB_INT64, &dim_domain[2], &tile_extent_1);
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
  rc = tiledb_array_schema_alloc(ctx_, &array_schema, TILEDB_DENSE);
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
  rc = tiledb_array_open(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, &query, array, query_type);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffers(
      ctx_, query, attributes, 1, buffers, buffer_sizes);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, query_layout);
  REQUIRE(rc == TILEDB_OK);

  // Read from array
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx_, query);
  REQUIRE(rc == TILEDB_OK);
  rc =
      tiledb_query_finalize(ctx_, query);  // Second time must create no problem
  REQUIRE(rc == TILEDB_OK);

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

void DenseArrayFx::update_dense_array_2D(
    const std::string& array_name,
    const int64_t domain_size_0,
    const int64_t domain_size_1,
    int64_t update_num,
    int seed,
    void** buffers,
    uint64_t* buffer_sizes) {
  // Error code
  int rc;

  // For easy reference
  auto buffer_a1 = (int*)buffers[0];
  auto buffer_coords = (int64_t*)buffers[1];

  // Specify attributes to be written
  const char* attributes[] = {ATTR_NAME, TILEDB_COORDS};

  // Populate buffers with random updates
  std::srand(seed);
  int64_t x, y, v;
  int64_t coords_index = 0L;
  std::map<std::string, int> my_map;
  std::map<std::string, int>::iterator it;
  my_map.clear();
  for (int64_t i = 0; i < update_num; ++i) {
    std::ostringstream rand_stream;
    do {
      std::ostringstream rand_stream;
      x = std::rand() % domain_size_0;
      y = std::rand() % domain_size_1;
      v = std::rand();
      rand_stream << x << "," << y;
      it = my_map.find(rand_stream.str());
    } while (it != my_map.end());
    rand_stream << x << "," << y;
    my_map[rand_stream.str()] = v;
    buffer_coords[coords_index++] = x;
    buffer_coords[coords_index++] = y;
    buffer_a1[i] = v;
  }

  // Open array
  tiledb_array_t* array;
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, &query, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffers(
      ctx_, query, attributes, 2, buffers, buffer_sizes);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  REQUIRE(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  CHECK(array == nullptr);
  tiledb_query_free(&query);
  CHECK(query == nullptr);
}

void DenseArrayFx::write_dense_array_by_tiles(
    const std::string& array_name,
    const int64_t domain_size_0,
    const int64_t domain_size_1,
    const int64_t tile_extent_0,
    const int64_t tile_extent_1) {
  // Error code
  int rc;

  // Other initializations
  auto buffer = generate_2D_buffer(domain_size_0, domain_size_1);
  int64_t cell_num_in_tile = tile_extent_0 * tile_extent_1;
  auto buffer_a1 = new int[cell_num_in_tile];
  for (int64_t i = 0; i < cell_num_in_tile; ++i)
    buffer_a1[i] = 0;
  void* buffers[2];
  buffers[0] = buffer_a1;
  uint64_t buffer_sizes[2];
  int64_t index = 0L;
  uint64_t buffer_size = 0L;

  const char* attributes[] = {ATTR_NAME};

  // Open array
  tiledb_array_t* array;
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, &query, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffers(
      ctx_, query, attributes, 1, buffers, buffer_sizes);
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
      int64_t k = 0, l = 0;
      for (k = 0; k < tile_rows; ++k) {
        for (l = 0; l < tile_cols; ++l) {
          index = uint64_t(k * tile_cols + l);
          buffer_a1[index] = buffer[uint64_t(i + k)][uint64_t(j + l)];
        }
      }
      buffer_size = k * l * sizeof(int);
      buffer_sizes[0] = {buffer_size};

      //    tiledb_query_reset_buffers(ctx_, query, buffers, buffer_sizes);
      rc = tiledb_query_submit(ctx_, query);
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
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, &query, array, query_type);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffers(
      ctx_, query, attributes, 1, buffers, buffer_sizes);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, query_layout);
  REQUIRE(rc == TILEDB_OK);

  // Submit query
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
  rc = tiledb_array_open(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, &query, array, query_type);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffers(
      ctx_, query, attributes, 1, buffers, buffer_sizes);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, query_layout);
  REQUIRE(rc == TILEDB_OK);

  // Submit the same query several times, some may be duplicates, some may
  // be cancelled, it doesn't matter since it's all the same data being written.
  // TODO: this doesn't trigger the cancelled path very often.
  for (unsigned i = 0; i < num_writes; i++) {
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
  }

  rc = tiledb_query_finalize(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

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
    REQUIRE(buffer != NULL);

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
  rc = tiledb_array_open(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Check out of bounds ubarray
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, &query, array, TILEDB_READ);
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
    REQUIRE(read_buffer != NULL);

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
  rc = tiledb_array_open(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Global order
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, &query, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffers(
      ctx_, query, attributes, 1, buffers, buffer_sizes);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx_, query);
  REQUIRE(rc == TILEDB_ERR);
  tiledb_query_free(&query);

  // Ordered layout
  rc = tiledb_query_alloc(ctx_, &query, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffers(
      ctx_, query, attributes, 1, buffers, buffer_sizes);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_ERR);
  rc = tiledb_query_finalize(ctx_, query);
  REQUIRE(rc == TILEDB_OK);
  tiledb_query_free(&query);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
}

void DenseArrayFx::check_sparse_writes(const std::string& path) {
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
  uint64_t update_num = 100;
  int seed = 7;
  std::string array_name = path + "sparse_writes_array";

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

  // Read the entire array back to memory
  int* before_update = read_dense_array_2D(
      array_name,
      domain_0_lo,
      domain_0_hi,
      domain_1_lo,
      domain_1_hi,
      TILEDB_READ,
      TILEDB_GLOBAL_ORDER);
  REQUIRE(before_update != NULL);

  // Prepare random updates
  auto buffer_a1 = new int[update_num];
  auto buffer_coords = new int64_t[2 * update_num];
  void* buffers[] = {buffer_a1, buffer_coords};
  uint64_t buffer_sizes[2];
  buffer_sizes[0] = update_num * sizeof(int);
  buffer_sizes[1] = 2 * update_num * sizeof(int64_t);

  update_dense_array_2D(
      array_name,
      domain_size_0,
      domain_size_1,
      update_num,
      seed,
      buffers,
      buffer_sizes);

  // Read the entire array back to memory after update
  int* after_update = read_dense_array_2D(
      array_name,
      domain_0_lo,
      domain_0_hi,
      domain_1_lo,
      domain_1_hi,
      TILEDB_READ,
      TILEDB_GLOBAL_ORDER);
  REQUIRE(after_update != NULL);

  // Compare array before and after
  check_buffer_after_updates(
      before_update,
      after_update,
      buffer_a1,
      buffer_coords,
      domain_size_0,
      domain_size_1,
      update_num);

  // Clean up
  delete[] before_update;
  delete[] after_update;
  delete[] buffer_a1;
  delete[] buffer_coords;
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
    subarrays.push_back({{domain_0_lo,
                          domain_0_lo + tile_extent_0 - 1,
                          domain_1_lo,
                          domain_1_lo + tile_extent_1 - 1}});
    buffer_sizes.push_back({{tile_extent_0 * tile_extent_1 * sizeof(int)}});
    buffers.push_back(new int[buffer_sizes.back()[0] / sizeof(int)]);
  }

  // TODO: open an array here

  // Write multiple subarrays in parallel with a shared context.
  for (int i = 0; i < nthreads; i++) {
    threads.emplace_back([&, i]() {
      const int writes_per_thread = 5;
      for (int j = 0; j < writes_per_thread; j++) {
        write_dense_subarray_2D(
            // TODO: pass open array here
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

  // TODO: close the array here

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

  int64_t subarray[] = {domain_0_lo,
                        domain_0_lo + tile_extent_0 - 1,
                        domain_1_lo,
                        domain_1_lo + tile_extent_1 - 1};
  uint64_t buffer_sizes[] = {tile_extent_0 * tile_extent_1 * sizeof(int)};
  auto buffer = new int[buffer_sizes[0] / sizeof(int)];

  // Prepare buffer
  int64_t subarray_length[2] = {subarray[1] - subarray[0] + 1,
                                subarray[3] - subarray[2] + 1};
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
  REQUIRE(read_buffer != NULL);

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
      ctx_, &d1, "d1", TILEDB_UINT64, &dim_domain[0], &tile_extents[0]);
  CHECK(rc == TILEDB_OK);
  tiledb_dimension_t* d2;
  rc = tiledb_dimension_alloc(
      ctx_, &d2, "d2", TILEDB_UINT64, &dim_domain[2], &tile_extents[1]);
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
  rc = tiledb_attribute_alloc(ctx_, &a1, "a1", TILEDB_INT32);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_compressor(ctx_, a1, TILEDB_BLOSC_LZ, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a1, 1);
  CHECK(rc == TILEDB_OK);
  tiledb_attribute_t* a2;
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_alloc(ctx_, &a2, "a2", TILEDB_CHAR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_compressor(ctx_, a2, TILEDB_GZIP, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a2, TILEDB_VAR_NUM);
  CHECK(rc == TILEDB_OK);
  tiledb_attribute_t* a3;
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_alloc(ctx_, &a3, "a3", TILEDB_FLOAT32);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_compressor(ctx_, a3, TILEDB_ZSTD, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a3, 2);
  CHECK(rc == TILEDB_OK);

  // Create array schema
  tiledb_array_schema_t* array_schema;
  rc = tiledb_array_schema_alloc(ctx_, &array_schema, TILEDB_DENSE);
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

void DenseArrayFx::check_return_coords(const std::string& path) {
  std::string array_name = path + "return_coords";
  create_dense_array(array_name);
  write_dense_array(array_name);
  read_dense_array_with_coords_full_global(array_name);
  read_dense_array_with_coords_full_row(array_name);
  read_dense_array_with_coords_full_col(array_name);
  read_dense_array_with_coords_subarray_global(array_name);
  read_dense_array_with_coords_subarray_row(array_name);
  read_dense_array_with_coords_subarray_col(array_name);
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
  rc = tiledb_array_open(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, &query, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffers(
      ctx_, query, attributes, 3, buffers, buffer_sizes);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

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
  rc = tiledb_array_open(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, &query, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffers(
      ctx_, query, attributes, 3, buffers, buffer_sizes);
  CHECK(rc == TILEDB_OK);
  uint64_t subarray[] = {3, 4, 3, 4};
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void DenseArrayFx::read_dense_array_with_coords_full_global(
    const std::string& array_name) {
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
  uint64_t c_buffer_coords[] = {1, 1, 1, 2, 2, 1, 2, 2, 1, 3, 1, 4, 2, 3, 2, 4,
                                3, 1, 3, 2, 4, 1, 4, 2, 3, 3, 3, 4, 4, 3, 4, 4};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Compute max buffer sizes
  const char* attributes[] = {"a1", "a2", "a3", TILEDB_COORDS};
  uint64_t max_buffer_sizes[5];
  uint64_t subarray[] = {1, 4, 1, 4};
  rc = tiledb_array_compute_max_read_buffer_sizes(
      ctx_, array, subarray, attributes, 4, &max_buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);

  // Prepare cell buffers
  auto buffer_a1 = (int*)malloc(max_buffer_sizes[0]);
  auto buffer_a2 = (uint64_t*)malloc(max_buffer_sizes[1]);
  auto buffer_var_a2 = (char*)malloc(max_buffer_sizes[2]);
  auto buffer_a3 = (float*)malloc(max_buffer_sizes[3]);
  auto buffer_coords = (uint64_t*)malloc(max_buffer_sizes[4]);
  void* buffers[] = {
      buffer_a1, buffer_a2, buffer_var_a2, buffer_a3, buffer_coords};
  uint64_t buffer_sizes[] = {max_buffer_sizes[0],
                             max_buffer_sizes[1],
                             max_buffer_sizes[2],
                             max_buffer_sizes[3],
                             max_buffer_sizes[4]};

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, &query, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffers(
      ctx_, query, attributes, 4, buffers, buffer_sizes);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_COMPLETED);

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Check buffers
  CHECK(sizeof(c_buffer_a1) <= max_buffer_sizes[0]);
  CHECK(sizeof(c_buffer_a2) <= max_buffer_sizes[1]);
  CHECK(sizeof(c_buffer_var_a2) <= max_buffer_sizes[2]);
  CHECK(sizeof(c_buffer_a3) <= max_buffer_sizes[3]);
  CHECK(sizeof(c_buffer_coords) <= max_buffer_sizes[4]);
  CHECK(!memcmp(buffer_a1, c_buffer_a1, sizeof(c_buffer_a1)));
  CHECK(!memcmp(buffer_a2, c_buffer_a2, sizeof(c_buffer_a2)));
  CHECK(!memcmp(buffer_var_a2, c_buffer_var_a2, sizeof(c_buffer_var_a2) - 1));
  CHECK(!memcmp(buffer_a3, c_buffer_a3, sizeof(c_buffer_a3)));
  CHECK(!memcmp(buffer_coords, c_buffer_coords, sizeof(c_buffer_coords)));

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  free(buffer_a1);
  free(buffer_a2);
  free(buffer_var_a2);
  free(buffer_a3);
  free(buffer_coords);
}

void DenseArrayFx::read_dense_array_with_coords_full_row(
    const std::string& array_name) {
  // Correct buffers
  int c_buffer_a1[] = {0, 1, 4, 5, 2, 3, 6, 7, 8, 9, 12, 13, 10, 11, 14, 15};
  uint64_t c_buffer_a2[] = {
      0, 1, 3, 4, 6, 9, 13, 16, 20, 21, 23, 24, 26, 29, 33, 36};
  char c_buffer_var_a2[] =
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
  uint64_t c_buffer_coords[] = {1, 1, 1, 2, 1, 3, 1, 4, 2, 1, 2, 2, 2, 3, 2, 4,
                                3, 1, 3, 2, 3, 3, 3, 4, 4, 1, 4, 2, 4, 3, 4, 4};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Compute max buffer sizes
  const char* attributes[] = {"a1", "a2", "a3", TILEDB_COORDS};
  uint64_t max_buffer_sizes[5];
  uint64_t subarray[] = {1, 4, 1, 4};
  rc = tiledb_array_compute_max_read_buffer_sizes(
      ctx_, array, subarray, attributes, 4, &max_buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);

  // Prepare cell buffers
  auto buffer_a1 = (int*)malloc(max_buffer_sizes[0]);
  auto buffer_a2 = (uint64_t*)malloc(max_buffer_sizes[1]);
  auto buffer_var_a2 = (char*)malloc(max_buffer_sizes[2]);
  auto buffer_a3 = (float*)malloc(max_buffer_sizes[3]);
  auto buffer_coords = (uint64_t*)malloc(max_buffer_sizes[4]);
  void* buffers[] = {
      buffer_a1, buffer_a2, buffer_var_a2, buffer_a3, buffer_coords};
  uint64_t buffer_sizes[] = {max_buffer_sizes[0],
                             max_buffer_sizes[1],
                             max_buffer_sizes[2],
                             max_buffer_sizes[3],
                             max_buffer_sizes[4]};

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, &query, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffers(
      ctx_, query, attributes, 4, buffers, buffer_sizes);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_COMPLETED);

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Check buffers
  CHECK(sizeof(c_buffer_a1) <= max_buffer_sizes[0]);
  CHECK(sizeof(c_buffer_a2) <= max_buffer_sizes[1]);
  CHECK(sizeof(c_buffer_var_a2) <= max_buffer_sizes[2]);
  CHECK(sizeof(c_buffer_a3) <= max_buffer_sizes[3]);
  CHECK(sizeof(c_buffer_coords) <= max_buffer_sizes[4]);
  CHECK(!memcmp(buffer_a1, c_buffer_a1, sizeof(c_buffer_a1)));
  CHECK(!memcmp(buffer_a2, c_buffer_a2, sizeof(c_buffer_a2)));
  CHECK(!memcmp(buffer_var_a2, c_buffer_var_a2, sizeof(c_buffer_var_a2) - 1));
  CHECK(!memcmp(buffer_a3, c_buffer_a3, sizeof(c_buffer_a3)));
  CHECK(!memcmp(buffer_coords, c_buffer_coords, sizeof(c_buffer_coords)));

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  free(buffer_a1);
  free(buffer_a2);
  free(buffer_var_a2);
  free(buffer_a3);
  free(buffer_coords);
}

void DenseArrayFx::read_dense_array_with_coords_full_col(
    const std::string& array_name) {
  // Correct buffers
  int c_buffer_a1[] = {0, 2, 8, 10, 1, 3, 9, 11, 4, 6, 12, 14, 5, 7, 13, 15};
  uint64_t c_buffer_a2[] = {
      0, 1, 4, 5, 8, 10, 14, 16, 20, 21, 24, 25, 28, 30, 34, 36};
  char c_buffer_var_a2[] =
      "acccikkk"
      "bbddddjjllll"
      "egggmooo"
      "ffhhhhnnpppp";
  float c_buffer_a3[] = {
      0.1f,  0.2f,  2.1f, 2.2f,  8.1f,  8.2f, 10.1f, 10.2f, 1.1f,  1.2f,  3.1f,
      3.2f,  9.1f,  9.2f, 11.1f, 11.2f, 4.1f, 4.2f,  6.1f,  6.2f,  12.1f, 12.2f,
      14.1f, 14.2f, 5.1f, 5.2f,  7.1f,  7.2f, 13.1f, 13.2f, 15.1f, 15.2f,
  };
  uint64_t c_buffer_coords[] = {1, 1, 2, 1, 3, 1, 4, 1, 1, 2, 2, 2, 3, 2, 4, 2,
                                1, 3, 2, 3, 3, 3, 4, 3, 1, 4, 2, 4, 3, 4, 4, 4};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Compute max buffer sizes
  const char* attributes[] = {"a1", "a2", "a3", TILEDB_COORDS};
  uint64_t max_buffer_sizes[5];
  uint64_t subarray[] = {1, 4, 1, 4};
  rc = tiledb_array_compute_max_read_buffer_sizes(
      ctx_, array, subarray, attributes, 4, &max_buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);

  // Prepare cell buffers
  auto buffer_a1 = (int*)malloc(max_buffer_sizes[0]);
  auto buffer_a2 = (uint64_t*)malloc(max_buffer_sizes[1]);
  auto buffer_var_a2 = (char*)malloc(max_buffer_sizes[2]);
  auto buffer_a3 = (float*)malloc(max_buffer_sizes[3]);
  auto buffer_coords = (uint64_t*)malloc(max_buffer_sizes[4]);
  void* buffers[] = {
      buffer_a1, buffer_a2, buffer_var_a2, buffer_a3, buffer_coords};
  uint64_t buffer_sizes[] = {max_buffer_sizes[0],
                             max_buffer_sizes[1],
                             max_buffer_sizes[2],
                             max_buffer_sizes[3],
                             max_buffer_sizes[4]};

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, &query, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_COL_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffers(
      ctx_, query, attributes, 4, buffers, buffer_sizes);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_COMPLETED);

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Check buffers
  CHECK(sizeof(c_buffer_a1) <= max_buffer_sizes[0]);
  CHECK(sizeof(c_buffer_a2) <= max_buffer_sizes[1]);
  CHECK(sizeof(c_buffer_var_a2) <= max_buffer_sizes[2]);
  CHECK(sizeof(c_buffer_a3) <= max_buffer_sizes[3]);
  CHECK(sizeof(c_buffer_coords) <= max_buffer_sizes[4]);
  CHECK(!memcmp(buffer_a1, c_buffer_a1, sizeof(c_buffer_a1)));
  CHECK(!memcmp(buffer_a2, c_buffer_a2, sizeof(c_buffer_a2)));
  CHECK(!memcmp(buffer_var_a2, c_buffer_var_a2, sizeof(c_buffer_var_a2) - 1));
  CHECK(!memcmp(buffer_a3, c_buffer_a3, sizeof(c_buffer_a3)));
  CHECK(!memcmp(buffer_coords, c_buffer_coords, sizeof(c_buffer_coords)));

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  free(buffer_a1);
  free(buffer_a2);
  free(buffer_var_a2);
  free(buffer_a3);
  free(buffer_coords);
}

void DenseArrayFx::read_dense_array_with_coords_subarray_global(
    const std::string& array_name) {
  // Correct buffers
  int c_buffer_a1[] = {9, 11, 12, 13, 14, 15};
  uint64_t c_buffer_a2[] = {0, 2, 6, 7, 9, 12};
  char c_buffer_var_a2[] = "jjllllmnnooopppp";
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
  uint64_t c_buffer_coords[] = {3, 2, 4, 2, 3, 3, 3, 4, 4, 3, 4, 4};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Compute max buffer sizes
  const char* attributes[] = {"a1", "a2", "a3", TILEDB_COORDS};
  uint64_t max_buffer_sizes[5];
  uint64_t subarray[] = {3, 4, 2, 4};
  rc = tiledb_array_compute_max_read_buffer_sizes(
      ctx_, array, subarray, attributes, 4, &max_buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);

  // Prepare cell buffers
  auto buffer_a1 = (int*)malloc(max_buffer_sizes[0]);
  auto buffer_a2 = (uint64_t*)malloc(max_buffer_sizes[1]);
  auto buffer_var_a2 = (char*)malloc(max_buffer_sizes[2]);
  auto buffer_a3 = (float*)malloc(max_buffer_sizes[3]);
  auto buffer_coords = (uint64_t*)malloc(max_buffer_sizes[4]);
  void* buffers[] = {
      buffer_a1, buffer_a2, buffer_var_a2, buffer_a3, buffer_coords};
  uint64_t buffer_sizes[] = {max_buffer_sizes[0],
                             max_buffer_sizes[1],
                             max_buffer_sizes[2],
                             max_buffer_sizes[3],
                             max_buffer_sizes[4]};

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, &query, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffers(
      ctx_, query, attributes, 4, buffers, buffer_sizes);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_COMPLETED);

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Check buffers
  CHECK(sizeof(c_buffer_a1) <= max_buffer_sizes[0]);
  CHECK(sizeof(c_buffer_a2) <= max_buffer_sizes[1]);
  CHECK(sizeof(c_buffer_var_a2) <= max_buffer_sizes[2]);
  CHECK(sizeof(c_buffer_a3) <= max_buffer_sizes[3]);
  CHECK(sizeof(c_buffer_coords) <= max_buffer_sizes[4]);
  CHECK(!memcmp(buffer_a1, c_buffer_a1, sizeof(c_buffer_a1)));
  CHECK(!memcmp(buffer_a2, c_buffer_a2, sizeof(c_buffer_a2)));
  CHECK(!memcmp(buffer_var_a2, c_buffer_var_a2, sizeof(c_buffer_var_a2) - 1));
  CHECK(!memcmp(buffer_a3, c_buffer_a3, sizeof(c_buffer_a3)));
  CHECK(!memcmp(buffer_coords, c_buffer_coords, sizeof(c_buffer_coords)));

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  free(buffer_a1);
  free(buffer_a2);
  free(buffer_var_a2);
  free(buffer_a3);
  free(buffer_coords);
}

void DenseArrayFx::read_dense_array_with_coords_subarray_row(
    const std::string& array_name) {
  // Correct buffers
  int c_buffer_a1[] = {9, 12, 13, 11, 14, 15};
  uint64_t c_buffer_a2[] = {0, 2, 3, 5, 9, 12};
  char c_buffer_var_a2[] = "jjmnnllllooopppp";
  float c_buffer_a3[] = {9.1f,
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
  uint64_t c_buffer_coords[] = {3, 2, 3, 3, 3, 4, 4, 2, 4, 3, 4, 4};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Compute max buffer sizes
  const char* attributes[] = {"a1", "a2", "a3", TILEDB_COORDS};
  uint64_t max_buffer_sizes[5];
  uint64_t subarray[] = {3, 4, 2, 4};
  rc = tiledb_array_compute_max_read_buffer_sizes(
      ctx_, array, subarray, attributes, 4, &max_buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);

  // Prepare cell buffers
  auto buffer_a1 = (int*)malloc(max_buffer_sizes[0]);
  auto buffer_a2 = (uint64_t*)malloc(max_buffer_sizes[1]);
  auto buffer_var_a2 = (char*)malloc(max_buffer_sizes[2]);
  auto buffer_a3 = (float*)malloc(max_buffer_sizes[3]);
  auto buffer_coords = (uint64_t*)malloc(max_buffer_sizes[4]);
  void* buffers[] = {
      buffer_a1, buffer_a2, buffer_var_a2, buffer_a3, buffer_coords};
  uint64_t buffer_sizes[] = {max_buffer_sizes[0],
                             max_buffer_sizes[1],
                             max_buffer_sizes[2],
                             max_buffer_sizes[3],
                             max_buffer_sizes[4]};

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, &query, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffers(
      ctx_, query, attributes, 4, buffers, buffer_sizes);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_COMPLETED);

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Check buffers
  CHECK(sizeof(c_buffer_a1) <= max_buffer_sizes[0]);
  CHECK(sizeof(c_buffer_a2) <= max_buffer_sizes[1]);
  CHECK(sizeof(c_buffer_var_a2) <= max_buffer_sizes[2]);
  CHECK(sizeof(c_buffer_a3) <= max_buffer_sizes[3]);
  CHECK(sizeof(c_buffer_coords) <= max_buffer_sizes[4]);
  CHECK(!memcmp(buffer_a1, c_buffer_a1, sizeof(c_buffer_a1)));
  CHECK(!memcmp(buffer_a2, c_buffer_a2, sizeof(c_buffer_a2)));
  CHECK(!memcmp(buffer_var_a2, c_buffer_var_a2, sizeof(c_buffer_var_a2) - 1));
  CHECK(!memcmp(buffer_a3, c_buffer_a3, sizeof(c_buffer_a3)));
  CHECK(!memcmp(buffer_coords, c_buffer_coords, sizeof(c_buffer_coords)));

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  free(buffer_a1);
  free(buffer_a2);
  free(buffer_var_a2);
  free(buffer_a3);
  free(buffer_coords);
}

void DenseArrayFx::read_dense_array_with_coords_subarray_col(
    const std::string& array_name) {
  // Correct buffers
  int c_buffer_a1[] = {9, 11, 12, 14, 13, 15};
  uint64_t c_buffer_a2[] = {0, 2, 6, 7, 10, 12};
  char c_buffer_var_a2[] = "jjllllmooonnpppp";
  float c_buffer_a3[] = {9.1f,
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
  uint64_t c_buffer_coords[] = {3, 2, 4, 2, 3, 3, 4, 3, 3, 4, 4, 4};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Compute max buffer sizes
  const char* attributes[] = {"a1", "a2", "a3", TILEDB_COORDS};
  uint64_t max_buffer_sizes[5];
  uint64_t subarray[] = {3, 4, 2, 4};
  rc = tiledb_array_compute_max_read_buffer_sizes(
      ctx_, array, subarray, attributes, 4, &max_buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);

  // Prepare cell buffers
  auto buffer_a1 = (int*)malloc(max_buffer_sizes[0]);
  auto buffer_a2 = (uint64_t*)malloc(max_buffer_sizes[1]);
  auto buffer_var_a2 = (char*)malloc(max_buffer_sizes[2]);
  auto buffer_a3 = (float*)malloc(max_buffer_sizes[3]);
  auto buffer_coords = (uint64_t*)malloc(max_buffer_sizes[4]);
  void* buffers[] = {
      buffer_a1, buffer_a2, buffer_var_a2, buffer_a3, buffer_coords};
  uint64_t buffer_sizes[] = {max_buffer_sizes[0],
                             max_buffer_sizes[1],
                             max_buffer_sizes[2],
                             max_buffer_sizes[3],
                             max_buffer_sizes[4]};

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, &query, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_COL_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffers(
      ctx_, query, attributes, 4, buffers, buffer_sizes);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_COMPLETED);

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Check buffers
  CHECK(sizeof(c_buffer_a1) <= max_buffer_sizes[0]);
  CHECK(sizeof(c_buffer_a2) <= max_buffer_sizes[1]);
  CHECK(sizeof(c_buffer_var_a2) <= max_buffer_sizes[2]);
  CHECK(sizeof(c_buffer_a3) <= max_buffer_sizes[3]);
  CHECK(sizeof(c_buffer_coords) <= max_buffer_sizes[4]);
  CHECK(!memcmp(buffer_a1, c_buffer_a1, sizeof(c_buffer_a1)));
  CHECK(!memcmp(buffer_a2, c_buffer_a2, sizeof(c_buffer_a2)));
  CHECK(!memcmp(buffer_var_a2, c_buffer_var_a2, sizeof(c_buffer_var_a2) - 1));
  CHECK(!memcmp(buffer_a3, c_buffer_a3, sizeof(c_buffer_a3)));
  CHECK(!memcmp(buffer_coords, c_buffer_coords, sizeof(c_buffer_coords)));

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  free(buffer_a1);
  free(buffer_a2);
  free(buffer_var_a2);
  free(buffer_a3);
  free(buffer_coords);
}

void DenseArrayFx::check_subarray_partitions(const std::string& path) {
  std::string array_name = path + "subarray_partitions";
  create_dense_array(array_name);
  write_dense_array(array_name);
  check_subarray_partitions_2_row(array_name);
  check_subarray_partitions_2_col(array_name);
  check_subarray_partitions_0(array_name);
}

void DenseArrayFx::check_non_empty_domain(const std::string& path) {
  std::string array_name = path + "dense_non_empty_domain";
  create_dense_array(array_name);

  // Check empty domain
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array);
  CHECK(rc == TILEDB_OK);
  int is_empty;
  uint64_t domain[4];
  rc = tiledb_array_get_non_empty_domain(ctx_, array, domain, &is_empty);
  CHECK(rc == TILEDB_OK);
  CHECK(is_empty == 1);
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Write
  write_partial_dense_array(array_name);

  // Check non-empty domain
  rc = tiledb_array_open(ctx_, array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_get_non_empty_domain(ctx_, array, domain, &is_empty);
  CHECK(rc == TILEDB_OK);
  CHECK(is_empty == 0);
  uint64_t c_domain[] = {3, 4, 3, 4};
  CHECK(!memcmp(domain, c_domain, sizeof(c_domain)));
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
};

void DenseArrayFx::check_subarray_partitions_2_row(
    const std::string& array_name) {
  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Get subarray partitions
  const char* attributes[] = {"a1"};
  uint64_t buffer_sizes[] = {32};
  uint64_t subarray[] = {1, 4, 1, 4};
  void** subarray_partitions = nullptr;
  uint64_t npartitions;
  rc = tiledb_array_partition_subarray(
      ctx_,
      array,
      subarray,
      TILEDB_ROW_MAJOR,
      attributes,
      1,
      buffer_sizes,
      &subarray_partitions,
      &npartitions);
  CHECK(rc == TILEDB_OK);

  // Check subarray partitions
  uint64_t c_subarray_partition_1[] = {1, 2, 1, 4};
  uint64_t c_subarray_partition_2[] = {3, 4, 1, 4};
  REQUIRE(npartitions == 2);
  CHECK(!memcmp(
      subarray_partitions[0],
      c_subarray_partition_1,
      sizeof(c_subarray_partition_1)));
  CHECK(!memcmp(
      subarray_partitions[1],
      c_subarray_partition_2,
      sizeof(c_subarray_partition_2)));

  if (subarray_partitions != nullptr) {
    for (uint64_t i = 0; i < npartitions; ++i)
      free(subarray_partitions[i]);
    free(subarray_partitions);
  }

  // Close and free array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
}

void DenseArrayFx::check_subarray_partitions_2_col(
    const std::string& array_name) {
  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Get subarray partitions
  const char* attributes[] = {"a1"};
  uint64_t buffer_sizes[] = {32};
  uint64_t subarray[] = {1, 4, 1, 4};
  void** subarray_partitions = nullptr;
  uint64_t npartitions;
  rc = tiledb_array_partition_subarray(
      ctx_,
      array,
      subarray,
      TILEDB_COL_MAJOR,
      attributes,
      1,
      buffer_sizes,
      &subarray_partitions,
      &npartitions);
  CHECK(rc == TILEDB_OK);

  // Check subarray partitions
  uint64_t c_subarray_partition_1[] = {1, 4, 1, 2};
  uint64_t c_subarray_partition_2[] = {1, 4, 3, 4};
  REQUIRE(npartitions == 2);
  CHECK(!memcmp(
      subarray_partitions[0],
      c_subarray_partition_1,
      sizeof(c_subarray_partition_1)));
  CHECK(!memcmp(
      subarray_partitions[1],
      c_subarray_partition_2,
      sizeof(c_subarray_partition_2)));

  if (subarray_partitions != nullptr) {
    for (uint64_t i = 0; i < npartitions; ++i)
      std::free(subarray_partitions[i]);
    std::free(subarray_partitions);
  }

  // Close and free array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
}

void DenseArrayFx::check_subarray_partitions_0(const std::string& array_name) {
  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Get subarray partitions
  const char* attributes[] = {"a1"};
  uint64_t buffer_sizes[] = {3};
  uint64_t subarray[] = {1, 4, 1, 4};
  void** subarray_partitions = nullptr;
  uint64_t npartitions;
  rc = tiledb_array_partition_subarray(
      ctx_,
      array,
      subarray,
      TILEDB_COL_MAJOR,
      attributes,
      1,
      buffer_sizes,
      &subarray_partitions,
      &npartitions);
  CHECK(rc == TILEDB_OK);
  CHECK(npartitions == 0);
  CHECK(subarray_partitions == NULL);

  // Close and free array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
}

std::string DenseArrayFx::random_bucket_name(const std::string& prefix) {
  std::stringstream ss;
  ss << prefix << "-" << std::this_thread::get_id() << "-"
     << tiledb::sm::utils::timestamp_ms();
  return ss.str();
}

TEST_CASE_METHOD(
    DenseArrayFx, "C API: Test dense array, sorted reads", "[capi], [dense]") {
  if (supports_s3_) {
    // S3
    create_temp_dir(S3_TEMP_DIR);
    check_sorted_reads(S3_TEMP_DIR);
    remove_temp_dir(S3_TEMP_DIR);
  } else if (supports_hdfs_) {
    // HDFS
    create_temp_dir(HDFS_TEMP_DIR);
    check_sorted_reads(HDFS_TEMP_DIR);
    remove_temp_dir(HDFS_TEMP_DIR);
  } else {
    // File
    create_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
    check_sorted_reads(FILE_URI_PREFIX + FILE_TEMP_DIR);
    remove_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
  }
}

TEST_CASE_METHOD(
    DenseArrayFx,
    "C API: Test dense array, invalid number of cells in dense writes",
    "[capi], [dense]") {
  if (supports_s3_) {
    // S3
    create_temp_dir(S3_TEMP_DIR);
    check_invalid_cell_num_in_dense_writes(S3_TEMP_DIR);
    remove_temp_dir(S3_TEMP_DIR);
  } else if (supports_hdfs_) {
    // HDFS
    create_temp_dir(HDFS_TEMP_DIR);
    check_invalid_cell_num_in_dense_writes(HDFS_TEMP_DIR);
    remove_temp_dir(HDFS_TEMP_DIR);
  } else {
    // File
    create_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
    check_invalid_cell_num_in_dense_writes(FILE_URI_PREFIX + FILE_TEMP_DIR);
    remove_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
  }
}

TEST_CASE_METHOD(
    DenseArrayFx, "C API: Test dense array, sorted writes", "[capi], [dense]") {
  if (supports_s3_) {
    // S3
    create_temp_dir(S3_TEMP_DIR);
    check_sorted_writes(S3_TEMP_DIR);
    remove_temp_dir(S3_TEMP_DIR);
  } else if (supports_hdfs_) {
    // HDFS
    create_temp_dir(HDFS_TEMP_DIR);
    check_sorted_writes(HDFS_TEMP_DIR);
    remove_temp_dir(HDFS_TEMP_DIR);
  } else {
    // File
    create_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
    check_sorted_writes(FILE_URI_PREFIX + FILE_TEMP_DIR);
    remove_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
  }
}

TEST_CASE_METHOD(
    DenseArrayFx,
    "C API: Test dense array, sparse writes",
    "[capi], [dense], [dense-sparse-writes]") {
  if (supports_s3_) {
    // S3
    create_temp_dir(S3_TEMP_DIR);
    check_sparse_writes(S3_TEMP_DIR);
    remove_temp_dir(S3_TEMP_DIR);
  } else if (supports_hdfs_) {
    // HDFS
    create_temp_dir(HDFS_TEMP_DIR);
    check_sparse_writes(HDFS_TEMP_DIR);
    remove_temp_dir(HDFS_TEMP_DIR);
  } else {
    // File
    create_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
    check_sparse_writes(FILE_URI_PREFIX + FILE_TEMP_DIR);
    remove_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
  }
}

TEST_CASE_METHOD(
    DenseArrayFx,
    "C API: Test dense array, simultaneous writes",
    "[capi], [dense], [dense-simultaneous-writes]") {
  std::string temp_dir;
  if (supports_s3_) {
    temp_dir = S3_TEMP_DIR;
  } else if (supports_hdfs_) {
    temp_dir = HDFS_TEMP_DIR;
  } else {
    temp_dir = FILE_URI_PREFIX + FILE_TEMP_DIR;
  }
  create_temp_dir(temp_dir);
  check_simultaneous_writes(temp_dir);
  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    DenseArrayFx,
    "C API: Test dense array, cancel and retry writes",
    "[capi], [dense], [async], [cancel]") {
  std::string temp_dir;
  if (supports_s3_) {
    temp_dir = S3_TEMP_DIR;
  } else if (supports_hdfs_) {
    temp_dir = HDFS_TEMP_DIR;
  } else {
    temp_dir = FILE_URI_PREFIX + FILE_TEMP_DIR;
  }
  create_temp_dir(temp_dir);
  check_cancel_and_retry_writes(temp_dir);
  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    DenseArrayFx,
    "C API: Test dense array, return coordinates",
    "[capi], [dense], [return-coords]") {
  std::string temp_dir;
  if (supports_s3_) {
    temp_dir = S3_TEMP_DIR;
  } else if (supports_hdfs_) {
    temp_dir = HDFS_TEMP_DIR;
  } else {
    temp_dir = FILE_URI_PREFIX + FILE_TEMP_DIR;
  }
  create_temp_dir(temp_dir);
  check_return_coords(temp_dir);
  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    DenseArrayFx,
    "C API: Test dense array, subarray partitions",
    "[capi], [dense], [subarray-partitions]") {
  std::string temp_dir;
  if (supports_s3_) {
    temp_dir = S3_TEMP_DIR;
  } else if (supports_hdfs_) {
    temp_dir = HDFS_TEMP_DIR;
  } else {
    temp_dir = FILE_URI_PREFIX + FILE_TEMP_DIR;
  }
  create_temp_dir(temp_dir);
  check_subarray_partitions(temp_dir);
  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    DenseArrayFx,
    "C API: Test dense array, non-empty domain",
    "[capi], [dense], [dense-non-empty]") {
  std::string temp_dir = FILE_URI_PREFIX + FILE_TEMP_DIR;
  create_temp_dir(temp_dir);
  check_non_empty_domain(temp_dir);
  remove_temp_dir(temp_dir);
}
