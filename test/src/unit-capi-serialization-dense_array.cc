/**
 * @file   unit-capi-serialization-dense_array.cc
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
 * Tests of C API for serialization api dense array operations.
 */

#include "catch.hpp"
#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif
#include "test/src/helpers.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/misc/utils.h"

#include <array>
#include <cassert>
#include <climits>
#include <cstring>
#include <ctime>
#include <iostream>
#include <map>
#include <sstream>
#include <thread>

struct SerializationDenseArrayFx {
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

  tiledb_serialization_type_t serialization_type_ =
      (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP;

  // Functions
  SerializationDenseArrayFx();
  ~SerializationDenseArrayFx();
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
  void check_non_empty_domain(const std::string& path);
  void create_dense_array(const std::string& array_name);
  void create_dense_array_1_attribute(const std::string& array_name);
  void write_dense_array(const std::string& array_name);
  void write_dense_array_missing_attributes(const std::string& array_name);
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

SerializationDenseArrayFx::SerializationDenseArrayFx() {
  // Supported filesystems
  set_supported_fs();

  // Create TileDB context
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);
  REQUIRE(
      tiledb_config_set(
          config,
          "sm.rest_server_serialization_format",
          tiledb::sm::serialization_type_str(
              tiledb::sm::SerializationType(serialization_type_))
              .c_str(),
          &error) == TILEDB_OK);
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
  REQUIRE(tiledb_ctx_alloc(config, &ctx_) == TILEDB_OK);
  REQUIRE(error == nullptr);
  vfs_ = nullptr;
  REQUIRE(tiledb_vfs_alloc(ctx_, config, &vfs_) == TILEDB_OK);
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

SerializationDenseArrayFx::~SerializationDenseArrayFx() {
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

void SerializationDenseArrayFx::set_supported_fs() {
  tiledb_ctx_t* ctx = nullptr;
  REQUIRE(tiledb_ctx_alloc(nullptr, &ctx) == TILEDB_OK);

  int is_supported = 0;
  int rc = tiledb_ctx_is_supported_fs(ctx, TILEDB_S3, &is_supported);
  REQUIRE(rc == TILEDB_OK);
  supports_s3_ = (bool)is_supported;
  rc = tiledb_ctx_is_supported_fs(ctx, TILEDB_HDFS, &is_supported);
  REQUIRE(rc == TILEDB_OK);
  supports_hdfs_ = (bool)is_supported;

  tiledb_ctx_free(&ctx);
}

void SerializationDenseArrayFx::create_temp_dir(const std::string& path) {
  remove_temp_dir(path);
  REQUIRE(tiledb_vfs_create_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

void SerializationDenseArrayFx::remove_temp_dir(const std::string& path) {
  int is_dir = 0;
  REQUIRE(tiledb_vfs_is_dir(ctx_, vfs_, path.c_str(), &is_dir) == TILEDB_OK);
  if (is_dir)
    REQUIRE(tiledb_vfs_remove_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

void SerializationDenseArrayFx::check_buffer_after_updates(
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

void SerializationDenseArrayFx::create_dense_array_2D(
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
  rc = tiledb_array_create(ctx_, array_name.c_str(), array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  tiledb_attribute_free(&a);
  tiledb_dimension_free(&d1);
  tiledb_dimension_free(&d2);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

int** SerializationDenseArrayFx::generate_2D_buffer(
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

int* SerializationDenseArrayFx::read_dense_array_2D(
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
  rc = tiledb_array_open(ctx_, array, query_type);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, query_type, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
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
  char* serialized_string;
  uint64_t serialized_string_length;
  rc = tiledb_query_serialize(
      ctx_,
      query,
      serialization_type_,
      &serialized_string,
      &serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_deserialize(
      ctx_,
      query,
      serialization_type_,
      serialized_string,
      serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  delete[] serialized_string;
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Now the query must have results
  rc = tiledb_query_has_results(ctx_, query, &has_results);
  CHECK(rc == TILEDB_OK);
  CHECK(has_results);

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

void SerializationDenseArrayFx::update_dense_array_2D(
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
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[1], buffers[1], &buffer_sizes[1]);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  REQUIRE(rc == TILEDB_OK);

  // Submit query
  char* serialized_string;
  uint64_t serialized_string_length;
  rc = tiledb_query_serialize(
      ctx_,
      query,
      serialization_type_,
      &serialized_string,
      &serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_deserialize(
      ctx_,
      query,
      serialization_type_,
      serialized_string,
      serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  delete[] serialized_string;
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

void SerializationDenseArrayFx::write_dense_array_by_tiles(
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
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);
  if (rc != TILEDB_OK) {
    tiledb_error_t* err = nullptr;
    tiledb_ctx_get_last_error(ctx_, &err);
    const char* errMsg;
    tiledb_error_message(err, &errMsg);
    printf("%s\n", errMsg);
  }

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
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
      int64_t k = 0, l = 0;
      for (k = 0; k < tile_rows; ++k) {
        for (l = 0; l < tile_cols; ++l) {
          index = uint64_t(k * tile_cols + l);
          buffer_a1[index] = buffer[uint64_t(i + k)][uint64_t(j + l)];
        }
      }
      buffer_size = k * l * sizeof(int);
      buffer_sizes[0] = {buffer_size};

      char* serialized_string;
      uint64_t serialized_string_length;
      rc = tiledb_query_serialize(
          ctx_,
          query,
          serialization_type_,
          &serialized_string,
          &serialized_string_length);
      REQUIRE(rc == TILEDB_OK);
      rc = tiledb_query_deserialize(
          ctx_,
          query,
          serialization_type_,
          serialized_string,
          serialized_string_length);
      REQUIRE(rc == TILEDB_OK);
      delete[] serialized_string;
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

void SerializationDenseArrayFx::write_dense_subarray_2D(
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
  rc = tiledb_array_open(ctx_, array, query_type);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, query_type, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, query_layout);
  REQUIRE(rc == TILEDB_OK);

  // Submit query
  char* serialized_string;
  uint64_t serialized_string_length;
  rc = tiledb_query_serialize(
      ctx_,
      query,
      serialization_type_,
      &serialized_string,
      &serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_deserialize(
      ctx_,
      query,
      serialization_type_,
      serialized_string,
      serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  delete[] serialized_string;
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

void SerializationDenseArrayFx::write_dense_subarray_2D_with_cancel(
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
  rc = tiledb_array_open(ctx_, array, query_type);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, query_type, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, query_layout);
  REQUIRE(rc == TILEDB_OK);

  // Submit the same query several times, some may be duplicates, some may
  // be cancelled, it doesn't matter since it's all the same data being written.
  // TODO: this doesn't trigger the cancelled path very often.
  for (unsigned i = 0; i < num_writes; i++) {
    char* serialized_string;
    uint64_t serialized_string_length;
    rc = tiledb_query_serialize(
        ctx_,
        query,
        serialization_type_,
        &serialized_string,
        &serialized_string_length);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_query_deserialize(
        ctx_,
        query,
        serialization_type_,
        serialized_string,
        serialized_string_length);
    REQUIRE(rc == TILEDB_OK);
    delete[] serialized_string;
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
      char* serialized_string;
      uint64_t serialized_string_length;
      rc = tiledb_query_serialize(
          ctx_,
          query,
          serialization_type_,
          &serialized_string,
          &serialized_string_length);
      REQUIRE(rc == TILEDB_OK);
      rc = tiledb_query_deserialize(
          ctx_,
          query,
          serialization_type_,
          serialized_string,
          serialized_string_length);
      REQUIRE(rc == TILEDB_OK);
      delete[] serialized_string;
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

void SerializationDenseArrayFx::check_sorted_reads(const std::string& path) {
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
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Check out of bounds subarray
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
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

void SerializationDenseArrayFx::check_sorted_writes(const std::string& path) {
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

void SerializationDenseArrayFx::check_invalid_cell_num_in_dense_writes(
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
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Global order
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  REQUIRE(rc == TILEDB_OK);
  char* serialized_string;
  uint64_t serialized_string_length;
  rc = tiledb_query_serialize(
      ctx_,
      query,
      serialization_type_,
      &serialized_string,
      &serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_deserialize(
      ctx_,
      query,
      serialization_type_,
      serialized_string,
      serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  delete[] serialized_string;
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx_, query);
  REQUIRE(rc == TILEDB_ERR);
  tiledb_query_free(&query);

  // Ordered layout
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_serialize(
      ctx_,
      query,
      serialization_type_,
      &serialized_string,
      &serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_deserialize(
      ctx_,
      query,
      serialization_type_,
      serialized_string,
      serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  delete[] serialized_string;
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

void SerializationDenseArrayFx::check_sparse_writes(const std::string& path) {
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

void SerializationDenseArrayFx::check_simultaneous_writes(
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

void SerializationDenseArrayFx::check_cancel_and_retry_writes(
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

void SerializationDenseArrayFx::create_dense_array(
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

void SerializationDenseArrayFx::create_dense_array_1_attribute(
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

void SerializationDenseArrayFx::check_return_coords(const std::string& path) {
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

void SerializationDenseArrayFx::write_dense_array(
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
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_,
      query,
      attributes[1],
      (uint64_t*)buffers[1],
      &buffer_sizes[1],
      buffers[2],
      &buffer_sizes[2]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[2], buffers[3], &buffer_sizes[3]);
  CHECK(rc == TILEDB_OK);

  // Submit query
  char* serialized_string;
  uint64_t serialized_string_length;
  rc = tiledb_query_serialize(
      ctx_,
      query,
      serialization_type_,
      &serialized_string,
      &serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_deserialize(
      ctx_,
      query,
      serialization_type_,
      serialized_string,
      serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  delete[] serialized_string;
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

void SerializationDenseArrayFx::write_dense_array_missing_attributes(
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
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_,
      query,
      attributes[1],
      (uint64_t*)buffers[1],
      &buffer_sizes[1],
      buffers[2],
      &buffer_sizes[2]);
  CHECK(rc == TILEDB_OK);

  // Observe we omit setting buffer for one of the attributes (a3)

  // Submit query - this should fail
  char* serialized_string;
  uint64_t serialized_string_length;
  rc = tiledb_query_serialize(
      ctx_,
      query,
      serialization_type_,
      &serialized_string,
      &serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_deserialize(
      ctx_,
      query,
      serialization_type_,
      serialized_string,
      serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  delete[] serialized_string;
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_ERR);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void SerializationDenseArrayFx::write_partial_dense_array(
    const std::string& array_name) {
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
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[0], buffers[0], &buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_,
      query,
      attributes[1],
      (uint64_t*)buffers[1],
      &buffer_sizes[1],
      buffers[2],
      &buffer_sizes[2]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[2], buffers[3], &buffer_sizes[3]);
  CHECK(rc == TILEDB_OK);
  uint64_t subarray[] = {3, 4, 3, 4};
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  // Submit query
  char* serialized_string;
  uint64_t serialized_string_length;
  rc = tiledb_query_serialize(
      ctx_,
      query,
      serialization_type_,
      &serialized_string,
      &serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_deserialize(
      ctx_,
      query,
      serialization_type_,
      serialized_string,
      serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  delete[] serialized_string;
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

void SerializationDenseArrayFx::read_dense_array_with_coords_full_global(
    const std::string& array_name) {
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
  uint64_t c_buffer_coords[] = {1, 1, 1, 2, 2, 1, 2, 2, 1, 3, 1, 4, 2, 3, 2, 4,
                                3, 1, 3, 2, 4, 1, 4, 2, 3, 3, 3, 4, 4, 3, 4, 4};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Compute max buffer sizes
  uint64_t subarray[] = {1, 4, 1, 4};
  uint64_t buffer_a1_size, buffer_a2_off_size, buffer_a2_val_size,
      buffer_a3_size, buffer_coords_size;
  rc = tiledb_array_max_buffer_size(
      ctx_, array, "a1", subarray, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size_var(
      ctx_, array, "a2", subarray, &buffer_a2_off_size, &buffer_a2_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size(
      ctx_, array, "a3", subarray, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size(
      ctx_, array, TILEDB_COORDS, subarray, &buffer_coords_size);
  CHECK(rc == TILEDB_OK);

  // Prepare cell buffers
  auto buffer_a1 = (int*)malloc(buffer_a1_size);
  auto buffer_a2_off = (uint64_t*)malloc(buffer_a2_off_size);
  auto buffer_a2_val = (char*)malloc(buffer_a2_val_size);
  auto buffer_a3 = (float*)malloc(buffer_a3_size);
  auto buffer_coords = (uint64_t*)malloc(buffer_coords_size);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", buffer_a1, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_,
      query,
      "a2",
      buffer_a2_off,
      &buffer_a2_off_size,
      buffer_a2_val,
      &buffer_a2_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a3", buffer_a3, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, TILEDB_COORDS, buffer_coords, &buffer_coords_size);
  CHECK(rc == TILEDB_OK);

  // Submit query
  char* serialized_string;
  uint64_t serialized_string_length;
  rc = tiledb_query_serialize(
      ctx_,
      query,
      serialization_type_,
      &serialized_string,
      &serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_deserialize(
      ctx_,
      query,
      serialization_type_,
      serialized_string,
      serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  delete[] serialized_string;
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_COMPLETED);

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Check buffers
  CHECK(sizeof(c_buffer_a1) == buffer_a1_size);
  CHECK(sizeof(c_buffer_a2_off) == buffer_a2_off_size);
  CHECK(sizeof(c_buffer_a2_val) - 1 == buffer_a2_val_size);
  CHECK(sizeof(c_buffer_a3) == buffer_a3_size);
  CHECK(sizeof(c_buffer_coords) == buffer_coords_size);
  CHECK(!memcmp(buffer_a1, c_buffer_a1, sizeof(c_buffer_a1)));
  CHECK(!memcmp(buffer_a2_off, c_buffer_a2_off, sizeof(c_buffer_a2_off)));
  CHECK(!memcmp(buffer_a2_val, c_buffer_a2_val, sizeof(c_buffer_a2_val) - 1));
  CHECK(!memcmp(buffer_a3, c_buffer_a3, sizeof(c_buffer_a3)));
  CHECK(!memcmp(buffer_coords, c_buffer_coords, sizeof(c_buffer_coords)));

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
  free(buffer_coords);
}

void SerializationDenseArrayFx::read_dense_array_with_coords_full_row(
    const std::string& array_name) {
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
  uint64_t c_buffer_coords[] = {1, 1, 1, 2, 1, 3, 1, 4, 2, 1, 2, 2, 2, 3, 2, 4,
                                3, 1, 3, 2, 3, 3, 3, 4, 4, 1, 4, 2, 4, 3, 4, 4};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Compute max buffer sizes
  uint64_t subarray[] = {1, 4, 1, 4};
  uint64_t buffer_a1_size, buffer_a2_off_size, buffer_a2_val_size,
      buffer_a3_size, buffer_coords_size;
  rc = tiledb_array_max_buffer_size(
      ctx_, array, "a1", subarray, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size_var(
      ctx_, array, "a2", subarray, &buffer_a2_off_size, &buffer_a2_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size(
      ctx_, array, "a3", subarray, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size(
      ctx_, array, TILEDB_COORDS, subarray, &buffer_coords_size);
  CHECK(rc == TILEDB_OK);

  // Prepare cell buffers
  auto buffer_a1 = (int*)malloc(buffer_a1_size);
  auto buffer_a2_off = (uint64_t*)malloc(buffer_a2_off_size);
  auto buffer_a2_val = (char*)malloc(buffer_a2_val_size);
  auto buffer_a3 = (float*)malloc(buffer_a3_size);
  auto buffer_coords = (uint64_t*)malloc(buffer_coords_size);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", buffer_a1, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_,
      query,
      "a2",
      buffer_a2_off,
      &buffer_a2_off_size,
      buffer_a2_val,
      &buffer_a2_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a3", buffer_a3, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, TILEDB_COORDS, buffer_coords, &buffer_coords_size);
  CHECK(rc == TILEDB_OK);

  // Submit query
  char* serialized_string;
  uint64_t serialized_string_length;
  rc = tiledb_query_serialize(
      ctx_,
      query,
      serialization_type_,
      &serialized_string,
      &serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_deserialize(
      ctx_,
      query,
      serialization_type_,
      serialized_string,
      serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  delete[] serialized_string;
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_COMPLETED);

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Check buffers
  CHECK(sizeof(c_buffer_a1) == buffer_a1_size);
  CHECK(sizeof(c_buffer_a2_off) == buffer_a2_off_size);
  CHECK(sizeof(c_buffer_a2_val) - 1 == buffer_a2_val_size);
  CHECK(sizeof(c_buffer_a3) == buffer_a3_size);
  CHECK(sizeof(c_buffer_coords) == buffer_coords_size);
  CHECK(!memcmp(buffer_a1, c_buffer_a1, sizeof(c_buffer_a1)));
  CHECK(!memcmp(buffer_a2_off, c_buffer_a2_off, sizeof(c_buffer_a2_off)));
  CHECK(!memcmp(buffer_a2_val, c_buffer_a2_val, sizeof(c_buffer_a2_val) - 1));
  CHECK(!memcmp(buffer_a3, c_buffer_a3, sizeof(c_buffer_a3)));
  CHECK(!memcmp(buffer_coords, c_buffer_coords, sizeof(c_buffer_coords)));

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
  free(buffer_coords);
}

void SerializationDenseArrayFx::read_dense_array_with_coords_full_col(
    const std::string& array_name) {
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
  uint64_t c_buffer_coords[] = {1, 1, 2, 1, 3, 1, 4, 1, 1, 2, 2, 2, 3, 2, 4, 2,
                                1, 3, 2, 3, 3, 3, 4, 3, 1, 4, 2, 4, 3, 4, 4, 4};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Compute max buffer sizes
  uint64_t subarray[] = {1, 4, 1, 4};
  uint64_t buffer_a1_size, buffer_a2_off_size, buffer_a2_val_size,
      buffer_a3_size, buffer_coords_size;
  rc = tiledb_array_max_buffer_size(
      ctx_, array, "a1", subarray, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size_var(
      ctx_, array, "a2", subarray, &buffer_a2_off_size, &buffer_a2_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size(
      ctx_, array, "a3", subarray, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size(
      ctx_, array, TILEDB_COORDS, subarray, &buffer_coords_size);
  CHECK(rc == TILEDB_OK);

  // Prepare cell buffers
  auto buffer_a1 = (int*)malloc(buffer_a1_size);
  auto buffer_a2_off = (uint64_t*)malloc(buffer_a2_off_size);
  auto buffer_a2_val = (char*)malloc(buffer_a2_val_size);
  auto buffer_a3 = (float*)malloc(buffer_a3_size);
  auto buffer_coords = (uint64_t*)malloc(buffer_coords_size);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_COL_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", buffer_a1, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_,
      query,
      "a2",
      buffer_a2_off,
      &buffer_a2_off_size,
      buffer_a2_val,
      &buffer_a2_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a3", buffer_a3, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, TILEDB_COORDS, buffer_coords, &buffer_coords_size);
  CHECK(rc == TILEDB_OK);

  // Submit query
  char* serialized_string;
  uint64_t serialized_string_length;
  rc = tiledb_query_serialize(
      ctx_,
      query,
      serialization_type_,
      &serialized_string,
      &serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_deserialize(
      ctx_,
      query,
      serialization_type_,
      serialized_string,
      serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  delete[] serialized_string;
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_COMPLETED);

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Check buffers
  CHECK(sizeof(c_buffer_a1) == buffer_a1_size);
  CHECK(sizeof(c_buffer_a2_off) == buffer_a2_off_size);
  CHECK(sizeof(c_buffer_a2_val) - 1 == buffer_a2_val_size);
  CHECK(sizeof(c_buffer_a3) == buffer_a3_size);
  CHECK(sizeof(c_buffer_coords) == buffer_coords_size);
  CHECK(!memcmp(buffer_a1, c_buffer_a1, sizeof(c_buffer_a1)));
  CHECK(!memcmp(buffer_a2_off, c_buffer_a2_off, sizeof(c_buffer_a2_off)));
  CHECK(!memcmp(buffer_a2_val, c_buffer_a2_val, sizeof(c_buffer_a2_val) - 1));
  CHECK(!memcmp(buffer_a3, c_buffer_a3, sizeof(c_buffer_a3)));
  CHECK(!memcmp(buffer_coords, c_buffer_coords, sizeof(c_buffer_coords)));

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
  free(buffer_coords);
}

void SerializationDenseArrayFx::read_dense_array_with_coords_subarray_global(
    const std::string& array_name) {
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
  uint64_t c_buffer_coords[] = {3, 2, 4, 2, 3, 3, 3, 4, 4, 3, 4, 4};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Compute max buffer sizes
  uint64_t subarray[] = {3, 4, 2, 4};
  uint64_t buffer_a1_size, buffer_a2_off_size, buffer_a2_val_size,
      buffer_a3_size, buffer_coords_size;
  rc = tiledb_array_max_buffer_size(
      ctx_, array, "a1", subarray, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size_var(
      ctx_, array, "a2", subarray, &buffer_a2_off_size, &buffer_a2_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size(
      ctx_, array, "a3", subarray, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size(
      ctx_, array, TILEDB_COORDS, subarray, &buffer_coords_size);
  CHECK(rc == TILEDB_OK);

  // Prepare cell buffers
  auto buffer_a1 = (int*)malloc(buffer_a1_size);
  auto buffer_a2_off = (uint64_t*)malloc(buffer_a2_off_size);
  auto buffer_a2_val = (char*)malloc(buffer_a2_val_size);
  auto buffer_a3 = (float*)malloc(buffer_a3_size);
  auto buffer_coords = (uint64_t*)malloc(buffer_coords_size);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", buffer_a1, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_,
      query,
      "a2",
      buffer_a2_off,
      &buffer_a2_off_size,
      buffer_a2_val,
      &buffer_a2_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a3", buffer_a3, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, TILEDB_COORDS, buffer_coords, &buffer_coords_size);
  CHECK(rc == TILEDB_OK);

  // Submit query
  char* serialized_string;
  uint64_t serialized_string_length;
  rc = tiledb_query_serialize(
      ctx_,
      query,
      serialization_type_,
      &serialized_string,
      &serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_deserialize(
      ctx_,
      query,
      serialization_type_,
      serialized_string,
      serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  delete[] serialized_string;
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_COMPLETED);

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Check buffers
  CHECK(sizeof(c_buffer_a1) <= buffer_a1_size);
  CHECK(sizeof(c_buffer_a2_off) <= buffer_a2_off_size);
  CHECK(sizeof(c_buffer_a2_val) - 1 <= buffer_a2_val_size);
  CHECK(sizeof(c_buffer_a3) <= buffer_a3_size);
  CHECK(sizeof(c_buffer_coords) <= buffer_coords_size);
  CHECK(!memcmp(buffer_a1, c_buffer_a1, sizeof(c_buffer_a1)));
  CHECK(!memcmp(buffer_a2_off, c_buffer_a2_off, sizeof(c_buffer_a2_off)));
  CHECK(!memcmp(buffer_a2_val, c_buffer_a2_val, sizeof(c_buffer_a2_val) - 1));
  CHECK(!memcmp(buffer_a3, c_buffer_a3, sizeof(c_buffer_a3)));
  CHECK(!memcmp(buffer_coords, c_buffer_coords, sizeof(c_buffer_coords)));

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
  free(buffer_coords);
}

void SerializationDenseArrayFx::read_dense_array_with_coords_subarray_row(
    const std::string& array_name) {
  // Correct buffers
  int c_buffer_a1[] = {9, 12, 13, 11, 14, 15};
  uint64_t c_buffer_a2_off[] = {0, 2, 3, 5, 9, 12};
  char c_buffer_a2_val[] = "jjmnnllllooopppp";
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
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Compute max buffer sizes
  uint64_t subarray[] = {3, 4, 2, 4};
  uint64_t buffer_a1_size, buffer_a2_off_size, buffer_a2_val_size,
      buffer_a3_size, buffer_coords_size;
  rc = tiledb_array_max_buffer_size(
      ctx_, array, "a1", subarray, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size_var(
      ctx_, array, "a2", subarray, &buffer_a2_off_size, &buffer_a2_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size(
      ctx_, array, "a3", subarray, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size(
      ctx_, array, TILEDB_COORDS, subarray, &buffer_coords_size);
  CHECK(rc == TILEDB_OK);

  // Prepare cell buffers
  auto buffer_a1 = (int*)malloc(buffer_a1_size);
  auto buffer_a2_off = (uint64_t*)malloc(buffer_a2_off_size);
  auto buffer_a2_val = (char*)malloc(buffer_a2_val_size);
  auto buffer_a3 = (float*)malloc(buffer_a3_size);
  auto buffer_coords = (uint64_t*)malloc(buffer_coords_size);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", buffer_a1, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_,
      query,
      "a2",
      buffer_a2_off,
      &buffer_a2_off_size,
      buffer_a2_val,
      &buffer_a2_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a3", buffer_a3, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, TILEDB_COORDS, buffer_coords, &buffer_coords_size);
  CHECK(rc == TILEDB_OK);

  // Submit query
  char* serialized_string;
  uint64_t serialized_string_length;
  rc = tiledb_query_serialize(
      ctx_,
      query,
      serialization_type_,
      &serialized_string,
      &serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_deserialize(
      ctx_,
      query,
      serialization_type_,
      serialized_string,
      serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  delete[] serialized_string;
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_COMPLETED);

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Check buffers
  CHECK(sizeof(c_buffer_a1) == buffer_a1_size);
  CHECK(sizeof(c_buffer_a2_off) == buffer_a2_off_size);
  CHECK(sizeof(c_buffer_a2_val) - 1 == buffer_a2_val_size);
  CHECK(sizeof(c_buffer_a3) == buffer_a3_size);
  CHECK(sizeof(c_buffer_coords) == buffer_coords_size);
  CHECK(!memcmp(buffer_a1, c_buffer_a1, sizeof(c_buffer_a1)));
  CHECK(!memcmp(buffer_a2_off, c_buffer_a2_off, sizeof(c_buffer_a2_off)));
  CHECK(!memcmp(buffer_a2_val, c_buffer_a2_val, sizeof(c_buffer_a2_val) - 1));
  CHECK(!memcmp(buffer_a3, c_buffer_a3, sizeof(c_buffer_a3)));
  CHECK(!memcmp(buffer_coords, c_buffer_coords, sizeof(c_buffer_coords)));

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
  free(buffer_coords);
}

void SerializationDenseArrayFx::read_dense_array_with_coords_subarray_col(
    const std::string& array_name) {
  // Correct buffers
  int c_buffer_a1[] = {9, 11, 12, 14, 13, 15};
  uint64_t c_buffer_a2_off[] = {0, 2, 6, 7, 10, 12};
  char c_buffer_a2_val[] = "jjllllmooonnpppp";
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
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Compute max buffer sizes
  uint64_t subarray[] = {3, 4, 2, 4};
  uint64_t buffer_a1_size, buffer_a2_off_size, buffer_a2_val_size,
      buffer_a3_size, buffer_coords_size;
  rc = tiledb_array_max_buffer_size(
      ctx_, array, "a1", subarray, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size_var(
      ctx_, array, "a2", subarray, &buffer_a2_off_size, &buffer_a2_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size(
      ctx_, array, "a3", subarray, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size(
      ctx_, array, TILEDB_COORDS, subarray, &buffer_coords_size);
  CHECK(rc == TILEDB_OK);

  // Prepare cell buffers
  auto buffer_a1 = (int*)malloc(buffer_a1_size);
  auto buffer_a2_off = (uint64_t*)malloc(buffer_a2_off_size);
  auto buffer_a2_val = (char*)malloc(buffer_a2_val_size);
  auto buffer_a3 = (float*)malloc(buffer_a3_size);
  auto buffer_coords = (uint64_t*)malloc(buffer_coords_size);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_COL_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", buffer_a1, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_,
      query,
      "a2",
      buffer_a2_off,
      &buffer_a2_off_size,
      buffer_a2_val,
      &buffer_a2_val_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a3", buffer_a3, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, TILEDB_COORDS, buffer_coords, &buffer_coords_size);
  CHECK(rc == TILEDB_OK);

  // Submit query
  char* serialized_string;
  uint64_t serialized_string_length;
  rc = tiledb_query_serialize(
      ctx_,
      query,
      serialization_type_,
      &serialized_string,
      &serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_deserialize(
      ctx_,
      query,
      serialization_type_,
      serialized_string,
      serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  delete[] serialized_string;
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(status == TILEDB_COMPLETED);

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Check buffers
  CHECK(sizeof(c_buffer_a1) == buffer_a1_size);
  CHECK(sizeof(c_buffer_a2_off) == buffer_a2_off_size);
  CHECK(sizeof(c_buffer_a2_val) - 1 == buffer_a2_val_size);
  CHECK(sizeof(c_buffer_a3) == buffer_a3_size);
  CHECK(sizeof(c_buffer_coords) == buffer_coords_size);
  CHECK(!memcmp(buffer_a1, c_buffer_a1, sizeof(c_buffer_a1)));
  CHECK(!memcmp(buffer_a2_off, c_buffer_a2_off, sizeof(c_buffer_a2_off)));
  CHECK(!memcmp(buffer_a2_val, c_buffer_a2_val, sizeof(c_buffer_a2_val) - 1));
  CHECK(!memcmp(buffer_a3, c_buffer_a3, sizeof(c_buffer_a3)));
  CHECK(!memcmp(buffer_coords, c_buffer_coords, sizeof(c_buffer_coords)));

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
  free(buffer_coords);
}

void SerializationDenseArrayFx::check_non_empty_domain(
    const std::string& path) {
  std::string array_name = path + "dense_non_empty_domain";
  create_dense_array(array_name);

  // Check empty domain
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
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
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
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

std::string SerializationDenseArrayFx::random_bucket_name(
    const std::string& prefix) {
  std::stringstream ss;
  ss << prefix << "-" << std::this_thread::get_id() << "-"
     << tiledb::sm::utils::time::timestamp_now_ms();
  return ss.str();
}

TEST_CASE_METHOD(
    SerializationDenseArrayFx,
    "C API: Test Serialization dense array, sorted reads",
    "[capi], [serialization], [dense]") {
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
    SerializationDenseArrayFx,
    "C API: Test Serialization dense array, invalid number of cells in dense "
    "writes",
    "[capi], [serialization], [dense]") {
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
    SerializationDenseArrayFx,
    "C API: Test Serialization dense array, sorted writes",
    "[capi], [serialization], [dense]") {
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
    SerializationDenseArrayFx,
    "C API: Test Serialization dense array, sparse writes",
    "[capi], [serialization], [dense], [dense-sparse-writes]") {
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
    SerializationDenseArrayFx,
    "C API: Test Serialization dense array, simultaneous writes",
    "[capi], [serialization], [dense], [dense-simultaneous-writes]") {
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
    SerializationDenseArrayFx,
    "C API: Test Serialization dense array, cancel and retry writes",
    "[capi], [serialization], [dense], [async], [cancel]") {
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
    SerializationDenseArrayFx,
    "C API: Test Serialization dense array, return coordinates",
    "[capi], [serialization], [dense], [return-coords]") {
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
    SerializationDenseArrayFx,
    "C API: Test Serialization dense array, non-empty domain",
    "[capi], [serialization], [dense], [dense-non-empty]") {
  std::string temp_dir = FILE_URI_PREFIX + FILE_TEMP_DIR;
  create_temp_dir(temp_dir);
  check_non_empty_domain(temp_dir);
  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    SerializationDenseArrayFx,
    "C API: Test Serialization dense array, invalid set query buffer",
    "[capi], [serialization], [dense], [dense-invalid-set-query-buffer]") {
  std::string temp_dir = FILE_URI_PREFIX + FILE_TEMP_DIR;
  create_temp_dir(temp_dir);

  // Create and write dense array
  std::string array_name = temp_dir + "dense_non_empty_domain";
  create_dense_array(array_name);
  write_dense_array(array_name);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Allocate query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);

  // Aux variables
  uint64_t off[1];
  uint64_t off_size;
  int a1[1];
  uint64_t a1_size = sizeof(a1);

  // Check invalid attribute
  rc = tiledb_query_set_buffer(ctx_, query, "foo", a1, &a1_size);
  CHECK(rc == TILEDB_ERR);

  // Check invalid attribute
  rc = tiledb_query_set_buffer_var(
      ctx_, query, "foo", off, &off_size, a1, &a1_size);
  CHECK(rc == TILEDB_ERR);

  // Check non-fixed attribute
  rc = tiledb_query_set_buffer(ctx_, query, "a2", a1, &a1_size);
  CHECK(rc == TILEDB_ERR);

  // Check non-var attribute
  rc = tiledb_query_set_buffer_var(
      ctx_, query, "a1", off, &off_size, a1, &a1_size);
  CHECK(rc == TILEDB_ERR);

  // Check no buffers set
  char* serialized_string;
  uint64_t serialized_string_length;
  rc = tiledb_query_serialize(
      ctx_,
      query,
      serialization_type_,
      &serialized_string,
      &serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_deserialize(
      ctx_,
      query,
      serialization_type_,
      serialized_string,
      serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  delete[] serialized_string;
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_ERR);

  // Issue an incomplete query
  rc = tiledb_query_set_buffer(ctx_, query, "a1", a1, &a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_serialize(
      ctx_,
      query,
      serialization_type_,
      &serialized_string,
      &serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_deserialize(
      ctx_,
      query,
      serialization_type_,
      serialized_string,
      serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  delete[] serialized_string;
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Check that setting a new attribute for an incomplete query fails
  rc = tiledb_query_set_buffer_var(
      ctx_, query, "a2", off, &off_size, a1, &a1_size);
  CHECK(rc == TILEDB_ERR);

  // But resetting an existing attribute buffer succeeds
  rc = tiledb_query_set_buffer(ctx_, query, "a1", a1, &a1_size);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_query_free(&query);
  tiledb_array_free(&array);

  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    SerializationDenseArrayFx,
    "C API: Test Serialization dense array, open array checks",
    "[capi], [serialization], [dense], [dense-open-array-checks]") {
  std::string temp_dir = FILE_URI_PREFIX + FILE_TEMP_DIR;
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
  uint64_t subarray[] = {1, 4, 1, 4};
  uint64_t domain[4];
  int is_empty = false;
  rc = tiledb_array_get_non_empty_domain(ctx_, array, domain, &is_empty);
  CHECK(rc == TILEDB_ERR);

  // Getting the max buffer sizes should fail for an array opened for writes
  uint64_t buffer_a1_size;
  rc = tiledb_array_max_buffer_size(
      ctx_, array, "a1", subarray, &buffer_a1_size);
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
  char* serialized_string;
  uint64_t serialized_string_length;
  rc = tiledb_query_serialize(
      ctx_,
      query,
      serialization_type_,
      &serialized_string,
      &serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_deserialize(
      ctx_,
      query,
      serialization_type_,
      serialized_string,
      serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  delete[] serialized_string;
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_ERR);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Submitting the query after closing an array should fail
  rc = tiledb_query_serialize(
      ctx_,
      query,
      serialization_type_,
      &serialized_string,
      &serialized_string_length);
  REQUIRE(rc == TILEDB_ERR);
  rc = tiledb_query_deserialize(
      ctx_,
      query,
      serialization_type_,
      serialized_string,
      serialized_string_length);
  REQUIRE(rc == TILEDB_ERR);
  delete[] serialized_string;
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_ERR);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    SerializationDenseArrayFx,
    "C API: Test Serialization dense array, reopen array checks",
    "[capi], [serialization], [dense], [dense-reopen-array-checks]") {
  std::string temp_dir = FILE_URI_PREFIX + FILE_TEMP_DIR;
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

  // Open array for reads
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Write something in the meantime
  write_partial_dense_array(array_name);

  // Prepare buffer
  const uint64_t subarray[] = {3, 3, 4, 4};
  int a1_buffer[1];
  uint64_t a1_buff_size = sizeof(a1_buffer);
  CHECK(rc == TILEDB_OK);

  // Submit a read query
  tiledb_query_t* query_1;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query_1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query_1, "a1", &a1_buffer, &a1_buff_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query_1, &subarray[0]);
  CHECK(rc == TILEDB_OK);
  char* serialized_string;
  uint64_t serialized_string_length;
  rc = tiledb_query_serialize(
      ctx_,
      query_1,
      serialization_type_,
      &serialized_string,
      &serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_deserialize(
      ctx_,
      query_1,
      serialization_type_,
      serialized_string,
      serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  delete[] serialized_string;
  rc = tiledb_query_submit(ctx_, query_1);
  CHECK(rc == TILEDB_OK);

  // The read query should not see the second fragment
  CHECK(a1_buffer[0] == 13);

  // Reopen the array to see the new fragment
  rc = tiledb_array_reopen(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Submit a new query
  tiledb_query_t* query_2;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query_2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query_2, "a1", &a1_buffer, &a1_buff_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query_2, &subarray[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_serialize(
      ctx_,
      query_2,
      serialization_type_,
      &serialized_string,
      &serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_deserialize(
      ctx_,
      query_2,
      serialization_type_,
      serialized_string,
      serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  delete[] serialized_string;
  rc = tiledb_query_submit(ctx_, query_2);
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
    SerializationDenseArrayFx,
    "C API: Test Serialization dense array, reset read subarray",
    "[capi], [serialization], [dense], [reset-read-subarray]") {
  std::string temp_dir = FILE_URI_PREFIX + FILE_TEMP_DIR;
  std::string array_name = temp_dir + "reset_read_subarray";
  create_temp_dir(temp_dir);
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

  int a1[1];
  uint64_t a1_size = sizeof(a1);
  uint64_t subarray[] = {1, 2, 1, 2};
  uint64_t subarray_2[] = {3, 3, 3, 3};

  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", a1, &a1_size);
  CHECK(rc == TILEDB_OK);
  char* serialized_string;
  uint64_t serialized_string_length;
  rc = tiledb_query_serialize(
      ctx_,
      query,
      serialization_type_,
      &serialized_string,
      &serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_deserialize(
      ctx_,
      query,
      serialization_type_,
      serialized_string,
      serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  delete[] serialized_string;
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(rc == TILEDB_OK);
  CHECK(status == TILEDB_INCOMPLETE);

  rc = tiledb_query_set_subarray(ctx_, query, subarray_2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(rc == TILEDB_OK);
  CHECK(status == TILEDB_UNINITIALIZED);

  rc = tiledb_query_serialize(
      ctx_,
      query,
      serialization_type_,
      &serialized_string,
      &serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_deserialize(
      ctx_,
      query,
      serialization_type_,
      serialized_string,
      serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  delete[] serialized_string;
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(rc == TILEDB_OK);
  CHECK(status == TILEDB_COMPLETED);

  CHECK(a1[0] == 12);

  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_query_free(&query);
  tiledb_array_free(&array);

  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    SerializationDenseArrayFx,
    "C API: Test Serialization dense array, reset write subarray",
    "[capi], [serialization], [dense], [reset-write-subarray]") {
  std::string temp_dir = FILE_URI_PREFIX + FILE_TEMP_DIR;
  std::string array_name = temp_dir + "reset_write_subarray";
  create_temp_dir(temp_dir);
  create_dense_array(array_name);

  // -- WRITE QUERY --

  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);

  int a1[] = {100, 101, 102, 103};
  uint64_t a1_size = sizeof(a1);
  char a2_data[] = {'a', 'b', 'c', 'd'};
  uint64_t a2_data_size = sizeof(a2_data);
  uint64_t a2_off[] = {0, 1, 2, 3};
  uint64_t a2_off_size = sizeof(a2_off);
  uint64_t subarray[] = {1, 2, 1, 2};
  float a3[] = {1.1f, 1.2f, 2.1f, 2.2f, 3.1f, 3.2f, 4.1f, 4.2f};
  uint64_t a3_size = sizeof(a3);

  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", a1, &a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_, query, "a2", a2_off, &a2_off_size, a2_data, &a2_data_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a3", a3, &a3_size);
  CHECK(rc == TILEDB_OK);
  char* serialized_string;
  uint64_t serialized_string_length;
  rc = tiledb_query_serialize(
      ctx_,
      query,
      serialization_type_,
      &serialized_string,
      &serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_deserialize(
      ctx_,
      query,
      serialization_type_,
      serialized_string,
      serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  delete[] serialized_string;
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(rc == TILEDB_OK);
  CHECK(status == TILEDB_COMPLETED);

  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(rc == TILEDB_OK);
  CHECK(status == TILEDB_UNINITIALIZED);

  rc = tiledb_query_serialize(
      ctx_,
      query,
      serialization_type_,
      &serialized_string,
      &serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_deserialize(
      ctx_,
      query,
      serialization_type_,
      serialized_string,
      serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  delete[] serialized_string;
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(rc == TILEDB_OK);
  CHECK(status == TILEDB_COMPLETED);

  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_query_free(&query);
  tiledb_array_free(&array);

  // -- READ QUERY --

  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", a1, &a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_serialize(
      ctx_,
      query,
      serialization_type_,
      &serialized_string,
      &serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_deserialize(
      ctx_,
      query,
      serialization_type_,
      serialized_string,
      serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  delete[] serialized_string;
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(rc == TILEDB_OK);
  CHECK(status == TILEDB_COMPLETED);

  CHECK(a1[0] == 100);
  CHECK(a1[1] == 101);
  CHECK(a1[2] == 102);
  CHECK(a1[3] == 103);

  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_query_free(&query);
  tiledb_array_free(&array);

  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    SerializationDenseArrayFx,
    "C API: Test Serialization dense array, URI ending in a slash",
    "[capi], [serialization], [dense], [uri-ending-slash]") {
  std::string temp_dir = FILE_URI_PREFIX + FILE_TEMP_DIR;
  std::string array_name = temp_dir + "with_ending_slash/";
  create_temp_dir(temp_dir);
  create_dense_array(array_name);
  write_dense_array(array_name);
  read_dense_array_with_coords_full_global(array_name);
  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    SerializationDenseArrayFx,
    "C API: Test Serialization dense array, missing attributes in writes",
    "[capi], [serialization], [dense], [dense-write-missing-attributes]") {
  std::string temp_dir = FILE_URI_PREFIX + FILE_TEMP_DIR;
  std::string array_name = temp_dir + "dense_write_missing_attributes/";
  create_temp_dir(temp_dir);
  create_dense_array(array_name);
  write_dense_array_missing_attributes(array_name);
  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    SerializationDenseArrayFx,
    "C API: Test Serialization dense array, read subarrays with empty cells",
    "[capi], [serialization], [dense], [dense-read-empty]") {
  std::string temp_dir = FILE_URI_PREFIX + FILE_TEMP_DIR;
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
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[0], write_a1, &write_a1_size);
  CHECK(rc == TILEDB_OK);
  uint64_t subarray[] = {2, 3, 1, 2};
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
  char* serialized_string;
  uint64_t serialized_string_length;
  rc = tiledb_query_serialize(
      ctx_,
      query,
      serialization_type_,
      &serialized_string,
      &serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_deserialize(
      ctx_,
      query,
      serialization_type_,
      serialized_string,
      serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  delete[] serialized_string;
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  // Read whole array
  int c_a1[] = {INT_MIN,
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
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[0], read_a1, &read_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_serialize(
      ctx_,
      query,
      serialization_type_,
      &serialized_string,
      &serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_deserialize(
      ctx_,
      query,
      serialization_type_,
      serialized_string,
      serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  delete[] serialized_string;
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  CHECK(!memcmp(c_a1, read_a1, sizeof(c_a1)));

  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    SerializationDenseArrayFx,
    "C API: Test Serialization dense array, read subarrays with empty areas "
    "around "
    "sparse "
    "cells",
    "[capi], [serialization], [dense], [dense-read-empty], "
    "[dense-read-empty-sparse]") {
  std::string temp_dir = FILE_URI_PREFIX + FILE_TEMP_DIR;
  std::string array_name = temp_dir + "dense_read_empty_sparse/";
  create_temp_dir(temp_dir);

  create_dense_array_1_attribute(array_name);

  // Write a slice
  int write_a1[] = {1, 2, 3, 4};
  uint64_t write_a1_size = sizeof(write_a1);
  uint64_t write_coords[] = {1, 2, 2, 1, 4, 3, 1, 4};
  uint64_t write_coords_size = sizeof(write_coords);
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", write_a1, &write_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, TILEDB_COORDS, write_coords, &write_coords_size);
  CHECK(rc == TILEDB_OK);
  char* serialized_string;
  uint64_t serialized_string_length;
  rc = tiledb_query_serialize(
      ctx_,
      query,
      serialization_type_,
      &serialized_string,
      &serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_deserialize(
      ctx_,
      query,
      serialization_type_,
      serialized_string,
      serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  delete[] serialized_string;
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  // Read whole array
  int c_a1[] = {INT_MIN,
                1,
                INT_MIN,
                4,
                2,
                INT_MIN,
                INT_MIN,
                INT_MIN,
                INT_MIN,
                INT_MIN,
                INT_MIN,
                INT_MIN,
                INT_MIN,
                INT_MIN,
                3,
                INT_MIN};
  uint64_t c_coords[] = {1, 1, 1, 2, 1, 3, 1, 4, 2, 1, 2, 2, 2, 3, 2, 4,
                         3, 1, 3, 2, 3, 3, 3, 4, 4, 1, 4, 2, 4, 3, 4, 4};
  int read_a1[16];
  uint64_t read_a1_size = sizeof(read_a1);
  uint64_t read_coords[32];
  uint64_t read_coords_size = sizeof(read_coords);
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", read_a1, &read_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, TILEDB_COORDS, read_coords, &read_coords_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_serialize(
      ctx_,
      query,
      serialization_type_,
      &serialized_string,
      &serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_deserialize(
      ctx_,
      query,
      serialization_type_,
      serialized_string,
      serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  delete[] serialized_string;
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  // These checks will not work until special attributes aka COORDS is
  // supported.
  //  CHECK(!memcmp(c_a1, read_a1, sizeof(c_a1)));
  //  CHECK(!memcmp(c_coords, read_coords, sizeof(c_coords)));

  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    SerializationDenseArrayFx,
    "C API: Test Serialization dense array, read subarrays with empty areas, "
    "merging "
    "adjacent cell ranges",
    "[capi], [serialization], [dense], [dense-read-empty], "
    "[dense-read-empty-merge]") {
  std::string temp_dir = FILE_URI_PREFIX + FILE_TEMP_DIR;
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
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[0], write_a1, &write_a1_size);
  CHECK(rc == TILEDB_OK);
  uint64_t subarray[] = {2, 3, 2, 3};
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
  char* serialized_string;
  uint64_t serialized_string_length;
  rc = tiledb_query_serialize(
      ctx_,
      query,
      serialization_type_,
      &serialized_string,
      &serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_deserialize(
      ctx_,
      query,
      serialization_type_,
      serialized_string,
      serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  delete[] serialized_string;
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  // Read whole array
  int c_a1[] = {INT_MIN,
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
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[0], read_a1, &read_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_serialize(
      ctx_,
      query,
      serialization_type_,
      &serialized_string,
      &serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_deserialize(
      ctx_,
      query,
      serialization_type_,
      serialized_string,
      serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  delete[] serialized_string;
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  CHECK(!memcmp(c_a1, read_a1, sizeof(c_a1)));

  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    SerializationDenseArrayFx,
    "C API: Test Serialization dense array, multi-fragment reads",
    "[capi], [serialization], [dense], [dense-multi-fragment]") {
  std::string temp_dir = FILE_URI_PREFIX + FILE_TEMP_DIR;
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
  rc = tiledb_query_set_buffer(ctx_, query, "a1", write_a1, &write_a1_size);
  CHECK(rc == TILEDB_OK);
  uint64_t subarray_1[] = {1, 2, 1, 4};
  rc = tiledb_query_set_subarray(ctx_, query, subarray_1);
  CHECK(rc == TILEDB_OK);
  char* serialized_string;
  uint64_t serialized_string_length;
  rc = tiledb_query_serialize(
      ctx_,
      query,
      serialization_type_,
      &serialized_string,
      &serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_deserialize(
      ctx_,
      query,
      serialization_type_,
      serialized_string,
      serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
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
  rc = tiledb_query_set_buffer(ctx_, query, "a1", write_a2, &write_a2_size);
  CHECK(rc == TILEDB_OK);
  uint64_t subarray_2[] = {2, 3, 2, 3};
  rc = tiledb_query_set_subarray(ctx_, query, subarray_2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_serialize(
      ctx_,
      query,
      serialization_type_,
      &serialized_string,
      &serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_deserialize(
      ctx_,
      query,
      serialization_type_,
      serialized_string,
      serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  // Read whole array
  int c_a[] = {1,
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
  rc = tiledb_query_set_buffer(ctx_, query, "a1", read_a, &read_a_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_serialize(
      ctx_,
      query,
      serialization_type_,
      &serialized_string,
      &serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_deserialize(
      ctx_,
      query,
      serialization_type_,
      serialized_string,
      serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  CHECK(!memcmp(c_a, read_a, sizeof(c_a)));

  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    SerializationDenseArrayFx,
    "C API: Test Serialization dense array, check if open",
    "[capi], [serialization], [dense], [dense-is-open]") {
  std::string temp_dir = FILE_URI_PREFIX + FILE_TEMP_DIR;
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
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);

  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    SerializationDenseArrayFx,
    "C API: Test Serialization dense array, get schema from opened array",
    "[capi], [serialization], [dense], [dense-get-schema]") {
  std::string temp_dir = FILE_URI_PREFIX + FILE_TEMP_DIR;
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
    SerializationDenseArrayFx,
    "C API: Test Serialization dense array, set subarray in sparse writes "
    "should error",
    "[capi], [serialization], [dense], [dense-set-subarray-sparse]") {
  std::string array_name =
      FILE_URI_PREFIX + FILE_TEMP_DIR + "dense_set_subarray_sparse";
  std::string temp_dir = FILE_URI_PREFIX + FILE_TEMP_DIR;
  create_temp_dir(temp_dir);
  create_dense_array(array_name);

  // Create TileDB context
  tiledb_ctx_t* ctx = nullptr;
  REQUIRE(tiledb_ctx_alloc(nullptr, &ctx) == TILEDB_OK);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create WRITE query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);

  uint64_t subarray[] = {1, 1, 1, 1};

  // Set some subarray BEFORE setting the layout to UNORDERED
  rc = tiledb_query_set_subarray(ctx, query, subarray);
  CHECK(rc == TILEDB_OK);

  // Set some subarray AFTER setting the layout to UNORDERED
  rc = tiledb_query_set_layout(ctx, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx, query, subarray);
  CHECK(rc == TILEDB_ERR);

  // Close array
  CHECK(tiledb_array_close(ctx, array) == TILEDB_OK);

  // Clean up
  tiledb_query_free(&query);
  tiledb_array_free(&array);
  tiledb_ctx_free(&ctx);

  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    SerializationDenseArrayFx,
    "C API: Test Serialization dense array, check if coords exist in unordered "
    "writes",
    "[capi], [serialization], [dense], [dense-coords-exist-unordered]") {
  std::string array_name =
      FILE_URI_PREFIX + FILE_TEMP_DIR + "dense_coords_exist_unordered";
  std::string temp_dir = FILE_URI_PREFIX + FILE_TEMP_DIR;
  create_temp_dir(temp_dir);
  create_dense_array(array_name);

  // Create TileDB context
  tiledb_ctx_t* ctx = nullptr;
  REQUIRE(tiledb_ctx_alloc(nullptr, &ctx) == TILEDB_OK);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create WRITE query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);

  // Set attribute buffers
  int a1[] = {1, 2};
  uint64_t a1_size = sizeof(a1);
  rc = tiledb_query_set_buffer(ctx, query, "a1", a1, &a1_size);
  CHECK(rc == TILEDB_OK);
  char a2[] = {'a', 'b'};
  uint64_t a2_size = sizeof(a2);
  uint64_t a2_off[] = {0, 1};
  uint64_t a2_off_size = sizeof(a2_off);
  rc = tiledb_query_set_buffer_var(
      ctx, query, "a2", a2_off, &a2_off_size, a2, &a2_size);
  CHECK(rc == TILEDB_OK);
  float a3[] = {1.1f, 1.2f, 2.1f, 2.2f};
  uint64_t a3_size = sizeof(a3);
  rc = tiledb_query_set_buffer(ctx, query, "a3", a3, &a3_size);
  CHECK(rc == TILEDB_OK);

  // Submit query - should error
  char* serialized_string;
  uint64_t serialized_string_length;
  rc = tiledb_query_serialize(
      ctx_,
      query,
      serialization_type_,
      &serialized_string,
      &serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_deserialize(
      ctx_,
      query,
      serialization_type_,
      serialized_string,
      serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  delete[] serialized_string;
  CHECK(tiledb_query_submit(ctx, query) == TILEDB_ERR);

  // Set coordinates
  uint64_t coords[] = {1, 2, 1, 1};
  uint64_t coords_size = sizeof(coords);
  rc = tiledb_query_set_buffer(ctx, query, TILEDB_COORDS, coords, &coords_size);
  CHECK(rc == TILEDB_OK);

  // Submit query - ok
  rc = tiledb_query_serialize(
      ctx_,
      query,
      serialization_type_,
      &serialized_string,
      &serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_deserialize(
      ctx_,
      query,
      serialization_type_,
      serialized_string,
      serialized_string_length);
  REQUIRE(rc == TILEDB_OK);
  delete[] serialized_string;
  CHECK(tiledb_query_submit(ctx, query) == TILEDB_OK);

  // Close array
  CHECK(tiledb_array_close(ctx, array) == TILEDB_OK);

  // Clean up
  tiledb_query_free(&query);
  tiledb_array_free(&array);
  tiledb_ctx_free(&ctx);

  remove_temp_dir(temp_dir);
}
