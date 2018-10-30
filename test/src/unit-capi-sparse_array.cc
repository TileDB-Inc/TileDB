/**
 * @file   unit-capi-sparse_array.cc
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
 * Tests of C API for sparse array operations.
 */

#include "catch.hpp"
#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/misc/utils.h"

#include "test/src/helpers.h"

#include <cassert>
#include <cstring>
#include <ctime>
#include <iostream>
#include <map>
#include <sstream>
#include <thread>

struct SparseArrayFx {
  // Constant parameters
  std::string ATTR_NAME = "a";
  const char* DIM1_NAME = "x";
  const char* DIM2_NAME = "y";
  const tiledb_datatype_t ATTR_TYPE = TILEDB_INT32;
  const tiledb_datatype_t DIM_TYPE = TILEDB_INT64;
  const tiledb_array_type_t ARRAY_TYPE = TILEDB_SPARSE;
  int COMPRESSION_LEVEL = -1;
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
  int ITER_NUM = 5;
  const std::string ARRAY = "sparse_array";

  tiledb_encryption_type_t encryption_type = TILEDB_NO_ENCRYPTION;
  const char* encryption_key = nullptr;

  // TileDB context
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;

  // Supported filesystems
  bool supports_s3_;
  bool supports_hdfs_;

  // Functions
  SparseArrayFx();
  ~SparseArrayFx();

  void create_sparse_array(const std::string& array_name);
  void check_sparse_array_unordered_with_duplicates_error(
      const std::string& array_name);
  void check_sparse_array_unordered_with_duplicates_no_check(
      const std::string& array_name);
  void check_sparse_array_unordered_with_duplicates_dedup(
      const std::string& array_name);
  void check_sparse_array_unordered_with_all_duplicates_dedup(
      const std::string& array_name);
  void check_sparse_array_global_with_duplicates_error(
      const std::string& array_name);
  void check_sparse_array_global_with_duplicates_no_check(
      const std::string& array_name);
  void check_sparse_array_global_with_duplicates_dedup(
      const std::string& array_name);
  void check_sparse_array_global_with_all_duplicates_dedup(
      const std::string& array_name);
  void check_sparse_array_no_results(const std::string& array_name);
  void check_non_empty_domain(const std::string& path);
  void check_invalid_offsets(const std::string& array_name);
  void write_partial_sparse_array(const std::string& array_name);
  void write_sparse_array_missing_attributes(const std::string& array_name);
  void write_sparse_array(const std::string& array_name);
  void set_supported_fs();
  void create_temp_dir(const std::string& path);
  void remove_temp_dir(const std::string& path);
  static std::string random_bucket_name(const std::string& prefix);
  void check_sorted_reads(
      const std::string& array_name,
      tiledb_filter_type_t compressor,
      tiledb_layout_t tile_order,
      tiledb_layout_t cell_order);

  /**
   * Creates a 2D sparse array.
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
  void create_sparse_array_2D(
      const std::string& array_name,
      const int64_t tile_extent_0,
      const int64_t tile_extent_1,
      const int64_t domain_0_lo,
      const int64_t domain_0_hi,
      const int64_t domain_1_lo,
      const int64_t domain_1_hi,
      const uint64_t capacity,
      const tiledb_filter_type_t compressor,
      const tiledb_layout_t cell_order,
      const tiledb_layout_t tile_order);

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
  int* read_sparse_array_2D(
      const std::string& array_name,
      const int64_t domain_0_lo,
      const int64_t domain_0_hi,
      const int64_t domain_1_lo,
      const int64_t domain_1_hi,
      const tiledb_query_type_t query_type,
      const tiledb_layout_t query_layout);

  /**
   * Write random values in unsorted mode. The buffer is initialized with each
   * cell being equalt to row_id*domain_size_1+col_id.
   *
   * @param array_name The array name.
   * @param domain_size_0 The domain size of the first dimension.
   * @param domain_size_1 The domain size of the second dimension.
   */
  void write_sparse_array_unsorted_2D(
      const std::string& array_name,
      const int64_t domain_size_0,
      const int64_t domain_size_1);

  void test_random_subarrays(
      const std::string& array_name,
      int64_t domain_size_0,
      int64_t domain_size_1,
      int iter_num);
};

SparseArrayFx::SparseArrayFx() {
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

  create_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
  if (supports_s3_)
    create_temp_dir(S3_TEMP_DIR);
  if (supports_hdfs_)
    create_temp_dir(HDFS_TEMP_DIR);
}

SparseArrayFx::~SparseArrayFx() {
  remove_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);

  if (supports_s3_) {
    remove_temp_dir(S3_TEMP_DIR);
    int is_bucket = 0;
    int rc = tiledb_vfs_is_bucket(ctx_, vfs_, S3_BUCKET.c_str(), &is_bucket);
    CHECK(rc == TILEDB_OK);
    if (is_bucket) {
      CHECK(
          tiledb_vfs_remove_bucket(ctx_, vfs_, S3_BUCKET.c_str()) == TILEDB_OK);
    }
  }
  if (supports_hdfs_)
    remove_temp_dir(HDFS_TEMP_DIR);

  tiledb_vfs_free(&vfs_);
  tiledb_ctx_free(&ctx_);
}

void SparseArrayFx::set_supported_fs() {
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

void SparseArrayFx::create_temp_dir(const std::string& path) {
  remove_temp_dir(path);
  REQUIRE(tiledb_vfs_create_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

void SparseArrayFx::remove_temp_dir(const std::string& path) {
  int is_dir = 0;
  REQUIRE(tiledb_vfs_is_dir(ctx_, vfs_, path.c_str(), &is_dir) == TILEDB_OK);
  if (is_dir)
    REQUIRE(tiledb_vfs_remove_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

std::string SparseArrayFx::random_bucket_name(const std::string& prefix) {
  std::stringstream ss;
  ss << prefix << "-" << std::this_thread::get_id() << "-"
     << TILEDB_TIMESTAMP_NOW_MS;
  return ss.str();
}

void SparseArrayFx::create_sparse_array_2D(
    const std::string& array_name,
    const int64_t tile_extent_0,
    const int64_t tile_extent_1,
    const int64_t domain_0_lo,
    const int64_t domain_0_hi,
    const int64_t domain_1_lo,
    const int64_t domain_1_hi,
    const uint64_t capacity,
    const tiledb_filter_type_t compressor,
    const tiledb_layout_t cell_order,
    const tiledb_layout_t tile_order) {
  // Prepare and set the array schema object and data structures
  int64_t dim_domain[] = {domain_0_lo, domain_0_hi, domain_1_lo, domain_1_hi};

  // Create attribute
  tiledb_attribute_t* a;
  int rc = tiledb_attribute_alloc(ctx_, ATTR_NAME.c_str(), ATTR_TYPE, &a);
  REQUIRE(rc == TILEDB_OK);

  tiledb_filter_t* filter;
  tiledb_filter_list_t* list;
  rc = tiledb_filter_alloc(ctx_, compressor, &filter);
  CHECK(rc == TILEDB_OK);
  if (compressor != TILEDB_FILTER_NONE) {
    rc = tiledb_filter_set_option(
        ctx_, filter, TILEDB_COMPRESSION_LEVEL, &COMPRESSION_LEVEL);
    CHECK(rc == TILEDB_OK);
  }
  rc = tiledb_filter_list_alloc(ctx_, &list);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_filter_list_add_filter(ctx_, list, filter);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_filter_list(ctx_, a, list);
  CHECK(rc == TILEDB_OK);

  // Create dimensions
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
  rc = tiledb_array_schema_alloc(ctx_, ARRAY_TYPE, &array_schema);
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
  if (encryption_type == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_create(ctx_, array_name.c_str(), array_schema);
  } else {
    rc = tiledb_array_create_with_key(
        ctx_,
        array_name.c_str(),
        array_schema,
        encryption_type,
        encryption_key,
        (uint32_t)strlen(encryption_key));
  }
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  tiledb_attribute_free(&a);
  tiledb_dimension_free(&d1);
  tiledb_dimension_free(&d2);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

int* SparseArrayFx::read_sparse_array_2D(
    const std::string& array_name,
    const int64_t domain_0_lo,
    const int64_t domain_0_hi,
    const int64_t domain_1_lo,
    const int64_t domain_1_hi,
    const tiledb_query_type_t query_type,
    const tiledb_layout_t query_layout) {
  // Initialize a subarray
  const int64_t subarray[] = {
      domain_0_lo, domain_0_hi, domain_1_lo, domain_1_hi};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  if (encryption_type == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_open(ctx_, array, query_type);
    CHECK(rc == TILEDB_OK);
  } else {
    rc = tiledb_array_open_with_key(
        ctx_,
        array,
        query_type,
        encryption_type,
        encryption_key,
        (uint32_t)strlen(encryption_key));
    CHECK(rc == TILEDB_OK);
  }

  // Prepare the buffers that will store the result
  uint64_t buffer_size;
  rc = tiledb_array_max_buffer_size(
      ctx_, array, ATTR_NAME.c_str(), subarray, &buffer_size);

  CHECK(rc == TILEDB_OK);
  auto buffer = new int[buffer_size / sizeof(int)];
  REQUIRE(buffer != nullptr);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, query_type, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, ATTR_NAME.c_str(), buffer, &buffer_size);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, query_layout);
  REQUIRE(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_COMPLETED);

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  // Success - return the created buffer
  return buffer;
}

void SparseArrayFx::write_sparse_array(const std::string& array_name) {
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

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  const char* attributes[] = {"a1", "a2", "a3", TILEDB_COORDS};
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
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[3], buffers[4], &buffer_sizes[4]);
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

void SparseArrayFx::write_sparse_array_unsorted_2D(
    const std::string& array_name,
    const int64_t domain_size_0,
    const int64_t domain_size_1) {
  // Generate random attribute values and coordinates for sparse write
  int64_t cell_num = domain_size_0 * domain_size_1;
  auto buffer_a1 = new int[cell_num];
  auto buffer_coords = new int64_t[2 * cell_num];
  int64_t coords_index = 0L;
  for (int64_t i = 0; i < domain_size_0; ++i) {
    for (int64_t j = 0; j < domain_size_1; ++j) {
      buffer_a1[i * domain_size_1 + j] = i * domain_size_1 + j;
      buffer_coords[2 * coords_index] = i;
      buffer_coords[2 * coords_index + 1] = j;
      coords_index++;
    }
  }

  // Prepare buffers
  void* buffers[] = {buffer_a1, buffer_coords};
  uint64_t buffer_sizes[2];
  buffer_sizes[0] = cell_num * sizeof(int);
  buffer_sizes[1] = 2 * cell_num * sizeof(int64_t);

  // Set attributes
  const char* attributes[] = {ATTR_NAME.c_str(), TILEDB_COORDS};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  if (encryption_type == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
    CHECK(rc == TILEDB_OK);
  } else {
    rc = tiledb_array_open_with_key(
        ctx_,
        array,
        TILEDB_WRITE,
        encryption_type,
        encryption_key,
        (uint32_t)strlen(encryption_key));
    CHECK(rc == TILEDB_OK);
  }

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
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  // Clean up
  delete[] buffer_a1;
  delete[] buffer_coords;
}

void SparseArrayFx::test_random_subarrays(
    const std::string& array_name,
    int64_t domain_size_0,
    int64_t domain_size_1,
    int iter_num) {
  // write array_schema cells with value = row id * columns + col id to disk
  write_sparse_array_unsorted_2D(array_name, domain_size_0, domain_size_1);

  // test random subarrays and check with corresponding value set by
  // row_id*dim1+col_id. top left corner is always 4,4.
  int64_t d0_lo = 4;
  int64_t d0_hi = 0;
  int64_t d1_lo = 4;
  int64_t d1_hi = 0;
  int64_t height = 0, width = 0;

  for (int iter = 0; iter < iter_num; ++iter) {
    height = rand() % (domain_size_0 - d0_lo);
    width = rand() % (domain_size_1 - d1_lo);
    d0_hi = d0_lo + height;
    d1_hi = d1_lo + width;
    int64_t index = 0;

    // Read subarray
    int* buffer = read_sparse_array_2D(
        array_name, d0_lo, d0_hi, d1_lo, d1_hi, TILEDB_READ, TILEDB_ROW_MAJOR);
    CHECK(buffer != NULL);

    // check
    bool allok = true;
    for (int64_t i = d0_lo; i <= d0_hi; ++i) {
      for (int64_t j = d1_lo; j <= d1_hi; ++j) {
        bool match = buffer[index] == i * domain_size_1 + j;
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
    // clean up
    delete[] buffer;
    CHECK(allok);
  }
}

void SparseArrayFx::check_sorted_reads(
    const std::string& array_name,
    tiledb_filter_type_t compressor,
    tiledb_layout_t tile_order,
    tiledb_layout_t cell_order) {
  // Parameters used in this test
  int64_t domain_size_0 = 5000;
  int64_t domain_size_1 = 1000;
  int64_t tile_extent_0 = 100;
  int64_t tile_extent_1 = 100;
  int64_t domain_0_lo = 0;
  int64_t domain_0_hi = domain_size_0 - 1;
  int64_t domain_1_lo = 0;
  int64_t domain_1_hi = domain_size_1 - 1;
  int64_t capacity = 100000;
  int iter_num = (compressor != TILEDB_FILTER_BZIP2) ? ITER_NUM : 1;

  create_sparse_array_2D(
      array_name,
      tile_extent_0,
      tile_extent_1,
      domain_0_lo,
      domain_0_hi,
      domain_1_lo,
      domain_1_hi,
      capacity,
      compressor,
      tile_order,
      cell_order);
  test_random_subarrays(array_name, domain_size_0, domain_size_1, iter_num);
}

void SparseArrayFx::create_sparse_array(const std::string& array_name) {
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
  rc = tiledb_attribute_alloc(ctx_, "a2", TILEDB_CHAR, &a2);
  CHECK(rc == TILEDB_OK);
  rc = set_attribute_compression_filter(ctx_, a2, TILEDB_FILTER_GZIP, -1);
  CHECK(rc == TILEDB_OK);
  tiledb_attribute_set_cell_val_num(ctx_, a2, TILEDB_VAR_NUM);
  tiledb_attribute_t* a3;
  rc = tiledb_attribute_alloc(ctx_, "a3", TILEDB_FLOAT32, &a3);
  CHECK(rc == TILEDB_OK);
  rc = set_attribute_compression_filter(ctx_, a3, TILEDB_FILTER_ZSTD, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a3, 2);
  CHECK(rc == TILEDB_OK);

  // Create array schema
  tiledb_array_schema_t* array_schema;
  rc = tiledb_array_schema_alloc(ctx_, TILEDB_SPARSE, &array_schema);
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

void SparseArrayFx::check_sparse_array_unordered_with_duplicates_error(
    const std::string& array_name) {
  // Prepare cell buffers
  int buffer_a1[] = {7, 5, 0, 6, 4, 3, 1, 2};
  uint64_t buffer_a2[] = {0, 4, 6, 7, 10, 11, 15, 17};
  char buffer_a2_var[] = "hhhhffagggeddddbbccc";
  float buffer_a3[] = {7.1f,
                       7.2f,
                       5.1f,
                       5.2f,
                       0.1f,
                       0.2f,
                       6.1f,
                       6.2f,
                       4.1f,
                       4.2f,
                       3.1f,
                       3.2f,
                       1.1f,
                       1.2f,
                       2.1f,
                       2.2f};
  uint64_t buffer_coords[] = {3, 4, 4, 2, 1, 1, 3, 3, 3, 3, 2, 3, 1, 2, 1, 4};
  void* buffers[] = {
      buffer_a1, buffer_a2, buffer_a2_var, buffer_a3, buffer_coords};
  uint64_t buffer_sizes[] = {sizeof(buffer_a1),
                             sizeof(buffer_a2),
                             sizeof(buffer_a2_var) - 1,
                             sizeof(buffer_a3),
                             sizeof(buffer_coords)};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  const char* attributes[] = {"a1", "a2", "a3", TILEDB_COORDS};
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
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
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[3], buffers[4], &buffer_sizes[4]);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_ERR);

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

void SparseArrayFx::check_sparse_array_unordered_with_duplicates_no_check(
    const std::string& array_name) {
  // Create TileDB context with config
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);
  REQUIRE(
      tiledb_config_set(config, "sm.check_coord_dups", "false", &error) ==
      TILEDB_OK);
  REQUIRE(error == nullptr);
  tiledb_ctx_t* ctx = nullptr;
  REQUIRE(tiledb_ctx_alloc(config, &ctx) == TILEDB_OK);
  REQUIRE(error == nullptr);
  tiledb_config_free(&config);

  // Prepare cell buffers
  int buffer_a1[] = {7, 5, 0, 6, 4, 3, 1, 2};
  uint64_t buffer_a2[] = {0, 4, 6, 7, 10, 11, 15, 17};
  char buffer_a2_var[] = "hhhhffagggeddddbbccc";
  float buffer_a3[] = {7.1f,
                       7.2f,
                       5.1f,
                       5.2f,
                       0.1f,
                       0.2f,
                       6.1f,
                       6.2f,
                       4.1f,
                       4.2f,
                       3.1f,
                       3.2f,
                       1.1f,
                       1.2f,
                       2.1f,
                       2.2f};
  uint64_t buffer_coords[] = {3, 4, 4, 2, 1, 1, 3, 3, 3, 3, 2, 3, 1, 2, 1, 4};
  void* buffers[] = {
      buffer_a1, buffer_a2, buffer_a2_var, buffer_a3, buffer_coords};
  uint64_t buffer_sizes[] = {sizeof(buffer_a1),
                             sizeof(buffer_a2),
                             sizeof(buffer_a2_var) - 1,
                             sizeof(buffer_a3),
                             sizeof(buffer_coords)};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  const char* attributes[] = {"a1", "a2", "a3", TILEDB_COORDS};
  rc = tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx, query, attributes[0], buffers[0], &buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx,
      query,
      attributes[1],
      (uint64_t*)buffers[1],
      &buffer_sizes[1],
      buffers[2],
      &buffer_sizes[2]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx, query, attributes[2], buffers[3], &buffer_sizes[3]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx, query, attributes[3], buffers[4], &buffer_sizes[4]);
  CHECK(rc == TILEDB_OK);

  // Submit query - this is unsafe but it should be passing
  rc = tiledb_query_submit(ctx, query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx, query);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_ctx_free(&ctx);
}

void SparseArrayFx::check_sparse_array_unordered_with_duplicates_dedup(
    const std::string& array_name) {
  // Create TileDB context, setting
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);
  REQUIRE(
      tiledb_config_set(config, "sm.dedup_coords", "true", &error) ==
      TILEDB_OK);
  REQUIRE(error == nullptr);
  tiledb_ctx_t* ctx = nullptr;
  REQUIRE(tiledb_ctx_alloc(config, &ctx) == TILEDB_OK);
  REQUIRE(error == nullptr);
  tiledb_config_free(&config);

  // Prepare cell buffers for WRITE
  int buffer_a1[] = {7, 5, 0, 6, 6, 3, 1, 2};
  uint64_t buffer_a2[] = {0, 4, 6, 7, 10, 13, 17, 19};
  char buffer_a2_var[] = "hhhhffaggggggddddbbccc";
  float buffer_a3[] = {7.1f,
                       7.2f,
                       5.1f,
                       5.2f,
                       0.1f,
                       0.2f,
                       6.1f,
                       6.2f,
                       6.1f,
                       6.2f,
                       3.1f,
                       3.2f,
                       1.1f,
                       1.2f,
                       2.1f,
                       2.2f};
  uint64_t buffer_coords[] = {3, 4, 4, 2, 1, 1, 3, 3, 3, 3, 2, 3, 1, 2, 1, 4};
  void* buffers[] = {
      buffer_a1, buffer_a2, buffer_a2_var, buffer_a3, buffer_coords};
  uint64_t buffer_sizes[] = {sizeof(buffer_a1),
                             sizeof(buffer_a2),
                             sizeof(buffer_a2_var) - 1,
                             sizeof(buffer_a3),
                             sizeof(buffer_coords)};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create WRITE query
  tiledb_query_t* query;
  const char* attributes[] = {"a1", "a2", "a3", TILEDB_COORDS};
  rc = tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx, query, attributes[0], buffers[0], &buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx,
      query,
      attributes[1],
      (uint64_t*)buffers[1],
      &buffer_sizes[1],
      buffers[2],
      &buffer_sizes[2]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx, query, attributes[2], buffers[3], &buffer_sizes[3]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx, query, attributes[3], buffers[4], &buffer_sizes[4]);
  CHECK(rc == TILEDB_OK);

  // Submit WRITE query
  rc = tiledb_query_submit(ctx, query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx, query);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_query_free(&query);

  // Prepare cell buffers for READ
  int r_buffer_a1[20];
  uint64_t r_buffer_a2[20];
  char r_buffer_a2_var[40];
  float r_buffer_a3[40];
  uint64_t r_buffer_coords[40];
  void* r_buffers[] = {
      r_buffer_a1, r_buffer_a2, r_buffer_a2_var, r_buffer_a3, r_buffer_coords};
  uint64_t r_buffer_sizes[] = {sizeof(r_buffer_a1),
                               sizeof(r_buffer_a2),
                               sizeof(r_buffer_a2_var),
                               sizeof(r_buffer_a3),
                               sizeof(r_buffer_coords)};

  // Open array
  rc = tiledb_array_open(ctx, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create READ query
  rc = tiledb_query_alloc(ctx, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx, query, attributes[0], r_buffers[0], &r_buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx,
      query,
      attributes[1],
      (uint64_t*)r_buffers[1],
      &r_buffer_sizes[1],
      r_buffers[2],
      &r_buffer_sizes[2]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx, query, attributes[2], r_buffers[3], &r_buffer_sizes[3]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx, query, attributes[3], r_buffers[4], &r_buffer_sizes[4]);
  CHECK(rc == TILEDB_OK);

  // Submit READ query
  rc = tiledb_query_submit(ctx, query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx, query);
  CHECK(rc == TILEDB_OK);

  // Check correctness
  int c_buffer_a1[] = {0, 1, 2, 3, 6, 7, 5};
  uint64_t c_buffer_a2[] = {0, 1, 3, 6, 10, 13, 17};
  char c_buffer_a2_var[] = "abbcccddddggghhhhff";
  uint64_t c_buffer_coords[] = {1, 1, 1, 2, 1, 4, 2, 3, 3, 3, 3, 4, 4, 2};
  float c_buffer_a3[] = {0.1f,
                         0.2f,
                         1.1f,
                         1.2f,
                         2.1f,
                         2.2f,
                         3.1f,
                         3.2f,
                         6.1f,
                         6.2f,
                         7.1f,
                         7.2f,
                         5.1f,
                         5.2f};
  CHECK(!memcmp(r_buffer_a1, c_buffer_a1, sizeof(c_buffer_a1)));
  CHECK(!memcmp(r_buffer_a2, c_buffer_a2, sizeof(c_buffer_a2)));
  CHECK(!memcmp(r_buffer_a2_var, c_buffer_a2_var, sizeof(c_buffer_a2_var) - 1));
  CHECK(!memcmp(r_buffer_a3, c_buffer_a3, sizeof(c_buffer_a3)));
  CHECK(!memcmp(r_buffer_coords, c_buffer_coords, sizeof(c_buffer_coords)));

  // Close array
  rc = tiledb_array_close(ctx, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_ctx_free(&ctx);
}

void SparseArrayFx::check_sparse_array_unordered_with_all_duplicates_dedup(
    const std::string& array_name) {
  // Create TileDB context, setting
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);
  REQUIRE(
      tiledb_config_set(config, "sm.dedup_coords", "true", &error) ==
      TILEDB_OK);
  REQUIRE(error == nullptr);
  tiledb_ctx_t* ctx = nullptr;
  REQUIRE(tiledb_ctx_alloc(config, &ctx) == TILEDB_OK);
  REQUIRE(error == nullptr);
  tiledb_config_free(&config);

  // Prepare cell buffers for WRITE
  int buffer_a1[] = {0, 0, 0, 0, 0, 0, 0, 0};
  uint64_t buffer_a2[] = {0, 1, 2, 3, 4, 5, 6, 7};
  char buffer_a2_var[] = "aaaaaaaa";
  float buffer_a3[] = {0.1f,
                       0.2f,
                       0.1f,
                       0.2f,
                       0.1f,
                       0.2f,
                       0.1f,
                       0.2f,
                       0.1f,
                       0.2f,
                       0.1f,
                       0.2f,
                       0.1f,
                       0.2f,
                       0.1f,
                       0.2f};
  uint64_t buffer_coords[] = {3, 4, 3, 4, 3, 4, 3, 4, 3, 4, 3, 4, 3, 4, 3, 4};
  void* buffers[] = {
      buffer_a1, buffer_a2, buffer_a2_var, buffer_a3, buffer_coords};
  uint64_t buffer_sizes[] = {sizeof(buffer_a1),
                             sizeof(buffer_a2),
                             sizeof(buffer_a2_var) - 1,
                             sizeof(buffer_a3),
                             sizeof(buffer_coords)};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create WRITE query
  tiledb_query_t* query;
  const char* attributes[] = {"a1", "a2", "a3", TILEDB_COORDS};
  rc = tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx, query, attributes[0], buffers[0], &buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx,
      query,
      attributes[1],
      (uint64_t*)buffers[1],
      &buffer_sizes[1],
      buffers[2],
      &buffer_sizes[2]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx, query, attributes[2], buffers[3], &buffer_sizes[3]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx, query, attributes[3], buffers[4], &buffer_sizes[4]);
  CHECK(rc == TILEDB_OK);

  // Submit WRITE query
  rc = tiledb_query_submit(ctx, query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx, query);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_query_free(&query);

  // Prepare cell buffers for READ
  int r_buffer_a1[20];
  uint64_t r_buffer_a2[20];
  char r_buffer_a2_var[40];
  float r_buffer_a3[40];
  uint64_t r_buffer_coords[40];
  void* r_buffers[] = {
      r_buffer_a1, r_buffer_a2, r_buffer_a2_var, r_buffer_a3, r_buffer_coords};
  uint64_t r_buffer_sizes[] = {sizeof(r_buffer_a1),
                               sizeof(r_buffer_a2),
                               sizeof(r_buffer_a2_var),
                               sizeof(r_buffer_a3),
                               sizeof(r_buffer_coords)};

  // Open array
  rc = tiledb_array_open(ctx, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create READ query
  rc = tiledb_query_alloc(ctx, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx, query, attributes[0], r_buffers[0], &r_buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx,
      query,
      attributes[1],
      (uint64_t*)r_buffers[1],
      &r_buffer_sizes[1],
      r_buffers[2],
      &r_buffer_sizes[2]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx, query, attributes[2], r_buffers[3], &r_buffer_sizes[3]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx, query, attributes[3], r_buffers[4], &r_buffer_sizes[4]);
  CHECK(rc == TILEDB_OK);

  // Submit READ query
  rc = tiledb_query_submit(ctx, query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx, query);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  // Check correctness
  int c_buffer_a1[] = {0};
  uint64_t c_buffer_a2[] = {0};
  char c_buffer_a2_var[] = "a";
  float c_buffer_a3[] = {0.1f, 0.2f};
  uint64_t c_buffer_coords[] = {3, 4};
  CHECK(!memcmp(r_buffer_a1, c_buffer_a1, sizeof(c_buffer_a1)));
  CHECK(!memcmp(r_buffer_a2, c_buffer_a2, sizeof(c_buffer_a2)));
  CHECK(!memcmp(r_buffer_a2_var, c_buffer_a2_var, sizeof(c_buffer_a2_var) - 1));
  CHECK(!memcmp(r_buffer_a3, c_buffer_a3, sizeof(c_buffer_a3)));
  CHECK(!memcmp(r_buffer_coords, c_buffer_coords, sizeof(c_buffer_coords)));

  tiledb_ctx_free(&ctx);
}

void SparseArrayFx::check_sparse_array_global_with_duplicates_error(
    const std::string& array_name) {
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
  uint64_t buffer_coords[] = {1, 1, 1, 2, 1, 4, 2, 3, 4, 2, 3, 3, 3, 3, 3, 4};
  void* buffers[] = {
      buffer_a1, buffer_a2, buffer_var_a2, buffer_a3, buffer_coords};
  uint64_t buffer_sizes[] = {sizeof(buffer_a1),
                             sizeof(buffer_a2),
                             sizeof(buffer_var_a2) - 1,
                             sizeof(buffer_a3),
                             sizeof(buffer_coords)};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  const char* attributes[] = {"a1", "a2", "a3", TILEDB_COORDS};
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
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[3], buffers[4], &buffer_sizes[4]);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_ERR);
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void SparseArrayFx::check_sparse_array_global_with_duplicates_no_check(
    const std::string& array_name) {
  // Create TileDB context, setting
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);
  REQUIRE(
      tiledb_config_set(config, "sm.check_coord_dups", "false", &error) ==
      TILEDB_OK);
  REQUIRE(error == nullptr);
  tiledb_ctx_t* ctx = nullptr;
  REQUIRE(tiledb_ctx_alloc(config, &ctx) == TILEDB_OK);
  REQUIRE(error == nullptr);
  tiledb_config_free(&config);

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
  uint64_t buffer_coords[] = {1, 1, 1, 2, 1, 4, 2, 3, 4, 2, 3, 3, 3, 3, 3, 4};
  void* buffers[] = {
      buffer_a1, buffer_a2, buffer_var_a2, buffer_a3, buffer_coords};
  uint64_t buffer_sizes[] = {sizeof(buffer_a1),
                             sizeof(buffer_a2),
                             sizeof(buffer_var_a2) - 1,
                             sizeof(buffer_a3),
                             sizeof(buffer_coords)};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  const char* attributes[] = {"a1", "a2", "a3", TILEDB_COORDS};
  rc = tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx, query, attributes[0], buffers[0], &buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx,
      query,
      attributes[1],
      (uint64_t*)buffers[1],
      &buffer_sizes[1],
      buffers[2],
      &buffer_sizes[2]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx, query, attributes[2], buffers[3], &buffer_sizes[3]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx, query, attributes[3], buffers[4], &buffer_sizes[4]);
  CHECK(rc == TILEDB_OK);

  // Submit query - this is unsafe but it should be passing
  rc = tiledb_query_submit(ctx, query);
  CHECK(rc == TILEDB_OK);

  // Finalize query
  rc = tiledb_query_finalize(ctx, query);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_ctx_free(&ctx);
}

void SparseArrayFx::check_sparse_array_global_with_duplicates_dedup(
    const std::string& array_name) {
  // Create TileDB context, setting
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);
  REQUIRE(
      tiledb_config_set(config, "sm.dedup_coords", "true", &error) ==
      TILEDB_OK);
  REQUIRE(error == nullptr);
  tiledb_ctx_t* ctx = nullptr;
  REQUIRE(tiledb_ctx_alloc(config, &ctx) == TILEDB_OK);
  REQUIRE(error == nullptr);
  tiledb_config_free(&config);

  // Prepare cell buffers for WRITE
  int buffer_a1[] = {0, 1, 2, 3, 4, 5, 5, 7};
  uint64_t buffer_a2[] = {0, 1, 3, 6, 10, 11, 14, 17};
  char buffer_var_a2[] = "abbcccddddegggggghhhh";
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
                       5.1f,
                       5.2f,
                       7.1f,
                       7.2f};
  uint64_t buffer_coords[] = {1, 1, 1, 2, 1, 4, 2, 3, 4, 2, 3, 3, 3, 3, 3, 4};
  void* buffers[] = {
      buffer_a1, buffer_a2, buffer_var_a2, buffer_a3, buffer_coords};
  uint64_t buffer_sizes[] = {sizeof(buffer_a1),
                             sizeof(buffer_a2),
                             sizeof(buffer_var_a2) - 1,
                             sizeof(buffer_a3),
                             sizeof(buffer_coords)};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create WRITE query
  tiledb_query_t* query;
  const char* attributes[] = {"a1", "a2", "a3", TILEDB_COORDS};
  rc = tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx, query, attributes[0], buffers[0], &buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx,
      query,
      attributes[1],
      (uint64_t*)buffers[1],
      &buffer_sizes[1],
      buffers[2],
      &buffer_sizes[2]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx, query, attributes[2], buffers[3], &buffer_sizes[3]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx, query, attributes[3], buffers[4], &buffer_sizes[4]);
  CHECK(rc == TILEDB_OK);

  // Submit WRITE query
  rc = tiledb_query_submit(ctx, query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx, query);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_query_free(&query);

  // Prepare cell buffers for READ
  int r_buffer_a1[20];
  uint64_t r_buffer_a2[20];
  char r_buffer_a2_var[40];
  float r_buffer_a3[40];
  uint64_t r_buffer_coords[40];
  void* r_buffers[] = {
      r_buffer_a1, r_buffer_a2, r_buffer_a2_var, r_buffer_a3, r_buffer_coords};
  uint64_t r_buffer_sizes[] = {sizeof(r_buffer_a1),
                               sizeof(r_buffer_a2),
                               sizeof(r_buffer_a2_var),
                               sizeof(r_buffer_a3),
                               sizeof(r_buffer_coords)};

  // Open array
  rc = tiledb_array_open(ctx, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create READ query
  rc = tiledb_query_alloc(ctx, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx, query, attributes[0], r_buffers[0], &r_buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx,
      query,
      attributes[1],
      (uint64_t*)r_buffers[1],
      &r_buffer_sizes[1],
      r_buffers[2],
      &r_buffer_sizes[2]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx, query, attributes[2], r_buffers[3], &r_buffer_sizes[3]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx, query, attributes[3], r_buffers[4], &r_buffer_sizes[4]);
  CHECK(rc == TILEDB_OK);

  // Submit READ query
  rc = tiledb_query_submit(ctx, query);
  CHECK(rc == TILEDB_OK);

  // Finalize query
  rc = tiledb_query_finalize(ctx, query);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  // Check correctness
  int c_buffer_a1[] = {0, 1, 2, 3, 4, 5, 7};
  uint64_t c_buffer_a2[] = {0, 1, 3, 6, 10, 11, 14};
  char c_buffer_a2_var[] = "abbcccddddeggghhhh";
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
                         7.1f,
                         7.2f};
  uint64_t c_buffer_coords[] = {1, 1, 1, 2, 1, 4, 2, 3, 4, 2, 3, 3, 3, 4};

  CHECK(!memcmp(r_buffer_a1, c_buffer_a1, sizeof(c_buffer_a1)));
  CHECK(!memcmp(r_buffer_a2, c_buffer_a2, sizeof(c_buffer_a2)));
  CHECK(!memcmp(r_buffer_a2_var, c_buffer_a2_var, sizeof(c_buffer_a2_var) - 1));
  CHECK(!memcmp(r_buffer_a3, c_buffer_a3, sizeof(c_buffer_a3)));
  CHECK(!memcmp(r_buffer_coords, c_buffer_coords, sizeof(c_buffer_coords)));

  tiledb_ctx_free(&ctx);
}

void SparseArrayFx::check_sparse_array_global_with_all_duplicates_dedup(
    const std::string& array_name) {
  // Create TileDB context, setting
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);
  REQUIRE(
      tiledb_config_set(config, "sm.dedup_coords", "true", &error) ==
      TILEDB_OK);
  REQUIRE(error == nullptr);
  tiledb_ctx_t* ctx = nullptr;
  REQUIRE(tiledb_ctx_alloc(config, &ctx) == TILEDB_OK);
  REQUIRE(error == nullptr);
  tiledb_config_free(&config);

  // Prepare cell buffers for WRITE
  int buffer_a1[] = {0, 0, 0, 0, 0, 0, 0, 0};
  uint64_t buffer_a2[] = {0, 1, 2, 3, 4, 5, 6, 7};
  char buffer_var_a2[] = "aaaaaaaa";
  float buffer_a3[] = {0.1f,
                       0.2f,
                       0.1f,
                       0.2f,
                       0.1f,
                       0.2f,
                       0.1f,
                       0.2f,
                       0.1f,
                       0.2f,
                       0.1f,
                       0.2f,
                       0.1f,
                       0.2f,
                       0.1f,
                       0.2f};
  uint64_t buffer_coords[] = {1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2};
  void* buffers[] = {
      buffer_a1, buffer_a2, buffer_var_a2, buffer_a3, buffer_coords};
  uint64_t buffer_sizes[] = {sizeof(buffer_a1),
                             sizeof(buffer_a2),
                             sizeof(buffer_var_a2) - 1,
                             sizeof(buffer_a3),
                             sizeof(buffer_coords)};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create WRITE query
  tiledb_query_t* query;
  const char* attributes[] = {"a1", "a2", "a3", TILEDB_COORDS};
  rc = tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx, query, attributes[0], buffers[0], &buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx,
      query,
      attributes[1],
      (uint64_t*)buffers[1],
      &buffer_sizes[1],
      buffers[2],
      &buffer_sizes[2]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx, query, attributes[2], buffers[3], &buffer_sizes[3]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx, query, attributes[3], buffers[4], &buffer_sizes[4]);
  CHECK(rc == TILEDB_OK);

  // Submit WRITE query
  rc = tiledb_query_submit(ctx, query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx, query);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_query_free(&query);

  // Prepare cell buffers for READ
  int r_buffer_a1[20];
  uint64_t r_buffer_a2[20];
  char r_buffer_a2_var[40];
  float r_buffer_a3[40];
  uint64_t r_buffer_coords[40];
  void* r_buffers[] = {
      r_buffer_a1, r_buffer_a2, r_buffer_a2_var, r_buffer_a3, r_buffer_coords};
  uint64_t r_buffer_sizes[] = {sizeof(r_buffer_a1),
                               sizeof(r_buffer_a2),
                               sizeof(r_buffer_a2_var),
                               sizeof(r_buffer_a3),
                               sizeof(r_buffer_coords)};

  // Open array
  rc = tiledb_array_open(ctx, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create READ query
  rc = tiledb_query_alloc(ctx, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx, query, attributes[0], r_buffers[0], &r_buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx,
      query,
      attributes[1],
      (uint64_t*)r_buffers[1],
      &r_buffer_sizes[1],
      r_buffers[2],
      &r_buffer_sizes[2]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx, query, attributes[2], r_buffers[3], &r_buffer_sizes[3]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx, query, attributes[3], r_buffers[4], &r_buffer_sizes[4]);
  CHECK(rc == TILEDB_OK);

  // Submit READ query
  rc = tiledb_query_submit(ctx, query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx, query);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  // Check correctness
  int c_buffer_a1[] = {0};
  uint64_t c_buffer_a2[] = {0};
  char c_buffer_a2_var[] = "a";
  float c_buffer_a3[] = {0.1f, 0.2f};
  uint64_t c_buffer_coords[] = {1, 2};

  CHECK(!memcmp(r_buffer_a1, c_buffer_a1, sizeof(c_buffer_a1)));
  CHECK(!memcmp(r_buffer_a2, c_buffer_a2, sizeof(c_buffer_a2)));
  CHECK(!memcmp(r_buffer_a2_var, c_buffer_a2_var, sizeof(c_buffer_a2_var) - 1));
  CHECK(!memcmp(r_buffer_a3, c_buffer_a3, sizeof(c_buffer_a3)));
  CHECK(!memcmp(r_buffer_coords, c_buffer_coords, sizeof(c_buffer_coords)));

  tiledb_ctx_free(&ctx);
}

void SparseArrayFx::check_non_empty_domain(const std::string& path) {
  std::string array_name = path + "sparse_non_empty";
  create_sparse_array(array_name);

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
  write_partial_sparse_array(array_name);

  // Check non-empty domain
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  rc = tiledb_array_get_non_empty_domain(ctx_, array, domain, &is_empty);
  CHECK(rc == TILEDB_OK);
  CHECK(is_empty == 0);
  uint64_t c_domain[] = {3, 4, 2, 4};
  CHECK(!memcmp(domain, c_domain, sizeof(c_domain)));
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
}

void SparseArrayFx::check_invalid_offsets(const std::string& array_name) {
  const char* attributes[] = {"a2", TILEDB_COORDS};
  uint64_t buffer_a2[] = {0, 4, 6};
  char buffer_a2_var[] = "hhhhffa";
  uint64_t buffer_coords[] = {3, 4, 4, 2, 3, 3};
  void* buffers[] = {buffer_a2, buffer_a2_var, buffer_coords};
  uint64_t buffer_sizes[] = {
      sizeof(buffer_a2), sizeof(buffer_a2_var) - 1, sizeof(buffer_coords)};

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
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);

  // Check duplicate offsets error
  buffer_a2[0] = 0;
  buffer_a2[1] = 4;
  buffer_a2[2] = 4;
  rc = tiledb_query_set_buffer_var(
      ctx_,
      query,
      "a2",
      (uint64_t*)buffers[0],
      &buffer_sizes[0],
      buffers[1],
      &buffer_sizes[1]);
  CHECK(rc == TILEDB_ERR);

  // Check non-ascending offsets error
  buffer_a2[0] = 0;
  buffer_a2[1] = 6;
  buffer_a2[2] = 4;
  rc = tiledb_query_set_buffer_var(
      ctx_,
      query,
      "a2",
      (uint64_t*)buffers[0],
      &buffer_sizes[0],
      buffers[1],
      &buffer_sizes[1]);
  CHECK(rc == TILEDB_ERR);

  // Check out-of-bounds offsets error
  buffer_a2[0] = 0;
  buffer_a2[1] = 4;
  buffer_a2[2] = 7;
  rc = tiledb_query_set_buffer_var(
      ctx_,
      query,
      "a2",
      (uint64_t*)buffers[0],
      &buffer_sizes[0],
      buffers[1],
      &buffer_sizes[1]);
  CHECK(rc == TILEDB_ERR);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void SparseArrayFx::check_sparse_array_no_results(
    const std::string& array_name) {
  // Initialize a subarray
  const int64_t subarray[] = {1, 2, 1, 2};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Prepare the buffers that will store the result
  uint64_t buffer_size;
  rc = tiledb_array_max_buffer_size(ctx_, array, "a1", subarray, &buffer_size);
  CHECK(rc == TILEDB_OK);

  auto buffer = new int[buffer_size / sizeof(int)];
  REQUIRE(buffer != nullptr);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", buffer, &buffer_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);

  // Check that it has no results before submission
  int has_results;
  rc = tiledb_query_has_results(ctx_, query, &has_results);
  CHECK(rc == TILEDB_OK);
  CHECK(!has_results);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Check that it has no results also after submission
  rc = tiledb_query_has_results(ctx_, query, &has_results);
  CHECK(rc == TILEDB_OK);
  CHECK(!has_results);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_COMPLETED);

  // Finalize query
  rc = tiledb_query_finalize(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void SparseArrayFx::write_partial_sparse_array(const std::string& array_name) {
  // Prepare cell buffers
  int buffer_a1[] = {7, 5, 0};
  uint64_t buffer_a2[] = {0, 4, 6};
  char buffer_a2_var[] = "hhhhffa";
  float buffer_a3[] = {7.1f, 7.2f, 5.1f, 5.2f, 0.1f, 0.2f};
  uint64_t buffer_coords[] = {3, 4, 4, 2, 3, 3};
  void* buffers[] = {
      buffer_a1, buffer_a2, buffer_a2_var, buffer_a3, buffer_coords};
  uint64_t buffer_sizes[] = {sizeof(buffer_a1),
                             sizeof(buffer_a2),
                             sizeof(buffer_a2_var) - 1,
                             sizeof(buffer_a3),
                             sizeof(buffer_coords)};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  const char* attributes[] = {"a1", "a2", "a3", TILEDB_COORDS};
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
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
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[3], buffers[4], &buffer_sizes[4]);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void SparseArrayFx::write_sparse_array_missing_attributes(
    const std::string& array_name) {
  // Prepare cell buffers
  int buffer_a1[] = {7, 5, 0};
  uint64_t buffer_a2[] = {0, 4, 6};
  char buffer_a2_var[] = "hhhhffa";
  float buffer_a3[] = {7.1f, 7.2f, 5.1f, 5.2f, 0.1f, 0.2f};
  uint64_t buffer_coords[] = {3, 4, 4, 2, 3, 3};
  void* buffers[] = {
      buffer_a1, buffer_a2, buffer_a2_var, buffer_a3, buffer_coords};
  uint64_t buffer_sizes[] = {sizeof(buffer_a1),
                             sizeof(buffer_a2),
                             sizeof(buffer_a2_var) - 1,
                             sizeof(buffer_a3),
                             sizeof(buffer_coords)};

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  const char* attributes[] = {"a1", "a2", "a3", TILEDB_COORDS};
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
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
  // Omit setting the coordinates

  // Submit query - This should fail
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_ERR);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

TEST_CASE_METHOD(
    SparseArrayFx, "C API: Test sparse sorted reads", "[capi], [sparse]") {
  std::string array_name;

  SECTION("- no compression, row/row-major") {
    if (supports_s3_) {
      // S3
      array_name = S3_TEMP_DIR + ARRAY;
      check_sorted_reads(
          array_name, TILEDB_FILTER_NONE, TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
    } else if (supports_hdfs_) {
      // HDFS
      array_name = HDFS_TEMP_DIR + ARRAY;
      check_sorted_reads(
          array_name, TILEDB_FILTER_NONE, TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
    } else {
      // File
      array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + ARRAY;
      check_sorted_reads(
          array_name, TILEDB_FILTER_NONE, TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
    }
  }

  SECTION("- no compression, col/col-major") {
    if (supports_s3_) {
      // S3
      array_name = S3_TEMP_DIR + ARRAY;
      check_sorted_reads(
          array_name, TILEDB_FILTER_NONE, TILEDB_COL_MAJOR, TILEDB_COL_MAJOR);
    } else if (supports_hdfs_) {
      // HDFS
      array_name = HDFS_TEMP_DIR + ARRAY;
      check_sorted_reads(
          array_name, TILEDB_FILTER_NONE, TILEDB_COL_MAJOR, TILEDB_COL_MAJOR);
    } else {
      // File
      array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + ARRAY;
      check_sorted_reads(
          array_name, TILEDB_FILTER_NONE, TILEDB_COL_MAJOR, TILEDB_COL_MAJOR);
    }
  }

  SECTION("- no compression, row/col-major") {
    if (supports_s3_) {
      // S3
      array_name = S3_TEMP_DIR + ARRAY;
      check_sorted_reads(
          array_name, TILEDB_FILTER_NONE, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
    } else if (supports_hdfs_) {
      // HDFS
      array_name = HDFS_TEMP_DIR + ARRAY;
      check_sorted_reads(
          array_name, TILEDB_FILTER_NONE, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
    } else {
      // File
      array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + ARRAY;
      check_sorted_reads(
          array_name, TILEDB_FILTER_NONE, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
    }
  }

  SECTION("- gzip compression, row/row-major") {
    if (supports_s3_) {
      // S3
      array_name = S3_TEMP_DIR + ARRAY;
      check_sorted_reads(
          array_name, TILEDB_FILTER_GZIP, TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
    } else if (supports_hdfs_) {
      // HDFS
      array_name = HDFS_TEMP_DIR + ARRAY;
      check_sorted_reads(
          array_name, TILEDB_FILTER_GZIP, TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
    } else {
      // File
      array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + ARRAY;
      check_sorted_reads(
          array_name, TILEDB_FILTER_GZIP, TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
    }
  }

  SECTION("- gzip compression, col/col-major") {
    if (supports_s3_) {
      // S3
      array_name = S3_TEMP_DIR + ARRAY;
      check_sorted_reads(
          array_name, TILEDB_FILTER_GZIP, TILEDB_COL_MAJOR, TILEDB_COL_MAJOR);
    } else if (supports_hdfs_) {
      // HDFS
      array_name = HDFS_TEMP_DIR + ARRAY;
      check_sorted_reads(
          array_name, TILEDB_FILTER_GZIP, TILEDB_COL_MAJOR, TILEDB_COL_MAJOR);
    } else {
      // File
      array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + ARRAY;
      check_sorted_reads(
          array_name, TILEDB_FILTER_GZIP, TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
    }
  }

  SECTION("- gzip compression, row/col-major") {
    if (supports_s3_) {
      // S3
      array_name = S3_TEMP_DIR + ARRAY;
      check_sorted_reads(
          array_name, TILEDB_FILTER_GZIP, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
    } else if (supports_hdfs_) {
      // HDFS
      array_name = HDFS_TEMP_DIR + ARRAY;
      check_sorted_reads(
          array_name, TILEDB_FILTER_GZIP, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
    } else {
      // File
      array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + ARRAY;
      check_sorted_reads(
          array_name, TILEDB_FILTER_GZIP, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
    }
  }

  SECTION("- bzip compression, row/col-major") {
    if (supports_s3_) {
      // S3
      array_name = S3_TEMP_DIR + ARRAY;
      check_sorted_reads(
          array_name, TILEDB_FILTER_BZIP2, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
    } else if (supports_hdfs_) {
      // HDFS
      array_name = HDFS_TEMP_DIR + ARRAY;
      check_sorted_reads(
          array_name, TILEDB_FILTER_BZIP2, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
    } else {
      // File
      array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + ARRAY;
      check_sorted_reads(
          array_name, TILEDB_FILTER_BZIP2, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
    }
  }

  SECTION("- lz4 compression, row/col-major") {
    if (supports_s3_) {
      // S3
      array_name = S3_TEMP_DIR + ARRAY;
      check_sorted_reads(
          array_name, TILEDB_FILTER_LZ4, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
    } else if (supports_hdfs_) {
      // HDFS
      array_name = HDFS_TEMP_DIR + ARRAY;
      check_sorted_reads(
          array_name, TILEDB_FILTER_LZ4, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
    } else {
      // File
      array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + ARRAY;
      check_sorted_reads(
          array_name, TILEDB_FILTER_LZ4, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
    }
  }

  SECTION("- rle compression, row/col-major") {
    if (supports_s3_) {
      // S3
      array_name = S3_TEMP_DIR + ARRAY;
      check_sorted_reads(
          array_name, TILEDB_FILTER_RLE, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
    } else if (supports_hdfs_) {
      // HDFS
      array_name = HDFS_TEMP_DIR + ARRAY;
      check_sorted_reads(
          array_name, TILEDB_FILTER_RLE, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
    } else {
      // File
      array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + ARRAY;
      check_sorted_reads(
          array_name, TILEDB_FILTER_RLE, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
    }
  }

  SECTION("- zstd compression, row/col-major") {
    if (supports_s3_) {
      // S3
      array_name = S3_TEMP_DIR + ARRAY;
      check_sorted_reads(
          array_name, TILEDB_FILTER_ZSTD, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
    } else if (supports_hdfs_) {
      // HDFS
      array_name = HDFS_TEMP_DIR + ARRAY;
      check_sorted_reads(
          array_name, TILEDB_FILTER_ZSTD, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
    } else {
      // File
      array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + ARRAY;
      check_sorted_reads(
          array_name, TILEDB_FILTER_ZSTD, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
    }
  }

  SECTION("- double-delta compression, row/col-major") {
    if (supports_s3_) {
      // S3
      array_name = S3_TEMP_DIR + ARRAY;
      check_sorted_reads(
          array_name,
          TILEDB_FILTER_DOUBLE_DELTA,
          TILEDB_ROW_MAJOR,
          TILEDB_COL_MAJOR);
    } else if (supports_hdfs_) {
      // HDFS
      array_name = HDFS_TEMP_DIR + ARRAY;
      check_sorted_reads(
          array_name,
          TILEDB_FILTER_DOUBLE_DELTA,
          TILEDB_ROW_MAJOR,
          TILEDB_COL_MAJOR);
    } else {
      // File
      array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + ARRAY;
      check_sorted_reads(
          array_name,
          TILEDB_FILTER_DOUBLE_DELTA,
          TILEDB_ROW_MAJOR,
          TILEDB_COL_MAJOR);
    }
  }
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse duplicates",
    "[capi], [sparse], [sparse-dups]") {
  std::string array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + "dups";
  create_sparse_array(array_name);

  SECTION("- unordered, error check") {
    check_sparse_array_unordered_with_duplicates_error(array_name);
  }

  SECTION("- unordered, no error check") {
    check_sparse_array_unordered_with_duplicates_no_check(array_name);
  }

  SECTION("- unordered, dedup") {
    check_sparse_array_unordered_with_duplicates_dedup(array_name);
  }

  SECTION("- unordered, all duplicates, dedup") {
    check_sparse_array_unordered_with_all_duplicates_dedup(array_name);
  }

  SECTION("- global, error check") {
    check_sparse_array_global_with_duplicates_error(array_name);
  }

  SECTION("- global, no error check") {
    check_sparse_array_global_with_duplicates_no_check(array_name);
  }

  SECTION("- global, dedup") {
    check_sparse_array_global_with_duplicates_dedup(array_name);
  }

  SECTION("- global, all duplicates, dedup") {
    check_sparse_array_global_with_all_duplicates_dedup(array_name);
  }
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, non-empty domain",
    "[capi], [sparse], [sparse-non-empty]") {
  std::string temp_dir = FILE_URI_PREFIX + FILE_TEMP_DIR;
  create_temp_dir(temp_dir);
  check_non_empty_domain(temp_dir);
  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, invalid offsets on write",
    "[capi], [sparse], [invalid-offsets]") {
  std::string array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + "invalid_offs";
  create_sparse_array(array_name);
  check_invalid_offsets(array_name);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, anonymous attribute",
    "[capi], [sparse], [anon-attr], [sparse-anon-attr]") {
  ATTR_NAME = "";
  std::string array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + "anon_attr";
  check_sorted_reads(
      array_name, TILEDB_FILTER_NONE, TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, no results",
    "[capi], [sparse], [no-results]") {
  std::string array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + "no_results";
  create_sparse_array(array_name);
  write_partial_sparse_array(array_name);
  check_sparse_array_no_results(array_name);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, missing attributes in writes",
    "[capi], [sparse], [sparse-write-missing-attributes]") {
  std::string array_name =
      FILE_URI_PREFIX + FILE_TEMP_DIR + "sparse_write_missing_attributes";
  create_sparse_array(array_name);
  write_sparse_array_missing_attributes(array_name);
  check_sparse_array_no_results(array_name);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, set subarray should error",
    "[capi], [sparse], [sparse-set-subarray]") {
  std::string array_name =
      FILE_URI_PREFIX + FILE_TEMP_DIR + "sparse_set_subarray";
  create_sparse_array(array_name);

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

  // Set some subarray
  uint64_t subarray[] = {1, 1, 1, 1};
  rc = tiledb_query_set_subarray(ctx, query, subarray);
  CHECK(rc == TILEDB_ERR);

  // Close array
  CHECK(tiledb_array_close(ctx, array) == TILEDB_OK);

  // Clean up
  tiledb_query_free(&query);
  tiledb_array_free(&array);
  tiledb_ctx_free(&ctx);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, check if coords exist",
    "[capi], [sparse], [sparse-coords-exist]") {
  std::string array_name =
      FILE_URI_PREFIX + FILE_TEMP_DIR + "sparse_coords_exist";
  create_sparse_array(array_name);

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
  rc = tiledb_query_set_layout(ctx, query, TILEDB_GLOBAL_ORDER);
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
  CHECK(tiledb_query_submit(ctx, query) == TILEDB_ERR);

  // Set coordinates
  uint64_t coords[] = {1, 1, 1, 2};
  uint64_t coords_size = sizeof(coords);
  rc = tiledb_query_set_buffer(ctx, query, TILEDB_COORDS, coords, &coords_size);
  CHECK(rc == TILEDB_OK);

  // Submit query - ok
  CHECK(tiledb_query_submit(ctx, query) == TILEDB_OK);

  // Close array
  CHECK(tiledb_array_close(ctx, array) == TILEDB_OK);

  // Clean up
  tiledb_query_free(&query);
  tiledb_array_free(&array);
  tiledb_ctx_free(&ctx);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, global order check on write",
    "[capi], [sparse], [sparse-write-global-check]") {
  std::string array_name =
      FILE_URI_PREFIX + FILE_TEMP_DIR + "sparse_write_global_check";
  create_sparse_array(array_name);

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
  rc = tiledb_query_set_layout(ctx, query, TILEDB_GLOBAL_ORDER);
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

  // Set coordinates
  uint64_t coords[] = {1, 2, 1, 1};
  uint64_t coords_size = sizeof(coords);
  rc = tiledb_query_set_buffer(ctx, query, TILEDB_COORDS, coords, &coords_size);
  CHECK(rc == TILEDB_OK);

  // Submit query - ok
  CHECK(tiledb_query_submit(ctx, query) == TILEDB_ERR);

  // Close array
  CHECK(tiledb_array_close(ctx, array) == TILEDB_OK);

  // Clean up
  tiledb_query_free(&query);
  tiledb_array_free(&array);
  tiledb_ctx_free(&ctx);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, invalidate cached max buffer sizes",
    "[capi], [sparse], [sparse-invalidate-max-sizes]") {
  std::string array_name =
      FILE_URI_PREFIX + FILE_TEMP_DIR + "sparse_invalidate_max_sizes";
  create_sparse_array(array_name);
  write_sparse_array(array_name);

  // Create TileDB context
  tiledb_ctx_t* ctx = nullptr;
  REQUIRE(tiledb_ctx_alloc(nullptr, &ctx) == TILEDB_OK);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // ---- First READ query (empty)
  tiledb_query_t* empty_query;
  rc = tiledb_query_alloc(ctx, array, TILEDB_READ, &empty_query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx, empty_query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);

  // Get max buffer sizes for empty query
  uint64_t subarray[] = {1, 1, 3, 3};
  uint64_t a1_size, a2_off_size, a2_size, a3_size, coords_size;
  rc = tiledb_array_max_buffer_size(ctx_, array, "a1", subarray, &a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size_var(
      ctx_, array, "a2", subarray, &a2_off_size, &a2_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size(ctx_, array, "a3", subarray, &a3_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size(
      ctx_, array, TILEDB_COORDS, subarray, &coords_size);
  CHECK(rc == TILEDB_OK);

  // Set attribute buffers
  auto a1 = (int*)malloc(a1_size);
  rc = tiledb_query_set_buffer(ctx, empty_query, "a1", a1, &a1_size);
  CHECK(rc == TILEDB_OK);
  auto a2_off = (uint64_t*)malloc(a2_off_size);
  auto a2 = (char*)malloc(a2_size);
  rc = tiledb_query_set_buffer_var(
      ctx, empty_query, "a2", a2_off, &a2_off_size, a2, &a2_size);
  CHECK(rc == TILEDB_OK);
  auto a3 = (float*)malloc(a3_size);
  rc = tiledb_query_set_buffer(ctx, empty_query, "a3", a3, &a3_size);
  CHECK(rc == TILEDB_OK);
  auto coords = (uint64_t*)malloc(coords_size);
  rc = tiledb_query_set_buffer(
      ctx, empty_query, TILEDB_COORDS, coords, &coords_size);
  CHECK(rc == TILEDB_OK);

  // Set subarray
  rc = tiledb_query_set_subarray(ctx_, empty_query, subarray);
  CHECK(rc == TILEDB_OK);

  // Submit query
  CHECK(tiledb_query_submit(ctx, empty_query) == TILEDB_OK);

  // Check that there are no results
  CHECK(a1_size == 0);
  CHECK(a2_off_size == 0);
  CHECK(a2_size == 0);
  CHECK(a3_size == 0);

  // Clean up
  free(a1);
  free(a2_off);
  free(a2);
  free(a3);
  tiledb_query_free(&empty_query);

  // ---- Second READ query (non-empty)
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);

  // Get max buffer sizes for non-empty query
  uint64_t subarray_2[] = {1, 1, 1, 2};
  rc = tiledb_array_max_buffer_size(ctx_, array, "a1", subarray_2, &a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size_var(
      ctx_, array, "a2", subarray_2, &a2_off_size, &a2_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size(ctx_, array, "a3", subarray_2, &a3_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size(
      ctx_, array, TILEDB_COORDS, subarray_2, &coords_size);
  CHECK(rc == TILEDB_OK);

  // Set attribute buffers
  a1 = (int*)malloc(a1_size);
  rc = tiledb_query_set_buffer(ctx, query, "a1", a1, &a1_size);
  CHECK(rc == TILEDB_OK);
  a2_off = (uint64_t*)malloc(a2_off_size);
  a2 = (char*)malloc(a2_size);
  rc = tiledb_query_set_buffer_var(
      ctx, query, "a2", a2_off, &a2_off_size, a2, &a2_size);
  CHECK(rc == TILEDB_OK);
  a3 = (float*)malloc(a3_size);
  rc = tiledb_query_set_buffer(ctx, query, "a3", a3, &a3_size);
  CHECK(rc == TILEDB_OK);
  coords = (uint64_t*)malloc(coords_size);
  rc = tiledb_query_set_buffer(ctx, query, TILEDB_COORDS, coords, &coords_size);
  CHECK(rc == TILEDB_OK);

  // Set subarray
  rc = tiledb_query_set_subarray(ctx_, query, subarray_2);
  CHECK(rc == TILEDB_OK);

  // Submit query
  CHECK(tiledb_query_submit(ctx, query) == TILEDB_OK);

  // Check that there are no results
  REQUIRE(a1_size == 2 * sizeof(int));
  REQUIRE(a2_off_size == 2 * sizeof(uint64_t));
  REQUIRE(a2_size == 3 * sizeof(char));
  REQUIRE(a3_size == 4 * sizeof(float));
  CHECK(a1[0] == 0);
  CHECK(a1[1] == 1);
  CHECK(a2_off[0] == 0);
  CHECK(a2_off[1] == 1);
  CHECK(a2[0] == 'a');
  CHECK(a2[1] == 'b');
  CHECK(a2[2] == 'b');
  CHECK(a3[0] == 0.1f);
  CHECK(a3[1] == 0.2f);
  CHECK(a3[2] == 1.1f);
  CHECK(a3[3] == 1.2f);

  // Clean up
  free(a1);
  free(a2_off);
  free(a2);
  free(a3);
  tiledb_query_free(&query);

  // Clean up
  CHECK(tiledb_array_close(ctx, array) == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_ctx_free(&ctx);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, encrypted",
    "[capi], [sparse], [encryption]") {
  std::string array_name;
  encryption_type = TILEDB_AES_256_GCM;
  encryption_key = "0123456789abcdeF0123456789abcdeF";

  if (supports_s3_) {
    // S3
    array_name = S3_TEMP_DIR + ARRAY;
    check_sorted_reads(
        array_name, TILEDB_FILTER_BZIP2, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
  } else if (supports_hdfs_) {
    // HDFS
    array_name = HDFS_TEMP_DIR + ARRAY;
    check_sorted_reads(
        array_name, TILEDB_FILTER_BZIP2, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
  } else {
    // File
    array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + ARRAY;
    check_sorted_reads(
        array_name, TILEDB_FILTER_BZIP2, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
  }
}