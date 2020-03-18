/**
 * @file   unit-capi-sparse_array.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB Inc.
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
#include "test/src/helpers.h"
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

using namespace tiledb::test;

const uint64_t DIM_DOMAIN[4] = {1, 4, 1, 4};

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
  const std::string S3_BUCKET = S3_PREFIX + random_name("tiledb") + "/";
  const std::string S3_TEMP_DIR = S3_BUCKET + "tiledb_test/";
  const std::string AZURE_PREFIX = "azure://";
  const std::string bucket = AZURE_PREFIX + random_name("tiledb") + "/";
  const std::string AZURE_TEMP_DIR = bucket + "tiledb_test/";
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
  bool supports_azure_;

  // Functions
  SparseArrayFx();
  ~SparseArrayFx();

  void create_sparse_array(
      const std::string& array_name,
      tiledb_layout_t layout = TILEDB_ROW_MAJOR,
      const uint64_t* dim_domain = DIM_DOMAIN);
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
  void write_sparse_array(
      const std::string& array_name,
      const std::vector<uint64_t>& coords,
      const std::vector<int>& a1,
      const std::vector<uint64_t>& a2_off,
      const std::vector<char>& a2_val,
      const std::vector<float>& a3);
  void set_supported_fs();
  void create_temp_dir(const std::string& path);
  void remove_temp_dir(const std::string& path);
  static std::string random_name(const std::string& prefix);
  void check_sorted_reads(
      const std::string& array_name,
      tiledb_filter_type_t compressor,
      tiledb_layout_t tile_order,
      tiledb_layout_t cell_order);
  void remove_array(const std::string& array_name);
  bool is_array(const std::string& array_name);

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
        tiledb_config_set(config, "vfs.s3.scheme", "https", &error) ==
        TILEDB_OK);
    REQUIRE(
        tiledb_config_set(
            config, "vfs.s3.use_virtual_addressing", "false", &error) ==
        TILEDB_OK);
    REQUIRE(
        tiledb_config_set(config, "vfs.s3.verify_ssl", "false", &error) ==
        TILEDB_OK);
    REQUIRE(error == nullptr);
#endif
  }
  if (supports_azure_) {
    REQUIRE(
        tiledb_config_set(
            config,
            "vfs.azure.storage_account_name",
            "devstoreaccount1",
            &error) == TILEDB_OK);
    REQUIRE(
        tiledb_config_set(
            config,
            "vfs.azure.storage_account_key",
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
            "K1SZFPTOtr/KBHBeksoGMGw==",
            &error) == TILEDB_OK);
    REQUIRE(
        tiledb_config_set(
            config,
            "vfs.azure.blob_endpoint",
            "127.0.0.1:10000/devstoreaccount1",
            &error) == TILEDB_OK);
    REQUIRE(
        tiledb_config_set(config, "vfs.azure.use_https", "false", &error) ==
        TILEDB_OK);
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

  // Connect to Azure
  if (supports_azure_) {
    int is_container = 0;
    int rc = tiledb_vfs_is_bucket(ctx_, vfs_, bucket.c_str(), &is_container);
    REQUIRE(rc == TILEDB_OK);
    if (!is_container) {
      rc = tiledb_vfs_create_bucket(ctx_, vfs_, bucket.c_str());
      REQUIRE(rc == TILEDB_OK);
    }
  }

  std::srand(0);

  create_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
  if (supports_s3_)
    create_temp_dir(S3_TEMP_DIR);
  if (supports_azure_)
    create_temp_dir(AZURE_TEMP_DIR);
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

  if (supports_azure_) {
    int is_container = 0;
    int rc = tiledb_vfs_is_bucket(ctx_, vfs_, bucket.c_str(), &is_container);
    REQUIRE(rc == TILEDB_OK);
    if (is_container) {
      rc = tiledb_vfs_remove_bucket(ctx_, vfs_, bucket.c_str());
      REQUIRE(rc == TILEDB_OK);
    }
  }

  if (supports_hdfs_)
    remove_temp_dir(HDFS_TEMP_DIR);

  tiledb_vfs_free(&vfs_);
  tiledb_ctx_free(&ctx_);
}

bool SparseArrayFx::is_array(const std::string& array_name) {
  tiledb_object_t type = TILEDB_INVALID;
  REQUIRE(tiledb_object_type(ctx_, array_name.c_str(), &type) == TILEDB_OK);
  return type == TILEDB_ARRAY;
}

void SparseArrayFx::remove_array(const std::string& array_name) {
  if (!is_array(array_name))
    return;

  CHECK(tiledb_object_remove(ctx_, array_name.c_str()) == TILEDB_OK);
}

void SparseArrayFx::set_supported_fs() {
  tiledb_ctx_t* ctx = nullptr;
  REQUIRE(tiledb_ctx_alloc(nullptr, &ctx) == TILEDB_OK);

  get_supported_fs(&supports_s3_, &supports_hdfs_, &supports_azure_);

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

std::string SparseArrayFx::random_name(const std::string& prefix) {
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
  int64_t s[] = {domain_0_lo, domain_0_hi, domain_1_lo, domain_1_hi};
  rc = tiledb_array_max_buffer_size(
      ctx_, array, ATTR_NAME.c_str(), s, &buffer_size);

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

  // Set a subarray
  int64_t s0[] = {domain_0_lo, domain_0_hi};
  int64_t s1[] = {domain_1_lo, domain_1_hi};
  rc = tiledb_query_set_layout(ctx_, query, query_layout);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 0, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 1, &s1[0], &s1[1], nullptr);
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

void SparseArrayFx::write_sparse_array(
    const std::string& array_name,
    const std::vector<uint64_t>& coords,
    const std::vector<int>& a1,
    const std::vector<uint64_t>& a2_off,
    const std::vector<char>& a2_val,
    const std::vector<float>& a3) {
  const void* buffers[] = {
      a1.data(), a2_off.data(), a2_val.data(), a3.data(), coords.data()};
  uint64_t buffer_sizes[] = {a1.size() * sizeof(int),
                             a2_off.size() * sizeof(uint64_t),
                             a2_val.size() * sizeof(char),
                             a3.size() * sizeof(float),
                             coords.size() * sizeof(uint64_t)};

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
      ctx_, query, attributes[0], (void*)buffers[0], &buffer_sizes[0]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_,
      query,
      attributes[1],
      (uint64_t*)buffers[1],
      &buffer_sizes[1],
      (void*)buffers[2],
      &buffer_sizes[2]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[2], (void*)buffers[3], &buffer_sizes[3]);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, attributes[3], (void*)buffers[4], &buffer_sizes[4]);
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

void SparseArrayFx::create_sparse_array(
    const std::string& array_name,
    tiledb_layout_t layout,
    const uint64_t* dim_domain) {
  // Create dimensions
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
  rc = tiledb_array_schema_set_cell_order(ctx_, array_schema, layout);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx_, array_schema, layout);
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
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);

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
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  REQUIRE(rc == TILEDB_OK);

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
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);

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
  int c_buffer_a1[] = {0, 1, 2, 3, 5, 7, 4};
  uint64_t c_buffer_a2[] = {0, 1, 3, 6, 10, 13, 17};
  char c_buffer_a2_var[] = "abbcccddddggghhhhe";
  float c_buffer_a3[] = {0.1f,
                         0.2f,
                         1.1f,
                         1.2f,
                         2.1f,
                         2.2f,
                         3.1f,
                         3.2f,
                         5.1f,
                         5.2f,
                         7.1f,
                         7.2f,
                         4.1f,
                         4.2f};
  uint64_t c_buffer_coords[] = {1, 1, 1, 2, 1, 4, 2, 3, 3, 3, 3, 4, 4, 2};

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
  rc = tiledb_query_set_layout(ctx, query, TILEDB_UNORDERED);
  REQUIRE(rc == TILEDB_OK);

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
  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Prepare the buffers that will store the result
  uint64_t buffer_size;
  uint64_t s[] = {1, 2, 1, 2};
  rc = tiledb_array_max_buffer_size(ctx_, array, "a1", s, &buffer_size);
  CHECK(rc == TILEDB_OK);

  auto buffer = new int[buffer_size / sizeof(int)];
  REQUIRE(buffer != nullptr);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", buffer, &buffer_size);
  REQUIRE(rc == TILEDB_OK);

  // Set subarray
  uint64_t s0[] = {1, 2};
  uint64_t s1[] = {1, 2};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 0, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 1, &s1[0], &s1[1], nullptr);
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
    SparseArrayFx,
    "C API: Test sparse array, sorted reads",
    "[capi][sparse][sorted-reads]") {
  std::string array_name;

  SECTION("- no compression, row/row-major") {
    if (supports_s3_) {
      // S3
      array_name = S3_TEMP_DIR + ARRAY;
      check_sorted_reads(
          array_name, TILEDB_FILTER_NONE, TILEDB_ROW_MAJOR, TILEDB_ROW_MAJOR);
    } else if (supports_azure_) {
      // Azure
      array_name = AZURE_TEMP_DIR + ARRAY;
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
    } else if (supports_azure_) {
      // Azure
      array_name = AZURE_TEMP_DIR + ARRAY;
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
    } else if (supports_azure_) {
      // Azure
      array_name = AZURE_TEMP_DIR + ARRAY;
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
    } else if (supports_azure_) {
      // Azure
      array_name = AZURE_TEMP_DIR + ARRAY;
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
    } else if (supports_azure_) {
      // Azure
      array_name = AZURE_TEMP_DIR + ARRAY;
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
    } else if (supports_azure_) {
      // Azure
      array_name = AZURE_TEMP_DIR + ARRAY;
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
    } else if (supports_azure_) {
      // Azure
      array_name = AZURE_TEMP_DIR + ARRAY;
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
    } else if (supports_azure_) {
      // Azure
      array_name = AZURE_TEMP_DIR + ARRAY;
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
    } else if (supports_azure_) {
      // Azure
      array_name = AZURE_TEMP_DIR + ARRAY;
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
    } else if (supports_azure_) {
      // Azure
      array_name = AZURE_TEMP_DIR + ARRAY;
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
    } else if (supports_azure_) {
      // Azure
      array_name = AZURE_TEMP_DIR + ARRAY;
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
    "C API: Test sparse array, duplicates",
    "[capi][sparse][dups]") {
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
    "[capi][sparse][non-empty]") {
  std::string temp_dir = FILE_URI_PREFIX + FILE_TEMP_DIR;
  create_temp_dir(temp_dir);
  check_non_empty_domain(temp_dir);
  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, invalid offsets on write",
    "[capi][sparse][invalid-offsets]") {
  std::string array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + "invalid_offs";
  create_sparse_array(array_name);
  check_invalid_offsets(array_name);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, no results",
    "[capi][sparse][no-results]") {
  std::string array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + "no_results";
  create_sparse_array(array_name);
  write_partial_sparse_array(array_name);
  check_sparse_array_no_results(array_name);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, missing attributes in writes",
    "[capi][sparse][write-missing-attributes]") {
  std::string array_name =
      FILE_URI_PREFIX + FILE_TEMP_DIR + "sparse_write_missing_attributes";
  create_sparse_array(array_name);
  write_sparse_array_missing_attributes(array_name);
  check_sparse_array_no_results(array_name);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, set subarray should error",
    "[capi][sparse][set-subarray]") {
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

  // Set subarray
  uint64_t s0[] = {1, 1};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 0, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_ERR);

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
    "[capi][sparse][coords-exist]") {
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
    "[capi][sparse][write-global-check]") {
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
    "[capi][sparse][invalidate-max-sizes]") {
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

  // Get max buffer sizes for empty query
  uint64_t s[] = {1, 1, 3, 3};
  uint64_t a1_size, a2_off_size, a2_size, a3_size, coords_size;
  rc = tiledb_array_max_buffer_size(ctx_, array, "a1", s, &a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size_var(
      ctx_, array, "a2", s, &a2_off_size, &a2_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_max_buffer_size(ctx_, array, "a3", s, &a3_size);
  CHECK(rc == TILEDB_OK);
  rc =
      tiledb_array_max_buffer_size(ctx_, array, TILEDB_COORDS, s, &coords_size);
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
  uint64_t s0[] = {1, 1};
  uint64_t s1[] = {3, 3};
  rc = tiledb_query_set_layout(ctx_, empty_query, TILEDB_UNORDERED);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, empty_query, 0, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, empty_query, 1, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);

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
  s0[0] = 1;
  s0[1] = 1;
  s1[0] = 1;
  s1[1] = 2;
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 0, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 1, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);

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
    "[capi][sparse][encryption]") {
  std::string array_name;
  encryption_type = TILEDB_AES_256_GCM;
  encryption_key = "0123456789abcdeF0123456789abcdeF";

  if (supports_s3_) {
    // S3
    array_name = S3_TEMP_DIR + ARRAY;
    check_sorted_reads(
        array_name, TILEDB_FILTER_BZIP2, TILEDB_ROW_MAJOR, TILEDB_COL_MAJOR);
  } else if (supports_azure_) {
    // Azure
    array_name = AZURE_TEMP_DIR + ARRAY;
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

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, calibrate est size",
    "[capi][sparse][calibrate-est-size]") {
  std::string array_name =
      FILE_URI_PREFIX + FILE_TEMP_DIR + "sparse_calibrate_est_size";
  remove_array(array_name);
  create_sparse_array(array_name);

  // Write twice (2 fragments)
  write_sparse_array(array_name);
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

  // Create WRITE query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  int a1[2];
  uint64_t a1_size = sizeof(a1);
  char a2[6];
  uint64_t a2_size = sizeof(a2);
  uint64_t a2_off[2];
  uint64_t a2_off_size = sizeof(a2_off);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", a1, &a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_, query, "a2", a2_off, &a2_off_size, a2, &a2_size);
  REQUIRE(rc == TILEDB_OK);

  // Set some subarray
  uint64_t s0[] = {1, 1};
  uint64_t s1[] = {1, 2};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 0, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 1, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_submit(ctx, query);
  CHECK(rc == TILEDB_OK);
  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_COMPLETED);

  CHECK(a1_size == sizeof(a1));
  CHECK(a2_off_size == sizeof(a2_off));
  CHECK(a2_size == 3 * sizeof(char));
  CHECK(a1[0] == 0);
  CHECK(a1[1] == 1);
  CHECK(a2_off[0] == 0);
  CHECK(a2_off[1] == 1);
  CHECK(a2[0] == 'a');
  CHECK(a2[1] == 'b');
  CHECK(a2[2] == 'b');

  // Close array
  CHECK(tiledb_array_close(ctx, array) == TILEDB_OK);

  // Clean up
  tiledb_query_free(&query);
  tiledb_array_free(&array);
  tiledb_ctx_free(&ctx);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, calibrate est size, unary",
    "[capi][sparse][calibrate-est-size-unary]") {
  std::string array_name =
      FILE_URI_PREFIX + FILE_TEMP_DIR + "sparse_calibrate_est_size_unary";
  remove_array(array_name);
  create_sparse_array(array_name);

  // Write twice (2 fragments)
  write_sparse_array(array_name);
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

  // Create WRITE query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  int a1[1];
  uint64_t a1_size = sizeof(a1);
  char a2[2];
  uint64_t a2_size = sizeof(a2);
  uint64_t a2_off[1];
  uint64_t a2_off_size = sizeof(a2_off);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", a1, &a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_, query, "a2", a2_off, &a2_off_size, a2, &a2_size);
  REQUIRE(rc == TILEDB_OK);

  // Set some subarray
  uint64_t s0[] = {1, 1};
  uint64_t s1[] = {1, 1};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 0, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 1, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_submit(ctx, query);
  CHECK(rc == TILEDB_OK);
  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_COMPLETED);

  CHECK(a1[0] == 0);
  CHECK(a1_size == sizeof(a1));
  CHECK(a2_off[0] == 0);
  CHECK(a2_off_size == sizeof(a2_off));
  CHECK(a2[0] == 'a');
  CHECK(a2_size == sizeof(char));

  // Close array
  CHECK(tiledb_array_close(ctx, array) == TILEDB_OK);

  // Clean up
  tiledb_query_free(&query);
  tiledb_array_free(&array);
  tiledb_ctx_free(&ctx);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, calibrate est size, huge range",
    "[capi][sparse][calibrate-est-size-huge-range]") {
  std::string array_name =
      FILE_URI_PREFIX + FILE_TEMP_DIR + "sparse_calibrate_est_size_huge_range";
  const uint64_t dim_domain[4] = {1, UINT64_MAX - 1, 1, UINT64_MAX - 1};
  remove_array(array_name);
  create_sparse_array(array_name, TILEDB_ROW_MAJOR, dim_domain);

  // Write twice (2 fragments)
  write_sparse_array(array_name);
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

  // Create WRITE query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  int a1[2] = {-1, -1};
  uint64_t a1_size = sizeof(a1);
  char a2[6];
  uint64_t a2_size = sizeof(a2);
  uint64_t a2_off[2];
  uint64_t a2_off_size = sizeof(a2_off);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", a1, &a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_, query, "a2", a2_off, &a2_off_size, a2, &a2_size);
  REQUIRE(rc == TILEDB_OK);

  // Set some subarray
  uint64_t s0[] = {1, UINT64_MAX - 1};
  uint64_t s1[] = {1, UINT64_MAX - 1};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 0, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 1, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_submit(ctx, query);
  CHECK(rc == TILEDB_OK);
  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  CHECK(a1[0] == 0);
  CHECK(a1[1] == 1);
  CHECK(a1_size == sizeof(a1));
  CHECK(a2_off[0] == 0);
  CHECK(a2_off[1] == 1);
  CHECK(a2_off_size == sizeof(a2_off));
  CHECK(a2[0] == 'a');
  CHECK(a2[1] == 'b');
  CHECK(a2[2] == 'b');
  CHECK(a2_size == 3 * sizeof(char));

  // Close array
  CHECK(tiledb_array_close(ctx, array) == TILEDB_OK);

  // Clean up
  tiledb_query_free(&query);
  tiledb_array_free(&array);
  tiledb_ctx_free(&ctx);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, multi-subarray, 2D, complete",
    "[capi][sparse][MR][2D][complete]") {
  std::string array_name =
      FILE_URI_PREFIX + FILE_TEMP_DIR + "sparse_multi_subarray_2d_complete";
  remove_array(array_name);
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

  // Create WRITE query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  int a1[20];
  uint64_t a1_size = sizeof(a1);
  uint64_t coords[20];
  uint64_t coords_size = sizeof(coords);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", a1, &a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc =
      tiledb_query_set_buffer(ctx_, query, TILEDB_COORDS, coords, &coords_size);
  REQUIRE(rc == TILEDB_OK);

  // Set some subarray
  uint64_t s00[] = {1, 1};
  uint64_t s01[] = {3, 4};
  uint64_t s10[] = {2, 2};
  uint64_t s11[] = {3, 4};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 0, &s00[0], &s00[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 0, &s01[0], &s01[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 1, &s10[0], &s10[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 1, &s11[0], &s11[1], nullptr);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_submit(ctx, query);
  CHECK(rc == TILEDB_OK);
  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_COMPLETED);

  CHECK(a1[0] == 1);
  CHECK(a1[1] == 2);
  CHECK(a1[2] == 5);
  CHECK(a1[3] == 6);
  CHECK(a1[4] == 7);
  CHECK(a1_size == 5 * sizeof(int));
  CHECK(coords_size == 10 * sizeof(uint64_t));
  CHECK(coords[0] == 1);
  CHECK(coords[1] == 2);
  CHECK(coords[2] == 1);
  CHECK(coords[3] == 4);
  CHECK(coords[4] == 4);
  CHECK(coords[5] == 2);
  CHECK(coords[6] == 3);
  CHECK(coords[7] == 3);
  CHECK(coords[8] == 3);
  CHECK(coords[9] == 4);

  // Close array
  CHECK(tiledb_array_close(ctx, array) == TILEDB_OK);

  // Clean up
  tiledb_query_free(&query);
  tiledb_array_free(&array);
  tiledb_ctx_free(&ctx);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, multi-subarray, 2D, multiplicities",
    "[capi][sparse][multi-subarray-2d-multiplicities]") {
  std::string array_name = FILE_URI_PREFIX + FILE_TEMP_DIR +
                           "sparse_multi_subarray_2d_multiplicities";
  remove_array(array_name);
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

  // Create WRITE query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  int a1[20];
  uint64_t a1_size = sizeof(a1);
  uint64_t coords[20];
  uint64_t coords_size = sizeof(coords);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", a1, &a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc =
      tiledb_query_set_buffer(ctx_, query, TILEDB_COORDS, coords, &coords_size);
  REQUIRE(rc == TILEDB_OK);

  // Set some subarray
  uint64_t s00[] = {1, 1};
  uint64_t s01[] = {3, 4};
  uint64_t s10[] = {2, 2};
  uint64_t s11[] = {3, 4};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 0, &s00[0], &s00[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 0, &s01[0], &s01[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 0, &s00[0], &s00[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 1, &s10[0], &s10[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 1, &s11[0], &s11[1], nullptr);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_submit(ctx, query);
  CHECK(rc == TILEDB_OK);
  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_COMPLETED);

  CHECK(a1[0] == 1);
  CHECK(a1[1] == 2);
  CHECK(a1[2] == 5);
  CHECK(a1[3] == 6);
  CHECK(a1[4] == 7);
  CHECK(a1[5] == 1);
  CHECK(a1[6] == 2);
  CHECK(a1_size == 7 * sizeof(int));
  CHECK(coords_size == 14 * sizeof(uint64_t));
  CHECK(coords[0] == 1);
  CHECK(coords[1] == 2);
  CHECK(coords[2] == 1);
  CHECK(coords[3] == 4);
  CHECK(coords[4] == 4);
  CHECK(coords[5] == 2);
  CHECK(coords[6] == 3);
  CHECK(coords[7] == 3);
  CHECK(coords[8] == 3);
  CHECK(coords[9] == 4);
  CHECK(coords[10] == 1);
  CHECK(coords[11] == 2);
  CHECK(coords[12] == 1);
  CHECK(coords[13] == 4);

  // Close array
  CHECK(tiledb_array_close(ctx, array) == TILEDB_OK);

  // Clean up
  tiledb_query_free(&query);
  tiledb_array_free(&array);
  tiledb_ctx_free(&ctx);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, multi-subarray, 2D, incomplete",
    "[capi][sparse][multi-subarray-2d-incomplete]") {
  std::string array_name =
      FILE_URI_PREFIX + FILE_TEMP_DIR + "sparse_multi_subarray_2d_incomplete";
  remove_array(array_name);
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

  // Create WRITE query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  int a1[3];
  uint64_t a1_size = sizeof(a1);
  uint64_t coords[6];
  uint64_t coords_size = sizeof(coords);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", a1, &a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc =
      tiledb_query_set_buffer(ctx_, query, TILEDB_COORDS, coords, &coords_size);
  REQUIRE(rc == TILEDB_OK);

  // Set some subarray
  uint64_t s00[] = {1, 1};
  uint64_t s01[] = {3, 4};
  uint64_t s10[] = {2, 2};
  uint64_t s11[] = {3, 4};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 0, &s00[0], &s00[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 0, &s01[0], &s01[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 1, &s10[0], &s10[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 1, &s11[0], &s11[1], nullptr);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_submit(ctx, query);
  CHECK(rc == TILEDB_OK);
  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  CHECK(a1_size == 2 * sizeof(int));
  CHECK(a1[0] == 1);
  CHECK(a1[1] == 2);
  CHECK(coords_size == 4 * sizeof(uint64_t));
  CHECK(coords[0] == 1);
  CHECK(coords[1] == 2);
  CHECK(coords[2] == 1);
  CHECK(coords[3] == 4);

  rc = tiledb_query_submit(ctx, query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_COMPLETED);

  CHECK(a1_size == 3 * sizeof(int));
  CHECK(a1[0] == 5);
  CHECK(a1[1] == 6);
  CHECK(a1[2] == 7);
  CHECK(coords_size == 6 * sizeof(uint64_t));
  CHECK(coords[0] == 4);
  CHECK(coords[1] == 2);
  CHECK(coords[2] == 3);
  CHECK(coords[3] == 3);
  CHECK(coords[4] == 3);
  CHECK(coords[5] == 4);

  // Close array
  CHECK(tiledb_array_close(ctx, array) == TILEDB_OK);

  // Clean up
  tiledb_query_free(&query);
  tiledb_array_free(&array);
  tiledb_ctx_free(&ctx);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, multi-subarray, 2D, complete, col",
    "[capi][sparse][multi-subarray-2d-complete-col]") {
  std::string array_name =
      FILE_URI_PREFIX + FILE_TEMP_DIR + "sparse_multi_subarray_2d_complete_col";
  remove_array(array_name);
  create_sparse_array(array_name, TILEDB_COL_MAJOR);
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

  // Create WRITE query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  int a1[20];
  uint64_t a1_size = sizeof(a1);
  uint64_t coords[20];
  uint64_t coords_size = sizeof(coords);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", a1, &a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc =
      tiledb_query_set_buffer(ctx_, query, TILEDB_COORDS, coords, &coords_size);
  REQUIRE(rc == TILEDB_OK);

  // Set some subarray
  uint64_t s00[] = {1, 1};
  uint64_t s01[] = {3, 4};
  uint64_t s10[] = {2, 2};
  uint64_t s11[] = {3, 4};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 0, &s00[0], &s00[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 0, &s01[0], &s01[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 1, &s10[0], &s10[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 1, &s11[0], &s11[1], nullptr);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_submit(ctx, query);
  CHECK(rc == TILEDB_OK);
  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_COMPLETED);

  CHECK(a1_size == 5 * sizeof(int));
  CHECK(a1[0] == 1);
  CHECK(a1[1] == 5);
  CHECK(a1[2] == 2);
  CHECK(a1[3] == 6);
  CHECK(a1[4] == 7);
  CHECK(coords_size == 10 * sizeof(uint64_t));
  CHECK(coords[0] == 1);
  CHECK(coords[1] == 2);
  CHECK(coords[2] == 4);
  CHECK(coords[3] == 2);
  CHECK(coords[4] == 1);
  CHECK(coords[5] == 4);
  CHECK(coords[6] == 3);
  CHECK(coords[7] == 3);
  CHECK(coords[8] == 3);
  CHECK(coords[9] == 4);

  // Close array
  CHECK(tiledb_array_close(ctx, array) == TILEDB_OK);

  // Clean up
  tiledb_query_free(&query);
  tiledb_array_free(&array);
  tiledb_ctx_free(&ctx);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array 2, multi-range subarray, row-major",
    "[capi][sparse][multi-range-row]") {
  std::string array_name =
      FILE_URI_PREFIX + FILE_TEMP_DIR + "sparse_multi_range_row";
  remove_array(array_name);

  // Create array
  uint64_t domain[] = {1, 10, 1, 10};
  create_sparse_array(array_name, TILEDB_ROW_MAJOR, domain);
  write_sparse_array(array_name);

  // Create array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);

  // Open the array
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create buffers
  int b_a1[20];
  uint64_t b_a1_size = sizeof(b_a1);
  uint64_t b_a2_off[20];
  uint64_t b_a2_off_size = sizeof(b_a2_off);
  char b_a2_val[20];
  uint64_t b_a2_val_size = sizeof(b_a2_val);
  float b_a3[20];
  uint64_t b_a3_size = sizeof(b_a3);
  uint64_t b_coords[20];
  uint64_t b_coords_size = sizeof(b_coords);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", b_a1, &b_a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_, query, "a2", b_a2_off, &b_a2_off_size, b_a2_val, &b_a2_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a3", b_a3, &b_a3_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, TILEDB_COORDS, b_coords, &b_coords_size);
  REQUIRE(rc == TILEDB_OK);

  // Create a subarray
  int64_t s0[] = {1, 2};
  int64_t s1[] = {3, 4};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 0, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 0, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 1, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 1, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_COMPLETED);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Check buffers
  CHECK(b_a1_size == 8 * sizeof(int));
  CHECK(b_a2_off_size == 8 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 20 * sizeof(char));
  CHECK(b_a3_size == 16 * sizeof(float));
  CHECK(b_coords_size == 16 * sizeof(uint64_t));

  CHECK(b_a1[0] == 0);
  CHECK(b_a1[1] == 1);
  CHECK(b_a1[2] == 2);
  CHECK(b_a1[3] == 3);
  CHECK(b_a1[4] == 4);
  CHECK(b_a1[5] == 6);
  CHECK(b_a1[6] == 7);
  CHECK(b_a1[7] == 5);

  CHECK(b_a2_off[0] == 0);
  CHECK(b_a2_off[1] == 1);
  CHECK(b_a2_off[2] == 3);
  CHECK(b_a2_off[3] == 6);
  CHECK(b_a2_off[4] == 10);
  CHECK(b_a2_off[5] == 11);
  CHECK(b_a2_off[6] == 14);
  CHECK(b_a2_off[7] == 18);

  CHECK(b_a2_val[0] == 'a');
  CHECK(b_a2_val[1] == 'b');
  CHECK(b_a2_val[2] == 'b');
  CHECK(b_a2_val[3] == 'c');
  CHECK(b_a2_val[4] == 'c');
  CHECK(b_a2_val[5] == 'c');
  CHECK(b_a2_val[6] == 'd');
  CHECK(b_a2_val[7] == 'd');
  CHECK(b_a2_val[8] == 'd');
  CHECK(b_a2_val[9] == 'd');
  CHECK(b_a2_val[10] == 'e');
  CHECK(b_a2_val[11] == 'g');
  CHECK(b_a2_val[12] == 'g');
  CHECK(b_a2_val[13] == 'g');
  CHECK(b_a2_val[14] == 'h');
  CHECK(b_a2_val[15] == 'h');
  CHECK(b_a2_val[16] == 'h');
  CHECK(b_a2_val[17] == 'h');
  CHECK(b_a2_val[18] == 'f');
  CHECK(b_a2_val[19] == 'f');

  CHECK(b_a3[0] == 0.1f);
  CHECK(b_a3[1] == 0.2f);
  CHECK(b_a3[2] == 1.1f);
  CHECK(b_a3[3] == 1.2f);
  CHECK(b_a3[4] == 2.1f);
  CHECK(b_a3[5] == 2.2f);
  CHECK(b_a3[6] == 3.1f);
  CHECK(b_a3[7] == 3.2f);
  CHECK(b_a3[8] == 4.1f);
  CHECK(b_a3[9] == 4.2f);
  CHECK(b_a3[10] == 6.1f);
  CHECK(b_a3[11] == 6.2f);
  CHECK(b_a3[12] == 7.1f);
  CHECK(b_a3[13] == 7.2f);
  CHECK(b_a3[14] == 5.1f);
  CHECK(b_a3[15] == 5.2f);

  CHECK(b_coords[0] == 1);
  CHECK(b_coords[1] == 1);
  CHECK(b_coords[2] == 1);
  CHECK(b_coords[3] == 2);
  CHECK(b_coords[4] == 1);
  CHECK(b_coords[5] == 4);
  CHECK(b_coords[6] == 2);
  CHECK(b_coords[7] == 3);
  CHECK(b_coords[8] == 3);
  CHECK(b_coords[9] == 1);
  CHECK(b_coords[10] == 3);
  CHECK(b_coords[11] == 3);
  CHECK(b_coords[12] == 3);
  CHECK(b_coords[13] == 4);
  CHECK(b_coords[14] == 4);
  CHECK(b_coords[15] == 2);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, multi-range subarray, col-major",
    "[capi][sparse][multi-range-col]") {
  std::string array_name =
      FILE_URI_PREFIX + FILE_TEMP_DIR + "sparse_multi_range_col";
  remove_array(array_name);

  // Create array
  uint64_t domain[] = {1, 10, 1, 10};
  create_sparse_array(array_name, TILEDB_ROW_MAJOR, domain);
  write_sparse_array(array_name);

  // Create array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);

  // Open the array
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create buffers
  int b_a1[20];
  uint64_t b_a1_size = sizeof(b_a1);
  uint64_t b_a2_off[20];
  uint64_t b_a2_off_size = sizeof(b_a2_off);
  char b_a2_val[20];
  uint64_t b_a2_val_size = sizeof(b_a2_val);
  float b_a3[20];
  uint64_t b_a3_size = sizeof(b_a3);
  uint64_t b_coords[20];
  uint64_t b_coords_size = sizeof(b_coords);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", b_a1, &b_a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_, query, "a2", b_a2_off, &b_a2_off_size, b_a2_val, &b_a2_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a3", b_a3, &b_a3_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, TILEDB_COORDS, b_coords, &b_coords_size);
  REQUIRE(rc == TILEDB_OK);

  // Set a subarray
  int64_t s0[] = {1, 2};
  int64_t s1[] = {3, 4};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_COL_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 0, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 0, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 1, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 1, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_COMPLETED);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Check buffers
  CHECK(b_a1_size == 8 * sizeof(int));
  CHECK(b_a2_off_size == 8 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 20 * sizeof(char));
  CHECK(b_a3_size == 16 * sizeof(float));
  CHECK(b_coords_size == 16 * sizeof(uint64_t));

  CHECK(b_a1[0] == 0);
  CHECK(b_a1[1] == 4);
  CHECK(b_a1[2] == 1);
  CHECK(b_a1[3] == 5);
  CHECK(b_a1[4] == 3);
  CHECK(b_a1[5] == 6);
  CHECK(b_a1[6] == 2);
  CHECK(b_a1[7] == 7);

  CHECK(b_a2_off[0] == 0);
  CHECK(b_a2_off[1] == 1);
  CHECK(b_a2_off[2] == 2);
  CHECK(b_a2_off[3] == 4);
  CHECK(b_a2_off[4] == 6);
  CHECK(b_a2_off[5] == 10);
  CHECK(b_a2_off[6] == 13);
  CHECK(b_a2_off[7] == 16);

  CHECK(b_a2_val[0] == 'a');
  CHECK(b_a2_val[1] == 'e');
  CHECK(b_a2_val[2] == 'b');
  CHECK(b_a2_val[3] == 'b');
  CHECK(b_a2_val[4] == 'f');
  CHECK(b_a2_val[5] == 'f');
  CHECK(b_a2_val[6] == 'd');
  CHECK(b_a2_val[7] == 'd');
  CHECK(b_a2_val[8] == 'd');
  CHECK(b_a2_val[9] == 'd');
  CHECK(b_a2_val[10] == 'g');
  CHECK(b_a2_val[11] == 'g');
  CHECK(b_a2_val[12] == 'g');
  CHECK(b_a2_val[13] == 'c');
  CHECK(b_a2_val[14] == 'c');
  CHECK(b_a2_val[15] == 'c');
  CHECK(b_a2_val[16] == 'h');
  CHECK(b_a2_val[17] == 'h');
  CHECK(b_a2_val[18] == 'h');
  CHECK(b_a2_val[19] == 'h');

  CHECK(b_a3[0] == 0.1f);
  CHECK(b_a3[1] == 0.2f);
  CHECK(b_a3[2] == 4.1f);
  CHECK(b_a3[3] == 4.2f);
  CHECK(b_a3[4] == 1.1f);
  CHECK(b_a3[5] == 1.2f);
  CHECK(b_a3[6] == 5.1f);
  CHECK(b_a3[7] == 5.2f);
  CHECK(b_a3[8] == 3.1f);
  CHECK(b_a3[9] == 3.2f);
  CHECK(b_a3[10] == 6.1f);
  CHECK(b_a3[11] == 6.2f);
  CHECK(b_a3[12] == 2.1f);
  CHECK(b_a3[13] == 2.2f);
  CHECK(b_a3[14] == 7.1f);
  CHECK(b_a3[15] == 7.2f);

  CHECK(b_coords[0] == 1);
  CHECK(b_coords[1] == 1);
  CHECK(b_coords[2] == 3);
  CHECK(b_coords[3] == 1);
  CHECK(b_coords[4] == 1);
  CHECK(b_coords[5] == 2);
  CHECK(b_coords[6] == 4);
  CHECK(b_coords[7] == 2);
  CHECK(b_coords[8] == 2);
  CHECK(b_coords[9] == 3);
  CHECK(b_coords[10] == 3);
  CHECK(b_coords[11] == 3);
  CHECK(b_coords[12] == 1);
  CHECK(b_coords[13] == 4);
  CHECK(b_coords[14] == 3);
  CHECK(b_coords[15] == 4);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, multi-range subarray, row-major, incomplete 1",
    "[capi][sparse][multi-range-row-incomplete-1]") {
  std::string array_name =
      FILE_URI_PREFIX + FILE_TEMP_DIR + "sparse_multi_range_row_incomplete_1";
  remove_array(array_name);

  // Create array
  uint64_t domain[] = {1, 10, 1, 10};
  create_sparse_array(array_name, TILEDB_ROW_MAJOR, domain);
  write_sparse_array(array_name);

  // Create array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);

  // Open the array
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create buffers
  int b_a1[6];
  uint64_t b_a1_size = sizeof(b_a1);
  uint64_t b_a2_off[20];
  uint64_t b_a2_off_size = sizeof(b_a2_off);
  char b_a2_val[20];
  uint64_t b_a2_val_size = sizeof(b_a2_val);
  float b_a3[20];
  uint64_t b_a3_size = sizeof(b_a3);
  uint64_t b_coords[20];
  uint64_t b_coords_size = sizeof(b_coords);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", b_a1, &b_a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_, query, "a2", b_a2_off, &b_a2_off_size, b_a2_val, &b_a2_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a3", b_a3, &b_a3_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, TILEDB_COORDS, b_coords, &b_coords_size);
  REQUIRE(rc == TILEDB_OK);

  // Create a subarray
  int64_t s0[] = {1, 2};
  int64_t s1[] = {3, 4};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 0, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 0, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 1, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 1, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 4 * sizeof(int));
  CHECK(b_a2_off_size == 4 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 10 * sizeof(char));
  CHECK(b_a3_size == 8 * sizeof(float));
  CHECK(b_coords_size == 8 * sizeof(uint64_t));

  CHECK(b_a1[0] == 0);
  CHECK(b_a1[1] == 1);
  CHECK(b_a1[2] == 2);
  CHECK(b_a1[3] == 3);

  CHECK(b_a2_off[0] == 0);
  CHECK(b_a2_off[1] == 1);
  CHECK(b_a2_off[2] == 3);
  CHECK(b_a2_off[3] == 6);

  CHECK(b_a2_val[0] == 'a');
  CHECK(b_a2_val[1] == 'b');
  CHECK(b_a2_val[2] == 'b');
  CHECK(b_a2_val[3] == 'c');
  CHECK(b_a2_val[4] == 'c');
  CHECK(b_a2_val[5] == 'c');
  CHECK(b_a2_val[6] == 'd');
  CHECK(b_a2_val[7] == 'd');
  CHECK(b_a2_val[8] == 'd');
  CHECK(b_a2_val[9] == 'd');

  CHECK(b_a3[0] == 0.1f);
  CHECK(b_a3[1] == 0.2f);
  CHECK(b_a3[2] == 1.1f);
  CHECK(b_a3[3] == 1.2f);
  CHECK(b_a3[4] == 2.1f);
  CHECK(b_a3[5] == 2.2f);
  CHECK(b_a3[6] == 3.1f);
  CHECK(b_a3[7] == 3.2f);

  CHECK(b_coords[0] == 1);
  CHECK(b_coords[1] == 1);
  CHECK(b_coords[2] == 1);
  CHECK(b_coords[3] == 2);
  CHECK(b_coords[4] == 1);
  CHECK(b_coords[5] == 4);
  CHECK(b_coords[6] == 2);
  CHECK(b_coords[7] == 3);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_COMPLETED);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Check buffers
  CHECK(b_a1_size == 4 * sizeof(int));
  CHECK(b_a2_off_size == 4 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 10 * sizeof(char));
  CHECK(b_a3_size == 8 * sizeof(float));
  CHECK(b_coords_size == 8 * sizeof(uint64_t));

  CHECK(b_a1[0] == 4);
  CHECK(b_a1[1] == 6);
  CHECK(b_a1[2] == 7);
  CHECK(b_a1[3] == 5);

  CHECK(b_a2_off[0] == 0);
  CHECK(b_a2_off[1] == 1);
  CHECK(b_a2_off[2] == 4);
  CHECK(b_a2_off[3] == 8);

  CHECK(b_a2_val[0] == 'e');
  CHECK(b_a2_val[1] == 'g');
  CHECK(b_a2_val[2] == 'g');
  CHECK(b_a2_val[3] == 'g');
  CHECK(b_a2_val[4] == 'h');
  CHECK(b_a2_val[5] == 'h');
  CHECK(b_a2_val[6] == 'h');
  CHECK(b_a2_val[7] == 'h');
  CHECK(b_a2_val[8] == 'f');
  CHECK(b_a2_val[9] == 'f');

  CHECK(b_a3[0] == 4.1f);
  CHECK(b_a3[1] == 4.2f);
  CHECK(b_a3[2] == 6.1f);
  CHECK(b_a3[3] == 6.2f);
  CHECK(b_a3[4] == 7.1f);
  CHECK(b_a3[5] == 7.2f);
  CHECK(b_a3[6] == 5.1f);
  CHECK(b_a3[7] == 5.2f);

  CHECK(b_coords[0] == 3);
  CHECK(b_coords[1] == 1);
  CHECK(b_coords[2] == 3);
  CHECK(b_coords[3] == 3);
  CHECK(b_coords[4] == 3);
  CHECK(b_coords[5] == 4);
  CHECK(b_coords[6] == 4);
  CHECK(b_coords[7] == 2);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, multi-range subarray, col-major, incomplete 1",
    "[capi][sparse][multi-range-col-incomplete-1]") {
  std::string array_name =
      FILE_URI_PREFIX + FILE_TEMP_DIR + "sparse_multi_range_col_incomplete_1";
  remove_array(array_name);

  // Create array
  uint64_t domain[] = {1, 10, 1, 10};
  create_sparse_array(array_name, TILEDB_ROW_MAJOR, domain);
  write_sparse_array(array_name);

  // Create array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);

  // Open the array
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create buffers
  int b_a1[6];
  uint64_t b_a1_size = sizeof(b_a1);
  uint64_t b_a2_off[20];
  uint64_t b_a2_off_size = sizeof(b_a2_off);
  char b_a2_val[20];
  uint64_t b_a2_val_size = sizeof(b_a2_val);
  float b_a3[20];
  uint64_t b_a3_size = sizeof(b_a3);
  uint64_t b_coords[20];
  uint64_t b_coords_size = sizeof(b_coords);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", b_a1, &b_a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_, query, "a2", b_a2_off, &b_a2_off_size, b_a2_val, &b_a2_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a3", b_a3, &b_a3_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, TILEDB_COORDS, b_coords, &b_coords_size);
  REQUIRE(rc == TILEDB_OK);

  // Create a subarray
  int64_t s0[] = {1, 2};
  int64_t s1[] = {3, 4};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_COL_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 0, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 0, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 1, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 1, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 4 * sizeof(int));
  CHECK(b_a2_off_size == 4 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 6 * sizeof(char));
  CHECK(b_a3_size == 8 * sizeof(float));
  CHECK(b_coords_size == 8 * sizeof(uint64_t));

  CHECK(b_a1[0] == 0);
  CHECK(b_a1[1] == 4);
  CHECK(b_a1[2] == 1);
  CHECK(b_a1[3] == 5);

  CHECK(b_a2_off[0] == 0);
  CHECK(b_a2_off[1] == 1);
  CHECK(b_a2_off[2] == 2);
  CHECK(b_a2_off[3] == 4);

  CHECK(b_a2_val[0] == 'a');
  CHECK(b_a2_val[1] == 'e');
  CHECK(b_a2_val[2] == 'b');
  CHECK(b_a2_val[3] == 'b');
  CHECK(b_a2_val[4] == 'f');
  CHECK(b_a2_val[5] == 'f');

  CHECK(b_a3[0] == 0.1f);
  CHECK(b_a3[1] == 0.2f);
  CHECK(b_a3[2] == 4.1f);
  CHECK(b_a3[3] == 4.2f);
  CHECK(b_a3[4] == 1.1f);
  CHECK(b_a3[5] == 1.2f);
  CHECK(b_a3[6] == 5.1f);
  CHECK(b_a3[7] == 5.2f);

  CHECK(b_coords[0] == 1);
  CHECK(b_coords[1] == 1);
  CHECK(b_coords[2] == 3);
  CHECK(b_coords[3] == 1);
  CHECK(b_coords[4] == 1);
  CHECK(b_coords[5] == 2);
  CHECK(b_coords[6] == 4);
  CHECK(b_coords[7] == 2);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_COMPLETED);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Check buffers
  CHECK(b_a1_size == 4 * sizeof(int));
  CHECK(b_a2_off_size == 4 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 14 * sizeof(char));
  CHECK(b_a3_size == 8 * sizeof(float));
  CHECK(b_coords_size == 8 * sizeof(uint64_t));

  CHECK(b_a1[0] == 3);
  CHECK(b_a1[1] == 6);
  CHECK(b_a1[2] == 2);
  CHECK(b_a1[3] == 7);

  CHECK(b_a2_off[0] == 0);
  CHECK(b_a2_off[1] == 4);
  CHECK(b_a2_off[2] == 7);
  CHECK(b_a2_off[3] == 10);

  CHECK(b_a2_val[0] == 'd');
  CHECK(b_a2_val[1] == 'd');
  CHECK(b_a2_val[2] == 'd');
  CHECK(b_a2_val[3] == 'd');
  CHECK(b_a2_val[4] == 'g');
  CHECK(b_a2_val[5] == 'g');
  CHECK(b_a2_val[6] == 'g');
  CHECK(b_a2_val[7] == 'c');
  CHECK(b_a2_val[8] == 'c');
  CHECK(b_a2_val[9] == 'c');
  CHECK(b_a2_val[10] == 'h');
  CHECK(b_a2_val[11] == 'h');
  CHECK(b_a2_val[12] == 'h');
  CHECK(b_a2_val[13] == 'h');

  CHECK(b_a3[0] == 3.1f);
  CHECK(b_a3[1] == 3.2f);
  CHECK(b_a3[2] == 6.1f);
  CHECK(b_a3[3] == 6.2f);
  CHECK(b_a3[4] == 2.1f);
  CHECK(b_a3[5] == 2.2f);
  CHECK(b_a3[6] == 7.1f);
  CHECK(b_a3[7] == 7.2f);

  CHECK(b_coords[0] == 2);
  CHECK(b_coords[1] == 3);
  CHECK(b_coords[2] == 3);
  CHECK(b_coords[3] == 3);
  CHECK(b_coords[4] == 1);
  CHECK(b_coords[5] == 4);
  CHECK(b_coords[6] == 3);
  CHECK(b_coords[7] == 4);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, multi-range subarray, row-major, incomplete 2",
    "[capi][sparse][multi-range-row-incomplete-2]") {
  std::string array_name =
      FILE_URI_PREFIX + FILE_TEMP_DIR + "sparse_multi_range_row_incomplete_2";
  remove_array(array_name);

  // Create array
  uint64_t domain[] = {1, 10, 1, 10};
  create_sparse_array(array_name, TILEDB_ROW_MAJOR, domain);
  write_sparse_array(array_name);

  // Create array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);

  // Open the array
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create buffers
  int b_a1[3];
  uint64_t b_a1_size = sizeof(b_a1);
  uint64_t b_a2_off[20];
  uint64_t b_a2_off_size = sizeof(b_a2_off);
  char b_a2_val[20];
  uint64_t b_a2_val_size = sizeof(b_a2_val);
  float b_a3[20];
  uint64_t b_a3_size = sizeof(b_a3);
  uint64_t b_coords[20];
  uint64_t b_coords_size = sizeof(b_coords);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", b_a1, &b_a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_, query, "a2", b_a2_off, &b_a2_off_size, b_a2_val, &b_a2_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a3", b_a3, &b_a3_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, TILEDB_COORDS, b_coords, &b_coords_size);
  REQUIRE(rc == TILEDB_OK);

  // Create a subarray
  int64_t s0[] = {1, 2};
  int64_t s1[] = {3, 4};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 0, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 0, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 1, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 1, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 3 * sizeof(int));
  CHECK(b_a2_off_size == 3 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 6 * sizeof(char));
  CHECK(b_a3_size == 6 * sizeof(float));
  CHECK(b_coords_size == 6 * sizeof(uint64_t));

  CHECK(b_a1[0] == 0);
  CHECK(b_a1[1] == 1);
  CHECK(b_a1[2] == 2);

  CHECK(b_a2_off[0] == 0);
  CHECK(b_a2_off[1] == 1);
  CHECK(b_a2_off[2] == 3);

  CHECK(b_a2_val[0] == 'a');
  CHECK(b_a2_val[1] == 'b');
  CHECK(b_a2_val[2] == 'b');
  CHECK(b_a2_val[3] == 'c');
  CHECK(b_a2_val[4] == 'c');
  CHECK(b_a2_val[5] == 'c');

  CHECK(b_a3[0] == 0.1f);
  CHECK(b_a3[1] == 0.2f);
  CHECK(b_a3[2] == 1.1f);
  CHECK(b_a3[3] == 1.2f);
  CHECK(b_a3[4] == 2.1f);
  CHECK(b_a3[5] == 2.2f);

  CHECK(b_coords[0] == 1);
  CHECK(b_coords[1] == 1);
  CHECK(b_coords[2] == 1);
  CHECK(b_coords[3] == 2);
  CHECK(b_coords[4] == 1);
  CHECK(b_coords[5] == 4);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 4 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 2 * sizeof(uint64_t));

  CHECK(b_a1[0] == 3);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'd');
  CHECK(b_a2_val[1] == 'd');
  CHECK(b_a2_val[2] == 'd');
  CHECK(b_a2_val[3] == 'd');

  CHECK(b_a3[0] == 3.1f);
  CHECK(b_a3[1] == 3.2f);

  CHECK(b_coords[0] == 2);
  CHECK(b_coords[1] == 3);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 3 * sizeof(int));
  CHECK(b_a2_off_size == 3 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 8 * sizeof(char));
  CHECK(b_a3_size == 6 * sizeof(float));
  CHECK(b_coords_size == 6 * sizeof(uint64_t));

  CHECK(b_a1[0] == 4);
  CHECK(b_a1[1] == 6);
  CHECK(b_a1[2] == 7);

  CHECK(b_a2_off[0] == 0);
  CHECK(b_a2_off[1] == 1);
  CHECK(b_a2_off[2] == 4);

  CHECK(b_a2_val[0] == 'e');
  CHECK(b_a2_val[1] == 'g');
  CHECK(b_a2_val[2] == 'g');
  CHECK(b_a2_val[3] == 'g');
  CHECK(b_a2_val[4] == 'h');
  CHECK(b_a2_val[5] == 'h');
  CHECK(b_a2_val[6] == 'h');
  CHECK(b_a2_val[7] == 'h');

  CHECK(b_a3[0] == 4.1f);
  CHECK(b_a3[1] == 4.2f);
  CHECK(b_a3[2] == 6.1f);
  CHECK(b_a3[3] == 6.2f);
  CHECK(b_a3[4] == 7.1f);
  CHECK(b_a3[5] == 7.2f);

  CHECK(b_coords[0] == 3);
  CHECK(b_coords[1] == 1);
  CHECK(b_coords[2] == 3);
  CHECK(b_coords[3] == 3);
  CHECK(b_coords[4] == 3);
  CHECK(b_coords[5] == 4);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_COMPLETED);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 2 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 2 * sizeof(uint64_t));

  CHECK(b_a1[0] == 5);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'f');
  CHECK(b_a2_val[1] == 'f');

  CHECK(b_a3[0] == 5.1f);
  CHECK(b_a3[1] == 5.2f);

  CHECK(b_coords[0] == 4);
  CHECK(b_coords[1] == 2);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, multi-range subarray, col-major, incomplete 2",
    "[capi][sparse][multi-range-col-incomplete-2]") {
  std::string array_name =
      FILE_URI_PREFIX + FILE_TEMP_DIR + "sparse_multi_range_col_incomplete_2";
  remove_array(array_name);

  // Create array
  uint64_t domain[] = {1, 10, 1, 10};
  create_sparse_array(array_name, TILEDB_ROW_MAJOR, domain);
  write_sparse_array(array_name);

  // Create array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);

  // Open the array
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create buffers
  int b_a1[3];
  uint64_t b_a1_size = sizeof(b_a1);
  uint64_t b_a2_off[20];
  uint64_t b_a2_off_size = sizeof(b_a2_off);
  char b_a2_val[20];
  uint64_t b_a2_val_size = sizeof(b_a2_val);
  float b_a3[20];
  uint64_t b_a3_size = sizeof(b_a3);
  uint64_t b_coords[20];
  uint64_t b_coords_size = sizeof(b_coords);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", b_a1, &b_a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_, query, "a2", b_a2_off, &b_a2_off_size, b_a2_val, &b_a2_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a3", b_a3, &b_a3_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, TILEDB_COORDS, b_coords, &b_coords_size);
  REQUIRE(rc == TILEDB_OK);

  // Create a subarray
  int64_t s0[] = {1, 2};
  int64_t s1[] = {3, 4};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_COL_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 0, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 0, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 1, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 1, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 2 * sizeof(int));
  CHECK(b_a2_off_size == 2 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 2 * sizeof(char));
  CHECK(b_a3_size == 4 * sizeof(float));
  CHECK(b_coords_size == 4 * sizeof(uint64_t));

  CHECK(b_a1[0] == 0);
  CHECK(b_a1[1] == 4);

  CHECK(b_a2_off[0] == 0);
  CHECK(b_a2_off[1] == 1);

  CHECK(b_a2_val[0] == 'a');
  CHECK(b_a2_val[1] == 'e');

  CHECK(b_a3[0] == 0.1f);
  CHECK(b_a3[1] == 0.2f);
  CHECK(b_a3[2] == 4.1f);
  CHECK(b_a3[3] == 4.2f);

  CHECK(b_coords[0] == 1);
  CHECK(b_coords[1] == 1);
  CHECK(b_coords[2] == 3);
  CHECK(b_coords[3] == 1);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 2 * sizeof(int));
  CHECK(b_a2_off_size == 2 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 4 * sizeof(char));
  CHECK(b_a3_size == 4 * sizeof(float));
  CHECK(b_coords_size == 4 * sizeof(uint64_t));

  CHECK(b_a1[0] == 1);
  CHECK(b_a1[1] == 5);

  CHECK(b_a2_off[0] == 0);
  CHECK(b_a2_off[1] == 2);

  CHECK(b_a2_val[0] == 'b');
  CHECK(b_a2_val[1] == 'b');
  CHECK(b_a2_val[2] == 'f');
  CHECK(b_a2_val[3] == 'f');

  CHECK(b_a3[0] == 1.1f);
  CHECK(b_a3[1] == 1.2f);
  CHECK(b_a3[2] == 5.1f);
  CHECK(b_a3[3] == 5.2f);

  CHECK(b_coords[0] == 1);
  CHECK(b_coords[1] == 2);
  CHECK(b_coords[2] == 4);
  CHECK(b_coords[3] == 2);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 2 * sizeof(int));
  CHECK(b_a2_off_size == 2 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 7 * sizeof(char));
  CHECK(b_a3_size == 4 * sizeof(float));
  CHECK(b_coords_size == 4 * sizeof(uint64_t));

  CHECK(b_a1[0] == 3);
  CHECK(b_a1[1] == 6);

  CHECK(b_a2_off[0] == 0);
  CHECK(b_a2_off[1] == 4);

  CHECK(b_a2_val[0] == 'd');
  CHECK(b_a2_val[1] == 'd');
  CHECK(b_a2_val[2] == 'd');
  CHECK(b_a2_val[3] == 'd');
  CHECK(b_a2_val[4] == 'g');
  CHECK(b_a2_val[5] == 'g');
  CHECK(b_a2_val[6] == 'g');

  CHECK(b_a3[0] == 3.1f);
  CHECK(b_a3[1] == 3.2f);
  CHECK(b_a3[2] == 6.1f);
  CHECK(b_a3[3] == 6.2f);

  CHECK(b_coords[0] == 2);
  CHECK(b_coords[1] == 3);
  CHECK(b_coords[2] == 3);
  CHECK(b_coords[3] == 3);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_COMPLETED);

  // Check buffers
  CHECK(b_a1_size == 2 * sizeof(int));
  CHECK(b_a2_off_size == 2 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 7 * sizeof(char));
  CHECK(b_a3_size == 4 * sizeof(float));
  CHECK(b_coords_size == 4 * sizeof(uint64_t));

  CHECK(b_a1[0] == 2);
  CHECK(b_a1[1] == 7);

  CHECK(b_a2_off[0] == 0);
  CHECK(b_a2_off[1] == 3);

  CHECK(b_a2_val[0] == 'c');
  CHECK(b_a2_val[1] == 'c');
  CHECK(b_a2_val[2] == 'c');
  CHECK(b_a2_val[3] == 'h');
  CHECK(b_a2_val[4] == 'h');
  CHECK(b_a2_val[5] == 'h');
  CHECK(b_a2_val[6] == 'h');

  CHECK(b_a3[0] == 2.1f);
  CHECK(b_a3[1] == 2.2f);
  CHECK(b_a3[2] == 7.1f);
  CHECK(b_a3[3] == 7.2f);

  CHECK(b_coords[0] == 1);
  CHECK(b_coords[1] == 4);
  CHECK(b_coords[2] == 3);
  CHECK(b_coords[3] == 4);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, multi-range subarray, row-major, incomplete 3",
    "[capi][sparse][multi-range-row-incomplete-3]") {
  std::string array_name =
      FILE_URI_PREFIX + FILE_TEMP_DIR + "sparse_multi_range_row_incomplete_3";
  remove_array(array_name);

  // Create array
  uint64_t domain[] = {1, 10, 1, 10};
  create_sparse_array(array_name, TILEDB_ROW_MAJOR, domain);
  write_sparse_array(array_name);

  // Create array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);

  // Open the array
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create buffers
  int b_a1[2];
  uint64_t b_a1_size = sizeof(b_a1);
  uint64_t b_a2_off[20];
  uint64_t b_a2_off_size = sizeof(b_a2_off);
  char b_a2_val[20];
  uint64_t b_a2_val_size = sizeof(b_a2_val);
  float b_a3[20];
  uint64_t b_a3_size = sizeof(b_a3);
  uint64_t b_coords[20];
  uint64_t b_coords_size = sizeof(b_coords);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", b_a1, &b_a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_, query, "a2", b_a2_off, &b_a2_off_size, b_a2_val, &b_a2_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a3", b_a3, &b_a3_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, TILEDB_COORDS, b_coords, &b_coords_size);
  REQUIRE(rc == TILEDB_OK);

  // Create a subarray
  int64_t s0[] = {1, 2};
  int64_t s1[] = {3, 4};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 0, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 0, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 1, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 1, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 2 * sizeof(int));
  CHECK(b_a2_off_size == 2 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 3 * sizeof(char));
  CHECK(b_a3_size == 4 * sizeof(float));
  CHECK(b_coords_size == 4 * sizeof(uint64_t));

  CHECK(b_a1[0] == 0);
  CHECK(b_a1[1] == 1);

  CHECK(b_a2_off[0] == 0);
  CHECK(b_a2_off[1] == 1);

  CHECK(b_a2_val[0] == 'a');
  CHECK(b_a2_val[1] == 'b');
  CHECK(b_a2_val[2] == 'b');

  CHECK(b_a3[0] == 0.1f);
  CHECK(b_a3[1] == 0.2f);
  CHECK(b_a3[2] == 1.1f);
  CHECK(b_a3[3] == 1.2f);

  CHECK(b_coords[0] == 1);
  CHECK(b_coords[1] == 1);
  CHECK(b_coords[2] == 1);
  CHECK(b_coords[3] == 2);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 3 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 2 * sizeof(uint64_t));

  CHECK(b_a1[0] == 2);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'c');
  CHECK(b_a2_val[1] == 'c');
  CHECK(b_a2_val[2] == 'c');

  CHECK(b_a3[0] == 2.1f);
  CHECK(b_a3[1] == 2.2f);

  CHECK(b_coords[0] == 1);
  CHECK(b_coords[1] == 4);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 4 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 2 * sizeof(uint64_t));

  CHECK(b_a1[0] == 3);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'd');
  CHECK(b_a2_val[1] == 'd');
  CHECK(b_a2_val[2] == 'd');
  CHECK(b_a2_val[3] == 'd');

  CHECK(b_a3[0] == 3.1f);
  CHECK(b_a3[1] == 3.2f);

  CHECK(b_coords[0] == 2);
  CHECK(b_coords[1] == 3);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 1 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 2 * sizeof(uint64_t));

  CHECK(b_a1[0] == 4);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'e');

  CHECK(b_a3[0] == 4.1f);
  CHECK(b_a3[1] == 4.2f);

  CHECK(b_coords[0] == 3);
  CHECK(b_coords[1] == 1);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 2 * sizeof(int));
  CHECK(b_a2_off_size == 2 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 7 * sizeof(char));
  CHECK(b_a3_size == 4 * sizeof(float));
  CHECK(b_coords_size == 4 * sizeof(uint64_t));

  CHECK(b_a1[0] == 6);
  CHECK(b_a1[1] == 7);

  CHECK(b_a2_off[0] == 0);
  CHECK(b_a2_off[1] == 3);

  CHECK(b_a2_val[0] == 'g');
  CHECK(b_a2_val[1] == 'g');
  CHECK(b_a2_val[2] == 'g');
  CHECK(b_a2_val[3] == 'h');
  CHECK(b_a2_val[4] == 'h');
  CHECK(b_a2_val[5] == 'h');
  CHECK(b_a2_val[6] == 'h');

  CHECK(b_a3[0] == 6.1f);
  CHECK(b_a3[1] == 6.2f);
  CHECK(b_a3[2] == 7.1f);
  CHECK(b_a3[3] == 7.2f);

  CHECK(b_coords[0] == 3);
  CHECK(b_coords[1] == 3);
  CHECK(b_coords[2] == 3);
  CHECK(b_coords[3] == 4);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_COMPLETED);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 2 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 2 * sizeof(uint64_t));

  CHECK(b_a1[0] == 5);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'f');
  CHECK(b_a2_val[1] == 'f');

  CHECK(b_a3[0] == 5.1f);
  CHECK(b_a3[1] == 5.2f);

  CHECK(b_coords[0] == 4);
  CHECK(b_coords[1] == 2);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, multi-range subarray, row-major, incomplete 4",
    "[capi][sparse][multi-range-row-incomplete-4]") {
  std::string array_name =
      FILE_URI_PREFIX + FILE_TEMP_DIR + "sparse_multi_range_row_incomplete_4";
  remove_array(array_name);

  // Create array
  uint64_t domain[] = {1, 10, 1, 10};
  create_sparse_array(array_name, TILEDB_ROW_MAJOR, domain);
  write_sparse_array(array_name);

  // Create array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);

  // Open the array
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create buffers
  int b_a1[1];
  uint64_t b_a1_size = sizeof(b_a1);
  uint64_t b_a2_off[20];
  uint64_t b_a2_off_size = sizeof(b_a2_off);
  char b_a2_val[20];
  uint64_t b_a2_val_size = sizeof(b_a2_val);
  float b_a3[20];
  uint64_t b_a3_size = sizeof(b_a3);
  uint64_t b_coords[20];
  uint64_t b_coords_size = sizeof(b_coords);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", b_a1, &b_a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_, query, "a2", b_a2_off, &b_a2_off_size, b_a2_val, &b_a2_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a3", b_a3, &b_a3_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, TILEDB_COORDS, b_coords, &b_coords_size);
  REQUIRE(rc == TILEDB_OK);

  // Create a subarray
  int64_t s0[] = {1, 2};
  int64_t s1[] = {3, 4};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 0, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 0, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 1, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 1, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 1 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 2 * sizeof(uint64_t));

  CHECK(b_a1[0] == 0);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'a');

  CHECK(b_a3[0] == 0.1f);
  CHECK(b_a3[1] == 0.2f);

  CHECK(b_coords[0] == 1);
  CHECK(b_coords[1] == 1);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 2 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 2 * sizeof(uint64_t));

  CHECK(b_a1[0] == 1);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'b');
  CHECK(b_a2_val[1] == 'b');

  CHECK(b_a3[0] == 1.1f);
  CHECK(b_a3[1] == 1.2f);

  CHECK(b_coords[0] == 1);
  CHECK(b_coords[1] == 2);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 3 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 2 * sizeof(uint64_t));

  CHECK(b_a1[0] == 2);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'c');
  CHECK(b_a2_val[1] == 'c');
  CHECK(b_a2_val[2] == 'c');

  CHECK(b_a3[0] == 2.1f);
  CHECK(b_a3[1] == 2.2f);

  CHECK(b_coords[0] == 1);
  CHECK(b_coords[1] == 4);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 4 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 2 * sizeof(uint64_t));

  CHECK(b_a1[0] == 3);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'd');
  CHECK(b_a2_val[1] == 'd');
  CHECK(b_a2_val[2] == 'd');
  CHECK(b_a2_val[3] == 'd');

  CHECK(b_a3[0] == 3.1f);
  CHECK(b_a3[1] == 3.2f);

  CHECK(b_coords[0] == 2);
  CHECK(b_coords[1] == 3);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 1 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 2 * sizeof(uint64_t));

  CHECK(b_a1[0] == 4);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'e');

  CHECK(b_a3[0] == 4.1f);
  CHECK(b_a3[1] == 4.2f);

  CHECK(b_coords[0] == 3);
  CHECK(b_coords[1] == 1);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 3 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 2 * sizeof(uint64_t));

  CHECK(b_a1[0] == 6);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'g');
  CHECK(b_a2_val[1] == 'g');
  CHECK(b_a2_val[1] == 'g');

  CHECK(b_a3[0] == 6.1f);
  CHECK(b_a3[1] == 6.2f);

  CHECK(b_coords[0] == 3);
  CHECK(b_coords[1] == 3);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 4 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 2 * sizeof(uint64_t));

  CHECK(b_a1[0] == 7);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'h');
  CHECK(b_a2_val[1] == 'h');
  CHECK(b_a2_val[1] == 'h');
  CHECK(b_a2_val[2] == 'h');

  CHECK(b_a3[0] == 7.1f);
  CHECK(b_a3[1] == 7.2f);

  CHECK(b_coords[0] == 3);
  CHECK(b_coords[1] == 4);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_COMPLETED);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 2 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 2 * sizeof(uint64_t));

  CHECK(b_a1[0] == 5);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'f');
  CHECK(b_a2_val[1] == 'f');

  CHECK(b_a3[0] == 5.1f);
  CHECK(b_a3[1] == 5.2f);

  CHECK(b_coords[0] == 4);
  CHECK(b_coords[1] == 2);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, multi-range subarray, col-major, incomplete 4",
    "[capi][sparse][multi-range-col-incomplete-4]") {
  std::string array_name =
      FILE_URI_PREFIX + FILE_TEMP_DIR + "sparse_multi_range_col_incomplete_4";
  remove_array(array_name);

  // Create array
  uint64_t domain[] = {1, 10, 1, 10};
  create_sparse_array(array_name, TILEDB_ROW_MAJOR, domain);
  write_sparse_array(array_name);

  // Create array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);

  // Open the array
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create buffers
  int b_a1[1];
  uint64_t b_a1_size = sizeof(b_a1);
  uint64_t b_a2_off[20];
  uint64_t b_a2_off_size = sizeof(b_a2_off);
  char b_a2_val[20];
  uint64_t b_a2_val_size = sizeof(b_a2_val);
  float b_a3[20];
  uint64_t b_a3_size = sizeof(b_a3);
  uint64_t b_coords[20];
  uint64_t b_coords_size = sizeof(b_coords);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", b_a1, &b_a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_, query, "a2", b_a2_off, &b_a2_off_size, b_a2_val, &b_a2_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a3", b_a3, &b_a3_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, TILEDB_COORDS, b_coords, &b_coords_size);
  REQUIRE(rc == TILEDB_OK);

  // Create a subarray
  int64_t s0[] = {1, 2};
  int64_t s1[] = {3, 4};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_COL_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 0, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 0, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 1, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 1, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 1 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 2 * sizeof(uint64_t));

  CHECK(b_a1[0] == 0);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'a');

  CHECK(b_a3[0] == 0.1f);
  CHECK(b_a3[1] == 0.2f);

  CHECK(b_coords[0] == 1);
  CHECK(b_coords[1] == 1);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 1 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 2 * sizeof(uint64_t));

  CHECK(b_a1[0] == 4);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'e');

  CHECK(b_a3[0] == 4.1f);
  CHECK(b_a3[1] == 4.2f);

  CHECK(b_coords[0] == 3);
  CHECK(b_coords[1] == 1);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 2 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 2 * sizeof(uint64_t));

  CHECK(b_a1[0] == 1);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'b');
  CHECK(b_a2_val[1] == 'b');

  CHECK(b_a3[0] == 1.1f);
  CHECK(b_a3[1] == 1.2f);

  CHECK(b_coords[0] == 1);
  CHECK(b_coords[1] == 2);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 2 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 2 * sizeof(uint64_t));

  CHECK(b_a1[0] == 5);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'f');
  CHECK(b_a2_val[1] == 'f');

  CHECK(b_a3[0] == 5.1f);
  CHECK(b_a3[1] == 5.2f);

  CHECK(b_coords[0] == 4);
  CHECK(b_coords[1] == 2);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 4 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 2 * sizeof(uint64_t));

  CHECK(b_a1[0] == 3);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'd');
  CHECK(b_a2_val[1] == 'd');
  CHECK(b_a2_val[2] == 'd');
  CHECK(b_a2_val[3] == 'd');

  CHECK(b_a3[0] == 3.1f);
  CHECK(b_a3[1] == 3.2f);

  CHECK(b_coords[0] == 2);
  CHECK(b_coords[1] == 3);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 3 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 2 * sizeof(uint64_t));

  CHECK(b_a1[0] == 6);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'g');
  CHECK(b_a2_val[1] == 'g');
  CHECK(b_a2_val[1] == 'g');

  CHECK(b_a3[0] == 6.1f);
  CHECK(b_a3[1] == 6.2f);

  CHECK(b_coords[0] == 3);
  CHECK(b_coords[1] == 3);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 3 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 2 * sizeof(uint64_t));

  CHECK(b_a1[0] == 2);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'c');
  CHECK(b_a2_val[1] == 'c');
  CHECK(b_a2_val[1] == 'c');

  CHECK(b_a3[0] == 2.1f);
  CHECK(b_a3[1] == 2.2f);

  CHECK(b_coords[0] == 1);
  CHECK(b_coords[1] == 4);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_COMPLETED);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 4 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 2 * sizeof(uint64_t));

  CHECK(b_a1[0] == 7);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'h');
  CHECK(b_a2_val[1] == 'h');
  CHECK(b_a2_val[2] == 'h');
  CHECK(b_a2_val[3] == 'h');

  CHECK(b_a3[0] == 7.1f);
  CHECK(b_a3[1] == 7.2f);

  CHECK(b_coords[0] == 3);
  CHECK(b_coords[1] == 4);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, multi-range subarray, row-major, incomplete 5",
    "[capi][sparse][multi-range-row-incomplete-5]") {
  std::string array_name =
      FILE_URI_PREFIX + FILE_TEMP_DIR + "sparse_multi_range_row_incomplete_5";
  remove_array(array_name);

  // Create array
  uint64_t domain[] = {1, 10, 1, 10};
  create_sparse_array(array_name, TILEDB_ROW_MAJOR, domain);
  write_sparse_array(array_name);

  // Create array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);

  // Open the array
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create buffers
  int b_a1[1];
  uint64_t b_a1_size = 1;
  uint64_t b_a2_off[20];
  uint64_t b_a2_off_size = sizeof(b_a2_off);
  char b_a2_val[20];
  uint64_t b_a2_val_size = sizeof(b_a2_val);
  float b_a3[20];
  uint64_t b_a3_size = sizeof(b_a3);
  uint64_t b_coords[20];
  uint64_t b_coords_size = sizeof(b_coords);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", b_a1, &b_a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_, query, "a2", b_a2_off, &b_a2_off_size, b_a2_val, &b_a2_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a3", b_a3, &b_a3_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, TILEDB_COORDS, b_coords, &b_coords_size);
  REQUIRE(rc == TILEDB_OK);

  // Create a subarray
  int64_t s0[] = {1, 2};
  int64_t s1[] = {3, 4};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 0, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 0, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 1, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 1, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 0);
  CHECK(b_a2_off_size == 0);
  CHECK(b_a2_val_size == 0);
  CHECK(b_a3_size == 0);
  CHECK(b_coords_size == 0);

  // Reset buffer
  b_a1_size = 1 * sizeof(int);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", b_a1, &b_a1_size);
  REQUIRE(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 1 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 2 * sizeof(uint64_t));

  CHECK(b_a1[0] == 0);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'a');

  CHECK(b_a3[0] == 0.1f);
  CHECK(b_a3[1] == 0.2f);

  CHECK(b_coords[0] == 1);
  CHECK(b_coords[1] == 1);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, multi-range subarray, col-major, incomplete 5",
    "[capi][sparse][multi-range-col-incomplete-5]") {
  std::string array_name =
      FILE_URI_PREFIX + FILE_TEMP_DIR + "sparse_multi_range_col_incomplete_5";
  remove_array(array_name);

  // Create array
  uint64_t domain[] = {1, 10, 1, 10};
  create_sparse_array(array_name, TILEDB_ROW_MAJOR, domain);
  write_sparse_array(array_name);

  // Create array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);

  // Open the array
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create buffers
  int b_a1[1];
  uint64_t b_a1_size = 1;
  uint64_t b_a2_off[20];
  uint64_t b_a2_off_size = sizeof(b_a2_off);
  char b_a2_val[20];
  uint64_t b_a2_val_size = sizeof(b_a2_val);
  float b_a3[20];
  uint64_t b_a3_size = sizeof(b_a3);
  uint64_t b_coords[20];
  uint64_t b_coords_size = sizeof(b_coords);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", b_a1, &b_a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_, query, "a2", b_a2_off, &b_a2_off_size, b_a2_val, &b_a2_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a3", b_a3, &b_a3_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, TILEDB_COORDS, b_coords, &b_coords_size);
  REQUIRE(rc == TILEDB_OK);

  // Set a subarray
  int64_t s0[] = {1, 2};
  int64_t s1[] = {3, 4};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_COL_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 0, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 0, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 1, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 1, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 0);
  CHECK(b_a2_off_size == 0);
  CHECK(b_a2_val_size == 0);
  CHECK(b_a3_size == 0);
  CHECK(b_coords_size == 0);

  // Reset buffer
  b_a1_size = 1 * sizeof(int);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", b_a1, &b_a1_size);
  REQUIRE(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_INCOMPLETE);

  // Check buffers
  CHECK(b_a1_size == 1 * sizeof(int));
  CHECK(b_a2_off_size == 1 * sizeof(uint64_t));
  CHECK(b_a2_val_size == 1 * sizeof(char));
  CHECK(b_a3_size == 2 * sizeof(float));
  CHECK(b_coords_size == 2 * sizeof(uint64_t));

  CHECK(b_a1[0] == 0);

  CHECK(b_a2_off[0] == 0);

  CHECK(b_a2_val[0] == 'a');

  CHECK(b_a3[0] == 0.1f);
  CHECK(b_a3[1] == 0.2f);

  CHECK(b_coords[0] == 1);
  CHECK(b_coords[1] == 1);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, global order with 0-sized buffers",
    "[capi][sparse][global-check][zero-buffers]") {
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

  // Prepare attribute buffers
  int a1[1];
  char a2[1];
  uint64_t a2_off[1];
  float a3[1];
  uint64_t coords[1];
  uint64_t zero_size = 0;

  // Set buffers with zero size
  rc = tiledb_query_set_buffer(ctx, query, "a1", a1, &zero_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx, query, "a2", a2_off, &zero_size, a2, &zero_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx, query, "a3", a3, &zero_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx, query, TILEDB_COORDS, coords, &zero_size);
  CHECK(rc == TILEDB_OK);

  // Submit query
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
    "C API: Test sparse array, split coordinate buffers",
    "[capi][sparse][split-coords]") {
  std::string array_name =
      FILE_URI_PREFIX + FILE_TEMP_DIR + "sparse_split_coords";
  create_sparse_array(array_name);

  // ---- WRITE ----

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
  uint64_t buffer_a1_size = sizeof(buffer_a1);
  uint64_t buffer_a2_size = sizeof(buffer_a2);
  // No need to store the last '\0' character
  uint64_t buffer_var_a2_size = sizeof(buffer_var_a2) - 1;
  uint64_t buffer_a3_size = sizeof(buffer_a3);
  uint64_t buffer_d1[] = {1, 1, 1, 2, 3, 3, 3, 4};
  uint64_t buffer_d1_size = sizeof(buffer_d1);
  uint64_t buffer_d2[] = {1, 2, 4, 3, 1, 3, 4, 2};
  uint64_t buffer_d2_size = sizeof(buffer_d2);

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
  rc = tiledb_query_set_buffer(ctx_, query, "a1", buffer_a1, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_,
      query,
      "a2",
      (uint64_t*)buffer_a2,
      &buffer_a2_size,
      buffer_var_a2,
      &buffer_var_a2_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a3", buffer_a3, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "d1", buffer_d1, &buffer_d1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "d2", buffer_d2, &buffer_d2_size);
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

  // ---- READ ----

  // Create array
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);

  // Open the array
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create buffers
  int b_a1[30];
  uint64_t b_a1_size = sizeof(b_a1);
  uint64_t b_a2_off[30];
  uint64_t b_a2_off_size = sizeof(b_a2_off);
  char b_a2_val[30];
  uint64_t b_a2_val_size = sizeof(b_a2_val);
  float b_a3[30];
  uint64_t b_a3_size = sizeof(b_a3);
  uint64_t b_coords[30];
  uint64_t b_coords_size = sizeof(b_coords);

  // Create query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", b_a1, &b_a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_, query, "a2", b_a2_off, &b_a2_off_size, b_a2_val, &b_a2_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a3", b_a3, &b_a3_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, TILEDB_COORDS, b_coords, &b_coords_size);
  REQUIRE(rc == TILEDB_OK);

  // Set a subarray
  uint64_t subarray[] = {1, 4, 1, 4};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  REQUIRE(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_COMPLETED);

  // Check buffer sizes
  CHECK(b_a1_size == buffer_a1_size);
  CHECK(b_a2_off_size == buffer_a2_size);
  CHECK(b_a2_val_size == buffer_var_a2_size);
  CHECK(b_a3_size == buffer_a3_size);
  CHECK(b_coords_size == buffer_d1_size + buffer_d2_size);

  // Check buffer data
  int c_b_a1[] = {0, 1, 2, 3, 4, 5, 6, 7};
  uint64_t c_b_a2_off[] = {0, 1, 3, 6, 10, 11, 13, 16};
  char c_b_a2_val[] = "abbcccddddeffggghhhh";
  float c_b_a3[] = {0.1f,
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
  uint64_t c_b_coords[] = {1, 1, 1, 2, 1, 4, 2, 3, 3, 1, 3, 3, 3, 4, 4, 2};
  CHECK(!memcmp(c_b_a1, b_a1, b_a1_size));
  CHECK(!memcmp(c_b_a2_off, b_a2_off, b_a2_off_size));
  CHECK(!memcmp(c_b_a2_val, b_a2_val, b_a2_val_size));
  CHECK(!memcmp(c_b_a3, b_a3, b_a3_size));
  CHECK(!memcmp(c_b_coords, b_coords, b_coords_size));

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, split coordinate buffers, global write",
    "[capi][sparse][split-coords][global]") {
  std::string array_name =
      FILE_URI_PREFIX + FILE_TEMP_DIR + "sparse_split_coords_global";
  create_sparse_array(array_name);

  // ---- WRITE ----

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
  uint64_t buffer_a1_size = sizeof(buffer_a1);
  uint64_t buffer_a2_size = sizeof(buffer_a2);
  // No need to store the last '\0' character
  uint64_t buffer_var_a2_size = sizeof(buffer_var_a2) - 1;
  uint64_t buffer_a3_size = sizeof(buffer_a3);
  uint64_t buffer_d1[] = {1, 1, 1, 2, 3, 4, 3, 3};
  uint64_t buffer_d1_size = sizeof(buffer_d1);
  uint64_t buffer_d2[] = {1, 2, 4, 3, 1, 2, 3, 4};
  uint64_t buffer_d2_size = sizeof(buffer_d2);

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
  rc = tiledb_query_set_buffer(ctx_, query, "a1", buffer_a1, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_,
      query,
      "a2",
      (uint64_t*)buffer_a2,
      &buffer_a2_size,
      buffer_var_a2,
      &buffer_var_a2_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a3", buffer_a3, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "d1", buffer_d1, &buffer_d1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "d2", buffer_d2, &buffer_d2_size);
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

  // ---- READ ----

  // Create array
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);

  // Open the array
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create buffers
  int b_a1[30];
  uint64_t b_a1_size = sizeof(b_a1);
  uint64_t b_a2_off[30];
  uint64_t b_a2_off_size = sizeof(b_a2_off);
  char b_a2_val[30];
  uint64_t b_a2_val_size = sizeof(b_a2_val);
  float b_a3[30];
  uint64_t b_a3_size = sizeof(b_a3);
  uint64_t b_coords[30];
  uint64_t b_coords_size = sizeof(b_coords);

  // Create query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", b_a1, &b_a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_, query, "a2", b_a2_off, &b_a2_off_size, b_a2_val, &b_a2_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a3", b_a3, &b_a3_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, TILEDB_COORDS, b_coords, &b_coords_size);
  REQUIRE(rc == TILEDB_OK);

  // Set a subarray
  uint64_t subarray[] = {1, 4, 1, 4};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  REQUIRE(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_COMPLETED);

  // Check buffer sizes
  CHECK(b_a1_size == buffer_a1_size);
  CHECK(b_a2_off_size == buffer_a2_size);
  CHECK(b_a2_val_size == buffer_var_a2_size);
  CHECK(b_a3_size == buffer_a3_size);
  CHECK(b_coords_size == buffer_d1_size + buffer_d2_size);

  // Check buffer data
  int c_b_a1[] = {0, 1, 2, 3, 4, 5, 6, 7};
  uint64_t c_b_a2_off[] = {0, 1, 3, 6, 10, 11, 13, 16};
  char c_b_a2_val[] = "abbcccddddeffggghhhh";
  float c_b_a3[] = {0.1f,
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
  uint64_t c_b_coords[] = {1, 1, 1, 2, 1, 4, 2, 3, 3, 1, 4, 2, 3, 3, 3, 4};
  CHECK(!memcmp(c_b_a1, b_a1, b_a1_size));
  CHECK(!memcmp(c_b_a2_off, b_a2_off, b_a2_off_size));
  CHECK(!memcmp(c_b_a2_val, b_a2_val, b_a2_val_size));
  CHECK(!memcmp(c_b_a3, b_a3, b_a3_size));
  CHECK(!memcmp(c_b_coords, b_coords, b_coords_size));

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, split coordinate buffers, errors",
    "[capi][sparse][split-coords][errors]") {
  std::string array_name =
      FILE_URI_PREFIX + FILE_TEMP_DIR + "sparse_split_coords_errors";
  create_sparse_array(array_name);

  // Prepare cell buffers
  uint64_t buffer_d1[] = {1, 1, 1, 2, 3, 3, 3, 4};
  uint64_t buffer_d1_size = sizeof(buffer_d1);
  uint64_t buffer_d2[] = {1, 2, 4, 3, 1, 3};
  uint64_t buffer_d2_size = sizeof(buffer_d2);

  // Open array for writing
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

  // Set buffers with different coord number
  rc = tiledb_query_set_buffer(ctx_, query, "d1", buffer_d1, &buffer_d1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "d2", buffer_d2, &buffer_d2_size);
  CHECK(rc == TILEDB_ERR);

  // Set zipped coordinates buffer after separate coordinate buffers
  // have been set
  uint64_t buffer_coords[] = {1, 1, 2, 3};
  uint64_t buffer_coords_size = sizeof(buffer_coords);
  rc = tiledb_query_set_buffer(
      ctx_, query, TILEDB_COORDS, buffer_coords, &buffer_coords_size);
  CHECK(rc == TILEDB_ERR);

  // Set coordinates buffer for dimension that does not exist
  rc = tiledb_query_set_buffer(ctx_, query, "foo", buffer_d1, &buffer_d1_size);
  CHECK(rc == TILEDB_ERR);

  // Set rest of the attribute buffers
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
  uint64_t buffer_a1_size = sizeof(buffer_a1);
  uint64_t buffer_a2_size = sizeof(buffer_a2);
  // No need to store the last '\0' character
  uint64_t buffer_var_a2_size = sizeof(buffer_var_a2) - 1;
  uint64_t buffer_a3_size = sizeof(buffer_a3);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", buffer_a1, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a3", buffer_a3, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_,
      query,
      "a2",
      (uint64_t*)buffer_a2,
      &buffer_a2_size,
      buffer_var_a2,
      &buffer_var_a2_size);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_ERR);

  tiledb_query_free(&query);

  // Set zipped coordinates first and the separate coordinate buffers
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, TILEDB_COORDS, buffer_coords, &buffer_coords_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "d1", buffer_d1, &buffer_d1_size);
  CHECK(rc == TILEDB_ERR);
  tiledb_query_free(&query);

  // Set separate coordinate buffers and then zipped coordinates
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "d1", buffer_d1, &buffer_d1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, TILEDB_COORDS, buffer_coords, &buffer_coords_size);
  CHECK(rc == TILEDB_ERR);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  // Open array for reading
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Set zipped coordinates first and the separate coordinate buffers
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, TILEDB_COORDS, buffer_coords, &buffer_coords_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "d1", buffer_d1, &buffer_d1_size);
  CHECK(rc == TILEDB_ERR);
  tiledb_query_free(&query);

  // Set separate coordinate buffers and then zipped coordinates
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "d1", buffer_d1, &buffer_d1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx_, query, TILEDB_COORDS, buffer_coords, &buffer_coords_size);
  CHECK(rc == TILEDB_ERR);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, split coordinate buffers for reads",
    "[capi][sparse][split-coords][read]") {
  std::string array_name =
      FILE_URI_PREFIX + FILE_TEMP_DIR + "sparse_split_coords_read";
  create_sparse_array(array_name);

  // ---- WRITE ----

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
  uint64_t buffer_a1_size = sizeof(buffer_a1);
  uint64_t buffer_a2_size = sizeof(buffer_a2);
  // No need to store the last '\0' character
  uint64_t buffer_var_a2_size = sizeof(buffer_var_a2) - 1;
  uint64_t buffer_a3_size = sizeof(buffer_a3);
  uint64_t buffer_d1[] = {1, 1, 1, 2, 3, 3, 3, 4};
  uint64_t buffer_d1_size = sizeof(buffer_d1);
  uint64_t buffer_d2[] = {1, 2, 4, 3, 1, 3, 4, 2};
  uint64_t buffer_d2_size = sizeof(buffer_d2);

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
  rc = tiledb_query_set_buffer(ctx_, query, "a1", buffer_a1, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_,
      query,
      "a2",
      (uint64_t*)buffer_a2,
      &buffer_a2_size,
      buffer_var_a2,
      &buffer_var_a2_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a3", buffer_a3, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "d1", buffer_d1, &buffer_d1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "d2", buffer_d2, &buffer_d2_size);
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

  // ---- READ ----

  // Create array
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);

  // Open the array
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create buffers
  int b_a1[30];
  uint64_t b_a1_size = sizeof(b_a1);
  uint64_t b_a2_off[30];
  uint64_t b_a2_off_size = sizeof(b_a2_off);
  char b_a2_val[30];
  uint64_t b_a2_val_size = sizeof(b_a2_val);
  float b_a3[30];
  uint64_t b_a3_size = sizeof(b_a3);
  uint64_t b_d1[30];
  uint64_t b_d1_size = sizeof(b_d1);
  uint64_t b_d2[30];
  uint64_t b_d2_size = sizeof(b_d2);

  // Create query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", b_a1, &b_a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_, query, "a2", b_a2_off, &b_a2_off_size, b_a2_val, &b_a2_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a3", b_a3, &b_a3_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "d1", b_d1, &b_d1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "d2", b_d2, &b_d2_size);
  CHECK(rc == TILEDB_OK);

  // Set a subarray
  uint64_t subarray[] = {1, 4, 1, 4};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  REQUIRE(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_COMPLETED);

  // Check buffer sizes
  CHECK(b_a1_size == buffer_a1_size);
  CHECK(b_a2_off_size == buffer_a2_size);
  CHECK(b_a2_val_size == buffer_var_a2_size);
  CHECK(b_a3_size == buffer_a3_size);
  CHECK(b_d1_size == buffer_d1_size);
  CHECK(b_d2_size == buffer_d2_size);

  // Check buffer data
  int c_b_a1[] = {0, 1, 2, 3, 4, 5, 6, 7};
  uint64_t c_b_a2_off[] = {0, 1, 3, 6, 10, 11, 13, 16};
  char c_b_a2_val[] = "abbcccddddeffggghhhh";
  float c_b_a3[] = {0.1f,
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
  uint64_t c_b_d1[] = {1, 1, 1, 2, 3, 3, 3, 4};
  uint64_t c_b_d2[] = {1, 2, 4, 3, 1, 3, 4, 2};
  CHECK(!memcmp(c_b_a1, b_a1, b_a1_size));
  CHECK(!memcmp(c_b_a2_off, b_a2_off, b_a2_off_size));
  CHECK(!memcmp(c_b_a2_val, b_a2_val, b_a2_val_size));
  CHECK(!memcmp(c_b_a3, b_a3, b_a3_size));
  CHECK(!memcmp(c_b_d1, b_d1, b_d1_size));
  CHECK(!memcmp(c_b_d2, b_d2, b_d2_size));

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  remove_array(array_name);
}

TEST_CASE_METHOD(
    SparseArrayFx,
    "C API: Test sparse array, split coordinate buffers for reads, subset of "
    "dimensions",
    "[capi][sparse][split-coords][read][subset]") {
  std::string array_name =
      FILE_URI_PREFIX + FILE_TEMP_DIR + "sparse_split_coords_read_subset";
  create_sparse_array(array_name);

  // ---- WRITE ----

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
  uint64_t buffer_a1_size = sizeof(buffer_a1);
  uint64_t buffer_a2_size = sizeof(buffer_a2);
  // No need to store the last '\0' character
  uint64_t buffer_var_a2_size = sizeof(buffer_var_a2) - 1;
  uint64_t buffer_a3_size = sizeof(buffer_a3);
  uint64_t buffer_d1[] = {1, 1, 1, 2, 3, 3, 3, 4};
  uint64_t buffer_d1_size = sizeof(buffer_d1);
  uint64_t buffer_d2[] = {1, 2, 4, 3, 1, 3, 4, 2};
  uint64_t buffer_d2_size = sizeof(buffer_d2);

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
  rc = tiledb_query_set_buffer(ctx_, query, "a1", buffer_a1, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_,
      query,
      "a2",
      (uint64_t*)buffer_a2,
      &buffer_a2_size,
      buffer_var_a2,
      &buffer_var_a2_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a3", buffer_a3, &buffer_a3_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "d1", buffer_d1, &buffer_d1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "d2", buffer_d2, &buffer_d2_size);
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

  // ---- READ ----

  // Create array
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);

  // Open the array
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Create buffers
  int b_a1[30];
  uint64_t b_a1_size = sizeof(b_a1);
  uint64_t b_a2_off[30];
  uint64_t b_a2_off_size = sizeof(b_a2_off);
  char b_a2_val[30];
  uint64_t b_a2_val_size = sizeof(b_a2_val);
  float b_a3[30];
  uint64_t b_a3_size = sizeof(b_a3);
  uint64_t b_d1[30];
  uint64_t b_d1_size = sizeof(b_d1);

  // Create query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a1", b_a1, &b_a1_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer_var(
      ctx_, query, "a2", b_a2_off, &b_a2_off_size, b_a2_val, &b_a2_val_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a3", b_a3, &b_a3_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "d1", b_d1, &b_d1_size);
  CHECK(rc == TILEDB_OK);

  // Set a subarray
  uint64_t subarray[] = {1, 4, 1, 4};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  REQUIRE(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(status == TILEDB_COMPLETED);

  // Check buffer sizes
  CHECK(b_a1_size == buffer_a1_size);
  CHECK(b_a2_off_size == buffer_a2_size);
  CHECK(b_a2_val_size == buffer_var_a2_size);
  CHECK(b_a3_size == buffer_a3_size);
  CHECK(b_d1_size == buffer_d1_size);

  // Check buffer data
  int c_b_a1[] = {0, 1, 2, 3, 4, 5, 6, 7};
  uint64_t c_b_a2_off[] = {0, 1, 3, 6, 10, 11, 13, 16};
  char c_b_a2_val[] = "abbcccddddeffggghhhh";
  float c_b_a3[] = {0.1f,
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
  uint64_t c_b_d1[] = {1, 1, 1, 2, 3, 3, 3, 4};
  CHECK(!memcmp(c_b_a1, b_a1, b_a1_size));
  CHECK(!memcmp(c_b_a2_off, b_a2_off, b_a2_off_size));
  CHECK(!memcmp(c_b_a2_val, b_a2_val, b_a2_val_size));
  CHECK(!memcmp(c_b_a3, b_a3, b_a3_size));
  CHECK(!memcmp(c_b_d1, b_d1, b_d1_size));

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  remove_array(array_name);
}