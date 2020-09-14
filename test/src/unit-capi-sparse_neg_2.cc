/**
 * @file   unit-capi-sparse_neg.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB Inc.
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
 * Tests of C API for sparse arrays with negative domains.
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

#include <iostream>
#include <sstream>
#include <thread>

using namespace tiledb::test;

struct SparseNegFx2 {
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

  // TileDB context
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;

  // Supported filesystems
  bool supports_s3_;
  bool supports_hdfs_;
  bool supports_azure_;

  // Functions
  SparseNegFx2();
  ~SparseNegFx2();
  void create_temp_dir(const std::string& path);
  void remove_temp_dir(const std::string& path);
  void create_sparse_vector(const std::string& path);
  void create_sparse_array(const std::string& path);
  void write_sparse_vector(const std::string& path);
  void write_sparse_array(const std::string& path);
  void read_sparse_vector(const std::string& path);
  void read_sparse_array_row(const std::string& path);
  void read_sparse_array_col(const std::string& path);
  static std::string random_name(const std::string& prefix);
  void set_supported_fs();
};

SparseNegFx2::SparseNegFx2() {
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
}

SparseNegFx2::~SparseNegFx2() {
  if (supports_s3_) {
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

  tiledb_vfs_free(&vfs_);
  tiledb_ctx_free(&ctx_);
}

void SparseNegFx2::set_supported_fs() {
  tiledb_ctx_t* ctx = nullptr;
  REQUIRE(tiledb_ctx_alloc(nullptr, &ctx) == TILEDB_OK);

  get_supported_fs(&supports_s3_, &supports_hdfs_, &supports_azure_);

  tiledb_ctx_free(&ctx);
}

void SparseNegFx2::create_temp_dir(const std::string& path) {
  remove_temp_dir(path);
  REQUIRE(tiledb_vfs_create_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

void SparseNegFx2::remove_temp_dir(const std::string& path) {
  int is_dir = 0;
  REQUIRE(tiledb_vfs_is_dir(ctx_, vfs_, path.c_str(), &is_dir) == TILEDB_OK);
  if (is_dir)
    REQUIRE(tiledb_vfs_remove_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

std::string SparseNegFx2::random_name(const std::string& prefix) {
  std::stringstream ss;
  ss << prefix << "-" << std::this_thread::get_id() << "-"
     << TILEDB_TIMESTAMP_NOW_MS;
  return ss.str();
}

void SparseNegFx2::create_sparse_vector(const std::string& path) {
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
  rc = tiledb_array_schema_alloc(ctx_, TILEDB_SPARSE, &array_schema);
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
  tiledb_array_schema_free(&array_schema);
}

void SparseNegFx2::create_sparse_array(const std::string& path) {
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
  tiledb_attribute_free(&a);
  tiledb_dimension_free(&d1);
  tiledb_dimension_free(&d2);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

void SparseNegFx2::write_sparse_vector(const std::string& path) {
  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, path.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  int a[] = {0, 1};
  uint64_t a_size = sizeof(a);
  int64_t coords[] = {-1, 1};
  uint64_t coords_size = sizeof(coords);
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a", a, &a_size);
  REQUIRE(rc == TILEDB_OK);
  rc =
      tiledb_query_set_buffer(ctx_, query, "d0", coords, &coords_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
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

void SparseNegFx2::write_sparse_array(const std::string& path) {
  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, path.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  int a[] = {1, 2, 3, 4};
  uint64_t a_size = sizeof(a);
  int64_t coords_dim1[] = {-2, 1, -1, 1};
  int64_t coords_dim2[] = {0, 1, -1, -1};
  uint64_t coords_size = sizeof(coords_dim1);
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a", a, &a_size);
  REQUIRE(rc == TILEDB_OK);
  rc =
      tiledb_query_set_buffer(ctx_, query, "d1", coords_dim1, &coords_size);
  REQUIRE(rc == TILEDB_OK);
rc =
      tiledb_query_set_buffer(ctx_, query, "d2", coords_dim2, &coords_size);
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

void SparseNegFx2::read_sparse_vector(const std::string& path) {
  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, path.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  int a[2];
  uint64_t a_size = sizeof(a);
  int64_t coords[2];
  uint64_t coords_size = sizeof(coords);
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);

  // Set some subarray
  int64_t s0[] = {-1, 2};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 0, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_set_buffer(ctx_, query, "a", a, &a_size);
  REQUIRE(rc == TILEDB_OK);
  rc =
      tiledb_query_set_buffer(ctx_, query, "d0", coords, &coords_size);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  int a_c[] = {0, 1};
  int64_t coords_c[] = {-1, 1};
  CHECK(a_size == sizeof(a_c));
  CHECK(coords_size == sizeof(coords_c));
  CHECK(!memcmp(a, a_c, sizeof(a_c)));
  CHECK(!memcmp(coords, coords_c, sizeof(coords_c)));

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void SparseNegFx2::read_sparse_array_row(const std::string& path) {
  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, path.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  int a[4];
  uint64_t a_size = sizeof(a);
  int64_t coords[8];
  uint64_t coords_size = sizeof(coords);
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a", a, &a_size);
  REQUIRE(rc == TILEDB_OK);
  rc =
      tiledb_query_set_buffer(ctx_, query, TILEDB_COORDS, coords, &coords_size);
  REQUIRE(rc == TILEDB_OK);

  // Create some subarray
  int64_t s0[] = {-2, 1};
  int64_t s1[] = {-2, 1};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 0, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 1, &s1[0], &s1[1], nullptr);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  int a_c[] = {1, 3, 4, 2};
  int64_t coords_c[] = {-2, 0, -1, -1, 1, -1, 1, 1};
  CHECK(a_size == sizeof(a_c));
  CHECK(coords_size == sizeof(coords_c));
  CHECK(!memcmp(a, a_c, sizeof(a_c)));
  CHECK(!memcmp(coords, coords_c, sizeof(coords_c)));

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

void SparseNegFx2::read_sparse_array_col(const std::string& path) {
  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, path.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  int a[4];
  uint64_t a_size = sizeof(a);
  int64_t coords[8];
  uint64_t coords_size = sizeof(coords);
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a", a, &a_size);
  REQUIRE(rc == TILEDB_OK);
  rc =
      tiledb_query_set_buffer(ctx_, query, TILEDB_COORDS, coords, &coords_size);
  REQUIRE(rc == TILEDB_OK);

  // Set some subarray
  int64_t s0[] = {-2, 1};
  int64_t s1[] = {-2, 1};
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_COL_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 0, &s0[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 1, &s1[0], &s0[1], nullptr);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_query_submit(ctx_, query);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx_, query);
  REQUIRE(rc == TILEDB_OK);

  int a_c[] = {3, 4, 1, 2};
  int64_t coords_c[] = {-1, -1, 1, -1, -2, 0, 1, 1};
  CHECK(a_size == sizeof(a_c));
  CHECK(coords_size == sizeof(coords_c));
  CHECK(!memcmp(a, a_c, sizeof(a_c)));
  CHECK(!memcmp(coords, coords_c, sizeof(coords_c)));

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
}

TEST_CASE_METHOD(
    SparseNegFx2,
    "C API: Test 1d sparse vector with negative domain 2",
    "[capi], [sparse-neg-2], [sparse-neg-vector-2]") {
  std::string vector_name =
      FILE_URI_PREFIX + FILE_TEMP_DIR + "sparse_neg_vector";
  create_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);

  create_sparse_vector(vector_name);
  write_sparse_vector(vector_name);
  read_sparse_vector(vector_name);

  remove_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
}

TEST_CASE_METHOD(
    SparseNegFx2,
    "C API: Test 2d sparse array with negative domain 2",
    "[capi], [sparse-neg-2], [sparse-neg-array-2]") {
  std::string vector_name =
      FILE_URI_PREFIX + FILE_TEMP_DIR + "sparse_neg_array";
  create_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);

  create_sparse_array(vector_name);
  write_sparse_array(vector_name);
  read_sparse_array_row(vector_name);
  read_sparse_array_col(vector_name);

  remove_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
}
