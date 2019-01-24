/**
 * @file   unit-capi-array.cc
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
 * Tests of C API for (dense or sparse) array operations.
 */

#include "catch.hpp"
#include "tiledb/sm/c_api/tiledb.h"

#include <iostream>

#include "catch.hpp"
#ifdef _WIN32
#include <Windows.h>
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/misc/utils.h"

#include <chrono>
#include <iostream>
#include <sstream>
#include <thread>

struct ArrayFx {
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

  // TileDB context
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;

  // Supported filesystems
  bool supports_s3_;
  bool supports_hdfs_;

  // Encryption parameters
  tiledb_encryption_type_t encryption_type_ = TILEDB_NO_ENCRYPTION;
  const char* encryption_key_ = nullptr;

  // Functions
  ArrayFx();
  ~ArrayFx();
  void create_temp_dir(const std::string& path);
  void remove_temp_dir(const std::string& path);
  void create_sparse_vector(const std::string& path);
  void create_sparse_array(const std::string& path);
  void create_dense_vector(const std::string& path);
  void create_dense_array(const std::string& path);
  static std::string random_bucket_name(const std::string& prefix);
  void set_supported_fs();
};

ArrayFx::ArrayFx() {
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
}

ArrayFx::~ArrayFx() {
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
  tiledb_ctx_free(&ctx_);
}

void ArrayFx::set_supported_fs() {
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

void ArrayFx::create_temp_dir(const std::string& path) {
  remove_temp_dir(path);
  REQUIRE(tiledb_vfs_create_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

void ArrayFx::remove_temp_dir(const std::string& path) {
  int is_dir = 0;
  REQUIRE(tiledb_vfs_is_dir(ctx_, vfs_, path.c_str(), &is_dir) == TILEDB_OK);
  if (is_dir)
    REQUIRE(tiledb_vfs_remove_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

std::string ArrayFx::random_bucket_name(const std::string& prefix) {
  std::stringstream ss;
  ss << prefix << "-" << std::this_thread::get_id() << "-"
     << TILEDB_TIMESTAMP_NOW_MS;
  return ss.str();
}

void ArrayFx::create_sparse_vector(const std::string& path) {
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

void ArrayFx::create_sparse_array(const std::string& path) {
  int rc;
  int64_t dim_domain[] = {1, 10, 1, 10};
  int64_t tile_extent = 2;

  // Create domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx_, &domain);
  REQUIRE(rc == TILEDB_OK);
  tiledb_dimension_t* dim_1;
  rc = tiledb_dimension_alloc(
      ctx_, "d1", TILEDB_INT64, dim_domain, &tile_extent, &dim_1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, dim_1);
  REQUIRE(rc == TILEDB_OK);
  tiledb_dimension_t* dim_2;
  rc = tiledb_dimension_alloc(
      ctx_, "d2", TILEDB_INT64, &dim_domain[2], &tile_extent, &dim_2);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, dim_2);
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
  tiledb_dimension_free(&dim_1);
  tiledb_dimension_free(&dim_2);
  tiledb_array_schema_free(&array_schema);
}

void ArrayFx::create_dense_vector(const std::string& path) {
  int rc;
  int64_t dim_domain[] = {1, 10};
  int64_t tile_extent = 2;

  // Create domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx_, &domain);
  REQUIRE(rc == TILEDB_OK);
  tiledb_dimension_t* dim;
  rc = tiledb_dimension_alloc(
      ctx_, "dim", TILEDB_INT64, dim_domain, &tile_extent, &dim);
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
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_create(ctx_, path.c_str(), array_schema);
  } else {
    rc = tiledb_array_create_with_key(
        ctx_,
        path.c_str(),
        array_schema,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_));
  }
  REQUIRE(rc == TILEDB_OK);
  tiledb_attribute_free(&attr);
  tiledb_dimension_free(&dim);
  tiledb_array_schema_free(&array_schema);
}

void ArrayFx::create_dense_array(const std::string& path) {
  int rc;
  int64_t dim_domain[] = {1, 10, 1, 10};
  int64_t tile_extent = 2;

  // Create domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx_, &domain);
  REQUIRE(rc == TILEDB_OK);
  tiledb_dimension_t* dim_1;
  rc = tiledb_dimension_alloc(
      ctx_, "d1", TILEDB_INT64, dim_domain, &tile_extent, &dim_1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, dim_1);
  REQUIRE(rc == TILEDB_OK);
  tiledb_dimension_t* dim_2;
  rc = tiledb_dimension_alloc(
      ctx_, "d2", TILEDB_INT64, &dim_domain[2], &tile_extent, &dim_2);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, dim_2);
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
  tiledb_dimension_free(&dim_1);
  tiledb_dimension_free(&dim_2);
  tiledb_array_schema_free(&array_schema);
}

TEST_CASE_METHOD(
    ArrayFx, "C API: Test getting array URI", "[capi], [array], [array-uri]") {
  std::string array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + "array_uri";
  create_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);

  // Create array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);

  // Get URI when array is not opened (should not error)
  const char* uri = nullptr;
  rc = tiledb_array_get_uri(ctx_, array, &uri);
  CHECK(rc == TILEDB_OK);

  // Get URI when array is opened
  create_sparse_vector(array_name);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_get_uri(ctx_, array, &uri);
  CHECK(rc == TILEDB_OK);

#ifdef _WIN32
  char path[MAX_PATH];
  unsigned length;
  tiledb_uri_to_path(ctx_, uri, path, &length);
  CHECK(!strcmp(path, array_name.c_str()));
#else
  CHECK(!strcmp(uri, array_name.c_str()));
#endif

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);

  remove_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
}

TEST_CASE_METHOD(
    ArrayFx, "C API: Set null URI", "[capi], [array], [array-null-uri]") {
  // Create context
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, nullptr, &array);
  CHECK(rc == TILEDB_ERR);
}

TEST_CASE_METHOD(
    ArrayFx,
    "C API: Test array with encryption",
    "[capi], [array], [encryption]") {
  // Create array schema
  tiledb_array_schema_t* array_schema;
  int rc = tiledb_array_schema_alloc(ctx_, TILEDB_SPARSE, &array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Create dimensions
  tiledb_dimension_t* d1;
  const int64_t d1_domain[2] = {0, 99};
  const int64_t tile_extent[1] = {10};
  rc = tiledb_dimension_alloc(
      ctx_, "", TILEDB_INT64, &d1_domain[0], &tile_extent[0], &d1);
  REQUIRE(rc == TILEDB_OK);

  // Set domain
  tiledb_domain_t* domain;
  rc = tiledb_domain_alloc(ctx_, &domain);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_domain_add_dimension(ctx_, domain, d1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  REQUIRE(rc == TILEDB_OK);

  // Set attribute
  tiledb_attribute_t* attr1;
  rc = tiledb_attribute_alloc(ctx_, "foo", TILEDB_INT32, &attr1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, attr1, TILEDB_VAR_NUM);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_add_attribute(ctx_, array_schema, attr1);
  REQUIRE(rc == TILEDB_OK);

  // Set schema members
  rc = tiledb_array_schema_set_capacity(ctx_, array_schema, 500);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_cell_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_schema_set_tile_order(ctx_, array_schema, TILEDB_ROW_MAJOR);
  REQUIRE(rc == TILEDB_OK);

  // Check for invalid array schema
  rc = tiledb_array_schema_check(ctx_, array_schema);
  REQUIRE(rc == TILEDB_OK);

  std::string array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + "encrypyted_array";
  create_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);

  SECTION("- API calls with encrypted schema") {
    const char key[] = "0123456789abcdeF0123456789abcdeF";
    uint32_t key_len = (uint32_t)strlen(key);

    // Check error with invalid key length
    rc = tiledb_array_create_with_key(
        ctx_, array_name.c_str(), array_schema, TILEDB_AES_256_GCM, key, 31);
    REQUIRE(rc == TILEDB_ERR);
    rc = tiledb_array_create_with_key(
        ctx_,
        array_name.c_str(),
        array_schema,
        TILEDB_NO_ENCRYPTION,
        key,
        key_len);
    REQUIRE(rc == TILEDB_ERR);

    // Create array with proper key
    rc = tiledb_array_create_with_key(
        ctx_,
        array_name.c_str(),
        array_schema,
        TILEDB_AES_256_GCM,
        key,
        key_len);
    REQUIRE(rc == TILEDB_OK);

    // Clean up
    tiledb_attribute_free(&attr1);
    tiledb_dimension_free(&d1);
    tiledb_domain_free(&domain);
    tiledb_array_schema_free(&array_schema);

    // Check getting encryption type
    tiledb_encryption_type_t enc_type;
    rc = tiledb_array_encryption_type(ctx_, array_name.c_str(), &enc_type);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(enc_type == TILEDB_AES_256_GCM);

    // Open array
    tiledb_array_t* array;
    rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
    REQUIRE(rc == TILEDB_OK);
    // Check error with no key
    rc = tiledb_array_open(ctx_, array, TILEDB_READ);
    REQUIRE(rc == TILEDB_ERR);
    int is_open;
    rc = tiledb_array_is_open(ctx_, array, &is_open);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(is_open == 0);
    // Check error with wrong algorithm
    rc = tiledb_array_open_with_key(
        ctx_, array, TILEDB_READ, TILEDB_NO_ENCRYPTION, key, key_len);
    REQUIRE(rc == TILEDB_ERR);
    rc = tiledb_array_is_open(ctx_, array, &is_open);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(is_open == 0);
    // Check error with bad key
    char bad_key[32];
    rc = tiledb_array_open_with_key(
        ctx_, array, TILEDB_READ, TILEDB_AES_256_GCM, bad_key, key_len);
    REQUIRE(rc == TILEDB_ERR);
    rc = tiledb_array_is_open(ctx_, array, &is_open);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(is_open == 0);
    // Check error with bad key length
    rc = tiledb_array_open_with_key(
        ctx_, array, TILEDB_READ, TILEDB_AES_256_GCM, key, key_len - 1);
    REQUIRE(rc == TILEDB_ERR);
    rc = tiledb_array_is_open(ctx_, array, &is_open);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(is_open == 0);
    // Use correct key
    rc = tiledb_array_open_with_key(
        ctx_, array, TILEDB_READ, TILEDB_AES_256_GCM, key, key_len);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_array_is_open(ctx_, array, &is_open);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(is_open == 1);
    tiledb_array_schema_t* read_schema;
    rc = tiledb_array_get_schema(ctx_, array, &read_schema);
    REQUIRE(rc == TILEDB_OK);

    // Check opening again still requires correct key
    tiledb_array_t* array2;
    rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array2);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_array_open(ctx_, array2, TILEDB_READ);
    REQUIRE(rc == TILEDB_ERR);
    rc = tiledb_array_open_with_key(
        ctx_, array2, TILEDB_READ, TILEDB_AES_256_GCM, bad_key, key_len);
    REQUIRE(rc == TILEDB_ERR);
    rc = tiledb_array_open_with_key(
        ctx_, array2, TILEDB_READ, TILEDB_AES_256_GCM, key, key_len);
    REQUIRE(rc == TILEDB_OK);

    // Check reopening works
    rc = tiledb_array_reopen(ctx_, array);
    REQUIRE(rc == TILEDB_OK);

    // Close arrays
    rc = tiledb_array_close(ctx_, array2);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_array_close(ctx_, array);
    REQUIRE(rc == TILEDB_OK);

    // Check loading schema requires key
    tiledb_array_schema_free(&read_schema);
    rc = tiledb_array_schema_load(ctx_, array_name.c_str(), &read_schema);
    REQUIRE(rc == TILEDB_ERR);
    // Check with bad key
    rc = tiledb_array_schema_load_with_key(
        ctx_,
        array_name.c_str(),
        TILEDB_AES_256_GCM,
        bad_key,
        key_len,
        &read_schema);
    REQUIRE(rc == TILEDB_ERR);
    // Check with correct key
    rc = tiledb_array_schema_load_with_key(
        ctx_,
        array_name.c_str(),
        TILEDB_AES_256_GCM,
        key,
        key_len,
        &read_schema);
    REQUIRE(rc == TILEDB_OK);

    // Check opening after closing still requires a key.
    rc = tiledb_array_open(ctx_, array, TILEDB_READ);
    REQUIRE(rc == TILEDB_ERR);
    rc = tiledb_array_is_open(ctx_, array, &is_open);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(is_open == 0);
    rc = tiledb_array_open_with_key(
        ctx_, array, TILEDB_READ, TILEDB_AES_256_GCM, bad_key, key_len);
    REQUIRE(rc == TILEDB_ERR);
    rc = tiledb_array_is_open(ctx_, array, &is_open);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(is_open == 0);
    rc = tiledb_array_open_with_key(
        ctx_, array, TILEDB_READ, TILEDB_AES_256_GCM, key, key_len);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_array_is_open(ctx_, array, &is_open);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(is_open == 1);
    rc = tiledb_array_close(ctx_, array);
    REQUIRE(rc == TILEDB_OK);

    // Clean up
    tiledb_array_schema_free(&read_schema);
    tiledb_array_free(&array);
    tiledb_array_free(&array2);
    remove_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
  }

  SECTION("- API calls with unencrypted schema") {
    // Check for invalid array schema
    rc = tiledb_array_schema_check(ctx_, array_schema);
    REQUIRE(rc == TILEDB_OK);

    // Check create ok with null key
    rc = tiledb_array_create_with_key(
        ctx_,
        array_name.c_str(),
        array_schema,
        TILEDB_NO_ENCRYPTION,
        nullptr,
        0);
    REQUIRE(rc == TILEDB_OK);

    // Clean up
    tiledb_attribute_free(&attr1);
    tiledb_dimension_free(&d1);
    tiledb_domain_free(&domain);
    tiledb_array_schema_free(&array_schema);

    // Check getting encryption type
    tiledb_encryption_type_t enc_type;
    rc = tiledb_array_encryption_type(ctx_, array_name.c_str(), &enc_type);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(enc_type == TILEDB_NO_ENCRYPTION);

    // Open array
    tiledb_array_t* array;
    rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
    REQUIRE(rc == TILEDB_OK);
    // Check error with key
    char key[32];
    uint32_t key_len = 32;
    rc = tiledb_array_open_with_key(
        ctx_, array, TILEDB_READ, TILEDB_AES_256_GCM, key, key_len);
    REQUIRE(rc == TILEDB_ERR);
    int is_open;
    rc = tiledb_array_is_open(ctx_, array, &is_open);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(is_open == 0);
    // Check ok with null key
    rc = tiledb_array_open_with_key(
        ctx_, array, TILEDB_READ, TILEDB_NO_ENCRYPTION, nullptr, 0);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_array_is_open(ctx_, array, &is_open);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(is_open == 1);
    tiledb_array_schema_t* read_schema;
    rc = tiledb_array_get_schema(ctx_, array, &read_schema);
    REQUIRE(rc == TILEDB_OK);

    rc = tiledb_array_close(ctx_, array);
    REQUIRE(rc == TILEDB_OK);

    // Check loading schema with key is error
    tiledb_array_schema_free(&read_schema);
    rc = tiledb_array_schema_load_with_key(
        ctx_,
        array_name.c_str(),
        TILEDB_AES_256_GCM,
        key,
        key_len,
        &read_schema);
    REQUIRE(rc == TILEDB_ERR);
    // Check ok with nullptr
    rc = tiledb_array_schema_load_with_key(
        ctx_,
        array_name.c_str(),
        TILEDB_NO_ENCRYPTION,
        nullptr,
        0,
        &read_schema);
    REQUIRE(rc == TILEDB_OK);

    // Clean up
    tiledb_array_schema_free(&read_schema);
    tiledb_array_free(&array);
    remove_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
  }
}

TEST_CASE_METHOD(
    ArrayFx,
    "C API: Test opening array at timestamp",
    "[capi], [array], [array-open-at]") {
  std::string temp_dir;
  if (supports_s3_)
    temp_dir = S3_TEMP_DIR;
  else if (supports_hdfs_)
    temp_dir = HDFS_TEMP_DIR;
  else
    temp_dir = FILE_URI_PREFIX + FILE_TEMP_DIR;

  std::string array_name = temp_dir + "array-open-at";
  SECTION("- without encryption") {
    encryption_type_ = TILEDB_NO_ENCRYPTION;
    encryption_key_ = nullptr;
  }

  SECTION("- with encryption") {
    encryption_type_ = TILEDB_AES_256_GCM;
    encryption_key_ = "0123456789abcdeF0123456789abcdeF";
  }

  create_temp_dir(temp_dir);

  create_dense_vector(array_name);

  // ---- FIRST WRITE ----
  // Prepare cell buffers
  int buffer_a1[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  uint64_t buffer_a1_size = sizeof(buffer_a1);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_open_at(ctx_, array, TILEDB_WRITE, 0);
  } else {
    rc = tiledb_array_open_at_with_key(
        ctx_,
        array,
        TILEDB_WRITE,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_),
        0);
  }
  CHECK(rc == TILEDB_ERR);  // open_at is applicable only to reads
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  } else {
    rc = tiledb_array_open_with_key(
        ctx_,
        array,
        TILEDB_WRITE,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_));
  }
  CHECK(rc == TILEDB_OK);

  // Submit query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a", buffer_a1, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Close array and clean up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  // Get timestamp after first write
  auto timestamp = TILEDB_TIMESTAMP_NOW_MS;
  std::this_thread::sleep_for(std::chrono::milliseconds(1));

  // ---- UPDATE ----
  int buffer_upd[] = {50, 60, 70};
  uint64_t buffer_upd_size = sizeof(buffer_upd);
  int64_t subarray[] = {5, 7};

  // Open array
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  } else {
    rc = tiledb_array_open_with_key(
        ctx_,
        array,
        TILEDB_WRITE,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_));
  }
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a", buffer_upd, &buffer_upd_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Close array and clean up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  // ---- NORMAL READ ----
  int buffer_read[10];
  uint64_t buffer_read_size = sizeof(buffer_read);

  // Open array
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  } else {
    rc = tiledb_array_open_with_key(
        ctx_,
        array,
        TILEDB_READ,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_));
  }
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc =
      tiledb_query_set_buffer(ctx_, query, "a", buffer_read, &buffer_read_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Close array and clean up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  // Check correctness
  int buffer_read_c[] = {1, 2, 3, 4, 50, 60, 70, 8, 9, 10};
  CHECK(!std::memcmp(buffer_read, buffer_read_c, sizeof(buffer_read_c)));
  CHECK(buffer_read_size == sizeof(buffer_read_c));

  // ---- READ AT ZERO TIMESTAMP ----
  // Open array
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_open_at(ctx_, array, TILEDB_READ, 0);
  } else {
    rc = tiledb_array_open_at_with_key(
        ctx_,
        array,
        TILEDB_READ,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_),
        0);
  }
  CHECK(rc == TILEDB_OK);

  // Check timestamp
  uint64_t timestamp_get;
  rc = tiledb_array_get_timestamp(ctx_, array, &timestamp_get);
  CHECK(rc == TILEDB_OK);
  CHECK(timestamp_get == 0);

  // Submit query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc =
      tiledb_query_set_buffer(ctx_, query, "a", buffer_read, &buffer_read_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Close array and clean up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  // Check correctness
  CHECK(buffer_read_size == 0);  // Empty array

  // ---- READ AT TIMESTAMP BEFORE UPDATE ----
  buffer_read_size = sizeof(buffer_read);

  // Open array
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_open_at(ctx_, array, TILEDB_READ, timestamp);
  } else {
    rc = tiledb_array_open_at_with_key(
        ctx_,
        array,
        TILEDB_READ,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_),
        timestamp);
  }
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc =
      tiledb_query_set_buffer(ctx_, query, "a", buffer_read, &buffer_read_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Close array and clean up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  // Check correctness
  int buffer_read_at_c[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  CHECK(!std::memcmp(buffer_read, buffer_read_at_c, sizeof(buffer_read_at_c)));
  CHECK(buffer_read_size == sizeof(buffer_read_at_c));

  // ---- READ AT LATER TIMESTAMP ----
  uint64_t first_timestamp = timestamp;
  timestamp = TILEDB_TIMESTAMP_NOW_MS;
  // Open array
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_array_open_at(ctx_, array, TILEDB_READ, timestamp);
  } else {
    rc = tiledb_array_open_at_with_key(
        ctx_,
        array,
        TILEDB_READ,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_),
        timestamp);
  }
  CHECK(rc == TILEDB_OK);

  // Check timestamp
  rc = tiledb_array_get_timestamp(ctx_, array, &timestamp_get);
  CHECK(rc == TILEDB_OK);
  CHECK(timestamp_get == timestamp);

  // Submit query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc =
      tiledb_query_set_buffer(ctx_, query, "a", buffer_read, &buffer_read_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Clean up but don't close the array yet (we will reopen it).
  tiledb_query_free(&query);

  // Check correctness
  CHECK(!std::memcmp(buffer_read, buffer_read_c, sizeof(buffer_read_c)));
  CHECK(buffer_read_size == sizeof(buffer_read_c));

  // ---- REOPEN AT FIRST TIMESTAMP ----
  buffer_read_size = sizeof(buffer_read);

  // Reopen array
  rc = tiledb_array_reopen_at(ctx_, array, first_timestamp);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc =
      tiledb_query_set_buffer(ctx_, query, "a", buffer_read, &buffer_read_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Close array and clean up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_query_free(&query);
  tiledb_array_free(&array);

  // Check correctness
  int buffer_read_reopen_c[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  CHECK(!std::memcmp(
      buffer_read, buffer_read_reopen_c, sizeof(buffer_read_reopen_c)));
  CHECK(buffer_read_size == sizeof(buffer_read_reopen_c));

  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    ArrayFx,
    "C API: Check writing coordinates out of bounds",
    "[capi], [array], [array-write-coords-oob]") {
  std::string temp_dir = FILE_URI_PREFIX + FILE_TEMP_DIR;
  std::string array_name = temp_dir + "array-write-coords-oob";
  create_temp_dir(temp_dir);

  int64_t buffer_coords[6];
  int buffer_a1[3];
  uint64_t buffer_a1_size, buffer_coords_size;
  int rc;
  bool check_coords_oob = true;

  // Create TileDB context
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  tiledb_ctx_t* ctx = nullptr;

  SECTION("- Check out-of-bounds coordinates") {
    check_coords_oob = true;
    REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
    REQUIRE(error == nullptr);
    rc = tiledb_config_set(config, "sm.check_coord_oob", "true", &error);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(error == nullptr);

    SECTION("** 1D") {
      SECTION("### dense") {
        create_dense_vector(array_name);
      }

      SECTION("### sparse") {
        create_dense_vector(array_name);
      }

      // Prepare cell buffers
      buffer_coords[0] = 1;
      buffer_coords[1] = 2;
      buffer_coords[2] = 30;
      buffer_a1[0] = 1;
      buffer_a1[1] = 2;
      buffer_a1[2] = 3;
      buffer_coords_size = 3 * sizeof(int64_t);
      buffer_a1_size = 3 * sizeof(int);
    }

    SECTION("** 2D") {
      SECTION("### dense") {
        create_dense_array(array_name);
      }

      SECTION("### sparse") {
        create_dense_array(array_name);
      }

      // Prepare cell buffers
      buffer_coords[0] = 1;
      buffer_coords[1] = 1;
      buffer_coords[2] = 2;
      buffer_coords[3] = 30;
      buffer_coords[4] = 3;
      buffer_coords[5] = 3;
      buffer_a1[0] = 1;
      buffer_a1[1] = 2;
      buffer_a1[2] = 3;
      buffer_coords_size = 6 * sizeof(int64_t);
      buffer_a1_size = 3 * sizeof(int);
    }
  }

  SECTION("- Do not check out-of-bounds coordinates") {
    check_coords_oob = false;
    REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
    REQUIRE(error == nullptr);
    rc = tiledb_config_set(config, "sm.check_coord_oob", "false", &error);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(error == nullptr);

    SECTION("** 1D") {
      SECTION("### dense") {
        create_dense_vector(array_name);
      }

      SECTION("### sparse") {
        create_dense_vector(array_name);
      }

      // Prepare cell buffers
      buffer_coords[0] = 1;
      buffer_coords[1] = 2;
      buffer_coords[2] = 30;
      buffer_a1[0] = 1;
      buffer_a1[1] = 2;
      buffer_a1[2] = 3;
      buffer_coords_size = 3 * sizeof(int64_t);
      buffer_a1_size = 3 * sizeof(int);
    }

    SECTION("** 2D") {
      SECTION("### dense") {
        create_dense_array(array_name);
      }

      SECTION("### sparse") {
        create_sparse_array(array_name);
      }

      // Prepare cell buffers
      buffer_coords[0] = 1;
      buffer_coords[1] = 1;
      buffer_coords[2] = 2;
      buffer_coords[3] = 30;
      buffer_coords[4] = 3;
      buffer_coords[5] = 3;
      buffer_a1[0] = 1;
      buffer_a1[1] = 2;
      buffer_a1[2] = 3;
      buffer_coords_size = 6 * sizeof(int64_t);
      buffer_a1_size = 3 * sizeof(int);
    }
  }

  REQUIRE(tiledb_ctx_alloc(config, &ctx) == TILEDB_OK);
  REQUIRE(error == nullptr);
  tiledb_config_free(&config);

  // Open array
  tiledb_array_t* array;
  rc = tiledb_array_alloc(ctx, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Submit query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx, query, "a", buffer_a1, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(
      ctx, query, TILEDB_COORDS, buffer_coords, &buffer_coords_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx, query);
  if (check_coords_oob)
    CHECK(rc == TILEDB_ERR);
  else
    CHECK(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx, query);
  CHECK(rc == TILEDB_OK);

  // Close array and clean up
  rc = tiledb_array_close(ctx, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_ctx_free(&ctx);

  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    ArrayFx, "C API: Test empty array", "[capi], [array], [array-empty]") {
  std::string array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + "array_empty";
  create_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);

  create_sparse_vector(array_name);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  // Buffers
  int buff_a[10];
  uint64_t buff_a_size = sizeof(buff_a);

  // Submit query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_buffer(ctx_, query, "a", buff_a, &buff_a_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Check status
  tiledb_query_status_t status;
  rc = tiledb_query_get_status(ctx_, query, &status);
  CHECK(rc == TILEDB_OK);
  CHECK(status == TILEDB_COMPLETED);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // No results
  CHECK(buff_a_size == 0);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  remove_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
}
