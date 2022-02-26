/**
 * @file   unit-capi-array.cc
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
 * Tests of C API for (dense or sparse) array operations.
 */

#include "catch.hpp"
#include "tiledb/sm/c_api/tiledb.h"

#include <iostream>

#include "catch.hpp"
#include "test/src/helpers.h"
#include "test/src/vfs_helpers.h"
#ifdef _WIN32
#include <Windows.h>
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif
#include "tiledb/common/stdx_string.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_serialization.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/global_state/unit_test_config.h"
#include "tiledb/sm/misc/utils.h"

#include <chrono>
#include <climits>
#include <iostream>
#include <sstream>
#include <thread>

using namespace tiledb::test;

struct ArrayFx {
  // TileDB context
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;

  // Vector of supported filesystems
  const std::vector<std::unique_ptr<SupportedFs>> fs_vec_;

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
  static std::string random_name(const std::string& prefix);
  static int get_fragment_timestamps(const char* path, void* data);
  void array_serialize_wrapper(
      tiledb_array_t* array, tiledb_array_t* new_array);
};

static const std::string test_ca_path =
    std::string(TILEDB_TEST_INPUTS_DIR) + "/test_certs";

static const std::string test_ca_file =
    std::string(TILEDB_TEST_INPUTS_DIR) + "/test_certs/public.crt";

ArrayFx::ArrayFx()
    : fs_vec_(vfs_test_get_fs_vec()) {
  // Initialize vfs test
  REQUIRE(vfs_test_init(fs_vec_, &ctx_, &vfs_).ok());
}

ArrayFx::~ArrayFx() {
  // Close vfs test
  REQUIRE(vfs_test_close(fs_vec_, ctx_, vfs_).ok());
  tiledb_vfs_free(&vfs_);
  tiledb_ctx_free(&ctx_);
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

std::string ArrayFx::random_name(const std::string& prefix) {
  std::stringstream ss;
  ss << prefix << "-" << std::this_thread::get_id() << "-"
     << TILEDB_TIMESTAMP_NOW_MS;
  return ss.str();
}

int ArrayFx::get_fragment_timestamps(const char* path, void* data) {
  auto data_vec = (std::vector<uint64_t>*)data;
  std::pair<uint64_t, uint64_t> timestamp_range;
  if (tiledb::sm::utils::parse::ends_with(
          path, tiledb::sm::constants::write_file_suffix)) {
    auto uri = tiledb::sm::URI(path);
    if (tiledb::sm::utils::parse::get_timestamp_range(uri, &timestamp_range)
            .ok())
      data_vec->push_back(timestamp_range.first);
  }

  return 1;
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
      ctx_, "d1", TILEDB_INT64, dim_domain, &tile_extent, &dim);
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
  tiledb_domain_free(&domain);
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
  tiledb_domain_free(&domain);
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
      ctx_, "d1", TILEDB_INT64, dim_domain, &tile_extent, &dim);
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
  if (encryption_type_ != TILEDB_NO_ENCRYPTION) {
    tiledb_ctx_free(&ctx_);
    tiledb_vfs_free(&vfs_);
    tiledb_config_t* cfg;
    tiledb_error_t* err = nullptr;
    rc = tiledb_config_alloc(&cfg, &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    std::string encryption_type_string =
        encryption_type_str((tiledb::sm::EncryptionType)encryption_type_);
    rc = tiledb_config_set(
        cfg, "sm.encryption_type", encryption_type_string.c_str(), &err);
    REQUIRE(err == nullptr);
    rc = tiledb_config_set(cfg, "sm.encryption_key", encryption_key_, &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    tiledb::sm::UnitTestConfig::instance().array_encryption_key_length.reset();
    REQUIRE(vfs_test_init(fs_vec_, &ctx_, &vfs_, cfg).ok());
    tiledb_config_free(&cfg);
  }
  rc = tiledb_array_create(ctx_, path.c_str(), array_schema);
  REQUIRE(rc == TILEDB_OK);
  tiledb_attribute_free(&attr);
  tiledb_dimension_free(&dim);
  tiledb_domain_free(&domain);
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
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

void ArrayFx::array_serialize_wrapper(
    tiledb_array_t* array, tiledb_array_t* new_array) {
  // Serialize the array
  tiledb_buffer_t* buff;
  REQUIRE(
      tiledb_serialize_array(
          ctx_,
          array,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          1,
          &buff) == TILEDB_OK);

  // Load array from the rest server
  REQUIRE(
      tiledb_deserialize_array(
          ctx_,
          buff,
          (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
          0,
          &new_array) == TILEDB_OK);

  // Clean up.
  tiledb_buffer_free(&buff);
}

TEST_CASE_METHOD(
    ArrayFx, "C API: Test getting array URI", "[capi][array][array-uri]") {
  SupportedFsLocal local_fs;
  std::string array_name =
      local_fs.file_prefix() + local_fs.temp_dir() + "array_uri";
  create_temp_dir(local_fs.file_prefix() + local_fs.temp_dir());

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
  unsigned length = MAX_PATH;
  rc = tiledb_uri_to_path(ctx_, uri, path, &length);
  CHECK(rc == TILEDB_OK);
  CHECK(!strcmp(path, array_name.c_str()));
#else
  CHECK(!strcmp(uri, array_name.c_str()));
#endif

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);

  remove_temp_dir(local_fs.file_prefix() + local_fs.temp_dir());
}

TEST_CASE_METHOD(
    ArrayFx, "C API: Set null URI", "[capi][array][array-null-uri]") {
  // Create context
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, nullptr, &array);
  CHECK(rc == TILEDB_ERR);
}

TEST_CASE_METHOD(
    ArrayFx, "C API: Set invalid URI", "[capi][array][array-invalid-uri]") {
  std::string array_name = "this_is_not_a_valid_array_uri";
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_ERR);
  int is_open;
  rc = tiledb_array_is_open(ctx_, array, &is_open);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_open == 0);
  tiledb_array_free(&array);
}

TEST_CASE_METHOD(
    ArrayFx, "C API: Test array with encryption", "[capi][array][encryption]") {
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

  // Instantiate local class
  SupportedFsLocal local_fs;

  // Check for invalid array schema
  rc = tiledb_array_schema_check(ctx_, array_schema);
  REQUIRE(rc == TILEDB_OK);

  std::string array_name =
      local_fs.file_prefix() + local_fs.temp_dir() + "encrypyted_array";
  create_temp_dir(local_fs.file_prefix() + local_fs.temp_dir());

  SECTION("- API calls with encrypted schema") {
    const char key[] = "0123456789abcdeF0123456789abcdeF";
    uint32_t key_len = (uint32_t)strlen(key);

    // Check error with invalid key length
    tiledb_config_t* cfg;
    tiledb_error_t* err = nullptr;
    rc = tiledb_config_alloc(&cfg, &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_config_set(cfg, "sm.encryption_type", "AES_256_GCM", &err);
    REQUIRE(err == nullptr);
    rc = tiledb_config_set(cfg, "sm.encryption_key", key, &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    tiledb::sm::UnitTestConfig::instance().array_encryption_key_length.set(31);
    tiledb_ctx_t* ctx_invalid_key_len_1;
    tiledb_vfs_t* vfs_invalid_key_len_1;
    REQUIRE(vfs_test_init(
                fs_vec_, &ctx_invalid_key_len_1, &vfs_invalid_key_len_1, cfg)
                .ok());
    rc = tiledb_array_create(
        ctx_invalid_key_len_1, array_name.c_str(), array_schema);
    REQUIRE(rc == TILEDB_ERR);
    tiledb_ctx_free(&ctx_invalid_key_len_1);
    tiledb_vfs_free(&vfs_invalid_key_len_1);

    rc = tiledb_config_set(
        cfg, "sm.encryption_type", "TILEDB_NO_ENCRYPTION", &err);
    REQUIRE(err == nullptr);
    tiledb::sm::UnitTestConfig::instance().array_encryption_key_length.set(
        key_len);
    tiledb_ctx_t* ctx_invalid_key_len_2;
    tiledb_vfs_t* vfs_invalid_key_len_2;
    REQUIRE(vfs_test_init(
                fs_vec_, &ctx_invalid_key_len_2, &vfs_invalid_key_len_2, cfg)
                .ok());
    rc = tiledb_array_create(
        ctx_invalid_key_len_2, array_name.c_str(), array_schema);
    REQUIRE(rc == TILEDB_ERR);
    tiledb_ctx_free(&ctx_invalid_key_len_2);
    tiledb_vfs_free(&vfs_invalid_key_len_2);
    // remove the empty array directory
    remove_temp_dir(array_name);

    // Create array with proper key
    rc = tiledb_config_set(cfg, "sm.encryption_type", "AES_256_GCM", &err);
    REQUIRE(err == nullptr);
    tiledb_ctx_t* ctx_proper_key;
    tiledb_vfs_t* vfs_proper_key;
    REQUIRE(vfs_test_init(fs_vec_, &ctx_proper_key, &vfs_proper_key, cfg).ok());
    rc = tiledb_array_create(ctx_proper_key, array_name.c_str(), array_schema);
    REQUIRE(rc == TILEDB_OK);
    tiledb_ctx_free(&ctx_proper_key);
    tiledb_vfs_free(&vfs_proper_key);

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
    REQUIRE(tiledb_config_alloc(&cfg, &err) == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_config_set(cfg, "sm.encryption_type", "NO_ENCRYPTION", &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_config_set(cfg, "sm.encryption_key", key, &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_array_set_config(ctx_, array, cfg);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_array_open(ctx_, array, TILEDB_READ);
    REQUIRE(rc == TILEDB_ERR);
    rc = tiledb_array_is_open(ctx_, array, &is_open);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(is_open == 0);

    // Check error with bad key
    char bad_key[32];
    rc = tiledb_config_set(cfg, "sm.encryption_type", "AES_256_GCM", &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_config_set(cfg, "sm.encryption_key", bad_key, &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_array_set_config(ctx_, array, cfg);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_array_open(ctx_, array, TILEDB_READ);
    REQUIRE(rc == TILEDB_ERR);
    rc = tiledb_array_is_open(ctx_, array, &is_open);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(is_open == 0);

    // Check error with bad key length
    REQUIRE(
        tiledb_config_set(cfg, "sm.encryption_key", key, &err) == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_array_set_config(ctx_, array, cfg);
    REQUIRE(rc == TILEDB_OK);
    tiledb::sm::UnitTestConfig::instance().array_encryption_key_length.set(
        key_len - 1);
    rc = tiledb_array_open(ctx_, array, TILEDB_READ);
    REQUIRE(rc == TILEDB_ERR);
    rc = tiledb_array_is_open(ctx_, array, &is_open);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(is_open == 0);

    // Use correct key
    tiledb::sm::UnitTestConfig::instance().array_encryption_key_length.set(
        key_len);
    rc = tiledb_array_open(ctx_, array, TILEDB_READ);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_array_is_open(ctx_, array, &is_open);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(is_open == 1);
    tiledb_array_schema_t* read_schema;
    rc = tiledb_array_get_schema(ctx_, array, &read_schema);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_config_set(cfg, "sm.encryption_key", bad_key, &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_array_set_config(ctx_, array, cfg);
    REQUIRE(rc == TILEDB_OK);

    // Opening an already open array without a key should fail
    tiledb_array_t* array2;
    rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array2);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_array_open(ctx_, array2, TILEDB_READ);
    REQUIRE(rc == TILEDB_ERR);

    // Opening an array with a bad key should fail
    rc = tiledb_array_set_config(ctx_, array2, cfg);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_array_open(ctx_, array2, TILEDB_READ);
    REQUIRE(rc == TILEDB_ERR);

    // Check reopening works
    rc = tiledb_array_reopen(ctx_, array);
    REQUIRE(rc == TILEDB_OK);

    // Close arrays
    rc = tiledb_array_close(ctx_, array2);
    REQUIRE(
        rc == TILEDB_OK);  // Array not opened successfully, closing is a noop
    rc = tiledb_array_close(ctx_, array);
    REQUIRE(rc == TILEDB_OK);

    // Check loading schema requires key
    tiledb_array_schema_free(&read_schema);
    rc = tiledb_array_schema_load(ctx_, array_name.c_str(), &read_schema);
    REQUIRE(rc == TILEDB_ERR);
    // Check with bad key
    rc = tiledb_config_set(cfg, "sm.encryption_type", "AES_256_GCM", &err);
    REQUIRE(err == nullptr);
    rc = tiledb_config_set(cfg, "sm.encryption_key", bad_key, &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    tiledb_ctx_t* ctx_bad_key;
    tiledb_vfs_t* vfs_bad_key;
    REQUIRE(vfs_test_init(fs_vec_, &ctx_bad_key, &vfs_bad_key, cfg).ok());
    rc =
        tiledb_array_schema_load(ctx_bad_key, array_name.c_str(), &read_schema);
    REQUIRE(rc == TILEDB_ERR);
    tiledb_ctx_free(&ctx_bad_key);
    tiledb_vfs_free(&vfs_bad_key);
    // Check with correct key
    rc = tiledb_config_set(cfg, "sm.encryption_type", "AES_256_GCM", &err);
    REQUIRE(err == nullptr);
    rc = tiledb_config_set(cfg, "sm.encryption_key", key, &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    tiledb_ctx_t* ctx_correct_key;
    tiledb_vfs_t* vfs_correct_key;
    REQUIRE(
        vfs_test_init(fs_vec_, &ctx_correct_key, &vfs_correct_key, cfg).ok());
    rc = tiledb_array_schema_load(
        ctx_correct_key, array_name.c_str(), &read_schema);
    REQUIRE(rc == TILEDB_OK);
    tiledb_ctx_free(&ctx_correct_key);
    tiledb_vfs_free(&vfs_correct_key);

    // Check opening after closing still requires a key.
    rc = tiledb_array_open(ctx_, array, TILEDB_READ);
    REQUIRE(rc == TILEDB_ERR);
    rc = tiledb_array_is_open(ctx_, array, &is_open);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(is_open == 0);
    rc = tiledb_config_set(cfg, "sm.encryption_key", bad_key, &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_array_set_config(ctx_, array, cfg);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_array_set_config(ctx_, array, cfg);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_array_open(ctx_, array, TILEDB_READ);
    REQUIRE(rc == TILEDB_ERR);
    rc = tiledb_array_is_open(ctx_, array, &is_open);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(is_open == 0);
    rc = tiledb_config_set(cfg, "sm.encryption_key", key, &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_array_set_config(ctx_, array, cfg);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_array_open(ctx_, array, TILEDB_READ);
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
    tiledb_config_free(&cfg);
    remove_temp_dir(local_fs.file_prefix() + local_fs.temp_dir());
  }

  SECTION("- API calls with unencrypted schema") {
    // Check for invalid array schema
    rc = tiledb_array_schema_check(ctx_, array_schema);
    REQUIRE(rc == TILEDB_OK);

    // Check create ok with null key
    tiledb_config_t* cfg;
    tiledb_error_t* err = nullptr;
    rc = tiledb_config_alloc(&cfg, &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    tiledb_ctx_t* ctx_null_key;
    tiledb_vfs_t* vfs_null_key;
    rc = tiledb_config_set(cfg, "sm.encryption_key", "", &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    REQUIRE(vfs_test_init(fs_vec_, &ctx_null_key, &vfs_null_key, cfg).ok());
    rc = tiledb_array_create(ctx_, array_name.c_str(), array_schema);
    REQUIRE(rc == TILEDB_OK);
    tiledb_ctx_free(&ctx_null_key);
    tiledb_vfs_free(&vfs_null_key);

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
    const char key[] = "0123456789abcdeF0123456789abcdeF";
    rc = tiledb_config_set(cfg, "sm.encryption_type", "AES_256_GCM", &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_config_set(cfg, "sm.encryption_key", key, &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    tiledb::sm::UnitTestConfig::instance().array_encryption_key_length.reset();
    rc = tiledb_array_set_config(ctx_, array, cfg);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_array_open(ctx_, array, TILEDB_READ);
    REQUIRE(rc == TILEDB_ERR);
    int is_open;
    rc = tiledb_array_is_open(ctx_, array, &is_open);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(is_open == 0);

    // Check ok with null key
    rc = tiledb_config_set(cfg, "sm.encryption_type", "NO_ENCRYPTION", &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_config_set(cfg, "sm.encryption_key", "0", &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_array_set_config(ctx_, array, cfg);
    REQUIRE(rc == TILEDB_OK);
    tiledb::sm::UnitTestConfig::instance().array_encryption_key_length.set(0);
    rc = tiledb_array_open(ctx_, array, TILEDB_READ);
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
    rc = tiledb_config_set(cfg, "sm.encryption_type", "AES_256_GCM", &err);
    REQUIRE(err == nullptr);
    rc = tiledb_config_set(cfg, "sm.encryption_key", key, &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    tiledb_ctx_t* ctx_schema;
    tiledb_vfs_t* vfs_schema;
    REQUIRE(vfs_test_init(fs_vec_, &ctx_schema, &vfs_schema, cfg).ok());
    rc = tiledb_array_schema_load(ctx_schema, array_name.c_str(), &read_schema);
    REQUIRE(rc == TILEDB_ERR);
    tiledb_ctx_free(&ctx_schema);
    tiledb_vfs_free(&vfs_schema);

    // Check ok with nullptr
    rc = tiledb_config_set(cfg, "sm.encryption_key", "", &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    tiledb_ctx_t* ctx_nullptr;
    tiledb_vfs_t* vfs_nullptr;
    REQUIRE(vfs_test_init(fs_vec_, &ctx_nullptr, &vfs_nullptr, cfg).ok());
    rc = tiledb_array_schema_load(ctx_, array_name.c_str(), &read_schema);
    REQUIRE(rc == TILEDB_OK);
    tiledb_ctx_free(&ctx_nullptr);
    tiledb_vfs_free(&vfs_nullptr);

    // Clean up
    tiledb_array_schema_free(&read_schema);
    tiledb_array_free(&array);
    tiledb_config_free(&cfg);
    remove_temp_dir(local_fs.file_prefix() + local_fs.temp_dir());
  }
}

TEST_CASE_METHOD(
    ArrayFx,
    "C API: Test opening array at timestamp, reads",
    "[capi][array][open-at][reads]") {
  // TODO: refactor for each supported FS.
  std::string temp_dir = fs_vec_[0]->temp_dir();

  std::string array_name = temp_dir + "array-open-at-reads";
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
  if (encryption_type_ != TILEDB_NO_ENCRYPTION) {
    tiledb_config_t* cfg;
    tiledb_error_t* err = nullptr;
    rc = tiledb_config_alloc(&cfg, &err);
    CHECK(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    std::string encryption_type_string =
        encryption_type_str((tiledb::sm::EncryptionType)encryption_type_);
    rc = tiledb_config_set(
        cfg, "sm.encryption_type", encryption_type_string.c_str(), &err);
    CHECK(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_config_set(cfg, "sm.encryption_key", encryption_key_, &err);
    CHECK(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_array_set_config(ctx_, array, cfg);
    CHECK(rc == TILEDB_OK);
    tiledb_config_free(&cfg);
    uint32_t key_len = (uint32_t)strlen(encryption_key_);
    tiledb::sm::UnitTestConfig::instance().array_encryption_key_length.set(
        key_len);
  }
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Submit query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a", buffer_a1, &buffer_a1_size);
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

  // ---- UPDATE ----
  int buffer_upd[] = {50, 60, 70};
  uint64_t buffer_upd_size = sizeof(buffer_upd);
  int64_t subarray[] = {5, 7};

  // Open array
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  if (encryption_type_ != TILEDB_NO_ENCRYPTION) {
    tiledb_config_t* cfg;
    tiledb_error_t* err = nullptr;
    rc = tiledb_config_alloc(&cfg, &err);
    CHECK(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    std::string encryption_type_string =
        encryption_type_str((tiledb::sm::EncryptionType)encryption_type_);
    rc = tiledb_config_set(
        cfg, "sm.encryption_type", encryption_type_string.c_str(), &err);
    CHECK(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_config_set(cfg, "sm.encryption_key", encryption_key_, &err);
    CHECK(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_array_set_config(ctx_, array, cfg);
    CHECK(rc == TILEDB_OK);
    tiledb_config_free(&cfg);
  }
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a", buffer_upd, &buffer_upd_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Close array and clean up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  std::vector<uint64_t> fragment_timestamps;
  rc = tiledb_vfs_ls(
      ctx_,
      vfs_,
      get_commit_dir(array_name).c_str(),
      &get_fragment_timestamps,
      &fragment_timestamps);
  CHECK(rc == TILEDB_OK);

  // ---- NORMAL READ ----
  int buffer_read[10];
  uint64_t buffer_read_size = sizeof(buffer_read);

  // Open array
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  if (encryption_type_ != TILEDB_NO_ENCRYPTION) {
    tiledb_config_t* cfg;
    tiledb_error_t* err = nullptr;
    rc = tiledb_config_alloc(&cfg, &err);
    CHECK(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    std::string encryption_type_string =
        encryption_type_str((tiledb::sm::EncryptionType)encryption_type_);
    rc = tiledb_config_set(
        cfg, "sm.encryption_type", encryption_type_string.c_str(), &err);
    CHECK(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_config_set(cfg, "sm.encryption_key", encryption_key_, &err);
    CHECK(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_array_set_config(ctx_, array, cfg);
    CHECK(rc == TILEDB_OK);
    tiledb_config_free(&cfg);
  }
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Submit query
  int64_t subarray_read[] = {1, 10};
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray_read);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a", buffer_read, &buffer_read_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Close array and clean up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  // Check correctness
  int32_t buffer_read_c[] = {1, 2, 3, 4, 50, 60, 70, 8, 9, 10};
  CHECK(!std::memcmp(buffer_read, buffer_read_c, sizeof(buffer_read_c)));
  CHECK(buffer_read_size == sizeof(buffer_read_c));

  // ---- READ AT ZERO TIMESTAMP ----

  tiledb_config_t* cfg;
  tiledb_error_t* err = nullptr;
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  REQUIRE(tiledb_config_alloc(&cfg, &err) == TILEDB_OK);
  REQUIRE(err == nullptr);
  rc = tiledb_array_set_open_timestamp_end(ctx_, array, 0);
  REQUIRE(rc == TILEDB_OK);

  // Open array
  if (encryption_type_ != TILEDB_NO_ENCRYPTION) {
    std::string encryption_type_string =
        encryption_type_str((tiledb::sm::EncryptionType)encryption_type_);
    rc = tiledb_config_set(
        cfg, "sm.encryption_type", encryption_type_string.c_str(), &err);
    CHECK(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_config_set(cfg, "sm.encryption_key", encryption_key_, &err);
    CHECK(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_array_set_config(ctx_, array, cfg);
    CHECK(rc == TILEDB_OK);
  }
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Check timestamp
  uint64_t timestamp_get;
  rc = tiledb_array_get_open_timestamp_end(ctx_, array, &timestamp_get);
  CHECK(rc == TILEDB_OK);
  CHECK(timestamp_get == 0);

  // Submit query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray_read);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a", buffer_read, &buffer_read_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Close array and clean up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_config_free(&cfg);

  // Check correctness
  // Empty array still returns fill values
  CHECK(buffer_read_size == 10 * sizeof(int32_t));

  // ---- READ AT TIMESTAMP BEFORE UPDATE ----
  buffer_read_size = sizeof(buffer_read);

  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_set_open_timestamp_end(ctx_, array, fragment_timestamps[0]);
  REQUIRE(rc == TILEDB_OK);

  // Open array
  REQUIRE(tiledb_config_alloc(&cfg, &err) == TILEDB_OK);
  REQUIRE(err == nullptr);
  if (encryption_type_ != TILEDB_NO_ENCRYPTION) {
    std::string encryption_type_string =
        encryption_type_str((tiledb::sm::EncryptionType)encryption_type_);
    rc = tiledb_config_set(
        cfg, "sm.encryption_type", encryption_type_string.c_str(), &err);
    CHECK(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_config_set(cfg, "sm.encryption_key", encryption_key_, &err);
    CHECK(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_array_set_config(ctx_, array, cfg);
    CHECK(rc == TILEDB_OK);
  }
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray_read);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a", buffer_read, &buffer_read_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Close array and clean up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_config_free(&cfg);

  // Check correctness
  int buffer_read_at_c[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  CHECK(!std::memcmp(buffer_read, buffer_read_at_c, sizeof(buffer_read_at_c)));
  CHECK(buffer_read_size == sizeof(buffer_read_at_c));

  // ---- READ AT LATER TIMESTAMP ----
  // Open array
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);

  rc = tiledb_array_set_open_timestamp_end(ctx_, array, fragment_timestamps[1]);
  REQUIRE(rc == TILEDB_OK);

  REQUIRE(tiledb_config_alloc(&cfg, &err) == TILEDB_OK);
  REQUIRE(err == nullptr);
  if (encryption_type_ != TILEDB_NO_ENCRYPTION) {
    std::string encryption_type_string =
        encryption_type_str((tiledb::sm::EncryptionType)encryption_type_);
    rc = tiledb_config_set(
        cfg, "sm.encryption_type", encryption_type_string.c_str(), &err);
    CHECK(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_config_set(cfg, "sm.encryption_key", encryption_key_, &err);
    CHECK(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_array_set_config(ctx_, array, cfg);
    CHECK(rc == TILEDB_OK);
  }

  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Check timestamp
  rc = tiledb_array_get_open_timestamp_end(ctx_, array, &timestamp_get);
  CHECK(rc == TILEDB_OK);
  CHECK(timestamp_get == fragment_timestamps[1]);

  // Submit query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray_read);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a", buffer_read, &buffer_read_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Clean up but don't close the array yet (we will reopen it).
  tiledb_query_free(&query);
  tiledb_config_free(&cfg);

  // Check correctness
  CHECK(!std::memcmp(buffer_read, buffer_read_c, sizeof(buffer_read_c)));
  CHECK(buffer_read_size == sizeof(buffer_read_c));

  // ---- REOPEN AT FIRST TIMESTAMP ----
  buffer_read_size = sizeof(buffer_read);

  rc = tiledb_array_set_open_timestamp_end(
      ctx_, array, fragment_timestamps[1] - 1);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_array_reopen(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray_read);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a", buffer_read, &buffer_read_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Clean up but don't close the array yet (we will reopen it).
  tiledb_query_free(&query);

  // Check correctness
  int buffer_read_reopen_c[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  CHECK(!std::memcmp(
      buffer_read, buffer_read_reopen_c, sizeof(buffer_read_reopen_c)));
  CHECK(buffer_read_size == sizeof(buffer_read_reopen_c));

  // ---- REOPEN STARTING AT FIRST TIMESTAMP ----
  buffer_read_size = sizeof(buffer_read);

  // Reopen array
  rc = tiledb_array_set_open_timestamp_start(
      ctx_, array, fragment_timestamps[0] + 1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_set_open_timestamp_end(ctx_, array, UINT64_MAX);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_reopen(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray_read);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a", buffer_read, &buffer_read_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Close array and clean up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_query_free(&query);
  tiledb_array_free(&array);

  // Check correctness
  int buffer_read_reopen_start_c[] = {INT_MIN,
                                      INT_MIN,
                                      INT_MIN,
                                      INT_MIN,
                                      50,
                                      60,
                                      70,
                                      INT_MIN,
                                      INT_MIN,
                                      INT_MIN};
  CHECK(!std::memcmp(
      buffer_read,
      buffer_read_reopen_start_c,
      sizeof(buffer_read_reopen_start_c)));
  CHECK(buffer_read_size == sizeof(buffer_read_reopen_start_c));

  // ---- OPEN STARTING AT FIRST TIMESTAMP ----
  buffer_read_size = sizeof(buffer_read);

  // Open array
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_set_open_timestamp_start(
      ctx_, array, fragment_timestamps[1]);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(tiledb_config_alloc(&cfg, &err) == TILEDB_OK);
  REQUIRE(err == nullptr);
  if (encryption_type_ != TILEDB_NO_ENCRYPTION) {
    std::string encryption_type_string =
        encryption_type_str((tiledb::sm::EncryptionType)encryption_type_);
    rc = tiledb_config_set(
        cfg, "sm.encryption_type", encryption_type_string.c_str(), &err);
    CHECK(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_config_set(cfg, "sm.encryption_key", encryption_key_, &err);
    CHECK(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_array_set_config(ctx_, array, cfg);
    CHECK(rc == TILEDB_OK);
  }
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray_read);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a", buffer_read, &buffer_read_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Close array and clean up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_query_free(&query);
  tiledb_array_free(&array);
  tiledb_config_free(&cfg);

  // Check correctness
  // Check correctness
  int buffer_read_open_start_c[] = {INT_MIN,
                                    INT_MIN,
                                    INT_MIN,
                                    INT_MIN,
                                    50,
                                    60,
                                    70,
                                    INT_MIN,
                                    INT_MIN,
                                    INT_MIN};
  CHECK(!std::memcmp(
      buffer_read, buffer_read_open_start_c, sizeof(buffer_read_open_start_c)));
  CHECK(buffer_read_size == sizeof(buffer_read_open_start_c));

  // ---- OPEN STARTING AT PAST LAST TIMESTAMP ----
  buffer_read_size = sizeof(buffer_read);

  // Open array
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_set_open_timestamp_start(
      ctx_, array, fragment_timestamps[1] + 1);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(tiledb_config_alloc(&cfg, &err) == TILEDB_OK);
  REQUIRE(err == nullptr);
  if (encryption_type_ != TILEDB_NO_ENCRYPTION) {
    std::string encryption_type_string =
        encryption_type_str((tiledb::sm::EncryptionType)encryption_type_);
    rc = tiledb_config_set(
        cfg, "sm.encryption_type", encryption_type_string.c_str(), &err);
    CHECK(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_config_set(cfg, "sm.encryption_key", encryption_key_, &err);
    CHECK(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_array_set_config(ctx_, array, cfg);
    CHECK(rc == TILEDB_OK);
  }
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray_read);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a", buffer_read, &buffer_read_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Close array and clean up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_query_free(&query);
  tiledb_array_free(&array);
  tiledb_config_free(&cfg);

  // Check correctness
  int buffer_read_open_start_now_c[] = {INT_MIN,
                                        INT_MIN,
                                        INT_MIN,
                                        INT_MIN,
                                        INT_MIN,
                                        INT_MIN,
                                        INT_MIN,
                                        INT_MIN,
                                        INT_MIN,
                                        INT_MIN};
  CHECK(!std::memcmp(
      buffer_read,
      buffer_read_open_start_now_c,
      sizeof(buffer_read_open_start_now_c)));
  CHECK(buffer_read_size == sizeof(buffer_read_open_start_now_c));

  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    ArrayFx,
    "C API: Test opening array at timestamp, writes",
    "[capi][array][open-at][writes]") {
  // TODO: refactor for each supported FS.
  std::string temp_dir = fs_vec_[0]->temp_dir();

  std::string array_name = temp_dir + "array-open-at-writes";
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

  // ---- WRITE ----
  // Prepare cell buffers
  int buffer_a1[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  uint64_t buffer_a1_size = sizeof(buffer_a1);

  // Some timestamp, it could be anything
  uint64_t timestamp = 1000;

  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  tiledb_config_t* cfg = nullptr;
  tiledb_error_t* err = nullptr;
  REQUIRE(tiledb_config_alloc(&cfg, &err) == TILEDB_OK);
  REQUIRE(err == nullptr);

  rc = tiledb_array_set_open_timestamp_end(ctx_, array, timestamp);
  REQUIRE(rc == TILEDB_OK);

  // Open array
  if (encryption_type_ != TILEDB_NO_ENCRYPTION) {
    std::string encryption_type_string =
        encryption_type_str((tiledb::sm::EncryptionType)encryption_type_);
    rc = tiledb_config_set(
        cfg, "sm.encryption_type", encryption_type_string.c_str(), &err);
    CHECK(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_config_set(cfg, "sm.encryption_key", encryption_key_, &err);
    CHECK(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_array_set_config(ctx_, array, cfg);
    CHECK(rc == TILEDB_OK);
  }
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Submit query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a", buffer_a1, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Get written timestamp
  uint64_t timestamp_get;
  rc = tiledb_array_get_open_timestamp_end(ctx_, array, &timestamp_get);
  CHECK(rc == TILEDB_OK);

  uint64_t t1, t2;
  rc = tiledb_query_get_fragment_timestamp_range(ctx_, query, 0, &t1, &t2);
  CHECK(rc == TILEDB_OK);
  CHECK(timestamp_get == t1);
  CHECK(timestamp_get == t2);

  // Close array and clean up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_config_free(&cfg);

  // ---- READ AT ZERO TIMESTAMP ----

  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_set_open_timestamp_end(ctx_, array, 0);
  REQUIRE(rc == TILEDB_OK);

  // Open array
  REQUIRE(tiledb_config_alloc(&cfg, &err) == TILEDB_OK);
  REQUIRE(err == nullptr);
  if (encryption_type_ != TILEDB_NO_ENCRYPTION) {
    std::string encryption_type_string =
        encryption_type_str((tiledb::sm::EncryptionType)encryption_type_);
    rc = tiledb_config_set(
        cfg, "sm.encryption_type", encryption_type_string.c_str(), &err);
    CHECK(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_config_set(cfg, "sm.encryption_key", encryption_key_, &err);
    CHECK(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_array_set_config(ctx_, array, cfg);
    CHECK(rc == TILEDB_OK);
  }
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Check timestamp
  rc = tiledb_array_get_open_timestamp_end(ctx_, array, &timestamp_get);
  CHECK(rc == TILEDB_OK);
  CHECK(timestamp_get == 0);

  // Submit query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  int64_t subarray_read[] = {1, 10};
  rc = tiledb_query_set_subarray(ctx_, query, subarray_read);
  CHECK(rc == TILEDB_OK);
  int buffer_read[10];
  uint64_t buffer_read_size = sizeof(buffer_read);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a", buffer_read, &buffer_read_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Close array and clean up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_config_free(&cfg);

  // Check correctness
  // Empty array still returns fill values
  CHECK(buffer_read_size == 10 * sizeof(int32_t));

  // ---- READ AT THE WRITTEN TIMESTAMP ----
  buffer_read_size = sizeof(buffer_read);

  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_set_open_timestamp_end(ctx_, array, timestamp);
  REQUIRE(rc == TILEDB_OK);

  // Open array
  REQUIRE(tiledb_config_alloc(&cfg, &err) == TILEDB_OK);
  REQUIRE(err == nullptr);
  if (encryption_type_ != TILEDB_NO_ENCRYPTION) {
    std::string encryption_type_string =
        encryption_type_str((tiledb::sm::EncryptionType)encryption_type_);
    rc = tiledb_config_set(
        cfg, "sm.encryption_type", encryption_type_string.c_str(), &err);
    CHECK(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_config_set(cfg, "sm.encryption_key", encryption_key_, &err);
    CHECK(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_array_set_config(ctx_, array, cfg);
    CHECK(rc == TILEDB_OK);
  }
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray(ctx_, query, subarray_read);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a", buffer_read, &buffer_read_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Close array and clean up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_config_free(&cfg);

  // Check correctness
  int buffer_read_at_c[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  CHECK(!std::memcmp(buffer_read, buffer_read_at_c, sizeof(buffer_read_at_c)));
  CHECK(buffer_read_size == sizeof(buffer_read_at_c));

  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    ArrayFx,
    "C API: Check writing coordinates out of bounds",
    "[capi][array][array-write-coords-oob]") {
  SupportedFsLocal local_fs;
  std::string temp_dir = local_fs.file_prefix() + local_fs.temp_dir();
  std::string array_name = temp_dir + "array-write-coords-oob";
  create_temp_dir(temp_dir);

  int dimension = 0;
  int64_t buffer_coords_dim1[3];
  int64_t buffer_coords_dim2[3];
  int buffer_a1[3];
  uint64_t buffer_a1_size, buffer_coords_size;
  int rc;
  bool check_coords_oob = true;

  // Create TileDB context
  tiledb_config_t* cfg = nullptr;
  tiledb_error_t* err = nullptr;
  tiledb_ctx_t* ctx = nullptr;

  SECTION("- Check out-of-bounds coordinates") {
    check_coords_oob = true;
    REQUIRE(tiledb_config_alloc(&cfg, &err) == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_config_set(cfg, "sm.check_coord_oob", "true", &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);

    SECTION("** 1D") {
      dimension = 1;
      create_sparse_vector(array_name);

      // Prepare cell buffers
      buffer_coords_dim1[0] = 1;
      buffer_coords_dim1[1] = 2;
      buffer_coords_dim1[2] = 30;
      buffer_a1[0] = 1;
      buffer_a1[1] = 2;
      buffer_a1[2] = 3;
      buffer_coords_size = 3 * sizeof(int64_t);
      buffer_a1_size = 3 * sizeof(int);
    }

    SECTION("** 2D") {
      dimension = 2;
      create_sparse_array(array_name);

      // Prepare cell buffers
      buffer_coords_dim1[0] = 1;
      buffer_coords_dim1[1] = 2;
      buffer_coords_dim1[2] = 3;
      buffer_coords_dim2[0] = 1;
      buffer_coords_dim2[1] = 30;
      buffer_coords_dim2[2] = 3;
      buffer_a1[0] = 1;
      buffer_a1[1] = 2;
      buffer_a1[2] = 3;
      buffer_coords_size = 3 * sizeof(int64_t);
      buffer_a1_size = 3 * sizeof(int);
    }
  }

  SECTION("- Do not check out-of-bounds coordinates") {
    check_coords_oob = false;
    REQUIRE(tiledb_config_alloc(&cfg, &err) == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_config_set(cfg, "sm.check_coord_oob", "false", &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);

    SECTION("** 1D") {
      dimension = 1;
      create_sparse_vector(array_name);

      // Prepare cell buffers
      buffer_coords_dim1[0] = 1;
      buffer_coords_dim1[1] = 2;
      buffer_coords_dim1[2] = 30;
      buffer_a1[0] = 1;
      buffer_a1[1] = 2;
      buffer_a1[2] = 3;
      buffer_coords_size = 3 * sizeof(int64_t);
      buffer_a1_size = 3 * sizeof(int);
    }

    SECTION("** 2D") {
      dimension = 2;
      create_sparse_array(array_name);

      // Prepare cell buffers
      buffer_coords_dim1[0] = 1;
      buffer_coords_dim1[1] = 2;
      buffer_coords_dim1[2] = 3;
      buffer_coords_dim2[0] = 1;
      buffer_coords_dim2[1] = 30;
      buffer_coords_dim2[2] = 3;
      buffer_a1[0] = 1;
      buffer_a1[1] = 2;
      buffer_a1[2] = 3;
      buffer_coords_size = 3 * sizeof(int64_t);
      buffer_a1_size = 3 * sizeof(int);
    }
  }

  REQUIRE(tiledb_ctx_alloc(cfg, &ctx) == TILEDB_OK);
  REQUIRE(err == nullptr);
  tiledb_config_free(&cfg);

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
  rc =
      tiledb_query_set_data_buffer(ctx, query, "a", buffer_a1, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx, query, "d1", buffer_coords_dim1, &buffer_coords_size);
  CHECK(rc == TILEDB_OK);
  if (dimension == 2) {
    rc = tiledb_query_set_data_buffer(
        ctx, query, "d2", buffer_coords_dim2, &buffer_coords_size);
    CHECK(rc == TILEDB_OK);
  }
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
    ArrayFx, "C API: Test empty array", "[capi][array][array-empty]") {
  SupportedFsLocal local_fs;
  std::string array_name =
      local_fs.file_prefix() + local_fs.temp_dir() + "array_empty";
  create_temp_dir(local_fs.file_prefix() + local_fs.temp_dir());

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
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", buff_a, &buff_a_size);
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

  remove_temp_dir(local_fs.file_prefix() + local_fs.temp_dir());
}

TEST_CASE_METHOD(
    ArrayFx,
    "C API: Test query errors, getting subarray info from write queries in "
    "sparse arrays",
    "[capi][query][error][sparse]") {
  SupportedFsLocal local_fs;
  std::string array_name =
      local_fs.file_prefix() + local_fs.temp_dir() + "query_error_sparse";
  create_temp_dir(local_fs.file_prefix() + local_fs.temp_dir());

  create_sparse_vector(array_name);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  // Prepare query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  uint64_t range_num;
  rc = tiledb_query_get_range_num(ctx_, query, 0, &range_num);
  CHECK(rc == TILEDB_ERR);
  const void *start, *end, *stride;
  rc = tiledb_query_get_range(ctx_, query, 0, 0, &start, &end, &stride);
  CHECK(rc == TILEDB_ERR);
  int64_t s = 10;
  int64_t e = 20;
  rc = tiledb_query_add_range(ctx_, query, 0, &s, &e, nullptr);
  CHECK(rc == TILEDB_ERR);
  int64_t subarray[] = {-1, 2};
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_ERR);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  remove_temp_dir(local_fs.file_prefix() + local_fs.temp_dir());
}

TEST_CASE_METHOD(
    ArrayFx,
    "C API: Test query errors, dense writes",
    "[capi][query][error][dense]") {
  SupportedFsLocal local_fs;
  std::string array_name =
      local_fs.file_prefix() + local_fs.temp_dir() + "query_error_dense";
  create_temp_dir(local_fs.file_prefix() + local_fs.temp_dir());

  create_dense_array(array_name);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  int32_t a[] = {1, 2, 3, 4};
  uint64_t a_size = sizeof(a);

  // Prepare query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", a, &a_size);
  CHECK(rc == TILEDB_OK);
  uint64_t range_num;
  rc = tiledb_query_get_range_num(ctx_, query, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);  // The default
  const void *start, *end, *stride;
  rc = tiledb_query_get_range(ctx_, query, 0, 0, &start, &end, &stride);
  CHECK(rc == TILEDB_OK);
  CHECK(*(const uint64_t*)start == 1);
  CHECK(*(const uint64_t*)end == 10);
  int64_t s = 1;
  int64_t e = 2;
  rc = tiledb_query_add_range(ctx_, query, 0, &s, &e, nullptr);
  CHECK(rc == TILEDB_OK);

  int64_t subarray[] = {2, 3, 4, 5};
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_add_range(ctx_, query, 0, &s, &e, nullptr);
  CHECK(rc == TILEDB_ERR);

  rc = tiledb_query_get_range_num(ctx_, query, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);
  rc = tiledb_query_get_range(ctx_, query, 0, 0, &start, &end, &stride);
  CHECK(rc == TILEDB_OK);
  CHECK(*(const uint64_t*)start == 2);
  CHECK(*(const uint64_t*)end == 3);
  rc = tiledb_query_get_range(ctx_, query, 1, 0, &start, &end, &stride);
  CHECK(rc == TILEDB_OK);
  CHECK(*(const uint64_t*)start == 4);
  CHECK(*(const uint64_t*)end == 5);

  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_ERR);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  remove_temp_dir(local_fs.file_prefix() + local_fs.temp_dir());
}

TEST_CASE_METHOD(
    ArrayFx,
    "C API: Test query errors, dense unordered writes",
    "[capi][query][error][dense]") {
  SupportedFsLocal local_fs;
  std::string array_name =
      local_fs.file_prefix() + local_fs.temp_dir() + "query_error_dense";
  create_temp_dir(local_fs.file_prefix() + local_fs.temp_dir());

  create_dense_array(array_name);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  // Prepare query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);

  rc = tiledb_query_set_layout(ctx_, query, TILEDB_UNORDERED);
  CHECK(rc == TILEDB_ERR);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  remove_temp_dir(local_fs.file_prefix() + local_fs.temp_dir());
}

TEST_CASE_METHOD(
    ArrayFx,
    "C API: Test query errors, dense reads in global order",
    "[capi][query][error][dense]") {
  SupportedFsLocal local_fs;
  std::string array_name =
      local_fs.file_prefix() + local_fs.temp_dir() + "query_error_dense";
  create_temp_dir(local_fs.file_prefix() + local_fs.temp_dir());

  create_dense_array(array_name);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  int32_t a[4];
  uint64_t a_size = sizeof(a);

  // Prepare query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", a, &a_size);
  CHECK(rc == TILEDB_OK);

  int64_t subarray[] = {2, 3, 4, 5};
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
  int64_t s = 1;
  int64_t e = 2;
  rc = tiledb_query_add_range(ctx_, query, 0, &s, &e, nullptr);
  CHECK(rc == TILEDB_OK);

  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_ERR);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  remove_temp_dir(local_fs.file_prefix() + local_fs.temp_dir());
}

TEST_CASE_METHOD(
    ArrayFx, "Test array serialization", "[array][serialization]") {
#ifdef TILEDB_SERIALIZATION
  SupportedFsLocal local_fs;
  std::string array_name =
      local_fs.file_prefix() + local_fs.temp_dir() + "array_serialization";
  create_temp_dir(local_fs.file_prefix() + local_fs.temp_dir());

  create_dense_array(array_name);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  int32_t a[4];
  uint64_t a_size = sizeof(a);

  // Prepare query
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", a, &a_size);
  CHECK(rc == TILEDB_OK);

  int64_t subarray[] = {2, 3, 4, 5};
  rc = tiledb_query_set_subarray(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);

  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);

  // Reopen array in WRITE mode
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  // Write metadata
  int32_t v = 5;
  rc = tiledb_array_put_metadata(ctx_, array, "aaa", TILEDB_INT32, 1, &v);
  CHECK(rc == TILEDB_OK);
  float f[] = {1.1f, 1.2f};
  rc = tiledb_array_put_metadata(ctx_, array, "bb", TILEDB_FLOAT32, 2, f);
  CHECK(rc == TILEDB_OK);

  // Serialize array and deserialize into new_array
  tiledb_array_t* new_array = nullptr;
  array_serialize_wrapper(array, new_array);

  // Close array and clean up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &new_array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, new_array, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  int is_empty;
  uint64_t domain[4];
  rc = tiledb_array_get_non_empty_domain(ctx_, new_array, domain, &is_empty);
  CHECK(rc == TILEDB_OK);
  CHECK(is_empty == 1);

  // Close new_array
  rc = tiledb_array_close(ctx_, new_array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&new_array);

  /** Validate metadata. **/
  // Open new_array in read mode
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &new_array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, new_array, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  // Read
  const void* v_r;
  tiledb_datatype_t v_type;
  uint32_t v_num;
  rc = tiledb_array_get_metadata(ctx_, new_array, "aaa", &v_type, &v_num, &v_r);
  CHECK(rc == TILEDB_OK);
  CHECK(v_type == TILEDB_INT32);
  CHECK(v_num == 1);
  CHECK(*((const int32_t*)v_r) == 5);

  rc = tiledb_array_get_metadata(ctx_, new_array, "bb", &v_type, &v_num, &v_r);
  CHECK(rc == TILEDB_OK);
  CHECK(v_type == TILEDB_FLOAT32);
  CHECK(v_num == 2);
  CHECK(((const float*)v_r)[0] == 1.1f);
  CHECK(((const float*)v_r)[1] == 1.2f);

  uint64_t num = 0;
  rc = tiledb_array_get_metadata_num(ctx_, new_array, &num);
  CHECK(rc == TILEDB_OK);
  CHECK(num == 2);

  const char* key;
  uint32_t key_len;
  rc = tiledb_array_get_metadata_from_index(
      ctx_, new_array, 1, &key, &key_len, &v_type, &v_num, &v_r);
  CHECK(rc == TILEDB_OK);
  CHECK(v_type == TILEDB_FLOAT32);
  CHECK(v_num == 2);
  CHECK(((const float*)v_r)[0] == 1.1f);
  CHECK(((const float*)v_r)[1] == 1.2f);
  CHECK(key_len == strlen("bb"));
  CHECK(!strncmp(key, "bb", strlen("bb")));

  // Check has_key
  int32_t has_key = 0;
  rc = tiledb_array_has_metadata_key(ctx_, new_array, "bb", &v_type, &has_key);
  CHECK(rc == TILEDB_OK);
  CHECK(v_type == TILEDB_FLOAT32);
  CHECK(has_key == 1);

  // Close array
  rc = tiledb_array_close(ctx_, new_array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&new_array);

  remove_temp_dir(local_fs.file_prefix() + local_fs.temp_dir());
#endif
}