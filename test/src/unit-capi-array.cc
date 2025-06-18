/**
 * @file   unit-capi-array.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB Inc.
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
#include "tiledb/api/c_api/array/array_api_internal.h"
#include "tiledb/api/c_api/array_schema/array_schema_api_external.h"
#include "tiledb/api/c_api/array_schema/array_schema_api_internal.h"
#include "tiledb/api/c_api/buffer/buffer_api_internal.h"
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/common/stdx_string.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_serialization.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/fragment/fragment_identifier.h"
#include "tiledb/sm/global_state/unit_test_config.h"
#include "tiledb/sm/serialization/array.h"
#include "tiledb/sm/serialization/fragments.h"

#include <chrono>
#include <climits>
#include <iostream>
#include <sstream>
#include <thread>

#ifdef _WIN32
#if !defined(NOMINMAX)
#define NOMINMAX
#endif
#if !defined(WIN32_LEAN_AND_MEAN)
#define WIN32_LEAN_AND_MEAN
#endif
#include <Windows.h>
#endif

using namespace tiledb::test;
using namespace tiledb::common;
using namespace tiledb::sm;

struct ArrayFx {
  // The memory tracker
  shared_ptr<tiledb::sm::MemoryTracker> memory_tracker_;

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
  void write_fragment(tiledb_array_t* array, uint64_t timestamp);
  static int get_fragment_timestamps(const char* path, void* data);
};

static const std::string test_ca_path =
    std::string(TILEDB_TEST_INPUTS_DIR) + "/test_certs";

static const std::string test_ca_file =
    std::string(TILEDB_TEST_INPUTS_DIR) + "/test_certs/public.crt";

ArrayFx::ArrayFx()
    : memory_tracker_(tiledb::test::create_test_memory_tracker())
    , fs_vec_(vfs_test_get_fs_vec()) {
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

int ArrayFx::get_fragment_timestamps(const char* path, void* data) {
  auto data_vec = (std::vector<uint64_t>*)data;
  if (utils::parse::ends_with(path, constants::write_file_suffix)) {
    FragmentID fragment_id{path};
    auto timestamp_range{fragment_id.timestamp_range()};
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

void ArrayFx::write_fragment(tiledb_array_t* array, uint64_t timestamp) {
  // Open the array at the given timestamp
  int rc = tiledb_array_set_open_timestamp_end(ctx_, array, timestamp);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  // Buffers
  int buffer[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  uint64_t buffer_size = sizeof(buffer);

  // Write to the array
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", buffer, &buffer_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit_and_finalize(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Clean up
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_query_free(&query);
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
    ArrayFx,
    "C API: Test array with encryption",
    "[capi][array][encryption][non-rest]") {
  // Create array schema
  tiledb_array_schema_t* array_schema;
  int rc = tiledb_array_schema_alloc(ctx_, TILEDB_SPARSE, &array_schema);
  REQUIRE(rc == TILEDB_OK);

  // Create dimensions
  tiledb_dimension_t* d1;
  const int64_t d1_domain[2] = {0, 99};
  const int64_t tile_extent[1] = {10};
  rc = tiledb_dimension_alloc(
      ctx_, "d", TILEDB_INT64, &d1_domain[0], &tile_extent[0], &d1);
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
    const char bad_key_len[] = "bad_key_len";
    const char wrong_key[] = "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF";

    // Check error with invalid key length
    tiledb_config_t* cfg;
    tiledb_error_t* err = nullptr;
    rc = tiledb_config_alloc(&cfg, &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_config_set(cfg, "sm.encryption_type", "AES_256_GCM", &err);
    REQUIRE(err == nullptr);
    rc = tiledb_config_set(cfg, "sm.encryption_key", bad_key_len, &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
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
    rc = tiledb_config_set(cfg, "sm.encryption_key", key, &err);
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

    // Check error with wrong key
    rc = tiledb_config_set(cfg, "sm.encryption_type", "AES_256_GCM", &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_config_set(cfg, "sm.encryption_key", wrong_key, &err);
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
        tiledb_config_set(cfg, "sm.encryption_key", bad_key_len, &err) ==
        TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_array_set_config(ctx_, array, cfg);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_array_open(ctx_, array, TILEDB_READ);
    REQUIRE(rc == TILEDB_ERR);
    rc = tiledb_array_is_open(ctx_, array, &is_open);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(is_open == 0);

    // Use correct key
    REQUIRE(
        tiledb_config_set(cfg, "sm.encryption_key", key, &err) == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_array_set_config(ctx_, array, cfg);
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
    rc = tiledb_config_set(cfg, "sm.encryption_key", key, &err);
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
    REQUIRE(
        tiledb_config_set(cfg, "sm.encryption_key", wrong_key, &err) ==
        TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_array_set_config(ctx_, array2, cfg);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_array_open(ctx_, array2, TILEDB_READ);
    REQUIRE(rc == TILEDB_ERR);

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
    rc = tiledb_config_set(cfg, "sm.encryption_key", wrong_key, &err);
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
    rc = tiledb_config_set(cfg, "sm.encryption_key", "", &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_array_set_config(ctx_, array, cfg);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_array_open(ctx_, array, TILEDB_READ);
    REQUIRE(rc == TILEDB_ERR);
    rc = tiledb_array_is_open(ctx_, array, &is_open);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(is_open == 0);
    rc = tiledb_config_set(cfg, "sm.encryption_key", wrong_key, &err);
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
    "[capi][array][open-at][reads][rest]") {
  // TODO: refactor for each supported FS.
  std::string temp_dir = fs_vec_[0]->temp_dir();
  std::string array_path = temp_dir + "array-open-at-reads";
  std::string array_name = vfs_array_uri(fs_vec_[0], array_path, ctx_);

  SECTION("- without encryption") {
    encryption_type_ = TILEDB_NO_ENCRYPTION;
    encryption_key_ = nullptr;
  }

  if (!fs_vec_[0]->is_rest()) {
    SECTION("- with encryption") {
      encryption_type_ = TILEDB_AES_256_GCM;
      encryption_key_ = "0123456789abcdeF0123456789abcdeF";
    }
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
  rc = tiledb_query_submit_and_finalize(ctx_, query);
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

  // Create subarray
  tiledb_subarray_t* sub;
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, sub);
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
  tiledb_subarray_free(&sub);

  std::vector<uint64_t> fragment_timestamps;
  rc = tiledb_vfs_ls(
      ctx_,
      vfs_,
      get_commit_dir(array_path).c_str(),
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

  // Create subarray
  int64_t subarray_read[] = {1, 10};
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray_read);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, sub);
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
  tiledb_subarray_free(&sub);

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

  // Create subarray
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray_read);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, sub);
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
  tiledb_subarray_free(&sub);
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

  // Create subarray
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray_read);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, sub);
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
  tiledb_subarray_free(&sub);
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

  // Create subarray
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray_read);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, sub);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a", buffer_read, &buffer_read_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Clean up but don't close the array yet (we will reopen it).
  tiledb_query_free(&query);
  tiledb_subarray_free(&sub);
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

  // Create subarray
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray_read);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, sub);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "a", buffer_read, &buffer_read_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_OK);

  // Clean up but don't close the array yet (we will reopen it).
  tiledb_query_free(&query);
  tiledb_subarray_free(&sub);

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

  // Create subarray
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray_read);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, sub);
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
  tiledb_subarray_free(&sub);

  // Check correctness
  int buffer_read_reopen_start_c[] = {
      INT_MIN,
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

  // Create subarray
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray_read);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, sub);
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
  tiledb_subarray_free(&sub);
  tiledb_config_free(&cfg);

  // Check correctness
  int buffer_read_open_start_c[] = {
      INT_MIN,
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

  // Create subarray
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray_read);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, sub);
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
  tiledb_subarray_free(&sub);
  tiledb_config_free(&cfg);

  // Check correctness
  int buffer_read_open_start_now_c[] = {
      INT_MIN,
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
  tiledb::Array::delete_array(ctx_, array_name);
}

TEST_CASE_METHOD(
    ArrayFx,
    "C API: Test opening array at timestamp, writes",
    "[capi][array][open-at][writes][rest-fails][sc-42722]") {
  // TODO: refactor for each supported FS.
  std::string temp_dir = fs_vec_[0]->temp_dir();
  std::string array_name =
      vfs_array_uri(fs_vec_[0], temp_dir + "array-open-at-writes", ctx_);

  SECTION("- without encryption") {
    encryption_type_ = TILEDB_NO_ENCRYPTION;
    encryption_key_ = nullptr;
  }

  if (!fs_vec_[0]->is_rest()) {
    SECTION("- with encryption") {
      encryption_type_ = TILEDB_AES_256_GCM;
      encryption_key_ = "0123456789abcdeF0123456789abcdeF";
    }
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
  rc = tiledb_query_submit_and_finalize(ctx_, query);
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

  // Create subarray
  int64_t subarray_read[] = {1, 10};
  tiledb_subarray_t* sub;
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray_read);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, sub);
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
  tiledb_subarray_free(&sub);
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

  // Create subarray
  rc = tiledb_subarray_alloc(ctx_, array, &sub);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_set_subarray(ctx_, sub, subarray_read);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_alloc(ctx_, array, TILEDB_READ, &query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query, TILEDB_ROW_MAJOR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, sub);
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
  tiledb_subarray_free(&sub);
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
    "[capi][array][array-write-coords-oob][rest]") {
  std::string temp_dir = fs_vec_[0]->temp_dir();
  std::string array_name =
      vfs_array_uri(fs_vec_[0], temp_dir + "array-write-coords-oob", ctx_);
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

  tiledb_ctx_free(&ctx_);
  tiledb_vfs_free(&vfs_);
  // reallocate with input config
  vfs_test_init(fs_vec_, &ctx_, &vfs_, cfg).ok();
  tiledb_config_free(&cfg);

  // Open array
  tiledb_array_t* array;
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  CHECK(rc == TILEDB_OK);
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
  rc = tiledb_query_set_data_buffer(
      ctx_, query, "d1", buffer_coords_dim1, &buffer_coords_size);
  CHECK(rc == TILEDB_OK);
  if (dimension == 2) {
    rc = tiledb_query_set_data_buffer(
        ctx_, query, "d2", buffer_coords_dim2, &buffer_coords_size);
    CHECK(rc == TILEDB_OK);
  }
  rc = tiledb_query_submit_and_finalize(ctx_, query);
  if (check_coords_oob) {
    CHECK(rc == TILEDB_ERR);
  } else {
    CHECK(rc == TILEDB_OK);
  }

  // Close array and clean up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  remove_temp_dir(temp_dir);
  tiledb::Array::delete_array(ctx_, array_name);
}

TEST_CASE_METHOD(
    ArrayFx, "C API: Test empty array", "[capi][array][array-empty][rest]") {
  std::string temp_dir = fs_vec_[0]->temp_dir();
  std::string array_name =
      vfs_array_uri(fs_vec_[0], temp_dir + "array_empty", ctx_);

  create_temp_dir(temp_dir);

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

  remove_temp_dir(temp_dir);
  tiledb::Array::delete_array(ctx_, array_name);
}

TEST_CASE_METHOD(
    ArrayFx, "C API: Test deletion of array", "[capi][array][delete][rest]") {
  std::string temp_dir = fs_vec_[0]->temp_dir();
  std::string array_path = temp_dir + "array_delete";
  std::string array_name = vfs_array_uri(fs_vec_[0], array_path, ctx_);

  create_temp_dir(temp_dir);

  create_dense_vector(array_name);

  // Conditionally consolidate
  // Note: there's no need to vacuum; delete_array will delete all fragments
  bool consolidate = GENERATE(true, false);

  // Write fragments at timestamps 1, 3, 5
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  write_fragment(array, 1);
  write_fragment(array, 3);
  write_fragment(array, 5);

  // Check write
  // Get the number of write files in the commit directory
  tiledb::Context ctx(ctx_, false);
  tiledb::VFS vfs(ctx);
  CommitsDirectory commits_dir(vfs, array_path);
  CHECK(commits_dir.file_count(constants::write_file_suffix) == 3);
  auto uris = vfs.ls(
      array_path + "/" + tiledb::sm::constants::array_fragments_dir_name);
  CHECK(static_cast<uint32_t>(uris.size()) == 3);

  // Conditionally consolidate commits
  if (consolidate && !fs_vec_[0]->is_rest()) {
    tiledb_config_t* cfg;
    tiledb_error_t* err = nullptr;
    rc = tiledb_config_alloc(&cfg, &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_config_set(cfg, "sm.consolidation.mode", "commits", &err);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(err == nullptr);
    rc = tiledb_array_consolidate(ctx_, array_name.c_str(), cfg);
    REQUIRE(rc == TILEDB_OK);
    tiledb_config_free(&cfg);

    // Validate working directory
    CommitsDirectory commits_dir2(vfs, array_path);
    CHECK(commits_dir2.file_count(constants::write_file_suffix) == 3);
    auto uris2 = vfs.ls(
        array_path + "/" + tiledb::sm::constants::array_fragments_dir_name);
    CHECK(static_cast<uint32_t>(uris2.size()) == 3);
  }

  // Delete array data
  rc = tiledb_array_delete(ctx_, array_name.c_str());

  // Validate working directory after delete
  CommitsDirectory commits_dir3(vfs, array_path);
  CHECK(commits_dir3.file_count(constants::write_file_suffix) == 0);
  auto uris3 = vfs.ls(
      array_path + "/" + tiledb::sm::constants::array_fragments_dir_name);
  CHECK(static_cast<uint32_t>(uris3.size()) == 0);

  // Try to open array
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_ERR);

  // Clean up
  tiledb_array_free(&array);
  remove_temp_dir(temp_dir);
  tiledb::Array::delete_array(ctx_, array_name);
}

TEST_CASE_METHOD(
    ArrayFx,
    "C API: Test subarray errors, getting subarray info from write queries in "
    "sparse arrays",
    "[capi][subarray][error][sparse][rest]") {
  std::string temp_dir = fs_vec_[0]->temp_dir();
  std::string array_name =
      vfs_array_uri(fs_vec_[0], temp_dir + "query_error_sparse", ctx_);
  create_temp_dir(temp_dir);

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

  // Prepare subarray
  tiledb_subarray_t* subarray;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);
  uint64_t range_num;

  rc = tiledb_subarray_get_range_num(ctx_, subarray, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);

  // Note: deprecated function.
  const void *start, *end, *stride;
  rc = tiledb_subarray_get_range(ctx_, subarray, 0, 0, &start, &end, &stride);
  CHECK(rc == TILEDB_OK);
  CHECK(*static_cast<const int64_t*>(start) == -1);
  CHECK(*static_cast<const int64_t*>(end) == 2);
  CHECK(stride == nullptr);

  int64_t s = 10;
  int64_t e = 20;
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s, &e, nullptr);
  CHECK(rc == TILEDB_ERR);
  int64_t sub[] = {-1, 2};
  rc = tiledb_subarray_set_subarray(ctx_, subarray, sub);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_ERR);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_subarray_free(&subarray);

  remove_temp_dir(temp_dir);
  tiledb::Array::delete_array(ctx_, array_name);
}

TEST_CASE_METHOD(
    ArrayFx,
    "C API: Test query errors, dense writes",
    "[capi][query][error][dense][rest]") {
  std::string temp_dir = fs_vec_[0]->temp_dir();
  std::string array_name =
      vfs_array_uri(fs_vec_[0], temp_dir + "query_error_dense", ctx_);
  create_temp_dir(temp_dir);

  create_dense_array(array_name);

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  int32_t a[] = {1, 2, 3, 4};
  uint64_t a_size = sizeof(a);

  // Prepare query/subarray
  tiledb_query_t* query;
  rc = tiledb_query_alloc(ctx_, array, TILEDB_WRITE, &query);
  CHECK(rc == TILEDB_OK);
  tiledb_subarray_t* subarray;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(ctx_, query, "a", a, &a_size);
  CHECK(rc == TILEDB_OK);
  uint64_t range_num;
  rc = tiledb_subarray_get_range_num(ctx_, subarray, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 1);  // The default
  const void *start, *end, *stride;
  rc = tiledb_subarray_get_range(ctx_, subarray, 0, 0, &start, &end, &stride);
  CHECK(rc == TILEDB_OK);
  CHECK(*(const uint64_t*)start == 1);
  CHECK(*(const uint64_t*)end == 10);
  int64_t s = 1;
  int64_t e = 2;
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s, &e, nullptr);
  CHECK(rc == TILEDB_OK);

  int64_t sub[] = {2, 3, 4, 5};
  rc = tiledb_subarray_set_subarray(ctx_, subarray, sub);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s, &e, nullptr);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_subarray_get_range_num(ctx_, subarray, 0, &range_num);
  CHECK(rc == TILEDB_OK);
  CHECK(range_num == 2);
  rc = tiledb_subarray_get_range(ctx_, subarray, 0, 0, &start, &end, &stride);
  CHECK(rc == TILEDB_OK);
  CHECK(*(const uint64_t*)start == 2);
  CHECK(*(const uint64_t*)end == 3);
  rc = tiledb_subarray_get_range(ctx_, subarray, 1, 0, &start, &end, &stride);
  CHECK(rc == TILEDB_OK);
  CHECK(*(const uint64_t*)start == 4);
  CHECK(*(const uint64_t*)end == 5);

  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_ERR);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_subarray_free(&subarray);

  remove_temp_dir(temp_dir);
  tiledb::Array::delete_array(ctx_, array_name);
}

TEST_CASE_METHOD(
    ArrayFx,
    "C API: Test query errors, dense unordered writes",
    "[capi][query][error][dense][rest]") {
  std::string temp_dir = fs_vec_[0]->temp_dir();
  std::string array_name =
      vfs_array_uri(fs_vec_[0], temp_dir + "query_error_dense", ctx_);

  create_temp_dir(temp_dir);

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

  remove_temp_dir(temp_dir);
  tiledb::Array::delete_array(ctx_, array_name);
}

TEST_CASE_METHOD(
    ArrayFx,
    "C API: Test query errors, dense reads in global order",
    "[capi][query][error][dense][rest]") {
  std::string temp_dir = fs_vec_[0]->temp_dir();
  std::string array_name =
      vfs_array_uri(fs_vec_[0], temp_dir + "query_error_dense", ctx_);

  create_temp_dir(temp_dir);

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

  tiledb_subarray_t* subarray;
  rc = tiledb_subarray_alloc(ctx_, array, &subarray);
  CHECK(rc == TILEDB_OK);
  int64_t sub[] = {2, 3, 4, 5};
  rc = tiledb_subarray_set_subarray(ctx_, subarray, sub);
  CHECK(rc == TILEDB_OK);
  int64_t s = 1;
  int64_t e = 2;
  rc = tiledb_subarray_add_range(ctx_, subarray, 0, &s, &e, nullptr);
  CHECK(rc == TILEDB_OK);

  rc = tiledb_query_set_layout(ctx_, query, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_subarray_t(ctx_, query, subarray);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_submit(ctx_, query);
  CHECK(rc == TILEDB_ERR);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query);
  tiledb_subarray_free(&subarray);

  remove_temp_dir(temp_dir);
  tiledb::Array::delete_array(ctx_, array_name);
}

TEST_CASE_METHOD(
    ArrayFx, "Test array serialization", "[array][serialization]") {
#ifdef TILEDB_SERIALIZATION
  SupportedFsLocal local_fs;
  std::string array_name =
      local_fs.file_prefix() + local_fs.temp_dir() + "array_serialization";
  create_temp_dir(local_fs.file_prefix() + local_fs.temp_dir());

  create_dense_vector(array_name);

  // Test for both versions of array open
  bool array_v2 = GENERATE(true, false);
  if (array_v2) {
    // Set the needed config variables
    tiledb_ctx_free(&ctx_);
    tiledb_config_t* config;
    tiledb_error_t* error;
    tiledb_config_alloc(&config, &error);
    tiledb_config_set(config, "rest.use_refactored_array_open", "true", &error);
    tiledb_config_set(
        config, "rest.load_metadata_on_array_open", "true", &error);
    tiledb_config_set(
        config, "rest.load_non_empty_domain_on_array_open", "true", &error);
    tiledb_ctx_alloc(config, &ctx_);
  }

  // Open array to WRITE metadata
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  // Write metadata
  int32_t v = 5;
  rc = tiledb_array_put_metadata(ctx_, array, "aaa", TILEDB_INT32, 1, &v);
  CHECK(rc == TILEDB_OK);
  float f[] = {1.1f, 1.2f};
  rc = tiledb_array_put_metadata(ctx_, array, "bb", TILEDB_FLOAT32, 2, f);
  CHECK(rc == TILEDB_OK);

  // Write some data so that non empty domain is not empty
  int buffer_a1[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  uint64_t buffer_a1_size = sizeof(buffer_a1);

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

  // Get a reference value to check against after deserialization
  auto all_arrays = array->array_schemas_all();

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Open array to test serialization
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  // Metadata and non empty domain are not loaded automatically
  // in array open v1 but with separate requests, so we simulate
  // this here by forcing metadata loading
  if (!array_v2) {
    auto metadata = &array->metadata();
    CHECK(metadata != nullptr);
    array->non_empty_domain();
  }

  // Serialize array and deserialize into new_array
  tiledb_array_t* new_array = nullptr;
  array_serialize_wrapper(
      ctx_,
      array,
      &new_array,
      (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP);

  // Close array and clean up
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_query_free(&query);

  // Check the retrieved array schema
  auto& new_array_schema = new_array->array_schema_latest();

  auto cell_order = new_array_schema.cell_order();
  CHECK(cell_order == Layout::ROW_MAJOR);

  auto tile_order = new_array_schema.tile_order();
  CHECK(tile_order == Layout::ROW_MAJOR);

  auto num_attributes = new_array_schema.attribute_num();
  CHECK(num_attributes == 1);

  auto ndim = new_array_schema.dim_num();
  CHECK(ndim == 1);

  // Check all the retrieved arrays
  auto all_arrays_new = new_array->array_schemas_all();
  CHECK(all_arrays.size() == all_arrays_new.size());
  CHECK(std::equal(
      all_arrays.begin(),
      all_arrays.end(),
      all_arrays_new.begin(),
      [](auto a, auto b) { return a.first == b.first; }));

  // Check the retrieved non empty domain
  auto non_empty_domain = new_array->loaded_non_empty_domain();
  CHECK(non_empty_domain.empty() == false);

  // Check the retrieved metadata
  Datatype type;
  const void* v_r;
  uint32_t v_num;
  auto new_metadata = &new_array->metadata();
  new_metadata->get("aaa", &type, &v_num, &v_r);
  CHECK(static_cast<tiledb_datatype_t>(type) == TILEDB_INT32);
  CHECK(v_num == 1);
  CHECK(*((const int32_t*)v_r) == 5);

  new_metadata->get("bb", &type, &v_num, &v_r);
  CHECK(static_cast<tiledb_datatype_t>(type) == TILEDB_FLOAT32);
  CHECK(v_num == 2);
  CHECK(((const float*)v_r)[0] == 1.1f);
  CHECK(((const float*)v_r)[1] == 1.2f);

  auto num = new_metadata->num();
  CHECK(num == 2);

  tiledb_array_free(&new_array);
  remove_temp_dir(local_fs.file_prefix() + local_fs.temp_dir());
#endif
}

TEST_CASE_METHOD(
    ArrayFx, "Test dimension datatypes", "[array][dimension][datatypes]") {
  SupportedFsLocal local_fs;
  std::string array_name =
      local_fs.file_prefix() + local_fs.temp_dir() + "array_dim_types";
  create_temp_dir(local_fs.file_prefix() + local_fs.temp_dir());

  uint64_t dim_domain[] = {1, 10, 1, 10};
  uint64_t tile_extent = 2;
  tiledb_dimension_t* dim;

  SECTION("- valid and supported Datatypes") {
    std::vector<tiledb_datatype_t> valid_supported_types = {
        TILEDB_UINT64, TILEDB_INT64};

    for (auto dim_type : valid_supported_types) {
      int rc = tiledb_dimension_alloc(
          ctx_, "dim", dim_type, dim_domain, &tile_extent, &dim);
      REQUIRE(rc == TILEDB_OK);
    }
    tiledb_dimension_free(&dim);
  }

  SECTION("- valid and unsupported Datatypes") {
    std::vector<tiledb_datatype_t> valid_unsupported_types = {
        TILEDB_CHAR, TILEDB_BOOL};

    for (auto dim_type : valid_unsupported_types) {
      int rc = tiledb_dimension_alloc(
          ctx_, "dim", dim_type, dim_domain, &tile_extent, &dim);
      REQUIRE(rc == TILEDB_ERR);
    }
  }

  SECTION("- invalid Datatypes") {
    std::vector<std::underlying_type_t<tiledb_datatype_t>> invalid_datatypes = {
        42, 100};

    for (auto dim_type : invalid_datatypes) {
      int rc = tiledb_dimension_alloc(
          ctx_,
          "dim",
          tiledb_datatype_t(dim_type),
          dim_domain,
          &tile_extent,
          &dim);
      REQUIRE(rc == TILEDB_ERR);
    }
  }
}

TEST_CASE_METHOD(
    ArrayFx,
    "Test array open serialization",
    "[array][array_open][serialization]") {
#ifdef TILEDB_SERIALIZATION
  const char* array_name = "array_open_serialization";

  // Create TileDB context
  tiledb_ctx_t* ctx;
  tiledb_ctx_alloc(NULL, &ctx);

  // The array will be 4x4 with dimensions "rows" and "cols", with domain [1,4].
  int dim_domain[] = {1, 4, 1, 4};
  int tile_extents[] = {4, 4};
  tiledb_dimension_t* d1;
  tiledb_dimension_alloc(
      ctx, "rows", TILEDB_INT32, &dim_domain[0], &tile_extents[0], &d1);
  tiledb_dimension_t* d2;
  tiledb_dimension_alloc(
      ctx, "cols", TILEDB_INT32, &dim_domain[2], &tile_extents[1], &d2);

  // Create domain
  tiledb_domain_t* domain;
  tiledb_domain_alloc(ctx, &domain);
  tiledb_domain_add_dimension(ctx, domain, d1);
  tiledb_domain_add_dimension(ctx, domain, d2);

  // Create a single attribute "a" so each (i,j) cell can store an integer
  tiledb_attribute_t* a;
  tiledb_attribute_alloc(ctx, "a", TILEDB_INT32, &a);

  // Create array schema
  tiledb_array_schema_t* array_schema;
  tiledb_array_schema_alloc(ctx, TILEDB_DENSE, &array_schema);
  tiledb_array_schema_set_cell_order(ctx, array_schema, TILEDB_ROW_MAJOR);
  tiledb_array_schema_set_tile_order(ctx, array_schema, TILEDB_ROW_MAJOR);
  tiledb_array_schema_set_domain(ctx, array_schema, domain);
  tiledb_array_schema_add_attribute(ctx, array_schema, a);

  // Set a few config variables
  tiledb_ctx_free(&ctx);
  tiledb_config_t* config;
  tiledb_error_t* error;
  tiledb_config_alloc(&config, &error);
  tiledb_config_set(config, "rest.use_refactored_array_open", "true", &error);
  tiledb_config_set(
      config, "rest.load_metadata_on_array_open", "false", &error);
  tiledb_config_set(
      config, "rest.load_non_empty_domain_on_array_open", "false", &error);
  tiledb_ctx_alloc(config, &ctx);

  // Create the array
  tiledb_array_create(ctx, array_name, array_schema);

  // Serialize array and deserialize into deserialized_array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx, array_name, &array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_t* deserialized_array = nullptr;
  rc = tiledb_array_open_serialize(
      ctx,
      array,
      &deserialized_array,
      (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP);
  REQUIRE(rc == TILEDB_OK);

  // Check that the original and de-serialized array have the same config
  tiledb_config_t* deserialized_config;
  rc = tiledb_array_get_config(ctx, deserialized_array, &deserialized_config);
  REQUIRE(rc == TILEDB_OK);
  uint8_t equal = 0;
  rc = tiledb_config_compare(config, deserialized_config, &equal);
  REQUIRE(rc == TILEDB_OK);
  // Check that they are equal
  CHECK(equal == 1);

  // Check that the config variables have the right value in deserialized array
  const char* value = nullptr;
  rc = tiledb_config_get(
      config, "rest.use_refactored_array_open", &value, &error);
  CHECK(rc == TILEDB_OK);
  CHECK(error == nullptr);
  CHECK(!strcmp(value, "true"));
  value = nullptr;
  rc = tiledb_config_get(
      config, "rest.load_metadata_on_array_open", &value, &error);
  CHECK(rc == TILEDB_OK);
  CHECK(error == nullptr);
  CHECK(!strcmp(value, "false"));
  value = nullptr;
  rc = tiledb_config_get(
      config, "rest.load_non_empty_domain_on_array_open", &value, &error);
  CHECK(rc == TILEDB_OK);
  CHECK(error == nullptr);
  CHECK(!strcmp(value, "false"));

  // Clean up
  tiledb_array_free(&deserialized_array);
  tiledb_attribute_free(&a);
  tiledb_dimension_free(&d1);
  tiledb_dimension_free(&d2);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
  tiledb_config_free(&config);
  tiledb_ctx_free(&ctx);
  remove_temp_dir(array_name);
#endif
}

TEST_CASE_METHOD(
    ArrayFx,
    "Test array and query serialization",
    "[array][serialization][query]") {
#ifdef TILEDB_SERIALIZATION
  SupportedFsLocal local_fs;
  std::string array_name =
      local_fs.file_prefix() + local_fs.temp_dir() + "array_serialization";
  create_temp_dir(local_fs.file_prefix() + local_fs.temp_dir());

  create_dense_vector(array_name);

  // Set the related config variables (optional)
  tiledb_ctx_free(&ctx_);
  tiledb_config_t* config;
  tiledb_error_t* error;
  tiledb_config_alloc(&config, &error);
  tiledb_config_set(config, "rest.load_metadata_on_array_open", "true", &error);
  tiledb_config_set(
      config, "rest.load_non_empty_domain_on_array_open", "true", &error);
  tiledb_ctx_alloc(config, &ctx_);

  // Allocate array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);

  // Simulate serializing array_open request to Server and deserializing on
  // server side. First set the query_type that will be serialized
  auto query_type_w = QueryType::WRITE;
  array->set_query_type(query_type_w);
  tiledb_array_t* deserialized_array_server = nullptr;
  // 1. Client -> Server : Send array_open_request (serialize)
  // 2. Server : Receive and deserialize array_open_request
  rc = tiledb_array_open_serialize(
      ctx_,
      array,
      &deserialized_array_server,
      (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP);
  REQUIRE(rc == TILEDB_OK);

  // Check that the original and de-serialized array have the same query type
  REQUIRE(deserialized_array_server->get_query_type() == query_type_w);

  // 3. Server: Open the array the request was received for in the requested
  // mode
  // This is needed in test, as the deserialized array has a dummy array_uri of
  // "deserialized_array" set instead of the original one. The Cloud side
  // already knows the array URI so it's not a problem in the real life scenario
  deserialized_array_server->set_array_uri(array->array_uri());
  rc = tiledb_array_open(
      ctx_,
      deserialized_array_server,
      static_cast<tiledb_query_type_t>(query_type_w));
  REQUIRE(rc == TILEDB_OK);

  // 4. Server -> Client: Send opened Array (serialize)
  // 5. Client: Receive and deserialize Array (into "array")
  tiledb_buffer_t* buff;
  rc = tiledb_serialize_array(
      ctx_,
      deserialized_array_server,
      (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
      1,
      &buff);
  REQUIRE(rc == TILEDB_OK);
  tiledb::sm::serialization::array_deserialize(
      array->array().get(),
      tiledb::sm::SerializationType::CAPNP,
      buff->buffer(),
      ctx_->context().resources(),
      memory_tracker_);

  // 6. Server: Close array and clean up
  rc = tiledb_array_close(ctx_, deserialized_array_server);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&deserialized_array_server);
  tiledb_buffer_free(&buff);

  // 7. Client: Prepare query
  int buffer_a1[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  uint64_t buffer_a1_size = sizeof(buffer_a1);
  tiledb_query_t* query_client;
  rc = tiledb_query_alloc(
      ctx_,
      array,
      static_cast<tiledb_query_type_t>(query_type_w),
      &query_client);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_layout(ctx_, query_client, TILEDB_GLOBAL_ORDER);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_data_buffer(
      ctx_, query_client, "a", buffer_a1, &buffer_a1_size);
  CHECK(rc == TILEDB_OK);

  // 8. Client -> Server : Send query request for writing into the array we just
  // opened
  tiledb_query_t* deserialized_query;
  std::vector<uint8_t> serialized;
  rc = serialize_query(ctx_, query_client, &serialized, 1);
  REQUIRE(rc == TILEDB_OK);

  // 9. Server: Deserialize query request
  // Allocate new context for server
  tiledb_ctx_t* server_ctx;
  tiledb_ctx_alloc(config, &server_ctx);
  rc = deserialize_array_and_query(
      server_ctx, serialized, &deserialized_query, array_name.c_str(), 0);

  // 10. Server: Submit query WITHOUT re-opening the array, using under the hood
  // the array found in the deserialized query
  rc = tiledb_query_submit(server_ctx, deserialized_query);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_finalize(server_ctx, deserialized_query);
  CHECK(rc == TILEDB_OK);

  // 11. Server: serialize the query with the results back to the client
  rc = serialize_query(server_ctx, deserialized_query, &serialized, 0);
  REQUIRE(rc == TILEDB_OK);

  tiledb_ctx_free(&server_ctx);
  CHECK(server_ctx == nullptr);

  // 12. Client: Deserialize query
  rc = deserialize_query(ctx_, serialized, query_client, 1);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  tiledb_array_free(&array);
  tiledb_query_free(&query_client);
  tiledb_query_free(&deserialized_query);

  remove_temp_dir(local_fs.file_prefix() + local_fs.temp_dir());
#endif
}

TEST_CASE_METHOD(
    ArrayFx,
    "Test array fragments serialization",
    "[array][fragments][serialization]") {
#ifdef TILEDB_SERIALIZATION
  SupportedFsLocal local_fs;
  std::string array_name = local_fs.file_prefix() + local_fs.temp_dir() +
                           "array_fragments_serialization";
  create_temp_dir(local_fs.file_prefix() + local_fs.temp_dir());
  create_dense_vector(array_name);

  // Write fragments at timestamps 1, 2
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  uint64_t start_timestamp = 1;
  uint64_t end_timestamp = 2;
  write_fragment(array, start_timestamp);
  write_fragment(array, end_timestamp);
  CHECK(tiledb::test::num_commits(array_name) == 2);
  CHECK(tiledb::test::num_fragments(array_name) == 2);

  // Reopen for modify exclusive.
  tiledb_array_free(&array);
  rc = tiledb_array_alloc(ctx_, array_name.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_array_open(ctx_, array, TILEDB_MODIFY_EXCLUSIVE);
  REQUIRE(rc == TILEDB_OK);

  // ALlocate buffer
  tiledb_buffer_t* buff;
  rc = tiledb_buffer_alloc(ctx_, &buff);
  REQUIRE(rc == TILEDB_OK);

  SECTION("delete_fragments") {
    // Serialize fragment timestamps and deserialize delete request
    tiledb::sm::serialization::serialize_delete_fragments_timestamps_request(
        array->config(),
        start_timestamp,
        end_timestamp,
        tiledb::sm::SerializationType::CAPNP,
        buff->buffer());
    rc = tiledb_handle_array_delete_fragments_timestamps_request(
        ctx_,
        array,
        (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
        buff);
    REQUIRE(rc == TILEDB_OK);
    CHECK(tiledb::test::num_commits(array_name) == 0);
    CHECK(tiledb::test::num_fragments(array_name) == 0);
  }

  SECTION("delete_fragments_list") {
    // Get the fragment info object
    tiledb_fragment_info_t* fragment_info = nullptr;
    rc = tiledb_fragment_info_alloc(ctx_, array_name.c_str(), &fragment_info);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_fragment_info_load(ctx_, fragment_info);
    REQUIRE(rc == TILEDB_OK);

    // Get the fragment URIs
    const char* uri1;
    rc = tiledb_fragment_info_get_fragment_uri(ctx_, fragment_info, 0, &uri1);
    REQUIRE(rc == TILEDB_OK);
    const char* uri2;
    rc = tiledb_fragment_info_get_fragment_uri(ctx_, fragment_info, 1, &uri2);
    REQUIRE(rc == TILEDB_OK);

    std::vector<URI> fragments;
    fragments.emplace_back(URI(uri1));
    fragments.emplace_back(URI(uri2));

    // Serialize fragments list and deserialize delete request
    tiledb::sm::serialization::serialize_delete_fragments_list_request(
        array->config(),
        fragments,
        tiledb::sm::SerializationType::CAPNP,
        buff->buffer());
    rc = tiledb_handle_array_delete_fragments_list_request(
        ctx_,
        array,
        (tiledb_serialization_type_t)tiledb::sm::SerializationType::CAPNP,
        buff);
    REQUIRE(rc == TILEDB_OK);
    CHECK(tiledb::test::num_commits(array_name) == 0);
    CHECK(tiledb::test::num_fragments(array_name) == 0);
    tiledb_fragment_info_free(&fragment_info);
  }

  // Clean up
  tiledb_array_free(&array);
  tiledb_buffer_free(&buff);
  remove_temp_dir(local_fs.file_prefix() + local_fs.temp_dir());
#endif
}
