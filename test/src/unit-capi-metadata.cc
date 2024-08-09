/**
 * @file unit-capi-metadata.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
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
 * Tests the C API for array metadata.
 */

#include "test/support/src/helpers.h"
#include "test/support/src/vfs_helpers.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/global_state/unit_test_config.h"
#include "tiledb/sm/misc/tdb_time.h"

#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif

#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <iostream>
#include <thread>

using namespace tiledb::sm;
using namespace tiledb::test;

/* ********************************* */
/*         STRUCT DEFINITION         */
/* ********************************* */

struct CMetadataFx {
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;
  const std::vector<std::unique_ptr<SupportedFs>> fs_vec_;
  std::string temp_dir_;
  std::string array_name_;
  const char* ARRAY_NAME = "test_metadata";
  tiledb_array_t* array_ = nullptr;
  const char* key_ = "0123456789abcdeF0123456789abcdeF";
  const tiledb_encryption_type_t enc_type_ = TILEDB_AES_256_GCM;

  void create_default_array_1d();
  void create_default_array_1d_with_key();

  // Used to get the number of directories or files of another directory
  struct get_num_struct {
    int num;
  };

  static int get_meta_num(const char* path, void* data);

  CMetadataFx();
  ~CMetadataFx();
};

CMetadataFx::CMetadataFx()
    : fs_vec_(vfs_test_get_fs_vec()) {
  // Initialize vfs test
  REQUIRE(vfs_test_init(fs_vec_, &ctx_, &vfs_).ok());

// Create temporary directory based on the supported filesystem
#ifdef _WIN32
  SupportedFsLocal windows_fs;
  temp_dir_ = windows_fs.file_prefix() + windows_fs.temp_dir();
#else
  SupportedFsLocal posix_fs;
  temp_dir_ = posix_fs.file_prefix() + posix_fs.temp_dir();
#endif

  create_dir(temp_dir_, ctx_, vfs_);

  array_name_ = temp_dir_ + ARRAY_NAME;
  int rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array_);
  CHECK(rc == TILEDB_OK);
}

CMetadataFx::~CMetadataFx() {
  tiledb_array_free(&array_);
  remove_dir(temp_dir_, ctx_, vfs_);
  tiledb_ctx_free(&ctx_);
  tiledb_vfs_free(&vfs_);
}

int CMetadataFx::get_meta_num(const char* path, void* data) {
  (void)path;
  auto data_struct = (CMetadataFx::get_num_struct*)data;
  ++data_struct->num;

  return 1;
}

void CMetadataFx::create_default_array_1d() {
  uint64_t domain[] = {1, 10};
  uint64_t tile_extent = 5;
  create_array(
      ctx_,
      array_name_,
      TILEDB_DENSE,
      {"d"},
      {TILEDB_UINT64},
      {domain},
      {&tile_extent},
      {"a", "b", "c"},
      {TILEDB_INT32, TILEDB_CHAR, TILEDB_FLOAT32},
      {1, TILEDB_VAR_NUM, 2},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1),
       tiledb::test::Compressor(TILEDB_FILTER_ZSTD, -1),
       tiledb::test::Compressor(TILEDB_FILTER_LZ4, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      2);
}

void CMetadataFx::create_default_array_1d_with_key() {
  uint64_t domain[] = {1, 10};
  uint64_t tile_extent = 5;
  create_array(
      ctx_,
      array_name_,
      enc_type_,
      key_,
      TILEDB_DENSE,
      {"d"},
      {TILEDB_UINT64},
      {domain},
      {&tile_extent},
      {"a", "b", "c"},
      {TILEDB_INT32, TILEDB_CHAR, TILEDB_FLOAT32},
      {1, TILEDB_VAR_NUM, 2},
      {tiledb::test::Compressor(TILEDB_FILTER_NONE, -1),
       tiledb::test::Compressor(TILEDB_FILTER_ZSTD, -1),
       tiledb::test::Compressor(TILEDB_FILTER_LZ4, -1)},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      2);
}

/* ********************************* */
/*                TESTS              */
/* ********************************* */

TEST_CASE_METHOD(
    CMetadataFx, "C API: Metadata, basic errors", "[capi][metadata][errors]") {
  // Create default array
  create_default_array_1d();

  // Create array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);

  // Put metadata on an array that is not opened
  int v = 5;
  rc = tiledb_array_put_metadata(ctx_, array, "key", TILEDB_INT32, 1, &v);
  CHECK(rc == TILEDB_ERR);

  // Write metadata on an array opened in READ mode
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_put_metadata(ctx_, array, "key", TILEDB_INT32, 1, &v);
  CHECK(rc == TILEDB_ERR);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);

  // Reopen array in WRITE mode
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  // Write null key
  rc = tiledb_array_put_metadata(ctx_, array, NULL, TILEDB_INT32, 1, &v);
  CHECK(rc == TILEDB_ERR);

  // Write value type ANY
  rc = tiledb_array_put_metadata(ctx_, array, "key", TILEDB_ANY, 1, &v);
  CHECK(rc == TILEDB_ERR);

  // Write a correct item
  rc = tiledb_array_put_metadata(ctx_, array, "key", TILEDB_INT32, 1, &v);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);

  // Open with key
  tiledb_config_t* config;
  tiledb_error_t* error = nullptr;
  rc = tiledb_config_alloc(&config, &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  std::string encryption_type_string =
      encryption_type_str((tiledb::sm::EncryptionType)enc_type_);
  rc = tiledb_config_set(
      config, "sm.encryption_type", encryption_type_string.c_str(), &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_config_set(config, "sm.encryption_key", key_, &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_array_set_config(ctx_, array, config);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_ERR);

  // Clean up
  tiledb_array_free(&array);
  tiledb_config_free(&config);
}

TEST_CASE_METHOD(
    CMetadataFx, "C API: Metadata, write/read", "[capi][metadata][read]") {
  // Create default array
  create_default_array_1d();

  // Create and open array in write mode
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  // Write items
  int32_t v = 5;
  rc = tiledb_array_put_metadata(ctx_, array, "aaa", TILEDB_INT32, 1, &v);
  CHECK(rc == TILEDB_OK);
  float f[] = {1.1f, 1.2f};
  rc = tiledb_array_put_metadata(ctx_, array, "bb", TILEDB_FLOAT32, 2, f);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);

  // Open the array in read mode
  rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  // Read
  const void* v_r;
  tiledb_datatype_t v_type;
  uint32_t v_num;
  rc = tiledb_array_get_metadata(ctx_, array, "aaa", &v_type, &v_num, &v_r);
  CHECK(rc == TILEDB_OK);
  CHECK(v_type == TILEDB_INT32);
  CHECK(v_num == 1);
  CHECK(*((const int32_t*)v_r) == 5);

  rc = tiledb_array_get_metadata(ctx_, array, "bb", &v_type, &v_num, &v_r);
  CHECK(rc == TILEDB_OK);
  CHECK(v_type == TILEDB_FLOAT32);
  CHECK(v_num == 2);
  CHECK(((const float*)v_r)[0] == 1.1f);
  CHECK(((const float*)v_r)[1] == 1.2f);

  rc = tiledb_array_get_metadata(ctx_, array, "foo", &v_type, &v_num, &v_r);
  CHECK(rc == TILEDB_OK);
  CHECK(v_r == nullptr);

  uint64_t num = 0;
  rc = tiledb_array_get_metadata_num(ctx_, array, &num);
  CHECK(rc == TILEDB_OK);
  CHECK(num == 2);

  const char* key;
  uint32_t key_len;
  rc = tiledb_array_get_metadata_from_index(
      ctx_, array, 10, &key, &key_len, &v_type, &v_num, &v_r);
  CHECK(rc == TILEDB_ERR);

  rc = tiledb_array_get_metadata_from_index(
      ctx_, array, 1, &key, &key_len, &v_type, &v_num, &v_r);
  CHECK(rc == TILEDB_OK);
  CHECK(v_type == TILEDB_FLOAT32);
  CHECK(v_num == 2);
  CHECK(((const float*)v_r)[0] == 1.1f);
  CHECK(((const float*)v_r)[1] == 1.2f);
  CHECK(key_len == strlen("bb"));
  CHECK(!strncmp(key, "bb", strlen("bb")));

  // Check has_key
  int32_t has_key = 0;
  rc = tiledb_array_has_metadata_key(ctx_, array, "bb", &v_type, &has_key);
  CHECK(rc == TILEDB_OK);
  CHECK(v_type == TILEDB_FLOAT32);
  CHECK(has_key == 1);

  // Check not has_key
  v_type = (tiledb_datatype_t)std::numeric_limits<int32_t>::max();
  rc = tiledb_array_has_metadata_key(
      ctx_, array, "non-existent-key", &v_type, &has_key);
  CHECK(rc == TILEDB_OK);
  // The API does not touch v_type when no key is found.
  CHECK((int32_t)v_type == std::numeric_limits<int32_t>::max());
  CHECK(has_key == 0);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);
}

TEST_CASE_METHOD(
    CMetadataFx,
    "C API: Array Metadata, sub-millisecond writes",
    "[capi][array][metadata][sub-millisecond]") {
  int32_t one = 1;
  int32_t two = 2;
  const void* v_r = nullptr;
  tiledb_datatype_t v_type;
  uint32_t v_num;

  // Run the test body 100 times
  for (int i = 0; i < 100; i++) {
    // Create and open array in write mode
    create_default_array_1d();
    tiledb_array_t* array;
    int rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
    REQUIRE(rc == TILEDB_OK);

    // Write to disk twice
    rc = tiledb_array_put_metadata(ctx_, array, "aaa", TILEDB_INT32, 1, &one);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_array_close(ctx_, array);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_array_put_metadata(ctx_, array, "aaa", TILEDB_INT32, 1, &two);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_array_close(ctx_, array);
    REQUIRE(rc == TILEDB_OK);
    tiledb_array_free(&array);

    // Open the array in read mode
    rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_array_open(ctx_, array, TILEDB_READ);
    REQUIRE(rc == TILEDB_OK);

    // Read
    rc = tiledb_array_get_metadata(ctx_, array, "aaa", &v_type, &v_num, &v_r);
    CHECK(rc == TILEDB_OK);
    CHECK(v_type == TILEDB_INT32);
    CHECK(v_num == 1);
    CHECK(*((const int32_t*)v_r) == 2);

    // Cleanup
    rc = tiledb_array_close(ctx_, array);
    REQUIRE(rc == TILEDB_OK);
    tiledb_array_free(&array);
    remove_dir(array_name_, ctx_, vfs_);
  }
}

TEST_CASE_METHOD(
    CMetadataFx,
    "C API: Group Metadata, sub-millisecond writes",
    "[capi][group][metadata][sub-millisecond]") {
  std::string group_name = "test_group_meta_sub_millisecond_writes";
  int32_t one = 1;
  int32_t two = 2;
  const void* v_r = nullptr;
  tiledb_datatype_t v_type;
  uint32_t v_num;

  // Run the test body 100 times
  for (int i = 0; i < 100; i++) {
    // Create and open group in write mode
    create_dir(group_name, ctx_, vfs_);
    REQUIRE(tiledb_group_create(ctx_, group_name.c_str()) == TILEDB_OK);
    tiledb_group_t* group;
    REQUIRE(tiledb_group_alloc(ctx_, group_name.c_str(), &group) == TILEDB_OK);
    REQUIRE(tiledb_group_open(ctx_, group, TILEDB_WRITE) == TILEDB_OK);

    // Write to disk twice
    int rc =
        tiledb_group_put_metadata(ctx_, group, "aaa", TILEDB_INT32, 1, &one);
    REQUIRE(tiledb_group_close(ctx_, group) == TILEDB_OK);
    REQUIRE(tiledb_group_open(ctx_, group, TILEDB_WRITE) == TILEDB_OK);
    rc = tiledb_group_put_metadata(ctx_, group, "aaa", TILEDB_INT32, 1, &two);
    CHECK(rc == TILEDB_OK);
    REQUIRE(tiledb_group_close(ctx_, group) == TILEDB_OK);
    tiledb_group_free(&group);

    // Open the group in read mode
    REQUIRE(tiledb_group_alloc(ctx_, group_name.c_str(), &group) == TILEDB_OK);
    REQUIRE(tiledb_group_open(ctx_, group, TILEDB_READ) == TILEDB_OK);

    // Read
    rc = tiledb_group_get_metadata(ctx_, group, "aaa", &v_type, &v_num, &v_r);
    CHECK(rc == TILEDB_OK);
    CHECK(v_type == TILEDB_INT32);
    CHECK(v_num == 1);
    CHECK(*((const int32_t*)v_r) == 2);

    // Cleanup
    REQUIRE(tiledb_group_close(ctx_, group) == TILEDB_OK);
    tiledb_group_free(&group);
    remove_dir(group_name, ctx_, vfs_);
  }
}

TEST_CASE_METHOD(
    CMetadataFx, "C API: Metadata, UTF-8", "[capi][metadata][utf-8]") {
  // Create default array
  create_default_array_1d();

  // Create and open array in write mode
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  // Write UTF-8 (≥ holds 3 bytes)
  int32_t v = 5;
  rc = tiledb_array_put_metadata(ctx_, array, "≥", TILEDB_INT32, 1, &v);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);

  // Open the array in read mode
  rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  // Read
  const void* v_r;
  tiledb_datatype_t v_type;
  uint32_t v_num;
  rc = tiledb_array_get_metadata(ctx_, array, "≥", &v_type, &v_num, &v_r);
  CHECK(rc == TILEDB_OK);
  CHECK(v_type == TILEDB_INT32);
  CHECK(v_num == 1);
  CHECK(*((const int32_t*)v_r) == 5);

  const char* key;
  uint32_t key_len;
  rc = tiledb_array_get_metadata_from_index(
      ctx_, array, 0, &key, &key_len, &v_type, &v_num, &v_r);
  CHECK(rc == TILEDB_OK);
  CHECK(v_type == TILEDB_INT32);
  CHECK(v_num == 1);
  CHECK(*((const int32_t*)v_r) == 5);
  CHECK(key_len == strlen("≥"));
  CHECK(!strncmp(key, "≥", strlen("≥")));

  // Close array
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);
}

TEST_CASE_METHOD(
    CMetadataFx, "C API: Metadata, delete", "[capi][metadata][delete]") {
  // Create default array
  create_default_array_1d();

  // Create and open array in write mode
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  // Write items
  int32_t v = 5;
  rc = tiledb_array_put_metadata(ctx_, array, "aaa", TILEDB_INT32, 1, &v);
  CHECK(rc == TILEDB_OK);
  float f[] = {1.1f, 1.2f};
  rc = tiledb_array_put_metadata(ctx_, array, "bb", TILEDB_FLOAT32, 2, f);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);

  // Prevent array metadata filename/timestamp conflicts
  std::this_thread::sleep_for(std::chrono::milliseconds(1));

  // Delete an item that exists and one that does not exist
  rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_delete_metadata(ctx_, array, "aaa");
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_delete_metadata(ctx_, array, "foo");
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);

  // Open the array in read mode
  rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  // Read
  const void* v_r;
  tiledb_datatype_t v_type;
  uint32_t v_num;
  rc = tiledb_array_get_metadata(ctx_, array, "aaa", &v_type, &v_num, &v_r);
  CHECK(rc == TILEDB_OK);
  CHECK(v_r == nullptr);

  rc = tiledb_array_get_metadata(ctx_, array, "bb", &v_type, &v_num, &v_r);
  CHECK(rc == TILEDB_OK);
  CHECK(v_type == TILEDB_FLOAT32);
  CHECK(v_num == 2);
  CHECK(((const float*)v_r)[0] == 1.1f);
  CHECK(((const float*)v_r)[1] == 1.2f);

  rc = tiledb_array_get_metadata(ctx_, array, "foo", &v_type, &v_num, &v_r);
  CHECK(rc == TILEDB_OK);
  CHECK(v_r == nullptr);

  uint64_t num = 0;
  rc = tiledb_array_get_metadata_num(ctx_, array, &num);
  CHECK(rc == TILEDB_OK);
  CHECK(num == 1);

  const char* key;
  uint32_t key_len;
  rc = tiledb_array_get_metadata_from_index(
      ctx_, array, 0, &key, &key_len, &v_type, &v_num, &v_r);
  CHECK(rc == TILEDB_OK);
  CHECK(v_type == TILEDB_FLOAT32);
  CHECK(v_num == 2);
  CHECK(((const float*)v_r)[0] == 1.1f);
  CHECK(((const float*)v_r)[1] == 1.2f);
  CHECK(key_len == strlen("bb"));
  CHECK(!strncmp(key, "bb", strlen("bb")));

  // Close array
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);
}

TEST_CASE_METHOD(
    CMetadataFx,
    "C API: Metadata, multiple metadata and consolidate",
    "[capi][metadata][multiple][consolidation]") {
  // Create default array
  create_default_array_1d();

  // Create and open array in write mode
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_set_open_timestamp_end(ctx_, array, 1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  // Write items
  int32_t v = 5;
  rc = tiledb_array_put_metadata(ctx_, array, "aaa", TILEDB_INT32, 1, &v);
  CHECK(rc == TILEDB_OK);
  float f[] = {1.1f, 1.2f};
  rc = tiledb_array_put_metadata(ctx_, array, "bb", TILEDB_FLOAT32, 2, f);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);

  // Update
  rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_set_open_timestamp_end(ctx_, array, 2);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_delete_metadata(ctx_, array, "aaa");
  CHECK(rc == TILEDB_OK);
  v = 10;
  rc = tiledb_array_put_metadata(ctx_, array, "cccc", TILEDB_INT32, 1, &v);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);

  // Open the array in read mode
  rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  // Read
  const void* v_r;
  tiledb_datatype_t v_type;
  uint32_t v_num;
  rc = tiledb_array_get_metadata(ctx_, array, "aaa", &v_type, &v_num, &v_r);
  CHECK(rc == TILEDB_OK);
  CHECK(v_r == nullptr);

  rc = tiledb_array_get_metadata(ctx_, array, "bb", &v_type, &v_num, &v_r);
  CHECK(rc == TILEDB_OK);
  CHECK(v_type == TILEDB_FLOAT32);
  CHECK(v_num == 2);
  CHECK(((const float*)v_r)[0] == 1.1f);
  CHECK(((const float*)v_r)[1] == 1.2f);

  rc = tiledb_array_get_metadata(ctx_, array, "cccc", &v_type, &v_num, &v_r);
  CHECK(rc == TILEDB_OK);
  CHECK(v_type == TILEDB_INT32);
  CHECK(v_num == 1);
  CHECK(*((const int32_t*)v_r) == 10);

  uint64_t num = 0;
  rc = tiledb_array_get_metadata_num(ctx_, array, &num);
  CHECK(rc == TILEDB_OK);
  CHECK(num == 2);

  const char* key;
  uint32_t key_len;
  rc = tiledb_array_get_metadata_from_index(
      ctx_, array, 0, &key, &key_len, &v_type, &v_num, &v_r);
  CHECK(rc == TILEDB_OK);
  CHECK(v_type == TILEDB_FLOAT32);
  CHECK(v_num == 2);
  CHECK(((const float*)v_r)[0] == 1.1f);
  CHECK(((const float*)v_r)[1] == 1.2f);
  CHECK(key_len == strlen("bb"));
  CHECK(!strncmp(key, "bb", strlen("bb")));

  // Close array
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);

  // Consolidate
  SECTION("tiledb_array_consolidate") {
    // Configuration for consolidating array metadata
    tiledb_config_t* config = nullptr;
    tiledb_error_t* error = nullptr;
    REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
    REQUIRE(error == nullptr);
    int rc = tiledb_config_set(
        config, "sm.consolidation.mode", "array_meta", &error);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(error == nullptr);
    rc = tiledb_array_consolidate(ctx_, array_name_.c_str(), config);
    CHECK(rc == TILEDB_OK);
    tiledb_config_free(&config);
  }

  // Check number of metadata files
  get_num_struct data = {0};
  auto meta_dir =
      array_name_ + "/" + tiledb::sm::constants::array_metadata_dir_name;
  rc = tiledb_vfs_ls(ctx_, vfs_, meta_dir.c_str(), &get_meta_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.num == 4);

  // Read at timestamp 1
  rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_set_open_timestamp_end(ctx_, array, 1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_get_metadata(ctx_, array, "aaa", &v_type, &v_num, &v_r);
  CHECK(rc == TILEDB_OK);
  CHECK(v_type == TILEDB_INT32);
  CHECK(*(const int32_t*)v_r == 5);
  CHECK(v_num == 1);
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);

  // Vacuum
  tiledb_config_t* config_v = nullptr;
  tiledb_error_t* error_v = nullptr;
  REQUIRE(tiledb_config_alloc(&config_v, &error_v) == TILEDB_OK);
  REQUIRE(error_v == nullptr);
  rc = tiledb_config_set(config_v, "sm.vacuum.mode", "array_meta", &error_v);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error_v == nullptr);
  rc = tiledb_array_vacuum(ctx_, array_name_.c_str(), config_v);
  CHECK(rc == TILEDB_OK);
  tiledb_config_free(&config_v);

  // Check number of metadata files
  data = {0};
  rc = tiledb_vfs_ls(ctx_, vfs_, meta_dir.c_str(), &get_meta_num, &data);
  CHECK(rc == TILEDB_OK);
  CHECK(data.num == 1);

  // Open the array in read mode
  rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  num = 0;
  rc = tiledb_array_get_metadata_num(ctx_, array, &num);
  CHECK(rc == TILEDB_OK);
  CHECK(num == 2);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);

  // Write once more
  rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  // Write items
  v = 50;
  rc = tiledb_array_put_metadata(ctx_, array, "d", TILEDB_INT32, 1, &v);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);

  // Consolidate again
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_config_set(config, "sm.consolidation.mode", "array_meta", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_array_consolidate(ctx_, array_name_.c_str(), config);
  CHECK(rc == TILEDB_OK);
  tiledb_config_free(&config);
  tiledb_array_free(&array);

  // Open the array in read mode
  rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  num = 0;
  rc = tiledb_array_get_metadata_num(ctx_, array, &num);
  CHECK(rc == TILEDB_OK);
  CHECK(num == 3);

  rc = tiledb_array_get_metadata(ctx_, array, "cccc", &v_type, &v_num, &v_r);
  CHECK(rc == TILEDB_OK);
  CHECK(v_type == TILEDB_INT32);
  CHECK(v_num == 1);
  CHECK(*((const int32_t*)v_r) == 10);

  rc = tiledb_array_get_metadata(ctx_, array, "d", &v_type, &v_num, &v_r);
  CHECK(rc == TILEDB_OK);
  CHECK(v_type == TILEDB_INT32);
  CHECK(v_num == 1);
  CHECK(*((const int32_t*)v_r) == 50);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);
}

TEST_CASE_METHOD(
    CMetadataFx, "C API: Metadata, open at", "[capi][metadata][open-at]") {
  // Create default array
  create_default_array_1d();

  // Create and open array in write mode
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_set_open_timestamp_end(ctx_, array, 1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  // Write items
  int32_t v = 5;
  rc = tiledb_array_put_metadata(ctx_, array, "aaa", TILEDB_INT32, 1, &v);
  CHECK(rc == TILEDB_OK);
  float f[] = {1.1f, 1.2f};
  rc = tiledb_array_put_metadata(ctx_, array, "bb", TILEDB_FLOAT32, 2, f);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);

  // Update
  rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_set_open_timestamp_end(ctx_, array, 2);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_delete_metadata(ctx_, array, "aaa");
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);

  // Open the array in read mode at a timestamp
  rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_set_open_timestamp_end(ctx_, array, 1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  // Read
  const void* v_r;
  tiledb_datatype_t v_type;
  uint32_t v_num;
  rc = tiledb_array_get_metadata(ctx_, array, "aaa", &v_type, &v_num, &v_r);
  CHECK(rc == TILEDB_OK);
  CHECK(v_type == TILEDB_INT32);
  CHECK(v_num == 1);
  CHECK(*((const int32_t*)v_r) == 5);

  uint64_t num = 0;
  rc = tiledb_array_get_metadata_num(ctx_, array, &num);
  CHECK(rc == TILEDB_OK);
  CHECK(num == 2);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);
}

TEST_CASE_METHOD(
    CMetadataFx, "C API: Metadata, reopen", "[capi][metadata][reopen]") {
  // Create default array
  create_default_array_1d();

  // Create and open array in write mode
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_set_open_timestamp_end(ctx_, array, 1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  // Write items
  int32_t v = 5;
  rc = tiledb_array_put_metadata(ctx_, array, "aaa", TILEDB_INT32, 1, &v);
  CHECK(rc == TILEDB_OK);
  float f[] = {1.1f, 1.2f};
  rc = tiledb_array_put_metadata(ctx_, array, "bb", TILEDB_FLOAT32, 2, f);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);

  // Update
  rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_set_open_timestamp_end(ctx_, array, 2);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_delete_metadata(ctx_, array, "aaa");
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);

  // Open the array in read mode at a timestamp
  rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_set_open_timestamp_end(ctx_, array, 1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  // Read
  const void* v_r;
  tiledb_datatype_t v_type;
  uint32_t v_num;
  rc = tiledb_array_get_metadata(ctx_, array, "aaa", &v_type, &v_num, &v_r);
  CHECK(rc == TILEDB_OK);
  CHECK(v_type == TILEDB_INT32);
  CHECK(v_num == 1);
  CHECK(*((const int32_t*)v_r) == 5);

  uint64_t num = 0;
  rc = tiledb_array_get_metadata_num(ctx_, array, &num);
  CHECK(rc == TILEDB_OK);
  CHECK(num == 2);

  // Reopen
  rc = tiledb_array_set_open_timestamp_end(
      ctx_, array, tiledb::sm::utils::time::timestamp_now_ms());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_reopen(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Read
  rc = tiledb_array_get_metadata(ctx_, array, "aaa", &v_type, &v_num, &v_r);
  CHECK(rc == TILEDB_OK);
  CHECK(v_r == nullptr);

  num = 0;
  rc = tiledb_array_get_metadata_num(ctx_, array, &num);
  CHECK(rc == TILEDB_OK);
  CHECK(num == 1);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);
}

TEST_CASE_METHOD(
    CMetadataFx,
    "C API: Metadata, timestamp_end",
    "[capi][metadata][timestamp-end]") {
  // Create default array
  create_default_array_1d();

  // Create and open array in write mode
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_set_open_timestamp_end(ctx_, array, 1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  // Write items
  int32_t v1 = 4;
  rc = tiledb_array_put_metadata(ctx_, array, "aaa", TILEDB_INT32, 1, &v1);
  CHECK(rc == TILEDB_OK);
  float f1[] = {1.0f, 1.2f};
  rc = tiledb_array_put_metadata(ctx_, array, "bb", TILEDB_FLOAT32, 2, f1);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);

  // Update
  rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_set_open_timestamp_end(ctx_, array, 2);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_delete_metadata(ctx_, array, "aaa");
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);

  // Create and open array in write mode
  rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_set_open_timestamp_end(ctx_, array, 3);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  // Write items
  int32_t v2 = 5;
  rc = tiledb_array_put_metadata(ctx_, array, "aaa", TILEDB_INT32, 1, &v2);
  CHECK(rc == TILEDB_OK);
  float f2[] = {1.1f, 1.2f};
  rc = tiledb_array_put_metadata(ctx_, array, "bb", TILEDB_FLOAT32, 2, f2);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);

  // Update
  rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_set_open_timestamp_end(ctx_, array, 4);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_delete_metadata(ctx_, array, "aaa");
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);

  // Create and open array in write mode
  rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_set_open_timestamp_end(ctx_, array, 5);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  // Write items
  int32_t v3 = 6;
  rc = tiledb_array_put_metadata(ctx_, array, "aaa", TILEDB_INT32, 1, &v3);
  CHECK(rc == TILEDB_OK);
  float f3[] = {1.2f, 1.3f};
  rc = tiledb_array_put_metadata(ctx_, array, "bb", TILEDB_FLOAT32, 2, f3);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);

  // Update
  rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_set_open_timestamp_end(ctx_, array, 6);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_delete_metadata(ctx_, array, "aaa");
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);

  // Open the array in read mode between timestamp1 and timestamp2
  rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  rc = tiledb_array_set_open_timestamp_start(ctx_, array, 2);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_set_open_timestamp_end(ctx_, array, 3);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  // Read - ensure that the data is only that written between
  // timestamp1 and timestamp2
  const void* v_r;
  tiledb_datatype_t v_type;
  uint32_t v_num;
  rc = tiledb_array_get_metadata(ctx_, array, "aaa", &v_type, &v_num, &v_r);
  CHECK(rc == TILEDB_OK);
  CHECK(v_type == TILEDB_INT32);
  CHECK(v_num == 1);
  CHECK(*((const int32_t*)v_r) == 5);

  uint64_t num = 0;
  rc = tiledb_array_get_metadata_num(ctx_, array, &num);
  CHECK(rc == TILEDB_OK);
  CHECK(num == 2);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);
}

TEST_CASE_METHOD(
    CMetadataFx,
    "C API: Metadata, encryption",
    "[capi][metadata][encryption]") {
  // Create default array
  create_default_array_1d_with_key();

  // Create and open array in write mode
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_config_t* config;
  tiledb_error_t* error = nullptr;
  rc = tiledb_config_alloc(&config, &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  std::string encryption_type_string =
      encryption_type_str((tiledb::sm::EncryptionType)enc_type_);
  rc = tiledb_config_set(
      config, "sm.encryption_type", encryption_type_string.c_str(), &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_config_set(config, "sm.encryption_key", key_, &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_array_set_config(ctx_, array, config);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  // Write items
  int32_t v = 5;
  rc = tiledb_array_put_metadata(ctx_, array, "aaa", TILEDB_INT32, 1, &v);
  CHECK(rc == TILEDB_OK);
  float f[] = {1.1f, 1.2f};
  rc = tiledb_array_put_metadata(ctx_, array, "bb", TILEDB_FLOAT32, 2, f);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);

  // Prevent array metadata filename/timestamp conflicts
  std::this_thread::sleep_for(std::chrono::milliseconds(1));

  // Update
  rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_set_config(ctx_, array, config);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_delete_metadata(ctx_, array, "aaa");
  CHECK(rc == TILEDB_OK);
  v = 10;
  rc = tiledb_array_put_metadata(ctx_, array, "cccc", TILEDB_INT32, 1, &v);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);

  // Open the array in read mode
  rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_set_config(ctx_, array, config);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  // Read
  const void* v_r;
  tiledb_datatype_t v_type;
  uint32_t v_num;
  rc = tiledb_array_get_metadata(ctx_, array, "aaa", &v_type, &v_num, &v_r);
  CHECK(rc == TILEDB_OK);
  CHECK(v_r == nullptr);

  rc = tiledb_array_get_metadata(ctx_, array, "bb", &v_type, &v_num, &v_r);
  CHECK(rc == TILEDB_OK);
  CHECK(v_type == TILEDB_FLOAT32);
  CHECK(v_num == 2);
  CHECK(((const float*)v_r)[0] == 1.1f);
  CHECK(((const float*)v_r)[1] == 1.2f);

  rc = tiledb_array_get_metadata(ctx_, array, "cccc", &v_type, &v_num, &v_r);
  CHECK(rc == TILEDB_OK);
  CHECK(v_type == TILEDB_INT32);
  CHECK(v_num == 1);
  CHECK(*((const int32_t*)v_r) == 10);

  uint64_t num = 0;
  rc = tiledb_array_get_metadata_num(ctx_, array, &num);
  CHECK(rc == TILEDB_OK);
  CHECK(num == 2);

  const char* key;
  uint32_t key_len;
  rc = tiledb_array_get_metadata_from_index(
      ctx_, array, 0, &key, &key_len, &v_type, &v_num, &v_r);
  CHECK(rc == TILEDB_OK);
  CHECK(v_type == TILEDB_FLOAT32);
  CHECK(v_num == 2);
  CHECK(((const float*)v_r)[0] == 1.1f);
  CHECK(((const float*)v_r)[1] == 1.2f);
  CHECK(key_len == strlen("bb"));
  CHECK(!strncmp(key, "bb", strlen("bb")));

  // Close array
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);

  // Consolidate without key - error
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_config_set(config, "sm.consolidation.mode", "array_meta", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_array_consolidate(ctx_, array_name_.c_str(), config);
  CHECK(rc == TILEDB_ERR);
  tiledb_config_free(&config);

  // Consolidate with key - ok
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_config_set(config, "sm.consolidation.mode", "array_meta", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  encryption_type_string =
      encryption_type_str((tiledb::sm::EncryptionType)enc_type_);
  rc = tiledb_config_set(
      config, "sm.encryption_type", encryption_type_string.c_str(), &error);
  REQUIRE(error == nullptr);
  rc = tiledb_config_set(config, "sm.encryption_key", key_, &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_array_consolidate(ctx_, array_name_.c_str(), config);
  CHECK(rc == TILEDB_OK);
  tiledb_config_free(&config);
  tiledb_array_free(&array);

  // Open the array in read mode
  rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);
  encryption_type_string =
      encryption_type_str((tiledb::sm::EncryptionType)enc_type_);
  rc = tiledb_config_set(
      config, "sm.encryption_type", encryption_type_string.c_str(), &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_config_set(config, "sm.encryption_key", key_, &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_array_set_config(ctx_, array, config);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  num = 0;
  rc = tiledb_array_get_metadata_num(ctx_, array, &num);
  CHECK(rc == TILEDB_OK);
  CHECK(num == 2);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);

  // Write once more
  rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_set_config(ctx_, array, config);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  // Write items
  v = 50;
  rc = tiledb_array_put_metadata(ctx_, array, "d", TILEDB_INT32, 1, &v);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);

  // Consolidate again
  rc = tiledb_config_set(config, "sm.consolidation.mode", "array_meta", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  encryption_type_string =
      encryption_type_str((tiledb::sm::EncryptionType)enc_type_);
  rc = tiledb_config_set(
      config, "sm.encryption_type", encryption_type_string.c_str(), &error);
  REQUIRE(error == nullptr);
  rc = tiledb_config_set(config, "sm.encryption_key", key_, &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);

  rc = tiledb_array_consolidate(ctx_, array_name_.c_str(), config);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);

  // Open the array in read mode
  rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_set_config(ctx_, array, config);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  num = 0;
  rc = tiledb_array_get_metadata_num(ctx_, array, &num);
  CHECK(rc == TILEDB_OK);
  CHECK(num == 3);

  rc = tiledb_array_get_metadata(ctx_, array, "cccc", &v_type, &v_num, &v_r);
  CHECK(rc == TILEDB_OK);
  CHECK(v_type == TILEDB_INT32);
  CHECK(v_num == 1);
  CHECK(*((const int32_t*)v_r) == 10);

  rc = tiledb_array_get_metadata(ctx_, array, "d", &v_type, &v_num, &v_r);
  CHECK(rc == TILEDB_OK);
  CHECK(v_type == TILEDB_INT32);
  CHECK(v_num == 1);
  CHECK(*((const int32_t*)v_r) == 50);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);
  tiledb_config_free(&config);
}

TEST_CASE_METHOD(
    CMetadataFx, "C API: Metadata, overwrite", "[capi][metadata][overwrite]") {
  // Create default array
  create_default_array_1d();

  // Open array
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);

  // Write and overwrite
  int32_t v = 5;
  rc = tiledb_array_put_metadata(ctx_, array, "aaa", TILEDB_INT32, 1, &v);
  CHECK(rc == TILEDB_OK);
  int32_t v2 = 10;
  rc = tiledb_array_put_metadata(ctx_, array, "aaa", TILEDB_INT32, 1, &v2);
  CHECK(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Read back
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);
  const void* vback_ptr = NULL;
  tiledb_datatype_t vtype;
  uint32_t vnum = 0;
  rc = tiledb_array_get_metadata(ctx_, array, "aaa", &vtype, &vnum, &vback_ptr);
  CHECK(rc == TILEDB_OK);
  CHECK(vtype == TILEDB_INT32);
  CHECK(vnum == 1);
  CHECK(*((int32_t*)vback_ptr) == 10);
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);
  tiledb_array_free(&array);

  // Prevent array metadata filename/timestamp conflicts
  std::this_thread::sleep_for(std::chrono::milliseconds(1));

  // Overwrite again
  rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  CHECK(rc == TILEDB_OK);
  int32_t v3 = 20;
  rc = tiledb_array_put_metadata(ctx_, array, "aaa", TILEDB_INT32, 1, &v3);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  // Read back
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_array_get_metadata(ctx_, array, "aaa", &vtype, &vnum, &vback_ptr);
  CHECK(rc == TILEDB_OK);
  CHECK(vtype == TILEDB_INT32);
  CHECK(vnum == 1);
  CHECK(*((int32_t*)vback_ptr) == 20);
  rc = tiledb_array_close(ctx_, array);
  CHECK(rc == TILEDB_OK);

  tiledb_array_free(&array);
}

TEST_CASE_METHOD(
    CMetadataFx,
    "C API: Metadata, write/read zero-valued",
    "[capi][metadata][read][zero-valued]") {
  // Create default array
  create_default_array_1d();

  // Create and open array in write mode
  tiledb_array_t* array;
  int rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  // Write items
  rc = tiledb_array_put_metadata(ctx_, array, "aaa", TILEDB_CHAR, 0, nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_put_metadata(ctx_, array, "b", TILEDB_INT32, 1, nullptr);
  REQUIRE(rc == TILEDB_OK);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);

  // Open the array in read mode
  rc = tiledb_array_alloc(ctx_, array_name_.c_str(), &array);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_array_open(ctx_, array, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  // Read
  const void* v_r;
  tiledb_datatype_t v_type;
  uint32_t v_num;
  rc = tiledb_array_get_metadata(ctx_, array, "aaa", &v_type, &v_num, &v_r);
  CHECK(rc == TILEDB_OK);
  CHECK(v_type == TILEDB_CHAR);
  CHECK(v_num == 1);
  CHECK(v_r == nullptr);
  rc = tiledb_array_get_metadata(ctx_, array, "b", &v_type, &v_num, &v_r);
  CHECK(rc == TILEDB_OK);
  CHECK(v_type == TILEDB_INT32);
  CHECK(v_num == 1);
  CHECK(v_r == nullptr);

  // Close array
  rc = tiledb_array_close(ctx_, array);
  REQUIRE(rc == TILEDB_OK);
  tiledb_array_free(&array);
}
