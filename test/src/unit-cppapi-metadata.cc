/**
 * @file unit-cppapi-metadata.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * Tests the C++ API for array metadata.
 */

#include <tiledb/api/c_api/array/array_api_internal.h>
#include <latch>
#include "test/support/src/helpers.h"
#include "test/support/src/vfs_helpers.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/cpp_api/tiledb"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/misc/tdb_time.h"

#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif

#include <test/support/tdb_catch.h>
#include <chrono>
#include <iostream>
#include <thread>

using namespace tiledb;
using namespace tiledb::test;

/* ********************************* */
/*         STRUCT DEFINITION         */
/* ********************************* */

struct CPPMetadataFx {
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

  CPPMetadataFx();
  ~CPPMetadataFx();
};

CPPMetadataFx::CPPMetadataFx()
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

CPPMetadataFx::~CPPMetadataFx() {
  tiledb_array_free(&array_);
  remove_dir(temp_dir_, ctx_, vfs_);
  tiledb_ctx_free(&ctx_);
  tiledb_vfs_free(&vfs_);
}

void CPPMetadataFx::create_default_array_1d() {
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

void CPPMetadataFx::create_default_array_1d_with_key() {
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
    CPPMetadataFx,
    "C++ API: Metadata, basic errors",
    "[cppapi][metadata][errors]") {
  // Create default array
  create_default_array_1d();

  // Put metadata in an array opened for reads - error
  tiledb::Context ctx;
  tiledb::Array array(ctx, std::string(array_name_), TILEDB_READ);
  int v = 5;
  CHECK_THROWS(array.put_metadata("key", TILEDB_INT32, 1, &v));
  array.close();

  // Reopen array in WRITE mode
  array.open(TILEDB_WRITE);

  // Write value type ANY
  CHECK_THROWS(array.put_metadata("key", TILEDB_ANY, 1, &v));

  // Write a correct item
  array.put_metadata("key", TILEDB_INT32, 1, &v);

  // Close array
  array.close();
}

TEST_CASE_METHOD(
    CPPMetadataFx,
    "C++ API: Metadata write / read multithread",
    "[cppapi][metadata][multithread]") {
  create_default_array_1d();
  Context ctx;
  for (int i = 1; i <= 100; i++) {
    // Grow the size of metadata each write.
    std::vector<uint64_t> b(100 * i);
    std::iota(b.begin(), b.end(), 0);
    std::latch get_metadata(2);
    std::thread t1([&]() {
      Array array(ctx, std::string(array_name_), TILEDB_WRITE);
      array.put_metadata("a", TILEDB_UINT64, (uint32_t)b.size(), b.data());
      get_metadata.count_down();
      array.close();
    });

    std::thread t2([&]() {
      get_metadata.arrive_and_wait();
      Array read_array(ctx, std::string(array_name_), TILEDB_READ);
      tiledb_datatype_t type;
      uint32_t value_num = 0;
      const void* data;
      read_array.get_metadata("a", &type, &value_num, &data);
      read_array.close();
    });

    t1.join(), t2.join();
  }
}

TEST_CASE_METHOD(
    CPPMetadataFx,
    "C++ API: Metadata, write/read",
    "[cppapi][metadata][read]") {
  // Create default array
  create_default_array_1d();

  // Open array in write mode
  tiledb::Context ctx;
  tiledb::Array array(ctx, std::string(array_name_), TILEDB_WRITE);

  // Write items
  int32_t v = 5;
  array.put_metadata("aaa", TILEDB_INT32, 1, &v);
  float f[] = {1.1f, 1.2f};
  array.put_metadata("bb", TILEDB_FLOAT32, 2, f);

  // Write null value
  array.put_metadata("zero_val", TILEDB_FLOAT32, 1, NULL);

  // Close array
  array.close();

  // Open the array in read mode
  array.open(TILEDB_READ);

  // Read
  const void* v_r;
  tiledb_datatype_t v_type;
  uint32_t v_num;
  array.get_metadata("aaa", &v_type, &v_num, &v_r);
  CHECK(v_type == TILEDB_INT32);
  CHECK(v_num == 1);
  CHECK(*((const int32_t*)v_r) == 5);

  array.get_metadata("bb", &v_type, &v_num, &v_r);
  CHECK(v_type == TILEDB_FLOAT32);
  CHECK(v_num == 2);
  CHECK(((const float*)v_r)[0] == 1.1f);
  CHECK(((const float*)v_r)[1] == 1.2f);

  array.get_metadata("zero_val", &v_type, &v_num, &v_r);
  CHECK(v_type == TILEDB_FLOAT32);
  CHECK(v_num == 1);
  CHECK(v_r == nullptr);

  array.get_metadata("foo", &v_type, &v_num, &v_r);
  CHECK(v_r == nullptr);

  uint64_t num = array.metadata_num();
  CHECK(num == 3);

  std::string key;
  CHECK_THROWS(array.get_metadata_from_index(10, &key, &v_type, &v_num, &v_r));

  array.get_metadata_from_index(1, &key, &v_type, &v_num, &v_r);
  CHECK(v_type == TILEDB_FLOAT32);
  CHECK(v_num == 2);
  CHECK(((const float*)v_r)[0] == 1.1f);
  CHECK(((const float*)v_r)[1] == 1.2f);
  CHECK(key.size() == strlen("bb"));
  CHECK(!strncmp(key.data(), "bb", strlen("bb")));

  // idx 2 is 'zero_val'
  array.get_metadata_from_index(2, &key, &v_type, &v_num, &v_r);
  CHECK(v_type == TILEDB_FLOAT32);
  CHECK(v_num == 1);
  CHECK(v_r == nullptr);

  // Check has_key
  bool has_key;
  v_type = (tiledb_datatype_t)std::numeric_limits<int32_t>::max();
  has_key = array.has_metadata("bb", &v_type);
  CHECK(has_key == true);
  CHECK(v_type == TILEDB_FLOAT32);

  // Check not has_key
  v_type = (tiledb_datatype_t)std::numeric_limits<int32_t>::max();
  has_key = array.has_metadata("non-existent-key", &v_type);
  CHECK(has_key == false);
  CHECK(v_type == (tiledb_datatype_t)std::numeric_limits<int32_t>::max());

  // Close array
  array.close();
}

TEST_CASE_METHOD(
    CPPMetadataFx, "C++ API: Metadata, UTF-8", "[cppapi][metadata][utf-8]") {
  // Create default array
  create_default_array_1d();

  // Open array in write mode
  tiledb::Context ctx;
  tiledb::Array array(ctx, std::string(array_name_), TILEDB_WRITE);

  // Write UTF-8 (≥ holds 3 bytes)
  int32_t v = 5;
  array.put_metadata("≥", TILEDB_INT32, 1, &v);

  // Close array
  array.close();

  // Open the array in read mode
  array.open(TILEDB_READ);

  // Read
  const void* v_r;
  tiledb_datatype_t v_type;
  uint32_t v_num;
  array.get_metadata("≥", &v_type, &v_num, &v_r);
  CHECK(v_type == TILEDB_INT32);
  CHECK(v_num == 1);
  CHECK(*((const int32_t*)v_r) == 5);

  std::string key;
  array.get_metadata_from_index(0, &key, &v_type, &v_num, &v_r);
  CHECK(v_type == TILEDB_INT32);
  CHECK(v_num == 1);
  CHECK(*((const int32_t*)v_r) == 5);
  CHECK(key.size() == strlen("≥"));
  CHECK(!strncmp(key.data(), "≥", strlen("≥")));

  // Close array
  array.close();
}

TEST_CASE_METHOD(
    CPPMetadataFx, "C++ API: Metadata, delete", "[cppapi][metadata][delete]") {
  // Create default array
  create_default_array_1d();

  // Create and open array in write mode
  tiledb::Context ctx;
  tiledb::Array array(
      ctx,
      std::string(array_name_),
      TILEDB_WRITE,
      tiledb::TemporalPolicy(tiledb::TimeTravel, 1));

  // Write items
  int32_t v = 5;
  array.put_metadata("aaa", TILEDB_INT32, 1, &v);
  float f[] = {1.1f, 1.2f};
  array.put_metadata("bb", TILEDB_FLOAT32, 2, f);

  // Close array
  array.close();

  // Delete an item that exists and one that does not exist
  array.open(TILEDB_WRITE, 2);
  array.delete_metadata("aaa");
  array.delete_metadata("foo");
  array.close();

  // Open the array in read mode
  array.open(TILEDB_READ);

  // Read
  const void* v_r;
  tiledb_datatype_t v_type;
  uint32_t v_num;
  array.get_metadata("aaa", &v_type, &v_num, &v_r);
  CHECK(v_r == nullptr);
  array.get_metadata("bb", &v_type, &v_num, &v_r);
  CHECK(v_type == TILEDB_FLOAT32);
  CHECK(v_num == 2);
  CHECK(((const float*)v_r)[0] == 1.1f);
  CHECK(((const float*)v_r)[1] == 1.2f);

  array.get_metadata("foo", &v_type, &v_num, &v_r);
  CHECK(v_r == nullptr);

  uint64_t num = array.metadata_num();
  CHECK(num == 1);

  std::string key;
  array.get_metadata_from_index(0, &key, &v_type, &v_num, &v_r);
  CHECK(v_type == TILEDB_FLOAT32);
  CHECK(v_num == 2);
  CHECK(((const float*)v_r)[0] == 1.1f);
  CHECK(((const float*)v_r)[1] == 1.2f);
  CHECK(key.size() == strlen("bb"));
  CHECK(!strncmp(key.data(), "bb", strlen("bb")));

  // Close array
  array.close();
}

TEST_CASE_METHOD(
    CPPMetadataFx,
    "C++ API: Metadata, multiple metadata and consolidate",
    "[cppapi][metadata][multiple][consolidation]") {
  Config cfg;
  cfg["sm.consolidation.buffer_size"] = "10000";

  // Create default array
  create_default_array_1d();

  // Create and open array in write mode
  tiledb::Context ctx(cfg);
  tiledb::Array array(ctx, array_name_, TILEDB_WRITE);

  // Write items
  int32_t v = 5;
  array.put_metadata("aaa", TILEDB_INT32, 1, &v);
  float f[] = {1.1f, 1.2f};
  array.put_metadata("bb", TILEDB_FLOAT32, 2, f);

  // Close array
  array.close();

  // Prevent array metadata filename/timestamp conflicts
  std::this_thread::sleep_for(std::chrono::milliseconds(1));

  // Update
  array.open(TILEDB_WRITE);
  array.delete_metadata("aaa");
  v = 10;
  array.put_metadata("cccc", TILEDB_INT32, 1, &v);
  array.close();

  // Open the array in read mode
  array.open(TILEDB_READ);

  // Read
  const void* v_r;
  tiledb_datatype_t v_type;
  uint32_t v_num;
  array.get_metadata("aaa", &v_type, &v_num, &v_r);
  CHECK(v_r == nullptr);

  array.get_metadata("bb", &v_type, &v_num, &v_r);
  CHECK(v_type == TILEDB_FLOAT32);
  CHECK(v_num == 2);
  CHECK(((const float*)v_r)[0] == 1.1f);
  CHECK(((const float*)v_r)[1] == 1.2f);

  array.get_metadata("cccc", &v_type, &v_num, &v_r);
  CHECK(v_type == TILEDB_INT32);
  CHECK(v_num == 1);
  CHECK(*((const int32_t*)v_r) == 10);

  uint64_t num = array.metadata_num();
  CHECK(num == 2);

  std::string key;
  array.get_metadata_from_index(0, &key, &v_type, &v_num, &v_r);
  CHECK(v_type == TILEDB_FLOAT32);
  CHECK(v_num == 2);
  CHECK(((const float*)v_r)[0] == 1.1f);
  CHECK(((const float*)v_r)[1] == 1.2f);
  CHECK(key.size() == strlen("bb"));
  CHECK(!strncmp(key.data(), "bb", strlen("bb")));

  // Close array
  array.close();

  // Consolidate
  tiledb::Config consolidation_cfg;
  consolidation_cfg["sm.consolidation.mode"] = "array_meta";
  tiledb::Array::consolidate(ctx, array_name_, &consolidation_cfg);

  // Open the array in read mode
  array.open(TILEDB_READ);

  num = array.metadata_num();
  CHECK(num == 2);

  // Close array
  array.close();

  // Write once more
  array.open(TILEDB_WRITE);

  // Write items
  v = 50;
  array.put_metadata("d", TILEDB_INT32, 1, &v);

  // Close array
  array.close();

  // Consolidate again
  tiledb::Array::consolidate(ctx, array_name_, &consolidation_cfg);

  // Open the array in read mode
  array.open(TILEDB_READ);

  num = array.metadata_num();
  CHECK(num == 3);

  array.get_metadata("cccc", &v_type, &v_num, &v_r);
  CHECK(v_type == TILEDB_INT32);
  CHECK(v_num == 1);
  CHECK(*((const int32_t*)v_r) == 10);

  array.get_metadata("d", &v_type, &v_num, &v_r);
  CHECK(v_type == TILEDB_INT32);
  CHECK(v_num == 1);
  CHECK(*((const int32_t*)v_r) == 50);

  // Close array
  array.close();
}

TEST_CASE_METHOD(
    CPPMetadataFx, "C++ Metadata, open at", "[cppapi][metadata][open-at]") {
  // Create default array
  create_default_array_1d();

  // Create and open array in write mode
  tiledb::Context ctx;
  tiledb::Array array(ctx, array_name_, TILEDB_WRITE);

  // Write items
  int32_t v = 5;
  array.put_metadata("aaa", TILEDB_INT32, 1, &v);
  float f[] = {1.1f, 1.2f};
  array.put_metadata("bb", TILEDB_FLOAT32, 2, f);

  // Close array
  array.close();

  // Prevent array metadata filename/timestamp conflicts
  auto timestamp = tiledb::sm::utils::time::timestamp_now_ms();
  std::this_thread::sleep_for(std::chrono::milliseconds(1));

  // Update
  array.open(TILEDB_WRITE);
  array.delete_metadata("aaa");
  array.close();

  // Open the array in read mode at a timestamp
  array.set_open_timestamp_end(timestamp);
  array.open(TILEDB_READ);

  // Read
  const void* v_r;
  tiledb_datatype_t v_type;
  uint32_t v_num;
  array.get_metadata("aaa", &v_type, &v_num, &v_r);
  CHECK(v_type == TILEDB_INT32);
  CHECK(v_num == 1);
  CHECK(*((const int32_t*)v_r) == 5);

  uint64_t num = array.metadata_num();
  CHECK(num == 2);

  // Close array
  array.close();
}

TEST_CASE_METHOD(
    CPPMetadataFx, "C++ Metadata, reopen", "[cppapi][metadata][reopen]") {
  // Create default array
  create_default_array_1d();

  // Open array in write mode
  tiledb::Context ctx;
  tiledb::Array array(ctx, array_name_, TILEDB_WRITE);

  // Write items
  int32_t v = 5;
  array.put_metadata("aaa", TILEDB_INT32, 1, &v);
  float f[] = {1.1f, 1.2f};
  array.put_metadata("bb", TILEDB_FLOAT32, 2, f);

  // Close array
  array.close();

  // Prevent array metadata filename/timestamp conflicts
  auto timestamp = tiledb::sm::utils::time::timestamp_now_ms();
  std::this_thread::sleep_for(std::chrono::milliseconds(1));

  // Update
  array.open(TILEDB_WRITE);
  array.delete_metadata("aaa");
  array.close();

  // Open the array in read mode at a timestamp
  array.set_open_timestamp_end(timestamp);
  array.open(TILEDB_READ);

  // Read
  const void* v_r;
  tiledb_datatype_t v_type;
  uint32_t v_num;
  array.get_metadata("aaa", &v_type, &v_num, &v_r);
  CHECK(v_type == TILEDB_INT32);
  CHECK(v_num == 1);
  CHECK(*((const int32_t*)v_r) == 5);

  uint64_t num = array.metadata_num();
  CHECK(num == 2);

  // Reopen
  array.reopen();

  // Read
  array.get_metadata("aaa", &v_type, &v_num, &v_r);
  CHECK(v_r == nullptr);

  num = array.metadata_num();
  CHECK(num == 1);

  // Close array
  array.close();
}

TEST_CASE_METHOD(
    CPPMetadataFx,
    "C++ Metadata, encryption",
    "[cppapi][metadata][encryption]") {
  // Create default array
  create_default_array_1d_with_key();

  // Create and open array in write mode
  tiledb::Config cfg;
  std::string enc_type_str =
      encryption_type_str((tiledb::sm::EncryptionType)enc_type_);
  cfg["sm.encryption_type"] = enc_type_str.c_str();
  cfg["sm.encryption_key"] = key_;
  tiledb::Context ctx(cfg);
  tiledb::Array array(ctx, array_name_, TILEDB_WRITE);

  // Write items
  int32_t v = 5;
  array.put_metadata("aaa", TILEDB_INT32, 1, &v);
  float f[] = {1.1f, 1.2f};
  array.put_metadata("bb", TILEDB_FLOAT32, 2, f);

  // Close array
  array.close();

  // Prevent array metadata filename/timestamp conflicts
  std::this_thread::sleep_for(std::chrono::milliseconds(1));

  // Update
  array.open(TILEDB_WRITE);
  array.delete_metadata("aaa");
  v = 10;
  array.put_metadata("cccc", TILEDB_INT32, 1, &v);
  array.close();

  // Open the array in read mode
  array.open(TILEDB_READ);

  // Read
  const void* v_r;
  tiledb_datatype_t v_type;
  uint32_t v_num;
  array.get_metadata("aaa", &v_type, &v_num, &v_r);
  CHECK(v_r == nullptr);

  array.get_metadata("bb", &v_type, &v_num, &v_r);
  CHECK(v_type == TILEDB_FLOAT32);
  CHECK(v_num == 2);
  CHECK(((const float*)v_r)[0] == 1.1f);
  CHECK(((const float*)v_r)[1] == 1.2f);

  array.get_metadata("cccc", &v_type, &v_num, &v_r);
  CHECK(v_type == TILEDB_INT32);
  CHECK(v_num == 1);
  CHECK(*((const int32_t*)v_r) == 10);

  uint64_t num = array.metadata_num();
  CHECK(num == 2);

  std::string key;
  array.get_metadata_from_index(0, &key, &v_type, &v_num, &v_r);
  CHECK(v_type == TILEDB_FLOAT32);
  CHECK(v_num == 2);
  CHECK(((const float*)v_r)[0] == 1.1f);
  CHECK(((const float*)v_r)[1] == 1.2f);
  CHECK(key.size() == strlen("bb"));
  CHECK(!strncmp(key.data(), "bb", strlen("bb")));

  // Close array
  array.close();

  // Consolidate without key - error
  tiledb::Config consolidate_without_key;
  tiledb::Context ctx_without_key(consolidate_without_key);
  CHECK_THROWS(tiledb::Array::consolidate(
      ctx_without_key, array_name_, &consolidate_without_key));

  // Consolidate with key - ok
  tiledb::Config consolidation_cfg;
  consolidation_cfg["sm.consolidation.mode"] = "array_meta";
  tiledb::Array::consolidate(ctx, array_name_, &consolidation_cfg);

  // Open the array in read mode
  array.open(TILEDB_READ);

  num = array.metadata_num();
  CHECK(num == 2);

  // Close array
  array.close();

  // Write once more
  array.open(TILEDB_WRITE);

  // Write items
  v = 50;
  array.put_metadata("d", TILEDB_INT32, 1, &v);

  // Close array
  array.close();

  // Consolidate again
  tiledb::Array::consolidate_metadata(ctx, array_name_, &consolidation_cfg);

  // Open the array in read mode
  array.open(TILEDB_READ);

  num = array.metadata_num();
  CHECK(num == 3);

  array.get_metadata("cccc", &v_type, &v_num, &v_r);
  CHECK(v_type == TILEDB_INT32);
  CHECK(v_num == 1);
  CHECK(*((const int32_t*)v_r) == 10);

  array.get_metadata("d", &v_type, &v_num, &v_r);
  CHECK(v_type == TILEDB_INT32);
  CHECK(v_num == 1);
  CHECK(*((const int32_t*)v_r) == 50);

  // Close array
  array.close();
}
