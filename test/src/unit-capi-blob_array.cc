/**
 * @file   unit-capi-file.cc
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
 * Tests the TileDB File type with the C API.
 */

#include "catch.hpp"
#include "test/src/helpers.h"
#include "test/src/vfs_helpers.h"
#include "tiledb/sm/c_api/tiledb_experimental.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/global_state/unit_test_config.h"

#include <iostream>

using namespace tiledb::test;

static const std::string files_dir =
    std::string(TILEDB_TEST_INPUTS_DIR) + "/files";

struct BlobArrayFx {
  // TileDB context
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;
  tiledb_config_t* config_;

  // Vector of supported filesystems
  const std::vector<std::unique_ptr<SupportedFs>> fs_vec_;

  // Encryption parameters
  tiledb_encryption_type_t encryption_type_ = TILEDB_NO_ENCRYPTION;
  const char* encryption_key_ = nullptr;

  // Functions
  BlobArrayFx();
  ~BlobArrayFx();
  void create_temp_dir(const std::string& path) const;
  void remove_temp_dir(const std::string& path) const;
  static std::string random_name(const std::string& prefix);
};

BlobArrayFx::BlobArrayFx()
    : fs_vec_(vfs_test_get_fs_vec()) {
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config_, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);
  // Initialize vfs test
  REQUIRE(vfs_test_init(fs_vec_, &ctx_, &vfs_, config_).ok());
}

BlobArrayFx::~BlobArrayFx() {
  // Close vfs test
  REQUIRE(vfs_test_close(fs_vec_, ctx_, vfs_).ok());
  tiledb_vfs_free(&vfs_);
  tiledb_ctx_free(&ctx_);
  tiledb_config_free(&config_);
}

void BlobArrayFx::create_temp_dir(const std::string& path) const {
  remove_temp_dir(path);
  REQUIRE(tiledb_vfs_create_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

void BlobArrayFx::remove_temp_dir(const std::string& path) const {
  int is_dir = 0;
  REQUIRE(tiledb_vfs_is_dir(ctx_, vfs_, path.c_str(), &is_dir) == TILEDB_OK);
  if (is_dir)
    REQUIRE(tiledb_vfs_remove_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
  else {
    int is_file = 0;
    REQUIRE(
        tiledb_vfs_is_file(ctx_, vfs_, path.c_str(), &is_file) == TILEDB_OK);
    if (is_file)
      REQUIRE(tiledb_vfs_remove_file(ctx_, vfs_, path.c_str()) == TILEDB_OK);
  }
}

std::string BlobArrayFx::random_name(const std::string& prefix) {
  std::stringstream ss;
  ss << prefix << "-" << std::this_thread::get_id() << "-"
     << TILEDB_TIMESTAMP_NOW_MS;
  return ss.str();
}

TEST_CASE_METHOD(
    BlobArrayFx, "C API: Test blob_array create default", "[capi][blob_array][basic]") {
  std::string temp_dir = fs_vec_[0]->temp_dir();

  std::string array_name = temp_dir + "blob_array_test_create";

  SECTION("- without encryption") {
    encryption_type_ = TILEDB_NO_ENCRYPTION;
    encryption_key_ = nullptr;
  }

  SECTION("- with encryption") {
    encryption_type_ = TILEDB_AES_256_GCM;
    encryption_key_ = "0123456789abcdeF0123456789abcdeF";
  }

  create_temp_dir(temp_dir);
  
//  CHECK(tiledb_array_as_file_create(ctx, array_name.c_str()) == TILEDB_OK);
  
//  remote_temp_dir(temp_dir);

//  tiledb_file_t* file;
//  CHECK(tiledb_file_alloc(ctx_, array_name.c_str(), &file) == TILEDB_OK);

  tiledb_config_t* cfg = nullptr;
  tiledb_error_t* err = nullptr;
  if (encryption_type_ != TILEDB_NO_ENCRYPTION) {
    int32_t rc = tiledb_config_alloc(&cfg, &err);
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
//    rc = tiledb_file_set_config(ctx_, file, cfg);
//    CHECK(rc == TILEDB_OK);
//    tiledb_config_free(&cfg);
    uint32_t key_len = (uint32_t)strlen(encryption_key_);
    tiledb::sm::UnitTestConfig::instance().array_encryption_key_length.set(
        key_len);
//    CHECK(tiledb_file_create_default(ctx_, file, nullptr) == TILEDB_OK);
  }

  //CHECK(tiledb_array_as_file_create(ctx_, array_name.c_str(), cfg) == TILEDB_OK);

  tiledb_array_t* array;
  CHECK(
      tiledb_array_as_file_obtain(ctx_, &array, array_name.c_str(), cfg) ==
      TILEDB_OK);

//  if (cfg) {
//    CHECK(tiledb_array_set_config(ctx_, array, cfg) == TILEDB_OK);
//  }

  // Clean up
  if(cfg){
    tiledb_config_free(&cfg);
  }
  if(err){
    //??
  }
  remove_temp_dir(array_name);
  //tiledb_file_free(&file);

}

TEST_CASE_METHOD(
    BlobArrayFx, "C API: Test blob_array create with import from uri", "[capi][blob_array][basic]") {
  std::string temp_dir = fs_vec_[0]->temp_dir();

  std::string array_name = temp_dir + "blob_array_test_create";
#if 0
  SECTION("- without encryption") {
    encryption_type_ = TILEDB_NO_ENCRYPTION;
    encryption_key_ = nullptr;
  }
#endif

  SECTION("- with encryption") {
    encryption_type_ = TILEDB_AES_256_GCM;
    encryption_key_ = "0123456789abcdeF0123456789abcdeF";
  }

  create_temp_dir(temp_dir);

//  tiledb_file_t* file;
//  CHECK(tiledb_file_alloc(ctx_, array_name.c_str(), &file) == TILEDB_OK);

  tiledb_config_t* cfg = nullptr;
  tiledb_error_t* err = nullptr;
  if (encryption_type_ != TILEDB_NO_ENCRYPTION) {
    int32_t rc = tiledb_config_alloc(&cfg, &err);
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
//    rc = tiledb_file_set_config(ctx_, file, cfg);
//    CHECK(rc == TILEDB_OK);
//    tiledb_config_free(&cfg);
    uint32_t key_len = (uint32_t)strlen(encryption_key_);
    tiledb::sm::UnitTestConfig::instance().array_encryption_key_length.set(
        key_len);
  }

  const std::string csv_path = files_dir + "/" + "quickstart_dense.csv";
  tiledb_array_t* array;
  CHECK(
      tiledb_array_as_file_obtain(ctx_, &array, array_name.c_str(), cfg) ==
      TILEDB_OK);
//  if (cfg) {
//    CHECK(tiledb_array_set_config(ctx_, array, cfg) == TILEDB_OK);
//  }
  CHECK(
      tiledb_array_as_file_import(
      ctx_, array, csv_path.c_str()) == TILEDB_OK);

  //  CHECK(
  //      tiledb_file_create_from_uri(ctx_, file, csv_path.c_str(), nullptr) ==
//      TILEDB_OK);

  // Clean up, ctx_/vfs_ are freed on test destructor
  if(cfg) {
    tiledb_config_free(&cfg);
  }
  if(err){
    //??
  }
  remove_temp_dir(array_name);
  //tiledb_file_free(&file);
}

#if 0
TEST_CASE_METHOD(
    BlobArrayFx, "C API: Test file create from vfsfh", "[capi][file][basic]") {
  std::string temp_dir = fs_vec_[0]->temp_dir();

  std::string array_name = temp_dir + "file_test_create";
  SECTION("- without encryption") {
    encryption_type_ = TILEDB_NO_ENCRYPTION;
    encryption_key_ = nullptr;
  }

  SECTION("- with encryption") {
    encryption_type_ = TILEDB_AES_256_GCM;
    encryption_key_ = "0123456789abcdeF0123456789abcdeF";
  }

  create_temp_dir(temp_dir);

  tiledb_file_t* file;
  CHECK(tiledb_file_alloc(ctx_, array_name.c_str(), &file) == TILEDB_OK);

  if (encryption_type_ != TILEDB_NO_ENCRYPTION) {
    tiledb_config_t* cfg;
    tiledb_error_t* err = nullptr;
    int32_t rc = tiledb_config_alloc(&cfg, &err);
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
    rc = tiledb_file_set_config(ctx_, file, cfg);
    CHECK(rc == TILEDB_OK);
    tiledb_config_free(&cfg);
    uint32_t key_len = (uint32_t)strlen(encryption_key_);
    tiledb::sm::UnitTestConfig::instance().array_encryption_key_length.set(
        key_len);
  }

  const std::string csv_path = files_dir + "/" + "quickstart_dense.csv";
  tiledb_vfs_fh_t* fh;

  int rc =
      tiledb_vfs_open(ctx_, vfs_, csv_path.c_str(), TILEDB_VFS_APPEND, &fh);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_file_create_from_vfs_fh(ctx_, file, fh, nullptr);
  REQUIRE(rc == TILEDB_ERR);

  // Reopen in read mode
  rc = tiledb_vfs_close(ctx_, fh);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_open(ctx_, vfs_, csv_path.c_str(), TILEDB_VFS_READ, &fh);
  REQUIRE(rc == TILEDB_OK);
  CHECK(tiledb_file_create_from_vfs_fh(ctx_, file, fh, nullptr) == TILEDB_OK);

  // Clean up, ctx_/vfs_ are freed on test destructor
  tiledb_file_free(&file);
  rc = tiledb_vfs_close(ctx_, fh);
  REQUIRE(rc == TILEDB_OK);
  tiledb_vfs_fh_free(&fh);
  remove_temp_dir(array_name);
}
#endif

TEST_CASE_METHOD(
    BlobArrayFx,
    "C API: Test blob_array save and export from uri",
    "[capi][blob_array][basic]") {
  std::string temp_dir = fs_vec_[0]->temp_dir();

  std::string array_name = temp_dir + "blob_array_test_create";
  std::string output_path = temp_dir + "out";
  SECTION("- without encryption") {
    encryption_type_ = TILEDB_NO_ENCRYPTION;
    encryption_key_ = nullptr;
  }

  SECTION("- with encryption") {
    encryption_type_ = TILEDB_AES_256_GCM;
    encryption_key_ = "0123456789abcdeF0123456789abcdeF";
  }

  create_temp_dir(temp_dir);

  tiledb_config_t* cfg = nullptr;
  tiledb_error_t* err = nullptr;
  if (encryption_type_ != TILEDB_NO_ENCRYPTION) {
    int32_t rc = tiledb_config_alloc(&cfg, &err);
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
    uint32_t key_len = (uint32_t)strlen(encryption_key_);
    tiledb::sm::UnitTestConfig::instance().array_encryption_key_length.set(
        key_len);
  }

  const std::string csv_name = "quickstart_dense.csv";
  const std::string csv_path = files_dir + "/" + csv_name;
  tiledb_array_t* array;
  CHECK(
      tiledb_array_as_file_obtain(ctx_, &array, array_name.c_str(), cfg) ==
      TILEDB_OK);
  CHECK(
      tiledb_array_as_file_import(ctx_, array, csv_path.c_str()) == TILEDB_OK);

  CHECK(
      tiledb_array_as_file_export(ctx_, array, output_path.c_str()) ==
      TILEDB_OK);

  uint64_t original_file_size = 0;
  CHECK(
      tiledb_vfs_file_size(ctx_, vfs_, csv_path.c_str(), &original_file_size) ==
      TILEDB_OK);
  uint64_t exported_file_size = 0;
  CHECK(
      tiledb_vfs_file_size(
          ctx_, vfs_, output_path.c_str(), &exported_file_size) == TILEDB_OK);

  REQUIRE(exported_file_size == original_file_size);

  //TDB: compare actual contents for equality?

  remove_temp_dir(array_name);
  remove_temp_dir(output_path);
}

