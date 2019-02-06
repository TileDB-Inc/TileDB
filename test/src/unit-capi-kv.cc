/**
 * @file   unit-capi-kv.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB Inc.
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
 * Tests of C API for key-value store operations.
 */

#include "catch.hpp"
#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif
#include "test/src/helpers.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/misc/utils.h"

#include <iostream>
#include <sstream>
#include <thread>

struct KVFx {
  std::string ATTR_1 = "a1";
  const char* ATTR_2 = "a2";
  const char* ATTR_3 = "a3";
  const char* KV_NAME = "kv";
  int KEY1 = 100;
  const int KEY1_A1 = 1;
  const char* KEY1_A2 = "a";
  const float KEY1_A3[2] = {1.1f, 1.2f};
  const float KEY2 = 200.0;
  const int KEY2_A1 = 2;
  const char* KEY2_A2 = "bb";
  const float KEY2_A3[2] = {2.1f, 2.2f};
  const double KEY3[2] = {300.0, 300.1};
  const int KEY3_A1 = 3;
  const char* KEY3_A2 = "ccc";
  const float KEY3_A3[2] = {3.1f, 3.2f};
  const char* KEY4 = "key_4";
  const int KEY4_A1 = 4;
  const char* KEY4_A2 = "dddd";
  const float KEY4_A3[2] = {4.1f, 4.2f};

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
  KVFx();
  ~KVFx();
  void check_single_read(const std::string& path);
  void check_iter(const std::string& path);
  void check_kv_item();
  void check_kv_item(tiledb_kv_item_t* kv_item);
  void check_write(const std::string& path);
  void create_kv(const std::string& path);
  void create_simple_kv(const std::string& path);
  void create_temp_dir(const std::string& path);
  void remove_temp_dir(const std::string& path);
  static std::string random_bucket_name(const std::string& prefix);
  void set_supported_fs();
};

KVFx::KVFx() {
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

KVFx::~KVFx() {
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

void KVFx::set_supported_fs() {
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

void KVFx::create_temp_dir(const std::string& path) {
  remove_temp_dir(path);
  REQUIRE(tiledb_vfs_create_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

void KVFx::remove_temp_dir(const std::string& path) {
  int is_dir = 0;
  REQUIRE(tiledb_vfs_is_dir(ctx_, vfs_, path.c_str(), &is_dir) == TILEDB_OK);
  if (is_dir)
    REQUIRE(tiledb_vfs_remove_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

std::string KVFx::random_bucket_name(const std::string& prefix) {
  std::stringstream ss;
  ss << prefix << "-" << std::this_thread::get_id() << "-"
     << TILEDB_TIMESTAMP_NOW_MS;
  return ss.str();
}

void KVFx::create_kv(const std::string& path) {
  // Create attributes
  tiledb_attribute_t* a1;
  int rc = tiledb_attribute_alloc(ctx_, ATTR_1.c_str(), TILEDB_INT32, &a1);
  CHECK(rc == TILEDB_OK);
  rc = set_attribute_compression_filter(ctx_, a1, TILEDB_FILTER_LZ4, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a1, 1);
  CHECK(rc == TILEDB_OK);
  tiledb_attribute_t* a2;
  rc = tiledb_attribute_alloc(ctx_, ATTR_2, TILEDB_CHAR, &a2);
  CHECK(rc == TILEDB_OK);
  rc = set_attribute_compression_filter(ctx_, a2, TILEDB_FILTER_GZIP, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a2, TILEDB_VAR_NUM);
  CHECK(rc == TILEDB_OK);
  tiledb_attribute_t* a3;
  rc = tiledb_attribute_alloc(ctx_, ATTR_3, TILEDB_FLOAT32, &a3);
  CHECK(rc == TILEDB_OK);
  rc = set_attribute_compression_filter(ctx_, a3, TILEDB_FILTER_ZSTD, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a3, 2);
  CHECK(rc == TILEDB_OK);

  // Create key-value schema
  tiledb_kv_schema_t* kv_schema;
  rc = tiledb_kv_schema_alloc(ctx_, &kv_schema);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_schema_add_attribute(ctx_, kv_schema, a1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_schema_add_attribute(ctx_, kv_schema, a2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_schema_add_attribute(ctx_, kv_schema, a3);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_schema_set_capacity(ctx_, kv_schema, 10);
  CHECK(rc == TILEDB_OK);

  // Check array schema
  rc = tiledb_kv_schema_check(ctx_, kv_schema);
  CHECK(rc == TILEDB_OK);

  // Create key-value store
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_kv_create(ctx_, path.c_str(), kv_schema);
  } else {
    rc = tiledb_kv_create_with_key(
        ctx_,
        path.c_str(),
        kv_schema,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_));
  }
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_attribute_free(&a1);
  tiledb_attribute_free(&a2);
  tiledb_attribute_free(&a3);
  tiledb_kv_schema_free(&kv_schema);

  // Check getting encryption type
  tiledb_encryption_type_t enc_type;
  rc = tiledb_kv_encryption_type(ctx_, path.c_str(), &enc_type);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(enc_type == encryption_type_);

  // Load schema
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_kv_schema_load(ctx_, path.c_str(), &kv_schema);
  } else {
    rc = tiledb_kv_schema_load_with_key(
        ctx_,
        path.c_str(),
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_),
        &kv_schema);
  }
  CHECK(rc == TILEDB_OK);

  uint64_t capacity;
  rc = tiledb_kv_schema_get_capacity(ctx_, kv_schema, &capacity);
  CHECK(rc == TILEDB_OK);
  CHECK(capacity == 10);

  // Clean up again
  tiledb_kv_schema_free(&kv_schema);
}

void KVFx::create_simple_kv(const std::string& path) {
  // Create attribute
  tiledb_attribute_t* a;
  int rc = tiledb_attribute_alloc(ctx_, "a", TILEDB_INT32, &a);
  CHECK(rc == TILEDB_OK);

  // Create key-value schema
  tiledb_kv_schema_t* kv_schema;
  rc = tiledb_kv_schema_alloc(ctx_, &kv_schema);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_schema_add_attribute(ctx_, kv_schema, a);
  CHECK(rc == TILEDB_OK);

  // Check array schema
  rc = tiledb_kv_schema_check(ctx_, kv_schema);
  CHECK(rc == TILEDB_OK);

  // Create key-value store
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_kv_create(ctx_, path.c_str(), kv_schema);
  } else {
    rc = tiledb_kv_create_with_key(
        ctx_,
        path.c_str(),
        kv_schema,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_));
  }
  CHECK(rc == TILEDB_OK);

  // Clean up
  tiledb_attribute_free(&a);
  tiledb_kv_schema_free(&kv_schema);
}

void KVFx::check_kv_item() {
  tiledb_kv_item_t* kv_item;
  int rc = tiledb_kv_item_alloc(ctx_, &kv_item);
  REQUIRE(rc == TILEDB_OK);

  const void* key;
  uint64_t key_size;
  tiledb_datatype_t key_type;
  rc = tiledb_kv_item_get_key(ctx_, kv_item, &key, &key_type, &key_size);
  REQUIRE(rc == TILEDB_OK);
  CHECK(key == nullptr);

  std::string k("key");
  rc = tiledb_kv_item_set_key(
      ctx_, kv_item, k.c_str(), TILEDB_CHAR, k.size() + 1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_kv_item_get_key(ctx_, kv_item, &key, &key_type, &key_size);
  REQUIRE(rc == TILEDB_OK);
  CHECK(!strcmp((const char*)key, k.c_str()));
  CHECK(key_type == TILEDB_CHAR);
  CHECK(key_size == k.size() + 1);

  const void* value;
  uint64_t value_size;
  tiledb_datatype_t value_type;
  rc = tiledb_kv_item_get_value(
      ctx_, kv_item, "foo", &value, &value_type, &value_size);
  REQUIRE(rc == TILEDB_ERR);

  rc = tiledb_kv_item_get_value(
      ctx_, kv_item, nullptr, &value, &value_type, &value_size);
  REQUIRE(rc == TILEDB_ERR);

  int value_int;
  rc = tiledb_kv_item_set_value(
      ctx_, kv_item, "attr", &value_int, TILEDB_INT32, sizeof(int));
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_kv_item_get_value(
      ctx_, kv_item, "attr", &value, &value_type, &value_size);
  REQUIRE(rc == TILEDB_OK);
  CHECK(*(int*)value == value_int);
  CHECK(value_type == TILEDB_INT32);
  CHECK(value_size == sizeof(int));

  tiledb_kv_item_free(&kv_item);
  REQUIRE(rc == TILEDB_OK);
}

void KVFx::check_write(const std::string& path) {
  tiledb_kv_t* kv;
  int rc = tiledb_kv_alloc(ctx_, path.c_str(), &kv);
  REQUIRE(rc == TILEDB_OK);
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_kv_open(ctx_, kv, TILEDB_WRITE);
  } else {
    rc = tiledb_kv_open_with_key(
        ctx_,
        kv,
        TILEDB_WRITE,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_));
  }
  REQUIRE(rc == TILEDB_OK);

  // The kv is not dirty
  int is_dirty;
  rc = tiledb_kv_is_dirty(ctx_, kv, &is_dirty);
  CHECK(rc == TILEDB_OK);
  CHECK(is_dirty == 0);

  // Add key-value item #1
  tiledb_kv_item_t* kv_item1;
  rc = tiledb_kv_item_alloc(ctx_, &kv_item1);
  REQUIRE(rc == TILEDB_OK);
  rc =
      tiledb_kv_item_set_key(ctx_, kv_item1, &KEY1, TILEDB_INT32, sizeof(KEY1));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_value(
      ctx_, kv_item1, ATTR_1.c_str(), &KEY1_A1, TILEDB_INT32, sizeof(int));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_value(
      ctx_, kv_item1, ATTR_2, KEY1_A2, TILEDB_CHAR, strlen(KEY1_A2) + 1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_value(
      ctx_, kv_item1, ATTR_3, KEY1_A3, TILEDB_FLOAT32, 2 * sizeof(float));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_add_item(ctx_, kv, kv_item1);
  CHECK(rc == TILEDB_OK);

  // Add key-value item #2
  tiledb_kv_item_t* kv_item2;
  rc = tiledb_kv_item_alloc(ctx_, &kv_item2);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_key(
      ctx_, kv_item2, &KEY2, TILEDB_FLOAT32, sizeof(KEY2));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_value(
      ctx_, kv_item2, ATTR_1.c_str(), &KEY2_A1, TILEDB_INT32, sizeof(int));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_value(
      ctx_, kv_item2, ATTR_2, KEY2_A2, TILEDB_CHAR, strlen(KEY2_A2) + 1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_value(
      ctx_, kv_item2, ATTR_3, KEY2_A3, TILEDB_FLOAT32, 2 * sizeof(float));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_add_item(ctx_, kv, kv_item2);
  CHECK(rc == TILEDB_OK);

  // The kv is now dirty
  rc = tiledb_kv_is_dirty(ctx_, kv, &is_dirty);
  CHECK(rc == TILEDB_OK);
  CHECK(is_dirty == 1);

  // Flush
  rc = tiledb_kv_flush(ctx_, kv);
  REQUIRE(rc == TILEDB_OK);

  // The kv is not dirty after the flush
  rc = tiledb_kv_is_dirty(ctx_, kv, &is_dirty);
  CHECK(rc == TILEDB_OK);
  CHECK(is_dirty == 0);

  // Add key-value item #3
  tiledb_kv_item_t* kv_item3;
  rc = tiledb_kv_item_alloc(ctx_, &kv_item3);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_key(
      ctx_, kv_item3, KEY3, TILEDB_FLOAT64, sizeof(KEY3));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_value(
      ctx_, kv_item3, ATTR_1.c_str(), &KEY3_A1, TILEDB_INT32, sizeof(int));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_value(
      ctx_, kv_item3, ATTR_2, KEY3_A2, TILEDB_CHAR, strlen(KEY3_A2) + 1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_value(
      ctx_, kv_item3, ATTR_3, KEY3_A3, TILEDB_FLOAT32, 2 * sizeof(float));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_add_item(ctx_, kv, kv_item3);
  CHECK(rc == TILEDB_OK);

  // Add key-value item #4
  tiledb_kv_item_t* kv_item4;
  rc = tiledb_kv_item_alloc(ctx_, &kv_item4);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_key(
      ctx_, kv_item4, KEY4, TILEDB_CHAR, strlen(KEY4) + 1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_value(
      ctx_, kv_item4, ATTR_1.c_str(), &KEY4_A1, TILEDB_INT32, sizeof(int));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_value(
      ctx_, kv_item4, ATTR_2, KEY4_A2, TILEDB_CHAR, strlen(KEY4_A2) + 1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_value(
      ctx_, kv_item4, ATTR_3, KEY4_A3, TILEDB_FLOAT32, 2 * sizeof(float));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_add_item(ctx_, kv, kv_item4);
  CHECK(rc == TILEDB_OK);

  // Add key with invalid attributes
  tiledb_kv_item_t* kv_item5;
  rc = tiledb_kv_item_alloc(ctx_, &kv_item5);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_key(
      ctx_, kv_item5, KEY4, TILEDB_CHAR, strlen(KEY4) + 1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_value(
      ctx_, kv_item5, "foo", &KEY4_A1, TILEDB_INT32, sizeof(int));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_add_item(ctx_, kv, kv_item5);
  CHECK(rc == TILEDB_ERR);

  // Add key with invalid type
  tiledb_kv_item_t* kv_item6;
  rc = tiledb_kv_item_alloc(ctx_, &kv_item6);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_key(
      ctx_, kv_item6, KEY4, TILEDB_CHAR, strlen(KEY4) + 1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_value(
      ctx_,
      kv_item6,
      ATTR_1.c_str(),
      &KEY4_A1,
      TILEDB_UINT32,
      sizeof(unsigned));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_value(
      ctx_, kv_item6, ATTR_2, KEY4_A2, TILEDB_CHAR, strlen(KEY4_A2) + 1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_value(
      ctx_, kv_item6, ATTR_3, KEY4_A3, TILEDB_FLOAT32, 2 * sizeof(float));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_add_item(ctx_, kv, kv_item6);
  CHECK(rc == TILEDB_ERR);

  // Flush
  rc = tiledb_kv_flush(ctx_, kv);
  REQUIRE(rc == TILEDB_OK);

  // Close kv
  rc = tiledb_kv_close(ctx_, kv);
  REQUIRE(rc == TILEDB_OK);

  // Consolidate
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_kv_consolidate(ctx_, path.c_str(), nullptr);
  } else {
    rc = tiledb_kv_consolidate_with_key(
        ctx_,
        path.c_str(),
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_),
        nullptr);
  }
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  tiledb_kv_free(&kv);
  CHECK(kv == nullptr);
  tiledb_kv_item_free(&kv_item1);
  tiledb_kv_item_free(&kv_item2);
  tiledb_kv_item_free(&kv_item3);
  tiledb_kv_item_free(&kv_item4);
  tiledb_kv_item_free(&kv_item5);
  tiledb_kv_item_free(&kv_item6);
  CHECK(kv_item6 == nullptr);
}

void KVFx::check_single_read(const std::string& path) {
  // Open key-value store
  tiledb_kv_t* kv;
  int rc = tiledb_kv_alloc(ctx_, path.c_str(), &kv);
  REQUIRE(rc == TILEDB_OK);

  // Re-open should fail before open
  rc = tiledb_kv_reopen(ctx_, kv);
  REQUIRE(rc == TILEDB_ERR);

  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_kv_open(ctx_, kv, TILEDB_READ);
  } else {
    rc = tiledb_kv_open_with_key(
        ctx_,
        kv,
        TILEDB_READ,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_));
  }
  REQUIRE(rc == TILEDB_OK);

  // Re-open should succeed after open
  rc = tiledb_kv_reopen(ctx_, kv);
  REQUIRE(rc == TILEDB_OK);

  // For retrieving values
  const void *a1, *a2, *a3;
  uint64_t a1_size, a2_size, a3_size;
  tiledb_datatype_t a1_type, a2_type, a3_type;

  // Prepare keys
  int key1 = 100;
  tiledb_datatype_t key1_type = TILEDB_INT32;
  uint64_t key1_size = sizeof(int);

  float key2 = 200.0;
  tiledb_datatype_t key2_type = TILEDB_FLOAT32;
  uint64_t key2_size = sizeof(float);

  double key3[] = {300.0, 300.1};
  tiledb_datatype_t key3_type = TILEDB_FLOAT64;
  uint64_t key3_size = 2 * sizeof(double);

  char key4[] = "key_4";
  const char* key5 = "invalid";
  tiledb_datatype_t key4_type = TILEDB_CHAR;
  tiledb_datatype_t key5_type = TILEDB_CHAR;
  uint64_t key4_size = strlen(key4) + 1;
  uint64_t key5_size = strlen(key5) + 1;

  int has_key;

  // Get key-value item #1
  tiledb_kv_item_t* kv_item1;
  rc = tiledb_kv_get_item(ctx_, kv, &key1, key1_type, key1_size, &kv_item1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_kv_item_get_value(
      ctx_, kv_item1, ATTR_1.c_str(), &a1, &a1_type, &a1_size);
  CHECK(rc == TILEDB_OK);
  CHECK(*(int*)a1 == KEY1_A1);
  CHECK(a1_type == TILEDB_INT32);
  CHECK(a1_size == sizeof(int));
  rc =
      tiledb_kv_item_get_value(ctx_, kv_item1, ATTR_2, &a2, &a2_type, &a2_size);
  CHECK(rc == TILEDB_OK);
  CHECK(!strcmp((const char*)a2, KEY1_A2));
  CHECK(a2_type == TILEDB_CHAR);
  CHECK(a2_size == strlen(KEY1_A2) + 1);
  rc =
      tiledb_kv_item_get_value(ctx_, kv_item1, ATTR_3, &a3, &a3_type, &a3_size);
  CHECK(rc == TILEDB_OK);
  CHECK(!memcmp(a3, KEY1_A3, 2 * sizeof(float)));
  CHECK(a3_type == TILEDB_FLOAT32);
  CHECK(a3_size == 2 * sizeof(float));
  rc = tiledb_kv_has_key(ctx_, kv, &key1, key1_type, key1_size, &has_key);
  CHECK(rc == TILEDB_OK);
  CHECK(has_key == 1);

  // Get key-value item #2
  tiledb_kv_item_t* kv_item2;
  rc = tiledb_kv_has_key(ctx_, kv, &key2, key2_type, key2_size, &has_key);
  CHECK(rc == TILEDB_OK);
  CHECK(has_key == 1);
  rc = tiledb_kv_get_item(ctx_, kv, &key2, key2_type, key2_size, &kv_item2);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_kv_item_get_value(
      ctx_, kv_item2, ATTR_1.c_str(), &a1, &a1_type, &a1_size);
  CHECK(rc == TILEDB_OK);
  CHECK(*(int*)a1 == KEY2_A1);
  CHECK(a1_type == TILEDB_INT32);
  CHECK(a1_size == sizeof(int));
  rc =
      tiledb_kv_item_get_value(ctx_, kv_item2, ATTR_2, &a2, &a2_type, &a2_size);
  CHECK(rc == TILEDB_OK);
  CHECK(!strcmp((const char*)a2, KEY2_A2));
  CHECK(a2_type == TILEDB_CHAR);
  CHECK(a2_size == strlen(KEY2_A2) + 1);
  rc =
      tiledb_kv_item_get_value(ctx_, kv_item2, ATTR_3, &a3, &a3_type, &a3_size);
  CHECK(rc == TILEDB_OK);
  CHECK(!memcmp(a3, KEY2_A3, 2 * sizeof(float)));
  CHECK(a3_type == TILEDB_FLOAT32);
  CHECK(a3_size == 2 * sizeof(float));

  // Get key-value item #3
  tiledb_kv_item_t* kv_item3;
  rc = tiledb_kv_get_item(ctx_, kv, key3, key3_type, key3_size, &kv_item3);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_kv_item_get_value(
      ctx_, kv_item3, ATTR_1.c_str(), &a1, &a1_type, &a1_size);
  CHECK(rc == TILEDB_OK);
  CHECK(*(int*)a1 == KEY3_A1);
  CHECK(a1_type == TILEDB_INT32);
  CHECK(a1_size == sizeof(int));
  rc =
      tiledb_kv_item_get_value(ctx_, kv_item3, ATTR_2, &a2, &a2_type, &a2_size);
  CHECK(rc == TILEDB_OK);
  CHECK(!strcmp((const char*)a2, KEY3_A2));
  CHECK(a2_type == TILEDB_CHAR);
  CHECK(a2_size == strlen(KEY3_A2) + 1);
  rc =
      tiledb_kv_item_get_value(ctx_, kv_item3, ATTR_3, &a3, &a3_type, &a3_size);
  CHECK(rc == TILEDB_OK);
  CHECK(!memcmp(a3, KEY3_A3, 2 * sizeof(float)));
  CHECK(a3_type == TILEDB_FLOAT32);
  CHECK(a3_size == 2 * sizeof(float));

  // Get key-value item #4
  tiledb_kv_item_t* kv_item4;
  rc = tiledb_kv_get_item(ctx_, kv, key4, key4_type, key4_size, &kv_item4);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_kv_item_get_value(
      ctx_, kv_item4, ATTR_1.c_str(), &a1, &a1_type, &a1_size);
  CHECK(rc == TILEDB_OK);
  CHECK(*(int*)a1 == KEY4_A1);
  CHECK(a1_type == TILEDB_INT32);
  CHECK(a1_size == sizeof(int));
  rc =
      tiledb_kv_item_get_value(ctx_, kv_item4, ATTR_2, &a2, &a2_type, &a2_size);
  CHECK(rc == TILEDB_OK);
  CHECK(!strcmp((const char*)a2, KEY4_A2));
  CHECK(a2_type == TILEDB_CHAR);
  CHECK(a2_size == strlen(KEY4_A2) + 1);
  rc =
      tiledb_kv_item_get_value(ctx_, kv_item4, ATTR_3, &a3, &a3_type, &a3_size);
  CHECK(rc == TILEDB_OK);
  CHECK(!memcmp(a3, KEY4_A3, 2 * sizeof(float)));
  CHECK(a3_type == TILEDB_FLOAT32);
  CHECK(a3_size == 2 * sizeof(float));

  // Invalid key
  tiledb_kv_item_t* kv_item5;
  rc = tiledb_kv_get_item(ctx_, kv, key5, key5_type, key5_size, &kv_item5);
  REQUIRE(rc == TILEDB_OK);
  CHECK(kv_item5 == nullptr);
  rc = tiledb_kv_has_key(ctx_, kv, key5, key5_type, key5_size, &has_key);
  CHECK(rc == TILEDB_OK);
  CHECK(has_key == 0);

  // Close key-value store
  rc = tiledb_kv_close(ctx_, kv);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  tiledb_kv_free(&kv);
  tiledb_kv_item_free(&kv_item1);
  tiledb_kv_item_free(&kv_item2);
  tiledb_kv_item_free(&kv_item3);
  tiledb_kv_item_free(&kv_item4);
}

void KVFx::check_kv_item(tiledb_kv_item_t* kv_item) {
  const void *key, *value;
  tiledb_datatype_t key_type, value_type;
  uint64_t key_size, value_size;
  int rc = tiledb_kv_item_get_key(ctx_, kv_item, &key, &key_type, &key_size);
  REQUIRE(rc == TILEDB_OK);

  if (key_size == sizeof(int) && !memcmp(key, &KEY1, key_size)) {
    REQUIRE(key_type == TILEDB_INT32);
    rc = tiledb_kv_item_get_value(
        ctx_, kv_item, ATTR_1.c_str(), &value, &value_type, &value_size);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(value_type == TILEDB_INT32);
    REQUIRE(value_size == sizeof(int));
    REQUIRE(!memcmp(value, &KEY1_A1, value_size));
    rc = tiledb_kv_item_get_value(
        ctx_, kv_item, ATTR_2, &value, &value_type, &value_size);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(value_type == TILEDB_CHAR);
    REQUIRE(value_size == strlen(KEY1_A2) + 1);
    REQUIRE(!memcmp(value, KEY1_A2, value_size));
    rc = tiledb_kv_item_get_value(
        ctx_, kv_item, ATTR_3, &value, &value_type, &value_size);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(value_type == TILEDB_FLOAT32);
    REQUIRE(value_size == 2 * sizeof(float));
    REQUIRE(!memcmp(value, KEY1_A3, value_size));
  } else if (key_size == sizeof(float) && !memcmp(key, &KEY2, key_size)) {
    REQUIRE(key_type == TILEDB_FLOAT32);
    rc = tiledb_kv_item_get_value(
        ctx_, kv_item, ATTR_1.c_str(), &value, &value_type, &value_size);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(value_type == TILEDB_INT32);
    REQUIRE(value_size == sizeof(int));
    REQUIRE(!memcmp(value, &KEY2_A1, value_size));
    rc = tiledb_kv_item_get_value(
        ctx_, kv_item, ATTR_2, &value, &value_type, &value_size);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(value_type == TILEDB_CHAR);
    REQUIRE(value_size == strlen(KEY2_A2) + 1);
    REQUIRE(!memcmp(value, KEY2_A2, value_size));
    rc = tiledb_kv_item_get_value(
        ctx_, kv_item, ATTR_3, &value, &value_type, &value_size);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(value_type == TILEDB_FLOAT32);
    REQUIRE(value_size == 2 * sizeof(float));
    REQUIRE(!memcmp(value, KEY2_A3, value_size));
  } else if (key_size == 2 * sizeof(double) && !memcmp(key, KEY3, key_size)) {
    REQUIRE(key_type == TILEDB_FLOAT64);
    rc = tiledb_kv_item_get_value(
        ctx_, kv_item, ATTR_1.c_str(), &value, &value_type, &value_size);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(value_type == TILEDB_INT32);
    REQUIRE(value_size == sizeof(int));
    REQUIRE(!memcmp(value, &KEY3_A1, value_size));
    rc = tiledb_kv_item_get_value(
        ctx_, kv_item, ATTR_2, &value, &value_type, &value_size);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(value_type == TILEDB_CHAR);
    REQUIRE(value_size == strlen(KEY3_A2) + 1);
    REQUIRE(!memcmp(value, KEY3_A2, value_size));
    rc = tiledb_kv_item_get_value(
        ctx_, kv_item, ATTR_3, &value, &value_type, &value_size);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(value_type == TILEDB_FLOAT32);
    REQUIRE(value_size == 2 * sizeof(float));
    REQUIRE(!memcmp(value, KEY3_A3, value_size));
  } else if ((key_size == strlen(KEY4) + 1) && !memcmp(key, KEY4, key_size)) {
    REQUIRE(key_type == TILEDB_CHAR);
    rc = tiledb_kv_item_get_value(
        ctx_, kv_item, ATTR_1.c_str(), &value, &value_type, &value_size);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(value_type == TILEDB_INT32);
    REQUIRE(value_size == sizeof(int));
    REQUIRE(!memcmp(value, &KEY4_A1, value_size));
    rc = tiledb_kv_item_get_value(
        ctx_, kv_item, ATTR_2, &value, &value_type, &value_size);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(value_type == TILEDB_CHAR);
    REQUIRE(value_size == strlen(KEY4_A2) + 1);
    REQUIRE(!memcmp(value, KEY4_A2, value_size));
    rc = tiledb_kv_item_get_value(
        ctx_, kv_item, ATTR_3, &value, &value_type, &value_size);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(value_type == TILEDB_FLOAT32);
    REQUIRE(value_size == 2 * sizeof(float));
    REQUIRE(!memcmp(value, KEY4_A3, value_size));
  } else {
    REQUIRE(false);  // This should never happen
  }
}

void KVFx::check_iter(const std::string& path) {
  // Create key-value iterator
  tiledb_kv_t* kv;
  int rc = tiledb_kv_alloc(ctx_, path.c_str(), &kv);
  REQUIRE(rc == TILEDB_OK);
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_kv_open(ctx_, kv, TILEDB_READ);
  } else {
    rc = tiledb_kv_open_with_key(
        ctx_,
        kv,
        TILEDB_READ,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_));
  }
  REQUIRE(rc == TILEDB_OK);
  tiledb_kv_iter_t* kv_iter;
  rc = tiledb_kv_iter_alloc(ctx_, kv, &kv_iter);
  REQUIRE(rc == TILEDB_OK);

  int done;
  rc = tiledb_kv_iter_done(ctx_, kv_iter, &done);
  REQUIRE(rc == TILEDB_OK);

  while (!(bool)done) {
    tiledb_kv_item_t* kv_item;
    rc = tiledb_kv_iter_here(ctx_, kv_iter, &kv_item);
    REQUIRE(rc == TILEDB_OK);
    check_kv_item(kv_item);
    tiledb_kv_item_free(&kv_item);
    rc = tiledb_kv_iter_next(ctx_, kv_iter);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_kv_iter_done(ctx_, kv_iter, &done);
    REQUIRE(rc == TILEDB_OK);
  }
  CHECK(done);

  rc = tiledb_kv_iter_reset(ctx_, kv_iter);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_kv_iter_done(ctx_, kv_iter, &done);
  REQUIRE(rc == TILEDB_OK);
  CHECK(!done);

  rc = tiledb_kv_close(ctx_, kv);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  tiledb_kv_free(&kv);
  tiledb_kv_iter_free(&kv_iter);
}

TEST_CASE_METHOD(
    KVFx, "C API: Test key-value mode", "[capi], [kv], [kv-mode]") {
  std::string array_name;
  create_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
  array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + KV_NAME;
  create_kv(array_name);

  // Open key-value store in WRITE mode
  tiledb_kv_t* kv;
  int rc = tiledb_kv_alloc(ctx_, array_name.c_str(), &kv);
  REQUIRE(rc == TILEDB_OK);

  int is_open = -1;
  rc = tiledb_kv_is_open(ctx_, kv, &is_open);
  REQUIRE(rc == TILEDB_OK);
  CHECK(is_open == 0);
  rc = tiledb_kv_open(ctx_, kv, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_kv_is_open(ctx_, kv, &is_open);
  REQUIRE(rc == TILEDB_OK);
  CHECK(is_open == 1);

  char key[] = "key";
  int has_key;
  rc = tiledb_kv_has_key(ctx_, kv, key, TILEDB_CHAR, strlen(key), &has_key);
  CHECK(rc == TILEDB_ERR);

  tiledb_kv_item_t* kv_item;
  rc = tiledb_kv_get_item(ctx_, kv, &key, TILEDB_CHAR, strlen(key), &kv_item);
  CHECK(rc == TILEDB_ERR);

  rc = tiledb_kv_reopen(ctx_, kv);
  CHECK(rc == TILEDB_ERR);

  tiledb_kv_iter_t* kv_iter;
  rc = tiledb_kv_iter_alloc(ctx_, kv, &kv_iter);
  CHECK(rc == TILEDB_ERR);

  rc = tiledb_kv_close(ctx_, kv);
  CHECK(rc == TILEDB_OK);

  // Open key-value store in READ mode
  rc = tiledb_kv_open(ctx_, kv, TILEDB_READ);

  int dirty;
  rc = tiledb_kv_is_dirty(ctx_, kv, &dirty);
  CHECK(rc == TILEDB_ERR);

  rc = tiledb_kv_flush(ctx_, kv);
  CHECK(rc == TILEDB_ERR);

  rc = tiledb_kv_item_alloc(ctx_, &kv_item);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_key(ctx_, kv_item, key, TILEDB_CHAR, strlen(key));
  REQUIRE(rc == TILEDB_OK);
  int value_int = 0;
  rc = tiledb_kv_item_set_value(
      ctx_, kv_item, "attr", &value_int, TILEDB_INT32, sizeof(int));
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_kv_add_item(ctx_, kv, kv_item);
  CHECK(rc == TILEDB_ERR);

  // Clean up
  rc = tiledb_kv_close(ctx_, kv);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_is_open(ctx_, kv, &is_open);
  CHECK(is_open == 0);

  tiledb_kv_free(&kv);
  tiledb_kv_item_free(&kv_item);

  remove_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
}

TEST_CASE_METHOD(
    KVFx, "C API: Test key-value write, read and iter", "[capi], [kv]") {
  std::string array_name;

  if (supports_s3_) {
    // S3
    create_temp_dir(S3_TEMP_DIR);
    array_name = S3_TEMP_DIR + KV_NAME;
    create_kv(array_name);
    check_write(array_name);
    check_single_read(array_name);
    check_iter(array_name);
    remove_temp_dir(S3_TEMP_DIR);
  } else if (supports_hdfs_) {
    create_temp_dir(HDFS_TEMP_DIR);
    array_name = HDFS_TEMP_DIR + KV_NAME;
    create_kv(array_name);
    check_write(array_name);
    check_single_read(array_name);
    check_iter(array_name);
    remove_temp_dir(HDFS_TEMP_DIR);
  } else {
    // File
    create_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
    array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + KV_NAME;
    create_kv(array_name);
    check_kv_item();
    check_write(array_name);
    check_single_read(array_name);
    check_iter(array_name);
    remove_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
  }
}

TEST_CASE_METHOD(
    KVFx,
    "C API: Test encrypted key-value write, read and iter",
    "[capi], [kv], [encryption]") {
  std::string array_name;

  encryption_type_ = TILEDB_AES_256_GCM;
  encryption_key_ = "0123456789abcdeF0123456789abcdeF";

  if (supports_s3_) {
    // S3
    create_temp_dir(S3_TEMP_DIR);
    array_name = S3_TEMP_DIR + KV_NAME;
    create_kv(array_name);
    check_write(array_name);
    check_single_read(array_name);
    check_iter(array_name);
    remove_temp_dir(S3_TEMP_DIR);
  } else if (supports_hdfs_) {
    create_temp_dir(HDFS_TEMP_DIR);
    array_name = HDFS_TEMP_DIR + KV_NAME;
    create_kv(array_name);
    check_write(array_name);
    check_single_read(array_name);
    check_iter(array_name);
    remove_temp_dir(HDFS_TEMP_DIR);
  } else {
    // File
    create_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
    array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + KV_NAME;
    create_kv(array_name);
    check_kv_item();
    check_write(array_name);
    check_single_read(array_name);
    check_iter(array_name);
    remove_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
  }
}

TEST_CASE_METHOD(
    KVFx,
    "C API: Test key-value, anonymous attribute",
    "[capi], [kv], [anon-attr]") {
  ATTR_1 = "";

  std::string array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + KV_NAME;
  create_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
  create_kv(array_name);
  check_kv_item();
  check_write(array_name);
  check_single_read(array_name);
  check_iter(array_name);
  remove_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
}

TEST_CASE_METHOD(
    KVFx,
    "C API: Test key-value, get schema from opened kv",
    "[capi], [kv], [kv-get-schema]") {
  std::string array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + KV_NAME;
  create_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
  create_kv(array_name);

  // Open kv
  tiledb_kv_t* kv;
  int rc = tiledb_kv_alloc(ctx_, array_name.c_str(), &kv);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_kv_open(ctx_, kv, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  // Get schema
  tiledb_kv_schema_t* kv_schema;
  rc = tiledb_kv_get_schema(ctx_, kv, &kv_schema);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_schema_check(ctx_, kv_schema);
  CHECK(rc == TILEDB_OK);

  int32_t has_attr = 0;
  rc = tiledb_kv_schema_has_attribute(
      ctx_, kv_schema, ATTR_1.c_str(), &has_attr);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(has_attr == 1);
  has_attr = 0;
  rc = tiledb_kv_schema_has_attribute(ctx_, kv_schema, "", &has_attr);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(has_attr == 0);
  has_attr = 0;
  rc = tiledb_kv_schema_has_attribute(ctx_, kv_schema, "foo", &has_attr);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(has_attr == 0);

  // Clean up
  rc = tiledb_kv_close(ctx_, kv);
  REQUIRE(rc == TILEDB_OK);
  tiledb_kv_free(&kv);
  tiledb_kv_schema_free(&kv_schema);

  remove_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
}

TEST_CASE_METHOD(
    KVFx,
    "C API: Test opening key-value store at timestamp",
    "[capi], [kv], [kv-open-at]") {
  std::string temp_dir;
  if (supports_s3_)
    temp_dir = S3_TEMP_DIR;
  else if (supports_hdfs_)
    temp_dir = HDFS_TEMP_DIR;
  else
    temp_dir = FILE_URI_PREFIX + FILE_TEMP_DIR;
  std::string kv_name = temp_dir + "kv-open-at";

  SECTION("- without encryption") {
    encryption_type_ = TILEDB_NO_ENCRYPTION;
    encryption_key_ = nullptr;
  }

  SECTION("- with encryption") {
    encryption_type_ = TILEDB_AES_256_GCM;
    encryption_key_ = "0123456789abcdeF0123456789abcdeF";
  }

  create_temp_dir(temp_dir);
  create_simple_kv(kv_name);

  // --- WRITE ---
  // Open KV for writes
  tiledb_kv_t* kv;
  int rc = tiledb_kv_alloc(ctx_, kv_name.c_str(), &kv);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_kv_open_at(ctx_, kv, TILEDB_WRITE, 0);
  CHECK(rc == TILEDB_ERR);  // open_at is applicable only to reads
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_kv_open(ctx_, kv, TILEDB_WRITE);
  } else {
    rc = tiledb_kv_open_with_key(
        ctx_,
        kv,
        TILEDB_WRITE,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_));
  }
  CHECK(rc == TILEDB_OK);

  // Add a key-value item
  tiledb_kv_item_t* item;
  rc = tiledb_kv_item_alloc(ctx_, &item);
  int v = 10;
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_key(ctx_, item, "key", TILEDB_CHAR, strlen("key"));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_value(ctx_, item, "a", &v, TILEDB_INT32, sizeof(v));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_add_item(ctx_, kv, item);
  CHECK(rc == TILEDB_OK);

  // Flush
  rc = tiledb_kv_flush(ctx_, kv);
  REQUIRE(rc == TILEDB_OK);

  // Close kv
  rc = tiledb_kv_close(ctx_, kv);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  tiledb_kv_free(&kv);
  tiledb_kv_item_free(&item);

  // Get timestamp after first write
  uint64_t first_timestamp = TILEDB_TIMESTAMP_NOW_MS;

  // --- NORMAL OPEN AND READ ---
  // Open key-value store
  rc = tiledb_kv_alloc(ctx_, kv_name.c_str(), &kv);
  REQUIRE(rc == TILEDB_OK);
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_kv_open(ctx_, kv, TILEDB_READ);
  } else {
    rc = tiledb_kv_open_with_key(
        ctx_,
        kv,
        TILEDB_READ,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_));
  }
  REQUIRE(rc == TILEDB_OK);

  // For retrieving values
  const void* read_v;
  uint64_t read_v_size;
  tiledb_datatype_t read_v_type;
  const char* key = "key";
  uint64_t key_size = strlen(key);
  tiledb_datatype_t key_type = TILEDB_CHAR;

  // Check if key exists
  int has_key;
  rc = tiledb_kv_has_key(ctx_, kv, key, key_type, key_size, &has_key);
  CHECK(rc == TILEDB_OK);
  CHECK(has_key == 1);

  // Get key-value item
  tiledb_kv_item_t* read_item;
  rc = tiledb_kv_get_item(ctx_, kv, key, key_type, key_size, &read_item);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_kv_item_get_value(
      ctx_, read_item, "a", &read_v, &read_v_type, &read_v_size);
  CHECK(rc == TILEDB_OK);
  CHECK(read_v_type == TILEDB_INT32);
  CHECK(read_v_size == sizeof(int));
  CHECK(*(const int*)read_v == 10);

  // Close key-value store
  rc = tiledb_kv_close(ctx_, kv);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  tiledb_kv_free(&kv);
  tiledb_kv_item_free(&read_item);

  // --- OPEN AT ZERO TIMESTAMP ---
  // Open key-value store
  rc = tiledb_kv_alloc(ctx_, kv_name.c_str(), &kv);
  REQUIRE(rc == TILEDB_OK);
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_kv_open_at(ctx_, kv, TILEDB_READ, 0);
  } else {
    rc = tiledb_kv_open_at_with_key(
        ctx_,
        kv,
        TILEDB_READ,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_),
        0);
  }
  REQUIRE(rc == TILEDB_OK);

  // Check timestamp
  uint64_t timestamp_get;
  rc = tiledb_kv_get_timestamp(ctx_, kv, &timestamp_get);
  CHECK(rc == TILEDB_OK);
  CHECK(timestamp_get == 0);

  // Check if key exists
  rc = tiledb_kv_has_key(ctx_, kv, key, key_type, key_size, &has_key);
  CHECK(rc == TILEDB_OK);
  CHECK(has_key == 0);

  // Close key-value store
  rc = tiledb_kv_close(ctx_, kv);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  tiledb_kv_free(&kv);
  tiledb_kv_item_free(&read_item);

  // --- OPEN AT A LATER TIMESTAMP ---
  // Open key-value store
  rc = tiledb_kv_alloc(ctx_, kv_name.c_str(), &kv);
  REQUIRE(rc == TILEDB_OK);
  auto timestamp = TILEDB_TIMESTAMP_NOW_MS;
  if (encryption_type_ == TILEDB_NO_ENCRYPTION) {
    rc = tiledb_kv_open_at(ctx_, kv, TILEDB_READ, timestamp);
  } else {
    rc = tiledb_kv_open_at_with_key(
        ctx_,
        kv,
        TILEDB_READ,
        encryption_type_,
        encryption_key_,
        (uint32_t)strlen(encryption_key_),
        timestamp);
  }
  REQUIRE(rc == TILEDB_OK);

  // Check timestamp
  rc = tiledb_kv_get_timestamp(ctx_, kv, &timestamp_get);
  CHECK(rc == TILEDB_OK);
  CHECK(timestamp_get == timestamp);

  // Check if key exists
  rc = tiledb_kv_has_key(ctx_, kv, key, key_type, key_size, &has_key);
  CHECK(rc == TILEDB_OK);
  CHECK(has_key == 1);

  // Get key-value item
  rc = tiledb_kv_get_item(ctx_, kv, key, key_type, key_size, &read_item);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_kv_item_get_value(
      ctx_, read_item, "a", &read_v, &read_v_type, &read_v_size);
  CHECK(rc == TILEDB_OK);
  CHECK(read_v_type == TILEDB_INT32);
  CHECK(read_v_size == sizeof(int));
  CHECK(*(const int*)read_v == 10);

  // Clean up but don't close the KV yet.
  tiledb_kv_item_free(&read_item);

  // --- REOPEN AT FIRST TIMESTAMP ---
  // Reopen key-value store
  rc = tiledb_kv_reopen_at(ctx_, kv, first_timestamp);
  REQUIRE(rc == TILEDB_OK);

  // Check timestamp
  rc = tiledb_kv_get_timestamp(ctx_, kv, &timestamp_get);
  CHECK(rc == TILEDB_OK);
  CHECK(timestamp_get == first_timestamp);

  // Check if key exists
  rc = tiledb_kv_has_key(ctx_, kv, key, key_type, key_size, &has_key);
  CHECK(rc == TILEDB_OK);
  CHECK(has_key == 1);

  // Get key-value item
  rc = tiledb_kv_get_item(ctx_, kv, key, key_type, key_size, &read_item);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_kv_item_get_value(
      ctx_, read_item, "a", &read_v, &read_v_type, &read_v_size);
  CHECK(rc == TILEDB_OK);
  CHECK(read_v_type == TILEDB_INT32);
  CHECK(read_v_size == sizeof(int));
  CHECK(*(const int*)read_v == 10);

  // Close key-value store
  rc = tiledb_kv_close(ctx_, kv);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  tiledb_kv_free(&kv);
  tiledb_kv_item_free(&read_item);

  remove_temp_dir(temp_dir);
}
