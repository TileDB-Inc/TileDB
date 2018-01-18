/**
 * @file   unit-capi-kv.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB Inc.
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
#include "win_filesystem.h"
#else
#include "posix_filesystem.h"
#endif
#include "tiledb.h"
#include "utils.h"

#include <thread>

struct KVFx {
  const char* ATTR_1 = "a1";
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

#ifdef HAVE_HDFS
  const std::string HDFS_TEMP_DIR = "hdfs:///tiledb_test/";
#endif
#ifdef HAVE_S3
  const std::string S3_PREFIX = "s3://";
  const std::string S3_BUCKET = S3_PREFIX + random_bucket_name("tiledb") + "/";
  const std::string S3_TEMP_DIR = S3_BUCKET + "tiledb_test/";
#endif
#ifdef _WIN32
  const std::string FILE_URI_PREFIX = "";
  const std::string FILE_TEMP_DIR =
      tiledb::win::current_dir() + "\\tiledb_test\\";
#else
  const std::string FILE_URI_PREFIX = "file://";
  const std::string FILE_TEMP_DIR =
      tiledb::posix::current_dir() + "/tiledb_test/";
#endif

  // TileDB context
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;

  // Functions
  KVFx();
  ~KVFx();
  void check_single_read(const std::string& path);
  void check_iter(const std::string& path);
  void check_kv_item();
  void check_interleaved_read_write(const std::string& path);
  void check_write(const std::string& path);
  void create_kv(const std::string& path);
  void create_temp_dir(const std::string& path);
  void remove_temp_dir(const std::string& path);
  static std::string random_bucket_name(const std::string& prefix);
};

KVFx::KVFx() {
  // Create TileDB context
  tiledb_config_t* config = nullptr;
  REQUIRE(tiledb_config_create(&config) == TILEDB_OK);
#ifdef HAVE_S3
  REQUIRE(
      tiledb_config_set(config, "vfs.s3.endpoint_override", "localhost:9999") ==
      TILEDB_OK);
#endif
  REQUIRE(tiledb_ctx_create(&ctx_, config) == TILEDB_OK);
  REQUIRE(tiledb_config_free(config) == TILEDB_OK);
  vfs_ = nullptr;
  REQUIRE(tiledb_vfs_create(ctx_, &vfs_, nullptr) == TILEDB_OK);

// Connect to S3
#ifdef HAVE_S3
  // Create bucket if it does not exist
  int is_bucket = 0;
  int rc = tiledb_vfs_is_bucket(ctx_, vfs_, S3_BUCKET.c_str(), &is_bucket);
  REQUIRE(rc == TILEDB_OK);
  if (!is_bucket) {
    rc = tiledb_vfs_create_bucket(ctx_, vfs_, S3_BUCKET.c_str());
    REQUIRE(rc == TILEDB_OK);
  }
#endif
}

KVFx::~KVFx() {
#ifdef HAVE_S3
  int is_bucket = 0;
  int rc = tiledb_vfs_is_bucket(ctx_, vfs_, S3_BUCKET.c_str(), &is_bucket);
  CHECK(rc == TILEDB_OK);
  if (is_bucket) {
    CHECK(tiledb_vfs_remove_bucket(ctx_, vfs_, S3_BUCKET.c_str()) == TILEDB_OK);
  }
#endif

  CHECK(tiledb_vfs_free(ctx_, vfs_) == TILEDB_OK);
  CHECK(tiledb_ctx_free(ctx_) == TILEDB_OK);
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
     << tiledb::utils::timestamp_ms();
  return ss.str();
}

void KVFx::create_kv(const std::string& path) {
  // Create attributes
  tiledb_attribute_t* a1;
  int rc = tiledb_attribute_create(ctx_, &a1, ATTR_1, TILEDB_INT32);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_compressor(ctx_, a1, TILEDB_BLOSC, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a1, 1);
  CHECK(rc == TILEDB_OK);
  tiledb_attribute_t* a2;
  rc = tiledb_attribute_create(ctx_, &a2, ATTR_2, TILEDB_CHAR);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_compressor(ctx_, a2, TILEDB_GZIP, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a2, TILEDB_VAR_NUM);
  CHECK(rc == TILEDB_OK);
  tiledb_attribute_t* a3;
  rc = tiledb_attribute_create(ctx_, &a3, ATTR_3, TILEDB_FLOAT32);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_compressor(ctx_, a3, TILEDB_ZSTD, -1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_set_cell_val_num(ctx_, a3, 2);
  CHECK(rc == TILEDB_OK);

  // Create key-value schema
  tiledb_kv_schema_t* kv_schema;
  rc = tiledb_kv_schema_create(ctx_, &kv_schema);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_schema_add_attribute(ctx_, kv_schema, a1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_schema_add_attribute(ctx_, kv_schema, a2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_schema_add_attribute(ctx_, kv_schema, a3);
  CHECK(rc == TILEDB_OK);

  // Check array schema
  rc = tiledb_kv_schema_check(ctx_, kv_schema);
  CHECK(rc == TILEDB_OK);

  // Create key-value store
  rc = tiledb_kv_create(ctx_, path.c_str(), kv_schema);
  CHECK(rc == TILEDB_OK);

  // Clean up
  rc = tiledb_attribute_free(ctx_, a1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_free(ctx_, a2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_attribute_free(ctx_, a3);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_schema_free(ctx_, kv_schema);
  CHECK(rc == TILEDB_OK);
}

void KVFx::check_kv_item() {
  tiledb_kv_item_t* kv_item;
  int rc = tiledb_kv_item_create(ctx_, &kv_item);
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

  rc = tiledb_kv_item_free(ctx_, kv_item);
  REQUIRE(rc == TILEDB_OK);
}

void KVFx::check_write(const std::string& path) {
  tiledb_kv_t* kv;
  int rc = tiledb_kv_open(ctx_, &kv, path.c_str(), nullptr, 0);
  REQUIRE(rc == TILEDB_OK);

  // Add key-value item #1
  tiledb_kv_item_t* kv_item1;
  rc = tiledb_kv_item_create(ctx_, &kv_item1);
  REQUIRE(rc == TILEDB_OK);
  rc =
      tiledb_kv_item_set_key(ctx_, kv_item1, &KEY1, TILEDB_INT32, sizeof(KEY1));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_value(
      ctx_, kv_item1, ATTR_1, &KEY1_A1, TILEDB_INT32, sizeof(int));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_value(
      ctx_, kv_item1, ATTR_2, KEY1_A2, TILEDB_CHAR, strlen(KEY1_A2) + 1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_value(
      ctx_, kv_item1, ATTR_3, KEY1_A3, TILEDB_FLOAT32, 2 * sizeof(float));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_add_item(ctx_, kv, kv_item1);

  // Add key-value item #2
  tiledb_kv_item_t* kv_item2;
  rc = tiledb_kv_item_create(ctx_, &kv_item2);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_key(
      ctx_, kv_item2, &KEY2, TILEDB_FLOAT32, sizeof(KEY2));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_value(
      ctx_, kv_item2, ATTR_1, &KEY2_A1, TILEDB_INT32, sizeof(int));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_value(
      ctx_, kv_item2, ATTR_2, KEY2_A2, TILEDB_CHAR, strlen(KEY2_A2) + 1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_value(
      ctx_, kv_item2, ATTR_3, KEY2_A3, TILEDB_FLOAT32, 2 * sizeof(float));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_add_item(ctx_, kv, kv_item2);
  CHECK(rc == TILEDB_OK);

  // Flush
  rc = tiledb_kv_flush(ctx_, kv);
  REQUIRE(rc == TILEDB_OK);

  // Add key-value item #3
  tiledb_kv_item_t* kv_item3;
  rc = tiledb_kv_item_create(ctx_, &kv_item3);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_key(
      ctx_, kv_item3, KEY3, TILEDB_FLOAT64, sizeof(KEY3));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_value(
      ctx_, kv_item3, ATTR_1, &KEY3_A1, TILEDB_INT32, sizeof(int));
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
  rc = tiledb_kv_item_create(ctx_, &kv_item4);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_key(ctx_, kv_item4, KEY4, TILEDB_CHAR, strlen(KEY4));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_value(
      ctx_, kv_item4, ATTR_1, &KEY4_A1, TILEDB_INT32, sizeof(int));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_value(
      ctx_, kv_item4, ATTR_2, KEY4_A2, TILEDB_CHAR, strlen(KEY4_A2) + 1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_value(
      ctx_, kv_item4, ATTR_3, KEY4_A3, TILEDB_FLOAT32, 2 * sizeof(float));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_add_item(ctx_, kv, kv_item4);
  CHECK(rc == TILEDB_OK);

  // Close kv
  rc = tiledb_kv_close(ctx_, kv);
  REQUIRE(rc == TILEDB_OK);

  // Consolidate
  rc = tiledb_kv_consolidate(ctx_, path.c_str());
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  rc = tiledb_kv_item_free(ctx_, kv_item1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_kv_item_free(ctx_, kv_item2);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_kv_item_free(ctx_, kv_item3);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_kv_item_free(ctx_, kv_item4);
  REQUIRE(rc == TILEDB_OK);
}

void KVFx::check_single_read(const std::string& path) {
  // Open key-value store
  const char* attributes[] = {ATTR_1, ATTR_2, ATTR_3};
  tiledb_kv_t* kv;
  int rc = tiledb_kv_open(ctx_, &kv, path.c_str(), attributes, 3);
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
  uint64_t key4_size = strlen(key4);

  // Get key-value item #1
  tiledb_kv_item_t* kv_item1;
  rc = tiledb_kv_get_item(ctx_, kv, &kv_item1, &key1, key1_type, key1_size);
  REQUIRE(rc == TILEDB_OK);
  rc =
      tiledb_kv_item_get_value(ctx_, kv_item1, ATTR_1, &a1, &a1_type, &a1_size);
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

  // Get key-value item #2
  tiledb_kv_item_t* kv_item2;
  rc = tiledb_kv_get_item(ctx_, kv, &kv_item2, &key2, key2_type, key2_size);
  REQUIRE(rc == TILEDB_OK);
  rc =
      tiledb_kv_item_get_value(ctx_, kv_item2, ATTR_1, &a1, &a1_type, &a1_size);
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
  rc = tiledb_kv_get_item(ctx_, kv, &kv_item3, key3, key3_type, key3_size);
  REQUIRE(rc == TILEDB_OK);
  rc =
      tiledb_kv_item_get_value(ctx_, kv_item3, ATTR_1, &a1, &a1_type, &a1_size);
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
  rc = tiledb_kv_get_item(ctx_, kv, &kv_item4, key4, key4_type, key4_size);
  REQUIRE(rc == TILEDB_OK);
  rc =
      tiledb_kv_item_get_value(ctx_, kv_item4, ATTR_1, &a1, &a1_type, &a1_size);
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
  rc = tiledb_kv_get_item(ctx_, kv, &kv_item5, key5, key4_type, key4_size);
  REQUIRE(rc == TILEDB_OK);
  CHECK(kv_item5 == nullptr);

  // Close key-value store
  rc = tiledb_kv_close(ctx_, kv);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  rc = tiledb_kv_item_free(ctx_, kv_item1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_kv_item_free(ctx_, kv_item2);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_kv_item_free(ctx_, kv_item3);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_kv_item_free(ctx_, kv_item4);
  REQUIRE(rc == TILEDB_OK);
}

void KVFx::check_interleaved_read_write(const std::string& path) {
  // Open the key-value store
  tiledb_kv_t* kv;
  int rc = tiledb_kv_open(ctx_, &kv, path.c_str(), nullptr, 0);
  REQUIRE(rc == TILEDB_OK);

  // Add an item
  int new_key = 123;
  tiledb_kv_item_t* kv_item1;
  rc = tiledb_kv_item_create(ctx_, &kv_item1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_key(
      ctx_, kv_item1, &new_key, TILEDB_INT32, sizeof(int));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_value(
      ctx_, kv_item1, ATTR_1, &KEY1_A1, TILEDB_INT32, sizeof(int));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_value(
      ctx_, kv_item1, ATTR_2, KEY1_A2, TILEDB_CHAR, strlen(KEY1_A2) + 1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_item_set_value(
      ctx_, kv_item1, ATTR_3, KEY1_A3, TILEDB_FLOAT32, 2 * sizeof(float));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_add_item(ctx_, kv, kv_item1);
  CHECK(rc == TILEDB_OK);

  // Read the new item
  tiledb_kv_item_t* kv_item2;
  const void *a1, *a2, *a3;
  tiledb_datatype_t a1_type, a2_type, a3_type;
  uint64_t a1_size, a2_size, a3_size;
  rc = tiledb_kv_get_item(
      ctx_, kv, &kv_item2, &new_key, TILEDB_INT32, sizeof(int));
  REQUIRE(rc == TILEDB_OK);
  rc =
      tiledb_kv_item_get_value(ctx_, kv_item1, ATTR_1, &a1, &a1_type, &a1_size);
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

  // Flush
  rc = tiledb_kv_flush(ctx_, kv);
  REQUIRE(rc == TILEDB_OK);

  // Read the new item again
  tiledb_kv_item_t* kv_item3;
  rc = tiledb_kv_get_item(
      ctx_, kv, &kv_item3, &new_key, TILEDB_INT32, sizeof(int));
  REQUIRE(rc == TILEDB_OK);
  rc =
      tiledb_kv_item_get_value(ctx_, kv_item3, ATTR_1, &a1, &a1_type, &a1_size);
  CHECK(rc == TILEDB_OK);
  CHECK(*(int*)a1 == KEY1_A1);
  CHECK(a1_type == TILEDB_INT32);
  CHECK(a1_size == sizeof(int));
  rc =
      tiledb_kv_item_get_value(ctx_, kv_item3, ATTR_2, &a2, &a2_type, &a2_size);
  CHECK(rc == TILEDB_OK);
  CHECK(!strcmp((const char*)a2, KEY1_A2));
  CHECK(a2_type == TILEDB_CHAR);
  CHECK(a2_size == strlen(KEY1_A2) + 1);
  rc =
      tiledb_kv_item_get_value(ctx_, kv_item3, ATTR_3, &a3, &a3_type, &a3_size);
  CHECK(rc == TILEDB_OK);
  CHECK(!memcmp(a3, KEY1_A3, 2 * sizeof(float)));
  CHECK(a3_type == TILEDB_FLOAT32);
  CHECK(a3_size == 2 * sizeof(float));

  // Close key-value store
  rc = tiledb_kv_close(ctx_, kv);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  rc = tiledb_kv_item_free(ctx_, kv_item1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_kv_item_free(ctx_, kv_item2);
  REQUIRE(rc == TILEDB_OK);
}

void KVFx::check_iter(const std::string& path) {
  (void)path;
  /*
// Set attributes
const char* attributes[] = {ATTR_1, ATTR_2, ATTR_3};
tiledb_datatype_t types[] = {TILEDB_INT32, TILEDB_CHAR, TILEDB_FLOAT32};
unsigned int nitems[] = {1, TILEDB_VAR_NUM, 2};

// Create key-values
tiledb_kv_t* kv;
int rc = tiledb_kv_create(ctx_, &kv, 3, attributes, types, nitems);
CHECK(rc == TILEDB_OK);
rc = tiledb_kv_set_buffer_size(ctx_, kv, 1000);
CHECK(rc == TILEDB_OK);

// Create query
tiledb_query_t* query;
rc = tiledb_query_create(ctx_, &query, path.c_str(), TILEDB_READ);
REQUIRE(rc == TILEDB_OK);
rc = tiledb_query_set_kv(ctx_, query, kv);
CHECK(rc == TILEDB_OK);

// Submit query
rc = tiledb_query_submit(ctx_, query);
CHECK(rc == TILEDB_OK);

// Check the number of results
uint64_t key_num, a1_num, a2_num, a3_num;
rc = tiledb_kv_get_key_num(ctx_, kv, &key_num);
CHECK(rc == TILEDB_OK);
CHECK(key_num == 4);
rc = tiledb_kv_get_value_num(ctx_, kv, 0, &a1_num);
CHECK(rc == TILEDB_OK);
CHECK(a1_num == 4);
rc = tiledb_kv_get_value_num(ctx_, kv, 1, &a2_num);
CHECK(rc == TILEDB_OK);
CHECK(a2_num == 4);
rc = tiledb_kv_get_value_num(ctx_, kv, 2, &a3_num);
CHECK(rc == TILEDB_OK);
CHECK(a3_num == 4);

// Get the key-value order
uint64_t order[4] = {0, 1, 2, 3};
void* key;
uint64_t key_size;
tiledb_datatype_t key_type;
for (uint64_t i = 0; i < 4; ++i) {
  rc = tiledb_kv_get_key(ctx_, kv, i, &key, &key_type, &key_size);
  CHECK(rc == TILEDB_OK);
  if (key_type == TILEDB_INT32) {
    CHECK(key_size == sizeof(int));
    CHECK(!memcmp(key, &KEY1, key_size));
    order[0] = i;
  } else if (key_type == TILEDB_FLOAT32) {
    CHECK(key_size == sizeof(float));
    CHECK(!memcmp(key, &KEY2, key_size));
    order[1] = i;
  } else if (key_type == TILEDB_FLOAT64) {
    CHECK(key_size == 2 * sizeof(double));
    CHECK(!memcmp(key, KEY3, key_size));
    order[2] = i;
  } else if (key_type == TILEDB_CHAR) {
    CHECK(key_size == strlen(KEY4));
    CHECK(!memcmp(key, KEY4, key_size));
    order[3] = i;
  } else {
    CHECK(false);
  }
}

// Check the results
void *a1, *a2, *a3;
uint64_t a2_size;
rc = tiledb_kv_get_value(ctx_, kv, order[0], 0, &a1);
CHECK(rc == TILEDB_OK);
CHECK((*(int*)a1) == KEY1_A1);
rc = tiledb_kv_get_value_var(ctx_, kv, order[0], 1, &a2, &a2_size);
CHECK(rc == TILEDB_OK);
CHECK(a2_size == 1 * sizeof(char));
CHECK(!memcmp(a2, KEY1_A2, a2_size));
rc = tiledb_kv_get_value(ctx_, kv, order[0], 2, &a3);
CHECK(rc == TILEDB_OK);
CHECK(!memcmp(a3, &KEY1_A3, 2 * sizeof(float)));

rc = tiledb_kv_get_value(ctx_, kv, order[1], 0, &a1);
CHECK(rc == TILEDB_OK);
CHECK((*(int*)a1) == KEY2_A1);
rc = tiledb_kv_get_value_var(ctx_, kv, order[1], 1, &a2, &a2_size);
CHECK(rc == TILEDB_OK);
CHECK(a2_size == 2 * sizeof(char));
CHECK(!memcmp(a2, KEY2_A2, a2_size));
rc = tiledb_kv_get_value(ctx_, kv, order[1], 2, &a3);
CHECK(rc == TILEDB_OK);
CHECK(!memcmp(a3, &KEY2_A3, 2 * sizeof(float)));

rc = tiledb_kv_get_value(ctx_, kv, order[2], 0, &a1);
CHECK(rc == TILEDB_OK);
CHECK((*(int*)a1) == KEY3_A1);
rc = tiledb_kv_get_value_var(ctx_, kv, order[2], 1, &a2, &a2_size);
CHECK(rc == TILEDB_OK);
CHECK(a2_size == 3 * sizeof(char));
CHECK(!memcmp(a2, KEY3_A2, a2_size));
rc = tiledb_kv_get_value(ctx_, kv, order[2], 2, &a3);
CHECK(rc == TILEDB_OK);
CHECK(!memcmp(a3, &KEY3_A3, 2 * sizeof(float)));

rc = tiledb_kv_get_value(ctx_, kv, order[3], 0, &a1);
CHECK(rc == TILEDB_OK);
CHECK((*(int*)a1) == KEY4_A1);
rc = tiledb_kv_get_value_var(ctx_, kv, order[3], 1, &a2, &a2_size);
CHECK(rc == TILEDB_OK);
CHECK(a2_size == 4 * sizeof(char));
CHECK(!memcmp(a2, KEY4_A2, a2_size));
rc = tiledb_kv_get_value(ctx_, kv, order[3], 2, &a3);
CHECK(rc == TILEDB_OK);
CHECK(!memcmp(a3, &KEY4_A3, 2 * sizeof(float)));

// Clean up
rc = tiledb_kv_free(ctx_, kv);
CHECK(rc == TILEDB_OK);
rc = tiledb_query_free(ctx_, query);
CHECK(rc == TILEDB_OK);
   */
}

TEST_CASE_METHOD(KVFx, "C API: Test key-value", "[capi], [kv]") {
  // File
  create_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);

  // create & write
  std::string array_name = FILE_URI_PREFIX + FILE_TEMP_DIR + KV_NAME;
  create_kv(array_name);
  check_kv_item();
  check_write(array_name);
  check_single_read(array_name);
  check_interleaved_read_write(array_name);
  check_iter(array_name);

  remove_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);

#ifdef HAVE_S3
  create_temp_dir(S3_TEMP_DIR);

  array_name = S3_TEMP_DIR + KV_NAME;
  create_kv(array_name);
  check_write(array_name);
  check_single_read(array_name);
  check_interleaved_read_write(array_name);
  check_iter(array_name);

  remove_temp_dir(S3_TEMP_DIR);
#endif

#ifdef HAVE_HDFS
  create_temp_dir(HDFS_TEMP_DIR);

  array_name = HDFS_TEMP_DIR + KV_NAME;
  create_kv(array_name);
  check_write(array_name);
  check_single_read(array_name);
  check_interleaved_read_write(array_name);
  check_iter(array_name);

  remove_temp_dir(HDFS_TEMP_DIR);
#endif
}
