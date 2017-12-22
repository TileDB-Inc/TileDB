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
#include "posix_filesystem.h"
#include "tiledb.h"

#include <cassert>
#include <iostream>

// Constant parameters
const char* ATTR_1 = "a1";
const char* ATTR_2 = "a2";
const char* ATTR_3 = "a3";
const char* KV_NAME = "kv";
int KEY1 = 100;
const int KEY1_A1 = 1;
const char* KEY1_A2 = "a";
const float KEY1_A3[] = {1.1, 1.2};
const float KEY2 = 200.0;
const int KEY2_A1 = 2;
const char* KEY2_A2 = "bb";
const float KEY2_A3[] = {2.1, 2.2};
const double KEY3[] = {300.0, 300.1};
const int KEY3_A1 = 3;
const char* KEY3_A2 = "ccc";
const float KEY3_A3[] = {3.1, 3.2};
const char KEY4[] = "key_4";
const int KEY4_A1 = 4;
const char* KEY4_A2 = "dddd";
const float KEY4_A3[] = {4.1, 4.2};

struct KVFx {
// Group folder name
#ifdef HAVE_HDFS
  const std::string URI_PREFIX = "hdfs://";
  const std::string TEMP_DIR = "/tiledb_test/";
#else
  const std::string URI_PREFIX = "file://";
  const std::string TEMP_DIR = tiledb::posix::current_dir() + "/";
#endif
  const std::string GROUP = "my_group/";

  // Key-value name
  std::string kv_name_;

  // TileDB context
  tiledb_ctx_t* ctx_;

  KVFx() {
    // Initialize context
    int rc = tiledb_ctx_create(&ctx_);
    if (rc != TILEDB_OK) {
      std::cerr << "KVFx() Error creating tiledb_ctx_t" << std::endl;
      std::exit(1);
    }
    // Create group, delete it if it already exists
    if (dir_exists(TEMP_DIR + GROUP)) {
      bool success = remove_dir(TEMP_DIR + GROUP);
      if (!success) {
        tiledb_ctx_free(ctx_);
        std::cerr << "KVFx() Error deleting existing test group" << std::endl;
        std::exit(1);
      }
    }
    rc = tiledb_group_create(ctx_, (URI_PREFIX + TEMP_DIR + GROUP).c_str());
    if (rc != TILEDB_OK) {
      tiledb_ctx_free(ctx_);
      std::cerr << "KVFx() Error creating test group" << std::endl;
      std::exit(1);
    }

    set_kv_name();
  }

  ~KVFx() {
    // Finalize TileDB context
    tiledb_ctx_free(ctx_);

    // Remove the temporary group
    bool success = remove_dir(TEMP_DIR + GROUP);
    if (!success) {
      std::cerr << "KVFx() Error deleting test group" << std::endl;
      std::exit(1);
    }
  }

  bool dir_exists(std::string path) {
#ifdef HAVE_HDFS
    std::string cmd = std::string("hadoop fs -test -d ") + path;
#else
    std::string cmd = std::string("test -d ") + path;
#endif
    return (system(cmd.c_str()) == 0);
  }

  bool remove_dir(std::string path) {
#ifdef HAVE_HDFS
    std::string cmd = std::string("hadoop fs -rm -r -f ") + path;
#else
    std::string cmd = std::string("rm -r -f ") + path;
#endif
    return (system(cmd.c_str()) == 0);
  }

  /** Sets the key-value name for the current test. */
  void set_kv_name() {
    kv_name_ = URI_PREFIX + TEMP_DIR + GROUP + KV_NAME;
  }

  void create_kv() {
    int rc;

    // Create attributes
    tiledb_attribute_t* a1;
    rc = tiledb_attribute_create(ctx_, &a1, ATTR_1, TILEDB_INT32);
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

    // Create array metadata
    tiledb_array_metadata_t* array_metadata;
    rc = tiledb_array_metadata_create(ctx_, &array_metadata, kv_name_.c_str());
    rc = tiledb_array_metadata_add_attribute(ctx_, array_metadata, a1);
    rc = tiledb_array_metadata_add_attribute(ctx_, array_metadata, a2);
    rc = tiledb_array_metadata_add_attribute(ctx_, array_metadata, a3);

    // Set array as key-value
    rc = tiledb_array_metadata_set_as_kv(ctx_, array_metadata);
    CHECK(rc == TILEDB_OK);

    // Check array metadata
    rc = tiledb_array_metadata_check(ctx_, array_metadata);
    CHECK(rc == TILEDB_OK);

    // Check if array is defined as kv
    int as_kv;
    rc = tiledb_array_metadata_get_as_kv(ctx_, array_metadata, &as_kv);
    CHECK(rc == TILEDB_OK);
    CHECK(as_kv == 1);

    // Create key-value store
    rc = tiledb_array_create(ctx_, array_metadata);
    CHECK(rc == TILEDB_OK);

    // Clean up
    rc = tiledb_attribute_free(ctx_, a1);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_attribute_free(ctx_, a2);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_attribute_free(ctx_, a3);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_array_metadata_free(ctx_, array_metadata);
    CHECK(rc == TILEDB_OK);
  }

  void write_kv() {
    int rc;

    // Attributes and value sizes
    const char* attributes[] = {ATTR_1, ATTR_2, ATTR_3};
    tiledb_datatype_t types[] = {TILEDB_INT32, TILEDB_CHAR, TILEDB_FLOAT32};
    unsigned int nitems[] = {1, tiledb_var_num(), 2};

    // Create key-values
    tiledb_kv_t* kv;
    rc = tiledb_kv_create(ctx_, &kv, 3, attributes, types, nitems);
    CHECK(rc == TILEDB_OK);

    // Add keys
    rc = tiledb_kv_add_key(ctx_, kv, &KEY1, TILEDB_INT32, sizeof(KEY1));
    CHECK(rc == TILEDB_OK);
    rc = tiledb_kv_add_key(ctx_, kv, &KEY2, TILEDB_FLOAT32, sizeof(KEY2));
    CHECK(rc == TILEDB_OK);
    rc = tiledb_kv_add_key(ctx_, kv, KEY3, TILEDB_FLOAT64, sizeof(KEY3));
    CHECK(rc == TILEDB_OK);
    rc = tiledb_kv_add_key(ctx_, kv, KEY4, TILEDB_CHAR, strlen(KEY4));
    CHECK(rc == TILEDB_OK);

    // Add attribute "a1" values
    rc = tiledb_kv_add_value(ctx_, kv, 0, &KEY1_A1);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_kv_add_value(ctx_, kv, 0, &KEY2_A1);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_kv_add_value(ctx_, kv, 0, &KEY3_A1);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_kv_add_value(ctx_, kv, 0, &KEY4_A1);
    CHECK(rc == TILEDB_OK);

    // Add attribute "a2" values
    rc = tiledb_kv_add_value_var(ctx_, kv, 1, KEY1_A2, strlen(KEY1_A2));
    CHECK(rc == TILEDB_OK);
    rc = tiledb_kv_add_value_var(ctx_, kv, 1, KEY2_A2, strlen(KEY2_A2));
    CHECK(rc == TILEDB_OK);
    rc = tiledb_kv_add_value_var(ctx_, kv, 1, KEY3_A2, strlen(KEY3_A2));
    CHECK(rc == TILEDB_OK);
    rc = tiledb_kv_add_value_var(ctx_, kv, 1, KEY4_A2, strlen(KEY4_A2));
    CHECK(rc == TILEDB_OK);

    // Add attribute "a3" values
    rc = tiledb_kv_add_value(ctx_, kv, 2, &KEY1_A3);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_kv_add_value(ctx_, kv, 2, &KEY2_A3);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_kv_add_value(ctx_, kv, 2, &KEY3_A3);
    CHECK(rc == TILEDB_OK);
    // One value is missing - rectified below

    // Create query
    tiledb_query_t* query;
    rc = tiledb_query_create(ctx_, &query, kv_name_.c_str(), TILEDB_WRITE);
    CHECK(rc == TILEDB_OK);

    // Check write mismatch and rectify
    rc = tiledb_query_set_kv(ctx_, query, kv);
    CHECK(rc == TILEDB_ERR);
    rc = tiledb_kv_add_value(ctx_, kv, 2, &KEY4_A3);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_query_set_kv(ctx_, query, kv);
    CHECK(rc == TILEDB_OK);

    // Submit query
    rc = tiledb_query_submit(ctx_, query);
    CHECK(rc == TILEDB_OK);

    // Clean up
    rc = tiledb_query_free(ctx_, query);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_kv_free(ctx_, kv);
    CHECK(rc == TILEDB_OK);
  }
};

TEST_CASE_METHOD(KVFx, "C API: Test key-value; Single-key read", "[kv]") {
  int rc;

  // Create key-value store
  create_kv();
  write_kv();

  // Set attributes
  const char* attributes[] = {ATTR_1, ATTR_2, ATTR_3};
  tiledb_datatype_t types[] = {TILEDB_INT32, TILEDB_CHAR, TILEDB_FLOAT32};
  unsigned int nitems[] = {1, tiledb_var_num(), 2};

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
  tiledb_datatype_t key4_type = TILEDB_CHAR;
  uint64_t key4_size = strlen(key4);

  // Create key-values #1
  tiledb_kv_t* kv_1;
  rc = tiledb_kv_create(ctx_, &kv_1, 3, attributes, types, nitems);
  CHECK(rc == TILEDB_OK);

  // Query #1
  tiledb_query_t* query_1;
  rc = tiledb_query_create(ctx_, &query_1, kv_name_.c_str(), TILEDB_READ);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_kv_key(ctx_, query_1, &key1, key1_type, key1_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_kv(ctx_, query_1, kv_1);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query_1);
  CHECK(rc == TILEDB_OK);

  // Key is not retrieved
  void* key_r;
  uint64_t key_size_r;
  tiledb_datatype_t key_type_r;
  rc = tiledb_kv_get_key(ctx_, kv_1, 0, &key_r, &key_type_r, &key_size_r);
  CHECK(rc == TILEDB_ERR);

  // Check result
  void *a1, *a2, *a3;
  uint64_t a2_size;
  rc = tiledb_kv_get_value(ctx_, kv_1, 0, 0, &a1);
  CHECK(rc == TILEDB_OK);
  CHECK((*(int*)a1) == KEY1_A1);
  rc = tiledb_kv_get_value_var(ctx_, kv_1, 0, 1, &a2, &a2_size);
  CHECK(rc == TILEDB_OK);
  CHECK(a2_size == 1 * sizeof(char));
  CHECK(!memcmp(a2, KEY1_A2, a2_size));
  rc = tiledb_kv_get_value(ctx_, kv_1, 0, 2, &a3);
  CHECK(rc == TILEDB_OK);
  CHECK(!memcmp(a3, &KEY1_A3, 2 * sizeof(float)));

  // Create key-values #2
  tiledb_kv_t* kv_2;
  rc = tiledb_kv_create(ctx_, &kv_2, 3, attributes, types, nitems);
  CHECK(rc == TILEDB_OK);

  // Query #2
  tiledb_query_t* query_2;
  rc = tiledb_query_create(ctx_, &query_2, kv_name_.c_str(), TILEDB_READ);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_kv_key(ctx_, query_2, &key2, key2_type, key2_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_kv(ctx_, query_2, kv_2);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query_2);
  CHECK(rc == TILEDB_OK);

  // Check result
  rc = tiledb_kv_get_value(ctx_, kv_2, 0, 0, &a1);
  CHECK(rc == TILEDB_OK);
  CHECK((*(int*)a1) == KEY2_A1);
  rc = tiledb_kv_get_value_var(ctx_, kv_2, 0, 1, &a2, &a2_size);
  CHECK(rc == TILEDB_OK);
  CHECK(a2_size == 2 * sizeof(char));
  CHECK(!memcmp(a2, KEY2_A2, a2_size));
  rc = tiledb_kv_get_value(ctx_, kv_2, 0, 2, &a3);
  CHECK(rc == TILEDB_OK);
  CHECK(!memcmp(a3, &KEY2_A3, 2 * sizeof(float)));

  // Create key-values #3
  tiledb_kv_t* kv_3;
  rc = tiledb_kv_create(ctx_, &kv_3, 3, attributes, types, nitems);
  CHECK(rc == TILEDB_OK);

  // Query #3
  tiledb_query_t* query_3;
  rc = tiledb_query_create(ctx_, &query_3, kv_name_.c_str(), TILEDB_READ);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_kv_key(ctx_, query_3, key3, key3_type, key3_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_kv(ctx_, query_3, kv_3);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query_3);
  CHECK(rc == TILEDB_OK);

  // Check result
  rc = tiledb_kv_get_value(ctx_, kv_3, 0, 0, &a1);
  CHECK(rc == TILEDB_OK);
  CHECK((*(int*)a1) == KEY3_A1);
  rc = tiledb_kv_get_value_var(ctx_, kv_3, 0, 1, &a2, &a2_size);
  CHECK(rc == TILEDB_OK);
  CHECK(a2_size == 3 * sizeof(char));
  CHECK(!memcmp(a2, KEY3_A2, a2_size));
  rc = tiledb_kv_get_value(ctx_, kv_3, 0, 2, &a3);
  CHECK(rc == TILEDB_OK);
  CHECK(!memcmp(a3, KEY3_A3, 2 * sizeof(float)));

  // Create key-values #4
  tiledb_kv_t* kv_4;
  rc = tiledb_kv_create(ctx_, &kv_4, 3, attributes, types, nitems);
  CHECK(rc == TILEDB_OK);

  // Query #4
  tiledb_query_t* query_4;
  rc = tiledb_query_create(ctx_, &query_4, kv_name_.c_str(), TILEDB_READ);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_kv_key(ctx_, query_4, key4, key4_type, key4_size);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_kv(ctx_, query_4, kv_4);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query_4);
  CHECK(rc == TILEDB_OK);

  // Check result
  rc = tiledb_kv_get_value(ctx_, kv_4, 0, 0, &a1);
  CHECK(rc == TILEDB_OK);
  CHECK((*(int*)a1) == KEY4_A1);
  rc = tiledb_kv_get_value_var(ctx_, kv_4, 0, 1, &a2, &a2_size);
  CHECK(rc == TILEDB_OK);
  CHECK(a2_size == 4 * sizeof(char));
  CHECK(!memcmp(a2, KEY4_A2, a2_size));
  rc = tiledb_kv_get_value(ctx_, kv_4, 0, 2, &a3);
  CHECK(rc == TILEDB_OK);
  CHECK(!memcmp(a3, &KEY4_A3, 2 * sizeof(float)));

  // Check that we can consolidate kv-arrays
  rc = tiledb_array_consolidate(ctx_, kv_name_.c_str());
  CHECK(rc == TILEDB_OK);

  // Create key-values #5
  tiledb_kv_t* kv_5;
  rc = tiledb_kv_create(ctx_, &kv_5, 3, attributes, types, nitems);
  CHECK(rc == TILEDB_OK);

  // Query #5 - Key does not exist
  tiledb_query_t* query_5;
  const char* key5 = "invalid";
  rc = tiledb_query_create(ctx_, &query_5, kv_name_.c_str(), TILEDB_READ);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_kv_key(ctx_, query_5, key5, TILEDB_CHAR, strlen(key5));
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_set_kv(ctx_, query_5, kv_5);
  CHECK(rc == TILEDB_OK);

  // Submit query
  rc = tiledb_query_submit(ctx_, query_5);
  CHECK(rc == TILEDB_OK);

  // Get number of values
  uint64_t num;
  rc = tiledb_kv_get_value_num(ctx_, kv_5, 0, &num);
  CHECK(rc == TILEDB_OK);
  CHECK(num == 0);

  // Clean up
  rc = tiledb_kv_free(ctx_, kv_1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_free(ctx_, kv_2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_free(ctx_, kv_3);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_free(ctx_, kv_4);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_free(ctx_, kv_5);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_free(ctx_, query_1);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_free(ctx_, query_2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_free(ctx_, query_3);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_free(ctx_, query_4);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_query_free(ctx_, query_5);
  CHECK(rc == TILEDB_OK);
}

TEST_CASE_METHOD(KVFx, "C API: Test key-value; Read all", "[kv]") {
  int rc;

  // Create key-value store
  create_kv();
  write_kv();

  // Set attributes
  const char* attributes[] = {ATTR_1, ATTR_2, ATTR_3};
  tiledb_datatype_t types[] = {TILEDB_INT32, TILEDB_CHAR, TILEDB_FLOAT32};
  unsigned int nitems[] = {1, TILEDB_VAR_NUM, 2};

  // Create key-values
  tiledb_kv_t* kv;
  rc = tiledb_kv_create(ctx_, &kv, 3, attributes, types, nitems);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_kv_set_buffer_size(ctx_, kv, 1000);
  CHECK(rc == TILEDB_OK);

  // Create query
  tiledb_query_t* query;
  rc = tiledb_query_create(ctx_, &query, kv_name_.c_str(), TILEDB_READ);
  CHECK(rc == TILEDB_OK);
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
  int order[4];
  void* key;
  uint64_t key_size;
  tiledb_datatype_t key_type;
  for (int i = 0; i < 4; ++i) {
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
}
