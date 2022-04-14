/**
 * @file   unit-capi-group.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB Inc.
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
 * Tests for the C API object management code.
 */

#include "catch.hpp"
#include "test/src/helpers.h"
#include "test/src/serialization_wrappers.h"
#include "test/src/vfs_helpers.h"
#ifdef _WIN32
#include "tiledb/sm/filesystem/path_win.h"
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif

#include "serialization_wrappers.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/global_state/unit_test_config.h"
#include "tiledb/sm/misc/utils.h"

#include <iostream>
#include <sstream>
#include <thread>

using namespace tiledb::test;

struct GroupFx {
  const std::string GROUP = "group/";
  const std::string ARRAY = "array/";

  // TileDB context
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;

  // Vector of supported filsystems
  const std::vector<std::unique_ptr<SupportedFs>> fs_vec_;

  /**
   * If true, array schema is serialized before submission, to test the
   * serialization paths.
   */
  bool serialize_ = false;

  const char* key_ = "0123456789abcdeF0123456789abcdeF";
  const uint32_t key_len_ =
      (uint32_t)strlen("0123456789abcdeF0123456789abcdeF");
  const tiledb_encryption_type_t enc_type_ = TILEDB_AES_256_GCM;

  // Functions
  GroupFx();
  ~GroupFx();
  void create_array(const std::string& path);
  void create_temp_dir(const std::string& path) const;
  void remove_temp_dir(const std::string& path) const;
  std::string get_golden_walk(const std::string& path);
  std::string get_golden_ls(const std::string& path);
  static std::string random_name(const std::string& prefix);
  std::vector<std::pair<tiledb::sm::URI, tiledb_object_t>> read_group(
      tiledb_group_t* group) const;
  void set_group_timestamp(
      tiledb_group_t* group, const uint64_t& timestamp) const;
};

GroupFx::GroupFx()
    : fs_vec_(vfs_test_get_fs_vec()) {
  // Initialize vfs test
  ctx_ = nullptr;
  vfs_ = nullptr;
  REQUIRE(vfs_test_init(fs_vec_, &ctx_, &vfs_).ok());
}

GroupFx::~GroupFx() {
  // Close vfs test
  REQUIRE(vfs_test_close(fs_vec_, ctx_, vfs_).ok());
  tiledb_vfs_free(&vfs_);
  tiledb_ctx_free(&ctx_);
}

void GroupFx::set_group_timestamp(
    tiledb_group_t* group, const uint64_t& timestamp) const {
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);
  int rc = tiledb_config_set(
      config,
      "sm.group.timestamp_end",
      std::to_string(timestamp).c_str(),
      &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);

  rc = tiledb_group_set_config(ctx_, group, config);
  REQUIRE(rc == TILEDB_OK);

  tiledb_config_free(&config);
}

std::vector<std::pair<tiledb::sm::URI, tiledb_object_t>> GroupFx::read_group(
    tiledb_group_t* group) const {
  std::vector<std::pair<tiledb::sm::URI, tiledb_object_t>> ret;
  uint64_t count = 0;
  char* uri;
  tiledb_object_t type;
  char* name;
  int rc = tiledb_group_get_member_count(ctx_, group, &count);
  REQUIRE(rc == TILEDB_OK);
  for (uint64_t i = 0; i < count; i++) {
    rc = tiledb_group_get_member_by_index(ctx_, group, i, &uri, &type, &name);
    REQUIRE(rc == TILEDB_OK);
    ret.emplace_back(uri, type);
    std::free(uri);
    std::free(name);
  }
  return ret;
}

void GroupFx::create_temp_dir(const std::string& path) const {
  remove_temp_dir(path);
  REQUIRE(tiledb_vfs_create_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

void GroupFx::remove_temp_dir(const std::string& path) const {
  int is_dir = 0;
  REQUIRE(tiledb_vfs_is_dir(ctx_, vfs_, path.c_str(), &is_dir) == TILEDB_OK);
  if (is_dir)
    REQUIRE(tiledb_vfs_remove_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

void GroupFx::create_array(const std::string& path) {
  tiledb_attribute_t* a1;
  tiledb_attribute_alloc(ctx_, "a1", TILEDB_FLOAT32, &a1);

  // Domain and tile extents
  int64_t dim_domain[2] = {1, 1};
  int64_t tile_extents[] = {1};

  // Create dimension
  tiledb_dimension_t* d1;
  tiledb_dimension_alloc(
      ctx_, "d1", TILEDB_INT64, &dim_domain[0], &tile_extents[0], &d1);

  // Domain
  tiledb_domain_t* domain;
  tiledb_domain_alloc(ctx_, &domain);
  tiledb_domain_add_dimension(ctx_, domain, d1);

  // Create array schema
  tiledb_array_schema_t* array_schema;
  tiledb_array_schema_alloc(ctx_, TILEDB_DENSE, &array_schema);
  tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  tiledb_array_schema_add_attribute(ctx_, array_schema, a1);

  // Check array schema
  REQUIRE(tiledb_array_schema_check(ctx_, array_schema) == TILEDB_OK);

  // Create array
  REQUIRE(
      tiledb_array_create_serialization_wrapper(
          ctx_, path, array_schema, serialize_) == TILEDB_OK);

  // Free objects
  tiledb_attribute_free(&a1);
  tiledb_dimension_free(&d1);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

std::string GroupFx::get_golden_walk(const std::string& path) {
  std::string golden;

  // Preorder traversal
  golden += path + "dense_arrays GROUP\n";
  golden += path + "dense_arrays/array_A ARRAY\n";
  golden += path + "dense_arrays/array_B ARRAY\n";
  golden += path + "sparse_arrays GROUP\n";
  golden += path + "sparse_arrays/array_C ARRAY\n";
  golden += path + "sparse_arrays/array_D ARRAY\n";

  // Postorder traversal
  golden += path + "dense_arrays/array_A ARRAY\n";
  golden += path + "dense_arrays/array_B ARRAY\n";
  golden += path + "dense_arrays GROUP\n";
  golden += path + "sparse_arrays/array_C ARRAY\n";
  golden += path + "sparse_arrays/array_D ARRAY\n";
  golden += path + "sparse_arrays GROUP\n";

  return golden;
}

std::string GroupFx::get_golden_ls(const std::string& path) {
  std::string golden;

  golden += path + "dense_arrays GROUP\n";
  golden += path + "sparse_arrays GROUP\n";

  return golden;
}

std::string GroupFx::random_name(const std::string& prefix) {
  std::stringstream ss;
  ss << prefix << "-" << std::this_thread::get_id() << "-"
     << TILEDB_TIMESTAMP_NOW_MS;
  return ss.str();
}

TEST_CASE_METHOD(
    GroupFx, "C API: Test group metadata", "[capi][group][metadata]") {
  // TODO: refactor for each supported FS.
  std::string temp_dir = fs_vec_[0]->temp_dir();
  create_temp_dir(temp_dir);

  std::string group1_uri = temp_dir + "group1";
  REQUIRE(tiledb_group_create(ctx_, group1_uri.c_str()) == TILEDB_OK);
  tiledb_group_t* group;
  REQUIRE(tiledb_group_alloc(ctx_, group1_uri.c_str(), &group) == TILEDB_OK);

  // Put metadata on a group that is not opened
  int v = 5;
  int32_t rc =
      tiledb_group_put_metadata(ctx_, group, "key", TILEDB_INT32, 1, &v);
  CHECK(rc == TILEDB_ERR);

  // Write metadata on an group opened in READ mode
  set_group_timestamp(group, 1);
  rc = tiledb_group_open(ctx_, group, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_group_put_metadata(ctx_, group, "key", TILEDB_INT32, 1, &v);
  CHECK(rc == TILEDB_ERR);

  // Close group
  rc = tiledb_group_close(ctx_, group);
  REQUIRE(rc == TILEDB_OK);
  set_group_timestamp(group, 1);
  REQUIRE(tiledb_group_open(ctx_, group, TILEDB_WRITE) == TILEDB_OK);

  // Write null key
  rc = tiledb_group_put_metadata(ctx_, group, NULL, TILEDB_INT32, 1, &v);
  CHECK(rc == TILEDB_ERR);

  // Write value type BLOB
  rc = tiledb_group_put_metadata(ctx_, group, "key", TILEDB_ANY, 1, &v);
  CHECK(rc == TILEDB_ERR);

  // Write a correct item
  rc = tiledb_group_put_metadata(ctx_, group, "key", TILEDB_INT32, 1, &v);
  CHECK(rc == TILEDB_OK);

  // Close group
  rc = tiledb_group_close(ctx_, group);
  REQUIRE(rc == TILEDB_OK);

  // Clean up
  tiledb_group_free(&group);
  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    GroupFx,
    "C API: Group Metadata, write/read",
    "[capi][group][metadata][read]") {
  // Create and open group in write mode
  // TODO: refactor for each supported FS.
  std::string temp_dir = fs_vec_[0]->temp_dir();
  create_temp_dir(temp_dir);

  std::string group1_uri = temp_dir + "group1";
  REQUIRE(tiledb_group_create(ctx_, group1_uri.c_str()) == TILEDB_OK);

  tiledb_group_t* group;
  int rc = tiledb_group_alloc(ctx_, group1_uri.c_str(), &group);
  REQUIRE(rc == TILEDB_OK);
  set_group_timestamp(group, 1);
  rc = tiledb_group_open(ctx_, group, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  // Write items
  int32_t v = 5;
  rc = tiledb_group_put_metadata(ctx_, group, "aaa", TILEDB_INT32, 1, &v);
  CHECK(rc == TILEDB_OK);
  float f[] = {1.1f, 1.2f};
  rc = tiledb_group_put_metadata(ctx_, group, "bb", TILEDB_FLOAT32, 2, f);
  CHECK(rc == TILEDB_OK);

  // Close group
  rc = tiledb_group_close(ctx_, group);
  REQUIRE(rc == TILEDB_OK);
  tiledb_group_free(&group);

  // Open the group in read mode
  rc = tiledb_group_alloc(ctx_, group1_uri.c_str(), &group);
  REQUIRE(rc == TILEDB_OK);
  set_group_timestamp(group, 1);
  rc = tiledb_group_open(ctx_, group, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  // Read
  const void* v_r;
  tiledb_datatype_t v_type;
  uint32_t v_num;
  rc = tiledb_group_get_metadata(ctx_, group, "aaa", &v_type, &v_num, &v_r);
  CHECK(rc == TILEDB_OK);
  CHECK(v_type == TILEDB_INT32);
  CHECK(v_num == 1);
  CHECK(*((const int32_t*)v_r) == 5);

  rc = tiledb_group_get_metadata(ctx_, group, "bb", &v_type, &v_num, &v_r);
  CHECK(rc == TILEDB_OK);
  CHECK(v_type == TILEDB_FLOAT32);
  CHECK(v_num == 2);
  CHECK(((const float*)v_r)[0] == 1.1f);
  CHECK(((const float*)v_r)[1] == 1.2f);

  rc = tiledb_group_get_metadata(ctx_, group, "foo", &v_type, &v_num, &v_r);
  CHECK(rc == TILEDB_OK);
  CHECK(v_r == nullptr);

  uint64_t num = 0;
  rc = tiledb_group_get_metadata_num(ctx_, group, &num);
  CHECK(rc == TILEDB_OK);
  CHECK(num == 2);

  const char* key;
  uint32_t key_len;
  rc = tiledb_group_get_metadata_from_index(
      ctx_, group, 10, &key, &key_len, &v_type, &v_num, &v_r);
  CHECK(rc == TILEDB_ERR);

  rc = tiledb_group_get_metadata_from_index(
      ctx_, group, 1, &key, &key_len, &v_type, &v_num, &v_r);
  CHECK(rc == TILEDB_OK);
  CHECK(v_type == TILEDB_FLOAT32);
  CHECK(v_num == 2);
  CHECK(((const float*)v_r)[0] == 1.1f);
  CHECK(((const float*)v_r)[1] == 1.2f);
  CHECK(key_len == strlen("bb"));
  CHECK(!strncmp(key, "bb", strlen("bb")));

  // Check has_key
  int32_t has_key = 0;
  rc = tiledb_group_has_metadata_key(ctx_, group, "bb", &v_type, &has_key);
  CHECK(rc == TILEDB_OK);
  CHECK(v_type == TILEDB_FLOAT32);
  CHECK(has_key == 1);

  // Check not has_key
  v_type = (tiledb_datatype_t)std::numeric_limits<int32_t>::max();
  rc = tiledb_group_has_metadata_key(
      ctx_, group, "non-existent-key", &v_type, &has_key);
  CHECK(rc == TILEDB_OK);
  // The API does not touch v_type when no key is found.
  CHECK((int32_t)v_type == std::numeric_limits<int32_t>::max());
  CHECK(has_key == 0);

  // Close group
  rc = tiledb_group_close(ctx_, group);
  REQUIRE(rc == TILEDB_OK);
  tiledb_group_free(&group);
  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    GroupFx, "C API: Group, write/read", "[capi][group][metadata][read]") {
  // Create and open group in write mode
  // TODO: refactor for each supported FS.
  std::string temp_dir = fs_vec_[0]->temp_dir();
  create_temp_dir(temp_dir);

  const tiledb::sm::URI array1_uri(temp_dir + "array1");
  const tiledb::sm::URI array2_uri(temp_dir + "array2");
  const tiledb::sm::URI array3_uri(temp_dir + "array3");
  create_array(array1_uri.to_string());
  create_array(array2_uri.to_string());
  create_array(array3_uri.to_string());

  tiledb::sm::URI group1_uri(temp_dir + "group1");
  REQUIRE(tiledb_group_create(ctx_, group1_uri.c_str()) == TILEDB_OK);

  tiledb::sm::URI group2_uri(temp_dir + "group2");
  REQUIRE(tiledb_group_create(ctx_, group2_uri.c_str()) == TILEDB_OK);

  // Set expected
  std::vector<std::pair<tiledb::sm::URI, tiledb_object_t>> group1_expected = {
      {array1_uri, TILEDB_ARRAY},
      {array2_uri, TILEDB_ARRAY},
      {group2_uri, TILEDB_GROUP},
  };
  std::vector<std::pair<tiledb::sm::URI, tiledb_object_t>> group2_expected = {
      {array3_uri, TILEDB_ARRAY},
  };

  tiledb_group_t* group1;
  int rc = tiledb_group_alloc(ctx_, group1_uri.c_str(), &group1);
  REQUIRE(rc == TILEDB_OK);
  set_group_timestamp(group1, 1);
  rc = tiledb_group_open(ctx_, group1, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  tiledb_group_t* group2;
  rc = tiledb_group_alloc(ctx_, group2_uri.c_str(), &group2);
  REQUIRE(rc == TILEDB_OK);
  set_group_timestamp(group2, 1);
  rc = tiledb_group_open(ctx_, group2, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  rc =
      tiledb_group_add_member(ctx_, group1, array1_uri.c_str(), false, nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc =
      tiledb_group_add_member(ctx_, group1, array2_uri.c_str(), false, nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc =
      tiledb_group_add_member(ctx_, group2, array3_uri.c_str(), false, nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc =
      tiledb_group_add_member(ctx_, group1, group2_uri.c_str(), false, nullptr);
  REQUIRE(rc == TILEDB_OK);

  // Close group from write mode
  rc = tiledb_group_close(ctx_, group1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_group_close(ctx_, group2);
  REQUIRE(rc == TILEDB_OK);

  // Reopen in read mode
  rc = tiledb_group_open(ctx_, group1, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_group_open(ctx_, group2, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  std::vector<std::pair<tiledb::sm::URI, tiledb_object_t>> group1_received =
      read_group(group1);
  REQUIRE_THAT(
      group1_received, Catch::Matchers::UnorderedEquals(group1_expected));

  std::vector<std::pair<tiledb::sm::URI, tiledb_object_t>> group2_received =
      read_group(group2);
  REQUIRE_THAT(
      group2_received, Catch::Matchers::UnorderedEquals(group2_expected));

  // Close group
  rc = tiledb_group_close(ctx_, group1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_group_close(ctx_, group2);
  REQUIRE(rc == TILEDB_OK);

  // Remove assets from group
  set_group_timestamp(group1, 2);
  rc = tiledb_group_open(ctx_, group1, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  set_group_timestamp(group2, 2);
  rc = tiledb_group_open(ctx_, group2, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_group_remove_member(ctx_, group1, group2_uri.c_str());
  REQUIRE(rc == TILEDB_OK);
  // Group is the latest element
  group1_expected.resize(group1_expected.size() - 1);

  rc = tiledb_group_remove_member(ctx_, group2, array3_uri.c_str());
  REQUIRE(rc == TILEDB_OK);
  // There should be nothing left in group2
  group2_expected.clear();

  // Close group
  rc = tiledb_group_close(ctx_, group1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_group_close(ctx_, group2);
  REQUIRE(rc == TILEDB_OK);

  // Check read again
  set_group_timestamp(group1, 2);
  rc = tiledb_group_open(ctx_, group1, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  set_group_timestamp(group2, 2);
  rc = tiledb_group_open(ctx_, group2, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  group1_received = read_group(group1);
  REQUIRE_THAT(
      group1_received, Catch::Matchers::UnorderedEquals(group1_expected));

  group2_received = read_group(group2);
  REQUIRE_THAT(
      group2_received, Catch::Matchers::UnorderedEquals(group2_expected));

  // Close group
  rc = tiledb_group_close(ctx_, group1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_group_close(ctx_, group2);
  REQUIRE(rc == TILEDB_OK);
  tiledb_group_free(&group1);
  tiledb_group_free(&group2);
  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    GroupFx,
    "C API: Group, write/read, relative",
    "[capi][group][metadata][read]") {
  // Create and open group in write mode
  // TODO: refactor for each supported FS.
  std::string temp_dir = fs_vec_[0]->temp_dir();
  create_temp_dir(temp_dir);

  tiledb::sm::URI group1_uri(temp_dir + "group1");
  REQUIRE(tiledb_group_create(ctx_, group1_uri.c_str()) == TILEDB_OK);

  tiledb::sm::URI group2_uri(temp_dir + "group2");
  REQUIRE(tiledb_group_create(ctx_, group2_uri.c_str()) == TILEDB_OK);

  REQUIRE(
      tiledb_vfs_create_dir(ctx_, vfs_, (temp_dir + "group1/arrays").c_str()) ==
      TILEDB_OK);
  REQUIRE(
      tiledb_vfs_create_dir(ctx_, vfs_, (temp_dir + "group2/arrays").c_str()) ==
      TILEDB_OK);

  const std::string array1_relative_uri("arrays/array1");
  const tiledb::sm::URI array1_uri(temp_dir + "group1/arrays/array1");
  const std::string array2_relative_uri("arrays/array2");
  const tiledb::sm::URI array2_uri(temp_dir + "group1/arrays/array2");
  const std::string array3_relative_uri("arrays/array3");
  const tiledb::sm::URI array3_uri(temp_dir + "group2/arrays/array3");
  create_array(array1_uri.to_string());
  create_array(array2_uri.to_string());
  create_array(array3_uri.to_string());

  // Set expected
  std::vector<std::pair<tiledb::sm::URI, tiledb_object_t>> group1_expected = {
      {array1_uri, TILEDB_ARRAY},
      {array2_uri, TILEDB_ARRAY},
      {group2_uri, TILEDB_GROUP},
  };
  std::vector<std::pair<tiledb::sm::URI, tiledb_object_t>> group2_expected = {
      {array3_uri, TILEDB_ARRAY},
  };

  tiledb_group_t* group1;
  int rc = tiledb_group_alloc(ctx_, group1_uri.c_str(), &group1);
  REQUIRE(rc == TILEDB_OK);
  set_group_timestamp(group1, 1);
  rc = tiledb_group_open(ctx_, group1, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  tiledb_group_t* group2;
  rc = tiledb_group_alloc(ctx_, group2_uri.c_str(), &group2);
  REQUIRE(rc == TILEDB_OK);
  set_group_timestamp(group2, 1);
  rc = tiledb_group_open(ctx_, group2, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_group_add_member(
      ctx_, group1, array1_relative_uri.c_str(), true, nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_group_add_member(
      ctx_, group1, array2_relative_uri.c_str(), true, nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_group_add_member(
      ctx_, group2, array3_relative_uri.c_str(), true, nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc =
      tiledb_group_add_member(ctx_, group1, group2_uri.c_str(), false, nullptr);
  REQUIRE(rc == TILEDB_OK);

  // Close group from write mode
  rc = tiledb_group_close(ctx_, group1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_group_close(ctx_, group2);
  REQUIRE(rc == TILEDB_OK);

  // Reopen in read mode
  rc = tiledb_group_open(ctx_, group1, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_group_open(ctx_, group2, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  std::vector<std::pair<tiledb::sm::URI, tiledb_object_t>> group1_received =
      read_group(group1);
  REQUIRE_THAT(
      group1_received, Catch::Matchers::UnorderedEquals(group1_expected));

  std::vector<std::pair<tiledb::sm::URI, tiledb_object_t>> group2_received =
      read_group(group2);
  REQUIRE_THAT(
      group2_received, Catch::Matchers::UnorderedEquals(group2_expected));

  // Close group
  rc = tiledb_group_close(ctx_, group1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_group_close(ctx_, group2);
  REQUIRE(rc == TILEDB_OK);

  // Remove assets from group
  set_group_timestamp(group1, 2);
  rc = tiledb_group_open(ctx_, group1, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  set_group_timestamp(group2, 2);
  rc = tiledb_group_open(ctx_, group2, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_group_remove_member(ctx_, group1, group2_uri.c_str());
  REQUIRE(rc == TILEDB_OK);
  // Group is the latest element
  group1_expected.resize(group1_expected.size() - 1);

  rc = tiledb_group_remove_member(ctx_, group2, array3_relative_uri.c_str());
  REQUIRE(rc == TILEDB_OK);
  // There should be nothing left in group2
  group2_expected.clear();

  // Close group
  rc = tiledb_group_close(ctx_, group1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_group_close(ctx_, group2);
  REQUIRE(rc == TILEDB_OK);

  // Check read again
  set_group_timestamp(group1, 2);
  rc = tiledb_group_open(ctx_, group1, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  set_group_timestamp(group2, 2);
  rc = tiledb_group_open(ctx_, group2, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  group1_received = read_group(group1);
  REQUIRE_THAT(
      group1_received, Catch::Matchers::UnorderedEquals(group1_expected));

  group2_received = read_group(group2);
  REQUIRE_THAT(
      group2_received, Catch::Matchers::UnorderedEquals(group2_expected));

  // Close group
  rc = tiledb_group_close(ctx_, group1);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_group_close(ctx_, group2);
  REQUIRE(rc == TILEDB_OK);
  tiledb_group_free(&group1);
  tiledb_group_free(&group2);
  remove_temp_dir(temp_dir);
}

#ifdef TILEDB_SERIALIZATION
TEST_CASE_METHOD(
    GroupFx,
    "C API: Group, write/read, serialization",
    "[capi][group][metadata][read]") {
  // Create and open group in write mode
  // TODO: refactor for each supported FS.
  std::string temp_dir = fs_vec_[0]->temp_dir();
  create_temp_dir(temp_dir);

  const tiledb::sm::URI array1_uri(temp_dir + "array1");
  const tiledb::sm::URI array2_uri(temp_dir + "array2");
  const tiledb::sm::URI array3_uri(temp_dir + "array3");
  create_array(array1_uri.to_string());
  create_array(array2_uri.to_string());
  create_array(array3_uri.to_string());

  tiledb::sm::URI group1_uri(temp_dir + "group1");
  REQUIRE(tiledb_group_create(ctx_, group1_uri.c_str()) == TILEDB_OK);

  tiledb::sm::URI group2_uri(temp_dir + "group2");
  REQUIRE(tiledb_group_create(ctx_, group2_uri.c_str()) == TILEDB_OK);

  tiledb::sm::URI group3_uri(temp_dir + "group1_deserialized");
  REQUIRE(tiledb_group_create(ctx_, group3_uri.c_str()) == TILEDB_OK);

  tiledb::sm::URI group4_uri(temp_dir + "group2_deserialized");
  REQUIRE(tiledb_group_create(ctx_, group4_uri.c_str()) == TILEDB_OK);

  // Set expected
  std::vector<std::pair<tiledb::sm::URI, tiledb_object_t>> group1_expected = {
      {array1_uri, TILEDB_ARRAY},
      {array2_uri, TILEDB_ARRAY},
      {group2_uri, TILEDB_GROUP},
  };
  std::vector<std::pair<tiledb::sm::URI, tiledb_object_t>> group2_expected = {
      {array3_uri, TILEDB_ARRAY},
  };
  std::vector<std::pair<tiledb::sm::URI, tiledb_object_t>> group3_expected = {
      {array1_uri, TILEDB_ARRAY},
      {array2_uri, TILEDB_ARRAY},
      {group2_uri, TILEDB_GROUP},
  };
  std::vector<std::pair<tiledb::sm::URI, tiledb_object_t>> group4_expected = {
      {array3_uri, TILEDB_ARRAY},
  };

  tiledb_group_t* group1_write;
  int rc = tiledb_group_alloc(ctx_, group1_uri.c_str(), &group1_write);
  REQUIRE(rc == TILEDB_OK);
  set_group_timestamp(group1_write, 1);
  rc = tiledb_group_open(ctx_, group1_write, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  tiledb_group_t* group2_write;
  rc = tiledb_group_alloc(ctx_, group2_uri.c_str(), &group2_write);
  REQUIRE(rc == TILEDB_OK);
  set_group_timestamp(group2_write, 1);
  rc = tiledb_group_open(ctx_, group2_write, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_group_add_member(
      ctx_, group1_write, array1_uri.c_str(), false, nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_group_add_member(
      ctx_, group1_write, array2_uri.c_str(), false, nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_group_add_member(
      ctx_, group2_write, array3_uri.c_str(), false, nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_group_add_member(
      ctx_, group1_write, group2_uri.c_str(), false, nullptr);
  REQUIRE(rc == TILEDB_OK);

  // Close
  rc = tiledb_group_close(ctx_, group1_write);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_group_close(ctx_, group2_write);
  REQUIRE(rc == TILEDB_OK);

  // Free
  tiledb_group_free(&group1_write);
  tiledb_group_free(&group2_write);

  // Reopen in read mode
  tiledb_group_t* group1_read;
  rc = tiledb_group_alloc(ctx_, group1_uri.c_str(), &group1_read);
  REQUIRE(rc == TILEDB_OK);
  set_group_timestamp(group1_read, 1);
  rc = tiledb_group_open(ctx_, group1_read, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  tiledb_group_t* group2_read;
  rc = tiledb_group_alloc(ctx_, group2_uri.c_str(), &group2_read);
  REQUIRE(rc == TILEDB_OK);
  set_group_timestamp(group2_read, 1);
  rc = tiledb_group_open(ctx_, group2_read, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  std::vector<std::pair<tiledb::sm::URI, tiledb_object_t>> group1_received =
      read_group(group1_read);
  REQUIRE_THAT(
      group1_received, Catch::Matchers::UnorderedEquals(group1_expected));

  std::vector<std::pair<tiledb::sm::URI, tiledb_object_t>> group2_received =
      read_group(group2_read);
  REQUIRE_THAT(
      group2_received, Catch::Matchers::UnorderedEquals(group2_expected));

  // Now that we validated the groups were written correctly
  // lets test taking a read, serializing it and writing
  tiledb_group_t* group3_write_deserialized;
  rc = tiledb_group_alloc(ctx_, group3_uri.c_str(), &group3_write_deserialized);
  REQUIRE(rc == TILEDB_OK);
  set_group_timestamp(group3_write_deserialized, 1);
  rc = tiledb_group_open(ctx_, group3_write_deserialized, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_group_serialize(
      ctx_, group1_read, group3_write_deserialized, TILEDB_JSON);
  REQUIRE(rc == TILEDB_OK);

  // Close deserialized write group
  rc = tiledb_group_close(ctx_, group3_write_deserialized);
  REQUIRE(rc == TILEDB_OK);
  tiledb_group_free(&group3_write_deserialized);

  tiledb_group_t* group4_write_deserialized;
  rc = tiledb_group_alloc(ctx_, group4_uri.c_str(), &group4_write_deserialized);
  REQUIRE(rc == TILEDB_OK);
  set_group_timestamp(group4_write_deserialized, 1);
  rc = tiledb_group_open(ctx_, group4_write_deserialized, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_group_serialize(
      ctx_, group2_read, group4_write_deserialized, TILEDB_JSON);
  REQUIRE(rc == TILEDB_OK);

  // Close deserialized write group
  rc = tiledb_group_close(ctx_, group4_write_deserialized);
  REQUIRE(rc == TILEDB_OK);
  tiledb_group_free(&group4_write_deserialized);

  // Check group3 read
  tiledb_group_t* group3_read;
  rc = tiledb_group_alloc(ctx_, group3_uri.c_str(), &group3_read);
  REQUIRE(rc == TILEDB_OK);
  set_group_timestamp(group3_read, 1);
  rc = tiledb_group_open(ctx_, group3_read, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  std::vector<std::pair<tiledb::sm::URI, tiledb_object_t>> group3_received =
      read_group(group3_read);
  REQUIRE_THAT(
      group3_received, Catch::Matchers::UnorderedEquals(group3_expected));

  // Check group4 read
  tiledb_group_t* group4_read;
  rc = tiledb_group_alloc(ctx_, group4_uri.c_str(), &group4_read);
  REQUIRE(rc == TILEDB_OK);
  set_group_timestamp(group4_read, 1);
  rc = tiledb_group_open(ctx_, group4_read, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  std::vector<std::pair<tiledb::sm::URI, tiledb_object_t>> group4_received =
      read_group(group4_read);
  REQUIRE_THAT(
      group4_received, Catch::Matchers::UnorderedEquals(group4_expected));

  // Close read groups
  rc = tiledb_group_close(ctx_, group1_read);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_group_close(ctx_, group2_read);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_group_close(ctx_, group3_read);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_group_close(ctx_, group4_read);
  REQUIRE(rc == TILEDB_OK);

  // Remove assets from group
  rc = tiledb_group_alloc(ctx_, group1_uri.c_str(), &group1_write);
  REQUIRE(rc == TILEDB_OK);
  set_group_timestamp(group1_write, 2);
  rc = tiledb_group_open(ctx_, group1_write, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_group_alloc(ctx_, group2_uri.c_str(), &group2_write);
  REQUIRE(rc == TILEDB_OK);
  set_group_timestamp(group2_write, 2);
  rc = tiledb_group_open(ctx_, group2_write, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_group_remove_member(ctx_, group1_write, group2_uri.c_str());
  REQUIRE(rc == TILEDB_OK);
  // Group is the latest element
  group1_expected.resize(group1_expected.size() - 1);
  group3_expected.resize(group3_expected.size() - 1);

  rc = tiledb_group_remove_member(ctx_, group2_write, array3_uri.c_str());
  REQUIRE(rc == TILEDB_OK);
  // There should be nothing left in group2/group4
  group2_expected.clear();
  group4_expected.clear();

  // Materialized groups
  rc = tiledb_group_close(ctx_, group1_write);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_group_close(ctx_, group2_write);
  REQUIRE(rc == TILEDB_OK);

  tiledb_group_free(&group1_write);
  tiledb_group_free(&group2_write);

  // Check read again
  set_group_timestamp(group1_read, 2);
  rc = tiledb_group_open(ctx_, group1_read, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  set_group_timestamp(group2_read, 2);
  rc = tiledb_group_open(ctx_, group2_read, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);

  group1_received = read_group(group1_read);
  REQUIRE_THAT(
      group1_received, Catch::Matchers::UnorderedEquals(group1_expected));

  group2_received = read_group(group2_read);
  REQUIRE_THAT(
      group2_received, Catch::Matchers::UnorderedEquals(group2_expected));

  // Now that we validated the groups were written correctly
  // lets test taking a read, serializing it and writing
  rc = tiledb_group_alloc(ctx_, group3_uri.c_str(), &group3_write_deserialized);
  REQUIRE(rc == TILEDB_OK);
  set_group_timestamp(group3_write_deserialized, 2);
  rc = tiledb_group_open(ctx_, group3_write_deserialized, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_group_serialize(
      ctx_, group1_read, group3_write_deserialized, TILEDB_JSON);
  REQUIRE(rc == TILEDB_OK);

  // Close deserialized write group
  rc = tiledb_group_close(ctx_, group3_write_deserialized);
  REQUIRE(rc == TILEDB_OK);
  tiledb_group_free(&group3_write_deserialized);

  rc = tiledb_group_alloc(ctx_, group4_uri.c_str(), &group4_write_deserialized);
  REQUIRE(rc == TILEDB_OK);
  set_group_timestamp(group4_write_deserialized, 2);
  rc = tiledb_group_open(ctx_, group4_write_deserialized, TILEDB_WRITE);
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_group_serialize(
      ctx_, group2_read, group4_write_deserialized, TILEDB_JSON);
  REQUIRE(rc == TILEDB_OK);

  // Close deserialized write group
  rc = tiledb_group_close(ctx_, group4_write_deserialized);
  REQUIRE(rc == TILEDB_OK);
  tiledb_group_free(&group4_write_deserialized);

  // Check deserialized group3
  set_group_timestamp(group3_read, 2);
  rc = tiledb_group_open(ctx_, group3_read, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);
  group3_received = read_group(group3_read);
  REQUIRE_THAT(
      group3_received, Catch::Matchers::UnorderedEquals(group3_expected));

  // Check deserialized group4
  set_group_timestamp(group4_read, 2);
  rc = tiledb_group_open(ctx_, group4_read, TILEDB_READ);
  REQUIRE(rc == TILEDB_OK);
  group4_received = read_group(group4_read);
  REQUIRE_THAT(
      group4_received, Catch::Matchers::UnorderedEquals(group4_expected));

  // Close group
  rc = tiledb_group_close(ctx_, group1_read);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_group_close(ctx_, group2_read);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_group_close(ctx_, group3_read);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_group_close(ctx_, group4_read);
  REQUIRE(rc == TILEDB_OK);
  tiledb_group_free(&group1_read);
  tiledb_group_free(&group2_read);
  tiledb_group_free(&group3_read);
  tiledb_group_free(&group4_read);
  remove_temp_dir(temp_dir);
}
#endif