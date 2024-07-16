/**
 * @file   unit-cppapi-group.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2024 TileDB Inc.
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
 * Tests for the C++ API object management code.
 */

#include <test/support/tdb_catch.h>
#include "test/support/src/helpers.h"
#include "test/support/src/serialization_wrappers.h"
#include "test/support/src/vfs_helpers.h"
#ifdef _WIN32
#include "tiledb/sm/filesystem/path_win.h"
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif

#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/cpp_api/group.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/global_state/unit_test_config.h"

#include <iostream>
#include <sstream>
#include <thread>

using namespace tiledb::test;

struct GroupCPPFx {
  const std::string GROUP = "group/";
  const std::string ARRAY = "array/";

  // TileDB context
  tiledb::Context ctx_;
  tiledb_ctx_t* ctx_c_;
  tiledb_vfs_t* vfs_;

  // Vector of supported filesystems
  const std::vector<std::unique_ptr<SupportedFs>> fs_vec_;

  /**
   * If true, array schema is serialized before submission, to test the
   * serialization paths.
   */
  bool serialize_ = false;

  // Functions
  GroupCPPFx();
  ~GroupCPPFx();
  void create_array(const std::string& path) const;
  void create_temp_dir(const std::string& path) const;
  void remove_temp_dir(const std::string& path) const;
  std::vector<tiledb::Object> read_group(const tiledb::Group& group) const;
  void set_group_timestamp(
      tiledb::Group* group, const uint64_t& timestamp) const;
};

GroupCPPFx::GroupCPPFx()
    : fs_vec_(vfs_test_get_fs_vec()) {
  // Initialize vfs test
  ctx_c_ = nullptr;
  vfs_ = nullptr;
  REQUIRE(vfs_test_init(fs_vec_, &ctx_c_, &vfs_).ok());
  ctx_ = tiledb::Context(ctx_c_, false);
}

GroupCPPFx::~GroupCPPFx() {
  // Close vfs test
  REQUIRE(vfs_test_close(fs_vec_, ctx_c_, vfs_).ok());
  tiledb_vfs_free(&vfs_);
  tiledb_ctx_free(&ctx_c_);
}

void GroupCPPFx::set_group_timestamp(
    tiledb::Group* group, const uint64_t& timestamp) const {
  tiledb::Config config;
  config.set("sm.group.timestamp_end", std::to_string(timestamp));
  group->set_config(config);
}

std::vector<tiledb::Object> GroupCPPFx::read_group(
    const tiledb::Group& group) const {
  std::vector<tiledb::Object> ret;
  uint64_t count = group.member_count();
  for (uint64_t i = 0; i < count; i++) {
    tiledb::Object obj = group.member(i);
    ret.emplace_back(obj);
  }
  return ret;
}

void GroupCPPFx::create_temp_dir(const std::string& path) const {
  remove_temp_dir(path);
  REQUIRE(tiledb_vfs_create_dir(ctx_c_, vfs_, path.c_str()) == TILEDB_OK);
}

void GroupCPPFx::remove_temp_dir(const std::string& path) const {
  int is_dir = 0;
  REQUIRE(tiledb_vfs_is_dir(ctx_c_, vfs_, path.c_str(), &is_dir) == TILEDB_OK);
  if (is_dir)
    REQUIRE(tiledb_vfs_remove_dir(ctx_c_, vfs_, path.c_str()) == TILEDB_OK);
}

void GroupCPPFx::create_array(const std::string& path) const {
  tiledb_attribute_t* a1;
  tiledb_attribute_alloc(ctx_c_, "a1", TILEDB_FLOAT32, &a1);

  // Domain and tile extents
  int64_t dim_domain[2] = {1, 1};
  int64_t tile_extents[] = {1};

  // Create dimension
  tiledb_dimension_t* d1;
  tiledb_dimension_alloc(
      ctx_c_, "d1", TILEDB_INT64, &dim_domain[0], &tile_extents[0], &d1);

  // Domain
  tiledb_domain_t* domain;
  tiledb_domain_alloc(ctx_c_, &domain);
  tiledb_domain_add_dimension(ctx_c_, domain, d1);

  // Create array schema
  tiledb_array_schema_t* array_schema;
  tiledb_array_schema_alloc(ctx_c_, TILEDB_DENSE, &array_schema);
  tiledb_array_schema_set_domain(ctx_c_, array_schema, domain);
  tiledb_array_schema_add_attribute(ctx_c_, array_schema, a1);

  // Check array schema
  REQUIRE(tiledb_array_schema_check(ctx_c_, array_schema) == TILEDB_OK);

  // Create array
  REQUIRE(
      tiledb_array_create_serialization_wrapper(
          ctx_c_, path, array_schema, serialize_) == TILEDB_OK);

  // Free objects
  tiledb_attribute_free(&a1);
  tiledb_dimension_free(&d1);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

TEST_CASE_METHOD(
    GroupCPPFx,
    "C++ API: Test creating group with config",
    "[cppapi][group][config]") {
  // TODO: refactor for each supported FS.
  std::string temp_dir = fs_vec_[0]->temp_dir();
  create_temp_dir(temp_dir);

  std::string group1_uri = temp_dir + "group1";
  tiledb::Group::create(ctx_, group1_uri);

  const std::string& test_key = "foo";
  const std::string& test_value = "bar";
  tiledb::Config config;
  config[test_key] = test_value;

  tiledb::Group group(ctx_, group1_uri, TILEDB_WRITE, config);

  CHECK(group.config().get(test_key) == test_value);

  group.close();
}

TEST_CASE_METHOD(
    GroupCPPFx, "C++ API: Test group metadata", "[cppapi][group][metadata]") {
  // TODO: refactor for each supported FS.
  std::string temp_dir = fs_vec_[0]->temp_dir();
  create_temp_dir(temp_dir);

  std::string group1_uri = temp_dir + "group1";
  tiledb::Group::create(ctx_, group1_uri);
  tiledb::Group group(ctx_, group1_uri, TILEDB_WRITE);
  group.close();

  // Put metadata on a group that is not opened
  int v = 5;
  REQUIRE_THROWS(group.put_metadata("key", TILEDB_INT32, 1, &v));

  // Write metadata on an group opened in READ mode
  set_group_timestamp(&group, 1);
  group.open(TILEDB_READ);
  REQUIRE_THROWS(group.put_metadata("key", TILEDB_INT32, 1, &v));

  // Close group
  group.close();
  set_group_timestamp(&group, 1);
  group.open(TILEDB_WRITE);

  // Write value type BLOB
  REQUIRE_THROWS(group.put_metadata("key", TILEDB_ANY, 1, &v));

  // Write a correct item
  group.put_metadata("key", TILEDB_INT32, 1, &v);

  // Consolidate and vacuum metadata with default config
  group.consolidate_metadata(ctx_, group1_uri);
  group.vacuum_metadata(ctx_, group1_uri);

  // Close group
  group.close();

  // Clean up
  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    GroupCPPFx,
    "C++ API: Group Metadata, write/read",
    "[cppapi][group][metadata][read]") {
  // Create and open group in write mode
  // TODO: refactor for each supported FS.
  std::string temp_dir = fs_vec_[0]->temp_dir();
  create_temp_dir(temp_dir);

  std::string group1_uri = temp_dir + "group1";
  tiledb::Group::create(ctx_, group1_uri);
  // Open group in write mode
  tiledb::Group group(ctx_, std::string(group1_uri), TILEDB_WRITE);
  // Reopen a timestamp
  group.close();
  set_group_timestamp(&group, 1);
  group.open(TILEDB_WRITE);

  // Write items
  int32_t v = 5;
  group.put_metadata("aaa", TILEDB_INT32, 1, &v);
  float f[] = {1.1f, 1.2f};
  group.put_metadata("bb", TILEDB_FLOAT32, 2, f);

  // Write null value
  group.put_metadata("zero_val", TILEDB_FLOAT32, 1, NULL);

  // Close group
  group.close();

  // Open the group in read mode
  set_group_timestamp(&group, 1);
  group.open(TILEDB_READ);

  // Read
  const void* v_r;
  tiledb_datatype_t v_type;
  uint32_t v_num;
  group.get_metadata("aaa", &v_type, &v_num, &v_r);
  CHECK(v_type == TILEDB_INT32);
  CHECK(v_num == 1);
  CHECK(*((const int32_t*)v_r) == 5);

  group.get_metadata("bb", &v_type, &v_num, &v_r);
  CHECK(v_type == TILEDB_FLOAT32);
  CHECK(v_num == 2);
  CHECK(((const float*)v_r)[0] == 1.1f);
  CHECK(((const float*)v_r)[1] == 1.2f);

  group.get_metadata("zero_val", &v_type, &v_num, &v_r);
  CHECK(v_type == TILEDB_FLOAT32);
  CHECK(v_num == 1);
  CHECK(v_r == nullptr);

  group.get_metadata("foo", &v_type, &v_num, &v_r);
  CHECK(v_r == nullptr);

  uint64_t num = group.metadata_num();
  CHECK(num == 3);

  std::string key;
  CHECK_THROWS(group.get_metadata_from_index(10, &key, &v_type, &v_num, &v_r));

  group.get_metadata_from_index(1, &key, &v_type, &v_num, &v_r);
  CHECK(v_type == TILEDB_FLOAT32);
  CHECK(v_num == 2);
  CHECK(((const float*)v_r)[0] == 1.1f);
  CHECK(((const float*)v_r)[1] == 1.2f);
  CHECK(key.size() == strlen("bb"));
  CHECK(!strncmp(key.data(), "bb", strlen("bb")));

  // idx 2 is 'zero_val'
  group.get_metadata_from_index(2, &key, &v_type, &v_num, &v_r);
  CHECK(v_type == TILEDB_FLOAT32);
  CHECK(v_num == 1);
  CHECK(v_r == nullptr);

  // Check has_key
  bool has_key;
  v_type = (tiledb_datatype_t)std::numeric_limits<int32_t>::max();
  has_key = group.has_metadata("bb", &v_type);
  CHECK(has_key == true);
  CHECK(v_type == TILEDB_FLOAT32);

  // Check not has_key
  v_type = (tiledb_datatype_t)std::numeric_limits<int32_t>::max();
  has_key = group.has_metadata("non-existent-key", &v_type);
  CHECK(has_key == false);
  CHECK(v_type == (tiledb_datatype_t)std::numeric_limits<int32_t>::max());

  // Close group
  group.close();
  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    GroupCPPFx, "C++ API: Group, set name", "[cppapi][group][read]") {
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
  tiledb::Group::create(ctx_, group1_uri.to_string());

  tiledb::sm::URI group2_uri(temp_dir + "group2");
  tiledb::Group::create(ctx_, group2_uri.to_string());

  // Set expected
  std::vector<tiledb::Object> group1_expected = {
      tiledb::Object(
          tiledb::Object::Type::Array, array1_uri.to_string(), "array1"),
      tiledb::Object(
          tiledb::Object::Type::Array, array2_uri.to_string(), "array2"),
      tiledb::Object(
          tiledb::Object::Type::Group, group2_uri.to_string(), "group2"),
  };
  std::vector<tiledb::Object> group2_expected = {
      tiledb::Object(
          tiledb::Object::Type::Array, array3_uri.to_string(), "array3"),
  };

  tiledb::Group group1(ctx_, group1_uri.to_string(), TILEDB_WRITE);
  group1.close();
  set_group_timestamp(&group1, 1);
  group1.open(TILEDB_WRITE);

  tiledb::Group group2(ctx_, group2_uri.to_string(), TILEDB_WRITE);
  group2.close();
  set_group_timestamp(&group2, 1);
  group2.open(TILEDB_WRITE);

  group1.add_member(array1_uri.to_string(), false, "array1");
  group1.add_member(array2_uri.to_string(), false, "array2");
  group1.add_member(group2_uri.to_string(), false, "group2");

  group2.add_member(array3_uri.to_string(), false, "array3");

  // Close group from write mode
  group1.close();
  group2.close();

  // Reopen in read mode
  set_group_timestamp(&group1, 1);
  group1.open(TILEDB_READ);
  set_group_timestamp(&group2, 1);
  group2.open(TILEDB_READ);

  std::vector<tiledb::Object> group1_received = read_group(group1);
  REQUIRE_THAT(
      group1_received, Catch::Matchers::UnorderedEquals(group1_expected));

  std::vector<tiledb::Object> group2_received = read_group(group2);
  REQUIRE_THAT(
      group2_received, Catch::Matchers::UnorderedEquals(group2_expected));

  // Close group
  group1.close();
  group2.close();

  // Remove assets from group
  set_group_timestamp(&group1, 2);
  group1.open(TILEDB_WRITE);
  set_group_timestamp(&group2, 2);
  group2.open(TILEDB_WRITE);

  group1.remove_member("group2");
  // Group is the latest element
  group1_expected.resize(group1_expected.size() - 1);

  group2.remove_member("array3");
  // There should be nothing left in group2
  group2_expected.clear();

  // Close group
  group1.close();
  group2.close();

  // Check read again
  set_group_timestamp(&group1, 2);
  group1.open(TILEDB_READ);
  set_group_timestamp(&group2, 2);
  group2.open(TILEDB_READ);

  group1_received = read_group(group1);
  REQUIRE_THAT(
      group1_received, Catch::Matchers::UnorderedEquals(group1_expected));

  const auto& obj = group1.member(group1_expected[0].name().value());
  REQUIRE(obj == group1_expected[0]);

  group2_received = read_group(group2);
  REQUIRE_THAT(
      group2_received, Catch::Matchers::UnorderedEquals(group2_expected));

  // Check that non-existent name throws
  REQUIRE_THROWS(group1.member("10"));
  // Checks for off by one indexing
  REQUIRE_THROWS(group1.member(group1_expected.size()));

  // Close group
  group1.close();
  group2.close();
  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    GroupCPPFx, "C++ API: Group, write/read", "[cppapi][group][read]") {
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
  tiledb::Group::create(ctx_, group1_uri.to_string());

  tiledb::sm::URI group2_uri(temp_dir + "group2");
  tiledb::Group::create(ctx_, group2_uri.to_string());

  // Set expected
  std::vector<tiledb::Object> group1_expected = {
      tiledb::Object(
          tiledb::Object::Type::Array, array1_uri.to_string(), std::nullopt),
      tiledb::Object(
          tiledb::Object::Type::Array, array2_uri.to_string(), std::nullopt),
      tiledb::Object(
          tiledb::Object::Type::Group, group2_uri.to_string(), std::nullopt),
  };
  std::vector<tiledb::Object> group2_expected = {
      tiledb::Object(
          tiledb::Object::Type::Array, array3_uri.to_string(), std::nullopt),
  };

  tiledb::Group group1(ctx_, group1_uri.to_string(), TILEDB_WRITE);
  group1.close();
  set_group_timestamp(&group1, 1);
  group1.open(TILEDB_WRITE);

  tiledb::Group group2(ctx_, group2_uri.to_string(), TILEDB_WRITE);
  group2.close();
  set_group_timestamp(&group2, 1);
  group2.open(TILEDB_WRITE);

  group1.add_member(array1_uri.to_string(), false);
  group1.add_member(array2_uri.to_string(), false);
  group1.add_member(group2_uri.to_string(), false);

  group2.add_member(array3_uri.to_string(), false);

  // Close group from write mode
  group1.close();
  group2.close();

  // Reopen in read mode
  group1.open(TILEDB_READ);
  group2.open(TILEDB_READ);

  std::vector<tiledb::Object> group1_received = read_group(group1);
  REQUIRE_THAT(
      group1_received, Catch::Matchers::UnorderedEquals(group1_expected));

  std::vector<tiledb::Object> group2_received = read_group(group2);
  REQUIRE_THAT(
      group2_received, Catch::Matchers::UnorderedEquals(group2_expected));

  // Close group
  group1.close();
  group2.close();

  // Remove assets from group
  set_group_timestamp(&group1, 2);
  group1.open(TILEDB_WRITE);
  set_group_timestamp(&group2, 2);
  group2.open(TILEDB_WRITE);

  group1.remove_member(group2_uri.to_string());
  // Group is the latest element
  group1_expected.resize(group1_expected.size() - 1);

  group2.remove_member(array3_uri.to_string());
  // There should be nothing left in group2
  group2_expected.clear();

  // Close group
  group1.close();
  group2.close();

  // Check read again
  set_group_timestamp(&group1, 2);
  group1.open(TILEDB_READ);
  set_group_timestamp(&group2, 2);
  group2.open(TILEDB_READ);

  group1_received = read_group(group1);
  REQUIRE_THAT(
      group1_received, Catch::Matchers::UnorderedEquals(group1_expected));

  group2_received = read_group(group2);
  REQUIRE_THAT(
      group2_received, Catch::Matchers::UnorderedEquals(group2_expected));

  // Check that out of bounds indexing throws
  REQUIRE_THROWS(group1.member(10));
  // Checks for off by one indexing
  REQUIRE_THROWS(group1.member(group1_expected.size()));

  // Close group
  group1.close();
  group2.close();
  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    GroupCPPFx,
    "C++ API: Group, write/read, relative",
    "[cppapi][group][read]") {
  // Create and open group in write mode
  // TODO: refactor for each supported FS.
  std::string temp_dir = fs_vec_[0]->temp_dir();
  create_temp_dir(temp_dir);

  tiledb::sm::URI group1_uri(temp_dir + "group1");
  tiledb::Group::create(ctx_, group1_uri.to_string());

  tiledb::sm::URI group2_uri(temp_dir + "group2");
  tiledb::Group::create(ctx_, group2_uri.to_string());

  REQUIRE(
      tiledb_vfs_create_dir(
          ctx_.ptr().get(), vfs_, (temp_dir + "group1/arrays").c_str()) ==
      TILEDB_OK);
  REQUIRE(
      tiledb_vfs_create_dir(
          ctx_.ptr().get(), vfs_, (temp_dir + "group2/arrays").c_str()) ==
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
  std::vector<tiledb::Object> group1_expected = {
      tiledb::Object(
          tiledb::Object::Type::Array, array1_uri.to_string(), std::nullopt),
      tiledb::Object(
          tiledb::Object::Type::Array, array2_uri.to_string(), std::nullopt),
      tiledb::Object(
          tiledb::Object::Type::Group, group2_uri.to_string(), std::nullopt),
  };
  std::vector<tiledb::Object> group2_expected = {
      tiledb::Object(
          tiledb::Object::Type::Array, array3_uri.to_string(), std::nullopt),
  };

  tiledb::Group group1(ctx_, group1_uri.to_string(), TILEDB_WRITE);
  group1.close();
  set_group_timestamp(&group1, 1);
  group1.open(TILEDB_WRITE);

  tiledb::Group group2(ctx_, group2_uri.to_string(), TILEDB_WRITE);
  group2.close();
  set_group_timestamp(&group2, 1);
  group2.open(TILEDB_WRITE);

  group1.add_member(array1_relative_uri, true);
  group1.add_member(array2_relative_uri, true);
  group1.add_member(group2_uri.to_string(), false);

  group2.add_member(array3_relative_uri, true);

  // Close group from write mode
  group1.close();
  group2.close();

  // Reopen in read mode
  group1.open(TILEDB_READ);
  group2.open(TILEDB_READ);

  std::vector<tiledb::Object> group1_received = read_group(group1);
  REQUIRE_THAT(
      group1_received, Catch::Matchers::UnorderedEquals(group1_expected));

  std::vector<tiledb::Object> group2_received = read_group(group2);
  REQUIRE_THAT(
      group2_received, Catch::Matchers::UnorderedEquals(group2_expected));

  // Close group
  group1.close();
  group2.close();

  // Remove assets from group
  set_group_timestamp(&group1, 2);
  group1.open(TILEDB_WRITE);
  set_group_timestamp(&group2, 2);
  group2.open(TILEDB_WRITE);

  group1.remove_member(group2_uri.to_string());
  // Group is the latest element
  group1_expected.resize(group1_expected.size() - 1);

  group2.remove_member(array3_relative_uri);
  // There should be nothing left in group2
  group2_expected.clear();

  // Close group
  group1.close();
  group2.close();

  // Check read again
  set_group_timestamp(&group1, 2);
  group1.open(TILEDB_READ);
  set_group_timestamp(&group2, 2);
  group2.open(TILEDB_READ);

  group1_received = read_group(group1);
  REQUIRE_THAT(
      group1_received, Catch::Matchers::UnorderedEquals(group1_expected));

  group2_received = read_group(group2);
  REQUIRE_THAT(
      group2_received, Catch::Matchers::UnorderedEquals(group2_expected));

  // Close group
  group1.close();
  group2.close();
  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    GroupCPPFx,
    "C++ API: Group, write/read, relative named",
    "[cppapi][group][read]") {
  bool remove_by_name = GENERATE(true, false);

  // Create and open group in write mode
  // TODO: refactor for each supported FS.
  std::string temp_dir = fs_vec_[0]->temp_dir();
  create_temp_dir(temp_dir);

  tiledb::sm::URI group1_uri(temp_dir + "group1");
  tiledb::Group::create(ctx_, group1_uri.to_string());

  tiledb::sm::URI group2_uri(temp_dir + "group2");
  tiledb::Group::create(ctx_, group2_uri.to_string());

  REQUIRE(
      tiledb_vfs_create_dir(
          ctx_.ptr().get(), vfs_, (temp_dir + "group1/arrays").c_str()) ==
      TILEDB_OK);
  REQUIRE(
      tiledb_vfs_create_dir(
          ctx_.ptr().get(), vfs_, (temp_dir + "group2/arrays").c_str()) ==
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
  std::vector<tiledb::Object> group1_expected = {
      tiledb::Object(
          tiledb::Object::Type::Array, array1_uri.to_string(), "one"),
      tiledb::Object(
          tiledb::Object::Type::Array, array2_uri.to_string(), "two"),
      tiledb::Object(
          tiledb::Object::Type::Group, group2_uri.to_string(), "three"),
  };
  std::vector<tiledb::Object> group2_expected = {
      tiledb::Object(
          tiledb::Object::Type::Array, array3_uri.to_string(), "four"),
  };

  tiledb::Group group1(ctx_, group1_uri.to_string(), TILEDB_WRITE);
  group1.close();
  set_group_timestamp(&group1, 1);
  group1.open(TILEDB_WRITE);

  tiledb::Group group2(ctx_, group2_uri.to_string(), TILEDB_WRITE);
  group2.close();
  set_group_timestamp(&group2, 1);
  group2.open(TILEDB_WRITE);

  group1.add_member(array1_relative_uri, true, "one");
  group1.add_member(array2_relative_uri, true, "two");
  group1.add_member(group2_uri.to_string(), false, "three");

  group2.add_member(array3_relative_uri, true, "four");

  // Close group from write mode
  group1.close();
  group2.close();

  // Reopen in read mode
  group1.open(TILEDB_READ);
  group2.open(TILEDB_READ);

  std::vector<tiledb::Object> group1_received = read_group(group1);
  REQUIRE_THAT(
      group1_received, Catch::Matchers::UnorderedEquals(group1_expected));

  std::vector<tiledb::Object> group2_received = read_group(group2);
  REQUIRE_THAT(
      group2_received, Catch::Matchers::UnorderedEquals(group2_expected));

  bool is_relative;
  is_relative = group1.is_relative("one");
  REQUIRE(is_relative == true);
  is_relative = group1.is_relative("two");
  REQUIRE(is_relative == true);
  is_relative = group1.is_relative("three");
  REQUIRE(is_relative == false);
  is_relative = group2.is_relative("four");
  REQUIRE(is_relative == true);

  REQUIRE_THROWS(group2.is_relative("nonexistent"));

  // Close group
  group1.close();
  group2.close();

  // Remove assets from group
  set_group_timestamp(&group1, 2);
  group1.open(TILEDB_WRITE);
  set_group_timestamp(&group2, 2);
  group2.open(TILEDB_WRITE);

  if (remove_by_name) {
    group1.remove_member("three");
  } else {
    group1.remove_member(group2_uri.to_string());
  }

  // Group is the latest element
  group1_expected.resize(group1_expected.size() - 1);

  if (remove_by_name) {
    group2.remove_member("four");
  } else {
    group2.remove_member(array3_relative_uri);
  }

  // There should be nothing left in group2
  group2_expected.clear();

  // Close group
  group1.close();
  group2.close();

  // Check read again
  set_group_timestamp(&group1, 2);
  group1.open(TILEDB_READ);
  set_group_timestamp(&group2, 2);
  group2.open(TILEDB_READ);

  group1_received = read_group(group1);
  REQUIRE_THAT(
      group1_received, Catch::Matchers::UnorderedEquals(group1_expected));

  group2_received = read_group(group2);
  REQUIRE_THAT(
      group2_received, Catch::Matchers::UnorderedEquals(group2_expected));

  // Close group
  group1.close();
  group2.close();
  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    GroupCPPFx,
    "C++ API: Group, delete by URI, duplicates",
    "[cppapi][group][delete]") {
  bool nameless_uri = GENERATE(true, false);

  // Create and open group in write mode
  // TODO: refactor for each supported FS.
  std::string temp_dir = fs_vec_[0]->temp_dir();
  create_temp_dir(temp_dir);

  tiledb::sm::URI group1_uri(temp_dir + "group1");
  tiledb::Group::create(ctx_, group1_uri.to_string());

  REQUIRE(
      tiledb_vfs_create_dir(
          ctx_.ptr().get(), vfs_, (temp_dir + "group1/arrays").c_str()) ==
      TILEDB_OK);

  const std::string array1_relative_uri("arrays/array1");
  const tiledb::sm::URI array1_uri(temp_dir + "group1/arrays/array1");
  const std::string array2_relative_uri("arrays/array2");
  const tiledb::sm::URI array2_uri(temp_dir + "group1/arrays/array2");
  create_array(array1_uri.to_string());
  create_array(array2_uri.to_string());

  // Set expected
  std::vector<tiledb::Object> group1_expected = {
      tiledb::Object(
          tiledb::Object::Type::Array, array1_uri.to_string(), "one"),
      tiledb::Object(
          tiledb::Object::Type::Array, array2_uri.to_string(), "two"),
      nameless_uri ?
          tiledb::Object(
              tiledb::Object::Type::Array, array2_uri.to_string(), nullopt) :
          tiledb::Object(
              tiledb::Object::Type::Array, array2_uri.to_string(), "three"),
  };

  tiledb::Group group1(ctx_, group1_uri.to_string(), TILEDB_WRITE);
  group1.close();
  set_group_timestamp(&group1, 1);
  group1.open(TILEDB_WRITE);

  group1.add_member(array1_relative_uri, true, "one");
  group1.add_member(array2_relative_uri, true, "two");
  group1.add_member(
      array2_relative_uri,
      true,
      nameless_uri ? nullopt : std::make_optional<std::string>("three"));

  // Close group from write mode
  group1.close();

  // Reopen in read mode
  group1.open(TILEDB_READ);

  std::vector<tiledb::Object> group1_received = read_group(group1);
  REQUIRE_THAT(
      group1_received, Catch::Matchers::UnorderedEquals(group1_expected));

  bool is_relative;
  is_relative = group1.is_relative("one");
  REQUIRE(is_relative == true);
  is_relative = group1.is_relative("two");
  REQUIRE(is_relative == true);

  if (!nameless_uri) {
    is_relative = group1.is_relative("three");
    REQUIRE(is_relative == true);
  }

  // Close group
  group1.close();

  // Remove assets from group
  set_group_timestamp(&group1, 2);
  group1.open(TILEDB_WRITE);
  if (nameless_uri) {
    group1.remove_member(array2_relative_uri);
  } else {
    CHECK_THROWS_WITH(
        group1.remove_member(array2_relative_uri),
        Catch::Matchers::ContainsSubstring(
            "there are multiple members with the "
            "same URI, please remove by name."));
    group1.remove_member("three");
  }

  // Group is the latest element
  group1_expected.resize(group1_expected.size() - 1);

  // Close group
  group1.close();

  // Check read again
  set_group_timestamp(&group1, 2);
  group1.open(TILEDB_READ);

  group1_received = read_group(group1);
  REQUIRE_THAT(
      group1_received, Catch::Matchers::UnorderedEquals(group1_expected));

  // Close group
  group1.close();
  remove_temp_dir(temp_dir);
}

/** Test Exception For Assertability */
class GroupDtorDoesntThrowException : public std::exception {
 public:
  explicit GroupDtorDoesntThrowException()
      : std::exception() {
  }
};

TEST_CASE("C++ API: Group close group with error", "[cppapi][group][error]") {
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);
  std::vector<std::string> dnames = {"main_group", "main_group_renamed"};

  auto cleaner = [&]() -> void {
    for (auto dir : dnames) {
      if (vfs.is_dir(dir)) {
        vfs.remove_dir(dir);
      }
    }
  };
  cleaner();

  // This test is a bit subtle in that we're attempting to assert that a
  // destructor doesn't throw. Since throwing from a destructor is perfectly
  // valid (although extremely discouraged outside of extremely niche cases)
  // we have to force a situation such that an exception thrown from the
  // destructor would cause some other observable behavior.
  //
  // For this test, I'm using the case where throwing an exception while the
  // stack is already being unwound being C++ mandated as a call to
  // std::terminate which will fail this test quite hard.
  //
  // The GroupDtroDoesntThrowException is a specific class so that we know
  // we've caught the exception we expect. The thrown_correctly assertion
  // is mostly for show since the real failure would be the std::terminate
  // which is unrecoverable.

  bool thrown_correctly = false;

  try {
    // Create our group preliminaries
    tiledb::create_group(ctx, "main_group");
    tiledb::create_group(ctx, "main_group/sub_group");

    tiledb::Group group(ctx, "main_group", TILEDB_WRITE);
    group.add_member("main_group/sub_group", false, "sub_group");

    // Muck with the filesystem so that when the group.close() is called it
    // throws an exception due to missing paths
    REQUIRE(rename("main_group", "main_group_renamed") == 0);

    // Check that group.close() actually throws
    REQUIRE_THROWS(group.close());

    // By throwing here, Group will go out of scope calling close. If this
    // throws we end up with the exact reason why throwing from a destructor
    // is a bad idea because throwing an exception while the stack is
    // unwinding is specified by the language to call std::terminate
    throw GroupDtorDoesntThrowException();

  } catch (GroupDtorDoesntThrowException& exc) {
    thrown_correctly = true;
  }

  REQUIRE(thrown_correctly);

  cleaner();
}

TEST_CASE(
    "C++ API: Group with Relative URI members, write/read, rest",
    "[cppapi][group][relative][rest]") {
  VFSTestSetup vfs_test_setup;
  tiledb::Context ctx{vfs_test_setup.ctx()};
  auto group_name{vfs_test_setup.array_uri("groups_relative")};
  auto subgroup_name = group_name + "/subgroup";

  // Create groups
  tiledb::create_group(ctx, group_name);
  tiledb::create_group(ctx, subgroup_name);

  // Open group in write mode
  {
    auto group = tiledb::Group(ctx, group_name, TILEDB_WRITE);
    if (vfs_test_setup.is_rest()) {
      CHECK_THROWS_WITH(
          group.add_member("subgroup", true, "subgroup"),
          Catch::Matchers::EndsWith("Cannot add member; Remote groups do not "
                                    "support members with relative "
                                    "URIs"));
    } else {
      CHECK_NOTHROW(group.add_member("subgroup", true, "subgroup"));
    }
    group.close();
  }

  if (!vfs_test_setup.is_rest()) {
    auto group = tiledb::Group(ctx, group_name, TILEDB_READ);

    auto subgroup_member = group.member("subgroup");

    CHECK(subgroup_member.type() == tiledb::Object::Type::Group);
    CHECK(subgroup_member.name() == "subgroup");
    CHECK(group.is_relative("subgroup"));
  }
}
