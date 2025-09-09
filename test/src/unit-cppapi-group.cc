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
  tiledb::test::VFSTestSetup vfs_test_setup_;
  tiledb_ctx_t* ctx_c_;
  tiledb::Context ctx_;
  tiledb::VFS vfs_;

  // Functions
  GroupCPPFx();
  void create_array(const std::string& path) const;
  std::vector<tiledb::Object> read_group(const tiledb::Group& group) const;
  std::vector<std::tuple<tiledb::Object::Type, std::optional<std::string>>>
  read_group_details(const tiledb::Group& group) const;
  // compare type and name of objects, but not the URIs that do not match on
  // remote arrays
  void set_group_timestamp(
      tiledb::Group* group, const uint64_t& timestamp) const;
};

GroupCPPFx::GroupCPPFx()
    : ctx_c_(vfs_test_setup_.ctx_c)
    , ctx_(vfs_test_setup_.ctx())
    , vfs_(ctx_, vfs_test_setup_.vfs_c, false) {
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

std::vector<std::tuple<tiledb::Object::Type, std::optional<std::string>>>
GroupCPPFx::read_group_details(const tiledb::Group& group) const {
  std::vector<std::tuple<tiledb::Object::Type, std::optional<std::string>>> ret;
  uint64_t count = group.member_count();
  for (uint64_t i = 0; i < count; i++) {
    tiledb::Object obj = group.member(i);
    ret.emplace_back(obj.type(), obj.name());
  }
  return ret;
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
  REQUIRE(tiledb_array_create(ctx_c_, path.c_str(), array_schema) == TILEDB_OK);

  // Free objects
  tiledb_attribute_free(&a1);
  tiledb_dimension_free(&d1);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
}

TEST_CASE_METHOD(
    GroupCPPFx,
    "C++ API: Test creating group with config",
    "[cppapi][group][config][rest]") {
  std::string group1_uri = vfs_test_setup_.array_uri("group1");
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
    GroupCPPFx,
    "C++ API: Test group metadata",
    "[cppapi][group][metadata][rest]") {
  std::string group1_uri = vfs_test_setup_.array_uri("group1");

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

  // For some reason we don't yet allow group metadata consolidation so
  // disabling for the time being from REST testing
  if (!vfs_test_setup_.is_rest()) {
    // Consolidate and vacuum metadata with default config
    group.consolidate_metadata(ctx_, group1_uri);
    group.vacuum_metadata(ctx_, group1_uri);
  }

  // Close group
  group.close();
}

TEST_CASE_METHOD(
    GroupCPPFx,
    "C++ API: Group Metadata, write/read",
    "[cppapi][group][metadata][read][rest]") {
  // Create and open group in write mode
  std::string group1_uri = vfs_test_setup_.array_uri("group1");

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
}

TEST_CASE_METHOD(
    GroupCPPFx,
    "C++ API: Group on object storage with empty subfolders",
    "[cppapi][group][empty_subfolders]") {
  if (vfs_test_setup_.is_local()) {
    SKIP("Test only makes sense in object storage.");
  }

  std::string group1_uri = vfs_test_setup_.array_uri("group1");

  tiledb::Group::create(ctx_, group1_uri);
  tiledb::Group group(ctx_, group1_uri, TILEDB_WRITE);
  // Add non-existent member. Because we supply its type, we don't check if it
  // exists.
  group.add_member("my-array", true, "my-array", TILEDB_ARRAY);
  group.close();

  // Create an object with the same name as the __group directory.
  // It should be filtered out when listing.
  vfs_.touch(group1_uri + "/__group");

  // Try to read from the group with empty files
  // Note: MinIO will delete the actual commits if commits_uri is deleted,
  // per the s3 implementation limitation, making the group invalid
  try {
    group.open(TILEDB_READ);
  } catch (std::exception& e) {
    REQUIRE_THAT(
        e.what(), Catch::Matchers::ContainsSubstring("Cannot list given uri"));
  }
}

TEST_CASE_METHOD(
    GroupCPPFx,
    "C++ API: Group, set name",
    "[cppapi][group][read][rest-fails][sc-57867]") {
  std::string array1_uri = vfs_test_setup_.array_uri("array1");
  std::string array2_uri = vfs_test_setup_.array_uri("array2");
  std::string array3_uri = vfs_test_setup_.array_uri("array3");

  create_array(array1_uri);
  create_array(array2_uri);
  create_array(array3_uri);

  std::string group1_uri = vfs_test_setup_.array_uri("group1");
  std::string group2_uri = vfs_test_setup_.array_uri("group2");
  tiledb::Group::create(ctx_, group1_uri);
  tiledb::Group::create(ctx_, group2_uri);

  std::vector<tiledb::Object> group1_expected = {
      tiledb::Object(
          tiledb::Object::Type::Array,
          tiledb::sm::URI(array1_uri).to_string(),
          "array1"),
      tiledb::Object(
          tiledb::Object::Type::Array,
          tiledb::sm::URI(array2_uri).to_string(),
          "array2"),
      tiledb::Object(
          tiledb::Object::Type::Group,
          tiledb::sm::URI(group2_uri).to_string(),
          "group2"),
  };
  std::vector<std::tuple<tiledb::Object::Type, std::optional<std::string>>>
      group1_exp_det = {
          {tiledb::Object::Type::Array, "array1"},
          {tiledb::Object::Type::Array, "array2"},
          {tiledb::Object::Type::Group, "group2"}};
  std::vector<tiledb::Object> group2_expected = {
      tiledb::Object(
          tiledb::Object::Type::Array,
          tiledb::sm::URI(array3_uri).to_string(),
          "array3"),
  };
  std::vector<std::tuple<tiledb::Object::Type, std::optional<std::string>>>
      group2_exp_det = {{tiledb::Object::Type::Array, "array3"}};

  tiledb::Group group1(ctx_, group1_uri, TILEDB_WRITE);
  group1.close();
  set_group_timestamp(&group1, 1);
  group1.open(TILEDB_WRITE);

  tiledb::Group group2(ctx_, group2_uri, TILEDB_WRITE);
  group2.close();
  set_group_timestamp(&group2, 1);
  group2.open(TILEDB_WRITE);

  bool add_with_type = GENERATE(true, false);
  if (add_with_type) {
    group1.add_member(array1_uri, false, "array1", TILEDB_ARRAY);
    group1.add_member(array2_uri, false, "array2", TILEDB_ARRAY);
    group1.add_member(group2_uri, false, "group2", TILEDB_GROUP);
    group2.add_member(array3_uri, false, "array3", TILEDB_ARRAY);
  } else {
    group1.add_member(array1_uri, false, "array1");
    group1.add_member(array2_uri, false, "array2");
    group1.add_member(group2_uri, false, "group2");
    group2.add_member(array3_uri, false, "array3");
  }

  // Close group from write mode
  group1.close();
  group2.close();

  // Reopen in read mode
  set_group_timestamp(&group1, 1);
  group1.open(TILEDB_READ);
  set_group_timestamp(&group2, 1);
  group2.open(TILEDB_READ);

  // group URIs returned on remote array open are actually trasformed by the
  // REST server if the form `tiledb://UUID` so they don't match the initial
  // `tiledb://{namespace}/fs://temp_dir/name` format, so let's only compare the
  // other fields of the objects.
  if (vfs_test_setup_.is_rest()) {
    REQUIRE_THAT(
        read_group_details(group1),
        Catch::Matchers::UnorderedEquals(group1_exp_det));
    REQUIRE_THAT(
        read_group_details(group2),
        Catch::Matchers::UnorderedEquals(group2_exp_det));
  } else {
    REQUIRE_THAT(
        read_group(group1), Catch::Matchers::UnorderedEquals(group1_expected));
    REQUIRE_THAT(
        read_group(group2), Catch::Matchers::UnorderedEquals(group2_expected));
  }

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
  group1_exp_det.resize(group1_exp_det.size() - 1);

  group2.remove_member("array3");
  // There should be nothing left in group2
  group2_expected.clear();
  group2_exp_det.clear();

  // Close group
  group1.close();
  group2.close();

  // Check read again
  set_group_timestamp(&group1, 2);
  group1.open(TILEDB_READ);
  set_group_timestamp(&group2, 2);
  group2.open(TILEDB_READ);

  if (vfs_test_setup_.is_rest()) {
    REQUIRE_THAT(
        read_group_details(group1),
        Catch::Matchers::UnorderedEquals(group1_exp_det));
  } else {
    REQUIRE_THAT(
        read_group(group1), Catch::Matchers::UnorderedEquals(group1_expected));
    const auto& obj = group1.member(group1_expected[0].name().value());
    REQUIRE(obj == group1_expected[0]);
  }

  auto group2_received = read_group(group2);
  REQUIRE_THAT(
      group2_received, Catch::Matchers::UnorderedEquals(group2_expected));

  // Check that non-existent name throws
  REQUIRE_THROWS(group1.member("10"));
  // Checks for off by one indexing
  REQUIRE_THROWS(group1.member(group1_expected.size()));

  // Close group
  group1.close();
  group2.close();
}

TEST_CASE_METHOD(
    GroupCPPFx,
    "C++ API: Group, write/read",
    "[cppapi][group][read][rest-fails][sc-57858]") {
  // Create and open group in write mode
  std::string array1_uri = vfs_test_setup_.array_uri("array1");
  std::string array2_uri = vfs_test_setup_.array_uri("array2");
  std::string array3_uri = vfs_test_setup_.array_uri("array3");

  create_array(array1_uri);
  create_array(array2_uri);
  create_array(array3_uri);

  std::string group1_uri = vfs_test_setup_.array_uri("group1");
  std::string group2_uri = vfs_test_setup_.array_uri("group2");
  tiledb::Group::create(ctx_, group1_uri);
  tiledb::Group::create(ctx_, group2_uri);

  std::vector<tiledb::Object> group1_expected = {
      tiledb::Object(
          tiledb::Object::Type::Array,
          tiledb::sm::URI(array1_uri).to_string(),
          std::nullopt),
      tiledb::Object(
          tiledb::Object::Type::Array,
          tiledb::sm::URI(array2_uri).to_string(),
          std::nullopt),
      tiledb::Object(
          tiledb::Object::Type::Group,
          tiledb::sm::URI(group2_uri).to_string(),
          std::nullopt),
  };
  std::vector<std::tuple<tiledb::Object::Type, std::optional<std::string>>>
      group1_exp_det = {
          {tiledb::Object::Type::Array, std::nullopt},
          {tiledb::Object::Type::Array, std::nullopt},
          {tiledb::Object::Type::Group, std::nullopt}};
  std::vector<tiledb::Object> group2_expected = {
      tiledb::Object(
          tiledb::Object::Type::Array,
          tiledb::sm::URI(array3_uri).to_string(),
          std::nullopt),
  };
  std::vector<std::tuple<tiledb::Object::Type, std::optional<std::string>>>
      group2_exp_det = {{tiledb::Object::Type::Array, std::nullopt}};

  tiledb::Group group1(ctx_, group1_uri, TILEDB_WRITE);
  group1.close();
  set_group_timestamp(&group1, 1);
  group1.open(TILEDB_WRITE);

  tiledb::Group group2(ctx_, group2_uri, TILEDB_WRITE);
  group2.close();
  set_group_timestamp(&group2, 1);
  group2.open(TILEDB_WRITE);

  bool add_with_type = GENERATE(true, false);
  if (add_with_type) {
    group1.add_member(array1_uri, false, std::nullopt, TILEDB_ARRAY);
    group1.add_member(array2_uri, false, std::nullopt, TILEDB_ARRAY);
    group1.add_member(group2_uri, false, std::nullopt, TILEDB_GROUP);
    group2.add_member(array3_uri, false, std::nullopt, TILEDB_ARRAY);
  } else {
    group1.add_member(array1_uri, false);
    group1.add_member(array2_uri, false);
    group1.add_member(group2_uri, false);
    group2.add_member(array3_uri, false);
  }

  // Close group from write mode
  group1.close();
  group2.close();

  // Reopen in read mode
  group1.open(TILEDB_READ);
  group2.open(TILEDB_READ);

  // group URIs returned on remote array open are actually trasformed by the
  // REST server if the form `tiledb://UUID` so they don't match the initial
  // `tiledb://{namespace}/fs://temp_dir/name` format, so let's only compare the
  // other fields of the objects.
  if (vfs_test_setup_.is_rest()) {
    // Those checks fail for REST because of sc-57858: names are not empty as
    // they are supposed to but are equal to the REST Uri of the asset
    REQUIRE_THAT(
        read_group_details(group1),
        Catch::Matchers::UnorderedEquals(group1_exp_det));
    REQUIRE_THAT(
        read_group_details(group2),
        Catch::Matchers::UnorderedEquals(group2_exp_det));
  } else {
    REQUIRE_THAT(
        read_group(group1), Catch::Matchers::UnorderedEquals(group1_expected));
    REQUIRE_THAT(
        read_group(group2), Catch::Matchers::UnorderedEquals(group2_expected));
  }

  // Close group
  group1.close();
  group2.close();

  // Remove assets from group
  set_group_timestamp(&group1, 2);
  group1.open(TILEDB_WRITE);
  set_group_timestamp(&group2, 2);
  group2.open(TILEDB_WRITE);

  group1.remove_member(group2_uri);
  // Group is the latest element
  group1_expected.resize(group1_expected.size() - 1);
  group1_exp_det.resize(group1_exp_det.size() - 1);

  group2.remove_member(array3_uri);
  // There should be nothing left in group2
  group2_expected.clear();
  group2_exp_det.clear();

  // Close group
  group1.close();
  group2.close();

  // Check read again
  set_group_timestamp(&group1, 2);
  group1.open(TILEDB_READ);
  set_group_timestamp(&group2, 2);
  group2.open(TILEDB_READ);

  if (vfs_test_setup_.is_rest()) {
    REQUIRE_THAT(
        read_group_details(group1),
        Catch::Matchers::UnorderedEquals(group1_exp_det));
    REQUIRE_THAT(
        read_group_details(group2),
        Catch::Matchers::UnorderedEquals(group2_exp_det));
  } else {
    REQUIRE_THAT(
        read_group(group1), Catch::Matchers::UnorderedEquals(group1_expected));
    REQUIRE_THAT(
        read_group(group2), Catch::Matchers::UnorderedEquals(group2_expected));
  }

  // Check that out of bounds indexing throws
  REQUIRE_THROWS(group1.member(10));
  // Checks for off by one indexing
  REQUIRE_THROWS(group1.member(group1_expected.size()));

  // Close group
  group1.close();
  group2.close();
}

TEST_CASE_METHOD(
    GroupCPPFx,
    "C++ API: Group, write/read, relative",
    "[cppapi][group][read][non-rest]") {
  // Create and open group in write mode
  std::string group1_uri = vfs_test_setup_.array_uri("group1");
  std::string group2_uri = vfs_test_setup_.array_uri("group2");
  tiledb::Group::create(ctx_, group1_uri);
  tiledb::Group::create(ctx_, group2_uri);

  REQUIRE(
      tiledb_vfs_create_dir(
          ctx_.ptr().get(),
          vfs_test_setup_.vfs_c,
          (group1_uri + "/arrays").c_str()) == TILEDB_OK);
  REQUIRE(
      tiledb_vfs_create_dir(
          ctx_.ptr().get(),
          vfs_test_setup_.vfs_c,
          (group2_uri + "/arrays").c_str()) == TILEDB_OK);

  const std::string array1_relative_uri("arrays/array1");
  std::string array1_uri = vfs_test_setup_.array_uri("group1/arrays/array1");
  const std::string array2_relative_uri("arrays/array2");
  std::string array2_uri = vfs_test_setup_.array_uri("group1/arrays/array2");
  const std::string array3_relative_uri("arrays/array3");
  std::string array3_uri = vfs_test_setup_.array_uri("group2/arrays/array3");

  create_array(array1_uri);
  create_array(array2_uri);
  create_array(array3_uri);

  // Set expected
  std::vector<tiledb::Object> group1_expected = {
      tiledb::Object(
          tiledb::Object::Type::Array,
          tiledb::sm::URI(array1_uri).to_string(),
          std::nullopt),
      tiledb::Object(
          tiledb::Object::Type::Array,
          tiledb::sm::URI(array2_uri).to_string(),
          std::nullopt),
      tiledb::Object(
          tiledb::Object::Type::Group,
          tiledb::sm::URI(group2_uri).to_string(),
          std::nullopt),
  };
  std::vector<tiledb::Object> group2_expected = {
      tiledb::Object(
          tiledb::Object::Type::Array,
          tiledb::sm::URI(array3_uri).to_string(),
          std::nullopt),
  };

  tiledb::Group group1(ctx_, group1_uri, TILEDB_WRITE);
  group1.close();
  set_group_timestamp(&group1, 1);
  group1.open(TILEDB_WRITE);

  tiledb::Group group2(ctx_, group2_uri, TILEDB_WRITE);
  group2.close();
  set_group_timestamp(&group2, 1);
  group2.open(TILEDB_WRITE);

  bool add_with_type = GENERATE(true, false);
  if (add_with_type) {
    group1.add_member(array1_relative_uri, true, std::nullopt, TILEDB_ARRAY);
    group1.add_member(array2_relative_uri, true, std::nullopt, TILEDB_ARRAY);
    group1.add_member(group2_uri, false, std::nullopt, TILEDB_GROUP);
    group2.add_member(array3_relative_uri, true, std::nullopt, TILEDB_ARRAY);
  } else {
    group1.add_member(array1_relative_uri, true);
    group1.add_member(array2_relative_uri, true);
    group1.add_member(group2_uri, false);
    group2.add_member(array3_relative_uri, true);
  }

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

  group1.remove_member(group2_uri);
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
}

TEST_CASE_METHOD(
    GroupCPPFx,
    "C++ API: Group, write/read, relative named",
    "[cppapi][group][read][non-rest]") {
  bool remove_by_name = GENERATE(true, false);

  // Create and open group in write mode
  std::string group1_uri = vfs_test_setup_.array_uri("group1");
  std::string group2_uri = vfs_test_setup_.array_uri("group2");
  tiledb::Group::create(ctx_, group1_uri);
  tiledb::Group::create(ctx_, group2_uri);

  REQUIRE(
      tiledb_vfs_create_dir(
          ctx_.ptr().get(),
          vfs_test_setup_.vfs_c,
          (group1_uri + "/arrays").c_str()) == TILEDB_OK);
  REQUIRE(
      tiledb_vfs_create_dir(
          ctx_.ptr().get(),
          vfs_test_setup_.vfs_c,
          (group2_uri + "/arrays").c_str()) == TILEDB_OK);

  const std::string array1_relative_uri("arrays/array1");
  std::string array1_uri = vfs_test_setup_.array_uri("group1/arrays/array1");
  const std::string array2_relative_uri("arrays/array2");
  std::string array2_uri = vfs_test_setup_.array_uri("group1/arrays/array2");
  const std::string array3_relative_uri("arrays/array3");
  std::string array3_uri = vfs_test_setup_.array_uri("group2/arrays/array3");

  create_array(array1_uri);
  create_array(array2_uri);
  create_array(array3_uri);

  // Set expected
  std::vector<tiledb::Object> group1_expected = {
      tiledb::Object(
          tiledb::Object::Type::Array,
          tiledb::sm::URI(array1_uri).to_string(),
          "one"),
      tiledb::Object(
          tiledb::Object::Type::Array,
          tiledb::sm::URI(array2_uri).to_string(),
          "two"),
      tiledb::Object(
          tiledb::Object::Type::Group,
          tiledb::sm::URI(group2_uri).to_string(),
          "three"),
  };
  std::vector<tiledb::Object> group2_expected = {
      tiledb::Object(
          tiledb::Object::Type::Array,
          tiledb::sm::URI(array3_uri).to_string(),
          "four"),
  };

  tiledb::Group group1(ctx_, group1_uri, TILEDB_WRITE);
  group1.close();
  set_group_timestamp(&group1, 1);
  group1.open(TILEDB_WRITE);

  tiledb::Group group2(ctx_, group2_uri, TILEDB_WRITE);
  group2.close();
  set_group_timestamp(&group2, 1);
  group2.open(TILEDB_WRITE);

  bool add_with_type = GENERATE(true, false);
  if (add_with_type) {
    group1.add_member(array1_relative_uri, true, "one", TILEDB_ARRAY);
    group1.add_member(array2_relative_uri, true, "two", TILEDB_ARRAY);
    group1.add_member(group2_uri, false, "three", TILEDB_GROUP);
    group2.add_member(array3_relative_uri, true, "four", TILEDB_ARRAY);
  } else {
    group1.add_member(array1_relative_uri, true, "one");
    group1.add_member(array2_relative_uri, true, "two");
    group1.add_member(group2_uri, false, "three");
    group2.add_member(array3_relative_uri, true, "four");
  }

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
    group1.remove_member(group2_uri);
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
}

TEST_CASE_METHOD(
    GroupCPPFx,
    "C++ API: Group, delete by URI, duplicates",
    "[cppapi][group][delete][non-rest]") {
  bool nameless_uri = GENERATE(true, false);

  // Create and open group in write mode
  std::string group1_uri = vfs_test_setup_.array_uri("group1");
  tiledb::Group::create(ctx_, group1_uri);

  REQUIRE(
      tiledb_vfs_create_dir(
          ctx_.ptr().get(),
          vfs_test_setup_.vfs_c,
          (group1_uri + "/arrays").c_str()) == TILEDB_OK);

  const std::string array1_relative_uri("arrays/array1");
  std::string array1_uri = vfs_test_setup_.array_uri("group1/arrays/array1");
  const std::string array2_relative_uri("arrays/array2");
  std::string array2_uri = vfs_test_setup_.array_uri("group1/arrays/array2");

  create_array(array1_uri);
  create_array(array2_uri);

  // Set expected
  std::vector<tiledb::Object> group1_expected = {
      tiledb::Object(
          tiledb::Object::Type::Array,
          tiledb::sm::URI(array1_uri).to_string(),
          "one"),
      tiledb::Object(
          tiledb::Object::Type::Array,
          tiledb::sm::URI(array2_uri).to_string(),
          "two"),
      nameless_uri ? tiledb::Object(
                         tiledb::Object::Type::Array,
                         tiledb::sm::URI(array2_uri).to_string(),
                         nullopt) :
                     tiledb::Object(
                         tiledb::Object::Type::Array,
                         tiledb::sm::URI(array2_uri).to_string(),
                         "three"),
  };

  tiledb::Group group1(ctx_, group1_uri, TILEDB_WRITE);
  group1.close();
  set_group_timestamp(&group1, 1);
  group1.open(TILEDB_WRITE);

  bool add_with_type = GENERATE(true, false);
  if (add_with_type) {
    group1.add_member(array1_relative_uri, true, "one", TILEDB_ARRAY);
    group1.add_member(array2_relative_uri, true, "two", TILEDB_ARRAY);
    group1.add_member(
        array2_relative_uri,
        true,
        nameless_uri ? nullopt : std::make_optional<std::string>("three"),
        TILEDB_ARRAY);
  } else {
    group1.add_member(array1_relative_uri, true, "one");
    group1.add_member(array2_relative_uri, true, "two");
    group1.add_member(
        array2_relative_uri,
        true,
        nameless_uri ? nullopt : std::make_optional<std::string>("three"));
  }

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
}

TEST_CASE(
    "C++ API: Group delete recursive", "[cppapi][group][delete][recursive]") {
  // Initialize context and VFS.
  // NOTE: This test makes sense to only run on the local filesystem.
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);

  // Setup group structure
  tiledb::create_group(ctx, "my_group");
  tiledb::create_group(ctx, "my_group/my_subgroup");
  auto schema =
      tiledb::ArraySchema(ctx, TILEDB_DENSE)
          .set_domain(tiledb::Domain(ctx).add_dimension(
              tiledb::Dimension::create<int32_t>(ctx, "d1", {0, 100}, 10)))
          .add_attribute(tiledb::Attribute(ctx, "a1", TILEDB_INT32));
  tiledb::Array::create("my_group/my_array", schema);

  {
    // Add group members.
    tiledb::Group g{ctx, "my_group", TILEDB_WRITE};
    g.add_member("my_subgroup", true);
    g.add_member("my_array", true);
  }

  // Create some empty directories to make sure that deleting the group will not
  // delete them.
  bool add_empty_dirs = GENERATE(true, false);
  if (add_empty_dirs) {
    vfs.create_dir("my_group/my_subgroup/my_data");
    vfs.create_dir("my_group/my_array/my_data");
  }

  // Recursively delete the group.
  {
    tiledb::Group g{ctx, "my_group", TILEDB_MODIFY_EXCLUSIVE};
    g.delete_group("my_group", true);
  }

  if (add_empty_dirs) {
    // Check that the custom directories are preserved.
    REQUIRE(vfs.is_dir("my_group/my_subgroup/my_data"));
    REQUIRE(vfs.is_dir("my_group/my_array/my_data"));

    REQUIRE_FALSE(vfs.is_dir("my_group/my_array/__schema"));
    REQUIRE_FALSE(vfs.is_dir("my_group/my_subgroup/__tiledb_group.tdb"));

    // Cleanup
    vfs.remove_dir("my_group");
  } else {
    // Check that the entire directory and its subdirectories are gone.
    REQUIRE_FALSE(vfs.is_dir("my_group"));
  }
}

TEST_CASE(
    "C++ API: Group with relative URI members, write/read, rest",
    "[cppapi][group][create][relative][rest]") {
  VFSTestSetup vfs_test_setup;
  if (!vfs_test_setup.is_rest()) {
    // 3.0 REST must add the child asset as a group member for this to pass.
    // Legacy REST must return an error for this to pass.
    SKIP("This test relies on serverside REST logic");
  }
  tiledb::Context ctx{vfs_test_setup.ctx()};
  auto group_uri{vfs_test_setup.array_uri("groups_relative")};
  tiledb::create_group(ctx, group_uri);

  auto member_uri_str = group_uri + "/relative_group";
  if (vfs_test_setup.is_legacy_rest()) {
    // Legacy REST does not support relative group members.
    auto group = tiledb::Group(ctx, group_uri, TILEDB_WRITE);
    tiledb::create_group(ctx, member_uri_str);
    CHECK_NOTHROW(group.add_member("relative_group", true, "subgroup"));
    CHECK_THROWS_WITH(
        group.close(),
        Catch::Matchers::ContainsSubstring(
            "relative paths are not yet supported for cloud groups"));
  } else {
    // For 3.0 REST construct an asset path relative to the parent path above.
    // 3.0 REST will create a child asset under the parent group's asset path
    // _and_ register the asset as a member of `groups_relative`.
    tiledb::sm::URI::RESTURIComponents components;
    SECTION("Creating a group member as a child asset") {
      tiledb::sm::URI member_uri(member_uri_str);
      CHECK(
          member_uri
              .get_rest_components(vfs_test_setup.is_legacy_rest(), &components)
              .ok());
      // Append the child group to the server asset path in the URI.
      components.server_path.resize(components.server_path.find('/'));
      components.server_path += "/groups_relative";
      member_uri_str = "tiledb://" + components.server_namespace + "/" +
                       components.server_path + "/" + components.asset_storage;
      tiledb::create_group(ctx, member_uri_str);

      auto group = tiledb::Group(ctx, group_uri, TILEDB_READ);
      auto member = group.member("relative_group");
      CHECK(member.type() == tiledb::Object::Type::Group);
      CHECK(member.name() == "relative_group");
      std::string expected_uri =
          build_tiledb_uri(member_uri, "groups_relative/relative_group");
      CHECK(member.uri() == expected_uri);
      CHECK(group.is_relative("relative_group"));
    }

    SECTION("Creating an array member as a child asset") {
      tiledb::sm::URI member_uri(group_uri + "/relative_array");
      CHECK(
          member_uri
              .get_rest_components(vfs_test_setup.is_legacy_rest(), &components)
              .ok());
      // Append the child group to the server asset path in the URI.
      components.server_path.resize(components.server_path.find('/'));
      components.server_path += "/groups_relative";
      member_uri_str = "tiledb://" + components.server_namespace + "/" +
                       components.server_path + "/" + components.asset_storage;

      tiledb::Domain domain(ctx);
      domain.add_dimension(
          tiledb::Dimension::create<int>(ctx, "rows", {{1, 10}}, 10));
      tiledb::ArraySchema schema(ctx, TILEDB_SPARSE);
      schema.set_domain(domain);
      schema.add_attribute(tiledb::Attribute::create<int>(ctx, "a1"));
      tiledb::Array::create(ctx, member_uri_str, schema);

      auto group = tiledb::Group(ctx, group_uri, TILEDB_READ);
      auto member = group.member("relative_array");
      CHECK(member.type() == tiledb::Object::Type::Array);
      CHECK(member.name() == "relative_array");
      std::string expected_uri =
          build_tiledb_uri(member_uri, "groups_relative/relative_array");
      CHECK(member.uri() == expected_uri);
      CHECK(group.is_relative("relative_array"));
    }
  }
}

TEST_CASE(
    "C++ API: Group add_member with relative URI members, write/read, rest",
    "[cppapi][group][add_member][relative][rest]") {
  VFSTestSetup vfs_test_setup;
  tiledb::Context ctx{vfs_test_setup.ctx()};
  if (vfs_test_setup.is_legacy_rest()) {
    SKIP("Relative group URIs are not supported in legacy REST servers");
  }
  auto group_uri{vfs_test_setup.array_uri("groups_relative")};
  auto subgroup_uri = group_uri + "/relative_group";
  // Create parent group using tiledb URI.
  tiledb::create_group(ctx, group_uri);

  tiledb::sm::URI member_uri(subgroup_uri);
  SECTION("Create the child group using S3 URI") {
    tiledb::sm::URI::RESTURIComponents components;
    // Use the full backend storage URI if not in tiledb format.
    components.asset_storage = subgroup_uri;
    if (member_uri.is_tiledb()) {
      CHECK(
          member_uri
              .get_rest_components(vfs_test_setup.is_legacy_rest(), &components)
              .ok());
    }
    // Checks that we handle an unregistered group correctly.
    REQUIRE_NOTHROW(tiledb::create_group(ctx, components.asset_storage));
  }
  SECTION("Create the child group on REST using tiledb URI") {
    // Checks that we handle a pre-registered group correctly.
    REQUIRE_NOTHROW(tiledb::create_group(ctx, subgroup_uri));
  }
  SECTION("Adding the same relative member multiple times") {
    REQUIRE_NOTHROW(tiledb::create_group(ctx, subgroup_uri));
    auto group = tiledb::Group(ctx, group_uri, TILEDB_WRITE);
    CHECK_NOTHROW(group.add_member("relative_group", true));
    CHECK_NOTHROW(group.close());
  }
  SECTION("Create the child group using default storage") {
    tiledb::sm::URI::RESTURIComponents components;
    if (member_uri.is_tiledb()) {
      tiledb::VFS vfs(ctx);
      auto default_bucket = vfs_test_setup.default_bucket();
      if (!vfs.is_bucket(default_bucket)) {
        CHECK_NOTHROW(vfs.create_bucket(default_bucket));
        REQUIRE(vfs.is_bucket(default_bucket));
      }
      CHECK(
          member_uri
              .get_rest_components(vfs_test_setup.is_legacy_rest(), &components)
              .ok());
      auto default_storage = build_tiledb_uri(member_uri, "relative_array");
      REQUIRE_NOTHROW(tiledb::create_group(ctx, default_storage));
    }
  }

  // Open group in write mode and add the relative member.
  {
    auto group = tiledb::Group(ctx, group_uri, TILEDB_WRITE);
    CHECK_NOTHROW(group.add_member("relative_group", true, "relative_group"));
    CHECK_NOTHROW(
        group.add_member("relative_group", true, "relative_group_rename"));
    CHECK_NOTHROW(group.close());
  }

  auto group = tiledb::Group(ctx, group_uri, TILEDB_READ);
  CHECK(group.member_count() == 2);
  for (const auto& name : {"relative_group", "relative_group_rename"}) {
    auto member = group.member(name);
    CHECK(member.type() == tiledb::Object::Type::Group);
    CHECK(member.name() == name);
    std::string expected_uri = member_uri.to_string();
    if (vfs_test_setup.is_rest()) {
      expected_uri =
          build_tiledb_uri(member_uri, std::string("groups_relative/") + name);
    }
    CHECK(member.uri() == expected_uri);
    CHECK(group.is_relative(name));
  }
}

TEST_CASE(
    "C++ API: Group add_member with relative URI array members, write/read, "
    "rest",
    "[cppapi][group][add_member][relative][rest]") {
  VFSTestSetup vfs_test_setup;
  tiledb::Context ctx{vfs_test_setup.ctx()};
  if (vfs_test_setup.is_legacy_rest()) {
    SKIP("Relative group URIs are not supported in legacy REST servers");
  }
  auto group_uri{vfs_test_setup.array_uri("groups_relative")};
  auto array_member_uri = group_uri + "/relative_array";
  // Create parent group using tiledb URI.
  tiledb::create_group(ctx, group_uri);

  tiledb::Domain domain(ctx);
  domain.add_dimension(
      tiledb::Dimension::create<int>(ctx, "rows", {{1, 10}}, 10));
  tiledb::ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain);
  schema.add_attribute(tiledb::Attribute::create<int>(ctx, "a1"));
  // Create the group member using an S3 URI.
  tiledb::sm::URI member_uri(array_member_uri);
  SECTION("Create the child array using S3 URI") {
    tiledb::sm::URI::RESTURIComponents components;
    // Use the full backend storage URI if not in tiledb format.
    components.asset_storage = array_member_uri;
    if (member_uri.is_tiledb()) {
      CHECK(
          member_uri
              .get_rest_components(vfs_test_setup.is_legacy_rest(), &components)
              .ok());
    }
    // Checks that we handle an unregistered array correctly.
    REQUIRE_NOTHROW(
        tiledb::Array::create(ctx, components.asset_storage, schema));
  }
  SECTION("Create the child array on REST using tiledb URI") {
    // Checks that we handle a pre-registered array correctly.
    REQUIRE_NOTHROW(tiledb::Array::create(ctx, array_member_uri, schema));
  }
  SECTION("Adding the same relative member multiple times") {
    REQUIRE_NOTHROW(tiledb::Array::create(ctx, array_member_uri, schema));
    auto group = tiledb::Group(ctx, group_uri, TILEDB_WRITE);
    CHECK_NOTHROW(group.add_member("relative_array", true));
    CHECK_NOTHROW(group.close());
  }
  SECTION("Create the child array using default storage") {
    tiledb::sm::URI::RESTURIComponents components;
    if (member_uri.is_tiledb()) {
      tiledb::VFS vfs(ctx);
      auto default_bucket = vfs_test_setup.default_bucket();
      if (!vfs.is_bucket(default_bucket)) {
        CHECK_NOTHROW(vfs.create_bucket(default_bucket));
        REQUIRE(vfs.is_bucket(default_bucket));
      }
      CHECK(
          member_uri
              .get_rest_components(vfs_test_setup.is_legacy_rest(), &components)
              .ok());
      auto default_storage = build_tiledb_uri(member_uri, "relative_array");
      REQUIRE_NOTHROW(tiledb::Array::create(ctx, default_storage, schema));
    }
  }

  {
    // Open group in write mode and add the relative member.
    auto group = tiledb::Group(ctx, group_uri, TILEDB_WRITE);
    CHECK_NOTHROW(group.add_member("relative_array", true, "relative_array"));
    CHECK_NOTHROW(
        group.add_member("relative_array", true, "relative_array_rename"));
    CHECK_NOTHROW(group.close());
  }

  auto group = tiledb::Group(ctx, group_uri, TILEDB_READ);
  CHECK(group.member_count() == 2);
  for (const auto& name : {"relative_array", "relative_array_rename"}) {
    auto member = group.member(name);
    CHECK(member.type() == tiledb::Object::Type::Array);
    CHECK(member.name() == name);
    std::string expected_uri = member_uri.to_string();
    if (vfs_test_setup.is_rest()) {
      expected_uri =
          build_tiledb_uri(member_uri, std::string("groups_relative/") + name);
    }
    CHECK(member.uri() == expected_uri);
    CHECK(group.is_relative(name));
  }
}
