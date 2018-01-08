/**
 * @file   unit-capi-resource_management.cc
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
 * Tests for the C API resource management code
 */

#include "catch.hpp"

#ifdef _WIN32
#include "win_filesystem.h"
#else
#include "posix_filesystem.h"
#endif

#include "tiledb.h"

struct ResourceMgmtFx {
#ifdef HAVE_HDFS
  const std::string HDFS_TEMP_DIR = "hdfs:///tiledb_test/";
#endif
#ifdef HAVE_S3
  const tiledb::URI S3_BUCKET = tiledb::URI("s3://tiledb");
  const std::string S3_TEMP_DIR = "s3://tiledb/tiledb_test/";
#endif
#ifdef _WIN32
  const std::string FILE_URI_PREFIX = "";
  const std::string FILE_TEMP_DIR =
      tiledb::win::current_dir() + "\\tiledb_test\\";
  const std::string GROUP = "group\\";
  const std::string ARRAY = "array\\";
#else
  const std::string FILE_URI_PREFIX = "file://";
  const std::string FILE_TEMP_DIR =
      tiledb::posix::current_dir() + "/tiledb_test/";
  const std::string GROUP = "group/";
  const std::string ARRAY = "array/";
#endif

  // TileDB context
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;

  // Functions
  ResourceMgmtFx();
  ~ResourceMgmtFx();
  void check_object_type(const std::string& path);
  void check_delete(const std::string& path);
  void check_move(const std::string& path);
  void create_array(const std::string& path);
  void create_temp_dir(const std::string& path);
  void remove_temp_dir(const std::string& path);
};

ResourceMgmtFx::ResourceMgmtFx() {
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

ResourceMgmtFx::~ResourceMgmtFx() {
  CHECK(tiledb_vfs_free(ctx_, vfs_) == TILEDB_OK);
  CHECK(tiledb_ctx_free(ctx_) == TILEDB_OK);
}

void ResourceMgmtFx::create_temp_dir(const std::string& path) {
  remove_temp_dir(path);
  REQUIRE(tiledb_vfs_create_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

void ResourceMgmtFx::remove_temp_dir(const std::string& path) {
  int is_dir = 0;
  REQUIRE(tiledb_vfs_is_dir(ctx_, vfs_, path.c_str(), &is_dir) == TILEDB_OK);
  if (is_dir)
    REQUIRE(tiledb_vfs_remove_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

void ResourceMgmtFx::create_array(const std::string& path) {
  tiledb_attribute_t* a1;
  tiledb_attribute_create(ctx_, &a1, "a1", TILEDB_FLOAT32);

  // Domain and tile extents
  int64_t dim_domain[2] = {1, 1};
  int64_t tile_extents[] = {1};

  // Create dimension
  tiledb_dimension_t* d1;
  tiledb_dimension_create(
      ctx_, &d1, "d1", TILEDB_INT64, &dim_domain[0], &tile_extents[0]);

  // Domain
  tiledb_domain_t* domain;
  tiledb_domain_create(ctx_, &domain);
  tiledb_domain_add_dimension(ctx_, domain, d1);

  // Create array schema
  tiledb_array_schema_t* array_schema;
  tiledb_array_schema_create(ctx_, &array_schema);
  tiledb_array_schema_set_array_type(ctx_, array_schema, TILEDB_DENSE);
  tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  tiledb_array_schema_add_attribute(ctx_, array_schema, a1);

  // Check array schema
  REQUIRE(tiledb_array_schema_check(ctx_, array_schema) == TILEDB_OK);

  // Create array
  REQUIRE(tiledb_array_create(ctx_, path.c_str(), array_schema) == TILEDB_OK);
  tiledb_dimension_free(ctx_, d1);
}

void ResourceMgmtFx::check_object_type(const std::string& path) {
  std::string group, array;
  tiledb_object_t type;

  // Check group
  group = path + "group/";
  REQUIRE(tiledb_group_create(ctx_, group.c_str()) == TILEDB_OK);
  REQUIRE(tiledb_object_type(ctx_, group.c_str(), &type) == TILEDB_OK);
  CHECK(type == TILEDB_GROUP);

  // Check invalid
  array = group + "array/";
  REQUIRE(tiledb_object_type(ctx_, array.c_str(), &type) == TILEDB_OK);
  CHECK(type == TILEDB_INVALID);

  // Check array
  create_array(array);
  REQUIRE(tiledb_object_type(ctx_, array.c_str(), &type) == TILEDB_OK);
  CHECK(type == TILEDB_ARRAY);
}

void ResourceMgmtFx::check_delete(const std::string& path) {
  std::string group, array, invalid;
  tiledb_object_t type;

  // Check simple delete
  group = path + "group/";
  CHECK(tiledb_delete(ctx_, group.c_str()) == TILEDB_OK);

  // Check invalid delete
  invalid = group + "foo";
  CHECK(tiledb_delete(ctx_, invalid.c_str()) == TILEDB_ERR);

  // Check recursive delete
  REQUIRE(tiledb_group_create(ctx_, group.c_str()) == TILEDB_OK);
  REQUIRE(tiledb_group_create(ctx_, (group + "l1").c_str()) == TILEDB_OK);
  REQUIRE(tiledb_group_create(ctx_, (group + "l1/l2").c_str()) == TILEDB_OK);
  REQUIRE(tiledb_group_create(ctx_, (group + "l1/l2/l3").c_str()) == TILEDB_OK);
  REQUIRE(tiledb_object_type(ctx_, (group + "l1").c_str(), &type) == TILEDB_OK);
  CHECK(type == TILEDB_GROUP);
  REQUIRE(
      tiledb_object_type(ctx_, (group + "l1/l2").c_str(), &type) == TILEDB_OK);
  CHECK(type == TILEDB_GROUP);
  REQUIRE(
      tiledb_object_type(ctx_, (group + "l1/l2/l3").c_str(), &type) ==
      TILEDB_OK);
  CHECK(type == TILEDB_GROUP);
  REQUIRE(tiledb_delete(ctx_, (group + "l1").c_str()) == TILEDB_OK);
  REQUIRE(
      tiledb_object_type(ctx_, (group + "l1/l2/l3").c_str(), &type) ==
      TILEDB_OK);
  CHECK(type == TILEDB_INVALID);
  REQUIRE(
      tiledb_object_type(ctx_, (group + "l1/l2").c_str(), &type) == TILEDB_OK);
  CHECK(type == TILEDB_INVALID);
  REQUIRE(tiledb_object_type(ctx_, (group + "l1").c_str(), &type) == TILEDB_OK);
  CHECK(type == TILEDB_INVALID);
}

void ResourceMgmtFx::check_move(const std::string& path) {
  // Move group
  auto group = path + "group/";
  auto old1 = group + "old1";
  auto old2 = group + "old2";
  auto new1 = group + "new1";
  auto new2 = group + "new2";
  REQUIRE(tiledb_group_create(ctx_, old1.c_str()) == TILEDB_OK);
  REQUIRE(tiledb_group_create(ctx_, old2.c_str()) == TILEDB_OK);
  CHECK(tiledb_move(ctx_, old1.c_str(), new1.c_str(), false) == TILEDB_OK);

  tiledb_object_t type;
  CHECK(tiledb_object_type(ctx_, new1.c_str(), &type) == TILEDB_OK);
  CHECK(type == TILEDB_GROUP);

  // Check error on name conflict
  CHECK(tiledb_move(ctx_, new1.c_str(), old2.c_str(), false) == TILEDB_ERR);

  // Check force move works on name conflict
  CHECK(tiledb_move(ctx_, new1.c_str(), old2.c_str(), true) == TILEDB_OK);

  // Check move array
  auto array = group + ARRAY;
  auto array2 = group + "new_array";
  create_array(array);
  CHECK(tiledb_move(ctx_, array.c_str(), array2.c_str(), false) == TILEDB_OK);

  // Check error on invalid path
  auto inv1 = path + "invalid_path";
  auto inv2 = path + "new_invalid_path";
  CHECK(tiledb_move(ctx_, inv1.c_str(), inv2.c_str(), false) == TILEDB_ERR);
}

TEST_CASE_METHOD(
    ResourceMgmtFx,
    "C API: Test resource management methods",
    "[capi], [resource]") {
  // File
  create_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
  check_object_type(FILE_URI_PREFIX + FILE_TEMP_DIR);
  check_delete(FILE_URI_PREFIX + FILE_TEMP_DIR);
  check_move(FILE_URI_PREFIX + FILE_TEMP_DIR);
  remove_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);

// S3
#ifdef HAVE_S3
  create_temp_dir(S3_TEMP_DIR);
  check_object_type(S3_TEMP_DIR);
  check_delete(S3_TEMP_DIR);
  check_move(S3_TEMP_DIR);
  remove_temp_dir(S3_TEMP_DIR);
#endif

// HDFS
#ifdef HAVE_HDFS
  create_temp_dir(HDFS_TEMP_DIR);
  check_object_type(HDFS_TEMP_DIR);
  check_delete(HDFS_TEMP_DIR);
  check_move(HDFS_TEMP_DIR);
  remove_temp_dir(HDFS_TEMP_DIR);
#endif
}
