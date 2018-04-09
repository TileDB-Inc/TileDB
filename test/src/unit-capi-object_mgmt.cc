/**
 * @file   unit-capi-object_mgmt.cc
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
 * Tests for the C API object management code.
 */

#include "catch.hpp"

#ifdef _WIN32
#include "tiledb/sm/filesystem/win_filesystem.h"
#else
#include "tiledb/sm/filesystem/posix_filesystem.h"
#endif

#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/misc/utils.h"

#include <iostream>
#include <sstream>
#include <thread>

struct ObjectMgmtFx {
  const std::string HDFS_TEMP_DIR = "hdfs:///tiledb_test/";
  const std::string HDFS_FULL_TEMP_DIR = "hdfs://localhost:9000/tiledb_test/";
  const std::string S3_PREFIX = "s3://";
  const std::string S3_BUCKET = S3_PREFIX + random_bucket_name("tiledb") + "/";
  const std::string S3_TEMP_DIR = S3_BUCKET + "tiledb_test/";
#ifdef _WIN32
  const std::string FILE_URI_PREFIX = "";
  const std::string FILE_TEMP_DIR =
      tiledb::sm::win::current_dir() + "\\tiledb_test\\";
  const std::string FILE_FULL_TEMP_DIR = FILE_TEMP_DIR;
  const std::string GROUP = "group\\";
  const std::string ARRAY = "array\\";
#else
  const std::string FILE_URI_PREFIX = "file://";
  const std::string FILE_TEMP_DIR =
      tiledb::sm::posix::current_dir() + "/tiledb_test/";
  const std::string FILE_FULL_TEMP_DIR = std::string("file://") + FILE_TEMP_DIR;
  const std::string GROUP = "group/";
  const std::string ARRAY = "array/";
#endif

  // TileDB context
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;

  // Supported filesystems
  bool supports_s3_;
  bool supports_hdfs_;

  // Functions
  ObjectMgmtFx();
  ~ObjectMgmtFx();
  void check_object_type(const std::string& path);
  void check_delete(const std::string& path);
  void check_move(const std::string& path);
  void create_array(const std::string& path);
  void create_temp_dir(const std::string& path);
  void remove_temp_dir(const std::string& path);
  void create_hierarchy(const std::string& path);
  std::string get_golden_walk(const std::string& path);
  std::string get_golden_ls(const std::string& path);
  static int write_path(const char* path, tiledb_object_t type, void* data);
  static std::string random_bucket_name(const std::string& prefix);
  void set_supported_fs();
};

ObjectMgmtFx::ObjectMgmtFx() {
  // Supported filesystems
  set_supported_fs();

  // Create TileDB context
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_create(&config, &error) == TILEDB_OK);
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

  REQUIRE(tiledb_ctx_create(&ctx_, config) == TILEDB_OK);
  REQUIRE(error == nullptr);
  vfs_ = nullptr;
  REQUIRE(tiledb_vfs_create(ctx_, &vfs_, config) == TILEDB_OK);
  REQUIRE(tiledb_config_free(&config) == TILEDB_OK);

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

ObjectMgmtFx::~ObjectMgmtFx() {
  if (supports_s3_) {
    int is_bucket = 0;
    int rc = tiledb_vfs_is_bucket(ctx_, vfs_, S3_BUCKET.c_str(), &is_bucket);
    CHECK(rc == TILEDB_OK);
    if (is_bucket) {
      CHECK(
          tiledb_vfs_remove_bucket(ctx_, vfs_, S3_BUCKET.c_str()) == TILEDB_OK);
    }
  }

  CHECK(tiledb_vfs_free(ctx_, &vfs_) == TILEDB_OK);
  CHECK(tiledb_ctx_free(&ctx_) == TILEDB_OK);
}

void ObjectMgmtFx::set_supported_fs() {
  tiledb_ctx_t* ctx = nullptr;
  REQUIRE(tiledb_ctx_create(&ctx, nullptr) == TILEDB_OK);

  int is_supported = 0;
  int rc = tiledb_ctx_is_supported_fs(ctx, TILEDB_S3, &is_supported);
  REQUIRE(rc == TILEDB_OK);
  supports_s3_ = (bool)is_supported;
  rc = tiledb_ctx_is_supported_fs(ctx, TILEDB_HDFS, &is_supported);
  REQUIRE(rc == TILEDB_OK);
  supports_hdfs_ = (bool)is_supported;

  REQUIRE(tiledb_ctx_free(&ctx) == TILEDB_OK);
}

void ObjectMgmtFx::create_temp_dir(const std::string& path) {
  remove_temp_dir(path);
  REQUIRE(tiledb_vfs_create_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

void ObjectMgmtFx::remove_temp_dir(const std::string& path) {
  int is_dir = 0;
  REQUIRE(tiledb_vfs_is_dir(ctx_, vfs_, path.c_str(), &is_dir) == TILEDB_OK);
  if (is_dir)
    REQUIRE(tiledb_vfs_remove_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

void ObjectMgmtFx::create_array(const std::string& path) {
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
  tiledb_array_schema_create(ctx_, &array_schema, TILEDB_DENSE);
  tiledb_array_schema_set_domain(ctx_, array_schema, domain);
  tiledb_array_schema_add_attribute(ctx_, array_schema, a1);

  // Check array schema
  REQUIRE(tiledb_array_schema_check(ctx_, array_schema) == TILEDB_OK);

  // Create array
  REQUIRE(tiledb_array_create(ctx_, path.c_str(), array_schema) == TILEDB_OK);
  tiledb_dimension_free(ctx_, &d1);
}

void ObjectMgmtFx::check_object_type(const std::string& path) {
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

void ObjectMgmtFx::check_delete(const std::string& path) {
  std::string group, array, invalid;
  tiledb_object_t type;

  // Check simple delete
  group = path + "group/";
  CHECK(tiledb_object_remove(ctx_, group.c_str()) == TILEDB_OK);

  // Check invalid delete
  invalid = group + "foo";
  CHECK(tiledb_object_remove(ctx_, invalid.c_str()) == TILEDB_ERR);

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
  REQUIRE(tiledb_object_remove(ctx_, (group + "l1").c_str()) == TILEDB_OK);
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

void ObjectMgmtFx::check_move(const std::string& path) {
  // Move group
  auto group = path + "group/";
  auto old1 = group + "old1";
  auto old2 = group + "old2";
  auto new1 = group + "new1";
  auto new2 = group + "new2";
  REQUIRE(tiledb_group_create(ctx_, old1.c_str()) == TILEDB_OK);
  REQUIRE(tiledb_group_create(ctx_, old2.c_str()) == TILEDB_OK);
  CHECK(tiledb_object_move(ctx_, old1.c_str(), new1.c_str()) == TILEDB_OK);

  tiledb_object_t type;
  CHECK(tiledb_object_type(ctx_, new1.c_str(), &type) == TILEDB_OK);
  CHECK(type == TILEDB_GROUP);

  // Check move array
  auto array = group + ARRAY;
  auto array2 = group + "new_array";
  create_array(array);
  CHECK(tiledb_object_move(ctx_, array.c_str(), array2.c_str()) == TILEDB_OK);

  // Check error on invalid path
  auto inv1 = path + "invalid_path";
  auto inv2 = path + "new_invalid_path";
  CHECK(tiledb_object_move(ctx_, inv1.c_str(), inv2.c_str()) == TILEDB_ERR);
}

/**
 * Create the following directory hierarchy:
 * TEMP_DIR
 *    |_ dense_arrays
 *    |       |_ __tiledb_group.tdb
 *    |       |_ array_A
 *    |       |     |_ __array_schema.tdb
 *    |       |_ array_B
 *    |       |     |_ __array_schema.tdb
 *    |       |_ kv
 *    |             |_ __kv_schema.tdb
 *    |_ sparse_arrays
 *            |_ __tiledb_group.tdb
 *            |_ array_C
 *            |     |_ __array_schema.tdb
 *            |_ array_D
 *                  |_ __array_schema.tdb
 */
void ObjectMgmtFx::create_hierarchy(const std::string& path) {
  int rc = tiledb_vfs_create_dir(ctx_, vfs_, path.c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_create_dir(ctx_, vfs_, (path + "dense_arrays").c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_touch(
      ctx_, vfs_, (path + "dense_arrays/__tiledb_group.tdb").c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_create_dir(
      ctx_, vfs_, (path + "dense_arrays/array_A").c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_touch(
      ctx_, vfs_, (path + "dense_arrays/array_A/__array_schema.tdb").c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_create_dir(
      ctx_, vfs_, (path + "dense_arrays/array_B").c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_touch(
      ctx_, vfs_, (path + "dense_arrays/array_B/__array_schema.tdb").c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_create_dir(ctx_, vfs_, (path + "sparse_arrays").c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_touch(
      ctx_, vfs_, (path + "sparse_arrays/__tiledb_group.tdb").c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_create_dir(
      ctx_, vfs_, (path + "sparse_arrays/array_C").c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_touch(
      ctx_, vfs_, (path + "sparse_arrays/array_C/__array_schema.tdb").c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_create_dir(
      ctx_, vfs_, (path + "sparse_arrays/array_D").c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_touch(
      ctx_, vfs_, (path + "sparse_arrays/array_D/__array_schema.tdb").c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_create_dir(ctx_, vfs_, (path + "dense_arrays/kv").c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_touch(
      ctx_, vfs_, (path + "dense_arrays/kv/__kv_schema.tdb").c_str());
  REQUIRE(rc == TILEDB_OK);
}

std::string ObjectMgmtFx::get_golden_walk(const std::string& path) {
  std::string golden;

  // Preorder traversal
  golden += path + "dense_arrays GROUP\n";
  golden += path + "dense_arrays/array_A ARRAY\n";
  golden += path + "dense_arrays/array_B ARRAY\n";
  golden += path + "dense_arrays/kv KEY_VALUE\n";
  golden += path + "sparse_arrays GROUP\n";
  golden += path + "sparse_arrays/array_C ARRAY\n";
  golden += path + "sparse_arrays/array_D ARRAY\n";

  // Postorder traversal
  golden += path + "dense_arrays/array_A ARRAY\n";
  golden += path + "dense_arrays/array_B ARRAY\n";
  golden += path + "dense_arrays/kv KEY_VALUE\n";
  golden += path + "dense_arrays GROUP\n";
  golden += path + "sparse_arrays/array_C ARRAY\n";
  golden += path + "sparse_arrays/array_D ARRAY\n";
  golden += path + "sparse_arrays GROUP\n";

  return golden;
}

std::string ObjectMgmtFx::get_golden_ls(const std::string& path) {
  std::string golden;

  golden += path + "dense_arrays GROUP\n";
  golden += path + "sparse_arrays GROUP\n";

  return golden;
}

int ObjectMgmtFx::write_path(
    const char* path, tiledb_object_t type, void* data) {
  // Cast data to string
  auto* str = static_cast<std::string*>(data);

  // Simply print the path and type
  std::string path_str = path;
  if (path_str.back() == '/')
    path_str.pop_back();
  (*str) += path_str + " ";
  switch (type) {
    case TILEDB_ARRAY:
      (*str) += "ARRAY";
      break;
    case TILEDB_GROUP:
      (*str) += "GROUP";
      break;
    case TILEDB_KEY_VALUE:
      (*str) += "KEY_VALUE";
      break;
    default:
      (*str) += "INVALID";
  }
  (*str) += "\n";

  // Always iterate till the end
  return 1;
}

std::string ObjectMgmtFx::random_bucket_name(const std::string& prefix) {
  std::stringstream ss;
  ss << prefix << "-" << std::this_thread::get_id() << "-"
     << tiledb::sm::utils::timestamp_ms();
  return ss.str();
}

TEST_CASE_METHOD(
    ObjectMgmtFx,
    "C API: Test object management methods: object_type, delete, move",
    "[capi], [object], [delete], [move], [object_type]") {
  if (supports_s3_) {
    // S3
    create_temp_dir(S3_TEMP_DIR);
    check_object_type(S3_TEMP_DIR);
    check_delete(S3_TEMP_DIR);
    check_move(S3_TEMP_DIR);
    remove_temp_dir(S3_TEMP_DIR);
  } else if (supports_hdfs_) {
    // HDFS
    create_temp_dir(HDFS_TEMP_DIR);
    check_object_type(HDFS_TEMP_DIR);
    check_delete(HDFS_TEMP_DIR);
    check_move(HDFS_TEMP_DIR);
    remove_temp_dir(HDFS_TEMP_DIR);
  } else {
    // File
    create_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
    check_object_type(FILE_URI_PREFIX + FILE_TEMP_DIR);
    check_delete(FILE_URI_PREFIX + FILE_TEMP_DIR);
    check_move(FILE_URI_PREFIX + FILE_TEMP_DIR);
    remove_temp_dir(FILE_URI_PREFIX + FILE_TEMP_DIR);
  }
}

TEST_CASE_METHOD(
    ObjectMgmtFx,
    "C API: Test object management methods: walk, ls",
    "[capi], [object], [walk], [ls]") {
  std::string golden_walk, golden_ls;
  std::string walk_str, ls_str;
  int rc;

  if (supports_s3_) {
    // S3
    remove_temp_dir(S3_TEMP_DIR);
    create_hierarchy(S3_TEMP_DIR);
    golden_walk = get_golden_walk(S3_TEMP_DIR);
    golden_ls = get_golden_ls(S3_TEMP_DIR);
    walk_str.clear();
    ls_str.clear();
    rc = tiledb_object_walk(
        ctx_, S3_TEMP_DIR.c_str(), TILEDB_PREORDER, write_path, &walk_str);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_object_walk(
        ctx_, S3_TEMP_DIR.c_str(), TILEDB_POSTORDER, write_path, &walk_str);
    CHECK(rc == TILEDB_OK);
    CHECK_THAT(golden_walk, Catch::Equals(walk_str));
    rc = tiledb_object_ls(ctx_, S3_TEMP_DIR.c_str(), write_path, &ls_str);
    CHECK(rc == TILEDB_OK);
    CHECK_THAT(golden_ls, Catch::Equals(ls_str));
    remove_temp_dir(S3_TEMP_DIR);
  } else if (supports_hdfs_) {
    // HDFS
    remove_temp_dir(HDFS_TEMP_DIR);
    create_hierarchy(HDFS_TEMP_DIR);
    golden_walk = get_golden_walk(HDFS_FULL_TEMP_DIR);
    golden_ls = get_golden_ls(HDFS_FULL_TEMP_DIR);
    walk_str.clear();
    ls_str.clear();
    rc = tiledb_object_walk(
        ctx_, HDFS_TEMP_DIR.c_str(), TILEDB_PREORDER, write_path, &walk_str);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_object_walk(
        ctx_, HDFS_TEMP_DIR.c_str(), TILEDB_POSTORDER, write_path, &walk_str);
    CHECK(rc == TILEDB_OK);
    CHECK_THAT(golden_walk, Catch::Equals(walk_str));
    rc = tiledb_object_ls(ctx_, HDFS_TEMP_DIR.c_str(), write_path, &ls_str);
    CHECK(rc == TILEDB_OK);
    CHECK_THAT(golden_ls, Catch::Equals(ls_str));
    remove_temp_dir(HDFS_TEMP_DIR);
  } else {
    // File
    remove_temp_dir(FILE_FULL_TEMP_DIR);
    create_hierarchy(FILE_FULL_TEMP_DIR);
#ifdef _WIN32
    // `VFS::ls(...)` returns `file:///` URIs instead of Windows paths.
    golden_walk =
        get_golden_walk(tiledb::sm::win::uri_from_path(FILE_FULL_TEMP_DIR));
    golden_ls =
        get_golden_ls(tiledb::sm::win::uri_from_path(FILE_FULL_TEMP_DIR));
#else
    golden_walk = get_golden_walk(FILE_FULL_TEMP_DIR);
    golden_ls = get_golden_ls(FILE_FULL_TEMP_DIR);
#endif

    walk_str.clear();
    ls_str.clear();
    rc = tiledb_object_walk(
        ctx_,
        FILE_FULL_TEMP_DIR.c_str(),
        TILEDB_PREORDER,
        write_path,
        &walk_str);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_object_walk(
        ctx_,
        FILE_FULL_TEMP_DIR.c_str(),
        TILEDB_POSTORDER,
        write_path,
        &walk_str);
    CHECK(rc == TILEDB_OK);
    CHECK_THAT(golden_walk, Catch::Equals(walk_str));
    rc =
        tiledb_object_ls(ctx_, FILE_FULL_TEMP_DIR.c_str(), write_path, &ls_str);
    CHECK(rc == TILEDB_OK);
    CHECK_THAT(golden_ls, Catch::Equals(ls_str));
    remove_temp_dir(FILE_FULL_TEMP_DIR);
  }
}
