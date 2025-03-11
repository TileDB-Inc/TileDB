/**
 * @file   unit-capi-object_mgmt.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB Inc.
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

#include <test/support/tdb_catch.h>
#include "test/support/src/helpers.h"
#include "test/support/src/vfs_helpers.h"
#ifdef _WIN32
#include "tiledb/sm/filesystem/path_win.h"
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif

#include "tiledb/sm/c_api/tiledb.h"

#include <iostream>
#include <sstream>
#include <thread>

using namespace tiledb::test;

struct ObjectMgmtFx {
  const std::string GROUP = "group/";
  const std::string ARRAY = "array/";

  // TileDB context
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;

  // Vector of supported filsystems
  const std::vector<std::unique_ptr<SupportedFs>>& fs_vec_;

  // Functions
  ObjectMgmtFx();
  ~ObjectMgmtFx();
  void check_object_type(const std::string& path);
  void check_delete(const std::string& path);
  void check_move(const std::string& path);
  void check_ls_1000(const std::string& path);
  void create_array(const std::string& path);
  void create_temp_dir(const std::string& path);
  void remove_temp_dir(const std::string& path);
  void create_hierarchy(const std::string& path);
  std::string get_golden_walk(const std::string& path);
  std::string get_golden_ls(const std::string& path);
  static int write_path(const char* path, tiledb_object_t type, void* data);
};

ObjectMgmtFx::ObjectMgmtFx()
    : fs_vec_(vfs_test_get_fs_vec()) {
  // Initialize vfs test
  REQUIRE(vfs_test_init(fs_vec_, &ctx_, &vfs_).ok());
}

ObjectMgmtFx::~ObjectMgmtFx() {
  // Close vfs test
  REQUIRE(vfs_test_close(fs_vec_, ctx_, vfs_).ok());
  tiledb_vfs_free(&vfs_);
  tiledb_ctx_free(&ctx_);
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
  REQUIRE(tiledb_array_create(ctx_, path.c_str(), array_schema) == TILEDB_OK);

  // Free objects
  tiledb_attribute_free(&a1);
  tiledb_dimension_free(&d1);
  tiledb_domain_free(&domain);
  tiledb_array_schema_free(&array_schema);
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

void ObjectMgmtFx::check_ls_1000(const std::string& path) {
  // Create a group
  auto group = path + std::string("group/");
  int rc = tiledb_group_create(ctx_, group.c_str());
  CHECK(rc == TILEDB_OK);

  // Create 1000 files inside the group
  for (int i = 0; i < 1000; ++i) {
    std::stringstream file;
    file << group << i;
    rc = tiledb_vfs_touch(ctx_, vfs_, file.str().c_str());
    CHECK(rc == TILEDB_OK);
  }

  // Check type
  tiledb_object_t type = TILEDB_INVALID;
  CHECK(tiledb_object_type(ctx_, group.c_str(), &type) == TILEDB_OK);
  CHECK(type == TILEDB_GROUP);
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
}

std::string ObjectMgmtFx::get_golden_walk(const std::string& path) {
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
    default:
      (*str) += "INVALID";
  }
  (*str) += "\n";

  // Always iterate till the end
  return 1;
}

TEST_CASE_METHOD(
    ObjectMgmtFx,
    "C API: Test object management methods: object_type, delete, move",
    "[capi][object][delete][move][object_type]") {
  // TODO: refactor for each supported FS.
  std::string temp_dir = fs_vec_[0]->temp_dir();
  create_temp_dir(temp_dir);
  check_object_type(temp_dir);
  check_delete(temp_dir);
  check_move(temp_dir);
  remove_temp_dir(temp_dir);
}

TEST_CASE_METHOD(
    ObjectMgmtFx,
    "C API: Test object management methods: walk, ls",
    "[capi][object][walk][ls]") {
  std::string golden_walk, golden_ls;
  std::string walk_str, ls_str;
  int rc;

  SupportedFs* const fs = fs_vec_[0].get();
  if (dynamic_cast<SupportedFsLocal*>(fs) != nullptr) {
    SupportedFsLocal local_fs;
    std::string local_dir = local_fs.file_prefix() + local_fs.temp_dir();
    remove_temp_dir(local_dir);
    create_hierarchy(local_dir);
#ifdef _WIN32
    // `VFS::ls(...)` returns `file:///` URIs instead of Windows paths.
    SupportedFsLocal windows_fs;
    std::string temp_dir = windows_fs.file_prefix() + windows_fs.temp_dir();
    golden_walk =
        get_golden_walk(tiledb::sm::path_win::uri_from_path(temp_dir));
    golden_ls = get_golden_ls(tiledb::sm::path_win::uri_from_path(temp_dir));
#else
    SupportedFsLocal posix_fs;
    std::string temp_dir = posix_fs.file_prefix() + posix_fs.temp_dir();
    golden_walk = get_golden_walk(temp_dir);
    golden_ls = get_golden_ls(temp_dir);
#endif

    walk_str.clear();
    ls_str.clear();
    rc = tiledb_object_walk(
        ctx_, temp_dir.c_str(), TILEDB_PREORDER, write_path, &walk_str);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_object_walk(
        ctx_, temp_dir.c_str(), TILEDB_POSTORDER, write_path, &walk_str);
    CHECK(rc == TILEDB_OK);
    CHECK_THAT(golden_walk, Catch::Matchers::Equals(walk_str));
    rc = tiledb_object_ls(ctx_, temp_dir.c_str(), write_path, &ls_str);
    CHECK(rc == TILEDB_OK);
    CHECK_THAT(golden_ls, Catch::Matchers::Equals(ls_str));
    remove_temp_dir(temp_dir);
  } else if (dynamic_cast<SupportedFsHDFS*>(fs) != nullptr) {
    std::string temp_dir = "hdfs://localhost:9000/tiledb_test/";
    remove_temp_dir(temp_dir);
    create_hierarchy(temp_dir);
    golden_walk = get_golden_walk(temp_dir);
    golden_ls = get_golden_ls(temp_dir);
    walk_str.clear();
    ls_str.clear();
    rc = tiledb_object_walk(
        ctx_, temp_dir.c_str(), TILEDB_PREORDER, write_path, &walk_str);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_object_walk(
        ctx_, temp_dir.c_str(), TILEDB_POSTORDER, write_path, &walk_str);
    CHECK(rc == TILEDB_OK);
    CHECK_THAT(golden_walk, Catch::Matchers::Equals(walk_str));
    rc = tiledb_object_ls(ctx_, temp_dir.c_str(), write_path, &ls_str);
    CHECK(rc == TILEDB_OK);
    CHECK_THAT(golden_ls, Catch::Matchers::Equals(ls_str));
    remove_temp_dir(temp_dir);
  } else {
    // TODO: refactor for each supported FS.
    std::string temp_dir = fs->temp_dir();
    remove_temp_dir(temp_dir);
    create_hierarchy(temp_dir);
    golden_walk = get_golden_walk(temp_dir);
    golden_ls = get_golden_ls(temp_dir);
    walk_str.clear();
    ls_str.clear();
    rc = tiledb_object_walk(
        ctx_, temp_dir.c_str(), TILEDB_PREORDER, write_path, &walk_str);
    CHECK(rc == TILEDB_OK);
    rc = tiledb_object_walk(
        ctx_, temp_dir.c_str(), TILEDB_POSTORDER, write_path, &walk_str);
    CHECK(rc == TILEDB_OK);
    CHECK_THAT(golden_walk, Catch::Matchers::Equals(walk_str));
    rc = tiledb_object_ls(ctx_, temp_dir.c_str(), write_path, &ls_str);
    CHECK(rc == TILEDB_OK);
    CHECK_THAT(golden_ls, Catch::Matchers::Equals(ls_str));
    remove_temp_dir(temp_dir);
  }
}

TEST_CASE_METHOD(
    ObjectMgmtFx,
    "C API: Test listing directory with >1000 objects on S3",
    "[capi][object][ls-1000]") {
  // TODO: refactor for each supported FS.
  std::string temp_dir = fs_vec_[0]->temp_dir();
  create_temp_dir(temp_dir);
  check_ls_1000(temp_dir);
  remove_temp_dir(temp_dir);
}
