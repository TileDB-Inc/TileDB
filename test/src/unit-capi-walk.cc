/**
 * @file   unit-capi-walk.cc
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
 * Tests for the C API walk code.
 */

#include "catch.hpp"
#include "constants.h"
#include "posix_filesystem.h"
#include "tiledb.h"
#include "uri.h"

#include <iostream>

struct WalkFx {
#ifdef HAVE_HDFS
  const std::string HDFS_TEMP_DIR = "hdfs:///tiledb_test/";
  const std::string HDFS_FULL_TEMP_DIR = "hdfs://localhost:9000/tiledb_test/";
#endif
#ifdef HAVE_S3
  const tiledb::URI S3_BUCKET = tiledb::URI("s3://tiledb/");
  const std::string S3_TEMP_DIR = "s3://tiledb/tiledb_test/";
#endif
  const std::string FILE_TEMP_DIR = "tiledb_test/";
  const std::string FILE_FULL_TEMP_DIR =
      std::string("file://") + tiledb::posix::current_dir() + "/tiledb_test/";

  // TileDB context and VFS
  tiledb_ctx_t *ctx_;
  tiledb_vfs_t *vfs_;

  // Functions
  WalkFx();
  ~WalkFx();
  void remove_temp_dir(const std::string &path);
  void create_hierarchy(const std::string &path);
  std::string get_golden_output(const std::string &path);
  static int write_path(const char *path, tiledb_object_t type, void *data);
};

WalkFx::WalkFx() {
  // Create TileDB context
  tiledb_config_t *config = nullptr;
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

WalkFx::~WalkFx() {
  CHECK(tiledb_vfs_free(ctx_, vfs_) == TILEDB_OK);
  CHECK(tiledb_ctx_free(ctx_) == TILEDB_OK);
}

void WalkFx::remove_temp_dir(const std::string &path) {
  int is_dir = 0;
  REQUIRE(tiledb_vfs_is_dir(ctx_, vfs_, path.c_str(), &is_dir) == TILEDB_OK);
  if (is_dir)
    REQUIRE(tiledb_vfs_remove_dir(ctx_, vfs_, path.c_str()) == TILEDB_OK);
}

/**
 * Create the following directory hierarchy:
 * TEMP_DIR
 *    |_ dense_arrays
 *    |       |_ __tiledb_group.tdb
 *    |       |_ array_A
 *    |       |     |_ __array_metadata.tdb
 *    |       |_ array_B
 *    |       |     |_ __array_metadata.tdb
 *    |       |     |_ __array_metadata.tdb
 *    |       |_ kv
 *    |             |_ __kv.tdb
 *    |_ sparse_arrays
 *            |_ __tiledb_group.tdb
 *            |_ array_C
 *            |     |_ __array_metadata.tdb
 *            |_ array_D
 *                  |_ __array_metadata.tdb
 */
void WalkFx::create_hierarchy(const std::string &path) {
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
      ctx_, vfs_, (path + "dense_arrays/array_A/__array_metadata.tdb").c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_create_dir(
      ctx_, vfs_, (path + "dense_arrays/array_B").c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_touch(
      ctx_, vfs_, (path + "dense_arrays/array_B/__array_metadata.tdb").c_str());
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
      ctx_,
      vfs_,
      (path + "sparse_arrays/array_C/__array_metadata.tdb").c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_create_dir(
      ctx_, vfs_, (path + "sparse_arrays/array_D").c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_touch(
      ctx_,
      vfs_,
      (path + "sparse_arrays/array_D/__array_metadata.tdb").c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_create_dir(ctx_, vfs_, (path + "dense_arrays/kv").c_str());
  REQUIRE(rc == TILEDB_OK);
  rc =
      tiledb_vfs_touch(ctx_, vfs_, (path + "dense_arrays/kv/__kv.tdb").c_str());
  REQUIRE(rc == TILEDB_OK);
}

std::string WalkFx::get_golden_output(const std::string &path) {
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

int WalkFx::write_path(const char *path, tiledb_object_t type, void *data) {
  // Cast data to string
  auto *str = static_cast<std::string *>(data);

  // Simply print the path and type
  (*str) += (std::string(path) + " ");
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

TEST_CASE_METHOD(WalkFx, "C API: Test walk", "[capi], [walk]") {
  std::string golden;
  std::string walk_str;
  int rc;

  // File
  remove_temp_dir(FILE_FULL_TEMP_DIR);
  create_hierarchy(FILE_FULL_TEMP_DIR);
  golden = get_golden_output(FILE_FULL_TEMP_DIR);
  walk_str.clear();
  rc = tiledb_walk(
      ctx_, FILE_FULL_TEMP_DIR.c_str(), TILEDB_PREORDER, write_path, &walk_str);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_walk(
      ctx_,
      FILE_FULL_TEMP_DIR.c_str(),
      TILEDB_POSTORDER,
      write_path,
      &walk_str);
  CHECK(rc == TILEDB_OK);
  CHECK_THAT(golden, Catch::Equals(walk_str));
  remove_temp_dir(FILE_FULL_TEMP_DIR);

#ifdef HAVE_S3
  remove_temp_dir(S3_TEMP_DIR);
  create_hierarchy(S3_TEMP_DIR);
  golden = get_golden_output(S3_TEMP_DIR);
  walk_str.clear();
  rc = tiledb_walk(
      ctx_, S3_TEMP_DIR.c_str(), TILEDB_PREORDER, write_path, &walk_str);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_walk(
      ctx_, S3_TEMP_DIR.c_str(), TILEDB_POSTORDER, write_path, &walk_str);
  CHECK(rc == TILEDB_OK);
  CHECK_THAT(golden, Catch::Equals(walk_str));
  remove_temp_dir(S3_TEMP_DIR);
#endif

#ifdef HAVE_HDFS
  remove_temp_dir(HDFS_TEMP_DIR);
  create_hierarchy(HDFS_TEMP_DIR);
  golden = get_golden_output(HDFS_FULL_TEMP_DIR);
  walk_str.clear();
  rc = tiledb_walk(
      ctx_, HDFS_TEMP_DIR.c_str(), TILEDB_PREORDER, write_path, &walk_str);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_walk(
      ctx_, HDFS_TEMP_DIR.c_str(), TILEDB_POSTORDER, write_path, &walk_str);
  CHECK(rc == TILEDB_OK);
  CHECK_THAT(golden, Catch::Equals(walk_str));
  remove_temp_dir(HDFS_TEMP_DIR);
#endif
}
