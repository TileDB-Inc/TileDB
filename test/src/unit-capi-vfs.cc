/**
 * @file   unit-capi-vfs.cc
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
 * Tests the C API VFS object.
 */

#include "catch.hpp"
#include "posix_filesystem.h"
#include "tiledb.h"

#include <iostream>

struct VFSFx {
#ifdef HAVE_HDFS
  const std::string HDFS_TEMP_DIR = "hdfs:///tiledb_test/";
#endif
#ifdef HAVE_S3
  const std::string S3_BUCKET = "s3://tiledb/";
  const std::string S3_TEMP_DIR = "s3://tiledb/tiledb_test/";
#endif
  const std::string FILE_TEMP_DIR =
      std::string("file://") + tiledb::posix::current_dir() + "/tiledb_test/";

  // TileDB context and vfs
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;

  // Functions
  VFSFx();
  ~VFSFx();
  void check_vfs(const std::string& path);
  void check_write(const std::string& path);
  void check_move(const std::string& path);
  void check_read(const std::string& path);
  void check_append(const std::string& path);
  void check_append_after_sync(const std::string& path);
};

VFSFx::VFSFx() {
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
  int rc = tiledb_vfs_create(ctx_, &vfs_, nullptr);
  REQUIRE(rc == TILEDB_OK);
}

VFSFx::~VFSFx() {
  CHECK(tiledb_vfs_free(ctx_, vfs_) == TILEDB_OK);
  CHECK(tiledb_ctx_free(ctx_) == TILEDB_OK);
}

void VFSFx::check_vfs(const std::string& path) {
#ifdef HAVE_S3
  // Check S3 bucket functionality
  if (path == S3_TEMP_DIR) {
    int is_bucket = 0;
    int rc = tiledb_vfs_is_bucket(ctx_, vfs_, S3_BUCKET.c_str(), &is_bucket);
    REQUIRE(rc == TILEDB_OK);
    if (is_bucket) {
      rc = tiledb_vfs_remove_bucket(ctx_, vfs_, S3_BUCKET.c_str());
      REQUIRE(rc == TILEDB_OK);
    }
    rc = tiledb_vfs_is_bucket(ctx_, vfs_, S3_BUCKET.c_str(), &is_bucket);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(!is_bucket);

    rc = tiledb_vfs_create_bucket(ctx_, vfs_, S3_BUCKET.c_str());
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_vfs_is_bucket(ctx_, vfs_, S3_BUCKET.c_str(), &is_bucket);
    REQUIRE(is_bucket);
  }
#endif

  // Create director, is directory, remove directory
  int is_dir = 0;
  int rc = tiledb_vfs_is_dir(ctx_, vfs_, path.c_str(), &is_dir);
  REQUIRE(rc == TILEDB_OK);
  if (is_dir) {
    rc = tiledb_vfs_remove_dir(ctx_, vfs_, path.c_str());
    REQUIRE(rc == TILEDB_OK);
  }
  rc = tiledb_vfs_is_dir(ctx_, vfs_, path.c_str(), &is_dir);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(!is_dir);
  rc = tiledb_vfs_create_dir(ctx_, vfs_, path.c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_is_dir(ctx_, vfs_, path.c_str(), &is_dir);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_dir);

  // Remove directory recursively
  auto subdir = path + "subdir/";
  rc = tiledb_vfs_create_dir(ctx_, vfs_, subdir.c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_is_dir(ctx_, vfs_, path.c_str(), &is_dir);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_dir);
  rc = tiledb_vfs_remove_dir(ctx_, vfs_, path.c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_is_dir(ctx_, vfs_, path.c_str(), &is_dir);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(!is_dir);
  rc = tiledb_vfs_is_dir(ctx_, vfs_, subdir.c_str(), &is_dir);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(!is_dir);

  // Move
  rc = tiledb_vfs_create_dir(ctx_, vfs_, path.c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_create_dir(ctx_, vfs_, subdir.c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_is_dir(ctx_, vfs_, subdir.c_str(), &is_dir);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_dir);
  auto subdir2 = path + "subdir2/";
  rc = tiledb_vfs_move(ctx_, vfs_, subdir.c_str(), subdir2.c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_is_dir(ctx_, vfs_, subdir.c_str(), &is_dir);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(!is_dir);
  rc = tiledb_vfs_is_dir(ctx_, vfs_, subdir2.c_str(), &is_dir);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_dir);

  // Invalid file
  int is_file = 0;
  void* buffer = nullptr;
  uint64_t offset = 0;
  uint64_t nbytes = 10;
  std::string foo_file = path + "foo";
  rc = tiledb_vfs_is_file(ctx_, vfs_, foo_file.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(!is_file);
  rc = tiledb_vfs_read(ctx_, vfs_, foo_file.c_str(), offset, buffer, nbytes);
  REQUIRE(rc == TILEDB_ERR);

  // Touch file
  rc = tiledb_vfs_touch(ctx_, vfs_, foo_file.c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_is_file(ctx_, vfs_, foo_file.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_file);
  rc = tiledb_vfs_remove_file(ctx_, vfs_, foo_file.c_str());
  REQUIRE(rc == TILEDB_OK);

  // Check write and append
  check_write(path);
  check_append(path);
  check_append_after_sync(path);

  // Read file
  check_read(path);

  // Move
  check_move(path);

  // Check if filesystem is supported
  int supports = 0;
  rc = tiledb_vfs_supports_fs(ctx_, vfs_, TILEDB_HDFS, &supports);
  CHECK(rc == TILEDB_OK);
#ifdef HAVE_HDFS
  CHECK(supports);
#else
  CHECK(!supports);
#endif
  rc = tiledb_vfs_supports_fs(ctx_, vfs_, TILEDB_S3, &supports);
  CHECK(rc == TILEDB_OK);
#ifdef HAVE_S3
  CHECK(supports);
#else
  CHECK(!supports);
#endif

  // Clean up
  rc = tiledb_vfs_remove_dir(ctx_, vfs_, path.c_str());
  REQUIRE(rc == TILEDB_OK);

#ifdef HAVE_S3
  if (path == S3_TEMP_DIR) {
    rc = tiledb_vfs_remove_bucket(ctx_, vfs_, S3_BUCKET.c_str());
    REQUIRE(rc == TILEDB_OK);
  }
#endif
}

void VFSFx::check_move(const std::string& path) {
  // Move and remove file
  auto file = path + "file";
  auto file2 = path + "file2";
  int is_file = 0;
  int rc = tiledb_vfs_touch(ctx_, vfs_, file.c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_is_file(ctx_, vfs_, file.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_file);
  rc = tiledb_vfs_move(ctx_, vfs_, file.c_str(), file2.c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_is_file(ctx_, vfs_, file.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(!is_file);
  rc = tiledb_vfs_is_file(ctx_, vfs_, file2.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_file);
  rc = tiledb_vfs_remove_file(ctx_, vfs_, file2.c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_is_file(ctx_, vfs_, file2.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(!is_file);

  // Move directory with subdirectories and files
  auto dir = path + "dir/";
  auto dir2 = path + "dir2/";
  auto subdir = path + "dir/subdir/";
  auto subdir2 = path + "dir2/subdir/";
  file = dir + "file";
  file2 = subdir + "file2";
  auto new_file = dir2 + "file";
  auto new_file2 = subdir2 + "file2";
  int is_dir = 0;
  rc = tiledb_vfs_create_dir(ctx_, vfs_, dir.c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_is_dir(ctx_, vfs_, dir.c_str(), &is_dir);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_dir);
  rc = tiledb_vfs_create_dir(ctx_, vfs_, subdir.c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_is_dir(ctx_, vfs_, subdir.c_str(), &is_dir);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_dir);
  rc = tiledb_vfs_touch(ctx_, vfs_, file.c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_is_file(ctx_, vfs_, file.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_file);
  rc = tiledb_vfs_touch(ctx_, vfs_, file2.c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_is_file(ctx_, vfs_, file2.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_file);
  rc = tiledb_vfs_move(ctx_, vfs_, dir.c_str(), dir2.c_str());
  REQUIRE(rc == TILEDB_OK);

  rc = tiledb_vfs_is_dir(ctx_, vfs_, dir.c_str(), &is_dir);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(!is_dir);
  rc = tiledb_vfs_is_dir(ctx_, vfs_, subdir.c_str(), &is_dir);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(!is_dir);
  rc = tiledb_vfs_is_file(ctx_, vfs_, file.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(!is_file);
  rc = tiledb_vfs_is_file(ctx_, vfs_, file2.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(!is_file);

  rc = tiledb_vfs_is_dir(ctx_, vfs_, dir2.c_str(), &is_dir);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_dir);
  rc = tiledb_vfs_is_dir(ctx_, vfs_, subdir2.c_str(), &is_dir);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_dir);
  rc = tiledb_vfs_is_file(ctx_, vfs_, new_file.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_file);
  rc = tiledb_vfs_is_file(ctx_, vfs_, new_file2.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_file);

  // Move from one bucket to another (only for S3)
#ifdef HAVE_S3
  if (path == S3_TEMP_DIR) {
    std::string bucket2 = "s3://tiledb2/";
    std::string subdir3 = bucket2 + "tiledb_test/subdir3/";
    std::string file3 = subdir3 + "file2";
    int is_bucket = 0;

    rc = tiledb_vfs_is_bucket(ctx_, vfs_, bucket2.c_str(), &is_bucket);
    REQUIRE(rc == TILEDB_OK);
    if (is_bucket) {
      rc = tiledb_vfs_remove_bucket(ctx_, vfs_, bucket2.c_str());
      REQUIRE(rc == TILEDB_OK);
    }

    rc = tiledb_vfs_create_bucket(ctx_, vfs_, bucket2.c_str());
    REQUIRE(rc == TILEDB_OK);

    rc = tiledb_vfs_move(ctx_, vfs_, subdir2.c_str(), subdir3.c_str());
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_vfs_is_file(ctx_, vfs_, file3.c_str(), &is_file);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(is_file);

    rc = tiledb_vfs_remove_bucket(ctx_, vfs_, bucket2.c_str());
    REQUIRE(rc == TILEDB_OK);
  }
#endif
}

void VFSFx::check_write(const std::string& path) {
  // File write and file size
  int is_file = 0;
  auto file = path + "file";
  int rc = tiledb_vfs_is_file(ctx_, vfs_, file.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  if (is_file) {
    rc = tiledb_vfs_remove_file(ctx_, vfs_, file.c_str());
    REQUIRE(rc == TILEDB_OK);
  }
  rc = tiledb_vfs_is_file(ctx_, vfs_, file.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(!is_file);
  std::string to_write = "This will be written to the file";
  rc = tiledb_vfs_write(
      ctx_, vfs_, file.c_str(), to_write.c_str(), to_write.size());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_sync(ctx_, vfs_, file.c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_is_file(ctx_, vfs_, file.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_file);
  uint64_t file_size = 0;
  rc = tiledb_vfs_file_size(ctx_, vfs_, file.c_str(), &file_size);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(file_size == to_write.size());

  // Check correctness with read
  std::string to_read;
  to_read.resize(to_write.size());
  rc = tiledb_vfs_read(ctx_, vfs_, file.c_str(), 0, &to_read[0], file_size);
  REQUIRE(rc == TILEDB_OK);
  CHECK_THAT(to_read, Catch::Equals(to_write));

  // Remove file
  rc = tiledb_vfs_remove_file(ctx_, vfs_, file.c_str());
  REQUIRE(rc == TILEDB_OK);
}

void VFSFx::check_append(const std::string& path) {
  // File write and file size
  auto file = path + "file";
  std::string to_write = "This will be written to the file";
  int rc = tiledb_vfs_write(
      ctx_, vfs_, file.c_str(), to_write.c_str(), to_write.size());
  REQUIRE(rc == TILEDB_OK);
  std::string to_write_2 = "This will be appended to the end of the file";
  rc = tiledb_vfs_write(
      ctx_, vfs_, file.c_str(), to_write_2.c_str(), to_write_2.size());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_sync(ctx_, vfs_, file.c_str());
  REQUIRE(rc == TILEDB_OK);
  uint64_t file_size = 0;
  rc = tiledb_vfs_file_size(ctx_, vfs_, file.c_str(), &file_size);
  REQUIRE(rc == TILEDB_OK);
  uint64_t total_size = to_write.size() + to_write_2.size();
  CHECK(file_size == total_size);

  // Check correctness with read
  std::string to_read;
  to_read.resize(total_size);
  rc = tiledb_vfs_read(ctx_, vfs_, file.c_str(), 0, &to_read[0], total_size);
  REQUIRE(rc == TILEDB_OK);
  CHECK_THAT(to_read, Catch::Equals(to_write + to_write_2));

  // Remove file
  rc = tiledb_vfs_remove_file(ctx_, vfs_, file.c_str());
  REQUIRE(rc == TILEDB_OK);
}

void VFSFx::check_read(const std::string& path) {
  auto file = path + "file";
  int is_file = 0;
  int rc = tiledb_vfs_is_file(ctx_, vfs_, file.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  if (is_file) {
    rc = tiledb_vfs_remove_file(ctx_, vfs_, file.c_str());
    REQUIRE(rc == TILEDB_OK);
  }
  std::string to_write = "This will be written to the file";
  rc = tiledb_vfs_write(
      ctx_, vfs_, file.c_str(), to_write.c_str(), to_write.size());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_sync(ctx_, vfs_, file.c_str());
  REQUIRE(rc == TILEDB_OK);

  // Read only the "will be written" portion of the file
  std::string to_check = "will be written";
  std::string to_read;
  to_read.resize(to_check.size());
  uint64_t offset = 5;
  rc = tiledb_vfs_read(
      ctx_, vfs_, file.c_str(), offset, &to_read[0], to_check.size());
  REQUIRE(rc == TILEDB_OK);
  CHECK_THAT(to_read, Catch::Equals(to_check));

  // Remove file
  rc = tiledb_vfs_remove_file(ctx_, vfs_, file.c_str());
  REQUIRE(rc == TILEDB_OK);
}

void VFSFx::check_append_after_sync(const std::string& path) {
  (void)path;
  /*
  // File write and file size
  auto file = path + "file";
  std::string to_write = "This will be written to the file";
  int rc = tiledb_vfs_write(ctx_, vfs_, file.c_str(), to_write.c_str(),
to_write.size()); REQUIRE(rc == TILEDB_OK);
//  rc = tiledb_vfs_sync(ctx_, vfs, file.c_str());
//  REQUIRE(rc == TILEDB_OK);
//  rc = tiledb_vfs_is_file(ctx_, vfs, file.c_str(), &is_file);
//  REQUIRE(rc == TILEDB_OK);
//  REQUIRE(is_file);
  uint64_t file_size = 0;
//  rc = tiledb_vfs_file_size(ctx_, vfs, file.c_str(), &file_size);
//  REQUIRE(rc == TILEDB_OK);
//  REQUIRE(file_size == to_write.size());
  std::string to_write_2 = "This will be appended to the end of the file";
  rc = tiledb_vfs_write(ctx_, vfs, file.c_str(), to_write_2.c_str(),
to_write_2.size()); REQUIRE(rc == TILEDB_OK); rc = tiledb_vfs_sync(ctx_, vfs,
file.c_str()); REQUIRE(rc == TILEDB_OK); rc = tiledb_vfs_file_size(ctx_, vfs,
file.c_str(), &file_size); REQUIRE(rc == TILEDB_OK);

  CHECK(file_size == to_write.size() + to_write_2.size());

  rc = tiledb_vfs_remove_file(ctx_, vfs_, file.c_str());
  REQUIRE(rc == TILEDB_OK);
   */
}

TEST_CASE_METHOD(VFSFx, "C API: Test virtual filesystem", "[capi], [vfs]") {
  check_vfs(FILE_TEMP_DIR);

#ifdef HAVE_S3
  // S3
  check_vfs(S3_TEMP_DIR);
#endif

#ifdef HAVE_HDFS
  // HDFS
  check_vfs(HDFS_TEMP_DIR);
#endif
}

#ifndef HAVE_S3
TEST_CASE_METHOD(
    VFSFx,
    "C API: Test virtual filesystem when S3 is not supported",
    "[capi], [vfs]") {
  tiledb_vfs_t* vfs;
  int rc = tiledb_vfs_create(ctx_, &vfs, nullptr);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_create_bucket(ctx_, vfs, "s3://foo");
  REQUIRE(rc == TILEDB_ERR);
  rc = tiledb_vfs_free(ctx_, vfs);
  CHECK(rc == TILEDB_OK);
}
#endif
