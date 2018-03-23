/**
 * @file   unit-capi-vfs.cc
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
 * Tests the C API VFS object.
 */

#include "catch.hpp"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/misc/stats.h"
#include "tiledb/sm/misc/utils.h"
#ifdef _WIN32
#include "tiledb/sm/filesystem/win_filesystem.h"
#else
#include "tiledb/sm/filesystem/posix_filesystem.h"
#endif

#include <iostream>
#include <sstream>
#include <thread>

struct VFSFx {
  const std::string HDFS_TEMP_DIR = "hdfs:///tiledb_test/";
  const std::string S3_PREFIX = "s3://";
  const std::string S3_BUCKET = S3_PREFIX + random_bucket_name("tiledb") + "/";
  const std::string S3_TEMP_DIR = S3_BUCKET + "tiledb_test/";
#ifdef _WIN32
  const std::string FILE_TEMP_DIR =
      tiledb::sm::win::current_dir() + "\\tiledb_test\\";
#else
  const std::string FILE_TEMP_DIR = std::string("file://") +
                                    tiledb::sm::posix::current_dir() +
                                    "/tiledb_test/";
#endif

  // TileDB context and vfs
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;

  // Supported filesystems
  bool supports_s3_;
  bool supports_hdfs_;

  // Functions
  VFSFx();
  ~VFSFx();
  void check_vfs(const std::string& path);
  void check_write(const std::string& path);
  void check_move(const std::string& path);
  void check_read(const std::string& path);
  void check_append(const std::string& path);
  static std::string random_bucket_name(const std::string& prefix);
  void set_supported_fs();
  void set_num_vfs_threads(unsigned num_threads);
};

VFSFx::VFSFx() {
  ctx_ = nullptr;
  vfs_ = nullptr;

  // Supported filesystems
  set_supported_fs();

  // Create context and VFS with 1 thread
  set_num_vfs_threads(1);
}

VFSFx::~VFSFx() {
  CHECK(tiledb_vfs_free(ctx_, &vfs_) == TILEDB_OK);
  CHECK(tiledb_ctx_free(&ctx_) == TILEDB_OK);
}

void VFSFx::set_supported_fs() {
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

void VFSFx::set_num_vfs_threads(unsigned num_threads) {
  if (vfs_ != nullptr) {
    CHECK(tiledb_vfs_free(ctx_, &vfs_) == TILEDB_OK);
  }
  if (ctx_ != nullptr) {
    CHECK(tiledb_ctx_free(&ctx_) == TILEDB_OK);
  }

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

  // Set number of threads
  REQUIRE(
      tiledb_config_set(
          config,
          "vfs.max_parallel_ops",
          std::to_string(num_threads).c_str(),
          &error) == TILEDB_OK);
  // Set very small parallelization threshold (ignored when there is only 1
  // thread).
  REQUIRE(
      tiledb_config_set(
          config, "vfs.min_parallel_size", std::to_string(1).c_str(), &error) ==
      TILEDB_OK);
  REQUIRE(error == nullptr);

  REQUIRE(tiledb_ctx_create(&ctx_, config) == TILEDB_OK);
  REQUIRE(error == nullptr);
  int rc = tiledb_vfs_create(ctx_, &vfs_, config);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(tiledb_config_free(&config) == TILEDB_OK);
}

void VFSFx::check_vfs(const std::string& path) {
  if (supports_s3_) {
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
  }

  // Create directory, is directory, remove directory
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
  if (path == S3_TEMP_DIR)
    REQUIRE(!is_dir);  // No empty dirs exist in S3
  else
    REQUIRE(is_dir);
  // Second time succeeds as well
  rc = tiledb_vfs_create_dir(ctx_, vfs_, path.c_str());
  REQUIRE(rc == TILEDB_OK);

  // Remove directory recursively
  auto subdir = path + "subdir/";
  rc = tiledb_vfs_create_dir(ctx_, vfs_, subdir.c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_is_dir(ctx_, vfs_, path.c_str(), &is_dir);
  REQUIRE(rc == TILEDB_OK);
  if (path == S3_TEMP_DIR)
    REQUIRE(!is_dir);  // No empty dirs exist in S3
  else
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
  if (path == S3_TEMP_DIR)
    REQUIRE(!is_dir);  // No empty dirs exist in S3
  else
    REQUIRE(is_dir);
  int is_file = 0;
  std::string some_file = subdir + "some_file";
  rc = tiledb_vfs_touch(ctx_, vfs_, some_file.c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_is_file(ctx_, vfs_, some_file.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  auto subdir2 = path + "subdir2/";
  rc = tiledb_vfs_move_dir(ctx_, vfs_, subdir.c_str(), subdir2.c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_is_dir(ctx_, vfs_, subdir.c_str(), &is_dir);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(!is_dir);
  rc = tiledb_vfs_is_dir(ctx_, vfs_, subdir2.c_str(), &is_dir);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_dir);

  // Invalid file
  std::string foo_file = path + "foo";
  rc = tiledb_vfs_is_file(ctx_, vfs_, foo_file.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(!is_file);
  tiledb_vfs_fh_t* fh;
  rc = tiledb_vfs_open(ctx_, vfs_, foo_file.c_str(), TILEDB_VFS_READ, &fh);
  REQUIRE(rc == TILEDB_ERR);
  REQUIRE(fh == nullptr);

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

  // Read file
  check_read(path);

  // Move
  check_move(path);

  if (supports_s3_ && path == S3_TEMP_DIR) {
    int is_empty;
    rc = tiledb_vfs_is_empty_bucket(ctx_, vfs_, S3_BUCKET.c_str(), &is_empty);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(!(bool)is_empty);
  }

  if (!supports_s3_) {
    rc = tiledb_vfs_remove_dir(ctx_, vfs_, path.c_str());
    REQUIRE(rc == TILEDB_OK);
  }

  if (supports_s3_ && path == S3_TEMP_DIR) {
    rc = tiledb_vfs_empty_bucket(ctx_, vfs_, S3_BUCKET.c_str());
    REQUIRE(rc == TILEDB_OK);

    int is_empty;
    rc = tiledb_vfs_is_empty_bucket(ctx_, vfs_, S3_BUCKET.c_str(), &is_empty);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE((bool)is_empty);

    rc = tiledb_vfs_remove_bucket(ctx_, vfs_, S3_BUCKET.c_str());
    REQUIRE(rc == TILEDB_OK);
  }
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
  rc = tiledb_vfs_move_file(ctx_, vfs_, file.c_str(), file2.c_str());
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
  if (path == S3_TEMP_DIR)
    REQUIRE(!is_dir);  // No empty dirs exist in S3
  else
    REQUIRE(is_dir);
  rc = tiledb_vfs_create_dir(ctx_, vfs_, subdir.c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_is_dir(ctx_, vfs_, subdir.c_str(), &is_dir);
  REQUIRE(rc == TILEDB_OK);
  if (path == S3_TEMP_DIR)
    REQUIRE(!is_dir);  // No empty dirs exist in S3
  else
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
  rc = tiledb_vfs_move_dir(ctx_, vfs_, dir.c_str(), dir2.c_str());
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
  if (supports_s3_) {
    if (path == S3_TEMP_DIR) {
      std::string bucket2 = S3_PREFIX + random_bucket_name("tiledb") + "/";
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

      rc = tiledb_vfs_move_dir(ctx_, vfs_, subdir2.c_str(), subdir3.c_str());
      REQUIRE(rc == TILEDB_OK);
      rc = tiledb_vfs_is_file(ctx_, vfs_, file3.c_str(), &is_file);
      REQUIRE(rc == TILEDB_OK);
      REQUIRE(is_file);

      rc = tiledb_vfs_remove_bucket(ctx_, vfs_, bucket2.c_str());
      REQUIRE(rc == TILEDB_OK);
    }
  }
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
  tiledb_vfs_fh_t* fh;
  rc = tiledb_vfs_open(ctx_, vfs_, file.c_str(), TILEDB_VFS_WRITE, &fh);
  REQUIRE(rc == TILEDB_OK);
  int is_closed = 0;
  rc = tiledb_vfs_fh_is_closed(ctx_, fh, &is_closed);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_closed == 0);
  rc = tiledb_vfs_write(ctx_, fh, to_write.c_str(), to_write.size());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_sync(ctx_, fh);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_is_file(ctx_, vfs_, file.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);

  // Only for S3, sync still does not create the file
  uint64_t file_size = 0;
  if (path.find("s3://") == 0) {
    REQUIRE(!is_file);
  } else {
    REQUIRE(is_file);
    rc = tiledb_vfs_file_size(ctx_, vfs_, file.c_str(), &file_size);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(file_size == to_write.size());
  }

  // Close file
  rc = tiledb_vfs_close(ctx_, fh);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_fh_is_closed(ctx_, fh, &is_closed);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_closed == 1);
  rc = tiledb_vfs_fh_free(ctx_, &fh);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_is_file(ctx_, vfs_, file.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_file);  // It is a file even for S3
  rc = tiledb_vfs_file_size(ctx_, vfs_, file.c_str(), &file_size);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(file_size == to_write.size());

  // Check correctness with read
  std::string to_read;
  to_read.resize(to_write.size());
  rc = tiledb_vfs_open(ctx_, vfs_, file.c_str(), TILEDB_VFS_READ, &fh);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_read(ctx_, fh, 0, &to_read[0], file_size);
  REQUIRE(rc == TILEDB_OK);
  CHECK_THAT(to_read, Catch::Equals(to_write));
  rc = tiledb_vfs_close(ctx_, fh);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_fh_free(ctx_, &fh);
  REQUIRE(rc == TILEDB_OK);

  // Open in WRITE mode again - previous file will be removed
  rc = tiledb_vfs_open(ctx_, vfs_, file.c_str(), TILEDB_VFS_WRITE, &fh);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_write(ctx_, fh, to_write.c_str(), to_write.size());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_close(ctx_, fh);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_fh_free(ctx_, &fh);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_file_size(ctx_, vfs_, file.c_str(), &file_size);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(file_size == to_write.size());  // Not 2*to_write.size()

  // Opening and closing the file without writing, first deletes previous
  // file, and then creates an empty file
  rc = tiledb_vfs_open(ctx_, vfs_, file.c_str(), TILEDB_VFS_WRITE, &fh);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_close(ctx_, fh);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_fh_free(ctx_, &fh);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_is_file(ctx_, vfs_, file.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_file);
  rc = tiledb_vfs_file_size(ctx_, vfs_, file.c_str(), &file_size);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(file_size == 0);  // Not 2*to_write.size()
}

void VFSFx::check_append(const std::string& path) {
  // File write and file size
  auto file = path + "file";
  tiledb_vfs_fh_t* fh;

  // First write
  std::string to_write = "This will be written to the file";
  int rc = tiledb_vfs_open(ctx_, vfs_, file.c_str(), TILEDB_VFS_WRITE, &fh);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_write(ctx_, fh, to_write.c_str(), to_write.size());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_close(ctx_, fh);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_fh_free(ctx_, &fh);
  REQUIRE(rc == TILEDB_OK);

  // Second write - append
  std::string to_write_2 = "This will be appended to the end of the file";
  rc = tiledb_vfs_open(ctx_, vfs_, file.c_str(), TILEDB_VFS_APPEND, &fh);
  if (path.find("s3://") == 0) {  // S3 does not support append
    REQUIRE(rc == TILEDB_ERR);
    REQUIRE(fh == nullptr);
  } else {
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_vfs_write(ctx_, fh, to_write_2.c_str(), to_write_2.size());
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_vfs_close(ctx_, fh);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_vfs_fh_free(ctx_, &fh);
    REQUIRE(rc == TILEDB_OK);
    uint64_t file_size = 0;
    rc = tiledb_vfs_file_size(ctx_, vfs_, file.c_str(), &file_size);
    REQUIRE(rc == TILEDB_OK);
    uint64_t total_size = to_write.size() + to_write_2.size();
    CHECK(file_size == total_size);

    // Check correctness with read
    std::string to_read;
    to_read.resize(total_size);
    rc = tiledb_vfs_open(ctx_, vfs_, file.c_str(), TILEDB_VFS_READ, &fh);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_vfs_read(ctx_, fh, 0, &to_read[0], total_size);
    REQUIRE(rc == TILEDB_OK);
    CHECK_THAT(to_read, Catch::Equals(to_write + to_write_2));
    rc = tiledb_vfs_close(ctx_, fh);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_vfs_fh_free(ctx_, &fh);
    REQUIRE(rc == TILEDB_OK);
  }

  // Remove file
  rc = tiledb_vfs_remove_file(ctx_, vfs_, file.c_str());
  REQUIRE(rc == TILEDB_OK);
}

void VFSFx::check_read(const std::string& path) {
  auto file = path + "file";
  std::string to_write = "This will be written to the file";
  tiledb_vfs_fh_t* fh;
  int rc = tiledb_vfs_open(ctx_, vfs_, file.c_str(), TILEDB_VFS_WRITE, &fh);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_write(ctx_, fh, to_write.c_str(), to_write.size());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_close(ctx_, fh);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_fh_free(ctx_, &fh);
  REQUIRE(rc == TILEDB_OK);

  // Read only the "will be written" portion of the file
  std::string to_check = "will be written";
  std::string to_read;
  to_read.resize(to_check.size());
  uint64_t offset = 5;
  rc = tiledb_vfs_open(ctx_, vfs_, file.c_str(), TILEDB_VFS_READ, &fh);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_read(ctx_, fh, offset, &to_read[0], to_check.size());
  REQUIRE(rc == TILEDB_OK);
  CHECK_THAT(to_read, Catch::Equals(to_check));
  rc = tiledb_vfs_close(ctx_, fh);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_fh_free(ctx_, &fh);
  REQUIRE(rc == TILEDB_OK);

  // Remove file
  rc = tiledb_vfs_remove_file(ctx_, vfs_, file.c_str());
  REQUIRE(rc == TILEDB_OK);
}

std::string VFSFx::random_bucket_name(const std::string& prefix) {
  std::stringstream ss;
  ss << prefix << "-" << std::this_thread::get_id() << "-"
     << tiledb::sm::utils::timestamp_ms();
  return ss.str();
}

TEST_CASE_METHOD(VFSFx, "C API: Test virtual filesystem", "[capi], [vfs]") {
  tiledb_stats_enable();
  tiledb_stats_reset();

  if (supports_s3_)
    check_vfs(S3_TEMP_DIR);
  else if (supports_hdfs_)
    check_vfs(HDFS_TEMP_DIR);
  else
    check_vfs(FILE_TEMP_DIR);

  CHECK(tiledb::sm::stats::all_stats.counter_vfs_read_num_parallelized == 0);
}

TEST_CASE_METHOD(
    VFSFx,
    "C API: Test virtual filesystem when S3 is not supported",
    "[capi], [vfs]") {
  if (!supports_s3_) {
    tiledb_vfs_t* vfs;
    int rc = tiledb_vfs_create(ctx_, &vfs, nullptr);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_vfs_create_bucket(ctx_, vfs, "s3://foo");
    REQUIRE(rc == TILEDB_ERR);
    rc = tiledb_vfs_free(ctx_, &vfs);
    CHECK(rc == TILEDB_OK);
  }
}

TEST_CASE_METHOD(
    VFSFx, "C API: Test virtual filesystem config", "[capi], [vfs]") {
  // Prepare a config
  tiledb_error_t* error = nullptr;
  tiledb_config_t* config;
  int rc = tiledb_config_create(&config, &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_config_set(config, "vfs.s3.scheme", "http", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Create VFS
  tiledb_vfs_t* vfs;
  rc = tiledb_vfs_create(ctx_, &vfs, config);
  REQUIRE(rc == TILEDB_OK);

  // Get VFS config and check
  tiledb_config_t* config2;
  rc = tiledb_vfs_get_config(ctx_, vfs, &config2);
  REQUIRE(rc == TILEDB_OK);
  const char* value;
  rc = tiledb_config_get(config2, "vfs.s3.scheme", &value, &error);
  REQUIRE(error == nullptr);
  CHECK(!strncmp(value, "http", strlen("http")));
  rc = tiledb_config_get(config2, "sm.tile_cache_size", &value, &error);
  REQUIRE(error == nullptr);
  CHECK(!strncmp(value, "10000000", strlen("10000000")));

  // Clean up
  rc = tiledb_config_free(&config);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_config_free(&config2);
  CHECK(rc == TILEDB_OK);
  rc = tiledb_vfs_free(ctx_, &vfs);
  CHECK(rc == TILEDB_OK);
}

TEST_CASE_METHOD(VFSFx, "C API: Test VFS parallel I/O", "[capi], [vfs]") {
  tiledb_stats_enable();
  tiledb_stats_reset();
  set_num_vfs_threads(4);

  if (supports_s3_)
    check_vfs(S3_TEMP_DIR);
  else if (supports_hdfs_)
    check_vfs(HDFS_TEMP_DIR);
  else
    check_vfs(FILE_TEMP_DIR);

  CHECK(tiledb::sm::stats::all_stats.counter_vfs_read_num_parallelized > 0);
}
