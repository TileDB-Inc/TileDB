/**
 * @file   unit-capi-vfs.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB Inc.
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
#include "test/src/helpers.h"
#include "test/src/vfs_helpers.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/misc/utils.h"
#ifdef _WIN32
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif

#include <iostream>
#include <sstream>
#include <thread>

using namespace tiledb::test;

struct VFSFx {
  // TileDB context and vfs
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;

  // Vector of supported filsystems
  const std::vector<std::unique_ptr<SupportedFs>> fs_vec_;

  // Functions
  VFSFx();
  ~VFSFx();
  void check_vfs(const std::string& path);
  void check_write(const std::string& path);
  void check_move(const std::string& path);
  void check_copy(const std::string& path);
  void check_read(const std::string& path);
  void check_append(const std::string& path);
  void check_ls(const std::string& path);
  static std::string random_name(const std::string& prefix);
  void set_num_vfs_threads(unsigned num_threads);
};

VFSFx::VFSFx()
    : fs_vec_(vfs_test_get_fs_vec()) {
  ctx_ = nullptr;
  vfs_ = nullptr;

  // Create context and VFS with 1 thread
  set_num_vfs_threads(1);
}

VFSFx::~VFSFx() {
  tiledb_vfs_free(&vfs_);
  tiledb_ctx_free(&ctx_);
}

void VFSFx::set_num_vfs_threads(unsigned num_threads) {
  if (vfs_ != nullptr)
    tiledb_vfs_free(&vfs_);
  if (ctx_ != nullptr)
    tiledb_ctx_free(&ctx_);

  // Create TileDB context
  tiledb_config_t* config = nullptr;
  tiledb_error_t* error = nullptr;
  REQUIRE(tiledb_config_alloc(&config, &error) == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Initialize vfs test
  REQUIRE(vfs_test_init(fs_vec_, &ctx_, &vfs_, config).ok());

  SupportedFs* const fs = fs_vec_[0].get();
  if (dynamic_cast<SupportedFsS3*>(fs) != nullptr) {
    SupportedFsS3 s3_fs;
    REQUIRE(s3_fs.prepare_threads(config, error, num_threads).ok());
  } else if (dynamic_cast<SupportedFsLocal*>(fs) != nullptr) {
    SupportedFsLocal local_fs;
    REQUIRE(local_fs.prepare_threads(config, error, num_threads).ok());
  }

  REQUIRE(tiledb_ctx_alloc(config, &ctx_) == TILEDB_OK);
  REQUIRE(error == nullptr);
  int rc = tiledb_vfs_alloc(ctx_, config, &vfs_);
  REQUIRE(rc == TILEDB_OK);
  tiledb_config_free(&config);
}

void VFSFx::check_vfs(const std::string& path) {
  SupportedFs* const fs = fs_vec_[0].get();
  if (dynamic_cast<SupportedFsS3*>(fs) != nullptr) {
    // Create S3 bucket
    SupportedFsS3 s3_fs;
    REQUIRE(s3_fs.init(ctx_, vfs_).ok());

    // Remove S3 bucket
    REQUIRE(s3_fs.close(ctx_, vfs_).ok());
  }

  // Create directory, is directory, remove directory
  SupportedFsS3 s3_fs;
  std::string temp_dir = s3_fs.temp_dir();
  std::string s3_bucket = s3_fs.bucket();
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
  if (path == temp_dir)
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
  if (path == temp_dir)
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
  if (path == temp_dir)
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
  rc = tiledb_vfs_is_file(ctx_, vfs_, foo_file.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(!is_file);

  // Check write and append
  check_write(path);
  check_append(path);

  // Read file
  check_read(path);

  // Move
  check_move(path);

#ifndef _WIN32
  // Copy
  SupportedFsMem mem_fs;
  std::string mem_dir = mem_fs.temp_dir();
  if (path != mem_dir) {
    // copy not yet supported for memfs
    check_copy(path);
  }
#endif

  // Ls
  check_ls(path);

  if (dynamic_cast<SupportedFsS3*>(fs) != nullptr && path == temp_dir) {
    int is_empty;
    rc = tiledb_vfs_is_empty_bucket(ctx_, vfs_, s3_bucket.c_str(), &is_empty);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(!(bool)is_empty);
  }

  if (dynamic_cast<SupportedFsS3*>(fs) == nullptr) {
    rc = tiledb_vfs_remove_dir(ctx_, vfs_, path.c_str());
    REQUIRE(rc == TILEDB_OK);
  }

  if (dynamic_cast<SupportedFsS3*>(fs) != nullptr && path == temp_dir) {
    rc = tiledb_vfs_empty_bucket(ctx_, vfs_, s3_bucket.c_str());
    REQUIRE(rc == TILEDB_OK);

    int is_empty;
    rc = tiledb_vfs_is_empty_bucket(ctx_, vfs_, s3_bucket.c_str(), &is_empty);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE((bool)is_empty);

    rc = tiledb_vfs_remove_bucket(ctx_, vfs_, s3_bucket.c_str());
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
  SupportedFsS3 s3_fs;
  std::string temp_dir = s3_fs.temp_dir();
  std::string s3_bucket = s3_fs.bucket();
  rc = tiledb_vfs_create_dir(ctx_, vfs_, dir.c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_is_dir(ctx_, vfs_, dir.c_str(), &is_dir);
  REQUIRE(rc == TILEDB_OK);
  if (path == temp_dir)
    REQUIRE(!is_dir);  // No empty dirs exist in S3
  else
    REQUIRE(is_dir);
  rc = tiledb_vfs_create_dir(ctx_, vfs_, subdir.c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_is_dir(ctx_, vfs_, subdir.c_str(), &is_dir);
  REQUIRE(rc == TILEDB_OK);
  if (path == temp_dir)
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
  SupportedFs* const fs = fs_vec_[0].get();
  if (dynamic_cast<SupportedFsS3*>(fs) != nullptr) {
    if (path == temp_dir) {
      std::string bucket2 = s3_bucket;
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

#ifndef _WIN32
void VFSFx::check_copy(const std::string& path) {
  // Do not support HDFS
  SupportedFs* const fs = fs_vec_[0].get();
  if (dynamic_cast<SupportedFsHDFS*>(fs) != nullptr)
    return;

  // Copy file
  auto file = path + "file";
  auto file2 = path + "file2";
  int is_file = 0;
  SupportedFsS3 s3_fs;
  std::string temp_dir = s3_fs.temp_dir();
  std::string s3_bucket = s3_fs.bucket();
  int rc = tiledb_vfs_touch(ctx_, vfs_, file.c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_is_file(ctx_, vfs_, file.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_file);
  rc = tiledb_vfs_copy_file(ctx_, vfs_, file.c_str(), file2.c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_is_file(ctx_, vfs_, file.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_file);
  rc = tiledb_vfs_is_file(ctx_, vfs_, file2.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_file);

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
  if (path == temp_dir)
    REQUIRE(!is_dir);  // No empty dirs exist in S3
  else
    REQUIRE(is_dir);
  rc = tiledb_vfs_create_dir(ctx_, vfs_, subdir.c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_is_dir(ctx_, vfs_, subdir.c_str(), &is_dir);
  REQUIRE(rc == TILEDB_OK);
  if (path == temp_dir)
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

  rc = tiledb_vfs_is_dir(ctx_, vfs_, dir2.c_str(), &is_dir);
  REQUIRE(rc == TILEDB_OK);
  if (is_dir) {
    rc = tiledb_vfs_remove_dir(ctx_, vfs_, dir2.c_str());
    REQUIRE(rc == TILEDB_OK);
  }
  rc = tiledb_vfs_copy_dir(ctx_, vfs_, dir.c_str(), dir2.c_str());
  REQUIRE(rc == TILEDB_OK);
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

  // Copy from one bucket to another (only for S3)
  if (dynamic_cast<SupportedFsS3*>(fs) != nullptr) {
    std::string bucket2 = s3_bucket;
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

    rc = tiledb_vfs_copy_dir(ctx_, vfs_, subdir2.c_str(), subdir3.c_str());
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_vfs_is_file(ctx_, vfs_, file3.c_str(), &is_file);
    REQUIRE(rc == TILEDB_OK);
    REQUIRE(is_file);

    rc = tiledb_vfs_remove_bucket(ctx_, vfs_, bucket2.c_str());
    REQUIRE(rc == TILEDB_OK);
  }
}
#endif

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
  tiledb_vfs_fh_free(&fh);
  rc = tiledb_vfs_is_file(ctx_, vfs_, file.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_file);  // It is a file even for S3
  rc = tiledb_vfs_file_size(ctx_, vfs_, file.c_str(), &file_size);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(file_size == to_write.size());

  // Write a second file
  auto file2 = path + "file2";
  rc = tiledb_vfs_is_file(ctx_, vfs_, file2.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  if (is_file) {
    rc = tiledb_vfs_remove_file(ctx_, vfs_, file2.c_str());
    REQUIRE(rc == TILEDB_OK);
  }
  rc = tiledb_vfs_is_file(ctx_, vfs_, file2.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(!is_file);
  tiledb_vfs_fh_t* fh2;
  rc = tiledb_vfs_open(ctx_, vfs_, file2.c_str(), TILEDB_VFS_WRITE, &fh2);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_fh_is_closed(ctx_, fh2, &is_closed);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_closed == 0);
  rc = tiledb_vfs_write(ctx_, fh2, to_write.c_str(), to_write.size());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_is_file(ctx_, vfs_, file2.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_close(ctx_, fh2);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_fh_is_closed(ctx_, fh2, &is_closed);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_closed == 1);
  tiledb_vfs_fh_free(&fh2);
  rc = tiledb_vfs_is_file(ctx_, vfs_, file2.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(is_file);
  rc = tiledb_vfs_file_size(ctx_, vfs_, file2.c_str(), &file_size);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(file_size == to_write.size());

  // Directory size
  uint64_t dir_size;
  rc = tiledb_vfs_dir_size(ctx_, vfs_, path.c_str(), &dir_size);
  CHECK(rc == TILEDB_OK);
  CHECK(dir_size == 2 * to_write.size());

  // Write another file in a subdir
  std::string subdir = path + "subdir";
  rc = tiledb_vfs_create_dir(ctx_, vfs_, subdir.c_str());
  auto file3 = subdir + "file3";
  rc = tiledb_vfs_is_file(ctx_, vfs_, file3.c_str(), &is_file);
  REQUIRE(rc == TILEDB_OK);
  if (is_file) {
    rc = tiledb_vfs_remove_file(ctx_, vfs_, file3.c_str());
    REQUIRE(rc == TILEDB_OK);
  }
  tiledb_vfs_fh_t* fh3;
  rc = tiledb_vfs_open(ctx_, vfs_, file3.c_str(), TILEDB_VFS_WRITE, &fh3);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_write(ctx_, fh3, to_write.c_str(), to_write.size());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_close(ctx_, fh3);
  REQUIRE(rc == TILEDB_OK);
  tiledb_vfs_fh_free(&fh3);

  rc = tiledb_vfs_dir_size(ctx_, vfs_, path.c_str(), &dir_size);
  CHECK(rc == TILEDB_OK);
  CHECK(dir_size == 3 * to_write.size());

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
  tiledb_vfs_fh_free(&fh);
  REQUIRE(rc == TILEDB_OK);

  // Open in WRITE mode again - previous file will be removed
  rc = tiledb_vfs_open(ctx_, vfs_, file.c_str(), TILEDB_VFS_WRITE, &fh);
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_write(ctx_, fh, to_write.c_str(), to_write.size());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_close(ctx_, fh);
  REQUIRE(rc == TILEDB_OK);
  tiledb_vfs_fh_free(&fh);
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
  tiledb_vfs_fh_free(&fh);
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
  tiledb_vfs_fh_free(&fh);
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
    tiledb_vfs_fh_free(&fh);
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
    tiledb_vfs_fh_free(&fh);
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
  tiledb_vfs_fh_free(&fh);
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
  tiledb_vfs_fh_free(&fh);
  REQUIRE(rc == TILEDB_OK);

  // Remove file
  rc = tiledb_vfs_remove_file(ctx_, vfs_, file.c_str());
  REQUIRE(rc == TILEDB_OK);
}

int ls_getter(const char* path, void* data) {
  auto vec = static_cast<std::vector<std::string>*>(data);
  vec->emplace_back(path);
  return 1;
}

void VFSFx::check_ls(const std::string& path) {
  std::string dir = path + "ls_dir";
  std::string file = dir + "/file";
  std::string file2 = dir + "/file2";
  std::string subdir = dir + "/subdir";
  std::string subdir2 = dir + "/subdir2";
  std::string subdir_file = subdir + "/file";
  std::string subdir_file2 = subdir2 + "/file2";

  // Create directories and files
  int rc = tiledb_vfs_create_dir(ctx_, vfs_, dir.c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_create_dir(ctx_, vfs_, subdir.c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_create_dir(ctx_, vfs_, subdir2.c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_touch(ctx_, vfs_, file.c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_touch(ctx_, vfs_, file2.c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_touch(ctx_, vfs_, subdir_file.c_str());
  REQUIRE(rc == TILEDB_OK);
  rc = tiledb_vfs_touch(ctx_, vfs_, subdir_file2.c_str());
  REQUIRE(rc == TILEDB_OK);

  // List
  std::vector<std::string> children;
  rc = tiledb_vfs_ls(ctx_, vfs_, (dir + "/").c_str(), ls_getter, &children);
  REQUIRE(rc == TILEDB_OK);

  // Normalization
  for (auto& child : children) {
    if (child.back() == '/')
      child.pop_back();
  }

#ifdef _WIN32
  // Normalization only for Windows
  file = tiledb::sm::Win::uri_from_path(file);
  file2 = tiledb::sm::Win::uri_from_path(file2);
  subdir = tiledb::sm::Win::uri_from_path(subdir);
  subdir2 = tiledb::sm::Win::uri_from_path(subdir2);
#endif

  // Check results
  std::sort(children.begin(), children.end());
  REQUIRE(children.size() == 4);
  CHECK(children[0] == file);
  CHECK(children[1] == file2);
  CHECK(children[2] == subdir);
  CHECK(children[3] == subdir2);
}

std::string VFSFx::random_name(const std::string& prefix) {
  std::stringstream ss;
  ss << prefix << "-" << std::this_thread::get_id() << "-"
     << TILEDB_TIMESTAMP_NOW_MS;
  return ss.str();
}

TEST_CASE_METHOD(VFSFx, "C API: Test virtual filesystem", "[capi], [vfs]") {
  tiledb_stats_enable();
  tiledb_stats_reset();

  SupportedFsLocal local_fs;
  std::string temp_dir = local_fs.file_prefix() + local_fs.temp_dir();
  check_vfs(temp_dir);

  SupportedFs* const fs = fs_vec_[0].get();
  if (dynamic_cast<SupportedFsS3*>(fs) != nullptr) {
    SupportedFsS3 s3_fs;
    check_vfs(s3_fs.temp_dir());
  } else if (dynamic_cast<SupportedFsHDFS*>(fs) != nullptr) {
    SupportedFsHDFS hdfs_fs;
    check_vfs(hdfs_fs.temp_dir());
  } else {
    SupportedFsLocal local_fs;
    check_vfs(local_fs.file_prefix() + local_fs.temp_dir());
    if (dynamic_cast<SupportedFsMem*>(fs) != nullptr) {
      SupportedFsMem mem_fs;
      check_vfs(mem_fs.temp_dir());
    }
  }
}

TEST_CASE_METHOD(
    VFSFx,
    "C API: Test virtual filesystem when S3 is not supported",
    "[capi], [vfs]") {
  SupportedFs* const fs = fs_vec_[0].get();
  if (dynamic_cast<SupportedFsS3*>(fs) == nullptr) {
    tiledb_vfs_t* vfs;
    int rc = tiledb_vfs_alloc(ctx_, nullptr, &vfs);
    REQUIRE(rc == TILEDB_OK);
    rc = tiledb_vfs_create_bucket(ctx_, vfs, "s3://foo");
    REQUIRE(rc == TILEDB_ERR);
    tiledb_vfs_free(&vfs);
  }
}

TEST_CASE_METHOD(
    VFSFx, "C API: Test virtual filesystem config", "[capi], [vfs]") {
  // Prepare a config
  tiledb_error_t* error = nullptr;
  tiledb_config_t* config;
  int rc = tiledb_config_alloc(&config, &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);
  rc = tiledb_config_set(config, "vfs.s3.scheme", "https", &error);
  REQUIRE(rc == TILEDB_OK);
  REQUIRE(error == nullptr);

  // Create VFS
  tiledb_vfs_t* vfs;
  rc = tiledb_vfs_alloc(ctx_, config, &vfs);
  REQUIRE(rc == TILEDB_OK);

  // Get VFS config and check
  tiledb_config_t* config2;
  rc = tiledb_vfs_get_config(ctx_, vfs, &config2);
  REQUIRE(rc == TILEDB_OK);
  const char* value;
  rc = tiledb_config_get(config2, "vfs.s3.scheme", &value, &error);
  REQUIRE(error == nullptr);
  CHECK(!strncmp(value, "https", strlen("https")));
  rc = tiledb_config_get(config2, "sm.tile_cache_size", &value, &error);
  REQUIRE(error == nullptr);
  CHECK(!strncmp(value, "10000000", strlen("10000000")));

  // Clean up
  tiledb_config_free(&config);
  tiledb_config_free(&config2);
  tiledb_vfs_free(&vfs);
}

TEST_CASE_METHOD(VFSFx, "C API: Test VFS parallel I/O", "[capi], [vfs]") {
  tiledb_stats_enable();
  tiledb_stats_reset();
  set_num_vfs_threads(4);

  SupportedFs* const fs = fs_vec_[0].get();
  if (dynamic_cast<SupportedFsS3*>(fs) != nullptr) {
    SupportedFsS3 s3_fs;
    check_vfs(s3_fs.temp_dir());
  } else if (dynamic_cast<SupportedFsHDFS*>(fs) != nullptr) {
    SupportedFsHDFS hdfs_fs;
    check_vfs(hdfs_fs.temp_dir());
  } else {
    SupportedFsLocal local_fs;
    check_vfs(local_fs.file_prefix() + local_fs.temp_dir());
    if (dynamic_cast<SupportedFsMem*>(fs) != nullptr) {
      SupportedFsMem mem_fs;
      check_vfs(mem_fs.temp_dir());
    }
  }
}
