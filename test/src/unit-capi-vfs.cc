/**
 * @file   unit-capi-vfs.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2023 TileDB Inc.
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

#include <test/support/tdb_catch.h>
#include "test/support/src/helpers.h"
#include "test/support/src/temporary_local_directory.h"
#include "tiledb/api/c_api/vfs/vfs_api_internal.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/misc/utils.h"
#ifdef _WIN32
#include "tiledb/sm/filesystem/path_win.h"
#include "tiledb/sm/filesystem/win.h"
#else
#include "tiledb/sm/filesystem/posix.h"
#endif

#include <iostream>
#include <optional>
#include <sstream>
#include <thread>

using namespace tiledb::test;

struct VFSFx {
  // The unique directory object
  tiledb::sm::TemporaryLocalDirectory temp_dir_{"tiledb_test_"};

  // TileDB context and vfs
  tiledb_ctx_t* ctx_;
  tiledb_vfs_t* vfs_;

  // Functions
  VFSFx();
  ~VFSFx();
  void check_write(const std::string& path);
  void check_move(const std::string& path);
  void check_copy(const std::string& path);
  void check_read(const std::string& path);
  void check_append(const std::string& path);
  void check_ls(const std::string& path);

  inline void require_tiledb_ok(capi_return_t rc) const {
    tiledb::test::require_tiledb_ok(ctx_, (int)rc);
  }
};

VFSFx::VFSFx() {
  create_ctx_and_vfs(&ctx_, &vfs_);
}

VFSFx::~VFSFx() {
  tiledb_vfs_free(&vfs_);
  tiledb_ctx_free(&ctx_);
}

void VFSFx::check_move(const std::string& path) {
  auto backend_name = tiledb::sm::utils::parse::backend_name(path);
  // Move and remove file
  auto file = path + "file";
  auto file2 = path + "file2";
  int is_file = 0;
  require_tiledb_ok(tiledb_vfs_touch(ctx_, vfs_, file.c_str()));
  require_tiledb_ok(tiledb_vfs_is_file(ctx_, vfs_, file.c_str(), &is_file));
  REQUIRE(is_file);
  require_tiledb_ok(
      tiledb_vfs_move_file(ctx_, vfs_, file.c_str(), file2.c_str()));
  require_tiledb_ok(tiledb_vfs_is_file(ctx_, vfs_, file.c_str(), &is_file));
  REQUIRE(!is_file);
  require_tiledb_ok(tiledb_vfs_is_file(ctx_, vfs_, file2.c_str(), &is_file));
  REQUIRE(is_file);
  require_tiledb_ok(tiledb_vfs_remove_file(ctx_, vfs_, file2.c_str()));
  require_tiledb_ok(tiledb_vfs_is_file(ctx_, vfs_, file2.c_str(), &is_file));
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
  require_tiledb_ok(tiledb_vfs_create_dir(ctx_, vfs_, dir.c_str()));
  require_tiledb_ok(tiledb_vfs_is_dir(ctx_, vfs_, dir.c_str(), &is_dir));
  if (backend_name == "s3")
    REQUIRE(!is_dir);  // No empty dirs exist in S3
  else
    REQUIRE(is_dir);
  require_tiledb_ok(tiledb_vfs_create_dir(ctx_, vfs_, subdir.c_str()));
  require_tiledb_ok(tiledb_vfs_is_dir(ctx_, vfs_, subdir.c_str(), &is_dir));
  if (backend_name == "s3")
    REQUIRE(!is_dir);  // No empty dirs exist in S3
  else
    REQUIRE(is_dir);
  require_tiledb_ok(tiledb_vfs_touch(ctx_, vfs_, file.c_str()));
  require_tiledb_ok(tiledb_vfs_is_file(ctx_, vfs_, file.c_str(), &is_file));
  REQUIRE(is_file);
  require_tiledb_ok(tiledb_vfs_touch(ctx_, vfs_, file2.c_str()));
  require_tiledb_ok(tiledb_vfs_is_file(ctx_, vfs_, file2.c_str(), &is_file));
  REQUIRE(is_file);
  require_tiledb_ok(tiledb_vfs_move_dir(ctx_, vfs_, dir.c_str(), dir2.c_str()));

  require_tiledb_ok(tiledb_vfs_is_dir(ctx_, vfs_, dir.c_str(), &is_dir));
  REQUIRE(!is_dir);
  require_tiledb_ok(tiledb_vfs_is_dir(ctx_, vfs_, subdir.c_str(), &is_dir));
  REQUIRE(!is_dir);
  require_tiledb_ok(tiledb_vfs_is_file(ctx_, vfs_, file.c_str(), &is_file));
  REQUIRE(!is_file);
  require_tiledb_ok(tiledb_vfs_is_file(ctx_, vfs_, file2.c_str(), &is_file));
  REQUIRE(!is_file);

  require_tiledb_ok(tiledb_vfs_is_dir(ctx_, vfs_, dir2.c_str(), &is_dir));
  REQUIRE(is_dir);
  require_tiledb_ok(tiledb_vfs_is_dir(ctx_, vfs_, subdir2.c_str(), &is_dir));
  REQUIRE(is_dir);
  require_tiledb_ok(tiledb_vfs_is_file(ctx_, vfs_, new_file.c_str(), &is_file));
  REQUIRE(is_file);
  require_tiledb_ok(
      tiledb_vfs_is_file(ctx_, vfs_, new_file2.c_str(), &is_file));
  REQUIRE(is_file);

  // Move from one bucket to another (only for S3)
  if constexpr (tiledb::sm::filesystem::s3_enabled) {
    if (backend_name == "s3") {
      std::string bucket2 = "s3://" + random_name("tiledb") + "/";
      std::string subdir3 = bucket2 + "tiledb_test/subdir3/";
      std::string file3 = subdir3 + "file2";
      int is_bucket = 0;

      require_tiledb_ok(
          tiledb_vfs_is_bucket(ctx_, vfs_, bucket2.c_str(), &is_bucket));
      if (is_bucket) {
        require_tiledb_ok(
            tiledb_vfs_remove_bucket(ctx_, vfs_, bucket2.c_str()));
      }

      require_tiledb_ok(tiledb_vfs_create_bucket(ctx_, vfs_, bucket2.c_str()));

      require_tiledb_ok(
          tiledb_vfs_move_dir(ctx_, vfs_, subdir2.c_str(), subdir3.c_str()));
      require_tiledb_ok(
          tiledb_vfs_is_file(ctx_, vfs_, file3.c_str(), &is_file));
      REQUIRE(is_file);

      require_tiledb_ok(tiledb_vfs_remove_bucket(ctx_, vfs_, bucket2.c_str()));
    }
  }
}

#ifndef _WIN32
void VFSFx::check_copy(const std::string& path) {
  auto backend_name = tiledb::sm::utils::parse::backend_name(path);
  // Do not support HDFS
  if constexpr (tiledb::sm::filesystem::hdfs_enabled) {
    return;
  }

  // Copy file
  auto file = path + "file";
  auto file2 = path + "file2";
  int is_file = 0;
  require_tiledb_ok(tiledb_vfs_touch(ctx_, vfs_, file.c_str()));
  require_tiledb_ok(tiledb_vfs_is_file(ctx_, vfs_, file.c_str(), &is_file));
  REQUIRE(is_file);
  require_tiledb_ok(
      tiledb_vfs_copy_file(ctx_, vfs_, file.c_str(), file2.c_str()));
  require_tiledb_ok(tiledb_vfs_is_file(ctx_, vfs_, file.c_str(), &is_file));
  REQUIRE(is_file);
  require_tiledb_ok(tiledb_vfs_is_file(ctx_, vfs_, file2.c_str(), &is_file));
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
  require_tiledb_ok(tiledb_vfs_create_dir(ctx_, vfs_, dir.c_str()));
  require_tiledb_ok(tiledb_vfs_is_dir(ctx_, vfs_, dir.c_str(), &is_dir));
  if (backend_name == "s3")
    REQUIRE(!is_dir);  // No empty dirs exist in S3
  else
    REQUIRE(is_dir);
  require_tiledb_ok(tiledb_vfs_create_dir(ctx_, vfs_, subdir.c_str()));
  require_tiledb_ok(tiledb_vfs_is_dir(ctx_, vfs_, subdir.c_str(), &is_dir));
  if (backend_name == "s3")
    REQUIRE(!is_dir);  // No empty dirs exist in S3
  else
    REQUIRE(is_dir);
  require_tiledb_ok(tiledb_vfs_touch(ctx_, vfs_, file.c_str()));
  require_tiledb_ok(tiledb_vfs_is_file(ctx_, vfs_, file.c_str(), &is_file));
  REQUIRE(is_file);
  require_tiledb_ok(tiledb_vfs_touch(ctx_, vfs_, file2.c_str()));
  require_tiledb_ok(tiledb_vfs_is_file(ctx_, vfs_, file2.c_str(), &is_file));
  REQUIRE(is_file);

  require_tiledb_ok(tiledb_vfs_is_dir(ctx_, vfs_, dir2.c_str(), &is_dir));
  if (is_dir) {
    require_tiledb_ok(tiledb_vfs_remove_dir(ctx_, vfs_, dir2.c_str()));
  }
  require_tiledb_ok(tiledb_vfs_copy_dir(ctx_, vfs_, dir.c_str(), dir2.c_str()));
  require_tiledb_ok(tiledb_vfs_is_dir(ctx_, vfs_, dir2.c_str(), &is_dir));
  REQUIRE(is_dir);
  require_tiledb_ok(tiledb_vfs_is_dir(ctx_, vfs_, subdir2.c_str(), &is_dir));
  REQUIRE(is_dir);
  require_tiledb_ok(tiledb_vfs_is_file(ctx_, vfs_, new_file.c_str(), &is_file));
  REQUIRE(is_file);
  require_tiledb_ok(
      tiledb_vfs_is_file(ctx_, vfs_, new_file2.c_str(), &is_file));
  REQUIRE(is_file);

  // Copy from one bucket to another (only for S3)
  if (tiledb::sm::utils::parse::backend_name(path) == "s3") {
    std::string bucket2 = "s3://" + random_name("tiledb") + "/";
    std::string subdir3 = bucket2 + "tiledb_test/subdir3/";
    std::string file3 = subdir3 + "file2";
    int is_bucket = 0;

    require_tiledb_ok(
        tiledb_vfs_is_bucket(ctx_, vfs_, bucket2.c_str(), &is_bucket));
    if (is_bucket) {
      require_tiledb_ok(tiledb_vfs_remove_bucket(ctx_, vfs_, bucket2.c_str()));
    }

    require_tiledb_ok(tiledb_vfs_create_bucket(ctx_, vfs_, bucket2.c_str()));

    require_tiledb_ok(
        tiledb_vfs_copy_dir(ctx_, vfs_, subdir2.c_str(), subdir3.c_str()));
    require_tiledb_ok(tiledb_vfs_is_file(ctx_, vfs_, file3.c_str(), &is_file));
    REQUIRE(is_file);

    require_tiledb_ok(tiledb_vfs_remove_bucket(ctx_, vfs_, bucket2.c_str()));
  }
}
#endif

void VFSFx::check_write(const std::string& path) {
  // File write and file size
  int is_file = 0;
  auto file = path + "file";
  require_tiledb_ok(tiledb_vfs_is_file(ctx_, vfs_, file.c_str(), &is_file));
  if (is_file) {
    require_tiledb_ok(tiledb_vfs_remove_file(ctx_, vfs_, file.c_str()));
  }
  require_tiledb_ok(tiledb_vfs_is_file(ctx_, vfs_, file.c_str(), &is_file));
  REQUIRE(!is_file);
  std::string to_write = "This will be written to the file";
  tiledb_vfs_fh_t* fh;
  require_tiledb_ok(
      tiledb_vfs_open(ctx_, vfs_, file.c_str(), TILEDB_VFS_WRITE, &fh));
  int is_closed = 0;
  require_tiledb_ok(tiledb_vfs_fh_is_closed(ctx_, fh, &is_closed));
  REQUIRE(is_closed == 0);
  require_tiledb_ok(
      tiledb_vfs_write(ctx_, fh, to_write.c_str(), to_write.size()));
  require_tiledb_ok(tiledb_vfs_sync(ctx_, fh));
  require_tiledb_ok(tiledb_vfs_is_file(ctx_, vfs_, file.c_str(), &is_file));

  // Only for S3, sync still does not create the file
  uint64_t file_size = 0;
  if (path.find("s3://") == 0) {
    REQUIRE(!is_file);
  } else {
    REQUIRE(is_file);
    require_tiledb_ok(
        tiledb_vfs_file_size(ctx_, vfs_, file.c_str(), &file_size));
    REQUIRE(file_size == to_write.size());
  }

  // Close file
  require_tiledb_ok(tiledb_vfs_close(ctx_, fh));
  require_tiledb_ok(tiledb_vfs_fh_is_closed(ctx_, fh, &is_closed));
  REQUIRE(is_closed == 1);
  tiledb_vfs_fh_free(&fh);
  require_tiledb_ok(tiledb_vfs_is_file(ctx_, vfs_, file.c_str(), &is_file));
  REQUIRE(is_file);  // It is a file even for S3
  require_tiledb_ok(tiledb_vfs_file_size(ctx_, vfs_, file.c_str(), &file_size));
  REQUIRE(file_size == to_write.size());

  // Write a second file
  auto file2 = path + "file2";
  require_tiledb_ok(tiledb_vfs_is_file(ctx_, vfs_, file2.c_str(), &is_file));
  if (is_file) {
    require_tiledb_ok(tiledb_vfs_remove_file(ctx_, vfs_, file2.c_str()));
  }
  require_tiledb_ok(tiledb_vfs_is_file(ctx_, vfs_, file2.c_str(), &is_file));
  REQUIRE(!is_file);
  tiledb_vfs_fh_t* fh2;
  require_tiledb_ok(
      tiledb_vfs_open(ctx_, vfs_, file2.c_str(), TILEDB_VFS_WRITE, &fh2));
  require_tiledb_ok(tiledb_vfs_fh_is_closed(ctx_, fh2, &is_closed));
  REQUIRE(is_closed == 0);
  require_tiledb_ok(
      tiledb_vfs_write(ctx_, fh2, to_write.c_str(), to_write.size()));
  require_tiledb_ok(tiledb_vfs_is_file(ctx_, vfs_, file2.c_str(), &is_file));
  require_tiledb_ok(tiledb_vfs_close(ctx_, fh2));
  require_tiledb_ok(tiledb_vfs_fh_is_closed(ctx_, fh2, &is_closed));
  REQUIRE(is_closed == 1);
  tiledb_vfs_fh_free(&fh2);
  require_tiledb_ok(tiledb_vfs_is_file(ctx_, vfs_, file2.c_str(), &is_file));
  REQUIRE(is_file);
  require_tiledb_ok(
      tiledb_vfs_file_size(ctx_, vfs_, file2.c_str(), &file_size));
  REQUIRE(file_size == to_write.size());

  // Directory size
  uint64_t dir_size;
  require_tiledb_ok(tiledb_vfs_dir_size(ctx_, vfs_, path.c_str(), &dir_size));
  CHECK(dir_size == 2 * to_write.size());

  // Write another file in a subdir
  std::string subdir = path + "subdir";
  require_tiledb_ok(tiledb_vfs_create_dir(ctx_, vfs_, subdir.c_str()));
  auto file3 = subdir + "file3";
  require_tiledb_ok(tiledb_vfs_is_file(ctx_, vfs_, file3.c_str(), &is_file));
  if (is_file) {
    require_tiledb_ok(tiledb_vfs_remove_file(ctx_, vfs_, file3.c_str()));
  }
  tiledb_vfs_fh_t* fh3;
  require_tiledb_ok(
      tiledb_vfs_open(ctx_, vfs_, file3.c_str(), TILEDB_VFS_WRITE, &fh3));
  require_tiledb_ok(
      tiledb_vfs_write(ctx_, fh3, to_write.c_str(), to_write.size()));
  require_tiledb_ok(tiledb_vfs_close(ctx_, fh3));
  tiledb_vfs_fh_free(&fh3);

  require_tiledb_ok(tiledb_vfs_dir_size(ctx_, vfs_, path.c_str(), &dir_size));
  CHECK(dir_size == 3 * to_write.size());

  // Check correctness with read
  std::string to_read;
  to_read.resize(to_write.size());
  require_tiledb_ok(
      tiledb_vfs_open(ctx_, vfs_, file.c_str(), TILEDB_VFS_READ, &fh));
  require_tiledb_ok(tiledb_vfs_read(ctx_, fh, 0, &to_read[0], file_size));
  CHECK_THAT(to_read, Catch::Matchers::Equals(to_write));
  require_tiledb_ok(tiledb_vfs_close(ctx_, fh));
  tiledb_vfs_fh_free(&fh);

  // Open in WRITE mode again - previous file will be removed
  require_tiledb_ok(
      tiledb_vfs_open(ctx_, vfs_, file.c_str(), TILEDB_VFS_WRITE, &fh));
  require_tiledb_ok(
      tiledb_vfs_write(ctx_, fh, to_write.c_str(), to_write.size()));
  require_tiledb_ok(tiledb_vfs_close(ctx_, fh));
  tiledb_vfs_fh_free(&fh);
  require_tiledb_ok(tiledb_vfs_file_size(ctx_, vfs_, file.c_str(), &file_size));
  REQUIRE(file_size == to_write.size());  // Not 2*to_write.size()

  // Opening and closing the file without writing, first deletes previous
  // file, and then creates an empty file
  require_tiledb_ok(
      tiledb_vfs_open(ctx_, vfs_, file.c_str(), TILEDB_VFS_WRITE, &fh));
  require_tiledb_ok(tiledb_vfs_close(ctx_, fh));
  tiledb_vfs_fh_free(&fh);
  require_tiledb_ok(tiledb_vfs_is_file(ctx_, vfs_, file.c_str(), &is_file));
  REQUIRE(is_file);
  require_tiledb_ok(tiledb_vfs_file_size(ctx_, vfs_, file.c_str(), &file_size));
  REQUIRE(file_size == 0);  // Not 2*to_write.size()
}

void VFSFx::check_append(const std::string& path) {
  // File write and file size
  auto file = path + "file";
  tiledb_vfs_fh_t* fh;

  // First write
  std::string to_write = "This will be written to the file";
  require_tiledb_ok(
      tiledb_vfs_open(ctx_, vfs_, file.c_str(), TILEDB_VFS_WRITE, &fh));
  require_tiledb_ok(
      tiledb_vfs_write(ctx_, fh, to_write.c_str(), to_write.size()));
  require_tiledb_ok(tiledb_vfs_close(ctx_, fh));
  tiledb_vfs_fh_free(&fh);

  // Second write - append
  std::string to_write_2 = "This will be appended to the end of the file";
  int rc = tiledb_vfs_open(ctx_, vfs_, file.c_str(), TILEDB_VFS_APPEND, &fh);
  if (path.find("s3://") == 0) {  // S3 does not support append
    REQUIRE(rc == TILEDB_ERR);
    REQUIRE(fh == nullptr);
  } else {
    REQUIRE(rc == TILEDB_OK);
    require_tiledb_ok(
        tiledb_vfs_write(ctx_, fh, to_write_2.c_str(), to_write_2.size()));
    require_tiledb_ok(tiledb_vfs_close(ctx_, fh));
    tiledb_vfs_fh_free(&fh);
    uint64_t file_size = 0;
    require_tiledb_ok(
        tiledb_vfs_file_size(ctx_, vfs_, file.c_str(), &file_size));
    uint64_t total_size = to_write.size() + to_write_2.size();
    CHECK(file_size == total_size);

    // Check correctness with read
    std::string to_read;
    to_read.resize(total_size);
    require_tiledb_ok(
        tiledb_vfs_open(ctx_, vfs_, file.c_str(), TILEDB_VFS_READ, &fh));
    require_tiledb_ok(tiledb_vfs_read(ctx_, fh, 0, &to_read[0], total_size));
    CHECK_THAT(to_read, Catch::Matchers::Equals(to_write + to_write_2));
    require_tiledb_ok(tiledb_vfs_close(ctx_, fh));
    tiledb_vfs_fh_free(&fh);
  }

  // Remove file
  require_tiledb_ok(tiledb_vfs_remove_file(ctx_, vfs_, file.c_str()));
}

void VFSFx::check_read(const std::string& path) {
  auto file = path + "file";
  std::string to_write = "This will be written to the file";
  tiledb_vfs_fh_t* fh;
  require_tiledb_ok(
      tiledb_vfs_open(ctx_, vfs_, file.c_str(), TILEDB_VFS_WRITE, &fh));
  require_tiledb_ok(
      tiledb_vfs_write(ctx_, fh, to_write.c_str(), to_write.size()));
  require_tiledb_ok(tiledb_vfs_close(ctx_, fh));
  tiledb_vfs_fh_free(&fh);

  // Read only the "will be written" portion of the file
  std::string to_check = "will be written";
  std::string to_read;
  to_read.resize(to_check.size());
  uint64_t offset = 5;
  require_tiledb_ok(
      tiledb_vfs_open(ctx_, vfs_, file.c_str(), TILEDB_VFS_READ, &fh));
  require_tiledb_ok(
      tiledb_vfs_read(ctx_, fh, offset, &to_read[0], to_check.size()));
  CHECK_THAT(to_read, Catch::Matchers::Equals(to_check));
  require_tiledb_ok(tiledb_vfs_close(ctx_, fh));
  tiledb_vfs_fh_free(&fh);

  // Remove file
  require_tiledb_ok(tiledb_vfs_remove_file(ctx_, vfs_, file.c_str()));
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
  require_tiledb_ok(tiledb_vfs_create_dir(ctx_, vfs_, dir.c_str()));
  require_tiledb_ok(tiledb_vfs_create_dir(ctx_, vfs_, subdir.c_str()));
  require_tiledb_ok(tiledb_vfs_create_dir(ctx_, vfs_, subdir2.c_str()));
  require_tiledb_ok(tiledb_vfs_touch(ctx_, vfs_, file.c_str()));
  require_tiledb_ok(tiledb_vfs_touch(ctx_, vfs_, file2.c_str()));
  require_tiledb_ok(tiledb_vfs_touch(ctx_, vfs_, subdir_file.c_str()));
  require_tiledb_ok(tiledb_vfs_touch(ctx_, vfs_, subdir_file2.c_str()));

  // List
  std::vector<std::string> children;
  require_tiledb_ok(
      tiledb_vfs_ls(ctx_, vfs_, (dir + "/").c_str(), ls_getter, &children));

  // Normalization
  for (auto& child : children) {
    if (child.back() == '/')
      child.pop_back();
  }

#ifdef _WIN32
  // Normalization only for Windows
  file = tiledb::sm::path_win::uri_from_path(file);
  file2 = tiledb::sm::path_win::uri_from_path(file2);
  subdir = tiledb::sm::path_win::uri_from_path(subdir);
  subdir2 = tiledb::sm::path_win::uri_from_path(subdir2);
#endif

  // Check results
  std::sort(children.begin(), children.end());
  REQUIRE(children.size() == 4);
  CHECK(children[0] == file);
  CHECK(children[1] == file2);
  CHECK(children[2] == subdir);
  CHECK(children[3] == subdir2);
}

TEST_CASE_METHOD(
    VFSFx,
    "C API: Test virtual filesystem when S3 is not supported",
    "[capi][vfs]") {
  if constexpr (!tiledb::sm::filesystem::s3_enabled) {
    tiledb_vfs_t* vfs;
    require_tiledb_ok(tiledb_vfs_alloc(ctx_, nullptr, &vfs));
    int rc = tiledb_vfs_create_bucket(ctx_, vfs, "s3://foo");
    REQUIRE(rc == TILEDB_ERR);
    tiledb_vfs_free(&vfs);
  }
}

TEST_CASE_METHOD(
    VFSFx, "C API: Test virtual filesystem config", "[capi][vfs]") {
  // Prepare a config
  tiledb_error_t* error = nullptr;
  tiledb_config_t* config;
  require_tiledb_ok(tiledb_config_alloc(&config, &error));
  REQUIRE(error == nullptr);
  require_tiledb_ok(
      tiledb_config_set(config, "vfs.s3.scheme", "https", &error));
  REQUIRE(error == nullptr);

  // Create VFS
  tiledb_vfs_t* vfs;
  require_tiledb_ok(tiledb_vfs_alloc(ctx_, config, &vfs));

  // Get VFS config and check
  tiledb_config_t* config2;
  require_tiledb_ok(tiledb_vfs_get_config(ctx_, vfs, &config2));
  const char* value;
  require_tiledb_ok(
      tiledb_config_get(config2, "vfs.s3.scheme", &value, &error));
  REQUIRE(error == nullptr);
  CHECK(!strncmp(value, "https", strlen("https")));

  // Clean up
  tiledb_config_free(&config);
  tiledb_config_free(&config2);
  tiledb_vfs_free(&vfs);
}

TEST_CASE_METHOD(VFSFx, "C API: Test virtual filesystem", "[capi][vfs]") {
  tiledb_stats_enable();
  tiledb_stats_reset();

  SECTION("Parallel I/O with 4 threads") {
    tiledb_config_t* config;
    require_tiledb_ok(tiledb_vfs_get_config(ctx_, vfs_, &config));

    // Set number of threads to 4.
    tiledb_error_t* error = nullptr;
    require_tiledb_ok(tiledb_config_set(
        config, "vfs.s3.max_parallel_ops", std::to_string(4).c_str(), &error));
    // Set small parallelization threshold (ignored when there is only 1 thread)
    require_tiledb_ok(tiledb_config_set(
        config, "vfs.min_parallel_size", std::to_string(1).c_str(), &error));
    REQUIRE(error == nullptr);
  }

  // Sections to test each filesystem, if enabled
  std::string path = "";
  if constexpr (tiledb::sm::filesystem::hdfs_enabled) {
    SECTION("Filesystem: HDFS") {
      path = "hdfs://localhost:9000/tiledb_test/";
    }
  }

  std::string s3_bucket;
  if constexpr (tiledb::sm::filesystem::s3_enabled) {
    SECTION("Filesystem: S3") {
      path = "s3://" + random_name("tiledb") + "/tiledb_test/";
      s3_bucket = path.substr(0, path.find("tiledb_test/"));

      // Check S3 bucket functionality
      int is_bucket = 0;
      require_tiledb_ok(
          tiledb_vfs_is_bucket(ctx_, vfs_, s3_bucket.c_str(), &is_bucket));
      if (is_bucket) {
        require_tiledb_ok(
            tiledb_vfs_remove_bucket(ctx_, vfs_, s3_bucket.c_str()));
      }
      require_tiledb_ok(
          tiledb_vfs_is_bucket(ctx_, vfs_, s3_bucket.c_str(), &is_bucket));
      REQUIRE(!is_bucket);

      require_tiledb_ok(
          tiledb_vfs_create_bucket(ctx_, vfs_, s3_bucket.c_str()));
      require_tiledb_ok(
          tiledb_vfs_is_bucket(ctx_, vfs_, s3_bucket.c_str(), &is_bucket));
      REQUIRE(is_bucket);
    }
  }

  // Note: Azure not currently tested...?
  if constexpr (tiledb::sm::filesystem::azure_enabled) {
    SECTION("Filesystem: Azure") {
      path = "azure://" + random_name("tiledb") + "/tiledb_test/";
    }
  }

  std::string local_prefix = "";
  SECTION("Filesystem: Local") {
    if constexpr (!tiledb::sm::filesystem::windows_enabled) {
      local_prefix = "file://";
    }
    path = local_prefix + temp_dir_.path();
  }

  SECTION("Filesystem: MemFS") {
    path = "mem://tiledb_test/";
  }

  // Check VFS operations
  if (!path.empty()) {
    auto backend_name = tiledb::sm::utils::parse::backend_name(path);

    // Create directory, is directory, remove directory
    int is_dir = 0;
    require_tiledb_ok(tiledb_vfs_is_dir(ctx_, vfs_, path.c_str(), &is_dir));
    if (is_dir) {
      require_tiledb_ok(tiledb_vfs_remove_dir(ctx_, vfs_, path.c_str()));
    }
    require_tiledb_ok(tiledb_vfs_is_dir(ctx_, vfs_, path.c_str(), &is_dir));
    REQUIRE(!is_dir);
    require_tiledb_ok(tiledb_vfs_create_dir(ctx_, vfs_, path.c_str()));
    require_tiledb_ok(tiledb_vfs_is_dir(ctx_, vfs_, path.c_str(), &is_dir));
    if (backend_name == "s3")
      REQUIRE(!is_dir);  // No empty dirs exist in S3
    else
      REQUIRE(is_dir);
    // Second time succeeds as well
    require_tiledb_ok(tiledb_vfs_create_dir(ctx_, vfs_, path.c_str()));

    // Remove directory recursively
    auto subdir = path + "subdir/";
    require_tiledb_ok(tiledb_vfs_create_dir(ctx_, vfs_, subdir.c_str()));
    require_tiledb_ok(tiledb_vfs_is_dir(ctx_, vfs_, subdir.c_str(), &is_dir));
    if (backend_name == "s3")
      REQUIRE(!is_dir);  // No empty dirs exist in S3
    else
      REQUIRE(is_dir);
    require_tiledb_ok(tiledb_vfs_remove_dir(ctx_, vfs_, path.c_str()));
    require_tiledb_ok(tiledb_vfs_is_dir(ctx_, vfs_, path.c_str(), &is_dir));
    REQUIRE(!is_dir);
    require_tiledb_ok(tiledb_vfs_is_dir(ctx_, vfs_, subdir.c_str(), &is_dir));
    REQUIRE(!is_dir);

    // Move
    require_tiledb_ok(tiledb_vfs_create_dir(ctx_, vfs_, path.c_str()));
    require_tiledb_ok(tiledb_vfs_create_dir(ctx_, vfs_, subdir.c_str()));
    require_tiledb_ok(tiledb_vfs_is_dir(ctx_, vfs_, subdir.c_str(), &is_dir));
    if (backend_name == "s3")
      REQUIRE(!is_dir);  // No empty dirs exist in S3
    else
      REQUIRE(is_dir);
    int is_file = 0;
    std::string some_file = subdir + "some_file";
    require_tiledb_ok(tiledb_vfs_touch(ctx_, vfs_, some_file.c_str()));
    require_tiledb_ok(
        tiledb_vfs_is_file(ctx_, vfs_, some_file.c_str(), &is_file));
    auto subdir2 = path + "subdir2/";
    require_tiledb_ok(
        tiledb_vfs_move_dir(ctx_, vfs_, subdir.c_str(), subdir2.c_str()));
    require_tiledb_ok(tiledb_vfs_is_dir(ctx_, vfs_, subdir.c_str(), &is_dir));
    REQUIRE(!is_dir);
    require_tiledb_ok(tiledb_vfs_is_dir(ctx_, vfs_, subdir2.c_str(), &is_dir));
    REQUIRE(is_dir);

    // Invalid file
    std::string foo_file = path + "foo";
    require_tiledb_ok(
        tiledb_vfs_is_file(ctx_, vfs_, foo_file.c_str(), &is_file));
    REQUIRE(!is_file);
    tiledb_vfs_fh_t* fh = nullptr;
    int rc =
        tiledb_vfs_open(ctx_, vfs_, foo_file.c_str(), TILEDB_VFS_READ, &fh);
    REQUIRE(rc == TILEDB_ERR);
    REQUIRE(fh == nullptr);

    // Touch file
    require_tiledb_ok(tiledb_vfs_touch(ctx_, vfs_, foo_file.c_str()));
    require_tiledb_ok(
        tiledb_vfs_is_file(ctx_, vfs_, foo_file.c_str(), &is_file));
    REQUIRE(is_file);
    require_tiledb_ok(tiledb_vfs_remove_file(ctx_, vfs_, foo_file.c_str()));
    require_tiledb_ok(
        tiledb_vfs_is_file(ctx_, vfs_, foo_file.c_str(), &is_file));
    REQUIRE(!is_file);

    // Check write and append
    check_write(path);
    check_append(path);

    // Read file
    check_read(path);

    // Move
    check_move(path);

    // Copy (not yet supported for memfs)
    if constexpr (!tiledb::sm::filesystem::windows_enabled) {
      if (backend_name != "mem") {
        check_copy(path);
      }
    }

    // Ls
    check_ls(path);

    if (backend_name == "s3") {
      int is_empty;
      require_tiledb_ok(
          tiledb_vfs_is_empty_bucket(ctx_, vfs_, s3_bucket.c_str(), &is_empty));
      REQUIRE(!(bool)is_empty);
    }

    if constexpr (!tiledb::sm::filesystem::s3_enabled) {
      if (backend_name != "windows" && backend_name != "posix") {
        require_tiledb_ok(tiledb_vfs_remove_dir(ctx_, vfs_, path.c_str()));
      }
    }

    if (backend_name == "s3") {
      require_tiledb_ok(tiledb_vfs_empty_bucket(ctx_, vfs_, s3_bucket.c_str()));

      int is_empty;
      require_tiledb_ok(
          tiledb_vfs_is_empty_bucket(ctx_, vfs_, s3_bucket.c_str(), &is_empty));
      REQUIRE((bool)is_empty);

      require_tiledb_ok(
          tiledb_vfs_remove_bucket(ctx_, vfs_, s3_bucket.c_str()));
    }
  }
}
