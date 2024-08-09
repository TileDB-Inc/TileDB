/**
 * @file   unit-capi-vfs.cc
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
 * Tests the C API VFS object.
 */

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include "test/support/src/helpers.h"
#include "test/support/src/temporary_local_directory.h"
#include "test/support/src/vfs_helpers.h"
#include "tiledb/api/c_api/vfs/vfs_api_internal.h"
#include "tiledb/api/c_api_test_support/testsupport_capi_vfs.h"
#include "tiledb/platform/platform.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/filesystem/uri.h"
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
using tiledb::api::test_support::ordinary_vfs;

// The unique directory object
tiledb::sm::TemporaryLocalDirectory temp_dir_{"tiledb_test_"};

void require_tiledb_ok(capi_return_t rc) {
  REQUIRE(rc == TILEDB_OK);
}

void require_tiledb_err(capi_return_t rc) {
  REQUIRE(rc == TILEDB_ERR);
}

int ls_getter(const char* path, void* data) {
  auto vec = static_cast<std::vector<std::string>*>(data);
  vec->emplace_back(path);
  return 1;
}

TEST_CASE(
    "C API: Test virtual filesystem when S3 is not supported", "[capi][vfs]") {
  if constexpr (!tiledb::sm::filesystem::s3_enabled) {
    ordinary_vfs x;
    require_tiledb_err(tiledb_vfs_create_bucket(x.ctx, x.vfs, "s3://foo"));
  }
}

TEST_CASE("C API: Test virtual filesystem config", "[capi][vfs]") {
  // Prepare a config
  tiledb_error_t* error = nullptr;
  tiledb_config_t* config;
  require_tiledb_ok(tiledb_config_alloc(&config, &error));
  REQUIRE(error == nullptr);
  require_tiledb_ok(
      tiledb_config_set(config, "vfs.s3.scheme", "https", &error));
  REQUIRE(error == nullptr);

  // Create VFS
  ordinary_vfs x(config);

  // Get VFS config and check
  tiledb_config_t* config2;
  require_tiledb_ok(tiledb_vfs_get_config(x.ctx, x.vfs, &config2));
  const char* value;
  require_tiledb_ok(
      tiledb_config_get(config2, "vfs.s3.scheme", &value, &error));
  REQUIRE(error == nullptr);
  CHECK(!strncmp(value, "https", strlen("https")));

  // Clean up
  tiledb_config_free(&config);
  tiledb_config_free(&config2);
}

TEST_CASE("C API: Test virtual filesystem", "[capi][vfs]") {
  tiledb_stats_enable();
  tiledb_stats_reset();

  vfs_config v;
  auto config = v.config;
  tiledb_error_t* error = nullptr;

  SECTION("Parallel I/O with 4 threads") {
    // Set number of threads to 4.
    require_tiledb_ok(tiledb_config_set(
        config, "vfs.s3.max_parallel_ops", std::to_string(4).c_str(), &error));
    // Set small parallelization threshold (ignored when there is only 1 thread)
    require_tiledb_ok(tiledb_config_set(
        config, "vfs.min_parallel_size", std::to_string(1).c_str(), &error));
    REQUIRE(error == nullptr);
  }

  // Get enabled filesystems
  // Note: get_supported_fs handles VFS support override from `--vfs`.
  bool s3_enabled;
  bool hdfs_enabled;
  bool azure_enabled;
  bool gcs_enabled;
  bool rest_s3_enabled;
  get_supported_fs(
      &s3_enabled,
      &hdfs_enabled,
      &azure_enabled,
      &gcs_enabled,
      &rest_s3_enabled);

  // Sections to test each filesystem, if enabled
  ordinary_vfs x(config);
  std::string path = "";
  if (hdfs_enabled) {
    SECTION("Filesystem: HDFS") {
      path = "hdfs://localhost:9000/tiledb_test/";
    }
  }

  std::string s3_bucket;
  if (s3_enabled) {
    SECTION("Filesystem: S3") {
      path = "s3://tiledb-" + random_label() + "/tiledb_test/";
      s3_bucket = path.substr(0, path.find("tiledb_test/"));

      // Check S3 bucket functionality
      int is_bucket = 0;
      require_tiledb_ok(
          tiledb_vfs_is_bucket(x.ctx, x.vfs, s3_bucket.c_str(), &is_bucket));
      if (is_bucket) {
        require_tiledb_ok(
            tiledb_vfs_remove_bucket(x.ctx, x.vfs, s3_bucket.c_str()));
      }
      require_tiledb_ok(
          tiledb_vfs_is_bucket(x.ctx, x.vfs, s3_bucket.c_str(), &is_bucket));
      REQUIRE(!is_bucket);

      require_tiledb_ok(
          tiledb_vfs_create_bucket(x.ctx, x.vfs, s3_bucket.c_str()));
      require_tiledb_ok(
          tiledb_vfs_is_bucket(x.ctx, x.vfs, s3_bucket.c_str(), &is_bucket));
      REQUIRE(is_bucket);
    }
  }

  /** Note: Azure testing not currently enabled.
  if (azure_enabled) {
    SECTION("Filesystem: Azure") {
      path = "azure://tiledb-" + random_label() + "/tiledb_test/";
    }
  } */

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
    bool backend_is_s3 = tiledb::sm::URI::is_s3(path);

    // Create directory
    int is_dir = 0;
    require_tiledb_ok(tiledb_vfs_is_dir(x.ctx, x.vfs, path.c_str(), &is_dir));
    if (is_dir) {
      require_tiledb_ok(tiledb_vfs_remove_dir(x.ctx, x.vfs, path.c_str()));
    }
    require_tiledb_ok(tiledb_vfs_is_dir(x.ctx, x.vfs, path.c_str(), &is_dir));
    REQUIRE(!is_dir);
    require_tiledb_ok(tiledb_vfs_create_dir(x.ctx, x.vfs, path.c_str()));
    require_tiledb_ok(tiledb_vfs_is_dir(x.ctx, x.vfs, path.c_str(), &is_dir));
    if (backend_is_s3)
      REQUIRE(!is_dir);  // No empty dirs exist in S3
    else
      REQUIRE(is_dir);
    // Second time succeeds as well
    require_tiledb_ok(tiledb_vfs_create_dir(x.ctx, x.vfs, path.c_str()));

    // Create a subdirectory
    auto subdir = path + "subdir/";
    require_tiledb_ok(tiledb_vfs_create_dir(x.ctx, x.vfs, subdir.c_str()));

    // Touch
    auto file = path + "file";
    int is_file = 0;
    require_tiledb_ok(tiledb_vfs_touch(x.ctx, x.vfs, file.c_str()));
    require_tiledb_ok(tiledb_vfs_is_file(x.ctx, x.vfs, file.c_str(), &is_file));
    REQUIRE(is_file);

    // Invalid file
    std::string foo_file = path + "foo";
    require_tiledb_ok(
        tiledb_vfs_is_file(x.ctx, x.vfs, foo_file.c_str(), &is_file));
    REQUIRE(!is_file);
    tiledb_vfs_fh_t* fh = nullptr;
    int rc =
        tiledb_vfs_open(x.ctx, x.vfs, foo_file.c_str(), TILEDB_VFS_READ, &fh);
    REQUIRE(rc == TILEDB_ERR);
    REQUIRE(fh == nullptr);

    // Check ls
    std::vector<std::string> children;
    require_tiledb_ok(
        tiledb_vfs_ls(x.ctx, x.vfs, (path).c_str(), ls_getter, &children));
    std::sort(children.begin(), children.end());
#ifdef _WIN32
    // Normalization only for Windows
    file = tiledb::sm::path_win::uri_from_path(file);
    subdir = tiledb::sm::path_win::uri_from_path(subdir);
#endif
    CHECK(children[0] == file);
    if (!backend_is_s3) {
      CHECK(children[1] + "/" == subdir);
    }

    // Check write
    // File write and file size
    require_tiledb_ok(tiledb_vfs_remove_file(x.ctx, x.vfs, file.c_str()));
    require_tiledb_ok(tiledb_vfs_is_file(x.ctx, x.vfs, file.c_str(), &is_file));
    REQUIRE(!is_file);
    std::string to_write = "This will be written to the file";
    require_tiledb_ok(
        tiledb_vfs_open(x.ctx, x.vfs, file.c_str(), TILEDB_VFS_WRITE, &fh));
    int is_closed = 0;
    require_tiledb_ok(tiledb_vfs_fh_is_closed(x.ctx, fh, &is_closed));
    REQUIRE(is_closed == 0);
    require_tiledb_ok(
        tiledb_vfs_write(x.ctx, fh, to_write.c_str(), to_write.size()));
    require_tiledb_ok(tiledb_vfs_sync(x.ctx, fh));
    require_tiledb_ok(tiledb_vfs_is_file(x.ctx, x.vfs, file.c_str(), &is_file));

    // Only for S3, sync still does not create the file
    uint64_t file_size = 0;
    if (backend_is_s3) {
      REQUIRE(!is_file);
    } else {
      REQUIRE(is_file);
      require_tiledb_ok(
          tiledb_vfs_file_size(x.ctx, x.vfs, file.c_str(), &file_size));
      REQUIRE(file_size == to_write.size());
    }

    // Close file
    require_tiledb_ok(tiledb_vfs_close(x.ctx, fh));
    require_tiledb_ok(tiledb_vfs_fh_is_closed(x.ctx, fh, &is_closed));
    REQUIRE(is_closed == 1);
    tiledb_vfs_fh_free(&fh);
    require_tiledb_ok(tiledb_vfs_is_file(x.ctx, x.vfs, file.c_str(), &is_file));
    REQUIRE(is_file);  // It is a file even for S3
    require_tiledb_ok(
        tiledb_vfs_file_size(x.ctx, x.vfs, file.c_str(), &file_size));
    REQUIRE(file_size == to_write.size());

    // Write another file in a subdir
    auto file2 = subdir + "file2";
    require_tiledb_ok(
        tiledb_vfs_is_file(x.ctx, x.vfs, file2.c_str(), &is_file));
    if (is_file) {
      require_tiledb_ok(tiledb_vfs_remove_file(x.ctx, x.vfs, file2.c_str()));
    }
    tiledb_vfs_fh_t* fh2;
    require_tiledb_ok(
        tiledb_vfs_open(x.ctx, x.vfs, file2.c_str(), TILEDB_VFS_WRITE, &fh2));
    require_tiledb_ok(
        tiledb_vfs_write(x.ctx, fh2, to_write.c_str(), to_write.size()));
    require_tiledb_ok(tiledb_vfs_close(x.ctx, fh2));
    tiledb_vfs_fh_free(&fh2);

    // Directory size
    uint64_t dir_size;
    require_tiledb_ok(
        tiledb_vfs_dir_size(x.ctx, x.vfs, path.c_str(), &dir_size));
    CHECK(dir_size == 2 * to_write.size());

    // Check correctness with read
    std::string to_read;
    to_read.resize(to_write.size());
    require_tiledb_ok(
        tiledb_vfs_open(x.ctx, x.vfs, file.c_str(), TILEDB_VFS_READ, &fh));
    require_tiledb_ok(tiledb_vfs_read(x.ctx, fh, 0, &to_read[0], file_size));
    CHECK_THAT(to_read, Catch::Matchers::Equals(to_write));
    require_tiledb_ok(tiledb_vfs_close(x.ctx, fh));
    tiledb_vfs_fh_free(&fh);

    // Read only the "will be written" portion of the file
    std::string to_check = "will be written";
    to_read.resize(to_check.size());
    uint64_t offset = 5;
    require_tiledb_ok(
        tiledb_vfs_open(x.ctx, x.vfs, file.c_str(), TILEDB_VFS_READ, &fh));
    require_tiledb_ok(
        tiledb_vfs_read(x.ctx, fh, offset, &to_read[0], to_check.size()));
    CHECK_THAT(to_read, Catch::Matchers::Equals(to_check));
    require_tiledb_ok(tiledb_vfs_close(x.ctx, fh));
    tiledb_vfs_fh_free(&fh);

    // Check append
    std::string to_write_2 = "This will be appended to the end of the file";
    rc = tiledb_vfs_open(x.ctx, x.vfs, file.c_str(), TILEDB_VFS_APPEND, &fh);
    if (backend_is_s3) {  // S3 does not support append
      REQUIRE(rc == TILEDB_ERR);
      REQUIRE(fh == nullptr);
    } else {
      REQUIRE(rc == TILEDB_OK);
      require_tiledb_ok(
          tiledb_vfs_write(x.ctx, fh, to_write_2.c_str(), to_write_2.size()));
      require_tiledb_ok(tiledb_vfs_close(x.ctx, fh));
      tiledb_vfs_fh_free(&fh);
      uint64_t file_size = 0;
      require_tiledb_ok(
          tiledb_vfs_file_size(x.ctx, x.vfs, file.c_str(), &file_size));
      uint64_t total_size = to_write.size() + to_write_2.size();
      CHECK(file_size == total_size);

      // Check correctness with read
      std::string to_read;
      to_read.resize(total_size);
      require_tiledb_ok(
          tiledb_vfs_open(x.ctx, x.vfs, file.c_str(), TILEDB_VFS_READ, &fh));
      require_tiledb_ok(tiledb_vfs_read(x.ctx, fh, 0, &to_read[0], total_size));
      CHECK_THAT(to_read, Catch::Matchers::Equals(to_write + to_write_2));
      require_tiledb_ok(tiledb_vfs_close(x.ctx, fh));
      tiledb_vfs_fh_free(&fh);
    }

    // Open in WRITE mode again - previous file will be removed
    require_tiledb_ok(
        tiledb_vfs_open(x.ctx, x.vfs, file.c_str(), TILEDB_VFS_WRITE, &fh));
    require_tiledb_ok(
        tiledb_vfs_write(x.ctx, fh, to_write.c_str(), to_write.size()));
    require_tiledb_ok(tiledb_vfs_close(x.ctx, fh));
    tiledb_vfs_fh_free(&fh);
    require_tiledb_ok(
        tiledb_vfs_file_size(x.ctx, x.vfs, file.c_str(), &file_size));
    REQUIRE(file_size == to_write.size());  // Not 2*to_write.size()

    // Opening and closing the file without writing, first deletes previous
    // file, and then creates an empty file
    require_tiledb_ok(
        tiledb_vfs_open(x.ctx, x.vfs, file.c_str(), TILEDB_VFS_WRITE, &fh));
    require_tiledb_ok(tiledb_vfs_close(x.ctx, fh));
    tiledb_vfs_fh_free(&fh);
    require_tiledb_ok(tiledb_vfs_is_file(x.ctx, x.vfs, file.c_str(), &is_file));
    REQUIRE(is_file);
    require_tiledb_ok(
        tiledb_vfs_file_size(x.ctx, x.vfs, file.c_str(), &file_size));
    REQUIRE(file_size == 0);  // Not 2*to_write.size()

    // Move file
    file2 = subdir + "file";
    require_tiledb_ok(
        tiledb_vfs_move_file(x.ctx, x.vfs, file.c_str(), file2.c_str()));
    require_tiledb_ok(tiledb_vfs_is_file(x.ctx, x.vfs, file.c_str(), &is_file));
    REQUIRE(!is_file);
    require_tiledb_ok(
        tiledb_vfs_is_file(x.ctx, x.vfs, file2.c_str(), &is_file));
    REQUIRE(is_file);

    // Move directory
    auto subdir2 = path + "subdir2/";
    require_tiledb_ok(
        tiledb_vfs_move_dir(x.ctx, x.vfs, subdir.c_str(), subdir2.c_str()));
    require_tiledb_ok(tiledb_vfs_is_dir(x.ctx, x.vfs, subdir.c_str(), &is_dir));
    REQUIRE(!is_dir);
    require_tiledb_ok(
        tiledb_vfs_is_dir(x.ctx, x.vfs, subdir2.c_str(), &is_dir));
    file2 = subdir2 + "file";
    require_tiledb_ok(
        tiledb_vfs_is_file(x.ctx, x.vfs, file2.c_str(), &is_file));
    REQUIRE(is_file);

    // Move from one bucket to another (only for S3)
    if (backend_is_s3) {
      std::string bucket2 = "s3://tiledb-" + random_label() + "/";
      std::string subdir3 = bucket2 + "tiledb_test/subdir3/";

      // Remove and recreate bucket if already exists
      int is_bucket = 0;
      require_tiledb_ok(
          tiledb_vfs_is_bucket(x.ctx, x.vfs, bucket2.c_str(), &is_bucket));
      if (is_bucket) {
        require_tiledb_ok(
            tiledb_vfs_remove_bucket(x.ctx, x.vfs, bucket2.c_str()));
      }
      require_tiledb_ok(
          tiledb_vfs_create_bucket(x.ctx, x.vfs, bucket2.c_str()));

      // Move bucket
      require_tiledb_ok(
          tiledb_vfs_move_dir(x.ctx, x.vfs, subdir2.c_str(), subdir3.c_str()));
      require_tiledb_ok(
          tiledb_vfs_is_dir(x.ctx, x.vfs, subdir3.c_str(), &is_dir));
      REQUIRE(is_dir);
      file2 = subdir3 + "file";
      require_tiledb_ok(
          tiledb_vfs_is_file(x.ctx, x.vfs, file2.c_str(), &is_file));
      REQUIRE(is_file);

      // Move back
      require_tiledb_ok(
          tiledb_vfs_move_dir(x.ctx, x.vfs, subdir3.c_str(), subdir2.c_str()));
      require_tiledb_ok(
          tiledb_vfs_is_dir(x.ctx, x.vfs, subdir2.c_str(), &is_dir));
      REQUIRE(is_dir);

      // Remove bucket
      require_tiledb_ok(
          tiledb_vfs_remove_bucket(x.ctx, x.vfs, bucket2.c_str()));
    }

    // Check copy (not yet supported for MemFS or HDFS)
    if constexpr (!tiledb::platform::is_os_windows) {
      if (!tiledb::sm::URI::is_memfs(path) && !tiledb::sm::URI::is_hdfs(path)) {
        auto dir = path + "dir/";
        auto file = dir + "file";
        int is_file = 0;
        require_tiledb_ok(tiledb_vfs_create_dir(x.ctx, x.vfs, dir.c_str()));
        require_tiledb_ok(tiledb_vfs_touch(x.ctx, x.vfs, file.c_str()));
        int is_dir = 0;
        require_tiledb_ok(
            tiledb_vfs_is_dir(x.ctx, x.vfs, dir.c_str(), &is_dir));
        REQUIRE(is_dir);
        require_tiledb_ok(
            tiledb_vfs_is_file(x.ctx, x.vfs, file.c_str(), &is_file));
        REQUIRE(is_file);

        // Copy file
        auto file2 = dir + "file2";
        require_tiledb_ok(
            tiledb_vfs_copy_file(x.ctx, x.vfs, file.c_str(), file2.c_str()));
        require_tiledb_ok(
            tiledb_vfs_is_file(x.ctx, x.vfs, file.c_str(), &is_file));
        REQUIRE(is_file);
        require_tiledb_ok(
            tiledb_vfs_is_file(x.ctx, x.vfs, file2.c_str(), &is_file));
        REQUIRE(is_file);

        // Copy directory
        auto dir2 = path + "dir2/";
        file2 = dir2 + "file2";
        require_tiledb_ok(
            tiledb_vfs_copy_dir(x.ctx, x.vfs, dir.c_str(), dir2.c_str()));
        require_tiledb_ok(
            tiledb_vfs_is_dir(x.ctx, x.vfs, dir2.c_str(), &is_dir));
        REQUIRE(is_dir);
        require_tiledb_ok(
            tiledb_vfs_is_file(x.ctx, x.vfs, file2.c_str(), &is_file));
        REQUIRE(is_file);

        // Copy from one bucket to another (only for S3)
        if (backend_is_s3) {
          std::string bucket2 = "s3://tiledb-" + random_label() + "/";
          std::string subdir3 = bucket2 + "tiledb_test/subdir3/";

          // Remove and recreate bucket if already exists
          int is_bucket = 0;
          require_tiledb_ok(
              tiledb_vfs_is_bucket(x.ctx, x.vfs, bucket2.c_str(), &is_bucket));
          if (is_bucket) {
            require_tiledb_ok(
                tiledb_vfs_remove_bucket(x.ctx, x.vfs, bucket2.c_str()));
          }
          require_tiledb_ok(
              tiledb_vfs_create_bucket(x.ctx, x.vfs, bucket2.c_str()));

          // Copy bucket
          require_tiledb_ok(
              tiledb_vfs_copy_dir(x.ctx, x.vfs, dir2.c_str(), subdir3.c_str()));
          require_tiledb_ok(
              tiledb_vfs_is_dir(x.ctx, x.vfs, subdir3.c_str(), &is_dir));
          REQUIRE(is_dir);
          file2 = subdir3 + "file";
          require_tiledb_ok(
              tiledb_vfs_is_file(x.ctx, x.vfs, file2.c_str(), &is_file));
          REQUIRE(is_file);

          // Remove bucket
          require_tiledb_ok(
              tiledb_vfs_remove_bucket(x.ctx, x.vfs, bucket2.c_str()));
        }
      }
    }

    // Clean up
    if (backend_is_s3) {
      // Empty s3 bucket
      int is_empty;
      require_tiledb_ok(tiledb_vfs_is_empty_bucket(
          x.ctx, x.vfs, s3_bucket.c_str(), &is_empty));
      REQUIRE(!(bool)is_empty);

      require_tiledb_ok(
          tiledb_vfs_empty_bucket(x.ctx, x.vfs, s3_bucket.c_str()));

      require_tiledb_ok(tiledb_vfs_is_empty_bucket(
          x.ctx, x.vfs, s3_bucket.c_str(), &is_empty));
      REQUIRE((bool)is_empty);

      require_tiledb_ok(
          tiledb_vfs_remove_bucket(x.ctx, x.vfs, s3_bucket.c_str()));
    } else {
      // Remove directory recursively
      require_tiledb_ok(tiledb_vfs_is_dir(x.ctx, x.vfs, path.c_str(), &is_dir));
      REQUIRE(is_dir);
      require_tiledb_ok(
          tiledb_vfs_is_dir(x.ctx, x.vfs, subdir2.c_str(), &is_dir));
      REQUIRE(is_dir);
      require_tiledb_ok(tiledb_vfs_remove_dir(x.ctx, x.vfs, path.c_str()));
      require_tiledb_ok(tiledb_vfs_is_dir(x.ctx, x.vfs, path.c_str(), &is_dir));
      REQUIRE(!is_dir);
      require_tiledb_ok(
          tiledb_vfs_is_dir(x.ctx, x.vfs, subdir2.c_str(), &is_dir));
      REQUIRE(!is_dir);
    }
  }
}
