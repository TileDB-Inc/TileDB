/**
 * @file   unit-vfs.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2023 TileDB, Inc.
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
 * Tests the `VFS` class.
 */

#include <test/support/tdb_catch.h>
#include "test/support/src/helpers.h"
#include "test/support/src/temporary_local_directory.h"
#include "test/support/src/vfs_helpers.h"
#include "tiledb/sm/filesystem/vfs.h"
#ifdef _WIN32
#include "tiledb/sm/filesystem/path_win.h"
#endif
#include "tiledb/sm/tile/tile.h"

#include <atomic>

using namespace tiledb::common;
using namespace tiledb::sm;
using namespace tiledb::test;

// The unique local directory object
tiledb::sm::TemporaryLocalDirectory unit_vfs_dir_{"tiledb_test"};

void require_tiledb_ok(Status st) {
  REQUIRE(st.ok());
}

void require_tiledb_err(Status st) {
  REQUIRE(!st.ok());
}

Config set_config_params() {
  Config config;

  if constexpr (tiledb::sm::filesystem::gcs_enabled) {
    require_tiledb_ok(config.set("vfs.gcs.project_id", "TODO"));
  }

  if constexpr (tiledb::sm::filesystem::s3_enabled) {
    require_tiledb_ok(config.set("vfs.s3.endpoint_override", "localhost:9999"));
    require_tiledb_ok(config.set("vfs.s3.scheme", "https"));
    require_tiledb_ok(config.set("vfs.s3.use_virtual_addressing", "false"));
    require_tiledb_ok(config.set("vfs.s3.verify_ssl", "false"));
  }

  if constexpr (tiledb::sm::filesystem::azure_enabled) {
    require_tiledb_ok(
        config.set("vfs.azure.storage_account_name", "devstoreaccount1"));
    require_tiledb_ok(config.set(
        "vfs.azure.storage_account_key",
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
        "K1SZFPTOtr/KBHBeksoGMGw=="));
    require_tiledb_ok(config.set(
        "vfs.azure.blob_endpoint", "http://127.0.0.1:10000/devstoreaccount1"));
    // Currently disabled because it does not work with the Azurite emulator
    // The SAS path was manually tested against the Azure Blob Service.
    // require_tiledb_ok(config.set(
    //   "vfs.azure.storage_account_name", "devstoreaccount2"));
    // require_tiledb_ok(config.set("vfs.azure.storage_sas_token", ""));
    // require_tiledb_ok(config.set(
    //   "vfs.azure.blob_endpoint", "http://127.0.0.1:10000/devstoreaccount2"));
  }

  return config;
}

TEST_CASE("VFS: Test long local paths", "[vfs]") {
  ThreadPool compute_tp(4);
  ThreadPool io_tp(4);
  VFS vfs{&g_helper_stats, &compute_tp, &io_tp, Config{}};

  SECTION("- Deep hierarchy") {
    // Create a nested path with a long total length
    std::string local_prefix = "";
    if constexpr (!tiledb::sm::filesystem::windows_enabled) {
      local_prefix = "file://";
    }
    std::string tmpdir = local_prefix + unit_vfs_dir_.path();
    bool success = true;
    while (tmpdir.size() < 512) {
      tmpdir += "subdir/";
      success &= vfs.create_dir(URI(tmpdir)).ok();
      if constexpr (tiledb::sm::filesystem::posix_enabled) {
        REQUIRE(success);
      }
    }

    // On some Windows platforms, the path length of a directory must be <= 248
    // chars. On others (that have opted in to a configuration that allows
    // long paths) the limit is ~32,767. Here we check for either case.
    if (success) {
      // Check we can create files within the deep hierarchy
      URI testfile(tmpdir + "file.txt");
      REQUIRE(!testfile.is_invalid());
      bool exists = false;
      require_tiledb_ok(vfs.is_file(testfile, &exists));
      if (exists) {
        require_tiledb_ok(vfs.remove_file(testfile));
      }
      require_tiledb_ok(vfs.touch(testfile));
      require_tiledb_ok(vfs.remove_file(testfile));
    } else {
      // Don't check anything; directory creation failed.
    }
  }

  SECTION("- Too long name") {
    // This may not be long enough on some filesystems to pass the fail check.
    std::string name;
    for (unsigned i = 0; i < 256; i++) {
      name += "x";
    }
    std::string local_prefix = "";
    if constexpr (!tiledb::sm::filesystem::windows_enabled) {
      local_prefix = "file://";
    }
    std::string tmpdir = local_prefix + unit_vfs_dir_.path();
    URI testfile(tmpdir + name);

    // Creating the URI and checking its existence is fine on posix
    if constexpr (tiledb::sm::filesystem::posix_enabled) {
      REQUIRE(!testfile.is_invalid());
      bool exists = false;
      require_tiledb_ok(vfs.is_file(testfile, &exists));

      // Creating the file is not
      require_tiledb_err(vfs.touch(testfile));
    }

    // Creating the URI is invalid on Win32 (failure to canonicalize path)
    if constexpr (tiledb::sm::filesystem::windows_enabled) {
      REQUIRE(testfile.is_invalid());
    }
  }
}

TEST_CASE("VFS: URI semantics", "[vfs][uri]") {
  ThreadPool compute_tp(4);
  ThreadPool io_tp(4);
  Config config = set_config_params();
  VFS vfs{&g_helper_stats, &compute_tp, &io_tp, config};

  // Sections to test each enabled filesystem
  URI path;
  std::string local_prefix = "";
  SECTION("Filesystem: Local") {
    if constexpr (!tiledb::sm::filesystem::windows_enabled) {
      local_prefix = "file://";
    }
    path = URI(local_prefix + unit_vfs_dir_.path());
  }

  if constexpr (tiledb::sm::filesystem::gcs_enabled) {
    SECTION("Filesystem: GCS") {
      path = URI("gcs://vfs-" + random_label() + "/");
    }
  }

  if constexpr (tiledb::sm::filesystem::s3_enabled) {
    SECTION("Filesystem: S3") {
      path = URI("s3://vfs-" + random_label() + "/");
    }
  }

  if constexpr (tiledb::sm::filesystem::hdfs_enabled) {
    SECTION("Filesystem: HDFS") {
      path = URI("hdfs:///vfs-" + random_label() + "/");
    }
  }

  if constexpr (tiledb::sm::filesystem::azure_enabled) {
    SECTION("Filesystem: Azure") {
      path = URI("azure://vfs-" + random_label() + "/");
    }
  }

  // Set up
  bool exists = false;
  if (path.is_gcs() || path.is_s3() || path.is_azure()) {
    require_tiledb_ok(vfs.is_bucket(path, &exists));
    if (exists) {
      require_tiledb_ok(vfs.remove_bucket(path));
    }
    require_tiledb_ok(vfs.create_bucket(path));
  } else {
    require_tiledb_ok(vfs.is_dir(path, &exists));
    if (exists) {
      require_tiledb_ok(vfs.remove_dir(path));
    }
    require_tiledb_ok(vfs.create_dir(path));
  }

  /* Create the following file hierarchy:
   *
   * path/dir1/subdir/file1
   * path/dir1/subdir/file2
   * path/dir1/file3
   * path/file4
   * path/file5
   */
  auto dir1 = URI(path.to_string() + "dir1/");
  auto subdir = URI(dir1.to_string() + "subdir/");
  auto file1 = URI(subdir.to_string() + "file1");
  auto file2 = URI(subdir.to_string() + "file2");
  auto file3 = URI(dir1.to_string() + "file3");
  auto file4 = URI(path.to_string() + "file4");
  auto file5 = URI(path.to_string() + "file5");
  require_tiledb_ok(vfs.create_dir(URI(dir1)));
  require_tiledb_ok(vfs.create_dir(URI(subdir)));
  require_tiledb_ok(vfs.touch(file1));
  require_tiledb_ok(vfs.touch(file2));
  require_tiledb_ok(vfs.touch(file3));
  require_tiledb_ok(vfs.touch(file4));
  require_tiledb_ok(vfs.touch(file5));

  /**
   * URI Semantics
   */
  {
    std::vector<std::string> expected_uri_names = {"file4", "file5", "dir1"};
    std::vector<URI> uris;
    require_tiledb_ok(vfs.ls(path, &uris));

    for (const auto& uri : uris) {
      // Ensure that the URIs do not contain a trailing backslash.
      REQUIRE(uri.to_string().back() != '/');

      // Get the trailing file/dir name.
      const size_t idx = uri.to_string().find_last_of('/');
      REQUIRE(idx != std::string::npos);
      const std::string trailing_name =
          uri.to_string().substr(idx + 1, uri.to_string().length());

      // Verify we expected this file/dir name.
      const auto iter = std::find(
          expected_uri_names.begin(), expected_uri_names.end(), trailing_name);
      REQUIRE(iter != expected_uri_names.end());

      // Erase it from the expected names vector to ensure
      // we see each expected name exactly once.
      REQUIRE(
          std::count(
              expected_uri_names.begin(),
              expected_uri_names.end(),
              trailing_name) == 1);
      expected_uri_names.erase(iter);
    }

    // Verify we found all expected file/dir names.
    REQUIRE(expected_uri_names.empty());
  }  // URI Semantics

  /**
   * File Management
   */
  {
    // Check invalid file
    require_tiledb_ok(vfs.is_file(URI(path.to_string() + "foo"), &exists));
    CHECK(!exists);

    // List with prefix
    std::vector<URI> paths;
    require_tiledb_ok(vfs.ls(path, &paths));
    CHECK(paths.size() == 3);
    paths.clear();
    require_tiledb_ok(vfs.ls(dir1, &paths));
    CHECK(paths.size() == 2);
    paths.clear();
    require_tiledb_ok(vfs.ls(subdir, &paths));
    CHECK(paths.size() == 2);
    paths.clear();

    // Check if a directory exists
    require_tiledb_ok(vfs.is_dir(file1, &exists));
    CHECK(!exists);  // Not a dir
    require_tiledb_ok(vfs.is_dir(file4, &exists));
    CHECK(!exists);  // Not a dir
    require_tiledb_ok(vfs.is_dir(dir1, &exists));
    CHECK(exists);  // This is viewed as a dir
    require_tiledb_ok(vfs.is_dir(URI(path.to_string() + "dir1"), &exists));
    CHECK(exists);  // This is viewed as a dir

    // Check ls_with_sizes
    std::string s = "abcdef";
    require_tiledb_ok(vfs.write(file3, s.data(), s.size()));
    require_tiledb_ok(vfs.close_file(file3));
    auto&& [status, opt_children] = vfs.ls_with_sizes(dir1);
    require_tiledb_ok(status);
    auto children = opt_children.value();
#ifdef _WIN32
    // Normalization only for Windows
    file3 = URI(tiledb::sm::path_win::uri_from_path(file3.to_string()));
#endif
    REQUIRE(children.size() == 2);
    CHECK(URI(children[0].path().native()) == file3);
    CHECK(URI(children[1].path().native()) == subdir.remove_trailing_slash());
    CHECK(children[0].file_size() == s.size());
    CHECK(children[1].file_size() == 0);  // Directories don't get a size

    // Move file
    auto file6 = URI(path.to_string() + "file6");
    require_tiledb_ok(vfs.move_file(file5, file6));
    require_tiledb_ok(vfs.is_file(file5, &exists));
    CHECK(!exists);
    require_tiledb_ok(vfs.is_file(file6, &exists));
    CHECK(exists);
    paths.clear();

    // Move directory
    auto dir2 = URI(path.to_string() + "dir2/");
    require_tiledb_ok(vfs.move_dir(dir1, URI(dir2)));
    require_tiledb_ok(vfs.is_dir(dir1, &exists));
    CHECK(!exists);
    require_tiledb_ok(vfs.is_dir(dir2, &exists));
    CHECK(exists);
    paths.clear();

    // Remove files
    require_tiledb_ok(vfs.remove_file(file4));
    require_tiledb_ok(vfs.is_file(file4, &exists));
    CHECK(!exists);
    require_tiledb_ok(vfs.remove_file(file6));
    require_tiledb_ok(vfs.is_file(file6, &exists));
    CHECK(!exists);

    // Remove directories
    require_tiledb_ok(vfs.remove_dir(dir2));
    require_tiledb_ok(vfs.is_file(file1, &exists));
    CHECK(!exists);
    require_tiledb_ok(vfs.is_file(file2, &exists));
    CHECK(!exists);
    require_tiledb_ok(vfs.is_file(file3, &exists));
    CHECK(!exists);
    require_tiledb_ok(vfs.is_dir(dir2, &exists));
    CHECK(!exists);
  }  // File Management

  // Clean up
  if (path.is_gcs() || path.is_s3() || path.is_azure()) {
    require_tiledb_ok(vfs.remove_bucket(path));
    require_tiledb_ok(vfs.is_bucket(path, &exists));
    REQUIRE(!exists);
  } else {
    require_tiledb_ok(vfs.remove_dir(path));
    require_tiledb_ok(vfs.is_dir(path, &exists));
    REQUIRE(!exists);
  }
}
