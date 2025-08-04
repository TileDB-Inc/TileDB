/**
 * @file   unit-vfs.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2025 TileDB, Inc.
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
#ifdef HAVE_S3
#include "tiledb/sm/filesystem/s3.h"
#endif
#ifdef HAVE_AZURE
#include <azure/storage/blobs.hpp>
#include "tiledb/sm/filesystem/azure.h"
#endif
#ifdef HAVE_GCS
#include <google/cloud/internal/credentials_impl.h>
#include <google/cloud/storage/client.h>
#include "tiledb/sm/filesystem/gcs.h"
#endif
#include "test/support/src/vfs_helpers.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/global_state/unit_test_config.h"
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

Config set_config_params(
    bool disable_multipart = false, uint64_t parallel_ops = 1) {
  Config config;

  if constexpr (tiledb::sm::filesystem::gcs_enabled) {
    require_tiledb_ok(config.set("vfs.gcs.project_id", "TODO"));
    if (parallel_ops != 1) {
      require_tiledb_ok(
          config.set("vfs.gcs.max_parallel_ops", std::to_string(parallel_ops)));
      require_tiledb_ok(config.set(
          "vfs.gcs.multi_part_size", std::to_string(4 * 1024 * 1024)));
    }
    if (disable_multipart) {
      require_tiledb_ok(
          config.set("vfs.gcs.max_parallel_ops", std::to_string(1)));
      require_tiledb_ok(config.set("vfs.gcs.use_multi_part_upload", "false"));
      require_tiledb_ok(config.set(
          "vfs.gcs.max_direct_upload_size", std::to_string(4 * 1024 * 1024)));
    }
  }

  if constexpr (tiledb::sm::filesystem::s3_enabled) {
    require_tiledb_ok(config.set("vfs.s3.endpoint_override", "localhost:9999"));
    require_tiledb_ok(config.set("vfs.s3.scheme", "https"));
    require_tiledb_ok(config.set("vfs.s3.use_virtual_addressing", "false"));
    require_tiledb_ok(config.set("vfs.s3.verify_ssl", "false"));
    if (disable_multipart) {
      require_tiledb_ok(config.set("vfs.s3.max_parallel_ops", "1"));
      require_tiledb_ok(config.set("vfs.s3.multipart_part_size", "10000000"));
      require_tiledb_ok(config.set("vfs.s3.use_multipart_upload", "false"));
    }
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
    if (parallel_ops != 1) {
      require_tiledb_ok(config.set(
          "vfs.azure.max_parallel_ops", std::to_string(parallel_ops)));
      require_tiledb_ok(config.set(
          "vfs.azure.block_list_block_size", std::to_string(4 * 1024 * 1024)));
    }
    if (disable_multipart) {
      require_tiledb_ok(config.set("vfs.azure.use_block_list_upload", "false"));
    }
  }

  return config;
}

std::string local_path() {
  std::string local_prefix = "";
  if constexpr (!tiledb::sm::filesystem::windows_enabled) {
    local_prefix = "file://";
  }

  return local_prefix + unit_vfs_dir_.path();
}

TEST_CASE("VFS: Test long local paths", "[vfs][long-paths]") {
  ThreadPool compute_tp(4);
  ThreadPool io_tp(4);
  VFS vfs{
      &g_helper_stats, g_helper_logger().get(), &compute_tp, &io_tp, Config{}};

  SECTION("- Deep hierarchy") {
    // Create a nested path with a long total length
    std::string tmpdir = local_path();
    bool success = true;
    while (tmpdir.size() < 512) {
      tmpdir += "subdir/";
      try {
        vfs.create_dir(URI(tmpdir));
      } catch (...) {
        success = false;
      }
      if constexpr (tiledb::sm::filesystem::posix_enabled) {
        REQUIRE(success);
      }
    }

    // On some Windows platforms, the path length of a directory must be <=
    // 248 chars. On others (that have opted in to a configuration that allows
    // long paths) the limit is ~32,767. Here we check for either case.
    if (success) {
      // Check we can create files within the deep hierarchy
      URI testfile(tmpdir + "file.txt");
      REQUIRE(!testfile.is_invalid());
      if (vfs.is_file(testfile)) {
        REQUIRE_NOTHROW(vfs.remove_file(testfile));
      }
      vfs.touch(testfile);
      REQUIRE_NOTHROW(vfs.remove_file(testfile));
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
    std::string tmpdir = local_path();
    URI testfile(tmpdir + name);

    // Creating the URI and checking its existence is fine on posix
    if constexpr (tiledb::sm::filesystem::posix_enabled) {
      REQUIRE(!testfile.is_invalid());
      REQUIRE_NOTHROW(vfs.is_file(testfile));

      // Creating the file is not
      REQUIRE_THROWS(vfs.touch(testfile));
    }

    // Creating the URI is invalid on Win32 (failure to canonicalize path)
    if constexpr (tiledb::sm::filesystem::windows_enabled) {
      REQUIRE(testfile.is_invalid());
    }
  }
}

using AllBackends = std::tuple<LocalFsTest, GCSTest, GSTest, S3Test, AzureTest>;
TEMPLATE_LIST_TEST_CASE(
    "VFS: URI semantics and file management", "[vfs][uri]", AllBackends) {
  TestType fs({0});
  if (!fs.is_supported()) {
    return;
  }

  ThreadPool compute_tp(4);
  ThreadPool io_tp(4);
  Config config = set_config_params();
  VFS vfs{
      &g_helper_stats, g_helper_logger().get(), &compute_tp, &io_tp, config};

  URI path = fs.temp_dir_.add_trailing_slash();

  // Set up
  if (path.is_gcs() || path.is_s3() || path.is_azure()) {
    if (vfs.is_bucket(path)) {
      REQUIRE_NOTHROW(vfs.remove_bucket(path));
    }
    REQUIRE_NOTHROW(vfs.create_bucket(path));
  } else {
    if (vfs.is_dir(path)) {
      REQUIRE_NOTHROW(vfs.remove_dir(path));
    }
    REQUIRE_NOTHROW(vfs.create_dir(path));
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
  REQUIRE_NOTHROW(vfs.create_dir(URI(dir1)));
  REQUIRE_NOTHROW(vfs.create_dir(URI(subdir)));
  REQUIRE_NOTHROW(vfs.touch(file1));
  REQUIRE_NOTHROW(vfs.touch(file2));
  REQUIRE_NOTHROW(vfs.touch(file3));
  REQUIRE_NOTHROW(vfs.touch(file4));
  REQUIRE_NOTHROW(vfs.touch(file5));

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
    CHECK(!vfs.is_file(URI(path.to_string() + "foo")));

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
    CHECK(!vfs.is_dir(file1));                          // Not a dir
    CHECK(!vfs.is_dir(file4));                          // Not a dir
    CHECK(vfs.is_dir(dir1));                            // Viewed as a dir
    CHECK(vfs.is_dir(URI(path.to_string() + "dir1")));  // Viewed as a dir

    // Check ls_with_sizes
    URI ls_dir = dir1;
    URI ls_subdir = subdir;
    URI ls_file = file3;
    std::string s = "abcdef";
    REQUIRE_NOTHROW(vfs.write(ls_file, s.data(), s.size()));
    require_tiledb_ok(vfs.close_file(ls_file));
    auto children = vfs.ls_with_sizes(ls_dir);
#ifdef _WIN32
    // Normalization only for Windows
    ls_file = URI(tiledb::sm::path_win::uri_from_path(ls_file.to_string()));
#endif
    REQUIRE(children.size() == 2);
    CHECK(URI(children[0].path().native()) == ls_file);
    CHECK(
        URI(children[1].path().native()) == ls_subdir.remove_trailing_slash());
    CHECK(children[0].file_size() == s.size());
    CHECK(children[1].file_size() == 0);  // Directories don't get a size

    // Move file
    auto file6 = URI(path.to_string() + "file6");
    REQUIRE_NOTHROW(vfs.move_file(file5, file6));
    CHECK(!vfs.is_file(file5));
    CHECK(vfs.is_file(file6));
    paths.clear();

    // Move directory
    auto dir2 = URI(path.to_string() + "dir2/");
    REQUIRE_NOTHROW(vfs.move_dir(dir1, URI(dir2)));
    CHECK(!vfs.is_dir(dir1));
    CHECK(vfs.is_dir(dir2));
    paths.clear();

    // Remove files
    REQUIRE_NOTHROW(vfs.remove_file(file4));
    CHECK(!vfs.is_file(file4));
    REQUIRE_NOTHROW(vfs.remove_file(file6));
    CHECK(!vfs.is_file(file6));

    // Remove directories
    REQUIRE_NOTHROW(vfs.remove_dir(dir2));
    CHECK(!vfs.is_file(file1));
    CHECK(!vfs.is_file(file2));
    CHECK(!vfs.is_file(file3));
    CHECK(!vfs.is_dir(dir2));
  }  // File Management

  // Clean up
  if (path.is_gcs() || path.is_s3() || path.is_azure()) {
    REQUIRE_NOTHROW(vfs.remove_bucket(path));
    REQUIRE(!vfs.is_bucket(path));
  } else {
    REQUIRE_NOTHROW(vfs.remove_dir(path));
    REQUIRE(!vfs.is_dir(path));
  }
}

TEST_CASE("VFS: Create directory", "[vfs][create-dir]") {
  LocalFsTest fs({0});
  if (!fs.is_supported()) {
    return;
  }

  URI path = fs.temp_dir_.add_trailing_slash();

  URI subdir = URI(path.to_string() + "subdir/");
  URI subdir2 = URI(path.to_string() + "subdir/nested/nested2/");

  REQUIRE_NOTHROW(fs.vfs_.create_dir(subdir));
  REQUIRE(fs.vfs_.is_dir(subdir));
  REQUIRE_NOTHROW(fs.vfs_.create_dir(subdir2));
  REQUIRE(fs.vfs_.is_dir(subdir2));

  // Try creating existing directory.
  REQUIRE_NOTHROW(fs.vfs_.create_dir(subdir));
}

TEMPLATE_LIST_TEST_CASE("VFS: Copy directory", "[vfs][copy-dir]", AllBackends) {
  TestType fs({0});
  if (!fs.is_supported()) {
    SKIP("unsupported backend");
  }

  URI path = fs.temp_dir_.add_trailing_slash();

  URI subdir1 = URI(path.to_string() + "subdir1/");
  URI file1 = URI(subdir1.to_string() + "file1");
  URI file2 = URI(subdir1.to_string() + "file2");
  URI subdir2 = URI(path.to_string() + "subdir2/");
  URI file2_2 = URI(subdir2.to_string() + "file2");
  URI file3 = URI(subdir2.to_string() + "file3");

  auto write_all_text = [&](const URI& uri, const std::string& text) {
    REQUIRE_NOTHROW(fs.vfs_.write(uri, text.data(), text.size()));
    require_tiledb_ok(fs.vfs_.close_file(uri));
  };

  auto read_all_text = [&](const URI& uri) {
    auto size = fs.vfs_.file_size(uri);
    std::string ret(size, '\0');
    REQUIRE_NOTHROW(fs.vfs_.read_exactly(uri, 0, ret.data(), size));
    return ret;
  };

  REQUIRE_NOTHROW(write_all_text(file1, "file1"));
  REQUIRE_NOTHROW(write_all_text(file2, "file2_1"));
  REQUIRE_NOTHROW(write_all_text(file2_2, "file2_2"));
  REQUIRE_NOTHROW(write_all_text(file3, "file3"));

  // Copy two directories with a common file.
  REQUIRE(fs.vfs_.ls_with_sizes(subdir2).size() == 2);
  REQUIRE_NOTHROW(fs.vfs_.copy_dir(subdir1, subdir2));
  REQUIRE(fs.vfs_.ls_with_sizes(subdir2).size() == 3);

  REQUIRE(read_all_text(URI(subdir2.to_string() + "file1")) == "file1");
  REQUIRE(read_all_text(file2_2) == "file2_1");
  REQUIRE(read_all_text(file3) == "file3");
}

TEMPLATE_LIST_TEST_CASE("VFS: File I/O", "[vfs][uri][file_io]", AllBackends) {
  TestType fs({0});
  if (!fs.is_supported()) {
    return;
  }

  bool disable_multipart = GENERATE(true, false);
  uint64_t max_parallel_ops = 1;
  uint64_t chunk_size = 1024 * 1024;
  int multiplier = 5;

  URI path = fs.temp_dir_.add_trailing_slash();

  if constexpr (
      std::is_same<TestType, GCSTest>::value ||
      std::is_same<TestType, GSTest>::value) {
    chunk_size = 4 * 1024 * 1024;
    multiplier = 1;

    if (!disable_multipart) {
      max_parallel_ops = GENERATE(1, 4);
    }
  }

  if constexpr (std::is_same<TestType, AzureTest>::value) {
    max_parallel_ops = 2;
    chunk_size = 4 * 1024 * 1024;
    if (disable_multipart) {
      multiplier = 1;
    }
  }

  ThreadPool compute_tp(4);
  ThreadPool io_tp(4);
  Config config = set_config_params(disable_multipart, max_parallel_ops);
  VFS vfs{
      &g_helper_stats, g_helper_logger().get(), &compute_tp, &io_tp, config};

  // Getting file_size on a nonexistent blob shouldn't crash on Azure
  URI non_existent = URI(path.to_string() + "non_existent");
  if (path.is_file()) {
    // #TODO Ensure this doesn't fail. This test case seems incorrect.
    CHECK_THROWS(vfs.file_size(non_existent));
  }

  // Set up
  if (path.is_gcs() || path.is_s3() || path.is_azure()) {
    if (vfs.is_bucket(path)) {
      REQUIRE_NOTHROW(vfs.remove_bucket(path));
    }
    REQUIRE_NOTHROW(vfs.create_bucket(path));
  } else {
    if (vfs.is_dir(path)) {
      REQUIRE_NOTHROW(vfs.remove_dir(path));
    }
    REQUIRE_NOTHROW(vfs.create_dir(path));
    // Bucket-specific operations are only valid for object store filesystems.
    CHECK_THROWS(vfs.create_bucket(path));
  }

  // Prepare buffers
  uint64_t buffer_size = multiplier * max_parallel_ops * chunk_size;
  auto write_buffer = new char[buffer_size];
  for (uint64_t i = 0; i < buffer_size; i++)
    write_buffer[i] = (char)('a' + (i % 26));
  uint64_t buffer_size_small = 1024 * 1024;
  auto write_buffer_small = new char[buffer_size_small];
  for (uint64_t i = 0; i < buffer_size_small; i++)
    write_buffer_small[i] = (char)('a' + (i % 26));

  // Write to two files
  URI largefile = URI(path.to_string() + "largefile");
  REQUIRE_NOTHROW(vfs.write(largefile, write_buffer, buffer_size));
  URI smallfile = URI(path.to_string() + "smallfile");
  REQUIRE_NOTHROW(vfs.write(smallfile, write_buffer_small, buffer_size_small));

  // On non-local systems, before flushing, the files do not exist
  if (!(path.is_file())) {
    CHECK(!vfs.is_file(largefile));
    CHECK(!vfs.is_file(smallfile));

    // Flush the files
    require_tiledb_ok(vfs.close_file(largefile));
    require_tiledb_ok(vfs.close_file(smallfile));
  }

  // After flushing, the files exist
  CHECK(vfs.is_file(largefile));
  CHECK(vfs.is_file(smallfile));

  // Get file sizes
  CHECK(vfs.file_size(largefile) == buffer_size);
  CHECK(vfs.file_size(smallfile) == buffer_size_small);

  // Read from the beginning
  auto read_buffer = new char[26];
  require_tiledb_ok(vfs.read_exactly(largefile, 0, read_buffer, 26));
  bool allok = true;
  for (int i = 0; i < 26; i++) {
    if (read_buffer[i] != static_cast<char>('a' + i)) {
      allok = false;
      break;
    }
  }
  CHECK(allok);

  // Read from a different offset
  require_tiledb_ok(vfs.read_exactly(largefile, 11, read_buffer, 26));
  allok = true;
  for (int i = 0; i < 26; i++) {
    if (read_buffer[i] != static_cast<char>('a' + (i + 11) % 26)) {
      allok = false;
      break;
    }
  }
  CHECK(allok);

  // Clean up
  if (path.is_gcs() || path.is_s3() || path.is_azure()) {
    REQUIRE_NOTHROW(vfs.remove_bucket(path));
    REQUIRE(!vfs.is_bucket(path));
  } else {
    REQUIRE_NOTHROW(vfs.remove_dir(path));
    REQUIRE(!vfs.is_dir(path));
  }
}

TEST_CASE("VFS: Test end-to-end", "[.vfs-e2e]") {
  auto test_file_ptr = getenv("TILEDB_VFS_E2E_TEST_FILE");
  if (test_file_ptr == nullptr) {
    FAIL("TILEDB_VFS_E2E_TEST_FILE variable is not specified");
  }
  URI test_file{test_file_ptr};

  ThreadPool compute_tp(1);
  ThreadPool io_tp(1);
  // Will be configured from environment variables.
  Config config;

  VFS vfs{
      &g_helper_stats, g_helper_logger().get(), &compute_tp, &io_tp, config};
  REQUIRE(vfs.supports_uri_scheme(test_file));
  CHECK(vfs.file_size(test_file) > 0);
}

TEST_CASE("VFS: test ls_with_sizes", "[vfs][ls-with-sizes]") {
  ThreadPool compute_tp(4);
  ThreadPool io_tp(4);
  VFS vfs_ls{
      &g_helper_stats, g_helper_logger().get(), &compute_tp, &io_tp, Config{}};

  std::string path = local_path();
  std::string dir = path + "ls_dir";
  std::string file = dir + "/file";
  std::string subdir = dir + "/subdir";
  std::string subdir_file = subdir + "/file";

  // Create directories and files
  REQUIRE_NOTHROW(vfs_ls.create_dir(URI(path)));
  REQUIRE_NOTHROW(vfs_ls.create_dir(URI(dir)));
  REQUIRE_NOTHROW(vfs_ls.create_dir(URI(subdir)));
  REQUIRE_NOTHROW(vfs_ls.touch(URI(file)));
  REQUIRE_NOTHROW(vfs_ls.touch(URI(subdir_file)));

  // Write to file
  std::string s1 = "abcdef";
  REQUIRE_NOTHROW(vfs_ls.write(URI(file), s1.data(), s1.size()));

  // Write to subdir file
  std::string s2 = "abcdef";
  REQUIRE_NOTHROW(vfs_ls.write(URI(subdir_file), s2.data(), s2.size()));

  // List
  auto children = vfs_ls.ls_with_sizes(URI(dir));

#ifdef _WIN32
  // Normalization only for Windows
  file = tiledb::sm::path_win::uri_from_path(file);
  subdir = tiledb::sm::path_win::uri_from_path(subdir);
#endif

  // Check results
  REQUIRE(children.size() == 2);
  REQUIRE(children[0].path().native() == URI(file).to_path());
  REQUIRE(children[1].path().native() == URI(subdir).to_path());
  REQUIRE(children[0].file_size() == 6);

  // Directories don't get a size
  REQUIRE(children[1].file_size() == 0);

  // Touch does not overwrite an existing file.
  REQUIRE_NOTHROW(vfs_ls.touch(URI(subdir_file)));
  REQUIRE(vfs_ls.file_size(URI(subdir_file)) == 6);

  // Clean up
  REQUIRE_NOTHROW(vfs_ls.remove_dir(URI(path)));
}

// Currently only local, S3, Azure and GCS are supported for VFS::ls_recursive.
using TestBackends = std::tuple<LocalFsTest, S3Test, AzureTest, GCSTest>;
TEMPLATE_LIST_TEST_CASE(
    "VFS: Test internal ls_filtered recursion argument",
    "[vfs][ls_filtered][ls_filtered_v2][recursion]",
    TestBackends) {
  TestType fs({10, 50});
  if (!fs.is_supported()) {
    return;
  }

  bool recursive = GENERATE(true, false);
  // If testing with recursion use the root directory, otherwise use a subdir.
  auto path = recursive ? fs.temp_dir_ : fs.temp_dir_.join_path("subdir_1");
  DYNAMIC_SECTION(
      fs.temp_dir_.backend_name()
      << " ls_filtered with recursion: " << (recursive ? "true" : "false")) {
    auto ls_objects =
        fs.vfs_.ls_filtered(path, tiledb::sm::LsScanner::accept_all, recursive);
    auto expected = fs.expected_results();
    if (!recursive) {
      // If non-recursive all objects in the first directory should be
      // returned, excluding the subdir_1/ prefix.
      std::erase_if(expected, [](const auto& p) {
        return p.first.find("subdir_1/test_file") == std::string::npos;
      });
    }

    CHECK(ls_objects.size() == expected.size());
    CHECK_THAT(ls_objects, Catch::Matchers::UnorderedEquals(expected));
  }
  DYNAMIC_SECTION(
      fs.temp_dir_.backend_name()
      << " ls_filtered_v2 with recursion: " << (recursive ? "true" : "false")) {
    auto ls_objects = fs.vfs_.ls_filtered_v2(
        path, tiledb::sm::LsScanner::accept_all_v2, recursive);
    auto expected = fs.expected_results_v2();
    if (!recursive) {
      // If non-recursive all objects in the first directory should be
      // returned, excluding the subdir_1/ prefix.
      std::erase_if(expected, [](const auto& p) {
        return p.first.find("subdir_1/test_file") == std::string::npos;
      });
    }

    CHECK(ls_objects.size() == expected.size());
    CHECK_THAT(ls_objects, Catch::Matchers::UnorderedEquals(expected));
  }
}

TEST_CASE(
    "VFS: ls_recursive throws for unsupported backends",
    "[vfs][ls_recursive][ls_recursive_v2]") {
  // LocalFs tests are in tiledb/sm/filesystem/test/unit_ls_filtered.cc
  // TODO: This test can be deleted when support for memfs is added.
  std::string prefix = GENERATE("s3://", "azure://", "gcs://");
  VFSTest vfs_test({1}, prefix);
  if (!vfs_test.is_supported()) {
    return;
  }
  std::string backend = vfs_test.temp_dir_.backend_name();

  DYNAMIC_SECTION(backend << " supported backend should not throw") {
    CHECK_NOTHROW(vfs_test.vfs_.ls_recursive(
        vfs_test.temp_dir_, tiledb::sm::LsScanner::accept_all));
    CHECK_NOTHROW(vfs_test.vfs_.ls_recursive_v2(
        vfs_test.temp_dir_, tiledb::sm::LsScanner::accept_all_v2));
  }
}

TEST_CASE("VFS: Throwing filters for ls_recursive", "[vfs][ls_recursive]") {
  std::string prefix = GENERATE("s3://", "azure://", "gcs://", "gs://");
  VFSTest vfs_test({0}, prefix);
  if (!vfs_test.is_supported()) {
    return;
  }

  auto filter = [](const std::string_view&, uint64_t) -> bool {
    throw std::logic_error("Throwing filter");
  };
  auto filter_v2 = [](const std::string_view&, uint64_t, bool) -> bool {
    throw std::logic_error("Throwing filter v2");
  };
  SECTION("Throwing filter with 0 objects should not throw") {
    CHECK_NOTHROW(vfs_test.vfs_.ls_recursive(vfs_test.temp_dir_, filter));
    CHECK_NOTHROW(vfs_test.vfs_.ls_recursive_v2(vfs_test.temp_dir_, filter_v2));
  }

  SECTION("Throwing filter with N objects should throw") {
    REQUIRE_NOTHROW(vfs_test.vfs_.touch(vfs_test.temp_dir_.join_path("file")));
    CHECK_THROWS_AS(
        vfs_test.vfs_.ls_recursive(vfs_test.temp_dir_, filter),
        std::logic_error);
    CHECK_THROWS_WITH(
        vfs_test.vfs_.ls_recursive(vfs_test.temp_dir_, filter),
        Catch::Matchers::ContainsSubstring("Throwing filter"));

    CHECK_THROWS_AS(
        vfs_test.vfs_.ls_recursive_v2(vfs_test.temp_dir_, filter_v2),
        std::logic_error);
    CHECK_THROWS_WITH(
        vfs_test.vfs_.ls_recursive_v2(vfs_test.temp_dir_, filter_v2),
        Catch::Matchers::ContainsSubstring("Throwing filter v2"));
  }
}

TEST_CASE("VFS: Test remove_dir_if_empty", "[vfs][remove-dir-if-empty]") {
  ThreadPool tp(1);
  VFS vfs{&g_helper_stats, g_helper_logger().get(), &tp, &tp, Config{}};

  std::string path = local_path();
  std::string dir = path + "remove_dir_if_empty/";
  std::string subdir = dir + "subdir/";
  std::string file1 = dir + "file1";

  // Create directories and files
  REQUIRE_NOTHROW(vfs.create_dir(URI(path)));
  REQUIRE_NOTHROW(vfs.create_dir(URI(dir)));
  REQUIRE_NOTHROW(vfs.create_dir(URI(subdir)));
  REQUIRE_NOTHROW(vfs.touch(URI(file1)));

  // Check that remove_dir_if_empty fails for non-empty directories
  vfs.remove_dir_if_empty(URI(dir));
  CHECK(vfs.is_dir(URI(dir)));

  // Check that it succeeds for empty directories
  vfs.remove_dir_if_empty(URI(subdir));
  CHECK_FALSE(vfs.is_dir(URI(subdir)));

  // Empty the directory and try again
  REQUIRE_NOTHROW(vfs.remove_file(URI(file1)));
  vfs.remove_dir_if_empty(URI(dir));
  CHECK_FALSE(vfs.is_dir(URI(dir)));

  // Clean up
  REQUIRE_NOTHROW(vfs.remove_dir(URI(path)));
}

#ifdef HAVE_AZURE
TEST_CASE("VFS: Construct Azure Blob Storage endpoint URIs", "[azure][uri]") {
  // Test the construction of Azure Blob Storage URIs from account name and SAS
  // token. We are not actually connecting to Azure Blob Storage in this test.
  std::string sas_token, custom_endpoint, expected_endpoint;
  SECTION("No SAS token") {
    sas_token = "";
    expected_endpoint = "https://exampleaccount.blob.core.windows.net";
  }
  SECTION("SAS token without leading question mark") {
    sas_token = "baz=qux&foo=bar";
    expected_endpoint =
        "https://exampleaccount.blob.core.windows.net?baz=qux&foo=bar";
  }
  SECTION("SAS token with leading question mark") {
    sas_token = "?baz=qux&foo=bar";
    expected_endpoint =
        "https://exampleaccount.blob.core.windows.net?baz=qux&foo=bar";
  }
  SECTION("SAS token in both endpoint and config option") {
    sas_token = "baz=qux&foo=bar";
    custom_endpoint =
        "https://exampleaccount.blob.core.windows.net?baz=qux&foo=bar";
    expected_endpoint =
        "https://exampleaccount.blob.core.windows.net?baz=qux&foo=bar";
  }
  Config config;
  require_tiledb_ok(
      config.set("vfs.azure.storage_account_name", "exampleaccount"));
  require_tiledb_ok(config.set("vfs.azure.blob_endpoint", custom_endpoint));
  require_tiledb_ok(config.set("vfs.azure.storage_sas_token", sas_token));
  if (sas_token.empty()) {
    // If the SAS token is empty, the VFS will try to connect to Microsoft Entra
    // ID to obtain credentials, which can take a long time because of retries.
    // Set a dummy access key (which won't be used because we are not going to
    // perform any requests) to prevent Entra ID from being chosen.
    require_tiledb_ok(config.set("vfs.azure.storage_account_key", "foobar"));
  }
  ThreadPool thread_pool(1);
  tiledb::sm::Azure azure(&thread_pool, config);
  REQUIRE(azure.client().GetUrl() == expected_endpoint);
}
#endif

#ifdef HAVE_S3
TEST_CASE(
    "Validate vfs.s3.custom_headers.*", "[s3][custom-headers][!mayfail]") {
  // In newer versions of the AWS SDK, setting Content-MD5 outside of the
  // official method on the request object is not supported.
  Config cfg = set_config_params(true);

  // Check the edge case of a key matching the ConfigIter prefix.
  REQUIRE(cfg.set("vfs.s3.custom_headers.", "").ok());

  // Set an unexpected value for Content-MD5, which minio should reject
  REQUIRE(cfg.set("vfs.s3.custom_headers.Content-MD5", "unexpected").ok());

  // Recreate a new S3 client because config is not dynamic
  ThreadPool thread_pool(2);
  S3 s3{&g_helper_stats, &thread_pool, cfg};
  auto uri = URI("s3://tiledb-" + random_label() + "/writefailure");

  // This is a buffered write, which is why it should not throw.
  CHECK_NOTHROW(s3.write(uri, "Validate s3 custom headers", 26));

  auto matcher = Catch::Matchers::ContainsSubstring(
      "The Content-Md5 you specified is not valid.");
  REQUIRE_THROWS_WITH(s3.flush(uri), matcher);
}
#endif

#ifdef HAVE_GCS
TEST_CASE(
    "Validate GCS service account impersonation",
    "[gcs][credentials][impersonation]") {
  ThreadPool thread_pool(2);
  Config cfg = set_config_params(true);
  std::string impersonate_service_account, target_service_account;
  std::vector<std::string> delegates;

  SECTION("Simple") {
    impersonate_service_account = "account1";
    target_service_account = "account1";
    delegates = {};
  }

  SECTION("Delegated") {
    impersonate_service_account = "account1,account2,account3";
    target_service_account = "account3";
    delegates = {"account1", "account2"};
  }

  // Test parsing an edge case.
  SECTION("Invalid") {
    impersonate_service_account = ",";
    target_service_account = "";
    delegates = {""};
  }

  require_tiledb_ok(cfg.set(
      "vfs.gcs.impersonate_service_account", impersonate_service_account));
  GCS gcs(&thread_pool, cfg);
  auto credentials = gcs.make_credentials({});

  // We are using an internal class only for inspection purposes.
  auto impersonate_credentials =
      dynamic_cast<google::cloud::internal::ImpersonateServiceAccountConfig*>(
          credentials.get());

  REQUIRE(impersonate_credentials != nullptr);
  REQUIRE(
      impersonate_credentials->target_service_account() ==
      target_service_account);
  REQUIRE(impersonate_credentials->delegates() == delegates);
}

TEST_CASE(
    "Validate GCS service account credentials",
    "[gcs][credentials][service-account]") {
  ThreadPool thread_pool(2);
  Config cfg = set_config_params(true);
  // The content of the credentials does not matter; it does not get parsed
  // until it is used in an API request, which we are not doing.
  std::string service_account_key = "{\"foo\": \"bar\"}";

  require_tiledb_ok(
      cfg.set("vfs.gcs.service_account_key", service_account_key));
  GCS gcs(&thread_pool, cfg);
  auto credentials = gcs.make_credentials({});

  // We are using an internal class only for inspection purposes.
  auto service_account =
      dynamic_cast<google::cloud::internal::ServiceAccountConfig*>(
          credentials.get());

  REQUIRE(service_account != nullptr);
  REQUIRE(service_account->json_object() == service_account_key);
}

TEST_CASE(
    "Validate GCS service account credentials with impersonation",
    "[gcs][credentials][service-account-and-impersonation]") {
  ThreadPool thread_pool(2);
  Config cfg = set_config_params(true);
  // The content of the credentials does not matter; it does not get parsed
  // until it is used in an API request, which we are not doing.
  std::string service_account_key = "{\"foo\": \"bar\"}";
  std::string impersonate_service_account = "account1,account2,account3";

  require_tiledb_ok(
      cfg.set("vfs.gcs.service_account_key", service_account_key));
  require_tiledb_ok(cfg.set(
      "vfs.gcs.impersonate_service_account", impersonate_service_account));
  GCS gcs(&thread_pool, cfg);
  auto credentials = gcs.make_credentials({});

  // We are using an internal class only for inspection purposes.
  auto impersonate_credentials =
      dynamic_cast<google::cloud::internal::ImpersonateServiceAccountConfig*>(
          credentials.get());
  REQUIRE(impersonate_credentials != nullptr);
  REQUIRE(impersonate_credentials->target_service_account() == "account3");
  REQUIRE(
      impersonate_credentials->delegates() ==
      std::vector<std::string>{"account1", "account2"});

  auto inner_service_account =
      dynamic_cast<google::cloud::internal::ServiceAccountConfig*>(
          impersonate_credentials->base_credentials().get());

  REQUIRE(inner_service_account != nullptr);
  REQUIRE(inner_service_account->json_object() == service_account_key);
}

TEST_CASE(
    "Validate GCS external account credentials",
    "[gcs][credentials][external-account]") {
  ThreadPool thread_pool(2);
  Config cfg = set_config_params(true);
  // The content of the credentials does not matter; it does not get parsed
  // until it is used in an API request, which we are not doing.
  std::string workload_identity_configuration = "{\"foo\": \"bar\"}";
  require_tiledb_ok(cfg.set(
      "vfs.gcs.workload_identity_configuration",
      workload_identity_configuration));
  GCS gcs(&thread_pool, cfg);
  auto credentials = gcs.make_credentials({});

  // We are using an internal class only for inspection purposes.
  auto external_account =
      dynamic_cast<google::cloud::internal::ExternalAccountConfig*>(
          credentials.get());

  REQUIRE(external_account != nullptr);
  REQUIRE(external_account->json_object() == workload_identity_configuration);
}
#endif
