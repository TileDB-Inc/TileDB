/**
 * @file   unit-vfs.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2024 TileDB, Inc.
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
#ifdef HAVE_AZURE
#include <azure/storage/blobs.hpp>
#include "tiledb/sm/filesystem/azure.h"
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

TEST_CASE("VFS: Test long local paths", "[vfs]") {
  ThreadPool compute_tp(4);
  ThreadPool io_tp(4);
  VFS vfs{&g_helper_stats, &compute_tp, &io_tp, Config{}};

  SECTION("- Deep hierarchy") {
    // Create a nested path with a long total length
    std::string tmpdir = local_path();
    bool success = true;
    while (tmpdir.size() < 512) {
      tmpdir += "subdir/";
      success &= vfs.create_dir(URI(tmpdir)).ok();
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
    std::string tmpdir = local_path();
    URI testfile(tmpdir + name);

    // Creating the URI and checking its existence is fine on posix
    if constexpr (tiledb::sm::filesystem::posix_enabled) {
      REQUIRE(!testfile.is_invalid());
      bool exists = false;
      require_tiledb_ok(vfs.is_file(testfile, &exists));

      // Creating the file is not
      REQUIRE_THROWS(vfs.touch(testfile));
    }

    // Creating the URI is invalid on Win32 (failure to canonicalize path)
    if constexpr (tiledb::sm::filesystem::windows_enabled) {
      REQUIRE(testfile.is_invalid());
    }
  }
}

using AllBackends =
    std::tuple<LocalFsTest, GCSTest, GSTest, S3Test, AzureTest, HDFSTest>;
TEMPLATE_LIST_TEST_CASE(
    "VFS: URI semantics and file management", "[vfs][uri]", AllBackends) {
  TestType fs({0});
  if (!fs.is_supported()) {
    return;
  }

  ThreadPool compute_tp(4);
  ThreadPool io_tp(4);
  Config config = set_config_params();
  VFS vfs{&g_helper_stats, &compute_tp, &io_tp, config};

  URI path = fs.temp_dir_.add_trailing_slash();

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
    URI ls_dir = dir1;
    URI ls_subdir = subdir;
    URI ls_file = file3;
    if (path.is_hdfs()) {
      // HDFS requires localhost-resolved paths for ls_with_sizes
      auto localdir1 =
          URI("hdfs://localhost:9000/vfs-" + random_label() + "/dir1/");
      require_tiledb_ok(vfs.create_dir(localdir1));
      auto localsubdir = URI(localdir1.to_string() + "subdir/");
      require_tiledb_ok(vfs.create_dir(localsubdir));
      auto localfile3 = URI(localdir1.to_string() + "file3");
      require_tiledb_ok(vfs.touch(localfile3));
      ls_dir = localdir1;
      ls_subdir = localsubdir;
      ls_file = localfile3;
    }
    std::string s = "abcdef";
    require_tiledb_ok(vfs.write(ls_file, s.data(), s.size()));
    require_tiledb_ok(vfs.close_file(ls_file));
    auto&& [status, opt_children] = vfs.ls_with_sizes(ls_dir);
    require_tiledb_ok(status);
    auto children = opt_children.value();
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

    if (path.is_hdfs()) {
      // Clean up
      require_tiledb_ok(vfs.remove_dir(ls_dir));
      require_tiledb_ok(vfs.is_dir(ls_dir, &exists));
      CHECK(!exists);
    }

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
  VFS vfs{&g_helper_stats, &compute_tp, &io_tp, config};

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
  require_tiledb_ok(vfs.write(largefile, write_buffer, buffer_size));
  URI smallfile = URI(path.to_string() + "smallfile");
  require_tiledb_ok(
      vfs.write(smallfile, write_buffer_small, buffer_size_small));

  // On non-local and hdfs systems, before flushing, the files do not exist
  if (!(path.is_file() || path.is_hdfs())) {
    require_tiledb_ok(vfs.is_file(largefile, &exists));
    CHECK(!exists);
    require_tiledb_ok(vfs.is_file(smallfile, &exists));
    CHECK(!exists);

    // Flush the files
    require_tiledb_ok(vfs.close_file(largefile));
    require_tiledb_ok(vfs.close_file(smallfile));
  }

  // After flushing, the files exist
  require_tiledb_ok(vfs.is_file(largefile, &exists));
  CHECK(exists);
  require_tiledb_ok(vfs.is_file(smallfile, &exists));
  CHECK(exists);

  // Get file sizes
  uint64_t nbytes = 0;
  require_tiledb_ok(vfs.file_size(largefile, &nbytes));
  CHECK(nbytes == (buffer_size));
  require_tiledb_ok(vfs.file_size(smallfile, &nbytes));
  CHECK(nbytes == buffer_size_small);

  // Read from the beginning
  auto read_buffer = new char[26];
  require_tiledb_ok(vfs.read(largefile, 0, read_buffer, 26));
  bool allok = true;
  for (int i = 0; i < 26; i++) {
    if (read_buffer[i] != static_cast<char>('a' + i)) {
      allok = false;
      break;
    }
  }
  CHECK(allok);

  // Read from a different offset
  require_tiledb_ok(vfs.read(largefile, 11, read_buffer, 26));
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
    require_tiledb_ok(vfs.remove_bucket(path));
    require_tiledb_ok(vfs.is_bucket(path, &exists));
    REQUIRE(!exists);
  } else {
    require_tiledb_ok(vfs.remove_dir(path));
    require_tiledb_ok(vfs.is_dir(path, &exists));
    REQUIRE(!exists);
  }
}

TEST_CASE("VFS: test ls_with_sizes", "[vfs][ls-with-sizes]") {
  ThreadPool compute_tp(4);
  ThreadPool io_tp(4);
  VFS vfs_ls{&g_helper_stats, &compute_tp, &io_tp, Config{}};

  std::string path = local_path();
  std::string dir = path + "ls_dir";
  std::string file = dir + "/file";
  std::string subdir = dir + "/subdir";
  std::string subdir_file = subdir + "/file";

  // Create directories and files
  require_tiledb_ok(vfs_ls.create_dir(URI(path)));
  require_tiledb_ok(vfs_ls.create_dir(URI(dir)));
  require_tiledb_ok(vfs_ls.create_dir(URI(subdir)));
  require_tiledb_ok(vfs_ls.touch(URI(file)));
  require_tiledb_ok(vfs_ls.touch(URI(subdir_file)));

  // Write to file
  std::string s1 = "abcdef";
  require_tiledb_ok(vfs_ls.write(URI(file), s1.data(), s1.size()));

  // Write to subdir file
  std::string s2 = "abcdef";
  require_tiledb_ok(vfs_ls.write(URI(subdir_file), s2.data(), s2.size()));

  // List
  auto&& [status, rv] = vfs_ls.ls_with_sizes(URI(dir));
  auto children = *rv;
  require_tiledb_ok(status);

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

  // Clean up
  require_tiledb_ok(vfs_ls.remove_dir(URI(path)));
}

// Currently only S3 is supported for VFS::ls_recursive.
using TestBackends = std::tuple<S3Test>;
TEMPLATE_LIST_TEST_CASE(
    "VFS: Test internal ls_filtered recursion argument",
    "[vfs][ls_filtered][recursion]",
    TestBackends) {
  TestType fs({10, 50});
  if (!fs.is_supported()) {
    return;
  }

  bool recursive = GENERATE(true, false);
  DYNAMIC_SECTION(
      fs.temp_dir_.backend_name()
      << " ls_filtered with recursion: " << (recursive ? "true" : "false")) {
#ifdef HAVE_S3
    // If testing with recursion use the root directory, otherwise use a subdir.
    auto path = recursive ? fs.temp_dir_ : fs.temp_dir_.join_path("subdir_1");
    auto ls_objects = fs.get_s3().ls_filtered(
        path, VFSTestBase::accept_all_files, accept_all_dirs, recursive);

    auto expected = fs.expected_results();
    if (!recursive) {
      // If non-recursive, all objects in the first directory should be
      // returned.
      std::erase_if(expected, [](const auto& p) {
        return p.first.find("subdir_1") == std::string::npos;
      });
    }

    CHECK(ls_objects.size() == expected.size());
    CHECK(ls_objects == expected);
#endif
  }
}

TEST_CASE(
    "VFS: ls_recursive throws for unsupported backends",
    "[vfs][ls_recursive]") {
  // Local and mem fs tests are in tiledb/sm/filesystem/test/unit_ls_filtered.cc
  std::string prefix = GENERATE("s3://", "hdfs://", "azure://", "gcs://");
  VFSTest vfs_test({1}, prefix);
  if (!vfs_test.is_supported()) {
    return;
  }
  std::string backend = vfs_test.temp_dir_.backend_name();

  if (vfs_test.temp_dir_.is_s3()) {
    DYNAMIC_SECTION(backend << " supported backend should not throw") {
      CHECK_NOTHROW(vfs_test.vfs_.ls_recursive(
          vfs_test.temp_dir_, VFSTestBase::accept_all_files));
    }
  } else {
    DYNAMIC_SECTION(backend << " unsupported backend should throw") {
      CHECK_THROWS_WITH(
          vfs_test.vfs_.ls_recursive(
              vfs_test.temp_dir_, VFSTestBase::accept_all_files),
          Catch::Matchers::ContainsSubstring(
              "storage backend is not supported"));
    }
  }
}

TEST_CASE(
    "VFS: Throwing FileFilter for ls_recursive",
    "[vfs][ls_recursive][file-filter]") {
  std::string prefix = "s3://";
  VFSTest vfs_test({0}, prefix);
  if (!vfs_test.is_supported()) {
    return;
  }

  auto file_filter = [](const std::string_view&, uint64_t) -> bool {
    throw std::logic_error("Throwing FileFilter");
  };
  SECTION("Throwing FileFilter with 0 objects should not throw") {
    CHECK_NOTHROW(vfs_test.vfs_.ls_recursive(
        vfs_test.temp_dir_, file_filter, tiledb::sm::accept_all_dirs));
  }
  SECTION("Throwing FileFilter with N objects should throw") {
    vfs_test.vfs_.touch(vfs_test.temp_dir_.join_path("file")).ok();
    CHECK_THROWS_AS(
        vfs_test.vfs_.ls_recursive(vfs_test.temp_dir_, file_filter),
        std::logic_error);
    CHECK_THROWS_WITH(
        vfs_test.vfs_.ls_recursive(vfs_test.temp_dir_, file_filter),
        Catch::Matchers::ContainsSubstring("Throwing FileFilter"));
  }
}

#ifdef HAVE_AZURE
TEST_CASE("VFS: Construct Azure Blob Storage endpoint URIs", "[azure][uri]") {
  std::string sas_token, custom_endpoint, expected_endpoint;
  SECTION("No SAS token") {
    sas_token = "";
    expected_endpoint = "https://devstoreaccount1.blob.core.windows.net";
  }
  SECTION("SAS token without leading question mark") {
    sas_token = "baz=qux&foo=bar";
    expected_endpoint =
        "https://devstoreaccount1.blob.core.windows.net?baz=qux&foo=bar";
  }
  SECTION("SAS token with leading question mark") {
    sas_token = "?baz=qux&foo=bar";
    expected_endpoint =
        "https://devstoreaccount1.blob.core.windows.net?baz=qux&foo=bar";
  }
  SECTION("SAS token in both endpoint and config option") {
    sas_token = "baz=qux&foo=bar";
    custom_endpoint =
        "https://devstoreaccount1.blob.core.windows.net?baz=qux&foo=bar";
    expected_endpoint =
        "https://devstoreaccount1.blob.core.windows.net?baz=qux&foo=bar";
  }
  SECTION("No SAS token") {
    sas_token = "";
    expected_endpoint = "https://devstoreaccount1.blob.core.windows.net";
  }
  Config config;
  require_tiledb_ok(
      config.set("vfs.azure.storage_account_name", "devstoreaccount1"));
  require_tiledb_ok(config.set("vfs.azure.blob_endpoint", custom_endpoint));
  require_tiledb_ok(config.set("vfs.azure.storage_sas_token", sas_token));
  tiledb::sm::Azure azure;
  ThreadPool thread_pool(1);
  require_tiledb_ok(azure.init(config, &thread_pool));
  REQUIRE(azure.client().GetUrl() == expected_endpoint);
}
#endif

#ifdef HAVE_S3
TEST_CASE("Validate vfs.s3.custom_headers.*", "[s3][custom-headers]") {
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
  REQUIRE_THROWS_WITH(s3.flush_object(uri), matcher);
}
#endif
