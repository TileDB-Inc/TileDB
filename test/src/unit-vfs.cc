/**
 * @file   unit-vfs.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2022 TileDB, Inc.
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
#include <atomic>
#include "test/src/helpers.h"
#include "tiledb/sm/filesystem/vfs.h"
#ifdef _WIN32
#include "tiledb/sm/filesystem/path_win.h"
#endif
#include "tiledb/sm/tile/tile.h"

using namespace tiledb::common;
using namespace tiledb::sm;
using namespace tiledb::test;

TEST_CASE("VFS: Test read batching", "[vfs]") {
  ThreadPool compute_tp(4);
  ThreadPool io_tp(4);
  Config config;

  URI testfile("vfs_unit_test_data");
  std::unique_ptr<VFS> vfs(new VFS);
  REQUIRE(vfs->init(&g_helper_stats, &compute_tp, &io_tp, config).ok());

  bool exists = false;
  REQUIRE(vfs->is_file(testfile, &exists).ok());
  if (exists) {
    vfs->remove_file(testfile);
  }

  // Write some data.
  const unsigned nelts = 100;
  uint32_t data_write[nelts];
  for (unsigned i = 0; i < nelts; i++) {
    data_write[i] = i;
  }
  REQUIRE(vfs->write(testfile, data_write, nelts * sizeof(uint32_t)).ok());
  REQUIRE(vfs->terminate().ok());

  Tile tile[nelts];
  for (uint64_t i = 0; i < nelts; i++) {
    tile[i].filtered_buffer().expand(nelts * sizeof(uint32_t));
  }

  std::vector<tuple<uint64_t, Tile*, uint64_t>> batches;
  std::vector<ThreadPool::Task> tasks;

  SECTION("- Default config") {
    // Check reading in one batch: single read operation.
    std::memset(tile[0].filtered_buffer().data(), 0, nelts * sizeof(uint32_t));
    batches.emplace_back(0, &tile[0], nelts * sizeof(uint32_t));
    REQUIRE(vfs->init(&g_helper_stats, &compute_tp, &io_tp, config).ok());
    REQUIRE(vfs->read_all(testfile, batches, &io_tp, &tasks).ok());
    REQUIRE(io_tp.wait_all(tasks).ok());
    tasks.clear();
    for (unsigned i = 0; i < nelts; i++)
      REQUIRE(tile[0].filtered_buffer().data_as<uint32_t>()[i] == i);

    // Check reading first and last element: 1 reads due to the default
    // batch size.
    std::memset(tile[0].filtered_buffer().data(), 0, nelts * sizeof(uint32_t));
    std::memset(tile[1].filtered_buffer().data(), 0, nelts * sizeof(uint32_t));
    batches.clear();
    batches.emplace_back(0, &tile[0], sizeof(uint32_t));
    batches.emplace_back(
        (nelts - 1) * sizeof(uint32_t), &tile[1], sizeof(uint32_t));
    REQUIRE(vfs->read_all(testfile, batches, &io_tp, &tasks).ok());
    REQUIRE(io_tp.wait_all(tasks).ok());
    tasks.clear();
    REQUIRE(tile[0].filtered_buffer().data_as<uint32_t>()[0] == 0);
    REQUIRE(tile[1].filtered_buffer().data_as<uint32_t>()[0] == nelts - 1);

    // Check each element as a different region: single read because there is no
    // amplification required (all work is useful).
    batches.clear();
    for (unsigned i = 0; i < nelts; i++) {
      std::memset(
          tile[i].filtered_buffer().data(), 0, nelts * sizeof(uint32_t));
      batches.emplace_back(i * sizeof(uint32_t), &tile[i], sizeof(uint32_t));
    }
    REQUIRE(vfs->read_all(testfile, batches, &io_tp, &tasks).ok());
    REQUIRE(io_tp.wait_all(tasks).ok());
    tasks.clear();
    for (unsigned i = 0; i < nelts; i++) {
      REQUIRE(tile[i].filtered_buffer().data_as<uint32_t>()[0] == i);
    }
    REQUIRE(vfs->terminate().ok());
  }

  SECTION("- Reduce min batch size and min batch gap") {
    // Set a smaller min batch size and min batch gap
    config.set("vfs.min_batch_size", "0");
    config.set("vfs.min_batch_gap", "0");
    REQUIRE(vfs->init(&g_helper_stats, &compute_tp, &io_tp, config).ok());

    // Check large batches are not split up.
    std::memset(tile[0].filtered_buffer().data(), 0, nelts * sizeof(uint32_t));
    batches.emplace_back(0, &tile[0], nelts * sizeof(uint32_t));
    REQUIRE(vfs->read_all(testfile, batches, &io_tp, &tasks).ok());
    REQUIRE(io_tp.wait_all(tasks).ok());
    tasks.clear();
    for (unsigned i = 0; i < nelts; i++) {
      REQUIRE(tile[0].filtered_buffer().data_as<uint32_t>()[i] == i);
    }

    // Check each element as a different region (results in several read
    // operations).
    batches.clear();
    for (unsigned i = 0; i < nelts / 2; i++) {
      std::memset(
          tile[i].filtered_buffer().data(), 0, nelts * sizeof(uint32_t));
      batches.emplace_back(
          2 * i * sizeof(uint32_t), &tile[i], sizeof(uint32_t));
    }
    REQUIRE(vfs->read_all(testfile, batches, &io_tp, &tasks).ok());
    REQUIRE(io_tp.wait_all(tasks).ok());
    tasks.clear();
    for (unsigned i = 0; i < nelts / 2; i++) {
      REQUIRE(tile[i].filtered_buffer().data_as<uint32_t>()[0] == 2 * i);
    }

    // Check reading first and last element (results in 2 reads because the
    // whole region is too big).
    std::memset(tile[0].filtered_buffer().data(), 0, nelts * sizeof(uint32_t));
    std::memset(tile[1].filtered_buffer().data(), 0, nelts * sizeof(uint32_t));
    batches.clear();
    batches.emplace_back(0, &tile[0], sizeof(uint32_t));
    batches.emplace_back(
        (nelts - 1) * sizeof(uint32_t), &tile[1], sizeof(uint32_t));
    REQUIRE(vfs->read_all(testfile, batches, &io_tp, &tasks).ok());
    REQUIRE(io_tp.wait_all(tasks).ok());
    tasks.clear();
    REQUIRE(tile[0].filtered_buffer().data_as<uint32_t>()[0] == 0);
    REQUIRE(tile[1].filtered_buffer().data_as<uint32_t>()[0] == nelts - 1);
    REQUIRE(vfs->terminate().ok());
  }

  SECTION("- Reduce min batch size but not min batch gap") {
    // Set a smaller min batch size
    Config config;
    config.set("vfs.min_batch_size", "0");
    REQUIRE(vfs->init(&g_helper_stats, &compute_tp, &io_tp, config).ok());

    // There should be a single read due to the gap
    batches.clear();
    for (unsigned i = 0; i < nelts; i++) {
      std::memset(
          tile[i].filtered_buffer().data(), 0, nelts * sizeof(uint32_t));
      batches.emplace_back(i * sizeof(uint32_t), &tile[i], sizeof(uint32_t));
    }
    REQUIRE(vfs->read_all(testfile, batches, &io_tp, &tasks).ok());
    REQUIRE(io_tp.wait_all(tasks).ok());
    tasks.clear();
    for (unsigned i = 0; i < nelts; i++) {
      REQUIRE(tile[i].filtered_buffer().data_as<uint32_t>()[0] == i);
    }
    REQUIRE(vfs->terminate().ok());
  }

  SECTION("- Reduce min batch gap but not min batch size") {
    // Set a smaller min batch size
    Config config;
    config.set("vfs.min_batch_gap", "0");
    REQUIRE(vfs->init(&g_helper_stats, &compute_tp, &io_tp, config).ok());

    // There should be a single read due to the batch size
    batches.clear();
    for (unsigned i = 0; i < nelts; i++) {
      std::memset(
          tile[i].filtered_buffer().data(), 0, nelts * sizeof(uint32_t));
      batches.emplace_back(i * sizeof(uint32_t), &tile[i], sizeof(uint32_t));
    }
    REQUIRE(vfs->read_all(testfile, batches, &io_tp, &tasks).ok());
    REQUIRE(io_tp.wait_all(tasks).ok());
    tasks.clear();
    for (unsigned i = 0; i < nelts; i++) {
      REQUIRE(tile[i].filtered_buffer().data_as<uint32_t>()[0] == i);
    }
    REQUIRE(vfs->terminate().ok());
  }

  REQUIRE(vfs->init(&g_helper_stats, &compute_tp, &io_tp, config).ok());
  REQUIRE(vfs->is_file(testfile, &exists).ok());
  if (exists)
    REQUIRE(vfs->remove_file(testfile).ok());

  REQUIRE(vfs->terminate().ok());
}

#ifdef _WIN32

TEST_CASE("VFS: Test long paths (Win32)", "[vfs][windows]") {
  ThreadPool compute_tp(4);
  ThreadPool io_tp(4);
  Config config;

  std::unique_ptr<VFS> vfs(new VFS);
  std::string tmpdir_base = tiledb::sm::Win::current_dir() + "\\tiledb_test\\";
  REQUIRE(vfs->init(&g_helper_stats, &compute_tp, &io_tp, config).ok());
  REQUIRE(vfs->create_dir(URI(tmpdir_base)).ok());

  SECTION("- Deep hierarchy") {
    // On some Windows platforms, the path length of a directory must be <= 248
    // chars. On others (that have opted in to a configuration that allows
    // long paths) the limit is ~32,767. Here we check for either case.
    std::string tmpdir = tmpdir_base;
    bool success = true;
    while (tmpdir.size() < 512) {
      tmpdir += "subdir\\";
      success &= vfs->create_dir(URI(tmpdir)).ok();
    }

    if (success) {
      // Check we can create files within the deep hierarchy
      URI testfile(tmpdir + "file.txt");
      REQUIRE(!testfile.is_invalid());
      bool exists = false;
      REQUIRE(vfs->is_file(testfile, &exists).ok());
      if (exists)
        REQUIRE(vfs->remove_file(testfile).ok());
      REQUIRE(vfs->touch(testfile).ok());
      REQUIRE(vfs->remove_file(testfile).ok());
    } else {
      // Don't check anything; directory creation failed.
    }
  }

  SECTION("- Too long name") {
    std::string name;
    for (unsigned i = 0; i < 256; i++)
      name += "x";

    // Creating the URI is invalid on Win32 (failure to canonicalize path)
    URI testfile(tmpdir_base + name);
    REQUIRE(testfile.is_invalid());
  }

  REQUIRE(vfs->remove_dir(URI(tmpdir_base)).ok());
}

#else

TEST_CASE("VFS: Test long posix paths", "[vfs]") {
  ThreadPool compute_tp(4);
  ThreadPool io_tp(4);
  Config config;

  std::unique_ptr<VFS> vfs(new VFS);
  REQUIRE(vfs->init(&g_helper_stats, &compute_tp, &io_tp, config).ok());

  std::string tmpdir_base = Posix::current_dir() + "/tiledb_test/";
  REQUIRE(vfs->create_dir(URI(tmpdir_base)).ok());

  SECTION("- Deep hierarchy") {
    // Create a nested path with a long total length
    std::string tmpdir = tmpdir_base;
    while (tmpdir.size() < 512) {
      tmpdir += "subdir/";
      REQUIRE(vfs->create_dir(URI(tmpdir)).ok());
    }

    // Check we can create files within the deep hierarchy
    URI testfile("file://" + tmpdir + "file.txt");
    REQUIRE(!testfile.is_invalid());
    bool exists = false;
    REQUIRE(vfs->is_file(testfile, &exists).ok());
    if (exists)
      REQUIRE(vfs->remove_file(testfile).ok());
    REQUIRE(vfs->touch(testfile).ok());
    REQUIRE(vfs->remove_file(testfile).ok());
  }

  SECTION("- Too long name") {
    // This may not be long enough on some filesystems to pass the fail check.
    std::string name;
    for (unsigned i = 0; i < 256; i++)
      name += "x";

    // Creating the URI and checking its existence is fine
    URI testfile("file://" + tmpdir_base + name);
    REQUIRE(!testfile.is_invalid());
    bool exists = false;
    REQUIRE(vfs->is_file(testfile, &exists).ok());

    // Creating the file is not
    REQUIRE(!vfs->touch(testfile).ok());
  }

  REQUIRE(vfs->remove_dir(URI(tmpdir_base)).ok());
  REQUIRE(vfs->terminate().ok());
}

#endif

TEST_CASE("VFS: URI semantics", "[vfs][uri]") {
  ThreadPool compute_tp(4);
  ThreadPool io_tp(4);

  bool s3_supported = false;
  bool hdfs_supported = false;
  bool azure_supported = false;
  bool gcs_supported = false;
  tiledb::test::get_supported_fs(
      &s3_supported, &hdfs_supported, &azure_supported, &gcs_supported);

  std::vector<std::pair<URI, Config>> root_pairs;
  if (s3_supported) {
    Config config;
    REQUIRE(config.set("vfs.s3.endpoint_override", "localhost:9999").ok());
    REQUIRE(config.set("vfs.s3.scheme", "https").ok());
    REQUIRE(config.set("vfs.s3.use_virtual_addressing", "false").ok());
    REQUIRE(config.set("vfs.s3.verify_ssl", "false").ok());

    root_pairs.emplace_back(
        URI("s3://" + tiledb::test::random_name("vfs") + "/"),
        std::move(config));
  }
  if (hdfs_supported) {
    Config config;
    root_pairs.emplace_back(
        URI("hdfs:///" + tiledb::test::random_name("vfs") + "/"),
        std::move(config));
  }
  if (azure_supported) {
    Config config;
    REQUIRE(
        config.set("vfs.azure.storage_account_name", "devstoreaccount1").ok());
    REQUIRE(config
                .set(
                    "vfs.azure.storage_account_key",
                    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4"
                    "I6tq/"
                    "K1SZFPTOtr/KBHBeksoGMGw==")
                .ok());
    REQUIRE(
        config
            .set("vfs.azure.blob_endpoint", "127.0.0.1:10000/devstoreaccount1")
            .ok());
    REQUIRE(config.set("vfs.azure.use_https", "false").ok());

    root_pairs.emplace_back(
        URI("azure://" + tiledb::test::random_name("vfs") + "/"),
        std::move(config));
  }

  Config config;
#ifdef _WIN32
  root_pairs.emplace_back(
      URI(tiledb::sm::Win::current_dir() + "\\" +
          tiledb::test::random_name("vfs") + "\\"),
      std::move(config));
#else
  root_pairs.emplace_back(
      URI(Posix::current_dir() + "/" + tiledb::test::random_name("vfs") + "/"),
      std::move(config));
#endif

  for (const auto& root_pair : root_pairs) {
    const URI& root = root_pair.first;
    const Config& config = root_pair.second;

    VFS vfs;
    REQUIRE(vfs.init(&g_helper_stats, &compute_tp, &io_tp, config).ok());

    bool exists = false;
    if (root.is_s3() || root.is_azure()) {
      REQUIRE(vfs.is_bucket(root, &exists).ok());
      if (exists) {
        REQUIRE(vfs.remove_bucket(root).ok());
      }
      REQUIRE(vfs.create_bucket(root).ok());
    } else {
      REQUIRE(vfs.is_dir(root, &exists).ok());
      if (exists) {
        REQUIRE(vfs.remove_dir(root).ok());
      }
      REQUIRE(vfs.create_dir(root).ok());
    }

    std::string dir1 = root.to_string() + "dir1";
    REQUIRE(vfs.create_dir(URI(dir1)).ok());

    std::string dir2 = root.to_string() + "dir1/dir2/";
    REQUIRE(vfs.create_dir(URI(dir2)).ok());

    URI file1(root.to_string() + "file1");
    REQUIRE(vfs.touch(file1).ok());

    URI file2(root.to_string() + "file2");
    REQUIRE(vfs.touch(file2).ok());

    URI file3(root.to_string() + "dir1/file3");
    REQUIRE(vfs.touch(file3).ok());

    URI file4(root.to_string() + "dir1/dir2/file4");
    REQUIRE(vfs.touch(file4).ok());

    URI file5(root.to_string() + "file5/");
    REQUIRE(!vfs.touch(file5).ok());

    std::vector<URI> uris;
    REQUIRE(vfs.ls(root, &uris).ok());

    std::vector<std::string> expected_uri_names = {"file1", "file2", "dir1"};

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

    if (root.is_s3() || root.is_azure()) {
      REQUIRE(vfs.remove_bucket(root).ok());
    } else {
      REQUIRE(vfs.remove_dir(root).ok());
    }
    vfs.terminate();
  }
}

TEST_CASE("VFS: test ls_with_sizes", "[vfs][ls-with-sizes]") {
  ThreadPool compute_tp(4);
  ThreadPool io_tp(4);
  VFS vfs;
  Config config;
  REQUIRE(vfs.init(&g_helper_stats, &compute_tp, &io_tp, config).ok());

#ifdef _WIN32
  std::string path = tiledb::sm::Win::current_dir() + "\\vfs_test\\";
#else
  std::string path =
      std::string("file://") + tiledb::sm::Posix::current_dir() + "/vfs_test/";
#endif

  // Clean up
  bool is_dir;
  vfs.is_dir(URI(path), &is_dir);
  if (is_dir)
    vfs.remove_dir(URI(path));

  std::string dir = path + "ls_dir";
  std::string file = dir + "/file";
  std::string subdir = dir + "/subdir";
  std::string subdir_file = subdir + "/file";

  // Create directories and files
  vfs.create_dir(URI(path));
  vfs.create_dir(URI(dir));
  vfs.create_dir(URI(subdir));
  vfs.touch(URI(file));
  vfs.touch(URI(subdir_file));

  // Write to file
  std::string s1 = "abcdef";
  REQUIRE(vfs.write(URI(file), s1.data(), s1.size()).ok());

  // Write to subdir file
  std::string s2 = "abcdef";
  REQUIRE(vfs.write(URI(subdir_file), s2.data(), s2.size()).ok());

  // List
  auto&& [status, rv] = vfs.ls_with_sizes(URI(dir));
  auto children = *rv;

  REQUIRE(status.ok());

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
  vfs.remove_dir(URI(path));
}
