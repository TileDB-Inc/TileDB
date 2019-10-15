/**
 * @file   unit-vfs.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2019 TileDB, Inc.
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

#include <atomic>
#include <catch.hpp>
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/misc/stats.h"

using namespace tiledb::sm;

TEST_CASE("VFS: Test read batching", "[vfs]") {
  URI testfile("vfs_unit_test_data");
  std::unique_ptr<VFS> vfs(new VFS);
  REQUIRE(vfs->init(nullptr, nullptr).ok());

  bool exists = false;
  REQUIRE(vfs->is_file(testfile, &exists).ok());
  if (exists)
    vfs->remove_file(testfile);

  // Write some data.
  const unsigned nelts = 100;
  uint32_t data_write[nelts], data_read[nelts];
  for (unsigned i = 0; i < nelts; i++)
    data_write[i] = i;
  REQUIRE(vfs->write(testfile, data_write, nelts * sizeof(uint32_t)).ok());

  // Enable stats.
  stats::all_stats.set_enabled(true);
  stats::all_stats.reset();

  std::vector<std::tuple<uint64_t, void*, uint64_t>> batches;
  ThreadPool thread_pool;
  REQUIRE(thread_pool.init(4).ok());
  std::vector<std::future<Status>> tasks;

  SECTION("- Default config") {
    // Check reading in one batch: single read operation.
    std::memset(data_read, 0, nelts * sizeof(uint32_t));
    batches.emplace_back(0, data_read, nelts * sizeof(uint32_t));
    REQUIRE(vfs->read_all(testfile, batches, &thread_pool, &tasks).ok());
    REQUIRE(thread_pool.wait_all(tasks).ok());
    tasks.clear();
    for (unsigned i = 0; i < nelts; i++)
      REQUIRE(data_read[i] == i);
    REQUIRE(stats::all_stats.vfs_read_call_count == 1);
    REQUIRE(
        stats::all_stats.counter_vfs_read_total_bytes ==
        nelts * sizeof(uint32_t));

    // Check reading first and last element: 1 reads due to the default
    // batch size.
    stats::all_stats.reset();
    std::memset(data_read, 0, nelts * sizeof(uint32_t));
    batches.clear();
    batches.emplace_back(0, &data_read[0], sizeof(uint32_t));
    batches.emplace_back(
        (nelts - 1) * sizeof(uint32_t), &data_read[1], sizeof(uint32_t));
    REQUIRE(vfs->read_all(testfile, batches, &thread_pool, &tasks).ok());
    REQUIRE(thread_pool.wait_all(tasks).ok());
    tasks.clear();
    REQUIRE(data_read[0] == 0);
    REQUIRE(data_read[1] == nelts - 1);
    REQUIRE(stats::all_stats.vfs_read_call_count == 1);
    REQUIRE(
        stats::all_stats.counter_vfs_read_total_bytes ==
        100 * sizeof(uint32_t));

    // Check each element as a different region: single read because there is no
    // amplification required (all work is useful).
    stats::all_stats.reset();
    std::memset(data_read, 0, nelts * sizeof(uint32_t));
    batches.clear();
    for (unsigned i = 0; i < nelts; i++)
      batches.emplace_back(
          i * sizeof(uint32_t), &data_read[i], sizeof(uint32_t));
    REQUIRE(vfs->read_all(testfile, batches, &thread_pool, &tasks).ok());
    REQUIRE(thread_pool.wait_all(tasks).ok());
    tasks.clear();
    for (unsigned i = 0; i < nelts; i++)
      REQUIRE(data_read[i] == i);
    REQUIRE(stats::all_stats.vfs_read_call_count == 1);
    REQUIRE(
        stats::all_stats.counter_vfs_read_total_bytes ==
        nelts * sizeof(uint32_t));
  }

  SECTION("- Reduce min batch size and min batch gap") {
    // Set a smaller min batch size and min batch gap
    Config default_config, vfs_config;
    vfs_config.set("vfs.min_batch_size", "0");
    vfs_config.set("vfs.min_batch_gap", "0");
    REQUIRE(vfs->init(&default_config, &vfs_config).ok());

    // Check large batches are not split up.
    std::memset(data_read, 0, nelts * sizeof(uint32_t));
    batches.emplace_back(0, data_read, nelts * sizeof(uint32_t));
    REQUIRE(vfs->read_all(testfile, batches, &thread_pool, &tasks).ok());
    REQUIRE(thread_pool.wait_all(tasks).ok());
    tasks.clear();
    for (unsigned i = 0; i < nelts; i++)
      REQUIRE(data_read[i] == i);
    REQUIRE(stats::all_stats.vfs_read_call_count == 1);
    REQUIRE(
        stats::all_stats.counter_vfs_read_total_bytes ==
        nelts * sizeof(uint32_t));

    // Check each element as a different region (results in several read
    // operations).
    stats::all_stats.reset();
    std::memset(data_read, 0, nelts * sizeof(uint32_t));
    batches.clear();
    for (unsigned i = 0; i < nelts / 2; i++)
      batches.emplace_back(
          2 * i * sizeof(uint32_t), &data_read[i], sizeof(uint32_t));
    REQUIRE(vfs->read_all(testfile, batches, &thread_pool, &tasks).ok());
    REQUIRE(thread_pool.wait_all(tasks).ok());
    tasks.clear();
    for (unsigned i = 0; i < nelts / 2; i++)
      REQUIRE(data_read[i] == 2 * i);
    CHECK(stats::all_stats.vfs_read_call_count == nelts / 2);
    CHECK(
        stats::all_stats.counter_vfs_read_total_bytes ==
        (nelts / 2) * sizeof(uint32_t));

    // Check reading first and last element (results in 2 reads because the
    // whole region is too big).
    stats::all_stats.reset();
    std::memset(data_read, 0, nelts * sizeof(uint32_t));
    batches.clear();
    batches.emplace_back(0, &data_read[0], sizeof(uint32_t));
    batches.emplace_back(
        (nelts - 1) * sizeof(uint32_t), &data_read[1], sizeof(uint32_t));
    REQUIRE(vfs->read_all(testfile, batches, &thread_pool, &tasks).ok());
    REQUIRE(thread_pool.wait_all(tasks).ok());
    tasks.clear();
    REQUIRE(data_read[0] == 0);
    REQUIRE(data_read[1] == nelts - 1);
    REQUIRE(stats::all_stats.vfs_read_call_count == 2);
    REQUIRE(
        stats::all_stats.counter_vfs_read_total_bytes == 2 * sizeof(uint32_t));
    REQUIRE(vfs->terminate().ok());
  }

  SECTION("- Reduce min batch size but not min batch gap") {
    // Set a smaller min batch size
    Config default_config, vfs_config;
    vfs_config.set("vfs.min_batch_size", "0");
    REQUIRE(vfs->init(&default_config, &vfs_config).ok());

    // There should be a single read due to the gap
    std::memset(data_read, 0, nelts * sizeof(uint32_t));
    batches.clear();
    for (unsigned i = 0; i < nelts; i++)
      batches.emplace_back(
          i * sizeof(uint32_t), &data_read[i], sizeof(uint32_t));
    REQUIRE(vfs->read_all(testfile, batches, &thread_pool, &tasks).ok());
    REQUIRE(thread_pool.wait_all(tasks).ok());
    tasks.clear();
    for (unsigned i = 0; i < nelts; i++)
      REQUIRE(data_read[i] == i);
    CHECK(stats::all_stats.vfs_read_call_count == 1);
    CHECK(
        stats::all_stats.counter_vfs_read_total_bytes ==
        nelts * sizeof(uint32_t));
  }

  SECTION("- Reduce min batch gap but not min batch size") {
    // Set a smaller min batch size
    Config default_config, vfs_config;
    vfs_config.set("vfs.min_batch_gap", "0");
    REQUIRE(vfs->init(&default_config, &vfs_config).ok());

    // There should be a single read due to the batch size
    std::memset(data_read, 0, nelts * sizeof(uint32_t));
    batches.clear();
    for (unsigned i = 0; i < nelts; i++)
      batches.emplace_back(
          i * sizeof(uint32_t), &data_read[i], sizeof(uint32_t));
    REQUIRE(vfs->read_all(testfile, batches, &thread_pool, &tasks).ok());
    REQUIRE(thread_pool.wait_all(tasks).ok());
    tasks.clear();
    for (unsigned i = 0; i < nelts; i++)
      REQUIRE(data_read[i] == i);
    CHECK(stats::all_stats.vfs_read_call_count == 1);
    CHECK(
        stats::all_stats.counter_vfs_read_total_bytes ==
        nelts * sizeof(uint32_t));
  }

  REQUIRE(vfs->is_file(testfile, &exists).ok());
  if (exists)
    REQUIRE(vfs->remove_file(testfile).ok());

  REQUIRE(vfs->terminate().ok());
}

#ifdef _WIN32

TEST_CASE("VFS: Test long paths (Win32)", "[vfs][windows]") {
  std::unique_ptr<VFS> vfs(new VFS);
  std::string tmpdir_base = tiledb::sm::Win::current_dir() + "\\tiledb_test\\";
  REQUIRE(vfs->init(nullptr, nullptr).ok());
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
  std::unique_ptr<VFS> vfs(new VFS);
  REQUIRE(vfs->init(nullptr, nullptr).ok());

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
}

#endif
