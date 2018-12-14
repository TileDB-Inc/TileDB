/**
 * @file   unit-vfs.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018 TileDB, Inc.
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

  SECTION("- Default config") {
    // Check reading in one batch: single read operation.
    std::memset(data_read, 0, nelts * sizeof(uint32_t));
    batches.emplace_back(0, data_read, nelts * sizeof(uint32_t));
    REQUIRE(vfs->read_all(testfile, batches).ok());
    for (unsigned i = 0; i < nelts; i++)
      REQUIRE(data_read[i] == i);
    REQUIRE(stats::all_stats.vfs_read_call_count == 1);
    REQUIRE(
        stats::all_stats.counter_vfs_read_total_bytes ==
        nelts * sizeof(uint32_t));

    // Check reading first and last element: 2 reads due to the default
    // amplification limit.
    stats::all_stats.reset();
    std::memset(data_read, 0, nelts * sizeof(uint32_t));
    batches.clear();
    batches.emplace_back(0, &data_read[0], sizeof(uint32_t));
    batches.emplace_back(
        (nelts - 1) * sizeof(uint32_t), &data_read[1], sizeof(uint32_t));
    REQUIRE(vfs->read_all(testfile, batches).ok());
    REQUIRE(data_read[0] == 0);
    REQUIRE(data_read[1] == nelts - 1);
    REQUIRE(stats::all_stats.vfs_read_call_count == 2);
    REQUIRE(
        stats::all_stats.counter_vfs_read_total_bytes == 2 * sizeof(uint32_t));

    // Check each element as a different region: single read because there is no
    // amplification required (all work is useful).
    stats::all_stats.reset();
    std::memset(data_read, 0, nelts * sizeof(uint32_t));
    batches.clear();
    for (unsigned i = 0; i < nelts; i++)
      batches.emplace_back(
          i * sizeof(uint32_t), &data_read[i], sizeof(uint32_t));
    REQUIRE(vfs->read_all(testfile, batches).ok());
    for (unsigned i = 0; i < nelts; i++)
      REQUIRE(data_read[i] == i);
    REQUIRE(stats::all_stats.vfs_read_call_count == 1);
    REQUIRE(
        stats::all_stats.counter_vfs_read_total_bytes ==
        nelts * sizeof(uint32_t));
  }

  SECTION("- Limit max batch size") {
    // Set a smaller max batch size
    const unsigned batch_bytes = 13;
    Config::VFSParams vfs_params;
    vfs_params.max_batch_read_size_ = batch_bytes;
    REQUIRE(vfs->init(vfs_params).ok());

    // Check large batches are not split up.
    std::memset(data_read, 0, nelts * sizeof(uint32_t));
    batches.emplace_back(0, data_read, nelts * sizeof(uint32_t));
    REQUIRE(vfs->read_all(testfile, batches).ok());
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
    for (unsigned i = 0; i < nelts; i++)
      batches.emplace_back(
          i * sizeof(uint32_t), &data_read[i], sizeof(uint32_t));
    REQUIRE(vfs->read_all(testfile, batches).ok());
    for (unsigned i = 0; i < nelts; i++)
      REQUIRE(data_read[i] == i);
    unsigned expected_num_reads =
        nelts / (batch_bytes / sizeof(uint32_t)) +
        unsigned(nelts % (batch_bytes / sizeof(uint32_t)) != 0);
    REQUIRE(stats::all_stats.vfs_read_call_count == expected_num_reads);
    REQUIRE(
        stats::all_stats.counter_vfs_read_total_bytes ==
        nelts * sizeof(uint32_t));

    // Check reading first and last element (results in 2 reads because the
    // whole region is too big).
    stats::all_stats.reset();
    std::memset(data_read, 0, nelts * sizeof(uint32_t));
    batches.clear();
    batches.emplace_back(0, &data_read[0], sizeof(uint32_t));
    batches.emplace_back(
        (nelts - 1) * sizeof(uint32_t), &data_read[1], sizeof(uint32_t));
    REQUIRE(vfs->read_all(testfile, batches).ok());
    REQUIRE(data_read[0] == 0);
    REQUIRE(data_read[1] == nelts - 1);
    REQUIRE(stats::all_stats.vfs_read_call_count == 2);
    REQUIRE(
        stats::all_stats.counter_vfs_read_total_bytes == 2 * sizeof(uint32_t));
  }

  SECTION("- Limit amplification") {
    // No amplification allowed.
    Config::VFSParams vfs_params;
    vfs_params.max_batch_read_amplification_ = 1;
    REQUIRE(vfs->init(vfs_params).ok());

    // Check each element as a different region: still a single read because
    // there is no amplification necessary (all work is useful).
    stats::all_stats.reset();
    std::memset(data_read, 0, nelts * sizeof(uint32_t));
    batches.clear();
    for (unsigned i = 0; i < nelts; i++)
      batches.emplace_back(
          i * sizeof(uint32_t), &data_read[i], sizeof(uint32_t));
    REQUIRE(vfs->read_all(testfile, batches).ok());
    for (unsigned i = 0; i < nelts; i++)
      REQUIRE(data_read[i] == i);
    REQUIRE(stats::all_stats.vfs_read_call_count == 1);
    REQUIRE(
        stats::all_stats.counter_vfs_read_total_bytes ==
        nelts * sizeof(uint32_t));

    // Read elements 0 and 2: 2 reads because no amplification allowed.
    stats::all_stats.reset();
    std::memset(data_read, 0, nelts * sizeof(uint32_t));
    batches.clear();
    batches.emplace_back(0, &data_read[0], sizeof(uint32_t));
    batches.emplace_back(2 * sizeof(uint32_t), &data_read[1], sizeof(uint32_t));
    REQUIRE(vfs->read_all(testfile, batches).ok());
    REQUIRE(data_read[0] == 0);
    REQUIRE(data_read[1] == 2);
    REQUIRE(stats::all_stats.vfs_read_call_count == 2);
    REQUIRE(
        stats::all_stats.counter_vfs_read_total_bytes == 2 * sizeof(uint32_t));

    // Increase amplification to 1.5x.
    vfs_params.max_batch_read_amplification_ = 1.5;
    REQUIRE(vfs->init(vfs_params).ok());

    // Read elements 0 and 2 again: 1 read operation because reading 12 bytes
    // and using 8 bytes is 12/8 = 1.5x amplification.
    stats::all_stats.reset();
    std::memset(data_read, 0, nelts * sizeof(uint32_t));
    batches.clear();
    batches.emplace_back(0, &data_read[0], sizeof(uint32_t));
    batches.emplace_back(2 * sizeof(uint32_t), &data_read[1], sizeof(uint32_t));
    REQUIRE(vfs->read_all(testfile, batches).ok());
    REQUIRE(data_read[0] == 0);
    REQUIRE(data_read[1] == 2);
    REQUIRE(stats::all_stats.vfs_read_call_count == 1);
    REQUIRE(
        stats::all_stats.counter_vfs_read_total_bytes == 3 * sizeof(uint32_t));
  }

  REQUIRE(vfs->is_file(testfile, &exists).ok());
  if (exists)
    vfs->remove_file(testfile);
}
