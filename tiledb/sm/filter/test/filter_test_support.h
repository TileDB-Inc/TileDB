/**
 * @file filter_test_support.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * This file contains helper functions and test data classes for running tests
 * on filters and filter pipelines.
 *
 **/

#ifndef TILEDB_FILTER_TEST_SUPPORT_H
#define TILEDB_FILTER_TEST_SUPPORT_H

#include <vector>
#include "filtered_tile_checker.h"
#include "tile_data_generator.h"

using namespace tiledb::sm;

namespace tiledb::sm {

// A dummy `Stats` instance.
static stats::Stats dummy_stats("test");

/**
 * Original variable length test from the pipeline tests.
 *
 * For this test target size is 10 cells per chunk. Below is a list of value
 * cell lengths, the cell they are added to, and the rational.
 *
 * target = 8 cells
 * min = 4 cells
 * max = 12 cells
 *
 * | # Cells | Previous/New # Cell in Chunk | Notes                            |
 * |:-------:|:--------|:------------------------------------------------------|
 * |  4      |  0 / 4  | chunk 0: initial chunk                                |
 * |  10     |  4 / 14 | chunk 0: new > max, prev. <= min (next new)           |
 * |  6      |  0 / 6  | chunk 1: new <= target                                |
 * |  11     |  6 / 11 | chunk 2: target < new <= max, prev. > min  (next new) |
 * |  7      |  0 / 7  | chunk 3: new <= target                                |
 * |  9      |  7 / 16 | chunk 4: new > max, prev. > min (this new)            |
 * |  1      |  9 / 10 | chunk 4: new <= target                                |
 * |  10     | 10 / 20 | chunk 5: new > max, prev. > min (this new)            |
 * |  20     |  0 / 20 | chunk 6: new > max, prev. < min (next new)            |
 * |  2      |  0 / 2  | chunk 7: new <= target                                |
 * |  2      |  2 / 4  | chunk 7: new <= target                                |
 * |  2      |  4 / 6  | chunk 7: new <= target                                |
 * |  2      |  6 / 8  | chunk 7: new <= target                                |
 * |  2      |  8 / 10 | chunk 7: new <= target                                |
 * |  12     | 10 / 24 | chunk 8: new > max, prev. > min (this new)            |
 *
 */
class SimpleVariableTestData {
 public:
  SimpleVariableTestData();

  ~SimpleVariableTestData();

  /** Returns the number elements (cells) stored in each chunk. */
  inline const std::vector<uint64_t>& elements_per_chunk() const {
    return elements_per_chunk_;
  }

  /** Returns the tile data generator for the test case. */
  inline const TileDataGenerator& tile_data_generator() const {
    return tile_data_generator_;
  }

 private:
  uint64_t target_ncells_per_chunk_{};
  std::vector<uint64_t> elements_per_chunk_{};
  IncrementTileDataGenerator<uint64_t, Datatype::UINT64> tile_data_generator_;
};

/**
 * Checks the following:
 *
 * 1. Pipeline runs forward without error.
 * 2. Filtered buffer data is as expected.
 * 3. Pipeline runs backward without error.
 * 4. Result from roundtrip matches the original data.
 */
void check_run_pipeline_full(
    Config& config,
    ThreadPool& tp,
    shared_ptr<WriterTile>& tile,
    std::optional<shared_ptr<WriterTile>>& offsets_tile,
    FilterPipeline& pipeline,
    const TileDataGenerator* test_data,
    const FilteredTileChecker& filtered_buffer_checker,
    shared_ptr<MemoryTracker> memory_tracker);

/**
 * Checks the following:
 *
 * 1. Pipeline runs forward without error.
 * 2. Pipeline runs backward without error.
 * 3. Result from roundtrip matches the original data.
 */
void check_run_pipeline_roundtrip(
    Config& config,
    ThreadPool& tp,
    shared_ptr<WriterTile> tile,
    std::optional<shared_ptr<WriterTile>>& offsets_tile,
    const FilterPipeline& pipeline,
    const TileDataGenerator* test_data,
    shared_ptr<MemoryTracker> memory_tracker);

/** Legacy test helper. Do no use in new tests. */
shared_ptr<WriterTile> make_increasing_tile(
    const uint64_t nelts, shared_ptr<MemoryTracker> tracker);

/** Legacy test helper. Do no use in new tests. */
shared_ptr<WriterTile> make_offsets_tile(
    std::vector<uint64_t>& offsets, shared_ptr<MemoryTracker> tracker);

/** Legacy test helper. Do no use in new tests. */
Tile create_tile_for_unfiltering(
    uint64_t nelts,
    shared_ptr<WriterTile> tile,
    shared_ptr<MemoryTracker> tracker);

/** Legacy test helper. Do no use in new tests. */
void run_reverse(
    const tiledb::sm::Config& config,
    ThreadPool& tp,
    Tile& unfiltered_tile,
    FilterPipeline& pipeline,
    bool success = true);

}  // namespace tiledb::sm
#endif
