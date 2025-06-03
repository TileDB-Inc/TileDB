/**
 * @file filter_test_support.cc
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

#include "filter_test_support.h"

using namespace tiledb::common;

namespace tiledb::sm {

SimpleVariableTestData::SimpleVariableTestData()
    : target_ncells_per_chunk_{10}
    , elements_per_chunk_{14, 6, 11, 7, 10, 10, 20, 10, 12}
    , tile_data_generator_{{4, 10, 6, 11, 7, 9, 1, 10, 20, 2, 2, 2, 2, 2, 12}} {
  WhiteboxWriterTile::set_max_tile_chunk_size(
      target_ncells_per_chunk_ * sizeof(uint64_t));
}

SimpleVariableTestData::~SimpleVariableTestData() {
  WhiteboxWriterTile::set_max_tile_chunk_size(constants::max_tile_chunk_size);
}

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
    shared_ptr<MemoryTracker> memory_tracker) {
  // Run the pipeline forward.
  pipeline.run_forward(
      &dummy_stats,
      tile.get(),
      offsets_tile.has_value() ? offsets_tile.value().get() : nullptr,
      &tp);

  // Check the original unfiltered data was removed.
  CHECK(tile->size() == 0);

  // Check the filtered buffer has the expected data.
  auto filtered_buffer = tile->filtered_buffer();
  filtered_buffer_checker.check(filtered_buffer);

  // Run the data in reverse.
  auto unfiltered_tile =
      test_data->create_filtered_buffer_tile(filtered_buffer, memory_tracker);
  ChunkData chunk_data;
  unfiltered_tile.load_chunk_data(chunk_data);
  CHECK(pipeline
            .run_reverse(
                &dummy_stats,
                &unfiltered_tile,
                nullptr,
                chunk_data,
                0,
                chunk_data.filtered_chunks_.size(),
                tp.concurrency_level(),
                config)
            .ok());

  // Check the original data is reverted.
  test_data->check_tile_data(unfiltered_tile);
}

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
    shared_ptr<MemoryTracker> memory_tracker) {
  // Run the pipeline forward.
  pipeline.run_forward(
      &dummy_stats,
      tile.get(),
      offsets_tile.has_value() ? offsets_tile.value().get() : nullptr,
      &tp);

  // Check the original unfiltered data was removed.
  CHECK(tile->size() == 0);

  // Run the data in reverse.
  auto unfiltered_tile = test_data->create_filtered_buffer_tile(
      tile->filtered_buffer(), memory_tracker);
  ChunkData chunk_data;
  unfiltered_tile.load_chunk_data(chunk_data);
  REQUIRE(pipeline
              .run_reverse(
                  &dummy_stats,
                  &unfiltered_tile,
                  nullptr,
                  chunk_data,
                  0,
                  chunk_data.filtered_chunks_.size(),
                  tp.concurrency_level(),
                  config)
              .ok());

  // Check the original data is reverted.
  test_data->check_tile_data(unfiltered_tile);
}

shared_ptr<WriterTile> make_increasing_tile(
    const uint64_t nelts, shared_ptr<MemoryTracker> tracker) {
  const uint64_t tile_size = nelts * sizeof(uint64_t);
  const uint64_t cell_size = sizeof(uint64_t);

  auto tile = make_shared<WriterTile>(
      HERE(),
      constants::format_version,
      Datatype::UINT64,
      cell_size,
      tile_size,
      tracker);
  for (uint64_t i = 0; i < nelts; i++) {
    CHECK_NOTHROW(tile->write(&i, i * sizeof(uint64_t), sizeof(uint64_t)));
  }

  return tile;
}

shared_ptr<WriterTile> make_offsets_tile(
    std::vector<uint64_t>& offsets, shared_ptr<MemoryTracker> tracker) {
  const uint64_t offsets_tile_size =
      offsets.size() * constants::cell_var_offset_size;

  auto offsets_tile = make_shared<WriterTile>(
      HERE(),
      constants::format_version,
      Datatype::UINT64,
      constants::cell_var_offset_size,
      offsets_tile_size,
      tracker);

  // Set up test data
  for (uint64_t i = 0; i < offsets.size(); i++) {
    CHECK_NOTHROW(offsets_tile->write(
        &offsets[i],
        i * constants::cell_var_offset_size,
        constants::cell_var_offset_size));
  }

  return offsets_tile;
}

Tile create_tile_for_unfiltering(
    uint64_t nelts,
    shared_ptr<WriterTile> tile,
    shared_ptr<MemoryTracker> tracker) {
  return {
      tile->format_version(),
      tile->type(),
      tile->cell_size(),
      0,
      tile->cell_size() * nelts,
      tile->filtered_buffer().data(),
      tile->filtered_buffer().size(),
      tracker};
}

void run_reverse(
    const tiledb::sm::Config& config,
    ThreadPool& tp,
    Tile& unfiltered_tile,
    FilterPipeline& pipeline,
    bool success) {
  ChunkData chunk_data;
  unfiltered_tile.load_chunk_data(chunk_data);
  CHECK(
      success == pipeline
                     .run_reverse(
                         &dummy_stats,
                         &unfiltered_tile,
                         nullptr,
                         chunk_data,
                         0,
                         chunk_data.filtered_chunks_.size(),
                         tp.concurrency_level(),
                         config)
                     .ok());
}

}  // namespace tiledb::sm
