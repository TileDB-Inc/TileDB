/**
 * @file unit_run_filter_pipeline.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * This set of unit tests checks running the filter pipeline forward and
 * backward. Most tests here use simplified filters that perform basic
 * operations such as adding 1 to each value in the original data so that the
 * filtered data itself can be checked after running the filter pipeline
 * forward.
 *
 *
 * Notes on variable length data:
 *
 * The filtered pipeline will break-up tile data into chunks for filtering.
 * Below we describe the decision process for adding to the existing chunk vs
 * creating a new chunk for variable length data.
 *
 * Define the following when adding a value of variable length data:
 *
 * * "current size": the size of the current chunk before adding the data
 * * "new size": the size of the current chunk if the new data is added to it
 * * "target size": the target size for chunks
 * * "min size": 50% the target size for chunks
 * * "max size": 150% the target size for chunks
 *
 * A new chunk is created if the total size > target size.
 *
 * When a new chunk is created, if either of the following are met, then add
 * the current component to the existing chunk:
 *
 *  Condition 1. current size < min size
 *  Condition 2. new size < max size
 *
 */

#include <algorithm>
#include <optional>
#include <random>

#include <test/support/tdb_catch.h>

#include "../bit_width_reduction_filter.h"
#include "../bitshuffle_filter.h"
#include "../byteshuffle_filter.h"
#include "../checksum_md5_filter.h"
#include "../checksum_sha256_filter.h"
#include "../compression_filter.h"
#include "../encryption_aes256gcm_filter.h"
#include "../filter.h"
#include "../filter_pipeline.h"
#include "../positive_delta_filter.h"
#include "add_1_in_place_filter.h"
#include "add_1_including_metadata_filter.h"
#include "add_1_out_of_place_filter.h"
#include "add_n_in_place_filter.h"
#include "filtered_tile_checker.h"
#include "pseudo_checksum_filter.h"
#include "tile_data_generator.h"
#include "tiledb/sm/crypto/encryption_key.h"
#include "tiledb/sm/enums/compressor.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/filter_option.h"
#include "tiledb/sm/enums/filter_type.h"
#include "tiledb/sm/tile/tile.h"

using namespace tiledb::sm;

// A dummy `Stats` instance.
static tiledb::sm::stats::Stats dummy_stats("test");

class SimpleTestData {};

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
  SimpleVariableTestData()
      : target_ncells_per_chunk_{10}
      , elements_per_chunk_{14, 6, 11, 7, 10, 10, 20, 10, 12}
      , tile_data_generator_{
            {4, 10, 6, 11, 7, 9, 1, 10, 20, 2, 2, 2, 2, 2, 12}} {
    WriterTile::set_max_tile_chunk_size(
        target_ncells_per_chunk_ * sizeof(uint64_t));
  }
  ~SimpleVariableTestData() {
    WriterTile::set_max_tile_chunk_size(constants::max_tile_chunk_size);
  }

  /** Returns the number elements (cells) stored in each chunk. */
  const std::vector<uint64_t>& elements_per_chunk() const {
    return elements_per_chunk_;
  }

  const TileDataGenerator& tile_data_generator() const {
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
    WriterTile& tile,
    std::optional<WriterTile>& offsets_tile,
    FilterPipeline& pipeline,
    const TileDataGenerator* test_data,
    const FilteredTileChecker& filtered_buffer_checker) {
  // Run the pipeline forward.
  CHECK(pipeline
            .run_forward(
                &dummy_stats,
                &tile,
                offsets_tile.has_value() ? &offsets_tile.value() : nullptr,
                &tp)
            .ok());

  // Check the original unfiltered data was removed.
  CHECK(tile.size() == 0);

  // Check the filtered buffer has the expected data.
  auto filtered_buffer = tile.filtered_buffer();
  filtered_buffer_checker.check(filtered_buffer);

  // Run the data in reverse.
  auto unfiltered_tile =
      test_data->create_filtered_buffer_tile(filtered_buffer);
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
    WriterTile& tile,
    std::optional<WriterTile>& offsets_tile,
    FilterPipeline& pipeline,
    TileDataGenerator* test_data) {
  // Run the pipeline forward.
  CHECK(pipeline
            .run_forward(
                &dummy_stats,
                &tile,
                offsets_tile.has_value() ? &offsets_tile.value() : nullptr,
                &tp)
            .ok());

  // Check the original unfiltered data was removed.
  CHECK(tile.size() == 0);

  // Run the data in reverse.
  auto unfiltered_tile =
      test_data->create_filtered_buffer_tile(tile.filtered_buffer());
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

TEST_CASE("Filter: Test empty pipeline", "[filter][empty-pipeline]") {
  // Create TileDB needed for running pipeline.
  Config config;
  ThreadPool tp(4);

  // Set-up test data.
  IncrementTileDataGenerator<uint64_t, Datatype::UINT64> tile_data_generator(
      100);
  auto&& [tile, offsets_tile] = tile_data_generator.create_writer_tiles();
  std::vector<uint64_t> elements_per_chunk{100};

  // Create pipeline.
  FilterPipeline pipeline;

  // Create  expected filtered data checker.
  auto filtered_buffer_checker =
      FilteredTileChecker::create_uncompressed_with_grid_chunks<uint64_t>(
          elements_per_chunk, 0, 1);

  // Run the pipeline tests.
  check_run_pipeline_full(
      config,
      tp,
      tile,
      offsets_tile,
      pipeline,
      &tile_data_generator,
      filtered_buffer_checker);
}

TEST_CASE(
    "Filter: Test empty pipeline on uint16 data", "[filter][empty-pipeline]") {
  // Create TileDB needed for running pipeline.
  Config config;
  ThreadPool tp(4);

  // Set-up test data.
  IncrementTileDataGenerator<uint16_t, Datatype::UINT16> tile_data_generator(
      100);
  auto&& [tile, offsets_tile] = tile_data_generator.create_writer_tiles();
  std::vector<uint64_t> elements_per_chunk{100};

  // Create pipeline.
  FilterPipeline pipeline;

  // Create  expected filtered data checker.
  auto filtered_buffer_checker =
      FilteredTileChecker::create_uncompressed_with_grid_chunks<uint16_t>(
          elements_per_chunk, 0, 1);

  // Run the pipeline tests.
  check_run_pipeline_full(
      config,
      tp,
      tile,
      offsets_tile,
      pipeline,
      &tile_data_generator,
      filtered_buffer_checker);
}

TEST_CASE(
    "Filter: Test empty pipeline var sized", "[filter][empty-pipeline][var]") {
  // Create TileDB needed for running pipeline.
  Config config;
  ThreadPool tp(4);

  // Set-up test data.
  SimpleVariableTestData test_data{};
  const auto& tile_data_generator = test_data.tile_data_generator();
  auto&& [tile, offsets_tile] = tile_data_generator.create_writer_tiles();
  const auto& elements_per_chunk = test_data.elements_per_chunk();

  // Create pipeline to test and expected filtered data checker.
  FilterPipeline pipeline;
  auto filtered_buffer_checker =
      FilteredTileChecker::create_uncompressed_with_grid_chunks<uint64_t>(
          elements_per_chunk, 0, 1);

  // Run the round-trip test.
  check_run_pipeline_full(
      config,
      tp,
      tile,
      offsets_tile,
      pipeline,
      &tile_data_generator,
      filtered_buffer_checker);
}

TEST_CASE(
    "Filter: Test simple in-place pipeline", "[filter][simple-in-place]") {
  // Create TileDB needed for running pipeline.
  Config config;
  ThreadPool tp(4);

  // Set-up test data.
  IncrementTileDataGenerator<uint64_t, Datatype::UINT64> tile_data_generator(
      100);
  auto&& [tile, offsets_tile] = tile_data_generator.create_writer_tiles();
  std::vector<uint64_t> elements_per_chunk{100};

  FilterPipeline pipeline;
  pipeline.add_filter(Add1InPlace(Datatype::UINT64));

  SECTION("- Single stage") {
    // Create expected filtered data checker.
    auto filtered_buffer_checker =
        FilteredTileChecker::create_uncompressed_with_grid_chunks<uint64_t>(
            elements_per_chunk, 1, 1);

    // Run the pipeline tests.
    check_run_pipeline_full(
        config,
        tp,
        tile,
        offsets_tile,
        pipeline,
        &tile_data_generator,
        filtered_buffer_checker);
  }

  SECTION("- Multi-stage") {
    // Add a few more +1 filters and re-run.
    pipeline.add_filter(Add1InPlace(Datatype::UINT64));
    pipeline.add_filter(Add1InPlace(Datatype::UINT64));

    // Create expected filtered data checker.
    auto filtered_buffer_checker =
        FilteredTileChecker::create_uncompressed_with_grid_chunks<uint64_t>(
            elements_per_chunk, 3, 1);

    // Run the pipeline tests.
    check_run_pipeline_full(
        config,
        tp,
        tile,
        offsets_tile,
        pipeline,
        &tile_data_generator,
        filtered_buffer_checker);
  }
}

TEST_CASE(
    "Filter: Test simple in-place pipeline var",
    "[filter][simple-in-place][var]") {
  // Create TileDB needed for running pipeline.
  Config config;
  ThreadPool tp(4);

  // Set-up test data.
  SimpleVariableTestData test_data{};
  const auto& tile_data_generator = test_data.tile_data_generator();
  auto&& [tile, offsets_tile] = tile_data_generator.create_writer_tiles();
  const auto& elements_per_chunk = test_data.elements_per_chunk();

  FilterPipeline pipeline;
  pipeline.add_filter(Add1InPlace(Datatype::UINT64));

  SECTION("- Single stage") {
    // Create expected filtered data checker.
    auto filtered_buffer_checker =
        FilteredTileChecker::create_uncompressed_with_grid_chunks<uint64_t>(
            elements_per_chunk, 1, 1);

    // Run the pipeline tests.
    check_run_pipeline_full(
        config,
        tp,
        tile,
        offsets_tile,
        pipeline,
        &tile_data_generator,
        filtered_buffer_checker);
  }

  SECTION("- Multi-stage") {
    // Add a few more +1 filters and re-run.
    WriterTile::set_max_tile_chunk_size(80);
    pipeline.add_filter(Add1InPlace(Datatype::UINT64));
    pipeline.add_filter(Add1InPlace(Datatype::UINT64));

    // Create expected filtered data checker.
    auto filtered_buffer_checker =
        FilteredTileChecker::create_uncompressed_with_grid_chunks<uint64_t>(
            elements_per_chunk, 3, 1);

    // Run the pipeline tests.
    check_run_pipeline_full(
        config,
        tp,
        tile,
        offsets_tile,
        pipeline,
        &tile_data_generator,
        filtered_buffer_checker);
  }
}

TEST_CASE(
    "Filter: Test simple out-of-place pipeline",
    "[filter][simple-out-of-place]") {
  // Create TileDB needed for running pipeline.
  Config config;
  ThreadPool tp(4);

  // Set-up test data.
  IncrementTileDataGenerator<uint64_t, Datatype::UINT64> tile_data_generator(
      100);
  auto&& [tile, offsets_tile] = tile_data_generator.create_writer_tiles();
  std::vector<uint64_t> elements_per_chunk{100};

  // Create pipeline to test.
  FilterPipeline pipeline;
  pipeline.add_filter(Add1OutOfPlace(Datatype::UINT64));

  SECTION("- Single stage") {
    auto filtered_buffer_checker =
        FilteredTileChecker::create_uncompressed_with_grid_chunks<uint64_t>(
            elements_per_chunk, 1, 1);

    // Run the pipeline tests.
    check_run_pipeline_full(
        config,
        tp,
        tile,
        offsets_tile,
        pipeline,
        &tile_data_generator,
        filtered_buffer_checker);
  }

  SECTION("- Multi-stage") {
    // Add a few more +1 filters and re-run.
    pipeline.add_filter(Add1OutOfPlace(Datatype::UINT64));
    pipeline.add_filter(Add1OutOfPlace(Datatype::UINT64));

    auto filtered_buffer_checker =
        FilteredTileChecker::create_uncompressed_with_grid_chunks<uint64_t>(
            elements_per_chunk, 3, 1);

    // Run the pipeline tests.
    check_run_pipeline_full(
        config,
        tp,
        tile,
        offsets_tile,
        pipeline,
        &tile_data_generator,
        filtered_buffer_checker);
  }
}

TEST_CASE(
    "Filter: Test simple out-of-place pipeline var",
    "[filter][simple-out-of-place][var]") {
  // Create TileDB needed for running pipeline.
  Config config;
  ThreadPool tp(4);

  // Set-up test data.
  SimpleVariableTestData test_data{};
  const auto& tile_data_generator = test_data.tile_data_generator();
  auto&& [tile, offsets_tile] = tile_data_generator.create_writer_tiles();
  const auto& elements_per_chunk = test_data.elements_per_chunk();

  FilterPipeline pipeline;
  pipeline.add_filter(Add1OutOfPlace(Datatype::UINT64));

  SECTION("- Single stage") {
    // Create expected filtered data checker.
    auto filtered_buffer_checker =
        FilteredTileChecker::create_uncompressed_with_grid_chunks<uint64_t>(
            elements_per_chunk, 1, 1);

    // run the pipeline tests.
    check_run_pipeline_full(
        config,
        tp,
        tile,
        offsets_tile,
        pipeline,
        &tile_data_generator,
        filtered_buffer_checker);
  }

  SECTION("- Multi-stage") {
    pipeline.add_filter(Add1OutOfPlace(Datatype::UINT64));
    pipeline.add_filter(Add1OutOfPlace(Datatype::UINT64));

    // Create expected filtered data checker.
    auto filtered_buffer_checker =
        FilteredTileChecker::create_uncompressed_with_grid_chunks<uint64_t>(
            elements_per_chunk, 3, 1);

    // run the pipeline tests.
    check_run_pipeline_full(
        config,
        tp,
        tile,
        offsets_tile,
        pipeline,
        &tile_data_generator,
        filtered_buffer_checker);
  }
}

TEST_CASE(
    "Filter: Test mixed in- and out-of-place pipeline",
    "[filter][in-out-place]") {
  // Create TileDB needed for running pipeline.
  Config config;
  ThreadPool tp(4);

  // Set-up test data.
  IncrementTileDataGenerator<uint64_t, Datatype::UINT64> tile_data_generator(
      100);
  auto&& [tile, offsets_tile] = tile_data_generator.create_writer_tiles();
  std::vector<uint64_t> elements_per_chunk{100};

  // Create filter pipeline.
  FilterPipeline pipeline;
  pipeline.add_filter(Add1InPlace(Datatype::UINT64));
  pipeline.add_filter(Add1OutOfPlace(Datatype::UINT64));
  pipeline.add_filter(Add1InPlace(Datatype::UINT64));
  pipeline.add_filter(Add1OutOfPlace(Datatype::UINT64));

  // Create expected filtered data checker.
  auto filtered_buffer_checker =
      FilteredTileChecker::create_uncompressed_with_grid_chunks<uint64_t>(
          elements_per_chunk, 4, 1);

  // Run the pipeline tests.
  check_run_pipeline_full(
      config,
      tp,
      tile,
      offsets_tile,
      pipeline,
      &tile_data_generator,
      filtered_buffer_checker);
}

TEST_CASE(
    "Filter: Test mixed in- and out-of-place pipeline var",
    "[filter][in-out-place][var]") {
  // Create TileDB needed for running pipeline.
  Config config;
  ThreadPool tp(4);

  // Set-up test data.
  SimpleVariableTestData test_data{};
  const auto& tile_data_generator = test_data.tile_data_generator();
  auto&& [tile, offsets_tile] = tile_data_generator.create_writer_tiles();
  const auto& elements_per_chunk = test_data.elements_per_chunk();

  FilterPipeline pipeline;
  pipeline.add_filter(Add1InPlace(Datatype::UINT64));
  pipeline.add_filter(Add1OutOfPlace(Datatype::UINT64));
  pipeline.add_filter(Add1InPlace(Datatype::UINT64));
  pipeline.add_filter(Add1OutOfPlace(Datatype::UINT64));

  // Create expected filtered data checker.
  auto filtered_buffer_checker =
      FilteredTileChecker::create_uncompressed_with_grid_chunks<uint64_t>(
          elements_per_chunk, 4, 1);

  // Run the pipeline tests.
  check_run_pipeline_full(
      config,
      tp,
      tile,
      offsets_tile,
      pipeline,
      &tile_data_generator,
      filtered_buffer_checker);
}

TEST_CASE("Filter: Test pseudo-checksum", "[filter][pseudo-checksum]") {
  // Create TileDB needed for running pipeline.
  Config config;
  ThreadPool tp(4);

  // Set-up test data.
  IncrementTileDataGenerator<uint64_t, Datatype::UINT64> tile_data_generator(
      100);
  auto&& [tile, offsets_tile] = tile_data_generator.create_writer_tiles();
  std::vector<uint64_t> elements_per_chunk{100};

  // Create filter pipeline.
  FilterPipeline pipeline;
  pipeline.add_filter(PseudoChecksumFilter(Datatype::UINT64));
  const uint64_t expected_checksum = 4950;

  SECTION("- Single stage") {
    // Create filtered buffer checker.
    auto filtered_buffer_checker =
        FilteredTileChecker::create_uncompressed_with_grid_chunks<uint64_t>(
            elements_per_chunk, {{expected_checksum}}, 0, 1);

    // Run the pipeline tests.
    check_run_pipeline_full(
        config,
        tp,
        tile,
        offsets_tile,
        pipeline,
        &tile_data_generator,
        filtered_buffer_checker);
  }

  SECTION("- Multi-stage") {
    // Add filters.
    pipeline.add_filter(Add1OutOfPlace(Datatype::UINT64));
    pipeline.add_filter(Add1InPlace(Datatype::UINT64));
    pipeline.add_filter(PseudoChecksumFilter(Datatype::UINT64));

    // Create filtered buffer checker.
    //    const uint64_t expected_checksum = 2 * 4950;
    // Compute the second (final) checksum value.
    const uint64_t expected_checksum_2 = 5150;

    // Create filtered buffer checker.
    auto filtered_buffer_checker =
        FilteredTileChecker::create_uncompressed_with_grid_chunks<uint64_t>(
            elements_per_chunk,
            {{expected_checksum_2, expected_checksum}},
            2,
            1);

    // Run the pipeline tests.
    check_run_pipeline_full(
        config,
        tp,
        tile,
        offsets_tile,
        pipeline,
        &tile_data_generator,
        filtered_buffer_checker);
  }
}

TEST_CASE(
    "Filter: Test pseudo-checksum var", "[filter][pseudo-checksum][var]") {
  // Create TileDB needed for running pipeline.
  Config config;
  ThreadPool tp(4);

  // Set-up test data.
  SimpleVariableTestData test_data{};
  const auto& tile_data_generator = test_data.tile_data_generator();
  auto&& [tile, offsets_tile] = tile_data_generator.create_writer_tiles();
  const auto& elements_per_chunk = test_data.elements_per_chunk();

  // Create filter pipeline.
  FilterPipeline pipeline;
  pipeline.add_filter(PseudoChecksumFilter(Datatype::UINT64));

  SECTION("- Single stage") {
    // Create expected filtered data checker.
    std::vector<std::vector<uint64_t>> expected_checksums{
        {91}, {99}, {275}, {238}, {425}, {525}, {1350}, {825}, {1122}};
    auto filtered_buffer_checker =
        FilteredTileChecker::create_uncompressed_with_grid_chunks<uint64_t>(
            elements_per_chunk, expected_checksums, 0, 1);

    // Run the pipeline tests.
    check_run_pipeline_full(
        config,
        tp,
        tile,
        offsets_tile,
        pipeline,
        &tile_data_generator,
        filtered_buffer_checker);
  }

  SECTION("- Multi-stage") {
    // Update pipeline.
    pipeline.add_filter(Add1OutOfPlace(Datatype::UINT64));
    pipeline.add_filter(Add1InPlace(Datatype::UINT64));
    pipeline.add_filter(PseudoChecksumFilter(Datatype::UINT64));

    // Create expected filtered data checker.
    std::vector<std::vector<uint64_t>> expected_checksums{
        {119, 91},
        {111, 99},
        {297, 275},
        {252, 238},
        {445, 425},
        {545, 525},
        {1390, 1350},
        {845, 825},
        {1146, 1122}};
    auto filtered_buffer_checker =
        FilteredTileChecker::create_uncompressed_with_grid_chunks<uint64_t>(
            elements_per_chunk, expected_checksums, 2, 1);

    // Run the pipeline tests.
    check_run_pipeline_full(
        config,
        tp,
        tile,
        offsets_tile,
        pipeline,
        &tile_data_generator,
        filtered_buffer_checker);
  }
}

TEST_CASE("Filter: Test pipeline modify filter", "[filter][modify]") {
  // Create TileDB needed for running pipeline.
  Config config;
  ThreadPool tp(4);

  // Set-up test data.
  IncrementTileDataGenerator<uint64_t, Datatype::UINT64> tile_data_generator(
      100);
  auto&& [tile, offsets_tile] = tile_data_generator.create_writer_tiles();
  std::vector<uint64_t> elements_per_chunk{100};

  // Create filter pipeline.
  FilterPipeline pipeline;
  pipeline.add_filter(Add1InPlace(Datatype::UINT64));
  pipeline.add_filter(AddNInPlace(Datatype::UINT64));
  pipeline.add_filter(Add1InPlace(Datatype::UINT64));

  // Get non-existent filter instance
  auto* cksum = pipeline.get_filter<PseudoChecksumFilter>();
  CHECK(cksum == nullptr);

  // Modify +N filter
  auto* add_n = pipeline.get_filter<AddNInPlace>();
  CHECK(add_n != nullptr);
  add_n->set_increment(2);

  // Create pipeline to test and expected filtered data checker.
  auto filtered_buffer_checker =
      FilteredTileChecker::create_uncompressed_with_grid_chunks<uint64_t>(
          elements_per_chunk, 4, 1);

  // Run the pipeline tests.
  check_run_pipeline_full(
      config,
      tp,
      tile,
      offsets_tile,
      pipeline,
      &tile_data_generator,
      filtered_buffer_checker);
}

TEST_CASE("Filter: Test pipeline modify filter var", "[filter][modify][var]") {
  // Create TileDB needed for running pipeline.
  Config config;
  ThreadPool tp(4);

  // Set-up test data.
  SimpleVariableTestData test_data{};
  auto&& [tile, offsets_tile] =
      test_data.tile_data_generator().create_writer_tiles();
  const auto& elements_per_chunk = test_data.elements_per_chunk();

  FilterPipeline pipeline;
  pipeline.add_filter(Add1InPlace(Datatype::UINT64));
  pipeline.add_filter(AddNInPlace(Datatype::UINT64));
  pipeline.add_filter(Add1InPlace(Datatype::UINT64));

  // Get non-existent filter instance
  auto* cksum = pipeline.get_filter<PseudoChecksumFilter>();
  CHECK(cksum == nullptr);

  // Modify +N filter
  auto* add_n = pipeline.get_filter<AddNInPlace>();
  CHECK(add_n != nullptr);
  add_n->set_increment(2);

  // Create expected filtered data checker.
  auto filtered_buffer_checker =
      FilteredTileChecker::create_uncompressed_with_grid_chunks<uint64_t>(
          elements_per_chunk, 4, 1);

  // Run the pipeline tests.
  check_run_pipeline_full(
      config,
      tp,
      tile,
      offsets_tile,
      pipeline,
      &test_data.tile_data_generator(),
      filtered_buffer_checker);
}

TEST_CASE("Filter: Test pipeline copy", "[filter][copy]") {
  // Create TileDB needed for running pipeline.
  Config config;
  ThreadPool tp(4);

  // Set-up test data.
  IncrementTileDataGenerator<uint64_t, Datatype::UINT64> tile_data_generator(
      100);
  auto&& [tile, offsets_tile] = tile_data_generator.create_writer_tiles();
  std::vector<uint64_t> elements_per_chunk{100};

  const uint64_t expected_checksum = 5350;

  FilterPipeline pipeline;
  pipeline.add_filter(Add1InPlace(Datatype::UINT64));
  pipeline.add_filter(AddNInPlace(Datatype::UINT64));
  pipeline.add_filter(Add1InPlace(Datatype::UINT64));
  pipeline.add_filter(PseudoChecksumFilter(Datatype::UINT64));

  // Modify +N filter
  auto* add_n = pipeline.get_filter<AddNInPlace>();
  CHECK(add_n != nullptr);
  add_n->set_increment(2);

  // Copy pipeline
  FilterPipeline pipeline_copy(pipeline);

  // Check +N filter cloned correctly
  auto* add_n_2 = pipeline_copy.get_filter<AddNInPlace>();
  CHECK(add_n_2 != add_n);
  CHECK(add_n_2 != nullptr);
  CHECK(add_n_2->increment() == 2);

  // Create filtered buffer checker.
  auto filtered_buffer_checker =
      FilteredTileChecker::create_uncompressed_with_grid_chunks<uint64_t>(
          elements_per_chunk, {{expected_checksum}}, 4, 1);

  // Run the pipeline tests.
  // Run the pipeline tests.
  check_run_pipeline_full(
      config,
      tp,
      tile,
      offsets_tile,
      pipeline,
      &tile_data_generator,
      filtered_buffer_checker);
}

TEST_CASE("Filter: Test random pipeline", "[filter][random]") {
  // Create TileDB needed for running pipeline.
  Config config;
  ThreadPool tp(4);

  // Create an encryption key.
  EncryptionKey encryption_key;
  REQUIRE(encryption_key
              .set_key(
                  EncryptionType::AES_256_GCM,
                  "abcdefghijklmnopqrstuvwxyz012345",
                  32)
              .ok());

  // List of potential filters to use. All of these filters can occur anywhere
  // in the pipeline.
  std::vector<std::function<tiledb::sm::Filter*(void)>> constructors = {
      []() { return tdb_new(Add1InPlace, Datatype::UINT64); },
      []() { return tdb_new(Add1OutOfPlace, Datatype::UINT64); },
      []() { return tdb_new(Add1IncludingMetadataFilter, Datatype::UINT64); },
      []() { return tdb_new(BitWidthReductionFilter, Datatype::UINT64); },
      []() { return tdb_new(BitshuffleFilter, Datatype::UINT64); },
      []() { return tdb_new(ByteshuffleFilter, Datatype::UINT64); },
      []() {
        return tdb_new(
            CompressionFilter,
            tiledb::sm::Compressor::BZIP2,
            -1,
            Datatype::UINT64);
      },
      []() { return tdb_new(PseudoChecksumFilter, Datatype::UINT64); },
      []() { return tdb_new(ChecksumMD5Filter, Datatype::UINT64); },
      []() { return tdb_new(ChecksumSHA256Filter, Datatype::UINT64); },
      [&encryption_key]() {
        return tdb_new(
            EncryptionAES256GCMFilter, encryption_key, Datatype::UINT64);
      },
  };

  // List of potential filters that must occur at the beginning of the
  // pipeline.
  std::vector<std::function<tiledb::sm::Filter*(void)>> constructors_first = {
      // Pos-delta would (correctly) return error after e.g. compression.
      []() { return tdb_new(PositiveDeltaFilter, Datatype::UINT64); }};

  // Create tile data generator.
  IncrementTileDataGenerator<uint64_t, Datatype::UINT64> tile_data_generator(
      100);
  for (int i = 0; i < 100; i++) {
    // Create fresh input tiles.
    auto&& [tile, offsets_tile] = tile_data_generator.create_writer_tiles();

    // Construct a random pipeline
    FilterPipeline pipeline;
    const unsigned max_num_filters = 6;
    std::random_device rd;
    auto pipeline_seed = rd();
    std::mt19937 gen(pipeline_seed);
    std::uniform_int_distribution<> rng_num_filters(0, max_num_filters),
        rng_bool(0, 1), rng_constructors(0, (int)(constructors.size() - 1)),
        rng_constructors_first(0, (int)(constructors_first.size() - 1));

    INFO("Random pipeline seed: " << pipeline_seed);

    auto num_filters = (unsigned)rng_num_filters(gen);
    for (unsigned j = 0; j < num_filters; j++) {
      if (j == 0 && rng_bool(gen) == 1) {
        auto idx = (unsigned)rng_constructors_first(gen);
        tiledb::sm::Filter* filter = constructors_first[idx]();
        pipeline.add_filter(*filter);
        tdb_delete(filter);
      } else {
        auto idx = (unsigned)rng_constructors(gen);
        tiledb::sm::Filter* filter = constructors[idx]();
        pipeline.add_filter(*filter);
        tdb_delete(filter);
      }
    }

    // Check the pipelines run forward and backward without error and return the
    // input data.
    // Run the pipeline tests.
    check_run_pipeline_roundtrip(
        config, tp, tile, offsets_tile, pipeline, &tile_data_generator);
  }
}
