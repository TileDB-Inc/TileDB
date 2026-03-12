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

#include <test/support/src/mem_helpers.h>
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
#include "filter_test_support.h"
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

TEST_CASE("Filter: Test empty pipeline", "[filter][empty-pipeline]") {
  // Create TileDB needed for running pipeline.
  Config config;
  ThreadPool tp(4);

  auto tracker = tiledb::test::create_test_memory_tracker();

  // Set-up test data.
  IncrementTileDataGenerator<uint64_t, Datatype::UINT64> tile_data_generator(
      100);
  auto&& [tile, offsets_tile] =
      tile_data_generator.create_writer_tiles(tracker);
  std::vector<uint64_t> elements_per_chunk{100};

  // Create pipeline.
  FilterPipeline pipeline;

  // Create expected filtered data checker.
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
      filtered_buffer_checker,
      tracker);
}

TEST_CASE(
    "Filter: Test empty pipeline on uint16 data", "[filter][empty-pipeline]") {
  // Create TileDB needed for running pipeline.
  Config config;
  ThreadPool tp(4);

  auto tracker = tiledb::test::create_test_memory_tracker();

  // Set-up test data.
  IncrementTileDataGenerator<uint16_t, Datatype::UINT16> tile_data_generator(
      100);
  auto&& [tile, offsets_tile] =
      tile_data_generator.create_writer_tiles(tracker);
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
      filtered_buffer_checker,
      tracker);
}

TEST_CASE(
    "Filter: Test empty pipeline var sized", "[filter][empty-pipeline][var]") {
  // Create TileDB needed for running pipeline.
  Config config;
  ThreadPool tp(4);

  auto tracker = tiledb::test::create_test_memory_tracker();

  // Set-up test data.
  SimpleVariableTestData test_data{};
  const auto& tile_data_generator = test_data.tile_data_generator();
  auto&& [tile, offsets_tile] =
      tile_data_generator.create_writer_tiles(tracker);
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
      filtered_buffer_checker,
      tracker);
}

TEST_CASE(
    "Filter: Test simple in-place pipeline", "[filter][simple-in-place]") {
  // Create TileDB needed for running pipeline.
  Config config;
  ThreadPool tp(4);

  auto tracker = tiledb::test::create_test_memory_tracker();

  // Set-up test data.
  IncrementTileDataGenerator<uint64_t, Datatype::UINT64> tile_data_generator(
      100);
  auto&& [tile, offsets_tile] =
      tile_data_generator.create_writer_tiles(tracker);
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
        filtered_buffer_checker,
        tracker);
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
        filtered_buffer_checker,
        tracker);
  }
}

TEST_CASE(
    "Filter: Test simple in-place pipeline var",
    "[filter][simple-in-place][var]") {
  // Create TileDB needed for running pipeline.
  Config config;
  ThreadPool tp(4);

  auto tracker = tiledb::test::create_test_memory_tracker();

  // Set-up test data.
  SimpleVariableTestData test_data{};
  const auto& tile_data_generator = test_data.tile_data_generator();
  auto&& [tile, offsets_tile] =
      tile_data_generator.create_writer_tiles(tracker);
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
        filtered_buffer_checker,
        tracker);
  }

  SECTION("- Multi-stage") {
    // Add a few more +1 filters and re-run.
    WhiteboxWriterTile::set_max_tile_chunk_size(80);
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
        filtered_buffer_checker,
        tracker);
  }
}

TEST_CASE(
    "Filter: Test simple out-of-place pipeline",
    "[filter][simple-out-of-place]") {
  // Create TileDB needed for running pipeline.
  Config config;
  ThreadPool tp(4);

  auto tracker = tiledb::test::create_test_memory_tracker();

  // Set-up test data.
  IncrementTileDataGenerator<uint64_t, Datatype::UINT64> tile_data_generator(
      100);
  auto&& [tile, offsets_tile] =
      tile_data_generator.create_writer_tiles(tracker);
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
        filtered_buffer_checker,
        tracker);
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
        filtered_buffer_checker,
        tracker);
  }
}

TEST_CASE(
    "Filter: Test simple out-of-place pipeline var",
    "[filter][simple-out-of-place][var]") {
  // Create TileDB needed for running pipeline.
  Config config;
  ThreadPool tp(4);

  auto tracker = tiledb::test::create_test_memory_tracker();

  // Set-up test data.
  SimpleVariableTestData test_data{};
  const auto& tile_data_generator = test_data.tile_data_generator();
  auto&& [tile, offsets_tile] =
      tile_data_generator.create_writer_tiles(tracker);
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
        filtered_buffer_checker,
        tracker);
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
        filtered_buffer_checker,
        tracker);
  }
}

TEST_CASE(
    "Filter: Test mixed in- and out-of-place pipeline",
    "[filter][in-out-place]") {
  // Create TileDB needed for running pipeline.
  Config config;
  ThreadPool tp(4);

  auto tracker = tiledb::test::create_test_memory_tracker();

  // Set-up test data.
  IncrementTileDataGenerator<uint64_t, Datatype::UINT64> tile_data_generator(
      100);
  auto&& [tile, offsets_tile] =
      tile_data_generator.create_writer_tiles(tracker);
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
      filtered_buffer_checker,
      tracker);
}

TEST_CASE(
    "Filter: Test mixed in- and out-of-place pipeline var",
    "[filter][in-out-place][var]") {
  // Create TileDB needed for running pipeline.
  Config config;
  ThreadPool tp(4);

  auto tracker = tiledb::test::create_test_memory_tracker();

  // Set-up test data.
  SimpleVariableTestData test_data{};
  const auto& tile_data_generator = test_data.tile_data_generator();
  auto&& [tile, offsets_tile] =
      tile_data_generator.create_writer_tiles(tracker);
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
      filtered_buffer_checker,
      tracker);
}

TEST_CASE("Filter: Test pseudo-checksum", "[filter][pseudo-checksum]") {
  // Create TileDB needed for running pipeline.
  Config config;
  ThreadPool tp(4);

  auto tracker = tiledb::test::create_test_memory_tracker();

  // Set-up test data.
  IncrementTileDataGenerator<uint64_t, Datatype::UINT64> tile_data_generator(
      100);
  auto&& [tile, offsets_tile] =
      tile_data_generator.create_writer_tiles(tracker);
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
        filtered_buffer_checker,
        tracker);
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
        filtered_buffer_checker,
        tracker);
  }
}

TEST_CASE(
    "Filter: Test pseudo-checksum var", "[filter][pseudo-checksum][var]") {
  // Create TileDB needed for running pipeline.
  Config config;
  ThreadPool tp(4);

  auto tracker = tiledb::test::create_test_memory_tracker();

  // Set-up test data.
  SimpleVariableTestData test_data{};
  const auto& tile_data_generator = test_data.tile_data_generator();
  auto&& [tile, offsets_tile] =
      tile_data_generator.create_writer_tiles(tracker);
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
        filtered_buffer_checker,
        tracker);
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
        filtered_buffer_checker,
        tracker);
  }
}

TEST_CASE("Filter: Test pipeline modify filter", "[filter][modify]") {
  // Create TileDB needed for running pipeline.
  Config config;
  ThreadPool tp(4);

  auto tracker = tiledb::test::create_test_memory_tracker();

  // Set-up test data.
  IncrementTileDataGenerator<uint64_t, Datatype::UINT64> tile_data_generator(
      100);
  auto&& [tile, offsets_tile] =
      tile_data_generator.create_writer_tiles(tracker);
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
      filtered_buffer_checker,
      tracker);
}

TEST_CASE("Filter: Test pipeline modify filter var", "[filter][modify][var]") {
  // Create TileDB needed for running pipeline.
  Config config;
  ThreadPool tp(4);

  auto tracker = tiledb::test::create_test_memory_tracker();

  // Set-up test data.
  SimpleVariableTestData test_data{};
  auto&& [tile, offsets_tile] =
      test_data.tile_data_generator().create_writer_tiles(tracker);
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
      filtered_buffer_checker,
      tracker);
}

TEST_CASE("Filter: Test pipeline copy", "[filter][copy]") {
  // Create TileDB needed for running pipeline.
  Config config;
  ThreadPool tp(4);

  auto tracker = tiledb::test::create_test_memory_tracker();

  // Set-up test data.
  IncrementTileDataGenerator<uint64_t, Datatype::UINT64> tile_data_generator(
      100);
  auto&& [tile, offsets_tile] =
      tile_data_generator.create_writer_tiles(tracker);
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
  check_run_pipeline_full(
      config,
      tp,
      tile,
      offsets_tile,
      pipeline,
      &tile_data_generator,
      filtered_buffer_checker,
      tracker);
}

TEST_CASE("Filter: Test random pipeline", "[filter][random]") {
  // Create TileDB needed for running pipeline.
  Config config;
  ThreadPool tp(4);

  auto tracker = tiledb::test::create_test_memory_tracker();

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

  auto doit = [&](const FilterPipeline& pipeline) {
    // Create tile data generator.
    IncrementTileDataGenerator<uint64_t, Datatype::UINT64> tile_data_generator(
        100);

    // Create fresh input tiles.
    auto&& [tile, offsets_tile] =
        tile_data_generator.create_writer_tiles(tracker);

    // Check the pipelines run forward and backward without error and return the
    // input data.
    // Run the pipeline tests.
    check_run_pipeline_roundtrip(
        config,
        tp,
        tile,
        offsets_tile,
        pipeline,
        &tile_data_generator,
        tracker);
  };

  SECTION("Random pipeline") {
    for (int i = 0; i < 100; i++) {
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

      doit(pipeline);
    }
  }

  SECTION("Example") {
    FilterPipeline pipeline;
    pipeline.add_filter(Add1IncludingMetadataFilter(Datatype::UINT64));
    pipeline.add_filter(Add1IncludingMetadataFilter(Datatype::UINT64));
    pipeline.add_filter(
        CompressionFilter(tiledb::sm::Compressor::BZIP2, -1, Datatype::UINT64));
    pipeline.add_filter(BitWidthReductionFilter(Datatype::UINT64));

    doit(pipeline);
  }
}

TEST_CASE("Filter: Test compression", "[filter][compression]") {
  // Create resources for running pipeline tests.
  Config config;
  ThreadPool tp(4);
  FilterPipeline pipeline;
  auto tracker = tiledb::test::create_test_memory_tracker();

  //  Set-up test data.
  IncrementTileDataGenerator<uint64_t, Datatype::UINT64> tile_data_generator(
      100);
  auto&& [tile, offsets_tile] =
      tile_data_generator.create_writer_tiles(tracker);

  SECTION("- Simple") {
    pipeline.add_filter(Add1InPlace(Datatype::UINT64));
    pipeline.add_filter(Add1OutOfPlace(Datatype::UINT64));
    pipeline.add_filter(
        CompressionFilter(tiledb::sm::Compressor::LZ4, 5, Datatype::UINT64));

    // Check the pipelines run forward and backward without error and returns
    // the input data.
    check_run_pipeline_roundtrip(
        config,
        tp,
        tile,
        offsets_tile,
        pipeline,
        &tile_data_generator,
        tracker);
  }

  SECTION("- With checksum stage") {
    pipeline.add_filter(PseudoChecksumFilter(Datatype::UINT64));
    pipeline.add_filter(
        CompressionFilter(tiledb::sm::Compressor::LZ4, 5, Datatype::UINT64));

    // Check the pipelines run forward and backward without error and returns
    // the input data.
    check_run_pipeline_roundtrip(
        config,
        tp,
        tile,
        offsets_tile,
        pipeline,
        &tile_data_generator,
        tracker);
  }

  SECTION("- With multiple stages") {
    pipeline.add_filter(Add1InPlace(Datatype::UINT64));
    pipeline.add_filter(PseudoChecksumFilter(Datatype::UINT64));
    pipeline.add_filter(Add1OutOfPlace(Datatype::UINT64));
    pipeline.add_filter(
        CompressionFilter(tiledb::sm::Compressor::LZ4, 5, Datatype::UINT64));

    // Check the pipelines run forward and backward without error and returns
    // the input data.
    check_run_pipeline_roundtrip(
        config,
        tp,
        tile,
        offsets_tile,
        pipeline,
        &tile_data_generator,
        tracker);
  }
}

TEST_CASE("Filter: Test compression var", "[filter][compression][var]") {
  // Create TileDB resources for running the filter pipeline.
  Config config;
  ThreadPool tp(4);
  auto tracker = tiledb::test::create_test_memory_tracker();

  // Set-up test data.
  SimpleVariableTestData test_data{};
  const auto& tile_data_generator = test_data.tile_data_generator();
  auto&& [tile, offsets_tile] =
      tile_data_generator.create_writer_tiles(tracker);

  FilterPipeline pipeline;

  SECTION("- Simple") {
    pipeline.add_filter(Add1InPlace(Datatype::UINT64));
    pipeline.add_filter(Add1OutOfPlace(Datatype::UINT64));
    pipeline.add_filter(
        CompressionFilter(tiledb::sm::Compressor::LZ4, 5, Datatype::UINT64));

    // Check the pipelines run forward and backward without error and returns
    // the input data.
    check_run_pipeline_roundtrip(
        config,
        tp,
        tile,
        offsets_tile,
        pipeline,
        &tile_data_generator,
        tracker);
  }

  SECTION("- With checksum stage") {
    pipeline.add_filter(PseudoChecksumFilter(Datatype::UINT64));
    pipeline.add_filter(
        CompressionFilter(tiledb::sm::Compressor::LZ4, 5, Datatype::UINT64));

    // Check the pipelines run forward and backward without error and returns
    // the input data.
    check_run_pipeline_roundtrip(
        config,
        tp,
        tile,
        offsets_tile,
        pipeline,
        &tile_data_generator,
        tracker);
  }

  SECTION("- With multiple stages") {
    pipeline.add_filter(Add1InPlace(Datatype::UINT64));
    pipeline.add_filter(PseudoChecksumFilter(Datatype::UINT64));
    pipeline.add_filter(Add1OutOfPlace(Datatype::UINT64));
    pipeline.add_filter(
        CompressionFilter(tiledb::sm::Compressor::LZ4, 5, Datatype::UINT64));

    // Check the pipelines run forward and backward without error and returns
    // the input data.
    check_run_pipeline_roundtrip(
        config,
        tp,
        tile,
        offsets_tile,
        pipeline,
        &tile_data_generator,
        tracker);
  }
}

TEST_CASE(
    "Filter: Test delta filter reinterpret_datatype validity",
    "[filter][delta]") {
  // Create TileDB needed for running pipeline.
  Config config;
  ThreadPool tp(4);

  auto tracker = tiledb::test::create_test_memory_tracker();

  constexpr Datatype input_datatype = Datatype::UINT8;

  // Set-up test data.
  IncrementTileDataGenerator<uint8_t, input_datatype> tile_data_generator(100);
  auto&& [tile, offsets_tile] =
      tile_data_generator.create_writer_tiles(tracker);
  std::vector<uint64_t> elements_per_chunk{100};

  auto reinterpret_datatype = GENERATE(
      Datatype::UINT8, Datatype::UINT16, Datatype::UINT32, Datatype::UINT64);

  DYNAMIC_SECTION(
      "input_datatype = " + datatype_str(input_datatype) +
      ", reinterpret_datatype = " + datatype_str(reinterpret_datatype)) {
    if (datatype_size(input_datatype) % datatype_size(reinterpret_datatype) ==
        0) {
      // There is an integral number of units of `reinterpret_datatype`,
      // should always work
      auto compressor = CompressionFilter(
          Compressor::DELTA, 1, input_datatype, reinterpret_datatype);

      FilterPipeline pipeline;
      pipeline.add_filter(compressor);
      check_run_pipeline_roundtrip(
          config,
          tp,
          tile,
          offsets_tile,
          pipeline,
          &tile_data_generator,
          tracker);
    } else {
      // there may be a partial instance of `reinterpret_datatype`
      auto compressor = CompressionFilter(
          Compressor::DELTA, 1, input_datatype, reinterpret_datatype);
      FilterPipeline pipeline;
      pipeline.add_filter(compressor);
      CHECK_THROWS(
          FilterPipeline::check_filter_types(pipeline, input_datatype, false));
      CHECK_THROWS(
          FilterPipeline::check_filter_types(pipeline, input_datatype, true));
    }
  }
}

TEST_CASE(
    "Filter: Test double delta filter reinterpret_datatype validity",
    "[filter][double-delta]") {
  // Create TileDB needed for running pipeline.
  Config config;
  ThreadPool tp(4);

  auto tracker = tiledb::test::create_test_memory_tracker();

  constexpr Datatype input_datatype = Datatype::UINT8;

  // Set-up test data.
  IncrementTileDataGenerator<uint8_t, input_datatype> tile_data_generator(100);
  auto&& [tile, offsets_tile] =
      tile_data_generator.create_writer_tiles(tracker);
  std::vector<uint64_t> elements_per_chunk{100};

  auto reinterpret_datatype = GENERATE(
      Datatype::UINT8, Datatype::UINT16, Datatype::UINT32, Datatype::UINT64);

  DYNAMIC_SECTION(
      "input_datatype = " + datatype_str(input_datatype) +
      ", reinterpret_datatype = " + datatype_str(reinterpret_datatype)) {
    if (datatype_size(input_datatype) % datatype_size(reinterpret_datatype) ==
        0) {
      // there is an integral number of units of `reinterpret_datatype`,
      // should always work
      auto compressor = CompressionFilter(
          Compressor::DOUBLE_DELTA, 1, input_datatype, reinterpret_datatype);

      FilterPipeline pipeline;
      pipeline.add_filter(compressor);
      check_run_pipeline_roundtrip(
          config,
          tp,
          tile,
          offsets_tile,
          pipeline,
          &tile_data_generator,
          tracker);
    } else {
      // there may be a partial instance of `reinterpret_datatype`
      auto compressor = CompressionFilter(
          Compressor::DOUBLE_DELTA, 1, input_datatype, reinterpret_datatype);
      FilterPipeline pipeline;
      pipeline.add_filter(compressor);
      CHECK_THROWS(
          FilterPipeline::check_filter_types(pipeline, input_datatype, false));
      CHECK_THROWS(
          FilterPipeline::check_filter_types(pipeline, input_datatype, true));
    }
  }
}

/**
 * See CORE-516.
 * This input is not very compressible so it requires the output size bound
 * of the gzip filter to be precise.
 *
 * Fun fact, this was found by property-based testing from Rust!
 */
TEST_CASE("Filter: test gzip buffer bound", "[filter-pipeline]") {
  FilterPipeline p;
  p.add_filter(CompressionFilter(Compressor::GZIP, 1, Datatype::CHAR));

  const int64_t data[] = {
      3551333333829840811,  2113732879076740746,  2375226389734203873,
      2589822604193314615,  1871988512415060700,  3508685021343897386,
      -3834071236965847027, -2772420577987242138, 3349967468562279937,
      -950663581787022603,  2416685487500793956,  -1625543128091485559,
      3511790309348225193,  2953796755454355056,  3080084306427493739,
      -2413749633453415301, -2067025021835691641, 3476642722086622100,
      2746835612002922331,  2093886906912784115,  -1313202034982337494,
      2867530497544228581,  3025396011351762164,  618381829843101957,
      -904167282816907157,  -345707307927147141,  -358537551863629019,
      1037673533721687268,  -3593846445154622770, 653629117987321041,
      1279930142389298988,  2807183951427911522,  3153116840996992498,
      1894148333477799108,  1823353273319592427,  1951555154576512133,
      2567768216855452278,  -244033327020389091,  -4056403705255489516,
      -289650668909136755,  -2334762777738668752, 508340505904257148,
      -2724427569769804759, -3789678418356781812, 1020000133012711284,
      512939886835237360,   -482751012983854713,  -2810608991241566220,
      3358693737494898932,  -663293640897843991,  -902569064150030825,
      -1155067619317322531, -2711186757140226014, -2184031176125064534,
      1659621097056179140,  -462948709407437990,  -823361642795276285,
      58855053410787337,    2202829385443066807,  510683027975813367,
      3447260444411807896,  1715431109371435092,  3022800671531845803,
      526563573776599272,   3106745164240633678,  534805805577963708,
      2758953995328355398,  -1932111230495085510, -2760784779674055162,
      307227600664451241,   961346485347644408,   -3027677281782137456,
      1281027862409484706,  1523026102172466504,  1839813957309729206,
      2142262559751682266,  1986996811016073904,  2074338234591914098,
      -1604862126708794032, 2799152394181758301,  -3551436365858034584,
      -4020794622375784174, 1516253083469223765,  75083510816023566,
      2296197111109376760,  -2066222769566435460, 1374365999476913534,
      -660005355576247775,  -3783254105379441990, 1594350860297321407,
      -2067504117891756718, -811625626583356365,  -3907817788720742468,
      -1766835179830812765, -441739759747853800,  -3108560232090670073,
      -3949311834027733816, 1563884028499159973,  -1363082684460179689,
      -1714643488372856208, 2363265775223202939,  -404287179138650187,
      993302523062230968,   1576197382129420051,  811652136123150930,
      -1830647739129971550, 473531375473819175,   -2874653331801736398,
      -597160993875014060,  -3252337357601618016, 261898249056498440,
      2854919239029342185,  539754456698334658,   -1855349342344155890,
      1349549187718559515,  -3018174935059975862, -44332871685287588,
      -3103783760532160791, 3410396565712125651,  -2930841300063153177,
      2615282377003817242,  2044505450880533162,  -2149106610689656432,
      -2867619322250304779, 3705087542699603834,  3536377331544656157,
      3412651335211777831,  -2494455829118260661, 3452496883902511391,
      -323559212825254219,  565307003605802481,   -1535749853527170503,
      3240397200613907919,  -3992108226671179760, -679971601911502873,
      832369473342433448,   3140381908884258175,  12664737336871968,
      -234968783691159983,  3418981371806301300,  -2150467420010427137,
      305081554626241300,   -3773443654935151608, 1940464567922311762,
      2348797831827459497,  780125146440424326,   2537995880288195620,
      -1146704079907578042, -4018128364806409884, -2350514813993082640,
      515745441442448160,   -1865769592301110227, 3596937275093132336,
      -3876581241296489719, 1577916435039288911,  2792562980332322558,
      -1777015034702580622, -4036684001936351469, 683464309561128837,
      2854437532102103056,  2357166384846589517,  -2267635574650555873,
      249950250364217105,   -2838581525101138747, 2570719487798157598,
      -419044542056955572,  3455393239989404826,  -1630327645659235870,
      -1597083911619319773, 2432379693032929124,  -800218617193389781,
      1359354308733791471,  26124494153866502,    1780359187568944079,
      -1762149172596370372, 1815269682165421393,  -3608330761822164237,
      -2815374998929538278, 2464153540643848683,  -2527200367656577674,
      2243499386972747375,  3689960011497677288,  1479695771581695295,
      2545509751515709642,  -3250222706966241897, 958793758948119005,
      212821181344423054,   3362086486602519953,  -1864633848045686788,
      1458790043254253689,  3691596458084670386,  -2357826576629495365,
      -456231160930973145,  2744196939784110832,  2482707654313482273,
      3150583500668368718,  1544366626017388432,  -1858716132550703192,
      -3370227949303351744, 398815715371732785,   2193697974678961550,
      961966006091529788,   -230162345275269610,  -2320892752178032861,
      3468146188424305100,  -3283460824123008304, -1593543314839761674,
      -2333557730667064247, -4228734732580657695, 3281110374397000444,
      -1004921167566506738, 3137154040722501711,  -1053556026590237927,
      588140952526885954,   -1368881323224580724, 3458104998903153788,
      805760177507436864,   -3384473804439978945, 2884404858043730595,
      -1243256059944438167, -3965915725178551953, 741490284626393610,
      2482272991182124815,  1193107799401277376,  -170468819504185354,
      -2511763400098566594, -1976454551661840169, -2252547043245120281,
      1604912201485677648,  1007034688569083209,  -2524114315585313653,
      757363908436789800,   3528101646392995567,  -676534416032295874,
      -2966691013692250045, -2695735518238098269, -3112586154507231212,
      -1594158427547047846, 3058634424097202095,  1122248793505892047,
      -395795028565803657,  -777243971224718758,  2047527682352632427,
      -3977666792853782769, 1665167782240621696,  -1951621096220151333,
      2751028556734582464,  -118548668467150983,  2659798413019350217,
      -3110823536519587824, -950294314018753994,  -1148374419609715227,
      2780658601876557627,  679722839613911250,   299730946487894869,
      -1474971244395215116, -1230255023835471737, -647900976383660325,
      -2769567250726571157, 2538813795003212853,  3072569852680180417,
      1906319217186811990,  -2191868943531905600, 3087083827945733805,
      -3458476253409016242, -3575848685792719471, -1952522683705343492,
      1446914740409986212,  -576273295440292399,  -2567071463279138044,
      -2667781909886031182, -3087514971815073273, 3440798517595901124,
      -404089003558519640,  3590274385027308664,  516986143608403125,
      -1862154834150724729, -3935506496699482981, 460277536363109393,
      -1685985721733431843, -1346480221974984924, 1974296001008720787,
      -2283507251203791643, 2734655215732832089,  -2120137758053195095,
      3210325845586960496,  -1096678583170932718, -3533614237608606279,
      -2042244433037424617, -2475858400202392690, 3150551954087965389,
      790541802615674769,   3629696542155391448,  1004426030795875278,
      3380540678916617468,  -201152865599088265,  2880401344097416129,
      3070130337306028835,  376766889982255966,   2568065775674221903,
      -2563845981378442875, 1601529930996546921,  894213496475718333,
      -2213754965173635320, -547981895221292851,  -2727877869161775824,
      1721816197120403740,  -443765280028063447,  1305342671769709575,
      3255964209256619698,  -3389352827094939458, -798230792098240236,
      1437022132620318717,  -1904374017637079578, 883526659061450309,
      3547619551348971716,  -3927079304909365835, -2020011519380037124,
      1578419273518987964,  1053024133502529953,  -1865316720001227964,
      -515064602133324703,  -289494662546858858,  1529466902959425439,
      615651985654650709,   -1558262506649938097, -1049350964376812596,
      -2703684396510765047, -3091782385597136521, -3772396399503434091,
      1515085832084430676,  -3146766947912201691, 2330783661287186668,
      -3917960380360795248, -2159186259743420491, 1870954426037717401,
      -534265400886609896,  -2071027778500530390, -2425151589788438628,
      -1304112541328113644, 847628211240324607,   -2750840091790435429,
      1188918567291918758,  2018484073395511601,  1770756268762582336,
      -2555647729794531831, 3196182669975658288,  338174787852541506,
      1990372879981318359,  -384588691338155717,  -1706334042457440304,
      -952623325262789513,  -375759025066517694,  -979157270719554794,
      1528336816716499103,  -572728592660403430,  -196950686214401437,
      2176226463057605205,  1290474956368606551,  -4170998260530153706,
      471596129978597388,   2453749825957688975,  -2338881430394776463,
      1906124579363489694,  155282490277369374,   154006590212114569,
      -1423025951162164979, -2386335177249157927, 1490012859228787481,
      621281128451898934,   3695818783307650427,  852639142390562266,
      -3379151410208825964, -44032404308493677,   2679343611289203464,
      967340869015049932,   566637340313870750,   -3762525737258903030,
      -1918908835563658969, 3102755408726586177,  -3365212461324151959,
      410940645707148049,   2432600790726188128,  -1322033310635157182,
      -83403869325757228,   -408816308167771512,  -508662817232625192,
      -3903038525460792685, -2523711390205685972, -1034702911482292527,
      2263673716272110184,  -2777227626480446180, 1235433950320824334,
      -2703627382999835273, -4218353720300570994, 1488370180842229811,
      186326891605532305,   -397326765410497714,  1724053072398676736,
      -966923704814848472,  -218148720459336177,  -1527181538939179590,
      85189598307268332,    -19471123034487298,   683379361899621230,
      -1485582697372625896, -2701220749266969623, -3219748106781018246,
      -1613468387761330667, 3695100497672305600,  3628368935114482388,
      -30063420377455213,   1581402035464068389,  863907871831940256,
      1687998553907988160,  -3263578964049498902, 251791813526818314,
      3182523059010105073,  -1216763293601467448, -2365290959658916103,
      -1147956870625239170, -881608636970940294,  -2630421808322872532,
      1287378779038706807,  -2916489190661910481, -1637471673290152877,
      612481259169135806,   -1420493764960063359, 436166904679704963,
      -3033642373885985500, -3785337116697055807, 1320581384494164254,
      -2132565160524372605, -37080584141896916,   -674891293383545746,
      495030310699365228,   -212751438685435433,  -3661838776359938005,
      1893776423968194671,  -1477240222593038537, 781903265034115388,
      -4032798018048881405, -1915707497327569192, 968728716966289910,
      -1719437235339127042, 1354057297494634219,  -1019588743280609289,
      1676412666363592225,  -1938402176290527910, 1146155153414385120,
      1816500939354167382,  -954742587922587210,  363662849589605214,
      -103743905714052756,  -1016088318341491446, 1196930683267292585,
      -2559020104389398763, -3199506363927049665, -3660981836384832594,
      -1003220984010073519, 358865068528461333,   3715005436715264727,
      1244143545842849923,  2582469783989075327,  2198086601449958425,
      3548592709005518877,  721189220498934175,   -3994623894591253199,
      1129880487144319951,  -3711599232772900476, 7208114994127379,
      1035855485899688989,  2420878509016211875,  -3697637195857046582,
      2199962226031667268,  3372370824034842645,  3170864239262959043,
      1662506495690223660,  3373536171597682330,  -2377440361999123810,
      -3634834971920777321, -2668108150724773778, -2824293350906502983,
      1265878921426085534,  -3822429357654080809, 596252399208760995,
      419266183575256680,   2171844436462598552,  2621627952683403222,
      -2258054849024212171, -2805600322600619867, -3333393707059599026,
      822592504998022935,   -2080847400293410617, 2723211236911567676,
      169706194260401753,   2874799703629958625,  -615902871509497545,
      2136376186855708059,  2444370855844988340,  -2526048063455965320,
      3523515260040418803,  3438256651494938912,  2603055865721573531,
      -1939752828896925921, -4124422548530581928, -1497596775451449695,
      -1787402485082551066, -607195732934385357,  2095864836000791276,
      -3904058622044836957, -2037925205414796515, -4017947749461857427,
      -2879074305846377187, 2101930170482888537,  -306348197819652063,
      -649269368969707520,  3130735110692914183,  -2437843792842291152,
      -2709091087083533105, -1106427406977495734, 3189628249647790960,
      2267249350700455761,  -3916995512250927344, 2005528805712201280,
      3614913425614939554,  -1265294558720277717, -3375883571344217280,
      -3291100891839533777, -1470624483809192921, -2415484492934596624,
      -3240302152195170328, -23879516755671995,   -3139475461909001381,
      -1213605449401231529, 2750784549367090551,  1910889252349132570,
      -1368571328140762616, 1003385638756015209,  -2819323259090855292,
      3124803299107573499,  -577885162688990860,  2299076131188836961,
      1445179444646631879,  3350892657376438997,  3329098805704965005,
      -1723367994008189777, -3232273336301380407, -1899866051452210897,
      -2819528615534610850, -1024305078627452288, 370519996149424899,
      -2120845372269406514, 2671782013286511290,  -1242855450193782757,
      2117034810996287977,  1569835738250415841,  -364683693854425550,
      1195680965978557891,  -2252563142755979693, -556455428340693609,
      3235513074634290246,  -2140075427755231684, 1937337341029802859,
      2122689201498925396,  -3437567893420880004, -1189257050809577588,
      -2018971987402260764, 2749348758969569768,  -509591914112928033,
      -1717720984747012544, 2325359965967764737,  -2162330381474866075,
      2635818812053781428,  1880393202560326672,  1832384782230081974,
      2665871821079909038,  -3376334364314787761, 780342158387092018,
      2295468847349557157,  -329979628004761332,  -2773688810149150835,
      -4015559974536464922, -3970319717831049795, 947407776983547972,
      1116402793785948271,  2557512355973689354,  1180759379138644856,
      251644463254006843,   -3128395476766434294, 3291485386087857742,
      -2185279695226848396, 559722933936826169,   -1702704789700411671,
      3372601624229644060,  3634745028300616949,  3431454964777756677,
      -1666680187584217397, -726530350038857890,  -521704561752271527,
      -2089685555872691687, -2758896730252078358, -2054395972231782889,
      1914638074389884704,  -4155439691173353120, 3112001740339697911,
      -1845585005294850894, -2703026590231504392, -3592050930643504494,
      1109145295504303973,  -1051715449134995603, -318023060405477909,
      2197141443503297546,  582646192407957714,   694541816122212233,
      3230275128849243884,  1885413673216564855,  -1990557286205455914,
      -1446870833325453223, 1970195990828611181,  -1401390681596925990,
      1421780088604843363,  -722790469509789821,  2495453766829118693,
      1325277847058139865,  -2166053512662976097, -685968470740250224,
      -2963105873980358877, 2680964938333381530,  -1228746978806146296,
      -4041350977112908992, 686542494999574451,   -2207683356009339185,
      1462340294344095240,  3313363517791596759,  -3047286155857197342,
      1556466396732191036,  -237782717893362911,  2792395204994335502,
      -594469630081313662,  2013867125894195101,  1638873445777253853,
      -812243766433190095,  2851452146002599006,  -3419127489326287366,
      261291614396499731,   2818417410066856162,  1788015655690066323,
      1373343910729136613,  1967851653429980499,  -3229383029867817397,
      1330740522702265818,  3663025534139733986,  -231580996946524162,
      -374402565164480819,  -2854590742893801006, -3807390304728303483,
      3389625472586873991,  -2910312491691330836, 2207914241425183918,
      1162239005533892134,  -3679137897086875961, -1073329343230312755,
      3527249970882109218,  1163628768044036740,  798847702896890113,
      1891191977469462465,  444594133213675871,   2533483918586708889,
      -1580776836517521860, -2040906100311077624, 1053794161780247125,
      2729754238829168430,  2014221955343616523,  -4127226387916909174,
      -2915634232198126333, 377425248199264717,   -78218672118423240,
      -4207655723246586089, -4071339329872009810, -2250884107538266183,
      -1078246908500168069, 2003849510944416678,  2136272351933896519,
      -4159833088821671954, -2385412684841034621, -1242877254511591472,
      2259981268151137691,  136989996561450500,   276365211545931952,
      -2510624582115536932, -2978567944806741995, 107727376388407793,
      -1357256601433086912, -1307708136588316215, -239122237597857409,
      -3410898958096035624, 2421116877770617833,  -4225329690190497214,
      -2969251543022783801, -2222398637745751923, -155802979385209128,
      1757015924284457551,  2311872633632893653,  -2405738475083571679,
      -3211508543372331105, 696013745434329752,   -263021573700326885,
      1657834919043780742,  1091850946548868906,  3453013209124025606,
      -3083493346580731815, 12876555424844598,    2336333552678953512,
      -512500211120187151,  3295548640327565804,  -913862699897795704,
      670115649546806292,   -1107640093351404041, -3585443491985229861,
      -3018122907774643117, -2297872422482984226, -1931601333326477454,
      2780022376408308168,  47087987500938142,    -65838230489667534,
      2213174902968529605,  -2203735266875163901, 1844970466674691296,
      2007760965391377125,  1088883162003994364,  -3974968407435216757,
      116709567716427179,   129365973818347926,   -3482899250117959444,
      392186464851182913,   -1598392019025610461, 623093837071753655,
      3621981579470917911,  1273192292035179717,  2970688107157360202,
      -1356428016253282476, -4145579084385059650, 2121942725507403684,
      2375947831891966946,  -3371969165922611601, 2200016228925060770,
      2651991050635204175,  1385265471666067782,  2847348140990269081,
      2829381843462594853,  3304507623013156157,  -4056280590684703851,
      152550940291178271,   321623328011537648,   3138566859389736796,
      -1330134928563619719, 3607987699410530105,  -1272780193935063274,
      -661599718458202509,  -292605008981814148,  2934496349358199523,
      -28033995043026932,   2792614815613431672,  -3757478730139821657,
      -502552973053368829,  -3707273691500918225, -1057295020407475965,
      2011954979808607597,  2317361746257065887,  -2596556583818666183,
      -3176823515181622916, -2829408505919631515, -3284237518802468169,
      -161264495093855770,  30441197500084472,    -1087313142578237461,
      -2731272192176956429, -1377545828829493171, -1104239139142292376,
      3554889403907885312,  -731327984624094805,  -357832034487734315,
      2838500760797476432,  1912397922748065456,  -1925205243571764499,
      840344345649441142,   -3547201872785174626, 1087765669966883923,
      -1894379835152164115, -2672887303281589690, 1286995749362812811,
      -4018649710259000191, 104527154626448205,   -1327065454949489388,
      585066633740287957,   -209327950310126332,  -4216604032157514556,
      -3913779181094646672, -2274206209232978784, 3560190533859672727,
      -1187268389756043750, -289353228585215697,  1572837074295355260,
      -3188654178760789128, 2913458245227119223,  3379112917832639929,
      -77862552986985360,   2487391891720980705,  2181665053301125789,
      -2122823117456283693, -1096959115870491350, -3876689945587905625,
      -1665494319921008673, 2374113258442580201,  -1029354734694331369,
      -1028959335437041016, -2757011817580520863, -2398952198439254450,
      1992201199396171909,  -114384337818850938,  1484400156840423855,
      3042347344313557605,  -2006976408297283030, 3707026708956813994,
      1386548981389114737,  -585721104891575841,  -1258215244763578704,
      -380115989817743064,  67389999001105976,    -577932080822132435,
      841471858433675249,   -2663041785777627275, 2074322710303098845,
      3701900510497946008,  1910554285249466084,  2432232446926606912,
      -1968183769775695834, -3789696593343261480, -1255651834966969704,
      2419494998313690276,  -285678262209470777,  -3143565512700249700,
      -907933918631726193,  2588236529124754616,  2154405316784864091,
      484222893299096448,   -993047787302143540,  -2979694428985101799,
      2097944431617358176,  2019504786210500905,  1318080169138233546,
      -1791630603378941306, -4009258686375424821, 2863596555512253514,
      1118169212512524501,  -3635920922439297698, -903729476531059417,
      -2661042874299599013, -717134693002979942,  2308532345327302075,
      -2008526083534498577, 2153738839614595309,  -521675695098710884,
      2206901546169384528,  -3915552703008336754, 2235084427259946811,
      1219812592327993626,  -1548459023746638914, 1341625737594211808,
      329199718003784216,   1986720000372069511,  -1628847720106385568,
      -3607963041382091351, -636112259472898202,  -1435082788655334046,
      1745509347259568429,  2821117209440686489,  -4012112060829434236,
      -1952892421552092739, -661741888610579499,  1929657489695868129,
      -1847633989572811620, -3860805527993812381, -2377170587512170689,
      1643266262865204633,  1300092335818162404,  -3315253190667191332,
      -363622553857374752,  -816208982571528243,  3072208129808103441,
      434590205734520243,   2482598257065622149,  3603814294366340897,
      -1190984713757447254, -3035281054263282019, 2473535768358741672,
      -3440197577989065829, 2966189860431166373,  -381183424587579839,
      2362526046718015165,  -3442872068761962816, 3712588029535619679,
      -2800668657546957283, 1570360595494489920,  -2863036639871903151,
      1035181900223094464,  -2383031501310489001, -2715535366564087029,
      -697861248631112835,  -1634448290918112692, -89104413726176219,
      2807579061280713356,  -4155590260011243219, -3087104180268820724,
      1844123662866508701,  643078497093072324,   -3942353327822987370,
      2942812829675669585,  -2834410507484876849, -4083093138930178493,
      3673707765974849576,  -1522010097630995173, 1249306330706893464,
      -2685350632028767549, 85218649948366245,    -31356093369969993,
      331258322145968503,   -3818622197315547498, 632046048020929041,
      384361871976156819,   -191131505016922123,  -2519755382256503560,
      57367736104329804,    -550004202215627258,  -2355544122234839715,
      -342452680181469978,  1625487376582275736,  507265243436019738,
      -451473930303893381,  2357811078144829331,  944308919576472249,
      2029739152140714968,  3344266469469071546,  1852113977238986775,
      656578519749930157,   25605678680476035,    2001789302284030372,
      1526510419960993193,  4717961244848484,     -3944011619188444499,
      -3507685050699588612, 3204281574693641874,  3283556160461775455,
      -1032429004508429045, 2093613453667000303,  -1658843175251475850,
      1961450222290886695,  689118226852389521,   -2525207916155820045,
      613001903973711803,   603330273951112985,   88759263955276282,
      3334933359759552441,  -3624397256058615884, -4199293974509589773,
      3592235428744176135,  1317789811332587839,  -2111143592656208144,
      -324671670153065338,  -3479602636519597529, -3099168121791224133,
      -2902707864862631547, 3718593477614237554,  -2587629581970276300,
      -4177387484549466281, 1642128440557092808,  -2350135301262593021,
      -2151537792539864570, -282768338791577177,  2300392217917577009,
      649163804673425660,   -474481563638379946,  -36854571356272297,
      -3659381408073630609, 1783281371062943837,  2973073955973390718,
      3481689158469185801,  798112524327122341,   1328138009597972888,
      2003164140800482822,  -2200000836794049340, 2547310312245482757,
      29940068353821849,    2876019641414033738,  1517987117010164738,
      3188844186701201805,  2168467177753836817,  -541378950242701772,
      -2312948636481841450, 2302488346877130873,  117652848561376448,
      -4113126423410662334, 1916228811594080392,  2553838016183225106,
      3107588728834524904,  3593918381489380872,  460652443592616957,
      -1703533133111738627, 1918194168124394544,  -591172557440556587,
      -4158269184445269576, -38385669354855368,   134482829852926193,
      -4154304121442269655, 2196097169994657093,  2814841621720486503,
      -3977655109582025072, -1202225092637756433, -3629779891721298105,
      -727146749469943758,  1270272714890891373,  1209065681880930084,
      -1603948547441047824, 419116496934392465,   3084524197992017731,
      3704975436663689189,  2781763399116899935,  2185712296734577702,
      -3138886558703828545, 2665563347268658156,  485465604836007088,
      133243140265565030,   3673457527043067711,  -3511935826869146043,
      -1638994931988241798, 2433848566636881585,  -4215258201893806744,
      2648748446162841832,  -3372983789230541138, -1934438567435948293,
      -750739055441834090,  -3708431286488135621, 1971325263417582005,
      -1145889835904395930, -3982222470324111415, -3519108279076563929,
      2900554789521563486,  724408039324249082,   -2755340344819562182,
      911870923575446692,   -2252147731812414360, -3639445096256624566,
      -3030608678800998325, -2544797200236199585, -2695499245948836192,
      -3159086601157769859, 2336747908402981845,  584893063194318289,
      1401657458614206491,  1497564754007694678,  2929118979324346958,
      -2450250091841286470, -843472551664814039,  -985758153339127969,
      3216703578155217922,  -1023750130653436051, -2134162074983328812,
      -2225950052864762732, 124789799004388321,   1010996567950961434,
      1368723146289817365,  1992712630526624505,  -1451584403260556149,
      -123033405305257062,  1350150321608664936,  1240418675514272812,
      -2826999826279852788, 1732436482838788769,  -3759747933806990380,
      -1619099939004327612, -2392015962745489037, 3170098536967012018,
      -580612507734356610,  -3149829401017899527, -634586244209576702,
      -902086934232538172,  2393845038559575748,  -1176946215483782675,
      2699647294333230192,  883912468736182616,   -475831841245700693,
      -855232778171122,     2902639464319596925,  -1581617719894108169,
      2511948680865342989,  3692704446743491453,  3102230211810495107,
      -2686523080586650664, 2218310230143283554,  1806535903390267132,
      -1451715143785599014, 3409662295157324151,  2492620515320003309,
      714604940971766374,   -525616645008269400,  -316621888631124036,
      1755430724750404898,  -3052470565575492323, 2941941367595196157,
      -3402523580838826775, 1919814124740022691,  -3737396488538633268,
      31361887581538159,    1672919518774857947,  826306482229789725,
      -1102197142222999047, -1348243221129900146, 3701233994763920252,
      -2763799755278445221, 2410135699389016062,  2329198438008959580,
      1737972559391378503,  -1542323409607916042, -795718801403634112,
      -701284603552793870,  2242053118848845739,  203765847452378896,
      -2435805142389750183, 816283279276110304,   2828288189831637157,
      -2386089154650252188, -3341378857963304839, -3793075825631772307,
      -2996458676594596079, 1014204367648481326,  -780004675528448784,
      691336432319950859,   -1386415541082866998, 14264538570649942,
      -2076675601826474774, -3206986759593191031, 901988680230883445,
      2200094096422139016,  2378637633147079738,  612355883843346805,
      3487008983203222394,  2024418917590182518,  -1369709162850456953,
      -2365058702272884894, 947950097315520310,   -2433357321852582796,
      2719257737627444237,  -2102636111317797177, -1505859396868517589,
      919457154155976193,   -798680204811919270,  3438577376779498968,
      -2542976262670658115, 3142417476204281369,  577085939836536279,
      1318045932022729112,  3352591355449199100,  1021588911084670371,
      321339554297002537,   980819781558777317,   1666958075721080611,
      -1360058524512777356, 2959200571487202979,  2867658590824022842,
      -3897766682884295082, -3342501205722553388, -1705283707010186366,
      -776788462350900607,  -3404645821997940061, -1162205005696447055,
      106096490982930856,   -1145435996332569160, -3115518202409561362,
      2010360758572286950,  535256315568298305,   762007833775371334,
      -4151116413164687448, 2884123478579465244,  3222638437545925014,
      -3004914127133215632, 1751424055485202040,  -532487333577698059,
      -921716271405587461,  91058716086001986,    3320883797312500255,
      624786876818003338,   -2919355621469878577, 906252544668305797,
      -3709296583439934662, 436967185758831060,   -2891191097080024259,
      -1022975933487558134, 2579702150146356527,  413940475922732696,
      3024124338751881479,  2033179084408278716,  -3766217116809193538,
      2820384784430493484,  2835630614863674420,  -1797861383468239888,
      -2903652295397170807, -2259364769332691836, 129795531114846332,
      -2514590288037739874, -3911251768086729746, -2395496768718160517,
      638536232490369962,   -102964383062033826,  2981042272541962372,
      1430120813583973904,  2312205470895455929,  -3773625537133749760,
      -33670412601852234,   -1785245334055989490, -1382856200733887159,
      2448785367475980748,  -802865507376596499,  -1252797237373590231,
      2519022548687969004,  1598297304951442494,  -1425689721680085067,
      -3415603517755741138, 833561690600576137,   -2515820297475314417,
      -3551309697128224458, 1799661504102637593,  -4174003989465767710,
      -139519215491423920,  -2643722730271937276, 2095719584647580145,
      -2036900041018297068, -3421766647612364982, -3798874108039195998,
      -3010427324584561711, 398374880407342307,   -2778761038358095266,
      -4114691732810318303, -3339227044368650827, -3070706531698473744,
      -2733409691917656875, 2959989913879544,     927081800140931509,
      1497048882061464898,  2268391028613986678,  -3357027355232632133,
      1451093344649879847,  -1300142009307531712, 2363271891757579498,
      -3242307205739022660, -526942394184936743,  2711954011353604517,
      3299497103960473712,  -2696162668355698322, -2305867825130849759,
      -3963490508205415370, -2451085861866198342, 387620247137379817,
      -1981371583504960734, -435604048906518234,  -2992764778388551072,
      -3374631727872368700, -454974806722800439,  3630176472477071840,
      -2390147536456871136, -3112922701582628688, 1862373840992156177,
      -2304550158918241542, 406235234416903861,   -179787336290998645,
      1347738818850788145,  -4118395336977265384, -2573039033899612635,
      461726658895385291,   2058036020639940591,  -3617942015944916597,
      -1075508451820817617, 1365258174250146813,  3182771885927326158,
      1386156337940032138,  -441519455112845698,  -2461259667016762790,
      -403502178066382241,  1292705746300053380,  -34265791574433397,
      2908111897796510070,  -1432679850410296293, -3293581683630667160,
      1238824596119199015,  3297495331678053177,  1106849800707319014,
      1333249057404939109,  1479743623868365526,  1611008471229370525,
      2673468981520379384,  -3887584716140674654, 343686673471983635,
      -3654340426371576199, 1660987641148044058,  -1591610546082143576,
      -469034799995384073,  357546191126892825,   3367753179841577949,
      -3099436717006441077, 839359939596505983,   -4172341929055996958,
      928218115776387116,   1850130331849595576,  1466663718316475588,
      3598892852336484292,  -4083042826600950286, 211899229936739035,
      -446633970873898557,  -2066324697014117624, -3886261694126289510,
      -3330620059311601003, 731595638504353988,   -914292098869174333,
      2006426032509639531,  -1340204203257747994, 199115433960373192,
      -1448336114027863046, 2125795462911408028,  -36377100561254049,
      -1441910514293533972, 3083646225439140839,  3700728544379748616,
      -2900610047145420168, -2762459858074260201, -2985918541824589235,
      300835901893937436,   946190777458193232,   851307384302060811,
      2068839413825743758,  -2026052374221806229, 917966233693755214,
      1129192986575667535,  1782054658563553887,  -15679251713942684,
      -2639519876173483250, -1051614482879979600, 1482697161201891814,
      -535656071072296782,  -1709108702994161467, 374445360090768061,
      947706710805773949,   2950384750652987448,  3086517634222137151,
      3245682399514391114,  2561299281242463069,  -310926134320357910,
      -3523451012312040865, -802107411736840745,  1637286312411538920,
      2752302703883358546,  -1181920840953453653, 3063913649007973520,
      1779036748762397595,  -3583146300338349782, -1355864185343123680,
      3426124474113617878,  -536106619165605421,  -701817663516727599,
      -3302053175602497083, -672197329226323532,  -2956760386448823322,
      -2542689095379187431, -375025234652231555,  762707076642322410,
      -2897620305809155576, -3516438215522152519, 3550458727112391575,
      777082442353767506,   216789922648880497,   -1076387095601835876,
      381306586859937770,   -2841255631462136096, 3330803101708769674,
      -1790525281617474702, -4087446625566127416, -2707743337799299739,
      -884753029885457692,  -2791340472428741491, 993060394771909394,
      -4029717012610707361, -1249539990793290035, -45877422604225362,
      3612225087896041624,  -662837893995418124,  1832981567518615818,
      -2014068669069994373, -411821391929675716,  2657634921569476513,
      -949187163346952196,  -2682693430466937889, -4228037646830360101,
      -361825383771057802,  -3000128283573296611, -2086596996917827698,
      -1902777448340969164, -2518214315357318101, -3887580066660397103,
      -3757807455995304711, -3497162131826619360, 1827771389693598550,
      1522304245440027468,  -1824167631220555576, -800656936140866633,
      3683777809750374964,  -3036989904555341984, -4014670420371256728,
      -912269688375413599,  1282397638685861158,  2694934518055044093,
      2720723816605410868,  -87514085045208634,   1150280990701877420,
      3078542979416088655,  -933368074212825572,  -2524627460671873516,
      -531139168022780665,  1802420756730867194,  -1407753122949541548,
      -4048410607709623008, -3311675160857692025, 310219886642235976,
      -3526699359947986348, -820418779717474234,  523535012135798831,
      -3640711111573743480, 2774277124505758781,  -2357440635833590053,
      2111018657176825573,  1400485524930872932,  936653969044557783,
      2357835599170199278,  -3777071870139947523, -1484226374059774303,
      -657851906158312777,  -949982564664947235,  1535430098233034579,
      621993115718684478,   -4127410769465857511, -3482939302969197224,
      -769082827283390584,  1209882130322312387,  -41134724416606758,
      -573234900584355979,  1385686450149025516,  256215514650554646,
      -2424072741370475273, -1923942369663732622, 2210402634994339113,
      2209641155686612746,  -3285404710837739488, 57392489273123277,
      1111875261860769629,  1953137778488366090,  -3301171332967251117,
      -2407692007569073240, 658005992646717528,   355311125802914253,
      -1349125920330259626, -4126525660770687564, 87562635503608959,
      -1026377801199191257, 2618517954300332846,  -1842412902367668334,
      -1134357186016505559, -305095472580001631,  -2392117278916197055,
      3333291390049271195,  1177390054535136786,  -3913768011068276055,
      -719822825760664062,  -355785763098676166,  1108425294835355675,
      2113347810617555562,  1803509988461630208,  -3041503408537870023,
      -1091776582655076,    3692191519530464020,  1883103712852173657,
      -4050979840889897064, -533271815156012502,  -510870508619022626,
      533133146386965767,   2607749395670177306,  -716228023120387513,
      2163532779812192740,  -4111432969169872665, -2627656790019525476,
      -250304092889835933,  2439051730604563586,  379613427939826711,
      2301830364711088343,  2443825083345431791,  -3755856510140824219,
      -1201126489396096444, -2558304105019657805, -68955590526852485,
      47524565297141924,    2604022416416842238,  997265969052064842,
      -3596442503458478543, -132410081205151412,  2453683358153767166,
      -2412491647356393855, 283440860474744529,   947417114534513075,
      1189769339235435156,  1032377078105332948,  -3296684322180832132,
      -2237861031829895184, -1535425291235041633, 3449945480633762035,
      -3987380231655304189, 2479561363224475636,  -2214303152830510914,
      -1104121658046217244, 1368837420658603867,  3536557703590547687,
      -2207314188895453518, 1539169144815478782,  3008767724692082529,
      -4144054305371482092, 3043086158691324925,  -1991080887170721068,
      -3302849154440236211, -3472520680582249372, -1987721498683709733,
      73687753937365249,    3029588930533929834,  -3428798990880892273,
      -730612313229098969,  -3932135369543042514, -200218096847216510,
      192546379280833526,   80639202558060738,    2311289688081456749,
      -2898373159887901367, 3347232507017807629,  -3897829392722991012,
      -3833610562124908187, -2234821862463496326, -291897315585009049,
      -174264799076167802,  -2997721415077308318, -657589103505323583,
      -2716144067484434164, 694387804494767751,   3629217089088366166,
      -2836751774922232744, -3593874266457703192, 2748846939163422472,
      -359661279250497225,  438714190494073618,   1747793622594754124,
      -673939360436453094,  -1925819018776560304, 2544564889660564438,
      -2644822401160999223, 205494359625325714,   2721885496445351693,
      1024176495642194114,  1791002473951031664,  -4214721823375910881,
      -2962836349056376740, -1713681757404173327, 2521990142242358685,
      3468413296885055015,  1276946321736461203,  -1673716407542488237,
      795780942694249619,   2643537423440619856,  1425931474160475732,
      2319076371067235373,  1681252794252886343,  -2367462613960122483,
      -1587098922780789572, 3640262700435143198,  2550612292706152800,
      -1144219364465356671, -2478643018440393254, -2995666452942671904,
      -252763494664026717,  -293872252400927663,  -3833155479559170976,
      703105223947357668,   -4164685504632127281, -24491732014982869,
      -825853953323407096,  181413234438753516,   1456614774166394418,
      -3384617553917438008, 3197725283040753208,  3153897280944255220,
      -1930768462864210040, -3899777895148556978, 1591628472648500541,
      -2668071119540845405, -3771913032202455862, 829314892135207117,
      3080055729558499661,  3092325107119431615,  -375664552394308759,
      -2509804802527112687, -883827733012107061,  2292050198073567686,
      -923448102017636790,  -2840810054492383673, 3150349616644532555,
      2411684044050898262,  59171624162280049,    -266192860902410746,
      -2956278325787870872, -3729732260893474440, -2055454908270712343,
      1230494892772523860,  2386638256024693247,  -2185625198992412046,
      2504580943969600080,  2810426720615830800,  -2735791231289001817,
      -1658674227867300593, 2094300051917831737,  1587977667297603824,
      -2122142285430823056, -1813519373660115579, -4215186250206610732,
      -148936645726466337,  -673273759439364388,  -2464345390713987253,
      3264394601214668269,  -1306628568802921299, -1163886753664543907,
      -672111813219066040,  1419564467219630414,  300557028243628444,
      2526917900978646539,  -1360009295192051860, -3975804434101207360,
      -936485380970680135,  3278518529608296048,  3626015175335043024,
      -334228225400801247,  -1362543890316805308, 1302681849033558075,
      642682784876902200,   1586344700102422497,  -2565746170404729852,
      -1836961132883814079, 1389845628064865777,  -452252846214724421,
      -2774413759066256316, -3311479296957937124, -981347392095733023,
      -3175164014326328573, 2451921805139488793,  2229574648690993228,
      -1349124367541496544, -4080302479285652262, -3275616084696554418,
      -1896716718970100567, 644477106066565073,   750301998030260870,
      1061024925167750089,  -2339945549326626483, 3142775653374880864,
      -2067020385360391249, -3822713684493603596, -380047079466088552,
      916754523415168687,   -869954802186806922,  2051428488619705501,
      -2024036770976075882, -2905080482026797718, -2795170042374027053,
      1696476622182306593,  1078284458890733045,  -1669242173619020060,
      -3026225597109005719, -3583447246176661805, 2259844780188322263,
      864184099796578507,   -572835186543341511,  595596869705398528,
      -3438255054031242218, -3007539927808106615, -967190812276727897,
      -529728382167765667,  2178977711908700533,  -1421599808699631033,
      3145528369680091011,  2183965765827691920,  2505617957115417873,
      2348302410396945386,  1975765372345029676,  -2303806587099973399,
      1027448950897936545,  -1869226038794082476, 1247696284862876650,
      1966601114771596475,  -3437052710647196184, -1494830000855437603,
      -1509149768163081346, 2032895940011099386,  -3985337935308652365,
      -41538394147412205,   1904290664813440194,  1190142297518752509,
      1616410678728544791,  -3799992316530789886, 1360853270382712095,
      -1130609468063649162, 2941063573104401605,  -1786232343065207482,
      -2446366352776353526, -300160419686132030,  1507523719309751125,
      1086007779386165948,  -1070709394854854378, 2429482261011822915,
      579367259710893577,   1273864743144389006,  -3402682692216442175,
      -2075694399091773200, -1413344271007677213, 3065358954732498771,
      -2635588206924378437, -1836098598276961597, -140123763322870260,
      1220484763830923459,  3121060441317100504,  -809536429647327348,
      2732061438265059830,  -3879767192656225532, -3516964551167239171,
      -3195771313371915723, 2579571448994452727,  585183544757682704,
      3124311361928113507,  3067246114750051713,  1922203335874097108,
      -2979685677556885726, -3424752633341574465, -3520790645066446525,
      -9012027295058746,    2126323991912875219,  -3869260333491651544,
      2722957766567797203,  -4223008737165977438, 1940674452665039539,
      -1834681932271461416, -1270193967053250227, -1432787772591398582,
      -2243275492625065033, -1991962956572130756, -3120712435985388850,
      2025637878757479681,  3143302847286474756,  3016215631768749704,
      -357596814567496014,  -1745954674562838739, 3446166135545735356,
      48007229545337210,    -534192930794628378,  -602203949071744013,
      -1731680139348134623, 1585925946097046944,  -4040890793698020950,
      3675730136625603053,  2050018440709939536,  -1266595626326655785,
      640272953864438896,   -2089458660603742207, -1615606556905992354,
      -47588546353661073,   66577407113901410,    -269321323212953796,
      -1424555971371991804, 3687900044384564639,  -2075412573223050217,
      1708956832477831101,  1162131072596063377,  1495621887192378374,
      2954950422691379236,  -399231022267648736,  619849814368845978,
      3164697909351234895,  -3918558026563160791, -1955743964482962039,
      662027490394501660,   -2681185899921228575, -4113798524461384865,
      -3373040159767659788, -3564167049305429897, -3784674865255201527,
      -2742515927743937446, -983515484418844440,  1696297166848501345,
      1796138855344201125,  1720247066080370455,  3137046017878324977,
      -215302800952054424,  -3628607216421056525, -2453249352725216000,
      835111876989586263,   -66861602439467270,   -602880929942248147,
      2173863341365038657,  1774079588193439706,  -2595706405445802094,
      694544482358677179,   -3874481977995393779, 276543902880124873,
      3706890312280835260,  851979336293452000,   -1865166087420730084,
      -382516418655133098,  -526031183133980604,  -3485506699582380300,
      -1201103486438813697, -4154929383333907032, -2749989327094080971,
      3545854022782561744,  -2561352998080174542, -2666746701262017088,
      3031595918803660173,  3080712845334154686,  3676705896382554293,
      2196644944835362880,  -1315070505784453199, -614091045935804455,
      2986296227784769416,  -2613449325203155660, -3386875239537382933,
      -285896851637805332,  -2168930040389096135, -2307890918565841978,
      2402546372761367979,  2157711451348553358,  -1599246431215118188,
      -1974265256059469453, 551545103908000386,   3719153001002120126,
      1516754299478246144,  -2663628256129802913, 2857255190195690558,
      819214415739220882,   -1860925309029287746, -987056652048812056,
      3290224932648865520,  -925099432185804514,  -3492917650304779352,
      1513590478313324885,  -2982282949306455078, -2378855746440800050,
      1546019605316120990,  3347128395463564038,  2053123415905633810,
      -4042487677144873070, -2548806892906210925, 3530339844579093725,
      -1052448257305241254, -4082995347924680273, 823151769931851530,
      3168735471663267716,  1564805578803351865,  -273680851305432928,
      1819857561453412588,  -1906305547785499222, -3835895033168580213,
      -3323994724922067210, 2650015686316767625,  -1266987339076084033,
      -2719592797295652274, 1001715882254361093,  2985705174833580074,
      624998344570078493,   178171879627002364,   -2685134926876324676,
      -2182518240673755762, -2520592560099933175, -3321506220418475765,
      -2796991756129194437, -30756760619269485,   1773440867362552662,
      -515018926286325995,  2729509428998845600,  344042011560269889,
      -353893528645616404,  -2602781565020067934, -1113568615392196928,
      -3110979762389338548, -799728648002378514,  -661664826491589530,
      -986907045591873256,  -3514330002058864388, -2880984100493381148,
      -2408394900974335025, 2338927689143856574,  -2965008068122043049,
      1910724724371360534,  837948280392618262,   968718257477236744,
      566857914916672612,   593580232400466295,   -1275833980477072038,
      3220311865941375107,  3484487044758238748,  -3362613249332940442,
      -2878156595798136201, 2573690689327892073,  2159858827254714526,
      -2764563411760883184, 3174707008615662988,  186671929268185990,
      1821628454421950809,  -1171354976813275172, 1857586255194078761,
      -1457940790919656157, -1011135037118730287, -565636285955450346,
      -2017203945957248297, -4216965763573876965, -3534593025742239458,
      -1094023828855093368, 1649210173428898215,  1346095042896739089,
      1858402312777688956,  -596131195688077691,  -3886883360630945996,
      3289790283969995675,  3711889742407472711,  2230117749484817739,
      -3925845684881130865, 43828776292867623,    1068222716750813835,
      -3047668448345954977, -661281057729196156,  2162647595175988899,
      -997285998848281929,  -2804359322850646459, -1336448033167398840,
      46676972462619278,    -3205799900321413484, 1592698370668946134,
      2991132202276133573,  914497827114379873,   -2454546381634110044,
      2393432690119994236,  -910249225604715901,  -4988446780522283,
      279049095904058558,   2080240807741682215,  -2893567206255507563,
      -1883768482880100859, -1186947640215550601, 1763411570598311963,
      -3672125958884880864, -1118267589268018454, -352808778190183409,
      -1868489950299493216, -1042547830416658919, 2260854015295619774,
      -3379969426647483331, -3269275628187402860, -372870795492745166,
      -3699674284049042381, 1469640433669960877,  -1306810527679794779,
      624827148559509220,   1250561973239013949,  -3486089631187408836,
      -2656493603711385116, -962132994086946854,  2127035420649765032,
      1814279860198276054,  1374699551451163115,  2243078540385733028,
      -1389915554375437199, 1778226429978022441,  -1217194586127481439,
      -4115109613487052284, 128959488020243554,   80035900559887900,
      -1436203383767964655, 2295285833716303419,  -1871318092325624160,
      264917927055027392,   -3601603380109000093, -2255403117673283024,
      817916963160130911,   -572077602107357065,  -3378012544036971788,
      -2694024574250446509, 2875739931011157914,  -2913840366690459817,
      -3839357617843629789, -1363779559076225365, -663500565174883769,
      -3365681800247605538, 888464192616166734,   -3418036881664656544,
      -1786793048248677315, 3528329390609178397,  2007833791738093438,
      -2492382925417292405, -1400426555030372416, 2585334720519092550,
      402625172564568795,   -487924307581244256,  3170295315465499824,
      -2138613217551322373, 694459974407589533,   3058469189232299830,
      3639475123910406669,  1458415076466175861,  22997444691921957,
      -3040880377408810276, -1201539327872872342, -1054932553471149983,
      914850877631870970,   3223470389293964095,  3121028228023098030,
      -2477525045230889018, 2780881826750335434,  -1294524557612484164,
      -386511316684531296,  1891875268100889532,  1488693978156901628,
      -221774101631240349,  -3171483070444273915, -1556050045668703812,
      -3155499313454649646, 3014799953922369053,  -99689314913000772,
      3534611924624205478,  -2851834896953630747, -2230029543346962015,
      -3114050103406231380, -885051144444512736,  -251162591853159869,
      -4192623882409325872, 784264339528510503,   -3895905351861388352,
      589503371574441297,   2795047999964528252,  2936835619138991277,
      1030471170683287345,  -1147868339226719428, -123339940242865678,
      -1084951393551108280, -126744302247694875,  963535282863087746,
      1827802363388405181,  -2580806263569607966, -1109744277685187338,
      3262010814205925380,  -381598872403801053,  2411212007747112113,
      1903729351001587226,  1921854907847927577,  3243257500263203441,
      -2893860166114470494, 1844338488129693589,  -2916416928398132320,
      654491230269596549,   2756470189494923249,  2937928418511804589,
      -3431964675727060364, 1630516547810864675,  -2419261214773216357,
      2886691265546135284,  3728571513092787262,  2745987463053020641,
      2790749906937995181,  1401208003562828359,  2257869372182707639,
      -216211272515621510,  1992506902001818065,  -97675052069955712,
      -1434431902875427508, -2263935420060672268, -4177880847099790715,
      143387686404798917,   -1581500284651955866, -4042910854446538536,
      -1337389391260661218, 228290225356485794,   2162549638868578336,
      -600975757389788547,  -2393255775992715139, -489322035440258528,
      1514654191564353075,  -2566946121739501800, 3608879879243309421,
      -1799816772073814277, 2856278885940785218,  -3287320660092732241,
      -3450554112692574875, -1917047169975971962, 2389083554637879708,
      578557267399599558,   -992378940286133190,  -4063345648746421667,
      3425806691116041918,  -3218176007103847397, 2706696000382644277,
      959106360940814011,   -3166159364814256742, 3457263158903935961,
      -794658791884175106,  -2848008085051045849, -3736315460173406785,
      -2380591091436675195, -3404728105679560424, -822660922211604169,
      -3481026222841380836, -2473214192094112486, -2167881021098116416,
      1183103225061878441,  3060140513317349020,  2508750856948023035,
      694062250779138935,   -1197872958134360236, 3662636093102133439,
      -2825976845375223048, 1219155110751130520,  1392364794874474886,
      -1216685808831950362, -4071595318916939527, 977375714581392530,
      3702978529176744756,  -4180215057277222557, -2093618048232632297,
      649348044945903912,   -3686722250661970900, 3567306997125436187,
      58598435339235727,    2571037695662997437,  -2750634829255080633,
      -1643255923313677057, -2812023477093652045, 2802101053059061842,
      -1290372083166900988, -1476422254726564154, 614465696347321768,
      1387728472007629598,  -1403149827210247050, -2093809742591806132,
      2102993453413313134,  -2475108955851590453, -1045330439550096611,
      364325082609763127,   2777826092972017247,  -805848714476448107,
      1855580761183142170,  -3698858720437824714, 2512849486572535015,
      2991183469357625558,  -2964974879193691035, 2904656462145885570,
      295105116697208427,   65218698374260957,    -981408173447653006,
      1440691594245096195,  -192799713599236766,  -2860956334271609920,
      -2464474861828656073, 2175438187837672191,  -3287548590757979044,
      -2247515342120755588, -3904042135724127812, 2864783639087490657,
      2241733930901198451,  -940264635298158446,  3368845338192836097,
      479556870897207416,   -2489211647724551138, 465988218804491767,
      11008473421608930,    3237150112292239321,  -3309152575425465900,
      -1433964151504213853, -3301562875615750860, -2471668095252266631,
      388435203295655023,   -4199017772807090055, 473721543577502801,
      -628707207912625110,  1385892163711797011,  -757340605893717916,
      -2079899184141156724, -1860908577410374000, 2384862694272458161,
      3038035886313722015,  3115793961656364113,  516919241123917550,
      3511445284380946418,  1166202172115124940,  -968859728190863848,
      224728406665419119,   686311754857335551,   3269498727775554343,
      -2507316079350659127, 1091077325028741933,  -1900341853756552304,
      1604997632090652085,  834048418853512513,   1055302579710383932,
      -573859429579600231,  3561245180885998814,  -2876449974714882771,
      -3939237212036005280, -1139811762357249683, 3110576513776316606,
      -2129194151354675783, 756328808073794477,   3025506174464266332,
      -771355122766193284,  -572337986336952434,  -448695630519639876,
      -1175475055511916485, -1533883812293479249, -2949134021174640682,
      2352313481965402817,  -2919168362093100490, -1580065335967530328,
      267864527650655642,   -612792333909231543,  3369681580060668924,
      -2878015937597622385, -1610770628516443319, -234555141561392478,
      -1666887793180007732, -401863943901945134,  2986240605015726116,
      1398485673064052221,  424841886641496984,   -2271389902438693058,
      2643923354093100720,  -3935508038470450608, 1451820330957009049,
      -2209302772965146468, 1055623821097363236,  -4081411503828603467,
      2585630474030462534,  923584109211997590,   720195252021563942,
      3275479659390702722,  1160581616025702480,  -3991180781352096239,
      1524571285187578708,  -1367332746434357132, -3783916917268732291,
      -2566299351817384280, 351909284445159139,   1798423977769434754,
      1356691462768141410,  3682550751654393055,  3080892134798281552,
      -4193145804999318587, -1605690998085275008, -2560221323201411622,
      -264372534615330141,  2404154869790204244,  -2477971205354882385,
      -35697071039983264,   1528422121020678059,  -3319959593211454033,
      2036170413285972926,  -412472436116127187,  -3643120853535010801,
      -341321387860978131,  -2815021852998527306, -241128842165821603,
      -3377737576176849881, -1405507305022844897, 2389519284728079008,
      2923931929918639552,  -48289448787549829,   974755048191334622,
      2903586969794434544,  -2932685456865120605, -1469084853511730916,
      -702698634738522542,  -1541527204227205884, 1447363093617146807,
      -2089492438353815299, -1840567286480309058, -1890701574170082550,
      1068935850080075470,  -975720872926805477,  -2293315807080455248,
      -2837924001458112674, -4106921476058852114, -2693495047277161862,
      -338041524528336202,  -856610960429509198,  -529722619605585071,
      -1188837437012449400, 3609387544026819629,  -3341537551598613982,
      923849213290771584,   -3630845215246932177, -3834128456896603853,
      -2732252542872520634, -695274557096511433,  -3892702352124234524,
      874088847435847898,   -1270362854281978296, 210112501203808539,
      -2779657846419005765, 1528650311723709304,  -1621719215430886955,
      2281521354688948119,  -1832378404166745955, 983516424044494928,
      -2644702522304132179, -3136861948142883393, 3635710612515891395,
      -1909886282266687927, 2180578606647309286,  -566861624303206318,
      -59082388870579672,   1241439294528865005,  -2869594314115032961,
      491360765214877852,   -1084389286859508725, 933746940549430804,
      407678231471191648,   -3631902334097528735, 3008864998116862363,
      -4180912788863469396, 2738947626021883464,  -307613826710680156,
      2781142719401856184,  579213094791515630,   -2998485996067732164,
      -373044243128049725,  1959179980369013260,  2651051892252772798,
      2404955272671007699,  3215762497057185887,  -3507547411239047653,
      -119793025189257020,  -3112215516780631419, -2412690618901881646,
      3361543946954173471,  -376295449652621247,  3167233759693665346,
      1522650679764280494,  -3513604371790511429, 3302802567792185688,
      -410399011356379073,  191398700100911473,   802849932974552870,
      -3557499468967191370, 3557710165446458791,  2182448609548176198,
      2180490422892775306,  -3045399639505952868, 3501273248754170911,
      3069812717718154691,  604139375424478037,   -2106920689943122891,
      -359749258065136906,  1365338706210932920,  -1185921531485557736,
      -2187211781551947026, 2227318262926207190,  -1481096149097964303,
      -817786566671457912,  2043005872558913392,  1572588619683741009,
      3384159446811307610,  -1277698046236819916, -294730986643809959,
      2602839119081529781,  -681767532192125854,  -1118090177127457701,
      -84143221951576175,   2105690535100954008,  -1929541532477388432,
      -581145722107764872,  -3441559520078626028, -2370003742051812163,
      -2528543698686190373, -483811935468829419,  1514859562595355291,
      3247226007623004370,  2667173904165096353,  -2201173415356040409,
      174831927039686611,   2064720891199760583,  2501383868472067026,
      -4003556012057279285, -2865850237358971241, -3734929374306577056,
      1263609506522132677,  3139674084455962311,  -885211364304800108,
      2235414627802748659,  2481810918463647090,  2553876944405423586,
      3584402170699941857,  2801167110696880090,  -2754078139782119261,
      -99795023425092750,   132183354022310162,   -1430087777932499905,
      490152674672373136,   3070980790133765142,  2232779366460498497,
      -1096376688131319174, -2828984037570780558, 965500153044579011,
      2437397931989046488,  -3524638066841043481, -3982959454093358301,
      -4228238333466851532, -1558773572710019944, -3291178899585902603,
      3254575428699145946,  -3537525016415029284, 2764651560255929739,
      422675040355772724,   -3697111073883148489, -1492782075814649506,
      398656907577705918,   -1144080616953105495, -1690304943636921489,
      -659207809606841077,  -972134230815216852,  1496842987547455761,
      2691662814170802770,  2709024907420647985,  -157677866759545743,
      -3691472931051899581, 2672670286515558608,  -3797996265247102224,
      -4013848420735232716, 1250007544214113586,  -2014353750537118388,
      3425242271586013909,  2704591948673395026,  -1375805685056677544,
      -1644184565147197427, 1350598781631506481,  -4226831815344538247,
      -840077862757112667,  -3207972121103981525, -1286277646872295159,
      3501115359951942722,  -3176176424380763100, -3104513188736999746,
      -2894542091899784056, -3320993389237553915, 900640386583305963,
      -2002613435593966929, 3467769448852039478,  2479507030273330624,
      -4109047740129815615, 1403935171552301963,  1545400242370442873,
      1154831344955835104,  -3685216867081140797, -34648589300000026,
      -3207852147431120693, -2076350489908451775, 610231036268414131,
      3223256677371249290,  -1415303165858859959, 1950075405006185024,
      1053343126975633504,  1640182734824938424,  -2237589519136035076,
      -138609235715384695,  2649114893106077727,  -3801575053873959466,
      -1230750767596956,    -2481549179876754352, 3033510111995200287,
      652438634494849634,   -1411852481053509063, 1796139404606389506,
      2235383495682380462,  -3313534430576080189, -3165068097949371818,
      1733304703742425538,  1768313291605506076,  1328589030571172133,
      1976337773158684189,  261034971627840801,   2497523634256075128,
      -1291754170585311198, -1397574895033684171, 2695670283083878159,
      -1755547357241551229, -1413466562793748670, 19755938786307603,
      -4168689165926725993, -4118052056441618443, 1238688514373468754,
      -1217846372273937346, -2722198800743443336, 3614497654708963266,
      2017219705077256652,  2322915096558993219,  -2687770533740696892,
      466983766610098796,   1468402789497713379,  -3021101885678503099,
      -1309291769086957965, 201297507004386698,   3630138972027826076,
      938708357440968931,   -2717473811696196002, 1387869741407883035,
      3117795832911698886,  -1420072197522681865, 1193283778940840635,
      -2794973339400849938, 1575358242525017086,  -3138529617163836565,
      -388746679510495687,  -944493845710275495,  -4176175009835431952,
      -1163287260635395447, 3258044625545667879,  1054332909750966486,
      -2431266465825953412, 1695295640627140792,  1471264978878562900,
      2731858958652707107,  2786915930356061272,  2029515406452718285,
      1519561647152445388,  3005106154748923352,  332952660997955022,
      2557213802972350275,  112774614568982830,   -1189245151155751276,
      -1251354766453080997, 779574361461303048,   2076638503454030001,
      3242611328077108508,  373845416420386786,   -1717950470270716830,
      -2326916371045190609, -1780795000552012611, -372563455360644756,
      3702505116445042428,  -2777914561518913288, -55140198692069574,
      168934057766157038,   789268289827738431,   416294904776848000,
      -795131452669784841,  3319637937365099505,  -1777257315306099234,
      -2013858274949846372, -3984757665679118185, -67245828709047634,
      -3521522418870656766, 2170412956556596854,  1091572616576353617,
      -4195611657549815427, 2481654005765055345,  2355866273435412502,
      -3618421932287052138, 3258497895628375806,  -543394812901831677,
      1894130956797415851,  2626512407790567264,  -1443176235503476233,
      252889743876484720,   -316824247029810567,  998804295610728782,
      -2525896671834984577, -791981868256375959,  3498627998654553356,
      1193646056519933127,  -1949072476734130695, 3230643483423806118,
      272571356728064104,   -902445434591874041,  -1212134253074928706,
      1874099474554768013,  -508044039607844474,  -3954963827645082546,
      3576411339807734474,  1128631295528025928,  2024855902852045099,
      2029966791754528860,  -1567797879587933124, -642629871135350382,
      -2177005400911372968, -1855936985220637446, 2625460954389109493,
      48031365492992553,    38949159669032131,    915051371143681162,
      -3646751783287175398, 3319366776766059965,  -3469881764360339626,
      3459437800917102008,  -4196678044861597425, -2430286472245793027,
      -3198675198453587664, -696119752442987199,  1829202346679999882,
      1486945740782969557,  -291628741502444818,  1613370091821512504,
      2221454803170638611,  -2922500149464888952, -1506823897667085750,
      -3466010894614127294, -3759109872679986577, 2254817799899007889,
      -378937141655432262,  2205357712848911265,  -1152595154465360151,
      3474016593142391794,  -2971496002190847235, 1739042370708319675,
      1347432463143076012,  2374865958273882935,  1190241619445188247,
      -3678799448747384747, -1100727663641570402, 1933149123149114786,
      3197772546079729766,  995857701587938067,   -719544532984246890,
      881020865047726605,   2728514328491916249,  1069922122085166530,
      2700131339454196815,  2519369779686813142,  -2369478125109200009,
      1570458797461049202,  -3529649797140634392, -761472985637503609,
      -3529396597039865805, 2920984655820521739,  -3639423038105522602,
      -489344889339177441,  -238358221352194236,  2943819216020853868,
      3615170653853732813,  3349771975545505430,  -1168405413774761990,
      -1855299362588055561, 2027965833495070668,  1638217722850133056,
      829564731849061080,   1250448433026295027,  2637808234184386395,
      1731652853620189370,  -576207264694448456,  -2092175126398185922,
      -2871045481503629063, 499536576476281444,   -2788806495633132956,
      712269980355129878,   207915089680792892,   -2602983436216863362,
      -2703782743736656030, 2583121689141111663,  -1007831665966264007,
      2963107076842103730,  1976133113796123907,  -1079885516898344791,
      1827013036499158852,  -904969923093876838,  1948926728670687000,
      -3295445346686422354, -206535795701312732,  -2454909094536705058,
      -229127215681378237,  2366717838743164302,  -2220491679584877013,
      -1046946002829834891, 584615187735164997,   -4188250420226894174,
      2242346430140166028,  2899326989731999906,  36115318275066849,
      1096410164067976034,  2989013233262088980,  3128729415888754865,
      1225118431821223877,  328586782342333802,   1939285929834778911,
      2164591440549707982,  -2411298565546126108, -2845900818106742149,
      547751498417549191,   -1380993683735033854, -1821910607765774586,
      -599559716882501725,  2473283665017241911,  3024037635012547311,
      1323674486638384220,  -2261442067904721554, -689057861014477409,
      3340211293807432802,  1837763104304424543,  3624081120845677549,
      2091308229357312918,  -4171522428056270339, 2473160868557641423,
      -2649629902507497751, 2653815956220175773,  1324145333421853042,
      -92400403797780620,   6843462930401871,     647328771598541084,
      1444591826999700745,  2209305748206751213,  1647493393119960732,
      3594099451333539861,  -721509466743012681,  -192701153518584317,
      -224785977836587614,  -1197978681760522648, 2046146371767752764,
      1613146024184947115,  -3896833981656984405, -853605920672475553,
      2796612098932421117,  2543274269375949479,  501740253798428273,
      -2544919150944230038, 1896433768806984604,  3541557163618823991,
      -3983374123866780561, 1159602465981028450,  -2018267811840237017,
      -1827875374263463608, -2024661571162494309, 3144166790589933978,
      1688090004499895535,  1551513479423947764,  -1286115298037175169,
      2130994800382648621,  -907795885856253301,  -1722428728356061977,
      120480584396474147,   -1017598892391674377, -1238801327999714261,
      -2869418636621342479, -2231497841419147842, -968169873660369120,
      -816021715474850336,  -4159176695502795824, 1321340497370032205,
      -1440665177152551888, 1713425800176817472,  -639910880920261054,
      -2948843490010269148, -4068455508720595679, -3427026944142547441,
      2918597982127740550,  684177772565517672,   512842852147579190,
      -3106838730311189097, 3396680303312673212,  -1391651581721108115,
      894433731832874844,   -2611751112815164287, 187746792775010881,
      672983032039532366,   787057665537116221,   2582079733434673732,
      -3102683730253225576, -2736549262877595189, -2228724779086300534,
      -2852170967934161071, 2330055500336648067,  3166241089438132834,
      3565537445695832660,  1546491114536484971,  580437549749885162,
      2970551518663300888,  36239961894210358,    303795792419474649,
      1364606830785099107,  -2407994032059719054, 804563821316012820,
      3546247236493707060,  382329704755204186,   3140836407383878692,
      -3115980771018776612, -3340263902239646539, 2544673040173301322,
      2284702234974921729,  2469942917107077462,  2901202224256020584,
      1390525270189193569,  -3395552273928812910, 625009824427532198,
      2135324309350425826,  3082647320969105749,  742720354115960701,
      500928680026937983,   -1286900265800357042, -628941711734476408,
      -533138073745463191,  -416086937168735655,  -2614574591339900957,
      1914973938250540185,  -910134200976916556,  -2692992721401007503,
      -2285693111616431446, -2785388312321369768, -383723437373550312,
      -2102255452626072116, -3728767633806740767, 3322994038309873939,
      1634800473015352070,  1422095342735675304,  -599589843482451634,
      -3925968232946192160, -3700769820291037104, 3080203751075801212,
      517162049702385971,   183777110954686566,   1100652412040285689,
      -82827663047330980,   1133747387254206119,  1039231568290955950,
      -1322715666419828520, -3914251988375479612, 453491454070690295,
      3694347772189911893,  521297898008971737,   8156830656541572,
      935636911874954747,   -640221251434620691,  -47049762997190506,
      828518990679347705,   -1026519324386554522, -2029874401027623103,
      3518354364042849246,  -445054750364783064,  -225463461108619118,
      1685953959235374747,  -4054077353886463682, 937172515552454230,
      -2593438272282389680, 1398533439497876977,  3177969256102631224,
      1304104632594032259,  3540670274952004153,  2843594083180708478,
      -2943793080797989931, 3098416056560184891,  -587387795477169315,
      -2449785293150150603, -915041553386002238,  3058589631622355354,
      -905599302580667657,  2551945606973525790,  -1103136962804499493,
      2602009983297378624,  1462048225680350887,  1751872659564881694,
      1318455429864418203,  -2629188123840573376, -2303968449673333174,
      2371082097096099035,  382496988719042946,   -1710412994630562153,
      -674185243510866915,  3237455540978415922,  -767627976996526363,
      -3265240357516708123, 2615319606625558464,  2347977811683078746,
      3452143875618459835,  -2288048213341784940, 1723602637286498016,
      3344004682075832882,  2631847540557204800,  3505029924921263495,
      560720456287774769,   -1865449251429872034, -820618439770873457,
      -792280511677569836,  1752964384302956123,  628046866898236909,
      -1378398841310645676, -3075138158201495544, -4152508101310954098,
      -2874812271459410615, 738564920028514438,   -1578887747278098345,
      1874946665773459520,  -713707892473281515,  2135873003140936116,
      -167515079362481335,  3482105292138587679,  -3950696289338098371,
      3264644222569451000,  -2112305988256735087, -2908850941305201926,
      -3099584779920289264, -3220039872171576812, -3556540122175269276,
      -1454889647039559813, -3912193361083899761, -139571931614112743,
      -3911066745576549720, 2155632746550848898,  -2965883528912009228,
      -3033785833226642897, 3554705064827460244,  -715772177044844346,
      1894229629858529079,  267111900921729565,   -3541608794100507155,
      1715500905922148219,  2904326430139080232,  1091758696798583199,
      -3916194686548378621, 2081422399176924274,  1307472008731358033,
      1555359476310012735,  -2000963806106570291, -3678836425503233253,
      3491331997827943059,  -4189542016749616421, -2331568432149019247,
      1712688281268292721,  49396091069953040,    -1738269289877945329,
      3442660428402311550,  2115574502482556823,  -1303471075287583875,
      330403927967172183,   -3393453046292654130, -478887911817502493,
      -3351840500912326357, 2510530238917187059,  -711655440107258527,
      2411136336338936490,  2312135022882002467,  -3080373130517483557,
      -2247690226184699581, 2673895641206127546,  -3862380497894332726,
      -874245034770376929,  -2179862202870297502, -1869531985502347947,
      -4160076736857490199, -1084636554128746979, -1302222699896573297,
      982122255691982075,   -1459402537863158730, 1604238322423151495,
      3094065488910039285,  2646275615410543889,  -4059161969209771755,
      -3862608450933424399, -2348691137726875284, -2777429506809900711,
      -3614588343250404002, -3620370537608707807, -1557286662662938974,
      -3590569425180135074, 1847324200479769869,  1614098244547802415,
      -2017341981774886322, -3644304058451355197, 535360581077108080,
      -2197806667216156217, -1418293016113807962, 611691923513530215,
      -209708016473985833,  1618654788065348402,  -1227336187780912776,
      -3195132345969570725, -1748230867413587913, 905235924846791023,
      2780879100850947764,  -1413709427589212880, 3392598029665474851,
      -3738615070883830633, -1395207852751728353, -2035999278604904502,
      -4206064609496346809, -3249841557614181947, 868671693030922914,
      702714771965386231,   1182733169496371969,  12689280004545690,
      3044375339348887651,  545945680458203798,   -4014354020807549952,
      -1340756469934705832, 509925158138372762,   2081577040404915731,
      -1684043019444018612, 551167715583955671,   -2909873163496910277,
      1494964691748413820,  -2954979557289806097, 860971051407366245,
      -2899106462893271400, -367471411604556582,  501771712908915508,
      2936942677368166753,  1388564764372086210,  2511178523783091689,
      914520655327826335,   154581013197195537,   1150535222523971719,
      -1843502488002904464, 3252838839555735290,  1130031378074852517,
      739648588942265956,   -2672536402863115512, 2596916176598940558,
      -2369929519677606663, 1056697536776091452,  1711428344205461874,
      -4188652541984237230, -1741160824257237545, 621118308318788913,
      1459455003983757283,  -741086890436376472,  -2086760205921471607,
      1146294536638283150,  -1451947686135481821, 3708730221144507461,
      550249845739815104,   -2813458258982445488, 63866543503767119,
      3414572965726725129,  -1525679032655628135, 1804621842353553910,
      2111855450387510077,  -3353125787284830842, 1462493453661866931,
      2848648611063074032,  945350377583267861,   963653615879139255,
      2541892161850578597,  -2778147903034116769, 1271114944663237795,
      2479284872177022438,  -2068308941803428196, 2011879130658947311,
      2034377704269688137,  574638983813992513,   -2586706112211985042,
      2569419773866305000,  3332279208868683610,  -3598612880107649925,
      193942901233525071,   1908332166040792699,  -1223258573190020891,
      2915713638599598946,  -4161738010074610901, 1291657541928308082,
      697641222114386389,   -1411996992988263320, 808031293829732399,
      2676114760442443709,  -3861053973632206044, -1625402828408434005,
      -237815221044059523,  2701810523444397261,  752588605171580020,
      -1551580049845693011, -3101640584902927161, 757501987066639344,
      3628161455295897192,  2106444578985509789,  785956487824527307,
      3196367164424413697,  -2258728741283099696, -147190103048244069,
      -2098718522508419172, 667688443484053599,   2035271608362085909,
      2634598089589706997,  3191461799746985228,  -323851097590275760,
      1327004539462371971,  3156997946730756391,  -4000308211022783434,
      2795613751775234717,  -3714177051171482679, -460404780425845046,
      2068562678100771552,  -2082652299025117538, 121225277365898524,
      2306241468404854398,  2095155105699140169,  56501388022353710,
      2100062808901747755,  -2083753951914584008, -112737560468905493,
      2957885631591336217,  1379632771946489529,  3133250313496061329,
      -712852242592983171,  -333963824454716748,  -1318202643174653974,
      -1820582289280926827, 2595314207568451716,  1085529059992784396,
      1443122795520448388,  -743969591660572744,  1466832835352910722,
      -3690881142129044487, -1637860923584276363, 2388212430903899261,
      -3768336503654476064, -509172205022491351,  -2399940375088030298,
      -2989916798413150890, 2127704465616557237,  3260266234011796101,
      3626579661567215991,  2702214503634668691,  -4037488931397834181,
      2332592465365580025,  2470825562732980156,  738551136157596375,
      2818271977256416190,  -1151297363301504026, 1937186044866328111,
      3300701367727452737,  1877884401081305088,  1429766048780325122,
      3168913915327695274,  -19807648597435815,   -2957257824155943865,
      -580005307915876212,  -2643099042212977885, 2052015660643199710,
      -482468340089566333,  -3140649717377596855, -3938146207673795489,
      3110189778793694061,  -1538817827841541606, -2257199330256943375,
      -1646369330496207304, -1905380427741278810, -1270509152132429542,
      3353949281356280104,  -959218655642947279,  -2026593596493155995,
      -3259148417189711893, -443882669067266237,  1680623204293682832,
      -3758605015971360945, 258402769691209792,   1996238472301179009,
      -1716138185146642569, 3047091833245423601,  -132336755805776217,
      3288923569369887969,  3627009838560697270,  -2318290898379136222,
      -1514965617154840108, -3438867621967711569, -3252593728298485471,
      322740949389811821,   1117635341735050641,  2207625799443099780,
      -2490589487978010229, 2185094983915615414,  2871145019434856013,
      2208046316278480687,  2231240623956953583,  -3920177615023158266,
      -723094650842344928,  -1922395477009398211, 3459375346306157296,
      798438263708674599,   1628936746939910525,  893612239847174728,
      3191984493356434011,  -2022979798783075409, 2177213763510490300,
      2099249908551638804,  3569979815831392954,  1203011177065987866,
      -157264687688123561,  2484434595069773465,  -4059561842927420700,
      1252934698308797657,  1583955076712955344,  -1313938455925004220,
      -3044055785792910960, -569760764078452364,  -3404375929357690722,
      -3773716367108937013, -2026169708336930278, 1728503677323848597,
      3625586121492883698,  1055980141895234353,  2882159003657254469,
      666719900790700727,   1033314647348730771,  -4224632110532025434,
      -302120892281557359,  -3219532176367672343, 2594104819200086697,
      1280347706998565567,  2605764039452536764,  2790653707452351073,
      2807179292987441260,  -3690373923901084870, -662058139230039440,
      1890603401861844486,  3330244819841590603,  366792085251234016,
      -1787617747031817852, -677847777058993236,  -1332062980080940772,
      733660502055183501,   3024520934534035228,  2517324128681461875,
      662451146900663993,   2868074526450604219,  -2539907731716527678,
      -2729806117895533690, 173059692951328533,   2478179762482077886,
      134167662932555631,   -1455306399306679334, 1482946088294875980,
      -1642218159655493316, -2274374710984283131, -4137471697430853600,
      3560155889362807482,  1089878524151782807,  -1215304202811754557,
      3654298252880795137,  74903365974178964,    -4203370813659872458,
      -3421265334079793250, 647747792143493646,   -1188451365336335080,
      1437004839060960619,  -3412508639288959316, 1520160394874862101,
      -1071238291754555963, 1089128257415404247,  718866140407408029,
      -2570364329144463287, -2716089542670566479, -3314853413110881428,
      -922619633854238027,  -954715739290185947,  835620742260547059,
      -4178117961627008122, 2808681832258131926,  1022584702823357790,
      -629121208957303094,  -3625805787092200373, -1664770937193822440,
      -2186608691581889121, -2943972584854141527, 1434182377769377219,
      3288899783467834004,  -1563036079332144484, -529695424906559855,
      102821388026504800,   -1651303381282228123, -3011344592927552806,
      -2516415162828361582, -414541707928204573,  3530912219707250095,
      3557626867572033402,  -1587878078441540290, 3073348801398300851,
      -2265945410384407966, 1958653821901067291,  -2986863585546172692,
      1180978475278228033,  -2682483085761749654, -3059236007453764155,
      -1752620604295362039, 533271504993985266,   -2161840941265384544,
      -2718079587879581036, 1492156704571807685,  1596544086023435891,
      3339909922268322921,  1609117849600701684,  -1873404421089992199,
      -2801903714225697005, 1425015883537856123,  2482441862780860225,
      -1457852233280075907, -1757285976340966956, -1123719810911594989,
      3637908349992333276,  -3474885851654699026, 2105932742068763493,
      2935718788493473345,  3118645946102442985,  -3622496093985230313,
      895135006354509487,   -1733775204410246656, -1189021420192743299,
      661508301870962621,   -2522405374554590533, 1511862021203657271,
      -3372465022332972531, 230100053581572457,   -32448657351778929,
      -607280224939854960,  -1915998655733671920, -3452342051792146744,
      678365441770598777,   -1153103971554140390, 3063027404891922454,
      -307991872328623692,  -2108254633656431133, 2197113399057508541,
      -2285227928062369142, 3138548620237945112,  -1578589375572648854,
      -529311769292824601,  1071768619779863324,  -2192081927711413426,
      -1364657891974797395, -2721967464746344224, -2390103506630395432,
      3315633938858903574,  3680740973496062949,  -4203446834898427367,
      -3595274309838054742, -378239613160769195,  -3149074895451089101,
      928751696581723496,   832152956508202476,   -2044349331093197723,
      -941009655848965703,  -2089453831587914170, 2822951820679083491,
      2279032948600331412,  2175650975154093472,  -1382724154423950878,
      -901545976480079837,  2490790097944969842,  1145842614520781014,
      2023129963381641072,  1732261231512422168,  -3810646296932059675,
      -691602922524709737,  1160499156987621792,  -1136442734689248683,
      -3465662591347211517, 2493926823699888206,  265875176036857531,
      -3073873150112298441, 1951970587334663358,  2426226446804490062,
      2813877807662367591,  -537779689087469789,  -1107104628919014629,
      -3628107259647195488, -305200066400104154,  2959896961894546478,
      2909759559670640767,  -2923797578326900467, -1965671104034664293,
      848054412670583543,   2723337301015137971,  -2852843704828952306,
      921976829060139074,   1796174071951695672,  -458285173127159006,
      -2476885102801609356, -2677961418993892229, -2742418917492377335,
      3603529603767277827,  -2476917525417243585, 1871584850906011072,
      -1273185589326106937, -408083591627226322,  -1330210586823418579,
      -3558611176143071880, 3662945525412534751,  1575364861915639445,
      3693461178244834825,  1630380369383834814,  1629695255535040568,
      -1246297640691321330, 1322591190069873167,  -629833643694271149,
      1672244418357716429,  3314375047577244541,  3547917031496885743,
      1948719746237994265,  -1566473935327925010, 1088408293466813873,
      2270446610097492210,  2169905118856762475,  299844477912756341,
      -3791317193817950456, -3909539190961354895, 2248346091619413534,
      -3168048948122539798, 49471325793357388,    2899789181293776244,
      -547713377921543072,  22938603314594739,    -3718875704723512183,
      -1237278276213373737, -4165608998538247590, 1702369290620157076,
      -1242920307517065341, -3734119638192893543, -3006458313599103175,
      3255730627447998207,  -1637961457730545656, -916009190092371475,
      543344179994516559,   2529297792218199733,  559482692085808607,
      -274401285772936851,  2588329858532044785,  -2720336544792841119,
      1042950667773055562,  -1171977927448665661, 1892062006968069116,
      3480168937280156685,  2819857276247932111,  -3233454344782985982,
      2433111502982640789,  -1656098093833967607, 1798511173027835574,
      354074500367390374,   -1206168610598895218, -3994717011719951753,
      1523274736874986496,  1841034894944013673,  351545358774876995,
      -1751503275254204788, -508006610303575995,  -2303520909776321334,
      3568918080770533574,  3392056667545048728,  -1204978870890436821,
      -1006001069358378601, 90950473607683118,    3278772698211205403,
      65331393103800021,    -941994064731790606,  3179874810463339721,
      2729606716071941207,  -1171857908165589022, 3246075917897033547,
      -297929761120636992,  3495440957971500572,  -787483400653517438,
      -968549170613886201,  2769497487347612600,  -3404511036919522899,
      110339045428090431,   2642358294435679456,  -3870284474942375788,
      -1020420895066851161, 761737073132731975,   1469570377811301722,
      -2316072304452438066, -1094960549217869716, -229077314521573797,
      3678302835937176138,  -3162459328741238768, 1571863776282499199,
      -3053873983596941113, -2330339992910135529, -1856073091306913435,
      -1013182883565559951, -3942312974393721702, -3909282503508177177,
      2235876275338002469,  2084376386236135983,  -2969239736634965875,
      2076182223574838470,  3029429528238587456,  -3865020888188493225,
      766458413914986883,   911585408095395435,   2631214499084308373,
      3093813751502649673,  -933545193085551834,  -2784075758227347053,
      -2780921722663298536, -3566977559933055331, -1660897481435639124,
      -2465752867571378808, -1093689525860686830, -2301115208757063045,
      -1860434152301367461, -2456448266663539257, -1032780000485219339,
      -2999322700718658208, -1652365117855541661, -225300421931538564,
      -1636444809277991638, -1908492590797488335, -2864378501941804969,
      297528420550953995,   2205727716751778079,  -402408830091576332,
      -269858407067162274,  1081705008667947705,  -1206072557932132000,
      -4136519744395236859, 3646960581486431949,  3133402605637584069,
      -2490677883576410182, -2794918337925027039, -3575502442927024325,
      689761452080713439,   -846668313146981929,  -847268344640692165,
      489796205936816919,   3467074294526841485,  -1498253171380656877,
      2737674349200679081,  2275587352152486679,  2019202957727512086,
      -3278848799192451768, -3554517842019119527, 1414735035059885923,
      1119805359335900301,  -455912451529764541,  -804517448737353633,
      3471430462615430821,  2714980031626801431,  -189597642637991235,
      -3284011333419665799, 3035062627194502959,  3131543753569085854,
      -73465506856797453,   -4176506781965193222, -2199136394499395192,
      -1364515849876853578, 2600300522230980314,  -171323912827434246,
      -498487694646508995,  -1098712296689675905, -754050663600873425,
      -2149058150436645439, 1662851628672181481,  2765473172454394844,
      -503221732791976457,  131405428787096741,   1216766494835886185,
      882707096320990889,   -662273516486964824,  -3995193152286313641,
      2944225419484315728,  126767010986961178,   -1077725115275035415,
      -1974871750921140036, -2535398139043212682, 822231578677925915,
      -382211339705685785,  -487410016590752804,  853325869955730498,
      -2203577428225077299, -1868965715061165843, -675891535419024243,
      -3588723202703971694, 2632107335451098914,  2088417038004781177,
      -1354416537756852310, -3295053165404234875, -3682507206292107624,
      -3574022303850453888, 855538369010792564,   -1503888084756815619,
      -2210891697967350737, 1990300968815323353,  -474287061541355649,
      -1349885168052906472, 3711229938579706694,  2750542277068466186,
      -1274985057836933046, 1090409158296172364,  3246863095605189186,
      -338096928525701975,  2737707418666988420,  -1139304816926727054,
      -883700141535380640,  2277791370749332607,  1257970335390162895,
      3488423249073361239,  2468357678000047660,  -822014943607608290,
      1077840338944360789,  3495214680164826339,  2311636616347925110,
      -2693986505672702943, -1521982011329959414, 2849585075899255779,
      -4106806763301532059, 343580579568434023,   -2142328270988287386,
      -2381969911843852688, 1548034562868364887,  716120660878171164,
      975600295877738907,   -747643276509912289,  3032995917932290081,
      1229819721809378571,  -4055924379371156,    -3860879174337141982,
      -746705968246949003,  -2796869234222034924, 2182094310509113306,
      -331657803362759241,  1730787742672177087,  2752140131846221176,
      -2841302389887200544, 3099834237916118331,  -1850107336129540535,
      519317033105170321,   2108161379561160391,  -3874950458569978709,
      -1316314198386305125, -3441985100832796708, -377377397989843353,
      -3329869706591572468, 2659986398742734271,  -2246442506911605788,
      -2882843505232232835, -1129217049262698310, 1106078498883512375,
      -574429026149420887,  -3609088772779672627, 397050866456055143,
      82612562527533442,    -447854835726940743,  -4089254320966862152,
      599197015742142686,   300602405590819335,   3552320182152235223,
      402529888629354023,   -3858895723825100947, -3374235604402870846,
      -2268679892511698839, -1115441984244964127, 1170463065856219575,
      -3377593351906231499, 3113168663266051921,  -2243842392496177849,
      3232551808792352028,  -3828700932470490017, -983806833403418515,
      2850874103047401708,  -1390645660620021813, -3960675925237155134,
      2390965438855311360,  2816075521207118016,  -764480061292291538,
      -933575467144789820,  3263782627709941011,  -1048306148206776622,
      -1699576589286236275, 3053775049985214804,  3523940080100718438,
      -3739474315308282875, -1072753983903695042, -2493534651468674041,
      -3305026400980053351, -2904253291899222249, -2714674646838230337,
      -285152942888192829,  -3752972194557492457, 2581714570794376936,
      -120063254688927395,  1537456350427195180,  1970060445938851185,
      2741428563971144396,  2162983509366227394,  -741188389166061201,
      383645343267192080,   1299953338388383966,  -209370592183556583,
      1846409895362509214,  -291408435754116595,  -337379534490685280,
      -1413941606634873857, 1530837761430317470,  -1077014450473202418,
      3269959976221369314,  -3381424569770796385, 171807035572019097,
      -242280810095109506,  3726999568732745839,  3160973391757073894,
      450102226440705496,   -4198754889493102203, 869220591414230296,
      -738230571928239382,  808652721469395313,   1103077095683285705,
      1892731970033918994,  -3654195692037936119, 1623180259281396232,
      2328632552927689839,  -4112683426153544399, -3983370639841600420,
      -1018017574633416879, -3096494739898026741, -4119701449344935146,
      -2719939747289270075, -4037899885598872264, -1821434081383140534,
      1007039386125060694,  339593611882672951,   2458497514761763181,
      3066054721715714581,  -3482478184461022184, 1695468277255155300,
      -3573433875801627773, 2073486054182543555,  -830494538674462560,
      -2648827844136128157, 1112138630712561706,  -1258264018511439756,
      3323052165703682016,  -760036865030755085,  -499318521488981687,
      2281856110533339189,  -4130893804003070422, 255735891474680078,
      -3291398524029856906, 3120490756917863737,  3292425613615868394,
      1443247441838798333,  354991241758098346,   -2083628637168979770,
      2800712125782371753,  -1100833476060686088, 3472539153405479266,
      -571017574906652784,  989338445581025226,   -1385098446996831051,
      -396649770072344113,  -1828177269694513061, 1650598080741728097,
      610429647218343811,   -238952199852858428,  1616326600558404185,
      -693475856055737249,  1466288264801257792,  -496830869401189910,
      -4102883476487885055, 2037867090732177935,  -3566473639089528831,
      3327893001789487182,  1183731719471307737,  378635704758601628,
      -3360483671074720510, 1177476441423928145,  -2614123464568194926,
      -3270136834395820385, -4163455559007090192, -2851082332718085669,
      -2685304669396353338, -3740757160752256234, 110384368665568499,
      3100605744633285836,  -138536211433508660,  945068880999993150,
      3297581309278827815,  -572911464339479138,  1305895547859994200,
      -149296799245007380,  -3723526862970200800, 2388547471252253416,
      -1939161161629734358, -1816362139431133540, -101686332779152333,
      3416523440297694761,  -2233597298640757658, -248138166332244132,
      2371539129626988523,  1295309259223227671,  3674358322537850328,
      3713690504617078469,  -2671728454241333551, 1367645998522217952,
      760616951619936464,   2735028138005601132,  -2794538436656089616,
      -718898473128523763,  1094684722771890234,  1855276870638474901,
      -3262085760975163967, 1104915368256135002,  -3280296788751011309,
      1170130654298714315,  2244317651663955172,  -2076307500116102703,
      -540292874862862714,  -1583504336446407615, 1608065030004397648,
      -765643464446817545,  -1438151107675587289, -2456378647597928433,
      471573977488971920,   -1440982867866095380, -1315036042433497710,
      -2902083165060319577, 3690034399169621993,  -429553646567953039,
      -3218327350025418644, 3372067765362704550,  2992202154324638757,
      -2283957139127361589, 1052494126570858618,  -1166750073638676161,
      -3842250181805184710, -3007796900861924699, 2889156626910841606,
      1594362368830281129,  780221885098990224,   -2701479561532278580,
      2318083926104385396,  642051634877596947,   1412939074604759173,
      3670779299756799242,  2789343318676643265,  673103020327183380,
      -202678532719027039,  28993786883835882,    1116876393661487720,
      1757724273274045575,  3050884125177946090,  -4125862985576607160,
      888722306003642809,   1503297524261652647,  -2139870215490514091,
      -1926748294685464549, 2249989831010024516,  -1877994503223557528,
      2220622760197971769,  506229403513130830,   1968757028688564806,
      2382271850509560869,  -1610902834190189999, -379363230200813717,
      3424901501756976554,  2662642121623173860,  3282240068139581639,
      -737800079758990251,  -1247174549859306203, 3198903300377865777,
      1490222631660612726,  1515343777936766935,  2903038684648710141,
      -1623792962580134068, -631475165303439178,  -2376226836656944988,
      -1313509580459415069, -2135575435793903732, 936803736473476573,
      1522214139801026976,  -3802218042336473486, 1541498970356429485,
      3547263023288630458,  -702345536379464630,  3442576825309506404,
      -4149629132430308853, 1973881132427987409,  -1603143979546672947,
      2735876884363448474,  -1168497439567008518, -2135775911915574104,
      -1595531555919615917, 2516042393788143204,  -3772799062916557317,
      831484274244185547,   -3765506072638422352, 1373854218025058004,
      -1522455981978017338, -1581896571916175635, -1151296606819570632,
      3527519556118078010,  1946255647907009589,  2313333066559550637,
      1441939384850741981,  3712221943516034337,  831232513512188124,
      3627621130516880478,  2909540116006443234,  -3517538516183459602,
      3540934345321518769,  3320411968361187552,  3661513930760137913,
      1398657828128548952,  1984038856804596117,  -2728060751624214799,
      -47591279857052427,   2884428930825548445,  -2366736504501696502,
      -3932256633378005890, -2264239504270151683, -2961827586729925282,
      164661800201549481,   2431386888555950057,  2778655641890437617,
      1508846771042977516,  2009374613154483799,  3384425260756424312,
      -4025111458927073524, 3555556186672173300,  344124453976580067,
      993273056042826159,   1458375589459450442,  554932281871008294,
      -250453598071847043,  2808925656219819444,  492524877045237924,
      616757054636036233,   -4132464731321633429, 1780943814873535330,
      619868617796624904,   1960840332852046080,  3508666629890149935,
      -2149212418556547303, -729089717494467261,  -1051246448270766443,
      -722647691653329233,  2412211239618907151,  2184343329850666722,
      2370128083593983498,  -2114295669121571890, -49630257820713768,
      -2140275388254679164, -1017018874794797108, 1866397009345151985,
      -1807654480926097168, -2995340866782103999, -2243430805664298055,
      599330673910319729,   559877403891637210,   465708728498330756,
      1942599523530097320,  -924783535100562415,  -280983031455684384,
      -3007634217456883117, 3274421507410109068,  -1045417049949898771,
      -2072190778290107136, 1696348313683314659,  -3224549530357122960,
      -1901475762254995359, -1863479017681502031, -2763708522604868783,
      420790933082636174,   -2756192078758949603, 47512932790894149,
      2503294364570336373,  2095463667589802189,  -2598525864459153567,
      2238810376087184251,  1830428201454932885,  53138212188021291,
      -2811957698495280684, -2472987523867429988, 2944622371252489827,
      -3218506519497133452, 1673483763541658801,  -3437927531137872603,
      -1343243726755756940, 1254341615249360377,  -72023755290404558,
      -1792908098639567833, -3957785059771247248, 2077028535668947131,
      -2355544330609724825, 833521311639913097,   3548573930682806531,
      1680625859854588259,  2514237893664977737,  175148512633177018,
      3296095356887813296,  1479195004452222340,  -2710329715454444156,
      -3528134659544357394, 3366558409958307620,  69436042792768942,
      1478567416337596046,  1728496884069493314,  -1749728889972752608,
      2936895266887747082,  1698397404943499909,  -2322513979013960557,
      2659153665376298659,  1407074403225298790,  -994349008825630254,
      -295612965597598677,  1005406510322803863,  -2540363785511619583,
      -404356222137173267,  2425205000934366595,  2713157191479384685,
      1151849711983243242,  300247273222060349,   2301841381405901933,
      -3656227695005377155, 391968594052775339,   -2996057256883553969,
      -3364816453717242296, -4163090969306970841, 2645566779986861844,
      -3409679450307517965, -1912349524053027474, -1392065589140030674,
      3329348326923773755,  2085524097936524312,  442617362623784390,
      -3383178520514441581, 1132218398279677042,  -2969730359799114196,
      498883255500048449,   -507868229194991891,  1359013366163885575,
      891196454606109146,   3261473978591868923,  3490253119908547853,
      2850888633304346932,  -52706244369792501,   -284456024623900725,
      1573017218281446927,  365348110422092705,   3058753570808706574,
      -151486525207895375,  719160046335254611,   -875338468236182312,
      1018442588345693650,  2290099953516086012,  -1310380706909709030,
      -2099722220453474104, -595719179528544982,  2475769461219550459,
      -616409737441545903,  -1705029048001549399, 1802879711760157730,
      675674168123179495,   2668369203919779676,  -825484075183426206,
      -106050099417005592,  3008981089234597986,  2903250540990431194,
      151080666507760502,   586699600953924529,   2101136276772566705,
      1924559876955384431,  -3500819300401879240, -439646310936063345,
      -3318342765881547766, -3235377565565843439, -1191163038519081153,
      698713253349570423,   -3723288005093536508, -1832422496312632848,
      2879861704917894644,  2527424969739657355,  -1569069881070066015,
      -3049268178381991536, 1554785199019903433,  1784469321657239187,
      3601456334494951911,  3313035645018337813,  -349741495849773405,
      -3985073553686148362, 128967565014146054,   -3940707630406390948,
      -2557093971871986678, -829231821204495918,  3239327299911660499,
      1745891165159267982,  -282650427105833111,  -1005869426985269548,
      3560391784970288547,  -3456143443453695,    1394490532805699105,
      1469501007598587204,
  };

  VecDataGenerator<tiledb::test::AsserterCatch> tilegen(
      Datatype::CHAR,
      std::span(reinterpret_cast<const uint8_t*>(&data[0]), sizeof(data)));

  auto memory_tracker = tiledb::test::create_test_memory_tracker();
  auto&& [tile, offsets_tile] = tilegen.create_writer_tiles(memory_tracker);

  Config config;
  ThreadPool threadpool(1);
  check_run_pipeline_roundtrip(
      config, threadpool, tile, offsets_tile, p, &tilegen, memory_tracker);
}
