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
#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

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

  // Create tile data generator.
  IncrementTileDataGenerator<uint64_t, Datatype::UINT64> tile_data_generator(
      100);
  for (int i = 0; i < 100; i++) {
    // Create fresh input tiles.
    auto&& [tile, offsets_tile] =
        tile_data_generator.create_writer_tiles(tracker);

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
        config,
        tp,
        tile,
        offsets_tile,
        pipeline,
        &tile_data_generator,
        tracker);
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
