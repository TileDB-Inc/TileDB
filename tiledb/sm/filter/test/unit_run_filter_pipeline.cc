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
 */

#include <algorithm>
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
#include "filter_test_support.h"
#include "pseudo_checksum_filter.h"
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

WriterTile make_increasing_tile(const uint64_t nelts) {
  const uint64_t tile_size = nelts * sizeof(uint64_t);
  const uint64_t cell_size = sizeof(uint64_t);

  WriterTile tile(
      constants::format_version, Datatype::UINT64, cell_size, tile_size);
  for (uint64_t i = 0; i < nelts; i++) {
    CHECK_NOTHROW(tile.write(&i, i * sizeof(uint64_t), sizeof(uint64_t)));
  }

  return tile;
}

WriterTile make_offsets_tile(std::vector<uint64_t>& offsets) {
  const uint64_t offsets_tile_size =
      offsets.size() * constants::cell_var_offset_size;

  WriterTile offsets_tile(
      constants::format_version,
      Datatype::UINT64,
      constants::cell_var_offset_size,
      offsets_tile_size);

  // Set up test data
  for (uint64_t i = 0; i < offsets.size(); i++) {
    CHECK_NOTHROW(offsets_tile.write(
        &offsets[i],
        i * constants::cell_var_offset_size,
        constants::cell_var_offset_size));
  }

  return offsets_tile;
}

Tile create_tile_for_unfiltering(uint64_t nelts, WriterTile& tile) {
  Tile ret(
      tile.format_version(),
      tile.type(),
      tile.cell_size(),
      0,
      tile.cell_size() * nelts,
      tile.filtered_buffer().data(),
      tile.filtered_buffer().size());
  return ret;
}

void run_reverse(
    const tiledb::sm::Config& config,
    ThreadPool& tp,
    Tile& unfiltered_tile,
    FilterPipeline& pipeline,
    bool success = true) {
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

TEST_CASE("Filter: Test empty pipeline", "[filter][empty-pipeline]") {
  tiledb::sm::Config config;

  // Set up test data
  const uint64_t nelts = 100;
  auto tile = make_increasing_tile(nelts);

  FilterPipeline pipeline;
  ThreadPool tp(4);
  GridChunkChecker<uint64_t> filtered_chunk_checker{
      nelts * sizeof(uint64_t), nelts, 0, 1};
  GridTileChecker<uint64_t> unfiltered_tile_checker{nelts, 0, 1};

  CHECK(pipeline.run_forward(&dummy_stats, &tile, nullptr, &tp).ok());

  // Check size and number of chunks
  CHECK(tile.size() == 0);

  const auto& filtered_buffer = tile.filtered_buffer();
  FilteredBufferChunkInfo buffer_chunk_info{filtered_buffer};
  auto chunk_info = buffer_chunk_info.chunk_info(0);
  CHECK(chunk_info.original_chunk_length() == nelts * sizeof(uint64_t));
  CHECK(chunk_info.filtered_chunk_length() == nelts * sizeof(uint64_t));
  CHECK(chunk_info.metadata_length() == 0);

  CHECK(filtered_buffer.size() == buffer_chunk_info.size());
  CHECK(
      filtered_buffer.size() ==
      nelts * sizeof(uint64_t) + sizeof(uint64_t) + 3 * sizeof(uint32_t));

  // Check the number of chunks.
  CHECK(buffer_chunk_info.nchunks() == 1);

  // Check the chunks.
  filtered_chunk_checker.check(filtered_buffer, buffer_chunk_info, 0);

  // Run the data in reverse.
  auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
  run_reverse(config, tp, unfiltered_tile, pipeline);

  // Check the original data is reverted.
  unfiltered_tile_checker.check(unfiltered_tile);
}

/*
TEST_CASE(
    "Filter: Test empty pipeline var sized", "[filter][empty-pipeline][var]") {
  tiledb::sm::Config config;

  // Set up test data
  const uint64_t nelts = 100;
  auto tile = make_increasing_tile(nelts);

  // TODO: HERE
  // Move all the checking for the chunks into this chunk checker array.
  std::array<tdb_unique_ptr<ChunkCheckerBase>, 9> chunk_checkers;
  chunk_checkers[0] = tdb_unique_ptr<ChunkCheckerBase>(
      tdb_new(GridChunkChecker<uint64_t>, 14 * sizeof(uint64_t), 14, 0, 1));
  chunk_checkers[1] = tdb_unique_ptr<ChunkCheckerBase>(
      tdb_new(GridChunkChecker<uint64_t>, 6 * sizeof(uint64_t), 6, 0, 1));
  chunk_checkers[2] = tdb_unique_ptr<ChunkCheckerBase>(
      tdb_new(GridChunkChecker<uint64_t>, 11 * sizeof(uint64_t), 11, 0, 1));
  chunk_checkers[3] = tdb_unique_ptr<ChunkCheckerBase>(
      tdb_new(GridChunkChecker<uint64_t>, 7 * sizeof(uint64_t), 7, 0, 1));
  chunk_checkers[4] = tdb_unique_ptr<ChunkCheckerBase>(
      tdb_new(GridChunkChecker<uint64_t>, 10 * sizeof(uint64_t), 10, 0, 1));
  chunk_checkers[5] = tdb_unique_ptr<ChunkCheckerBase>(
      tdb_new(GridChunkChecker<uint64_t>, 10 * sizeof(uint64_t), 10, 0, 1));
  chunk_checkers[6] = tdb_unique_ptr<ChunkCheckerBase>(
      tdb_new(GridChunkChecker<uint64_t>, 20 * sizeof(uint64_t), 20, 0, 1));
  chunk_checkers[7] = tdb_unique_ptr<ChunkCheckerBase>(
      tdb_new(GridChunkChecker<uint64_t>, 10 * sizeof(uint64_t), 10, 0, 1));
  chunk_checkers[8] = tdb_unique_ptr<ChunkCheckerBase>(
      tdb_new(GridChunkChecker<uint64_t>, 12 * sizeof(uint64_t), 12, 0, 1));

  // CHECK(chunk_checkers[0] != nullptr);
  // CHECK(goo != nullptr);

  // Set up test data
  std::vector<uint64_t> sizes{
      0,
      32,   // Chunk0: 4 cells.
      80,   // 10 cells, still makes it into this chunk as current size < 50%.
      48,   // Chunk1: 6 cells.
      88,   // Chunk2: 11 cells, size > 50% and > than 10 cells.
      56,   // Chunk3: 7 cells.
      72,   // Chunk4: 9 cells, size > 50%.
      8,    // Chunk4: 10 cell, full.
      80,   // Chunk5: 10 cells.
      160,  // Chunk6: 20 cells.
      16,   // Chunk7: 2 cells.
      16,   // Chunk7: 4 cells.
      16,   // Chunk7: 6 cells.
      16,   // Chunk7: 8 cells.
      16,   // Chunk7: 10 cells.
  };        // Chunk8: 12 cells.

  std::vector<uint64_t> out_sizes{112, 48, 88, 56, 80, 80, 160, 80, 96};

  std::vector<uint64_t> offsets(sizes.size());
  uint64_t offset = 0;
  for (uint64_t i = 0; i < offsets.size() - 1; i++) {
    offsets[i] = offset;
    offset += sizes[i + 1];
  }
  offsets[offsets.size() - 1] = offset;

  auto offsets_tile = make_offsets_tile(offsets);

  FilterPipeline pipeline;
  ThreadPool tp(4);
  WriterTile::set_max_tile_chunk_size(80);

  CHECK(pipeline.run_forward(&dummy_stats, &tile, &offsets_tile, &tp).ok());

  // Check all data is stored in filtered buffer.
  CHECK(tile.size() == 0);

  // Check the total size.
  CHECK(
      tile.filtered_buffer().size() ==
      nelts * sizeof(uint64_t) + sizeof(uint64_t) + 3 * 9 * sizeof(uint32_t));

  // Check the number of tiles.
  uint64_t nchunks_expected = 9;
  auto nchunks_actual = tile.filtered_buffer().value_at_as<uint64_t>(0);
  CHECK(nchunks_actual == nchunks_expected);  // Number of chunks
  auto nchunks_to_check = std::min(nchunks_expected, nchunks_actual);

  // TODO: Get the chunk offset.
  auto filtered_buffer = tile.filtered_buffer();
  std::vector<uint64_t> chunk_offsets(nchunks_to_check);
  chunk_offsets[0] = sizeof(uint64_t);

  for (uint64_t chunk_index = 0; chunk_index < nchunks_to_check - 1;
       ++chunk_index) {
    auto actual_chunk_size =
        chunk_checkers[chunk_index]->actual_total_chunk_size(
            filtered_buffer, chunk_offsets[chunk_index]);
    auto expected_chunk_size =
        chunk_checkers[chunk_index]->expected_total_chunk_size();
    CHECK(actual_chunk_size == expected_chunk_size);

    chunk_offsets[chunk_index + 1] = expected_chunk_size;
  }

  uint64_t el = 0;
  offset = sizeof(uint64_t);
  for (uint64_t i = 0; i < 9; i++) {
    CHECK(
        tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
        out_sizes[i]);  // Chunk orig size
    offset += sizeof(uint32_t);
    CHECK(
        tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
        out_sizes[i]);  // Chunk filtered size
    offset += sizeof(uint32_t);
    CHECK(
        tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
        0);  // Chunk metadata size
    offset += sizeof(uint32_t);

    // Check all elements unchanged.
    for (uint64_t j = 0; j < out_sizes[i] / sizeof(uint64_t); j++) {
      CHECK(tile.filtered_buffer().value_at_as<uint64_t>(offset) == el++);
      offset += sizeof(uint64_t);
    }
  }

  auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
  run_reverse(config, tp, unfiltered_tile, pipeline);
  for (uint64_t i = 0; i < nelts; i++) {
    uint64_t elt = 0;
    CHECK_NOTHROW(
        unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
    CHECK(elt == i);
  }

  WriterTile::set_max_tile_chunk_size(constants::max_tile_chunk_size);
}
*/

TEST_CASE(
    "Filter: Test simple in-place pipeline", "[filter][simple-in-place]") {
  tiledb::sm::Config config;

  const uint64_t nelts = 100;
  auto tile = make_increasing_tile(nelts);

  FilterPipeline pipeline;
  ThreadPool tp(4);
  pipeline.add_filter(Add1InPlace(Datatype::UINT64));

  SECTION("- Single stage") {
    CHECK(pipeline.run_forward(&dummy_stats, &tile, nullptr, &tp).ok());

    // Check size and number of chunks
    CHECK(tile.size() == 0);
    CHECK(
        tile.filtered_buffer().size() ==
        nelts * sizeof(uint64_t) + sizeof(uint64_t) + 3 * sizeof(uint32_t));

    uint64_t offset = 0;
    CHECK(
        tile.filtered_buffer().value_at_as<uint64_t>(offset) ==
        1);  // Number of chunks
    offset += sizeof(uint64_t);
    CHECK(
        tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
        nelts * sizeof(uint64_t));  // First chunk orig size
    offset += sizeof(uint32_t);
    CHECK(
        tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
        nelts * sizeof(uint64_t));  // First chunk filtered size
    offset += sizeof(uint32_t);
    CHECK(
        tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
        0);  // First chunk metadata size
    offset += sizeof(uint32_t);

    // Check all elements incremented.
    for (uint64_t i = 0; i < nelts; i++) {
      CHECK(tile.filtered_buffer().value_at_as<uint64_t>(offset) == (i + 1));
      offset += sizeof(uint64_t);
    }

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
    run_reverse(config, tp, unfiltered_tile, pipeline);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
      CHECK(elt == i);
    }
  }

  SECTION("- Multi-stage") {
    // Add a few more +1 filters and re-run.
    pipeline.add_filter(Add1InPlace(Datatype::UINT64));
    pipeline.add_filter(Add1InPlace(Datatype::UINT64));
    CHECK(pipeline.run_forward(&dummy_stats, &tile, nullptr, &tp).ok());

    // Check size and number of chunks
    CHECK(tile.size() == 0);
    CHECK(
        tile.filtered_buffer().size() ==
        nelts * sizeof(uint64_t) + sizeof(uint64_t) + 3 * sizeof(uint32_t));

    uint64_t offset = 0;
    CHECK(
        tile.filtered_buffer().value_at_as<uint64_t>(offset) ==
        1);  // Number of chunks
    offset += sizeof(uint64_t);
    CHECK(
        tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
        nelts * sizeof(uint64_t));  // First chunk orig size
    offset += sizeof(uint32_t);
    CHECK(
        tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
        nelts * sizeof(uint64_t));  // First chunk filtered size
    offset += sizeof(uint32_t);
    CHECK(
        tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
        0);  // First chunk metadata size
    offset += sizeof(uint32_t);

    // Check all elements incremented.
    for (uint64_t i = 0; i < nelts; i++) {
      CHECK(tile.filtered_buffer().value_at_as<uint64_t>(offset) == (i + 3));
      offset += sizeof(uint64_t);
    }

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
    run_reverse(config, tp, unfiltered_tile, pipeline);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
      CHECK(elt == i);
    }
  }
}

TEST_CASE(
    "Filter: Test simple in-place pipeline var",
    "[filter][simple-in-place][var]") {
  tiledb::sm::Config config;

  const uint64_t nelts = 100;
  auto tile = make_increasing_tile(nelts);

  // Set up test data
  std::vector<uint64_t> sizes{
      0,
      32,   // Chunk0: 4 cells.
      80,   // 10 cells, still makes it into this chunk as current size < 50%.
      48,   // Chunk1: 6 cells.
      88,   // Chunk2: 11 cells, size > 50% and > than 10 cells.
      56,   // Chunk3: 7 cells.
      72,   // Chunk4: 9 cells, size > 50%.
      8,    // Chunk4: 10 cell, full.
      80,   // Chunk5: 10 cells.
      160,  // Chunk6: 20 cells.
      16,   // Chunk7: 2 cells.
      16,   // Chunk7: 4 cells.
      16,   // Chunk7: 6 cells.
      16,   // Chunk7: 8 cells.
      16,   // Chunk7: 10 cells.
  };        // Chunk8: 12 cells.

  std::vector<uint64_t> out_sizes{112, 48, 88, 56, 80, 80, 160, 80, 96};

  std::vector<uint64_t> offsets(sizes.size());
  uint64_t offset = 0;
  for (uint64_t i = 0; i < offsets.size() - 1; i++) {
    offsets[i] = offset;
    offset += sizes[i + 1];
  }
  offsets[offsets.size() - 1] = offset;

  auto offsets_tile = make_offsets_tile(offsets);

  FilterPipeline pipeline;
  ThreadPool tp(4);
  pipeline.add_filter(Add1InPlace(Datatype::UINT64));

  SECTION("- Single stage") {
    WriterTile::set_max_tile_chunk_size(80);
    CHECK(pipeline.run_forward(&dummy_stats, &tile, &offsets_tile, &tp).ok());

    // Check size and number of chunks
    CHECK(tile.size() == 0);
    CHECK(
        tile.filtered_buffer().size() ==
        nelts * sizeof(uint64_t) + sizeof(uint64_t) + 3 * 9 * sizeof(uint32_t));

    uint64_t offset = 0;
    CHECK(
        tile.filtered_buffer().value_at_as<uint64_t>(offset) ==
        9);  // Number of chunks
    offset += sizeof(uint64_t);

    uint64_t el = 0;
    for (uint64_t i = 0; i < 9; i++) {
      CHECK(
          tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
          out_sizes[i]);  // Chunk orig size
      offset += sizeof(uint32_t);
      CHECK(
          tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
          out_sizes[i]);  // Chunk filtered size
      offset += sizeof(uint32_t);
      CHECK(
          tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
          0);  // Chunk metadata size
      offset += sizeof(uint32_t);

      // Check all elements incremented.
      for (uint64_t j = 0; j < out_sizes[i] / sizeof(uint64_t); j++) {
        CHECK(tile.filtered_buffer().value_at_as<uint64_t>(offset) == ++el);
        offset += sizeof(uint64_t);
      }
    }

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
    run_reverse(config, tp, unfiltered_tile, pipeline);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
      CHECK(elt == i);
    }
  }

  SECTION("- Multi-stage") {
    // Add a few more +1 filters and re-run.
    WriterTile::set_max_tile_chunk_size(80);
    pipeline.add_filter(Add1InPlace(Datatype::UINT64));
    pipeline.add_filter(Add1InPlace(Datatype::UINT64));
    CHECK(pipeline.run_forward(&dummy_stats, &tile, &offsets_tile, &tp).ok());

    // Check size and number of chunks
    CHECK(tile.size() == 0);
    CHECK(
        tile.filtered_buffer().size() ==
        nelts * sizeof(uint64_t) + sizeof(uint64_t) + 3 * 9 * sizeof(uint32_t));

    uint64_t offset = 0;
    CHECK(
        tile.filtered_buffer().value_at_as<uint64_t>(offset) ==
        9);  // Number of chunks
    offset += sizeof(uint64_t);

    uint64_t el = 0;
    for (uint64_t i = 0; i < 9; i++) {
      CHECK(
          tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
          out_sizes[i]);  // Chunk orig size
      offset += sizeof(uint32_t);
      CHECK(
          tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
          out_sizes[i]);  // Chunk filtered size
      offset += sizeof(uint32_t);
      CHECK(
          tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
          0);  // Chunk metadata size
      offset += sizeof(uint32_t);

      // Check all elements incremented.
      for (uint64_t j = 0; j < out_sizes[i] / sizeof(uint64_t); j++) {
        CHECK(tile.filtered_buffer().value_at_as<uint64_t>(offset) == ++el + 2);
        offset += sizeof(uint64_t);
      }
    }

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
    run_reverse(config, tp, unfiltered_tile, pipeline);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
      CHECK(elt == i);
    }
  }

  WriterTile::set_max_tile_chunk_size(constants::max_tile_chunk_size);
}

TEST_CASE(
    "Filter: Test simple out-of-place pipeline",
    "[filter][simple-out-of-place]") {
  tiledb::sm::Config config;

  // Set up test data
  const uint64_t nelts = 100;
  auto tile = make_increasing_tile(nelts);

  FilterPipeline pipeline;
  ThreadPool tp(4);
  pipeline.add_filter(Add1OutOfPlace(Datatype::UINT64));

  SECTION("- Single stage") {
    CHECK(pipeline.run_forward(&dummy_stats, &tile, nullptr, &tp).ok());

    // Check size and number of chunks
    CHECK(tile.size() == 0);
    CHECK(
        tile.filtered_buffer().size() ==
        nelts * sizeof(uint64_t) + sizeof(uint64_t) + 3 * sizeof(uint32_t));

    uint64_t offset = 0;
    CHECK(
        tile.filtered_buffer().value_at_as<uint64_t>(offset) ==
        1);  // Number of chunks
    offset += sizeof(uint64_t);
    CHECK(
        tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
        nelts * sizeof(uint64_t));  // First chunk orig size
    offset += sizeof(uint32_t);
    CHECK(
        tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
        nelts * sizeof(uint64_t));  // First chunk filtered size
    offset += sizeof(uint32_t);
    CHECK(
        tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
        0);  // First chunk metadata size
    offset += sizeof(uint32_t);

    // Check all elements incremented.
    for (uint64_t i = 0; i < nelts; i++) {
      CHECK(tile.filtered_buffer().value_at_as<uint64_t>(offset) == (i + 1));
      offset += sizeof(uint64_t);
    }

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
    run_reverse(config, tp, unfiltered_tile, pipeline);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
      CHECK(elt == i);
    }
  }

  SECTION("- Multi-stage") {
    // Add a few more +1 filters and re-run.
    pipeline.add_filter(Add1OutOfPlace(Datatype::UINT64));
    pipeline.add_filter(Add1OutOfPlace(Datatype::UINT64));
    CHECK(pipeline.run_forward(&dummy_stats, &tile, nullptr, &tp).ok());

    // Check size and number of chunks
    CHECK(tile.size() == 0);
    CHECK(
        tile.filtered_buffer().size() ==
        nelts * sizeof(uint64_t) + sizeof(uint64_t) + 3 * sizeof(uint32_t));

    uint64_t offset = 0;
    CHECK(
        tile.filtered_buffer().value_at_as<uint64_t>(offset) ==
        1);  // Number of chunks
    offset += sizeof(uint64_t);
    CHECK(
        tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
        nelts * sizeof(uint64_t));  // First chunk orig size
    offset += sizeof(uint32_t);
    CHECK(
        tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
        nelts * sizeof(uint64_t));  // First chunk filtered size
    offset += sizeof(uint32_t);
    CHECK(
        tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
        0);  // First chunk metadata size
    offset += sizeof(uint32_t);

    // Check all elements incremented.
    for (uint64_t i = 0; i < nelts; i++) {
      CHECK(tile.filtered_buffer().value_at_as<uint64_t>(offset) == (i + 3));
      offset += sizeof(uint64_t);
    }

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
    run_reverse(config, tp, unfiltered_tile, pipeline);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
      CHECK(elt == i);
    }
  }
}

TEST_CASE(
    "Filter: Test simple out-of-place pipeline var",
    "[filter][simple-out-of-place][var]") {
  tiledb::sm::Config config;

  const uint64_t nelts = 100;
  auto tile = make_increasing_tile(nelts);

  // Set up test data
  std::vector<uint64_t> sizes{
      0,
      32,   // Chunk0: 4 cells.
      80,   // 10 cells, still makes it into this chunk as current size < 50%.
      48,   // Chunk1: 6 cells.
      88,   // Chunk2: 11 cells, size > 50% and > than 10 cells.
      56,   // Chunk3: 7 cells.
      72,   // Chunk4: 9 cells, size > 50%.
      8,    // Chunk4: 10 cell, full.
      80,   // Chunk5: 10 cells.
      160,  // Chunk6: 20 cells.
      16,   // Chunk7: 2 cells.
      16,   // Chunk7: 4 cells.
      16,   // Chunk7: 6 cells.
      16,   // Chunk7: 8 cells.
      16,   // Chunk7: 10 cells.
  };        // Chunk8: 12 cells.

  std::vector<uint64_t> out_sizes{112, 48, 88, 56, 80, 80, 160, 80, 96};

  std::vector<uint64_t> offsets(sizes.size());
  uint64_t offset = 0;
  for (uint64_t i = 0; i < offsets.size() - 1; i++) {
    offsets[i] = offset;
    offset += sizes[i + 1];
  }
  offsets[offsets.size() - 1] = offset;

  auto offsets_tile = make_offsets_tile(offsets);

  FilterPipeline pipeline;
  ThreadPool tp(4);
  pipeline.add_filter(Add1OutOfPlace(Datatype::UINT64));

  SECTION("- Single stage") {
    WriterTile::set_max_tile_chunk_size(80);
    CHECK(pipeline.run_forward(&dummy_stats, &tile, &offsets_tile, &tp).ok());

    // Check size and number of chunks
    CHECK(tile.size() == 0);
    CHECK(
        tile.filtered_buffer().size() ==
        nelts * sizeof(uint64_t) + sizeof(uint64_t) + 3 * 9 * sizeof(uint32_t));

    uint64_t offset = 0;
    CHECK(
        tile.filtered_buffer().value_at_as<uint64_t>(offset) ==
        9);  // Number of chunks
    offset += sizeof(uint64_t);

    uint64_t el = 0;
    for (uint64_t i = 0; i < 9; i++) {
      CHECK(
          tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
          out_sizes[i]);  // Chunk orig size
      offset += sizeof(uint32_t);
      CHECK(
          tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
          out_sizes[i]);  // Chunk filtered size
      offset += sizeof(uint32_t);
      CHECK(
          tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
          0);  // Chunk metadata size
      offset += sizeof(uint32_t);

      // Check all elements incremented.
      for (uint64_t j = 0; j < out_sizes[i] / sizeof(uint64_t); j++) {
        CHECK(tile.filtered_buffer().value_at_as<uint64_t>(offset) == ++el);
        offset += sizeof(uint64_t);
      }
    }

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
    run_reverse(config, tp, unfiltered_tile, pipeline);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
      CHECK(elt == i);
    }
  }

  SECTION("- Multi-stage") {
    // Add a few more +1 filters and re-run.
    WriterTile::set_max_tile_chunk_size(80);
    pipeline.add_filter(Add1OutOfPlace(Datatype::UINT64));
    pipeline.add_filter(Add1OutOfPlace(Datatype::UINT64));
    CHECK(pipeline.run_forward(&dummy_stats, &tile, &offsets_tile, &tp).ok());

    // Check size and number of chunks
    CHECK(tile.size() == 0);
    CHECK(
        tile.filtered_buffer().size() ==
        nelts * sizeof(uint64_t) + sizeof(uint64_t) + 3 * 9 * sizeof(uint32_t));

    uint64_t offset = 0;
    CHECK(
        tile.filtered_buffer().value_at_as<uint64_t>(offset) ==
        9);  // Number of chunks
    offset += sizeof(uint64_t);

    uint64_t el = 0;
    for (uint64_t i = 0; i < 9; i++) {
      CHECK(
          tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
          out_sizes[i]);  // Chunk orig size
      offset += sizeof(uint32_t);
      CHECK(
          tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
          out_sizes[i]);  // Chunk filtered size
      offset += sizeof(uint32_t);
      CHECK(
          tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
          0);  // Chunk metadata size
      offset += sizeof(uint32_t);

      // Check all elements incremented.
      for (uint64_t j = 0; j < out_sizes[i] / sizeof(uint64_t); j++) {
        CHECK(tile.filtered_buffer().value_at_as<uint64_t>(offset) == ++el + 2);
        offset += sizeof(uint64_t);
      }
    }

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
    run_reverse(config, tp, unfiltered_tile, pipeline);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
      CHECK(elt == i);
    }
  }

  WriterTile::set_max_tile_chunk_size(constants::max_tile_chunk_size);
}

TEST_CASE(
    "Filter: Test mixed in- and out-of-place pipeline",
    "[filter][in-out-place]") {
  tiledb::sm::Config config;

  // Set up test data
  const uint64_t nelts = 100;
  auto tile = make_increasing_tile(nelts);

  FilterPipeline pipeline;
  ThreadPool tp(4);
  pipeline.add_filter(Add1InPlace(Datatype::UINT64));
  pipeline.add_filter(Add1OutOfPlace(Datatype::UINT64));
  pipeline.add_filter(Add1InPlace(Datatype::UINT64));
  pipeline.add_filter(Add1OutOfPlace(Datatype::UINT64));
  CHECK(pipeline.run_forward(&dummy_stats, &tile, nullptr, &tp).ok());

  CHECK(tile.size() == 0);
  CHECK(
      tile.filtered_buffer().size() ==
      nelts * sizeof(uint64_t) + sizeof(uint64_t) + 3 * sizeof(uint32_t));

  uint64_t offset = 0;
  CHECK(
      tile.filtered_buffer().value_at_as<uint64_t>(offset) ==
      1);  // Number of chunks
  offset += sizeof(uint64_t);
  CHECK(
      tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
      nelts * sizeof(uint64_t));  // First chunk orig size
  offset += sizeof(uint32_t);
  CHECK(
      tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
      nelts * sizeof(uint64_t));  // First chunk filtered size
  offset += sizeof(uint32_t);
  CHECK(
      tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
      0);  // First chunk metadata size
  offset += sizeof(uint32_t);

  // Check all elements incremented.
  for (uint64_t i = 0; i < nelts; i++) {
    CHECK(tile.filtered_buffer().value_at_as<uint64_t>(offset) == (i + 4));
    offset += sizeof(uint64_t);
  }

  auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
  run_reverse(config, tp, unfiltered_tile, pipeline);
  for (uint64_t i = 0; i < nelts; i++) {
    uint64_t elt = 0;
    CHECK_NOTHROW(
        unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
    CHECK(elt == i);
  }
}

TEST_CASE(
    "Filter: Test mixed in- and out-of-place pipeline var",
    "[filter][in-out-place][var]") {
  tiledb::sm::Config config;

  const uint64_t nelts = 100;
  auto tile = make_increasing_tile(nelts);

  // Set up test data
  std::vector<uint64_t> sizes{
      0,
      32,   // Chunk0: 4 cells.
      80,   // 10 cells, still makes it into this chunk as current size < 50%.
      48,   // Chunk1: 6 cells.
      88,   // Chunk2: 11 cells, size > 50% and > than 10 cells.
      56,   // Chunk3: 7 cells.
      72,   // Chunk4: 9 cells, size > 50%.
      8,    // Chunk4: 10 cell, full.
      80,   // Chunk5: 10 cells.
      160,  // Chunk6: 20 cells.
      16,   // Chunk7: 2 cells.
      16,   // Chunk7: 4 cells.
      16,   // Chunk7: 6 cells.
      16,   // Chunk7: 8 cells.
      16,   // Chunk7: 10 cells.
  };        // Chunk8: 12 cells.

  std::vector<uint64_t> out_sizes{112, 48, 88, 56, 80, 80, 160, 80, 96};

  std::vector<uint64_t> offsets(sizes.size());
  uint64_t offset = 0;
  for (uint64_t i = 0; i < offsets.size() - 1; i++) {
    offsets[i] = offset;
    offset += sizes[i + 1];
  }
  offsets[offsets.size() - 1] = offset;

  auto offsets_tile = make_offsets_tile(offsets);

  FilterPipeline pipeline;
  ThreadPool tp(4);
  WriterTile::set_max_tile_chunk_size(80);
  pipeline.add_filter(Add1InPlace(Datatype::UINT64));
  pipeline.add_filter(Add1OutOfPlace(Datatype::UINT64));
  pipeline.add_filter(Add1InPlace(Datatype::UINT64));
  pipeline.add_filter(Add1OutOfPlace(Datatype::UINT64));
  CHECK(pipeline.run_forward(&dummy_stats, &tile, &offsets_tile, &tp).ok());

  // Check size and number of chunks
  CHECK(tile.size() == 0);
  CHECK(
      tile.filtered_buffer().size() ==
      nelts * sizeof(uint64_t) + sizeof(uint64_t) + 3 * 9 * sizeof(uint32_t));

  offset = 0;
  CHECK(
      tile.filtered_buffer().value_at_as<uint64_t>(offset) ==
      9);  // Number of chunks
  offset += sizeof(uint64_t);

  uint64_t el = 0;
  for (uint64_t i = 0; i < 9; i++) {
    CHECK(
        tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
        out_sizes[i]);  // Chunk orig size
    offset += sizeof(uint32_t);
    CHECK(
        tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
        out_sizes[i]);  // Chunk filtered size
    offset += sizeof(uint32_t);
    CHECK(
        tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
        0);  // Chunk metadata size
    offset += sizeof(uint32_t);

    // Check all elements incremented.
    for (uint64_t j = 0; j < out_sizes[i] / sizeof(uint64_t); j++) {
      CHECK(tile.filtered_buffer().value_at_as<uint64_t>(offset) == ++el + 3);
      offset += sizeof(uint64_t);
    }
  }

  auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
  run_reverse(config, tp, unfiltered_tile, pipeline);
  for (uint64_t i = 0; i < nelts; i++) {
    uint64_t elt = 0;
    CHECK_NOTHROW(
        unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
    CHECK(elt == i);
  }

  WriterTile::set_max_tile_chunk_size(constants::max_tile_chunk_size);
}

TEST_CASE("Filter: Test pseudo-checksum", "[filter][pseudo-checksum]") {
  tiledb::sm::Config config;

  // Set up test data
  const uint64_t nelts = 100;
  const uint64_t expected_checksum = 4950;
  auto tile = make_increasing_tile(nelts);

  FilterPipeline pipeline;
  ThreadPool tp(4);
  pipeline.add_filter(PseudoChecksumFilter(Datatype::UINT64));

  SECTION("- Single stage") {
    CHECK(pipeline.run_forward(&dummy_stats, &tile, nullptr, &tp).ok());

    // Check size and number of chunks
    CHECK(tile.size() == 0);
    CHECK(
        tile.filtered_buffer().size() ==
        nelts * sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t) +
            3 * sizeof(uint32_t));

    uint64_t offset = 0;
    CHECK(
        tile.filtered_buffer().value_at_as<uint64_t>(offset) ==
        1);  // Number of chunks
    offset += sizeof(uint64_t);
    CHECK(
        tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
        nelts * sizeof(uint64_t));  // First chunk orig size
    offset += sizeof(uint32_t);
    CHECK(
        tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
        nelts * sizeof(uint64_t));  // First chunk filtered size
    offset += sizeof(uint32_t);
    CHECK(
        tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
        sizeof(uint64_t));  // First chunk metadata size
    offset += sizeof(uint32_t);

    // Checksum
    CHECK(
        tile.filtered_buffer().value_at_as<uint64_t>(offset) ==
        expected_checksum);
    offset += sizeof(uint64_t);

    // Check all elements are the same.
    for (uint64_t i = 0; i < nelts; i++) {
      CHECK(tile.filtered_buffer().value_at_as<uint64_t>(offset) == i);
      offset += sizeof(uint64_t);
    }

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
    run_reverse(config, tp, unfiltered_tile, pipeline);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
      CHECK(elt == i);
    }
  }

  SECTION("- Multi-stage") {
    pipeline.add_filter(Add1OutOfPlace(Datatype::UINT64));
    pipeline.add_filter(Add1InPlace(Datatype::UINT64));
    pipeline.add_filter(PseudoChecksumFilter(Datatype::UINT64));
    CHECK(pipeline.run_forward(&dummy_stats, &tile, nullptr, &tp).ok());

    // Compute the second (final) checksum value.
    uint64_t expected_checksum_2 = 0;
    for (uint64_t i = 0; i < nelts; i++)
      expected_checksum_2 += i + 2;

    // Check size and number of chunks.
    CHECK(tile.size() == 0);
    CHECK(
        tile.filtered_buffer().size() ==
        nelts * sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t) +
            sizeof(uint64_t) + 3 * sizeof(uint32_t));

    uint64_t offset = 0;
    CHECK(
        tile.filtered_buffer().value_at_as<uint64_t>(offset) ==
        1);  // Number of chunks
    offset += sizeof(uint64_t);
    CHECK(
        tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
        nelts * sizeof(uint64_t));  // First chunk orig size
    offset += sizeof(uint32_t);
    CHECK(
        tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
        nelts * sizeof(uint64_t));  // First chunk filtered size
    offset += sizeof(uint32_t);
    CHECK(
        tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
        2 * sizeof(uint64_t));  // First chunk metadata size
    offset += sizeof(uint32_t);

    // Outer checksum
    CHECK(
        tile.filtered_buffer().value_at_as<uint64_t>(offset) ==
        expected_checksum_2);
    offset += sizeof(uint64_t);

    // Inner checksum
    CHECK(
        tile.filtered_buffer().value_at_as<uint64_t>(offset) ==
        expected_checksum);
    offset += sizeof(uint64_t);

    // Check all elements are correct.
    for (uint64_t i = 0; i < nelts; i++) {
      CHECK(tile.filtered_buffer().value_at_as<uint64_t>(offset) == i + 2);
      offset += sizeof(uint64_t);
    }

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
    run_reverse(config, tp, unfiltered_tile, pipeline);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
      CHECK(elt == i);
    }
  }
}

TEST_CASE(
    "Filter: Test pseudo-checksum var", "[filter][pseudo-checksum][var]") {
  tiledb::sm::Config config;

  const uint64_t nelts = 100;
  auto tile = make_increasing_tile(nelts);

  // Set up test data
  std::vector<uint64_t> sizes{
      0,
      32,   // Chunk0: 4 cells.
      80,   // 10 cells, still makes it into this chunk as current size < 50%.
      48,   // Chunk1: 6 cells.
      88,   // Chunk2: 11 cells, size > 50% and > than 10 cells.
      56,   // Chunk3: 7 cells.
      72,   // Chunk4: 9 cells, size > 50%.
      8,    // Chunk4: 10 cell, full.
      80,   // Chunk5: 10 cells.
      160,  // Chunk6: 20 cells.
      16,   // Chunk7: 2 cells.
      16,   // Chunk7: 4 cells.
      16,   // Chunk7: 6 cells.
      16,   // Chunk7: 8 cells.
      16,   // Chunk7: 10 cells.
  };        // Chunk8: 12 cells.

  std::vector<uint64_t> out_sizes{112, 48, 88, 56, 80, 80, 160, 80, 96};

  std::vector<uint64_t> offsets(sizes.size());
  uint64_t offset = 0;
  for (uint64_t i = 0; i < offsets.size() - 1; i++) {
    offsets[i] = offset;
    offset += sizes[i + 1];
  }
  offsets[offsets.size() - 1] = offset;

  auto offsets_tile = make_offsets_tile(offsets);

  std::vector<uint64_t> expected_checksums{
      91, 99, 275, 238, 425, 525, 1350, 825, 1122};

  FilterPipeline pipeline;
  ThreadPool tp(4);
  pipeline.add_filter(PseudoChecksumFilter(Datatype::UINT64));

  SECTION("- Single stage") {
    WriterTile::set_max_tile_chunk_size(80);
    CHECK(pipeline.run_forward(&dummy_stats, &tile, &offsets_tile, &tp).ok());

    // Check size and number of chunks
    CHECK(tile.size() == 0);
    CHECK(
        tile.filtered_buffer().size() ==
        nelts * sizeof(uint64_t) + sizeof(uint64_t) + 3 * 9 * sizeof(uint32_t) +
            9 * sizeof(uint64_t));

    uint64_t offset = 0;
    CHECK(
        tile.filtered_buffer().value_at_as<uint64_t>(offset) ==
        9);  // Number of chunks
    offset += sizeof(uint64_t);

    uint64_t el = 0;
    for (uint64_t i = 0; i < 9; i++) {
      CHECK(
          tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
          out_sizes[i]);  // Chunk orig size
      offset += sizeof(uint32_t);
      CHECK(
          tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
          out_sizes[i]);  // Chunk filtered size
      offset += sizeof(uint32_t);
      CHECK(
          tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
          sizeof(uint64_t));  // Chunk metadata size
      offset += sizeof(uint32_t);

      // Checksum
      CHECK(
          tile.filtered_buffer().value_at_as<uint64_t>(offset) ==
          expected_checksums[i]);
      offset += sizeof(uint64_t);

      // Check all elements are the same.
      for (uint64_t j = 0; j < out_sizes[i] / sizeof(uint64_t); j++) {
        CHECK(tile.filtered_buffer().value_at_as<uint64_t>(offset) == el++);
        offset += sizeof(uint64_t);
      }
    }

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
    run_reverse(config, tp, unfiltered_tile, pipeline);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
      CHECK(elt == i);
    }
  }

  SECTION("- Multi-stage") {
    WriterTile::set_max_tile_chunk_size(80);
    pipeline.add_filter(Add1OutOfPlace(Datatype::UINT64));
    pipeline.add_filter(Add1InPlace(Datatype::UINT64));
    pipeline.add_filter(PseudoChecksumFilter(Datatype::UINT64));
    CHECK(pipeline.run_forward(&dummy_stats, &tile, &offsets_tile, &tp).ok());

    std::vector<uint64_t> expected_checksums2{
        119, 111, 297, 252, 445, 545, 1390, 845, 1146};

    // Check size and number of chunks
    CHECK(tile.size() == 0);
    CHECK(
        tile.filtered_buffer().size() ==
        nelts * sizeof(uint64_t) + sizeof(uint64_t) + 3 * 9 * sizeof(uint32_t) +
            2 * 9 * sizeof(uint64_t));

    uint64_t offset = 0;
    CHECK(
        tile.filtered_buffer().value_at_as<uint64_t>(offset) ==
        9);  // Number of chunks
    offset += sizeof(uint64_t);

    uint64_t el = 0;
    for (uint64_t i = 0; i < 9; i++) {
      CHECK(
          tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
          out_sizes[i]);  // Chunk orig size
      offset += sizeof(uint32_t);
      CHECK(
          tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
          out_sizes[i]);  // Chunk filtered size
      offset += sizeof(uint32_t);
      CHECK(
          tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
          2 * sizeof(uint64_t));  // Chunk metadata size
      offset += sizeof(uint32_t);

      // Checksums
      CHECK(
          tile.filtered_buffer().value_at_as<uint64_t>(offset) ==
          expected_checksums2[i]);
      offset += sizeof(uint64_t);
      CHECK(
          tile.filtered_buffer().value_at_as<uint64_t>(offset) ==
          expected_checksums[i]);
      offset += sizeof(uint64_t);

      // Check all elements are incremented.
      for (uint64_t j = 0; j < out_sizes[i] / sizeof(uint64_t); j++) {
        CHECK(tile.filtered_buffer().value_at_as<uint64_t>(offset) == ++el + 1);
        offset += sizeof(uint64_t);
      }
    }

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
    run_reverse(config, tp, unfiltered_tile, pipeline);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
      CHECK(elt == i);
    }
  }

  WriterTile::set_max_tile_chunk_size(constants::max_tile_chunk_size);
}

TEST_CASE("Filter: Test pipeline modify filter", "[filter][modify]") {
  tiledb::sm::Config config;

  // Set up test data
  const uint64_t nelts = 100;
  auto tile = make_increasing_tile(nelts);

  FilterPipeline pipeline;
  ThreadPool tp(4);
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

  CHECK(pipeline.run_forward(&dummy_stats, &tile, nullptr, &tp).ok());

  CHECK(tile.size() == 0);
  CHECK(tile.filtered_buffer().size() != 0);

  uint64_t offset = 0;
  CHECK(
      tile.filtered_buffer().value_at_as<uint64_t>(offset) ==
      1);  // Number of chunks
  offset += sizeof(uint64_t);
  CHECK(
      tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
      nelts * sizeof(uint64_t));  // First chunk orig size
  offset += sizeof(uint32_t);
  CHECK(
      tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
      nelts * sizeof(uint64_t));  // First chunk filtered size
  offset += sizeof(uint32_t);
  CHECK(
      tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
      0);  // First chunk metadata size
  offset += sizeof(uint32_t);

  // Check all elements incremented.
  for (uint64_t i = 0; i < nelts; i++) {
    CHECK(tile.filtered_buffer().value_at_as<uint64_t>(offset) == (i + 4));
    offset += sizeof(uint64_t);
  }

  auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
  run_reverse(config, tp, unfiltered_tile, pipeline);

  for (uint64_t i = 0; i < nelts; i++) {
    uint64_t elt = 0;
    CHECK_NOTHROW(
        unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
    CHECK(elt == i);
  }
}

TEST_CASE("Filter: Test pipeline modify filter var", "[filter][modify][var]") {
  tiledb::sm::Config config;

  const uint64_t nelts = 100;
  auto tile = make_increasing_tile(nelts);

  // Set up test data
  std::vector<uint64_t> sizes{
      0,
      32,   // Chunk0: 4 cells.
      80,   // 10 cells, still makes it into this chunk as current size < 50%.
      48,   // Chunk1: 6 cells.
      88,   // Chunk2: 11 cells, size > 50% and > than 10 cells.
      56,   // Chunk3: 7 cells.
      72,   // Chunk4: 9 cells, size > 50%.
      8,    // Chunk4: 10 cell, full.
      80,   // Chunk5: 10 cells.
      160,  // Chunk6: 20 cells.
      16,   // Chunk7: 2 cells.
      16,   // Chunk7: 4 cells.
      16,   // Chunk7: 6 cells.
      16,   // Chunk7: 8 cells.
      16,   // Chunk7: 10 cells.
  };        // Chunk8: 12 cells.

  std::vector<uint64_t> out_sizes{112, 48, 88, 56, 80, 80, 160, 80, 96};

  std::vector<uint64_t> offsets(sizes.size());
  uint64_t offset = 0;
  for (uint64_t i = 0; i < offsets.size() - 1; i++) {
    offsets[i] = offset;
    offset += sizes[i + 1];
  }
  offsets[offsets.size() - 1] = offset;

  auto offsets_tile = make_offsets_tile(offsets);

  FilterPipeline pipeline;
  ThreadPool tp(4);
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

  WriterTile::set_max_tile_chunk_size(80);
  CHECK(pipeline.run_forward(&dummy_stats, &tile, &offsets_tile, &tp).ok());

  // Check size and number of chunks
  CHECK(tile.size() == 0);
  CHECK(
      tile.filtered_buffer().size() ==
      nelts * sizeof(uint64_t) + sizeof(uint64_t) + 3 * 9 * sizeof(uint32_t));

  offset = 0;
  CHECK(
      tile.filtered_buffer().value_at_as<uint64_t>(offset) ==
      9);  // Number of chunks
  offset += sizeof(uint64_t);

  uint64_t el = 0;
  for (uint64_t i = 0; i < 9; i++) {
    CHECK(
        tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
        out_sizes[i]);  // Chunk orig size
    offset += sizeof(uint32_t);
    CHECK(
        tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
        out_sizes[i]);  // Chunk filtered size
    offset += sizeof(uint32_t);
    CHECK(
        tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
        0);  // Chunk metadata size
    offset += sizeof(uint32_t);

    // Check all elements are incremented.
    for (uint64_t j = 0; j < out_sizes[i] / sizeof(uint64_t); j++) {
      CHECK(tile.filtered_buffer().value_at_as<uint64_t>(offset) == ++el + 3);
      offset += sizeof(uint64_t);
    }
  }

  auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
  run_reverse(config, tp, unfiltered_tile, pipeline);

  for (uint64_t i = 0; i < nelts; i++) {
    uint64_t elt = 0;
    CHECK_NOTHROW(
        unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
    CHECK(elt == i);
  }

  WriterTile::set_max_tile_chunk_size(constants::max_tile_chunk_size);
}

TEST_CASE("Filter: Test pipeline copy", "[filter][copy]") {
  tiledb::sm::Config config;

  const uint64_t expected_checksum = 5350;

  const uint64_t nelts = 100;
  auto tile = make_increasing_tile(nelts);

  FilterPipeline pipeline;
  ThreadPool tp(4);
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

  CHECK(pipeline_copy.run_forward(&dummy_stats, &tile, nullptr, &tp).ok());

  CHECK(tile.size() == 0);
  CHECK(tile.filtered_buffer().size() != 0);

  uint64_t offset = 0;
  CHECK(
      tile.filtered_buffer().value_at_as<uint64_t>(offset) ==
      1);  // Number of chunks
  offset += sizeof(uint64_t);
  CHECK(
      tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
      nelts * sizeof(uint64_t));  // First chunk orig size
  offset += sizeof(uint32_t);
  CHECK(
      tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
      nelts * sizeof(uint64_t));  // First chunk filtered size
  offset += sizeof(uint32_t);
  CHECK(
      tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
      sizeof(uint64_t));  // First chunk metadata size
  offset += sizeof(uint32_t);

  // Checksum
  CHECK(
      tile.filtered_buffer().value_at_as<uint64_t>(offset) ==
      expected_checksum);
  offset += sizeof(uint64_t);

  // Check all elements incremented.
  for (uint64_t i = 0; i < nelts; i++) {
    CHECK(tile.filtered_buffer().value_at_as<uint64_t>(offset) == (i + 4));
    offset += sizeof(uint64_t);
  }

  auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
  run_reverse(config, tp, unfiltered_tile, pipeline);

  for (uint64_t i = 0; i < nelts; i++) {
    uint64_t elt = 0;
    CHECK_NOTHROW(
        unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
    CHECK(elt == i);
  }
}

TEST_CASE("Filter: Test random pipeline", "[filter][random]") {
  tiledb::sm::Config config;

  const uint64_t nelts = 100;

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

  // List of potential filters that must occur at the beginning of the pipeline.
  std::vector<std::function<tiledb::sm::Filter*(void)>> constructors_first = {
      // Pos-delta would (correctly) return error after e.g. compression.
      []() { return tdb_new(PositiveDeltaFilter, Datatype::UINT64); }};

  ThreadPool tp(4);
  for (int i = 0; i < 100; i++) {
    auto tile = make_increasing_tile(nelts);

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

    // End result should always be the same as the input.
    CHECK(pipeline.run_forward(&dummy_stats, &tile, nullptr, &tp).ok());
    CHECK(tile.size() == 0);
    CHECK(tile.filtered_buffer().size() != 0);

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
    run_reverse(config, tp, unfiltered_tile, pipeline);

    for (uint64_t n = 0; n < nelts; n++) {
      uint64_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile.read(&elt, n * sizeof(uint64_t), sizeof(uint64_t)));
      CHECK(elt == n);
    }
  }
}
