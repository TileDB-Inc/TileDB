/**
 * @file unit_bitshuffle_pipeline.cc
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
 * This set of unit tests checks running the filter pipeline with the bit
 * shuffle filter.
 *
 */

#include <test/support/src/mem_helpers.h>
#include <test/support/tdb_catch.h>
#include "../bitshuffle_filter.h"
#include "filter_test_support.h"

using namespace tiledb::sm;

TEST_CASE("Filter: Test bitshuffle", "[filter][bitshuffle]") {
  tiledb::sm::Config config;

  auto tracker = tiledb::test::create_test_memory_tracker();

  // Set up test data
  const uint64_t nelts = 1000;
  auto tile = make_increasing_tile(nelts, tracker);

  FilterPipeline pipeline;
  ThreadPool tp(4);
  pipeline.add_filter(BitshuffleFilter(Datatype::UINT64));

  SECTION("- Single stage") {
    pipeline.run_forward(&dummy_stats, tile.get(), nullptr, &tp);
    CHECK(tile->size() == 0);
    CHECK(tile->filtered_buffer().size() != 0);

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile, tracker);
    run_reverse(config, tp, unfiltered_tile, pipeline);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
      CHECK(elt == i);
    }
  }

  SECTION("- Indivisible by 8") {
    const uint32_t nelts2 = 1001;
    const uint64_t tile_size2 = nelts2 * sizeof(uint32_t);

    auto tile2 = make_shared<WriterTile>(
        HERE(),
        constants::format_version,
        Datatype::UINT32,
        sizeof(uint32_t),
        tile_size2,
        tracker);

    // Set up test data
    for (uint32_t i = 0; i < nelts2; i++) {
      CHECK_NOTHROW(tile2->write(&i, i * sizeof(uint32_t), sizeof(uint32_t)));
    }

    pipeline.run_forward(&dummy_stats, tile2.get(), nullptr, &tp);
    CHECK(tile2->size() == 0);
    CHECK(tile2->filtered_buffer().size() != 0);

    auto unfiltered_tile2 = create_tile_for_unfiltering(nelts2, tile2, tracker);
    run_reverse(config, tp, unfiltered_tile2, pipeline);
    for (uint64_t i = 0; i < nelts2; i++) {
      uint32_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile2.read(&elt, i * sizeof(uint32_t), sizeof(uint32_t)));
      CHECK(elt == i);
    }
  }
}

TEST_CASE("Filter: Test bitshuffle var", "[filter][bitshuffle][var]") {
  tiledb::sm::Config config;

  auto tracker = tiledb::test::create_test_memory_tracker();

  const uint64_t nelts = 100;
  auto tile = make_increasing_tile(nelts, tracker);

  // Set up test data
  std::vector<uint64_t> sizes{
      0,
      32,   // Chunk0: 4 cells.
      80,   // 10 cells, still makes it into this chunk as current size < 50%.
      48,   // Chunk1: 6 cells.
      88,   // Chunk2: 11 cells, new size > 50% and > than 10 cells.
      56,   // Chunk3: 7 cells.
      72,   // Chunk4: 9 cells, new size > 50%.
      8,    // Chunk4: 10 cell, full.
      80,   // Chunk5: 10 cells.
      160,  // Chunk6: 20 cells.
      16,   // Chunk7: 2 cells.
      16,   // Chunk7: 4 cells.
      16,   // Chunk7: 6 cells.
      16,   // Chunk7: 8 cells.
      16,   // Chunk7: 10 cells.
  };  // Chunk8: 12 cells.

  std::vector<uint64_t> out_sizes{112, 48, 88, 56, 80, 80, 160, 80, 96};

  std::vector<uint64_t> offsets(sizes.size());
  uint64_t offset = 0;
  for (uint64_t i = 0; i < offsets.size() - 1; i++) {
    offsets[i] = offset;
    offset += sizes[i + 1];
  }
  offsets[offsets.size() - 1] = offset;

  auto offsets_tile = make_offsets_tile(offsets, tracker);

  FilterPipeline pipeline;
  ThreadPool tp(4);
  pipeline.add_filter(BitshuffleFilter(Datatype::UINT64));

  SECTION("- Single stage") {
    WhiteboxWriterTile::set_max_tile_chunk_size(80);
    pipeline.run_forward(&dummy_stats, tile.get(), offsets_tile.get(), &tp);
    CHECK(tile->size() == 0);
    CHECK(tile->filtered_buffer().size() != 0);

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile, tracker);
    run_reverse(config, tp, unfiltered_tile, pipeline);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
      CHECK(elt == i);
    }
  }

  SECTION("- Indivisible by 8") {
    WhiteboxWriterTile::set_max_tile_chunk_size(80);
    const uint32_t nelts2 = 1001;
    const uint64_t tile_size2 = nelts2 * sizeof(uint32_t);

    auto tile2 = make_shared<WriterTile>(
        HERE(),
        constants::format_version,
        Datatype::UINT32,
        sizeof(uint32_t),
        tile_size2,
        tracker);

    // Set up test data
    for (uint32_t i = 0; i < nelts2; i++) {
      CHECK_NOTHROW(tile2->write(&i, i * sizeof(uint32_t), sizeof(uint32_t)));
    }

    pipeline.run_forward(&dummy_stats, tile2.get(), offsets_tile.get(), &tp);
    CHECK(tile2->size() == 0);
    CHECK(tile2->filtered_buffer().size() != 0);

    auto unfiltered_tile2 = create_tile_for_unfiltering(nelts2, tile2, tracker);
    run_reverse(config, tp, unfiltered_tile2, pipeline);
    for (uint64_t i = 0; i < nelts2; i++) {
      uint32_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile2.read(&elt, i * sizeof(uint32_t), sizeof(uint32_t)));
      CHECK(elt == i);
    }
  }

  WhiteboxWriterTile::set_max_tile_chunk_size(constants::max_tile_chunk_size);
}
