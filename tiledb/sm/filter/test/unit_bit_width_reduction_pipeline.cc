/**
 * @file unit_bit_width_reduction_pipeline.cc
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
 * This set of unit tests checks running the filter pipeline with the bit-width
 * reduction filter.
 *
 */

#include <test/support/src/mem_helpers.h>
#include <test/support/tdb_catch.h>
#include <random>
#include "../bit_width_reduction_filter.h"
#include "filter_test_support.h"

using namespace tiledb::sm;

TEST_CASE("Filter: Test bit width reduction", "[filter][bit-width-reduction]") {
  tiledb::sm::Config config;

  auto tracker = tiledb::test::create_test_memory_tracker();

  // Set up test data
  const uint64_t nelts = 1000;

  FilterPipeline pipeline;
  ThreadPool tp(4);
  pipeline.add_filter(BitWidthReductionFilter(Datatype::UINT64));

  SECTION("- Single stage") {
    auto tile = make_increasing_tile(nelts, tracker);

    pipeline.run_forward(&dummy_stats, tile.get(), nullptr, &tp);

    CHECK(tile->size() == 0);
    CHECK(tile->filtered_buffer().size() != 0);

    // Sanity check number of windows value
    uint64_t offset = 0;
    offset += sizeof(uint64_t);  // Number of chunks
    offset += sizeof(uint32_t);  // First chunk orig size
    offset += sizeof(uint32_t);  // First chunk filtered size
    offset += sizeof(uint32_t);  // First chunk metadata size

    CHECK(
        tile->filtered_buffer().value_at_as<uint32_t>(offset) ==
        nelts * sizeof(uint64_t));  // Original length
    offset += sizeof(uint32_t);

    auto max_win_size =
        pipeline.get_filter<BitWidthReductionFilter>()->max_window_size();
    auto expected_num_win =
        (nelts * sizeof(uint64_t)) / max_win_size +
        uint32_t(bool((nelts * sizeof(uint64_t)) % max_win_size));
    CHECK(
        tile->filtered_buffer().value_at_as<uint32_t>(offset) ==
        expected_num_win);  // Number of windows

    // Check compression worked
    auto compressed_size = tile->filtered_buffer().size();
    CHECK(compressed_size < nelts * sizeof(uint64_t));

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile, tracker);
    run_reverse(config, tp, unfiltered_tile, pipeline);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
      CHECK(elt == i);
    }
  }

  SECTION("- Window sizes") {
    std::vector<uint32_t> window_sizes = {
        32, 64, 128, 256, 437, 512, 1024, 2000};
    for (auto window_size : window_sizes) {
      auto tile = make_increasing_tile(nelts, tracker);

      pipeline.get_filter<BitWidthReductionFilter>()->set_max_window_size(
          window_size);

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
  }

  SECTION("- Random values") {
    std::random_device rd;
    auto seed = rd();
    std::mt19937 gen(seed), gen_copy(seed);
    std::uniform_int_distribution<> rng(0, std::numeric_limits<int32_t>::max());
    INFO("Random element seed: " << seed);

    auto tile = make_shared<WriterTile>(
        HERE(),
        constants::format_version,
        Datatype::UINT64,
        sizeof(uint64_t),
        nelts * sizeof(uint64_t),
        tracker);

    // Set up test data
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t val = (uint64_t)rng(gen);
      CHECK_NOTHROW(tile->write(&val, i * sizeof(uint64_t), sizeof(uint64_t)));
    }

    pipeline.run_forward(&dummy_stats, tile.get(), nullptr, &tp);
    CHECK(tile->size() == 0);
    CHECK(tile->filtered_buffer().size() != 0);

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile, tracker);
    run_reverse(config, tp, unfiltered_tile, pipeline);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
      CHECK((int64_t)elt == rng(gen_copy));
    }
  }

  SECTION(" - Random signed values") {
    std::random_device rd;
    auto seed = rd();
    std::mt19937 gen(seed), gen_copy(seed);
    std::uniform_int_distribution<> rng(
        std::numeric_limits<int32_t>::lowest(),
        std::numeric_limits<int32_t>::max());
    INFO("Random element seed: " << seed);

    auto tile = make_shared<WriterTile>(
        HERE(),
        constants::format_version,
        Datatype::UINT32,
        sizeof(uint32_t),
        nelts * sizeof(uint32_t),
        tracker);

    // Set up test data
    for (uint64_t i = 0; i < nelts; i++) {
      uint32_t val = (uint32_t)rng(gen);
      CHECK_NOTHROW(tile->write(&val, i * sizeof(uint32_t), sizeof(uint32_t)));
    }

    pipeline.run_forward(&dummy_stats, tile.get(), nullptr, &tp);
    CHECK(tile->size() == 0);
    CHECK(tile->filtered_buffer().size() != 0);

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile, tracker);
    run_reverse(config, tp, unfiltered_tile, pipeline);
    for (uint64_t i = 0; i < nelts; i++) {
      int32_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile.read(&elt, i * sizeof(int32_t), sizeof(int32_t)));
      CHECK(elt == rng(gen_copy));
    }
  }

  SECTION("- Byte overflow") {
    auto tile = make_shared<WriterTile>(
        HERE(),
        constants::format_version,
        Datatype::UINT64,
        sizeof(uint64_t),
        nelts * sizeof(uint64_t),
        tracker);

    // Set up test data
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t val = i % 257;
      CHECK_NOTHROW(tile->write(&val, i * sizeof(uint64_t), sizeof(uint64_t)));
    }

    pipeline.run_forward(&dummy_stats, tile.get(), nullptr, &tp);
    CHECK(tile->size() == 0);
    CHECK(tile->filtered_buffer().size() != 0);

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile, tracker);
    run_reverse(config, tp, unfiltered_tile, pipeline);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
      CHECK(elt == i % 257);
    }
  }
}

TEST_CASE(
    "Filter: Test bit width reduction var",
    "[filter][bit-width-reduction][var]") {
  tiledb::sm::Config config;

  auto tracker = tiledb::test::create_test_memory_tracker();

  const uint64_t nelts = 100;

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
  };        // Chunk8: 12 cells.

  std::vector<uint64_t> out_sizes{112, 48, 88, 56, 80, 80, 160, 80, 96};

  std::vector<uint64_t> offsets(sizes.size());
  uint64_t offset = 0;
  for (uint64_t i = 0; i < offsets.size() - 1; i++) {
    offsets[i] = offset;
    offset += sizes[i + 1];
  }
  offsets[offsets.size() - 1] = offset;

  const uint64_t offsets_tile_size =
      offsets.size() * constants::cell_var_offset_size;

  FilterPipeline pipeline;
  ThreadPool tp(4);
  pipeline.add_filter(BitWidthReductionFilter(Datatype::UINT64));

  SECTION("- Single stage") {
    auto tile = make_increasing_tile(nelts, tracker);
    auto offsets_tile = make_offsets_tile(offsets, tracker);

    WhiteboxWriterTile::set_max_tile_chunk_size(80);
    pipeline.run_forward(&dummy_stats, tile.get(), offsets_tile.get(), &tp);
    CHECK(tile->size() == 0);
    CHECK(tile->filtered_buffer().size() != 0);

    uint64_t offset = 0;
    CHECK(
        tile->filtered_buffer().value_at_as<uint64_t>(offset) ==
        9);  // Number of chunks
    offset += sizeof(uint64_t);

    for (uint64_t i = 0; i < 9; i++) {
      CHECK(
          tile->filtered_buffer().value_at_as<uint32_t>(offset) ==
          out_sizes[i]);  // Chunk orig size
      offset += sizeof(uint32_t);
      CHECK(
          tile->filtered_buffer().value_at_as<uint32_t>(offset) ==
          out_sizes[i] / 8);  // Chunk filtered size
      offset += sizeof(uint32_t);

      uint32_t md_size = tile->filtered_buffer().value_at_as<uint32_t>(offset);
      offset += sizeof(uint32_t);

      CHECK(
          tile->filtered_buffer().value_at_as<uint32_t>(offset) ==
          out_sizes[i]);  // Original length
      offset += sizeof(uint32_t);

      // Check window value.
      auto max_win_size =
          pipeline.get_filter<BitWidthReductionFilter>()->max_window_size();
      auto expected_num_win = out_sizes[i] / max_win_size +
                              uint32_t(bool(out_sizes[0] % max_win_size));
      CHECK(
          tile->filtered_buffer().value_at_as<uint32_t>(offset) ==
          expected_num_win);  // Number of windows

      offset += md_size - sizeof(uint32_t);

      // Check all elements are good.
      uint8_t el = 0;
      for (uint64_t j = 0; j < out_sizes[i] / sizeof(uint64_t); j++) {
        CHECK(tile->filtered_buffer().value_at_as<uint8_t>(offset) == el++);
        offset += sizeof(uint8_t);
      }
    }

    // Check compression worked
    auto compressed_size = tile->filtered_buffer().size();
    CHECK(compressed_size < nelts * sizeof(uint64_t));

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile, tracker);
    run_reverse(config, tp, unfiltered_tile, pipeline);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
      CHECK(elt == i);
    }
  }

  SECTION("- Window sizes") {
    WhiteboxWriterTile::set_max_tile_chunk_size(80);
    std::vector<uint32_t> window_sizes = {
        32, 64, 128, 256, 437, 512, 1024, 2000};
    for (auto window_size : window_sizes) {
      auto tile = make_increasing_tile(nelts, tracker);
      auto offsets_tile = make_offsets_tile(offsets, tracker);
      pipeline.get_filter<BitWidthReductionFilter>()->set_max_window_size(
          window_size);
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
  }

  SECTION("- Random values") {
    WhiteboxWriterTile::set_max_tile_chunk_size(80);
    std::random_device rd;
    auto seed = rd();
    std::mt19937 gen(seed), gen_copy(seed);
    std::uniform_int_distribution<> rng(0, std::numeric_limits<int32_t>::max());
    INFO("Random element seed: " << seed);

    auto tile = make_shared<WriterTile>(
        HERE(),
        constants::format_version,
        Datatype::UINT64,
        sizeof(uint64_t),
        nelts * sizeof(uint64_t),
        tracker);
    auto offsets_tile = make_offsets_tile(offsets, tracker);

    // Set up test data
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t val = (uint64_t)rng(gen);
      CHECK_NOTHROW(tile->write(&val, i * sizeof(uint64_t), sizeof(uint64_t)));
    }

    pipeline.run_forward(&dummy_stats, tile.get(), offsets_tile.get(), &tp);
    CHECK(tile->size() == 0);
    CHECK(tile->filtered_buffer().size() != 0);

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile, tracker);
    run_reverse(config, tp, unfiltered_tile, pipeline);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
      CHECK((int64_t)elt == rng(gen_copy));
    }
  }

  SECTION(" - Random signed values") {
    WhiteboxWriterTile::set_max_tile_chunk_size(80);
    std::random_device rd;
    auto seed = rd();
    std::mt19937 gen(seed), gen_copy(seed);
    std::uniform_int_distribution<> rng(
        std::numeric_limits<int32_t>::lowest(),
        std::numeric_limits<int32_t>::max());
    INFO("Random element seed: " << seed);

    auto tile = make_shared<WriterTile>(
        HERE(),
        constants::format_version,
        Datatype::UINT32,
        sizeof(uint32_t),
        nelts * sizeof(uint32_t),
        tracker);

    // Set up test data
    for (uint64_t i = 0; i < nelts; i++) {
      uint32_t val = (uint32_t)rng(gen);
      CHECK_NOTHROW(tile->write(&val, i * sizeof(uint32_t), sizeof(uint32_t)));
    }

    std::vector<uint64_t> offsets32(offsets);
    for (uint64_t i = 0; i < offsets32.size(); i++) {
      offsets32[i] /= 2;
    }

    WriterTile offsets_tile32(
        constants::format_version,
        Datatype::UINT64,
        constants::cell_var_offset_size,
        offsets_tile_size,
        tracker);

    // Set up test data
    for (uint64_t i = 0; i < offsets.size(); i++) {
      CHECK_NOTHROW(offsets_tile32.write(
          &offsets32[i],
          i * constants::cell_var_offset_size,
          constants::cell_var_offset_size));
    }

    pipeline.run_forward(&dummy_stats, tile.get(), &offsets_tile32, &tp);
    CHECK(tile->size() == 0);
    CHECK(tile->filtered_buffer().size() != 0);

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile, tracker);
    run_reverse(config, tp, unfiltered_tile, pipeline);
    for (uint64_t i = 0; i < nelts; i++) {
      int32_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile.read(&elt, i * sizeof(int32_t), sizeof(int32_t)));
      CHECK(elt == rng(gen_copy));
    }
  }

  SECTION("- Byte overflow") {
    WhiteboxWriterTile::set_max_tile_chunk_size(80);
    auto tile = make_shared<WriterTile>(
        HERE(),
        constants::format_version,
        Datatype::UINT64,
        sizeof(uint64_t),
        nelts * sizeof(uint64_t),
        tracker);

    // Set up test data
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t val = i % 257;
      CHECK_NOTHROW(tile->write(&val, i * sizeof(uint64_t), sizeof(uint64_t)));
    }

    auto offsets_tile = make_offsets_tile(offsets, tracker);
    pipeline.run_forward(&dummy_stats, tile.get(), offsets_tile.get(), &tp);
    CHECK(tile->size() == 0);
    CHECK(tile->filtered_buffer().size() != 0);

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile, tracker);
    run_reverse(config, tp, unfiltered_tile, pipeline);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
      CHECK(elt == i % 257);
    }
  }

  WhiteboxWriterTile::set_max_tile_chunk_size(constants::max_tile_chunk_size);
}
