/**
 * @file unit-filter-pipeline.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2022 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * Tests the `FilterPipeline` class.
 */

#include "test/support/src/helpers.h"
#include "tiledb/common/common.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/crypto/encryption_key.h"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/compressor.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/filter_option.h"
#include "tiledb/sm/enums/filter_type.h"
#include "tiledb/sm/filter/bit_width_reduction_filter.h"
#include "tiledb/sm/filter/bitshuffle_filter.h"
#include "tiledb/sm/filter/byteshuffle_filter.h"
#include "tiledb/sm/filter/checksum_md5_filter.h"
#include "tiledb/sm/filter/checksum_sha256_filter.h"
#include "tiledb/sm/filter/compression_filter.h"
#include "tiledb/sm/filter/encryption_aes256gcm_filter.h"
#include "tiledb/sm/filter/filter_pipeline.h"
#include "tiledb/sm/filter/float_scaling_filter.h"
#include "tiledb/sm/filter/positive_delta_filter.h"
#include "tiledb/sm/filter/webp_filter.h"
#include "tiledb/sm/filter/xor_filter.h"
#include "tiledb/sm/tile/tile.h"
#include "tiledb/stdx/utility/to_underlying.h"

#include <test/support/tdb_catch.h>
#include <functional>
#include <random>

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm;

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
                         &test::g_helper_stats,
                         &unfiltered_tile,
                         nullptr,
                         chunk_data,
                         0,
                         chunk_data.filtered_chunks_.size(),
                         tp.concurrency_level(),
                         config)
                     .ok());
}

/**
 * Simple filter that modifies the input stream by adding 1 to every input
 * element.
 */
class Add1InPlace : public tiledb::sm::Filter {
 public:
  // Just use a dummy filter type
  Add1InPlace(Datatype filter_data_type)
      : Filter(FilterType::FILTER_NONE, filter_data_type) {
  }

  void dump(FILE* out) const override {
    (void)out;
  }

  Status run_forward(
      const WriterTile&,
      WriterTile* const,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const override {
    auto input_size = input->size();
    RETURN_NOT_OK(output->append_view(input));
    output->reset_offset();

    uint64_t nelts = input_size / sizeof(uint64_t);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t* val = output->value_ptr<uint64_t>();
      *val += 1;
      output->advance_offset(sizeof(uint64_t));
    }

    // Metadata not modified by this filter.
    RETURN_NOT_OK(output_metadata->append_view(input_metadata));

    return Status::Ok();
  }

  Status run_reverse(
      const Tile&,
      Tile*,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output,
      const tiledb::sm::Config& config) const override {
    (void)config;

    auto input_size = input->size();
    RETURN_NOT_OK(output->append_view(input));
    output->reset_offset();

    uint64_t nelts = input_size / sizeof(uint64_t);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t* val = output->value_ptr<uint64_t>();
      *val -= 1;
      output->advance_offset(sizeof(uint64_t));
    }

    // Metadata not modified by this filter.
    RETURN_NOT_OK(output_metadata->append_view(input_metadata));

    return Status::Ok();
  }

  Add1InPlace* clone_impl() const override {
    return new Add1InPlace(filter_data_type_);
  }
};

/**
 * Simple filter that increments every element of the input stream, writing the
 * output to a new buffer. Does not modify the input stream.
 */
class Add1OutOfPlace : public tiledb::sm::Filter {
 public:
  // Just use a dummy filter type
  Add1OutOfPlace(Datatype filter_data_type)
      : Filter(FilterType::FILTER_NONE, filter_data_type) {
  }

  void dump(FILE* out) const override {
    (void)out;
  }

  Status run_forward(
      const WriterTile&,
      WriterTile* const,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const override {
    auto input_size = input->size();
    auto nelts = input_size / sizeof(uint64_t);

    // Add a new output buffer.
    RETURN_NOT_OK(output->prepend_buffer(input_size));
    output->reset_offset();

    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t inc;
      RETURN_NOT_OK(input->read(&inc, sizeof(uint64_t)));
      inc++;
      RETURN_NOT_OK(output->write(&inc, sizeof(uint64_t)));
    }

    // Finish any remaining bytes to ensure no data loss.
    auto rem = input_size % sizeof(uint64_t);
    for (unsigned i = 0; i < rem; i++) {
      char byte;
      RETURN_NOT_OK(input->read(&byte, sizeof(char)));
      RETURN_NOT_OK(output->write(&byte, sizeof(char)));
    }

    // Metadata not modified by this filter.
    RETURN_NOT_OK(output_metadata->append_view(input_metadata));

    return Status::Ok();
  }

  Status run_reverse(
      const Tile&,
      Tile*,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output,
      const tiledb::sm::Config& config) const override {
    (void)config;

    auto input_size = input->size();
    auto nelts = input->size() / sizeof(uint64_t);

    // Add a new output buffer.
    RETURN_NOT_OK(output->prepend_buffer(input->size()));
    output->reset_offset();

    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t inc;
      RETURN_NOT_OK(input->read(&inc, sizeof(uint64_t)));
      inc--;
      RETURN_NOT_OK(output->write(&inc, sizeof(uint64_t)));
    }

    auto rem = input_size % sizeof(uint64_t);
    for (unsigned i = 0; i < rem; i++) {
      char byte;
      RETURN_NOT_OK(input->read(&byte, sizeof(char)));
      RETURN_NOT_OK(output->write(&byte, sizeof(char)));
    }

    // Metadata not modified by this filter.
    RETURN_NOT_OK(output_metadata->append_view(input_metadata));

    return Status::Ok();
  }

  Add1OutOfPlace* clone_impl() const override {
    return new Add1OutOfPlace(filter_data_type_);
  }
};

/**
 * Simple filter which computes the sum of its input and prepends the sum
 * to the output. In reverse execute, checks that the sum is correct.
 */
class PseudoChecksumFilter : public tiledb::sm::Filter {
 public:
  // Just use a dummy filter type
  PseudoChecksumFilter(Datatype filter_data_type)
      : Filter(FilterType::FILTER_NONE, filter_data_type) {
  }

  void dump(FILE* out) const override {
    (void)out;
  }

  Status run_forward(
      const WriterTile&,
      WriterTile* const,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const override {
    auto input_size = input->size();
    auto nelts = input_size / sizeof(uint64_t);

    // The input is unmodified by this filter.
    RETURN_NOT_OK(output->append_view(input));

    // Forward the existing metadata and prepend a metadata buffer for the
    // checksum.
    RETURN_NOT_OK(output_metadata->append_view(input_metadata));
    RETURN_NOT_OK(output_metadata->prepend_buffer(sizeof(uint64_t)));
    output_metadata->reset_offset();

    uint64_t sum = 0;
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t val;
      RETURN_NOT_OK(input->read(&val, sizeof(uint64_t)));
      sum += val;
    }

    RETURN_NOT_OK(output_metadata->write(&sum, sizeof(uint64_t)));

    return Status::Ok();
  }

  Status run_reverse(
      const Tile&,
      Tile*,
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output,
      const tiledb::sm::Config& config) const override {
    (void)config;

    auto input_size = input->size();
    auto nelts = input_size / sizeof(uint64_t);

    uint64_t input_sum;
    RETURN_NOT_OK(input_metadata->read(&input_sum, sizeof(uint64_t)));

    uint64_t sum = 0;
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t val;
      RETURN_NOT_OK(input->read(&val, sizeof(uint64_t)));
      sum += val;
    }

    if (sum != input_sum)
      return Status_FilterError("Filter error; sum does not match.");

    // The output metadata is just a view on the input metadata, skipping the
    // checksum bytes.
    RETURN_NOT_OK(output_metadata->append_view(
        input_metadata,
        sizeof(uint64_t),
        input_metadata->size() - sizeof(uint64_t)));

    // The output data is just a view on the unmodified input.
    RETURN_NOT_OK(output->append_view(input));

    return Status::Ok();
  }

  PseudoChecksumFilter* clone_impl() const override {
    return new PseudoChecksumFilter(filter_data_type_);
  }
};

TEST_CASE("Filter: Test compression", "[filter][compression]") {
  tiledb::sm::Config config;

  const uint64_t nelts = 100;
  auto tile = make_increasing_tile(nelts);

  // Set up dummy array schema (needed by compressor filter for cell size, etc).
  uint32_t dim_dom[] = {1, 10};
  auto dim{make_shared<tiledb::sm::Dimension>(HERE(), "", Datatype::INT32)};
  CHECK(dim->set_domain(dim_dom).ok());
  auto domain{make_shared<tiledb::sm::Domain>(HERE())};
  CHECK(domain->add_dimension(dim).ok());
  tiledb::sm::ArraySchema schema(
      make_shared<MemoryTracker>(HERE()), ArrayType::DENSE);
  tiledb::sm::Attribute attr("attr", Datatype::UINT64);
  CHECK(schema.add_attribute(make_shared<tiledb::sm::Attribute>(HERE(), attr))
            .ok());
  CHECK(schema.set_domain(domain).ok());

  FilterPipeline pipeline;
  ThreadPool tp(4);

  SECTION("- Simple") {
    pipeline.add_filter(Add1InPlace(Datatype::UINT64));
    pipeline.add_filter(Add1OutOfPlace(Datatype::UINT64));
    pipeline.add_filter(
        CompressionFilter(tiledb::sm::Compressor::LZ4, 5, Datatype::UINT64));

    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());
    // Check compression worked
    CHECK(tile.size() == 0);
    CHECK(tile.filtered_buffer().size() < nelts * sizeof(uint64_t));

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
    run_reverse(config, tp, unfiltered_tile, pipeline);

    // Check all elements original values.
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
      CHECK(elt == i);
    }
  }

  SECTION("- With checksum stage") {
    pipeline.add_filter(PseudoChecksumFilter(Datatype::UINT64));
    pipeline.add_filter(
        CompressionFilter(tiledb::sm::Compressor::LZ4, 5, Datatype::UINT64));

    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());
    // Check compression worked
    CHECK(tile.size() == 0);
    CHECK(tile.filtered_buffer().size() < nelts * sizeof(uint64_t));

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
    run_reverse(config, tp, unfiltered_tile, pipeline);

    // Check all elements original values.
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
      CHECK(elt == i);
    }
  }

  SECTION("- With multiple stages") {
    pipeline.add_filter(Add1InPlace(Datatype::UINT64));
    pipeline.add_filter(PseudoChecksumFilter(Datatype::UINT64));
    pipeline.add_filter(Add1OutOfPlace(Datatype::UINT64));
    pipeline.add_filter(
        CompressionFilter(tiledb::sm::Compressor::LZ4, 5, Datatype::UINT64));

    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());
    // Check compression worked
    CHECK(tile.size() == 0);
    CHECK(tile.filtered_buffer().size() < nelts * sizeof(uint64_t));

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
    run_reverse(config, tp, unfiltered_tile, pipeline);

    // Check all elements original values.
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
      CHECK(elt == i);
    }
  }
}

TEST_CASE("Filter: Test compression var", "[filter][compression][var]") {
  tiledb::sm::Config config;

  const uint64_t nelts = 100;
  auto tile = make_increasing_tile(nelts);

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

  auto offsets_tile = make_offsets_tile(offsets);

  // Set up dummy array schema (needed by compressor filter for cell size, etc).
  uint32_t dim_dom[] = {1, 10};
  auto dim{make_shared<tiledb::sm::Dimension>(HERE(), "", Datatype::INT32)};
  CHECK(dim->set_domain(dim_dom).ok());
  auto domain{make_shared<tiledb::sm::Domain>(HERE())};
  CHECK(domain->add_dimension(dim).ok());
  tiledb::sm::ArraySchema schema(
      make_shared<MemoryTracker>(HERE()), ArrayType::DENSE);
  tiledb::sm::Attribute attr("attr", Datatype::UINT64);
  CHECK(schema.add_attribute(make_shared<tiledb::sm::Attribute>(HERE(), attr))
            .ok());
  CHECK(schema.set_domain(domain).ok());

  FilterPipeline pipeline;
  ThreadPool tp(4);

  SECTION("- Simple") {
    WriterTile::set_max_tile_chunk_size(80);
    pipeline.add_filter(Add1InPlace(Datatype::UINT64));
    pipeline.add_filter(Add1OutOfPlace(Datatype::UINT64));
    pipeline.add_filter(
        CompressionFilter(tiledb::sm::Compressor::LZ4, 5, Datatype::UINT64));

    CHECK(pipeline.run_forward(&test::g_helper_stats, &tile, &offsets_tile, &tp)
              .ok());
    // Check number of chunks
    CHECK(tile.size() == 0);
    CHECK(
        tile.filtered_buffer().value_at_as<uint64_t>(0) ==
        9);  // Number of chunks

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
    run_reverse(config, tp, unfiltered_tile, pipeline);

    // Check all elements original values.
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
      CHECK(elt == i);
    }
  }

  SECTION("- With checksum stage") {
    WriterTile::set_max_tile_chunk_size(80);
    pipeline.add_filter(PseudoChecksumFilter(Datatype::UINT64));
    pipeline.add_filter(
        CompressionFilter(tiledb::sm::Compressor::LZ4, 5, Datatype::UINT64));

    CHECK(pipeline.run_forward(&test::g_helper_stats, &tile, &offsets_tile, &tp)
              .ok());
    // Check number of chunks
    CHECK(tile.size() == 0);
    CHECK(
        tile.filtered_buffer().value_at_as<uint64_t>(0) ==
        9);  // Number of chunks

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
    run_reverse(config, tp, unfiltered_tile, pipeline);

    // Check all elements original values.
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
      CHECK(elt == i);
    }
  }

  SECTION("- With multiple stages") {
    WriterTile::set_max_tile_chunk_size(80);
    pipeline.add_filter(Add1InPlace(Datatype::UINT64));
    pipeline.add_filter(PseudoChecksumFilter(Datatype::UINT64));
    pipeline.add_filter(Add1OutOfPlace(Datatype::UINT64));
    pipeline.add_filter(
        CompressionFilter(tiledb::sm::Compressor::LZ4, 5, Datatype::UINT64));

    CHECK(pipeline.run_forward(&test::g_helper_stats, &tile, &offsets_tile, &tp)
              .ok());
    // Check number of chunks
    CHECK(tile.size() == 0);
    CHECK(
        tile.filtered_buffer().value_at_as<uint64_t>(0) ==
        9);  // Number of chunks

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
    run_reverse(config, tp, unfiltered_tile, pipeline);

    // Check all elements original values.
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
    "Filter: Test skip checksum validation",
    "[filter][skip-checksum-validation]") {
  tiledb::sm::Config config;
  REQUIRE(config.set("sm.skip_checksum_validation", "true").ok());

  const uint64_t nelts = 100;
  auto tile = make_increasing_tile(nelts);

  // MD5
  FilterPipeline md5_pipeline;
  ThreadPool tp(4);
  ChecksumMD5Filter md5_filter(Datatype::UINT64);
  md5_pipeline.add_filter(md5_filter);
  CHECK(md5_pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp)
            .ok());
  CHECK(tile.size() == 0);
  CHECK(tile.filtered_buffer().size() != 0);

  auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
  run_reverse(config, tp, unfiltered_tile, md5_pipeline);

  for (uint64_t n = 0; n < nelts; n++) {
    uint64_t elt = 0;
    CHECK_NOTHROW(
        unfiltered_tile.read(&elt, n * sizeof(uint64_t), sizeof(uint64_t)));
    CHECK(elt == n);
  }

  // SHA256
  auto tile2 = make_increasing_tile(nelts);

  FilterPipeline sha_256_pipeline;
  ChecksumMD5Filter sha_256_filter(Datatype::UINT64);
  sha_256_pipeline.add_filter(sha_256_filter);
  CHECK(
      sha_256_pipeline.run_forward(&test::g_helper_stats, &tile2, nullptr, &tp)
          .ok());
  CHECK(tile2.size() == 0);
  CHECK(tile2.filtered_buffer().size() != 0);

  auto unfiltered_tile2 = create_tile_for_unfiltering(nelts, tile2);
  run_reverse(config, tp, unfiltered_tile2, sha_256_pipeline);
  for (uint64_t n = 0; n < nelts; n++) {
    uint64_t elt = 0;
    CHECK_NOTHROW(
        unfiltered_tile2.read(&elt, n * sizeof(uint64_t), sizeof(uint64_t)));
    CHECK(elt == n);
  }
}

TEST_CASE("Filter: Test bit width reduction", "[filter][bit-width-reduction]") {
  tiledb::sm::Config config;

  // Set up test data
  const uint64_t nelts = 1000;

  FilterPipeline pipeline;
  ThreadPool tp(4);
  pipeline.add_filter(BitWidthReductionFilter(Datatype::UINT64));

  SECTION("- Single stage") {
    auto tile = make_increasing_tile(nelts);

    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());

    CHECK(tile.size() == 0);
    CHECK(tile.filtered_buffer().size() != 0);

    // Sanity check number of windows value
    uint64_t offset = 0;
    offset += sizeof(uint64_t);  // Number of chunks
    offset += sizeof(uint32_t);  // First chunk orig size
    offset += sizeof(uint32_t);  // First chunk filtered size
    offset += sizeof(uint32_t);  // First chunk metadata size

    CHECK(
        tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
        nelts * sizeof(uint64_t));  // Original length
    offset += sizeof(uint32_t);

    auto max_win_size =
        pipeline.get_filter<BitWidthReductionFilter>()->max_window_size();
    auto expected_num_win =
        (nelts * sizeof(uint64_t)) / max_win_size +
        uint32_t(bool((nelts * sizeof(uint64_t)) % max_win_size));
    CHECK(
        tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
        expected_num_win);  // Number of windows

    // Check compression worked
    auto compressed_size = tile.filtered_buffer().size();
    CHECK(compressed_size < nelts * sizeof(uint64_t));

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
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
      auto tile = make_increasing_tile(nelts);

      pipeline.get_filter<BitWidthReductionFilter>()->set_max_window_size(
          window_size);

      CHECK(pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp)
                .ok());
      CHECK(tile.size() == 0);
      CHECK(tile.filtered_buffer().size() != 0);

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

  SECTION("- Random values") {
    std::random_device rd;
    auto seed = rd();
    std::mt19937 gen(seed), gen_copy(seed);
    std::uniform_int_distribution<> rng(0, std::numeric_limits<int32_t>::max());
    INFO("Random element seed: " << seed);

    WriterTile tile(
        constants::format_version,
        Datatype::UINT64,
        sizeof(uint64_t),
        nelts * sizeof(uint64_t));

    // Set up test data
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t val = (uint64_t)rng(gen);
      CHECK_NOTHROW(tile.write(&val, i * sizeof(uint64_t), sizeof(uint64_t)));
    }

    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());
    CHECK(tile.size() == 0);
    CHECK(tile.filtered_buffer().size() != 0);

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
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

    WriterTile tile(
        constants::format_version,
        Datatype::UINT32,
        sizeof(uint32_t),
        nelts * sizeof(uint32_t));

    // Set up test data
    for (uint64_t i = 0; i < nelts; i++) {
      uint32_t val = (uint32_t)rng(gen);
      CHECK_NOTHROW(tile.write(&val, i * sizeof(uint32_t), sizeof(uint32_t)));
    }

    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());
    CHECK(tile.size() == 0);
    CHECK(tile.filtered_buffer().size() != 0);

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
    run_reverse(config, tp, unfiltered_tile, pipeline);
    for (uint64_t i = 0; i < nelts; i++) {
      int32_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile.read(&elt, i * sizeof(int32_t), sizeof(int32_t)));
      CHECK(elt == rng(gen_copy));
    }
  }

  SECTION("- Byte overflow") {
    WriterTile tile(
        constants::format_version,
        Datatype::UINT64,
        sizeof(uint64_t),
        nelts * sizeof(uint64_t));

    // Set up test data
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t val = i % 257;
      CHECK_NOTHROW(tile.write(&val, i * sizeof(uint64_t), sizeof(uint64_t)));
    }

    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());
    CHECK(tile.size() == 0);
    CHECK(tile.filtered_buffer().size() != 0);

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
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
    auto tile = make_increasing_tile(nelts);
    auto offsets_tile = make_offsets_tile(offsets);

    WriterTile::set_max_tile_chunk_size(80);
    CHECK(pipeline.run_forward(&test::g_helper_stats, &tile, &offsets_tile, &tp)
              .ok());

    CHECK(tile.size() == 0);
    CHECK(tile.filtered_buffer().size() != 0);

    uint64_t offset = 0;
    CHECK(
        tile.filtered_buffer().value_at_as<uint64_t>(offset) ==
        9);  // Number of chunks
    offset += sizeof(uint64_t);

    for (uint64_t i = 0; i < 9; i++) {
      CHECK(
          tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
          out_sizes[i]);  // Chunk orig size
      offset += sizeof(uint32_t);
      CHECK(
          tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
          out_sizes[i] / 8);  // Chunk filtered size
      offset += sizeof(uint32_t);

      uint32_t md_size = tile.filtered_buffer().value_at_as<uint32_t>(offset);
      offset += sizeof(uint32_t);

      CHECK(
          tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
          out_sizes[i]);  // Original length
      offset += sizeof(uint32_t);

      // Check window value.
      auto max_win_size =
          pipeline.get_filter<BitWidthReductionFilter>()->max_window_size();
      auto expected_num_win = out_sizes[i] / max_win_size +
                              uint32_t(bool(out_sizes[0] % max_win_size));
      CHECK(
          tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
          expected_num_win);  // Number of windows

      offset += md_size - sizeof(uint32_t);

      // Check all elements are good.
      uint8_t el = 0;
      for (uint64_t j = 0; j < out_sizes[i] / sizeof(uint64_t); j++) {
        CHECK(tile.filtered_buffer().value_at_as<uint8_t>(offset) == el++);
        offset += sizeof(uint8_t);
      }
    }

    // Check compression worked
    auto compressed_size = tile.filtered_buffer().size();
    CHECK(compressed_size < nelts * sizeof(uint64_t));

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
    run_reverse(config, tp, unfiltered_tile, pipeline);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
      CHECK(elt == i);
    }
  }

  SECTION("- Window sizes") {
    WriterTile::set_max_tile_chunk_size(80);
    std::vector<uint32_t> window_sizes = {
        32, 64, 128, 256, 437, 512, 1024, 2000};
    for (auto window_size : window_sizes) {
      auto tile = make_increasing_tile(nelts);
      auto offsets_tile = make_offsets_tile(offsets);
      pipeline.get_filter<BitWidthReductionFilter>()->set_max_window_size(
          window_size);

      CHECK(
          pipeline.run_forward(&test::g_helper_stats, &tile, &offsets_tile, &tp)
              .ok());
      CHECK(tile.size() == 0);
      CHECK(tile.filtered_buffer().size() != 0);

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

  SECTION("- Random values") {
    WriterTile::set_max_tile_chunk_size(80);
    std::random_device rd;
    auto seed = rd();
    std::mt19937 gen(seed), gen_copy(seed);
    std::uniform_int_distribution<> rng(0, std::numeric_limits<int32_t>::max());
    INFO("Random element seed: " << seed);

    WriterTile tile(
        constants::format_version,
        Datatype::UINT64,
        sizeof(uint64_t),
        nelts * sizeof(uint64_t));
    auto offsets_tile = make_offsets_tile(offsets);

    // Set up test data
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t val = (uint64_t)rng(gen);
      CHECK_NOTHROW(tile.write(&val, i * sizeof(uint64_t), sizeof(uint64_t)));
    }

    CHECK(pipeline.run_forward(&test::g_helper_stats, &tile, &offsets_tile, &tp)
              .ok());
    CHECK(tile.size() == 0);
    CHECK(tile.filtered_buffer().size() != 0);

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
    run_reverse(config, tp, unfiltered_tile, pipeline);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
      CHECK((int64_t)elt == rng(gen_copy));
    }
  }

  SECTION(" - Random signed values") {
    WriterTile::set_max_tile_chunk_size(80);
    std::random_device rd;
    auto seed = rd();
    std::mt19937 gen(seed), gen_copy(seed);
    std::uniform_int_distribution<> rng(
        std::numeric_limits<int32_t>::lowest(),
        std::numeric_limits<int32_t>::max());
    INFO("Random element seed: " << seed);

    WriterTile tile(
        constants::format_version,
        Datatype::UINT32,
        sizeof(uint32_t),
        nelts * sizeof(uint32_t));

    // Set up test data
    for (uint64_t i = 0; i < nelts; i++) {
      uint32_t val = (uint32_t)rng(gen);
      CHECK_NOTHROW(tile.write(&val, i * sizeof(uint32_t), sizeof(uint32_t)));
    }

    std::vector<uint64_t> offsets32(offsets);
    for (uint64_t i = 0; i < offsets32.size(); i++) {
      offsets32[i] /= 2;
    }

    WriterTile offsets_tile32(
        constants::format_version,
        Datatype::UINT64,
        constants::cell_var_offset_size,
        offsets_tile_size);

    // Set up test data
    for (uint64_t i = 0; i < offsets.size(); i++) {
      CHECK_NOTHROW(offsets_tile32.write(
          &offsets32[i],
          i * constants::cell_var_offset_size,
          constants::cell_var_offset_size));
    }

    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile, &offsets_tile32, &tp)
            .ok());
    CHECK(tile.size() == 0);
    CHECK(tile.filtered_buffer().size() != 0);

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
    run_reverse(config, tp, unfiltered_tile, pipeline);
    for (uint64_t i = 0; i < nelts; i++) {
      int32_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile.read(&elt, i * sizeof(int32_t), sizeof(int32_t)));
      CHECK(elt == rng(gen_copy));
    }
  }

  SECTION("- Byte overflow") {
    WriterTile::set_max_tile_chunk_size(80);
    WriterTile tile(
        constants::format_version,
        Datatype::UINT64,
        sizeof(uint64_t),
        nelts * sizeof(uint64_t));

    // Set up test data
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t val = i % 257;
      CHECK_NOTHROW(tile.write(&val, i * sizeof(uint64_t), sizeof(uint64_t)));
    }

    auto offsets_tile = make_offsets_tile(offsets);

    CHECK(pipeline.run_forward(&test::g_helper_stats, &tile, &offsets_tile, &tp)
              .ok());
    CHECK(tile.size() == 0);
    CHECK(tile.filtered_buffer().size() != 0);

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
    run_reverse(config, tp, unfiltered_tile, pipeline);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
      CHECK(elt == i % 257);
    }
  }

  WriterTile::set_max_tile_chunk_size(constants::max_tile_chunk_size);
}

TEST_CASE("Filter: Test positive-delta encoding", "[filter][positive-delta]") {
  tiledb::sm::Config config;

  // Set up test data
  const uint64_t nelts = 1000;

  FilterPipeline pipeline;
  ThreadPool tp(4);
  pipeline.add_filter(PositiveDeltaFilter(Datatype::UINT64));

  SECTION("- Single stage") {
    auto tile = make_increasing_tile(nelts);
    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());

    CHECK(tile.size() == 0);
    CHECK(tile.filtered_buffer().size() != 0);

    auto pipeline_metadata_size = sizeof(uint64_t) + 3 * sizeof(uint32_t);

    uint64_t offset = 0;
    offset += sizeof(uint64_t);  // Number of chunks
    offset += sizeof(uint32_t);  // First chunk orig size
    offset += sizeof(uint32_t);  // First chunk filtered size
    auto filter_metadata_size = tile.filtered_buffer().value_at_as<uint32_t>(
        offset);  // First chunk metadata size
    offset += sizeof(uint32_t);

    auto max_win_size =
        pipeline.get_filter<PositiveDeltaFilter>()->max_window_size();
    auto expected_num_win =
        (nelts * sizeof(uint64_t)) / max_win_size +
        uint32_t(bool((nelts * sizeof(uint64_t)) % max_win_size));
    CHECK(
        tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
        expected_num_win);  // Number of windows

    // Check encoded size
    auto encoded_size = tile.filtered_buffer().size();
    CHECK(
        encoded_size == pipeline_metadata_size + filter_metadata_size +
                            nelts * sizeof(uint64_t));

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
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
      auto tile = make_increasing_tile(nelts);
      pipeline.get_filter<PositiveDeltaFilter>()->set_max_window_size(
          window_size);

      CHECK(pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp)
                .ok());
      CHECK(tile.size() == 0);
      CHECK(tile.filtered_buffer().size() != 0);

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

  SECTION("- Error on non-positive delta data") {
    auto tile = make_increasing_tile(nelts);
    for (uint64_t i = 0; i < nelts; i++) {
      auto val = nelts - i;
      CHECK_NOTHROW(tile.write(&val, i * sizeof(uint64_t), sizeof(uint64_t)));
    }

    CHECK(
        !pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());
  }
}

TEST_CASE(
    "Filter: Test positive-delta encoding var",
    "[filter][positive-delta][var]") {
  tiledb::sm::Config config;

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

  FilterPipeline pipeline;
  ThreadPool tp(4);
  pipeline.add_filter(PositiveDeltaFilter(Datatype::UINT64));

  SECTION("- Single stage") {
    auto tile = make_increasing_tile(nelts);
    auto offsets_tile = make_offsets_tile(offsets);

    WriterTile::set_max_tile_chunk_size(80);
    CHECK(pipeline.run_forward(&test::g_helper_stats, &tile, &offsets_tile, &tp)
              .ok());

    CHECK(tile.size() == 0);
    CHECK(tile.filtered_buffer().size() != 0);

    uint64_t offset = 0;
    CHECK(
        tile.filtered_buffer().value_at_as<uint64_t>(offset) ==
        9);  // Number of chunks
    offset += sizeof(uint64_t);

    uint64_t total_md_size = 0;
    for (uint64_t i = 0; i < 9; i++) {
      CHECK(
          tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
          out_sizes[i]);  // Chunk orig size
      offset += sizeof(uint32_t);
      CHECK(
          tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
          out_sizes[i]);  // Chunk filtered size
      offset += sizeof(uint32_t);

      uint32_t md_size = tile.filtered_buffer().value_at_as<uint32_t>(offset);
      offset += sizeof(uint32_t);
      total_md_size += md_size;

      auto max_win_size =
          pipeline.get_filter<PositiveDeltaFilter>()->max_window_size();
      auto expected_num_win =
          (nelts * sizeof(uint64_t)) / max_win_size +
          uint32_t(bool((nelts * sizeof(uint64_t)) % max_win_size));
      CHECK(
          tile.filtered_buffer().value_at_as<uint32_t>(offset) ==
          expected_num_win);  // Number of windows

      offset += md_size;

      // Check all elements are good.
      for (uint64_t j = 0; j < out_sizes[i] / sizeof(uint64_t); j++) {
        CHECK(
            tile.filtered_buffer().value_at_as<uint64_t>(offset) ==
            (j == 0 ? 0 : 1));
        offset += sizeof(uint64_t);
      }
    }

    // Check encoded size
    auto pipeline_metadata_size = sizeof(uint64_t) + 9 * 3 * sizeof(uint32_t);
    auto encoded_size = tile.filtered_buffer().size();
    CHECK(
        encoded_size ==
        pipeline_metadata_size + total_md_size + nelts * sizeof(uint64_t));

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
    run_reverse(config, tp, unfiltered_tile, pipeline);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
      CHECK(elt == i);
    }
  }

  SECTION("- Window sizes") {
    WriterTile::set_max_tile_chunk_size(80);
    std::vector<uint32_t> window_sizes = {
        32, 64, 128, 256, 437, 512, 1024, 2000};
    for (auto window_size : window_sizes) {
      auto tile = make_increasing_tile(nelts);
      auto offsets_tile = make_offsets_tile(offsets);

      pipeline.get_filter<PositiveDeltaFilter>()->set_max_window_size(
          window_size);

      CHECK(
          pipeline.run_forward(&test::g_helper_stats, &tile, &offsets_tile, &tp)
              .ok());
      CHECK(tile.size() == 0);
      CHECK(tile.filtered_buffer().size() != 0);

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

  SECTION("- Error on non-positive delta data") {
    auto tile = make_increasing_tile(nelts);
    auto offsets_tile = make_offsets_tile(offsets);

    WriterTile::set_max_tile_chunk_size(80);
    for (uint64_t i = 0; i < nelts; i++) {
      auto val = nelts - i;
      CHECK_NOTHROW(tile.write(&val, i * sizeof(uint64_t), sizeof(uint64_t)));
    }

    CHECK(
        !pipeline.run_forward(&test::g_helper_stats, &tile, &offsets_tile, &tp)
             .ok());
  }

  WriterTile::set_max_tile_chunk_size(constants::max_tile_chunk_size);
}

TEST_CASE("Filter: Test bitshuffle", "[filter][bitshuffle]") {
  tiledb::sm::Config config;

  // Set up test data
  const uint64_t nelts = 1000;
  auto tile = make_increasing_tile(nelts);

  FilterPipeline pipeline;
  ThreadPool tp(4);
  pipeline.add_filter(BitshuffleFilter(Datatype::UINT64));

  SECTION("- Single stage") {
    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());
    CHECK(tile.size() == 0);
    CHECK(tile.filtered_buffer().size() != 0);

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
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

    WriterTile tile2(
        constants::format_version,
        Datatype::UINT32,
        sizeof(uint32_t),
        tile_size2);

    // Set up test data
    for (uint32_t i = 0; i < nelts2; i++) {
      CHECK_NOTHROW(tile2.write(&i, i * sizeof(uint32_t), sizeof(uint32_t)));
    }

    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile2, nullptr, &tp).ok());
    CHECK(tile2.size() == 0);
    CHECK(tile2.filtered_buffer().size() != 0);

    auto unfiltered_tile2 = create_tile_for_unfiltering(nelts2, tile2);
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

  const uint64_t nelts = 100;
  auto tile = make_increasing_tile(nelts);

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

  auto offsets_tile = make_offsets_tile(offsets);

  FilterPipeline pipeline;
  ThreadPool tp(4);
  pipeline.add_filter(BitshuffleFilter(Datatype::UINT64));

  SECTION("- Single stage") {
    WriterTile::set_max_tile_chunk_size(80);
    CHECK(pipeline.run_forward(&test::g_helper_stats, &tile, &offsets_tile, &tp)
              .ok());
    CHECK(tile.size() == 0);
    CHECK(tile.filtered_buffer().size() != 0);

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
    run_reverse(config, tp, unfiltered_tile, pipeline);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
      CHECK(elt == i);
    }
  }

  SECTION("- Indivisible by 8") {
    WriterTile::set_max_tile_chunk_size(80);
    const uint32_t nelts2 = 1001;
    const uint64_t tile_size2 = nelts2 * sizeof(uint32_t);

    WriterTile tile2(
        constants::format_version,
        Datatype::UINT32,
        sizeof(uint32_t),
        tile_size2);

    // Set up test data
    for (uint32_t i = 0; i < nelts2; i++) {
      CHECK_NOTHROW(tile2.write(&i, i * sizeof(uint32_t), sizeof(uint32_t)));
    }

    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile2, &offsets_tile, &tp)
            .ok());
    CHECK(tile2.size() == 0);
    CHECK(tile2.filtered_buffer().size() != 0);

    auto unfiltered_tile2 = create_tile_for_unfiltering(nelts2, tile2);
    run_reverse(config, tp, unfiltered_tile2, pipeline);
    for (uint64_t i = 0; i < nelts2; i++) {
      uint32_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile2.read(&elt, i * sizeof(uint32_t), sizeof(uint32_t)));
      CHECK(elt == i);
    }
  }

  WriterTile::set_max_tile_chunk_size(constants::max_tile_chunk_size);
}

TEST_CASE("Filter: Test byteshuffle", "[filter][byteshuffle]") {
  tiledb::sm::Config config;

  // Set up test data
  const uint64_t nelts = 1000;
  auto tile = make_increasing_tile(nelts);

  FilterPipeline pipeline;
  ThreadPool tp(4);
  pipeline.add_filter(ByteshuffleFilter(Datatype::UINT64));

  SECTION("- Single stage") {
    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());
    CHECK(tile.size() == 0);
    CHECK(tile.filtered_buffer().size() != 0);

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
    run_reverse(config, tp, unfiltered_tile, pipeline);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
      CHECK(elt == i);
    }
  }

  SECTION("- Uneven number of elements") {
    const uint32_t nelts2 = 1001;
    const uint64_t tile_size2 = nelts2 * sizeof(uint32_t);

    WriterTile tile2(
        constants::format_version,
        Datatype::UINT32,
        sizeof(uint32_t),
        tile_size2);

    // Set up test data
    for (uint32_t i = 0; i < nelts2; i++) {
      CHECK_NOTHROW(tile2.write(&i, i * sizeof(uint32_t), sizeof(uint32_t)));
    }

    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile2, nullptr, &tp).ok());
    CHECK(tile2.size() == 0);
    CHECK(tile2.filtered_buffer().size() != 0);

    auto unfiltered_tile2 = create_tile_for_unfiltering(nelts2, tile2);
    run_reverse(config, tp, unfiltered_tile2, pipeline);
    for (uint64_t i = 0; i < nelts2; i++) {
      uint32_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile2.read(&elt, i * sizeof(uint32_t), sizeof(uint32_t)));
      CHECK(elt == i);
    }
  }
}

TEST_CASE("Filter: Test byteshuffle var", "[filter][byteshuffle][var]") {
  tiledb::sm::Config config;

  const uint64_t nelts = 100;
  auto tile = make_increasing_tile(nelts);

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

  auto offsets_tile = make_offsets_tile(offsets);

  FilterPipeline pipeline;
  ThreadPool tp(4);
  pipeline.add_filter(ByteshuffleFilter(Datatype::UINT64));

  SECTION("- Single stage") {
    WriterTile::set_max_tile_chunk_size(80);
    CHECK(pipeline.run_forward(&test::g_helper_stats, &tile, &offsets_tile, &tp)
              .ok());
    CHECK(tile.size() == 0);
    CHECK(tile.filtered_buffer().size() != 0);

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
    run_reverse(config, tp, unfiltered_tile, pipeline);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
      CHECK(elt == i);
    }
  }

  SECTION("- Uneven number of elements") {
    WriterTile::set_max_tile_chunk_size(80);
    const uint32_t nelts2 = 1001;
    const uint64_t tile_size2 = nelts2 * sizeof(uint32_t);

    WriterTile tile2(
        constants::format_version,
        Datatype::UINT32,
        sizeof(uint32_t),
        tile_size2);

    // Set up test data
    for (uint32_t i = 0; i < nelts2; i++) {
      CHECK_NOTHROW(tile2.write(&i, i * sizeof(uint32_t), sizeof(uint32_t)));
    }

    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile2, &offsets_tile, &tp)
            .ok());
    CHECK(tile2.size() == 0);
    CHECK(tile2.filtered_buffer().size() != 0);

    auto unfiltered_tile2 = create_tile_for_unfiltering(nelts2, tile2);
    run_reverse(config, tp, unfiltered_tile2, pipeline);
    for (uint64_t i = 0; i < nelts2; i++) {
      uint32_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile2.read(&elt, i * sizeof(uint32_t), sizeof(uint32_t)));
      CHECK(elt == i);
    }
  }

  WriterTile::set_max_tile_chunk_size(constants::max_tile_chunk_size);
}

TEST_CASE("Filter: Test encryption", "[filter][encryption]") {
  tiledb::sm::Config config;

  // Set up test data
  const uint64_t nelts = 1000;
  auto tile = make_increasing_tile(nelts);

  SECTION("- AES-256-GCM") {
    FilterPipeline pipeline;
    ThreadPool tp(4);
    pipeline.add_filter(EncryptionAES256GCMFilter(Datatype::UINT64));

    // No key set
    CHECK(
        !pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());

    // Create and set a key
    char key[32];
    for (unsigned i = 0; i < 32; i++)
      key[i] = (char)i;
    auto filter = pipeline.get_filter<EncryptionAES256GCMFilter>();
    filter->set_key(key);

    // Check success
    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());
    CHECK(tile.size() == 0);
    CHECK(tile.filtered_buffer().size() != 0);

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
    run_reverse(config, tp, unfiltered_tile, pipeline);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
      CHECK(elt == i);
    }

    // Check error decrypting with wrong key.
    tile = make_increasing_tile(nelts);
    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());
    key[0]++;
    filter->set_key(key);

    unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
    run_reverse(config, tp, unfiltered_tile, pipeline, false);

    // Fix key and check success.
    unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
    key[0]--;
    filter->set_key(key);
    run_reverse(config, tp, unfiltered_tile, pipeline);

    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK_NOTHROW(
          unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
      CHECK(elt == i);
    }
  }
}

template <typename FloatingType, typename IntType>
void testing_float_scaling_filter() {
  tiledb::sm::Config config;

  // Set up test data
  const uint64_t nelts = 100;
  const uint64_t tile_size = nelts * sizeof(FloatingType);
  const uint64_t cell_size = sizeof(FloatingType);

  Datatype t = Datatype::FLOAT32;
  switch (sizeof(FloatingType)) {
    case 4: {
      t = Datatype::FLOAT32;
    } break;
    case 8: {
      t = Datatype::FLOAT64;
    } break;
    default: {
      INFO(
          "testing_float_scaling_filter: passed floating type with size of not "
          "4 bytes or 8 bytes.");
      CHECK(false);
    }
  }

  WriterTile tile(constants::format_version, t, cell_size, tile_size);

  std::vector<FloatingType> float_result_vec;
  double scale = 2.53;
  double foffset = 0.31589;
  uint64_t byte_width = sizeof(IntType);

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_real_distribution<FloatingType> dis(0.0, 213.0);

  for (uint64_t i = 0; i < nelts; i++) {
    FloatingType f = dis(gen);
    CHECK_NOTHROW(
        tile.write(&f, i * sizeof(FloatingType), sizeof(FloatingType)));

    IntType val = static_cast<IntType>(round(
        (f - static_cast<FloatingType>(foffset)) /
        static_cast<FloatingType>(scale)));

    FloatingType val_float = static_cast<FloatingType>(
        scale * static_cast<FloatingType>(val) + foffset);
    float_result_vec.push_back(val_float);
  }

  FilterPipeline pipeline;
  ThreadPool tp(4);
  pipeline.add_filter(FloatScalingFilter(t));
  CHECK(pipeline.get_filter<FloatScalingFilter>()
            ->set_option(FilterOption::SCALE_FLOAT_BYTEWIDTH, &byte_width)
            .ok());
  CHECK(pipeline.get_filter<FloatScalingFilter>()
            ->set_option(FilterOption::SCALE_FLOAT_FACTOR, &scale)
            .ok());
  CHECK(pipeline.get_filter<FloatScalingFilter>()
            ->set_option(FilterOption::SCALE_FLOAT_OFFSET, &foffset)
            .ok());

  CHECK(pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());

  // Check new size and number of chunks
  CHECK(tile.size() == 0);
  CHECK(tile.filtered_buffer().size() != 0);

  auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
  run_reverse(config, tp, unfiltered_tile, pipeline);
  for (uint64_t i = 0; i < nelts; i++) {
    FloatingType elt = 0.0f;
    CHECK_NOTHROW(unfiltered_tile.read(
        &elt, i * sizeof(FloatingType), sizeof(FloatingType)));
    CHECK(elt == float_result_vec[i]);
  }
}

TEMPLATE_TEST_CASE(
    "Filter: Test float scaling",
    "[filter][float-scaling]",
    int8_t,
    int16_t,
    int32_t,
    int64_t) {
  testing_float_scaling_filter<float, TestType>();
  testing_float_scaling_filter<double, TestType>();
}

/*
 Defining distribution types to pass into the testing_xor_filter function.
 */
typedef typename std::uniform_int_distribution<int64_t> IntDistribution;
typedef typename std::uniform_real_distribution<double> FloatDistribution;

template <typename T, typename Distribution = IntDistribution>
void testing_xor_filter(Datatype t) {
  tiledb::sm::Config config;

  // Set up test data
  const uint64_t nelts = 100;
  const uint64_t tile_size = nelts * sizeof(T);
  const uint64_t cell_size = sizeof(T);

  WriterTile tile(constants::format_version, t, cell_size, tile_size);

  // Setting up the random number generator for the XOR filter testing.
  std::mt19937_64 gen(0x57A672DE);
  Distribution dis(
      std::numeric_limits<T>::min(), std::numeric_limits<T>::max());

  std::vector<T> results;

  for (uint64_t i = 0; i < nelts; i++) {
    T val = static_cast<T>(dis(gen));
    CHECK_NOTHROW(tile.write(&val, i * sizeof(T), sizeof(T)));
    results.push_back(val);
  }

  FilterPipeline pipeline;
  ThreadPool tp(4);
  pipeline.add_filter(XORFilter(t));

  CHECK(pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());

  // Check new size and number of chunks
  CHECK(tile.size() == 0);
  CHECK(tile.filtered_buffer().size() != 0);

  auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
  run_reverse(config, tp, unfiltered_tile, pipeline);
  for (uint64_t i = 0; i < nelts; i++) {
    T elt = 0;
    CHECK_NOTHROW(unfiltered_tile.read(&elt, i * sizeof(T), sizeof(T)));
    CHECK(elt == results[i]);
  }
}

TEST_CASE("Filter: Test XOR", "[filter][xor]") {
  testing_xor_filter<int8_t>(Datatype::INT8);
  testing_xor_filter<uint8_t>(Datatype::UINT8);
  testing_xor_filter<int16_t>(Datatype::INT16);
  testing_xor_filter<uint16_t>(Datatype::UINT16);
  testing_xor_filter<int32_t>(Datatype::INT32);
  testing_xor_filter<uint32_t>(Datatype::UINT32);
  testing_xor_filter<int64_t>(Datatype::INT64);
  testing_xor_filter<uint64_t, std::uniform_int_distribution<uint64_t>>(
      Datatype::UINT64);
  testing_xor_filter<float, FloatDistribution>(Datatype::FLOAT32);
  testing_xor_filter<double, FloatDistribution>(Datatype::FLOAT64);
  testing_xor_filter<char>(Datatype::CHAR);
  testing_xor_filter<int64_t>(Datatype::DATETIME_YEAR);
  testing_xor_filter<int64_t>(Datatype::DATETIME_MONTH);
  testing_xor_filter<int64_t>(Datatype::DATETIME_WEEK);
  testing_xor_filter<int64_t>(Datatype::DATETIME_DAY);
  testing_xor_filter<int64_t>(Datatype::DATETIME_HR);
  testing_xor_filter<int64_t>(Datatype::DATETIME_MIN);
  testing_xor_filter<int64_t>(Datatype::DATETIME_SEC);
  testing_xor_filter<int64_t>(Datatype::DATETIME_MS);
  testing_xor_filter<int64_t>(Datatype::DATETIME_US);
  testing_xor_filter<int64_t>(Datatype::DATETIME_NS);
  testing_xor_filter<int64_t>(Datatype::DATETIME_PS);
  testing_xor_filter<int64_t>(Datatype::DATETIME_FS);
  testing_xor_filter<int64_t>(Datatype::DATETIME_AS);
}

TEST_CASE("Filter: Pipeline filtered output types", "[filter][pipeline]") {
  FilterPipeline pipeline;

  SECTION("- DoubleDelta filter reinterprets float->int32") {
    pipeline.add_filter(CompressionFilter(
        tiledb::sm::Compressor::DOUBLE_DELTA,
        0,
        Datatype::FLOAT32,
        Datatype::INT32));
    pipeline.add_filter(BitWidthReductionFilter(Datatype::INT32));
  }

  SECTION("- Delta filter reinterprets float->int32") {
    pipeline.add_filter(CompressionFilter(
        tiledb::sm::Compressor::DELTA, 0, Datatype::FLOAT32, Datatype::INT32));
    pipeline.add_filter(BitWidthReductionFilter(Datatype::INT32));
  }

  SECTION("- FloatScale filter converts float->int32") {
    pipeline.add_filter(
        FloatScalingFilter(sizeof(int32_t), 1.0f, 0.0f, Datatype::FLOAT32));
    pipeline.add_filter(PositiveDeltaFilter(Datatype::INT32));
    pipeline.add_filter(
        CompressionFilter(tiledb::sm::Compressor::DELTA, 0, Datatype::INT32));
    pipeline.add_filter(
        CompressionFilter(tiledb::sm::Compressor::BZIP2, 2, Datatype::INT32));
    pipeline.add_filter(BitshuffleFilter(Datatype::INT32));
    pipeline.add_filter(ByteshuffleFilter(Datatype::INT32));
    pipeline.add_filter(BitWidthReductionFilter(Datatype::INT32));
  }

  size_t byte_width = 0;
  SECTION("- XOR filter expected output types") {
    byte_width = GENERATE(
        sizeof(int8_t), sizeof(int16_t), sizeof(int32_t), sizeof(int64_t));
    pipeline.add_filter(
        FloatScalingFilter(byte_width, 1.0f, 0.0f, Datatype::FLOAT32));
    auto byte_width_t =
        pipeline.get_filter<FloatScalingFilter>()->output_datatype(
            Datatype::FLOAT32);
    pipeline.add_filter(XORFilter(byte_width_t));
  }

  SECTION("- XOR filter expected output types large pipeline") {
    byte_width = GENERATE(
        sizeof(int8_t), sizeof(int16_t), sizeof(int32_t), sizeof(int64_t));
    pipeline.add_filter(
        FloatScalingFilter(byte_width, 1.0f, 0.0f, Datatype::FLOAT32));
    auto byte_width_t =
        pipeline.get_filter<FloatScalingFilter>()->output_datatype(
            Datatype::FLOAT32);
    pipeline.add_filter(PositiveDeltaFilter(byte_width_t));
    pipeline.add_filter(BitshuffleFilter(byte_width_t));
    pipeline.add_filter(ByteshuffleFilter(byte_width_t));
    pipeline.add_filter(XORFilter(byte_width_t));
  }

  // Initial type of tile is float.
  std::vector<float> data = {
      1.0f, 2.1f, 3.2f, 4.3f, 5.4f, 6.5f, 7.6f, 8.7f, 9.8f, 10.9f};
  WriterTile tile(
      constants::format_version,
      Datatype::FLOAT32,
      sizeof(float),
      sizeof(float) * data.size());
  for (size_t i = 0; i < data.size(); i++) {
    CHECK_NOTHROW(tile.write(&data[i], i * sizeof(float), sizeof(float)));
  }

  ThreadPool tp(4);
  REQUIRE(
      pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());
  CHECK(tile.size() == 0);
  CHECK(tile.filtered_buffer().size() != 0);

  auto unfiltered_tile = create_tile_for_unfiltering(data.size(), tile);
  ChunkData chunk_data;
  unfiltered_tile.load_chunk_data(chunk_data);
  REQUIRE(pipeline
              .run_reverse(
                  &test::g_helper_stats,
                  &unfiltered_tile,
                  nullptr,
                  chunk_data,
                  0,
                  chunk_data.filtered_chunks_.size(),
                  tp.concurrency_level(),
                  tiledb::sm::Config())
              .ok());
  std::vector<float> results{
      1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 7.0f, 8.0f, 9.0f, 10.0f, 11.0f};
  for (size_t i = 0; i < data.size(); i++) {
    float val = 0;
    CHECK_NOTHROW(unfiltered_tile.read(&val, i * sizeof(float), sizeof(float)));
    if (pipeline.has_filter(tiledb::sm::FilterType::FILTER_SCALE_FLOAT)) {
      // Loss of precision from rounding in FloatScale filter.
      CHECK(val == results[i]);
    } else {
      CHECK(val == data[i]);
    }
  }
}

TEST_CASE(
    "C++ API: Pipeline with filtered type conversions",
    "[cppapi][filter][pipeline]") {
  tiledb::Context ctx;
  tiledb::VFS vfs(ctx);
  std::string array_name = "cpp_test_array";
  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }

  tiledb::Domain domain(ctx);
  float domain_lo = static_cast<float>(std::numeric_limits<int64_t>::min());
  float domain_hi = static_cast<float>(std::numeric_limits<int64_t>::max() - 1);

  // Create and initialize dimension.
  auto d1 = tiledb::Dimension::create<float>(
      ctx, "d1", {{domain_lo, domain_hi}}, 2048);

  tiledb::Filter float_scale(ctx, TILEDB_FILTER_SCALE_FLOAT);
  double scale = 1.0f;
  double offset = 0.0f;
  uint64_t byte_width = sizeof(int32_t);

  // Float scale converting tile data from float->int32
  float_scale.set_option(TILEDB_SCALE_FLOAT_BYTEWIDTH, &byte_width);
  float_scale.set_option(TILEDB_SCALE_FLOAT_FACTOR, &scale);
  float_scale.set_option(TILEDB_SCALE_FLOAT_OFFSET, &offset);

  // Delta filter reinterprets int32->uint32
  tiledb::Filter delta(ctx, TILEDB_FILTER_DELTA);

  // Pass uint32 data to BitWidthReduction filter
  tiledb::Filter bit_width_reduction(ctx, TILEDB_FILTER_BIT_WIDTH_REDUCTION);

  tiledb::FilterList filters(ctx);
  filters.add_filter(float_scale);
  filters.add_filter(delta);
  filters.add_filter(bit_width_reduction);

  // Apply filters to both attribute and dimension.
  REQUIRE_NOTHROW(d1.set_filter_list(filters));
  domain.add_dimension(d1);

  auto a1 = tiledb::Attribute::create<float>(ctx, "a1");
  REQUIRE_NOTHROW(a1.set_filter_list(filters));

  tiledb::ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain);
  schema.add_attribute(a1);
  schema.set_cell_order(TILEDB_ROW_MAJOR);
  schema.set_tile_order(TILEDB_ROW_MAJOR);
  REQUIRE_NOTHROW(tiledb::Array::create(array_name, schema));
  std::vector<float> d1_data = {
      1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f, 8.0f, 9.0f, 10.0f};
  std::vector<float> a1_data = {
      1.0f, 2.1f, 3.2f, 4.3f, 5.4f, 6.5f, 7.6f, 8.7f, 9.8f, 10.9f};

  // Write to array.
  {
    tiledb::Array array(ctx, array_name, TILEDB_WRITE);
    tiledb::Query query(ctx, array);
    query.set_data_buffer("d1", d1_data);
    query.set_data_buffer("a1", a1_data);
    query.submit();
    CHECK(tiledb::Query::Status::COMPLETE == query.query_status());
  }

  // Read from array.
  {
    std::vector<float> d1_read(10);
    std::vector<float> a1_read(10);
    tiledb::Array array(ctx, array_name, TILEDB_READ);
    tiledb::Query query(ctx, array);
    query.set_subarray({domain_lo, domain_hi});
    query.set_data_buffer("a1", a1_read);
    query.set_data_buffer("d1", d1_read);
    query.submit();
    CHECK(tiledb::Query::Status::COMPLETE == query.query_status());
    // Some loss of precision from rounding in FloatScale.
    CHECK(
        std::vector<float>{
            1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 7.0f, 8.0f, 9.0f, 10.0f, 11.0f} ==
        a1_read);
    CHECK(d1_data == d1_read);
  }

  // Cleanup.
  if (vfs.is_dir(array_name)) {
    vfs.remove_dir(array_name);
  }
}

TEST_CASE(
    "C++ API: Filter pipeline validation",
    "[cppapi][filter][pipeline][validation]") {
  tiledb::Context ctx;

  tiledb::Domain domain(ctx);
  float domain_lo = static_cast<float>(std::numeric_limits<int64_t>::min());
  float domain_hi = static_cast<float>(std::numeric_limits<int64_t>::max() - 1);
  auto d1 = tiledb::Dimension::create<float>(
      ctx, "d1", {{domain_lo, domain_hi}}, 2048);
  auto a1 = tiledb::Attribute::create<float>(ctx, "a1");

  // FloatScale used for testing different float->integral pipelines.
  tiledb::Filter float_scale(ctx, TILEDB_FILTER_SCALE_FLOAT);
  double scale = 1.0f;
  double offset = 0.0f;
  uint64_t byte_width = sizeof(int32_t);
  // Float scale converting tile data from float->int32
  float_scale.set_option(TILEDB_SCALE_FLOAT_BYTEWIDTH, &byte_width);
  float_scale.set_option(TILEDB_SCALE_FLOAT_FACTOR, &scale);
  float_scale.set_option(TILEDB_SCALE_FLOAT_OFFSET, &offset);

  tiledb::FilterList filters(ctx);
  SECTION("- FloatScale filter accepts float or double byte width input") {
    auto d2 = tiledb::Dimension::create<int8_t>(ctx, "d2", {{1, 100}}, 10);
    auto a2 = tiledb::Attribute::create<int32_t>(ctx, "a2");
    filters.add_filter(float_scale);
    CHECK_THROWS(d2.set_filter_list(filters));
    CHECK_NOTHROW(a2.set_filter_list(filters));
  }

  SECTION("- Delta filters do not accept real datatypes") {
    auto test_filter = GENERATE(
        TILEDB_FILTER_POSITIVE_DELTA,
        TILEDB_FILTER_DOUBLE_DELTA,
        TILEDB_FILTER_DELTA);
    tiledb::Filter delta_filter(ctx, test_filter);
    filters.add_filter(delta_filter);
    // Delta compressors don't accept floats. Should fail without FloatScale.
    CHECK_THROWS(d1.set_filter_list(filters));
    CHECK_THROWS(a1.set_filter_list(filters));

    // Test using FloatScale to convert to integral is accepted.
    tiledb::FilterList filters2(ctx);
    filters2.add_filter(float_scale);
    filters2.add_filter(delta_filter);
    CHECK_NOTHROW(d1.set_filter_list(filters2));
    CHECK_NOTHROW(a1.set_filter_list(filters2));
  }

  SECTION("- Webp filter supports only uint8 attributes") {
    if (webp_filter_exists) {
      tiledb::Filter webp(ctx, TILEDB_FILTER_WEBP);
      filters.add_filter(webp);
      CHECK_THROWS(d1.set_filter_list(filters));
      CHECK_THROWS(a1.set_filter_list(filters));
    }
  }

  SECTION("- Bit width reduction filter supports integral input") {
    tiledb::Filter bit_width_reduction(ctx, TILEDB_FILTER_BIT_WIDTH_REDUCTION);
    filters.add_filter(bit_width_reduction);
    CHECK_THROWS(d1.set_filter_list(filters));
    CHECK_THROWS(a1.set_filter_list(filters));

    // Test using FloatScale to convert to integral is accepted.
    tiledb::FilterList filters2(ctx);
    filters2.add_filter(float_scale);
    filters2.add_filter(bit_width_reduction);
    CHECK_NOTHROW(d1.set_filter_list(filters2));
    CHECK_NOTHROW(a1.set_filter_list(filters2));
  }

  SECTION("- XOR filter interprets datatype as integral") {
    // Datatype byte size must match size of int8, int16, int32, or int64
    tiledb::Filter xor_filter(ctx, TILEDB_FILTER_XOR);
    filters.add_filter(xor_filter);
    CHECK_NOTHROW(d1.set_filter_list(filters));
    CHECK_NOTHROW(a1.set_filter_list(filters));
  }

  SECTION("- Multiple compressors") {
    tiledb::Filter bzip(ctx, TILEDB_FILTER_BZIP2);
    auto compressor = GENERATE(
        TILEDB_FILTER_GZIP,
        TILEDB_FILTER_LZ4,
        TILEDB_FILTER_RLE,
        TILEDB_FILTER_ZSTD);
    tiledb::Filter compressor_filter(ctx, compressor);
    filters.add_filter(bzip);
    filters.add_filter(compressor_filter);

    CHECK_NOTHROW(d1.set_filter_list(filters));
    CHECK_NOTHROW(a1.set_filter_list(filters));

    // Should throw without FloatScale to convert float->int32.
    auto delta_compressor = GENERATE(
        TILEDB_FILTER_POSITIVE_DELTA,
        TILEDB_FILTER_DOUBLE_DELTA,
        TILEDB_FILTER_DELTA);
    tiledb::Filter delta_filter(ctx, delta_compressor);
    filters.add_filter(delta_filter);
    CHECK_THROWS(d1.set_filter_list(filters));
    CHECK_THROWS(a1.set_filter_list(filters));
  }

  SECTION("- Multiple compressors following type conversion") {
    auto compressor = GENERATE(
        TILEDB_FILTER_DOUBLE_DELTA,
        TILEDB_FILTER_DELTA,
        TILEDB_FILTER_GZIP,
        TILEDB_FILTER_LZ4,
        TILEDB_FILTER_RLE,
        TILEDB_FILTER_ZSTD);
    tiledb::Filter compressor_filter(ctx, compressor);
    tiledb::Filter bzip(ctx, TILEDB_FILTER_BZIP2);
    filters.add_filter(float_scale);
    filters.add_filter(bzip);
    filters.add_filter(compressor_filter);

    CHECK_NOTHROW(d1.set_filter_list(filters));
    CHECK_NOTHROW(a1.set_filter_list(filters));
  }
}
