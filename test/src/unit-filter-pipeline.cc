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
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/crypto/encryption_key.h"
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
    CHECK(tile.write(&i, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
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
    CHECK(offsets_tile
              .write(
                  &offsets[i],
                  i * constants::cell_var_offset_size,
                  constants::cell_var_offset_size)
              .ok());
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
  Add1InPlace()
      : Filter(FilterType::FILTER_NONE) {
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
    return new Add1InPlace();
  }
};

/**
 * Simple filter that increments every element of the input stream, writing the
 * output to a new buffer. Does not modify the input stream.
 */
class Add1OutOfPlace : public tiledb::sm::Filter {
 public:
  // Just use a dummy filter type
  Add1OutOfPlace()
      : Filter(FilterType::FILTER_NONE) {
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
    return new Add1OutOfPlace();
  }
};

/**
 * Simple filter that modifies the input stream by adding a constant value to
 * every input element.
 */
class AddNInPlace : public tiledb::sm::Filter {
 public:
  // Just use a dummy filter type
  AddNInPlace()
      : Filter(FilterType::FILTER_NONE) {
    increment_ = 1;
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
      *val += increment_;
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
      *val -= increment_;
      output->advance_offset(sizeof(uint64_t));
    }

    // Metadata not modified by this filter.
    RETURN_NOT_OK(output_metadata->append_view(input_metadata));

    return Status::Ok();
  }

  uint64_t increment() const {
    return increment_;
  }

  void set_increment(uint64_t increment) {
    increment_ = increment;
  }

  AddNInPlace* clone_impl() const override {
    auto clone = new AddNInPlace();
    clone->increment_ = increment_;
    return clone;
  }

 private:
  uint64_t increment_;
};

/**
 * Simple filter which computes the sum of its input and prepends the sum
 * to the output. In reverse execute, checks that the sum is correct.
 */
class PseudoChecksumFilter : public tiledb::sm::Filter {
 public:
  // Just use a dummy filter type
  PseudoChecksumFilter()
      : Filter(FilterType::FILTER_NONE) {
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
    return new PseudoChecksumFilter();
  }
};

/**
 * Simple filter that increments every element of the input stream, writing the
 * output to a new buffer. The input metadata is treated as a part of the input
 * data.
 */
class Add1IncludingMetadataFilter : public tiledb::sm::Filter {
 public:
  // Just use a dummy filter type
  Add1IncludingMetadataFilter()
      : Filter(FilterType::FILTER_NONE) {
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
    auto input_size = static_cast<uint32_t>(input->size()),
         input_md_size = static_cast<uint32_t>(input_metadata->size());
    auto nelts = input_size / sizeof(uint64_t),
         md_nelts = input_md_size / sizeof(uint64_t);

    // Add a new output buffer.
    RETURN_NOT_OK(output->prepend_buffer(input_size + input_md_size));
    output->reset_offset();

    // Filter input data
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

    // Now filter input metadata.
    for (uint64_t i = 0; i < md_nelts; i++) {
      uint64_t inc;
      RETURN_NOT_OK(input_metadata->read(&inc, sizeof(uint64_t)));
      inc++;
      RETURN_NOT_OK(output->write(&inc, sizeof(uint64_t)));
    }
    rem = input_md_size % sizeof(uint64_t);
    for (unsigned i = 0; i < rem; i++) {
      char byte;
      RETURN_NOT_OK(input_metadata->read(&byte, sizeof(char)));
      RETURN_NOT_OK(output->write(&byte, sizeof(char)));
    }

    // Because this filter modifies the input metadata, we need output metadata
    // that allows the original metadata to be reconstructed on reverse. Also
    // note that contrary to most filters, we don't forward the input metadata.
    RETURN_NOT_OK(output_metadata->prepend_buffer(2 * sizeof(uint32_t)));
    RETURN_NOT_OK(output_metadata->write(&input_size, sizeof(uint32_t)));
    RETURN_NOT_OK(output_metadata->write(&input_md_size, sizeof(uint32_t)));

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

    if (input_metadata->size() != 2 * sizeof(uint32_t))
      return Status_FilterError("Unexpected input metadata length");

    uint32_t orig_input_size, orig_md_size;
    RETURN_NOT_OK(input_metadata->read(&orig_input_size, sizeof(uint32_t)));
    RETURN_NOT_OK(input_metadata->read(&orig_md_size, sizeof(uint32_t)));

    // Add a new output buffer.
    RETURN_NOT_OK(output->prepend_buffer(orig_input_size));
    // Add a new output metadata buffer.
    RETURN_NOT_OK(output_metadata->prepend_buffer(orig_md_size));

    // Restore original data
    auto nelts = orig_input_size / sizeof(uint64_t);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t inc;
      RETURN_NOT_OK(input->read(&inc, sizeof(uint64_t)));
      inc--;
      RETURN_NOT_OK(output->write(&inc, sizeof(uint64_t)));
    }
    auto rem = orig_input_size % sizeof(uint64_t);
    for (unsigned i = 0; i < rem; i++) {
      char byte;
      RETURN_NOT_OK(input->read(&byte, sizeof(char)));
      RETURN_NOT_OK(output->write(&byte, sizeof(char)));
    }

    // Restore original metadata
    nelts = orig_md_size / sizeof(uint64_t);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t inc;
      RETURN_NOT_OK(input->read(&inc, sizeof(uint64_t)));
      inc--;
      RETURN_NOT_OK(output_metadata->write(&inc, sizeof(uint64_t)));
    }
    rem = orig_md_size % sizeof(uint64_t);
    for (unsigned i = 0; i < rem; i++) {
      char byte;
      RETURN_NOT_OK(input->read(&byte, sizeof(char)));
      RETURN_NOT_OK(output_metadata->write(&byte, sizeof(char)));
    }

    return Status::Ok();
  }

  Add1IncludingMetadataFilter* clone_impl() const override {
    return new Add1IncludingMetadataFilter;
  }
};

TEST_CASE("Filter: Test empty pipeline", "[filter][empty-pipeline]") {
  tiledb::sm::Config config;

  // Set up test data
  const uint64_t nelts = 100;
  auto tile = make_increasing_tile(nelts);

  FilterPipeline pipeline;
  ThreadPool tp(4);
  CHECK(pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());

  // Check new size and number of chunks
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

  // Check all elements unchanged.
  for (uint64_t i = 0; i < nelts; i++) {
    CHECK(tile.filtered_buffer().value_at_as<uint64_t>(offset) == i);
    offset += sizeof(uint64_t);
  }

  auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
  run_reverse(config, tp, unfiltered_tile, pipeline);
  for (uint64_t i = 0; i < nelts; i++) {
    uint64_t elt = 0;
    CHECK(unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t))
              .ok());
    CHECK(elt == i);
  }
}

TEST_CASE(
    "Filter: Test empty pipeline var sized", "[filter][empty-pipeline][var]") {
  tiledb::sm::Config config;

  // Set up test data
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
  WriterTile::set_max_tile_chunk_size(80);
  CHECK(pipeline.run_forward(&test::g_helper_stats, &tile, &offsets_tile, &tp)
            .ok());

  // Check new size and number of chunks
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
    CHECK(unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t))
              .ok());
    CHECK(elt == i);
  }

  WriterTile::set_max_tile_chunk_size(constants::max_tile_chunk_size);
}

TEST_CASE(
    "Filter: Test simple in-place pipeline", "[filter][simple-in-place]") {
  tiledb::sm::Config config;

  const uint64_t nelts = 100;
  auto tile = make_increasing_tile(nelts);

  FilterPipeline pipeline;
  ThreadPool tp(4);
  pipeline.add_filter(Add1InPlace());

  SECTION("- Single stage") {
    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());

    // Check new size and number of chunks
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
      CHECK(unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t))
                .ok());
      CHECK(elt == i);
    }
  }

  SECTION("- Multi-stage") {
    // Add a few more +1 filters and re-run.
    pipeline.add_filter(Add1InPlace());
    pipeline.add_filter(Add1InPlace());
    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());

    // Check new size and number of chunks
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
      CHECK(unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t))
                .ok());
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
  pipeline.add_filter(Add1InPlace());

  SECTION("- Single stage") {
    WriterTile::set_max_tile_chunk_size(80);
    CHECK(pipeline.run_forward(&test::g_helper_stats, &tile, &offsets_tile, &tp)
              .ok());

    // Check new size and number of chunks
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
      CHECK(unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t))
                .ok());
      CHECK(elt == i);
    }
  }

  SECTION("- Multi-stage") {
    // Add a few more +1 filters and re-run.
    WriterTile::set_max_tile_chunk_size(80);
    pipeline.add_filter(Add1InPlace());
    pipeline.add_filter(Add1InPlace());
    CHECK(pipeline.run_forward(&test::g_helper_stats, &tile, &offsets_tile, &tp)
              .ok());

    // Check new size and number of chunks
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
      CHECK(unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t))
                .ok());
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
  pipeline.add_filter(Add1OutOfPlace());

  SECTION("- Single stage") {
    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());

    // Check new size and number of chunks
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
      CHECK(unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t))
                .ok());
      CHECK(elt == i);
    }
  }

  SECTION("- Multi-stage") {
    // Add a few more +1 filters and re-run.
    pipeline.add_filter(Add1OutOfPlace());
    pipeline.add_filter(Add1OutOfPlace());
    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());

    // Check new size and number of chunks
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
      CHECK(unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t))
                .ok());
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
  pipeline.add_filter(Add1OutOfPlace());

  SECTION("- Single stage") {
    WriterTile::set_max_tile_chunk_size(80);
    CHECK(pipeline.run_forward(&test::g_helper_stats, &tile, &offsets_tile, &tp)
              .ok());

    // Check new size and number of chunks
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
      CHECK(unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t))
                .ok());
      CHECK(elt == i);
    }
  }

  SECTION("- Multi-stage") {
    // Add a few more +1 filters and re-run.
    WriterTile::set_max_tile_chunk_size(80);
    pipeline.add_filter(Add1OutOfPlace());
    pipeline.add_filter(Add1OutOfPlace());
    CHECK(pipeline.run_forward(&test::g_helper_stats, &tile, &offsets_tile, &tp)
              .ok());

    // Check new size and number of chunks
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
      CHECK(unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t))
                .ok());
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
  pipeline.add_filter(Add1InPlace());
  pipeline.add_filter(Add1OutOfPlace());
  pipeline.add_filter(Add1InPlace());
  pipeline.add_filter(Add1OutOfPlace());
  CHECK(pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());

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
    CHECK(unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t))
              .ok());
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
  WriterTile::set_max_tile_chunk_size(80);
  pipeline.add_filter(Add1InPlace());
  pipeline.add_filter(Add1OutOfPlace());
  pipeline.add_filter(Add1InPlace());
  pipeline.add_filter(Add1OutOfPlace());
  CHECK(pipeline.run_forward(&test::g_helper_stats, &tile, &offsets_tile, &tp)
            .ok());

  // Check new size and number of chunks
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
    CHECK(unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t))
              .ok());
    CHECK(elt == i);
  }

  WriterTile::set_max_tile_chunk_size(constants::max_tile_chunk_size);
}

TEST_CASE("Filter: Test compression", "[filter][compression]") {
  tiledb::sm::Config config;

  const uint64_t nelts = 100;
  auto tile = make_increasing_tile(nelts);

  // Set up dummy array schema (needed by compressor filter for cell size, etc).
  uint32_t dim_dom[] = {1, 10};
  auto dim{make_shared<tiledb::sm::Dimension>(HERE(), "", Datatype::INT32)};
  CHECK(dim->set_domain(dim_dom).ok());
  tiledb::sm::Domain domain;
  CHECK(domain.add_dimension(dim).ok());
  tiledb::sm::ArraySchema schema;
  tiledb::sm::Attribute attr("attr", Datatype::UINT64);
  CHECK(schema.add_attribute(make_shared<tiledb::sm::Attribute>(HERE(), &attr))
            .ok());
  CHECK(
      schema.set_domain(make_shared<tiledb::sm::Domain>(HERE(), &domain)).ok());
  CHECK(schema.init().ok());

  FilterPipeline pipeline;
  ThreadPool tp(4);

  SECTION("- Simple") {
    pipeline.add_filter(Add1InPlace());
    pipeline.add_filter(Add1OutOfPlace());
    pipeline.add_filter(CompressionFilter(tiledb::sm::Compressor::LZ4, 5));

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
      CHECK(unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t))
                .ok());
      CHECK(elt == i);
    }
  }

  SECTION("- With checksum stage") {
    pipeline.add_filter(PseudoChecksumFilter());
    pipeline.add_filter(CompressionFilter(tiledb::sm::Compressor::LZ4, 5));

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
      CHECK(unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t))
                .ok());
      CHECK(elt == i);
    }
  }

  SECTION("- With multiple stages") {
    pipeline.add_filter(Add1InPlace());
    pipeline.add_filter(PseudoChecksumFilter());
    pipeline.add_filter(Add1OutOfPlace());
    pipeline.add_filter(CompressionFilter(tiledb::sm::Compressor::LZ4, 5));

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
      CHECK(unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t))
                .ok());
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
  tiledb::sm::Domain domain;
  CHECK(domain.add_dimension(dim).ok());
  tiledb::sm::ArraySchema schema;
  tiledb::sm::Attribute attr("attr", Datatype::UINT64);
  CHECK(schema.add_attribute(make_shared<tiledb::sm::Attribute>(HERE(), &attr))
            .ok());
  CHECK(
      schema.set_domain(make_shared<tiledb::sm::Domain>(HERE(), &domain)).ok());
  CHECK(schema.init().ok());

  FilterPipeline pipeline;
  ThreadPool tp(4);

  SECTION("- Simple") {
    WriterTile::set_max_tile_chunk_size(80);
    pipeline.add_filter(Add1InPlace());
    pipeline.add_filter(Add1OutOfPlace());
    pipeline.add_filter(CompressionFilter(tiledb::sm::Compressor::LZ4, 5));

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
      CHECK(unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t))
                .ok());
      CHECK(elt == i);
    }
  }

  SECTION("- With checksum stage") {
    WriterTile::set_max_tile_chunk_size(80);
    pipeline.add_filter(PseudoChecksumFilter());
    pipeline.add_filter(CompressionFilter(tiledb::sm::Compressor::LZ4, 5));

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
      CHECK(unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t))
                .ok());
      CHECK(elt == i);
    }
  }

  SECTION("- With multiple stages") {
    WriterTile::set_max_tile_chunk_size(80);
    pipeline.add_filter(Add1InPlace());
    pipeline.add_filter(PseudoChecksumFilter());
    pipeline.add_filter(Add1OutOfPlace());
    pipeline.add_filter(CompressionFilter(tiledb::sm::Compressor::LZ4, 5));

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
      CHECK(unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t))
                .ok());
      CHECK(elt == i);
    }
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
  pipeline.add_filter(PseudoChecksumFilter());

  SECTION("- Single stage") {
    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());

    // Check new size and number of chunks
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
      CHECK(unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t))
                .ok());
      CHECK(elt == i);
    }
  }

  SECTION("- Multi-stage") {
    pipeline.add_filter(Add1OutOfPlace());
    pipeline.add_filter(Add1InPlace());
    pipeline.add_filter(PseudoChecksumFilter());
    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());

    // Compute the second (final) checksum value.
    uint64_t expected_checksum_2 = 0;
    for (uint64_t i = 0; i < nelts; i++)
      expected_checksum_2 += i + 2;

    // Check new size and number of chunks.
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
      CHECK(unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t))
                .ok());
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

  std::vector<uint64_t> expected_checksums{
      91, 99, 275, 238, 425, 525, 1350, 825, 1122};

  FilterPipeline pipeline;
  ThreadPool tp(4);
  pipeline.add_filter(PseudoChecksumFilter());

  SECTION("- Single stage") {
    WriterTile::set_max_tile_chunk_size(80);
    CHECK(pipeline.run_forward(&test::g_helper_stats, &tile, &offsets_tile, &tp)
              .ok());

    // Check new size and number of chunks
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
      CHECK(unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t))
                .ok());
      CHECK(elt == i);
    }
  }

  SECTION("- Multi-stage") {
    WriterTile::set_max_tile_chunk_size(80);
    pipeline.add_filter(Add1OutOfPlace());
    pipeline.add_filter(Add1InPlace());
    pipeline.add_filter(PseudoChecksumFilter());
    CHECK(pipeline.run_forward(&test::g_helper_stats, &tile, &offsets_tile, &tp)
              .ok());

    std::vector<uint64_t> expected_checksums2{
        119, 111, 297, 252, 445, 545, 1390, 845, 1146};

    // Check new size and number of chunks
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
      CHECK(unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t))
                .ok());
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
  pipeline.add_filter(Add1InPlace());
  pipeline.add_filter(AddNInPlace());
  pipeline.add_filter(Add1InPlace());

  // Get non-existent filter instance
  auto* cksum = pipeline.get_filter<PseudoChecksumFilter>();
  CHECK(cksum == nullptr);

  // Modify +N filter
  auto* add_n = pipeline.get_filter<AddNInPlace>();
  CHECK(add_n != nullptr);
  add_n->set_increment(2);

  CHECK(pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());

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
    CHECK(unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t))
              .ok());
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
  pipeline.add_filter(Add1InPlace());
  pipeline.add_filter(AddNInPlace());
  pipeline.add_filter(Add1InPlace());

  // Get non-existent filter instance
  auto* cksum = pipeline.get_filter<PseudoChecksumFilter>();
  CHECK(cksum == nullptr);

  // Modify +N filter
  auto* add_n = pipeline.get_filter<AddNInPlace>();
  CHECK(add_n != nullptr);
  add_n->set_increment(2);

  WriterTile::set_max_tile_chunk_size(80);
  CHECK(pipeline.run_forward(&test::g_helper_stats, &tile, &offsets_tile, &tp)
            .ok());

  // Check new size and number of chunks
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
    CHECK(unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t))
              .ok());
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
  pipeline.add_filter(Add1InPlace());
  pipeline.add_filter(AddNInPlace());
  pipeline.add_filter(Add1InPlace());
  pipeline.add_filter(PseudoChecksumFilter());

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

  CHECK(pipeline_copy.run_forward(&test::g_helper_stats, &tile, nullptr, &tp)
            .ok());

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
    CHECK(unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t))
              .ok());
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
      []() { return new Add1InPlace(); },
      []() { return new Add1OutOfPlace(); },
      []() { return new Add1IncludingMetadataFilter(); },
      []() { return new BitWidthReductionFilter(); },
      []() { return new BitshuffleFilter(); },
      []() { return new ByteshuffleFilter(); },
      []() { return new CompressionFilter(tiledb::sm::Compressor::BZIP2, -1); },
      []() { return new PseudoChecksumFilter(); },
      []() { return new ChecksumMD5Filter(); },
      []() { return new ChecksumSHA256Filter(); },
      [&encryption_key]() {
        return new EncryptionAES256GCMFilter(encryption_key);
      },
  };

  // List of potential filters that must occur at the beginning of the pipeline.
  std::vector<std::function<tiledb::sm::Filter*(void)>> constructors_first = {
      // Pos-delta would (correctly) return error after e.g. compression.
      []() { return new PositiveDeltaFilter(); }};

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
        delete filter;
      } else {
        auto idx = (unsigned)rng_constructors(gen);
        tiledb::sm::Filter* filter = constructors[idx]();
        pipeline.add_filter(*filter);
        delete filter;
      }
    }

    // End result should always be the same as the input.
    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());
    CHECK(tile.size() == 0);
    CHECK(tile.filtered_buffer().size() != 0);

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
    run_reverse(config, tp, unfiltered_tile, pipeline);

    for (uint64_t n = 0; n < nelts; n++) {
      uint64_t elt = 0;
      CHECK(unfiltered_tile.read(&elt, n * sizeof(uint64_t), sizeof(uint64_t))
                .ok());
      CHECK(elt == n);
    }
  }
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
  ChecksumMD5Filter md5_filter;
  md5_pipeline.add_filter(md5_filter);
  CHECK(md5_pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp)
            .ok());
  CHECK(tile.size() == 0);
  CHECK(tile.filtered_buffer().size() != 0);

  auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
  run_reverse(config, tp, unfiltered_tile, md5_pipeline);

  for (uint64_t n = 0; n < nelts; n++) {
    uint64_t elt = 0;
    CHECK(unfiltered_tile.read(&elt, n * sizeof(uint64_t), sizeof(uint64_t))
              .ok());
    CHECK(elt == n);
  }

  // SHA256
  auto tile2 = make_increasing_tile(nelts);

  FilterPipeline sha_256_pipeline;
  ChecksumMD5Filter sha_256_filter;
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
    CHECK(unfiltered_tile2.read(&elt, n * sizeof(uint64_t), sizeof(uint64_t))
              .ok());
    CHECK(elt == n);
  }
}

TEST_CASE("Filter: Test bit width reduction", "[filter][bit-width-reduction]") {
  tiledb::sm::Config config;

  // Set up test data
  const uint64_t nelts = 1000;

  FilterPipeline pipeline;
  ThreadPool tp(4);
  pipeline.add_filter(BitWidthReductionFilter());

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
      CHECK(unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t))
                .ok());
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
        CHECK(unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t))
                  .ok());
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
      CHECK(tile.write(&val, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
    }

    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());
    CHECK(tile.size() == 0);
    CHECK(tile.filtered_buffer().size() != 0);

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
    run_reverse(config, tp, unfiltered_tile, pipeline);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK(unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t))
                .ok());
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
      CHECK(tile.write(&val, i * sizeof(uint32_t), sizeof(uint32_t)).ok());
    }

    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());
    CHECK(tile.size() == 0);
    CHECK(tile.filtered_buffer().size() != 0);

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
    run_reverse(config, tp, unfiltered_tile, pipeline);
    for (uint64_t i = 0; i < nelts; i++) {
      int32_t elt = 0;
      CHECK(unfiltered_tile.read(&elt, i * sizeof(int32_t), sizeof(int32_t))
                .ok());
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
      CHECK(tile.write(&val, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
    }

    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());
    CHECK(tile.size() == 0);
    CHECK(tile.filtered_buffer().size() != 0);

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
    run_reverse(config, tp, unfiltered_tile, pipeline);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK(unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t))
                .ok());
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
  pipeline.add_filter(BitWidthReductionFilter());

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
      CHECK(unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t))
                .ok());
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
        CHECK(unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t))
                  .ok());
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
      CHECK(tile.write(&val, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
    }

    CHECK(pipeline.run_forward(&test::g_helper_stats, &tile, &offsets_tile, &tp)
              .ok());
    CHECK(tile.size() == 0);
    CHECK(tile.filtered_buffer().size() != 0);

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
    run_reverse(config, tp, unfiltered_tile, pipeline);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK(unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t))
                .ok());
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
      CHECK(tile.write(&val, i * sizeof(uint32_t), sizeof(uint32_t)).ok());
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
      CHECK(offsets_tile32
                .write(
                    &offsets32[i],
                    i * constants::cell_var_offset_size,
                    constants::cell_var_offset_size)
                .ok());
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
      CHECK(unfiltered_tile.read(&elt, i * sizeof(int32_t), sizeof(int32_t))
                .ok());
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
      CHECK(tile.write(&val, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
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
      CHECK(unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t))
                .ok());
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
  pipeline.add_filter(PositiveDeltaFilter());

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
      CHECK(unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t))
                .ok());
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
        CHECK(unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t))
                  .ok());
        CHECK(elt == i);
      }
    }
  }

  SECTION("- Error on non-positive delta data") {
    auto tile = make_increasing_tile(nelts);
    for (uint64_t i = 0; i < nelts; i++) {
      auto val = nelts - i;
      CHECK(tile.write(&val, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
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
  pipeline.add_filter(PositiveDeltaFilter());

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
      CHECK(unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t))
                .ok());
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
        CHECK(unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t))
                  .ok());
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
      CHECK(tile.write(&val, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
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
  pipeline.add_filter(BitshuffleFilter());

  SECTION("- Single stage") {
    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());
    CHECK(tile.size() == 0);
    CHECK(tile.filtered_buffer().size() != 0);

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
    run_reverse(config, tp, unfiltered_tile, pipeline);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK(unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t))
                .ok());
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
      CHECK(tile2.write(&i, i * sizeof(uint32_t), sizeof(uint32_t)).ok());
    }

    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile2, nullptr, &tp).ok());
    CHECK(tile2.size() == 0);
    CHECK(tile2.filtered_buffer().size() != 0);

    auto unfiltered_tile2 = create_tile_for_unfiltering(nelts2, tile2);
    run_reverse(config, tp, unfiltered_tile2, pipeline);
    for (uint64_t i = 0; i < nelts2; i++) {
      uint32_t elt = 0;
      CHECK(unfiltered_tile2.read(&elt, i * sizeof(uint32_t), sizeof(uint32_t))
                .ok());
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
  pipeline.add_filter(BitshuffleFilter());

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
      CHECK(unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t))
                .ok());
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
      CHECK(tile2.write(&i, i * sizeof(uint32_t), sizeof(uint32_t)).ok());
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
      CHECK(unfiltered_tile2.read(&elt, i * sizeof(uint32_t), sizeof(uint32_t))
                .ok());
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
  pipeline.add_filter(ByteshuffleFilter());

  SECTION("- Single stage") {
    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());
    CHECK(tile.size() == 0);
    CHECK(tile.filtered_buffer().size() != 0);

    auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
    run_reverse(config, tp, unfiltered_tile, pipeline);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK(unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t))
                .ok());
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
      CHECK(tile2.write(&i, i * sizeof(uint32_t), sizeof(uint32_t)).ok());
    }

    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile2, nullptr, &tp).ok());
    CHECK(tile2.size() == 0);
    CHECK(tile2.filtered_buffer().size() != 0);

    auto unfiltered_tile2 = create_tile_for_unfiltering(nelts2, tile2);
    run_reverse(config, tp, unfiltered_tile2, pipeline);
    for (uint64_t i = 0; i < nelts2; i++) {
      uint32_t elt = 0;
      CHECK(unfiltered_tile2.read(&elt, i * sizeof(uint32_t), sizeof(uint32_t))
                .ok());
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
  pipeline.add_filter(ByteshuffleFilter());

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
      CHECK(unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t))
                .ok());
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
      CHECK(tile2.write(&i, i * sizeof(uint32_t), sizeof(uint32_t)).ok());
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
      CHECK(unfiltered_tile2.read(&elt, i * sizeof(uint32_t), sizeof(uint32_t))
                .ok());
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
    pipeline.add_filter(EncryptionAES256GCMFilter());

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
      CHECK(unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t))
                .ok());
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
      CHECK(unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t))
                .ok());
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
    CHECK(tile.write(&f, i * sizeof(FloatingType), sizeof(FloatingType)).ok());

    IntType val = static_cast<IntType>(round(
        (f - static_cast<FloatingType>(foffset)) /
        static_cast<FloatingType>(scale)));

    FloatingType val_float = static_cast<FloatingType>(
        scale * static_cast<FloatingType>(val) + foffset);
    float_result_vec.push_back(val_float);
  }

  FilterPipeline pipeline;
  ThreadPool tp(4);
  pipeline.add_filter(FloatScalingFilter());
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
  // Tile datatype will be updated to the final filtered type after run_forward.
  // Set the tile datatype to the schema type for context in run_reverse.
  unfiltered_tile.set_datatype(t);
  run_reverse(config, tp, unfiltered_tile, pipeline);
  for (uint64_t i = 0; i < nelts; i++) {
    FloatingType elt = 0.0f;
    CHECK(unfiltered_tile
              .read(&elt, i * sizeof(FloatingType), sizeof(FloatingType))
              .ok());
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
    CHECK(tile.write(&val, i * sizeof(T), sizeof(T)).ok());
    results.push_back(val);
  }

  FilterPipeline pipeline;
  ThreadPool tp(4);
  pipeline.add_filter(XORFilter());

  CHECK(pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());

  // Check new size and number of chunks
  CHECK(tile.size() == 0);
  CHECK(tile.filtered_buffer().size() != 0);

  auto unfiltered_tile = create_tile_for_unfiltering(nelts, tile);
  run_reverse(config, tp, unfiltered_tile, pipeline);
  for (uint64_t i = 0; i < nelts; i++) {
    T elt = 0;
    CHECK(unfiltered_tile.read(&elt, i * sizeof(T), sizeof(T)).ok());
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
  testing_xor_filter<uint64_t>(Datatype::UINT64);
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
  // Float scale converting tile data from float->int32
  pipeline.add_filter(FloatScalingFilter(sizeof(int32_t), 1.0f, 0.0f));
  SECTION("- Delta filter reinterprets int32->uint32") {
    // Test with / without these filters to test tile types.
    pipeline.add_filter(
        CompressionFilter(tiledb::sm::Compressor::DELTA, 0, Datatype::UINT32));
    pipeline.add_filter(BitWidthReductionFilter());
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
    CHECK(tile.write(&data[i], i * sizeof(float), sizeof(float)).ok());
  }

  ThreadPool tp(4);
  CHECK(pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());
  CHECK(tile.size() == 0);
  CHECK(tile.filtered_buffer().size() != 0);
  if (pipeline.has_filter(tiledb::sm::FilterType::FILTER_DELTA)) {
    // FloatScale->Delta->BitWidthReduction final filtered datatype is uint32_t.
    CHECK(tile.type() == Datatype::UINT32);
  } else {
    // FloatScale filtered datatype is int32_t.
    CHECK(tile.type() == Datatype::INT32);
  }

  auto unfiltered_tile = create_tile_for_unfiltering(data.size(), tile);
  unfiltered_tile.set_datatype(Datatype::FLOAT32);
  run_reverse(tiledb::sm::Config(), tp, unfiltered_tile, pipeline);
  std::vector<float> results{
      1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 7.0f, 8.0f, 9.0f, 10.0f, 11.0f};
  for (size_t i = 0; i < data.size(); i++) {
    float val = 0;
    CHECK(unfiltered_tile.read(&val, i * sizeof(float), sizeof(float)).ok());
    CHECK(val == results[i]);
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
  delta.set_option(TILEDB_COMPRESSION_REINTERPRET_DATATYPE, TILEDB_UINT32);

  // Pass uint32 data to BitWidthReduction filter
  tiledb::Filter bit_width_reduction(ctx, TILEDB_FILTER_BIT_WIDTH_REDUCTION);

  tiledb::FilterList filters(ctx);
  filters.add_filter(float_scale);
  filters.add_filter(delta);
  filters.add_filter(bit_width_reduction);

  // Apply filters to both attribute and dimension.
  d1.set_filter_list(filters);
  domain.add_dimension(d1);

  auto a1 = tiledb::Attribute::create<float>(ctx, "a1");
  a1.set_filter_list(filters);

  tiledb::ArraySchema schema(ctx, TILEDB_SPARSE);
  schema.set_domain(domain);
  schema.add_attribute(a1);
  schema.set_cell_order(TILEDB_ROW_MAJOR);
  schema.set_tile_order(TILEDB_ROW_MAJOR);
  CHECK_NOTHROW(tiledb::Array::create(array_name, schema));
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
