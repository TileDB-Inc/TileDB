/**
 * @file unit-run-filter-pipeline.cc
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
 * Tests running the filter pipeline forward and backward.
 */

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
#include "../float_scaling_filter.h"
#include "../positive_delta_filter.h"
#include "../webp_filter.h"
#include "../xor_filter.h"
#include "tiledb/common/logger_public.h"
#include "tiledb/sm/crypto/encryption_key.h"
#include "tiledb/sm/enums/compressor.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/filter_option.h"
#include "tiledb/sm/enums/filter_type.h"
#include "tiledb/sm/stats/stats.h"
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
 * Simple filter that modifies the input stream by adding a constant value to
 * every input element.
 */
class AddNInPlace : public tiledb::sm::Filter {
 public:
  // Just use a dummy filter type
  AddNInPlace(Datatype filter_data_type)
      : Filter(FilterType::FILTER_NONE, filter_data_type) {
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
    auto clone = new AddNInPlace(filter_data_type_);
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

/**
 * Simple filter that increments every element of the input stream, writing the
 * output to a new buffer. The input metadata is treated as a part of the input
 * data.
 */
class Add1IncludingMetadataFilter : public tiledb::sm::Filter {
 public:
  // Just use a dummy filter type
  Add1IncludingMetadataFilter(Datatype filter_data_type)
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
    return new Add1IncludingMetadataFilter(filter_data_type_);
  }
};

TEST_CASE("Filter: Test empty pipeline", "[filter][empty-pipeline]") {
  tiledb::sm::Config config;

  // Set up test data
  const uint64_t nelts = 100;
  auto tile = make_increasing_tile(nelts);

  FilterPipeline pipeline;
  ThreadPool tp(4);
  CHECK(pipeline.run_forward(&dummy_stats, &tile, nullptr, &tp).ok());

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
    CHECK_NOTHROW(
        unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
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
  CHECK(pipeline.run_forward(&dummy_stats, &tile, &offsets_tile, &tp).ok());

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
    CHECK_NOTHROW(
        unfiltered_tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)));
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
  pipeline.add_filter(Add1InPlace(Datatype::UINT64));

  SECTION("- Single stage") {
    CHECK(pipeline.run_forward(&dummy_stats, &tile, nullptr, &tp).ok());

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
  pipeline.add_filter(Add1InPlace(Datatype::UINT64));

  SECTION("- Single stage") {
    WriterTile::set_max_tile_chunk_size(80);
    CHECK(pipeline.run_forward(&dummy_stats, &tile, &offsets_tile, &tp).ok());

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
  pipeline.add_filter(Add1OutOfPlace(Datatype::UINT64));

  SECTION("- Single stage") {
    WriterTile::set_max_tile_chunk_size(80);
    CHECK(pipeline.run_forward(&dummy_stats, &tile, &offsets_tile, &tp).ok());

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
  pipeline.add_filter(Add1InPlace(Datatype::UINT64));
  pipeline.add_filter(Add1OutOfPlace(Datatype::UINT64));
  pipeline.add_filter(Add1InPlace(Datatype::UINT64));
  pipeline.add_filter(Add1OutOfPlace(Datatype::UINT64));
  CHECK(pipeline.run_forward(&dummy_stats, &tile, &offsets_tile, &tp).ok());

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
  pipeline.add_filter(PseudoChecksumFilter(Datatype::UINT64));

  SECTION("- Single stage") {
    WriterTile::set_max_tile_chunk_size(80);
    CHECK(pipeline.run_forward(&dummy_stats, &tile, &offsets_tile, &tp).ok());

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
      []() { return new Add1InPlace(Datatype::UINT64); },
      []() { return new Add1OutOfPlace(Datatype::UINT64); },
      []() { return new Add1IncludingMetadataFilter(Datatype::UINT64); },
      []() { return new BitWidthReductionFilter(Datatype::UINT64); },
      []() { return new BitshuffleFilter(Datatype::UINT64); },
      []() { return new ByteshuffleFilter(Datatype::UINT64); },
      []() {
        return new CompressionFilter(
            tiledb::sm::Compressor::BZIP2, -1, Datatype::UINT64);
      },
      []() { return new PseudoChecksumFilter(Datatype::UINT64); },
      []() { return new ChecksumMD5Filter(Datatype::UINT64); },
      []() { return new ChecksumSHA256Filter(Datatype::UINT64); },
      [&encryption_key]() {
        return new EncryptionAES256GCMFilter(encryption_key, Datatype::UINT64);
      },
  };

  // List of potential filters that must occur at the beginning of the pipeline.
  std::vector<std::function<tiledb::sm::Filter*(void)>> constructors_first = {
      // Pos-delta would (correctly) return error after e.g. compression.
      []() { return new PositiveDeltaFilter(Datatype::UINT64); }};

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
