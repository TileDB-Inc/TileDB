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

#include "test/src/helpers.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/crypto/encryption_key.h"
#include "tiledb/sm/enums/compressor.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/filter_type.h"
#include "tiledb/sm/filter/bit_width_reduction_filter.h"
#include "tiledb/sm/filter/bitshuffle_filter.h"
#include "tiledb/sm/filter/byteshuffle_filter.h"
#include "tiledb/sm/filter/checksum_md5_filter.h"
#include "tiledb/sm/filter/checksum_sha256_filter.h"
#include "tiledb/sm/filter/compression_filter.h"
#include "tiledb/sm/filter/encryption_aes256gcm_filter.h"
#include "tiledb/sm/filter/filter_pipeline.h"
#include "tiledb/sm/filter/positive_delta_filter.h"
#include "tiledb/sm/tile/tile.h"

#include <catch.hpp>
#include <functional>
#include <iostream>
#include <random>

using namespace tiledb;
using namespace tiledb::common;
using namespace tiledb::sm;

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
      const Tile&,
      Tile* const,
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
      Tile* const,
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
      const Tile&,
      Tile* const,
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
      Tile* const,
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
      const Tile&,
      Tile* const,
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
      Tile* const,
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
      const Tile&,
      Tile* const,
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
      Tile* const,
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
      const Tile&,
      Tile* const,
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
      Tile* const,
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
  const uint64_t tile_size = nelts * sizeof(uint64_t);
  const uint64_t cell_size = sizeof(uint64_t);
  const uint32_t dim_num = 0;

  Tile tile;
  tile.init_unfiltered(
      constants::format_version,
      Datatype::UINT64,
      tile_size,
      cell_size,
      dim_num);

  // Set up test data
  for (uint64_t i = 0; i < nelts; i++) {
    CHECK(tile.write(&i, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
  }

  FilterPipeline pipeline;
  ThreadPool tp;
  CHECK(tp.init(4).ok());
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

  CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
  CHECK(pipeline.run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
            .ok());
  CHECK(tile.filtered_buffer().size() == 0);
  for (uint64_t i = 0; i < nelts; i++) {
    uint64_t elt = 0;
    CHECK(tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
    CHECK(elt == i);
  }
}

TEST_CASE(
    "Filter: Test empty pipeline var sized", "[filter][empty-pipeline][var]") {
  tiledb::sm::Config config;

  // Set up test data
  const uint64_t nelts = 100;
  const uint64_t tile_size = nelts * sizeof(uint64_t);
  const uint64_t cell_size = sizeof(uint64_t);
  const uint32_t dim_num = 0;

  Tile tile;
  tile.init_unfiltered(
      constants::format_version,
      Datatype::UINT64,
      tile_size,
      cell_size,
      dim_num);

  // Set up test data
  for (uint64_t i = 0; i < nelts; i++) {
    CHECK(tile.write(&i, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
  }

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

  Tile offsets_tile;
  offsets_tile.init_unfiltered(
      constants::format_version,
      Datatype::UINT64,
      offsets_tile_size,
      constants::cell_var_offset_size,
      dim_num);

  // Set up test data
  for (uint64_t i = 0; i < offsets.size(); i++) {
    CHECK(offsets_tile
              .write(
                  &offsets[i],
                  i * constants::cell_var_offset_size,
                  constants::cell_var_offset_size)
              .ok());
  }

  FilterPipeline pipeline;
  ThreadPool tp;
  CHECK(tp.init(4).ok());
  Tile::set_max_tile_chunk_size(80);
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

  CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
  CHECK(pipeline.run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
            .ok());
  CHECK(tile.filtered_buffer().size() == 0);
  for (uint64_t i = 0; i < nelts; i++) {
    uint64_t elt = 0;
    CHECK(tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
    CHECK(elt == i);
  }

  Tile::set_max_tile_chunk_size(constants::max_tile_chunk_size);
}

TEST_CASE(
    "Filter: Test simple in-place pipeline", "[filter][simple-in-place]") {
  tiledb::sm::Config config;

  const uint64_t nelts = 100;
  const uint64_t tile_size = nelts * sizeof(uint64_t);
  const uint64_t cell_size = sizeof(uint64_t);
  const uint32_t dim_num = 0;

  Tile tile;
  tile.init_unfiltered(
      constants::format_version,
      Datatype::UINT64,
      tile_size,
      cell_size,
      dim_num);

  // Set up test data
  for (uint64_t i = 0; i < nelts; i++) {
    CHECK(tile.write(&i, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
  }

  FilterPipeline pipeline;
  ThreadPool tp;
  CHECK(tp.init(4).ok());
  CHECK(pipeline.add_filter(Add1InPlace()).ok());

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

    CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
    CHECK(
        pipeline.run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
            .ok());
    CHECK(tile.filtered_buffer().size() == 0);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK(tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
      CHECK(elt == i);
    }
  }

  SECTION("- Multi-stage") {
    // Add a few more +1 filters and re-run.
    CHECK(pipeline.add_filter(Add1InPlace()).ok());
    CHECK(pipeline.add_filter(Add1InPlace()).ok());
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

    CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
    CHECK(
        pipeline.run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
            .ok());
    CHECK(tile.filtered_buffer().size() == 0);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK(tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
      CHECK(elt == i);
    }
  }
}

TEST_CASE(
    "Filter: Test simple in-place pipeline var",
    "[filter][simple-in-place][var]") {
  tiledb::sm::Config config;

  const uint64_t nelts = 100;
  const uint64_t tile_size = nelts * sizeof(uint64_t);
  const uint64_t cell_size = sizeof(uint64_t);
  const uint32_t dim_num = 0;

  Tile tile;
  tile.init_unfiltered(
      constants::format_version,
      Datatype::UINT64,
      tile_size,
      cell_size,
      dim_num);

  // Set up test data
  for (uint64_t i = 0; i < nelts; i++) {
    CHECK(tile.write(&i, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
  }

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

  Tile offsets_tile;
  offsets_tile.init_unfiltered(
      constants::format_version,
      Datatype::UINT64,
      offsets_tile_size,
      constants::cell_var_offset_size,
      dim_num);

  // Set up test data
  for (uint64_t i = 0; i < offsets.size(); i++) {
    CHECK(offsets_tile
              .write(
                  &offsets[i],
                  i * constants::cell_var_offset_size,
                  constants::cell_var_offset_size)
              .ok());
  }

  FilterPipeline pipeline;
  ThreadPool tp;
  CHECK(tp.init(4).ok());
  CHECK(pipeline.add_filter(Add1InPlace()).ok());

  SECTION("- Single stage") {
    Tile::set_max_tile_chunk_size(80);
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

    CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
    CHECK(
        pipeline.run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
            .ok());
    CHECK(tile.filtered_buffer().size() == 0);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK(tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
      CHECK(elt == i);
    }
  }

  SECTION("- Multi-stage") {
    // Add a few more +1 filters and re-run.
    Tile::set_max_tile_chunk_size(80);
    CHECK(pipeline.add_filter(Add1InPlace()).ok());
    CHECK(pipeline.add_filter(Add1InPlace()).ok());
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

    CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
    CHECK(
        pipeline.run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
            .ok());
    CHECK(tile.filtered_buffer().size() == 0);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK(tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
      CHECK(elt == i);
    }
  }

  Tile::set_max_tile_chunk_size(constants::max_tile_chunk_size);
}

TEST_CASE(
    "Filter: Test simple out-of-place pipeline",
    "[filter][simple-out-of-place]") {
  tiledb::sm::Config config;

  // Set up test data
  const uint64_t nelts = 100;
  const uint64_t tile_size = nelts * sizeof(uint64_t);
  const uint64_t cell_size = sizeof(uint64_t);
  const uint32_t dim_num = 0;

  Tile tile;
  tile.init_unfiltered(
      constants::format_version,
      Datatype::UINT64,
      tile_size,
      cell_size,
      dim_num);

  // Set up test data
  for (uint64_t i = 0; i < nelts; i++) {
    CHECK(tile.write(&i, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
  }

  FilterPipeline pipeline;
  ThreadPool tp;
  CHECK(tp.init(4).ok());
  CHECK(pipeline.add_filter(Add1OutOfPlace()).ok());

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

    CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
    CHECK(
        pipeline.run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
            .ok());
    CHECK(tile.filtered_buffer().size() == 0);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK(tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
      CHECK(elt == i);
    }
  }

  SECTION("- Multi-stage") {
    // Add a few more +1 filters and re-run.
    CHECK(pipeline.add_filter(Add1OutOfPlace()).ok());
    CHECK(pipeline.add_filter(Add1OutOfPlace()).ok());
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

    CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
    CHECK(
        pipeline.run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
            .ok());
    CHECK(tile.filtered_buffer().size() == 0);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK(tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
      CHECK(elt == i);
    }
  }
}

TEST_CASE(
    "Filter: Test simple out-of-place pipeline var",
    "[filter][simple-out-of-place][var]") {
  tiledb::sm::Config config;

  const uint64_t nelts = 100;
  const uint64_t tile_size = nelts * sizeof(uint64_t);
  const uint64_t cell_size = sizeof(uint64_t);
  const uint32_t dim_num = 0;

  Tile tile;
  tile.init_unfiltered(
      constants::format_version,
      Datatype::UINT64,
      tile_size,
      cell_size,
      dim_num);

  // Set up test data
  for (uint64_t i = 0; i < nelts; i++) {
    CHECK(tile.write(&i, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
  }

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

  Tile offsets_tile;
  offsets_tile.init_unfiltered(
      constants::format_version,
      Datatype::UINT64,
      offsets_tile_size,
      constants::cell_var_offset_size,
      dim_num);

  // Set up test data
  for (uint64_t i = 0; i < offsets.size(); i++) {
    CHECK(offsets_tile
              .write(
                  &offsets[i],
                  i * constants::cell_var_offset_size,
                  constants::cell_var_offset_size)
              .ok());
  }

  FilterPipeline pipeline;
  ThreadPool tp;
  CHECK(tp.init(4).ok());
  CHECK(pipeline.add_filter(Add1OutOfPlace()).ok());

  SECTION("- Single stage") {
    Tile::set_max_tile_chunk_size(80);
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

    CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
    CHECK(
        pipeline.run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
            .ok());
    CHECK(tile.filtered_buffer().size() == 0);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK(tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
      CHECK(elt == i);
    }
  }

  SECTION("- Multi-stage") {
    // Add a few more +1 filters and re-run.
    Tile::set_max_tile_chunk_size(80);
    CHECK(pipeline.add_filter(Add1OutOfPlace()).ok());
    CHECK(pipeline.add_filter(Add1OutOfPlace()).ok());
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

    CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
    CHECK(
        pipeline.run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
            .ok());
    CHECK(tile.filtered_buffer().size() == 0);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK(tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
      CHECK(elt == i);
    }
  }

  Tile::set_max_tile_chunk_size(constants::max_tile_chunk_size);
}

TEST_CASE(
    "Filter: Test mixed in- and out-of-place pipeline",
    "[filter][in-out-place]") {
  tiledb::sm::Config config;

  // Set up test data
  const uint64_t nelts = 100;
  const uint64_t tile_size = nelts * sizeof(uint64_t);
  const uint64_t cell_size = sizeof(uint64_t);
  const uint32_t dim_num = 0;

  Tile tile;
  tile.init_unfiltered(
      constants::format_version,
      Datatype::UINT64,
      tile_size,
      cell_size,
      dim_num);

  // Set up test data
  for (uint64_t i = 0; i < nelts; i++) {
    CHECK(tile.write(&i, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
  }

  FilterPipeline pipeline;
  ThreadPool tp;
  CHECK(tp.init(4).ok());
  CHECK(pipeline.add_filter(Add1InPlace()).ok());
  CHECK(pipeline.add_filter(Add1OutOfPlace()).ok());
  CHECK(pipeline.add_filter(Add1InPlace()).ok());
  CHECK(pipeline.add_filter(Add1OutOfPlace()).ok());
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

  // Set up test data
  CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
  CHECK(pipeline.run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
            .ok());
  CHECK(tile.filtered_buffer().size() == 0);
  for (uint64_t i = 0; i < nelts; i++) {
    uint64_t elt = 0;
    CHECK(tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
    CHECK(elt == i);
  }
}

TEST_CASE(
    "Filter: Test mixed in- and out-of-place pipeline var",
    "[filter][in-out-place][var]") {
  tiledb::sm::Config config;

  const uint64_t nelts = 100;
  const uint64_t tile_size = nelts * sizeof(uint64_t);
  const uint64_t cell_size = sizeof(uint64_t);
  const uint32_t dim_num = 0;

  Tile tile;
  tile.init_unfiltered(
      constants::format_version,
      Datatype::UINT64,
      tile_size,
      cell_size,
      dim_num);

  // Set up test data
  for (uint64_t i = 0; i < nelts; i++) {
    CHECK(tile.write(&i, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
  }

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

  Tile offsets_tile;
  offsets_tile.init_unfiltered(
      constants::format_version,
      Datatype::UINT64,
      offsets_tile_size,
      constants::cell_var_offset_size,
      dim_num);

  // Set up test data
  for (uint64_t i = 0; i < offsets.size(); i++) {
    CHECK(offsets_tile
              .write(
                  &offsets[i],
                  i * constants::cell_var_offset_size,
                  constants::cell_var_offset_size)
              .ok());
  }

  FilterPipeline pipeline;
  ThreadPool tp;
  CHECK(tp.init(4).ok());
  Tile::set_max_tile_chunk_size(80);
  CHECK(pipeline.add_filter(Add1InPlace()).ok());
  CHECK(pipeline.add_filter(Add1OutOfPlace()).ok());
  CHECK(pipeline.add_filter(Add1InPlace()).ok());
  CHECK(pipeline.add_filter(Add1OutOfPlace()).ok());
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

  CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
  CHECK(pipeline.run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
            .ok());
  CHECK(tile.filtered_buffer().size() == 0);
  for (uint64_t i = 0; i < nelts; i++) {
    uint64_t elt = 0;
    CHECK(tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
    CHECK(elt == i);
  }

  Tile::set_max_tile_chunk_size(constants::max_tile_chunk_size);
}

TEST_CASE("Filter: Test compression", "[filter][compression]") {
  tiledb::sm::Config config;

  const uint64_t nelts = 100;
  const uint64_t tile_size = nelts * sizeof(uint64_t);
  const uint64_t cell_size = sizeof(uint64_t);
  const uint32_t dim_num = 0;

  Tile tile;
  tile.init_unfiltered(
      constants::format_version,
      Datatype::UINT64,
      tile_size,
      cell_size,
      dim_num);

  // Set up test data
  for (uint64_t i = 0; i < nelts; i++) {
    CHECK(tile.write(&i, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
  }

  // Set up dummy array schema (needed by compressor filter for cell size, etc).
  uint32_t dim_dom[] = {1, 10};
  tiledb::sm::Dimension dim{"", Datatype::INT32};
  dim.set_domain(dim_dom);
  tiledb::sm::Domain domain;
  domain.add_dimension(&dim);
  tiledb::sm::ArraySchema schema;
  tiledb::sm::Attribute attr("attr", Datatype::UINT64);
  schema.add_attribute(tdb::make_shared<tiledb::sm::Attribute>(HERE(), &attr));
  schema.set_domain(&domain);
  schema.init();

  FilterPipeline pipeline;
  ThreadPool tp;
  CHECK(tp.init(4).ok());

  SECTION("- Simple") {
    CHECK(pipeline.add_filter(Add1InPlace()).ok());
    CHECK(pipeline.add_filter(Add1OutOfPlace()).ok());
    CHECK(pipeline.add_filter(CompressionFilter(tiledb::sm::Compressor::LZ4, 5))
              .ok());

    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());
    // Check compression worked
    CHECK(tile.size() == 0);
    CHECK(tile.filtered_buffer().size() < nelts * sizeof(uint64_t));

    CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
    CHECK(
        pipeline.run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
            .ok());
    CHECK(tile.filtered_buffer().size() == 0);

    // Check all elements original values.
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK(tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
      CHECK(elt == i);
    }
  }

  SECTION("- With checksum stage") {
    CHECK(pipeline.add_filter(PseudoChecksumFilter()).ok());
    CHECK(pipeline.add_filter(CompressionFilter(tiledb::sm::Compressor::LZ4, 5))
              .ok());

    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());
    // Check compression worked
    CHECK(tile.size() == 0);
    CHECK(tile.filtered_buffer().size() < nelts * sizeof(uint64_t));

    CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
    CHECK(
        pipeline.run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
            .ok());
    CHECK(tile.filtered_buffer().size() == 0);

    // Check all elements original values.
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK(tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
      CHECK(elt == i);
    }
  }

  SECTION("- With multiple stages") {
    CHECK(pipeline.add_filter(Add1InPlace()).ok());
    CHECK(pipeline.add_filter(PseudoChecksumFilter()).ok());
    CHECK(pipeline.add_filter(Add1OutOfPlace()).ok());
    CHECK(pipeline.add_filter(CompressionFilter(tiledb::sm::Compressor::LZ4, 5))
              .ok());

    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());
    // Check compression worked
    CHECK(tile.size() == 0);
    CHECK(tile.filtered_buffer().size() < nelts * sizeof(uint64_t));

    CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
    CHECK(
        pipeline.run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
            .ok());
    CHECK(tile.filtered_buffer().size() == 0);

    // Check all elements original values.
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK(tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
      CHECK(elt == i);
    }
  }
}

TEST_CASE("Filter: Test compression var", "[filter][compression][var]") {
  tiledb::sm::Config config;

  const uint64_t nelts = 100;
  const uint64_t tile_size = nelts * sizeof(uint64_t);
  const uint64_t cell_size = sizeof(uint64_t);
  const uint32_t dim_num = 0;

  Tile tile;
  tile.init_unfiltered(
      constants::format_version,
      Datatype::UINT64,
      tile_size,
      cell_size,
      dim_num);

  // Set up test data
  for (uint64_t i = 0; i < nelts; i++) {
    CHECK(tile.write(&i, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
  }

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

  Tile offsets_tile;
  offsets_tile.init_unfiltered(
      constants::format_version,
      Datatype::UINT64,
      offsets_tile_size,
      constants::cell_var_offset_size,
      dim_num);

  // Set up test data
  for (uint64_t i = 0; i < offsets.size(); i++) {
    CHECK(offsets_tile
              .write(
                  &offsets[i],
                  i * constants::cell_var_offset_size,
                  constants::cell_var_offset_size)
              .ok());
  }

  // Set up dummy array schema (needed by compressor filter for cell size, etc).
  uint32_t dim_dom[] = {1, 10};
  tiledb::sm::Dimension dim{"", Datatype::INT32};
  dim.set_domain(dim_dom);
  tiledb::sm::Domain domain;
  domain.add_dimension(&dim);
  tiledb::sm::ArraySchema schema;
  tiledb::sm::Attribute attr("attr", Datatype::UINT64);
  schema.add_attribute(tdb::make_shared<tiledb::sm::Attribute>(HERE(), &attr));
  schema.set_domain(&domain);
  schema.init();

  FilterPipeline pipeline;
  ThreadPool tp;
  CHECK(tp.init(4).ok());

  SECTION("- Simple") {
    Tile::set_max_tile_chunk_size(80);
    CHECK(pipeline.add_filter(Add1InPlace()).ok());
    CHECK(pipeline.add_filter(Add1OutOfPlace()).ok());
    CHECK(pipeline.add_filter(CompressionFilter(tiledb::sm::Compressor::LZ4, 5))
              .ok());

    CHECK(pipeline.run_forward(&test::g_helper_stats, &tile, &offsets_tile, &tp)
              .ok());
    // Check number of chunks
    CHECK(tile.size() == 0);
    CHECK(
        tile.filtered_buffer().value_at_as<uint64_t>(0) ==
        9);  // Number of chunks

    CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
    CHECK(
        pipeline.run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
            .ok());
    CHECK(tile.filtered_buffer().size() == 0);

    // Check all elements original values.
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK(tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
      CHECK(elt == i);
    }
  }

  SECTION("- With checksum stage") {
    Tile::set_max_tile_chunk_size(80);
    CHECK(pipeline.add_filter(PseudoChecksumFilter()).ok());
    CHECK(pipeline.add_filter(CompressionFilter(tiledb::sm::Compressor::LZ4, 5))
              .ok());

    CHECK(pipeline.run_forward(&test::g_helper_stats, &tile, &offsets_tile, &tp)
              .ok());
    // Check number of chunks
    CHECK(tile.size() == 0);
    CHECK(
        tile.filtered_buffer().value_at_as<uint64_t>(0) ==
        9);  // Number of chunks

    CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
    CHECK(
        pipeline.run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
            .ok());
    CHECK(tile.filtered_buffer().size() == 0);

    // Check all elements original values.
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK(tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
      CHECK(elt == i);
    }
  }

  SECTION("- With multiple stages") {
    Tile::set_max_tile_chunk_size(80);
    CHECK(pipeline.add_filter(Add1InPlace()).ok());
    CHECK(pipeline.add_filter(PseudoChecksumFilter()).ok());
    CHECK(pipeline.add_filter(Add1OutOfPlace()).ok());
    CHECK(pipeline.add_filter(CompressionFilter(tiledb::sm::Compressor::LZ4, 5))
              .ok());

    CHECK(pipeline.run_forward(&test::g_helper_stats, &tile, &offsets_tile, &tp)
              .ok());
    // Check number of chunks
    CHECK(tile.size() == 0);
    CHECK(
        tile.filtered_buffer().value_at_as<uint64_t>(0) ==
        9);  // Number of chunks

    CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
    CHECK(
        pipeline.run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
            .ok());
    CHECK(tile.filtered_buffer().size() == 0);

    // Check all elements original values.
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK(tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
      CHECK(elt == i);
    }
  }

  Tile::set_max_tile_chunk_size(constants::max_tile_chunk_size);
}

TEST_CASE("Filter: Test pseudo-checksum", "[filter][pseudo-checksum]") {
  tiledb::sm::Config config;

  // Set up test data
  const uint64_t nelts = 100;
  const uint64_t expected_checksum = 4950;
  const uint64_t tile_size = nelts * sizeof(uint64_t);
  const uint64_t cell_size = sizeof(uint64_t);
  const uint32_t dim_num = 0;

  Tile tile;
  tile.init_unfiltered(
      constants::format_version,
      Datatype::UINT64,
      tile_size,
      cell_size,
      dim_num);

  // Set up test data
  for (uint64_t i = 0; i < nelts; i++) {
    CHECK(tile.write(&i, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
  }

  FilterPipeline pipeline;
  ThreadPool tp;
  CHECK(tp.init(4).ok());
  CHECK(pipeline.add_filter(PseudoChecksumFilter()).ok());

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

    CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
    CHECK(
        pipeline.run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
            .ok());
    CHECK(tile.filtered_buffer().size() == 0);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK(tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
      CHECK(elt == i);
    }
  }

  SECTION("- Multi-stage") {
    CHECK(pipeline.add_filter(Add1OutOfPlace()).ok());
    CHECK(pipeline.add_filter(Add1InPlace()).ok());
    CHECK(pipeline.add_filter(PseudoChecksumFilter()).ok());
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

    CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
    CHECK(
        pipeline.run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
            .ok());
    CHECK(tile.filtered_buffer().size() == 0);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK(tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
      CHECK(elt == i);
    }
  }
}

TEST_CASE(
    "Filter: Test pseudo-checksum var", "[filter][pseudo-checksum][var]") {
  tiledb::sm::Config config;

  const uint64_t nelts = 100;
  const uint64_t tile_size = nelts * sizeof(uint64_t);
  const uint64_t cell_size = sizeof(uint64_t);
  const uint32_t dim_num = 0;

  Tile tile;
  tile.init_unfiltered(
      constants::format_version,
      Datatype::UINT64,
      tile_size,
      cell_size,
      dim_num);

  // Set up test data
  for (uint64_t i = 0; i < nelts; i++) {
    CHECK(tile.write(&i, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
  }

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

  Tile offsets_tile;
  offsets_tile.init_unfiltered(
      constants::format_version,
      Datatype::UINT64,
      offsets_tile_size,
      constants::cell_var_offset_size,
      dim_num);

  // Set up test data
  for (uint64_t i = 0; i < offsets.size(); i++) {
    CHECK(offsets_tile
              .write(
                  &offsets[i],
                  i * constants::cell_var_offset_size,
                  constants::cell_var_offset_size)
              .ok());
  }

  std::vector<uint64_t> expected_checksums{
      91, 99, 275, 238, 425, 525, 1350, 825, 1122};

  FilterPipeline pipeline;
  ThreadPool tp;
  CHECK(tp.init(4).ok());
  CHECK(pipeline.add_filter(PseudoChecksumFilter()).ok());

  SECTION("- Single stage") {
    Tile::set_max_tile_chunk_size(80);
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

    CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
    CHECK(
        pipeline.run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
            .ok());
    CHECK(tile.filtered_buffer().size() == 0);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK(tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
      CHECK(elt == i);
    }
  }

  SECTION("- Multi-stage") {
    Tile::set_max_tile_chunk_size(80);
    CHECK(pipeline.add_filter(Add1OutOfPlace()).ok());
    CHECK(pipeline.add_filter(Add1InPlace()).ok());
    CHECK(pipeline.add_filter(PseudoChecksumFilter()).ok());
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

    CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
    CHECK(
        pipeline.run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
            .ok());
    CHECK(tile.filtered_buffer().size() == 0);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK(tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
      CHECK(elt == i);
    }
  }

  Tile::set_max_tile_chunk_size(constants::max_tile_chunk_size);
}

TEST_CASE("Filter: Test pipeline modify filter", "[filter][modify]") {
  tiledb::sm::Config config;

  // Set up test data
  const uint64_t nelts = 100;
  const uint64_t tile_size = nelts * sizeof(uint64_t);
  const uint64_t cell_size = sizeof(uint64_t);
  const uint32_t dim_num = 0;

  Tile tile;
  tile.init_unfiltered(
      constants::format_version,
      Datatype::UINT64,
      tile_size,
      cell_size,
      dim_num);

  // Set up test data
  for (uint64_t i = 0; i < nelts; i++) {
    CHECK(tile.write(&i, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
  }

  FilterPipeline pipeline;
  ThreadPool tp;
  CHECK(tp.init(4).ok());
  CHECK(pipeline.add_filter(Add1InPlace()).ok());
  CHECK(pipeline.add_filter(AddNInPlace()).ok());
  CHECK(pipeline.add_filter(Add1InPlace()).ok());

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

  CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
  CHECK(pipeline.run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
            .ok());
  CHECK(tile.filtered_buffer().size() == 0);
  for (uint64_t i = 0; i < nelts; i++) {
    uint64_t elt = 0;
    CHECK(tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
    CHECK(elt == i);
  }
}

TEST_CASE("Filter: Test pipeline modify filter var", "[filter][modify][var]") {
  tiledb::sm::Config config;

  const uint64_t nelts = 100;
  const uint64_t tile_size = nelts * sizeof(uint64_t);
  const uint64_t cell_size = sizeof(uint64_t);
  const uint32_t dim_num = 0;

  Tile tile;
  tile.init_unfiltered(
      constants::format_version,
      Datatype::UINT64,
      tile_size,
      cell_size,
      dim_num);

  // Set up test data
  for (uint64_t i = 0; i < nelts; i++) {
    CHECK(tile.write(&i, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
  }

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

  Tile offsets_tile;
  offsets_tile.init_unfiltered(
      constants::format_version,
      Datatype::UINT64,
      offsets_tile_size,
      constants::cell_var_offset_size,
      dim_num);

  // Set up test data
  for (uint64_t i = 0; i < offsets.size(); i++) {
    CHECK(offsets_tile
              .write(
                  &offsets[i],
                  i * constants::cell_var_offset_size,
                  constants::cell_var_offset_size)
              .ok());
  }

  FilterPipeline pipeline;
  ThreadPool tp;
  CHECK(tp.init(4).ok());
  CHECK(pipeline.add_filter(Add1InPlace()).ok());
  CHECK(pipeline.add_filter(AddNInPlace()).ok());
  CHECK(pipeline.add_filter(Add1InPlace()).ok());

  // Get non-existent filter instance
  auto* cksum = pipeline.get_filter<PseudoChecksumFilter>();
  CHECK(cksum == nullptr);

  // Modify +N filter
  auto* add_n = pipeline.get_filter<AddNInPlace>();
  CHECK(add_n != nullptr);
  add_n->set_increment(2);

  Tile::set_max_tile_chunk_size(80);
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

  CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
  CHECK(pipeline.run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
            .ok());
  CHECK(tile.filtered_buffer().size() == 0);
  for (uint64_t i = 0; i < nelts; i++) {
    uint64_t elt = 0;
    CHECK(tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
    CHECK(elt == i);
  }

  Tile::set_max_tile_chunk_size(constants::max_tile_chunk_size);
}

TEST_CASE("Filter: Test pipeline copy", "[filter][copy]") {
  tiledb::sm::Config config;

  const uint64_t expected_checksum = 5350;

  const uint64_t nelts = 100;
  const uint64_t tile_size = nelts * sizeof(uint64_t);
  const uint64_t cell_size = sizeof(uint64_t);
  const uint32_t dim_num = 0;

  Tile tile;
  tile.init_unfiltered(
      constants::format_version,
      Datatype::UINT64,
      tile_size,
      cell_size,
      dim_num);

  // Set up test data
  for (uint64_t i = 0; i < nelts; i++) {
    CHECK(tile.write(&i, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
  }

  FilterPipeline pipeline;
  ThreadPool tp;
  CHECK(tp.init(4).ok());
  CHECK(pipeline.add_filter(Add1InPlace()).ok());
  CHECK(pipeline.add_filter(AddNInPlace()).ok());
  CHECK(pipeline.add_filter(Add1InPlace()).ok());
  CHECK(pipeline.add_filter(PseudoChecksumFilter()).ok());

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

  CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
  CHECK(pipeline.run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
            .ok());
  CHECK(tile.filtered_buffer().size() == 0);
  for (uint64_t i = 0; i < nelts; i++) {
    uint64_t elt = 0;
    CHECK(tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
    CHECK(elt == i);
  }
}

TEST_CASE("Filter: Test random pipeline", "[filter][random]") {
  tiledb::sm::Config config;

  const uint64_t nelts = 100;
  const uint64_t tile_size = nelts * sizeof(uint64_t);
  const uint64_t cell_size = sizeof(uint64_t);
  const uint32_t dim_num = 0;

  Tile tile;
  tile.init_unfiltered(
      constants::format_version,
      Datatype::UINT64,
      tile_size,
      cell_size,
      dim_num);

  // Set up test data
  for (uint64_t i = 0; i < nelts; i++) {
    CHECK(tile.write(&i, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
  }

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

  ThreadPool tp;
  CHECK(tp.init(4).ok());
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
        CHECK(pipeline.add_filter(*filter).ok());
        delete filter;
      } else {
        auto idx = (unsigned)rng_constructors(gen);
        tiledb::sm::Filter* filter = constructors[idx]();
        CHECK(pipeline.add_filter(*filter).ok());
        delete filter;
      }
    }

    // End result should always be the same as the input.
    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());
    CHECK(tile.size() == 0);
    CHECK(tile.filtered_buffer().size() != 0);
    CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
    CHECK(
        pipeline.run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
            .ok());
    CHECK(tile.filtered_buffer().size() == 0);
    for (uint64_t n = 0; n < nelts; n++) {
      uint64_t elt = 0;
      CHECK(tile.read(&elt, n * sizeof(uint64_t), sizeof(uint64_t)).ok());
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
  const uint64_t tile_size = nelts * sizeof(uint64_t);
  const uint64_t cell_size = sizeof(uint64_t);
  const uint32_t dim_num = 0;

  Tile tile;
  tile.init_unfiltered(
      constants::format_version,
      Datatype::UINT64,
      tile_size,
      cell_size,
      dim_num);

  // Set up test data
  for (uint64_t i = 0; i < nelts; i++) {
    CHECK(tile.write(&i, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
  }

  // MD5
  FilterPipeline md5_pipeline;
  ThreadPool tp;
  CHECK(tp.init(4).ok());
  ChecksumMD5Filter md5_filter;
  CHECK(md5_pipeline.add_filter(md5_filter).ok());
  CHECK(md5_pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp)
            .ok());
  CHECK(tile.size() == 0);
  CHECK(tile.filtered_buffer().size() != 0);
  CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
  CHECK(md5_pipeline
            .run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
            .ok());
  CHECK(tile.filtered_buffer().size() == 0);
  for (uint64_t n = 0; n < nelts; n++) {
    uint64_t elt = 0;
    CHECK(tile.read(&elt, n * sizeof(uint64_t), sizeof(uint64_t)).ok());
    CHECK(elt == n);
  }

  // SHA256
  FilterPipeline sha_256_pipeline;
  ChecksumMD5Filter sha_256_filter;
  CHECK(sha_256_pipeline.add_filter(sha_256_filter).ok());
  CHECK(sha_256_pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp)
            .ok());
  CHECK(tile.size() == 0);
  CHECK(tile.filtered_buffer().size() != 0);
  CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
  CHECK(sha_256_pipeline
            .run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
            .ok());
  CHECK(tile.filtered_buffer().size() == 0);
  for (uint64_t n = 0; n < nelts; n++) {
    uint64_t elt = 0;
    CHECK(tile.read(&elt, n * sizeof(uint64_t), sizeof(uint64_t)).ok());
    CHECK(elt == n);
  }
}

TEST_CASE("Filter: Test bit width reduction", "[filter][bit-width-reduction]") {
  tiledb::sm::Config config;

  // Set up test data
  const uint64_t nelts = 1000;
  const uint64_t tile_size = nelts * sizeof(uint64_t);
  const uint64_t cell_size = sizeof(uint64_t);
  const uint32_t dim_num = 0;

  Tile tile;
  tile.init_unfiltered(
      constants::format_version,
      Datatype::UINT64,
      tile_size,
      cell_size,
      dim_num);

  // Set up test data
  for (uint64_t i = 0; i < nelts; i++) {
    CHECK(tile.write(&i, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
  }

  FilterPipeline pipeline;
  ThreadPool tp;
  CHECK(tp.init(4).ok());
  CHECK(pipeline.add_filter(BitWidthReductionFilter()).ok());

  SECTION("- Single stage") {
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

    CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
    CHECK(
        pipeline.run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
            .ok());
    CHECK(tile.filtered_buffer().size() == 0);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK(tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
      CHECK(elt == i);
    }
  }

  SECTION("- Window sizes") {
    std::vector<uint32_t> window_sizes = {
        32, 64, 128, 256, 437, 512, 1024, 2000};
    for (auto window_size : window_sizes) {
      pipeline.get_filter<BitWidthReductionFilter>()->set_max_window_size(
          window_size);

      CHECK(pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp)
                .ok());
      CHECK(tile.size() == 0);
      CHECK(tile.filtered_buffer().size() != 0);
      CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
      CHECK(pipeline
                .run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
                .ok());
      CHECK(tile.filtered_buffer().size() == 0);
      for (uint64_t i = 0; i < nelts; i++) {
        uint64_t elt = 0;
        CHECK(tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
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

    Tile tile;
    tile.init_unfiltered(
        constants::format_version,
        Datatype::UINT64,
        tile_size,
        cell_size,
        dim_num);

    // Set up test data
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t val = (uint64_t)rng(gen);
      CHECK(tile.write(&val, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
    }

    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());
    CHECK(tile.size() == 0);
    CHECK(tile.filtered_buffer().size() != 0);
    CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
    CHECK(
        pipeline.run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
            .ok());
    CHECK(tile.filtered_buffer().size() == 0);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK(tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
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

    const uint64_t tile_size2 = nelts * sizeof(uint32_t);

    Tile tile;
    tile.init_unfiltered(
        constants::format_version,
        Datatype::UINT64,
        tile_size2,
        cell_size,
        dim_num);

    // Set up test data
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t val = (uint32_t)rng(gen);
      CHECK(tile.write(&val, i * sizeof(uint32_t), sizeof(uint32_t)).ok());
    }

    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());
    CHECK(tile.size() == 0);
    CHECK(tile.filtered_buffer().size() != 0);
    CHECK(tile.alloc_data(nelts * sizeof(uint32_t)).ok());
    CHECK(
        pipeline.run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
            .ok());
    CHECK(tile.filtered_buffer().size() == 0);
    for (uint64_t i = 0; i < nelts; i++) {
      int32_t elt = 0;
      CHECK(tile.read(&elt, i * sizeof(int32_t), sizeof(int32_t)).ok());
      CHECK(elt == rng(gen_copy));
    }
  }

  SECTION("- Byte overflow") {
    Tile tile;
    tile.init_unfiltered(
        constants::format_version,
        Datatype::UINT64,
        tile_size,
        cell_size,
        dim_num);

    // Set up test data
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t val = i % 257;
      CHECK(tile.write(&val, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
    }

    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());
    CHECK(tile.size() == 0);
    CHECK(tile.filtered_buffer().size() != 0);
    CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
    CHECK(
        pipeline.run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
            .ok());
    CHECK(tile.filtered_buffer().size() == 0);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK(tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
      CHECK(elt == i % 257);
    }
  }
}

TEST_CASE(
    "Filter: Test bit width reduction var",
    "[filter][bit-width-reduction][var]") {
  tiledb::sm::Config config;

  const uint64_t nelts = 100;
  const uint64_t tile_size = nelts * sizeof(uint64_t);
  const uint64_t cell_size = sizeof(uint64_t);
  const uint32_t dim_num = 0;

  Tile tile;
  tile.init_unfiltered(
      constants::format_version,
      Datatype::UINT64,
      tile_size,
      cell_size,
      dim_num);

  // Set up test data
  for (uint64_t i = 0; i < nelts; i++) {
    CHECK(tile.write(&i, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
  }

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

  Tile offsets_tile;
  offsets_tile.init_unfiltered(
      constants::format_version,
      Datatype::UINT64,
      offsets_tile_size,
      constants::cell_var_offset_size,
      dim_num);

  // Set up test data
  for (uint64_t i = 0; i < offsets.size(); i++) {
    CHECK(offsets_tile
              .write(
                  &offsets[i],
                  i * constants::cell_var_offset_size,
                  constants::cell_var_offset_size)
              .ok());
  }

  FilterPipeline pipeline;
  ThreadPool tp;
  CHECK(tp.init(4).ok());
  CHECK(pipeline.add_filter(BitWidthReductionFilter()).ok());

  SECTION("- Single stage") {
    Tile::set_max_tile_chunk_size(80);
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

    CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
    CHECK(
        pipeline.run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
            .ok());
    CHECK(tile.filtered_buffer().size() == 0);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK(tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
      CHECK(elt == i);
    }
  }

  SECTION("- Window sizes") {
    Tile::set_max_tile_chunk_size(80);
    std::vector<uint32_t> window_sizes = {
        32, 64, 128, 256, 437, 512, 1024, 2000};
    for (auto window_size : window_sizes) {
      pipeline.get_filter<BitWidthReductionFilter>()->set_max_window_size(
          window_size);

      CHECK(
          pipeline.run_forward(&test::g_helper_stats, &tile, &offsets_tile, &tp)
              .ok());
      CHECK(tile.size() == 0);
      CHECK(tile.filtered_buffer().size() != 0);
      CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
      CHECK(pipeline
                .run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
                .ok());
      CHECK(tile.filtered_buffer().size() == 0);
      for (uint64_t i = 0; i < nelts; i++) {
        uint64_t elt = 0;
        CHECK(tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
        CHECK(elt == i);
      }
    }
  }

  SECTION("- Random values") {
    Tile::set_max_tile_chunk_size(80);
    std::random_device rd;
    auto seed = rd();
    std::mt19937 gen(seed), gen_copy(seed);
    std::uniform_int_distribution<> rng(0, std::numeric_limits<int32_t>::max());
    INFO("Random element seed: " << seed);

    Tile tile;
    tile.init_unfiltered(
        constants::format_version,
        Datatype::UINT64,
        tile_size,
        cell_size,
        dim_num);

    // Set up test data
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t val = (uint64_t)rng(gen);
      CHECK(tile.write(&val, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
    }

    CHECK(pipeline.run_forward(&test::g_helper_stats, &tile, &offsets_tile, &tp)
              .ok());
    CHECK(tile.size() == 0);
    CHECK(tile.filtered_buffer().size() != 0);
    CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
    CHECK(
        pipeline.run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
            .ok());
    CHECK(tile.filtered_buffer().size() == 0);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK(tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
      CHECK((int64_t)elt == rng(gen_copy));
    }
  }

  SECTION(" - Random signed values") {
    Tile::set_max_tile_chunk_size(80);
    std::random_device rd;
    auto seed = rd();
    std::mt19937 gen(seed), gen_copy(seed);
    std::uniform_int_distribution<> rng(
        std::numeric_limits<int32_t>::lowest(),
        std::numeric_limits<int32_t>::max());
    INFO("Random element seed: " << seed);

    const uint64_t tile_size2 = nelts * sizeof(uint32_t);

    Tile tile;
    tile.init_unfiltered(
        constants::format_version,
        Datatype::UINT64,
        tile_size2,
        cell_size,
        dim_num);

    // Set up test data
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t val = (uint32_t)rng(gen);
      CHECK(tile.write(&val, i * sizeof(uint32_t), sizeof(uint32_t)).ok());
    }

    std::vector<uint64_t> offsets32(offsets);
    for (uint64_t i = 0; i < offsets32.size(); i++) {
      offsets32[i] /= 2;
    }

    Tile offsets_tile32;
    offsets_tile32.init_unfiltered(
        constants::format_version,
        Datatype::UINT64,
        offsets_tile_size,
        constants::cell_var_offset_size,
        dim_num);

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
    CHECK(tile.alloc_data(nelts * sizeof(uint32_t)).ok());
    CHECK(
        pipeline.run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
            .ok());
    CHECK(tile.filtered_buffer().size() == 0);
    for (uint64_t i = 0; i < nelts; i++) {
      int32_t elt = 0;
      CHECK(tile.read(&elt, i * sizeof(int32_t), sizeof(int32_t)).ok());
      CHECK(elt == rng(gen_copy));
    }
  }

  SECTION("- Byte overflow") {
    Tile::set_max_tile_chunk_size(80);
    Tile tile;
    tile.init_unfiltered(
        constants::format_version,
        Datatype::UINT64,
        tile_size,
        cell_size,
        dim_num);

    // Set up test data
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t val = i % 257;
      CHECK(tile.write(&val, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
    }

    CHECK(pipeline.run_forward(&test::g_helper_stats, &tile, &offsets_tile, &tp)
              .ok());
    CHECK(tile.size() == 0);
    CHECK(tile.filtered_buffer().size() != 0);
    CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
    CHECK(
        pipeline.run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
            .ok());
    CHECK(tile.filtered_buffer().size() == 0);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK(tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
      CHECK(elt == i % 257);
    }
  }

  Tile::set_max_tile_chunk_size(constants::max_tile_chunk_size);
}

TEST_CASE("Filter: Test positive-delta encoding", "[filter][positive-delta]") {
  tiledb::sm::Config config;

  // Set up test data
  const uint64_t nelts = 1000;
  const uint64_t tile_size = nelts * sizeof(uint64_t);
  const uint64_t cell_size = sizeof(uint64_t);
  const uint32_t dim_num = 0;

  Tile tile;
  tile.init_unfiltered(
      constants::format_version,
      Datatype::UINT64,
      tile_size,
      cell_size,
      dim_num);

  // Set up test data
  for (uint64_t i = 0; i < nelts; i++) {
    CHECK(tile.write(&i, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
  }

  FilterPipeline pipeline;
  ThreadPool tp;
  CHECK(tp.init(4).ok());
  CHECK(pipeline.add_filter(PositiveDeltaFilter()).ok());

  SECTION("- Single stage") {
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

    CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
    CHECK(
        pipeline.run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
            .ok());
    CHECK(tile.filtered_buffer().size() == 0);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK(tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
      CHECK(elt == i);
    }
  }

  SECTION("- Window sizes") {
    std::vector<uint32_t> window_sizes = {
        32, 64, 128, 256, 437, 512, 1024, 2000};
    for (auto window_size : window_sizes) {
      pipeline.get_filter<PositiveDeltaFilter>()->set_max_window_size(
          window_size);

      CHECK(pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp)
                .ok());
      CHECK(tile.size() == 0);
      CHECK(tile.filtered_buffer().size() != 0);
      CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
      CHECK(pipeline
                .run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
                .ok());
      CHECK(tile.filtered_buffer().size() == 0);
      for (uint64_t i = 0; i < nelts; i++) {
        uint64_t elt = 0;
        CHECK(tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
        CHECK(elt == i);
      }
    }
  }

  SECTION("- Error on non-positive delta data") {
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
  const uint64_t tile_size = nelts * sizeof(uint64_t);
  const uint64_t cell_size = sizeof(uint64_t);
  const uint32_t dim_num = 0;

  Tile tile;
  tile.init_unfiltered(
      constants::format_version,
      Datatype::UINT64,
      tile_size,
      cell_size,
      dim_num);

  // Set up test data
  for (uint64_t i = 0; i < nelts; i++) {
    CHECK(tile.write(&i, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
  }

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

  Tile offsets_tile;
  offsets_tile.init_unfiltered(
      constants::format_version,
      Datatype::UINT64,
      offsets_tile_size,
      constants::cell_var_offset_size,
      dim_num);

  // Set up test data
  for (uint64_t i = 0; i < offsets.size(); i++) {
    CHECK(offsets_tile
              .write(
                  &offsets[i],
                  i * constants::cell_var_offset_size,
                  constants::cell_var_offset_size)
              .ok());
  }

  FilterPipeline pipeline;
  ThreadPool tp;
  CHECK(tp.init(4).ok());
  CHECK(pipeline.add_filter(PositiveDeltaFilter()).ok());

  SECTION("- Single stage") {
    Tile::set_max_tile_chunk_size(80);
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

    CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
    CHECK(
        pipeline.run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
            .ok());
    CHECK(tile.filtered_buffer().size() == 0);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK(tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
      CHECK(elt == i);
    }
  }

  SECTION("- Window sizes") {
    Tile::set_max_tile_chunk_size(80);
    std::vector<uint32_t> window_sizes = {
        32, 64, 128, 256, 437, 512, 1024, 2000};
    for (auto window_size : window_sizes) {
      pipeline.get_filter<PositiveDeltaFilter>()->set_max_window_size(
          window_size);

      CHECK(
          pipeline.run_forward(&test::g_helper_stats, &tile, &offsets_tile, &tp)
              .ok());
      CHECK(tile.size() == 0);
      CHECK(tile.filtered_buffer().size() != 0);
      CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
      CHECK(pipeline
                .run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
                .ok());
      CHECK(tile.filtered_buffer().size() == 0);
      for (uint64_t i = 0; i < nelts; i++) {
        uint64_t elt = 0;
        CHECK(tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
        CHECK(elt == i);
      }
    }
  }

  SECTION("- Error on non-positive delta data") {
    Tile::set_max_tile_chunk_size(80);
    for (uint64_t i = 0; i < nelts; i++) {
      auto val = nelts - i;
      CHECK(tile.write(&val, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
    }

    CHECK(
        !pipeline.run_forward(&test::g_helper_stats, &tile, &offsets_tile, &tp)
             .ok());
  }

  Tile::set_max_tile_chunk_size(constants::max_tile_chunk_size);
}

TEST_CASE("Filter: Test bitshuffle", "[filter][bitshuffle]") {
  tiledb::sm::Config config;

  // Set up test data
  const uint64_t nelts = 1000;
  const uint64_t tile_size = nelts * sizeof(uint64_t);
  const uint64_t cell_size = sizeof(uint64_t);
  const uint32_t dim_num = 0;

  Tile tile;
  tile.init_unfiltered(
      constants::format_version,
      Datatype::UINT64,
      tile_size,
      cell_size,
      dim_num);

  // Set up test data
  for (uint64_t i = 0; i < nelts; i++) {
    CHECK(tile.write(&i, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
  }

  FilterPipeline pipeline;
  ThreadPool tp;
  CHECK(tp.init(4).ok());
  CHECK(pipeline.add_filter(BitshuffleFilter()).ok());

  SECTION("- Single stage") {
    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());
    CHECK(tile.size() == 0);
    CHECK(tile.filtered_buffer().size() != 0);
    CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
    CHECK(
        pipeline.run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
            .ok());
    CHECK(tile.filtered_buffer().size() == 0);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK(tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
      CHECK(elt == i);
    }
  }

  SECTION("- Indivisible by 8") {
    const uint32_t nelts2 = 1001;
    const uint64_t tile_size2 = nelts2 * sizeof(uint32_t);

    Tile tile2;
    tile2.init_unfiltered(
        constants::format_version,
        Datatype::UINT32,
        tile_size2,
        sizeof(uint32_t),
        dim_num);

    // Set up test data
    for (uint32_t i = 0; i < nelts2; i++) {
      CHECK(tile2.write(&i, i * sizeof(uint32_t), sizeof(uint32_t)).ok());
    }

    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile2, nullptr, &tp).ok());
    CHECK(tile2.size() == 0);
    CHECK(tile2.filtered_buffer().size() != 0);
    CHECK(tile2.alloc_data(nelts2 * sizeof(uint32_t)).ok());
    CHECK(pipeline
              .run_reverse(&test::g_helper_stats, &tile2, nullptr, &tp, config)
              .ok());
    CHECK(tile2.filtered_buffer().size() == 0);
    for (uint64_t i = 0; i < nelts2; i++) {
      uint32_t elt = 0;
      CHECK(tile2.read(&elt, i * sizeof(uint32_t), sizeof(uint32_t)).ok());
      CHECK(elt == i);
    }
  }
}

TEST_CASE("Filter: Test bitshuffle var", "[filter][bitshuffle][var]") {
  tiledb::sm::Config config;

  const uint64_t nelts = 100;
  const uint64_t tile_size = nelts * sizeof(uint64_t);
  const uint64_t cell_size = sizeof(uint64_t);
  const uint32_t dim_num = 0;

  Tile tile;
  tile.init_unfiltered(
      constants::format_version,
      Datatype::UINT64,
      tile_size,
      cell_size,
      dim_num);

  // Set up test data
  for (uint64_t i = 0; i < nelts; i++) {
    CHECK(tile.write(&i, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
  }

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

  Tile offsets_tile;
  offsets_tile.init_unfiltered(
      constants::format_version,
      Datatype::UINT64,
      offsets_tile_size,
      constants::cell_var_offset_size,
      dim_num);

  // Set up test data
  for (uint64_t i = 0; i < offsets.size(); i++) {
    CHECK(offsets_tile
              .write(
                  &offsets[i],
                  i * constants::cell_var_offset_size,
                  constants::cell_var_offset_size)
              .ok());
  }

  FilterPipeline pipeline;
  ThreadPool tp;
  CHECK(tp.init(4).ok());
  CHECK(pipeline.add_filter(BitshuffleFilter()).ok());

  SECTION("- Single stage") {
    Tile::set_max_tile_chunk_size(80);
    CHECK(pipeline.run_forward(&test::g_helper_stats, &tile, &offsets_tile, &tp)
              .ok());
    CHECK(tile.size() == 0);
    CHECK(tile.filtered_buffer().size() != 0);
    CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
    CHECK(
        pipeline.run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
            .ok());
    CHECK(tile.filtered_buffer().size() == 0);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK(tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
      CHECK(elt == i);
    }
  }

  SECTION("- Indivisible by 8") {
    Tile::set_max_tile_chunk_size(80);
    const uint32_t nelts2 = 1001;
    const uint64_t tile_size2 = nelts2 * sizeof(uint32_t);

    Tile tile2;
    tile2.init_unfiltered(
        constants::format_version,
        Datatype::UINT32,
        tile_size2,
        sizeof(uint32_t),
        dim_num);

    // Set up test data
    for (uint32_t i = 0; i < nelts2; i++) {
      CHECK(tile2.write(&i, i * sizeof(uint32_t), sizeof(uint32_t)).ok());
    }

    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile2, &offsets_tile, &tp)
            .ok());
    CHECK(tile2.size() == 0);
    CHECK(tile2.filtered_buffer().size() != 0);
    CHECK(tile2.alloc_data(nelts2 * sizeof(uint32_t)).ok());
    CHECK(pipeline
              .run_reverse(&test::g_helper_stats, &tile2, nullptr, &tp, config)
              .ok());
    CHECK(tile2.filtered_buffer().size() == 0);
    for (uint64_t i = 0; i < nelts2; i++) {
      uint32_t elt = 0;
      CHECK(tile2.read(&elt, i * sizeof(uint32_t), sizeof(uint32_t)).ok());
      CHECK(elt == i);
    }
  }

  Tile::set_max_tile_chunk_size(constants::max_tile_chunk_size);
}

TEST_CASE("Filter: Test byteshuffle", "[filter][byteshuffle]") {
  tiledb::sm::Config config;

  // Set up test data
  const uint64_t nelts = 1000;
  const uint64_t tile_size = nelts * sizeof(uint64_t);
  const uint64_t cell_size = sizeof(uint64_t);
  const uint32_t dim_num = 0;

  Tile tile;
  tile.init_unfiltered(
      constants::format_version,
      Datatype::UINT64,
      tile_size,
      cell_size,
      dim_num);

  // Set up test data
  for (uint64_t i = 0; i < nelts; i++) {
    CHECK(tile.write(&i, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
  }

  FilterPipeline pipeline;
  ThreadPool tp;
  CHECK(tp.init(4).ok());
  CHECK(pipeline.add_filter(ByteshuffleFilter()).ok());

  SECTION("- Single stage") {
    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());
    CHECK(tile.size() == 0);
    CHECK(tile.filtered_buffer().size() != 0);
    CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
    CHECK(
        pipeline.run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
            .ok());
    CHECK(tile.filtered_buffer().size() == 0);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK(tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
      CHECK(elt == i);
    }
  }

  SECTION("- Uneven number of elements") {
    const uint32_t nelts2 = 1001;
    const uint64_t tile_size2 = nelts2 * sizeof(uint32_t);

    Tile tile2;
    tile2.init_unfiltered(
        constants::format_version,
        Datatype::UINT32,
        tile_size2,
        sizeof(uint32_t),
        dim_num);

    // Set up test data
    for (uint32_t i = 0; i < nelts2; i++) {
      CHECK(tile2.write(&i, i * sizeof(uint32_t), sizeof(uint32_t)).ok());
    }

    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile2, nullptr, &tp).ok());
    CHECK(tile2.size() == 0);
    CHECK(tile2.filtered_buffer().size() != 0);
    CHECK(tile2.alloc_data(nelts2 * sizeof(uint32_t)).ok());
    CHECK(pipeline
              .run_reverse(&test::g_helper_stats, &tile2, nullptr, &tp, config)
              .ok());
    CHECK(tile2.filtered_buffer().size() == 0);
    for (uint64_t i = 0; i < nelts2; i++) {
      uint32_t elt = 0;
      CHECK(tile2.read(&elt, i * sizeof(uint32_t), sizeof(uint32_t)).ok());
      CHECK(elt == i);
    }
  }
}

TEST_CASE("Filter: Test byteshuffle var", "[filter][byteshuffle][var]") {
  tiledb::sm::Config config;

  const uint64_t nelts = 100;
  const uint64_t tile_size = nelts * sizeof(uint64_t);
  const uint64_t cell_size = sizeof(uint64_t);
  const uint32_t dim_num = 0;

  Tile tile;
  tile.init_unfiltered(
      constants::format_version,
      Datatype::UINT64,
      tile_size,
      cell_size,
      dim_num);

  // Set up test data
  for (uint64_t i = 0; i < nelts; i++) {
    CHECK(tile.write(&i, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
  }

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

  Tile offsets_tile;
  offsets_tile.init_unfiltered(
      constants::format_version,
      Datatype::UINT64,
      offsets_tile_size,
      constants::cell_var_offset_size,
      dim_num);

  // Set up test data
  for (uint64_t i = 0; i < offsets.size(); i++) {
    CHECK(offsets_tile
              .write(
                  &offsets[i],
                  i * constants::cell_var_offset_size,
                  constants::cell_var_offset_size)
              .ok());
  }

  FilterPipeline pipeline;
  ThreadPool tp;
  CHECK(tp.init(4).ok());
  CHECK(pipeline.add_filter(ByteshuffleFilter()).ok());

  SECTION("- Single stage") {
    Tile::set_max_tile_chunk_size(80);
    CHECK(pipeline.run_forward(&test::g_helper_stats, &tile, &offsets_tile, &tp)
              .ok());
    CHECK(tile.size() == 0);
    CHECK(tile.filtered_buffer().size() != 0);
    CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
    CHECK(
        pipeline.run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
            .ok());
    CHECK(tile.filtered_buffer().size() == 0);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK(tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
      CHECK(elt == i);
    }
  }

  SECTION("- Uneven number of elements") {
    Tile::set_max_tile_chunk_size(80);
    const uint32_t nelts2 = 1001;
    const uint64_t tile_size2 = nelts2 * sizeof(uint32_t);

    Tile tile2;
    tile2.init_unfiltered(
        constants::format_version,
        Datatype::UINT32,
        tile_size2,
        sizeof(uint32_t),
        dim_num);

    // Set up test data
    for (uint32_t i = 0; i < nelts2; i++) {
      CHECK(tile2.write(&i, i * sizeof(uint32_t), sizeof(uint32_t)).ok());
    }

    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile2, &offsets_tile, &tp)
            .ok());
    CHECK(tile2.size() == 0);
    CHECK(tile2.filtered_buffer().size() != 0);
    CHECK(tile2.alloc_data(nelts2 * sizeof(uint32_t)).ok());
    CHECK(pipeline
              .run_reverse(&test::g_helper_stats, &tile2, nullptr, &tp, config)
              .ok());
    CHECK(tile2.filtered_buffer().size() == 0);
    for (uint64_t i = 0; i < nelts2; i++) {
      uint32_t elt = 0;
      CHECK(tile2.read(&elt, i * sizeof(uint32_t), sizeof(uint32_t)).ok());
      CHECK(elt == i);
    }
  }

  Tile::set_max_tile_chunk_size(constants::max_tile_chunk_size);
}

TEST_CASE("Filter: Test encryption", "[filter][encryption]") {
  tiledb::sm::Config config;

  // Set up test data
  const uint64_t nelts = 1000;
  const uint64_t tile_size = nelts * sizeof(uint64_t);
  const uint64_t cell_size = sizeof(uint64_t);
  const uint32_t dim_num = 0;

  Tile tile;
  tile.init_unfiltered(
      constants::format_version,
      Datatype::UINT64,
      tile_size,
      cell_size,
      dim_num);

  // Set up test data
  for (uint64_t i = 0; i < nelts; i++) {
    CHECK(tile.write(&i, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
  }

  SECTION("- AES-256-GCM") {
    FilterPipeline pipeline;
    ThreadPool tp;
    CHECK(tp.init(4).ok());
    CHECK(pipeline.add_filter(EncryptionAES256GCMFilter()).ok());

    // No key set
    CHECK(
        !pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());

    // Create and set a key
    char key[32];
    for (unsigned i = 0; i < 32; i++)
      key[i] = (char)i;
    auto filter = pipeline.get_filter<EncryptionAES256GCMFilter>();
    CHECK(filter->set_key(key).ok());

    // Check success
    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());
    CHECK(tile.size() == 0);
    CHECK(tile.filtered_buffer().size() != 0);
    CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
    CHECK(
        pipeline.run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
            .ok());
    CHECK(tile.filtered_buffer().size() == 0);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK(tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
      CHECK(elt == i);
    }

    // Check error decrypting with wrong key.
    CHECK(
        pipeline.run_forward(&test::g_helper_stats, &tile, nullptr, &tp).ok());
    key[0]++;
    CHECK(filter->set_key(key).ok());
    CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
    CHECK(!pipeline
               .run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
               .ok());

    // Fix key and check success. Note: this test depends on the implementation
    // leaving the tile data unmodified when the decryption fails, which is not
    // true in general use of the filter pipeline.
    key[0]--;
    CHECK(filter->set_key(key).ok());
    CHECK(tile.alloc_data(nelts * sizeof(uint64_t)).ok());
    CHECK(
        pipeline.run_reverse(&test::g_helper_stats, &tile, nullptr, &tp, config)
            .ok());
    CHECK(tile.filtered_buffer().size() == 0);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t elt = 0;
      CHECK(tile.read(&elt, i * sizeof(uint64_t), sizeof(uint64_t)).ok());
      CHECK(elt == i);
    }
  }
}
