/**
 * @file unit-filter-pipeline.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2019 TileDB, Inc.
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

#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/filter/bit_width_reduction_filter.h"
#include "tiledb/sm/filter/bitshuffle_filter.h"
#include "tiledb/sm/filter/byteshuffle_filter.h"
#include "tiledb/sm/filter/compression_filter.h"
#include "tiledb/sm/filter/encryption_aes256gcm_filter.h"
#include "tiledb/sm/filter/filter_pipeline.h"
#include "tiledb/sm/filter/positive_delta_filter.h"
#include "tiledb/sm/tile/tile.h"

#include <catch.hpp>
#include <functional>
#include <iostream>
#include <random>

using namespace tiledb::sm;

/**
 * Simple filter that modifies the input stream by adding 1 to every input
 * element.
 */
class Add1InPlace : public Filter {
 public:
  // Just use a dummy filter type
  Add1InPlace()
      : Filter(FilterType::FILTER_NONE) {
  }

  Status run_forward(
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
class Add1OutOfPlace : public Filter {
 public:
  // Just use a dummy filter type
  Add1OutOfPlace()
      : Filter(FilterType::FILTER_NONE) {
  }

  Status run_forward(
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
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const override {
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
class AddNInPlace : public Filter {
 public:
  // Just use a dummy filter type
  AddNInPlace()
      : Filter(FilterType::FILTER_NONE) {
    increment_ = 1;
  }

  Status run_forward(
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
class PseudoChecksumFilter : public Filter {
 public:
  // Just use a dummy filter type
  PseudoChecksumFilter()
      : Filter(FilterType::FILTER_NONE) {
  }
  Status run_forward(
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
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const override {
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
      return Status::FilterError("Filter error; sum does not match.");

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
class Add1IncludingMetadataFilter : public Filter {
 public:
  // Just use a dummy filter type
  Add1IncludingMetadataFilter()
      : Filter(FilterType::FILTER_NONE) {
  }

  Status run_forward(
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
      FilterBuffer* input_metadata,
      FilterBuffer* input,
      FilterBuffer* output_metadata,
      FilterBuffer* output) const override {
    if (input_metadata->size() != 2 * sizeof(uint32_t))
      return Status::FilterError("Unexpected input metadata length");

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

TEST_CASE("Filter: Test empty pipeline", "[filter]") {
  // Set up test data
  const uint64_t nelts = 100;
  Buffer buff;
  for (uint64_t i = 0; i < nelts; i++)
    CHECK(buff.write(&i, sizeof(uint64_t)).ok());
  CHECK(buff.size() == nelts * sizeof(uint64_t));

  Tile tile(Datatype::UINT64, sizeof(uint64_t), 0, &buff, false);

  FilterPipeline pipeline;
  CHECK(pipeline.run_forward(&tile).ok());

  // Check new size and number of chunks
  CHECK(
      buff.size() ==
      nelts * sizeof(uint64_t) + sizeof(uint64_t) + 3 * sizeof(uint32_t));
  buff.reset_offset();
  CHECK(buff.value<uint64_t>() == 1);  // Number of chunks
  buff.advance_offset(sizeof(uint64_t));
  CHECK(
      buff.value<uint32_t>() ==
      nelts * sizeof(uint64_t));  // First chunk orig size
  buff.advance_offset(sizeof(uint32_t));
  CHECK(
      buff.value<uint32_t>() ==
      nelts * sizeof(uint64_t));  // First chunk filtered size
  buff.advance_offset(sizeof(uint32_t));
  CHECK(buff.value<uint32_t>() == 0);  // First chunk metadata size
  buff.advance_offset(sizeof(uint32_t));

  // Check all elements unchanged.
  for (uint64_t i = 0; i < nelts; i++) {
    CHECK(buff.value<uint64_t>() == i);
    buff.advance_offset(sizeof(uint64_t));
  }

  CHECK(pipeline.run_reverse(&tile).ok());
  CHECK(tile.buffer() == &buff);
  CHECK(buff.size() == nelts * sizeof(uint64_t));
  buff.reset_offset();
  for (uint64_t i = 0; i < nelts; i++)
    CHECK(buff.value<uint64_t>(i * sizeof(uint64_t)) == i);
}

TEST_CASE("Filter: Test simple in-place pipeline", "[filter]") {
  // Set up test data
  const uint64_t nelts = 100;
  Buffer buff;
  for (uint64_t i = 0; i < nelts; i++)
    CHECK(buff.write(&i, sizeof(uint64_t)).ok());
  CHECK(buff.size() == nelts * sizeof(uint64_t));

  Tile tile(Datatype::UINT64, sizeof(uint64_t), 0, &buff, false);

  // Save the original allocation so that we can check that after running
  // through the pipeline, the tile buffer points to a different memory
  // region.
  auto original_alloc = tile.internal_data();

  FilterPipeline pipeline;
  CHECK(pipeline.add_filter(Add1InPlace()).ok());

  SECTION("- Single stage") {
    CHECK(pipeline.run_forward(&tile).ok());

    // Check new size and number of chunks
    CHECK(tile.buffer() == &buff);
    CHECK(tile.internal_data() != original_alloc);
    CHECK(
        buff.size() ==
        nelts * sizeof(uint64_t) + sizeof(uint64_t) + 3 * sizeof(uint32_t));
    buff.reset_offset();
    CHECK(buff.value<uint64_t>() == 1);  // Number of chunks
    buff.advance_offset(sizeof(uint64_t));
    CHECK(
        buff.value<uint32_t>() ==
        nelts * sizeof(uint64_t));  // First chunk orig size
    buff.advance_offset(sizeof(uint32_t));
    CHECK(
        buff.value<uint32_t>() ==
        nelts * sizeof(uint64_t));  // First chunk filtered size
    buff.advance_offset(sizeof(uint32_t));
    CHECK(buff.value<uint32_t>() == 0);  // First chunk metadata size
    buff.advance_offset(sizeof(uint32_t));

    // Check all elements incremented.
    for (uint64_t i = 0; i < nelts; i++) {
      CHECK(buff.value<uint64_t>() == (i + 1));
      buff.advance_offset(sizeof(uint64_t));
    }

    CHECK(pipeline.run_reverse(&tile).ok());
    CHECK(tile.buffer() == &buff);
    CHECK(buff.size() == nelts * sizeof(uint64_t));
    buff.reset_offset();
    for (uint64_t i = 0; i < nelts; i++)
      CHECK(buff.value<uint64_t>(i * sizeof(uint64_t)) == i);
  }

  SECTION("- Multi-stage") {
    // Add a few more +1 filters and re-run.
    CHECK(pipeline.add_filter(Add1InPlace()).ok());
    CHECK(pipeline.add_filter(Add1InPlace()).ok());
    CHECK(pipeline.run_forward(&tile).ok());

    // Check new size and number of chunks
    CHECK(tile.buffer() == &buff);
    CHECK(
        buff.size() ==
        nelts * sizeof(uint64_t) + sizeof(uint64_t) + 3 * sizeof(uint32_t));
    buff.reset_offset();
    CHECK(buff.value<uint64_t>() == 1);  // Number of chunks
    buff.advance_offset(sizeof(uint64_t));
    CHECK(
        buff.value<uint32_t>() ==
        nelts * sizeof(uint64_t));  // First chunk orig size
    buff.advance_offset(sizeof(uint32_t));
    CHECK(
        buff.value<uint32_t>() ==
        nelts * sizeof(uint64_t));  // First chunk filtered size
    buff.advance_offset(sizeof(uint32_t));
    CHECK(buff.value<uint32_t>() == 0);  // First chunk metadata size
    buff.advance_offset(sizeof(uint32_t));

    // Check all elements incremented.
    for (uint64_t i = 0; i < nelts; i++) {
      CHECK(buff.value<uint64_t>() == (i + 3));
      buff.advance_offset(sizeof(uint64_t));
    }

    CHECK(pipeline.run_reverse(&tile).ok());
    CHECK(tile.buffer() == &buff);
    CHECK(buff.size() == nelts * sizeof(uint64_t));
    buff.reset_offset();
    for (uint64_t i = 0; i < nelts; i++)
      CHECK(buff.value<uint64_t>(i * sizeof(uint64_t)) == i);
  }
}

TEST_CASE("Filter: Test simple out-of-place pipeline", "[filter]") {
  // Set up test data
  const uint64_t nelts = 100;
  Buffer buff;
  for (uint64_t i = 0; i < nelts; i++)
    CHECK(buff.write(&i, sizeof(uint64_t)).ok());
  CHECK(buff.size() == nelts * sizeof(uint64_t));

  Tile tile(Datatype::UINT64, sizeof(uint64_t), 0, &buff, false);

  FilterPipeline pipeline;
  CHECK(pipeline.add_filter(Add1OutOfPlace()).ok());

  SECTION("- Single stage") {
    CHECK(pipeline.run_forward(&tile).ok());

    // Check new size and number of chunks
    CHECK(tile.buffer() == &buff);
    CHECK(
        buff.size() ==
        nelts * sizeof(uint64_t) + sizeof(uint64_t) + 3 * sizeof(uint32_t));
    buff.reset_offset();
    CHECK(buff.value<uint64_t>() == 1);  // Number of chunks
    buff.advance_offset(sizeof(uint64_t));
    CHECK(
        buff.value<uint32_t>() ==
        nelts * sizeof(uint64_t));  // First chunk orig size
    buff.advance_offset(sizeof(uint32_t));
    CHECK(
        buff.value<uint32_t>() ==
        nelts * sizeof(uint64_t));  // First chunk filtered size
    buff.advance_offset(sizeof(uint32_t));
    CHECK(buff.value<uint32_t>() == 0);  // First chunk metadata size
    buff.advance_offset(sizeof(uint32_t));

    // Check all elements incremented.
    for (uint64_t i = 0; i < nelts; i++) {
      CHECK(buff.value<uint64_t>() == (i + 1));
      buff.advance_offset(sizeof(uint64_t));
    }

    CHECK(pipeline.run_reverse(&tile).ok());
    CHECK(tile.buffer() == &buff);
    CHECK(buff.size() == nelts * sizeof(uint64_t));
    buff.reset_offset();
    for (uint64_t i = 0; i < nelts; i++)
      CHECK(buff.value<uint64_t>(i * sizeof(uint64_t)) == i);
  }

  SECTION("- Multi-stage") {
    // Add a few more +1 filters and re-run.
    CHECK(pipeline.add_filter(Add1OutOfPlace()).ok());
    CHECK(pipeline.add_filter(Add1OutOfPlace()).ok());
    CHECK(pipeline.run_forward(&tile).ok());

    // Check new size and number of chunks
    CHECK(tile.buffer() == &buff);
    CHECK(
        buff.size() ==
        nelts * sizeof(uint64_t) + sizeof(uint64_t) + 3 * sizeof(uint32_t));
    buff.reset_offset();
    CHECK(buff.value<uint64_t>() == 1);  // Number of chunks
    buff.advance_offset(sizeof(uint64_t));
    CHECK(
        buff.value<uint32_t>() ==
        nelts * sizeof(uint64_t));  // First chunk orig size
    buff.advance_offset(sizeof(uint32_t));
    CHECK(
        buff.value<uint32_t>() ==
        nelts * sizeof(uint64_t));  // First chunk filtered size
    buff.advance_offset(sizeof(uint32_t));
    CHECK(buff.value<uint32_t>() == 0);  // First chunk metadata size
    buff.advance_offset(sizeof(uint32_t));

    // Check all elements incremented.
    for (uint64_t i = 0; i < nelts; i++) {
      CHECK(buff.value<uint64_t>() == (i + 3));
      buff.advance_offset(sizeof(uint64_t));
    }

    CHECK(pipeline.run_reverse(&tile).ok());
    CHECK(tile.buffer() == &buff);
    CHECK(buff.size() == nelts * sizeof(uint64_t));
    buff.reset_offset();
    for (uint64_t i = 0; i < nelts; i++)
      CHECK(buff.value<uint64_t>(i * sizeof(uint64_t)) == i);
  }
}

TEST_CASE("Filter: Test mixed in- and out-of-place pipeline", "[filter]") {
  // Set up test data
  const uint64_t nelts = 100;
  Buffer buff;
  for (uint64_t i = 0; i < nelts; i++)
    CHECK(buff.write(&i, sizeof(uint64_t)).ok());
  CHECK(buff.size() == nelts * sizeof(uint64_t));

  Tile tile(Datatype::UINT64, sizeof(uint64_t), 0, &buff, false);

  FilterPipeline pipeline;
  CHECK(pipeline.add_filter(Add1InPlace()).ok());
  CHECK(pipeline.add_filter(Add1OutOfPlace()).ok());
  CHECK(pipeline.add_filter(Add1InPlace()).ok());
  CHECK(pipeline.add_filter(Add1OutOfPlace()).ok());
  CHECK(pipeline.run_forward(&tile).ok());

  // Check new size and number of chunks
  CHECK(tile.buffer() == &buff);
  CHECK(
      buff.size() ==
      nelts * sizeof(uint64_t) + sizeof(uint64_t) + 3 * sizeof(uint32_t));
  buff.reset_offset();
  CHECK(buff.value<uint64_t>() == 1);  // Number of chunks
  buff.advance_offset(sizeof(uint64_t));
  CHECK(
      buff.value<uint32_t>() ==
      nelts * sizeof(uint64_t));  // First chunk orig size
  buff.advance_offset(sizeof(uint32_t));
  CHECK(
      buff.value<uint32_t>() ==
      nelts * sizeof(uint64_t));  // First chunk filtered size
  buff.advance_offset(sizeof(uint32_t));
  CHECK(buff.value<uint32_t>() == 0);  // First chunk metadata size
  buff.advance_offset(sizeof(uint32_t));

  // Check all elements incremented.
  for (uint64_t i = 0; i < nelts; i++) {
    CHECK(buff.value<uint64_t>() == (i + 4));
    buff.advance_offset(sizeof(uint64_t));
  }

  CHECK(pipeline.run_reverse(&tile).ok());
  CHECK(tile.buffer() == &buff);
  CHECK(buff.size() == nelts * sizeof(uint64_t));
  buff.reset_offset();
  for (uint64_t i = 0; i < nelts; i++)
    CHECK(buff.value<uint64_t>(i * sizeof(uint64_t)) == i);
}

TEST_CASE("Filter: Test compression", "[filter], [compression]") {
  // Set up test data
  const uint64_t nelts = 100;
  Buffer buff;
  for (uint64_t i = 0; i < nelts; i++)
    CHECK(buff.write(&i, sizeof(uint64_t)).ok());
  CHECK(buff.size() == nelts * sizeof(uint64_t));

  Tile tile(Datatype::UINT64, sizeof(uint64_t), 0, &buff, false);

  // Set up dummy array schema (needed by compressor filter for cell size, etc).
  uint32_t dim_dom[] = {1, 10};
  Dimension dim;
  dim.set_domain(dim_dom);
  Domain domain;
  domain.add_dimension(&dim);
  ArraySchema schema;
  Attribute attr("attr", Datatype::UINT64);
  schema.add_attribute(&attr);
  schema.set_domain(&domain);
  schema.init();

  FilterPipeline pipeline;

  SECTION("- Simple") {
    CHECK(pipeline.add_filter(Add1InPlace()).ok());
    CHECK(pipeline.add_filter(Add1OutOfPlace()).ok());
    CHECK(pipeline.add_filter(CompressionFilter(Compressor::LZ4, 5)).ok());

    CHECK(pipeline.run_forward(&tile).ok());
    // Check compression worked
    CHECK(tile.buffer()->size() < nelts * sizeof(uint64_t));

    CHECK(pipeline.run_reverse(&tile).ok());
    CHECK(tile.buffer()->size() == nelts * sizeof(uint64_t));

    // Check all elements original values.
    buff.reset_offset();
    for (uint64_t i = 0; i < nelts; i++) {
      CHECK(buff.value<uint64_t>() == i);
      buff.advance_offset(sizeof(uint64_t));
    }
  }

  SECTION("- With checksum stage") {
    CHECK(pipeline.add_filter(PseudoChecksumFilter()).ok());
    CHECK(pipeline.add_filter(CompressionFilter(Compressor::LZ4, 5)).ok());

    CHECK(pipeline.run_forward(&tile).ok());
    // Check compression worked
    CHECK(tile.buffer()->size() < nelts * sizeof(uint64_t));

    CHECK(pipeline.run_reverse(&tile).ok());
    CHECK(tile.buffer()->size() == nelts * sizeof(uint64_t));

    // Check all elements original values.
    buff.reset_offset();
    for (uint64_t i = 0; i < nelts; i++) {
      CHECK(buff.value<uint64_t>() == i);
      buff.advance_offset(sizeof(uint64_t));
    }
  }

  SECTION("- With multiple stages") {
    CHECK(pipeline.add_filter(Add1InPlace()).ok());
    CHECK(pipeline.add_filter(PseudoChecksumFilter()).ok());
    CHECK(pipeline.add_filter(Add1OutOfPlace()).ok());
    CHECK(pipeline.add_filter(CompressionFilter(Compressor::LZ4, 5)).ok());

    CHECK(pipeline.run_forward(&tile).ok());
    // Check compression worked
    CHECK(tile.buffer()->size() < nelts * sizeof(uint64_t));

    CHECK(pipeline.run_reverse(&tile).ok());
    CHECK(tile.buffer()->size() == nelts * sizeof(uint64_t));

    // Check all elements original values.
    buff.reset_offset();
    for (uint64_t i = 0; i < nelts; i++) {
      CHECK(buff.value<uint64_t>() == i);
      buff.advance_offset(sizeof(uint64_t));
    }
  }
}

TEST_CASE("Filter: Test pseudo-checksum", "[filter]") {
  // Set up test data
  const uint64_t nelts = 100;
  const uint64_t expected_checksum = 4950;
  Buffer buff;
  for (uint64_t i = 0; i < nelts; i++)
    CHECK(buff.write(&i, sizeof(uint64_t)).ok());
  CHECK(buff.size() == nelts * sizeof(uint64_t));

  Tile tile(Datatype::UINT64, sizeof(uint64_t), 0, &buff, false);

  FilterPipeline pipeline;
  CHECK(pipeline.add_filter(PseudoChecksumFilter()).ok());

  SECTION("- Single stage") {
    CHECK(pipeline.run_forward(&tile).ok());

    // Check new size and number of chunks
    CHECK(tile.buffer() == &buff);
    CHECK(
        buff.size() == nelts * sizeof(uint64_t) + sizeof(uint64_t) +
                           sizeof(uint64_t) + 3 * sizeof(uint32_t));
    buff.reset_offset();
    CHECK(buff.value<uint64_t>() == 1);  // Number of chunks
    buff.advance_offset(sizeof(uint64_t));
    CHECK(
        buff.value<uint32_t>() ==
        nelts * sizeof(uint64_t));  // First chunk orig size
    buff.advance_offset(sizeof(uint32_t));
    CHECK(
        buff.value<uint32_t>() ==
        nelts * sizeof(uint64_t));  // First chunk filtered size
    buff.advance_offset(sizeof(uint32_t));
    CHECK(
        buff.value<uint32_t>() ==
        sizeof(uint64_t));  // First chunk metadata size
    buff.advance_offset(sizeof(uint32_t));

    // Checksum
    CHECK(buff.value<uint64_t>() == expected_checksum);
    buff.advance_offset(sizeof(uint64_t));

    // Check all elements are the same.
    for (uint64_t i = 0; i < nelts; i++) {
      CHECK(buff.value<uint64_t>() == i);
      buff.advance_offset(sizeof(uint64_t));
    }

    CHECK(pipeline.run_reverse(&tile).ok());
    CHECK(tile.buffer() == &buff);
    CHECK(buff.size() == nelts * sizeof(uint64_t));
    buff.reset_offset();
    for (uint64_t i = 0; i < nelts; i++)
      CHECK(buff.value<uint64_t>(i * sizeof(uint64_t)) == i);
  }

  SECTION("- Multi-stage") {
    CHECK(pipeline.add_filter(Add1OutOfPlace()).ok());
    CHECK(pipeline.add_filter(Add1InPlace()).ok());
    CHECK(pipeline.add_filter(PseudoChecksumFilter()).ok());
    CHECK(pipeline.run_forward(&tile).ok());

    // Compute the second (final) checksum value.
    uint64_t expected_checksum_2 = 0;
    for (uint64_t i = 0; i < nelts; i++)
      expected_checksum_2 += i + 2;

    // Check new size and number of chunks
    CHECK(tile.buffer() == &buff);
    CHECK(
        buff.size() == nelts * sizeof(uint64_t) + sizeof(uint64_t) +
                           sizeof(uint64_t) + sizeof(uint64_t) +
                           3 * sizeof(uint32_t));
    buff.reset_offset();
    CHECK(buff.value<uint64_t>() == 1);  // Number of chunks
    buff.advance_offset(sizeof(uint64_t));
    CHECK(
        buff.value<uint32_t>() ==
        nelts * sizeof(uint64_t));  // First chunk orig size
    buff.advance_offset(sizeof(uint32_t));
    CHECK(
        buff.value<uint32_t>() ==
        nelts * sizeof(uint64_t));  // First chunk filtered size
    buff.advance_offset(sizeof(uint32_t));
    CHECK(
        buff.value<uint32_t>() ==
        2 * sizeof(uint64_t));  // First chunk metadata size
    buff.advance_offset(sizeof(uint32_t));

    // Outer checksum
    CHECK(buff.value<uint64_t>() == expected_checksum_2);
    buff.advance_offset(sizeof(uint64_t));

    // Inner checksum
    CHECK(buff.value<uint64_t>() == expected_checksum);
    buff.advance_offset(sizeof(uint64_t));

    // Check all elements are correct.
    for (uint64_t i = 0; i < nelts; i++) {
      CHECK(buff.value<uint64_t>() == i + 2);
      buff.advance_offset(sizeof(uint64_t));
    }

    CHECK(pipeline.run_reverse(&tile).ok());
    CHECK(tile.buffer() == &buff);
    CHECK(buff.size() == nelts * sizeof(uint64_t));
    buff.reset_offset();
    for (uint64_t i = 0; i < nelts; i++)
      CHECK(buff.value<uint64_t>(i * sizeof(uint64_t)) == i);
  }
}

TEST_CASE("Filter: Test pipeline modify filter", "[filter]") {
  // Set up test data
  const uint64_t nelts = 100;
  Buffer buff;
  for (uint64_t i = 0; i < nelts; i++)
    CHECK(buff.write(&i, sizeof(uint64_t)).ok());
  CHECK(buff.size() == nelts * sizeof(uint64_t));

  Tile tile(Datatype::UINT64, sizeof(uint64_t), 0, &buff, false);

  FilterPipeline pipeline;
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

  CHECK(pipeline.run_forward(&tile).ok());

  buff.reset_offset();
  CHECK(buff.value<uint64_t>() == 1);  // Number of chunks
  buff.advance_offset(sizeof(uint64_t));
  CHECK(
      buff.value<uint32_t>() ==
      nelts * sizeof(uint64_t));  // First chunk orig size
  buff.advance_offset(sizeof(uint32_t));
  CHECK(
      buff.value<uint32_t>() ==
      nelts * sizeof(uint64_t));  // First chunk filtered size
  buff.advance_offset(sizeof(uint32_t));
  CHECK(buff.value<uint32_t>() == 0);  // First chunk metadata size
  buff.advance_offset(sizeof(uint32_t));

  // Check all elements incremented.
  for (uint64_t i = 0; i < nelts; i++) {
    CHECK(buff.value<uint64_t>() == (i + 4));
    buff.advance_offset(sizeof(uint64_t));
  }

  CHECK(pipeline.run_reverse(&tile).ok());
  buff.reset_offset();
  for (uint64_t i = 0; i < nelts; i++)
    CHECK(buff.value<uint64_t>(i * sizeof(uint64_t)) == i);
}

TEST_CASE("Filter: Test pipeline copy", "[filter]") {
  // Set up test data
  const uint64_t nelts = 100;
  const uint64_t expected_checksum = 5350;
  Buffer buff;
  for (uint64_t i = 0; i < nelts; i++)
    CHECK(buff.write(&i, sizeof(uint64_t)).ok());
  CHECK(buff.size() == nelts * sizeof(uint64_t));

  Tile tile(Datatype::UINT64, sizeof(uint64_t), 0, &buff, false);

  FilterPipeline pipeline;
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

  CHECK(pipeline_copy.run_forward(&tile).ok());

  buff.reset_offset();
  CHECK(buff.value<uint64_t>() == 1);  // Number of chunks
  buff.advance_offset(sizeof(uint64_t));
  CHECK(
      buff.value<uint32_t>() ==
      nelts * sizeof(uint64_t));  // First chunk orig size
  buff.advance_offset(sizeof(uint32_t));
  CHECK(
      buff.value<uint32_t>() ==
      nelts * sizeof(uint64_t));  // First chunk filtered size
  buff.advance_offset(sizeof(uint32_t));
  CHECK(
      buff.value<uint32_t>() == sizeof(uint64_t));  // First chunk metadata size
  buff.advance_offset(sizeof(uint32_t));

  // Checksum
  CHECK(buff.value<uint64_t>() == expected_checksum);
  buff.advance_offset(sizeof(uint64_t));

  // Check all elements incremented.
  for (uint64_t i = 0; i < nelts; i++) {
    CHECK(buff.value<uint64_t>() == (i + 4));
    buff.advance_offset(sizeof(uint64_t));
  }

  CHECK(pipeline_copy.run_reverse(&tile).ok());
  buff.reset_offset();
  for (uint64_t i = 0; i < nelts; i++)
    CHECK(buff.value<uint64_t>(i * sizeof(uint64_t)) == i);
}

TEST_CASE("Filter: Test random pipeline", "[filter]") {
  // Set up test data
  const uint64_t nelts = 10000;
  Buffer buff;
  for (uint64_t i = 0; i < nelts; i++)
    CHECK(buff.write(&i, sizeof(uint64_t)).ok());
  CHECK(buff.size() == nelts * sizeof(uint64_t));

  Tile tile(Datatype::UINT64, sizeof(uint64_t), 0, &buff, false);

  EncryptionKey encryption_key;
  REQUIRE(encryption_key
              .set_key(
                  EncryptionType::AES_256_GCM,
                  "abcdefghijklmnopqrstuvwxyz012345",
                  32)
              .ok());

  // List of potential filters to use. All of these filters can occur anywhere
  // in the pipeline.
  std::vector<std::function<Filter*(void)>> constructors = {
      []() { return new Add1InPlace(); },
      []() { return new Add1OutOfPlace(); },
      []() { return new Add1IncludingMetadataFilter(); },
      []() { return new BitWidthReductionFilter(); },
      []() { return new BitshuffleFilter(); },
      []() { return new ByteshuffleFilter(); },
      []() { return new CompressionFilter(Compressor::BZIP2, -1); },
      []() { return new PseudoChecksumFilter(); },
      [&encryption_key]() {
        return new EncryptionAES256GCMFilter(encryption_key);
      },
  };

  // List of potential filters that must occur at the beginning of the pipeline.
  std::vector<std::function<Filter*(void)>> constructors_first = {
      // Pos-delta would (correctly) return error after e.g. compression.
      []() { return new PositiveDeltaFilter(); }};

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
        Filter* filter = constructors_first[idx]();
        CHECK(pipeline.add_filter(*filter).ok());
      } else {
        auto idx = (unsigned)rng_constructors(gen);
        Filter* filter = constructors[idx]();
        CHECK(pipeline.add_filter(*filter).ok());
      }
    }

    // End result should always be the same as the input.
    CHECK(pipeline.run_forward(&tile).ok());
    CHECK(pipeline.run_reverse(&tile).ok());
    auto* tile_buff = tile.buffer();
    for (uint64_t i = 0; i < nelts; i++)
      REQUIRE(tile_buff->value<uint64_t>(i * sizeof(uint64_t)) == i);
  }
}

TEST_CASE("Filter: Test bit width reduction", "[filter]") {
  // Set up test data
  const uint64_t nelts = 1000;
  Buffer buff;
  for (uint64_t i = 0; i < nelts; i++)
    CHECK(buff.write(&i, sizeof(uint64_t)).ok());
  CHECK(buff.size() == nelts * sizeof(uint64_t));

  Tile tile(Datatype::UINT64, sizeof(uint64_t), 0, &buff, false);

  FilterPipeline pipeline;
  CHECK(pipeline.add_filter(BitWidthReductionFilter()).ok());

  SECTION("- Single stage") {
    CHECK(pipeline.run_forward(&tile).ok());

    // Sanity check number of windows value
    buff.reset_offset();
    buff.advance_offset(sizeof(uint64_t));  // Number of chunks
    buff.advance_offset(sizeof(uint32_t));  // First chunk orig size
    buff.advance_offset(sizeof(uint32_t));  // First chunk filtered size
    buff.advance_offset(sizeof(uint32_t));  // First chunk metadata size

    CHECK(
        buff.value<uint32_t>() == nelts * sizeof(uint64_t));  // Original length
    buff.advance_offset(sizeof(uint32_t));

    auto max_win_size =
        pipeline.get_filter<BitWidthReductionFilter>()->max_window_size();
    auto expected_num_win =
        (nelts * sizeof(uint64_t)) / max_win_size +
        uint32_t(bool((nelts * sizeof(uint64_t)) % max_win_size));
    CHECK(buff.value<uint32_t>() == expected_num_win);  // Number of windows

    // Check compression worked
    auto compressed_size = buff.size();
    CHECK(compressed_size < nelts * sizeof(uint64_t));

    CHECK(pipeline.run_reverse(&tile).ok());
    CHECK(tile.buffer() == &buff);
    CHECK(tile.buffer()->size() == nelts * sizeof(uint64_t));
    tile.buffer()->reset_offset();
    for (uint64_t i = 0; i < nelts; i++)
      CHECK(tile.buffer()->value<uint64_t>(i * sizeof(uint64_t)) == i);
  }

  SECTION("- Window sizes") {
    std::vector<uint32_t> window_sizes = {
        32, 64, 128, 256, 437, 512, 1024, 2000};
    for (auto window_size : window_sizes) {
      pipeline.get_filter<BitWidthReductionFilter>()->set_max_window_size(
          window_size);

      CHECK(pipeline.run_forward(&tile).ok());
      CHECK(pipeline.run_reverse(&tile).ok());
      CHECK(tile.buffer()->size() == nelts * sizeof(uint64_t));
      tile.buffer()->reset_offset();
      for (uint64_t i = 0; i < nelts; i++)
        CHECK(tile.buffer()->value<uint64_t>(i * sizeof(uint64_t)) == i);
    }
  }

  SECTION("- Random values") {
    std::random_device rd;
    auto seed = rd();
    std::mt19937 gen(seed), gen_copy(seed);
    std::uniform_int_distribution<> rng(0, std::numeric_limits<int32_t>::max());
    INFO("Random element seed: " << seed);

    Buffer buff;
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t val = (uint64_t)rng(gen);
      CHECK(buff.write(&val, sizeof(uint64_t)).ok());
    }
    CHECK(buff.size() == nelts * sizeof(uint64_t));

    Tile tile(Datatype::UINT64, sizeof(uint64_t), 0, &buff, false);

    CHECK(pipeline.run_forward(&tile).ok());
    CHECK(pipeline.run_reverse(&tile).ok());
    CHECK(tile.buffer()->size() == nelts * sizeof(uint64_t));
    tile.buffer()->reset_offset();
    for (uint64_t i = 0; i < nelts; i++)
      CHECK(
          tile.buffer()->value<uint64_t>(i * sizeof(uint64_t)) ==
          rng(gen_copy));
  }

  SECTION(" - Random signed values") {
    std::random_device rd;
    auto seed = rd();
    std::mt19937 gen(seed), gen_copy(seed);
    std::uniform_int_distribution<> rng(
        std::numeric_limits<int32_t>::lowest(),
        std::numeric_limits<int32_t>::max());
    INFO("Random element seed: " << seed);

    Buffer buff;
    for (uint64_t i = 0; i < nelts; i++) {
      int32_t val = (int32_t)rng(gen);
      CHECK(buff.write(&val, sizeof(int32_t)).ok());
    }
    CHECK(buff.size() == nelts * sizeof(int32_t));

    Tile tile(Datatype::INT32, sizeof(int32_t), 0, &buff, false);

    CHECK(pipeline.run_forward(&tile).ok());
    CHECK(pipeline.run_reverse(&tile).ok());
    CHECK(tile.buffer()->size() == nelts * sizeof(int32_t));
    tile.buffer()->reset_offset();
    for (uint64_t i = 0; i < nelts; i++)
      CHECK(
          tile.buffer()->value<int32_t>(i * sizeof(int32_t)) == rng(gen_copy));
  }

  SECTION("- Byte overflow") {
    Buffer buff;
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t val = i % 257;
      CHECK(buff.write(&val, sizeof(uint64_t)).ok());
    }
    CHECK(buff.size() == nelts * sizeof(uint64_t));

    Tile tile(Datatype::UINT64, sizeof(uint64_t), 0, &buff, false);

    CHECK(pipeline.run_forward(&tile).ok());
    CHECK(pipeline.run_reverse(&tile).ok());
    CHECK(tile.buffer()->size() == nelts * sizeof(uint64_t));
    tile.buffer()->reset_offset();
    for (uint64_t i = 0; i < nelts; i++)
      CHECK(tile.buffer()->value<uint64_t>(i * sizeof(uint64_t)) == i % 257);
  }
}

TEST_CASE("Filter: Test positive-delta encoding", "[filter]") {
  // Set up test data
  const uint64_t nelts = 1000;
  Buffer buff;
  for (uint64_t i = 0; i < nelts; i++)
    CHECK(buff.write(&i, sizeof(uint64_t)).ok());
  CHECK(buff.size() == nelts * sizeof(uint64_t));

  Tile tile(Datatype::UINT64, sizeof(uint64_t), 0, &buff, false);

  FilterPipeline pipeline;
  CHECK(pipeline.add_filter(PositiveDeltaFilter()).ok());

  SECTION("- Single stage") {
    CHECK(pipeline.run_forward(&tile).ok());

    auto pipeline_metadata_size = sizeof(uint64_t) + 3 * sizeof(uint32_t);

    buff.reset_offset();
    buff.advance_offset(sizeof(uint64_t));  // Number of chunks
    buff.advance_offset(sizeof(uint32_t));  // First chunk orig size
    buff.advance_offset(sizeof(uint32_t));  // First chunk filtered size
    auto filter_metadata_size =
        buff.value<uint32_t>();  // First chunk metadata size
    buff.advance_offset(sizeof(uint32_t));

    auto max_win_size =
        pipeline.get_filter<PositiveDeltaFilter>()->max_window_size();
    auto expected_num_win =
        (nelts * sizeof(uint64_t)) / max_win_size +
        uint32_t(bool((nelts * sizeof(uint64_t)) % max_win_size));
    CHECK(buff.value<uint32_t>() == expected_num_win);  // Number of windows

    // Check encoded size
    auto encoded_size = buff.size();
    CHECK(
        encoded_size == pipeline_metadata_size + filter_metadata_size +
                            nelts * sizeof(uint64_t));

    CHECK(pipeline.run_reverse(&tile).ok());
    CHECK(tile.buffer() == &buff);
    CHECK(tile.buffer()->size() == nelts * sizeof(uint64_t));
    tile.buffer()->reset_offset();
    for (uint64_t i = 0; i < nelts; i++)
      CHECK(tile.buffer()->value<uint64_t>(i * sizeof(uint64_t)) == i);
  }

  SECTION("- Window sizes") {
    std::vector<uint32_t> window_sizes = {
        32, 64, 128, 256, 437, 512, 1024, 2000};
    for (auto window_size : window_sizes) {
      pipeline.get_filter<PositiveDeltaFilter>()->set_max_window_size(
          window_size);

      CHECK(pipeline.run_forward(&tile).ok());
      CHECK(pipeline.run_reverse(&tile).ok());
      CHECK(tile.buffer()->size() == nelts * sizeof(uint64_t));
      tile.buffer()->reset_offset();
      for (uint64_t i = 0; i < nelts; i++)
        CHECK(tile.buffer()->value<uint64_t>(i * sizeof(uint64_t)) == i);
    }
  }

  SECTION("- Error on non-positive delta data") {
    buff.reset_offset();
    for (uint64_t i = 0; i < nelts; i++) {
      auto val = nelts - i;
      CHECK(buff.write(&val, sizeof(uint64_t)).ok());
    }

    CHECK(!pipeline.run_forward(&tile).ok());
  }
}

TEST_CASE("Filter: Test bitshuffle", "[filter]") {
  // Set up test data
  const uint64_t nelts = 1000;
  Buffer buff;
  for (uint64_t i = 0; i < nelts; i++)
    CHECK(buff.write(&i, sizeof(uint64_t)).ok());
  CHECK(buff.size() == nelts * sizeof(uint64_t));

  Tile tile(Datatype::UINT64, sizeof(uint64_t), 0, &buff, false);

  FilterPipeline pipeline;
  CHECK(pipeline.add_filter(BitshuffleFilter()).ok());

  SECTION("- Single stage") {
    CHECK(pipeline.run_forward(&tile).ok());
    CHECK(pipeline.run_reverse(&tile).ok());
    CHECK(tile.buffer()->size() == nelts * sizeof(uint64_t));
    tile.buffer()->reset_offset();
    for (uint64_t i = 0; i < nelts; i++)
      CHECK(tile.buffer()->value<uint64_t>(i * sizeof(uint64_t)) == i);
  }

  SECTION("- Indivisible by 8") {
    const uint32_t nelts = 1001;
    Buffer buff2;
    for (uint32_t i = 0; i < nelts; i++)
      CHECK(buff2.write(&i, sizeof(uint32_t)).ok());
    CHECK(buff2.size() == nelts * sizeof(uint32_t));
    Tile tile2(Datatype::UINT32, sizeof(uint32_t), 0, &buff2, false);

    CHECK(pipeline.run_forward(&tile2).ok());
    CHECK(pipeline.run_reverse(&tile2).ok());
    CHECK(tile2.buffer()->size() == nelts * sizeof(uint32_t));
    tile2.buffer()->reset_offset();
    for (uint32_t i = 0; i < nelts; i++)
      CHECK(tile2.buffer()->value<uint32_t>(i * sizeof(uint32_t)) == i);
  }
}

TEST_CASE("Filter: Test byteshuffle", "[filter]") {
  // Set up test data
  const uint64_t nelts = 1000;
  Buffer buff;
  for (uint64_t i = 0; i < nelts; i++)
    CHECK(buff.write(&i, sizeof(uint64_t)).ok());
  CHECK(buff.size() == nelts * sizeof(uint64_t));

  Tile tile(Datatype::UINT64, sizeof(uint64_t), 0, &buff, false);

  FilterPipeline pipeline;
  CHECK(pipeline.add_filter(ByteshuffleFilter()).ok());

  SECTION("- Single stage") {
    CHECK(pipeline.run_forward(&tile).ok());
    CHECK(pipeline.run_reverse(&tile).ok());
    CHECK(tile.buffer()->size() == nelts * sizeof(uint64_t));
    tile.buffer()->reset_offset();
    for (uint64_t i = 0; i < nelts; i++)
      CHECK(tile.buffer()->value<uint64_t>(i * sizeof(uint64_t)) == i);
  }

  SECTION("- Uneven number of elements") {
    const uint32_t nelts = 1001;
    Buffer buff2;
    for (uint32_t i = 0; i < nelts; i++)
      CHECK(buff2.write(&i, sizeof(uint32_t)).ok());
    CHECK(buff2.size() == nelts * sizeof(uint32_t));
    Tile tile2(Datatype::UINT32, sizeof(uint32_t), 0, &buff2, false);

    CHECK(pipeline.run_forward(&tile2).ok());
    CHECK(pipeline.run_reverse(&tile2).ok());
    CHECK(tile2.buffer()->size() == nelts * sizeof(uint32_t));
    tile2.buffer()->reset_offset();
    for (uint32_t i = 0; i < nelts; i++)
      CHECK(tile2.buffer()->value<uint32_t>(i * sizeof(uint32_t)) == i);
  }
}

TEST_CASE("Filter: Test encryption", "[filter], [encryption]") {
  // Set up test data
  const uint64_t nelts = 1000;
  Buffer buff;
  for (uint64_t i = 0; i < nelts; i++)
    CHECK(buff.write(&i, sizeof(uint64_t)).ok());
  CHECK(buff.size() == nelts * sizeof(uint64_t));

  Tile tile(Datatype::UINT64, sizeof(uint64_t), 0, &buff, false);

  SECTION("- AES-256-GCM") {
    FilterPipeline pipeline;
    CHECK(pipeline.add_filter(EncryptionAES256GCMFilter()).ok());

    // No key set
    CHECK(!pipeline.run_forward(&tile).ok());

    // Create and set a key
    char key[32];
    for (unsigned i = 0; i < 32; i++)
      key[i] = (char)i;
    auto filter = pipeline.get_filter<EncryptionAES256GCMFilter>();
    CHECK(filter->set_key(key).ok());

    // Check success
    CHECK(pipeline.run_forward(&tile).ok());
    CHECK(pipeline.run_reverse(&tile).ok());
    CHECK(tile.buffer()->size() == nelts * sizeof(uint64_t));
    tile.buffer()->reset_offset();
    for (uint64_t i = 0; i < nelts; i++)
      CHECK(tile.buffer()->value<uint64_t>(i * sizeof(uint64_t)) == i);

    // Check error decrypting with wrong key.
    CHECK(pipeline.run_forward(&tile).ok());
    key[0]++;
    CHECK(filter->set_key(key).ok());
    CHECK(!pipeline.run_reverse(&tile).ok());

    // Fix key and check success. Note: this test depends on the implementation
    // leaving the tile data unmodified when the decryption fails, which is not
    // true in general use of the filter pipeline.
    key[0]--;
    CHECK(filter->set_key(key).ok());
    CHECK(pipeline.run_reverse(&tile).ok());
    CHECK(tile.buffer()->size() == nelts * sizeof(uint64_t));
    tile.buffer()->reset_offset();
    for (uint64_t i = 0; i < nelts; i++)
      CHECK(tile.buffer()->value<uint64_t>(i * sizeof(uint64_t)) == i);
  }
}
