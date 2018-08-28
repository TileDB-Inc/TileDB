/**
 * @file unit-filter-pipeline.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
#include "tiledb/sm/filter/compression_filter.h"
#include "tiledb/sm/filter/filter_pipeline.h"
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
      : Filter(FilterType::COMPRESSION) {
  }

  Status run_forward(FilterBuffer* input, FilterBuffer* output) const override {
    auto input_size = input->size();
    RETURN_NOT_OK(output->append_view(input, 0, input_size));
    output->reset_offset();

    uint64_t nelts = input_size / sizeof(uint64_t);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t* val = output->value_ptr<uint64_t>();
      *val += 1;
      output->advance_offset(sizeof(uint64_t));
    }

    return Status::Ok();
  }

  Status run_reverse(FilterBuffer* input, FilterBuffer* output) const override {
    auto input_size = input->size();
    RETURN_NOT_OK(output->append_view(input, 0, input_size));
    output->reset_offset();

    uint64_t nelts = input_size / sizeof(uint64_t);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t* val = output->value_ptr<uint64_t>();
      *val -= 1;
      output->advance_offset(sizeof(uint64_t));
    }

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
      : Filter(FilterType::COMPRESSION) {
  }

  Status run_forward(FilterBuffer* input, FilterBuffer* output) const override {
    auto nelts = input->size() / sizeof(uint64_t);

    // Add a new output buffer.
    RETURN_NOT_OK(output->prepend_buffer(input->size()))
    output->reset_offset();

    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t inc;
      RETURN_NOT_OK(input->read(&inc, sizeof(uint64_t)));
      inc++;
      RETURN_NOT_OK(output->write(&inc, sizeof(uint64_t)));
    }

    return Status::Ok();
  }

  Status run_reverse(FilterBuffer* input, FilterBuffer* output) const override {
    auto nelts = input->size() / sizeof(uint64_t);

    // Add a new output buffer.
    RETURN_NOT_OK(output->prepend_buffer(input->size()))
    output->reset_offset();

    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t inc;
      RETURN_NOT_OK(input->read(&inc, sizeof(uint64_t)));
      inc--;
      RETURN_NOT_OK(output->write(&inc, sizeof(uint64_t)));
    }

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
      : Filter(FilterType::COMPRESSION) {
    increment_ = 1;
  }

  Status run_forward(FilterBuffer* input, FilterBuffer* output) const override {
    auto input_size = input->size();
    RETURN_NOT_OK(output->append_view(input, 0, input_size));
    output->reset_offset();

    uint64_t nelts = input_size / sizeof(uint64_t);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t* val = output->value_ptr<uint64_t>();
      *val += increment_;
      output->advance_offset(sizeof(uint64_t));
    }

    return Status::Ok();
  }

  Status run_reverse(FilterBuffer* input, FilterBuffer* output) const override {
    auto input_size = input->size();
    RETURN_NOT_OK(output->append_view(input, 0, input_size));
    output->reset_offset();

    uint64_t nelts = input_size / sizeof(uint64_t);
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t* val = output->value_ptr<uint64_t>();
      *val -= increment_;
      output->advance_offset(sizeof(uint64_t));
    }

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
      : Filter(FilterType::COMPRESSION) {
  }
  Status run_forward(FilterBuffer* input, FilterBuffer* output) const override {
    auto input_size = input->size();
    auto nelts = input_size / sizeof(uint64_t);

    // The output will be the checksum value followed by the unmodified input,
    // so we'll add a view of the input, and then prepend a buffer for the
    // checksum.
    RETURN_NOT_OK(output->append_view(input, 0, input_size));
    RETURN_NOT_OK(output->prepend_buffer(sizeof(uint64_t)));
    output->reset_offset();

    uint64_t sum = 0;
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t val;
      RETURN_NOT_OK(input->read(&val, sizeof(uint64_t)));
      sum += val;
    }

    RETURN_NOT_OK(output->write(&sum, sizeof(uint64_t)));

    return Status::Ok();
  }

  Status run_reverse(FilterBuffer* input, FilterBuffer* output) const override {
    auto input_size = input->size();
    uint64_t input_sum;
    RETURN_NOT_OK(input->read(&input_sum, sizeof(uint64_t)));
    auto nelts = (input_size - sizeof(uint64_t)) / sizeof(uint64_t);

    uint64_t sum = 0;
    for (uint64_t i = 0; i < nelts; i++) {
      uint64_t val;
      RETURN_NOT_OK(input->read(&val, sizeof(uint64_t)));
      sum += val;
    }

    if (sum != input_sum)
      return Status::FilterError("Filter error; sum does not match.");

    // The output is just a view on the input, skipping the checksum bytes.
    RETURN_NOT_OK(output->append_view(
        input, sizeof(uint64_t), input_size - sizeof(uint64_t)));

    return Status::Ok();
  }

  PseudoChecksumFilter* clone_impl() const override {
    return new PseudoChecksumFilter();
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
      nelts * sizeof(uint64_t) + sizeof(uint64_t) + 2 * sizeof(uint32_t));
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
  auto original_alloc = tile.data();

  FilterPipeline pipeline;
  CHECK(pipeline.add_filter(Add1InPlace()).ok());

  SECTION("- Single stage") {
    CHECK(pipeline.run_forward(&tile).ok());

    // Check new size and number of chunks
    CHECK(tile.buffer() == &buff);
    CHECK(tile.data() != original_alloc);
    CHECK(
        buff.size() ==
        nelts * sizeof(uint64_t) + sizeof(uint64_t) + 2 * sizeof(uint32_t));
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
        nelts * sizeof(uint64_t) + sizeof(uint64_t) + 2 * sizeof(uint32_t));
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
        nelts * sizeof(uint64_t) + sizeof(uint64_t) + 2 * sizeof(uint32_t));
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
        nelts * sizeof(uint64_t) + sizeof(uint64_t) + 2 * sizeof(uint32_t));
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
      nelts * sizeof(uint64_t) + sizeof(uint64_t) + 2 * sizeof(uint32_t));
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
    CHECK(
        pipeline.add_filter(CompressionFilter(Compressor::BLOSC_LZ4, 5)).ok());

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
    CHECK(
        pipeline.add_filter(CompressionFilter(Compressor::BLOSC_LZ4, 5)).ok());

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
    CHECK(
        pipeline.add_filter(CompressionFilter(Compressor::BLOSC_LZ4, 5)).ok());

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
                           sizeof(uint64_t) + 2 * sizeof(uint32_t));
    buff.reset_offset();
    CHECK(buff.value<uint64_t>() == 1);  // Number of chunks
    buff.advance_offset(sizeof(uint64_t));
    CHECK(
        buff.value<uint32_t>() ==
        nelts * sizeof(uint64_t));  // First chunk orig size
    buff.advance_offset(sizeof(uint32_t));
    CHECK(
        buff.value<uint32_t>() ==
        nelts * sizeof(uint64_t) +
            sizeof(uint64_t));  // First chunk filtered size
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
    uint64_t expected_checksum_2 = expected_checksum + 2;
    for (uint64_t i = 0; i < nelts; i++)
      expected_checksum_2 += i + 2;

    // Check new size and number of chunks
    CHECK(tile.buffer() == &buff);
    CHECK(
        buff.size() == nelts * sizeof(uint64_t) + sizeof(uint64_t) +
                           sizeof(uint64_t) + sizeof(uint64_t) +
                           2 * sizeof(uint32_t));
    buff.reset_offset();
    CHECK(buff.value<uint64_t>() == 1);  // Number of chunks
    buff.advance_offset(sizeof(uint64_t));
    CHECK(
        buff.value<uint32_t>() ==
        nelts * sizeof(uint64_t));  // First chunk orig size
    buff.advance_offset(sizeof(uint32_t));
    CHECK(
        buff.value<uint32_t>() ==
        nelts * sizeof(uint64_t) + sizeof(uint64_t) +
            sizeof(uint64_t));  // First chunk filtered size
    buff.advance_offset(sizeof(uint32_t));

    // Checksum
    CHECK(buff.value<uint64_t>() == expected_checksum_2);
    buff.advance_offset(sizeof(uint64_t));

    // Inner checksum (which was modified by the later Add1 filters).
    CHECK(buff.value<uint64_t>() == expected_checksum + 2);
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
      nelts * sizeof(uint64_t) +
          sizeof(uint64_t));  // First chunk filtered size
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

  // List of potential filters to use
  std::vector<std::function<Filter*(void)>> constructors = {
      []() { return new Add1InPlace(); },
      []() { return new Add1OutOfPlace(); },
      []() { return new PseudoChecksumFilter(); }};

  for (int i = 0; i < 100; i++) {
    // Construct a random pipeline
    FilterPipeline pipeline;
    const unsigned max_num_filters = 6;
    std::random_device rd;
    auto pipeline_seed = rd();
    std::mt19937 gen(pipeline_seed);
    std::uniform_int_distribution<> rng1(0, max_num_filters),
        rng2(0, (int)(constructors.size() - 1)), rng01(0, 1);

    INFO("Random pipeline seed: " << pipeline_seed);

    unsigned num_filters = (unsigned)rng1(gen);
    for (unsigned i = 0; i < num_filters; i++) {
      unsigned idx = (unsigned)rng2(gen);
      Filter* filter = constructors[idx]();
      CHECK(pipeline.add_filter(*filter).ok());
    }

    // Maybe add a compressor at the end.
    if (rng01(gen) == 0) {
      CHECK(pipeline.add_filter(CompressionFilter(Compressor::BZIP2, -1)).ok());
    }

    // End result should always be the same as the input.
    CHECK(pipeline.run_forward(&tile).ok());
    CHECK(pipeline.run_reverse(&tile).ok());
    auto* tile_buff = tile.buffer();
    for (uint64_t i = 0; i < nelts; i++)
      REQUIRE(tile_buff->value<uint64_t>(i * sizeof(uint64_t)) == i);
  }
}
