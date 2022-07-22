/**
 * @file   xor_filter.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * This file implements class XORFilter.
 */

#include "tiledb/common/logger.h"
#include "tiledb/sm/enums/filter_option.h"
#include "tiledb/sm/filter/filter_buffer.h"
#include "tiledb/sm/tile/tile.h"

#include "tiledb/sm/filter/xor_filter.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

void XORFilter::dump(FILE* out) const {
  if (out == nullptr)
    out = stdout;
  fprintf(out, "XORFilter");
}

Status XORFilter::run_forward(
    const Tile& tile,
    Tile* const,  // offsets_tile
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  auto tile_type = tile.type();

  switch (tile_type) {
    case Datatype::INT8: {
      return run_forward<int8_t>(
          input_metadata, input, output_metadata, output);
    }
    case Datatype::INT16: {
      return run_forward<int16_t>(
          input_metadata, input, output_metadata, output);
    }
    case Datatype::INT32: {
      return run_forward<int32_t>(
          input_metadata, input, output_metadata, output);
    }
    case Datatype::INT64: {
      return run_forward<int64_t>(
          input_metadata, input, output_metadata, output);
    }
    default: {
      return Status_FilterError("XORFilter::run_forward: invalid datatype.");
    }
  }

  return Status::Ok();
}

template <typename T>
Status XORFilter::run_forward(
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  // Output size does not change with this filter.
  RETURN_NOT_OK(output->prepend_buffer(input->size()));
  Buffer* output_buf = output->buffer_ptr(0);
  assert(output_buf != nullptr);

  // Write the metadata
  auto parts = input->buffers();
  auto num_parts = (uint32_t)parts.size();
  uint32_t metadata_size = sizeof(uint32_t) + num_parts * sizeof(uint32_t);
  RETURN_NOT_OK(output_metadata->append_view(input_metadata));
  RETURN_NOT_OK(output_metadata->prepend_buffer(metadata_size));
  RETURN_NOT_OK(output_metadata->write(&num_parts, sizeof(uint32_t)));

  // Shuffle all parts
  for (const auto& part : parts) {
    auto part_size = (uint32_t)part.size();
    RETURN_NOT_OK(output_metadata->write(&part_size, sizeof(uint32_t)));
    RETURN_NOT_OK(shuffle_part<T>(&part, output_buf));
  }

  return Status::Ok();
}

template <typename T>
Status XORFilter::shuffle_part(const ConstBuffer* part, Buffer* output) const {
  uint32_t s = part->size();
  assert(s % sizeof(T) == 0);
  uint32_t num_elems_in_part = s / sizeof(T);

  if (num_elems_in_part == 0) {
    return Status::Ok();
  }

  // Write starting element.
  const T* part_array = static_cast<const T*>(part->data());
  RETURN_NOT_OK(output->write(part_array, sizeof(T)));
  output->advance_offset(sizeof(T));

  for (uint32_t j = 1; j < num_elems_in_part; ++j) {
    T value = part_array[j] - part_array[j - 1];
    RETURN_NOT_OK(output->write(&value, sizeof(T)));

    if (j != num_elems_in_part - 1) {
      output->advance_offset(sizeof(T));
    }
  }

  return Status::Ok();
}

Status XORFilter::run_reverse(
    const Tile& tile,
    Tile* const,  // offsets_tile
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output,
    const Config& config) const {
  (void)config;

  auto tile_type = tile.type();

  switch (tile_type) {
    case Datatype::INT8: {
      return run_reverse<int8_t>(
          input_metadata, input, output_metadata, output);
    }
    case Datatype::INT16: {
      return run_reverse<int16_t>(
          input_metadata, input, output_metadata, output);
    }
    case Datatype::INT32: {
      return run_reverse<int32_t>(
          input_metadata, input, output_metadata, output);
    }
    case Datatype::INT64: {
      return run_reverse<int64_t>(
          input_metadata, input, output_metadata, output);
    }
    default: {
      return Status_FilterError("XORFilter::run_forward: invalid datatype.");
    }
  }

  return Status::Ok();
}

template <typename T>
Status XORFilter::run_reverse(
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  // Get number of parts
  uint32_t num_parts;
  RETURN_NOT_OK(input_metadata->read(&num_parts, sizeof(uint32_t)));

  RETURN_NOT_OK(output->prepend_buffer(input->size()));
  Buffer* output_buf = output->buffer_ptr(0);
  assert(output_buf != nullptr);

  for (uint32_t i = 0; i < num_parts; i++) {
    uint32_t part_size;
    RETURN_NOT_OK(input_metadata->read(&part_size, sizeof(uint32_t)));
    ConstBuffer part(nullptr, 0);
    RETURN_NOT_OK(input->get_const_buffer(part_size, &part));

    RETURN_NOT_OK(unshuffle_part<T>(&part, output_buf));

    if (output_buf->owns_data())
      output_buf->advance_size(part_size);
    output_buf->advance_offset(part_size);
    input->advance_offset(part_size);
  }

  // Output metadata is a view on the input metadata, skipping what was used
  // by this filter.
  auto md_offset = input_metadata->offset();
  RETURN_NOT_OK(output_metadata->append_view(
      input_metadata, md_offset, input_metadata->size() - md_offset));

  return Status::Ok();
}

template <typename T>
Status XORFilter::unshuffle_part(
    const ConstBuffer* part, Buffer* output) const {
  uint32_t s = part->size();
  assert(s % sizeof(T) == 0);
  uint32_t num_elems_in_part = s / sizeof(T);

  if (num_elems_in_part == 0) {
    return Status::Ok();
  }

  // Write starting element.
  const T* part_array = static_cast<const T*>(part->data());
  RETURN_NOT_OK(output->write(part_array, sizeof(T)));
  output->advance_offset(sizeof(T));

  T last_element = part_array[0];
  for (uint32_t j = 1; j < num_elems_in_part; ++j) {
    T value = last_element + part_array[j];
    RETURN_NOT_OK(output->write(&value, sizeof(T)));

    last_element = value;
    if (j != num_elems_in_part - 1) {
      output->advance_offset(sizeof(T));
    }
  }

  return Status::Ok();
}

/** Returns a new clone of this filter. */
XORFilter* XORFilter::clone_impl() const {
  return tdb_new(XORFilter);
}

}  // namespace sm
}  // namespace tiledb