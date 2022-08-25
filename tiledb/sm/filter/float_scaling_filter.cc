/**
 * @file   float_scaling_filter.cc
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
 * This file implements class FloatScalingFilter.
 */

#include "tiledb/common/logger.h"
#include "tiledb/sm/enums/filter_option.h"
#include "tiledb/sm/filter/filter_buffer.h"
#include "tiledb/sm/tile/tile.h"

#include "tiledb/sm/filter/float_scaling_filter.h"

#include <cassert>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

void FloatScalingFilter::dump(FILE* out) const {
  if (out == nullptr)
    out = stdout;
  fprintf(
      out,
      "FloatScalingFilter: BYTE_WIDTH=%u, SCALE=%lf, OFFSET=%lf",
      static_cast<uint32_t>(byte_width_),
      scale_,
      offset_);
}

void FloatScalingFilter::serialize_impl(Serializer& serializer) const {
  FilterConfig buffer_struct = {scale_, offset_, byte_width_};
  serializer.write(buffer_struct);
}

template <typename T, typename W>
Status FloatScalingFilter::run_forward(
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  auto input_parts = input->buffers();
  uint32_t num_parts = static_cast<uint32_t>(input_parts.size());
  uint32_t metadata_size = sizeof(uint32_t) + num_parts * sizeof(uint32_t);
  RETURN_NOT_OK(output_metadata->append_view(input_metadata));
  RETURN_NOT_OK(output_metadata->prepend_buffer(metadata_size));
  RETURN_NOT_OK(output_metadata->write(&num_parts, sizeof(uint32_t)));

  // Iterate through all the input buffers.
  for (auto& i : input_parts) {
    uint32_t s = i.size();
    assert(s % sizeof(T) == 0);
    uint32_t num_elems_in_part = (s / sizeof(T));
    uint32_t new_size = num_elems_in_part * sizeof(W);
    RETURN_NOT_OK(output_metadata->write(&new_size, sizeof(uint32_t)));
    RETURN_NOT_OK(output->prepend_buffer(new_size));

    // Iterate through each input buffer, storing each raw float as
    // an integer with the value round((raw_float - offset) / scale).
    const T* part_data = static_cast<const T*>(i.data());
    for (uint32_t j = 0; j < num_elems_in_part; ++j) {
      T elem = part_data[j];
      W converted_elem = static_cast<W>(
          round((elem - static_cast<T>(offset_)) / static_cast<T>(scale_)));
      RETURN_NOT_OK(output->write(&converted_elem, sizeof(W)));
      if (j != num_elems_in_part - 1) {
        output->advance_offset(sizeof(W));
      }
    }
  }

  return Status::Ok();
}

template <typename T>
Status FloatScalingFilter::run_forward(
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  switch (byte_width_) {
    case sizeof(int8_t): {
      return run_forward<T, int8_t>(
          input_metadata, input, output_metadata, output);
    } break;
    case sizeof(int16_t): {
      return run_forward<T, int16_t>(
          input_metadata, input, output_metadata, output);
    } break;
    case sizeof(int32_t): {
      return run_forward<T, int32_t>(
          input_metadata, input, output_metadata, output);
    } break;
    case sizeof(int64_t): {
      return run_forward<T, int64_t>(
          input_metadata, input, output_metadata, output);
    } break;
    default: {
      throw std::logic_error(
          "FloatScalingFilter::run_forward: byte_width_ does not reflect the "
          "size of an integer type.");
    }
  }
}

Status FloatScalingFilter::run_forward(
    const Tile& tile,
    Tile* const,  // offsets_tile
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  auto tile_type = tile.type();
  auto tile_type_size = static_cast<uint8_t>(datatype_size(tile_type));
  switch (tile_type_size) {
    case sizeof(float): {
      return run_forward<float>(input_metadata, input, output_metadata, output);
    } break;
    case sizeof(double): {
      return run_forward<double>(
          input_metadata, input, output_metadata, output);
    } break;
    default: {
      throw std::logic_error(
          "FloatScalingFilter::run_forward: tile_type_size does not reflect "
          "the size of a floating point type.");
    }
  }
}

template <typename T, typename W>
Status FloatScalingFilter::run_reverse(
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  // Get number of parts.
  uint32_t num_parts;
  RETURN_NOT_OK(input_metadata->read(&num_parts, sizeof(uint32_t)));

  // Iterate through the input buffers.
  for (uint32_t i = 0; i < num_parts; ++i) {
    uint32_t part_size;
    RETURN_NOT_OK(input_metadata->read(&part_size, sizeof(uint32_t)));
    ConstBuffer part(nullptr, 0);
    RETURN_NOT_OK(input->get_const_buffer(part_size, &part));

    uint32_t num_elems_in_part = part.size() / sizeof(W);
    RETURN_NOT_OK(output->prepend_buffer(num_elems_in_part * sizeof(T)));

    // Iterate through each input buffer, reversing the value of the stored
    // integer value and writing in the value scale * stored_int + offset.
    const W* part_data = static_cast<const W*>(part.data());
    for (uint32_t j = 0; j < num_elems_in_part; ++j) {
      T elem = static_cast<T>(part_data[j]);
      T converted_elem = static_cast<T>(scale_ * elem + offset_);
      RETURN_NOT_OK(output->write(&converted_elem, sizeof(T)));
    }
  }

  // Output metadata is a view on the input metadata, skipping what was used
  // by this filter.
  auto md_offset = input_metadata->offset();
  RETURN_NOT_OK(output_metadata->append_view(
      input_metadata, md_offset, input_metadata->size() - md_offset));

  return Status::Ok();
}

template <typename T>
Status FloatScalingFilter::run_reverse(
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  switch (byte_width_) {
    case sizeof(int8_t): {
      return run_reverse<T, int8_t>(
          input_metadata, input, output_metadata, output);
    } break;
    case sizeof(int16_t): {
      return run_reverse<T, int16_t>(
          input_metadata, input, output_metadata, output);
    } break;
    case sizeof(int32_t): {
      return run_reverse<T, int32_t>(
          input_metadata, input, output_metadata, output);
    } break;
    case sizeof(int64_t): {
      return run_reverse<T, int64_t>(
          input_metadata, input, output_metadata, output);
    } break;
    default: {
      throw std::logic_error(
          "FloatScalingFilter::run_reverse: byte_width_ does not reflect the "
          "size of an integer type.");
    }
  }
}

Status FloatScalingFilter::run_reverse(
    const Tile& tile,
    Tile* const,  // offsets_tile
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output,
    const Config& config) const {
  (void)config;
  auto tile_type = tile.type();
  auto tile_type_size = static_cast<uint8_t>(datatype_size(tile_type));
  switch (tile_type_size) {
    case sizeof(float): {
      return run_reverse<float>(input_metadata, input, output_metadata, output);
    } break;
    case sizeof(double): {
      return run_reverse<double>(
          input_metadata, input, output_metadata, output);
    } break;
    default: {
      throw std::logic_error(
          "FloatScalingFilter::run_reverse: tile_type_size does not reflect "
          "the size of a floating point type.");
    }
  }
}

Status FloatScalingFilter::set_option_impl(
    FilterOption option, const void* value) {
  if (value == nullptr)
    return LOG_STATUS(
        Status_FilterError("Float scaling filter error; invalid option value"));

  switch (option) {
    case FilterOption::SCALE_FLOAT_BYTEWIDTH: {
      byte_width_ = *(uint64_t*)value;
    } break;
    case FilterOption::SCALE_FLOAT_FACTOR: {
      scale_ = *(double*)value;
    } break;
    case FilterOption::SCALE_FLOAT_OFFSET: {
      offset_ = *(double*)value;
    } break;
    default:
      return LOG_STATUS(
          Status_FilterError("Float scaling filter error; unknown option"));
  }

  return Status::Ok();
}

Status FloatScalingFilter::get_option_impl(
    FilterOption option, void* value) const {
  switch (option) {
    case FilterOption::SCALE_FLOAT_BYTEWIDTH: {
      *(uint64_t*)value = byte_width_;
    } break;
    case FilterOption::SCALE_FLOAT_FACTOR: {
      *(double*)value = scale_;
    } break;
    case FilterOption::SCALE_FLOAT_OFFSET: {
      *(double*)value = offset_;
    } break;
    default:
      return LOG_STATUS(
          Status_FilterError("Float scaling filter error; unknown option"));
  }
  return Status::Ok();
}

/** Returns a new clone of this filter. */
FloatScalingFilter* FloatScalingFilter::clone_impl() const {
  return tdb_new(FloatScalingFilter, byte_width_, scale_, offset_);
}

}  // namespace sm
}  // namespace tiledb