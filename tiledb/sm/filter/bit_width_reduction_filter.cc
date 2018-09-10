/**
 * @file   bit_width_reduction_filter.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
 * This file defines class BitWidthReductionFilter.
 */

#include "tiledb/sm/filter/bit_width_reduction_filter.h"
#include "bit_width_reduction_filter.h"
#include "tiledb/sm/filter/filter_pipeline.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/tile/tile.h"

namespace tiledb {
namespace sm {

/** Compute the number of bits required to represent a signed integral value. */
template <typename T>
static inline uint8_t bits_required(T value, std::true_type) {
  if (value >= std::numeric_limits<int8_t>::lowest() &&
      value <= std::numeric_limits<int8_t>::max())
    return 8;
  else if (
      value >= std::numeric_limits<int16_t>::lowest() &&
      value <= std::numeric_limits<int16_t>::max())
    return 16;
  else if (
      value >= std::numeric_limits<int32_t>::lowest() &&
      value <= std::numeric_limits<int32_t>::max())
    return 32;
  else
    return 64;
}

/**
 * Compute the number of bits required to represent an unsigned integral value.
 */
template <typename T>
static inline uint8_t bits_required(T value, std::false_type) {
  uint8_t bits = 0;
  while (value > 0) {
    bits++;
    value >>= 1;
  }
  return bits;
}

/** Compute the number of bits required to represent an integral value. */
template <typename T>
static inline uint8_t bits_required(T value) {
  return bits_required(value, std::is_signed<T>());
}

BitWidthReductionFilter::BitWidthReductionFilter()
    : Filter(FilterType::FILTER_BIT_WIDTH_REDUCTION) {
  max_window_size_ = 256;
}

Status BitWidthReductionFilter::run_forward(
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  auto tile_type = pipeline_->current_tile()->type();
  auto tile_type_size = static_cast<uint8_t>(datatype_size(tile_type));

  // If bit width compression can't work, just return the input unmodified.
  if (!datatype_is_integer(tile_type) || tile_type_size == 1) {
    RETURN_NOT_OK(output->append_view(input));
    RETURN_NOT_OK(output_metadata->append_view(input_metadata));
    return Status::Ok();
  }

  switch (tile_type) {
    case Datatype::INT8:
      return run_forward<int8_t>(
          input_metadata, input, output_metadata, output);
    case Datatype::UINT8:
      return run_forward<uint8_t>(
          input_metadata, input, output_metadata, output);
    case Datatype::INT16:
      return run_forward<int16_t>(
          input_metadata, input, output_metadata, output);
    case Datatype::UINT16:
      return run_forward<uint16_t>(
          input_metadata, input, output_metadata, output);
    case Datatype::INT32:
      return run_forward<int>(input_metadata, input, output_metadata, output);
    case Datatype::UINT32:
      return run_forward<unsigned>(
          input_metadata, input, output_metadata, output);
    case Datatype::INT64:
      return run_forward<int64_t>(
          input_metadata, input, output_metadata, output);
    case Datatype::UINT64:
      return run_forward<uint64_t>(
          input_metadata, input, output_metadata, output);
    default:
      return LOG_STATUS(
          Status::FilterError("Cannot filter; Unsupported input type"));
  }
}

template <typename T>
Status BitWidthReductionFilter::run_forward(
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  auto input_size = static_cast<uint32_t>(input->size());

  // Compute the upper bound on the size of the output.
  std::vector<ConstBuffer> parts = input->buffers();
  auto num_parts = (uint32_t)parts.size();
  uint64_t output_size_ub = 0;
  uint32_t metadata_size = 2 * sizeof(uint32_t);
  uint32_t total_num_windows = 0;
  for (unsigned i = 0; i < num_parts; i++) {
    auto part = &parts[i];
    auto part_size = static_cast<uint32_t>(part->size());
    // Compute overhead for the part
    uint32_t window_size =
        std::min(part_size, max_window_size_) / sizeof(T) * sizeof(T);
    uint32_t num_windows =
        part_size / window_size + uint32_t(bool(part_size % window_size));
    uint32_t overhead =
        num_windows * (sizeof(uint32_t) + sizeof(T) + sizeof(uint8_t));
    output_size_ub += part_size;
    metadata_size += overhead;
    total_num_windows += num_windows;
  }

  // Allocate space in output buffer for the upper bound.
  RETURN_NOT_OK(output->prepend_buffer(output_size_ub));
  Buffer* buffer_ptr = output->buffer_ptr(0);
  buffer_ptr->reset_offset();
  assert(buffer_ptr != nullptr);

  // Forward the existing metadata
  RETURN_NOT_OK(output_metadata->append_view(input_metadata));
  // Allocate a buffer for this filter's metadata and write the header.
  RETURN_NOT_OK(output_metadata->prepend_buffer(metadata_size));
  RETURN_NOT_OK(output_metadata->write(&input_size, sizeof(uint32_t)));
  RETURN_NOT_OK(output_metadata->write(&total_num_windows, sizeof(uint32_t)));

  // Compress all parts.
  for (unsigned i = 0; i < num_parts; i++) {
    RETURN_NOT_OK(compress_part<T>(&parts[i], output, output_metadata));
  }

  return Status::Ok();
}

template <typename T>
Status BitWidthReductionFilter::compress_part(
    ConstBuffer* input,
    FilterBuffer* output,
    FilterBuffer* output_metadata) const {
  // Compute window size in bytes as a multiple of the element width
  auto input_bytes = static_cast<uint32_t>(input->size());
  uint32_t window_size = std::min(input_bytes, max_window_size_);
  window_size = window_size / sizeof(T) * sizeof(T);

  // Compute number of windows
  uint32_t num_windows =
      input_bytes / window_size + uint32_t(bool(input_bytes % window_size));

  // Write each window.
  for (uint32_t i = 0; i < num_windows; i++) {
    // Compute the actual size in bytes of the window (may be smaller at the end
    // of the input if the window size doesn't evenly divide).
    auto window_nbytes = static_cast<uint32_t>(
        std::min(window_size, input_bytes - i * window_size));
    uint32_t window_nelts = window_nbytes / sizeof(T);

    // Write window metadata.
    T window_value_offset;
    uint8_t orig_bits = sizeof(T) * 8;
    uint8_t compressed_bits =
        compute_bits_required(input, window_nelts, &window_value_offset);
    RETURN_NOT_OK(output_metadata->write(&window_value_offset, sizeof(T)));
    RETURN_NOT_OK(output_metadata->write(&compressed_bits, sizeof(uint8_t)));
    RETURN_NOT_OK(output_metadata->write(&window_nbytes, sizeof(uint32_t)));

    if (compressed_bits >= orig_bits || window_nbytes % sizeof(T) != 0) {
      // Can't compress; just write the window bytes unmodified.
      RETURN_NOT_OK(
          output->write((char*)input->data() + input->offset(), window_nbytes));
      input->advance_offset(window_nbytes);
    } else {
      // Compress and write the relative values to output.
      for (uint32_t j = 0; j < window_nelts; j++) {
        T relative_value = input->value<T>() - window_value_offset;

        RETURN_NOT_OK(
            write_compressed_value(output, relative_value, compressed_bits));

        input->advance_offset(sizeof(T));
      }
    }
  }

  return Status::Ok();
}

Status BitWidthReductionFilter::run_reverse(
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  auto tile_type = pipeline_->current_tile()->type();
  auto tile_type_size = static_cast<uint8_t>(datatype_size(tile_type));

  // If bit width compression wasn't applied, just return the input unmodified.
  if (!datatype_is_integer(tile_type) || tile_type_size == 1) {
    RETURN_NOT_OK(output->append_view(input));
    RETURN_NOT_OK(output_metadata->append_view(input_metadata));
    return Status::Ok();
  }

  switch (tile_type) {
    case Datatype::INT8:
      return run_reverse<int8_t>(
          input_metadata, input, output_metadata, output);
    case Datatype::UINT8:
      return run_reverse<uint8_t>(
          input_metadata, input, output_metadata, output);
    case Datatype::INT16:
      return run_reverse<int16_t>(
          input_metadata, input, output_metadata, output);
    case Datatype::UINT16:
      return run_reverse<uint16_t>(
          input_metadata, input, output_metadata, output);
    case Datatype::INT32:
      return run_reverse<int>(input_metadata, input, output_metadata, output);
    case Datatype::UINT32:
      return run_reverse<unsigned>(
          input_metadata, input, output_metadata, output);
    case Datatype::INT64:
      return run_reverse<int64_t>(
          input_metadata, input, output_metadata, output);
    case Datatype::UINT64:
      return run_reverse<uint64_t>(
          input_metadata, input, output_metadata, output);
    default:
      return LOG_STATUS(
          Status::FilterError("Cannot filter; Unsupported input type"));
  }
}

template <typename T>
Status BitWidthReductionFilter::run_reverse(
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  auto tile_type = pipeline_->current_tile()->type();
  auto tile_type_size = datatype_size(tile_type);

  uint32_t num_windows, orig_length;
  RETURN_NOT_OK(input_metadata->read(&orig_length, sizeof(uint32_t)));
  RETURN_NOT_OK(input_metadata->read(&num_windows, sizeof(uint32_t)));

  RETURN_NOT_OK(output->prepend_buffer(orig_length));
  output->reset_offset();

  // Read each window
  for (uint32_t i = 0; i < num_windows; i++) {
    uint32_t window_nbytes;
    T window_value_offset;
    uint8_t orig_bits = sizeof(T) * 8, compressed_bits;
    // Read window header
    RETURN_NOT_OK(input_metadata->read(&window_value_offset, tile_type_size));
    RETURN_NOT_OK(input_metadata->read(&compressed_bits, sizeof(uint8_t)));
    RETURN_NOT_OK(input_metadata->read(&window_nbytes, sizeof(uint32_t)));

    if (compressed_bits >= orig_bits || window_nbytes % sizeof(T) != 0) {
      // Window was not compressed.
      RETURN_NOT_OK(output->write(input, window_nbytes));
      input->advance_offset(window_nbytes);
    } else {
      // Read and uncompress each window value.
      uint32_t window_nelts = window_nbytes / sizeof(T);
      for (uint32_t j = 0; j < window_nelts; j++) {
        T input_value = 0;
        RETURN_NOT_OK(
            read_compressed_value(input, compressed_bits, &input_value));
        input_value += window_value_offset;
        RETURN_NOT_OK(output->write(&input_value, tile_type_size));
      }
    }
  }

  // Output metadata is a view on the input metadata, skipping what was used by
  // this filter.
  auto md_offset = input_metadata->offset();
  RETURN_NOT_OK(output_metadata->append_view(
      input_metadata, md_offset, input_metadata->size() - md_offset));

  return Status::Ok();
}

template <typename T>
uint8_t BitWidthReductionFilter::compute_bits_required(
    ConstBuffer* buffer, uint32_t num_elements, T* min_value) const {
  // Compute the min and max element values within the window.
  T window_min = std::numeric_limits<T>::max(),
    window_max = std::numeric_limits<T>::lowest();
  auto orig_offset = buffer->offset();
  for (uint32_t j = 0; j < num_elements; j++) {
    T input_value = buffer->value<T>();
    window_min = std::min(window_min, input_value);
    window_max = std::max(window_max, input_value);
    buffer->advance_offset(sizeof(T));
  }
  buffer->set_offset(orig_offset);

  // Check for overflow
  T range = window_max - window_min;
  if (range == std::numeric_limits<T>::max())
    return sizeof(T) * 8;

  // Compute the number of bits required to store the max (normalized) window
  // value, rounding to the nearest C integer type width.
  uint8_t bits = bits_required(range + 1);
  if (bits <= 8)
    bits = 8;
  else if (bits <= 16)
    bits = 16;
  else if (bits <= 32)
    bits = 32;
  else
    bits = 64;

  *min_value = window_min;

  return bits;
}

template <typename T>
Status BitWidthReductionFilter::write_compressed_value(
    FilterBuffer* buffer, T value, uint8_t num_bits) const {
  switch (num_bits) {
    case 8: {
      auto val = static_cast<
          typename std::conditional<std::is_signed<T>::value, int8_t, uint8_t>::
              type>(value);
      RETURN_NOT_OK(buffer->write(&val, sizeof(val)));
      break;
    }
    case 16: {
      auto val = static_cast<
          typename std::
              conditional<std::is_signed<T>::value, int16_t, uint16_t>::type>(
          value);
      RETURN_NOT_OK(buffer->write(&val, sizeof(val)));
      break;
    }
    case 32: {
      auto val = static_cast<
          typename std::
              conditional<std::is_signed<T>::value, int32_t, uint32_t>::type>(
          value);
      RETURN_NOT_OK(buffer->write(&val, sizeof(val)));
      break;
    }
    case 64: {
      auto val = static_cast<
          typename std::
              conditional<std::is_signed<T>::value, int64_t, uint64_t>::type>(
          value);
      RETURN_NOT_OK(buffer->write(&val, sizeof(val)));
      break;
    }
    default:
      assert(false);
  }

  return Status::Ok();
}

template <typename T>
Status BitWidthReductionFilter::read_compressed_value(
    FilterBuffer* buffer, uint8_t compressed_bits, T* value) const {
  switch (compressed_bits) {
    case 8: {
      typename std::conditional<std::is_signed<T>::value, int8_t, uint8_t>::type
          val;
      RETURN_NOT_OK(buffer->read(&val, sizeof(val)));
      *value = val;
      break;
    }
    case 16: {
      typename std::conditional<std::is_signed<T>::value, int16_t, uint16_t>::
          type val;
      RETURN_NOT_OK(buffer->read(&val, sizeof(val)));
      *value = val;
      break;
    }
    case 32: {
      typename std::conditional<std::is_signed<T>::value, int32_t, uint32_t>::
          type val;
      RETURN_NOT_OK(buffer->read(&val, sizeof(val)));
      *value = val;
      break;
    }
    case 64: {
      typename std::conditional<std::is_signed<T>::value, int64_t, uint64_t>::
          type val;
      RETURN_NOT_OK(buffer->read(&val, sizeof(val)));
      *value = val;
      break;
    }
    default:
      assert(false);
  }

  return Status::Ok();
}

Status BitWidthReductionFilter::set_option_impl(
    FilterOption option, const void* value) {
  if (value == nullptr)
    return LOG_STATUS(Status::FilterError(
        "Bit width reduction filter error; invalid option value"));

  switch (option) {
    case FilterOption::BIT_WIDTH_MAX_WINDOW:
      max_window_size_ = *(uint32_t*)value;
      return Status::Ok();
    default:
      return LOG_STATUS(Status::FilterError(
          "Bit width reduction filter error; unknown option"));
  }
}

Status BitWidthReductionFilter::get_option_impl(
    FilterOption option, void* value) const {
  switch (option) {
    case FilterOption::BIT_WIDTH_MAX_WINDOW:
      *(uint32_t*)value = max_window_size_;
      return Status::Ok();
    default:
      return LOG_STATUS(Status::FilterError(
          "Bit width reduction filter error; unknown option"));
  }
}

uint32_t BitWidthReductionFilter::max_window_size() const {
  return max_window_size_;
}

void BitWidthReductionFilter::set_max_window_size(uint32_t max_window_size) {
  max_window_size_ = max_window_size;
}

BitWidthReductionFilter* BitWidthReductionFilter::clone_impl() const {
  auto clone = new BitWidthReductionFilter;
  clone->max_window_size_ = max_window_size_;
  return clone;
}

Status BitWidthReductionFilter::deserialize_impl(ConstBuffer* buff) {
  RETURN_NOT_OK(buff->read(&max_window_size_, sizeof(uint32_t)));
  return Status::Ok();
}

Status BitWidthReductionFilter::serialize_impl(Buffer* buff) const {
  RETURN_NOT_OK(buff->write(&max_window_size_, sizeof(uint32_t)));
  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb