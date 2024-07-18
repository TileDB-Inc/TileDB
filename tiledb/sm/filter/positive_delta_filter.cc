/**
 * @file   positive_delta_filter.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
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
 * This file defines class PositiveDeltaFilter.
 */

#include "tiledb/sm/filter/positive_delta_filter.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/filter_option.h"
#include "tiledb/sm/enums/filter_type.h"
#include "tiledb/sm/filter/filter_pipeline.h"
#include "tiledb/sm/tile/tile.h"

using namespace tiledb::common;

namespace tiledb::sm {

PositiveDeltaFilter::PositiveDeltaFilter(Datatype filter_data_type)
    : Filter(FilterType::FILTER_POSITIVE_DELTA, filter_data_type) {
  max_window_size_ = 1024;
}

PositiveDeltaFilter::PositiveDeltaFilter(
    uint32_t max_window_size, Datatype filter_data_type)
    : Filter(FilterType::FILTER_POSITIVE_DELTA, filter_data_type)
    , max_window_size_(max_window_size) {
}

std::ostream& PositiveDeltaFilter::output(std::ostream& os) const {
  os << "PositiveDelta: POSITIVE_DELTA_MAX_WINDOW=";
  os << std::to_string(max_window_size_);
  return os;
}

bool PositiveDeltaFilter::accepts_input_datatype(Datatype datatype) const {
  if (datatype_is_integer(datatype) || datatype_is_datetime(datatype) ||
      datatype_is_time(datatype) || datatype_is_byte(datatype)) {
    return true;
  }
  return false;
}

void PositiveDeltaFilter::run_forward(
    const WriterTile& tile,
    WriterTile* const offsets_tile,
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  /* Note: Arithmetic operations cannot be performed on std::byte.
    We will use uint8_t for the byte-type cases as it is the same size as
    std::byte and can have arithmetic performed on it. */
  switch (filter_data_type_) {
    case Datatype::INT8:
      return run_forward<int8_t>(
          tile, offsets_tile, input_metadata, input, output_metadata, output);
    case Datatype::BLOB:
    case Datatype::GEOM_WKB:
    case Datatype::GEOM_WKT:
    case Datatype::BOOL:
    case Datatype::UINT8:
      return run_forward<uint8_t>(
          tile, offsets_tile, input_metadata, input, output_metadata, output);
    case Datatype::INT16:
      return run_forward<int16_t>(
          tile, offsets_tile, input_metadata, input, output_metadata, output);
    case Datatype::UINT16:
      return run_forward<uint16_t>(
          tile, offsets_tile, input_metadata, input, output_metadata, output);
    case Datatype::INT32:
      return run_forward<int>(
          tile, offsets_tile, input_metadata, input, output_metadata, output);
    case Datatype::UINT32:
      return run_forward<unsigned>(
          tile, offsets_tile, input_metadata, input, output_metadata, output);
    case Datatype::INT64:
      return run_forward<int64_t>(
          tile, offsets_tile, input_metadata, input, output_metadata, output);
    case Datatype::UINT64:
      return run_forward<uint64_t>(
          tile, offsets_tile, input_metadata, input, output_metadata, output);
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
      if (tile.format_version() < 20) {
        // Return data as-is for backwards compatibility.
        throw_if_not_ok(output->append_view(input));
        throw_if_not_ok(output_metadata->append_view(input_metadata));
        return;
      }
      return run_forward<int64_t>(
          tile, offsets_tile, input_metadata, input, output_metadata, output);
    default:
      // If encoding can't work, just return the input unmodified.
      throw_if_not_ok(output->append_view(input));
      throw_if_not_ok(output_metadata->append_view(input_metadata));
      return;
  }
}

template <typename T>
void PositiveDeltaFilter::run_forward(
    const WriterTile&,
    WriterTile* const,
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
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
    uint32_t overhead = num_windows * (sizeof(uint32_t) + sizeof(T));
    output_size_ub += part_size;
    metadata_size += overhead;
    total_num_windows += num_windows;
  }

  // Allocate space in output buffer for the upper bound.
  throw_if_not_ok(output->prepend_buffer(output_size_ub));
  Buffer* buffer_ptr = output->buffer_ptr(0);
  buffer_ptr->reset_offset();
  assert(buffer_ptr != nullptr);

  // Forward the existing metadata
  throw_if_not_ok(output_metadata->append_view(input_metadata));
  // Allocate a buffer for this filter's metadata and write the header.
  throw_if_not_ok(output_metadata->prepend_buffer(metadata_size));
  throw_if_not_ok(output_metadata->write(&total_num_windows, sizeof(uint32_t)));

  // Compress all parts.
  for (unsigned i = 0; i < num_parts; i++) {
    throw_if_not_ok(encode_part<T>(&parts[i], output, output_metadata));
  }
}

template <typename T>
Status PositiveDeltaFilter::encode_part(
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

    // Write window header.
    T window_value_offset = input->value<T>();
    RETURN_NOT_OK(output_metadata->write(&window_value_offset, sizeof(T)));
    RETURN_NOT_OK(output_metadata->write(&window_nbytes, sizeof(uint32_t)));

    if (window_nbytes % sizeof(T) != 0) {
      // Can't encode; just write the window bytes unmodified.
      RETURN_NOT_OK(
          output->write((char*)input->data() + input->offset(), window_nbytes));
      input->advance_offset(window_nbytes);
    } else {
      // Encode and write the relative values to output.
      T prev_value = input->value<T>();
      for (uint32_t j = 0; j < window_nelts; j++) {
        T curr_value = input->value<T>();
        if (curr_value < prev_value)
          return LOG_STATUS(Status_FilterError(
              "Positive delta filter error: delta is not positive."));

        T delta = curr_value - prev_value;
        RETURN_NOT_OK(output->write(&delta, sizeof(T)));
        input->advance_offset(sizeof(T));

        prev_value = curr_value;
      }
    }
  }

  return Status::Ok();
}

Status PositiveDeltaFilter::run_reverse(
    const Tile& tile,
    Tile* const offsets_tile,
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output,
    const Config&) const {
  /* Note: Arithmetic operations cannot be performed on std::byte.
    We will use uint8_t for the byte-type cases as it is the same size as
    std::byte and can have arithmetic perfomed on it. */
  switch (filter_data_type_) {
    case Datatype::INT8:
      return run_reverse<int8_t>(
          tile, offsets_tile, input_metadata, input, output_metadata, output);
    case Datatype::BLOB:
    case Datatype::GEOM_WKB:
    case Datatype::GEOM_WKT:
    case Datatype::BOOL:
    case Datatype::UINT8:
      return run_reverse<uint8_t>(
          tile, offsets_tile, input_metadata, input, output_metadata, output);
    case Datatype::INT16:
      return run_reverse<int16_t>(
          tile, offsets_tile, input_metadata, input, output_metadata, output);
    case Datatype::UINT16:
      return run_reverse<uint16_t>(
          tile, offsets_tile, input_metadata, input, output_metadata, output);
    case Datatype::INT32:
      return run_reverse<int>(
          tile, offsets_tile, input_metadata, input, output_metadata, output);
    case Datatype::UINT32:
      return run_reverse<unsigned>(
          tile, offsets_tile, input_metadata, input, output_metadata, output);
    case Datatype::INT64:
      return run_reverse<int64_t>(
          tile, offsets_tile, input_metadata, input, output_metadata, output);
    case Datatype::UINT64:
      return run_reverse<uint64_t>(
          tile, offsets_tile, input_metadata, input, output_metadata, output);
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
      if (tile.format_version() < 20) {
        // Return data as-is for backwards compatibility.
        RETURN_NOT_OK(output->append_view(input));
        RETURN_NOT_OK(output_metadata->append_view(input_metadata));
        return Status::Ok();
      }
      return run_reverse<int64_t>(
          tile, offsets_tile, input_metadata, input, output_metadata, output);
    default:
      // If encoding wasn't applied, just return the input unmodified.
      RETURN_NOT_OK(output->append_view(input));
      RETURN_NOT_OK(output_metadata->append_view(input_metadata));
      return Status::Ok();
  }
}

template <typename T>
Status PositiveDeltaFilter::run_reverse(
    const Tile&,
    Tile* const,
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  auto tile_type_size = datatype_size(filter_data_type_);

  uint32_t num_windows;
  RETURN_NOT_OK(input_metadata->read(&num_windows, sizeof(uint32_t)));

  RETURN_NOT_OK(output->prepend_buffer(input->size()));
  output->reset_offset();

  // Read each window
  for (uint32_t i = 0; i < num_windows; i++) {
    uint32_t window_nbytes;
    T window_value_offset;
    // Read window header
    RETURN_NOT_OK(input_metadata->read(&window_value_offset, tile_type_size));
    RETURN_NOT_OK(input_metadata->read(&window_nbytes, sizeof(uint32_t)));

    if (window_nbytes % sizeof(T) != 0) {
      // Window was not encoded.
      RETURN_NOT_OK(output->write(input, window_nbytes));
      input->advance_offset(window_nbytes);
    } else {
      // Read and decode each window value.
      uint32_t window_nelts = window_nbytes / sizeof(T);
      T prev_value = window_value_offset;
      for (uint32_t j = 0; j < window_nelts; j++) {
        T delta;
        RETURN_NOT_OK(input->read(&delta, sizeof(T)));
        T decoded_value = prev_value + delta;
        RETURN_NOT_OK(output->write(&decoded_value, tile_type_size));

        prev_value = decoded_value;
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

Status PositiveDeltaFilter::set_option_impl(
    FilterOption option, const void* value) {
  if (value == nullptr)
    return LOG_STATUS(Status_FilterError(
        "Positive delta filter error; invalid option value"));

  switch (option) {
    case FilterOption::POSITIVE_DELTA_MAX_WINDOW:
      max_window_size_ = *(uint32_t*)value;
      return Status::Ok();
    default:
      return LOG_STATUS(
          Status_FilterError("Positive delta filter error; unknown option"));
  }
}

Status PositiveDeltaFilter::get_option_impl(
    FilterOption option, void* value) const {
  switch (option) {
    case FilterOption::POSITIVE_DELTA_MAX_WINDOW:
      *(uint32_t*)value = max_window_size_;
      return Status::Ok();
    default:
      return LOG_STATUS(
          Status_FilterError("Positive delta filter error; unknown option"));
  }
}

uint32_t PositiveDeltaFilter::max_window_size() const {
  return max_window_size_;
}

void PositiveDeltaFilter::set_max_window_size(uint32_t max_window_size) {
  max_window_size_ = max_window_size;
}

PositiveDeltaFilter* PositiveDeltaFilter::clone_impl() const {
  auto clone = tdb_new(PositiveDeltaFilter, filter_data_type_);
  clone->max_window_size_ = max_window_size_;
  return clone;
}

void PositiveDeltaFilter::serialize_impl(Serializer& serializer) const {
  serializer.write<uint32_t>(max_window_size_);
}

}  // namespace tiledb::sm
