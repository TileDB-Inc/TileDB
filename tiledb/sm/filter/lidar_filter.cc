/**
 * @file   lidar_filter.cc
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
 * This file implements the lidar filter class.
 */

#include "tiledb/sm/filter/lidar_filter.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/filter/filter_buffer.h"
#include "tiledb/sm/filter/filter_storage.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/tile/tile.h"

#include <cmath>
#include <utility>
#include <vector>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

void LidarFilter::dump(FILE* out) const {
  if (out == nullptr)
    out = stdout;
  fprintf(out, "LidarFilter");
}

/**
 * Run forward. TODO: COMMENT
 */
Status LidarFilter::run_forward(
    const Tile& tile,
    Tile* const,  // offsets_tile
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output) const {
  auto tile_type = tile.type();

  switch (tile_type) {
    case Datatype::FLOAT32: {
      return run_forward<int32_t>(
          input_metadata, input, output_metadata, output);
    }
    case Datatype::FLOAT64: {
      return run_forward<int64_t>(
          input_metadata, input, output_metadata, output);
    }
    default: {
      return Status_FilterError(
          "LidarFilter::run_forward: datatype is not a floating point type.");
    }
  }

  return Status_FilterError("LidarFilter::run_forward: invalid datatype.");
}

template <typename T>
Status LidarFilter::run_forward(
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
    FilterBuffer b;
    RETURN_NOT_OK(shuffle_part<T>(&i, &b));
    output->append_view(&b);
  }

  return Status::Ok();
}

template <typename T>
Status LidarFilter::shuffle_part(
    ConstBuffer* input_buffer, FilterBuffer* output_buffer) const {
  assert(sizeof(T) == 4 || sizeof(T) == 8);
  assert((input_buffer->size() % sizeof(T)) == 0 && input_buffer->size() > 0);
  size_t len = input_buffer->size() / sizeof(T);
  std::vector<std::pair<T, size_t>> vals;
  for (size_t i = 0; i < len; ++i) {
    T val = input_buffer->value<T>(i * sizeof(T));
    vals.push_back(std::make_pair(val, i));
  }

  // Sort values
  std::sort(vals.begin(), vals.end());

  std::vector<T> num_vals;
  std::vector<size_t> positions;
  for (const auto& elem : vals) {
    num_vals.push_back(elem.first);
    positions.push_back(elem.second);
  }

  // Apply XOR filter.
  Datatype int_type = sizeof(T) == 4 ? Datatype::INT32 : Datatype::INT64;
  Tile tile(int_type, 0, 0, nullptr, 0);
  FilterBuffer input;
  input.init(num_vals.data(), num_vals.size() * sizeof(T));
  FilterBuffer xor_output;
  FilterBuffer input_metadata;
  FilterBuffer xor_output_metadata;
  xor_filter_.run_forward(
      tile,
      nullptr,
      &input,
      &input_metadata,
      &xor_output,
      &xor_output_metadata);

  assert(output.num_buffers() == 1);
  FilterBuffer bzip_output_metadata;
  FilterBuffer bzip_output;
  RETURN_NOT_OK(compressor_filter_.run_forward(
      tile,
      nullptr,
      &xor_output_metadata,
      &xor_output,
      &bzip_output_metadata,
      &bzip_output));

  // Write output to output buffer.
  RETURN_NOT_OK(output_buffer->write(&len, sizeof(size_t)));
  output_buffer->advance_offset(sizeof(size_t));
  RETURN_NOT_OK(output_buffer->write(
      positions.data(), positions.size() * sizeof(size_t)));
  output_buffer->advance_offset(positions.size() * sizeof(size_t));
  RETURN_NOT_OK(output_buffer->append_view(&xor_output));
  return Status::Ok();
}

/**
 * Run reverse. TODO: comment
 */
Status LidarFilter::run_reverse(
    const Tile& tile,
    Tile* const,  // offsets_tile
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output,
    const Config& config) const {
  auto tile_type = tile.type();

  switch (tile_type) {
    case Datatype::FLOAT32: {
      return run_reverse<int32_t>(
          input_metadata, input, output_metadata, output, config);
    }
    case Datatype::FLOAT64: {
      return run_reverse<int64_t>(
          input_metadata, input, output_metadata, output, config);
    }
    default: {
      return Status_FilterError(
          "LidarFilter::run_forward: datatype is not a floating point type.");
    }
  }

  return Status_FilterError("LidarFilter::run_reverse: invalid datatype.");
}

template <typename T>
Status LidarFilter::run_reverse(
    FilterBuffer* input_metadata,
    FilterBuffer* input,
    FilterBuffer* output_metadata,
    FilterBuffer* output,
    const Config& config) const {
  // Get number of parts
  uint32_t num_parts;
  RETURN_NOT_OK(input_metadata->read(&num_parts, sizeof(uint32_t)));

  for (uint32_t i = 0; i < num_parts; i++) {
    uint32_t part_size;
    RETURN_NOT_OK(input_metadata->read(&part_size, sizeof(uint32_t)));
    ConstBuffer part(nullptr, 0);
    RETURN_NOT_OK(input->get_const_buffer(part_size, &part));

    FilterBuffer output_buf;
    RETURN_NOT_OK(unshuffle_part<T>(&part, &output_buf, config));
    output->append_view(&output_buf);

    input->advance_offset(part_size);
  }

  // Output metadata is a vieT on the input metadata, skipping what was used
  // by this filter.
  auto md_offset = input_metadata->offset();
  RETURN_NOT_OK(output_metadata->append_view(
      input_metadata, md_offset, input_metadata->size() - md_offset));

  return Status::Ok();
}

template <typename T>
Status LidarFilter::unshuffle_part(
    ConstBuffer* input_buffer,
    FilterBuffer* output_buffer,
    const Config& config) const {
  size_t len;
  RETURN_NOT_OK(input_buffer->read(&len, sizeof(size_t)));
  std::vector<size_t> positions(len, 0);
  RETURN_NOT_OK(input_buffer->read(positions.data(), len * sizeof(size_t)));
  ConstBuffer gzip_decompress_buffer(
      input_buffer->cur_data(), input_buffer->nbytes_left_to_read());
  std::vector<T> decompressed_data(len, 0);
  PreallocatedBuffer gzip_output(decompressed_data.data(), len * sizeof(T));
  BZip::decompress(&gzip_decompress_buffer, &gzip_output);

  // XOR filter reverse.
  Datatype int_type = sizeof(T) == 4 ? Datatype::INT32 : Datatype::INT64;
  Tile tile;
  tile.init_unfiltered(constants::format_version, int_type, 0, 1, 0);
  uint32_t arr[2];
  arr[0] = 1;
  arr[1] = len * sizeof(T);
  FilterBuffer xor_input_metadata;
  xor_input_metadata.init(static_cast<void*>(arr), sizeof(uint32_t) * 2);
  FilterBuffer xor_input;
  xor_input.init(decompressed_data.data(), len * sizeof(T));
  FilterBuffer xor_output_metadata;
  FilterBuffer xor_output;

  xor_filter_.run_reverse(
      tile,
      nullptr,
      &xor_input_metadata,
      &xor_input,
      &xor_output_metadata,
      &xor_output,
      config);
  assert(xor_output.num_buffers() == 1);

  auto xor_output_buffer = xor_output.buffers()[0];

  std::vector<T> original_vals(len, 0);
  for (size_t i = 0; i < len; ++i) {
    original_vals[positions[i]] = xor_output_buffer.value<T>(i * sizeof(T));
  }

  output_buffer->write(original_vals.data(), len * sizeof(T));

  return Status::Ok();
}

LidarFilter* LidarFilter::clone_impl() const {
  return tdb_new(LidarFilter);
}

}  // namespace sm
}  // namespace tiledb
