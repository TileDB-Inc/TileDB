/**
 * @file   lidar_compressor.cc
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
 * This file implements the lidar compressor class.
 */

#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/compressors/lidar_compressor.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/filter/filter_buffer.h"
#include "tiledb/sm/filter/filter_storage.h"
#include "tiledb/sm/compressors/bzip_compressor.h"
#include "tiledb/sm/misc/parallel_functions.h"
#include "tiledb/sm/filter/xor_filter.h"

#include <cmath>
#include <vector>
#include <utility>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

template<typename W>
struct LidarSortCmp {
  bool operator()(const std::pair<W, size_t>& a, const std::pair<W, size_t>& b) const {
    return a.first < b.first;
  }
};

template<typename W>
Status Lidar::compress(
    Datatype type, int level, ConstBuffer* input_buffer, Buffer* output_buffer) {
  assert(sizeof(W) == 4 || sizeof(W) == 8);
  assert((input_buffer->size() % sizeof(W)) == 0 && input_buffer->size() > 0);
  size_t len = input_buffer->size()/sizeof(W);
  std::vector<std::pair<W, size_t>> vals;
  for (size_t i = 0; i < len; ++i) {
    W val = input_buffer->value<W>(i * sizeof(W));
    vals.push_back(std::make_pair(val, i));
  }
  if (Lidar::compute_tp_ == nullptr) {
    Lidar::compute_tp_ = tdb_new(ThreadPool, 4); // TODO: change later
  }

  // Sort values
  parallel_sort(Lidar::compute_tp_, vals.begin(), vals.end(), LidarSortCmp<W>());
  // switch things back oops

  std::vector<W> num_vals;
  std::vector<size_t> positions;
  for (const auto &elem : vals) {
    num_vals.push_back(elem.first);
    positions.push_back(elem.second);
  }

  // Apply XOR filter.
  // convert std::vector to filter buffer?? 
  // examples of using xor filter in buffer
  Datatype int_type = type == Datatype::FLOAT32 ? Datatype::INT32 : Datatype::INT64;
  Tile tile;
    tile.init_unfiltered(
        constants::format_version, int_type, 0, 1, 0);
  FilterBuffer input;
  input.init(num_vals.data(), num_vals.size() * sizeof(W));
  FilterBuffer output;
  FilterBuffer input_metadata;
  FilterBuffer output_metadata;
  xor_filter_.run_forward(tile, nullptr, &input, &input_metadata, &output, &output_metadata);

  assert(output.num_buffers() == 1);
  Buffer bzip_output;
  RETURN_NOT_OK(BZip::compress(level, &output.buffers()[0], &bzip_output));

  // Write output to output buffer.
  RETURN_NOT_OK(output_buffer->write(&len, sizeof(size_t)));
  output_buffer->advance_offset(sizeof(size_t));
  RETURN_NOT_OK(output_buffer->write(positions.data(), positions.size() * sizeof(size_t)));
  output_buffer->advance_offset(positions.size() * sizeof(size_t));
  RETURN_NOT_OK(output_buffer->write(bzip_output.data(), bzip_output.size()));
  return Status::Ok();
}

Status Lidar::compress(
    Datatype type, int level, ConstBuffer* input_buffer, Buffer* output_buffer) {
  switch (type) {
    case Datatype::FLOAT32: {
      return compress<int32_t>(type, level, input_buffer, output_buffer);
    }
    case Datatype::FLOAT64: {
      return compress<int64_t>(type, level, input_buffer, output_buffer);
    }
    default: {
      return Status_CompressionError("Lidar::compress: attribute type is not a floating point type.");
    }
  }
}

Status Lidar::compress(Datatype type, ConstBuffer* input_buffer, Buffer* output_buffer) {
    return compress(type, default_level_, input_buffer, output_buffer);
}

template<typename W>
Status Lidar::decompress(ConstBuffer* input_buffer, PreallocatedBuffer* output_buffer) {
  size_t len;
  RETURN_NOT_OK(input_buffer->read(&len, sizeof(size_t)));
  std::vector<size_t> positions(len, 0);
  RETURN_NOT_OK(input_buffer->read(positions.data(), len * sizeof(size_t)));
  ConstBuffer gzip_decompress_buffer(input_buffer->cur_data(), input_buffer->nbytes_left_to_read());
  std::vector<W> decompressed_data(len, 0);
  PreallocatedBuffer gzip_output(decompressed_data.data(), len * sizeof(W));
  BZip::decompress(&gzip_decompress_buffer, &gzip_output);

  // XOR filter reverse.
   Datatype int_type = sizeof(W) == 4 ? Datatype::INT32 : Datatype::INT64;
  Tile tile;
    tile.init_unfiltered(
        constants::format_version, int_type, 0, 1, 0);
  uint32_t arr[2];
  arr[0] = 1; arr[1] = len * sizeof(W);
  FilterBuffer xor_input_metadata;
  xor_input_metadata.init(static_cast<void*>(arr), sizeof(uint32_t) * 2);
  FilterBuffer xor_input;
  xor_input.init(decompressed_data.data(), len * sizeof(W));
  FilterBuffer xor_output_metadata;
  FilterBuffer xor_output;
  Config config;

  Lidar::xor_filter_.run_reverse(tile, nullptr, &xor_input_metadata, &xor_input, &xor_output_metadata, &xor_output, config);
  assert(xor_output.num_buffers() == 1);

  auto xor_output_buffer = xor_output.buffers()[0];

  std::vector<W> original_vals(len, 0);
  for (size_t i = 0; i < len; ++i) {
    original_vals[positions[i]] = xor_output_buffer.value<W>(i * sizeof(W));
  }

  output_buffer->write(original_vals.data(), len * sizeof(W));
  
  return Status::Ok();
}

Status Lidar::decompress(
    Datatype type, ConstBuffer* input_buffer, PreallocatedBuffer* output_buffer) {
  switch (type) {
    case Datatype::FLOAT32: {
      return decompress<int32_t>(input_buffer, output_buffer);
    }
    case Datatype::FLOAT64: {
      return decompress<int64_t>(input_buffer, output_buffer);
    }
    default: {
      return Status_CompressionError("Lidar::compress: attribute type is not a floating point type.");
    }
  }
}

uint64_t Lidar::overhead(uint64_t nbytes) {
  return BZip::overhead(nbytes);
}

}  // namespace sm
}  // namespace tiledb
