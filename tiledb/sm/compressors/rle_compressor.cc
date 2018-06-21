/**
 * @file   rle_compressor.cc
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
 * This file implements the rle compressor class.
 */

#include <cassert>
#include <iostream>

#include "tiledb/sm/compressors/rle_compressor.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/stats.h"

namespace tiledb {
namespace sm {

Status RLE::compress(
    uint64_t value_size, ConstBuffer* input_buffer, Buffer* output_buffer) {
  STATS_FUNC_IN(compressor_rle_compress);

  // Sanity check
  if (input_buffer->data() == nullptr)
    return LOG_STATUS(Status::CompressionError(
        "Failed compressing with RLE; null input buffer"));
  unsigned int cur_run_len = 1;
  unsigned int max_run_len = 65535;
  auto input_cur = (const unsigned char*)input_buffer->data() + value_size;
  auto input_prev = (const unsigned char*)input_buffer->data();
  uint64_t value_num = input_buffer->size() / value_size;
  unsigned char byte;

  // Trivial case
  if (value_num == 0)
    return Status::Ok();

  // Sanity check on input buffer
  if (input_buffer->size() % value_size != 0) {
    return LOG_STATUS(Status::CompressionError(
        "Failed compressing with RLE; invalid input buffer format"));
  }

  // Make runs
  for (uint64_t i = 1; i < value_num; ++i) {
    if (std::memcmp(input_cur, input_prev, value_size) == 0 &&
        cur_run_len < max_run_len) {  // Expand the run
      ++cur_run_len;
    } else {  // Save the run
      // Copy to output buffer
      RETURN_NOT_OK(output_buffer->write(input_prev, value_size));
      byte = (unsigned char)(cur_run_len >> 8);
      RETURN_NOT_OK(output_buffer->write(&byte, sizeof(char)));
      byte = (unsigned char)(cur_run_len % 256);
      RETURN_NOT_OK(output_buffer->write(&byte, sizeof(char)));

      // Reset current run length
      cur_run_len = 1;
    }

    // Update run info
    input_prev = input_cur;
    input_cur = input_prev + value_size;
  }

  // Save final run
  RETURN_NOT_OK(output_buffer->write(input_prev, value_size));
  byte = (unsigned char)(cur_run_len >> 8);
  RETURN_NOT_OK(output_buffer->write(&byte, sizeof(char)));
  byte = (unsigned char)(cur_run_len % 256);
  RETURN_NOT_OK(output_buffer->write(&byte, sizeof(char)));

  return Status::Ok();

  STATS_FUNC_OUT(compressor_rle_compress);
}

Status RLE::decompress(
    uint64_t value_size,
    ConstBuffer* input_buffer,
    PreallocatedBuffer* output_buffer) {
  STATS_FUNC_IN(compressor_rle_decompress);

  // Sanity check
  if (input_buffer->data() == nullptr)
    return LOG_STATUS(Status::CompressionError(
        "Failed decompressing with RLE; null input buffer"));

  auto input_cur = static_cast<const unsigned char*>(input_buffer->data());
  uint64_t run_size = value_size + 2 * sizeof(char);
  uint64_t run_num = input_buffer->size() / run_size;
  unsigned char byte;

  // Trivial case
  if (run_num == 0)
    return Status::Ok();

  // Sanity check on input buffer format
  if (input_buffer->size() % run_size != 0) {
    return LOG_STATUS(Status::CompressionError(
        "Failed decompressing with RLE; invalid input buffer format"));
  }

  // Decompress runs
  for (uint64_t i = 0; i < run_num; ++i) {
    // Retrieve the current run length
    std::memcpy(&byte, input_cur + value_size, sizeof(char));
    uint64_t run_len = (((uint64_t)byte) << 8);
    std::memcpy(&byte, input_cur + value_size + sizeof(char), sizeof(char));
    run_len += (uint64_t)byte;

    // Copy to output buffer
    for (uint64_t j = 0; j < run_len; ++j)
      RETURN_NOT_OK(output_buffer->write(input_cur, value_size))

    // Update input/output tracking info
    input_cur += run_size;
  }

  return Status::Ok();

  STATS_FUNC_OUT(compressor_rle_decompress);
}

uint64_t RLE::overhead(uint64_t nbytes, uint64_t value_size) {
  // In the worst case, RLE adds two bytes per every value in the buffer.
  uint64_t value_num = nbytes / value_size;
  return value_num * 2;
}

}  // namespace sm
}  // namespace tiledb
