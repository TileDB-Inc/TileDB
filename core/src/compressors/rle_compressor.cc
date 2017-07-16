/**
 * @file   rle_compressor.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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

#include "rle_compressor.h"
#include <cassert>
#include <limits>
#include "logger.h"

namespace tiledb {

size_t RLE::compress_bound(size_t nbytes, size_t type_size) {
  // In the worst case, RLE adds two bytes per every value in the buffer.
  // TODO: this can overflow
  int64_t value_num = nbytes / type_size;
  return nbytes + value_num * 2;
}

Status RLE::compress(
    size_t type_size,
    void* input_buffer,
    size_t input_buffer_size,
    void* output_buffer,
    size_t output_buffer_size,
    size_t* compressed_size) {
  *compressed_size = 0;
  int cur_run_len = 1;
  int max_run_len = 65535;
  const unsigned char* input_cur =
      static_cast<unsigned char*>(input_buffer) + type_size;
  const unsigned char* input_prev = static_cast<unsigned char*>(input_buffer);
  unsigned char* output_cur = static_cast<unsigned char*>(output_buffer);
  int64_t value_num = input_buffer_size / type_size;
  int64_t _output_size = 0;
  size_t run_size = type_size + 2 * sizeof(char);
  unsigned char byte;

  // Trivial case
  if (value_num == 0) {
    return Status::Ok();
  }

  // Sanity check on input buffer
  if (input_buffer_size % type_size) {
    return LOG_STATUS(Status::CompressionError(
        "Failed compressing with RLE; invalid input buffer format"));
  }

  // Make runs
  for (int64_t i = 1; i < value_num; ++i) {
    if (!memcmp(input_cur, input_prev, type_size) &&
        cur_run_len < max_run_len) {  // Expand the run
      ++cur_run_len;
    } else {  // Save the run
      // Sanity check on output size
      if (_output_size + run_size > output_buffer_size) {
        return LOG_STATUS(Status::CompressionError(
            "Failed compressing with RLE; output buffer overflow"));
      }

      // Copy to output buffer
      memcpy(output_cur, input_prev, type_size);
      output_cur += type_size;
      byte = (unsigned char)(cur_run_len >> 8);
      memcpy(output_cur, &byte, sizeof(char));
      output_cur += sizeof(char);
      byte = (unsigned char)(cur_run_len % 256);
      memcpy(output_cur, &byte, sizeof(char));
      output_cur += sizeof(char);
      _output_size += run_size;

      // Reset current run length
      cur_run_len = 1;
    }

    // Update run info
    input_prev = input_cur;
    input_cur = input_prev + type_size;
  }

  // Save final run
  // --- Sanity check on size
  if (_output_size + run_size > output_buffer_size) {
    return LOG_STATUS(Status::CompressionError(
        "Failed compressing with RLE; output buffer overflow"));
  }

  // --- Copy to output buffer
  memcpy(output_cur, input_prev, type_size);
  output_cur += type_size;
  byte = (unsigned char)(cur_run_len >> 8);
  memcpy(output_cur, &byte, sizeof(char));
  output_cur += sizeof(char);
  byte = (unsigned char)(cur_run_len % 256);
  memcpy(output_cur, &byte, sizeof(char));
  output_cur += sizeof(char);
  _output_size += run_size;

  assert(_output_size <= std::numeric_limits<size_t>::max());
  *compressed_size = _output_size;
  return Status::Ok();
}

Status RLE::decompress(
    size_t type_size,
    void* input_buffer,
    size_t input_buffer_size,
    void* output_buffer,
    size_t output_buffer_size,
    size_t* decompressed_size) {
  *decompressed_size = 0;
  const unsigned char* input_cur = static_cast<unsigned char*>(input_buffer);
  unsigned char* output_cur = static_cast<unsigned char*>(output_buffer);
  int64_t output_size = 0;
  int64_t run_len;
  size_t run_size = type_size + 2 * sizeof(char);
  int64_t run_num = input_buffer_size / run_size;
  unsigned char byte;

  // Trivial case
  if (input_buffer_size == 0)
    return Status::Ok();

  // Sanity check on input buffer format
  if (input_buffer_size % run_size) {
    return LOG_STATUS(Status::CompressionError(
        "Failed decompressing with RLE; invalid input buffer format"));
  }

  // Decompress runs
  for (int64_t i = 0; i < run_num; ++i) {
    // Retrieve the current run length
    memcpy(&byte, input_cur + type_size, sizeof(char));
    run_len = (((int64_t)byte) << 8);
    memcpy(&byte, input_cur + type_size + sizeof(char), sizeof(char));
    run_len += (int64_t)byte;

    // Sanity check on size
    if (output_size + type_size * run_len > output_buffer_size) {
      return LOG_STATUS(Status::CompressionError(
          "Failed decompressing with RLE; output buffer overflow"));
    }

    // Copy to output buffer
    for (int64_t j = 0; j < run_len; ++j) {
      memcpy(output_cur, input_cur, type_size);
      output_cur += type_size;
    }

    // Update input/output tracking info
    *decompressed_size += type_size * run_len;
    input_cur += run_size;
  }

  return Status::Ok();
}

}  // namespace tiledb
