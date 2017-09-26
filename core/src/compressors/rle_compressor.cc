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

#include <cassert>
#include <iostream>

#include "logger.h"
#include "rle_compressor.h"

namespace tiledb {

uint64_t RLE::compress_bound(uint64_t nbytes, uint64_t type_size) {
  // In the worst case, RLE adds two bytes per every value in the buffer.
  uint64_t value_num = nbytes / type_size;
  return nbytes + value_num * 2;
}

Status RLE::compress(
    uint64_t type_size, const Buffer* input_buffer, Buffer* output_buffer) {
  // Sanity check
  if (input_buffer->data() == nullptr || output_buffer->data() == nullptr)
    return LOG_STATUS(Status::CompressionError(
        "Failed compressing with RLE; invalid buffer format"));

  unsigned int cur_run_len = 1;
  unsigned int max_run_len = 65535;
  const unsigned char* input_cur =
      static_cast<unsigned char*>(input_buffer->data()) + type_size;
  auto input_prev = static_cast<const unsigned char*>(input_buffer->data());
  auto output_cur = static_cast<unsigned char*>(output_buffer->data());
  uint64_t value_num = input_buffer->offset() / type_size;
  uint64_t _output_size = 0;
  uint64_t run_size = type_size + 2 * sizeof(char);
  unsigned char byte;

  // Trivial case
  if (value_num == 0) {
    return Status::Ok();
  }

  // Sanity check on input buffer
  if (input_buffer->offset() % type_size != 0) {
    return LOG_STATUS(Status::CompressionError(
        "Failed compressing with RLE; invalid input buffer format"));
  }

  // Make runs
  for (uint64_t i = 1; i < value_num; ++i) {
    if (std::memcmp(input_cur, input_prev, type_size) == 0 &&
        cur_run_len < max_run_len) {  // Expand the run
      ++cur_run_len;
    } else {  // Save the run
      // Sanity check on output size
      if (_output_size + run_size > output_buffer->size()) {
        return LOG_STATUS(Status::CompressionError(
            "Failed compressing with RLE; output buffer overflow"));
      }

      // Copy to output buffer
      std::memcpy(output_cur, input_prev, type_size);
      output_cur += type_size;
      byte = (unsigned char)(cur_run_len >> 8);
      std::memcpy(output_cur, &byte, sizeof(char));
      output_cur += sizeof(char);
      byte = (unsigned char)(cur_run_len % 256);
      std::memcpy(output_cur, &byte, sizeof(char));
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
  if (_output_size + run_size > output_buffer->size()) {
    return LOG_STATUS(Status::CompressionError(
        "Failed compressing with RLE; output buffer overflow"));
  }

  // --- Copy to output buffer
  std::memcpy(output_cur, input_prev, type_size);
  output_cur += type_size;
  byte = (unsigned char)(cur_run_len >> 8);
  std::memcpy(output_cur, &byte, sizeof(char));
  output_cur += sizeof(char);
  byte = (unsigned char)(cur_run_len % 256);
  std::memcpy(output_cur, &byte, sizeof(char));
  _output_size += run_size;

  // Set size of compressed data
  output_buffer->set_offset(_output_size);

  return Status::Ok();
}

Status RLE::decompress(
    uint64_t type_size, const Buffer* input_buffer, Buffer* output_buffer) {
  // Sanity check
  if (input_buffer->data() == nullptr || output_buffer->data() == nullptr)
    return LOG_STATUS(Status::CompressionError(
        "Failed decompressing with RLE; invalid buffer format"));

  auto input_cur = static_cast<const unsigned char*>(input_buffer->data());
  auto output_cur = static_cast<unsigned char*>(output_buffer->data());
  uint64_t run_len;
  uint64_t run_size = type_size + 2 * sizeof(char);
  uint64_t run_num = input_buffer->size() / run_size;
  unsigned char byte;

  // Trivial case
  if (input_buffer->size() == 0)
    return Status::Ok();

  // Sanity check on input buffer format
  if (input_buffer->size() % run_size != 0) {
    return LOG_STATUS(Status::CompressionError(
        "Failed decompressing with RLE; invalid input buffer format"));
  }

  // Decompress runs
  for (uint64_t i = 0; i < run_num; ++i) {
    // Retrieve the current run length
    std::memcpy(&byte, input_cur + type_size, sizeof(char));
    run_len = (((uint64_t)byte) << 8);
    std::memcpy(&byte, input_cur + type_size + sizeof(char), sizeof(char));
    run_len += (uint64_t)byte;

    // Sanity check on size
    if (type_size * run_len > output_buffer->size()) {
      return LOG_STATUS(Status::CompressionError(
          "Failed decompressing with RLE; output buffer overflow"));
    }

    // Copy to output buffer
    for (uint64_t j = 0; j < run_len; ++j) {
      std::memcpy(output_cur, input_cur, type_size);
      output_cur += type_size;
    }

    // Update input/output tracking info
    input_cur += run_size;
  }

  return Status::Ok();
}

}  // namespace tiledb
