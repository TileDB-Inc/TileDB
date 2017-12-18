/**
 * @file   lz4_compressor.cc
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
 * This file implements the lz4 compressor class.
 */

#include <lz4.h>
#include <limits>

#include "logger.h"
#include "lz4_compressor.h"

namespace tiledb {

Status LZ4::compress(
    int level, ConstBuffer* input_buffer, Buffer* output_buffer) {
  // Sanity check
  if (input_buffer->data() == nullptr || output_buffer->data() == nullptr)
    return LOG_STATUS(Status::CompressionError(
        "Failed compressing with LZ4; invalid buffer format"));

  // TODO: level is ignored using the simple api interface
  (void)level;
// Compress
#if LZ4_VERSION_NUMBER >= 10705
  int ret = LZ4_compress_default(
      (char*)input_buffer->data(),
      (char*)output_buffer->cur_data(),
      (int)input_buffer->size(),
      (int)output_buffer->free_space());
#else
  // deprecated lz4 api
  int ret = LZ4_compress(
      (char*)input_buffer->data(),
      (char*)output_buffer->cur_data(),
      (int)input_buffer->size());
#endif

  // Check error
  if (ret < 0)
    return Status::CompressionError("LZ4 compression failed");

  // Set size of compressed data
  output_buffer->advance_size(static_cast<uint64_t>(ret));
  output_buffer->advance_offset(static_cast<uint64_t>(ret));

  return Status::Ok();
}

Status LZ4::decompress(ConstBuffer* input_buffer, Buffer* output_buffer) {
  // Sanity check
  if (input_buffer->data() == nullptr || output_buffer->data() == nullptr)
    return LOG_STATUS(Status::CompressionError(
        "Failed decompressing with LZ4; invalid buffer format"));

  // Decompress
  int ret = LZ4_decompress_safe(
      (char*)input_buffer->data(),
      (char*)output_buffer->cur_data(),
      (int)input_buffer->size(),
      (int)output_buffer->free_space());

  // Check error
  if (ret < 0)
    return Status::CompressionError("LZ4 decompression failed");

  // Set size of decompressed data
  output_buffer->advance_size(static_cast<uint64_t>(ret));
  output_buffer->advance_offset(static_cast<uint64_t>(ret));

  return Status::Ok();
}

uint64_t LZ4::overhead(uint64_t nbytes) {
  // So that we avoid overflow
  auto half_bound =
      static_cast<uint64_t>(LZ4_compressBound((int)ceil(nbytes / 2.0)));
  return 2 * half_bound - nbytes;
}

}  // namespace tiledb