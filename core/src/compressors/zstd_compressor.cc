/**
 * @file   zstd_compressor.cc
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
 * This file implements the zstd compressor class.
 */

#include "zstd_compressor.h"
#include <zstd.h>
#include "logger.h"

namespace tiledb {

uint64_t ZStd::compress_bound(uint64_t nbytes) {
  return ZSTD_compressBound(nbytes);
}

Status ZStd::compress(
    int level, const Buffer* input_buffer, Buffer* output_buffer) {
  // Sanity check
  if (input_buffer->data() == nullptr || output_buffer->data() == nullptr)
    return LOG_STATUS(Status::CompressionError(
        "Failed compressing with ZStd; invalid buffer format"));

  // Compress
  size_t zstd_code = ZSTD_compress(
      output_buffer->data(),
      output_buffer->size(),
      input_buffer->data(),
      input_buffer->offset(),
      level < 0 ? ZStd::default_level() : level);

  // Handle error
  if (ZSTD_isError(zstd_code)) {
    const char* msg = ZSTD_getErrorName(zstd_code);
    return LOG_STATUS(Status::CompressionError(
        std::string("ZStd compression failed: ") + msg));
  }

  // Set size and offset of compressed data
  output_buffer->set_offset(zstd_code);

  return Status::Ok();
}

Status ZStd::decompress(const Buffer* input_buffer, Buffer* output_buffer) {
  // Sanity check
  if (input_buffer->data() == nullptr || output_buffer->data() == nullptr)
    return LOG_STATUS(Status::CompressionError(
        "Failed decompressing with ZStd; invalid buffer format"));

  // Decompress
  size_t zstd_code = ZSTD_decompress(
      output_buffer->data(),
      output_buffer->size(),
      input_buffer->data(),
      input_buffer->size());

  // Check error
  if (ZSTD_isError(zstd_code)) {
    const char* msg = ZSTD_getErrorName(zstd_code);
    return LOG_STATUS(Status::CompressionError(
        std::string("ZStd decompression failed: ") + msg));
  }

  return Status::Ok();
}

}  // namespace tiledb
