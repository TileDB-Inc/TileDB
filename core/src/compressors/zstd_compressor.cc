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

size_t ZStd::compress_bound(size_t nbytes) {
  return ZSTD_compressBound(nbytes);
}

Status ZStd::compress(
    size_t type_size,
    void* input_buffer,
    size_t input_buffer_size,
    void* output_buffer,
    size_t output_buffer_size,
    size_t* compressed_size) {
  *compressed_size = 0;
  // TODO: level
  size_t zstd_code = ZSTD_compress(
      output_buffer, output_buffer_size, input_buffer, input_buffer_size, 1);
  if (ZSTD_isError(zstd_code)) {
    const char* msg = ZSTD_getErrorName(zstd_code);
    return LOG_STATUS(Status::CompressionError(
        std::string("ZStd compression failed: ") + msg));
  }
  *compressed_size = zstd_code;
  return Status::Ok();
}

Status ZStd::decompress(
    size_t type_size,
    void* input_buffer,
    size_t input_buffer_size,
    void* output_buffer,
    size_t output_buffer_size,
    size_t* decompressed_size) {
  *decompressed_size = 0;
  size_t zstd_code = ZSTD_decompress(
      output_buffer, output_buffer_size, input_buffer, input_buffer_size);
  if (ZSTD_isError(zstd_code)) {
    const char* msg = ZSTD_getErrorName(zstd_code);
    return LOG_STATUS(Status::CompressionError(
        std::string("ZStd decompression failed: ") + msg));
  }
  *decompressed_size = zstd_code;
  return Status::Ok();
};

}  // namespace tiledb
