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
#include <cassert>
#include <limits>

#include "logger.h"
#include "lz4_compressor.h"

namespace tiledb {

uint64_t LZ4::compress_bound(uint64_t nbytes) {
  assert(nbytes <= std::numeric_limits<int>::max());
  return static_cast<uint64_t>(LZ4_compressBound(static_cast<int>(nbytes)));
}

Status LZ4::compress(
    int level, const Buffer* input_buffer, Buffer* output_buffer) {
  // Sanity check
  if (input_buffer->data() == nullptr || output_buffer->data() == nullptr)
    return LOG_STATUS(Status::CompressionError(
        "Failed compressing with LZ4; invalid buffer format"));

  // TODO: level is ignored using the simple api interface

  // Sanity check
  // TODO: these parameters are int and can overflow with uint64_t, need to
  // check
  // TODO: need something better than an assertion here
  // TODO: this can also be handled by chunk-wise compression
  assert(input_buffer->offset() <= std::numeric_limits<int>::max());
  assert(output_buffer->size() <= std::numeric_limits<int>::max());

  // Compress
#if LZ4_VERSION_NUMBER >= 10705
  int ret = LZ4_compress_default(
      static_cast<char*>(input_buffer->data()),
      static_cast<char*>(output_buffer->data()),
      input_buffer->offset(),
      output_buffer->size());
#else
  // deprecated lz4 api
  int ret = LZ4_compress(
      static_cast<char*>(input_buffer->data()),
      static_cast<char*>(output_buffer->data()),
      input_buffer->offset());
#endif

  // Check error
  if (ret < 0)
    return Status::CompressionError("LZ4 compression failed");

  // Set size of compressed data
  output_buffer->set_offset(static_cast<uint64_t>(ret));

  return Status::Ok();
}

Status LZ4::decompress(const Buffer* input_buffer, Buffer* output_buffer) {
  // Sanity check
  if (input_buffer->data() == nullptr || output_buffer->data() == nullptr)
    return LOG_STATUS(Status::CompressionError(
        "Failed decompressing with LZ4; invalid buffer format"));

  // Sanity check
  assert(input_buffer->size() <= std::numeric_limits<int>::max());
  assert(output_buffer->size() <= std::numeric_limits<int>::max());

  // Decompress
  int ret = LZ4_decompress_safe(
      static_cast<char*>(input_buffer->data()),
      static_cast<char*>(output_buffer->data()),
      input_buffer->size(),
      output_buffer->size());

  // Check error
  if (ret < 0)
    return Status::CompressionError("LZ4 decompression failed");

  return Status::Ok();
}

}  // namespace tiledb