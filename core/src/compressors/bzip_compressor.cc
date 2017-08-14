/**
 * @file   bzip_compressor.cc
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
 * This file implements the bzip compressor class.
 */

#include "bzip_compressor.h"
#include "logger.h"

#include <bzlib.h>
#include <cassert>
#include <cmath>
#include <limits>

namespace tiledb {

uint64_t BZip::compress_bound(uint64_t nbytes) {
  // From the BZip2 documentation:
  // To guarantee that the compressed data will fit in its buffer, allocate an
  // output buffer of size 1% larger than the uncompressed data, plus six
  // hundred extra bytes.
  float fbytes = ceil(nbytes * 1.01) + 600;
  assert(fbytes <= std::numeric_limits<size_t>::max());
  return static_cast<uint64_t>(fbytes);
}

Status BZip::compress(
    int level, const Buffer* input_buffer, Buffer* output_buffer) {
  // Sanity check
  if (input_buffer->data() == nullptr || output_buffer->data() == nullptr)
    return LOG_STATUS(Status::CompressionError(
        "Failed compressing with BZip; invalid buffer format"));

  // Sanity checks
  // TODO: put something better than assertions here
  assert(input_buffer->offset() <= std::numeric_limits<unsigned int>::max());
  assert(output_buffer->size() <= std::numeric_limits<unsigned int>::max());
  unsigned int in_size = input_buffer->offset();
  unsigned int out_size = output_buffer->size();

  // Compress
  int rc = BZ2_bzBuffToBuffCompress(
      static_cast<char*>(output_buffer->data()),
      &out_size,
      static_cast<char*>(input_buffer->data()),
      in_size,
      level < 1 ? BZip::default_level() : level,  // block size 100k
      0,                                          // verbosity
      0);                                         // work factor

  // Handle error
  if (rc != BZ_OK) {
    switch (rc) {
      case BZ_CONFIG_ERROR:
        return Status::CompressionError(
            "BZip compression error: library has been miscompiled");
      case BZ_PARAM_ERROR:
        return Status::CompressionError(
            "BZip compression error: 'output_buffer' or 'output_buffer_size' "
            "is NULL");
      case BZ_MEM_ERROR:
        return Status::CompressionError(
            "BZip compression error: insufficient memory");
      case BZ_OUTBUFF_FULL:
        return Status::CompressionError(
            "BZip compression error: compressed size exceeds limits for "
            "'output_buffer_size'");
      default:
        return Status::CompressionError(
            "BZip compression error: unknown error code");
    }
  }

  // Set size of compressed data
  output_buffer->set_offset(out_size);

  return Status::Ok();
}

Status BZip::decompress(const Buffer* input_buffer, Buffer* output_buffer) {
  // Sanity check
  if (input_buffer->data() == nullptr || output_buffer->data() == nullptr)
    return LOG_STATUS(Status::CompressionError(
        "Failed decompressing with BZip; invalid buffer format"));

  // Sanity checks
  // TODO: put something better than assertions
  assert(input_buffer->size() <= std::numeric_limits<unsigned int>::max());
  assert(output_buffer->size() <= std::numeric_limits<unsigned int>::max());

  // Decompress
  unsigned int out_size = output_buffer->size();
  int rc = BZ2_bzBuffToBuffDecompress(
      static_cast<char*>(output_buffer->data()),
      &out_size,
      static_cast<char*>(input_buffer->data()),
      input_buffer->size(),
      0,   // small bzip data format stream
      0);  // verbositiy

  // Handle error
  if (rc != BZ_OK) {
    switch (rc) {
      case BZ_CONFIG_ERROR:
        return Status::CompressionError(
            "BZip decompression error: library has been miscompiled");
      case BZ_PARAM_ERROR:
        return Status::CompressionError(
            "BZip decompression error: 'output_buffer' or 'output_buffer_size' "
            "is NULL");
      case BZ_MEM_ERROR:
        return Status::CompressionError(
            "BZip decompression error: insufficient memory");
      case BZ_DATA_ERROR:
      case BZ_DATA_ERROR_MAGIC:
      case BZ_UNEXPECTED_EOF:
        return Status::CompressionError(
            "BZip decompression error: compressed data is corrupted");
      default:
        return Status::CompressionError(
            "BZip decompression error: unknown error code ");
    }
  }

  return Status::Ok();
}

}  // namespace tiledb