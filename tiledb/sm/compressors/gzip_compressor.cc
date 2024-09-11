/**
 * @file   gzip_compressor.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * This file implements the gzip compressor class.
 */

#include "tiledb/sm/compressors/gzip_compressor.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/buffer/buffer.h"

#include <zlib.h>
#include <cmath>
#include <iostream>

using namespace tiledb::common;

namespace tiledb::sm {

class GZipException : public StatusException {
 public:
  explicit GZipException(const std::string& message)
      : StatusException("GZipException", message) {
  }
};

void GZip::compress(
    int level, ConstBuffer* input_buffer, Buffer* output_buffer) {
  // Sanity check
  if (input_buffer->data() == nullptr || output_buffer->data() == nullptr)
    throw GZipException("Failed compressing with GZip; invalid buffer format");

  if (level > GZip::maximum_level())
    throw GZipException(
        "Failed compressing with GZip; invalid compression level.");

  int ret;
  z_stream strm;

  // Allocate deflate state
  strm.zalloc = Z_NULL;
  strm.zfree = Z_NULL;
  strm.opaque = Z_NULL;
  ret =
      deflateInit(&strm, level < level_limit_ ? GZip::default_level() : level);

  if (ret != Z_OK) {
    if ((ret != Z_MEM_ERROR && ret != Z_STREAM_ERROR)) {
      (void)deflateEnd(&strm);
    }
    throw GZipException("Cannot compress with GZIP");
  }

  // Compress
  strm.next_in = (unsigned char*)input_buffer->data();
  strm.next_out = (unsigned char*)output_buffer->cur_data();
  strm.avail_in = (uInt)input_buffer->size();
  strm.avail_out = (uInt)output_buffer->free_space();
  ret = deflate(&strm, Z_FINISH);

  // Clean up
  (void)deflateEnd(&strm);

  // Return
  if (ret != Z_STREAM_END || strm.avail_in != 0) {
    if (ret == Z_OK) {
      throw GZipException("Cannot compress with GZIP; output buffer too small");
    }
    throw GZipException(
        "Cannot compress with GZIP; error code " + std::to_string(ret));
  }

  // Set size of compressed data
  uint64_t compressed_size = output_buffer->free_space() - strm.avail_out;
  output_buffer->advance_size(compressed_size);
  output_buffer->advance_offset(compressed_size);
}

void GZip::compress(ConstBuffer* input_buffer, Buffer* output_buffer) {
  return GZip::compress(GZip::default_level(), input_buffer, output_buffer);
}

void GZip::decompress(
    ConstBuffer* input_buffer, PreallocatedBuffer* output_buffer) {
  // Sanity check
  if (input_buffer->data() == nullptr || output_buffer->data() == nullptr)
    throw GZipException(
        "Failed decompressing with GZip; invalid buffer format");

  int ret;
  z_stream strm;

  // Allocate deflate state
  strm.zalloc = Z_NULL;
  strm.zfree = Z_NULL;
  strm.opaque = Z_NULL;
  strm.avail_in = 0;
  strm.next_in = Z_NULL;
  ret = inflateInit(&strm);

  if (ret != Z_OK) {
    throw GZipException("Cannot decompress with GZIP");
  }

  // Decompress
  strm.next_in = (unsigned char*)input_buffer->data();
  strm.next_out = (unsigned char*)output_buffer->cur_data();
  strm.avail_in = (uInt)input_buffer->size();
  strm.avail_out = (uInt)output_buffer->free_space();
  ret = inflate(&strm, Z_FINISH);

  if (ret != Z_STREAM_END) {
    throw GZipException("Cannot decompress with GZIP, Stream Error");
  }

  // Set size of decompressed data
  uint64_t compressed_size = output_buffer->free_space() - strm.avail_out;
  output_buffer->advance_offset(compressed_size);

  // Clean up
  (void)inflateEnd(&strm);
}

uint64_t GZip::overhead(uint64_t buffer_size) {
  // The zlib encoding adds 6 bytes (we don't use compression dictionary)
  // and the overhead of deflate was taken from
  // https://stackoverflow.com/a/23578269.
  return 6 + 5 * uint64_t(std::floor(buffer_size / 16834.0) + 1);
}

}  // namespace tiledb::sm
