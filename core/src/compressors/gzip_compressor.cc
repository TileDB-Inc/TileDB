/**
 * @file   gzip_compressor.cc
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
 * This file implements the gzip compressor class.
 */

#include <zlib.h>

#include "gzip_compressor.h"
#include "logger.h"

namespace tiledb {

Status GZip::compress(
    size_t type_size,
    void* input_buffer,
    size_t input_buffer_size,
    void* output_buffer,
    size_t output_buffer_size,
    size_t* compressed_size) {
  ssize_t ret;
  z_stream strm;

  // Allocate deflate state
  strm.zalloc = Z_NULL;
  strm.zfree = Z_NULL;
  strm.opaque = Z_NULL;
  ret = deflateInit(
      &strm, Z_DEFAULT_COMPRESSION);  // TODO: this should be part of the schema

  if (ret != Z_OK) {
    (void)deflateEnd(&strm);
    return LOG_STATUS(Status::GZipError("Cannot compress with GZIP"));
  }

  // Compress
  strm.next_in = static_cast<unsigned char*>(input_buffer);
  strm.next_out = static_cast<unsigned char*>(output_buffer);
  strm.avail_in = input_buffer_size;
  strm.avail_out = output_buffer_size;
  ret = deflate(&strm, Z_FINISH);

  // Clean up
  (void)deflateEnd(&strm);

  // Return
  if (ret == Z_STREAM_ERROR || strm.avail_in != 0) {
    return LOG_STATUS(Status::GZipError("Cannot compress with GZIP"));
  };
  // Return size of compressed data
  *compressed_size = output_buffer_size - strm.avail_out;
  return Status::Ok();
}

Status GZip::decompress(
    size_t type_size,
    void* input_buffer,
    size_t input_buffer_size,
    void* output_buffer,
    size_t output_buffer_size,
    size_t* decompressed_size) {
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
    return LOG_STATUS(Status::GZipError("Cannot decompress with GZIP"));
  }

  // Decompress
  strm.next_in = static_cast<unsigned char*>(input_buffer);
  strm.next_out = static_cast<unsigned char*>(output_buffer);
  strm.avail_in = input_buffer_size;
  strm.avail_out = output_buffer_size;
  ret = inflate(&strm, Z_FINISH);

  if (ret == Z_STREAM_ERROR || ret != Z_STREAM_END) {
    return LOG_STATUS(
        Status::GZipError("Cannot decompress with GZIP, Stream Error"));
  }

  // Clean up
  (void)inflateEnd(&strm);

  // Calculate size of compressed data
  *decompressed_size = output_buffer_size - strm.avail_out;

  // Success
  return Status::Ok();
}

};  // namespace tiledb
