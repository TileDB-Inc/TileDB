/**
 * @file   gzip_compressor.h
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
 * This file defines the gzip compressor class.
 */

#ifndef TILEDB_GZIP_H
#define TILEDB_GZIP_H

#include "base_compressor.h"
#include "status.h"

namespace tiledb {

class GZip : public BaseCompressor {
 public:
  static size_t compress_bound(size_t nbytes);

  /**
   * GZIPs the input buffer and stores the result in the output buffer,
   * returning the size of compressed data.
   *
   * @param in The input buffer.
   * @param in_size The size of the input buffer.
   * @param out The output buffer.
   * @param out_size The available size in the output buffer.
   * @return Status
   */
  static Status compress(
      size_t type_size,
      void* input_buffer,
      size_t input_buffer_size,
      void* output_buffer,
      size_t output_buffer_size,
      size_t* compressed_size);
  /**
   * Decompresses the GZIPed input buffer and stores the result in the output
   * buffer, of maximum size avail_out.
   *
   * @param in The input buffer.
   * @param in_size The size of the input buffer.
   * @param out The output buffer.
   * @param avail_out_size The available size in the output buffer.
   * @param out_size The size of the decompressed data.
   * @return Status
   */
  static Status decompress(
      size_t type_size,
      void* input_buffer,
      size_t input_buffer_size,
      void* output_buffer,
      size_t output_buffer_size,
      size_t* decompressed_size);
};

};  // namespace tiledb

#endif  // TILEDB_GZIP_H
