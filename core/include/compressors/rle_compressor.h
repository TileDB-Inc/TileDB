/**
 * @file   rle_compressor.h
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
 * This file defines the rle compressor class.
 */

#ifndef TILEDB_RLE_COMPRESSOR_H
#define TILEDB_RLE_COMPRESSOR_H

#include "base_compressor.h"
#include "status.h"

namespace tiledb {

class RLE : public BaseCompressor {
 public:
  /**
   * Returns the maximum size of the output of RLE compression.
   *
   * @param nbytes The input buffer size in bytes.
   * @param type_size The size of a single value type in the input buffer.
   * @return The maximum size of the output after RLE-compressing the input with
   *     size input_size.
   */
  static size_t compress_bound(size_t nbytes, size_t type_size);

  static Status compress(
      size_t type_size,
      int level,
      void* input_buffer,
      size_t input_buffer_size,
      void* output_buffer,
      size_t output_buffer_size,
      size_t* compressed_size);

  static Status decompress(
      size_t type_size,
      void* input_buffer,
      size_t input_buffer_size,
      void* output_buffer,
      size_t output_buffer_size,
      size_t* decompressed_size);

  static int default_level() {
    return -1;
  }
};

}  // namespace tiledb
#endif  // TILEDB_RLE_COMPRESSOR_H
