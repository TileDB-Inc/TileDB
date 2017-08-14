/**
 * @file   blosc_compressor.h
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
 * This file defines the blosc compressor class.
 */

#ifndef TILEDB_BLOSC_COMPRESSOR_H
#define TILEDB_BLOSC_COMPRESSOR_H

#include "buffer.h"
#include "status.h"

namespace tiledb {

/** Handles compression/decompression with the blosc library. */
class Blosc {
 public:
  /** Type of compressor used by Blosc. */
  enum class Compressor : char {
    LZ,
    LZ4,
    LZ4HC,
    SNAPPY,
    Zlib,
    ZStd,
  };

  /** Returns the maximum compression size for the given input. */
  static uint64_t compress_bound(uint64_t nbytes);

  /**
   * Compression function.
   *
   * @param compressor Type of Blosc compressor.
   * @param type_size The size of the data type.
   * @param level The compression level.
   * @param input_buffer Input buffer to read from.
   * @param output_buffer Output buffer to write to the compressed data.
   * @return Status
   */
  static Status compress(
      const char* compressor,
      uint64_t type_size,
      int level,
      const Buffer* input_buffer,
      Buffer* output_buffer);

  /**
   * Decompression function.
   *
   * @param input_buffer Input buffer to read from.
   * @param output_buffer Output buffer to write to the decompressed data.
   * @return Status
   */
  static Status decompress(const Buffer* input_buffer, Buffer* output_buffer);

  static int default_level() {
    return 5;
  }
};

}  // namespace tiledb

#endif  // TILEDB_BLOSC_COMPRESSOR_H
