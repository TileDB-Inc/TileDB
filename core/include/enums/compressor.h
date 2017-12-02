/**
 * @file compressor.h
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
 * This defines the tiledb CompressorType enum that maps to tiledb_compressor_t
 * C-api enum.
 */

#ifndef TILEDB_COMPRESSOR_H
#define TILEDB_COMPRESSOR_H

#include "constants.h"

namespace tiledb {

/** Defines the compressor type. */
enum class Compressor : char {
#define TILEDB_COMPRESSOR_ENUM(id) id
#include "tiledb_enum.inc"
#undef TILEDB_COMPRESSOR_ENUM
};

/** Returns the string representation of the input compressor. */
inline const char* compressor_str(Compressor type) {
  switch (type) {
    case Compressor::NO_COMPRESSION:
      return constants::no_compression_str;
    case Compressor::GZIP:
      return constants::gzip_str;
    case Compressor::ZSTD:
      return constants::zstd_str;
    case Compressor::LZ4:
      return constants::lz4_str;
    case Compressor::BLOSC:
      return constants::blosc_str;
    case Compressor::BLOSC_LZ4:
      return constants::blosc_lz4_str;
    case Compressor::BLOSC_LZ4HC:
      return constants::blosc_lz4hc_str;
    case Compressor::BLOSC_SNAPPY:
      return constants::blosc_snappy_str;
    case Compressor::BLOSC_ZLIB:
      return constants::blosc_zlib_str;
    case Compressor::BLOSC_ZSTD:
      return constants::blosc_zstd_str;
    case Compressor::RLE:
      return constants::rle_str;
    case Compressor::BZIP2:
      return constants::bzip2_str;
    case Compressor::DOUBLE_DELTA:
      return constants::double_delta_str;
    default:
      return "";
  }
}

}  // namespace tiledb

#endif  // TILEDB_COMPRESSOR_H
