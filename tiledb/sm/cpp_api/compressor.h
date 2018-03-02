/**
 * @file   tiledb_cpp_api_compressor.h
 *
 * @author Ravi Gaddipati
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
 * This file declares the C++ API for the TileDB Compressor object.
 */

#ifndef TILEDB_CPP_API_COMPRESSOR_H
#define TILEDB_CPP_API_COMPRESSOR_H

#include "tiledb.h"

#include <iostream>
#include <string>

namespace tiledb {

/**
 * Represents a compression scheme. Composed of a compression algo + a
 * compression level. A compression level of -1 indicates the default
 * level.
 *
 * **Example:**
 *
 * @code{.cpp}
 * auto a1 = tiledb::Attribute::create<int>(ctx, "a1");
 * a1.set_compressor({TILEDB_BLOSC_LZ, -1});
 * @endcode
 */
class TILEDB_EXPORT Compressor {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  Compressor() = default;
  explicit Compressor(tiledb_compressor_t c)
      : compressor_(c)
      , level_(-1) {
  }

  /**
   * Create a compressor with a given method and level.
   * -1 for default.
   */
  Compressor(tiledb_compressor_t compressor, int level)
      : compressor_(compressor)
      , level_(level) {
  }

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /** Returns the compressor algorithm. */
  tiledb_compressor_t compressor() const {
    return compressor_;
  }

  /** Returns the compression level. */
  int level() const {
    return level_;
  }

  /* ********************************* */
  /*          STATIC FUNCTIONS         */
  /* ********************************* */

  /** Converts the input compressor type to a string. */
  static std::string to_str(tiledb_compressor_t c) {
    switch (c) {
      case TILEDB_NO_COMPRESSION:
        return "NO_COMPRESSION";
      case TILEDB_GZIP:
        return "GZIP";
      case TILEDB_ZSTD:
        return "ZSTD";
      case TILEDB_LZ4:
        return "LZ4";
      case TILEDB_BLOSC_LZ:
        return "BLOSC_LZ";
      case TILEDB_BLOSC_LZ4:
        return "BLOSC_LZ4";
      case TILEDB_BLOSC_LZ4HC:
        return "BLOSC_LZ4HC";
      case TILEDB_BLOSC_SNAPPY:
        return "BLOSC_SNAPPY";
      case TILEDB_BLOSC_ZLIB:
        return "BLOSC_ZLIB";
      case TILEDB_BLOSC_ZSTD:
        return "BLOSC_ZSTD";
      case TILEDB_RLE:
        return "RLE";
      case TILEDB_BZIP2:
        return "BZIP2";
      case TILEDB_DOUBLE_DELTA:
        return "DOUBLE_DELTA";
    }
    return "Invalid";
  }

 private:
  /* ********************************* */
  /*          PRIVATE ATTRIBUTES       */
  /* ********************************* */

  /** The compression algorithm. */
  tiledb_compressor_t compressor_;

  /** The compression level. */
  int level_;
};

/** Writes the object contents to an output stream. */
inline std::ostream& operator<<(std::ostream& os, const Compressor& c) {
  os << '(' << Compressor::to_str(c.compressor()) << ", " << c.level() << ')';
  return os;
}

}  // namespace tiledb

#endif  // TILEDB_CPP_API_COMPRESSOR_H
