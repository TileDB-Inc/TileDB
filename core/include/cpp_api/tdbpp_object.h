/**
 * @file   tiledb.h
 *
 * @author Ravi Gaddipati
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
 * This file declares the C++ API for TileDB.
 */

#ifndef TILEDB_GENOMICS_OBJECT_H
#define TILEDB_GENOMICS_OBJECT_H

#include "tiledb.h"
#include <iostream>
#include <type_traits>
#include <string>

namespace tdb {

  /**
   * Represents a tiledb object: an array, group, keyvalue, or none (invalid)
   */
  struct Object {
    enum class Type {Array, Group, Invalid, KeyValue};

    Type type = Type::Invalid;
    std::string uri;

    Object() = default;
    Object(const Object &o) = default;
    Object(Object &&o) = default;
    Object(const Type &type, std::string uri="") : type(type), uri(uri) {}
    Object &operator=(const Object &o) = default;
    Object &operator=(Object &&o) = default;

    void set(const tiledb_object_t t);

    std::string to_str() const;
  };

  /**
   * Represents a compression scheme. Composed of a compression algo + a compression level.
   */
  struct Compressor {
    Compressor() = default;
    Compressor(tiledb_compressor_t c) : compressor(c), level(-1) {}
    Compressor(tiledb_compressor_t compressor, int level) : compressor(compressor), level(level) {}
    tiledb_compressor_t compressor;
    int level;
  };

  std::string from_tiledb(const tiledb_layout_t &layout);
  std::string from_tiledb(const tiledb_array_type_t &type);

}

std::ostream &operator<<(std::ostream &os, const tdb::Object &obj);
std::ostream &operator<<(std::ostream &os, const tdb::Compressor &obj);


#endif //TILEDB_GENOMICS_OBJECT_H
