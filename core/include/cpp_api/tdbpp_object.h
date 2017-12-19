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

namespace tdb {

  struct Object {
    enum class Type {Array, Group, Invalid, KeyValue};

    Type type = Type::Invalid;
    std::string uri;

    Object() = default;
    Object(const Object &o) = default;
    Object(Object &&o) = default;
    Object(const Type &type, std::string uri="") : type(type), uri(std::move(uri)) {}
    Object &operator=(const Object &o) = default;
    Object &operator=(Object &&o) = default;

    void set(const tiledb_object_t t) {
      switch(t) {
        case TILEDB_ARRAY:
          type = Type::Array;
          break;
        case TILEDB_GROUP:
          type = Type::Group;
          break;
        case TILEDB_INVALID:
          type = Type::Invalid;
          break;
        case TILEDB_KEY_VALUE:
          type = Type::KeyValue;
          break;
      }
    }

    std::string to_str() const;
  };

  struct Compressor {
    Compressor() = default;
    Compressor(tiledb_compressor_t compressor, int level) : compressor(compressor), level(level) {}
    tiledb_compressor_t compressor;
    int level;
  };

}

std::ostream &operator<<(std::ostream &os, const tdb::Object &obj);
std::ostream &operator<<(std::ostream &os, const tdb::Compressor &obj);


#endif //TILEDB_GENOMICS_OBJECT_H
