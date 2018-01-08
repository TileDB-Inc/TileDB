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

#include <functional>
#include <iostream>
#include <string>
#include <type_traits>

namespace tdb {

class Context;

/**
 * Represents a tiledb object: an array, group, keyvalue, or none (invalid)
 */
struct Object {
  enum class Type { Array, Group, Invalid, KeyValue };

  Type type = Type::Invalid;
  std::string uri;

  Object() = default;
  Object(const Object &o) = default;
  Object(Object &&o) = default;
  Object(const Type &type, std::string uri = "")
      : type(type)
      , uri(uri) {
  }
  Object &operator=(const Object &o) = default;
  Object &operator=(Object &&o) = default;

  void set(const tiledb_object_t t);

  std::string to_str() const;
};

/**
 * Represents a compression scheme. Composed of a compression algo + a
 * compression level.
 */
struct Compressor {
  Compressor() = default;
  Compressor(tiledb_compressor_t c)
      : compressor(c)
      , level(-1) {
  }
  Compressor(tiledb_compressor_t compressor, int level)
      : compressor(compressor)
      , level(level) {
  }
  tiledb_compressor_t compressor;
  int level;
};

/**
 * Deleter for various tiledb types. Useful for sharedptr.
 *
 * @code{.cpp}
 * tdb::Context ctx;
 * _Deleter _deleter(ctx);
 * std::shared_ptr<tiledb_type_t> p = std::shared_ptr<tiledb_type_t>(ptr,
 * _deleter);
 * @endcode
 */
struct _Deleter {
  _Deleter(Context &ctx)
      : _ctx(ctx) {
  }
  _Deleter(const _Deleter &) = default;
  void operator()(tiledb_query_t *p);
  void operator()(tiledb_array_schema_t *p);
  void operator()(tiledb_attribute_t *p);
  void operator()(tiledb_dimension_t *p);
  void operator()(tiledb_domain_t *p);

 private:
  std::reference_wrapper<Context> _ctx;
};

std::string from_tiledb(const tiledb_layout_t &layout);
std::string from_tiledb(const tiledb_array_type_t &type);
std::string from_tiledb(const tiledb_query_type_t &qtype);
std::string from_tiledb(const tiledb_compressor_t &c);

std::ostream &operator<<(std::ostream &os, const Object &obj);
std::ostream &operator<<(std::ostream &os, const Compressor &c);
}  // namespace tdb

#endif  // TILEDB_GENOMICS_OBJECT_H
