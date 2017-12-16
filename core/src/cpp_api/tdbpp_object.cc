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

#include "tdbpp_object.h"
#include "tdbpp_context.h"

std::ostream &tdb::operator<<(std::ostream &os, const tdb::Object &obj) {
  os << obj.to_str();
  return os;
}

std::string tdb::Object::to_str() const {
  std::string ret = "Obj<";
  switch (type) {
    case Type::Array:
      ret += "ARRAY";
      break;
    case Type::Group:
      ret += "GROUP";
      break;
    case Type::Invalid:
      ret += "INVALID";
      break;
    case Type::KeyValue:
      ret += "KEYVALUE";
      break;
  }
  ret += " \"" + uri + "\"";
  return ret;
}

void tdb::Object::set(const tiledb_object_t t) {
  switch (t) {
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

void tdb::_Deleter::operator()(tiledb_query_t *p) {
  _ctx.get().handle_error(tiledb_query_free(_ctx.get(), p));
}

void tdb::_Deleter::operator()(tiledb_array_schema_t *p) {
  _ctx.get().handle_error(tiledb_array_schema_free(_ctx.get(), p));
}

void tdb::_Deleter::operator()(tiledb_attribute_t *p) {
  _ctx.get().handle_error(tiledb_attribute_free(_ctx.get(), p));
}

void tdb::_Deleter::operator()(tiledb_dimension_t *p) {
  _ctx.get().handle_error(tiledb_dimension_free(_ctx.get(), p));
}

void tdb::_Deleter::operator()(tiledb_domain_t *p) {
  _ctx.get().handle_error(tiledb_domain_free(_ctx.get(), p));
}

std::string tdb::from_tiledb(const tiledb_layout_t &layout) {
  switch (layout) {
    case TILEDB_GLOBAL_ORDER:
      return "GLOBAL";
    case TILEDB_ROW_MAJOR:
      return "ROW-MAJOR";
    case TILEDB_COL_MAJOR:
      return "COL-MAJOR";
    case TILEDB_UNORDERED:
      return "UNORDERED";
  }
  return "";
}

std::string tdb::from_tiledb(const tiledb_array_type_t &type) {
  return type == TILEDB_DENSE ? "DENSE" : "SPARSE";
}

std::string tdb::from_tiledb(const tiledb_query_type_t &qtype) {
  switch (qtype) {
    case TILEDB_READ:
      return "READ";
    case TILEDB_WRITE:
      return "WRITE";
  }
  return "";  // silence error
}

std::string tdb::from_tiledb(const tiledb_compressor_t &c) {
  switch (c) {
    case TILEDB_NO_COMPRESSION:
      return "NO_COMPRESSION";
    case TILEDB_GZIP:
      return "GZIP";
    case TILEDB_ZSTD:
      return "ZSTD";
    case TILEDB_LZ4:
      return "LZ4";
    case TILEDB_BLOSC:
      return "BLOSC";
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

std::ostream & ::tdb::operator<<(std::ostream &os, const tdb::Compressor &c) {
  os << '(' << from_tiledb(c.compressor) << ", " << c.level << ')';
  return os;
}
