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

std::ostream &operator<<(std::ostream &os, const tdb::Object &obj) {
  os << obj.to_str();
  return os;
}

std::string tdb::Object::to_str() const {
  std::string ret = "Obj<";
  switch(type) {
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
  ret += " " + uri + '>';
  return ret;
}

void tdb::Object::set(const tiledb_object_t t) {
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
