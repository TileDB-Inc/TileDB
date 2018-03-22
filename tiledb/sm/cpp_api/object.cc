/**
 * @file   tiledb_cpp_api_object.cc
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
 * This file defines the C++ API for the TileDB Object object.
 */

#include "object.h"
#include "context.h"

namespace tiledb {

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

Object::Object(const Type& type, const std::string& uri)
    : type_(type)
    , uri_(uri) {
}

Object::Object(tiledb_object_t type, const std::string& uri)
    : uri_(uri) {
  switch (type) {
    case TILEDB_ARRAY:
      type_ = Type::Array;
      break;
    case TILEDB_GROUP:
      type_ = Type::Group;
      break;
    case TILEDB_INVALID:
      type_ = Type::Invalid;
      break;
    case TILEDB_KEY_VALUE:
      type_ = Type::KeyValue;
      break;
  }
}

Object::Object()
    : type_(Type::Invalid) {
}

/* ********************************* */
/*               API                 */
/* ********************************* */

std::string Object::to_str() const {
  std::string ret = "Obj<";
  switch (type_) {
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
  ret += " \"" + uri_ + "\">";
  return ret;
}

Object::Type Object::type() const {
  return type_;
}

std::string Object::uri() const {
  return uri_;
}

/* ********************************* */
/*          STATIC FUNCTIONS         */
/* ********************************* */

Object Object::object(const Context& ctx, const std::string& uri) {
  tiledb_object_t type;
  ctx.handle_error(tiledb_object_type(ctx, uri.c_str(), &type));
  Object ret(type, uri);
  return ret;
}

void Object::remove(const Context& ctx, const std::string& uri) {
  ctx.handle_error(tiledb_object_remove(ctx, uri.c_str()));
}

void Object::move(
    const Context& ctx,
    const std::string& old_uri,
    const std::string& new_uri) {
  ctx.handle_error(tiledb_object_move(ctx, old_uri.c_str(), new_uri.c_str()));
}

/* ********************************* */
/*               MISC                */
/* ********************************* */

std::ostream& operator<<(std::ostream& os, const Object& obj) {
  os << obj.to_str();
  return os;
}

}  // namespace tiledb
