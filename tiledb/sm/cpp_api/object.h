/**
 * @file   object.h
 *
 * @author Ravi Gaddipati
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * This file declares the C++ API for the TileDB Object object.
 */

#ifndef TILEDB_CPP_API_OBJECT_H
#define TILEDB_CPP_API_OBJECT_H

#include "context.h"
#include "object.h"
#include "tiledb.h"

#include <functional>
#include <iostream>
#include <string>
#include <type_traits>

namespace tiledb {

/**
 * Represents a TileDB object: array, group, key-value (map), or none
 * (invalid).
 */
class Object {
 public:
  /* ********************************* */
  /*           TYPE DEFINITIONS        */
  /* ********************************* */

  /** The object type. */
  enum class Type {
    /** TileDB array object. */
    Array,
    /** TileDB group object. */
    Group,
    /** Invalid or unknown object type. */
    Invalid
  };

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  explicit Object(const Type& type, const std::string& uri = "")
      : type_(type)
      , uri_(uri) {
  }

  explicit Object(tiledb_object_t type, const std::string& uri = "")
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
    }
  }

  Object();
  Object(const Object&) = default;
  Object(Object&&) = default;
  Object& operator=(const Object&) = default;
  Object& operator=(Object&&) = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Returns a string representation of the object, including its type
   * and URI.
   */
  std::string to_str() const {
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
    }
    ret += " \"" + uri_ + "\">";
    return ret;
  }

  /** Returns the object type. */
  Type type() const {
    return type_;
  }

  /** Returns the object URI. */
  std::string uri() const {
    return uri_;
  }

  /* ********************************* */
  /*          STATIC FUNCTIONS         */
  /* ********************************* */

  /**
   * Gets an Object object that encapsulates the object type of the given path.
   *
   * @param ctx The TileDB context
   * @param uri The path to the object.
   * @return An object that contains the type along with the URI.
   */
  static Object object(const Context& ctx, const std::string& uri) {
    tiledb_object_t type;
    ctx.handle_error(tiledb_object_type(ctx.ptr().get(), uri.c_str(), &type));
    Object ret(type, uri);
    return ret;
  }

  /**
   * Deletes a TileDB object at the given URI from disk/persistent storage.
   *
   * @param ctx The TileDB context
   * @param uri The path to the object to be removed.
   */
  static void remove(const Context& ctx, const std::string& uri) {
    ctx.handle_error(tiledb_object_remove(ctx.ptr().get(), uri.c_str()));
  }

  /**
   * Moves/renames a TileDB object.
   *
   * @param old_uri The path to the old object.
   * @param new_uri The path to the new object.
   */
  static void move(
      const Context& ctx,
      const std::string& old_uri,
      const std::string& new_uri) {
    ctx.handle_error(
        tiledb_object_move(ctx.ptr().get(), old_uri.c_str(), new_uri.c_str()));
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The object type. */
  Type type_;

  /** The obkect uri. */
  std::string uri_;
};

/* ********************************* */
/*               MISC                */
/* ********************************* */

/** Writes object in string format to an output stream. */
inline std::ostream& operator<<(std::ostream& os, const Object& obj) {
  os << obj.to_str();
  return os;
}

}  // namespace tiledb

#endif  // TILEDB_CPP_API_OBJECT_H
