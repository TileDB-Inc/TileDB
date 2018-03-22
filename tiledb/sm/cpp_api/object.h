/**
 * @file   tiledb_cpp_api_object.h
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
 * This file declares the C++ API for the TileDB Object object.
 */

#ifndef TILEDB_CPP_API_OBJECT_H
#define TILEDB_CPP_API_OBJECT_H

#include "tiledb.h"

#include <functional>
#include <iostream>
#include <string>
#include <type_traits>

namespace tiledb {

class Context;

/** Represents a TileDB object: array, group, key-value, or none (invalid). */
class TILEDB_EXPORT Object {
 public:
  /* ********************************* */
  /*           TYPE DEFINITIONS        */
  /* ********************************* */

  /** The object type. */
  enum class Type { Array, Group, Invalid, KeyValue };

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  explicit Object(const Type& type, const std::string& uri = "");
  explicit Object(tiledb_object_t type, const std::string& uri = "");
  Object();
  Object(const Object& o) = default;
  Object(Object&& o) = default;
  Object& operator=(const Object& o) = default;
  Object& operator=(Object&& o) = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Returns a string representation of the object, including its type
   * and URI.
   */
  std::string to_str() const;

  /** Returns the object type. */
  Type type() const;

  /** Returns the object URI. */
  std::string uri() const;

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
  static Object object(const Context& ctx, const std::string& uri);

  /**
   * Deletes a tiledb object.
   *
   * @param ctx The TileDB context
   * @param uri The path to the object to be deleted.
   */
  static void remove(const Context& ctx, const std::string& uri);

  /**
   * Moves/renames a tiledb object.
   *
   * @param old_uri The path to the old object.
   * @param new_uri The path to the new object.
   */
  static void move(
      const Context& ctx,
      const std::string& old_uri,
      const std::string& new_uri);

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
std::ostream& operator<<(std::ostream& os, const Object& obj);

}  // namespace tiledb

#endif  // TILEDB_CPP_API_OBJECT_H
