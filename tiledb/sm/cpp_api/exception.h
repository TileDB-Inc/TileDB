/**
 * @file   exception.h
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
 * This file declares exceptions used by the C++ API.
 */
#ifndef TILEDB_TILEDB_CPP_API_EXCEPTION_H
#define TILEDB_TILEDB_CPP_API_EXCEPTION_H

#include "core_interface.h"
#include "tiledb.h"
#include "type.h"

#include <stdexcept>

namespace tiledb {

/** Exception indicating a TileDB error. */
struct TileDBError : std::runtime_error {
  TileDBError(const std::string& msg)
      : std::runtime_error(msg) {
  }
};

/** Exception indicating a mismatch between a static and runtime type **/
struct TypeError : public TileDBError {
  TypeError(const std::string& msg)
      : TileDBError(msg) {
  }
};

/** Exception indicating the requested operation does not match array schema **/
struct SchemaMismatch : public TileDBError {
  SchemaMismatch(const std::string& msg)
      : TileDBError(msg) {
  }
};

/** Error related to attributes **/
struct AttributeError : public TileDBError {
  AttributeError(const std::string& msg)
      : TileDBError(msg) {
  }
};

namespace impl {

// 'identity' used only to remove warning C4127 for MSVC compilers
template <typename T>
const T& identity(const T& t) {
  return t;
}

/** Checks if the input type complies with the template type. */
template <typename T, typename Handler = TypeHandler<T>>
inline void type_check(tiledb_datatype_t type, unsigned num = 0) {
  if (tiledb_string_type(type)) {
    if (Handler::tiledb_type != identity(TILEDB_CHAR)) {
      throw TypeError(
          "Static type (" + impl::type_to_str(Handler::tiledb_type) +
          ") does not match expected container type CHAR for tiledb type (" +
          impl::type_to_str(type) + ")");
    }
  } else if (tiledb_datetime_type(type)) {
    if (!std::is_same<T, int64_t>::value) {
      throw TypeError(
          "Static type does not match expected container type int64_t for "
          "tiledb datetime type");
    }
  } else if (tiledb_time_type(type)) {
    if (!std::is_same<T, int64_t>::value) {
      throw TypeError(
          "Static type does not match expected container type int64_t for "
          "tiledb time type");
    }
  } else if (Handler::tiledb_type != type) {
    throw TypeError(
        "Static type (" + impl::type_to_str(Handler::tiledb_type) +
        ") does not match expected type (" + impl::type_to_str(type) + ")");
  }
  if (num != 0 && num != TILEDB_VAR_NUM &&
      Handler::tiledb_num != TILEDB_VAR_NUM && Handler::tiledb_num != num) {
    throw TypeError(
        "Expected num of " + std::to_string(num) + ", static type has num of " +
        std::to_string(Handler::tiledb_num));
  }
}

}  // namespace impl

}  // namespace tiledb

#endif  // TILEDB_CPP_API_EXCEPTION_H
