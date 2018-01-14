/**
 * @file   tiledb_cpp_api_exception.h
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
 * This file declares exceptions used by the C++ API.
 */
#ifndef TILEDB_TILEDB_CPP_API_EXCEPTION_H
#define TILEDB_TILEDB_CPP_API_EXCEPTION_H

#include "tiledb.h"
#include "tiledb_cpp_api_type.h"

#include <stdexcept>

namespace tdb {

/** Exception indicating a mismatch between a static and runtime type **/
struct TypeError : public std::runtime_error {
  TypeError(const std::string &msg)
      : std::runtime_error(msg) {
  }

  /** Make a type error between static type DataT and expected_type **/
  template <typename DataT>
  static TypeError create(tiledb_datatype_t expected_type) {
    return TypeError(
        "Attempting to use static type " + std::string(DataT::name) +
        " for expected type " + impl::to_str(expected_type));
  }
};

/** Exception indicating the requested operation does not match array schema **/
struct SchemaMismatch : public std::runtime_error {
  SchemaMismatch(const std::string &msg)
      : std::runtime_error(msg) {
  }
};

}  // namespace tdb

#endif  // TILEDB_CPP_API_EXCEPTION_H
