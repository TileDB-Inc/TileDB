/**
 * @file array_type.h
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
 * This defines the tiledb ArrayType enum that maps to tiledb_array_type_t C-api
 * enum.
 */

#ifndef TILEDB_ARRAY_TYPE_H
#define TILEDB_ARRAY_TYPE_H

#include <cassert>

#include "tiledb/common/status.h"
#include "tiledb/sm/misc/constants.h"

namespace tiledb {
namespace sm {

/** Defines the array type. */
enum class ArrayType : uint8_t {
#define TILEDB_ARRAY_TYPE_ENUM(id) id
#include "tiledb/sm/c_api/tiledb_enum.h"
#undef TILEDB_ARRAY_TYPE_ENUM
};

/** Returns the string representation of the input array type. */
inline const std::string& array_type_str(ArrayType array_type) {
  switch (array_type) {
    case ArrayType::DENSE:
      return constants::dense_str;
    case ArrayType::SPARSE:
      return constants::sparse_str;
    default:
      return constants::empty_str;
  }
}

/** Returns the array type given a string representation. */
inline Status array_type_enum(
    const std::string& array_type_str, ArrayType* array_type_enum) {
  if (array_type_str == constants::dense_str)
    *array_type_enum = ArrayType::DENSE;
  else if (array_type_str == constants::sparse_str)
    *array_type_enum = ArrayType::SPARSE;
  else {
    return Status_Error("Invalid ArrayType " + array_type_str);
  }
  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_ARRAY_TYPE_H
