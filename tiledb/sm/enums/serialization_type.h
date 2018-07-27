/**
 * @file serialization_type.h
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
 * This defines the TileDB SerializationType enum that maps to
 * tiledb_serialization_type_t C-API enum.
 */

#ifndef TILEDB_SERIALIZATION_TYPE_H
#define TILEDB_SERIALIZATION_TYPE_H

#include <cassert>
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/status.h"

namespace tiledb {
namespace sm {

/** Defines the query type. */
enum class SerializationType : char {
#define TILEDB_SERIALIZATION_TYPE_ENUM(id) id
#include "tiledb/sm/c_api/tiledb_enum.h"
#undef TILEDB_SERIALIZATION_TYPE_ENUM
};

/** Returns the string representation of the input query type. */
inline const std::string& serialization_type_str(
    SerializationType serialization_type) {
  switch (serialization_type) {
    case SerializationType::JSON:
      return constants::serialization_type_json_str;
    case SerializationType::CAPNP:
      return constants::serialization_type_capnp_str;
    default:
      assert(0);
      return constants::empty_str;
  }
}

/** Returns the query type given a string representation. */
inline Status serialization_type_enum(
    const std::string& serialization_type_str,
    SerializationType* serialization_type) {
  if (serialization_type_str == constants::serialization_type_json_str)
    *serialization_type = SerializationType::JSON;
  else if (serialization_type_str == constants::serialization_type_capnp_str)
    *serialization_type = SerializationType::CAPNP;
  else {
    return Status::Error("Invalid SerializationType " + serialization_type_str);
  }
  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_SERIALIZATION_TYPE_H
