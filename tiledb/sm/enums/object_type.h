/**
 * @file object_type.h
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
 * This file defines the tiledb ObjectType enum that maps to the
 * tiledb_object_t C-api enum
 */

#ifndef TILEDB_OBJECT_TYPE_H
#define TILEDB_OBJECT_TYPE_H

#include "tiledb/common/status.h"
#include "tiledb/sm/misc/constants.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

enum class ObjectType : uint8_t {
#define TILEDB_OBJECT_TYPE_ENUM(id) id
#include "tiledb/api/c_api/object/object_api_enum.h"
#undef TILEDB_OBJECT_TYPE_ENUM
};

/** Returns the string representation of the input object type. */
inline const std::string& object_type_str(ObjectType object_type) {
  switch (object_type) {
    case ObjectType::INVALID:
      return constants::object_type_invalid_str;
    case ObjectType::GROUP:
      return constants::object_type_group_str;
    case ObjectType::ARRAY:
      return constants::object_type_array_str;
    default:
      return constants::empty_str;
  }
}

/** Returns the object type given a string representation. */
inline Status object_type_enum(
    const std::string& object_type_str, ObjectType* object_type) {
  if (object_type_str == constants::object_type_invalid_str)
    *object_type = ObjectType::INVALID;
  else if (object_type_str == constants::object_type_group_str)
    *object_type = ObjectType::GROUP;
  else if (object_type_str == constants::object_type_array_str)
    *object_type = ObjectType::ARRAY;
  else
    return Status_Error("Invalid ObjectType " + object_type_str);

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_OBJECT_TYPE_H
