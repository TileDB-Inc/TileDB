/**
 * @file datatype.h
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
 * This defines the tiledb Datatype enum that maps to tiledb_datatype_t C-api
 * enum.
 */

#ifndef TILEDB_DATATYPE_H
#define TILEDB_DATATYPE_H

#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/status.h"

#include <cassert>

namespace tiledb {
namespace sm {

/** Defines a datatype. */
enum class Datatype : uint8_t {
#define TILEDB_DATATYPE_ENUM(id) id
#include "tiledb/sm/c_api/tiledb_enum.h"
#undef TILEDB_DATATYPE_ENUM
};

/** Returns the datatype size. */
inline uint64_t datatype_size(Datatype type) {
  switch (type) {
    case Datatype::INT32:
      return sizeof(int);
    case Datatype::INT64:
      return sizeof(int64_t);
    case Datatype::FLOAT32:
      return sizeof(float);
    case Datatype::FLOAT64:
      return sizeof(double);
    case Datatype::CHAR:
      return sizeof(char);
    case Datatype::INT8:
      return sizeof(int8_t);
    case Datatype::UINT8:
      return sizeof(uint8_t);
    case Datatype::INT16:
      return sizeof(int16_t);
    case Datatype::UINT16:
      return sizeof(uint16_t);
    case Datatype::UINT32:
      return sizeof(uint32_t);
    case Datatype::UINT64:
      return sizeof(uint64_t);
    case Datatype::STRING_ASCII:
      return sizeof(uint8_t);
    case Datatype::STRING_UTF8:
      return sizeof(uint8_t);
    case Datatype::STRING_UTF16:
      return sizeof(uint16_t);
    case Datatype::STRING_UTF32:
      return sizeof(uint32_t);
    case Datatype::STRING_UCS2:
      return sizeof(uint16_t);
    case Datatype::STRING_UCS4:
      return sizeof(uint32_t);
    case Datatype::ANY:
      return sizeof(uint8_t);
  }

  assert(false);
  return 0;
}

/** Returns the string representation of the input datatype. */
inline const std::string& datatype_str(Datatype type) {
  switch (type) {
    case Datatype::INT32:
      return constants::int32_str;
    case Datatype::INT64:
      return constants::int64_str;
    case Datatype::FLOAT32:
      return constants::float32_str;
    case Datatype::FLOAT64:
      return constants::float64_str;
    case Datatype::CHAR:
      return constants::char_str;
    case Datatype::INT8:
      return constants::int8_str;
    case Datatype::UINT8:
      return constants::uint8_str;
    case Datatype::INT16:
      return constants::int16_str;
    case Datatype::UINT16:
      return constants::uint16_str;
    case Datatype::UINT32:
      return constants::uint32_str;
    case Datatype::UINT64:
      return constants::uint64_str;
    case Datatype::STRING_ASCII:
      return constants::string_ascii_str;
    case Datatype::STRING_UTF8:
      return constants::string_utf8_str;
    case Datatype::STRING_UTF16:
      return constants::string_utf16_str;
    case Datatype::STRING_UTF32:
      return constants::string_utf32_str;
    case Datatype::STRING_UCS2:
      return constants::string_ucs2_str;
    case Datatype::STRING_UCS4:
      return constants::string_ucs4_str;
    case Datatype::ANY:
      return constants::any_str;
    default:
      assert(0);
      return constants::empty_str;
  }
}

/** Returns the datatype given a string representation. */
inline Status datatype_enum(
    const std::string& datatype_str, Datatype* datatype) {
  if (datatype_str == constants::int32_str)
    *datatype = Datatype::INT32;
  else if (datatype_str == constants::int64_str)
    *datatype = Datatype::INT64;
  else if (datatype_str == constants::float32_str)
    *datatype = Datatype::FLOAT32;
  else if (datatype_str == constants::float64_str)
    *datatype = Datatype::FLOAT64;
  else if (datatype_str == constants::char_str)
    *datatype = Datatype::CHAR;
  else if (datatype_str == constants::int8_str)
    *datatype = Datatype::INT8;
  else if (datatype_str == constants::uint8_str)
    *datatype = Datatype::UINT8;
  else if (datatype_str == constants::int16_str)
    *datatype = Datatype::INT16;
  else if (datatype_str == constants::uint16_str)
    *datatype = Datatype::UINT16;
  else if (datatype_str == constants::uint32_str)
    *datatype = Datatype::UINT32;
  else if (datatype_str == constants::uint64_str)
    *datatype = Datatype::UINT64;
  else if (datatype_str == constants::string_ascii_str)
    *datatype = Datatype::STRING_ASCII;
  else if (datatype_str == constants::string_utf8_str)
    *datatype = Datatype::STRING_UTF8;
  else if (datatype_str == constants::string_utf16_str)
    *datatype = Datatype::STRING_UTF16;
  else if (datatype_str == constants::string_utf32_str)
    *datatype = Datatype::STRING_UTF32;
  else if (datatype_str == constants::string_ucs2_str)
    *datatype = Datatype::STRING_UCS2;
  else if (datatype_str == constants::string_ucs4_str)
    *datatype = Datatype::STRING_UCS4;
  else if (datatype_str == constants::any_str)
    *datatype = Datatype::ANY;
  else {
    return Status::Error("Invalid Datatype " + datatype_str);
  }
  return Status::Ok();
}

/** Returns true if the input datatype is a string type. */
inline bool datatype_is_string(Datatype type) {
  return (
      type == Datatype::STRING_ASCII || type == Datatype::STRING_UTF8 ||
      type == Datatype::STRING_UTF16 || type == Datatype::STRING_UTF32 ||
      type == Datatype::STRING_UCS2 || type == Datatype::STRING_UCS4);
}

/** Returns true if the input datatype is an integer type. */
inline bool datatype_is_integer(Datatype type) {
  return (
      type == Datatype::INT8 || type == Datatype::UINT8 ||
      type == Datatype::INT16 || type == Datatype::UINT16 ||
      type == Datatype::INT32 || type == Datatype::UINT32 ||
      type == Datatype::INT64 || type == Datatype::UINT64);
}

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_DATATYPE_H
