/**
 * @file datatype.h
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
 * This defines the tiledb Datatype enum that maps to tiledb_datatype_t C-api
 * enum.
 */

#ifndef TILEDB_DATATYPE_H
#define TILEDB_DATATYPE_H

#include "constants.h"

namespace tiledb {

/** Defines a datatype. */
enum class Datatype : char {
#define TILEDB_DATATYPE_ENUM(id) id
#include "tiledb_enum.inc"
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
    default:
      return 0;
  }
}

/** Returns the string representation of the input datatype. */
inline const char* datatype_str(Datatype type) {
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
    default:
      return "";
  }
}

}  // namespace tiledb

#endif  // TILEDB_DATATYPE_H
