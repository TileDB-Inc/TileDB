/**
 * @file   type_conversion.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * This file defines simple type conversion for tests.
 */

#include "tiledb/common/common.h"
#include "tiledb/sm/misc/types.h"

using Datatype = tiledb::sm::Datatype;

#ifndef TILEDB_TYPE_CONVERSION_H
#define TILEDB_TYPE_CONVERSION_H

namespace tiledb {
namespace test {

template <typename T>
struct type_to_tiledb {
  static const Datatype tiledb_type = Datatype::STRING_ASCII;
};

template <>
struct type_to_tiledb<bool> {
  static const Datatype tiledb_type = Datatype::BOOL;
};

template <>
struct type_to_tiledb<int8_t> {
  static const Datatype tiledb_type = Datatype::INT8;
};

template <>
struct type_to_tiledb<uint8_t> {
  static const Datatype tiledb_type = Datatype::UINT8;
};

template <>
struct type_to_tiledb<int16_t> {
  static const Datatype tiledb_type = Datatype::INT16;
};

template <>
struct type_to_tiledb<uint16_t> {
  static const Datatype tiledb_type = Datatype::UINT16;
};

template <>
struct type_to_tiledb<int32_t> {
  static const Datatype tiledb_type = Datatype::INT32;
};

template <>
struct type_to_tiledb<uint32_t> {
  static const Datatype tiledb_type = Datatype::UINT32;
};

template <>
struct type_to_tiledb<int64_t> {
  static const Datatype tiledb_type = Datatype::INT64;
};

template <>
struct type_to_tiledb<uint64_t> {
  static const Datatype tiledb_type = Datatype::UINT64;
};

template <>
struct type_to_tiledb<float> {
  static const Datatype tiledb_type = Datatype::FLOAT32;
};

}  // namespace test
}  // namespace tiledb

#endif  // TILEDB_TYPE_CONVERSION_H