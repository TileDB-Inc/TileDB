/**
 * @file tiledb/type/datatype_traits.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023-2024 TileDB, Inc.
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
 * This file declares `datatype_traits` which provides definitions for
 * generic programming over Datatypes
 */

#ifndef TILEDB_DATATYPE_TRAITS_H
#define TILEDB_DATATYPE_TRAITS_H

#include "tiledb/sm/enums/datatype.h"

using tiledb::sm::Datatype;

namespace tiledb::type {

/**
 * `datatype_traits`
 *
 * This will be specialized for each `Datatype`
 */
template <Datatype type>
struct datatype_traits {};

template <>
struct datatype_traits<Datatype::INT32> {
  using value_type = int32_t;
};

template <>
struct datatype_traits<Datatype::INT64> {
  using value_type = int64_t;
};

template <>
struct datatype_traits<Datatype::INT8> {
  using value_type = int8_t;
};

template <>
struct datatype_traits<Datatype::UINT8> {
  using value_type = uint8_t;
};

template <>
struct datatype_traits<Datatype::INT16> {
  using value_type = int16_t;
};

template <>
struct datatype_traits<Datatype::UINT16> {
  using value_type = uint16_t;
};

template <>
struct datatype_traits<Datatype::UINT32> {
  using value_type = uint32_t;
};

template <>
struct datatype_traits<Datatype::UINT64> {
  using value_type = uint64_t;
};

template <>
struct datatype_traits<Datatype::FLOAT32> {
  using value_type = float;
};

template <>
struct datatype_traits<Datatype::FLOAT64> {
  using value_type = double;
};

struct datatype_datetime_traits {
  using value_type = int64_t;
};

template <>
struct datatype_traits<Datatype::DATETIME_YEAR>
    : private datatype_datetime_traits {
  using datatype_datetime_traits::value_type;
};

template <>
struct datatype_traits<Datatype::DATETIME_MONTH>
    : private datatype_datetime_traits {
  using datatype_datetime_traits::value_type;
};

template <>
struct datatype_traits<Datatype::DATETIME_WEEK>
    : private datatype_datetime_traits {
  using datatype_datetime_traits::value_type;
};

template <>
struct datatype_traits<Datatype::DATETIME_DAY>
    : private datatype_datetime_traits {
  using datatype_datetime_traits::value_type;
};

template <>
struct datatype_traits<Datatype::DATETIME_HR>
    : private datatype_datetime_traits {
  using datatype_datetime_traits::value_type;
};

template <>
struct datatype_traits<Datatype::DATETIME_MIN>
    : private datatype_datetime_traits {
  using datatype_datetime_traits::value_type;
};

template <>
struct datatype_traits<Datatype::DATETIME_SEC>
    : private datatype_datetime_traits {
  using datatype_datetime_traits::value_type;
};

template <>
struct datatype_traits<Datatype::DATETIME_MS>
    : private datatype_datetime_traits {
  using datatype_datetime_traits::value_type;
};

template <>
struct datatype_traits<Datatype::DATETIME_US>
    : private datatype_datetime_traits {
  using datatype_datetime_traits::value_type;
};

template <>
struct datatype_traits<Datatype::DATETIME_NS>
    : private datatype_datetime_traits {
  using datatype_datetime_traits::value_type;
};

template <>
struct datatype_traits<Datatype::DATETIME_PS>
    : private datatype_datetime_traits {
  using datatype_datetime_traits::value_type;
};

template <>
struct datatype_traits<Datatype::DATETIME_FS>
    : private datatype_datetime_traits {
  using datatype_datetime_traits::value_type;
};

template <>
struct datatype_traits<Datatype::DATETIME_AS>
    : private datatype_datetime_traits {
  using datatype_datetime_traits::value_type;
};

template <>
struct datatype_traits<Datatype::TIME_HR> : private datatype_datetime_traits {
  using datatype_datetime_traits::value_type;
};

template <>
struct datatype_traits<Datatype::TIME_MIN> : private datatype_datetime_traits {
  using datatype_datetime_traits::value_type;
};

template <>
struct datatype_traits<Datatype::TIME_SEC> : private datatype_datetime_traits {
  using datatype_datetime_traits::value_type;
};

template <>
struct datatype_traits<Datatype::TIME_MS> : private datatype_datetime_traits {
  using datatype_datetime_traits::value_type;
};

template <>
struct datatype_traits<Datatype::TIME_US> : private datatype_datetime_traits {
  using datatype_datetime_traits::value_type;
};

template <>
struct datatype_traits<Datatype::TIME_NS> : private datatype_datetime_traits {
  using datatype_datetime_traits::value_type;
};

template <>
struct datatype_traits<Datatype::TIME_PS> : private datatype_datetime_traits {
  using datatype_datetime_traits::value_type;
};

template <>
struct datatype_traits<Datatype::TIME_FS> : private datatype_datetime_traits {
  using datatype_datetime_traits::value_type;
};

template <>
struct datatype_traits<Datatype::TIME_AS> : private datatype_datetime_traits {
  using datatype_datetime_traits::value_type;
};

template <>
struct datatype_traits<Datatype::STRING_ASCII> {
  using value_type = char;
};

}  // namespace tiledb::type

#endif
