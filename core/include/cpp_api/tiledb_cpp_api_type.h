/**
 * @file   tiledb_cpp_api_type.h
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
 * This defines TileDB datatypes for the C++ API.
 */

#ifndef TILEDB_CPP_API_TYPE_H
#define TILEDB_CPP_API_TYPE_H

#include <cstdint>
#include <string>
#include <vector>
#include "tiledb.h"

namespace tdb {
namespace type {
constexpr char char_repr[] = "CHAR";
constexpr char int8_repr[] = "INT8";
constexpr char uint8_repr[] = "UINT8";
constexpr char int16_repr[] = "INT16";
constexpr char uint16_repr[] = "UINT16";
constexpr char int32_repr[] = "INT32";
constexpr char uint32_repr[] = "UINT32";
constexpr char int64_repr[] = "INT64";
constexpr char uint64_repr[] = "UINT64";
constexpr char float32_repr[] = "FLOAT32";
constexpr char float64_repr[] = "FLOAT64";

/**
 * Repr of a datatype, tiledb datatype, and name.
 *
 * @tparam T underlying type
 * @tparam TDB_TYPE tiledb_data_type repr
 * @tparam NAME string repr
 */
template <typename T, tiledb_datatype_t TDB_TYPE, const char *NAME>
struct Type {
  Type() = delete;
  using type = T;
  static constexpr tiledb_datatype_t tiledb_datatype = TDB_TYPE;
  static constexpr const char *name = NAME;
};

using CHAR = Type<char, TILEDB_CHAR, char_repr>;
using INT8 = Type<int8_t, TILEDB_INT8, int8_repr>;
using UINT8 = Type<uint8_t, TILEDB_UINT8, uint8_repr>;
using INT16 = Type<int16_t, TILEDB_INT16, int16_repr>;
using UINT16 = Type<uint16_t, TILEDB_UINT16, uint16_repr>;
using INT32 = Type<int32_t, TILEDB_INT32, int32_repr>;
using UINT32 = Type<uint32_t, TILEDB_UINT32, uint32_repr>;
using INT64 = Type<int64_t, TILEDB_INT64, int64_repr>;
using UINT64 = Type<uint64_t, TILEDB_UINT64, uint64_repr>;
using FLOAT32 = Type<float, TILEDB_FLOAT32, float32_repr>;
using FLOAT64 = Type<double, TILEDB_FLOAT64, float64_repr>;

template <tiledb_datatype_t T>
struct type_from_tiledb;
template <>
struct type_from_tiledb<TILEDB_CHAR> {
  using type = CHAR;
};
template <>
struct type_from_tiledb<TILEDB_INT8> {
  using type = INT8;
};
template <>
struct type_from_tiledb<TILEDB_UINT8> {
  using type = UINT8;
};
template <>
struct type_from_tiledb<TILEDB_INT16> {
  using type = INT16;
};
template <>
struct type_from_tiledb<TILEDB_UINT16> {
  using type = UINT16;
};
template <>
struct type_from_tiledb<TILEDB_INT32> {
  using type = INT32;
};
template <>
struct type_from_tiledb<TILEDB_UINT32> {
  using type = UINT32;
};
template <>
struct type_from_tiledb<TILEDB_INT64> {
  using type = INT64;
};
template <>
struct type_from_tiledb<TILEDB_UINT64> {
  using type = UINT64;
};
template <>
struct type_from_tiledb<TILEDB_FLOAT32> {
  using type = FLOAT32;
};
template <>
struct type_from_tiledb<TILEDB_FLOAT64> {
  using type = FLOAT64;
};

template <typename T>
struct type_from_native;
template <>
struct type_from_native<char> {
  using type = CHAR;
};
template <>
struct type_from_native<int8_t> {
  using type = INT8;
};
template <>
struct type_from_native<uint8_t> {
  using type = UINT8;
};
template <>
struct type_from_native<int16_t> {
  using type = INT16;
};
template <>
struct type_from_native<uint16_t> {
  using type = UINT16;
};
template <>
struct type_from_native<int32_t> {
  using type = INT32;
};
template <>
struct type_from_native<uint32_t> {
  using type = UINT32;
};
template <>
struct type_from_native<int64_t> {
  using type = INT64;
};
template <>
struct type_from_native<uint64_t> {
  using type = UINT64;
};
template <>
struct type_from_native<float> {
  using type = FLOAT32;
};
template <>
struct type_from_native<double> {
  using type = FLOAT64;
};

std::string from_tiledb(const tiledb_datatype_t &type);
}  // namespace type

template <class T, template <class...> class Template>
struct is_specialization : std::false_type {};

template <template <class...> class Template, class... Args>
struct is_specialization<Template<Args...>, Template> : std::true_type {};
}  // namespace tdb

#endif  // TILEDB_CPP_API_TYPE_H
