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

#include "tiledb.h"

#include <cstdint>
#include <string>
#include <vector>

namespace tdb {

namespace impl {

/**
 * Repr of a datatype / tiledb datatype.
 *
 * @tparam T underlying type
 * @tparam TDB_TYPE tiledb_data_type repr
 */
  template <typename T, tiledb_datatype_t TDB_TYPE>
  struct Type {
    Type() = delete;
    using type = T;
    static constexpr tiledb_datatype_t tiledb_datatype = TDB_TYPE;
    static constexpr const char *name =
    std::is_same<T, char>::value ? "char" :
    (std::is_same<T, int8_t>::value ? "int8_t" :
     (std::is_same<T, uint8_t>::value ? "uint8_t" :
      (std::is_same<T, int16_t>::value ? "int16_t" :
       (std::is_same<T, uint16_t>::value ? "uint16_t" :
        (std::is_same<T, int32_t>::value ? "int32_t" :
         (std::is_same<T, uint32_t>::value ? "uint32_t" :
          (std::is_same<T, int64_t>::value ? "int64_t" :
           (std::is_same<T, uint64_t>::value ? "uint64_t" :
            (std::is_same<T, float>::value ? "float" :
             (std::is_same<T, double>::value ? "double" :
              "INVALID"))))))))));
  };

  using CHAR = Type<char, TILEDB_CHAR>;
  using INT8 = Type<int8_t, TILEDB_INT8>;
  using UINT8 = Type<uint8_t, TILEDB_UINT8>;
  using INT16 = Type<int16_t, TILEDB_INT16>;
  using UINT16 = Type<uint16_t, TILEDB_UINT16>;
  using INT32 = Type<int32_t, TILEDB_INT32>;
  using UINT32 = Type<uint32_t, TILEDB_UINT32>;
  using INT64 = Type<int64_t, TILEDB_INT64>;
  using UINT64 = Type<uint64_t, TILEDB_UINT64>;
  using FLOAT32 = Type<float, TILEDB_FLOAT32>;
  using FLOAT64 = Type<double, TILEDB_FLOAT64>;

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

std::string to_str(const tiledb_datatype_t &type);

}  // namespace impl

template <class T, template <class...> class Template>
struct is_specialization : std::false_type {};

template <template <class...> class Template, class... Args>
struct is_specialization<Template<Args...>, Template> : std::true_type {};

}  // namespace tdb

#endif  // TILEDB_CPP_API_TYPE_H
