/**
 * @file   type.h
 *
 * @author Ravi Gaddipati
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
 * This defines TileDB datatypes for the C++ API.
 */

#ifndef TILEDB_CPP_API_TYPE_H
#define TILEDB_CPP_API_TYPE_H

#include "tiledb.h"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <limits>
#include <string>
#include <type_traits>
#include <vector>

// Workaround for GCC < 5.0
#if __GNUG__ && __GNUC__ < 5
#define IS_TRIVIALLY_COPYABLE(T) __has_trivial_copy(T)
#else
#define IS_TRIVIALLY_COPYABLE(T) std::is_trivially_copyable<T>::value
#endif

namespace tiledb {

namespace impl {

/** Used to defer compilation of static_assert until type is known. **/
template <typename T>
struct defer_assert : std::false_type {};

/** Used to statically type check for std::array. */
template <typename T>
struct is_stl_array : std::false_type {};
template <typename T, std::size_t N>
struct is_stl_array<std::array<T, N>> : std::true_type {};

/** SFINAE handler for types that make sense to be bitwise copied. **/
template <typename T>
using enable_trivial = typename std::enable_if<
    IS_TRIVIALLY_COPYABLE(T) && !std::is_pointer<T>::value &&
    !std::is_array<T>::value && !is_stl_array<T>::value>::type;

/**
 * Convert a type into a tiledb_datatype_t. The default for all
 * copyable types is char.
 */
template <typename T>
struct type_to_tiledb {
  static_assert(IS_TRIVIALLY_COPYABLE(T), "Type must be trivially copyable.");
  using type = char;
  static const tiledb_datatype_t tiledb_type = TILEDB_STRING_ASCII;
  static constexpr const char* name = "Trivially Copyable (CHAR)";
};

template <>
struct type_to_tiledb<std::byte> {
  using type = std::byte;
  static const tiledb_datatype_t tiledb_type = TILEDB_BLOB;
  static constexpr const char* name = "BLOB";
};

template <>
struct type_to_tiledb<int8_t> {
  using type = int8_t;
  static const tiledb_datatype_t tiledb_type = TILEDB_INT8;
  static constexpr const char* name = "INT8";
};

template <>
struct type_to_tiledb<uint8_t> {
  using type = uint8_t;
  static const tiledb_datatype_t tiledb_type = TILEDB_UINT8;
  static constexpr const char* name = "UINT8";
};

template <>
struct type_to_tiledb<int16_t> {
  using type = int16_t;
  static const tiledb_datatype_t tiledb_type = TILEDB_INT16;
  static constexpr const char* name = "INT16";
};

template <>
struct type_to_tiledb<uint16_t> {
  using type = uint16_t;
  static const tiledb_datatype_t tiledb_type = TILEDB_UINT16;
  static constexpr const char* name = "UINT16";
};

template <>
struct type_to_tiledb<int32_t> {
  using type = int32_t;
  static const tiledb_datatype_t tiledb_type = TILEDB_INT32;
  static constexpr const char* name = "INT32";
};

template <>
struct type_to_tiledb<uint32_t> {
  using type = uint32_t;
  static const tiledb_datatype_t tiledb_type = TILEDB_UINT32;
  static constexpr const char* name = "UINT32";
};

template <>
struct type_to_tiledb<int64_t> {
  using type = int64_t;
  static const tiledb_datatype_t tiledb_type = TILEDB_INT64;
  static constexpr const char* name = "INT64";
};

template <>
struct type_to_tiledb<uint64_t> {
  using type = uint64_t;
  static const tiledb_datatype_t tiledb_type = TILEDB_UINT64;
  static constexpr const char* name = "UINT64";
};

template <>
struct type_to_tiledb<float> {
  using type = float;
  static const tiledb_datatype_t tiledb_type = TILEDB_FLOAT32;
  static constexpr const char* name = "FLOAT32";
};

template <>
struct type_to_tiledb<double> {
  using type = double;
  static const tiledb_datatype_t tiledb_type = TILEDB_FLOAT64;
  static constexpr const char* name = "FLOAT64";
};

/** Convert tiledb_datatype_t to a type. **/
template <tiledb_datatype_t T>
struct tiledb_to_type;

template <>
struct tiledb_to_type<TILEDB_CHAR> {
  using type = char;
  static const tiledb_datatype_t tiledb_type = TILEDB_CHAR;
  static constexpr const char* name = "CHAR";
};

template <>
struct tiledb_to_type<TILEDB_BLOB> {
  using type = std::byte;
  static const tiledb_datatype_t tiledb_type = TILEDB_BLOB;
  static constexpr const char* name = "BLOB";
};

template <>
struct tiledb_to_type<TILEDB_INT8> {
  using type = int8_t;
  static const tiledb_datatype_t tiledb_type = TILEDB_INT8;
  static constexpr const char* name = "INT8";
};

template <>
struct tiledb_to_type<TILEDB_UINT8> {
  using type = uint8_t;
  static const tiledb_datatype_t tiledb_type = TILEDB_UINT8;
  static constexpr const char* name = "UINT8";
};

template <>
struct tiledb_to_type<TILEDB_INT16> {
  using type = int16_t;
  static const tiledb_datatype_t tiledb_type = TILEDB_INT16;
  static constexpr const char* name = "INT16";
};

template <>
struct tiledb_to_type<TILEDB_UINT16> {
  using type = uint16_t;
  static const tiledb_datatype_t tiledb_type = TILEDB_UINT16;
  static constexpr const char* name = "UINT16";
};

template <>
struct tiledb_to_type<TILEDB_INT32> {
  using type = int32_t;
  static const tiledb_datatype_t tiledb_type = TILEDB_INT32;
  static constexpr const char* name = "INT32";
};

template <>
struct tiledb_to_type<TILEDB_UINT32> {
  using type = uint32_t;
  static const tiledb_datatype_t tiledb_type = TILEDB_UINT32;
  static constexpr const char* name = "UINT32";
};

template <>
struct tiledb_to_type<TILEDB_INT64> {
  using type = int64_t;
  static const tiledb_datatype_t tiledb_type = TILEDB_INT64;
  static constexpr const char* name = "INT64";
};

template <>
struct tiledb_to_type<TILEDB_UINT64> {
  using type = uint64_t;
  static const tiledb_datatype_t tiledb_type = TILEDB_UINT64;
  static constexpr const char* name = "UINT64";
};

template <>
struct tiledb_to_type<TILEDB_FLOAT32> {
  using type = float;
  static const tiledb_datatype_t tiledb_type = TILEDB_FLOAT32;
  static constexpr const char* name = "FLOAT32";
};

template <>
struct tiledb_to_type<TILEDB_FLOAT64> {
  using type = double;
  static const tiledb_datatype_t tiledb_type = TILEDB_FLOAT64;
  static constexpr const char* name = "FLOAT64";
};

/** Convert a tiledb datatype to a str. */
inline std::string type_to_str(tiledb_datatype_t type) {
  const char* c_str;
  tiledb_datatype_to_str(type, &c_str);
  return std::string(c_str);
}

inline bool tiledb_string_type(tiledb_datatype_t type) {
  switch (type) {
    case TILEDB_CHAR:
    case TILEDB_STRING_ASCII:
    case TILEDB_STRING_UTF8:
    case TILEDB_STRING_UTF16:
    case TILEDB_STRING_UTF32:
    case TILEDB_STRING_UCS2:
    case TILEDB_STRING_UCS4:
      return true;
    default:
      return false;
  }
}

inline bool tiledb_datetime_type(tiledb_datatype_t type) {
  switch (type) {
    case TILEDB_DATETIME_YEAR:
    case TILEDB_DATETIME_MONTH:
    case TILEDB_DATETIME_WEEK:
    case TILEDB_DATETIME_DAY:
    case TILEDB_DATETIME_HR:
    case TILEDB_DATETIME_MIN:
    case TILEDB_DATETIME_SEC:
    case TILEDB_DATETIME_MS:
    case TILEDB_DATETIME_US:
    case TILEDB_DATETIME_NS:
    case TILEDB_DATETIME_PS:
    case TILEDB_DATETIME_FS:
    case TILEDB_DATETIME_AS:
      return true;
    default:
      return false;
  }
}

inline bool tiledb_time_type(tiledb_datatype_t type) {
  switch (type) {
    case TILEDB_TIME_HR:
    case TILEDB_TIME_MIN:
    case TILEDB_TIME_SEC:
    case TILEDB_TIME_MS:
    case TILEDB_TIME_US:
    case TILEDB_TIME_NS:
    case TILEDB_TIME_PS:
    case TILEDB_TIME_FS:
    case TILEDB_TIME_AS:
      return true;
    default:
      return false;
  }
}

/**
 * A type handler provides a mapping from a C++ type to a TileDB
 * representation.
 *
 * @details
 * Expected members:
 *
 * - value_type : The type, e.x. int for T=int, or char for T=std::string
 * - tiledb_type : TileDB datatype used to store T
 * - tiledb_num : The number of tiledb_type needed to store T.
 *      std::numeric_limits<unsigned>::max() for variable number.
 * - static size_t size(const T&); : The number of elements in the type.
 * - static value_type *data(T&); : Pointer to a contigous region of data.
 * - static void set(T&, const void*, size_t); : Given a T destination object
 *   and a data pointer, set T. This may be omitted for read-only.
 *
 * @tparam T C++ type to handle
 * @tparam Enable Parameter to use for SFINAE dispatch.
 */
template <typename T, typename Enable = void>
struct TypeHandler {
  static_assert(defer_assert<T>::value, "No TypeHandler exists for type.");
};

/** Handler for all trivially copyable types, such as POD structs. **/
template <typename T>
struct TypeHandler<T, enable_trivial<T>> {
  using value_type = T;
  static constexpr tiledb_datatype_t tiledb_type =
      type_to_tiledb<T>::tiledb_type;
  static constexpr unsigned tiledb_num =
      sizeof(T) / sizeof(typename type_to_tiledb<T>::type);

  static size_t size(const T&) {
    return 1;
  }

  static value_type* data(T& v) {
    return &v;
  }

  static value_type const* data(T const& v) {
    return &v;
  }

  static void set(T& dest, const void* d, size_t size) {
    if (size != sizeof(value_type))
      throw std::runtime_error("Attempting to set type with incorrect size.");
    memcpy(data(dest), d, size);
  }
};

/** Handler for strings **/
template <typename T>
struct TypeHandler<std::basic_string<T>, enable_trivial<T>> {
  using element_th = TypeHandler<T>;
  using value_type = typename element_th::value_type;
  static constexpr tiledb_datatype_t tiledb_type = element_th::tiledb_type;
  static constexpr unsigned tiledb_num =
      std::numeric_limits<unsigned>::max();  // TODO constexpr VAR_NUM

  static size_t size(const std::basic_string<T>& v) {
    return v.size();
  }

  static value_type* data(std::basic_string<T>& v) {
    return const_cast<char*>(v.data());
  }

  static value_type const* data(std::basic_string<T> const& v) {
    return v.data();
  }

  static void set(std::basic_string<T>& dest, const void* d, size_t size) {
    auto num = size / sizeof(value_type);
    dest.resize(num);
    memcpy(data(dest), d, size);
  }
};

/** Handler for constant strings. Setting is disallowed. **/
template <>
struct TypeHandler<const char*> {
  using element_th = TypeHandler<char>;
  using value_type = typename element_th::value_type;
  static constexpr tiledb_datatype_t tiledb_type = element_th::tiledb_type;
  static constexpr unsigned tiledb_num =
      std::numeric_limits<unsigned>::max();  // TODO constexpr VAR_NUM

  static size_t size(const char* v) {
    return strlen(v);
  }

  static value_type const* data(const char* v) {
    return v;
  }
};

/** Handler for vectors. **/
template <typename T>
struct TypeHandler<std::vector<T>, enable_trivial<T>> {
  using element_th = TypeHandler<T>;
  using value_type = typename element_th::value_type;
  static constexpr tiledb_datatype_t tiledb_type = element_th::tiledb_type;
  static constexpr unsigned tiledb_num =
      std::numeric_limits<unsigned>::max();  // TODO constexpr VAR_NUM

  static size_t size(const std::vector<T>& v) {
    return v.size();
  }

  static value_type* data(std::vector<T>& v) {
    return v.data();
  }

  static value_type const* data(std::vector<T> const& v) {
    return v.data();
  }

  static void set(std::vector<T>& dest, const void* d, size_t size) {
    auto num = size / sizeof(value_type);
    dest.resize(num);
    memcpy(data(dest), d, size);
  }
};

/** Handler for STL arrays. **/
template <typename T, std::size_t N>
struct TypeHandler<std::array<T, N>, enable_trivial<T>> {
  using element_th = TypeHandler<T>;
  using value_type = typename element_th::value_type;
  static constexpr tiledb_datatype_t tiledb_type = element_th::tiledb_type;
  static constexpr unsigned tiledb_num = N * element_th::tiledb_num;

  static size_t size(const std::array<T, N>& a) {
    return a.size();
  }

  static value_type* data(std::array<T, N>& a) {
    return a.data();
  }

  static value_type const* data(std::array<T, N> const& a) {
    return a.data();
  }

  static void set(std::array<T, N>& dest, const void* d, size_t size) {
    auto num = size / sizeof(value_type);
    dest.resize(num);
    memcpy(data(dest), d, size);
  }
};

/** Handler for compile-time C style arrays. **/
template <typename T, unsigned N>
struct TypeHandler<T[N], enable_trivial<T>> {
  using element_th = TypeHandler<T>;
  using value_type = typename element_th::value_type;
  static constexpr tiledb_datatype_t tiledb_type = element_th::tiledb_type;
  static constexpr unsigned tiledb_num = N * element_th::tiledb_num;

  static size_t size(T const[N]) {
    return N;
  }

  static value_type* data(T v[N]) {
    return v;
  }

  static value_type const* data(T const v[N]) {
    return v;
  }

  static void set(T& dest, const void* d, size_t size) {
    if (size != sizeof(value_type) * N)
      throw std::runtime_error("Attempting to set type with incorrect size.");
    memcpy(data(dest), d, size);
  }
};

}  // namespace impl
}  // namespace tiledb

#endif  // TILEDB_CPP_API_TYPE_H
