/**
 * @file   datatype_traits.h
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
 *
 */

#ifndef TILEDB_DATATYPE_TRAITS_H
#define TILEDB_DATATYPE_TRAITS_H

#include "tiledb/sm/enums/datatype.h"

namespace tiledb {
namespace sm {

/*
 * Datatype Traits
 *
 * Datatype trait classes record information accessed previously only through
 * runtime functions. Trait classes allow such information to be used both at
 * compile time and at runtime. In addition, certain information useful for
 * templates can be recorded with traits but can't be returned from a function,
 * for example `value_type`.
 */

/**
 * Base class for datatype traits holds common information and defaults.
 *
 * @tparam D
 */
template <Datatype D>
struct datatype_traits_base {
  /* Common to all datatype_traits specializations */
  static constexpr Datatype datatype = D;
  static constexpr bool is_valid = true;

  /* Defaults */
  static constexpr bool is_string = false;
  static constexpr bool is_integer = false;
  static constexpr bool is_real = false;
  static constexpr bool is_datetime = false;
  static constexpr bool is_time = false;
};

/**
 * Generic traits class should not be instantiated.
 * @tparam D
 */
template <Datatype D>
struct datatype_traits {
  /**
   * Validity of the template argument D.
   *
   * Default value is false, meaning that only explicitly specialized templates
   * represent valid Datatypes.
   */
  static constexpr bool is_valid = false;

  /**
   * C++ type that holds a the value of datatype D
   */
  typedef void value_type;

  /**
   * String representation of D
   *
   * Using char[] instead of std::basic_string because constexpr constructors
   * for it aren't available until C++20.
   */
  static constexpr char str[] = "";
};

template <>
struct datatype_traits<Datatype::CHAR>
    : public datatype_traits_base<Datatype::CHAR> {
  typedef char value_type;
  /* Not in any of the is_* categories */
  static constexpr char str[] = "CHAR";
  static constexpr value_type fill_value = std::numeric_limits<char>::min();
};

template <>
struct datatype_traits<Datatype::INT8>
    : public datatype_traits_base<Datatype::INT8> {
  typedef int8_t value_type;
  static constexpr bool is_integer = true;
  static constexpr char str[] = "INT8";
  static constexpr value_type fill_value = std::numeric_limits<int8_t>::min();
};

template <>
struct datatype_traits<Datatype::INT16>
    : public datatype_traits_base<Datatype::INT16> {
  typedef int16_t value_type;
  static constexpr bool is_integer = true;
  static constexpr char str[] = "INT16";
  static constexpr value_type fill_value = std::numeric_limits<int16_t>::min();
};

template <>
struct datatype_traits<Datatype::INT32>
    : public datatype_traits_base<Datatype::INT32> {
  typedef int32_t value_type;
  static constexpr bool is_integer = true;
  static constexpr char str[] = "INT32";
  static constexpr value_type fill_value = std::numeric_limits<int32_t>::min();
};

template <>
struct datatype_traits<Datatype::INT64>
    : public datatype_traits_base<Datatype::INT64> {
  typedef int64_t value_type;
  static constexpr bool is_integer = true;
  static constexpr char str[] = "INT64";
  static constexpr value_type fill_value = std::numeric_limits<int64_t>::min();
};

template <>
struct datatype_traits<Datatype::UINT8>
    : public datatype_traits_base<Datatype::UINT8> {
  typedef uint8_t value_type;
  static constexpr bool is_integer = true;
  static constexpr char str[] = "UINT8";
  static constexpr value_type fill_value = std::numeric_limits<uint8_t>::max();
};

template <>
struct datatype_traits<Datatype::UINT16>
    : public datatype_traits_base<Datatype::UINT16> {
  typedef uint16_t value_type;
  static constexpr bool is_integer = true;
  static constexpr char str[] = "UINT16";
  static constexpr value_type fill_value = std::numeric_limits<uint16_t>::max();
};

template <>
struct datatype_traits<Datatype::UINT32>
    : public datatype_traits_base<Datatype::UINT32> {
  typedef uint32_t value_type;
  static constexpr bool is_integer = true;
  static constexpr char str[] = "UINT32";
  static constexpr value_type fill_value = std::numeric_limits<uint32_t>::max();
};

template <>
struct datatype_traits<Datatype::UINT64>
    : public datatype_traits_base<Datatype::UINT64> {
  typedef uint64_t value_type;
  static constexpr bool is_integer = true;
  static constexpr char str[] = "UINT64";
  static constexpr value_type fill_value = std::numeric_limits<uint64_t>::max();
};

template <>
struct datatype_traits<Datatype::FLOAT32>
    : public datatype_traits_base<Datatype::FLOAT32> {
  typedef float value_type;
  static constexpr bool is_float = true;
  static constexpr char str[] = "FLOAT32";
  static constexpr value_type fill_value =
      std::numeric_limits<float>::quiet_NaN();
};

template <>
struct datatype_traits<Datatype::FLOAT64>
    : public datatype_traits_base<Datatype::FLOAT64> {
  typedef double value_type;
  static constexpr bool is_float = true;
  static constexpr char str[] = "FLOAT64";
  static constexpr value_type fill_value =
      std::numeric_limits<double>::quiet_NaN();
};

template <>
struct datatype_traits<Datatype::STRING_ASCII>
    : public datatype_traits_base<Datatype::STRING_ASCII> {
  typedef uint8_t value_type;
  static constexpr bool is_string = true;
  static constexpr char str[] = "STRING_ASCII";
  static constexpr value_type fill_value = 0;
};

template <>
struct datatype_traits<Datatype::STRING_UTF8>
    : public datatype_traits_base<Datatype::STRING_UTF8> {
  typedef uint8_t value_type;
  static constexpr bool is_string = true;
  static constexpr char str[] = "STRING_UTF8";
  static constexpr value_type fill_value = 0;
};

template <>
struct datatype_traits<Datatype::STRING_UTF16>
    : public datatype_traits_base<Datatype::STRING_UTF16> {
  typedef uint16_t value_type;
  static constexpr bool is_string = true;
  static constexpr char str[] = "STRING_UTF16";
  static constexpr value_type fill_value = 0;
};

template <>
struct datatype_traits<Datatype::STRING_UTF32>
    : public datatype_traits_base<Datatype::STRING_UTF32> {
  typedef uint32_t value_type;
  static constexpr bool is_string = true;
  static constexpr char str[] = "STRING_UTF32";
  static constexpr value_type fill_value = 0;
};

template <>
struct datatype_traits<Datatype::STRING_UCS2>
    : public datatype_traits_base<Datatype::STRING_UCS2> {
  typedef uint16_t value_type;
  static constexpr bool is_string = true;
  static constexpr char str[] = "STRING_UCS2";
  static constexpr value_type fill_value = 0;
};

template <>
struct datatype_traits<Datatype::STRING_UCS4>
    : public datatype_traits_base<Datatype::STRING_UCS4> {
  typedef uint32_t value_type;
  static constexpr bool is_string = true;
  static constexpr char str[] = "STRING_UCS4";
  static constexpr value_type fill_value = 0;
};

template <>
struct datatype_traits<Datatype::ANY>
    : public datatype_traits_base<Datatype::ANY> {
  typedef uint8_t value_type;
  /* Not in any of the is_* categories */
  static constexpr char str[] = "ANY";
  static constexpr value_type fill_value = 0;
};

template <>
struct datatype_traits<Datatype::DATETIME_YEAR>
    : public datatype_traits_base<Datatype::DATETIME_YEAR> {
  typedef int64_t value_type;
  static constexpr bool is_datetime = true;
  static constexpr char str[] = "DATETIME_YEAR";
  static constexpr value_type fill_value = std::numeric_limits<int64_t>::min();
};

template <>
struct datatype_traits<Datatype::DATETIME_MONTH>
    : public datatype_traits_base<Datatype::DATETIME_MONTH> {
  typedef int64_t value_type;
  static constexpr bool is_datetime = true;
  static constexpr char str[] = "DATETIME_MONTH";
  static constexpr value_type fill_value = std::numeric_limits<int64_t>::min();
};

template <>
struct datatype_traits<Datatype::DATETIME_WEEK>
    : public datatype_traits_base<Datatype::DATETIME_WEEK> {
  typedef int64_t value_type;
  static constexpr bool is_datetime = true;
  static constexpr char str[] = "DATETIME_WEEK";
  static constexpr value_type fill_value = std::numeric_limits<int64_t>::min();
};

template <>
struct datatype_traits<Datatype::DATETIME_DAY>
    : public datatype_traits_base<Datatype::DATETIME_DAY> {
  typedef int64_t value_type;
  static constexpr bool is_datetime = true;
  static constexpr char str[] = "DATETIME_DAY";
  static constexpr value_type fill_value = std::numeric_limits<int64_t>::min();
};

template <>
struct datatype_traits<Datatype::DATETIME_HR>
    : public datatype_traits_base<Datatype::DATETIME_HR> {
  typedef int64_t value_type;
  static constexpr bool is_datetime = true;
  static constexpr char str[] = "DATETIME_HR";
  static constexpr value_type fill_value = std::numeric_limits<int64_t>::min();
};

template <>
struct datatype_traits<Datatype::DATETIME_MIN>
    : public datatype_traits_base<Datatype::DATETIME_MIN> {
  typedef int64_t value_type;
  static constexpr bool is_datetime = true;
  static constexpr char str[] = "DATETIME_MIN";
  static constexpr value_type fill_value = std::numeric_limits<int64_t>::min();
};

template <>
struct datatype_traits<Datatype::DATETIME_SEC>
    : public datatype_traits_base<Datatype::DATETIME_SEC> {
  typedef int64_t value_type;
  static constexpr bool is_datetime = true;
  static constexpr char str[] = "DATETIME_SEC";
  static constexpr value_type fill_value = std::numeric_limits<int64_t>::min();
};

template <>
struct datatype_traits<Datatype::DATETIME_MS>
    : public datatype_traits_base<Datatype::DATETIME_MS> {
  typedef int64_t value_type;
  static constexpr bool is_datetime = true;
  static constexpr char str[] = "DATETIME_MS";
  static constexpr value_type fill_value = std::numeric_limits<int64_t>::min();
};

template <>
struct datatype_traits<Datatype::DATETIME_US>
    : public datatype_traits_base<Datatype::DATETIME_US> {
  typedef int64_t value_type;
  static constexpr bool is_datetime = true;
  static constexpr char str[] = "DATETIME_US";
  static constexpr value_type fill_value = std::numeric_limits<int64_t>::min();
};

template <>
struct datatype_traits<Datatype::DATETIME_NS>
    : public datatype_traits_base<Datatype::DATETIME_NS> {
  typedef int64_t value_type;
  static constexpr bool is_datetime = true;
  static constexpr char str[] = "DATETIME_NS";
  static constexpr value_type fill_value = std::numeric_limits<int64_t>::min();
};

template <>
struct datatype_traits<Datatype::DATETIME_PS>
    : public datatype_traits_base<Datatype::DATETIME_PS> {
  typedef int64_t value_type;
  static constexpr bool is_datetime = true;
  static constexpr char str[] = "DATETIME_PS";
  static constexpr value_type fill_value = std::numeric_limits<int64_t>::min();
};

template <>
struct datatype_traits<Datatype::DATETIME_FS>
    : public datatype_traits_base<Datatype::DATETIME_FS> {
  typedef int64_t value_type;
  static constexpr bool is_datetime = true;
  static constexpr char str[] = "DATETIME_FS";
  static constexpr value_type fill_value = std::numeric_limits<int64_t>::min();
};

template <>
struct datatype_traits<Datatype::DATETIME_AS>
    : public datatype_traits_base<Datatype::DATETIME_AS> {
  typedef int64_t value_type;
  static constexpr bool is_datetime = true;
  static constexpr char str[] = "DATETIME_AS";
  static constexpr value_type fill_value = std::numeric_limits<int64_t>::min();
};

template <>
struct datatype_traits<Datatype::TIME_HR>
    : public datatype_traits_base<Datatype::TIME_HR> {
  typedef int64_t value_type;
  static constexpr bool is_time = true;
  static constexpr char str[] = "TIME_HR";
  static constexpr value_type fill_value = std::numeric_limits<int64_t>::min();
};

template <>
struct datatype_traits<Datatype::TIME_MIN>
    : public datatype_traits_base<Datatype::TIME_MIN> {
  typedef int64_t value_type;
  static constexpr bool is_time = true;
  static constexpr char str[] = "TIME_MIN";
  static constexpr value_type fill_value = std::numeric_limits<int64_t>::min();
};

template <>
struct datatype_traits<Datatype::TIME_SEC>
    : public datatype_traits_base<Datatype::TIME_SEC> {
  typedef int64_t value_type;
  static constexpr bool is_time = true;
  static constexpr char str[] = "TIME_SEC";
  static constexpr value_type fill_value = std::numeric_limits<int64_t>::min();
};

template <>
struct datatype_traits<Datatype::TIME_MS>
    : public datatype_traits_base<Datatype::TIME_MS> {
  typedef int64_t value_type;
  static constexpr bool is_time = true;
  static constexpr char str[] = "TIME_MS";
  static constexpr value_type fill_value = std::numeric_limits<int64_t>::min();
};

template <>
struct datatype_traits<Datatype::TIME_US>
    : public datatype_traits_base<Datatype::TIME_US> {
  typedef int64_t value_type;
  static constexpr bool is_time = true;
  static constexpr char str[] = "TIME_US";
  static constexpr value_type fill_value = std::numeric_limits<int64_t>::min();
};

template <>
struct datatype_traits<Datatype::TIME_NS>
    : public datatype_traits_base<Datatype::TIME_NS> {
  typedef int64_t value_type;
  static constexpr bool is_time = true;
  static constexpr char str[] = "TIME_NS";
  static constexpr value_type fill_value = std::numeric_limits<int64_t>::min();
};

template <>
struct datatype_traits<Datatype::TIME_PS>
    : public datatype_traits_base<Datatype::TIME_PS> {
  typedef int64_t value_type;
  static constexpr bool is_time = true;
  static constexpr char str[] = "TIME_PS";
  static constexpr value_type fill_value = std::numeric_limits<int64_t>::min();
};

template <>
struct datatype_traits<Datatype::TIME_FS>
    : public datatype_traits_base<Datatype::TIME_FS> {
  typedef int64_t value_type;
  static constexpr bool is_time = true;
  static constexpr char str[] = "TIME_FS";
  static constexpr value_type fill_value = std::numeric_limits<int64_t>::min();
};

template <>
struct datatype_traits<Datatype::TIME_AS>
    : public datatype_traits_base<Datatype::TIME_AS> {
  typedef int64_t value_type;
  static constexpr bool is_time = true;
  static constexpr char str[] = "TIME_AS";
  static constexpr value_type fill_value = std::numeric_limits<int64_t>::min();
};

/**
 * Predicate function returning validity of datatype. Returns the same value as
 * if evaluated at compile-time as a datatype trait.
 *
 * @param d
 * @return datatype_traits<d>.is_valid
 */
static constexpr bool is_valid_datatype(Datatype d);

/**
 * A constant map from a datatype to some type, that is, a function with domain
 * Datatype and range T.
 *
 * These maps depend sensitively on the datatype enumeration values in
 * tiledb_enum.h.
 *
 * @tparam T the range of the map
 * @tparam M map template that provides values in the range
 */
template <class T, template <typename> class M>
class datatype_map {
  static constexpr size_t array_length = 40;

 public:
  /**
   *
   * @param d
   * @return
   */
  static constexpr T map[array_length] = {
      M<datatype_traits<Datatype::INT32>>::value,
      M<datatype_traits<Datatype::INT64>>::value,
      M<datatype_traits<Datatype::FLOAT32>>::value,
      M<datatype_traits<Datatype::FLOAT64>>::value,
      M<datatype_traits<Datatype::CHAR>>::value,
      M<datatype_traits<Datatype::INT8>>::value,
      M<datatype_traits<Datatype::UINT8>>::value,
      M<datatype_traits<Datatype::INT16>>::value,
      M<datatype_traits<Datatype::UINT16>>::value,
      M<datatype_traits<Datatype::UINT32>>::value,
      M<datatype_traits<Datatype::UINT64>>::value,
      M<datatype_traits<Datatype::STRING_ASCII>>::value,
      M<datatype_traits<Datatype::STRING_UTF8>>::value,
      M<datatype_traits<Datatype::STRING_UTF16>>::value,
      M<datatype_traits<Datatype::STRING_UTF32>>::value,
      M<datatype_traits<Datatype::STRING_UCS2>>::value,
      M<datatype_traits<Datatype::STRING_UCS4>>::value,
      M<datatype_traits<Datatype::ANY>>::value,
      M<datatype_traits<Datatype::DATETIME_YEAR>>::value,
      M<datatype_traits<Datatype::DATETIME_MONTH>>::value,
      M<datatype_traits<Datatype::DATETIME_WEEK>>::value,
      M<datatype_traits<Datatype::DATETIME_DAY>>::value,
      M<datatype_traits<Datatype::DATETIME_HR>>::value,
      M<datatype_traits<Datatype::DATETIME_MIN>>::value,
      M<datatype_traits<Datatype::DATETIME_SEC>>::value,
      M<datatype_traits<Datatype::DATETIME_MS>>::value,
      M<datatype_traits<Datatype::DATETIME_US>>::value,
      M<datatype_traits<Datatype::DATETIME_NS>>::value,
      M<datatype_traits<Datatype::DATETIME_PS>>::value,
      M<datatype_traits<Datatype::DATETIME_FS>>::value,
      M<datatype_traits<Datatype::DATETIME_AS>>::value,
      M<datatype_traits<Datatype::TIME_HR>>::value,
      M<datatype_traits<Datatype::TIME_MIN>>::value,
      M<datatype_traits<Datatype::TIME_SEC>>::value,
      M<datatype_traits<Datatype::TIME_MS>>::value,
      M<datatype_traits<Datatype::TIME_US>>::value,
      M<datatype_traits<Datatype::TIME_NS>>::value,
      M<datatype_traits<Datatype::TIME_PS>>::value,
      M<datatype_traits<Datatype::TIME_FS>>::value,
      M<datatype_traits<Datatype::TIME_AS>>::value,
  };
};

template <class DT>
struct dtm_datatype {
  typedef Datatype value_type;
  static constexpr value_type value = DT::datatype;
};

template <class DT>
struct dtm_is_valid {
  typedef bool value_type;
  static constexpr value_type value = DT::is_valid;
};

template <class DT>
struct dtm_sizeof_value_type {
  typedef uint64_t value_type;
  static constexpr value_type value = sizeof(DT::value_type);
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_DATATYPE_TRAITS_H
