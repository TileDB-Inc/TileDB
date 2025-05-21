/**
 * @file datatype.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
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

#include "tiledb/common/status.h"
#include "tiledb/common/unreachable.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/stdx/utility/to_underlying.h"

#include <cassert>

using namespace tiledb::common;

namespace tiledb::sm {

#if defined(_WIN32) && defined(TIME_MS)
#pragma message("WARNING: Windows.h may have already been included before")
#pragma message("         tiledb/sm/enums/datatype.h which will likely cause a")
#pragma message("         syntax error while defining the Datatype enum class.")
#endif

/** Defines a datatype. */
enum class Datatype : uint8_t {
#define TILEDB_DATATYPE_ENUM(id) id
#include "tiledb/api/c_api/datatype/datatype_api_enum.h"
#undef TILEDB_DATATYPE_ENUM
};

/**
 *  Returns the datatype size.
 *
 *  This function also serves as a datatype validation function;
 *  If 0 is returned, the input Datatype is invalid.
 */
inline uint64_t datatype_size(Datatype type) noexcept {
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
    case Datatype::BLOB:
      return sizeof(std::byte);
    case Datatype::GEOM_WKB:
      return sizeof(std::byte);
    case Datatype::GEOM_WKT:
      return sizeof(std::byte);
    case Datatype::BOOL:
      return sizeof(uint8_t);
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
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
      return sizeof(int64_t);
  }

  ::stdx::unreachable();
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
    case Datatype::BLOB:
      return constants::blob_str;
    case Datatype::GEOM_WKB:
      return constants::geom_wkb_str;
    case Datatype::GEOM_WKT:
      return constants::geom_wkt_str;
    case Datatype::BOOL:
      return constants::bool_str;
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
    case Datatype::DATETIME_YEAR:
      return constants::datetime_year_str;
    case Datatype::DATETIME_MONTH:
      return constants::datetime_month_str;
    case Datatype::DATETIME_WEEK:
      return constants::datetime_week_str;
    case Datatype::DATETIME_DAY:
      return constants::datetime_day_str;
    case Datatype::DATETIME_HR:
      return constants::datetime_hr_str;
    case Datatype::DATETIME_MIN:
      return constants::datetime_min_str;
    case Datatype::DATETIME_SEC:
      return constants::datetime_sec_str;
    case Datatype::DATETIME_MS:
      return constants::datetime_ms_str;
    case Datatype::DATETIME_US:
      return constants::datetime_us_str;
    case Datatype::DATETIME_NS:
      return constants::datetime_ns_str;
    case Datatype::DATETIME_PS:
      return constants::datetime_ps_str;
    case Datatype::DATETIME_FS:
      return constants::datetime_fs_str;
    case Datatype::DATETIME_AS:
      return constants::datetime_as_str;
    case Datatype::TIME_HR:
      return constants::time_hr_str;
    case Datatype::TIME_MIN:
      return constants::time_min_str;
    case Datatype::TIME_SEC:
      return constants::time_sec_str;
    case Datatype::TIME_MS:
      return constants::time_ms_str;
    case Datatype::TIME_US:
      return constants::time_us_str;
    case Datatype::TIME_NS:
      return constants::time_ns_str;
    case Datatype::TIME_PS:
      return constants::time_ps_str;
    case Datatype::TIME_FS:
      return constants::time_fs_str;
    case Datatype::TIME_AS:
      return constants::time_as_str;
    default:
      return constants::empty_str;
  }
}

/** Returns the datatype given a string representation. */
inline Datatype datatype_enum(const std::string& datatype_str) {
  if (datatype_str == constants::int32_str)
    return Datatype::INT32;
  else if (datatype_str == constants::int64_str)
    return Datatype::INT64;
  else if (datatype_str == constants::float32_str)
    return Datatype::FLOAT32;
  else if (datatype_str == constants::float64_str)
    return Datatype::FLOAT64;
  else if (datatype_str == constants::char_str)
    return Datatype::CHAR;
  else if (datatype_str == constants::blob_str)
    return Datatype::BLOB;
  else if (datatype_str == constants::geom_wkb_str)
    return Datatype::GEOM_WKB;
  else if (datatype_str == constants::geom_wkt_str)
    return Datatype::GEOM_WKT;
  else if (datatype_str == constants::bool_str)
    return Datatype::BOOL;
  else if (datatype_str == constants::int8_str)
    return Datatype::INT8;
  else if (datatype_str == constants::uint8_str)
    return Datatype::UINT8;
  else if (datatype_str == constants::int16_str)
    return Datatype::INT16;
  else if (datatype_str == constants::uint16_str)
    return Datatype::UINT16;
  else if (datatype_str == constants::uint32_str)
    return Datatype::UINT32;
  else if (datatype_str == constants::uint64_str)
    return Datatype::UINT64;
  else if (datatype_str == constants::string_ascii_str)
    return Datatype::STRING_ASCII;
  else if (datatype_str == constants::string_utf8_str)
    return Datatype::STRING_UTF8;
  else if (datatype_str == constants::string_utf16_str)
    return Datatype::STRING_UTF16;
  else if (datatype_str == constants::string_utf32_str)
    return Datatype::STRING_UTF32;
  else if (datatype_str == constants::string_ucs2_str)
    return Datatype::STRING_UCS2;
  else if (datatype_str == constants::string_ucs4_str)
    return Datatype::STRING_UCS4;
  else if (datatype_str == constants::any_str)
    return Datatype::ANY;
  else if (datatype_str == constants::datetime_year_str)
    return Datatype::DATETIME_YEAR;
  else if (datatype_str == constants::datetime_month_str)
    return Datatype::DATETIME_MONTH;
  else if (datatype_str == constants::datetime_week_str)
    return Datatype::DATETIME_WEEK;
  else if (datatype_str == constants::datetime_day_str)
    return Datatype::DATETIME_DAY;
  else if (datatype_str == constants::datetime_hr_str)
    return Datatype::DATETIME_HR;
  else if (datatype_str == constants::datetime_min_str)
    return Datatype::DATETIME_MIN;
  else if (datatype_str == constants::datetime_sec_str)
    return Datatype::DATETIME_SEC;
  else if (datatype_str == constants::datetime_ms_str)
    return Datatype::DATETIME_MS;
  else if (datatype_str == constants::datetime_us_str)
    return Datatype::DATETIME_US;
  else if (datatype_str == constants::datetime_ns_str)
    return Datatype::DATETIME_NS;
  else if (datatype_str == constants::datetime_ps_str)
    return Datatype::DATETIME_PS;
  else if (datatype_str == constants::datetime_fs_str)
    return Datatype::DATETIME_FS;
  else if (datatype_str == constants::datetime_as_str)
    return Datatype::DATETIME_AS;
  else if (datatype_str == constants::time_hr_str)
    return Datatype::TIME_HR;
  else if (datatype_str == constants::time_min_str)
    return Datatype::TIME_MIN;
  else if (datatype_str == constants::time_sec_str)
    return Datatype::TIME_SEC;
  else if (datatype_str == constants::time_ms_str)
    return Datatype::TIME_MS;
  else if (datatype_str == constants::time_us_str)
    return Datatype::TIME_US;
  else if (datatype_str == constants::time_ns_str)
    return Datatype::TIME_NS;
  else if (datatype_str == constants::time_ps_str)
    return Datatype::TIME_PS;
  else if (datatype_str == constants::time_fs_str)
    return Datatype::TIME_FS;
  else if (datatype_str == constants::time_as_str)
    return Datatype::TIME_AS;
  else {
    throw std::runtime_error(
        "Invalid Datatype string (\"" + datatype_str + "\")");
  }
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
      type == Datatype::BOOL || type == Datatype::INT8 ||
      type == Datatype::UINT8 || type == Datatype::INT16 ||
      type == Datatype::UINT16 || type == Datatype::INT32 ||
      type == Datatype::UINT32 || type == Datatype::INT64 ||
      type == Datatype::UINT64);
}

/**
 * Return the largest integral value for the provided type.
 *
 * @param type The Datatype to return the maximum value of.
 * @returns uint64_t The maximum integral value for the provided type.
 */
inline uint64_t datatype_max_integral_value(Datatype type) {
  switch (type) {
    case Datatype::BOOL:
      return static_cast<uint64_t>(std::numeric_limits<uint8_t>::max());
    case Datatype::INT8:
      return static_cast<uint64_t>(std::numeric_limits<int8_t>::max());
    case Datatype::UINT8:
      return static_cast<uint64_t>(std::numeric_limits<uint8_t>::max());
    case Datatype::INT16:
      return static_cast<uint64_t>(std::numeric_limits<int16_t>::max());
    case Datatype::UINT16:
      return static_cast<uint64_t>(std::numeric_limits<uint16_t>::max());
    case Datatype::INT32:
      return static_cast<uint64_t>(std::numeric_limits<int32_t>::max());
    case Datatype::UINT32:
      return static_cast<uint64_t>(std::numeric_limits<uint32_t>::max());
    case Datatype::INT64:
      return static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
    case Datatype::UINT64:
      return static_cast<uint64_t>(std::numeric_limits<uint64_t>::max());
    default:
      throw std::runtime_error(
          "Datatype (" + datatype_str(type) + ") is not integral.");
  }
}

/** Returns true if the input datatype is a real type. */
inline bool datatype_is_real(Datatype type) {
  return (type == Datatype::FLOAT32 || type == Datatype::FLOAT64);
}

/** Returns true if the input datatype is a datetime type. */
inline bool datatype_is_datetime(Datatype type) {
  return (
      type == Datatype::DATETIME_YEAR || type == Datatype::DATETIME_MONTH ||
      type == Datatype::DATETIME_WEEK || type == Datatype::DATETIME_DAY ||
      type == Datatype::DATETIME_HR || type == Datatype::DATETIME_MIN ||
      type == Datatype::DATETIME_SEC || type == Datatype::DATETIME_MS ||
      type == Datatype::DATETIME_US || type == Datatype::DATETIME_NS ||
      type == Datatype::DATETIME_PS || type == Datatype::DATETIME_FS ||
      type == Datatype::DATETIME_AS);
}

/** Returns true if the input datatype is a time type. */
inline bool datatype_is_time(Datatype type) {
  return (
      type == Datatype::TIME_HR || type == Datatype::TIME_MIN ||
      type == Datatype::TIME_SEC || type == Datatype::TIME_MS ||
      type == Datatype::TIME_US || type == Datatype::TIME_NS ||
      type == Datatype::TIME_PS || type == Datatype::TIME_FS ||
      type == Datatype::TIME_AS);
}

/** Returns true if the input datatype is a std::byte type. */
inline bool datatype_is_byte(Datatype type) {
  return (
      type == Datatype::BLOB || type == Datatype::GEOM_WKB ||
      type == Datatype::GEOM_WKT);
}

/** Returns true if the input datatype is a boolean type. */
inline bool datatype_is_boolean(Datatype type) {
  return (type == Datatype::BOOL);
}

/** Throws error if the input Datatype's enum is not between 0 and 43. */
inline void ensure_datatype_is_valid(uint8_t datatype_enum) {
  if (datatype_enum > 43) {
    throw std::runtime_error(
        "Invalid Datatype (" + std::to_string(datatype_enum) + ")");
  }
}

/** Throws error if the input Datatype's enum is not between 0 and 43. */
inline void ensure_datatype_is_valid(Datatype type) {
  ensure_datatype_is_valid(::stdx::to_underlying(type));
}

/**
 * Throws error if:
 *
 * the datatype string is not valid as a datatype.
 * the datatype string's enum is not between 0 and 43.
 **/
inline void ensure_datatype_is_valid(const std::string& datatype_str) {
  Datatype datatype_type = datatype_enum(datatype_str);
  ensure_datatype_is_valid(datatype_type);
}

/** Throws an error if the input type is not supported for dimensions. */
inline void ensure_dimension_datatype_is_valid(Datatype type) {
  switch (type) {
    case Datatype::INT32:
    case Datatype::INT64:
    case Datatype::FLOAT32:
    case Datatype::FLOAT64:
    case Datatype::INT8:
    case Datatype::UINT8:
    case Datatype::INT16:
    case Datatype::UINT16:
    case Datatype::UINT32:
    case Datatype::UINT64:
    case Datatype::STRING_ASCII:
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
      return;
    default:
      throw std::runtime_error(
          "Datatype '" + datatype_str(type) +
          "' is not a valid dimension datatype.");
  }
}

/**
 * Throws and error if the input type is not supported on attributes with
 * increasing or decreasing order.
 */
inline void ensure_ordered_attribute_datatype_is_valid(Datatype type) {
  switch (type) {
    case Datatype::INT32:
    case Datatype::INT64:
    case Datatype::FLOAT32:
    case Datatype::FLOAT64:
    case Datatype::INT8:
    case Datatype::UINT8:
    case Datatype::INT16:
    case Datatype::UINT16:
    case Datatype::UINT32:
    case Datatype::UINT64:
    case Datatype::STRING_ASCII:
    case Datatype::DATETIME_YEAR:
    case Datatype::DATETIME_MONTH:
    case Datatype::DATETIME_WEEK:
    case Datatype::DATETIME_DAY:
    case Datatype::DATETIME_HR:
    case Datatype::DATETIME_MIN:
    case Datatype::DATETIME_SEC:
    case Datatype::DATETIME_MS:
    case Datatype::DATETIME_US:
    case Datatype::DATETIME_NS:
    case Datatype::DATETIME_PS:
    case Datatype::DATETIME_FS:
    case Datatype::DATETIME_AS:
    case Datatype::TIME_HR:
    case Datatype::TIME_MIN:
    case Datatype::TIME_SEC:
    case Datatype::TIME_MS:
    case Datatype::TIME_US:
    case Datatype::TIME_NS:
    case Datatype::TIME_PS:
    case Datatype::TIME_FS:
    case Datatype::TIME_AS:
      return;
    default:
      throw std::runtime_error(
          "Datatype '" + datatype_str(type) +
          "' is not a valid datatype for an ordered attribute.");
  }
}

}  // namespace tiledb::sm

#endif  // TILEDB_DATATYPE_H
