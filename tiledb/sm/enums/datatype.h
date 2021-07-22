/**
 * @file datatype.h
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
 * This defines the tiledb Datatype enum that maps to tiledb_datatype_t C-api
 * enum.
 */

#ifndef TILEDB_DATATYPE_H
#define TILEDB_DATATYPE_H

#include "tiledb/common/status.h"
#include "tiledb/sm/misc/constants.h"

#include <cassert>

using namespace tiledb::common;

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
  else if (datatype_str == constants::datetime_year_str)
    *datatype = Datatype::DATETIME_YEAR;
  else if (datatype_str == constants::datetime_month_str)
    *datatype = Datatype::DATETIME_MONTH;
  else if (datatype_str == constants::datetime_week_str)
    *datatype = Datatype::DATETIME_WEEK;
  else if (datatype_str == constants::datetime_day_str)
    *datatype = Datatype::DATETIME_DAY;
  else if (datatype_str == constants::datetime_hr_str)
    *datatype = Datatype::DATETIME_HR;
  else if (datatype_str == constants::datetime_min_str)
    *datatype = Datatype::DATETIME_MIN;
  else if (datatype_str == constants::datetime_sec_str)
    *datatype = Datatype::DATETIME_SEC;
  else if (datatype_str == constants::datetime_ms_str)
    *datatype = Datatype::DATETIME_MS;
  else if (datatype_str == constants::datetime_us_str)
    *datatype = Datatype::DATETIME_US;
  else if (datatype_str == constants::datetime_ns_str)
    *datatype = Datatype::DATETIME_NS;
  else if (datatype_str == constants::datetime_ps_str)
    *datatype = Datatype::DATETIME_PS;
  else if (datatype_str == constants::datetime_fs_str)
    *datatype = Datatype::DATETIME_FS;
  else if (datatype_str == constants::datetime_as_str)
    *datatype = Datatype::DATETIME_AS;
  else if (datatype_str == constants::time_hr_str)
    *datatype = Datatype::TIME_HR;
  else if (datatype_str == constants::time_min_str)
    *datatype = Datatype::TIME_MIN;
  else if (datatype_str == constants::time_sec_str)
    *datatype = Datatype::TIME_SEC;
  else if (datatype_str == constants::time_ms_str)
    *datatype = Datatype::TIME_MS;
  else if (datatype_str == constants::time_us_str)
    *datatype = Datatype::TIME_US;
  else if (datatype_str == constants::time_ns_str)
    *datatype = Datatype::TIME_NS;
  else if (datatype_str == constants::time_ps_str)
    *datatype = Datatype::TIME_PS;
  else if (datatype_str == constants::time_fs_str)
    *datatype = Datatype::TIME_FS;
  else if (datatype_str == constants::time_as_str)
    *datatype = Datatype::TIME_AS;
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

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_DATATYPE_H
