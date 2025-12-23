/**
 * @file parse_argument.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2025 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * This file implements useful (global) functions.
 */

#include "parse_argument.h"
#include "tiledb/common/unreachable.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/serialization_type.h"

#include <algorithm>
#include <charconv>
#include <iomanip>
#include <sstream>

using namespace tiledb::common;

namespace tiledb::sm::utils::parse {

/* ********************************* */
/*          PARSING FUNCTIONS        */
/* ********************************* */
template <typename T>
static Status convert(std::string_view str, T* value, const char* errdetail) {
  const auto rc = std::from_chars(str.begin(), str.end(), *value);
  if (rc.ptr != str.end() || rc.ec == std::errc::invalid_argument) {
    std::stringstream err;
    err << "Failed to convert string " << str << " to " << errdetail
        << "; Invalid argument";
    return LOG_STATUS(Status_UtilsError(err.str()));
  } else if (rc.ec == std::errc::result_out_of_range) {
    std::stringstream err;
    err << "Failed to convert string " << str << " to " << errdetail
        << "; Value out of range";
    return LOG_STATUS(Status_UtilsError(err.str()));
  } else {
    return Status::Ok();
  }
}

Status convert(std::string_view str, int* value) {
  return convert<int>(str, value, "int");
}

Status convert(std::string_view str, uint32_t* value) {
  return convert<uint32_t>(str, value, "uint32_t");
}

Status convert(std::string_view str, uint64_t* value) {
  return convert<uint64_t>(str, value, "uint64_t");
}

Status convert(std::string_view str, int64_t* value) {
  return convert<int64_t>(str, value, "int64_t");
}

Status convert(std::string_view str, float* value) {
  return convert<float>(str, value, "float32_t");
}

Status convert(std::string_view str, double* value) {
  return convert<double>(str, value, "float64_t");
}

Status convert(std::string_view str, bool* value) {
  std::string lvalue(str);
  std::transform(lvalue.begin(), lvalue.end(), lvalue.begin(), ::tolower);
  if (lvalue == "true") {
    *value = true;
  } else if (lvalue == "false") {
    *value = false;
  } else {
    return LOG_STATUS(Status_UtilsError(
        "Failed to convert string to bool; Value not 'true' or 'false'"));
  }

  return Status::Ok();
}

Status convert(std::string_view str, SerializationType* value) {
  std::string lvalue(str);
  std::transform(lvalue.begin(), lvalue.end(), lvalue.begin(), ::tolower);
  if (lvalue == "json") {
    *value = SerializationType::JSON;
  } else if (lvalue == "capnp") {
    *value = SerializationType::CAPNP;
  } else {
    return LOG_STATUS(
        Status_UtilsError("Failed to convert string to SerializationType; "
                          "Value not 'json' or 'capnp'"));
  }

  return Status::Ok();
}

template <class T>
std::string to_str(const T& value) {
  std::stringstream ss;
  ss << value;
  return ss.str();
}

/**
 * Helper function to print a code unit as a character if printable,
 * otherwise as a hex escape sequence.
 */
template <typename T>
void print_code_unit(std::stringstream& ss, T c) {
  constexpr int width = sizeof(T) * 2;
  if ((sizeof(T) == 1 || c < 0x80) &&
      std::isprint(static_cast<unsigned char>(c))) {
    ss << static_cast<char>(c);
  } else {
    ss << "\\x" << std::hex << std::setw(width) << std::setfill('0')
       << static_cast<unsigned int>(
              static_cast<typename std::make_unsigned<T>::type>(c));
  }
}

std::string to_str(const void* value, Datatype type) {
  std::stringstream ss;
  switch (type) {
    case Datatype::INT8:
      // cast to int32 to avoid char conversion to ASCII
      ss << static_cast<int32_t>(*(const int8_t*)value);
      break;
    case Datatype::UINT8:
      // cast to uint32 to avoid char conversion to ASCII
      ss << static_cast<uint32_t>(*(const uint8_t*)value);
      break;
    case Datatype::INT16:
      ss << *(const int16_t*)value;
      break;
    case Datatype::UINT16:
      ss << *(const uint16_t*)value;
      break;
    case Datatype::INT32:
      ss << *(const int32_t*)value;
      break;
    case Datatype::UINT32:
      ss << *(const uint32_t*)value;
      break;
    case Datatype::INT64:
      ss << *(const int64_t*)value;
      break;
    case Datatype::UINT64:
      ss << *(const uint64_t*)value;
      break;
    case Datatype::FLOAT32:
      ss << *(const float*)value;
      break;
    case Datatype::FLOAT64:
      ss << *(const double*)value;
      break;
    case Datatype::CHAR: {
      char c_char = *(const char*)value;
      print_code_unit(ss, c_char);
      break;
    }
    case Datatype::ANY: {
      uint8_t c_any = *(const uint8_t*)value;
      print_code_unit(ss, c_any);
      break;
    }
    case Datatype::STRING_ASCII: {
      uint8_t c_ascii = *(const uint8_t*)value;
      print_code_unit(ss, c_ascii);
      break;
    }
    case Datatype::STRING_UTF8: {
      uint8_t c_utf8 = *(const uint8_t*)value;
      print_code_unit(ss, c_utf8);
      break;
    }
    case Datatype::STRING_UTF16: {
      uint16_t c_utf16 = *(const uint16_t*)value;
      print_code_unit(ss, c_utf16);
      break;
    }
    case Datatype::STRING_UTF32: {
      uint32_t c_utf32 = *(const uint32_t*)value;
      print_code_unit(ss, c_utf32);
      break;
    }
    case Datatype::STRING_UCS2: {
      uint16_t c_ucs2 = *(const uint16_t*)value;
      print_code_unit(ss, c_ucs2);
      break;
    }
    case Datatype::STRING_UCS4: {
      uint32_t c_ucs4 = *(const uint32_t*)value;
      print_code_unit(ss, c_ucs4);
      break;
    }
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
      ss << *(const int64_t*)value;
      break;
    case Datatype::BLOB:
    case Datatype::GEOM_WKB:
    case Datatype::GEOM_WKT:
      // For printing to string use unsigned int value
      ss << *(const uint8_t*)value;
      break;
    case Datatype::BOOL:
      ss << *(const bool*)value;
      break;
    default:
      stdx::unreachable();
  }

  return ss.str();
}

template std::string to_str<int32_t>(const int32_t& value);
template std::string to_str<uint32_t>(const uint32_t& value);

}  // namespace tiledb::sm::utils::parse
