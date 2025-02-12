/**
 * @file parse_argument.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
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
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/serialization_type.h"

#include <algorithm>
#include <sstream>

using namespace tiledb::common;

namespace tiledb::sm::utils::parse {

/* ********************************* */
/*          PARSING FUNCTIONS        */
/* ********************************* */

Status convert(const std::string& str, int* value) {
  if (!is_int(str)) {
    auto errmsg = std::string("Failed to convert string '") + str +
                  "' to int; Invalid argument";
    return LOG_STATUS(Status_UtilsError(errmsg));
  }

  try {
    *value = std::stoi(str);
  } catch (std::invalid_argument& e) {
    auto errmsg = std::string("Failed to convert string '") + str +
                  "' to int; Invalid argument";
    return LOG_STATUS(Status_UtilsError(errmsg));
  } catch (std::out_of_range& e) {
    auto errmsg = std::string("Failed to convert string '") + str +
                  "' to int; Value out of range";
    return LOG_STATUS(Status_UtilsError(errmsg));
  }

  return Status::Ok();
}

Status convert(const std::string& str, uint32_t* value) {
  if (!is_uint(str)) {
    auto errmsg = std::string("Failed to convert string '") + str +
                  "' to uint32_t; Invalid argument";
    return LOG_STATUS(Status_UtilsError(errmsg));
  }

  try {
    auto v = std::stoul(str);
    if (v > UINT32_MAX)
      throw std::out_of_range("Cannot convert long to unsigned int");
    *value = (uint32_t)v;
  } catch (std::invalid_argument& e) {
    auto errmsg = std::string("Failed to convert string '") + str +
                  "' to uint32_t; Invalid argument";
    return LOG_STATUS(Status_UtilsError(errmsg));
  } catch (std::out_of_range& e) {
    auto errmsg = std::string("Failed to convert string '") + str +
                  "' to uint32_t; Value out of range";
    return LOG_STATUS(Status_UtilsError(errmsg));
  }

  return Status::Ok();
}

Status convert(const std::string& str, uint64_t* value) {
  if (!is_uint(str)) {
    auto errmsg = std::string("Failed to convert string '") + str +
                  "' to uint64_t; Invalid argument";
    return LOG_STATUS(Status_UtilsError(errmsg));
  }

  try {
    *value = std::stoull(str);
  } catch (std::invalid_argument& e) {
    auto errmsg = std::string("Failed to convert string '") + str +
                  "' to uint64_t; Invalid argument";
    return LOG_STATUS(Status_UtilsError(errmsg));
  } catch (std::out_of_range& e) {
    auto errmsg = std::string("Failed to convert string '") + str +
                  "' to uint64_t; Value out of range";
    return LOG_STATUS(Status_UtilsError(errmsg));
  }

  return Status::Ok();
}

Status convert(const std::string& str, int64_t* value) {
  if (!is_int(str)) {
    auto errmsg = std::string("Failed to convert string '") + str +
                  "' to int64_t; Invalid argument";
    return LOG_STATUS(Status_UtilsError(errmsg));
  }

  try {
    *value = std::stoll(str);
  } catch (std::invalid_argument& e) {
    auto errmsg = std::string("Failed to convert string '") + str +
                  "' to int64_t; Invalid argument";
    return LOG_STATUS(Status_UtilsError(errmsg));
  } catch (std::out_of_range& e) {
    auto errmsg = std::string("Failed to convert string '") + str +
                  "' to int64_t; Value out of range";
    return LOG_STATUS(Status_UtilsError(errmsg));
  }

  return Status::Ok();
}

Status convert(const std::string& str, float* value) {
  try {
    *value = std::stof(str);
  } catch (std::invalid_argument& e) {
    return LOG_STATUS(Status_UtilsError(
        "Failed to convert string to float32_t; Invalid argument"));
  } catch (std::out_of_range& e) {
    return LOG_STATUS(Status_UtilsError(
        "Failed to convert string to float32_t; Value out of range"));
  }

  return Status::Ok();
}

Status convert(const std::string& str, double* value) {
  try {
    *value = std::stod(str);
  } catch (std::invalid_argument& e) {
    return LOG_STATUS(Status_UtilsError(
        "Failed to convert string to float64_t; Invalid argument"));
  } catch (std::out_of_range& e) {
    return LOG_STATUS(Status_UtilsError(
        "Failed to convert string to float64_t; Value out of range"));
  }

  return Status::Ok();
}

Status convert(const std::string& str, bool* value) {
  std::string lvalue = str;
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

Status convert(const std::string& str, SerializationType* value) {
  std::string lvalue = str;
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

bool is_int(const std::string& str) {
  // Check if empty
  if (str.empty())
    return false;

  // Check first character
  if (str[0] != '+' && str[0] != '-' && !(bool)isdigit(str[0]))
    return false;

  // Check rest of characters
  for (size_t i = 1; i < str.size(); ++i)
    if (!(bool)isdigit(str[i]))
      return false;

  return true;
}

bool is_uint(const std::string& str) {
  // Check if empty
  if (str.empty())
    return false;

  // Check first character
  if (str[0] != '+' && !isdigit(str[0]))
    return false;

  // Check characters
  for (size_t i = 1; i < str.size(); ++i)
    if (!(bool)isdigit(str[i]))
      return false;

  return true;
}

template <class T>
std::string to_str(const T& value) {
  std::stringstream ss;
  ss << value;
  return ss.str();
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
    case Datatype::CHAR:
      ss << *(const char*)value;
      break;
    case Datatype::ANY:
      ss << *(const uint8_t*)value;
      break;
    case Datatype::STRING_ASCII:
      ss << *(const uint8_t*)value;
      break;
    case Datatype::STRING_UTF8:
      ss << *(const uint8_t*)value;
      break;
    case Datatype::STRING_UTF16:
      ss << *(const uint16_t*)value;
      break;
    case Datatype::STRING_UTF32:
      ss << *(const uint32_t*)value;
      break;
    case Datatype::STRING_UCS2:
      ss << *(const uint16_t*)value;
      break;
    case Datatype::STRING_UCS4:
      ss << *(const uint32_t*)value;
      break;
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
      assert(false);
  }

  return ss.str();
}

template std::string to_str<int32_t>(const int32_t& value);
template std::string to_str<uint32_t>(const uint32_t& value);

}  // namespace tiledb::sm::utils::parse
