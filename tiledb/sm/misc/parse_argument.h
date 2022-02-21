/**
 * @file parse_argument.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * This file contains useful (global) functions.
 */

#ifndef TILEDB_UTILS_H
#define TILEDB_UTILS_H

#include <cassert>
#include <cmath>
#include <string>
#include <type_traits>
#include <vector>

#include "tiledb/common/logger_public.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/types.h"

using namespace tiledb::common;

namespace tiledb::sm {
enum class Datatype : uint8_t;
}
namespace tiledb::sm::utils::parse {

/* ********************************* */
/*          PARSING FUNCTIONS        */
/* ********************************* */

/** Converts the input string into an `int` value. */
Status convert(const std::string& str, int* value);

/** Converts the input string into an `int64_t` value. */
Status convert(const std::string& str, int64_t* value);

/** Converts the input string into a `uint64_t` value. */
Status convert(const std::string& str, uint64_t* value);

/** Converts the input string into a `uint32_t` value. */
Status convert(const std::string& str, uint32_t* value);

/** Converts the input string into a `float` value. */
Status convert(const std::string& str, float* value);

/** Converts the input string into a `double` value. */
Status convert(const std::string& str, double* value);

/** Converts the input string into a `bool` value. */
Status convert(const std::string& str, bool* value);

/** Converts the input string into a `SerializationType` value. */
Status convert(const std::string& str, SerializationType* value);

/** Converts the input string into a `std::vector<T>` value. */
template <class T>
Status convert(const std::string& str, std::vector<T>* value) {
  try {
    uint64_t start = 0;
    auto end = str.find(constants::config_delimiter);
    do {
      T v;
      RETURN_NOT_OK(convert(str.substr(start, end - start), &v));
      value->emplace_back(v);
      start = end + constants::config_delimiter.length();
      end = str.find(constants::config_delimiter, start);
    } while (end != std::string::npos);

  } catch (std::invalid_argument& e) {
    return LOG_STATUS(Status_UtilsError(
        "Failed to convert string to vector of " +
        std::string(typeid(T).name()) + "; Invalid argument"));
  } catch (std::out_of_range& e) {
    return LOG_STATUS(Status_UtilsError(
        "Failed to convert string to vector of " +
        std::string(typeid(T).name()) + "; Value out of range"));
  }

  return Status::Ok();
}

/** Returns `true` if the input string is a (potentially signed) integer. */
bool is_int(const std::string& str);

/** Returns `true` if the input string is an unsigned integer. */
bool is_uint(const std::string& str);

/** Converts the input value to string. */
template <class T>
std::string to_str(const T& value);

/** Converts the input value of input type to string. */
std::string to_str(const void* value, Datatype type);

}  // namespace tiledb::sm::utils::parse

#endif  // TILEDB_UTILS_H
