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

using namespace tiledb::common;

namespace tiledb::sm {
enum class Datatype : uint8_t;
}
namespace tiledb::sm::utils::parse {

/* ********************************* */
/*          PARSING FUNCTIONS        */
/* ********************************* */

/** Converts the input string into an `int` value. */
Status convert(std::string_view str, int* value);

/** Converts the input string into an `int64_t` value. */
Status convert(std::string_view str, int64_t* value);

/** Converts the input string into a `uint64_t` value. */
Status convert(std::string_view str, uint64_t* value);

/** Converts the input string into a `uint32_t` value. */
Status convert(std::string_view str, uint32_t* value);

/** Converts the input string into a `float` value. */
Status convert(std::string_view str, float* value);

/** Converts the input string into a `double` value. */
Status convert(std::string_view str, double* value);

/** Converts the input string into a `bool` value. */
Status convert(std::string_view str, bool* value);

/** Converts the input string into a `SerializationType` value. */
Status convert(std::string_view str, SerializationType* value);

/** Converts the input string into a `std::vector<T>` value. */
template <class T>
Status convert(std::string_view str, std::vector<T>* value) {
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

template <class T>
concept Convertible = requires(const std::string& str, T x) {
  { convert(str, &x) } -> std::convertible_to<Status>;
};

/** Converts the input string into an std::optional value. Returns nullopt if
 * the input string is empty. */
template <Convertible T>
std::optional<T> convert_optional(const std::string& str) {
  if (str.empty()) {
    return nullopt;
  }
  T result;
  throw_if_not_ok(convert(str, &result));
  return result;
}

/** Converts the input value to string. */
template <class T>
std::string to_str(const T& value);

/** Converts the input value of input type to string. */
std::string to_str(const void* value, Datatype type);

}  // namespace tiledb::sm::utils::parse

#endif  // TILEDB_UTILS_H
