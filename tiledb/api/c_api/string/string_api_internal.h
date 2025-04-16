/**
 * @file tiledb/api/c_api/string/string_api_internal.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * This file contains internal implementation details of the string section of
 * the C API for TileDB.
 */

#ifndef TILEDB_CAPI_STRING_INTERNAL_H
#define TILEDB_CAPI_STRING_INTERNAL_H

#include "tiledb/api/c_api_support/handle/handle.h"

/**
 * Handle `struct` for API string objects.
 */
struct tiledb_string_handle_t
    : public tiledb::api::CAPIHandle<tiledb_string_handle_t> {
  /**
   * Type name
   */
  static constexpr std::string_view object_type_name{"string"};

 private:
  /**
   * The content of an string object is a std::string. Not quite duh, but close.
   */
  std::string value_;

 public:
  /**
   * Default constructor constructs an empty string
   */
  tiledb_string_handle_t()
      : value_{} {
  }

  /**
   * Ordinary constructor.
   * @param s A string
   */
  explicit tiledb_string_handle_t(const std::string_view s)
      : value_{s} {
  }

  [[nodiscard]] inline const std::string_view view() const {
    return std::string_view{value_};
  }
};

namespace tiledb::api {

/**
 * Returns after successfully validating a string handle. Throws otherwise.
 *
 * @param string Possibly-valid pointer to a string handle
 */
inline void ensure_string_is_valid(const tiledb_string_handle_t* string) {
  ensure_handle_is_valid(string);
}

}  // namespace tiledb::api

#endif  // TILEDB_CAPI_STRING_INTERNAL_H
