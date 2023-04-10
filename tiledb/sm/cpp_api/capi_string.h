/**
 * @file   capi_string.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2023 TileDB, Inc.
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
 * This file declares the C++ API for the TileDB CAPIString object.
 */

#ifndef TILEDB_CPP_API_CAPI_STRING_H
#define TILEDB_CPP_API_CAPI_STRING_H

#include <cassert>
#include <optional>
#include <stdexcept>
#include "tiledb.h"

namespace tiledb::impl {

/**
 * Manages the lifetime of a tiledb_string_t* handle and provides operations on
 * it.
 */
class CAPIString {
 public:
  CAPIString(tiledb_string_t*&& handle) noexcept {
    string_ = handle;
    handle = nullptr;
  }
  ~CAPIString() {
    if (string_ == nullptr) {
      return;
    }

    capi_status_t result = tiledb_status(tiledb_string_free(&string_));
    if (result != TILEDB_OK) {
      log_warn("Could not free string; Error code: " + std::to_string(result));
    }
  }

  // Disable copy and move.
  CAPIString(const CAPIString&) = delete;
  CAPIString operator=(const CAPIString&) = delete;
  CAPIString(const CAPIString&&) = delete;
  CAPIString operator=(const CAPIString&&) = delete;

  /**
   * Returns a C++ string with the handle's data.
   *
   * If the handle is null returns nullopt.
   */
  static std::optional<std::string> to_string_optional(
      tiledb_string_t*&& handle) {
    if (handle == nullptr) {
      return std::nullopt;
    }
    return to_string(std::move(handle));
  }

  /**
   * Returns a C++ string with the handle's data.
   */
  static std::string to_string(tiledb_string_t*&& handle) {
    return CAPIString(std::move(handle)).str();
  }

  std::string str() const {
    const char* c;
    size_t size;
    capi_status_t status =
        tiledb_status(tiledb_string_view(string_, &c, &size));
    if (status != TILEDB_OK) {
      throw std::runtime_error(
          "Could not view string; Error code: " + std::to_string(status));
    }
    return std::string(c, size);
  }

 private:
  tiledb_string_t* string_;
};

}  // namespace tiledb::impl

#endif  // TILEDB_CPP_API_CAPI_STRING_H
