/**
 * @file tiledb/api/c_api/error/error_api_internal.h
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
 * This file contains internal implementation details of the error section of
 * the C API for TileDB.
 */

#ifndef TILEDB_CAPI_ERROR_INTERNAL_H
#define TILEDB_CAPI_ERROR_INTERNAL_H

#include "tiledb/api/c_api_support/handle/handle.h"

/**
 * Handle `struct` for API error objects.
 */
struct tiledb_error_handle_t : public tiledb::api::CAPIHandle {
  /**
   * Type name
   */
  static constexpr std::string_view object_type_name{"error"};

 private:
  /**
   * The content of an error object is only a string.
   */
  std::string errmsg_;

 public:
  /**
   * Default constructor is deleted. An error without a message makes no sense.
   */
  tiledb_error_handle_t() = delete;

  /**
   * Ordinary constructor.
   * @param message An error message
   */
  explicit tiledb_error_handle_t(const std::string& message)
      : errmsg_{message} {
  }

  [[nodiscard]] inline const std::string& message() const {
    return errmsg_;
  }
};

namespace tiledb::api {
/*
 * Create a C API error object with a given string.
 *
 * Used by the closest thing an error has to a `*_alloc` function,
 * `tiledb_ctx_get_last_error`. The error that a context stores is not an API
 * handle, but an underlying error object. This function creates that handle.
 *
 * @pre `error != nullptr`. Error arguments must always be validated, because
 * on error they're assigned an error handle and on success they're assigned
 * `nullptr`.
 *
 * @param error A non-null pointer to `tiledb_error_t *` object
 * @param message An error message
 */
void create_error(tiledb_error_handle_t** error, const std::string& message);

/**
 * Returns after successfully validating an error. Throws otherwise.
 *
 * @param error Possibly-valid pointer to an error
 */
inline void ensure_error_is_valid(const tiledb_error_handle_t* error) {
  ensure_handle_is_valid(error);
}

}  // namespace tiledb::api

#endif  // TILEDB_CAPI_ERROR_INTERNAL_H
