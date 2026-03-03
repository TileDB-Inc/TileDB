/**
 * @file tiledb/api/c_api/error/error_api.cc
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
 * This file defines the error section of the C API for TileDB.
 */

#include "error_api_external.h"
#include "error_api_internal.h"
#include "tiledb/api/c_api_support/c_api_support.h"

namespace tiledb::api {

void create_error(tiledb_error_handle_t** error, const std::string& message) {
  *error = make_handle<tiledb_error_handle_t>(message);
}

capi_return_t tiledb_error_message(
    tiledb_error_handle_t* err, const char** errmsg) {
  ensure_error_is_valid(err);
  ensure_output_pointer_is_valid(errmsg);

  const auto& error_message{err->message()};
  if (error_message.empty()) {
    *errmsg = nullptr;
  } else {
    *errmsg = error_message.c_str();
  }
  return TILEDB_OK;
}

void tiledb_error_free(tiledb_error_handle_t** err) {
  ensure_output_pointer_is_valid(err);
  ensure_error_is_valid(*err);
  break_handle(*err);
}

}  // namespace tiledb::api

CAPI_INTERFACE(error_message, tiledb_error_handle_t* err, const char** errmsg) {
  return tiledb::api::api_entry_plain<tiledb::api::tiledb_error_message>(
      err, errmsg);
}

CAPI_INTERFACE_VOID(error_free, tiledb_error_handle_t** err) {
  return tiledb::api::api_entry_void<tiledb::api::tiledb_error_free>(err);
}
