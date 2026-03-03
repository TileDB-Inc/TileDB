/**
 * @file tiledb/api/c_api/string/string_api.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2024 TileDB, Inc.
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
 * This file defines the string section of the C API for TileDB.
 */

#include "string_api_external.h"
#include "string_api_internal.h"
#include "tiledb/api/c_api_support/c_api_support.h"

namespace tiledb::api {

capi_return_t tiledb_string_view(
    tiledb_string_handle_t* s, const char** data, size_t* length) {
  ensure_string_is_valid(s);
  ensure_output_pointer_is_valid(data);
  ensure_output_pointer_is_valid(length);
  auto sv{s->view()};
  *data = sv.data();
  *length = sv.length();
  return TILEDB_OK;
}

capi_return_t tiledb_string_free(tiledb_string_handle_t** s) {
  ensure_output_pointer_is_valid(s);
  ensure_string_is_valid(*s);
  break_handle(*s);
  return TILEDB_OK;
}

}  // namespace tiledb::api

CAPI_INTERFACE(
    string_view, tiledb_string_handle_t* s, const char** data, size_t* length) {
  return tiledb::api::api_entry_plain<tiledb::api::tiledb_string_view>(
      s, data, length);
}

CAPI_INTERFACE(string_free, tiledb_string_handle_t** s) {
  return tiledb::api::api_entry_plain<tiledb::api::tiledb_string_free>(s);
}
