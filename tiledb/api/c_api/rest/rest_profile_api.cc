/**
 * @file tiledb/api/c_api/rest/rest_profile_api.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2025 TileDB, Inc.
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
 * This file defines C API functions for the rest_profile section.
 */

#include "rest_profile_api_internal.h"
#include "tiledb/api/c_api_support/c_api_support.h"

namespace tiledb::api {

capi_return_t tiledb_rest_profile_alloc(
    const char* name, tiledb_rest_profile_t** rest_profile) {
  ensure_output_pointer_is_valid(rest_profile);

  // Create RestProfile object
  if (name) {
    *rest_profile = tiledb_rest_profile_t::make_handle(std::string(name));
  } else {
    *rest_profile = tiledb_rest_profile_t::make_handle();
  }
  return TILEDB_OK;
}

capi_return_t tiledb_rest_profile_alloc_test(
    const char* name,
    const char* homedir,
    tiledb_rest_profile_t** rest_profile) {
  ensure_output_pointer_is_valid(rest_profile);
  if (!name || std::string(name).empty()) {
    throw std::invalid_argument(
        "[tiledb_rest_profile_alloc_test] Invalid name.");
  }

  if (!homedir || std::string(homedir).empty()) {
    throw std::invalid_argument(
        "[tiledb_rest_profile_alloc_test] Invalid $HOME directory.");
  }

  // Create RestProfile object
  *rest_profile = tiledb_rest_profile_t::make_handle(
      std::string(name), std::string(homedir));
  return TILEDB_OK;
}

void tiledb_rest_profile_free(tiledb_rest_profile_t** rest_profile) {
  ensure_output_pointer_is_valid(rest_profile);
  ensure_rest_profile_is_valid(*rest_profile);
  tiledb_rest_profile_t::break_handle(*rest_profile);
}

}  // namespace tiledb::api

using tiledb::api::api_entry_plain;
using tiledb::api::api_entry_void;

CAPI_INTERFACE(
    rest_profile_alloc,
    tiledb_rest_profile_t** rest_profile,
    const char* name) {
  return api_entry_plain<tiledb::api::tiledb_rest_profile_alloc>(
      name, rest_profile);
}

CAPI_INTERFACE(
    rest_profile_alloc_test,
    const char* name,
    const char* homedir,
    tiledb_rest_profile_t** rest_profile) {
  return api_entry_plain<tiledb::api::tiledb_rest_profile_alloc_test>(
      name, homedir, rest_profile);
}

CAPI_INTERFACE_VOID(rest_profile_free, tiledb_rest_profile_t** rest_profile) {
  return api_entry_void<tiledb::api::tiledb_rest_profile_free>(rest_profile);
}
