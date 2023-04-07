/**
 * @file tiledb/api/c_api/fragments_list/fragments_list_api.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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
 * This file defines the fragments list section of the C API for TileDB.
 */

#include "fragments_list_api_external.h"
#include "fragments_list_api_internal.h"
#include "tiledb/api/c_api_support/c_api_support.h"

namespace tiledb::api {

capi_return_t tiledb_fragments_list_get_fragment_uri(
    tiledb_fragments_list_t* f,
    uint32_t index,
    const char** uri,
    size_t* uri_length) {
  ensure_fragments_list_is_valid(f);
  ensure_output_pointer_is_valid(uri);

  *uri = f->fragment_uri(index).c_str();
  *uri_length = strlen(f->fragment_uri(index).last_path_part().c_str());
  return TILEDB_OK;
}

capi_return_t tiledb_fragments_list_get_fragment_index(
    tiledb_fragments_list_t* f, const char* uri, uint32_t* index) {
  ensure_fragments_list_is_valid(f);
  ensure_output_pointer_is_valid(index);

  *index = static_cast<uint32_t>(f->fragment_index(uri));
  return TILEDB_OK;
}

void tiledb_fragments_list_free(tiledb_fragments_list_t** f) {
  ensure_output_pointer_is_valid(f);
  ensure_fragments_list_is_valid(*f);
  tiledb_fragments_list_handle_t::break_handle(*f);
}

}  // namespace tiledb::api

using tiledb::api::api_entry_plain;
using tiledb::api::api_entry_void;

capi_return_t tiledb_fragments_list_get_fragment_uri(
    tiledb_fragments_list_t* f,
    uint32_t index,
    const char** uri,
    size_t* uri_length) noexcept {
  return api_entry_plain<tiledb::api::tiledb_fragments_list_get_fragment_uri>(
      f, index, uri, uri_length);
}

capi_return_t tiledb_fragments_list_get_fragment_index(
    tiledb_fragments_list_t* f, const char* uri, uint32_t* index) noexcept {
  return api_entry_plain<tiledb::api::tiledb_fragments_list_get_fragment_index>(
      f, uri, index);
}

void tiledb_fragments_list_free(tiledb_fragments_list_t** f) noexcept {
  return api_entry_void<tiledb::api::tiledb_fragments_list_free>(f);
}
