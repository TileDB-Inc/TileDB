/**
 * @file tiledb/api/c_api/array/array_api.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * This file defines C API functions for the array section.
 */

#include "array_api_internal.h"
#include "tiledb/sm/c_api/tiledb.h"

#include "tiledb/api/c_api/array_schema/array_schema_api_internal.h"
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/api/c_api_support/c_api_support.h"
#include "tiledb/sm/array/array_directory.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/rest/rest_client.h"

namespace tiledb::api {

capi_return_t tiledb_array_schema_load(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_array_schema_t** array_schema) {
  ensure_output_pointer_is_valid(array_schema);

  auto uri = tiledb::sm::URI(array_uri);
  if (uri.is_invalid()) {
    throw CAPIException("Invalid input uri.");
  }

  if (uri.is_tiledb()) {
    auto& rest_client = ctx->context().rest_client();
    auto&& [st, array_schema_rest] =
        rest_client.get_array_schema_from_rest(uri);
    if (!st.ok()) {
      throw CAPIException("Failed to load array schema; " + st.message());
    }
    *array_schema =
        tiledb_array_schema_t::make_handle(*(array_schema_rest->get()));
  } else {
    // Create key
    tiledb::sm::EncryptionKey key;
    throw_if_not_ok(
        key.set_key(tiledb::sm::EncryptionType::NO_ENCRYPTION, nullptr, 0));

    // Load URIs from the array directory
    optional<tiledb::sm::ArrayDirectory> array_dir;
    try {
      array_dir.emplace(
          ctx->resources(),
          uri,
          0,
          UINT64_MAX,
          tiledb::sm::ArrayDirectoryMode::SCHEMA_ONLY);
    } catch (const std::logic_error& le) {
      throw CAPIException(
          "Failed to load array schema; " +
          Status_ArrayDirectoryError(le.what()).message());
    }

    // Load latest array schema
    auto tracker = ctx->resources().ephemeral_memory_tracker();
    auto&& array_schema_latest =
        array_dir->load_array_schema_latest(key, tracker);
    *array_schema =
        tiledb_array_schema_t::make_handle(*(array_schema_latest.get()));
  }
  return TILEDB_OK;
}

}  // namespace tiledb::api

using tiledb::api::api_entry_with_context;

CAPI_INTERFACE(
    array_schema_load,
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_array_schema_t** array_schema) {
  return api_entry_with_context<tiledb::api::tiledb_array_schema_load>(
      ctx, array_uri, array_schema);
}
