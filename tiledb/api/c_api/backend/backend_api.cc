/**
 * @file tiledb/api/c_api/backend/backend_api.cc
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
 * This file defines the backend section of the C API for TileDB.
 */

#include "backend_api_external.h"
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/api/c_api_support/c_api_support.h"
#include "tiledb/sm/filesystem/uri.h"
#include "tiledb/sm/rest/rest_client.h"

namespace tiledb::api {

capi_return_t tiledb_uri_get_backend_protocol(
    tiledb_ctx_t* ctx, const char* uri, tiledb_backend_t* uri_backend) {
  ensure_output_pointer_is_valid(uri_backend);

  auto uri_to_check = tiledb::sm::URI(uri);
  if (uri_to_check.is_invalid()) {
    throw CAPIException("Cannot get backend name; Invalid URI");
  }

  if (tiledb::sm::URI::is_s3(uri)) {
    *uri_backend = TILEDB_BACKEND_S3;
  } else if (tiledb::sm::URI::is_azure(uri)) {
    *uri_backend = TILEDB_BACKEND_AZURE;
  } else if (tiledb::sm::URI::is_gcs(uri)) {
    *uri_backend = TILEDB_BACKEND_GCS;
  } else if (tiledb::sm::URI::is_tiledb(uri)) {
    if (ctx->rest_client().rest_legacy()) {
      *uri_backend = TILEDB_BACKEND_TILEDB_v1;
    } else {
      *uri_backend = TILEDB_BACKEND_TILEDB_v2;
    }
  } else {
    throw CAPIException("Cannot get backend name; Unknown backend");
  }

  return TILEDB_OK;
}

}  // namespace tiledb::api

using tiledb::api::api_entry_with_context;

CAPI_INTERFACE(
    uri_get_backend_name,
    tiledb_ctx_t* ctx,
    const char* uri,
    tiledb_backend_t* uri_backend) {
  return api_entry_with_context<tiledb::api::tiledb_uri_get_backend_protocol>(
      ctx, uri, uri_backend);
}
