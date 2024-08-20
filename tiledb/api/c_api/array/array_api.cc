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
#include "tiledb/sm/array_schema/array_schema_operations.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/rest/rest_client.h"

namespace tiledb::api {

int32_t tiledb_array_schema_load(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_array_schema_t** array_schema) {
  // Create array schema
  ensure_context_is_valid(ctx);
  ensure_output_pointer_is_valid(array_schema);

  // Use a default constructed config to load the schema with default options.
  *array_schema = tiledb_array_schema_t::make_handle(
      load_array_schema(ctx->context(), sm::URI(array_uri), sm::Config()));

  return TILEDB_OK;
}

int32_t tiledb_array_schema_load_with_config(
    tiledb_ctx_t* ctx,
    tiledb_config_t* config,
    const char* array_uri,
    tiledb_array_schema_t** array_schema) {
  ensure_context_is_valid(ctx);
  ensure_config_is_valid(config);
  ensure_output_pointer_is_valid(array_schema);

  // Use passed config or context config to load the schema with set options.
  *array_schema = tiledb_array_schema_t::make_handle(load_array_schema(
      ctx->context(),
      sm::URI(array_uri),
      config ? config->config() : ctx->config()));

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

CAPI_INTERFACE(
    array_schema_load_with_config,
    tiledb_ctx_t* ctx,
    tiledb_config_t* config,
    const char* array_uri,
    tiledb_array_schema_t** array_schema) {
  return api_entry_with_context<
      tiledb::api::tiledb_array_schema_load_with_config>(
      ctx, config, array_uri, array_schema);
}
