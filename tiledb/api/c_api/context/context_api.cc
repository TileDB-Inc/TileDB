/**
 * @file tiledb/api/c_api/error/context_api.cc
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
 * This file defines the context section of the C API for TileDB.
 */

#include "../config/config_api_internal.h"
#include "../filesystem/filesystem_api_external.h"
#include "context_api_external.h"
#include "context_api_internal.h"
#include "tiledb/api/c_api_support/c_api_support.h"

namespace tiledb::api {

bool save_error(tiledb_ctx_handle_t* ctx, const Status& st) {
  if (st.ok()) {
    // no error
    return false;
  }
  // there is an error
  ctx->context().save_error(st);
  return true;
}

capi_return_t tiledb_ctx_alloc(
    tiledb_config_handle_t* config, tiledb_ctx_handle_t** ctx) {
  ensure_output_pointer_is_valid(ctx);
  if (config == nullptr) {
    *ctx = tiledb_ctx_handle_t::make_handle(sm::Config{});
  } else {
    ensure_config_is_valid(config);
    *ctx = tiledb_ctx_handle_t::make_handle(config->config());
  }
  return TILEDB_OK;
}

void tiledb_ctx_free(tiledb_ctx_t** ctx) {
  ensure_output_pointer_is_valid(ctx);
  ensure_context_is_valid(*ctx);
  tiledb_ctx_handle_t::break_handle(*ctx);
}

capi_return_t tiledb_ctx_get_stats(
    tiledb_ctx_handle_t* ctx, char** stats_json) {
  ensure_output_pointer_is_valid(stats_json);

  const std::string str = ctx->context().resources().stats().dump(2, 0);

  *stats_json = static_cast<char*>(std::malloc(str.size() + 1));
  if (*stats_json == nullptr)
    return TILEDB_ERR;

  std::memcpy(*stats_json, str.data(), str.size());
  (*stats_json)[str.size()] = '\0';

  return TILEDB_OK;
}

capi_return_t tiledb_ctx_get_config(
    tiledb_ctx_handle_t* ctx, tiledb_config_handle_t** config) {
  api::ensure_output_pointer_is_valid(config);
  *config =
      tiledb_config_handle_t::make_handle(ctx->storage_manager()->config());
  return TILEDB_OK;
}

capi_return_t tiledb_ctx_get_last_error(
    tiledb_ctx_handle_t* ctx, tiledb_error_handle_t** err) {
  api::ensure_output_pointer_is_valid(err);
  auto last_error{ctx->last_error()};

  // No last error
  if (!last_error.has_value()) {
    *err = nullptr;
    return TILEDB_OK;
  }

  // Create error struct
  create_error(err, last_error.value());
  return TILEDB_OK;
}

capi_return_t tiledb_ctx_is_supported_fs(
    tiledb_ctx_t* ctx, tiledb_filesystem_t fs, int32_t* is_supported) {
  ensure_output_pointer_is_valid(is_supported);
  *is_supported = (int32_t)ctx->storage_manager()->vfs()->supports_fs(
      static_cast<tiledb::sm::Filesystem>(fs));
  return TILEDB_OK;
}

capi_return_t tiledb_ctx_cancel_tasks(tiledb_ctx_t* ctx) {
  throw_if_not_ok(ctx->storage_manager()->cancel_all_tasks());
  return TILEDB_OK;
}

capi_return_t tiledb_ctx_set_tag(
    tiledb_ctx_t* ctx, const char* key, const char* value) {
  if (key == nullptr) {
    throw CAPIStatusException("tiledb_ctx_set_tag: key may not be null");
  }
  if (value == nullptr) {
    throw CAPIStatusException("tiledb_ctx_set_tag: value may not be null");
  }
  throw_if_not_ok(ctx->storage_manager()->set_tag(key, value));
  return TILEDB_OK;
}

}  // namespace tiledb::api

using tiledb::api::api_entry_with_context;

/*
 * API Audit: No channel to return error message (failure code only)
 */
capi_return_t tiledb_ctx_alloc(
    tiledb_config_handle_t* config, tiledb_ctx_handle_t** ctx) noexcept {
  return tiledb::api::api_entry_plain<tiledb::api::tiledb_ctx_alloc>(
      config, ctx);
}

/*
 * We have a special case with tiledb_ctx_alloc_with_error. It's declared in
 * tiledb_experimental.h. Rather than all the apparatus needed to split up that
 * header (as tiledb.h is), we declare its linkage separately at the point of
 * definition.
 *
 * Not including the experimental header means that we're not using the compiler
 * to check the definition against the declaration. This won't scale
 * particularly well, but it doesn't need to for the time being.
 */
extern "C" {

capi_return_t tiledb_ctx_alloc_with_error(
    tiledb_config_handle_t* config,
    tiledb_ctx_handle_t** ctx,
    tiledb_error_handle_t** error) noexcept {
  /*
   * Wrapped with the `api_entry_error` variation. Note that the same function
   * is wrapped with `api_entry_plain` above.
   */
  return tiledb::api::api_entry_error<tiledb::api::tiledb_ctx_alloc>(
      error, config, ctx);
}

}  // extern "C"

/*
 * API Audit: void return
 */
void tiledb_ctx_free(tiledb_ctx_handle_t** ctx) noexcept {
  return tiledb::api::api_entry_void<tiledb::api::tiledb_ctx_free>(ctx);
}

capi_return_t tiledb_ctx_get_stats(
    tiledb_ctx_t* ctx, char** stats_json) noexcept {
  return api_entry_with_context<tiledb::api::tiledb_ctx_get_stats>(
      ctx, stats_json);
}

capi_return_t tiledb_ctx_get_config(
    tiledb_ctx_t* ctx, tiledb_config_handle_t** config) noexcept {
  return api_entry_with_context<tiledb::api::tiledb_ctx_get_config>(
      ctx, config);
}

capi_return_t tiledb_ctx_get_last_error(
    tiledb_ctx_t* ctx, tiledb_error_handle_t** err) noexcept {
  return api_entry_with_context<tiledb::api::tiledb_ctx_get_last_error>(
      ctx, err);
}

capi_return_t tiledb_ctx_is_supported_fs(
    tiledb_ctx_t* ctx, tiledb_filesystem_t fs, int32_t* is_supported) noexcept {
  return api_entry_with_context<tiledb::api::tiledb_ctx_is_supported_fs>(
      ctx, fs, is_supported);
}

capi_return_t tiledb_ctx_cancel_tasks(tiledb_ctx_t* ctx) noexcept {
  return api_entry_with_context<tiledb::api::tiledb_ctx_cancel_tasks>(ctx);
}

capi_return_t tiledb_ctx_set_tag(
    tiledb_ctx_t* ctx, const char* key, const char* value) noexcept {
  return api_entry_with_context<tiledb::api::tiledb_ctx_set_tag>(
      ctx, key, value);
}
