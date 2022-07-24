/**
 * @file tiledb/api/c_api/context/context_internal.h
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
 * This file declares the C API for TileDB.
 */

#ifndef TILEDB_CAPI_CONTEXT_INTERNAL_H
#define TILEDB_CAPI_CONTEXT_INTERNAL_H

#include "../api_external_common.h"
#include "../config/config_api_external.h"
#include "../error/error_api_external.h"
#include "context_api_external.h"
#include "tiledb/sm/storage_manager/context.h"

/**
 * Forward declarations for context carrier.
 */
namespace tiledb::api {
void ensure_context_is_valid_enough_for_errors(tiledb_ctx_t*);
void ensure_context_is_fully_valid(tiledb_ctx_t*);
}  // namespace tiledb::api
namespace tiledb::common::detail {
int32_t tiledb_ctx_alloc(tiledb_config_t*, tiledb_ctx_t**);
void tiledb_ctx_free(tiledb_ctx_t**);
int32_t tiledb_ctx_get_stats(tiledb_ctx_t*, char**);
}  // namespace tiledb::common::detail
extern "C" {
TILEDB_EXPORT int32_t tiledb_ctx_alloc_with_error(
    tiledb_config_t*, tiledb_ctx_t**, tiledb_error_t**) noexcept;
}

struct tiledb_ctx_handle_t {
  friend void tiledb::api::ensure_context_is_valid_enough_for_errors(
      tiledb_ctx_t*);
  friend void tiledb::api::ensure_context_is_fully_valid(tiledb_ctx_t*);
  friend bool save_error(tiledb_ctx_t*, const Status&);
  friend int32_t tiledb::common::detail::tiledb_ctx_alloc(
      tiledb_config_t*, tiledb_ctx_t**);
  friend void tiledb::common::detail::tiledb_ctx_free(tiledb_ctx_t**);
  friend int32_t tiledb::common::detail::tiledb_ctx_get_stats(
      tiledb_ctx_t*, char**);
  friend int32_t tiledb_ctx_alloc_with_error(
      tiledb_config_t*, tiledb_ctx_t**, tiledb_error_t**) noexcept;

 private:
  tiledb::sm::Context* ctx_ = nullptr;

 public:
  inline tiledb::sm::StorageManager* storage_manager() {
    return ctx_->storage_manager();
  }
  inline optional<std::string> last_error() {
    return ctx_->last_error();
  }
};

#ifdef __cplusplus
extern "C" {
#endif

#ifdef __cplusplus
}
#endif

#endif  // TILEDB_CAPI_CONTEXT_INTERNAL_H
