/**
 * @file tiledb/api/c_api/context/context_api_internal.h
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

#include "../../c_api_support/handle/handle.h"
#include "../config/config_api_internal.h"
#include "../error/error_api_internal.h"
#include "tiledb/sm/storage_manager/cancellation_source.h"
#include "tiledb/sm/storage_manager/context.h"

struct tiledb_ctx_handle_t : public tiledb::api::CAPIHandle {
  /**
   * Type name
   */
  static constexpr std::string_view object_type_name{"context"};

 private:
  tiledb::sm::Context ctx_;

 public:
  tiledb_ctx_handle_t() = delete;

  tiledb_ctx_handle_t(const tiledb::sm::Config& config)
      : ctx_(config) {
  }

  inline tiledb::sm::Context& context() {
    return ctx_;
  }

  inline tiledb::sm::ContextResources& resources() {
    return ctx_.resources();
  }

  inline tiledb::sm::Config& config() {
    return ctx_.resources().config();
  }

  inline tiledb::sm::StorageManager* storage_manager() {
    return ctx_.storage_manager();
  }

  inline tiledb::sm::CancellationSource cancellation_source() {
    return ctx_.cancellation_source();
  }

  inline tiledb::sm::RestClient& rest_client() {
    return ctx_.rest_client();
  }

  inline bool has_rest_client() {
    return ctx_.has_rest_client();
  }

  inline optional<std::string> last_error() {
    return ctx_.last_error();
  }
};

namespace tiledb::api {
/**
 * Saves a status inside the context object.
 *
 * Note that even though a function called `save_error` is defined here, it's
 * not about a C API error object, the kind that's wrapped in a handle. It's a
 * a wrapper for the "last error" facility within a `Context` object.
 */
bool save_error(tiledb_ctx_handle_t* ctx, const tiledb::common::Status& st);

/**
 * Returns if the argument is a valid context: non-null, valid as a handle
 *
 * @tparam E Exception type to throw if context is not valid
 * @param ctx A context of unknown validity
 */
template <class E = CAPIException>
inline void ensure_context_is_valid(const tiledb_ctx_handle_t* ctx) {
  ensure_handle_is_valid<tiledb_ctx_handle_t, E>(ctx);
}

inline bool is_context_valid(const tiledb_ctx_handle_t* ctx) {
  return is_handle_valid(ctx);
}

}  // namespace tiledb::api
#endif  // TILEDB_CAPI_CONTEXT_INTERNAL_H
