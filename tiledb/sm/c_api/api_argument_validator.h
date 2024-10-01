/**
 * @file   api_argument_validator.h
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
 * This file declares argument validator functions used in the c-api
 * implementation
 */

#ifndef TILEDB_CAPI_HELPERS_H
#define TILEDB_CAPI_HELPERS_H

#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/api/c_api_support/argument_validation.h"
#include "tiledb/common/exception/exception.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"

/* ********************************* */
/*         AUXILIARY FUNCTIONS       */
/* ********************************* */

namespace tiledb::api {
/**
 * Returns if a subarray handle (old style) is valid. Throws otherwise.
 */
inline void ensure_subarray_is_valid(const tiledb_subarray_t* p) {
  if (p == nullptr || p->subarray_ == nullptr ||
      p->subarray_->array() == nullptr) {
    throw CAPIException("Invalid TileDB subarray object");
  }
}
}  // namespace tiledb::api

inline int32_t sanity_check(tiledb_ctx_t* ctx, const tiledb_query_t* query) {
  if (query == nullptr || query->query_ == nullptr) {
    auto st = Status_Error("Invalid TileDB query object");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int32_t sanity_check(
    tiledb_ctx_t* ctx, const tiledb_query_condition_t* cond) {
  if (cond == nullptr || cond->query_condition_ == nullptr) {
    auto st = Status_Error("Invalid TileDB query condition object");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int32_t sanity_check(
    tiledb_ctx_t* ctx, const tiledb_consolidation_plan_t* consolidation_plan) {
  if (consolidation_plan == nullptr ||
      consolidation_plan->consolidation_plan_ == nullptr) {
    auto st = Status_Error("Invalid TileDB consolidation plan object");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

/**
 * Helper macro similar to save_error() that catches all exceptions when
 * executing 'stmt'.
 *
 * @param ctx TileDB context
 * @param stmt Statement to execute
 */
#define SAVE_ERROR_CATCH(ctx, stmt)                                        \
  [&]() {                                                                  \
    auto _s = Status::Ok();                                                \
    try {                                                                  \
      _s = (stmt);                                                         \
    } catch (const std::exception& e) {                                    \
      auto st = Status_Error(                                              \
          std::string("Internal TileDB uncaught exception; ") + e.what()); \
      LOG_STATUS_NO_RETURN_VALUE(st);                                      \
      save_error(ctx, st);                                                 \
      return true;                                                         \
    }                                                                      \
    return save_error(ctx, _s);                                            \
  }()

#endif  // TILEDB_CAPI_HELPERS_H
