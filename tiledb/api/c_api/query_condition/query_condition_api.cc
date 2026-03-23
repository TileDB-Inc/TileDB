/**
 * @file tiledb/api/c_api/query_condition/query_condition_api.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2026 TileDB, Inc.
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
 * This file defines the query condition section of the C API for TileDB.
 */

#include "query_condition_api_external.h"
#include "query_condition_api_external_experimental.h"
#include "tiledb/api/c_api_support/c_api_support.h"
#include "tiledb/sm/c_api/api_argument_validator.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/query/query_condition.h"

#include <new>

namespace tiledb::api {

/* ****************************** */
/*          QUERY CONDITION       */
/* ****************************** */

int32_t tiledb_query_condition_alloc(
    tiledb_ctx_t* const ctx, tiledb_query_condition_t** const cond) {
  // Create query condition struct
  *cond = new (std::nothrow) tiledb_query_condition_t;
  if (*cond == nullptr) {
    auto st = Status_Error(
        "Failed to create TileDB query condition object; Memory allocation "
        "error");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Create QueryCondition object
  (*cond)->query_condition_ = new (std::nothrow) tiledb::sm::QueryCondition();
  if ((*cond)->query_condition_ == nullptr) {
    auto st = Status_Error("Failed to allocate TileDB query condition object");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    delete *cond;
    *cond = nullptr;
    return TILEDB_OOM;
  }

  // Success
  return TILEDB_OK;
}

capi_return_t tiledb_query_condition_alloc_set_membership(
    const char* field_name,
    const void* data,
    uint64_t data_size,
    const void* offsets,
    uint64_t offsets_size,
    tiledb_query_condition_op_t op,
    tiledb_query_condition_t** cond) {
  // Validate input arguments. The data, data_size, offsets, and offsets_size,
  // and op arguments are validated in the QueryCondition constructor so
  // there's no need to validate them here.
  if (field_name == nullptr) {
    throw api::CAPIStatusException(
        "QueryCondition field name must not be nullptr");
  }
  ensure_output_pointer_is_valid(cond);

  // Create query condition struct
  *cond = new tiledb_query_condition_t;
  if (*cond == nullptr) {
    throw api::CAPIStatusException(
        "Failed to create TileDB query condition "
        "object; Memory allocation error");
  }

  // Create QueryCondition object
  (*cond)->query_condition_ = new tiledb::sm::QueryCondition(
      field_name,
      data,
      data_size,
      offsets,
      offsets_size,
      static_cast<tiledb::sm::QueryConditionOp>(op));
  if ((*cond)->query_condition_ == nullptr) {
    delete *cond;
    throw api::CAPIStatusException(
        "Failed to allocate TileDB query condition object");
  }

  return TILEDB_OK;
}

void tiledb_query_condition_free(tiledb_query_condition_t** cond) {
  if (cond != nullptr && *cond != nullptr) {
    delete (*cond)->query_condition_;
    delete *cond;
    *cond = nullptr;
  }
}

int32_t tiledb_query_condition_init(
    tiledb_ctx_t* const ctx,
    tiledb_query_condition_t* const cond,
    const char* const attribute_name,
    const void* const condition_value,
    const uint64_t condition_value_size,
    const tiledb_query_condition_op_t op) {
  if (sanity_check(ctx, cond) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  // Initialize the QueryCondition object
  auto st = cond->query_condition_->init(
      std::string(attribute_name),
      condition_value,
      condition_value_size,
      static_cast<tiledb::sm::QueryConditionOp>(op));
  if (!st.ok()) {
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  // Success
  return TILEDB_OK;
}

int32_t tiledb_query_condition_combine(
    tiledb_ctx_t* const ctx,
    const tiledb_query_condition_t* const left_cond,
    const tiledb_query_condition_t* const right_cond,
    const tiledb_query_condition_combination_op_t combination_op,
    tiledb_query_condition_t** const combined_cond) {
  // Sanity check
  if (sanity_check(ctx, left_cond) == TILEDB_ERR ||
      (combination_op != TILEDB_NOT &&
       sanity_check(ctx, right_cond) == TILEDB_ERR) ||
      (combination_op == TILEDB_NOT && right_cond != nullptr))
    return TILEDB_ERR;

  // Create the combined query condition struct
  *combined_cond = new (std::nothrow) tiledb_query_condition_t;
  if (*combined_cond == nullptr) {
    auto st = Status_Error(
        "Failed to create TileDB query condition object; Memory allocation "
        "error");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Create the combined QueryCondition object
  (*combined_cond)->query_condition_ =
      new (std::nothrow) tiledb::sm::QueryCondition();
  if ((*combined_cond)->query_condition_ == nullptr) {
    auto st = Status_Error("Failed to allocate TileDB query condition object");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    delete *combined_cond;
    *combined_cond = nullptr;
    return TILEDB_OOM;
  }

  if (combination_op == TILEDB_NOT) {
    if (SAVE_ERROR_CATCH(
            ctx,
            left_cond->query_condition_->negate(
                static_cast<tiledb::sm::QueryConditionCombinationOp>(
                    combination_op),
                (*combined_cond)->query_condition_))) {
      delete (*combined_cond)->query_condition_;
      delete *combined_cond;
      return TILEDB_ERR;
    }
  } else {
    if (SAVE_ERROR_CATCH(
            ctx,
            left_cond->query_condition_->combine(
                *right_cond->query_condition_,
                static_cast<tiledb::sm::QueryConditionCombinationOp>(
                    combination_op),
                (*combined_cond)->query_condition_))) {
      delete (*combined_cond)->query_condition_;
      delete *combined_cond;
      return TILEDB_ERR;
    }
  }

  return TILEDB_OK;
}

int32_t tiledb_query_condition_negate(
    tiledb_ctx_t* const ctx,
    const tiledb_query_condition_t* const cond,
    tiledb_query_condition_t** const negated_cond) {
  return api::tiledb_query_condition_combine(
      ctx, cond, nullptr, TILEDB_NOT, negated_cond);
}

capi_return_t tiledb_query_condition_set_use_enumeration(
    tiledb_ctx_t* ctx,
    const tiledb_query_condition_t* cond,
    int use_enumeration) {
  if (sanity_check(ctx, cond) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  cond->query_condition_->set_use_enumeration(
      use_enumeration != 0 ? true : false);
  return TILEDB_OK;
}

}  // namespace tiledb::api

using tiledb::api::api_entry_context;
using tiledb::api::api_entry_void;
template <auto f>
constexpr auto api_entry = tiledb::api::api_entry_with_context<f>;

/* ****************************** */
/*          QUERY CONDITION       */
/* ****************************** */

CAPI_INTERFACE(
    query_condition_alloc,
    tiledb_ctx_t* const ctx,
    tiledb_query_condition_t** const cond) {
  return api_entry<tiledb::api::tiledb_query_condition_alloc>(ctx, cond);
}

CAPI_INTERFACE(
    query_condition_alloc_set_membership,
    tiledb_ctx_t* ctx,
    const char* field_name,
    const void* data,
    uint64_t data_size,
    const void* offsets,
    uint64_t offsets_size,
    tiledb_query_condition_op_t op,
    tiledb_query_condition_t** cond) {
  return api_entry_context<
      tiledb::api::tiledb_query_condition_alloc_set_membership>(
      ctx, field_name, data, data_size, offsets, offsets_size, op, cond);
}

CAPI_INTERFACE_VOID(query_condition_free, tiledb_query_condition_t** cond) {
  return api_entry_void<tiledb::api::tiledb_query_condition_free>(cond);
}

CAPI_INTERFACE(
    query_condition_init,
    tiledb_ctx_t* const ctx,
    tiledb_query_condition_t* const cond,
    const char* const attribute_name,
    const void* const condition_value,
    const uint64_t condition_value_size,
    const tiledb_query_condition_op_t op) {
  return api_entry<tiledb::api::tiledb_query_condition_init>(
      ctx, cond, attribute_name, condition_value, condition_value_size, op);
}

CAPI_INTERFACE(
    query_condition_combine,
    tiledb_ctx_t* const ctx,
    const tiledb_query_condition_t* const left_cond,
    const tiledb_query_condition_t* const right_cond,
    const tiledb_query_condition_combination_op_t combination_op,
    tiledb_query_condition_t** const combined_cond) {
  return api_entry<tiledb::api::tiledb_query_condition_combine>(
      ctx, left_cond, right_cond, combination_op, combined_cond);
}

CAPI_INTERFACE(
    query_condition_negate,
    tiledb_ctx_t* const ctx,
    const tiledb_query_condition_t* const cond,
    tiledb_query_condition_t** const negated_cond) {
  return api_entry<tiledb::api::tiledb_query_condition_negate>(
      ctx, cond, negated_cond);
}

CAPI_INTERFACE(
    query_condition_set_use_enumeration,
    tiledb_ctx_t* const ctx,
    const tiledb_query_condition_t* const cond,
    int use_enumeration) {
  return api_entry<tiledb::api::tiledb_query_condition_set_use_enumeration>(
      ctx, cond, use_enumeration);
}
