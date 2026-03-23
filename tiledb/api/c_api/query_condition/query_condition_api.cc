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
#include "query_condition_api_internal.h"
#include "tiledb/api/c_api_support/c_api_support.h"
#include "tiledb/sm/c_api/api_argument_validator.h"
#include "tiledb/sm/query/query_condition.h"

#include <new>

namespace tiledb::api {

/* ****************************** */
/*          QUERY CONDITION       */
/* ****************************** */

int32_t tiledb_query_condition_alloc(tiledb_query_condition_t** const cond) {
  *cond = make_handle<tiledb_query_condition_t>();
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

  // Create QueryCondition object
  *cond = make_handle<tiledb_query_condition_handle_t>(
      std::in_place,
      field_name,
      data,
      data_size,
      offsets,
      offsets_size,
      static_cast<tiledb::sm::QueryConditionOp>(op));
  return TILEDB_OK;
}

void tiledb_query_condition_free(tiledb_query_condition_t** cond) {
  break_handle(*cond);
}

int32_t tiledb_query_condition_init(
    tiledb_query_condition_t* const cond,
    const char* const attribute_name,
    const void* const condition_value,
    const uint64_t condition_value_size,
    const tiledb_query_condition_op_t op) {
  ensure_handle_is_valid(cond);

  // Initialize the QueryCondition object
  throw_if_not_ok(cond->query_condition()->init(
      std::string(attribute_name),
      condition_value,
      condition_value_size,
      static_cast<tiledb::sm::QueryConditionOp>(op)));

  return TILEDB_OK;
}

int32_t tiledb_query_condition_combine(
    const tiledb_query_condition_t* const left_cond,
    const tiledb_query_condition_t* const right_cond,
    const tiledb_query_condition_combination_op_t combination_op,
    tiledb_query_condition_t** const combined_cond) {
  // Sanity check
  ensure_handle_is_valid(left_cond);
  ensure_handle_is_valid(right_cond);
  ensure_output_pointer_is_valid(combined_cond);

  // Create the combined QueryCondition object
  auto combined_cond_obj = make_shared<tiledb::sm::QueryCondition>(HERE());

  if (combination_op == TILEDB_NOT) {
    throw_if_not_ok(
        left_cond->query_condition()->negate(combined_cond_obj.get()));
  } else {
    throw_if_not_ok(left_cond->query_condition()->combine(
        *right_cond->query_condition(),
        static_cast<tiledb::sm::QueryConditionCombinationOp>(combination_op),
        combined_cond_obj.get()));
  }

  *combined_cond =
      make_handle<tiledb_query_condition_handle_t>(combined_cond_obj);
  return TILEDB_OK;
}

int32_t tiledb_query_condition_negate(
    const tiledb_query_condition_t* const cond,
    tiledb_query_condition_t** const negated_cond) {
  return api::tiledb_query_condition_combine(
      cond, nullptr, TILEDB_NOT, negated_cond);
}

capi_return_t tiledb_query_condition_set_use_enumeration(
    const tiledb_query_condition_t* cond, int use_enumeration) {
  ensure_handle_is_valid(cond);
  cond->query_condition()->set_use_enumeration(
      use_enumeration != 0 ? true : false);
  return TILEDB_OK;
}

}  // namespace tiledb::api

using tiledb::api::api_entry_context;
using tiledb::api::api_entry_void;
template <auto f>
constexpr auto api_entry = tiledb::api::api_entry_context<f>;

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
  return api_entry<tiledb::api::tiledb_query_condition_alloc_set_membership>(
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
