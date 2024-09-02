/**
 * @file tiledb/api/c_api/query_aggregate/query_aggregate_api.cc
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
 * This file defines the query_aggregate C API of TileDB.
 **/

#include "../string/string_api_internal.h"
#include "query_aggregate_api_external_experimental.h"
#include "query_aggregate_api_internal.h"
#include "tiledb/api/c_api/query/query_api_internal.h"
#include "tiledb/api/c_api/query_field/query_field_api_external_experimental.h"
#include "tiledb/api/c_api/query_field/query_field_api_internal.h"
#include "tiledb/api/c_api_support/c_api_support.h"

const tiledb_channel_operator_handle_t* tiledb_channel_operator_sum =
    tiledb_channel_operator_handle_t::make_handle(
        tiledb::sm::constants::aggregate_sum_str);
const tiledb_channel_operator_handle_t* tiledb_channel_operator_min =
    tiledb_channel_operator_handle_t::make_handle(
        tiledb::sm::constants::aggregate_min_str);
const tiledb_channel_operator_handle_t* tiledb_channel_operator_max =
    tiledb_channel_operator_handle_t::make_handle(
        tiledb::sm::constants::aggregate_max_str);
const tiledb_channel_operator_handle_t* tiledb_channel_operator_mean =
    tiledb_channel_operator_handle_t::make_handle(
        tiledb::sm::constants::aggregate_mean_str);
const tiledb_channel_operator_handle_t* tiledb_channel_operator_null_count =
    tiledb_channel_operator_handle_t::make_handle(
        tiledb::sm::constants::aggregate_null_count_str);

const tiledb_channel_operation_handle_t* tiledb_aggregate_count =
    tiledb_channel_operation_handle_t::make_handle(
        std::make_shared<tiledb::sm::CountOperation>());

namespace tiledb::api {

/**
 * Returns if the argument is a valid char pointer
 *
 * @param input_field A char pointer
 * @param op An aggregation operator name
 */
inline void ensure_input_field_is_valid(
    const char* input_field, const std::string& op) {
  if (!input_field) {
    throw CAPIStatusException(
        "argument `input_field` may not be nullptr for operator " + op);
  }
}

/**
 * Returns if the argument is a valid char pointer
 *
 * @param output_field A char pointer
 */
inline void ensure_output_field_is_valid(const char* output_field) {
  if (!output_field) {
    throw CAPIStatusException("argument `output_field` may not be nullptr");
  }
}

/**
 * Returns if the argument is a valid channel operator
 *
 * @param op A channel operator handle
 */
inline void ensure_channel_operator_is_valid(
    const tiledb_channel_operator_handle_t* op) {
  ensure_handle_is_valid(op);
}

/**
 * Returns if the argument is a valid channel operation
 *
 * @param operation A channel operation handle
 */
inline void ensure_operation_is_valid(
    const tiledb_channel_operation_handle_t* operation) {
  ensure_handle_is_valid(operation);
}

/**
 * Returns if the argument is a valid query channel
 *
 * @param channel A query channel handle
 */
inline void ensure_query_channel_is_valid(
    const tiledb_query_channel_handle_t* channel) {
  ensure_handle_is_valid(channel);
}

capi_return_t tiledb_channel_operator_sum_get(
    tiledb_ctx_t*, const tiledb_channel_operator_t** op) {
  ensure_output_pointer_is_valid(op);
  *op = tiledb_channel_operator_sum;
  return TILEDB_OK;
}

capi_return_t tiledb_channel_operator_mean_get(
    tiledb_ctx_t*, const tiledb_channel_operator_t** op) {
  ensure_output_pointer_is_valid(op);
  *op = tiledb_channel_operator_mean;
  return TILEDB_OK;
}

capi_return_t tiledb_channel_operator_null_count_get(
    tiledb_ctx_t*, const tiledb_channel_operator_t** op) {
  ensure_output_pointer_is_valid(op);
  *op = tiledb_channel_operator_null_count;
  return TILEDB_OK;
}

capi_return_t tiledb_channel_operator_min_get(
    tiledb_ctx_t*, const tiledb_channel_operator_t** op) {
  ensure_output_pointer_is_valid(op);
  *op = tiledb_channel_operator_min;
  return TILEDB_OK;
}

capi_return_t tiledb_channel_operator_max_get(
    tiledb_ctx_t*, const tiledb_channel_operator_t** op) {
  ensure_output_pointer_is_valid(op);
  *op = tiledb_channel_operator_max;
  return TILEDB_OK;
}

capi_return_t tiledb_aggregate_count_get(
    tiledb_ctx_t*, const tiledb_channel_operation_t** operation) {
  ensure_output_pointer_is_valid(operation);
  *operation = tiledb_aggregate_count;
  return TILEDB_OK;
}

capi_return_t tiledb_query_get_default_channel(
    tiledb_ctx_t*, tiledb_query_t* query, tiledb_query_channel_t** channel) {
  ensure_query_is_valid(query);
  ensure_output_pointer_is_valid(channel);
  *channel = tiledb_query_channel_handle_t::make_handle(
      query->query_->default_channel());

  return TILEDB_OK;
}

capi_return_t tiledb_create_unary_aggregate(
    tiledb_ctx_t*,
    tiledb_query_t* query,
    const tiledb_channel_operator_t* op,
    const char* input_field_name,
    tiledb_channel_operation_t** operation) {
  ensure_query_is_valid(query);
  ensure_query_is_not_initialized(query);
  ensure_channel_operator_is_valid(op);
  ensure_output_pointer_is_valid(operation);
  ensure_input_field_is_valid(input_field_name, op->name());
  std::string field_name(input_field_name);
  auto& schema = query->query_->array_schema();

  // Getting the field errors if there is no input_field_name associated with
  // the query.
  tiledb_query_field_t* field =
      tiledb_query_field_handle_t::make_handle(query, input_field_name);
  tiledb_query_field_handle_t::break_handle(field);

  const auto is_dense_dim{schema.dense() && schema.is_dim(field_name)};
  const auto cell_order{schema.cell_order()};

  // Get the dimension index for the dense case. It is used below to know if the
  // dimenson to be aggregated is the last dimension for ROW_MAJOR or first for
  // COL_MAJOR. This is used at the aggregate level to know if we need to change
  // the dimension value when we move cells.
  unsigned dim_idx = 0;
  if (is_dense_dim) {
    dim_idx = schema.domain().get_dimension_index(field_name);
  }

  const bool is_slab_dim =
      is_dense_dim && (cell_order == sm::Layout::ROW_MAJOR) ?
          (dim_idx == schema.dim_num() - 1) :
          (dim_idx == 0);

  auto fi = tiledb::sm::FieldInfo(
      field_name,
      schema.var_size(field_name),
      schema.is_nullable(field_name),
      is_dense_dim,
      is_slab_dim,
      schema.cell_val_num(field_name),
      schema.type(field_name));

  *operation =
      tiledb_channel_operation_handle_t::make_handle(op->make_operation(fi));

  return TILEDB_OK;
}

capi_return_t tiledb_channel_apply_aggregate(
    tiledb_ctx_t*,
    tiledb_query_channel_t* channel,
    const char* output_field_name,
    const tiledb_channel_operation_t* operation) {
  ensure_query_channel_is_valid(channel);
  ensure_query_is_not_initialized(channel->query());
  ensure_output_field_is_valid(output_field_name);
  ensure_operation_is_valid(operation);
  channel->add_aggregate(output_field_name, operation);

  return TILEDB_OK;
}

capi_return_t tiledb_aggregate_free(
    tiledb_ctx_t*, tiledb_channel_operation_t** operation) {
  ensure_output_pointer_is_valid(operation);
  ensure_operation_is_valid(*operation);
  tiledb_channel_operation_handle_t::break_handle(*operation);

  return TILEDB_OK;
}

capi_return_t tiledb_query_channel_free(
    tiledb_ctx_t*, tiledb_query_channel_t** channel) {
  ensure_output_pointer_is_valid(channel);
  ensure_query_channel_is_valid(*channel);
  tiledb_query_channel_handle_t::break_handle(*channel);

  return TILEDB_OK;
}

}  // namespace tiledb::api

shared_ptr<tiledb::sm::Operation>
tiledb_channel_operator_handle_t::make_operation(
    const tiledb::sm::FieldInfo& fi) const {
  return tiledb::sm::Operation::make_operation(this->name(), fi);
}

using tiledb::api::api_entry_with_context;

CAPI_INTERFACE(
    channel_operator_sum_get,
    tiledb_ctx_t* ctx,
    const tiledb_channel_operator_t** op) {
  return api_entry_with_context<tiledb::api::tiledb_channel_operator_sum_get>(
      ctx, op);
}

CAPI_INTERFACE(
    channel_operator_mean_get,
    tiledb_ctx_t* ctx,
    const tiledb_channel_operator_t** op) {
  return api_entry_with_context<tiledb::api::tiledb_channel_operator_mean_get>(
      ctx, op);
}

CAPI_INTERFACE(
    channel_operator_min_get,
    tiledb_ctx_t* ctx,
    const tiledb_channel_operator_t** op) {
  return api_entry_with_context<tiledb::api::tiledb_channel_operator_min_get>(
      ctx, op);
}

CAPI_INTERFACE(
    channel_operator_max_get,
    tiledb_ctx_t* ctx,
    const tiledb_channel_operator_t** op) {
  return api_entry_with_context<tiledb::api::tiledb_channel_operator_max_get>(
      ctx, op);
}

CAPI_INTERFACE(
    channel_operator_null_count_get,
    tiledb_ctx_t* ctx,
    const tiledb_channel_operator_t** op) {
  return api_entry_with_context<
      tiledb::api::tiledb_channel_operator_null_count_get>(ctx, op);
}

CAPI_INTERFACE(
    aggregate_count_get,
    tiledb_ctx_t* ctx,
    const tiledb_channel_operation_t** operation) {
  return api_entry_with_context<tiledb::api::tiledb_aggregate_count_get>(
      ctx, operation);
}

CAPI_INTERFACE(
    query_get_default_channel,
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    tiledb_query_channel_t** channel) {
  return api_entry_with_context<tiledb::api::tiledb_query_get_default_channel>(
      ctx, query, channel);
}

CAPI_INTERFACE(
    create_unary_aggregate,
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const tiledb_channel_operator_t* op,
    const char* input_field_name,
    tiledb_channel_operation_t** operation) {
  return api_entry_with_context<tiledb::api::tiledb_create_unary_aggregate>(
      ctx, query, op, input_field_name, operation);
}

CAPI_INTERFACE(
    channel_apply_aggregate,
    tiledb_ctx_t* ctx,
    tiledb_query_channel_t* channel,
    const char* output_field_name,
    const tiledb_channel_operation_t* operation) {
  return api_entry_with_context<tiledb::api::tiledb_channel_apply_aggregate>(
      ctx, channel, output_field_name, operation);
}

CAPI_INTERFACE(
    aggregate_free, tiledb_ctx_t* ctx, tiledb_channel_operation_t** operation) {
  return tiledb::api::api_entry_with_context<
      tiledb::api::tiledb_aggregate_free>(ctx, operation);
}

CAPI_INTERFACE(
    query_channel_free, tiledb_ctx_t* ctx, tiledb_query_channel_t** channel) {
  return tiledb::api::api_entry_with_context<
      tiledb::api::tiledb_query_channel_free>(ctx, channel);
}
