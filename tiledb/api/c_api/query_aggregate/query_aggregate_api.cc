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
#include "tiledb/api/c_api_support/c_api_support.h"
#include "tiledb/type/apply_with_type.h"

const tiledb_channel_operator_handle_t* tiledb_channel_operator_sum =
    tiledb_channel_operator_handle_t::make_handle(
        TILEDB_QUERY_CHANNEL_OPERATOR_SUM, "SUM");
const tiledb_channel_operator_handle_t* tiledb_channel_operator_min =
    tiledb_channel_operator_handle_t::make_handle(
        TILEDB_QUERY_CHANNEL_OPERATOR_MIN, "MIN");
const tiledb_channel_operator_handle_t* tiledb_channel_operator_max =
    tiledb_channel_operator_handle_t::make_handle(
        TILEDB_QUERY_CHANNEL_OPERATOR_MAX, "MAX");

const tiledb_channel_operation_handle_t* tiledb_aggregate_count =
    tiledb_channel_operation_handle_t::make_handle(
        std::make_shared<CountOperation>());

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

inline void ensure_aggregates_enabled_via_config(tiledb_ctx_t* ctx) {
  bool enabled = ctx->context().resources().config().get<bool>(
      "sm.allow_aggregates_experimental", sm::Config::MustFindMarker());
  if (!enabled) {
    throw CAPIStatusException(
        "The aggregates API is disabled. Please set the "
        "sm.allow_aggregates_experimental config option to enable it");
  }
}

capi_return_t tiledb_channel_operator_sum_get(
    tiledb_ctx_t* ctx, const tiledb_channel_operator_t** op) {
  ensure_aggregates_enabled_via_config(ctx);
  ensure_output_pointer_is_valid(op);
  *op = tiledb_channel_operator_sum;
  return TILEDB_OK;
}

capi_return_t tiledb_channel_operator_min_get(
    tiledb_ctx_t* ctx, const tiledb_channel_operator_t** op) {
  ensure_aggregates_enabled_via_config(ctx);
  ensure_output_pointer_is_valid(op);
  *op = tiledb_channel_operator_min;
  return TILEDB_OK;
}

capi_return_t tiledb_channel_operator_max_get(
    tiledb_ctx_t* ctx, const tiledb_channel_operator_t** op) {
  ensure_aggregates_enabled_via_config(ctx);
  ensure_output_pointer_is_valid(op);
  *op = tiledb_channel_operator_max;
  return TILEDB_OK;
}

capi_return_t tiledb_aggregate_count_get(
    tiledb_ctx_t* ctx, const tiledb_channel_operation_t** operation) {
  ensure_aggregates_enabled_via_config(ctx);
  ensure_output_pointer_is_valid(operation);
  *operation = tiledb_aggregate_count;
  return TILEDB_OK;
}

capi_return_t tiledb_query_get_default_channel(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    tiledb_query_channel_t** channel) {
  ensure_aggregates_enabled_via_config(ctx);
  ensure_query_is_valid(query);
  ensure_output_pointer_is_valid(channel);

  // We don't have an internal representation of a channel,
  // the default channel is currently just a hashmap, so only pass the query
  // to the channel constructor to be carried until next the api call.
  *channel = tiledb_query_channel_handle_t::make_handle(query);

  return TILEDB_OK;
}

capi_return_t tiledb_create_unary_aggregate(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const tiledb_channel_operator_t* op,
    const char* input_field_name,
    tiledb_channel_operation_t** operation) {
  ensure_aggregates_enabled_via_config(ctx);
  ensure_query_is_valid(query);
  ensure_channel_operator_is_valid(op);
  ensure_output_pointer_is_valid(operation);
  ensure_input_field_is_valid(input_field_name, op->name());
  std::string field_name(input_field_name);
  auto& schema = query->query_->array_schema();

  auto fi = tiledb::sm::FieldInfo(
      field_name,
      schema.var_size(field_name),
      schema.is_nullable(field_name),
      schema.cell_val_num(field_name),
      schema.type(field_name));

  *operation =
      tiledb_channel_operation_handle_t::make_handle(op->make_operation(fi));

  return TILEDB_OK;
}

capi_return_t tiledb_channel_apply_aggregate(
    tiledb_ctx_t* ctx,
    tiledb_query_channel_t* channel,
    const char* output_field_name,
    const tiledb_channel_operation_t* operation) {
  ensure_aggregates_enabled_via_config(ctx);
  ensure_query_channel_is_valid(channel);
  ensure_output_field_is_valid(output_field_name);
  ensure_operation_is_valid(operation);
  channel->add_aggregate(output_field_name, operation);

  return TILEDB_OK;
}

capi_return_t tiledb_aggregate_free(
    tiledb_ctx_t* ctx, tiledb_channel_operation_t** operation) {
  ensure_aggregates_enabled_via_config(ctx);
  ensure_output_pointer_is_valid(operation);
  ensure_operation_is_valid(*operation);
  tiledb_channel_operation_handle_t::break_handle(*operation);

  return TILEDB_OK;
}

capi_return_t tiledb_query_channel_free(
    tiledb_ctx_t* ctx, tiledb_query_channel_t** channel) {
  ensure_aggregates_enabled_via_config(ctx);
  ensure_output_pointer_is_valid(channel);
  ensure_query_channel_is_valid(*channel);
  tiledb_query_channel_handle_t::break_handle(*channel);

  return TILEDB_OK;
}

}  // namespace tiledb::api

using tiledb::api::api_entry_with_context;

capi_return_t tiledb_channel_operator_sum_get(
    tiledb_ctx_t* ctx, const tiledb_channel_operator_t** op) noexcept {
  return api_entry_with_context<tiledb::api::tiledb_channel_operator_sum_get>(
      ctx, op);
}

capi_return_t tiledb_channel_operator_min_get(
    tiledb_ctx_t* ctx, const tiledb_channel_operator_t** op) noexcept {
  return api_entry_with_context<tiledb::api::tiledb_channel_operator_min_get>(
      ctx, op);
}

capi_return_t tiledb_channel_operator_max_get(
    tiledb_ctx_t* ctx, const tiledb_channel_operator_t** op) noexcept {
  return api_entry_with_context<tiledb::api::tiledb_channel_operator_max_get>(
      ctx, op);
}

capi_return_t tiledb_aggregate_count_get(
    tiledb_ctx_t* ctx, const tiledb_channel_operation_t** operation) noexcept {
  return api_entry_with_context<tiledb::api::tiledb_aggregate_count_get>(
      ctx, operation);
}

capi_return_t tiledb_query_get_default_channel(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    tiledb_query_channel_t** channel) noexcept {
  return api_entry_with_context<tiledb::api::tiledb_query_get_default_channel>(
      ctx, query, channel);
}

capi_return_t tiledb_create_unary_aggregate(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const tiledb_channel_operator_t* op,
    const char* input_field_name,
    tiledb_channel_operation_t** operation) noexcept {
  return api_entry_with_context<tiledb::api::tiledb_create_unary_aggregate>(
      ctx, query, op, input_field_name, operation);
}

capi_return_t tiledb_channel_apply_aggregate(
    tiledb_ctx_t* ctx,
    tiledb_query_channel_t* channel,
    const char* output_field_name,
    const tiledb_channel_operation_t* operation) noexcept {
  return api_entry_with_context<tiledb::api::tiledb_channel_apply_aggregate>(
      ctx, channel, output_field_name, operation);
}

capi_return_t tiledb_aggregate_free(
    tiledb_ctx_t* ctx, tiledb_channel_operation_t** operation) noexcept {
  return tiledb::api::api_entry_with_context<
      tiledb::api::tiledb_aggregate_free>(ctx, operation);
}

capi_return_t tiledb_query_channel_free(
    tiledb_ctx_t* ctx, tiledb_query_channel_t** channel) noexcept {
  return tiledb::api::api_entry_with_context<
      tiledb::api::tiledb_query_channel_free>(ctx, channel);
}

MaxOperation::MaxOperation(
    const tiledb::sm::FieldInfo& fi,
    const tiledb_channel_operator_handle_t* op) {
  auto g = [&](auto T) {
    if constexpr (tiledb::type::TileDBFundamental<decltype(T)>) {
      if constexpr (tiledb::type::TileDBNumeric<decltype(T)>) {
        ensure_aggregate_numeric_field(op, fi);
      }

      // This is a min/max on strings, should be refactored out once we
      // change (STRING_ASCII,CHAR) mapping in apply_with_type
      if constexpr (std::is_same_v<char, decltype(T)>) {
        aggregator_ =
            std::make_shared<tiledb::sm::MaxAggregator<std::string>>(fi);
      } else {
        aggregator_ =
            std::make_shared<tiledb::sm::MaxAggregator<decltype(T)>>(fi);
      }
    } else {
      throw std::logic_error(
          "MAX aggregates can only be requested on numeric and string "
          "types");
    }
  };
  apply_with_type(g, fi.type_);
}

MinOperation::MinOperation(
    const tiledb::sm::FieldInfo& fi,
    const tiledb_channel_operator_handle_t* op) {
  auto g = [&](auto T) {
    if constexpr (tiledb::type::TileDBFundamental<decltype(T)>) {
      if constexpr (tiledb::type::TileDBNumeric<decltype(T)>) {
        ensure_aggregate_numeric_field(op, fi);
      }

      // This is a min/max on strings, should be refactored out once we
      // change (STRING_ASCII,CHAR) mapping in apply_with_type
      if constexpr (std::is_same_v<char, decltype(T)>) {
        aggregator_ =
            std::make_shared<tiledb::sm::MinAggregator<std::string>>(fi);
      } else {
        aggregator_ =
            std::make_shared<tiledb::sm::MinAggregator<decltype(T)>>(fi);
      }
    } else {
      throw std::logic_error(
          "MIN aggregates can only be requested on numeric and string "
          "types");
    }
  };
  apply_with_type(g, fi.type_);
}

SumOperation::SumOperation(
    const tiledb::sm::FieldInfo& fi,
    const tiledb_channel_operator_handle_t* op) {
  auto g = [&](auto T) {
    if constexpr (tiledb::type::TileDBNumeric<decltype(T)>) {
      ensure_aggregate_numeric_field(op, fi);
      aggregator_ =
          std::make_shared<tiledb::sm::SumAggregator<decltype(T)>>(fi);
    } else {
      throw std::logic_error(
          "Sum aggregates can only be requested on numeric types");
    }
  };
  apply_with_type(g, fi.type_);
}
