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
#include "tiledb/api/c_api_support/c_api_support.h"

#include "tiledb/sm/query_aggregate/query_aggregate.h"

const tiledb_channel_operator_handle_t* tiledb_channel_operator_count =
    tiledb_channel_operator_handle_t::make_handle(
        QueryChannelOperator::COUNT, "COUNT");
const tiledb_channel_operator_handle_t* tiledb_channel_operator_sum =
    tiledb_channel_operator_handle_t::make_handle(
        QueryChannelOperator::SUM, "SUM");

namespace tiledb::api {

/**
 * Returns if the argument is a valid query pointer
 *
 * @param query A C api query pointer
 */
inline void ensure_query_arg_is_valid(tiledb_query_t* query) {
  if (!query) {
    throw CAPIStatusException("argument `query` may not be nullptr");
  }
}

/**
 * Returns if the argument is a valid char pointer
 *
 * @param input_field A char pointer
 */
inline void ensure_input_field_is_valid(
    const char* input_field, const std::string& op) {
  if (!input_field) {
    throw CAPIStatusException(
        "argument `input_field` may not be nullptr for operator " + op);
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

capi_return_t tiledb_query_get_default_channel(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    tiledb_query_channel_t** channel) {
  (void)ctx;
  ensure_query_arg_is_valid(query);
  ensure_output_pointer_is_valid(channel);

  // We don't have an internal representation of a channel,
  // the default channel is currently just a hashmap, so only pass the query
  // to the channel constructor to be carried until next the api call.
  *channel = tiledb_query_channel_handle_t::make_handle(query);

  return TILEDB_OK;
}

capi_return_t tiledb_channel_operation_field_create(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const tiledb_channel_operator_t* op,
    const char* input_field_name,
    tiledb_channel_operation_t** operation) {
  (void)ctx;
  ensure_query_arg_is_valid(query);
  ensure_channel_operator_is_valid(op);
  ensure_output_pointer_is_valid(operation);

  shared_ptr<tiledb::sm::IAggregator> aggregator;
  switch (op->value()) {
    case QueryChannelOperator::COUNT:
      aggregator = std::make_shared<tiledb::sm::CountAggregator>();
      break;
    case QueryChannelOperator::SUM: {
      ensure_input_field_is_valid(input_field_name, op->name());
      std::string field_name(input_field_name);

      auto& schema = query->query_->array_schema();
      auto g = [&](auto T) {
        if constexpr (!std::is_same_v<decltype(T), char>) {
          aggregator = std::make_shared<tiledb::sm::SumAggregator<decltype(T)>>(
              field_name,
              schema.var_size(field_name),
              schema.is_nullable(field_name),
              schema.cell_val_num(field_name));
        }
      };
      execute_callback_with_type(schema.type(field_name), g);
      break;
    }
    default:
      throw CAPIStatusException(
          "operator argument `op` has unsupported value: " +
          std::to_string(static_cast<uint8_t>(op->value())));
      break;
  }
  *operation = tiledb_channel_operation_handle_t::make_handle(aggregator);

  return TILEDB_OK;
}

capi_return_t tiledb_channel_add_aggregate(
    tiledb_ctx_t* ctx,
    tiledb_query_channel_t* channel,
    const char* output_field_name,
    tiledb_channel_operation_t* operation) {
  (void)ctx;
  channel->add_aggregate(output_field_name, operation);

  return TILEDB_OK;
}

}  // namespace tiledb::api

using tiledb::api::api_entry_with_context;

capi_return_t tiledb_query_get_default_channel(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    tiledb_query_channel_t** channel) noexcept {
  return api_entry_with_context<tiledb::api::tiledb_query_get_default_channel>(
      ctx, query, channel);
}

capi_return_t tiledb_channel_operation_field_create(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const tiledb_channel_operator_t* op,
    const char* input_field_name,
    tiledb_channel_operation_t** operation) noexcept {
  return api_entry_with_context<
      tiledb::api::tiledb_channel_operation_field_create>(
      ctx, query, op, input_field_name, operation);
}

capi_return_t tiledb_channel_add_aggregate(
    tiledb_ctx_t* ctx,
    tiledb_query_channel_t* channel,
    const char* output_field_name,
    tiledb_channel_operation_t* operation) noexcept {
  return api_entry_with_context<tiledb::api::tiledb_channel_add_aggregate>(
      ctx, channel, output_field_name, operation);
}