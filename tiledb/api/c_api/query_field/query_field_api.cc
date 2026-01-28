/**
 * @file tiledb/api/c_api/query_field/query_field_api.cc
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
 * This file defines the query_field C API of TileDB.
 **/

#include "query_field_api_external_experimental.h"
#include "query_field_api_internal.h"
#include "tiledb/api/c_api/datatype/datatype_api_external.h"
#include "tiledb/api/c_api/query/query_api_internal.h"
#include "tiledb/api/c_api_support/c_api_support.h"
#include "tiledb/sm/misc/constants.h"

tiledb_field_origin_t FieldFromDimension::origin() {
  return TILEDB_DIMENSION_FIELD;
}

tiledb_field_origin_t FieldFromAttribute::origin() {
  return TILEDB_ATTRIBUTE_FIELD;
}

tiledb_field_origin_t FieldFromAggregate::origin() {
  return TILEDB_AGGREGATE_FIELD;
}

tiledb_query_field_handle_t::tiledb_query_field_handle_t(
    tiledb_query_t* query, const char* field_name)
    : query_(query->query_)
    , field_name_(field_name) {
  bool is_aggregate{false};
  if (field_name_ == tiledb::sm::constants::coords) {
    field_origin_ = std::make_shared<FieldFromDimension>();
    type_ = query_->array_schema().domain().dimension_ptr(0)->type();
    is_nullable_ = false;
    cell_val_num_ = 1;
  } else if (field_name_ == tiledb::sm::constants::timestamps) {
    field_origin_ = std::make_shared<FieldFromAttribute>();
    type_ = tiledb::sm::constants::timestamp_type;
    is_nullable_ = false;
    cell_val_num_ = 1;
  } else if (query_->array_schema().is_attr(field_name_)) {
    field_origin_ = std::make_shared<FieldFromAttribute>();
    type_ = query_->array_schema().attribute(field_name_)->type();
    is_nullable_ = query_->array_schema().attribute(field_name_)->nullable();
    cell_val_num_ =
        query_->array_schema().attribute(field_name_)->cell_val_num();
  } else if (query_->array_schema().is_dim(field_name_)) {
    field_origin_ = std::make_shared<FieldFromDimension>();
    type_ = query_->array_schema().dimension_ptr(field_name_)->type();
    is_nullable_ = false;
    cell_val_num_ =
        query_->array_schema().dimension_ptr(field_name_)->cell_val_num();
  } else if (query_->is_aggregate(field_name_)) {
    is_aggregate = true;
    field_origin_ = std::make_shared<FieldFromAggregate>();
    auto aggregate = query_->get_aggregate(field_name_).value();
    type_ = aggregate->output_datatype();
    is_nullable_ = aggregate->aggregation_nullable();
    cell_val_num_ =
        aggregate->aggregation_var_sized() ? tiledb::sm::constants::var_num : 1;
  } else {
    throw tiledb::api::CAPIStatusException("There is no field " + field_name_);
  }
  /*
   * We have no `class QueryField` that would already know its own aggregate,
   * so we mirror the channel selection process that `class Query` has
   * responsibility for.
   */
  if (is_aggregate) {
    channel_ = query_->aggegate_channel();
  } else {
    channel_ = query_->default_channel();
  }
}

namespace tiledb::api {

/**
 * Ensure the argument is a valid char pointer
 *
 * @param field_name A char pointer
 */
inline void ensure_field_name_is_valid(const char* field_name) {
  if (!field_name) {
    throw CAPIStatusException("argument `field_name` may not be nullptr");
  }
}

/**
 * Ensure the argument is a valid query field handle
 *
 * @param field A query field handle
 */
inline void ensure_query_field_is_valid(
    const tiledb_query_field_handle_t* field) {
  ensure_handle_is_valid(field);
}

capi_return_t tiledb_query_get_field(
    tiledb_query_t* query,
    const char* field_name,
    tiledb_query_field_t** field) {
  ensure_query_is_valid(query);
  ensure_field_name_is_valid(field_name);
  ensure_output_pointer_is_valid(field);

  *field = tiledb_query_field_handle_t::make_handle(query, field_name);
  return TILEDB_OK;
}

capi_return_t tiledb_query_field_free(tiledb_query_field_t** field) {
  ensure_output_pointer_is_valid(field);
  ensure_query_field_is_valid(*field);
  tiledb_query_field_handle_t::break_handle(*field);
  return TILEDB_OK;
}

capi_return_t tiledb_field_datatype(
    tiledb_query_field_t* field, tiledb_datatype_t* type) {
  ensure_query_field_is_valid(field);
  ensure_output_pointer_is_valid(type);
  *type = static_cast<tiledb_datatype_t>(field->type());
  return TILEDB_OK;
}

capi_return_t tiledb_field_cell_val_num(
    tiledb_query_field_t* field, uint32_t* cell_val_num) {
  ensure_query_field_is_valid(field);
  ensure_output_pointer_is_valid(cell_val_num);
  *cell_val_num = field->cell_val_num();
  return TILEDB_OK;
}

capi_return_t tiledb_field_get_nullable(
    tiledb_query_field_t* field, uint8_t* nullable) {
  ensure_query_field_is_valid(field);
  ensure_output_pointer_is_valid(nullable);
  *nullable = field->is_nullable();
  return TILEDB_OK;
}

capi_return_t tiledb_field_origin(
    tiledb_query_field_t* field, tiledb_field_origin_t* origin) {
  ensure_query_field_is_valid(field);
  ensure_output_pointer_is_valid(origin);
  *origin = field->origin();
  return TILEDB_OK;
}

capi_return_t tiledb_field_channel(
    tiledb_query_field_t* field, tiledb_query_channel_handle_t** channel) {
  ensure_query_field_is_valid(field);
  ensure_output_pointer_is_valid(channel);
  *channel = field->channel();
  return TILEDB_OK;
}

}  // namespace tiledb::api

using tiledb::api::api_entry_context;

CAPI_INTERFACE(
    query_get_field,
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* field_name,
    tiledb_query_field_t** field) {
  return api_entry_context<tiledb::api::tiledb_query_get_field>(
      ctx, query, field_name, field);
}

CAPI_INTERFACE(query_field_free, tiledb_ctx_t*, tiledb_query_field_t** field) {
  return tiledb::api::api_entry_plain<tiledb::api::tiledb_query_field_free>(
      field);
}

CAPI_INTERFACE(
    field_datatype,
    tiledb_ctx_t* ctx,
    tiledb_query_field_t* field,
    tiledb_datatype_t* type) {
  return api_entry_context<tiledb::api::tiledb_field_datatype>(
      ctx, field, type);
}

CAPI_INTERFACE(
    field_cell_val_num,
    tiledb_ctx_t* ctx,
    tiledb_query_field_t* field,
    uint32_t* cell_val_num) {
  return api_entry_context<tiledb::api::tiledb_field_cell_val_num>(
      ctx, field, cell_val_num);
}

CAPI_INTERFACE(
    field_get_nullable,
    tiledb_ctx_t* ctx,
    tiledb_query_field_t* field,
    uint8_t* nullable) {
  return api_entry_context<tiledb::api::tiledb_field_get_nullable>(
      ctx, field, nullable);
}

CAPI_INTERFACE(
    field_origin,
    tiledb_ctx_t* ctx,
    tiledb_query_field_t* field,
    tiledb_field_origin_t* origin) {
  return api_entry_context<tiledb::api::tiledb_field_origin>(
      ctx, field, origin);
}

CAPI_INTERFACE(
    field_channel,
    tiledb_ctx_t* ctx,
    tiledb_query_field_t* field,
    tiledb_query_channel_handle_t** channel) {
  return api_entry_context<tiledb::api::tiledb_field_channel>(
      ctx, field, channel);
}
