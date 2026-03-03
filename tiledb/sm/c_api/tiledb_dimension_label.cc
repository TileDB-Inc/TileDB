/**
 * @file tiledb/sm/c_api/tiledb_dimension_label.cc
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
 */

#include "tiledb/api/c_api/array_schema/array_schema_api_internal.h"
#include "tiledb/api/c_api/attribute/attribute_api_internal.h"
#include "tiledb/api/c_api/dimension/dimension_api_internal.h"
#include "tiledb/api/c_api/dimension_label/dimension_label_api_internal.h"
#include "tiledb/api/c_api/filter_list/filter_list_api_internal.h"
#include "tiledb/api/c_api/subarray/subarray_api_internal.h"
#include "tiledb/api/c_api_support/c_api_support.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_dimension_label_experimental.h"

namespace tiledb::api {

capi_return_t tiledb_array_schema_add_dimension_label(
    tiledb_array_schema_t* array_schema,
    const uint32_t dim_index,
    const char* name,
    tiledb_data_order_t label_order,
    tiledb_datatype_t label_type) {
  ensure_array_schema_is_valid(array_schema);
  array_schema->add_dimension_label(
      dim_index,
      name,
      static_cast<tiledb::sm::DataOrder>(label_order),
      static_cast<tiledb::sm::Datatype>(label_type));
  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_get_dimension_label_from_name(
    tiledb_array_schema_t* array_schema,
    const char* label_name,
    tiledb_dimension_label_t** dim_label) {
  ensure_array_schema_is_valid(array_schema);
  ensure_output_pointer_is_valid(dim_label);
  *dim_label = make_handle<tiledb_dimension_label_t>(
      array_schema->array_uri(), array_schema->dimension_label(label_name));
  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_has_dimension_label(
    const tiledb_array_schema_t* array_schema,
    const char* name,
    int32_t* has_dim_label) {
  ensure_array_schema_is_valid(array_schema);
  bool is_dim_label = array_schema->is_dim_label(name);
  *has_dim_label = is_dim_label ? 1 : 0;
  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_set_dimension_label_filter_list(
    tiledb_array_schema_t* array_schema,
    const char* label_name,
    tiledb_filter_list_t* filter_list) {
  ensure_array_schema_is_valid(array_schema);
  ensure_filter_list_is_valid(filter_list);
  array_schema->set_dimension_label_filter_pipeline(
      label_name, filter_list->pipeline());
  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_set_dimension_label_tile_extent(
    tiledb_array_schema_t* array_schema,
    const char* label_name,
    tiledb_datatype_t type,
    const void* tile_extent) {
  ensure_array_schema_is_valid(array_schema);
  array_schema->set_dimension_label_tile_extent(
      label_name, static_cast<tiledb::sm::Datatype>(type), tile_extent);
  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_get_dimension_label_num(
    tiledb_array_schema_t* array_schema, uint64_t* dim_label_num) {
  ensure_array_schema_is_valid(array_schema);
  ensure_output_pointer_is_valid(dim_label_num);
  *dim_label_num = array_schema->dim_label_num();
  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_get_dimension_label_from_index(
    tiledb_array_schema_t* array_schema,
    uint64_t dim_label_index,
    tiledb_dimension_label_t** dim_label) {
  ensure_array_schema_is_valid(array_schema);
  ensure_output_pointer_is_valid(dim_label);
  *dim_label = make_handle<tiledb_dimension_label_t>(
      array_schema->array_uri(),
      array_schema->dimension_label(dim_label_index));
  return TILEDB_OK;
}

capi_return_t tiledb_subarray_add_label_range(
    tiledb_subarray_t* subarray,
    const char* label_name,
    const void* start,
    const void* end,
    const void* stride) {
  ensure_subarray_is_valid(subarray);
  ensure_unsupported_stride_is_null(stride);
  subarray->add_label_range(label_name, start, end);
  return TILEDB_OK;
}

capi_return_t tiledb_subarray_add_label_range_var(
    tiledb_subarray_t* subarray,
    const char* label_name,
    const void* start,
    uint64_t start_size,
    const void* end,
    uint64_t end_size) {
  ensure_subarray_is_valid(subarray);
  subarray->add_label_range_var(label_name, start, start_size, end, end_size);
  return TILEDB_OK;
}

capi_return_t tiledb_subarray_get_label_name(
    tiledb_subarray_t* subarray, uint32_t dim_idx, const char** label_name) {
  ensure_subarray_is_valid(subarray);
  ensure_output_pointer_is_valid(label_name);
  const auto& name = subarray->get_label_name(dim_idx);
  *label_name = name.c_str();
  return TILEDB_OK;
}

capi_return_t tiledb_subarray_get_label_range(
    const tiledb_subarray_t* subarray,
    const char* dim_name,
    uint64_t range_idx,
    const void** start,
    const void** end,
    const void** stride) {
  ensure_subarray_is_valid(subarray);
  ensure_output_pointer_is_valid(start);
  ensure_output_pointer_is_valid(end);
  if (stride != nullptr) {
    *stride = nullptr;
  }
  subarray->get_label_range(dim_name, range_idx, start, end);
  return TILEDB_OK;
}

capi_return_t tiledb_subarray_get_label_range_num(
    const tiledb_subarray_t* subarray,
    const char* dim_name,
    uint64_t* range_num) {
  ensure_subarray_is_valid(subarray);
  ensure_output_pointer_is_valid(range_num);
  subarray->get_label_range_num(dim_name, range_num);
  return TILEDB_OK;
}

capi_return_t tiledb_subarray_get_label_range_var(
    const tiledb_subarray_t* subarray,
    const char* dim_name,
    uint64_t range_idx,
    void* start,
    void* end) {
  ensure_subarray_is_valid(subarray);
  ensure_output_pointer_is_valid(start);
  ensure_output_pointer_is_valid(end);
  subarray->get_label_range_var(dim_name, range_idx, start, end);
  return TILEDB_OK;
}

capi_return_t tiledb_subarray_get_label_range_var_size(
    const tiledb_subarray_t* subarray,
    const char* dim_name,
    uint64_t range_idx,
    uint64_t* start_size,
    uint64_t* end_size) {
  ensure_subarray_is_valid(subarray);
  ensure_output_pointer_is_valid(start_size);
  ensure_output_pointer_is_valid(end_size);
  subarray->get_label_range_var_size(dim_name, range_idx, start_size, end_size);
  return TILEDB_OK;
}

capi_return_t tiledb_subarray_has_label_ranges(
    const tiledb_subarray_t* subarray,
    const uint32_t dim_idx,
    int32_t* has_label_ranges) {
  ensure_subarray_is_valid(subarray);
  ensure_output_pointer_is_valid(has_label_ranges);
  bool has_ranges = subarray->has_label_ranges(dim_idx);
  *has_label_ranges = has_ranges ? 1 : 0;
  return TILEDB_OK;
}

}  // namespace tiledb::api

using tiledb::api::api_entry_context;
using tiledb::api::api_entry_with_context;

template <auto f>
constexpr auto api_entry = tiledb::api::api_entry_with_context<f>;

CAPI_INTERFACE(
    array_schema_add_dimension_label,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    const uint32_t dim_index,
    const char* name,
    tiledb_data_order_t label_order,
    tiledb_datatype_t label_type) {
  return api_entry_context<
      tiledb::api::tiledb_array_schema_add_dimension_label>(
      ctx, array_schema, dim_index, name, label_order, label_type);
}

CAPI_INTERFACE(
    array_schema_get_dimension_label_from_name,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    const char* label_name,
    tiledb_dimension_label_t** dim_label) {
  return api_entry_context<
      tiledb::api::tiledb_array_schema_get_dimension_label_from_name>(
      ctx, array_schema, label_name, dim_label);
}

CAPI_INTERFACE(
    array_schema_has_dimension_label,
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    const char* name,
    int32_t* has_dim_label) {
  return api_entry_context<
      tiledb::api::tiledb_array_schema_has_dimension_label>(
      ctx, array_schema, name, has_dim_label);
}

CAPI_INTERFACE(
    array_schema_set_dimension_label_filter_list,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    const char* label_name,
    tiledb_filter_list_t* filter_list) {
  return api_entry_context<
      tiledb::api::tiledb_array_schema_set_dimension_label_filter_list>(
      ctx, array_schema, label_name, filter_list);
}

CAPI_INTERFACE(
    array_schema_set_dimension_label_tile_extent,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    const char* label_name,
    tiledb_datatype_t type,
    const void* tile_extent) {
  return api_entry_context<
      tiledb::api::tiledb_array_schema_set_dimension_label_tile_extent>(
      ctx, array_schema, label_name, type, tile_extent);
}

CAPI_INTERFACE(
    array_schema_get_dimension_label_num,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    uint64_t* dim_label_num) {
  return api_entry_context<
      tiledb::api::tiledb_array_schema_get_dimension_label_num>(
      ctx, array_schema, dim_label_num);
}

CAPI_INTERFACE(
    array_schema_get_dimension_label_from_index,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    uint64_t dim_label_index,
    tiledb_dimension_label_t** dim_label) {
  return api_entry_context<
      tiledb::api::tiledb_array_schema_get_dimension_label_from_index>(
      ctx, array_schema, dim_label_index, dim_label);
}

CAPI_INTERFACE(
    subarray_add_label_range,
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray,
    const char* label_name,
    const void* start,
    const void* end,
    const void* stride) {
  return api_entry_context<tiledb::api::tiledb_subarray_add_label_range>(
      ctx, subarray, label_name, start, end, stride);
}

CAPI_INTERFACE(
    subarray_add_label_range_var,
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray,
    const char* label_name,
    const void* start,
    uint64_t start_size,
    const void* end,
    uint64_t end_size) {
  return api_entry_context<tiledb::api::tiledb_subarray_add_label_range_var>(
      ctx, subarray, label_name, start, start_size, end, end_size);
}

CAPI_INTERFACE(
    subarray_get_label_name,
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray,
    uint32_t dim_idx,
    const char** label_name) {
  return api_entry_context<tiledb::api::tiledb_subarray_get_label_name>(
      ctx, subarray, dim_idx, label_name);
}

CAPI_INTERFACE(
    subarray_get_label_range,
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    const char* dim_name,
    uint64_t range_idx,
    const void** start,
    const void** end,
    const void** stride) {
  return api_entry_context<tiledb::api::tiledb_subarray_get_label_range>(
      ctx, subarray, dim_name, range_idx, start, end, stride);
}

CAPI_INTERFACE(
    subarray_get_label_range_num,
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    const char* dim_name,
    uint64_t* range_num) {
  return api_entry_context<tiledb::api::tiledb_subarray_get_label_range_num>(
      ctx, subarray, dim_name, range_num);
}

CAPI_INTERFACE(
    subarray_get_label_range_var,
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    const char* dim_name,
    uint64_t range_idx,
    void* start,
    void* end) {
  return api_entry_context<tiledb::api::tiledb_subarray_get_label_range_var>(
      ctx, subarray, dim_name, range_idx, start, end);
}

CAPI_INTERFACE(
    subarray_get_label_range_var_size,
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    const char* dim_name,
    uint64_t range_idx,
    uint64_t* start_size,
    uint64_t* end_size) {
  return api_entry_context<
      tiledb::api::tiledb_subarray_get_label_range_var_size>(
      ctx, subarray, dim_name, range_idx, start_size, end_size);
}

CAPI_INTERFACE(
    subarray_has_label_ranges,
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    const uint32_t dim_idx,
    int32_t* has_label_ranges) {
  return api_entry_context<tiledb::api::tiledb_subarray_has_label_ranges>(
      ctx, subarray, dim_idx, has_label_ranges);
}
