/**
 * @file tiledb/sm/c_api/tiledb_dimension_label.cc
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
 */

#include "tiledb/sm/c_api/tiledb_dimension_label.h"
#include "tiledb/api/c_api/filter_list/filter_list_api_internal.h"
#include "tiledb/api/c_api_support/c_api_support.h"
#include "tiledb/sm/array_schema/dimension_label_reference.h"
#include "tiledb/sm/c_api/api_argument_validator.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_experimental.h"
#include "tiledb/sm/group/group_v1.h"
#include "tiledb/sm/rest/rest_client.h"

using namespace tiledb::common;

namespace tiledb::common::detail {

int32_t tiledb_array_schema_add_dimension_label(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    const uint32_t dim_index,
    const char* name,
    tiledb_data_order_t label_order,
    tiledb_datatype_t label_type) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, array_schema)) {
    return TILEDB_ERR;
  }
  array_schema->array_schema_->add_dimension_label(
      dim_index,
      name,
      static_cast<tiledb::sm::DataOrder>(label_order),
      static_cast<tiledb::sm::Datatype>(label_type));
  return TILEDB_OK;
}

int32_t tiledb_array_schema_has_dimension_label(
    const tiledb_array_schema_t* array_schema,
    const char* name,
    int32_t* has_dim_label) {
  bool is_dim_label = array_schema->array_schema_->is_dim_label(name);
  *has_dim_label = is_dim_label ? 1 : 0;
  return TILEDB_OK;
}

int32_t tiledb_array_schema_set_dimension_label_filter_list(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    const char* label_name,
    tiledb_filter_list_t* filter_list) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, array_schema)) {
    return TILEDB_ERR;
  }
  api::ensure_filter_list_is_valid(filter_list);
  array_schema->array_schema_->set_dimension_label_filter_pipeline(
      label_name, filter_list->pipeline());
  return TILEDB_OK;
}

int32_t tiledb_array_schema_set_dimension_label_tile_extent(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    const char* label_name,
    tiledb_datatype_t type,
    const void* tile_extent) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, array_schema)) {
    return TILEDB_ERR;
  }
  array_schema->array_schema_->set_dimension_label_tile_extent(
      label_name, static_cast<tiledb::sm::Datatype>(type), tile_extent);
  return TILEDB_OK;
}

int32_t tiledb_subarray_add_label_range(
    tiledb_subarray_t* subarray,
    const char* label_name,
    const void* start,
    const void* end,
    const void* stride) {
  subarray->subarray_->add_label_range(label_name, start, end, stride);
  return TILEDB_OK;
}

int32_t tiledb_subarray_add_label_range_var(
    tiledb_subarray_t* subarray,
    const char* label_name,
    const void* start,
    uint64_t start_size,
    const void* end,
    uint64_t end_size) {
  subarray->subarray_->add_label_range_var(
      label_name, start, start_size, end, end_size);
  return TILEDB_OK;
}

int32_t tiledb_subarray_get_label_range(
    const tiledb_subarray_t* subarray,
    const char* dim_name,
    uint64_t range_idx,
    const void** start,
    const void** end,
    const void** stride) {
  subarray->subarray_->get_label_range(dim_name, range_idx, start, end, stride);
  return TILEDB_OK;
}

int32_t tiledb_subarray_get_label_range_num(
    const tiledb_subarray_t* subarray,
    const char* dim_name,
    uint64_t* range_num) {
  subarray->subarray_->get_label_range_num(dim_name, range_num);
  return TILEDB_OK;
}

int32_t tiledb_subarray_get_label_range_var(
    const tiledb_subarray_t* subarray,
    const char* dim_name,
    uint64_t range_idx,
    void* start,
    void* end) {
  subarray->subarray_->get_label_range_var(dim_name, range_idx, start, end);
  return TILEDB_OK;
}

int32_t tiledb_subarray_get_label_range_var_size(
    const tiledb_subarray_t* subarray,
    const char* dim_name,
    uint64_t range_idx,
    uint64_t* start_size,
    uint64_t* end_size) {
  subarray->subarray_->get_label_range_var_size(
      dim_name, range_idx, start_size, end_size);
  return TILEDB_OK;
}

}  // namespace tiledb::common::detail

using tiledb::api::api_entry_context;
using tiledb::api::api_entry_with_context;

template <auto f>
constexpr auto api_entry = tiledb::api::api_entry_with_context<f>;

int32_t tiledb_array_schema_add_dimension_label(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    const uint32_t dim_index,
    const char* name,
    tiledb_data_order_t label_order,
    tiledb_datatype_t label_type) noexcept {
  return api_entry<detail::tiledb_array_schema_add_dimension_label>(
      ctx, array_schema, dim_index, name, label_order, label_type);
}

int32_t tiledb_array_schema_has_dimension_label(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    const char* name,
    int32_t* has_dim_label) noexcept {
  return api_entry_context<detail::tiledb_array_schema_has_dimension_label>(
      ctx, array_schema, name, has_dim_label);
}

int32_t tiledb_array_schema_set_dimension_label_filter_list(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    const char* label_name,
    tiledb_filter_list_t* filter_list) noexcept {
  return api_entry<detail::tiledb_array_schema_set_dimension_label_filter_list>(
      ctx, array_schema, label_name, filter_list);
}

int32_t tiledb_array_schema_set_dimension_label_tile_extent(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    const char* label_name,
    tiledb_datatype_t type,
    const void* tile_extent) noexcept {
  return api_entry<detail::tiledb_array_schema_set_dimension_label_tile_extent>(
      ctx, array_schema, label_name, type, tile_extent);
}

int32_t tiledb_subarray_add_label_range(
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray,
    const char* label_name,
    const void* start,
    const void* end,
    const void* stride) noexcept {
  return api_entry_context<detail::tiledb_subarray_add_label_range>(
      ctx, subarray, label_name, start, end, stride);
}

int32_t tiledb_subarray_add_label_range_var(
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray,
    const char* label_name,
    const void* start,
    uint64_t start_size,
    const void* end,
    uint64_t end_size) noexcept {
  return api_entry_context<detail::tiledb_subarray_add_label_range_var>(
      ctx, subarray, label_name, start, start_size, end, end_size);
}

int32_t tiledb_subarray_get_label_range(
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    const char* dim_name,
    uint64_t range_idx,
    const void** start,
    const void** end,
    const void** stride) noexcept {
  return api_entry_context<detail::tiledb_subarray_get_label_range>(
      ctx, subarray, dim_name, range_idx, start, end, stride);
}

int32_t tiledb_subarray_get_label_range_num(
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    const char* dim_name,
    uint64_t* range_num) noexcept {
  return api_entry_context<detail::tiledb_subarray_get_label_range_num>(
      ctx, subarray, dim_name, range_num);
}

int32_t tiledb_subarray_get_label_range_var(
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    const char* dim_name,
    uint64_t range_idx,
    void* start,
    void* end) noexcept {
  return api_entry_context<detail::tiledb_subarray_get_label_range_var>(
      ctx, subarray, dim_name, range_idx, start, end);
}

int32_t tiledb_subarray_get_label_range_var_size(
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    const char* dim_name,
    uint64_t range_idx,
    uint64_t* start_size,
    uint64_t* end_size) noexcept {
  return api_entry_context<detail::tiledb_subarray_get_label_range_var_size>(
      ctx, subarray, dim_name, range_idx, start_size, end_size);
}
