/**
 * @file tiledb/api/c_api/subarray/subarray_api.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * This file defines C API functions for the fragment info section.
 */

#include "subarray_api_experimental.h"
#include "subarray_api_internal.h"
#include "tiledb/api/c_api/array/array_api_internal.h"
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/api/c_api_support/c_api_support.h"

namespace tiledb::api {

capi_return_t tiledb_subarray_alloc(
    tiledb_ctx_t* ctx,
    const tiledb_array_t* array,
    tiledb_subarray_t** subarray) {
  ensure_array_is_valid(array);
  ensure_output_pointer_is_valid(subarray);

  // Error if array is not open
  if (!array->is_open()) {
    throw CAPIException("Cannot create subarray; array is not open");
  }

  // Create Subarray object
  *subarray = tiledb_subarray_t::make_handle(
      array->array().get(),
      (tiledb::sm::stats::Stats*)nullptr,
      ctx->resources().logger(),
      true);
  return TILEDB_OK;
}

void tiledb_subarray_free(tiledb_subarray_t** subarray) {
  ensure_output_pointer_is_valid(subarray);
  ensure_subarray_is_valid(*subarray);
  tiledb_subarray_t::break_handle(*subarray);
}

capi_return_t tiledb_subarray_set_config(
    tiledb_subarray_t* subarray, tiledb_config_t* config) {
  ensure_subarray_is_valid(subarray);
  ensure_config_is_valid(config);
  subarray->set_config(tiledb::sm::QueryType::READ, config->config());
  return TILEDB_OK;
}

capi_return_t tiledb_subarray_set_coalesce_ranges(
    tiledb_subarray_t* subarray, int coalesce_ranges) {
  ensure_subarray_is_valid(subarray);
  subarray->set_coalesce_ranges(coalesce_ranges != 0);
  return TILEDB_OK;
}

capi_return_t tiledb_subarray_set_subarray(
    tiledb_subarray_t* subarray, const void* subarray_vals) {
  ensure_subarray_is_valid(subarray);
  subarray->set_subarray(subarray_vals);
  return TILEDB_OK;
}

capi_return_t tiledb_subarray_add_point_ranges(
    tiledb_subarray_t* subarray,
    uint32_t dim_idx,
    const void* start,
    uint64_t count) {
  ensure_subarray_is_valid(subarray);
  subarray->add_point_ranges(dim_idx, start, count);
  return TILEDB_OK;
}

capi_return_t tiledb_subarray_add_range(
    tiledb_subarray_t* subarray,
    uint32_t dim_idx,
    const void* start,
    const void* end,
    const void* stride) {
  ensure_subarray_is_valid(subarray);
  ensure_unsupported_stride_is_null(stride);
  subarray->add_range(dim_idx, start, end);
  return TILEDB_OK;
}

capi_return_t tiledb_subarray_add_range_by_name(
    tiledb_subarray_t* subarray,
    const char* dim_name,
    const void* start,
    const void* end,
    const void* stride) {
  ensure_subarray_is_valid(subarray);
  ensure_unsupported_stride_is_null(stride);
  subarray->add_range_by_name(dim_name, start, end);
  return TILEDB_OK;
}

capi_return_t tiledb_subarray_add_range_var(
    tiledb_subarray_t* subarray,
    uint32_t dim_idx,
    const void* start,
    uint64_t start_size,
    const void* end,
    uint64_t end_size) {
  ensure_subarray_is_valid(subarray);
  subarray->add_range_var(dim_idx, start, start_size, end, end_size);
  return TILEDB_OK;
}

capi_return_t tiledb_subarray_add_range_var_by_name(
    tiledb_subarray_t* subarray,
    const char* dim_name,
    const void* start,
    uint64_t start_size,
    const void* end,
    uint64_t end_size) {
  ensure_subarray_is_valid(subarray);
  subarray->add_range_var_by_name(dim_name, start, start_size, end, end_size);
  return TILEDB_OK;
}

capi_return_t tiledb_subarray_get_range_num(
    const tiledb_subarray_t* subarray, uint32_t dim_idx, uint64_t* range_num) {
  ensure_subarray_is_valid(subarray);
  ensure_output_pointer_is_valid(range_num);
  subarray->get_range_num(dim_idx, range_num);
  return TILEDB_OK;
}

capi_return_t tiledb_subarray_get_range_num_from_name(
    const tiledb_subarray_t* subarray,
    const char* dim_name,
    uint64_t* range_num) {
  ensure_subarray_is_valid(subarray);
  ensure_output_pointer_is_valid(range_num);
  subarray->get_range_num_from_name(dim_name, range_num);
  return TILEDB_OK;
}

capi_return_t tiledb_subarray_get_range(
    const tiledb_subarray_t* subarray,
    uint32_t dim_idx,
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
  subarray->get_range(dim_idx, range_idx, start, end);
  return TILEDB_OK;
}

capi_return_t tiledb_subarray_get_range_from_name(
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
  subarray->get_range_from_name(dim_name, range_idx, start, end);
  return TILEDB_OK;
}

capi_return_t tiledb_subarray_get_range_var_size(
    const tiledb_subarray_t* subarray,
    uint32_t dim_idx,
    uint64_t range_idx,
    uint64_t* start_size,
    uint64_t* end_size) {
  ensure_subarray_is_valid(subarray);
  ensure_output_pointer_is_valid(start_size);
  ensure_output_pointer_is_valid(end_size);
  subarray->get_range_var_size(dim_idx, range_idx, start_size, end_size);
  return TILEDB_OK;
}

capi_return_t tiledb_subarray_get_range_var_size_from_name(
    const tiledb_subarray_t* subarray,
    const char* dim_name,
    uint64_t range_idx,
    uint64_t* start_size,
    uint64_t* end_size) {
  ensure_subarray_is_valid(subarray);
  ensure_output_pointer_is_valid(start_size);
  ensure_output_pointer_is_valid(end_size);
  subarray->get_range_var_size_from_name(
      dim_name, range_idx, start_size, end_size);
  return TILEDB_OK;
}

capi_return_t tiledb_subarray_get_range_var(
    const tiledb_subarray_t* subarray,
    uint32_t dim_idx,
    uint64_t range_idx,
    void* start,
    void* end) {
  ensure_subarray_is_valid(subarray);
  ensure_output_pointer_is_valid(start);
  ensure_output_pointer_is_valid(end);
  subarray->get_range_var(dim_idx, range_idx, start, end);
  return TILEDB_OK;
}

capi_return_t tiledb_subarray_get_range_var_from_name(
    const tiledb_subarray_t* subarray,
    const char* dim_name,
    uint64_t range_idx,
    void* start,
    void* end) {
  ensure_subarray_is_valid(subarray);
  ensure_output_pointer_is_valid(start);
  ensure_output_pointer_is_valid(end);
  subarray->get_range_var_from_name(dim_name, range_idx, start, end);
  return TILEDB_OK;
}

}  // namespace tiledb::api

using tiledb::api::api_entry_context;
using tiledb::api::api_entry_void;
using tiledb::api::api_entry_with_context;

CAPI_INTERFACE(
    subarray_alloc,
    tiledb_ctx_t* ctx,
    const tiledb_array_t* array,
    tiledb_subarray_t** subarray) {
  return api_entry_with_context<tiledb::api::tiledb_subarray_alloc>(
      ctx, array, subarray);
}

CAPI_INTERFACE_VOID(subarray_free, tiledb_subarray_t** subarray) {
  return api_entry_void<tiledb::api::tiledb_subarray_free>(subarray);
}

CAPI_INTERFACE(
    subarray_set_config,
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray,
    tiledb_config_t* config) {
  return api_entry_context<tiledb::api::tiledb_subarray_set_config>(
      ctx, subarray, config);
}

CAPI_INTERFACE(
    subarray_set_coalesce_ranges,
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray,
    int coalesce_ranges) {
  return api_entry_context<tiledb::api::tiledb_subarray_set_coalesce_ranges>(
      ctx, subarray, coalesce_ranges);
}

CAPI_INTERFACE(
    subarray_set_subarray,
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray_obj,
    const void* subarray_vals) {
  return api_entry_context<tiledb::api::tiledb_subarray_set_subarray>(
      ctx, subarray_obj, subarray_vals);
}

CAPI_INTERFACE(
    subarray_add_point_ranges,
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray,
    uint32_t dim_idx,
    const void* start,
    uint64_t count) {
  return api_entry_context<tiledb::api::tiledb_subarray_add_point_ranges>(
      ctx, subarray, dim_idx, start, count);
}

CAPI_INTERFACE(
    subarray_add_range,
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray,
    uint32_t dim_idx,
    const void* start,
    const void* end,
    const void* stride) {
  return api_entry_context<tiledb::api::tiledb_subarray_add_range>(
      ctx, subarray, dim_idx, start, end, stride);
}

CAPI_INTERFACE(
    subarray_add_range_by_name,
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray,
    const char* dim_name,
    const void* start,
    const void* end,
    const void* stride) {
  return api_entry_context<tiledb::api::tiledb_subarray_add_range_by_name>(
      ctx, subarray, dim_name, start, end, stride);
}

CAPI_INTERFACE(
    subarray_add_range_var,
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray,
    uint32_t dim_idx,
    const void* start,
    uint64_t start_size,
    const void* end,
    uint64_t end_size) {
  return api_entry_context<tiledb::api::tiledb_subarray_add_range_var>(
      ctx, subarray, dim_idx, start, start_size, end, end_size);
}

CAPI_INTERFACE(
    subarray_add_range_var_by_name,
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray,
    const char* dim_name,
    const void* start,
    uint64_t start_size,
    const void* end,
    uint64_t end_size) {
  return api_entry_context<tiledb::api::tiledb_subarray_add_range_var_by_name>(
      ctx, subarray, dim_name, start, start_size, end, end_size);
}

CAPI_INTERFACE(
    subarray_get_range_num,
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    uint32_t dim_idx,
    uint64_t* range_num) {
  return api_entry_context<tiledb::api::tiledb_subarray_get_range_num>(
      ctx, subarray, dim_idx, range_num);
}

CAPI_INTERFACE(
    subarray_get_range_num_from_name,
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    const char* dim_name,
    uint64_t* range_num) {
  return api_entry_context<
      tiledb::api::tiledb_subarray_get_range_num_from_name>(
      ctx, subarray, dim_name, range_num);
}

CAPI_INTERFACE(
    subarray_get_range,
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    uint32_t dim_idx,
    uint64_t range_idx,
    const void** start,
    const void** end,
    const void** stride) {
  return api_entry_context<tiledb::api::tiledb_subarray_get_range>(
      ctx, subarray, dim_idx, range_idx, start, end, stride);
}

CAPI_INTERFACE(
    subarray_get_range_from_name,
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    const char* dim_name,
    uint64_t range_idx,
    const void** start,
    const void** end,
    const void** stride) {
  return api_entry_context<tiledb::api::tiledb_subarray_get_range_from_name>(
      ctx, subarray, dim_name, range_idx, start, end, stride);
}

CAPI_INTERFACE(
    subarray_get_range_var_size,
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    uint32_t dim_idx,
    uint64_t range_idx,
    uint64_t* start_size,
    uint64_t* end_size) {
  return api_entry_context<tiledb::api::tiledb_subarray_get_range_var_size>(
      ctx, subarray, dim_idx, range_idx, start_size, end_size);
}

CAPI_INTERFACE(
    subarray_get_range_var_size_from_name,
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    const char* dim_name,
    uint64_t range_idx,
    uint64_t* start_size,
    uint64_t* end_size) {
  return api_entry_context<
      tiledb::api::tiledb_subarray_get_range_var_size_from_name>(
      ctx, subarray, dim_name, range_idx, start_size, end_size);
}

CAPI_INTERFACE(
    subarray_get_range_var,
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    uint32_t dim_idx,
    uint64_t range_idx,
    void* start,
    void* end) {
  return api_entry_context<tiledb::api::tiledb_subarray_get_range_var>(
      ctx, subarray, dim_idx, range_idx, start, end);
}

CAPI_INTERFACE(
    subarray_get_range_var_from_name,
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    const char* dim_name,
    uint64_t range_idx,
    void* start,
    void* end) {
  return api_entry_context<
      tiledb::api::tiledb_subarray_get_range_var_from_name>(
      ctx, subarray, dim_name, range_idx, start, end);
}
