/**
 * @file tiledb/api/c_api/filter_list/filter_list_api.cc
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
 *
 * @section DESCRIPTION
 *
 * This file defines C API functions for the filter list section.
 */

#include "../filter/filter_api_internal.h"
#include "filter_list_api_external.h"
#include "filter_list_api_internal.h"
#include "tiledb/api/c_api_support/c_api_support.h"

namespace tiledb::api {

capi_return_t tiledb_filter_list_alloc(
    tiledb_ctx_t*, tiledb_filter_list_t** filter_list) {
  ensure_output_pointer_is_valid(filter_list);
  *filter_list = make_handle<tiledb_filter_list_t>(sm::FilterPipeline());
  return TILEDB_OK;
}

void tiledb_filter_list_free(tiledb_filter_list_t** filter_list) {
  ensure_output_pointer_is_valid(filter_list);
  ensure_filter_list_is_valid(*filter_list);
  break_handle(*filter_list);
}

capi_return_t tiledb_filter_list_add_filter(
    tiledb_filter_list_t* filter_list, tiledb_filter_t* filter) {
  ensure_filter_list_is_valid(filter_list);
  ensure_filter_is_valid(filter);
  filter_list->pipeline().add_filter(filter->filter());
  return TILEDB_OK;
}

capi_return_t tiledb_filter_list_set_max_chunk_size(
    tiledb_filter_list_t* filter_list, uint32_t max_chunk_size) {
  ensure_filter_list_is_valid(filter_list);
  filter_list->pipeline().set_max_chunk_size(max_chunk_size);
  return TILEDB_OK;
}

capi_return_t tiledb_filter_list_get_nfilters(
    const tiledb_filter_list_t* filter_list, uint32_t* nfilters) {
  ensure_filter_list_is_valid(filter_list);
  ensure_output_pointer_is_valid(nfilters);
  *nfilters = filter_list->pipeline().size();
  return TILEDB_OK;
}

capi_return_t tiledb_filter_list_get_filter_from_index(
    const tiledb_filter_list_t* filter_list,
    uint32_t index,
    tiledb_filter_t** filter) {
  ensure_filter_list_is_valid(filter_list);
  ensure_output_pointer_is_valid(filter);

  uint32_t nfilters{filter_list->pipeline().size()};
  if (index >= nfilters) {
    throw CAPIStatusException(
        "Filter " + std::to_string(index) + " out of bounds, filter list has " +
        std::to_string(nfilters) + " filters.");
  }

  auto f = filter_list->pipeline().get_filter(index);
  if (f == nullptr) {
    throw CAPIStatusException(
        "Failed to retrieve filter at index " + std::to_string(index));
  }
  *filter = make_handle<tiledb_filter_t>(f->clone());
  return TILEDB_OK;
}

capi_return_t tiledb_filter_list_get_max_chunk_size(
    const tiledb_filter_list_t* filter_list, uint32_t* max_chunk_size) {
  ensure_filter_list_is_valid(filter_list);
  ensure_output_pointer_is_valid(max_chunk_size);
  *max_chunk_size = filter_list->pipeline().max_chunk_size();
  return TILEDB_OK;
}

}  // namespace tiledb::api

using tiledb::api::api_entry_context;

CAPI_INTERFACE(
    filter_list_alloc, tiledb_ctx_t* ctx, tiledb_filter_list_t** filter_list) {
  return tiledb::api::api_entry_with_context<
      tiledb::api::tiledb_filter_list_alloc>(ctx, filter_list);
}

CAPI_INTERFACE_VOID(filter_list_free, tiledb_filter_list_t** filter_list) {
  return tiledb::api::api_entry_void<tiledb::api::tiledb_filter_list_free>(
      filter_list);
}

CAPI_INTERFACE(
    filter_list_add_filter,
    tiledb_ctx_t* ctx,
    tiledb_filter_list_t* filter_list,
    tiledb_filter_t* filter) {
  return api_entry_context<tiledb::api::tiledb_filter_list_add_filter>(
      ctx, filter_list, filter);
}

CAPI_INTERFACE(
    filter_list_set_max_chunk_size,
    tiledb_ctx_t* ctx,
    tiledb_filter_list_t* filter_list,
    uint32_t max_chunk_size) {
  return api_entry_context<tiledb::api::tiledb_filter_list_set_max_chunk_size>(
      ctx, filter_list, max_chunk_size);
}

CAPI_INTERFACE(
    filter_list_get_nfilters,
    tiledb_ctx_t* ctx,
    const tiledb_filter_list_t* filter_list,
    uint32_t* nfilters) {
  return api_entry_context<tiledb::api::tiledb_filter_list_get_nfilters>(
      ctx, filter_list, nfilters);
}

CAPI_INTERFACE(
    filter_list_get_filter_from_index,
    tiledb_ctx_t* ctx,
    const tiledb_filter_list_t* filter_list,
    uint32_t index,
    tiledb_filter_t** filter) {
  return api_entry_context<
      tiledb::api::tiledb_filter_list_get_filter_from_index>(
      ctx, filter_list, index, filter);
}

CAPI_INTERFACE(
    filter_list_get_max_chunk_size,
    tiledb_ctx_t* ctx,
    const tiledb_filter_list_t* filter_list,
    uint32_t* max_chunk_size) {
  return api_entry_context<tiledb::api::tiledb_filter_list_get_max_chunk_size>(
      ctx, filter_list, max_chunk_size);
}
