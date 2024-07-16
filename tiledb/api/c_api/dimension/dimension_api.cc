/**
 * @file tiledb/api/c_api/dimension/dimension_api.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023-2024 TileDB, Inc.
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
 * This file defines C API functions for the dimension section.
 */

#include "../../c_api_support/c_api_support.h"
#include "../filter_list/filter_list_api_internal.h"
#include "../string/string_api_internal.h"
#include "dimension_api_external.h"
#include "dimension_api_internal.h"
#include "tiledb/api/c_api_support/exception_wrapper/exception_wrapper.h"
#include "tiledb/common/memory_tracker.h"

namespace tiledb::api {

int32_t tiledb_dimension_alloc(
    tiledb_ctx_handle_t* ctx,
    const char* name,
    tiledb_datatype_t type,
    const void* dim_domain,
    const void* tile_extent,
    tiledb_dimension_t** dim) {
  if (name == nullptr) {
    throw CAPIStatusException("Dimension name must not be NULL");
  }
  ensure_output_pointer_is_valid(dim);
  auto memory_tracker = ctx->resources().create_memory_tracker();
  memory_tracker->set_type(sm::MemoryTrackerType::ARRAY_CREATE);
  *dim = tiledb_dimension_handle_t::make_handle(
      name, static_cast<tiledb::sm::Datatype>(type), memory_tracker);
  try {
    (*dim)->set_domain(dim_domain);
    (*dim)->set_tile_extent(tile_extent);
  } catch (...) {
    tiledb_dimension_handle_t::break_handle(*dim);
    throw;
  }
  // Success
  return TILEDB_OK;
}

void tiledb_dimension_free(tiledb_dimension_t** dim) {
  ensure_output_pointer_is_valid(dim);
  ensure_dimension_is_valid(*dim);
  tiledb_dimension_handle_t::break_handle(*dim);
}

int32_t tiledb_dimension_set_filter_list(
    tiledb_dimension_t* dim, tiledb_filter_list_t* filter_list) {
  ensure_dimension_is_valid(dim);
  ensure_filter_list_is_valid(filter_list);
  dim->set_filter_pipeline(filter_list->pipeline());
  return TILEDB_OK;
}

int32_t tiledb_dimension_set_cell_val_num(
    tiledb_dimension_t* dim, uint32_t cell_val_num) {
  ensure_dimension_is_valid(dim);
  dim->set_cell_val_num(cell_val_num);
  return TILEDB_OK;
}

int32_t tiledb_dimension_get_filter_list(
    tiledb_dimension_t* dim, tiledb_filter_list_t** filter_list) {
  ensure_dimension_is_valid(dim);
  ensure_output_pointer_is_valid(filter_list);
  // Copy-construct a separate FilterPipeline object
  *filter_list =
      tiledb_filter_list_t::make_handle(sm::FilterPipeline{dim->filters()});
  return TILEDB_OK;
}

int32_t tiledb_dimension_get_cell_val_num(
    const tiledb_dimension_t* dim, uint32_t* cell_val_num) {
  ensure_dimension_is_valid(dim);
  ensure_output_pointer_is_valid(cell_val_num);
  *cell_val_num = dim->cell_val_num();
  return TILEDB_OK;
}

int32_t tiledb_dimension_get_name(
    const tiledb_dimension_t* dim, const char** name) {
  ensure_dimension_is_valid(dim);
  ensure_output_pointer_is_valid(name);
  *name = dim->name().c_str();
  return TILEDB_OK;
}

int32_t tiledb_dimension_get_type(
    const tiledb_dimension_t* dim, tiledb_datatype_t* type) {
  ensure_dimension_is_valid(dim);
  ensure_output_pointer_is_valid(type);
  *type = static_cast<tiledb_datatype_t>(dim->type());
  return TILEDB_OK;
}

int32_t tiledb_dimension_get_domain(
    const tiledb_dimension_t* dim, const void** domain) {
  ensure_dimension_is_valid(dim);
  ensure_output_pointer_is_valid(domain);
  *domain = dim->domain().data();
  return TILEDB_OK;
}

int32_t tiledb_dimension_get_tile_extent(
    const tiledb_dimension_t* dim, const void** tile_extent) {
  ensure_dimension_is_valid(dim);
  ensure_output_pointer_is_valid(tile_extent);
  *tile_extent = dim->tile_extent().data();
  return TILEDB_OK;
}

int32_t tiledb_dimension_dump(const tiledb_dimension_t* dim, FILE* out) {
  ensure_dimension_is_valid(dim);
  ensure_cstream_handle_is_valid(out);

  std::stringstream ss;
  ss << *dim;
  size_t r = fwrite(ss.str().c_str(), sizeof(char), ss.str().size(), out);
  if (r != ss.str().size()) {
    throw CAPIException(
        "Error writing dimension " + dim->name() + " to output stream");
  }

  return TILEDB_OK;
}

int32_t tiledb_dimension_dump_str(
    const tiledb_dimension_t* dim, tiledb_string_handle_t** out) {
  ensure_dimension_is_valid(dim);
  ensure_output_pointer_is_valid(out);

  std::stringstream ss;
  ss << *dim;
  *out = tiledb_string_handle_t::make_handle(ss.str());
  return TILEDB_OK;
}

}  // namespace tiledb::api

std::ostream& operator<<(
    std::ostream& os, const tiledb_dimension_handle_t& dim) {
  os << *dim.dimension_;
  return os;
}

using tiledb::api::api_entry_context;

CAPI_INTERFACE(
    dimension_alloc,
    tiledb_ctx_t* ctx,
    const char* name,
    tiledb_datatype_t type,
    const void* dim_domain,
    const void* tile_extent,
    tiledb_dimension_t** dim) {
  return tiledb::api::api_entry_with_context<
      tiledb::api::tiledb_dimension_alloc>(
      ctx, name, type, dim_domain, tile_extent, dim);
}

CAPI_INTERFACE_VOID(dimension_free, tiledb_dimension_t** dim) {
  return tiledb::api::api_entry_void<tiledb::api::tiledb_dimension_free>(dim);
}

CAPI_INTERFACE(
    dimension_set_filter_list,
    tiledb_ctx_t* ctx,
    tiledb_dimension_t* dim,
    tiledb_filter_list_t* filter_list) {
  return api_entry_context<tiledb::api::tiledb_dimension_set_filter_list>(
      ctx, dim, filter_list);
}

CAPI_INTERFACE(
    dimension_set_cell_val_num,
    tiledb_ctx_t* ctx,
    tiledb_dimension_t* dim,
    uint32_t cell_val_num) {
  return api_entry_context<tiledb::api::tiledb_dimension_set_cell_val_num>(
      ctx, dim, cell_val_num);
}

CAPI_INTERFACE(
    dimension_get_filter_list,
    tiledb_ctx_t* ctx,
    tiledb_dimension_t* dim,
    tiledb_filter_list_t** filter_list) {
  return api_entry_context<tiledb::api::tiledb_dimension_get_filter_list>(
      ctx, dim, filter_list);
}

CAPI_INTERFACE(
    dimension_get_cell_val_num,
    tiledb_ctx_t* ctx,
    const tiledb_dimension_t* dim,
    uint32_t* cell_val_num) {
  return api_entry_context<tiledb::api::tiledb_dimension_get_cell_val_num>(
      ctx, dim, cell_val_num);
}

CAPI_INTERFACE(
    dimension_get_name,
    tiledb_ctx_t* ctx,
    const tiledb_dimension_t* dim,
    const char** name) {
  return api_entry_context<tiledb::api::tiledb_dimension_get_name>(
      ctx, dim, name);
}

CAPI_INTERFACE(
    dimension_get_type,
    tiledb_ctx_t* ctx,
    const tiledb_dimension_t* dim,
    tiledb_datatype_t* type) {
  return api_entry_context<tiledb::api::tiledb_dimension_get_type>(
      ctx, dim, type);
}

CAPI_INTERFACE(
    dimension_get_domain,
    tiledb_ctx_t* ctx,
    const tiledb_dimension_t* dim,
    const void** domain) {
  return api_entry_context<tiledb::api::tiledb_dimension_get_domain>(
      ctx, dim, domain);
}

CAPI_INTERFACE(
    dimension_get_tile_extent,
    tiledb_ctx_t* ctx,
    const tiledb_dimension_t* dim,
    const void** tile_extent) {
  return api_entry_context<tiledb::api::tiledb_dimension_get_tile_extent>(
      ctx, dim, tile_extent);
}

CAPI_INTERFACE(
    dimension_dump,
    tiledb_ctx_t* ctx,
    const tiledb_dimension_t* dim,
    FILE* out) {
  return api_entry_context<tiledb::api::tiledb_dimension_dump>(ctx, dim, out);
}

CAPI_INTERFACE(
    dimension_dump_str,
    tiledb_ctx_t* ctx,
    const tiledb_dimension_t* dimension,
    tiledb_string_handle_t** out) {
  return api_entry_context<tiledb::api::tiledb_dimension_dump_str>(
      ctx, dimension, out);
}
