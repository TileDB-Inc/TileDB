/**
 * @file tiledb/api/c_api/ndrectangle/ndrectangle_api.cc
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
 * This file defines the NDRectangle C API of TileDB.
 **/

#include "ndrectangle_api_external_experimental.h"
#include "ndrectangle_api_internal.h"
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/api/c_api/dimension/dimension_api_internal.h"
#include "tiledb/api/c_api/domain/domain_api_internal.h"
#include "tiledb/api/c_api_support/c_api_support.h"
#include "tiledb/common/memory_tracker.h"

namespace tiledb::api {

/**
 * Ensure the argument is a valid range pointer
 *
 * @param range A range struct
 */
inline void ensure_range_ptr_is_valid(tiledb_range_t* range) {
  ensure_output_pointer_is_valid(range);
}

/**
 * Ensure the argument is a valid char pointer
 *
 * @param name A char pointer
 */
inline void ensure_dim_name_is_valid(const char* name) {
  if (!name) {
    throw CAPIStatusException("argument `name` may not be nullptr");
  }
}

/**
 * Lays out an internal Range into a tiledb_range_t
 *
 * @param r The internal sm Range
 * @param range The C API range
 */
inline void internal_range_to_range(const Range& r, tiledb_range_t* range) {
  if (r.var_size()) {
    range->min_size = r.start_size();
    range->max_size = r.end_size();
    range->min = static_cast<const void*>(r.start_str().data());
    range->max = static_cast<const void*>(r.end_str().data());
  } else {
    range->min_size = r.size() / 2;
    range->max_size = r.size() / 2;
    range->min = static_cast<const void*>(r.start_fixed());
    range->max = static_cast<const void*>(r.end_fixed());
  }
}

capi_return_t tiledb_ndrectangle_alloc(
    tiledb_ctx_t* ctx, tiledb_domain_t* domain, tiledb_ndrectangle_t** ndr) {
  ensure_context_is_valid(ctx);
  ensure_domain_is_valid(domain);
  ensure_output_pointer_is_valid(ndr);

  auto memory_tracker = ctx->resources().create_memory_tracker();
  memory_tracker->set_type(tiledb::sm::MemoryTrackerType::ARRAY_CREATE);
  *ndr = tiledb_ndrectangle_handle_t::make_handle(
      memory_tracker, domain->copy_domain());
  return TILEDB_OK;
}

capi_return_t tiledb_ndrectangle_free(tiledb_ndrectangle_t** ndr) {
  ensure_output_pointer_is_valid(ndr);
  ensure_handle_is_valid(*ndr);
  tiledb_ndrectangle_handle_t::break_handle(*ndr);

  return TILEDB_OK;
}

capi_return_t tiledb_ndrectangle_get_range_from_name(
    tiledb_ndrectangle_t* ndr, const char* name, tiledb_range_t* range) {
  ensure_handle_is_valid(ndr);
  ensure_dim_name_is_valid(name);
  ensure_range_ptr_is_valid(range);

  auto& r = ndr->ndrectangle()->get_range_for_name(name);

  internal_range_to_range(r, range);

  return TILEDB_OK;
}

capi_return_t tiledb_ndrectangle_get_range(
    tiledb_ndrectangle_t* ndr, uint32_t idx, tiledb_range_t* range) {
  ensure_handle_is_valid(ndr);
  ensure_range_ptr_is_valid(range);

  auto& r = ndr->ndrectangle()->get_range(idx);
  internal_range_to_range(r, range);

  return TILEDB_OK;
}

capi_return_t tiledb_ndrectangle_set_range_for_name(
    tiledb_ndrectangle_t* ndr, const char* name, tiledb_range_t* range) {
  ensure_handle_is_valid(ndr);
  ensure_dim_name_is_valid(name);
  ensure_range_ptr_is_valid(range);

  Range r;
  auto idx = ndr->ndrectangle()->domain()->get_dimension_index(name);
  if (ndr->ndrectangle()->domain()->dimension_ptr(idx)->var_size()) {
    r.set_range_var(range->min, range->min_size, range->max, range->max_size);
  } else {
    r.set_range_fixed(range->min, range->max, range->min_size);
  }

  ndr->ndrectangle()->set_range_for_name(r, name);

  return TILEDB_OK;
}

capi_return_t tiledb_ndrectangle_set_range(
    tiledb_ndrectangle_t* ndr, uint32_t idx, tiledb_range_t* range) {
  ensure_handle_is_valid(ndr);
  ensure_range_ptr_is_valid(range);

  Range r;
  if (ndr->ndrectangle()->domain()->dimension_ptr(idx)->var_size()) {
    r.set_range_var(range->min, range->min_size, range->max, range->max_size);
  } else {
    r.set_range_fixed(range->min, range->max, range->min_size);
  }

  ndr->ndrectangle()->set_range(r, idx);

  return TILEDB_OK;
}

capi_return_t tiledb_ndrectangle_get_dtype(
    tiledb_ctx_t* ctx,
    tiledb_ndrectangle_t* ndr,
    uint32_t idx,
    tiledb_datatype_t* type) {
  ensure_context_is_valid(ctx);
  ensure_handle_is_valid(ndr);
  ensure_output_pointer_is_valid(type);

  *type = static_cast<tiledb_datatype_t>(ndr->ndrectangle()->range_dtype(idx));

  return TILEDB_OK;
}

capi_return_t tiledb_ndrectangle_get_dtype_from_name(
    tiledb_ctx_t* ctx,
    tiledb_ndrectangle_t* ndr,
    const char* name,
    tiledb_datatype_t* type) {
  ensure_context_is_valid(ctx);
  ensure_handle_is_valid(ndr);
  ensure_dim_name_is_valid(name);
  ensure_output_pointer_is_valid(type);

  *type = static_cast<tiledb_datatype_t>(
      ndr->ndrectangle()->range_dtype_for_name(name));

  return TILEDB_OK;
}

capi_return_t tiledb_ndrectangle_get_dim_num(
    tiledb_ctx_t* ctx, tiledb_ndrectangle_t* ndr, uint32_t* ndim) {
  ensure_context_is_valid(ctx);
  ensure_handle_is_valid(ndr);
  ensure_output_pointer_is_valid(ndim);

  *ndim = ndr->ndrectangle()->domain()->dim_num();

  return TILEDB_OK;
}

}  // namespace tiledb::api

using tiledb::api::api_entry_context;
using tiledb::api::api_entry_plain;
using tiledb::api::api_entry_with_context;

CAPI_INTERFACE(
    ndrectangle_alloc,
    tiledb_ctx_t* ctx,
    tiledb_domain_t* domain,
    tiledb_ndrectangle_t** ndr) {
  return api_entry_with_context<tiledb::api::tiledb_ndrectangle_alloc>(
      ctx, domain, ndr);
}

CAPI_INTERFACE(ndrectangle_free, tiledb_ndrectangle_t** ndr) {
  return api_entry_plain<tiledb::api::tiledb_ndrectangle_free>(ndr);
}

CAPI_INTERFACE(
    ndrectangle_get_range_from_name,
    tiledb_ctx_t* ctx,
    tiledb_ndrectangle_t* ndr,
    const char* name,
    tiledb_range_t* range) {
  return api_entry_context<tiledb::api::tiledb_ndrectangle_get_range_from_name>(
      ctx, ndr, name, range);
}

CAPI_INTERFACE(
    ndrectangle_get_range,
    tiledb_ctx_t* ctx,
    tiledb_ndrectangle_t* ndr,
    uint32_t idx,
    tiledb_range_t* range) {
  return api_entry_context<tiledb::api::tiledb_ndrectangle_get_range>(
      ctx, ndr, idx, range);
}

CAPI_INTERFACE(
    ndrectangle_set_range_for_name,
    tiledb_ctx_t* ctx,
    tiledb_ndrectangle_t* ndr,
    const char* name,
    tiledb_range_t* range) {
  return api_entry_context<tiledb::api::tiledb_ndrectangle_set_range_for_name>(
      ctx, ndr, name, range);
}

CAPI_INTERFACE(
    ndrectangle_set_range,
    tiledb_ctx_t* ctx,
    tiledb_ndrectangle_t* ndr,
    uint32_t idx,
    tiledb_range_t* range) {
  return api_entry_context<tiledb::api::tiledb_ndrectangle_set_range>(
      ctx, ndr, idx, range);
}

CAPI_INTERFACE(
    ndrectangle_get_dtype,
    tiledb_ctx_t* ctx,
    tiledb_ndrectangle_t* ndr,
    uint32_t idx,
    tiledb_datatype_t* type) {
  return api_entry_with_context<tiledb::api::tiledb_ndrectangle_get_dtype>(
      ctx, ndr, idx, type);
}

CAPI_INTERFACE(
    ndrectangle_get_dtype_from_name,
    tiledb_ctx_t* ctx,
    tiledb_ndrectangle_t* ndr,
    const char* name,
    tiledb_datatype_t* type) {
  return api_entry_with_context<
      tiledb::api::tiledb_ndrectangle_get_dtype_from_name>(
      ctx, ndr, name, type);
}

CAPI_INTERFACE(
    ndrectangle_get_dim_num,
    tiledb_ctx_t* ctx,
    tiledb_ndrectangle_t* ndr,
    uint32_t* ndim) {
  return api_entry_with_context<tiledb::api::tiledb_ndrectangle_get_dim_num>(
      ctx, ndr, ndim);
}
