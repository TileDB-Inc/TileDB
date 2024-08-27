/**
 * @file tiledb/api/c_api/current_domain/current_domain_api.cc
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
 * This file defines the current domain C API of TileDB.
 **/

#include "current_domain_api_external_experimental.h"
#include "current_domain_api_internal.h"
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/api/c_api/domain/domain_api_internal.h"
#include "tiledb/api/c_api/ndrectangle/ndrectangle_api_internal.h"
#include "tiledb/api/c_api_support/c_api_support.h"
#include "tiledb/common/memory_tracker.h"

namespace tiledb::api {

capi_return_t tiledb_current_domain_create(
    tiledb_ctx_t* ctx, tiledb_current_domain_t** current_domain) {
  ensure_context_is_valid(ctx);
  ensure_output_pointer_is_valid(current_domain);

  auto memory_tracker = ctx->resources().create_memory_tracker();
  memory_tracker->set_type(tiledb::sm::MemoryTrackerType::ARRAY_CREATE);
  *current_domain = tiledb_current_domain_handle_t::make_handle(
      memory_tracker, sm::constants::current_domain_version);
  return TILEDB_OK;
}

capi_return_t tiledb_current_domain_free(
    tiledb_current_domain_t** current_domain) {
  ensure_output_pointer_is_valid(current_domain);
  ensure_handle_is_valid(*current_domain);
  tiledb_current_domain_handle_t::break_handle(*current_domain);

  return TILEDB_OK;
}

capi_return_t tiledb_current_domain_set_ndrectangle(
    tiledb_current_domain_t* current_domain, tiledb_ndrectangle_t* ndr) {
  ensure_handle_is_valid(current_domain);
  ensure_handle_is_valid(ndr);

  current_domain->current_domain()->set_ndrectangle(ndr->ndrectangle());

  return TILEDB_OK;
}

capi_return_t tiledb_current_domain_get_ndrectangle(
    tiledb_current_domain_t* current_domain, tiledb_ndrectangle_t** ndr) {
  ensure_handle_is_valid(current_domain);
  ensure_output_pointer_is_valid(ndr);

  *ndr = tiledb_ndrectangle_handle_t::make_handle(
      current_domain->current_domain()->ndrectangle());

  return TILEDB_OK;
}

capi_return_t tiledb_current_domain_get_is_empty(
    tiledb_current_domain_t* current_domain, uint32_t* is_empty) {
  ensure_handle_is_valid(current_domain);
  ensure_output_pointer_is_valid(is_empty);

  *is_empty = current_domain->current_domain()->empty();

  return TILEDB_OK;
}

capi_return_t tiledb_current_domain_get_type(
    tiledb_current_domain_t* current_domain,
    tiledb_current_domain_type_t* type) {
  ensure_handle_is_valid(current_domain);
  ensure_output_pointer_is_valid(type);

  *type = static_cast<tiledb_current_domain_type_t>(
      current_domain->current_domain()->type());

  return TILEDB_OK;
}

}  // namespace tiledb::api

using tiledb::api::api_entry_context;
using tiledb::api::api_entry_plain;
using tiledb::api::api_entry_with_context;

CAPI_INTERFACE(
    current_domain_create,
    tiledb_ctx_t* ctx,
    tiledb_current_domain_t** current_domain) {
  return api_entry_with_context<tiledb::api::tiledb_current_domain_create>(
      ctx, current_domain);
}

CAPI_INTERFACE(current_domain_free, tiledb_current_domain_t** current_domain) {
  return api_entry_plain<tiledb::api::tiledb_current_domain_free>(
      current_domain);
}

CAPI_INTERFACE(
    current_domain_set_ndrectangle,
    tiledb_ctx_t* ctx,
    tiledb_current_domain_t* current_domain,
    tiledb_ndrectangle_t* ndr) {
  return api_entry_context<tiledb::api::tiledb_current_domain_set_ndrectangle>(
      ctx, current_domain, ndr);
}

CAPI_INTERFACE(
    current_domain_get_ndrectangle,
    tiledb_ctx_t* ctx,
    tiledb_current_domain_t* current_domain,
    tiledb_ndrectangle_t** ndr) {
  return api_entry_context<tiledb::api::tiledb_current_domain_get_ndrectangle>(
      ctx, current_domain, ndr);
}

CAPI_INTERFACE(
    current_domain_get_is_empty,
    tiledb_ctx_t* ctx,
    tiledb_current_domain_t* current_domain,
    uint32_t* is_empty) {
  return api_entry_context<tiledb::api::tiledb_current_domain_get_is_empty>(
      ctx, current_domain, is_empty);
}

CAPI_INTERFACE(
    current_domain_get_type,
    tiledb_ctx_t* ctx,
    tiledb_current_domain_t* current_domain,
    tiledb_current_domain_type_t* type) {
  return api_entry_context<tiledb::api::tiledb_current_domain_get_type>(
      ctx, current_domain, type);
}
