/**
 * @file tiledb/api/c_api/domain/domain_api.cc
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
 */

#include "../../c_api_support/c_api_support.h"
#include "../dimension/dimension_api_internal.h"
#include "../string/string_api_internal.h"
#include "domain_api_external.h"
#include "domain_api_internal.h"
#include "tiledb/common/memory_tracker.h"

namespace tiledb::api {

int32_t tiledb_domain_alloc(
    tiledb_ctx_t* ctx, tiledb_domain_handle_t** domain) {
  ensure_output_pointer_is_valid(domain);
  auto memory_tracker = ctx->resources().create_memory_tracker();
  memory_tracker->set_type(tiledb::sm::MemoryTrackerType::ARRAY_CREATE);
  *domain = tiledb_domain_handle_t::make_handle(memory_tracker);
  return TILEDB_OK;
}

void tiledb_domain_free(tiledb_domain_handle_t** domain) {
  ensure_output_pointer_is_valid(domain);
  ensure_domain_is_valid(*domain);
  tiledb_domain_handle_t::break_handle(*domain);
}

int32_t tiledb_domain_get_type(
    const tiledb_domain_t* domain, tiledb_datatype_t* type) {
  ensure_domain_is_valid(domain);
  ensure_output_pointer_is_valid(type);

  if (domain->dim_num() == 0) {
    throw CAPIStatusException(
        "Cannot get domain type; Domain has no dimensions");
  }

  if (!domain->all_dims_same_type()) {
    throw CAPIStatusException(
        "Cannot get domain type; Not applicable to heterogeneous dimensions");
  }

  *type = static_cast<tiledb_datatype_t>(domain->dimension_ptr(0)->type());
  return TILEDB_OK;
}

int32_t tiledb_domain_get_ndim(const tiledb_domain_t* domain, uint32_t* ndim) {
  ensure_domain_is_valid(domain);
  ensure_output_pointer_is_valid(ndim);
  *ndim = domain->dim_num();
  return TILEDB_OK;
}

int32_t tiledb_domain_add_dimension(
    tiledb_domain_t* domain, tiledb_dimension_t* dim) {
  ensure_domain_is_valid(domain);
  if (dim == nullptr) {
    throw CAPIStatusException(
        "May not add a missing dimension; argument is NULL");
  }
  throw_if_not_ok(domain->add_dimension(dim->copy_dimension()));
  return TILEDB_OK;
}

int32_t tiledb_domain_get_dimension_from_index(
    const tiledb_domain_t* domain, uint32_t index, tiledb_dimension_t** dim) {
  ensure_domain_is_valid(domain);
  ensure_output_pointer_is_valid(dim);
  auto ndim = domain->dim_num();
  if (ndim == 0 && index == 0) {
    *dim = nullptr;
    return TILEDB_OK;
  }
  // The index must be in the interval [0,ndim)
  if (ndim <= index) {
    throw CAPIStatusException(
        "Dimension index " + std::to_string(index) +
        " is out of bounds; valid indices are 0 to " +
        std::to_string(ndim - 1));
  }
  auto dimension{domain->shared_dimension(index)};
  // `shared_dimension` never returns `nullptr`
  *dim = tiledb_dimension_handle_t::make_handle(dimension);
  return TILEDB_OK;
}

int32_t tiledb_domain_get_dimension_from_name(
    const tiledb_domain_t* domain,
    const char* name_arg,
    tiledb_dimension_t** dim) {
  ensure_domain_is_valid(domain);
  ensure_output_pointer_is_valid(dim);
  auto ndim = domain->dim_num();
  if (ndim == 0) {
    *dim = nullptr;
    return TILEDB_OK;
  }
  std::string name{name_arg};
  auto dimension = domain->shared_dimension(name);
  if (!dimension) {
    throw CAPIStatusException("Dimension '" + name + "' does not exist");
  }
  *dim = tiledb_dimension_handle_t::make_handle(dimension);
  return TILEDB_OK;
}

int32_t tiledb_domain_has_dimension(
    const tiledb_domain_t* domain, const char* name, int32_t* has_dim) {
  ensure_domain_is_valid(domain);
  /*
   * We are _not_ checking that `name` not be NULL because we allow dimension
   * names to be empty strings.
   */
  ensure_output_pointer_is_valid(has_dim);
  *has_dim = domain->has_dimension(name) ? 1 : 0;
  return TILEDB_OK;
}

int32_t tiledb_domain_dump_str(
    const tiledb_domain_t* domain, tiledb_string_handle_t** out) {
  ensure_domain_is_valid(domain);
  ensure_output_pointer_is_valid(out);

  std::stringstream ss;
  ss << *domain;
  *out = tiledb_string_handle_t::make_handle(ss.str());
  return TILEDB_OK;
}

}  // namespace tiledb::api

std::ostream& operator<<(
    std::ostream& os, const tiledb_domain_handle_t& domain) {
  os << *domain.domain_;
  return os;
}

using tiledb::api::api_entry_context;

CAPI_INTERFACE(domain_alloc, tiledb_ctx_t* ctx, tiledb_domain_t** domain) {
  return tiledb::api::api_entry_with_context<tiledb::api::tiledb_domain_alloc>(
      ctx, domain);
}

CAPI_INTERFACE_VOID(domain_free, tiledb_domain_t** domain) {
  return tiledb::api::api_entry_void<tiledb::api::tiledb_domain_free>(domain);
}

CAPI_INTERFACE(
    domain_get_type,
    tiledb_ctx_t* ctx,
    const tiledb_domain_t* domain,
    tiledb_datatype_t* type) {
  return api_entry_context<tiledb::api::tiledb_domain_get_type>(
      ctx, domain, type);
}

CAPI_INTERFACE(
    domain_get_ndim,
    tiledb_ctx_t* ctx,
    const tiledb_domain_t* domain,
    uint32_t* ndim) {
  return api_entry_context<tiledb::api::tiledb_domain_get_ndim>(
      ctx, domain, ndim);
}

CAPI_INTERFACE(
    domain_add_dimension,
    tiledb_ctx_t* ctx,
    tiledb_domain_t* domain,
    tiledb_dimension_t* dim) {
  return api_entry_context<tiledb::api::tiledb_domain_add_dimension>(
      ctx, domain, dim);
}

CAPI_INTERFACE(
    domain_get_dimension_from_index,
    tiledb_ctx_t* ctx,
    const tiledb_domain_t* domain,
    uint32_t index,
    tiledb_dimension_t** dim) {
  return api_entry_context<tiledb::api::tiledb_domain_get_dimension_from_index>(
      ctx, domain, index, dim);
}

CAPI_INTERFACE(
    domain_get_dimension_from_name,
    tiledb_ctx_t* ctx,
    const tiledb_domain_t* domain,
    const char* name,
    tiledb_dimension_t** dim) {
  return api_entry_context<tiledb::api::tiledb_domain_get_dimension_from_name>(
      ctx, domain, name, dim);
}

CAPI_INTERFACE(
    domain_has_dimension,
    tiledb_ctx_t* ctx,
    const tiledb_domain_t* domain,
    const char* name,
    int32_t* has_dim) {
  return api_entry_context<tiledb::api::tiledb_domain_has_dimension>(
      ctx, domain, name, has_dim);
}

CAPI_INTERFACE(
    domain_dump_str,
    tiledb_ctx_t* ctx,
    const tiledb_domain_t* domain,
    tiledb_string_handle_t** out) {
  return api_entry_context<tiledb::api::tiledb_domain_dump_str>(
      ctx, domain, out);
}
