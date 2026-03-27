/**
 * @file tiledb/api/c_api/array_schema_evolution/array_schema_evolution_api.cc
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
 * This file defines C API functions for the array schema evolution section.
 */

#include "array_schema_evolution_api_experimental.h"
#include "array_schema_evolution_api_internal.h"

#include "tiledb/api/c_api/attribute/attribute_api_internal.h"
#include "tiledb/api/c_api/buffer/buffer_api_internal.h"
#include "tiledb/api/c_api/current_domain/current_domain_api_internal.h"
#include "tiledb/api/c_api/enumeration/enumeration_api_internal.h"
#include "tiledb/api/c_api_support/c_api_support.h"
#include "tiledb/common/memory_tracker.h"
#include "tiledb/sm/c_api/api_argument_validator.h"
#include "tiledb/sm/serialization/array_schema_evolution.h"

namespace tiledb::api {

/* ********************************* */
/*            SCHEMA EVOLUTION       */
/* ********************************* */

capi_return_t tiledb_array_schema_evolution_alloc(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t** array_schema_evolution) {
  ensure_output_pointer_is_valid(array_schema_evolution);
  auto memory_tracker = ctx->resources().create_memory_tracker();
  memory_tracker->set_type(sm::MemoryTrackerType::SCHEMA_EVOLUTION);
  *array_schema_evolution =
      make_handle<tiledb_array_schema_evolution_t>(memory_tracker);
  return TILEDB_OK;
}

void tiledb_array_schema_evolution_free(
    tiledb_array_schema_evolution_t** array_schema_evolution) {
  ensure_output_pointer_is_valid(array_schema_evolution);
  ensure_handle_is_valid(*array_schema_evolution);
  break_handle(*array_schema_evolution);
}

int32_t tiledb_array_schema_evolution_add_attribute(
    tiledb_array_schema_evolution_t* array_schema_evolution,
    tiledb_attribute_t* attr) {
  ensure_handle_is_valid(array_schema_evolution);
  ensure_attribute_is_valid(attr);
  array_schema_evolution->array_schema_evolution()->add_attribute(
      attr->copy_attribute());

  return TILEDB_OK;
}

int32_t tiledb_array_schema_evolution_drop_attribute(
    tiledb_array_schema_evolution_t* array_schema_evolution,
    const char* attribute_name) {
  ensure_handle_is_valid(array_schema_evolution);

  array_schema_evolution->array_schema_evolution()->drop_attribute(
      attribute_name);
  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_evolution_add_enumeration(
    tiledb_array_schema_evolution_t* array_schema_evolution,
    tiledb_enumeration_t* enumeration) {
  ensure_handle_is_valid(array_schema_evolution);
  ensure_enumeration_is_valid(enumeration);

  auto enmr = enumeration->copy();
  array_schema_evolution->array_schema_evolution()->add_enumeration(enmr);

  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_evolution_extend_enumeration(
    tiledb_array_schema_evolution_t* array_schema_evolution,
    tiledb_enumeration_t* enumeration) {
  ensure_handle_is_valid(array_schema_evolution);
  ensure_enumeration_is_valid(enumeration);

  auto enmr = enumeration->copy();
  array_schema_evolution->array_schema_evolution()->extend_enumeration(enmr);

  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_evolution_drop_enumeration(
    tiledb_array_schema_evolution_t* array_schema_evolution,
    const char* enmr_name) {
  ensure_handle_is_valid(array_schema_evolution);

  if (enmr_name == nullptr) {
    return TILEDB_ERR;
  }

  array_schema_evolution->array_schema_evolution()->drop_enumeration(enmr_name);

  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_evolution_expand_current_domain(
    tiledb_array_schema_evolution_t* array_schema_evolution,
    tiledb_current_domain_t* expanded_domain) {
  ensure_handle_is_valid(array_schema_evolution);
  ensure_handle_is_valid(expanded_domain);

  array_schema_evolution->array_schema_evolution()->expand_current_domain(
      expanded_domain->current_domain());

  return TILEDB_OK;
}

int32_t tiledb_array_schema_evolution_set_timestamp_range(
    tiledb_array_schema_evolution_t* array_schema_evolution,
    uint64_t lo,
    uint64_t hi) {
  ensure_handle_is_valid(array_schema_evolution);

  array_schema_evolution->array_schema_evolution()->set_timestamp_range(
      {lo, hi});
  return TILEDB_OK;
}

}  // namespace tiledb::api

using tiledb::api::api_entry_void;
using tiledb::api::api_entry_with_context;
template <auto f>
constexpr auto api_entry = tiledb::api::api_entry_context<f>;

/* ********************************* */
/*            SCHEMA EVOLUTION       */
/* ********************************* */

CAPI_INTERFACE(
    array_schema_evolution_alloc,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t** array_schema_evolution) {
  return api_entry_with_context<
      tiledb::api::tiledb_array_schema_evolution_alloc>(
      ctx, array_schema_evolution);
}

CAPI_INTERFACE_VOID(
    array_schema_evolution_free,
    tiledb_array_schema_evolution_t** array_schema_evolution) {
  return api_entry_void<tiledb::api::tiledb_array_schema_evolution_free>(
      array_schema_evolution);
}

CAPI_INTERFACE(
    array_schema_evolution_add_attribute,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t* array_schema_evolution,
    tiledb_attribute_t* attr) {
  return api_entry<tiledb::api::tiledb_array_schema_evolution_add_attribute>(
      ctx, array_schema_evolution, attr);
}

CAPI_INTERFACE(
    array_schema_evolution_drop_attribute,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t* array_schema_evolution,
    const char* attribute_name) {
  return api_entry<tiledb::api::tiledb_array_schema_evolution_drop_attribute>(
      ctx, array_schema_evolution, attribute_name);
}

CAPI_INTERFACE(
    array_schema_evolution_add_enumeration,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t* array_schema_evolution,
    tiledb_enumeration_t* enmr) {
  return api_entry<tiledb::api::tiledb_array_schema_evolution_add_enumeration>(
      ctx, array_schema_evolution, enmr);
}

CAPI_INTERFACE(
    array_schema_evolution_extend_enumeration,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t* array_schema_evolution,
    tiledb_enumeration_t* enmr) {
  return api_entry<
      tiledb::api::tiledb_array_schema_evolution_extend_enumeration>(
      ctx, array_schema_evolution, enmr);
}

CAPI_INTERFACE(
    array_schema_evolution_drop_enumeration,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t* array_schema_evolution,
    const char* enumeration_name) {
  return api_entry<tiledb::api::tiledb_array_schema_evolution_drop_enumeration>(
      ctx, array_schema_evolution, enumeration_name);
}

CAPI_INTERFACE(
    array_schema_evolution_expand_current_domain,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t* array_schema_evolution,
    tiledb_current_domain_t* expanded_domain) {
  return api_entry<
      tiledb::api::tiledb_array_schema_evolution_expand_current_domain>(
      ctx, array_schema_evolution, expanded_domain);
}

CAPI_INTERFACE(
    array_schema_evolution_set_timestamp_range,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t* array_schema_evolution,
    uint64_t lo,
    uint64_t hi) {
  return api_entry<
      tiledb::api::tiledb_array_schema_evolution_set_timestamp_range>(
      ctx, array_schema_evolution, lo, hi);
}
