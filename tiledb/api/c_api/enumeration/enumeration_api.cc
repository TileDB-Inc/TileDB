/**
 * @file tiledb/api/c_api/enumeration/enumeration_api.cc
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
 * This file defines C API functions for the enumeration section.
 */

#include "../../c_api_support/c_api_support.h"
#include "../string/string_api_internal.h"
#include "enumeration_api_experimental.h"
#include "enumeration_api_internal.h"
#include "tiledb/common/memory_tracker.h"

namespace tiledb::api {

capi_return_t tiledb_enumeration_alloc(
    tiledb_ctx_t* ctx,
    const char* name,
    tiledb_datatype_t type,
    uint32_t cell_val_num,
    int ordered,
    const void* data,
    uint64_t data_size,
    const void* offsets,
    uint64_t offsets_size,
    tiledb_enumeration_t** enumeration) {
  ensure_output_pointer_is_valid(enumeration);

  // The nullptr-ness of data and offsets are handled by the underlying
  // Enumeration constructor. As this is non-trivial (though not very
  // complicated) logic, it's not replicated here.
  if (name == nullptr) {
    throw CAPIStatusException(
        "tiledb_enumeration_alloc: name must not be null");
  }

  auto datatype = static_cast<sm::Datatype>(type);

  bool is_ordered = false;
  if (ordered != 0) {
    is_ordered = true;
  }

  try {
    auto memory_tracker = ctx->context().resources().create_memory_tracker();
    memory_tracker->set_type(tiledb::sm::MemoryTrackerType::ENUMERATION_CREATE);

    *enumeration = tiledb_enumeration_handle_t::make_handle(
        ctx->context().resources(),
        std::string(name),
        datatype,
        cell_val_num,
        is_ordered,
        data,
        data_size,
        offsets,
        offsets_size,
        memory_tracker);
  } catch (...) {
    *enumeration = nullptr;
    throw;
  }

  return TILEDB_OK;
}

capi_return_t tiledb_enumeration_extend(
    tiledb_ctx_t* ctx,
    tiledb_enumeration_t* old_enumeration,
    const void* data,
    uint64_t data_size,
    const void* offsets,
    uint64_t offsets_size,
    tiledb_enumeration_t** new_enumeration) {
  ensure_enumeration_is_valid(old_enumeration);
  ensure_output_pointer_is_valid(new_enumeration);

  auto new_enmr =
      old_enumeration->extend(ctx, data, data_size, offsets, offsets_size);

  try {
    *new_enumeration = tiledb_enumeration_handle_t::make_handle(new_enmr);
  } catch (...) {
    *new_enumeration = nullptr;
    throw;
  }

  return TILEDB_OK;
}

void tiledb_enumeration_free(tiledb_enumeration_t** enumeration) {
  ensure_output_pointer_is_valid(enumeration);
  ensure_enumeration_is_valid(*enumeration);
  tiledb_enumeration_handle_t::break_handle(*enumeration);
}

capi_return_t tiledb_enumeration_get_name(
    tiledb_enumeration_t* enumeration, tiledb_string_handle_t** name) {
  ensure_enumeration_is_valid(enumeration);
  ensure_output_pointer_is_valid(name);
  *name = tiledb_string_handle_t::make_handle(enumeration->name());
  return TILEDB_OK;
}

capi_return_t tiledb_enumeration_get_value_index(
    tiledb_enumeration_t* enumeration,
    const void* value,
    uint64_t value_size,
    int* exist,
    uint64_t* index) {
  ensure_enumeration_is_valid(enumeration);
  ensure_output_pointer_is_valid(exist);
  ensure_output_pointer_is_valid(index);

  *index = enumeration->enumeration()->index_of(value, value_size);
  *exist = *index != sm::constants::enumeration_missing_value ? 1 : 0;

  return TILEDB_OK;
}

capi_return_t tiledb_enumeration_get_type(
    tiledb_enumeration_t* enumeration, tiledb_datatype_t* type) {
  ensure_enumeration_is_valid(enumeration);
  ensure_output_pointer_is_valid(type);
  *type = static_cast<tiledb_datatype_t>(enumeration->type());
  return TILEDB_OK;
}

capi_return_t tiledb_enumeration_get_cell_val_num(
    tiledb_enumeration_t* enumeration, uint32_t* cell_val_num) {
  ensure_enumeration_is_valid(enumeration);
  ensure_output_pointer_is_valid(cell_val_num);
  *cell_val_num = enumeration->cell_val_num();
  return TILEDB_OK;
}

capi_return_t tiledb_enumeration_get_ordered(
    tiledb_enumeration_t* enumeration, int* ordered) {
  ensure_enumeration_is_valid(enumeration);
  ensure_output_pointer_is_valid(ordered);
  *ordered = static_cast<int>(enumeration->ordered());
  return TILEDB_OK;
}

capi_return_t tiledb_enumeration_get_data(
    tiledb_enumeration_t* enumeration, const void** data, uint64_t* data_size) {
  ensure_enumeration_is_valid(enumeration);
  ensure_output_pointer_is_valid(data);
  ensure_output_pointer_is_valid(data_size);
  auto dspan = enumeration->data();
  *data = dspan.data();
  *data_size = dspan.size();
  return TILEDB_OK;
}

capi_return_t tiledb_enumeration_get_offsets(
    tiledb_enumeration_t* enumeration,
    const void** offsets,
    uint64_t* offsets_size) {
  ensure_enumeration_is_valid(enumeration);
  ensure_output_pointer_is_valid(offsets);
  ensure_output_pointer_is_valid(offsets_size);
  auto ospan = enumeration->offsets();
  *offsets = ospan.data();
  *offsets_size = ospan.size();
  return TILEDB_OK;
}

capi_return_t tiledb_enumeration_dump(
    tiledb_enumeration_t* enumeration, FILE* out) {
  ensure_enumeration_is_valid(enumeration);
  ensure_cstream_handle_is_valid(out);

  std::stringstream ss;
  ss << *enumeration;
  size_t r = fwrite(ss.str().c_str(), sizeof(char), ss.str().size(), out);
  if (r != ss.str().size()) {
    throw CAPIException(
        "Error writing enumeration " + enumeration->name() + " to file");
  }
  return TILEDB_OK;
}

capi_return_t tiledb_enumeration_dump_str(
    tiledb_enumeration_t* enumeration, tiledb_string_handle_t** out) {
  ensure_enumeration_is_valid(enumeration);
  ensure_output_pointer_is_valid(out);
  std::stringstream ss;
  ss << *enumeration;
  *out = tiledb_string_handle_t::make_handle(ss.str());
  return TILEDB_OK;
}

}  // namespace tiledb::api

std::ostream& operator<<(
    std::ostream& os, const tiledb_enumeration_handle_t& enumeration) {
  os << *enumeration.enumeration_;
  return os;
}

using tiledb::api::api_entry_context;
using tiledb::api::api_entry_void;

template <auto f>
constexpr auto api_entry = tiledb::api::api_entry_with_context<f>;

CAPI_INTERFACE(
    enumeration_alloc,
    tiledb_ctx_t* ctx,
    const char* name,
    tiledb_datatype_t type,
    uint32_t cell_val_num,
    int ordered,
    const void* data,
    uint64_t data_size,
    const void* offsets,
    uint64_t offsets_size,
    tiledb_enumeration_t** enumeration) {
  return api_entry<tiledb::api::tiledb_enumeration_alloc>(
      ctx,
      name,
      type,
      cell_val_num,
      ordered,
      data,
      data_size,
      offsets,
      offsets_size,
      enumeration);
}

CAPI_INTERFACE(
    enumeration_extend,
    tiledb_ctx_t* ctx,
    tiledb_enumeration_t* old_enumeration,
    const void* data,
    uint64_t data_size,
    const void* offsets,
    uint64_t offsets_size,
    tiledb_enumeration_t** new_enumeration) {
  return api_entry<tiledb::api::tiledb_enumeration_extend>(
      ctx,
      old_enumeration,
      data,
      data_size,
      offsets,
      offsets_size,
      new_enumeration);
}

CAPI_INTERFACE_VOID(enumeration_free, tiledb_enumeration_t** enumeration) {
  return api_entry_void<tiledb::api::tiledb_enumeration_free>(enumeration);
}

CAPI_INTERFACE(
    enumeration_get_name,
    tiledb_ctx_t* ctx,
    tiledb_enumeration_t* enumeration,
    tiledb_string_handle_t** name) {
  return api_entry_context<tiledb::api::tiledb_enumeration_get_name>(
      ctx, enumeration, name);
}

CAPI_INTERFACE(
    enumeration_get_value_index,
    tiledb_ctx_t* ctx,
    tiledb_enumeration_t* enumeration,
    const void* value,
    uint64_t value_size,
    int* exist,
    uint64_t* index) {
  return api_entry_context<tiledb::api::tiledb_enumeration_get_value_index>(
      ctx, enumeration, value, value_size, exist, index);
}

CAPI_INTERFACE(
    enumeration_get_type,
    tiledb_ctx_t* ctx,
    tiledb_enumeration_t* enumeration,
    tiledb_datatype_t* type) {
  return api_entry_context<tiledb::api::tiledb_enumeration_get_type>(
      ctx, enumeration, type);
}

CAPI_INTERFACE(
    enumeration_get_cell_val_num,
    tiledb_ctx_t* ctx,
    tiledb_enumeration_t* enumeration,
    uint32_t* cell_val_num) {
  return api_entry_context<tiledb::api::tiledb_enumeration_get_cell_val_num>(
      ctx, enumeration, cell_val_num);
}

CAPI_INTERFACE(
    enumeration_get_ordered,
    tiledb_ctx_t* ctx,
    tiledb_enumeration_t* enumeration,
    int* ordered) {
  return api_entry_context<tiledb::api::tiledb_enumeration_get_ordered>(
      ctx, enumeration, ordered);
}

CAPI_INTERFACE(
    enumeration_get_data,
    tiledb_ctx_t* ctx,
    tiledb_enumeration_t* enumeration,
    const void** data,
    uint64_t* data_size) {
  return api_entry_context<tiledb::api::tiledb_enumeration_get_data>(
      ctx, enumeration, data, data_size);
}

CAPI_INTERFACE(
    enumeration_get_offsets,
    tiledb_ctx_t* ctx,
    tiledb_enumeration_t* enumeration,
    const void** offsets,
    uint64_t* offsets_size) {
  return api_entry_context<tiledb::api::tiledb_enumeration_get_offsets>(
      ctx, enumeration, offsets, offsets_size);
}

CAPI_INTERFACE(
    enumeration_dump,
    tiledb_ctx_t* ctx,
    tiledb_enumeration_t* enumeration,
    FILE* out) {
  return api_entry_context<tiledb::api::tiledb_enumeration_dump>(
      ctx, enumeration, out);
}

CAPI_INTERFACE(
    enumeration_dump_str,
    tiledb_ctx_t* ctx,
    tiledb_enumeration_handle_t* enumeration,
    tiledb_string_handle_t** out) {
  return api_entry_context<tiledb::api::tiledb_enumeration_dump_str>(
      ctx, enumeration, out);
}
