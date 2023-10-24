/**
 * @file tiledb/api/c_api/enumeration/enumeration_api.cc
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
 *
 * @section DESCRIPTION
 *
 * This file defines C API functions for the enumeration section.
 */

#include "../../c_api_support/c_api_support.h"
#include "../string/string_api_internal.h"
#include "enumeration_api_experimental.h"
#include "enumeration_api_internal.h"

namespace tiledb::api {

capi_return_t tiledb_enumeration_alloc(
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
    *enumeration = tiledb_enumeration_handle_t::make_handle(
        std::string(name),
        datatype,
        cell_val_num,
        is_ordered,
        data,
        data_size,
        offsets,
        offsets_size);
  } catch (...) {
    *enumeration = nullptr;
    throw;
  }

  return TILEDB_OK;
}

capi_return_t tiledb_enumeration_extend(
    tiledb_enumeration_t* old_enumeration,
    const void* data,
    uint64_t data_size,
    const void* offsets,
    uint64_t offsets_size,
    tiledb_enumeration_t** new_enumeration) {
  ensure_enumeration_is_valid(old_enumeration);
  ensure_output_pointer_is_valid(new_enumeration);

  auto new_enmr =
      old_enumeration->extend(data, data_size, offsets, offsets_size);

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
    tiledb_enumeration_t* enumeration, tiledb_string_t** name) {
  ensure_enumeration_is_valid(enumeration);
  ensure_output_pointer_is_valid(name);
  *name = tiledb_string_handle_t::make_handle(enumeration->name());
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
  enumeration->dump(out);
  return TILEDB_OK;
}

}  // namespace tiledb::api

using tiledb::api::api_entry_context;
using tiledb::api::api_entry_void;

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
    tiledb_enumeration_t** enumeration) noexcept {
  return api_entry_context<tiledb::api::tiledb_enumeration_alloc>(
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

capi_return_t tiledb_enumeration_extend(
    tiledb_ctx_t* ctx,
    tiledb_enumeration_t* old_enumeration,
    const void* data,
    uint64_t data_size,
    const void* offsets,
    uint64_t offsets_size,
    tiledb_enumeration_t** new_enumeration) noexcept {
  return api_entry_context<tiledb::api::tiledb_enumeration_extend>(
      ctx,
      old_enumeration,
      data,
      data_size,
      offsets,
      offsets_size,
      new_enumeration);
}

void tiledb_enumeration_free(tiledb_enumeration_t** enumeration) noexcept {
  return api_entry_void<tiledb::api::tiledb_enumeration_free>(enumeration);
}

capi_return_t tiledb_enumeration_get_name(
    tiledb_ctx_t* ctx,
    tiledb_enumeration_t* enumeration,
    tiledb_string_t** name) noexcept {
  return api_entry_context<tiledb::api::tiledb_enumeration_get_name>(
      ctx, enumeration, name);
}

capi_return_t tiledb_enumeration_get_type(
    tiledb_ctx_t* ctx,
    tiledb_enumeration_t* enumeration,
    tiledb_datatype_t* type) noexcept {
  return api_entry_context<tiledb::api::tiledb_enumeration_get_type>(
      ctx, enumeration, type);
}

capi_return_t tiledb_enumeration_get_cell_val_num(
    tiledb_ctx_t* ctx,
    tiledb_enumeration_t* enumeration,
    uint32_t* cell_val_num) noexcept {
  return api_entry_context<tiledb::api::tiledb_enumeration_get_cell_val_num>(
      ctx, enumeration, cell_val_num);
}

capi_return_t tiledb_enumeration_get_ordered(
    tiledb_ctx_t* ctx,
    tiledb_enumeration_t* enumeration,
    int* ordered) noexcept {
  return api_entry_context<tiledb::api::tiledb_enumeration_get_ordered>(
      ctx, enumeration, ordered);
}

capi_return_t tiledb_enumeration_get_data(
    tiledb_ctx_t* ctx,
    tiledb_enumeration_t* enumeration,
    const void** data,
    uint64_t* data_size) noexcept {
  return api_entry_context<tiledb::api::tiledb_enumeration_get_data>(
      ctx, enumeration, data, data_size);
}

capi_return_t tiledb_enumeration_get_offsets(
    tiledb_ctx_t* ctx,
    tiledb_enumeration_t* enumeration,
    const void** offsets,
    uint64_t* offsets_size) noexcept {
  return api_entry_context<tiledb::api::tiledb_enumeration_get_offsets>(
      ctx, enumeration, offsets, offsets_size);
}

capi_return_t tiledb_enumeration_dump(
    tiledb_ctx_t* ctx, tiledb_enumeration_t* enumeration, FILE* out) noexcept {
  return api_entry_context<tiledb::api::tiledb_enumeration_dump>(
      ctx, enumeration, out);
}
