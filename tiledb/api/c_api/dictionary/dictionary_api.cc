/**
 * @file tiledb/api/c_api/dictionary/dictionary_api.cc
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
 * This file defines C API functions for the config section.
 */

#include "../../c_api_support/c_api_support.h"

#include "dictionary_api_external.h"
#include "dictionary_api_internal.h"

namespace tiledb::api {

capi_return_t tiledb_dictionary_alloc(
    tiledb_datatype_t type,
    tiledb_dictionary_t** dict) {
  ensure_output_pointer_is_valid(dict);
  *dict = tiledb_dictionary_handle_t::make_handle(
      static_cast<tiledb::sm::Datatype>(type));

  // Success
  return TILEDB_OK;
}

void tiledb_dictionary_free(tiledb_dictionary_t** dict) {
  ensure_output_pointer_is_valid(dict);
  ensure_dictionary_is_valid(*dict);
  tiledb_dictionary_handle_t::break_handle(*dict);
}

capi_return_t tiledb_dictionary_get_type(
    const tiledb_dictionary_t* dict, tiledb_datatype_t* type) {
  ensure_dictionary_is_valid(dict);
  ensure_output_pointer_is_valid(type);
  *type = static_cast<tiledb_datatype_t>(dict->type());
  return TILEDB_OK;
}

capi_return_t tiledb_dictionary_set_cell_val_num(
    tiledb_dictionary_t* dict, uint32_t cell_val_num) {
  ensure_dictionary_is_valid(dict);
  dict->set_cell_val_num(cell_val_num);
  return TILEDB_OK;
}

capi_return_t tiledb_dictionary_get_cell_val_num(
    const tiledb_dictionary_t* dict, uint32_t* cell_val_num) {
  ensure_dictionary_is_valid(dict);
  ensure_output_pointer_is_valid(cell_val_num);
  *cell_val_num = dict->cell_val_num();
  return TILEDB_OK;
}

capi_return_t tiledb_dictionary_set_nullable(
    tiledb_dictionary_t* dict, uint8_t nullable) {
  ensure_dictionary_is_valid(dict);
  dict->set_nullable(nullable);
  return TILEDB_OK;
}

capi_return_t tiledb_dictionary_get_nullable(
    tiledb_dictionary_t* dict, uint8_t* nullable) {
  ensure_dictionary_is_valid(dict);
  ensure_output_pointer_is_valid(nullable);
  *nullable = dict->nullable();
  return TILEDB_OK;
}

capi_return_t tiledb_dictionary_set_ordered(
    tiledb_dictionary_t* dict, uint8_t ordered) {
  ensure_dictionary_is_valid(dict);
  dict->set_ordered(ordered);
  return TILEDB_OK;
}

capi_return_t tiledb_dictionary_get_ordered(
    tiledb_dictionary_t* dict, uint8_t* ordered) {
  ensure_dictionary_is_valid(dict);
  ensure_output_pointer_is_valid(ordered);
  *ordered = dict->ordered();
  return TILEDB_OK;
}

capi_return_t tiledb_dictionary_set_data_buffer(
    tiledb_dictionary_t* dict, void* buffer, uint64_t buffer_size) {
  ensure_dictionary_is_valid(dict);
  if (buffer == nullptr) {
    throw CAPIStatusError("Dictionary data buffer must not be NULL");
  }
  dict->set_data_buffer(buffer, buffer_size);
  return TILEDB_OK;
}

capi_return_t tiledb_dictionary_get_data_buffer(
    tiledb_dictionary_t* dict, void** buffer, uint64_t* buffer_size) {
  ensure_dictionary_is_valid(dict);
  ensure_output_pointer_is_valid(buffer);
  ensure_output_pointer_is_valid(buffer_size);
  dict->get_data_buffer(buffer, buffer_size);
  return TILEDB_OK;
}

capi_return_t tiledb_dictionary_set_offsets_buffer(
    tiledb_dictionary_t* dict, void* buffer, uint64_t buffer_size) {
  ensure_dictionary_is_valid(dict);
  if (buffer == nullptr) {
    throw CAPIStatusError("Dictionary offsets buffer must not be NULL");
  }
  dict->set_offsets_buffer(buffer, buffer_size);
  return TILEDB_OK;
}

capi_return_t tiledb_dictionary_get_offsets_buffer(
    tiledb_dictionary_t* dict, void** buffer, uint64_t* buffer_size) {
  ensure_dictionary_is_valid(dict);
  ensure_output_pointer_is_valid(buffer);
  ensure_output_pointer_is_valid(buffer_size);
  dict->get_offsets_buffer(buffer, buffer_size);
  return TILEDB_OK;
}

capi_return_t tiledb_dictionary_set_validity_buffer(
    tiledb_dictionary_t* dict, void* buffer, uint64_t buffer_size) {
  ensure_dictionary_is_valid(dict);
  if (buffer == nullptr) {
    throw CAPIStatusError("Dictionary validity buffer must not be NULL");
  }
  dict->set_validity_buffer(buffer, buffer_size);
  return TILEDB_OK;
}

capi_return_t tiledb_dictionary_get_validity_buffer(
    tiledb_dictionary_t* dict, void** buffer, uint64_t* buffer_size) {
  ensure_dictionary_is_valid(dict);
  ensure_output_pointer_is_valid(buffer);
  ensure_output_pointer_is_valid(buffer_size);
  dict->get_validity_buffer(buffer, buffer_size);
  return TILEDB_OK;
}

capi_return_t tiledb_dictionary_dump(const tiledb_dictionary_t* dict, FILE* out) {
  ensure_dictionary_is_valid(dict);
  dict->dump(out);
  return TILEDB_OK;
}

}  // namespace tiledb::api

using tiledb::api::api_entry_context;

capi_return_t tiledb_dictionary_alloc(
    tiledb_ctx_t* ctx,
    tiledb_datatype_t type,
    tiledb_dictionary_t** dict) noexcept {
  return api_entry_context<tiledb::api::tiledb_dictionary_alloc>(
      ctx, type, dict);
}

void tiledb_dictionary_free(tiledb_dictionary_t** dict) noexcept {
  return tiledb::api::api_entry_void<tiledb::api::tiledb_dictionary_free>(dict);
}

capi_return_t tiledb_dictionary_get_type(
    tiledb_ctx_t* ctx,
    tiledb_dictionary_t* dict,
    tiledb_datatype_t* type) noexcept {
  return api_entry_context<tiledb::api::tiledb_dictionary_get_type>(
    ctx, dict, type);
}

capi_return_t tiledb_dictionary_set_cell_val_num(
    tiledb_ctx_t* ctx,
    tiledb_dictionary_t* dict,
    uint32_t cell_val_num) noexcept {
  return api_entry_context<tiledb::api::tiledb_dictionary_set_cell_val_num>(
    ctx, dict, cell_val_num);
}

capi_return_t tiledb_dictionary_get_cell_val_num(
  tiledb_ctx_t* ctx,
  tiledb_dictionary_t* dict,
  uint32_t* cell_val_num) noexcept {
  return api_entry_context<tiledb::api::tiledb_dictionary_get_cell_val_num>(
    ctx, dict, cell_val_num);
}

capi_return_t tiledb_dictionary_set_nullable(
  tiledb_ctx_t* ctx,
  tiledb_dictionary_t* dict,
  uint8_t nullable) noexcept {
  return api_entry_context<tiledb::api::tiledb_dictionary_set_nullable>(
    ctx, dict, nullable);
}

capi_return_t tiledb_dictionary_get_nullable(
  tiledb_ctx_t* ctx,
  tiledb_dictionary_t* dict,
  uint8_t* nullable) noexcept {
  return api_entry_context<tiledb::api::tiledb_dictionary_get_nullable>(
    ctx, dict, nullable);
}

capi_return_t tiledb_dictionary_set_ordered(
  tiledb_ctx_t* ctx,
  tiledb_dictionary_t* dict,
  uint8_t ordered) noexcept {
  return api_entry_context<tiledb::api::tiledb_dictionary_set_ordered>(
    ctx, dict, ordered);
}

capi_return_t tiledb_dictionary_get_ordered(
  tiledb_ctx_t* ctx,
  tiledb_dictionary_t* dict,
  uint8_t* ordered) noexcept {
  return api_entry_context<tiledb::api::tiledb_dictionary_get_ordered>(
    ctx, dict, ordered);
}

capi_return_t tiledb_dictionary_set_data_buffer(
  tiledb_ctx_t* ctx,
  tiledb_dictionary_t* dict,
  void* buffer,
  uint64_t buffer_size) noexcept {
  return api_entry_context<tiledb::api::tiledb_dictionary_set_data_buffer>(
    ctx, dict, buffer, buffer_size);
}

capi_return_t tiledb_dictionary_get_data_buffer(
  tiledb_ctx_t* ctx,
  tiledb_dictionary_t* dict,
  void** buffer,
  uint64_t* buffer_size) noexcept {
  return api_entry_context<tiledb::api::tiledb_dictionary_get_data_buffer>(
    ctx, dict, buffer, buffer_size);
}

capi_return_t tiledb_dictionary_set_offsets_buffer(
  tiledb_ctx_t* ctx,
  tiledb_dictionary_t* dict,
  void* buffer,
  uint64_t buffer_size) noexcept {
  return api_entry_context<tiledb::api::tiledb_dictionary_set_offsets_buffer>(
    ctx, dict, buffer, buffer_size);
}

capi_return_t tiledb_dictionary_get_offsets_buffer(
  tiledb_ctx_t* ctx,
  tiledb_dictionary_t* dict,
  void** buffer,
  uint64_t* buffer_size) noexcept {
  return api_entry_context<tiledb::api::tiledb_dictionary_get_offsets_buffer>(
    ctx, dict, buffer, buffer_size);
}

capi_return_t tiledb_dictionary_set_validity_buffer(
  tiledb_ctx_t* ctx,
  tiledb_dictionary_t* dict,
  void* buffer,
  uint64_t buffer_size) noexcept {
  return api_entry_context<tiledb::api::tiledb_dictionary_set_validity_buffer>(
    ctx, dict, buffer, buffer_size);
}

capi_return_t tiledb_dictionary_get_validity_buffer(
  tiledb_ctx_t* ctx,
  tiledb_dictionary_t* dict,
  void** buffer,
  uint64_t* buffer_size) noexcept {
  return api_entry_context<tiledb::api::tiledb_dictionary_get_validity_buffer>(
    ctx, dict, buffer, buffer_size);
}

capi_return_t tiledb_dictionary_dump(
    tiledb_ctx_t* ctx, const tiledb_dictionary_t* dict, FILE* out) noexcept {
  return api_entry_context<tiledb::api::tiledb_dictionary_dump>(ctx, dict, out);
}
