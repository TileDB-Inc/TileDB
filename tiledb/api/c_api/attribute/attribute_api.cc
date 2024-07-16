/**
 * @file tiledb/api/c_api/attribute/attribute_api.cc
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
 * This file defines C API functions for the attribute section.
 */

#include "../../c_api_support/c_api_support.h"
#include "../filter_list/filter_list_api_internal.h"
#include "../string/string_api_internal.h"
#include "attribute_api_external.h"
#include "attribute_api_external_experimental.h"
#include "attribute_api_internal.h"
#include "tiledb/sm/enums/datatype.h"

namespace tiledb::api {

int32_t tiledb_attribute_alloc(
    const char* name,
    tiledb_datatype_t type,
    tiledb_attribute_handle_t** attr) {
  ensure_output_pointer_is_valid(attr);
  if (!name) {
    throw CAPIStatusException("Argument \"name\" may not be NULL");
  }
  sm::ensure_datatype_is_valid(type);
  *attr = tiledb_attribute_handle_t::make_handle(
      name, static_cast<tiledb::sm::Datatype>(type));
  return TILEDB_OK;
}

void tiledb_attribute_free(tiledb_attribute_handle_t** attr) {
  ensure_output_pointer_is_valid(attr);
  ensure_attribute_is_valid(*attr);
  tiledb_attribute_handle_t::break_handle(*attr);
}

int32_t tiledb_attribute_set_nullable(
    tiledb_attribute_handle_t* attr, uint8_t nullable) {
  ensure_attribute_is_valid(attr);
  attr->set_nullable(static_cast<bool>(nullable));
  return TILEDB_OK;
}

int32_t tiledb_attribute_set_filter_list(
    tiledb_attribute_handle_t* attr, tiledb_filter_list_handle_t* filter_list) {
  ensure_attribute_is_valid(attr);
  api::ensure_filter_list_is_valid(filter_list);
  attr->set_filter_pipeline(filter_list->pipeline());
  return TILEDB_OK;
}

int32_t tiledb_attribute_set_cell_val_num(
    tiledb_attribute_handle_t* attr, uint32_t cell_val_num) {
  ensure_attribute_is_valid(attr);
  attr->set_cell_val_num(cell_val_num);
  return TILEDB_OK;
}

int32_t tiledb_attribute_get_name(
    const tiledb_attribute_handle_t* attr, const char** name) {
  ensure_attribute_is_valid(attr);
  api::ensure_output_pointer_is_valid(name);
  *name = attr->name().c_str();
  return TILEDB_OK;
}

int32_t tiledb_attribute_get_type(
    const tiledb_attribute_handle_t* attr, tiledb_datatype_t* type) {
  ensure_attribute_is_valid(attr);
  api::ensure_output_pointer_is_valid(type);
  *type = static_cast<tiledb_datatype_t>(attr->type());
  return TILEDB_OK;
}

int32_t tiledb_attribute_get_nullable(
    tiledb_attribute_handle_t* attr, uint8_t* nullable) {
  ensure_attribute_is_valid(attr);
  api::ensure_output_pointer_is_valid(nullable);
  *nullable = attr->nullable() ? 1 : 0;
  return TILEDB_OK;
}

int32_t tiledb_attribute_get_filter_list(
    tiledb_attribute_handle_t* attr, tiledb_filter_list_t** filter_list) {
  ensure_attribute_is_valid(attr);
  api::ensure_output_pointer_is_valid(filter_list);
  // Copy-construct a separate FilterPipeline object
  *filter_list =
      tiledb_filter_list_t::make_handle(sm::FilterPipeline{attr->filters()});
  return TILEDB_OK;
}

int32_t tiledb_attribute_get_cell_val_num(
    const tiledb_attribute_handle_t* attr, uint32_t* cell_val_num) {
  ensure_attribute_is_valid(attr);
  api::ensure_output_pointer_is_valid(cell_val_num);
  *cell_val_num = attr->cell_val_num();
  return TILEDB_OK;
}

int32_t tiledb_attribute_get_cell_size(
    const tiledb_attribute_handle_t* attr, uint64_t* cell_size) {
  ensure_attribute_is_valid(attr);
  api::ensure_output_pointer_is_valid(cell_size);
  *cell_size = attr->cell_size();
  return TILEDB_OK;
}

int32_t tiledb_attribute_dump(
    const tiledb_attribute_handle_t* attr, FILE* out) {
  ensure_attribute_is_valid(attr);
  ensure_cstream_handle_is_valid(out);

  std::stringstream ss;
  ss << *attr;
  size_t r = fwrite(ss.str().c_str(), sizeof(char), ss.str().size(), out);
  if (r != ss.str().size()) {
    throw CAPIException(
        "Error writing attribute " + attr->name() + " to output stream");
  }

  return TILEDB_OK;
}

int32_t tiledb_attribute_dump_str(
    const tiledb_attribute_handle_t* attr, tiledb_string_handle_t** out) {
  ensure_attribute_is_valid(attr);
  ensure_output_pointer_is_valid(out);

  std::stringstream ss;
  ss << *attr;
  *out = tiledb_string_handle_t::make_handle(ss.str());

  return TILEDB_OK;
}

int32_t tiledb_attribute_set_fill_value(
    tiledb_attribute_handle_t* attr, const void* value, uint64_t size) {
  ensure_attribute_is_valid(attr);
  attr->set_fill_value(value, size);
  return TILEDB_OK;
}

int32_t tiledb_attribute_get_fill_value(
    tiledb_attribute_handle_t* attr, const void** value, uint64_t* size) {
  ensure_attribute_is_valid(attr);
  api::ensure_output_pointer_is_valid(value);
  api::ensure_output_pointer_is_valid(size);
  attr->get_fill_value(value, size);
  return TILEDB_OK;
}

int32_t tiledb_attribute_set_fill_value_nullable(
    tiledb_attribute_handle_t* attr,
    const void* value,
    uint64_t size,
    uint8_t valid) {
  ensure_attribute_is_valid(attr);
  attr->set_fill_value(value, size, valid);
  return TILEDB_OK;
}

int32_t tiledb_attribute_get_fill_value_nullable(
    tiledb_attribute_handle_t* attr,
    const void** value,
    uint64_t* size,
    uint8_t* valid) {
  ensure_attribute_is_valid(attr);
  api::ensure_output_pointer_is_valid(value);
  api::ensure_output_pointer_is_valid(size);
  api::ensure_output_pointer_is_valid(valid);
  attr->get_fill_value(value, size, valid);
  return TILEDB_OK;
}

capi_return_t tiledb_attribute_set_enumeration_name(
    tiledb_attribute_t* attr, const char* enumeration_name) {
  ensure_attribute_is_valid(attr);
  attr->set_enumeration_name(enumeration_name);
  return TILEDB_OK;
}

capi_return_t tiledb_attribute_get_enumeration_name(
    tiledb_attribute_t* attr, tiledb_string_handle_t** name) {
  ensure_attribute_is_valid(attr);
  ensure_output_pointer_is_valid(name);

  auto enmr_name = attr->get_enumeration_name();
  if (!enmr_name.has_value()) {
    *name = nullptr;
    return TILEDB_OK;
  }

  *name = tiledb_string_handle_t::make_handle(enmr_name.value());

  return TILEDB_OK;
}

}  // namespace tiledb::api

std::ostream& operator<<(
    std::ostream& os, const tiledb_attribute_handle_t& attr) {
  os << *attr.attr_;
  return os;
}

using tiledb::api::api_entry_context;

CAPI_INTERFACE(
    attribute_alloc,
    tiledb_ctx_t* ctx,
    const char* name,
    tiledb_datatype_t type,
    tiledb_attribute_handle_t** attr) {
  return api_entry_context<tiledb::api::tiledb_attribute_alloc>(
      ctx, name, type, attr);
}

CAPI_INTERFACE_VOID(attribute_free, tiledb_attribute_handle_t** attr) {
  return tiledb::api::api_entry_void<tiledb::api::tiledb_attribute_free>(attr);
}

CAPI_INTERFACE(
    attribute_set_nullable,
    tiledb_ctx_t* ctx,
    tiledb_attribute_handle_t* attr,
    uint8_t nullable) {
  return api_entry_context<tiledb::api::tiledb_attribute_set_nullable>(
      ctx, attr, nullable);
}

CAPI_INTERFACE(
    attribute_set_filter_list,
    tiledb_ctx_t* ctx,
    tiledb_attribute_handle_t* attr,
    tiledb_filter_list_t* filter_list) {
  return api_entry_context<tiledb::api::tiledb_attribute_set_filter_list>(
      ctx, attr, filter_list);
}

CAPI_INTERFACE(
    attribute_set_cell_val_num,
    tiledb_ctx_t* ctx,
    tiledb_attribute_handle_t* attr,
    uint32_t cell_val_num) {
  return api_entry_context<tiledb::api::tiledb_attribute_set_cell_val_num>(
      ctx, attr, cell_val_num);
}

CAPI_INTERFACE(
    attribute_get_name,
    tiledb_ctx_t* ctx,
    const tiledb_attribute_handle_t* attr,
    const char** name) {
  return api_entry_context<tiledb::api::tiledb_attribute_get_name>(
      ctx, attr, name);
}

CAPI_INTERFACE(
    attribute_get_type,
    tiledb_ctx_t* ctx,
    const tiledb_attribute_handle_t* attr,
    tiledb_datatype_t* type) {
  return api_entry_context<tiledb::api::tiledb_attribute_get_type>(
      ctx, attr, type);
}

CAPI_INTERFACE(
    attribute_get_nullable,
    tiledb_ctx_t* ctx,
    tiledb_attribute_handle_t* attr,
    uint8_t* nullable) {
  return api_entry_context<tiledb::api::tiledb_attribute_get_nullable>(
      ctx, attr, nullable);
}

CAPI_INTERFACE(
    attribute_get_filter_list,
    tiledb_ctx_t* ctx,
    tiledb_attribute_handle_t* attr,
    tiledb_filter_list_t** filter_list) {
  return api_entry_context<tiledb::api::tiledb_attribute_get_filter_list>(
      ctx, attr, filter_list);
}

CAPI_INTERFACE(
    attribute_get_cell_val_num,
    tiledb_ctx_t* ctx,
    const tiledb_attribute_handle_t* attr,
    uint32_t* cell_val_num) {
  return api_entry_context<tiledb::api::tiledb_attribute_get_cell_val_num>(
      ctx, attr, cell_val_num);
}

CAPI_INTERFACE(
    attribute_get_cell_size,
    tiledb_ctx_t* ctx,
    const tiledb_attribute_handle_t* attr,
    uint64_t* cell_size) {
  return api_entry_context<tiledb::api::tiledb_attribute_get_cell_size>(
      ctx, attr, cell_size);
}

CAPI_INTERFACE(
    attribute_dump,
    tiledb_ctx_t* ctx,
    const tiledb_attribute_handle_t* attr,
    FILE* out) {
  return api_entry_context<tiledb::api::tiledb_attribute_dump>(ctx, attr, out);
}

CAPI_INTERFACE(
    attribute_dump_str,
    tiledb_ctx_t* ctx,
    const tiledb_attribute_handle_t* attr,
    tiledb_string_handle_t** out) {
  return api_entry_context<tiledb::api::tiledb_attribute_dump_str>(
      ctx, attr, out);
}

CAPI_INTERFACE(
    attribute_set_fill_value,
    tiledb_ctx_t* ctx,
    tiledb_attribute_handle_t* attr,
    const void* value,
    uint64_t size) {
  return api_entry_context<tiledb::api::tiledb_attribute_set_fill_value>(
      ctx, attr, value, size);
}

CAPI_INTERFACE(
    attribute_get_fill_value,
    tiledb_ctx_t* ctx,
    tiledb_attribute_handle_t* attr,
    const void** value,
    uint64_t* size) {
  return api_entry_context<tiledb::api::tiledb_attribute_get_fill_value>(
      ctx, attr, value, size);
}

CAPI_INTERFACE(
    attribute_set_fill_value_nullable,
    tiledb_ctx_t* ctx,
    tiledb_attribute_handle_t* attr,
    const void* value,
    uint64_t size,
    uint8_t valid) {
  return api_entry_context<
      tiledb::api::tiledb_attribute_set_fill_value_nullable>(
      ctx, attr, value, size, valid);
}

CAPI_INTERFACE(
    attribute_get_fill_value_nullable,
    tiledb_ctx_t* ctx,
    tiledb_attribute_handle_t* attr,
    const void** value,
    uint64_t* size,
    uint8_t* valid) {
  return api_entry_context<
      tiledb::api::tiledb_attribute_get_fill_value_nullable>(
      ctx, attr, value, size, valid);
}

CAPI_INTERFACE(
    attribute_set_enumeration_name,
    tiledb_ctx_t* ctx,
    tiledb_attribute_t* attr,
    const char* enumeration_name) {
  return api_entry_context<tiledb::api::tiledb_attribute_set_enumeration_name>(
      ctx, attr, enumeration_name);
}

CAPI_INTERFACE(
    attribute_get_enumeration_name,
    tiledb_ctx_t* ctx,
    tiledb_attribute_t* attr,
    tiledb_string_handle_t** name) {
  return api_entry_context<tiledb::api::tiledb_attribute_get_enumeration_name>(
      ctx, attr, name);
}
