/**
 * @file tiledb/api/c_api/dimension_label/dimension_label_api.cc
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
 * This file defines C API functions for the dimension label section.
 */

#include "dimension_label_api_external.h"
#include "dimension_label_api_internal.h"
#include "tiledb/api/c_api_support/c_api_support.h"

namespace tiledb::api {

void tiledb_dimension_label_free(tiledb_dimension_label_t** dim_label) {
  tiledb::api::ensure_output_pointer_is_valid(dim_label);
  ensure_dimension_label_is_valid(*dim_label);
  break_handle(*dim_label);
}

capi_return_t tiledb_dimension_label_get_dimension_index(
    tiledb_dimension_label_t* dim_label, uint32_t* dim_index) {
  ensure_dimension_label_is_valid(dim_label);
  ensure_output_pointer_is_valid(dim_index);
  *dim_index = dim_label->dimension_label().dimension_index();
  return TILEDB_OK;
}

capi_return_t tiledb_dimension_label_get_label_attr_name(
    tiledb_dimension_label_t* dim_label, const char** label_attr_name) {
  ensure_dimension_label_is_valid(dim_label);
  ensure_output_pointer_is_valid(label_attr_name);
  *label_attr_name = dim_label->dimension_label().label_attr_name().c_str();
  return TILEDB_OK;
}

capi_return_t tiledb_dimension_label_get_label_cell_val_num(
    tiledb_dimension_label_t* dim_label, uint32_t* label_cell_val_num) {
  ensure_dimension_label_is_valid(dim_label);
  ensure_output_pointer_is_valid(label_cell_val_num);
  *label_cell_val_num = dim_label->dimension_label().label_cell_val_num();
  return TILEDB_OK;
}

capi_return_t tiledb_dimension_label_get_label_order(
    tiledb_dimension_label_t* dim_label, tiledb_data_order_t* label_order) {
  ensure_dimension_label_is_valid(dim_label);
  ensure_output_pointer_is_valid(label_order);
  *label_order = static_cast<tiledb_data_order_t>(
      dim_label->dimension_label().label_order());
  return TILEDB_OK;
}

capi_return_t tiledb_dimension_label_get_label_type(
    tiledb_dimension_label_t* dim_label, tiledb_datatype_t* label_type) {
  ensure_dimension_label_is_valid(dim_label);
  ensure_output_pointer_is_valid(label_type);
  *label_type =
      static_cast<tiledb_datatype_t>(dim_label->dimension_label().label_type());
  return TILEDB_OK;
}

capi_return_t tiledb_dimension_label_get_name(
    tiledb_dimension_label_t* dim_label, const char** name) {
  ensure_dimension_label_is_valid(dim_label);
  ensure_output_pointer_is_valid(name);
  *name = dim_label->dimension_label().name().c_str();
  return TILEDB_OK;
}

capi_return_t tiledb_dimension_label_get_uri(
    tiledb_dimension_label_t* dim_label, const char** uri) {
  ensure_dimension_label_is_valid(dim_label);
  ensure_output_pointer_is_valid(uri);
  *uri = dim_label->uri().c_str();
  return TILEDB_OK;
}

}  // namespace tiledb::api

using tiledb::api::api_entry_context;
using tiledb::api::api_entry_void;

CAPI_INTERFACE_VOID(
    dimension_label_free, tiledb_dimension_label_t** dim_label) {
  return api_entry_void<tiledb::api::tiledb_dimension_label_free>(dim_label);
}

CAPI_INTERFACE(
    dimension_label_get_dimension_index,
    tiledb_ctx_t* ctx,
    tiledb_dimension_label_t* dim_label,
    uint32_t* dim_index) {
  return api_entry_context<
      tiledb::api::tiledb_dimension_label_get_dimension_index>(
      ctx, dim_label, dim_index);
}

CAPI_INTERFACE(
    dimension_label_get_label_attr_name,
    tiledb_ctx_t* ctx,
    tiledb_dimension_label_t* dim_label,
    const char** label_attr_name) {
  return api_entry_context<
      tiledb::api::tiledb_dimension_label_get_label_attr_name>(
      ctx, dim_label, label_attr_name);
}

CAPI_INTERFACE(
    dimension_label_get_label_cell_val_num,
    tiledb_ctx_t* ctx,
    tiledb_dimension_label_t* dim_label,
    uint32_t* label_cell_val_num) {
  return api_entry_context<
      tiledb::api::tiledb_dimension_label_get_label_cell_val_num>(
      ctx, dim_label, label_cell_val_num);
}

CAPI_INTERFACE(
    dimension_label_get_label_order,
    tiledb_ctx_t* ctx,
    tiledb_dimension_label_t* dim_label,
    tiledb_data_order_t* label_order) {
  return api_entry_context<tiledb::api::tiledb_dimension_label_get_label_order>(
      ctx, dim_label, label_order);
}

CAPI_INTERFACE(
    dimension_label_get_label_type,
    tiledb_ctx_t* ctx,
    tiledb_dimension_label_t* dim_label,
    tiledb_datatype_t* label_type) {
  return api_entry_context<tiledb::api::tiledb_dimension_label_get_label_type>(
      ctx, dim_label, label_type);
}

CAPI_INTERFACE(
    dimension_label_get_name,
    tiledb_ctx_t* ctx,
    tiledb_dimension_label_t* dim_label,
    const char** name) {
  return api_entry_context<tiledb::api::tiledb_dimension_label_get_name>(
      ctx, dim_label, name);
}

CAPI_INTERFACE(
    dimension_label_get_uri,
    tiledb_ctx_t* ctx,
    tiledb_dimension_label_t* dim_label,
    const char** uri) {
  return api_entry_context<tiledb::api::tiledb_dimension_label_get_uri>(
      ctx, dim_label, uri);
}
