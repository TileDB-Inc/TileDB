/**
 * @file tiledb/api/c_api/filter/filter_api.cc
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
 * This file defines C API functions for the filter section.
 */

#include "filter_api_external.h"
#include "filter_api_internal.h"
#include "tiledb/api/c_api_support/c_api_support.h"
#include "tiledb/sm/enums/filter_type.h"

namespace tiledb::api {

capi_return_t tiledb_filter_alloc(
    tiledb_ctx_t*, tiledb_filter_type_t type, tiledb_filter_t** filter) {
  ensure_output_pointer_is_valid(filter);

  *filter = make_handle<tiledb_filter_t>(tiledb::sm::FilterCreate::make(
      static_cast<tiledb::sm::FilterType>(type)));
  return TILEDB_OK;
}

void tiledb_filter_free(tiledb_filter_t** filter) {
  ensure_output_pointer_is_valid(filter);
  ensure_filter_is_valid(*filter);
  break_handle(*filter);
}

capi_return_t tiledb_filter_get_type(
    tiledb_filter_t* filter, tiledb_filter_type_t* type) {
  ensure_filter_is_valid(filter);
  ensure_output_pointer_is_valid(type);
  *type = static_cast<tiledb_filter_type_t>(filter->type());
  return TILEDB_OK;
}

capi_return_t tiledb_filter_set_option(
    tiledb_filter_t* filter, tiledb_filter_option_t option, const void* value) {
  ensure_filter_is_valid(filter);
  auto st{
      filter->set_option(static_cast<tiledb::sm::FilterOption>(option), value)};
  if (!st.ok()) {
    throw StatusException(st);
  }
  return TILEDB_OK;
}

capi_return_t tiledb_filter_get_option(
    tiledb_filter_t* filter, tiledb_filter_option_t option, void* value) {
  ensure_filter_is_valid(filter);
  ensure_output_pointer_is_valid(value);
  auto st{
      filter->get_option(static_cast<tiledb::sm::FilterOption>(option), value)};
  if (!st.ok()) {
    throw StatusException(st);
  }
  return TILEDB_OK;
}

capi_return_t tiledb_filter_type_to_str(
    tiledb_filter_type_t filter_type, const char** str) {
  const auto& strval =
      tiledb::sm::filter_type_str((tiledb::sm::FilterType)filter_type);
  *str = strval.c_str();
  return strval.empty() ? TILEDB_ERR : TILEDB_OK;
}

capi_return_t tiledb_filter_type_from_str(
    const char* str, tiledb_filter_type_t* filter_type) {
  tiledb::sm::FilterType val = tiledb::sm::FilterType::FILTER_NONE;
  if (!tiledb::sm::filter_type_enum(str, &val).ok())
    return TILEDB_ERR;
  *filter_type = (tiledb_filter_type_t)val;
  return TILEDB_OK;
}

capi_return_t tiledb_filter_option_to_str(
    tiledb_filter_option_t filter_option, const char** str) {
  const auto& strval =
      tiledb::sm::filter_option_str((tiledb::sm::FilterOption)filter_option);
  *str = strval.c_str();
  return strval.empty() ? TILEDB_ERR : TILEDB_OK;
}

capi_return_t tiledb_filter_option_from_str(
    const char* str, tiledb_filter_option_t* filter_option) {
  tiledb::sm::FilterOption val = tiledb::sm::FilterOption::COMPRESSION_LEVEL;
  if (!tiledb::sm::filter_option_enum(str, &val).ok())
    return TILEDB_ERR;
  *filter_option = (tiledb_filter_option_t)val;
  return TILEDB_OK;
}

}  // namespace tiledb::api

using tiledb::api::api_entry_context;
using tiledb::api::api_entry_plain;

CAPI_INTERFACE(
    filter_alloc,
    tiledb_ctx_t* ctx,
    tiledb_filter_type_t type,
    tiledb_filter_t** filter) {
  return tiledb::api::api_entry_with_context<tiledb::api::tiledb_filter_alloc>(
      ctx, type, filter);
}

CAPI_INTERFACE_VOID(filter_free, tiledb_filter_t** filter) {
  return tiledb::api::api_entry_void<tiledb::api::tiledb_filter_free>(filter);
}

CAPI_INTERFACE(
    filter_get_type,
    tiledb_ctx_t* ctx,
    tiledb_filter_t* filter,
    tiledb_filter_type_t* type) {
  return api_entry_context<tiledb::api::tiledb_filter_get_type>(
      ctx, filter, type);
}

CAPI_INTERFACE(
    filter_set_option,
    tiledb_ctx_t* ctx,
    tiledb_filter_t* filter,
    tiledb_filter_option_t option,
    const void* value) {
  return api_entry_context<tiledb::api::tiledb_filter_set_option>(
      ctx, filter, option, value);
}

CAPI_INTERFACE(
    filter_get_option,
    tiledb_ctx_t* ctx,
    tiledb_filter_t* filter,
    tiledb_filter_option_t option,
    void* value) {
  return api_entry_context<tiledb::api::tiledb_filter_get_option>(
      ctx, filter, option, value);
}

CAPI_INTERFACE(
    filter_type_to_str, tiledb_filter_type_t filter_type, const char** str) {
  return api_entry_plain<tiledb::api::tiledb_filter_type_to_str>(
      filter_type, str);
}

CAPI_INTERFACE(
    filter_type_from_str, const char* str, tiledb_filter_type_t* filter_type) {
  return api_entry_plain<tiledb::api::tiledb_filter_type_from_str>(
      str, filter_type);
}

CAPI_INTERFACE(
    filter_option_to_str,
    tiledb_filter_option_t filter_option,
    const char** str) {
  return api_entry_plain<tiledb::api::tiledb_filter_option_to_str>(
      filter_option, str);
}

CAPI_INTERFACE(
    filter_option_from_str,
    const char* str,
    tiledb_filter_option_t* filter_option) {
  return api_entry_plain<tiledb::api::tiledb_filter_option_from_str>(
      str, filter_option);
}
