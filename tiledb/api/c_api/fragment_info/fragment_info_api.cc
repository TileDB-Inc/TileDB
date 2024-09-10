/**
 * @file tiledb/api/c_api/fragment_info/fragment_info_api.cc
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
 * This file defines C API functions for the fragment info section.
 */

#include "fragment_info_api_experimental.h"
#include "fragment_info_api_internal.h"
#include "tiledb/api/c_api/array_schema/array_schema_api_internal.h"
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/api/c_api/string/string_api_internal.h"
#include "tiledb/api/c_api_support/c_api_support.h"

namespace tiledb::api {

capi_return_t tiledb_fragment_info_alloc(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_fragment_info_t** fragment_info) {
  ensure_output_pointer_is_valid(fragment_info);
  // Check array URI
  auto uri = tiledb::sm::URI(array_uri);
  if (uri.is_invalid()) {
    throw CAPIException(
        "Failed to create TileDB fragment info object; Invalid URI");
  }

  // Create FragmentInfo object
  *fragment_info = tiledb_fragment_info_t::make_handle(uri, ctx->resources());
  return TILEDB_OK;
}

void tiledb_fragment_info_free(tiledb_fragment_info_t** fragment_info) {
  ensure_output_pointer_is_valid(fragment_info);
  ensure_fragment_info_is_valid(*fragment_info);
  tiledb_fragment_info_t::break_handle(*fragment_info);
}

capi_return_t tiledb_fragment_info_set_config(
    tiledb_fragment_info_t* fragment_info, tiledb_config_t* config) {
  ensure_fragment_info_is_valid(fragment_info);
  ensure_config_is_valid(config);
  fragment_info->set_config(config->config());
  return TILEDB_OK;
}

capi_return_t tiledb_fragment_info_get_config(
    tiledb_fragment_info_t* fragment_info, tiledb_config_t** config) {
  ensure_fragment_info_is_valid(fragment_info);
  ensure_output_pointer_is_valid(config);
  *config = tiledb_config_handle_t::make_handle(fragment_info->config());
  return TILEDB_OK;
}

capi_return_t tiledb_fragment_info_load(tiledb_fragment_info_t* fragment_info) {
  ensure_fragment_info_is_valid(fragment_info);
  throw_if_not_ok(fragment_info->load());
  return TILEDB_OK;
}

capi_return_t tiledb_fragment_info_get_fragment_name_v2(
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    tiledb_string_t** name) {
  ensure_fragment_info_is_valid(fragment_info);
  ensure_output_pointer_is_valid(name);
  *name =
      tiledb_string_handle_t::make_handle(fragment_info->fragment_name(fid));
  return TILEDB_OK;
}

capi_return_t tiledb_fragment_info_get_fragment_num(
    tiledb_fragment_info_t* fragment_info, uint32_t* fragment_num) {
  ensure_fragment_info_is_valid(fragment_info);
  ensure_output_pointer_is_valid(fragment_num);
  *fragment_num = fragment_info->fragment_num();
  return TILEDB_OK;
}

capi_return_t tiledb_fragment_info_get_fragment_uri(
    tiledb_fragment_info_t* fragment_info, uint32_t fid, const char** uri) {
  ensure_fragment_info_is_valid(fragment_info);
  ensure_output_pointer_is_valid(uri);
  throw_if_not_ok(fragment_info->get_fragment_uri(fid, uri));
  return TILEDB_OK;
}

capi_return_t tiledb_fragment_info_get_fragment_size(
    tiledb_fragment_info_t* fragment_info, uint32_t fid, uint64_t* size) {
  ensure_fragment_info_is_valid(fragment_info);
  ensure_output_pointer_is_valid(size);
  throw_if_not_ok(fragment_info->get_fragment_size(fid, size));
  return TILEDB_OK;
}

capi_return_t tiledb_fragment_info_get_dense(
    tiledb_fragment_info_t* fragment_info, uint32_t fid, int32_t* dense) {
  ensure_fragment_info_is_valid(fragment_info);
  ensure_output_pointer_is_valid(dense);
  throw_if_not_ok(fragment_info->get_dense(fid, dense));
  return TILEDB_OK;
}

capi_return_t tiledb_fragment_info_get_sparse(
    tiledb_fragment_info_t* fragment_info, uint32_t fid, int32_t* sparse) {
  ensure_fragment_info_is_valid(fragment_info);
  ensure_output_pointer_is_valid(sparse);
  throw_if_not_ok(fragment_info->get_sparse(fid, sparse));
  return TILEDB_OK;
}

capi_return_t tiledb_fragment_info_get_timestamp_range(
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint64_t* start,
    uint64_t* end) {
  ensure_fragment_info_is_valid(fragment_info);
  ensure_output_pointer_is_valid(start);
  ensure_output_pointer_is_valid(end);
  throw_if_not_ok(fragment_info->get_timestamp_range(fid, start, end));
  return TILEDB_OK;
}

capi_return_t tiledb_fragment_info_get_non_empty_domain_from_index(
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t did,
    void* domain) {
  ensure_fragment_info_is_valid(fragment_info);
  ensure_output_pointer_is_valid(domain);
  throw_if_not_ok(fragment_info->get_non_empty_domain(fid, did, domain));
  return TILEDB_OK;
}

capi_return_t tiledb_fragment_info_get_non_empty_domain_from_name(
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    const char* dim_name,
    void* domain) {
  ensure_fragment_info_is_valid(fragment_info);
  ensure_output_pointer_is_valid(domain);
  throw_if_not_ok(fragment_info->get_non_empty_domain(fid, dim_name, domain));
  return TILEDB_OK;
}

capi_return_t tiledb_fragment_info_get_non_empty_domain_var_size_from_index(
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t did,
    uint64_t* start_size,
    uint64_t* end_size) {
  ensure_fragment_info_is_valid(fragment_info);
  ensure_output_pointer_is_valid(start_size);
  ensure_output_pointer_is_valid(end_size);
  throw_if_not_ok(fragment_info->get_non_empty_domain_var_size(
      fid, did, start_size, end_size));
  return TILEDB_OK;
}

capi_return_t tiledb_fragment_info_get_non_empty_domain_var_size_from_name(
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    const char* dim_name,
    uint64_t* start_size,
    uint64_t* end_size) {
  ensure_fragment_info_is_valid(fragment_info);
  ensure_output_pointer_is_valid(start_size);
  ensure_output_pointer_is_valid(end_size);
  throw_if_not_ok(fragment_info->get_non_empty_domain_var_size(
      fid, dim_name, start_size, end_size));
  return TILEDB_OK;
}

capi_return_t tiledb_fragment_info_get_non_empty_domain_var_from_index(
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t did,
    void* start,
    void* end) {
  ensure_fragment_info_is_valid(fragment_info);
  ensure_output_pointer_is_valid(start);
  ensure_output_pointer_is_valid(end);
  throw_if_not_ok(
      fragment_info->get_non_empty_domain_var(fid, did, start, end));
  return TILEDB_OK;
}

capi_return_t tiledb_fragment_info_get_non_empty_domain_var_from_name(
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    const char* dim_name,
    void* start,
    void* end) {
  ensure_fragment_info_is_valid(fragment_info);
  ensure_output_pointer_is_valid(start);
  ensure_output_pointer_is_valid(end);
  throw_if_not_ok(
      fragment_info->get_non_empty_domain_var(fid, dim_name, start, end));
  return TILEDB_OK;
}

capi_return_t tiledb_fragment_info_get_mbr_num(
    tiledb_fragment_info_t* fragment_info, uint32_t fid, uint64_t* mbr_num) {
  ensure_fragment_info_is_valid(fragment_info);
  ensure_output_pointer_is_valid(mbr_num);
  throw_if_not_ok(fragment_info->get_mbr_num(fid, mbr_num));
  return TILEDB_OK;
}

capi_return_t tiledb_fragment_info_get_mbr_from_index(
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t mid,
    uint32_t did,
    void* mbr) {
  ensure_fragment_info_is_valid(fragment_info);
  ensure_output_pointer_is_valid(mbr);
  throw_if_not_ok(fragment_info->get_mbr(fid, mid, did, mbr));
  return TILEDB_OK;
}

capi_return_t tiledb_fragment_info_get_mbr_from_name(
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t mid,
    const char* dim_name,
    void* mbr) {
  ensure_fragment_info_is_valid(fragment_info);
  ensure_output_pointer_is_valid(mbr);
  throw_if_not_ok(fragment_info->get_mbr(fid, mid, dim_name, mbr));
  return TILEDB_OK;
}

capi_return_t tiledb_fragment_info_get_mbr_var_size_from_index(
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t mid,
    uint32_t did,
    uint64_t* start_size,
    uint64_t* end_size) {
  ensure_fragment_info_is_valid(fragment_info);
  ensure_output_pointer_is_valid(start_size);
  ensure_output_pointer_is_valid(end_size);
  throw_if_not_ok(
      fragment_info->get_mbr_var_size(fid, mid, did, start_size, end_size));
  return TILEDB_OK;
}

capi_return_t tiledb_fragment_info_get_mbr_var_size_from_name(
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t mid,
    const char* dim_name,
    uint64_t* start_size,
    uint64_t* end_size) {
  ensure_fragment_info_is_valid(fragment_info);
  ensure_output_pointer_is_valid(start_size);
  ensure_output_pointer_is_valid(end_size);
  throw_if_not_ok(fragment_info->get_mbr_var_size(
      fid, mid, dim_name, start_size, end_size));
  return TILEDB_OK;
}

capi_return_t tiledb_fragment_info_get_mbr_var_from_index(
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t mid,
    uint32_t did,
    void* start,
    void* end) {
  ensure_fragment_info_is_valid(fragment_info);
  ensure_output_pointer_is_valid(start);
  ensure_output_pointer_is_valid(end);
  throw_if_not_ok(fragment_info->get_mbr_var(fid, mid, did, start, end));
  return TILEDB_OK;
}

capi_return_t tiledb_fragment_info_get_mbr_var_from_name(
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t mid,
    const char* dim_name,
    void* start,
    void* end) {
  ensure_fragment_info_is_valid(fragment_info);
  ensure_output_pointer_is_valid(start);
  ensure_output_pointer_is_valid(end);
  throw_if_not_ok(fragment_info->get_mbr_var(fid, mid, dim_name, start, end));
  return TILEDB_OK;
}

capi_return_t tiledb_fragment_info_get_cell_num(
    tiledb_fragment_info_t* fragment_info, uint32_t fid, uint64_t* cell_num) {
  ensure_fragment_info_is_valid(fragment_info);
  ensure_output_pointer_is_valid(cell_num);
  throw_if_not_ok(fragment_info->get_cell_num(fid, cell_num));
  return TILEDB_OK;
}

capi_return_t tiledb_fragment_info_get_total_cell_num(
    tiledb_fragment_info_t* fragment_info, uint64_t* cell_num) {
  ensure_fragment_info_is_valid(fragment_info);
  ensure_output_pointer_is_valid(cell_num);
  throw_if_not_ok(fragment_info->get_total_cell_num(cell_num));
  return TILEDB_OK;
}

capi_return_t tiledb_fragment_info_get_version(
    tiledb_fragment_info_t* fragment_info, uint32_t fid, uint32_t* version) {
  ensure_fragment_info_is_valid(fragment_info);
  ensure_output_pointer_is_valid(version);
  throw_if_not_ok(fragment_info->get_version(fid, version));
  return TILEDB_OK;
}

capi_return_t tiledb_fragment_info_has_consolidated_metadata(
    tiledb_fragment_info_t* fragment_info, uint32_t fid, int32_t* has) {
  ensure_fragment_info_is_valid(fragment_info);
  ensure_output_pointer_is_valid(has);
  throw_if_not_ok(fragment_info->has_consolidated_metadata(fid, has));
  return TILEDB_OK;
}

capi_return_t tiledb_fragment_info_get_unconsolidated_metadata_num(
    tiledb_fragment_info_t* fragment_info, uint32_t* unconsolidated) {
  ensure_fragment_info_is_valid(fragment_info);
  ensure_output_pointer_is_valid(unconsolidated);
  *unconsolidated = fragment_info->unconsolidated_metadata_num();
  return TILEDB_OK;
}

capi_return_t tiledb_fragment_info_get_to_vacuum_num(
    tiledb_fragment_info_t* fragment_info, uint32_t* to_vacuum_num) {
  ensure_fragment_info_is_valid(fragment_info);
  ensure_output_pointer_is_valid(to_vacuum_num);
  *to_vacuum_num = fragment_info->to_vacuum_num();
  return TILEDB_OK;
}

capi_return_t tiledb_fragment_info_get_to_vacuum_uri(
    tiledb_fragment_info_t* fragment_info, uint32_t fid, const char** uri) {
  ensure_fragment_info_is_valid(fragment_info);
  ensure_output_pointer_is_valid(uri);
  throw_if_not_ok(fragment_info->get_to_vacuum_uri(fid, uri));
  return TILEDB_OK;
}

capi_return_t tiledb_fragment_info_get_array_schema(
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    tiledb_array_schema_t** array_schema) {
  ensure_fragment_info_is_valid(fragment_info);
  ensure_output_pointer_is_valid(array_schema);
  auto&& array_schema_get = fragment_info->get_array_schema(fid);
  *array_schema = tiledb_array_schema_t::make_handle(array_schema_get);
  return TILEDB_OK;
}

capi_return_t tiledb_fragment_info_get_array_schema_name(
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    const char** schema_name) {
  ensure_fragment_info_is_valid(fragment_info);
  ensure_output_pointer_is_valid(schema_name);
  throw_if_not_ok(fragment_info->get_array_schema_name(fid, schema_name));
  assert(schema_name != nullptr);
  return TILEDB_OK;
}

capi_return_t tiledb_fragment_info_dump(
    const tiledb_fragment_info_t* fragment_info, FILE* out) {
  ensure_fragment_info_is_valid(fragment_info);
  ensure_cstream_handle_is_valid(out);

  std::stringstream ss;
  ss << *fragment_info->fragment_info();
  size_t r = fwrite(ss.str().c_str(), sizeof(char), ss.str().size(), out);
  if (r != ss.str().size()) {
    throw CAPIException("Error writing fragment info to output stream");
  }

  return TILEDB_OK;
}

capi_return_t tiledb_fragment_info_dump_str(
    const tiledb_fragment_info_t* fragment_info, tiledb_string_t** out) {
  ensure_fragment_info_is_valid(fragment_info);
  ensure_output_pointer_is_valid(out);

  std::stringstream ss;
  ss << *fragment_info->fragment_info();
  *out = tiledb_string_handle_t::make_handle(ss.str());

  return TILEDB_OK;
}

}  // namespace tiledb::api

using tiledb::api::api_entry_context;
using tiledb::api::api_entry_plain;
using tiledb::api::api_entry_void;
using tiledb::api::api_entry_with_context;

CAPI_INTERFACE(
    fragment_info_alloc,
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_fragment_info_t** fragment_info) {
  return api_entry_with_context<tiledb::api::tiledb_fragment_info_alloc>(
      ctx, array_uri, fragment_info);
}

CAPI_INTERFACE_VOID(
    fragment_info_free, tiledb_fragment_info_t** fragment_info) {
  return api_entry_void<tiledb::api::tiledb_fragment_info_free>(fragment_info);
}

CAPI_INTERFACE(
    fragment_info_set_config,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    tiledb_config_t* config) {
  return api_entry_context<tiledb::api::tiledb_fragment_info_set_config>(
      ctx, fragment_info, config);
}

CAPI_INTERFACE(
    fragment_info_get_config,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    tiledb_config_t** config) {
  return api_entry_context<tiledb::api::tiledb_fragment_info_get_config>(
      ctx, fragment_info, config);
}

CAPI_INTERFACE(
    fragment_info_load,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info) {
  return api_entry_context<tiledb::api::tiledb_fragment_info_load>(
      ctx, fragment_info);
}

CAPI_INTERFACE(
    fragment_info_get_fragment_name_v2,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    tiledb_string_t** name) {
  return api_entry_context<
      tiledb::api::tiledb_fragment_info_get_fragment_name_v2>(
      ctx, fragment_info, fid, name);
}

CAPI_INTERFACE(
    fragment_info_get_fragment_num,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t* fragment_num) {
  return api_entry_context<tiledb::api::tiledb_fragment_info_get_fragment_num>(
      ctx, fragment_info, fragment_num);
}

CAPI_INTERFACE(
    fragment_info_get_fragment_uri,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    const char** uri) {
  return api_entry_context<tiledb::api::tiledb_fragment_info_get_fragment_uri>(
      ctx, fragment_info, fid, uri);
}

CAPI_INTERFACE(
    fragment_info_get_fragment_size,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint64_t* size) {
  return api_entry_context<tiledb::api::tiledb_fragment_info_get_fragment_size>(
      ctx, fragment_info, fid, size);
}

CAPI_INTERFACE(
    fragment_info_get_dense,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    int32_t* dense) {
  return api_entry_context<tiledb::api::tiledb_fragment_info_get_dense>(
      ctx, fragment_info, fid, dense);
}

CAPI_INTERFACE(
    fragment_info_get_sparse,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    int32_t* sparse) {
  return api_entry_context<tiledb::api::tiledb_fragment_info_get_sparse>(
      ctx, fragment_info, fid, sparse);
}

CAPI_INTERFACE(
    fragment_info_get_timestamp_range,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint64_t* start,
    uint64_t* end) {
  return api_entry_context<
      tiledb::api::tiledb_fragment_info_get_timestamp_range>(
      ctx, fragment_info, fid, start, end);
}

CAPI_INTERFACE(
    fragment_info_get_non_empty_domain_from_index,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t did,
    void* domain) {
  return api_entry_context<
      tiledb::api::tiledb_fragment_info_get_non_empty_domain_from_index>(
      ctx, fragment_info, fid, did, domain);
}

CAPI_INTERFACE(
    fragment_info_get_non_empty_domain_from_name,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    const char* dim_name,
    void* domain) {
  return api_entry_context<
      tiledb::api::tiledb_fragment_info_get_non_empty_domain_from_name>(
      ctx, fragment_info, fid, dim_name, domain);
}

CAPI_INTERFACE(
    fragment_info_get_non_empty_domain_var_size_from_index,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t did,
    uint64_t* start_size,
    uint64_t* end_size) {
  return api_entry_context<
      tiledb::api::
          tiledb_fragment_info_get_non_empty_domain_var_size_from_index>(
      ctx, fragment_info, fid, did, start_size, end_size);
}

CAPI_INTERFACE(
    fragment_info_get_non_empty_domain_var_size_from_name,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    const char* dim_name,
    uint64_t* start_size,
    uint64_t* end_size) {
  return api_entry_context<
      tiledb::api::
          tiledb_fragment_info_get_non_empty_domain_var_size_from_name>(
      ctx, fragment_info, fid, dim_name, start_size, end_size);
}

CAPI_INTERFACE(
    fragment_info_get_non_empty_domain_var_from_index,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t did,
    void* start,
    void* end) {
  return api_entry_context<
      tiledb::api::tiledb_fragment_info_get_non_empty_domain_var_from_index>(
      ctx, fragment_info, fid, did, start, end);
}

CAPI_INTERFACE(
    fragment_info_get_non_empty_domain_var_from_name,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    const char* dim_name,
    void* start,
    void* end) {
  return api_entry_context<
      tiledb::api::tiledb_fragment_info_get_non_empty_domain_var_from_name>(
      ctx, fragment_info, fid, dim_name, start, end);
}

CAPI_INTERFACE(
    fragment_info_get_mbr_num,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint64_t* mbr_num) {
  return api_entry_context<tiledb::api::tiledb_fragment_info_get_mbr_num>(
      ctx, fragment_info, fid, mbr_num);
}

CAPI_INTERFACE(
    fragment_info_get_mbr_from_index,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t mid,
    uint32_t did,
    void* mbr) {
  return api_entry_context<
      tiledb::api::tiledb_fragment_info_get_mbr_from_index>(
      ctx, fragment_info, fid, mid, did, mbr);
}

CAPI_INTERFACE(
    fragment_info_get_mbr_from_name,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t mid,
    const char* dim_name,
    void* mbr) {
  return api_entry_context<tiledb::api::tiledb_fragment_info_get_mbr_from_name>(
      ctx, fragment_info, fid, mid, dim_name, mbr);
}

CAPI_INTERFACE(
    fragment_info_get_mbr_var_size_from_index,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t mid,
    uint32_t did,
    uint64_t* start_size,
    uint64_t* end_size) {
  return api_entry_context<
      tiledb::api::tiledb_fragment_info_get_mbr_var_size_from_index>(
      ctx, fragment_info, fid, mid, did, start_size, end_size);
}

CAPI_INTERFACE(
    fragment_info_get_mbr_var_size_from_name,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t mid,
    const char* dim_name,
    uint64_t* start_size,
    uint64_t* end_size) {
  return api_entry_context<
      tiledb::api::tiledb_fragment_info_get_mbr_var_size_from_name>(
      ctx, fragment_info, fid, mid, dim_name, start_size, end_size);
}

CAPI_INTERFACE(
    fragment_info_get_mbr_var_from_index,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t mid,
    uint32_t did,
    void* start,
    void* end) {
  return api_entry_context<
      tiledb::api::tiledb_fragment_info_get_mbr_var_from_index>(
      ctx, fragment_info, fid, mid, did, start, end);
}

CAPI_INTERFACE(
    fragment_info_get_mbr_var_from_name,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t mid,
    const char* dim_name,
    void* start,
    void* end) {
  return api_entry_context<
      tiledb::api::tiledb_fragment_info_get_mbr_var_from_name>(
      ctx, fragment_info, fid, mid, dim_name, start, end);
}

CAPI_INTERFACE(
    fragment_info_get_cell_num,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint64_t* cell_num) {
  return api_entry_context<tiledb::api::tiledb_fragment_info_get_cell_num>(
      ctx, fragment_info, fid, cell_num);
}

CAPI_INTERFACE(
    fragment_info_get_total_cell_num,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint64_t* cell_num) {
  return api_entry_context<
      tiledb::api::tiledb_fragment_info_get_total_cell_num>(
      ctx, fragment_info, cell_num);
}

CAPI_INTERFACE(
    fragment_info_get_version,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t* version) {
  return api_entry_context<tiledb::api::tiledb_fragment_info_get_version>(
      ctx, fragment_info, fid, version);
}

CAPI_INTERFACE(
    fragment_info_has_consolidated_metadata,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    int32_t* has) {
  return api_entry_context<
      tiledb::api::tiledb_fragment_info_has_consolidated_metadata>(
      ctx, fragment_info, fid, has);
}

CAPI_INTERFACE(
    fragment_info_get_unconsolidated_metadata_num,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t* unconsolidated) {
  return api_entry_context<
      tiledb::api::tiledb_fragment_info_get_unconsolidated_metadata_num>(
      ctx, fragment_info, unconsolidated);
}

CAPI_INTERFACE(
    fragment_info_get_to_vacuum_num,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t* to_vacuum_num) {
  return api_entry_context<tiledb::api::tiledb_fragment_info_get_to_vacuum_num>(
      ctx, fragment_info, to_vacuum_num);
}

CAPI_INTERFACE(
    fragment_info_get_to_vacuum_uri,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    const char** uri) {
  return api_entry_context<tiledb::api::tiledb_fragment_info_get_to_vacuum_uri>(
      ctx, fragment_info, fid, uri);
}

CAPI_INTERFACE(
    fragment_info_get_array_schema,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    tiledb_array_schema_t** array_schema) {
  return api_entry_context<tiledb::api::tiledb_fragment_info_get_array_schema>(
      ctx, fragment_info, fid, array_schema);
}

CAPI_INTERFACE(
    fragment_info_get_array_schema_name,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    const char** schema_name) {
  return api_entry_context<
      tiledb::api::tiledb_fragment_info_get_array_schema_name>(
      ctx, fragment_info, fid, schema_name);
}

CAPI_INTERFACE(
    fragment_info_dump,
    tiledb_ctx_t* ctx,
    const tiledb_fragment_info_t* fragment_info,
    FILE* out) {
  return api_entry_context<tiledb::api::tiledb_fragment_info_dump>(
      ctx, fragment_info, out);
}

CAPI_INTERFACE(
    fragment_info_dump_str,
    tiledb_ctx_t* ctx,
    const tiledb_fragment_info_t* fragment_info,
    tiledb_string_t** out) {
  return api_entry_context<tiledb::api::tiledb_fragment_info_dump_str>(
      ctx, fragment_info, out);
}
