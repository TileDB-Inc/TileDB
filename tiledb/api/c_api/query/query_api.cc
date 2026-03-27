/**
 * @file tiledb/api/c_api/query/query_api.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2024 TileDB, Inc.
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
 * This file defines the query section of the C API for TileDB.
 */

#include "query_api_external_experimental.h"
#include "query_api_internal.h"
#include "tiledb/api/c_api/array/array_api_internal.h"
#include "tiledb/api/c_api/config/config_api_internal.h"
#include "tiledb/api/c_api/query_condition/query_condition_api_internal.h"
#include "tiledb/api/c_api/subarray/subarray_api_internal.h"
#include "tiledb/api/c_api_support/c_api_support.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/enums/query_type.h"

namespace tiledb::api {

int32_t tiledb_query_type_to_str(
    tiledb_query_type_t query_type, const char** str) {
  const auto& strval =
      tiledb::sm::query_type_str((tiledb::sm::QueryType)query_type);
  *str = strval.c_str();
  return strval.empty() ? TILEDB_ERR : TILEDB_OK;
}

int32_t tiledb_query_type_from_str(
    const char* str, tiledb_query_type_t* query_type) {
  tiledb::sm::QueryType val = tiledb::sm::QueryType::READ;
  if (!tiledb::sm::query_type_enum(str, &val).ok())
    return TILEDB_ERR;
  *query_type = (tiledb_query_type_t)val;
  return TILEDB_OK;
}

capi_return_t tiledb_query_alloc(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_query_type_t query_type,
    tiledb_query_t** query) {
  ensure_array_is_valid(array);
  ensure_output_pointer_is_valid(query);

  if (!array->is_open()) {
    throw CAPIStatusException("Cannot create query; Input array is not open");
  }

  tiledb::sm::QueryType array_query_type = array->get_query_type();
  if (query_type != static_cast<tiledb_query_type_t>(array_query_type)) {
    throw CAPIStatusException(
        "Cannot create query; Array query type does not match declared query "
        "type: (" +
        std::string(tiledb::sm::query_type_str(array_query_type)) + " != " +
        std::string(tiledb::sm::query_type_str(
            static_cast<tiledb::sm::QueryType>(query_type))) +
        ")");
  }

  *query = make_handle<tiledb_query_handle_t>(
      ctx->resources(),
      ctx->cancellation_source(),
      ctx->storage_manager(),
      array->array());
  return TILEDB_OK;
}

capi_return_t tiledb_query_get_stats(tiledb_query_t* query, char** stats_json) {
  ensure_query_is_valid(query);

  if (stats_json == nullptr) {
    throw CAPIStatusException("Pointer to stats_json may not be NULL");
  }

  const std::string str = query->query()->stats()->dump(2, 0);

  *stats_json = static_cast<char*>(std::malloc(str.size() + 1));
  if (*stats_json == nullptr) {
    throw CAPIStatusException(
        "Failed to allocate stats string; Memory allocation failed");
  }

  std::memcpy(*stats_json, str.data(), str.size());
  (*stats_json)[str.size()] = '\0';

  return TILEDB_OK;
}

capi_return_t tiledb_query_set_config(
    tiledb_query_t* query, tiledb_config_t* config) {
  ensure_query_is_valid(query);
  ensure_config_is_valid(config);
  query->query()->set_config(config->config());
  return TILEDB_OK;
}

capi_return_t tiledb_query_get_config(
    tiledb_query_t* query, tiledb_config_t** config) {
  ensure_query_is_valid(query);
  ensure_output_pointer_is_valid(config);
  *config = make_handle<tiledb_config_handle_t>(query->query()->config());
  return TILEDB_OK;
}

capi_return_t tiledb_query_set_subarray_t(
    tiledb_query_t* query, const tiledb_subarray_t* subarray) {
  ensure_query_is_valid(query);
  ensure_subarray_is_valid(subarray);
  query->query()->set_subarray(*subarray->subarray());
  return TILEDB_OK;
}

capi_return_t tiledb_query_set_data_buffer(
    tiledb_query_t* query,
    const char* name,
    void* buffer,
    uint64_t* buffer_size) {
  ensure_query_is_valid(query);
  query->query()->set_data_buffer(name, buffer, buffer_size);
  return TILEDB_OK;
}

capi_return_t tiledb_query_set_offsets_buffer(
    tiledb_query_t* query,
    const char* name,
    uint64_t* buffer_offsets,
    uint64_t* buffer_offsets_size) {
  ensure_query_is_valid(query);
  query->query()->set_offsets_buffer(name, buffer_offsets, buffer_offsets_size);
  return TILEDB_OK;
}

capi_return_t tiledb_query_set_validity_buffer(
    tiledb_query_t* query,
    const char* name,
    uint8_t* buffer_validity,
    uint64_t* buffer_validity_size) {
  ensure_query_is_valid(query);
  query->query()->set_validity_buffer(
      name, buffer_validity, buffer_validity_size);
  return TILEDB_OK;
}

capi_return_t tiledb_query_get_data_buffer(
    tiledb_query_t* query,
    const char* name,
    void** buffer,
    uint64_t** buffer_size) {
  ensure_query_is_valid(query);
  std::tie(*buffer, *buffer_size) = query->query()->get_data_buffer(name);
  return TILEDB_OK;
}

capi_return_t tiledb_query_get_offsets_buffer(
    tiledb_query_t* query,
    const char* name,
    uint64_t** buffer,
    uint64_t** buffer_size) {
  ensure_query_is_valid(query);
  std::tie(*buffer, *buffer_size) = query->query()->get_offsets_buffer(name);
  return TILEDB_OK;
}

capi_return_t tiledb_query_get_validity_buffer(
    tiledb_query_t* query,
    const char* name,
    uint8_t** buffer,
    uint64_t** buffer_size) {
  ensure_query_is_valid(query);
  std::tie(*buffer, *buffer_size) = query->query()->get_validity_buffer(name);
  return TILEDB_OK;
}

capi_return_t tiledb_query_set_layout(
    tiledb_query_t* query, tiledb_layout_t layout) {
  ensure_query_is_valid(query);
  query->query()->set_layout(static_cast<tiledb::sm::Layout>(layout));
  return TILEDB_OK;
}

capi_return_t tiledb_query_set_condition(
    tiledb_query_t* query, const tiledb_query_condition_t* cond) {
  ensure_query_is_valid(query);
  ensure_handle_is_valid(cond);
  query->query()->set_condition(*cond->query_condition());
  return TILEDB_OK;
}

capi_return_t tiledb_query_finalize(tiledb_query_t* query) {
  if (query == nullptr) {
    return TILEDB_OK;
  }
  ensure_query_is_valid(query);
  query->query()->finalize();
  return TILEDB_OK;
}

capi_return_t tiledb_query_submit_and_finalize(tiledb_query_t* query) {
  if (query == nullptr) {
    return TILEDB_OK;
  }
  ensure_query_is_valid(query);
  query->query()->submit_and_finalize();
  return TILEDB_OK;
}

void tiledb_query_free(tiledb_query_t** query) {
  ensure_output_pointer_is_valid(query);
  ensure_handle_is_valid(*query);
  break_handle(*query);
}

capi_return_t tiledb_query_submit(tiledb_query_t* query) {
  ensure_query_is_valid(query);
  query->query()->submit();
  return TILEDB_OK;
}

capi_return_t tiledb_query_has_results(
    tiledb_query_t* query, int32_t* has_results) {
  ensure_query_is_valid(query);
  *has_results = query->query()->has_results();
  return TILEDB_OK;
}

capi_return_t tiledb_query_get_status(
    tiledb_query_t* query, tiledb_query_status_t* status) {
  ensure_query_is_valid(query);
  *status = (tiledb_query_status_t)query->query()->status();
  return TILEDB_OK;
}

capi_return_t tiledb_query_get_type(
    tiledb_query_t* query, tiledb_query_type_t* query_type) {
  ensure_query_is_valid(query);
  *query_type = static_cast<tiledb_query_type_t>(query->query()->type());
  return TILEDB_OK;
}

capi_return_t tiledb_query_get_layout(
    tiledb_query_t* query, tiledb_layout_t* query_layout) {
  ensure_query_is_valid(query);
  *query_layout = static_cast<tiledb_layout_t>(query->query()->layout());
  return TILEDB_OK;
}

capi_return_t tiledb_query_get_array(
    tiledb_query_t* query, tiledb_array_t** array) {
  ensure_query_is_valid(query);
  ensure_output_pointer_is_valid(array);
  *array = make_handle<tiledb_array_t>(query->query()->array_shared());
  return TILEDB_OK;
}

capi_return_t tiledb_query_get_est_result_size(
    const tiledb_query_t* query, const char* name, uint64_t* size) {
  ensure_query_is_valid(query);
  auto field_name{to_string_view<"field name">(name)};
  if (size == nullptr) {
    throw CAPIStatusException("Pointer to size may not be NULL");
  }
  auto est_size{query->query()->get_est_result_size_fixed_nonnull(field_name)};
  *size = est_size.fixed_;
  return TILEDB_OK;
}

capi_return_t tiledb_query_get_est_result_size_var(
    const tiledb_query_t* query,
    const char* name,
    uint64_t* size_off,
    uint64_t* size_val) {
  ensure_query_is_valid(query);
  auto field_name{to_string_view<"field name">(name)};
  if (size_off == nullptr) {
    throw CAPIStatusException("Pointer to offset size may not be NULL");
  }
  if (size_val == nullptr) {
    throw CAPIStatusException("Pointer to value size may not be NULL");
  }
  auto est_size{
      query->query()->get_est_result_size_variable_nonnull(field_name)};
  *size_off = est_size.fixed_;
  *size_val = est_size.variable_;
  return TILEDB_OK;
}

capi_return_t tiledb_query_get_est_result_size_nullable(
    const tiledb_query_t* query,
    const char* name,
    uint64_t* size_val,
    uint64_t* size_validity) {
  ensure_query_is_valid(query);
  auto field_name{to_string_view<"field name">(name)};
  if (size_val == nullptr) {
    throw CAPIStatusException("Pointer to value size may not be NULL");
  }
  if (size_validity == nullptr) {
    throw CAPIStatusException("Pointer to validity size may not be NULL");
  }
  auto est_size{query->query()->get_est_result_size_fixed_nullable(field_name)};
  *size_val = est_size.fixed_;
  *size_validity = est_size.validity_;
  return TILEDB_OK;
}

capi_return_t tiledb_query_get_est_result_size_var_nullable(
    const tiledb_query_t* query,
    const char* name,
    uint64_t* size_off,
    uint64_t* size_val,
    uint64_t* size_validity) {
  ensure_query_is_valid(query);
  auto field_name{to_string_view<"field name">(name)};
  if (size_off == nullptr) {
    throw CAPIStatusException("Pointer to offset size may not be NULL");
  }
  if (size_val == nullptr) {
    throw CAPIStatusException("Pointer to value size may not be NULL");
  }
  if (size_validity == nullptr) {
    throw CAPIStatusException("Pointer to validity size may not be NULL");
  }
  auto est_size{
      query->query()->get_est_result_size_variable_nullable(field_name)};
  *size_off = est_size.fixed_;
  *size_val = est_size.variable_;
  *size_validity = est_size.validity_;
  return TILEDB_OK;
}

capi_return_t tiledb_query_get_fragment_num(
    const tiledb_query_t* query, uint32_t* num) {
  ensure_query_is_valid(query);
  *num = query->query()->get_written_fragment_num();
  return TILEDB_OK;
}

capi_return_t tiledb_query_get_fragment_uri(
    const tiledb_query_t* query, uint64_t idx, const char** uri) {
  ensure_query_is_valid(query);
  *uri = query->query()->get_written_fragment_uri(idx).c_str();
  return TILEDB_OK;
}

capi_return_t tiledb_query_get_fragment_timestamp_range(
    const tiledb_query_t* query, uint64_t idx, uint64_t* t1, uint64_t* t2) {
  ensure_query_is_valid(query);
  std::tie(*t1, *t2) =
      query->query()->get_written_fragment_timestamp_range(idx);
  return TILEDB_OK;
}

capi_return_t tiledb_query_get_subarray_t(
    const tiledb_query_t* query, tiledb_subarray_t** subarray) {
  ensure_query_is_valid(query);
  ensure_output_pointer_is_valid(subarray);
  *subarray = make_handle<tiledb_subarray_t>(*(query->query()->subarray()));
  return TILEDB_OK;
}

int32_t tiledb_query_get_relevant_fragment_num(
    const tiledb_query_t* query, uint64_t* relevant_fragment_num) {
  ensure_query_is_valid(query);
  *relevant_fragment_num = query->query()
                               ->subarray()
                               ->relevant_fragments()
                               .relevant_fragments_size();

  return TILEDB_OK;
}

int32_t tiledb_query_add_update_value(
    tiledb_query_t* query,
    const char* field_name,
    const void* update_value,
    uint64_t update_value_size) {
  ensure_query_is_valid(query);
  query->query()->add_update_value(field_name, update_value, update_value_size);

  // Success
  return TILEDB_OK;
}

}  // namespace tiledb::api

using tiledb::api::api_entry_plain;
using tiledb::api::api_entry_void;
using tiledb::api::api_entry_with_context;
template <auto f>
constexpr auto api_entry = tiledb::api::api_entry_context<f>;

CAPI_INTERFACE(
    query_type_to_str, tiledb_query_type_t query_type, const char** str) {
  return api_entry_plain<tiledb::api::tiledb_query_type_to_str>(
      query_type, str);
}

CAPI_INTERFACE(
    query_type_from_str, const char* str, tiledb_query_type_t* query_type) {
  return api_entry_plain<tiledb::api::tiledb_query_type_from_str>(
      str, query_type);
}

CAPI_INTERFACE(
    query_alloc,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_query_type_t query_type,
    tiledb_query_t** query) {
  return api_entry_with_context<tiledb::api::tiledb_query_alloc>(
      ctx, array, query_type, query);
}

CAPI_INTERFACE(
    query_get_stats,
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    char** stats_json) {
  return api_entry<tiledb::api::tiledb_query_get_stats>(ctx, query, stats_json);
}

CAPI_INTERFACE(
    query_set_config,
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    tiledb_config_t* config) {
  return api_entry<tiledb::api::tiledb_query_set_config>(ctx, query, config);
}

CAPI_INTERFACE(
    query_get_config,
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    tiledb_config_t** config) {
  return api_entry<tiledb::api::tiledb_query_get_config>(ctx, query, config);
}

CAPI_INTERFACE(
    query_set_subarray_t,
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const tiledb_subarray_t* subarray) {
  return api_entry<tiledb::api::tiledb_query_set_subarray_t>(
      ctx, query, subarray);
}

CAPI_INTERFACE(
    query_set_data_buffer,
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* name,
    void* buffer,
    uint64_t* buffer_size) {
  return api_entry<tiledb::api::tiledb_query_set_data_buffer>(
      ctx, query, name, buffer, buffer_size);
}

CAPI_INTERFACE(
    query_set_offsets_buffer,
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* name,
    uint64_t* buffer,
    uint64_t* buffer_size) {
  return api_entry<tiledb::api::tiledb_query_set_offsets_buffer>(
      ctx, query, name, buffer, buffer_size);
}

CAPI_INTERFACE(
    query_set_validity_buffer,
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* name,
    uint8_t* buffer,
    uint64_t* buffer_size) {
  return api_entry<tiledb::api::tiledb_query_set_validity_buffer>(
      ctx, query, name, buffer, buffer_size);
}

CAPI_INTERFACE(
    query_get_data_buffer,
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* name,
    void** buffer,
    uint64_t** buffer_size) {
  return api_entry<tiledb::api::tiledb_query_get_data_buffer>(
      ctx, query, name, buffer, buffer_size);
}

CAPI_INTERFACE(
    query_get_offsets_buffer,
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* name,
    uint64_t** buffer,
    uint64_t** buffer_size) {
  return api_entry<tiledb::api::tiledb_query_get_offsets_buffer>(
      ctx, query, name, buffer, buffer_size);
}

CAPI_INTERFACE(
    query_get_validity_buffer,
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* name,
    uint8_t** buffer,
    uint64_t** buffer_size) {
  return api_entry<tiledb::api::tiledb_query_get_validity_buffer>(
      ctx, query, name, buffer, buffer_size);
}

CAPI_INTERFACE(
    query_set_layout,
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    tiledb_layout_t layout) {
  return api_entry<tiledb::api::tiledb_query_set_layout>(ctx, query, layout);
}

CAPI_INTERFACE(
    query_set_condition,
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const tiledb_query_condition_t* cond) {
  return api_entry<tiledb::api::tiledb_query_set_condition>(ctx, query, cond);
}

CAPI_INTERFACE(query_finalize, tiledb_ctx_t* ctx, tiledb_query_t* query) {
  return api_entry<tiledb::api::tiledb_query_finalize>(ctx, query);
}

CAPI_INTERFACE(
    query_submit_and_finalize, tiledb_ctx_t* ctx, tiledb_query_t* query) {
  return api_entry<tiledb::api::tiledb_query_submit_and_finalize>(ctx, query);
}

CAPI_INTERFACE_VOID(query_free, tiledb_query_t** query) {
  return api_entry_void<tiledb::api::tiledb_query_free>(query);
}

CAPI_INTERFACE(query_submit, tiledb_ctx_t* ctx, tiledb_query_t* query) {
  return api_entry<tiledb::api::tiledb_query_submit>(ctx, query);
}

CAPI_INTERFACE(
    query_has_results,
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    int32_t* has_results) {
  return api_entry<tiledb::api::tiledb_query_has_results>(
      ctx, query, has_results);
}

CAPI_INTERFACE(
    query_get_status,
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    tiledb_query_status_t* status) {
  return api_entry<tiledb::api::tiledb_query_get_status>(ctx, query, status);
}

CAPI_INTERFACE(
    query_get_type,
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    tiledb_query_type_t* query_type) {
  return api_entry<tiledb::api::tiledb_query_get_type>(ctx, query, query_type);
}

CAPI_INTERFACE(
    query_get_layout,
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    tiledb_layout_t* query_layout) {
  return api_entry<tiledb::api::tiledb_query_get_layout>(
      ctx, query, query_layout);
}

CAPI_INTERFACE(
    query_get_array,
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    tiledb_array_t** array) {
  return api_entry<tiledb::api::tiledb_query_get_array>(ctx, query, array);
}

CAPI_INTERFACE(
    query_get_est_result_size,
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    const char* name,
    uint64_t* size) {
  return api_entry<tiledb::api::tiledb_query_get_est_result_size>(
      ctx, query, name, size);
}

CAPI_INTERFACE(
    query_get_est_result_size_var,
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    const char* name,
    uint64_t* size_off,
    uint64_t* size_val) {
  return api_entry<tiledb::api::tiledb_query_get_est_result_size_var>(
      ctx, query, name, size_off, size_val);
}

CAPI_INTERFACE(
    query_get_est_result_size_nullable,
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    const char* name,
    uint64_t* size_val,
    uint64_t* size_validity) {
  return api_entry<tiledb::api::tiledb_query_get_est_result_size_nullable>(
      ctx, query, name, size_val, size_validity);
}

CAPI_INTERFACE(
    query_get_est_result_size_var_nullable,
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    const char* name,
    uint64_t* size_off,
    uint64_t* size_val,
    uint64_t* size_validity) {
  return api_entry<tiledb::api::tiledb_query_get_est_result_size_var_nullable>(
      ctx, query, name, size_off, size_val, size_validity);
}

CAPI_INTERFACE(
    query_get_fragment_num,
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    uint32_t* num) {
  return api_entry<tiledb::api::tiledb_query_get_fragment_num>(ctx, query, num);
}

CAPI_INTERFACE(
    query_get_fragment_uri,
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    uint64_t idx,
    const char** uri) {
  return api_entry<tiledb::api::tiledb_query_get_fragment_uri>(
      ctx, query, idx, uri);
}

CAPI_INTERFACE(
    query_get_fragment_timestamp_range,
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    uint64_t idx,
    uint64_t* t1,
    uint64_t* t2) {
  return api_entry<tiledb::api::tiledb_query_get_fragment_timestamp_range>(
      ctx, query, idx, t1, t2);
}

CAPI_INTERFACE(
    query_get_subarray_t,
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    tiledb_subarray_t** subarray) {
  return api_entry<tiledb::api::tiledb_query_get_subarray_t>(
      ctx, query, subarray);
}

CAPI_INTERFACE(
    query_get_relevant_fragment_num,
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    uint64_t* relevant_fragment_num) {
  return api_entry<tiledb::api::tiledb_query_get_relevant_fragment_num>(
      ctx, query, relevant_fragment_num);
}

CAPI_INTERFACE(
    query_add_update_value,
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* field_name,
    const void* update_value,
    uint64_t update_value_size) {
  return api_entry<tiledb::api::tiledb_query_add_update_value>(
      ctx, query, field_name, update_value, update_value_size);
}
