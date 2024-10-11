/**
 * @file tiledb/api/c_api/array/array_api.cc
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
 * This file defines C API functions for the array section.
 */

#include "array_api_experimental.h"
#include "array_api_internal.h"

#include "tiledb/api/c_api/array_schema/array_schema_api_internal.h"
#include "tiledb/api/c_api/array_schema_evolution/array_schema_evolution_api_internal.h"
#include "tiledb/api/c_api/context/context_api_internal.h"
#include "tiledb/api/c_api/enumeration/enumeration_api_internal.h"
#include "tiledb/api/c_api/query/query_api_external.h"
#include "tiledb/api/c_api_support/c_api_support.h"
#include "tiledb/sm/array/array_directory.h"
#include "tiledb/sm/array_schema/array_schema_operations.h"
#include "tiledb/sm/array_schema/dimension_label.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/rest/rest_client.h"

using namespace tiledb::sm;

namespace tiledb::api {

capi_return_t tiledb_encryption_type_to_str(
    tiledb_encryption_type_t encryption_type, const char** str) {
  const auto& strval = tiledb::sm::encryption_type_str(
      (tiledb::sm::EncryptionType)encryption_type);
  *str = strval.c_str();
  return strval.empty() ? TILEDB_ERR : TILEDB_OK;
}

capi_return_t tiledb_encryption_type_from_str(
    const char* str, tiledb_encryption_type_t* encryption_type) {
  auto [st, et] = tiledb::sm::encryption_type_enum(str);
  if (!st.ok()) {
    return TILEDB_ERR;
  }
  *encryption_type = (tiledb_encryption_type_t)et.value();
  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_load(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_array_schema_t** array_schema) {
  ensure_output_pointer_is_valid(array_schema);

  // Use a default constructed config to load the schema with default options.
  *array_schema = tiledb_array_schema_t::make_handle(
      load_array_schema(ctx->context(), sm::URI(array_uri), sm::Config()));

  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_load_with_config(
    tiledb_ctx_t* ctx,
    tiledb_config_t* config,
    const char* array_uri,
    tiledb_array_schema_t** array_schema) {
  ensure_config_is_valid_if_present(config);
  ensure_output_pointer_is_valid(array_schema);

  // Use passed config or context config to load the schema with set options.
  *array_schema = tiledb_array_schema_t::make_handle(load_array_schema(
      ctx->context(),
      sm::URI(array_uri),
      config ? config->config() : ctx->config()));

  return TILEDB_OK;
}

capi_return_t tiledb_array_alloc(
    tiledb_ctx_t* ctx, const char* array_uri, tiledb_array_t** array) {
  ensure_output_pointer_is_valid(array);

  // Create Array object
  *array = tiledb_array_t::make_handle(
      ctx->resources(), URI(array_uri, URI::must_be_valid));
  return TILEDB_OK;
}

void tiledb_array_free(tiledb_array_t** array) {
  ensure_output_pointer_is_valid(array);
  ensure_array_is_valid(*array);
  tiledb_array_t::break_handle(*array);
}

capi_return_t tiledb_array_create(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    const tiledb_array_schema_t* array_schema) {
  ensure_array_schema_is_valid(array_schema);

  tiledb::sm::URI uri(array_uri, URI::must_be_valid);
  if (uri.is_tiledb()) {
    auto& rest_client = ctx->context().rest_client();
    throw_if_not_ok(rest_client.post_array_schema_to_rest(
        uri, *(array_schema->array_schema())));
  } else {
    // Create key
    tiledb::sm::EncryptionKey key;
    throw_if_not_ok(
        key.set_key(tiledb::sm::EncryptionType::NO_ENCRYPTION, nullptr, 0));
    // Create the array
    tiledb::sm::Array::create(
        ctx->resources(), uri, array_schema->array_schema(), key);

    // Create any dimension labels in the array.
    for (tiledb::sm::ArraySchema::dimension_label_size_type ilabel{0};
         ilabel < array_schema->dim_label_num();
         ++ilabel) {
      // Get dimension label information and define URI and name.
      const auto& dim_label_ref = array_schema->dimension_label(ilabel);
      if (dim_label_ref.is_external())
        continue;
      if (!dim_label_ref.has_schema()) {
        throw CAPIException(
            "Failed to create array. Dimension labels that are "
            "not external must have a schema.");
      }

      // Create the dimension label array with the same key.
      tiledb::sm::Array::create(
          ctx->resources(),
          dim_label_ref.uri(uri),
          dim_label_ref.schema(),
          key);
    }
  }
  return TILEDB_OK;
}

capi_return_t tiledb_array_open(
    tiledb_array_t* array, tiledb_query_type_t query_type) {
  ensure_array_is_valid(array);

  // Open array
  throw_if_not_ok(array->open(
      static_cast<tiledb::sm::QueryType>(query_type),
      tiledb::sm::EncryptionType::NO_ENCRYPTION,
      nullptr,
      0));

  return TILEDB_OK;
}

capi_return_t tiledb_array_is_open(tiledb_array_t* array, int32_t* is_open) {
  ensure_array_is_valid(array);
  ensure_output_pointer_is_valid(is_open);
  *is_open = (int32_t)array->is_open();
  return TILEDB_OK;
}

capi_return_t tiledb_array_close(tiledb_array_t* array) {
  ensure_array_is_valid(array);
  throw_if_not_ok(array->close());
  return TILEDB_OK;
}

capi_return_t tiledb_array_reopen(tiledb_array_t* array) {
  ensure_array_is_valid(array);
  throw_if_not_ok(array->reopen());
  return TILEDB_OK;
}

capi_return_t tiledb_array_delete(tiledb_ctx_t* ctx, const char* array_uri) {
  // Create Array object
  auto uri = tiledb::sm::URI(array_uri, URI::must_be_valid);
  tiledb_array_t* array = tiledb_array_t::make_handle(ctx->resources(), uri);

  // Open the array for exclusive modification
  try {
    throw_if_not_ok(array->open(
        QueryType::MODIFY_EXCLUSIVE,
        EncryptionType::NO_ENCRYPTION,
        nullptr,
        0));
  } catch (...) {
    tiledb_array_t::break_handle(array);
    throw;
  }

  // Delete the array
  try {
    array->delete_array(uri);
  } catch (...) {
    throw_if_not_ok(array->close());
    tiledb_array_t::break_handle(array);
    throw;
  }
  tiledb_array_t::break_handle(array);

  return TILEDB_OK;
}

capi_return_t tiledb_array_delete_fragments_v2(
    tiledb_ctx_t* ctx,
    const char* uri_str,
    uint64_t timestamp_start,
    uint64_t timestamp_end) {
  // Allocate an array object
  auto uri = tiledb::sm::URI(uri_str, URI::must_be_valid);
  tiledb_array_t* array = tiledb_array_t::make_handle(ctx->resources(), uri);

  // Set array open timestamps and open the array for exclusive modification
  try {
    array->set_timestamp_start(timestamp_start);
    array->set_timestamp_end(timestamp_end);
    throw_if_not_ok(array->open(
        static_cast<QueryType>(TILEDB_MODIFY_EXCLUSIVE),
        static_cast<EncryptionType>(TILEDB_NO_ENCRYPTION),
        nullptr,
        0));
  } catch (...) {
    tiledb_array_t::break_handle(array);
    throw;
  }

  // Delete fragments
  try {
    array->delete_fragments(uri, timestamp_start, timestamp_end);
  } catch (...) {
    throw_if_not_ok(array->close());
    tiledb_array_t::break_handle(array);
    throw;
  }

  // Close and delete the array
  throw_if_not_ok(array->close());
  tiledb_array_t::break_handle(array);

  return TILEDB_OK;
}

capi_return_t tiledb_array_delete_fragments_list(
    tiledb_ctx_t* ctx,
    const char* uri_str,
    const char* fragment_uris[],
    const size_t num_fragments) {
  if (num_fragments < 1) {
    throw CAPIException(
        "Failed to delete fragments list; Invalid input number of fragments");
  }

  for (size_t i = 0; i < num_fragments; i++) {
    if (URI(fragment_uris[i]).is_invalid()) {
      throw CAPIException(
          "Failed to delete fragments list; Invalid input fragment uri");
    }
  }

  // Convert the list of fragment uris to a vector
  std::vector<URI> uris;
  uris.reserve(num_fragments);
  for (size_t i = 0; i < num_fragments; i++) {
    uris.emplace_back(URI(fragment_uris[i]));
  }

  // Allocate an array object
  tiledb_array_t* array = tiledb_array_t::make_handle(
      ctx->resources(), URI(uri_str, URI::must_be_valid));

  // Open the array for exclusive modification
  try {
    throw_if_not_ok(array->open(
        static_cast<QueryType>(TILEDB_MODIFY_EXCLUSIVE),
        static_cast<EncryptionType>(TILEDB_NO_ENCRYPTION),
        nullptr,
        0));
  } catch (...) {
    tiledb_array_t::break_handle(array);
    throw;
  }

  // Delete fragments list
  try {
    array->delete_fragments_list(uris);
  } catch (...) {
    throw_if_not_ok(array->close());
    tiledb_array_t::break_handle(array);
    throw CAPIException("Failed to delete fragments list");
  }

  // Close the array
  throw_if_not_ok(array->close());
  tiledb_array_t::break_handle(array);

  return TILEDB_OK;
}

capi_return_t tiledb_array_set_config(
    tiledb_array_t* array, tiledb_config_t* config) {
  ensure_array_is_valid(array);
  ensure_config_is_valid(config);
  array->set_config(config->config());
  return TILEDB_OK;
}

capi_return_t tiledb_array_set_open_timestamp_start(
    tiledb_array_t* array, uint64_t timestamp_start) {
  ensure_array_is_valid(array);
  array->set_timestamp_start(timestamp_start);
  return TILEDB_OK;
}

capi_return_t tiledb_array_set_open_timestamp_end(
    tiledb_array_t* array, uint64_t timestamp_end) {
  ensure_array_is_valid(array);
  array->set_timestamp_end(timestamp_end);
  return TILEDB_OK;
}

capi_return_t tiledb_array_get_config(
    tiledb_array_t* array, tiledb_config_t** config) {
  ensure_array_is_valid(array);
  ensure_output_pointer_is_valid(config);
  *config = tiledb_config_handle_t::make_handle(array->config());
  return TILEDB_OK;
}

capi_return_t tiledb_array_get_open_timestamp_start(
    tiledb_array_t* array, uint64_t* timestamp_start) {
  ensure_array_is_valid(array);
  ensure_output_pointer_is_valid(timestamp_start);
  *timestamp_start = array->timestamp_start();
  return TILEDB_OK;
}

capi_return_t tiledb_array_get_open_timestamp_end(
    tiledb_array_t* array, uint64_t* timestamp_end) {
  ensure_array_is_valid(array);
  ensure_output_pointer_is_valid(timestamp_end);
  *timestamp_end = array->timestamp_end_opened_at();
  return TILEDB_OK;
}

capi_return_t tiledb_array_get_schema(
    tiledb_array_t* array, tiledb_array_schema_t** array_schema) {
  ensure_array_is_valid(array);
  ensure_output_pointer_is_valid(array_schema);

  // Get schema
  auto&& [st, array_schema_get] = array->get_array_schema();
  throw_if_not_ok(st);
  *array_schema = tiledb_array_schema_t::make_handle(array_schema_get.value());

  return TILEDB_OK;
}

capi_return_t tiledb_array_get_query_type(
    tiledb_array_t* array, tiledb_query_type_t* query_type) {
  ensure_array_is_valid(array);
  ensure_output_pointer_is_valid(query_type);
  *query_type = static_cast<tiledb_query_type_t>(array->get_query_type());
  return TILEDB_OK;
}

capi_return_t tiledb_array_get_uri(
    tiledb_array_t* array, const char** array_uri) {
  ensure_array_is_valid(array);
  ensure_output_pointer_is_valid(array_uri);
  *array_uri = array->array_uri().c_str();
  return TILEDB_OK;
}

capi_return_t tiledb_array_upgrade_version(
    tiledb_ctx_t* ctx, const char* array_uri, tiledb_config_t* config) {
  ensure_config_is_valid_if_present(config);
  tiledb::sm::Array::upgrade_version(
      ctx->resources(),
      URI(array_uri, URI::must_be_valid),
      (config == nullptr) ? ctx->config() : config->config());
  return TILEDB_OK;
}

capi_return_t tiledb_array_get_non_empty_domain(
    tiledb_array_t* array, void* domain, int32_t* is_empty) {
  ensure_array_is_valid(array);
  ensure_output_pointer_is_valid(domain);
  ensure_output_pointer_is_valid(is_empty);

  bool is_empty_b;
  array->non_empty_domain(domain, &is_empty_b);
  *is_empty = (int32_t)is_empty_b;

  return TILEDB_OK;
}

capi_return_t tiledb_array_get_non_empty_domain_from_index(
    tiledb_array_t* array, uint32_t idx, void* domain, int32_t* is_empty) {
  ensure_array_is_valid(array);
  ensure_output_pointer_is_valid(domain);
  ensure_output_pointer_is_valid(is_empty);

  bool is_empty_b;
  array->non_empty_domain_from_index(idx, domain, &is_empty_b);
  *is_empty = (int32_t)is_empty_b;

  return TILEDB_OK;
}

capi_return_t tiledb_array_get_non_empty_domain_from_name(
    tiledb_array_t* array, const char* name, void* domain, int32_t* is_empty) {
  ensure_array_is_valid(array);
  auto field_name{to_string_view<"field name">(name)};
  ensure_output_pointer_is_valid(domain);
  ensure_output_pointer_is_valid(is_empty);

  bool is_empty_b;
  array->non_empty_domain_from_name(field_name, domain, &is_empty_b);
  *is_empty = (int32_t)is_empty_b;

  return TILEDB_OK;
}

capi_return_t tiledb_array_get_non_empty_domain_var_size_from_index(
    tiledb_array_t* array,
    uint32_t idx,
    uint64_t* start_size,
    uint64_t* end_size,
    int32_t* is_empty) {
  ensure_array_is_valid(array);
  ensure_output_pointer_is_valid(start_size);
  ensure_output_pointer_is_valid(end_size);
  ensure_output_pointer_is_valid(is_empty);

  bool is_empty_b = true;
  array->non_empty_domain_var_size_from_index(
      idx, start_size, end_size, &is_empty_b);
  *is_empty = (int32_t)is_empty_b;

  return TILEDB_OK;
}

capi_return_t tiledb_array_get_non_empty_domain_var_size_from_name(
    tiledb_array_t* array,
    const char* name,
    uint64_t* start_size,
    uint64_t* end_size,
    int32_t* is_empty) {
  ensure_array_is_valid(array);
  ensure_output_pointer_is_valid(start_size);
  ensure_output_pointer_is_valid(end_size);
  ensure_output_pointer_is_valid(is_empty);

  bool is_empty_b = true;
  array->non_empty_domain_var_size_from_name(
      name, start_size, end_size, &is_empty_b);
  *is_empty = (int32_t)is_empty_b;

  return TILEDB_OK;
}

capi_return_t tiledb_array_get_non_empty_domain_var_from_index(
    tiledb_array_t* array,
    uint32_t idx,
    void* start,
    void* end,
    int32_t* is_empty) {
  ensure_array_is_valid(array);
  ensure_output_pointer_is_valid(start);
  ensure_output_pointer_is_valid(end);
  ensure_output_pointer_is_valid(is_empty);

  bool is_empty_b = true;
  array->non_empty_domain_var_from_index(idx, start, end, &is_empty_b);
  *is_empty = (int32_t)is_empty_b;

  return TILEDB_OK;
}

capi_return_t tiledb_array_get_non_empty_domain_var_from_name(
    tiledb_array_t* array,
    const char* name,
    void* start,
    void* end,
    int32_t* is_empty) {
  ensure_array_is_valid(array);
  ensure_output_pointer_is_valid(start);
  ensure_output_pointer_is_valid(end);
  ensure_output_pointer_is_valid(is_empty);

  bool is_empty_b = true;
  array->non_empty_domain_var_from_name(name, start, end, &is_empty_b);
  *is_empty = (int32_t)is_empty_b;

  return TILEDB_OK;
}

capi_return_t tiledb_array_encryption_type(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_encryption_type_t* encryption_type) {
  ensure_output_pointer_is_valid(encryption_type);

  // Get encryption type
  tiledb::sm::EncryptionType enc;
  sm::Array::encryption_type(
      ctx->resources(), URI(array_uri, URI::must_be_valid), &enc);
  *encryption_type = static_cast<tiledb_encryption_type_t>(enc);

  return TILEDB_OK;
}

capi_return_t tiledb_array_put_metadata(
    tiledb_array_t* array,
    const char* key,
    tiledb_datatype_t value_type,
    uint32_t value_num,
    const void* value) {
  ensure_array_is_valid(array);
  array->put_metadata(
      key, static_cast<tiledb::sm::Datatype>(value_type), value_num, value);
  return TILEDB_OK;
}

capi_return_t tiledb_array_delete_metadata(
    tiledb_array_t* array, const char* key) {
  ensure_array_is_valid(array);
  array->delete_metadata(key);
  return TILEDB_OK;
}

capi_return_t tiledb_array_get_metadata(
    tiledb_array_t* array,
    const char* key,
    tiledb_datatype_t* value_type,
    uint32_t* value_num,
    const void** value) {
  ensure_array_is_valid(array);
  ensure_output_pointer_is_valid(value_type);
  ensure_output_pointer_is_valid(value_num);
  ensure_output_pointer_is_valid(value);

  // Get metadata
  tiledb::sm::Datatype type;
  array->get_metadata(key, &type, value_num, value);
  *value_type = static_cast<tiledb_datatype_t>(type);

  return TILEDB_OK;
}

capi_return_t tiledb_array_get_metadata_num(
    tiledb_array_t* array, uint64_t* num) {
  ensure_array_is_valid(array);
  ensure_output_pointer_is_valid(num);
  *num = array->metadata_num();
  return TILEDB_OK;
}

capi_return_t tiledb_array_get_metadata_from_index(
    tiledb_array_t* array,
    uint64_t index,
    const char** key,
    uint32_t* key_len,
    tiledb_datatype_t* value_type,
    uint32_t* value_num,
    const void** value) {
  ensure_array_is_valid(array);
  ensure_output_pointer_is_valid(key);
  ensure_output_pointer_is_valid(key_len);
  ensure_output_pointer_is_valid(value_type);
  ensure_output_pointer_is_valid(value_num);
  ensure_output_pointer_is_valid(value);

  // Get metadata
  tiledb::sm::Datatype type;
  array->get_metadata(index, key, key_len, &type, value_num, value);
  *value_type = static_cast<tiledb_datatype_t>(type);

  return TILEDB_OK;
}

capi_return_t tiledb_array_has_metadata_key(
    tiledb_array_t* array,
    const char* key,
    tiledb_datatype_t* value_type,
    int32_t* has_key) {
  ensure_array_is_valid(array);
  ensure_output_pointer_is_valid(value_type);
  ensure_output_pointer_is_valid(has_key);

  // Check whether metadata has_key
  std::optional<tiledb::sm::Datatype> type = array->metadata_type(key);

  *has_key = type.has_value();
  if (*has_key) {
    *value_type = static_cast<tiledb_datatype_t>(type.value());
  }
  return TILEDB_OK;
}

capi_return_t tiledb_array_evolve(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_array_schema_evolution_t* array_schema_evolution) {
  ensure_array_schema_evolution_is_valid(array_schema_evolution);

  // Create key
  tiledb::sm::EncryptionKey key;
  throw_if_not_ok(
      key.set_key(tiledb::sm::EncryptionType::NO_ENCRYPTION, nullptr, 0));

  // Evolve schema
  tiledb::sm::Array::evolve_array_schema(
      ctx->resources(),
      URI(array_uri, URI::must_be_valid),
      array_schema_evolution->array_schema_evolution_,
      key);

  // Success
  return TILEDB_OK;
}

capi_return_t tiledb_array_get_enumeration(
    const tiledb_array_t* array,
    const char* attr_name,
    tiledb_enumeration_t** enumeration) {
  ensure_array_is_valid(array);
  ensure_output_pointer_is_valid(enumeration);

  if (attr_name == nullptr) {
    throw CAPIException("'attr_name' must not be null");
  }

  auto ptr = array->get_enumeration(attr_name);
  *enumeration = tiledb_enumeration_handle_t::make_handle(ptr);

  return TILEDB_OK;
}

capi_return_t tiledb_array_load_all_enumerations(
    const tiledb_array_t* array, uint8_t all_schemas) {
  ensure_array_is_valid(array);
  array->load_all_enumerations(all_schemas > 0);
  return TILEDB_OK;
}

}  // namespace tiledb::api

using tiledb::api::api_entry_context;
using tiledb::api::api_entry_plain;
using tiledb::api::api_entry_void;
using tiledb::api::api_entry_with_context;

CAPI_INTERFACE(
    encryption_type_to_str,
    tiledb_encryption_type_t encryption_type,
    const char** str) {
  return api_entry_plain<tiledb::api::tiledb_encryption_type_to_str>(
      encryption_type, str);
}

CAPI_INTERFACE(
    encryption_type_from_str,
    const char* str,
    tiledb_encryption_type_t* encryption_type) {
  return api_entry_plain<tiledb::api::tiledb_encryption_type_from_str>(
      str, encryption_type);
}

CAPI_INTERFACE(
    array_schema_load,
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_array_schema_t** array_schema) {
  return api_entry_with_context<tiledb::api::tiledb_array_schema_load>(
      ctx, array_uri, array_schema);
}

CAPI_INTERFACE(
    array_schema_load_with_config,
    tiledb_ctx_t* ctx,
    tiledb_config_t* config,
    const char* array_uri,
    tiledb_array_schema_t** array_schema) {
  return api_entry_with_context<
      tiledb::api::tiledb_array_schema_load_with_config>(
      ctx, config, array_uri, array_schema);
}

CAPI_INTERFACE(
    array_alloc,
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_array_t** array) {
  return api_entry_with_context<tiledb::api::tiledb_array_alloc>(
      ctx, array_uri, array);
}

CAPI_INTERFACE_VOID(array_free, tiledb_array_t** array) {
  return api_entry_void<tiledb::api::tiledb_array_free>(array);
}

CAPI_INTERFACE(
    array_create,
    tiledb_ctx_t* ctx,
    const char* array_uri,
    const tiledb_array_schema_t* array_schema) {
  return api_entry_with_context<tiledb::api::tiledb_array_create>(
      ctx, array_uri, array_schema);
}

CAPI_INTERFACE(
    array_open,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_query_type_t query_type) {
  return api_entry_context<tiledb::api::tiledb_array_open>(
      ctx, array, query_type);
}

CAPI_INTERFACE(
    array_is_open, tiledb_ctx_t* ctx, tiledb_array_t* array, int32_t* is_open) {
  return api_entry_context<tiledb::api::tiledb_array_is_open>(
      ctx, array, is_open);
}

CAPI_INTERFACE(array_close, tiledb_ctx_t* ctx, tiledb_array_t* array) {
  return api_entry_context<tiledb::api::tiledb_array_close>(ctx, array);
}

CAPI_INTERFACE(array_reopen, tiledb_ctx_t* ctx, tiledb_array_t* array) {
  return api_entry_context<tiledb::api::tiledb_array_reopen>(ctx, array);
}

CAPI_INTERFACE(array_delete, tiledb_ctx_t* ctx, const char* uri) {
  return api_entry_with_context<tiledb::api::tiledb_array_delete>(ctx, uri);
}

CAPI_INTERFACE(
    array_delete_fragments_v2,
    tiledb_ctx_t* ctx,
    const char* uri_str,
    uint64_t timestamp_start,
    uint64_t timestamp_end) {
  return api_entry_with_context<tiledb::api::tiledb_array_delete_fragments_v2>(
      ctx, uri_str, timestamp_start, timestamp_end);
}

CAPI_INTERFACE(
    array_delete_fragments_list,
    tiledb_ctx_t* ctx,
    const char* uri_str,
    const char* fragment_uris[],
    const size_t num_fragments) {
  return api_entry_with_context<
      tiledb::api::tiledb_array_delete_fragments_list>(
      ctx, uri_str, fragment_uris, num_fragments);
}

CAPI_INTERFACE(
    array_set_config,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_config_t* config) {
  return api_entry_context<tiledb::api::tiledb_array_set_config>(
      ctx, array, config);
}

CAPI_INTERFACE(
    array_set_open_timestamp_start,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    uint64_t timestamp_start) {
  return api_entry_context<tiledb::api::tiledb_array_set_open_timestamp_start>(
      ctx, array, timestamp_start);
}

CAPI_INTERFACE(
    array_set_open_timestamp_end,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    uint64_t timestamp_end) {
  return api_entry_context<tiledb::api::tiledb_array_set_open_timestamp_end>(
      ctx, array, timestamp_end);
}

CAPI_INTERFACE(
    array_get_config,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_config_t** config) {
  return api_entry_context<tiledb::api::tiledb_array_get_config>(
      ctx, array, config);
}

CAPI_INTERFACE(
    array_get_open_timestamp_start,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    uint64_t* timestamp_start) {
  return api_entry_context<tiledb::api::tiledb_array_get_open_timestamp_start>(
      ctx, array, timestamp_start);
}

CAPI_INTERFACE(
    array_get_open_timestamp_end,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    uint64_t* timestamp_end) {
  return api_entry_context<tiledb::api::tiledb_array_get_open_timestamp_end>(
      ctx, array, timestamp_end);
}

CAPI_INTERFACE(
    array_get_schema,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_array_schema_t** array_schema) {
  return api_entry_context<tiledb::api::tiledb_array_get_schema>(
      ctx, array, array_schema);
}

CAPI_INTERFACE(
    array_get_query_type,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_query_type_t* query_type) {
  return api_entry_context<tiledb::api::tiledb_array_get_query_type>(
      ctx, array, query_type);
}

CAPI_INTERFACE(
    array_get_uri,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const char** array_uri) {
  return api_entry_context<tiledb::api::tiledb_array_get_uri>(
      ctx, array, array_uri);
}

CAPI_INTERFACE(
    array_upgrade_version,
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_config_t* config) {
  return api_entry_with_context<tiledb::api::tiledb_array_upgrade_version>(
      ctx, array_uri, config);
}

CAPI_INTERFACE(
    array_get_non_empty_domain,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    void* domain,
    int32_t* is_empty) {
  return api_entry_context<tiledb::api::tiledb_array_get_non_empty_domain>(
      ctx, array, domain, is_empty);
}

CAPI_INTERFACE(
    array_get_non_empty_domain_from_index,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    uint32_t idx,
    void* domain,
    int32_t* is_empty) {
  return api_entry_context<
      tiledb::api::tiledb_array_get_non_empty_domain_from_index>(
      ctx, array, idx, domain, is_empty);
}

CAPI_INTERFACE(
    array_get_non_empty_domain_from_name,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const char* name,
    void* domain,
    int32_t* is_empty) {
  return api_entry_context<
      tiledb::api::tiledb_array_get_non_empty_domain_from_name>(
      ctx, array, name, domain, is_empty);
}

CAPI_INTERFACE(
    array_get_non_empty_domain_var_size_from_index,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    uint32_t idx,
    uint64_t* start_size,
    uint64_t* end_size,
    int32_t* is_empty) {
  return api_entry_context<
      tiledb::api::tiledb_array_get_non_empty_domain_var_size_from_index>(
      ctx, array, idx, start_size, end_size, is_empty);
}

CAPI_INTERFACE(
    array_get_non_empty_domain_var_size_from_name,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const char* name,
    uint64_t* start_size,
    uint64_t* end_size,
    int32_t* is_empty) {
  return api_entry_context<
      tiledb::api::tiledb_array_get_non_empty_domain_var_size_from_name>(
      ctx, array, name, start_size, end_size, is_empty);
}

CAPI_INTERFACE(
    array_get_non_empty_domain_var_from_index,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    uint32_t idx,
    void* start,
    void* end,
    int32_t* is_empty) {
  return api_entry_context<
      tiledb::api::tiledb_array_get_non_empty_domain_var_from_index>(
      ctx, array, idx, start, end, is_empty);
}

CAPI_INTERFACE(
    array_get_non_empty_domain_var_from_name,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const char* name,
    void* start,
    void* end,
    int32_t* is_empty) {
  return api_entry_context<
      tiledb::api::tiledb_array_get_non_empty_domain_var_from_name>(
      ctx, array, name, start, end, is_empty);
}

CAPI_INTERFACE(
    array_encryption_type,
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_encryption_type_t* encryption_type) {
  return api_entry_with_context<tiledb::api::tiledb_array_encryption_type>(
      ctx, array_uri, encryption_type);
}

CAPI_INTERFACE(
    array_put_metadata,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const char* key,
    tiledb_datatype_t value_type,
    uint32_t value_num,
    const void* value) {
  return api_entry_context<tiledb::api::tiledb_array_put_metadata>(
      ctx, array, key, value_type, value_num, value);
}

CAPI_INTERFACE(
    array_delete_metadata,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const char* key) {
  return api_entry_context<tiledb::api::tiledb_array_delete_metadata>(
      ctx, array, key);
}

CAPI_INTERFACE(
    array_get_metadata,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const char* key,
    tiledb_datatype_t* value_type,
    uint32_t* value_num,
    const void** value) {
  return api_entry_context<tiledb::api::tiledb_array_get_metadata>(
      ctx, array, key, value_type, value_num, value);
}

CAPI_INTERFACE(
    array_get_metadata_num,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    uint64_t* num) {
  return api_entry_context<tiledb::api::tiledb_array_get_metadata_num>(
      ctx, array, num);
}

CAPI_INTERFACE(
    array_get_metadata_from_index,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    uint64_t index,
    const char** key,
    uint32_t* key_len,
    tiledb_datatype_t* value_type,
    uint32_t* value_num,
    const void** value) {
  return api_entry_context<tiledb::api::tiledb_array_get_metadata_from_index>(
      ctx, array, index, key, key_len, value_type, value_num, value);
}

CAPI_INTERFACE(
    array_has_metadata_key,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const char* key,
    tiledb_datatype_t* value_type,
    int32_t* has_key) {
  return api_entry_context<tiledb::api::tiledb_array_has_metadata_key>(
      ctx, array, key, value_type, has_key);
}

CAPI_INTERFACE(
    array_evolve,
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_array_schema_evolution_t* array_schema_evolution) {
  return api_entry_with_context<tiledb::api::tiledb_array_evolve>(
      ctx, array_uri, array_schema_evolution);
}

CAPI_INTERFACE(
    array_get_enumeration,
    tiledb_ctx_t* ctx,
    const tiledb_array_t* array,
    const char* attr_name,
    tiledb_enumeration_t** enumeration) {
  return api_entry_context<tiledb::api::tiledb_array_get_enumeration>(
      ctx, array, attr_name, enumeration);
}

CAPI_INTERFACE(
    array_load_all_enumerations,
    tiledb_ctx_t* ctx,
    const tiledb_array_t* array,
    uint8_t all_schemas) {
  return api_entry_context<tiledb::api::tiledb_array_load_all_enumerations>(
      ctx, array, all_schemas);
}
