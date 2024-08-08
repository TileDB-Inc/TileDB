/**
 * @file tiledb/api/c_api/group/group_api.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
 * This file defines the group C API of TileDB.
 **/

#include "tiledb/api/c_api_support/c_api_support.h"
#include "tiledb/sm/c_api/tiledb_serialization.h"
#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/group/group_details_v1.h"
#include "tiledb/sm/serialization/array.h"
#include "tiledb/sm/serialization/group.h"

#include "../buffer/buffer_api_internal.h"
#include "../config/config_api_internal.h"
#include "../context/context_api_internal.h"
#include "../string/string_api_internal.h"

#include "group_api_internal.h"

namespace tiledb::api {

inline void ensure_group_uri_argument_is_valid(const char* group_uri) {
  if (group_uri == nullptr) {
    throw CAPIStatusException("argument `group_uri` may not be nullptr");
  }
}

inline void ensure_key_argument_is_valid(const char* key) {
  if (key == nullptr) {
    throw CAPIStatusException("argument `key` may not be nullptr");
  }
}

inline void ensure_name_argument_is_valid(const char* name) {
  if (name == nullptr) {
    throw CAPIStatusException("argument `name` may not be nullptr");
  }
}

inline char* copy_string(const std::string& str) {
  auto ret = static_cast<char*>(std::malloc(str.size() + 1));
  if (ret == nullptr) {
    return nullptr;
  }

  std::memcpy(ret, str.data(), str.size());
  ret[str.size()] = '\0';

  return ret;
}

capi_return_t tiledb_group_create(
    tiledb_ctx_handle_t* ctx, const char* group_uri) {
  ensure_group_uri_argument_is_valid(group_uri);

  tiledb::sm::Group::create(ctx->resources(), tiledb::sm::URI(group_uri));

  return TILEDB_OK;
}

capi_return_t tiledb_group_alloc(
    tiledb_ctx_handle_t* ctx,
    const char* group_uri,
    tiledb_group_handle_t** group) {
  ensure_output_pointer_is_valid(group);
  ensure_group_uri_argument_is_valid(group_uri);

  auto uri = tiledb::sm::URI(group_uri);
  if (uri.is_invalid()) {
    throw CAPIStatusException(
        "Failed to allocate TileDB group API object; Invalid URI");
  }

  *group = tiledb_group_handle_t::make_handle(ctx->resources(), uri);

  return TILEDB_OK;
}

void tiledb_group_free(tiledb_group_handle_t** group) {
  ensure_output_pointer_is_valid(group);
  ensure_group_is_valid(*group);
  tiledb_group_handle_t::break_handle(*group);
}

capi_return_t tiledb_group_open(
    tiledb_group_handle_t* group, tiledb_query_type_t query_type) {
  ensure_group_is_valid(group);

  group->group().open(static_cast<tiledb::sm::QueryType>(query_type));

  return TILEDB_OK;
}

capi_return_t tiledb_group_close(tiledb_group_handle_t* group) {
  ensure_group_is_valid(group);

  group->group().close();

  return TILEDB_OK;
}

capi_return_t tiledb_group_set_config(
    tiledb_group_handle_t* group, tiledb_config_handle_t* config) {
  ensure_group_is_valid(group);
  ensure_config_is_valid(config);

  group->group().set_config(config->config());

  return TILEDB_OK;
}

capi_return_t tiledb_group_get_config(
    tiledb_group_handle_t* group, tiledb_config_t** config) {
  ensure_group_is_valid(group);
  ensure_output_pointer_is_valid(config);

  *config = tiledb_config_handle_t::make_handle(group->group().config());

  return TILEDB_OK;
}

capi_return_t tiledb_group_put_metadata(
    tiledb_group_handle_t* group,
    const char* key,
    tiledb_datatype_t value_type,
    uint32_t value_num,
    const void* value) {
  ensure_group_is_valid(group);
  ensure_key_argument_is_valid(key);

  group->group().put_metadata(
      key, static_cast<tiledb::sm::Datatype>(value_type), value_num, value);

  return TILEDB_OK;
}

capi_return_t tiledb_group_delete_group(
    tiledb_group_t* group, const char* uri, const uint8_t recursive) {
  ensure_group_is_valid(group);
  ensure_group_uri_argument_is_valid(uri);

  group->group().delete_group(tiledb::sm::URI(uri), recursive);
  return TILEDB_OK;
}

capi_return_t tiledb_group_delete_metadata(
    tiledb_group_handle_t* group, const char* key) {
  ensure_group_is_valid(group);
  ensure_key_argument_is_valid(key);

  group->group().delete_metadata(key);

  return TILEDB_OK;
}

capi_return_t tiledb_group_get_metadata(
    tiledb_group_handle_t* group,
    const char* key,
    tiledb_datatype_t* value_type,
    uint32_t* value_num,
    const void** value) {
  ensure_group_is_valid(group);
  ensure_key_argument_is_valid(key);
  ensure_output_pointer_is_valid(value_type);
  ensure_output_pointer_is_valid(value_num);
  ensure_output_pointer_is_valid(value);

  tiledb::sm::Datatype type;
  group->group().get_metadata(key, &type, value_num, value);

  *value_type = static_cast<tiledb_datatype_t>(type);

  return TILEDB_OK;
}

capi_return_t tiledb_group_get_metadata_num(
    tiledb_group_handle_t* group, uint64_t* num) {
  ensure_group_is_valid(group);
  ensure_output_pointer_is_valid(num);

  *num = group->group().get_metadata_num();

  return TILEDB_OK;
}

capi_return_t tiledb_group_get_metadata_from_index(
    tiledb_group_handle_t* group,
    uint64_t index,
    const char** key,
    uint32_t* key_len,
    tiledb_datatype_t* value_type,
    uint32_t* value_num,
    const void** value) {
  ensure_group_is_valid(group);
  ensure_output_pointer_is_valid(key);
  ensure_output_pointer_is_valid(key_len);
  ensure_output_pointer_is_valid(value_type);
  ensure_output_pointer_is_valid(value_num);
  ensure_output_pointer_is_valid(value);

  tiledb::sm::Datatype type;
  group->group().get_metadata(index, key, key_len, &type, value_num, value);

  *value_type = static_cast<tiledb_datatype_t>(type);

  return TILEDB_OK;
}

capi_return_t tiledb_group_has_metadata_key(
    tiledb_group_handle_t* group,
    const char* key,
    tiledb_datatype_t* value_type,
    int32_t* has_key) {
  ensure_group_is_valid(group);
  ensure_key_argument_is_valid(key);
  ensure_output_pointer_is_valid(value_type);
  ensure_output_pointer_is_valid(has_key);

  std::optional<tiledb::sm::Datatype> type = group->group().metadata_type(key);

  *has_key = type.has_value();
  if (*has_key) {
    *value_type = static_cast<tiledb_datatype_t>(type.value());
  }

  return TILEDB_OK;
}

capi_return_t tiledb_group_add_member(
    tiledb_group_handle_t* group,
    const char* group_uri,
    const uint8_t relative,
    const char* name) {
  ensure_group_is_valid(group);
  ensure_group_uri_argument_is_valid(group_uri);

  auto uri = tiledb::sm::URI(group_uri, !relative);

  std::optional<std::string> name_optional = std::nullopt;
  if (name != nullptr) {
    name_optional = name;
  }

  group->group().mark_member_for_addition(uri, relative, name_optional);

  return TILEDB_OK;
}

capi_return_t tiledb_group_remove_member(
    tiledb_group_handle_t* group, const char* name_or_uri) {
  ensure_group_is_valid(group);
  ensure_name_argument_is_valid(name_or_uri);

  group->group().mark_member_for_removal(name_or_uri);

  return TILEDB_OK;
}

capi_return_t tiledb_group_get_member_count(
    tiledb_group_handle_t* group, uint64_t* count) {
  ensure_group_is_valid(group);
  ensure_output_pointer_is_valid(count);

  *count = group->group().member_count();

  return TILEDB_OK;
}

capi_return_t tiledb_group_get_member_by_index_v2(
    tiledb_group_handle_t* group,
    uint64_t index,
    tiledb_string_handle_t** uri,
    tiledb_object_t* type,
    tiledb_string_handle_t** name) {
  ensure_group_is_valid(group);
  ensure_output_pointer_is_valid(uri);
  ensure_output_pointer_is_valid(type);
  ensure_output_pointer_is_valid(name);

  auto&& [uri_str, object_type, name_str] =
      group->group().member_by_index(index);

  *uri = tiledb_string_handle_t::make_handle(uri_str);
  *type = static_cast<tiledb_object_t>(object_type);
  try {
    *name = name_str.has_value() ?
                tiledb_string_handle_t::make_handle(*name_str) :
                nullptr;
  } catch (...) {
    tiledb_string_handle_t::break_handle(*uri);
    throw;
  }

  return TILEDB_OK;
}

capi_return_t tiledb_group_get_member_by_name_v2(
    tiledb_group_handle_t* group,
    const char* name,
    tiledb_string_handle_t** uri,
    tiledb_object_t* type) {
  ensure_group_is_valid(group);
  ensure_output_pointer_is_valid(uri);
  ensure_output_pointer_is_valid(type);

  std::string uri_str;
  sm::ObjectType object_type;
  std::tie(uri_str, object_type, std::ignore, std::ignore) =
      group->group().member_by_name(name);

  *uri = tiledb_string_handle_t::make_handle(std::move(uri_str));
  *type = static_cast<tiledb_object_t>(object_type);

  return TILEDB_OK;
}

capi_return_t tiledb_group_get_is_relative_uri_by_name(
    tiledb_group_handle_t* group, const char* name, uint8_t* is_relative) {
  ensure_group_is_valid(group);
  ensure_name_argument_is_valid(name);
  ensure_output_pointer_is_valid(is_relative);

  auto&& [uri_str, object_type, name_str, relative] =
      group->group().member_by_name(name);

  *is_relative = relative ? 1 : 0;

  return TILEDB_OK;
}

capi_return_t tiledb_group_is_open(
    tiledb_group_handle_t* group, int32_t* is_open) {
  ensure_group_is_valid(group);
  ensure_output_pointer_is_valid(is_open);

  *is_open = static_cast<int32_t>(group->group().is_open());

  return TILEDB_OK;
}

capi_return_t tiledb_group_get_uri(
    tiledb_group_handle_t* group, const char** group_uri) {
  ensure_group_is_valid(group);
  ensure_output_pointer_is_valid(group_uri);

  *group_uri = group->group().group_uri().c_str();

  return TILEDB_OK;
}

capi_return_t tiledb_group_get_query_type(
    tiledb_group_handle_t* group, tiledb_query_type_t* query_type) {
  ensure_group_is_valid(group);
  ensure_output_pointer_is_valid(query_type);

  // Get query_type
  tiledb::sm::QueryType type = group->group().get_query_type();

  *query_type = static_cast<tiledb_query_type_t>(type);

  return TILEDB_OK;
}

capi_return_t tiledb_group_dump_str(
    tiledb_group_handle_t* group, char** dump_ascii, const uint8_t recursive) {
  ensure_group_is_valid(group);
  ensure_output_pointer_is_valid(dump_ascii);

  const std::string str = group->group().dump(2, 0, recursive);
  *dump_ascii = copy_string(str);
  if (*dump_ascii == nullptr) {
    return TILEDB_ERR;
  }

  return TILEDB_OK;
}

capi_return_t tiledb_serialize_group(
    const tiledb_group_handle_t* group,
    tiledb_serialization_type_t serialize_type,
    int32_t,
    tiledb_buffer_t** buffer) {
  ensure_group_is_valid(group);
  ensure_output_pointer_is_valid(buffer);

  // ALERT: Conditional Handle Construction
  auto buf = tiledb_buffer_handle_t::make_handle();

  // We're not using throw_if_not_ok here because we have to
  // clean up our allocated buffer if serialization fails.
  auto st = tiledb::sm::serialization::group_serialize(
      &(group->group()),
      static_cast<tiledb::sm::SerializationType>(serialize_type),
      &(buf->buffer()));

  if (!st.ok()) {
    tiledb_buffer_handle_t::break_handle(buf);
    throw StatusException(st);
  }

  *buffer = buf;

  return TILEDB_OK;
}

capi_return_t tiledb_deserialize_group(
    const tiledb_buffer_handle_t* buffer,
    tiledb_serialization_type_t serialize_type,
    int32_t,
    tiledb_group_handle_t* group) {
  ensure_buffer_is_valid(buffer);
  ensure_output_pointer_is_valid(group);

  throw_if_not_ok(tiledb::sm::serialization::group_deserialize(
      &(group->group()),
      static_cast<tiledb::sm::SerializationType>(serialize_type),
      buffer->buffer()));

  return TILEDB_OK;
}

capi_return_t tiledb_serialize_group_metadata(
    const tiledb_group_handle_t* group,
    tiledb_serialization_type_t serialize_type,
    tiledb_buffer_t** buffer) {
  ensure_group_is_valid(group);
  ensure_output_pointer_is_valid(buffer);

  // ALERT: Conditional Handle Construction
  auto buf = tiledb_buffer_handle_t::make_handle();

  // Get metadata to serialize, this will load it if it does not exist
  auto metadata = group->group().metadata();

  auto st = tiledb::sm::serialization::metadata_serialize(
      metadata,
      static_cast<tiledb::sm::SerializationType>(serialize_type),
      &(buf->buffer()));

  if (!st.ok()) {
    tiledb_buffer_handle_t::break_handle(buf);
    throw StatusException(st);
  }

  *buffer = buf;

  return TILEDB_OK;
}

capi_return_t tiledb_deserialize_group_metadata(
    tiledb_group_handle_t* group,
    tiledb_serialization_type_t serialize_type,
    const tiledb_buffer_handle_t* buffer) {
  ensure_group_is_valid(group);
  ensure_buffer_is_valid(buffer);

  throw_if_not_ok(tiledb::sm::serialization::metadata_deserialize(
      group->group().unsafe_metadata(),
      group->group().config(),
      static_cast<tiledb::sm::SerializationType>(serialize_type),
      buffer->buffer()));

  return TILEDB_OK;
}

capi_return_t tiledb_group_consolidate_metadata(
    tiledb_ctx_handle_t* ctx, const char* group_uri, tiledb_config_t* config) {
  ensure_group_uri_argument_is_valid(group_uri);

  auto cfg = (config == nullptr) ? ctx->config() : config->config();
  tiledb::sm::Group::consolidate_metadata(ctx->resources(), group_uri, cfg);

  return TILEDB_OK;
}

capi_return_t tiledb_group_vacuum_metadata(
    tiledb_ctx_handle_t* ctx, const char* group_uri, tiledb_config_t* config) {
  ensure_group_uri_argument_is_valid(group_uri);

  auto cfg = (config == nullptr) ? ctx->config() : config->config();
  sm::Group::vacuum_metadata(ctx->resources(), group_uri, cfg);

  return TILEDB_OK;
}

}  // namespace tiledb::api

using tiledb::api::api_entry_context;
using tiledb::api::api_entry_void;
using tiledb::api::api_entry_with_context;

CAPI_INTERFACE(group_create, tiledb_ctx_t* ctx, const char* group_uri) {
  return api_entry_with_context<tiledb::api::tiledb_group_create>(
      ctx, group_uri);
}

CAPI_INTERFACE(
    group_alloc,
    tiledb_ctx_t* ctx,
    const char* group_uri,
    tiledb_group_t** group) {
  return api_entry_with_context<tiledb::api::tiledb_group_alloc>(
      ctx, group_uri, group);
}

CAPI_INTERFACE(
    group_open,
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    tiledb_query_type_t query_type) {
  return api_entry_context<tiledb::api::tiledb_group_open>(
      ctx, group, query_type);
}

CAPI_INTERFACE(group_close, tiledb_ctx_t* ctx, tiledb_group_t* group) {
  return api_entry_context<tiledb::api::tiledb_group_close>(ctx, group);
}

CAPI_INTERFACE_VOID(group_free, tiledb_group_t** group) {
  return api_entry_void<tiledb::api::tiledb_group_free>(group);
}

CAPI_INTERFACE(
    group_set_config,
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    tiledb_config_t* config) {
  return api_entry_context<tiledb::api::tiledb_group_set_config>(
      ctx, group, config);
}

CAPI_INTERFACE(
    group_get_config,
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    tiledb_config_t** config) {
  return api_entry_context<tiledb::api::tiledb_group_get_config>(
      ctx, group, config);
}

CAPI_INTERFACE(
    group_put_metadata,
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    const char* key,
    tiledb_datatype_t value_type,
    uint32_t value_num,
    const void* value) {
  return api_entry_context<tiledb::api::tiledb_group_put_metadata>(
      ctx, group, key, value_type, value_num, value);
}

CAPI_INTERFACE(
    group_delete_group,
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    const char* uri,
    const uint8_t recursive) {
  return api_entry_context<tiledb::api::tiledb_group_delete_group>(
      ctx, group, uri, recursive);
}

CAPI_INTERFACE(
    group_delete_metadata,
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    const char* key) {
  return api_entry_context<tiledb::api::tiledb_group_delete_metadata>(
      ctx, group, key);
}

CAPI_INTERFACE(
    group_get_metadata,
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    const char* key,
    tiledb_datatype_t* value_type,
    uint32_t* value_num,
    const void** value) {
  return api_entry_context<tiledb::api::tiledb_group_get_metadata>(
      ctx, group, key, value_type, value_num, value);
}

CAPI_INTERFACE(
    group_get_metadata_num,
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    uint64_t* num) {
  return api_entry_context<tiledb::api::tiledb_group_get_metadata_num>(
      ctx, group, num);
}

CAPI_INTERFACE(
    group_get_metadata_from_index,
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    uint64_t index,
    const char** key,
    uint32_t* key_len,
    tiledb_datatype_t* value_type,
    uint32_t* value_num,
    const void** value) {
  return api_entry_context<tiledb::api::tiledb_group_get_metadata_from_index>(
      ctx, group, index, key, key_len, value_type, value_num, value);
}

CAPI_INTERFACE(
    group_has_metadata_key,
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    const char* key,
    tiledb_datatype_t* value_type,
    int32_t* has_key) {
  return api_entry_context<tiledb::api::tiledb_group_has_metadata_key>(
      ctx, group, key, value_type, has_key);
}

CAPI_INTERFACE(
    group_add_member,
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    const char* uri,
    const uint8_t relative,
    const char* name) {
  return api_entry_context<tiledb::api::tiledb_group_add_member>(
      ctx, group, uri, relative, name);
}

CAPI_INTERFACE(
    group_remove_member,
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    const char* name) {
  return api_entry_context<tiledb::api::tiledb_group_remove_member>(
      ctx, group, name);
}

CAPI_INTERFACE(
    group_get_member_count,
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    uint64_t* count) {
  return api_entry_context<tiledb::api::tiledb_group_get_member_count>(
      ctx, group, count);
}

CAPI_INTERFACE(
    group_get_member_by_index_v2,
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    uint64_t index,
    tiledb_string_handle_t** uri,
    tiledb_object_t* type,
    tiledb_string_handle_t** name) {
  return api_entry_context<tiledb::api::tiledb_group_get_member_by_index_v2>(
      ctx, group, index, uri, type, name);
}

CAPI_INTERFACE(
    group_get_member_by_name_v2,
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    const char* name,
    tiledb_string_handle_t** uri,
    tiledb_object_t* type) {
  return api_entry_context<tiledb::api::tiledb_group_get_member_by_name_v2>(
      ctx, group, name, uri, type);
}

CAPI_INTERFACE(
    group_get_is_relative_uri_by_name,
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    const char* name,
    uint8_t* relative) {
  return api_entry_context<
      tiledb::api::tiledb_group_get_is_relative_uri_by_name>(
      ctx, group, name, relative);
}

CAPI_INTERFACE(
    group_is_open, tiledb_ctx_t* ctx, tiledb_group_t* group, int32_t* is_open) {
  return api_entry_context<tiledb::api::tiledb_group_is_open>(
      ctx, group, is_open);
}

CAPI_INTERFACE(
    group_get_uri,
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    const char** group_uri) {
  return api_entry_context<tiledb::api::tiledb_group_get_uri>(
      ctx, group, group_uri);
}

CAPI_INTERFACE(
    group_get_query_type,
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    tiledb_query_type_t* query_type) {
  return api_entry_context<tiledb::api::tiledb_group_get_query_type>(
      ctx, group, query_type);
}

CAPI_INTERFACE(
    group_dump_str,
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    char** dump_ascii,
    const uint8_t recursive) {
  return api_entry_context<tiledb::api::tiledb_group_dump_str>(
      ctx, group, dump_ascii, recursive);
}

CAPI_INTERFACE(
    serialize_group,
    tiledb_ctx_t* ctx,
    const tiledb_group_t* group,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_buffer_t** buffer_list) {
  return api_entry_context<tiledb::api::tiledb_serialize_group>(
      ctx, group, serialize_type, client_side, buffer_list);
}

CAPI_INTERFACE(
    deserialize_group,
    tiledb_ctx_t* ctx,
    const tiledb_buffer_t* buffer,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_group_t* group) {
  return api_entry_context<tiledb::api::tiledb_deserialize_group>(
      ctx, buffer, serialize_type, client_side, group);
}

CAPI_INTERFACE(
    serialize_group_metadata,
    tiledb_ctx_t* ctx,
    const tiledb_group_t* group,
    tiledb_serialization_type_t serialization_type,
    tiledb_buffer_t** buffer) {
  return api_entry_context<tiledb::api::tiledb_serialize_group_metadata>(
      ctx, group, serialization_type, buffer);
}

CAPI_INTERFACE(
    deserialize_group_metadata,
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    tiledb_serialization_type_t serialization_type,
    const tiledb_buffer_t* buffer) {
  return api_entry_context<tiledb::api::tiledb_deserialize_group_metadata>(
      ctx, group, serialization_type, buffer);
}

CAPI_INTERFACE(
    group_consolidate_metadata,
    tiledb_ctx_t* ctx,
    const char* group_uri,
    tiledb_config_t* config) {
  return api_entry_with_context<tiledb::api::tiledb_group_consolidate_metadata>(
      ctx, group_uri, config);
}

CAPI_INTERFACE(
    group_vacuum_metadata,
    tiledb_ctx_t* ctx,
    const char* group_uri,
    tiledb_config_t* config) {
  return api_entry_with_context<tiledb::api::tiledb_group_vacuum_metadata>(
      ctx, group_uri, config);
}
