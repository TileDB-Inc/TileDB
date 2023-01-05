/**
 * @file tiledb/api/c_api/group/group_api.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2022 TileDB, Inc.
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
#include "tiledb/sm/group/group_v1.h"
#include "tiledb/sm/serialization/array.h"
#include "tiledb/sm/serialization/group.h"

#include "../buffer/buffer_api_internal.h"
#include "../config/config_api_internal.h"
#include "../context/context_api_internal.h"

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

  throw_if_not_ok(ctx->storage_manager()->group_create(group_uri));

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
        "Failed to create TileDB group object; Invalid URI");
  }

  *group = tiledb_group_handle_t::make_handle(uri, ctx->storage_manager());

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

  throw_if_not_ok(
      group->group().open(static_cast<tiledb::sm::QueryType>(query_type)));

  return TILEDB_OK;
}

capi_return_t tiledb_group_close(tiledb_group_handle_t* group) {
  ensure_group_is_valid(group);

  throw_if_not_ok(group->group().close());

  return TILEDB_OK;
}

capi_return_t tiledb_group_set_config(
    tiledb_group_handle_t* group, tiledb_config_handle_t* config) {
  ensure_group_is_valid(group);
  ensure_config_is_valid(config);

  throw_if_not_ok(group->group().set_config(config->config()));

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

  throw_if_not_ok(group->group().put_metadata(
      key, static_cast<tiledb::sm::Datatype>(value_type), value_num, value));

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

  throw_if_not_ok(group->group().delete_metadata(key));

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
  throw_if_not_ok(group->group().get_metadata(key, &type, value_num, value));

  *value_type = static_cast<tiledb_datatype_t>(type);

  return TILEDB_OK;
}

capi_return_t tiledb_group_get_metadata_num(
    tiledb_group_handle_t* group, uint64_t* num) {
  ensure_group_is_valid(group);
  ensure_output_pointer_is_valid(num);

  throw_if_not_ok(group->group().get_metadata_num(num));

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
  throw_if_not_ok(group->group().get_metadata(
      index, key, key_len, &type, value_num, value));

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

  tiledb::sm::Datatype type;
  bool has_the_key;
  throw_if_not_ok(group->group().has_metadata_key(key, &type, &has_the_key));

  *has_key = has_the_key ? 1 : 0;
  if (has_the_key) {
    *value_type = static_cast<tiledb_datatype_t>(type);
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

  throw_if_not_ok(
      group->group().mark_member_for_addition(uri, relative, name_optional));

  return TILEDB_OK;
}

capi_return_t tiledb_group_remove_member(
    tiledb_group_handle_t* group, const char* group_uri) {
  ensure_group_is_valid(group);
  ensure_group_uri_argument_is_valid(group_uri);

  throw_if_not_ok(group->group().mark_member_for_removal(group_uri));

  return TILEDB_OK;
}

capi_return_t tiledb_group_get_member_count(
    tiledb_group_handle_t* group, uint64_t* count) {
  ensure_group_is_valid(group);
  ensure_output_pointer_is_valid(count);

  *count = group->group().member_count();

  return TILEDB_OK;
}

capi_return_t tiledb_group_get_member_by_index(
    tiledb_group_handle_t* group,
    uint64_t index,
    char** uri,
    tiledb_object_t* type,
    char** name) {
  ensure_group_is_valid(group);
  ensure_output_pointer_is_valid(uri);
  ensure_output_pointer_is_valid(type);
  ensure_output_pointer_is_valid(name);

  char* tmp_uri = nullptr;
  char* tmp_name = nullptr;

  auto&& [uri_str, object_type, name_str] =
      group->group().member_by_index(index);

  tmp_uri = copy_string(uri_str);
  if (tmp_uri == nullptr) {
    goto error;
  }

  if (name_str.has_value()) {
    tmp_name = copy_string(name_str.value());
    if (tmp_name == nullptr) {
      goto error;
    }
  }

  *uri = tmp_uri;
  *type = static_cast<tiledb_object_t>(object_type);
  *name = tmp_name;

  return TILEDB_OK;

error:

  if (tmp_uri != nullptr) {
    std::free(tmp_uri);
  }

  if (tmp_name != nullptr) {
    std::free(tmp_name);
  }

  return TILEDB_OK;
}

capi_return_t tiledb_group_get_member_by_name(
    tiledb_group_handle_t* group,
    const char* name,
    char** uri,
    tiledb_object_t* type) {
  ensure_group_is_valid(group);
  ensure_name_argument_is_valid(name);
  ensure_output_pointer_is_valid(uri);
  ensure_output_pointer_is_valid(type);

  auto&& [uri_str, object_type, name_str, ignored_relative] =
      group->group().member_by_name(name);

  *uri = copy_string(uri_str);
  if (*uri == nullptr) {
    return TILEDB_ERR;
  }

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
  tiledb::sm::QueryType type;
  throw_if_not_ok(group->group().get_query_type(&type));

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
  tiledb::sm::Metadata* metadata;
  auto st = group->group().metadata(&metadata);
  if (!st.ok()) {
    tiledb_buffer_handle_t::break_handle(buf);
    throw StatusException(st);
  }

  st = tiledb::sm::serialization::metadata_serialize(
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
      static_cast<tiledb::sm::SerializationType>(serialize_type),
      buffer->buffer()));

  return TILEDB_OK;
}

capi_return_t tiledb_group_consolidate_metadata(
    tiledb_ctx_handle_t* ctx, const char* group_uri, tiledb_config_t* config) {
  ensure_group_uri_argument_is_valid(group_uri);
  ensure_config_is_valid(config);

  auto cfg =
      (config == nullptr) ? ctx->storage_manager()->config() : config->config();
  throw_if_not_ok(
      ctx->storage_manager()->group_metadata_consolidate(group_uri, cfg));

  return TILEDB_OK;
}

capi_return_t tiledb_group_vacuum_metadata(
    tiledb_ctx_handle_t* ctx, const char* group_uri, tiledb_config_t* config) {
  ensure_group_uri_argument_is_valid(group_uri);
  ensure_config_is_valid(config);

  auto cfg =
      (config == nullptr) ? ctx->storage_manager()->config() : config->config();
  ctx->storage_manager()->group_metadata_vacuum(group_uri, cfg);

  return TILEDB_OK;
}

}  // namespace tiledb::api

using tiledb::api::api_entry_context;
using tiledb::api::api_entry_void;
using tiledb::api::api_entry_with_context;

capi_return_t tiledb_group_create(tiledb_ctx_t* ctx, const char* group_uri)
    TILEDB_NOEXCEPT {
  return api_entry_with_context<tiledb::api::tiledb_group_create>(
      ctx, group_uri);
}

capi_return_t tiledb_group_alloc(
    tiledb_ctx_t* ctx,
    const char* group_uri,
    tiledb_group_t** group) TILEDB_NOEXCEPT {
  return api_entry_with_context<tiledb::api::tiledb_group_alloc>(
      ctx, group_uri, group);
}

capi_return_t tiledb_group_open(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    tiledb_query_type_t query_type) TILEDB_NOEXCEPT {
  return api_entry_context<tiledb::api::tiledb_group_open>(
      ctx, group, query_type);
}

capi_return_t tiledb_group_close(tiledb_ctx_t* ctx, tiledb_group_t* group)
    TILEDB_NOEXCEPT {
  return api_entry_context<tiledb::api::tiledb_group_close>(ctx, group);
}

void tiledb_group_free(tiledb_group_t** group) TILEDB_NOEXCEPT {
  return api_entry_void<tiledb::api::tiledb_group_free>(group);
}

capi_return_t tiledb_group_set_config(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    tiledb_config_t* config) TILEDB_NOEXCEPT {
  return api_entry_context<tiledb::api::tiledb_group_set_config>(
      ctx, group, config);
}

capi_return_t tiledb_group_get_config(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    tiledb_config_t** config) TILEDB_NOEXCEPT {
  return api_entry_context<tiledb::api::tiledb_group_get_config>(
      ctx, group, config);
}

capi_return_t tiledb_group_put_metadata(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    const char* key,
    tiledb_datatype_t value_type,
    uint32_t value_num,
    const void* value) TILEDB_NOEXCEPT {
  return api_entry_context<tiledb::api::tiledb_group_put_metadata>(
      ctx, group, key, value_type, value_num, value);
}

capi_return_t tiledb_group_delete_group(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    const char* uri,
    const uint8_t recursive) TILEDB_NOEXCEPT {
  return api_entry_context<tiledb::api::tiledb_group_delete_group>(
      ctx, group, uri, recursive);
}

capi_return_t tiledb_group_delete_metadata(
    tiledb_ctx_t* ctx, tiledb_group_t* group, const char* key) TILEDB_NOEXCEPT {
  return api_entry_context<tiledb::api::tiledb_group_delete_metadata>(
      ctx, group, key);
}

capi_return_t tiledb_group_get_metadata(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    const char* key,
    tiledb_datatype_t* value_type,
    uint32_t* value_num,
    const void** value) TILEDB_NOEXCEPT {
  return api_entry_context<tiledb::api::tiledb_group_get_metadata>(
      ctx, group, key, value_type, value_num, value);
}

capi_return_t tiledb_group_get_metadata_num(
    tiledb_ctx_t* ctx, tiledb_group_t* group, uint64_t* num) TILEDB_NOEXCEPT {
  return api_entry_context<tiledb::api::tiledb_group_get_metadata_num>(
      ctx, group, num);
}

capi_return_t tiledb_group_get_metadata_from_index(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    uint64_t index,
    const char** key,
    uint32_t* key_len,
    tiledb_datatype_t* value_type,
    uint32_t* value_num,
    const void** value) TILEDB_NOEXCEPT {
  return api_entry_context<tiledb::api::tiledb_group_get_metadata_from_index>(
      ctx, group, index, key, key_len, value_type, value_num, value);
}

capi_return_t tiledb_group_has_metadata_key(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    const char* key,
    tiledb_datatype_t* value_type,
    int32_t* has_key) TILEDB_NOEXCEPT {
  return api_entry_context<tiledb::api::tiledb_group_has_metadata_key>(
      ctx, group, key, value_type, has_key);
}

capi_return_t tiledb_group_add_member(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    const char* uri,
    const uint8_t relative,
    const char* name) TILEDB_NOEXCEPT {
  return api_entry_context<tiledb::api::tiledb_group_add_member>(
      ctx, group, uri, relative, name);
}

capi_return_t tiledb_group_remove_member(
    tiledb_ctx_t* ctx, tiledb_group_t* group, const char* uri) TILEDB_NOEXCEPT {
  return api_entry_context<tiledb::api::tiledb_group_remove_member>(
      ctx, group, uri);
}

capi_return_t tiledb_group_get_member_count(
    tiledb_ctx_t* ctx, tiledb_group_t* group, uint64_t* count) TILEDB_NOEXCEPT {
  return api_entry_context<tiledb::api::tiledb_group_get_member_count>(
      ctx, group, count);
}

capi_return_t tiledb_group_get_member_by_index(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    uint64_t index,
    char** uri,
    tiledb_object_t* type,
    char** name) TILEDB_NOEXCEPT {
  return api_entry_context<tiledb::api::tiledb_group_get_member_by_index>(
      ctx, group, index, uri, type, name);
}

capi_return_t tiledb_group_get_member_by_name(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    const char* name,
    char** uri,
    tiledb_object_t* type) TILEDB_NOEXCEPT {
  return api_entry_context<tiledb::api::tiledb_group_get_member_by_name>(
      ctx, group, name, uri, type);
}

capi_return_t tiledb_group_get_is_relative_uri_by_name(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    const char* name,
    uint8_t* relative) TILEDB_NOEXCEPT {
  return api_entry_context<
      tiledb::api::tiledb_group_get_is_relative_uri_by_name>(
      ctx, group, name, relative);
}

capi_return_t tiledb_group_is_open(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    int32_t* is_open) TILEDB_NOEXCEPT {
  return api_entry_context<tiledb::api::tiledb_group_is_open>(
      ctx, group, is_open);
}

capi_return_t tiledb_group_get_uri(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    const char** group_uri) TILEDB_NOEXCEPT {
  return api_entry_context<tiledb::api::tiledb_group_get_uri>(
      ctx, group, group_uri);
}

capi_return_t tiledb_group_get_query_type(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    tiledb_query_type_t* query_type) TILEDB_NOEXCEPT {
  return api_entry_context<tiledb::api::tiledb_group_get_query_type>(
      ctx, group, query_type);
}

capi_return_t tiledb_group_dump_str(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    char** dump_ascii,
    const uint8_t recursive) TILEDB_NOEXCEPT {
  return api_entry_context<tiledb::api::tiledb_group_dump_str>(
      ctx, group, dump_ascii, recursive);
}

capi_return_t tiledb_serialize_group(
    tiledb_ctx_t* ctx,
    const tiledb_group_t* group,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_buffer_t** buffer_list) TILEDB_NOEXCEPT {
  return api_entry_context<tiledb::api::tiledb_serialize_group>(
      ctx, group, serialize_type, client_side, buffer_list);
}

capi_return_t tiledb_deserialize_group(
    tiledb_ctx_t* ctx,
    const tiledb_buffer_t* buffer,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_group_t* group) TILEDB_NOEXCEPT {
  return api_entry_context<tiledb::api::tiledb_deserialize_group>(
      ctx, buffer, serialize_type, client_side, group);
}

capi_return_t tiledb_serialize_group_metadata(
    tiledb_ctx_t* ctx,
    const tiledb_group_t* group,
    tiledb_serialization_type_t serialization_type,
    tiledb_buffer_t** buffer) TILEDB_NOEXCEPT {
  return api_entry_context<tiledb::api::tiledb_serialize_group_metadata>(
      ctx, group, serialization_type, buffer);
}

capi_return_t tiledb_deserialize_group_metadata(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    tiledb_serialization_type_t serialization_type,
    const tiledb_buffer_t* buffer) TILEDB_NOEXCEPT {
  return api_entry_context<tiledb::api::tiledb_deserialize_group_metadata>(
      ctx, group, serialization_type, buffer);
}

capi_return_t tiledb_group_consolidate_metadata(
    tiledb_ctx_t* ctx,
    const char* group_uri,
    tiledb_config_t* config) TILEDB_NOEXCEPT {
  return api_entry_with_context<tiledb::api::tiledb_group_consolidate_metadata>(
      ctx, group_uri, config);
}

capi_return_t tiledb_group_vacuum_metadata(
    tiledb_ctx_t* ctx,
    const char* group_uri,
    tiledb_config_t* config) TILEDB_NOEXCEPT {
  return api_entry_with_context<tiledb::api::tiledb_group_vacuum_metadata>(
      ctx, group_uri, config);
}
