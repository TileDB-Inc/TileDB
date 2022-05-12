/**
 * @file   tiledb_group.cc
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
 * This file defines the C API of TileDB for groups.
 **/

#include "tiledb/sm/c_api/api_argument_validator.h"
#include "tiledb/sm/c_api/api_exception_safety.h"
#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/c_api/tiledb_experimental.h"
#include "tiledb/sm/c_api/tiledb_serialization.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/group/group_v1.h"
#include "tiledb/sm/serialization/array.h"
#include "tiledb/sm/serialization/group.h"

namespace tiledb::common::detail {

/* ****************************** */
/*              GROUP             */
/* ****************************** */

int32_t tiledb_group_create(tiledb_ctx_t* ctx, const char* group_uri) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  // Check for error
  if (group_uri == nullptr) {
    auto st = Status_Error("Invalid group directory argument is NULL");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  // Create the group
  if (SAVE_ERROR_CATCH(
          ctx, ctx->ctx_->storage_manager()->group_create(group_uri)))
    return TILEDB_ERR;

  // Success
  return TILEDB_OK;
}

int32_t tiledb_group_alloc(
    tiledb_ctx_t* ctx, const char* group_uri, tiledb_group_t** group) {
  if (sanity_check(ctx) == TILEDB_ERR) {
    *group = nullptr;
    return TILEDB_ERR;
  }

  // Create group struct
  *group = new (std::nothrow) tiledb_group_t;
  if (*group == nullptr) {
    auto st = Status_Error(
        "Failed to create TileDB group object; Memory allocation error");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Check group URI
  auto uri = tiledb::sm::URI(group_uri);
  if (uri.is_invalid()) {
    auto st = Status_Error("Failed to create TileDB group object; Invalid URI");
    delete *group;
    *group = nullptr;
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  // Allocate a group object
  (*group)->group_ = tdb_unique_ptr<tiledb::sm::Group>(
      tdb_new(tiledb::sm::GroupV1, uri, ctx->ctx_->storage_manager()));
  if ((*group)->group_ == nullptr) {
    delete *group;
    *group = nullptr;
    auto st = Status_Error(
        "Failed to create TileDB group object; Memory allocation "
        "error");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Success
  return TILEDB_OK;
}

int32_t tiledb_group_open(
    tiledb_ctx_t* ctx, tiledb_group_t* group, tiledb_query_type_t query_type) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, group) == TILEDB_ERR)
    return TILEDB_ERR;

  // Open group
  if (SAVE_ERROR_CATCH(
          ctx,
          group->group_->open(static_cast<tiledb::sm::QueryType>(query_type))))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_group_close(tiledb_ctx_t* ctx, tiledb_group_t* group) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, group) == TILEDB_ERR)
    return TILEDB_ERR;

  // Close group
  if (SAVE_ERROR_CATCH(ctx, group->group_->close()))
    return TILEDB_ERR;

  return TILEDB_OK;
}

void tiledb_group_free(tiledb_group_t** group) {
  if (group != nullptr && *group != nullptr) {
    (*group)->group_.reset(nullptr);
    delete *group;
    *group = nullptr;
  }
}

int32_t tiledb_group_set_config(
    tiledb_ctx_t* ctx, tiledb_group_t* group, tiledb_config_t* config) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, group) == TILEDB_ERR ||
      sanity_check(ctx, config) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(ctx, group->group_->set_config(*(config->config_))))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_group_get_config(
    tiledb_ctx_t* ctx, tiledb_group_t* group, tiledb_config_t** config) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, group) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create a new config struct
  *config = new (std::nothrow) tiledb_config_t;
  if (*config == nullptr)
    return TILEDB_OOM;

  // Get the group config
  (*config)->config_ = new (std::nothrow) tiledb::sm::Config();
  *((*config)->config_) = *group->group_->config();
  if ((*config)->config_ == nullptr) {
    delete (*config);
    *config = nullptr;
    return TILEDB_OOM;
  }

  return TILEDB_OK;
}

int32_t tiledb_group_put_metadata(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    const char* key,
    tiledb_datatype_t value_type,
    uint32_t value_num,
    const void* value) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, group) == TILEDB_ERR)
    return TILEDB_ERR;

  // Put metadata
  if (SAVE_ERROR_CATCH(
          ctx,
          group->group_->put_metadata(
              key,
              static_cast<tiledb::sm::Datatype>(value_type),
              value_num,
              value)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_group_delete_metadata(
    tiledb_ctx_t* ctx, tiledb_group_t* group, const char* key) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, group) == TILEDB_ERR)
    return TILEDB_ERR;

  // Put metadata
  if (SAVE_ERROR_CATCH(ctx, group->group_->delete_metadata(key)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_group_get_metadata(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    const char* key,
    tiledb_datatype_t* value_type,
    uint32_t* value_num,
    const void** value) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, group) == TILEDB_ERR)
    return TILEDB_ERR;

  // Get metadata
  tiledb::sm::Datatype type;
  if (SAVE_ERROR_CATCH(
          ctx, group->group_->get_metadata(key, &type, value_num, value)))
    return TILEDB_ERR;

  *value_type = static_cast<tiledb_datatype_t>(type);

  return TILEDB_OK;
}

int32_t tiledb_group_get_metadata_num(
    tiledb_ctx_t* ctx, tiledb_group_t* group, uint64_t* num) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, group) == TILEDB_ERR)
    return TILEDB_ERR;

  // Get metadata num
  if (SAVE_ERROR_CATCH(ctx, group->group_->get_metadata_num(num)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_group_get_metadata_from_index(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    uint64_t index,
    const char** key,
    uint32_t* key_len,
    tiledb_datatype_t* value_type,
    uint32_t* value_num,
    const void** value) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, group) == TILEDB_ERR)
    return TILEDB_ERR;

  // Get metadata
  tiledb::sm::Datatype type;
  if (SAVE_ERROR_CATCH(
          ctx,
          group->group_->get_metadata(
              index, key, key_len, &type, value_num, value)))
    return TILEDB_ERR;

  *value_type = static_cast<tiledb_datatype_t>(type);

  return TILEDB_OK;
}

int32_t tiledb_group_has_metadata_key(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    const char* key,
    tiledb_datatype_t* value_type,
    int32_t* has_key) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, group) == TILEDB_ERR)
    return TILEDB_ERR;

  // Check whether metadata has_key
  bool has_the_key;
  tiledb::sm::Datatype type;
  if (SAVE_ERROR_CATCH(
          ctx, group->group_->has_metadata_key(key, &type, &has_the_key)))
    return TILEDB_ERR;

  *has_key = has_the_key ? 1 : 0;
  if (has_the_key) {
    *value_type = static_cast<tiledb_datatype_t>(type);
  }
  return TILEDB_OK;
}

int32_t tiledb_serialize_group(
    tiledb_ctx_t* ctx,
    const tiledb_group_t* group,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_buffer_t** buffer) {
  // client_side is currently unused
  (void)client_side;
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, group) == TILEDB_ERR)
    return TILEDB_ERR;

  // Allocate a buffer list
  if (tiledb_buffer_alloc(ctx, buffer) != TILEDB_OK ||
      sanity_check(ctx, *buffer) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::group_serialize(
              group->group_.get(),
              (tiledb::sm::SerializationType)serialize_type,
              (*buffer)->buffer_))) {
    tiledb_buffer_free(buffer);
    return TILEDB_ERR;
  }

  return TILEDB_OK;
}

int32_t tiledb_deserialize_group(
    tiledb_ctx_t* ctx,
    const tiledb_buffer_t* buffer,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_group_t* group) {
  // client_side is currently unused
  (void)client_side;
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, group) == TILEDB_ERR ||
      sanity_check(ctx, buffer) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::group_deserialize(
              group->group_.get(),
              (tiledb::sm::SerializationType)serialize_type,
              *buffer->buffer_)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_group_add_member(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    const char* uri,
    const uint8_t relative,
    const char* name) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, group) == TILEDB_ERR)
    return TILEDB_ERR;

  std::optional<std::string> name_optional = std::nullopt;
  if (name != nullptr) {
    name_optional = name;
  }

  if (SAVE_ERROR_CATCH(
          ctx,
          group->group_->mark_member_for_addition(
              tiledb::sm::URI(uri, !relative), relative, name_optional)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_group_remove_member(
    tiledb_ctx_t* ctx, tiledb_group_t* group, const char* uri) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, group) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(ctx, group->group_->mark_member_for_removal(uri)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_group_get_member_count(
    tiledb_ctx_t* ctx, tiledb_group_t* group, uint64_t* count) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, group) == TILEDB_ERR)
    return TILEDB_ERR;

  try {
    auto&& [st, member_count] = group->group_->member_count();
    if (!st.ok()) {
      save_error(ctx, st);
      return TILEDB_ERR;
    }

    *count = member_count.value();

    return TILEDB_OK;
  } catch (const std::exception& e) {
    auto st = Status_Error(
        std::string("Internal TileDB uncaught exception; ") + e.what());
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  return TILEDB_ERR;
}

int32_t tiledb_group_get_member_by_index(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    uint64_t index,
    char** uri,
    tiledb_object_t* type,
    char** name) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, group) == TILEDB_ERR)
    return TILEDB_ERR;

  try {
    auto&& [st, uri_str, object_type, name_str] =
        group->group_->member_by_index(index);
    if (!st.ok()) {
      save_error(ctx, st);
      return TILEDB_ERR;
    }

    *type = static_cast<tiledb_object_t>(object_type.value());
    *uri = static_cast<char*>(std::malloc(uri_str.value().size() + 1));
    if (*uri == nullptr)
      return TILEDB_ERR;

    std::memcpy(*uri, uri_str.value().data(), uri_str.value().size());
    (*uri)[uri_str.value().size()] = '\0';

    *name = nullptr;
    if (name_str.has_value()) {
      *name = static_cast<char*>(std::malloc(name_str.value().size() + 1));
      if (*name == nullptr)
        return TILEDB_ERR;

      std::memcpy(*name, name_str.value().data(), name_str.value().size());
      (*name)[name_str.value().size()] = '\0';
    }

    return TILEDB_OK;
  } catch (const std::exception& e) {
    auto st = Status_Error(
        std::string("Internal TileDB uncaught exception; ") + e.what());
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  return TILEDB_ERR;
}

int32_t tiledb_group_get_member_by_name(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    const char* name,
    char** uri,
    tiledb_object_t* type) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, group) == TILEDB_ERR)
    return TILEDB_ERR;

  try {
    auto&& [st, uri_str, object_type, name_str] =
        group->group_->member_by_name(name);
    if (!st.ok()) {
      save_error(ctx, st);
      return TILEDB_ERR;
    }

    *type = static_cast<tiledb_object_t>(object_type.value());
    *uri = static_cast<char*>(std::malloc(uri_str.value().size() + 1));
    if (*uri == nullptr)
      return TILEDB_ERR;

    std::memcpy(*uri, uri_str.value().data(), uri_str.value().size());
    (*uri)[uri_str.value().size()] = '\0';

    return TILEDB_OK;
  } catch (const std::exception& e) {
    auto st = Status_Error(
        std::string("Internal TileDB uncaught exception; ") + e.what());
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  return TILEDB_ERR;
}

int32_t tiledb_group_get_uri(
    tiledb_ctx_t* ctx, tiledb_group_t* group, const char** group_uri) {
  // Sanity checks
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, group) == TILEDB_ERR)
    return TILEDB_ERR;

  *group_uri = group->group_->group_uri().c_str();

  return TILEDB_OK;
}

int32_t tiledb_group_get_query_type(
    tiledb_ctx_t* ctx, tiledb_group_t* group, tiledb_query_type_t* query_type) {
  // Sanity checks
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, group) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  // Get query_type
  tiledb::sm::QueryType type;
  if (SAVE_ERROR_CATCH(ctx, group->group_->get_query_type(&type)))
    return TILEDB_ERR;

  *query_type = static_cast<tiledb_query_type_t>(type);

  return TILEDB_OK;
}

int32_t tiledb_group_is_open(
    tiledb_ctx_t* ctx, tiledb_group_t* group, int32_t* is_open) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, group) == TILEDB_ERR)
    return TILEDB_ERR;

  *is_open = (int32_t)group->group_->is_open();

  return TILEDB_OK;
}

int32_t tiledb_group_dump_str(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    char** dump_ascii,
    const uint8_t recursive) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  if (dump_ascii == nullptr)
    return TILEDB_ERR;

  const std::string str = group->group_->dump(2, 0, recursive);

  *dump_ascii = static_cast<char*>(std::malloc(str.size() + 1));
  if (*dump_ascii == nullptr)
    return TILEDB_ERR;

  std::memcpy(*dump_ascii, str.data(), str.size());
  (*dump_ascii)[str.size()] = '\0';

  return TILEDB_OK;
}

int32_t tiledb_serialize_group_metadata(
    tiledb_ctx_t* ctx,
    const tiledb_group_t* group,
    tiledb_serialization_type_t serialize_type,
    tiledb_buffer_t** buffer) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, group) == TILEDB_ERR)
    return TILEDB_ERR;

  // Allocate buffer
  if (tiledb_buffer_alloc(ctx, buffer) != TILEDB_OK ||
      sanity_check(ctx, *buffer) == TILEDB_ERR)
    return TILEDB_ERR;

  // Get metadata to serialize, this will load it if it does not exist
  tiledb::sm::Metadata* metadata;
  if (SAVE_ERROR_CATCH(ctx, group->group_->metadata(&metadata))) {
    return TILEDB_ERR;
  }

  // Serialize
  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::metadata_serialize(
              metadata,
              (tiledb::sm::SerializationType)serialize_type,
              (*buffer)->buffer_))) {
    tiledb_buffer_free(buffer);
    return TILEDB_ERR;
  }

  return TILEDB_OK;
}

int32_t tiledb_deserialize_group_metadata(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    tiledb_serialization_type_t serialize_type,
    const tiledb_buffer_t* buffer) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, group) == TILEDB_ERR ||
      sanity_check(ctx, buffer) == TILEDB_ERR)
    return TILEDB_ERR;

  // Deserialize
  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::metadata_deserialize(
              group->group_->unsafe_metadata(),
              (tiledb::sm::SerializationType)serialize_type,
              *(buffer->buffer_)))) {
    return TILEDB_ERR;
  }

  return TILEDB_OK;
}

int32_t tiledb_group_consolidate_metadata(
    tiledb_ctx_t* ctx, const char* group_uri, tiledb_config_t* config) {
  // Sanity checks
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(
          ctx,
          ctx->ctx_->storage_manager()->group_metadata_consolidate(
              group_uri,
              (config == nullptr) ? &ctx->ctx_->storage_manager()->config() :
                                    config->config_)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

}  // namespace tiledb::common::detail

int32_t tiledb_group_create(tiledb_ctx_t* ctx, const char* group_uri)
    TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_group_create>(ctx, group_uri);
}

TILEDB_EXPORT int32_t tiledb_group_alloc(
    tiledb_ctx_t* ctx,
    const char* group_uri,
    tiledb_group_t** group) TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_group_alloc>(ctx, group_uri, group);
}

TILEDB_EXPORT int32_t tiledb_group_open(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    tiledb_query_type_t query_type) TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_group_open>(ctx, group, query_type);
}

TILEDB_EXPORT int32_t
tiledb_group_close(tiledb_ctx_t* ctx, tiledb_group_t* group) TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_group_close>(ctx, group);
}

TILEDB_EXPORT void tiledb_group_free(tiledb_group_t** group) TILEDB_NOEXCEPT {
  return api_entry_void<detail::tiledb_group_free>(group);
}

TILEDB_EXPORT int32_t tiledb_group_set_config(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    tiledb_config_t* config) TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_group_set_config>(ctx, group, config);
}

TILEDB_EXPORT int32_t tiledb_group_get_config(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    tiledb_config_t** config) TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_group_get_config>(ctx, group, config);
}

TILEDB_EXPORT int32_t tiledb_group_put_metadata(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    const char* key,
    tiledb_datatype_t value_type,
    uint32_t value_num,
    const void* value) TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_group_put_metadata>(
      ctx, group, key, value_type, value_num, value);
}

TILEDB_EXPORT int32_t tiledb_group_delete_metadata(
    tiledb_ctx_t* ctx, tiledb_group_t* group, const char* key) TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_group_delete_metadata>(ctx, group, key);
}

TILEDB_EXPORT int32_t tiledb_group_get_metadata(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    const char* key,
    tiledb_datatype_t* value_type,
    uint32_t* value_num,
    const void** value) TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_group_get_metadata>(
      ctx, group, key, value_type, value_num, value);
}

TILEDB_EXPORT int32_t tiledb_group_get_metadata_num(
    tiledb_ctx_t* ctx, tiledb_group_t* group, uint64_t* num) TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_group_get_metadata_num>(ctx, group, num);
}

TILEDB_EXPORT int32_t tiledb_group_get_metadata_from_index(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    uint64_t index,
    const char** key,
    uint32_t* key_len,
    tiledb_datatype_t* value_type,
    uint32_t* value_num,
    const void** value) TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_group_get_metadata_from_index>(
      ctx, group, index, key, key_len, value_type, value_num, value);
}

TILEDB_EXPORT int32_t tiledb_group_has_metadata_key(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    const char* key,
    tiledb_datatype_t* value_type,
    int32_t* has_key) TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_group_has_metadata_key>(
      ctx, group, key, value_type, has_key);
}

TILEDB_EXPORT int32_t tiledb_group_add_member(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    const char* uri,
    const uint8_t relative,
    const char* name) TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_group_add_member>(
      ctx, group, uri, relative, name);
}

TILEDB_EXPORT int32_t tiledb_group_remove_member(
    tiledb_ctx_t* ctx, tiledb_group_t* group, const char* uri) TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_group_remove_member>(ctx, group, uri);
}

TILEDB_EXPORT int32_t tiledb_group_get_member_count(
    tiledb_ctx_t* ctx, tiledb_group_t* group, uint64_t* count) TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_group_get_member_count>(ctx, group, count);
}

TILEDB_EXPORT int32_t tiledb_group_get_member_by_index(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    uint64_t index,
    char** uri,
    tiledb_object_t* type,
    char** name) TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_group_get_member_by_index>(
      ctx, group, index, uri, type, name);
}

TILEDB_EXPORT int32_t tiledb_group_get_member_by_name(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    const char* name,
    char** uri,
    tiledb_object_t* type) TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_group_get_member_by_name>(
      ctx, group, name, uri, type);
}

TILEDB_EXPORT int32_t tiledb_group_is_open(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    int32_t* is_open) TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_group_is_open>(ctx, group, is_open);
}

TILEDB_EXPORT int32_t tiledb_group_get_uri(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    const char** group_uri) TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_group_get_uri>(ctx, group, group_uri);
}

TILEDB_EXPORT int32_t tiledb_group_get_query_type(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    tiledb_query_type_t* query_type) TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_group_get_query_type>(ctx, group, query_type);
}

TILEDB_EXPORT int32_t tiledb_group_dump_str(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    char** dump_ascii,
    const uint8_t recursive) TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_group_dump_str>(
      ctx, group, dump_ascii, recursive);
}

TILEDB_EXPORT int32_t tiledb_serialize_group_metadata(
    tiledb_ctx_t* ctx,
    const tiledb_group_t* group,
    tiledb_serialization_type_t serialization_type,
    tiledb_buffer_t** buffer) TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_serialize_group_metadata>(
      ctx, group, serialization_type, buffer);
}

TILEDB_EXPORT int32_t tiledb_deserialize_group_metadata(
    tiledb_ctx_t* ctx,
    tiledb_group_t* group,
    tiledb_serialization_type_t serialization_type,
    const tiledb_buffer_t* buffer) TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_deserialize_group_metadata>(
      ctx, group, serialization_type, buffer);
}

TILEDB_EXPORT int32_t tiledb_serialize_group(
    tiledb_ctx_t* ctx,
    const tiledb_group_t* group,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_buffer_t** buffer_list) TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_serialize_group>(
      ctx, group, serialize_type, client_side, buffer_list);
}

TILEDB_EXPORT int32_t tiledb_deserialize_group(
    tiledb_ctx_t* ctx,
    const tiledb_buffer_t* buffer,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_group_t* group) TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_deserialize_group>(
      ctx, buffer, serialize_type, client_side, group);
}

TILEDB_EXPORT int32_t tiledb_group_consolidate_metadata(
    tiledb_ctx_t* ctx,
    const char* group_uri,
    tiledb_config_t* config) TILEDB_NOEXCEPT {
  return api_entry<detail::tiledb_group_consolidate_metadata>(
      ctx, group_uri, config);
}
