/**
 * @file   tiledb.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB, Inc.
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
 * This file defines the C API of TileDB.
 */

#include "tiledb/sm/c_api/tiledb.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/c_api/tiledb_serialization.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/config/config_iter.h"
#include "tiledb/sm/cpp_api/core_interface.h"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/filesystem.h"
#include "tiledb/sm/enums/filter_option.h"
#include "tiledb/sm/enums/filter_type.h"
#include "tiledb/sm/enums/object_type.h"
#include "tiledb/sm/enums/query_status.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/enums/vfs_mode.h"
#include "tiledb/sm/filesystem/vfs.h"
#include "tiledb/sm/filesystem/vfs_file_handle.h"
#include "tiledb/sm/filter/compression_filter.h"
#include "tiledb/sm/filter/filter_pipeline.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/sm/rest/rest_client.h"
#include "tiledb/sm/serialization/array_schema.h"
#include "tiledb/sm/serialization/query.h"
#include "tiledb/sm/stats/stats.h"
#include "tiledb/sm/storage_manager/context.h"
#include "tiledb/sm/subarray/subarray.h"
#include "tiledb/sm/subarray/subarray_partitioner.h"

#include <map>
#include <memory>
#include <sstream>

/* ****************************** */
/*       ENUMS TO/FROM STR        */
/* ****************************** */

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

int32_t tiledb_object_type_to_str(
    tiledb_object_t object_type, const char** str) {
  const auto& strval =
      tiledb::sm::object_type_str((tiledb::sm::ObjectType)object_type);
  *str = strval.c_str();
  return strval.empty() ? TILEDB_ERR : TILEDB_OK;
}

int32_t tiledb_object_type_from_str(
    const char* str, tiledb_object_t* object_type) {
  tiledb::sm::ObjectType val = tiledb::sm::ObjectType::INVALID;
  if (!tiledb::sm::object_type_enum(str, &val).ok())
    return TILEDB_ERR;
  *object_type = (tiledb_object_t)val;
  return TILEDB_OK;
}

int32_t tiledb_filesystem_to_str(
    tiledb_filesystem_t filesystem, const char** str) {
  const auto& strval =
      tiledb::sm::filesystem_str((tiledb::sm::Filesystem)filesystem);
  *str = strval.c_str();
  return strval.empty() ? TILEDB_ERR : TILEDB_OK;
}

int32_t tiledb_filesystem_from_str(
    const char* str, tiledb_filesystem_t* filesystem) {
  tiledb::sm::Filesystem val = tiledb::sm::Filesystem::S3;
  if (!tiledb::sm::filesystem_enum(str, &val).ok())
    return TILEDB_ERR;
  *filesystem = (tiledb_filesystem_t)val;
  return TILEDB_OK;
}

int32_t tiledb_datatype_to_str(tiledb_datatype_t datatype, const char** str) {
  const auto& strval = tiledb::sm::datatype_str((tiledb::sm::Datatype)datatype);
  *str = strval.c_str();
  return strval.empty() ? TILEDB_ERR : TILEDB_OK;
}

int32_t tiledb_datatype_from_str(const char* str, tiledb_datatype_t* datatype) {
  tiledb::sm::Datatype val = tiledb::sm::Datatype::UINT8;
  if (!tiledb::sm::datatype_enum(str, &val).ok())
    return TILEDB_ERR;
  *datatype = (tiledb_datatype_t)val;
  return TILEDB_OK;
}

int32_t tiledb_array_type_to_str(
    tiledb_array_type_t array_type, const char** str) {
  const auto& strval =
      tiledb::sm::array_type_str((tiledb::sm::ArrayType)array_type);
  *str = strval.c_str();
  return strval.empty() ? TILEDB_ERR : TILEDB_OK;
}

int32_t tiledb_array_type_from_str(
    const char* str, tiledb_array_type_t* array_type) {
  tiledb::sm::ArrayType val = tiledb::sm::ArrayType::DENSE;
  if (!tiledb::sm::array_type_enum(str, &val).ok())
    return TILEDB_ERR;
  *array_type = (tiledb_array_type_t)val;
  return TILEDB_OK;
}

int32_t tiledb_layout_to_str(tiledb_layout_t layout, const char** str) {
  const auto& strval = tiledb::sm::layout_str((tiledb::sm::Layout)layout);
  *str = strval.c_str();
  return strval.empty() ? TILEDB_ERR : TILEDB_OK;
}

int32_t tiledb_layout_from_str(const char* str, tiledb_layout_t* layout) {
  tiledb::sm::Layout val = tiledb::sm::Layout::ROW_MAJOR;
  if (!tiledb::sm::layout_enum(str, &val).ok())
    return TILEDB_ERR;
  *layout = (tiledb_layout_t)val;
  return TILEDB_OK;
}

int32_t tiledb_filter_type_to_str(
    tiledb_filter_type_t filter_type, const char** str) {
  const auto& strval =
      tiledb::sm::filter_type_str((tiledb::sm::FilterType)filter_type);
  *str = strval.c_str();
  return strval.empty() ? TILEDB_ERR : TILEDB_OK;
}

int32_t tiledb_filter_type_from_str(
    const char* str, tiledb_filter_type_t* filter_type) {
  tiledb::sm::FilterType val = tiledb::sm::FilterType::FILTER_NONE;
  if (!tiledb::sm::filter_type_enum(str, &val).ok())
    return TILEDB_ERR;
  *filter_type = (tiledb_filter_type_t)val;
  return TILEDB_OK;
}

int32_t tiledb_filter_option_to_str(
    tiledb_filter_option_t filter_option, const char** str) {
  const auto& strval =
      tiledb::sm::filter_option_str((tiledb::sm::FilterOption)filter_option);
  *str = strval.c_str();
  return strval.empty() ? TILEDB_ERR : TILEDB_OK;
}

int32_t tiledb_filter_option_from_str(
    const char* str, tiledb_filter_option_t* filter_option) {
  tiledb::sm::FilterOption val = tiledb::sm::FilterOption::COMPRESSION_LEVEL;
  if (!tiledb::sm::filter_option_enum(str, &val).ok())
    return TILEDB_ERR;
  *filter_option = (tiledb_filter_option_t)val;
  return TILEDB_OK;
}

int32_t tiledb_encryption_type_to_str(
    tiledb_encryption_type_t encryption_type, const char** str) {
  const auto& strval = tiledb::sm::encryption_type_str(
      (tiledb::sm::EncryptionType)encryption_type);
  *str = strval.c_str();
  return strval.empty() ? TILEDB_ERR : TILEDB_OK;
}

int32_t tiledb_encryption_type_from_str(
    const char* str, tiledb_encryption_type_t* encryption_type) {
  tiledb::sm::EncryptionType val = tiledb::sm::EncryptionType::NO_ENCRYPTION;
  if (!tiledb::sm::encryption_type_enum(str, &val).ok())
    return TILEDB_ERR;
  *encryption_type = (tiledb_encryption_type_t)val;
  return TILEDB_OK;
}

int32_t tiledb_query_status_to_str(
    tiledb_query_status_t query_status, const char** str) {
  const auto& strval =
      tiledb::sm::query_status_str((tiledb::sm::QueryStatus)query_status);
  *str = strval.c_str();
  return strval.empty() ? TILEDB_ERR : TILEDB_OK;
}

int32_t tiledb_query_status_from_str(
    const char* str, tiledb_query_status_t* query_status) {
  tiledb::sm::QueryStatus val = tiledb::sm::QueryStatus::UNINITIALIZED;
  if (!tiledb::sm::query_status_enum(str, &val).ok())
    return TILEDB_ERR;
  *query_status = (tiledb_query_status_t)val;
  return TILEDB_OK;
}

int32_t tiledb_serialization_type_to_str(
    tiledb_serialization_type_t serialization_type, const char** str) {
  const auto& strval = tiledb::sm::serialization_type_str(
      (tiledb::sm::SerializationType)serialization_type);
  *str = strval.c_str();
  return strval.empty() ? TILEDB_ERR : TILEDB_OK;
}

int32_t tiledb_serialization_type_from_str(
    const char* str, tiledb_serialization_type_t* serialization_type) {
  tiledb::sm::SerializationType val = tiledb::sm::SerializationType::CAPNP;
  if (!tiledb::sm::serialization_type_enum(str, &val).ok())
    return TILEDB_ERR;
  *serialization_type = (tiledb_serialization_type_t)val;
  return TILEDB_OK;
}

int32_t tiledb_walk_order_to_str(
    tiledb_walk_order_t walk_order, const char** str) {
  const auto& strval =
      tiledb::sm::walkorder_str((tiledb::sm::WalkOrder)walk_order);
  *str = strval.c_str();
  return strval.empty() ? TILEDB_ERR : TILEDB_OK;
}

int32_t tiledb_walk_order_from_str(
    const char* str, tiledb_walk_order_t* walk_order) {
  tiledb::sm::WalkOrder val = tiledb::sm::WalkOrder::PREORDER;
  if (!tiledb::sm::walkorder_enum(str, &val).ok())
    return TILEDB_ERR;
  *walk_order = (tiledb_walk_order_t)val;
  return TILEDB_OK;
}

int32_t tiledb_vfs_mode_to_str(tiledb_vfs_mode_t vfs_mode, const char** str) {
  const auto& strval = tiledb::sm::vfsmode_str((tiledb::sm::VFSMode)vfs_mode);
  *str = strval.c_str();
  return strval.empty() ? TILEDB_ERR : TILEDB_OK;
}

int32_t tiledb_vfs_mode_from_str(const char* str, tiledb_vfs_mode_t* vfs_mode) {
  tiledb::sm::VFSMode val = tiledb::sm::VFSMode::VFS_READ;
  if (!tiledb::sm::vfsmode_enum(str, &val).ok())
    return TILEDB_ERR;
  *vfs_mode = (tiledb_vfs_mode_t)val;
  return TILEDB_OK;
}

/* ****************************** */
/*            CONSTANTS           */
/* ****************************** */

const char* tiledb_coords() {
  return tiledb::sm::constants::coords.c_str();
}

uint32_t tiledb_var_num() {
  return tiledb::sm::constants::var_num;
}

uint32_t tiledb_max_path() {
  return tiledb::sm::constants::path_max_len;
}

uint64_t tiledb_offset_size() {
  return tiledb::sm::constants::cell_var_offset_size;
}

uint64_t tiledb_datatype_size(tiledb_datatype_t type) {
  return tiledb::sm::datatype_size(static_cast<tiledb::sm::Datatype>(type));
}

uint64_t tiledb_timestamp_now_ms() {
  return tiledb::sm::utils::time::timestamp_now_ms();
}

/* ****************************** */
/*            VERSION             */
/* ****************************** */

void tiledb_version(int32_t* major, int32_t* minor, int32_t* rev) {
  *major = tiledb::sm::constants::library_version[0];
  *minor = tiledb::sm::constants::library_version[1];
  *rev = tiledb::sm::constants::library_version[2];
}

/* ********************************* */
/*         AUXILIARY FUNCTIONS       */
/* ********************************* */

/* Saves a status inside the context object. */
static bool save_error(tiledb_ctx_t* ctx, const tiledb::sm::Status& st) {
  // No error
  if (st.ok())
    return false;

  // Store new error
  ctx->ctx_->save_error(st);

  // There is an error
  return true;
}

static bool create_error(tiledb_error_t** error, const tiledb::sm::Status& st) {
  if (st.ok())
    return false;

  (*error) = new (std::nothrow) tiledb_error_t;
  if (*error == nullptr)
    return true;
  (*error)->errmsg_ = st.to_string();

  return true;
}

inline int32_t sanity_check(tiledb_ctx_t* ctx, const tiledb_array_t* array) {
  if (array == nullptr || array->array_ == nullptr) {
    auto st = tiledb::sm::Status::Error("Invalid TileDB array object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int32_t sanity_check(tiledb_ctx_t* ctx, const tiledb_buffer_t* buffer) {
  if (buffer == nullptr || buffer->buffer_ == nullptr) {
    auto st = tiledb::sm::Status::Error("Invalid TileDB buffer object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int32_t sanity_check(
    tiledb_ctx_t* ctx, const tiledb_buffer_list_t* buffer_list) {
  if (buffer_list == nullptr || buffer_list->buffer_list_ == nullptr) {
    auto st = tiledb::sm::Status::Error("Invalid TileDB buffer list object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int32_t sanity_check(tiledb_config_t* config, tiledb_error_t** error) {
  if (config == nullptr || config->config_ == nullptr) {
    auto st =
        tiledb::sm::Status::Error("Cannot set config; Invalid config object");
    LOG_STATUS(st);
    create_error(error, st);
    return TILEDB_ERR;
  }

  *error = nullptr;
  return TILEDB_OK;
}

inline int32_t sanity_check(
    tiledb_config_iter_t* config_iter, tiledb_error_t** error) {
  if (config_iter == nullptr || config_iter->config_iter_ == nullptr) {
    auto st = tiledb::sm::Status::Error(
        "Cannot set config; Invalid config iterator object");
    LOG_STATUS(st);
    create_error(error, st);
    return TILEDB_ERR;
  }

  *error = nullptr;
  return TILEDB_OK;
}

inline int32_t sanity_check(tiledb_ctx_t* ctx) {
  if (ctx == nullptr)
    return TILEDB_ERR;
  if (ctx->ctx_ == nullptr || ctx->ctx_->storage_manager() == nullptr) {
    auto st = tiledb::sm::Status::Error("Invalid TileDB context");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int32_t sanity_check(tiledb_ctx_t* ctx, const tiledb_error_t* err) {
  if (err == nullptr) {
    auto st = tiledb::sm::Status::Error("Invalid TileDB error object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int32_t sanity_check(tiledb_ctx_t* ctx, const tiledb_attribute_t* attr) {
  if (attr == nullptr || attr->attr_ == nullptr) {
    auto st = tiledb::sm::Status::Error("Invalid TileDB attribute object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int32_t sanity_check(tiledb_ctx_t* ctx, const tiledb_filter_t* filter) {
  if (filter == nullptr || filter->filter_ == nullptr) {
    auto st = tiledb::sm::Status::Error("Invalid TileDB filter object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int32_t sanity_check(
    tiledb_ctx_t* ctx, const tiledb_filter_list_t* filter_list) {
  if (filter_list == nullptr || filter_list->pipeline_ == nullptr) {
    auto st = tiledb::sm::Status::Error("Invalid TileDB filter list object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int32_t sanity_check(tiledb_ctx_t* ctx, const tiledb_dimension_t* dim) {
  if (dim == nullptr || dim->dim_ == nullptr) {
    auto st = tiledb::sm::Status::Error("Invalid TileDB dimension object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int32_t sanity_check(
    tiledb_ctx_t* ctx, const tiledb_array_schema_t* array_schema) {
  if (array_schema == nullptr || array_schema->array_schema_ == nullptr) {
    auto st = tiledb::sm::Status::Error("Invalid TileDB array schema object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int32_t sanity_check(tiledb_ctx_t* ctx, const tiledb_domain_t* domain) {
  if (domain == nullptr || domain->domain_ == nullptr) {
    auto st = tiledb::sm::Status::Error("Invalid TileDB domain object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int32_t sanity_check(tiledb_ctx_t* ctx, const tiledb_query_t* query) {
  if (query == nullptr || query->query_ == nullptr) {
    auto st = tiledb::sm::Status::Error("Invalid TileDB query object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int32_t sanity_check(tiledb_ctx_t* ctx, const tiledb_vfs_t* vfs) {
  if (vfs == nullptr || vfs->vfs_ == nullptr) {
    auto st =
        tiledb::sm::Status::Error("Invalid TileDB virtual filesystem object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int32_t sanity_check(tiledb_ctx_t* ctx, const tiledb_vfs_fh_t* fh) {
  if (fh == nullptr || fh->vfs_fh_ == nullptr) {
    auto st = tiledb::sm::Status::Error(
        "Invalid TileDB virtual filesystem file handle");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int32_t check_filter_type(
    tiledb_ctx_t* ctx, tiledb_filter_t* filter, tiledb_filter_type_t type) {
  auto cpp_type = static_cast<tiledb::sm::FilterType>(type);
  if (filter->filter_->type() != cpp_type) {
    auto st = tiledb::sm::Status::FilterError(
        "Invalid filter type (expected " + filter_type_str(cpp_type) + ")");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

/**
 * Helper macro similar to save_error() that catches all exceptions when
 * executing 'stmt'.
 *
 * @param ctx TileDB context
 * @param stmt Statement to execute
 */
#define SAVE_ERROR_CATCH(ctx, stmt)                                        \
  [&]() {                                                                  \
    auto _s = tiledb::sm::Status::Ok();                                    \
    try {                                                                  \
      _s = (stmt);                                                         \
    } catch (const std::exception& e) {                                    \
      auto st = tiledb::sm::Status::Error(                                 \
          std::string("Internal TileDB uncaught exception; ") + e.what()); \
      LOG_STATUS(st);                                                      \
      save_error(ctx, st);                                                 \
      return true;                                                         \
    }                                                                      \
    return save_error(ctx, _s);                                            \
  }()

/** For debugging, use this definition instead to not catch exceptions. */
//#define SAVE_ERROR_CATCH(ctx, stmt) save_error(ctx, (stmt))

/* ********************************* */
/*              ERROR                */
/* ********************************* */

int32_t tiledb_error_message(tiledb_error_t* err, const char** errmsg) {
  if (err == nullptr)
    return TILEDB_ERR;
  if (err->errmsg_.empty())
    *errmsg = nullptr;
  else
    *errmsg = err->errmsg_.c_str();
  return TILEDB_OK;
}

void tiledb_error_free(tiledb_error_t** err) {
  if (err != nullptr && *err != nullptr) {
    delete (*err);
    *err = nullptr;
  }
}

/* ********************************* */
/*              BUFFER               */
/* ********************************* */

int32_t tiledb_buffer_alloc(tiledb_ctx_t* ctx, tiledb_buffer_t** buffer) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create a buffer struct
  *buffer = new (std::nothrow) tiledb_buffer_t;
  if (*buffer == nullptr) {
    auto st =
        tiledb::sm::Status::Error("Failed to allocate TileDB buffer object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Create a new buffer object
  (*buffer)->buffer_ = new (std::nothrow) tiledb::sm::Buffer();
  if ((*buffer)->buffer_ == nullptr) {
    delete *buffer;
    auto st =
        tiledb::sm::Status::Error("Failed to allocate TileDB buffer object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Success
  return TILEDB_OK;
}

void tiledb_buffer_free(tiledb_buffer_t** buffer) {
  if (buffer != nullptr && *buffer != nullptr) {
    delete (*buffer)->buffer_;
    delete (*buffer);
    *buffer = nullptr;
  }
}

int32_t tiledb_buffer_set_type(
    tiledb_ctx_t* ctx, tiledb_buffer_t* buffer, tiledb_datatype_t datatype) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, buffer) == TILEDB_ERR)
    return TILEDB_ERR;

  buffer->datatype_ = static_cast<tiledb::sm::Datatype>(datatype);

  return TILEDB_OK;
}

int32_t tiledb_buffer_get_type(
    tiledb_ctx_t* ctx,
    const tiledb_buffer_t* buffer,
    tiledb_datatype_t* datatype) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, buffer) == TILEDB_ERR)
    return TILEDB_ERR;

  *datatype = static_cast<tiledb_datatype_t>(buffer->datatype_);

  return TILEDB_OK;
}

int32_t tiledb_buffer_get_data(
    tiledb_ctx_t* ctx,
    const tiledb_buffer_t* buffer,
    void** data,
    uint64_t* num_bytes) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, buffer) == TILEDB_ERR)
    return TILEDB_ERR;

  *data = buffer->buffer_->data();
  *num_bytes = buffer->buffer_->size();

  return TILEDB_OK;
}

int32_t tiledb_buffer_set_data(
    tiledb_ctx_t* ctx, tiledb_buffer_t* buffer, void* data, uint64_t size) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, buffer) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create a temporary Buffer object as a wrapper.
  tiledb::sm::Buffer tmp_buffer(data, size);

  // Swap with the given buffer.
  if (SAVE_ERROR_CATCH(ctx, buffer->buffer_->swap(tmp_buffer)))
    return TILEDB_ERR;

  // 'tmp_buffer' now destructs, freeing the old allocation (if any) of the
  // given buffer.

  return TILEDB_OK;
}

/* ********************************* */
/*            BUFFER LIST            */
/* ********************************* */

int32_t tiledb_buffer_list_alloc(
    tiledb_ctx_t* ctx, tiledb_buffer_list_t** buffer_list) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create a buffer list struct
  *buffer_list = new (std::nothrow) tiledb_buffer_list_t;
  if (*buffer_list == nullptr) {
    auto st = tiledb::sm::Status::Error(
        "Failed to allocate TileDB buffer list object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Create a new buffer_list object
  (*buffer_list)->buffer_list_ = new (std::nothrow) tiledb::sm::BufferList;
  if ((*buffer_list)->buffer_list_ == nullptr) {
    delete *buffer_list;
    auto st = tiledb::sm::Status::Error(
        "Failed to allocate TileDB buffer list object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Success
  return TILEDB_OK;
}

void tiledb_buffer_list_free(tiledb_buffer_list_t** buffer_list) {
  if (buffer_list != nullptr && *buffer_list != nullptr) {
    delete (*buffer_list)->buffer_list_;
    delete (*buffer_list);
    *buffer_list = nullptr;
  }
}

int32_t tiledb_buffer_list_get_num_buffers(
    tiledb_ctx_t* ctx,
    const tiledb_buffer_list_t* buffer_list,
    uint64_t* num_buffers) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, buffer_list) == TILEDB_ERR)
    return TILEDB_ERR;

  *num_buffers = buffer_list->buffer_list_->num_buffers();

  return TILEDB_OK;
}

int32_t tiledb_buffer_list_get_buffer(
    tiledb_ctx_t* ctx,
    const tiledb_buffer_list_t* buffer_list,
    uint64_t buffer_idx,
    tiledb_buffer_t** buffer) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, buffer_list) == TILEDB_ERR)
    return TILEDB_ERR;

  // Get the underlying buffer
  tiledb::sm::Buffer* b;
  if (SAVE_ERROR_CATCH(
          ctx, buffer_list->buffer_list_->get_buffer(buffer_idx, &b)))
    return TILEDB_ERR;

  // Create a buffer struct
  *buffer = new (std::nothrow) tiledb_buffer_t;
  if (*buffer == nullptr) {
    auto st =
        tiledb::sm::Status::Error("Failed to allocate TileDB buffer object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Set the buffer pointer to a non-owning wrapper of the underlying buffer
  (*buffer)->buffer_ =
      new (std::nothrow) tiledb::sm::Buffer(b->data(), b->size());
  if ((*buffer)->buffer_ == nullptr) {
    delete *buffer;
    auto st =
        tiledb::sm::Status::Error("Failed to allocate TileDB buffer object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  return TILEDB_OK;
}

int32_t tiledb_buffer_list_get_total_size(
    tiledb_ctx_t* ctx,
    const tiledb_buffer_list_t* buffer_list,
    uint64_t* total_size) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, buffer_list) == TILEDB_ERR)
    return TILEDB_ERR;

  *total_size = buffer_list->buffer_list_->total_size();

  return TILEDB_OK;
}

int32_t tiledb_buffer_list_flatten(
    tiledb_ctx_t* ctx,
    const tiledb_buffer_list_t* buffer_list,
    tiledb_buffer_t** buffer) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, buffer_list) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create a buffer instance
  if (tiledb_buffer_alloc(ctx, buffer) == TILEDB_ERR ||
      sanity_check(ctx, *buffer) == TILEDB_ERR)
    return TILEDB_ERR;

  // Resize the dest buffer
  const auto nbytes = buffer_list->buffer_list_->total_size();
  if (SAVE_ERROR_CATCH(ctx, (*buffer)->buffer_->realloc(nbytes)))
    return TILEDB_ERR;

  // Read all into the dest buffer
  buffer_list->buffer_list_->reset_offset();
  if (SAVE_ERROR_CATCH(
          ctx,
          buffer_list->buffer_list_->read((*buffer)->buffer_->data(), nbytes)))
    return TILEDB_ERR;

  // Set the result size
  (*buffer)->buffer_->set_size(nbytes);

  return TILEDB_OK;
}

/* ****************************** */
/*            CONFIG              */
/* ****************************** */

int32_t tiledb_config_alloc(tiledb_config_t** config, tiledb_error_t** error) {
  // Create a new config struct
  *config = new (std::nothrow) tiledb_config_t;
  if (*config == nullptr) {
    auto st = tiledb::sm::Status::Error(
        "Cannot create config object; Memory allocation failed");
    LOG_STATUS(st);
    create_error(error, st);
    return TILEDB_OOM;
  }

  // Create storage manager
  (*config)->config_ = new (std::nothrow) tiledb::sm::Config();
  if ((*config)->config_ == nullptr) {
    auto st = tiledb::sm::Status::Error(
        "Cannot create config object; Memory allocation failed");
    LOG_STATUS(st);
    create_error(error, st);
    return TILEDB_OOM;
  }

  // Success
  *error = nullptr;
  return TILEDB_OK;
}

void tiledb_config_free(tiledb_config_t** config) {
  if (config != nullptr && *config != nullptr) {
    delete (*config)->config_;
    delete (*config);
    *config = nullptr;
  }
}

int32_t tiledb_config_set(
    tiledb_config_t* config,
    const char* param,
    const char* value,
    tiledb_error_t** error) {
  if (sanity_check(config, error) == TILEDB_ERR)
    return TILEDB_ERR;

  if (create_error(error, config->config_->set(param, value)))
    return TILEDB_ERR;

  *error = nullptr;
  return TILEDB_OK;
}

int32_t tiledb_config_get(
    tiledb_config_t* config,
    const char* param,
    const char** value,
    tiledb_error_t** error) {
  if (sanity_check(config, error) == TILEDB_ERR)
    return TILEDB_ERR;

  if (create_error(error, config->config_->get(param, value)))
    return TILEDB_ERR;

  *error = nullptr;
  return TILEDB_OK;
}

int32_t tiledb_config_load_from_file(
    tiledb_config_t* config, const char* filename, tiledb_error_t** error) {
  if (sanity_check(config, error) == TILEDB_ERR)
    return TILEDB_ERR;

  if (filename == nullptr) {
    auto st =
        tiledb::sm::Status::Error("Cannot load from file; Invalid filename");
    LOG_STATUS(st);
    create_error(error, st);
  }

  if (create_error(error, config->config_->load_from_file(filename)))
    return TILEDB_ERR;

  *error = nullptr;
  return TILEDB_OK;
}

int32_t tiledb_config_save_to_file(
    tiledb_config_t* config, const char* filename, tiledb_error_t** error) {
  if (sanity_check(config, error) == TILEDB_ERR)
    return TILEDB_ERR;

  if (filename == nullptr) {
    auto st =
        tiledb::sm::Status::Error("Cannot save to file; Invalid filename");
    LOG_STATUS(st);
    create_error(error, st);
  }

  if (create_error(error, config->config_->save_to_file(filename)))
    return TILEDB_ERR;

  *error = nullptr;
  return TILEDB_OK;
}

int32_t tiledb_config_unset(
    tiledb_config_t* config, const char* param, tiledb_error_t** error) {
  if (sanity_check(config, error) == TILEDB_ERR)
    return TILEDB_ERR;

  if (create_error(error, config->config_->unset(param)))
    return TILEDB_ERR;

  *error = nullptr;
  return TILEDB_OK;
}

/* ****************************** */
/*           CONFIG ITER          */
/* ****************************** */

int32_t tiledb_config_iter_alloc(
    tiledb_config_t* config,
    const char* prefix,
    tiledb_config_iter_t** config_iter,
    tiledb_error_t** error) {
  if (sanity_check(config, error) == TILEDB_ERR)
    return TILEDB_ERR;

  *config_iter = new (std::nothrow) tiledb_config_iter_t;
  if (*config_iter == nullptr) {
    auto st = tiledb::sm::Status::Error(
        "Cannot create config iterator object; Memory allocation failed");
    LOG_STATUS(st);
    create_error(error, st);
    return TILEDB_OOM;
  }

  std::string prefix_str = (prefix == nullptr) ? "" : std::string(prefix);
  (*config_iter)->config_iter_ =
      new (std::nothrow) tiledb::sm::ConfigIter(config->config_, prefix_str);
  if ((*config_iter)->config_iter_ == nullptr) {
    auto st = tiledb::sm::Status::Error(
        "Cannot create config iterator object; Memory allocation failed");
    LOG_STATUS(st);
    create_error(error, st);
    delete *config_iter;
    *config_iter = nullptr;
    return TILEDB_OOM;
  }

  *error = nullptr;
  return TILEDB_OK;
}

int32_t tiledb_config_iter_reset(
    tiledb_config_t* config,
    tiledb_config_iter_t* config_iter,
    const char* prefix,
    tiledb_error_t** error) {
  if (sanity_check(config, error) == TILEDB_ERR ||
      sanity_check(config_iter, error) == TILEDB_ERR)
    return TILEDB_ERR;

  std::string prefix_str = (prefix == nullptr) ? "" : std::string(prefix);
  config_iter->config_iter_->reset(config->config_, prefix_str);

  *error = nullptr;
  return TILEDB_OK;
}

void tiledb_config_iter_free(tiledb_config_iter_t** config_iter) {
  if (config_iter != nullptr && *config_iter != nullptr) {
    delete (*config_iter)->config_iter_;
    delete *config_iter;
    *config_iter = nullptr;
  }
}

int32_t tiledb_config_iter_here(
    tiledb_config_iter_t* config_iter,
    const char** param,
    const char** value,
    tiledb_error_t** error) {
  if (sanity_check(config_iter, error) == TILEDB_ERR)
    return TILEDB_ERR;

  if (config_iter->config_iter_->end()) {
    *param = nullptr;
    *value = nullptr;
  } else {
    *param = config_iter->config_iter_->param().c_str();
    *value = config_iter->config_iter_->value().c_str();
  }

  *error = nullptr;
  return TILEDB_OK;
}

int32_t tiledb_config_iter_next(
    tiledb_config_iter_t* config_iter, tiledb_error_t** error) {
  if (sanity_check(config_iter, error) == TILEDB_ERR)
    return TILEDB_ERR;

  config_iter->config_iter_->next();

  *error = nullptr;
  return TILEDB_OK;
}

int32_t tiledb_config_iter_done(
    tiledb_config_iter_t* config_iter, int32_t* done, tiledb_error_t** error) {
  if (sanity_check(config_iter, error) == TILEDB_ERR)
    return TILEDB_ERR;

  *done = (int32_t)config_iter->config_iter_->end();

  *error = nullptr;
  return TILEDB_OK;
}

/* ****************************** */
/*            CONTEXT             */
/* ****************************** */

int32_t tiledb_ctx_alloc(tiledb_config_t* config, tiledb_ctx_t** ctx) {
  if (config != nullptr && config->config_ == nullptr)
    return TILEDB_ERR;

  // Create a context object
  *ctx = new (std::nothrow) tiledb_ctx_t;
  if (*ctx == nullptr)
    return TILEDB_OOM;

  // Create a context object
  (*ctx)->ctx_ = new (std::nothrow) tiledb::sm::Context();
  if ((*ctx)->ctx_ == nullptr) {
    delete (*ctx);
    (*ctx) = nullptr;
    return TILEDB_OOM;
  }

  // Initialize the context
  auto conf =
      (config == nullptr) ? (tiledb::sm::Config*)nullptr : config->config_;
  if (!(*ctx)->ctx_->init(conf).ok()) {
    delete (*ctx)->ctx_;
    delete (*ctx);
    (*ctx) = nullptr;
    return TILEDB_ERR;
  }

  // Success
  return TILEDB_OK;
}

void tiledb_ctx_free(tiledb_ctx_t** ctx) {
  if (ctx != nullptr && *ctx != nullptr) {
    delete (*ctx)->ctx_;
    delete (*ctx);
    *ctx = nullptr;
  }
}

int32_t tiledb_ctx_get_config(tiledb_ctx_t* ctx, tiledb_config_t** config) {
  // Create a new config struct
  *config = new (std::nothrow) tiledb_config_t;
  if (*config == nullptr)
    return TILEDB_OOM;

  (*config)->config_ = new (std::nothrow) tiledb::sm::Config();
  if ((*config)->config_ == nullptr) {
    delete (*config);
    return TILEDB_OOM;
  }

  *((*config)->config_) = ctx->ctx_->storage_manager()->config();

  return TILEDB_OK;
}

int32_t tiledb_ctx_get_last_error(tiledb_ctx_t* ctx, tiledb_error_t** err) {
  // sanity check
  if (ctx == nullptr || ctx->ctx_ == nullptr)
    return TILEDB_ERR;

  tiledb::sm::Status last_error = ctx->ctx_->last_error();

  // No last error
  if (last_error.ok()) {
    *err = nullptr;
    return TILEDB_OK;
  }

  // Create error struct
  *err = new (std::nothrow) tiledb_error_t;
  if (*err == nullptr)
    return TILEDB_OOM;

  // Set error message
  (*err)->errmsg_ = last_error.to_string();

  // Success
  return TILEDB_OK;
}

int32_t tiledb_ctx_is_supported_fs(
    tiledb_ctx_t* ctx, tiledb_filesystem_t fs, int32_t* is_supported) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  *is_supported = (int32_t)ctx->ctx_->storage_manager()->vfs()->supports_fs(
      static_cast<tiledb::sm::Filesystem>(fs));

  return TILEDB_OK;
}

int32_t tiledb_ctx_cancel_tasks(tiledb_ctx_t* ctx) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(ctx, ctx->ctx_->storage_manager()->cancel_all_tasks()))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_ctx_set_tag(
    tiledb_ctx_t* ctx, const char* key, const char* value) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(ctx, ctx->ctx_->storage_manager()->set_tag(key, value)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

/* ****************************** */
/*              GROUP             */
/* ****************************** */

int32_t tiledb_group_create(tiledb_ctx_t* ctx, const char* group_uri) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  // Check for error
  if (group_uri == nullptr) {
    auto st =
        tiledb::sm::Status::Error("Invalid group directory argument is NULL");
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

/* ********************************* */
/*              FILTER               */
/* ********************************* */

int32_t tiledb_filter_alloc(
    tiledb_ctx_t* ctx, tiledb_filter_type_t type, tiledb_filter_t** filter) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create a filter struct
  *filter = new (std::nothrow) tiledb_filter_t;
  if (*filter == nullptr) {
    auto st =
        tiledb::sm::Status::Error("Failed to allocate TileDB filter object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Create a new Filter object of the given type
  (*filter)->filter_ =
      tiledb::sm::Filter::create(static_cast<tiledb::sm::FilterType>(type));
  if ((*filter)->filter_ == nullptr) {
    delete *filter;
    auto st =
        tiledb::sm::Status::Error("Failed to allocate TileDB filter object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Success
  return TILEDB_OK;
}

void tiledb_filter_free(tiledb_filter_t** filter) {
  if (filter != nullptr && *filter != nullptr) {
    delete (*filter)->filter_;
    delete (*filter);
    *filter = nullptr;
  }
}

int32_t tiledb_filter_get_type(
    tiledb_ctx_t* ctx, tiledb_filter_t* filter, tiledb_filter_type_t* type) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, filter) == TILEDB_ERR)
    return TILEDB_ERR;

  *type = static_cast<tiledb_filter_type_t>(filter->filter_->type());

  return TILEDB_OK;
}

int32_t tiledb_filter_set_option(
    tiledb_ctx_t* ctx,
    tiledb_filter_t* filter,
    tiledb_filter_option_t option,
    const void* value) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, filter) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(
          ctx,
          filter->filter_->set_option(
              static_cast<tiledb::sm::FilterOption>(option), value)))
    return TILEDB_ERR;

  // Success
  return TILEDB_OK;
}

int32_t tiledb_filter_get_option(
    tiledb_ctx_t* ctx,
    tiledb_filter_t* filter,
    tiledb_filter_option_t option,
    void* value) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, filter) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(
          ctx,
          filter->filter_->get_option(
              static_cast<tiledb::sm::FilterOption>(option), value)))
    return TILEDB_ERR;

  // Success
  return TILEDB_OK;
}

/* ********************************* */
/*            FILTER LIST            */
/* ********************************* */

int32_t tiledb_filter_list_alloc(
    tiledb_ctx_t* ctx, tiledb_filter_list_t** filter_list) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create a filter struct
  *filter_list = new (std::nothrow) tiledb_filter_list_t;
  if (*filter_list == nullptr) {
    auto st = tiledb::sm::Status::Error(
        "Failed to allocate TileDB filter list object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Create a new FilterPipeline object
  (*filter_list)->pipeline_ = new (std::nothrow) tiledb::sm::FilterPipeline();
  if ((*filter_list)->pipeline_ == nullptr) {
    delete *filter_list;
    auto st = tiledb::sm::Status::Error(
        "Failed to allocate TileDB filter list object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Success
  return TILEDB_OK;
}

void tiledb_filter_list_free(tiledb_filter_list_t** filter_list) {
  if (filter_list != nullptr && *filter_list != nullptr) {
    delete (*filter_list)->pipeline_;
    delete (*filter_list);
    *filter_list = nullptr;
  }
}

int32_t tiledb_filter_list_add_filter(
    tiledb_ctx_t* ctx,
    tiledb_filter_list_t* filter_list,
    tiledb_filter_t* filter) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, filter_list) == TILEDB_ERR ||
      sanity_check(ctx, filter) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(
          ctx, filter_list->pipeline_->add_filter(*filter->filter_)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_filter_list_set_max_chunk_size(
    tiledb_ctx_t* ctx,
    const tiledb_filter_list_t* filter_list,
    uint32_t max_chunk_size) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, filter_list) == TILEDB_ERR)
    return TILEDB_ERR;

  filter_list->pipeline_->set_max_chunk_size(max_chunk_size);

  return TILEDB_OK;
}

int32_t tiledb_filter_list_get_nfilters(
    tiledb_ctx_t* ctx,
    const tiledb_filter_list_t* filter_list,
    uint32_t* nfilters) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, filter_list) == TILEDB_ERR)
    return TILEDB_ERR;

  *nfilters = filter_list->pipeline_->size();

  return TILEDB_OK;
}

int32_t tiledb_filter_list_get_filter_from_index(
    tiledb_ctx_t* ctx,
    const tiledb_filter_list_t* filter_list,
    uint32_t index,
    tiledb_filter_t** filter) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, filter_list) == TILEDB_ERR)
    return TILEDB_ERR;

  uint32_t nfilters = filter_list->pipeline_->size();
  if (nfilters == 0 && index == 0) {
    *filter = nullptr;
    return TILEDB_OK;
  }

  if (index >= nfilters) {
    auto st = tiledb::sm::Status::Error(
        "Filter " + std::to_string(index) + " out of bounds, filter list has " +
        std::to_string(nfilters) + " filters.");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  auto f = filter_list->pipeline_->get_filter(index);
  if (f == nullptr) {
    auto st = tiledb::sm::Status::Error("Failed to retrieve filter at index");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  *filter = new (std::nothrow) tiledb_filter_t;
  if (*filter == nullptr) {
    auto st =
        tiledb::sm::Status::Error("Failed to allocate TileDB filter object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  (*filter)->filter_ = f->clone();

  return TILEDB_OK;
}

int32_t tiledb_filter_list_get_max_chunk_size(
    tiledb_ctx_t* ctx,
    const tiledb_filter_list_t* filter_list,
    uint32_t* max_chunk_size) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, filter_list) == TILEDB_ERR)
    return TILEDB_ERR;

  *max_chunk_size = filter_list->pipeline_->max_chunk_size();

  return TILEDB_OK;
}

/* ********************************* */
/*            ATTRIBUTE              */
/* ********************************* */

int32_t tiledb_attribute_alloc(
    tiledb_ctx_t* ctx,
    const char* name,
    tiledb_datatype_t type,
    tiledb_attribute_t** attr) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create an attribute struct
  *attr = new (std::nothrow) tiledb_attribute_t;
  if (*attr == nullptr) {
    auto st =
        tiledb::sm::Status::Error("Failed to allocate TileDB attribute object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Create a new Attribute object
  (*attr)->attr_ = new (std::nothrow)
      tiledb::sm::Attribute(name, static_cast<tiledb::sm::Datatype>(type));
  if ((*attr)->attr_ == nullptr) {
    delete *attr;
    auto st =
        tiledb::sm::Status::Error("Failed to allocate TileDB attribute object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Success
  return TILEDB_OK;
}

void tiledb_attribute_free(tiledb_attribute_t** attr) {
  if (attr != nullptr && *attr != nullptr) {
    delete (*attr)->attr_;
    delete (*attr);
    *attr = nullptr;
  }
}

int32_t tiledb_attribute_set_filter_list(
    tiledb_ctx_t* ctx,
    tiledb_attribute_t* attr,
    tiledb_filter_list_t* filter_list) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, attr) == TILEDB_ERR ||
      sanity_check(ctx, filter_list) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(
          ctx, attr->attr_->set_filter_pipeline(filter_list->pipeline_)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_attribute_set_cell_val_num(
    tiledb_ctx_t* ctx, tiledb_attribute_t* attr, uint32_t cell_val_num) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, attr) == TILEDB_ERR)
    return TILEDB_ERR;
  if (SAVE_ERROR_CATCH(ctx, attr->attr_->set_cell_val_num(cell_val_num)))
    return TILEDB_ERR;
  return TILEDB_OK;
}

int32_t tiledb_attribute_get_name(
    tiledb_ctx_t* ctx, const tiledb_attribute_t* attr, const char** name) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, attr) == TILEDB_ERR)
    return TILEDB_ERR;

  *name = attr->attr_->name().c_str();

  return TILEDB_OK;
}

int32_t tiledb_attribute_get_type(
    tiledb_ctx_t* ctx,
    const tiledb_attribute_t* attr,
    tiledb_datatype_t* type) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, attr) == TILEDB_ERR)
    return TILEDB_ERR;
  *type = static_cast<tiledb_datatype_t>(attr->attr_->type());
  return TILEDB_OK;
}

int32_t tiledb_attribute_get_filter_list(
    tiledb_ctx_t* ctx,
    tiledb_attribute_t* attr,
    tiledb_filter_list_t** filter_list) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, attr) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create a filter list struct
  *filter_list = new (std::nothrow) tiledb_filter_list_t;
  if (*filter_list == nullptr) {
    auto st = tiledb::sm::Status::Error(
        "Failed to allocate TileDB filter list object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Create a new FilterPipeline object
  (*filter_list)->pipeline_ =
      new (std::nothrow) tiledb::sm::FilterPipeline(attr->attr_->filters());
  if ((*filter_list)->pipeline_ == nullptr) {
    delete *filter_list;
    auto st = tiledb::sm::Status::Error(
        "Failed to allocate TileDB filter list object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  return TILEDB_OK;
}

int32_t tiledb_attribute_get_cell_val_num(
    tiledb_ctx_t* ctx, const tiledb_attribute_t* attr, uint32_t* cell_val_num) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, attr) == TILEDB_ERR)
    return TILEDB_ERR;
  *cell_val_num = attr->attr_->cell_val_num();
  return TILEDB_OK;
}

int32_t tiledb_attribute_get_cell_size(
    tiledb_ctx_t* ctx, const tiledb_attribute_t* attr, uint64_t* cell_size) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, attr) == TILEDB_ERR)
    return TILEDB_ERR;
  *cell_size = attr->attr_->cell_size();
  return TILEDB_OK;
}

int32_t tiledb_attribute_dump(
    tiledb_ctx_t* ctx, const tiledb_attribute_t* attr, FILE* out) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, attr) == TILEDB_ERR)
    return TILEDB_ERR;
  attr->attr_->dump(out);
  return TILEDB_OK;
}

/* ********************************* */
/*              DOMAIN               */
/* ********************************* */

int32_t tiledb_domain_alloc(tiledb_ctx_t* ctx, tiledb_domain_t** domain) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create a domain struct
  *domain = new (std::nothrow) tiledb_domain_t;
  if (*domain == nullptr) {
    auto st =
        tiledb::sm::Status::Error("Failed to allocate TileDB domain object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Create a new Domain object
  (*domain)->domain_ = new (std::nothrow) tiledb::sm::Domain();
  if ((*domain)->domain_ == nullptr) {
    delete *domain;
    auto st =
        tiledb::sm::Status::Error("Failed to allocate TileDB domain object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }
  return TILEDB_OK;
}

void tiledb_domain_free(tiledb_domain_t** domain) {
  if (domain != nullptr && *domain != nullptr) {
    delete (*domain)->domain_;
    delete *domain;
    *domain = nullptr;
  }
}

int32_t tiledb_domain_get_type(
    tiledb_ctx_t* ctx, const tiledb_domain_t* domain, tiledb_datatype_t* type) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, domain) == TILEDB_ERR)
    return TILEDB_ERR;

  if (domain->domain_->dim_num() == 0) {
    auto st = tiledb::sm::Status::Error(
        "Cannot get domain type; Domain has no dimensions");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  if (!domain->domain_->all_dims_same_type()) {
    auto st = tiledb::sm::Status::Error(
        "Cannot get domain type; Not applicable to heterogeneous dimensions");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  *type = static_cast<tiledb_datatype_t>(domain->domain_->dimension(0)->type());
  return TILEDB_OK;
}

int32_t tiledb_domain_get_ndim(
    tiledb_ctx_t* ctx, const tiledb_domain_t* domain, uint32_t* ndim) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, domain) == TILEDB_ERR)
    return TILEDB_ERR;
  *ndim = domain->domain_->dim_num();
  return TILEDB_OK;
}

int32_t tiledb_domain_add_dimension(
    tiledb_ctx_t* ctx, tiledb_domain_t* domain, tiledb_dimension_t* dim) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, domain) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(ctx, domain->domain_->add_dimension(dim->dim_)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_domain_dump(
    tiledb_ctx_t* ctx, const tiledb_domain_t* domain, FILE* out) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, domain) == TILEDB_ERR)
    return TILEDB_ERR;
  domain->domain_->dump(out);
  return TILEDB_OK;
}

/* ********************************* */
/*             DIMENSION             */
/* ********************************* */

int32_t tiledb_dimension_alloc(
    tiledb_ctx_t* ctx,
    const char* name,
    tiledb_datatype_t type,
    const void* dim_domain,
    const void* tile_extent,
    tiledb_dimension_t** dim) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create a dimension struct
  *dim = new (std::nothrow) tiledb_dimension_t;
  if (*dim == nullptr) {
    auto st =
        tiledb::sm::Status::Error("Failed to allocate TileDB dimension object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Create a new Dimension object
  (*dim)->dim_ = new (std::nothrow)
      tiledb::sm::Dimension(name, static_cast<tiledb::sm::Datatype>(type));
  if ((*dim)->dim_ == nullptr) {
    delete *dim;
    auto st =
        tiledb::sm::Status::Error("Failed to allocate TileDB dimension object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Set domain
  if (SAVE_ERROR_CATCH(ctx, (*dim)->dim_->set_domain(dim_domain))) {
    delete (*dim)->dim_;
    delete *dim;
    return TILEDB_ERR;
  }

  // Set tile extent
  if (SAVE_ERROR_CATCH(ctx, (*dim)->dim_->set_tile_extent(tile_extent))) {
    delete (*dim)->dim_;
    delete *dim;
    return TILEDB_ERR;
  }

  // Success
  return TILEDB_OK;
}

void tiledb_dimension_free(tiledb_dimension_t** dim) {
  if (dim != nullptr && *dim != nullptr) {
    delete (*dim)->dim_;
    delete *dim;
    *dim = nullptr;
  }
}

int32_t tiledb_dimension_set_filter_list(
    tiledb_ctx_t* ctx,
    tiledb_dimension_t* dim,
    tiledb_filter_list_t* filter_list) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, dim) == TILEDB_ERR ||
      sanity_check(ctx, filter_list) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(
          ctx, dim->dim_->set_filter_pipeline(filter_list->pipeline_)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_dimension_set_cell_val_num(
    tiledb_ctx_t* ctx, tiledb_dimension_t* dim, uint32_t cell_val_num) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, dim) == TILEDB_ERR)
    return TILEDB_ERR;
  if (SAVE_ERROR_CATCH(ctx, dim->dim_->set_cell_val_num(cell_val_num)))
    return TILEDB_ERR;
  return TILEDB_OK;
}

int32_t tiledb_dimension_get_filter_list(
    tiledb_ctx_t* ctx,
    tiledb_dimension_t* dim,
    tiledb_filter_list_t** filter_list) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, dim) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create a filter list struct
  *filter_list = new (std::nothrow) tiledb_filter_list_t;
  if (*filter_list == nullptr) {
    auto st = tiledb::sm::Status::Error(
        "Failed to allocate TileDB filter list object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Create a new FilterPipeline object
  (*filter_list)->pipeline_ =
      new (std::nothrow) tiledb::sm::FilterPipeline(dim->dim_->filters());
  if ((*filter_list)->pipeline_ == nullptr) {
    delete *filter_list;
    auto st = tiledb::sm::Status::Error(
        "Failed to allocate TileDB filter list object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  return TILEDB_OK;
}

int32_t tiledb_dimension_get_cell_val_num(
    tiledb_ctx_t* ctx, const tiledb_dimension_t* dim, uint32_t* cell_val_num) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, dim) == TILEDB_ERR)
    return TILEDB_ERR;
  *cell_val_num = dim->dim_->cell_val_num();
  return TILEDB_OK;
}

int32_t tiledb_dimension_get_name(
    tiledb_ctx_t* ctx, const tiledb_dimension_t* dim, const char** name) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, dim) == TILEDB_ERR)
    return TILEDB_ERR;
  *name = dim->dim_->name().c_str();
  return TILEDB_OK;
}

int32_t tiledb_dimension_get_type(
    tiledb_ctx_t* ctx, const tiledb_dimension_t* dim, tiledb_datatype_t* type) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, dim) == TILEDB_ERR)
    return TILEDB_ERR;
  *type = static_cast<tiledb_datatype_t>(dim->dim_->type());
  return TILEDB_OK;
}

int32_t tiledb_dimension_get_domain(
    tiledb_ctx_t* ctx, const tiledb_dimension_t* dim, const void** domain) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, dim) == TILEDB_ERR)
    return TILEDB_ERR;
  *domain = dim->dim_->domain().data();
  return TILEDB_OK;
}

int32_t tiledb_dimension_get_tile_extent(
    tiledb_ctx_t* ctx,
    const tiledb_dimension_t* dim,
    const void** tile_extent) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, dim) == TILEDB_ERR)
    return TILEDB_ERR;
  *tile_extent = dim->dim_->tile_extent().data();
  return TILEDB_OK;
}

int32_t tiledb_dimension_dump(
    tiledb_ctx_t* ctx, const tiledb_dimension_t* dim, FILE* out) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, dim) == TILEDB_ERR)
    return TILEDB_ERR;
  dim->dim_->dump(out);
  return TILEDB_OK;
}

int32_t tiledb_domain_get_dimension_from_index(
    tiledb_ctx_t* ctx,
    const tiledb_domain_t* domain,
    uint32_t index,
    tiledb_dimension_t** dim) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, domain) == TILEDB_ERR) {
    return TILEDB_ERR;
  }
  uint32_t ndim = domain->domain_->dim_num();
  if (ndim == 0 && index == 0) {
    *dim = nullptr;
    return TILEDB_OK;
  }
  if (index > (ndim - 1)) {
    std::ostringstream errmsg;
    errmsg << "Dimension " << index << " out of bounds, domain has rank "
           << ndim;
    auto st = tiledb::sm::Status::DomainError(errmsg.str());
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  *dim = new (std::nothrow) tiledb_dimension_t;
  if (*dim == nullptr) {
    auto st =
        tiledb::sm::Status::Error("Failed to allocate TileDB dimension object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }
  (*dim)->dim_ = new (std::nothrow)
      tiledb::sm::Dimension(domain->domain_->dimension(index));
  if ((*dim)->dim_ == nullptr) {
    delete *dim;
    auto st =
        tiledb::sm::Status::Error("Failed to allocate TileDB dimension object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }
  return TILEDB_OK;
}

int32_t tiledb_domain_get_dimension_from_name(
    tiledb_ctx_t* ctx,
    const tiledb_domain_t* domain,
    const char* name,
    tiledb_dimension_t** dim) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, domain) == TILEDB_ERR) {
    return TILEDB_ERR;
  }
  uint32_t ndim = domain->domain_->dim_num();
  if (ndim == 0) {
    *dim = nullptr;
    return TILEDB_OK;
  }
  std::string name_string(name);
  auto found_dim = domain->domain_->dimension(name_string);

  if (found_dim == nullptr) {
    tiledb::sm::Status st = tiledb::sm::Status::DomainError(
        std::string("Dimension \"") + name + "\" does not exist");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  *dim = new (std::nothrow) tiledb_dimension_t;
  if (*dim == nullptr) {
    auto st =
        tiledb::sm::Status::Error("Failed to allocate TileDB dimension object");
    save_error(ctx, st);
    return TILEDB_OOM;
  }
  (*dim)->dim_ = new (std::nothrow) tiledb::sm::Dimension(found_dim);
  if ((*dim)->dim_ == nullptr) {
    delete *dim;
    auto st =
        tiledb::sm::Status::Error("Failed to allocate TileDB dimension object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }
  return TILEDB_OK;
}

int32_t tiledb_domain_has_dimension(
    tiledb_ctx_t* ctx,
    const tiledb_domain_t* domain,
    const char* name,
    int32_t* has_dim) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, domain) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  bool b;
  if (SAVE_ERROR_CATCH(ctx, domain->domain_->has_dimension(name, &b)))
    return TILEDB_ERR;

  *has_dim = b ? 1 : 0;

  return TILEDB_OK;
}

/* ****************************** */
/*           ARRAY SCHEMA         */
/* ****************************** */

int32_t tiledb_array_schema_alloc(
    tiledb_ctx_t* ctx,
    tiledb_array_type_t array_type,
    tiledb_array_schema_t** array_schema) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create array schema struct
  *array_schema = new (std::nothrow) tiledb_array_schema_t;
  if (*array_schema == nullptr) {
    auto st = tiledb::sm::Status::Error(
        "Failed to allocate TileDB array schema object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Create a new ArraySchema object
  (*array_schema)->array_schema_ = new (std::nothrow)
      tiledb::sm::ArraySchema(static_cast<tiledb::sm::ArrayType>(array_type));
  if ((*array_schema)->array_schema_ == nullptr) {
    delete *array_schema;
    *array_schema = nullptr;
    auto st = tiledb::sm::Status::Error(
        "Failed to allocate TileDB array schema object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Success
  return TILEDB_OK;
}

void tiledb_array_schema_free(tiledb_array_schema_t** array_schema) {
  if (array_schema != nullptr && *array_schema != nullptr) {
    delete (*array_schema)->array_schema_;
    delete *array_schema;
    *array_schema = nullptr;
  }
}

int32_t tiledb_array_schema_add_attribute(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_attribute_t* attr) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR ||
      sanity_check(ctx, attr) == TILEDB_ERR)
    return TILEDB_ERR;
  if (SAVE_ERROR_CATCH(
          ctx, array_schema->array_schema_->add_attribute(attr->attr_)))
    return TILEDB_ERR;
  return TILEDB_OK;
}

int32_t tiledb_array_schema_set_allows_dups(
    tiledb_ctx_t* ctx, tiledb_array_schema_t* array_schema, int allows_dups) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  if (SAVE_ERROR_CATCH(
          ctx, array_schema->array_schema_->set_allows_dups(allows_dups)))
    return TILEDB_ERR;
  return TILEDB_OK;
}

int32_t tiledb_array_schema_get_allows_dups(
    tiledb_ctx_t* ctx, tiledb_array_schema_t* array_schema, int* allows_dups) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  *allows_dups = (int)array_schema->array_schema_->allows_dups();
  return TILEDB_OK;
}

int32_t tiledb_array_schema_set_domain(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_domain_t* domain) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  if (SAVE_ERROR_CATCH(
          ctx, array_schema->array_schema_->set_domain(domain->domain_)))
    return TILEDB_ERR;
  return TILEDB_OK;
}

int32_t tiledb_array_schema_set_capacity(
    tiledb_ctx_t* ctx, tiledb_array_schema_t* array_schema, uint64_t capacity) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  array_schema->array_schema_->set_capacity(capacity);
  return TILEDB_OK;
}

int32_t tiledb_array_schema_set_cell_order(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_layout_t cell_order) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  array_schema->array_schema_->set_cell_order(
      static_cast<tiledb::sm::Layout>(cell_order));
  return TILEDB_OK;
}

int32_t tiledb_array_schema_set_tile_order(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_layout_t tile_order) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  array_schema->array_schema_->set_tile_order(
      static_cast<tiledb::sm::Layout>(tile_order));
  return TILEDB_OK;
}

int32_t tiledb_array_schema_set_coords_filter_list(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_filter_list_t* filter_list) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR ||
      sanity_check(ctx, filter_list) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(
          ctx,
          array_schema->array_schema_->set_coords_filter_pipeline(
              filter_list->pipeline_)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_array_schema_set_offsets_filter_list(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_filter_list_t* filter_list) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR ||
      sanity_check(ctx, filter_list) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(
          ctx,
          array_schema->array_schema_->set_cell_var_offsets_filter_pipeline(
              filter_list->pipeline_)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_array_schema_check(
    tiledb_ctx_t* ctx, tiledb_array_schema_t* array_schema) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(ctx, array_schema->array_schema_->check()))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_array_schema_load(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_array_schema_t** array_schema) {
  return tiledb_array_schema_load_with_key(
      ctx, array_uri, TILEDB_NO_ENCRYPTION, nullptr, 0, array_schema);
}

int32_t tiledb_array_schema_load_with_key(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_encryption_type_t encryption_type,
    const void* encryption_key,
    uint32_t key_length,
    tiledb_array_schema_t** array_schema) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;
  // Create array schema
  *array_schema = new (std::nothrow) tiledb_array_schema_t;
  if (*array_schema == nullptr) {
    auto st = tiledb::sm::Status::Error(
        "Failed to allocate TileDB array schema object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Check array name
  tiledb::sm::URI uri(array_uri);
  if (uri.is_invalid()) {
    auto st = tiledb::sm::Status::Error(
        "Failed to load array schema; Invalid array URI");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  if (uri.is_tiledb()) {
    // Check REST client
    auto rest_client = ctx->ctx_->storage_manager()->rest_client();
    if (rest_client == nullptr) {
      auto st = tiledb::sm::Status::Error(
          "Failed to load array schema; remote array with no REST client.");
      LOG_STATUS(st);
      save_error(ctx, st);
      return TILEDB_ERR;
    }

    if (SAVE_ERROR_CATCH(
            ctx,
            rest_client->get_array_schema_from_rest(
                uri, &(*array_schema)->array_schema_))) {
      delete *array_schema;
      return TILEDB_ERR;
    }
  } else {
    // Create key
    tiledb::sm::EncryptionKey key;
    if (SAVE_ERROR_CATCH(
            ctx,
            key.set_key(
                static_cast<tiledb::sm::EncryptionType>(encryption_type),
                encryption_key,
                key_length)))
      return TILEDB_ERR;

    // Load array schema
    auto storage_manager = ctx->ctx_->storage_manager();
    if (SAVE_ERROR_CATCH(
            ctx,
            storage_manager->load_array_schema(
                uri, key, &((*array_schema)->array_schema_)))) {
      delete *array_schema;
      return TILEDB_ERR;
    }
  }
  return TILEDB_OK;
}

int32_t tiledb_array_schema_get_array_type(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    tiledb_array_type_t* array_type) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  *array_type = static_cast<tiledb_array_type_t>(
      array_schema->array_schema_->array_type());
  return TILEDB_OK;
}

int32_t tiledb_array_schema_get_capacity(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    uint64_t* capacity) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  *capacity = array_schema->array_schema_->capacity();
  return TILEDB_OK;
}

int32_t tiledb_array_schema_get_cell_order(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    tiledb_layout_t* cell_order) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  *cell_order =
      static_cast<tiledb_layout_t>(array_schema->array_schema_->cell_order());
  return TILEDB_OK;
}

int32_t tiledb_array_schema_get_coords_filter_list(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_filter_list_t** filter_list) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create a filter list struct
  *filter_list = new (std::nothrow) tiledb_filter_list_t;
  if (*filter_list == nullptr) {
    auto st = tiledb::sm::Status::Error(
        "Failed to allocate TileDB filter list object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Create a new FilterPipeline object
  (*filter_list)->pipeline_ = new (std::nothrow)
      tiledb::sm::FilterPipeline(array_schema->array_schema_->coords_filters());
  if ((*filter_list)->pipeline_ == nullptr) {
    delete *filter_list;
    auto st = tiledb::sm::Status::Error(
        "Failed to allocate TileDB filter list object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  return TILEDB_OK;
}

int32_t tiledb_array_schema_get_offsets_filter_list(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_filter_list_t** filter_list) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create a filter list struct
  *filter_list = new (std::nothrow) tiledb_filter_list_t;
  if (*filter_list == nullptr) {
    auto st = tiledb::sm::Status::Error(
        "Failed to allocate TileDB filter list object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Create a new FilterPipeline object
  (*filter_list)->pipeline_ = new (std::nothrow) tiledb::sm::FilterPipeline(
      array_schema->array_schema_->cell_var_offsets_filters());
  if ((*filter_list)->pipeline_ == nullptr) {
    delete *filter_list;
    auto st = tiledb::sm::Status::Error(
        "Failed to allocate TileDB filter list object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  return TILEDB_OK;
}

int32_t tiledb_array_schema_get_domain(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    tiledb_domain_t** domain) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create a domain struct
  *domain = new (std::nothrow) tiledb_domain_t;
  if (*domain == nullptr) {
    auto st =
        tiledb::sm::Status::Error("Failed to allocate TileDB domain object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Create a new Domain object
  (*domain)->domain_ = new (std::nothrow)
      tiledb::sm::Domain(array_schema->array_schema_->domain());
  if ((*domain)->domain_ == nullptr) {
    delete *domain;
    tiledb::sm::Status st = tiledb::sm::Status::Error(
        "Failed to allocate TileDB domain object in object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }
  return TILEDB_OK;
}

int32_t tiledb_array_schema_get_tile_order(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    tiledb_layout_t* tile_order) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  *tile_order =
      static_cast<tiledb_layout_t>(array_schema->array_schema_->tile_order());
  return TILEDB_OK;
}

int32_t tiledb_array_schema_get_attribute_num(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    uint32_t* attribute_num) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  *attribute_num = array_schema->array_schema_->attribute_num();
  return TILEDB_OK;
}

int32_t tiledb_array_schema_dump(
    tiledb_ctx_t* ctx, const tiledb_array_schema_t* array_schema, FILE* out) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  array_schema->array_schema_->dump(out);
  return TILEDB_OK;
}

int32_t tiledb_array_schema_get_attribute_from_index(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    uint32_t index,
    tiledb_attribute_t** attr) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR) {
    return TILEDB_ERR;
  }
  uint32_t attribute_num = array_schema->array_schema_->attribute_num();
  if (attribute_num == 0) {
    *attr = nullptr;
    return TILEDB_OK;
  }
  if (index >= attribute_num) {
    std::ostringstream errmsg;
    errmsg << "Attribute index: " << index << " out of bounds given "
           << attribute_num << " attributes in array "
           << array_schema->array_schema_->array_uri().to_string();
    auto st = tiledb::sm::Status::ArraySchemaError(errmsg.str());
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  auto found_attr = array_schema->array_schema_->attribute(index);
  assert(found_attr != nullptr);

  *attr = new (std::nothrow) tiledb_attribute_t;
  if (*attr == nullptr) {
    auto st = tiledb::sm::Status::Error("Failed to allocate TileDB attribute");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Create an attribute object
  (*attr)->attr_ = new (std::nothrow) tiledb::sm::Attribute(found_attr);

  // Check for allocation error
  if ((*attr)->attr_ == nullptr) {
    delete *attr;
    auto st = tiledb::sm::Status::Error("Failed to allocate TileDB attribute");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }
  return TILEDB_OK;
}

int32_t tiledb_array_schema_get_attribute_from_name(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    const char* name,
    tiledb_attribute_t** attr) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR) {
    return TILEDB_ERR;
  }
  uint32_t attribute_num = array_schema->array_schema_->attribute_num();
  if (attribute_num == 0) {
    *attr = nullptr;
    return TILEDB_OK;
  }
  std::string name_string(name);
  auto found_attr = array_schema->array_schema_->attribute(name_string);
  if (found_attr == nullptr) {
    tiledb::sm::Status st = tiledb::sm::Status::ArraySchemaError(
        std::string("Attribute name: ") +
        (name_string.empty() ? "<anonymous>" : name) +
        " does not exist for array " +
        array_schema->array_schema_->array_uri().to_string());
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  *attr = new (std::nothrow) tiledb_attribute_t;
  if (*attr == nullptr) {
    auto st = tiledb::sm::Status::Error("Failed to allocate TileDB attribute");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }
  // Create an attribute object
  (*attr)->attr_ = new (std::nothrow) tiledb::sm::Attribute(found_attr);
  // Check for allocation error
  if ((*attr)->attr_ == nullptr) {
    delete *attr;
    auto st = tiledb::sm::Status::Error("Failed to allocate TileDB attribute");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }
  return TILEDB_OK;
}

int32_t tiledb_array_schema_has_attribute(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    const char* name,
    int32_t* has_attr) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  bool b;
  if (SAVE_ERROR_CATCH(
          ctx, array_schema->array_schema_->has_attribute(name, &b)))
    return TILEDB_ERR;

  *has_attr = b ? 1 : 0;

  return TILEDB_OK;
}

/* ****************************** */
/*              QUERY             */
/* ****************************** */

int32_t tiledb_query_alloc(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_query_type_t query_type,
    tiledb_query_t** query) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  // Error if array is not open
  if (!array->array_->is_open()) {
    auto st = tiledb::sm::Status::Error(
        "Cannot create query; Input array is not open");
    *query = nullptr;
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  // Error is the query type and array query type do not match
  tiledb::sm::QueryType array_query_type;
  if (SAVE_ERROR_CATCH(ctx, array->array_->get_query_type(&array_query_type)))
    return TILEDB_ERR;
  if (query_type != static_cast<tiledb_query_type_t>(array_query_type)) {
    std::stringstream errmsg;
    errmsg << "Cannot create query; "
           << "Array query type does not match declared query type: "
           << "(" << query_type_str(array_query_type) << " != "
           << tiledb::sm::query_type_str(
                  static_cast<tiledb::sm::QueryType>(query_type))
           << ")";
    *query = nullptr;
    auto st = tiledb::sm::Status::Error(errmsg.str());
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  // Create query struct
  *query = new (std::nothrow) tiledb_query_t;
  if (*query == nullptr) {
    auto st = tiledb::sm::Status::Error(
        "Failed to allocate TileDB query object; Memory allocation failed");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Create query
  (*query)->query_ = new (std::nothrow)
      tiledb::sm::Query(ctx->ctx_->storage_manager(), array->array_);
  if ((*query)->query_ == nullptr) {
    auto st = tiledb::sm::Status::Error(
        "Failed to allocate TileDB query object; Memory allocation failed");
    delete *query;
    *query = nullptr;
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Success
  return TILEDB_OK;
}

int32_t tiledb_query_set_subarray(
    tiledb_ctx_t* ctx, tiledb_query_t* query, const void* subarray) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  // Set subarray
  if (SAVE_ERROR_CATCH(ctx, query->query_->set_subarray(subarray)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_query_set_buffer(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* name,
    void* buffer,
    uint64_t* buffer_size) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  // Set attribute buffer
  if (SAVE_ERROR_CATCH(
          ctx, query->query_->set_buffer(name, buffer, buffer_size)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_query_set_buffer_var(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* name,
    uint64_t* buffer_off,
    uint64_t* buffer_off_size,
    void* buffer_val,
    uint64_t* buffer_val_size) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  // On writes, check the provided offsets for validity.
  if (query->query_->type() == tiledb::sm::QueryType::WRITE &&
      save_error(
          ctx,
          tiledb::sm::Query::check_var_attr_offsets(
              buffer_off, buffer_off_size, buffer_val_size)))
    return TILEDB_ERR;

  // Set attribute buffers
  if (SAVE_ERROR_CATCH(
          ctx,
          query->query_->set_buffer(
              name, buffer_off, buffer_off_size, buffer_val, buffer_val_size)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_query_get_buffer(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* name,
    void** buffer,
    uint64_t** buffer_size) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  // Set attribute buffer
  if (SAVE_ERROR_CATCH(
          ctx, query->query_->get_buffer(name, buffer, buffer_size)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_query_get_buffer_var(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* name,
    uint64_t** buffer_off,
    uint64_t** buffer_off_size,
    void** buffer_val,
    uint64_t** buffer_val_size) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  // Get attribute buffers
  if (SAVE_ERROR_CATCH(
          ctx,
          query->query_->get_buffer(
              name, buffer_off, buffer_off_size, buffer_val, buffer_val_size)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_query_set_layout(
    tiledb_ctx_t* ctx, tiledb_query_t* query, tiledb_layout_t layout) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  // Set layout
  if (SAVE_ERROR_CATCH(
          ctx,
          query->query_->set_layout(static_cast<tiledb::sm::Layout>(layout))))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_query_finalize(tiledb_ctx_t* ctx, tiledb_query_t* query) {
  // Trivial case
  if (query == nullptr)
    return TILEDB_OK;

  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  // Flush query
  if (SAVE_ERROR_CATCH(ctx, query->query_->finalize()))
    return TILEDB_ERR;

  return TILEDB_OK;
}

void tiledb_query_free(tiledb_query_t** query) {
  if (query != nullptr && *query != nullptr) {
    delete (*query)->query_;
    delete *query;
    *query = nullptr;
  }
}

int32_t tiledb_query_submit(tiledb_ctx_t* ctx, tiledb_query_t* query) {
  // Sanity checks
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(ctx, query->query_->submit()))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_query_submit_async(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    void (*callback)(void*),
    void* callback_data) {
  // Sanity checks
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(
          ctx, query->query_->submit_async(callback, callback_data)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_query_has_results(
    tiledb_ctx_t* ctx, tiledb_query_t* query, int32_t* has_results) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  *has_results = query->query_->has_results();

  return TILEDB_OK;
}

int32_t tiledb_query_get_status(
    tiledb_ctx_t* ctx, tiledb_query_t* query, tiledb_query_status_t* status) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  *status = (tiledb_query_status_t)query->query_->status();

  return TILEDB_OK;
}

int32_t tiledb_query_get_type(
    tiledb_ctx_t* ctx, tiledb_query_t* query, tiledb_query_type_t* query_type) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  *query_type = static_cast<tiledb_query_type_t>(query->query_->type());

  return TILEDB_OK;
}

int32_t tiledb_query_get_layout(
    tiledb_ctx_t* ctx, tiledb_query_t* query, tiledb_layout_t* query_layout) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  *query_layout = static_cast<tiledb_layout_t>(query->query_->layout());

  return TILEDB_OK;
}

int32_t tiledb_query_add_range(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    uint32_t dim_idx,
    const void* start,
    const void* end,
    const void* stride) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(
          ctx, query->query_->add_range(dim_idx, start, end, stride)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_query_add_range_var(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    uint32_t dim_idx,
    const void* start,
    uint64_t start_size,
    const void* end,
    uint64_t end_size) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(
          ctx,
          query->query_->add_range_var(
              dim_idx, start, start_size, end, end_size)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_query_get_range_num(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    uint32_t dim_idx,
    uint64_t* range_num) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(ctx, query->query_->get_range_num(dim_idx, range_num)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_query_get_range(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    uint32_t dim_idx,
    uint64_t range_idx,
    const void** start,
    const void** end,
    const void** stride) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(
          ctx,
          query->query_->get_range(dim_idx, range_idx, start, end, stride)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_query_get_range_var_size(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    uint32_t dim_idx,
    uint64_t range_idx,
    uint64_t* start_size,
    uint64_t* end_size) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(
          ctx,
          query->query_->get_range_var_size(
              dim_idx, range_idx, start_size, end_size)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_query_get_range_var(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    uint32_t dim_idx,
    uint64_t range_idx,
    void* start,
    void* end) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(
          ctx, query->query_->get_range_var(dim_idx, range_idx, start, end)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_query_get_est_result_size(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    const char* name,
    uint64_t* size) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(ctx, query->query_->get_est_result_size(name, size)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_query_get_est_result_size_var(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    const char* name,
    uint64_t* size_off,
    uint64_t* size_val) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(
          ctx, query->query_->get_est_result_size(name, size_off, size_val)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_query_get_fragment_num(
    tiledb_ctx_t* ctx, const tiledb_query_t* query, uint32_t* num) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(ctx, query->query_->get_written_fragment_num(num)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_query_get_fragment_uri(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    uint64_t idx,
    const char** uri) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(ctx, query->query_->get_written_fragment_uri(idx, uri)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_query_get_fragment_timestamp_range(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    uint64_t idx,
    uint64_t* t1,
    uint64_t* t2) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(
          ctx,
          query->query_->get_written_fragment_timestamp_range(idx, t1, t2)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

/* ****************************** */
/*              ARRAY             */
/* ****************************** */

int32_t tiledb_array_alloc(
    tiledb_ctx_t* ctx, const char* array_uri, tiledb_array_t** array) {
  if (sanity_check(ctx) == TILEDB_ERR) {
    *array = nullptr;
    return TILEDB_ERR;
  }

  // Create array struct
  *array = new (std::nothrow) tiledb_array_t;
  if (*array == nullptr) {
    auto st = tiledb::sm::Status::Error(
        "Failed to create TileDB array object; Memory allocation error");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Check array URI
  auto uri = tiledb::sm::URI(array_uri);
  if (uri.is_invalid()) {
    auto st = tiledb::sm::Status::Error(
        "Failed to create TileDB array object; Invalid URI");
    delete *array;
    *array = nullptr;
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  // Allocate an array object
  (*array)->array_ =
      new (std::nothrow) tiledb::sm::Array(uri, ctx->ctx_->storage_manager());
  if ((*array)->array_ == nullptr) {
    delete *array;
    *array = nullptr;
    tiledb::sm::Status st = tiledb::sm::Status::Error(
        "Failed to create TileDB array object; Memory allocation "
        "error");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Success
  return TILEDB_OK;
}

int32_t tiledb_array_open(
    tiledb_ctx_t* ctx, tiledb_array_t* array, tiledb_query_type_t query_type) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  // Open array
  if (SAVE_ERROR_CATCH(
          ctx,
          array->array_->open(
              static_cast<tiledb::sm::QueryType>(query_type),
              static_cast<tiledb::sm::EncryptionType>(TILEDB_NO_ENCRYPTION),
              nullptr,
              0)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_array_open_at(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_query_type_t query_type,
    uint64_t timestamp) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  // Open array
  if (SAVE_ERROR_CATCH(
          ctx,
          array->array_->open(
              static_cast<tiledb::sm::QueryType>(query_type),
              timestamp,
              static_cast<tiledb::sm::EncryptionType>(TILEDB_NO_ENCRYPTION),
              nullptr,
              0)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_array_open_with_key(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_query_type_t query_type,
    tiledb_encryption_type_t encryption_type,
    const void* encryption_key,
    uint32_t key_length) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  // Open array
  if (SAVE_ERROR_CATCH(
          ctx,
          array->array_->open(
              static_cast<tiledb::sm::QueryType>(query_type),
              static_cast<tiledb::sm::EncryptionType>(encryption_type),
              encryption_key,
              key_length)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_array_open_at_with_key(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_query_type_t query_type,
    tiledb_encryption_type_t encryption_type,
    const void* encryption_key,
    uint32_t key_length,
    uint64_t timestamp) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  // Open array
  if (SAVE_ERROR_CATCH(
          ctx,
          array->array_->open(
              static_cast<tiledb::sm::QueryType>(query_type),
              timestamp,
              static_cast<tiledb::sm::EncryptionType>(encryption_type),
              encryption_key,
              key_length)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_array_is_open(
    tiledb_ctx_t* ctx, tiledb_array_t* array, int32_t* is_open) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  *is_open = (int32_t)array->array_->is_open();

  return TILEDB_OK;
}

int32_t tiledb_array_reopen(tiledb_ctx_t* ctx, tiledb_array_t* array) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  // Reopen array
  if (SAVE_ERROR_CATCH(ctx, array->array_->reopen()))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_array_reopen_at(
    tiledb_ctx_t* ctx, tiledb_array_t* array, uint64_t timestamp) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  // Reopen array
  if (SAVE_ERROR_CATCH(ctx, array->array_->reopen(timestamp)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_array_get_timestamp(
    tiledb_ctx_t* ctx, tiledb_array_t* array, uint64_t* timestamp) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  *timestamp = array->array_->timestamp();

  return TILEDB_OK;
}

int32_t tiledb_array_close(tiledb_ctx_t* ctx, tiledb_array_t* array) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  // Close array
  if (SAVE_ERROR_CATCH(ctx, array->array_->close()))
    return TILEDB_ERR;

  return TILEDB_OK;
}

void tiledb_array_free(tiledb_array_t** array) {
  if (array != nullptr && *array != nullptr) {
    delete (*array)->array_;
    delete *array;
    *array = nullptr;
  }
}

int32_t tiledb_array_get_schema(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_array_schema_t** array_schema) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  *array_schema = new (std::nothrow) tiledb_array_schema_t;
  if (*array_schema == nullptr) {
    auto st =
        tiledb::sm::Status::Error("Failed to allocate TileDB array schema");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Get schema
  auto schema = (tiledb::sm::ArraySchema*)nullptr;
  if (SAVE_ERROR_CATCH(ctx, array->array_->get_array_schema(&schema))) {
    delete *array_schema;
    *array_schema = nullptr;
    return TILEDB_ERR;
  }

  (*array_schema)->array_schema_ =
      new (std::nothrow) tiledb::sm::ArraySchema(schema);

  return TILEDB_OK;
}

int32_t tiledb_array_get_query_type(
    tiledb_ctx_t* ctx, tiledb_array_t* array, tiledb_query_type_t* query_type) {
  // Sanity checks
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  // Get query_type
  tiledb::sm::QueryType type;
  if (SAVE_ERROR_CATCH(ctx, array->array_->get_query_type(&type)))
    return TILEDB_ERR;

  *query_type = static_cast<tiledb_query_type_t>(type);

  return TILEDB_OK;
}

int32_t tiledb_array_create(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    const tiledb_array_schema_t* array_schema) {
  return tiledb_array_create_with_key(
      ctx, array_uri, array_schema, TILEDB_NO_ENCRYPTION, nullptr, 0);
}

int32_t tiledb_array_create_with_key(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    const tiledb_array_schema_t* array_schema,
    tiledb_encryption_type_t encryption_type,
    const void* encryption_key,
    uint32_t key_length) {
  // Sanity checks
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;

  // Check array name
  tiledb::sm::URI uri(array_uri);
  if (uri.is_invalid()) {
    auto st =
        tiledb::sm::Status::Error("Failed to create array; Invalid array URI");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  if (uri.is_tiledb()) {
    // Check unencrypted
    if (encryption_type != TILEDB_NO_ENCRYPTION) {
      auto st = tiledb::sm::Status::Error(
          "Failed to create array; encrypted remote arrays are not supported.");
      LOG_STATUS(st);
      save_error(ctx, st);
      return TILEDB_ERR;
    }

    // Check REST client
    auto rest_client = ctx->ctx_->storage_manager()->rest_client();
    if (rest_client == nullptr) {
      auto st = tiledb::sm::Status::Error(
          "Failed to create array; remote array with no REST client.");
      LOG_STATUS(st);
      save_error(ctx, st);
      return TILEDB_ERR;
    }

    if (SAVE_ERROR_CATCH(
            ctx,
            rest_client->post_array_schema_to_rest(
                uri, array_schema->array_schema_)))
      return TILEDB_ERR;
  } else {
    // Create key
    tiledb::sm::EncryptionKey key;
    if (SAVE_ERROR_CATCH(
            ctx,
            key.set_key(
                static_cast<tiledb::sm::EncryptionType>(encryption_type),
                encryption_key,
                key_length)))
      return TILEDB_ERR;

    // Create the array
    if (SAVE_ERROR_CATCH(
            ctx,
            ctx->ctx_->storage_manager()->array_create(
                uri, array_schema->array_schema_, key)))
      return TILEDB_ERR;
  }
  return TILEDB_OK;
}

int32_t tiledb_array_consolidate(
    tiledb_ctx_t* ctx, const char* array_uri, tiledb_config_t* config) {
  return tiledb_array_consolidate_with_key(
      ctx, array_uri, TILEDB_NO_ENCRYPTION, nullptr, 0, config);
}

int32_t tiledb_array_consolidate_with_key(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_encryption_type_t encryption_type,
    const void* encryption_key,
    uint32_t key_length,
    tiledb_config_t* config) {
  // Sanity checks
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(
          ctx,
          ctx->ctx_->storage_manager()->array_consolidate(
              array_uri,
              static_cast<tiledb::sm::EncryptionType>(encryption_type),
              encryption_key,
              key_length,
              (config == nullptr) ? nullptr : config->config_)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_array_vacuum(
    tiledb_ctx_t* ctx, const char* array_uri, tiledb_config_t* config) {
  // Sanity checks
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(
          ctx,
          ctx->ctx_->storage_manager()->array_vacuum(
              array_uri, (config == nullptr) ? nullptr : config->config_)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_array_get_non_empty_domain(
    tiledb_ctx_t* ctx, tiledb_array_t* array, void* domain, int32_t* is_empty) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  bool is_empty_b;

  if (SAVE_ERROR_CATCH(
          ctx,
          ctx->ctx_->storage_manager()->array_get_non_empty_domain(
              array->array_, domain, &is_empty_b)))
    return TILEDB_ERR;

  *is_empty = (int32_t)is_empty_b;

  return TILEDB_OK;
}

int32_t tiledb_array_get_non_empty_domain_from_index(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    uint32_t idx,
    void* domain,
    int32_t* is_empty) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  bool is_empty_b;

  if (SAVE_ERROR_CATCH(
          ctx,
          ctx->ctx_->storage_manager()->array_get_non_empty_domain_from_index(
              array->array_, idx, domain, &is_empty_b)))
    return TILEDB_ERR;

  *is_empty = (int32_t)is_empty_b;

  return TILEDB_OK;
}

int32_t tiledb_array_get_non_empty_domain_from_name(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const char* name,
    void* domain,
    int32_t* is_empty) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  bool is_empty_b;

  if (SAVE_ERROR_CATCH(
          ctx,
          ctx->ctx_->storage_manager()->array_get_non_empty_domain_from_name(
              array->array_, name, domain, &is_empty_b)))
    return TILEDB_ERR;

  *is_empty = (int32_t)is_empty_b;

  return TILEDB_OK;
}

int32_t tiledb_array_get_non_empty_domain_var_size_from_index(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    uint32_t idx,
    uint64_t* start_size,
    uint64_t* end_size,
    int32_t* is_empty) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  bool is_empty_b = true;

  if (SAVE_ERROR_CATCH(
          ctx,
          ctx->ctx_->storage_manager()
              ->array_get_non_empty_domain_var_size_from_index(
                  array->array_, idx, start_size, end_size, &is_empty_b)))
    return TILEDB_ERR;

  *is_empty = (int32_t)is_empty_b;

  return TILEDB_OK;
}

int32_t tiledb_array_get_non_empty_domain_var_size_from_name(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const char* name,
    uint64_t* start_size,
    uint64_t* end_size,
    int32_t* is_empty) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  bool is_empty_b = true;

  if (SAVE_ERROR_CATCH(
          ctx,
          ctx->ctx_->storage_manager()
              ->array_get_non_empty_domain_var_size_from_name(
                  array->array_, name, start_size, end_size, &is_empty_b)))
    return TILEDB_ERR;

  *is_empty = (int32_t)is_empty_b;

  return TILEDB_OK;
}

int32_t tiledb_array_get_non_empty_domain_var_from_index(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    uint32_t idx,
    void* start,
    void* end,
    int32_t* is_empty) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  bool is_empty_b = true;

  if (SAVE_ERROR_CATCH(
          ctx,
          ctx->ctx_->storage_manager()
              ->array_get_non_empty_domain_var_from_index(
                  array->array_, idx, start, end, &is_empty_b)))
    return TILEDB_ERR;

  *is_empty = (int32_t)is_empty_b;

  return TILEDB_OK;
}

int32_t tiledb_array_get_non_empty_domain_var_from_name(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const char* name,
    void* start,
    void* end,
    int32_t* is_empty) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  bool is_empty_b = true;

  if (SAVE_ERROR_CATCH(
          ctx,
          ctx->ctx_->storage_manager()
              ->array_get_non_empty_domain_var_from_name(
                  array->array_, name, start, end, &is_empty_b)))
    return TILEDB_ERR;

  *is_empty = (int32_t)is_empty_b;

  return TILEDB_OK;
}

int32_t tiledb_array_max_buffer_size(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const char* name,
    const void* subarray,
    uint64_t* buffer_size) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(
          ctx, array->array_->get_max_buffer_size(name, subarray, buffer_size)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_array_max_buffer_size_var(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const char* name,
    const void* subarray,
    uint64_t* buffer_off_size,
    uint64_t* buffer_val_size) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(
          ctx,
          array->array_->get_max_buffer_size(
              name, subarray, buffer_off_size, buffer_val_size)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_array_get_uri(
    tiledb_ctx_t* ctx, tiledb_array_t* array, const char** array_uri) {
  // Sanity checks
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  *array_uri = array->array_->array_uri().c_str();

  return TILEDB_OK;
}

int32_t tiledb_array_encryption_type(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_encryption_type_t* encryption_type) {
  // Sanity checks
  if (sanity_check(ctx) == TILEDB_ERR || array_uri == nullptr ||
      encryption_type == nullptr)
    return TILEDB_ERR;

  tiledb::sm::EncryptionType enc;
  if (SAVE_ERROR_CATCH(
          ctx,
          ctx->ctx_->storage_manager()->array_get_encryption(array_uri, &enc)))
    return TILEDB_ERR;

  *encryption_type = static_cast<tiledb_encryption_type_t>(enc);

  return TILEDB_OK;
}

int32_t tiledb_array_put_metadata(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const char* key,
    tiledb_datatype_t value_type,
    uint32_t value_num,
    const void* value) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  // Put metadata
  if (SAVE_ERROR_CATCH(
          ctx,
          array->array_->put_metadata(
              key,
              static_cast<tiledb::sm::Datatype>(value_type),
              value_num,
              value)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_array_delete_metadata(
    tiledb_ctx_t* ctx, tiledb_array_t* array, const char* key) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  // Put metadata
  if (SAVE_ERROR_CATCH(ctx, array->array_->delete_metadata(key)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_array_get_metadata(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const char* key,
    tiledb_datatype_t* value_type,
    uint32_t* value_num,
    const void** value) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  // Get metadata
  tiledb::sm::Datatype type;
  if (SAVE_ERROR_CATCH(
          ctx, array->array_->get_metadata(key, &type, value_num, value)))
    return TILEDB_ERR;

  *value_type = static_cast<tiledb_datatype_t>(type);

  return TILEDB_OK;
}

int32_t tiledb_array_get_metadata_num(
    tiledb_ctx_t* ctx, tiledb_array_t* array, uint64_t* num) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  // Get metadata num
  if (SAVE_ERROR_CATCH(ctx, array->array_->get_metadata_num(num)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_array_get_metadata_from_index(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    uint64_t index,
    const char** key,
    uint32_t* key_len,
    tiledb_datatype_t* value_type,
    uint32_t* value_num,
    const void** value) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  // Get metadata
  tiledb::sm::Datatype type;
  if (SAVE_ERROR_CATCH(
          ctx,
          array->array_->get_metadata(
              index, key, key_len, &type, value_num, value)))
    return TILEDB_ERR;

  *value_type = static_cast<tiledb_datatype_t>(type);

  return TILEDB_OK;
}

int32_t tiledb_array_has_metadata_key(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const char* key,
    tiledb_datatype_t* value_type,
    int32_t* has_key) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  // Check whether metadata has_key
  bool has_the_key;
  tiledb::sm::Datatype type;
  if (SAVE_ERROR_CATCH(
          ctx, array->array_->has_metadata_key(key, &type, &has_the_key)))
    return TILEDB_ERR;

  *has_key = has_the_key ? 1 : 0;
  if (has_the_key) {
    *value_type = static_cast<tiledb_datatype_t>(type);
  }
  return TILEDB_OK;
}

int32_t tiledb_array_consolidate_metadata(
    tiledb_ctx_t* ctx, const char* array_uri, tiledb_config_t* config) {
  // Sanity checks
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(
          ctx,
          ctx->ctx_->storage_manager()->array_metadata_consolidate(
              array_uri,
              static_cast<tiledb::sm::EncryptionType>(TILEDB_NO_ENCRYPTION),
              nullptr,
              0,
              (config == nullptr) ? nullptr : config->config_)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_array_consolidate_metadata_with_key(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_encryption_type_t encryption_type,
    const void* encryption_key,
    uint32_t key_length,
    tiledb_config_t* config) {
  // Sanity checks
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(
          ctx,
          ctx->ctx_->storage_manager()->array_metadata_consolidate(
              array_uri,
              static_cast<tiledb::sm::EncryptionType>(encryption_type),
              encryption_key,
              key_length,
              (config == nullptr) ? nullptr : config->config_)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

/* ****************************** */
/*         OBJECT MANAGEMENT      */
/* ****************************** */

int32_t tiledb_object_type(
    tiledb_ctx_t* ctx, const char* path, tiledb_object_t* type) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  auto uri = tiledb::sm::URI(path);
  tiledb::sm::ObjectType object_type;
  if (SAVE_ERROR_CATCH(
          ctx, ctx->ctx_->storage_manager()->object_type(uri, &object_type)))
    return TILEDB_ERR;

  *type = static_cast<tiledb_object_t>(object_type);
  return TILEDB_OK;
}

int32_t tiledb_object_remove(tiledb_ctx_t* ctx, const char* path) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;
  if (SAVE_ERROR_CATCH(ctx, ctx->ctx_->storage_manager()->object_remove(path)))
    return TILEDB_ERR;
  return TILEDB_OK;
}

int32_t tiledb_object_move(
    tiledb_ctx_t* ctx, const char* old_path, const char* new_path) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;
  if (SAVE_ERROR_CATCH(
          ctx, ctx->ctx_->storage_manager()->object_move(old_path, new_path)))
    return TILEDB_ERR;
  return TILEDB_OK;
}

int32_t tiledb_object_walk(
    tiledb_ctx_t* ctx,
    const char* path,
    tiledb_walk_order_t order,
    int32_t (*callback)(const char*, tiledb_object_t, void*),
    void* data) {
  // Sanity checks
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;
  if (callback == nullptr) {
    tiledb::sm::Status st = tiledb::sm::Status::Error(
        "Cannot initiate walk; Invalid callback function");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  // Create an object iterator
  tiledb::sm::StorageManager::ObjectIter* obj_iter;
  if (SAVE_ERROR_CATCH(
          ctx,
          ctx->ctx_->storage_manager()->object_iter_begin(
              &obj_iter, path, static_cast<tiledb::sm::WalkOrder>(order))))
    return TILEDB_ERR;

  // For as long as there is another object and the callback indicates to
  // continue, walk over the TileDB objects in the path
  const char* obj_name;
  tiledb::sm::ObjectType obj_type;
  bool has_next;
  int32_t rc = 0;
  do {
    if (SAVE_ERROR_CATCH(
            ctx,
            ctx->ctx_->storage_manager()->object_iter_next(
                obj_iter, &obj_name, &obj_type, &has_next))) {
      ctx->ctx_->storage_manager()->object_iter_free(obj_iter);
      return TILEDB_ERR;
    }
    if (!has_next)
      break;
    rc = callback(obj_name, tiledb_object_t(obj_type), data);
  } while (rc == 1);

  // Clean up
  ctx->ctx_->storage_manager()->object_iter_free(obj_iter);

  if (rc == -1)
    return TILEDB_ERR;
  return TILEDB_OK;
}

int32_t tiledb_object_ls(
    tiledb_ctx_t* ctx,
    const char* path,
    int32_t (*callback)(const char*, tiledb_object_t, void*),
    void* data) {
  // Sanity checks
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;
  if (callback == nullptr) {
    auto st = tiledb::sm::Status::Error(
        "Cannot initiate object ls; Invalid callback function");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  // Create an object iterator
  tiledb::sm::StorageManager::ObjectIter* obj_iter;
  if (SAVE_ERROR_CATCH(
          ctx,
          ctx->ctx_->storage_manager()->object_iter_begin(&obj_iter, path)))
    return TILEDB_ERR;

  // For as long as there is another object and the callback indicates to
  // continue, walk over the TileDB objects in the path
  const char* obj_name;
  tiledb::sm::ObjectType obj_type;
  bool has_next;
  int32_t rc = 0;
  do {
    if (SAVE_ERROR_CATCH(
            ctx,
            ctx->ctx_->storage_manager()->object_iter_next(
                obj_iter, &obj_name, &obj_type, &has_next))) {
      ctx->ctx_->storage_manager()->object_iter_free(obj_iter);
      return TILEDB_ERR;
    }
    if (!has_next)
      break;
    rc = callback(obj_name, tiledb_object_t(obj_type), data);
  } while (rc == 1);

  // Clean up
  ctx->ctx_->storage_manager()->object_iter_free(obj_iter);

  if (rc == -1)
    return TILEDB_ERR;
  return TILEDB_OK;
}

/* ****************************** */
/*        VIRTUAL FILESYSTEM      */
/* ****************************** */

int32_t tiledb_vfs_alloc(
    tiledb_ctx_t* ctx, tiledb_config_t* config, tiledb_vfs_t** vfs) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  if (config != nullptr && config->config_ == nullptr) {
    auto st = tiledb::sm::Status::Error("Cannot create VFS; Invalid config");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  // Create VFS struct
  *vfs = new (std::nothrow) tiledb_vfs_t;
  if (*vfs == nullptr) {
    tiledb::sm::Status st = tiledb::sm::Status::Error(
        "Failed to allocate TileDB virtual filesystem object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Create VFS object
  (*vfs)->vfs_ = new tiledb::sm::VFS();
  if ((*vfs)->vfs_ == nullptr) {
    tiledb::sm::Status st = tiledb::sm::Status::Error(
        "Failed to allocate TileDB virtual filesystem object");
    LOG_STATUS(st);
    save_error(ctx, st);
    delete *vfs;
    return TILEDB_OOM;
  }

  // Initialize VFS object
  auto vfs_config = config ? config->config_ : nullptr;
  auto ctx_config = ctx->ctx_->storage_manager()->config();
  if (SAVE_ERROR_CATCH(ctx, (*vfs)->vfs_->init(&ctx_config, vfs_config))) {
    delete (*vfs)->vfs_;
    delete vfs;
    return TILEDB_ERR;
  }

  // Success
  return TILEDB_OK;
}

void tiledb_vfs_free(tiledb_vfs_t** vfs) {
  const tiledb::sm::Status st = (*vfs)->vfs_->terminate();
  if (!st.ok()) {
    LOG_STATUS(st);
  }

  if (vfs != nullptr && *vfs != nullptr) {
    delete (*vfs)->vfs_;
    delete *vfs;
    *vfs = nullptr;
  }
}

int32_t tiledb_vfs_get_config(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, tiledb_config_t** config) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, vfs) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create a new config struct
  *config = new (std::nothrow) tiledb_config_t;
  if (*config == nullptr)
    return TILEDB_OOM;

  // Create storage manager
  (*config)->config_ = new (std::nothrow) tiledb::sm::Config();
  if ((*config)->config_ == nullptr) {
    delete (*config);
    return TILEDB_OOM;
  }

  *((*config)->config_) = vfs->vfs_->config();

  // Success
  return TILEDB_OK;
}

int32_t tiledb_vfs_create_bucket(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, vfs) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(ctx, vfs->vfs_->create_bucket(tiledb::sm::URI(uri))))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_vfs_remove_bucket(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, vfs) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(ctx, vfs->vfs_->remove_bucket(tiledb::sm::URI(uri))))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_vfs_empty_bucket(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, vfs) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(ctx, vfs->vfs_->empty_bucket(tiledb::sm::URI(uri))))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_vfs_is_empty_bucket(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri, int32_t* is_empty) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, vfs) == TILEDB_ERR)
    return TILEDB_ERR;

  bool b;
  if (SAVE_ERROR_CATCH(
          ctx, vfs->vfs_->is_empty_bucket(tiledb::sm::URI(uri), &b)))
    return TILEDB_ERR;
  *is_empty = (int32_t)b;

  return TILEDB_OK;
}

int32_t tiledb_vfs_is_bucket(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri, int32_t* is_bucket) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, vfs) == TILEDB_ERR)
    return TILEDB_ERR;

  bool exists;
  if (SAVE_ERROR_CATCH(
          ctx, vfs->vfs_->is_bucket(tiledb::sm::URI(uri), &exists)))
    return TILEDB_ERR;

  *is_bucket = (int32_t)exists;

  return TILEDB_OK;
}

int32_t tiledb_vfs_create_dir(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, vfs) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(ctx, vfs->vfs_->create_dir(tiledb::sm::URI(uri))))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_vfs_is_dir(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri, int32_t* is_dir) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, vfs) == TILEDB_ERR)
    return TILEDB_ERR;

  bool exists;
  if (SAVE_ERROR_CATCH(ctx, vfs->vfs_->is_dir(tiledb::sm::URI(uri), &exists)))
    return TILEDB_ERR;
  *is_dir = (int32_t)exists;

  return TILEDB_OK;
}

int32_t tiledb_vfs_remove_dir(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, vfs) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(ctx, vfs->vfs_->remove_dir(tiledb::sm::URI(uri))))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_vfs_is_file(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri, int32_t* is_file) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, vfs) == TILEDB_ERR)
    return TILEDB_ERR;

  bool exists;
  if (SAVE_ERROR_CATCH(ctx, vfs->vfs_->is_file(tiledb::sm::URI(uri), &exists)))
    return TILEDB_ERR;
  *is_file = (int32_t)exists;

  return TILEDB_OK;
}

int32_t tiledb_vfs_remove_file(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, vfs) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(ctx, vfs->vfs_->remove_file(tiledb::sm::URI(uri))))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_vfs_dir_size(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri, uint64_t* size) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, vfs) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(ctx, vfs->vfs_->dir_size(tiledb::sm::URI(uri), size)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_vfs_file_size(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri, uint64_t* size) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, vfs) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(ctx, vfs->vfs_->file_size(tiledb::sm::URI(uri), size)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_vfs_move_file(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* old_uri,
    const char* new_uri) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, vfs) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(
          ctx,
          vfs->vfs_->move_file(
              tiledb::sm::URI(old_uri), tiledb::sm::URI(new_uri))))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_vfs_move_dir(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* old_uri,
    const char* new_uri) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, vfs) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(
          ctx,
          vfs->vfs_->move_dir(
              tiledb::sm::URI(old_uri), tiledb::sm::URI(new_uri))))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_vfs_open(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* uri,
    tiledb_vfs_mode_t mode,
    tiledb_vfs_fh_t** fh) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, vfs) == TILEDB_ERR)
    return TILEDB_ERR;

  *fh = new (std::nothrow) tiledb_vfs_fh_t;
  if (*fh == nullptr) {
    auto st = tiledb::sm::Status::Error(
        "Failed to create TileDB VFS file handle; Memory allocation error");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Check URI
  auto fh_uri = tiledb::sm::URI(uri);
  if (fh_uri.is_invalid()) {
    auto st = tiledb::sm::Status::Error(
        "Failed to create TileDB VFS file handle; Invalid URI");
    delete *fh;
    *fh = nullptr;
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  auto vfs_mode = static_cast<tiledb::sm::VFSMode>(mode);

  // Create VFS file handle
  (*fh)->vfs_fh_ =
      new (std::nothrow) tiledb::sm::VFSFileHandle(fh_uri, vfs->vfs_, vfs_mode);
  if ((*fh)->vfs_fh_ == nullptr) {
    auto st = tiledb::sm::Status::Error(
        "Failed to create TileDB VFS file handle; Memory allocation error");
    LOG_STATUS(st);
    save_error(ctx, st);
    delete (*fh);
    *fh = nullptr;
    return TILEDB_OOM;
  }

  // Open VFS file
  if (SAVE_ERROR_CATCH(ctx, (*fh)->vfs_fh_->open())) {
    delete (*fh)->vfs_fh_;
    delete (*fh);
    *fh = nullptr;
    return TILEDB_ERR;
  }

  return TILEDB_OK;
}

int32_t tiledb_vfs_close(tiledb_ctx_t* ctx, tiledb_vfs_fh_t* fh) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, fh) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(ctx, fh->vfs_fh_->close()))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_vfs_read(
    tiledb_ctx_t* ctx,
    tiledb_vfs_fh_t* fh,
    uint64_t offset,
    void* buffer,
    uint64_t nbytes) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, fh) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(ctx, fh->vfs_fh_->read(offset, buffer, nbytes)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_vfs_write(
    tiledb_ctx_t* ctx,
    tiledb_vfs_fh_t* fh,
    const void* buffer,
    uint64_t nbytes) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, fh) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(ctx, fh->vfs_fh_->write(buffer, nbytes)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_vfs_sync(tiledb_ctx_t* ctx, tiledb_vfs_fh_t* fh) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, fh) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(ctx, fh->vfs_fh_->sync()))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_vfs_ls(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* path,
    int32_t (*callback)(const char*, void*),
    void* data) {
  // Sanity checks
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;
  if (callback == nullptr) {
    auto st = tiledb::sm::Status::Error(
        "Cannot initiate VFS ls; Invalid callback function");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  // Get children
  std::vector<tiledb::sm::URI> children;
  auto st = vfs->vfs_->ls(tiledb::sm::URI(path), &children);

  if (!st.ok())
    return TILEDB_ERR;

  // Apply the callback to every child
  int rc = 1;
  for (const auto& uri : children) {
    rc = callback(uri.to_string().c_str(), data);
    if (rc != 1)
      break;
  }

  if (rc == -1)
    return TILEDB_ERR;
  return TILEDB_OK;
}

void tiledb_vfs_fh_free(tiledb_vfs_fh_t** fh) {
  if (fh != nullptr && *fh != nullptr) {
    delete (*fh)->vfs_fh_;
    delete *fh;
    *fh = nullptr;
  }
}

int32_t tiledb_vfs_fh_is_closed(
    tiledb_ctx_t* ctx, tiledb_vfs_fh_t* fh, int32_t* is_closed) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, fh) == TILEDB_ERR)
    return TILEDB_ERR;

  *is_closed = !fh->vfs_fh_->is_open();

  return TILEDB_OK;
}

int32_t tiledb_vfs_touch(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, vfs) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(ctx, vfs->vfs_->touch(tiledb::sm::URI(uri))))
    return TILEDB_ERR;

  return TILEDB_OK;
}

/* ****************************** */
/*              URI               */
/* ****************************** */

int32_t tiledb_uri_to_path(
    tiledb_ctx_t* ctx, const char* uri, char* path_out, uint32_t* path_length) {
  if (sanity_check(ctx) == TILEDB_ERR || uri == nullptr ||
      path_out == nullptr || path_length == nullptr)
    return TILEDB_ERR;

  std::string path = tiledb::sm::URI::to_path(uri);
  if (path.empty() || path.length() + 1 > *path_length) {
    *path_length = 0;
    return TILEDB_ERR;
  } else {
    *path_length = static_cast<uint32_t>(path.length());
    path.copy(path_out, path.length());
    path_out[path.length()] = '\0';
    return TILEDB_OK;
  }
}

/* ****************************** */
/*             Stats              */
/* ****************************** */

int32_t tiledb_stats_enable() {
  tiledb::sm::stats::all_stats.set_enabled(true);
  return TILEDB_OK;
}

int32_t tiledb_stats_disable() {
  tiledb::sm::stats::all_stats.set_enabled(false);
  return TILEDB_OK;
}

int32_t tiledb_stats_reset() {
  tiledb::sm::stats::all_stats.reset();
  return TILEDB_OK;
}

int32_t tiledb_stats_dump(FILE* out) {
  tiledb::sm::stats::all_stats.dump(out);
  return TILEDB_OK;
}

int32_t tiledb_stats_dump_str(char** out) {
  if (out == nullptr)
    return TILEDB_ERR;

  std::string str;
  tiledb::sm::stats::all_stats.dump(&str);

  *out = static_cast<char*>(std::malloc(str.size() + 1));
  if (*out == nullptr)
    return TILEDB_ERR;

  std::memcpy(*out, str.data(), str.size());
  (*out)[str.size()] = '\0';

  return TILEDB_OK;
}

int32_t tiledb_stats_raw_dump(FILE* out) {
  tiledb::sm::stats::all_stats.raw_dump(out);
  return TILEDB_OK;
}

int32_t tiledb_stats_raw_dump_str(char** out) {
  if (out == nullptr)
    return TILEDB_ERR;

  std::string str;
  tiledb::sm::stats::all_stats.raw_dump(&str);

  *out = static_cast<char*>(std::malloc(str.size() + 1));
  if (*out == nullptr)
    return TILEDB_ERR;

  std::memcpy(*out, str.data(), str.size());
  (*out)[str.size()] = '\0';

  return TILEDB_OK;
}

int32_t tiledb_stats_free_str(char** out) {
  if (out != nullptr) {
    std::free(*out);
    *out = nullptr;
  }
  return TILEDB_OK;
}

/* ****************************** */
/*          Serialization         */
/* ****************************** */

int32_t tiledb_serialize_array_schema(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_buffer_t** buffer) {
  // Currently unused:
  (void)client_side;

  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create buffer
  if (tiledb_buffer_alloc(ctx, buffer) != TILEDB_OK ||
      sanity_check(ctx, *buffer) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::array_schema_serialize(
              array_schema->array_schema_,
              (tiledb::sm::SerializationType)serialize_type,
              (*buffer)->buffer_)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_deserialize_array_schema(
    tiledb_ctx_t* ctx,
    const tiledb_buffer_t* buffer,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_array_schema_t** array_schema) {
  // Currently unused:
  (void)client_side;

  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, buffer) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create array schema struct
  *array_schema = new (std::nothrow) tiledb_array_schema_t;
  if (*array_schema == nullptr) {
    auto st = tiledb::sm::Status::Error(
        "Failed to allocate TileDB array schema object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::array_schema_deserialize(
              &((*array_schema)->array_schema_),
              (tiledb::sm::SerializationType)serialize_type,
              *buffer->buffer_))) {
    delete *array_schema;
    return TILEDB_ERR;
  }

  return TILEDB_OK;
}

int32_t tiledb_serialize_query(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_buffer_list_t** buffer_list) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  // Allocate a buffer list
  if (tiledb_buffer_list_alloc(ctx, buffer_list) != TILEDB_OK ||
      sanity_check(ctx, *buffer_list) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::query_serialize(
              query->query_,
              (tiledb::sm::SerializationType)serialize_type,
              client_side == 1,
              (*buffer_list)->buffer_list_)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_deserialize_query(
    tiledb_ctx_t* ctx,
    const tiledb_buffer_t* buffer,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_query_t* query) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, query) == TILEDB_ERR ||
      sanity_check(ctx, buffer) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::query_deserialize(
              *buffer->buffer_,
              (tiledb::sm::SerializationType)serialize_type,
              client_side == 1,
              nullptr,
              query->query_)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_serialize_array_nonempty_domain(
    tiledb_ctx_t* ctx,
    const tiledb_array_t* array,
    const void* nonempty_domain,
    int32_t is_empty,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_buffer_t** buffer) {
  // Currently unused:
  (void)client_side;

  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create buffer
  if (tiledb_buffer_alloc(ctx, buffer) != TILEDB_OK ||
      sanity_check(ctx, *buffer) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::nonempty_domain_serialize(
              array->array_,
              nonempty_domain,
              is_empty,
              (tiledb::sm::SerializationType)serialize_type,
              (*buffer)->buffer_)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_deserialize_array_nonempty_domain(
    tiledb_ctx_t* ctx,
    const tiledb_array_t* array,
    const tiledb_buffer_t* buffer,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    void* nonempty_domain,
    int32_t* is_empty) {
  // Currently unused:
  (void)client_side;

  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array) == TILEDB_ERR ||
      sanity_check(ctx, buffer) == TILEDB_ERR)
    return TILEDB_ERR;

  bool is_empty_bool;
  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::nonempty_domain_deserialize(
              array->array_,
              *buffer->buffer_,
              (tiledb::sm::SerializationType)serialize_type,
              nonempty_domain,
              &is_empty_bool)))
    return TILEDB_ERR;

  *is_empty = is_empty_bool ? 1 : 0;

  return TILEDB_OK;
}

int32_t tiledb_serialize_array_non_empty_domain_all_dimensions(
    tiledb_ctx_t* ctx,
    const tiledb_array_t* array,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_buffer_t** buffer) {
  // Currently unused:
  (void)client_side;

  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create buffer
  if (tiledb_buffer_alloc(ctx, buffer) != TILEDB_OK ||
      sanity_check(ctx, *buffer) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::nonempty_domain_serialize(
              array->array_,
              (tiledb::sm::SerializationType)serialize_type,
              (*buffer)->buffer_)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_deserialize_array_non_empty_domain_all_dimensions(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const tiledb_buffer_t* buffer,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side) {
  // Currently unused:
  (void)client_side;

  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array) == TILEDB_ERR ||
      sanity_check(ctx, buffer) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::nonempty_domain_deserialize(
              array->array_,
              *buffer->buffer_,
              (tiledb::sm::SerializationType)serialize_type)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_serialize_array_max_buffer_sizes(
    tiledb_ctx_t* ctx,
    const tiledb_array_t* array,
    const void* subarray,
    tiledb_serialization_type_t serialize_type,
    tiledb_buffer_t** buffer) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  // Allocate buffer
  if (tiledb_buffer_alloc(ctx, buffer) != TILEDB_OK ||
      sanity_check(ctx, *buffer) == TILEDB_ERR)
    return TILEDB_ERR;

  // Serialize
  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::max_buffer_sizes_serialize(
              array->array_,
              subarray,
              (tiledb::sm::SerializationType)serialize_type,
              (*buffer)->buffer_))) {
    tiledb_buffer_free(buffer);
    return TILEDB_ERR;
  }

  return TILEDB_OK;
}

int32_t tiledb_serialize_array_metadata(
    tiledb_ctx_t* ctx,
    const tiledb_array_t* array,
    tiledb_serialization_type_t serialize_type,
    tiledb_buffer_t** buffer) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  // Allocate buffer
  if (tiledb_buffer_alloc(ctx, buffer) != TILEDB_OK ||
      sanity_check(ctx, *buffer) == TILEDB_ERR)
    return TILEDB_ERR;

  // Serialize
  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::array_metadata_serialize(
              array->array_,
              (tiledb::sm::SerializationType)serialize_type,
              (*buffer)->buffer_))) {
    tiledb_buffer_free(buffer);
    return TILEDB_ERR;
  }

  return TILEDB_OK;
}

int32_t tiledb_deserialize_array_metadata(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_serialization_type_t serialize_type,
    const tiledb_buffer_t* buffer) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array) == TILEDB_ERR ||
      sanity_check(ctx, buffer) == TILEDB_ERR)
    return TILEDB_ERR;

  // Deserialize
  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::array_metadata_deserialize(
              array->array_,
              (tiledb::sm::SerializationType)serialize_type,
              *(buffer->buffer_)))) {
    return TILEDB_ERR;
  }

  return TILEDB_OK;
}

int32_t tiledb_serialize_query_est_result_sizes(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_buffer_t** buffer) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  // Allocate buffer
  if (tiledb_buffer_alloc(ctx, buffer) != TILEDB_OK ||
      sanity_check(ctx, *buffer) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::query_est_result_size_serialize(
              query->query_,
              (tiledb::sm::SerializationType)serialize_type,
              client_side == 1,
              (*buffer)->buffer_)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_deserialize_query_est_result_sizes(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    const tiledb_buffer_t* buffer) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, query) == TILEDB_ERR ||
      sanity_check(ctx, buffer) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::query_est_result_size_deserialize(
              query->query_,
              (tiledb::sm::SerializationType)serialize_type,
              client_side == 1,
              *buffer->buffer_)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

/* ****************************** */
/*            C++ API             */
/* ****************************** */

int32_t tiledb::impl::tiledb_query_submit_async_func(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    void* callback_func,
    void* callback_data) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, query) == TILEDB_ERR || callback_func == nullptr)
    return TILEDB_ERR;

  std::function<void(void*)> callback =
      *reinterpret_cast<std::function<void(void*)>*>(callback_func);

  if (SAVE_ERROR_CATCH(
          ctx, query->query_->submit_async(callback, callback_data)))
    return TILEDB_ERR;

  return TILEDB_OK;
}
