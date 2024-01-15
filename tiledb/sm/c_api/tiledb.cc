/**
 * @file   tiledb.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2023 TileDB, Inc.
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

#include "tiledb.h"
#include "tiledb_experimental.h"
#include "tiledb_serialization.h"
#include "tiledb_struct_def.h"

#include "tiledb/api/c_api/attribute/attribute_api_internal.h"
#include "tiledb/api/c_api/buffer/buffer_api_internal.h"
#include "tiledb/api/c_api/buffer_list/buffer_list_api_internal.h"
#include "tiledb/api/c_api/config/config_api_internal.h"
#include "tiledb/api/c_api/dimension/dimension_api_internal.h"
#include "tiledb/api/c_api/domain/domain_api_internal.h"
#include "tiledb/api/c_api/enumeration/enumeration_api_internal.h"
#include "tiledb/api/c_api/error/error_api_internal.h"
#include "tiledb/api/c_api/filter_list/filter_list_api_internal.h"
#include "tiledb/api/c_api/string/string_api_internal.h"
#include "tiledb/api/c_api_support/c_api_support.h"
#include "tiledb/as_built/as_built.h"
#include "tiledb/common/common.h"
#include "tiledb/common/dynamic_memory/dynamic_memory.h"
#include "tiledb/common/heap_profiler.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/dimension_label.h"
#include "tiledb/sm/c_api/api_argument_validator.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/config/config_iter.h"
#include "tiledb/sm/cpp_api/core_interface.h"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/filesystem.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/enums/mime_type.h"
#include "tiledb/sm/enums/object_type.h"
#include "tiledb/sm/enums/query_status.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/filter/filter_pipeline.h"
#include "tiledb/sm/misc/tdb_time.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/sm/query/query_condition.h"
#include "tiledb/sm/rest/rest_client.h"
#include "tiledb/sm/serialization/array.h"
#include "tiledb/sm/serialization/array_schema.h"
#include "tiledb/sm/serialization/array_schema_evolution.h"
#include "tiledb/sm/serialization/config.h"
#include "tiledb/sm/serialization/consolidation.h"
#include "tiledb/sm/serialization/enumeration.h"
#include "tiledb/sm/serialization/fragment_info.h"
#include "tiledb/sm/serialization/fragments.h"
#include "tiledb/sm/serialization/query.h"
#include "tiledb/sm/serialization/query_plan.h"
#include "tiledb/sm/stats/global_stats.h"
#include "tiledb/sm/storage_manager/context.h"
#include "tiledb/sm/subarray/subarray.h"
#include "tiledb/sm/subarray/subarray_partitioner.h"

#include <memory>
#include <sstream>

/**
 * Helper class to aid shimming access from _query... routines in this module to
 * _subarray... routines deprecating them.
 */

struct tiledb_subarray_transient_local_t : public tiledb_subarray_t {
  explicit tiledb_subarray_transient_local_t(const tiledb_query_t* query) {
    this->subarray_ =
        const_cast<tiledb::sm::Subarray*>(query->query_->subarray());
  }
};

/*
 * The Definition for a "C" function can't be in a header.
 */
capi_status_t tiledb_status_code(capi_return_t x) {
  return tiledb_status(x);  // An inline C++ function
}

/* ****************************** */
/*  IMPLEMENTATION FUNCTIONS      */
/* ****************************** */
/*
 * The `tiledb::api` namespace block contains all the implementations of the C
 * API functions defined below. The C API interface functions themselves are in
 * the global namespace and each wraps its implementation function using one of
 * the API transformers.
 *
 * Each C API function requires an implementation function defined in this block
 * and a corresponding wrapped C API function below. The convention reuses
 * `function_name` in two namespaces. We have a `tiledb::api::function_name`
 * definition for the unwrapped function and a `function_name` definition for
 * the wrapped function.
 */
namespace tiledb::api {

/* ****************************** */
/*       ENUMS TO/FROM STR        */
/* ****************************** */

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

int32_t tiledb_encryption_type_to_str(
    tiledb_encryption_type_t encryption_type, const char** str) {
  const auto& strval = tiledb::sm::encryption_type_str(
      (tiledb::sm::EncryptionType)encryption_type);
  *str = strval.c_str();
  return strval.empty() ? TILEDB_ERR : TILEDB_OK;
}

int32_t tiledb_encryption_type_from_str(
    const char* str, tiledb_encryption_type_t* encryption_type) {
  auto [st, et] = tiledb::sm::encryption_type_enum(str);
  if (!st.ok()) {
    return TILEDB_ERR;
  }
  *encryption_type = (tiledb_encryption_type_t)et.value();
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

/* ********************************* */
/*             LOGGING               */
/* ********************************* */

capi_return_t tiledb_log_warn(tiledb_ctx_t* ctx, const char* message) {
  if (message == nullptr) {
    return TILEDB_ERR;
  }

  auto logger = ctx->storage_manager()->logger();
  logger->warn(message);

  return TILEDB_OK;
}

/* ********************************* */
/*              AS BUILT             */
/* ********************************* */
capi_return_t tiledb_as_built_dump(tiledb_string_t** out) {
  ensure_output_pointer_is_valid(out);
  *out = tiledb_string_handle_t::make_handle(as_built::dump());
  return TILEDB_OK;
}

/* ****************************** */
/*           ARRAY SCHEMA         */
/* ****************************** */

int32_t tiledb_array_schema_alloc(
    tiledb_ctx_t* ctx,
    tiledb_array_type_t array_type,
    tiledb_array_schema_t** array_schema) {
  // Create array schema struct
  *array_schema = new (std::nothrow) tiledb_array_schema_t;
  if (*array_schema == nullptr) {
    auto st = Status_Error("Failed to allocate TileDB array schema object");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Create a new ArraySchema object
  (*array_schema)->array_schema_ = make_shared<tiledb::sm::ArraySchema>(
      HERE(), static_cast<tiledb::sm::ArrayType>(array_type));
  if ((*array_schema)->array_schema_ == nullptr) {
    auto st = Status_Error("Failed to allocate TileDB array schema object");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Success
  return TILEDB_OK;
}

void tiledb_array_schema_free(tiledb_array_schema_t** array_schema) {
  if (array_schema != nullptr && *array_schema != nullptr) {
    delete *array_schema;
    *array_schema = nullptr;
  }
}

int32_t tiledb_array_schema_add_attribute(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_attribute_t* attr) {
  if (sanity_check(ctx, array_schema) == TILEDB_ERR) {
    return TILEDB_ERR;
  }
  ensure_attribute_is_valid(attr);
  /** Note: The call to make_shared creates a copy of the attribute and
   * the user-visible handle to the attr no longer refers to the same object
   * that's in the array_schema.
   **/
  throw_if_not_ok(
      array_schema->array_schema_->add_attribute(attr->copy_attribute()));
  return TILEDB_OK;
}

int32_t tiledb_array_schema_set_allows_dups(
    tiledb_ctx_t* ctx, tiledb_array_schema_t* array_schema, int allows_dups) {
  if (sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  throw_if_not_ok(array_schema->array_schema_->set_allows_dups(allows_dups));
  return TILEDB_OK;
}

int32_t tiledb_array_schema_get_allows_dups(
    tiledb_ctx_t* ctx, tiledb_array_schema_t* array_schema, int* allows_dups) {
  if (sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  *allows_dups = (int)array_schema->array_schema_->allows_dups();
  return TILEDB_OK;
}

int32_t tiledb_array_schema_get_version(
    tiledb_ctx_t* ctx, tiledb_array_schema_t* array_schema, uint32_t* version) {
  if (sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  *version = (uint32_t)array_schema->array_schema_->version();
  return TILEDB_OK;
}

int32_t tiledb_array_schema_set_domain(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_domain_t* domain) {
  if (sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  throw_if_not_ok(
      array_schema->array_schema_->set_domain(domain->copy_domain()));
  return TILEDB_OK;
}

int32_t tiledb_array_schema_set_capacity(
    tiledb_ctx_t* ctx, tiledb_array_schema_t* array_schema, uint64_t capacity) {
  if (sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  array_schema->array_schema_->set_capacity(capacity);
  return TILEDB_OK;
}

int32_t tiledb_array_schema_set_cell_order(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_layout_t cell_order) {
  if (sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  throw_if_not_ok(array_schema->array_schema_->set_cell_order(
      static_cast<tiledb::sm::Layout>(cell_order)));
  return TILEDB_OK;
}

int32_t tiledb_array_schema_set_tile_order(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_layout_t tile_order) {
  if (sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  throw_if_not_ok(array_schema->array_schema_->set_tile_order(
      static_cast<tiledb::sm::Layout>(tile_order)));
  return TILEDB_OK;
}

int32_t tiledb_array_schema_timestamp_range(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    uint64_t* lo,
    uint64_t* hi) {
  if (sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;

  auto timestamp_range = array_schema->array_schema_->timestamp_range();
  *lo = std::get<0>(timestamp_range);
  *hi = std::get<1>(timestamp_range);

  return TILEDB_OK;
}

int32_t tiledb_array_schema_add_enumeration(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_enumeration_t* enumeration) {
  if (sanity_check(ctx, array_schema) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  api::ensure_enumeration_is_valid(enumeration);

  array_schema->array_schema_->add_enumeration(enumeration->copy());

  return TILEDB_OK;
}

int32_t tiledb_array_schema_set_coords_filter_list(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_filter_list_t* filter_list) {
  if (sanity_check(ctx, array_schema) == TILEDB_ERR) {
    return TILEDB_ERR;
  }
  api::ensure_filter_list_is_valid(filter_list);

  throw_if_not_ok(array_schema->array_schema_->set_coords_filter_pipeline(
      filter_list->pipeline()));

  return TILEDB_OK;
}

int32_t tiledb_array_schema_set_offsets_filter_list(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_filter_list_t* filter_list) {
  if (sanity_check(ctx, array_schema) == TILEDB_ERR) {
    return TILEDB_ERR;
  }
  api::ensure_filter_list_is_valid(filter_list);

  throw_if_not_ok(
      array_schema->array_schema_->set_cell_var_offsets_filter_pipeline(
          filter_list->pipeline()));

  return TILEDB_OK;
}

int32_t tiledb_array_schema_set_validity_filter_list(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_filter_list_t* filter_list) {
  if (sanity_check(ctx, array_schema) == TILEDB_ERR) {
    return TILEDB_ERR;
  }
  api::ensure_filter_list_is_valid(filter_list);

  throw_if_not_ok(
      array_schema->array_schema_->set_cell_validity_filter_pipeline(
          filter_list->pipeline()));

  return TILEDB_OK;
}

int32_t tiledb_array_schema_check(
    tiledb_ctx_t* ctx, tiledb_array_schema_t* array_schema) {
  if (sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  array_schema->array_schema_->check(ctx->resources().config());
  return TILEDB_OK;
}

int32_t tiledb_array_schema_load(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_array_schema_t** array_schema) {
  // Create array schema
  *array_schema = new (std::nothrow) tiledb_array_schema_t;
  if (*array_schema == nullptr) {
    auto st = Status_Error("Failed to allocate TileDB array schema object");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Check array name
  tiledb::sm::URI uri(array_uri);
  if (uri.is_invalid()) {
    auto st = Status_Error("Failed to load array schema; Invalid array URI");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  if (uri.is_tiledb()) {
    // Check REST client
    auto rest_client = ctx->storage_manager()->rest_client();
    if (rest_client == nullptr) {
      auto st = Status_Error(
          "Failed to load array schema; remote array with no REST client.");
      LOG_STATUS_NO_RETURN_VALUE(st);
      save_error(ctx, st);
      return TILEDB_ERR;
    }

    auto&& [st, array_schema_rest] =
        rest_client->get_array_schema_from_rest(uri);
    if (!st.ok()) {
      LOG_STATUS_NO_RETURN_VALUE(st);
      save_error(ctx, st);
      delete *array_schema;
      return TILEDB_ERR;
    }
    (*array_schema)->array_schema_ = array_schema_rest.value();
  } else {
    // Create key
    tiledb::sm::EncryptionKey key;
    throw_if_not_ok(
        key.set_key(tiledb::sm::EncryptionType::NO_ENCRYPTION, nullptr, 0));

    // For easy reference
    auto storage_manager{ctx->storage_manager()};

    // Load URIs from the array directory
    tiledb::sm::ArrayDirectory array_dir(storage_manager->resources(), uri);
    try {
      array_dir = tiledb::sm::ArrayDirectory(
          storage_manager->resources(),
          uri,
          0,
          UINT64_MAX,
          tiledb::sm::ArrayDirectoryMode::SCHEMA_ONLY);
    } catch (const std::logic_error& le) {
      auto st = Status_ArrayDirectoryError(le.what());
      LOG_STATUS_NO_RETURN_VALUE(st);
      save_error(ctx, st);
      delete *array_schema;
      return TILEDB_ERR;
    }

    // Load latest array schema
    auto&& array_schema_latest = array_dir.load_array_schema_latest(key);
    (*array_schema)->array_schema_ = array_schema_latest;
  }
  return TILEDB_OK;
}

int32_t tiledb_array_schema_load_with_key(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_encryption_type_t encryption_type,
    const void* encryption_key,
    uint32_t key_length,
    tiledb_array_schema_t** array_schema) {
  // Create array schema
  *array_schema = new (std::nothrow) tiledb_array_schema_t;
  if (*array_schema == nullptr) {
    auto st = Status_Error("Failed to allocate TileDB array schema object");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Check array name
  tiledb::sm::URI uri(array_uri);
  if (uri.is_invalid()) {
    delete *array_schema;
    *array_schema = nullptr;
    auto st = Status_Error("Failed to load array schema; Invalid array URI");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  if (uri.is_tiledb()) {
    // Check REST client
    auto rest_client = ctx->storage_manager()->rest_client();
    if (rest_client == nullptr) {
      delete *array_schema;
      *array_schema = nullptr;
      auto st = Status_Error(
          "Failed to load array schema; remote array with no REST client.");
      LOG_STATUS_NO_RETURN_VALUE(st);
      save_error(ctx, st);
      return TILEDB_ERR;
    }

    auto&& [st, array_schema_rest] =
        rest_client->get_array_schema_from_rest(uri);
    if (!st.ok()) {
      LOG_STATUS_NO_RETURN_VALUE(st);
      save_error(ctx, st);
      delete *array_schema;
      *array_schema = nullptr;
      return TILEDB_ERR;
    }
    (*array_schema)->array_schema_ = array_schema_rest.value();
  } else {
    // Create key
    tiledb::sm::EncryptionKey key;
    if (SAVE_ERROR_CATCH(
            ctx,
            key.set_key(
                static_cast<tiledb::sm::EncryptionType>(encryption_type),
                encryption_key,
                key_length))) {
      delete *array_schema;
      *array_schema = nullptr;
      return TILEDB_ERR;
    }

    // For easy reference
    auto storage_manager{ctx->storage_manager()};

    // Load URIs from the array directory
    tiledb::sm::ArrayDirectory array_dir(storage_manager->resources(), uri);
    try {
      array_dir = tiledb::sm::ArrayDirectory(
          storage_manager->resources(),
          uri,
          0,
          UINT64_MAX,
          tiledb::sm::ArrayDirectoryMode::SCHEMA_ONLY);
    } catch (const std::logic_error& le) {
      auto st = Status_ArrayDirectoryError(le.what());
      LOG_STATUS_NO_RETURN_VALUE(st);
      save_error(ctx, st);
      delete *array_schema;
      return TILEDB_ERR;
    }

    // Load latest array schema
    auto&& array_schema_latest = array_dir.load_array_schema_latest(key);
    (*array_schema)->array_schema_ = array_schema_latest;
  }
  return TILEDB_OK;
}

int32_t tiledb_array_schema_get_array_type(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    tiledb_array_type_t* array_type) {
  if (sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  *array_type = static_cast<tiledb_array_type_t>(
      array_schema->array_schema_->array_type());
  return TILEDB_OK;
}

int32_t tiledb_array_schema_get_capacity(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    uint64_t* capacity) {
  if (sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  *capacity = array_schema->array_schema_->capacity();
  return TILEDB_OK;
}

int32_t tiledb_array_schema_get_cell_order(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    tiledb_layout_t* cell_order) {
  if (sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  *cell_order =
      static_cast<tiledb_layout_t>(array_schema->array_schema_->cell_order());
  return TILEDB_OK;
}

int32_t tiledb_array_schema_get_coords_filter_list(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_filter_list_t** filter_list) {
  if (sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  api::ensure_output_pointer_is_valid(filter_list);
  // Copy-construct a separate FilterPipeline object
  *filter_list = tiledb_filter_list_t::make_handle(
      sm::FilterPipeline{array_schema->array_schema_->coords_filters()});
  return TILEDB_OK;
}

int32_t tiledb_array_schema_get_offsets_filter_list(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_filter_list_t** filter_list) {
  if (sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  api::ensure_output_pointer_is_valid(filter_list);
  // Copy-construct a separate FilterPipeline object
  *filter_list = tiledb_filter_list_t::make_handle(sm::FilterPipeline{
      array_schema->array_schema_->cell_var_offsets_filters()});
  return TILEDB_OK;
}

int32_t tiledb_array_schema_get_validity_filter_list(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_filter_list_t** filter_list) {
  if (sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  api::ensure_output_pointer_is_valid(filter_list);
  // Copy-construct a separate FilterPipeline object
  *filter_list = tiledb_filter_list_t::make_handle(
      sm::FilterPipeline{array_schema->array_schema_->cell_validity_filters()});
  return TILEDB_OK;
}

int32_t tiledb_array_schema_get_domain(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    tiledb_domain_t** domain) {
  if (sanity_check(ctx, array_schema) == TILEDB_ERR) {
    return TILEDB_ERR;
  }
  ensure_output_pointer_is_valid(domain);
  *domain = tiledb_domain_handle_t::make_handle(
      array_schema->array_schema_->shared_domain());
  return TILEDB_OK;
}

int32_t tiledb_array_schema_get_tile_order(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    tiledb_layout_t* tile_order) {
  if (sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  *tile_order =
      static_cast<tiledb_layout_t>(array_schema->array_schema_->tile_order());
  return TILEDB_OK;
}

int32_t tiledb_array_schema_get_attribute_num(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    uint32_t* attribute_num) {
  if (sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  *attribute_num = array_schema->array_schema_->attribute_num();
  return TILEDB_OK;
}

int32_t tiledb_array_schema_dump(
    tiledb_ctx_t* ctx, const tiledb_array_schema_t* array_schema, FILE* out) {
  if (sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  array_schema->array_schema_->dump(out);
  return TILEDB_OK;
}

int32_t tiledb_array_schema_get_attribute_from_index(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    uint32_t index,
    tiledb_attribute_t** attr) {
  if (sanity_check(ctx, array_schema) == TILEDB_ERR) {
    return TILEDB_ERR;
  }
  ensure_output_pointer_is_valid(attr);
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
    auto st = Status_ArraySchemaError(errmsg.str());
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  auto found_attr = array_schema->array_schema_->shared_attribute(index);
  if (!found_attr) {
    throw CAPIStatusException("Attribute not found, but index is valid!");
  }
  *attr = tiledb_attribute_handle_t::make_handle(found_attr);
  return TILEDB_OK;
}

int32_t tiledb_array_schema_get_attribute_from_name(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    const char* name,
    tiledb_attribute_t** attr) {
  if (sanity_check(ctx, array_schema) == TILEDB_ERR) {
    return TILEDB_ERR;
  }
  ensure_output_pointer_is_valid(attr);
  uint32_t attribute_num = array_schema->array_schema_->attribute_num();
  if (attribute_num == 0) {
    *attr = nullptr;
    return TILEDB_OK;
  }
  std::string name_string(name);
  auto found_attr = array_schema->array_schema_->shared_attribute(name_string);
  if (!found_attr) {
    throw CAPIException(
        std::string("Attribute name: ") +
        (name_string.empty() ? "<anonymous>" : name) +
        " does not exist for array " +
        array_schema->array_schema_->array_uri().to_string());
  }
  *attr = tiledb_attribute_handle_t::make_handle(found_attr);
  return TILEDB_OK;
}

int32_t tiledb_array_schema_has_attribute(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    const char* name,
    int32_t* has_attr) {
  if (sanity_check(ctx, array_schema) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  bool b;
  throw_if_not_ok(array_schema->array_schema_->has_attribute(name, &b));

  *has_attr = b ? 1 : 0;

  return TILEDB_OK;
}

/* ********************************* */
/*            SCHEMA EVOLUTION       */
/* ********************************* */

int32_t tiledb_array_schema_evolution_alloc(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t** array_schema_evolution) {
  // Sanity check

  // Create schema evolution struct
  *array_schema_evolution = new (std::nothrow) tiledb_array_schema_evolution_t;
  if (*array_schema_evolution == nullptr) {
    auto st =
        Status_Error("Failed to allocate TileDB array schema evolution object");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Create a new SchemaEvolution object
  (*array_schema_evolution)->array_schema_evolution_ =
      new (std::nothrow) tiledb::sm::ArraySchemaEvolution();
  if ((*array_schema_evolution)->array_schema_evolution_ == nullptr) {
    delete *array_schema_evolution;
    *array_schema_evolution = nullptr;
    auto st =
        Status_Error("Failed to allocate TileDB array schema evolution object");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Success
  return TILEDB_OK;
}

void tiledb_array_schema_evolution_free(
    tiledb_array_schema_evolution_t** array_schema_evolution) {
  if (array_schema_evolution != nullptr && *array_schema_evolution != nullptr) {
    delete (*array_schema_evolution)->array_schema_evolution_;
    delete *array_schema_evolution;
    *array_schema_evolution = nullptr;
  }
}

int32_t tiledb_array_schema_evolution_add_attribute(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t* array_schema_evolution,
    tiledb_attribute_t* attr) {
  if (sanity_check(ctx, array_schema_evolution) == TILEDB_ERR) {
    return TILEDB_ERR;
  }
  ensure_attribute_is_valid(attr);
  array_schema_evolution->array_schema_evolution_->add_attribute(
      attr->copy_attribute());

  return TILEDB_OK;
}

int32_t tiledb_array_schema_evolution_drop_attribute(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t* array_schema_evolution,
    const char* attribute_name) {
  if (sanity_check(ctx, array_schema_evolution) == TILEDB_ERR)
    return TILEDB_ERR;

  array_schema_evolution->array_schema_evolution_->drop_attribute(
      attribute_name);
  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_evolution_add_enumeration(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t* array_schema_evolution,
    tiledb_enumeration_t* enumeration) {
  if (sanity_check(ctx, array_schema_evolution) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  api::ensure_enumeration_is_valid(enumeration);

  auto enmr = enumeration->copy();
  array_schema_evolution->array_schema_evolution_->add_enumeration(enmr);

  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_evolution_extend_enumeration(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t* array_schema_evolution,
    tiledb_enumeration_t* enumeration) {
  if (sanity_check(ctx, array_schema_evolution) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  api::ensure_enumeration_is_valid(enumeration);

  auto enmr = enumeration->copy();
  array_schema_evolution->array_schema_evolution_->extend_enumeration(enmr);

  return TILEDB_OK;
}

capi_return_t tiledb_array_schema_evolution_drop_enumeration(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t* array_schema_evolution,
    const char* enmr_name) {
  if (sanity_check(ctx, array_schema_evolution) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  if (enmr_name == nullptr) {
    return TILEDB_ERR;
  }

  array_schema_evolution->array_schema_evolution_->drop_enumeration(enmr_name);

  return TILEDB_OK;
}

int32_t tiledb_array_schema_evolution_set_timestamp_range(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t* array_schema_evolution,
    uint64_t lo,
    uint64_t hi) {
  if (sanity_check(ctx, array_schema_evolution) == TILEDB_ERR)
    return TILEDB_ERR;

  array_schema_evolution->array_schema_evolution_->set_timestamp_range(
      {lo, hi});
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
  if (sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  // Error if array is not open
  if (!array->array_->is_open()) {
    auto st = Status_Error("Cannot create query; Input array is not open");
    *query = nullptr;
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  // Error if the query type and array query type do not match
  tiledb::sm::QueryType array_query_type;
  try {
    array_query_type = array->array_->get_query_type();
  } catch (StatusException& e) {
    return TILEDB_ERR;
  }

  if (query_type != static_cast<tiledb_query_type_t>(array_query_type)) {
    std::stringstream errmsg;
    errmsg << "Cannot create query; "
           << "Array query type does not match declared query type: "
           << "(" << query_type_str(array_query_type) << " != "
           << tiledb::sm::query_type_str(
                  static_cast<tiledb::sm::QueryType>(query_type))
           << ")";
    *query = nullptr;
    auto st = Status_Error(errmsg.str());
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  // Create query struct
  *query = new (std::nothrow) tiledb_query_t;
  if (*query == nullptr) {
    auto st = Status_Error(
        "Failed to allocate TileDB query object; Memory allocation failed");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Create query
  (*query)->query_ = new (std::nothrow)
      tiledb::sm::Query(ctx->storage_manager(), array->array_);
  if ((*query)->query_ == nullptr) {
    auto st = Status_Error(
        "Failed to allocate TileDB query object; Memory allocation failed");
    delete *query;
    *query = nullptr;
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Success
  return TILEDB_OK;
}

int32_t tiledb_query_get_stats(
    tiledb_ctx_t* ctx, tiledb_query_t* query, char** stats_json) {
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  if (stats_json == nullptr)
    return TILEDB_ERR;

  const std::string str = query->query_->stats()->dump(2, 0);

  *stats_json = static_cast<char*>(std::malloc(str.size() + 1));
  if (*stats_json == nullptr)
    return TILEDB_ERR;

  std::memcpy(*stats_json, str.data(), str.size());
  (*stats_json)[str.size()] = '\0';

  return TILEDB_OK;
}

int32_t tiledb_query_set_config(
    tiledb_ctx_t* ctx, tiledb_query_t* query, tiledb_config_t* config) {
  // Sanity check
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;
  api::ensure_config_is_valid(config);
  query->query_->set_config(config->config());
  return TILEDB_OK;
}

int32_t tiledb_query_get_config(
    tiledb_ctx_t* ctx, tiledb_query_t* query, tiledb_config_t** config) {
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;
  api::ensure_output_pointer_is_valid(config);
  *config = tiledb_config_handle_t::make_handle(query->query_->config());
  return TILEDB_OK;
}

int32_t tiledb_query_set_subarray(
    tiledb_ctx_t* ctx, tiledb_query_t* query, const void* subarray_vals) {
  // Sanity check
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  // Set subarray
  query->query_->set_subarray(subarray_vals);

  return TILEDB_OK;
}

int32_t tiledb_query_set_subarray_t(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const tiledb_subarray_t* subarray) {
  // Sanity check
  if (sanity_check(ctx, query) == TILEDB_ERR ||
      sanity_check(ctx, subarray) == TILEDB_ERR)
    return TILEDB_ERR;

  query->query_->set_subarray(*subarray->subarray_);

  return TILEDB_OK;
}

int32_t tiledb_query_set_data_buffer(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* name,
    void* buffer,
    uint64_t* buffer_size) {  // Sanity check
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  // Set attribute buffer
  throw_if_not_ok(query->query_->set_data_buffer(name, buffer, buffer_size));

  return TILEDB_OK;
}

int32_t tiledb_query_set_offsets_buffer(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* name,
    uint64_t* buffer_offsets,
    uint64_t* buffer_offsets_size) {  // Sanity check
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  // Set attribute buffer
  throw_if_not_ok(query->query_->set_offsets_buffer(
      name, buffer_offsets, buffer_offsets_size));

  return TILEDB_OK;
}

int32_t tiledb_query_set_validity_buffer(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* name,
    uint8_t* buffer_validity,
    uint64_t* buffer_validity_size) {  // Sanity check
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  // Set attribute buffer
  throw_if_not_ok(query->query_->set_validity_buffer(
      name, buffer_validity, buffer_validity_size));

  return TILEDB_OK;
}

int32_t tiledb_query_get_data_buffer(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* name,
    void** buffer,
    uint64_t** buffer_size) {
  // Sanity check
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  // Get attribute buffer
  throw_if_not_ok(query->query_->get_data_buffer(name, buffer, buffer_size));

  return TILEDB_OK;
}

int32_t tiledb_query_get_offsets_buffer(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* name,
    uint64_t** buffer,
    uint64_t** buffer_size) {
  // Sanity check
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  // Get attribute buffer
  throw_if_not_ok(query->query_->get_offsets_buffer(name, buffer, buffer_size));

  return TILEDB_OK;
}

int32_t tiledb_query_get_validity_buffer(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* name,
    uint8_t** buffer,
    uint64_t** buffer_size) {
  // Sanity check
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  // Get attribute buffer
  throw_if_not_ok(
      query->query_->get_validity_buffer(name, buffer, buffer_size));

  return TILEDB_OK;
}

int32_t tiledb_query_set_layout(
    tiledb_ctx_t* ctx, tiledb_query_t* query, tiledb_layout_t layout) {
  // Sanity check
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  // Set layout
  throw_if_not_ok(
      query->query_->set_layout(static_cast<tiledb::sm::Layout>(layout)));

  return TILEDB_OK;
}

int32_t tiledb_query_set_condition(
    tiledb_ctx_t* const ctx,
    tiledb_query_t* const query,
    const tiledb_query_condition_t* const cond) {
  // Sanity check
  if (sanity_check(ctx, query) == TILEDB_ERR ||
      sanity_check(ctx, cond) == TILEDB_ERR)
    return TILEDB_ERR;

  // Set layout
  throw_if_not_ok(query->query_->set_condition(*cond->query_condition_));

  return TILEDB_OK;
}

int32_t tiledb_query_finalize(tiledb_ctx_t* ctx, tiledb_query_t* query) {
  // Trivial case
  if (query == nullptr)
    return TILEDB_OK;

  // Sanity check
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  // Flush query
  throw_if_not_ok(query->query_->finalize());

  return TILEDB_OK;
}

int32_t tiledb_query_submit_and_finalize(
    tiledb_ctx_t* ctx, tiledb_query_t* query) {
  // Trivial case
  if (query == nullptr)
    return TILEDB_OK;

  // Sanity check
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(query->query_->submit_and_finalize());

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
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(query->query_->submit());

  return TILEDB_OK;
}

int32_t tiledb_query_submit_async(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    void (*callback)(void*),
    void* callback_data) {
  // Sanity checks
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;
  throw_if_not_ok(query->query_->submit_async(callback, callback_data));

  return TILEDB_OK;
}

int32_t tiledb_query_has_results(
    tiledb_ctx_t* ctx, tiledb_query_t* query, int32_t* has_results) {
  // Sanity check
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  *has_results = query->query_->has_results();

  return TILEDB_OK;
}

int32_t tiledb_query_get_status(
    tiledb_ctx_t* ctx, tiledb_query_t* query, tiledb_query_status_t* status) {
  // Sanity check
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  *status = (tiledb_query_status_t)query->query_->status();

  return TILEDB_OK;
}

int32_t tiledb_query_get_type(
    tiledb_ctx_t* ctx, tiledb_query_t* query, tiledb_query_type_t* query_type) {
  // Sanity check
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  *query_type = static_cast<tiledb_query_type_t>(query->query_->type());

  return TILEDB_OK;
}

int32_t tiledb_query_get_layout(
    tiledb_ctx_t* ctx, tiledb_query_t* query, tiledb_layout_t* query_layout) {
  // Sanity check
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  *query_layout = static_cast<tiledb_layout_t>(query->query_->layout());

  return TILEDB_OK;
}

int32_t tiledb_query_get_array(
    tiledb_ctx_t* ctx, tiledb_query_t* query, tiledb_array_t** array) {
  // Sanity check
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create array datatype
  *array = new (std::nothrow) tiledb_array_t;
  if (*array == nullptr) {
    auto st = Status_Error(
        "Failed to create TileDB array object; Memory allocation error");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Allocate an array object, taken from the query's array.
  (*array)->array_ = query->query_->array_shared();

  return TILEDB_OK;
}

int32_t tiledb_query_add_range(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    uint32_t dim_idx,
    const void* start,
    const void* end,
    const void* stride) {
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  tiledb_subarray_transient_local_t query_subarray(query);
  return tiledb_subarray_add_range(
      ctx, &query_subarray, dim_idx, start, end, stride);
}

int32_t tiledb_query_add_point_ranges(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    uint32_t dim_idx,
    const void* start,
    uint64_t count) {
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  /*
   * WARNING: C API implementation function calling C API function. Error
   * handling may not be as expected.
   *
   * An earlier version of this function was casting away `const`, which may not
   * have had the effect intended. This function deserves an audit.
   */
  tiledb_subarray_transient_local_t query_subarray(query);
  auto local_cfg{tiledb_config_handle_t::make_handle(query->query_->config())};
  tiledb_subarray_set_config(ctx, &query_subarray, local_cfg);
  tiledb_config_handle_t::break_handle(local_cfg);
  return tiledb_subarray_add_point_ranges(
      ctx, &query_subarray, dim_idx, start, count);
}

int32_t tiledb_query_add_range_by_name(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* dim_name,
    const void* start,
    const void* end,
    const void* stride) {
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  tiledb_subarray_transient_local_t query_subarray(query);
  return tiledb_subarray_add_range_by_name(
      ctx, &query_subarray, dim_name, start, end, stride);
}

int32_t tiledb_query_add_range_var(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    uint32_t dim_idx,
    const void* start,
    uint64_t start_size,
    const void* end,
    uint64_t end_size) {
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  tiledb_subarray_transient_local_t query_subarray(query);
  return tiledb_subarray_add_range_var(
      ctx, &query_subarray, dim_idx, start, start_size, end, end_size);
}

int32_t tiledb_query_add_range_var_by_name(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* dim_name,
    const void* start,
    uint64_t start_size,
    const void* end,
    uint64_t end_size) {
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  tiledb_subarray_transient_local_t query_subarray(query);
  return tiledb_subarray_add_range_var_by_name(
      ctx, &query_subarray, dim_name, start, start_size, end, end_size);
}

int32_t tiledb_query_get_range_num(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    uint32_t dim_idx,
    uint64_t* range_num) {
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  tiledb_subarray_transient_local_t query_subarray(query);
  return tiledb_subarray_get_range_num(
      ctx, &query_subarray, dim_idx, range_num);
}

int32_t tiledb_query_get_range_num_from_name(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    const char* dim_name,
    uint64_t* range_num) {
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  tiledb_subarray_transient_local_t query_subarray(query);
  return tiledb_subarray_get_range_num_from_name(
      ctx, &query_subarray, dim_name, range_num);
}

int32_t tiledb_query_get_range(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    uint32_t dim_idx,
    uint64_t range_idx,
    const void** start,
    const void** end,
    const void** stride) {
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  tiledb_subarray_transient_local_t query_subarray(query);
  return tiledb_subarray_get_range(
      ctx, &query_subarray, dim_idx, range_idx, start, end, stride);
}

int32_t tiledb_query_get_range_from_name(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    const char* dim_name,
    uint64_t range_idx,
    const void** start,
    const void** end,
    const void** stride) {
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  tiledb_subarray_transient_local_t query_subarray(query);
  return tiledb_subarray_get_range_from_name(
      ctx, &query_subarray, dim_name, range_idx, start, end, stride);
}

int32_t tiledb_query_get_range_var_size(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    uint32_t dim_idx,
    uint64_t range_idx,
    uint64_t* start_size,
    uint64_t* end_size) {
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  tiledb_subarray_transient_local_t which_subarray(query);
  return tiledb_subarray_get_range_var_size(
      ctx, &which_subarray, dim_idx, range_idx, start_size, end_size);
}

int32_t tiledb_query_get_range_var_size_from_name(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    const char* dim_name,
    uint64_t range_idx,
    uint64_t* start_size,
    uint64_t* end_size) {
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  tiledb_subarray_transient_local_t query_subarray(query);
  return tiledb_subarray_get_range_var_size_from_name(
      ctx, &query_subarray, dim_name, range_idx, start_size, end_size);
}

int32_t tiledb_query_get_range_var(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    uint32_t dim_idx,
    uint64_t range_idx,
    void* start,
    void* end) {
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  tiledb_subarray_transient_local_t query_subarray(query);
  return tiledb_subarray_get_range_var(
      ctx, &query_subarray, dim_idx, range_idx, start, end);
}

int32_t tiledb_query_get_range_var_from_name(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    const char* dim_name,
    uint64_t range_idx,
    void* start,
    void* end) {
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  tiledb_subarray_transient_local_t query_subarray(query);
  return tiledb_subarray_get_range_var_from_name(
      ctx, &query_subarray, dim_name, range_idx, start, end);
}

int32_t tiledb_query_get_est_result_size(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    const char* name,
    uint64_t* size) {
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(query->query_->get_est_result_size(name, size));

  return TILEDB_OK;
}

int32_t tiledb_query_get_est_result_size_var(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    const char* name,
    uint64_t* size_off,
    uint64_t* size_val) {
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(query->query_->get_est_result_size(name, size_off, size_val));

  return TILEDB_OK;
}

int32_t tiledb_query_get_est_result_size_nullable(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    const char* name,
    uint64_t* size_val,
    uint64_t* size_validity) {
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(query->query_->get_est_result_size_nullable(
      name, size_val, size_validity));

  return TILEDB_OK;
}

int32_t tiledb_query_get_est_result_size_var_nullable(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    const char* name,
    uint64_t* size_off,
    uint64_t* size_val,
    uint64_t* size_validity) {
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(query->query_->get_est_result_size_nullable(
      name, size_off, size_val, size_validity));

  return TILEDB_OK;
}

int32_t tiledb_query_get_fragment_num(
    tiledb_ctx_t* ctx, const tiledb_query_t* query, uint32_t* num) {
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(query->query_->get_written_fragment_num(num));

  return TILEDB_OK;
}

int32_t tiledb_query_get_fragment_uri(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    uint64_t idx,
    const char** uri) {
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(query->query_->get_written_fragment_uri(idx, uri));

  return TILEDB_OK;
}

int32_t tiledb_query_get_fragment_timestamp_range(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    uint64_t idx,
    uint64_t* t1,
    uint64_t* t2) {
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(
      query->query_->get_written_fragment_timestamp_range(idx, t1, t2));

  return TILEDB_OK;
}

int32_t tiledb_query_get_subarray_t(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    tiledb_subarray_t** subarray) {
  *subarray = nullptr;
  // Sanity check
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  *subarray = new (std::nothrow) tiledb_subarray_t;
  if (*subarray == nullptr) {
    auto st = Status_Error("Failed to allocate TileDB subarray object");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  (*subarray)->subarray_ =
      const_cast<tiledb::sm::Subarray*>(query->query_->subarray());

  return TILEDB_OK;
}

int32_t tiledb_query_get_relevant_fragment_num(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    uint64_t* relevant_fragment_num) {
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  *relevant_fragment_num =
      query->query_->subarray()->relevant_fragments().relevant_fragments_size();

  return TILEDB_OK;
}

int32_t tiledb_query_add_update_value(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* field_name,
    const void* update_value,
    uint64_t update_value_size) {
  // Sanity check
  if (sanity_check(ctx, query) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  // Add update value.
  if (SAVE_ERROR_CATCH(
          ctx,
          query->query_->add_update_value(
              field_name, update_value, update_value_size))) {
    return TILEDB_ERR;
  }

  // Success
  return TILEDB_OK;
}

/* ****************************** */
/*         SUBARRAY               */
/* ****************************** */

capi_return_t tiledb_subarray_alloc(
    tiledb_ctx_t* ctx,
    const tiledb_array_t* array,
    tiledb_subarray_t** subarray) {
  api::ensure_array_is_valid(array);
  api::ensure_output_pointer_is_valid(subarray);

  // Error if array is not open
  if (!array->array_->is_open()) {
    throw api::CAPIStatusException("Cannot create subarray; array is not open");
  }

  // Create a new subarray object
  *subarray = new tiledb_subarray_t;
  try {
    (*subarray)->subarray_ = new tiledb::sm::Subarray(
        array->array_.get(),
        (tiledb::sm::stats::Stats*)nullptr,
        ctx->storage_manager()->logger(),
        true,
        ctx->storage_manager());
    (*subarray)->is_allocated_ = true;
  } catch (...) {
    delete *subarray;
    *subarray = nullptr;
    throw api::CAPIStatusException("Failed to create subarray");
  }

  return TILEDB_OK;
}

int32_t tiledb_subarray_set_config(
    tiledb_ctx_t* ctx, tiledb_subarray_t* subarray, tiledb_config_t* config) {
  if (sanity_check(ctx, subarray) == TILEDB_ERR)
    return TILEDB_ERR;
  api::ensure_config_is_valid(config);
  subarray->subarray_->set_config(config->config());
  return TILEDB_OK;
}

void tiledb_subarray_free(tiledb_subarray_t** subarray) {
  if (subarray != nullptr && *subarray != nullptr) {
    if ((*subarray)->is_allocated_) {
      delete (*subarray)->subarray_;
    } else {
      (*subarray)->subarray_ = nullptr;
    }
    delete (*subarray);
    *subarray = nullptr;
  }
}

int32_t tiledb_subarray_set_coalesce_ranges(
    tiledb_ctx_t* ctx, tiledb_subarray_t* subarray, int coalesce_ranges) {
  // Sanity check
  if (sanity_check(ctx, subarray) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(
      subarray->subarray_->set_coalesce_ranges(coalesce_ranges != 0));

  return TILEDB_OK;
}

int32_t tiledb_subarray_set_subarray(
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray_obj,
    const void* subarray_vals) {
  if (sanity_check(ctx, subarray_obj) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(subarray_obj->subarray_->set_subarray(subarray_vals));

  return TILEDB_OK;
}

int32_t tiledb_subarray_add_range(
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray,
    uint32_t dim_idx,
    const void* start,
    const void* end,
    const void* stride) {
  if (sanity_check(ctx, subarray) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(subarray->subarray_->add_range(dim_idx, start, end, stride));

  return TILEDB_OK;
}

int32_t tiledb_subarray_add_point_ranges(
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray,
    uint32_t dim_idx,
    const void* start,
    uint64_t count) {
  if (sanity_check(ctx, subarray) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(subarray->subarray_->add_point_ranges(dim_idx, start, count));

  return TILEDB_OK;
}

int32_t tiledb_subarray_add_range_by_name(
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray,
    const char* dim_name,
    const void* start,
    const void* end,
    const void* stride) {
  if (sanity_check(ctx, subarray) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(
      subarray->subarray_->add_range_by_name(dim_name, start, end, stride));

  return TILEDB_OK;
}

int32_t tiledb_subarray_add_range_var(
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray,
    uint32_t dim_idx,
    const void* start,
    uint64_t start_size,
    const void* end,
    uint64_t end_size) {
  if (sanity_check(ctx, subarray) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(subarray->subarray_->add_range_var(
      dim_idx, start, start_size, end, end_size));

  return TILEDB_OK;
}

int32_t tiledb_subarray_add_range_var_by_name(
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray,
    const char* dim_name,
    const void* start,
    uint64_t start_size,
    const void* end,
    uint64_t end_size) {
  if (sanity_check(ctx, subarray) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(subarray->subarray_->add_range_var_by_name(
      dim_name, start, start_size, end, end_size));

  return TILEDB_OK;
}

int32_t tiledb_subarray_get_range_num(
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    uint32_t dim_idx,
    uint64_t* range_num) {
  if (sanity_check(ctx, subarray) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(subarray->subarray_->get_range_num(dim_idx, range_num));

  return TILEDB_OK;
}

int32_t tiledb_subarray_get_range_num_from_name(
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    const char* dim_name,
    uint64_t* range_num) {
  if (sanity_check(ctx, subarray) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(
      subarray->subarray_->get_range_num_from_name(dim_name, range_num));

  return TILEDB_OK;
}

int32_t tiledb_subarray_get_range(
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    uint32_t dim_idx,
    uint64_t range_idx,
    const void** start,
    const void** end,
    const void** stride) {
  if (sanity_check(ctx, subarray) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(
      subarray->subarray_->get_range(dim_idx, range_idx, start, end, stride));

  return TILEDB_OK;
}

int32_t tiledb_subarray_get_range_var_size(
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    uint32_t dim_idx,
    uint64_t range_idx,
    uint64_t* start_size,
    uint64_t* end_size) {
  if (sanity_check(ctx, subarray) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(subarray->subarray_->get_range_var_size(
      dim_idx, range_idx, start_size, end_size));

  return TILEDB_OK;
}

int32_t tiledb_subarray_get_range_from_name(
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    const char* dim_name,
    uint64_t range_idx,
    const void** start,
    const void** end,
    const void** stride) {
  if (sanity_check(ctx, subarray) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(subarray->subarray_->get_range_from_name(
      dim_name, range_idx, start, end, stride));

  return TILEDB_OK;
}

int32_t tiledb_subarray_get_range_var_size_from_name(
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    const char* dim_name,
    uint64_t range_idx,
    uint64_t* start_size,
    uint64_t* end_size) {
  if (sanity_check(ctx, subarray) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(subarray->subarray_->get_range_var_size_from_name(
      dim_name, range_idx, start_size, end_size));

  return TILEDB_OK;
}

int32_t tiledb_subarray_get_range_var(
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    uint32_t dim_idx,
    uint64_t range_idx,
    void* start,
    void* end) {
  if (sanity_check(ctx, subarray) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(
      subarray->subarray_->get_range_var(dim_idx, range_idx, start, end));

  return TILEDB_OK;
}

int32_t tiledb_subarray_get_range_var_from_name(
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    const char* dim_name,
    uint64_t range_idx,
    void* start,
    void* end) {
  if (sanity_check(ctx, subarray) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(subarray->subarray_->get_range_var_from_name(
      dim_name, range_idx, start, end));

  return TILEDB_OK;
}

/* ****************************** */
/*          QUERY CONDITION       */
/* ****************************** */

int32_t tiledb_query_condition_alloc(
    tiledb_ctx_t* const ctx, tiledb_query_condition_t** const cond) {
  // Create query condition struct
  *cond = new (std::nothrow) tiledb_query_condition_t;
  if (*cond == nullptr) {
    auto st = Status_Error(
        "Failed to create TileDB query condition object; Memory allocation "
        "error");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Create QueryCondition object
  (*cond)->query_condition_ = new (std::nothrow) tiledb::sm::QueryCondition();
  if ((*cond)->query_condition_ == nullptr) {
    auto st = Status_Error("Failed to allocate TileDB query condition object");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    delete *cond;
    *cond = nullptr;
    return TILEDB_OOM;
  }

  // Success
  return TILEDB_OK;
}

capi_return_t tiledb_query_condition_alloc_set_membership(
    const char* field_name,
    const void* data,
    uint64_t data_size,
    const void* offsets,
    uint64_t offsets_size,
    tiledb_query_condition_op_t op,
    tiledb_query_condition_t** cond) {
  // Validate input arguments. The data, data_size, offsets, and offsets_size,
  // and op arguments are validated in the QueryCondition constructor so
  // there's no need to validate them here.
  if (field_name == nullptr) {
    throw api::CAPIStatusException(
        "QueryCondition field name must not be nullptr");
  }
  ensure_output_pointer_is_valid(cond);

  // Create query condition struct
  *cond = new tiledb_query_condition_t;
  if (*cond == nullptr) {
    throw api::CAPIStatusException(
        "Failed to create TileDB query condition "
        "object; Memory allocation error");
  }

  // Create QueryCondition object
  (*cond)->query_condition_ = new tiledb::sm::QueryCondition(
      field_name,
      data,
      data_size,
      offsets,
      offsets_size,
      static_cast<tiledb::sm::QueryConditionOp>(op));
  if ((*cond)->query_condition_ == nullptr) {
    delete *cond;
    throw api::CAPIStatusException(
        "Failed to allocate TileDB query condition object");
  }

  return TILEDB_OK;
}

void tiledb_query_condition_free(tiledb_query_condition_t** cond) {
  if (cond != nullptr && *cond != nullptr) {
    delete (*cond)->query_condition_;
    delete *cond;
    *cond = nullptr;
  }
}

int32_t tiledb_query_condition_init(
    tiledb_ctx_t* const ctx,
    tiledb_query_condition_t* const cond,
    const char* const attribute_name,
    const void* const condition_value,
    const uint64_t condition_value_size,
    const tiledb_query_condition_op_t op) {
  if (sanity_check(ctx, cond) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  // Initialize the QueryCondition object
  auto st = cond->query_condition_->init(
      std::string(attribute_name),
      condition_value,
      condition_value_size,
      static_cast<tiledb::sm::QueryConditionOp>(op));
  if (!st.ok()) {
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  // Success
  return TILEDB_OK;
}

int32_t tiledb_query_condition_combine(
    tiledb_ctx_t* const ctx,
    const tiledb_query_condition_t* const left_cond,
    const tiledb_query_condition_t* const right_cond,
    const tiledb_query_condition_combination_op_t combination_op,
    tiledb_query_condition_t** const combined_cond) {
  // Sanity check
  if (sanity_check(ctx, left_cond) == TILEDB_ERR ||
      (combination_op != TILEDB_NOT &&
       sanity_check(ctx, right_cond) == TILEDB_ERR) ||
      (combination_op == TILEDB_NOT && right_cond != nullptr))
    return TILEDB_ERR;

  // Create the combined query condition struct
  *combined_cond = new (std::nothrow) tiledb_query_condition_t;
  if (*combined_cond == nullptr) {
    auto st = Status_Error(
        "Failed to create TileDB query condition object; Memory allocation "
        "error");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Create the combined QueryCondition object
  (*combined_cond)->query_condition_ =
      new (std::nothrow) tiledb::sm::QueryCondition();
  if ((*combined_cond)->query_condition_ == nullptr) {
    auto st = Status_Error("Failed to allocate TileDB query condition object");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    delete *combined_cond;
    *combined_cond = nullptr;
    return TILEDB_OOM;
  }

  if (combination_op == TILEDB_NOT) {
    if (SAVE_ERROR_CATCH(
            ctx,
            left_cond->query_condition_->negate(
                static_cast<tiledb::sm::QueryConditionCombinationOp>(
                    combination_op),
                (*combined_cond)->query_condition_))) {
      delete (*combined_cond)->query_condition_;
      delete *combined_cond;
      return TILEDB_ERR;
    }
  } else {
    if (SAVE_ERROR_CATCH(
            ctx,
            left_cond->query_condition_->combine(
                *right_cond->query_condition_,
                static_cast<tiledb::sm::QueryConditionCombinationOp>(
                    combination_op),
                (*combined_cond)->query_condition_))) {
      delete (*combined_cond)->query_condition_;
      delete *combined_cond;
      return TILEDB_ERR;
    }
  }

  return TILEDB_OK;
}

int32_t tiledb_query_condition_negate(
    tiledb_ctx_t* const ctx,
    const tiledb_query_condition_t* const cond,
    tiledb_query_condition_t** const negated_cond) {
  return api::tiledb_query_condition_combine(
      ctx, cond, nullptr, TILEDB_NOT, negated_cond);
}

capi_return_t tiledb_query_condition_set_use_enumeration(
    tiledb_ctx_t* ctx,
    const tiledb_query_condition_t* cond,
    int use_enumeration) {
  if (sanity_check(ctx, cond) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  cond->query_condition_->set_use_enumeration(
      use_enumeration != 0 ? true : false);
  return TILEDB_OK;
}

/* ****************************** */
/*              ARRAY             */
/* ****************************** */

int32_t tiledb_array_alloc(
    tiledb_ctx_t* ctx, const char* array_uri, tiledb_array_t** array) {
  // Create array struct
  *array = new (std::nothrow) tiledb_array_t;
  if (*array == nullptr) {
    auto st = Status_Error(
        "Failed to create TileDB array object; Memory allocation error");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Check array URI
  auto uri = tiledb::sm::URI(array_uri);
  if (uri.is_invalid()) {
    auto st = Status_Error("Failed to create TileDB array object; Invalid URI");
    delete *array;
    *array = nullptr;
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  // Allocate an array object
  try {
    (*array)->array_ =
        make_shared<tiledb::sm::Array>(HERE(), uri, ctx->storage_manager());
  } catch (std::bad_alloc&) {
    auto st = Status_Error(
        "Failed to create TileDB array object; Memory allocation error");
    delete *array;
    *array = nullptr;
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Success
  return TILEDB_OK;
}

int32_t tiledb_array_set_open_timestamp_start(
    tiledb_ctx_t* ctx, tiledb_array_t* array, uint64_t timestamp_start) {
  if (sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  array->array_->set_timestamp_start(timestamp_start);

  return TILEDB_OK;
}

int32_t tiledb_array_set_open_timestamp_end(
    tiledb_ctx_t* ctx, tiledb_array_t* array, uint64_t timestamp_end) {
  if (sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  array->array_->set_timestamp_end(timestamp_end);

  return TILEDB_OK;
}

int32_t tiledb_array_get_open_timestamp_start(
    tiledb_ctx_t* ctx, tiledb_array_t* array, uint64_t* timestamp_start) {
  if (sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  *timestamp_start = array->array_->timestamp_start();

  return TILEDB_OK;
}

int32_t tiledb_array_get_open_timestamp_end(
    tiledb_ctx_t* ctx, tiledb_array_t* array, uint64_t* timestamp_end) {
  if (sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  *timestamp_end = array->array_->timestamp_end_opened_at();

  return TILEDB_OK;
}

int32_t tiledb_array_delete(tiledb_ctx_t* ctx, const char* uri) {
  // Allocate an array object
  tiledb_array_t* array = new (std::nothrow) tiledb_array_t;
  try {
    array->array_ = make_shared<tiledb::sm::Array>(
        HERE(), tiledb::sm::URI(uri), ctx->storage_manager());
  } catch (std::bad_alloc&) {
    auto st = Status_Error(
        "Failed to create TileDB array object; Memory allocation error");
    delete array;
    array = nullptr;
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Open the array for exclusive modification
  throw_if_not_ok(array->array_->open(
      tiledb::sm::QueryType::MODIFY_EXCLUSIVE,
      tiledb::sm::EncryptionType::NO_ENCRYPTION,
      nullptr,
      0));

  try {
    array->array_->delete_array(tiledb::sm::URI(uri));
  } catch (std::exception& e) {
    auto st = Status_ArrayError(e.what());
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  return TILEDB_OK;
}

int32_t tiledb_array_delete_array(
    tiledb_ctx_t* ctx, tiledb_array_t* array, const char* uri) {
  if (sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  try {
    array->array_->delete_array(tiledb::sm::URI(uri));
  } catch (std::exception& e) {
    auto st = Status_ArrayError(e.what());
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  return TILEDB_OK;
}

int32_t tiledb_array_delete_fragments(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const char* uri,
    uint64_t timestamp_start,
    uint64_t timestamp_end) {
  if (sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  LOG_WARN(
      "tiledb_array_delete_fragments is deprecated. Please use "
      "tiledb_array_delete_fragments_v2 instead.");

  try {
    array->array_->delete_fragments(
        tiledb::sm::URI(uri), timestamp_start, timestamp_end);
  } catch (std::exception& e) {
    auto st = Status_ArrayError(e.what());
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  return TILEDB_OK;
}

capi_return_t tiledb_array_delete_fragments_v2(
    tiledb_ctx_t* ctx,
    const char* uri_str,
    uint64_t timestamp_start,
    uint64_t timestamp_end) {
  auto uri = tiledb::sm::URI(uri_str);
  if (uri.is_invalid()) {
    throw api::CAPIStatusException(
        "Failed to delete fragments; Invalid input uri");
  }

  // Allocate an array object
  tiledb_array_t* array = new (std::nothrow) tiledb_array_t;
  try {
    array->array_ =
        make_shared<tiledb::sm::Array>(HERE(), uri, ctx->storage_manager());
  } catch (...) {
    delete array;
    array = nullptr;
    throw api::CAPIStatusException("Failed to create array");
  }

  // Set array open timestamps
  array->array_->set_timestamp_start(timestamp_start);
  array->array_->set_timestamp_end(timestamp_end);

  // Open the array for exclusive modification
  throw_if_not_ok(array->array_->open(
      static_cast<tiledb::sm::QueryType>(TILEDB_MODIFY_EXCLUSIVE),
      static_cast<tiledb::sm::EncryptionType>(TILEDB_NO_ENCRYPTION),
      nullptr,
      0));

  // Delete fragments
  try {
    array->array_->delete_fragments(uri, timestamp_start, timestamp_end);
  } catch (...) {
    throw_if_not_ok(array->array_->close());
    delete array;
    array = nullptr;
    throw api::CAPIStatusException("Failed to delete fragments");
  }

  // Close and delete the array
  throw_if_not_ok(array->array_->close());
  delete array;
  array = nullptr;

  return TILEDB_OK;
}

capi_return_t tiledb_array_delete_fragments_list(
    tiledb_ctx_t* ctx,
    const char* uri_str,
    const char* fragment_uris[],
    const size_t num_fragments) {
  auto uri = tiledb::sm::URI(uri_str);
  if (uri.is_invalid()) {
    throw api::CAPIStatusException(
        "Failed to delete_fragments_list; Invalid input uri");
  }

  if (num_fragments < 1) {
    throw api::CAPIStatusException(
        "Failed to delete_fragments_list; Invalid input number of fragments");
  }

  for (size_t i = 0; i < num_fragments; i++) {
    if (tiledb::sm::URI(fragment_uris[i]).is_invalid()) {
      throw api::CAPIStatusException(
          "Failed to delete_fragments_list; Invalid input fragment uri");
    }
  }

  // Convert the list of fragment uris to a vector
  std::vector<tiledb::sm::URI> uris;
  uris.reserve(num_fragments);
  for (size_t i = 0; i < num_fragments; i++) {
    uris.emplace_back(tiledb::sm::URI(fragment_uris[i]));
  }

  // Allocate an array object
  tiledb_array_t* array = new (std::nothrow) tiledb_array_t;
  try {
    array->array_ =
        make_shared<tiledb::sm::Array>(HERE(), uri, ctx->storage_manager());
  } catch (...) {
    delete array;
    array = nullptr;
    throw api::CAPIStatusException("Failed to create array");
  }

  // Open the array for exclusive modification
  throw_if_not_ok(array->array_->open(
      static_cast<tiledb::sm::QueryType>(TILEDB_MODIFY_EXCLUSIVE),
      static_cast<tiledb::sm::EncryptionType>(TILEDB_NO_ENCRYPTION),
      nullptr,
      0));

  // Delete fragments list
  try {
    array->array_->delete_fragments_list(uris);
  } catch (...) {
    throw_if_not_ok(array->array_->close());
    delete array;
    array = nullptr;
    throw api::CAPIStatusException("Failed to delete fragments_list");
  }

  // Close the array
  throw_if_not_ok(array->array_->close());
  delete array;
  array = nullptr;

  return TILEDB_OK;
}

int32_t tiledb_array_open(
    tiledb_ctx_t* ctx, tiledb_array_t* array, tiledb_query_type_t query_type) {
  if (sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  // Open array
  throw_if_not_ok(array->array_->open(
      static_cast<tiledb::sm::QueryType>(query_type),
      tiledb::sm::EncryptionType::NO_ENCRYPTION,
      nullptr,
      0));

  return TILEDB_OK;
}

int32_t tiledb_array_is_open(
    tiledb_ctx_t* ctx, tiledb_array_t* array, int32_t* is_open) {
  if (sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  *is_open = (int32_t)array->array_->is_open();

  return TILEDB_OK;
}

int32_t tiledb_array_reopen(tiledb_ctx_t* ctx, tiledb_array_t* array) {
  if (sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  // Reopen array
  throw_if_not_ok(array->array_->reopen());

  return TILEDB_OK;
}

int32_t tiledb_array_set_config(
    tiledb_ctx_t* ctx, tiledb_array_t* array, tiledb_config_t* config) {
  // Sanity check
  if (sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;
  api::ensure_config_is_valid(config);
  array->array_->set_config(config->config());
  return TILEDB_OK;
}

int32_t tiledb_array_get_config(
    tiledb_ctx_t* ctx, tiledb_array_t* array, tiledb_config_t** config) {
  if (sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;
  api::ensure_output_pointer_is_valid(config);
  *config = tiledb_config_handle_t::make_handle(array->array_->config());
  return TILEDB_OK;
}

int32_t tiledb_array_close(tiledb_ctx_t* ctx, tiledb_array_t* array) {
  if (sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  // Close array
  throw_if_not_ok(array->array_->close());

  return TILEDB_OK;
}

void tiledb_array_free(tiledb_array_t** array) {
  if (array != nullptr && *array != nullptr) {
    delete *array;
    *array = nullptr;
  }
}

int32_t tiledb_array_get_schema(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_array_schema_t** array_schema) {
  if (sanity_check(ctx, array) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  *array_schema = new (std::nothrow) tiledb_array_schema_t;
  if (*array_schema == nullptr) {
    auto st = Status_Error("Failed to allocate TileDB array schema");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Get schema
  auto&& [st, array_schema_get] = array->array_->get_array_schema();
  if (!st.ok()) {
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    delete *array_schema;
    *array_schema = nullptr;
    return TILEDB_ERR;
  }
  (*array_schema)->array_schema_ = array_schema_get.value();

  return TILEDB_OK;
}

int32_t tiledb_array_get_query_type(
    tiledb_ctx_t* ctx, tiledb_array_t* array, tiledb_query_type_t* query_type) {
  // Sanity checks
  if (sanity_check(ctx, array) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  // Get query_type
  tiledb::sm::QueryType type;
  try {
    type = array->array_->get_query_type();
  } catch (StatusException& e) {
    return TILEDB_ERR;
  }

  *query_type = static_cast<tiledb_query_type_t>(type);

  return TILEDB_OK;
}

int32_t tiledb_array_create(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    const tiledb_array_schema_t* array_schema) {
  // Sanity checks
  if (sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;

  // Check array name
  tiledb::sm::URI uri(array_uri);
  if (uri.is_invalid()) {
    auto st = Status_Error("Failed to create array; Invalid array URI");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  if (uri.is_tiledb()) {
    // Check REST client
    auto rest_client = ctx->storage_manager()->rest_client();
    if (rest_client == nullptr) {
      auto st = Status_Error(
          "Failed to create array; remote array with no REST client.");
      LOG_STATUS_NO_RETURN_VALUE(st);
      save_error(ctx, st);
      return TILEDB_ERR;
    }

    throw_if_not_ok(rest_client->post_array_schema_to_rest(
        uri, *(array_schema->array_schema_.get())));
  } else {
    // Create key
    tiledb::sm::EncryptionKey key;
    throw_if_not_ok(
        key.set_key(tiledb::sm::EncryptionType::NO_ENCRYPTION, nullptr, 0));
    // Create the array
    throw_if_not_ok(ctx->storage_manager()->array_create(
        uri, array_schema->array_schema_, key));

    // Create any dimension labels in the array.
    for (tiledb::sm::ArraySchema::dimension_label_size_type ilabel{0};
         ilabel < array_schema->array_schema_->dim_label_num();
         ++ilabel) {
      // Get dimension label information and define URI and name.
      const auto& dim_label_ref =
          array_schema->array_schema_->dimension_label(ilabel);
      if (dim_label_ref.is_external())
        continue;
      if (!dim_label_ref.has_schema()) {
        throw StatusException(
            Status_Error("Failed to create array. Dimension labels that are "
                         "not external must have a schema."));
      }

      // Create the dimension label array with the same key.
      throw_if_not_ok(ctx->storage_manager()->array_create(
          dim_label_ref.uri(uri), dim_label_ref.schema(), key));
    }
  }
  return TILEDB_OK;
}

int32_t tiledb_array_create_with_key(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    const tiledb_array_schema_t* array_schema,
    tiledb_encryption_type_t encryption_type,
    const void* encryption_key,
    uint32_t key_length) {
  // Sanity checks
  if (sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;

  // Check array name
  tiledb::sm::URI uri(array_uri);
  if (uri.is_invalid()) {
    auto st = Status_Error("Failed to create array; Invalid array URI");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  if (uri.is_tiledb()) {
    // Check unencrypted
    if (encryption_type != TILEDB_NO_ENCRYPTION) {
      auto st = Status_Error(
          "Failed to create array; encrypted remote arrays are not "
          "supported.");
      LOG_STATUS_NO_RETURN_VALUE(st);
      save_error(ctx, st);
      return TILEDB_ERR;
    }

    // Check REST client
    auto rest_client = ctx->storage_manager()->rest_client();
    if (rest_client == nullptr) {
      auto st = Status_Error(
          "Failed to create array; remote array with no REST client.");
      LOG_STATUS_NO_RETURN_VALUE(st);
      save_error(ctx, st);
      return TILEDB_ERR;
    }

    throw_if_not_ok(rest_client->post_array_schema_to_rest(
        uri, *(array_schema->array_schema_.get())));
  } else {
    // Create key
    tiledb::sm::EncryptionKey key;
    throw_if_not_ok(key.set_key(
        static_cast<tiledb::sm::EncryptionType>(encryption_type),
        encryption_key,
        key_length));

    // Create the array
    throw_if_not_ok(ctx->storage_manager()->array_create(
        uri, array_schema->array_schema_, key));

    // Create any dimension labels in the array.
    for (tiledb::sm::ArraySchema::dimension_label_size_type ilabel{0};
         ilabel < array_schema->array_schema_->dim_label_num();
         ++ilabel) {
      // Get dimension label information and define URI and name.
      const auto& dim_label_ref =
          array_schema->array_schema_->dimension_label(ilabel);
      if (dim_label_ref.is_external())
        continue;
      if (!dim_label_ref.has_schema()) {
        throw StatusException(
            Status_Error("Failed to create array. Dimension labels that are "
                         "not external must have a schema."));
      }

      // Create the dimension label array with the same key.
      throw_if_not_ok(ctx->storage_manager()->array_create(
          dim_label_ref.uri(uri), dim_label_ref.schema(), key));
    }
  }
  return TILEDB_OK;
}

int32_t tiledb_array_consolidate(
    tiledb_ctx_t* ctx, const char* array_uri, tiledb_config_t* config) {
  api::ensure_config_is_valid_if_present(config);
  throw_if_not_ok(ctx->storage_manager()->array_consolidate(
      array_uri,
      tiledb::sm::EncryptionType::NO_ENCRYPTION,
      nullptr,
      0,
      (config == nullptr) ? ctx->storage_manager()->config() :
                            config->config()));
  return TILEDB_OK;
}

int32_t tiledb_array_consolidate_with_key(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_encryption_type_t encryption_type,
    const void* encryption_key,
    uint32_t key_length,
    tiledb_config_t* config) {
  // Sanity checks

  throw_if_not_ok(ctx->storage_manager()->array_consolidate(
      array_uri,
      static_cast<tiledb::sm::EncryptionType>(encryption_type),
      encryption_key,
      key_length,
      (config == nullptr) ? ctx->storage_manager()->config() :
                            config->config()));

  return TILEDB_OK;
}

int32_t tiledb_array_consolidate_fragments(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    const char** fragment_uris,
    const uint64_t num_fragments,
    tiledb_config_t* config) {
  // Sanity checks

  // Convert the list of fragments to a vector
  std::vector<std::string> uris;
  uris.reserve(num_fragments);
  for (uint64_t i = 0; i < num_fragments; i++) {
    uris.emplace_back(fragment_uris[i]);
  }

  throw_if_not_ok(ctx->storage_manager()->fragments_consolidate(
      array_uri,
      tiledb::sm::EncryptionType::NO_ENCRYPTION,
      nullptr,
      0,
      uris,
      (config == nullptr) ? ctx->storage_manager()->config() :
                            config->config()));

  return TILEDB_OK;
}

int32_t tiledb_array_vacuum(
    tiledb_ctx_t* ctx, const char* array_uri, tiledb_config_t* config) {
  ctx->storage_manager()->array_vacuum(
      array_uri,
      (config == nullptr) ? ctx->storage_manager()->config() :
                            config->config());

  return TILEDB_OK;
}

int32_t tiledb_array_get_non_empty_domain(
    tiledb_ctx_t* ctx, tiledb_array_t* array, void* domain, int32_t* is_empty) {
  if (sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  bool is_empty_b;

  throw_if_not_ok(ctx->storage_manager()->array_get_non_empty_domain(
      array->array_.get(), domain, &is_empty_b));

  *is_empty = (int32_t)is_empty_b;

  return TILEDB_OK;
}

int32_t tiledb_array_get_non_empty_domain_from_index(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    uint32_t idx,
    void* domain,
    int32_t* is_empty) {
  if (sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  bool is_empty_b;

  throw_if_not_ok(ctx->storage_manager()->array_get_non_empty_domain_from_index(
      array->array_.get(), idx, domain, &is_empty_b));

  *is_empty = (int32_t)is_empty_b;

  return TILEDB_OK;
}

int32_t tiledb_array_get_non_empty_domain_from_name(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const char* name,
    void* domain,
    int32_t* is_empty) {
  if (sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  bool is_empty_b;

  throw_if_not_ok(ctx->storage_manager()->array_get_non_empty_domain_from_name(
      array->array_.get(), name, domain, &is_empty_b));

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
  if (sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  bool is_empty_b = true;

  throw_if_not_ok(
      ctx->storage_manager()->array_get_non_empty_domain_var_size_from_index(
          array->array_.get(), idx, start_size, end_size, &is_empty_b));

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
  if (sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  bool is_empty_b = true;

  throw_if_not_ok(
      ctx->storage_manager()->array_get_non_empty_domain_var_size_from_name(
          array->array_.get(), name, start_size, end_size, &is_empty_b));

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
  if (sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  bool is_empty_b = true;

  throw_if_not_ok(
      ctx->storage_manager()->array_get_non_empty_domain_var_from_index(
          array->array_.get(), idx, start, end, &is_empty_b));

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
  if (sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  bool is_empty_b = true;

  throw_if_not_ok(
      ctx->storage_manager()->array_get_non_empty_domain_var_from_name(
          array->array_.get(), name, start, end, &is_empty_b));

  *is_empty = (int32_t)is_empty_b;

  return TILEDB_OK;
}

int32_t tiledb_array_get_uri(
    tiledb_ctx_t* ctx, tiledb_array_t* array, const char** array_uri) {
  // Sanity checks
  if (sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  *array_uri = array->array_.get()->array_uri().c_str();

  return TILEDB_OK;
}

int32_t tiledb_array_encryption_type(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_encryption_type_t* encryption_type) {
  // Sanity checks
  if (array_uri == nullptr || encryption_type == nullptr)
    return TILEDB_ERR;

  // For easy reference
  auto storage_manager = ctx->storage_manager();
  auto uri = tiledb::sm::URI(array_uri);

  // Load URIs from the array directory
  tiledb::sm::ArrayDirectory array_dir(storage_manager->resources(), uri);
  try {
    array_dir = tiledb::sm::ArrayDirectory(
        storage_manager->resources(),
        uri,
        0,
        UINT64_MAX,
        tiledb::sm::ArrayDirectoryMode::SCHEMA_ONLY);
  } catch (const std::logic_error& le) {
    auto st = Status_ArrayDirectoryError(le.what());
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  // Get encryption type
  tiledb::sm::EncryptionType enc;
  throw_if_not_ok(
      ctx->storage_manager()->array_get_encryption(array_dir, &enc));

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
  if (sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  // Put metadata
  array->array_->put_metadata(
      key, static_cast<tiledb::sm::Datatype>(value_type), value_num, value);

  return TILEDB_OK;
}

int32_t tiledb_array_delete_metadata(
    tiledb_ctx_t* ctx, tiledb_array_t* array, const char* key) {
  if (sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  // Put metadata
  array->array_->delete_metadata(key);

  return TILEDB_OK;
}

int32_t tiledb_array_get_metadata(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const char* key,
    tiledb_datatype_t* value_type,
    uint32_t* value_num,
    const void** value) {
  if (sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  // Get metadata
  tiledb::sm::Datatype type;
  array->array_->get_metadata(key, &type, value_num, value);

  *value_type = static_cast<tiledb_datatype_t>(type);

  return TILEDB_OK;
}

int32_t tiledb_array_get_metadata_num(
    tiledb_ctx_t* ctx, tiledb_array_t* array, uint64_t* num) {
  if (sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  // Get metadata num
  *num = array->array_->metadata_num();

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
  if (sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  // Get metadata
  tiledb::sm::Datatype type;
  array->array_->get_metadata(index, key, key_len, &type, value_num, value);

  *value_type = static_cast<tiledb_datatype_t>(type);

  return TILEDB_OK;
}

int32_t tiledb_array_has_metadata_key(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const char* key,
    tiledb_datatype_t* value_type,
    int32_t* has_key) {
  if (sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  // Check whether metadata has_key
  std::optional<tiledb::sm::Datatype> type = array->array_->metadata_type(key);

  *has_key = type.has_value();
  if (*has_key) {
    *value_type = static_cast<tiledb_datatype_t>(type.value());
  }
  return TILEDB_OK;
}

int32_t tiledb_array_evolve(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_array_schema_evolution_t* array_schema_evolution) {
  // Sanity Checks
  if (sanity_check(ctx, array_schema_evolution) == TILEDB_ERR)
    return TILEDB_ERR;

  // Check array name
  tiledb::sm::URI uri(array_uri);
  if (uri.is_invalid()) {
    auto st = Status_Error("Failed to create array; Invalid array URI");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  // Create key
  tiledb::sm::EncryptionKey key;
  throw_if_not_ok(
      key.set_key(tiledb::sm::EncryptionType::NO_ENCRYPTION, nullptr, 0));
  // Evolve schema
  throw_if_not_ok(ctx->storage_manager()->array_evolve_schema(
      uri, array_schema_evolution->array_schema_evolution_, key));

  // Success
  return TILEDB_OK;
}

capi_return_t tiledb_array_get_enumeration(
    tiledb_ctx_t* ctx,
    const tiledb_array_t* array,
    const char* attr_name,
    tiledb_enumeration_t** enumeration) {
  if (sanity_check(ctx, array) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  if (attr_name == nullptr) {
    throw api::CAPIStatusException("'attr_name' must not be null");
  }

  api::ensure_output_pointer_is_valid(enumeration);
  auto ptr = array->array_->get_enumeration(attr_name);
  *enumeration = tiledb_enumeration_handle_t::make_handle(ptr);

  return TILEDB_OK;
}

capi_return_t tiledb_array_load_all_enumerations(
    tiledb_ctx_t* ctx, const tiledb_array_t* array) {
  if (sanity_check(ctx, array) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  array->array_->load_all_enumerations();

  return TILEDB_OK;
}

int32_t tiledb_array_upgrade_version(
    tiledb_ctx_t* ctx, const char* array_uri, tiledb_config_t* config) {
  // Sanity Checks

  // Check array name
  tiledb::sm::URI uri(array_uri);
  if (uri.is_invalid()) {
    auto st = Status_Error("Failed to find the array; Invalid array URI");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  // Upgrade version
  throw_if_not_ok(ctx->storage_manager()->array_upgrade_version(
      uri,
      (config == nullptr) ? ctx->storage_manager()->config() :
                            config->config()));

  return TILEDB_OK;
}

/* ****************************** */
/*         OBJECT MANAGEMENT      */
/* ****************************** */

int32_t tiledb_object_type(
    tiledb_ctx_t* ctx, const char* path, tiledb_object_t* type) {
  auto uri = tiledb::sm::URI(path);
  tiledb::sm::ObjectType object_type;
  throw_if_not_ok(ctx->storage_manager()->object_type(uri, &object_type));

  *type = static_cast<tiledb_object_t>(object_type);
  return TILEDB_OK;
}

int32_t tiledb_object_remove(tiledb_ctx_t* ctx, const char* path) {
  throw_if_not_ok(ctx->storage_manager()->object_remove(path));
  return TILEDB_OK;
}

int32_t tiledb_object_move(
    tiledb_ctx_t* ctx, const char* old_path, const char* new_path) {
  throw_if_not_ok(ctx->storage_manager()->object_move(old_path, new_path));
  return TILEDB_OK;
}

int32_t tiledb_object_walk(
    tiledb_ctx_t* ctx,
    const char* path,
    tiledb_walk_order_t order,
    int32_t (*callback)(const char*, tiledb_object_t, void*),
    void* data) {
  // Sanity checks

  if (callback == nullptr) {
    auto st = Status_Error("Cannot initiate walk; Invalid callback function");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  // Create an object iterator
  tiledb::sm::StorageManager::ObjectIter* obj_iter;
  throw_if_not_ok(ctx->storage_manager()->object_iter_begin(
      &obj_iter, path, static_cast<tiledb::sm::WalkOrder>(order)));

  // For as long as there is another object and the callback indicates to
  // continue, walk over the TileDB objects in the path
  const char* obj_name;
  tiledb::sm::ObjectType obj_type;
  bool has_next;
  int32_t rc = 0;
  do {
    if (SAVE_ERROR_CATCH(
            ctx,
            ctx->storage_manager()->object_iter_next(
                obj_iter, &obj_name, &obj_type, &has_next))) {
      ctx->storage_manager()->object_iter_free(obj_iter);
      return TILEDB_ERR;
    }
    if (!has_next)
      break;
    rc = callback(obj_name, tiledb_object_t(obj_type), data);
  } while (rc == 1);

  // Clean up
  ctx->storage_manager()->object_iter_free(obj_iter);

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

  if (callback == nullptr) {
    auto st =
        Status_Error("Cannot initiate object ls; Invalid callback function");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  // Create an object iterator
  tiledb::sm::StorageManager::ObjectIter* obj_iter;
  throw_if_not_ok(ctx->storage_manager()->object_iter_begin(&obj_iter, path));

  // For as long as there is another object and the callback indicates to
  // continue, walk over the TileDB objects in the path
  const char* obj_name;
  tiledb::sm::ObjectType obj_type;
  bool has_next;
  int32_t rc = 0;
  do {
    if (SAVE_ERROR_CATCH(
            ctx,
            ctx->storage_manager()->object_iter_next(
                obj_iter, &obj_name, &obj_type, &has_next))) {
      ctx->storage_manager()->object_iter_free(obj_iter);
      return TILEDB_ERR;
    }
    if (!has_next)
      break;
    rc = callback(obj_name, tiledb_object_t(obj_type), data);
  } while (rc == 1);

  // Clean up
  ctx->storage_manager()->object_iter_free(obj_iter);

  if (rc == -1)
    return TILEDB_ERR;
  return TILEDB_OK;
}

/* ****************************** */
/*              URI               */
/* ****************************** */

int32_t tiledb_uri_to_path(
    tiledb_ctx_t*, const char* uri, char* path_out, uint32_t* path_length) {
  if (uri == nullptr || path_out == nullptr || path_length == nullptr)
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
/*          Heap Profiler         */
/* ****************************** */

int32_t tiledb_heap_profiler_enable(
    const char* const file_name_prefix,
    const uint64_t dump_interval_ms,
    const uint64_t dump_interval_bytes,
    const uint64_t dump_threshold_bytes) {
  tiledb::common::heap_profiler.enable(
      file_name_prefix ? std::string(file_name_prefix) : "",
      dump_interval_ms,
      dump_interval_bytes,
      dump_threshold_bytes);
  return TILEDB_OK;
}

/* ****************************** */
/*          Serialization         */
/* ****************************** */

int32_t tiledb_serialize_array(
    tiledb_ctx_t* ctx,
    const tiledb_array_t* array,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_buffer_t** buffer) {
  // Sanity check
  if (sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  auto buf = tiledb_buffer_handle_t::make_handle();

  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::array_serialize(
              array->array_.get(),
              (tiledb::sm::SerializationType)serialize_type,
              &(buf->buffer()),
              client_side))) {
    tiledb_buffer_handle_t::break_handle(buf);
    return TILEDB_ERR;
  }

  *buffer = buf;

  return TILEDB_OK;
}

int32_t tiledb_deserialize_array(
    tiledb_ctx_t* ctx,
    const tiledb_buffer_t* buffer,
    tiledb_serialization_type_t serialize_type,
    int32_t,
    tiledb_array_t** array) {
  // Sanity check

  api::ensure_buffer_is_valid(buffer);

  // Create array struct
  *array = new (std::nothrow) tiledb_array_t;
  if (*array == nullptr) {
    auto st = Status_Error("Failed to allocate TileDB array object");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Check array URI
  auto uri = tiledb::sm::URI("deserialized_array");
  if (uri.is_invalid()) {
    auto st = Status_Error("Failed to create TileDB array object; Invalid URI");
    delete *array;
    *array = nullptr;
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  // Allocate an array object
  try {
    (*array)->array_ =
        make_shared<tiledb::sm::Array>(HERE(), uri, ctx->storage_manager());
  } catch (std::bad_alloc&) {
    auto st = Status_Error(
        "Failed to create TileDB array object; Memory allocation "
        "error");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::array_deserialize(
              (*array)->array_.get(),
              (tiledb::sm::SerializationType)serialize_type,
              buffer->buffer(),
              ctx->storage_manager()))) {
    delete *array;
    *array = nullptr;
    return TILEDB_ERR;
  }

  return TILEDB_OK;
}

int32_t tiledb_serialize_array_schema(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_buffer_t** buffer) {
  // Sanity check
  if (sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create buffer
  auto buf = tiledb_buffer_handle_t::make_handle();

  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::array_schema_serialize(
              *(array_schema->array_schema_.get()),
              (tiledb::sm::SerializationType)serialize_type,
              &(buf->buffer()),
              client_side))) {
    tiledb_buffer_handle_t::break_handle(buf);
    return TILEDB_ERR;
  }

  *buffer = buf;

  return TILEDB_OK;
}

int32_t tiledb_deserialize_array_schema(
    tiledb_ctx_t* ctx,
    const tiledb_buffer_t* buffer,
    tiledb_serialization_type_t serialize_type,
    int32_t,
    tiledb_array_schema_t** array_schema) {
  api::ensure_buffer_is_valid(buffer);

  // Create array schema struct
  *array_schema = new (std::nothrow) tiledb_array_schema_t;
  if (*array_schema == nullptr) {
    auto st = Status_Error("Failed to allocate TileDB array schema object");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  try {
    (*array_schema)->array_schema_ = make_shared<tiledb::sm::ArraySchema>(
        HERE(),
        tiledb::sm::serialization::array_schema_deserialize(
            (tiledb::sm::SerializationType)serialize_type, buffer->buffer()));
  } catch (...) {
    delete *array_schema;
    *array_schema = nullptr;
    throw;
  }

  return TILEDB_OK;
}

int32_t tiledb_serialize_array_open(
    tiledb_ctx_t* ctx,
    const tiledb_array_t* array,
    tiledb_serialization_type_t serialize_type,
    int32_t,
    tiledb_buffer_t** buffer) {
  // Currently no different behaviour is required if array open is serialized by
  // the client or the Cloud server, so the variable is unused
  // Sanity check
  if (sanity_check(ctx, array) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  auto buf = tiledb_buffer_handle_t::make_handle();

  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::array_open_serialize(
              *(array->array_.get()),
              (tiledb::sm::SerializationType)serialize_type,
              &(buf->buffer())))) {
    tiledb_buffer_handle_t::break_handle(buf);
    return TILEDB_ERR;
  }

  *buffer = buf;

  return TILEDB_OK;
}

int32_t tiledb_deserialize_array_open(
    tiledb_ctx_t* ctx,
    const tiledb_buffer_t* buffer,
    tiledb_serialization_type_t serialize_type,
    int32_t,
    tiledb_array_t** array) {
  // Currently no different behaviour is required if array open is deserialized
  // by the client or the Cloud server
  api::ensure_buffer_is_valid(buffer);

  // Create array struct
  *array = new (std::nothrow) tiledb_array_t;
  if (*array == nullptr) {
    auto st = Status_Error("Failed to allocate TileDB array object");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Check array URI
  auto uri = tiledb::sm::URI("deserialized_array");
  if (uri.is_invalid()) {
    auto st = Status_Error("Failed to create TileDB array object; Invalid URI");
    delete *array;
    *array = nullptr;
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  // Allocate an array object
  try {
    (*array)->array_ =
        make_shared<tiledb::sm::Array>(HERE(), uri, ctx->storage_manager());
  } catch (std::bad_alloc&) {
    auto st = Status_Error(
        "Failed to create TileDB array object; Memory allocation "
        "error");
    delete *array;
    *array = nullptr;
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::array_open_deserialize(
              (*array)->array_.get(),
              (tiledb::sm::SerializationType)serialize_type,
              buffer->buffer()))) {
    delete *array;
    *array = nullptr;
    return TILEDB_ERR;
  }

  return TILEDB_OK;
}

int32_t tiledb_serialize_array_schema_evolution(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_evolution_t* array_schema_evolution,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_buffer_t** buffer) {
  // Sanity check
  if (sanity_check(ctx, array_schema_evolution) == TILEDB_ERR)
    return TILEDB_ERR;

  auto buf = tiledb_buffer_handle_t::make_handle();

  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::array_schema_evolution_serialize(
              array_schema_evolution->array_schema_evolution_,
              (tiledb::sm::SerializationType)serialize_type,
              &(buf->buffer()),
              client_side))) {
    tiledb_buffer_handle_t::break_handle(buf);
    return TILEDB_ERR;
  }

  *buffer = buf;

  return TILEDB_OK;
}

int32_t tiledb_deserialize_array_schema_evolution(
    tiledb_ctx_t* ctx,
    const tiledb_buffer_t* buffer,
    tiledb_serialization_type_t serialize_type,
    int32_t,
    tiledb_array_schema_evolution_t** array_schema_evolution) {
  // Sanity check

  api::ensure_buffer_is_valid(buffer);

  // Create array schema struct
  *array_schema_evolution = new (std::nothrow) tiledb_array_schema_evolution_t;
  if (*array_schema_evolution == nullptr) {
    auto st =
        Status_Error("Failed to allocate TileDB array schema evolution object");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::array_schema_evolution_deserialize(
              &((*array_schema_evolution)->array_schema_evolution_),
              (tiledb::sm::SerializationType)serialize_type,
              buffer->buffer()))) {
    delete *array_schema_evolution;
    *array_schema_evolution = nullptr;
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
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  // Allocate a buffer list
  if (tiledb_buffer_list_alloc(ctx, buffer_list) != TILEDB_OK)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::query_serialize(
              query->query_,
              (tiledb::sm::SerializationType)serialize_type,
              client_side == 1,
              &(*buffer_list)->buffer_list()))) {
    tiledb_buffer_list_free(buffer_list);
    return TILEDB_ERR;
  }

  return TILEDB_OK;
}

int32_t tiledb_deserialize_query(
    tiledb_ctx_t* ctx,
    const tiledb_buffer_t* buffer,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_query_t* query) {
  // Sanity check
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  api::ensure_buffer_is_valid(buffer);

  throw_if_not_ok(tiledb::sm::serialization::query_deserialize(
      buffer->buffer(),
      (tiledb::sm::SerializationType)serialize_type,
      client_side == 1,
      nullptr,
      query->query_,
      ctx->storage_manager()->compute_tp()));

  return TILEDB_OK;
}

int32_t tiledb_deserialize_query_and_array(
    tiledb_ctx_t* ctx,
    const tiledb_buffer_t* buffer,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    const char* array_uri,
    tiledb_query_t** query,
    tiledb_array_t** array) {
  // Sanity check
  if (query == nullptr) {
    return TILEDB_ERR;
  }

  api::ensure_buffer_is_valid(buffer);

  // Create array struct
  *array = new (std::nothrow) tiledb_array_t;
  if (*array == nullptr) {
    auto st = Status_Error(
        "Failed to create TileDB array object; Memory allocation error");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Check array URI
  auto uri = tiledb::sm::URI(array_uri);
  if (uri.is_invalid()) {
    auto st = Status_Error("Failed to create TileDB array object; Invalid URI");
    delete *array;
    *array = nullptr;
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  // Allocate an array object
  try {
    (*array)->array_ =
        make_shared<tiledb::sm::Array>(HERE(), uri, ctx->storage_manager());
  } catch (std::bad_alloc&) {
    auto st = Status_Error(
        "Failed to create TileDB array object; Memory allocation error");
    delete array;
    array = nullptr;
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // First deserialize the array included in the query
  throw_if_not_ok(tiledb::sm::serialization::array_from_query_deserialize(
      buffer->buffer(),
      (tiledb::sm::SerializationType)serialize_type,
      *(*array)->array_,
      ctx->storage_manager()));

  // Create query struct
  *query = new (std::nothrow) tiledb_query_t;
  if (*query == nullptr) {
    auto st = Status_Error(
        "Failed to allocate TileDB query object; Memory allocation failed");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Create query
  (*query)->query_ = new (std::nothrow)
      tiledb::sm::Query(ctx->storage_manager(), (*array)->array_);
  if ((*query)->query_ == nullptr) {
    auto st = Status_Error(
        "Failed to allocate TileDB query object; Memory allocation failed");
    delete *query;
    *query = nullptr;
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  throw_if_not_ok(tiledb::sm::serialization::query_deserialize(
      buffer->buffer(),
      (tiledb::sm::SerializationType)serialize_type,
      client_side == 1,
      nullptr,
      (*query)->query_,
      ctx->storage_manager()->compute_tp()));

  return TILEDB_OK;
}

int32_t tiledb_serialize_array_nonempty_domain(
    tiledb_ctx_t* ctx,
    const tiledb_array_t* array,
    const void* nonempty_domain,
    int32_t is_empty,
    tiledb_serialization_type_t serialize_type,
    int32_t,
    tiledb_buffer_t** buffer) {
  // Sanity check
  if (sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  auto buf = tiledb_buffer_handle_t::make_handle();

  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::nonempty_domain_serialize(
              array->array_.get(),
              nonempty_domain,
              is_empty,
              (tiledb::sm::SerializationType)serialize_type,
              &(buf->buffer())))) {
    tiledb_buffer_handle_t::break_handle(buf);
    return TILEDB_ERR;
  }

  *buffer = buf;

  return TILEDB_OK;
}

int32_t tiledb_deserialize_array_nonempty_domain(
    tiledb_ctx_t* ctx,
    const tiledb_array_t* array,
    const tiledb_buffer_t* buffer,
    tiledb_serialization_type_t serialize_type,
    int32_t,
    void* nonempty_domain,
    int32_t* is_empty) {
  // Sanity check
  if (sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  api::ensure_buffer_is_valid(buffer);

  bool is_empty_bool;
  throw_if_not_ok(tiledb::sm::serialization::nonempty_domain_deserialize(
      array->array_.get(),
      buffer->buffer(),
      (tiledb::sm::SerializationType)serialize_type,
      nonempty_domain,
      &is_empty_bool));

  *is_empty = is_empty_bool ? 1 : 0;

  return TILEDB_OK;
}

int32_t tiledb_serialize_array_non_empty_domain_all_dimensions(
    tiledb_ctx_t* ctx,
    const tiledb_array_t* array,
    tiledb_serialization_type_t serialize_type,
    int32_t,
    tiledb_buffer_t** buffer) {
  // Sanity check
  if (sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  auto buf = tiledb_buffer_handle_t::make_handle();

  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::nonempty_domain_serialize(
              array->array_.get(),
              (tiledb::sm::SerializationType)serialize_type,
              &(buf->buffer())))) {
    tiledb_buffer_handle_t::break_handle(buf);
    return TILEDB_ERR;
  }

  *buffer = buf;

  return TILEDB_OK;
}

int32_t tiledb_deserialize_array_non_empty_domain_all_dimensions(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const tiledb_buffer_t* buffer,
    tiledb_serialization_type_t serialize_type,
    int32_t) {
  // Sanity check
  if (sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  api::ensure_buffer_is_valid(buffer);

  throw_if_not_ok(tiledb::sm::serialization::nonempty_domain_deserialize(
      array->array_.get(),
      buffer->buffer(),
      (tiledb::sm::SerializationType)serialize_type));

  return TILEDB_OK;
}

int32_t tiledb_serialize_array_max_buffer_sizes(
    tiledb_ctx_t* ctx,
    const tiledb_array_t* array,
    const void* subarray,
    tiledb_serialization_type_t serialize_type,
    tiledb_buffer_t** buffer) {
  // Sanity check
  if (sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  auto buf = tiledb_buffer_handle_t::make_handle();

  // Serialize
  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::max_buffer_sizes_serialize(
              array->array_.get(),
              subarray,
              (tiledb::sm::SerializationType)serialize_type,
              &(buf->buffer())))) {
    tiledb_buffer_handle_t::break_handle(buf);
    return TILEDB_ERR;
  }

  *buffer = buf;

  return TILEDB_OK;
}

capi_return_t tiledb_handle_array_delete_fragments_timestamps_request(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_serialization_type_t serialize_type,
    const tiledb_buffer_t* request) {
  if (sanity_check(ctx, array) == TILEDB_ERR) {
    throw std::invalid_argument("Array paramter must be valid.");
  }

  api::ensure_buffer_is_valid(request);

  // Deserialize buffer
  auto [timestamp_start, timestamp_end] = tiledb::sm::serialization::
      deserialize_delete_fragments_timestamps_request(
          (tiledb::sm::SerializationType)serialize_type, request->buffer());

  // Delete fragments
  try {
    array->array_->delete_fragments(
        array->array_->array_uri(), timestamp_start, timestamp_end);
  } catch (...) {
    throw api::CAPIStatusException("Failed to delete fragments");
  }

  return TILEDB_OK;
}

capi_return_t tiledb_handle_array_delete_fragments_list_request(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_serialization_type_t serialize_type,
    const tiledb_buffer_t* request) {
  if (sanity_check(ctx, array) == TILEDB_ERR) {
    throw std::invalid_argument("Array paramter must be valid.");
  }

  api::ensure_buffer_is_valid(request);

  // Deserialize buffer
  auto uris =
      tiledb::sm::serialization::deserialize_delete_fragments_list_request(
          array->array_->array_uri(),
          (tiledb::sm::SerializationType)serialize_type,
          request->buffer());

  // Delete fragments list
  try {
    array->array_->delete_fragments_list(uris);
  } catch (...) {
    throw api::CAPIStatusException("Failed to delete fragments_list");
  }

  return TILEDB_OK;
}

int32_t tiledb_serialize_array_metadata(
    tiledb_ctx_t* ctx,
    const tiledb_array_t* array,
    tiledb_serialization_type_t serialize_type,
    tiledb_buffer_t** buffer) {
  // Sanity check
  if (sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  auto buf = tiledb_buffer_handle_t::make_handle();

  // Get metadata to serialize, this will load it if it does not exist
  tiledb::sm::Metadata* metadata;
  if (SAVE_ERROR_CATCH(ctx, array->array_->metadata(&metadata))) {
    tiledb_buffer_handle_t::break_handle(buf);
    return TILEDB_ERR;
  }

  // Serialize
  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::metadata_serialize(
              metadata,
              (tiledb::sm::SerializationType)serialize_type,
              &(buf->buffer())))) {
    tiledb_buffer_handle_t::break_handle(buf);
    return TILEDB_ERR;
  }

  *buffer = buf;

  return TILEDB_OK;
}

int32_t tiledb_deserialize_array_metadata(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_serialization_type_t serialize_type,
    const tiledb_buffer_t* buffer) {
  // Sanity check
  if (sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  api::ensure_buffer_is_valid(buffer);

  // Deserialize
  throw_if_not_ok(tiledb::sm::serialization::metadata_deserialize(
      array->array_->unsafe_metadata(),
      (tiledb::sm::SerializationType)serialize_type,
      buffer->buffer()));

  return TILEDB_OK;
}

int32_t tiledb_serialize_query_est_result_sizes(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_buffer_t** buffer) {
  // Sanity check
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  auto buf = tiledb_buffer_handle_t::make_handle();

  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::query_est_result_size_serialize(
              query->query_,
              (tiledb::sm::SerializationType)serialize_type,
              client_side == 1,
              &(buf->buffer())))) {
    tiledb_buffer_handle_t::break_handle(buf);
    return TILEDB_ERR;
  }

  *buffer = buf;

  return TILEDB_OK;
}

int32_t tiledb_deserialize_query_est_result_sizes(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    const tiledb_buffer_t* buffer) {
  // Sanity check
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  api::ensure_buffer_is_valid(buffer);

  throw_if_not_ok(tiledb::sm::serialization::query_est_result_size_deserialize(
      query->query_,
      (tiledb::sm::SerializationType)serialize_type,
      client_side == 1,
      buffer->buffer()));

  return TILEDB_OK;
}

int32_t tiledb_serialize_config(
    tiledb_ctx_t* ctx,
    const tiledb_config_t* config,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_buffer_t** buffer) {
  // Sanity check

  api::ensure_config_is_valid(config);

  auto buf = tiledb_buffer_handle_t::make_handle();

  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::config_serialize(
              config->config(),
              (tiledb::sm::SerializationType)serialize_type,
              &(buf->buffer()),
              client_side))) {
    tiledb_buffer_handle_t::break_handle(buf);
    return TILEDB_ERR;
  }

  *buffer = buf;

  return TILEDB_OK;
}

int32_t tiledb_deserialize_config(
    const tiledb_buffer_t* buffer,
    tiledb_serialization_type_t serialize_type,
    int32_t,
    tiledb_config_t** config) {
  api::ensure_buffer_is_valid(buffer);
  api::ensure_output_pointer_is_valid(config);

  /*
   * `config_deserialize` returns a pointer to an allocated `Config`. That was
   * acceptable when that was how `tiledb_config_t` was implemented. In the
   * interim, we copy the result. Later, the function should be updated to
   * return its object and throw on error.
   */
  tiledb::sm::Config* new_config;
  throw_if_not_ok(tiledb::sm::serialization::config_deserialize(
      &new_config,
      (tiledb::sm::SerializationType)serialize_type,
      buffer->buffer()));
  if (!new_config) {
    throw std::logic_error("Unexpected nullptr with OK status");
  }
  // Copy the result into a handle
  *config = tiledb_config_handle_t::make_handle(*new_config);
  // Caller of `config_deserialize` has the responsibility for deallocation
  delete new_config;
  return TILEDB_OK;
}

int32_t tiledb_serialize_fragment_info_request(
    tiledb_ctx_t* ctx,
    const tiledb_fragment_info_t* fragment_info,
    tiledb_serialization_type_t serialize_type,
    int32_t,
    tiledb_buffer_t** buffer) {
  // Currently no different behaviour is required if fragment info request is
  // serialized by the client or the Cloud server

  // Sanity check
  if (sanity_check(ctx, fragment_info) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  auto buf = tiledb_buffer_handle_t::make_handle();

  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::fragment_info_request_serialize(
              *fragment_info->fragment_info_,
              (tiledb::sm::SerializationType)serialize_type,
              &(buf->buffer())))) {
    tiledb_buffer_handle_t::break_handle(buf);
    return TILEDB_ERR;
  }

  *buffer = buf;

  return TILEDB_OK;
}

int32_t tiledb_deserialize_fragment_info_request(
    tiledb_ctx_t* ctx,
    const tiledb_buffer_t* buffer,
    tiledb_serialization_type_t serialize_type,
    int32_t,
    tiledb_fragment_info_t* fragment_info) {
  // Currently no different behaviour is required if fragment info request is
  // serialized by the client or the Cloud server

  // Sanity check
  if (sanity_check(ctx, fragment_info) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  api::ensure_buffer_is_valid(buffer);

  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::fragment_info_request_deserialize(
              fragment_info->fragment_info_,
              (tiledb::sm::SerializationType)serialize_type,
              buffer->buffer()))) {
    return TILEDB_ERR;
  }

  return TILEDB_OK;
}

int32_t tiledb_serialize_fragment_info(
    tiledb_ctx_t* ctx,
    const tiledb_fragment_info_t* fragment_info,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_buffer_t** buffer) {
  // Sanity check
  if (sanity_check(ctx, fragment_info) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  auto buf = tiledb_buffer_handle_t::make_handle();

  // Serialize
  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::fragment_info_serialize(
              *fragment_info->fragment_info_,
              (tiledb::sm::SerializationType)serialize_type,
              &(buf->buffer()),
              client_side))) {
    tiledb_buffer_handle_t::break_handle(buf);
    return TILEDB_ERR;
  }

  *buffer = buf;

  return TILEDB_OK;
}

int32_t tiledb_deserialize_fragment_info(
    tiledb_ctx_t* ctx,
    const tiledb_buffer_t* buffer,
    tiledb_serialization_type_t serialize_type,
    const char* array_uri,
    int32_t,
    tiledb_fragment_info_t* fragment_info) {
  // Currently no different behaviour is required if fragment info is
  // deserialized by the client or the Cloud server

  // Sanity check
  if (sanity_check(ctx, fragment_info) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  api::ensure_buffer_is_valid(buffer);

  // Check array uri
  tiledb::sm::URI uri(array_uri);
  if (uri.is_invalid()) {
    auto st =
        Status_Error("Failed to deserialize fragment info; Invalid array URI");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::fragment_info_deserialize(
              fragment_info->fragment_info_,
              (tiledb::sm::SerializationType)serialize_type,
              uri,
              buffer->buffer()))) {
    return TILEDB_ERR;
  }

  return TILEDB_OK;
}

capi_return_t tiledb_handle_load_array_schema_request(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_serialization_type_t serialization_type,
    const tiledb_buffer_t* request,
    tiledb_buffer_t* response) {
  if (sanity_check(ctx, array) == TILEDB_ERR) {
    throw std::invalid_argument("Array paramter must be valid.");
  }

  api::ensure_buffer_is_valid(request);
  api::ensure_buffer_is_valid(response);

  auto load_schema_req =
      tiledb::sm::serialization::deserialize_load_array_schema_request(
          static_cast<tiledb::sm::SerializationType>(serialization_type),
          request->buffer());

  if (load_schema_req.include_enumerations()) {
    array->array_->load_all_enumerations();
  }

  tiledb::sm::serialization::serialize_load_array_schema_response(
      array->array_->array_schema_latest(),
      static_cast<tiledb::sm::SerializationType>(serialization_type),
      response->buffer());

  return TILEDB_OK;
}

capi_return_t tiledb_handle_load_enumerations_request(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_serialization_type_t serialization_type,
    const tiledb_buffer_t* request,
    tiledb_buffer_t* response) {
  if (sanity_check(ctx, array) == TILEDB_ERR) {
    throw std::invalid_argument("Array paramter must be valid.");
  }

  api::ensure_buffer_is_valid(request);
  api::ensure_buffer_is_valid(response);

  auto enumeration_names =
      tiledb::sm::serialization::deserialize_load_enumerations_request(
          static_cast<tiledb::sm::SerializationType>(serialization_type),
          request->buffer());
  auto enumerations = array->array_->get_enumerations(enumeration_names);

  tiledb::sm::serialization::serialize_load_enumerations_response(
      enumerations,
      static_cast<tiledb::sm::SerializationType>(serialization_type),
      response->buffer());

  return TILEDB_OK;
}

capi_return_t tiledb_handle_query_plan_request(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_serialization_type_t serialization_type,
    const tiledb_buffer_t* request,
    tiledb_buffer_t* response) {
  if (sanity_check(ctx, array) == TILEDB_ERR) {
    throw std::invalid_argument("Array parameter must be valid.");
  }

  api::ensure_buffer_is_valid(request);
  api::ensure_buffer_is_valid(response);

  tiledb::sm::Query query(ctx->storage_manager(), array->array_);
  tiledb::sm::serialization::deserialize_query_plan_request(
      static_cast<tiledb::sm::SerializationType>(serialization_type),
      request->buffer(),
      ctx->resources().compute_tp(),
      query);
  sm::QueryPlan plan(query);

  tiledb::sm::serialization::serialize_query_plan_response(
      plan,
      static_cast<tiledb::sm::SerializationType>(serialization_type),
      response->buffer());

  return TILEDB_OK;
}

capi_return_t tiledb_handle_consolidation_plan_request(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_serialization_type_t serialization_type,
    const tiledb_buffer_t* request,
    tiledb_buffer_t* response) {
  if (sanity_check(ctx, array) == TILEDB_ERR) {
    throw std::invalid_argument("Array paramter must be valid.");
  }

  api::ensure_buffer_is_valid(request);
  api::ensure_buffer_is_valid(response);

  // Error if array is not open
  if (!array->array_->is_open()) {
    throw std::logic_error(
        "Cannot get consolidation plan. Input array is not open");
  }

  auto fragment_size =
      tiledb::sm::serialization::deserialize_consolidation_plan_request(
          static_cast<tiledb::sm::SerializationType>(serialization_type),
          request->buffer());
  sm::ConsolidationPlan plan(array->array_, fragment_size);

  tiledb::sm::serialization::serialize_consolidation_plan_response(
      plan,
      static_cast<tiledb::sm::SerializationType>(serialization_type),
      response->buffer());

  return TILEDB_OK;
}

/* ****************************** */
/*            C++ API             */
/* ****************************** */
namespace impl {
int32_t tiledb_query_submit_async_func(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    void* callback_func,
    void* callback_data) {
  if (sanity_check(ctx, query) == TILEDB_ERR || callback_func == nullptr)
    return TILEDB_ERR;

  std::function<void(void*)> callback =
      *reinterpret_cast<std::function<void(void*)>*>(callback_func);

  throw_if_not_ok(query->query_->submit_async(callback, callback_data));

  return TILEDB_OK;
}
}  // namespace impl

/* ****************************** */
/*          FRAGMENT INFO         */
/* ****************************** */

int32_t tiledb_fragment_info_alloc(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_fragment_info_t** fragment_info) {
  // Create fragment info struct
  *fragment_info = new (std::nothrow) tiledb_fragment_info_t;
  if (*fragment_info == nullptr) {
    auto st = Status_Error(
        "Failed to create TileDB fragment info object; Memory allocation "
        "error");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Check array URI
  auto uri = tiledb::sm::URI(array_uri);
  if (uri.is_invalid()) {
    auto st = Status_Error(
        "Failed to create TileDB fragment info object; Invalid URI");
    delete *fragment_info;
    *fragment_info = nullptr;
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  // Allocate a fragment info object
  (*fragment_info)->fragment_info_ =
      new (std::nothrow) tiledb::sm::FragmentInfo(uri, ctx->resources());
  if ((*fragment_info)->fragment_info_ == nullptr) {
    delete *fragment_info;
    *fragment_info = nullptr;
    auto st = Status_Error(
        "Failed to create TileDB fragment info object; Memory allocation "
        "error");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Success
  return TILEDB_OK;
}

void tiledb_fragment_info_free(tiledb_fragment_info_t** fragment_info) {
  if (fragment_info != nullptr && *fragment_info != nullptr) {
    delete (*fragment_info)->fragment_info_;
    delete *fragment_info;
    *fragment_info = nullptr;
  }
}

int32_t tiledb_fragment_info_set_config(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    tiledb_config_t* config) {
  // Sanity check
  if (sanity_check(ctx, fragment_info) == TILEDB_ERR)
    return TILEDB_ERR;
  api::ensure_config_is_valid(config);

  fragment_info->fragment_info_->set_config(config->config());

  return TILEDB_OK;
}

int32_t tiledb_fragment_info_get_config(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    tiledb_config_t** config) {
  // Sanity check
  if (sanity_check(ctx, fragment_info) == TILEDB_ERR)
    return TILEDB_ERR;

  api::ensure_output_pointer_is_valid(config);
  *config = tiledb_config_handle_t::make_handle(
      fragment_info->fragment_info_->config());
  return TILEDB_OK;
}

int32_t tiledb_fragment_info_load(
    tiledb_ctx_t* ctx, tiledb_fragment_info_t* fragment_info) {
  if (sanity_check(ctx, fragment_info) == TILEDB_ERR)
    return TILEDB_ERR;

  // Load fragment info
  throw_if_not_ok(fragment_info->fragment_info_->load());

  return TILEDB_OK;
}

int32_t tiledb_fragment_info_get_fragment_name(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    const char** name) {
  if (sanity_check(ctx, fragment_info) == TILEDB_ERR)
    return TILEDB_ERR;

  LOG_WARN(
      "tiledb_fragment_info_get_fragment_name is deprecated. Please use "
      "tiledb_fragment_info_get_fragment_name_v2 instead.");
  // This will leak the string but as a temporary solution until
  // this deprecated function is removed.
  *name = (new std::string(fragment_info->fragment_info_->fragment_name(fid)))
              ->c_str();

  return TILEDB_OK;
}

int32_t tiledb_fragment_info_get_fragment_name_v2(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    tiledb_string_t** name) {
  if (sanity_check(ctx, fragment_info) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  if (name == nullptr) {
    throw std::invalid_argument("Name cannot be null.");
  }

  *name = tiledb_string_handle_t::make_handle(
      fragment_info->fragment_info_->fragment_name(fid));

  return TILEDB_OK;
}

int32_t tiledb_fragment_info_get_fragment_num(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t* fragment_num) {
  if (sanity_check(ctx, fragment_info) == TILEDB_ERR)
    return TILEDB_ERR;

  *fragment_num = fragment_info->fragment_info_->fragment_num();

  return TILEDB_OK;
}

int32_t tiledb_fragment_info_get_fragment_uri(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    const char** uri) {
  if (sanity_check(ctx, fragment_info) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(fragment_info->fragment_info_->get_fragment_uri(fid, uri));

  return TILEDB_OK;
}

int32_t tiledb_fragment_info_get_fragment_size(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint64_t* size) {
  if (sanity_check(ctx, fragment_info) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(fragment_info->fragment_info_->get_fragment_size(fid, size));

  return TILEDB_OK;
}

int32_t tiledb_fragment_info_get_dense(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    int32_t* dense) {
  if (sanity_check(ctx, fragment_info) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(fragment_info->fragment_info_->get_dense(fid, dense));

  return TILEDB_OK;
}

int32_t tiledb_fragment_info_get_sparse(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    int32_t* sparse) {
  if (sanity_check(ctx, fragment_info) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(fragment_info->fragment_info_->get_sparse(fid, sparse));

  return TILEDB_OK;
}

int32_t tiledb_fragment_info_get_timestamp_range(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint64_t* start,
    uint64_t* end) {
  if (sanity_check(ctx, fragment_info) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(
      fragment_info->fragment_info_->get_timestamp_range(fid, start, end));

  return TILEDB_OK;
}

int32_t tiledb_fragment_info_get_non_empty_domain_from_index(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t did,
    void* domain) {
  if (sanity_check(ctx, fragment_info) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(
      fragment_info->fragment_info_->get_non_empty_domain(fid, did, domain));

  return TILEDB_OK;
}

int32_t tiledb_fragment_info_get_non_empty_domain_from_name(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    const char* dim_name,
    void* domain) {
  if (sanity_check(ctx, fragment_info) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(fragment_info->fragment_info_->get_non_empty_domain(
      fid, dim_name, domain));

  return TILEDB_OK;
}

int32_t tiledb_fragment_info_get_non_empty_domain_var_size_from_index(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t did,
    uint64_t* start_size,
    uint64_t* end_size) {
  if (sanity_check(ctx, fragment_info) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(fragment_info->fragment_info_->get_non_empty_domain_var_size(
      fid, did, start_size, end_size));

  return TILEDB_OK;
}

int32_t tiledb_fragment_info_get_non_empty_domain_var_size_from_name(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    const char* dim_name,
    uint64_t* start_size,
    uint64_t* end_size) {
  if (sanity_check(ctx, fragment_info) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(fragment_info->fragment_info_->get_non_empty_domain_var_size(
      fid, dim_name, start_size, end_size));

  return TILEDB_OK;
}

int32_t tiledb_fragment_info_get_non_empty_domain_var_from_index(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t did,
    void* start,
    void* end) {
  if (sanity_check(ctx, fragment_info) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(fragment_info->fragment_info_->get_non_empty_domain_var(
      fid, did, start, end));

  return TILEDB_OK;
}

int32_t tiledb_fragment_info_get_non_empty_domain_var_from_name(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    const char* dim_name,
    void* start,
    void* end) {
  if (sanity_check(ctx, fragment_info) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(fragment_info->fragment_info_->get_non_empty_domain_var(
      fid, dim_name, start, end));

  return TILEDB_OK;
}

int32_t tiledb_fragment_info_get_mbr_num(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint64_t* mbr_num) {
  if (sanity_check(ctx, fragment_info) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(fragment_info->fragment_info_->get_mbr_num(fid, mbr_num));

  return TILEDB_OK;
}

int32_t tiledb_fragment_info_get_mbr_from_index(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t mid,
    uint32_t did,
    void* mbr) {
  if (sanity_check(ctx, fragment_info) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(fragment_info->fragment_info_->get_mbr(fid, mid, did, mbr));

  return TILEDB_OK;
}

int32_t tiledb_fragment_info_get_mbr_from_name(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t mid,
    const char* dim_name,
    void* mbr) {
  if (sanity_check(ctx, fragment_info) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(
      fragment_info->fragment_info_->get_mbr(fid, mid, dim_name, mbr));

  return TILEDB_OK;
}

int32_t tiledb_fragment_info_get_mbr_var_size_from_index(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t mid,
    uint32_t did,
    uint64_t* start_size,
    uint64_t* end_size) {
  if (sanity_check(ctx, fragment_info) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(fragment_info->fragment_info_->get_mbr_var_size(
      fid, mid, did, start_size, end_size));

  return TILEDB_OK;
}

int32_t tiledb_fragment_info_get_mbr_var_size_from_name(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t mid,
    const char* dim_name,
    uint64_t* start_size,
    uint64_t* end_size) {
  if (sanity_check(ctx, fragment_info) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(fragment_info->fragment_info_->get_mbr_var_size(
      fid, mid, dim_name, start_size, end_size));

  return TILEDB_OK;
}

int32_t tiledb_fragment_info_get_mbr_var_from_index(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t mid,
    uint32_t did,
    void* start,
    void* end) {
  if (sanity_check(ctx, fragment_info) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(
      fragment_info->fragment_info_->get_mbr_var(fid, mid, did, start, end));

  return TILEDB_OK;
}

int32_t tiledb_fragment_info_get_mbr_var_from_name(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t mid,
    const char* dim_name,
    void* start,
    void* end) {
  if (sanity_check(ctx, fragment_info) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(fragment_info->fragment_info_->get_mbr_var(
      fid, mid, dim_name, start, end));

  return TILEDB_OK;
}

int32_t tiledb_fragment_info_get_cell_num(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint64_t* cell_num) {
  if (sanity_check(ctx, fragment_info) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(fragment_info->fragment_info_->get_cell_num(fid, cell_num));

  return TILEDB_OK;
}

int32_t tiledb_fragment_info_get_total_cell_num(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint64_t* cell_num) {
  if (sanity_check(ctx, fragment_info) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(fragment_info->fragment_info_->get_total_cell_num(cell_num));

  return TILEDB_OK;
}

int32_t tiledb_fragment_info_get_version(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t* version) {
  if (sanity_check(ctx, fragment_info) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(fragment_info->fragment_info_->get_version(fid, version));

  return TILEDB_OK;
}

int32_t tiledb_fragment_info_has_consolidated_metadata(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    int32_t* has) {
  if (sanity_check(ctx, fragment_info) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(
      fragment_info->fragment_info_->has_consolidated_metadata(fid, has));

  return TILEDB_OK;
}

int32_t tiledb_fragment_info_get_unconsolidated_metadata_num(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t* unconsolidated) {
  if (sanity_check(ctx, fragment_info) == TILEDB_ERR)
    return TILEDB_ERR;

  *unconsolidated =
      fragment_info->fragment_info_->unconsolidated_metadata_num();

  return TILEDB_OK;
}

int32_t tiledb_fragment_info_get_to_vacuum_num(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t* to_vacuum_num) {
  if (sanity_check(ctx, fragment_info) == TILEDB_ERR)
    return TILEDB_ERR;

  *to_vacuum_num = fragment_info->fragment_info_->to_vacuum_num();

  return TILEDB_OK;
}

int32_t tiledb_fragment_info_get_to_vacuum_uri(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    const char** uri) {
  if (sanity_check(ctx, fragment_info) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(fragment_info->fragment_info_->get_to_vacuum_uri(fid, uri));

  return TILEDB_OK;
}

int32_t tiledb_fragment_info_get_array_schema(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    tiledb_array_schema_t** array_schema) {
  if (sanity_check(ctx, fragment_info) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create array schema
  *array_schema = new (std::nothrow) tiledb_array_schema_t;
  if (*array_schema == nullptr) {
    auto st = Status_Error("Failed to allocate TileDB array schema object");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  auto&& array_schema_get =
      fragment_info->fragment_info_->get_array_schema(fid);
  (*array_schema)->array_schema_ = array_schema_get;

  return TILEDB_OK;
}

int32_t tiledb_fragment_info_get_array_schema_name(
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    const char** schema_name) {
  if (sanity_check(ctx, fragment_info) == TILEDB_ERR)
    return TILEDB_ERR;

  throw_if_not_ok(
      fragment_info->fragment_info_->get_array_schema_name(fid, schema_name));

  assert(schema_name != nullptr);

  return TILEDB_OK;
}

int32_t tiledb_fragment_info_dump(
    tiledb_ctx_t* ctx, const tiledb_fragment_info_t* fragment_info, FILE* out) {
  if (sanity_check(ctx, fragment_info) == TILEDB_ERR)
    return TILEDB_ERR;
  fragment_info->fragment_info_->dump(out);
  return TILEDB_OK;
}

/* ********************************* */
/*          EXPERIMENTAL APIs        */
/* ********************************* */

int32_t tiledb_query_get_status_details(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    tiledb_query_status_details_t* status) {
  // Sanity check
  if (sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  // Currently only one detailed reason. Retrieve it and set to user struct.
  tiledb_query_status_details_reason_t incomplete_reason =
      (tiledb_query_status_details_reason_t)
          query->query_->status_incomplete_reason();
  status->incomplete_reason = incomplete_reason;

  return TILEDB_OK;
}

int32_t tiledb_consolidation_plan_create_with_mbr(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    uint64_t fragment_size,
    tiledb_consolidation_plan_t** consolidation_plan) {
  // Sanity check
  if (sanity_check(ctx, array) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  // Create consolidation plan struct
  *consolidation_plan = new (std::nothrow) tiledb_consolidation_plan_t;
  if (*consolidation_plan == nullptr) {
    auto st = Status_Error(
        "Failed to create TileDB consolidation plan object; Memory allocation "
        "error");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Allocate a consolidation plan object
  try {
    (*consolidation_plan)->consolidation_plan_ =
        make_shared<tiledb::sm::ConsolidationPlan>(
            HERE(), array->array_, fragment_size);
  } catch (std::bad_alloc&) {
    auto st = Status_Error(
        "Failed to create TileDB consolidation plan object; Memory allocation "
        "error");
    delete *consolidation_plan;
    *consolidation_plan = nullptr;
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  return TILEDB_OK;
}

void tiledb_consolidation_plan_free(
    tiledb_consolidation_plan_t** consolidation_plan) {
  if (consolidation_plan != nullptr && *consolidation_plan != nullptr) {
    delete *consolidation_plan;
    *consolidation_plan = nullptr;
  }
}

int32_t tiledb_consolidation_plan_get_num_nodes(
    tiledb_ctx_t* ctx,
    tiledb_consolidation_plan_t* consolidation_plan,
    uint64_t* num_nodes) {
  if (sanity_check(ctx, consolidation_plan) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  *num_nodes = consolidation_plan->consolidation_plan_->get_num_nodes();
  return TILEDB_OK;
}

int32_t tiledb_consolidation_plan_get_num_fragments(
    tiledb_ctx_t* ctx,
    tiledb_consolidation_plan_t* consolidation_plan,
    uint64_t node_index,
    uint64_t* num_fragments) {
  if (sanity_check(ctx, consolidation_plan) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  try {
    *num_fragments =
        consolidation_plan->consolidation_plan_->get_num_fragments(node_index);
  } catch (StatusException& e) {
    auto st = Status_Error(e.what());
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  return TILEDB_OK;
}

int32_t tiledb_consolidation_plan_get_fragment_uri(
    tiledb_ctx_t* ctx,
    tiledb_consolidation_plan_t* consolidation_plan,
    uint64_t node_index,
    uint64_t fragment_index,
    const char** uri) {
  if (sanity_check(ctx, consolidation_plan) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  try {
    *uri = consolidation_plan->consolidation_plan_->get_fragment_uri(
        node_index, fragment_index);
  } catch (StatusException& e) {
    auto st = Status_Error(e.what());
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  return TILEDB_OK;
}

int32_t tiledb_consolidation_plan_dump_json_str(
    tiledb_ctx_t* ctx,
    const tiledb_consolidation_plan_t* consolidation_plan,
    char** out) {
  if (out == nullptr) {
    return TILEDB_ERR;
  }

  if (sanity_check(ctx, consolidation_plan) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  std::string str = consolidation_plan->consolidation_plan_->dump();

  *out = static_cast<char*>(std::malloc(str.size() + 1));
  if (*out == nullptr) {
    return TILEDB_ERR;
  }

  std::memcpy(*out, str.data(), str.size());
  (*out)[str.size()] = '\0';

  return TILEDB_OK;
}

int32_t tiledb_consolidation_plan_free_json_str(char** out) {
  if (out != nullptr) {
    std::free(*out);
    *out = nullptr;
  }
  return TILEDB_OK;
}

}  // namespace tiledb::api

/* ****************************** */
/*  C API Interface Functions     */
/* ****************************** */
/*
 * Each C API interface function below forwards its arguments to a transformed
 * implementation function of the same name defined in the `tiledb::api`
 * namespace above.
 *
 * Note: `std::forward` is not used here because it's not necessary. The C API
 * uses C linkage, and none of the types used in the signatures of the C API
 * function change with `std::forward`.
 */

using tiledb::api::api_entry_context;
using tiledb::api::api_entry_plain;
using tiledb::api::api_entry_void;
template <auto f>
constexpr auto api_entry = tiledb::api::api_entry_with_context<f>;

/* ****************************** */
/*       ENUMS TO/FROM STR        */
/* ****************************** */
CAPI_INTERFACE(
    array_type_to_str, tiledb_array_type_t array_type, const char** str) {
  return api_entry_plain<tiledb::api::tiledb_array_type_to_str>(
      array_type, str);
}

CAPI_INTERFACE(
    array_type_from_str, const char* str, tiledb_array_type_t* array_type) {
  return api_entry_plain<tiledb::api::tiledb_array_type_from_str>(
      str, array_type);
}

CAPI_INTERFACE(layout_to_str, tiledb_layout_t layout, const char** str) {
  return api_entry_plain<tiledb::api::tiledb_layout_to_str>(layout, str);
}

CAPI_INTERFACE(layout_from_str, const char* str, tiledb_layout_t* layout) {
  return api_entry_plain<tiledb::api::tiledb_layout_from_str>(str, layout);
}

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
    query_status_to_str, tiledb_query_status_t query_status, const char** str) {
  return api_entry_plain<tiledb::api::tiledb_query_status_to_str>(
      query_status, str);
}

CAPI_INTERFACE(
    query_status_from_str,
    const char* str,
    tiledb_query_status_t* query_status) {
  return api_entry_plain<tiledb::api::tiledb_query_status_from_str>(
      str, query_status);
}

CAPI_INTERFACE(
    serialization_type_to_str,
    tiledb_serialization_type_t serialization_type,
    const char** str) {
  return api_entry_plain<tiledb::api::tiledb_serialization_type_to_str>(
      serialization_type, str);
}

CAPI_INTERFACE(
    serialization_type_from_str,
    const char* str,
    tiledb_serialization_type_t* serialization_type) {
  return api_entry_plain<tiledb::api::tiledb_serialization_type_from_str>(
      str, serialization_type);
}

/* ****************************** */
/*            CONSTANTS           */
/* ****************************** */

uint32_t tiledb_var_num() noexcept {
  return tiledb::sm::constants::var_num;
}

uint32_t tiledb_max_path() noexcept {
  return tiledb::sm::constants::path_max_len;
}

uint64_t tiledb_offset_size() noexcept {
  return tiledb::sm::constants::cell_var_offset_size;
}

uint64_t tiledb_timestamp_now_ms() noexcept {
  /*
   * The existing implementation function is not marked `nothrow`. The
   * signature of this function cannot signal an error. Hence we normalize any
   * error by making the return value zero.
   */
  try {
    return tiledb::sm::utils::time::timestamp_now_ms();
  } catch (...) {
    LOG_ERROR("Error in retrieving current time");
    return 0;
  }
}

const char* tiledb_timestamps() noexcept {
  return tiledb::sm::constants::timestamps.c_str();
}

/* ****************************** */
/*            VERSION             */
/* ****************************** */

void tiledb_version(int32_t* major, int32_t* minor, int32_t* rev) noexcept {
  *major = tiledb::sm::constants::library_version[0];
  *minor = tiledb::sm::constants::library_version[1];
  *rev = tiledb::sm::constants::library_version[2];
}

/* ********************************* */
/*             LOGGING               */
/* ********************************* */

CAPI_INTERFACE(log_warn, tiledb_ctx_t* ctx, const char* message) {
  return api_entry<tiledb::api::tiledb_log_warn>(ctx, message);
}

/* ********************************* */
/*              AS BUILT             */
/* ********************************* */
CAPI_INTERFACE(as_built_dump, tiledb_string_t** out) {
  return api_entry_plain<tiledb::api::tiledb_as_built_dump>(out);
}

/* ****************************** */
/*           ARRAY SCHEMA         */
/* ****************************** */

CAPI_INTERFACE(
    array_schema_alloc,
    tiledb_ctx_t* ctx,
    tiledb_array_type_t array_type,
    tiledb_array_schema_t** array_schema) {
  return api_entry<tiledb::api::tiledb_array_schema_alloc>(
      ctx, array_type, array_schema);
}

CAPI_INTERFACE_VOID(array_schema_free, tiledb_array_schema_t** array_schema) {
  return api_entry_void<tiledb::api::tiledb_array_schema_free>(array_schema);
}

CAPI_INTERFACE(
    array_schema_add_attribute,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_attribute_t* attr) {
  return api_entry<tiledb::api::tiledb_array_schema_add_attribute>(
      ctx, array_schema, attr);
}

CAPI_INTERFACE(
    array_schema_set_allows_dups,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    int allows_dups) {
  return api_entry<tiledb::api::tiledb_array_schema_set_allows_dups>(
      ctx, array_schema, allows_dups);
}

CAPI_INTERFACE(
    array_schema_get_allows_dups,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    int* allows_dups) {
  return api_entry<tiledb::api::tiledb_array_schema_get_allows_dups>(
      ctx, array_schema, allows_dups);
}

CAPI_INTERFACE(
    array_schema_get_version,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    uint32_t* version) {
  return api_entry<tiledb::api::tiledb_array_schema_get_version>(
      ctx, array_schema, version);
}

CAPI_INTERFACE(
    array_schema_set_domain,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_domain_t* domain) {
  return api_entry<tiledb::api::tiledb_array_schema_set_domain>(
      ctx, array_schema, domain);
}

CAPI_INTERFACE(
    array_schema_set_capacity,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    uint64_t capacity) {
  return api_entry<tiledb::api::tiledb_array_schema_set_capacity>(
      ctx, array_schema, capacity);
}

CAPI_INTERFACE(
    array_schema_set_cell_order,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_layout_t cell_order) {
  return api_entry<tiledb::api::tiledb_array_schema_set_cell_order>(
      ctx, array_schema, cell_order);
}

CAPI_INTERFACE(
    array_schema_set_tile_order,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_layout_t tile_order) {
  return api_entry<tiledb::api::tiledb_array_schema_set_tile_order>(
      ctx, array_schema, tile_order);
}

CAPI_INTERFACE(
    array_schema_timestamp_range,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    uint64_t* lo,
    uint64_t* hi) {
  return api_entry<tiledb::api::tiledb_array_schema_timestamp_range>(
      ctx, array_schema, lo, hi);
}

CAPI_INTERFACE(
    array_schema_add_enumeration,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_enumeration_t* enumeration) {
  return api_entry<tiledb::api::tiledb_array_schema_add_enumeration>(
      ctx, array_schema, enumeration);
}

CAPI_INTERFACE(
    array_schema_set_coords_filter_list,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_filter_list_t* filter_list) {
  return api_entry<tiledb::api::tiledb_array_schema_set_coords_filter_list>(
      ctx, array_schema, filter_list);
}

CAPI_INTERFACE(
    array_schema_set_offsets_filter_list,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_filter_list_t* filter_list) {
  return api_entry<tiledb::api::tiledb_array_schema_set_offsets_filter_list>(
      ctx, array_schema, filter_list);
}

CAPI_INTERFACE(
    array_schema_set_validity_filter_list,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_filter_list_t* filter_list) {
  return api_entry<tiledb::api::tiledb_array_schema_set_validity_filter_list>(
      ctx, array_schema, filter_list);
}

CAPI_INTERFACE(
    array_schema_check,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema) {
  return api_entry<tiledb::api::tiledb_array_schema_check>(ctx, array_schema);
}

CAPI_INTERFACE(
    array_schema_load,
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_array_schema_t** array_schema) {
  return api_entry<tiledb::api::tiledb_array_schema_load>(
      ctx, array_uri, array_schema);
}

CAPI_INTERFACE(
    array_schema_load_with_key,
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_encryption_type_t encryption_type,
    const void* encryption_key,
    uint32_t key_length,
    tiledb_array_schema_t** array_schema) {
  return api_entry<tiledb::api::tiledb_array_schema_load_with_key>(
      ctx,
      array_uri,
      encryption_type,
      encryption_key,
      key_length,
      array_schema);
}

CAPI_INTERFACE(
    array_schema_get_array_type,
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    tiledb_array_type_t* array_type) {
  return api_entry<tiledb::api::tiledb_array_schema_get_array_type>(
      ctx, array_schema, array_type);
}

CAPI_INTERFACE(
    array_schema_get_capacity,
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    uint64_t* capacity) {
  return api_entry<tiledb::api::tiledb_array_schema_get_capacity>(
      ctx, array_schema, capacity);
}

CAPI_INTERFACE(
    array_schema_get_cell_order,
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    tiledb_layout_t* cell_order) {
  return api_entry<tiledb::api::tiledb_array_schema_get_cell_order>(
      ctx, array_schema, cell_order);
}

CAPI_INTERFACE(
    array_schema_get_coords_filter_list,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_filter_list_t** filter_list) {
  return api_entry<tiledb::api::tiledb_array_schema_get_coords_filter_list>(
      ctx, array_schema, filter_list);
}

CAPI_INTERFACE(
    array_schema_get_offsets_filter_list,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_filter_list_t** filter_list) {
  return api_entry<tiledb::api::tiledb_array_schema_get_offsets_filter_list>(
      ctx, array_schema, filter_list);
}

CAPI_INTERFACE(
    array_schema_get_validity_filter_list,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_filter_list_t** filter_list) {
  return api_entry<tiledb::api::tiledb_array_schema_get_validity_filter_list>(
      ctx, array_schema, filter_list);
}

CAPI_INTERFACE(
    array_schema_get_domain,
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    tiledb_domain_t** domain) {
  return api_entry<tiledb::api::tiledb_array_schema_get_domain>(
      ctx, array_schema, domain);
}

CAPI_INTERFACE(
    array_schema_get_tile_order,
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    tiledb_layout_t* tile_order) {
  return api_entry<tiledb::api::tiledb_array_schema_get_tile_order>(
      ctx, array_schema, tile_order);
}

CAPI_INTERFACE(
    array_schema_get_attribute_num,
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    uint32_t* attribute_num) {
  return api_entry<tiledb::api::tiledb_array_schema_get_attribute_num>(
      ctx, array_schema, attribute_num);
}

CAPI_INTERFACE(
    array_schema_dump,
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    FILE* out) {
  return api_entry<tiledb::api::tiledb_array_schema_dump>(
      ctx, array_schema, out);
}

CAPI_INTERFACE(
    array_schema_get_attribute_from_index,
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    uint32_t index,
    tiledb_attribute_t** attr) {
  return api_entry<tiledb::api::tiledb_array_schema_get_attribute_from_index>(
      ctx, array_schema, index, attr);
}

CAPI_INTERFACE(
    array_schema_get_attribute_from_name,
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    const char* name,
    tiledb_attribute_t** attr) {
  return api_entry<tiledb::api::tiledb_array_schema_get_attribute_from_name>(
      ctx, array_schema, name, attr);
}

CAPI_INTERFACE(
    array_schema_has_attribute,
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    const char* name,
    int32_t* has_attr) {
  return api_entry<tiledb::api::tiledb_array_schema_has_attribute>(
      ctx, array_schema, name, has_attr);
}

/* ********************************* */
/*            SCHEMA EVOLUTION       */
/* ********************************* */

CAPI_INTERFACE(
    array_schema_evolution_alloc,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t** array_schema_evolution) {
  return api_entry<tiledb::api::tiledb_array_schema_evolution_alloc>(
      ctx, array_schema_evolution);
}

CAPI_INTERFACE_VOID(
    array_schema_evolution_free,
    tiledb_array_schema_evolution_t** array_schema_evolution) {
  return api_entry_void<tiledb::api::tiledb_array_schema_evolution_free>(
      array_schema_evolution);
}

CAPI_INTERFACE(
    array_schema_evolution_add_attribute,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t* array_schema_evolution,
    tiledb_attribute_t* attr) {
  return api_entry<tiledb::api::tiledb_array_schema_evolution_add_attribute>(
      ctx, array_schema_evolution, attr);
}

CAPI_INTERFACE(
    array_schema_evolution_drop_attribute,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t* array_schema_evolution,
    const char* attribute_name) {
  return api_entry<tiledb::api::tiledb_array_schema_evolution_drop_attribute>(
      ctx, array_schema_evolution, attribute_name);
}

CAPI_INTERFACE(
    array_schema_evolution_add_enumeration,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t* array_schema_evolution,
    tiledb_enumeration_t* enmr) {
  return api_entry<tiledb::api::tiledb_array_schema_evolution_add_enumeration>(
      ctx, array_schema_evolution, enmr);
}

CAPI_INTERFACE(
    array_schema_evolution_extend_enumeration,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t* array_schema_evolution,
    tiledb_enumeration_t* enmr) {
  return api_entry<
      tiledb::api::tiledb_array_schema_evolution_extend_enumeration>(
      ctx, array_schema_evolution, enmr);
}

CAPI_INTERFACE(
    array_schema_evolution_drop_enumeration,
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t* array_schema_evolution,
    const char* enumeration_name) {
  return api_entry<tiledb::api::tiledb_array_schema_evolution_drop_enumeration>(
      ctx, array_schema_evolution, enumeration_name);
}

TILEDB_EXPORT int32_t tiledb_array_schema_evolution_set_timestamp_range(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_evolution_t* array_schema_evolution,
    uint64_t lo,
    uint64_t hi) noexcept {
  return api_entry<
      tiledb::api::tiledb_array_schema_evolution_set_timestamp_range>(
      ctx, array_schema_evolution, lo, hi);
}

/* ****************************** */
/*              QUERY             */
/* ****************************** */

CAPI_INTERFACE(
    query_alloc,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_query_type_t query_type,
    tiledb_query_t** query) {
  return api_entry<tiledb::api::tiledb_query_alloc>(
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
    query_set_subarray,
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const void* subarray_vals) {
  return api_entry<tiledb::api::tiledb_query_set_subarray>(
      ctx, query, subarray_vals);
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
    uint64_t* buffer_offsets,
    uint64_t* buffer_offsets_size) {
  return api_entry<tiledb::api::tiledb_query_set_offsets_buffer>(
      ctx, query, name, buffer_offsets, buffer_offsets_size);
}

CAPI_INTERFACE(
    query_set_validity_buffer,
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* name,
    uint8_t* buffer_validity,
    uint64_t* buffer_validity_size) {
  return api_entry<tiledb::api::tiledb_query_set_validity_buffer>(
      ctx, query, name, buffer_validity, buffer_validity_size);
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
    tiledb_ctx_t* const ctx,
    tiledb_query_t* const query,
    const tiledb_query_condition_t* const cond) {
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
    query_submit_async,
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    void (*callback)(void*),
    void* callback_data) {
  return api_entry<tiledb::api::tiledb_query_submit_async>(
      ctx, query, callback, callback_data);
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
    query_add_range,
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    uint32_t dim_idx,
    const void* start,
    const void* end,
    const void* stride) {
  return api_entry<tiledb::api::tiledb_query_add_range>(
      ctx, query, dim_idx, start, end, stride);
}

CAPI_INTERFACE(
    query_add_point_ranges,
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    uint32_t dim_idx,
    const void* start,
    uint64_t count) {
  return api_entry<tiledb::api::tiledb_query_add_point_ranges>(
      ctx, query, dim_idx, start, count);
}

CAPI_INTERFACE(
    query_add_range_by_name,
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* dim_name,
    const void* start,
    const void* end,
    const void* stride) {
  return api_entry<tiledb::api::tiledb_query_add_range_by_name>(
      ctx, query, dim_name, start, end, stride);
}

CAPI_INTERFACE(
    query_add_range_var,
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    uint32_t dim_idx,
    const void* start,
    uint64_t start_size,
    const void* end,
    uint64_t end_size) {
  return api_entry<tiledb::api::tiledb_query_add_range_var>(
      ctx, query, dim_idx, start, start_size, end, end_size);
}

CAPI_INTERFACE(
    query_add_range_var_by_name,
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* dim_name,
    const void* start,
    uint64_t start_size,
    const void* end,
    uint64_t end_size) {
  return api_entry<tiledb::api::tiledb_query_add_range_var_by_name>(
      ctx, query, dim_name, start, start_size, end, end_size);
}

CAPI_INTERFACE(
    query_get_range_num,
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    uint32_t dim_idx,
    uint64_t* range_num) {
  return api_entry<tiledb::api::tiledb_query_get_range_num>(
      ctx, query, dim_idx, range_num);
}

CAPI_INTERFACE(
    query_get_range_num_from_name,
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    const char* dim_name,
    uint64_t* range_num) {
  return api_entry<tiledb::api::tiledb_query_get_range_num_from_name>(
      ctx, query, dim_name, range_num);
}

CAPI_INTERFACE(
    query_get_range,
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    uint32_t dim_idx,
    uint64_t range_idx,
    const void** start,
    const void** end,
    const void** stride) {
  return api_entry<tiledb::api::tiledb_query_get_range>(
      ctx, query, dim_idx, range_idx, start, end, stride);
}

CAPI_INTERFACE(
    query_get_range_from_name,
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    const char* dim_name,
    uint64_t range_idx,
    const void** start,
    const void** end,
    const void** stride) {
  return api_entry<tiledb::api::tiledb_query_get_range_from_name>(
      ctx, query, dim_name, range_idx, start, end, stride);
}

CAPI_INTERFACE(
    query_get_range_var_size,
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    uint32_t dim_idx,
    uint64_t range_idx,
    uint64_t* start_size,
    uint64_t* end_size) {
  return api_entry<tiledb::api::tiledb_query_get_range_var_size>(
      ctx, query, dim_idx, range_idx, start_size, end_size);
}

CAPI_INTERFACE(
    query_get_range_var_size_from_name,
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    const char* dim_name,
    uint64_t range_idx,
    uint64_t* start_size,
    uint64_t* end_size) {
  return api_entry<tiledb::api::tiledb_query_get_range_var_size_from_name>(
      ctx, query, dim_name, range_idx, start_size, end_size);
}

CAPI_INTERFACE(
    query_get_range_var,
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    uint32_t dim_idx,
    uint64_t range_idx,
    void* start,
    void* end) {
  return api_entry<tiledb::api::tiledb_query_get_range_var>(
      ctx, query, dim_idx, range_idx, start, end);
}

CAPI_INTERFACE(
    query_get_range_var_from_name,
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    const char* dim_name,
    uint64_t range_idx,
    void* start,
    void* end) {
  return api_entry<tiledb::api::tiledb_query_get_range_var_from_name>(
      ctx, query, dim_name, range_idx, start, end);
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

/* ****************************** */
/*         SUBARRAY               */
/* ****************************** */

CAPI_INTERFACE(
    subarray_alloc,
    tiledb_ctx_t* ctx,
    const tiledb_array_t* array,
    tiledb_subarray_t** subarray) {
  return api_entry<tiledb::api::tiledb_subarray_alloc>(ctx, array, subarray);
}

CAPI_INTERFACE(
    subarray_set_config,
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray,
    tiledb_config_t* config) {
  return api_entry<tiledb::api::tiledb_subarray_set_config>(
      ctx, subarray, config);
}

CAPI_INTERFACE_VOID(subarray_free, tiledb_subarray_t** subarray) {
  return api_entry_void<tiledb::api::tiledb_subarray_free>(subarray);
}

CAPI_INTERFACE(
    subarray_set_coalesce_ranges,
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray,
    int coalesce_ranges) {
  return api_entry<tiledb::api::tiledb_subarray_set_coalesce_ranges>(
      ctx, subarray, coalesce_ranges);
}

CAPI_INTERFACE(
    subarray_set_subarray,
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray_obj,
    const void* subarray_vals) {
  return api_entry<tiledb::api::tiledb_subarray_set_subarray>(
      ctx, subarray_obj, subarray_vals);
}

CAPI_INTERFACE(
    subarray_add_range,
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray,
    uint32_t dim_idx,
    const void* start,
    const void* end,
    const void* stride) {
  return api_entry<tiledb::api::tiledb_subarray_add_range>(
      ctx, subarray, dim_idx, start, end, stride);
}

CAPI_INTERFACE(
    subarray_add_point_ranges,
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray,
    uint32_t dim_idx,
    const void* start,
    uint64_t count) {
  return api_entry<tiledb::api::tiledb_subarray_add_point_ranges>(
      ctx, subarray, dim_idx, start, count);
}

CAPI_INTERFACE(
    subarray_add_range_by_name,
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray,
    const char* dim_name,
    const void* start,
    const void* end,
    const void* stride) {
  return api_entry<tiledb::api::tiledb_subarray_add_range_by_name>(
      ctx, subarray, dim_name, start, end, stride);
}

CAPI_INTERFACE(
    subarray_add_range_var,
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray,
    uint32_t dim_idx,
    const void* start,
    uint64_t start_size,
    const void* end,
    uint64_t end_size) {
  return api_entry<tiledb::api::tiledb_subarray_add_range_var>(
      ctx, subarray, dim_idx, start, start_size, end, end_size);
}

CAPI_INTERFACE(
    subarray_add_range_var_by_name,
    tiledb_ctx_t* ctx,
    tiledb_subarray_t* subarray,
    const char* dim_name,
    const void* start,
    uint64_t start_size,
    const void* end,
    uint64_t end_size) {
  return api_entry<tiledb::api::tiledb_subarray_add_range_var_by_name>(
      ctx, subarray, dim_name, start, start_size, end, end_size);
}

CAPI_INTERFACE(
    subarray_get_range_num,
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    uint32_t dim_idx,
    uint64_t* range_num) {
  return api_entry<tiledb::api::tiledb_subarray_get_range_num>(
      ctx, subarray, dim_idx, range_num);
}

CAPI_INTERFACE(
    subarray_get_range_num_from_name,
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    const char* dim_name,
    uint64_t* range_num) {
  return api_entry<tiledb::api::tiledb_subarray_get_range_num_from_name>(
      ctx, subarray, dim_name, range_num);
}

CAPI_INTERFACE(
    subarray_get_range,
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    uint32_t dim_idx,
    uint64_t range_idx,
    const void** start,
    const void** end,
    const void** stride) {
  return api_entry<tiledb::api::tiledb_subarray_get_range>(
      ctx, subarray, dim_idx, range_idx, start, end, stride);
}

CAPI_INTERFACE(
    subarray_get_range_var_size,
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    uint32_t dim_idx,
    uint64_t range_idx,
    uint64_t* start_size,
    uint64_t* end_size) {
  return api_entry<tiledb::api::tiledb_subarray_get_range_var_size>(
      ctx, subarray, dim_idx, range_idx, start_size, end_size);
}

CAPI_INTERFACE(
    subarray_get_range_from_name,
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    const char* dim_name,
    uint64_t range_idx,
    const void** start,
    const void** end,
    const void** stride) {
  return api_entry<tiledb::api::tiledb_subarray_get_range_from_name>(
      ctx, subarray, dim_name, range_idx, start, end, stride);
}

CAPI_INTERFACE(
    subarray_get_range_var_size_from_name,
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    const char* dim_name,
    uint64_t range_idx,
    uint64_t* start_size,
    uint64_t* end_size) {
  return api_entry<tiledb::api::tiledb_subarray_get_range_var_size_from_name>(
      ctx, subarray, dim_name, range_idx, start_size, end_size);
}

CAPI_INTERFACE(
    subarray_get_range_var,
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    uint32_t dim_idx,
    uint64_t range_idx,
    void* start,
    void* end) {
  return api_entry<tiledb::api::tiledb_subarray_get_range_var>(
      ctx, subarray, dim_idx, range_idx, start, end);
}

CAPI_INTERFACE(
    subarray_get_range_var_from_name,
    tiledb_ctx_t* ctx,
    const tiledb_subarray_t* subarray,
    const char* dim_name,
    uint64_t range_idx,
    void* start,
    void* end) {
  return api_entry<tiledb::api::tiledb_subarray_get_range_var_from_name>(
      ctx, subarray, dim_name, range_idx, start, end);
}

/* ****************************** */
/*          QUERY CONDITION       */
/* ****************************** */

CAPI_INTERFACE(
    query_condition_alloc,
    tiledb_ctx_t* const ctx,
    tiledb_query_condition_t** const cond) {
  return api_entry<tiledb::api::tiledb_query_condition_alloc>(ctx, cond);
}

CAPI_INTERFACE(
    query_condition_alloc_set_membership,
    tiledb_ctx_t* ctx,
    const char* field_name,
    const void* data,
    uint64_t data_size,
    const void* offsets,
    uint64_t offsets_size,
    tiledb_query_condition_op_t op,
    tiledb_query_condition_t** cond) {
  return api_entry_context<
      tiledb::api::tiledb_query_condition_alloc_set_membership>(
      ctx, field_name, data, data_size, offsets, offsets_size, op, cond);
}

CAPI_INTERFACE_VOID(query_condition_free, tiledb_query_condition_t** cond) {
  return api_entry_void<tiledb::api::tiledb_query_condition_free>(cond);
}

CAPI_INTERFACE(
    query_condition_init,
    tiledb_ctx_t* const ctx,
    tiledb_query_condition_t* const cond,
    const char* const attribute_name,
    const void* const condition_value,
    const uint64_t condition_value_size,
    const tiledb_query_condition_op_t op) {
  return api_entry<tiledb::api::tiledb_query_condition_init>(
      ctx, cond, attribute_name, condition_value, condition_value_size, op);
}

CAPI_INTERFACE(
    query_condition_combine,
    tiledb_ctx_t* const ctx,
    const tiledb_query_condition_t* const left_cond,
    const tiledb_query_condition_t* const right_cond,
    const tiledb_query_condition_combination_op_t combination_op,
    tiledb_query_condition_t** const combined_cond) {
  return api_entry<tiledb::api::tiledb_query_condition_combine>(
      ctx, left_cond, right_cond, combination_op, combined_cond);
}

CAPI_INTERFACE(
    query_condition_negate,
    tiledb_ctx_t* const ctx,
    const tiledb_query_condition_t* const cond,
    tiledb_query_condition_t** const negated_cond) {
  return api_entry<tiledb::api::tiledb_query_condition_negate>(
      ctx, cond, negated_cond);
}

CAPI_INTERFACE(
    query_condition_set_use_enumeration,
    tiledb_ctx_t* const ctx,
    const tiledb_query_condition_t* const cond,
    int use_enumeration) {
  return api_entry<tiledb::api::tiledb_query_condition_set_use_enumeration>(
      ctx, cond, use_enumeration);
}

/* ****************************** */
/*         UPDATE CONDITION       */
/* ****************************** */

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

/* ****************************** */
/*              ARRAY             */
/* ****************************** */

CAPI_INTERFACE(
    array_alloc,
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_array_t** array) {
  return api_entry<tiledb::api::tiledb_array_alloc>(ctx, array_uri, array);
}

CAPI_INTERFACE(
    array_set_open_timestamp_start,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    uint64_t timestamp_start) {
  return api_entry<tiledb::api::tiledb_array_set_open_timestamp_start>(
      ctx, array, timestamp_start);
}

CAPI_INTERFACE(
    array_set_open_timestamp_end,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    uint64_t timestamp_end) {
  return api_entry<tiledb::api::tiledb_array_set_open_timestamp_end>(
      ctx, array, timestamp_end);
}

CAPI_INTERFACE(
    array_get_open_timestamp_start,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    uint64_t* timestamp_start) {
  return api_entry<tiledb::api::tiledb_array_get_open_timestamp_start>(
      ctx, array, timestamp_start);
}

CAPI_INTERFACE(
    array_get_open_timestamp_end,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    uint64_t* timestamp_end) {
  return api_entry<tiledb::api::tiledb_array_get_open_timestamp_end>(
      ctx, array, timestamp_end);
}

CAPI_INTERFACE(array_delete, tiledb_ctx_t* ctx, const char* uri) {
  return api_entry<tiledb::api::tiledb_array_delete>(ctx, uri);
}

CAPI_INTERFACE(
    array_delete_array,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const char* uri) {
  return api_entry<tiledb::api::tiledb_array_delete_array>(ctx, array, uri);
}

CAPI_INTERFACE(
    array_delete_fragments,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const char* uri,
    uint64_t timestamp_start,
    uint64_t timestamp_end) {
  return api_entry<tiledb::api::tiledb_array_delete_fragments>(
      ctx, array, uri, timestamp_start, timestamp_end);
}

CAPI_INTERFACE(
    array_delete_fragments_v2,
    tiledb_ctx_t* ctx,
    const char* uri_str,
    uint64_t timestamp_start,
    uint64_t timestamp_end) {
  return api_entry<tiledb::api::tiledb_array_delete_fragments_v2>(
      ctx, uri_str, timestamp_start, timestamp_end);
}

CAPI_INTERFACE(
    array_delete_fragments_list,
    tiledb_ctx_t* ctx,
    const char* uri_str,
    const char* fragment_uris[],
    const size_t num_fragments) {
  return api_entry<tiledb::api::tiledb_array_delete_fragments_list>(
      ctx, uri_str, fragment_uris, num_fragments);
}

CAPI_INTERFACE(
    array_open,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_query_type_t query_type) {
  return api_entry<tiledb::api::tiledb_array_open>(ctx, array, query_type);
}

CAPI_INTERFACE(
    array_is_open, tiledb_ctx_t* ctx, tiledb_array_t* array, int32_t* is_open) {
  return api_entry<tiledb::api::tiledb_array_is_open>(ctx, array, is_open);
}

CAPI_INTERFACE(array_reopen, tiledb_ctx_t* ctx, tiledb_array_t* array) {
  return api_entry<tiledb::api::tiledb_array_reopen>(ctx, array);
}

CAPI_INTERFACE(
    array_set_config,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_config_t* config) {
  return api_entry<tiledb::api::tiledb_array_set_config>(ctx, array, config);
}

CAPI_INTERFACE(
    array_get_config,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_config_t** config) {
  return api_entry<tiledb::api::tiledb_array_get_config>(ctx, array, config);
}

CAPI_INTERFACE(array_close, tiledb_ctx_t* ctx, tiledb_array_t* array) {
  return api_entry<tiledb::api::tiledb_array_close>(ctx, array);
}

CAPI_INTERFACE_VOID(array_free, tiledb_array_t** array) {
  return api_entry_void<tiledb::api::tiledb_array_free>(array);
}

CAPI_INTERFACE(
    array_get_schema,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_array_schema_t** array_schema) {
  return api_entry<tiledb::api::tiledb_array_get_schema>(
      ctx, array, array_schema);
}

CAPI_INTERFACE(
    array_get_query_type,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_query_type_t* query_type) {
  return api_entry<tiledb::api::tiledb_array_get_query_type>(
      ctx, array, query_type);
}

CAPI_INTERFACE(
    array_create,
    tiledb_ctx_t* ctx,
    const char* array_uri,
    const tiledb_array_schema_t* array_schema) {
  return api_entry<tiledb::api::tiledb_array_create>(
      ctx, array_uri, array_schema);
}

CAPI_INTERFACE(
    array_create_with_key,
    tiledb_ctx_t* ctx,
    const char* array_uri,
    const tiledb_array_schema_t* array_schema,
    tiledb_encryption_type_t encryption_type,
    const void* encryption_key,
    uint32_t key_length) {
  return api_entry<tiledb::api::tiledb_array_create_with_key>(
      ctx,
      array_uri,
      array_schema,
      encryption_type,
      encryption_key,
      key_length);
}

CAPI_INTERFACE(
    array_consolidate,
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_config_t* config) {
  return api_entry<tiledb::api::tiledb_array_consolidate>(
      ctx, array_uri, config);
}

CAPI_INTERFACE(
    array_consolidate_with_key,
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_encryption_type_t encryption_type,
    const void* encryption_key,
    uint32_t key_length,
    tiledb_config_t* config) {
  return api_entry<tiledb::api::tiledb_array_consolidate_with_key>(
      ctx, array_uri, encryption_type, encryption_key, key_length, config);
}

CAPI_INTERFACE(
    array_consolidate_fragments,
    tiledb_ctx_t* ctx,
    const char* array_uri,
    const char** fragment_uris,
    const uint64_t num_fragments,
    tiledb_config_t* config) {
  return api_entry<tiledb::api::tiledb_array_consolidate_fragments>(
      ctx, array_uri, fragment_uris, num_fragments, config);
}

CAPI_INTERFACE(
    array_vacuum,
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_config_t* config) {
  return api_entry<tiledb::api::tiledb_array_vacuum>(ctx, array_uri, config);
}

CAPI_INTERFACE(
    array_get_non_empty_domain,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    void* domain,
    int32_t* is_empty) {
  return api_entry<tiledb::api::tiledb_array_get_non_empty_domain>(
      ctx, array, domain, is_empty);
}

CAPI_INTERFACE(
    array_get_non_empty_domain_from_index,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    uint32_t idx,
    void* domain,
    int32_t* is_empty) {
  return api_entry<tiledb::api::tiledb_array_get_non_empty_domain_from_index>(
      ctx, array, idx, domain, is_empty);
}

CAPI_INTERFACE(
    array_get_non_empty_domain_from_name,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const char* name,
    void* domain,
    int32_t* is_empty) {
  return api_entry<tiledb::api::tiledb_array_get_non_empty_domain_from_name>(
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
  return api_entry<
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
  return api_entry<
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
  return api_entry<
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
  return api_entry<
      tiledb::api::tiledb_array_get_non_empty_domain_var_from_name>(
      ctx, array, name, start, end, is_empty);
}

CAPI_INTERFACE(
    array_get_uri,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const char** array_uri) {
  return api_entry<tiledb::api::tiledb_array_get_uri>(ctx, array, array_uri);
}

CAPI_INTERFACE(
    array_encryption_type,
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_encryption_type_t* encryption_type) {
  return api_entry<tiledb::api::tiledb_array_encryption_type>(
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
  return api_entry<tiledb::api::tiledb_array_put_metadata>(
      ctx, array, key, value_type, value_num, value);
}

CAPI_INTERFACE(
    array_delete_metadata,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const char* key) {
  return api_entry<tiledb::api::tiledb_array_delete_metadata>(ctx, array, key);
}

CAPI_INTERFACE(
    array_get_metadata,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const char* key,
    tiledb_datatype_t* value_type,
    uint32_t* value_num,
    const void** value) {
  return api_entry<tiledb::api::tiledb_array_get_metadata>(
      ctx, array, key, value_type, value_num, value);
}

CAPI_INTERFACE(
    array_get_metadata_num,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    uint64_t* num) {
  return api_entry<tiledb::api::tiledb_array_get_metadata_num>(ctx, array, num);
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
  return api_entry<tiledb::api::tiledb_array_get_metadata_from_index>(
      ctx, array, index, key, key_len, value_type, value_num, value);
}

CAPI_INTERFACE(
    array_has_metadata_key,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const char* key,
    tiledb_datatype_t* value_type,
    int32_t* has_key) {
  return api_entry<tiledb::api::tiledb_array_has_metadata_key>(
      ctx, array, key, value_type, has_key);
}

CAPI_INTERFACE(
    array_evolve,
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_array_schema_evolution_t* array_schema_evolution) {
  return api_entry<tiledb::api::tiledb_array_evolve>(
      ctx, array_uri, array_schema_evolution);
}

CAPI_INTERFACE(
    array_get_enumeration,
    tiledb_ctx_t* ctx,
    const tiledb_array_t* array,
    const char* attr_name,
    tiledb_enumeration_t** enumeration) {
  return api_entry<tiledb::api::tiledb_array_get_enumeration>(
      ctx, array, attr_name, enumeration);
}

CAPI_INTERFACE(
    array_load_all_enumerations,
    tiledb_ctx_t* ctx,
    const tiledb_array_t* array) {
  return api_entry<tiledb::api::tiledb_array_load_all_enumerations>(ctx, array);
}

CAPI_INTERFACE(
    array_upgrade_version,
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_config_t* config) {
  return api_entry<tiledb::api::tiledb_array_upgrade_version>(
      ctx, array_uri, config);
}

/* ****************************** */
/*         OBJECT MANAGEMENT      */
/* ****************************** */

CAPI_INTERFACE(
    object_type, tiledb_ctx_t* ctx, const char* path, tiledb_object_t* type) {
  return api_entry<tiledb::api::tiledb_object_type>(ctx, path, type);
}

CAPI_INTERFACE(object_remove, tiledb_ctx_t* ctx, const char* path) {
  return api_entry<tiledb::api::tiledb_object_remove>(ctx, path);
}

CAPI_INTERFACE(
    object_move,
    tiledb_ctx_t* ctx,
    const char* old_path,
    const char* new_path) {
  return api_entry<tiledb::api::tiledb_object_move>(ctx, old_path, new_path);
}

CAPI_INTERFACE(
    object_walk,
    tiledb_ctx_t* ctx,
    const char* path,
    tiledb_walk_order_t order,
    int32_t (*callback)(const char*, tiledb_object_t, void*),
    void* data) {
  return api_entry<tiledb::api::tiledb_object_walk>(
      ctx, path, order, callback, data);
}

CAPI_INTERFACE(
    object_ls,
    tiledb_ctx_t* ctx,
    const char* path,
    int32_t (*callback)(const char*, tiledb_object_t, void*),
    void* data) {
  return api_entry<tiledb::api::tiledb_object_ls>(ctx, path, callback, data);
}

/* ****************************** */
/*              URI               */
/* ****************************** */

CAPI_INTERFACE(
    uri_to_path,
    tiledb_ctx_t* ctx,
    const char* uri,
    char* path_out,
    uint32_t* path_length) {
  return api_entry<tiledb::api::tiledb_uri_to_path>(
      ctx, uri, path_out, path_length);
}

/* ****************************** */
/*             Stats              */
/* ****************************** */

CAPI_INTERFACE_NULL(stats_enable) {
  return api_entry_plain<tiledb::api::tiledb_stats_enable>();
}

CAPI_INTERFACE_NULL(stats_disable) {
  return api_entry_plain<tiledb::api::tiledb_stats_disable>();
}

CAPI_INTERFACE_NULL(stats_reset) {
  return api_entry_plain<tiledb::api::tiledb_stats_reset>();
}

CAPI_INTERFACE(stats_dump, FILE* out) {
  return api_entry_plain<tiledb::api::tiledb_stats_dump>(out);
}

CAPI_INTERFACE(stats_dump_str, char** out) {
  return api_entry_plain<tiledb::api::tiledb_stats_dump_str>(out);
}

CAPI_INTERFACE(stats_raw_dump, FILE* out) {
  return api_entry_plain<tiledb::api::tiledb_stats_raw_dump>(out);
}

CAPI_INTERFACE(stats_raw_dump_str, char** out) {
  return api_entry_plain<tiledb::api::tiledb_stats_raw_dump_str>(out);
}

CAPI_INTERFACE(stats_free_str, char** out) {
  return api_entry_plain<tiledb::api::tiledb_stats_free_str>(out);
}

/* ****************************** */
/*          Heap Profiler         */
/* ****************************** */

CAPI_INTERFACE(
    heap_profiler_enable,
    const char* const file_name_prefix,
    const uint64_t dump_interval_ms,
    const uint64_t dump_interval_bytes,
    const uint64_t dump_threshold_bytes) {
  return api_entry_plain<tiledb::api::tiledb_heap_profiler_enable>(
      file_name_prefix,
      dump_interval_ms,
      dump_interval_bytes,
      dump_threshold_bytes);
}

/* ****************************** */
/*          Serialization         */
/* ****************************** */

CAPI_INTERFACE(
    serialize_array,
    tiledb_ctx_t* ctx,
    const tiledb_array_t* array,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_buffer_t** buffer) {
  return api_entry<tiledb::api::tiledb_serialize_array>(
      ctx, array, serialize_type, client_side, buffer);
}

CAPI_INTERFACE(
    deserialize_array,
    tiledb_ctx_t* ctx,
    const tiledb_buffer_t* buffer,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_array_t** array) {
  return api_entry<tiledb::api::tiledb_deserialize_array>(
      ctx, buffer, serialize_type, client_side, array);
}

CAPI_INTERFACE(
    serialize_array_schema,
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_buffer_t** buffer) {
  return api_entry<tiledb::api::tiledb_serialize_array_schema>(
      ctx, array_schema, serialize_type, client_side, buffer);
}

CAPI_INTERFACE(
    deserialize_array_schema,
    tiledb_ctx_t* ctx,
    const tiledb_buffer_t* buffer,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_array_schema_t** array_schema) {
  return api_entry<tiledb::api::tiledb_deserialize_array_schema>(
      ctx, buffer, serialize_type, client_side, array_schema);
}

CAPI_INTERFACE(
    serialize_array_open,
    tiledb_ctx_t* ctx,
    const tiledb_array_t* array,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_buffer_t** buffer) {
  return api_entry<tiledb::api::tiledb_serialize_array_open>(
      ctx, array, serialize_type, client_side, buffer);
}

CAPI_INTERFACE(
    deserialize_array_open,
    tiledb_ctx_t* ctx,
    const tiledb_buffer_t* buffer,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_array_t** array) {
  return api_entry<tiledb::api::tiledb_deserialize_array_open>(
      ctx, buffer, serialize_type, client_side, array);
}

CAPI_INTERFACE(
    serialize_array_schema_evolution,
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_evolution_t* array_schema_evolution,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_buffer_t** buffer) {
  return api_entry<tiledb::api::tiledb_serialize_array_schema_evolution>(
      ctx, array_schema_evolution, serialize_type, client_side, buffer);
}

CAPI_INTERFACE(
    deserialize_array_schema_evolution,
    tiledb_ctx_t* ctx,
    const tiledb_buffer_t* buffer,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_array_schema_evolution_t** array_schema_evolution) {
  return api_entry<tiledb::api::tiledb_deserialize_array_schema_evolution>(
      ctx, buffer, serialize_type, client_side, array_schema_evolution);
}

CAPI_INTERFACE(
    serialize_query,
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_buffer_list_t** buffer_list) {
  return api_entry<tiledb::api::tiledb_serialize_query>(
      ctx, query, serialize_type, client_side, buffer_list);
}

CAPI_INTERFACE(
    deserialize_query,
    tiledb_ctx_t* ctx,
    const tiledb_buffer_t* buffer,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_query_t* query) {
  return api_entry<tiledb::api::tiledb_deserialize_query>(
      ctx, buffer, serialize_type, client_side, query);
}

CAPI_INTERFACE(
    deserialize_query_and_array,
    tiledb_ctx_t* ctx,
    const tiledb_buffer_t* buffer,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    const char* array_uri,
    tiledb_query_t** query,
    tiledb_array_t** array) {
  return api_entry<tiledb::api::tiledb_deserialize_query_and_array>(
      ctx, buffer, serialize_type, client_side, array_uri, query, array);
}

CAPI_INTERFACE(
    serialize_array_nonempty_domain,
    tiledb_ctx_t* ctx,
    const tiledb_array_t* array,
    const void* nonempty_domain,
    int32_t is_empty,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_buffer_t** buffer) {
  return api_entry<tiledb::api::tiledb_serialize_array_nonempty_domain>(
      ctx,
      array,
      nonempty_domain,
      is_empty,
      serialize_type,
      client_side,
      buffer);
}

CAPI_INTERFACE(
    deserialize_array_nonempty_domain,
    tiledb_ctx_t* ctx,
    const tiledb_array_t* array,
    const tiledb_buffer_t* buffer,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    void* nonempty_domain,
    int32_t* is_empty) {
  return api_entry<tiledb::api::tiledb_deserialize_array_nonempty_domain>(
      ctx,
      array,
      buffer,
      serialize_type,
      client_side,
      nonempty_domain,
      is_empty);
}

CAPI_INTERFACE(
    serialize_array_non_empty_domain_all_dimensions,
    tiledb_ctx_t* ctx,
    const tiledb_array_t* array,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_buffer_t** buffer) {
  return api_entry<
      tiledb::api::tiledb_serialize_array_non_empty_domain_all_dimensions>(
      ctx, array, serialize_type, client_side, buffer);
}

CAPI_INTERFACE(
    deserialize_array_non_empty_domain_all_dimensions,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const tiledb_buffer_t* buffer,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side) {
  return api_entry<
      tiledb::api::tiledb_deserialize_array_non_empty_domain_all_dimensions>(
      ctx, array, buffer, serialize_type, client_side);
}

CAPI_INTERFACE(
    serialize_array_max_buffer_sizes,
    tiledb_ctx_t* ctx,
    const tiledb_array_t* array,
    const void* subarray,
    tiledb_serialization_type_t serialize_type,
    tiledb_buffer_t** buffer) {
  return api_entry<tiledb::api::tiledb_serialize_array_max_buffer_sizes>(
      ctx, array, subarray, serialize_type, buffer);
}

CAPI_INTERFACE(
    handle_array_delete_fragments_timestamps_request,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_serialization_type_t serialize_type,
    const tiledb_buffer_t* request) {
  return api_entry<
      tiledb::api::tiledb_handle_array_delete_fragments_timestamps_request>(
      ctx, array, serialize_type, request);
}

CAPI_INTERFACE(
    handle_array_delete_fragments_list_request,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_serialization_type_t serialize_type,
    const tiledb_buffer_t* request) {
  return api_entry<
      tiledb::api::tiledb_handle_array_delete_fragments_list_request>(
      ctx, array, serialize_type, request);
}

CAPI_INTERFACE(
    serialize_array_metadata,
    tiledb_ctx_t* ctx,
    const tiledb_array_t* array,
    tiledb_serialization_type_t serialize_type,
    tiledb_buffer_t** buffer) {
  return api_entry<tiledb::api::tiledb_serialize_array_metadata>(
      ctx, array, serialize_type, buffer);
}

CAPI_INTERFACE(
    deserialize_array_metadata,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_serialization_type_t serialize_type,
    const tiledb_buffer_t* buffer) {
  return api_entry<tiledb::api::tiledb_deserialize_array_metadata>(
      ctx, array, serialize_type, buffer);
}

CAPI_INTERFACE(
    serialize_query_est_result_sizes,
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_buffer_t** buffer) {
  return api_entry<tiledb::api::tiledb_serialize_query_est_result_sizes>(
      ctx, query, serialize_type, client_side, buffer);
}

CAPI_INTERFACE(
    deserialize_query_est_result_sizes,
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    const tiledb_buffer_t* buffer) {
  return api_entry<tiledb::api::tiledb_deserialize_query_est_result_sizes>(
      ctx, query, serialize_type, client_side, buffer);
}

CAPI_INTERFACE(
    serialize_config,
    tiledb_ctx_t* ctx,
    const tiledb_config_t* config,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_buffer_t** buffer) {
  return api_entry<tiledb::api::tiledb_serialize_config>(
      ctx, config, serialize_type, client_side, buffer);
}

CAPI_INTERFACE(
    deserialize_config,
    tiledb_ctx_t* ctx,
    const tiledb_buffer_t* buffer,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_config_t** config) {
  return api_entry_context<tiledb::api::tiledb_deserialize_config>(
      ctx, buffer, serialize_type, client_side, config);
}

CAPI_INTERFACE(
    serialize_fragment_info_request,
    tiledb_ctx_t* ctx,
    const tiledb_fragment_info_t* fragment_info,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_buffer_t** buffer) {
  return api_entry<tiledb::api::tiledb_serialize_fragment_info_request>(
      ctx, fragment_info, serialize_type, client_side, buffer);
}

CAPI_INTERFACE(
    deserialize_fragment_info_request,
    tiledb_ctx_t* ctx,
    const tiledb_buffer_t* buffer,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_fragment_info_t* fragment_info) {
  return api_entry<tiledb::api::tiledb_deserialize_fragment_info_request>(
      ctx, buffer, serialize_type, client_side, fragment_info);
}

CAPI_INTERFACE(
    serialize_fragment_info,
    tiledb_ctx_t* ctx,
    const tiledb_fragment_info_t* fragment_info,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_buffer_t** buffer) {
  return api_entry<tiledb::api::tiledb_serialize_fragment_info>(
      ctx, fragment_info, serialize_type, client_side, buffer);
}

CAPI_INTERFACE(
    deserialize_fragment_info,
    tiledb_ctx_t* ctx,
    const tiledb_buffer_t* buffer,
    tiledb_serialization_type_t serialize_type,
    const char* array_uri,
    int32_t client_side,
    tiledb_fragment_info_t* fragment_info) {
  return api_entry<tiledb::api::tiledb_deserialize_fragment_info>(
      ctx, buffer, serialize_type, array_uri, client_side, fragment_info);
}

CAPI_INTERFACE(
    handle_load_array_schema_request,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_serialization_type_t serialization_type,
    const tiledb_buffer_t* request,
    tiledb_buffer_t* response) {
  return api_entry<tiledb::api::tiledb_handle_load_array_schema_request>(
      ctx, array, serialization_type, request, response);
}

CAPI_INTERFACE(
    handle_load_enumerations_request,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_serialization_type_t serialization_type,
    const tiledb_buffer_t* request,
    tiledb_buffer_t* response) {
  return api_entry<tiledb::api::tiledb_handle_load_enumerations_request>(
      ctx, array, serialization_type, request, response);
}

CAPI_INTERFACE(
    handle_query_plan_request,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_serialization_type_t serialization_type,
    const tiledb_buffer_t* request,
    tiledb_buffer_t* response) {
  return api_entry<tiledb::api::tiledb_handle_query_plan_request>(
      ctx, array, serialization_type, request, response);
}

CAPI_INTERFACE(
    handle_consolidation_plan_request,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_serialization_type_t serialization_type,
    const tiledb_buffer_t* request,
    tiledb_buffer_t* response) {
  return api_entry<tiledb::api::tiledb_handle_consolidation_plan_request>(
      ctx, array, serialization_type, request, response);
}

/* ****************************** */
/*            C++ API             */
/* ****************************** */
int32_t tiledb::impl::tiledb_query_submit_async_func(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    void* callback_func,
    void* callback_data) noexcept {
  return api_entry<tiledb::api::impl::tiledb_query_submit_async_func>(
      ctx, query, callback_func, callback_data);
}

/* ****************************** */
/*          FRAGMENT INFO         */
/* ****************************** */

CAPI_INTERFACE(
    fragment_info_alloc,
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_fragment_info_t** fragment_info) {
  return api_entry<tiledb::api::tiledb_fragment_info_alloc>(
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
  return api_entry<tiledb::api::tiledb_fragment_info_set_config>(
      ctx, fragment_info, config);
}

CAPI_INTERFACE(
    fragment_info_get_config,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    tiledb_config_t** config) {
  return api_entry<tiledb::api::tiledb_fragment_info_get_config>(
      ctx, fragment_info, config);
}

CAPI_INTERFACE(
    fragment_info_load,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info) {
  return api_entry<tiledb::api::tiledb_fragment_info_load>(ctx, fragment_info);
}

CAPI_INTERFACE(
    fragment_info_get_fragment_name,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    const char** name) {
  return api_entry<tiledb::api::tiledb_fragment_info_get_fragment_name>(
      ctx, fragment_info, fid, name);
}

CAPI_INTERFACE(
    fragment_info_get_fragment_name_v2,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    tiledb_string_t** name) {
  return api_entry<tiledb::api::tiledb_fragment_info_get_fragment_name_v2>(
      ctx, fragment_info, fid, name);
}

CAPI_INTERFACE(
    fragment_info_get_fragment_num,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t* fragment_num) {
  return api_entry<tiledb::api::tiledb_fragment_info_get_fragment_num>(
      ctx, fragment_info, fragment_num);
}

CAPI_INTERFACE(
    fragment_info_get_fragment_uri,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    const char** uri) {
  return api_entry<tiledb::api::tiledb_fragment_info_get_fragment_uri>(
      ctx, fragment_info, fid, uri);
}

CAPI_INTERFACE(
    fragment_info_get_fragment_size,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint64_t* size) {
  return api_entry<tiledb::api::tiledb_fragment_info_get_fragment_size>(
      ctx, fragment_info, fid, size);
}

CAPI_INTERFACE(
    fragment_info_get_dense,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    int32_t* dense) {
  return api_entry<tiledb::api::tiledb_fragment_info_get_dense>(
      ctx, fragment_info, fid, dense);
}

CAPI_INTERFACE(
    fragment_info_get_sparse,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    int32_t* sparse) {
  return api_entry<tiledb::api::tiledb_fragment_info_get_sparse>(
      ctx, fragment_info, fid, sparse);
}

CAPI_INTERFACE(
    fragment_info_get_timestamp_range,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint64_t* start,
    uint64_t* end) {
  return api_entry<tiledb::api::tiledb_fragment_info_get_timestamp_range>(
      ctx, fragment_info, fid, start, end);
}

CAPI_INTERFACE(
    fragment_info_get_non_empty_domain_from_index,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t did,
    void* domain) {
  return api_entry<
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
  return api_entry<
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
  return api_entry<
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
  return api_entry<
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
  return api_entry<
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
  return api_entry<
      tiledb::api::tiledb_fragment_info_get_non_empty_domain_var_from_name>(
      ctx, fragment_info, fid, dim_name, start, end);
}

CAPI_INTERFACE(
    fragment_info_get_mbr_num,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint64_t* mbr_num) {
  return api_entry<tiledb::api::tiledb_fragment_info_get_mbr_num>(
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
  return api_entry<tiledb::api::tiledb_fragment_info_get_mbr_from_index>(
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
  return api_entry<tiledb::api::tiledb_fragment_info_get_mbr_from_name>(
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
  return api_entry<
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
  return api_entry<
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
  return api_entry<tiledb::api::tiledb_fragment_info_get_mbr_var_from_index>(
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
  return api_entry<tiledb::api::tiledb_fragment_info_get_mbr_var_from_name>(
      ctx, fragment_info, fid, mid, dim_name, start, end);
}

CAPI_INTERFACE(
    fragment_info_get_cell_num,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint64_t* cell_num) {
  return api_entry<tiledb::api::tiledb_fragment_info_get_cell_num>(
      ctx, fragment_info, fid, cell_num);
}

CAPI_INTERFACE(
    fragment_info_get_total_cell_num,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint64_t* cell_num) {
  return api_entry<tiledb::api::tiledb_fragment_info_get_total_cell_num>(
      ctx, fragment_info, cell_num);
}

CAPI_INTERFACE(
    fragment_info_get_version,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    uint32_t* version) {
  return api_entry<tiledb::api::tiledb_fragment_info_get_version>(
      ctx, fragment_info, fid, version);
}

CAPI_INTERFACE(
    fragment_info_has_consolidated_metadata,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    int32_t* has) {
  return api_entry<tiledb::api::tiledb_fragment_info_has_consolidated_metadata>(
      ctx, fragment_info, fid, has);
}

CAPI_INTERFACE(
    fragment_info_get_unconsolidated_metadata_num,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t* unconsolidated) {
  return api_entry<
      tiledb::api::tiledb_fragment_info_get_unconsolidated_metadata_num>(
      ctx, fragment_info, unconsolidated);
}

CAPI_INTERFACE(
    fragment_info_get_to_vacuum_num,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t* to_vacuum_num) {
  return api_entry<tiledb::api::tiledb_fragment_info_get_to_vacuum_num>(
      ctx, fragment_info, to_vacuum_num);
}

CAPI_INTERFACE(
    fragment_info_get_to_vacuum_uri,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    const char** uri) {
  return api_entry<tiledb::api::tiledb_fragment_info_get_to_vacuum_uri>(
      ctx, fragment_info, fid, uri);
}

CAPI_INTERFACE(
    fragment_info_get_array_schema,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    tiledb_array_schema_t** array_schema) {
  return api_entry<tiledb::api::tiledb_fragment_info_get_array_schema>(
      ctx, fragment_info, fid, array_schema);
}

CAPI_INTERFACE(
    fragment_info_get_array_schema_name,
    tiledb_ctx_t* ctx,
    tiledb_fragment_info_t* fragment_info,
    uint32_t fid,
    const char** schema_name) {
  return api_entry<tiledb::api::tiledb_fragment_info_get_array_schema_name>(
      ctx, fragment_info, fid, schema_name);
}

CAPI_INTERFACE(
    fragment_info_dump,
    tiledb_ctx_t* ctx,
    const tiledb_fragment_info_t* fragment_info,
    FILE* out) {
  return api_entry<tiledb::api::tiledb_fragment_info_dump>(
      ctx, fragment_info, out);
}

/* ********************************* */
/*          EXPERIMENTAL APIs        */
/* ********************************* */

CAPI_INTERFACE(
    query_get_status_details,
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    tiledb_query_status_details_t* status) {
  return api_entry<tiledb::api::tiledb_query_get_status_details>(
      ctx, query, status);
}

CAPI_INTERFACE(
    consolidation_plan_create_with_mbr,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    uint64_t fragment_size,
    tiledb_consolidation_plan_t** consolidation_plan) {
  return api_entry<tiledb::api::tiledb_consolidation_plan_create_with_mbr>(
      ctx, array, fragment_size, consolidation_plan);
}

CAPI_INTERFACE_VOID(
    consolidation_plan_free, tiledb_consolidation_plan_t** consolidation_plan) {
  return api_entry_void<tiledb::api::tiledb_consolidation_plan_free>(
      consolidation_plan);
}

CAPI_INTERFACE(
    consolidation_plan_get_num_nodes,
    tiledb_ctx_t* ctx,
    tiledb_consolidation_plan_t* consolidation_plan,
    uint64_t* num_nodes) {
  return api_entry<tiledb::api::tiledb_consolidation_plan_get_num_nodes>(
      ctx, consolidation_plan, num_nodes);
}

CAPI_INTERFACE(
    consolidation_plan_get_num_fragments,
    tiledb_ctx_t* ctx,
    tiledb_consolidation_plan_t* consolidation_plan,
    uint64_t node_index,
    uint64_t* num_fragments) {
  return api_entry<tiledb::api::tiledb_consolidation_plan_get_num_fragments>(
      ctx, consolidation_plan, node_index, num_fragments);
}

CAPI_INTERFACE(
    consolidation_plan_get_fragment_uri,
    tiledb_ctx_t* ctx,
    tiledb_consolidation_plan_t* consolidation_plan,
    uint64_t node_index,
    uint64_t fragment_index,
    const char** uri) {
  return api_entry<tiledb::api::tiledb_consolidation_plan_get_fragment_uri>(
      ctx, consolidation_plan, node_index, fragment_index, uri);
}

CAPI_INTERFACE(
    consolidation_plan_dump_json_str,
    tiledb_ctx_t* ctx,
    const tiledb_consolidation_plan_t* consolidation_plan,
    char** out) {
  return api_entry<tiledb::api::tiledb_consolidation_plan_dump_json_str>(
      ctx, consolidation_plan, out);
}

CAPI_INTERFACE(consolidation_plan_free_json_str, char** out) {
  return api_entry_plain<tiledb::api::tiledb_consolidation_plan_free_json_str>(
      out);
}
