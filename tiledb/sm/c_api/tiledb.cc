/**
 * @file   tiledb.cc
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
 * This file defines the C API of TileDB.
 */

#include "tiledb.h"
#include "tiledb_experimental.h"
#include "tiledb_serialization.h"
#include "tiledb_struct_def.h"

#include "tiledb/api/c_api/array/array_api_internal.h"
#include "tiledb/api/c_api/array_schema/array_schema_api_internal.h"
#include "tiledb/api/c_api/array_schema_evolution/array_schema_evolution_api_internal.h"
#include "tiledb/api/c_api/attribute/attribute_api_internal.h"
#include "tiledb/api/c_api/buffer/buffer_api_internal.h"
#include "tiledb/api/c_api/buffer_list/buffer_list_api_internal.h"
#include "tiledb/api/c_api/config/config_api_internal.h"
#include "tiledb/api/c_api/current_domain/current_domain_api_internal.h"
#include "tiledb/api/c_api/dimension/dimension_api_internal.h"
#include "tiledb/api/c_api/domain/domain_api_internal.h"
#include "tiledb/api/c_api/enumeration/enumeration_api_internal.h"
#include "tiledb/api/c_api/error/error_api_internal.h"
#include "tiledb/api/c_api/filter_list/filter_list_api_internal.h"
#include "tiledb/api/c_api/fragment_info/fragment_info_api_internal.h"
#include "tiledb/api/c_api/query/query_api_internal.h"
#include "tiledb/api/c_api/string/string_api_internal.h"
#include "tiledb/api/c_api/subarray/subarray_api_internal.h"
#include "tiledb/api/c_api_support/c_api_support.h"
#include "tiledb/as_built/as_built.h"
#include "tiledb/common/common.h"
#include "tiledb/common/dynamic_memory/dynamic_memory.h"
#include "tiledb/common/heap_profiler.h"
#include "tiledb/common/logger.h"
#include "tiledb/common/memory_tracker.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/dimension_label.h"
#include "tiledb/sm/c_api/api_argument_validator.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/config/config_iter.h"
#include "tiledb/sm/consolidator/consolidator.h"
#include "tiledb/sm/cpp_api/core_interface.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/filesystem.h"
#include "tiledb/sm/enums/mime_type.h"
#include "tiledb/sm/enums/object_type.h"
#include "tiledb/sm/enums/query_status.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/enums/serialization_type.h"
#include "tiledb/sm/filter/filter_pipeline.h"
#include "tiledb/sm/misc/tdb_time.h"
#include "tiledb/sm/object/object.h"
#include "tiledb/sm/object/object_iter.h"
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

  auto logger = ctx->resources().logger();
  logger->warn(message);

  return TILEDB_OK;
}

/* ********************************* */
/*              AS BUILT             */
/* ********************************* */
capi_return_t tiledb_as_built_dump(tiledb_string_t** out) {
  ensure_output_pointer_is_valid(out);
  *out = make_handle<tiledb_string_handle_t>(as_built::dump());
  return TILEDB_OK;
}

/* ****************************** */
/*              ARRAY             */
/* ****************************** */

capi_return_t tiledb_array_consolidate(
    tiledb_ctx_t* ctx, const char* array_uri, tiledb_config_t* config) {
  api::ensure_config_is_valid_if_present(config);
  tiledb::sm::Consolidator::array_consolidate(
      ctx->resources(),
      array_uri,
      tiledb::sm::EncryptionType::NO_ENCRYPTION,
      nullptr,
      0,
      (config == nullptr) ? ctx->config() : config->config(),
      ctx->storage_manager());
  return TILEDB_OK;
}

capi_return_t tiledb_array_consolidate_fragments(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    const char** fragment_uris,
    const uint64_t num_fragments,
    tiledb_config_t* config) {
  // Convert the list of fragments to a vector
  std::vector<std::string> uris;
  uris.reserve(num_fragments);
  for (uint64_t i = 0; i < num_fragments; i++) {
    uris.emplace_back(fragment_uris[i]);
  }

  tiledb::sm::Consolidator::fragments_consolidate(
      ctx->resources(),
      array_uri,
      tiledb::sm::EncryptionType::NO_ENCRYPTION,
      nullptr,
      0,
      uris,
      (config == nullptr) ? ctx->config() : config->config(),
      ctx->storage_manager());

  return TILEDB_OK;
}

capi_return_t tiledb_array_vacuum(
    tiledb_ctx_t* ctx, const char* array_uri, tiledb_config_t* config) {
  tiledb::sm::Consolidator::array_vacuum(
      ctx->resources(),
      array_uri,
      (config == nullptr) ? ctx->config() : config->config(),
      ctx->storage_manager());

  return TILEDB_OK;
}

/* ****************************** */
/*         OBJECT MANAGEMENT      */
/* ****************************** */

int32_t tiledb_object_type(
    tiledb_ctx_t* ctx, const char* path, tiledb_object_t* type) {
  ensure_output_pointer_is_valid(type);
  *type = static_cast<tiledb_object_t>(
      tiledb::sm::object_type(ctx->resources(), tiledb::sm::URI(path)));
  return TILEDB_OK;
}

int32_t tiledb_object_remove(tiledb_ctx_t* ctx, const char* path) {
  object_remove(ctx->resources(), path);
  return TILEDB_OK;
}

int32_t tiledb_object_move(
    tiledb_ctx_t* ctx, const char* old_path, const char* new_path) {
  object_move(ctx->resources(), old_path, new_path);
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
  tiledb::sm::ObjectIter* obj_iter = tiledb::sm::object_iter_begin(
      ctx->resources(), path, static_cast<tiledb::sm::WalkOrder>(order));

  // For as long as there is another object and the callback indicates to
  // continue, walk over the TileDB objects in the path
  const char* obj_name;
  tiledb::sm::ObjectType obj_type;
  bool has_next;
  int32_t rc = 0;
  do {
    if (SAVE_ERROR_CATCH(
            ctx,
            tiledb::sm::object_iter_next(
                ctx->resources(), obj_iter, &obj_name, &obj_type, &has_next))) {
      tiledb::sm::object_iter_free(obj_iter);
      return TILEDB_ERR;
    }
    if (!has_next)
      break;
    rc = callback(obj_name, tiledb_object_t(obj_type), data);
  } while (rc == 1);

  // Clean up
  tiledb::sm::object_iter_free(obj_iter);

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
  tiledb::sm::ObjectIter* obj_iter =
      tiledb::sm::object_iter_begin(ctx->resources(), path);

  // For as long as there is another object and the callback indicates to
  // continue, walk over the TileDB objects in the path
  const char* obj_name;
  tiledb::sm::ObjectType obj_type;
  bool has_next;
  int32_t rc = 0;
  do {
    if (SAVE_ERROR_CATCH(
            ctx,
            tiledb::sm::object_iter_next(
                ctx->resources(), obj_iter, &obj_name, &obj_type, &has_next))) {
      tiledb::sm::object_iter_free(obj_iter);
      return TILEDB_ERR;
    }
    if (!has_next)
      break;
    rc = callback(obj_name, tiledb_object_t(obj_type), data);
  } while (rc == 1);

  // Clean up
  tiledb::sm::object_iter_free(obj_iter);

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

int32_t tiledb_stats_is_enabled(uint8_t* enabled) {
  ensure_output_pointer_is_valid(enabled);
  *enabled = tiledb::sm::stats::all_stats.enabled() ? 1 : 0;
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
  ensure_array_is_valid(array);

  auto buf = make_handle<tiledb_buffer_handle_t>(
      ctx->resources().serialization_memory_tracker());

  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::array_serialize(
              array->array().get(),
              (tiledb::sm::SerializationType)serialize_type,
              buf->buffer(),
              client_side))) {
    break_handle(buf);
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
    const char* array_uri,
    tiledb_array_t** array) {
  ensure_buffer_is_valid(buffer);
  ensure_output_pointer_is_valid(array);

  // Check array URI
  auto uri = tiledb::sm::URI(array_uri);
  if (uri.is_invalid()) {
    throw CAPIException("Cannot deserialize array; Invalid URI");
  }

  // Create Array object and deserialize
  *array = make_handle<tiledb_array_t>(ctx->resources(), uri);
  auto memory_tracker = ctx->resources().create_memory_tracker();
  memory_tracker->set_type(sm::MemoryTrackerType::ARRAY_LOAD);
  try {
    tiledb::sm::serialization::array_deserialize(
        (*array)->array().get(),
        (tiledb::sm::SerializationType)serialize_type,
        buffer->buffer(),
        ctx->resources(),
        memory_tracker);
  } catch (StatusException& e) {
    break_handle(*array);
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
  ensure_array_schema_is_valid(array_schema);

  // Create buffer
  auto buf = make_handle<tiledb_buffer_handle_t>(
      ctx->resources().serialization_memory_tracker());

  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::array_schema_serialize(
              *(array_schema->array_schema()),
              (tiledb::sm::SerializationType)serialize_type,
              buf->buffer(),
              client_side))) {
    break_handle(buf);
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
  ensure_output_pointer_is_valid(array_schema);

  // Create array schema struct
  auto memory_tracker = ctx->resources().create_memory_tracker();
  memory_tracker->set_type(sm::MemoryTrackerType::ARRAY_LOAD);
  auto shared_schema = tiledb::sm::serialization::array_schema_deserialize(
      (tiledb::sm::SerializationType)serialize_type,
      buffer->buffer(),
      memory_tracker);

  *array_schema = make_handle<tiledb_array_schema_t>(shared_schema);

  return TILEDB_OK;
}

int32_t tiledb_deserialize_array_create(
    tiledb_ctx_t* ctx,
    const tiledb_buffer_t* buffer,
    tiledb_serialization_type_t serialize_type,
    int32_t,
    tiledb_string_t** storage_uri,
    tiledb_array_schema_t** array_schema) {
  api::ensure_buffer_is_valid(buffer);
  ensure_output_pointer_is_valid(array_schema);
  ensure_output_pointer_is_valid(storage_uri);

  // Create array schema struct
  auto memory_tracker = ctx->resources().create_memory_tracker();
  memory_tracker->set_type(sm::MemoryTrackerType::ARRAY_LOAD);
  auto [storage, shared_schema] =
      tiledb::sm::serialization::array_create_deserialize(
          (tiledb::sm::SerializationType)serialize_type,
          buffer->buffer(),
          memory_tracker);

  *array_schema = make_handle<tiledb_array_schema_t>(shared_schema);
  *storage_uri = make_handle<tiledb_string_handle_t>(storage.c_str());

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
  ensure_array_is_valid(array);

  auto buf = make_handle<tiledb_buffer_handle_t>(
      ctx->resources().serialization_memory_tracker());

  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::array_open_serialize(
              *(array->array().get()),
              (tiledb::sm::SerializationType)serialize_type,
              buf->buffer()))) {
    break_handle(buf);
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
  ensure_buffer_is_valid(buffer);
  ensure_output_pointer_is_valid(array);

  // Check array URI
  auto uri = tiledb::sm::URI("deserialized_array");
  if (uri.is_invalid()) {
    throw CAPIException("Cannot deserialize array open; Invalid URI");
  }

  // Create Array object and deserialize
  *array = make_handle<tiledb_array_t>(ctx->resources(), uri);
  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::array_open_deserialize(
              (*array)->array().get(),
              (tiledb::sm::SerializationType)serialize_type,
              buffer->buffer()))) {
    break_handle(*array);
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
  ensure_handle_is_valid(array_schema_evolution);

  auto buf = make_handle<tiledb_buffer_handle_t>(
      ctx->resources().serialization_memory_tracker());

  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::array_schema_evolution_serialize(
              array_schema_evolution->array_schema_evolution().get(),
              (tiledb::sm::SerializationType)serialize_type,
              buf->buffer(),
              client_side))) {
    break_handle(buf);
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
  api::ensure_buffer_is_valid(buffer);
  ensure_output_pointer_is_valid(array_schema_evolution);

  auto memory_tracker = ctx->resources().create_memory_tracker();
  memory_tracker->set_type(sm::MemoryTrackerType::SCHEMA_EVOLUTION);
  tiledb::sm::ArraySchemaEvolution* ase_raw = nullptr;
  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::array_schema_evolution_deserialize(
              &ase_raw,
              ctx->config(),
              (tiledb::sm::SerializationType)serialize_type,
              buffer->buffer(),
              memory_tracker))) {
    return TILEDB_ERR;
  }

  tdb_unique_ptr<tiledb::sm::ArraySchemaEvolution> owned_ase(ase_raw);
  *array_schema_evolution = make_handle<tiledb_array_schema_evolution_t>(
      shared_ptr<tiledb::sm::ArraySchemaEvolution>(std::move(owned_ase)));

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
              query->query(),
              (tiledb::sm::SerializationType)serialize_type,
              client_side == 1,
              (*buffer_list)->buffer_list()))) {
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
      query->query(),
      &ctx->resources().compute_tp(),
      ctx->resources().serialization_memory_tracker()));

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
  ensure_buffer_is_valid(buffer);
  ensure_output_pointer_is_valid(query);
  ensure_output_pointer_is_valid(array);

  // Create array object
  *array = make_handle<tiledb_array_t>(
      ctx->resources(),
      tiledb::sm::URI(array_uri, tiledb::sm::URI::must_be_valid));

  // First deserialize the array included in the query
  auto memory_tracker = ctx->resources().create_memory_tracker();
  memory_tracker->set_type(tiledb::sm::MemoryTrackerType::ARRAY_LOAD);
  try {
    throw_if_not_ok(tiledb::sm::serialization::array_from_query_deserialize(
        buffer->buffer(),
        (tiledb::sm::SerializationType)serialize_type,
        *(*array)->array(),
        ctx->resources(),
        memory_tracker));
  } catch (...) {
    break_handle(*array);
    throw;
  }

  // Create query
  *query = make_handle<tiledb_query_handle_t>(
      ctx->resources(),
      ctx->cancellation_source(),
      ctx->storage_manager(),
      (*array)->array());

  try {
    throw_if_not_ok(tiledb::sm::serialization::query_deserialize(
        buffer->buffer(),
        (tiledb::sm::SerializationType)serialize_type,
        client_side == 1,
        nullptr,
        (*query)->query(),
        &ctx->resources().compute_tp(),
        ctx->resources().serialization_memory_tracker()));
  } catch (...) {
    break_handle(*query);
    break_handle(*array);
    throw;
  }

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
  ensure_array_is_valid(array);

  auto buf = make_handle<tiledb_buffer_handle_t>(
      ctx->resources().serialization_memory_tracker());

  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::nonempty_domain_serialize(
              array->array().get(),
              nonempty_domain,
              is_empty,
              (tiledb::sm::SerializationType)serialize_type,
              buf->buffer()))) {
    break_handle(buf);
    return TILEDB_ERR;
  }

  *buffer = buf;

  return TILEDB_OK;
}

int32_t tiledb_deserialize_array_nonempty_domain(
    const tiledb_array_t* array,
    const tiledb_buffer_t* buffer,
    tiledb_serialization_type_t serialize_type,
    int32_t,
    void* nonempty_domain,
    int32_t* is_empty) {
  ensure_array_is_valid(array);
  ensure_buffer_is_valid(buffer);

  bool is_empty_bool;
  throw_if_not_ok(tiledb::sm::serialization::nonempty_domain_deserialize(
      array->array().get(),
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
  ensure_array_is_valid(array);

  auto buf = make_handle<tiledb_buffer_handle_t>(
      ctx->resources().serialization_memory_tracker());

  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::nonempty_domain_serialize(
              array->array().get(),
              (tiledb::sm::SerializationType)serialize_type,
              buf->buffer()))) {
    break_handle(buf);
    return TILEDB_ERR;
  }

  *buffer = buf;

  return TILEDB_OK;
}

int32_t tiledb_deserialize_array_non_empty_domain_all_dimensions(
    tiledb_array_t* array,
    const tiledb_buffer_t* buffer,
    tiledb_serialization_type_t serialize_type,
    int32_t) {
  ensure_array_is_valid(array);
  ensure_buffer_is_valid(buffer);

  throw_if_not_ok(tiledb::sm::serialization::nonempty_domain_deserialize(
      array->array().get(),
      buffer->buffer(),
      (tiledb::sm::SerializationType)serialize_type));

  return TILEDB_OK;
}

int32_t tiledb_serialize_array_max_buffer_sizes(
    tiledb_ctx_t*,
    const tiledb_array_t*,
    const void*,
    tiledb_serialization_type_t,
    tiledb_buffer_t**) {
  throw CAPIException(
      "tiledb_serialize_array_max_buffer_sizes is no longer supported.");
}

capi_return_t tiledb_handle_array_delete_fragments_timestamps_request(
    tiledb_array_t* array,
    tiledb_serialization_type_t serialize_type,
    const tiledb_buffer_t* request) {
  ensure_array_is_valid(array);
  ensure_buffer_is_valid(request);

  // Deserialize buffer
  auto [timestamp_start, timestamp_end] = tiledb::sm::serialization::
      deserialize_delete_fragments_timestamps_request(
          (tiledb::sm::SerializationType)serialize_type, request->buffer());

  // Delete fragments
  try {
    array->delete_fragments(array->array_uri(), timestamp_start, timestamp_end);
  } catch (...) {
    throw CAPIException("Failed to delete fragments");
  }

  return TILEDB_OK;
}

capi_return_t tiledb_handle_array_delete_fragments_list_request(
    tiledb_array_t* array,
    tiledb_serialization_type_t serialize_type,
    const tiledb_buffer_t* request) {
  ensure_array_is_valid(array);
  ensure_buffer_is_valid(request);

  // Deserialize buffer
  auto uris =
      tiledb::sm::serialization::deserialize_delete_fragments_list_request(
          array->array_uri(),
          (tiledb::sm::SerializationType)serialize_type,
          request->buffer());

  // Delete fragments list
  try {
    array->delete_fragments_list(uris);
  } catch (...) {
    throw CAPIException("Failed to delete fragments_list");
  }

  return TILEDB_OK;
}

int32_t tiledb_serialize_array_metadata(
    tiledb_ctx_t* ctx,
    const tiledb_array_t* array,
    tiledb_serialization_type_t serialize_type,
    tiledb_buffer_t** buffer) {
  ensure_array_is_valid(array);

  auto buf = make_handle<tiledb_buffer_handle_t>(
      ctx->resources().serialization_memory_tracker());

  // Get metadata to serialize, this will load it if it does not exist
  sm::Metadata* metadata = nullptr;
  try {
    metadata = &array->metadata();
  } catch (StatusException& e) {
    auto st = Status_Error(e.what());
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  // Serialize
  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::metadata_serialize(
              metadata,
              (tiledb::sm::SerializationType)serialize_type,
              buf->buffer()))) {
    break_handle(buf);
    return TILEDB_ERR;
  }

  *buffer = buf;

  return TILEDB_OK;
}

int32_t tiledb_deserialize_array_metadata(
    tiledb_array_t* array,
    tiledb_serialization_type_t serialize_type,
    const tiledb_buffer_t* buffer) {
  ensure_array_is_valid(array);
  ensure_buffer_is_valid(buffer);

  // Deserialize
  throw_if_not_ok(tiledb::sm::serialization::metadata_deserialize(
      array->unsafe_metadata(),
      array->config(),
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

  auto buf = make_handle<tiledb_buffer_handle_t>(
      ctx->resources().serialization_memory_tracker());

  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::query_est_result_size_serialize(
              query->query(),
              (tiledb::sm::SerializationType)serialize_type,
              client_side == 1,
              buf->buffer()))) {
    break_handle(buf);
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
      query->query(),
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

  auto buf = make_handle<tiledb_buffer_handle_t>(
      ctx->resources().serialization_memory_tracker());

  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::config_serialize(
              config->config(),
              (tiledb::sm::SerializationType)serialize_type,
              buf->buffer(),
              client_side))) {
    break_handle(buf);
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
  *config = make_handle<tiledb_config_handle_t>(*new_config);
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
  ensure_fragment_info_is_valid(fragment_info);

  auto buf = make_handle<tiledb_buffer_handle_t>(
      ctx->resources().serialization_memory_tracker());

  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::fragment_info_request_serialize(
              *fragment_info->fragment_info(),
              (tiledb::sm::SerializationType)serialize_type,
              buf->buffer()))) {
    break_handle(buf);
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
  ensure_fragment_info_is_valid(fragment_info);
  ensure_buffer_is_valid(buffer);

  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::fragment_info_request_deserialize(
              fragment_info->fragment_info().get(),
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
  ensure_fragment_info_is_valid(fragment_info);

  auto buf = make_handle<tiledb_buffer_handle_t>(
      ctx->resources().serialization_memory_tracker());

  // Serialize
  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::fragment_info_serialize(
              *fragment_info->fragment_info(),
              (tiledb::sm::SerializationType)serialize_type,
              buf->buffer(),
              client_side))) {
    break_handle(buf);
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
  ensure_fragment_info_is_valid(fragment_info);
  ensure_buffer_is_valid(buffer);

  // Check array uri
  tiledb::sm::URI uri(array_uri);
  if (uri.is_invalid()) {
    auto st =
        Status_Error("Failed to deserialize fragment info; Invalid array URI");
    LOG_STATUS_NO_RETURN_VALUE(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  auto memory_tracker = ctx->resources().create_memory_tracker();
  memory_tracker->set_type(sm::MemoryTrackerType::FRAGMENT_INFO_LOAD);
  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::serialization::fragment_info_deserialize(
              fragment_info->fragment_info().get(),
              (tiledb::sm::SerializationType)serialize_type,
              uri,
              buffer->buffer(),
              memory_tracker))) {
    return TILEDB_ERR;
  }

  return TILEDB_OK;
}

capi_return_t tiledb_handle_load_array_schema_request(
    tiledb_array_t* array,
    tiledb_serialization_type_t serialization_type,
    const tiledb_buffer_t* request,
    tiledb_buffer_t* response) {
  ensure_array_is_valid(array);
  ensure_buffer_is_valid(request);
  ensure_buffer_is_valid(response);

  auto load_schema_req =
      tiledb::sm::serialization::deserialize_load_array_schema_request(
          static_cast<tiledb::sm::SerializationType>(serialization_type),
          request->buffer());

  if (load_schema_req.include_enumerations_latest_schema()) {
    array->load_all_enumerations();
  }

  tiledb::sm::serialization::serialize_load_array_schema_response(
      *array->array(),
      static_cast<tiledb::sm::SerializationType>(serialization_type),
      response->buffer());

  return TILEDB_OK;
}

capi_return_t tiledb_handle_load_enumerations_request(
    tiledb_array_t* array,
    tiledb_serialization_type_t serialization_type,
    const tiledb_buffer_t* request,
    tiledb_buffer_t* response) {
  ensure_array_is_valid(array);
  ensure_buffer_is_valid(request);
  ensure_buffer_is_valid(response);

  auto enumeration_names =
      tiledb::sm::serialization::deserialize_load_enumerations_request(
          static_cast<tiledb::sm::SerializationType>(serialization_type),
          request->buffer());
  std::unordered_map<
      std::string,
      std::vector<shared_ptr<const tiledb::sm::Enumeration>>>
      enumerations;
  if (enumeration_names.empty()) {
    enumerations = array->get_enumerations_all_schemas();
  } else {
    enumerations[array->array_schema_latest().name()] =
        array->get_enumerations(enumeration_names);
  }

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
  ensure_array_is_valid(array);
  ensure_buffer_is_valid(request);
  ensure_buffer_is_valid(response);

  tiledb::sm::Query query(
      ctx->resources(),
      ctx->cancellation_source(),
      ctx->storage_manager(),
      array->array());
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
    tiledb_array_t* array,
    tiledb_serialization_type_t serialization_type,
    const tiledb_buffer_t* request,
    tiledb_buffer_t* response) {
  ensure_array_is_valid(array);
  ensure_buffer_is_valid(request);
  ensure_buffer_is_valid(response);

  // Error if array is not open
  if (!array->is_open()) {
    throw std::logic_error(
        "Cannot get consolidation plan. Input array is not open");
  }

  auto fragment_size =
      tiledb::sm::serialization::deserialize_consolidation_plan_request(
          static_cast<tiledb::sm::SerializationType>(serialization_type),
          request->buffer());
  sm::ConsolidationPlan plan(array->array(), fragment_size);

  tiledb::sm::serialization::serialize_consolidation_plan_response(
      plan,
      static_cast<tiledb::sm::SerializationType>(serialization_type),
      response->buffer());

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
      (tiledb_query_status_details_reason_t)query->query()
          ->status_incomplete_reason();
  status->incomplete_reason = incomplete_reason;

  return TILEDB_OK;
}

int32_t tiledb_consolidation_plan_create_with_mbr(
    tiledb_array_t* array,
    uint64_t fragment_size,
    tiledb_consolidation_plan_t** consolidation_plan) {
  ensure_array_is_valid(array);
  ensure_output_pointer_is_valid(consolidation_plan);

  *consolidation_plan = make_handle<tiledb_consolidation_plan_handle_t>(
      std::in_place, array->array(), fragment_size);

  return TILEDB_OK;
}

void tiledb_consolidation_plan_free(
    tiledb_consolidation_plan_t** consolidation_plan) {
  break_handle(*consolidation_plan);
}

int32_t tiledb_consolidation_plan_get_num_nodes(
    tiledb_consolidation_plan_t* consolidation_plan, uint64_t* num_nodes) {
  ensure_handle_is_valid(consolidation_plan);

  *num_nodes = consolidation_plan->consolidation_plan().get_num_nodes();
  return TILEDB_OK;
}

int32_t tiledb_consolidation_plan_get_num_fragments(
    tiledb_consolidation_plan_t* consolidation_plan,
    uint64_t node_index,
    uint64_t* num_fragments) {
  ensure_handle_is_valid(consolidation_plan);

  *num_fragments =
      consolidation_plan->consolidation_plan().get_num_fragments(node_index);

  return TILEDB_OK;
}

int32_t tiledb_consolidation_plan_get_fragment_uri(
    tiledb_consolidation_plan_t* consolidation_plan,
    uint64_t node_index,
    uint64_t fragment_index,
    const char** uri) {
  ensure_handle_is_valid(consolidation_plan);

  *uri = consolidation_plan->consolidation_plan().get_fragment_uri(
      node_index, fragment_index);

  return TILEDB_OK;
}

int32_t tiledb_consolidation_plan_dump_json_str(
    const tiledb_consolidation_plan_t* consolidation_plan, char** out) {
  ensure_handle_is_valid(consolidation_plan);
  ensure_output_pointer_is_valid(out);

  std::string str = consolidation_plan->consolidation_plan().dump();

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
/*       ARRAY CONSOLIDATION      */
/* ****************************** */

CAPI_INTERFACE(
    array_consolidate,
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_config_t* config) {
  return api_entry<tiledb::api::tiledb_array_consolidate>(
      ctx, array_uri, config);
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

CAPI_INTERFACE(stats_is_enabled, uint8_t* enabled) {
  return api_entry_plain<tiledb::api::tiledb_stats_is_enabled>(enabled);
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
    const char* array_uri,
    tiledb_array_t** array) {
  return api_entry<tiledb::api::tiledb_deserialize_array>(
      ctx, buffer, serialize_type, client_side, array_uri, array);
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
    deserialize_array_create,
    tiledb_ctx_t* ctx,
    const tiledb_buffer_t* buffer,
    tiledb_serialization_type_t serialize_type,
    int32_t client_side,
    tiledb_string_t** storage,
    tiledb_array_schema_t** array_schema) {
  return api_entry<tiledb::api::tiledb_deserialize_array_create>(
      ctx, buffer, serialize_type, client_side, storage, array_schema);
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
  return api_entry_context<
      tiledb::api::tiledb_deserialize_array_nonempty_domain>(
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
  return api_entry_context<
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
  return api_entry_context<
      tiledb::api::tiledb_handle_array_delete_fragments_timestamps_request>(
      ctx, array, serialize_type, request);
}

CAPI_INTERFACE(
    handle_array_delete_fragments_list_request,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_serialization_type_t serialize_type,
    const tiledb_buffer_t* request) {
  return api_entry_context<
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
  return api_entry_context<tiledb::api::tiledb_deserialize_array_metadata>(
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
  return api_entry_context<
      tiledb::api::tiledb_handle_load_array_schema_request>(
      ctx, array, serialization_type, request, response);
}

CAPI_INTERFACE(
    handle_load_enumerations_request,
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    tiledb_serialization_type_t serialization_type,
    const tiledb_buffer_t* request,
    tiledb_buffer_t* response) {
  return api_entry_context<
      tiledb::api::tiledb_handle_load_enumerations_request>(
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
  return api_entry_context<
      tiledb::api::tiledb_handle_consolidation_plan_request>(
      ctx, array, serialization_type, request, response);
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
  return api_entry_context<
      tiledb::api::tiledb_consolidation_plan_create_with_mbr>(
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
  return api_entry_context<
      tiledb::api::tiledb_consolidation_plan_get_num_nodes>(
      ctx, consolidation_plan, num_nodes);
}

CAPI_INTERFACE(
    consolidation_plan_get_num_fragments,
    tiledb_ctx_t* ctx,
    tiledb_consolidation_plan_t* consolidation_plan,
    uint64_t node_index,
    uint64_t* num_fragments) {
  return api_entry_context<
      tiledb::api::tiledb_consolidation_plan_get_num_fragments>(
      ctx, consolidation_plan, node_index, num_fragments);
}

CAPI_INTERFACE(
    consolidation_plan_get_fragment_uri,
    tiledb_ctx_t* ctx,
    tiledb_consolidation_plan_t* consolidation_plan,
    uint64_t node_index,
    uint64_t fragment_index,
    const char** uri) {
  return api_entry_context<
      tiledb::api::tiledb_consolidation_plan_get_fragment_uri>(
      ctx, consolidation_plan, node_index, fragment_index, uri);
}

CAPI_INTERFACE(
    consolidation_plan_dump_json_str,
    tiledb_ctx_t* ctx,
    const tiledb_consolidation_plan_t* consolidation_plan,
    char** out) {
  return api_entry_context<
      tiledb::api::tiledb_consolidation_plan_dump_json_str>(
      ctx, consolidation_plan, out);
}

CAPI_INTERFACE(consolidation_plan_free_json_str, char** out) {
  return api_entry_plain<tiledb::api::tiledb_consolidation_plan_free_json_str>(
      out);
}
