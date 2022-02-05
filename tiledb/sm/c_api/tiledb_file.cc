/**
 * @file   tiledb_file.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * This file defines the C API of TileDB for tiledb_array_as_file... functions.
 */

#include <sstream>

#include "tiledb/appl/blob_array/blob_array.h"
#include "tiledb/appl/blob_array/blob_array_schema.h"
#include "tiledb/sm/c_api/tiledb_experimental.h"
#include "tiledb/sm/c_api/tiledb_helpers.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
#include "tiledb/sm/enums/encryption_type.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/misc/constants.h"

static int32_t is_blob_array(tiledb_ctx_t* ctx, tiledb_array_t* array) {
  // ASSERT( array is_open );
  if (sanity_check(ctx, array) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  tiledb_array_schema_t* array_schema;
  if (tiledb_array_get_schema(ctx, array, &array_schema) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  tiledb_attribute_t* attr;
  if (tiledb_array_schema_get_attribute_from_name(
          ctx,
          array_schema,
          tiledb::sm::constants::blob_array_attribute_name.c_str(),
          &attr) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  tiledb_datatype_t attr_type;
  if (tiledb_attribute_get_type(ctx, attr, &attr_type) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  if (attr_type != TILEDB_BLOB) {
    return TILEDB_ERR;
  }

  tiledb_attribute_free(&attr);
  tiledb_array_schema_free(&array_schema);

  return TILEDB_OK;
}

// 'array' parameter based 'file' api
TILEDB_EXPORT int32_t tiledb_array_as_file_obtain(
    tiledb_ctx_t* ctx,
    tiledb_array_t** array,
    const char* array_uri,
    tiledb_config_t* config) {
  // on sanity_check on 'config' as it can be nullptr.
  if (sanity_check(ctx) == TILEDB_ERR) {
    return TILEDB_ERR;
  }
  tiledb_array_schema_t* blob_array_schema = nullptr;
  if (tiledb_array_schema_create_default_blob_array(ctx, &blob_array_schema) ==
      TILEDB_ERR) {
    return TILEDB_ERR;
  }

  if (tiledb_array_alloc(ctx, array_uri, array) == TILEDB_ERR) {
    // expect tiledb_array_alloc() to have logged any necessary error.
    tiledb_array_schema_free(&blob_array_schema);
    return TILEDB_ERR;
  }
  // Check array URI
  auto uri_array = tiledb::sm::URI(array_uri);
  if (uri_array.is_invalid()) {
    std::ostringstream errmsg;
    errmsg << "Failed to create TileDB blob_array object; Invalid array URI \""
           << uri_array.to_string() << "\"";
    auto st = Status_Error(errmsg.str());
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  auto stg_mgr = ctx->ctx_->storage_manager();
  // release the 'default' allocated array, to be replaced afterwards
  delete (*array)->array_;
  tiledb::appl::BlobArray* blob_array;
  (*array)->array_ = blob_array =
      new (std::nothrow) tiledb::appl::BlobArray(uri_array, stg_mgr);
  if ((*array)->array_ == nullptr) {
    std::ostringstream errmsg;
    errmsg << "Failed to allocate TileDB blob_array object";
    auto st = Status_Error(errmsg.str());
    LOG_STATUS(st);
    save_error(ctx, st);
    tiledb_array_free(
        array);  // already returning an error, ignore any error here.
    tiledb_array_schema_free(&blob_array_schema);
    *array = nullptr;
    return TILEDB_OOM;
  }

  if (config) {
    if (tiledb_array_set_config(ctx, *array, config) == TILEDB_ERR) {
      delete *array;
      *array = nullptr;
      return TILEDB_ERR;
    }
  }

  if (tiledb_array_open(ctx, *array, TILEDB_READ) == TILEDB_ERR) {
#if 0
    // (lead-ins needed for below with "This works, but is deprecated)
    const char* encryption_type_str = nullptr;
    const char* encryption_key_str = nullptr;
    tiledb_encryption_type_t encryption_type = TILEDB_NO_ENCRYPTION;
    if (config) {
      if (SAVE_ERROR_CATCH(
              ctx, config->config_->get("sm.encryption_type", &encryption_type_str))) {
        return TILEDB_ERR;
      }
      auto [st, et] = tiledb::sm::encryption_type_enum(encryption_type_str);
      if (!st.ok()) {
        return TILEDB_ERR;
      }
      encryption_type = static_cast<tiledb_encryption_type_t>(et.value());
      if (SAVE_ERROR_CATCH(
              ctx,
              config->config_->get(
                  "sm.encryption_key", &encryption_key_str))) {
        return TILEDB_ERR;
      }
    }
   //Note: This works, but is deprecated
    if (tiledb_array_create_with_key(
          ctx,
          array_uri,
          blob_array_schema,
          encryption_type,
          encryption_key_str,
            encryption_type_str ?
                static_cast<uint32_t>(strlen(encryption_key_str)) :
                0) == TILEDB_ERR) {
#elif 0
    // While storage_manager->open() will apparently honor array config,
    // seems storage_manager->create uses -storage_manager- cfg which
    // we don't really want to be modifying...
    if (tiledb_array_create(ctx, array_uri, blob_array_schema) == TILEDB_ERR) {
#else
    auto cfg = config ? config->config_ : &stg_mgr->config();
    if (SAVE_ERROR_CATCH(ctx, blob_array->create(cfg))) {
#endif
    tiledb_array_free(array);
    tiledb_array_schema_free(&blob_array_schema);
    *array = nullptr;
    return TILEDB_ERR;
  }
}
else {
  if (tiledb_array_close(ctx, *array) == TILEDB_ERR) {
    tiledb_array_free(array);
    *array = nullptr;
    tiledb_array_schema_free(&blob_array_schema);
    return TILEDB_ERR;
  }
}

// returning an allocated tiledb_array_t at '*array' having default blob_array
// schema;

return TILEDB_OK;
}

TILEDB_EXPORT int32_t tiledb_array_as_file_import(
    tiledb_ctx_t* ctx, tiledb_array_t* array, const char* input_uri_filename) {
  if (sanity_check(ctx, array) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  int32_t is_open = 0;
  if (tiledb_array_is_open(ctx, array, &is_open) == TILEDB_ERR) {
    return TILEDB_ERR;
  }
  if (is_open) {
    tiledb_array_close(ctx, array);
  }

  // tiledb_array_open() passes NO_ENCRYPTION but ->array_->open() in that
  // circumstance uses any encryption previously set on the array.
  if (tiledb_array_open(ctx, array, TILEDB_WRITE) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  if (is_blob_array(ctx, array) == TILEDB_ERR) {
    std::ostringstream errmsg;
    errmsg << "Failed tiledb_array_as_file_import; array not valid for file "
              "action, (filename URI \""
           << input_uri_filename << ")\"";
    auto st = Status_Error(errmsg.str());
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  auto uri_filename = tiledb::sm::URI(input_uri_filename);
  if (uri_filename.is_invalid()) {
    std::ostringstream errmsg;
    errmsg << "Failed to create TileDB file object; Invalid filename URI \""
           << uri_filename.to_string() << "\"";
    auto st = Status_Error(errmsg.str());
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  auto blob_array = static_cast<tiledb::appl::BlobArray*>(array->array_);

  int32_t ret_stat = TILEDB_OK;
  if (SAVE_ERROR_CATCH(ctx, blob_array->save_from_uri(uri_filename, nullptr))) {
    ret_stat = TILEDB_ERR;
  }

  tiledb_array_close(ctx, array);

  return ret_stat;
}

TILEDB_EXPORT int32_t tiledb_array_as_file_export(
    tiledb_ctx_t* ctx, tiledb_array_t* array, const char* output_uri_filename) {
  if (sanity_check(ctx, array) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  int32_t is_open;
  if (tiledb_array_is_open(ctx, array, &is_open) == TILEDB_ERR) {
    tiledb_array_close(ctx, array);
  }

  // tiledb_array_open() passes NO_ENCRYPTION but ->array_->open() in that
  // circumstance uses any encryption previously set on the array.
  if (tiledb_array_open(ctx, array, TILEDB_READ) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  if (is_blob_array(ctx, array) == TILEDB_ERR) {
    std::ostringstream errmsg;
    errmsg << "Failed tiledb_array_as_file_export; array not valid for file "
              "action, (filename URI \""
           << output_uri_filename << ")\"";
    auto st = Status_Error(errmsg.str());
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  auto uri_filename = tiledb::sm::URI(output_uri_filename);
  if (uri_filename.is_invalid()) {
    std::ostringstream errmsg;
    errmsg << "Failed to create TileDB file object; Invalid filename URI \""
           << uri_filename.to_string() << "\"";
    auto st = Status_Error(errmsg.str());
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  auto blob_array = static_cast<tiledb::appl::BlobArray*>(array->array_);
  int32_t ret_stat = TILEDB_OK;
  if (SAVE_ERROR_CATCH(ctx, blob_array->export_to_uri(uri_filename, nullptr))) {
    ret_stat = TILEDB_ERR;
  }

  tiledb_array_close(ctx, array);

  return ret_stat;
}

TILEDB_EXPORT int32_t tiledb_array_schema_create_default_blob_array(
    tiledb_ctx_t* ctx, tiledb_array_schema_t** array_schema) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create array schema struct
  *array_schema = new (std::nothrow) tiledb_array_schema_t;
  if (*array_schema == nullptr) {
    auto st = Status_Error("Failed to allocate TileDB array schema object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Create a new ArraySchema object
  (*array_schema)->array_schema_ =
      new (std::nothrow) tiledb::appl::BlobArraySchema();
  if ((*array_schema)->array_schema_ == nullptr) {
    delete *array_schema;
    *array_schema = nullptr;
    auto st = Status_Error("Failed to allocate TileDB array schema object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Success
  return TILEDB_OK;
}
