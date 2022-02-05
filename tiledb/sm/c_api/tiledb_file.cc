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

#if 01
#include <sstream>

#include "tiledb/appl/blob_array/blob_array_schema.h"
#include "tiledb/appl/blob_array/blob_array.h"
#include "tiledb/sm/c_api/tiledb_experimental.h"
#include "tiledb/sm/c_api/tiledb_helpers.h"
#include "tiledb/sm/c_api/tiledb_struct_def.h"
//#include "tiledb/sm/file/file.h"
#include "tiledb/sm/enums/query_type.h"
#include "tiledb/sm/enums/encryption_type.h"


// 'array' parameter based 'file' api
TILEDB_EXPORT int32_t tiledb_array_as_file_obtain(
    tiledb_ctx_t* ctx, tiledb_array_t** array, const char* array_uri, tiledb_config_t *config) {
  if (sanity_check(ctx) == TILEDB_ERR) {
    return TILEDB_ERR;
  }
  tiledb_array_schema_t* blob_array_schema = nullptr;
  if (tiledb_array_schema_create_default_blob_array(ctx, &blob_array_schema) ==
      TILEDB_ERR) {
    return TILEDB_ERR;
  }

  if (tiledb_array_alloc(ctx, array_uri, array) == TILEDB_ERR) {
    //expect tiledb_array_alloc() to have logged any necessary error.
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
  delete (*array)->array_; // release the 'default' allocated array, to be replaced
  (*array)->array_ = new (std::nothrow) tiledb::appl::BlobArray(uri_array, stg_mgr);
  if ((*array)->array_ == nullptr) {
    std::ostringstream errmsg;
    errmsg << "Failed to allocate TileDB blob_array object";
    auto st = Status_Error(errmsg.str());
    LOG_STATUS(st);
    save_error(ctx, st);
    tiledb_array_free(array); // already returning an error, ignore any error here.
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
    //auto cfg = config ? *config : stg_mgr->config();
    const char* encryption_type_str = nullptr;
    const char* encryption_key_str = nullptr;
    tiledb_encryption_type_t encryption_type = TILEDB_NO_ENCRYPTION;
    if (config) {
      if (SAVE_ERROR_CATCH(
              ctx, config->config_->get("sm.encryption_type", &encryption_type_str))) {
        return TILEDB_ERR;
      }
      // tiledb::sm::EncryptionType encryption_type;
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

    if (tiledb_array_create_with_key(
          ctx,
          array_uri,
          blob_array_schema,
          encryption_type,
          encryption_key_str,
            encryption_type_str ?
                static_cast<uint32_t>(strlen(encryption_key_str)) :
                0) == TILEDB_ERR) {
      tiledb_array_free(array);
      tiledb_array_schema_free(&blob_array_schema);
      *array = nullptr;
      return TILEDB_ERR;
    }
  } else {
    //TBD: verify that it is a blob_array entity!
    // attribute 'contents' of type TILEDB_BLOB
    // ...
    if (tiledb_array_close(ctx, *array) == TILEDB_ERR) {
      tiledb_array_free(array);
      *array = nullptr;
      tiledb_array_schema_free(&blob_array_schema);
      return TILEDB_ERR;
    }
  }

  // returning an allocated tiledb_array_t at '*array' having default blob_array schema;

  return TILEDB_OK;
}

TILEDB_EXPORT int32_t tiledb_array_as_file_import(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const char* input_uri_filename) {
  if (sanity_check(ctx, array) == TILEDB_ERR) {
    return TILEDB_ERR;
  }
  //TDB: Validate that the array rudimentary blob_array requirements...
  //... has attribute 'contents' of type TILEDB_BLOB

  int32_t is_open = 0;
  if (tiledb_array_is_open(ctx, array, &is_open) == TILEDB_ERR) {
    return TILEDB_ERR;
  }
  if (is_open) {
    tiledb_array_close(ctx, array);
  }
//  if (tiledb_array_open(ctx, array, TILEDB_WRITE) == TILEDB_ERR) {
//    return TILEDB_ERR;
//  }
  //auto stg_mgr = ctx->ctx_->storage_manager();
  //TBD: copy not desireable, was there a ref'ing method()
  auto cfg = array->array_->config();
  const char* encryption_type_str = nullptr;
  const char* encryption_key_str = nullptr;
  if (SAVE_ERROR_CATCH(
          ctx, cfg.get("sm.encryption_type", &encryption_type_str))) {
    return TILEDB_ERR;
  }
  // tiledb::sm::EncryptionType encryption_type;
  auto [st, et] = tiledb::sm::encryption_type_enum(encryption_type_str);
  if (!st.ok()) {
    return TILEDB_ERR;
  }
  auto encryption_type = et.value();
  if (SAVE_ERROR_CATCH(
          ctx, cfg.get("sm.encryption_key", &encryption_key_str))) {
    return TILEDB_ERR;
  }

  if (tiledb_array_open_with_key(ctx, array, TILEDB_WRITE, static_cast<tiledb_encryption_type_t>(encryption_type), encryption_key_str, static_cast<uint32_t>(strlen(encryption_key_str))) == TILEDB_ERR) {
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

  //TBD: close or leave open? (may have error (saving) or no error
  tiledb_array_close(ctx, array);

  return ret_stat;
}

TILEDB_EXPORT int32_t tiledb_array_as_file_export(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const char* output_uri_filename) {
  if (sanity_check(ctx, array) == TILEDB_ERR) {
    return TILEDB_ERR;
  }
  int32_t is_open;
  if (tiledb_array_is_open(ctx, array, &is_open) == TILEDB_ERR) {
    tiledb_array_close(ctx, array);
  }
  if (tiledb_array_open(ctx, array, TILEDB_READ) == TILEDB_ERR) {
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

  // TBD: close or leave open? (may have error (saving) or no error
  tiledb_array_close(ctx, array);

  return TILEDB_OK;
}

// stretch - but we need underlying schema created, tho' guess can do directly
// as done inside here.
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

#if 0
// original story API
TILEDB_EXPORT int32_t
tiledb_array_as_file_create(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_config_t* config) {
  if (sanity_check(ctx) == TILEDB_ERR) // ||
      //sanity_check(ctx, config) == TILEDB_ERR)
    return TILEDB_ERR;

  // Check array URI
  auto uri_array = tiledb::sm::URI(array_uri);
  if (uri_array.is_invalid()) {
    //auto st = Status_Error("Failed to create TileDB file object; Invalid URI");
    std::ostringstream errmsg;
    errmsg << "Failed to create TileDB blob_array object; Invalid array URI \""
           << uri_array.to_string() << "\"" ;
    auto st = Status_Error(errmsg.str());
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  tiledb::appl::BlobArraySchema* blob_array_schema;
  blob_array_schema = new (std::nothrow) tiledb::appl::BlobArraySchema();
  if (blob_array_schema == nullptr) {
    auto st = Status_Error("Failed to allocate TileDB blob_array schema object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

//  ctx->ctx_->storage_manager()->config()  ;
  auto stg_mgr = ctx->ctx_->storage_manager();
  tiledb::appl::BlobArray* blob_array;
  blob_array = new (std::nothrow) tiledb::appl::BlobArray(uri_array, stg_mgr);
  if(blob_array == nullptr) {
    std::ostringstream errmsg;
    errmsg << "Failed to allocate TileDB blob_array object for uri \""
           << "\""
           << uri_array.to_string() << "\"" ;
    auto st = Status_Error(errmsg.str());
    LOG_STATUS(st);
    save_error(ctx, st);
    delete blob_array_schema;
    blob_array_schema = nullptr;
    return TILEDB_OOM;
  }

  auto cfg = (config && config->config_) ? config->config_ : &stg_mgr->config();
  int32_t ret_stat = TILEDB_OK;  
  if(SAVE_ERROR_CATCH(ctx, blob_array->create( cfg ) )){
    ret_stat =  TILEDB_ERR;
  }
  
  delete blob_array;
  blob_array = nullptr;

  delete blob_array_schema;
  blob_array_schema = nullptr;
  
  return ret_stat;
}

TILEDB_EXPORT int32_t tiledb_array_as_file_import(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    const char* input_uri_filename,
    tiledb_config_t* config) {

  if (sanity_check(ctx) == TILEDB_ERR) // ||
      //sanity_check(ctx, config) == TILEDB_ERR)
    return TILEDB_ERR;

  // Check array URI
  auto uri_array = tiledb::sm::URI(array_uri);
  if (uri_array.is_invalid()) {
    std::ostringstream errmsg;
    errmsg << "Failed to create TileDB blob_array object; Invalid array URI \""
           << uri_array.to_string()
           << "\"";
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

  tiledb::appl::BlobArraySchema* blob_array_schema;
  blob_array_schema = new (std::nothrow) tiledb::appl::BlobArraySchema();
  if (blob_array_schema == nullptr) {
    auto st = Status_Error("Failed to allocate TileDB blob_array schema object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

//  ctx->ctx_->storage_manager()->config()  ;
  auto stg_mgr = ctx->ctx_->storage_manager();
  tiledb::appl::BlobArray* blob_array;
  blob_array = new (std::nothrow) tiledb::appl::BlobArray(uri_array, stg_mgr);
  if(blob_array == nullptr) {
    std::ostringstream errmsg;
    errmsg << "Failed to allocate TileDB blob_array object for uri \""
           << "\""
           << uri_array.to_string() << "\"" ;
    auto st = Status_Error(errmsg.str());
    LOG_STATUS(st);
    save_error(ctx, st);
    delete blob_array_schema;
    blob_array_schema = nullptr;
    return TILEDB_OOM;
  }

  auto cfg = (config && config->config_) ? config->config_ : &stg_mgr->config();
  int32_t ret_stat = TILEDB_OK;  
  //TBD: we need to create-or-'update'...
  //...  Status save_from_uri(const URI& file, const Config* config);
  const char *encryption_type_str = nullptr;
  const char *encryption_key_str = nullptr;
  if(SAVE_ERROR_CATCH(ctx, cfg->get("sm.encryption_type", &encryption_type_str))){
    return TILEDB_ERR;
  }
  //tiledb::sm::EncryptionType encryption_type;
  auto [st, et] = tiledb::sm::encryption_type_enum(encryption_type_str);
  if (!st.ok()) {
    return TILEDB_ERR;
  }
  auto encryption_type = et.value();
//  if (SAVE_ERROR_CATCH(
//          ctx,
//          tiledb_encryption_type_from_str(
//              encryption_type_str, &encryption_type))) {
//    return TILEDB_ERR;
//  }
  if (SAVE_ERROR_CATCH(
          ctx, cfg->get("sm.encryption_key", &encryption_key_str))) {
    return TILEDB_ERR;
  }
  if (!SAVE_ERROR_CATCH(
          ctx,
          blob_array->open(
              tiledb::sm::QueryType::WRITE,
              encryption_type,
              encryption_key_str,
              static_cast<uint32_t>(strlen(encryption_key_str))))) {
    if(SAVE_ERROR_CATCH(ctx, blob_array->save_from_uri(uri_filename, cfg) ) ) {
      ret_stat =  TILEDB_ERR;
    }
    blob_array->close();
  }
  else {
    //blob_array->set_config(*cfg);
    if (SAVE_ERROR_CATCH(ctx, blob_array->create_from_uri(uri_filename, cfg))) {
      ret_stat = TILEDB_ERR;
    } else if(!SAVE_ERROR_CATCH(
          ctx,
          blob_array->open(
              tiledb::sm::QueryType::WRITE,
              encryption_type,
              encryption_key_str,
              static_cast<uint32_t>(strlen(encryption_key_str))))) {
        if (SAVE_ERROR_CATCH(
                ctx, blob_array->save_from_uri(uri_filename, cfg))) {
          ret_stat = TILEDB_ERR;
        }
        blob_array->close();
      }
    else {
      ret_stat = TILEDB_ERR;
    }
  }
  
  delete blob_array;
  blob_array = nullptr;

  delete blob_array_schema;
  blob_array_schema = nullptr;
  
  return ret_stat;
}

TILEDB_EXPORT int32_t tiledb_array_as_file_export(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    const char* output_uri_filename,
    tiledb_config_t* config) {
  if (sanity_check(ctx) == TILEDB_ERR) // ||
      //sanity_check(ctx, config) == TILEDB_ERR)
    return TILEDB_ERR;

  // Check file URI
  auto uri_array = tiledb::sm::URI(array_uri);
  if (uri_array.is_invalid()) {
    
    //auto st = Status_Error("Failed to create TileDB file object; Invalid URI");
    std::ostringstream errmsg;
    errmsg << "Failed to create TileDB blob_array object; Invalid array URI \""
           << uri_array.to_string() << "\"";
    auto st = Status_Error(errmsg.str());
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  // Check file URI
  auto uri_filename = tiledb::sm::URI(output_uri_filename);
  if (uri_filename.is_invalid()) {
    //auto st = Status_Error("Failed to create TileDB file object; Invalid URI");
    std::ostringstream errmsg;
    errmsg << "Failed to create TileDB blob_array object; Invalid filename URI \""
           << uri_filename.to_string() << "\"";
    auto st = Status_Error(errmsg.str());
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  tiledb::sm::ArraySchema* blob_array_schema;
  blob_array_schema = new (std::nothrow) tiledb::appl::BlobArraySchema();
  if (blob_array_schema == nullptr) {
    auto st = Status_Error("Failed to allocate TileDB blob_array schema object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }
  
  auto stg_mgr = ctx->ctx_->storage_manager();
  tiledb::appl::BlobArray* blob_array;
  blob_array = new (std::nothrow) tiledb::appl::BlobArray(uri_array, stg_mgr);
  if (blob_array == nullptr) {
    std::ostringstream errmsg;
    errmsg << "Failed to allocate TileDB blob_array object for uri \""
           << "\"" << uri_array.to_string() << "\"";
    auto st = Status_Error(errmsg.str());
    LOG_STATUS(st);
    save_error(ctx, st);
    delete blob_array_schema;
    blob_array_schema = nullptr;
    return TILEDB_OOM;
  }

  auto cfg = (config && config->config_) ? config->config_ : &stg_mgr->config();
  int32_t ret_stat = TILEDB_OK;
  // TBD: we need to create-or-'update'...
  //...  Status save_from_uri(const URI& file, const Config* config);
  const char* encryption_type_str = nullptr;
  const char* encryption_key_str = nullptr;
  if (SAVE_ERROR_CATCH(
          ctx, cfg->get("sm.encryption_type", &encryption_type_str))) {
    return TILEDB_ERR;
  }
  // tiledb::sm::EncryptionType encryption_type;
  auto [st, et] = tiledb::sm::encryption_type_enum(encryption_type_str);
  if (!st.ok()) {
    return TILEDB_ERR;
  }
  auto encryption_type = et.value();
  //  if (SAVE_ERROR_CATCH(
  //          ctx,
  //          tiledb_encryption_type_from_str(
  //              encryption_type_str, &encryption_type))) {
  //    return TILEDB_ERR;
  //  }
  if (SAVE_ERROR_CATCH(
          ctx, cfg->get("sm.encryption_key", &encryption_key_str))) {
    return TILEDB_ERR;
  }
  if (!SAVE_ERROR_CATCH(
          ctx,
          blob_array->open(
              tiledb::sm::QueryType::READ,
              encryption_type,
              encryption_key_str,
              static_cast<uint32_t>(strlen(encryption_key_str))))) {
    if (SAVE_ERROR_CATCH(ctx, blob_array->export_to_uri(uri_filename, cfg))) {
      ret_stat = TILEDB_ERR;
    }
  } else {
    ret_stat = TILEDB_ERR;
  }

  delete blob_array;
  blob_array = nullptr;
  delete blob_array_schema;
  blob_array_schema = nullptr;

  // Success
  return TILEDB_OK;
}

#endif
#else
//Seth's preliminary implementation.
#include "tiledb/sm/c_api/tiledb_experimental.h"
#include "tiledb/sm/c_api/tiledb_helpers.h"
#include "tiledb/sm/file/file.h"

int32_t tiledb_file_alloc(
    tiledb_ctx_t* ctx, const char* array_uri, tiledb_file_t** file) {
  if (sanity_check(ctx) == TILEDB_ERR) {
    *file = nullptr;
    return TILEDB_ERR;
  }

  // Create file struct
  *file = new (std::nothrow) tiledb_file_t;
  if (*file == nullptr) {
    auto st = Status_Error(
        "Failed to create TileDB file object; Memory allocation error");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Check file URI
  auto uri = tiledb::sm::URI(array_uri);
  if (uri.is_invalid()) {
    auto st = Status_Error("Failed to create TileDB file object; Invalid URI");
    delete *file;
    *file = nullptr;
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  // Allocate an file object
  (*file)->file_ =
      new (std::nothrow) tiledb::sm::File(uri, ctx->ctx_->storage_manager());
  if ((*file)->file_ == nullptr) {
    delete *file;
    *file = nullptr;
    auto st = Status_Error(
        "Failed to create TileDB file object; Memory allocation "
        "error");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Success
  return TILEDB_OK;
}

void tiledb_file_free(tiledb_file_t** file) {
  if (file != nullptr && *file != nullptr) {
    delete (*file)->file_;
    delete (*file);
    *file = nullptr;
  }
}

int32_t tiledb_file_set_config(
    tiledb_ctx_t* ctx, tiledb_file_t* file, tiledb_config_t* config) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, file) == TILEDB_ERR ||
      sanity_check(ctx, config) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(ctx, file->file_->set_config(*(config->config_))))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_file_get_config(
    tiledb_ctx_t* ctx, tiledb_file_t* file, tiledb_config_t** config) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, file) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create a new config struct
  *config = new (std::nothrow) tiledb_config_t;
  if (*config == nullptr)
    return TILEDB_OOM;

  // Create storage manager
  (*config)->config_ = new (std::nothrow) tiledb::sm::Config();
  if ((*config)->config_ == nullptr) {
    delete (*config);
    *config = nullptr;
    return TILEDB_OOM;
  }

  *((*config)->config_) = file->file_->config();

  // Success
  return TILEDB_OK;
}

int32_t tiledb_file_create_default(
    tiledb_ctx_t* ctx, tiledb_file_t* file, tiledb_config_t* config) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, file) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  if (SAVE_ERROR_CATCH(
          ctx,
          file->file_->create(
              config ? config->config_ :
                       &ctx->ctx_->storage_manager()->config()))) {
    return TILEDB_ERR;
  }

  return TILEDB_OK;
}

int32_t tiledb_file_create_from_uri(
    tiledb_ctx_t* ctx,
    tiledb_file_t* file,
    const char* input_uri,
    tiledb_config_t* config) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, file) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  tiledb::sm::URI uri(input_uri);
  if (uri.is_invalid()) {
    auto st = Status_Error(
        "Failed to create file from path; Invalid input file URI");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  if (SAVE_ERROR_CATCH(
          ctx,
          file->file_->create_from_uri(
              uri,
              config ? config->config_ :
                       &ctx->ctx_->storage_manager()->config()))) {
    return TILEDB_ERR;
  }

  return TILEDB_OK;
}

int32_t tiledb_file_create_from_vfs_fh(
    tiledb_ctx_t* ctx,
    tiledb_file_t* file,
    tiledb_vfs_fh_t* input,
    tiledb_config_t* config) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, file) == TILEDB_ERR ||
      sanity_check(ctx, input) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  if (SAVE_ERROR_CATCH(
          ctx,
          file->file_->create_from_vfs_fh(
              input->vfs_fh_,
              config ? config->config_ :
                       &ctx->ctx_->storage_manager()->config()))) {
    return TILEDB_ERR;
  }

  return TILEDB_OK;
}

int32_t tiledb_file_store_fh(
    tiledb_ctx_t* ctx, tiledb_file_t* file, FILE* in, tiledb_config_t* config) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, file) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  if (SAVE_ERROR_CATCH(
          ctx,
          file->file_->save_from_file_handle(
              in,
              config ? config->config_ :
                       &ctx->ctx_->storage_manager()->config()))) {
    return TILEDB_ERR;
  }

  return TILEDB_OK;
}

int32_t tiledb_file_store_buffer(
    tiledb_ctx_t* ctx,
    tiledb_file_t* file,
    void* bytes,
    uint64_t size,
    tiledb_config_t* config) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, file) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  if (SAVE_ERROR_CATCH(
          ctx,
          file->file_->save_from_buffer(
              bytes,
              size,
              config ? config->config_ :
                       &ctx->ctx_->storage_manager()->config()))) {
    return TILEDB_ERR;
  }

  return TILEDB_OK;
}

int32_t tiledb_file_store_uri(
    tiledb_ctx_t* ctx,
    tiledb_file_t* file,
    const char* input_uri,
    tiledb_config_t* config) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, file) == TILEDB_ERR) {
    return TILEDB_ERR;
  }
  tiledb::sm::URI uri(input_uri);
  if (uri.is_invalid()) {
    auto st = Status_Error(
        "Failed to create file from path; Invalid input file URI");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  if (SAVE_ERROR_CATCH(
          ctx,
          file->file_->save_from_uri(
              uri,
              config ? config->config_ :
                       &ctx->ctx_->storage_manager()->config()))) {
    return TILEDB_ERR;
  }

  return TILEDB_OK;
}

int32_t tiledb_file_store_vfs_fh(
    tiledb_ctx_t* ctx,
    tiledb_file_t* file,
    tiledb_vfs_fh_t* input,
    tiledb_config_t* config) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, file) == TILEDB_ERR ||
      sanity_check(ctx, input) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  if (SAVE_ERROR_CATCH(
          ctx,
          file->file_->save_from_vfs_fh(
              input->vfs_fh_,
              config ? config->config_ :
                       &ctx->ctx_->storage_manager()->config()))) {
    return TILEDB_ERR;
  }

  return TILEDB_OK;
}

int32_t tiledb_file_get_mime_type(
    tiledb_ctx_t* ctx, tiledb_file_t* file, const char** mime, uint32_t* size) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, file) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  if (SAVE_ERROR_CATCH(ctx, file->file_->mime_type(mime, size))) {
    return TILEDB_ERR;
  }

  return TILEDB_OK;
}

int32_t tiledb_file_get_mime_encoding(
    tiledb_ctx_t* ctx, tiledb_file_t* file, const char** mime, uint32_t* size) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, file) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  if (SAVE_ERROR_CATCH(ctx, file->file_->mime_encoding(mime, size))) {
    return TILEDB_ERR;
  }

  return TILEDB_OK;
}

int32_t tiledb_file_get_original_name(
    tiledb_ctx_t* ctx, tiledb_file_t* file, const char** name, uint32_t* size) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, file) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  if (SAVE_ERROR_CATCH(ctx, file->file_->original_name(name, size))) {
    return TILEDB_ERR;
  }

  return TILEDB_OK;
}

int32_t tiledb_file_get_extension(
    tiledb_ctx_t* ctx, tiledb_file_t* file, const char** ext, uint32_t* size) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, file) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  if (SAVE_ERROR_CATCH(ctx, file->file_->file_extension(ext, size))) {
    return TILEDB_ERR;
  }

  return TILEDB_OK;
}

int32_t tiledb_file_get_schema(
    tiledb_ctx_t* ctx,
    tiledb_file_t* file,
    tiledb_array_schema_t** array_schema) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, file) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  *array_schema = new (std::nothrow) tiledb_array_schema_t;
  if (*array_schema == nullptr) {
    auto st = Status_Error("Failed to allocate TileDB array schema");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Get schema
  auto schema = (tiledb::sm::ArraySchema*)nullptr;
  if (SAVE_ERROR_CATCH(ctx, file->file_->get_array_schema(&schema))) {
    delete *array_schema;
    *array_schema = nullptr;
    return TILEDB_ERR;
  }

  (*array_schema)->array_schema_ =
      new (std::nothrow) tiledb::sm::ArraySchema(schema);

  return TILEDB_OK;
}

int32_t tiledb_file_export_fh(
    tiledb_ctx_t* ctx,
    tiledb_file_t* file,
    FILE* out,
    tiledb_config_t* config) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, file) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  if (SAVE_ERROR_CATCH(
          ctx,
          file->file_->export_to_file_handle(
              out,
              config ? config->config_ :
                       &ctx->ctx_->storage_manager()->config()))) {
    return TILEDB_ERR;
  }

  return TILEDB_OK;
}

int32_t tiledb_file_export_buffer(
    tiledb_ctx_t* ctx,
    tiledb_file_t* file,
    void* bytes,
    uint64_t* size,
    uint64_t file_offset,
    tiledb_config_t* config) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, file) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  if (SAVE_ERROR_CATCH(
          ctx,
          file->file_->export_to_buffer(
              bytes,
              size,
              file_offset,
              config ? config->config_ :
                       &ctx->ctx_->storage_manager()->config()))) {
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

int32_t tiledb_file_export_uri(
    tiledb_ctx_t* ctx,
    tiledb_file_t* file,
    const char* output_uri,
    tiledb_config_t* config) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, file) == TILEDB_ERR) {
    return TILEDB_ERR;
  }
  tiledb::sm::URI uri(output_uri);
  if (uri.is_invalid()) {
    auto st =
        Status_Error("Failed to export file to path; Invalid output file URI");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  if (SAVE_ERROR_CATCH(
          ctx,
          file->file_->export_to_uri(
              uri,
              config ? config->config_ :
                       &ctx->ctx_->storage_manager()->config()))) {
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

int32_t tiledb_file_export_vfs_fh(
    tiledb_ctx_t* ctx,
    tiledb_file_t* file,
    tiledb_vfs_fh_t* output,
    tiledb_config_t* config) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, file) == TILEDB_ERR ||
      sanity_check(ctx, output) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  if (SAVE_ERROR_CATCH(
          ctx,
          file->file_->export_to_vfs_fh(
              output->vfs_fh_,
              config ? config->config_ :
                       &ctx->ctx_->storage_manager()->config()))) {
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

int32_t tiledb_file_set_open_timestamp_start(
    tiledb_ctx_t* ctx, tiledb_file_t* file, uint64_t timestamp_start) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, file) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(ctx, file->file_->set_timestamp_start(timestamp_start)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_file_set_open_timestamp_end(
    tiledb_ctx_t* ctx, tiledb_file_t* file, uint64_t timestamp_end) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, file) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(ctx, file->file_->set_timestamp_end(timestamp_end)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_file_get_open_timestamp_start(
    tiledb_ctx_t* ctx, tiledb_file_t* file, uint64_t* timestamp_start) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, file) == TILEDB_ERR)
    return TILEDB_ERR;

  *timestamp_start = file->file_->timestamp_start();

  return TILEDB_OK;
}

int32_t tiledb_file_get_open_timestamp_end(
    tiledb_ctx_t* ctx, tiledb_file_t* file, uint64_t* timestamp_end) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, file) == TILEDB_ERR)
    return TILEDB_ERR;

  *timestamp_end = file->file_->timestamp_end_opened_at();

  return TILEDB_OK;
}

int32_t tiledb_file_open(
    tiledb_ctx_t* ctx, tiledb_file_t* file, tiledb_query_type_t query_type) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, file) == TILEDB_ERR)
    return TILEDB_ERR;

  // Open file
  if (SAVE_ERROR_CATCH(
          ctx,
          file->file_->open(
              static_cast<tiledb::sm::QueryType>(query_type),
              static_cast<tiledb::sm::EncryptionType>(TILEDB_NO_ENCRYPTION),
              nullptr,
              0)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_file_close(tiledb_ctx_t* ctx, tiledb_file_t* file) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, file) == TILEDB_ERR)
    return TILEDB_ERR;

  // Close file
  if (SAVE_ERROR_CATCH(ctx, file->file_->close()))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_file_get_size(
    tiledb_ctx_t* ctx, tiledb_file_t* file, uint64_t* size) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, file) == TILEDB_ERR)
    return TILEDB_ERR;

  // Get file size
  if (SAVE_ERROR_CATCH(ctx, file->file_->size(size)))
    return TILEDB_ERR;

  return TILEDB_OK;
}
#endif
