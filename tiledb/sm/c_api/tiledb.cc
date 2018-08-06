/**
 * @file   tiledb.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
#include "tiledb/rest/capnp/array.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/cpp_api/core_interface.h"
#include "tiledb/sm/filesystem/vfs_file_handle.h"
#include "tiledb/sm/filter/compression_filter.h"
#include "tiledb/sm/filter/filter_pipeline.h"
#include "tiledb/sm/kv/kv.h"
#include "tiledb/sm/kv/kv_item.h"
#include "tiledb/sm/kv/kv_iter.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/stats.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/sm/storage_manager/config.h"
#include "tiledb/sm/storage_manager/config_iter.h"
#include "tiledb/sm/storage_manager/context.h"

#include <map>
#include <sstream>

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
/*           TILEDB TYPES            */
/* ********************************* */

struct tiledb_array_t {
  tiledb::sm::Array* array_ = nullptr;
};

struct tiledb_config_t {
  tiledb::sm::Config* config_ = nullptr;
};

struct tiledb_config_iter_t {
  tiledb::sm::ConfigIter* config_iter_ = nullptr;
};

struct tiledb_ctx_t {
  tiledb::sm::Context* ctx_ = nullptr;
};

struct tiledb_error_t {
  std::string errmsg_;
};

struct tiledb_attribute_t {
  tiledb::sm::Attribute* attr_ = nullptr;
};

struct tiledb_array_schema_t {
  tiledb::sm::ArraySchema* array_schema_ = nullptr;
};

struct tiledb_dimension_t {
  tiledb::sm::Dimension* dim_ = nullptr;
};

struct tiledb_domain_t {
  tiledb::sm::Domain* domain_ = nullptr;
};

struct tiledb_filter_t {
  tiledb::sm::Filter* filter_ = nullptr;
};

struct tiledb_filter_list_t {
  tiledb::sm::FilterPipeline* pipeline_ = nullptr;
};

struct tiledb_query_t {
  tiledb::sm::Query* query_ = nullptr;
};

struct tiledb_kv_schema_t {
  tiledb::sm::ArraySchema* array_schema_ = nullptr;
};

struct tiledb_kv_t {
  tiledb::sm::KV* kv_ = nullptr;
};

struct tiledb_kv_item_t {
  tiledb::sm::KVItem* kv_item_ = nullptr;
};

struct tiledb_kv_iter_t {
  tiledb::sm::KVIter* kv_iter_ = nullptr;
};

struct tiledb_vfs_t {
  tiledb::sm::VFS* vfs_ = nullptr;
};

struct tiledb_vfs_fh_t {
  tiledb::sm::VFSFileHandle* vfs_fh_ = nullptr;
};

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

inline int32_t sanity_check(
    tiledb_ctx_t* ctx, const tiledb_kv_schema_t* kv_schema) {
  if (kv_schema == nullptr || kv_schema->array_schema_ == nullptr) {
    auto st =
        tiledb::sm::Status::Error("Invalid TileDB key-value schema object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int32_t sanity_check(tiledb_ctx_t* ctx, const tiledb_kv_t* kv) {
  if (kv == nullptr || kv->kv_ == nullptr) {
    auto st =
        tiledb::sm::Status::Error("Invalid TileDB key-value store object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int32_t sanity_check(
    tiledb_ctx_t* ctx, const tiledb_kv_iter_t* kv_iter) {
  if (kv_iter == nullptr || kv_iter->kv_iter_ == nullptr) {
    auto st =
        tiledb::sm::Status::Error("Invalid TileDB key-value iterator object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int32_t sanity_check(
    tiledb_ctx_t* ctx, const tiledb_kv_item_t* kv_item) {
  if (kv_item == nullptr || kv_item->kv_item_ == nullptr) {
    auto st = tiledb::sm::Status::Error("Invalid TileDB key-value item object");
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

int32_t tiledb_attribute_set_compressor(
    tiledb_ctx_t* ctx,
    tiledb_attribute_t* attr,
    tiledb_compressor_t compressor,
    int32_t compression_level) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, attr) == TILEDB_ERR)
    return TILEDB_ERR;
  attr->attr_->set_compressor(static_cast<tiledb::sm::Compressor>(compressor));
  attr->attr_->set_compression_level(compression_level);
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

  // Hide anonymous attribute name from user
  if (attr->attr_->is_anonymous())
    *name = "";
  else
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
      new (std::nothrow) tiledb::sm::FilterPipeline(*attr->attr_->filters());
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

int32_t tiledb_attribute_get_compressor(
    tiledb_ctx_t* ctx,
    const tiledb_attribute_t* attr,
    tiledb_compressor_t* compressor,
    int32_t* compression_level) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, attr) == TILEDB_ERR)
    return TILEDB_ERR;
  *compressor = static_cast<tiledb_compressor_t>(attr->attr_->compressor());
  *compression_level = attr->attr_->compression_level();
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
  *type = static_cast<tiledb_datatype_t>(domain->domain_->type());
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
    tiledb_ctx_t* ctx, const tiledb_dimension_t* dim, void** domain) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, dim) == TILEDB_ERR)
    return TILEDB_ERR;
  *domain = dim->dim_->domain();
  return TILEDB_OK;
}

int32_t tiledb_dimension_get_tile_extent(
    tiledb_ctx_t* ctx, const tiledb_dimension_t* dim, void** tile_extent) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, dim) == TILEDB_ERR)
    return TILEDB_ERR;
  *tile_extent = dim->dim_->tile_extent();
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
  const tiledb::sm::Dimension* found_dim = nullptr;
  if (name_string.empty()) {  // anonymous dimension
    bool found_anonymous = false;
    for (uint32_t i = 0; i < ndim; i++) {
      auto dim = domain->domain_->dimension(i);
      if (dim->is_anonymous()) {
        if (found_anonymous) {
          tiledb::sm::Status st = tiledb::sm::Status::Error(
              "Dimension from name is ambiguous when "
              "there are multiple anonymous "
              "dimensions; Use index instead");
          LOG_STATUS(st);
          save_error(ctx, st);
          return TILEDB_ERR;
        }
        found_anonymous = true;
        found_dim = dim;
      }
    }
  } else {
    found_dim = domain->domain_->dimension(name_string);
  }
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

int32_t tiledb_array_schema_set_coords_compressor(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_compressor_t compressor,
    int32_t compression_level) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  array_schema->array_schema_->set_coords_compressor(
      static_cast<tiledb::sm::Compressor>(compressor));
  array_schema->array_schema_->set_coords_compression_level(compression_level);
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

int32_t tiledb_array_schema_set_offsets_compressor(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_compressor_t compressor,
    int32_t compression_level) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  array_schema->array_schema_->set_cell_var_offsets_compressor(
      static_cast<tiledb::sm::Compressor>(compressor));
  array_schema->array_schema_->set_cell_var_offsets_compression_level(
      compression_level);
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
  bool in_cache;
  if (SAVE_ERROR_CATCH(
          ctx,
          storage_manager->load_array_schema(
              tiledb::sm::URI(array_uri),
              tiledb::sm::ObjectType::ARRAY,
              key,
              &((*array_schema)->array_schema_),
              &in_cache))) {
    delete *array_schema;
    return TILEDB_ERR;
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
  (*filter_list)->pipeline_ = new (std::nothrow) tiledb::sm::FilterPipeline(
      *array_schema->array_schema_->coords_filters());
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

int32_t tiledb_array_schema_get_coords_compressor(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    tiledb_compressor_t* compressor,
    int32_t* compression_level) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;

  *compressor = static_cast<tiledb_compressor_t>(
      array_schema->array_schema_->coords_compression());
  *compression_level = array_schema->array_schema_->coords_compression_level();
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
      *array_schema->array_schema_->cell_var_offsets_filters());
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

int32_t tiledb_array_schema_get_offsets_compressor(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    tiledb_compressor_t* compressor,
    int32_t* compression_level) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;

  *compressor = static_cast<tiledb_compressor_t>(
      array_schema->array_schema_->cell_var_offsets_compression());
  *compression_level =
      array_schema->array_schema_->cell_var_offsets_compression_level();
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

int tiledb_array_schema_serialize(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    tiledb_serialization_type_t serialize_type,
    char** serialized_string,
    uint64_t* serialized_string_length) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  // Sanity check
  if (sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;

  auto st = tiledb::rest::array_schema_serialize(
      array_schema->array_schema_,
      (tiledb::sm::SerializationType)serialize_type,
      serialized_string,
      serialized_string_length);
  if (!st.ok()) {
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

int tiledb_array_schema_deserialize(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t** array_schema,
    tiledb_serialization_type_t serialize_type,
    const char* serialized_string,
    const uint64_t serialized_string_length) {
  // Sanity check
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
  auto st = tiledb::rest::array_schema_deserialize(
      &((*array_schema)->array_schema_),
      (tiledb::sm::SerializationType)serialize_type,
      serialized_string,
      serialized_string_length);
  if (!st.ok()) {
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
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
    const char* attribute,
    void* buffer,
    uint64_t* buffer_size) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  // Normalize name
  std::string normalized_name;
  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::ArraySchema::attribute_name_normalized(
              attribute, &normalized_name)))
    return TILEDB_ERR;

  // Set attribute buffer
  if (SAVE_ERROR_CATCH(
          ctx, query->query_->set_buffer(normalized_name, buffer, buffer_size)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_query_set_buffer_var(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* attribute,
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

  // Normalize name
  std::string normalized_name;
  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::ArraySchema::attribute_name_normalized(
              attribute, &normalized_name)))
    return TILEDB_ERR;

  // Set attribute buffers
  if (SAVE_ERROR_CATCH(
          ctx,
          query->query_->set_buffer(
              normalized_name,
              buffer_off,
              buffer_off_size,
              buffer_val,
              buffer_val_size)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_query_get_buffer(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* attribute,
    void** buffer,
    uint64_t** buffer_size) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  // Set attribute buffer
  if (SAVE_ERROR_CATCH(
          ctx, query->query_->get_buffer(attribute, buffer, buffer_size)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_query_get_buffer_var(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* attribute,
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
              attribute,
              buffer_off,
              buffer_off_size,
              buffer_val,
              buffer_val_size)))
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
          array->array_->open_at(
              static_cast<tiledb::sm::QueryType>(query_type),
              static_cast<tiledb::sm::EncryptionType>(TILEDB_NO_ENCRYPTION),
              nullptr,
              0,
              timestamp)))
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
          array->array_->open_at(
              static_cast<tiledb::sm::QueryType>(query_type),
              static_cast<tiledb::sm::EncryptionType>(encryption_type),
              encryption_key,
              key_length,
              timestamp)))
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
  if (SAVE_ERROR_CATCH(ctx, array->array_->reopen_at(timestamp)))
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

  return TILEDB_OK;
}

int32_t tiledb_array_consolidate(tiledb_ctx_t* ctx, const char* array_uri) {
  return tiledb_array_consolidate_with_key(
      ctx, array_uri, TILEDB_NO_ENCRYPTION, nullptr, 0);
}

int32_t tiledb_array_consolidate_with_key(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    tiledb_encryption_type_t encryption_type,
    const void* encryption_key,
    uint32_t key_length) {
  // Sanity checks
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(
          ctx,
          ctx->ctx_->storage_manager()->array_consolidate(
              array_uri,
              static_cast<tiledb::sm::EncryptionType>(encryption_type),
              encryption_key,
              key_length)))
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

int32_t tiledb_array_max_buffer_size(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const char* attribute,
    const void* subarray,
    uint64_t* buffer_size) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(
          ctx,
          array->array_->get_max_buffer_size(attribute, subarray, buffer_size)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_array_max_buffer_size_var(
    tiledb_ctx_t* ctx,
    tiledb_array_t* array,
    const char* attribute,
    const void* subarray,
    uint64_t* buffer_off_size,
    uint64_t* buffer_val_size) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(
          ctx,
          array->array_->get_max_buffer_size(
              attribute, subarray, buffer_off_size, buffer_val_size)))
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
        "Cannot initiate ls; Invalid callback function");
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
/*         KEY-VALUE SCHEMA       */
/* ****************************** */

int32_t tiledb_kv_schema_alloc(
    tiledb_ctx_t* ctx, tiledb_kv_schema_t** kv_schema) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create kv schema struct
  *kv_schema = new (std::nothrow) tiledb_kv_schema_t;
  if (*kv_schema == nullptr) {
    tiledb::sm::Status st = tiledb::sm::Status::Error(
        "Failed to allocate TileDB key-value schema object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Create a new ArraySchema object
  (*kv_schema)->array_schema_ = new (std::nothrow) tiledb::sm::ArraySchema();
  if ((*kv_schema)->array_schema_ == nullptr) {
    delete *kv_schema;
    *kv_schema = nullptr;
    tiledb::sm::Status st = tiledb::sm::Status::Error(
        "Failed to allocate TileDB key-value schema object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Set ArraySchema as kv
  if (SAVE_ERROR_CATCH(ctx, ((*kv_schema)->array_schema_->set_as_kv()))) {
    delete (*kv_schema)->array_schema_;
    delete *kv_schema;
    *kv_schema = nullptr;
    return TILEDB_ERR;
  }

  // Success
  return TILEDB_OK;
}

void tiledb_kv_schema_free(tiledb_kv_schema_t** kv_schema) {
  if (kv_schema != nullptr && *kv_schema != nullptr) {
    delete (*kv_schema)->array_schema_;
    delete *kv_schema;
    *kv_schema = nullptr;
  }
}

int32_t tiledb_kv_schema_add_attribute(
    tiledb_ctx_t* ctx,
    tiledb_kv_schema_t* kv_schema,
    tiledb_attribute_t* attr) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, kv_schema) == TILEDB_ERR ||
      sanity_check(ctx, attr) == TILEDB_ERR)
    return TILEDB_ERR;
  if (SAVE_ERROR_CATCH(
          ctx, kv_schema->array_schema_->add_attribute(attr->attr_)))
    return TILEDB_ERR;
  return TILEDB_OK;
}

int32_t tiledb_kv_schema_set_capacity(
    tiledb_ctx_t* ctx, tiledb_kv_schema_t* kv_schema, uint64_t capacity) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, kv_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  kv_schema->array_schema_->set_capacity(capacity);
  return TILEDB_OK;
}

int32_t tiledb_kv_schema_check(
    tiledb_ctx_t* ctx, tiledb_kv_schema_t* kv_schema) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, kv_schema) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(ctx, kv_schema->array_schema_->check()))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_kv_schema_load(
    tiledb_ctx_t* ctx, const char* kv_uri, tiledb_kv_schema_t** kv_schema) {
  return tiledb_kv_schema_load_with_key(
      ctx, kv_uri, TILEDB_NO_ENCRYPTION, nullptr, 0, kv_schema);
}

int32_t tiledb_kv_schema_load_with_key(
    tiledb_ctx_t* ctx,
    const char* kv_uri,
    tiledb_encryption_type_t encryption_type,
    const void* encryption_key,
    uint32_t key_length,
    tiledb_kv_schema_t** kv_schema) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;
  // Create array schema
  *kv_schema = new (std::nothrow) tiledb_kv_schema_t;
  if (*kv_schema == nullptr) {
    tiledb::sm::Status st = tiledb::sm::Status::Error(
        "Failed to allocate TileDB key-value schema object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

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
  bool in_cache;
  if (SAVE_ERROR_CATCH(
          ctx,
          storage_manager->load_array_schema(
              tiledb::sm::URI(kv_uri),
              tiledb::sm::ObjectType::KEY_VALUE,
              key,
              &((*kv_schema)->array_schema_),
              &in_cache))) {
    delete *kv_schema;
    return TILEDB_ERR;
  }

  return TILEDB_OK;
}

int32_t tiledb_kv_schema_get_capacity(
    tiledb_ctx_t* ctx,
    const tiledb_kv_schema_t* kv_schema,
    uint64_t* capacity) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, kv_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  *capacity = kv_schema->array_schema_->capacity();
  return TILEDB_OK;
}

int32_t tiledb_kv_schema_get_attribute_num(
    tiledb_ctx_t* ctx,
    const tiledb_kv_schema_t* kv_schema,
    uint32_t* attribute_num) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, kv_schema) == TILEDB_ERR)
    return TILEDB_ERR;

  // -1 because of the special key attribute in the key-value schema
  *attribute_num = kv_schema->array_schema_->attribute_num() - 1;
  return TILEDB_OK;
}

int32_t tiledb_kv_schema_get_attribute_from_index(
    tiledb_ctx_t* ctx,
    const tiledb_kv_schema_t* kv_schema,
    uint32_t index,
    tiledb_attribute_t** attr) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, kv_schema) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  // Important! Skips the first special key attributes in the key-value schema
  index += 1;

  uint32_t attribute_num = kv_schema->array_schema_->attribute_num();
  if (attribute_num == 0) {
    *attr = nullptr;
    return TILEDB_OK;
  }
  if (index > attribute_num) {
    std::ostringstream errmsg;
    errmsg << "Attribute index: " << index << " exceeds number of attributes("
           << attribute_num << ") for array "
           << kv_schema->array_schema_->array_uri().to_string();
    auto st = tiledb::sm::Status::ArraySchemaError(errmsg.str());
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  auto found_attr = kv_schema->array_schema_->attribute(index);

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

int32_t tiledb_kv_schema_get_attribute_from_name(
    tiledb_ctx_t* ctx,
    const tiledb_kv_schema_t* kv_schema,
    const char* name,
    tiledb_attribute_t** attr) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, kv_schema) == TILEDB_ERR) {
    return TILEDB_ERR;
  }
  uint32_t attribute_num = kv_schema->array_schema_->attribute_num();
  if (attribute_num == 0) {
    *attr = nullptr;
    return TILEDB_OK;
  }
  std::string name_string(name);
  auto found_attr = kv_schema->array_schema_->attribute(name_string);
  if (found_attr == nullptr) {
    tiledb::sm::Status st = tiledb::sm::Status::ArraySchemaError(
        std::string("Attribute name: ") + name + " does not exist for array " +
        kv_schema->array_schema_->array_uri().to_string());
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

int32_t tiledb_kv_schema_dump(
    tiledb_ctx_t* ctx, const tiledb_kv_schema_t* kv_schema, FILE* out) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, kv_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  kv_schema->array_schema_->dump(out);
  return TILEDB_OK;
}

/* ****************************** */
/*          KEY-VALUE ITEM        */
/* ****************************** */

int32_t tiledb_kv_item_alloc(tiledb_ctx_t* ctx, tiledb_kv_item_t** kv_item) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create key-value item struct
  *kv_item = new (std::nothrow) tiledb_kv_item_t;
  if (*kv_item == nullptr) {
    tiledb::sm::Status st = tiledb::sm::Status::Error(
        "Failed to allocate TileDB key-value item object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Create a new key-value item object
  (*kv_item)->kv_item_ = new tiledb::sm::KVItem();
  if ((*kv_item)->kv_item_ == nullptr) {
    delete *kv_item;
    *kv_item = nullptr;
    tiledb::sm::Status st = tiledb::sm::Status::Error(
        "Failed to allocate TileDB key-value item object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Success
  return TILEDB_OK;
}

void tiledb_kv_item_free(tiledb_kv_item_t** kv_item) {
  if (kv_item != nullptr && *kv_item != nullptr) {
    delete (*kv_item)->kv_item_;
    delete *kv_item;
    *kv_item = nullptr;
  }
}

int32_t tiledb_kv_item_set_key(
    tiledb_ctx_t* ctx,
    tiledb_kv_item_t* kv_item,
    const void* key,
    tiledb_datatype_t key_type,
    uint64_t key_size) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, kv_item) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(
          ctx,
          kv_item->kv_item_->set_key(
              key, static_cast<tiledb::sm::Datatype>(key_type), key_size)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_kv_item_set_value(
    tiledb_ctx_t* ctx,
    tiledb_kv_item_t* kv_item,
    const char* attribute,
    const void* value,
    tiledb_datatype_t value_type,
    uint64_t value_size) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, kv_item) == TILEDB_ERR)
    return TILEDB_ERR;

  // Check for null name
  if (attribute == nullptr) {
    auto st = tiledb::sm::Status::Error(
        "Failed to set key-value item value; Attribute cannot be null.");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  // Normalize name
  std::string normalized_name;
  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::ArraySchema::attribute_name_normalized(
              attribute, &normalized_name)))
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(
          ctx,
          kv_item->kv_item_->set_value(
              normalized_name,
              value,
              static_cast<tiledb::sm::Datatype>(value_type),
              value_size)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_kv_item_get_key(
    tiledb_ctx_t* ctx,
    tiledb_kv_item_t* kv_item,
    const void** key,
    tiledb_datatype_t* key_type,
    uint64_t* key_size) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, kv_item) == TILEDB_ERR)
    return TILEDB_ERR;

  auto key_ptr = kv_item->kv_item_->key();
  *key = key_ptr->key_;
  *key_size = key_ptr->key_size_;
  *key_type = static_cast<tiledb_datatype_t>(key_ptr->key_type_);

  return TILEDB_OK;
}

int32_t tiledb_kv_item_get_value(
    tiledb_ctx_t* ctx,
    tiledb_kv_item_t* kv_item,
    const char* attribute,
    const void** value,
    tiledb_datatype_t* value_type,
    uint64_t* value_size) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, kv_item) == TILEDB_ERR)
    return TILEDB_ERR;

  if (attribute == nullptr) {
    auto st = tiledb::sm::Status::Error(
        "Failed to get key-value item value; Attribute cannot be null.");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  // Normalize name
  std::string normalized_name;
  if (SAVE_ERROR_CATCH(
          ctx,
          tiledb::sm::ArraySchema::attribute_name_normalized(
              attribute, &normalized_name)))
    return TILEDB_ERR;

  auto value_ptr = kv_item->kv_item_->value(normalized_name);
  if (value_ptr == nullptr) {
    auto st = tiledb::sm::Status::Error(
        std::string("Failed to get key-value item value for attribute '") +
        attribute + "'");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  *value = value_ptr->value_;
  *value_size = value_ptr->value_size_;
  *value_type = static_cast<tiledb_datatype_t>(value_ptr->value_type_);

  return TILEDB_OK;
}

int32_t tiledb_kv_get_item(
    tiledb_ctx_t* ctx,
    tiledb_kv_t* kv,
    const void* key,
    tiledb_datatype_t key_type,
    uint64_t key_size,
    tiledb_kv_item_t** kv_item) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create key-value item struct
  *kv_item = new (std::nothrow) tiledb_kv_item_t;
  if (*kv_item == nullptr) {
    tiledb::sm::Status st = tiledb::sm::Status::Error(
        "Failed to allocate TileDB key-value item object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }
  (*kv_item)->kv_item_ = nullptr;

  // Get item from the key-value store
  if (SAVE_ERROR_CATCH(
          ctx,
          kv->kv_->get_item(
              key,
              static_cast<tiledb::sm::Datatype>(key_type),
              key_size,
              &((*kv_item)->kv_item_))))
    return TILEDB_ERR;

  // Handle case where item does not exist
  if ((*kv_item)->kv_item_ == nullptr) {
    delete *kv_item;
    *kv_item = nullptr;
  }

  // Success
  return TILEDB_OK;
}

int32_t tiledb_kv_has_key(
    tiledb_ctx_t* ctx,
    tiledb_kv_t* kv,
    const void* key,
    tiledb_datatype_t key_type,
    uint64_t key_size,
    int32_t* has_key) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  // Check if key exists
  bool has_key_b;
  if (SAVE_ERROR_CATCH(
          ctx,
          kv->kv_->has_key(
              key,
              static_cast<tiledb::sm::Datatype>(key_type),
              key_size,
              &has_key_b)))
    return TILEDB_ERR;

  *has_key = (int32_t)has_key_b;

  // Success
  return TILEDB_OK;
}

/* ****************************** */
/*             KEY-VALUE          */
/* ****************************** */

int32_t tiledb_kv_create(
    tiledb_ctx_t* ctx,
    const char* kv_uri,
    const tiledb_kv_schema_t* kv_schema) {
  return tiledb_kv_create_with_key(
      ctx, kv_uri, kv_schema, TILEDB_NO_ENCRYPTION, nullptr, 0);
}

int32_t tiledb_kv_create_with_key(
    tiledb_ctx_t* ctx,
    const char* kv_uri,
    const tiledb_kv_schema_t* kv_schema,
    tiledb_encryption_type_t encryption_type,
    const void* encryption_key,
    uint32_t key_length) {
  // Sanity checks
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, kv_schema) == TILEDB_ERR)
    return TILEDB_ERR;

  // Check key-value name
  tiledb::sm::URI uri(kv_uri);
  if (uri.is_invalid()) {
    tiledb::sm::Status st = tiledb::sm::Status::Error(
        "Failed to create key-value store; Invalid array URI");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  // Create key
  tiledb::sm::EncryptionKey key;
  if (SAVE_ERROR_CATCH(
          ctx,
          key.set_key(
              static_cast<tiledb::sm::EncryptionType>(encryption_type),
              encryption_key,
              key_length)))
    return TILEDB_ERR;

  // Create the key-value store
  if (SAVE_ERROR_CATCH(
          ctx,
          ctx->ctx_->storage_manager()->array_create(
              uri, kv_schema->array_schema_, key)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_kv_consolidate(tiledb_ctx_t* ctx, const char* kv_uri) {
  return tiledb_kv_consolidate_with_key(
      ctx, kv_uri, TILEDB_NO_ENCRYPTION, nullptr, 0);
}

int32_t tiledb_kv_consolidate_with_key(
    tiledb_ctx_t* ctx,
    const char* kv_uri,
    tiledb_encryption_type_t encryption_type,
    const void* encryption_key,
    uint32_t key_length) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(
          ctx,
          ctx->ctx_->storage_manager()->array_consolidate(
              kv_uri,
              static_cast<tiledb::sm::EncryptionType>(encryption_type),
              encryption_key,
              key_length)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_kv_alloc(
    tiledb_ctx_t* ctx, const char* kv_uri, tiledb_kv_t** kv) {
  // Sanity checks
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  // Check URI
  auto uri = tiledb::sm::URI(kv_uri);
  if (uri.is_invalid()) {
    auto st = tiledb::sm::Status::Error(
        "Failed to create TileDB key-value store object; Invalid URI");
    *kv = nullptr;
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  // Create key-value store struct
  *kv = new (std::nothrow) tiledb_kv_t;
  if (*kv == nullptr) {
    tiledb::sm::Status st = tiledb::sm::Status::Error(
        "Failed to create TileDB key-value store object; Memory allocation "
        "error");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }
  (*kv)->kv_ = nullptr;

  // Create a new key-value store object
  (*kv)->kv_ =
      new (std::nothrow) tiledb::sm::KV(uri, ctx->ctx_->storage_manager());
  if ((*kv)->kv_ == nullptr) {
    delete *kv;
    *kv = nullptr;
    tiledb::sm::Status st = tiledb::sm::Status::Error(
        "Failed to create TileDB key-value store object; Memory allocation "
        "error");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  return TILEDB_OK;
}

int32_t tiledb_kv_open(
    tiledb_ctx_t* ctx, tiledb_kv_t* kv, tiledb_query_type_t query_type) {
  // Sanity checks
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, kv) == TILEDB_ERR)
    return TILEDB_ERR;

  // Prepare the key-value store
  if (SAVE_ERROR_CATCH(
          ctx,
          kv->kv_->open(
              static_cast<tiledb::sm::QueryType>(query_type),
              static_cast<tiledb::sm::EncryptionType>(TILEDB_NO_ENCRYPTION),
              nullptr,
              0)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_kv_open_at(
    tiledb_ctx_t* ctx,
    tiledb_kv_t* kv,
    tiledb_query_type_t query_type,
    uint64_t timestamp) {
  // Sanity checks
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, kv) == TILEDB_ERR)
    return TILEDB_ERR;

  // Prepare the key-value store
  if (SAVE_ERROR_CATCH(
          ctx,
          kv->kv_->open_at(
              static_cast<tiledb::sm::QueryType>(query_type),
              static_cast<tiledb::sm::EncryptionType>(TILEDB_NO_ENCRYPTION),
              nullptr,
              0,
              timestamp)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_kv_open_with_key(
    tiledb_ctx_t* ctx,
    tiledb_kv_t* kv,
    tiledb_query_type_t query_type,
    tiledb_encryption_type_t encryption_type,
    const void* encryption_key,
    uint32_t key_length) {
  // Sanity checks
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, kv) == TILEDB_ERR)
    return TILEDB_ERR;

  // Prepare the key-value store
  if (SAVE_ERROR_CATCH(
          ctx,
          kv->kv_->open(
              static_cast<tiledb::sm::QueryType>(query_type),
              static_cast<tiledb::sm::EncryptionType>(encryption_type),
              encryption_key,
              key_length)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_kv_open_at_with_key(
    tiledb_ctx_t* ctx,
    tiledb_kv_t* kv,
    tiledb_query_type_t query_type,
    tiledb_encryption_type_t encryption_type,
    const void* encryption_key,
    uint32_t key_length,
    uint64_t timestamp) {
  // Sanity checks
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, kv) == TILEDB_ERR)
    return TILEDB_ERR;

  // Prepare the key-value store
  if (SAVE_ERROR_CATCH(
          ctx,
          kv->kv_->open_at(
              static_cast<tiledb::sm::QueryType>(query_type),
              static_cast<tiledb::sm::EncryptionType>(encryption_type),
              encryption_key,
              key_length,
              timestamp)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_kv_is_open(
    tiledb_ctx_t* ctx, tiledb_kv_t* kv, int32_t* is_open) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, kv) == TILEDB_ERR)
    return TILEDB_ERR;

  *is_open = (int32_t)kv->kv_->is_open();

  return TILEDB_OK;
}

int32_t tiledb_kv_reopen(tiledb_ctx_t* ctx, tiledb_kv_t* kv) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, kv) == TILEDB_ERR)
    return TILEDB_ERR;

  // Re-open kv
  if (SAVE_ERROR_CATCH(ctx, kv->kv_->reopen()))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_kv_reopen_at(
    tiledb_ctx_t* ctx, tiledb_kv_t* kv, uint64_t timestamp) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, kv) == TILEDB_ERR)
    return TILEDB_ERR;

  // Re-open kv
  if (SAVE_ERROR_CATCH(ctx, kv->kv_->reopen_at(timestamp)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_kv_get_timestamp(
    tiledb_ctx_t* ctx, tiledb_kv_t* kv, uint64_t* timestamp) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, kv) == TILEDB_ERR)
    return TILEDB_ERR;

  *timestamp = kv->kv_->timestamp();

  return TILEDB_OK;
}

int32_t tiledb_kv_close(tiledb_ctx_t* ctx, tiledb_kv_t* kv) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, kv) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(ctx, kv->kv_->close()))
    return TILEDB_ERR;

  return TILEDB_OK;
}

void tiledb_kv_free(tiledb_kv_t** kv) {
  if (kv != nullptr && *kv != nullptr) {
    delete (*kv)->kv_;
    delete *kv;
    *kv = nullptr;
  }
}

int32_t tiledb_kv_get_schema(
    tiledb_ctx_t* ctx, tiledb_kv_t* kv, tiledb_kv_schema_t** kv_schema) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, kv) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  // Error if the kv is not open
  if (!kv->kv_->is_open()) {
    *kv_schema = nullptr;
    auto st = tiledb::sm::Status::Error("Cannot get KV schema; KV is not open");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  *kv_schema = new (std::nothrow) tiledb_kv_schema_t;
  if (*kv_schema == nullptr) {
    auto st = tiledb::sm::Status::Error("Failed to allocate TileDB KV schema");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  (*kv_schema)->array_schema_ = new (std::nothrow)
      tiledb::sm::ArraySchema(kv->kv_->array()->array_schema());

  return TILEDB_OK;
}

int32_t tiledb_kv_is_dirty(
    tiledb_ctx_t* ctx, tiledb_kv_t* kv, int32_t* is_dirty) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, kv) == TILEDB_ERR)
    return TILEDB_ERR;

  bool dirty;
  if (SAVE_ERROR_CATCH(ctx, kv->kv_->is_dirty(&dirty)))
    return TILEDB_ERR;

  *is_dirty = (int32_t)dirty;

  return TILEDB_OK;
}

int32_t tiledb_kv_add_item(
    tiledb_ctx_t* ctx, tiledb_kv_t* kv, tiledb_kv_item_t* kv_item) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, kv) == TILEDB_ERR ||
      sanity_check(ctx, kv_item) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(ctx, kv->kv_->add_item(kv_item->kv_item_)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_kv_flush(tiledb_ctx_t* ctx, tiledb_kv_t* kv) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, kv) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(ctx, kv->kv_->flush()))
    return TILEDB_ERR;

  return TILEDB_OK;
}

/* ****************************** */
/*          KEY-VALUE ITER        */
/* ****************************** */

int32_t tiledb_kv_iter_alloc(
    tiledb_ctx_t* ctx, tiledb_kv_t* kv, tiledb_kv_iter_t** kv_iter) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, kv) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create KVIter struct
  *kv_iter = new (std::nothrow) tiledb_kv_iter_t;
  if (*kv_iter == nullptr) {
    tiledb::sm::Status st = tiledb::sm::Status::Error(
        "Failed to allocate TileDB key-value iterator object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Create KVIter object
  (*kv_iter)->kv_iter_ =
      new (std::nothrow) tiledb::sm::KVIter(ctx->ctx_->storage_manager());
  if ((*kv_iter)->kv_iter_ == nullptr) {
    tiledb::sm::Status st = tiledb::sm::Status::Error(
        "Failed to allocate TileDB key-value iterator object");
    LOG_STATUS(st);
    save_error(ctx, st);
    delete *kv_iter;
    return TILEDB_OOM;
  }

  // Initialize KVIter object
  if (SAVE_ERROR_CATCH(ctx, (*kv_iter)->kv_iter_->init(kv->kv_))) {
    delete (*kv_iter)->kv_iter_;
    delete (*kv_iter);
    return TILEDB_ERR;
  }

  // Success
  return TILEDB_OK;
}

void tiledb_kv_iter_free(tiledb_kv_iter_t** kv_iter) {
  if (kv_iter != nullptr && *kv_iter == nullptr) {
    delete (*kv_iter)->kv_iter_;
    delete *kv_iter;
    *kv_iter = nullptr;
  }
}

int32_t tiledb_kv_iter_here(
    tiledb_ctx_t* ctx, tiledb_kv_iter_t* kv_iter, tiledb_kv_item_t** kv_item) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, kv_iter) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create key-value item struct
  *kv_item = new (std::nothrow) tiledb_kv_item_t;
  if (*kv_item == nullptr) {
    tiledb::sm::Status st = tiledb::sm::Status::Error(
        "Failed to allocate TileDB key-value item object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  if (SAVE_ERROR_CATCH(ctx, kv_iter->kv_iter_->here(&((*kv_item)->kv_item_)))) {
    tiledb_kv_item_free(kv_item);
    return TILEDB_ERR;
  }

  return TILEDB_OK;
}

int32_t tiledb_kv_iter_next(tiledb_ctx_t* ctx, tiledb_kv_iter_t* kv_iter) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, kv_iter) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(ctx, kv_iter->kv_iter_->next()))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int32_t tiledb_kv_iter_done(
    tiledb_ctx_t* ctx, tiledb_kv_iter_t* kv_iter, int32_t* done) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, kv_iter) == TILEDB_ERR)
    return TILEDB_ERR;

  *done = kv_iter->kv_iter_->done();

  return TILEDB_OK;
}

int32_t tiledb_kv_iter_reset(tiledb_ctx_t* ctx, tiledb_kv_iter_t* kv_iter) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, kv_iter) == TILEDB_ERR)
    return TILEDB_ERR;

  if (SAVE_ERROR_CATCH(ctx, kv_iter->kv_iter_->reset()))
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
  tiledb::sm::Config::VFSParams vfs_params;  // This will get default values
  if (config != nullptr)
    vfs_params = config->config_->vfs_params();

  if (SAVE_ERROR_CATCH(ctx, (*vfs)->vfs_->init(vfs_params))) {
    delete (*vfs)->vfs_;
    delete vfs;
    return TILEDB_ERR;
  }

  // Success
  return TILEDB_OK;
}

void tiledb_vfs_free(tiledb_vfs_t** vfs) {
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
