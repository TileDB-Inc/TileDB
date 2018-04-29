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
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/cpp_api/core_interface.h"
#include "tiledb/sm/kv/kv.h"
#include "tiledb/sm/kv/kv_item.h"
#include "tiledb/sm/kv/kv_iter.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/stats.h"
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/query/query.h"
#include "tiledb/sm/storage_manager/config.h"

#include <map>
#include <sstream>

/* ****************************** */
/*            CONSTANTS           */
/* ****************************** */

const char* tiledb_coords() {
  return tiledb::sm::constants::coords;
}

unsigned int tiledb_var_num() {
  return tiledb::sm::constants::var_num;
}

unsigned int tiledb_max_path() {
  return tiledb::sm::constants::path_max_len;
}

uint64_t tiledb_offset_size() {
  return tiledb::sm::constants::cell_var_offset_size;
}

uint64_t tiledb_datatype_size(tiledb_datatype_t type) {
  return tiledb::sm::datatype_size(static_cast<tiledb::sm::Datatype>(type));
}

/* ****************************** */
/*            VERSION             */
/* ****************************** */

void tiledb_version(int* major, int* minor, int* rev) {
  *major = tiledb::sm::constants::version[0];
  *minor = tiledb::sm::constants::version[1];
  *rev = tiledb::sm::constants::version[2];
}

/* ********************************* */
/*           TILEDB TYPES            */
/* ********************************* */

struct tiledb_config_t {
  tiledb::sm::Config* config_;
};

struct tiledb_config_iter_t {
  std::map<std::string, std::string> param_values_;
  std::map<std::string, std::string>::iterator it_;
};

struct tiledb_ctx_t {
  tiledb::sm::StorageManager* storage_manager_;
  tiledb::sm::Status* last_error_;
  std::mutex* mtx_;
};

struct tiledb_error_t {
  // Pointer to a copy of the last TileDB error associated with a given ctx
  const tiledb::sm::Status* status_;
  std::string* errmsg_;
};

struct tiledb_attribute_t {
  tiledb::sm::Attribute* attr_;
};

struct tiledb_array_schema_t {
  tiledb::sm::ArraySchema* array_schema_;
};

struct tiledb_dimension_t {
  tiledb::sm::Dimension* dim_;
};

struct tiledb_domain_t {
  tiledb::sm::Domain* domain_;
};

struct tiledb_query_t {
  tiledb::sm::Query* query_;
  bool finalized_;
};

struct tiledb_kv_schema_t {
  tiledb::sm::ArraySchema* array_schema_;
};

struct tiledb_kv_t {
  tiledb::sm::KV* kv_;
};

struct tiledb_kv_item_t {
  tiledb::sm::KVItem* kv_item_;
};

struct tiledb_kv_iter_t {
  tiledb::sm::KVIter* kv_iter_;
};

struct tiledb_vfs_t {
  tiledb::sm::VFS* vfs_;
};

struct tiledb_vfs_fh_t {
  tiledb::sm::URI uri_;
  bool is_closed_;
  tiledb::sm::VFS* vfs_;
  tiledb::sm::VFSMode mode_;
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
  {
    std::lock_guard<std::mutex> lock(*(ctx->mtx_));
    delete ctx->last_error_;
    ctx->last_error_ = new (std::nothrow) tiledb::sm::Status(st);
  }

  // There is an error
  return true;
}

static bool create_error(tiledb_error_t** error, const tiledb::sm::Status& st) {
  if (st.ok())
    return false;

  (*error) = new (std::nothrow) tiledb_error_t;
  if (*error == nullptr)
    return true;
  (*error)->status_ = new (std::nothrow) tiledb::sm::Status(st);
  if ((*error)->status_ == nullptr) {
    delete (*error);
    *error = nullptr;
    return true;
  }
  (*error)->errmsg_ =
      new (std::nothrow) std::string((*error)->status_->to_string());
  if ((*error)->errmsg_ == nullptr) {
    delete (*error)->status_;
    delete (*error);
    (*error) = nullptr;
  }

  return true;
}

inline int sanity_check(tiledb_config_t* config, tiledb_error_t** error) {
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

inline int sanity_check(
    tiledb_config_iter_t* config_iter, tiledb_error_t** error) {
  if (config_iter == nullptr) {
    auto st = tiledb::sm::Status::Error(
        "Cannot set config; Invalid config iterator object");
    LOG_STATUS(st);
    create_error(error, st);
    return TILEDB_ERR;
  }

  *error = nullptr;
  return TILEDB_OK;
}

inline int sanity_check(tiledb_ctx_t* ctx) {
  if (ctx == nullptr || ctx->mtx_ == nullptr)
    return TILEDB_ERR;
  if (ctx->storage_manager_ == nullptr) {
    auto st = tiledb::sm::Status::Error("Invalid TileDB context");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int sanity_check(tiledb_ctx_t* ctx, const tiledb_error_t* err) {
  if (err == nullptr || err->status_ == nullptr) {
    auto st = tiledb::sm::Status::Error("Invalid TileDB error object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int sanity_check(tiledb_ctx_t* ctx, const tiledb_attribute_t* attr) {
  if (attr == nullptr || attr->attr_ == nullptr) {
    auto st = tiledb::sm::Status::Error("Invalid TileDB attribute object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int sanity_check(tiledb_ctx_t* ctx, const tiledb_dimension_t* dim) {
  if (dim == nullptr || dim->dim_ == nullptr) {
    auto st = tiledb::sm::Status::Error("Invalid TileDB dimension object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int sanity_check(
    tiledb_ctx_t* ctx, const tiledb_array_schema_t* array_schema) {
  if (array_schema == nullptr || array_schema->array_schema_ == nullptr) {
    auto st = tiledb::sm::Status::Error("Invalid TileDB array schema object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int sanity_check(tiledb_ctx_t* ctx, const tiledb_domain_t* domain) {
  if (domain == nullptr || domain->domain_ == nullptr) {
    auto st = tiledb::sm::Status::Error("Invalid TileDB domain object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int sanity_check(tiledb_ctx_t* ctx, const tiledb_query_t* query) {
  if (query == nullptr || query->query_ == nullptr) {
    auto st = tiledb::sm::Status::Error("Invalid TileDB query object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int sanity_check(
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

inline int sanity_check(tiledb_ctx_t* ctx, const tiledb_kv_t* kv) {
  if (kv == nullptr || kv->kv_ == nullptr) {
    auto st =
        tiledb::sm::Status::Error("Invalid TileDB key-value store object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int sanity_check(tiledb_ctx_t* ctx, const tiledb_kv_iter_t* kv_iter) {
  if (kv_iter == nullptr || kv_iter->kv_iter_ == nullptr) {
    auto st =
        tiledb::sm::Status::Error("Invalid TileDB key-value iterator object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int sanity_check(tiledb_ctx_t* ctx, const tiledb_kv_item_t* kv_item) {
  if (kv_item == nullptr || kv_item->kv_item_ == nullptr) {
    auto st = tiledb::sm::Status::Error("Invalid TileDB key-value item object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int sanity_check(tiledb_ctx_t* ctx, const tiledb_vfs_t* vfs) {
  if (vfs == nullptr || vfs->vfs_ == nullptr) {
    auto st =
        tiledb::sm::Status::Error("Invalid TileDB virtual filesystem object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int sanity_check(tiledb_ctx_t* ctx, const tiledb_vfs_fh_t* fh) {
  if (fh == nullptr) {
    auto st = tiledb::sm::Status::Error(
        "Invalid TileDB virtual filesystem file handle");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

/* ********************************* */
/*              ERROR                */
/* ********************************* */

int tiledb_error_message(tiledb_error_t* err, const char** errmsg) {
  if (err == nullptr || err->status_ == nullptr)
    return TILEDB_ERR;
  if (err->status_->ok() || err->errmsg_ == nullptr)
    *errmsg = nullptr;
  else
    *errmsg = err->errmsg_->c_str();
  return TILEDB_OK;
}

int tiledb_error_free(tiledb_error_t** err) {
  if (err != nullptr && *err != nullptr) {
    delete (*err)->status_;
    delete (*err)->errmsg_;
    delete (*err);
    *err = nullptr;
  }

  return TILEDB_OK;
}

/* ****************************** */
/*            CONFIG              */
/* ****************************** */

int tiledb_config_create(tiledb_config_t** config, tiledb_error_t** error) {
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

int tiledb_config_free(tiledb_config_t** config) {
  if (config != nullptr && *config != nullptr) {
    delete (*config)->config_;
    delete (*config);
    *config = nullptr;
  }

  return TILEDB_OK;
}

int tiledb_config_set(
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

int tiledb_config_get(
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

int tiledb_config_load_from_file(
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

int tiledb_config_save_to_file(
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

int tiledb_config_unset(
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

int tiledb_config_iter_create(
    tiledb_config_t* config,
    tiledb_config_iter_t** config_iter,
    const char* prefix,
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
  (*config_iter)->param_values_ = config->config_->param_values(prefix_str);
  (*config_iter)->it_ = (*config_iter)->param_values_.begin();

  *error = nullptr;
  return TILEDB_OK;
}

int tiledb_config_iter_reset(
    tiledb_config_t* config,
    tiledb_config_iter_t* config_iter,
    const char* prefix,
    tiledb_error_t** error) {
  if (sanity_check(config, error) == TILEDB_ERR ||
      sanity_check(config_iter, error) == TILEDB_ERR)
    return TILEDB_ERR;

  std::string prefix_str = (prefix == nullptr) ? "" : std::string(prefix);
  config_iter->param_values_ = config->config_->param_values(prefix_str);
  config_iter->it_ = config_iter->param_values_.begin();

  *error = nullptr;
  return TILEDB_OK;
}

int tiledb_config_iter_free(tiledb_config_iter_t** config_iter) {
  if (config_iter != nullptr && *config_iter != nullptr) {
    delete *config_iter;
    *config_iter = nullptr;
  }

  return TILEDB_OK;
}

int tiledb_config_iter_here(
    tiledb_config_iter_t* config_iter,
    const char** param,
    const char** value,
    tiledb_error_t** error) {
  if (sanity_check(config_iter, error) == TILEDB_ERR)
    return TILEDB_ERR;

  if (config_iter->it_ == config_iter->param_values_.end()) {
    *param = nullptr;
    *value = nullptr;
  } else {
    *param = config_iter->it_->first.c_str();
    *value = config_iter->it_->second.c_str();
  }

  *error = nullptr;
  return TILEDB_OK;
}

int tiledb_config_iter_next(
    tiledb_config_iter_t* config_iter, tiledb_error_t** error) {
  if (sanity_check(config_iter, error) == TILEDB_ERR)
    return TILEDB_ERR;

  if (config_iter->it_ != config_iter->param_values_.end())
    config_iter->it_++;

  *error = nullptr;
  return TILEDB_OK;
}

int tiledb_config_iter_done(
    tiledb_config_iter_t* config_iter, int* done, tiledb_error_t** error) {
  if (sanity_check(config_iter, error) == TILEDB_ERR)
    return TILEDB_ERR;

  *done = config_iter->it_ != config_iter->param_values_.end() ? 0 : 1;

  *error = nullptr;
  return TILEDB_OK;
}

/* ****************************** */
/*            CONTEXT             */
/* ****************************** */

int tiledb_ctx_create(tiledb_ctx_t** ctx, tiledb_config_t* config) {
  if (config != nullptr && config->config_ == nullptr)
    return TILEDB_ERR;

  // Initialize context
  *ctx = new (std::nothrow) tiledb_ctx_t;
  if (*ctx == nullptr)
    return TILEDB_OOM;

  // Create mutex
  (*ctx)->mtx_ = new (std::nothrow) std::mutex();

  // Create storage manager
  (*ctx)->storage_manager_ = new (std::nothrow) tiledb::sm::StorageManager();
  if ((*ctx)->storage_manager_ == nullptr) {
    auto st = tiledb::sm::Status::Error(
        "Failed to allocate storage manager in TileDB context");
    LOG_STATUS(st);
    save_error(*ctx, st);
    return TILEDB_ERR;
  }

  // Initialize last error
  (*ctx)->last_error_ = nullptr;

  // Initialize storage manager
  auto conf =
      (config == nullptr) ? (tiledb::sm::Config*)nullptr : config->config_;
  if (save_error(*ctx, ((*ctx)->storage_manager_->init(conf)))) {
    delete (*ctx)->storage_manager_;
    (*ctx)->storage_manager_ = nullptr;
    return TILEDB_ERR;
  }

  // Success
  return TILEDB_OK;
}

int tiledb_ctx_free(tiledb_ctx_t** ctx) {
  if (ctx != nullptr && *ctx != nullptr) {
    delete (*ctx)->storage_manager_;
    delete (*ctx)->last_error_;
    delete (*ctx)->mtx_;
    delete (*ctx);
    *ctx = nullptr;
  }

  // Always succeeds
  return TILEDB_OK;
}

int tiledb_ctx_get_config(tiledb_ctx_t* ctx, tiledb_config_t** config) {
  // Create a new config struct
  *config = new (std::nothrow) tiledb_config_t;
  if (*config == nullptr)
    return TILEDB_OOM;

  (*config)->config_ = new (std::nothrow) tiledb::sm::Config();
  if ((*config)->config_ == nullptr) {
    delete (*config);
    return TILEDB_OOM;
  }

  *((*config)->config_) = ctx->storage_manager_->config();

  return TILEDB_OK;
}

int tiledb_ctx_get_last_error(tiledb_ctx_t* ctx, tiledb_error_t** err) {
  // sanity check
  if (ctx == nullptr || ctx->mtx_ == nullptr) {
    return TILEDB_ERR;
  }

  {
    std::lock_guard<std::mutex> lock(*(ctx->mtx_));

    // No last error
    if (ctx->last_error_ == nullptr) {
      *err = nullptr;
      return TILEDB_OK;
    }

    // Create error struct
    *err = new (std::nothrow) tiledb_error_t;
    if (*err == nullptr) {
      return TILEDB_OOM;
    }

    // Create status
    (*err)->status_ =
        new (std::nothrow) tiledb::sm::Status(*(ctx->last_error_));
    if ((*err)->status_ == nullptr) {
      delete *err;
      return TILEDB_OOM;
    }

    // Set error message
    (*err)->errmsg_ =
        new (std::nothrow) std::string((*err)->status_->to_string());
  }

  // Success
  return TILEDB_OK;
}

int tiledb_ctx_is_supported_fs(
    tiledb_ctx_t* ctx, tiledb_filesystem_t fs, int* is_supported) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  *is_supported = (int)ctx->storage_manager_->vfs()->supports_fs(
      static_cast<tiledb::sm::Filesystem>(fs));

  return TILEDB_OK;
}

/* ****************************** */
/*              GROUP             */
/* ****************************** */

int tiledb_group_create(tiledb_ctx_t* ctx, const char* group_uri) {
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
  if (save_error(ctx, ctx->storage_manager_->group_create(group_uri)))
    return TILEDB_ERR;

  // Success
  return TILEDB_OK;
}

/* ********************************* */
/*            ATTRIBUTE              */
/* ********************************* */

int tiledb_attribute_create(
    tiledb_ctx_t* ctx,
    tiledb_attribute_t** attr,
    const char* name,
    tiledb_datatype_t type) {
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

int tiledb_attribute_free(tiledb_ctx_t* ctx, tiledb_attribute_t** attr) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  if (attr != nullptr && *attr != nullptr) {
    delete (*attr)->attr_;
    delete (*attr);
    *attr = nullptr;
  }

  return TILEDB_OK;
}

int tiledb_attribute_set_compressor(
    tiledb_ctx_t* ctx,
    tiledb_attribute_t* attr,
    tiledb_compressor_t compressor,
    int compression_level) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, attr) == TILEDB_ERR)
    return TILEDB_ERR;
  attr->attr_->set_compressor(static_cast<tiledb::sm::Compressor>(compressor));
  attr->attr_->set_compression_level(compression_level);
  return TILEDB_OK;
}

int tiledb_attribute_set_cell_val_num(
    tiledb_ctx_t* ctx, tiledb_attribute_t* attr, unsigned int cell_val_num) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, attr) == TILEDB_ERR)
    return TILEDB_ERR;
  if (save_error(ctx, attr->attr_->set_cell_val_num(cell_val_num)))
    return TILEDB_ERR;
  return TILEDB_OK;
}

int tiledb_attribute_get_name(
    tiledb_ctx_t* ctx, const tiledb_attribute_t* attr, const char** name) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, attr) == TILEDB_ERR)
    return TILEDB_ERR;
  *name = attr->attr_->name().c_str();
  return TILEDB_OK;
}

int tiledb_attribute_get_type(
    tiledb_ctx_t* ctx,
    const tiledb_attribute_t* attr,
    tiledb_datatype_t* type) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, attr) == TILEDB_ERR)
    return TILEDB_ERR;
  *type = static_cast<tiledb_datatype_t>(attr->attr_->type());
  return TILEDB_OK;
}

int tiledb_attribute_get_compressor(
    tiledb_ctx_t* ctx,
    const tiledb_attribute_t* attr,
    tiledb_compressor_t* compressor,
    int* compression_level) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, attr) == TILEDB_ERR)
    return TILEDB_ERR;
  *compressor = static_cast<tiledb_compressor_t>(attr->attr_->compressor());
  *compression_level = attr->attr_->compression_level();
  return TILEDB_OK;
}

int tiledb_attribute_get_cell_val_num(
    tiledb_ctx_t* ctx,
    const tiledb_attribute_t* attr,
    unsigned int* cell_val_num) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, attr) == TILEDB_ERR)
    return TILEDB_ERR;
  *cell_val_num = attr->attr_->cell_val_num();
  return TILEDB_OK;
}

int tiledb_attribute_get_cell_size(
    tiledb_ctx_t* ctx, const tiledb_attribute_t* attr, uint64_t* cell_size) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, attr) == TILEDB_ERR)
    return TILEDB_ERR;
  *cell_size = attr->attr_->cell_size();
  return TILEDB_OK;
}

int tiledb_attribute_dump(
    tiledb_ctx_t* ctx, const tiledb_attribute_t* attr, FILE* out) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, attr) == TILEDB_ERR)
    return TILEDB_ERR;
  attr->attr_->dump(out);
  return TILEDB_OK;
}

/* ********************************* */
/*              DOMAIN               */
/* ********************************* */

int tiledb_domain_create(tiledb_ctx_t* ctx, tiledb_domain_t** domain) {
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

int tiledb_domain_free(tiledb_ctx_t* ctx, tiledb_domain_t** domain) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  if (domain != nullptr && *domain != nullptr) {
    delete (*domain)->domain_;
    delete *domain;
    *domain = nullptr;
  }

  return TILEDB_OK;
}

int tiledb_domain_get_type(
    tiledb_ctx_t* ctx, const tiledb_domain_t* domain, tiledb_datatype_t* type) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, domain) == TILEDB_ERR)
    return TILEDB_ERR;
  *type = static_cast<tiledb_datatype_t>(domain->domain_->type());
  return TILEDB_OK;
}

int tiledb_domain_get_rank(
    tiledb_ctx_t* ctx, const tiledb_domain_t* domain, unsigned int* rank) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, domain) == TILEDB_ERR)
    return TILEDB_ERR;
  *rank = domain->domain_->dim_num();
  return TILEDB_OK;
}

int tiledb_domain_add_dimension(
    tiledb_ctx_t* ctx, tiledb_domain_t* domain, tiledb_dimension_t* dim) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, domain) == TILEDB_ERR)
    return TILEDB_ERR;

  if (save_error(ctx, domain->domain_->add_dimension(dim->dim_)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_domain_dump(
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

int tiledb_dimension_create(
    tiledb_ctx_t* ctx,
    tiledb_dimension_t** dim,
    const char* name,
    tiledb_datatype_t type,
    const void* dim_domain,
    const void* tile_extent) {
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
  if (save_error(ctx, (*dim)->dim_->set_domain(dim_domain))) {
    delete (*dim)->dim_;
    delete *dim;
    return TILEDB_ERR;
  }

  // Set tile extent
  if (save_error(ctx, (*dim)->dim_->set_tile_extent(tile_extent))) {
    delete (*dim)->dim_;
    delete *dim;
    return TILEDB_ERR;
  }

  // Success
  return TILEDB_OK;
}

int tiledb_dimension_free(tiledb_ctx_t* ctx, tiledb_dimension_t** dim) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  if (dim != nullptr && *dim != nullptr) {
    delete (*dim)->dim_;
    delete *dim;
    *dim = nullptr;
  }

  return TILEDB_OK;
}

int tiledb_dimension_get_name(
    tiledb_ctx_t* ctx, const tiledb_dimension_t* dim, const char** name) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, dim) == TILEDB_ERR)
    return TILEDB_ERR;
  *name = dim->dim_->name().c_str();
  return TILEDB_OK;
}

int tiledb_dimension_get_type(
    tiledb_ctx_t* ctx, const tiledb_dimension_t* dim, tiledb_datatype_t* type) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, dim) == TILEDB_ERR)
    return TILEDB_ERR;
  *type = static_cast<tiledb_datatype_t>(dim->dim_->type());
  return TILEDB_OK;
}

int tiledb_dimension_get_domain(
    tiledb_ctx_t* ctx, const tiledb_dimension_t* dim, void** domain) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, dim) == TILEDB_ERR)
    return TILEDB_ERR;
  *domain = dim->dim_->domain();
  return TILEDB_OK;
}

int tiledb_dimension_get_tile_extent(
    tiledb_ctx_t* ctx, const tiledb_dimension_t* dim, void** tile_extent) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, dim) == TILEDB_ERR)
    return TILEDB_ERR;
  *tile_extent = dim->dim_->tile_extent();
  return TILEDB_OK;
}

int tiledb_dimension_dump(
    tiledb_ctx_t* ctx, const tiledb_dimension_t* dim, FILE* out) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, dim) == TILEDB_ERR)
    return TILEDB_ERR;
  dim->dim_->dump(out);
  return TILEDB_OK;
}

int tiledb_domain_get_dimension_from_index(
    tiledb_ctx_t* ctx,
    const tiledb_domain_t* domain,
    unsigned int index,
    tiledb_dimension_t** dim) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, domain) == TILEDB_ERR) {
    return TILEDB_ERR;
  }
  unsigned int ndim = domain->domain_->dim_num();
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

int tiledb_domain_get_dimension_from_name(
    tiledb_ctx_t* ctx,
    const tiledb_domain_t* domain,
    const char* name,
    tiledb_dimension_t** dim) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, domain) == TILEDB_ERR) {
    return TILEDB_ERR;
  }
  unsigned int ndim = domain->domain_->dim_num();
  if (ndim == 0) {
    *dim = nullptr;
    return TILEDB_OK;
  }
  std::string name_string(name);
  const tiledb::sm::Dimension* found_dim = nullptr;
  if (name_string.empty()) {  // anonymous dimension
    bool found_anonymous = false;
    for (unsigned int i = 0; i < ndim; i++) {
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

int tiledb_array_schema_create(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t** array_schema,
    tiledb_array_type_t array_type) {
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

int tiledb_array_schema_free(
    tiledb_ctx_t* ctx, tiledb_array_schema_t** array_schema) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  if (array_schema != nullptr && *array_schema != nullptr) {
    delete (*array_schema)->array_schema_;
    delete *array_schema;
    *array_schema = nullptr;
  }

  return TILEDB_OK;
}

int tiledb_array_schema_add_attribute(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_attribute_t* attr) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR ||
      sanity_check(ctx, attr) == TILEDB_ERR)
    return TILEDB_ERR;
  if (save_error(ctx, array_schema->array_schema_->add_attribute(attr->attr_)))
    return TILEDB_ERR;
  return TILEDB_OK;
}

int tiledb_array_schema_set_domain(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_domain_t* domain) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  if (save_error(ctx, array_schema->array_schema_->set_domain(domain->domain_)))
    return TILEDB_ERR;
  return TILEDB_OK;
}

int tiledb_array_schema_set_capacity(
    tiledb_ctx_t* ctx, tiledb_array_schema_t* array_schema, uint64_t capacity) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  array_schema->array_schema_->set_capacity(capacity);
  return TILEDB_OK;
}

int tiledb_array_schema_set_cell_order(
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

int tiledb_array_schema_set_tile_order(
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

int tiledb_array_schema_set_coords_compressor(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_compressor_t compressor,
    int compression_level) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  array_schema->array_schema_->set_coords_compressor(
      static_cast<tiledb::sm::Compressor>(compressor));
  array_schema->array_schema_->set_coords_compression_level(compression_level);
  return TILEDB_OK;
}

int tiledb_array_schema_set_offsets_compressor(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_compressor_t compressor,
    int compression_level) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  array_schema->array_schema_->set_cell_var_offsets_compressor(
      static_cast<tiledb::sm::Compressor>(compressor));
  array_schema->array_schema_->set_cell_var_offsets_compression_level(
      compression_level);
  return TILEDB_OK;
}

int tiledb_array_schema_check(
    tiledb_ctx_t* ctx, tiledb_array_schema_t* array_schema) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;

  if (save_error(ctx, array_schema->array_schema_->check()))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_array_schema_load(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t** array_schema,
    const char* array_uri) {
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

  // Load array schema
  auto storage_manager = ctx->storage_manager_;
  if (save_error(
          ctx,
          storage_manager->load_array_schema(
              tiledb::sm::URI(array_uri), &((*array_schema)->array_schema_)))) {
    delete *array_schema;
    return TILEDB_ERR;
  }

  return TILEDB_OK;
}

int tiledb_array_schema_get_array_type(
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

int tiledb_array_schema_get_capacity(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    uint64_t* capacity) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  *capacity = array_schema->array_schema_->capacity();
  return TILEDB_OK;
}

int tiledb_array_schema_get_cell_order(
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

int tiledb_array_schema_get_coords_compressor(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    tiledb_compressor_t* compressor,
    int* compression_level) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;

  *compressor = static_cast<tiledb_compressor_t>(
      array_schema->array_schema_->coords_compression());
  *compression_level = array_schema->array_schema_->coords_compression_level();
  return TILEDB_OK;
}

int tiledb_array_schema_get_offsets_compressor(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    tiledb_compressor_t* compressor,
    int* compression_level) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;

  *compressor = static_cast<tiledb_compressor_t>(
      array_schema->array_schema_->cell_var_offsets_compression());
  *compression_level =
      array_schema->array_schema_->cell_var_offsets_compression_level();
  return TILEDB_OK;
}

int tiledb_array_schema_get_domain(
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

int tiledb_array_schema_get_tile_order(
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

int tiledb_array_schema_get_attribute_num(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    unsigned int* attribute_num) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  *attribute_num = array_schema->array_schema_->attribute_num();
  return TILEDB_OK;
}

int tiledb_array_schema_dump(
    tiledb_ctx_t* ctx, const tiledb_array_schema_t* array_schema, FILE* out) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  array_schema->array_schema_->dump(out);
  return TILEDB_OK;
}

int tiledb_array_schema_get_attribute_from_index(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    unsigned int index,
    tiledb_attribute_t** attr) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR) {
    return TILEDB_ERR;
  }
  unsigned int attribute_num = array_schema->array_schema_->attribute_num();
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

int tiledb_array_schema_get_attribute_from_name(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    const char* name,
    tiledb_attribute_t** attr) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR) {
    return TILEDB_ERR;
  }
  unsigned int attribute_num = array_schema->array_schema_->attribute_num();
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

/* ****************************** */
/*              QUERY             */
/* ****************************** */

int tiledb_query_create(
    tiledb_ctx_t* ctx,
    tiledb_query_t** query,
    const char* array_uri,
    tiledb_query_type_t type) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create query struct
  *query = new (std::nothrow) tiledb_query_t;
  if (*query == nullptr) {
    auto st =
        tiledb::sm::Status::Error("Failed to allocate TileDB query object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Create query object
  if (save_error(
          ctx,
          ctx->storage_manager_->query_init(
              &((*query)->query_),
              array_uri,
              static_cast<tiledb::sm::QueryType>(type)))) {
    delete (*query)->query_;
    delete *query;
    *query = nullptr;
    return TILEDB_ERR;
  }

  (*query)->finalized_ = false;

  // Success
  return TILEDB_OK;
}

int tiledb_query_set_subarray(
    tiledb_ctx_t* ctx, tiledb_query_t* query, const void* subarray) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  // Set subarray
  if (save_error(ctx, query->query_->set_subarray(subarray)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_query_set_buffers(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char** attributes,
    unsigned int attribute_num,
    void** buffers,
    uint64_t* buffer_sizes) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  // Set attributes and buffers
  if (save_error(
          ctx,
          query->query_->set_buffers(
              attributes, attribute_num, buffers, buffer_sizes)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_query_set_layout(
    tiledb_ctx_t* ctx, tiledb_query_t* query, tiledb_layout_t layout) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  // Set layout
  if (save_error(
          ctx,
          query->query_->set_layout(static_cast<tiledb::sm::Layout>(layout))))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_query_finalize(tiledb_ctx_t* ctx, tiledb_query_t* query) {
  // Trivial case
  if (query == nullptr || query->finalized_)
    return TILEDB_OK;

  query->finalized_ = true;

  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  // Finalize query and check error
  if (save_error(ctx, ctx->storage_manager_->query_finalize(query->query_)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_query_free(tiledb_ctx_t* ctx, tiledb_query_t** query) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  if (query == nullptr || *query == nullptr)
    return TILEDB_OK;

  delete (*query)->query_;
  delete *query;
  *query = nullptr;

  return TILEDB_OK;
}

int tiledb_query_submit(tiledb_ctx_t* ctx, tiledb_query_t* query) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  if (save_error(ctx, ctx->storage_manager_->query_submit(query->query_)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_query_submit_async(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    void (*callback)(void*),
    void* callback_data) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  if (save_error(
          ctx,
          ctx->storage_manager_->query_submit_async(
              query->query_, callback, callback_data)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_query_reset_buffers(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    void** buffers,
    uint64_t* buffer_sizes) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  // Reset buffers
  query->query_->set_buffers(buffers, buffer_sizes);

  // Success
  return TILEDB_OK;
}

int tiledb_query_get_status(
    tiledb_ctx_t* ctx, tiledb_query_t* query, tiledb_query_status_t* status) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  *status = (tiledb_query_status_t)query->query_->status();

  return TILEDB_OK;
}

/* ****************************** */
/*              ARRAY             */
/* ****************************** */

int tiledb_array_create(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    const tiledb_array_schema_t* array_schema) {
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

  // Create the array
  if (save_error(
          ctx,
          ctx->storage_manager_->array_create(
              uri, array_schema->array_schema_)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_array_consolidate(tiledb_ctx_t* ctx, const char* array_uri) {
  // Sanity checks
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  if (save_error(ctx, ctx->storage_manager_->array_consolidate(array_uri)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_array_get_non_empty_domain(
    tiledb_ctx_t* ctx, const char* array_uri, void* domain, int* is_empty) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  bool is_empty_b;

  if (save_error(
          ctx,
          ctx->storage_manager_->array_get_non_empty_domain(
              array_uri, domain, &is_empty_b)))
    return TILEDB_ERR;

  *is_empty = (int)is_empty_b;

  return TILEDB_OK;
}

int tiledb_array_compute_max_read_buffer_sizes(
    tiledb_ctx_t* ctx,
    const char* array_uri,
    const void* subarray,
    const char** attributes,
    unsigned attribute_num,
    uint64_t* buffer_sizes) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  if (save_error(
          ctx,
          ctx->storage_manager_->array_compute_max_read_buffer_sizes(
              array_uri, subarray, attributes, attribute_num, buffer_sizes)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

/* ****************************** */
/*         OBJECT MANAGEMENT      */
/* ****************************** */

int tiledb_object_type(
    tiledb_ctx_t* ctx, const char* path, tiledb_object_t* type) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  auto uri = tiledb::sm::URI(path);
  tiledb::sm::ObjectType object_type;
  if (save_error(ctx, ctx->storage_manager_->object_type(uri, &object_type)))
    return TILEDB_ERR;

  *type = static_cast<tiledb_object_t>(object_type);
  return TILEDB_OK;
}

int tiledb_object_remove(tiledb_ctx_t* ctx, const char* path) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;
  if (save_error(ctx, ctx->storage_manager_->object_remove(path)))
    return TILEDB_ERR;
  return TILEDB_OK;
}

int tiledb_object_move(
    tiledb_ctx_t* ctx, const char* old_path, const char* new_path) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;
  if (save_error(ctx, ctx->storage_manager_->object_move(old_path, new_path)))
    return TILEDB_ERR;
  return TILEDB_OK;
}

int tiledb_object_walk(
    tiledb_ctx_t* ctx,
    const char* path,
    tiledb_walk_order_t order,
    int (*callback)(const char*, tiledb_object_t, void*),
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
  if (save_error(
          ctx,
          ctx->storage_manager_->object_iter_begin(
              &obj_iter, path, static_cast<tiledb::sm::WalkOrder>(order))))
    return TILEDB_ERR;

  // For as long as there is another object and the callback indicates to
  // continue, walk over the TileDB objects in the path
  const char* obj_name;
  tiledb::sm::ObjectType obj_type;
  bool has_next;
  int rc = 0;
  do {
    if (save_error(
            ctx,
            ctx->storage_manager_->object_iter_next(
                obj_iter, &obj_name, &obj_type, &has_next))) {
      ctx->storage_manager_->object_iter_free(obj_iter);
      return TILEDB_ERR;
    }
    if (!has_next)
      break;
    rc = callback(obj_name, tiledb_object_t(obj_type), data);
  } while (rc == 1);

  // Clean up
  ctx->storage_manager_->object_iter_free(obj_iter);

  if (rc == -1)
    return TILEDB_ERR;
  return TILEDB_OK;
}

int tiledb_object_ls(
    tiledb_ctx_t* ctx,
    const char* path,
    int (*callback)(const char*, tiledb_object_t, void*),
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
  if (save_error(
          ctx, ctx->storage_manager_->object_iter_begin(&obj_iter, path)))
    return TILEDB_ERR;

  // For as long as there is another object and the callback indicates to
  // continue, walk over the TileDB objects in the path
  const char* obj_name;
  tiledb::sm::ObjectType obj_type;
  bool has_next;
  int rc = 0;
  do {
    if (save_error(
            ctx,
            ctx->storage_manager_->object_iter_next(
                obj_iter, &obj_name, &obj_type, &has_next))) {
      ctx->storage_manager_->object_iter_free(obj_iter);
      return TILEDB_ERR;
    }
    if (!has_next)
      break;
    rc = callback(obj_name, tiledb_object_t(obj_type), data);
  } while (rc == 1);

  // Clean up
  ctx->storage_manager_->object_iter_free(obj_iter);

  if (rc == -1)
    return TILEDB_ERR;
  return TILEDB_OK;
}

/* ****************************** */
/*         KEY-VALUE SCHEMA       */
/* ****************************** */

int tiledb_kv_schema_create(tiledb_ctx_t* ctx, tiledb_kv_schema_t** kv_schema) {
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
  if (save_error(ctx, ((*kv_schema)->array_schema_->set_as_kv()))) {
    delete (*kv_schema)->array_schema_;
    delete *kv_schema;
    *kv_schema = nullptr;
    return TILEDB_ERR;
  }

  // Success
  return TILEDB_OK;
}

int tiledb_kv_schema_free(tiledb_ctx_t* ctx, tiledb_kv_schema_t** kv_schema) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  if (kv_schema != nullptr && *kv_schema != nullptr) {
    delete (*kv_schema)->array_schema_;
    delete *kv_schema;
    *kv_schema = nullptr;
  }

  return TILEDB_OK;
}

int tiledb_kv_schema_add_attribute(
    tiledb_ctx_t* ctx,
    tiledb_kv_schema_t* kv_schema,
    tiledb_attribute_t* attr) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, kv_schema) == TILEDB_ERR ||
      sanity_check(ctx, attr) == TILEDB_ERR)
    return TILEDB_ERR;
  if (save_error(ctx, kv_schema->array_schema_->add_attribute(attr->attr_)))
    return TILEDB_ERR;
  return TILEDB_OK;
}

int tiledb_kv_schema_check(tiledb_ctx_t* ctx, tiledb_kv_schema_t* kv_schema) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, kv_schema) == TILEDB_ERR)
    return TILEDB_ERR;

  if (save_error(ctx, kv_schema->array_schema_->check()))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_kv_schema_load(
    tiledb_ctx_t* ctx, tiledb_kv_schema_t** kv_schema, const char* kv_uri) {
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

  // Load array schema
  auto storage_manager = ctx->storage_manager_;
  if (save_error(
          ctx,
          storage_manager->load_array_schema(
              tiledb::sm::URI(kv_uri), &((*kv_schema)->array_schema_)))) {
    delete *kv_schema;
    return TILEDB_ERR;
  }

  return TILEDB_OK;
}

int tiledb_kv_schema_get_attribute_num(
    tiledb_ctx_t* ctx,
    const tiledb_kv_schema_t* kv_schema,
    unsigned int* attribute_num) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, kv_schema) == TILEDB_ERR)
    return TILEDB_ERR;

  // -2 because of the first two special attributes in the key-value schema
  *attribute_num = kv_schema->array_schema_->attribute_num() - 2;
  return TILEDB_OK;
}

int tiledb_kv_schema_get_attribute_from_index(
    tiledb_ctx_t* ctx,
    const tiledb_kv_schema_t* kv_schema,
    unsigned int index,
    tiledb_attribute_t** attr) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, kv_schema) == TILEDB_ERR) {
    return TILEDB_ERR;
  }

  // Important! Skips the first two special attributes in the key-value schema
  index += 2;

  unsigned int attribute_num = kv_schema->array_schema_->attribute_num();
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

int tiledb_kv_schema_get_attribute_from_name(
    tiledb_ctx_t* ctx,
    const tiledb_kv_schema_t* kv_schema,
    const char* name,
    tiledb_attribute_t** attr) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, kv_schema) == TILEDB_ERR) {
    return TILEDB_ERR;
  }
  unsigned int attribute_num = kv_schema->array_schema_->attribute_num();
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

int tiledb_kv_schema_dump(
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

int tiledb_kv_item_create(tiledb_ctx_t* ctx, tiledb_kv_item_t** kv_item) {
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

int tiledb_kv_item_free(tiledb_ctx_t* ctx, tiledb_kv_item_t** kv_item) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  if (kv_item != nullptr && *kv_item != nullptr) {
    delete (*kv_item)->kv_item_;
    delete *kv_item;
    *kv_item = nullptr;
  }

  return TILEDB_OK;
}

int tiledb_kv_item_set_key(
    tiledb_ctx_t* ctx,
    tiledb_kv_item_t* kv_item,
    const void* key,
    tiledb_datatype_t key_type,
    uint64_t key_size) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, kv_item) == TILEDB_ERR)
    return TILEDB_ERR;

  if (save_error(
          ctx,
          kv_item->kv_item_->set_key(
              key, static_cast<tiledb::sm::Datatype>(key_type), key_size)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_kv_item_set_value(
    tiledb_ctx_t* ctx,
    tiledb_kv_item_t* kv_item,
    const char* attribute,
    const void* value,
    tiledb_datatype_t value_type,
    uint64_t value_size) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, kv_item) == TILEDB_ERR)
    return TILEDB_ERR;

  if (save_error(
          ctx,
          kv_item->kv_item_->set_value(
              attribute,
              value,
              static_cast<tiledb::sm::Datatype>(value_type),
              value_size)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_kv_item_get_key(
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

int tiledb_kv_item_get_value(
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

  auto value_ptr = kv_item->kv_item_->value(attribute);
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

int tiledb_kv_get_item(
    tiledb_ctx_t* ctx,
    tiledb_kv_t* kv,
    tiledb_kv_item_t** kv_item,
    const void* key,
    tiledb_datatype_t key_type,
    uint64_t key_size) {
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
  if (save_error(
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

/* ****************************** */
/*             KEY-VALUE          */
/* ****************************** */

int tiledb_kv_create(
    tiledb_ctx_t* ctx,
    const char* kv_uri,
    const tiledb_kv_schema_t* kv_schema) {
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

  // Create the key-value store
  if (save_error(
          ctx,
          ctx->storage_manager_->array_create(uri, kv_schema->array_schema_)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_kv_consolidate(tiledb_ctx_t* ctx, const char* kv_uri) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  if (save_error(ctx, ctx->storage_manager_->array_consolidate(kv_uri)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_kv_set_max_buffered_items(
    tiledb_ctx_t* ctx, tiledb_kv_t* kv, uint64_t max_items) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, kv) == TILEDB_ERR)
    return TILEDB_ERR;

  if (save_error(ctx, kv->kv_->set_max_buffered_items(max_items)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_kv_open(
    tiledb_ctx_t* ctx,
    tiledb_kv_t** kv,
    const char* kv_uri,
    const char** attributes,
    unsigned int attribute_num) {
  // Sanity checks
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create key-value store struct
  *kv = new (std::nothrow) tiledb_kv_t;
  if (*kv == nullptr) {
    tiledb::sm::Status st = tiledb::sm::Status::Error(
        "Failed to allocate TileDB key-value store object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Create a new key-value store object
  (*kv)->kv_ = new tiledb::sm::KV(ctx->storage_manager_);
  if ((*kv)->kv_ == nullptr) {
    delete *kv;
    *kv = nullptr;
    tiledb::sm::Status st = tiledb::sm::Status::Error(
        "Failed to allocate TileDB key-value store object");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Prepare the key-value store
  if (save_error(ctx, (*kv)->kv_->init(kv_uri, attributes, attribute_num)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_kv_close(tiledb_ctx_t* ctx, tiledb_kv_t** kv) {
  if (kv == nullptr || *kv == nullptr)
    return TILEDB_OK;

  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, *kv) == TILEDB_ERR)
    return TILEDB_ERR;

  if (save_error(ctx, (*kv)->kv_->finalize()))
    return TILEDB_ERR;

  delete (*kv)->kv_;
  delete *kv;
  *kv = nullptr;

  return TILEDB_OK;
}

int tiledb_kv_add_item(
    tiledb_ctx_t* ctx, tiledb_kv_t* kv, tiledb_kv_item_t* kv_item) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, kv) == TILEDB_ERR ||
      sanity_check(ctx, kv_item) == TILEDB_ERR)
    return TILEDB_ERR;

  if (save_error(ctx, kv->kv_->add_item(kv_item->kv_item_)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_kv_flush(tiledb_ctx_t* ctx, tiledb_kv_t* kv) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, kv) == TILEDB_ERR)
    return TILEDB_ERR;

  if (save_error(ctx, kv->kv_->flush()))
    return TILEDB_ERR;

  return TILEDB_OK;
}

/* ****************************** */
/*          KEY-VALUE ITER        */
/* ****************************** */

int tiledb_kv_iter_create(
    tiledb_ctx_t* ctx,
    tiledb_kv_iter_t** kv_iter,
    const char* kv_uri,
    const char** attributes,
    unsigned int attribute_num) {
  if (sanity_check(ctx) == TILEDB_ERR)
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
  (*kv_iter)->kv_iter_ = new tiledb::sm::KVIter(ctx->storage_manager_);
  if ((*kv_iter)->kv_iter_ == nullptr) {
    tiledb::sm::Status st = tiledb::sm::Status::Error(
        "Failed to allocate TileDB key-value iterator object");
    LOG_STATUS(st);
    save_error(ctx, st);
    delete *kv_iter;
    return TILEDB_OOM;
  }

  // Initialize KVIter object
  if (save_error(
          ctx, (*kv_iter)->kv_iter_->init(kv_uri, attributes, attribute_num))) {
    delete (*kv_iter)->kv_iter_;
    delete (*kv_iter);
    return TILEDB_ERR;
  }

  // Success
  return TILEDB_OK;
}

int tiledb_kv_iter_free(tiledb_ctx_t* ctx, tiledb_kv_iter_t** kv_iter) {
  if (kv_iter == nullptr || *kv_iter == nullptr)
    return TILEDB_OK;

  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, *kv_iter) == TILEDB_ERR)
    return TILEDB_ERR;

  if (save_error(ctx, (*kv_iter)->kv_iter_->finalize()))
    return TILEDB_ERR;

  delete (*kv_iter)->kv_iter_;
  delete *kv_iter;
  *kv_iter = nullptr;

  return TILEDB_OK;
}

int tiledb_kv_iter_here(
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

  if (save_error(ctx, kv_iter->kv_iter_->here(&((*kv_item)->kv_item_)))) {
    tiledb_kv_item_free(ctx, kv_item);
    return TILEDB_ERR;
  }

  return TILEDB_OK;
}

int tiledb_kv_iter_next(tiledb_ctx_t* ctx, tiledb_kv_iter_t* kv_iter) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, kv_iter) == TILEDB_ERR)
    return TILEDB_ERR;

  if (save_error(ctx, kv_iter->kv_iter_->next()))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_kv_iter_done(
    tiledb_ctx_t* ctx, tiledb_kv_iter_t* kv_iter, int* done) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, kv_iter) == TILEDB_ERR)
    return TILEDB_ERR;

  *done = kv_iter->kv_iter_->done();

  return TILEDB_OK;
}

/* ****************************** */
/*        VIRTUAL FILESYSTEM      */
/* ****************************** */

int tiledb_vfs_create(
    tiledb_ctx_t* ctx, tiledb_vfs_t** vfs, tiledb_config_t* config) {
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

  if (save_error(ctx, (*vfs)->vfs_->init(vfs_params))) {
    delete (*vfs)->vfs_;
    delete vfs;
    return TILEDB_ERR;
  }

  // Success
  return TILEDB_OK;
}

int tiledb_vfs_free(tiledb_ctx_t* ctx, tiledb_vfs_t** vfs) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  if (vfs != nullptr && *vfs != nullptr) {
    delete (*vfs)->vfs_;
    delete *vfs;
    *vfs = nullptr;
  }

  return TILEDB_OK;
}

int tiledb_vfs_get_config(
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

int tiledb_vfs_create_bucket(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, vfs) == TILEDB_ERR)
    return TILEDB_ERR;

  if (save_error(ctx, vfs->vfs_->create_bucket(tiledb::sm::URI(uri))))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_vfs_remove_bucket(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, vfs) == TILEDB_ERR)
    return TILEDB_ERR;

  if (save_error(ctx, vfs->vfs_->remove_bucket(tiledb::sm::URI(uri))))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_vfs_empty_bucket(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, vfs) == TILEDB_ERR)
    return TILEDB_ERR;

  if (save_error(ctx, vfs->vfs_->empty_bucket(tiledb::sm::URI(uri))))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_vfs_is_empty_bucket(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri, int* is_empty) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, vfs) == TILEDB_ERR)
    return TILEDB_ERR;

  bool b;
  if (save_error(ctx, vfs->vfs_->is_empty_bucket(tiledb::sm::URI(uri), &b)))
    return TILEDB_ERR;
  *is_empty = (int)b;

  return TILEDB_OK;
}

int tiledb_vfs_is_bucket(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri, int* is_bucket) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, vfs) == TILEDB_ERR)
    return TILEDB_ERR;

  bool exists;
  if (save_error(ctx, vfs->vfs_->is_bucket(tiledb::sm::URI(uri), &exists)))
    return TILEDB_ERR;

  *is_bucket = (int)exists;

  return TILEDB_OK;
}

int tiledb_vfs_create_dir(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, vfs) == TILEDB_ERR)
    return TILEDB_ERR;

  if (save_error(ctx, vfs->vfs_->create_dir(tiledb::sm::URI(uri))))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_vfs_is_dir(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri, int* is_dir) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, vfs) == TILEDB_ERR)
    return TILEDB_ERR;

  bool exists;
  if (save_error(ctx, vfs->vfs_->is_dir(tiledb::sm::URI(uri), &exists)))
    return TILEDB_ERR;
  *is_dir = (int)exists;

  return TILEDB_OK;
}

int tiledb_vfs_remove_dir(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, vfs) == TILEDB_ERR)
    return TILEDB_ERR;

  if (save_error(ctx, vfs->vfs_->remove_dir(tiledb::sm::URI(uri))))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_vfs_is_file(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri, int* is_file) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, vfs) == TILEDB_ERR)
    return TILEDB_ERR;

  bool exists;
  if (save_error(ctx, vfs->vfs_->is_file(tiledb::sm::URI(uri), &exists)))
    return TILEDB_ERR;
  *is_file = (int)exists;

  return TILEDB_OK;
}

int tiledb_vfs_remove_file(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, vfs) == TILEDB_ERR)
    return TILEDB_ERR;

  if (save_error(ctx, vfs->vfs_->remove_file(tiledb::sm::URI(uri))))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_vfs_file_size(
    tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri, uint64_t* size) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, vfs) == TILEDB_ERR)
    return TILEDB_ERR;

  if (save_error(ctx, vfs->vfs_->file_size(tiledb::sm::URI(uri), size)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_vfs_move_file(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* old_uri,
    const char* new_uri) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, vfs) == TILEDB_ERR)
    return TILEDB_ERR;

  if (save_error(
          ctx,
          vfs->vfs_->move_file(
              tiledb::sm::URI(old_uri), tiledb::sm::URI(new_uri))))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_vfs_move_dir(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* old_uri,
    const char* new_uri) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, vfs) == TILEDB_ERR)
    return TILEDB_ERR;

  if (save_error(
          ctx,
          vfs->vfs_->move_dir(
              tiledb::sm::URI(old_uri), tiledb::sm::URI(new_uri))))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_vfs_open(
    tiledb_ctx_t* ctx,
    tiledb_vfs_t* vfs,
    const char* uri,
    tiledb_vfs_mode_t mode,
    tiledb_vfs_fh_t** fh) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, vfs) == TILEDB_ERR)
    return TILEDB_ERR;

  *fh = new (std::nothrow) tiledb_vfs_fh_t;
  if (*fh == nullptr)
    return TILEDB_OOM;
  (*fh)->uri_ = tiledb::sm::URI(uri);

  if (save_error(
          ctx,
          vfs->vfs_->open_file(
              (*fh)->uri_, static_cast<tiledb::sm::VFSMode>(mode)))) {
    delete (*fh);
    *fh = nullptr;
    return TILEDB_ERR;
  }

  (*fh)->is_closed_ = false;
  (*fh)->vfs_ = vfs->vfs_;
  (*fh)->mode_ = static_cast<tiledb::sm::VFSMode>(mode);

  return TILEDB_OK;
}

int tiledb_vfs_close(tiledb_ctx_t* ctx, tiledb_vfs_fh_t* fh) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, fh) == TILEDB_ERR)
    return TILEDB_ERR;

  if (fh->is_closed_) {
    std::stringstream msg;
    msg << "Cannot close file '" << fh->uri_.to_string() << "'; File closed";
    auto st = tiledb::sm::Status::Error(msg.str());
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  // Close file in write or append mode
  if (fh->mode_ != tiledb::sm::VFSMode::VFS_READ) {
    if (save_error(ctx, fh->vfs_->close_file(fh->uri_)))
      return TILEDB_ERR;

    // Create an empty file if the file does not exist
    bool exists;
    if (save_error(ctx, fh->vfs_->is_file(fh->uri_, &exists)))
      return TILEDB_ERR;
    if (!exists)
      if (save_error(ctx, fh->vfs_->touch(fh->uri_)))
        return TILEDB_ERR;
  }

  fh->is_closed_ = true;
  return TILEDB_OK;
}

int tiledb_vfs_read(
    tiledb_ctx_t* ctx,
    tiledb_vfs_fh_t* fh,
    uint64_t offset,
    void* buffer,
    uint64_t nbytes) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, fh) == TILEDB_ERR)
    return TILEDB_ERR;

  if (fh->is_closed_) {
    std::stringstream msg;
    msg << "Cannot read from file '" << fh->uri_.to_string()
        << "'; File closed";
    auto st = tiledb::sm::Status::Error(msg.str());
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  if (save_error(ctx, fh->vfs_->read(fh->uri_, offset, buffer, nbytes)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_vfs_write(
    tiledb_ctx_t* ctx,
    tiledb_vfs_fh_t* fh,
    const void* buffer,
    uint64_t nbytes) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, fh) == TILEDB_ERR)
    return TILEDB_ERR;

  if (fh->is_closed_) {
    std::stringstream msg;
    msg << "Cannot write to file '" + fh->uri_.to_string() << "'; File closed";
    auto st = tiledb::sm::Status::Error(msg.str());
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  if (save_error(ctx, fh->vfs_->write(fh->uri_, buffer, nbytes)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_vfs_sync(tiledb_ctx_t* ctx, tiledb_vfs_fh_t* fh) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, fh) == TILEDB_ERR)
    return TILEDB_ERR;

  if (fh->is_closed_) {
    std::stringstream msg;
    msg << "Cannot sync file '" << fh->uri_.to_string() << "'; File closed";
    auto st = tiledb::sm::Status::Error(msg.str());
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  if (save_error(ctx, fh->vfs_->sync(fh->uri_)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_vfs_fh_free(tiledb_ctx_t* ctx, tiledb_vfs_fh_t** fh) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  if (fh != nullptr && *fh != nullptr) {
    delete *fh;
    *fh = nullptr;
  }

  return TILEDB_OK;
}

int tiledb_vfs_fh_is_closed(
    tiledb_ctx_t* ctx, tiledb_vfs_fh_t* fh, int* is_closed) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, fh) == TILEDB_ERR)
    return TILEDB_ERR;

  *is_closed = fh->is_closed_;

  return TILEDB_OK;
}

int tiledb_vfs_touch(tiledb_ctx_t* ctx, tiledb_vfs_t* vfs, const char* uri) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, vfs) == TILEDB_ERR)
    return TILEDB_ERR;

  if (save_error(ctx, vfs->vfs_->touch(tiledb::sm::URI(uri))))
    return TILEDB_ERR;

  return TILEDB_OK;
}

/* ****************************** */
/*              URI               */
/* ****************************** */

int tiledb_uri_to_path(
    tiledb_ctx_t* ctx, const char* uri, char* path_out, unsigned* path_length) {
  if (sanity_check(ctx) == TILEDB_ERR || uri == nullptr ||
      path_out == nullptr || path_length == nullptr)
    return TILEDB_ERR;

  std::string path = tiledb::sm::URI::to_path(uri);
  if (path.empty() || path.length() + 1 > *path_length) {
    *path_length = 0;
    return TILEDB_ERR;
  } else {
    *path_length = static_cast<unsigned>(path.length());
    path.copy(path_out, path.length());
    path_out[path.length()] = '\0';
    return TILEDB_OK;
  }
}

/* ****************************** */
/*             Stats              */
/* ****************************** */

int tiledb_stats_enable() {
  tiledb::sm::stats::all_stats.set_enabled(true);
  return TILEDB_OK;
}

int tiledb_stats_disable() {
  tiledb::sm::stats::all_stats.set_enabled(false);
  return TILEDB_OK;
}

int tiledb_stats_reset() {
  tiledb::sm::stats::all_stats.reset();
  return TILEDB_OK;
}

int tiledb_stats_dump(FILE* out) {
  tiledb::sm::stats::all_stats.dump(out);
  return TILEDB_OK;
}

/* ****************************** */
/*            C++ API             */
/* ****************************** */

int tiledb::impl::tiledb_query_submit_async(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    std::function<void(void*)> callback,
    void* callback_data) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  if (save_error(
          ctx,
          ctx->storage_manager_->query_submit_async(
              query->query_, callback, callback_data)))
    return TILEDB_ERR;

  return TILEDB_OK;
}
