/**
 * @file   tiledb.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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
#include "array_metadata.h"
#include "config.h"
#include "kv.h"
#include "logger.h"
#include "query.h"
#include "utils.h"

#include <sstream>

/* ****************************** */
/*            CONSTANTS           */
/* ****************************** */

const char* tiledb_coords() {
  return tiledb::constants::coords;
}

unsigned int tiledb_var_num() {
  return tiledb::constants::var_num;
}

/* ****************************** */
/*            VERSION             */
/* ****************************** */

void tiledb_version(int* major, int* minor, int* rev) {
  *major = tiledb::constants::version[0];
  *minor = tiledb::constants::version[1];
  *rev = tiledb::constants::version[2];
}

/* ********************************* */
/*           TILEDB TYPES            */
/* ********************************* */

struct tiledb_config_t {
  tiledb::Config* config_;
};

struct tiledb_ctx_t {
  tiledb::StorageManager* storage_manager_;
  tiledb::Status* last_error_;
  std::mutex* mtx_;
};

struct tiledb_error_t {
  // Pointer to a copy of the last TileDB error associated with a given ctx
  const tiledb::Status* status_;
  std::string* errmsg_;
};

struct tiledb_attribute_t {
  tiledb::Attribute* attr_;
};

struct tiledb_array_metadata_t {
  tiledb::ArrayMetadata* array_metadata_;
};

struct tiledb_attribute_iter_t {
  const tiledb_array_metadata_t* array_metadata_;
  tiledb_attribute_t* attr_;
  unsigned int attr_num_;
  unsigned int current_attr_;
};

struct tiledb_dimension_t {
  tiledb::Dimension* dim_;
};

struct tiledb_dimension_iter_t {
  const tiledb_domain_t* domain_;
  tiledb_dimension_t* dim_;
  unsigned int dim_num_;
  unsigned int current_dim_;
};

struct tiledb_domain_t {
  tiledb::Domain* domain_;
};

struct tiledb_query_t {
  tiledb::Query* query_;
};

struct tiledb_kv_t {
  tiledb::KV* kv_;
};

/* ********************************* */
/*         AUXILIARY FUNCTIONS       */
/* ********************************* */

/* Saves a status inside the context object. */
static bool save_error(tiledb_ctx_t* ctx, const tiledb::Status& st) {
  // No error
  if (st.ok())
    return false;

  // Store new error
  {
    std::lock_guard<std::mutex> lock(*(ctx->mtx_));
    delete ctx->last_error_;
    ctx->last_error_ = new (std::nothrow) tiledb::Status(st);
  }

  // There is an error
  return true;
}

inline int sanity_check(tiledb_config_t* config) {
  if (config == nullptr || config->config_ == nullptr)
    return TILEDB_ERR;
  return TILEDB_OK;
}

inline int sanity_check(tiledb_ctx_t* ctx) {
  if (ctx == nullptr)
    return TILEDB_ERR;
  if (ctx->storage_manager_ == nullptr) {
    tiledb::Status st = tiledb::Status::Error("Invalid TileDB context");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int sanity_check(tiledb_ctx_t* ctx, const tiledb_error_t* err) {
  if (err == nullptr || err->status_ == nullptr) {
    tiledb::Status st = tiledb::Status::Error("Invalid TileDB error struct");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int sanity_check(tiledb_ctx_t* ctx, const tiledb_attribute_t* attr) {
  if (attr == nullptr || attr->attr_ == nullptr) {
    tiledb::Status st =
        tiledb::Status::Error("Invalid TileDB attribute struct");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int sanity_check(
    tiledb_ctx_t* ctx, const tiledb_attribute_iter_t* attr_it) {
  if (attr_it == nullptr || attr_it->array_metadata_ == nullptr) {
    tiledb::Status st =
        tiledb::Status::Error("Invalid TileDB attribute iterator struct");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int sanity_check(
    tiledb_ctx_t* ctx, const tiledb_dimension_iter_t* dim_it) {
  if (dim_it == nullptr || dim_it->domain_ == nullptr) {
    tiledb::Status st =
        tiledb::Status::Error("Invalid TileDB dimension iterator struct");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int sanity_check(tiledb_ctx_t* ctx, const tiledb_dimension_t* dim) {
  if (dim == nullptr || dim->dim_ == nullptr) {
    tiledb::Status st =
        tiledb::Status::Error("Invalid TileDB dimension struct");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int sanity_check(
    tiledb_ctx_t* ctx, const tiledb_array_metadata_t* array_metadata) {
  if (array_metadata == nullptr || array_metadata->array_metadata_ == nullptr) {
    tiledb::Status st =
        tiledb::Status::Error("Invalid TileDB array metadata struct");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int sanity_check(tiledb_ctx_t* ctx, const tiledb_domain_t* domain) {
  if (domain == nullptr || domain->domain_ == nullptr) {
    tiledb::Status st = tiledb::Status::Error("Invalid TileDB domain struct");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int sanity_check(tiledb_ctx_t* ctx, const tiledb_query_t* query) {
  if (query == nullptr || query->query_ == nullptr) {
    tiledb::Status st = tiledb::Status::Error("Invalid TileDB query struct");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int sanity_check(tiledb_ctx_t* ctx, const tiledb_kv_t* kv) {
  if (kv == nullptr || kv->kv_ == nullptr) {
    tiledb::Status st = tiledb::Status::Error(
        "Invalid TileDB in-memory key-value store struct");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

/* ****************************** */
/*            CONFIG              */
/* ****************************** */

int tiledb_config_create(tiledb_config_t** config) {
  // Create a new config struct
  *config = new (std::nothrow) tiledb_config_t;
  if (*config == nullptr)
    return TILEDB_OOM;

  // Create storage manager
  (*config)->config_ = new (std::nothrow) tiledb::Config();
  if ((*config)->config_ == nullptr)
    return TILEDB_OOM;

  // Success
  return TILEDB_OK;
}

int tiledb_config_free(tiledb_config_t* config) {
  if (config != nullptr) {
    delete config->config_;
    delete config;
  }

  // Always succeeds
  return TILEDB_OK;
}

int tiledb_config_set(
    tiledb_config_t* config, const char* param, const char* value) {
  if (sanity_check(config) == TILEDB_ERR)
    return TILEDB_ERR;

  config->config_->set(param, value);

  return TILEDB_OK;
}

int tiledb_config_set_from_file(tiledb_config_t* config, const char* filename) {
  if (sanity_check(config) == TILEDB_ERR || filename == nullptr)
    return TILEDB_ERR;

  config->config_->set_config_filename(filename);

  return TILEDB_OK;
}

int tiledb_config_unset(tiledb_config_t* config, const char* param) {
  if (sanity_check(config) == TILEDB_ERR)
    return TILEDB_ERR;

  config->config_->unset(param);

  return TILEDB_OK;
}

/* ****************************** */
/*            CONTEXT             */
/* ****************************** */

int tiledb_ctx_create(tiledb_ctx_t** ctx, tiledb_config_t* config) {
  // Initialize context
  *ctx = new (std::nothrow) tiledb_ctx_t;
  if (*ctx == nullptr)
    return TILEDB_OOM;

  // Create mutex
  (*ctx)->mtx_ = new (std::nothrow) std::mutex();

  // Create storage manager
  (*ctx)->storage_manager_ = new (std::nothrow) tiledb::StorageManager();
  if ((*ctx)->storage_manager_ == nullptr) {
    tiledb::Status st = tiledb::Status::Error(
        "Failed to allocate storage manager in TileDB context");
    LOG_STATUS(st);
    save_error(*ctx, st);
    return TILEDB_ERR;
  }

  // Initialize last error
  (*ctx)->last_error_ = nullptr;

  // Initialize storage manager
  auto conf = (config == nullptr) ? (tiledb::Config*)nullptr : config->config_;
  if (save_error(*ctx, ((*ctx)->storage_manager_->init(conf)))) {
    delete (*ctx)->storage_manager_;
    (*ctx)->storage_manager_ = nullptr;
    return TILEDB_ERR;
  }

  // Success
  return TILEDB_OK;
}

int tiledb_ctx_free(tiledb_ctx_t* ctx) {
  if (ctx != nullptr) {
    delete ctx->storage_manager_;
    delete ctx->last_error_;
    delete ctx->mtx_;
    delete ctx;
  }

  // Always succeeds
  return TILEDB_OK;
}

/* ********************************* */
/*              ERROR                */
/* ********************************* */

int tiledb_error_last(tiledb_ctx_t* ctx, tiledb_error_t** err) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

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
      tiledb::Status st =
          tiledb::Status::Error("Failed to allocate error struct");
      LOG_STATUS(st);
      save_error(ctx, st);
      return TILEDB_OOM;
    }

    // Create status
    (*err)->status_ = new (std::nothrow) tiledb::Status(*(ctx->last_error_));
    if ((*err)->status_ == nullptr) {
      delete *err;
      tiledb::Status st = tiledb::Status::Error(
          "Failed to allocate status object in TileDB error struct");
      LOG_STATUS(st);
      save_error(ctx, st);
      return TILEDB_OOM;
    }

    // Set error message
    (*err)->errmsg_ =
        new (std::nothrow) std::string((*err)->status_->to_string());
  }

  // Success
  return TILEDB_OK;
}

int tiledb_error_message(
    tiledb_ctx_t* ctx, tiledb_error_t* err, const char** errmsg) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, err) == TILEDB_ERR)
    return TILEDB_ERR;
  // Set error message
  if (err->status_->ok() || err->errmsg_ == nullptr)
    *errmsg = nullptr;
  else
    *errmsg = err->errmsg_->c_str();
  return TILEDB_OK;
}

int tiledb_error_free(tiledb_ctx_t* ctx, tiledb_error_t* err) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, err) == TILEDB_ERR)
    return TILEDB_ERR;
  delete err->status_;
  delete err->errmsg_;
  delete err;
  return TILEDB_OK;
}

/* ****************************** */
/*              GROUP             */
/* ****************************** */

int tiledb_group_create(tiledb_ctx_t* ctx, const char* group) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  // Check for error
  if (group == nullptr) {
    tiledb::Status st =
        tiledb::Status::Error("Invalid group directory argument is NULL");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  // Create the group
  if (save_error(ctx, ctx->storage_manager_->group_create(group)))
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
    tiledb::Status st =
        tiledb::Status::Error("Failed to allocate TileDB attribute struct");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Create a new Attribute object
  (*attr)->attr_ = new (std::nothrow)
      tiledb::Attribute(name, static_cast<tiledb::Datatype>(type));
  if ((*attr)->attr_ == nullptr) {
    delete *attr;
    tiledb::Status st = tiledb::Status::Error(
        "Failed to allocate TileDB attribute object in struct");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Success
  return TILEDB_OK;
}

int tiledb_attribute_free(tiledb_ctx_t* ctx, tiledb_attribute_t* attr) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, attr) == TILEDB_ERR)
    return TILEDB_ERR;
  delete attr->attr_;
  delete attr;
  return TILEDB_OK;
}

int tiledb_attribute_set_compressor(
    tiledb_ctx_t* ctx,
    tiledb_attribute_t* attr,
    tiledb_compressor_t compressor,
    int compression_level) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, attr) == TILEDB_ERR)
    return TILEDB_ERR;
  attr->attr_->set_compressor(static_cast<tiledb::Compressor>(compressor));
  attr->attr_->set_compression_level(compression_level);
  return TILEDB_OK;
}

int tiledb_attribute_set_cell_val_num(
    tiledb_ctx_t* ctx, tiledb_attribute_t* attr, unsigned int cell_val_num) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, attr) == TILEDB_ERR)
    return TILEDB_ERR;
  attr->attr_->set_cell_val_num(cell_val_num);
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

int tiledb_attribute_dump(
    tiledb_ctx_t* ctx, const tiledb_attribute_t* attr, FILE* out) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, attr) == TILEDB_ERR)
    return TILEDB_ERR;
  attr->attr_->dump(out);
  return TILEDB_OK;
}

/* ********************************* */
/*            HYPERSPACE             */
/* ********************************* */

int tiledb_domain_create(
    tiledb_ctx_t* ctx, tiledb_domain_t** domain, tiledb_datatype_t type) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create a domain struct
  *domain = new (std::nothrow) tiledb_domain_t;
  if (*domain == nullptr) {
    tiledb::Status st =
        tiledb::Status::Error("Failed to allocate TileDB domain struct");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Create a new Domain object
  (*domain)->domain_ =
      new (std::nothrow) tiledb::Domain(static_cast<tiledb::Datatype>(type));
  if ((*domain)->domain_ == nullptr) {
    delete *domain;
    tiledb::Status st = tiledb::Status::Error(
        "Failed to allocate TileDB domain object in struct");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }
  return TILEDB_OK;
}

int tiledb_domain_free(tiledb_ctx_t* ctx, tiledb_domain_t* domain) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, domain) == TILEDB_ERR)
    return TILEDB_ERR;
  delete domain->domain_;
  delete domain;
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
    tiledb::Status st =
        tiledb::Status::Error("Failed to allocate TileDB dimension struct");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Create a new Dimension object
  (*dim)->dim_ = new (std::nothrow)
      tiledb::Dimension(name, static_cast<tiledb::Datatype>(type));
  if ((*dim)->dim_ == nullptr) {
    delete *dim;
    tiledb::Status st = tiledb::Status::Error(
        "Failed to allocate TileDB dimension object in struct");
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

int tiledb_dimension_free(tiledb_ctx_t* ctx, tiledb_dimension_t* dim) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, dim) == TILEDB_ERR)
    return TILEDB_ERR;
  delete dim->dim_;
  delete dim;
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

int tiledb_dimension_from_index(
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
    tiledb::Status st = tiledb::Status::DomainError(errmsg.str());
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  *dim = new (std::nothrow) tiledb_dimension_t;
  if (*dim == nullptr) {
    tiledb::Status st =
        tiledb::Status::Error("Failed to allocate TileDB dimension struct");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }
  (*dim)->dim_ =
      new (std::nothrow) tiledb::Dimension(domain->domain_->dimension(index));
  if ((*dim)->dim_ == nullptr) {
    delete *dim;
    tiledb::Status st =
        tiledb::Status::Error("Failed to allocate TileDB dimension struct");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }
  return TILEDB_OK;
}

int tiledb_dimension_from_name(
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
  const tiledb::Dimension* found_dim = nullptr;
  if (name_string.empty()) {  // anonymous dimension
    bool found_anonymous = false;
    for (unsigned int i = 0; i < ndim; i++) {
      auto dim = domain->domain_->dimension(i);
      if (dim->is_anonymous()) {
        if (found_anonymous) {
          tiledb::Status st = tiledb::Status::Error(
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
    tiledb::Status st = tiledb::Status::DomainError(
        std::string("Dimension \"") + name + "\" does not exist");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  *dim = new (std::nothrow) tiledb_dimension_t;
  if (*dim == nullptr) {
    tiledb::Status st =
        tiledb::Status::Error("Failed to allocate TileDB dimension struct");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }
  (*dim)->dim_ = new (std::nothrow) tiledb::Dimension(found_dim);
  if ((*dim)->dim_ == nullptr) {
    delete *dim;
    tiledb::Status st =
        tiledb::Status::Error("Failed to allocate TileDB dimension struct");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }
  return TILEDB_OK;
}

/* ********************************* */
/*        DIMENSION ITERATOR         */
/* ********************************* */

int tiledb_dimension_iter_create(
    tiledb_ctx_t* ctx,
    const tiledb_domain_t* domain,
    tiledb_dimension_iter_t** dim_it) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, domain) == TILEDB_ERR)
    return TILEDB_ERR;
  // Create dimension iterator struct
  *dim_it = new (std::nothrow) tiledb_dimension_iter_t;
  if (*dim_it == nullptr) {
    tiledb::Status st = tiledb::Status::Error(
        "Failed to allocate TileDB dimension iterator struct");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Initialize the iterator
  (*dim_it)->domain_ = domain;
  (*dim_it)->dim_num_ = domain->domain_->dim_num();
  (*dim_it)->current_dim_ = 0;
  if ((*dim_it)->dim_num_ == 0) {
    (*dim_it)->dim_ = nullptr;
  } else {
    // Create a dimension struct inside the iterator struct
    (*dim_it)->dim_ = new (std::nothrow) tiledb_dimension_t;
    if ((*dim_it)->dim_ == nullptr) {
      delete *dim_it;
      *dim_it = nullptr;
      tiledb::Status st = tiledb::Status::Error(
          "Failed to allocate TileDB dimension struct "
          "in iterator struct");
      LOG_STATUS(st);
      save_error(ctx, st);
      return TILEDB_OOM;
    }

    // Create a dimension object
    (*dim_it)->dim_->dim_ =
        new (std::nothrow) tiledb::Dimension(domain->domain_->dimension(0));

    // Check for allocation error
    if ((*dim_it)->dim_->dim_ == nullptr) {
      delete (*dim_it)->dim_;
      delete *dim_it;
      *dim_it = nullptr;
      tiledb::Status st = tiledb::Status::Error(
          "Failed to allocate TileDB dimension object "
          "in iterator struct");
      LOG_STATUS(st);
      save_error(ctx, st);
      return TILEDB_OOM;
    }
  }

  // Success
  return TILEDB_OK;
}

int tiledb_dimension_iter_free(
    tiledb_ctx_t* ctx, tiledb_dimension_iter_t* dim_it) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, dim_it) == TILEDB_ERR)
    return TILEDB_ERR;
  if (dim_it->dim_ != nullptr) {
    if (dim_it->dim_->dim_ != nullptr)
      delete (dim_it->dim_->dim_);
    delete dim_it->dim_;
  }
  delete dim_it;
  return TILEDB_OK;
}

int tiledb_dimension_iter_done(
    tiledb_ctx_t* ctx, tiledb_dimension_iter_t* dim_it, int* done) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, dim_it) == TILEDB_ERR)
    return TILEDB_ERR;
  *done = (dim_it->current_dim_ == dim_it->dim_num_) ? 1 : 0;
  return TILEDB_OK;
}

int tiledb_dimension_iter_next(
    tiledb_ctx_t* ctx, tiledb_dimension_iter_t* dim_it) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, dim_it) == TILEDB_ERR)
    return TILEDB_ERR;
  ++(dim_it->current_dim_);
  if (dim_it->dim_ != nullptr) {
    delete dim_it->dim_->dim_;
    if (dim_it->current_dim_ < dim_it->dim_num_)
      dim_it->dim_->dim_ = new (std::nothrow) tiledb::Dimension(
          dim_it->domain_->domain_->dimension(dim_it->current_dim_));
    else
      dim_it->dim_->dim_ = nullptr;
  }
  return TILEDB_OK;
}

int tiledb_dimension_iter_here(
    tiledb_ctx_t* ctx,
    tiledb_dimension_iter_t* dim_it,
    const tiledb_dimension_t** dim) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, dim_it) == TILEDB_ERR)
    return TILEDB_ERR;
  *dim = dim_it->dim_;
  return TILEDB_OK;
}

int tiledb_dimension_iter_first(
    tiledb_ctx_t* ctx, tiledb_dimension_iter_t* dim_it) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, dim_it) == TILEDB_ERR)
    return TILEDB_ERR;

  dim_it->current_dim_ = 0;
  if (dim_it->dim_ != nullptr) {
    delete dim_it->dim_->dim_;
    if (dim_it->dim_num_ > 0)
      dim_it->dim_->dim_ = new (std::nothrow)
          tiledb::Dimension(dim_it->domain_->domain_->dimension(0));
    else
      dim_it->dim_->dim_ = nullptr;
  }

  return TILEDB_OK;
}

/* ****************************** */
/*           ARRAY METADATA       */
/* ****************************** */

int tiledb_array_metadata_create(
    tiledb_ctx_t* ctx,
    tiledb_array_metadata_t** array_metadata,
    const char* array_name) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  // Check array name
  tiledb::URI array_uri(array_name);
  if (array_uri.is_invalid()) {
    tiledb::Status st = tiledb::Status::Error(
        "Failed to create array metadata; Invalid array URI");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  // Create array metadata struct
  *array_metadata = new (std::nothrow) tiledb_array_metadata_t;
  if (*array_metadata == nullptr) {
    tiledb::Status st = tiledb::Status::Error(
        "Failed to allocate TileDB array metadata struct");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Create a new ArrayMetadata object
  (*array_metadata)->array_metadata_ =
      new (std::nothrow) tiledb::ArrayMetadata(tiledb::URI(array_name));
  if ((*array_metadata)->array_metadata_ == nullptr) {
    delete *array_metadata;
    *array_metadata = nullptr;
    tiledb::Status st = tiledb::Status::Error(
        "Failed to allocate TileDB array metadata "
        "object in struct");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Success
  return TILEDB_OK;
}

int tiledb_array_metadata_free(
    tiledb_ctx_t* ctx, tiledb_array_metadata_t* array_metadata) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_metadata) == TILEDB_ERR)
    return TILEDB_ERR;

  delete array_metadata->array_metadata_;
  delete array_metadata;

  return TILEDB_OK;
}

int tiledb_array_metadata_add_attribute(
    tiledb_ctx_t* ctx,
    tiledb_array_metadata_t* array_metadata,
    tiledb_attribute_t* attr) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_metadata) == TILEDB_ERR ||
      sanity_check(ctx, attr) == TILEDB_ERR)
    return TILEDB_ERR;
  if (save_error(
          ctx, array_metadata->array_metadata_->add_attribute(attr->attr_)))
    return TILEDB_ERR;
  return TILEDB_OK;
}

int tiledb_array_metadata_set_domain(
    tiledb_ctx_t* ctx,
    tiledb_array_metadata_t* array_metadata,
    tiledb_domain_t* domain) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_metadata) == TILEDB_ERR)
    return TILEDB_ERR;
  if (save_error(
          ctx, array_metadata->array_metadata_->set_domain(domain->domain_)))
    return TILEDB_ERR;
  return TILEDB_OK;
}

int tiledb_array_metadata_set_as_kv(
    tiledb_ctx_t* ctx, tiledb_array_metadata_t* array_metadata) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_metadata) == TILEDB_ERR)
    return TILEDB_ERR;
  if (save_error(ctx, array_metadata->array_metadata_->set_as_kv()))
    return TILEDB_ERR;
  return TILEDB_OK;
}

int tiledb_array_metadata_set_capacity(
    tiledb_ctx_t* ctx,
    tiledb_array_metadata_t* array_metadata,
    uint64_t capacity) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_metadata) == TILEDB_ERR)
    return TILEDB_ERR;
  array_metadata->array_metadata_->set_capacity(capacity);
  return TILEDB_OK;
}

int tiledb_array_metadata_set_cell_order(
    tiledb_ctx_t* ctx,
    tiledb_array_metadata_t* array_metadata,
    tiledb_layout_t cell_order) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_metadata) == TILEDB_ERR)
    return TILEDB_ERR;
  array_metadata->array_metadata_->set_cell_order(
      static_cast<tiledb::Layout>(cell_order));
  return TILEDB_OK;
}

int tiledb_array_metadata_set_tile_order(
    tiledb_ctx_t* ctx,
    tiledb_array_metadata_t* array_metadata,
    tiledb_layout_t tile_order) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_metadata) == TILEDB_ERR)
    return TILEDB_ERR;
  array_metadata->array_metadata_->set_tile_order(
      static_cast<tiledb::Layout>(tile_order));
  return TILEDB_OK;
}

int tiledb_array_metadata_set_array_type(
    tiledb_ctx_t* ctx,
    tiledb_array_metadata_t* array_metadata,
    tiledb_array_type_t array_type) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_metadata) == TILEDB_ERR)
    return TILEDB_ERR;
  if (save_error(
          ctx,
          array_metadata->array_metadata_->set_array_type(
              static_cast<tiledb::ArrayType>(array_type))))
    return TILEDB_ERR;
  return TILEDB_OK;
}

int tiledb_array_metadata_set_coords_compressor(
    tiledb_ctx_t* ctx,
    tiledb_array_metadata_t* array_metadata,
    tiledb_compressor_t compressor,
    int compression_level) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_metadata) == TILEDB_ERR)
    return TILEDB_ERR;
  array_metadata->array_metadata_->set_coords_compressor(
      static_cast<tiledb::Compressor>(compressor));
  array_metadata->array_metadata_->set_coords_compression_level(
      compression_level);
  return TILEDB_OK;
}

int tiledb_array_metadata_set_offsets_compressor(
    tiledb_ctx_t* ctx,
    tiledb_array_metadata_t* array_metadata,
    tiledb_compressor_t compressor,
    int compression_level) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_metadata) == TILEDB_ERR)
    return TILEDB_ERR;
  array_metadata->array_metadata_->set_cell_var_offsets_compressor(
      static_cast<tiledb::Compressor>(compressor));
  array_metadata->array_metadata_->set_cell_var_offsets_compression_level(
      compression_level);
  return TILEDB_OK;
}

int tiledb_array_metadata_check(
    tiledb_ctx_t* ctx, tiledb_array_metadata_t* array_metadata) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_metadata) == TILEDB_ERR)
    return TILEDB_ERR;

  if (save_error(ctx, array_metadata->array_metadata_->check()))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_array_metadata_load(
    tiledb_ctx_t* ctx,
    tiledb_array_metadata_t** array_metadata,
    const char* array_name) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;
  // Create array metadata
  *array_metadata = new (std::nothrow) tiledb_array_metadata_t;
  if (*array_metadata == nullptr) {
    tiledb::Status st = tiledb::Status::Error(
        "Failed to allocate TileDB array_metadata struct");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Load array metadata
  auto storage_manager = ctx->storage_manager_;
  if (save_error(
          ctx,
          storage_manager->load_array_metadata(
              tiledb::URI(array_name),
              &((*array_metadata)->array_metadata_)))) {
    delete *array_metadata;
    return TILEDB_ERR;
  }

  return TILEDB_OK;
}

int tiledb_array_metadata_get_array_name(
    tiledb_ctx_t* ctx,
    const tiledb_array_metadata_t* array_metadata,
    const char** array_name) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_metadata) == TILEDB_ERR)
    return TILEDB_ERR;
  const tiledb::URI& uri = array_metadata->array_metadata_->array_uri();
  *array_name = uri.c_str();
  return TILEDB_OK;
}

int tiledb_array_metadata_get_array_type(
    tiledb_ctx_t* ctx,
    const tiledb_array_metadata_t* array_metadata,
    tiledb_array_type_t* array_type) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_metadata) == TILEDB_ERR)
    return TILEDB_ERR;
  *array_type = static_cast<tiledb_array_type_t>(
      array_metadata->array_metadata_->array_type());
  return TILEDB_OK;
}

int tiledb_array_metadata_get_capacity(
    tiledb_ctx_t* ctx,
    const tiledb_array_metadata_t* array_metadata,
    uint64_t* capacity) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_metadata) == TILEDB_ERR)
    return TILEDB_ERR;
  *capacity = array_metadata->array_metadata_->capacity();
  return TILEDB_OK;
}

int tiledb_array_metadata_get_cell_order(
    tiledb_ctx_t* ctx,
    const tiledb_array_metadata_t* array_metadata,
    tiledb_layout_t* cell_order) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_metadata) == TILEDB_ERR)
    return TILEDB_ERR;
  *cell_order = static_cast<tiledb_layout_t>(
      array_metadata->array_metadata_->cell_order());
  return TILEDB_OK;
}

int tiledb_array_metadata_get_coords_compressor(
    tiledb_ctx_t* ctx,
    const tiledb_array_metadata_t* array_metadata,
    tiledb_compressor_t* compressor,
    int* compression_level) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_metadata) == TILEDB_ERR)
    return TILEDB_ERR;

  *compressor = static_cast<tiledb_compressor_t>(
      array_metadata->array_metadata_->coords_compression());
  *compression_level =
      array_metadata->array_metadata_->coords_compression_level();
  return TILEDB_OK;
}

int tiledb_array_metadata_get_offsets_compressor(
    tiledb_ctx_t* ctx,
    const tiledb_array_metadata_t* array_metadata,
    tiledb_compressor_t* compressor,
    int* compression_level) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_metadata) == TILEDB_ERR)
    return TILEDB_ERR;

  *compressor = static_cast<tiledb_compressor_t>(
      array_metadata->array_metadata_->cell_var_offsets_compression());
  *compression_level =
      array_metadata->array_metadata_->cell_var_offsets_compression_level();
  return TILEDB_OK;
}

int tiledb_array_metadata_get_domain(
    tiledb_ctx_t* ctx,
    const tiledb_array_metadata_t* array_metadata,
    tiledb_domain_t** domain) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_metadata) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create a domain struct
  *domain = new (std::nothrow) tiledb_domain_t;
  if (*domain == nullptr) {
    tiledb::Status st =
        tiledb::Status::Error("Failed to allocate TileDB domain struct");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Create a new Domain object
  (*domain)->domain_ = new (std::nothrow)
      tiledb::Domain(array_metadata->array_metadata_->domain());
  if ((*domain)->domain_ == nullptr) {
    delete *domain;
    tiledb::Status st = tiledb::Status::Error(
        "Failed to allocate TileDB domain object in struct");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }
  return TILEDB_OK;
}

int tiledb_array_metadata_get_as_kv(
    tiledb_ctx_t* ctx, tiledb_array_metadata_t* array_metadata, int* as_kv) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_metadata) == TILEDB_ERR)
    return TILEDB_ERR;
  *as_kv = (int)array_metadata->array_metadata_->is_kv();
  return TILEDB_OK;
}

int tiledb_array_metadata_get_tile_order(
    tiledb_ctx_t* ctx,
    const tiledb_array_metadata_t* array_metadata,
    tiledb_layout_t* tile_order) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_metadata) == TILEDB_ERR)
    return TILEDB_ERR;
  *tile_order = static_cast<tiledb_layout_t>(
      array_metadata->array_metadata_->tile_order());
  return TILEDB_OK;
}

int tiledb_array_metadata_get_num_attributes(
    tiledb_ctx_t* ctx,
    const tiledb_array_metadata_t* array_metadata,
    unsigned int* num_attributes) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_metadata) == TILEDB_ERR)
    return TILEDB_ERR;
  *num_attributes = array_metadata->array_metadata_->attribute_num();
  return TILEDB_OK;
}

int tiledb_array_metadata_dump(
    tiledb_ctx_t* ctx,
    const tiledb_array_metadata_t* array_metadata,
    FILE* out) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_metadata) == TILEDB_ERR)
    return TILEDB_ERR;
  array_metadata->array_metadata_->dump(out);
  return TILEDB_OK;
}

int tiledb_attribute_from_index(
    tiledb_ctx_t* ctx,
    const tiledb_array_metadata_t* array_metadata,
    unsigned int index,
    tiledb_attribute_t** attr) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_metadata) == TILEDB_ERR) {
    return TILEDB_ERR;
  }
  unsigned int attribute_num = array_metadata->array_metadata_->attribute_num();
  if (attribute_num == 0) {
    *attr = nullptr;
    return TILEDB_OK;
  }
  if (index > attribute_num) {
    std::ostringstream errmsg;
    errmsg << "Attribute index: " << index << " exceeds number of attributes("
           << attribute_num << ") for array "
           << array_metadata->array_metadata_->array_uri().to_string();
    tiledb::Status st = tiledb::Status::ArrayMetadataError(errmsg.str());
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  auto found_attr = array_metadata->array_metadata_->attribute(index);

  *attr = new (std::nothrow) tiledb_attribute_t;
  if (*attr == nullptr) {
    tiledb::Status st =
        tiledb::Status::Error("Failed to allocate TileDB attribute");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Create an attribute object
  (*attr)->attr_ = new (std::nothrow) tiledb::Attribute(found_attr);

  // Check for allocation error
  if ((*attr)->attr_ == nullptr) {
    delete *attr;
    tiledb::Status st =
        tiledb::Status::Error("Failed to allocate TileDB attribute");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }
  return TILEDB_OK;
}

int tiledb_attribute_from_name(
    tiledb_ctx_t* ctx,
    const tiledb_array_metadata_t* array_metadata,
    const char* name,
    tiledb_attribute_t** attr) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_metadata) == TILEDB_ERR) {
    return TILEDB_ERR;
  }
  unsigned int attribute_num = array_metadata->array_metadata_->attribute_num();
  if (attribute_num == 0) {
    *attr = nullptr;
    return TILEDB_OK;
  }
  std::string name_string(name);
  auto found_attr = array_metadata->array_metadata_->attribute(name_string);
  if (found_attr == nullptr) {
    tiledb::Status st = tiledb::Status::ArrayMetadataError(
        std::string("Attribute name: ") + name + " does not exist for array " +
        array_metadata->array_metadata_->array_uri().to_string());
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }
  *attr = new (std::nothrow) tiledb_attribute_t;
  if (*attr == nullptr) {
    tiledb::Status st =
        tiledb::Status::Error("Failed to allocate TileDB attribute");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }
  // Create an attribute object
  (*attr)->attr_ = new (std::nothrow) tiledb::Attribute(found_attr);
  // Check for allocation error
  if ((*attr)->attr_ == nullptr) {
    delete *attr;
    tiledb::Status st =
        tiledb::Status::Error("Failed to allocate TileDB attribute");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }
  return TILEDB_OK;
}

/* ****************************** */
/*       ATTRIBUTE ITERATOR       */
/* ****************************** */

int tiledb_attribute_iter_create(
    tiledb_ctx_t* ctx,
    const tiledb_array_metadata_t* array_metadata,
    tiledb_attribute_iter_t** attr_it) {
  if (sanity_check(ctx, array_metadata) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create attribute iterator struct
  *attr_it = new (std::nothrow) tiledb_attribute_iter_t;
  if (*attr_it == nullptr) {
    tiledb::Status st = tiledb::Status::Error(
        "Failed to allocate TileDB attribute iterator struct");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Initialize the iterator for the array metadata
  (*attr_it)->array_metadata_ = array_metadata;
  (*attr_it)->attr_num_ = array_metadata->array_metadata_->attribute_num();

  // Initialize the rest members of the iterator
  (*attr_it)->current_attr_ = 0;
  if ((*attr_it)->attr_num_ == 0) {
    (*attr_it)->attr_ = nullptr;
  } else {
    // Create an attribute struct inside the iterator struct
    (*attr_it)->attr_ = new (std::nothrow) tiledb_attribute_t;
    if ((*attr_it)->attr_ == nullptr) {
      tiledb::Status st = tiledb::Status::Error(
          "Failed to allocate TileDB attribute struct "
          "in iterator struct");
      LOG_STATUS(st);
      save_error(ctx, st);
      delete *attr_it;
      return TILEDB_OOM;
    }

    // Create an attribute object
    (*attr_it)->attr_->attr_ = new (std::nothrow)
        tiledb::Attribute(array_metadata->array_metadata_->attribute(0));

    // Check for allocation error
    if ((*attr_it)->attr_->attr_ == nullptr) {
      delete (*attr_it)->attr_;
      delete *attr_it;
      tiledb::Status st = tiledb::Status::Error(
          "Failed to allocate TileDB attribute object "
          "in iterator struct");
      LOG_STATUS(st);
      save_error(ctx, st);
      return TILEDB_OOM;
    }
  }

  // Success
  return TILEDB_OK;
}

int tiledb_attribute_iter_free(
    tiledb_ctx_t* ctx, tiledb_attribute_iter_t* attr_it) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, attr_it) == TILEDB_ERR)

    if (attr_it->attr_ != nullptr) {
      if (attr_it->attr_->attr_ != nullptr)
        delete (attr_it->attr_->attr_);
      delete attr_it->attr_;
    }
  delete attr_it;
  return TILEDB_OK;
}

int tiledb_attribute_iter_done(
    tiledb_ctx_t* ctx, tiledb_attribute_iter_t* attr_it, int* done) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, attr_it) == TILEDB_ERR)
    return TILEDB_ERR;
  *done = (attr_it->current_attr_ == attr_it->attr_num_) ? 1 : 0;
  return TILEDB_OK;
}

int tiledb_attribute_iter_next(
    tiledb_ctx_t* ctx, tiledb_attribute_iter_t* attr_it) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, attr_it) == TILEDB_ERR)
    return TILEDB_ERR;
  ++(attr_it->current_attr_);
  if (attr_it->attr_ != nullptr) {
    delete attr_it->attr_->attr_;
    if (attr_it->current_attr_ < attr_it->attr_num_) {
      attr_it->attr_->attr_ = new (std::nothrow) tiledb::Attribute(
          attr_it->array_metadata_->array_metadata_->attribute(
              attr_it->current_attr_));
    } else {
      attr_it->attr_->attr_ = nullptr;
    }
  }
  return TILEDB_OK;
}

int tiledb_attribute_iter_here(
    tiledb_ctx_t* ctx,
    tiledb_attribute_iter_t* attr_it,
    const tiledb_attribute_t** attr) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, attr_it) == TILEDB_ERR)
    return TILEDB_ERR;
  *attr = attr_it->attr_;
  return TILEDB_OK;
}

int tiledb_attribute_iter_first(
    tiledb_ctx_t* ctx, tiledb_attribute_iter_t* attr_it) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, attr_it) == TILEDB_ERR)
    return TILEDB_ERR;
  attr_it->current_attr_ = 0;
  if (attr_it->attr_ != nullptr) {
    delete attr_it->attr_->attr_;
    if (attr_it->attr_num_ > 0) {
      attr_it->attr_->attr_ = new (std::nothrow) tiledb::Attribute(
          attr_it->array_metadata_->array_metadata_->attribute(0));
    } else {
      attr_it->attr_->attr_ = nullptr;
    }
  }
  return TILEDB_OK;
}

/* ****************************** */
/*              QUERY             */
/* ****************************** */

int tiledb_query_create(
    tiledb_ctx_t* ctx,
    tiledb_query_t** query,
    const char* array_name,
    tiledb_query_type_t type) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create query struct
  *query = new (std::nothrow) tiledb_query_t;
  if (*query == nullptr) {
    tiledb::Status st =
        tiledb::Status::Error("Failed to allocate TileDB query struct");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Create a new Query object
  (*query)->query_ = new (std::nothrow) tiledb::Query();
  if ((*query)->query_ == nullptr) {
    delete *query;
    tiledb::Status st = tiledb::Status::Error(
        "Failed to allocate TileDB query object in struct");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Create query object
  if (save_error(
          ctx,
          ctx->storage_manager_->query_init(
              ((*query)->query_),
              array_name,
              static_cast<tiledb::QueryType>(type)))) {
    delete (*query)->query_;
    delete *query;
    return TILEDB_ERR;
  }

  // Success
  return TILEDB_OK;
}

int tiledb_query_set_subarray(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const void* subarray,
    tiledb_datatype_t type) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  // Set subarray
  if (save_error(
          ctx,
          query->query_->set_subarray(
              subarray, static_cast<tiledb::Datatype>(type))))
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
          ctx, query->query_->set_layout(static_cast<tiledb::Layout>(layout))))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_query_free(tiledb_ctx_t* ctx, tiledb_query_t* query) {
  // Trivial case
  if (query == nullptr)
    return TILEDB_OK;

  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  // Finalize query and check error
  int rc = TILEDB_OK;
  if (save_error(ctx, ctx->storage_manager_->query_finalize(query->query_)))
    rc = TILEDB_ERR;

  delete query->query_;
  delete query;

  return rc;
}

int tiledb_query_set_kv(
    tiledb_ctx_t* ctx, tiledb_query_t* query, tiledb_kv_t* kv) {
  // Sanity checks
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, query) == TILEDB_ERR ||
      sanity_check(ctx, kv) == TILEDB_ERR)
    return TILEDB_ERR;

  // Get key only in case they query reads the entire key-value store
  bool get_key = (query->query_->subarray() == nullptr);
  bool get_coords = (query->query_->type() == tiledb::QueryType::WRITE);

  // Get query attributes
  const char** attributes;
  unsigned int attribute_num;
  if (save_error(
          ctx,
          kv->kv_->get_array_attributes(
              &attributes, &attribute_num, get_coords, get_key)))
    return TILEDB_ERR;

  // Get query buffers
  void** buffers;
  uint64_t* buffer_sizes;
  if (save_error(ctx, kv->kv_->get_array_buffers(&buffers, &buffer_sizes)))
    return TILEDB_ERR;

  return tiledb_query_set_buffers(
      ctx, query, attributes, attribute_num, buffers, buffer_sizes);
}

int tiledb_query_set_kv_key(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const void* key,
    tiledb_datatype_t type,
    uint64_t key_size) {
  // Sanity checks
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;
  if (!query->query_->array_metadata()->is_kv()) {
    tiledb::Status st = tiledb::Status::Error(
        "Cannot query by key; The queried array is not a key-value store");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  // Set key
  uint64_t subarray[4];
  tiledb::KV::compute_subarray(
      key, static_cast<tiledb::Datatype>(type), key_size, subarray);

  // Set query subarray
  return tiledb_query_set_subarray(ctx, query, subarray, TILEDB_UINT64);
}

int tiledb_query_submit(tiledb_ctx_t* ctx, tiledb_query_t* query) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  // Submit query
  if (save_error(ctx, ctx->storage_manager_->query_submit(query->query_)))
    return TILEDB_ERR;

  // Success
  return TILEDB_OK;
}

int tiledb_query_submit_async(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    void* (*callback)(void*),
    void* callback_data) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  // Submit query
  if (save_error(
          ctx,
          ctx->storage_manager_->query_submit_async(
              query->query_, callback, callback_data)))
    return TILEDB_ERR;

  // Success
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

int tiledb_query_get_attribute_status(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    const char* attribute_name,
    tiledb_query_status_t* status) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  // Check if the query is still in progress or failed
  auto query_status = query->query_->status();
  if (query_status == tiledb::QueryStatus::INPROGRESS ||
      query_status == tiledb::QueryStatus::COMPLETED ||
      query_status == tiledb::QueryStatus::FAILED) {
    *status = (tiledb_query_status_t)query_status;
    return TILEDB_OK;
  }

  unsigned int overflow;
  if (save_error(ctx, query->query_->overflow(attribute_name, &overflow)))
    return TILEDB_ERR;

  *status = (overflow == 1) ?
                (tiledb_query_status_t)tiledb::QueryStatus::INCOMPLETE :
                (tiledb_query_status_t)tiledb::QueryStatus::COMPLETED;

  return TILEDB_OK;
}

/* ****************************** */
/*              ARRAY             */
/* ****************************** */

int tiledb_array_create(
    tiledb_ctx_t* ctx, const tiledb_array_metadata_t* array_metadata) {
  // Sanity checks
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_metadata) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create the array_metadata
  if (save_error(
          ctx,
          ctx->storage_manager_->array_create(array_metadata->array_metadata_)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_array_consolidate(tiledb_ctx_t* ctx, const char* array_name) {
  // Sanity checks
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  if (save_error(ctx, ctx->storage_manager_->array_consolidate(array_name)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

/* ****************************** */
/*       RESOURCE  MANAGEMENT     */
/* ****************************** */

int tiledb_object_type(
    tiledb_ctx_t* ctx, const char* path, tiledb_object_t* type) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;
  auto uri = tiledb::URI(path);
  *type = static_cast<tiledb_object_t>(ctx->storage_manager_->object_type(uri));
  return TILEDB_OK;
}

int tiledb_delete(tiledb_ctx_t* ctx, const char* path) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;
  auto uri = tiledb::URI(path);
  if (save_error(ctx, ctx->storage_manager_->remove_path(uri)))
    return TILEDB_ERR;
  return TILEDB_OK;
}

int tiledb_move(
    tiledb_ctx_t* ctx, const char* old_path, const char* new_path, bool force) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;
  auto old_uri = tiledb::URI(old_path);
  auto new_uri = tiledb::URI(new_path);
  if (save_error(ctx, ctx->storage_manager_->move(old_uri, new_uri, force)))
    return TILEDB_ERR;
  return TILEDB_OK;
}

int tiledb_walk(
    tiledb_ctx_t* ctx,
    const char* path,
    tiledb_walk_order_t order,
    int (*callback)(const char*, tiledb_object_t, void*),
    void* data) {
  // Sanity checks
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;
  if (callback == nullptr) {
    tiledb::Status st = tiledb::Status::Error(
        "Cannot initiate walk; Invalid callback function");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_ERR;
  }

  // Create an object iterator
  tiledb::StorageManager::ObjectIter* obj_iter;
  if (save_error(
          ctx,
          ctx->storage_manager_->object_iter_begin(
              &obj_iter, path, static_cast<tiledb::WalkOrder>(order))))
    return TILEDB_ERR;

  // For as long as there is another object and the callback indicates to
  // continue, walk over the TileDB objects in the path
  const char* obj_name;
  tiledb::ObjectType obj_type;
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
/*             KEY-VALUE          */
/* ****************************** */

int tiledb_kv_create(
    tiledb_ctx_t* ctx,
    tiledb_kv_t** kv,
    unsigned int attribute_num,
    const char** attributes,
    tiledb_datatype_t* types,
    unsigned int* nitems) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create key-value store struct
  *kv = new (std::nothrow) tiledb_kv_t;
  if (*kv == nullptr) {
    tiledb::Status st = tiledb::Status::Error(
        "Failed to allocate TileDB in-memory key-value store struct");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Prepare attributes/types/nitems vectors
  std::vector<std::string> attributes_vec;
  std::vector<tiledb::Datatype> types_vec;
  std::vector<unsigned int> nitems_vec;
  for (unsigned int i = 0; i < attribute_num; ++i) {
    attributes_vec.emplace_back(attributes[i]);
    types_vec.emplace_back(static_cast<tiledb::Datatype>(types[i]));
    nitems_vec.emplace_back(nitems[i]);
  }

  // Create a new KV object
  (*kv)->kv_ = new tiledb::KV(attributes_vec, types_vec, nitems_vec);
  if ((*kv)->kv_ == nullptr) {
    delete *kv;
    *kv = nullptr;
    tiledb::Status st = tiledb::Status::Error(
        "Failed to allocate TileDB in-memory key-value "
        "store object in struct");
    LOG_STATUS(st);
    save_error(ctx, st);
    return TILEDB_OOM;
  }

  // Success
  return TILEDB_OK;
}

int tiledb_kv_free(tiledb_ctx_t* ctx, tiledb_kv_t* kv) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, kv) == TILEDB_ERR)
    return TILEDB_ERR;

  delete kv->kv_;
  delete kv;

  return TILEDB_OK;
}

int tiledb_kv_add_key(
    tiledb_ctx_t* ctx,
    tiledb_kv_t* kv,
    const void* key,
    tiledb_datatype_t key_type,
    uint64_t key_size) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, kv) == TILEDB_ERR)
    return TILEDB_ERR;

  // Set key
  if (save_error(
          ctx,
          kv->kv_->add_key(
              key, static_cast<tiledb::Datatype>(key_type), key_size)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_kv_add_value(
    tiledb_ctx_t* ctx,
    tiledb_kv_t* kv,
    unsigned int attribute_idx,
    const void* value) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, kv) == TILEDB_ERR)
    return TILEDB_ERR;

  // Set value
  if (save_error(ctx, kv->kv_->add_value(attribute_idx, value)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_kv_add_value_var(
    tiledb_ctx_t* ctx,
    tiledb_kv_t* kv,
    unsigned int attribute_idx,
    const void* value,
    uint64_t value_size) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, kv) == TILEDB_ERR)
    return TILEDB_ERR;

  // Set value
  if (save_error(ctx, kv->kv_->add_value(attribute_idx, value, value_size)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_kv_get_key_num(tiledb_ctx_t* ctx, tiledb_kv_t* kv, uint64_t* num) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, kv) == TILEDB_ERR)
    return TILEDB_ERR;

  *num = kv->kv_->key_num();

  return TILEDB_OK;
}

int tiledb_kv_get_value_num(
    tiledb_ctx_t* ctx,
    tiledb_kv_t* kv,
    unsigned int attribute_idx,
    uint64_t* num) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, kv) == TILEDB_ERR)
    return TILEDB_ERR;

  if (save_error(ctx, kv->kv_->value_num(attribute_idx, num)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_kv_get_key(
    tiledb_ctx_t* ctx,
    tiledb_kv_t* kv,
    uint64_t key_idx,
    void** key,
    tiledb_datatype_t* key_type,
    uint64_t* key_size) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, kv) == TILEDB_ERR)
    return TILEDB_ERR;

  // Get the key
  tiledb::Datatype key_type_d;
  if (save_error(ctx, kv->kv_->get_key(key_idx, key, &key_type_d, key_size)))
    return TILEDB_ERR;
  *key_type = static_cast<tiledb_datatype_t>(key_type_d);

  return TILEDB_OK;
}

int tiledb_kv_get_value(
    tiledb_ctx_t* ctx,
    tiledb_kv_t* kv,
    uint64_t obj_idx,
    unsigned int attr_idx,
    void** value) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, kv) == TILEDB_ERR)
    return TILEDB_ERR;

  // Get the value
  if (save_error(ctx, kv->kv_->get_value(obj_idx, attr_idx, value)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_kv_get_value_var(
    tiledb_ctx_t* ctx,
    tiledb_kv_t* kv,
    uint64_t obj_idx,
    unsigned int attr_idx,
    void** value,
    uint64_t* value_size) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, kv) == TILEDB_ERR)
    return TILEDB_ERR;

  // Get the value
  if (save_error(ctx, kv->kv_->get_value(obj_idx, attr_idx, value, value_size)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_kv_set_buffer_size(
    tiledb_ctx_t* ctx, tiledb_kv_t* kv, uint64_t nbytes) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, kv) == TILEDB_ERR)
    return TILEDB_ERR;

  // Set the buffer size
  kv->kv_->set_buffer_alloc_size(nbytes);

  return TILEDB_OK;
}
