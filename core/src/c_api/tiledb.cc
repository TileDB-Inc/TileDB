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

#include <cstdlib>

#include "array_schema.h"
#include "basic_array.h"
#include "metadata_schema.h"
#include "query.h"
#include "query_type.h"
#include "storage_manager.h"
#include "tiledb.h"
#include "utils.h"

/* ****************************** */
/*            CONSTANTS           */
/* ****************************** */

const char* tiledb_key() {
  return tiledb::Configurator::key();
}

int tiledb_var_num() {
  return tiledb::Configurator::var_num();
}

uint64_t tiledb_var_size() {
  return tiledb::Configurator::var_size();
}

/* ****************************** */
/*            VERSION             */
/* ****************************** */

void tiledb_version(int* major, int* minor, int* rev) {
  *major = TILEDB_VERSION_MAJOR;
  *minor = TILEDB_VERSION_MINOR;
  *rev = TILEDB_VERSION_REVISION;
}

/* ********************************* */
/*           TILEDB TYPES            */
/* ********************************* */

struct tiledb_ctx_t {
  // storage manager instance
  tiledb::StorageManager* storage_manager_;
  // last error associated with this context
  tiledb::Status* last_error_;
};

struct tiledb_config_t {
  // The configurator instance
  tiledb::Configurator* config_;
};

struct tiledb_error_t {
  // Pointer to a copy of the last TileDB error associated with a given ctx
  const tiledb::Status* status_;
  std::string* errmsg_;
};

struct tiledb_basic_array_t {
  tiledb::BasicArray* basic_array_;
};

struct tiledb_attribute_t {
  tiledb::Attribute* attr_;
};

struct tiledb_dimension_t {
  tiledb::Dimension* dim_;
};

struct tiledb_array_schema_t {
  tiledb::ArraySchema* array_schema_;
};

struct tiledb_attribute_iter_t {
  const tiledb_array_schema_t* array_schema_;
  const tiledb_metadata_schema_t* metadata_schema_;
  tiledb_attribute_t* attr_;
  int attr_num_;
  int current_attr_;
};

struct tiledb_dimension_iter_t {
  const tiledb_array_schema_t* array_schema_;
  tiledb_dimension_t* dim_;
  int dim_num_;
  int current_dim_;
};

struct tiledb_query_t {
  tiledb::Query* query_;
};

struct tiledb_array_t {
  tiledb::Array* array_;
};

struct tiledb_metadata_schema_t {
  tiledb::MetadataSchema* metadata_schema_;
};

struct tiledb_metadata_t {
  tiledb::Metadata* metadata_;
};

/* ********************************* */
/*         AUXILIARY FUNCTIONS       */
/* ********************************* */

/* Auxiliary function that saves a status inside the context object. */
static bool save_error(tiledb_ctx_t* ctx, const tiledb::Status& st) {
  // No error
  if (st.ok())
    return false;

  // Delete previous error
  if (ctx->last_error_ != nullptr)
    delete ctx->last_error_;

  // Store new error and return
  ctx->last_error_ = new tiledb::Status(st);

  // There is an error
  return true;
}

inline int sanity_check(tiledb_ctx_t* ctx) {
  if (ctx == nullptr)
    return TILEDB_ERR;
  if (ctx->storage_manager_ == nullptr) {
    save_error(ctx, tiledb::Status::Error("Invalid TileDB context"));
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int sanity_check(tiledb_ctx_t* ctx, const tiledb_config_t* config) {
  if (config == nullptr || config->config_ == nullptr) {
    save_error(
        ctx, tiledb::Status::Error("Invalid TileDB configurator struct"));
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int sanity_check(tiledb_ctx_t* ctx, const tiledb_error_t* err) {
  if (err == nullptr || err->status_ == nullptr) {
    save_error(ctx, tiledb::Status::Error("Invalid TileDB error struct"));
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int sanity_check(tiledb_ctx_t* ctx, const tiledb_attribute_t* attr) {
  if (attr == nullptr || attr->attr_ == nullptr) {
    save_error(ctx, tiledb::Status::Error("Invalid TileDB attribute struct"));
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int sanity_check(
    tiledb_ctx_t* ctx, const tiledb_attribute_iter_t* attr_it) {
  if (attr_it == nullptr || (attr_it->array_schema_ == nullptr &&
                             attr_it->metadata_schema_ == nullptr)) {
    save_error(
        ctx, tiledb::Status::Error("Invalid TileDB attribute iterator struct"));
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int sanity_check(
    tiledb_ctx_t* ctx, const tiledb_dimension_iter_t* dim_it) {
  if (dim_it == nullptr || dim_it->array_schema_ == nullptr) {
    save_error(
        ctx, tiledb::Status::Error("Invalid TileDB dimension iterator struct"));
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int sanity_check(tiledb_ctx_t* ctx, const tiledb_dimension_t* dim) {
  if (dim == nullptr || dim->dim_ == nullptr) {
    save_error(ctx, tiledb::Status::Error("Invalid TileDB dimension struct"));
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int sanity_check(
    tiledb_ctx_t* ctx, const tiledb_array_schema_t* array_schema) {
  if (array_schema == nullptr || array_schema->array_schema_ == nullptr) {
    save_error(
        ctx, tiledb::Status::Error("Invalid TileDB array schema struct"));
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int sanity_check(
    tiledb_ctx_t* ctx, const tiledb_metadata_schema_t* metadata_schema) {
  if (metadata_schema == nullptr ||
      metadata_schema->metadata_schema_ == nullptr) {
    save_error(
        ctx, tiledb::Status::Error("Invalid TileDB metadata schema struct"));
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int sanity_check(tiledb_ctx_t* ctx, const tiledb_query_t* query) {
  if (query == nullptr || query->query_ == nullptr) {
    save_error(ctx, tiledb::Status::Error("Invalid TileDB query struct"));
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

inline int sanity_check(tiledb_ctx_t* ctx, const tiledb_array_t* array) {
  if (array == nullptr || array->array_ == nullptr) {
    save_error(ctx, tiledb::Status::Error("Invalid TileDB array struct"));
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

/* ****************************** */
/*            CONTEXT             */
/* ****************************** */

int tiledb_ctx_create(tiledb_ctx_t** ctx) {
  // Initialize context
  *ctx = (tiledb_ctx_t*)malloc(sizeof(struct tiledb_ctx_t));
  if (*ctx == nullptr)
    return TILEDB_OOM;

  // Create storage manager
  (*ctx)->storage_manager_ = new tiledb::StorageManager();
  if ((*ctx)->storage_manager_ == nullptr) {
    save_error(
        *ctx,
        tiledb::Status::Error(
            "Failed to allocate storage manager in TileDB context"));
    return TILEDB_ERR;
  }

  // Initialize last error
  (*ctx)->last_error_ = nullptr;

  // Initialize storage manager
  if (save_error(*ctx, ((*ctx)->storage_manager_->init()))) {
    delete (*ctx)->storage_manager_;
    (*ctx)->storage_manager_ = nullptr;
    return TILEDB_ERR;
  }

  // Success
  return TILEDB_OK;
}

void tiledb_ctx_free(tiledb_ctx_t* ctx) {
  if (ctx == nullptr)
    return;
  if (ctx->storage_manager_ != nullptr)
    delete ctx->storage_manager_;
  if (ctx->last_error_ != nullptr)
    delete ctx->last_error_;
  free(ctx);
}

int tiledb_ctx_set_config(tiledb_ctx_t* ctx, tiledb_config_t* config) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, config) == TILEDB_ERR)
    return TILEDB_ERR;

  ctx->storage_manager_->config_set(config->config_);

  return TILEDB_OK;
}

/* ********************************* */
/*              CONFIG               */
/* ********************************* */

int tiledb_config_create(tiledb_ctx_t* ctx, tiledb_config_t** config) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;
  *config = (tiledb_config_t*)malloc(sizeof(tiledb_config_t));
  if (*config == nullptr) {
    save_error(
        ctx, tiledb::Status::Error("Failed to allocate configurator struct"));
    return TILEDB_OOM;
  }
  (*config)->config_ = new tiledb::Configurator();
  if ((*config)->config_ == nullptr) {  // Allocation error
    free(*config);
    *config = nullptr;
    save_error(
        ctx,
        tiledb::Status::Error(
            "Failed to allocate configurator object in struct"));
    return TILEDB_OOM;
  }
  return TILEDB_OK;
}

void tiledb_config_free(tiledb_config_t* config) {
  if (config == nullptr)
    return;
  if (config->config_ != nullptr)
    delete config->config_;
  free(config);
}

#ifdef HAVE_MPI
int tiledb_config_set_mpi_comm(
    tiledb_ctx_t* ctx, tiledb_config_t* config, MPI_Comm* mpi_comm) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, config) == TILEDB_ERR)
    return TILEDB_ERR;
  config->config_->set_mpi_comm(mpi_comm);
  return TILEDB_OK;
}
#endif

int tiledb_config_set_read_method(
    tiledb_ctx_t* ctx, tiledb_config_t* config, tiledb_io_t read_method) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, config) == TILEDB_ERR)
    return TILEDB_ERR;
  config->config_->set_read_method(static_cast<tiledb::IOMethod>(read_method));
  return TILEDB_OK;
}

int tiledb_config_set_write_method(
    tiledb_ctx_t* ctx, tiledb_config_t* config, tiledb_io_t write_method) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, config) == TILEDB_ERR)
    return TILEDB_ERR;
  config->config_->set_write_method(
      static_cast<tiledb::IOMethod>(write_method));
  return TILEDB_OK;
}

/* ********************************* */
/*              ERROR                */
/* ********************************* */

int tiledb_error_last(tiledb_ctx_t* ctx, tiledb_error_t** err) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  // No last error
  if (ctx->last_error_ == nullptr) {
    *err = nullptr;
    return TILEDB_OK;
  }

  // Create error struct
  *err = (tiledb_error_t*)malloc(sizeof(tiledb_error_t));
  if (*err == nullptr) {
    save_error(ctx, tiledb::Status::Error("Failed to allocate error struct"));
    return TILEDB_OOM;
  }

  // Create status
  (*err)->status_ = new tiledb::Status(*(ctx->last_error_));
  if ((*err)->status_ == nullptr) {
    free(*err);
    *err = nullptr;
    save_error(
        ctx,
        tiledb::Status::Error(
            "Failed to allocate status object in TileDB error struct"));
    return TILEDB_OOM;
  }

  // Set error message
  (*err)->errmsg_ = new std::string((*err)->status_->to_string());

  // Success
  return TILEDB_OK;
}

int tiledb_error_message(
    tiledb_ctx_t* ctx, tiledb_error_t* err, const char** errmsg) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, err) == TILEDB_ERR)
    return TILEDB_ERR;
  // Set error message
  if (err->status_->ok())
    *errmsg = nullptr;
  else
    *errmsg = err->errmsg_->c_str();
  return TILEDB_OK;
}

void tiledb_error_free(tiledb_error_t* err) {
  if (err != nullptr) {
    if (err->status_ != nullptr) {
      delete err->status_;
    }
    delete err->errmsg_;
    free(err);
  }
}

/* ****************************** */
/*              GROUP             */
/* ****************************** */

int tiledb_group_create(tiledb_ctx_t* ctx, const char* group) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create the group
  if (save_error(ctx, ctx->storage_manager_->group_create(group)))
    return TILEDB_ERR;

  // Success
  return TILEDB_OK;
}

/* ********************************* */
/*            BASIC ARRAY            */
/* ********************************* */

int tiledb_basic_array_create(tiledb_ctx_t* ctx, const char* name) {
  /* // TODO
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create the basic array
  if (save_error(ctx, ctx->storage_manager_->basic_array_create(name)))
    return TILEDB_ERR;

   */

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
  *attr = (tiledb_attribute_t*)malloc(sizeof(tiledb_attribute_t));
  if (*attr == nullptr) {
    save_error(
        ctx,
        tiledb::Status::Error("Failed to allocate TileDB attribute struct"));
    return TILEDB_OOM;
  }

  // Create a new Attribute object
  (*attr)->attr_ =
      new tiledb::Attribute(name, static_cast<tiledb::Datatype>(type));
  if ((*attr)->attr_ == nullptr) {
    free(*attr);
    *attr = nullptr;
    save_error(
        ctx,
        tiledb::Status::Error(
            "Failed to allocate TileDB attribute object in struct"));
    return TILEDB_OOM;
  }

  // Success
  return TILEDB_OK;
}

void tiledb_attribute_free(tiledb_attribute_t* attr) {
  if (attr == nullptr)
    return;
  if (attr->attr_ != nullptr)
    delete attr->attr_;
  free(attr);
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
/*            DIMENSION              */
/* ********************************* */

int tiledb_dimension_create(
    tiledb_ctx_t* ctx,
    tiledb_dimension_t** dim,
    const char* name,
    tiledb_datatype_t type,
    const void* domain,
    const void* tile_extent) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create an dimension struct
  *dim = (tiledb_dimension_t*)malloc(sizeof(tiledb_dimension_t));
  if (*dim == nullptr) {
    save_error(
        ctx,
        tiledb::Status::Error("Failed to allocate TileDB dimension struct"));
    return TILEDB_OOM;
  }
  // Create a new Dimension object
  (*dim)->dim_ = new tiledb::Dimension(
      name, static_cast<tiledb::Datatype>(type), domain, tile_extent);
  if ((*dim)->dim_ == nullptr) {
    free(*dim);
    *dim = nullptr;
    save_error(
        ctx,
        tiledb::Status::Error(
            "Failed to allocate TileDB dimension object in struct"));
    return TILEDB_OOM;
  }
  return TILEDB_OK;
}

void tiledb_dimension_free(tiledb_dimension_t* dim) {
  if (dim == nullptr)
    return;
  if (dim->dim_ != nullptr)
    delete dim->dim_;
  free(dim);
}

int tiledb_dimension_set_compressor(
    tiledb_ctx_t* ctx,
    tiledb_dimension_t* dim,
    tiledb_compressor_t compressor,
    int compression_level) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, dim) == TILEDB_ERR)
    return TILEDB_ERR;
  dim->dim_->set_compressor(static_cast<tiledb::Compressor>(compressor));
  dim->dim_->set_compression_level(compression_level);
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

int tiledb_dimension_get_compressor(
    tiledb_ctx_t* ctx,
    const tiledb_dimension_t* dim,
    tiledb_compressor_t* compressor,
    int* compression_level) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, dim) == TILEDB_ERR)
    return TILEDB_ERR;
  *compressor = static_cast<tiledb_compressor_t>(dim->dim_->compressor());
  *compression_level = dim->dim_->compression_level();
  return TILEDB_OK;
}

int tiledb_dimension_get_domain(
    tiledb_ctx_t* ctx, const tiledb_dimension_t* dim, const void** domain) {
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, dim) == TILEDB_ERR)
    return TILEDB_ERR;
  *domain = dim->dim_->domain();
  return TILEDB_OK;
}

int tiledb_dimension_get_tile_extent(
    tiledb_ctx_t* ctx,
    const tiledb_dimension_t* dim,
    const void** tile_extent) {
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

/* ****************************** */
/*           ARRAY SCHEMA         */
/* ****************************** */

int tiledb_array_schema_create(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t** array_schema,
    const char* array_name) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create array schema struct
  *array_schema = (tiledb_array_schema_t*)malloc(sizeof(tiledb_array_schema_t));
  if (*array_schema == nullptr) {
    save_error(
        ctx,
        tiledb::Status::Error("Failed to allocate TileDB array schema struct"));
    return TILEDB_OOM;
  }

  // Create a new ArraySchema object
  (*array_schema)->array_schema_ = new tiledb::ArraySchema(array_name);
  if ((*array_schema)->array_schema_ == nullptr) {
    free(*array_schema);
    *array_schema = nullptr;
    save_error(
        ctx,
        tiledb::Status::Error(
            "Failed to allocate TileDB array schema object in struct"));
    return TILEDB_OOM;
  }

  // Success
  return TILEDB_OK;
}

void tiledb_array_schema_free(tiledb_array_schema_t* array_schema) {
  if (array_schema == nullptr)
    return;

  if (array_schema->array_schema_ != nullptr)
    delete array_schema->array_schema_;

  free(array_schema);
}

int tiledb_array_schema_add_attribute(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_attribute_t* attr) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR ||
      sanity_check(ctx, attr) == TILEDB_ERR)
    return TILEDB_ERR;
  array_schema->array_schema_->add_attribute(attr->attr_);
  return TILEDB_OK;
}

int tiledb_array_schema_add_dimension(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_dimension_t* dim) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR ||
      sanity_check(ctx, dim) == TILEDB_ERR)
    return TILEDB_ERR;
  array_schema->array_schema_->add_dimension(dim->dim_);
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
      static_cast<tiledb::Layout>(cell_order));
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
      static_cast<tiledb::Layout>(tile_order));
  return TILEDB_OK;
}

int tiledb_array_schema_set_array_type(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t* array_schema,
    tiledb_array_type_t array_type) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  array_schema->array_schema_->set_array_type(
      static_cast<tiledb::ArrayType>(array_type));
  return TILEDB_OK;
}

int tiledb_array_schema_check(
    tiledb_ctx_t* ctx, tiledb_array_schema_t* array_schema) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  if (save_error(ctx, array_schema->array_schema_->check()))
    return TILEDB_ERR;
  else
    return TILEDB_OK;
}

int tiledb_array_schema_load(
    tiledb_ctx_t* ctx,
    tiledb_array_schema_t** array_schema,
    const char* array_name) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;
  // Create array schema
  *array_schema = (tiledb_array_schema_t*)malloc(sizeof(tiledb_array_schema_t));
  if (array_schema == nullptr) {
    save_error(
        ctx,
        tiledb::Status::Error("Failed to allocate TileDB array schema struct"));
    return TILEDB_OOM;
  }
  // Create ArraySchema object
  (*array_schema)->array_schema_ = new tiledb::ArraySchema();
  if ((*array_schema)->array_schema_ == nullptr) {
    free(*array_schema);
    *array_schema = nullptr;
    save_error(
        ctx,
        tiledb::Status::Error(
            "Failed to allocate TileDB array schema object in struct"));
    return TILEDB_OOM;
  }
  // Load array schema
  if (save_error(ctx, (*array_schema)->array_schema_->load(array_name))) {
    delete (*array_schema)->array_schema_;
    free(*array_schema);
    *array_schema = nullptr;
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

int tiledb_array_schema_get_array_name(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    const char** array_name) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  *array_name = array_schema->array_schema_->array_name().c_str();
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

int tiledb_array_schema_dump(
    tiledb_ctx_t* ctx, const tiledb_array_schema_t* array_schema, FILE* out) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  array_schema->array_schema_->dump(out);
  return TILEDB_OK;
}

/* ****************************** */
/*       ATTRIBUTE ITERATOR       */
/* ****************************** */

int tiledb_attribute_iter_create(
    tiledb_ctx_t* ctx,
    const void* schema,
    tiledb_attribute_iter_t** attr_it,
    tiledb_object_type_t object_type) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  // Check object type
  if (object_type != TILEDB_METADATA && object_type != TILEDB_ARRAY) {
    save_error(ctx, tiledb::Status::Error("Invalid object type"));
    return TILEDB_ERR;
  }

  // Get array or metadata schema
  const tiledb_array_schema_t* array_schema = nullptr;
  const tiledb_metadata_schema_t* metadata_schema = nullptr;
  if (object_type == TILEDB_ARRAY) {
    array_schema = static_cast<const tiledb_array_schema_t*>(schema);
    if (sanity_check(ctx, array_schema) == TILEDB_ERR)
      return TILEDB_ERR;
  } else {  // TILEDB_METADATA
    metadata_schema = static_cast<const tiledb_metadata_schema_t*>(schema);
    if (sanity_check(ctx, metadata_schema) == TILEDB_ERR)
      return TILEDB_ERR;
  }

  // Create attribute iterator struct
  *attr_it = (tiledb_attribute_iter_t*)malloc(sizeof(tiledb_attribute_iter_t));
  if (*attr_it == nullptr) {
    save_error(
        ctx,
        tiledb::Status::Error(
            "Failed to allocate TileDB attribute iterator struct"));
    return TILEDB_OOM;
  }

  // Initialize the iterator for the array schema
  (*attr_it)->array_schema_ = array_schema;
  if (array_schema != nullptr)
    (*attr_it)->attr_num_ = array_schema->array_schema_->attr_num();

  // Initialize the iterator for the metadata schema
  (*attr_it)->metadata_schema_ = metadata_schema;
  if (metadata_schema != nullptr)
    (*attr_it)->attr_num_ = metadata_schema->metadata_schema_->attr_num();

  // Initialize the rest members of the iterator
  (*attr_it)->current_attr_ = 0;
  if ((*attr_it)->attr_num_ <= 0) {
    (*attr_it)->attr_ = nullptr;
  } else {
    // Create an attribute struct inside the iterator struct
    (*attr_it)->attr_ = (tiledb_attribute_t*)malloc(sizeof(tiledb_attribute_t));
    if ((*attr_it)->attr_ == nullptr) {
      save_error(
          ctx,
          tiledb::Status::Error("Failed to allocate TileDB attribute struct "
                                "in iterator struct"));
      free(*attr_it);
      *attr_it = nullptr;
      return TILEDB_OOM;
    }

    // Create an attribute object
    if (array_schema != nullptr)
      (*attr_it)->attr_->attr_ =
          new tiledb::Attribute(array_schema->array_schema_->attr(0));
    if (metadata_schema != nullptr)
      (*attr_it)->attr_->attr_ =
          new tiledb::Attribute(metadata_schema->metadata_schema_->attr(0));

    // Check for allocation error
    if ((*attr_it)->attr_->attr_ == nullptr) {
      free((*attr_it)->attr_);
      free(*attr_it);
      *attr_it = nullptr;
      save_error(
          ctx,
          tiledb::Status::Error("Failed to allocate TileDB attribute object "
                                "in iterator struct"));
      return TILEDB_OOM;
    }
  }

  // Success
  return TILEDB_OK;
}

void tiledb_attribute_iter_free(tiledb_attribute_iter_t* attr_it) {
  if (attr_it == nullptr)
    return;

  if (attr_it->attr_ != nullptr) {
    if (attr_it->attr_->attr_ != nullptr)
      delete (attr_it->attr_->attr_);

    free(attr_it->attr_);
  }

  free(attr_it);
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
    if (attr_it->attr_->attr_ != nullptr)
      delete attr_it->attr_->attr_;
    if (attr_it->current_attr_ >= 0 &&
        attr_it->current_attr_ < attr_it->attr_num_) {
      if (attr_it->array_schema_ != nullptr)
        attr_it->attr_->attr_ =
            new tiledb::Attribute(attr_it->array_schema_->array_schema_->attr(
                attr_it->current_attr_));
      if (attr_it->metadata_schema_ != nullptr)
        attr_it->attr_->attr_ = new tiledb::Attribute(
            attr_it->metadata_schema_->metadata_schema_->attr(
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
    if (attr_it->attr_->attr_ != nullptr)
      delete attr_it->attr_->attr_;
    if (attr_it->attr_num_ > 0) {
      if (attr_it->array_schema_ != nullptr)
        attr_it->attr_->attr_ = new tiledb::Attribute(
            attr_it->array_schema_->array_schema_->attr(0));
      if (attr_it->metadata_schema_ != nullptr)
        attr_it->attr_->attr_ = new tiledb::Attribute(
            attr_it->metadata_schema_->metadata_schema_->attr(0));
    } else {
      attr_it->attr_->attr_ = nullptr;
    }
  }
  return TILEDB_OK;
}

/* ****************************** */
/*       DIMENSION ITERATOR       */
/* ****************************** */

int tiledb_dimension_iter_create(
    tiledb_ctx_t* ctx,
    const tiledb_array_schema_t* array_schema,
    tiledb_dimension_iter_t** dim_it) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  // Create attribute iterator struct
  *dim_it = (tiledb_dimension_iter_t*)malloc(sizeof(tiledb_dimension_iter_t));
  if (*dim_it == nullptr) {
    save_error(
        ctx,
        tiledb::Status::Error(
            "Failed to allocate TileDB dimension iterator struct"));
    return TILEDB_OOM;
  }

  // Initialize the iterator
  (*dim_it)->array_schema_ = array_schema;
  if (array_schema != nullptr)
    (*dim_it)->dim_num_ = array_schema->array_schema_->dim_num();
  (*dim_it)->current_dim_ = 0;
  if ((*dim_it)->dim_num_ <= 0) {
    (*dim_it)->dim_ = nullptr;
  } else {
    // Create a dimension struct inside the iterator struct
    (*dim_it)->dim_ = (tiledb_dimension_t*)malloc(sizeof(tiledb_dimension_t));
    if ((*dim_it)->dim_ == nullptr) {
      save_error(
          ctx,
          tiledb::Status::Error("Failed to allocate TileDB dimension struct "
                                "in iterator struct"));
      free(*dim_it);
      *dim_it = nullptr;
      return TILEDB_OOM;
    }

    // Create a dimension object
    (*dim_it)->dim_->dim_ =
        new tiledb::Dimension(array_schema->array_schema_->dim(0));

    // Check for allocation error
    if ((*dim_it)->dim_->dim_ == nullptr) {
      free((*dim_it)->dim_);
      free(*dim_it);
      *dim_it = nullptr;
      save_error(
          ctx,
          tiledb::Status::Error("Failed to allocate TileDB dimension object "
                                "in iterator struct"));
      return TILEDB_OOM;
    }
  }

  // Success
  return TILEDB_OK;
}

void tiledb_dimension_iter_free(tiledb_dimension_iter_t* dim_it) {
  if (dim_it == nullptr)
    return;

  if (dim_it->dim_ != nullptr) {
    if (dim_it->dim_->dim_ != nullptr)
      delete (dim_it->dim_->dim_);

    free(dim_it->dim_);
  }

  free(dim_it);
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
    if (dim_it->dim_->dim_ != nullptr)
      delete dim_it->dim_->dim_;

    if (dim_it->current_dim_ >= 0 && dim_it->current_dim_ < dim_it->dim_num_)
      dim_it->dim_->dim_ = new tiledb::Dimension(
          dim_it->array_schema_->array_schema_->dim(dim_it->current_dim_));
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
    if (dim_it->dim_->dim_ != nullptr)
      delete dim_it->dim_->dim_;
    if (dim_it->dim_num_ > 0)
      dim_it->dim_->dim_ =
          new tiledb::Dimension(dim_it->array_schema_->array_schema_->dim(0));
    else
      dim_it->dim_->dim_ = nullptr;
  }
  return TILEDB_OK;
}

/* ****************************** */
/*              QUERY             */
/* ****************************** */

int tiledb_query_create(
    tiledb_ctx_t* ctx,
    tiledb_query_t** query,
    void* object,
    tiledb_object_type_t object_type,
    tiledb_query_type_t* query_type) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  // Get object and sanity check
  tiledb::Array* array = (object_type == TILEDB_ARRAY) ?
                             static_cast<tiledb_array_t*>(object)->array_ :
                             nullptr;
  tiledb::Metadata* metadata =
      (object_type == TILEDB_METADATA) ?
          static_cast<tiledb_metadata_t*>(object)->metadata_ :
          nullptr;
  if (array == nullptr && metadata == nullptr) {
    save_error(
        ctx,
        tiledb::Status::Error("Failed to create TileDB query; Invalid input "
                              "object (it should be either array or "
                              "metadata)"));
    return TILEDB_ERR;
  }

  // Create query struct
  *query = (tiledb_query_t*)malloc(sizeof(tiledb_query_t));
  if (*query == nullptr) {
    save_error(
        ctx, tiledb::Status::Error("Failed to allocate TileDB query struct"));
    return TILEDB_OOM;
  }

  // Create a new Query object
  if (object_type == TILEDB_ARRAY)
    (*query)->query_ = new tiledb::Query(array);
  else if (object_type == TILEDB_METADATA)
    (*query)->query_ = new tiledb::Query(metadata);
  if ((*query)->query_ == nullptr) {
    free(*query);
    *query = nullptr;
    save_error(
        ctx,
        tiledb::Status::Error(
            "Failed to allocate TileDB query object in struct"));
    return TILEDB_OOM;
  }

  // Success
  return TILEDB_OK;
}

void tiledb_query_free(tiledb_query_t* query) {
  // Clean up
  if (query == nullptr)
    return;
  if (query->query_ != nullptr)
    delete query->query_;
  free(query);
}

int tiledb_query_set_async(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    void* (*callback)(void*),
    void* data) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  // Set async
  query->query_->set_async(callback, data);

  // Success
  return TILEDB_OK;
}

int tiledb_query_set_type(
    tiledb_ctx_t* ctx, tiledb_query_t* query, tiledb::QueryType query_type) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  query->query_->set_query_type(query_type);

  // Success
  return TILEDB_OK;
}

int tiledb_query_set_subarray(
    tiledb_ctx_t* ctx, tiledb_query_t* query, const void* subarray) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  // Set async
  if (save_error(ctx, query->query_->set_subarray(subarray)))
    return TILEDB_ERR;

  // Success
  return TILEDB_OK;
}

int tiledb_query_set_attribute(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* attr,
    void* buffer,
    uint64_t buffer_size) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  // Set attribute buffer
  if (save_error(
          ctx, query->query_->set_attribute_buffer(attr, buffer, buffer_size)))
    return TILEDB_ERR;

  // Success
  return TILEDB_OK;
}

int tiledb_query_set_var_attribute(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* attr,
    void* buffer,
    uint64_t buffer_size,
    void* buffer_var,
    uint64_t buffer_var_size) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  // Set attribute buffer
  if (save_error(
          ctx,
          query->query_->set_attribute_buffer(
              attr, buffer, buffer_size, buffer_var, buffer_var_size)))
    return TILEDB_ERR;

  // Success
  return TILEDB_OK;
}

int tiledb_query_set_dimension(
    tiledb_ctx_t* ctx,
    tiledb_query_t* query,
    const char* dim,
    void* buffer,
    uint64_t buffer_size) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  // Set dimension buffer
  if (save_error(
          ctx, query->query_->set_dimension_buffer(dim, buffer, buffer_size)))
    return TILEDB_ERR;

  // Success
  return TILEDB_OK;
}

int tiledb_query_get_status(
    tiledb_ctx_t* ctx, tiledb_query_t* query, tiledb_query_status_t* status) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  *status = static_cast<tiledb_query_status_t>(query->query_->status());

  // Success
  return TILEDB_OK;
}

int tiledb_query_get_overflow(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    const char* name,
    bool* overflow) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  if (save_error(ctx, query->query_->overflow(name, overflow)))
    return TILEDB_ERR;

  // Success
  return TILEDB_OK;
}

int tiledb_query_check(tiledb_ctx_t* ctx, tiledb_query_t* query) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  // Check query correcteness
  if (save_error(ctx, query->query_->check()))
    return TILEDB_ERR;

  // Success
  return TILEDB_OK;
}

int tiledb_query_process(tiledb_ctx_t* ctx, tiledb_query_t* query) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  // Check query correcteness
  if (save_error(ctx, ctx->storage_manager_->query_process(query->query_)))
    return TILEDB_ERR;

  // Success
  return TILEDB_OK;
}

int tiledb_query_sync(tiledb_ctx_t* ctx, tiledb_query_t* query) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, query) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create the array
  if (save_error(ctx, ctx->storage_manager_->query_sync(query->query_)))
    return TILEDB_ERR;

  // Success
  return TILEDB_OK;
}

/* ****************************** */
/*              ARRAY             */
/* ****************************** */

int tiledb_array_create(
    tiledb_ctx_t* ctx, const tiledb_array_schema_t* array_schema) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create the array
  if (save_error(
          ctx,
          ctx->storage_manager_->array_create(array_schema->array_schema_)))
    return TILEDB_ERR;

  // Success
  return TILEDB_OK;
}

int tiledb_array_open(
    tiledb_ctx_t* ctx, tiledb_array_t** array, const char* array_name) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create array struct
  *array = (tiledb_array_t*)malloc(sizeof(tiledb_array_t));
  if (*array == nullptr) {
    save_error(
        ctx, tiledb::Status::Error("Failed to allocate TileDB array struct"));
    return TILEDB_OOM;
  }

  // Open array
  if (save_error(
          ctx,
          ctx->storage_manager_->array_open(array_name, &((*array)->array_)))) {
    free(*array);
    *array = nullptr;
    return TILEDB_ERR;
  }

  // Success
  return TILEDB_OK;
}

int tiledb_array_close(tiledb_ctx_t* ctx, tiledb_array_t* array) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR || sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create the array
  bool err = save_error(ctx, ctx->storage_manager_->array_close(array->array_));

  // Clean up
  delete array->array_;
  free(array);

  // Return
  if (err)
    return TILEDB_ERR;
  else
    return TILEDB_OK;
}

int tiledb_array_consolidate(tiledb_ctx_t* ctx, const char* array) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create the array
  if (save_error(ctx, ctx->storage_manager_->array_consolidate(array)))
    return TILEDB_ERR;

  // Success
  return TILEDB_OK;
}

/* TODO:


int tiledb_array_init(
    tiledb_ctx_t* ctx,
    tiledb_array_t** tiledb_array,
    const char* array,
    tiledb_array_mode_t mode,
    const void* subarray,
    const char** attributes,
    int attribute_num) {
  // TODO: sanity checks here

  // Allocate memory for the array struct
  *tiledb_array = (tiledb_array_t*)malloc(sizeof(struct tiledb_array_t));
  if (tiledb_array == nullptr)
    return TILEDB_OOM;

  // Set TileDB context
  (*tiledb_array)->ctx_ = ctx;

  // Init the array
  if (save_error(
          ctx,
          ctx->storage_manager_->array_init(
              (*tiledb_array)->array_,
              array,
              static_cast<tiledb::ArrayMode>(mode),
              subarray,
              attributes,
              attribute_num))) {
    free(*tiledb_array);
    return TILEDB_ERR;
  };

  return TILEDB_OK;
}

tiledb_array_schema_t* tiledb_array_get_schema(
    const tiledb_array_t* tiledb_array) {
  // Sanity check
  // TODO: sanity checks here

  // Create an array schema struct object
  tiledb_array_schema_t* array_schema =
      (tiledb_array_schema_t*)malloc(sizeof(tiledb_array_schema_t));
  if (array_schema == nullptr)
    return nullptr;
  array_schema->array_schema_ =
      new tiledb::ArraySchema(tiledb_array->array_->array_schema());

  return array_schema;
}

int tiledb_array_write(
    const tiledb_array_t* tiledb_array,
    const void** buffers,
    const size_t* buffer_sizes) {
  // TODO: sanity checks here

  tiledb_ctx_t* ctx = tiledb_array->ctx_;

  if (save_error(ctx, tiledb_array->array_->write(buffers, buffer_sizes)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_array_read(
    const tiledb_array_t* tiledb_array, void** buffers, size_t* buffer_sizes) {
  // TODO: sanity checks here

  tiledb_ctx_t* ctx = tiledb_array->ctx_;

  if (save_error(ctx, tiledb_array->array_->read(buffers, buffer_sizes)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_array_overflow(
    const tiledb_array_t* tiledb_array, int attribute_id) {
  // TODO: sanity checks here

  tiledb_ctx_t* ctx = tiledb_array->ctx_;

  return (int)tiledb_array->array_->overflow(attribute_id);
}

int tiledb_array_consolidate(tiledb_ctx_t* ctx, const char* array) {
  // TODO: sanity checks here

  if (save_error(ctx, ctx->storage_manager_->array_consolidate(array)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_array_finalize(tiledb_array_t* tiledb_array) {
  // TODO: sanity checks here

  tiledb_ctx_t* ctx = tiledb_array->ctx_;

  if (save_error(
          ctx,
          tiledb_array->ctx_->storage_manager_->array_finalize(
              tiledb_array->array_))) {
    free(tiledb_array);
    return TILEDB_ERR;
  }

  free(tiledb_array);

  return TILEDB_OK;
}

int tiledb_array_sync(tiledb_array_t* tiledb_array) {
  // TODO: sanity checks here

  tiledb_ctx_t* ctx = tiledb_array->ctx_;

  if (save_error(
          ctx,
          tiledb_array->ctx_->storage_manager_->array_sync(
              tiledb_array->array_)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_array_sync_attribute(
    tiledb_array_t* tiledb_array, const char* attribute) {
  // TODO: sanity checks here

  tiledb_ctx_t* ctx = tiledb_array->ctx_;

  if (save_error(
          ctx,
          tiledb_array->ctx_->storage_manager_->array_sync_attribute(
              tiledb_array->array_, attribute)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

typedef struct tiledb_array_iterator_t {
  tiledb::ArrayIterator* array_it_;
  tiledb_ctx_t* ctx_;
} tiledb_array_iterator_t;

int tiledb_array_iterator_init(
    tiledb_ctx_t* ctx,
    tiledb_array_iterator_t** tiledb_array_it,
    const char* array,
    tiledb_array_mode_t mode,
    const void* subarray,
    const char** attributes,
    int attribute_num,
    void** buffers,
    size_t* buffer_sizes) {
  // TODO: sanity checks here

  *tiledb_array_it =
      (tiledb_array_iterator_t*)malloc(sizeof(struct tiledb_array_iterator_t));
  if (*tiledb_array_it == nullptr)
    return TILEDB_OOM;

  (*tiledb_array_it)->ctx_ = ctx;

  if (save_error(
          ctx,
          ctx->storage_manager_->array_iterator_init(
              (*tiledb_array_it)->array_it_,
              array,
              static_cast<tiledb::ArrayMode>(mode),
              subarray,
              attributes,
              attribute_num,
              buffers,
              buffer_sizes))) {
    free(*tiledb_array_it);
    return TILEDB_ERR;
  }

  return TILEDB_OK;
}

int tiledb_array_iterator_get_value(
    tiledb_array_iterator_t* tiledb_array_it,
    int attribute_id,
    const void** value,
    size_t* value_size) {
  tiledb_ctx_t* ctx = tiledb_array_it->ctx_;

  // TODO: sanity checks

  // Get value
  if (save_error(
          ctx,
          tiledb_array_it->array_it_->get_value(
              attribute_id, value, value_size)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_array_iterator_next(tiledb_array_iterator_t* tiledb_array_it) {
  // TODO: sanity checks

  tiledb_ctx_t* ctx = tiledb_array_it->ctx_;

  if (save_error(ctx, tiledb_array_it->array_it_->next()))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_array_iterator_end(tiledb_array_iterator_t* tiledb_array_it) {
  // TODO: sanity checks

  tiledb_ctx_t* ctx = tiledb_array_it->ctx_;

  return (int)tiledb_array_it->array_it_->end();
}

int tiledb_array_iterator_finalize(tiledb_array_iterator_t* tiledb_array_it) {
  // TODO: sanity checks

  tiledb_ctx_t* ctx = tiledb_array_it->ctx_;

  if (save_error(
          ctx,
          tiledb_array_it->ctx_->storage_manager_->array_iterator_finalize(
              tiledb_array_it->array_it_))) {
    free(tiledb_array_it);
    return TILEDB_ERR;
  }

  free(tiledb_array_it);

  return TILEDB_OK;
}
 */

/* ****************************** */
/*        METADATA SCHEMA         */
/* ****************************** */

int tiledb_metadata_schema_create(
    tiledb_ctx_t* ctx,
    tiledb_metadata_schema_t** metadata_schema,
    const char* metadata_name) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create metadata schema struct
  *metadata_schema =
      (tiledb_metadata_schema_t*)malloc(sizeof(tiledb_metadata_schema_t));
  if (*metadata_schema == nullptr) {
    save_error(
        ctx,
        tiledb::Status::Error(
            "Failed to allocate TileDB metadata schema struct"));
    return TILEDB_OOM;
  }

  // Create a new MetadataSchema object
  (*metadata_schema)->metadata_schema_ =
      new tiledb::MetadataSchema(metadata_name);

  if ((*metadata_schema)->metadata_schema_ == nullptr) {
    free(*metadata_schema);
    *metadata_schema = nullptr;
    save_error(
        ctx,
        tiledb::Status::Error(
            "Failed to allocate TileDB metadata schema object in struct"));
    return TILEDB_OOM;
  }

  // Success
  return TILEDB_OK;
}

void tiledb_metadata_schema_free(tiledb_metadata_schema_t* metadata_schema) {
  if (metadata_schema == nullptr)
    return;

  if (metadata_schema->metadata_schema_ != nullptr)
    delete metadata_schema->metadata_schema_;

  free(metadata_schema);
}

int tiledb_metadata_schema_add_attribute(
    tiledb_ctx_t* ctx,
    tiledb_metadata_schema_t* metadata_schema,
    tiledb_attribute_t* attr) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, metadata_schema) == TILEDB_ERR ||
      sanity_check(ctx, attr) == TILEDB_ERR)
    return TILEDB_ERR;

  metadata_schema->metadata_schema_->add_attribute(attr->attr_);

  return TILEDB_OK;
}

int tiledb_metadata_schema_set_capacity(
    tiledb_ctx_t* ctx,
    tiledb_metadata_schema_t* metadata_schema,
    uint64_t capacity) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, metadata_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  metadata_schema->metadata_schema_->set_capacity(capacity);
  return TILEDB_OK;
}

int tiledb_metadata_schema_set_cell_order(
    tiledb_ctx_t* ctx,
    tiledb_metadata_schema_t* metadata_schema,
    tiledb_layout_t cell_order) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, metadata_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  metadata_schema->metadata_schema_->set_cell_order(
      static_cast<tiledb::Layout>(cell_order));
  return TILEDB_OK;
}

int tiledb_metadata_schema_set_tile_order(
    tiledb_ctx_t* ctx,
    tiledb_metadata_schema_t* metadata_schema,
    tiledb_layout_t tile_order) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, metadata_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  metadata_schema->metadata_schema_->set_tile_order(
      static_cast<tiledb::Layout>(tile_order));
  return TILEDB_OK;
}

int tiledb_metadata_schema_check(
    tiledb_ctx_t* ctx, tiledb_metadata_schema_t* metadata_schema) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, metadata_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  if (save_error(ctx, metadata_schema->metadata_schema_->check()))
    return TILEDB_ERR;
  else
    return TILEDB_OK;
}

int tiledb_metadata_schema_load(
    tiledb_ctx_t* ctx,
    tiledb_metadata_schema_t** metadata_schema,
    const char* metadata_name) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create metadata schema
  *metadata_schema =
      (tiledb_metadata_schema_t*)malloc(sizeof(tiledb_metadata_schema_t));
  if (metadata_schema == nullptr) {
    save_error(
        ctx,
        tiledb::Status::Error(
            "Failed to allocate TileDB metadata schema struct"));
    return TILEDB_OOM;
  }

  // Create MetadataSchema object
  (*metadata_schema)->metadata_schema_ = new tiledb::MetadataSchema();
  if ((*metadata_schema)->metadata_schema_ == nullptr) {
    free(*metadata_schema);
    *metadata_schema = nullptr;
    save_error(
        ctx,
        tiledb::Status::Error(
            "Failed to allocate TileDB metadata schema object in struct"));
    return TILEDB_OOM;
  }

  // Load metadata schema
  if (save_error(
          ctx, (*metadata_schema)->metadata_schema_->load(metadata_name))) {
    delete (*metadata_schema)->metadata_schema_;
    free(*metadata_schema);
    *metadata_schema = nullptr;
    return TILEDB_ERR;
  }

  // Success
  return TILEDB_OK;
}

int tiledb_metadata_schema_get_metadata_name(
    tiledb_ctx_t* ctx,
    const tiledb_metadata_schema_t* metadata_schema,
    const char** metadata_name) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, metadata_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  *metadata_name = metadata_schema->metadata_schema_->metadata_name().c_str();
  return TILEDB_OK;
}

int tiledb_metadata_schema_get_capacity(
    tiledb_ctx_t* ctx,
    const tiledb_metadata_schema_t* metadata_schema,
    uint64_t* capacity) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, metadata_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  *capacity = metadata_schema->metadata_schema_->capacity();
  return TILEDB_OK;
}

int tiledb_metadata_schema_get_cell_order(
    tiledb_ctx_t* ctx,
    const tiledb_metadata_schema_t* metadata_schema,
    tiledb_layout_t* cell_order) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, metadata_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  *cell_order = static_cast<tiledb_layout_t>(
      metadata_schema->metadata_schema_->cell_order());
  return TILEDB_OK;
}

int tiledb_metadata_schema_get_tile_order(
    tiledb_ctx_t* ctx,
    const tiledb_metadata_schema_t* metadata_schema,
    tiledb_layout_t* tile_order) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, metadata_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  *tile_order = static_cast<tiledb_layout_t>(
      metadata_schema->metadata_schema_->tile_order());
  return TILEDB_OK;
}

int tiledb_metadata_schema_dump(
    tiledb_ctx_t* ctx,
    const tiledb_metadata_schema_t* metadata_schema,
    FILE* out) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, metadata_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  metadata_schema->metadata_schema_->dump(out);
  return TILEDB_OK;
}

/* ****************************** */
/*            METADATA            */
/* ****************************** */

/* TODO

int tiledb_metadata_create(
    tiledb_ctx_t* ctx, const tiledb_metadata_schema_t* metadata_schema) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, metadata_schema) == TILEDB_ERR)
    return TILEDB_ERR;
  if (save_error(
          ctx,
          ctx->storage_manager_->metadata_create(
              metadata_schema->metadata_schema_)))
    return TILEDB_ERR;
  return TILEDB_OK;
}

int tiledb_metadata_init(
    tiledb_ctx_t* ctx,
    tiledb_metadata_t** tiledb_metadata,
    const char* metadata,
    tiledb_metadata_mode_t mode,
    const char** attributes,
    int attribute_num) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  // Allocate memory for the array struct
  *tiledb_metadata =
      (tiledb_metadata_t*)malloc(sizeof(struct tiledb_metadata_t));
  if (*tiledb_metadata == nullptr)
    return TILEDB_OOM;

  // Set TileDB context
  (*tiledb_metadata)->ctx_ = ctx;

  // Init the metadata
  if (save_error(
          ctx,
          ctx->storage_manager_->metadata_init(
              (*tiledb_metadata)->metadata_,
              metadata,
              mode,
              attributes,
              attribute_num))) {
    free(*tiledb_metadata);
    return TILEDB_ERR;
  }

  return TILEDB_OK;
}

int tiledb_metadata_reset_attributes(
    const tiledb_metadata_t* tiledb_metadata,
    const char** attributes,
    int attribute_num) {
  // TODO: sanity checks here

  tiledb_ctx_t* ctx = tiledb_metadata->ctx_;

  // Reset attributes
  if (save_error(
          ctx,
          tiledb_metadata->metadata_->reset_attributes(
              attributes, attribute_num)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

tiledb_metadata_schema_t* tiledb_metadata_get_schema(
    const tiledb_metadata_t* tiledb_metadata) {
  // Sanity check
  // TODO

  // Create an array schema struct object
  tiledb_metadata_schema_t* metadata_schema =
      (tiledb_metadata_schema_t*)malloc(sizeof(tiledb_metadata_schema_t));
  if (metadata_schema == nullptr)
    return nullptr;
  metadata_schema->metadata_schema_ =
      new tiledb::MetadataSchema(tiledb_metadata->metadata_->metadata_schema());

  return metadata_schema;
}

int tiledb_metadata_write(
    const tiledb_metadata_t* tiledb_metadata,
    const char* keys,
    size_t keys_size,
    const void** buffers,
    const size_t* buffer_sizes) {
  // TODO: sanity checks here

  tiledb_ctx_t* ctx = tiledb_metadata->ctx_;

  if (save_error(
          ctx,
          tiledb_metadata->metadata_->write(
              keys, keys_size, buffers, buffer_sizes)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_metadata_read(
    const tiledb_metadata_t* tiledb_metadata,
    const char* key,
    void** buffers,
    size_t* buffer_sizes) {
  // TODO: sanity checks here

  tiledb_ctx_t* ctx = tiledb_metadata->ctx_;

  if (save_error(
          ctx, tiledb_metadata->metadata_->read(key, buffers, buffer_sizes)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_metadata_overflow(
    const tiledb_metadata_t* tiledb_metadata, int attribute_id) {
  // TODO: sanity checks here

  return (int)tiledb_metadata->metadata_->overflow(attribute_id);
}

int tiledb_metadata_consolidate(tiledb_ctx_t* ctx, const char* metadata) {
  // TODO: sanity checks here

  if (save_error(ctx, ctx->storage_manager_->metadata_consolidate(metadata)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_metadata_finalize(tiledb_metadata_t* tiledb_metadata) {
  // TODO: sanity checks here

  tiledb_ctx_t* ctx = tiledb_metadata->ctx_;

  if (save_error(
          ctx,
          tiledb_metadata->ctx_->storage_manager_->metadata_finalize(
              tiledb_metadata->metadata_))) {
    free(tiledb_metadata);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

typedef struct tiledb_metadata_iterator_t {
  tiledb::MetadataIterator* metadata_it_;
  tiledb_ctx_t* ctx_;
} tiledb_metadata_iterator_t;

int tiledb_metadata_iterator_init(
    tiledb_ctx_t* ctx,
    tiledb_metadata_iterator_t** tiledb_metadata_it,
    const char* metadata,
    const char** attributes,
    int attribute_num,
    void** buffers,
    size_t* buffer_sizes) {
  // TODO: sanity checks here

  // Allocate memory for the metadata struct
  *tiledb_metadata_it = (tiledb_metadata_iterator_t*)malloc(
      sizeof(struct tiledb_metadata_iterator_t));
  if (*tiledb_metadata_it == nullptr)
    return TILEDB_ERR;

  // Set TileDB context
  (*tiledb_metadata_it)->ctx_ = ctx;

  // Initialize the metadata iterator
  if (save_error(
          ctx,
          ctx->storage_manager_->metadata_iterator_init(
              (*tiledb_metadata_it)->metadata_it_,
              metadata,
              attributes,
              attribute_num,
              buffers,
              buffer_sizes))) {
    free(*tiledb_metadata_it);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

int tiledb_metadata_iterator_get_value(
    tiledb_metadata_iterator_t* tiledb_metadata_it,
    int attribute_id,
    const void** value,
    size_t* value_size) {
  // TODO: sanity checks here

  tiledb_ctx_t* ctx = tiledb_metadata_it->ctx_;

  if (save_error(
          ctx,
          tiledb_metadata_it->metadata_it_->get_value(
              attribute_id, value, value_size)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_metadata_iterator_next(
    tiledb_metadata_iterator_t* tiledb_metadata_it) {
  // TODO: sanity checks here

  tiledb_ctx_t* ctx = tiledb_metadata_it->ctx_;

  // Advance metadata iterator
  if (save_error(ctx, tiledb_metadata_it->metadata_it_->next()))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_metadata_iterator_end(
    tiledb_metadata_iterator_t* tiledb_metadata_it) {
  // TODO: sanity checks here

  // Check if the metadata iterator reached its end
  return (int)tiledb_metadata_it->metadata_it_->end();
}

int tiledb_metadata_iterator_finalize(
    tiledb_metadata_iterator_t* tiledb_metadata_it) {
  // TODO: sanity checks here

  tiledb_ctx_t* ctx = tiledb_metadata_it->ctx_;

  // Finalize metadata iterator
  if (save_error(
          ctx,
          tiledb_metadata_it->ctx_->storage_manager_
              ->metadata_iterator_finalize(tiledb_metadata_it->metadata_it_))) {
    free(tiledb_metadata_it);
    return TILEDB_ERR;
  }

  free(tiledb_metadata_it);

  return TILEDB_OK;
}
*/

/* ****************************** */
/*       DIRECTORY MANAGEMENT     */
/* ****************************** */

/* TODO

int tiledb_dir_type(tiledb_ctx_t* ctx, const char* dir) {
  if (ctx == nullptr)
    return TILEDB_ERR;
  return ctx->storage_manager_->dir_type(dir);
}

int tiledb_clear(tiledb_ctx_t* ctx, const char* dir) {
  // TODO: sanity checks here

  // TODO: do this everywhere
  if (dir == nullptr) {
    save_error(
        ctx, tiledb::Status::Error("Invalid directory argument is NULL"));
    return TILEDB_ERR;
  }

  if (save_error(ctx, ctx->storage_manager_->clear(dir)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_delete(tiledb_ctx_t* ctx, const char* dir) {
  // TODO: sanity checks here

  if (save_error(ctx, ctx->storage_manager_->delete_entire(dir)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_move(tiledb_ctx_t* ctx, const char* old_dir, const char* new_dir) {
  // TODO: sanity checks here

  if (save_error(ctx, ctx->storage_manager_->move(old_dir, new_dir)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_ls(
    tiledb_ctx_t* ctx,
    const char* parent_dir,
    char** dirs,
    tiledb_object_t* dir_types,
    int* dir_num) {
  // TODO: sanity checks here

  if (save_error(
          ctx, ctx->storage_manager_->ls(parent_dir, dirs, dir_types, dir_num)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_ls_c(tiledb_ctx_t* ctx, const char* parent_dir, int* dir_num) {
  // TODO: sanity checks here

  if (save_error(ctx, ctx->storage_manager_->ls_c(parent_dir, dir_num)))
    return TILEDB_ERR;

  return TILEDB_OK;
}
 */

/* ****************************** */
/*     ASYNCHRONOUS I/O (AIO)     */
/* ****************************** */

/* TODO: remove
int tiledb_aio_request_create(
    tiledb_ctx_t* ctx, tiledb_aio_request_t** aio_request) {
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create an AIO struct
  *aio_request = (tiledb_aio_request_t*)malloc(sizeof(tiledb_aio_request_t));
  if (*aio_request == nullptr) {
    save_error(
        ctx, tiledb::Status::Error("Failed to allocate TileDB AIO struct"));
    return TILEDB_OOM;
  }

  // Create a new AIORequest object
  (*aio_request)->aio_request_ = new tiledb::AIORequest();
  if ((*aio_request)->aio_request_ == nullptr) {
    free(*aio_request);
    *aio_request = nullptr;
    save_error(
        ctx,
        tiledb::Status::Error(
            "Failed to allocate TileDB AIORequest object in struct"));
    return TILEDB_OOM;
  }

  // Set status pointer
  (*aio_request)->aio_request_->set_status(&((*aio_request)->aio_status_));

  return TILEDB_OK;
}

void tiledb_aio_request_free(tiledb_aio_request_t* aio_request) {
  if (aio_request == nullptr)
    return;

  if (aio_request->aio_request_ != nullptr)
    delete aio_request->aio_request_;

  free(aio_request);
}

int tiledb_aio_request_set_array(
    tiledb_ctx_t* ctx,
    tiledb_aio_request_t* aio_request,
    tiledb_array_t* array) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, aio_request) == TILEDB_ERR ||
      sanity_check(ctx, array) == TILEDB_ERR)
    return TILEDB_ERR;

  aio_request->aio_request_->set_array(array->array_);
  aio_request->aio_request_->set_mode(array->array_->mode());

  return TILEDB_OK;
}

int tiledb_aio_request_set_buffers(
    tiledb_ctx_t* ctx,
    tiledb_aio_request_t* aio_request,
    void** buffers,
    size_t* buffer_sizes) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, aio_request) == TILEDB_ERR)
    return TILEDB_ERR;

  aio_request->aio_request_->set_buffers(buffers);
  aio_request->aio_request_->set_buffer_sizes(buffer_sizes);

  return TILEDB_OK;
}

int tiledb_aio_request_set_subarray(
    tiledb_ctx_t* ctx,
    tiledb_aio_request_t* aio_request,
    const void* subarray) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, aio_request) == TILEDB_ERR)
    return TILEDB_ERR;

  aio_request->aio_request_->set_subarray(subarray);

  return TILEDB_OK;
}

int tiledb_aio_request_set_callback(
    tiledb_ctx_t* ctx,
    tiledb_aio_request_t* aio_request,
    void* (*completion_handle)(void*),
    void* completion_data) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, aio_request) == TILEDB_ERR)
    return TILEDB_ERR;

  aio_request->aio_request_->set_callback(completion_handle, completion_data);

  return TILEDB_OK;
}

int tiledb_array_aio_submit(
    tiledb_ctx_t* ctx, tiledb_aio_request_t* aio_request) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, aio_request) == TILEDB_ERR)
    return TILEDB_ERR;

  // Submit the AIO read request
  if (save_error(
          ctx, ctx->storage_manager_->aio_submit(aio_request->aio_request_)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_aio_request_get_status(
    tiledb_ctx_t* ctx,
    tiledb_aio_request_t* aio_request,
    tiledb_aio_status_t* aio_status) {
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, aio_request) == TILEDB_ERR)
    return TILEDB_ERR;

  *aio_status = static_cast<tiledb_aio_status_t>(aio_request->aio_status_);

  return TILEDB_OK;
}

 */
