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
#include "array_schema.h"
#include "query.h"
#include "storage_manager.h"
#include "utils.h"

/* ****************************** */
/*            CONSTANTS           */
/* ****************************** */

const char* tiledb_coords() {
  return tiledb::constants::coords;
}

int tiledb_var_num() {
  return tiledb::constants::var_num;
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
  tiledb::StorageManager* storage_manager_;
  tiledb::Status* last_error_;
};

struct tiledb_error_t {
  // Pointer to a copy of the last TileDB error associated with a given ctx
  const tiledb::Status* status_;
  std::string* errmsg_;
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

/* ********************************* */
/*         AUXILIARY FUNCTIONS       */
/* ********************************* */

/* Saves a status inside the context object. */
static bool save_error(tiledb_ctx_t* ctx, const tiledb::Status& st) {
  // No error
  if (st.ok())
    return false;

  // Delete previous error
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
  if (attr_it == nullptr || attr_it->array_schema_ == nullptr) {
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

inline int sanity_check(tiledb_ctx_t* ctx, const tiledb_query_t* query) {
  if (query == nullptr || query->query_ == nullptr) {
    save_error(ctx, tiledb::Status::Error("Invalid TileDB query struct"));
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
  delete ctx->storage_manager_;
  delete ctx->last_error_;
  free(ctx);
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

  auto group_uri = tiledb::URI(group);

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
  (*array_schema)->array_schema_ =
      new tiledb::ArraySchema(tiledb::URI(array_name));
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
  if (*array_schema == nullptr) {
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
  const tiledb::URI& uri = array_schema->array_schema_->array_uri();
  *array_name = strdup(uri.to_string().c_str());
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
    const tiledb_array_schema_t* array_schema,
    tiledb_attribute_iter_t** attr_it) {
  if (sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;

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
  (*attr_it)->attr_num_ = array_schema->array_schema_->attr_num();

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
    (*attr_it)->attr_->attr_ =
        new tiledb::Attribute(array_schema->array_schema_->attr(0));

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
    delete attr_it->attr_->attr_;
    if (attr_it->current_attr_ >= 0 &&
        attr_it->current_attr_ < attr_it->attr_num_) {
      attr_it->attr_->attr_ = new tiledb::Attribute(
          attr_it->array_schema_->array_schema_->attr(attr_it->current_attr_));
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
      attr_it->attr_->attr_ =
          new tiledb::Attribute(attr_it->array_schema_->array_schema_->attr(0));
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
    const char* array_name,
    tiledb_query_mode_t mode,
    const void* subarray,
    const char** attributes,
    int attribute_num,
    void** buffers,
    uint64_t* buffer_sizes) {
  // Sanity check
  if (sanity_check(ctx) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create query struct
  *query = (tiledb_query_t*)malloc(sizeof(tiledb_query_t));
  if (*query == nullptr) {
    save_error(
        ctx, tiledb::Status::Error("Failed to allocate TileDB query struct"));
    return TILEDB_OOM;
  }

  // Create a new Query object
  (*query)->query_ = new tiledb::Query();
  if ((*query)->query_ == nullptr) {
    free(*query);
    *query = nullptr;
    save_error(
        ctx,
        tiledb::Status::Error(
            "Failed to allocate TileDB query object in struct"));
    return TILEDB_OOM;
  }

  // Create query object
  if (save_error(
          ctx,
          ctx->storage_manager_->query_init(
              ((*query)->query_),
              array_name,
              static_cast<tiledb::QueryMode>(mode),
              subarray,
              attributes,
              attribute_num,
              buffers,
              buffer_sizes))) {
    delete (*query)->query_;
    free(*query);
    *query = nullptr;
    return TILEDB_ERR;
  }

  // Success
  return TILEDB_OK;
}

void tiledb_query_free(tiledb_query_t* query) {
  if (query == nullptr)
    return;
  delete query->query_;  // TODO: this should return Status
  free(query);
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

int tiledb_query_get_status(
    tiledb_ctx_t* ctx, tiledb_query_t* query, tiledb_query_status_t* status) {
  // TODO

  return TILEDB_OK;
}

int tiledb_query_get_overflow(
    tiledb_ctx_t* ctx,
    const tiledb_query_t* query,
    const char* attribute_name,
    int* overflow) {
  // TODO

  return TILEDB_OK;
}

/* ****************************** */
/*              ARRAY             */
/* ****************************** */

int tiledb_array_create(
    tiledb_ctx_t* ctx, const tiledb_array_schema_t* array_schema) {
  // Sanity checks
  if (sanity_check(ctx) == TILEDB_ERR ||
      sanity_check(ctx, array_schema) == TILEDB_ERR)
    return TILEDB_ERR;

  // Create the array
  if (save_error(
          ctx,
          ctx->storage_manager_->array_create(array_schema->array_schema_)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

/*
int tiledb_array_consolidate(tiledb_ctx_t* ctx, const char* array_name) {
  // TODO: sanity checks here

  auto array_uri = tiledb::uri::URI(array_name);
  if (save_error(ctx, ctx->storage_manager_->array_consolidate(array_uri)))
    return TILEDB_ERR;

  return TILEDB_OK;
}
*/

/* ****************************** */
/*       DIRECTORY MANAGEMENT     */
/* ****************************** */

/*

int tiledb_dir_type(tiledb_ctx_t* ctx, const char* dir) {
  if (ctx == nullptr)
    return TILEDB_ERR;
  return ctx->storage_manager_->dir_type(dir);
}

int tiledb_clear(tiledb_ctx_t* ctx, const char* path) {
  // TODO: sanity checks here

  // TODO: do this everywhere
  if (path == nullptr) {
    save_error(
        ctx, tiledb::Status::Error("Invalid directory argument is NULL"));
    return TILEDB_ERR;
  }
  auto uri = tiledb::uri::URI(path);
  if (save_error(ctx, ctx->storage_manager_->clear(uri)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_delete(tiledb_ctx_t* ctx, const char* path) {
  // TODO: sanity checks here

  auto uri = tiledb::uri::URI(path);
  if (save_error(ctx, ctx->storage_manager_->delete_entire(uri)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_move(tiledb_ctx_t* ctx, const char* old_path, const char* new_path) {
  // TODO: sanity checks here

  auto old_uri = tiledb::uri::URI(old_path);
  auto new_uri = tiledb::uri::URI(new_path);
  if (save_error(ctx, ctx->storage_manager_->move(old_uri, new_uri)))
    return TILEDB_ERR;
  return TILEDB_OK;
}

int tiledb_ls(
    tiledb_ctx_t* ctx,
    const char* parent_path,
    char** dirs,
    tiledb_object_t* dir_types,
    int* dir_num) {
  // TODO: sanity checks here

  auto parent_uri = tiledb::uri::URI(parent_path);
  if (save_error(
          ctx, ctx->storage_manager_->ls(parent_uri, dirs, dir_types, dir_num)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_ls_c(tiledb_ctx_t* ctx, const char* parent_path, int* dir_num) {
  // TODO: sanity checks here

  auto parent_uri = tiledb::uri::URI(parent_path);
  if (save_error(ctx, ctx->storage_manager_->ls_c(parent_uri, dir_num)))
    return TILEDB_ERR;

  return TILEDB_OK;
}
 */
