/**
 * @file   tiledb.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2016 MIT, Intel Corporation and TileDB, Inc.
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
#include <cassert>
#include <cstring>
#include <iostream>
#include <stack>
#include "aio_request.h"
#include "array_schema.h"
#include "storage_manager.h"

/* ****************************** */
/*             MACROS             */
/* ****************************** */

#ifdef TILEDB_VERBOSE
#define PRINT_ERROR(x) std::cerr << TILEDB_ERRMSG << x << ".\n"
#else
#define PRINT_ERROR(x) \
  do {                 \
  } while (0)
#endif

/* ****************************** */
/*        GLOBAL VARIABLES        */
/* ****************************** */

// char tiledb_errmsg[TILEDB_ERRMSG_MAX_LEN];

/* ****************************** */
/*            TILEDB              */
/* ****************************** */

void tiledb_version(int* major, int* minor, int* rev) {
  *major = TILEDB_VERSION_MAJOR;
  *minor = TILEDB_VERSION_MINOR;
  *rev = TILEDB_VERSION_REVISION;
}

/* ****************************** */
/*            CONTEXT             */
/* ****************************** */

struct TileDB_CTX {
  // storage manager instance
  tiledb::StorageManager* storage_manager_;
  // last error assocated with this context
  tiledb::Status* last_error_;
};

static bool save_error(TileDB_CTX* ctx, const tiledb::Status& st) {
  if (st.ok()) {
    return false;
  }
  ctx->last_error_ = new tiledb::Status(st);
  return true;
}

tiledb::Status sanity_check(TileDB_CTX* ctx) {
  if (ctx == nullptr || ctx->storage_manager_ == nullptr) {
    return tiledb::Status::Error("Invalid TileDB context");
  }
  return tiledb::Status::Ok();
}

int tiledb_ctx_init(TileDB_CTX** ctx, const TileDB_Config* tiledb_config) {
  // Initialize context
  tiledb::Status st;
  *ctx = (TileDB_CTX*)malloc(sizeof(struct TileDB_CTX));
  if (*ctx == nullptr) {
    return TILEDB_OOM;
  }

  // Initialize a Config object
  tiledb::StorageManagerConfig* config = new tiledb::StorageManagerConfig();
  if (tiledb_config != nullptr)
    config->init(
        tiledb_config->home_,
#ifdef HAVE_MPI
        tiledb_config->mpi_comm_,
#endif
        static_cast<tiledb::IO>(tiledb_config->read_method_),
        static_cast<tiledb::IO>(tiledb_config->write_method_));

  // Create storage manager
  (*ctx)->storage_manager_ = new tiledb::StorageManager();
  (*ctx)->last_error_ = nullptr;
  if (save_error(*ctx, (*ctx)->storage_manager_->init(config)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_ctx_finalize(TileDB_CTX* ctx) {
  if (ctx == nullptr)
    return TILEDB_OK;

  tiledb::Status st;
  if (ctx->storage_manager_ != nullptr) {
    st = ctx->storage_manager_->finalize();
    delete ctx->storage_manager_;
  }

  if (ctx->last_error_ != nullptr)
    delete ctx->last_error_;

  free(ctx);

  return st.ok() ? TILEDB_OK : TILEDB_ERR;
}

typedef struct tiledb_error_t {
  // pointer to a copy of the last TileDB error assciated with a given ctx
  const tiledb::Status* status_;
} tiledb_error_t;

tiledb_error_t* tiledb_error_last(TileDB_CTX* ctx) {
  if (ctx == nullptr || ctx->last_error_ == nullptr)
    return nullptr;
  tiledb_error_t* err = (tiledb_error_t*)malloc(sizeof(tiledb_error_t));
  err->status_ = new tiledb::Status(*ctx->last_error_);
  return err;
}

const char* tiledb_error_message(tiledb_error_t* err) {
  if (err == nullptr || err->status_->ok()) {
    return "";
  }
  return err->status_->to_string().c_str();
}

int tiledb_error_free(tiledb_error_t* err) {
  if (err != nullptr) {
    if (err->status_ != nullptr) {
      delete err->status_;
    }
    free(err);
  }
  return TILEDB_OK;
}

tiledb::Status check_name_length(
    const char* obj_name, const char* path, size_t* length) {
  if (path == nullptr)
    return tiledb::Status::Error(
        std::string("Invalid ") + obj_name + " argument is NULL");
  size_t len = strlen(path);
  if (len > TILEDB_NAME_MAX_LEN) {
    return tiledb::Status::Error(
        std::string("Invalid ") + obj_name + " name length");
  }
  if (length != nullptr)
    *length = len;
  return tiledb::Status::Ok();
}

/* ****************************** */
/*              GROUP             */
/* ****************************** */

int tiledb_group_create(TileDB_CTX* ctx, const char* group) {
  if (!sanity_check(ctx).ok())
    return TILEDB_ERR;

  // Check group name length
  if (save_error(ctx, check_name_length("group", group, nullptr)))
    return TILEDB_ERR;

  // Create the group
  if (save_error(ctx, ctx->storage_manager_->group_create(group)))
    return TILEDB_ERR;

  // Success
  return TILEDB_OK;
}

/* ****************************** */
/*              ARRAY             */
/* ****************************** */

typedef struct TileDB_Array {
  tiledb::Array* array_;
  TileDB_CTX* ctx_;
} TileDB_Array;

tiledb::Status sanity_check(const TileDB_Array* tiledb_array) {
  if (tiledb_array == nullptr || tiledb_array->array_ == nullptr ||
      tiledb_array->ctx_ == nullptr) {
    return tiledb::Status::Error("Invalid TileDB array");
  }
  return tiledb::Status::Ok();
}

tiledb::Status sanity_check(TileDB_ArraySchema* sch) {
  if (sch == nullptr)
    return tiledb::Status::Error("Invalid TileDB array schema");
  return tiledb::Status::Ok();
}

int tiledb_array_set_schema(
    TileDB_CTX* ctx,
    TileDB_ArraySchema* tiledb_array_schema,
    const char* array_name,
    const char** attributes,
    int attribute_num,
    int64_t capacity,
    tiledb_layout_t cell_order,
    const int* cell_val_num,
    const tiledb_compressor_t* compression,
    int dense,
    const char** dimensions,
    int dim_num,
    const void* domain,
    size_t domain_len,
    const void* tile_extents,
    size_t tile_extents_len,
    tiledb_layout_t tile_order,
    const tiledb_datatype_t* types) {
  if (!sanity_check(ctx).ok())
    return TILEDB_ERR;

  if (save_error(ctx, sanity_check(tiledb_array_schema)))
    return TILEDB_ERR;

  size_t array_name_len = 0;
  if (save_error(ctx, check_name_length("array", array_name, &array_name_len)))
    return TILEDB_ERR;

  tiledb_array_schema->array_name_ = (char*)malloc(array_name_len + 1);
  if (tiledb_array_schema->array_name_ == nullptr)
    return TILEDB_OOM;
  strcpy(tiledb_array_schema->array_name_, array_name);

  // Set attributes and number of attributes
  tiledb_array_schema->attribute_num_ = attribute_num;
  tiledb_array_schema->attributes_ =
      (char**)malloc(attribute_num * sizeof(char*));
  if (tiledb_array_schema->attributes_ == nullptr)
    return TILEDB_OOM;
  for (int i = 0; i < attribute_num; ++i) {
    size_t attribute_len = 0;
    if (save_error(
            ctx, check_name_length("attribute", attributes[i], &attribute_len)))
      return TILEDB_ERR;
    tiledb_array_schema->attributes_[i] = (char*)malloc(attribute_len + 1);
    if (tiledb_array_schema->attributes_[i] == nullptr)
      return TILEDB_OOM;
    strcpy(tiledb_array_schema->attributes_[i], attributes[i]);
  }

  // Set dimensions
  tiledb_array_schema->dim_num_ = dim_num;
  tiledb_array_schema->dimensions_ = (char**)malloc(dim_num * sizeof(char*));
  if (tiledb_array_schema->dimensions_ == nullptr)
    return TILEDB_OOM;
  for (int i = 0; i < dim_num; ++i) {
    size_t dimension_len = 0;
    if (save_error(
            ctx, check_name_length("dimension", dimensions[i], &dimension_len)))
      return TILEDB_ERR;
    tiledb_array_schema->dimensions_[i] = (char*)malloc(dimension_len + 1);
    if (tiledb_array_schema->dimensions_[i] == nullptr)
      return TILEDB_OOM;
    strcpy(tiledb_array_schema->dimensions_[i], dimensions[i]);
  }

  // Set dense
  tiledb_array_schema->dense_ = dense;

  // Set domain
  tiledb_array_schema->domain_ = malloc(domain_len);
  memcpy(tiledb_array_schema->domain_, domain, domain_len);

  // Set tile extents
  if (tile_extents == nullptr) {
    tiledb_array_schema->tile_extents_ = nullptr;
  } else {
    tiledb_array_schema->tile_extents_ = malloc(tile_extents_len);
    if (tiledb_array_schema->tile_extents_ == nullptr)
      return TILEDB_OOM;
    memcpy(tiledb_array_schema->tile_extents_, tile_extents, tile_extents_len);
  }

  // Set types
  tiledb_array_schema->types_ =
      (tiledb_datatype_t*)malloc((attribute_num + 1) * sizeof(int));
  if (tiledb_array_schema->types_ == nullptr)
    return TILEDB_OOM;
  for (int i = 0; i < attribute_num + 1; ++i)
    tiledb_array_schema->types_[i] = types[i];

  // Set cell val num
  if (cell_val_num == nullptr) {
    tiledb_array_schema->cell_val_num_ = nullptr;
  } else {
    tiledb_array_schema->cell_val_num_ =
        (int*)malloc((attribute_num) * sizeof(int));
    if (tiledb_array_schema->cell_val_num_ == nullptr)
      return TILEDB_OOM;
    for (int i = 0; i < attribute_num; ++i) {
      tiledb_array_schema->cell_val_num_[i] = cell_val_num[i];
    }
  }

  // Set cell and tile order
  tiledb_array_schema->cell_order_ = cell_order;
  tiledb_array_schema->tile_order_ = tile_order;

  // Set capacity
  tiledb_array_schema->capacity_ = capacity;

  // Set compression
  if (compression == nullptr) {
    tiledb_array_schema->compressor_ = nullptr;
  } else {
    tiledb_array_schema->compressor_ =
        (tiledb_compressor_t*)malloc((attribute_num + 1) * sizeof(int));
    if (tiledb_array_schema->compressor_ == nullptr)
      return TILEDB_OOM;
    for (int i = 0; i < attribute_num + 1; ++i)
      tiledb_array_schema->compressor_[i] = compression[i];
  }

  return TILEDB_OK;
}

int tiledb_array_create(
    TileDB_CTX* ctx, const TileDB_ArraySchema* array_schema) {
  if (!sanity_check(ctx).ok())
    return TILEDB_ERR;

  // Copy array schema to a C struct
  ArraySchemaC array_schema_c;
  array_schema_c.array_name_ = array_schema->array_name_;
  array_schema_c.attributes_ = array_schema->attributes_;
  array_schema_c.attribute_num_ = array_schema->attribute_num_;
  array_schema_c.capacity_ = array_schema->capacity_;
  array_schema_c.cell_order_ = array_schema->cell_order_;
  array_schema_c.cell_val_num_ = array_schema->cell_val_num_;
  array_schema_c.compressor_ = array_schema->compressor_;
  array_schema_c.dense_ = array_schema->dense_;
  array_schema_c.dimensions_ = array_schema->dimensions_;
  array_schema_c.dim_num_ = array_schema->dim_num_;
  array_schema_c.domain_ = array_schema->domain_;
  array_schema_c.tile_extents_ = array_schema->tile_extents_;
  array_schema_c.tile_order_ = array_schema->tile_order_;
  array_schema_c.types_ = array_schema->types_;

  // Create the array
  if (save_error(ctx, ctx->storage_manager_->array_create(&array_schema_c)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_array_init(
    TileDB_CTX* ctx,
    TileDB_Array** tiledb_array,
    const char* array,
    tiledb_array_mode_t mode,
    const void* subarray,
    const char** attributes,
    int attribute_num) {
  if (!sanity_check(ctx).ok())
    return TILEDB_ERR;

  if (save_error(ctx, check_name_length("array", array, nullptr)))
    return TILEDB_ERR;

  // Allocate memory for the array struct
  *tiledb_array = (TileDB_Array*)malloc(sizeof(struct TileDB_Array));
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

int tiledb_array_reset_subarray(
    const TileDB_Array* tiledb_array, const void* subarray) {
  if (!sanity_check(tiledb_array).ok())
    return TILEDB_ERR;

  TileDB_CTX* ctx = tiledb_array->ctx_;

  if (save_error(ctx, tiledb_array->array_->reset_subarray(subarray)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_array_reset_attributes(
    const TileDB_Array* tiledb_array,
    const char** attributes,
    int attribute_num) {
  if (!sanity_check(tiledb_array).ok())
    return TILEDB_ERR;

  TileDB_CTX* ctx = tiledb_array->ctx_;

  // Re-Init the array
  if (save_error(
          ctx,
          tiledb_array->array_->reset_attributes(attributes, attribute_num)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_array_get_schema(
    const TileDB_Array* tiledb_array, TileDB_ArraySchema* tiledb_array_schema) {
  if (!sanity_check(tiledb_array).ok())
    return TILEDB_ERR;

  // Get the array schema
  ArraySchemaC array_schema_c;
  tiledb_array->array_->array_schema()->array_schema_export(&array_schema_c);

  // Copy the array schema C struct to the output
  tiledb_array_schema->array_name_ = array_schema_c.array_name_;
  tiledb_array_schema->attributes_ = array_schema_c.attributes_;
  tiledb_array_schema->attribute_num_ = array_schema_c.attribute_num_;
  tiledb_array_schema->capacity_ = array_schema_c.capacity_;
  tiledb_array_schema->cell_order_ = array_schema_c.cell_order_;
  tiledb_array_schema->cell_val_num_ = array_schema_c.cell_val_num_;
  tiledb_array_schema->compressor_ = array_schema_c.compressor_;
  tiledb_array_schema->dense_ = array_schema_c.dense_;
  tiledb_array_schema->dimensions_ = array_schema_c.dimensions_;
  tiledb_array_schema->dim_num_ = array_schema_c.dim_num_;
  tiledb_array_schema->domain_ = array_schema_c.domain_;
  tiledb_array_schema->tile_extents_ = array_schema_c.tile_extents_;
  tiledb_array_schema->tile_order_ = array_schema_c.tile_order_;
  tiledb_array_schema->types_ = array_schema_c.types_;

  return TILEDB_OK;
}

int tiledb_array_load_schema(
    TileDB_CTX* ctx,
    const char* array,
    TileDB_ArraySchema* tiledb_array_schema) {
  if (!sanity_check(ctx).ok())
    return TILEDB_ERR;

  // Check array name length
  if (save_error(ctx, check_name_length("array", array, nullptr)))
    return TILEDB_ERR;

  // Get the array schema
  tiledb::ArraySchema* array_schema;
  if (save_error(
          ctx, ctx->storage_manager_->array_load_schema(array, array_schema)))
    return TILEDB_ERR;

  ArraySchemaC array_schema_c;
  array_schema->array_schema_export(&array_schema_c);

  // Copy the array schema C struct to the output
  tiledb_array_schema->array_name_ = array_schema_c.array_name_;
  tiledb_array_schema->attributes_ = array_schema_c.attributes_;
  tiledb_array_schema->attribute_num_ = array_schema_c.attribute_num_;
  tiledb_array_schema->capacity_ = array_schema_c.capacity_;
  tiledb_array_schema->cell_order_ = array_schema_c.cell_order_;
  tiledb_array_schema->cell_val_num_ = array_schema_c.cell_val_num_;
  tiledb_array_schema->compressor_ = array_schema_c.compressor_;
  tiledb_array_schema->dense_ = array_schema_c.dense_;
  tiledb_array_schema->dimensions_ = array_schema_c.dimensions_;
  tiledb_array_schema->dim_num_ = array_schema_c.dim_num_;
  tiledb_array_schema->domain_ = array_schema_c.domain_;
  tiledb_array_schema->tile_extents_ = array_schema_c.tile_extents_;
  tiledb_array_schema->tile_order_ = array_schema_c.tile_order_;
  tiledb_array_schema->types_ = array_schema_c.types_;

  delete array_schema;

  return TILEDB_OK;
}

int tiledb_array_free_schema(TileDB_ArraySchema* tiledb_array_schema) {
  // Trivial case
  if (tiledb_array_schema == nullptr)
    return TILEDB_OK;

  // Free array name
  if (tiledb_array_schema->array_name_ != nullptr)
    free(tiledb_array_schema->array_name_);

  // Free attributes
  if (tiledb_array_schema->attributes_ != nullptr) {
    for (int i = 0; i < tiledb_array_schema->attribute_num_; ++i)
      if (tiledb_array_schema->attributes_[i] != nullptr)
        free(tiledb_array_schema->attributes_[i]);
    free(tiledb_array_schema->attributes_);
  }

  // Free dimensions
  if (tiledb_array_schema->dimensions_ != nullptr) {
    for (int i = 0; i < tiledb_array_schema->dim_num_; ++i)
      if (tiledb_array_schema->dimensions_[i] != nullptr)
        free(tiledb_array_schema->dimensions_[i]);
    free(tiledb_array_schema->dimensions_);
  }

  // Free domain
  if (tiledb_array_schema->domain_ != nullptr)
    free(tiledb_array_schema->domain_);

  // Free tile extents
  if (tiledb_array_schema->tile_extents_ != nullptr)
    free(tiledb_array_schema->tile_extents_);

  // Free types
  if (tiledb_array_schema->types_ != nullptr)
    free(tiledb_array_schema->types_);

  // Free compression
  if (tiledb_array_schema->compressor_ != nullptr)
    free(tiledb_array_schema->compressor_);

  // Free cell val num
  if (tiledb_array_schema->cell_val_num_ != nullptr)
    free(tiledb_array_schema->cell_val_num_);

  return TILEDB_OK;
}

int tiledb_array_write(
    const TileDB_Array* tiledb_array,
    const void** buffers,
    const size_t* buffer_sizes) {
  if (!sanity_check(tiledb_array).ok())
    return TILEDB_ERR;

  TileDB_CTX* ctx = tiledb_array->ctx_;

  if (save_error(ctx, sanity_check(tiledb_array)))
    return TILEDB_ERR;

  if (save_error(ctx, tiledb_array->array_->write(buffers, buffer_sizes)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_array_read(
    const TileDB_Array* tiledb_array, void** buffers, size_t* buffer_sizes) {
  if (!sanity_check(tiledb_array).ok())
    return TILEDB_ERR;

  TileDB_CTX* ctx = tiledb_array->ctx_;

  if (save_error(ctx, sanity_check(tiledb_array)))
    return TILEDB_ERR;

  if (save_error(ctx, tiledb_array->array_->read(buffers, buffer_sizes)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_array_overflow(const TileDB_Array* tiledb_array, int attribute_id) {
  if (!sanity_check(tiledb_array).ok())
    return TILEDB_ERR;

  TileDB_CTX* ctx = tiledb_array->ctx_;

  if (save_error(ctx, sanity_check(tiledb_array)))
    return TILEDB_ERR;

  return (int)tiledb_array->array_->overflow(attribute_id);
}

int tiledb_array_consolidate(TileDB_CTX* ctx, const char* array) {
  if (!sanity_check(ctx).ok())
    return TILEDB_ERR;

  if (save_error(ctx, check_name_length("array", array, nullptr)))
    return TILEDB_ERR;

  if (save_error(ctx, ctx->storage_manager_->array_consolidate(array)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_array_finalize(TileDB_Array* tiledb_array) {
  if (!sanity_check(tiledb_array).ok())
    return TILEDB_ERR;

  TileDB_CTX* ctx = tiledb_array->ctx_;

  if (save_error(ctx, sanity_check(tiledb_array)))
    return TILEDB_ERR;

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

int tiledb_array_sync(TileDB_Array* tiledb_array) {
  if (!sanity_check(tiledb_array).ok())
    return TILEDB_ERR;

  TileDB_CTX* ctx = tiledb_array->ctx_;

  if (save_error(ctx, sanity_check(tiledb_array)))
    return TILEDB_ERR;

  if (save_error(
          ctx,
          tiledb_array->ctx_->storage_manager_->array_sync(
              tiledb_array->array_)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_array_sync_attribute(
    TileDB_Array* tiledb_array, const char* attribute) {
  if (!sanity_check(tiledb_array).ok())
    return TILEDB_ERR;

  TileDB_CTX* ctx = tiledb_array->ctx_;

  if (save_error(ctx, sanity_check(tiledb_array)))
    return TILEDB_ERR;

  if (save_error(
          ctx,
          tiledb_array->ctx_->storage_manager_->array_sync_attribute(
              tiledb_array->array_, attribute)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

typedef struct TileDB_ArrayIterator {
  tiledb::ArrayIterator* array_it_;
  TileDB_CTX* ctx_;
} TileDB_ArrayIterator;

tiledb::Status sanity_check(const TileDB_ArrayIterator* tiledb_array_it) {
  if (tiledb_array_it == nullptr || tiledb_array_it->array_it_ == nullptr ||
      tiledb_array_it->ctx_ == nullptr) {
    return tiledb::Status::Error("Invalid TileDB array iterator");
  }
  return tiledb::Status::Ok();
}

int tiledb_array_iterator_init(
    TileDB_CTX* ctx,
    TileDB_ArrayIterator** tiledb_array_it,
    const char* array,
    tiledb_array_mode_t mode,
    const void* subarray,
    const char** attributes,
    int attribute_num,
    void** buffers,
    size_t* buffer_sizes) {
  if (!sanity_check(ctx).ok())
    return TILEDB_ERR;

  *tiledb_array_it =
      (TileDB_ArrayIterator*)malloc(sizeof(struct TileDB_ArrayIterator));
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
    TileDB_ArrayIterator* tiledb_array_it,
    int attribute_id,
    const void** value,
    size_t* value_size) {
  if (!sanity_check(tiledb_array_it).ok())
    return TILEDB_ERR;

  TileDB_CTX* ctx = tiledb_array_it->ctx_;

  if (save_error(ctx, sanity_check(tiledb_array_it)))
    return TILEDB_ERR;

  // Get value
  if (save_error(
          ctx,
          tiledb_array_it->array_it_->get_value(
              attribute_id, value, value_size)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_array_iterator_next(TileDB_ArrayIterator* tiledb_array_it) {
  if (!sanity_check(tiledb_array_it).ok())
    return TILEDB_ERR;

  TileDB_CTX* ctx = tiledb_array_it->ctx_;

  if (save_error(ctx, sanity_check(tiledb_array_it)))
    return TILEDB_ERR;

  if (save_error(ctx, tiledb_array_it->array_it_->next()))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_array_iterator_end(TileDB_ArrayIterator* tiledb_array_it) {
  if (!sanity_check(tiledb_array_it).ok())
    return TILEDB_ERR;

  TileDB_CTX* ctx = tiledb_array_it->ctx_;

  if (save_error(ctx, sanity_check(tiledb_array_it)))
    return TILEDB_ERR;

  return (int)tiledb_array_it->array_it_->end();
}

int tiledb_array_iterator_finalize(TileDB_ArrayIterator* tiledb_array_it) {
  if (!sanity_check(tiledb_array_it).ok())
    return TILEDB_ERR;

  TileDB_CTX* ctx = tiledb_array_it->ctx_;

  if (save_error(ctx, sanity_check(tiledb_array_it)))
    return TILEDB_ERR;

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

/* ****************************** */
/*            METADATA            */
/* ****************************** */

typedef struct TileDB_Metadata {
  tiledb::Metadata* metadata_;
  TileDB_CTX* ctx_;
} TileDB_Metadata;

tiledb::Status sanity_check(const TileDB_Metadata* tiledb_metadata) {
  if (tiledb_metadata == nullptr || tiledb_metadata->metadata_ == nullptr ||
      tiledb_metadata->ctx_ == nullptr)
    return tiledb::Status::Error("Invalid TileDB metadata");
  return tiledb::Status::Ok();
}

tiledb::Status sanity_check(const TileDB_MetadataSchema* sch) {
  if (sch == nullptr)
    return tiledb::Status::Error("Invalid metadata schema");
  return tiledb::Status::Ok();
}

int tiledb_metadata_set_schema(
    TileDB_CTX* ctx,
    TileDB_MetadataSchema* tiledb_metadata_schema,
    const char* metadata_name,
    const char** attributes,
    int attribute_num,
    int64_t capacity,
    const int* cell_val_num,
    const tiledb_compressor_t* compression,
    const tiledb_datatype_t* types) {
  if (!sanity_check(ctx).ok())
    return TILEDB_ERR;

  if (save_error(ctx, sanity_check(tiledb_metadata_schema)))
    return TILEDB_ERR;

  size_t metadata_name_len = 0;
  if (save_error(
          ctx,
          check_name_length("metadata", metadata_name, &metadata_name_len)))
    return TILEDB_ERR;

  tiledb_metadata_schema->metadata_name_ = (char*)malloc(metadata_name_len + 1);
  if (tiledb_metadata_schema->metadata_name_ == nullptr)
    return TILEDB_OOM;
  strcpy(tiledb_metadata_schema->metadata_name_, metadata_name);

  /* Set attributes and number of attributes. */
  tiledb_metadata_schema->attribute_num_ = attribute_num;
  tiledb_metadata_schema->attributes_ =
      (char**)malloc(attribute_num * sizeof(char*));
  if (tiledb_metadata_schema->attributes_ == nullptr)
    return TILEDB_OOM;
  for (int i = 0; i < attribute_num; ++i) {
    size_t attribute_len = 0;
    if (save_error(
            ctx, check_name_length("attribute", attributes[i], &attribute_len)))
      return TILEDB_ERR;
    tiledb_metadata_schema->attributes_[i] = (char*)malloc(attribute_len + 1);
    if (tiledb_metadata_schema->attributes_[i] == nullptr)
      return TILEDB_OOM;
    strcpy(tiledb_metadata_schema->attributes_[i], attributes[i]);
  }

  // Set types
  tiledb_metadata_schema->types_ =
      (tiledb_datatype_t*)malloc((attribute_num + 1) * sizeof(int));
  if (tiledb_metadata_schema->types_ == nullptr)
    return TILEDB_OOM;
  for (int i = 0; i < attribute_num + 1; ++i)
    tiledb_metadata_schema->types_[i] = types[i];

  // Set cell val num
  if (cell_val_num == nullptr) {
    tiledb_metadata_schema->cell_val_num_ = nullptr;
  } else {
    tiledb_metadata_schema->cell_val_num_ =
        (int*)malloc((attribute_num) * sizeof(int));
    if (tiledb_metadata_schema->cell_val_num_ == nullptr)
      return TILEDB_OOM;
    for (int i = 0; i < attribute_num; ++i) {
      tiledb_metadata_schema->cell_val_num_[i] = cell_val_num[i];
    }
  }

  // Set capacity
  tiledb_metadata_schema->capacity_ = capacity;

  // Set compression
  if (compression == nullptr) {
    tiledb_metadata_schema->compressor_ = nullptr;
  } else {
    tiledb_metadata_schema->compressor_ =
        (tiledb_compressor_t*)malloc((attribute_num + 1) * sizeof(int));
    if (tiledb_metadata_schema->compressor_ == nullptr)
      return TILEDB_OOM;
    for (int i = 0; i < attribute_num + 1; ++i)
      tiledb_metadata_schema->compressor_[i] = compression[i];
  }
  return TILEDB_OK;
}

int tiledb_metadata_create(
    TileDB_CTX* ctx, const TileDB_MetadataSchema* metadata_schema) {
  if (!sanity_check(ctx).ok())
    return TILEDB_ERR;

  if (save_error(ctx, sanity_check(metadata_schema)))
    return TILEDB_ERR;

  // Copy metadata schema to the proper struct
  MetadataSchemaC metadata_schema_c;
  metadata_schema_c.metadata_name_ = metadata_schema->metadata_name_;
  metadata_schema_c.attributes_ = metadata_schema->attributes_;
  metadata_schema_c.attribute_num_ = metadata_schema->attribute_num_;
  metadata_schema_c.capacity_ = metadata_schema->capacity_;
  metadata_schema_c.cell_val_num_ = metadata_schema->cell_val_num_;
  metadata_schema_c.compressor_ = metadata_schema->compressor_;
  metadata_schema_c.types_ = metadata_schema->types_;

  if (save_error(
          ctx, ctx->storage_manager_->metadata_create(&metadata_schema_c)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_metadata_init(
    TileDB_CTX* ctx,
    TileDB_Metadata** tiledb_metadata,
    const char* metadata,
    tiledb_metadata_mode_t mode,
    const char** attributes,
    int attribute_num) {
  if (!sanity_check(ctx).ok())
    return TILEDB_ERR;

  // Allocate memory for the array struct
  *tiledb_metadata = (TileDB_Metadata*)malloc(sizeof(struct TileDB_Metadata));
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
    const TileDB_Metadata* tiledb_metadata,
    const char** attributes,
    int attribute_num) {
  if (!sanity_check(tiledb_metadata).ok())
    return TILEDB_ERR;

  TileDB_CTX* ctx = tiledb_metadata->ctx_;

  // Reset attributes
  if (save_error(
          ctx,
          tiledb_metadata->metadata_->reset_attributes(
              attributes, attribute_num)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_metadata_get_schema(
    const TileDB_Metadata* tiledb_metadata,
    TileDB_MetadataSchema* tiledb_metadata_schema) {
  if (!sanity_check(tiledb_metadata).ok())
    return TILEDB_ERR;

  TileDB_CTX* ctx = tiledb_metadata->ctx_;

  if (save_error(ctx, sanity_check(tiledb_metadata)))
    return TILEDB_ERR;

  // Get the metadata schema
  MetadataSchemaC metadata_schema_c;
  tiledb_metadata->metadata_->array_schema()->array_schema_export(
      &metadata_schema_c);

  // Copy the metadata schema C struct to the output
  tiledb_metadata_schema->metadata_name_ = metadata_schema_c.metadata_name_;
  tiledb_metadata_schema->attributes_ = metadata_schema_c.attributes_;
  tiledb_metadata_schema->attribute_num_ = metadata_schema_c.attribute_num_;
  tiledb_metadata_schema->capacity_ = metadata_schema_c.capacity_;
  tiledb_metadata_schema->cell_val_num_ = metadata_schema_c.cell_val_num_;
  tiledb_metadata_schema->compressor_ = metadata_schema_c.compressor_;
  tiledb_metadata_schema->types_ = metadata_schema_c.types_;

  return TILEDB_OK;
}

int tiledb_metadata_load_schema(
    TileDB_CTX* ctx,
    const char* metadata,
    TileDB_MetadataSchema* tiledb_metadata_schema) {
  if (!sanity_check(ctx).ok())
    return TILEDB_ERR;

  if (save_error(ctx, check_name_length("metadata", metadata, nullptr)))
    return TILEDB_ERR;

  // Get the array schema
  tiledb::ArraySchema* array_schema;

  if (save_error(
          ctx,
          ctx->storage_manager_->metadata_load_schema(metadata, array_schema)))
    return TILEDB_ERR;

  MetadataSchemaC metadata_schema_c;
  array_schema->array_schema_export(&metadata_schema_c);

  // Copy the metadata schema C struct to the output
  tiledb_metadata_schema->metadata_name_ = metadata_schema_c.metadata_name_;
  tiledb_metadata_schema->attributes_ = metadata_schema_c.attributes_;
  tiledb_metadata_schema->attribute_num_ = metadata_schema_c.attribute_num_;
  tiledb_metadata_schema->capacity_ = metadata_schema_c.capacity_;
  tiledb_metadata_schema->cell_val_num_ = metadata_schema_c.cell_val_num_;
  tiledb_metadata_schema->compressor_ = metadata_schema_c.compressor_;
  tiledb_metadata_schema->types_ = metadata_schema_c.types_;

  delete array_schema;

  return TILEDB_OK;
}

int tiledb_metadata_free_schema(TileDB_MetadataSchema* tiledb_metadata_schema) {
  if (tiledb_metadata_schema == nullptr)
    return TILEDB_OK;

  // Free name
  if (tiledb_metadata_schema->metadata_name_ != nullptr)
    free(tiledb_metadata_schema->metadata_name_);

  // Free attributes
  if (tiledb_metadata_schema->attributes_ != nullptr) {
    for (int i = 0; i < tiledb_metadata_schema->attribute_num_; ++i)
      if (tiledb_metadata_schema->attributes_[i] != nullptr)
        free(tiledb_metadata_schema->attributes_[i]);
    free(tiledb_metadata_schema->attributes_);
  }

  // Free types
  if (tiledb_metadata_schema->types_ != nullptr)
    free(tiledb_metadata_schema->types_);

  // Free compression
  if (tiledb_metadata_schema->compressor_ != nullptr)
    free(tiledb_metadata_schema->compressor_);

  // Free cell val num
  if (tiledb_metadata_schema->cell_val_num_ != nullptr)
    free(tiledb_metadata_schema->cell_val_num_);

  return TILEDB_OK;
}

int tiledb_metadata_write(
    const TileDB_Metadata* tiledb_metadata,
    const char* keys,
    size_t keys_size,
    const void** buffers,
    const size_t* buffer_sizes) {
  if (!sanity_check(tiledb_metadata).ok())
    return TILEDB_ERR;

  TileDB_CTX* ctx = tiledb_metadata->ctx_;

  if (save_error(
          ctx,
          tiledb_metadata->metadata_->write(
              keys, keys_size, buffers, buffer_sizes)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_metadata_read(
    const TileDB_Metadata* tiledb_metadata,
    const char* key,
    void** buffers,
    size_t* buffer_sizes) {
  if (!sanity_check(tiledb_metadata).ok())
    return TILEDB_ERR;

  TileDB_CTX* ctx = tiledb_metadata->ctx_;

  if (save_error(
          ctx, tiledb_metadata->metadata_->read(key, buffers, buffer_sizes)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_metadata_overflow(
    const TileDB_Metadata* tiledb_metadata, int attribute_id) {
  if (!sanity_check(tiledb_metadata).ok())
    return TILEDB_ERR;

  return (int)tiledb_metadata->metadata_->overflow(attribute_id);
}

int tiledb_metadata_consolidate(TileDB_CTX* ctx, const char* metadata) {
  if (!sanity_check(ctx).ok())
    return TILEDB_ERR;

  if (save_error(ctx, check_name_length("metadata", metadata, nullptr)))
    return TILEDB_ERR;

  if (save_error(ctx, ctx->storage_manager_->metadata_consolidate(metadata)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_metadata_finalize(TileDB_Metadata* tiledb_metadata) {
  if (!sanity_check(tiledb_metadata).ok())
    return TILEDB_ERR;

  TileDB_CTX* ctx = tiledb_metadata->ctx_;

  if (save_error(
          ctx,
          tiledb_metadata->ctx_->storage_manager_->metadata_finalize(
              tiledb_metadata->metadata_))) {
    free(tiledb_metadata);
    return TILEDB_ERR;
  }
  return TILEDB_OK;
}

typedef struct TileDB_MetadataIterator {
  tiledb::MetadataIterator* metadata_it_;
  TileDB_CTX* ctx_;
} TileDB_MetadataIterator;

tiledb::Status sanity_check(const TileDB_MetadataIterator* tiledb_metadata_it) {
  if (tiledb_metadata_it == nullptr ||
      tiledb_metadata_it->metadata_it_ == nullptr ||
      tiledb_metadata_it->ctx_ == nullptr) {
    return tiledb::Status::Error("Invalid TileDB metadata iterator");
  }
  return tiledb::Status::Ok();
}

int tiledb_metadata_iterator_init(
    TileDB_CTX* ctx,
    TileDB_MetadataIterator** tiledb_metadata_it,
    const char* metadata,
    const char** attributes,
    int attribute_num,
    void** buffers,
    size_t* buffer_sizes) {
  if (!sanity_check(ctx).ok())
    return TILEDB_ERR;

  // Allocate memory for the metadata struct
  *tiledb_metadata_it =
      (TileDB_MetadataIterator*)malloc(sizeof(struct TileDB_MetadataIterator));
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
    TileDB_MetadataIterator* tiledb_metadata_it,
    int attribute_id,
    const void** value,
    size_t* value_size) {
  if (!sanity_check(tiledb_metadata_it).ok())
    return TILEDB_ERR;

  TileDB_CTX* ctx = tiledb_metadata_it->ctx_;

  if (save_error(ctx, sanity_check(tiledb_metadata_it)))
    return TILEDB_ERR;

  if (save_error(
          ctx,
          tiledb_metadata_it->metadata_it_->get_value(
              attribute_id, value, value_size)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_metadata_iterator_next(TileDB_MetadataIterator* tiledb_metadata_it) {
  if (!sanity_check(tiledb_metadata_it).ok())
    return TILEDB_ERR;

  TileDB_CTX* ctx = tiledb_metadata_it->ctx_;

  // Advance metadata iterator
  if (save_error(ctx, tiledb_metadata_it->metadata_it_->next()))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_metadata_iterator_end(TileDB_MetadataIterator* tiledb_metadata_it) {
  if (!sanity_check(tiledb_metadata_it).ok())
    return TILEDB_ERR;

  // Check if the metadata iterator reached its end
  return (int)tiledb_metadata_it->metadata_it_->end();
}

int tiledb_metadata_iterator_finalize(
    TileDB_MetadataIterator* tiledb_metadata_it) {
  if (!sanity_check(tiledb_metadata_it).ok())
    return TILEDB_ERR;

  TileDB_CTX* ctx = tiledb_metadata_it->ctx_;

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

/* ****************************** */
/*       DIRECTORY MANAGEMENT     */
/* ****************************** */

int tiledb_dir_type(TileDB_CTX* ctx, const char* dir) {
  if (ctx == nullptr)
    return TILEDB_ERR;
  return ctx->storage_manager_->dir_type(dir);
}

int tiledb_clear(TileDB_CTX* ctx, const char* dir) {
  if (!sanity_check(ctx).ok())
    return TILEDB_ERR;

  if (save_error(ctx, check_name_length("directory", dir, nullptr)))
    return TILEDB_ERR;

  if (save_error(ctx, ctx->storage_manager_->clear(dir)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_delete(TileDB_CTX* ctx, const char* dir) {
  if (!sanity_check(ctx).ok())
    return TILEDB_ERR;

  if (save_error(ctx, check_name_length("directory", dir, nullptr)))
    return TILEDB_ERR;

  if (save_error(ctx, ctx->storage_manager_->delete_entire(dir)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_move(TileDB_CTX* ctx, const char* old_dir, const char* new_dir) {
  if (!sanity_check(ctx).ok())
    return TILEDB_ERR;

  if (save_error(ctx, check_name_length("old directory ", old_dir, nullptr)))
    return TILEDB_ERR;

  if (save_error(ctx, check_name_length("new directory", new_dir, nullptr)))
    return TILEDB_ERR;

  if (save_error(ctx, ctx->storage_manager_->move(old_dir, new_dir)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_ls(
    TileDB_CTX* ctx,
    const char* parent_dir,
    char** dirs,
    tiledb_object_t* dir_types,
    int* dir_num) {
  if (!sanity_check(ctx).ok())
    return TILEDB_ERR;

  if (save_error(
          ctx, check_name_length("parent directory", parent_dir, nullptr)))
    return TILEDB_ERR;

  if (save_error(
          ctx,
          ctx->storage_manager_->ls(parent_dir, dirs, dir_types, *dir_num)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_ls_c(TileDB_CTX* ctx, const char* parent_dir, int* dir_num) {
  if (!sanity_check(ctx).ok())
    return TILEDB_ERR;

  if (save_error(
          ctx, check_name_length("parent directory", parent_dir, nullptr)))
    return TILEDB_ERR;

  if (save_error(ctx, ctx->storage_manager_->ls_c(parent_dir, *dir_num)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

/* ****************************** */
/*     ASYNCHRONOUS I/O (AIO      */
/* ****************************** */

int tiledb_array_aio_read(
    const TileDB_Array* tiledb_array, TileDB_AIO_Request* tiledb_aio_request) {
  if (!sanity_check(tiledb_array).ok())
    return TILEDB_ERR;

  TileDB_CTX* ctx = tiledb_array->ctx_;

  // Copy the AIO request
  AIO_Request* aio_request = (AIO_Request*)malloc(sizeof(struct AIO_Request));
  aio_request->id_ = (size_t)tiledb_aio_request;
  aio_request->buffers_ = tiledb_aio_request->buffers_;
  aio_request->buffer_sizes_ = tiledb_aio_request->buffer_sizes_;
  aio_request->mode_ =
      static_cast<tiledb_array_mode_t>(tiledb_array->array_->mode());
  aio_request->status_ = &(tiledb_aio_request->status_);
  aio_request->subarray_ = tiledb_aio_request->subarray_;
  aio_request->completion_handle_ = tiledb_aio_request->completion_handle_;
  aio_request->completion_data_ = tiledb_aio_request->completion_data_;

  // Submit the AIO read request
  if (save_error(ctx, tiledb_array->array_->aio_read(aio_request)))
    return TILEDB_ERR;

  return TILEDB_OK;
}

int tiledb_array_aio_write(
    const TileDB_Array* tiledb_array, TileDB_AIO_Request* tiledb_aio_request) {
  if (!sanity_check(tiledb_array).ok())
    return TILEDB_ERR;

  TileDB_CTX* ctx = tiledb_array->ctx_;

  // Copy the AIO request
  AIO_Request* aio_request = (AIO_Request*)malloc(sizeof(struct AIO_Request));
  aio_request->id_ = (size_t)tiledb_aio_request;
  aio_request->buffers_ = tiledb_aio_request->buffers_;
  aio_request->buffer_sizes_ = tiledb_aio_request->buffer_sizes_;
  aio_request->mode_ =
      static_cast<tiledb_array_mode_t>(tiledb_array->array_->mode());
  aio_request->status_ = &(tiledb_aio_request->status_);
  aio_request->subarray_ = tiledb_aio_request->subarray_;
  aio_request->completion_handle_ = tiledb_aio_request->completion_handle_;
  aio_request->completion_data_ = tiledb_aio_request->completion_data_;

  // Submit the AIO write request
  if (save_error(ctx, tiledb_array->array_->aio_write(aio_request)))
    return TILEDB_ERR;

  return TILEDB_OK;
}
