/**
 * @file   c_api.cc
 * @author Stavros Papadopoulos <stavrosp@csail.mit.edu>
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * Copyright (c) 2015 Stavros Papadopoulos <stavrosp@csail.mit.edu>
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

#include "c_api.h"
#include "array_schema_c.h"
#include "storage_manager.h"
#include <cassert>
#include <cstring>
#include <iostream>

/* ****************************** */
/*             MACROS             */
/* ****************************** */

#if VERBOSE == 1
#  define PRINT_ERROR(x) std::cerr << "[TileDB] Error: " << x << ".\n" 
#elif VERBOSE == 2
#  define PRINT_ERROR(x) std::cerr << "[TileDB::c_api] Error: " << x << ".\n" 
#else
#  define PRINT_ERROR(x) do { } while(0) 
#endif

/* ****************************** */
/*            CONTEXT             */
/* ****************************** */

typedef struct TileDB_CTX{
  StorageManager* storage_manager_;
} TileDB_CTX;

int tiledb_ctx_init(TileDB_CTX** tiledb_ctx, const char* config_filename) {
  // Initialize context
  *tiledb_ctx = (TileDB_CTX*) malloc(sizeof(struct TileDB_CTX));
  if(*tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot initialize TileDB context; Failed to allocate memory "
                "space for the context");
    return TILEDB_ERR;
  }

  // Create TileDB storage manager
  (*tiledb_ctx)->storage_manager_ = new StorageManager(config_filename);

  // Success
  return TILEDB_OK;
}

int tiledb_ctx_finalize(TileDB_CTX* tiledb_ctx) {
  // Trivial case
  if(tiledb_ctx == NULL)
    return TILEDB_OK;

  // Sanity checks
  assert(tiledb_ctx->storage_manager_);

  // Delete TileDB storage manager
  delete tiledb_ctx->storage_manager_;

  // Success
  return TILEDB_OK;
}

/* ****************************** */
/*          SANITY CHECKS         */
/* ****************************** */

bool sanity_check(const TileDB_CTX* tiledb_ctx) {
  if(tiledb_ctx == NULL || tiledb_ctx->storage_manager_ == NULL) {
    PRINT_ERROR("Invalid TileDB context");
    return false;
  } else {
    return true;
  }
}

bool sanity_check(const TileDB_Array* tiledb_array) {
  if(tiledb_array == NULL) {
    PRINT_ERROR("Invalid TileDB array");
    return false;
  } else {
    return true;
  }
}

/* ****************************** */
/*            WORKSPACE           */
/* ****************************** */

int tiledb_workspace_create(
    const TileDB_CTX* tiledb_ctx,
    const char* dir) {
  // Sanity check
  if(!sanity_check(tiledb_ctx))
    return TILEDB_ERR;

  // Create the workspace
  int rc = tiledb_ctx->storage_manager_->workspace_create(dir);

  // Return
  if(rc == TILEDB_SM_OK)
    return TILEDB_OK;
  else
    return TILEDB_ERR;
}

/* ****************************** */
/*              GROUP             */
/* ****************************** */

int tiledb_group_create(
    const TileDB_CTX* tiledb_ctx,
    const char* dir) {
  // Sanity check
  if(!sanity_check(tiledb_ctx))
    return TILEDB_ERR;

  // Create the group
  int rc = tiledb_ctx->storage_manager_->group_create(dir);

  // Return
  if(rc == TILEDB_SM_OK)
    return TILEDB_OK;
  else
    return TILEDB_ERR;
}

/* ****************************** */
/*              ARRAY             */
/* ****************************** */

typedef struct TileDB_Array {
  Array* array_;
  const TileDB_CTX* tiledb_ctx_;
} TileDB_Array;

int tiledb_array_create(
    const TileDB_CTX* tiledb_ctx,
    const TileDB_ArraySchema* array_schema) {
  // Sanity check
  if(!sanity_check(tiledb_ctx))
    return TILEDB_ERR;

  // Copy array schema to the proper struct
  ArraySchemaC array_schema_c;
  array_schema_c.array_name_ = array_schema->array_name_;
  array_schema_c.attributes_ = array_schema->attributes_;
  array_schema_c.attribute_num_ = array_schema->attribute_num_;
  array_schema_c.capacity_ = array_schema->capacity_;
  array_schema_c.cell_order_ = array_schema->cell_order_;
  array_schema_c.cell_val_num_ = array_schema->cell_val_num_;
  array_schema_c.compression_ = array_schema->compression_;
  array_schema_c.dense_ = array_schema->dense_;
  array_schema_c.dimensions_ = array_schema->dimensions_;
  array_schema_c.dim_num_ = array_schema->dim_num_;
  array_schema_c.domain_ = array_schema->domain_;
  array_schema_c.tile_extents_ = array_schema->tile_extents_;
  array_schema_c.tile_order_ = array_schema->tile_order_;
  array_schema_c.types_ = array_schema->types_;

  // Create the array
  int rc = tiledb_ctx->storage_manager_->array_create(&array_schema_c);

  // Return
  if(rc == TILEDB_SM_OK)
    return TILEDB_OK;
  else
    return TILEDB_ERR;
}

int tiledb_array_init(
    const TileDB_CTX* tiledb_ctx,
    TileDB_Array** tiledb_array,
    const char* dir,
    int mode,
    const void* range,
    const char** attributes,
    int attribute_num) {
  // Sanity check
  if(!sanity_check(tiledb_ctx))
    return TILEDB_ERR;

  // Allocate memory for the array struct
  *tiledb_array = (TileDB_Array*) malloc(sizeof(struct TileDB_Array));

  // Set TileDB context
  (*tiledb_array)->tiledb_ctx_ = tiledb_ctx;

  // Init the array
  int rc = tiledb_ctx->storage_manager_->array_init(
               (*tiledb_array)->array_,
               dir,
               mode, 
               range, 
               attributes,
               attribute_num);

  // Return
  if(rc == TILEDB_SM_OK) 
    return TILEDB_OK;
  else 
    return TILEDB_ERR; 
}

int tiledb_array_reinit_subarray(
    const TileDB_Array* tiledb_array,
    const void* subarray) {
  // Sanity check
  // TODO

  // Re-Init the array
  int rc = tiledb_array->tiledb_ctx_->storage_manager_->array_reinit_subarray(
               tiledb_array->array_,
               subarray);

  // Return
  if(rc == TILEDB_SM_OK) 
    return TILEDB_OK;
  else 
    return TILEDB_ERR; 
}

int tiledb_array_finalize(TileDB_Array* tiledb_array) {
  // Sanity check
  if(!sanity_check(tiledb_array->tiledb_ctx_))
    return TILEDB_ERR;

  // Finalize array
  int rc = tiledb_array->tiledb_ctx_->storage_manager_->array_finalize(
               tiledb_array->array_);

  free(tiledb_array);

  // Return
  if(rc == TILEDB_SM_OK) 
    return TILEDB_OK;
  else 
    return TILEDB_ERR; 
}

int tiledb_array_free_schema(
    TileDB_ArraySchema* tiledb_array_schema) {
  // Sanity check
  // TODO

  // Free name
  free(tiledb_array_schema->array_name_);

  // Free attributes
  for(int i=0; i<tiledb_array_schema->attribute_num_; ++i)
    free(tiledb_array_schema->attributes_[i]);
  free(tiledb_array_schema->attributes_);

  // Free dimensions
  for(int i=0; i<tiledb_array_schema->dim_num_; ++i)
    free(tiledb_array_schema->dimensions_[i]);
  free(tiledb_array_schema->dimensions_);

  // Free domain
  free(tiledb_array_schema->domain_);

  // Free tile extents
  if(tiledb_array_schema->tile_extents_ != NULL)
    free(tiledb_array_schema->tile_extents_);

  // Free types
  free(tiledb_array_schema->types_);

  // Free compression
  free(tiledb_array_schema->compression_);

  // Free cell val num
  free(tiledb_array_schema->cell_val_num_);

  // Success
  return TILEDB_OK;
}

int tiledb_array_load_schema(
    const TileDB_CTX* tiledb_ctx,
    const char* array,
    TileDB_ArraySchema* tiledb_array_schema) {
  // Sanity check
  if(!sanity_check(tiledb_ctx))
    return TILEDB_ERR;

  // Get the array schema
  ArraySchema* array_schema;
  // TODO: handle errors
  tiledb_ctx->storage_manager_->array_load_schema(array, array_schema); 
  ArraySchemaC array_schema_c;
  array_schema->array_schema_export(&array_schema_c);

  // Copy the array schema C struct to the output
  tiledb_array_schema->array_name_ = array_schema_c.array_name_;
  tiledb_array_schema->attributes_ = array_schema_c.attributes_; 
  tiledb_array_schema->attribute_num_ = array_schema_c.attribute_num_;
  tiledb_array_schema->capacity_ = array_schema_c.capacity_;
  tiledb_array_schema->cell_order_ = array_schema_c.cell_order_;
  tiledb_array_schema->cell_val_num_ = array_schema_c.cell_val_num_;
  tiledb_array_schema->compression_ = array_schema_c.compression_;
  tiledb_array_schema->dense_ = array_schema_c.dense_;
  tiledb_array_schema->dimensions_ = array_schema_c.dimensions_;
  tiledb_array_schema->dim_num_ = array_schema_c.dim_num_;
  tiledb_array_schema->domain_ = array_schema_c.domain_;
  tiledb_array_schema->tile_extents_ = array_schema_c.tile_extents_;
  tiledb_array_schema->tile_order_ = array_schema_c.tile_order_;
  tiledb_array_schema->types_ = array_schema_c.types_;

  // Clean up
  delete array_schema;

  // Success
  return TILEDB_OK;
}

int tiledb_array_get_schema(
    const TileDB_Array* tiledb_array,
    TileDB_ArraySchema* tiledb_array_schema) {
  // Sanity check
  if(!sanity_check(tiledb_array))
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
  tiledb_array_schema->compression_ = array_schema_c.compression_;
  tiledb_array_schema->dense_ = array_schema_c.dense_;
  tiledb_array_schema->dimensions_ = array_schema_c.dimensions_;
  tiledb_array_schema->dim_num_ = array_schema_c.dim_num_;
  tiledb_array_schema->domain_ = array_schema_c.domain_;
  tiledb_array_schema->tile_extents_ = array_schema_c.tile_extents_;
  tiledb_array_schema->tile_order_ = array_schema_c.tile_order_;
  tiledb_array_schema->types_ = array_schema_c.types_;

  // Success
  return TILEDB_OK;
}

int tiledb_array_set_schema(
    TileDB_ArraySchema* tiledb_array_schema,
    const char* array_name,
    const char** attributes,
    int attribute_num,
    const char** dimensions, 
    int dim_num,
    int dense,
    const void* domain,
    size_t domain_len,
    const void* tile_extents,
    size_t tile_extents_len,
    const int* types,
    const int* cell_val_num,
    int cell_order,
    int tile_order,
    int64_t capacity,
    const int* compression) {
  // set array name
  size_t array_name_len = strlen(array_name); 
  tiledb_array_schema->array_name_ = (char*) malloc(array_name_len+1);
  strcpy(tiledb_array_schema->array_name_, array_name);

  /* set attributes and number of attributes. */
  tiledb_array_schema->attribute_num_ = attribute_num;
  tiledb_array_schema->attributes_ = 
      (char**) malloc(attribute_num*sizeof(char*));
  for(int i=0; i<attribute_num; ++i) { 
    size_t attribute_len = strlen(attributes[i]);
    tiledb_array_schema->attributes_[i] = (char*) malloc(attribute_len+1);
    strcpy(tiledb_array_schema->attributes_[i], attributes[i]);
  }

  // set dimensions
  tiledb_array_schema->dim_num_ = dim_num; 
  tiledb_array_schema->dimensions_ = (char**) malloc(dim_num*sizeof(char*));
  for(int i=0; i<dim_num; ++i) { 
    size_t dimension_len = strlen(dimensions[i]);
    tiledb_array_schema->dimensions_[i] = (char*) malloc(dimension_len+1);
    strcpy(tiledb_array_schema->dimensions_[i], dimensions[i]);
  }

  // set dense
  tiledb_array_schema->dense_ = dense;

  // set domain
  tiledb_array_schema->domain_ = malloc(domain_len); 
  memcpy(tiledb_array_schema->domain_, domain, domain_len);

  // set tile extents
  if(tile_extents == NULL) {
    tiledb_array_schema->tile_extents_ = NULL;
  } else {
    tiledb_array_schema->tile_extents_ = malloc(tile_extents_len); 
    memcpy(tiledb_array_schema->tile_extents_, tile_extents, tile_extents_len);
  }

  // set types
  tiledb_array_schema->types_ = (int*) malloc((attribute_num+1)*sizeof(int));
  for(int i=0; i<attribute_num+1; ++i)
    tiledb_array_schema->types_[i] = types[i];

  // set cell val num
  if(cell_val_num == NULL) {
    tiledb_array_schema->cell_val_num_ = NULL; 
  } else {
    tiledb_array_schema->cell_val_num_ = 
        (int*) malloc((attribute_num)*sizeof(int));
    for(int i=0; i<attribute_num; ++i) {
      tiledb_array_schema->cell_val_num_[i] = cell_val_num[i];
    }
  }

  // set cell order
  tiledb_array_schema->cell_order_ = cell_order;

  // set tile order
  tiledb_array_schema->tile_order_ = tile_order;

  // set capacity
  tiledb_array_schema->capacity_ = capacity;

  // set cell val num
  if(compression == NULL) {
    tiledb_array_schema->compression_ = NULL; 
  } else {
    tiledb_array_schema->compression_ = 
        (int*) malloc((attribute_num+1)*sizeof(int));
    for(int i=0; i<attribute_num+1; ++i)
      tiledb_array_schema->compression_[i] = compression[i];
  }
}

int tiledb_array_write(
    const TileDB_Array* tiledb_array,
    const void** buffers,
    const size_t* buffer_sizes) {
  // Sanity check
  if(!sanity_check(tiledb_array->tiledb_ctx_))
    return TILEDB_ERR;

  // Write
  int rc = tiledb_array->tiledb_ctx_->storage_manager_->array_write(
               tiledb_array->array_, buffers, buffer_sizes);

  // Return
  if(rc == TILEDB_SM_OK) 
    return TILEDB_OK;
  else 
    return TILEDB_ERR;
}

int tiledb_array_read(
    const TileDB_Array* tiledb_array,
    void** buffers,
    size_t* buffer_sizes) {
  // Sanity check
  if(!sanity_check(tiledb_array->tiledb_ctx_))
    return TILEDB_ERR;

  // Read
  int rc = tiledb_array->tiledb_ctx_->storage_manager_->array_read(
               tiledb_array->array_, buffers, buffer_sizes);

  // Return
  if(rc == TILEDB_SM_OK) 
    return TILEDB_OK;
  else 
    return TILEDB_ERR;
}

/* ****************************** */
/*       CLEAR, DELETE, MOVE      */
/* ****************************** */

int tiledb_clear(
    const TileDB_CTX* tiledb_ctx,
    const char* dir) {
  // Sanity check
  if(!sanity_check(tiledb_ctx))
    return TILEDB_ERR;

  // Clear
  int rc = tiledb_ctx->storage_manager_->clear(dir);

  // Return
  if(rc == TILEDB_SM_OK)
    return TILEDB_OK;
  else
    return TILEDB_ERR;
}   

int tiledb_delete(
    const TileDB_CTX* tiledb_ctx,
    const char* dir) {
  // Sanity check
  if(!sanity_check(tiledb_ctx))
    return TILEDB_ERR;

  // Delete
  int rc = tiledb_ctx->storage_manager_->delete_entire(dir);

  // Return
  if(rc == TILEDB_SM_OK)
    return TILEDB_OK;
  else
    return TILEDB_ERR;
}

int tiledb_move(
    const TileDB_CTX* tiledb_ctx,
    const char* old_dir,
    const char* new_dir) {
  // Sanity check
  if(!sanity_check(tiledb_ctx))
    return TILEDB_ERR;

  // Move
  int rc = tiledb_ctx->storage_manager_->move(old_dir, new_dir);

  // Return
  if(rc == TILEDB_SM_OK)
    return TILEDB_OK;
  else
    return TILEDB_ERR;
}

/* ****************************** */
/*            METADATA            */
/* ****************************** */

typedef struct TileDB_Metadata {
  Metadata* metadata_;
  const TileDB_CTX* tiledb_ctx_;
} TileDB_Metadata;

int tiledb_metadata_create(
    const TileDB_CTX* tiledb_ctx,
    const TileDB_MetadataSchema* metadata_schema) {
  // Sanity check
  if(!sanity_check(tiledb_ctx))
    return TILEDB_ERR;

  // Copy metadata schema to the proper struct
  MetadataSchemaC metadata_schema_c;
  metadata_schema_c.metadata_name_ = metadata_schema->metadata_name_;
  metadata_schema_c.attributes_ = metadata_schema->attributes_;
  metadata_schema_c.attribute_num_ = metadata_schema->attribute_num_;
  metadata_schema_c.capacity_ = metadata_schema->capacity_;
  metadata_schema_c.cell_val_num_ = metadata_schema->cell_val_num_;
  metadata_schema_c.compression_ = metadata_schema->compression_;
  metadata_schema_c.types_ = metadata_schema->types_;

  // Create the metadata
  int rc = tiledb_ctx->storage_manager_->metadata_create(&metadata_schema_c);

  // Return
  if(rc == TILEDB_SM_OK)
    return TILEDB_OK;
  else
    return TILEDB_ERR;
}

int tiledb_metadata_set_schema(
    TileDB_MetadataSchema* tiledb_metadata_schema,
    const char* metadata_name,
    const char** attributes,
    int attribute_num,
    const int* types,
    const int* cell_val_num,
    int64_t capacity,
    const int* compression) {
  // set metadata name
  size_t metadata_name_len = strlen(metadata_name); 
  tiledb_metadata_schema->metadata_name_ = (char*) malloc(metadata_name_len+1);
  strcpy(tiledb_metadata_schema->metadata_name_, metadata_name);

  /* set attributes and number of attributes. */
  tiledb_metadata_schema->attribute_num_ = attribute_num;
  tiledb_metadata_schema->attributes_ = 
      (char**) malloc(attribute_num*sizeof(char*));
  for(int i=0; i<attribute_num; ++i) { 
    size_t attribute_len = strlen(attributes[i]);
    tiledb_metadata_schema->attributes_[i] = (char*) malloc(attribute_len+1);
    strcpy(tiledb_metadata_schema->attributes_[i], attributes[i]);
  }

  // set types
  tiledb_metadata_schema->types_ = (int*) malloc((attribute_num+1)*sizeof(int));
  for(int i=0; i<attribute_num+1; ++i)
    tiledb_metadata_schema->types_[i] = types[i];

  // set cell val num
  if(cell_val_num == NULL) {
    tiledb_metadata_schema->cell_val_num_ = NULL; 
  } else {
    tiledb_metadata_schema->cell_val_num_ = 
        (int*) malloc((attribute_num)*sizeof(int));
    for(int i=0; i<attribute_num; ++i) {
      tiledb_metadata_schema->cell_val_num_[i] = cell_val_num[i];
    }
  }

  // set capacity
  tiledb_metadata_schema->capacity_ = capacity;

  // set compression
  if(compression == NULL) {
    tiledb_metadata_schema->compression_ = NULL; 
  } else {
    tiledb_metadata_schema->compression_ = 
        (int*) malloc((attribute_num+1)*sizeof(int));
    for(int i=0; i<attribute_num+1; ++i)
      tiledb_metadata_schema->compression_[i] = compression[i];
  }
}

int tiledb_metadata_free_schema(
    TileDB_MetadataSchema* tiledb_metadata_schema) {
  // Sanity check
  // TODO

  // Free name
  free(tiledb_metadata_schema->metadata_name_);

  // Free attributes
  for(int i=0; i<tiledb_metadata_schema->attribute_num_; ++i)
    free(tiledb_metadata_schema->attributes_[i]);
  free(tiledb_metadata_schema->attributes_);

  // Free types
  free(tiledb_metadata_schema->types_);

  // Free compression
  free(tiledb_metadata_schema->compression_);

  // Free cell val num
  free(tiledb_metadata_schema->cell_val_num_);

  // Success
  return TILEDB_OK;
}

int tiledb_metadata_init(
    const TileDB_CTX* tiledb_ctx,
    TileDB_Metadata** tiledb_metadata,
    const char* dir,
    int mode,
    const char** attributes,
    int attribute_num) {
  // Sanity check
  // TODO

  // Allocate memory for the array struct
  *tiledb_metadata = (TileDB_Metadata*) malloc(sizeof(struct TileDB_Metadata));

  // Set TileDB context
  (*tiledb_metadata)->tiledb_ctx_ = tiledb_ctx;

  // Init the metadata
  int rc = tiledb_ctx->storage_manager_->metadata_init(
               (*tiledb_metadata)->metadata_,
               dir,
               mode, 
               attributes,
               attribute_num);

  // Return
  if(rc == TILEDB_SM_OK) 
    return TILEDB_OK;
  else 
    return TILEDB_ERR; 
}

int tiledb_metadata_finalize(TileDB_Metadata* tiledb_metadata) {
  // Sanity check
  // TODO

  // Finalize array
  int rc = tiledb_metadata->tiledb_ctx_->storage_manager_->metadata_finalize(
               tiledb_metadata->metadata_);

  free(tiledb_metadata);

  // Return
  if(rc == TILEDB_SM_OK) 
    return TILEDB_OK;
  else 
    return TILEDB_ERR; 
}

int tiledb_metadata_write(
    const TileDB_Metadata* tiledb_metadata,
    const char* keys,
    size_t keys_size,
    const void** buffers,
    const size_t* buffer_sizes) {
  // Sanity check
  // TODO

  // Write
  int rc = tiledb_metadata->tiledb_ctx_->storage_manager_->metadata_write(
               tiledb_metadata->metadata_, 
               keys, 
               keys_size, 
               buffers, 
               buffer_sizes);

  // Return
  if(rc == TILEDB_SM_OK) 
    return TILEDB_OK;
  else 
    return TILEDB_ERR;
}

int tiledb_metadata_read(
    const TileDB_Metadata* tiledb_metadata,
    const char* key,
    void** buffers,
    size_t* buffer_sizes) {
  // Sanity check
  // TODO

  // Read
  int rc = tiledb_metadata->tiledb_ctx_->storage_manager_->metadata_read(
               tiledb_metadata->metadata_, key, buffers, buffer_sizes);

  // Return
  if(rc == TILEDB_SM_OK) 
    return TILEDB_OK;
  else 
    return TILEDB_ERR;
}

int tiledb_metadata_get_schema(
    const TileDB_Metadata* tiledb_metadata,
    TileDB_MetadataSchema* tiledb_metadata_schema) {
  // Sanity check
  // TODO

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
  tiledb_metadata_schema->compression_ = metadata_schema_c.compression_;
  tiledb_metadata_schema->types_ = metadata_schema_c.types_;

  // Success
  return TILEDB_OK;
}

int tiledb_metadata_load_schema(
    const TileDB_CTX* tiledb_ctx,
    const char* metadata,
    TileDB_MetadataSchema* tiledb_metadata_schema) {
  // Sanity check
  if(!sanity_check(tiledb_ctx))
    return TILEDB_ERR;

  // Get the array schema
  ArraySchema* array_schema;
  // TODO: handle errors
  tiledb_ctx->storage_manager_->metadata_load_schema(metadata, array_schema); 
  MetadataSchemaC metadata_schema_c;
  array_schema->array_schema_export(&metadata_schema_c);
  array_schema->print();

  // Copy the metadata schema C struct to the output
  tiledb_metadata_schema->metadata_name_ = metadata_schema_c.metadata_name_;
  tiledb_metadata_schema->attributes_ = metadata_schema_c.attributes_; 
  tiledb_metadata_schema->attribute_num_ = metadata_schema_c.attribute_num_;
  tiledb_metadata_schema->capacity_ = metadata_schema_c.capacity_;
  tiledb_metadata_schema->cell_val_num_ = metadata_schema_c.cell_val_num_;
  tiledb_metadata_schema->compression_ = metadata_schema_c.compression_;
  tiledb_metadata_schema->types_ = metadata_schema_c.types_;

  // Clean up
  delete array_schema;

  // Success
  return TILEDB_OK;
}

/* ****************************** */
/*            ITERATORS           */
/* ****************************** */

typedef struct TileDB_ArrayIterator {
  ArrayIterator* array_it_;
  const TileDB_CTX* tiledb_ctx_;
} TileDB_ArrayIterator;

int tiledb_array_iterator_init(
    const TileDB_CTX* tiledb_ctx,
    TileDB_ArrayIterator** tiledb_array_iterator,
    const char* dir,
    const void* range,
    const char** attributes,
    int attribute_num,
    void** buffers,
    size_t* buffer_sizes) {
  // Sanity check
  // TODO

  // Allocate memory for the array struct
  *tiledb_array_iterator = 
      (TileDB_ArrayIterator*) malloc(sizeof(struct TileDB_ArrayIterator));

  // Set TileDB context
  (*tiledb_array_iterator)->tiledb_ctx_ = tiledb_ctx;

  // Init the array
  int rc = tiledb_ctx->storage_manager_->array_iterator_init(
               (*tiledb_array_iterator)->array_it_,
               dir,
               range, 
               attributes,
               attribute_num,
               buffers,
               buffer_sizes);

  // Return
  if(rc == TILEDB_SM_OK) 
    return TILEDB_OK;
  else 
    return TILEDB_ERR; 
}

int tiledb_array_iterator_finalize(
    TileDB_ArrayIterator* tiledb_array_iterator) {
  // Sanity check
  // TODO

  // Finalize array
  int rc = tiledb_array_iterator->tiledb_ctx_->
               storage_manager_->array_iterator_finalize(
                   tiledb_array_iterator->array_it_);

  free(tiledb_array_iterator);

  // Return
  if(rc == TILEDB_SM_OK) 
    return TILEDB_OK;
  else 
    return TILEDB_ERR; 
}

int tiledb_array_iterator_end(
    TileDB_ArrayIterator* tiledb_array_iterator) {
  // Sanity check
  // TODO

  return (int) tiledb_array_iterator->array_it_->end();
}

int tiledb_array_iterator_get_value(
    TileDB_ArrayIterator* tiledb_array_iterator,
    int attribute_id,
    const void** value,
    size_t* value_size) {
  // Sanity check
  // TODO

  if(tiledb_array_iterator->array_it_->get_value(
          attribute_id, 
          value, 
          value_size) != TILEDB_AIT_OK)
    return TILEDB_ERR;
  else
    return TILEDB_OK;
}

int tiledb_array_iterator_next(
    TileDB_ArrayIterator* tiledb_array_iterator) {
  // Sanity check
  // TODO

  if(tiledb_array_iterator->array_it_->next() != TILEDB_AIT_OK)
    return TILEDB_ERR;
  else
    return TILEDB_OK;
}

typedef struct TileDB_MetadataIterator {
  MetadataIterator* metadata_it_;
  const TileDB_CTX* tiledb_ctx_;
} TileDB_MetadataIterator;

int tiledb_metadata_iterator_init(
    const TileDB_CTX* tiledb_ctx,
    TileDB_MetadataIterator** tiledb_metadata_iterator,
    const char* dir,
    const char** attributes,
    int attribute_num,
    void** buffers,
    size_t* buffer_sizes) {
  // Sanity check
  // TODO

  // Allocate memory for the array struct
  *tiledb_metadata_iterator = 
      (TileDB_MetadataIterator*) malloc(sizeof(struct TileDB_MetadataIterator));

  // Set TileDB context
  (*tiledb_metadata_iterator)->tiledb_ctx_ = tiledb_ctx;

  // Init the array
  int rc = tiledb_ctx->storage_manager_->metadata_iterator_init(
               (*tiledb_metadata_iterator)->metadata_it_,
               dir,
               attributes,
               attribute_num,
               buffers,
               buffer_sizes);

  // Return
  if(rc == TILEDB_SM_OK) 
    return TILEDB_OK;
  else 
    return TILEDB_ERR; 
}

int tiledb_metadata_iterator_finalize(
    TileDB_MetadataIterator* tiledb_metadata_iterator) {
  // Sanity check
  // TODO

  // Finalize array
  int rc = tiledb_metadata_iterator->tiledb_ctx_->
               storage_manager_->metadata_iterator_finalize(
                   tiledb_metadata_iterator->metadata_it_);

  free(tiledb_metadata_iterator);

  // Return
  if(rc == TILEDB_SM_OK) 
    return TILEDB_OK;
  else 
    return TILEDB_ERR; 
}

int tiledb_metadata_iterator_end(
    TileDB_MetadataIterator* tiledb_metadata_iterator) {
  // Sanity check
  // TODO

  return (int) tiledb_metadata_iterator->metadata_it_->end();
}

int tiledb_metadata_iterator_get_value(
    TileDB_MetadataIterator* tiledb_metadata_iterator,
    int attribute_id,
    const void** value,
    size_t* value_size) {
  // Sanity check
  // TODO

  if(tiledb_metadata_iterator->metadata_it_->get_value(
          attribute_id, 
          value, 
          value_size) != TILEDB_MIT_OK)
    return TILEDB_ERR;
  else
    return TILEDB_OK;
}

int tiledb_metadata_iterator_next(
    TileDB_MetadataIterator* tiledb_metadata_iterator) {
  // Sanity check
  // TODO

  if(tiledb_metadata_iterator->metadata_it_->next() != TILEDB_MIT_OK)
    return TILEDB_ERR;
  else
    return TILEDB_OK;
}
