/**
 * @file   c_api.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2016 MIT and Intel Corp.
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

typedef struct TileDB_CTX {
  StorageManager* storage_manager_;
} TileDB_CTX;

int tiledb_ctx_init(TileDB_CTX** tiledb_ctx, const char* config_filename) {
  // Check config filename length
  if(config_filename != NULL) {
    if(strlen(config_filename) > TILEDB_NAME_MAX_LEN) {
      PRINT_ERROR("Invalid filename length");
      return TILEDB_ERR;
    }
  }

  // Initialize context
  *tiledb_ctx = (TileDB_CTX*) malloc(sizeof(struct TileDB_CTX));
  if(*tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot initialize TileDB context; Failed to allocate memory "
                "space for the context");
    return TILEDB_ERR;
  }

  // Create storage manager
  (*tiledb_ctx)->storage_manager_ = new StorageManager();
  if((*tiledb_ctx)->storage_manager_->init(config_filename) != TILEDB_SM_OK)
    return TILEDB_ERR;
  else
    return TILEDB_OK;
}

int tiledb_ctx_finalize(TileDB_CTX* tiledb_ctx) {
  // Trivial case
  if(tiledb_ctx == NULL)
    return TILEDB_OK;

  // Finalize storage manager
  int rc = TILEDB_OK;
  if(tiledb_ctx->storage_manager_ != NULL)
    rc = tiledb_ctx->storage_manager_->finalize();

  // Clean up
  delete tiledb_ctx->storage_manager_;
  free(tiledb_ctx);

  // Return
  if(rc == TILEDB_SM_OK)
    return TILEDB_OK;
  else
    return TILEDB_ERR;
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

bool sanity_check(const TileDB_ArrayIterator* tiledb_array_it) {
  if(tiledb_array_it == NULL) {
    PRINT_ERROR("Invalid TileDB array iterator");
    return false;
  } else {
    return true;
  }
}

bool sanity_check(const TileDB_Metadata* tiledb_metadata) {
  if(tiledb_metadata == NULL) {
    PRINT_ERROR("Invalid TileDB metadata");
    return false;
  } else {
    return true;
  }
}

bool sanity_check(const TileDB_MetadataIterator* tiledb_metadata_it) {
  if(tiledb_metadata_it == NULL) {
    PRINT_ERROR("Invalid TileDB metadata iterator");
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
    const char* workspace) {
  // Sanity check
  if(!sanity_check(tiledb_ctx))
    return TILEDB_ERR;

  // Check workspace name length
  if(workspace == NULL || strlen(workspace) > TILEDB_NAME_MAX_LEN) {
    PRINT_ERROR("Invalid workspace name length");
    return TILEDB_ERR;
  }

  // Create the workspace
  if(tiledb_ctx->storage_manager_->workspace_create(workspace) != TILEDB_SM_OK)
    return TILEDB_ERR;
  else
    return TILEDB_OK;
}




/* ****************************** */
/*              GROUP             */
/* ****************************** */

int tiledb_group_create(
    const TileDB_CTX* tiledb_ctx,
    const char* group) {
  // Sanity check
  if(!sanity_check(tiledb_ctx))
    return TILEDB_ERR;

  // Check group name length
  if(group == NULL || strlen(group) > TILEDB_NAME_MAX_LEN) {
    PRINT_ERROR("Invalid group name length");
    return TILEDB_ERR;
  }

  // Create the group
  if(tiledb_ctx->storage_manager_->group_create(group) != TILEDB_SM_OK)
    return TILEDB_ERR;
  else
    return TILEDB_OK;
}




/* ****************************** */
/*              ARRAY             */
/* ****************************** */

typedef struct TileDB_Array {
  Array* array_;
  const TileDB_CTX* tiledb_ctx_;
} TileDB_Array;

int tiledb_array_set_schema(
    TileDB_ArraySchema* tiledb_array_schema,
    const char* array_name,
    const char** attributes,
    int attribute_num,
    int64_t capacity,
    int cell_order,
    const int* cell_val_num,
    const int* compression,
    int dense,
    const char** dimensions, 
    int dim_num,
    const void* domain,
    size_t domain_len,
    const void* tile_extents,
    size_t tile_extents_len,
    int tile_order,
    const int* types) {
  // Sanity check
  if(tiledb_array_schema == NULL) {
    PRINT_ERROR("Invalid array schema pointer");
    return TILEDB_ERR;
  }

  // Set array name
  size_t array_name_len = strlen(array_name); 
  if(array_name == NULL || array_name_len > TILEDB_NAME_MAX_LEN) {
    PRINT_ERROR("Invalid array name length");
    return TILEDB_ERR;
  }
  tiledb_array_schema->array_name_ = (char*) malloc(array_name_len+1);
  strcpy(tiledb_array_schema->array_name_, array_name);

  // Set attributes and number of attributes
  tiledb_array_schema->attribute_num_ = attribute_num;
  tiledb_array_schema->attributes_ = 
      (char**) malloc(attribute_num*sizeof(char*));
  for(int i=0; i<attribute_num; ++i) { 
    size_t attribute_len = strlen(attributes[i]);
    if(attributes[i] == NULL || attribute_len > TILEDB_NAME_MAX_LEN) {
      PRINT_ERROR("Invalid attribute name length");
      return TILEDB_ERR;
    }
    tiledb_array_schema->attributes_[i] = (char*) malloc(attribute_len+1);
    strcpy(tiledb_array_schema->attributes_[i], attributes[i]);
  }

  // Set dimensions
  tiledb_array_schema->dim_num_ = dim_num; 
  tiledb_array_schema->dimensions_ = (char**) malloc(dim_num*sizeof(char*));
  for(int i=0; i<dim_num; ++i) { 
    size_t dimension_len = strlen(dimensions[i]);
    if(dimensions[i] == NULL || dimension_len > TILEDB_NAME_MAX_LEN) {
      PRINT_ERROR("Invalid attribute name length");
      return TILEDB_ERR;
    }
    tiledb_array_schema->dimensions_[i] = (char*) malloc(dimension_len+1);
    strcpy(tiledb_array_schema->dimensions_[i], dimensions[i]);
  }

  // Set dense
  tiledb_array_schema->dense_ = dense;

  // Set domain
  tiledb_array_schema->domain_ = malloc(domain_len); 
  memcpy(tiledb_array_schema->domain_, domain, domain_len);

  // Set tile extents
  if(tile_extents == NULL) {
    tiledb_array_schema->tile_extents_ = NULL;
  } else {
    tiledb_array_schema->tile_extents_ = malloc(tile_extents_len); 
    memcpy(tiledb_array_schema->tile_extents_, tile_extents, tile_extents_len);
  }

  // Set types
  tiledb_array_schema->types_ = (int*) malloc((attribute_num+1)*sizeof(int));
  for(int i=0; i<attribute_num+1; ++i)
    tiledb_array_schema->types_[i] = types[i];

  // Set cell val num
  if(cell_val_num == NULL) {
    tiledb_array_schema->cell_val_num_ = NULL; 
  } else {
    tiledb_array_schema->cell_val_num_ = 
        (int*) malloc((attribute_num)*sizeof(int));
    for(int i=0; i<attribute_num; ++i) {
      tiledb_array_schema->cell_val_num_[i] = cell_val_num[i];
    }
  }

  // Set cell and tile order
  tiledb_array_schema->cell_order_ = cell_order;
  tiledb_array_schema->tile_order_ = tile_order;

  // Set capacity
  tiledb_array_schema->capacity_ = capacity;

  // Set compression
  if(compression == NULL) {
    tiledb_array_schema->compression_ = NULL; 
  } else {
    tiledb_array_schema->compression_ = 
        (int*) malloc((attribute_num+1)*sizeof(int));
    for(int i=0; i<attribute_num+1; ++i)
      tiledb_array_schema->compression_[i] = compression[i];
  }

  // Success
  return TILEDB_OK;
}

int tiledb_array_create(
    const TileDB_CTX* tiledb_ctx,
    const TileDB_ArraySchema* array_schema) {
  // Sanity check
  if(!sanity_check(tiledb_ctx))
    return TILEDB_ERR;

  // Copy array schema to a C struct
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
  if(tiledb_ctx->storage_manager_->array_create(&array_schema_c) != 
     TILEDB_SM_OK)
    return TILEDB_ERR;
  else
    return TILEDB_OK;
}

int tiledb_array_init(
    const TileDB_CTX* tiledb_ctx,
    TileDB_Array** tiledb_array,
    const char* array,
    int mode,
    const void* subarray,
    const char** attributes,
    int attribute_num) {
  // Sanity check
  if(!sanity_check(tiledb_ctx))
    return TILEDB_ERR;

  // Check array name length
  if(array == NULL || strlen(array) > TILEDB_NAME_MAX_LEN) {
    PRINT_ERROR("Invalid array name length");
    return TILEDB_ERR;
  }

  // Allocate memory for the array struct
  *tiledb_array = (TileDB_Array*) malloc(sizeof(struct TileDB_Array));

  // Set TileDB context
  (*tiledb_array)->tiledb_ctx_ = tiledb_ctx;

  // Init the array
  int rc = tiledb_ctx->storage_manager_->array_init(
               (*tiledb_array)->array_,
               array,
               mode, 
               subarray, 
               attributes,
               attribute_num);

  // Return
  if(rc != TILEDB_SM_OK) {
    free(*tiledb_array);
    return TILEDB_ERR; 
  } else {
    return TILEDB_OK;
  }
}

int tiledb_array_reset_subarray(
    const TileDB_Array* tiledb_array,
    const void* subarray) {
  // Sanity check
  if(!sanity_check(tiledb_array))
    return TILEDB_ERR;

  // Reset subarray
  if(tiledb_array->array_->reset_subarray(subarray) != TILEDB_AR_OK)
    return TILEDB_ERR; 
  else 
    return TILEDB_OK;
}

int tiledb_array_reset_attributes(
    const TileDB_Array* tiledb_array,
    const char** attributes, 
    int attribute_num) {
  // Sanity check
  if(!sanity_check(tiledb_array))
    return TILEDB_ERR;

  // Re-Init the array
  if(tiledb_array->array_->reset_attributes(attributes, attribute_num) !=
     TILEDB_AR_OK)
    return TILEDB_ERR; 
  else 
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

int tiledb_array_load_schema(
    const TileDB_CTX* tiledb_ctx,
    const char* array,
    TileDB_ArraySchema* tiledb_array_schema) {
  // Sanity check
  if(!sanity_check(tiledb_ctx))
    return TILEDB_ERR;

  // Check array name length
  if(array == NULL || strlen(array) > TILEDB_NAME_MAX_LEN) {
    PRINT_ERROR("Invalid array name length");
    return TILEDB_ERR;
  }

  // Get the array schema
  ArraySchema* array_schema;
  if(tiledb_ctx->storage_manager_->array_load_schema(array, array_schema) !=
     TILEDB_SM_OK)
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

int tiledb_array_free_schema(
    TileDB_ArraySchema* tiledb_array_schema) {
  // Trivial case
  if(tiledb_array_schema == NULL)
    return TILEDB_OK;

  // Free array name
  if(tiledb_array_schema->array_name_ != NULL)
    free(tiledb_array_schema->array_name_);

  // Free attributes
  if(tiledb_array_schema->attributes_ != NULL) {
    for(int i=0; i<tiledb_array_schema->attribute_num_; ++i)
      if(tiledb_array_schema->attributes_[i] != NULL)
        free(tiledb_array_schema->attributes_[i]);
    free(tiledb_array_schema->attributes_);
  }

  // Free dimensions
  if(tiledb_array_schema->dimensions_ != NULL) {
    for(int i=0; i<tiledb_array_schema->dim_num_; ++i)
      if(tiledb_array_schema->dimensions_[i] != NULL)
        free(tiledb_array_schema->dimensions_[i]);
    free(tiledb_array_schema->dimensions_);
  }

  // Free domain
  if(tiledb_array_schema->domain_ != NULL)
    free(tiledb_array_schema->domain_);

  // Free tile extents
  if(tiledb_array_schema->tile_extents_ != NULL)
    free(tiledb_array_schema->tile_extents_);

  // Free types
  if(tiledb_array_schema->types_ != NULL)
    free(tiledb_array_schema->types_);

  // Free compression
  if(tiledb_array_schema->compression_ != NULL)
    free(tiledb_array_schema->compression_);

  // Free cell val num
  if(tiledb_array_schema->cell_val_num_ != NULL);
    free(tiledb_array_schema->cell_val_num_);

  // Success
  return TILEDB_OK;
}

int tiledb_array_write(
    const TileDB_Array* tiledb_array,
    const void** buffers,
    const size_t* buffer_sizes) {
  // Sanity check
  if(!sanity_check(tiledb_array))
    return TILEDB_ERR;

  // Write
  if(tiledb_array->array_->write(buffers, buffer_sizes) != TILEDB_AR_OK)
    return TILEDB_ERR;
  else 
    return TILEDB_OK;
}

int tiledb_array_read(
    const TileDB_Array* tiledb_array,
    void** buffers,
    size_t* buffer_sizes) {
  // Sanity check
  if(!sanity_check(tiledb_array))
    return TILEDB_ERR;

  // Read
  if(tiledb_array->array_->read(buffers, buffer_sizes) != TILEDB_AR_OK)
    return TILEDB_ERR;
  else 
    return TILEDB_OK;
}

int tiledb_array_overflow(
    const TileDB_Array* tiledb_array,
    int attribute_id) {
  // Sanity check
  if(!sanity_check(tiledb_array))
    return TILEDB_ERR;

  // Check overflow
  return (int) tiledb_array->array_->overflow(attribute_id);
}

int tiledb_array_consolidate(
    const TileDB_CTX* tiledb_ctx,
    const char* array) {
  // Check array name length
  if(array == NULL || strlen(array) > TILEDB_NAME_MAX_LEN) {
    PRINT_ERROR("Invalid array name length");
    return TILEDB_ERR;
  }

  // Consolidate
  if(tiledb_ctx->storage_manager_->array_consolidate(array) != TILEDB_SM_OK)
    return TILEDB_ERR;
  else 
    return TILEDB_OK;
}

int tiledb_array_finalize(TileDB_Array* tiledb_array) {
  // Sanity check
  if(!sanity_check(tiledb_array) ||
     !sanity_check(tiledb_array->tiledb_ctx_))
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

typedef struct TileDB_ArrayIterator {
  ArrayIterator* array_it_;
  const TileDB_CTX* tiledb_ctx_;
} TileDB_ArrayIterator;

int tiledb_array_iterator_init(
    const TileDB_CTX* tiledb_ctx,
    TileDB_ArrayIterator** tiledb_array_it,
    const char* array,
    const void* subarray,
    const char** attributes,
    int attribute_num,
    void** buffers,
    size_t* buffer_sizes) {
  // Sanity check
  if(!sanity_check(tiledb_ctx))
    return TILEDB_ERR;

  // Allocate memory for the array iterator struct
  *tiledb_array_it = 
      (TileDB_ArrayIterator*) malloc(sizeof(struct TileDB_ArrayIterator));

  // Set TileDB context
  (*tiledb_array_it)->tiledb_ctx_ = tiledb_ctx;

  // Initialize the array iterator
  int rc = tiledb_ctx->storage_manager_->array_iterator_init(
               (*tiledb_array_it)->array_it_,
               array,
               subarray, 
               attributes,
               attribute_num,
               buffers,
               buffer_sizes);

  // Return
  if(rc == TILEDB_SM_OK) {
    return TILEDB_OK;
  } else {  
    free(*tiledb_array_it);
    return TILEDB_ERR; 
  }
}

int tiledb_array_iterator_get_value(
    TileDB_ArrayIterator* tiledb_array_it,
    int attribute_id,
    const void** value,
    size_t* value_size) {
  // Sanity check
  if(!sanity_check(tiledb_array_it))
    return TILEDB_ERR;

  // Get value
  if(tiledb_array_it->array_it_->get_value(
          attribute_id, 
          value, 
          value_size) != TILEDB_AIT_OK)
    return TILEDB_ERR;
  else
    return TILEDB_OK;
}

int tiledb_array_iterator_next(
    TileDB_ArrayIterator* tiledb_array_it) {
  // Sanity check
  if(!sanity_check(tiledb_array_it))
    return TILEDB_ERR;

  // Advance iterator
  if(tiledb_array_it->array_it_->next() != TILEDB_AIT_OK)
    return TILEDB_ERR;
  else
    return TILEDB_OK;
}

int tiledb_array_iterator_end(
    TileDB_ArrayIterator* tiledb_array_it) {
  // Sanity check
  if(!sanity_check(tiledb_array_it))
    return TILEDB_ERR;

  // Check if the iterator reached the end
  return (int) tiledb_array_it->array_it_->end();
}

int tiledb_array_iterator_finalize(
    TileDB_ArrayIterator* tiledb_array_it) {
  // Sanity check
  if(!sanity_check(tiledb_array_it))
    return TILEDB_ERR;

  // Finalize array
  int rc = tiledb_array_it->tiledb_ctx_->
               storage_manager_->array_iterator_finalize(
                   tiledb_array_it->array_it_);

  free(tiledb_array_it);

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

int tiledb_metadata_set_schema(
    TileDB_MetadataSchema* tiledb_metadata_schema,
    const char* metadata_name,
    const char** attributes,
    int attribute_num,
    int64_t capacity,
    const int* cell_val_num,
    const int* compression,
    const int* types) {
  // Sanity check
  if(tiledb_metadata_schema == NULL) {
    PRINT_ERROR("Invalid metadata schema pointer");
    return TILEDB_ERR;
  }

  // Set metadata name
  size_t metadata_name_len = strlen(metadata_name); 
  if(metadata_name == NULL || metadata_name_len > TILEDB_NAME_MAX_LEN) {
    PRINT_ERROR("Invalid metadata name length");
    return TILEDB_ERR;
  }
  tiledb_metadata_schema->metadata_name_ = (char*) malloc(metadata_name_len+1);
  strcpy(tiledb_metadata_schema->metadata_name_, metadata_name);

  /* Set attributes and number of attributes. */
  tiledb_metadata_schema->attribute_num_ = attribute_num;
  tiledb_metadata_schema->attributes_ = 
      (char**) malloc(attribute_num*sizeof(char*));
  for(int i=0; i<attribute_num; ++i) { 
    size_t attribute_len = strlen(attributes[i]);
    if(attributes[i] == NULL || attribute_len > TILEDB_NAME_MAX_LEN) {
      PRINT_ERROR("Invalid attribute name length");
      return TILEDB_ERR;
    }
    tiledb_metadata_schema->attributes_[i] = (char*) malloc(attribute_len+1);
    strcpy(tiledb_metadata_schema->attributes_[i], attributes[i]);
  }

  // Set types
  tiledb_metadata_schema->types_ = (int*) malloc((attribute_num+1)*sizeof(int));
  for(int i=0; i<attribute_num+1; ++i)
    tiledb_metadata_schema->types_[i] = types[i];

  // Set cell val num
  if(cell_val_num == NULL) {
    tiledb_metadata_schema->cell_val_num_ = NULL; 
  } else {
    tiledb_metadata_schema->cell_val_num_ = 
        (int*) malloc((attribute_num)*sizeof(int));
    for(int i=0; i<attribute_num; ++i) {
      tiledb_metadata_schema->cell_val_num_[i] = cell_val_num[i];
    }
  }

  // Set capacity
  tiledb_metadata_schema->capacity_ = capacity;

  // Set compression
  if(compression == NULL) {
    tiledb_metadata_schema->compression_ = NULL; 
  } else {
    tiledb_metadata_schema->compression_ = 
        (int*) malloc((attribute_num+1)*sizeof(int));
    for(int i=0; i<attribute_num+1; ++i)
      tiledb_metadata_schema->compression_[i] = compression[i];
  }

  // Return
  return TILEDB_OK;
}

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
  if(tiledb_ctx->storage_manager_->metadata_create(&metadata_schema_c) !=
     TILEDB_SM_OK)
    return TILEDB_ERR;
  else
    return TILEDB_OK;
}

int tiledb_metadata_init(
    const TileDB_CTX* tiledb_ctx,
    TileDB_Metadata** tiledb_metadata,
    const char* metadata,
    int mode,
    const char** attributes,
    int attribute_num) {
  // Sanity check
  if(!sanity_check(tiledb_ctx))
    return TILEDB_ERR;

  // Allocate memory for the array struct
  *tiledb_metadata = (TileDB_Metadata*) malloc(sizeof(struct TileDB_Metadata));

  // Set TileDB context
  (*tiledb_metadata)->tiledb_ctx_ = tiledb_ctx;

  // Init the metadata
  if(tiledb_ctx->storage_manager_->metadata_init(
         (*tiledb_metadata)->metadata_,
         metadata,
         mode, 
         attributes,
         attribute_num) != TILEDB_SM_OK) {
    free(*tiledb_metadata);
    return TILEDB_ERR; 
  } else {
    return TILEDB_OK;
  }
}

int tiledb_metadata_reset_attributes(
    const TileDB_Metadata* tiledb_metadata,
    const char** attributes, 
    int attribute_num) {
  // Sanity check
  if(!sanity_check(tiledb_metadata))
    return TILEDB_ERR;  

  // Reset attributes
  if(tiledb_metadata->metadata_->reset_attributes(
               attributes, 
               attribute_num) != TILEDB_MT_OK)
    return TILEDB_ERR;
  else
    return TILEDB_OK;
}

int tiledb_metadata_get_schema(
    const TileDB_Metadata* tiledb_metadata,
    TileDB_MetadataSchema* tiledb_metadata_schema) {
  // Sanity check
  if(!sanity_check(tiledb_metadata))
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

  // Check metadata name length
  if(metadata == NULL || strlen(metadata) > TILEDB_NAME_MAX_LEN) {
    PRINT_ERROR("Invalid metadata name length");
    return TILEDB_ERR;
  }

  // Get the array schema
  ArraySchema* array_schema;
  if(tiledb_ctx->storage_manager_->metadata_load_schema(
         metadata, 
         array_schema) != TILEDB_SM_OK)
    return TILEDB_ERR;
  MetadataSchemaC metadata_schema_c;
  array_schema->array_schema_export(&metadata_schema_c);

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

int tiledb_metadata_free_schema(
    TileDB_MetadataSchema* tiledb_metadata_schema) {
  // Trivial case
  if(tiledb_metadata_schema == NULL)
    return TILEDB_OK;

  // Free name
  if(tiledb_metadata_schema->metadata_name_ != NULL)
    free(tiledb_metadata_schema->metadata_name_);

  // Free attributes
  if(tiledb_metadata_schema->attributes_ != NULL) {
    for(int i=0; i<tiledb_metadata_schema->attribute_num_; ++i)
      if(tiledb_metadata_schema->attributes_[i] != NULL)
        free(tiledb_metadata_schema->attributes_[i]);
    free(tiledb_metadata_schema->attributes_);
  }

  // Free types
  if(tiledb_metadata_schema->types_ != NULL)
    free(tiledb_metadata_schema->types_);

  // Free compression
  if(tiledb_metadata_schema->compression_ != NULL)
    free(tiledb_metadata_schema->compression_);

  // Free cell val num
  if(tiledb_metadata_schema->cell_val_num_ != NULL)
    free(tiledb_metadata_schema->cell_val_num_);

  // Success
  return TILEDB_OK;
}

int tiledb_metadata_write(
    const TileDB_Metadata* tiledb_metadata,
    const char* keys,
    size_t keys_size,
    const void** buffers,
    const size_t* buffer_sizes) {
  // Sanity check
  if(!sanity_check(tiledb_metadata))
    return TILEDB_ERR;

  // Write
  if(tiledb_metadata->metadata_->write(
         keys, 
         keys_size, 
         buffers, 
         buffer_sizes) != TILEDB_MT_OK)
    return TILEDB_ERR;
  else 
    return TILEDB_OK;
}

int tiledb_metadata_read(
    const TileDB_Metadata* tiledb_metadata,
    const char* key,
    void** buffers,
    size_t* buffer_sizes) {
  // Sanity check
  if(!sanity_check(tiledb_metadata))
    return TILEDB_ERR;

  // Read
  if(tiledb_metadata->metadata_->read(
         key,
         buffers, 
         buffer_sizes) != TILEDB_MT_OK)
    return TILEDB_ERR;
  else 
    return TILEDB_OK;
}

int tiledb_metadata_overflow(
    const TileDB_Metadata* tiledb_metadata,
    int attribute_id) {
  // Sanity check
  if(!sanity_check(tiledb_metadata))
    return TILEDB_ERR; 

  return (int) tiledb_metadata->metadata_->overflow(attribute_id);
}

int tiledb_metadata_consolidate(
    const TileDB_CTX* tiledb_ctx,
    const char* metadata) {
   // Check metadata name length
   if(metadata == NULL || strlen(metadata) > TILEDB_NAME_MAX_LEN) {
     PRINT_ERROR("Invalid metadata name length");
     return TILEDB_ERR;
   }

  // Consolidate
  if(tiledb_ctx->storage_manager_->metadata_consolidate(metadata) != 
     TILEDB_SM_OK)
    return TILEDB_ERR;
  else 
    return TILEDB_OK;
}

int tiledb_metadata_finalize(TileDB_Metadata* tiledb_metadata) {
  // Sanity check
  if(!sanity_check(tiledb_metadata))
    return TILEDB_ERR; 

  // Finalize metadata
  int rc = tiledb_metadata->tiledb_ctx_->storage_manager_->metadata_finalize(
               tiledb_metadata->metadata_);

  free(tiledb_metadata);

  // Return
  if(rc == TILEDB_SM_OK) 
    return TILEDB_OK;
  else 
    return TILEDB_ERR; 
}

typedef struct TileDB_MetadataIterator {
  MetadataIterator* metadata_it_;
  const TileDB_CTX* tiledb_ctx_;
} TileDB_MetadataIterator;

int tiledb_metadata_iterator_init(
    const TileDB_CTX* tiledb_ctx,
    TileDB_MetadataIterator** tiledb_metadata_it,
    const char* metadata,
    const char** attributes,
    int attribute_num,
    void** buffers,
    size_t* buffer_sizes) {
  // Sanity check
  if(!sanity_check(tiledb_ctx))
    return TILEDB_ERR;

  // Allocate memory for the metadata struct
  *tiledb_metadata_it = 
      (TileDB_MetadataIterator*) malloc(sizeof(struct TileDB_MetadataIterator));

  // Set TileDB context
  (*tiledb_metadata_it)->tiledb_ctx_ = tiledb_ctx;

  // Initialize the metadata iterator
  if(tiledb_ctx->storage_manager_->metadata_iterator_init(
         (*tiledb_metadata_it)->metadata_it_,
         metadata,
         attributes,
         attribute_num,
         buffers,
         buffer_sizes) != TILEDB_SM_OK) {
    free(*tiledb_metadata_it);
    return TILEDB_ERR; 
  } else {
    return TILEDB_OK;
  }
}

int tiledb_metadata_iterator_get_value(
    TileDB_MetadataIterator* tiledb_metadata_it,
    int attribute_id,
    const void** value,
    size_t* value_size) {
  // Sanity check
  if(!sanity_check(tiledb_metadata_it))
    return TILEDB_ERR; 

  // Get value
  if(tiledb_metadata_it->metadata_it_->get_value(
          attribute_id, 
          value, 
          value_size) != TILEDB_MIT_OK)
    return TILEDB_ERR;
  else
    return TILEDB_OK;
}

int tiledb_metadata_iterator_next(
    TileDB_MetadataIterator* tiledb_metadata_it) {
  // Sanity check
  if(!sanity_check(tiledb_metadata_it))
    return TILEDB_ERR; 

  // Advance metadata iterator
  if(tiledb_metadata_it->metadata_it_->next() != TILEDB_MIT_OK)
    return TILEDB_ERR;
  else
    return TILEDB_OK;
}

int tiledb_metadata_iterator_end(
    TileDB_MetadataIterator* tiledb_metadata_it) {
  // Sanity check
  if(!sanity_check(tiledb_metadata_it))
    return TILEDB_ERR; 

  // Check if the metadata iterator reached its end
  return (int) tiledb_metadata_it->metadata_it_->end();
}

int tiledb_metadata_iterator_finalize(
    TileDB_MetadataIterator* tiledb_metadata_it) {
  // Sanity check
  if(!sanity_check(tiledb_metadata_it))
    return TILEDB_ERR; 

  // Finalize metadata iterator
  int rc = tiledb_metadata_it->tiledb_ctx_->
               storage_manager_->metadata_iterator_finalize(
                   tiledb_metadata_it->metadata_it_);

  free(tiledb_metadata_it);

  // Return
  if(rc == TILEDB_SM_OK) 
    return TILEDB_OK;
  else 
    return TILEDB_ERR; 
}




/* ****************************** */
/*       DIRECTORY MANAGEMENT     */
/* ****************************** */

int tiledb_clear(
    const TileDB_CTX* tiledb_ctx,
    const char* dir) {
  // Sanity check
  if(!sanity_check(tiledb_ctx))
    return TILEDB_ERR;

  // Check directory name length
  if(dir == NULL || strlen(dir) > TILEDB_NAME_MAX_LEN) {
    PRINT_ERROR("Invalid directory name length");
    return TILEDB_ERR;
  }

  // Clear
  if(tiledb_ctx->storage_manager_->clear(dir) != TILEDB_SM_OK)
    return TILEDB_ERR;
  else
    return TILEDB_OK;
}   

int tiledb_delete(
    const TileDB_CTX* tiledb_ctx,
    const char* dir) {
  // Sanity check
  if(!sanity_check(tiledb_ctx))
    return TILEDB_ERR;

  // Check directory name length
  if(dir == NULL || strlen(dir) > TILEDB_NAME_MAX_LEN) {
    PRINT_ERROR("Invalid directory name length");
    return TILEDB_ERR;
  }

  // Delete
  if(tiledb_ctx->storage_manager_->delete_entire(dir) != TILEDB_SM_OK)
    return TILEDB_ERR;
  else
    return TILEDB_OK;
}

int tiledb_move(
    const TileDB_CTX* tiledb_ctx,
    const char* old_dir,
    const char* new_dir) {
  // Sanity check
  if(!sanity_check(tiledb_ctx))
    return TILEDB_ERR;

  // Check old directory name length
  if(old_dir == NULL || strlen(old_dir) > TILEDB_NAME_MAX_LEN) {
    PRINT_ERROR("Invalid old directory name length");
    return TILEDB_ERR;
  }

  // Check new directory name length
  if(new_dir == NULL || strlen(new_dir) > TILEDB_NAME_MAX_LEN) {
    PRINT_ERROR("Invalid new directory name length");
    return TILEDB_ERR;
  }

  // Move
  if(tiledb_ctx->storage_manager_->move(old_dir, new_dir) != TILEDB_SM_OK)
    return TILEDB_ERR;
  else
    return TILEDB_OK;
}

int tiledb_ls_workspaces(
    const TileDB_CTX* tiledb_ctx,
    char** workspaces,
    int* workspace_num) {
  // Sanity check
  if(!sanity_check(tiledb_ctx))
    return TILEDB_ERR;

  // List workspaces
  if(tiledb_ctx->storage_manager_->ls_workspaces(
               workspaces,
               *workspace_num) != TILEDB_SM_OK)
    return TILEDB_ERR;
  else 
    return TILEDB_OK;
}

int tiledb_ls(
    const TileDB_CTX* tiledb_ctx,
    const char* parent_dir,
    char** dirs,
    int* dir_types,
    int* dir_num) {
  // Sanity check
  if(!sanity_check(tiledb_ctx))
    return TILEDB_ERR;

  // Check parent directory name length
  if(parent_dir == NULL || strlen(parent_dir) > TILEDB_NAME_MAX_LEN) {
    PRINT_ERROR("Invalid parent directory name length");
    return TILEDB_ERR;
  }

  // List TileDB objects
  if(tiledb_ctx->storage_manager_->ls(
         parent_dir,
         dirs,
         dir_types,
         *dir_num) != TILEDB_SM_OK)
    return TILEDB_ERR;
  else 
    return TILEDB_OK;
}
