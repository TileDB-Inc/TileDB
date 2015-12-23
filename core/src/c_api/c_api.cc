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
  array_schema_c.attribute_num_ = array_schema->attribute_num_;
  array_schema_c.capacity_ = array_schema->capacity_;
  array_schema_c.cell_order_ = array_schema->cell_order_;
  array_schema_c.compression_ = array_schema->compression_;
  array_schema_c.consolidation_step_ = array_schema->consolidation_step_;
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
    TileDB_Array* tiledb_array,
    const char* dir,
    int mode,
    const void* range,
    const char** attributes,
    int attribute_num) {
  // Sanity check
  if(!sanity_check(tiledb_ctx))
    return TILEDB_ERR;

  // Init the array
  int rc = tiledb_ctx->storage_manager_->array_init(
               tiledb_array->array_,
               dir,
               mode, 
               range, 
               attributes,
               attribute_num);

  // Set TileDB context
  tiledb_array->tiledb_ctx_ = tiledb_ctx;

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

  // Return
  if(rc == TILEDB_SM_OK) 
    return TILEDB_OK;
  else 
    return TILEDB_ERR; 
}

int tiledb_array_write(
    const TileDB_Array* tiledb_array,
    const void** buffers,
    const size_t* buffer_sizes) {
  // Sanity check
  if(!sanity_check(tiledb_array->tiledb_ctx_))
    return TILEDB_ERR;

  // Finalize array
  int rc = tiledb_array->tiledb_ctx_->storage_manager_->array_write(
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
