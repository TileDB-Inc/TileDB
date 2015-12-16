/**
 * @file   tiledb.cc
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

#include "cell_crafter.h"
#include "data_generator.h"
#include "loader.h"
#include "query_processor.h"
#include "storage_manager.h"
#include "tiledb.h"
#include <cassert>
#include <openssl/md5.h>


/* ****************************** */
/*             MACROS             */
/* ****************************** */

#if VERBOSE == 1
#  define PRINT_ERROR(x) std::cerr << "[TileDB] Error: " << x << ".\n" 
#elif VERBOSE == 2
#  define PRINT_ERROR(x) std::cerr << "[TileDB::capis] Error: " << x << ".\n" 
#else
#  define PRINT_ERROR(x) do { } while(0) 
#endif

/* ****************************** */
/*            CONTEXT             */
/* ****************************** */

typedef struct TileDB_CTX{
  Loader* loader_;
  QueryProcessor* query_processor_;
  StorageManager* storage_manager_;
} TileDB_CTX;

int tiledb_ctx_init(TileDB_CTX** tiledb_ctx) {
  // Initialize context
  *tiledb_ctx = (TileDB_CTX*) malloc(sizeof(struct TileDB_CTX));
  if(*tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot initialize TileDB context; Failed to allocate memory "
                "space for the context");
    return TILEDB_ERR;
  }

  // Create modules
  
  (*tiledb_ctx)->storage_manager_ = new StorageManager();
  if(!(*tiledb_ctx)->storage_manager_->created_successfully())
    return TILEDB_ERR;
  (*tiledb_ctx)->loader_ = new Loader((*tiledb_ctx)->storage_manager_);
  if(!(*tiledb_ctx)->loader_->created_successfully())
    return TILEDB_ERR;
  (*tiledb_ctx)->query_processor_ =
      new QueryProcessor((*tiledb_ctx)->storage_manager_);
  if(!(*tiledb_ctx)->query_processor_->created_successfully())
    return TILEDB_ERR;

  // Success
  return TILEDB_OK;
}

int tiledb_ctx_finalize(TileDB_CTX* tiledb_ctx) {
  // Trivial case
  if(tiledb_ctx == NULL)
    return TILEDB_OK;

  // Sanity checks
  assert(tiledb_ctx->storage_manager_ &&
         tiledb_ctx->loader_ &&
         tiledb_ctx->query_processor_);

  // Clear the TileDB modules
  if(tiledb_ctx->storage_manager_->finalize())
    return TILEDB_ERR;
  delete tiledb_ctx->storage_manager_;

  if(tiledb_ctx->loader_->finalize())
    return TILEDB_ERR;
  delete tiledb_ctx->loader_;

  if(tiledb_ctx->query_processor_->finalize())
    return TILEDB_ERR;
  delete tiledb_ctx->query_processor_;

  // Success
  return TILEDB_OK;
}

/* ****************************** */
/*           WORKSPACE            */
/* ****************************** */

int tiledb_workspace_create(
    const TileDB_CTX* tiledb_ctx,
    const char* workspace,
    const char* master_catalog) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot create workspace; Invalid TileDB context");
    return TILEDB_ERR;
  }

  // Execute the query
  if(tiledb_ctx->storage_manager_->workspace_create(
             workspace,
             master_catalog) == TILEDB_SM_OK)
    return TILEDB_OK;
  else
    return TILEDB_ERR;
}

int tiledb_workspace_move(
    const TileDB_CTX* tiledb_ctx,
    const char* old_workspace,
    const char* new_workspace,
    const char* master_catalog) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot move workspace; Invalid TileDB context");
    return TILEDB_ERR;
  }

  if(tiledb_ctx->storage_manager_->workspace_move(
             old_workspace,
             new_workspace,
             master_catalog) == TILEDB_SM_OK)
    return TILEDB_OK;
  else
    return TILEDB_ERR;
}

int tiledb_workspace_clear(
    const TileDB_CTX* tiledb_ctx,
    const char* workspace) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot clear workspace; Invalid TileDB context");
    return TILEDB_ERR;
  }

  // Execute the query
  if(tiledb_ctx->storage_manager_->workspace_clear(workspace) == TILEDB_SM_OK)
    return TILEDB_OK;
  else
    return TILEDB_ERR;
}

int tiledb_workspace_delete(
    const TileDB_CTX* tiledb_ctx,
    const char* workspace,
    const char* master_catalog) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot delete workspace; Invalid TileDB context");
    return TILEDB_ERR;
  }

  // Execute the query
  if(tiledb_ctx->storage_manager_->workspace_delete(
             workspace,
             master_catalog) == TILEDB_SM_OK)
    return TILEDB_OK;
  else
    return TILEDB_ERR;
}

int tiledb_master_catalog_move(
    const TileDB_CTX* tiledb_ctx,
    const char* old_master_catalog,
    const char* new_master_catalog) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot move master catalog; Invalid TileDB context");
    return TILEDB_ERR;
  }

  if(tiledb_ctx->storage_manager_->master_catalog_move(
             old_master_catalog,
             new_master_catalog) == TILEDB_SM_OK)
    return TILEDB_OK;
  else
    return TILEDB_ERR;
}

int tiledb_master_catalog_delete(
    const TileDB_CTX* tiledb_ctx,
    const char* master_catalog) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot delete master catalog; Invalid TileDB context");
    return TILEDB_ERR;
  }

  // Execute the query
  if(tiledb_ctx->storage_manager_->master_catalog_delete(
             master_catalog) == TILEDB_SM_OK)
    return TILEDB_OK;
  else
    return TILEDB_ERR;
}

/* ****************************** */
/*             GROUP              */
/* ****************************** */

int tiledb_group_create(
    const TileDB_CTX* tiledb_ctx,
    const char* group) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot create group; Invalid TileDB context");
    return TILEDB_ERR;
  }

  // Execute the query
  if(tiledb_ctx->storage_manager_->group_create(group) == TILEDB_SM_OK)
    return TILEDB_OK;
  else
    return TILEDB_ERR;
}

int tiledb_group_move(
    const TileDB_CTX* tiledb_ctx,
    const char* old_group,
    const char* new_group) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot move group; Invalid TileDB context");
    return TILEDB_ERR;
  }

  // Execute the query
  if(tiledb_ctx->storage_manager_->group_move(
             old_group,
             new_group) == TILEDB_SM_OK)
    return TILEDB_OK;
  else
    return TILEDB_ERR;
}

int tiledb_group_clear(
    const TileDB_CTX* tiledb_ctx,
    const char* group) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot clear group; Invalid TileDB context");
    return TILEDB_ERR;
  }

  // Execute the query
  if(tiledb_ctx->storage_manager_->group_clear(group) == TILEDB_SM_OK)
    return TILEDB_OK;
  else
    return TILEDB_ERR;
}

int tiledb_group_delete(
    const TileDB_CTX* tiledb_ctx,
    const char* group) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot delete group; Invalid TileDB context");
    return TILEDB_ERR;
  }

  // Execute the query
  if(tiledb_ctx->storage_manager_->group_delete(group) == TILEDB_SM_OK)
    return TILEDB_OK;
  else
    return TILEDB_ERR;
}

/* ****************************** */
/*            CELL                */
/* ****************************** */

typedef struct TileDB_Cell{
  CellCrafter* cell_crafter_;
} TileDB_Cell;

int tiledb_cell_init(
    TileDB_CTX* tiledb_ctx,
    int ad,
    TileDB_Cell** tiledb_cell) {
   // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot initialize cell; Invalid TileDB context");
    return TILEDB_ERR;
  }

  // Get array schema
  const ArraySchema* array_schema;
  if(tiledb_ctx->storage_manager_->array_schema_get(
         ad, array_schema) == TILEDB_SM_ERR)
    return TILEDB_ERR;

  // Initialize cell struct
  *tiledb_cell = (TileDB_Cell*) malloc(sizeof(struct TileDB_Cell));
  (*tiledb_cell)->cell_crafter_ = new CellCrafter(array_schema);
}

int tiledb_cell_finalize(TileDB_Cell* tiledb_cell) {
  // Sanity check
  if(tiledb_cell == NULL) {
    PRINT_ERROR("Cannot finalize cell; Invalid TileDB cell");
    return TILEDB_ERR;
  }

  // Free memory
  delete tiledb_cell->cell_crafter_;
  free(tiledb_cell);
}

/* ****************************** */
/*            ARRAY               */
/* ****************************** */

int tiledb_array_create(
    const TileDB_CTX* tiledb_ctx,
    const TileDB_ArraySchema* array_schema_struct) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot create array; Invalid TileDB context");
    return TILEDB_ERR;
  }

  // Create an array schema from the input string
  ArraySchema* array_schema = new ArraySchema();
  if(array_schema->init(array_schema_struct)) {
    delete array_schema;
    return TILEDB_ERR;
  }

  // Store array schema
  int rc = tiledb_ctx->storage_manager_->array_schema_store(array_schema);

  // Clean up
  delete array_schema;

  // Return
  if(rc == TILEDB_SM_OK) 
    return TILEDB_OK;
  else
    return TILEDB_ERR;
}

int tiledb_array_move(
    const TileDB_CTX* tiledb_ctx,
    const char* old_array,
    const char* new_array) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot move array; Invalid TileDB context");
    return TILEDB_ERR;
  }

  // Execute query
  if(tiledb_ctx->storage_manager_->array_move(
             old_array,
             new_array) == TILEDB_SM_OK)
    return TILEDB_OK;
  else
    return TILEDB_ERR;
}

int tiledb_array_clear(
    const TileDB_CTX* tiledb_ctx,
    const char* array) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot clear array; Invalid TileDB context");
    return TILEDB_ERR;
  }

  // Execute query
  if(tiledb_ctx->storage_manager_->array_clear(array) == TILEDB_SM_OK)
    return TILEDB_OK;
  else
    return TILEDB_ERR;
}

int tiledb_array_delete(
    const TileDB_CTX* tiledb_ctx,
    const char* array) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot delete array; Invalid TileDB context");
    return TILEDB_ERR;
  }

  // Execute query
  if(tiledb_ctx->storage_manager_->array_delete(array) == TILEDB_SM_OK)
    return TILEDB_OK;
  else
    return TILEDB_ERR;
}

int tiledb_array_open(
    TileDB_CTX* tiledb_ctx,
    const char* array,
    int mode) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot open array; Invalid TileDB context");
    return TILEDB_ERR;
  }

  // Get mode
  const char* mode_str;
  if(mode == TILEDB_ARRAY_MODE_READ) {
    mode_str = "r";
  } else if(mode == TILEDB_ARRAY_MODE_WRITE) {
    mode_str = "w";
  } else {
    PRINT_ERROR("Cannot open array; Invalid array mode");
    return TILEDB_ERR;
  }

  // Open array
  int ad = tiledb_ctx->storage_manager_->array_open(array, mode_str);

  // Return array descriptor
  if(ad < 0) 
    return TILEDB_ERR;
  else
    return ad;
}

int tiledb_array_close(TileDB_CTX* tiledb_ctx, int ad) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot close array; Invalid TileDB context");
    return TILEDB_ERR;
  }

  // Close array
  if(tiledb_ctx->storage_manager_->array_close(ad) == TILEDB_SM_OK)
    return TILEDB_OK;
  else
    return TILEDB_ERR;
}

int tiledb_array_write(
    TileDB_CTX* tiledb_ctx, 
    int ad, 
    const void* cell) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot write cell to array; Invalid TileDB context");
    return TILEDB_ERR;
  }

  // For easy reference
  const ArraySchema* array_schema;
  if(tiledb_ctx->storage_manager_->array_schema_get(ad, array_schema))
    return -1;
  const std::type_info* type = array_schema->coords_type();

  // Write cell, templating on coordinates type
  int rc;
  if(type == &typeid(int)) {
    rc = tiledb_ctx->storage_manager_->cell_write<int>(ad, cell);
  } else if(type == &typeid(int64_t)) {
    rc = tiledb_ctx->storage_manager_->cell_write<int64_t>(ad, cell);
  } else if(type == &typeid(float)) {
    rc = tiledb_ctx->storage_manager_->cell_write<float>(ad, cell);
  } else if(type == &typeid(double)) {
    rc = tiledb_ctx->storage_manager_->cell_write<double>(ad, cell);
  } else {
    PRINT_ERROR("Cannot write cell to array; Invalid coordinates type");
    return TILEDB_ERR;
  }

  // Return
  if(rc == TILEDB_SM_OK)
    return TILEDB_OK;
  else
    return TILEDB_ERR;
}

int tiledb_array_write_sorted(
    TileDB_CTX* tiledb_ctx, 
    int ad, 
    const void* cell) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot write cell to array; Invalid TileDB context");
    return TILEDB_ERR;
  }

  // For easy reference
  const ArraySchema* array_schema;
  if(tiledb_ctx->storage_manager_->array_schema_get(ad, array_schema))
    return -1;
  const std::type_info* type = array_schema->coords_type();

  // Write cell, templating on coordinates type
  int rc;
  if(type == &typeid(int)) {
    rc = tiledb_ctx->storage_manager_->cell_write_sorted<int>(ad, cell);
  } else if(type == &typeid(int64_t)) {
    rc = tiledb_ctx->storage_manager_->cell_write_sorted<int64_t>(ad, cell);
  } else if(type == &typeid(float)) {
    rc = tiledb_ctx->storage_manager_->cell_write_sorted<float>(ad, cell);
  } else if(type == &typeid(double)) {
    rc = tiledb_ctx->storage_manager_->cell_write_sorted<double>(ad, cell);
  } else {
    PRINT_ERROR("Cannot write cell to array; Invalid coordinates type");
    return TILEDB_ERR;
  }

  // Return
  if(rc == TILEDB_SM_OK)
    return TILEDB_OK;
  else
    return TILEDB_ERR;
}

int tiledb_array_write_dense(
    const TileDB_CTX* tiledb_ctx,
    int ad,
    const void* cell) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot write to array; Invalid TileDB context");
    return TILEDB_ERR;
  }

  // Get the coordinates type
  const ArraySchema* array_schema;
  if(tiledb_ctx->storage_manager_->array_schema_get(ad, array_schema) 
     == TILEDB_SM_ERR)
    return TILEDB_ERR;
  const std::type_info* coords_type = array_schema->coords_type();

  // Perform the query
  int rc;
  if(coords_type == &typeid(int)) {
    rc = tiledb_ctx->storage_manager_->cell_write_sorted<int>(
             ad, cell, true);
  } else if(coords_type == &typeid(int64_t)) {
    rc = tiledb_ctx->storage_manager_->cell_write_sorted<int64_t>(
             ad, cell, true);
  } else {
    PRINT_ERROR("Cannot write to array; invalid coordinates type");
    return TILEDB_ERR;
  }

  if(rc == TILEDB_SM_OK)
    return TILEDB_OK;
  else
   return TILEDB_ERR;
}

int tiledb_array_read(
    const TileDB_CTX* tiledb_ctx,
    int ad,
    const double* range,
    const char** dimensions,
    int dim_num,
    const char** attributes,
    int attribute_num,
    void* buffer,
    size_t* buffer_size) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot read from array; Invalid TileDB context");
    return TILEDB_ERR;
  }

  // Get dimension ids, attribute ids and the coordinates type
  const ArraySchema* array_schema;
  if(tiledb_ctx->storage_manager_->array_schema_get(ad, array_schema) 
     == TILEDB_SM_ERR)
    return TILEDB_ERR;
  const std::type_info* coords_type = array_schema->coords_type();
  std::vector<int> dim_ids;
  std::vector<int> attribute_ids;
  int id;
  if(dimensions == NULL) {
    for(int i=0; i<dim_num; ++i) 
      dim_ids.push_back(i);
  } else if(dim_num == 1 && !strcmp(dimensions[0], "__hide")) {
    // Do nothing - dim_ids should be empty
  } else {
    for(int i=0; i<dim_num; ++i) {
      id = array_schema->dim_id(dimensions[i]);
      if(id == -1) {
        PRINT_ERROR("Cannot read from array; invalid dimension name");
        return TILEDB_ERR;
      }
      dim_ids.push_back(id);
    }
  }
  if(attributes == NULL) {
    for(int i=0; i<array_schema->attribute_num(); ++i) 
      attribute_ids.push_back(i);
  } else {
    for(int i=0; i<attribute_num; ++i) {
      id = array_schema->attribute_id(attributes[i]);
      if(id == -1) {
        PRINT_ERROR("Cannot read from array; invalid attribute name");
        return TILEDB_ERR;
      }
      attribute_ids.push_back(id);
    }
  }

  // Perform the query
  int rc;
  if(coords_type == &typeid(int)) { 
    int* new_range = new int[2*array_schema->dim_num()]; 
    convert(&range[0], new_range, 2*array_schema->dim_num());
    rc = tiledb_ctx->storage_manager_->array_read<int>(
        ad, new_range, dim_ids, attribute_ids, buffer, buffer_size);
    delete [] new_range;
  } else if(coords_type == &typeid(int64_t)) { 
    int64_t* new_range = new int64_t[2*array_schema->dim_num()]; 
    convert(&range[0], new_range, 2*array_schema->dim_num());
    rc = tiledb_ctx->storage_manager_->array_read<int64_t>(
        ad, new_range, dim_ids, attribute_ids, buffer, buffer_size);
    delete [] new_range;
  } else if(coords_type == &typeid(float)) { 
    float* new_range = new float[2*array_schema->dim_num()]; 
    convert(&range[0], new_range, 2*array_schema->dim_num());
    rc = tiledb_ctx->storage_manager_->array_read<float>(
        ad, new_range, dim_ids, attribute_ids, buffer, buffer_size);
    delete [] new_range;
  } else if(coords_type == &typeid(double)) { 
    double* new_range = new double[2*array_schema->dim_num()]; 
    convert(&range[0], new_range, 2*array_schema->dim_num());
    rc = tiledb_ctx->storage_manager_->array_read<double>(
        ad, new_range, dim_ids, attribute_ids, buffer, buffer_size);
    delete [] new_range;
  } else {
    PRINT_ERROR("Cannot read from array; invalid coordinates type");
    return TILEDB_ERR;
  }

  // Return 
  if(rc == TILEDB_SM_OK)
    return TILEDB_OK;
  else if(rc == TILEDB_SM_READ_BUFFER_OVERFLOW)
    return TILEDB_READ_BUFFER_OVERFLOW;
  else
    return TILEDB_ERR;
}

int tiledb_array_consolidate(
    const TileDB_CTX* tiledb_ctx,
    const char* array) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot consolidate array; Invalid TileDB context");
     return TILEDB_ERR;
  }

  // Perform the query
  if(tiledb_ctx->storage_manager_->array_consolidate(array) == TILEDB_SM_OK) 
    return TILEDB_OK;
  else
    return TILEDB_ERR;
}

/* ****************************** */
/*            METADATA            */
/* ****************************** */

int tiledb_metadata_create(
    const TileDB_CTX* tiledb_ctx,
    const TileDB_MetadataSchema* metadata_schema) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot create metadata; Invalid TileDB context");
    return TILEDB_ERR;
  }

  // Create an array schema from the metadata schema
  ArraySchema* array_schema = new ArraySchema();
  if(array_schema->init(metadata_schema)) 
    return TILEDB_ERR;

  // Store the array schema
  int rc = tiledb_ctx->storage_manager_->metadata_schema_store(
        array_schema);

  // Clean up
  delete array_schema;

  // Return
  if (rc == TILEDB_SM_OK)
    return TILEDB_OK;
  else
    return TILEDB_ERR;
}

int tiledb_metadata_move(
    const TileDB_CTX* tiledb_ctx,
    const char* old_metadata,
    const char* new_metadata) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot move metadata; Invalid TileDB context");
    return TILEDB_ERR;
  }

  // Execute query
  if(tiledb_ctx->storage_manager_->metadata_move(
             old_metadata,
             new_metadata) == TILEDB_SM_OK)
    return TILEDB_OK;
  else
    return TILEDB_ERR;
}

int tiledb_metadata_clear(
    const TileDB_CTX* tiledb_ctx,
    const char* metadata) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot clear metadata; Invalid TileDB context");
    return TILEDB_ERR;
  }

  // Execute query
  if(tiledb_ctx->storage_manager_->metadata_clear(metadata) == TILEDB_SM_OK)
    return TILEDB_OK;
  else
    return TILEDB_ERR;
}


int tiledb_metadata_delete(
    const TileDB_CTX* tiledb_ctx,
    const char* metadata) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot delete metadata; Invalid TileDB context");
    return TILEDB_ERR;
  }

  // Execute query
  if(tiledb_ctx->storage_manager_->metadata_delete(metadata) == TILEDB_SM_OK)
    return TILEDB_OK;
  else
    return TILEDB_ERR;
}

int tiledb_metadata_open(
    TileDB_CTX* tiledb_ctx,
    const char* metadata,
    int mode) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot open metadata; Invalid TileDB context");
    return TILEDB_ERR;
  }

  // Get mode
  const char* mode_str;
  if(mode == TILEDB_METADATA_MODE_READ) {
    mode_str = "r";
  } else if(mode == TILEDB_METADATA_MODE_WRITE) {
    mode_str = "w";
  } else {
    PRINT_ERROR("Cannot open metadata; Invalid array mode");
    return TILEDB_ERR;
  }

  // Open metadata
  int md = tiledb_ctx->storage_manager_->metadata_open(metadata, mode_str);

  // Return metadata descriptor
  if(md < 0) 
    return TILEDB_ERR;
  else
    return md;
}

int tiledb_metadata_close(TileDB_CTX* tiledb_ctx, int md) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot close metadata; Invalid TileDB context");
    return TILEDB_ERR;
  }

  // Close metadata
  if(tiledb_ctx->storage_manager_->metadata_close(md) == TILEDB_SM_OK)
    return TILEDB_OK;
  else
    return TILEDB_ERR;
}

int tiledb_metadata_write(
    const TileDB_CTX* tiledb_ctx,
    int md,
    const char* key,
    const void* value) {
  // Sanity checks
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot write to metadata; Invalid TileDB context");
    return TILEDB_ERR;
  }

  // For easy reference
  const ArraySchema* array_schema;
  if(tiledb_ctx->storage_manager_->metadata_schema_get(md, array_schema))
    return -1;

  // Get coordinates from key
  int coords[4];
  MD5((const unsigned char*) key, 
       strlen(key), 
       (unsigned char*) &coords);

  // Get cell size
  const void* attributes;
  size_t attributes_size, cell_size;
  size_t coords_size = array_schema->coords_size();
  bool var_size = (array_schema->cell_size() == VAR_SIZE);
  if(!var_size) { 
    cell_size = array_schema->cell_size();
    attributes_size = cell_size - coords_size;
    attributes = value;
  } else { 
    size_t value_size;
    memcpy(&value_size, value, sizeof(size_t));
    cell_size = coords_size + value_size; 
    attributes_size = value_size - sizeof(size_t);
    attributes = (const char*) value + sizeof(size_t);
  }

  // Create new cell
  size_t offset = 0;
  void* cell = malloc(cell_size);
  assert(cell != NULL);
  memcpy(cell, coords, coords_size);
  offset += coords_size;
  if(var_size) {
    memcpy((char*) cell + coords_size, &cell_size, sizeof(size_t));
    offset += sizeof(size_t);
  }
  memcpy((char*) cell + offset, attributes, attributes_size);

  // Write cell
  tiledb_ctx->storage_manager_->metadata_write<int>(md, cell);

  // Clean up
  free(cell);

  return TILEDB_OK;
}

int tiledb_metadata_read(
    const TileDB_CTX* tiledb_ctx,
    int md,
    const char* key,
    const char** attributes,
    int attributes_num,
    void* value,
    size_t* value_size) {
  // Sanity checks
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot read from metadata; Invalid TileDB context");
    return TILEDB_ERR;
  }
  
  // Get coordinates from key
  int coords[4];
  MD5((const unsigned char*) key, 
       strlen(key), 
       (unsigned char*) &coords);

  // Create a range covering a single cell
  int range[8];
  for(int i=0; i<4; ++i) {
    range[2*i] = coords[i];
    range[2*i+1] = coords[i];
  }

  // For easy reference
  const ArraySchema* array_schema;
  if(tiledb_ctx->storage_manager_->metadata_schema_get(md, array_schema))
    return -1;

  // Get attribute ids
  std::vector<int> attribute_ids;
  if(attributes == NULL) {
    attribute_ids = array_schema->attribute_ids();
  } else {  
    std::vector<std::string> attributes_vec;
    for(int i=0; i<attributes_num; ++i)
      attributes_vec.push_back(attributes[i]);
    if(array_schema->get_attribute_ids(attributes_vec, attribute_ids))
      return TILEDB_ERR;
  }

  // Create iterator
  ArrayConstCellIterator<int>* cell_it = 
      tiledb_ctx->storage_manager_->metadata_begin(md, range, attribute_ids);

  // Prepare a cell
  Cell cell(cell_it->array_schema(), cell_it->attribute_ids(), 0, true);

  // Write cell into the value buffer
  size_t cell_c_size = 0;
  size_t cell_c_capacity = 0;
  const void* cell_it_c = **cell_it;
  void* cell_c = NULL;
  if(cell_it_c == NULL) {
    *value_size = 0;
  } else {
    std::vector<int> dim_ids;
    cell.set_cell(cell_it_c);
    cell.cell<int>(dim_ids, attribute_ids, cell_c, cell_c_capacity, cell_c_size);
    // TODO: This can be optimized
    if(cell_c_size > *value_size) {
      PRINT_ERROR("Cannot read from metadata; input buffer overflow");
      free(cell_c);
      return TILEDB_ERR;
    } else {
      memcpy(value, cell_c, cell_c_size);
      *value_size = cell_c_size;
      free(cell_c);
    } 
  }

  return TILEDB_OK;
}

int tiledb_metadata_consolidate(
    const TileDB_CTX* tiledb_ctx,
    const char* metadata) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot consolidate metadata: Invalid TileDB context");
    return TILEDB_ERR;
  }

  if(tiledb_ctx->storage_manager_->metadata_consolidate(metadata) == 
     TILEDB_SM_OK)
    return TILEDB_OK;
  else
    return TILEDB_ERR; 
}




// ------------------------------------------- //
// TODO: Clean up from this point and onwards  //
// ------------------------------------------- //









int tiledb_array_exists(
    const TileDB_CTX* tiledb_ctx,
    const char* array) {
  return (tiledb_ctx->storage_manager_->array_exists(array) ? 1 : 0);
}

int tiledb_array_read_into_array(
    const TileDB_CTX* tiledb_ctx,
    const char* array,
    const char* out,
    const double* range,
    int range_num,
    const char** dimensions,
    int dimensions_num,
    const char** attributes,
    int attributes_num) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot compute subarray: Invalid TileDB context");
    return -1;
  }

  // Compute vectors for range and attribute names
  std::vector<std::string> attribute_names_vec;
  for(int i=0; i<attributes_num; ++i)
    attribute_names_vec.push_back(attributes[i]);
  std::vector<double> range_vec;
  for(int i=0; i<range_num; ++i)
    range_vec.push_back(range[i]);

  // Perform the subarray query
  return tiledb_ctx->query_processor_->subarray(
      array, out, 
      range_vec, attribute_names_vec);
}

int tiledb_array_read_into_file(
    const TileDB_CTX* tiledb_ctx,
    const char* array,
    const char* file,
    const double* range,
    int range_num,
    const char** dimensions,
    int dimensions_num,
    const char** attributes,
    int attributes_num,
    const char* format,
    char delimiter,
    int precision) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot export array: Invalid TileDB context");
    return -1;
  }

  // Compute vectors for dim_names, attribute names and range
  std::vector<std::string> dimensions_vec, attributes_vec;
  std::vector<double> range_vec;
  for(int i=0; i<dimensions_num; ++i)
    dimensions_vec.push_back(dimensions[i]);
  for(int i=0; i<attributes_num; ++i)
    attributes_vec.push_back(attributes[i]);
  for(int i=0; i<range_num; ++i)
    range_vec.push_back(range[i]);

  return tiledb_ctx->query_processor_->array_export(
      array, file, format,
      dimensions_vec, attributes_vec, range_vec,
      delimiter, precision);
}








int tiledb_metadata_read_into_file(
    const TileDB_CTX* tiledb_ctx,
    const char* metadata,
    const char* output,
    const char* key,
    const char** attributes,
    int attributes_num,
    const char* format,
    char delimiter,
    int precision) {
  // Sanity checks
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot read from metadata; Invalid TileDB context");
    return TILEDB_ERR;
  }
  if(output == NULL) {
    PRINT_ERROR("Cannot read from metadata; No output given");
    return TILEDB_ERR;
  }

  // Get output file
  std::string file;
  if(!strcmp(output, "/dev/stdout") || is_file(output)) {
    file = output;
  } else {
    PRINT_ERROR(std::string("Invalid output '") + output + "'");
    return TILEDB_ERR;
  }

  // Get attributes
  std::vector<std::string> attributes_vec;
  for(int i=0; i<attributes_num; ++i)
    attributes_vec.push_back(attributes[i]);

  // Determine format
  std::string format_str;
  if(format != NULL && format[0] != '\0')  // Format given
    format_str = format;
  else // Determine automatically
    std::string file_format = get_file_format(file);

  if(format_str == "") { // If still format not determined
    PRINT_ERROR("Could not automatically determine input format");
    return -1;
  }

  // Determine delimiter
  if(delimiter == 0)
    delimiter = CSV_DELIMITER;

  // Load the data into metadata
  if(tiledb_ctx->query_processor_->metadata_export(
          metadata, 
          file, 
          key,
          attributes_vec,
          format_str, 
          delimiter,
          precision) == TILEDB_LD_OK)
     return TILEDB_OK;
  else
     return TILEDB_ERR;
}

int tiledb_metadata_write_from_file(
    const TileDB_CTX* tiledb_ctx,
    const char* metadata,
    const char** inputs,
    int inputs_num,
    const char* format,
    char delimiter) {
  // Sanity checks
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot write to metadata; Invalid TileDB context");
    return TILEDB_ERR;
  }
  if(inputs == NULL || inputs_num == 0) {
    PRINT_ERROR("Cannot write to metadata; No input given");
    return TILEDB_ERR;
  }

  // Get input files
  std::vector<std::string> files;
  for(int i=0; i<inputs_num; ++i) {
    if(!strcmp(inputs[i], "/dev/stdin") || is_file(inputs[i])) {
      files.push_back(inputs[i]);
    } else if(is_dir(inputs[i])) {
      std::vector<std::string> dir_files = get_filenames(inputs[i]);
      files.insert(
          std::end(files), 
          std::begin(dir_files), 
          std::end(dir_files));
    } else {
      PRINT_ERROR(std::string("Unknown input '") + inputs[i] + "'");
      return TILEDB_ERR;
    }
  }

  // Determine format
  std::string format_str;
  if(format != NULL && format[0] != '\0') { // Format given
    format_str = format;
  } else { // Determine automatically
    int files_num = files.size();
    std::string file_format;
    for(int i=0; i<files_num; ++i) {
      file_format = get_file_format(files[i]); 
      if(format_str != "" && file_format != "" &&
         format_str != file_format) {
        PRINT_ERROR("Could not automatically determine input format; "
                    "conflicting inputs formats detected");
        return -1;
      }
      format_str = file_format;
    }
  }

  if(format_str == "") { // If still format not determined
    PRINT_ERROR("Could not automatically determine input format");
    return -1;
  }

  // Determine delimiter
  if(delimiter == 0)
    delimiter = CSV_DELIMITER;

  // Load the data into metadata
  if(tiledb_ctx->loader_->metadata_load(
          metadata, 
          files, 
          format_str, 
          delimiter) == TILEDB_LD_OK)
     return TILEDB_OK;
  else
     return TILEDB_ERR;
}

int tiledb_array_write_from_file(
    const TileDB_CTX* tiledb_ctx,
    const char* array,
    const char** inputs,
    int inputs_num,
    const char* format,
    char delimiter) {
  // Sanity checks
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot write to array; Invalid TileDB context");
    return TILEDB_ERR;
  }
  if(inputs == NULL || inputs_num == 0) {
    PRINT_ERROR("Cannot write to array; No input given");
    return TILEDB_ERR;
  }

  // Get input files
  std::vector<std::string> files;
  for(int i=0; i<inputs_num; ++i) {
    if(!strcmp(inputs[i], "/dev/stdin") || is_file(inputs[i])) {
      files.push_back(inputs[i]);
    } else if(is_dir(inputs[i])) {
      std::vector<std::string> dir_files = get_filenames(inputs[i]);
      files.insert(
          std::end(files), 
          std::begin(dir_files), 
          std::end(dir_files));
    } else {
      PRINT_ERROR(std::string("Unknown input '") + inputs[i] + "'");
      return TILEDB_ERR;
    }
  }

  // Determine format
  std::string format_str;
  if(format != NULL && format[0] != '\0') { // Format given
    format_str = format;
  } else { // Determine automatically
    int files_num = files.size();
    std::string file_format;
    for(int i=0; i<files_num; ++i) {
      file_format = get_file_format(files[i]); 
      if(format_str != "" && file_format != "" &&
         format_str != file_format) {
        PRINT_ERROR("Could not automatically determine input format; "
                    "conflicting input formats detected");
        return -1;
      }
      format_str = file_format;
    }
  }

  if(format_str == "") { // If still format not determined
    PRINT_ERROR("Could not automatically determine input format");
    return -1;
  }

  // Determine delimiter
  if(delimiter == 0)
    delimiter = CSV_DELIMITER;

  // Load the data into metadata
  if(tiledb_ctx->loader_->array_load(
          array, 
          files, 
          format_str, 
          delimiter) == TILEDB_LD_OK)
     return TILEDB_OK;
  else
     return TILEDB_ERR;
}



int tiledb_list(
    const TileDB_CTX* tiledb_ctx,
    const char* item) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot list item: Invalid TileDB context");
    return -1;
  }

  return tiledb_ctx->storage_manager_->list(item);
}



/* ****************************** */
/*               I/O              */
/* ****************************** */



int tiledb_array_open_2(
    TileDB_CTX* tiledb_ctx,
    const char* workspace,
    const char* group,
    const char* array_name,
    const char* mode) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot open array - Invalid TileDB context");
    return -1;
  }

  // Open array
  return tiledb_ctx->storage_manager_->array_open(
             workspace, group, array_name, mode);
}

int tiledb_cell_write(TileDB_CTX* tiledb_ctx, int ad, const void* cell) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot write cell - Invalid TileDB context");
    return -1;
  }

  // For easy reference
  const ArraySchema* array_schema;
  if(tiledb_ctx->storage_manager_->array_schema_get(ad, array_schema))
    return -1;
  const std::type_info* type = array_schema->coords_type();

  // Write cell, templating on coordinates type
  if(type == &typeid(int)) {
    return tiledb_ctx->storage_manager_->cell_write<int>(ad, cell);
  } else if(type == &typeid(int64_t)) {
    return tiledb_ctx->storage_manager_->cell_write<int64_t>(ad, cell);
  } else if(type == &typeid(float)) {
    return tiledb_ctx->storage_manager_->cell_write<float>(ad, cell);
  } else if(type == &typeid(double)) {
    return tiledb_ctx->storage_manager_->cell_write<double>(ad, cell);
  } else {
    PRINT_ERROR("Cannot write cell - Invalid coordinates type");
    return -1;
  }
}

int tiledb_cell_write_sorted(TileDB_CTX* tiledb_ctx, int ad, const void* cell) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot write cell - Invalid TileDB context");
    return -1;
  }

  // For easy reference
  const ArraySchema* array_schema;
  if(tiledb_ctx->storage_manager_->array_schema_get(ad, array_schema))
    return -1;
  const std::type_info* type = array_schema->coords_type();

  // Write cell, templating on coordinates type
  if(type == &typeid(int)) {
    return tiledb_ctx->storage_manager_->cell_write_sorted<int>(ad, cell);
  } else if(type == &typeid(int64_t)) {
    return tiledb_ctx->storage_manager_->cell_write_sorted<int64_t>(ad, cell);
  } else if(type == &typeid(float)) {
    return tiledb_ctx->storage_manager_->cell_write_sorted<float>(ad, cell);
  } else if(type == &typeid(double)) {
    return tiledb_ctx->storage_manager_->cell_write_sorted<double>(ad, cell);
  } else {
    PRINT_ERROR("Cannot write cell - Invalid coordinates type");
    return -1;
  }
}

int tiledb_subarray_buf(
    const TileDB_CTX* tiledb_ctx,
    int ad,
    const double* range,
    int range_size,
    const char** dim_names,
    int dim_names_num,
    const char** attribute_names,
    int attribute_names_num,
    void* buffer,
    size_t* buffer_size) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot compute subarray: Invalid TileDB context");
    return -1;
  }

  // Compute vectors for range and attribute names
  std::vector<std::string> dim_names_vec, attribute_names_vec;
  for(int i=0; i<dim_names_num; ++i)
    dim_names_vec.push_back(dim_names[i]);
  for(int i=0; i<attribute_names_num; ++i)
    attribute_names_vec.push_back(attribute_names[i]);
  std::vector<double> range_vec;
  for(int i=0; i<range_size; ++i)
    range_vec.push_back(range[i]);

  // Read cells
  return tiledb_ctx->query_processor_->subarray_buf(
      ad, range_vec, dim_names_vec, attribute_names_vec, buffer, *buffer_size);
}
 

/* ****************************** */
/*          CELL ITERATORS        */
/* ****************************** */

// DEFINITIONS

typedef struct TileDB_ConstCellIterator {
  /* The array schema. */
  const ArraySchema* array_schema_;
  /* The C++ iterator. */
  void* it_;
  /* True if the iterator is at its beginning, and no */
  /* cell has been fetched yet. */
  bool no_cell_fetched_yet_;
} TileDB_ConstCellIterator;

typedef struct TileDB_ConstReverseCellIterator {
  /* The array schema. */
  const ArraySchema* array_schema_;
  /* The C++ iterator. */
  void* it_;
  /* True if the iterator is at its beginning, and no */
  /* cell has been fetched yet.                       */
  bool no_cell_fetched_yet_;
} TileDB_ConstReverseCellIterator;

typedef struct TileDB_ConstDenseCellIterator {
  /* The array schema. */
  const ArraySchema* array_schema_;
  /* The C++ iterator. */
  void* it_;
  /* True if the iterator is at its beginning, and no */
  /* cell has been fetched yet.                       */
  bool no_cell_fetched_yet_;
} TileDB_ConstDenseCellIterator;

typedef struct TileDB_ConstReverseDenseCellIterator {
  /* The array schema. */
  const ArraySchema* array_schema_;
  /* The C++ iterator. */
  void* it_;
  /* True if the iterator is at its beginning, and no */
  /* cell has been fetched yet.                       */
  bool no_cell_fetched_yet_;
} TileDB_ConstReverseDenseCellIterator;

// FUNCTIONS

int tiledb_const_cell_iterator_finalize(
    TileDB_ConstCellIterator* cell_it) {
  // Trivial case
  if(cell_it == NULL)
    return 0;

  // For easy reference
  const std::type_info* type = cell_it->array_schema_->coords_type();

  // Finalize the iterator, templating on the coordinates type
  if(type == &typeid(int)) {
    delete (ArrayConstCellIterator<int>*) cell_it->it_;
  } else if(type == &typeid(int64_t)) {
    delete (ArrayConstCellIterator<int64_t>*) cell_it->it_;
  } else if(type == &typeid(float)) {
    delete (ArrayConstCellIterator<float>*) cell_it->it_;
  } else if(type == &typeid(double)) {
    delete (ArrayConstCellIterator<double>*) cell_it->it_;
  } else {
    PRINT_ERROR("Cannot finalize iterator - Invalid coordinates type");
    return -1;
  }

  // Clean up
  free(cell_it);

  // Success
  return 0;
}

int tiledb_const_dense_cell_iterator_finalize(
    TileDB_ConstDenseCellIterator* cell_it) {
  // Trivial case
  if(cell_it == NULL)
    return 0;

  // For easy reference
  const std::type_info* type = cell_it->array_schema_->coords_type();

  // Finalize the iterator, templating on the coordinates type
  if(type == &typeid(int)) {
    delete (ArrayConstDenseCellIterator<int>*) cell_it->it_;
  } else if(type == &typeid(int64_t)) {
    delete (ArrayConstDenseCellIterator<int64_t>*) cell_it->it_;
  } else if(type == &typeid(float)) {
    delete (ArrayConstDenseCellIterator<float>*) cell_it->it_;
  } else if(type == &typeid(double)) {
    delete (ArrayConstDenseCellIterator<double>*) cell_it->it_;
  } else {
    PRINT_ERROR("Cannot finalize iterator - Invalid coordinates type");
    return -1;
  }

  // Clean up
  free(cell_it);

  // Success
  return 0;
}

int tiledb_const_reverse_cell_iterator_finalize(
    TileDB_ConstReverseCellIterator* cell_it) {
  // Trivial case
  if(cell_it == NULL)
    return 0;

  // For easy reference
  const std::type_info* type = cell_it->array_schema_->coords_type();

  // Finalize the iterator, templating on the coordinates type
  if(type == &typeid(int)) {
    delete (ArrayConstReverseCellIterator<int>*) cell_it->it_;
  } else if(type == &typeid(int64_t)) {
    delete (ArrayConstReverseCellIterator<int64_t>*) cell_it->it_;
  } else if(type == &typeid(float)) {
    delete (ArrayConstReverseCellIterator<float>*) cell_it->it_;
  } else if(type == &typeid(double)) {
    delete (ArrayConstReverseCellIterator<double>*) cell_it->it_;
  } else {
    PRINT_ERROR("Cannot finalize iterator - Invalid coordinates type");
    return -1;
  }

  // Success
  free(cell_it);

  // Success
  return 0;
}

int tiledb_const_reverse_dense_cell_iterator_finalize(
    TileDB_ConstReverseCellIterator* cell_it) {
  // TODO
  PRINT_ERROR("Reverse dense cell iterator currently not supported");
  exit(-1);
}

int tiledb_const_cell_iterator_init(
    TileDB_CTX* tiledb_ctx,
    int ad,
    const char** attribute_names,
    int attribute_names_num,
    TileDB_ConstCellIterator*& cell_it) {
  // Sanity checks
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot initialize iterator - Invalid TileDB context");
    return -1;
  }
  if(attribute_names_num >= 0) {
    PRINT_ERROR("Cannot initialize iterator - Invalid number of attributes");
    return -1;
  }
  if((attribute_names_num > 0 && attribute_names == NULL) ||
     (attribute_names_num == 0 && attribute_names != NULL)) {
    PRINT_ERROR("Cannot initialize iterator - Discrepancy between attribute "
                "names and their number");
    return -1;
  }

  // Initialize cell_it
  cell_it = (TileDB_ConstCellIterator*)
             malloc(sizeof(struct TileDB_ConstCellIterator));

  if(cell_it == NULL) {
    PRINT_ERROR("Cannot initialize iterator - Failed to allocate memory space "
                "for the iterator");
    return -1;
  }
  cell_it->no_cell_fetched_yet_ = true;

  // Ger array schema
  const ArraySchema* array_schema;
  if(tiledb_ctx->storage_manager_->array_schema_get(
            ad, array_schema))
    return -1;

  // For easy reference
  const std::type_info* type = array_schema->coords_type();

  // Get vector of attribute ids
  std::vector<std::string> attribute_names_vec;
  for(int i=0; i<attribute_names_num; ++i)
    attribute_names_vec.push_back(attribute_names[i]);
  std::vector<int> attribute_ids;
  if(array_schema->get_attribute_ids(attribute_names_vec, attribute_ids))
    return -1; 

  // Initialize cell iterator, templating on coordinates type
  if(type == &typeid(int)) {
    cell_it->it_ =
        tiledb_ctx->storage_manager_->begin<int>(ad, attribute_ids);
    if(cell_it->it_ == NULL) 
      return -1;
    cell_it->array_schema_ =
        ((ArrayConstCellIterator<int>*) cell_it->it_)->array_schema();
  } else if(type == &typeid(int64_t)) {
    cell_it->it_ =
        tiledb_ctx->storage_manager_->begin<int64_t>(ad, attribute_ids);
    if(cell_it->it_ == NULL) 
      return -1;
    cell_it->array_schema_ =
        ((ArrayConstCellIterator<int64_t>*) cell_it->it_)->array_schema();
  } else if(type == &typeid(float)) {
    cell_it->it_ =
        tiledb_ctx->storage_manager_->begin<float>(ad, attribute_ids);
    if(cell_it->it_ == NULL) 
      return -1;
    cell_it->array_schema_ =
        ((ArrayConstCellIterator<float>*) cell_it->it_)->array_schema();
  } else if(type == &typeid(double)) {
    cell_it->it_ =
        tiledb_ctx->storage_manager_->begin<double>(ad, attribute_ids);
    if(cell_it->it_ == NULL) 
      return -1;
    cell_it->array_schema_ =
        ((ArrayConstCellIterator<double>*) cell_it->it_)->array_schema();
  } else {
    PRINT_ERROR("Cannot initialize iterator - Invalid coordinates type");
    return -1;
  }
 
  // Success
  return 0;
}

int tiledb_const_cell_iterator_in_range_init(
    TileDB_CTX* tiledb_ctx,
    int ad,
    const char** attribute_names,
    int attribute_names_num,
    const void* range,
    TileDB_ConstCellIterator*& cell_it) {
  // Sanity checks
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot initialize iterator - Invalid TileDB context");
    return -1;
  }
  if(attribute_names_num >= 0) {
    PRINT_ERROR("Cannot initialize iterator - Invalid number of attributes");
    return -1;
  }
  if((attribute_names_num > 0 && attribute_names == NULL) ||
     (attribute_names_num == 0 && attribute_names != NULL)) {
    PRINT_ERROR("Cannot initialize iterator - Discrepancy between attribute "
                "names and their number");
    return -1;
  }
  if(range == NULL) {
    PRINT_ERROR("Cannot initialize iterator - Invalid range");
    return -1;
  }

  // Initialize cell_it
  cell_it = (TileDB_ConstCellIterator*)
             malloc(sizeof(struct TileDB_ConstCellIterator));
  if(cell_it == NULL) {
    PRINT_ERROR("Cannot initialize iterator - Failed to allocate memory space "
                "for the iterator");
    return -1;
  }
  cell_it->no_cell_fetched_yet_ = true;

  // Get array schema
  const ArraySchema* array_schema;
  if(tiledb_ctx->storage_manager_->array_schema_get(
         ad, array_schema))
    return -1; 

  // For easy reference
  const std::type_info* type = array_schema->coords_type();

  // Get vector of attribute ids
  std::vector<std::string> attribute_names_vec;
  for(int i=0; i<attribute_names_num; ++i)
    attribute_names_vec.push_back(attribute_names[i]);
  std::vector<int> attribute_ids;
  if(array_schema->get_attribute_ids(attribute_names_vec, attribute_ids))
    return -1; 

  // Initialize cell iterator, templating on the coordinates type
  if(type == &typeid(int)) {
    cell_it->it_ =
        tiledb_ctx->storage_manager_->begin<int>(
             ad, (const int*) range, attribute_ids);
    if(cell_it->it_ == NULL) 
      return -1;
    cell_it->array_schema_ =
        ((ArrayConstCellIterator<int>*) cell_it->it_)->array_schema();
  } else if(type == &typeid(int64_t)) {
    cell_it->it_ =
        tiledb_ctx->storage_manager_->begin<int64_t>(
            ad, (const int64_t*) range, attribute_ids);
    if(cell_it->it_ == NULL) 
      return -1;
    cell_it->array_schema_ =
        ((ArrayConstCellIterator<int64_t>*) cell_it->it_)->array_schema();
  } else if(type == &typeid(float)) {
    cell_it->it_ =
        tiledb_ctx->storage_manager_->begin<float>(
            ad, (const float*) range, attribute_ids);
    if(cell_it->it_ == NULL) 
      return -1;
    cell_it->array_schema_ =
        ((ArrayConstCellIterator<float>*) cell_it->it_)->array_schema();
  } else if(type == &typeid(double)) {
    cell_it->it_ =
        tiledb_ctx->storage_manager_->begin<double>(
            ad, (const double*) range, attribute_ids);
    if(cell_it->it_ == NULL) 
      return -1;
    cell_it->array_schema_ =
        ((ArrayConstCellIterator<double>*) cell_it->it_)->array_schema();
  } else {
    PRINT_ERROR("Cannot initialize iterator - Invalid coordinates type");
    return -1;
  }

  // Success
  return 0;
}

int tiledb_const_reverse_cell_iterator_init(
    TileDB_CTX* tiledb_ctx,
    int ad,
    const char** attribute_names,
    int attribute_names_num,
    TileDB_ConstReverseCellIterator*& cell_it) {
  // Sanity checks
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot initialize iterator - Invalid TileDB context");
    return -1;
  }
  if(attribute_names_num >= 0) {
    PRINT_ERROR("Cannot initialize iterator - Invalid number of attributes");
    return -1;
  }
  if((attribute_names_num > 0 && attribute_names == NULL) ||
     (attribute_names_num == 0 && attribute_names != NULL)) {
    PRINT_ERROR("Cannot initialize iterator - Discrepancy between attribute "
                "names and their number");
    return -1;
  }

  // Initialize cell_it
  cell_it = (TileDB_ConstReverseCellIterator*)
             malloc(sizeof(struct TileDB_ConstReverseCellIterator));
  if(cell_it == NULL) {
    PRINT_ERROR("Cannot initialize iterator - Failed to allocate memory space "
                "for the iterator");
    return -1;
  }
  cell_it->no_cell_fetched_yet_ = true;

  // Get array schema
  const ArraySchema* array_schema;
  if(tiledb_ctx->storage_manager_->array_schema_get(
         ad, array_schema))
    return -1; 

  // For easy reference
  const std::type_info* type = array_schema->coords_type();

  // Get vector of attribute ids
  std::vector<std::string> attribute_names_vec;
  for(int i=0; i<attribute_names_num; ++i)
    attribute_names_vec.push_back(attribute_names[i]);
  std::vector<int> attribute_ids;
  if(array_schema->get_attribute_ids(attribute_names_vec, attribute_ids))
    return -1; 

  // Initialize cell iterator, templating on the coordinates type
  if(type == &typeid(int)) {
    cell_it->it_ =
        tiledb_ctx->storage_manager_->rbegin<int>(ad, attribute_ids);
    if(cell_it->it_ == NULL) 
      return -1;
    cell_it->array_schema_ =
        ((ArrayConstReverseCellIterator<int>*) cell_it->it_)->array_schema();
  } else if(type == &typeid(int64_t)) {
    cell_it->it_ =
        tiledb_ctx->storage_manager_->rbegin<int64_t>(ad, attribute_ids);
    if(cell_it->it_ == NULL) 
      return -1;
    cell_it->array_schema_ =
        ((ArrayConstReverseCellIterator<int64_t>*)
            cell_it->it_)->array_schema();
  } else if(type == &typeid(float)) {
    cell_it->it_ =
        tiledb_ctx->storage_manager_->rbegin<float>(ad, attribute_ids);
    if(cell_it->it_ == NULL) 
      return -1;
    cell_it->array_schema_ =
        ((ArrayConstReverseCellIterator<float>*) cell_it->it_)->array_schema();
  } else if(type == &typeid(double)) {
    cell_it->it_ =
        tiledb_ctx->storage_manager_->rbegin<double>(ad, attribute_ids);
    if(cell_it->it_ == NULL) 
      return -1;
    cell_it->array_schema_ =
        ((ArrayConstReverseCellIterator<double>*) cell_it->it_)->array_schema();
  } else {
    PRINT_ERROR("Cannot initialize iterator - Invalid coordinates type");
    return -1;
  }
 
  // Success
  return 0;
}

int tiledb_const_reverse_cell_iterator_in_range_init(
    TileDB_CTX* tiledb_ctx,
    int ad,
    const char** attribute_names,
    int attribute_names_num,
    const void* range,
    TileDB_ConstReverseCellIterator*& cell_it) {
  // Sanity checks
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot initialize iterator - Invalid TileDB context");
    return -1;
  }
  if(attribute_names_num >= 0) {
    PRINT_ERROR("Cannot initialize iterator - Invalid number of attributes");
    return -1;
  }
  if((attribute_names_num > 0 && attribute_names == NULL) ||
     (attribute_names_num == 0 && attribute_names != NULL)) {
    PRINT_ERROR("Cannot initialize iterator - Discrepancy between attribute "
                "names and their number");
    return -1;
  }
  if(range == NULL) {
    PRINT_ERROR("Cannot initialize iterator - Invalid range");
    return -1;
  }

  // Initialize cell_it
  cell_it = (TileDB_ConstReverseCellIterator*)
             malloc(sizeof(struct TileDB_ConstReverseCellIterator));
  if(cell_it == NULL) {
    PRINT_ERROR("Cannot initialize iterator - Failed to allocate memory space "
                "for the iterator");
    return -1;
  }
  cell_it->no_cell_fetched_yet_ = true;

  // Get array schema
  const ArraySchema* array_schema;
  if(tiledb_ctx->storage_manager_->array_schema_get(
         ad, array_schema))
    return -1; 

  // For easy reference
  const std::type_info* type = array_schema->coords_type();

  // Get vector of attribute ids
  std::vector<std::string> attribute_names_vec;
  for(int i=0; i<attribute_names_num; ++i)
    attribute_names_vec.push_back(attribute_names[i]);
  std::vector<int> attribute_ids;
  if(array_schema->get_attribute_ids(attribute_names_vec, attribute_ids))
    return -1; 

  // Initialize cell iterator, templating on the coordinate types
  if(type == &typeid(int)) {
    cell_it->it_ =
        tiledb_ctx->storage_manager_->rbegin<int>(
             ad, (const int*) range, attribute_ids);
    if(cell_it->it_ == NULL) 
      return -1;
    cell_it->array_schema_ =
        ((ArrayConstReverseCellIterator<int>*) cell_it->it_)->array_schema();
  } else if(type == &typeid(int64_t)) {
    cell_it->it_ =
        tiledb_ctx->storage_manager_->rbegin<int64_t>(
            ad, (const int64_t*) range, attribute_ids);
    if(cell_it->it_ == NULL) 
      return -1;
    cell_it->array_schema_ =
        ((ArrayConstReverseCellIterator<int64_t>*) 
              cell_it->it_)->array_schema();
  } else if(type == &typeid(float)) {
    cell_it->it_ =
        tiledb_ctx->storage_manager_->rbegin<float>(
            ad, (const float*) range, attribute_ids);
    if(cell_it->it_ == NULL) 
      return -1;
    cell_it->array_schema_ =
        ((ArrayConstReverseCellIterator<float>*) cell_it->it_)->array_schema();
  } else if(type == &typeid(double)) {
    cell_it->it_ =
        tiledb_ctx->storage_manager_->rbegin<double>(
            ad, (const double*) range, attribute_ids);
    if(cell_it->it_ == NULL) 
      return -1;
    cell_it->array_schema_ =
        ((ArrayConstReverseCellIterator<double>*) cell_it->it_)->array_schema();
  } else {
    PRINT_ERROR("Cannot initialize iterator - Invalid coordinates type");
    return -1;
  }

  // Success
  return 0;
}

int tiledb_const_dense_cell_iterator_init(
    TileDB_CTX* tiledb_ctx,
    int ad,
    const char** attribute_names,
    int attribute_names_num,
    TileDB_ConstDenseCellIterator*& cell_it) {
  // Sanity checks
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot initialize iterator - Invalid TileDB context");
    return -1;
  }
  if(attribute_names_num >= 0) {
    PRINT_ERROR("Cannot initialize iterator - Invalid number of attributes");
    return -1;
  }
  if((attribute_names_num > 0 && attribute_names == NULL) ||
     (attribute_names_num == 0 && attribute_names != NULL)) {
    PRINT_ERROR("Cannot initialize iterator - Discrepancy between attribute "
                "names and their number");
    return -1;
  }

  // Initialize cell_it
  cell_it = (TileDB_ConstDenseCellIterator*)
             malloc(sizeof(struct TileDB_ConstDenseCellIterator));
  if(cell_it == NULL) {
    PRINT_ERROR("Cannot initialize iterator - Failed to allocate memory space "
                "for the iterator");
    return -1;
  }
  cell_it->no_cell_fetched_yet_ = true;

  // Get array schema
  const ArraySchema* array_schema;
  if(tiledb_ctx->storage_manager_->array_schema_get(
         ad, array_schema))
    return -1; 

  // For easy reference
  const std::type_info* type = array_schema->coords_type();

  // Get vector of attribute ids
  std::vector<std::string> attribute_names_vec;
  for(int i=0; i<attribute_names_num; ++i)
    attribute_names_vec.push_back(attribute_names[i]);
  std::vector<int> attribute_ids;
  if(array_schema->get_attribute_ids(attribute_names_vec, attribute_ids))
    return -1; 

  // Initialize cell iterator, templating on the coordinates type
  if(type == &typeid(int)) {
    cell_it->it_ =
        tiledb_ctx->storage_manager_->begin_dense<int>(ad, attribute_ids);
    if(cell_it->it_ == NULL) 
      return -1;
    cell_it->array_schema_ =
        ((ArrayConstCellIterator<int>*) cell_it->it_)->array_schema();
  } else if(type == &typeid(int64_t)) {
    cell_it->it_ =
        tiledb_ctx->storage_manager_->begin_dense<int64_t>(ad, attribute_ids);
    if(cell_it->it_ == NULL) 
      return -1;
    cell_it->array_schema_ =
        ((ArrayConstCellIterator<int64_t>*) cell_it->it_)->array_schema();
  } else if(type == &typeid(float)) {
    cell_it->it_ =
        tiledb_ctx->storage_manager_->begin_dense<float>(ad, attribute_ids);
    if(cell_it->it_ == NULL) 
      return -1;
    cell_it->array_schema_ =
        ((ArrayConstCellIterator<float>*) cell_it->it_)->array_schema();
  } else if(type == &typeid(double)) {
    cell_it->it_ =
        tiledb_ctx->storage_manager_->begin_dense<double>(ad, attribute_ids);
    if(cell_it->it_ == NULL) 
      return -1;
    cell_it->array_schema_ =
        ((ArrayConstCellIterator<double>*) cell_it->it_)->array_schema();
  } else {
    PRINT_ERROR("Cannot initialize iterator - Invalid coordinates type");
    return -1;
  }
 
  // Success
  return 0;
}

int tiledb_const_dense_cell_iterator_in_range_init(
    TileDB_CTX* tiledb_ctx,
    int ad,
    const char** attribute_names,
    int attribute_names_num,
    const void* range,
    TileDB_ConstDenseCellIterator*& cell_it) {
  // Sanity checks
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot initialize iterator - Invalid TileDB context");
    return -1;
  }
  if(attribute_names_num >= 0) {
    PRINT_ERROR("Cannot initialize iterator - Invalid number of attributes");
    return -1;
  }
  if((attribute_names_num > 0 && attribute_names == NULL) ||
     (attribute_names_num == 0 && attribute_names != NULL)) {
    PRINT_ERROR("Cannot initialize iterator - Discrepancy between attribute "
                "names and their number");
    return -1;
  }
  if(range == NULL) {
    PRINT_ERROR("Cannot initialize iterator - Invalid range");
    return -1;
  }

  // Initialize cell_it
  cell_it = (TileDB_ConstDenseCellIterator*)
             malloc(sizeof(struct TileDB_ConstDenseCellIterator));
  if(cell_it == NULL) {
    PRINT_ERROR("Cannot initialize iterator - Failed to allocate memory space "
                "for the iterator");
    return -1;
  }
  cell_it->no_cell_fetched_yet_ = true;

  // Get array schema
  const ArraySchema* array_schema;
  if(tiledb_ctx->storage_manager_->array_schema_get(
         ad, array_schema))
    return -1; 

  // For easy reference
  const std::type_info* type = array_schema->coords_type();

  // Get vector of attribute ids
  std::vector<std::string> attribute_names_vec;
  for(int i=0; i<attribute_names_num; ++i)
    attribute_names_vec.push_back(attribute_names[i]);
  std::vector<int> attribute_ids;
  if(array_schema->get_attribute_ids(attribute_names_vec, attribute_ids))
    return -1; 

  // Initialize cell iterator, templating on the coordinates type
  if(type == &typeid(int)) {
    cell_it->it_ =
        tiledb_ctx->storage_manager_->begin_dense<int>(
             ad, (const int*) range, attribute_ids);
    if(cell_it->it_ == NULL) 
      return -1;
    cell_it->array_schema_ =
        ((ArrayConstCellIterator<int>*) cell_it->it_)->array_schema();
  } else if(type == &typeid(int64_t)) {
    cell_it->it_ =
        tiledb_ctx->storage_manager_->begin_dense<int64_t>(
            ad, (const int64_t*) range, attribute_ids);
    if(cell_it->it_ == NULL) 
      return -1;
    cell_it->array_schema_ =
        ((ArrayConstCellIterator<int64_t>*) cell_it->it_)->array_schema();
  } else if(type == &typeid(float)) {
    cell_it->it_ =
        tiledb_ctx->storage_manager_->begin_dense<float>(
            ad, (const float*) range, attribute_ids);
    if(cell_it->it_ == NULL) 
      return -1;
    cell_it->array_schema_ =
        ((ArrayConstCellIterator<float>*) cell_it->it_)->array_schema();
  } else if(type == &typeid(double)) {
    cell_it->it_ =
        tiledb_ctx->storage_manager_->begin_dense<double>(
            ad, (const double*) range, attribute_ids);
    if(cell_it->it_ == NULL) 
      return -1;
    cell_it->array_schema_ =
        ((ArrayConstCellIterator<double>*) cell_it->it_)->array_schema();
  } else {
    PRINT_ERROR("Cannot initialize iterator - Invalid coordinates type");
    return -1;
  }

  // Success
  return 0;
}

int tiledb_const_reverse_dense_cell_iterator_init(
    TileDB_CTX* tiledb_ctx,
    int ad,
    const char** attribute_names,
    int attribute_names_num,
    TileDB_ConstDenseCellIterator*& cell_it) {
  // Sanity checks
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot initialize iterator - Invalid TileDB context");
    return -1;
  }
  if(attribute_names_num >= 0) {
    PRINT_ERROR("Cannot initialize iterator - Invalid number of attributes");
    return -1;
  }
  if((attribute_names_num > 0 && attribute_names == NULL) ||
     (attribute_names_num == 0 && attribute_names != NULL)) {
    PRINT_ERROR("Cannot initialize iterator - Discrepancy between attribute "
                "names and their number");
    return -1;
  }

  // TODO
  PRINT_ERROR("Iterator not supported yet");
  exit(-1);
}

int tiledb_const_reverse_dense_cell_iterator_in_range_init(
    TileDB_CTX* tiledb_ctx,
    int ad,
    const char** attribute_names,
    int attribute_names_num,
    const void* range,
    TileDB_ConstDenseCellIterator*& cell_it) {
  // Sanity checks
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot initialize iterator - Invalid TileDB context");
    return -1;
  }
  if(attribute_names_num >= 0) {
    PRINT_ERROR("Cannot initialize iterator - Invalid number of attributes");
    return -1;
  }
  if((attribute_names_num > 0 && attribute_names == NULL) ||
     (attribute_names_num == 0 && attribute_names != NULL)) {
    PRINT_ERROR("Cannot initialize iterator - Discrepancy between attribute "
                "names and their number");
    return -1;
  }
  if(range == NULL) {
    PRINT_ERROR("Cannot initialize iterator - Invalid range");
    return -1;
  }

  // TODO
  PRINT_ERROR("Iterator not supported yet");
  exit(-1);
}

/* Auxiliary templated function for tiledb_const_cell_iterator_next. */
template<class T>
int const_cell_iterator_next(
    ArrayConstCellIterator<T>* it,
    bool& no_cell_fetched_yet,
    const void*& cell) {
  // Handle the case just before initialization
  if(!no_cell_fetched_yet) {
    if(++(*it))
      return -1;
  } else {
    no_cell_fetched_yet = false;
  }

  // Get next cell 
  if(it->end()) {
    cell = NULL;
  } else {
    cell = **it;
    if(cell == NULL) 
      return -1;
  }
  
  // Success
  return 0;
}

int tiledb_const_cell_iterator_next(
    TileDB_ConstCellIterator* cell_it,
    const void*& cell) {
  // Sanity check
  if(cell_it == NULL) {
    PRINT_ERROR("Cannot get next cell - Invalid iterator");
    return -1;
  }

  // For easy reference
  const ArraySchema* array_schema = cell_it->array_schema_;
  const std::type_info* type = array_schema->coords_type();

  // Get next cell, templating on the coordinates type
  if(type == &typeid(int)) {
    const_cell_iterator_next<int>(
        (ArrayConstCellIterator<int>*) cell_it->it_,
        cell_it->no_cell_fetched_yet_,
        cell);
  } else if(type == &typeid(int64_t)) {
    const_cell_iterator_next<int64_t>(
        (ArrayConstCellIterator<int64_t>*) cell_it->it_,
        cell_it->no_cell_fetched_yet_,
        cell);
  } else if(type == &typeid(float)) {
    const_cell_iterator_next<float>(
        (ArrayConstCellIterator<float>*) cell_it->it_,
        cell_it->no_cell_fetched_yet_,
        cell);
  } else if(type == &typeid(double)) {
    const_cell_iterator_next<double>(
        (ArrayConstCellIterator<double>*) cell_it->it_,
        cell_it->no_cell_fetched_yet_,
        cell);
  }

  // Success
  return 0;
}

/* Auxiliary templated function for tiledb_const_reverse_cell_iterator_next. */
template<class T>
int const_reverse_cell_iterator_next(
    ArrayConstReverseCellIterator<T>* it,
    bool& no_cell_fetched_yet,
    const void*& cell) {
  // Handle the case just before initialization
  if(!no_cell_fetched_yet) {
    if(++(*it))
      return -1;
  } else {
    no_cell_fetched_yet = false;
  }

  // Get next cell 
  if(it->end()) {
    cell = NULL;
  } else {
    cell = **it;
    if(cell == NULL) 
      return -1;
  }
  
  // Success
  return 0;
}

int tiledb_const_reverse_cell_iterator_next(
    TileDB_ConstReverseCellIterator* cell_it,
    const void*& cell) {
  // Sanity check
  if(cell_it == NULL) {
    PRINT_ERROR("Cannot get next cell - Invalid iterator");
    return -1;
  }

  // For easy reference
  const ArraySchema* array_schema = cell_it->array_schema_;
  const std::type_info* type = array_schema->coords_type();

  // Get next cell, templating on the coordinates type
  if(type == &typeid(int)) {
    const_reverse_cell_iterator_next<int>(
        (ArrayConstReverseCellIterator<int>*) cell_it->it_,
        cell_it->no_cell_fetched_yet_,
        cell);
  } else if(type == &typeid(int64_t)) {
    const_reverse_cell_iterator_next<int64_t>(
        (ArrayConstReverseCellIterator<int64_t>*) cell_it->it_,
        cell_it->no_cell_fetched_yet_,
        cell);
  } else if(type == &typeid(float)) {
    const_reverse_cell_iterator_next<float>(
        (ArrayConstReverseCellIterator<float>*) cell_it->it_,
        cell_it->no_cell_fetched_yet_,
        cell);
  } else if(type == &typeid(double)) {
    const_reverse_cell_iterator_next<double>(
        (ArrayConstReverseCellIterator<double>*) cell_it->it_,
        cell_it->no_cell_fetched_yet_,
        cell);
  }

  // Success
  return 0;
}

/* Auxiliary templated function for tiledb_const_dense_cell_iterator_next. */
template<class T>
int const_dense_cell_iterator_next(
    ArrayConstDenseCellIterator<T>* it,
    bool& no_cell_fetched_yet,
    const void*& cell) {
  // Handle the case just before initialization
  if(!no_cell_fetched_yet) {
    if(++(*it))
      return -1;
  } else {
    no_cell_fetched_yet = false;
  }

  // Get next cell 
  if(it->end()) {
    cell = NULL;
  } else {
    cell = **it;
    if(cell == NULL) 
      return -1;
  }
  
  // Success
  return 0;
}

int tiledb_const_dense_cell_iterator_next(
    TileDB_ConstDenseCellIterator* cell_it,
    const void*& cell) {
  // Sanity check
  if(cell_it == NULL) {
    PRINT_ERROR("Cannot get next cell - Invalid iterator");
    return -1;
  }

  // For easy reference
  const ArraySchema* array_schema = cell_it->array_schema_;
  const std::type_info* type = array_schema->coords_type();

  // Get next cell, templating on the coordinates type
  if(type == &typeid(int)) {
    const_dense_cell_iterator_next<int>(
        (ArrayConstDenseCellIterator<int>*) cell_it->it_,
        cell_it->no_cell_fetched_yet_,
        cell);
  } else if(type == &typeid(int64_t)) {
    const_dense_cell_iterator_next<int64_t>(
        (ArrayConstDenseCellIterator<int64_t>*) cell_it->it_,
        cell_it->no_cell_fetched_yet_,
        cell);
  } else if(type == &typeid(float)) {
    const_dense_cell_iterator_next<float>(
        (ArrayConstDenseCellIterator<float>*) cell_it->it_,
        cell_it->no_cell_fetched_yet_,
        cell);
  } else if(type == &typeid(double)) {
    const_dense_cell_iterator_next<double>(
        (ArrayConstDenseCellIterator<double>*) cell_it->it_,
        cell_it->no_cell_fetched_yet_,
        cell);
  }

  // Success
  return 0;
}

int tiledb_const_reverse_dense_cell_iterator_next(
    TileDB_ConstDenseCellIterator* cell_it,
    const void*& cell) {
  // Sanity check
  if(cell_it == NULL) {
    PRINT_ERROR("Cannot get next cell - Invalid iterator");
    return -1;
  }

  // TODO
  PRINT_ERROR("Const reverse dense cell iterator not supported yet");
  exit(-1);
}

/* ****************************** */
/*             QUERIES            */
/* ****************************** */

int tiledb_array_clear_2(
    const TileDB_CTX* tiledb_ctx,
    const char* workspace,
    const char* group,
    const char* array_name) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot clear array: Invalid TileDB context");
    return -1;
  }

  return tiledb_ctx->storage_manager_->array_clear(
                                           workspace, group, array_name);
}









int tiledb_array_consolidate_2(
    const TileDB_CTX* tiledb_ctx,
    const char* workspace,
    const char* group,
    const char* array_name) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot consolidate array: Invalid TileDB context");
    return -1;
  }

  return tiledb_ctx->storage_manager_->array_consolidate(
             workspace, group, array_name); 
}



int tiledb_array_define(
    const TileDB_CTX* tiledb_ctx,
    const char* workspace,
    const char* group,
    const char* array_schema_str) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot define array: Invalid TileDB context");
    return -1;
  }

  // Create an array schema from the input string
  ArraySchema* array_schema = new ArraySchema();
  if(array_schema->deserialize_csv(array_schema_str)) {
    delete array_schema;
    return -1;
  }

  // Define the array
  if(tiledb_ctx->storage_manager_->array_schema_store(
        workspace, group, array_schema)) {
    delete array_schema;
    return -1;
  }

  // Clean up
  delete array_schema;

  // Success
  return 0;
}

int tiledb_array_delete_2(
    const TileDB_CTX* tiledb_ctx,
    const char* workspace,
    const char* group,
    const char* array_name) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot delete array: Invalid TileDB context");
    return -1;
  }

  return tiledb_ctx->storage_manager_->array_delete(
                                           workspace, group, array_name);
}

int tiledb_array_export(
    const TileDB_CTX* tiledb_ctx,
    const char* workspace,
    const char* group,
    const char* array_name,
    const char* filename,
    const char* format,
    const char** dim_names,
    int dim_names_num,
    const char** attribute_names,
    int attribute_names_num,
    double* range,
    int range_size,
    char delimiter,
    int precision) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot export array: Invalid TileDB context");
    return -1;
  }

  // Compute vectors for dim_names, attribute names and range
  std::vector<std::string> dim_names_vec, attribute_names_vec;
  std::vector<double> range_vec;
  for(int i=0; i<dim_names_num; ++i)
    dim_names_vec.push_back(dim_names[i]);
  for(int i=0; i<attribute_names_num; ++i)
    attribute_names_vec.push_back(attribute_names[i]);
  for(int i=0; i<range_size; ++i)
    range_vec.push_back(range[i]);

  return tiledb_ctx->query_processor_->array_export(
      workspace, group, array_name, filename, format,
      dim_names_vec, attribute_names_vec, range_vec,
      delimiter, precision);
}

int tiledb_dataset_generate(
    const TileDB_CTX* tiledb_ctx,
    const char* workspace,
    const char* group,
    const char* array_name,
    const char* filename,
    const char* format,
    unsigned int seed,
    int64_t cell_num,
    char delimiter) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot generate dataset: Invalid TileDB context");
    return -1;
  }

  // Get the array schema from the storage manager
  ArraySchema* array_schema;
  if(tiledb_ctx->storage_manager_->array_schema_get(
           workspace, group, array_name, array_schema))
    return -1;

  // Generate the file
  int rc;
  DataGenerator data_generator(array_schema);
  if(!strcmp(format, "csv")) {
    rc = data_generator.generate_csv(seed, filename, cell_num, 
                                     CMP_NONE, delimiter);
  } else if(!strcmp(format, "csv.gz")) {
    rc = data_generator.generate_csv(seed, filename, cell_num,
                                     CMP_GZIP, delimiter);
  } else if(!strcmp(format, "bin")) {
    rc = data_generator.generate_bin(seed, filename, cell_num,
                                     CMP_NONE);
  } else if(!strcmp(format, "bin.gz")) {
    rc = data_generator.generate_bin(seed, filename, cell_num,
                                     CMP_GZIP);
  } else {
    PRINT_ERROR("Unknown format");
    rc = -1;
  }

  // Clean up
  delete array_schema;

  // Return 0 for success and -1 for failure
  return rc;
}

int tiledb_array_is_defined(
    const TileDB_CTX* tiledb_ctx,
    const char* workspace,
    const char* group,
    const char* array_name) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot check array: Invalid TileDB context");
    return -1;
  }

  bool is_def = tiledb_ctx->storage_manager_->array_is_defined(
                    workspace, group, array_name);
  return is_def ? 1 : 0;
}

int tiledb_array_load(
    const TileDB_CTX* tiledb_ctx,
    const char* workspace,
    const char* group,
    const char* array_name,
    const char* path,
    const char* format,
    char delimiter) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot load into array: Invalid TileDB context");
    return -1;
  }

  return tiledb_ctx->loader_->array_load(
             workspace, group, array_name, path, format, delimiter);
}

int tiledb_array_schema_get(
    const TileDB_CTX* tiledb_ctx, 
    const char* workspace, 
    const char* group, 
    const char* array_name, 
    char* array_schema_c_str,
    size_t* array_schema_c_str_length) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot get array schema: Invalid TileDB context");
    return -1;
  }
    
  // Get the array schema from the storage manager
  ArraySchema* array_schema;
  if(tiledb_ctx->storage_manager_->array_schema_get(
         workspace, group, array_name, array_schema))
    return -1;
  
  // Serialize array schema
  std::string array_schema_csv = array_schema->serialize_csv();
  size_t array_schema_csv_length = array_schema_csv.size() + 1;   
  if(*array_schema_c_str_length < array_schema_csv_length) {
    *array_schema_c_str_length = array_schema_csv_length;
  } else {
    *array_schema_c_str_length = array_schema_csv_length;
    memcpy(array_schema_c_str, 
           array_schema_csv.c_str(),  
           array_schema_csv_length);
  }

  // Clean up
  delete array_schema;

  // Success
  return 0;
}

int tiledb_array_schema_show(
    const TileDB_CTX* tiledb_ctx,
    const char* workspace,
    const char* group,
    const char* array_name) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot show array schema: Invalid TileDB context");
    return -1;
  }

  // Get the array schema from the storage manager
  ArraySchema* array_schema;
  if(tiledb_ctx->storage_manager_->array_schema_get(
         workspace, group, array_name, array_schema)) 
    return -1;

  // Print array schema
  array_schema->print();

  // Clean up
  delete array_schema;

  // Success
  return 0;
}

int tiledb_subarray(
    const TileDB_CTX* tiledb_ctx,
    const char* workspace,
    const char* workspace_sub,
    const char* group,
    const char* group_sub,
    const char* array_name,
    const char* array_name_sub,
    const double* range,
    int range_size,
    const char** attribute_names,
    int attribute_names_num) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot compute subarray: Invalid TileDB context");
    return -1;
  }

  // Compute vectors for range and attribute names
  std::vector<std::string> attribute_names_vec;
  for(int i=0; i<attribute_names_num; ++i)
    attribute_names_vec.push_back(attribute_names[i]);
  std::vector<double> range_vec;
  for(int i=0; i<range_size; ++i)
    range_vec.push_back(range[i]);

  // Perform the subarray query
  return tiledb_ctx->query_processor_->subarray(
      workspace, workspace_sub, group, group_sub,
      array_name, array_name_sub, 
      range_vec, attribute_names_vec);
}

int tiledb_array_update(
    const TileDB_CTX* tiledb_ctx,
    const char* workspace,
    const char* group,
    const char* array_name,
    const char* path,
    const char* format,
    char delimiter,
    int consolidate) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot compute subarray: Invalid TileDB context");
    return -1;
  }

  // Create a new fragment
  if(tiledb_ctx->loader_->array_load(workspace, group, array_name, path,
                                     format, delimiter, true))
    return -1;

  // Potentially consolidate fragments
  if(consolidate)
    if(tiledb_array_consolidate_2(tiledb_ctx, workspace, group, array_name))
      return -1;

  // Success
  return 0;
}

/* ****************************** */
/*             MISC               */
/* ****************************** */

const char* tiledb_version() {
  // Simply return a constant with the TileDB version information
  return TILEDB_VERSION;
}
