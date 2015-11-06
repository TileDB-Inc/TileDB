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
 * This file defines the C APIs of TileDB.
 */

#include "data_generator.h"
#include "loader.h"
#include "query_processor.h"
#include "storage_manager.h"
#include "tiledb.h"
#include <cassert>

// Macro for printing error message in VERBOSE mode
#ifdef VERBOSE
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

int tiledb_ctx_finalize(TileDB_CTX* tiledb_ctx) {
  // Trivial case
  if(tiledb_ctx == NULL)
    return 0;

  // Sanity checks
  assert(tiledb_ctx->storage_manager_ &&
         tiledb_ctx->loader_ &&
         tiledb_ctx->query_processor_);

  // Clear the TileDB modules
  if(tiledb_ctx->storage_manager_->finalize())
    return -1;
  delete tiledb_ctx->storage_manager_;

  if(tiledb_ctx->loader_->finalize())
    return -1;
  delete tiledb_ctx->loader_;

  if(tiledb_ctx->query_processor_->finalize())
    return -1;
  delete tiledb_ctx->query_processor_;

  // Delete the TileDB context
  free(tiledb_ctx);

  // Success
  return 0;
}

int tiledb_ctx_init(TileDB_CTX*& tiledb_ctx) {
  // Initialize context
  tiledb_ctx = (TileDB_CTX*) malloc(sizeof(struct TileDB_CTX));
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot initialize TileDB context - Failed to allocate memory "
                "space for the context");
    return -1;
  }

  // Create modules
  tiledb_ctx->storage_manager_ = new StorageManager();
  if(!tiledb_ctx->storage_manager_->created_successfully())
    return -1;
  tiledb_ctx->loader_ = new Loader(tiledb_ctx->storage_manager_);
  if(!tiledb_ctx->loader_->created_successfully())
    return -1;
  tiledb_ctx->query_processor_ =
      new QueryProcessor(tiledb_ctx->storage_manager_);
  if(!tiledb_ctx->query_processor_->created_successfully())
    return -1;

  // Success
  return 0;
}

/* ****************************** */
/*               I/O              */
/* ****************************** */

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

int tiledb_array_close(TileDB_CTX* tiledb_ctx, int ad) {
  // Sanity check
  if(tiledb_ctx == NULL) {
    PRINT_ERROR("Cannot close array - Invalid TileDB context");
    return -1;
  }

  // Close array
  return tiledb_ctx->storage_manager_->array_close(ad);
}

int tiledb_array_open(
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

int tiledb_array_clear(
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

int tiledb_array_consolidate(
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

int tiledb_array_delete(
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
    if(tiledb_array_consolidate(tiledb_ctx, workspace, group, array_name))
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
