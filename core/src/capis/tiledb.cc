/**
 * @file   tiledb.cc
 * @author Stavros Papadopoulos <stavrosp@csail.mit.edu>
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * Copyright (c) 2014 Stavros Papadopoulos <stavrosp@csail.mit.edu>
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
#include "tiledb_error.h"
#include <cassert>

/* ****************************** */
/*            CONTEXT             */
/* ****************************** */

typedef struct TileDB_CTX{
  std::string* workspace_;
  Loader* loader_;
  QueryProcessor* query_processor_;
  StorageManager* storage_manager_;
} TileDB_CTX;

int tiledb_ctx_finalize(TileDB_CTX* tiledb_ctx) {

  if(tiledb_ctx == NULL)
      return TILEDB_OK;

  assert(tiledb_ctx->storage_manager_ &&
         tiledb_ctx->loader_ &&
         tiledb_ctx->query_processor_ &&
         tiledb_ctx->workspace_);

  // Clear the TileDB modules
  delete tiledb_ctx->storage_manager_;
  delete tiledb_ctx->loader_;
  delete tiledb_ctx->query_processor_;
  delete tiledb_ctx->workspace_;

  // Delete the TileDB context
  free(tiledb_ctx);

  return TILEDB_OK;
}

int tiledb_ctx_init(const char* workspace, TileDB_CTX*& tiledb_ctx) {

  assert(workspace);

  tiledb_ctx = (TileDB_CTX*) malloc(sizeof(struct TileDB_CTX));

  // Save the workspace path
  tiledb_ctx-> workspace_ = new std::string(workspace);

  // Create Storage Manager
  tiledb_ctx->storage_manager_ = new StorageManager(workspace);
  if(tiledb_ctx->storage_manager_->err()) {
    delete tiledb_ctx->storage_manager_;
    free(tiledb_ctx);
    std::cerr << ERROR_MSG_HEADER << " Cannot create storage manager.\n";
    return TILEDB_ENSMCREAT;
  }

  // Create Loader
  tiledb_ctx->loader_ =
      new Loader(tiledb_ctx->storage_manager_, workspace);
  if(tiledb_ctx->loader_->err()) {
    delete tiledb_ctx->storage_manager_;
    delete tiledb_ctx->loader_;
    free(tiledb_ctx);
    std::cerr << ERROR_MSG_HEADER << " Cannot create loader.\n";
    return TILEDB_ENLDCREAT;
  }

  // Create Query Processor
  tiledb_ctx->query_processor_ =
      new QueryProcessor(tiledb_ctx->storage_manager_);
  if(tiledb_ctx->query_processor_->err()) {
    delete tiledb_ctx->storage_manager_;
    delete tiledb_ctx->loader_;
    delete tiledb_ctx->query_processor_;
    free(tiledb_ctx);
    std::cerr << ERROR_MSG_HEADER << " Cannot create query processor.\n";
    return TILEDB_ENQPCREAT;
  }

  return TILEDB_OK;
}

const char* tiledb_ctx_workspace(TileDB_CTX* tiledb_ctx) {
    assert(tiledb_ctx);
    return tiledb_ctx->workspace_->c_str();
}

/* ****************************** */
/*               I/O              */
/* ****************************** */

int tiledb_array_close(TileDB_CTX* tiledb_ctx, int ad) {

  assert(tiledb_ctx);
  assert(ad >= 0);

  // TODO: Error messages here
  tiledb_ctx->storage_manager_->close_array(ad);
  return TILEDB_OK;
}

int tiledb_array_open(
    TileDB_CTX* tiledb_ctx,
    const char* array_name,
    const char* mode) {

  assert(tiledb_ctx && array_name && mode);

  return tiledb_ctx->storage_manager_->open_array(array_name, mode);
}

int tiledb_write_cell(TileDB_CTX* tiledb_ctx, int ad, const void* cell) {

  assert(tiledb_ctx);
  assert(ad >= 0);

  // For easy reference
  int err;
  const ArraySchema* array_schema;
  err = tiledb_ctx->storage_manager_->get_array_schema(
            ad, array_schema);
  if(err == -1)
    return -1; // TODO: Error

  const std::type_info* type = array_schema->coords_type();

  if(type == &typeid(int))
    tiledb_ctx->storage_manager_->write_cell<int>(ad, cell);
  else if(type == &typeid(int64_t))
    tiledb_ctx->storage_manager_->write_cell<int64_t>(ad, cell);
  else if(type == &typeid(float))
    tiledb_ctx->storage_manager_->write_cell<float>(ad, cell);
  else if(type == &typeid(double))
    tiledb_ctx->storage_manager_->write_cell<double>(ad, cell);
  else
      return -1; // TODO: Error

  return TILEDB_OK;
}

int tiledb_write_cell_sorted(TileDB_CTX* tiledb_ctx, int ad, const void* cell) {

  assert(tiledb_ctx);
  assert(ad >= 0);

  // For easy reference
  int err;
  const ArraySchema* array_schema;
  err = tiledb_ctx->storage_manager_->get_array_schema(
            ad, array_schema);
  if(err == -1)
    return -1; // TODO: Error

  const std::type_info* type = array_schema->coords_type();

  if(type == &typeid(int))
    tiledb_ctx->storage_manager_->write_cell_sorted<int>(ad, cell);
  else if(type == &typeid(int64_t))
    tiledb_ctx->storage_manager_->write_cell_sorted<int64_t>(ad, cell);
  else if(type == &typeid(float))
    tiledb_ctx->storage_manager_->write_cell_sorted<float>(ad, cell);
  else if(type == &typeid(double))
    tiledb_ctx->storage_manager_->write_cell_sorted<double>(ad, cell);
  else
      return -1; // TODO: Error

  return TILEDB_OK;
}


/* ****************************** */
/*          CELL ITERATORS        */
/* ****************************** */

typedef struct TileDB_ConstCellIterator {
  const ArraySchema* array_schema_;
  void* it_;
  bool begin_;
} TileDB_ConstCellIterator;

typedef struct TileDB_ConstReverseCellIterator {
  const ArraySchema* array_schema_;
  void* it_;
  bool begin_;
} TileDB_ConstReverseCellIterator;

int tiledb_const_cell_iterator_finalize(
    TileDB_ConstCellIterator* cell_it) {

  const std::type_info* type = cell_it->array_schema_->coords_type();

  if(type == &typeid(int))
    delete (ArrayConstCellIterator<int>*) cell_it->it_;
  else if(type == &typeid(int64_t))
    delete (ArrayConstCellIterator<int64_t>*) cell_it->it_;
  else if(type == &typeid(float))
    delete (ArrayConstCellIterator<float>*) cell_it->it_;
  else if(type == &typeid(double))
    delete (ArrayConstCellIterator<double>*) cell_it->it_;

  free(cell_it);

  return TILEDB_OK;
}

int tiledb_const_reverse_cell_iterator_finalize(
    TileDB_ConstReverseCellIterator* cell_it) {
  const std::type_info* type = cell_it->array_schema_->coords_type();

  if(type == &typeid(int))
    delete (ArrayConstReverseCellIterator<int>*) cell_it->it_;
  else if(type == &typeid(int64_t))
    delete (ArrayConstReverseCellIterator<int64_t>*) cell_it->it_;
  else if(type == &typeid(float))
    delete (ArrayConstReverseCellIterator<float>*) cell_it->it_;
  else if(type == &typeid(double))
    delete (ArrayConstReverseCellIterator<double>*) cell_it->it_;

  free(cell_it);

  return TILEDB_OK;
}

int tiledb_const_cell_iterator_init(
    TileDB_CTX* tiledb_ctx,
    int ad,
    const char** attribute_names,
    int attribute_names_num,
    TileDB_ConstCellIterator*& cell_it) {

  assert(tiledb_ctx);
  assert(ad >= 0);
  assert(attribute_names_num >= 0);

  // Initialize cell_it
  cell_it = (TileDB_ConstCellIterator*)
             malloc(sizeof(struct TileDB_ConstCellIterator));
  cell_it->begin_ = true;

  // For easy reference
  int err;
  const ArraySchema* array_schema;
  err = tiledb_ctx->storage_manager_->get_array_schema(
            ad, array_schema);
  if(err == -1)
    return -1; // TODO
  const std::type_info* type = array_schema->coords_type();

  // Get vector of attribute ids
  std::vector<std::string> attribute_names_vec;
  for(int i=0; i<attribute_names_num; ++i)
    attribute_names_vec.push_back(attribute_names[i]);
  std::vector<int> attribute_ids;
  err = array_schema->get_attribute_ids(attribute_names_vec, attribute_ids);
  if(err == -1)
    return -1; // TODO

  // Initialize the proper templated cell iterator
  // TODO: Error messages here !!!
  if(type == &typeid(int)) {
    cell_it->it_ =
        tiledb_ctx->storage_manager_->begin<int>(ad, attribute_ids);
    cell_it->array_schema_ =
        ((ArrayConstCellIterator<int>*) cell_it->it_)->array_schema();
  } else if(type == &typeid(int64_t)) {
    cell_it->it_ =
        tiledb_ctx->storage_manager_->begin<int64_t>(ad, attribute_ids);
    cell_it->array_schema_ =
        ((ArrayConstCellIterator<int64_t>*) cell_it->it_)->array_schema();
  } else if(type == &typeid(float)) {
    cell_it->it_ =
        tiledb_ctx->storage_manager_->begin<float>(ad, attribute_ids);
    cell_it->array_schema_ =
        ((ArrayConstCellIterator<float>*) cell_it->it_)->array_schema();
  } else if(type == &typeid(double)) {
    cell_it->it_ =
        tiledb_ctx->storage_manager_->begin<double>(ad, attribute_ids);
    cell_it->array_schema_ =
        ((ArrayConstCellIterator<double>*) cell_it->it_)->array_schema();
  }

  return TILEDB_OK;
}

int tiledb_const_cell_iterator_init_in_range(
    TileDB_CTX* tiledb_ctx,
    int ad,
    const char** attribute_names,
    int attribute_names_num,
    const void* range,
    TileDB_ConstCellIterator*& cell_it) {

  assert(tiledb_ctx);
  assert(ad >= 0);
  assert(attribute_names_num >= 0);

  // Initialize cell_it
  cell_it = (TileDB_ConstCellIterator*)
             malloc(sizeof(struct TileDB_ConstCellIterator));
  cell_it->begin_ = true;

  // For easy reference
  int err;
  const ArraySchema* array_schema;
  err = tiledb_ctx->storage_manager_->get_array_schema(
            ad, array_schema);
  if(err == -1)
    return -1; // TODO
  const std::type_info* type = array_schema->coords_type();

  // Get vector of attribute ids
  std::vector<std::string> attribute_names_vec;
  for(int i=0; i<attribute_names_num; ++i)
    attribute_names_vec.push_back(attribute_names[i]);
  std::vector<int> attribute_ids;
  err = array_schema->get_attribute_ids(attribute_names_vec, attribute_ids);
  if(err == -1)
    return -1; // TODO

  // Initialize the proper templated cell iterator
  // TODO: Error messages here !!!
  if(type == &typeid(int)) {
    cell_it->it_ =
        tiledb_ctx->storage_manager_->begin<int>(
             ad, (const int*) range, attribute_ids);
    cell_it->array_schema_ =
        ((ArrayConstCellIterator<int>*) cell_it->it_)->array_schema();
  } else if(type == &typeid(int64_t)) {
    cell_it->it_ =
        tiledb_ctx->storage_manager_->begin<int64_t>(
            ad, (const int64_t*) range, attribute_ids);
    cell_it->array_schema_ =
        ((ArrayConstCellIterator<int64_t>*) cell_it->it_)->array_schema();
  } else if(type == &typeid(float)) {
    cell_it->it_ =
        tiledb_ctx->storage_manager_->begin<float>(
            ad, (const float*) range, attribute_ids);
    cell_it->array_schema_ =
        ((ArrayConstCellIterator<float>*) cell_it->it_)->array_schema();
  } else if(type == &typeid(double)) {
    cell_it->it_ =
        tiledb_ctx->storage_manager_->begin<double>(
            ad, (const double*) range, attribute_ids);
    cell_it->array_schema_ =
        ((ArrayConstCellIterator<double>*) cell_it->it_)->array_schema();
  }

  return TILEDB_OK;
}

int tiledb_const_reverse_cell_iterator_init(
    TileDB_CTX* tiledb_ctx,
    int ad,
    const char** attribute_names,
    int attribute_names_num,
    TileDB_ConstReverseCellIterator*& cell_it) {

  assert(tiledb_ctx);
  assert(ad >= 0);
  assert(attribute_names_num >= 0);

  // Initialize cell_it
  cell_it = (TileDB_ConstReverseCellIterator*)
             malloc(sizeof(struct TileDB_ConstReverseCellIterator));
  cell_it->begin_ = true;

  // For easy reference
  int err;
  const ArraySchema* array_schema;
  err = tiledb_ctx->storage_manager_->get_array_schema(
            ad, array_schema);
  if(err == -1)
    return -1; // TODO
  const std::type_info* type = array_schema->coords_type();

  // Get vector of attribute ids
  std::vector<std::string> attribute_names_vec;
  for(int i=0; i<attribute_names_num; ++i)
    attribute_names_vec.push_back(attribute_names[i]);
  std::vector<int> attribute_ids;
  err = array_schema->get_attribute_ids(attribute_names_vec, attribute_ids);
  if(err == -1)
    return -1; // TODO

  // Initialize the proper templated cell iterator
  // TODO: Error messages here !!!
  if(type == &typeid(int)) {
    cell_it->it_ =
        tiledb_ctx->storage_manager_->rbegin<int>(ad, attribute_ids);
    cell_it->array_schema_ =
        ((ArrayConstReverseCellIterator<int>*) cell_it->it_)->array_schema();
  } else if(type == &typeid(int64_t)) {
    cell_it->it_ =
        tiledb_ctx->storage_manager_->rbegin<int64_t>(ad, attribute_ids);
    cell_it->array_schema_ =
        ((ArrayConstReverseCellIterator<int64_t>*)
            cell_it->it_)->array_schema();
  } else if(type == &typeid(float)) {
    cell_it->it_ =
        tiledb_ctx->storage_manager_->rbegin<float>(ad, attribute_ids);
    cell_it->array_schema_ =
        ((ArrayConstReverseCellIterator<float>*) cell_it->it_)->array_schema();
  } else if(type == &typeid(double)) {
    cell_it->it_ =
        tiledb_ctx->storage_manager_->rbegin<double>(ad, attribute_ids);
    cell_it->array_schema_ =
        ((ArrayConstReverseCellIterator<double>*) cell_it->it_)->array_schema();
  }

  return TILEDB_OK;
}

int tiledb_const_reverse_cell_iterator_init_in_range(
    TileDB_CTX* tiledb_ctx,
    int ad,
    const char** attribute_names,
    int attribute_names_num,
    const void* range,
    TileDB_ConstReverseCellIterator*& cell_it) {

  assert(tiledb_ctx);
  assert(ad >= 0);
  assert(attribute_names_num >= 0);

  // Initialize cell_it
  cell_it = (TileDB_ConstReverseCellIterator*)
             malloc(sizeof(struct TileDB_ConstReverseCellIterator));
  cell_it->begin_ = true;

  // For easy reference
  int err;
  const ArraySchema* array_schema;
  err = tiledb_ctx->storage_manager_->get_array_schema(
            ad, array_schema);
  if(err == -1)
    return -1; // TODO
  const std::type_info* type = array_schema->coords_type();

  // Get vector of attribute ids
  std::vector<std::string> attribute_names_vec;
  for(int i=0; i<attribute_names_num; ++i)
    attribute_names_vec.push_back(attribute_names[i]);
  std::vector<int> attribute_ids;
  err = array_schema->get_attribute_ids(attribute_names_vec, attribute_ids);
  if(err == -1)
    return -1; // TODO

  // Initialize the proper templated cell iterator
  // TODO: Error messages here !!!
  if(type == &typeid(int)) {
    cell_it->it_ =
        tiledb_ctx->storage_manager_->rbegin<int>(
             ad, (const int*) range, attribute_ids);
    cell_it->array_schema_ =
        ((ArrayConstReverseCellIterator<int>*) cell_it->it_)->array_schema();
  } else if(type == &typeid(int64_t)) {
    cell_it->it_ =
        tiledb_ctx->storage_manager_->rbegin<int64_t>(
            ad, (const int64_t*) range, attribute_ids);
    cell_it->array_schema_ =
        ((ArrayConstReverseCellIterator<int64_t>*) cell_it->it_)->array_schema();
  } else if(type == &typeid(float)) {
    cell_it->it_ =
        tiledb_ctx->storage_manager_->rbegin<float>(
            ad, (const float*) range, attribute_ids);
    cell_it->array_schema_ =
        ((ArrayConstReverseCellIterator<float>*) cell_it->it_)->array_schema();
  } else if(type == &typeid(double)) {
    cell_it->it_ =
        tiledb_ctx->storage_manager_->rbegin<double>(
            ad, (const double*) range, attribute_ids);
    cell_it->array_schema_ =
        ((ArrayConstReverseCellIterator<double>*) cell_it->it_)->array_schema();
  }

  return TILEDB_OK;
}

int tiledb_const_cell_iterator_next(
    TileDB_ConstCellIterator* cell_it,
    const void*& cell) {

  assert(cell_it);

  // For easy reference
  const ArraySchema* array_schema = cell_it->array_schema_;
  const std::type_info* type = array_schema->coords_type();

  if(type == &typeid(int)) {
    ArrayConstCellIterator<int>* it =
        (ArrayConstCellIterator<int>*) cell_it->it_;
    if(!cell_it->begin_)
      ++(*it);
    else
      cell_it->begin_ = false;
    if(it->end())
      cell = NULL;
    else
      cell = **it;
  } else if(type == &typeid(int64_t)) {
    ArrayConstCellIterator<int64_t>* it =
        (ArrayConstCellIterator<int64_t>*) cell_it->it_;
    if(!cell_it->begin_)
      ++(*it);
    else
      cell_it->begin_ = false;
    if(it->end())
      cell = NULL;
    else
      cell = **it;
  } else if(type == &typeid(float)) {
    ArrayConstCellIterator<float>* it =
        (ArrayConstCellIterator<float>*) cell_it->it_;
    if(!cell_it->begin_)
      ++(*it);
    else
      cell_it->begin_ = false;
    if(it->end())
      cell = NULL;
    else
      cell = **it;
  } else if(type == &typeid(double)) {
    ArrayConstCellIterator<double>* it =
        (ArrayConstCellIterator<double>*) cell_it->it_;
    if(!cell_it->begin_)
      ++(*it);
    else
      cell_it->begin_ = false;
    if(it->end())
      cell = NULL;
    else
      cell = **it;
  }

  return TILEDB_OK;
}

int tiledb_const_reverse_cell_iterator_next(
    TileDB_ConstReverseCellIterator* cell_it,
    const void*& cell) {

  assert(cell_it);

  // For easy reference
  const ArraySchema* array_schema = cell_it->array_schema_;
  const std::type_info* type = array_schema->coords_type();

  if(type == &typeid(int)) {
    ArrayConstReverseCellIterator<int>* it =
        (ArrayConstReverseCellIterator<int>*) cell_it->it_;
    if(!cell_it->begin_)
      ++(*it);
    else
      cell_it->begin_ = false;
    if(it->end())
      cell = NULL;
    else
      cell = **it;
  } else if(type == &typeid(int64_t)) {
    ArrayConstReverseCellIterator<int64_t>* it =
        (ArrayConstReverseCellIterator<int64_t>*) cell_it->it_;
    if(!cell_it->begin_)
      ++(*it);
    else
      cell_it->begin_ = false;
    if(it->end())
      cell = NULL;
    else
      cell = **it;
  } else if(type == &typeid(float)) {
    ArrayConstReverseCellIterator<float>* it =
        (ArrayConstReverseCellIterator<float>*) cell_it->it_;
    if(!cell_it->begin_)
      ++(*it);
    else
      cell_it->begin_ = false;
    if(it->end())
      cell = NULL;
    else
      cell = **it;
  } else if(type == &typeid(double)) {
    ArrayConstReverseCellIterator<double>* it =
        (ArrayConstReverseCellIterator<double>*) cell_it->it_;
    if(!cell_it->begin_)
      ++(*it);
    else
      cell_it->begin_ = false;
    if(it->end())
      cell = NULL;
    else
      cell = **it;
  }

  return TILEDB_OK;
}

/* ****************************** */
/*             QUERIES            */
/* ****************************** */

int tiledb_clear_array(
    const TileDB_CTX* tiledb_ctx,
    const char* array_name) {

  assert(tiledb_ctx && array_name);

  return tiledb_ctx->storage_manager_->clear_array(array_name);
}

int tiledb_array_defined(
    const TileDB_CTX* tiledb_ctx,
    const char* array_name) {

    assert(tiledb_ctx && array_name);

    bool isdef = tiledb_ctx->storage_manager_->array_defined(array_name);
    return isdef ? 1 : 0;
}

int tiledb_array_schema(
    const TileDB_CTX* tiledb_ctx, 
    const char* array_name, 
    char* array_schema_str,
    size_t* schema_length) {

  assert(tiledb_ctx && array_schema_str);
  assert(*schema_length > 0);
    
  // Get the array schema from the storage manager
  ArraySchema* array_schema;
  int rc = tiledb_ctx->storage_manager_->get_array_schema(
           array_name, array_schema);
  if(rc)
    return rc;
  
  std::string csv_schema = array_schema->serialize_csv();
  size_t nbytes = csv_schema.size() + 1;   
  if(*schema_length < nbytes) {
    *schema_length = nbytes;
  } else {
    *schema_length = nbytes;
    memcpy(array_schema_str, csv_schema.c_str(), nbytes);
  }
  delete array_schema;
  return TILEDB_OK;
}

int tiledb_define_array(
    const TileDB_CTX* tiledb_ctx,
    const char* array_schema_str) {

  assert(tiledb_ctx && array_schema_str);

  // Create an array schema from the input string
  ArraySchema* array_schema = new ArraySchema();

  if(array_schema->deserialize_csv(array_schema_str)) {
    std::cerr << ERROR_MSG_HEADER << " Failed to parse array schema.\n";
    return TILEDB_EPARRSCHEMA;
  }

  // Define the array
  if(tiledb_ctx->storage_manager_->define_array(array_schema)) {
    std::cerr << ERROR_MSG_HEADER << " Failed to define array.\n";
// TODO: Print better message
    return TILEDB_EDEFARR;
  }

  // Clean up
  delete array_schema;

  return TILEDB_OK;
}

int tiledb_delete_array(
    const TileDB_CTX* tiledb_ctx,
    const char* array_name) {

  assert(tiledb_ctx && array_name);

  return tiledb_ctx->storage_manager_->delete_array(array_name);
}

int tiledb_export_csv(
    const TileDB_CTX* tiledb_ctx,
    const char* array_name,
    const char* filename,
    const char** dim_names,
    int dim_names_num,
    const char** attribute_names,
    int attribute_names_num,
    int reverse) {

  assert(tiledb_ctx && array_name && filename);
  assert(dim_names_num >= 0);
  assert(attribute_names_num >= 0);

  // Compute vectors for dim_names and attribute names
  std::vector<std::string> dim_names_vec, attribute_names_vec;
  for(int i=0; i<dim_names_num; ++i)
    dim_names_vec.push_back(dim_names[i]);
  for(int i=0; i<attribute_names_num; ++i)
    attribute_names_vec.push_back(attribute_names[i]);
  bool rev = reverse != 0;
  return tiledb_ctx->query_processor_->export_csv(
      array_name, filename, dim_names_vec, attribute_names_vec, rev);
}

int tiledb_generate_data(
    const TileDB_CTX* tiledb_ctx,
    const char* array_name,
    const char* filename,
    const char* filetype,
    unsigned int seed,
    int64_t cell_num) {

  assert(tiledb_ctx && array_name && filename && filetype);

  // Check cell_num
  if(cell_num <= 0) {
    std::cerr << ERROR_MSG_HEADER
             << " The number of cells must be a positive integer.\n";
    return TILEDB_EIARG;
  }

  // To handle return codes
  int rc;

  // Get the array schema from the storage manager
  ArraySchema* array_schema;
  rc = tiledb_ctx->storage_manager_->get_array_schema(
           array_name, array_schema);
  if(rc)
    return rc;

  // Generate the file
  DataGenerator data_generator(array_schema);
  if(strcmp(filetype, "csv") == 0) {
    rc = data_generator.generate_csv(seed, filename, cell_num);
  } else if(strcmp(filetype, "bin") == 0) {
    rc = data_generator.generate_bin(seed, filename, cell_num);
  } else {
    std::cerr << ERROR_MSG_HEADER
             << " Unknown file type '" << filetype << "'.\n";
    return TILEDB_EIARG;
  }

  // Clean up
  delete array_schema;

  // Handle errors
  if(rc)
    return rc;

  return TILEDB_OK;
}

int tiledb_load_bin(
    const TileDB_CTX* tiledb_ctx,
    const char* array_name,
    const char* path,
    int sorted) {

  assert(tiledb_ctx && array_name && path);

  bool issorted = sorted != 0;
  return tiledb_ctx->loader_->load_bin(array_name, path, issorted);
}

int tiledb_load_csv(
    const TileDB_CTX* tiledb_ctx,
    const char* array_name,
    const char* path,
    int sorted) {

  assert(tiledb_ctx && array_name && path);

  bool issorted = sorted != 0;
  return tiledb_ctx->loader_->load_csv(array_name, path, issorted);
}

int tiledb_show_array_schema(
    const TileDB_CTX* tiledb_ctx,
    const char* array_name) {
  // Get the array schema from the storage manager
  ArraySchema* array_schema;
  int rc = tiledb_ctx->storage_manager_->get_array_schema(
               array_name, array_schema);
  if(rc)
    return rc;

  // Print array schema
  array_schema->print();

  // Clean up
  delete array_schema;

  return TILEDB_OK;
}

int tiledb_subarray(
    const TileDB_CTX* tiledb_ctx,
    const char* array_name,
    const char* result_name,
    const double* range,
    int range_size,
    const char** attribute_names,
    int attribute_names_num) {

  assert(tiledb_ctx && array_name && result_name);
  assert(range_size >= 0);
  assert(attribute_names_num >= 0);

  // Compute vectors for range and attribute names
  std::vector<std::string> attribute_names_vec;
  for(int i=0; i<attribute_names_num; ++i)
    attribute_names_vec.push_back(attribute_names[i]);
  std::vector<double> range_vec;
  for(int i=0; i<range_size; ++i)
    range_vec.push_back(range[i]);

  return tiledb_ctx->query_processor_->subarray(
      array_name, range_vec, result_name, attribute_names_vec);
}

int tiledb_update_bin(
    const TileDB_CTX* tiledb_ctx,
    const char* array_name,
    const char* path,
    int sorted) {

  assert(tiledb_ctx && array_name && path);

  bool issorted = sorted != 0;
  return tiledb_ctx->loader_->update_bin(array_name, path, issorted);
}

int tiledb_update_csv(
    const TileDB_CTX* tiledb_ctx,
    const char* array_name,
    const char* path,
    int sorted) {

  assert(tiledb_ctx && array_name && path);

  bool issorted = sorted != 0;
  return tiledb_ctx->loader_->update_csv(array_name, path, issorted);
}
