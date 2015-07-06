/**
 * @file   tiledb_cell_iterators.cc
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
 * This file implements the C APIs for cell iterators on TileDB arrays.
 */

#include "loader.h"
#include "query_processor.h"
#include "storage_manager.h"
#include "tiledb_ctx.h"
#include "tiledb_error.h"
#include "tiledb_cell_iterators.h"
#include <iomanip>

typedef struct TileDB_CTX {
  Loader* loader_;
  QueryProcessor* query_processor_;
  StorageManager* storage_manager_;
} TileDB_CTX;

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
    TileDB_ConstCellIterator*& cell_it) {
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
  cell_it = NULL;

  return 0;
}

int tiledb_const_reverse_cell_iterator_finalize(
    TileDB_ConstReverseCellIterator*& cell_it) {
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
  cell_it = NULL;

  return 0;
}

int tiledb_const_cell_iterator_init(
    TileDB_CTX* tiledb_ctx,
    int ad,
    const char** attribute_names,
    int attribute_names_num,
    TileDB_ConstCellIterator*& cell_it) {
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

  return 0;
}

int tiledb_const_cell_iterator_init_in_range(
    TileDB_CTX* tiledb_ctx,
    int ad,
    const char** attribute_names,
    int attribute_names_num,
    const void* range,
    TileDB_ConstCellIterator*& cell_it) {
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

  return 0;
}

int tiledb_const_reverse_cell_iterator_init(
    TileDB_CTX* tiledb_ctx,
    int ad,
    const char** attribute_names,
    int attribute_names_num,
    TileDB_ConstReverseCellIterator*& cell_it) {
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

  return 0;
}

int tiledb_const_reverse_cell_iterator_init_in_range(
    TileDB_CTX* tiledb_ctx,
    int ad,
    const char** attribute_names,
    int attribute_names_num,
    const void* range,
    TileDB_ConstReverseCellIterator*& cell_it) {
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

  return 0;
}

int tiledb_const_cell_iterator_next(
    TileDB_ConstCellIterator* cell_it,
    const void*& cell) {
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

  return 0;
}

int tiledb_const_reverse_cell_iterator_next(
    TileDB_ConstReverseCellIterator* cell_it,
    const void*& cell) {
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

  return 0;
}
