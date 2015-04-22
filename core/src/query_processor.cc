/**
 * @file   query_processor.cc
 * @author Stavros Papadopoulos <stavrosp@csail.mit.edu>
 *
 * @section LICENSE
 *
 * The MIT License
 * 
 * @copyright Copyright (c) 2014 Stavros Papadopoulos <stavrosp@csail.mit.edu>
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
 * This file implements the QueryProcessor class.
 */
  
#include "query_processor.h"
#include "utils.h"
#include <stdio.h>
#include <typeinfo>
#include <stdlib.h>
#include <iostream>
#include <sys/stat.h>
#include <sys/uio.h>
#include <assert.h>
#include <algorithm>
//#include <parallel/algorithm>
#include <functional>
#include <math.h>
#include <string.h>

/******************************************************
********************* CONSTRUCTORS ********************
******************************************************/

QueryProcessor::QueryProcessor(StorageManager* storage_manager) 
    : storage_manager_(storage_manager) {
}

void QueryProcessor::export_to_csv(
    const std::string& array_name, const std::string& filename) const { 
  // Open array in read mode
  int ad = storage_manager_->open_array(array_name, "r");
  if(ad == -1)
    throw QueryProcessorException(std::string("Cannot open array ") +
                                  array_name + "."); 

  // For easy reference
  const ArraySchema* array_schema = storage_manager_->get_array_schema(ad); 
  int attribute_num = array_schema->attribute_num();
  const std::type_info& coords_type = *(array_schema->type(attribute_num));
 
  // Invoke the proper templated function
  if(coords_type == typeid(int)) 
    export_to_csv<int>(ad, filename); 
  else if(coords_type == typeid(int64_t)) 
    export_to_csv<int64_t>(ad, filename); 
  else if(coords_type == typeid(float)) 
    export_to_csv<float>(ad, filename); 
  else if(coords_type == typeid(double)) 
    export_to_csv<double>(ad, filename); 

  // Clean up
  storage_manager_->close_array(ad); 
}

template<class T>
void QueryProcessor::export_to_csv(
    int ad, const std::string& filename) const { 
  // For easy reference
  const ArraySchema* array_schema = storage_manager_->get_array_schema(ad); 

  // Prepare CSV file
  CSVFile csv_file;
  csv_file.open(filename, "w");

  // Prepare cell iterators
  StorageManager::Array::const_cell_iterator<T> cell_it = 
      storage_manager_->begin<T>(ad);

  // Write cells into the CSV file
  for(; !cell_it.end(); ++cell_it) 
    csv_file << cell_to_csv_line(*cell_it, array_schema); 
  
  // Clean up 
  csv_file.close();
}

void QueryProcessor::subarray(
    const std::string& array_name, 
    const void* range,
    const std::string& result_array_name) const { 
  // Open array in read mode
  int ad = storage_manager_->open_array(array_name, "r");
  if(ad == -1)
    throw QueryProcessorException(std::string("Cannot open array ") +
                                  array_name + "."); 

  // For easy reference
  const ArraySchema* array_schema = storage_manager_->get_array_schema(ad); 
  int attribute_num = array_schema->attribute_num();
  const std::type_info& coords_type = *(array_schema->type(attribute_num));

  // Create result array schema
  ArraySchema* result_array_schema = array_schema->clone(result_array_name); 

  // Define result array
  storage_manager_->define_array(result_array_schema);

  // Open result array in write mode
  int result_ad = storage_manager_->open_array(result_array_name, "w");
  if(result_ad == -1)
    throw QueryProcessorException(std::string("Cannot open array ") +
                                  result_array_name + "."); 

  // Invoke the proper templated function
  if(coords_type == typeid(int)) 
    subarray<int>(ad, static_cast<const int*>(range), result_ad); 
  else if(coords_type == typeid(int64_t)) 
    subarray<int64_t>(ad, static_cast<const int64_t*>(range), result_ad); 
  else if(coords_type == typeid(float)) 
    subarray<float>(ad, static_cast<const float*>(range), result_ad); 
  else if(coords_type == typeid(double)) 
    subarray<double>(ad, static_cast<const double*>(range), result_ad); 

  // Clean up
  delete result_array_schema;
  storage_manager_->close_array(ad); 
  storage_manager_->close_array(result_ad); 
}

template<class T>
void QueryProcessor::subarray(int ad, const T* range, int result_ad) const { 
  // Prepare cell iterator
  StorageManager::Array::const_cell_iterator<T> cell_it = 
      storage_manager_->begin<T>(ad, range);

  // Write cells into the CSV file
  for(; !cell_it.end(); ++cell_it) 
    storage_manager_->write_cell_sorted<T>(result_ad, *cell_it);
}

/*
void QueryProcessor::filter(
    const StorageManager::FragmentDescriptor* fd,
    const ExpressionTree* expression,
    const StorageManager::FragmentDescriptor* result_fd) const {
  if(fd->array_schema()->has_regular_tiles())
    filter_regular(fd, expression, result_fd);
  else 
    filter_irregular(fd, expression, result_fd);
} 

void QueryProcessor::filter(
    const std::vector<const StorageManager::FragmentDescriptor*>& fd,
    const ExpressionTree* expression,
    const StorageManager::FragmentDescriptor* result_fd) const {
  assert(fd.size() > 0);

  if(fd[0]->array_schema()->has_regular_tiles())
    filter_regular(fd, expression, result_fd);
  else 
    filter_irregular(fd, expression, result_fd);
}

void QueryProcessor::join(
    const StorageManager::FragmentDescriptor* fd_A, 
    const StorageManager::FragmentDescriptor* fd_B,
    const StorageManager::FragmentDescriptor* result_fd) const {
  if(fd_A->array_schema()->has_regular_tiles())
    join_regular(fd_A, fd_B, result_fd);
  else 
    join_irregular(fd_A, fd_B, result_fd);
} 

void QueryProcessor::join(
    const std::vector<const StorageManager::FragmentDescriptor*>& fd_A, 
    const std::vector<const StorageManager::FragmentDescriptor*>& fd_B, 
    const StorageManager::FragmentDescriptor* result_fd) const {
  assert(fd_A.size() > 0);
  assert(fd_B.size() > 0);

  if(fd_A[0]->array_schema()->has_regular_tiles())
    join_regular(fd_A, fd_B, result_fd);
  else 
    join_irregular(fd_A, fd_B, result_fd);
}

void QueryProcessor::nearest_neighbors(
    const StorageManager::FragmentDescriptor* fd,
    const std::vector<double>& q,
    uint64_t k,
    const StorageManager::FragmentDescriptor* result_fd) const { 
  if(fd->array_schema()->has_regular_tiles())
    nearest_neighbors_regular(fd, q, k, result_fd);
  else 
    nearest_neighbors_irregular(fd, q, k, result_fd);
}

template<class T>
void QueryProcessor::read_irregular(
    const StorageManager::FragmentDescriptor* fd,
    int attribute_id, const T* range,
    void*& coords, size_t& coords_size,
    void*& values, size_t& values_size) const {
    // For easy reference
  const ArraySchema* array_schema = fd->array_schema();
  int attribute_num = array_schema->attribute_num();
  int64_t capacity = array_schema->capacity();
  size_t coords_cell_size = array_schema->cell_size(attribute_num);
  size_t values_cell_size = array_schema->cell_size(attribute_id);
 
  // Check on attribute id
  assert(attribute_id >=0 && attribute_id < attribute_num);

  // Create tiles
  const Tile* tiles[2];

  // Create cell iterators
  Tile::const_cell_iterator cell_its[2]; 
  Tile::const_cell_iterator cell_it_end;

  // Get the tile ranks that overlap with the range
  TilePosOverlapVector overlapping_tile_pos;
  storage_manager_->get_overlapping_tile_pos(fd, range, 
                                             &overlapping_tile_pos);

  // Initialize tile iterators
  TilePosOverlapVector::const_iterator tile_pos_it = 
      overlapping_tile_pos.begin();
  TilePosOverlapVector::const_iterator tile_pos_it_end =
      overlapping_tile_pos.end();
 
  // Initialize result buffers
  int64_t buffer_capacity = capacity; // Initial buffer capacity
  values = malloc(buffer_capacity*values_cell_size);
  coords = malloc(buffer_capacity*coords_cell_size);

  // Number of result cells
  int64_t result_num = 0;

  // Iterate over all tiles
  for(; tile_pos_it != tile_pos_it_end; ++tile_pos_it) {
    tiles[0] = storage_manager_->get_tile_by_pos(fd, attribute_id,
                                                 tile_pos_it->first);
    tiles[1] = storage_manager_->get_tile_by_pos(fd, attribute_num,
                                                 tile_pos_it->first);

    // Initialize cell iterators
    cell_its[0] = tiles[0]->begin();
    cell_its[1] = tiles[1]->begin();
    cell_it_end = tiles[1]->end();

    if(tile_pos_it->second) { // Full overlap - all cells are results
      while(cell_its[1] != cell_it_end) {
        // Check if buffer expansion is needed
        if(result_num == buffer_capacity) {
          expand_buffer(values, buffer_capacity*values_cell_size);
          expand_buffer(coords, buffer_capacity*coords_cell_size);
          buffer_capacity *= 2;
        }

        // Append cell values to result buffers
        void* values_offset = static_cast<char*>(values) + 
                              result_num*values_cell_size;
        void* coords_offset = static_cast<char*>(coords) + 
                              result_num*coords_cell_size;
        memcpy(values_offset, *cell_its[0], values_cell_size);
        memcpy(coords_offset, *cell_its[1], coords_cell_size);
        ++result_num;

        // Advance cell iterators
        ++cell_its[0];
        ++cell_its[1];
      }
    } else { // Partial overlap - check each cell separately against the range
      while(cell_its[1] != cell_it_end) {
        if(cell_its[1].cell_inside_range(range)) { 
          // Check if buffer expansion is needed
          if(result_num == buffer_capacity) {
            expand_buffer(values, buffer_capacity*values_cell_size);
            expand_buffer(coords, buffer_capacity*coords_cell_size);
            buffer_capacity *= 2;
          }

          // Append cell values to result buffers
          void* values_offset = static_cast<char*>(values) + 
                                result_num*values_cell_size;
          void* coords_offset = static_cast<char*>(coords) + 
                                result_num*coords_cell_size;
          memcpy(values_offset, *cell_its[0], values_cell_size);
          memcpy(coords_offset, *cell_its[1], coords_cell_size);
          ++result_num;
        }

        // Advance cell iterators
        ++cell_its[0];
        ++cell_its[1];
      }
    }
  } 

  // Calculate the sizes of the result buffers
  values_size = result_num * values_cell_size; 
  coords_size = result_num * coords_cell_size; 
}

void QueryProcessor::read(
    const StorageManager::FragmentDescriptor* fd,
    int attribute_id, const void* range,
    void*& coords, size_t& coords_size,
    void*& values, size_t& values_size) const {
  // For easy reference
  const ArraySchema* array_schema = fd->array_schema();
  int attribute_num = array_schema->attribute_num();
  const std::type_info* type = array_schema->type(attribute_num);

  if(array_schema->has_irregular_tiles()) { // Irregular tiles
    if(*type == typeid(int)) 
      read_irregular(fd, attribute_id, static_cast<const int*>(range), 
                     coords, coords_size, values, values_size);
    else if(*type == typeid(int64_t)) 
      read_irregular(fd, attribute_id, static_cast<const int64_t*>(range), 
                     coords, coords_size, values, values_size);
    else if(*type == typeid(float)) 
      read_irregular(fd, attribute_id, static_cast<const float*>(range), 
                     coords, coords_size, values, values_size);
    else if(*type == typeid(double)) 
      read_irregular(fd, attribute_id, static_cast<const double*>(range), 
                      coords, coords_size, values, values_size);
  } else { // Regular tiles TODO
    throw QueryProcessorException(
        "Operations currently not supported for arrays with regular tiles."); 
  }
} 

void QueryProcessor::retile(
    const std::vector<const StorageManager::FragmentDescriptor*>& fd,
    uint64_t capacity, 
    ArraySchema::Order order,
    const std::vector<double>& tile_extents) const {
  // For easy reference
  const ArraySchema& array_schema = *(fd[0]->array_schema());

  if(array_schema.has_irregular_tiles()) 
    retile_irregular(fd, capacity, order, tile_extents);
  else
    retile_regular(fd, capacity, order, tile_extents);
}


void QueryProcessor::retile_irregular(
    const std::vector<const StorageManager::FragmentDescriptor*>& fd,
    uint64_t capacity, 
    ArraySchema::Order order,
    const std::vector<double>& tile_extents) const {
  // CASE #1: Tile extents provided 
  //          --> retile into regular tile                 TODO
  if(tile_extents.size() > 0)
    throw QueryProcessorException("Retiling of arrays with irregular tiles"
                                  " into regular tiles currently not"
                                  " supported.");
  // CASE #2: Tile extents not provided, order different
  //          --> retile (via export/reload)               TODO
  else if(order != ArraySchema::NONE && order != fd[0]->array_schema()->order())
    throw QueryProcessorException("Retiling of arrays with irregular tiles"
                                  " into irregular tiles with different"
                                  " cell order currently not supported.");
  // CASE #3: Tile extents not provided, order same, capacity different
  //          --> retile (efficiently) 
  else 
    retile_irregular_capacity(fd, capacity);
}

void QueryProcessor::retile_irregular_capacity(
    const std::vector<const StorageManager::FragmentDescriptor*>& fd,
    uint64_t capacity) const {
  // Moduify book-keeping structures of fragments
  for(unsigned int i=0; i<fd.size(); ++i) 
    storage_manager_.modify_fragment_bkp(fd[i], capacity);

  // Modify schema
  ArraySchema new_array_schema = fd[0]->array_schema()->clone(capacity);
  storage_manager_.modify_array_schema(new_array_schema);
}

void QueryProcessor::retile_regular(
    const std::vector<const StorageManager::FragmentDescriptor*>& fd,
    uint64_t capacity, 
    ArraySchema::Order order,
    const std::vector<double>& tile_extents) const {
  throw QueryProcessorException("Retiling of arrays with regular tiles"
                                " currently not supported.");

  // CASE #1: Tile extents same, order same, capacity different
  //          --> do nothing                               TODO
  // CASE #2: Tile extents same, order different
  //          --> retile (efficiently)                     TODO 
  // CASE #3: Tile extents different
  //          --> retile (via export/reload)               TODO
  // CASE #4: No tile extents; retile to irregular
  //          --> retile (via export/reload)               TODO
}

void QueryProcessor::subarray(
    const StorageManager::FragmentDescriptor* fd,
    const Tile::Range& range,
    const StorageManager::FragmentDescriptor* result_fd) const { 
  if(fd->array_schema()->has_regular_tiles())
    subarray_regular(fd, range, result_fd);
  else 
    subarray_irregular(fd, range, result_fd);
}

void QueryProcessor::subarray(
    const StorageManager::FragmentDescriptor* fd,
    const Tile::Range& range,
    unsigned int attribute_id,
    struct iovec& c_iovec,
    struct iovec& v_iovec) const { 
  if(fd->array_schema()->has_regular_tiles())
    throw QueryProcessorException("Operation currently not supported for "
                                  "regular tiles.");

  subarray_irregular(fd, range, attribute_id, c_iovec, v_iovec);
}

void QueryProcessor::subarray(
    const std::vector<const StorageManager::FragmentDescriptor*>& fd,
    const Tile::Range& range,
    const StorageManager::FragmentDescriptor* result_fd) const { 
  assert(fd.size() > 0);

  if(fd[0]->array_schema()->has_regular_tiles())
    subarray_regular(fd, range, result_fd);
  else 
    subarray_irregular(fd, range, result_fd);
}

*/ 

/******************************************************
******************* PRIVATE METHODS *******************
******************************************************/

/*

inline
void QueryProcessor::advance_cell_its(
    int attribute_num, Tile::const_cell_iterator* cell_its) const {
  for(int i=0; i<=attribute_num; ++i) 
      ++cell_its[i];
}


inline
void QueryProcessor::advance_cell_its(
    Tile::const_iterator* cell_its,
    const std::vector<unsigned int>& attribute_ids) const {
  for(unsigned int i=0; i<attribute_ids.size(); i++) 
    ++cell_its[attribute_ids[i]];
}

inline
void QueryProcessor::advance_cell_its(unsigned int attribute_num,
                                      Tile::const_iterator* cell_its,
                                      int64_t step) const {
  for(unsigned int i=0; i<attribute_num; i++) 
    cell_its[i] += step;
}

inline
void QueryProcessor::advance_cell_its(
    Tile::const_iterator* cell_its,
    const std::vector<unsigned int>& attribute_ids,
    int64_t step) const {
  for(unsigned int i=0; i<attribute_ids.size(); i++) 
    cell_its[attribute_ids[i]] += step;
}

inline
void QueryProcessor::advance_cell_tile_its(
    unsigned int attribute_num,
    Tile::const_iterator* cell_its,
    Tile::const_iterator& cell_it_end,
    StorageManager::const_iterator* tile_its,
    StorageManager::const_iterator& tile_it_end) const {
  advance_cell_its(attribute_num, cell_its);
  if(cell_its[attribute_num] == cell_it_end) {
    advance_tile_its(attribute_num, tile_its);
    if(tile_its[attribute_num] != tile_it_end)
      initialize_cell_its(tile_its, attribute_num, cell_its, cell_it_end);
  }
}

inline
void QueryProcessor::advance_cell_tile_its(
    unsigned int attribute_num,
    Tile::const_iterator* cell_its,
    Tile::const_iterator& cell_it_end,
    StorageManager::const_iterator* tile_its,
    StorageManager::const_iterator& tile_it_end,
    const std::vector<unsigned int>& attribute_ids,
    int64_t& skipped_tiles,
    uint64_t& skipped_cells,
    bool& non_cell_its_initialized) const {
  bool advanced_tile_its = false;

  advance_cell_its(cell_its, attribute_ids);
  ++cell_its[attribute_num];
  if(cell_its[attribute_num] == cell_it_end) {
     advance_tile_its(tile_its, attribute_ids);
     ++tile_its[attribute_num];
     advanced_tile_its = true;
     if(tile_its[attribute_num] != tile_it_end) {
       initialize_cell_its(tile_its, cell_its, attribute_ids);
       cell_its[attribute_num] = (*tile_its[attribute_num]).begin();
       cell_it_end = (*tile_its[attribute_num]).end();
     }
  }

  // If we advanced the tile iterators
  if(advanced_tile_its) {
    non_cell_its_initialized = false;
    ++skipped_tiles; 
    skipped_cells = 0;
   } else { // Otherwise, we advanced only the cell iterators
     ++skipped_cells;
   }
}

inline
void QueryProcessor::advance_cell_tile_its(
    const StorageManager::FragmentDescriptor* fd,
    unsigned int attribute_num,
    Tile::const_iterator* cell_its,
    Tile::const_iterator& cell_it_end,
    RankOverlapVector::const_iterator& tile_rank_it,
    RankOverlapVector::const_iterator& tile_rank_it_end,
    unsigned int fragment_num,
    const Tile** tiles,
    uint64_t& skipped_cells) const {
  ++cell_its[attribute_num];
  if(cell_its[attribute_num] == cell_it_end) {
    ++tile_rank_it;
    skipped_cells = 0;
    if(tile_rank_it != tile_rank_it_end) {
      get_tiles_by_rank(fd, tile_rank_it->first, tiles);
      initialize_cell_its(tiles, attribute_num, cell_its, cell_it_end);
     }
  } else {
    ++skipped_cells;
  }
}

inline
void QueryProcessor::advance_cell_tile_its(
    Tile::const_iterator& cell_it,
    const Tile::const_iterator& cell_it_end,
    StorageManager::const_iterator& tile_it,
    uint64_t& skipped_tiles,
    uint64_t& skipped_cells,
    bool& attribute_cell_its_initialized,
    bool& coordinate_cell_its_initialized) const {
  ++cell_it;
  ++skipped_cells;
  if(cell_it == cell_it_end) {
    ++tile_it;
    coordinate_cell_its_initialized = false;
    attribute_cell_its_initialized = false;
    ++skipped_tiles;
    skipped_cells = 0;
  }
}

inline
void QueryProcessor::advance_tile_its(
    int attribute_num, 
    StorageManager::const_tile_iterator* tile_its) const {
  for(int i=0; i<=attribute_num; ++i) 
    ++tile_its[i];
}

inline
void QueryProcessor::advance_tile_its(
    unsigned int attribute_num, 
    StorageManager::const_iterator* tile_its, 
    int64_t step) const {
  for(unsigned int i=0; i<attribute_num; i++) 
    tile_its[i] += step;
}

inline
void QueryProcessor::advance_tile_its(
    StorageManager::const_iterator* tile_its,
    const std::vector<unsigned int>& attribute_ids) const {
  for(unsigned int i=0; i<attribute_ids.size(); i++) 
    ++tile_its[attribute_ids[i]];
}

inline
void QueryProcessor::advance_tile_its(
    StorageManager::const_iterator* tile_its, 
    const std::vector<unsigned int>& attribute_ids,
    int64_t step) const {
  for(unsigned int i=0; i<attribute_ids.size(); i++) 
    tile_its[attribute_ids[i]] += step;
}

inline
void QueryProcessor::append_cell(const Tile::const_iterator* cell_its,
                                 Tile** tiles,
                                 unsigned int attribute_num) const {
  for(unsigned int i=0; i<=attribute_num; i++) 
    *tiles[i] << cell_its[i]; 
}

inline
void QueryProcessor::append_cell(const Tile::const_iterator* cell_its_A,
                                 const Tile::const_iterator* cell_its_B,
                                 Tile** tiles_C,
                                 unsigned int attribute_num_A,
                                 unsigned int attribute_num_B) const {
  for(unsigned int i=0; i<attribute_num_A; i++) 
    *tiles_C[i] << cell_its_A[i]; 
  for(unsigned int i=0; i<=attribute_num_B; i++)
    *tiles_C[attribute_num_A+i] << cell_its_B[i]; 
}

bool QueryProcessor::cell_satisfies_expression(
    const ArraySchema& array_schema,
    const Tile::const_iterator* cell_its,
    const std::vector<unsigned int>& attribute_ids,
    const ExpressionTree* expression) const {
  // Get the values of the attributes involved in the expression
  std::map<std::string, double> var_values;
  for(unsigned int i=0; i<attribute_ids.size(); ++i) {
    var_values[array_schema.attribute_name(attribute_ids[i])] = 
        *cell_its[attribute_ids[i]];
  }

  // Evaluate the expression
  return expression->evaluate(var_values);
}
*/

inline
CSVLine QueryProcessor::cell_to_csv_line(
    const void* cell, const ArraySchema* array_schema) const {
  // For easy reference
  int dim_num = array_schema->dim_num();
  int attribute_num = array_schema->attribute_num();

  // Prepare a CSV line
  CSVLine csv_line;

  // Append coordinates first
  const void* coords = cell;
  const std::type_info& coords_type = *(array_schema->type(attribute_num));
  if(coords_type == typeid(int)) { 
    for(int i=0; i<dim_num; ++i)
      csv_line << static_cast<const int*>(coords)[i];
  } else if(coords_type == typeid(int64_t)) { 
    for(int i=0; i<dim_num; ++i)
      csv_line << static_cast<const int64_t*>(coords)[i];
  } else if(coords_type == typeid(float)) { 
    for(int i=0; i<dim_num; ++i)
      csv_line << static_cast<const float*>(coords)[i];
  } else if(coords_type == typeid(double)) { 
    for(int i=0; i<dim_num; ++i)
      csv_line << static_cast<const double*>(coords)[i];
  }

  size_t offset = array_schema->cell_size(attribute_num);

  // Append attribute values next
  for(int i=0; i<attribute_num; ++i) {
    const void* v = static_cast<const char*>(cell) + offset;
    const std::type_info& attr_type = *(array_schema->type(i));
    if(attr_type == typeid(char)) { 
      csv_line << *(static_cast<const char*>(v));
    } else if(attr_type == typeid(int)) { 
      csv_line << *(static_cast<const int*>(v));
    } else if(attr_type == typeid(int64_t)) { 
      csv_line << *(static_cast<const int64_t*>(v));
    } else if(attr_type == typeid(float)) { 
      csv_line << *(static_cast<const float*>(v));
    } else if(attr_type == typeid(double)) { 
      csv_line << *(static_cast<const double*>(v));
    }
    offset += array_schema->cell_size(i);
  }

  return csv_line;
}

/*
bool QueryProcessor::coincides(
    const StorageManager::const_iterator& tile_it_A,
    const Tile::const_iterator& cell_it_A,
    bool coordinate_cell_its_initialized_A,
    const StorageManager::const_iterator& tile_it_B,
    const Tile::const_iterator& cell_it_B,
    bool coordinate_cell_its_initialized_B,
    const ArraySchema& array_schema) const {
  if(coordinate_cell_its_initialized_A && coordinate_cell_its_initialized_B) {
    return cell_it_A == cell_it_B;
  } else if(!coordinate_cell_its_initialized_A && 
            !coordinate_cell_its_initialized_B) {
    const std::vector<double>& coord_A = tile_it_A.bounding_coordinates().first;
    const std::vector<double>& coord_B = tile_it_B.bounding_coordinates().first;
    return std::equal(coord_A.begin(), coord_A.end(), coord_B.begin());
  } else if(coordinate_cell_its_initialized_A && 
            !coordinate_cell_its_initialized_B) {
    const std::vector<double> coord_A = *cell_it_A;
    const std::vector<double>& coord_B = tile_it_B.bounding_coordinates().first;
    return std::equal(coord_A.begin(), coord_A.end(), coord_B.begin());
  } else if(!coordinate_cell_its_initialized_A && 
            coordinate_cell_its_initialized_B) {
    const std::vector<double>& coord_A = tile_it_A.bounding_coordinates().first;
    const std::vector<double> coord_B = *cell_it_B;
    return std::equal(coord_A.begin(), coord_A.end(), coord_B.begin());
  }
}

std::vector<QueryProcessor::DistRank> 
QueryProcessor::compute_sorted_dist_ranks(
    const StorageManager::FragmentDescriptor* fd,
    const std::vector<double>& q) const {
  // Initializations
  // We store pairs of the form (dist, rank)
  std::vector<DistRank> dist_ranks;
  double dist;
  uint64_t rank;

  // Retrieve MBR iterators from storage manager
  StorageManager::MBRs::const_iterator mbr_it = 
      storage_manager_.MBR_begin(fd);
  StorageManager::MBRs::const_iterator mbr_it_end = 
      storage_manager_.MBR_end(fd);

  for(rank=0; mbr_it != mbr_it_end; ++mbr_it, ++rank) {
    dist = point_to_mbr_distance(q, *mbr_it);
    dist_ranks.push_back(DistRank(dist, rank));
  }
 
  // Sort ranks on distance and return them
  __gnu_parallel::sort(dist_ranks.begin(), dist_ranks.end());
  return dist_ranks;
}

std::vector<QueryProcessor::RankPosCoord> 
QueryProcessor::compute_sorted_kNN_coords(
    const StorageManager::FragmentDescriptor* fd,
    const std::vector<double>& q,
    uint64_t k,
    const std::vector<DistRank>& sorted_dist_ranks) const {
  std::priority_queue<DistRankPosCoord> kNN_coords;
  // For easy reference
  unsigned int attribute_num = fd->array_schema()->attribute_num();

  const Tile* tile;
  Tile::const_iterator cell_it, cell_it_end;

  // Find the k nearest neighbor coordinates
  uint64_t rank;
  double dist;
  std::vector<double> coord;
  uint64_t tile_num = sorted_dist_ranks.size();
  // Iterate over the (coordinate) tiles, sorted on their distance to q
  for(uint64_t i=0; i<tile_num; ++i) {
    // Stopping condition
    if(kNN_coords.size() == k && 
       sorted_dist_ranks[i].first > kNN_coords.top().first)
      break;

    // Get new coordinate tile and initalize cell iterators
    rank = sorted_dist_ranks[i].second;
    tile = storage_manager_.get_tile_by_rank(fd, attribute_num, rank); 
    cell_it = tile->begin();
    cell_it_end = tile->end();

    // Scan all (coordinate) cells
    for(uint64_t pos=0; cell_it != cell_it_end; ++cell_it, ++pos) {
      // Find new kNNs
      coord = *cell_it;
      dist = point_to_point_distance(q, coord);
      if(kNN_coords.size() < k || dist < kNN_coords.top().first) {
        kNN_coords.push(DistRankPosCoord(dist, 
                                         RankPosCoord(rank, 
                                                      PosCoord(pos, coord))));
        if(kNN_coords.size() > k)
          kNN_coords.pop();
      }
    }
  }

  // Make a new vector of (rank, (pos, coord)), sorted on rank, pos
  std::vector<RankPosCoord> resorted_kNN_coords;
  resorted_kNN_coords.reserve(k);
  while(kNN_coords.size() > 0) {
    resorted_kNN_coords.push_back(kNN_coords.top().second);
    kNN_coords.pop();
  }
  __gnu_parallel::sort(resorted_kNN_coords.begin(), resorted_kNN_coords.end());

  return resorted_kNN_coords;
}

void QueryProcessor::filter_irregular(
    const StorageManager::FragmentDescriptor* fd,
    const ExpressionTree* expression,
    const StorageManager::FragmentDescriptor* result_fd) const {
  // For easy reference
  const ArraySchema& array_schema = *(fd->array_schema());

  const unsigned int attribute_num = array_schema.attribute_num();
  uint64_t capacity = array_schema.capacity();
  
  // Create tiles 
  Tile** result_tiles = new Tile*[attribute_num+1];

  // Create and initialize tile iterators
  StorageManager::const_iterator *tile_its = 
      new StorageManager::const_iterator[attribute_num+1];
  StorageManager::const_iterator tile_it_end;

  // Create cell iterators
  Tile::const_iterator* cell_its = 
      new Tile::const_iterator[attribute_num+1];
  Tile::const_iterator cell_it_end;

  // Get the attribute names participating as variables in the expression
  const std::set<std::string>& expr_attribute_names = expression->vars();

  // Get the ids of the attribute names involved in the expression, and their
  // compliment.
  std::pair<ArraySchema::AttributeIds, ArraySchema::AttributeIds>
    ret = array_schema.get_attribute_ids(expr_attribute_names);
  const ArraySchema::AttributeIds& expr_attribute_ids = ret.first;
  const ArraySchema::AttributeIds& non_expr_attribute_ids = ret.second;

  // Initialize tile iterators
  unsigned int end_attribute_id = expr_attribute_ids[0];
  initialize_tile_its(fd, tile_its, tile_it_end, end_attribute_id);
  
  // Auxiliary variables for proper retrieval of qualifying tiles and cells.
  int64_t skipped_tiles = 0;
  uint64_t skipped_cells;
    
  // Create result tiles
  uint64_t tile_id = 0;
  new_tiles(array_schema, tile_id, result_tiles); 

  // Iterate over all tiles
  while(tile_its[end_attribute_id] != tile_it_end) {
    // Initialize cell its for the attributes involved in the expression
    initialize_cell_its(tile_its, cell_its, cell_it_end, expr_attribute_ids);
    skipped_cells = 0;
    bool non_expr_cell_its_initialized = false;

    // Iterate over all cells of each tile
    while(cell_its[end_attribute_id] != cell_it_end) {
      if(cell_satisfies_expression(array_schema, cell_its, 
                                   expr_attribute_ids, expression)) {
        if(skipped_tiles) {
          advance_tile_its(tile_its, non_expr_attribute_ids, skipped_tiles);
          skipped_tiles = 0;
        }
        if(!non_expr_cell_its_initialized) {
          initialize_cell_its(tile_its, cell_its, non_expr_attribute_ids);
          non_expr_cell_its_initialized = true;
        }
        if(skipped_cells) {
          advance_cell_its(cell_its, non_expr_attribute_ids, skipped_cells);
          skipped_cells = 0;
        }
        if(result_tiles[attribute_num]->cell_num() == capacity) {
          store_tiles(result_fd, result_tiles);
          new_tiles(array_schema, ++tile_id, result_tiles); 
        }
        append_cell(cell_its, result_tiles, attribute_num);
        advance_cell_its(attribute_num, cell_its);
      } else {
        advance_cell_its(cell_its, expr_attribute_ids);
        ++skipped_cells;
      }
    }
 
    // Advance tile iterators 
    advance_tile_its(tile_its, expr_attribute_ids);
    ++skipped_tiles;
  }
 
  // Send the lastly created tiles to storage manager
  store_tiles(result_fd, result_tiles);

  // Clean up
  delete [] result_tiles;
  delete [] tile_its;
  delete [] cell_its;
}

void QueryProcessor::filter_irregular(
    const std::vector<const StorageManager::FragmentDescriptor*>& fd,
    const ExpressionTree* expression,
    const StorageManager::FragmentDescriptor* result_fd) const {
  // For easy reference
  const ArraySchema& array_schema = *(fd[0]->array_schema());
  const unsigned int attribute_num = array_schema.attribute_num();
  uint64_t capacity = array_schema.capacity();
  unsigned int fragment_num = fd.size(); 
  
  // Create result tiles 
  Tile** result_tiles = new Tile*[attribute_num+1];
  uint64_t tile_id = 0;
  new_tiles(array_schema, tile_id, result_tiles); 

  // Get the attribute names participating as variables in the expression
  const std::set<std::string>& expr_attribute_names = expression->vars();

  // Get the ids of the attribute names involved in the expression, and
  // their compliment
  std::pair<ArraySchema::AttributeIds, ArraySchema::AttributeIds>
    ret = array_schema.get_attribute_ids(expr_attribute_names);
  const ArraySchema::AttributeIds& expr_attribute_ids = ret.first;
  ArraySchema::AttributeIds& non_expr_attribute_ids = ret.second;

  // Remove the coordinates id (attribute_num) from non_expr_attribute_ids.
  // Tiles and cells associated with the coordinates will be advanced along with
  // tiles and cells corresponding to the expr_attribute_ids. 
  non_expr_attribute_ids.pop_back();

  // Create and initialize tile iterators
  StorageManager::const_iterator** tile_its = 
      new StorageManager::const_iterator*[fragment_num];
  StorageManager::const_iterator* tile_it_end = 
      new StorageManager::const_iterator[fragment_num]; 
  for(unsigned int i=0; i<fragment_num; ++i) {
    tile_its[i] = new StorageManager::const_iterator[attribute_num+1];
    initialize_tile_its(fd[i], tile_its[i], tile_it_end[i]);
  }

  // Create cell iterators
  Tile::const_iterator** cell_its = 
      new Tile::const_iterator*[fragment_num];
  Tile::const_iterator* cell_it_end = 
      new Tile::const_iterator[fragment_num];
  for(unsigned int i=0; i<fragment_num; ++i) { 
    cell_its[i] = new Tile::const_iterator[attribute_num+1];
    initialize_cell_its(tile_its[i], cell_its[i], expr_attribute_ids);
    cell_its[i][attribute_num] = (*tile_its[i][attribute_num]).begin();
    cell_it_end[i] = (*tile_its[i][attribute_num]).end();
  }

  // Auxiliary variable
  bool* non_expr_cell_its_initialized = new bool[fragment_num];
  for(unsigned int i=0; i<fragment_num; ++i)
    non_expr_cell_its_initialized[i] = false;

  // Auxiliary variables for proper retrieval of qualifying tiles and cells.
  int64_t* skipped_tiles = new int64_t[fragment_num];
  uint64_t* skipped_cells = new uint64_t[fragment_num];
  for(unsigned int i=0; i<fragment_num; ++i) {
    skipped_tiles[i] = 0;
    skipped_cells[i] = 0;
  }

  // Get the index of the fragment from which we will get the next cell
  // to check against the filter condition. 
  int next_fragment_index =
      get_next_fragment_index(tile_its, tile_it_end, fragment_num, 
                              cell_its, cell_it_end, 
                              expr_attribute_ids,
                              skipped_tiles, skipped_cells,
                              non_expr_cell_its_initialized,
                              array_schema);

  // Iterate over all cells, until there are no more cells
  while(next_fragment_index != -1) {
    // For easy reference
    Tile::const_iterator* next_cell_its = cell_its[next_fragment_index];
    Tile::const_iterator& next_cell_it_end = cell_it_end[next_fragment_index];
    StorageManager::const_iterator* next_tile_its = 
        tile_its[next_fragment_index];
    StorageManager::const_iterator& next_tile_it_end = 
        tile_it_end[next_fragment_index];

    if(cell_satisfies_expression(array_schema, next_cell_its, 
                                 expr_attribute_ids, expression)) {
      if(skipped_tiles[next_fragment_index]) {
        advance_tile_its(next_tile_its, non_expr_attribute_ids, 
                         skipped_tiles[next_fragment_index]);
        skipped_tiles[next_fragment_index] = 0;
      }
      if(!non_expr_cell_its_initialized[next_fragment_index]) {
        initialize_cell_its(next_tile_its, next_cell_its, 
                            non_expr_attribute_ids);
        non_expr_cell_its_initialized[next_fragment_index] = true;
      }
      if(skipped_cells[next_fragment_index]) {
        advance_cell_its(next_cell_its, non_expr_attribute_ids, 
                         skipped_cells[next_fragment_index]);
        skipped_cells[next_fragment_index] = 0;
      }
      if(result_tiles[attribute_num]->cell_num() == capacity) {
        store_tiles(result_fd, result_tiles);
        new_tiles(array_schema, ++tile_id, result_tiles); 
      }
      append_cell(next_cell_its, result_tiles, attribute_num);
      advance_cell_tile_its(attribute_num, next_cell_its, next_cell_it_end,
                            next_tile_its, next_tile_it_end);
    } else {
      advance_cell_tile_its(
          attribute_num, 
          next_cell_its, next_cell_it_end,
          next_tile_its, next_tile_it_end,
          expr_attribute_ids,
          skipped_tiles[next_fragment_index],
          skipped_cells[next_fragment_index],
          non_expr_cell_its_initialized[next_fragment_index]);           
    }

    next_fragment_index =
        get_next_fragment_index(tile_its, tile_it_end,
                                fragment_num, 
                                cell_its, cell_it_end, 
                                expr_attribute_ids,
                                skipped_tiles,
                                skipped_cells,
                                non_expr_cell_its_initialized,
                                array_schema);
  }

  // Send the lastly created tiles to storage manager
  store_tiles(result_fd, result_tiles);

  // Clean up
  delete [] result_tiles;
  for(unsigned int i=0; i<fragment_num; ++i) 
    delete [] tile_its[i];
  delete [] tile_its;
  delete [] tile_it_end;
  for(unsigned int i=0; i<fragment_num; ++i) 
    delete [] cell_its[i];
  delete [] cell_its;
  delete [] cell_it_end;
  delete [] skipped_cells;
  delete [] skipped_tiles;
  delete [] non_expr_cell_its_initialized;
}

void QueryProcessor::filter_regular(
    const StorageManager::FragmentDescriptor* fd,
    const ExpressionTree* expression,
    const StorageManager::FragmentDescriptor* result_fd) const {
  // For easy reference
  const ArraySchema& array_schema = *(fd->array_schema());
  const unsigned int attribute_num = array_schema.attribute_num();
  
  // Create tiles 
  Tile** result_tiles = new Tile*[attribute_num+1];

  // Create tile iterators
  StorageManager::const_iterator *tile_its = 
      new StorageManager::const_iterator[attribute_num+1];
  StorageManager::const_iterator tile_it_end;

  // Create cell iterators
  Tile::const_iterator* cell_its = 
      new Tile::const_iterator[attribute_num+1];
  Tile::const_iterator cell_it_end;

  // Get the attribute names participating as variables in the expression
  const std::set<std::string>& expr_attribute_names = expression->vars();

  // Get the ids of the attribute names involved in the expression, and their
  // compliment
  std::pair<ArraySchema::AttributeIds, ArraySchema::AttributeIds>
    ret = array_schema.get_attribute_ids(expr_attribute_names);
  const ArraySchema::AttributeIds& expr_attribute_ids = ret.first;
  const ArraySchema::AttributeIds& non_expr_attribute_ids = ret.second;

  // Initialize tile iterators
  unsigned int end_attribute_id = expr_attribute_ids[0];
  initialize_tile_its(fd, tile_its, tile_it_end, end_attribute_id);
  
  // Auxiliary variables for proper retrieval of qualifying tiles and cells.
  int64_t skipped_tiles = 0;
  uint64_t skipped_cells;

  // Iterate over all tiles
  while(tile_its[end_attribute_id] != tile_it_end) {
    // Create new result tiles
    new_tiles(array_schema, tile_its[end_attribute_id].tile_id(), 
              result_tiles);
    // Initialize cell its for the attributes involved in the expression
    initialize_cell_its(tile_its, cell_its, cell_it_end, expr_attribute_ids);
    skipped_cells = 0;
    bool non_expr_cell_its_initialized = false;

    // Iterate over all cells of each tile
    while(cell_its[end_attribute_id] != cell_it_end) {
      if(cell_satisfies_expression(array_schema, cell_its, 
                                   expr_attribute_ids, expression)) {
        if(skipped_tiles) {
          advance_tile_its(tile_its, non_expr_attribute_ids, skipped_tiles);
          skipped_tiles = 0;
        }
        if(!non_expr_cell_its_initialized) {
          initialize_cell_its(tile_its, cell_its, non_expr_attribute_ids);
          non_expr_cell_its_initialized = true;
        }
        if(skipped_cells) {
          advance_cell_its(cell_its, non_expr_attribute_ids, skipped_cells);
          skipped_cells = 0;
        }
        append_cell(cell_its, result_tiles, attribute_num);
        advance_cell_its(attribute_num, cell_its);
      } else {
        advance_cell_its(cell_its, expr_attribute_ids);
        ++skipped_cells;
      }
    }
    
    // Send the lastly created tiles to storage manager
    store_tiles(result_fd, result_tiles);
    
    // Advance tile iterators 
    advance_tile_its(tile_its, expr_attribute_ids);
    ++skipped_tiles;
  }
 
  // Clean up
  delete [] result_tiles;
  delete [] tile_its;
  delete [] cell_its;
}

void QueryProcessor::filter_regular(
    const std::vector<const StorageManager::FragmentDescriptor*>& fd,
    const ExpressionTree* expression,
    const StorageManager::FragmentDescriptor* result_fd) const {
  // For easy reference
  const ArraySchema& array_schema = *(fd[0]->array_schema());
  const unsigned int attribute_num = array_schema.attribute_num();
  unsigned int fragment_num = fd.size(); 
  
  // Create result tiles 
  Tile** result_tiles = new Tile*[attribute_num+1];

  // Get the attribute names participating as variables in the expression
  const std::set<std::string>& expr_attribute_names = expression->vars();

  // Get the ids of the attribute names involved in the expression, and
  // their compliment
  std::pair<ArraySchema::AttributeIds, ArraySchema::AttributeIds>
    ret = array_schema.get_attribute_ids(expr_attribute_names);
  const ArraySchema::AttributeIds& expr_attribute_ids = ret.first;
  ArraySchema::AttributeIds& non_expr_attribute_ids = ret.second;

  // Remove the coordinates id (attribute_num) from non_expr_attribute_ids.
  // Tiles and cells associated with the coordinates will be advanced along with
  // tiles and cells corresponding to the expr_attribute_ids. 
  non_expr_attribute_ids.pop_back();

  // Create and initialize tile iterators
  StorageManager::const_iterator** tile_its = 
      new StorageManager::const_iterator*[fragment_num];
  StorageManager::const_iterator* tile_it_end = 
      new StorageManager::const_iterator[fragment_num]; 
  for(unsigned int i=0; i<fragment_num; ++i) {
    tile_its[i] = new StorageManager::const_iterator[attribute_num+1];
    initialize_tile_its(fd[i], tile_its[i], tile_it_end[i]);
  }

  // Create cell iterators
  Tile::const_iterator** cell_its = 
      new Tile::const_iterator*[fragment_num];
  Tile::const_iterator* cell_it_end = 
      new Tile::const_iterator[fragment_num];
  for(unsigned int i=0; i<fragment_num; ++i) { 
    cell_its[i] = new Tile::const_iterator[attribute_num+1];
    initialize_cell_its(tile_its[i], cell_its[i], expr_attribute_ids);
    cell_its[i][attribute_num] = (*tile_its[i][attribute_num]).begin();
    cell_it_end[i] = (*tile_its[i][attribute_num]).end();
  }

  // Auxiliary variable
  bool* non_expr_cell_its_initialized = new bool[fragment_num];
  for(unsigned int i=0; i<fragment_num; ++i)
    non_expr_cell_its_initialized[i] = false;

  // Auxiliary variables for proper retrieval of qualifying tiles and cells.
  int64_t* skipped_tiles = new int64_t[fragment_num];
  uint64_t* skipped_cells = new uint64_t[fragment_num];
  for(unsigned int i=0; i<fragment_num; ++i) {
    skipped_tiles[i] = 0;
    skipped_cells[i] = 0;
  }

  // Get the index of the fragment from which we will get the next cell
  // to check against the filter condition. 
  int next_fragment_index =
      get_next_fragment_index(tile_its, tile_it_end, fragment_num, 
                              cell_its, cell_it_end, 
                              expr_attribute_ids,
                              skipped_tiles, skipped_cells,
                              non_expr_cell_its_initialized,
                              array_schema);

  uint64_t tile_id;
  bool first_cell = true;

  // Iterate over all cells, until there are no more cells
  while(next_fragment_index != -1) {
    // For easy reference
    Tile::const_iterator* next_cell_its = cell_its[next_fragment_index];
    Tile::const_iterator& next_cell_it_end = cell_it_end[next_fragment_index];
    StorageManager::const_iterator* next_tile_its = 
        tile_its[next_fragment_index];
    StorageManager::const_iterator& next_tile_it_end = 
        tile_it_end[next_fragment_index];

    if(cell_satisfies_expression(array_schema, next_cell_its, 
                                 expr_attribute_ids, expression)) {

      if(first_cell) { // To initialize tile_id and result_tiles
        tile_id = next_cell_its[attribute_num].tile()->tile_id();
        first_cell = false;
        new_tiles(array_schema, tile_id, result_tiles);
      }
      if(skipped_tiles[next_fragment_index]) {
        advance_tile_its(next_tile_its, non_expr_attribute_ids, 
                         skipped_tiles[next_fragment_index]);
        skipped_tiles[next_fragment_index] = 0;
      }
      if(!non_expr_cell_its_initialized[next_fragment_index]) {
        initialize_cell_its(next_tile_its, next_cell_its, 
                            non_expr_attribute_ids);
        non_expr_cell_its_initialized[next_fragment_index] = true;
      }
      if(skipped_cells[next_fragment_index]) {
        advance_cell_its(next_cell_its, non_expr_attribute_ids, 
                         skipped_cells[next_fragment_index]);
        skipped_cells[next_fragment_index] = 0;
      }
      if(next_cell_its[0].tile()->tile_id() != tile_id) {
        // Change tile
        tile_id = next_cell_its[0].tile()->tile_id();
        store_tiles(result_fd, result_tiles);
        new_tiles(array_schema, tile_id, result_tiles);
      } 
      append_cell(next_cell_its, result_tiles, attribute_num);
      advance_cell_tile_its(attribute_num, next_cell_its, next_cell_it_end,
                            next_tile_its, next_tile_it_end);
    } else {
      advance_cell_tile_its(
          attribute_num, 
          next_cell_its, next_cell_it_end,
          next_tile_its, next_tile_it_end,
          expr_attribute_ids,
          skipped_tiles[next_fragment_index],
          skipped_cells[next_fragment_index],
          non_expr_cell_its_initialized[next_fragment_index]);           
    }

    next_fragment_index =
        get_next_fragment_index(tile_its, tile_it_end,
                                fragment_num, 
                                cell_its, cell_it_end, 
                                expr_attribute_ids,
                                skipped_tiles,
                                skipped_cells,
                                non_expr_cell_its_initialized,
                                array_schema);
  }

  // Send the lastly created tiles to storage manager
  store_tiles(result_fd, result_tiles);

  // Clean up
  delete [] result_tiles;
  for(unsigned int i=0; i<fragment_num; ++i) 
    delete [] tile_its[i];
  delete [] tile_its;
  delete [] tile_it_end;
  for(unsigned int i=0; i<fragment_num; ++i) 
    delete [] cell_its[i];
  delete [] cell_its;
  delete [] cell_it_end;
  delete [] skipped_cells;
  delete [] skipped_tiles;
  delete [] non_expr_cell_its_initialized;
}

void QueryProcessor::get_next_join_fragment_indexes_irregular(
    const std::vector<const StorageManager::FragmentDescriptor*>& fd_A,
    StorageManager::const_iterator** tile_its_A,
    StorageManager::const_iterator* tile_it_end_A,
    Tile::const_iterator** cell_its_A,
    Tile::const_iterator* cell_it_end_A,
    uint64_t* skipped_tiles_A, uint64_t* skipped_cells_A,
    bool* attribute_cell_its_initialized_A,
    bool* coordinate_cell_its_initialized_A,
    const std::vector<const StorageManager::FragmentDescriptor*>& fd_B,
    StorageManager::const_iterator** tile_its_B,
    StorageManager::const_iterator* tile_it_end_B,
    Tile::const_iterator** cell_its_B,
    Tile::const_iterator* cell_it_end_B,
    uint64_t* skipped_tiles_B, uint64_t* skipped_cells_B,
    bool* attribute_cell_its_initialized_B, 
    bool* coordinate_cell_its_initialized_B,
    bool& fragment_indexes_initialized,
    int& next_fragment_index_A, int& next_fragment_index_B) const {
  // For easy reference 
  unsigned int fragment_num_A = fd_A.size();
  unsigned int fragment_num_B = fd_B.size();
  const ArraySchema& array_schema_A = *(fd_A[0]->array_schema());
  const ArraySchema& array_schema_B = *(fd_B[0]->array_schema());
  unsigned int attribute_num_A = array_schema_A.attribute_num();
  unsigned int attribute_num_B = array_schema_B.attribute_num();

  // Initialization
  if(!fragment_indexes_initialized) { 
    next_fragment_index_A = get_next_fragment_index(
        fd_A, tile_its_A, tile_it_end_A,
        cell_its_A, cell_it_end_A,
        skipped_tiles_A, skipped_cells_A,
        attribute_cell_its_initialized_A,
        coordinate_cell_its_initialized_A);
    if(next_fragment_index_A == -1)
      return;
    next_fragment_index_B = get_next_fragment_index(
        fd_B, tile_its_B, tile_it_end_B,
        cell_its_B, cell_it_end_B,
        skipped_tiles_B, skipped_cells_B,
        attribute_cell_its_initialized_B,
        coordinate_cell_its_initialized_B);
    if(next_fragment_index_B == -1)
      return;
    fragment_indexes_initialized = true;
  } else { // There was a join result before
    // Advance iterators in A and find new fragment index 
    advance_cell_tile_its(
        cell_its_A[next_fragment_index_A][attribute_num_A],
        cell_it_end_A[next_fragment_index_A],
        tile_its_A[next_fragment_index_A][attribute_num_A],
        skipped_tiles_A[next_fragment_index_A],
        skipped_cells_A[next_fragment_index_A],
        attribute_cell_its_initialized_A[next_fragment_index_A],
        coordinate_cell_its_initialized_A[next_fragment_index_A]);
    next_fragment_index_A = get_next_fragment_index(
        fd_A, tile_its_A, tile_it_end_A,
        cell_its_A, cell_it_end_A,
        skipped_tiles_A, skipped_cells_A,
        attribute_cell_its_initialized_A,
        coordinate_cell_its_initialized_A);
    if(next_fragment_index_A == -1)
      return;
    // Advance iterators in B and find new fragment index 
    advance_cell_tile_its(
        cell_its_B[next_fragment_index_B][attribute_num_B],
        cell_it_end_B[next_fragment_index_B],
        tile_its_B[next_fragment_index_B][attribute_num_B],
        skipped_tiles_B[next_fragment_index_B],
        skipped_cells_B[next_fragment_index_B],
        attribute_cell_its_initialized_B[next_fragment_index_B],
        coordinate_cell_its_initialized_B[next_fragment_index_B]);
    next_fragment_index_B = get_next_fragment_index(
        fd_B, tile_its_B, tile_it_end_B,
        cell_its_B, cell_it_end_B,
        skipped_tiles_B, skipped_cells_B,
        attribute_cell_its_initialized_B,
        coordinate_cell_its_initialized_B);
    if(next_fragment_index_B == -1)
      return;
  }

  // Iterate until a joining result is found, or the cells in one
  // of the arrays are exhausted.
  while(true) {     
    // Compare tile to tile, or cell to tile.
    // This may advance/skip entire tiles.
    if(!coordinate_cell_its_initialized_A[next_fragment_index_A] || 
       !coordinate_cell_its_initialized_B[next_fragment_index_B]) {
      // Initializations for easy reference
      std::vector<double> coord_A_lower, coord_A_upper;
      std::vector<double> coord_B_lower, coord_B_upper;
      if(coordinate_cell_its_initialized_A[next_fragment_index_A]) {
        coord_A_lower = *cell_its_A[next_fragment_index_A][attribute_num_A];
        coord_A_upper = *cell_its_A[next_fragment_index_A][attribute_num_A];
      } else {
        coord_A_lower =  
            tile_its_A[next_fragment_index_A][attribute_num_A].
                       bounding_coordinates().first;
        coord_A_upper =  
            tile_its_A[next_fragment_index_A][attribute_num_A].
                       bounding_coordinates().second;
      }
      if(coordinate_cell_its_initialized_B[next_fragment_index_B]) {
        coord_B_lower = *cell_its_B[next_fragment_index_B][attribute_num_B];
        coord_B_upper = *cell_its_B[next_fragment_index_B][attribute_num_B];
      } else {
        coord_B_lower =  
            tile_its_B[next_fragment_index_B][attribute_num_B].
                       bounding_coordinates().first;
        coord_B_upper =  
            tile_its_B[next_fragment_index_B][attribute_num_B].
                       bounding_coordinates().second;
      }
      if(array_schema_A.succeeds(coord_A_lower, coord_B_upper)) {
        // Advance iterators in B
        if(coordinate_cell_its_initialized_B[next_fragment_index_B]) {
          advance_cell_tile_its(
              cell_its_B[next_fragment_index_B][attribute_num_B],
              cell_it_end_B[next_fragment_index_B],
              tile_its_B[next_fragment_index_B][attribute_num_B],
              skipped_tiles_B[next_fragment_index_B],
              skipped_cells_B[next_fragment_index_B],
              attribute_cell_its_initialized_B[next_fragment_index_B],
              coordinate_cell_its_initialized_B[next_fragment_index_B]);
        } else {
          ++tile_its_B[next_fragment_index_B][attribute_num_B];
          attribute_cell_its_initialized_B[next_fragment_index_B] = false;
          coordinate_cell_its_initialized_B[next_fragment_index_B] = false;
          ++skipped_tiles_B[next_fragment_index_B];
          skipped_cells_B[next_fragment_index_B] = 0;
        }
        next_fragment_index_B = get_next_fragment_index(
            fd_B, tile_its_B, tile_it_end_B,
            cell_its_B, cell_it_end_B,
            skipped_tiles_B, skipped_cells_B,
            attribute_cell_its_initialized_B,
            coordinate_cell_its_initialized_B);
        if(next_fragment_index_B == -1)
          return;
      } else if(array_schema_A.succeeds(coord_B_lower, coord_A_upper)) {
        // Advance iterators in A
        if(coordinate_cell_its_initialized_A[next_fragment_index_A]) {
          advance_cell_tile_its(
              cell_its_A[next_fragment_index_A][attribute_num_A],
              cell_it_end_A[next_fragment_index_A],
              tile_its_A[next_fragment_index_A][attribute_num_A],
              skipped_tiles_A[next_fragment_index_A],
              skipped_cells_A[next_fragment_index_A],
              attribute_cell_its_initialized_A[next_fragment_index_A],
              coordinate_cell_its_initialized_A[next_fragment_index_A]);
        } else {
          ++tile_its_A[next_fragment_index_A][attribute_num_A];
          attribute_cell_its_initialized_A[next_fragment_index_A] = false;
          coordinate_cell_its_initialized_A[next_fragment_index_A] = false;
          ++skipped_tiles_A[next_fragment_index_A];
          skipped_cells_A[next_fragment_index_A] = 0;
        }
        next_fragment_index_A = get_next_fragment_index(
            fd_A, tile_its_A, tile_it_end_A,
            cell_its_A, cell_it_end_A,
            skipped_tiles_A, skipped_cells_A,
            attribute_cell_its_initialized_A,
            coordinate_cell_its_initialized_A);
        if(next_fragment_index_A == -1)
          return;
      } else { // Tiles may join, initialize coordinate cell iterators
        if(!coordinate_cell_its_initialized_A[next_fragment_index_A]) {
          cell_its_A[next_fragment_index_A][attribute_num_A] = 
              (*tile_its_A[next_fragment_index_A][attribute_num_A]).begin();
          cell_it_end_A[next_fragment_index_A] = 
              (*tile_its_A[next_fragment_index_A][attribute_num_A]).end();
          coordinate_cell_its_initialized_A[next_fragment_index_A] = true;
        }
        if(!coordinate_cell_its_initialized_B[next_fragment_index_B]) {
          cell_its_B[next_fragment_index_B][attribute_num_B] = 
              (*tile_its_B[next_fragment_index_B][attribute_num_B]).begin();
          cell_it_end_B[next_fragment_index_B] = 
              (*tile_its_B[next_fragment_index_B][attribute_num_B]).end();
          coordinate_cell_its_initialized_B[next_fragment_index_B] = true;
        } 
      }
    }

    // Compare cell to cell.
    // This may advance individual cells.
    if(coordinate_cell_its_initialized_A[next_fragment_index_A] && 
       coordinate_cell_its_initialized_B[next_fragment_index_B]) {
      // Join result found
      if(cell_its_A[next_fragment_index_A][attribute_num_A] == 
         cell_its_B[next_fragment_index_B][attribute_num_B]) {
        return;
      } else if(array_schema_A.precedes(
                    cell_its_A[next_fragment_index_A][attribute_num_A],
                    cell_its_B[next_fragment_index_B][attribute_num_B])) {
        // Advance iterators in A 
        advance_cell_tile_its(
            cell_its_A[next_fragment_index_A][attribute_num_A],
            cell_it_end_A[next_fragment_index_A],
            tile_its_A[next_fragment_index_A][attribute_num_A],
            skipped_tiles_A[next_fragment_index_A],
            skipped_cells_A[next_fragment_index_A],
            attribute_cell_its_initialized_A[next_fragment_index_A],
            coordinate_cell_its_initialized_A[next_fragment_index_A]);
        next_fragment_index_A = get_next_fragment_index(
            fd_A, tile_its_A, tile_it_end_A,
            cell_its_A, cell_it_end_A,
            skipped_tiles_A, skipped_cells_A,
            attribute_cell_its_initialized_A,
            coordinate_cell_its_initialized_A);
        if(next_fragment_index_A == -1)
          return;
      } else {
        // Advance iterators in B 
        advance_cell_tile_its(
            cell_its_B[next_fragment_index_B][attribute_num_B],
            cell_it_end_B[next_fragment_index_B],
            tile_its_B[next_fragment_index_B][attribute_num_B],
            skipped_tiles_B[next_fragment_index_B],
            skipped_cells_B[next_fragment_index_B],
            attribute_cell_its_initialized_B[next_fragment_index_B],
            coordinate_cell_its_initialized_B[next_fragment_index_B]);
        next_fragment_index_B = get_next_fragment_index(
            fd_B, tile_its_B, tile_it_end_B,
            cell_its_B, cell_it_end_B,
            skipped_tiles_B, skipped_cells_B,
            attribute_cell_its_initialized_B,
            coordinate_cell_its_initialized_B);
        if(next_fragment_index_B == -1)
          return;
      }
    }
  }
}

void QueryProcessor::get_next_join_fragment_indexes_regular(
    const std::vector<const StorageManager::FragmentDescriptor*>& fd_A,
    StorageManager::const_iterator** tile_its_A,
    StorageManager::const_iterator* tile_it_end_A,
    Tile::const_iterator** cell_its_A,
    Tile::const_iterator* cell_it_end_A,
    uint64_t* skipped_tiles_A, uint64_t* skipped_cells_A,
    bool* attribute_cell_its_initialized_A,
    bool* coordinate_cell_its_initialized_A,
    const std::vector<const StorageManager::FragmentDescriptor*>& fd_B,
    StorageManager::const_iterator** tile_its_B,
    StorageManager::const_iterator* tile_it_end_B,
    Tile::const_iterator** cell_its_B,
    Tile::const_iterator* cell_it_end_B,
    uint64_t* skipped_tiles_B, uint64_t* skipped_cells_B,
    bool* attribute_cell_its_initialized_B, 
    bool* coordinate_cell_its_initialized_B,
    bool& fragment_indexes_initialized,
    int& next_fragment_index_A, int& next_fragment_index_B) const {
  // For easy reference 
  unsigned int fragment_num_A = fd_A.size();
  unsigned int fragment_num_B = fd_B.size();
  const ArraySchema& array_schema_A = *(fd_A[0]->array_schema());
  const ArraySchema& array_schema_B = *(fd_B[0]->array_schema());
  unsigned int attribute_num_A = array_schema_A.attribute_num();
  unsigned int attribute_num_B = array_schema_B.attribute_num();

  // Initialization
  if(!fragment_indexes_initialized) { 
    next_fragment_index_A = get_next_fragment_index(
        fd_A, tile_its_A, tile_it_end_A,
        cell_its_A, cell_it_end_A,
        skipped_tiles_A, skipped_cells_A,
        attribute_cell_its_initialized_A,
        coordinate_cell_its_initialized_A);
    if(next_fragment_index_A == -1)
      return;
    next_fragment_index_B = get_next_fragment_index(
        fd_B, tile_its_B, tile_it_end_B,
        cell_its_B, cell_it_end_B,
        skipped_tiles_B, skipped_cells_B,
        attribute_cell_its_initialized_B,
        coordinate_cell_its_initialized_B);
    if(next_fragment_index_B == -1)
      return;
    fragment_indexes_initialized = true;
  } else { // There was a join result before
    // Advance iterators in A and find new fragment index 
    advance_cell_tile_its(
        cell_its_A[next_fragment_index_A][attribute_num_A],
        cell_it_end_A[next_fragment_index_A],
        tile_its_A[next_fragment_index_A][attribute_num_A],
        skipped_tiles_A[next_fragment_index_A],
        skipped_cells_A[next_fragment_index_A],
        attribute_cell_its_initialized_A[next_fragment_index_A],
        coordinate_cell_its_initialized_A[next_fragment_index_A]);
    next_fragment_index_A = get_next_fragment_index(
        fd_A, tile_its_A, tile_it_end_A,
        cell_its_A, cell_it_end_A,
        skipped_tiles_A, skipped_cells_A,
        attribute_cell_its_initialized_A,
        coordinate_cell_its_initialized_A);
    if(next_fragment_index_A == -1)
      return;
    // Advance iterators in B and find new fragment index 
    advance_cell_tile_its(
        cell_its_B[next_fragment_index_B][attribute_num_B],
        cell_it_end_B[next_fragment_index_B],
        tile_its_B[next_fragment_index_B][attribute_num_B],
        skipped_tiles_B[next_fragment_index_B],
        skipped_cells_B[next_fragment_index_B],
        attribute_cell_its_initialized_B[next_fragment_index_B],
        coordinate_cell_its_initialized_B[next_fragment_index_B]);
    next_fragment_index_B = get_next_fragment_index(
        fd_B, tile_its_B, tile_it_end_B,
        cell_its_B, cell_it_end_B,
        skipped_tiles_B, skipped_cells_B,
        attribute_cell_its_initialized_B,
        coordinate_cell_its_initialized_B);
    if(next_fragment_index_B == -1)
      return;
  }

  // Iterate until a joining result is found, or the cells in one
  // of the arrays are exhausted.
  while(true) {     
    // Compare tile to tile, or cell to tile.
    // This may advance/skip entire tiles.
    if(!coordinate_cell_its_initialized_A[next_fragment_index_A] || 
       !coordinate_cell_its_initialized_B[next_fragment_index_B]) {
      // Initializations for easy reference
      uint64_t tile_id_A = 
          tile_its_A[next_fragment_index_A][attribute_num_A].tile_id();
      uint64_t tile_id_B = 
          tile_its_B[next_fragment_index_B][attribute_num_B].tile_id();
      if(tile_id_A > tile_id_B) {
        // Advance iterators in B
        if(coordinate_cell_its_initialized_B[next_fragment_index_B]) {
          advance_cell_tile_its(
              cell_its_B[next_fragment_index_B][attribute_num_B],
              cell_it_end_B[next_fragment_index_B],
              tile_its_B[next_fragment_index_B][attribute_num_B],
              skipped_tiles_B[next_fragment_index_B],
              skipped_cells_B[next_fragment_index_B],
              attribute_cell_its_initialized_B[next_fragment_index_B],
              coordinate_cell_its_initialized_B[next_fragment_index_B]);
        } else {
          ++tile_its_B[next_fragment_index_B][attribute_num_B];
          attribute_cell_its_initialized_B[next_fragment_index_B] = false;
          coordinate_cell_its_initialized_B[next_fragment_index_B] = false;
          ++skipped_tiles_B[next_fragment_index_B];
          skipped_cells_B[next_fragment_index_B] = 0;
        }
        next_fragment_index_B = get_next_fragment_index(
            fd_B, tile_its_B, tile_it_end_B,
            cell_its_B, cell_it_end_B,
            skipped_tiles_B, skipped_cells_B,
            attribute_cell_its_initialized_B,
            coordinate_cell_its_initialized_B);
        if(next_fragment_index_B == -1)
          return;
      } else if(tile_id_A < tile_id_B) {
        // Advance iterators in A
        if(coordinate_cell_its_initialized_A[next_fragment_index_A]) {
          advance_cell_tile_its(
              cell_its_A[next_fragment_index_A][attribute_num_A],
              cell_it_end_A[next_fragment_index_A],
              tile_its_A[next_fragment_index_A][attribute_num_A],
              skipped_tiles_A[next_fragment_index_A],
              skipped_cells_A[next_fragment_index_A],
              attribute_cell_its_initialized_A[next_fragment_index_A],
              coordinate_cell_its_initialized_A[next_fragment_index_A]);
        } else {
          ++tile_its_A[next_fragment_index_A][attribute_num_A];
          attribute_cell_its_initialized_A[next_fragment_index_A] = false;
          coordinate_cell_its_initialized_A[next_fragment_index_A] = false;
          ++skipped_tiles_A[next_fragment_index_A];
          skipped_cells_A[next_fragment_index_A] = 0;
        }
        next_fragment_index_A = get_next_fragment_index(
            fd_A, tile_its_A, tile_it_end_A,
            cell_its_A, cell_it_end_A,
            skipped_tiles_A, skipped_cells_A,
            attribute_cell_its_initialized_A,
            coordinate_cell_its_initialized_A);
        if(next_fragment_index_A == -1)
          return;
      } else {  // tile_id_A == tile_id_B
        // Tiles may join, initialize coordinate cell iterators
        if(!coordinate_cell_its_initialized_A[next_fragment_index_A]) {
          cell_its_A[next_fragment_index_A][attribute_num_A] = 
              (*tile_its_A[next_fragment_index_A][attribute_num_A]).begin();
          cell_it_end_A[next_fragment_index_A] = 
              (*tile_its_A[next_fragment_index_A][attribute_num_A]).end();
          coordinate_cell_its_initialized_A[next_fragment_index_A] = true;
        }
        if(!coordinate_cell_its_initialized_B[next_fragment_index_B]) {
          cell_its_B[next_fragment_index_B][attribute_num_B] = 
              (*tile_its_B[next_fragment_index_B][attribute_num_B]).begin();
          cell_it_end_B[next_fragment_index_B] = 
              (*tile_its_B[next_fragment_index_B][attribute_num_B]).end();
          coordinate_cell_its_initialized_B[next_fragment_index_B] = true;
        } 
      }
    }

    // Compare cell to cell.
    // This may advance individual cells.
    if(coordinate_cell_its_initialized_A[next_fragment_index_A] && 
       coordinate_cell_its_initialized_B[next_fragment_index_B]) {
      // Join result found
      if(cell_its_A[next_fragment_index_A][attribute_num_A] == 
         cell_its_B[next_fragment_index_B][attribute_num_B]) {
        return;
      } else if(array_schema_A.precedes(
                    cell_its_A[next_fragment_index_A][attribute_num_A],
                    cell_its_B[next_fragment_index_B][attribute_num_B])) {
        // Advance iterators in A 
        advance_cell_tile_its(
            cell_its_A[next_fragment_index_A][attribute_num_A],
            cell_it_end_A[next_fragment_index_A],
            tile_its_A[next_fragment_index_A][attribute_num_A],
            skipped_tiles_A[next_fragment_index_A],
            skipped_cells_A[next_fragment_index_A],
            attribute_cell_its_initialized_A[next_fragment_index_A],
            coordinate_cell_its_initialized_A[next_fragment_index_A]);
        next_fragment_index_A = get_next_fragment_index(
            fd_A, tile_its_A, tile_it_end_A,
            cell_its_A, cell_it_end_A,
            skipped_tiles_A, skipped_cells_A,
            attribute_cell_its_initialized_A,
            coordinate_cell_its_initialized_A);
        if(next_fragment_index_A == -1)
          return;
      } else {
        // Advance iterators in B 
        advance_cell_tile_its(
            cell_its_B[next_fragment_index_B][attribute_num_B],
            cell_it_end_B[next_fragment_index_B],
            tile_its_B[next_fragment_index_B][attribute_num_B],
            skipped_tiles_B[next_fragment_index_B],
            skipped_cells_B[next_fragment_index_B],
            attribute_cell_its_initialized_B[next_fragment_index_B],
            coordinate_cell_its_initialized_B[next_fragment_index_B]);
        next_fragment_index_B = get_next_fragment_index(
            fd_B, tile_its_B, tile_it_end_B,
            cell_its_B, cell_it_end_B,
            skipped_tiles_B, skipped_cells_B,
            attribute_cell_its_initialized_B,
            coordinate_cell_its_initialized_B);
        if(next_fragment_index_B == -1)
          return;
      }
    }
  }
}

int QueryProcessor::get_next_fragment_index(
      StorageManager::const_iterator** tile_its,
      StorageManager::const_iterator* tile_it_end,
      unsigned int fragment_num,
      Tile::const_iterator** cell_its,
      Tile::const_iterator* cell_it_end,
      const ArraySchema& array_schema) const {
  // For easy reference
  unsigned int attribute_num = array_schema.attribute_num();

  // Holds the (potentially multiple) indexes of the candidate fragments
  // from which we will get the next cell for consolidation. Note though 
  // that only the largest (last) index will be returned, which corresponds
  // to the latest update.
  std::vector<int> next_fragment_index;
  next_fragment_index.reserve(fragment_num);

  // Loop for as long as there is a NULL cell (i.e., a deletion)
  bool null;
  do {
    next_fragment_index.clear();

    for(unsigned int i=0; i<fragment_num; ++i) {
      if(cell_its[i][attribute_num] != cell_it_end[i]) {
        if(next_fragment_index.size() == 0 || cell_its[i][attribute_num] == 
           cell_its[next_fragment_index[0]][attribute_num]) {
          next_fragment_index.push_back(i);
        } else if(array_schema.precedes(
                      cell_its[i][attribute_num], 
                      cell_its[next_fragment_index[0]][attribute_num])) {
          next_fragment_index.clear();
          next_fragment_index.push_back(i);
        } 
      }
    }

    if(next_fragment_index.size() == 0)
      return -1;

    int num_of_iterators_to_advance = next_fragment_index.size()-1; 
    null = is_null(cell_its[next_fragment_index.back()][0]);
    if(null) 
      ++num_of_iterators_to_advance;

    // Advance cell (and potentially tile) iterators.
    // If the cell is deleted, all iterators are advanced, otherwise all except
    // for the last one.
    for(int i=0; i<num_of_iterators_to_advance; ++i)
      advance_cell_tile_its(attribute_num, 
                            cell_its[next_fragment_index[i]],
                            cell_it_end[next_fragment_index[i]],
                            tile_its[next_fragment_index[i]],
                            tile_it_end[next_fragment_index[i]]);

    if(!null)
      return next_fragment_index.back();

  } while(true);
}

int QueryProcessor::get_next_fragment_index(
      StorageManager::const_iterator** tile_its,
      StorageManager::const_iterator* tile_it_end,
      unsigned int fragment_num,
      Tile::const_iterator** cell_its,
      Tile::const_iterator* cell_it_end,
      const std::vector<unsigned int>& attribute_ids,
      int64_t* skipped_tiles,
      uint64_t* skipped_cells,
      bool* non_cell_its_initialized,
      const ArraySchema& array_schema) const {
  // For easy reference
  unsigned int attribute_num = array_schema.attribute_num();

  // Holds the (potentially multiple) indexes of the candidate fragments
  // from which we will get the next cell for consolidation. Note though 
  // that only the largest (last) index will be returned, which corresponds
  // to the latest update.
  std::vector<int> next_fragment_index;
  next_fragment_index.reserve(fragment_num);
  // Loop for as long as there is a NULL cell (i.e., a deletion)
  bool null;
  do {
    next_fragment_index.clear();

    for(unsigned int i=0; i<fragment_num; ++i) {
      if(cell_its[i][attribute_num] != cell_it_end[i]) {
        if(next_fragment_index.size() == 0 || cell_its[i][attribute_num] == 
           cell_its[next_fragment_index[0]][attribute_num]) {
          next_fragment_index.push_back(i);
        } else if(array_schema.precedes(
                      cell_its[i][attribute_num], 
                      cell_its[next_fragment_index[0]][attribute_num])) {
          next_fragment_index.clear();
          next_fragment_index.push_back(i);
        } 
      }
    }

    if(next_fragment_index.size() == 0)
      return -1;

    int num_of_iterators_to_advance = next_fragment_index.size()-1; 
    null = is_null(cell_its[next_fragment_index.back()][attribute_ids[0]]);
    if(null)
      ++num_of_iterators_to_advance;

    // Advance cell (and potentially tile) iterators.
    // If the cell is deleted, all iterators are advanced, otherwise all except
    // for the last one.
    for(int i=0; i<num_of_iterators_to_advance; ++i) 
      advance_cell_tile_its(attribute_num, 
                            cell_its[next_fragment_index[i]],
                            cell_it_end[next_fragment_index[i]],
                            tile_its[next_fragment_index[i]],
                            tile_it_end[next_fragment_index[i]],
                            attribute_ids,
                            skipped_tiles[next_fragment_index[i]],
                            skipped_cells[next_fragment_index[i]],
                            non_cell_its_initialized[next_fragment_index[i]]);

    if(!null)
      return next_fragment_index.back();

  } while(true);
}

int QueryProcessor::get_next_fragment_index(
      const std::vector<const StorageManager::FragmentDescriptor*>& fd,
      RankOverlapVector::const_iterator* tile_rank_it,
      RankOverlapVector::const_iterator* tile_rank_it_end,
      const Tile*** tiles,
      unsigned int fragment_num,
      Tile::const_iterator** cell_its,
      Tile::const_iterator* cell_it_end,
      uint64_t* skipped_cells,
      const ArraySchema& array_schema) const {
  // For easy reference
  unsigned int attribute_num = array_schema.attribute_num();

  // Holds the (potentially multiple) indexes of the candidate fragments
  // from which we will get the next cell for consolidation. Note though 
  // that only the largest (last) index will be returned, which corresponds
  // to the latest update.
  std::vector<int> next_fragment_index;
  next_fragment_index.reserve(fragment_num);

  // Loop for as long as there is a NULL cell (i.e., a deletion)
  bool null;
  do {
    next_fragment_index.clear();

    for(unsigned int i=0; i<fragment_num; ++i) {
      if(cell_its[i][attribute_num] != cell_it_end[i]) {
        if(next_fragment_index.size() == 0 || cell_its[i][attribute_num] == 
           cell_its[next_fragment_index[0]][attribute_num]) {
          next_fragment_index.push_back(i);
        } else if(array_schema.precedes(
                      cell_its[i][attribute_num], 
                      cell_its[next_fragment_index[0]][attribute_num])) {
          next_fragment_index.clear();
          next_fragment_index.push_back(i);
        } 
      }
    }

    if(next_fragment_index.size() == 0)
      return -1;

    int latest_fragment_index = next_fragment_index.back();
    int num_of_iterators_to_advance = next_fragment_index.size()-1; 

    // Synchronize cell iterators
    if(skipped_cells[latest_fragment_index]) {
      advance_cell_its(attribute_num, cell_its[latest_fragment_index], 
                       skipped_cells[latest_fragment_index]);
      skipped_cells[latest_fragment_index] = 0;
    }

    null = is_null(cell_its[latest_fragment_index][0]);
    if(null) 
      ++num_of_iterators_to_advance;

    // Advance cell (and potentially tile) iterators.
    // If the cell is deleted, all iterators are advanced, otherwise all except
    // for the last one.
    for(int i=0; i<num_of_iterators_to_advance; ++i) {
      advance_cell_tile_its(fd[next_fragment_index[i]], attribute_num,
                            cell_its[next_fragment_index[i]],
                            cell_it_end[next_fragment_index[i]],
                            tile_rank_it[next_fragment_index[i]], 
                            tile_rank_it_end[next_fragment_index[i]], 
                            fragment_num, tiles[i],
                            skipped_cells[next_fragment_index[i]]);
    }

    if(!null)
      return next_fragment_index.back();

  } while(true);

}

int QueryProcessor::get_next_fragment_index(
    const std::vector<const StorageManager::FragmentDescriptor*>& fd,
    StorageManager::const_iterator** tile_its,
    StorageManager::const_iterator* tile_it_end,
    Tile::const_iterator** cell_its,
    Tile::const_iterator* cell_it_end,
    uint64_t* skipped_tiles, uint64_t* skipped_cells,
    bool* attribute_cell_its_initialized,
    bool* coordinate_cell_its_initialized) const {
  // For easy reference
  const ArraySchema& array_schema = *(fd[0]->array_schema());
  unsigned int attribute_num = array_schema.attribute_num();
  unsigned int fragment_num = fd.size();

  // Holds the (potentially multiple) indexes of the candidate fragments
  // from which we will get the next cell for consolidation. Note though 
  // that only the largest (last) index will be returned, which corresponds
  // to the latest update.
  std::vector<int> next_fragment_index;
  next_fragment_index.reserve(fragment_num);

  // The loop will be broken when the next fragment is found
  while(true) {
    next_fragment_index.clear();

    for(unsigned int i=0; i<fragment_num; ++i) {
      if(tile_its[i][attribute_num] != tile_it_end[i]) {
        if(next_fragment_index.size() == 0 || 
           coincides( 
               tile_its[i][attribute_num],
               cell_its[i][attribute_num],
               coordinate_cell_its_initialized[i],
               tile_its[next_fragment_index[0]][attribute_num],
               cell_its[next_fragment_index[0]][attribute_num],
               coordinate_cell_its_initialized[next_fragment_index[0]],
               array_schema)) {
          next_fragment_index.push_back(i);
        } else if(precedes(
                      tile_its[i][attribute_num],
                      cell_its[i][attribute_num],
                      coordinate_cell_its_initialized[i],
                      tile_its[next_fragment_index[0]][attribute_num],
                      cell_its[next_fragment_index[0]][attribute_num],
                      coordinate_cell_its_initialized[next_fragment_index[0]],
                      array_schema)) {
          next_fragment_index.clear();
          next_fragment_index.push_back(i);
        } 
      }
    }

    if(next_fragment_index.size() == 0)
      return -1;

    int latest_fragment_index = next_fragment_index.back();

    // Retrieve a single attribute tile to check for null (i.e., deletion)
    uint64_t tile_offset = 
        tile_its[latest_fragment_index][attribute_num].rank() -
        tile_its[latest_fragment_index][0].rank();
    uint64_t cell_offset =
        (!coordinate_cell_its_initialized[latest_fragment_index]) ? 
            0 : cell_its[latest_fragment_index][attribute_num].pos();
    Tile::const_iterator cell_it = 
        (*(tile_its[latest_fragment_index][0] + tile_offset)).begin() +
        cell_offset;
    bool null = is_null(cell_it);

    // Initialize coordinates if not already initialized
    for(int i=0; i<next_fragment_index.size(); ++i) {
      if(!coordinate_cell_its_initialized[next_fragment_index[i]]) {
          cell_its[next_fragment_index[i]][attribute_num] = 
              (*tile_its[next_fragment_index[i]][attribute_num]).begin();
          cell_it_end[next_fragment_index[i]] = 
              (*tile_its[next_fragment_index[i]][attribute_num]).end();
          coordinate_cell_its_initialized[next_fragment_index[i]] = true;
      }
    }

    // Handle deletions or overwrites
    int num_of_iterators_to_advance = next_fragment_index.size()-1; 

    if(null) 
      ++num_of_iterators_to_advance;

    // Advance cell (and potentially tile) iterators.
    // If the cell is deleted, all iterators are advanced, otherwise all except
    // for the last one.
    for(int i=0; i<num_of_iterators_to_advance; ++i) {
      advance_cell_tile_its(
          cell_its[next_fragment_index[i]][attribute_num],
          cell_it_end[next_fragment_index[i]],
          tile_its[next_fragment_index[i]][attribute_num], 
          skipped_tiles[next_fragment_index[i]],
          skipped_cells[next_fragment_index[i]],
          attribute_cell_its_initialized[next_fragment_index[i]], 
          coordinate_cell_its_initialized[next_fragment_index[i]]); 
    }

    if(!null)
      return next_fragment_index.back();
  }
}

inline
void QueryProcessor::get_tiles(
    const StorageManager::FragmentDescriptor* fd, 
    uint64_t tile_id, const Tile** tiles) const {
  // For easy reference
  unsigned int attribute_num = fd->array_schema()->attribute_num();

  // Get attribute tiles
  for(unsigned int i=0; i<=attribute_num; i++) 
   tiles[i] = storage_manager_.get_tile(fd, i, tile_id);
}

inline
void QueryProcessor::get_tiles_by_rank(
    const StorageManager::FragmentDescriptor* fd, 
    uint64_t tile_rank, const Tile** tiles) const {
  // For easy reference
  unsigned int attribute_num = fd->array_schema()->attribute_num();

  // Get attribute tiles
  for(unsigned int i=0; i<=attribute_num; i++) 
   tiles[i] = storage_manager_.get_tile_by_rank(fd, i, tile_rank);
}

bool QueryProcessor::is_null(const Tile::const_iterator& cell_it) const {
  return cell_it.is_null();
}

inline
void QueryProcessor::initialize_cell_its(
    const Tile** tiles, int attribute_num,
    Tile::const_iterator* cell_its, Tile::const_iterator& cell_it_end) const {
  for(int i=0; i<=attribute_num; ++i)
    cell_its[i] = tiles[i]->begin();
  cell_it_end = tiles[attribute_num]->end();
}


inline
void QueryProcessor::initialize_cell_its(
    const Tile** tiles, int attribute_num,
    Tile::const_cell_iterator* cell_its) const {
  for(int i=0; i<attribute_num; ++i)
    cell_its[i] = tiles[i]->begin();
}

inline
void QueryProcessor::initialize_cell_its(
    const StorageManager::const_tile_iterator* tile_its, int attribute_num,
    Tile::const_cell_iterator* cell_its, 
    Tile::const_cell_iterator& cell_it_end) const {
  for(int i=0; i<=attribute_num; ++i)
    cell_its[i] = (*tile_its[i])->begin();
  cell_it_end = (*tile_its[attribute_num])->end();
}

inline
void QueryProcessor::initialize_cell_its(
    const StorageManager::const_iterator* tile_its, 
    Tile::const_iterator* cell_its, Tile::const_iterator& cell_it_end,
    const std::vector<unsigned int>& attribute_ids) const {
  for(unsigned int i=0; i<attribute_ids.size(); i++)
    cell_its[attribute_ids[i]] = (*tile_its[attribute_ids[i]]).begin();
  cell_it_end = (*tile_its[attribute_ids[0]]).end();
}

inline
void QueryProcessor::initialize_cell_its(
    const StorageManager::const_iterator* tile_its, 
    Tile::const_iterator* cell_its, 
    const std::vector<unsigned int>& attribute_ids) const {
  for(unsigned int i=0; i<attribute_ids.size(); i++)
    cell_its[attribute_ids[i]] = (*tile_its[attribute_ids[i]]).begin();
}

inline
void QueryProcessor::initialize_cell_its(
    const StorageManager::const_iterator* tile_its, unsigned int attribute_num,
    Tile::const_iterator* cell_its) const {
  for(unsigned int i=0; i<attribute_num; i++)
    cell_its[i] = (*tile_its[i]).begin();
}

inline
void QueryProcessor::initialize_tile_its(
    const StorageManager::FragmentDescriptor* fd,
    StorageManager::const_tile_iterator* tile_its, 
    StorageManager::const_tile_iterator& tile_it_end) const {
  // For easy reference
  int attribute_num = fd->array_schema()->attribute_num();

  for(int i=0; i<=attribute_num; ++i)
    tile_its[i] = storage_manager_->begin(fd, i);
  tile_it_end = storage_manager_->end(fd, attribute_num);
}

inline
void QueryProcessor::initialize_tile_its(
    const StorageManager::FragmentDescriptor* fd,
    StorageManager::const_iterator* tile_its, 
    StorageManager::const_iterator& tile_it_end,
    unsigned int end_attribute_id) const {
  // For easy reference
  unsigned int attribute_num = fd->array_schema()->attribute_num();

  for(unsigned int i=0; i<=attribute_num; i++)
    tile_its[i] = storage_manager_.begin(fd, i);
  tile_it_end = storage_manager_.end(fd, end_attribute_id);
}


void QueryProcessor::join_irregular(
    const StorageManager::FragmentDescriptor* fd_A,
    const StorageManager::FragmentDescriptor* fd_B,
    const StorageManager::FragmentDescriptor* fd_C) const {
  // For easy reference
  const ArraySchema& array_schema_A = *(fd_A->array_schema());
  const ArraySchema& array_schema_B = *(fd_B->array_schema());
  const ArraySchema& array_schema_C = *(fd_C->array_schema());
  unsigned int attribute_num_A = array_schema_A.attribute_num();
  unsigned int attribute_num_B = array_schema_B.attribute_num();
  unsigned int attribute_num_C = array_schema_C.attribute_num();

  // Create tiles 
  const Tile** tiles_A = new const Tile*[attribute_num_A+1];
  const Tile** tiles_B = new const Tile*[attribute_num_B+1];
  Tile** tiles_C = new Tile*[attribute_num_C+1];
  
  // Create and initialize tile iterators
  StorageManager::const_iterator *tile_its_A = 
      new StorageManager::const_iterator[attribute_num_A+1];
  StorageManager::const_iterator *tile_its_B = 
      new StorageManager::const_iterator[attribute_num_B+1];
  StorageManager::const_iterator tile_it_end_A;
  StorageManager::const_iterator tile_it_end_B;
  initialize_tile_its(fd_A, tile_its_A, tile_it_end_A);
  initialize_tile_its(fd_B, tile_its_B, tile_it_end_B);
  
  // Create cell iterators
  Tile::const_iterator* cell_its_A = 
      new Tile::const_iterator[attribute_num_A+1];
  Tile::const_iterator* cell_its_B = 
      new Tile::const_iterator[attribute_num_B+1];
  Tile::const_iterator cell_it_end_A, cell_it_end_B;

  // Auxiliary variables storing the number of skipped tiles when joining.
  // It is used to advance only the coordinates iterator when a tile is
  // finished/skipped, and then efficiently advance the attribute iterators only
  // when a tile joins.
  int64_t skipped_tiles_A = 0;
  int64_t skipped_tiles_B = 0;
  
  // Note that attribute cell iterators are initialized and advanced only
  // after the first join result is discovered. 
  bool attribute_cell_its_initialized_A = false;
  bool attribute_cell_its_initialized_B = false;

  // To capture the edge case where the first tiles from A and B may join
  bool coordinate_cell_its_initialized_A = false;
  bool coordinate_cell_its_initialized_B = false;

  // Initialize tiles with id 0 for C (result array)
  new_tiles(array_schema_C, 0, tiles_C); 

  // Join algorithm
  while(tile_its_A[attribute_num_A] != tile_it_end_A &&
        tile_its_B[attribute_num_B] != tile_it_end_B) {
    // Potential join result generation
    if(may_join(tile_its_A[attribute_num_A], tile_its_B[attribute_num_B])) { 
      // Update iterators in A
      if(skipped_tiles_A) {
        advance_tile_its(attribute_num_A, tile_its_A, skipped_tiles_A);
        skipped_tiles_A = 0;
        cell_its_A[attribute_num_A] = (*tile_its_A[attribute_num_A]).begin();
        cell_it_end_A = (*tile_its_A[attribute_num_A]).end();
        coordinate_cell_its_initialized_A = true;
        attribute_cell_its_initialized_A = false;
      } else if(!coordinate_cell_its_initialized_A) {
        cell_its_A[attribute_num_A] = (*tile_its_A[attribute_num_A]).begin();
        cell_it_end_A = (*tile_its_A[attribute_num_A]).end();
        coordinate_cell_its_initialized_A = true;
      }
      // Update iterators in B
      if(skipped_tiles_B) {
        advance_tile_its(attribute_num_B, tile_its_B, skipped_tiles_B);
        skipped_tiles_B = 0;
        cell_its_B[attribute_num_B] = (*tile_its_B[attribute_num_B]).begin();
        cell_it_end_B = (*tile_its_B[attribute_num_B]).end();
        coordinate_cell_its_initialized_B = true;
        attribute_cell_its_initialized_B = false;
      } else if(!coordinate_cell_its_initialized_B) {
        cell_its_B[attribute_num_B] = (*tile_its_B[attribute_num_B]).begin();
        cell_it_end_B = (*tile_its_B[attribute_num_B]).end();
        coordinate_cell_its_initialized_B = true;
      }
      // Join the tiles
      join_tiles_irregular(attribute_num_A, tile_its_A, 
                           cell_its_A, cell_it_end_A, 
                           attribute_num_B, tile_its_B, 
                           cell_its_B, cell_it_end_B,
                           fd_C, tiles_C,
                           attribute_cell_its_initialized_A, 
                           attribute_cell_its_initialized_B);
    }

    // Check which tile precedes the other in the global order
    // Note that operator '<', when the operands are from different
    // arrays, returns true if the first tile precedes the second
    // in the global order by checking their bounding coordinates.
    if(tile_its_A[attribute_num_A] < tile_its_B[attribute_num_B]) {
      ++tile_its_A[attribute_num_A];
      ++skipped_tiles_A;
    }
    else {
      ++tile_its_B[attribute_num_B];
      ++skipped_tiles_B;
    }
  }
  
  // Send the lastly created tiles to storage manager
  store_tiles(fd_C, tiles_C);

  // Clean up
  delete [] tiles_A;
  delete [] tiles_B;
  delete [] tiles_C;
  delete [] tile_its_A;
  delete [] tile_its_B;
  delete [] cell_its_A;
  delete [] cell_its_B;
}

void QueryProcessor::join_irregular(
    const std::vector<const StorageManager::FragmentDescriptor*>& fd_A,
    const std::vector<const StorageManager::FragmentDescriptor*>& fd_B,
    const StorageManager::FragmentDescriptor* fd_C) const {
  // For easy reference
  const ArraySchema& array_schema_A = *(fd_A[0]->array_schema());
  const ArraySchema& array_schema_B = *(fd_B[0]->array_schema());
  const ArraySchema& array_schema_C = *(fd_C->array_schema());
  unsigned int attribute_num_A = array_schema_A.attribute_num();
  unsigned int attribute_num_B = array_schema_B.attribute_num();
  unsigned int attribute_num_C = array_schema_C.attribute_num();
  unsigned int fragment_num_A = fd_A.size();
  unsigned int fragment_num_B = fd_B.size();
  uint64_t capacity = array_schema_C.capacity();

  // Create result tiles 
  Tile** tiles_C = new Tile*[attribute_num_C+1];

  // Create and initialize tile iterators
  StorageManager::const_iterator** tile_its_A = 
      new StorageManager::const_iterator*[fragment_num_A];
  StorageManager::const_iterator* tile_it_end_A =
      new StorageManager::const_iterator[fragment_num_A];
  for(unsigned int i=0; i<fragment_num_A; ++i) {
    tile_its_A[i] = new StorageManager::const_iterator[attribute_num_A+1];
    initialize_tile_its(fd_A[i], tile_its_A[i], tile_it_end_A[i]);
  }
  StorageManager::const_iterator** tile_its_B = 
      new StorageManager::const_iterator*[fragment_num_B];
  StorageManager::const_iterator* tile_it_end_B =
      new StorageManager::const_iterator[fragment_num_B];
  for(unsigned int i=0; i<fragment_num_B; ++i) {
    tile_its_B[i] = new StorageManager::const_iterator[attribute_num_B+1];
    initialize_tile_its(fd_B[i], tile_its_B[i], tile_it_end_B[i]);
  }
  
  // Create cell iterators
  Tile::const_iterator** cell_its_A = 
      new Tile::const_iterator*[fragment_num_A];
  Tile::const_iterator* cell_it_end_A = 
      new Tile::const_iterator[fragment_num_A];
  for(unsigned int i=0; i<fragment_num_A; ++i)
    cell_its_A[i] = new Tile::const_iterator[attribute_num_A+1];
  Tile::const_iterator** cell_its_B = 
      new Tile::const_iterator*[fragment_num_B];
  Tile::const_iterator* cell_it_end_B = 
      new Tile::const_iterator[fragment_num_B];
  for(unsigned int i=0; i<fragment_num_B; ++i)
    cell_its_B[i] = new Tile::const_iterator[attribute_num_B+1];

  // Auxiliary variables for synchronizing tiles and cells.
  uint64_t* skipped_tiles_A = new uint64_t[fragment_num_A];
  uint64_t* skipped_cells_A = new uint64_t[fragment_num_A];
  for(unsigned int i=0; i<fragment_num_A; ++i) {
    skipped_tiles_A[i] = 0;
    skipped_cells_A[i] = 0;
  }
  uint64_t* skipped_tiles_B = new uint64_t[fragment_num_B];
  uint64_t* skipped_cells_B = new uint64_t[fragment_num_B];
  for(unsigned int i=0; i<fragment_num_B; ++i) {
    skipped_tiles_B[i] = 0;
    skipped_cells_B[i] = 0;
  }
  
  // Note that attribute cell iterators may be initialized and advanced only
  // after a join result is discovered.
  bool* attribute_cell_its_initialized_A = new bool[fragment_num_A];
  bool* coordinate_cell_its_initialized_A = new bool[fragment_num_A];
  for(unsigned int i=0; i<fragment_num_A; ++i) {
    attribute_cell_its_initialized_A[i] = false;
    coordinate_cell_its_initialized_A[i] = false;
  }
  bool* attribute_cell_its_initialized_B = new bool[fragment_num_B];
  bool* coordinate_cell_its_initialized_B = new bool[fragment_num_B];
  for(unsigned int i=0; i<fragment_num_B; ++i) {
    attribute_cell_its_initialized_B[i] = false;
    coordinate_cell_its_initialized_B[i] = false;
  }

  // Initialize tiles with id 0 for C (result array)
  new_tiles(array_schema_C, 0, tiles_C); 

  // Get the indexes of the fragments in A and B respectively, whose current
  // coordinate cell iterators point to cells that join (and produce a result).
  int next_fragment_index_A;
  int next_fragment_index_B;
  bool fragment_indexes_initialized = false;
  get_next_join_fragment_indexes_irregular(
      fd_A, tile_its_A, tile_it_end_A, cell_its_A, cell_it_end_A, 
      skipped_tiles_A, skipped_cells_A,
      attribute_cell_its_initialized_A, coordinate_cell_its_initialized_A,
      fd_B, tile_its_B, tile_it_end_B, cell_its_B, cell_it_end_B, 
      skipped_tiles_B, skipped_cells_B,
      attribute_cell_its_initialized_B, coordinate_cell_its_initialized_B,
      fragment_indexes_initialized,
      next_fragment_index_A, next_fragment_index_B);

  // Iterate over all cells, until there are no more cells in some array
  while(next_fragment_index_A != -1 && next_fragment_index_B != -1) {
    // For easy reference
    Tile::const_iterator* next_cell_its_A = 
        cell_its_A[next_fragment_index_A];
    Tile::const_iterator& next_cell_it_end_A = 
        cell_it_end_A[next_fragment_index_A];
    StorageManager::const_iterator* next_tile_its_A = 
        tile_its_A[next_fragment_index_A];
    StorageManager::const_iterator& next_tile_it_end_A = 
        tile_it_end_A[next_fragment_index_A];
    uint64_t& next_skipped_cells_A = 
        skipped_cells_A[next_fragment_index_A];
    uint64_t& next_skipped_tiles_A = 
        skipped_tiles_A[next_fragment_index_A];
    bool& next_attribute_cell_its_initialized_A = 
        attribute_cell_its_initialized_A[next_fragment_index_A];
    bool& next_coordinate_cell_its_initialized_A = 
        coordinate_cell_its_initialized_A[next_fragment_index_A];
    Tile::const_iterator* next_cell_its_B = 
        cell_its_B[next_fragment_index_B];
    Tile::const_iterator& next_cell_it_end_B = 
        cell_it_end_B[next_fragment_index_B];
    StorageManager::const_iterator* next_tile_its_B = 
        tile_its_B[next_fragment_index_B];
    StorageManager::const_iterator& next_tile_it_end_B = 
        tile_it_end_B[next_fragment_index_B];
    uint64_t& next_skipped_cells_B =
        skipped_cells_B[next_fragment_index_B];
    uint64_t& next_skipped_tiles_B = 
        skipped_tiles_B[next_fragment_index_B];
    bool& next_attribute_cell_its_initialized_B = 
        attribute_cell_its_initialized_B[next_fragment_index_B];
    bool& next_coordinate_cell_its_initialized_B = 
        coordinate_cell_its_initialized_B[next_fragment_index_B];

    // Update iterators in A
    if(next_skipped_tiles_A) {
      advance_tile_its(attribute_num_A, next_tile_its_A, next_skipped_tiles_A);
      next_skipped_tiles_A = 0;  
    }
    if(!next_attribute_cell_its_initialized_A) {
      initialize_cell_its(next_tile_its_A, attribute_num_A, next_cell_its_A);
      next_attribute_cell_its_initialized_A = true;
    }
    if(next_skipped_cells_A) {
      advance_cell_its(attribute_num_A, next_cell_its_A, next_skipped_cells_A);
      next_skipped_cells_A = 0;
    }

    // Update iterators in B
    if(next_skipped_tiles_B) {
      advance_tile_its(attribute_num_B, next_tile_its_B, next_skipped_tiles_B);
      next_skipped_tiles_B = 0;  
    }
    if(!next_attribute_cell_its_initialized_B) {
      initialize_cell_its(next_tile_its_B, attribute_num_B, next_cell_its_B);
      next_attribute_cell_its_initialized_B = true;
    }
    if(next_skipped_cells_B) {
      advance_cell_its(attribute_num_B, next_cell_its_B, next_skipped_cells_B);
      next_skipped_cells_B = 0;
    }

    // Append result cell
    if(tiles_C[attribute_num_C]->cell_num() == capacity) {
      uint64_t new_tile_id = tiles_C[attribute_num_C]->tile_id() + 1;
      store_tiles(fd_C, tiles_C);
      new_tiles(array_schema_C, new_tile_id, tiles_C); 
    }
    append_cell(next_cell_its_A, next_cell_its_B, tiles_C, 
                attribute_num_A, attribute_num_B);

    // Get the indexes of the fragments in A and B respectively, whose current
    // coordinate cell iterators point to cells that join (and produce a result)
    get_next_join_fragment_indexes_irregular(
        fd_A, tile_its_A, tile_it_end_A, cell_its_A, cell_it_end_A, 
        skipped_tiles_A, skipped_cells_A,
        attribute_cell_its_initialized_A, coordinate_cell_its_initialized_A,
        fd_B, tile_its_B, tile_it_end_B, cell_its_B, cell_it_end_B, 
        skipped_tiles_B, skipped_cells_B,
        attribute_cell_its_initialized_B, coordinate_cell_its_initialized_B,
        fragment_indexes_initialized,
        next_fragment_index_A, next_fragment_index_B);
  }

  // Send the lastly created tiles to storage manager
  store_tiles(fd_C, tiles_C);

  // Clean up
  for(unsigned int i=0; i<fragment_num_A; ++i) {
    delete [] tile_its_A[i];
    delete [] cell_its_A[i];
  }
  delete [] tile_its_A;
  delete [] tile_it_end_A;
  delete [] cell_its_A;
  delete [] cell_it_end_A;

  for(unsigned int i=0; i<fragment_num_B; ++i) {
    delete [] tile_its_B[i];
    delete [] cell_its_B[i];
  }
  delete [] tile_its_B;
  delete [] tile_it_end_B;
  delete [] cell_its_B;
  delete [] cell_it_end_B;

  delete [] tiles_C;

  delete [] skipped_tiles_A;
  delete [] skipped_tiles_B;
  delete [] skipped_cells_A;
  delete [] skipped_cells_B;

  delete [] attribute_cell_its_initialized_A;
  delete [] coordinate_cell_its_initialized_A;
  delete [] attribute_cell_its_initialized_B;
  delete [] coordinate_cell_its_initialized_B;
}

void QueryProcessor::join_regular(
    const StorageManager::FragmentDescriptor* fd_A, 
    const StorageManager::FragmentDescriptor* fd_B,
    const StorageManager::FragmentDescriptor* fd_C) const {
  // For easy reference
  const ArraySchema& array_schema_A = *(fd_A->array_schema());
  const ArraySchema& array_schema_B = *(fd_B->array_schema());
  const ArraySchema& array_schema_C = *(fd_C->array_schema());
  unsigned int attribute_num_A = array_schema_A.attribute_num();
  unsigned int attribute_num_B = array_schema_B.attribute_num();
  unsigned int attribute_num_C = array_schema_C.attribute_num();
  uint64_t tile_id_A, tile_id_B;

  // Create tiles 
  const Tile** tiles_A = new const Tile*[attribute_num_A+1];
  const Tile** tiles_B = new const Tile*[attribute_num_B+1];
  Tile** tiles_C = new Tile*[attribute_num_C+1];
  
  // Create and initialize tile iterators
  StorageManager::const_iterator *tile_its_A = 
      new StorageManager::const_iterator[attribute_num_A+1];
  StorageManager::const_iterator *tile_its_B = 
      new StorageManager::const_iterator[attribute_num_B+1];
  StorageManager::const_iterator tile_it_end_A;
  StorageManager::const_iterator tile_it_end_B;
  initialize_tile_its(fd_A, tile_its_A, tile_it_end_A);
  initialize_tile_its(fd_B, tile_its_B, tile_it_end_B);
  
  // Create cell iterators
  Tile::const_iterator* cell_its_A = 
      new Tile::const_iterator[attribute_num_A+1];
  Tile::const_iterator* cell_its_B = 
      new Tile::const_iterator[attribute_num_B+1];
  Tile::const_iterator cell_it_end_A, cell_it_end_B;

  // Auxiliary variables storing the number of skipped tiles when joining.
  // It is used to advance only the coordinates iterator when a tile is
  // finished/skipped, and then efficiently advance the attribute iterators only
  // when a tile joins.
  int64_t skipped_tiles_A = 0;
  int64_t skipped_tiles_B = 0;

  // Join algorithm
  while(tile_its_A[attribute_num_A] != tile_it_end_A &&
        tile_its_B[attribute_num_B] != tile_it_end_B) {
    tile_id_A = tile_its_A[attribute_num_A].tile_id();
    tile_id_B = tile_its_B[attribute_num_B].tile_id();

    // Potential join result generation
    if(tile_id_A == tile_id_B) {
      // Update tile iterators in A
      if(skipped_tiles_A) {
        advance_tile_its(attribute_num_A, tile_its_A, skipped_tiles_A);
        skipped_tiles_A = 0;
      }
      // Initialize cell iterators for A
      cell_its_A[attribute_num_A] = (*tile_its_A[attribute_num_A]).begin();
      cell_it_end_A = (*tile_its_A[attribute_num_A]).end();
      // Update tile iterators in B
      if(skipped_tiles_B) {
        advance_tile_its(attribute_num_B, tile_its_B, skipped_tiles_B);
        skipped_tiles_B = 0;
      }
      // Initialize cell iterators for B
      cell_its_B[attribute_num_B] = (*tile_its_B[attribute_num_B]).begin();
      cell_it_end_B = (*tile_its_B[attribute_num_B]).end();
 
      // Initialize tiles for C (result array)
      new_tiles(array_schema_C, tile_id_A, tiles_C);

      // Join the tiles
      join_tiles_regular(attribute_num_A, tile_its_A, cell_its_A, cell_it_end_A,
                         attribute_num_B, tile_its_B, cell_its_B, cell_it_end_B,
                         fd_C, tiles_C);

      // Send the created tiles to storage manager
      store_tiles(fd_C, tiles_C);

      // Advance both tile iterators
      ++tile_its_A[attribute_num_A];
      ++skipped_tiles_A;
      ++tile_its_B[attribute_num_B];
      ++skipped_tiles_B;
    // Tile precedence in the case of regular tiles is simply determined
    // by the order of the tile ids.
    } else if(tile_id_A < tile_id_B) {
      ++tile_its_A[attribute_num_A];
      ++skipped_tiles_A;
    } else { // tile_id_A > tile_id_B
      ++tile_its_B[attribute_num_B];
      ++skipped_tiles_B;
    }
  } 
  
  // Clean up
  delete [] tiles_A;
  delete [] tiles_B;
  delete [] tiles_C;
  delete [] tile_its_A;
  delete [] tile_its_B;
  delete [] cell_its_A;
  delete [] cell_its_B;
}

void QueryProcessor::join_regular(
    const std::vector<const StorageManager::FragmentDescriptor*>& fd_A,
    const std::vector<const StorageManager::FragmentDescriptor*>& fd_B,
    const StorageManager::FragmentDescriptor* fd_C) const {
  // For easy reference
  const ArraySchema& array_schema_A = *(fd_A[0]->array_schema());
  const ArraySchema& array_schema_B = *(fd_B[0]->array_schema());
  const ArraySchema& array_schema_C = *(fd_C->array_schema());
  unsigned int attribute_num_A = array_schema_A.attribute_num();
  unsigned int attribute_num_B = array_schema_B.attribute_num();
  unsigned int attribute_num_C = array_schema_C.attribute_num();
  unsigned int fragment_num_A = fd_A.size();
  unsigned int fragment_num_B = fd_B.size();
  uint64_t capacity = array_schema_C.capacity();

  // Create result tiles 
  Tile** tiles_C = new Tile*[attribute_num_C+1];

  // Create and initialize tile iterators
  StorageManager::const_iterator** tile_its_A = 
      new StorageManager::const_iterator*[fragment_num_A];
  StorageManager::const_iterator* tile_it_end_A =
      new StorageManager::const_iterator[fragment_num_A];
  for(unsigned int i=0; i<fragment_num_A; ++i) {
    tile_its_A[i] = new StorageManager::const_iterator[attribute_num_A+1];
    initialize_tile_its(fd_A[i], tile_its_A[i], tile_it_end_A[i]);
  }
  StorageManager::const_iterator** tile_its_B = 
      new StorageManager::const_iterator*[fragment_num_B];
  StorageManager::const_iterator* tile_it_end_B =
      new StorageManager::const_iterator[fragment_num_B];
  for(unsigned int i=0; i<fragment_num_B; ++i) {
    tile_its_B[i] = new StorageManager::const_iterator[attribute_num_B+1];
    initialize_tile_its(fd_B[i], tile_its_B[i], tile_it_end_B[i]);
  }
  
  // Create cell iterators
  Tile::const_iterator** cell_its_A = 
      new Tile::const_iterator*[fragment_num_A];
  Tile::const_iterator* cell_it_end_A = 
      new Tile::const_iterator[fragment_num_A];
  for(unsigned int i=0; i<fragment_num_A; ++i)
    cell_its_A[i] = new Tile::const_iterator[attribute_num_A+1];
  Tile::const_iterator** cell_its_B = 
      new Tile::const_iterator*[fragment_num_B];
  Tile::const_iterator* cell_it_end_B = 
      new Tile::const_iterator[fragment_num_B];
  for(unsigned int i=0; i<fragment_num_B; ++i)
    cell_its_B[i] = new Tile::const_iterator[attribute_num_B+1];

  // Auxiliary variables for synchronizing tiles and cells.
  uint64_t* skipped_tiles_A = new uint64_t[fragment_num_A];
  uint64_t* skipped_cells_A = new uint64_t[fragment_num_A];
  for(unsigned int i=0; i<fragment_num_A; ++i) {
    skipped_tiles_A[i] = 0;
    skipped_cells_A[i] = 0;
  }
  uint64_t* skipped_tiles_B = new uint64_t[fragment_num_B];
  uint64_t* skipped_cells_B = new uint64_t[fragment_num_B];
  for(unsigned int i=0; i<fragment_num_B; ++i) {
    skipped_tiles_B[i] = 0;
    skipped_cells_B[i] = 0;
  }
  
  // Note that attribute cell iterators may be initialized and advanced only
  // after a join result is discovered.
  bool* attribute_cell_its_initialized_A = new bool[fragment_num_A];
  bool* coordinate_cell_its_initialized_A = new bool[fragment_num_A];
  for(unsigned int i=0; i<fragment_num_A; ++i) {
    attribute_cell_its_initialized_A[i] = false;
    coordinate_cell_its_initialized_A[i] = false;
  }
  bool* attribute_cell_its_initialized_B = new bool[fragment_num_B];
  bool* coordinate_cell_its_initialized_B = new bool[fragment_num_B];
  for(unsigned int i=0; i<fragment_num_B; ++i) {
    attribute_cell_its_initialized_B[i] = false;
    coordinate_cell_its_initialized_B[i] = false;
  }

  // Get the indexes of the fragments in A and B respectively, whose current
  // coordinate cell iterators point to cells that join (and produce a result).
  int next_fragment_index_A;
  int next_fragment_index_B;
  bool fragment_indexes_initialized = false;
  get_next_join_fragment_indexes_regular(
      fd_A, tile_its_A, tile_it_end_A, cell_its_A, cell_it_end_A, 
      skipped_tiles_A, skipped_cells_A,
      attribute_cell_its_initialized_A, coordinate_cell_its_initialized_A,
      fd_B, tile_its_B, tile_it_end_B, cell_its_B, cell_it_end_B, 
      skipped_tiles_B, skipped_cells_B,
      attribute_cell_its_initialized_B, coordinate_cell_its_initialized_B,
      fragment_indexes_initialized,
      next_fragment_index_A, next_fragment_index_B);

  uint64_t tile_id;
  bool first_cell = true;

  // Iterate over all cells, until there are no more cells in some array
  while(next_fragment_index_A != -1 && next_fragment_index_B != -1) {
    // For easy reference
    Tile::const_iterator* next_cell_its_A = 
        cell_its_A[next_fragment_index_A];
    Tile::const_iterator& next_cell_it_end_A = 
        cell_it_end_A[next_fragment_index_A];
    StorageManager::const_iterator* next_tile_its_A = 
        tile_its_A[next_fragment_index_A];
    StorageManager::const_iterator& next_tile_it_end_A = 
        tile_it_end_A[next_fragment_index_A];
    uint64_t& next_skipped_cells_A = 
        skipped_cells_A[next_fragment_index_A];
    uint64_t& next_skipped_tiles_A = 
        skipped_tiles_A[next_fragment_index_A];
    bool& next_attribute_cell_its_initialized_A = 
        attribute_cell_its_initialized_A[next_fragment_index_A];
    bool& next_coordinate_cell_its_initialized_A = 
        coordinate_cell_its_initialized_A[next_fragment_index_A];
    Tile::const_iterator* next_cell_its_B = 
        cell_its_B[next_fragment_index_B];
    Tile::const_iterator& next_cell_it_end_B = 
        cell_it_end_B[next_fragment_index_B];
    StorageManager::const_iterator* next_tile_its_B = 
        tile_its_B[next_fragment_index_B];
    StorageManager::const_iterator& next_tile_it_end_B = 
        tile_it_end_B[next_fragment_index_B];
    uint64_t& next_skipped_cells_B =
        skipped_cells_B[next_fragment_index_B];
    uint64_t& next_skipped_tiles_B = 
        skipped_tiles_B[next_fragment_index_B];
    bool& next_attribute_cell_its_initialized_B = 
        attribute_cell_its_initialized_B[next_fragment_index_B];
    bool& next_coordinate_cell_its_initialized_B = 
        coordinate_cell_its_initialized_B[next_fragment_index_B];

    // Update iterators in A
    if(next_skipped_tiles_A) {
      advance_tile_its(attribute_num_A, next_tile_its_A, next_skipped_tiles_A);
      next_skipped_tiles_A = 0;  
    }
    if(!next_attribute_cell_its_initialized_A) {
      initialize_cell_its(next_tile_its_A, attribute_num_A, next_cell_its_A);
      next_attribute_cell_its_initialized_A = true;
    }
    if(next_skipped_cells_A) {
      advance_cell_its(attribute_num_A, next_cell_its_A, next_skipped_cells_A);
      next_skipped_cells_A = 0;
    }

    // Update iterators in B
    if(next_skipped_tiles_B) {
      advance_tile_its(attribute_num_B, next_tile_its_B, next_skipped_tiles_B);
      next_skipped_tiles_B = 0;  
    }
    if(!next_attribute_cell_its_initialized_B) {
      initialize_cell_its(next_tile_its_B, attribute_num_B, next_cell_its_B);
      next_attribute_cell_its_initialized_B = true;
    }
    if(next_skipped_cells_B) {
      advance_cell_its(attribute_num_B, next_cell_its_B, next_skipped_cells_B);
      next_skipped_cells_B = 0;
    }

    // Append result cell
    if(first_cell) { // To initialize tile_id and result_tiles
      tile_id = next_cell_its_A[0].tile()->tile_id();
      first_cell = false;
      new_tiles(array_schema_C, tile_id, tiles_C);
    }
    if(next_cell_its_A[0].tile()->tile_id() != tile_id) {
      // Change tile
      tile_id = next_cell_its_A[0].tile()->tile_id();
      store_tiles(fd_C, tiles_C);
      new_tiles(array_schema_C, tile_id, tiles_C);
    } 
    append_cell(next_cell_its_A, next_cell_its_B, tiles_C, 
                attribute_num_A, attribute_num_B);

    // Get the indexes of the fragments in A and B respectively, whose current
    // coordinate cell iterators point to cells that join (and produce a result)
    get_next_join_fragment_indexes_regular(
        fd_A, tile_its_A, tile_it_end_A, cell_its_A, cell_it_end_A, 
        skipped_tiles_A, skipped_cells_A,
        attribute_cell_its_initialized_A, coordinate_cell_its_initialized_A,
        fd_B, tile_its_B, tile_it_end_B, cell_its_B, cell_it_end_B, 
        skipped_tiles_B, skipped_cells_B,
        attribute_cell_its_initialized_B, coordinate_cell_its_initialized_B,
        fragment_indexes_initialized,
        next_fragment_index_A, next_fragment_index_B);
  }

  // Send the lastly created tiles to storage manager
  store_tiles(fd_C, tiles_C);

  // Clean up
  for(unsigned int i=0; i<fragment_num_A; ++i) {
    delete [] tile_its_A[i];
    delete [] cell_its_A[i];
  }
  delete [] tile_its_A;
  delete [] tile_it_end_A;
  delete [] cell_its_A;
  delete [] cell_it_end_A;

  for(unsigned int i=0; i<fragment_num_B; ++i) {
    delete [] tile_its_B[i];
    delete [] cell_its_B[i];
  }
  delete [] tile_its_B;
  delete [] tile_it_end_B;
  delete [] cell_its_B;
  delete [] cell_it_end_B;

  delete [] tiles_C;

  delete [] skipped_tiles_A;
  delete [] skipped_tiles_B;
  delete [] skipped_cells_A;
  delete [] skipped_cells_B;

  delete [] attribute_cell_its_initialized_A;
  delete [] coordinate_cell_its_initialized_A;
  delete [] attribute_cell_its_initialized_B;
  delete [] coordinate_cell_its_initialized_B;

}

void QueryProcessor::join_tiles_irregular(
    unsigned int attribute_num_A, 
    const StorageManager::const_iterator* tile_its_A, 
    Tile::const_iterator* cell_its_A,
    Tile::const_iterator& cell_it_end_A, 
    unsigned int attribute_num_B, 
    const StorageManager::const_iterator* tile_its_B, 
    Tile::const_iterator* cell_its_B,
    Tile::const_iterator& cell_it_end_B,
    const StorageManager::FragmentDescriptor* fd_C, Tile** tiles_C,
    bool& attribute_cell_its_initialized_A,
    bool& attribute_cell_its_initialized_B) const {
  // For easy reference
  const ArraySchema& array_schema_C = *(fd_C->array_schema());
  uint64_t capacity = array_schema_C.capacity();
  unsigned int attribute_num_C = array_schema_C.attribute_num();

  // Auxiliary variables storing the number of skipped cells when joining.
  // It is used to advance only the coordinates iterator when a cell is
  // finished/skipped, and then efficiently advance the attribute iterators only
  // when a cell joins.
  int64_t skipped_cells_A;
  int64_t skipped_cells_B;

  if(!attribute_cell_its_initialized_A) 
    skipped_cells_A = cell_its_A[attribute_num_A].pos();
  else
    skipped_cells_A = cell_its_A[attribute_num_A].pos() - cell_its_A[0].pos();

  if(!attribute_cell_its_initialized_B) 
    skipped_cells_B = cell_its_B[attribute_num_B].pos();
  else
    skipped_cells_B = cell_its_B[attribute_num_B].pos() - cell_its_B[0].pos();

  while(cell_its_A[attribute_num_A] != cell_it_end_A &&
        cell_its_B[attribute_num_B] != cell_it_end_B) {
    // If the coordinates are equal
    // Note that operator '==', when the operands correspond to different
    // tiles, returns true if the cell values pointed by the iterators
    // are equal.
    if(cell_its_A[attribute_num_A] == cell_its_B[attribute_num_B]) {      
      if(!attribute_cell_its_initialized_A) {
        initialize_cell_its(tile_its_A, attribute_num_A, cell_its_A);
        attribute_cell_its_initialized_A = true;
      } 
      if(!attribute_cell_its_initialized_B) {
        initialize_cell_its(tile_its_B, attribute_num_B, cell_its_B);
        attribute_cell_its_initialized_B = true;
      }
      if(skipped_cells_A) {
        advance_cell_its(attribute_num_A, cell_its_A, skipped_cells_A);
        skipped_cells_A = 0;
      }
      if(skipped_cells_B) {
        advance_cell_its(attribute_num_B, cell_its_B, skipped_cells_B);
        skipped_cells_B = 0;
      }
      if(tiles_C[attribute_num_C]->cell_num() == capacity) {
        uint64_t new_tile_id = tiles_C[attribute_num_C]->tile_id() + 1;
        store_tiles(fd_C, tiles_C);
        new_tiles(array_schema_C, new_tile_id, tiles_C); 
      }
      append_cell(cell_its_A, cell_its_B, tiles_C, 
                  attribute_num_A, attribute_num_B);
      advance_cell_its(attribute_num_A, cell_its_A);
      advance_cell_its(attribute_num_B, cell_its_B);
    // Otherwise check which cell iterator to advance
    } else {
      if(array_schema_C.precedes(cell_its_A[attribute_num_A],
                                 cell_its_B[attribute_num_B])) {
        ++cell_its_A[attribute_num_A];
        ++skipped_cells_A;
      } else {
        ++cell_its_B[attribute_num_B];
        ++skipped_cells_B;
      }
    }
  }
}

void QueryProcessor::join_tiles_regular(
    unsigned int attribute_num_A, 
    const StorageManager::const_iterator* tile_its_A, 
    Tile::const_iterator* cell_its_A,
    Tile::const_iterator& cell_it_end_A, 
    unsigned int attribute_num_B, 
    const StorageManager::const_iterator* tile_its_B, 
    Tile::const_iterator* cell_its_B,
    Tile::const_iterator& cell_it_end_B,
    const StorageManager::FragmentDescriptor* fd_C, Tile** tiles_C) const {
  // For easy reference
  const ArraySchema& array_schema_C = *(fd_C->array_schema());
  unsigned int attribute_num_C = array_schema_C.attribute_num();

  // Auxiliary variables storing the number of skipped cells when joining.
  // It is used to advance only the coordinates iterator when a cell is
  // finished/skipped, and then efficiently advance the attribute iterators only
  // when a cell joins.
  int64_t skipped_cells_A = 0;
  int64_t skipped_cells_B = 0;
  
  // Note that attribute cell iterators are initialized and advanced only
  // after the first join result is discovered. 
  bool attribute_cell_its_initialized_A = false;
  bool attribute_cell_its_initialized_B = false;

  while(cell_its_A[attribute_num_A] != cell_it_end_A &&
        cell_its_B[attribute_num_B] != cell_it_end_B) {
    // If the coordinates are equal
    // Note that operator '==', when the operands correspond to different
    // tiles, returns true if the cell values pointed by the iterators
    // are equal.
    if(cell_its_A[attribute_num_A] == cell_its_B[attribute_num_B]) {
      if(!attribute_cell_its_initialized_A) {
        initialize_cell_its(tile_its_A, attribute_num_A, cell_its_A);
        attribute_cell_its_initialized_A = true;
      } 
      if(!attribute_cell_its_initialized_B) {
        initialize_cell_its(tile_its_B, attribute_num_B, cell_its_B);
        attribute_cell_its_initialized_B = true;
      }
      if(skipped_cells_A) { 
        advance_cell_its(attribute_num_A, cell_its_A, skipped_cells_A);
        skipped_cells_A = 0;
      }
      if(skipped_cells_B) { 
        advance_cell_its(attribute_num_B, cell_its_B, skipped_cells_B);
        skipped_cells_B = 0;
      }
      append_cell(cell_its_A, cell_its_B, tiles_C, 
                  attribute_num_A, attribute_num_B);
      advance_cell_its(attribute_num_A, cell_its_A);
      advance_cell_its(attribute_num_B, cell_its_B);
    // Otherwise check which cell iterator to advance
    } else {
      if(array_schema_C.precedes(cell_its_A[attribute_num_A],
                                 cell_its_B[attribute_num_B])) {
        ++cell_its_A[attribute_num_A];
        ++skipped_cells_A;
      } else {
        ++cell_its_B[attribute_num_B];
        ++skipped_cells_B;
      }
    }
  }
}

bool QueryProcessor::may_join(
    const StorageManager::const_iterator& it_A,
    const StorageManager::const_iterator& it_B) const {
  // For easy reference
  const ArraySchema& array_schema_A = it_A.array_schema();
  const MBR& mbr_A = it_A.mbr();
  const MBR& mbr_B = it_B.mbr();

  // Check if the tile MBRs overlap
  if(array_schema_A.has_irregular_tiles() && !overlap(mbr_A, mbr_B))
    return false;

  // For easy reference 
  BoundingCoordinatesPair bounding_coordinates_A = it_A.bounding_coordinates();
  BoundingCoordinatesPair bounding_coordinates_B = it_B.bounding_coordinates();

  // Check if the cell id ranges (along the global order) intersect
  if(!overlap(bounding_coordinates_A, bounding_coordinates_B, array_schema_A))
    return false;

  return true;
}

// NOTE: It is assumed that k is small enough for O(k) coordinates to fit
// in main memory. 
void QueryProcessor::nearest_neighbors_irregular(
    const StorageManager::FragmentDescriptor* fd,
    const std::vector<double>& q,
    uint64_t k,
    const StorageManager::FragmentDescriptor* result_fd) const {
  // For easy reference
  const ArraySchema& array_schema = *(fd->array_schema());
  unsigned int attribute_num = array_schema.attribute_num();
  uint64_t capacity = array_schema.capacity();

  // Create tiles 
  const Tile** tiles = new const Tile*[attribute_num];
  Tile** result_tiles = new Tile*[attribute_num+1];

  // Create cell iterators
  Tile::const_iterator* cell_its = 
      new Tile::const_iterator[attribute_num];
  Tile::const_iterator cell_it_end;

  // Get pairs (dist, rank), sorted on dist
  // rank is a tile rank and dist is its distance to q
  std::vector<DistRank> sorted_dist_ranks = compute_sorted_dist_ranks(fd, q);

  // Compute sorted kNN coordinates of the form (rank, (pos, coord))
  // coord is the cell coordinates, rank is the rank of the tile the
  // cell belongs to, and pos is the position of the cell in the tile
  std::vector<RankPosCoord> sorted_kNN_coords = 
      compute_sorted_kNN_coords(fd, q, k, sorted_dist_ranks); 
    
  // Prepare new result tiles
  uint64_t tile_id = 0; 
  new_tiles(array_schema, tile_id, result_tiles); 

  // Retrieve and store the actual k nearest neighbors
  int64_t current_rank = -1;
  uint64_t pos, rank;
  for(uint64_t i=0; i<sorted_kNN_coords.size(); ++i) {
    // For easy reference
    rank = sorted_kNN_coords[i].first; 
    pos = sorted_kNN_coords[i].second.first; 
    const std::vector<double>& coord = sorted_kNN_coords[i].second.second;

    // Retrieve tiles
    if(rank != current_rank) {
      current_rank = rank;
      for(unsigned int i=0; i<attribute_num; i++) 
        tiles[i] = storage_manager_.get_tile_by_rank(fd, i, rank);
    }
   
    // Store result tile if full
    if(result_tiles[attribute_num]->cell_num() == capacity) {
      store_tiles(result_fd, result_tiles);
      new_tiles(array_schema, ++tile_id, result_tiles); 
    } 
 
    // Append cell
    *result_tiles[attribute_num] << coord;
    for(unsigned int i=0; i<attribute_num; i++) {
      cell_its[i] = tiles[i]->begin() + pos;
      *result_tiles[i] << cell_its[i];
    }
  }
  
  // Send the lastly created tiles to storage manager
  store_tiles(result_fd, result_tiles);

  // Clean up 
  delete [] tiles;
  delete [] result_tiles;
  delete [] cell_its;
} 

// NOTE: It is assumed that k is small enough for O(k) coordinates to fit
// in main memory. 
void QueryProcessor::nearest_neighbors_regular(
    const StorageManager::FragmentDescriptor* fd,
    const std::vector<double>& q,
    uint64_t k,
    const StorageManager::FragmentDescriptor* result_fd) const {
  // For easy reference
  const ArraySchema& array_schema = *(fd->array_schema());
  unsigned int attribute_num = array_schema.attribute_num();

  // Create tiles 
  const Tile** tiles = new const Tile*[attribute_num];
  Tile** result_tiles = new Tile*[attribute_num+1];

  // Create cell iterators
  Tile::const_iterator* cell_its = 
      new Tile::const_iterator[attribute_num];
  Tile::const_iterator cell_it_end;

  // Get pairs (dist, rank), sorted on dist
  // rank is a tile rank and dist is its distance to q
  std::vector<DistRank> sorted_dist_ranks = compute_sorted_dist_ranks(fd, q);

  // Compute sorted kNN coordinates of the form (rank, (pos, coord))
  // coord is the cell coordinates, rank is the rank of the tile the
  // cell belongs to, and pos is the position of the cell in the tile
  std::vector<RankPosCoord> sorted_kNN_coords = 
      compute_sorted_kNN_coords(fd, q, k, sorted_dist_ranks); 
    
  // Retrieve and store the actual k nearest neighbors
  int64_t current_rank = -1;
  uint64_t pos, rank, tile_id;
  for(uint64_t i=0; i<sorted_kNN_coords.size(); ++i) {
    // For easy reference
    rank = sorted_kNN_coords[i].first; 
    pos = sorted_kNN_coords[i].second.first; 
    const std::vector<double>& coord = sorted_kNN_coords[i].second.second;

    // Retrieve tiles and create new result tiles
    if(rank != current_rank) {
      // Store previous result tiles
      if(current_rank != -1)
        store_tiles(result_fd, result_tiles);

      current_rank = rank;
      
      // Retrieve tiles
      for(unsigned int i=0; i<attribute_num; i++) 
        tiles[i] = storage_manager_.get_tile_by_rank(fd, i, rank);
  
      // Prepare new result tiles
      new_tiles(array_schema, tiles[0]->tile_id(), result_tiles); 
    }
   
    // Append cell
    *result_tiles[attribute_num] << coord;
    for(unsigned int i=0; i<attribute_num; i++) {
      cell_its[i] = tiles[i]->begin() + pos;
      *result_tiles[i] << cell_its[i];
    }
  }
    
  // Send the lastly created tiles to storage manager
  store_tiles(result_fd, result_tiles);
  
  // Clean up 
  delete [] tiles;
  delete [] result_tiles;
  delete [] cell_its;
} 

inline
void QueryProcessor::new_tiles(const ArraySchema& array_schema, 
                               uint64_t tile_id, Tile** tiles) const {
  // For easy reference
  unsigned int attribute_num = array_schema.attribute_num();
  uint64_t capacity = array_schema.capacity();

  for(unsigned int i=0; i<=attribute_num; i++)
    tiles[i] = storage_manager_.new_tile(array_schema, i, tile_id, capacity);
}

bool QueryProcessor::overlap(const MBR& mbr_A, const MBR& mbr_B) const {
  assert(mbr_A.size() == mbr_B.size());
  assert(mbr_A.size() % 2 == 0);

  // For easy rederence
  unsigned int dim_num = mbr_A.size() / 2;

  for(unsigned int i=0; i<dim_num; i++) 
    if(mbr_A[2*i+1] < mbr_B[2*i] || mbr_A[2*i] > mbr_B[2*i+1])
      return false;

  return true;
}

bool QueryProcessor::overlap(
    const BoundingCoordinatesPair& bounding_coordinates_A,
    const BoundingCoordinatesPair& bounding_coordinates_B, 
    const ArraySchema& array_schema) const {
  if(array_schema.precedes(bounding_coordinates_A.second, 
                           bounding_coordinates_B.first) ||
     array_schema.succeeds(bounding_coordinates_A.first, 
                           bounding_coordinates_B.second))
    return false;
  else
    return true;
}

double QueryProcessor::point_to_mbr_distance(
    const std::vector<double>& q, const std::vector<double>& mbr) const {
  // Check dimensionality
  assert(mbr.size() == 2*q.size());

  unsigned int dim_num = q.size();
  double width, centroid, dq;
  double dist = 0;
  for(unsigned int i=0; i<dim_num; ++i) {
    width = mbr[2*i+1] - mbr[2*i];
    centroid = mbr[2*i] + width/2;
    dq = std::max(abs(q[i]-centroid) - width/2, 0.0);
    dist += dq * dq; 
  }

  return dist;
}

double QueryProcessor::point_to_point_distance(
    const std::vector<double>& q, const std::vector<double>& p) const {
  // Check dimensionality
  assert(q.size() == p.size());

  unsigned int dim_num = q.size();
  double dist = 0, diff;
  for(unsigned int i=0; i<dim_num; ++i) {
    diff = q[i] - p[i];
    dist += diff * diff; 
  }

  return dist;
}

bool QueryProcessor::precedes(
    const StorageManager::const_iterator& tile_it_A,
    const Tile::const_iterator& cell_it_A,
    bool coordinate_cell_its_initialized_A,
    const StorageManager::const_iterator& tile_it_B,
    const Tile::const_iterator& cell_it_B,
    bool coordinate_cell_its_initialized_B,
    const ArraySchema& array_schema) const {
  // Only for regular tiles
  if(array_schema.has_regular_tiles()) {
    if(tile_it_A.tile_id() < tile_it_B.tile_id())
      return true;
    else if(tile_it_A.tile_id() > tile_it_B.tile_id())
      return false;
    // else
    // tile ids are equal - we must check the coordinates
  }

  if(coordinate_cell_its_initialized_A && coordinate_cell_its_initialized_B) {
    return array_schema.precedes(cell_it_A, cell_it_B);
  } else if(!coordinate_cell_its_initialized_A && 
            !coordinate_cell_its_initialized_B) {
    return array_schema.precedes(tile_it_A.bounding_coordinates().first, 
                                 tile_it_B.bounding_coordinates().first);
  } else if(coordinate_cell_its_initialized_A && 
            !coordinate_cell_its_initialized_B) {
    const std::vector<double> coord_A = *cell_it_A;
    return array_schema.precedes(coord_A, 
                                 tile_it_B.bounding_coordinates().first);
  } else if(!coordinate_cell_its_initialized_A && 
            coordinate_cell_its_initialized_B) {
    const std::vector<double> coord_B = *cell_it_B;
    return array_schema.precedes(tile_it_A.bounding_coordinates().first,
                                 coord_B); 
  }
}

inline
void QueryProcessor::store_tiles(const StorageManager::FragmentDescriptor* fd,
                                 Tile** tiles) const {
  // For easy reference
  unsigned int attribute_num = fd->array_schema()->attribute_num();

  // Append attribute tiles
  for(unsigned int i=0; i<=attribute_num; i++)
    storage_manager_.append_tile(tiles[i], fd, i);
} 

void QueryProcessor::subarray_irregular(
    const StorageManager::FragmentDescriptor* fd,
    const Tile::Range& range, 
    const StorageManager::FragmentDescriptor* result_fd) const { 
  // For easy reference
  const ArraySchema& array_schema = *(fd->array_schema());
  unsigned int attribute_num = array_schema.attribute_num();
  uint64_t capacity = array_schema.capacity();

  // Create tiles
  const Tile** tiles = new const Tile*[attribute_num+1];
  Tile** result_tiles = new Tile*[attribute_num+1];

  // Create cell iterators
  Tile::const_iterator* cell_its = 
      new Tile::const_iterator[attribute_num+1];
  Tile::const_iterator cell_it_end;

  // Get the tile ranks that overlap with the range
  RankOverlapVector overlapping_tile_ranks;
  storage_manager_.get_overlapping_tile_ranks(fd, range, 
                                              &overlapping_tile_ranks);

  // Initialize tile iterators
  RankOverlapVector::const_iterator tile_rank_it = 
      overlapping_tile_ranks.begin();
  RankOverlapVector::const_iterator tile_rank_it_end =
      overlapping_tile_ranks.end();
      
  // Create result tiles and load input array tiles 
  uint64_t tile_id = 0;
  new_tiles(array_schema, tile_id, result_tiles); 

  // Auxiliary variable storing the number of skipped cells when investigating
  // a tile partially overlapping the range. It is used to advance only the
  // coordinates iterator when a cell is not in range, and then efficiently
  // advance the attribute iterators only when a cell falls in the range.
  int64_t skipped_cells;

  // Iterate over all tiles
  for(; tile_rank_it != tile_rank_it_end; ++tile_rank_it) {
    get_tiles_by_rank(fd, tile_rank_it->first, tiles);
    skipped_cells = 0;

    if(tile_rank_it->second) { // Full overlap
      initialize_cell_its(tiles, attribute_num, cell_its, cell_it_end); 
      while(cell_its[attribute_num] != cell_it_end) {
        if(result_tiles[attribute_num]->cell_num() == capacity) {
          store_tiles(result_fd, result_tiles);
          new_tiles(array_schema, ++tile_id, result_tiles); 
        }
        append_cell(cell_its, result_tiles, attribute_num);
        advance_cell_its(attribute_num, cell_its);
      }
    } else { // Partial overlap
      cell_its[attribute_num] = tiles[attribute_num]->begin();
      cell_it_end = tiles[attribute_num]->end();
      bool attribute_cell_its_initialized = false;
      while(cell_its[attribute_num] != cell_it_end) {
        if(cell_its[attribute_num].cell_inside_range(range)) {
          if(result_tiles[attribute_num]->cell_num() == capacity) {
            store_tiles(result_fd, result_tiles);
            new_tiles(array_schema, ++tile_id, result_tiles); 
          }
          if(!attribute_cell_its_initialized) { 
            initialize_cell_its(tiles, attribute_num, cell_its);
            attribute_cell_its_initialized = true;
          }
          if(skipped_cells) {
            advance_cell_its(attribute_num, cell_its, skipped_cells);
            skipped_cells = 0;
          }
          append_cell(cell_its, result_tiles, attribute_num);
          advance_cell_its(attribute_num, cell_its);
        } else { // Advance only the coordinates cell iterator
          skipped_cells++;
          ++cell_its[attribute_num];
        }
      }
    }
  } 

  // Send the lastly created tiles to storage manager
  store_tiles(result_fd, result_tiles);
  
  // Clean up 
  delete [] tiles;
  delete [] result_tiles;
  delete [] cell_its;
}

void QueryProcessor::subarray_irregular(
    const StorageManager::FragmentDescriptor* fd,
    const Tile::Range& range, 
    unsigned int attribute_id,
    struct iovec& c_iovec,
    struct iovec& v_iovec) const { 
  // For easy reference
  const ArraySchema& array_schema = *(fd->array_schema());
  unsigned int attribute_num = array_schema.attribute_num();
  uint64_t capacity = array_schema.capacity();

  // Create tiles
  const Tile* tiles[2];
  Tile* result_tiles[2];
  result_tiles[0] = // tile for the attribute value 
      storage_manager_.new_tile(array_schema, attribute_id, 0, capacity);
  result_tiles[1] = // tile for the coordinates
      storage_manager_.new_tile(array_schema, attribute_num, 0, capacity);

  // Create cell iterators
  Tile::const_iterator cell_its[2]; 
  Tile::const_iterator cell_it_end;

  // Get the tile ranks that overlap with the range
  RankOverlapVector overlapping_tile_ranks;
  storage_manager_.get_overlapping_tile_ranks(fd, range, 
                                              &overlapping_tile_ranks);

  // Initialize tile iterators
  RankOverlapVector::const_iterator tile_rank_it = 
      overlapping_tile_ranks.begin();
  RankOverlapVector::const_iterator tile_rank_it_end =
      overlapping_tile_ranks.end();

  // Iterate over all tiles
  for(; tile_rank_it != tile_rank_it_end; ++tile_rank_it) {
    // Initialize tiles
    tiles[0] = storage_manager_.get_tile_by_rank(fd, attribute_id, 
                                                 tile_rank_it->first);
    tiles[1] = storage_manager_.get_tile_by_rank(fd, attribute_num,
                                                 tile_rank_it->first);

    // Initialize cell iterators
    cell_its[0] = tiles[0]->begin();
    cell_its[1] = tiles[1]->begin();
    cell_it_end = tiles[1]->end();

    while(cell_its[1] != cell_it_end) {
      if(cell_its[1].cell_inside_range(range)) {
        // Append cell
        *result_tiles[0] << cell_its[0];
        *result_tiles[1] << cell_its[1];
      }

      // Advance cell iterators          
      ++cell_its[0];
      ++cell_its[1];
    }
  }

  // Create buffers
  size_t v_buf_size = result_tiles[0]->tile_size();
  size_t c_buf_size = result_tiles[1]->tile_size();
  char* v_buf = new char[v_buf_size];
  char* c_buf = new char[c_buf_size];
 
  // Copy payloads
  result_tiles[0]->copy_payload(v_buf); 
  result_tiles[1]->copy_payload(c_buf); 
  
  // Set iovecs
  v_iovec.iov_base = v_buf;
  v_iovec.iov_len = v_buf_size;
  c_iovec.iov_base = c_buf;
  c_iovec.iov_len = c_buf_size;

  // Clean up
  delete result_tiles[0];
  delete result_tiles[1];
}

void QueryProcessor::subarray_irregular(
    const std::vector<const StorageManager::FragmentDescriptor*>& fd,
    const Tile::Range& range, 
    const StorageManager::FragmentDescriptor* result_fd) const { 
  // For easy reference
  const ArraySchema& array_schema = *(fd[0]->array_schema());
  const unsigned int attribute_num = array_schema.attribute_num();
  uint64_t capacity = array_schema.capacity();
  unsigned int fragment_num = fd.size(); 
  
  // Create input tiles
  const Tile*** tiles = new const Tile**[fragment_num];
  for(unsigned int i=0; i<fragment_num; ++i)
    tiles[i] = new const Tile*[attribute_num+1];

  // Create result tiles 
  Tile** result_tiles = new Tile*[attribute_num+1];
  uint64_t tile_id = 0;
  new_tiles(array_schema, tile_id, result_tiles); 

  // Get the tile ranks that overlap with the range
  RankOverlapVector* overlapping_tile_ranks = 
      new RankOverlapVector[fragment_num];
  for(unsigned int i=0; i<fragment_num; ++i) 
    storage_manager_.get_overlapping_tile_ranks(fd[i], range, 
                                                &overlapping_tile_ranks[i]);

  // Initialize tile iterators
  RankOverlapVector::const_iterator* tile_rank_it = 
    new RankOverlapVector::const_iterator[fragment_num];
  RankOverlapVector::const_iterator* tile_rank_it_end =
    new RankOverlapVector::const_iterator[fragment_num];
  for(unsigned int i=0; i<fragment_num; ++i) { 
      tile_rank_it[i] = overlapping_tile_ranks[i].begin();
      tile_rank_it_end[i] = overlapping_tile_ranks[i].end();
  }

  // Get first tiles and initialize cell_iterators for coordinates
  Tile::const_iterator** cell_its = 
      new Tile::const_iterator*[fragment_num];
  Tile::const_iterator* cell_it_end = 
      new Tile::const_iterator[fragment_num];
  for(unsigned int i=0; i<fragment_num; ++i) { 
    get_tiles_by_rank(fd[i], tile_rank_it[i]->first, tiles[i]);
    cell_its[i] = new Tile::const_iterator[attribute_num+1];
    initialize_cell_its(tiles[i], attribute_num, cell_its[i], cell_it_end[i]);
  }

  // Auxiliary variables for proper retrieval of qualifying cells.
  uint64_t* skipped_cells = new uint64_t[fragment_num];
  for(unsigned int i=0; i<fragment_num; ++i) 
    skipped_cells[i] = 0;

  // Get the index of the fragment from which we will get the next cell
  int next_fragment_index =
      get_next_fragment_index(fd, tile_rank_it, tile_rank_it_end, 
                              tiles, fragment_num, 
                              cell_its, cell_it_end, skipped_cells,
                              array_schema);

  // Iterate over all cells, until there are no more cells
  while(next_fragment_index != -1) {
    // For easy reference
    Tile::const_iterator* next_cell_its = cell_its[next_fragment_index];
    Tile::const_iterator& next_cell_it_end = cell_it_end[next_fragment_index];
    RankOverlapVector::const_iterator& next_tile_rank_it = 
        tile_rank_it[next_fragment_index];
    RankOverlapVector::const_iterator& next_tile_rank_it_end = 
        tile_rank_it_end[next_fragment_index];
 
    if(next_tile_rank_it->second ||
       next_cell_its[attribute_num].cell_inside_range(range)) {
      if(skipped_cells[next_fragment_index]) {
        advance_cell_its(attribute_num, next_cell_its,
                         skipped_cells[next_fragment_index]);
        skipped_cells[next_fragment_index] = 0;
      }
      if(result_tiles[attribute_num]->cell_num() == capacity) {
        store_tiles(result_fd, result_tiles);
        new_tiles(array_schema, ++tile_id, result_tiles); 
      }
      append_cell(next_cell_its, result_tiles, attribute_num);
    }

    advance_cell_tile_its(
        fd[next_fragment_index], attribute_num, 
        next_cell_its, next_cell_it_end,
        next_tile_rank_it, next_tile_rank_it_end,
        fragment_num, tiles[next_fragment_index],
        skipped_cells[next_fragment_index]);

    next_fragment_index =
        get_next_fragment_index(fd, tile_rank_it, tile_rank_it_end,
                                tiles, fragment_num,
                                cell_its, cell_it_end, 
                                skipped_cells, array_schema);
  }

  // Send the lastly created tiles to storage manager
  store_tiles(result_fd, result_tiles);

  // Clean up
  delete [] result_tiles;
  for(unsigned int i=0; i<fragment_num; ++i) {
    delete [] cell_its[i];
    delete [] tiles[i];
  }
  delete [] tiles;
  delete [] cell_its;
  delete [] cell_it_end;
  delete [] skipped_cells;
  delete [] overlapping_tile_ranks;
  delete [] tile_rank_it;
  delete [] tile_rank_it_end;
}

void QueryProcessor::subarray_regular(
    const StorageManager::FragmentDescriptor* fd,
    const Tile::Range& range, 
    const StorageManager::FragmentDescriptor* result_fd) const { 
  // For easy reference
  const ArraySchema& array_schema = *(fd->array_schema());
  unsigned int attribute_num = array_schema.attribute_num();

  // Create tiles 
  const Tile** tiles = new const Tile*[attribute_num+1];
  Tile** result_tiles = new Tile*[attribute_num+1];

  // Create cell iterators
  Tile::const_iterator* cell_its = 
      new Tile::const_iterator[attribute_num+1];
  Tile::const_iterator cell_it_end;

  // Get the tile ranks that overlap with the range
  RankOverlapVector overlapping_tile_ranks;
  storage_manager_.get_overlapping_tile_ranks(fd, range, 
                                              &overlapping_tile_ranks);

  // Initialize tile iterators
  RankOverlapVector::const_iterator tile_rank_it = 
      overlapping_tile_ranks.begin();
  RankOverlapVector::const_iterator tile_rank_it_end =
      overlapping_tile_ranks.end();

  // Auxiliary variable storing the number of skipped cells when investigating
  // a tile partially overlapping the range. It is used to advance only the
  // coordinates iterator when a cell is not in range, and then efficiently
  // advance the attribute iterators only when a cell falls in the range.
  int64_t skipped_cells;

  // Iterate over all overlapping tiles
  for(; tile_rank_it != tile_rank_it_end; ++tile_rank_it) {
    // Create result tiles and load input array tiles 
    new_tiles(array_schema, 
              storage_manager_.get_tile_id(fd, tile_rank_it->first), 
              result_tiles); 
    get_tiles_by_rank(fd, tile_rank_it->first, tiles); 
    skipped_cells = 0;
 
    if(tile_rank_it->second) { // Full overlap
      initialize_cell_its(tiles, attribute_num, cell_its, cell_it_end); 
      while(cell_its[attribute_num] != cell_it_end) {
        append_cell(cell_its, result_tiles, attribute_num);
        advance_cell_its(attribute_num, cell_its);
      }
    } else { // Partial overlap
      cell_its[attribute_num] = tiles[attribute_num]->begin();
      cell_it_end = tiles[attribute_num]->end();
      bool attribute_cell_its_initialized = false;
      while(cell_its[attribute_num] != cell_it_end) {
        if(cell_its[attribute_num].cell_inside_range(range)) {
          if(!attribute_cell_its_initialized) { 
            initialize_cell_its(tiles, attribute_num, cell_its);
            attribute_cell_its_initialized = true;
          }
          if(skipped_cells) {
            advance_cell_its(attribute_num, cell_its, skipped_cells);
            skipped_cells = 0;
          }
          append_cell(cell_its, result_tiles, attribute_num);
          advance_cell_its(attribute_num, cell_its);
        } else { // Advance only the coordinates cell iterator
          ++skipped_cells;
          ++cell_its[attribute_num];
        }
      }
    }
      
    // Send new tiles to storage manager
    store_tiles(result_fd, result_tiles);
  } 
  
  // Clean up 
  delete [] tiles;
  delete [] result_tiles;
  delete [] cell_its;
}

void QueryProcessor::subarray_regular(
    const std::vector<const StorageManager::FragmentDescriptor*>& fd,
    const Tile::Range& range, 
    const StorageManager::FragmentDescriptor* result_fd) const { 
  // For easy reference
  const ArraySchema& array_schema = *(fd[0]->array_schema());
  const unsigned int attribute_num = array_schema.attribute_num();
  unsigned int fragment_num = fd.size(); 
  
  // Create input tiles
  const Tile*** tiles = new const Tile**[fragment_num];
  for(unsigned int i=0; i<fragment_num; ++i)
    tiles[i] = new const Tile*[attribute_num+1];

  // Create result tiles 
  Tile** result_tiles = new Tile*[attribute_num+1];

  // Get the tile ranks that overlap with the range
  RankOverlapVector* overlapping_tile_ranks = 
      new RankOverlapVector[fragment_num];
  for(unsigned int i=0; i<fragment_num; ++i) 
    storage_manager_.get_overlapping_tile_ranks(fd[i], range, 
                                                &overlapping_tile_ranks[i]);

  // Initialize tile iterators
  RankOverlapVector::const_iterator* tile_rank_it = 
    new RankOverlapVector::const_iterator[fragment_num];
  RankOverlapVector::const_iterator* tile_rank_it_end =
    new RankOverlapVector::const_iterator[fragment_num];
  for(unsigned int i=0; i<fragment_num; ++i) { 
      tile_rank_it[i] = overlapping_tile_ranks[i].begin();
      tile_rank_it_end[i] = overlapping_tile_ranks[i].end();
  }

  // Get first tiles and initialize cell_iterators for coordinates
  Tile::const_iterator** cell_its = 
      new Tile::const_iterator*[fragment_num];
  Tile::const_iterator* cell_it_end = 
      new Tile::const_iterator[fragment_num];
  for(unsigned int i=0; i<fragment_num; ++i) { 
    get_tiles_by_rank(fd[i], tile_rank_it[i]->first, tiles[i]);
    cell_its[i] = new Tile::const_iterator[attribute_num+1];
    initialize_cell_its(tiles[i], attribute_num, cell_its[i], cell_it_end[i]);
  }

  // Auxiliary variables for proper retrieval of qualifying cells.
  uint64_t* skipped_cells = new uint64_t[fragment_num];
  for(unsigned int i=0; i<fragment_num; ++i) 
    skipped_cells[i] = 0;

  // Get the index of the fragment from which we will get the next cell
  int next_fragment_index =
      get_next_fragment_index(fd, tile_rank_it, tile_rank_it_end, 
                              tiles, fragment_num, 
                              cell_its, cell_it_end, skipped_cells,
                              array_schema);

  uint64_t tile_id;
  bool first_cell = true;

  // Iterate over all cells, until there are no more cells
  while(next_fragment_index != -1) {
    // For easy reference
    Tile::const_iterator* next_cell_its = cell_its[next_fragment_index];
    Tile::const_iterator& next_cell_it_end = cell_it_end[next_fragment_index];
    RankOverlapVector::const_iterator& next_tile_rank_it = 
        tile_rank_it[next_fragment_index];
    RankOverlapVector::const_iterator& next_tile_rank_it_end = 
        tile_rank_it_end[next_fragment_index];
  
    // If the cell satisfies the subarray query
    if(next_tile_rank_it->second ||
       next_cell_its[attribute_num].cell_inside_range(range)) {
      if(skipped_cells[next_fragment_index]) {
        advance_cell_its(attribute_num, next_cell_its,
                         skipped_cells[next_fragment_index]);
        skipped_cells[next_fragment_index] = 0;
      }
      if(first_cell) { // To initialize tile_id and result_tiles
        tile_id = next_cell_its[attribute_num].tile()->tile_id();
        first_cell = false;
        new_tiles(array_schema, tile_id, result_tiles);
      }
      if(next_cell_its[0].tile()->tile_id() != tile_id) {
        // Change tile
        tile_id = next_cell_its[0].tile()->tile_id();
        store_tiles(result_fd, result_tiles);
        new_tiles(array_schema, tile_id, result_tiles);
      } 
      append_cell(next_cell_its, result_tiles, attribute_num);
    }

    advance_cell_tile_its(
        fd[next_fragment_index], attribute_num, 
        next_cell_its, next_cell_it_end,
        next_tile_rank_it, next_tile_rank_it_end,
        fragment_num, tiles[next_fragment_index],
        skipped_cells[next_fragment_index]);

    next_fragment_index =
        get_next_fragment_index(fd, tile_rank_it, tile_rank_it_end,
                                tiles, fragment_num,
                                cell_its, cell_it_end, 
                                skipped_cells, array_schema);
  }

  // Send the lastly created tiles to storage manager
  store_tiles(result_fd, result_tiles);

  // Clean up
  delete [] result_tiles;
  for(unsigned int i=0; i<fragment_num; ++i) {
    delete [] cell_its[i];
    delete [] tiles[i];
  }
  delete [] tiles;
  delete [] cell_its;
  delete [] cell_it_end;
  delete [] skipped_cells;
  delete [] overlapping_tile_ranks;
  delete [] tile_rank_it;
  delete [] tile_rank_it_end;
}

*/


