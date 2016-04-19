/**
 * @file   array_read_state.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
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
 * This file implements the ArrayReadState class.
 */

#include "array_read_state.h"
#include "utils.h"
#include <cassert>
#include <cmath>




/* ****************************** */
/*             MACROS             */
/* ****************************** */

#if VERBOSE == 1
#  define PRINT_ERROR(x) std::cerr << "[TileDB] Error: " << x << ".\n" 
#  define PRINT_WARNING(x) std::cerr << "[TileDB] Warning: " \
                                     << x << ".\n"
#elif VERBOSE == 2
#  define PRINT_ERROR(x) std::cerr << "[TileDB::ArrayReadState] Error: " \
                                   << x << ".\n" 
#  define PRINT_WARNING(x) std::cerr << "[TileDB::ArrayReadState] Warning: " \
                                     << x << ".\n"
#else
#  define PRINT_ERROR(x) do { } while(0) 
#  define PRINT_WARNING(x) do { } while(0) 
#endif




/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

ArrayReadState::ArrayReadState(
    const Array* array)
    : array_(array) {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  int attribute_num = array_schema->attribute_num();

  // Initializations
  done_ = false;
  empty_cells_written_.resize(attribute_num+1);
  fragment_cell_pos_ranges_vec_pos_.resize(attribute_num+1);
  min_bounding_coords_end_ = NULL;
  read_round_done_.resize(attribute_num);
  subarray_tile_coords_ = NULL;
  subarray_tile_domain_ = NULL;

  for(int i=0; i<attribute_num+1; ++i) {
    empty_cells_written_[i] = 0;
    fragment_cell_pos_ranges_vec_pos_[i] = 0;
    read_round_done_[i] = true;
  }

  // Get fragment read states
  std::vector<Fragment*> fragments = array_->fragments();
  fragment_num_ = fragments.size();
  fragment_read_states_.resize(fragment_num_);
  for(int i=0; i<fragment_num_; ++i)
    fragment_read_states_[i] = fragments[i]->read_state(); 
}

ArrayReadState::~ArrayReadState() { 
  if(min_bounding_coords_end_ != NULL)
    free(min_bounding_coords_end_);

  if(subarray_tile_coords_ != NULL)
    free(subarray_tile_coords_);

  if(subarray_tile_domain_ != NULL)
    free(subarray_tile_domain_);

  int fragment_bounding_coords_num = fragment_bounding_coords_.size();
  for(int i=0; i<fragment_bounding_coords_num; ++i)
    if(fragment_bounding_coords_[i] != NULL)
      free(fragment_bounding_coords_[i]);
}




/* ****************************** */
/*           ACCESSORS            */
/* ****************************** */

bool ArrayReadState::overflow(int attribute_id) const {
  return overflow_[attribute_id];
}

int ArrayReadState::read(
    void** buffers, 
    size_t* buffer_sizes) {
  // Sanity check
  assert(fragment_num_);

  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  int attribute_num = array_schema->attribute_num();

  // Reset overflow
  overflow_.resize(attribute_num+1); 
  for(int i=0; i<attribute_num+1; ++i)
    overflow_[i] = false;
 
  for(int i=0; i<fragment_num_; ++i)
    fragment_read_states_[i]->reset_overflow();

  if(array_schema->dense())  // DENSE
    return read_dense(buffers, buffer_sizes);
  else                       // SPARSE
    return read_sparse(buffers, buffer_sizes);
}




/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

void ArrayReadState::clean_up_processed_fragment_cell_pos_ranges() {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  int attribute_num = array_schema->attribute_num();

  // Find the minimum overlapping tile position across all attributes
  const std::vector<int>& attribute_ids = array_->attribute_ids();
  int attribute_id_num = attribute_ids.size(); 
  int min_pos = fragment_cell_pos_ranges_vec_pos_[0];
  for(int i=1; i<attribute_id_num; ++i) 
    if(fragment_cell_pos_ranges_vec_pos_[attribute_ids[i]] < min_pos) 
      min_pos = fragment_cell_pos_ranges_vec_pos_[attribute_ids[i]];

  // Clean up processed overlapping tiles
  if(min_pos != 0) {
    // Remove overlapping tile elements from the vector
    FragmentCellPosRangesVec::iterator it_first = 
         fragment_cell_pos_ranges_vec_.begin(); 
    FragmentCellPosRangesVec::iterator it_last = it_first + min_pos; 
    fragment_cell_pos_ranges_vec_.erase(it_first, it_last); 

    // Update the positions
    for(int i=0; i<attribute_num+1; ++i)
      if(fragment_cell_pos_ranges_vec_pos_[i] != 0)
        fragment_cell_pos_ranges_vec_pos_[i] -= min_pos;
  }
}

template<class T>
int ArrayReadState::compute_fragment_cell_pos_ranges(
    FragmentCellRanges& fragment_cell_ranges,
    FragmentCellPosRanges& fragment_cell_pos_ranges) const {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  int dim_num = array_schema->dim_num();
  int fragment_i;
  int64_t fragment_cell_ranges_num = fragment_cell_ranges.size();

  // Compute fragment cell position ranges
  for(int64_t i=0; i<fragment_cell_ranges_num; ++i) { 
    fragment_i = fragment_cell_ranges[i].first.first;
    if(fragment_i == -1 ||
       fragment_read_states_[fragment_i]->dense()) {  // DENSE
      // Create a new fragment cell position range
      FragmentCellPosRange fragment_cell_pos_range;
      fragment_cell_pos_range.first = fragment_cell_ranges[i].first;
      CellPosRange& cell_pos_range = fragment_cell_pos_range.second;
      T* cell_range = static_cast<T*>(fragment_cell_ranges[i].second);
      cell_pos_range.first = array_schema->get_cell_pos(cell_range);
      cell_pos_range.second = array_schema->get_cell_pos(&cell_range[dim_num]);
      // Insert into the result
      fragment_cell_pos_ranges.push_back(fragment_cell_pos_range); 
    } else {                                          // SPARSE
      // Create a new fragment cell position range
      FragmentCellPosRange fragment_cell_pos_range;
      if(fragment_read_states_[fragment_cell_ranges[i].first.first]->
            get_fragment_cell_pos_range_sparse<T>(
                fragment_cell_ranges[i].first,
                static_cast<T*>(fragment_cell_ranges[i].second),
                fragment_cell_pos_range) != TILEDB_RS_OK) {
        // Clean up
        for(int j=i; j<fragment_cell_ranges_num; ++j) 
          free(fragment_cell_ranges[j].second);
        fragment_cell_ranges.clear();
        fragment_cell_pos_ranges.clear();
        // Exit
        return TILEDB_ARS_ERR;
      }
      // Insert into the result only valid fragment cell position ranges
      if(fragment_cell_pos_range.second.first != -1)
        fragment_cell_pos_ranges.push_back(fragment_cell_pos_range);
    }
    
    // Clean up corresponding input cell range
    free(fragment_cell_ranges[i].second);
  }

  // Clean up
  fragment_cell_ranges.clear();  

  // Success
  return TILEDB_ARS_OK;
}

template<class T>
void ArrayReadState::compute_min_bounding_coords_end() {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  int dim_num = array_schema->dim_num();
  size_t coords_size = array_schema->coords_size();

  // Allocate memeory
  if(min_bounding_coords_end_ == NULL)
    min_bounding_coords_end_ = malloc(coords_size);
  T* min_bounding_coords_end = static_cast<T*>(min_bounding_coords_end_);

  // Compute min bounding coords end
  bool first = true;
  for(int i=0; i<fragment_num_; ++i) {
    T* fragment_bounding_coords = static_cast<T*>(fragment_bounding_coords_[i]);
    if(fragment_bounding_coords != NULL) {
      if(first) { 
        memcpy(
            min_bounding_coords_end, 
            &fragment_bounding_coords[dim_num], 
            coords_size);
        first = false;
      } else if(array_schema->tile_cell_order_cmp(  
                    &fragment_bounding_coords[dim_num],
                    min_bounding_coords_end) < 0) {
        memcpy(
            min_bounding_coords_end, 
            &fragment_bounding_coords[dim_num], 
            coords_size);
      }
    }
  }
}

template<class T>
int ArrayReadState::compute_unsorted_fragment_cell_ranges_dense(
    FragmentCellRanges& unsorted_fragment_cell_ranges) {
  // Compute cell ranges for all fragments
  for(int i=0; i<fragment_num_; ++i) {
    if(!fragment_read_states_[i]->done()) {
      if(fragment_read_states_[i]->dense()) {     // DENSE
        // Get fragment cell ranges
        FragmentCellRanges fragment_cell_ranges;
        if(fragment_read_states_[i]->get_fragment_cell_ranges_dense<T>(
            i,
            fragment_cell_ranges) != TILEDB_RS_OK)
          return TILEDB_ARS_ERR;
        // Insert fragment cell ranges to the result
        unsorted_fragment_cell_ranges.insert(
            unsorted_fragment_cell_ranges.end(),
            fragment_cell_ranges.begin(),
            fragment_cell_ranges.end());
      } else {                                    // SPARSE
        FragmentCellRanges fragment_cell_ranges;
        do {
          // Get next overlapping tiles
          fragment_read_states_[i]->get_next_overlapping_tile_sparse<T>(
              static_cast<const T*>(subarray_tile_coords_));
          // Get fragment cell ranges
          fragment_cell_ranges.clear();
          if(fragment_read_states_[i]->get_fragment_cell_ranges_sparse<T>(
             i,
             fragment_cell_ranges) != TILEDB_RS_OK)
            return TILEDB_ARS_ERR;
          // Insert fragment cell ranges to the result
          unsorted_fragment_cell_ranges.insert(
              unsorted_fragment_cell_ranges.end(),
              fragment_cell_ranges.begin(),
              fragment_cell_ranges.end()); 
        } while(!fragment_read_states_[i]->done() &&
                fragment_read_states_[i]->mbr_overlaps_tile());
      }
    }
  }

  // Success
  return TILEDB_ARS_OK;
}

template<class T>
int ArrayReadState::compute_unsorted_fragment_cell_ranges_sparse(
    FragmentCellRanges& unsorted_fragment_cell_ranges) {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  int dim_num = array_schema->dim_num();
  size_t coords_size = array_schema->coords_size();
  T* min_bounding_coords_end = static_cast<T*>(min_bounding_coords_end_);

  // Compute the relevant fragment cell ranges
  for(int i=0; i<fragment_num_; ++i) {
    T* fragment_bounding_coords = static_cast<T*>(fragment_bounding_coords_[i]);

    // Compute new fragment cell ranges
    if(fragment_bounding_coords != NULL &&
       array_schema->tile_cell_order_cmp(
             fragment_bounding_coords,
             min_bounding_coords_end) <= 0) {
      FragmentCellRanges fragment_cell_ranges;
      if(fragment_read_states_[i]->get_fragment_cell_ranges_sparse<T>(
          i,
          fragment_bounding_coords,
          min_bounding_coords_end,
          fragment_cell_ranges) != TILEDB_RS_OK)
        return TILEDB_ARS_ERR;

      unsorted_fragment_cell_ranges.insert(
          unsorted_fragment_cell_ranges.end(),
          fragment_cell_ranges.begin(),
          fragment_cell_ranges.end());

      // If the end bounding coordinate is not the same as the smallest one, 
      // update the start bounding coordinate to exceed the smallest
      // end bounding coordinates
      if(memcmp(
             &fragment_bounding_coords[dim_num], 
             min_bounding_coords_end, 
             coords_size)) {
        // Get the first coordinates AFTER the min bounding coords end 
        bool coords_retrieved;
        if(fragment_read_states_[i]->get_coords_after<T>(
               min_bounding_coords_end, 
               fragment_bounding_coords,
               coords_retrieved) != TILEDB_RS_OK) {  
          return TILEDB_ARS_ERR;
        }

        // Sanity check for the sparse case
        assert(coords_retrieved);
      } 
    }
  }

  // Success
  return TILEDB_ARS_OK;
}

template<class T>
int ArrayReadState::copy_cells(
    int attribute_id,
    void* buffer,  
    size_t buffer_size,
    size_t& buffer_offset) {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  int64_t pos = fragment_cell_pos_ranges_vec_pos_[attribute_id];
  FragmentCellPosRanges& fragment_cell_pos_ranges = 
      fragment_cell_pos_ranges_vec_[pos];
  int64_t fragment_cell_pos_ranges_num = fragment_cell_pos_ranges.size();
  int fragment_i; // Fragment id
  int64_t tile_i; // Tile position in the fragment

  // Sanity check
  assert(!array_schema->var_size(attribute_id));

  // Copy the cell ranges one by one
  for(int64_t i=0; i<fragment_cell_pos_ranges_num; ++i) {
    fragment_i = fragment_cell_pos_ranges[i].first.first; 
    tile_i = fragment_cell_pos_ranges[i].first.second; 
    CellPosRange& cell_pos_range = fragment_cell_pos_ranges[i].second; 

    // Handle empty fragment
    if(fragment_i == -1) {
      copy_cells_with_empty<T>(
           attribute_id,
           buffer,
           buffer_size,
           buffer_offset,
           cell_pos_range);
      if(overflow_[attribute_id])
        break;
      else
        continue;
    }

    // Handle non-empty fragment
    if(fragment_read_states_[fragment_i]->copy_cells(
           attribute_id,
           tile_i,
           buffer,
           buffer_size,
           buffer_offset,
           cell_pos_range) != TILEDB_RS_OK)
       return TILEDB_ARS_ERR;

     // Handle overflow
     if(fragment_read_states_[fragment_i]->overflow(attribute_id)) {
       overflow_[attribute_id] = true;
       break;
     }
  } 

  // Handle the case the read round is done for this attribute
  if(!overflow_[attribute_id]) {
    ++fragment_cell_pos_ranges_vec_pos_[attribute_id];
    read_round_done_[attribute_id] = true;
  } else {
    read_round_done_[attribute_id] = false;
  }

  // Success
  return TILEDB_ARS_OK;
}

template<class T>
int ArrayReadState::copy_cells_var(
    int attribute_id,
    void* buffer,  
    size_t buffer_size,
    size_t& buffer_offset,
    void* buffer_var,  
    size_t buffer_var_size,
    size_t& buffer_var_offset) {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  int64_t pos = fragment_cell_pos_ranges_vec_pos_[attribute_id];
  FragmentCellPosRanges& fragment_cell_pos_ranges = 
      fragment_cell_pos_ranges_vec_[pos];
  int64_t fragment_cell_pos_ranges_num = fragment_cell_pos_ranges.size();
  int fragment_i; // Fragment id
  int64_t tile_i; // Tile position in the fragment

  // Sanity check
  assert(array_schema->var_size(attribute_id));

  // Copy the cell ranges one by one
  for(int64_t i=0; i<fragment_cell_pos_ranges_num; ++i) {
    tile_i = fragment_cell_pos_ranges[i].first.second; 
    fragment_i = fragment_cell_pos_ranges[i].first.first; 
    CellPosRange& cell_pos_range = fragment_cell_pos_ranges[i].second; 

    // Handle empty fragment
    if(fragment_i == -1) {
      copy_cells_with_empty_var<T>(
           attribute_id,
           buffer,
           buffer_size,
           buffer_offset,
           buffer_var,
           buffer_var_size,
           buffer_var_offset,
           cell_pos_range);
      if(overflow_[attribute_id])
        break;
      else
        continue;
    }

    // Handle non-empty fragment
    if(fragment_read_states_[fragment_i]->copy_cells_var(
           attribute_id,
           tile_i,
           buffer,
           buffer_size,
           buffer_offset,
           buffer_var,
           buffer_var_size,
           buffer_var_offset,
           cell_pos_range) != TILEDB_RS_OK)
       return TILEDB_ARS_ERR;

     // Handle overflow
     if(fragment_read_states_[fragment_i]->overflow(attribute_id)) {
       overflow_[attribute_id] = true;
       break;
     }
  } 

  // Handle the case the read round is done for this attribute
  if(!overflow_[attribute_id]) {
    ++fragment_cell_pos_ranges_vec_pos_[attribute_id];
    read_round_done_[attribute_id] = true;
  } else {
    read_round_done_[attribute_id] = false;
  }

  // Success
  return TILEDB_ARS_OK;
}

template<class T>
void ArrayReadState::copy_cells_with_empty(
    int attribute_id,
    void* buffer,  
    size_t buffer_size,
    size_t& buffer_offset,
    const CellPosRange& cell_pos_range) {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  size_t cell_size = array_schema->cell_size(attribute_id);
  char* buffer_c = static_cast<char*>(buffer);

  // Calculate free space in buffer
  size_t buffer_free_space = buffer_size - buffer_offset;
  buffer_free_space = (buffer_free_space / cell_size) * cell_size;
  if(buffer_free_space == 0) { // Overflow
    overflow_[attribute_id] = true;
    return;
  }

  // Sanity check
  assert(!array_schema->var_size(attribute_id));

  // Calculate number of empty cells to write
  int64_t cell_num_in_range = cell_pos_range.second - cell_pos_range.first + 1; 
  int64_t cell_num_left_to_copy = 
      cell_num_in_range - empty_cells_written_[attribute_id]; 
  size_t bytes_left_to_copy = cell_num_left_to_copy * cell_size;
  size_t bytes_to_copy = std::min(bytes_left_to_copy, buffer_free_space); 
  int64_t cell_num_to_copy = bytes_to_copy / cell_size; 

  // Get the empty value 
  int type = array_schema->type(attribute_id);
  void* empty_cell = malloc(cell_size);
  if(type == TILEDB_INT32) {
    int empty_cell_v = TILEDB_EMPTY_INT32;
    memcpy(empty_cell, &empty_cell_v, cell_size);
  } else if(type == TILEDB_INT64) {
    int64_t empty_cell_v = TILEDB_EMPTY_INT64;
    memcpy(empty_cell, &empty_cell_v, cell_size);
  } else if(type == TILEDB_FLOAT32) {
    float empty_cell_v = TILEDB_EMPTY_FLOAT32;
    memcpy(empty_cell, &empty_cell_v, cell_size);
  } else if(type == TILEDB_FLOAT64) {
    double empty_cell_v = TILEDB_EMPTY_FLOAT64;
    memcpy(empty_cell, &empty_cell_v, cell_size);
  } else if(type == TILEDB_CHAR) {
    char empty_cell_v = TILEDB_EMPTY_CHAR;
    memcpy(empty_cell, &empty_cell_v, cell_size);
  }

  // Copy empty cells to buffer
  for(int64_t i=0; i<cell_num_to_copy; ++i) {
    memcpy(buffer_c + buffer_offset, empty_cell, cell_size);
    buffer_offset += cell_size;
  } 
  empty_cells_written_[attribute_id] += cell_num_to_copy;

  // Handle buffer overflow
  if(empty_cells_written_[attribute_id] != cell_num_in_range) 
    overflow_[attribute_id] = true;
  else // Done copying this range
    empty_cells_written_[attribute_id] = 0;

  // Clean up
  free(empty_cell);
}

template<class T>
void ArrayReadState::copy_cells_with_empty_var(
    int attribute_id,
    void* buffer,  
    size_t buffer_size,
    size_t& buffer_offset,
    void* buffer_var,  
    size_t buffer_var_size,
    size_t& buffer_var_offset,
    const CellPosRange& cell_pos_range) {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  size_t cell_size = TILEDB_CELL_VAR_OFFSET_SIZE;
  char* buffer_c = static_cast<char*>(buffer);
  char* buffer_var_c = static_cast<char*>(buffer_var);

  // Calculate free space in buffer
  size_t buffer_free_space = buffer_size - buffer_offset;
  buffer_free_space = (buffer_free_space / cell_size) * cell_size;
  size_t buffer_var_free_space = buffer_var_size - buffer_var_offset;

  // Handle overflow
  if(buffer_free_space == 0 || buffer_var_free_space == 0) { // Overflow
    overflow_[attribute_id] = true; 
    return;
  }

  // Get the empty value 
  int type = array_schema->type(attribute_id);
  void* empty_cell = malloc(cell_size);
  size_t cell_size_var;
  if(type == TILEDB_INT32) {
    int empty_cell_v = TILEDB_EMPTY_INT32;
    memcpy(empty_cell, &empty_cell_v, cell_size);
    cell_size_var = sizeof(int);
  } else if(type == TILEDB_INT64) {
    int64_t empty_cell_v = TILEDB_EMPTY_INT64;
    memcpy(empty_cell, &empty_cell_v, cell_size);
    cell_size_var = sizeof(int64_t);
  } else if(type == TILEDB_FLOAT32) {
    float empty_cell_v = TILEDB_EMPTY_FLOAT32;
    memcpy(empty_cell, &empty_cell_v, cell_size);
    cell_size_var = sizeof(float);
  } else if(type == TILEDB_FLOAT64) {
    double empty_cell_v = TILEDB_EMPTY_FLOAT64;
    memcpy(empty_cell, &empty_cell_v, cell_size);
    cell_size_var = sizeof(double);
  } else if(type == TILEDB_CHAR) {
    char empty_cell_v = TILEDB_EMPTY_CHAR;
    memcpy(empty_cell, &empty_cell_v, cell_size);
    cell_size_var = sizeof(char);
  }

  // Sanity check
  assert(array_schema->var_size(attribute_id));

  // Calculate cell number to copy
  int64_t cell_num_in_range = cell_pos_range.second - cell_pos_range.first + 1; 
  int64_t cell_num_left_to_copy = 
      cell_num_in_range - empty_cells_written_[attribute_id]; 
  size_t bytes_left_to_copy = cell_num_left_to_copy * cell_size;
  size_t bytes_left_to_copy_var = cell_num_left_to_copy * cell_size_var;
  size_t bytes_to_copy = std::min(bytes_left_to_copy, buffer_free_space); 
  size_t bytes_to_copy_var = 
      std::min(bytes_left_to_copy_var, buffer_var_free_space); 
  int64_t cell_num_to_copy = bytes_to_copy / cell_size; 
  int64_t cell_num_to_copy_var = bytes_to_copy_var / cell_size_var; 
  cell_num_to_copy = std::min(cell_num_to_copy, cell_num_to_copy_var);

  // Copy empty cells to buffers
  for(int64_t i=0; i<cell_num_to_copy; ++i) {
    memcpy(
        buffer_c + buffer_offset, 
        &buffer_var_offset, 
        cell_size);
    buffer_offset += cell_size;
    memcpy(
        buffer_var_c + buffer_var_offset, 
        empty_cell,
        cell_size_var);
    buffer_var_offset += cell_size_var;
  }
  empty_cells_written_[attribute_id] += cell_num_to_copy;

  // Handle buffer overflow
  if(empty_cells_written_[attribute_id] != cell_num_in_range) 
    overflow_[attribute_id] = true;
  else // Done copying this range
    empty_cells_written_[attribute_id] = 0;

  // Clean up
  free(empty_cell);
}

template<class T>
int ArrayReadState::get_next_fragment_cell_ranges_dense() {
  // Trivial case
  if(done_)
    return TILEDB_ARS_OK;

  // Get the next overlapping tile for each fragment
  get_next_overlapping_tiles_dense<T>();

  // Return if there are no more overlapping tiles
  if(done_) 
    return TILEDB_ARS_OK;

  // Compute the unsorted fragment cell ranges needed for this read run
  FragmentCellRanges unsorted_fragment_cell_ranges;
  if(compute_unsorted_fragment_cell_ranges_dense<T>(
         unsorted_fragment_cell_ranges) != TILEDB_ARS_OK)
    return TILEDB_ARS_ERR;

  // Sort fragment cell ranges
  FragmentCellRanges fragment_cell_ranges;
  if(sort_fragment_cell_ranges<T>(
         unsorted_fragment_cell_ranges, 
         fragment_cell_ranges) != TILEDB_ARS_OK) 
    return TILEDB_ARS_ERR;

  // Compute the fragment cell position ranges
  FragmentCellPosRanges fragment_cell_pos_ranges;
  if(compute_fragment_cell_pos_ranges<T>(
         fragment_cell_ranges, 
         fragment_cell_pos_ranges) != TILEDB_ARS_OK) 
    return TILEDB_ARS_ERR;

  // Insert cell pos ranges in the state
  fragment_cell_pos_ranges_vec_.push_back(fragment_cell_pos_ranges);

  // Clean up processed overlapping tiles
  clean_up_processed_fragment_cell_pos_ranges();

  // Success
  return TILEDB_ARS_OK;
}


template<class T>
int ArrayReadState::get_next_fragment_cell_ranges_sparse() {
  // Trivial case
  if(done_)
    return TILEDB_ARS_OK;

  // Gets the next overlapping tiles in the fragment read states
  get_next_overlapping_tiles_sparse<T>();

  // Return if there are no more overlapping tiles
  if(done_) 
    return TILEDB_ARS_OK;

  // Compute smallest end bounding coordinates
  compute_min_bounding_coords_end<T>(); 

  // Compute the unsorted fragment cell ranges needed for this read run
  FragmentCellRanges unsorted_fragment_cell_ranges;
  if(compute_unsorted_fragment_cell_ranges_sparse<T>(
         unsorted_fragment_cell_ranges) != TILEDB_ARS_OK)
    return TILEDB_ARS_ERR;

  // Sort fragment cell ranges
  FragmentCellRanges fragment_cell_ranges;
  if(sort_fragment_cell_ranges<T>(
         unsorted_fragment_cell_ranges, 
         fragment_cell_ranges) != TILEDB_ARS_OK) 
    return TILEDB_ARS_ERR;

  // Compute the fragment cell position ranges
  FragmentCellPosRanges fragment_cell_pos_ranges;
  if(compute_fragment_cell_pos_ranges<T>(
         fragment_cell_ranges, 
         fragment_cell_pos_ranges) != TILEDB_ARS_OK) 
    return TILEDB_ARS_ERR;

  // Insert cell pos ranges in the state
  fragment_cell_pos_ranges_vec_.push_back(fragment_cell_pos_ranges);

  // Clean up processed overlapping tiles
  clean_up_processed_fragment_cell_pos_ranges();

  // Success
  return TILEDB_ARS_OK;
}

template<class T>
void ArrayReadState::get_next_overlapping_tiles_dense() {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  int dim_num = array_schema->dim_num();
  size_t coords_size = array_schema->coords_size();

  // Get the first overlapping tile for each fragment
  if(fragment_cell_pos_ranges_vec_.size() == 0) {

    // Initialize subarray tile coordinates
    init_subarray_tile_coords<T>();

    // Return if there are no more overlapping tiles
    if(subarray_tile_coords_ == NULL) {
      done_ = true;
      return;
    }

    // Get next overlapping tile
    for(int i=0; i<fragment_num_; ++i) { 
      if(fragment_read_states_[i]->dense())
        fragment_read_states_[i]->get_next_overlapping_tile_dense<T>(
            static_cast<const T*>(subarray_tile_coords_));
      // else, it is handled in compute_unsorted_fragment_cell_ranges
    }
  } else { 
    // Temporarily store the current subarray tile coordinates
    assert(subarray_tile_coords_ != NULL);
    T* previous_subarray_tile_coords = new T[dim_num];
    memcpy(
        previous_subarray_tile_coords,
        subarray_tile_coords_,
        coords_size);

    // Advance range coordinates
    get_next_subarray_tile_coords<T>();

    // Return if there are no more overlapping tiles
    if(subarray_tile_coords_ == NULL) {
      done_ = true;
      delete [] previous_subarray_tile_coords;
      return;
    }

    // Get next overlapping tiles for the processed fragments
    for(int i=0; i<fragment_num_; ++i) {
      if(!fragment_read_states_[i]->done()) {
        if(fragment_read_states_[i]->dense())
          fragment_read_states_[i]->get_next_overlapping_tile_dense<T>(
            static_cast<const T*>(subarray_tile_coords_));
        // else, it is handled in compute_unsorted_fragment_cell_ranges
      }
    }

    // Clean up
    delete [] previous_subarray_tile_coords;
  }
}

template<class T>
void ArrayReadState::get_next_overlapping_tiles_sparse() {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  int dim_num = array_schema->dim_num();
  size_t coords_size = array_schema->coords_size();

  // Get the first overlapping tile for each fragment
  if(fragment_cell_pos_ranges_vec_.size() == 0) {
    // Initializations 
    assert(fragment_bounding_coords_.size() == 0);
    fragment_bounding_coords_.resize(fragment_num_);

    // Get next overlapping tile and bounding coordinates 
    done_ = true;
    for(int i=0; i<fragment_num_; ++i) { 
      fragment_read_states_[i]->get_next_overlapping_tile_sparse<T>();
      if(!fragment_read_states_[i]->done()) {
        fragment_bounding_coords_[i] = malloc(2*coords_size);
        fragment_read_states_[i]->get_bounding_coords(
            fragment_bounding_coords_[i]);
        done_ = false;
      } else {
        fragment_bounding_coords_[i] = NULL;
      }
    }
  } else { 
    // Get the next overlapping tile for the appropriate fragments
    for(int i=0; i<fragment_num_; ++i) { 
      // Get next overlappint tile
      T* fragment_bounding_coords = 
          static_cast<T*>(fragment_bounding_coords_[i]);
      if(fragment_bounding_coords_[i] != NULL &&
         !memcmp(                            // Coinciding end bounding coords
             &fragment_bounding_coords[dim_num], 
             min_bounding_coords_end_, 
             coords_size)) { 
        fragment_read_states_[i]->get_next_overlapping_tile_sparse<T>();
        if(!fragment_read_states_[i]->done()) {
          fragment_read_states_[i]->get_bounding_coords(
              fragment_bounding_coords_[i]);
        } else {
          if(fragment_bounding_coords_[i])
            free(fragment_bounding_coords_[i]);
          fragment_bounding_coords_[i] = NULL;
        }
      }
    }

    // Check if done
    done_ = true;
    for(int i=0; i<fragment_num_; ++i) { 
      if(fragment_bounding_coords_[i] != NULL) { 
        done_ = false;
        break;
      }
    }
  }
}

template<class T>
void ArrayReadState::init_subarray_tile_coords() {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  int dim_num = array_schema->dim_num();
  size_t coords_size = array_schema->coords_size();
  const T* tile_extents = static_cast<const T*>(array_schema->tile_extents());
  const T* subarray = static_cast<const T*>(array_->subarray());

  // Sanity checks
  assert(tile_extents != NULL);
  assert(subarray_tile_domain_ == NULL);

  // Allocate space for tile domain and subarray tile domain
  T* tile_domain = new T[2*dim_num];
  subarray_tile_domain_ = malloc(2*dim_num*sizeof(T));
  T* subarray_tile_domain = static_cast<T*>(subarray_tile_domain_);

  // Get subarray in tile domain
  array_schema->get_subarray_tile_domain<T>(
      subarray, 
      tile_domain,
      subarray_tile_domain);

  // Check if there is any overlap between the subarray tile domain and the
  // array tile domain
  bool overlap = true;
  for(int i=0; i<dim_num; ++i) {
    if(subarray_tile_domain[2*i] > tile_domain[2*i+1] ||
       subarray_tile_domain[2*i+1] < tile_domain[2*i]) {
      overlap = false;
      break;
    }
  }

  // Calculate subarray tile coordinates
  if(!overlap) {  // No overlap
    free(subarray_tile_domain_);
    subarray_tile_domain_ = NULL; 
    assert(subarray_tile_coords_ == NULL); 
  } else {        // Overlap
    subarray_tile_coords_ = malloc(coords_size);
    T* subarray_tile_coords = static_cast<T*>(subarray_tile_coords_);
    for(int i=0; i<dim_num; ++i) 
      subarray_tile_coords[i] = subarray_tile_domain[2*i]; 
  }

  // Clean up
  delete [] tile_domain;
}

template<class T>
void ArrayReadState::get_next_subarray_tile_coords() {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  int dim_num = array_schema->dim_num();
  T* subarray_tile_domain = static_cast<T*>(subarray_tile_domain_);
  T* subarray_tile_coords = static_cast<T*>(subarray_tile_coords_);

  // Advance subarray tile coordinates
  array_schema->get_next_tile_coords<T>(
      subarray_tile_domain, 
      subarray_tile_coords);

  // Check if the new subarray coordinates fall out of the range domain
  bool inside_domain = true;
  for(int i=0; i<dim_num; ++i) {
    if(subarray_tile_coords[i] < subarray_tile_domain[2*i] ||
       subarray_tile_coords[i] > subarray_tile_domain[2*i+1]) {
      inside_domain = false;
      break;
    }
  }

  // The coordinates fall outside the domain
  if(!inside_domain) {  
    free(subarray_tile_domain_);
    subarray_tile_domain_ = NULL; 
    free(subarray_tile_coords_);
    subarray_tile_coords_ = NULL; 
  } 
}

int ArrayReadState::read_dense(
    void** buffers,  
    size_t* buffer_sizes) {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  std::vector<int> attribute_ids = array_->attribute_ids();
  int attribute_id_num = attribute_ids.size(); 

  // Read each attribute individually
  int buffer_i = 0;
  for(int i=0; i<attribute_id_num; ++i) {
    if(!array_schema->var_size(attribute_ids[i])) { // FIXED CELLS
      if(read_dense_attr(
             attribute_ids[i], 
             buffers[buffer_i], 
             buffer_sizes[buffer_i]) != TILEDB_ARS_OK)
        return TILEDB_ARS_ERR;
      ++buffer_i;
    } else {                                        // VARIABLE-SIZED CELLS
      if(read_dense_attr_var(
             attribute_ids[i], 
             buffers[buffer_i],       // offsets 
             buffer_sizes[buffer_i],
             buffers[buffer_i+1],     // actual values
             buffer_sizes[buffer_i+1]) != TILEDB_ARS_OK)
        return TILEDB_ARS_ERR;
      buffer_i += 2;
    }
  }

  // Success
  return TILEDB_ARS_OK; 
}

int ArrayReadState::read_dense_attr(
    int attribute_id,
    void* buffer,  
    size_t& buffer_size) {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  int coords_type = array_schema->coords_type();

  // Invoke the proper templated function
  if(coords_type == TILEDB_INT32) {
    return read_dense_attr<int>(
               attribute_id, 
               buffer, 
               buffer_size);
  } else if(coords_type == TILEDB_INT64) {
    return read_dense_attr<int64_t>(
               attribute_id, 
               buffer, 
               buffer_size);
  } else {
    PRINT_ERROR("Cannot read from array; Invalid coordinates type");
    return TILEDB_ARS_ERR;
  }
}

template<class T>
int ArrayReadState::read_dense_attr(
    int attribute_id,
    void* buffer,  
    size_t& buffer_size) {
  // Auxiliary variables
  size_t buffer_offset = 0;

  // Until read is done or there is a buffer overflow 
  for(;;) {
    // Continue copying from the previous unfinished read round
    if(!read_round_done_[attribute_id])
      if(copy_cells<T>(
             attribute_id,
             buffer, 
             buffer_size, 
             buffer_offset) != TILEDB_ARS_OK)
        return TILEDB_ARS_ERR;

    // Check for overflow
    if(overflow_[attribute_id]) {
      buffer_size = buffer_offset;
      return TILEDB_ARS_OK;
    }

    // Prepare the cell ranges for the next read round
    if(fragment_cell_pos_ranges_vec_pos_[attribute_id] >= 
       int64_t(fragment_cell_pos_ranges_vec_.size())) {
      // Get next cell ranges
      if(get_next_fragment_cell_ranges_dense<T>() != TILEDB_ARS_OK)
        return TILEDB_ARS_ERR;
    }

    // Check if read is done
    if(done_ &&
       fragment_cell_pos_ranges_vec_pos_[attribute_id] == 
       int64_t(fragment_cell_pos_ranges_vec_.size())) {
      buffer_size = buffer_offset;
      return TILEDB_ARS_OK;
    }
 
    // Copy cells to buffers
    if(copy_cells<T>(
           attribute_id, 
           buffer, 
           buffer_size, 
           buffer_offset) != TILEDB_ARS_OK)
      return TILEDB_ARS_ERR;

    // Check for buffer overflow
    if(overflow_[attribute_id]) {
      buffer_size = buffer_offset;
      return TILEDB_ARS_OK;
    }
  } 
}

int ArrayReadState::read_dense_attr_var(
    int attribute_id,
    void* buffer,  
    size_t& buffer_size,
    void* buffer_var,  
    size_t& buffer_var_size) {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  int coords_type = array_schema->coords_type();

  // Invoke the proper templated function
  if(coords_type == TILEDB_INT32) {
    return read_dense_attr_var<int>(
               attribute_id, 
               buffer, 
               buffer_size,
               buffer_var, 
               buffer_var_size);
  } else if(coords_type == TILEDB_INT64) {
    return read_dense_attr_var<int64_t>(
               attribute_id, 
               buffer, 
               buffer_size,
               buffer_var, 
               buffer_var_size);
  } else {
    PRINT_ERROR("Cannot read from array; Invalid coordinates type");
    return TILEDB_ARS_ERR;
  }
}

template<class T>
int ArrayReadState::read_dense_attr_var(
    int attribute_id,
    void* buffer,  
    size_t& buffer_size,
    void* buffer_var,  
    size_t& buffer_var_size) {
    // Auxiliary variables
  size_t buffer_offset = 0;
  size_t buffer_var_offset = 0;

  // Until read is done or there is a buffer overflow 
  for(;;) {
    // Continue copying from the previous unfinished read round
    if(!read_round_done_[attribute_id])
      if(copy_cells_var<T>(
             attribute_id,
             buffer, 
             buffer_size, 
             buffer_offset,
             buffer_var, 
             buffer_var_size,
             buffer_var_offset) != TILEDB_ARS_OK)
        return TILEDB_ARS_ERR;

    // Check for overflow
    if(overflow_[attribute_id]) {
      buffer_size = buffer_offset;
      buffer_var_size = buffer_var_offset;
      return TILEDB_ARS_OK;
    }

    // Prepare the cell ranges for the next read round
    if(fragment_cell_pos_ranges_vec_pos_[attribute_id] >= 
       int64_t(fragment_cell_pos_ranges_vec_.size())) {
      // Get next cell ranges
      if(get_next_fragment_cell_ranges_dense<T>() != TILEDB_ARS_OK)
        return TILEDB_ARS_ERR;
    }

    // Check if read is done
    if(done_ &&
       fragment_cell_pos_ranges_vec_pos_[attribute_id] == 
       int64_t(fragment_cell_pos_ranges_vec_.size())) {
      buffer_size = buffer_offset;
      buffer_var_size = buffer_var_offset;
      return TILEDB_ARS_OK;
    }
 
    // Copy cells to buffers
    if(copy_cells_var<T>(
             attribute_id,
             buffer, 
             buffer_size, 
             buffer_offset,
             buffer_var, 
             buffer_var_size,
             buffer_var_offset) != TILEDB_ARS_OK)
      return TILEDB_ARS_ERR;

    // Check for buffer overflow
    if(overflow_[attribute_id]) {
      buffer_size = buffer_offset;
      buffer_var_size = buffer_var_offset;
      return TILEDB_ARS_OK;
    }
  }
}

int ArrayReadState::read_sparse(
    void** buffers,  
    size_t* buffer_sizes) {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  int attribute_num = array_schema->attribute_num();
  std::vector<int> attribute_ids = array_->attribute_ids();
  int attribute_id_num = attribute_ids.size(); 

  // Find the coordinates buffer
  int coords_buffer_i = -1;
  int buffer_i = 0;
  for(int i=0; i<attribute_id_num; ++i) { 
    if(attribute_ids[i] == attribute_num) {
      coords_buffer_i = buffer_i;
      break;
    }
    if(!array_schema->var_size(attribute_ids[i])) // FIXED CELLS
      ++buffer_i;
    else                                          // VARIABLE-SIZED CELLS
      buffer_i +=2;
  }

  // Read coordinates attribute first
  if(coords_buffer_i != -1) {
    if(read_sparse_attr(
           attribute_num, 
           buffers[coords_buffer_i], 
           buffer_sizes[coords_buffer_i]) != TILEDB_ARS_OK)
      return TILEDB_ARS_ERR;
  }  

  // Read each attribute individually
  buffer_i = 0;
  for(int i=0; i<attribute_id_num; ++i) {
    // Skip coordinates attribute (already read)
    if(attribute_ids[i] == attribute_num) {
      ++buffer_i;
      continue;
    }

    if(!array_schema->var_size(attribute_ids[i])) { // FIXED CELLS
      if(read_sparse_attr(
             attribute_ids[i], 
             buffers[buffer_i], 
             buffer_sizes[buffer_i]) != TILEDB_ARS_OK)
        return TILEDB_ARS_ERR;

      ++buffer_i;
    } else {                                        // VARIABLE-SIZED CELLS
      if(read_sparse_attr_var(
             attribute_ids[i], 
             buffers[buffer_i],       // offsets 
             buffer_sizes[buffer_i],
             buffers[buffer_i+1],     // actual values
             buffer_sizes[buffer_i+1]) != TILEDB_ARS_OK)
        return TILEDB_ARS_ERR;

      buffer_i += 2;
    }
  }

  // Success
  return TILEDB_ARS_OK; 
}

int ArrayReadState::read_sparse_attr(
    int attribute_id,
    void* buffer,  
    size_t& buffer_size) {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  int coords_type = array_schema->coords_type();

  // Invoke the proper templated function
  if(coords_type == TILEDB_INT32) {
    return read_sparse_attr<int>(
               attribute_id, 
               buffer, 
               buffer_size);
  } else if(coords_type == TILEDB_INT64) {
    return read_sparse_attr<int64_t>(
               attribute_id, 
               buffer, 
               buffer_size);
  } else if(coords_type == TILEDB_FLOAT32) {
    return read_sparse_attr<float>(
               attribute_id, 
               buffer, 
               buffer_size);
  } else if(coords_type == TILEDB_FLOAT64) {
    return read_sparse_attr<double>(
               attribute_id, 
               buffer, 
               buffer_size);
  } else {
    PRINT_ERROR("Cannot read from array; Invalid coordinates type");
    return TILEDB_ARS_ERR;
  }
}

template<class T>
int ArrayReadState::read_sparse_attr(
    int attribute_id,
    void* buffer,  
    size_t& buffer_size) {
  // Auxiliary variables
  size_t buffer_offset = 0;

  // Until read is done or there is a buffer overflow 
  for(;;) {
    // Continue copying from the previous unfinished read round
    if(!read_round_done_[attribute_id])
      if(copy_cells<T>(
             attribute_id,
             buffer, 
             buffer_size, 
             buffer_offset) != TILEDB_ARS_OK)
        return TILEDB_ARS_ERR;

    // Check for overflow
    if(overflow_[attribute_id]) {
      buffer_size = buffer_offset;
      return TILEDB_ARS_OK;
    }

    // Prepare the cell ranges for the next read round
    if(fragment_cell_pos_ranges_vec_pos_[attribute_id] >= 
       int64_t(fragment_cell_pos_ranges_vec_.size())) {
      // Get next cell ranges
      if(get_next_fragment_cell_ranges_sparse<T>() != TILEDB_ARS_OK)
        return TILEDB_ARS_ERR;
    }

    // Check if read is done
    if(done_ && 
       fragment_cell_pos_ranges_vec_pos_[attribute_id] == 
       int64_t(fragment_cell_pos_ranges_vec_.size())) {
      buffer_size = buffer_offset;
      return TILEDB_ARS_OK;
    }

    // Copy cells to buffers
    if(copy_cells<T>(
           attribute_id, 
           buffer, 
           buffer_size, 
           buffer_offset) != TILEDB_ARS_OK)
      return TILEDB_ARS_ERR;

    // Check for buffer overflow
    if(overflow_[attribute_id]) {
      buffer_size = buffer_offset;
      return TILEDB_ARS_OK;
    }
  } 
}

int ArrayReadState::read_sparse_attr_var(
    int attribute_id,
    void* buffer,  
    size_t& buffer_size,
    void* buffer_var,  
    size_t& buffer_var_size) {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  int coords_type = array_schema->coords_type();

  // Invoke the proper templated function
  if(coords_type == TILEDB_INT32) {
    return read_sparse_attr_var<int>(
               attribute_id, 
               buffer, 
               buffer_size,
               buffer_var, 
               buffer_var_size);
  } else if(coords_type == TILEDB_INT64) {
    return read_sparse_attr_var<int64_t>(
               attribute_id, 
               buffer, 
               buffer_size,
               buffer_var, 
               buffer_var_size);
  } else if(coords_type == TILEDB_FLOAT32) {
    return read_sparse_attr_var<float>(
               attribute_id, 
               buffer, 
               buffer_size,
               buffer_var, 
               buffer_var_size);
  } else if(coords_type == TILEDB_FLOAT64) {
    return read_sparse_attr_var<double>(
               attribute_id, 
               buffer, 
               buffer_size,
               buffer_var, 
               buffer_var_size);
  } else {
    PRINT_ERROR("Cannot read from array; Invalid coordinates type");
    return TILEDB_ARS_ERR;
  }
}

template<class T>
int ArrayReadState::read_sparse_attr_var(
    int attribute_id,
    void* buffer,  
    size_t& buffer_size,
    void* buffer_var,  
    size_t& buffer_var_size) {
    // Auxiliary variables
  size_t buffer_offset = 0;
  size_t buffer_var_offset = 0;

  // Until read is done or there is a buffer overflow 
  for(;;) {
    // Continue copying from the previous unfinished read round
    if(!read_round_done_[attribute_id])
      if(copy_cells_var<T>(
             attribute_id,
             buffer, 
             buffer_size, 
             buffer_offset,
             buffer_var, 
             buffer_var_size,
             buffer_var_offset) != TILEDB_ARS_OK)
        return TILEDB_ARS_ERR;

    // Check for overflow
    if(overflow_[attribute_id]) {
      buffer_size = buffer_offset;
      buffer_var_size = buffer_var_offset;
      return TILEDB_ARS_OK;
    }

    // Prepare the cell ranges for the next read round
    if(fragment_cell_pos_ranges_vec_pos_[attribute_id] >= 
       int64_t(fragment_cell_pos_ranges_vec_.size())) {
      // Get next overlapping tiles
      if(get_next_fragment_cell_ranges_sparse<T>() != TILEDB_ARS_OK)
        return TILEDB_ARS_ERR;
    }

    // Check if read is done
    if(done_ &&
       fragment_cell_pos_ranges_vec_pos_[attribute_id] == 
       int64_t(fragment_cell_pos_ranges_vec_.size())) {
      buffer_size = buffer_offset;
      buffer_var_size = buffer_var_offset;
      return TILEDB_ARS_OK;
    }
 
    // Copy cells to buffers
    if(copy_cells_var<T>(
             attribute_id,
             buffer, 
             buffer_size, 
             buffer_offset,
             buffer_var, 
             buffer_var_size,
             buffer_var_offset) != TILEDB_ARS_OK)
      return TILEDB_ARS_ERR;

    // Check for buffer overflow
    if(overflow_[attribute_id]) {
      buffer_size = buffer_offset;
      buffer_var_size = buffer_var_offset;
      return TILEDB_ARS_OK;
    }
  }
}

template<class T>
int ArrayReadState::sort_fragment_cell_ranges(
    FragmentCellRanges& unsorted_fragment_cell_ranges,
    FragmentCellRanges& fragment_cell_ranges) const {
  // Trivial case - single fragment
  if(fragment_num_ == 1) {
    fragment_cell_ranges = unsorted_fragment_cell_ranges;
    unsorted_fragment_cell_ranges.clear();
    return TILEDB_ARS_OK;
  }

  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  int dim_num = array_schema->dim_num();
  size_t coords_size = array_schema->coords_size();
  const T* domain = static_cast<const T*>(array_schema->domain());
  const T* tile_extents = static_cast<const T*>(array_schema->tile_extents());
  const T* tile_coords = static_cast<const T*>(subarray_tile_coords_);
  int rc = TILEDB_ARS_OK;

  // Compute tile domain
  // This is non-NULL only in the dense array case
  T* tile_domain = NULL;
  if(tile_coords != NULL) {
    tile_domain = new T[2*dim_num];
    for(int i=0; i<dim_num; ++i) {
      tile_domain[2*i] = domain[2*i] + tile_coords[i] * tile_extents[i];
      tile_domain[2*i+1] = tile_domain[2*i] + tile_extents[i] - 1;
    } 
  }

  // Populate queue
  std::priority_queue<
      FragmentCellRange,
      FragmentCellRanges,
      SmallerFragmentCellRange<T> > pq(array_schema);
  int unsorted_fragment_cell_ranges_num = unsorted_fragment_cell_ranges.size();
  for(int64_t i=0; i<unsorted_fragment_cell_ranges_num; ++i)  
    pq.push(unsorted_fragment_cell_ranges[i]);
  unsorted_fragment_cell_ranges.clear();
  
  // Start processing the queue
  FragmentCellRange popped, top;
  int popped_fragment_i, top_fragment_i, popped_tile_i, top_tile_i;
  T *popped_range, *top_range;
  while(!pq.empty()) {
    // Pop the first entry and mark it as popped
    popped = pq.top();
    popped_fragment_i = popped.first.first;
    popped_tile_i = popped.first.second;
    popped_range = static_cast<T*>(popped.second);
    pq.pop();

    // Last range - just insert it into the results and stop
    if(pq.empty()) {
      fragment_cell_ranges.push_back(popped);
      break;
    }

    // Mark the second entry (now top) as top
    top = pq.top();
    top_fragment_i = top.first.first;
    top_tile_i = top.first.second;
    top_range = static_cast<T*>(top.second);

    // Dinstinguish two cases
    if(popped_fragment_i == -1 ||                       // DENSE OR UNARY POPPED
       fragment_read_states_[popped_fragment_i]->dense() ||
       !memcmp(popped_range, &popped_range[dim_num], coords_size)) { 
      // Keep on discarding ranges from the queue
      while(!pq.empty() &&
            top_fragment_i < popped_fragment_i &&
            array_schema->cell_order_cmp(top_range, popped_range) >= 0 &&
            array_schema->cell_order_cmp(
                top_range, 
                &popped_range[dim_num]) <= 0) {
        // Cut the top range and re-insert, only if there is partial overlap
        if(array_schema->cell_order_cmp(
               &top_range[dim_num], 
               &popped_range[dim_num]) > 0) {
          // Create the new trimmed top range
          FragmentCellRange trimmed_top;
          trimmed_top.first = FragmentInfo(top_fragment_i, top_tile_i);
          trimmed_top.second = malloc(2*coords_size);
          T* trimmed_top_range = static_cast<T*>(trimmed_top.second);
          memcpy(trimmed_top_range, &popped_range[dim_num], coords_size);
          memcpy(&trimmed_top_range[dim_num], &top_range[dim_num], coords_size);
          if(fragment_read_states_[top_fragment_i]->dense()) {
            array_schema->get_next_cell_coords<T>( // TOP IS DENSE
                tile_domain, 
                trimmed_top_range);
            pq.push(trimmed_top);
          } else {                                 // TOP IS SPARSE
            bool coords_retrieved;
            if(fragment_read_states_[top_fragment_i]->get_coords_after(
                   &popped_range[dim_num], 
                   trimmed_top_range,
                   coords_retrieved)) {
              free(trimmed_top_range);
              free(top_range);
              free(popped_range);
              return TILEDB_ARS_ERR;
            }
            if(coords_retrieved)
              pq.push(trimmed_top);
            else
              free(trimmed_top_range);
          }
        } 

        // Discard top and get a new one
        free(top.second);
        pq.pop();
        top = pq.top();
        top_fragment_i = top.first.first;
        top_tile_i = top.first.second;
        top_range = static_cast<T*>(top.second);
      }

      // Potentially trim the popped range
      if(!pq.empty() && 
         top_fragment_i > popped_fragment_i && 
         array_schema->cell_order_cmp(
             top_range, 
             &popped_range[dim_num]) <= 0) {         
        // Create a new popped range
        FragmentCellRange extra_popped;
        extra_popped.first.first = popped_fragment_i;
        extra_popped.first.second = popped_tile_i;
        extra_popped.second = malloc(2*coords_size);
        T* extra_popped_range = static_cast<T*>(extra_popped.second);

        memcpy(extra_popped_range, top_range, coords_size);
        memcpy(
            &extra_popped_range[dim_num], 
            &popped_range[dim_num], 
            coords_size);

        // Re-instert the extra popped range into the queue
        pq.push(extra_popped);

        // Trim last range coordinates of popped
        memcpy(&popped_range[dim_num], top_range, coords_size);

        // Get previous cell of the last range coordinates of popped
        array_schema->get_previous_cell_coords<T>(
            tile_domain, 
            &popped_range[dim_num]);
      }  
     
      // Insert the final popped range into the results
      fragment_cell_ranges.push_back(popped);
    } else {                               // SPARSE POPPED
      // If popped does not overlap with top, insert popped into results
      if(!pq.empty() && 
         array_schema->tile_cell_order_cmp(
             top_range,
             &popped_range[dim_num]) > 0) {
        fragment_cell_ranges.push_back(popped);
      } else {
        // Create up to 3 more ranges (left, new popped/right, unary)
        FragmentCellRange left;
        left.first.first = popped_fragment_i;
        left.first.second = popped_tile_i;
        left.second = malloc(2*coords_size);
        memcpy(left.second, popped_range, coords_size);
        T* left_range = static_cast<T*>(left.second);
        
        // Get the first two coordinates from the coordinates tile 
        bool left_retrieved, right_retrieved, target_exists;
        if(fragment_read_states_[popped_fragment_i]->get_enclosing_coords<T>(
               popped_tile_i,          // Tile
               top_range,              // Target coords
               popped_range,           // Start coords
               &popped_range[dim_num], // End coords
               &left_range[dim_num],   // Left coords
               popped_range,           // Right coords
               left_retrieved,         // Left retrieved 
               right_retrieved,        // Right retrieved 
               target_exists)          // Target exists
            != TILEDB_RS_OK) {  
          free(left.second);
          free(popped.second);
          rc = TILEDB_ARS_ERR;
          break;
        }

        // Insert left range to the result
        if(left_retrieved) 
          fragment_cell_ranges.push_back(left);
        else 
          free(left.second);

        // Re-insert right range to the priority queue
        if(right_retrieved)
          pq.push(popped);
        else
          free(popped.second);
        
        // Re-Insert unary range into the priority queue
        if(target_exists) {
          FragmentCellRange unary;
          unary.first.first = popped_fragment_i;
          unary.first.second = popped_tile_i;
          unary.second = malloc(2*coords_size);
          T* unary_range = static_cast<T*>(unary.second);
          memcpy(unary_range, top_range, coords_size); 
          memcpy(&unary_range[dim_num], top_range, coords_size); 
          pq.push(unary);
        }
      }
    }
  }

  // Clean up 
  if(tile_domain != NULL)
    delete [] tile_domain;

  // Clean up in case of error
  if(rc != TILEDB_ARS_OK) {
    while(!pq.empty()) {
      free(pq.top().second);
      pq.pop();
    }
    for(int64_t i=0; i<int64_t(fragment_cell_ranges.size()); ++i)
      free(fragment_cell_ranges[i].second);
    fragment_cell_ranges.clear();
  } else {
    assert(pq.empty()); // Sanity check
  }

  // Return
  return rc;
}




template<class T>
ArrayReadState::SmallerFragmentCellRange<T>::SmallerFragmentCellRange()
    : array_schema_(NULL) { 
}

template<class T>
ArrayReadState::SmallerFragmentCellRange<T>::SmallerFragmentCellRange(
    const ArraySchema* array_schema)
    : array_schema_(array_schema) { 
}

template<class T>
bool ArrayReadState::SmallerFragmentCellRange<T>::operator () (
    FragmentCellRange a, 
    FragmentCellRange b) const {
  // Sanity check
  assert(array_schema_ != NULL);

  // Get cell ordering information for the first range endpoints
  int cmp = array_schema_->cell_order_cmp<T>(
      static_cast<const T*>(a.second), 
      static_cast<const T*>(b.second)); 

  if(cmp < 0) {        // a's range start precedes b's
    return false;
  } else if(cmp > 0) { // b's range start preceded a's
    return true;
  } else {             // a's and b's range starts match - latest fragment wins
    if(a.first.first < b.first.first)
      return true;
    else if(a.first.first > b.first.first)
      return false;
    else
      return (a.first.second > b.first.second); 
  }
}

// Explicit template instantiations
template class ArrayReadState::SmallerFragmentCellRange<int>;
template class ArrayReadState::SmallerFragmentCellRange<int64_t>;
template class ArrayReadState::SmallerFragmentCellRange<float>;
template class ArrayReadState::SmallerFragmentCellRange<double>;

