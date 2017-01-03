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

#ifdef VERBOSE
#  define PRINT_ERROR(x) std::cerr << TILEDB_ARS_ERRMSG << x << ".\n" 
#else
#  define PRINT_ERROR(x) do { } while(0) 
#endif




/* ****************************** */
/*        GLOBAL VARIABLES        */
/* ****************************** */

std::string tiledb_ars_errmsg = "";




/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

ArrayReadState::ArrayReadState(
    const Array* array)
    : array_(array) {
  // For easy reference
  array_schema_ = array_->array_schema();
  attribute_num_ = array_schema_->attribute_num();
  coords_size_ = array_schema_->coords_size();

  // Initializations
  done_ = false;
  empty_cells_written_.resize(attribute_num_+1);
  fragment_cell_pos_ranges_vec_pos_.resize(attribute_num_+1);
  min_bounding_coords_end_ = NULL;
  read_round_done_.resize(attribute_num_);
  subarray_tile_coords_ = NULL;
  subarray_tile_domain_ = NULL;

  for(int i=0; i<attribute_num_+1; ++i) {
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

  int64_t fragment_cell_pos_ranges_vec_size = 
      fragment_cell_pos_ranges_vec_.size();
  for(int64_t i=0; i<fragment_cell_pos_ranges_vec_size; ++i)
    delete fragment_cell_pos_ranges_vec_[i];
}




/* ****************************** */
/*           ACCESSORS            */
/* ****************************** */

bool ArrayReadState::overflow() const {
  int attribute_num = (int) array_->attribute_ids().size();
  for(int i=0; i<attribute_num; ++i)
    if(overflow_[i])
      return true;

  return false;
}

bool ArrayReadState::overflow(int attribute_id) const {
  return overflow_[attribute_id];
}

int ArrayReadState::read(
    void** buffers, 
    size_t* buffer_sizes) {
  // Sanity check
  assert(fragment_num_);

  // Reset overflow
  overflow_.resize(attribute_num_+1); 
  for(int i=0; i<attribute_num_+1; ++i)
    overflow_[i] = false;
 
  for(int i=0; i<fragment_num_; ++i)
    fragment_read_states_[i]->reset_overflow();

  if(array_schema_->dense())  // DENSE
    return read_dense(buffers, buffer_sizes);
  else                       // SPARSE
    return read_sparse(buffers, buffer_sizes);
}




/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

void ArrayReadState::clean_up_processed_fragment_cell_pos_ranges() {
  // Find the minimum overlapping tile position across all attributes
  const std::vector<int>& attribute_ids = array_->attribute_ids();
  int attribute_id_num = attribute_ids.size(); 
  int64_t min_pos = fragment_cell_pos_ranges_vec_pos_[0];
  for(int i=1; i<attribute_id_num; ++i) 
    if(fragment_cell_pos_ranges_vec_pos_[attribute_ids[i]] < min_pos) 
      min_pos = fragment_cell_pos_ranges_vec_pos_[attribute_ids[i]];

  // Clean up processed overlapping tiles
  if(min_pos != 0) {
    // Remove overlapping tile elements from the vector
    for(int64_t i=0; i<min_pos; ++i)
      delete fragment_cell_pos_ranges_vec_[i];
    FragmentCellPosRangesVec::iterator it_first = 
         fragment_cell_pos_ranges_vec_.begin(); 
    FragmentCellPosRangesVec::iterator it_last = it_first + min_pos; 
    fragment_cell_pos_ranges_vec_.erase(it_first, it_last); 

    // Update the positions
    for(int i=0; i<attribute_num_+1; ++i)
      if(fragment_cell_pos_ranges_vec_pos_[i] != 0)
        fragment_cell_pos_ranges_vec_pos_[i] -= min_pos;
  }
}

template<class T>
int ArrayReadState::compute_fragment_cell_pos_ranges(
    FragmentCellRanges& fragment_cell_ranges,
    FragmentCellPosRanges& fragment_cell_pos_ranges) const {
  // For easy reference
  int dim_num = array_schema_->dim_num();
  int fragment_id;
  int64_t fragment_cell_ranges_num = fragment_cell_ranges.size();

  // Compute fragment cell position ranges
  for(int64_t i=0; i<fragment_cell_ranges_num; ++i) { 
    fragment_id = fragment_cell_ranges[i].first.first;
    if(fragment_id == -1 ||
       fragment_read_states_[fragment_id]->dense()) {  // DENSE
      // Create a new fragment cell position range
      FragmentCellPosRange fragment_cell_pos_range;
      fragment_cell_pos_range.first = fragment_cell_ranges[i].first;
      CellPosRange& cell_pos_range = fragment_cell_pos_range.second;
      T* cell_range = static_cast<T*>(fragment_cell_ranges[i].second);
      cell_pos_range.first = array_schema_->get_cell_pos(cell_range);
      cell_pos_range.second = array_schema_->get_cell_pos(&cell_range[dim_num]);

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
        // Error
        for(int j=i; j<fragment_cell_ranges_num; ++j) 
          free(fragment_cell_ranges[j].second);
        fragment_cell_ranges.clear();
        fragment_cell_pos_ranges.clear();
        tiledb_ars_errmsg = tiledb_rs_errmsg;
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
  int dim_num = array_schema_->dim_num();

  // Allocate memeory
  if(min_bounding_coords_end_ == NULL)
    min_bounding_coords_end_ = malloc(coords_size_);
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
            coords_size_);
        first = false;
      } else if(array_schema_->tile_cell_order_cmp(  
                    &fragment_bounding_coords[dim_num],
                    min_bounding_coords_end) < 0) {
        memcpy(
            min_bounding_coords_end, 
            &fragment_bounding_coords[dim_num], 
            coords_size_);
      }
    }
  }
}

template<class T>
int ArrayReadState::compute_unsorted_fragment_cell_ranges_dense(
    std::vector<FragmentCellRanges>& unsorted_fragment_cell_ranges) {
  // Compute cell ranges for all fragments
  for(int i=0; i<fragment_num_; ++i) {
    if(!fragment_read_states_[i]->done()) {
      if(fragment_read_states_[i]->dense()) {     // DENSE
        // Get fragment cell ranges
        FragmentCellRanges fragment_cell_ranges;
        if(fragment_read_states_[i]->get_fragment_cell_ranges_dense<T>(
            i,
            fragment_cell_ranges) != TILEDB_RS_OK) {
          tiledb_ars_errmsg = tiledb_rs_errmsg;
          return TILEDB_ARS_ERR;
        }
        // Insert fragment cell ranges to the result
        unsorted_fragment_cell_ranges.push_back(fragment_cell_ranges);
      } else {                                    // SPARSE
        FragmentCellRanges fragment_cell_ranges;
        FragmentCellRanges fragment_cell_ranges_tmp;
        do {
          // Get next overlapping tiles
          fragment_read_states_[i]->get_next_overlapping_tile_sparse<T>(
              static_cast<const T*>(subarray_tile_coords_));
          // Get fragment cell ranges
          fragment_cell_ranges_tmp.clear();
          if(fragment_read_states_[i]->get_fragment_cell_ranges_sparse<T>(
             i,
             fragment_cell_ranges_tmp) != TILEDB_RS_OK) {
            tiledb_ars_errmsg = tiledb_rs_errmsg;
            return TILEDB_ARS_ERR;
          }
          // Insert fragment cell ranges to temporary ranges
          fragment_cell_ranges.insert(
              fragment_cell_ranges.end(),
              fragment_cell_ranges_tmp.begin(),
              fragment_cell_ranges_tmp.end());
        } while(!fragment_read_states_[i]->done() &&
                fragment_read_states_[i]->mbr_overlaps_tile()); 
        unsorted_fragment_cell_ranges.push_back(fragment_cell_ranges);
      }
    } else {
      // Append an empty list
      unsorted_fragment_cell_ranges.push_back(FragmentCellRanges());
    }
  }

  // Check if some dense fragment completely covers the subarray
  bool subarray_area_covered = false;
  for(int i=0; i<fragment_num_; ++i) {
    if(!fragment_read_states_[i]->done() &&
       fragment_read_states_[i]->dense() && 
       fragment_read_states_[i]->subarray_area_covered()) {
      subarray_area_covered = true;
      break;
    }
  }

  // Add a fragment that accounts for the empty areas of the array
  if(!subarray_area_covered) 
    unsorted_fragment_cell_ranges.push_back(empty_fragment_cell_ranges<T>());

  // Success
  return TILEDB_ARS_OK;
}

template<class T>
int ArrayReadState::compute_unsorted_fragment_cell_ranges_sparse(
    std::vector<FragmentCellRanges>& unsorted_fragment_cell_ranges) {
  // For easy reference
  int dim_num = array_schema_->dim_num();
  T* min_bounding_coords_end = static_cast<T*>(min_bounding_coords_end_);

  // Compute the relevant fragment cell ranges
  for(int i=0; i<fragment_num_; ++i) {
    T* fragment_bounding_coords = static_cast<T*>(fragment_bounding_coords_[i]);

    // Compute new fragment cell ranges
    if(fragment_bounding_coords != NULL &&
       array_schema_->tile_cell_order_cmp(
             fragment_bounding_coords,
             min_bounding_coords_end) <= 0) {
      FragmentCellRanges fragment_cell_ranges;
      if(fragment_read_states_[i]->get_fragment_cell_ranges_sparse<T>(
          i,
          fragment_bounding_coords,
          min_bounding_coords_end,
          fragment_cell_ranges) != TILEDB_RS_OK) {
        tiledb_ars_errmsg = tiledb_rs_errmsg;
        return TILEDB_ARS_ERR;
      }

      unsorted_fragment_cell_ranges.push_back(fragment_cell_ranges);

      // If the end bounding coordinate is not the same as the smallest one, 
      // update the start bounding coordinate to exceed the smallest
      // end bounding coordinates
      if(memcmp(
             &fragment_bounding_coords[dim_num], 
             min_bounding_coords_end, 
             coords_size_)) {
        // Get the first coordinates AFTER the min bounding coords end 
        bool coords_retrieved;
        if(fragment_read_states_[i]->get_coords_after<T>(
               min_bounding_coords_end, 
               fragment_bounding_coords,
               coords_retrieved) != TILEDB_RS_OK) {  
          tiledb_ars_errmsg = tiledb_rs_errmsg;
          return TILEDB_ARS_ERR;
        }

        // Sanity check for the sparse case
        assert(coords_retrieved);
      } 
    } else {
      // Append an empty list
      unsorted_fragment_cell_ranges.push_back(FragmentCellRanges());
    }
  }

  // Success
  return TILEDB_ARS_OK;
}

int ArrayReadState::copy_cells(
    int attribute_id,
    void* buffer,  
    size_t buffer_size,
    size_t& buffer_offset) {
  // For easy reference
  int type = array_schema_->type(attribute_id);

  // Invoke the proper templated function
  int rc;
  if(type == TILEDB_INT32)
    rc = copy_cells<int>(
             attribute_id, 
             buffer, 
             buffer_size, 
             buffer_offset);
  else if(type == TILEDB_INT64)
    rc = copy_cells<int64_t>(
             attribute_id, 
             buffer, 
             buffer_size, 
             buffer_offset);
  else if(type == TILEDB_FLOAT32)
    rc = copy_cells<float>(
             attribute_id, 
             buffer, 
             buffer_size, 
             buffer_offset);
  else if(type == TILEDB_FLOAT64)
    rc = copy_cells<double>(
             attribute_id, 
             buffer, 
             buffer_size, 
             buffer_offset);
  else if(type == TILEDB_CHAR)
    rc = copy_cells<char>(
             attribute_id, 
             buffer, 
             buffer_size, 
             buffer_offset);
  else 
    rc = TILEDB_ARS_ERR;

  // Handle error
  if(rc != TILEDB_ARS_OK)
    return TILEDB_ARS_ERR;

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
  int64_t pos = fragment_cell_pos_ranges_vec_pos_[attribute_id];
  FragmentCellPosRanges& fragment_cell_pos_ranges = 
      *fragment_cell_pos_ranges_vec_[pos];
  int64_t fragment_cell_pos_ranges_num = fragment_cell_pos_ranges.size();
  int fragment_id; // Fragment id
  int64_t tile_pos; // Tile position in the fragment

  // Sanity check
  assert(!array_schema_->var_size(attribute_id));

  // Copy the cell ranges one by one
  for(int64_t i=0; i<fragment_cell_pos_ranges_num; ++i) {
    fragment_id = fragment_cell_pos_ranges[i].first.first; 
    tile_pos = fragment_cell_pos_ranges[i].first.second; 
    CellPosRange& cell_pos_range = fragment_cell_pos_ranges[i].second; 

    // Handle empty fragment
    if(fragment_id == -1) {
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
    if(fragment_read_states_[fragment_id]->copy_cells(
           attribute_id,
           tile_pos,
           buffer,
           buffer_size,
           buffer_offset,
           cell_pos_range) != TILEDB_RS_OK) {
       tiledb_ars_errmsg = tiledb_rs_errmsg;
       return TILEDB_ARS_ERR;
     }

     // Handle overflow
     if(fragment_read_states_[fragment_id]->overflow(attribute_id)) {
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

int ArrayReadState::copy_cells_var(
    int attribute_id,
    void* buffer,  
    size_t buffer_size,
    size_t& buffer_offset,
    void* buffer_var,  
    size_t buffer_var_size,
    size_t& buffer_var_offset) {
  // For easy reference
  int type = array_schema_->type(attribute_id);

  // Invoke the proper templated function
  int rc;
  if(type == TILEDB_INT32)
    rc = copy_cells_var<int>(
             attribute_id,
             buffer, 
             buffer_size, 
             buffer_offset,
             buffer_var, 
             buffer_var_size,
             buffer_var_offset);
  else if(type == TILEDB_INT64)
    rc = copy_cells_var<int64_t>(
             attribute_id,
             buffer, 
             buffer_size, 
             buffer_offset,
             buffer_var, 
             buffer_var_size,
             buffer_var_offset);
  else if(type == TILEDB_FLOAT32)
    rc = copy_cells_var<float>(
             attribute_id,
             buffer, 
             buffer_size, 
             buffer_offset,
             buffer_var, 
             buffer_var_size,
             buffer_var_offset);
  else if(type == TILEDB_FLOAT64)
    rc = copy_cells_var<double>(
             attribute_id,
             buffer, 
             buffer_size, 
             buffer_offset,
             buffer_var, 
             buffer_var_size,
             buffer_var_offset);
  else if(type == TILEDB_CHAR)
    rc = copy_cells_var<char>(
             attribute_id,
             buffer, 
             buffer_size, 
             buffer_offset,
             buffer_var, 
             buffer_var_size,
             buffer_var_offset);
  else
    rc = TILEDB_ARS_ERR;

  // Handle error
  if(rc != TILEDB_ARS_OK)
    return TILEDB_ARS_ERR;

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
  int64_t pos = fragment_cell_pos_ranges_vec_pos_[attribute_id];
  FragmentCellPosRanges& fragment_cell_pos_ranges = 
      *fragment_cell_pos_ranges_vec_[pos];
  int64_t fragment_cell_pos_ranges_num = fragment_cell_pos_ranges.size();
  int fragment_id; // Fragment id
  int64_t tile_pos; // Tile position in the fragment

  // Sanity check
  assert(array_schema_->var_size(attribute_id));

  // Copy the cell ranges one by one
  for(int64_t i=0; i<fragment_cell_pos_ranges_num; ++i) {
    tile_pos = fragment_cell_pos_ranges[i].first.second; 
    fragment_id = fragment_cell_pos_ranges[i].first.first; 
    CellPosRange& cell_pos_range = fragment_cell_pos_ranges[i].second; 

    // Handle empty fragment
    if(fragment_id == -1) {
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
    if(fragment_read_states_[fragment_id]->copy_cells_var(
           attribute_id,
           tile_pos,
           buffer,
           buffer_size,
           buffer_offset,
           buffer_var,
           buffer_var_size,
           buffer_var_offset,
           cell_pos_range) != TILEDB_RS_OK) {
       tiledb_ars_errmsg = tiledb_rs_errmsg;
       return TILEDB_ARS_ERR;
     }

     // Handle overflow
     if(fragment_read_states_[fragment_id]->overflow(attribute_id)) {
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

template<>
void ArrayReadState::copy_cells_with_empty<int>(
    int attribute_id,
    void* buffer,  
    size_t buffer_size,
    size_t& buffer_offset,
    const CellPosRange& cell_pos_range) {

  // For easy reference
  size_t cell_size = array_schema_->cell_size(attribute_id);
  char* buffer_c = static_cast<char*>(buffer);
  int cell_val_num = array_schema_->cell_val_num(attribute_id);

  // Calculate free space in buffer
  size_t buffer_free_space = buffer_size - buffer_offset;
  buffer_free_space = (buffer_free_space / cell_size) * cell_size;
  if(buffer_free_space == 0) { // Overflow
    overflow_[attribute_id] = true;
    return;
  }

  // Sanity check
  assert(!array_schema_->var_size(attribute_id));

  // Calculate number of empty cells to write
  int64_t cell_num_in_range = cell_pos_range.second - cell_pos_range.first + 1; 
  int64_t cell_num_left_to_copy = 
      cell_num_in_range - empty_cells_written_[attribute_id]; 
  size_t bytes_left_to_copy = cell_num_left_to_copy * cell_size;
  size_t bytes_to_copy = std::min(bytes_left_to_copy, buffer_free_space); 
  int64_t cell_num_to_copy = bytes_to_copy / cell_size; 

  // Copy empty cells to buffer
  int empty = TILEDB_EMPTY_INT32;
  for(int64_t i=0; i<cell_num_to_copy; ++i) {
    for(int j=0; j<cell_val_num; ++j) {
      memcpy(buffer_c + buffer_offset, &empty, sizeof(int));
      buffer_offset += sizeof(int);
    }
  } 
  empty_cells_written_[attribute_id] += cell_num_to_copy;

  // Handle buffer overflow
  if(empty_cells_written_[attribute_id] != cell_num_in_range) {
    overflow_[attribute_id] = true;
  } else { // Done copying this range
    empty_cells_written_[attribute_id] = 0;
  }
}

template<>
void ArrayReadState::copy_cells_with_empty<int64_t>(
    int attribute_id,
    void* buffer,  
    size_t buffer_size,
    size_t& buffer_offset,
    const CellPosRange& cell_pos_range) {
  // For easy reference
  size_t cell_size = array_schema_->cell_size(attribute_id);
  char* buffer_c = static_cast<char*>(buffer);
  int cell_val_num = array_schema_->cell_val_num(attribute_id);

  // Calculate free space in buffer
  size_t buffer_free_space = buffer_size - buffer_offset;
  buffer_free_space = (buffer_free_space / cell_size) * cell_size;
  if(buffer_free_space == 0) { // Overflow
    overflow_[attribute_id] = true;
    return;
  }

  // Sanity check
  assert(!array_schema_->var_size(attribute_id));

  // Calculate number of empty cells to write
  int64_t cell_num_in_range = cell_pos_range.second - cell_pos_range.first + 1; 
  int64_t cell_num_left_to_copy = 
      cell_num_in_range - empty_cells_written_[attribute_id]; 
  size_t bytes_left_to_copy = cell_num_left_to_copy * cell_size;
  size_t bytes_to_copy = std::min(bytes_left_to_copy, buffer_free_space); 
  int64_t cell_num_to_copy = bytes_to_copy / cell_size; 

  // Copy empty cells to buffer
  int64_t empty = TILEDB_EMPTY_INT64;
  for(int64_t i=0; i<cell_num_to_copy; ++i) {
    for(int j=0; j<cell_val_num; ++j) {
      memcpy(buffer_c + buffer_offset, &empty, sizeof(int64_t));
      buffer_offset += sizeof(int64_t);
    }
  } 
  empty_cells_written_[attribute_id] += cell_num_to_copy;

  // Handle buffer overflow
  if(empty_cells_written_[attribute_id] != cell_num_in_range) { 
    overflow_[attribute_id] = true;
  } else { // Done copying this range
    empty_cells_written_[attribute_id] = 0;
  }
}

template<>
void ArrayReadState::copy_cells_with_empty<float>(
    int attribute_id,
    void* buffer,  
    size_t buffer_size,
    size_t& buffer_offset,
    const CellPosRange& cell_pos_range) {
  // For easy reference
  size_t cell_size = array_schema_->cell_size(attribute_id);
  char* buffer_c = static_cast<char*>(buffer);
  int cell_val_num = array_schema_->cell_val_num(attribute_id);

  // Calculate free space in buffer
  size_t buffer_free_space = buffer_size - buffer_offset;
  buffer_free_space = (buffer_free_space / cell_size) * cell_size;
  if(buffer_free_space == 0) { // Overflow
    overflow_[attribute_id] = true;
    return;
  }

  // Sanity check
  assert(!array_schema_->var_size(attribute_id));

  // Calculate number of empty cells to write
  int64_t cell_num_in_range = cell_pos_range.second - cell_pos_range.first + 1; 
  int64_t cell_num_left_to_copy = 
      cell_num_in_range - empty_cells_written_[attribute_id]; 
  size_t bytes_left_to_copy = cell_num_left_to_copy * cell_size;
  size_t bytes_to_copy = std::min(bytes_left_to_copy, buffer_free_space); 
  int64_t cell_num_to_copy = bytes_to_copy / cell_size; 

  // Copy empty cells to buffer
  float empty = TILEDB_EMPTY_FLOAT32;
  for(int64_t i=0; i<cell_num_to_copy; ++i) {
    for(int j=0; j<cell_val_num; ++j) {
      memcpy(buffer_c + buffer_offset, &empty, sizeof(float));
      buffer_offset += sizeof(float);
    }
  } 
  empty_cells_written_[attribute_id] += cell_num_to_copy;

  // Handle buffer overflow
  if(empty_cells_written_[attribute_id] != cell_num_in_range) { 
    overflow_[attribute_id] = true;
  } else { // Done copying this range
    empty_cells_written_[attribute_id] = 0;
  }
}

template<>
void ArrayReadState::copy_cells_with_empty<double>(
    int attribute_id,
    void* buffer,  
    size_t buffer_size,
    size_t& buffer_offset,
    const CellPosRange& cell_pos_range) {
  // For easy reference
  size_t cell_size = array_schema_->cell_size(attribute_id);
  char* buffer_c = static_cast<char*>(buffer);
  int cell_val_num = array_schema_->cell_val_num(attribute_id);

  // Calculate free space in buffer
  size_t buffer_free_space = buffer_size - buffer_offset;
  buffer_free_space = (buffer_free_space / cell_size) * cell_size;
  if(buffer_free_space == 0) { // Overflow
    overflow_[attribute_id] = true;
    return;
  }

  // Sanity check
  assert(!array_schema_->var_size(attribute_id));

  // Calculate number of empty cells to write
  int64_t cell_num_in_range = cell_pos_range.second - cell_pos_range.first + 1; 
  int64_t cell_num_left_to_copy = 
      cell_num_in_range - empty_cells_written_[attribute_id]; 
  size_t bytes_left_to_copy = cell_num_left_to_copy * cell_size;
  size_t bytes_to_copy = std::min(bytes_left_to_copy, buffer_free_space); 
  int64_t cell_num_to_copy = bytes_to_copy / cell_size; 

  // Copy empty cells to buffer
  double empty = TILEDB_EMPTY_FLOAT64;
  for(int64_t i=0; i<cell_num_to_copy; ++i) {
    for(int j=0; j<cell_val_num; ++j) {
      memcpy(buffer_c + buffer_offset, &empty, sizeof(double));
      buffer_offset += sizeof(double);
    }
  } 
  empty_cells_written_[attribute_id] += cell_num_to_copy;

  // Handle buffer overflow
  if(empty_cells_written_[attribute_id] != cell_num_in_range) { 
    overflow_[attribute_id] = true;
  } else { // Done copying this range
    empty_cells_written_[attribute_id] = 0;
  }
}

template<>
void ArrayReadState::copy_cells_with_empty<char>(
    int attribute_id,
    void* buffer,  
    size_t buffer_size,
    size_t& buffer_offset,
    const CellPosRange& cell_pos_range) {
  // For easy reference
  size_t cell_size = array_schema_->cell_size(attribute_id);
  char* buffer_c = static_cast<char*>(buffer);
  int cell_val_num = array_schema_->cell_val_num(attribute_id);

  // Calculate free space in buffer
  size_t buffer_free_space = buffer_size - buffer_offset;
  buffer_free_space = (buffer_free_space / cell_size) * cell_size;
  if(buffer_free_space == 0) { // Overflow
    overflow_[attribute_id] = true;
    return;
  }

  // Sanity check
  assert(!array_schema_->var_size(attribute_id));

  // Calculate number of empty cells to write
  int64_t cell_num_in_range = cell_pos_range.second - cell_pos_range.first + 1; 
  int64_t cell_num_left_to_copy = 
      cell_num_in_range - empty_cells_written_[attribute_id]; 
  size_t bytes_left_to_copy = cell_num_left_to_copy * cell_size;
  size_t bytes_to_copy = std::min(bytes_left_to_copy, buffer_free_space); 
  int64_t cell_num_to_copy = bytes_to_copy / cell_size; 

  // Copy empty cells to buffer
  char empty = TILEDB_EMPTY_CHAR;
  for(int64_t i=0; i<cell_num_to_copy; ++i) {
    for(int j=0; j<cell_val_num; ++j) {
      memcpy(buffer_c + buffer_offset, &empty, sizeof(char));
      buffer_offset += sizeof(char);
    }
  } 
  empty_cells_written_[attribute_id] += cell_num_to_copy;

  // Handle buffer overflow
  if(empty_cells_written_[attribute_id] != cell_num_in_range) {
    overflow_[attribute_id] = true;
  } else { // Done copying this range
    empty_cells_written_[attribute_id] = 0;
  }
}

template<>
void ArrayReadState::copy_cells_with_empty_var<int>(
    int attribute_id,
    void* buffer,  
    size_t buffer_size,
    size_t& buffer_offset,
    void* buffer_var,  
    size_t buffer_var_size,
    size_t& buffer_var_offset,
    const CellPosRange& cell_pos_range) {
  // For easy reference
  size_t cell_size = TILEDB_CELL_VAR_OFFSET_SIZE;
  size_t cell_size_var = sizeof(int);
  char* buffer_c = static_cast<char*>(buffer);
  char* buffer_var_c = static_cast<char*>(buffer_var);

  // Calculate free space in buffer
  size_t buffer_free_space = buffer_size - buffer_offset;
  buffer_free_space = (buffer_free_space / cell_size) * cell_size;
  size_t buffer_var_free_space = buffer_var_size - buffer_var_offset;
  buffer_var_free_space = (buffer_var_free_space/cell_size_var)*cell_size_var;

  // Handle overflow
  if(buffer_free_space == 0 || buffer_var_free_space == 0) { // Overflow
    overflow_[attribute_id] = true; 
    return;
  }

  // Sanity check
  assert(array_schema_->var_size(attribute_id));

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
  int empty = TILEDB_EMPTY_INT32;
  for(int64_t i=0; i<cell_num_to_copy; ++i) {
    memcpy(
        buffer_c + buffer_offset, 
        &buffer_var_offset, 
        cell_size);
    buffer_offset += cell_size;
    memcpy(
        buffer_var_c + buffer_var_offset, 
        &empty,
        cell_size_var);
    buffer_var_offset += cell_size_var;
  }
  empty_cells_written_[attribute_id] += cell_num_to_copy;

  // Handle buffer overflow
  if(empty_cells_written_[attribute_id] != cell_num_in_range) {
    overflow_[attribute_id] = true;
  } else { // Done copying this range
    empty_cells_written_[attribute_id] = 0;
  }
}

template<>
void ArrayReadState::copy_cells_with_empty_var<int64_t>(
    int attribute_id,
    void* buffer,  
    size_t buffer_size,
    size_t& buffer_offset,
    void* buffer_var,  
    size_t buffer_var_size,
    size_t& buffer_var_offset,
    const CellPosRange& cell_pos_range) {
  // For easy reference
  size_t cell_size = TILEDB_CELL_VAR_OFFSET_SIZE;
  size_t cell_size_var = sizeof(int64_t);
  char* buffer_c = static_cast<char*>(buffer);
  char* buffer_var_c = static_cast<char*>(buffer_var);

  // Calculate free space in buffer
  size_t buffer_free_space = buffer_size - buffer_offset;
  buffer_free_space = (buffer_free_space / cell_size) * cell_size;
  size_t buffer_var_free_space = buffer_var_size - buffer_var_offset;
  buffer_var_free_space = (buffer_var_free_space/cell_size_var)*cell_size_var;

  // Handle overflow
  if(buffer_free_space == 0 || buffer_var_free_space == 0) { // Overflow
    overflow_[attribute_id] = true; 
    return;
  }

  // Sanity check
  assert(array_schema_->var_size(attribute_id));

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
  int64_t empty = TILEDB_EMPTY_INT64;
  for(int64_t i=0; i<cell_num_to_copy; ++i) {
    memcpy(
        buffer_c + buffer_offset, 
        &buffer_var_offset, 
        cell_size);
    buffer_offset += cell_size;
    memcpy(
        buffer_var_c + buffer_var_offset, 
        &empty,
        cell_size_var);
    buffer_var_offset += cell_size_var;
  }
  empty_cells_written_[attribute_id] += cell_num_to_copy;

  // Handle buffer overflow
  if(empty_cells_written_[attribute_id] != cell_num_in_range) { 
    overflow_[attribute_id] = true;
  } else { // Done copying this range
    empty_cells_written_[attribute_id] = 0;
  }
}

template<>
void ArrayReadState::copy_cells_with_empty_var<float>(
    int attribute_id,
    void* buffer,  
    size_t buffer_size,
    size_t& buffer_offset,
    void* buffer_var,  
    size_t buffer_var_size,
    size_t& buffer_var_offset,
    const CellPosRange& cell_pos_range) {
  // For easy reference
  size_t cell_size = TILEDB_CELL_VAR_OFFSET_SIZE;
  size_t cell_size_var = sizeof(float);
  char* buffer_c = static_cast<char*>(buffer);
  char* buffer_var_c = static_cast<char*>(buffer_var);

  // Calculate free space in buffer
  size_t buffer_free_space = buffer_size - buffer_offset;
  buffer_free_space = (buffer_free_space / cell_size) * cell_size;
  size_t buffer_var_free_space = buffer_var_size - buffer_var_offset;
  buffer_var_free_space = (buffer_var_free_space/cell_size_var)*cell_size_var;

  // Handle overflow
  if(buffer_free_space == 0 || buffer_var_free_space == 0) { // Overflow
    overflow_[attribute_id] = true; 
    return;
  }

  // Sanity check
  assert(array_schema_->var_size(attribute_id));

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
  float empty = TILEDB_EMPTY_FLOAT32;
  for(int64_t i=0; i<cell_num_to_copy; ++i) {
    memcpy(
        buffer_c + buffer_offset, 
        &buffer_var_offset, 
        cell_size);
    buffer_offset += cell_size;
    memcpy(
        buffer_var_c + buffer_var_offset, 
        &empty,
        cell_size_var);
    buffer_var_offset += cell_size_var;
  }
  empty_cells_written_[attribute_id] += cell_num_to_copy;

  // Handle buffer overflow
  if(empty_cells_written_[attribute_id] != cell_num_in_range) 
    overflow_[attribute_id] = true;
  else // Done copying this range
    empty_cells_written_[attribute_id] = 0;
}

template<>
void ArrayReadState::copy_cells_with_empty_var<double>(
    int attribute_id,
    void* buffer,  
    size_t buffer_size,
    size_t& buffer_offset,
    void* buffer_var,  
    size_t buffer_var_size,
    size_t& buffer_var_offset,
    const CellPosRange& cell_pos_range) {
  // For easy reference
  size_t cell_size = TILEDB_CELL_VAR_OFFSET_SIZE;
  size_t cell_size_var = sizeof(double);
  char* buffer_c = static_cast<char*>(buffer);
  char* buffer_var_c = static_cast<char*>(buffer_var);

  // Calculate free space in buffer
  size_t buffer_free_space = buffer_size - buffer_offset;
  buffer_free_space = (buffer_free_space / cell_size) * cell_size;
  size_t buffer_var_free_space = buffer_var_size - buffer_var_offset;
  buffer_var_free_space = (buffer_var_free_space/cell_size_var)*cell_size_var;

  // Handle overflow
  if(buffer_free_space == 0 || buffer_var_free_space == 0) { // Overflow
    overflow_[attribute_id] = true; 
    return;
  }

  // Sanity check
  assert(array_schema_->var_size(attribute_id));

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
  double empty = TILEDB_EMPTY_FLOAT64;
  for(int64_t i=0; i<cell_num_to_copy; ++i) {
    memcpy(
        buffer_c + buffer_offset, 
        &buffer_var_offset, 
        cell_size);
    buffer_offset += cell_size;
    memcpy(
        buffer_var_c + buffer_var_offset, 
        &empty,
        cell_size_var);
    buffer_var_offset += cell_size_var;
  }
  empty_cells_written_[attribute_id] += cell_num_to_copy;

  // Handle buffer overflow
  if(empty_cells_written_[attribute_id] != cell_num_in_range) 
    overflow_[attribute_id] = true;
  else // Done copying this range
    empty_cells_written_[attribute_id] = 0;
}

template<>
void ArrayReadState::copy_cells_with_empty_var<char>(
    int attribute_id,
    void* buffer,  
    size_t buffer_size,
    size_t& buffer_offset,
    void* buffer_var,  
    size_t buffer_var_size,
    size_t& buffer_var_offset,
    const CellPosRange& cell_pos_range) {
  // For easy reference
  size_t cell_size = TILEDB_CELL_VAR_OFFSET_SIZE;
  size_t cell_size_var = sizeof(char);
  char* buffer_c = static_cast<char*>(buffer);
  char* buffer_var_c = static_cast<char*>(buffer_var);

  // Calculate free space in buffer
  size_t buffer_free_space = buffer_size - buffer_offset;
  buffer_free_space = (buffer_free_space / cell_size) * cell_size;
  size_t buffer_var_free_space = buffer_var_size - buffer_var_offset;
  buffer_var_free_space = (buffer_var_free_space/cell_size_var)*cell_size_var;

  // Handle overflow
  if(buffer_free_space == 0 || buffer_var_free_space == 0) { // Overflow
    overflow_[attribute_id] = true; 
    return;
  }

  // Sanity check
  assert(array_schema_->var_size(attribute_id));

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
  char empty = TILEDB_EMPTY_CHAR;
  for(int64_t i=0; i<cell_num_to_copy; ++i) {
    memcpy(
        buffer_c + buffer_offset, 
        &buffer_var_offset, 
        cell_size);
    buffer_offset += cell_size;
    memcpy(
        buffer_var_c + buffer_var_offset, 
        &empty,
        cell_size_var);
    buffer_var_offset += cell_size_var;
  }
  empty_cells_written_[attribute_id] += cell_num_to_copy;

  // Handle buffer overflow
  if(empty_cells_written_[attribute_id] != cell_num_in_range)  
    overflow_[attribute_id] = true;
  else // Done copying this range
    empty_cells_written_[attribute_id] = 0;
}

template<class T>
ArrayReadState::FragmentCellRanges 
ArrayReadState::empty_fragment_cell_ranges() const {
  // For easy reference
  int dim_num = array_schema_->dim_num();
  int cell_order = array_schema_->cell_order();
  size_t cell_range_size = 2*coords_size_;
  const T* subarray = static_cast<const T*>(array_->subarray());
  const T* tile_coords = (const T*) subarray_tile_coords_;

  // To return
  FragmentInfo fragment_info = FragmentInfo(-1, -1);
  FragmentCellRanges fragment_cell_ranges;

  // Compute the tile subarray
  T* tile_subarray = new T[2*dim_num];
  array_schema_->get_tile_subarray(tile_coords, tile_subarray); 

  // Compute overlap of tile subarray with non-empty fragment domain 
  T* query_tile_overlap_subarray = new T[2*dim_num]; 
  int overlap = array_schema_->subarray_overlap(
                    subarray,
                    tile_subarray, 
                    query_tile_overlap_subarray);

  // Contiguous cells, single cell range
  if(overlap == 1 || overlap == 3) {
    void* cell_range = malloc(cell_range_size);
    T* cell_range_T = static_cast<T*>(cell_range);
    for(int i=0; i<dim_num; ++i) {
      cell_range_T[i] = query_tile_overlap_subarray[2*i];
      cell_range_T[dim_num + i] = query_tile_overlap_subarray[2*i+1];
    }

    // Insert the new range into the result vector
    fragment_cell_ranges.push_back(
        FragmentCellRange(fragment_info, cell_range));
  } else { // Non-contiguous cells, multiple ranges
    // Initialize the coordinates at the beginning of the global range
    T* coords = new T[dim_num];
    for(int i=0; i<dim_num; ++i)
      coords[i] = query_tile_overlap_subarray[2*i];

    // Handle the different cell orders
    int i;
    if(cell_order == TILEDB_ROW_MAJOR) {           // ROW
      while(coords[0] <= query_tile_overlap_subarray[1]) {
        // Make a cell range representing a slab       
        void* cell_range = malloc(cell_range_size);
        T* cell_range_T = static_cast<T*>(cell_range);
        for(int i=0; i<dim_num-1; ++i) { 
          cell_range_T[i] = coords[i];
          cell_range_T[dim_num+i] = coords[i];
        }
        cell_range_T[dim_num-1] = query_tile_overlap_subarray[2*(dim_num-1)];
        cell_range_T[2*dim_num-1] = 
            query_tile_overlap_subarray[2*(dim_num-1)+1];

        // Insert the new range into the result vector
        fragment_cell_ranges.push_back(
            FragmentCellRange(fragment_info, cell_range));

        // Advance coordinates
        i=dim_num-2;
        ++coords[i];
        while(i > 0 && coords[i] > query_tile_overlap_subarray[2*i+1]) {
          coords[i] = query_tile_overlap_subarray[2*i];
          ++coords[--i];
        } 
      }
    } else if(cell_order == TILEDB_COL_MAJOR) { // COLUMN
      while(coords[dim_num-1] <=  
            query_tile_overlap_subarray[2*(dim_num-1)+1]) {
        // Make a cell range representing a slab       
        void* cell_range = malloc(cell_range_size);
        T* cell_range_T = static_cast<T*>(cell_range);
        for(int i=dim_num-1; i>0; --i) { 
          cell_range_T[i] = coords[i];
          cell_range_T[dim_num+i] = coords[i];
        }
        cell_range_T[0] = query_tile_overlap_subarray[0];
        cell_range_T[dim_num] = query_tile_overlap_subarray[1];

        // Insert the new range into the result vector
        fragment_cell_ranges.push_back(
            FragmentCellRange(fragment_info, cell_range));

        // Advance coordinates
        i=1;
        ++coords[i];
        while(i <dim_num-1 && coords[i] > query_tile_overlap_subarray[2*i+1]) {
          coords[i] = query_tile_overlap_subarray[2*i];
          ++coords[++i];
        } 
      }
    } else {
      assert(0);
    }
    
    // Clean up
    delete [] coords;
  }

  // Clean up
  delete [] tile_subarray;
  delete [] query_tile_overlap_subarray;

  // Return
  return fragment_cell_ranges;
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
  std::vector<FragmentCellRanges> unsorted_fragment_cell_ranges;
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
  FragmentCellPosRanges* fragment_cell_pos_ranges = new FragmentCellPosRanges();
  if(compute_fragment_cell_pos_ranges<T>(
         fragment_cell_ranges, 
         *fragment_cell_pos_ranges) != TILEDB_ARS_OK) 
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
  std::vector<FragmentCellRanges> unsorted_fragment_cell_ranges;
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
  FragmentCellPosRanges* fragment_cell_pos_ranges = new FragmentCellPosRanges();
  if(compute_fragment_cell_pos_ranges<T>(
         fragment_cell_ranges, 
         *fragment_cell_pos_ranges) != TILEDB_ARS_OK) 
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
  int dim_num = array_schema_->dim_num();

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
        coords_size_);

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
  int dim_num = array_schema_->dim_num();

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
        fragment_bounding_coords_[i] = malloc(2*coords_size_);
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
             coords_size_)) { 
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
  int dim_num = array_schema_->dim_num();
  const T* tile_extents = static_cast<const T*>(array_schema_->tile_extents());
  const T* subarray = static_cast<const T*>(array_->subarray());

  // Sanity checks
  assert(tile_extents != NULL);
  assert(subarray_tile_domain_ == NULL);

  // Allocate space for tile domain and subarray tile domain
  T* tile_domain = new T[2*dim_num];
  subarray_tile_domain_ = malloc(2*dim_num*sizeof(T));
  T* subarray_tile_domain = static_cast<T*>(subarray_tile_domain_);

  // Get subarray in tile domain
  array_schema_->get_subarray_tile_domain<T>(
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
    subarray_tile_coords_ = malloc(coords_size_);
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
  int dim_num = array_schema_->dim_num();
  T* subarray_tile_domain = static_cast<T*>(subarray_tile_domain_);
  T* subarray_tile_coords = static_cast<T*>(subarray_tile_coords_);

  // Advance subarray tile coordinates
  array_schema_->get_next_tile_coords<T>(
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
  std::vector<int> attribute_ids = array_->attribute_ids();
  int attribute_id_num = attribute_ids.size(); 

  // Read each attribute individually
  int buffer_i = 0;
  for(int i=0; i<attribute_id_num; ++i) {
    if(!array_schema_->var_size(attribute_ids[i])) { // FIXED CELLS
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
  int coords_type = array_schema_->coords_type();

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
    std::string errmsg = "Cannot read from array; Invalid coordinates type";
    PRINT_ERROR(errmsg);
    tiledb_ars_errmsg = TILEDB_ARS_ERRMSG + errmsg;
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
      if(copy_cells(
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
    if(copy_cells(
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
  int coords_type = array_schema_->coords_type();

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
    std::string errmsg = "Cannot read from array; Invalid coordinates type";
    PRINT_ERROR(errmsg);
    tiledb_ars_errmsg = TILEDB_ARS_ERRMSG + errmsg;
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
      if(copy_cells_var(
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
    if(copy_cells_var(
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
  std::vector<int> attribute_ids = array_->attribute_ids();
  int attribute_id_num = attribute_ids.size(); 

  // Find the coordinates buffer
  int coords_buffer_i = -1;
  int buffer_i = 0;
  for(int i=0; i<attribute_id_num; ++i) { 
    if(attribute_ids[i] == attribute_num_) {
      coords_buffer_i = buffer_i;
      break;
    }
    if(!array_schema_->var_size(attribute_ids[i])) // FIXED CELLS
      ++buffer_i;
    else                                          // VARIABLE-SIZED CELLS
      buffer_i +=2;
  }

  // Read coordinates attribute first
  if(coords_buffer_i != -1) {
    if(read_sparse_attr(
           attribute_num_, 
           buffers[coords_buffer_i], 
           buffer_sizes[coords_buffer_i]) != TILEDB_ARS_OK)
      return TILEDB_ARS_ERR;
  }  

  // Read each attribute individually
  buffer_i = 0;
  for(int i=0; i<attribute_id_num; ++i) {
    // Skip coordinates attribute (already read)
    if(attribute_ids[i] == attribute_num_) {
      ++buffer_i;
      continue;
    }

    if(!array_schema_->var_size(attribute_ids[i])) { // FIXED CELLS
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
  int coords_type = array_schema_->coords_type();

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
    std::string errmsg = "Cannot read from array; Invalid coordinates type"; 
    PRINT_ERROR(errmsg);
    tiledb_ars_errmsg = TILEDB_ARS_ERRMSG + errmsg;
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
      if(copy_cells(
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
    if(copy_cells(
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
  int coords_type = array_schema_->coords_type();

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
    std::string errmsg = "Cannot read from array; Invalid coordinates type";
    PRINT_ERROR(errmsg);
    tiledb_ars_errmsg = TILEDB_ARS_ERRMSG + errmsg;
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
      if(copy_cells_var(
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
    if(copy_cells_var(
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
    std::vector<FragmentCellRanges>& unsorted_fragment_cell_ranges,
    FragmentCellRanges& fragment_cell_ranges) const {
  // For easy reference
  int fragment_num = (int) unsorted_fragment_cell_ranges.size();

  // Calculate the number of non-empty unsorted fragment range lists
  int non_empty = 0;
  int first_non_empty = -1;
  for(int i=0; i<fragment_num; ++i) {
    if(unsorted_fragment_cell_ranges[i].size() != 0) {
      ++non_empty;
      if(first_non_empty == -1) 
        first_non_empty = i;
    }
  }

  // Sanity check
  assert(non_empty > 0);

  // Trivial case - single fragment
  if(fragment_num == 1) {
    fragment_cell_ranges = unsorted_fragment_cell_ranges[first_non_empty];
    unsorted_fragment_cell_ranges.clear();
    return TILEDB_ARS_OK;
  }

  // For easy reference
  int dim_num = array_schema_->dim_num();
  const T* domain = static_cast<const T*>(array_schema_->domain());
  const T* tile_extents = static_cast<const T*>(array_schema_->tile_extents());
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

  // Initialization of book-keeping for unsorted ranges
  int64_t* rlen = new int64_t[fragment_num];
  int64_t* rid = new int64_t[fragment_num];
  int fid = 0;
  for(int i=0; i<fragment_num; ++i) {
    rlen[i] = unsorted_fragment_cell_ranges[i].size();
    rid[i] = 0;
  }

  // Initializations
  PQFragmentCellRange<T>* pq_fragment_cell_range;
  PQFragmentCellRange<T>* popped;
  PQFragmentCellRange<T>* top;
  PQFragmentCellRange<T>* trimmed_top;
  PQFragmentCellRange<T>* extra_popped;
  PQFragmentCellRange<T>* left;
  PQFragmentCellRange<T>* unary;
  FragmentCellRange result;

  // Populate queue
  std::priority_queue<
      PQFragmentCellRange<T>*,
      std::vector<PQFragmentCellRange<T>* >,
      SmallerPQFragmentCellRange<T> > pq(array_schema_);

  for(int i=0; i<fragment_num; ++i) { 
    if(rlen[i] != 0) {
      pq_fragment_cell_range = new PQFragmentCellRange<T>( 
          array_schema_,
          &fragment_read_states_);
      pq_fragment_cell_range->import_from(unsorted_fragment_cell_ranges[i][0]);
      pq.push(pq_fragment_cell_range);
      ++rid[i];
    }
  }
  
  // Start processing the queue
  while(!pq.empty()) {
    // Pop the first entry and mark it as popped
    popped = pq.top();
    pq.pop();

    // Last range - insert it into the results and get the next range
    // for that fragment
    if(pq.empty()) {
      popped->export_to(result); 
      fragment_cell_ranges.push_back(result);
      fid = (popped->fragment_id_ != -1) ? 
             popped->fragment_id_ : 
             fragment_num-1;
      delete popped;

      if(rid[fid] == rlen[fid]) {
        break;
      } else {
        pq_fragment_cell_range = new PQFragmentCellRange<T>( 
                                         array_schema_,
                                         &fragment_read_states_);
        pq_fragment_cell_range->import_from(
            unsorted_fragment_cell_ranges[fid][rid[fid]]);
        pq.push(pq_fragment_cell_range);
        ++rid[fid];
        continue;
      }
    }

    // Mark the second entry (now top) as top
    top = pq.top();

    // Dinstinguish two cases
    if(popped->dense() || popped->unary()) { // DENSE OR UNARY POPPED
      // Keep on trimming ranges from the queue
      while(!pq.empty() && popped->must_trim(top)) {
        // Cut the top range and re-insert, only if there is partial overlap
        if(top->ends_after(popped)) {
          // Create the new trimmed top range
          trimmed_top = new PQFragmentCellRange<T>(
              array_schema_,
              &fragment_read_states_);
          popped->trim(top, trimmed_top, tile_domain);
      
          // Discard top
          free(top->cell_range_);
          delete top;
          pq.pop();

          if(trimmed_top->cell_range_ != NULL) { 
            // Re-insert the trimmed range in pq
            pq.push(trimmed_top);
          } else {
            // Get the next range from the top fragment
            fid = (trimmed_top->fragment_id_ != -1) ?
                   trimmed_top->fragment_id_ : 
                   fragment_num-1;
            if(rid[fid] != rlen[fid]) {
              pq_fragment_cell_range = new PQFragmentCellRange<T>( 
                  array_schema_,
                  &fragment_read_states_);
              pq_fragment_cell_range->import_from(
                  unsorted_fragment_cell_ranges[fid][rid[fid]]);
              pq.push(pq_fragment_cell_range);
              ++rid[fid];
            }
            // Clear trimmed top
            delete trimmed_top;
          }
        } else {
          // Get the next range from the top fragment
          fid = (top->fragment_id_ != -1) ?
                 top->fragment_id_ : 
                 fragment_num-1;
          if(rid[fid] != rlen[fid]) {
            pq_fragment_cell_range = new PQFragmentCellRange<T>(
                                             array_schema_,
                                             &fragment_read_states_);
            pq_fragment_cell_range->import_from(
                unsorted_fragment_cell_ranges[fid][rid[fid]]);
          }

          // Discard top
          free(top->cell_range_);
          delete top;
          pq.pop();

          if(rid[fid] != rlen[fid]) {
            pq.push(pq_fragment_cell_range);
            ++rid[fid];
          }
        }

        // Get a new top
        if(!pq.empty())
          top = pq.top();
      }

      // Potentially split the popped range
      if(!pq.empty() && popped->must_be_split(top)) {
        // Split the popped range
        extra_popped = new PQFragmentCellRange<T>( 
            array_schema_,
            &fragment_read_states_);
        popped->split(top, extra_popped, tile_domain);
        // Re-instert the extra popped range into the queue
        pq.push(extra_popped);
      } else {
        // Get the next range from popped fragment
        fid = (popped->fragment_id_ != -1) ? 
               popped->fragment_id_ :
               fragment_num-1;
        if(rid[fid] != rlen[fid]) {
          pq_fragment_cell_range = new PQFragmentCellRange<T>( 
              array_schema_,
              &fragment_read_states_);
          pq_fragment_cell_range->import_from(
              unsorted_fragment_cell_ranges[fid][rid[fid]]);
          pq.push(pq_fragment_cell_range);
          ++rid[fid];
        }
      }
     
      // Insert the final popped range into the results
      popped->export_to(result);
      fragment_cell_ranges.push_back(result);
      delete popped;
    } else {                               // SPARSE POPPED
      // If popped does not overlap with top, insert popped into results
      if(!pq.empty() && top->begins_after(popped)) {
        popped->export_to(result);
        fragment_cell_ranges.push_back(result);
        // Get the next range from the popped fragment
        fid = popped->fragment_id_;
        if(rid[fid] != rlen[fid]) {
          pq_fragment_cell_range = new PQFragmentCellRange<T>( 
              array_schema_,
              &fragment_read_states_);
          pq_fragment_cell_range->import_from(
              unsorted_fragment_cell_ranges[fid][rid[fid]]);
          pq.push(pq_fragment_cell_range);
          ++rid[fid];
        }
        delete popped;
      } else {
        // Create up to 3 more ranges (left, unary, new popped/right)
        left = new PQFragmentCellRange<T>(
                   array_schema_, 
                   &fragment_read_states_);
        unary = new PQFragmentCellRange<T>( 
                   array_schema_,
                   &fragment_read_states_);
        popped->split_to_3(top, left, unary);

        // Get the next range from the popped fragment
        if(unary->cell_range_ == NULL && popped->cell_range_ == NULL) {
          fid = popped->fragment_id_;
          if(rid[fid] != rlen[fid]) {
            pq_fragment_cell_range = new PQFragmentCellRange<T>( 
                array_schema_,
                &fragment_read_states_);
            pq_fragment_cell_range->import_from(
                unsorted_fragment_cell_ranges[fid][rid[fid]]);
            pq.push(pq_fragment_cell_range);
            ++rid[fid];
          }
        }

        // Insert left to results or discard it
        if(left->cell_range_ != NULL) {
          left->export_to(result);
          fragment_cell_ranges.push_back(result);
        } 
        delete left;

        // Insert unary to the priority queue 
        if(unary->cell_range_ != NULL) 
          pq.push(unary); 
        else
          delete unary;

        // Re-insert new popped (right) range to the priority queue
        if(popped->cell_range_ != NULL) 
          pq.push(popped);
        else
          delete popped;
      }
    }
  }

  // Clean up 
  unsorted_fragment_cell_ranges.clear();
  if(tile_domain != NULL)
    delete [] tile_domain;
  delete [] rlen;
  delete [] rid;

  // Clean up in case of error
  if(rc != TILEDB_ARS_OK) {
    while(!pq.empty()) {
      free(pq.top()->cell_range_);
      delete pq.top();
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
ArrayReadState::PQFragmentCellRange<T>::PQFragmentCellRange(
    const ArraySchema* array_schema,
    const std::vector<ReadState*>* fragment_read_states) {
  array_schema_ = array_schema;
  fragment_read_states_ = fragment_read_states;

  cell_range_ = NULL;
  fragment_id_ = -1;
  tile_id_l_ = -1;
  tile_id_r_ = -1;
  tile_pos_ = -1;

  coords_size_ = array_schema_->coords_size();
  dim_num_ = array_schema_->dim_num();
}

template<class T>
bool ArrayReadState::PQFragmentCellRange<T>::begins_after(
    const PQFragmentCellRange* fcr) const {
  return tile_id_l_ > fcr->tile_id_r_ ||
         (tile_id_l_ == fcr->tile_id_r_ &&
          array_schema_->cell_order_cmp(
              cell_range_,
              &(fcr->cell_range_[dim_num_])) > 0);
}

template<class T>
bool ArrayReadState::PQFragmentCellRange<T>::dense() const {
  return fragment_id_ == -1 || (*fragment_read_states_)[fragment_id_]->dense();
}

template<class T>
bool ArrayReadState::PQFragmentCellRange<T>::ends_after(
    const PQFragmentCellRange* fcr) const {
  return tile_id_r_ > fcr->tile_id_r_ ||
         (tile_id_r_ == fcr->tile_id_r_ && 
          array_schema_->cell_order_cmp(
               &cell_range_[dim_num_],
               &fcr->cell_range_[dim_num_]) > 0);
}

template<class T>
void ArrayReadState::PQFragmentCellRange<T>::export_to(
    FragmentCellRange& fragment_cell_range) {
  // Copy members
  fragment_cell_range.second = cell_range_;
  fragment_cell_range.first.first = fragment_id_;
  fragment_cell_range.first.second = tile_pos_;
}

template<class T>
void ArrayReadState::PQFragmentCellRange<T>::import_from(
    const FragmentCellRange& fragment_cell_range) {
  // Copy members
  cell_range_ = static_cast<T*>(fragment_cell_range.second);
  fragment_id_ = fragment_cell_range.first.first;
  tile_pos_ = fragment_cell_range.first.second;
  
  // Compute tile ids
  tile_id_l_ = array_schema_->tile_id<T>(cell_range_);
  tile_id_r_ = array_schema_->tile_id<T>(&cell_range_[dim_num_]);
}

template<class T>
bool ArrayReadState::PQFragmentCellRange<T>::must_be_split(
    const PQFragmentCellRange* fcr) const {
  return fcr->fragment_id_ > fragment_id_ &&
         (fcr->tile_id_l_ < tile_id_r_ ||
          (fcr->tile_id_l_ == tile_id_r_ &&
           array_schema_->cell_order_cmp(
               fcr->cell_range_, 
               &cell_range_[dim_num_]) <= 0));        
}

template<class T>
bool ArrayReadState::PQFragmentCellRange<T>::must_trim(
    const PQFragmentCellRange* fcr) const {
  return fcr->fragment_id_ < fragment_id_ &&
         (fcr->tile_id_l_ > tile_id_l_ ||
          (fcr->tile_id_l_ == tile_id_l_ &&
          array_schema_->cell_order_cmp(fcr->cell_range_, cell_range_) >= 0)) &&
         (fcr->tile_id_l_ < tile_id_r_ ||
          (fcr->tile_id_l_ == tile_id_r_ &&
           array_schema_->cell_order_cmp(
               fcr->cell_range_, 
               &cell_range_[dim_num_]) <= 0));
}

template<class T>
void ArrayReadState::PQFragmentCellRange<T>::split(
    const PQFragmentCellRange* fcr,
    PQFragmentCellRange* fcr_new,
    const T* tile_domain) {
  // Create the new range
  fcr_new->fragment_id_ = fragment_id_;
  fcr_new->tile_pos_ = tile_pos_;
  fcr_new->cell_range_ = (T*) malloc(2*coords_size_);
  fcr_new->tile_id_l_ = fcr->tile_id_l_;
  memcpy(
      fcr_new->cell_range_, 
      fcr->cell_range_, 
      coords_size_);
  fcr_new->tile_id_r_ = tile_id_r_;
  memcpy(
      &(fcr_new->cell_range_[dim_num_]), 
      &cell_range_[dim_num_], 
      coords_size_);

  // Trim the calling object range
  memcpy(&cell_range_[dim_num_], fcr->cell_range_, coords_size_);
  array_schema_->get_previous_cell_coords<T>(
      tile_domain, 
      &cell_range_[dim_num_]);
  tile_id_r_ = array_schema_->tile_id<T>(&cell_range_[dim_num_]);
}

template<class T>
void ArrayReadState::PQFragmentCellRange<T>::split_to_3(
    const PQFragmentCellRange* fcr,
    PQFragmentCellRange* fcr_left,
    PQFragmentCellRange* fcr_unary) {
  // Initialize fcr_left
  fcr_left->fragment_id_ = fragment_id_;
  fcr_left->tile_pos_ = tile_pos_;
  fcr_left->cell_range_ = (T*) malloc(2*coords_size_);
  fcr_left->tile_id_l_ = tile_id_l_;
  memcpy(fcr_left->cell_range_, cell_range_, coords_size_);

  // Get enclosing coordinates
  bool left_retrieved, right_retrieved, target_exists;
  int rc = 
      (*fragment_read_states_)[fragment_id_]->template get_enclosing_coords<T>(
                tile_pos_,                        // Tile
                fcr->cell_range_,                 // Target coords
                cell_range_,                      // Start coords
                &cell_range_[dim_num_],           // End coords
                &fcr_left->cell_range_[dim_num_], // Left coords
                cell_range_,                      // Right coords
                left_retrieved,                   // Left retrieved 
                right_retrieved,                  // Right retrieved 
                target_exists);                   // Target exists
  assert(rc == TILEDB_RS_OK);
   
  // Clean up if necessary
  if(left_retrieved) { 
    fcr_left->tile_id_r_ = 
        array_schema_->tile_id<T>(&fcr_left->cell_range_[dim_num_]);
  } else {
    free(fcr_left->cell_range_);
    fcr_left->cell_range_ = NULL;
  }

  if(right_retrieved) {
    tile_id_l_ = array_schema_->tile_id<T>(cell_range_);
  } else {
    free(cell_range_);
    cell_range_ = NULL;
  }

  // Create unary range
  if(target_exists) {
    fcr_unary->fragment_id_ = fragment_id_;
    fcr_unary->tile_pos_ = tile_pos_;
    fcr_unary->cell_range_ = (T*) malloc(2*coords_size_);
    fcr_unary->tile_id_l_ = fcr->tile_id_l_;
    memcpy(fcr_unary->cell_range_, fcr->cell_range_, coords_size_); 
    fcr_unary->tile_id_r_ = fcr->tile_id_l_;
    memcpy(&(fcr_unary->cell_range_[dim_num_]), fcr->cell_range_, coords_size_);
  } else {
    fcr_unary->cell_range_ = NULL;
  } 
}


template<class T>
void ArrayReadState::PQFragmentCellRange<T>::trim(
    const PQFragmentCellRange* fcr,
    PQFragmentCellRange* fcr_trimmed,
    const T* tile_domain) const {
  // Construct trimmed range
  fcr_trimmed->fragment_id_ = fcr->fragment_id_;
  fcr_trimmed->tile_pos_ = fcr->tile_pos_;
  fcr_trimmed->cell_range_ = (T*) malloc(2*coords_size_);
  memcpy(fcr_trimmed->cell_range_, &cell_range_[dim_num_], coords_size_);
  fcr_trimmed->tile_id_l_ = tile_id_r_;
  memcpy(
      &(fcr_trimmed->cell_range_[dim_num_]), 
      &(fcr->cell_range_[dim_num_]), 
      coords_size_);
  fcr_trimmed->tile_id_r_ = fcr->tile_id_r_;

  // Advance the left endpoint of the trimmed range
  bool coords_retrieved;
  if(fcr_trimmed->dense()) {
    array_schema_->get_next_cell_coords<T>( // fcr is DENSE
        tile_domain, 
        fcr_trimmed->cell_range_,
        coords_retrieved);
  } else {                                  // fcr is SPARSE
    int rc = (*fragment_read_states_)[fcr->fragment_id_]->get_coords_after(
                 &(cell_range_[dim_num_]), 
                 fcr_trimmed->cell_range_,
                 coords_retrieved);
    assert(rc == TILEDB_RS_OK);
  }

  if(!coords_retrieved) {
    free(fcr_trimmed->cell_range_);
    fcr_trimmed->cell_range_ = NULL;
  }
}

template<class T>
bool ArrayReadState::PQFragmentCellRange<T>::unary() const {
  return !memcmp(cell_range_, &cell_range_[dim_num_], coords_size_);
}




template<class T>
ArrayReadState::SmallerPQFragmentCellRange<T>::SmallerPQFragmentCellRange() {
  array_schema_ = NULL;
}

template<class T>
ArrayReadState::SmallerPQFragmentCellRange<T>::SmallerPQFragmentCellRange(
    const ArraySchema* array_schema) {
  array_schema_ = array_schema; 
}

template<class T>
bool ArrayReadState::SmallerPQFragmentCellRange<T>::operator () (
    PQFragmentCellRange<T>* a, 
    PQFragmentCellRange<T>* b) const {
  // Sanity check
  assert(array_schema_ != NULL);

  // First check the tile ids
  if(a->tile_id_l_ < b->tile_id_l_)
    return false;
  else if(a->tile_id_l_ > b->tile_id_l_)
    return true;
  // else, check the coordinates

  // Get cell ordering information for the first range endpoints
  int cmp = array_schema_->cell_order_cmp<T>(
                a->cell_range_, 
                b->cell_range_); 

  if(cmp < 0) {        // a's range start precedes b's
    return false;
  } else if(cmp > 0) { // b's range start preceded a's
    return true;
  } else {             // a's and b's range starts match - latest fragment wins
    if(a->fragment_id_ < b->fragment_id_) {
      return true;
    } else if(a->fragment_id_ > b->fragment_id_) {
      return false;
    } else {
      assert(0); // This should never happen (equal coordinates and fragment id)
      return false;
    }
  }
}




// Explicit template instantiations
template class ArrayReadState::PQFragmentCellRange<int>;
template class ArrayReadState::PQFragmentCellRange<int64_t>;
template class ArrayReadState::PQFragmentCellRange<float>;
template class ArrayReadState::PQFragmentCellRange<double>;

template class ArrayReadState::SmallerPQFragmentCellRange<int>;
template class ArrayReadState::SmallerPQFragmentCellRange<int64_t>;
template class ArrayReadState::SmallerPQFragmentCellRange<float>;
template class ArrayReadState::SmallerPQFragmentCellRange<double>;

