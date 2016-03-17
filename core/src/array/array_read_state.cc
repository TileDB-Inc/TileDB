/**
 * @file   array_read_state.cc
 * @author Stavros Papadopoulos <stavrosp@csail.mit.edu>
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2015 Stavros Papadopoulos <stavrosp@csail.mit.edu>
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
#  define PRINT_ERROR(x) std::cerr << "[TileDB::ReadState] Error: " \
                                   << x << ".\n" 
#  define PRINT_WARNING(x) std::cerr << "[TileDB::ReadState] Warning: " \
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
  bounding_coords_end_ = NULL;
  empty_cells_written_.resize(attribute_num+1);
  tile_done_.resize(attribute_num);
  max_overlap_range_ = NULL;
  range_global_tile_coords_ = NULL;
  range_global_tile_domain_ = NULL;
  fragment_cell_pos_ranges_pos_.resize(attribute_num+1);
  fragment_cell_pos_ranges_vec_pos_.resize(attribute_num+1);
  for(int i=0; i<attribute_num+1; ++i) {
    empty_cells_written_[i] = 0;
    fragment_cell_pos_ranges_pos_[i] = 0;
    fragment_cell_pos_ranges_vec_pos_[i] = 0;
    tile_done_[i] = true;
  }
}

ArrayReadState::~ArrayReadState() { 
  if(max_overlap_range_ != NULL)
    free(max_overlap_range_);
 
  if(range_global_tile_coords_ != NULL)
    free(range_global_tile_coords_);

  if(range_global_tile_domain_ != NULL)
    free(range_global_tile_domain_);

  if(bounding_coords_end_ != NULL)
    free(bounding_coords_end_);

  for(int i=0; i<fragment_bounding_coords_.size(); ++i)
    if(fragment_bounding_coords_[i] != NULL)
      free(fragment_bounding_coords_[i]);
}

/* ****************************** */
/*           ACCESSORS            */
/* ****************************** */

bool ArrayReadState::overflow(int attribute_id) const {
  return overflow_[attribute_id];
}

/* ****************************** */
/*          READ FUNCTIONS        */
/* ****************************** */

int ArrayReadState::read_multiple_fragments(
    void** buffers, 
    size_t* buffer_sizes) {
  // Sanity check
  assert(array_->fragment_num());

  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  int attribute_num = array_schema->attribute_num();
  std::vector<Fragment*> fragments = array_->fragments();
  int fragment_num = fragments.size();

  // Reset overflow
  overflow_.resize(attribute_num+1); 
  for(int i=0; i<attribute_num+1; ++i)
    overflow_[i] = false;
 
  for(int i=0; i<fragment_num; ++i)
    fragments[i]->reset_overflow();

  if(array_schema->dense())  // DENSE case
    return read_multiple_fragments_dense(buffers, buffer_sizes);
  else                       // SPARSE case
    return read_multiple_fragments_sparse(buffers, buffer_sizes);
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
void ArrayReadState::copy_cell_range_with_empty(
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
  if(buffer_free_space == 0) { // Overflow
    overflow_[attribute_id] = true;
    return;
  }

  // Sanity check
  assert(!array_schema->var_size(attribute_id));

  // For each cell position range, copy the respective cells to the buffer
  size_t start_offset, end_offset;

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
void ArrayReadState::copy_cell_range_with_empty_var(
    int attribute_id,
    void* buffer,  
    size_t buffer_size,
    size_t& buffer_offset,
    void* buffer_var,  
    size_t buffer_var_size,
    size_t& buffer_var_offset,
    const CellPosRange& cell_pos_range) {
  // Calculate free space in buffer
  size_t buffer_free_space = buffer_size - buffer_offset;
  size_t buffer_var_free_space = buffer_var_size - buffer_var_offset;

  // Handle overflow
  if(buffer_free_space == 0 || buffer_var_free_space == 0) { // Overflow
    overflow_[attribute_id] = true; 
    return;
  }

  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  size_t cell_size = TILEDB_CELL_VAR_OFFSET_SIZE;
  char* buffer_c = static_cast<char*>(buffer);
  char* buffer_var_c = static_cast<char*>(buffer_var);

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
int ArrayReadState::copy_cell_ranges_var(
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
  size_t coords_size = array_schema->coords_size();
  FragmentCellPosRanges& fragment_cell_pos_ranges = 
      fragment_cell_pos_ranges_vec_[pos];
  int64_t fragment_cell_pos_ranges_num = fragment_cell_pos_ranges.size();
  std::vector<Fragment*> fragments = array_->fragments();
  int fragment_num = fragments.size();
  int fragment_i;
  int64_t tile_i;

  // Sanity check
  assert(array_schema->var_size(attribute_id));

  // Copy the cell ranges one by one
  for(int64_t i=0; i<fragment_cell_pos_ranges_num; ++i) {
    tile_i = fragment_cell_pos_ranges[i].first.second; 
    fragment_i = fragment_cell_pos_ranges[i].first.first; 
    CellPosRange& cell_pos_range = fragment_cell_pos_ranges[i].second; 

    // Handle empty fragment
    if(fragment_i == -1) {
      copy_cell_range_with_empty_var<T>(
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
    if(fragments[fragment_i]->copy_cell_range_var<T>(
           attribute_id,
           tile_i,
           buffer,
           buffer_size,
           buffer_offset,
           buffer_var,
           buffer_var_size,
           buffer_var_offset,
           cell_pos_range) != TILEDB_FG_OK)
       return TILEDB_ARS_ERR;

     // Handle overflow
     if(fragments[fragment_i]->overflow(attribute_id)) {
       overflow_[attribute_id] = true;
       break;
     }
  } 

  // Handle the case the tile is done for this attribute
  if(!overflow_[attribute_id]) {
//    for(int i=0; i<fragment_num; ++i)
//      if(fragment_global_tile_coords_[i] != NULL &&
//         !memcmp(
//              fragment_global_tile_coords_[i], 
//              range_global_tile_coords_, 
//              coords_size))
//        fragments[i]->tile_done<T>(attribute_id);
    ++fragment_cell_pos_ranges_vec_pos_[attribute_id];
    tile_done_[attribute_id] = true;
  } else {
    tile_done_[attribute_id] = false;
  }

  // Success
  return TILEDB_ARS_OK;
}

template<class T>
int ArrayReadState::copy_cell_ranges(
    int attribute_id,
    void* buffer,  
    size_t buffer_size,
    size_t& buffer_offset) {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  int64_t pos = fragment_cell_pos_ranges_vec_pos_[attribute_id];
  size_t coords_size = array_schema->coords_size();
  FragmentCellPosRanges& fragment_cell_pos_ranges = 
      fragment_cell_pos_ranges_vec_[pos];
  int64_t fragment_cell_pos_ranges_num = fragment_cell_pos_ranges.size();
  std::vector<Fragment*> fragments = array_->fragments();
  int fragment_num = fragments.size();
  int fragment_i;
  int64_t tile_i;

  // Sanity check
  assert(!array_schema->var_size(attribute_id));

  // Copy the cell ranges one by one
  for(int64_t i=0; i<fragment_cell_pos_ranges_num; ++i) {
    int64_t tile_i = fragment_cell_pos_ranges[i].first.second;
    fragment_i = fragment_cell_pos_ranges[i].first.first; 
    tile_i = fragment_cell_pos_ranges[i].first.second; 
    CellPosRange& cell_pos_range = fragment_cell_pos_ranges[i].second; 

    // Handle empty fragment
    if(fragment_i == -1) {
      copy_cell_range_with_empty<T>(
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
    if(fragments[fragment_i]->copy_cell_range<T>(
           attribute_id,
           tile_i,
           buffer,
           buffer_size,
           buffer_offset,
           cell_pos_range) != TILEDB_FG_OK)
       return TILEDB_ARS_ERR;

     // Handle overflow
     if(fragments[fragment_i]->overflow(attribute_id)) {
       overflow_[attribute_id] = true;
       break;
     }
  } 

  // Handle the case the tile is done for this attribute
  if(!overflow_[attribute_id]) {
//    for(int i=0; i<fragment_num; ++i)
//      if(fragment_global_tile_coords_[i] != NULL &&
//         !memcmp(
//              fragment_global_tile_coords_[i], 
//              range_global_tile_coords_, 
//              coords_size))
//        fragments[i]->tile_done<T>(attribute_id);
    ++fragment_cell_pos_ranges_vec_pos_[attribute_id];
    tile_done_[attribute_id] = true;
  } else {
    tile_done_[attribute_id] = false;
  }

  // Success
  return TILEDB_ARS_OK;
}

template<class T>
int ArrayReadState::compute_fragment_cell_pos_ranges(
    const FragmentCellRanges& unsorted_fragment_cell_ranges,
    FragmentCellPosRanges& fragment_cell_pos_ranges) const {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  int dim_num = array_schema->dim_num();
  size_t coords_size = array_schema->coords_size();
  const T* global_domain = static_cast<const T*>(array_schema->domain());
  const T* tile_extents = static_cast<const T*>(array_schema->tile_extents());
  const T* tile_coords = static_cast<const T*>(range_global_tile_coords_);
  std::vector<Fragment*> fragments = array_->fragments();
  int fragment_num = fragments.size();
  FragmentCellRanges fragment_cell_ranges;

  // Compute tile domain
  T* tile_domain = NULL;
  T* tile_domain_end = NULL;
  if(tile_coords != NULL) {
    tile_domain = new T[2*dim_num];
    tile_domain_end = new T[dim_num];
    for(int i=0; i<dim_num; ++i) {
      tile_domain[2*i] = global_domain[2*i] + tile_coords[i] * tile_extents[i];
      tile_domain[2*i+1] = tile_domain[2*i] + tile_extents[i] - 1;
      tile_domain_end[i] = tile_domain[2*i+1];
    }
  }

  if(fragment_num == 1) {
    fragment_cell_ranges = unsorted_fragment_cell_ranges;
  } else {
    // Populate queue
    std::priority_queue<
        FragmentCellRange,
        FragmentCellRanges,
        SmallerFragmentCellRange<T> > pq(array_schema);
    int unsorted_fragment_cell_ranges_num = unsorted_fragment_cell_ranges.size();
    for(int64_t i=0; i<unsorted_fragment_cell_ranges_num; ++i)  
      pq.push(unsorted_fragment_cell_ranges[i]);
  
    // Start processing the queue
    FragmentCellRange popped, top;
    int popped_fragment_i, top_fragment_i;
    int popped_tile_i, top_tile_i;
    T* popped_range, *top_range;
    while(!pq.empty()) {
      // Pop the first entry and mark it as popped
      popped = pq.top();
      popped_fragment_i = popped.first.first;
      popped_tile_i = popped.first.second;
      popped_range = static_cast<T*>(popped.second);
      pq.pop();

      // Trivial case: just insert the popped range into the results and stop
      if(pq.empty()) {
        if(popped_fragment_i == -1 ||
           fragments[popped_fragment_i]->dense() ||
           memcmp(
                 popped_range, 
                 &popped_range[dim_num], 
                 coords_size) || 
            fragments[popped_fragment_i]->coords_exist<T>(
                popped_tile_i, 
                popped_range)) {
          fragment_cell_ranges.push_back(popped);
          break;
        }
      }

      // Mark the second entry (now top) as top
      top = pq.top();
      top_fragment_i = top.first.first;
      top_tile_i = top.first.second;
      top_range = static_cast<T*>(top.second);

      // Dinstinguish two cases
      if(popped_fragment_i == -1 ||
         fragments[popped_fragment_i]->dense() ||
         !memcmp(
             popped_range, 
             &popped_range[dim_num], 
             coords_size)) {                          // DENSE POPPED OR UNARY
        // If the unary sparse range is empty, discard it
        if(popped_fragment_i != -1 &&
           !fragments[popped_fragment_i]->dense() && 
           !fragments[popped_fragment_i]->coords_exist<T>(
               popped_tile_i,
               popped_range)) {
          free(popped.second);
          continue;
        }

        // Keep on discarding ranges from the queue
        while(!pq.empty() &&
              top_fragment_i < popped_fragment_i &&
              array_schema->cell_order_cmp(
                  top_range, 
                  popped_range) >= 0 &&
              array_schema->cell_order_cmp(
                  top_range, 
                  &popped_range[dim_num]) <= 0) {

          // Cut the top range and re-insert, only if there is partial overlap
          if(array_schema->cell_order_cmp(
                 &top_range[dim_num], 
                 &popped_range[dim_num]) > 0) {
            // Create the new range
            FragmentCellRange trimmed_top;
            trimmed_top.first = FragmentInfo(top_fragment_i, top_tile_i);
            trimmed_top.second = malloc(2*coords_size);
            T* trimmed_top_range = static_cast<T*>(trimmed_top.second);
            memcpy(
                trimmed_top_range, 
                &popped_range[dim_num], 
                coords_size);
            memcpy(
                &trimmed_top_range[dim_num], 
                &top_range[dim_num], 
                coords_size);

            // Advance the first trimmed range coordinates by one
            if(fragments[top_fragment_i]->dense()) { // DENSE
              array_schema->get_next_cell_coords<T>(
                  tile_domain, 
                  trimmed_top_range);
              pq.push(trimmed_top);
            } else { // SPARSE
              FragmentCellRange unary;
              unary.first.first = top_fragment_i;
              unary.first.second = top_tile_i;
              unary.second = malloc(2*coords_size);
              T* unary_range = static_cast<T*>(unary.second);

              // Get the first two coordinates from the coordinates tile 
              if(fragments[top_fragment_i]->get_first_two_coords<T>(
                     top_tile_i,          // Tile
                     trimmed_top_range,   // Starting point
                     unary_range,         // First coords
                     trimmed_top_range)   // Second coords
                  != TILEDB_FG_OK) {  
                // Clean up
                free(unary.second);
                if(tile_domain != NULL)
                  delete [] tile_domain;
                if(tile_domain_end != NULL)
                  delete [] tile_domain_end;
                free(trimmed_top.second);
                while(!pq.empty()) {
                  free(pq.top().second);
                  pq.pop();
                }
                for(int i=0; i<fragment_cell_ranges.size(); ++i)
                  free(fragment_cell_ranges[i].second);
  
                return TILEDB_ARS_ERR;
              }

              // Check if the now trimmed popped range exceeds the tile domain
              bool inside_tile = true;
              if(empty_value(*unary_range)) 
                inside_tile = false;

              // Copy second boundary of unary and re-insert to the queue
              if(!inside_tile) {
                free(unary.second);
              } else {
                memcpy(&unary_range[dim_num], unary_range, coords_size);
                pq.push(unary);

                // Check if the now trimmed popped range exceeds the tile domain
                bool inside_tile = true;
                if(empty_value(*trimmed_top_range)) 
                  inside_tile = false;
       
                if(!inside_tile) { 
                  free(trimmed_top.second);
                } else { // Re-insert to the queue the now trimmed popped range
                  pq.push(trimmed_top);
                }
              }
            }
          } else { // Simply discard top and get a new one
            free(top.second);
          }

          // Get a new top
          pq.pop();
          top = pq.top();
          top_fragment_i = top.first.first;
          top_tile_i = top.first.second;
          top_range = static_cast<T*>(top.second);
        }

        //Potentially trim the popped range
        if(!pq.empty() && 
           top_fragment_i > popped_fragment_i && 
           array_schema->cell_order_cmp(
               top_range, 
               &popped_range[dim_num]) <= 0) {         
           // Top is sparse
           if(!fragments[top_fragment_i]->dense()) {
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
           } else if(array_schema->cell_order_cmp(
                    &top_range[dim_num], 
                    &popped_range[dim_num]) < 0) {
             // Create a new popped range
             FragmentCellRange extra_popped;
             extra_popped.first.first = popped_fragment_i;
             extra_popped.first.second = popped_tile_i;
             extra_popped.second = malloc(2*coords_size);
             T* extra_popped_range = static_cast<T*>(extra_popped.second);

             memcpy(extra_popped_range, &top_range[dim_num], coords_size);
             memcpy(
                 &extra_popped_range[dim_num], 
                 &popped_range[dim_num], 
                 coords_size);
             array_schema->get_next_cell_coords<T>(
                 tile_domain, 
                 extra_popped_range);

             // Re-instert the extra popped range into the queue
             pq.push(extra_popped);
           }

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
        int64_t top_range_tile_id, popped_range_tile_id;
        int cmp;
      
        if(!pq.empty()) {
          top_range_tile_id = array_schema->tile_id(top_range);
          popped_range_tile_id = array_schema->tile_id(&popped_range[dim_num]);

          if(top_range_tile_id == popped_range_tile_id) {
            cmp = array_schema->cell_order_cmp(
                                    top_range, 
                                    &popped_range[dim_num]);
          } else {
            if(top_range_tile_id < popped_range_tile_id)
              cmp = -1;
            else 
              cmp = 1;
          }
        }

        if(!pq.empty() && cmp > 0) {
          fragment_cell_ranges.push_back(popped);
        } else { // Need to expand popped
          // Create a new unary range
          FragmentCellRange unary;
          unary.first.first = popped_fragment_i;
          unary.first.second = popped_tile_i;
          unary.second = malloc(2*coords_size);
          T* unary_range = static_cast<T*>(unary.second);
        
          // Get the first two coordinates from the coordinates tile 
          if(fragments[popped_fragment_i]->get_first_two_coords<T>(
                 popped_tile_i,  // Tile
                 popped_range,   // Starting point
                 unary_range,    // First coords
                 popped_range)   // Second coords
              != TILEDB_FG_OK) {  
            // Clean up
            free(unary.second);
            if(tile_domain != NULL)
              delete [] tile_domain;
            if(tile_domain_end != NULL)
              delete [] tile_domain_end;
            free(popped.second);
            while(!pq.empty()) {
              free(pq.top().second);
              pq.pop();
            }
            for(int i=0; i<fragment_cell_ranges.size(); ++i)
              free(fragment_cell_ranges[i].second);

            return TILEDB_ARS_ERR;
          }

          // Check if the now trimmed popped range exceeds the tile domain
          bool inside_tile = true;
          if(tile_domain != NULL) {
            for(int i=0; i<dim_num; ++i) {
              if(unary_range[i] < tile_domain[2*i] || 
                 unary_range[i] > tile_domain[2*i+1]) {
                inside_tile = false;
                break;
              }
            }
          } else {
            if(empty_value(*unary_range)) 
              inside_tile = false;
          }

          // Copy second boundary of unary and re-insert to the queue
          if(!inside_tile) {
            free(unary.second);
          } else {
            memcpy(&unary_range[dim_num], unary_range, coords_size);
            pq.push(unary);

            // Check if the now trimmed popped range exceeds the tile domain
            bool inside_tile = true;
            if(tile_domain != NULL) {
              for(int i=0; i<dim_num; ++i) {
                if(popped_range[i] < tile_domain[2*i] || 
                   popped_range[i] > tile_domain[2*i+1]) {
                  inside_tile = false;
                  break;
                }
              } 
            } else {
              if(empty_value(*popped_range)) 
                inside_tile = false;
            }
       
            if(!inside_tile) { 
              free(popped.second);
            } else { // Re-insert to the queue the now trimmed popped range
              pq.push(popped);
            }
          }
        }
      }
    }

    // Sanity check
    assert(pq.empty());
  }

  // Compute fragment cell position ranges
  for(int64_t i=0; i<fragment_cell_ranges.size(); ++i) { 
    // Dense case
    if(fragment_cell_ranges[i].first.first == -1 ||
       fragments[fragment_cell_ranges[i].first.first]->dense()) {
      // Create a new fragment cell position range
      FragmentCellPosRange fragment_cell_pos_range;
      fragment_cell_pos_range.first = fragment_cell_ranges[i].first;
      CellPosRange& cell_pos_range = fragment_cell_pos_range.second;
      T* cell_range = static_cast<T*>(fragment_cell_ranges[i].second);

      // Normalize range
      for(int i=0; i<dim_num; ++i) { 
        cell_range[i] -= tile_domain[2*i];
        cell_range[dim_num+i] -= tile_domain[2*i];
      }
      cell_pos_range.first = array_schema->get_cell_pos(cell_range);
      cell_pos_range.second = array_schema->get_cell_pos(&cell_range[dim_num]);
      fragment_cell_pos_ranges.push_back(fragment_cell_pos_range); 
    } else { // Sparse case
     FragmentCellPosRanges sparse_cell_pos_ranges; 
     if(fragments[fragment_cell_ranges[i].first.first]->
            get_cell_pos_ranges_sparse<T>(
                fragment_cell_ranges[i].first,
                tile_domain,
                static_cast<T*>(fragment_cell_ranges[i].second),
                sparse_cell_pos_ranges) != TILEDB_FG_OK) {
        // Clean up
        for(int j=i; j < fragment_cell_ranges.size(); ++j) 
          free(fragment_cell_ranges[j].second);
        // Exit
        return TILEDB_ARS_ERR;
      }
      fragment_cell_pos_ranges.insert(
          fragment_cell_pos_ranges.end(),
          sparse_cell_pos_ranges.begin(), 
          sparse_cell_pos_ranges.end()); 
    }

    free(fragment_cell_ranges[i].second);
  }

  // Clean up 
  if(tile_domain != NULL)
    delete [] tile_domain;
  if(tile_domain_end != NULL)
    delete [] tile_domain_end;
  
  // Success
  return TILEDB_ARS_OK;
}

template<class T>
int ArrayReadState::get_next_cell_ranges_dense() {
  // Trivial case
  if(done_)
    return TILEDB_ARS_OK;

  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  int dim_num = array_schema->dim_num();
  size_t coords_size = array_schema->coords_size();
  std::vector<Fragment*> fragments = array_->fragments();
  int fragment_num = fragments.size();

  // Initializations
  last_tile_i_.resize(fragment_num);
  for(int i=0; i<fragment_num; ++i) 
    last_tile_i_[i] = 0;

  // First invocation: bring the first overlapping tile for each fragment
  if(fragment_cell_pos_ranges_vec_.size() == 0) {
    // Allocate space for the maximum overlap range
    max_overlap_range_ = malloc(2*coords_size);

    // Initialize range global tile coordinates
    init_range_global_tile_coords<T>();

    // Return if there are no more overlapping tiles
    if(range_global_tile_coords_ == NULL) {
      done_ = true;
      return TILEDB_ARS_OK;
    }

    // Get next overlapping tile and calculate its global position for
    // every fragment
    fragment_global_tile_coords_.resize(fragment_num);
    for(int i=0; i<fragment_num; ++i) { 
      fragments[i]->get_next_overlapping_tile_mult();
      fragment_global_tile_coords_[i] = 
          fragments[i]->get_global_tile_coords();
    }
  } else { 
    // Temporarily store the current range global tile coordinates
    assert(range_global_tile_coords_ != NULL);
    T* previous_range_global_tile_coords = new T[dim_num];
    memcpy(
        previous_range_global_tile_coords,
        range_global_tile_coords_,
        coords_size);

    // Advance range coordinates
    get_next_range_global_tile_coords<T>();

    // Return if there are no more overlapping tiles
    if(range_global_tile_coords_ == NULL) {
      done_ = true;
      delete [] previous_range_global_tile_coords;
      return TILEDB_ARS_OK;
    }

    // Subsequent invocations: get next overallping tiles for the processed
    // fragments
    for(int i=0; i<fragment_num; ++i) {
      if(fragment_global_tile_coords_[i] != NULL &&
         !memcmp(
             fragment_global_tile_coords_[i], 
             previous_range_global_tile_coords, 
             coords_size)) { 
        fragments[i]->get_next_overlapping_tile_mult();
        fragment_global_tile_coords_[i] = 
            fragments[i]->get_global_tile_coords(); 
      }
    }

    // Clean up
    delete [] previous_range_global_tile_coords;
  }

  // Advance properly the sparse fragments
  for(int i=0; i<fragment_num; ++i) {
    while(!fragments[i]->dense() && 
          fragment_global_tile_coords_[i] != NULL &&
          array_schema->tile_order_cmp<T>(
               static_cast<const T*>(fragment_global_tile_coords_[i]), 
               static_cast<const T*>(range_global_tile_coords_)) < 0) {
       fragments[i]->get_next_overlapping_tile_mult();
       fragment_global_tile_coords_[i] = 
           fragments[i]->get_global_tile_coords();
    }

  }

  // Compute the maximum overlap range for this tile
  compute_max_overlap_range<T>();

  // Find the most recent fragment with a full dense tile in the smallest global
  // tile position
  max_overlap_i_ = -1;
  for(int i=fragment_num-1; i>=0; --i) { 
    if(fragment_global_tile_coords_[i] != NULL &&
       !memcmp(
            fragment_global_tile_coords_[i], 
            range_global_tile_coords_, 
            coords_size) &&
       fragments[i]->max_overlap<T>(
           static_cast<const T*>(max_overlap_range_))) {
      max_overlap_i_ = i;
      break; 
    }
  } 

  // This will hold the unsorted fragment cell ranges
  FragmentCellRanges unsorted_fragment_cell_ranges;

  // Compute initial cell ranges for the fragment with the max overlap
  compute_max_overlap_fragment_cell_ranges<T>(unsorted_fragment_cell_ranges);  

  // Compute cell ranges for the rest of the relevant fragments
  for(int i=max_overlap_i_+1; i<fragment_num; ++i) {
    if(fragment_global_tile_coords_[i] != NULL &&
       !memcmp(
           fragment_global_tile_coords_[i], 
           range_global_tile_coords_, 
           coords_size)) { 
      // TODO: Put error
      fragments[i]->compute_fragment_cell_ranges<T>(
          FragmentInfo(i, fragments[i]->overlapping_tiles_num()-1),
          unsorted_fragment_cell_ranges);

      // Special case for sparse fragments having tiles in the same dense tile
      if(!fragments[i]->dense()) {
        for(;;) {
          fragments[i]->get_next_overlapping_tile_mult();
          fragment_global_tile_coords_[i] = 
              fragments[i]->get_global_tile_coords();
          if(fragment_global_tile_coords_[i] != NULL &&
             !memcmp(
                  fragment_global_tile_coords_[i], 
                  range_global_tile_coords_, 
                  coords_size)) {

            // TODO: Put error
            fragments[i]->compute_fragment_cell_ranges<T>(
                FragmentInfo(i, fragments[i]->overlapping_tiles_num()-1),
                unsorted_fragment_cell_ranges);
          } else {
            break;
          }
        }
      }
    }
  } 

  // Compute the fragment cell position ranges
  FragmentCellPosRanges fragment_cell_pos_ranges;
  if(compute_fragment_cell_pos_ranges<T>(
         unsorted_fragment_cell_ranges, 
         fragment_cell_pos_ranges) != TILEDB_ARS_OK) {
    // Clean up
    for(int64_t i=0; i<unsorted_fragment_cell_ranges.size(); ++i)
      free(unsorted_fragment_cell_ranges[i].second);
    return TILEDB_ARS_ERR;
  }

  // Insert cell pos ranges in the state
  fragment_cell_pos_ranges_vec_.push_back(fragment_cell_pos_ranges);

  // Clean up processed overlapping tiles
  clean_up_processed_fragment_cell_pos_ranges();

  // Success
  return TILEDB_ARS_OK;
}

template<class T>
int ArrayReadState::get_next_cell_ranges_sparse() {
  // Trivial case
  if(done_)
    return TILEDB_ARS_OK;

  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  int dim_num = array_schema->dim_num();
  size_t coords_size = array_schema->coords_size();
  std::vector<Fragment*> fragments = array_->fragments();
  int fragment_num = fragments.size();

  // First invocation: bring the first overlapping tile for each fragment
  if(fragment_cell_pos_ranges_vec_.size() == 0) {
    // Initializations 
    assert(fragment_bounding_coords_.size() == 0);
    fragment_bounding_coords_.resize(fragment_num);
    assert(bounding_coords_end_ == NULL);
    bounding_coords_end_ = malloc(coords_size);

    // Get next overlapping tile and MBR cell range
    done_ = true;
    for(int i=0; i<fragment_num; ++i) { 
      fragments[i]->get_next_overlapping_tile_sparse<T>();
      if(fragments[i]->overlaps()) {
        fragment_bounding_coords_[i] = malloc(2*coords_size);
        fragments[i]->get_bounding_coords(fragment_bounding_coords_[i]);
        done_ = false;
      } else {
        fragment_bounding_coords_[i] = NULL;
      }
    }

    // Return if there are no more overlapping tiles
    if(done_) 
      return TILEDB_ARS_OK;

  } else { 
    // Subsequent invocations: get next overallping tiles for the processed
    // fragments
    for(int i=0; i<fragment_num; ++i) { 
      T* fragment_bounding_coords = 
          static_cast<T*>(fragment_bounding_coords_[i]);
      if(fragment_bounding_coords_[i] != NULL &&
         !memcmp(
             &fragment_bounding_coords[dim_num], 
             bounding_coords_end_, 
             coords_size)) { 
        fragments[i]->get_next_overlapping_tile_sparse<T>();
        if(fragments[i]->overlaps()) {
          fragments[i]->get_bounding_coords(fragment_bounding_coords_[i]);
        } else {
          fragment_bounding_coords_[i] = NULL;
        }
      }
    }

    done_ = true;
    for(int i=0; i<fragment_num; ++i) { 
      if(fragment_bounding_coords_[i] != NULL) {
        done_ = false;
        break;
      }
    }

    // Return if there are no more overlapping tiles
    if(done_) 
      return TILEDB_ARS_OK;
  }

  // Find smallest end bounding coordinates
  int first = true;
  for(int i=0; i<fragment_num; ++i) {
    T* fragment_bounding_coords = static_cast<T*>(fragment_bounding_coords_[i]);
    T* bounding_coords_end = static_cast<T*>(bounding_coords_end_);
    if(fragment_bounding_coords != NULL) {
      if(first) {
        memcpy(
            bounding_coords_end, 
            &fragment_bounding_coords[dim_num], 
            coords_size);
        first = false;
      } else if(array_schema->cell_order_cmp_2( 
                    &fragment_bounding_coords[dim_num],
                    bounding_coords_end) < 0) {
        memcpy(
            bounding_coords_end, 
            &fragment_bounding_coords[dim_num], 
            coords_size);
      }
    }
  }

  // Compute the cell ranges needed for this run, and update bounding coords
  FragmentCellRanges unsorted_fragment_cell_ranges;
  for(int i=0; i<fragment_num; ++i) {
    T* fragment_bounding_coords = static_cast<T*>(fragment_bounding_coords_[i]);
    T* bounding_coords_end = static_cast<T*>(bounding_coords_end_);
    if(fragment_bounding_coords != NULL &&
       array_schema->cell_order_cmp_2(
             fragment_bounding_coords,
             bounding_coords_end) <= 0) {
      FragmentCellRange fragment_cell_range;
      fragment_cell_range.second = malloc(2*coords_size);
      T* cell_range = static_cast<T*>(fragment_cell_range.second);
      memcpy(cell_range, fragment_bounding_coords, coords_size);
      memcpy(&cell_range[dim_num], bounding_coords_end_, coords_size);
      fragment_cell_range.first = 
          FragmentInfo(i, fragments[i]->overlapping_tiles_num()-1);
      unsorted_fragment_cell_ranges.push_back(fragment_cell_range);

      // If the end bounding coordinate is not the same, update the start
      // bounding coordinate
      if(memcmp(
             &fragment_bounding_coords[dim_num], 
             bounding_coords_end, 
             coords_size)) {
        // Get the first coordinates AFTER the current position 
        if(fragments[i]->get_first_coords_after<T>(
               fragments[i]->overlapping_tiles_num()-1,  // Tile
               bounding_coords_end,                      // Starting point
               fragment_bounding_coords)                 // First coords
            != TILEDB_FG_OK) {  
          // Clean up
          // TODO
          return TILEDB_ARS_ERR;
        }

        // Sanity check
        assert(!empty_value(*fragment_bounding_coords));
      } 
    }
  }

  // Compute the fragment cell position ranges
  FragmentCellPosRanges fragment_cell_pos_ranges;
  if(compute_fragment_cell_pos_ranges<T>(
         unsorted_fragment_cell_ranges, 
         fragment_cell_pos_ranges) != TILEDB_ARS_OK) {
    // Clean up
    for(int64_t i=0; i<unsorted_fragment_cell_ranges.size(); ++i)
      free(unsorted_fragment_cell_ranges[i].second);
    return TILEDB_ARS_ERR;
  }

  // Insert cell pos ranges in the state
  fragment_cell_pos_ranges_vec_.push_back(fragment_cell_pos_ranges);

  // Clean up processed overlapping tiles
  clean_up_processed_fragment_cell_pos_ranges();

  // Success
  return TILEDB_ARS_OK;
}

template<class T>
void ArrayReadState::compute_max_overlap_fragment_cell_ranges(
    FragmentCellRanges& fragment_cell_ranges) const {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  int dim_num = array_schema->dim_num();
  size_t coords_size = array_schema->coords_size();
  int cell_order = array_schema->cell_order();
  size_t cell_range_size = 2*coords_size;
  const T* tile_extents = static_cast<const T*>(array_schema->tile_extents());
  const T* global_domain = static_cast<const T*>(array_schema->domain());
  const T* range_global_tile_coords = 
      static_cast<const T*>(range_global_tile_coords_);
  const T* max_overlap_range = 
      static_cast<const T*>(max_overlap_range_);
  std::vector<Fragment*> fragments = array_->fragments();

  // Compute global coordinates of max_overlap_range
  T* global_max_overlap_range = new T[2*dim_num]; 
  for(int i=0; i<dim_num; ++i) {
    global_max_overlap_range[2*i] = 
        range_global_tile_coords[i] * tile_extents[i] +
        max_overlap_range[2*i] + global_domain[2*i];
    global_max_overlap_range[2*i+1] = 
        range_global_tile_coords[i] * tile_extents[i] +
        max_overlap_range[2*i+1] + global_domain[2*i];
  }

  // Contiguous cells, single cell range
  if(max_overlap_type_ == FULL || max_overlap_type_ == PARTIAL_CONTIG) {
    void* cell_range = malloc(cell_range_size);
    T* cell_range_T = static_cast<T*>(cell_range);
    for(int i=0; i<dim_num; ++i) {
      cell_range_T[i] = global_max_overlap_range[2*i];
      cell_range_T[dim_num + i] = global_max_overlap_range[2*i+1];
    }
    if(max_overlap_i_ == -1)
      fragment_cell_ranges.push_back(
          FragmentCellRange(FragmentInfo(max_overlap_i_, 0), cell_range));
    else 
      fragment_cell_ranges.push_back(
          FragmentCellRange(
              FragmentInfo(
                  max_overlap_i_, 
                  fragments[max_overlap_i_]->overlapping_tiles_num()-1), 
              cell_range));
  } else { // Non-contiguous cells, multiple ranges
    // Initialize the coordinates at the beginning of the global range
    T* coords = new T[dim_num];
    for(int i=0; i<dim_num; ++i)
      coords[i] = global_max_overlap_range[2*i];

    // Handle the different cell orders
    int i;
    if(cell_order == TILEDB_ROW_MAJOR) {           // ROW
      while(coords[0] <= global_max_overlap_range[1]) {
        // Make a cell range representing a slab       
        void* cell_range = malloc(cell_range_size);
        T* cell_range_T = static_cast<T*>(cell_range);
        for(int i=0; i<dim_num-1; ++i) { 
          cell_range_T[i] = coords[i];
          cell_range_T[dim_num+i] = coords[i];
        }
        cell_range_T[dim_num-1] = global_max_overlap_range[2*(dim_num-1)];
        cell_range_T[2*dim_num-1] = global_max_overlap_range[2*(dim_num-1)+1];

        // Insert the new range into the result vector
        if(max_overlap_i_ == -1)
          fragment_cell_ranges.push_back(
              FragmentCellRange(FragmentInfo(max_overlap_i_, 0), cell_range));
        else 
          fragment_cell_ranges.push_back(
              FragmentCellRange(
                   FragmentInfo(
                       max_overlap_i_, 
                       fragments[max_overlap_i_]->overlapping_tiles_num()-1), 
                   cell_range));
 
        // Advance coordinates
        i=dim_num-2;
        ++coords[i];
        while(i > 0 && coords[i] > global_max_overlap_range[2*i+1]) {
          coords[i] = global_max_overlap_range[2*i];
          ++coords[--i];
        } 
      }
    } else if(cell_order == TILEDB_COL_MAJOR) { // COLUMN
      while(coords[dim_num-1] <= global_max_overlap_range[2*(dim_num-1)+1]) {
        // Make a cell range representing a slab       
        void* cell_range = malloc(cell_range_size);
        T* cell_range_T = static_cast<T*>(cell_range);
        for(int i=dim_num-1; i>0; --i) { 
          cell_range_T[i] = coords[i];
          cell_range_T[dim_num+i] = coords[i];
        }
        cell_range_T[0] = global_max_overlap_range[0];
        cell_range_T[dim_num] = global_max_overlap_range[1];

        // Insert the new range into the result vector
        if(max_overlap_i_ == -1)
          fragment_cell_ranges.push_back(
              FragmentCellRange(FragmentInfo(max_overlap_i_, 0), cell_range));
        else 
          fragment_cell_ranges.push_back(
              FragmentCellRange(
                   FragmentInfo(
                       max_overlap_i_, 
                       fragments[max_overlap_i_]->overlapping_tiles_num()-1), 
                   cell_range));
 
        // Advance coordinates
        i=1;
        ++coords[i];
        while(i <dim_num-1 && coords[i] > global_max_overlap_range[2*i+1]) {
          coords[i] = global_max_overlap_range[2*i];
          ++coords[++i];
        } 
      }
    } else {
      assert(0);
    }

    delete [] coords;
  }

  // Clean up
  delete [] global_max_overlap_range;
}

template<class T>
void ArrayReadState::compute_max_overlap_range() {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  int dim_num = array_schema->dim_num();
  int cell_order = array_schema->cell_order();
  const T* tile_extents = static_cast<const T*>(array_schema->tile_extents());
  const T* global_domain = static_cast<const T*>(array_schema->domain());
  const T* range = static_cast<const T*>(array_->subarray());

  T tile_coords;
  T* max_overlap_range = static_cast<T*>(max_overlap_range_);
  const T* range_global_tile_coords = 
      static_cast<const T*>(range_global_tile_coords_);
  for(int i=0; i<dim_num; ++i) {
    tile_coords = 
        range_global_tile_coords[i] * tile_extents[i] + global_domain[2*i];
    max_overlap_range[2*i] = std::max(range[2*i] - tile_coords, T(0));
    max_overlap_range[2*i+1] = 
        std::min(range[2*i+1] - tile_coords, tile_extents[i] - 1);
  }

  // Check overlap
  max_overlap_type_ = FULL;
  for(int i=0; i<dim_num; ++i) {
    if(max_overlap_range[2*i] != 0 ||
       max_overlap_range[2*i+1] != tile_extents[i] - 1) {
      max_overlap_type_ = PARTIAL_NON_CONTIG;
      break;
    }
  }

  if(max_overlap_type_ == PARTIAL_NON_CONTIG) {
    max_overlap_type_ = PARTIAL_CONTIG;
    if(cell_order == TILEDB_ROW_MAJOR) {   // Row major
      for(int i=1; i<dim_num; ++i) {
        if(max_overlap_range[2*i] != 0 ||
           max_overlap_range[2*i+1] != tile_extents[i] - 1) {
          max_overlap_type_ = PARTIAL_NON_CONTIG;
          break;
        }
      }
    } else if(cell_order == TILEDB_COL_MAJOR) { 
      // Column major
      for(int i=dim_num-2; i>=0; --i) {
        if(max_overlap_range[2*i] != 0 ||
           max_overlap_range[2*i+1] != tile_extents[i] - 1) {
          max_overlap_type_ = PARTIAL_NON_CONTIG;
          break;
        }
      }
    }
  }
}

template<class T>
void ArrayReadState::get_next_range_global_tile_coords() {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  int dim_num = array_schema->dim_num();
  T* range_global_tile_domain = static_cast<T*>(range_global_tile_domain_);
  T* range_global_tile_coords = static_cast<T*>(range_global_tile_coords_);

  // Advance range tile coordinates
  array_schema->get_next_tile_coords<T>(
      range_global_tile_domain, 
      range_global_tile_coords);

  // Check if the new range coordinates fall out of the range domain
  bool inside_domain = true;
  for(int i=0; i<dim_num; ++i) {
    if(range_global_tile_coords[i] < range_global_tile_domain[2*i] ||
       range_global_tile_coords[i] > range_global_tile_domain[2*i+1]) {
      inside_domain = false;
      break;
    }
  }

  // The coordinates fall outside the domain
  if(!inside_domain) {  
    free(range_global_tile_domain_);
    range_global_tile_domain_ = NULL; 
    free(range_global_tile_coords_);
    range_global_tile_coords_ = NULL; 
  } 
}

template<class T>
void ArrayReadState::init_range_global_tile_coords() {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  int dim_num = array_schema->dim_num();
  size_t coords_size = array_schema->coords_size();
  const T* domain = static_cast<const T*>(array_schema->domain());
  const T* tile_extents = static_cast<const T*>(array_schema->tile_extents());
  const T* range = static_cast<const T*>(array_->subarray());

  // Sanity check
  assert(tile_extents != NULL);

  // Compute tile domain
  T* tile_domain = new T[2*dim_num];
  T tile_num; // Per dimension
  for(int i=0; i<dim_num; ++i) {
    tile_num = ceil(double(domain[2*i+1] - domain[2*i] + 1) / tile_extents[i]);
    tile_domain[2*i] = 0;
    tile_domain[2*i+1] = tile_num - 1;
  }

  // Allocate space for the range in tile domain
  assert(range_global_tile_domain_ == NULL);
  range_global_tile_domain_ = malloc(2*dim_num*sizeof(T));

  // For easy reference
  T* range_global_tile_domain = static_cast<T*>(range_global_tile_domain_);

  // Calculate range in tile domain
  for(int i=0; i<dim_num; ++i) {
    range_global_tile_domain[2*i] = 
        std::max((range[2*i] - domain[2*i]) / tile_extents[i],
            tile_domain[2*i]); 
    range_global_tile_domain[2*i+1] = 
        std::min((range[2*i+1] - domain[2*i]) / tile_extents[i],
            tile_domain[2*i+1]); 
  }

  // Check if there is any overlap between the range and the tile domain
  bool overlap = true;
  for(int i=0; i<dim_num; ++i) {
    if(range_global_tile_domain[2*i] > tile_domain[2*i+1] ||
       range_global_tile_domain[2*i+1] < tile_domain[2*i]) {
      overlap = false;
      break;
    }
  }

  // Calculate range global tile coordinates
  if(!overlap) {  // No overlap
    free(range_global_tile_domain_);
    range_global_tile_domain_ = NULL; 
  } else {        // Overlap
    range_global_tile_coords_ = malloc(coords_size);
    T* range_global_tile_coords = static_cast<T*>(range_global_tile_coords_);
    for(int i=0; i<dim_num; ++i) 
      range_global_tile_coords[i] = range_global_tile_domain[2*i]; 
  }

  // Clean up
  delete [] tile_domain;
}

int ArrayReadState::read_multiple_fragments_dense(
    void** buffers,  
    size_t* buffer_sizes) {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  std::vector<int> attribute_ids = array_->attribute_ids();
  int attribute_id_num = attribute_ids.size(); 

  // Write each attribute individually
  int buffer_i = 0;
  int rc;
  for(int i=0; i<attribute_id_num; ++i) {
    if(!array_schema->var_size(attribute_ids[i])) { // FIXED CELLS
      rc = read_multiple_fragments_dense_attr(
               attribute_ids[i], 
               buffers[buffer_i], 
               buffer_sizes[buffer_i]);

      if(rc != TILEDB_AR_OK)
        break;
      ++buffer_i;
    } else {                                         // VARIABLE-SIZED CELLS
      rc = read_multiple_fragments_dense_attr_var(
               attribute_ids[i], 
               buffers[buffer_i],       // offsets 
               buffer_sizes[buffer_i],
               buffers[buffer_i+1],     // actual values
               buffer_sizes[buffer_i+1]);

      if(rc != TILEDB_AR_OK)
        break;
      buffer_i += 2;
    }
  }

  return rc;
}

int ArrayReadState::read_multiple_fragments_dense_attr(
    int attribute_id,
    void* buffer,  
    size_t& buffer_size) {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  int coords_type = array_schema->coords_type();

  // Invoke the proper templated function
  if(coords_type == TILEDB_INT32) {
    return read_multiple_fragments_dense_attr<int>(
               attribute_id, 
               buffer, 
               buffer_size);
  } else if(coords_type == TILEDB_INT64) {
    return read_multiple_fragments_dense_attr<int64_t>(
               attribute_id, 
               buffer, 
               buffer_size);
  } else {
    PRINT_ERROR("Cannot read from array; Invalid coordinates type");
    return TILEDB_ARS_ERR;
  }
}

template<class T>
int ArrayReadState::read_multiple_fragments_dense_attr(
    int attribute_id,
    void* buffer,  
    size_t& buffer_size) {
  // Auxiliary variables
  size_t buffer_offset = 0;

  // Until read is done or there is a buffer overflow 
  for(;;) {
    // Continue copying from the previous unfinished tile
    if(!tile_done_[attribute_id])
      if(copy_cell_ranges<T>(
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

    // Prepare the cell ranges for the next tile under investigation, if
    // has not be done already
    if(fragment_cell_pos_ranges_vec_pos_[attribute_id] >= 
       fragment_cell_pos_ranges_vec_.size()) {
      // Get next overlapping tiles
      if(get_next_cell_ranges_dense<T>() != TILEDB_ARS_OK)
        return TILEDB_ARS_ERR;
    }

    // Check if read is done
    if(done_ &&
       fragment_cell_pos_ranges_vec_pos_[attribute_id] == 
       fragment_cell_pos_ranges_vec_.size()) {
      buffer_size = buffer_offset;
      return TILEDB_ARS_OK;
    }
 
    // Process the heap and copy cells to buffers
    if(copy_cell_ranges<T>(
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

int ArrayReadState::read_multiple_fragments_dense_attr_var(
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
    return read_multiple_fragments_dense_attr_var<int>(
               attribute_id, 
               buffer, 
               buffer_size,
               buffer_var, 
               buffer_var_size);
  } else if(coords_type == TILEDB_INT64) {
    return read_multiple_fragments_dense_attr_var<int64_t>(
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
int ArrayReadState::read_multiple_fragments_dense_attr_var(
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
    // Continue copying from the previous unfinished tile
    if(!tile_done_[attribute_id])
      if(copy_cell_ranges_var<T>(
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

    // Prepare the cell ranges for the next tile under investigation, if
    // has not be done already
    if(fragment_cell_pos_ranges_vec_pos_[attribute_id] >= 
       fragment_cell_pos_ranges_vec_.size()) {
      // Get next overlapping tiles
      if(get_next_cell_ranges_dense<T>() != TILEDB_ARS_OK)
        return TILEDB_ARS_ERR;
    }

    // Check if read is done
    if(done_ &&
       fragment_cell_pos_ranges_vec_pos_[attribute_id] == 
       fragment_cell_pos_ranges_vec_.size()) {
      buffer_size = buffer_offset;
      buffer_var_size = buffer_var_offset;
      return TILEDB_ARS_OK;
    }
 
    // Process the heap and copy cells to buffers
    if(copy_cell_ranges_var<T>(
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

int ArrayReadState::read_multiple_fragments_sparse(
    void** buffers,  
    size_t* buffer_sizes) {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  std::vector<int> attribute_ids = array_->attribute_ids();
  int attribute_id_num = attribute_ids.size(); 

  // Write each attribute individually
  int buffer_i = 0;
  int rc;
  for(int i=0; i<attribute_id_num; ++i) {
    if(!array_schema->var_size(attribute_ids[i])) { // FIXED CELLS
      rc = read_multiple_fragments_sparse_attr(
               attribute_ids[i], 
               buffers[buffer_i], 
               buffer_sizes[buffer_i]);

      if(rc != TILEDB_AR_OK)
        break;
      ++buffer_i;
    } else {                                         // VARIABLE-SIZED CELLS
      rc = read_multiple_fragments_sparse_attr_var(
               attribute_ids[i], 
               buffers[buffer_i],       // offsets 
               buffer_sizes[buffer_i],
               buffers[buffer_i+1],     // actual values
               buffer_sizes[buffer_i+1]);

      if(rc != TILEDB_AR_OK)
        break;
      buffer_i += 2;
    }
  }

  return rc;
}

int ArrayReadState::read_multiple_fragments_sparse_attr(
    int attribute_id,
    void* buffer,  
    size_t& buffer_size) {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  int coords_type = array_schema->coords_type();

  // Invoke the proper templated function
  if(coords_type == TILEDB_INT32) {
    return read_multiple_fragments_sparse_attr<int>(
               attribute_id, 
               buffer, 
               buffer_size);
  } else if(coords_type == TILEDB_INT64) {
    return read_multiple_fragments_sparse_attr<int64_t>(
               attribute_id, 
               buffer, 
               buffer_size);
  } else if(coords_type == TILEDB_FLOAT32) {
    return read_multiple_fragments_sparse_attr<float>(
               attribute_id, 
               buffer, 
               buffer_size);
  } else if(coords_type == TILEDB_FLOAT64) {
    return read_multiple_fragments_sparse_attr<double>(
               attribute_id, 
               buffer, 
               buffer_size);
  } else {
    PRINT_ERROR("Cannot read from array; Invalid coordinates type");
    return TILEDB_ARS_ERR;
  }
}

template<class T>
int ArrayReadState::read_multiple_fragments_sparse_attr(
    int attribute_id,
    void* buffer,  
    size_t& buffer_size) {
  // Auxiliary variables
  size_t buffer_offset = 0;

  // Until read is done or there is a buffer overflow 
  for(;;) {
    // Continue copying from the previous unfinished tile
    if(!tile_done_[attribute_id])
      if(copy_cell_ranges<T>(
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

    // Prepare the cell ranges for the next tile under investigation, if
    // has not be done already
    if(fragment_cell_pos_ranges_vec_pos_[attribute_id] >= 
       fragment_cell_pos_ranges_vec_.size()) {
      // Get next overlapping tiles
      if(get_next_cell_ranges_sparse<T>() != TILEDB_ARS_OK)
        return TILEDB_ARS_ERR;
    }

    // Check if read is done
    if(done_ && 
       fragment_cell_pos_ranges_vec_pos_[attribute_id] == 
       fragment_cell_pos_ranges_vec_.size()) {
      buffer_size = buffer_offset;
      return TILEDB_ARS_OK;
    }

    // Process the heap and copy cells to buffers
    if(copy_cell_ranges<T>(
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

int ArrayReadState::read_multiple_fragments_sparse_attr_var(
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
    return read_multiple_fragments_sparse_attr_var<int>(
               attribute_id, 
               buffer, 
               buffer_size,
               buffer_var, 
               buffer_var_size);
  } else if(coords_type == TILEDB_INT64) {
    return read_multiple_fragments_sparse_attr_var<int64_t>(
               attribute_id, 
               buffer, 
               buffer_size,
               buffer_var, 
               buffer_var_size);
  } else if(coords_type == TILEDB_FLOAT32) {
    return read_multiple_fragments_sparse_attr_var<float>(
               attribute_id, 
               buffer, 
               buffer_size,
               buffer_var, 
               buffer_var_size);
  } else if(coords_type == TILEDB_FLOAT64) {
    return read_multiple_fragments_sparse_attr_var<double>(
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
int ArrayReadState::read_multiple_fragments_sparse_attr_var(
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
    // Continue copying from the previous unfinished tile
    if(!tile_done_[attribute_id])
      if(copy_cell_ranges_var<T>(
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

    // Prepare the cell ranges for the next tile under investigation, if
    // has not be done already
    if(fragment_cell_pos_ranges_vec_pos_[attribute_id] >= 
       fragment_cell_pos_ranges_vec_.size()) {
      // Get next overlapping tiles
      if(get_next_cell_ranges_sparse<T>() != TILEDB_ARS_OK)
        return TILEDB_ARS_ERR;
    }

    // Check if read is done
    if(done_ &&
       fragment_cell_pos_ranges_vec_pos_[attribute_id] == 
       fragment_cell_pos_ranges_vec_.size()) {
      buffer_size = buffer_offset;
      buffer_var_size = buffer_var_offset;
      return TILEDB_ARS_OK;
    }
 
    // Process the heap and copy cells to buffers
    if(copy_cell_ranges_var<T>(
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

