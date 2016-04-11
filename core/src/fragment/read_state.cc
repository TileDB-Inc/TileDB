/**
 * @file   read_state.cc
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
 * This file implements the ReadState class.
 */

#include "utils.h"
#include "read_state.h"
#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>




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

#ifdef _TILEDB_USE_MMAP
#  define READ_FROM_FILE read_from_file_with_mmap 
#  define READ_TILE_FROM_FILE_CMP_NONE read_tile_from_file_with_mmap_cmp_none
#  define READ_TILE_FROM_FILE_CMP_GZIP read_tile_from_file_with_mmap_cmp_gzip
#  define READ_TILE_FROM_FILE_VAR_CMP_NONE \
       read_tile_from_file_with_mmap_var_cmp_none
#  define READ_TILE_FROM_FILE_VAR_CMP_GZIP \
       read_tile_from_file_with_mmap_var_cmp_gzip
#else
#  define READ_FROM_FILE read_from_file 
#  define READ_TILE_FROM_FILE_CMP_NONE read_tile_from_file_cmp_none
#  define READ_TILE_FROM_FILE_CMP_GZIP read_tile_from_file_cmp_gzip
#  define READ_TILE_FROM_FILE_VAR_CMP_NONE read_tile_from_file_var_cmp_none
#  define READ_TILE_FROM_FILE_VAR_CMP_GZIP read_tile_from_file_var_cmp_gzip
#endif




/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

ReadState::ReadState(
    const Fragment* fragment, 
    BookKeeping* book_keeping)
    : book_keeping_(book_keeping),
      fragment_(fragment) {
  // For easy reference 
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();
  size_t coords_size = array_schema->coords_size();

  done_ = false;
  fetched_tile_.resize(attribute_num+2);
  overflow_.resize(attribute_num+1);
  last_tile_coords_ = NULL;
  map_addr_.resize(attribute_num+2);
  map_addr_lengths_.resize(attribute_num+2);
  map_addr_compressed_ = NULL;
  map_addr_compressed_length_ = 0;
  map_addr_var_.resize(attribute_num);
  map_addr_var_lengths_.resize(attribute_num);
  search_tile_overlap_subarray_ = malloc(2*coords_size);
  search_tile_pos_ = -1;
  tile_compressed_ = NULL;
  tile_compressed_allocated_size_ = 0;
  tiles_.resize(attribute_num+2);
  tiles_offsets_.resize(attribute_num+2);
  tiles_sizes_.resize(attribute_num+2);
  tiles_var_.resize(attribute_num);
  tiles_var_offsets_.resize(attribute_num);
  tiles_var_sizes_.resize(attribute_num);
  tiles_var_allocated_size_.resize(attribute_num);

  for(int i=0; i<attribute_num; ++i) {
    map_addr_var_[i] = NULL;
    map_addr_var_lengths_[i] = 0;
    tiles_var_[i] = NULL;
    tiles_var_offsets_[i] = 0;
    tiles_var_sizes_[i] = 0;
    tiles_var_allocated_size_[i] = 0;
  }

  for(int i=0; i<attribute_num+1; ++i)
    overflow_[i] = false;

  for(int i=0; i<attribute_num+2; ++i) {
    fetched_tile_[i] = -1;
    map_addr_[i] = NULL;
    map_addr_lengths_[i] = 0;
    tiles_[i] = NULL;
    tiles_offsets_[i] = 0;
    tiles_sizes_[i] = 0;
  }

  compute_tile_search_range();
}

ReadState::~ReadState() { 
  if(last_tile_coords_ != NULL)
    free(last_tile_coords_);

  for(int i=0; i<int(tiles_.size()); ++i) {
    if(map_addr_[i] == NULL && tiles_[i] != NULL)
      free(tiles_[i]);
  }

  for(int i=0; i<int(tiles_var_.size()); ++i) {
    if(map_addr_var_[i] == NULL && tiles_var_[i] != NULL)
      free(tiles_var_[i]);
  }

  if(map_addr_compressed_ == NULL && tile_compressed_ != NULL)
    free(tile_compressed_);

  for(int i=0; i<int(map_addr_.size()); ++i) {
    if(map_addr_[i] != NULL && munmap(map_addr_[i], map_addr_lengths_[i]))
      PRINT_WARNING("Problem in finalizing ReadState; Memory unmap error");
  }

  for(int i=0; i<int(map_addr_var_.size()); ++i) {
    if(map_addr_var_[i] != NULL && 
       munmap(map_addr_var_[i], map_addr_var_lengths_[i])) 
      PRINT_WARNING("Problem in finalizing ReadState; Memory unmap error");
  }

  if(map_addr_compressed_ != NULL &&  
     munmap(map_addr_compressed_, map_addr_compressed_length_))
    PRINT_WARNING("Problem in finalizing ReadState; Memory unmap error");

  if(search_tile_overlap_subarray_ != NULL)
    free(search_tile_overlap_subarray_);
}




/* ****************************** */
/*           ACCESSORS            */
/* ****************************** */

bool ReadState::dense() const {
  return fragment_->dense();
}

bool ReadState::done() const {
  return done_;
}

void ReadState::get_bounding_coords(void* bounding_coords) const {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  size_t coords_size = array_schema->coords_size();
  int64_t pos = search_tile_pos_;
  assert(pos != -1);
  memcpy(bounding_coords, book_keeping_->bounding_coords()[pos], 2*coords_size);
}

bool ReadState::mbr_overlaps_tile() const {
  return mbr_tile_overlap_;
}

bool ReadState::overflow(int attribute_id) const {
  return overflow_[attribute_id];
}




/* ****************************** */
/*           MUTATORS             */
/* ****************************** */

void ReadState::reset_overflow() {
  for(int i=0; i<int(overflow_.size()); ++i)
    overflow_[i] = false;
}




/* ****************************** */
/*             MISC               */
/* ****************************** */

int ReadState::copy_cells(
    int attribute_id,
    int tile_i,
    void* buffer,  
    size_t buffer_size,
    size_t& buffer_offset,
    const CellPosRange& cell_pos_range) {
  // Trivial case
  if(is_empty_attribute(attribute_id))
    return TILEDB_RS_OK;

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  size_t cell_size = array_schema->cell_size(attribute_id);

  // Fetch the attribute tile from disk if necessary
  int compression = array_schema->compression(attribute_id);
  int rc;
  if(compression == TILEDB_GZIP)
    rc = get_tile_from_disk_cmp_gzip(attribute_id, tile_i);
  else
    rc = get_tile_from_disk_cmp_none(attribute_id, tile_i);
  if(rc != TILEDB_RS_OK)
    return TILEDB_RS_ERR;

  // For easy reference
  char* tile = static_cast<char*>(tiles_[attribute_id]);

  // Calculate free space in buffer
  size_t buffer_free_space = buffer_size - buffer_offset; 
  buffer_free_space = (buffer_free_space / cell_size) * cell_size;
  if(buffer_free_space == 0) { // Overflow
    overflow_[attribute_id] = true;
    return TILEDB_RS_OK;
  }

  // Sanity check
  assert(!array_schema->var_size(attribute_id));

  // For each cell position range, copy the respective cells to the buffer
  size_t start_offset, end_offset;
  size_t bytes_left_to_copy, bytes_to_copy;

  // Calculate start and end offset in the tile
  start_offset = cell_pos_range.first * cell_size; 
  end_offset = (cell_pos_range.second + 1) * cell_size - 1;

  // Potentially set the tile offset to the beginning of the current range
  if(tiles_offsets_[attribute_id] < start_offset) 
    tiles_offsets_[attribute_id] = start_offset;
  else if(tiles_offsets_[attribute_id] > end_offset) // This range is written
    return TILEDB_RS_OK;

  // Calculate the total size to copy
  bytes_left_to_copy = end_offset - tiles_offsets_[attribute_id] + 1;
  bytes_to_copy = std::min(bytes_left_to_copy, buffer_free_space);  

  // Copy and update current buffer and tile offsets
  char* buffer_c = static_cast<char*>(buffer);
  if(bytes_to_copy != 0) {
    memcpy(
        buffer_c + buffer_offset, 
        tile + tiles_offsets_[attribute_id], 
        bytes_to_copy);
    buffer_offset += bytes_to_copy;
    tiles_offsets_[attribute_id] += bytes_to_copy; 
    buffer_free_space = buffer_size - buffer_offset;
  }

  // Handle buffer overflow
  if(tiles_offsets_[attribute_id] != end_offset + 1) 
    overflow_[attribute_id] = true;

  // Success
  return TILEDB_RS_OK;
}

int ReadState::copy_cells_var(
    int attribute_id,
    int tile_i,
    void* buffer,  
    size_t buffer_size,
    size_t& buffer_offset,
    void* buffer_var,  
    size_t buffer_var_size,
    size_t& buffer_var_offset,
    const CellPosRange& cell_pos_range) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  size_t cell_size = TILEDB_CELL_VAR_OFFSET_SIZE;

  // Calculate free space in buffer
  size_t buffer_free_space = buffer_size - buffer_offset; 
  buffer_free_space = (buffer_free_space / cell_size) * cell_size;
  size_t buffer_var_free_space = buffer_var_size - buffer_var_offset;

  // Handle overflow
  if(buffer_free_space == 0 || buffer_var_free_space == 0) { // Overflow
    overflow_[attribute_id] = true; 
    return TILEDB_RS_OK;
  }

  // Fetch the attribute tile from disk if necessary
  int compression = array_schema->compression(attribute_id);
  int rc;
  if(compression == TILEDB_GZIP)
    rc = get_tile_from_disk_var_cmp_gzip(attribute_id, tile_i);
  else
    rc = get_tile_from_disk_var_cmp_none(attribute_id, tile_i);
  if(rc != TILEDB_RS_OK)
    return TILEDB_RS_ERR;

  // For easy reference
  char* buffer_c = static_cast<char*>(buffer);
  void* buffer_start = buffer_c + buffer_offset;
  char* buffer_var_c = static_cast<char*>(buffer_var);
  char* tile = static_cast<char*>(tiles_[attribute_id]);
  size_t* tile_s = static_cast<size_t*>(tiles_[attribute_id]);
  char* tile_var = static_cast<char*>(tiles_var_[attribute_id]);

  // Sanity check
  assert(array_schema->var_size(attribute_id));

  // For each cell position range, copy the respective cells to the buffer
  size_t start_offset, end_offset;
  int64_t start_cell_pos, end_cell_pos;
  size_t bytes_left_to_copy, bytes_to_copy, bytes_var_to_copy;

  // Calculate start and end offset in the tile
  start_offset = cell_pos_range.first * cell_size; 
  end_offset = (cell_pos_range.second + 1) * cell_size - 1;

  // Potentially set the tile offset to the beginning of the current range
  if(tiles_offsets_[attribute_id] < start_offset) 
    tiles_offsets_[attribute_id] = start_offset;
  else if(tiles_offsets_[attribute_id] > end_offset) // This range is written
    return TILEDB_RS_OK;

  // Calculate the total size to copy
  bytes_left_to_copy = end_offset - tiles_offsets_[attribute_id] + 1;
  bytes_to_copy = std::min(bytes_left_to_copy, buffer_free_space); 

  // Compute actual bytes to copy
  start_cell_pos = tiles_offsets_[attribute_id] / cell_size;
  end_cell_pos = start_cell_pos + bytes_to_copy/cell_size - 1;
  compute_bytes_to_copy(
      attribute_id,
      start_cell_pos,
      end_cell_pos,
      buffer_free_space,
      buffer_var_free_space,
      bytes_to_copy,
      bytes_var_to_copy);

  // Potentially update tile offset to the beginning of the overlap range
  if(tiles_var_offsets_[attribute_id] < tile_s[start_cell_pos]) 
    tiles_var_offsets_[attribute_id] = tile_s[start_cell_pos];

  // Copy and update current buffer and tile offsets
  buffer_start = buffer_c + buffer_offset;
  if(bytes_to_copy != 0) {
    memcpy(
        buffer_start, 
        tile + tiles_offsets_[attribute_id], 
        bytes_to_copy);
    buffer_offset += bytes_to_copy;
    tiles_offsets_[attribute_id] += bytes_to_copy; 
    buffer_free_space = buffer_size - buffer_offset;

    // Shift variable offsets
    shift_var_offsets(
        buffer_start, 
        end_cell_pos - start_cell_pos + 1, 
        buffer_var_offset); 

    // Copy and update current variable buffer and tile offsets
    memcpy(
        buffer_var_c + buffer_var_offset, 
        tile_var + tiles_var_offsets_[attribute_id], 
        bytes_var_to_copy);
    buffer_var_offset += bytes_var_to_copy;
    tiles_var_offsets_[attribute_id] += bytes_var_to_copy; 
    buffer_var_free_space = buffer_var_size - buffer_var_offset;
  }

  // Check for overflow
  if(tiles_offsets_[attribute_id] != end_offset + 1) 
    overflow_[attribute_id] = true;

  // Entering this if condition implies that the var data in this cell is so large
  // that the allocated buffer cannot hold it
  if(buffer_offset == 0u && bytes_to_copy == 0u) {
    overflow_[attribute_id] = true; 
    return TILEDB_RS_OK;
  }

  // Success
  return TILEDB_RS_OK;
}

template<class T>
int ReadState::get_coords_after(
    const T* coords,
    T* coords_after,
    bool& coords_retrieved) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();
  int dim_num = array_schema->dim_num();
  int64_t cell_num = book_keeping_->cell_num(search_tile_pos_);  
  size_t coords_size = array_schema->coords_size();

  // Fetch the coordinates search tile from disk if necessary
  int compression = array_schema->compression(attribute_num);
  int rc;
  if(compression == TILEDB_GZIP)
    rc = get_tile_from_disk_cmp_gzip(attribute_num+1, search_tile_pos_);
  else
    rc = get_tile_from_disk_cmp_none(attribute_num+1, search_tile_pos_);
  if(rc != TILEDB_RS_OK)
    return TILEDB_RS_ERR;

  // For easy reference
  const T* tile = static_cast<const T*>(tiles_[attribute_num+1]);

  // Compute the cell position at or after the coords
  int64_t coords_after_pos = get_cell_pos_after(coords);

  // Invalid position
  if(coords_after_pos < 0 || coords_after_pos >= cell_num) {
    coords_retrieved = false;
    return TILEDB_RS_OK;
  }

  // Copy result
  memcpy(coords_after, &tile[coords_after_pos*dim_num], coords_size);
  coords_retrieved = true;
 
  // Success
  return TILEDB_RS_OK; 
}

template<class T>
int ReadState::get_enclosing_coords(
    int tile_i,
    const T* target_coords,
    const T* start_coords,
    const T* end_coords,
    T* left_coords,
    T* right_coords,
    bool& left_retrieved,
    bool& right_retrieved,
    bool& target_exists) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();
  int dim_num = array_schema->dim_num();
  size_t coords_size = array_schema->coords_size();

  // Fetch the coordinates search tile from disk if necessary
  int compression = array_schema->compression(attribute_num);
  int rc;
  if(compression == TILEDB_GZIP)
    rc = get_tile_from_disk_cmp_gzip(attribute_num+1, tile_i);
  else
    rc = get_tile_from_disk_cmp_none(attribute_num+1, tile_i);
  if(rc != TILEDB_RS_OK)
    return TILEDB_RS_ERR;

  // For easy reference
  const T* tile = static_cast<const T*>(tiles_[attribute_num+1]);

  // Compute the appropriate cell positions
  int64_t start_pos = get_cell_pos_at_or_after(start_coords);
  int64_t end_pos = get_cell_pos_at_or_before(end_coords);
  int64_t target_pos = get_cell_pos_at_or_before(target_coords);

  // Check if target exists
  if(target_pos >= start_pos && target_pos <= end_pos &&
     !memcmp(target_coords, &tile[target_pos*dim_num], coords_size)) 
    target_exists = true;
  else
    target_exists = false;

  // Calculate left and right pos
  int64_t left_pos = (target_exists) ? target_pos-1 : target_pos; 
  int64_t right_pos = target_pos+1;

  // Copy left if it exists
  if(left_pos >= start_pos && left_pos <= end_pos) {
    memcpy(left_coords, &tile[left_pos*dim_num], coords_size);
    left_retrieved = true;
  } else {
    left_retrieved = false;
  }

  // Copy right if it exists
  if(right_pos >= start_pos && right_pos <= end_pos) {
    memcpy(right_coords, &tile[right_pos*dim_num], coords_size);
    right_retrieved = true;
  } else {
    right_retrieved = false;
  }

  // Success
  return TILEDB_RS_OK;
}

template<class T>
int ReadState::get_fragment_cell_pos_range_sparse(
    const FragmentInfo& fragment_info,
    const T* cell_range, 
    FragmentCellPosRange& fragment_cell_pos_range) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();
  int dim_num = array_schema->dim_num();
  int64_t tile_i = fragment_info.second;

  // Fetch the coordinates search tile from disk if necessary
  int compression = array_schema->compression(attribute_num);
  int rc;
  if(compression == TILEDB_GZIP)
    rc = get_tile_from_disk_cmp_gzip(attribute_num+1, tile_i);
  else
    rc = get_tile_from_disk_cmp_none(attribute_num+1, tile_i);
  if(rc != TILEDB_RS_OK)
    return TILEDB_RS_ERR;

  // Compute the appropriate cell positions
  int64_t start_pos = get_cell_pos_at_or_after(cell_range);
  int64_t end_pos = get_cell_pos_at_or_before(&cell_range[dim_num]);

  // Create the result
  fragment_cell_pos_range.first = fragment_info;
  if(start_pos <= end_pos) // There are results
    fragment_cell_pos_range.second = CellPosRange(start_pos, end_pos);
  else                     // There are NO rsults
    fragment_cell_pos_range.second = CellPosRange(-1, -1);

  // Success
  return TILEDB_RS_OK;
}

template<class T>
int ReadState::get_fragment_cell_ranges_dense(
    int fragment_i,
    FragmentCellRanges& fragment_cell_ranges) {
  // Trivial cases
  if(done_ || !search_tile_overlap_)
    return TILEDB_RS_OK;

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int dim_num = array_schema->dim_num();
  int cell_order = array_schema->cell_order();
  size_t cell_range_size = 2*array_schema->coords_size();
  const T* search_tile_overlap_subarray = 
      static_cast<const T*>(search_tile_overlap_subarray_);
  FragmentInfo fragment_info = FragmentInfo(fragment_i, search_tile_pos_);

  // Contiguous cells, single cell range
  if(search_tile_overlap_ == 1 || 
     search_tile_overlap_ == 3) {
    void* cell_range = malloc(cell_range_size);
    T* cell_range_T = static_cast<T*>(cell_range);
    for(int i=0; i<dim_num; ++i) {
      cell_range_T[i] = search_tile_overlap_subarray[2*i];
      cell_range_T[dim_num + i] = search_tile_overlap_subarray[2*i+1];
    }

    fragment_cell_ranges.push_back(
        FragmentCellRange(fragment_info, cell_range));
  } else { // Non-contiguous cells, multiple ranges
    // Initialize the coordinates at the beginning of the global range
    T* coords = new T[dim_num];
    for(int i=0; i<dim_num; ++i)
      coords[i] = search_tile_overlap_subarray[2*i];

    // Handle the different cell orders
    int i;
    if(cell_order == TILEDB_ROW_MAJOR) {           // ROW
      while(coords[0] <= search_tile_overlap_subarray[1]) {
        // Make a cell range representing a slab       
        void* cell_range = malloc(cell_range_size);
        T* cell_range_T = static_cast<T*>(cell_range);
        for(int i=0; i<dim_num-1; ++i) { 
          cell_range_T[i] = coords[i];
          cell_range_T[dim_num+i] = coords[i];
        }
        cell_range_T[dim_num-1] = search_tile_overlap_subarray[2*(dim_num-1)];
        cell_range_T[2*dim_num-1] = 
            search_tile_overlap_subarray[2*(dim_num-1)+1];

        // Insert the new range into the result vector
        fragment_cell_ranges.push_back(
            FragmentCellRange(fragment_info, cell_range));
 
        // Advance coordinates
        i=dim_num-2;
        ++coords[i];
        while(i > 0 && coords[i] > search_tile_overlap_subarray[2*i+1]) {
          coords[i] = search_tile_overlap_subarray[2*i];
          ++coords[--i];
        } 
      }
    } else if(cell_order == TILEDB_COL_MAJOR) { // COLUMN
      while(coords[dim_num-1] <=  
            search_tile_overlap_subarray[2*(dim_num-1)+1]) {
        // Make a cell range representing a slab       
        void* cell_range = malloc(cell_range_size);
        T* cell_range_T = static_cast<T*>(cell_range);
        for(int i=dim_num-1; i>0; --i) { 
          cell_range_T[i] = coords[i];
          cell_range_T[dim_num+i] = coords[i];
        }
        cell_range_T[0] = search_tile_overlap_subarray[0];
        cell_range_T[dim_num] = search_tile_overlap_subarray[1];

        // Insert the new range into the result vector
        fragment_cell_ranges.push_back(
            FragmentCellRange(fragment_info, cell_range));
 
        // Advance coordinates
        i=1;
        ++coords[i];
        while(i <dim_num-1 && coords[i] > search_tile_overlap_subarray[2*i+1]) {
          coords[i] = search_tile_overlap_subarray[2*i];
          ++coords[++i];
        } 
      }
    } else {
      assert(0);
    }
    
    // Clean up
    delete [] coords;
  }

  // Success
  return TILEDB_RS_OK;
}

template<class T>
int ReadState::get_fragment_cell_ranges_sparse(
    int fragment_i,
    FragmentCellRanges& fragment_cell_ranges) {
  // Trivial cases
  if(done_ || !search_tile_overlap_ || !mbr_tile_overlap_)
    return TILEDB_RS_OK;

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int dim_num = array_schema->dim_num();
  const T* search_tile_overlap_subarray = 
      static_cast<const T*>(search_tile_overlap_subarray_);

  // Create start and end coordinates for the overlap
  T* start_coords = new T[dim_num];
  T* end_coords = new T[dim_num];
  for(int i=0; i<dim_num; ++i) {
    start_coords[i] = search_tile_overlap_subarray[2*i];
    end_coords[i] = search_tile_overlap_subarray[2*i+1];
  }
  
  // Get fragment cell ranges inside range [start_coords, end_coords]
  int rc = get_fragment_cell_ranges_sparse(
               fragment_i,
               start_coords,
               end_coords,
               fragment_cell_ranges); 

  // Clean up
  delete [] start_coords;
  delete [] end_coords;

  // Return
  return rc;
}

template<class T>
int ReadState::get_fragment_cell_ranges_sparse(
    int fragment_i,
    const T* start_coords,
    const T* end_coords,
    FragmentCellRanges& fragment_cell_ranges) {
  // Sanity checks
  assert(search_tile_pos_ >= tile_search_range_[0] &&
         search_tile_pos_ <= tile_search_range_[1]);
  assert(search_tile_overlap_);

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();
  int dim_num = array_schema->dim_num();
  size_t coords_size = array_schema->coords_size();
  const T* subarray = static_cast<const T*>(fragment_->array()->subarray());

  // Handle full overlap
  if(search_tile_overlap_ == 1) {
    FragmentCellRange fragment_cell_range;
    fragment_cell_range.first = FragmentInfo(fragment_i, search_tile_pos_); 
    fragment_cell_range.second = malloc(2*coords_size);
    T* cell_range = static_cast<T*>(fragment_cell_range.second);
    memcpy(cell_range, start_coords, coords_size);
    memcpy(&cell_range[dim_num], end_coords, coords_size);
    fragment_cell_ranges.push_back(fragment_cell_range); 
    return TILEDB_RS_OK;
  }

  // Fetch the coordinates search tile from disk if necessary
  int compression = array_schema->compression(attribute_num);
  int rc;
  if(compression == TILEDB_GZIP)
    rc = get_tile_from_disk_cmp_gzip(attribute_num+1, search_tile_pos_);
  else
    rc = get_tile_from_disk_cmp_none(attribute_num+1, search_tile_pos_);
  if(rc != TILEDB_RS_OK)
    return TILEDB_RS_ERR;

  // Get cell positions for the cell range
  int64_t start_pos = get_cell_pos_at_or_after(start_coords); 
  int64_t end_pos = get_cell_pos_at_or_before(end_coords); 

  // Get the cell ranges
  const T* cell;
  const T* tile = static_cast<const T*>(tiles_[attribute_num+1]);
  int64_t current_start_pos, current_end_pos = -2; 
  for(int64_t i=start_pos; i<=end_pos; ++i) {
     cell = &tile[i*dim_num];
     if(cell_in_subarray<T>(cell, subarray, dim_num)) {
      if(i-1 == current_end_pos) { // The range is expanded
       ++current_end_pos;
      } else {                     // A new range starts
        current_start_pos = i;
        current_end_pos = i;
      } 
    } else {
      if(i-1 == current_end_pos) { // The range needs to be added to the list
        FragmentCellRange fragment_cell_range;
        fragment_cell_range.first = FragmentInfo(fragment_i, search_tile_pos_);
        fragment_cell_range.second = malloc(2*coords_size);
        T* cell_range = static_cast<T*>(fragment_cell_range.second);
        memcpy(cell_range, &tile[current_start_pos*dim_num], coords_size);
        memcpy(
            &cell_range[dim_num], 
            &tile[current_end_pos*dim_num], 
            coords_size);
        fragment_cell_ranges.push_back(fragment_cell_range);
        current_end_pos = -2; // This indicates that there is no active range
      }
    }
  } 

  // Add last cell range
  if(current_end_pos != -2) {
    FragmentCellRange fragment_cell_range;
    fragment_cell_range.first = FragmentInfo(fragment_i, search_tile_pos_);
    fragment_cell_range.second = malloc(2*coords_size);
    T* cell_range = static_cast<T*>(fragment_cell_range.second);
    memcpy(cell_range, &tile[current_start_pos*dim_num], coords_size);
    memcpy(&cell_range[dim_num], &tile[current_end_pos*dim_num], coords_size);
    fragment_cell_ranges.push_back(fragment_cell_range);
  }

  // Success
  return TILEDB_RS_OK;
}

template<class T> 
void ReadState::get_next_overlapping_tile_dense(const T* tile_coords) {
  // Trivial case
  if(done_)
    return;

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int dim_num = array_schema->dim_num();
  const T* tile_extents = static_cast<const T*>(array_schema->tile_extents());
  const T* array_domain = static_cast<const T*>(array_schema->domain());
  const T* subarray = static_cast<const T*>(fragment_->array()->subarray());
  const T* domain = static_cast<const T*>(book_keeping_->domain());
  const T* non_empty_domain = 
      static_cast<const T*>(book_keeping_->non_empty_domain());
  
  // Compute the tile subarray
  T* tile_subarray = new T[2*dim_num];
  array_schema->get_tile_subarray(tile_coords, tile_subarray); 

  // Compute overlap of tile subarray with non-empty fragment domain 
  T* tile_domain_overlap_subarray = new T[2*dim_num];
  bool tile_domain_overlap = 
        array_schema->subarray_overlap(
            tile_subarray,
            non_empty_domain, 
            tile_domain_overlap_subarray);

  if(!tile_domain_overlap) {  // No overlap with the input tile
    search_tile_overlap_ = 0;
  } else {                    // Overlap with the input tile
    // Find the search tile position
    T* tile_coords_norm = new T[dim_num];
    for(int i=0; i<dim_num; ++i)
      tile_coords_norm[i] = 
          tile_coords[i] - (domain[2*i]-array_domain[2*i]) / tile_extents[i]; 
    search_tile_pos_ = array_schema->get_tile_pos(domain, tile_coords_norm);
    delete [] tile_coords_norm;

    // Compute overlap of the query subarray with tile
    T* query_tile_overlap_subarray = new T[2*dim_num];
    array_schema->subarray_overlap(
         subarray,
         tile_subarray, 
         query_tile_overlap_subarray);

    // Compute the overlap of the previous results with the non-empty domain 
    search_tile_overlap_ = 
        array_schema->subarray_overlap(
            query_tile_overlap_subarray,
            tile_domain_overlap_subarray, 
            static_cast<T*>(search_tile_overlap_subarray_));

    // Clean up
    delete [] query_tile_overlap_subarray;
  } 

  // Clean up
  delete [] tile_subarray;
  delete [] tile_domain_overlap_subarray;
}

template<class T>
void ReadState::get_next_overlapping_tile_sparse() {
  // Trivial case
  if(done_)
    return;

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  const std::vector<void*>& mbrs = book_keeping_->mbrs();
  const T* subarray = static_cast<const T*>(fragment_->array()->subarray());

  // Update the search tile position
  if(search_tile_pos_ == -1)
     search_tile_pos_ = tile_search_range_[0];
  else
    ++search_tile_pos_;

  // Find the position to the next overlapping tile with the query range
  for(;;) {
    // No overlap - exit
    if(search_tile_pos_ > tile_search_range_[1]) {
      done_ = true;
      return;
    }

    const T* mbr = static_cast<const T*>(mbrs[search_tile_pos_]);
    search_tile_overlap_ = 
        array_schema->subarray_overlap(
            subarray,
            mbr, 
            static_cast<T*>(search_tile_overlap_subarray_));

  if(!search_tile_overlap_)
    ++search_tile_pos_;
  else
    return;
  }
}

template<class T> 
void ReadState::get_next_overlapping_tile_sparse(
    const T* tile_coords) {
  // Trivial case
  if(done_)
    return;

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int dim_num = array_schema->dim_num();
  size_t coords_size = array_schema->coords_size();
  const std::vector<void*>& mbrs = book_keeping_->mbrs();
  const T* subarray = static_cast<const T*>(fragment_->array()->subarray());

  // Compute the tile subarray
  T* tile_subarray = new T[2*dim_num];
  T* mbr_tile_overlap_subarray = new T[2*dim_num];
  T* tile_subarray_end = new T[dim_num];
  array_schema->get_tile_subarray(tile_coords, tile_subarray); 
  for(int i=0; i<dim_num; ++i)
    tile_subarray_end[i] = tile_subarray[2*i+1];

  // Update the search tile position
  if(search_tile_pos_ == -1)
     search_tile_pos_ = tile_search_range_[0];
 
  // Reset overlaps
  search_tile_overlap_ = 0;
  mbr_tile_overlap_ = 0;

  // Check against last coordinates
  if(last_tile_coords_ == NULL) {
    last_tile_coords_ = malloc(coords_size);
    memcpy(last_tile_coords_, tile_coords, coords_size); 
  } else {
    if(!memcmp(last_tile_coords_, tile_coords, coords_size)) {
      // Advance only if the MBR does not exceed the tile
      const T* bounding_coords = 
          static_cast<const T*>(
              book_keeping_->bounding_coords()[search_tile_pos_]);
      if(array_schema->tile_cell_order_cmp(
             &bounding_coords[dim_num], 
             tile_subarray_end) <= 0) {
        ++search_tile_pos_;
      } else {
        return;
      }
    } else { 
      memcpy(last_tile_coords_, tile_coords, coords_size);
    }
  }

  // Find the position to the next overlapping tile with the input tile
  for(;;) {
    // No overlap - exit
    if(search_tile_pos_ > tile_search_range_[1]) {
      done_ = true;
      break;
    }

    // Get overlap between MBR and tile subarray
    const T* mbr = static_cast<const T*>(mbrs[search_tile_pos_]);
    mbr_tile_overlap_ = 
        array_schema->subarray_overlap(
            tile_subarray,
            mbr, 
            mbr_tile_overlap_subarray);

    // No overlap with the tile
    if(!mbr_tile_overlap_) {
      // Check if we need to break or continue
      const T* bounding_coords = 
          static_cast<const T*>(
              book_keeping_->bounding_coords()[search_tile_pos_]);
      if(array_schema->tile_cell_order_cmp(
             &bounding_coords[dim_num], 
             tile_subarray_end) > 0) {
        break;
      } else {
        ++search_tile_pos_;
        continue;
      }
    }
  
    // Get overlap of MBR with the query inside the tile subarray
    search_tile_overlap_ = 
        array_schema->subarray_overlap(
            subarray,
            mbr_tile_overlap_subarray, 
            static_cast<T*>(search_tile_overlap_subarray_));

    // Update the search tile overlap if necessary
    if(search_tile_overlap_) {
      // The overlap is full only when both the MBR-tile and MBR-tile-subarray
      // overlaps are full 
      search_tile_overlap_ = (mbr_tile_overlap_ == 1 && 
                              search_tile_overlap_ == 1) ? 1 : 2;
    }

    // The MBR overlaps with the tile. Regardless of overlap with the
    // query in the tile, break.
    break;
  }

  // Clean up
  delete [] tile_subarray;
  delete [] tile_subarray_end;
  delete [] mbr_tile_overlap_subarray;
}




/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

void ReadState::compute_bytes_to_copy(
    int attribute_id,
    int64_t start_cell_pos,
    int64_t& end_cell_pos,
    size_t buffer_free_space,
    size_t buffer_var_free_space,
    size_t& bytes_to_copy,
    size_t& bytes_var_to_copy) const {
  // Trivial case
  if(buffer_free_space == 0 || buffer_var_free_space == 0) {
    bytes_to_copy = 0;
    bytes_var_to_copy = 0;
    return;
  }

  // Calculate number of cells in the current tile for this attribute
  int64_t cell_num = book_keeping_->cell_num(fetched_tile_[attribute_id]);  

  // Calculate bytes to copy from the variable tile
  const size_t* tile = static_cast<const size_t*>(tiles_[attribute_id]);
  if(end_cell_pos + 1 < cell_num) 
    bytes_var_to_copy = tile[end_cell_pos + 1] - tile[start_cell_pos];
  else 
    bytes_var_to_copy = tiles_var_sizes_[attribute_id] - tile[start_cell_pos];

  // If bytes do not fit in variable buffer, we need to adjust
  if(bytes_var_to_copy > buffer_var_free_space) {
    // Perform binary search
    int64_t min = start_cell_pos + 1;
    int64_t max = end_cell_pos;
    int64_t med;
    // Invariants:
    // (tile[min-1] - tile[start_cell_pos]) < buffer_var_free_space AND
    // (tile[max+1] - tile[start_cell_pos]) > buffer_var_free_space
    while(min <= max) {
      med = min + ((max - min) / 2);

      // Calculate variable bytes to copy
      bytes_var_to_copy = tile[med] - tile[start_cell_pos];

      // Check condition
      if(bytes_var_to_copy > buffer_var_free_space) 
        max = med-1;
      else if(bytes_var_to_copy < buffer_var_free_space)
        min = med+1;
      else   
        break; 
    }

    // Determine the end position of the range
    int64_t tmp_end = -1;
    if(min > max)
      tmp_end = min - 2 ;
    else
      tmp_end = med - 1;

    end_cell_pos = std::max(tmp_end, start_cell_pos-1);

    // Update variable bytes to copy
    bytes_var_to_copy = tile[end_cell_pos + 1] - tile[start_cell_pos];
  }

  // Update bytes to copy
  bytes_to_copy = 
      (end_cell_pos - start_cell_pos + 1) * TILEDB_CELL_VAR_OFFSET_SIZE;

  // Sanity checks
  assert(bytes_to_copy <= buffer_free_space);
  assert(bytes_var_to_copy <= buffer_var_free_space);
}

void ReadState::compute_tile_search_range() {
   // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int coords_type = array_schema->coords_type();

  // Applicable only to sparse fragments
  if(fragment_->dense())
    return;

  // Invoke the proper templated function
  if(coords_type == TILEDB_INT32) {
    compute_tile_search_range<int>();
  } else if(coords_type == TILEDB_INT64) {
    compute_tile_search_range<int64_t>();
  } else if(coords_type == TILEDB_FLOAT32) {
    compute_tile_search_range<float>();
  } else if(coords_type == TILEDB_FLOAT64) {
    compute_tile_search_range<double>();
  } else {
    // The code should never reach here
    assert(0);
  } 
}

template<class T>
void ReadState::compute_tile_search_range() {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int cell_order = array_schema->cell_order();

  // Initialize the tile search range
  if(cell_order == TILEDB_HILBERT)  // HILBERT CELL ORDER
    compute_tile_search_range_hil<T>();
  else                              // COLUMN-  OR ROW-MAJOR 
    compute_tile_search_range_col_or_row<T>();

  // Handle no overlap
  if(tile_search_range_[0] == -1 ||
     tile_search_range_[1] == -1) 
    done_ = true; 
  
}

template<class T>
void ReadState::compute_tile_search_range_col_or_row() {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int dim_num = array_schema->dim_num();
  const T* subarray = static_cast<const T*>(fragment_->array()->subarray());
  int64_t tile_num = book_keeping_->tile_num();
  const std::vector<void*>& bounding_coords = 
      book_keeping_->bounding_coords();

  // Calculate subarray coordinates
  T* subarray_min_coords = new T[dim_num];
  T* subarray_max_coords = new T[dim_num];
  for(int i=0; i<dim_num; ++i) {
    subarray_min_coords[i] = subarray[2*i]; 
    subarray_max_coords[i] = subarray[2*i+1]; 
  }

  // --- Find the start tile in search range

  // Perform binary search
  int64_t min = 0;
  int64_t max = tile_num - 1;
  int64_t med;
  const T* tile_start_coords;
  const T* tile_end_coords;
  while(min <= max) {
    med = min + ((max - min) / 2);

    // Get info for bounding coordinates
    tile_start_coords = static_cast<const T*>(bounding_coords[med]);
    tile_end_coords = &(static_cast<const T*>(bounding_coords[med])[dim_num]);

    // Calculate precedence
    if(array_schema->tile_cell_order_cmp(
           subarray_min_coords,
           tile_start_coords) < 0) {   // Subarray min precedes MBR
      max = med-1;
    } else if(array_schema->tile_cell_order_cmp(
           subarray_min_coords,
           tile_end_coords) > 0) {     // Subarray min succeeds MBR
      min = med+1;
    } else {                           // Subarray min in MBR
      break; 
    }
  }

  bool is_unary = is_unary_subarray(subarray, dim_num);

  // Determine the start position of the range
  if(max < min)    // Subarray min precedes the tile at position min  
    tile_search_range_[0] = (is_unary) ? -1 : min;  
  else             // Subarray min included in a tile
    tile_search_range_[0] = med;

  if(is_unary) {  // Unary range
    // The end position is the same as the start
    tile_search_range_[1] = tile_search_range_[0];
  } else { // Need to find the end position
    // --- Finding the end tile in search range

    // Perform binary search
    min = 0;
    max = tile_num - 1;
    while(min <= max) {
      med = min + ((max - min) / 2);

      // Get info for bounding coordinates
      tile_start_coords = static_cast<const T*>(bounding_coords[med]);
      tile_end_coords = &(static_cast<const T*>(bounding_coords[med])[dim_num]);
     
      // Calculate precedence
      if(array_schema->tile_cell_order_cmp(
             subarray_max_coords,
             tile_start_coords) < 0) {   // Subarray max precedes MBR
        max = med-1;
      } else if(array_schema->tile_cell_order_cmp(
             subarray_max_coords,
             tile_end_coords) > 0) {     // Subarray max succeeds MBR
        min = med+1;
      } else {                           // Subarray max in MBR
        break; 
      }
    }

    // Determine the start position of the range
    if(max < min)    // Subarray max succeeds the tile at position max 
      tile_search_range_[1] = max;     
    else             // Subarray max included in a tile
      tile_search_range_[1] = med;
  }

  // No overlap
  if(tile_search_range_[1] < tile_search_range_[0]) {
    tile_search_range_[0] = -1;
    tile_search_range_[1] = -1;
  }

  // Clean up
  delete [] subarray_min_coords;
  delete [] subarray_max_coords;
}

template<class T>
void ReadState::compute_tile_search_range_hil() {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int dim_num = array_schema->dim_num();
  const T* subarray = static_cast<const T*>(fragment_->array()->subarray());
  int64_t tile_num = book_keeping_->tile_num();

  if(is_unary_subarray(subarray, dim_num)) {  // Unary range
    // For easy reference
    const std::vector<void*>& bounding_coords = 
        book_keeping_->bounding_coords();

    // Calculate range coordinates
    T* subarray_coords = new T[dim_num];
    for(int i=0; i<dim_num; ++i)
      subarray_coords[i] = subarray[2*i]; 

    // Perform binary search
    int64_t min = 0;
    int64_t max = tile_num - 1;
    int64_t med;
    const T* tile_start_coords;
    const T* tile_end_coords;
    while(min <= max) {
      med = min + ((max - min) / 2);

      // Get info for bounding coordinates
      tile_start_coords = static_cast<const T*>(bounding_coords[med]);
      tile_end_coords = &(static_cast<const T*>(bounding_coords[med])[dim_num]);
     
      // Calculate precedence
      if(array_schema->tile_cell_order_cmp(
             subarray_coords,
             tile_start_coords) < 0) {   // Unary subarray precedes MBR
        max = med-1;
      } else if(array_schema->tile_cell_order_cmp(
             subarray_coords,
             tile_end_coords) > 0) {     // Unary subarray succeeds MBR
        min = med+1;
      } else {                           // Unary subarray inside MBR
        break; 
      }
    }

    // Determine the start position of the range
    if(max < min)    // Unary subarray not inside a tile 
      tile_search_range_[0] = -1; 
    else             // Unary subarray inside a tile
      tile_search_range_[0] = med;

    // For unary ranges, start and end positions are the same
    tile_search_range_[1] = tile_search_range_[0];

    // Clean up
    delete [] subarray_coords;
  }  else {                            // Non-unary range
    if(book_keeping_->tile_num() > 0) {
      tile_search_range_[0] = 0;
      tile_search_range_[1] = book_keeping_->tile_num() - 1;
    } else {
      tile_search_range_[0] = -1;
      tile_search_range_[1] = -1;
    }
  }
} 

template<class T>
int64_t ReadState::get_cell_pos_after(const T* coords) const {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();
  int dim_num = array_schema->dim_num();
  const T* tile = static_cast<const T*>(tiles_[attribute_num+1]);
  int64_t cell_num = book_keeping_->cell_num(fetched_tile_[attribute_num+1]);  

  // Perform binary search to find the position of coords in the tile
  int64_t min = 0;
  int64_t max = cell_num - 1;
  int64_t med;
  int cmp;
  while(min <= max) {
    med = min + ((max - min) / 2);

    // Update search range
    cmp = array_schema->tile_cell_order_cmp<T>(coords, &tile[med*dim_num]); 
    if(cmp < 0) 
      max = med-1;
    else if(cmp > 0)  
      min = med+1;
    else       
      break; 
  }

  // Return
  if(max < min)    
    return min;       // After
  else             
    return med + 1;   // After (med is at)
}

template<class T>
int64_t ReadState::get_cell_pos_at_or_after(const T* coords) const {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();
  int dim_num = array_schema->dim_num();
  const T* tile = static_cast<const T*>(tiles_[attribute_num+1]);
  int64_t cell_num = book_keeping_->cell_num(fetched_tile_[attribute_num+1]);  

  // Perform binary search to find the position of coords in the tile
  int64_t min = 0;
  int64_t max = cell_num - 1;
  int64_t med;
  int cmp;
  while(min <= max) {
    med = min + ((max - min) / 2);

    // Update search range
    cmp = array_schema->tile_cell_order_cmp<T>(coords, &tile[med*dim_num]); 
    if(cmp < 0) 
      max = med-1;
    else if(cmp > 0)  
      min = med+1;
    else       
      break; 
  }

  // Return
  if(max < min)    
    return min;   // After
  else             
    return med;   // At
}

template<class T>
int64_t ReadState::get_cell_pos_at_or_before(const T* coords) const {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();
  int dim_num = array_schema->dim_num();
  const T* tile = static_cast<const T*>(tiles_[attribute_num+1]);
  int64_t cell_num = book_keeping_->cell_num(fetched_tile_[attribute_num+1]);  

  // Perform binary search to find the position of coords in the tile
  int64_t min = 0;
  int64_t max = cell_num - 1;
  int64_t med;
  int cmp;
  while(min <= max) {
    med = min + ((max - min) / 2);

    // Update search range
    cmp = array_schema->tile_cell_order_cmp<T>(coords, &tile[med*dim_num]); 
    if(cmp < 0) 
      max = med-1;
    else if(cmp > 0)  
      min = med+1;
    else       
      break; 
  }

  // Return
  if(max < min)    
    return max;   // Before
  else             
    return med;   // At
}

int ReadState::get_tile_from_disk_cmp_gzip(int attribute_id, int64_t tile_i) {
  // Return if the tile has already been fetched
  if(tile_i == fetched_tile_[attribute_id])
    return TILEDB_RS_OK;

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();

  // To handle the special case of the search tile
  // The real attribute id corresponds to an actual attribute or coordinates 
  int attribute_id_real = 
      (attribute_id == attribute_num+1) ? attribute_num : attribute_id;

  // For easy reference
  size_t cell_size = array_schema->cell_size(attribute_id_real);
  size_t full_tile_size = fragment_->tile_size(attribute_id_real);
  int64_t cell_num = book_keeping_->cell_num(tile_i);  
  size_t tile_size = cell_num * cell_size; 
  const std::vector<std::vector<off_t> >& tile_offsets = 
      book_keeping_->tile_offsets(); 
  int64_t tile_num = book_keeping_->tile_num();

  // Allocate space for the tile if needed
  if(tiles_[attribute_id] == NULL) 
    tiles_[attribute_id] = malloc(full_tile_size);

  // Prepare attribute file name
  std::string filename = fragment_->fragment_name() + "/" +
                         array_schema->attribute(attribute_id_real) +
                         TILEDB_FILE_SUFFIX;

  // Find file offset where the tile begins
  off_t file_offset = tile_offsets[attribute_id_real][tile_i];
  off_t file_size = ::file_size(filename);
  size_t tile_compressed_size = 
      (tile_i == tile_num-1) 
          ? file_size - tile_offsets[attribute_id_real][tile_i] 
          : tile_offsets[attribute_id_real][tile_i+1] - 
            tile_offsets[attribute_id_real][tile_i];

  // Read tile from file
  if(READ_TILE_FROM_FILE_CMP_GZIP(
         attribute_id, 
         file_offset, 
         tile_compressed_size) != TILEDB_RS_OK)
    return TILEDB_RS_ERR;

  // Decompress tile 
  size_t gunzip_out_size;
  if(gunzip(
         static_cast<unsigned char*>(tile_compressed_), 
         tile_compressed_size, 
         static_cast<unsigned char*>(tiles_[attribute_id]),
         full_tile_size,
         gunzip_out_size) != TILEDB_UT_OK)
    return TILEDB_RS_ERR;

  // Sanity check
  assert(gunzip_out_size == tile_size);

  // Set the tile size
  tiles_sizes_[attribute_id] = tile_size;

  // Set tile offset
  tiles_offsets_[attribute_id] = 0;

  // Mark as fetched
  fetched_tile_[attribute_id] = tile_i;
  
  // Success
  return TILEDB_RS_OK;
}

int ReadState::get_tile_from_disk_cmp_none(int attribute_id, int64_t tile_i) {
  // Return if the tile has already been fetched
  if(tile_i == fetched_tile_[attribute_id])
    return TILEDB_RS_OK;

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();

  // To handle the special case of the search tile
  // The real attribute id corresponds to an actual attribute or coordinates 
  int attribute_id_real = 
      (attribute_id == attribute_num+1) ? attribute_num : attribute_id;

  // For easy reference
  size_t cell_size = array_schema->cell_size(attribute_id_real);
  size_t full_tile_size = fragment_->tile_size(attribute_id_real);
  int64_t cell_num = book_keeping_->cell_num(tile_i);  
  size_t tile_size = cell_num * cell_size; 

  // Find file offset where the tile begins
  off_t file_offset = tile_i * full_tile_size;

  // Read tile from file
  if(READ_TILE_FROM_FILE_CMP_NONE(
         attribute_id, 
         file_offset, 
         tile_size) != TILEDB_RS_OK)
    return TILEDB_RS_ERR;

  // Set the tile size
  tiles_sizes_[attribute_id] = tile_size;

  // Set tile offset
  tiles_offsets_[attribute_id] = 0;

  // Mark as fetched
  fetched_tile_[attribute_id] = tile_i;
  
  // Success
  return TILEDB_RS_OK;
}

int ReadState::get_tile_from_disk_var_cmp_gzip(
    int attribute_id, 
    int64_t tile_i) {
  // Return if the tile has already been fetched
  if(tile_i == fetched_tile_[attribute_id])
    return TILEDB_RS_OK;

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();

  // Sanity check
  assert(attribute_id < attribute_num && array_schema->var_size(attribute_id));

  // For easy reference
  size_t cell_size = TILEDB_CELL_VAR_OFFSET_SIZE;
  size_t full_tile_size = fragment_->tile_size(attribute_id);
  int64_t cell_num = book_keeping_->cell_num(tile_i); 
  size_t tile_size = cell_num * cell_size;
  const std::vector<std::vector<off_t> >& tile_offsets = 
      book_keeping_->tile_offsets(); 
  const std::vector<std::vector<off_t> >& tile_var_offsets = 
      book_keeping_->tile_var_offsets(); 
  int64_t tile_num = book_keeping_->tile_num();

  // ========== Get tile with variable cell offsets ========== //

  // Prepare attribute file name
  std::string filename = fragment_->fragment_name() + "/" +
             array_schema->attribute(attribute_id) +
             TILEDB_FILE_SUFFIX;

  // Find file offset where the tile begins
  off_t file_offset = tile_offsets[attribute_id][tile_i];
  off_t file_size = ::file_size(filename);
  size_t tile_compressed_size = 
      (tile_i == tile_num-1) ? file_size - tile_offsets[attribute_id][tile_i]
                             : tile_offsets[attribute_id][tile_i+1] - 
                               tile_offsets[attribute_id][tile_i];

  // Allocate space for the tile if needed
  if(tiles_[attribute_id] == NULL) 
    tiles_[attribute_id] = malloc(full_tile_size);

  // Read tile from file
  if(READ_TILE_FROM_FILE_CMP_GZIP(
         attribute_id, 
         file_offset, 
         tile_compressed_size) != TILEDB_RS_OK)
    return TILEDB_RS_ERR;

  // Decompress tile 
  size_t gunzip_out_size;
  if(gunzip(
         static_cast<unsigned char*>(tile_compressed_), 
         tile_compressed_size, 
         static_cast<unsigned char*>(tiles_[attribute_id]),
         tile_size,
         gunzip_out_size) != TILEDB_UT_OK)
    return TILEDB_RS_ERR;

  // Sanity check
  assert(gunzip_out_size == tile_size);

  // Set the tile size
  tiles_sizes_[attribute_id] = tile_size;

  // Update tile offset
  tiles_offsets_[attribute_id] = 0;

  // ========== Get variable tile ========== //

  // Prepare variable attribute file name
  filename = fragment_->fragment_name() + "/" +
             array_schema->attribute(attribute_id) + "_var" +
             TILEDB_FILE_SUFFIX;

  // Calculate offset and compressed tile size
  file_offset = tile_var_offsets[attribute_id][tile_i];
  file_size = ::file_size(filename);
  tile_compressed_size = 
      (tile_i == tile_num-1) ? file_size-tile_var_offsets[attribute_id][tile_i]
                          : tile_var_offsets[attribute_id][tile_i+1] - 
                            tile_var_offsets[attribute_id][tile_i];

  // Get size of decompressed tile
  size_t tile_var_size = book_keeping_->tile_var_sizes()[attribute_id][tile_i];

  //Non-empty tile, decompress
  if(tile_var_size > 0u) {
    // Potentially allocate space for buffer
    if(tiles_var_[attribute_id] == NULL) {
      tiles_var_[attribute_id] = malloc(tile_var_size);
      tiles_var_allocated_size_[attribute_id] = tile_var_size;
    }

    // Potentially expand buffer
    if(tile_var_size > tiles_var_allocated_size_[attribute_id]) {
      tiles_var_[attribute_id] = 
          realloc(tiles_var_[attribute_id], tile_var_size);
      tiles_var_allocated_size_[attribute_id] = tile_var_size;
    }

    // Read tile from file
    if(READ_TILE_FROM_FILE_VAR_CMP_GZIP(
          attribute_id, 
          file_offset, 
          tile_compressed_size) != TILEDB_RS_OK)
      return TILEDB_RS_ERR;

    // Decompress tile 
    if(gunzip(
          static_cast<unsigned char*>(tile_compressed_), 
          tile_compressed_size, 
          static_cast<unsigned char*>(tiles_var_[attribute_id]),
          tile_var_size,
          gunzip_out_size) != TILEDB_UT_OK)
      return TILEDB_RS_ERR;

    // Sanity check
    assert(gunzip_out_size == tile_var_size);
  }

  // Set the variable tile size
  tiles_var_sizes_[attribute_id] = tile_var_size; 

  // Set the variable tile offset
  tiles_var_offsets_[attribute_id] = 0;

  // Shift variable cell offsets
  shift_var_offsets(attribute_id);

  // Mark as fetched
  fetched_tile_[attribute_id] = tile_i;

  // Success
  return TILEDB_RS_OK;
}

int ReadState::get_tile_from_disk_var_cmp_none(
    int attribute_id, 
    int64_t tile_i) {
  // Return if the tile has already been fetched
  if(tile_i == fetched_tile_[attribute_id])
    return TILEDB_RS_OK;

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();

  // Sanity check
  assert(attribute_id < attribute_num && array_schema->var_size(attribute_id));

  // For easy reference
  size_t full_tile_size = fragment_->tile_size(attribute_id);
  int64_t cell_num = book_keeping_->cell_num(tile_i); 
  size_t tile_size = cell_num * TILEDB_CELL_VAR_OFFSET_SIZE;
  int64_t tile_num = book_keeping_->tile_num();
  off_t file_offset = tile_i * full_tile_size;

  // Read tile from file
  if(READ_TILE_FROM_FILE_CMP_NONE(
         attribute_id, 
         file_offset, 
         tile_size) != TILEDB_RS_OK)
    return TILEDB_RS_ERR;

  // Set tile size
  tiles_sizes_[attribute_id] = tile_size;

  // Calculate the start and end offsets for the variable-sized tile,
  // as well as the variable tile size 
  const size_t* tile_s = static_cast<const size_t*>(tiles_[attribute_id]);
  off_t start_tile_var_offset = tile_s[0]; 
  off_t end_tile_var_offset;
  size_t tile_var_size;
  std::string filename = 
        fragment_->fragment_name() + "/" +
        array_schema->attribute(attribute_id) + 
        TILEDB_FILE_SUFFIX;

  if(tile_i != tile_num - 1) { // Not the last tile
    if(read_from_file(
           filename, file_offset + full_tile_size, 
           &end_tile_var_offset, 
           TILEDB_CELL_VAR_OFFSET_SIZE) != TILEDB_UT_OK)
      return TILEDB_RS_ERR;
    tile_var_size = end_tile_var_offset - tile_s[0];
  } else {                  // Last tile
    // Prepare variable attribute file name
    std::string filename = 
        fragment_->fragment_name() + "/" +
        array_schema->attribute(attribute_id) + "_var" +
        TILEDB_FILE_SUFFIX;
    tile_var_size = file_size(filename) - tile_s[0];
  }

  // Read tile from file
  if(READ_TILE_FROM_FILE_VAR_CMP_NONE(
         attribute_id, 
         start_tile_var_offset, 
         tile_var_size) != TILEDB_RS_OK)
    return TILEDB_RS_ERR;

  // Set offsets
  tiles_offsets_[attribute_id] = 0;
  tiles_var_offsets_[attribute_id] = 0;

  // Set variable tile size
  tiles_var_sizes_[attribute_id] = tile_var_size;

  // Shift starting offsets of variable-sized cells
  shift_var_offsets(attribute_id);

  // Mark as fetched
  fetched_tile_[attribute_id] = tile_i;

  // Success
  return TILEDB_RS_OK;
}

bool ReadState::is_empty_attribute(int attribute_id) const {
  // Prepare attribute file name
  std::string filename = 
      fragment_->fragment_name() + "/" +
      fragment_->array()->array_schema()->attribute(attribute_id) + 
      TILEDB_FILE_SUFFIX;

  // Check if the attribute file exists
  return !is_file(filename);
}

int ReadState::read_tile_from_file_cmp_gzip(
    int attribute_id,
    off_t offset,
    size_t tile_size) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();

  // To handle the special case of the search tile
  // The real attribute id corresponds to an actual attribute or coordinates 
  int attribute_id_real = 
      (attribute_id == attribute_num+1) ? attribute_num : attribute_id;

  // Potentially allocate compressed tile buffer
  if(tile_compressed_ == NULL) {
    size_t full_tile_size = fragment_->tile_size(attribute_id_real);
    size_t tile_max_size = 
        full_tile_size + 6 + 5*(ceil(full_tile_size/16834.0));
    tile_compressed_ = malloc(tile_max_size); 
    tile_compressed_allocated_size_ = tile_max_size;
  }

  // Prepare attribute file name
  std::string filename = 
      fragment_->fragment_name() + "/" +
      fragment_->array()->array_schema()->attribute(attribute_id_real) +
      TILEDB_FILE_SUFFIX;

  // Read from file
  if(read_from_file(filename, offset, tile_compressed_, tile_size) !=
     TILEDB_UT_OK)
    return TILEDB_RS_ERR;
  else
    return TILEDB_RS_OK;
}

int ReadState::read_tile_from_file_cmp_none(
    int attribute_id,
    off_t offset,
    size_t tile_size) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();

  // To handle the special case of the search tile
  // The real attribute id corresponds to an actual attribute or coordinates 
  int attribute_id_real = 
      (attribute_id == attribute_num+1) ? attribute_num : attribute_id;

  // Allocate space for the tile if needed
  if(tiles_[attribute_id] == NULL) {
    size_t full_tile_size = fragment_->tile_size(attribute_id_real);
    tiles_[attribute_id] = malloc(full_tile_size);
  }

  // Prepare attribute file name
  std::string filename = 
      fragment_->fragment_name() + "/" +
      fragment_->array()->array_schema()->attribute(attribute_id_real) +
      TILEDB_FILE_SUFFIX;

  // Read from file
  if(read_from_file(filename, offset, tiles_[attribute_id], tile_size) !=
     TILEDB_UT_OK)
    return TILEDB_RS_ERR;
  else
    return TILEDB_RS_OK;
}

int ReadState::read_tile_from_file_with_mmap_cmp_gzip(
    int attribute_id,
    off_t offset,
    size_t tile_size) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();

  // To handle the special case of the search tile
  // The real attribute id corresponds to an actual attribute or coordinates 
  int attribute_id_real = 
      (attribute_id == attribute_num+1) ? attribute_num : attribute_id;

  // Unmap
  if(map_addr_compressed_ != NULL) {
    if(munmap(map_addr_compressed_, map_addr_compressed_length_)) {
      PRINT_ERROR("Cannot read tile from file with map; Memory unmap error");
      return TILEDB_RS_ERR;
    }
  }

  // Prepare attribute file name
  std::string filename = 
      fragment_->fragment_name() + "/" +
      fragment_->array()->array_schema()->attribute(attribute_id_real) +
      TILEDB_FILE_SUFFIX;

  // Calculate offset considering the page size
  size_t page_size = sysconf(_SC_PAGE_SIZE);
  off_t start_offset = (offset / page_size) * page_size;
  size_t extra_offset = offset - start_offset;
  size_t new_length = tile_size + extra_offset;

  // Open file
  int fd = open(filename.c_str(), O_RDONLY);
  if(fd == -1) {
    munmap(map_addr_compressed_, map_addr_compressed_length_);
    map_addr_compressed_ = NULL;
    map_addr_compressed_length_ = 0;
    tile_compressed_ = NULL;
    PRINT_ERROR("Cannot read tile from file; File opening error");
    return TILEDB_RS_ERR;
  }

  // Map
  map_addr_compressed_ = mmap(
                             map_addr_compressed_, 
                             new_length, 
                             PROT_READ, 
                             MAP_SHARED, 
                             fd, 
                             start_offset);
  if(map_addr_compressed_ == MAP_FAILED) {
    map_addr_compressed_ = NULL;
    map_addr_compressed_length_ = 0;
    tile_compressed_ = NULL;
    PRINT_ERROR("Cannot read tile from file; Memory map error");
    return TILEDB_RS_ERR;
  }
  map_addr_compressed_length_ = new_length;

  // Set properly the compressed tile pointer
  tile_compressed_ = 
      static_cast<char*>(map_addr_compressed_) + extra_offset;

  // Close file
  if(close(fd)) {
    munmap(map_addr_compressed_, map_addr_compressed_length_);
    map_addr_compressed_ = NULL;
    map_addr_compressed_length_ = 0;
    tile_compressed_ = NULL;
    PRINT_ERROR("Cannot read tile from file; File closing error");
    return TILEDB_RS_ERR;
  }

  return TILEDB_RS_OK;
}

int ReadState::read_tile_from_file_with_mmap_cmp_none(
    int attribute_id,
    off_t offset,
    size_t tile_size) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();

  // To handle the special case of the search tile
  // The real attribute id corresponds to an actual attribute or coordinates 
  int attribute_id_real = 
      (attribute_id == attribute_num+1) ? attribute_num : attribute_id;

  // Unmap
  if(map_addr_[attribute_id] != NULL) {
    if(munmap(map_addr_[attribute_id], map_addr_lengths_[attribute_id])) {
      PRINT_ERROR("Cannot read tile from file with map; Memory unmap error");
      return TILEDB_RS_ERR;
    }
  }

  // Prepare attribute file name
  std::string filename = 
      fragment_->fragment_name() + "/" +
      fragment_->array()->array_schema()->attribute(attribute_id_real) +
      TILEDB_FILE_SUFFIX;

  // Calculate offset considering the page size
  size_t page_size = sysconf(_SC_PAGE_SIZE);
  off_t start_offset = (offset / page_size) * page_size;
  size_t extra_offset = offset - start_offset;
  size_t new_length = tile_size + extra_offset;

  // Open file
  int fd = open(filename.c_str(), O_RDONLY);
  if(fd == -1) {
    map_addr_[attribute_id] = NULL;
    map_addr_lengths_[attribute_id] = 0;
    tiles_[attribute_id] = NULL;
    tiles_sizes_[attribute_id] = 0;
    PRINT_ERROR("Cannot read tile from file; File opening error");
    return TILEDB_RS_ERR;
  }

  // Map
  bool var_size = 
      fragment_->array()->array_schema()->var_size(attribute_id_real);
  int prot = var_size ? (PROT_READ | PROT_WRITE) : PROT_READ;
  int flags = var_size ? MAP_PRIVATE : MAP_SHARED;
  map_addr_[attribute_id] = 
      mmap(map_addr_[attribute_id], new_length, prot, flags, fd, start_offset);
  if(map_addr_[attribute_id] == MAP_FAILED) {
    map_addr_[attribute_id] = NULL;
    map_addr_lengths_[attribute_id] = 0;
    tiles_[attribute_id] = NULL;
    tiles_sizes_[attribute_id] = 0;
    PRINT_ERROR("Cannot read tile from file; Memory map error");
    return TILEDB_RS_ERR;
  }
  map_addr_lengths_[attribute_id] = new_length;

  // Set properly the tile pointer
  tiles_[attribute_id] = 
      static_cast<char*>(map_addr_[attribute_id]) + extra_offset;

  // Close file
  if(close(fd)) {
    munmap(map_addr_[attribute_id], map_addr_lengths_[attribute_id]);
    map_addr_[attribute_id] = NULL;
    map_addr_lengths_[attribute_id] = 0;
    tiles_[attribute_id] = NULL;
    tiles_sizes_[attribute_id] = 0;
    PRINT_ERROR("Cannot read tile from file; File closing error");
    return TILEDB_RS_ERR;
  }

  return TILEDB_RS_OK;
}

int ReadState::read_tile_from_file_var_cmp_gzip(
    int attribute_id,
    off_t offset,
    size_t tile_size) {
  // Potentially allocate compressed tile buffer
  if(tile_compressed_ == NULL) {
    tile_compressed_ = malloc(tile_size); 
    tile_compressed_allocated_size_ = tile_size;
  }

  // Potentially expand compressed tile buffer
  if(tile_compressed_allocated_size_ < tile_size) {
    tile_compressed_ = realloc(tile_compressed_, tile_size); 
    tile_compressed_allocated_size_ = tile_size;
  }

  // Prepare attribute file name
  std::string filename = 
      fragment_->fragment_name() + "/" +
      fragment_->array()->array_schema()->attribute(attribute_id) + "_var" +
      TILEDB_FILE_SUFFIX;

  // Read from file
  if(read_from_file(filename, offset, tile_compressed_, tile_size) !=
     TILEDB_UT_OK)
    return TILEDB_RS_ERR;
  else
    return TILEDB_RS_OK;
}

int ReadState::read_tile_from_file_var_cmp_none(
    int attribute_id,
    off_t offset,
    size_t tile_size) {
  // Allocate space for the variable tile if needed
  if(tiles_var_[attribute_id] == NULL) { 
    tiles_var_[attribute_id] = malloc(tile_size);
    tiles_var_allocated_size_[attribute_id] = tile_size;
  }

  // Expand variable tile buffer if necessary
  if(tiles_var_allocated_size_[attribute_id] < tile_size) {
    tiles_var_[attribute_id] = realloc(tiles_var_[attribute_id], tile_size);
    tiles_var_allocated_size_[attribute_id] = tile_size;
  }

  // Set the actual variable tile size
  tiles_var_sizes_[attribute_id] = tile_size; 

  // Prepare variable attribute file name
  std::string filename = 
      fragment_->fragment_name() + "/" +
      fragment_->array()->array_schema()->attribute(attribute_id) + "_var" +
      TILEDB_FILE_SUFFIX;

  // Read from file
  if(read_from_file(filename, offset, tiles_var_[attribute_id], tile_size) !=
     TILEDB_UT_OK)
    return TILEDB_RS_ERR;
  else
   return TILEDB_RS_OK;
}

int ReadState::read_tile_from_file_with_mmap_var_cmp_gzip(
    int attribute_id,
    off_t offset,
    size_t tile_size) {
  // Unmap
  if(map_addr_compressed_ != NULL) {
    if(munmap(map_addr_compressed_, map_addr_compressed_length_)) {
      PRINT_ERROR("Cannot read tile from file with map; Memory unmap error");
      return TILEDB_RS_ERR;
    }
  }

  // Prepare attribute file name
  std::string filename = 
      fragment_->fragment_name() + "/" +
      fragment_->array()->array_schema()->attribute(attribute_id) + "_var" +
      TILEDB_FILE_SUFFIX;

  // Calculate offset considering the page size
  size_t page_size = sysconf(_SC_PAGE_SIZE);
  off_t start_offset = (offset / page_size) * page_size;
  size_t extra_offset = offset - start_offset;
  size_t new_length = tile_size + extra_offset;

  // Open file
  int fd = open(filename.c_str(), O_RDONLY);
  if(fd == -1) {
    munmap(map_addr_compressed_, map_addr_compressed_length_);
    map_addr_compressed_ = NULL;
    map_addr_compressed_length_ = 0;
    tile_compressed_ = NULL;
    PRINT_ERROR("Cannot read tile from file; File opening error");
    return TILEDB_RS_ERR;
  }

  // Map
  // new_length could be 0 for variable length fields, mmap will fail
  // if new_length == 0
  if(new_length > 0u) {
    map_addr_compressed_ = mmap(
        map_addr_compressed_, 
        new_length, 
        PROT_READ, 
        MAP_SHARED, 
        fd, 
        start_offset);
    if(map_addr_compressed_ == MAP_FAILED) {
      map_addr_compressed_ = NULL;
      map_addr_compressed_length_ = 0;
      tile_compressed_ = NULL;
      PRINT_ERROR("Cannot read tile from file; Memory map error");
      return TILEDB_RS_ERR;
    }
  } else {
    map_addr_var_[attribute_id] = 0;
  }
  map_addr_compressed_length_ = new_length;

  // Set properly the compressed tile pointer
  tile_compressed_ = 
      static_cast<char*>(map_addr_compressed_) + extra_offset;

  // Close file
  if(close(fd)) {
    munmap(map_addr_compressed_, map_addr_compressed_length_);
    map_addr_compressed_ = NULL;
    map_addr_compressed_length_ = 0;
    tile_compressed_ = NULL;
    PRINT_ERROR("Cannot read tile from file; File closing error");
    return TILEDB_RS_ERR;
  }

  return TILEDB_RS_OK;
}

int ReadState::read_tile_from_file_with_mmap_var_cmp_none(
    int attribute_id,
    off_t offset,
    size_t tile_size) {
  // Unmap
  if(map_addr_var_[attribute_id] != NULL) {
    if(munmap(
           map_addr_var_[attribute_id], 
           map_addr_var_lengths_[attribute_id])) {
      PRINT_ERROR("Cannot read tile from file with map; Memory unmap error");
      return TILEDB_RS_ERR;
    }
  }

  // Prepare attribute file name
  std::string filename = 
      fragment_->fragment_name() + "/" +
      fragment_->array()->array_schema()->attribute(attribute_id) + "_var" +
      TILEDB_FILE_SUFFIX;

  // Calculate offset considering the page size
  size_t page_size = sysconf(_SC_PAGE_SIZE);
  off_t start_offset = (offset / page_size) * page_size;
  size_t extra_offset = offset - start_offset;
  size_t new_length = tile_size + extra_offset;

  // Open file
  int fd = open(filename.c_str(), O_RDONLY);
  if(fd == -1) {
    map_addr_var_[attribute_id] = NULL;
    map_addr_var_lengths_[attribute_id] = 0;
    tiles_var_[attribute_id] = NULL;
    tiles_var_sizes_[attribute_id] = 0;
    PRINT_ERROR("Cannot read tile from file; File opening error");
    return TILEDB_RS_ERR;
  }

  // Map
  // new_length could be 0 for variable length fields, mmap will fail
  // if new_length == 0
  if(new_length > 0u) {
    map_addr_var_[attribute_id] = mmap(
        map_addr_var_[attribute_id], 
        new_length, 
        PROT_READ, 
        MAP_SHARED, 
        fd, 
        start_offset);
    if(map_addr_var_[attribute_id] == MAP_FAILED) {
      map_addr_var_[attribute_id] = NULL;
      map_addr_var_lengths_[attribute_id] = 0;
      tiles_var_[attribute_id] = NULL;
      tiles_var_sizes_[attribute_id] = 0;
      PRINT_ERROR("Cannot read tile from file; Memory map error");
      return TILEDB_RS_ERR;
    }
  } else {
    map_addr_var_[attribute_id] = 0;
  }
  map_addr_var_lengths_[attribute_id] = new_length;

  // Set properly the tile pointer
  tiles_var_[attribute_id] = 
      static_cast<char*>(map_addr_var_[attribute_id]) + extra_offset;
  tiles_var_sizes_[attribute_id] = tile_size; 

  // Close file
  if(close(fd)) {
    munmap(map_addr_var_[attribute_id], map_addr_var_lengths_[attribute_id]);
    map_addr_var_[attribute_id] = NULL;
    map_addr_var_lengths_[attribute_id] = 0;
    tiles_var_[attribute_id] = NULL;
    tiles_var_sizes_[attribute_id] = 0;
    PRINT_ERROR("Cannot read tile from file; File closing error");
    return TILEDB_RS_ERR;
  }

  // Success
  return TILEDB_RS_OK;
}

void ReadState::shift_var_offsets(int attribute_id) {
  // For easy reference
  int64_t cell_num = tiles_sizes_[attribute_id] / TILEDB_CELL_VAR_OFFSET_SIZE;
  size_t* tile_s = static_cast<size_t*>(tiles_[attribute_id]);
  size_t first_offset = tile_s[0];
  
  // Shift offsets
  for(int64_t i=0; i<cell_num; ++i)
    tile_s[i] -= first_offset;
}

void ReadState::shift_var_offsets(
    void* buffer, 
    int64_t offset_num,
    size_t new_start_offset) {
  // For easy reference
  size_t* buffer_s = static_cast<size_t*>(buffer);
  size_t start_offset = buffer_s[0];

  // Shift offsets
  for(int64_t i=0; i<offset_num; ++i) 
    buffer_s[i] = buffer_s[i] - start_offset + new_start_offset;
}




// Explicit template instantiations

template int ReadState::get_coords_after<int>(
    const int* coords,
    int* coords_after,
    bool& coords_retrieved);
template int ReadState::get_coords_after<int64_t>(
    const int64_t* coords,
    int64_t* coords_after,
    bool& coords_retrieved);
template int ReadState::get_coords_after<float>(
    const float* coords,
    float* coords_after,
    bool& coords_retrieved);
template int ReadState::get_coords_after<double>(
    const double* coords,
    double* coords_after,
    bool& coords_retrieved);

template int ReadState::get_enclosing_coords<int>(
    int tile_i,
    const int* target_coords,
    const int* start_coords,
    const int* end_coords,
    int* left_coords,
    int* right_coords,
    bool& left_retrieved,
    bool& right_retrieved,
    bool& target_exists);
template int ReadState::get_enclosing_coords<int64_t>(
    int tile_i,
    const int64_t* target_coords,
    const int64_t* start_coords,
    const int64_t* end_coords,
    int64_t* left_coords,
    int64_t* right_coords,
    bool& left_retrieved,
    bool& right_retrieved,
    bool& target_exists);
template int ReadState::get_enclosing_coords<float>(
    int tile_i,
    const float* target_coords,
    const float* start_coords,
    const float* end_coords,
    float* left_coords,
    float* right_coords,
    bool& left_retrieved,
    bool& right_retrieved,
    bool& target_exists);
template int ReadState::get_enclosing_coords<double>(
    int tile_i,
    const double* target_coords,
    const double* start_coords,
    const double* end_coords,
    double* left_coords,
    double* right_coords,
    bool& left_retrieved,
    bool& right_retrieved,
    bool& target_exists);

template int ReadState::get_fragment_cell_pos_range_sparse<int>(
    const FragmentInfo& fragment_info,
    const int* cell_range,
    FragmentCellPosRange& fragment_cell_pos_range);
template int ReadState::get_fragment_cell_pos_range_sparse<int64_t>(
    const FragmentInfo& fragment_info,
    const int64_t* cell_range,
    FragmentCellPosRange& fragment_cell_pos_range);
template int ReadState::get_fragment_cell_pos_range_sparse<float>(
    const FragmentInfo& fragment_info,
    const float* cell_range,
    FragmentCellPosRange& fragment_cell_pos_range);
template int ReadState::get_fragment_cell_pos_range_sparse<double>(
    const FragmentInfo& fragment_info,
    const double* cell_range,
    FragmentCellPosRange& fragment_cell_pos_range);

template int ReadState::get_fragment_cell_ranges_sparse<int>(
    int fragment_i,
    const int* start_coords,
    const int* end_coords,
    FragmentCellRanges& fragment_cell_ranges);
template int ReadState::get_fragment_cell_ranges_sparse<int64_t>(
    int fragment_i,
    const int64_t* start_coords,
    const int64_t* end_coords,
    FragmentCellRanges& fragment_cell_ranges);
template int ReadState::get_fragment_cell_ranges_sparse<float>(
    int fragment_i,
    const float* start_coords,
    const float* end_coords,
    FragmentCellRanges& fragment_cell_ranges);
template int ReadState::get_fragment_cell_ranges_sparse<double>(
    int fragment_i,
    const double* start_coords,
    const double* end_coords,
    FragmentCellRanges& fragment_cell_ranges);

template int ReadState::get_fragment_cell_ranges_sparse<int>(
    int fragment_i,
    FragmentCellRanges& fragment_cell_ranges);
template int ReadState::get_fragment_cell_ranges_sparse<int64_t>(
    int fragment_i,
    FragmentCellRanges& fragment_cell_ranges);

template int ReadState::get_fragment_cell_ranges_dense<int>(
    int fragment_i,
    FragmentCellRanges& fragment_cell_ranges);
template int ReadState::get_fragment_cell_ranges_dense<int64_t>(
    int fragment_i,
    FragmentCellRanges& fragment_cell_ranges);

template void ReadState::get_next_overlapping_tile_dense<int>(
    const int* tile_coords);
template void ReadState::get_next_overlapping_tile_dense<int64_t>(
    const int64_t* tile_coords);

template void ReadState::get_next_overlapping_tile_sparse<int>(
    const int* tile_coords);
template void ReadState::get_next_overlapping_tile_sparse<int64_t>(
    const int64_t* tile_coords);

template void ReadState::get_next_overlapping_tile_sparse<int>();
template void ReadState::get_next_overlapping_tile_sparse<int64_t>();
template void ReadState::get_next_overlapping_tile_sparse<float>();
template void ReadState::get_next_overlapping_tile_sparse<double>();

