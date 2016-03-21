/**
 * @file   read_state.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2016 MIT and Intel Corp.
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

  done_ = false;
  fetched_tile_.resize(attribute_num+2);
  search_tile_pos_ = -1;
  tile_coords_ = NULL;
  overflow_.resize(attribute_num+1);

  for(int i=0; i<attribute_num+1; ++i)
    overflow_[i] = false;


/* 
 TODO

  bool dense = fragment_->dense();

  // Initializations
  map_addr_.resize(attribute_num+1);
  map_addr_lengths_.resize(attribute_num+1);
  map_addr_compressed_ = NULL;
  map_addr_compressed_length_ = 0;
  map_addr_var_.resize(attribute_num+1);
  map_addr_var_lengths_.resize(attribute_num+1);
  overflow_.resize(attribute_num+1);
  overlapping_tiles_pos_.resize(attribute_num+1); 
  range_in_tile_domain_ = NULL;
  tile_compressed_ = NULL;
  tile_compressed_allocated_size_ = 0;
  tiles_.resize(attribute_num+1);
  tiles_offsets_.resize(attribute_num+1);
  tiles_sizes_.resize(attribute_num+1);
  tiles_var_.resize(attribute_num);
  tiles_var_offsets_.resize(attribute_num);
  tiles_var_sizes_.resize(attribute_num);
  tiles_var_allocated_size_.resize(attribute_num);

  for(int i=0; i<attribute_num+1; ++i) {
    map_addr_[i] = NULL;
    map_addr_lengths_[i] = 0;
    map_addr_var_[i] = NULL;
    map_addr_var_lengths_[i] = 0;
    overflow_[i] = false;
    overlapping_tiles_pos_[i] = 0;
    tiles_[i] = NULL;
    tiles_offsets_[i] = 0;
    tiles_sizes_[i] = 0;
  }

  for(int i=0; i<attribute_num; ++i) {
    tiles_var_[i] = NULL;
    tiles_var_offsets_[i] = 0;
    tiles_var_sizes_[i] = 0;
    tiles_var_allocated_size_[i] = 0;
  }

  search_tile_pos_ = -1;
 
  tile_pos_.resize(attribute_num+2); 
  for(int i=0; i<attribute_num+2; ++i)
    tile_pos_[i] = -1;

  if(dense)         // DENSE
    init_range_in_tile_domain();
  else              // SPARSE
    init_tile_search_range();
*/
}

ReadState::~ReadState() { 
/* TODO
  for(int i=0; i<int(overlapping_tiles_.size()); ++i) { 
    if(overlapping_tiles_[i].overlap_range_ != NULL)
      free(overlapping_tiles_[i].overlap_range_);

    if(overlapping_tiles_[i].coords_ != NULL)
      free(overlapping_tiles_[i].coords_);

    if(overlapping_tiles_[i].mbr_coords_ != NULL)
      free(overlapping_tiles_[i].mbr_coords_);

    if(overlapping_tiles_[i].mbr_in_tile_domain_ != NULL)
      free(overlapping_tiles_[i].mbr_in_tile_domain_);

    if(overlapping_tiles_[i].global_coords_ != NULL)
      free(overlapping_tiles_[i].global_coords_);
  }

  for(int i=0; i<int(tiles_.size()); ++i) {
    if(map_addr_[i] == NULL && tiles_[i] != NULL)
      free(tiles_[i]);
  }

  for(int i=0; i<int(tiles_var_.size()); ++i) {
    if(map_addr_var_[i] == NULL && tiles_var_[i] != NULL)
      free(tiles_var_[i]);
  }

  if(range_in_tile_domain_ != NULL)
    free(range_in_tile_domain_);

  if(map_addr_compressed_ == NULL && tile_compressed_ != NULL)
    free(tile_compressed_);

  for(int i=0; i<int(map_addr_.size()); ++i) {
    if(map_addr_[i] != NULL && munmap(map_addr_[i], map_addr_lengths_[i]))
      PRINT_WARNING("Problem in finalizing ReadState; Memory unmap error");
  }

  for(int i=0; i<int(map_addr_.size()); ++i) {
    if(map_addr_var_[i] != NULL && 
       munmap(map_addr_var_[i], map_addr_var_lengths_[i]))
      PRINT_WARNING("Problem in finalizing ReadState; Memory unmap error");
  }

  if(map_addr_compressed_ != NULL &&  
     munmap(map_addr_compressed_, map_addr_compressed_length_))
    PRINT_WARNING("Problem in finalizing ReadState; Memory unmap error");
*/
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
  int attribute_num = array_schema->attribute_num();
  size_t coords_size = array_schema->coords_size();
  int64_t pos = search_tile_pos_;
  assert(pos != -1);
  memcpy(bounding_coords, book_keeping_->bounding_coords()[pos], 2*coords_size);
}

const void* ReadState::get_tile_coords() const {
  return tile_coords_;
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
  int attribute_num = array_schema->attribute_num();
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
  int attribute_num = array_schema->attribute_num();
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
  // TODO
}

template<class T>
int ReadState::get_first_coords_after(
    T* start_coords_before,
    T* first_coords,
    bool& coords_retrieved) {
/* TODO
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  const void* tile_extents = array_schema->tile_extents();
  int attribute_num = array_schema->attribute_num();
  int dim_num = array_schema->dim_num();
  size_t coords_size = array_schema->coords_size();
  int compression = array_schema->compression(attribute_num);
  T empty_coord = 0;
  int rc;

// TODO: remove
int tile_i;

  // Important
  overlapping_tiles_pos_[attribute_num] = tile_i; 
  int64_t cell_num = overlapping_tiles_[tile_i].cell_num_;

  // Sanity check
  assert(cell_num >= 2);

  // Bring coordinates tile in main memory
  if(compression == TILEDB_GZIP)
    rc = get_tile_from_disk_cmp_gzip(attribute_num);
  else 
    rc = get_tile_from_disk_cmp_none(attribute_num);
  const T* tile = static_cast<const T*>(tiles_[attribute_num]);

  // Exit on error
  if(rc != TILEDB_RS_OK)
    return TILEDB_RS_ERR;

  // Perform binary search to find the position of start_coords in the tile
  int64_t min = 0;
  int64_t max = cell_num - 1;
  int64_t med;
  int64_t first_coords_pos;
  int cmp;
  int64_t start_coords_tile_id, tile_coords_tile_id;
  while(min <= max) {
    med = min + ((max - min) / 2);

    // Calculate precedence of tile ids
    if(tile_extents != NULL) {
      start_coords_tile_id = array_schema->tile_id(start_coords_before);
      tile_coords_tile_id = array_schema->tile_id(&tile[med*dim_num]);
    } else {
      start_coords_tile_id = 0;
      tile_coords_tile_id = 0;
    }

    // Calculate precedence of cells
    if(start_coords_tile_id == tile_coords_tile_id) {
      cmp = array_schema->cell_order_cmp<T>(
                start_coords_before,
                &tile[med*dim_num]);
    } else {
      if(start_coords_tile_id < tile_coords_tile_id)
        cmp = -1;
      else 
        cmp = 1;
    }

    // Update search range
    if(cmp < 0) 
      max = med-1;
    else if(cmp > 0)  
      min = med+1;
    else       
      break; 
  }

  // Find the position of the first coordinates
  if(max < min)    
    first_coords_pos = max + 1;
  else             
    first_coords_pos = med + 1;

  // Copy the first coordinates
  // TODO: replace with assert
  if(first_coords_pos < cell_num) {
    memcpy(first_coords, &tile[first_coords_pos * dim_num], coords_size);
  } else {
    // Initialize empty value
    if(&typeid(T) == &typeid(int))
      empty_coord = T(TILEDB_EMPTY_INT32);
    else if(&typeid(T) == &typeid(int64_t))
      empty_coord = T(TILEDB_EMPTY_INT64);
    else if(&typeid(T) == &typeid(float))
      empty_coord = T(TILEDB_EMPTY_FLOAT64);
    else if(&typeid(T) == &typeid(double))
      empty_coord = T(TILEDB_EMPTY_FLOAT64);

    // Copy empty value
    memcpy(first_coords, &empty_coord, sizeof(T));
  }

  // Success
  return TILEDB_RS_OK;
*/
}

template<class T>
int ReadState::get_fragment_cell_pos_range_sparse(
    const FragmentInfo& fragment_info,
    const T* cell_range, 
    FragmentCellPosRange& fragment_cell_pos_range) {
/* TODO

 // TODO: This may return no cell range... (-1)

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();
  int dim_num = array_schema->dim_num();
  size_t coords_size = array_schema->coords_size();
  int compression = array_schema->compression(attribute_num);
  int64_t cell_num = overlapping_tiles_.back().cell_num_;
  const T* start_range_coords = cell_range;
  const T* end_range_coords = &cell_range[dim_num];
  int rc;

  // Important
  overlapping_tiles_pos_[attribute_num] = fragment_info.second; 

  // Update only in the case of coordinates attribute
  if(fragment_info.second < overlapping_tiles_.size()-1 && 
     !overlapping_tiles_[fragment_info.second].coords_tile_fetched_reset_) {
    overlapping_tiles_[fragment_info.second].tile_fetched_[attribute_num] = false; 
    overlapping_tiles_[fragment_info.second].coords_tile_fetched_reset_ = true; 
  }

  // Bring coordinates tile in main memory
  if(compression == TILEDB_GZIP)
    rc = get_tile_from_disk_cmp_gzip(attribute_num);
  else 
    rc = get_tile_from_disk_cmp_none(attribute_num);
  const T* tile = static_cast<const T*>(tiles_[attribute_num]);

  // Exit on error
  if(rc != TILEDB_RS_OK)
    return TILEDB_RS_ERR;

  // Find the position of the first range end point with binary search
  int64_t min = 0;
  int64_t max = cell_num - 1;
  int64_t med;
  int64_t first_pos;
  int cmp;
  int64_t tile_coords_tile_id, start_range_coords_tile_id, end_range_coords_tile_id;
  while(min <= max) {
    med = min + ((max - min) / 2);

    // Calculate precedence of tile ids
    start_range_coords_tile_id = array_schema->tile_id(start_range_coords);
    tile_coords_tile_id = array_schema->tile_id(&tile[med*dim_num]);

    // Calculate precedence of cells
    if(start_range_coords_tile_id == tile_coords_tile_id) {
      cmp = array_schema->cell_order_cmp<T>(
                start_range_coords,
                &tile[med*dim_num]);
    } else {
      if(start_range_coords_tile_id < tile_coords_tile_id)
        cmp = -1;
      else 
        cmp = 1;
    }

    // Update search range
    if(cmp < 0) 
      max = med-1;
    else if(cmp > 0)  
      min = med+1;
    else       
      break; 
  }

  // Find the position of the first coordinates
  if(max < min)    
    first_pos = min;
  else             
    first_pos = med;

  // If the range is unary, create a single position range and return
  if(!memcmp(start_range_coords, end_range_coords, coords_size)) {
    FragmentCellPosRange fragment_cell_pos_range;
    fragment_cell_pos_range.first = fragment_info;
    fragment_cell_pos_range.second = CellPosRange(first_pos, first_pos);
    return TILEDB_RS_OK;
  }

  // If the range is not unary, find the position of the second range end point
  min = first_pos;
  max = cell_num - 1;
  int64_t second_pos;
  while(min <= max) {
    med = min + ((max - min) / 2);

        // Calculate precedence of tile ids
    end_range_coords_tile_id = array_schema->tile_id(end_range_coords);
    tile_coords_tile_id = array_schema->tile_id(&tile[med*dim_num]);

    // Calculate precedence of cells
    if(end_range_coords_tile_id == tile_coords_tile_id) {
      cmp = array_schema->cell_order_cmp<T>(
                end_range_coords,
                &tile[med*dim_num]);
    } else {
      if(end_range_coords_tile_id < tile_coords_tile_id)
        cmp = -1;
      else 
        cmp = 1;
    }

    // Update search range
    if(cmp < 0) 
      max = med-1;
    else if(cmp > 0)  
      min = med+1;
    else       
      break; 
  }

  // Find the position of the first coordinates
  if(max < min)    
    second_pos = max;
  else             
    second_pos = med;

  // Create the cell position ranges for all cells in the position range 
  const T* cell;
  int64_t current_start_pos, current_end_pos = -2; 
  for(int64_t i=first_pos; i<=second_pos; ++i) {
    cell = &tile[i*dim_num];
    if(cell_in_subarray<T>(
           cell, 
           static_cast<const T*>(overlapping_tiles_[fragment_info.second].overlap_range_), 
           dim_num)) {
      if(i-1 == current_end_pos) { // The range is expanded
       ++current_end_pos;
      } else {                     // A new range starts
        current_start_pos = i;
        current_end_pos = i;
      } 
    } else {
      if(i-1 == current_end_pos) { // The range needs to be added to the list
        FragmentCellPosRange fragment_cell_pos_range;
        fragment_cell_pos_range.first = fragment_info;
        fragment_cell_pos_range.second = CellPosRange(current_start_pos, current_end_pos);
        current_end_pos = -2; // This indicates that there is no active range
      }
    }
  }

  // Add last cell range
  if(current_end_pos != -2) {
    FragmentCellPosRange fragment_cell_pos_range;
    fragment_cell_pos_range.first = fragment_info;
    fragment_cell_pos_range.second = CellPosRange(current_start_pos, current_end_pos);
  }

  // Return 
  return TILEDB_RS_OK;
*/
}

template<class T>
int ReadState::get_fragment_cell_ranges_dense(
    int fragment_i,
    FragmentCellRanges& fragment_cell_ranges) {
  // TODO
}

template<class T>
int ReadState::get_fragment_cell_ranges_sparse(
    int fragment_i,
    FragmentCellRanges& fragment_cell_ranges) {
  // TODO
}

template<class T>
int ReadState::get_fragment_cell_ranges_sparse(
    int fragment_i,
    const T* start_coords,
    const T* end_coords,
    FragmentCellRanges& fragment_cell_ranges) {
  // TODO
}

template<class T> 
void ReadState::get_next_overlapping_tile_dense(
    const T* subarray_tile_coords) {
/*


// TODO: update tile coords

  // TODO
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int dim_num = array_schema->dim_num();
  size_t coords_size = array_schema->coords_size();

  // The next overlapping tile
  OverlappingTile overlapping_tile = {};
  overlapping_tile.coords_ = malloc(coords_size);
  overlapping_tile.overlap_range_ = malloc(2*coords_size);

  // For easy reference
  const T* domain = static_cast<const T*>(book_keeping_->domain());
  const T* non_empty_domain = 
      static_cast<const T*>(book_keeping_->non_empty_domain());
  const T* range_in_tile_domain = static_cast<const T*>(range_in_tile_domain_);
  T* coords = static_cast<T*>(overlapping_tile.coords_);
  T* overlap_range = static_cast<T*>(overlapping_tile.overlap_range_);
  const T* range = static_cast<const T*>(fragment_->array()->subarray());

  // Get coordinates
  if(overlapping_tiles_.size() == 0) {  // First tile
    // Special case of no overlap at all with the query range
    if(!overlap_) {
      overlapping_tile.overlap_ = NONE;
      // TODO: overlapping_tile.overlap_range_ = NULL; 
      // TODO: overlapping_tile.coords_ = NULL;
      overlapping_tiles_.push_back(overlapping_tile);
      return;
    }

    // Start from the upper left corner of the range in tile domain
    for(int i=0; i<dim_num; ++i)
      coords[i] = range_in_tile_domain[2*i]; 
  } else {                              // Every other tile
    // Advance coordinates from the previous ones
    const T* previous_coords = 
        static_cast<const T*>(overlapping_tiles_.back().coords_);
    for(int i=0; i<dim_num; ++i)
      coords[i] = previous_coords[i]; 
    array_schema->get_next_tile_coords<T>(range_in_tile_domain, coords);
  }

  // Get the tile position in the global tile order
  overlapping_tile.pos_ = array_schema->get_tile_pos<T>(domain, coords);

  // Get tile overlap
  int overlap;
//  array_schema->compute_tile_range_overlap<T>(
//      domain,          // fragment domain
//      non_empty_domain,// fragment non-empty domain
//      range,           // query range in array domain
//      coords,          // tile coordinates in range in tile domain
//      overlap_range,   // returned overlap in terms of cell coordinates in tile
//      overlap);        // returned type of overlap

  if(overlap == 0)
    overlapping_tile.overlap_ = NONE;
  else if(overlap == 1) 
    overlapping_tile.overlap_ = FULL;
  else if(overlap == 2) 
    overlapping_tile.overlap_ = PARTIAL_NON_CONTIG;
  else if(overlap == 3) 
    overlapping_tile.overlap_ = PARTIAL_CONTIG;

  // Set the number of cells in the tile
  overlapping_tile.cell_num_ = array_schema->cell_num_per_tile();
 
  // Store the new tile
  overlapping_tiles_.push_back(overlapping_tile); 

  // Clean up processed overlapping tiles
// TODO:  clean_up_processed_overlapping_tiles();
*/
}

template<class T>
void ReadState::get_next_overlapping_tile_sparse() {
/*
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();
  size_t coords_size = array_schema->coords_size();
  const T* range = static_cast<const T*>(fragment_->array()->subarray());
  int64_t tile_num = book_keeping_->mbrs().size();

  // The next overlapping tile
  OverlappingTile overlapping_tile = {};
  // TODO:  overlapping_tile.coords_ = NULL; // Applicable only to dense arrays
  overlapping_tile.overlap_range_ = malloc(2*coords_size);

  // Reset flag
  overlapping_tile.tile_fetched_.resize(attribute_num+1);
  for(int i=0; i<attribute_num+1; ++i)
    overlapping_tile.tile_fetched_[i] = false;

  // For easy reference
  T* overlap_range = static_cast<T*>(overlapping_tile.overlap_range_);

  // Get next tile position
  int64_t tile_pos;
  if(overlapping_tiles_.size() == 0)   // First tile
    tile_pos = tile_search_range_[0];
  else                                 // Every other tile
    tile_pos = overlapping_tiles_.back().pos_ + 1;
  overlapping_tile.overlap_ = NONE;

  // Find the tile position of the next overlapping tile
  if(tile_search_range_[0] >= 0 &&
     tile_search_range_[0] < tile_num) { 
    while(overlapping_tile.overlap_ == NONE && 
          tile_pos <= tile_search_range_[1]) {
      // Set tile position
      overlapping_tile.pos_ = tile_pos;

      // Get tile overlap
      int overlap;
      const T* mbr = static_cast<const T*>(book_keeping_->mbrs()[tile_pos]);

//      array_schema->compute_mbr_range_overlap<T>(
//          range,         // query range
//          mbr,           // tile mbr
//          overlap_range, // returned overlap in terms of absolute coordinates
//          overlap);      // returned type of overlap

      if(overlap == 0) {
        overlapping_tile.overlap_ = NONE;
      } else if(overlap == 1) {
        overlapping_tile.overlap_ = FULL;
      } else if(overlap == 2) {
        overlapping_tile.overlap_ = PARTIAL_NON_CONTIG;
      } else if(overlap == 3) {
        overlapping_tile.overlap_ = PARTIAL_CONTIG;
      } 

      // Update tile position
      ++tile_pos;
    }
  }

  // Compute number of cells in the tile
  if(overlapping_tile.pos_ != tile_num-1)  // Not last tile
    overlapping_tile.cell_num_ = array_schema->capacity();
  else                                     // Last tile
    overlapping_tile.cell_num_ = book_keeping_->last_tile_cell_num();

  // Store the new tile
  overlapping_tiles_.push_back(overlapping_tile); 

  // Clean up processed overlapping tiles
// TODO:  clean_up_processed_overlapping_tiles();

  // In case of partial overlap, find the ranges of qualifying cells
  if(overlapping_tile.overlap_ == PARTIAL_CONTIG ||
     overlapping_tile.overlap_ == PARTIAL_NON_CONTIG)
    compute_cell_pos_ranges<T>();
*/
}


template<class T> 
void ReadState::get_next_overlapping_tile_sparse(
    const T* subarray_tile_coords) {
  // TODO
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
  const ArraySchema* array_schema = fragment_->array()->array_schema();
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
    int64_t min = start_cell_pos;
    int64_t max = end_cell_pos;
    int64_t med;
    while(min < max) {
      med = min + ((max - min) / 2);

      // Calculate variable bytes to copy
      bytes_var_to_copy = tile[med] - tile[start_cell_pos];

      // Check condition
      if(bytes_var_to_copy > buffer_var_free_space) 
        max = med;
      else if(bytes_var_to_copy < buffer_var_free_space)
        min = med;
      else   
        break; 
    }

    // Determine the end position of the range
    if(min == max)  
      end_cell_pos = min - 1;     
    else 
      end_cell_pos = med - 1;  

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

  // Potentially allocate space for buffer
  if(tiles_var_[attribute_id] == NULL) {
    tiles_var_[attribute_id] = malloc(tile_var_size);
    tiles_var_allocated_size_[attribute_id] = tile_var_size;
  }

  // Potentially expand buffer
  if(tile_var_size > tiles_var_allocated_size_[attribute_id]) {
    tiles_var_[attribute_id] = realloc(tiles_var_[attribute_id], tile_var_size);
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

template int ReadState::get_first_coords_after<int>(
    int* start_coords_before,
    int* first_coords,
    bool& coords_retrieved);
template int ReadState::get_first_coords_after<int64_t>(
    int64_t* start_coords_before,
    int64_t* first_coords,
    bool& coords_retrieved);
template int ReadState::get_first_coords_after<float>(
    float* start_coords_before,
    float* first_coords,
    bool& coords_retrieved);
template int ReadState::get_first_coords_after<double>(
    double* start_coords_before,
    double* first_coords,
    bool& coords_retrieved);

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
    const int* subarray_tile_coords);
template void ReadState::get_next_overlapping_tile_dense<int64_t>(
    const int64_t* subarray_tile_coords);

template void ReadState::get_next_overlapping_tile_sparse<int>(
    const int* subarray_tile_coords);
template void ReadState::get_next_overlapping_tile_sparse<int64_t>(
    const int64_t* subarray_tile_coords);

template void ReadState::get_next_overlapping_tile_sparse<int>();
template void ReadState::get_next_overlapping_tile_sparse<int64_t>();
template void ReadState::get_next_overlapping_tile_sparse<float>();
template void ReadState::get_next_overlapping_tile_sparse<double>();

