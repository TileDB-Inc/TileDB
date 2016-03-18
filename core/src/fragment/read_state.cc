/**
 * @file   read_state.cc
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
  bool dense = fragment_->dense();

  // Initializations
  cell_pos_range_pos_.resize(attribute_num+1);
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
  coords_tile_offset_ = 0ull;
  tiles_sizes_.resize(attribute_num+1);
  tiles_var_.resize(attribute_num);
  tiles_var_offsets_.resize(attribute_num);
  tiles_var_sizes_.resize(attribute_num);
  tiles_var_allocated_size_.resize(attribute_num);

  for(int i=0; i<attribute_num+1; ++i) {
    cell_pos_range_pos_[i] = 0;
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

  if(dense)         // DENSE
    init_range_in_tile_domain();
  else              // SPARSE
    init_tile_search_range();
}

ReadState::~ReadState() { 
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
}

/* ****************************** */
/*          READ FUNCTIONS        */
/* ****************************** */

template<class T>
bool ReadState::coords_exist(
    int64_t tile_i,
    const T* coords) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();
  int dim_num = array_schema->dim_num();
  size_t coords_size = array_schema->coords_size();
  int compression = array_schema->compression(attribute_num);
  int rc;

  // Important
  overlapping_tiles_pos_[attribute_num] = tile_i; 
  int64_t cell_num = overlapping_tiles_[tile_i].cell_num_;

  // Bring coordinates tile in main memory
  if(compression == TILEDB_GZIP)
    rc = get_tile_from_disk_cmp_gzip(attribute_num);
  else 
    rc = get_tile_from_disk_cmp_none(attribute_num);
  const T* tile = static_cast<const T*>(tiles_[attribute_num]);

  // Exit on error
  if(rc != TILEDB_RS_OK)
    return false;

  // Perform binary search to find coords in the tile
  int64_t min = 0;
  int64_t max = cell_num - 1;
  int64_t med;
  int cmp;
  int64_t coords_tile_id, tile_coords_tile_id;
  while(min <= max) {
    med = min + ((max - min) / 2);

    // Calculate precedence of tile ids
    coords_tile_id = array_schema->tile_id(coords);
    tile_coords_tile_id = array_schema->tile_id(&tile[med*dim_num]);

    // Calculate precedence of cells
    if(coords_tile_id == tile_coords_tile_id) {
      cmp = array_schema->cell_order_cmp<T>(
                coords,
                &tile[med*dim_num]);
    } else {
      if(coords_tile_id < tile_coords_tile_id)
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

  if(max < min)    
    return false;
  else
    return true;
}

int64_t ReadState::overlapping_tiles_num() const {
  return overlapping_tiles_.size();
}

template<class T>
void ReadState::tile_done(int attribute_id) {
  if(fragment_->dense()) {
    tiles_offsets_[attribute_id] = tiles_sizes_[attribute_id];
    ++overlapping_tiles_pos_[attribute_id];
  } else {
    // Get next tile coordinates inside the MBR
    const ArraySchema* array_schema = fragment_->array()->array_schema();
    int dim_num = array_schema->dim_num();
    size_t coords_size = array_schema->coords_size();
    const T* mbr_in_tile_domain = 
        static_cast<const T*>(overlapping_tiles_.back().mbr_in_tile_domain_);
    T* mbr_coords = new T[dim_num];
    memcpy(mbr_coords, overlapping_tiles_.back().mbr_coords_, coords_size);
    array_schema->get_next_tile_coords<T>(mbr_in_tile_domain, mbr_coords);

    // Check if the new coordinates exceeded the MBR tile domain
    bool inside_mbr = true;
    for(int i=0; i<dim_num; ++i) {
      if(mbr_coords[i] > mbr_in_tile_domain[2*i+1] ||
         mbr_coords[i] < mbr_in_tile_domain[2*i]) {
        inside_mbr = false;
        break;
      }
    }

    // Clean up
    delete [] mbr_coords;

    // Advance overlapping tile
    if(!inside_mbr) {
      tiles_offsets_[attribute_id] = tiles_sizes_[attribute_id];
      ++overlapping_tiles_pos_[attribute_id];
    }
  }
}

template<class T>
int ReadState::copy_cell_range(
    int attribute_id,
    int tile_i,
    void* buffer,  
    size_t buffer_size,
    size_t& buffer_offset,
    const CellPosRange& cell_pos_range) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();
  size_t cell_size = array_schema->cell_size(attribute_id);
  OverlappingTile& overlapping_tile = 
      overlapping_tiles_[overlapping_tiles_pos_[attribute_id]];
  char* buffer_c = static_cast<char*>(buffer);
  int rc;
  int compression = array_schema->compression(attribute_id);

  // Important
  overlapping_tiles_pos_[attribute_id] = tile_i; 

  // Update only in the case of coordinates attribute
  if(attribute_id == attribute_num &&
     tile_i < overlapping_tiles_.size()-1 && 
     !overlapping_tiles_[tile_i].coords_tile_fetched_reset_) {
    overlapping_tiles_[tile_i].tile_fetched_[attribute_num] = false; 
    overlapping_tiles_[tile_i].coords_tile_fetched_reset_ = true; 
  }

  // Fetch the attribute tile from disk if necessary
  if(compression == TILEDB_GZIP)
    rc = get_tile_from_disk_cmp_gzip(attribute_id);
  else
    rc = get_tile_from_disk_cmp_none(attribute_id);
  if(rc != TILEDB_RS_OK)
    return TILEDB_RS_ERR;

  //Restore coords_tile_offset_
  if(attribute_id == attribute_num)
    tiles_offsets_[attribute_id] = coords_tile_offset_;

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
  if(bytes_to_copy != 0) {
    memcpy(
        buffer_c + buffer_offset, 
        tile + tiles_offsets_[attribute_id], 
        bytes_to_copy);
    buffer_offset += bytes_to_copy;
    tiles_offsets_[attribute_id] += bytes_to_copy; 
    buffer_free_space = buffer_size - buffer_offset;
  }

  //Backup coordinates tile offset
  if(attribute_id == attribute_num)
    coords_tile_offset_ = tiles_offsets_[attribute_id];

  // Handle buffer overflow
  if(tiles_offsets_[attribute_id] != end_offset + 1) 
    overflow_[attribute_id] = true;

  // Success
  return TILEDB_RS_OK;
}

template<class T>
int ReadState::copy_cell_range_var(
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

  // Important
  overlapping_tiles_pos_[attribute_id] = tile_i; 

  // Update only in the case of coordinates attribute
  if(attribute_id == attribute_num &&
     tile_i < overlapping_tiles_.size()-1 && 
     !overlapping_tiles_[tile_i].coords_tile_fetched_reset_) {
    overlapping_tiles_[tile_i].tile_fetched_[attribute_num] = false; 
    overlapping_tiles_[tile_i].coords_tile_fetched_reset_ = true; 
  }

  // For easy reference
  OverlappingTile& overlapping_tile = 
      overlapping_tiles_[overlapping_tiles_pos_[attribute_id]];
  char* buffer_c = static_cast<char*>(buffer);
  void* buffer_start = buffer_c + buffer_offset;
  char* buffer_var_c = static_cast<char*>(buffer_var);
  int compression = array_schema->compression(attribute_id);
  int rc;

  // Fetch the attribute tile from disk if necessary
  if(compression == TILEDB_GZIP)
    rc = get_tile_from_disk_var_cmp_gzip(attribute_id);
  else
    rc = get_tile_from_disk_var_cmp_none(attribute_id);
  if(rc != TILEDB_RS_OK)
    return TILEDB_RS_ERR;

  // For easy reference
  char* tile = static_cast<char*>(tiles_[attribute_id]);
  size_t* tile_s = static_cast<size_t*>(tiles_[attribute_id]);
  char* tile_var = static_cast<char*>(tiles_var_[attribute_id]);

  // Sanity check
  assert(array_schema->var_size(attribute_id));

  // For each cell position range, copy the respective cells to the buffer
  size_t start_offset, end_offset;
  int64_t start_cell_pos, end_cell_pos;
  size_t bytes_left_to_copy, bytes_to_copy, bytes_var_to_copy;
  int64_t cell_pos_ranges_num = overlapping_tile.cell_pos_ranges_.size();

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

int ReadState::read(void** buffers, size_t* buffer_sizes) {
  // For easy reference
  const Array* array = fragment_->array();

  // Reset buffer overflow
  reset_overflow();

  // Dispatch the proper write command
  if(array->mode() == TILEDB_ARRAY_READ) {                 // NORMAL READ
    if(fragment_->dense())                                // DENSE FRAGMENT
      return read_dense(buffers, buffer_sizes);       
    else                                                  // SPARSE FRAGMENT
      return read_sparse(buffers, buffer_sizes);       
  } else {
    PRINT_ERROR("Cannot read from fragment; Invalid mode");
    return TILEDB_RS_ERR;
  } 
}

/* ****************************** */
/*           ACCESSORS            */
/* ****************************** */

ReadState::Overlap ReadState::overlap() const {
  return overlapping_tiles_.back().overlap_;
}

bool ReadState::overflow(int attribute_id) const {
  return overflow_[attribute_id];
}

template<class T>
bool ReadState::max_overlap(const T* max_overlap_range) const {
  // Sparse fragments do not have full overlap
  if(!fragment_->dense())
    return false;

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int dim_num = array_schema->dim_num();
  const T* overlap_range = 
      static_cast<const T*>(overlapping_tiles_.back().overlap_range_);

  for(int i=0; i<dim_num; ++i) {
    if(overlap_range[2*i] != max_overlap_range[2*i] ||
       overlap_range[2*i+1] != max_overlap_range[2*i+1])
      return false;
  }

  return true;
}

template<class T>
void ReadState::compute_fragment_cell_ranges(
    const FragmentInfo& fragment_info,
    FragmentCellRanges& fragment_cell_ranges) const {

  if(fragment_->dense()) // DENSE
    compute_fragment_cell_ranges_dense<T>(fragment_info, fragment_cell_ranges);
  else                   // SPARSE
    compute_fragment_cell_ranges_sparse<T>(fragment_info, fragment_cell_ranges);
}

template<class T>
void ReadState::compute_fragment_cell_ranges_dense(
    const FragmentInfo& fragment_info,
    FragmentCellRanges& fragment_cell_ranges) const {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int dim_num = array_schema->dim_num();
  size_t coords_size = array_schema->coords_size();
  int cell_order = array_schema->cell_order();
  size_t cell_range_size = 2*coords_size;
  const T* tile_extents = static_cast<const T*>(array_schema->tile_extents());
  const T* global_domain = static_cast<const T*>(array_schema->domain());
  const T* global_tile_coords = 
      static_cast<const T*>(overlapping_tiles_.back().global_coords_);
  const T* overlap_range = 
      static_cast<const T*>(overlapping_tiles_.back().overlap_range_);
  Overlap overlap = overlapping_tiles_.back().overlap_;

  // Compute global coordinates of overlap_range
  T* global_overlap_range = new T[2*dim_num]; 
  for(int i=0; i<dim_num; ++i) {
    global_overlap_range[2*i] = 
        global_tile_coords[i] * tile_extents[i] +
        overlap_range[2*i] + global_domain[2*i];
    global_overlap_range[2*i+1] = 
        global_tile_coords[i] * tile_extents[i] +
        overlap_range[2*i+1] + global_domain[2*i];
  }

  // Contiguous cells, single cell range
  if(overlap == FULL || overlap == PARTIAL_CONTIG) {
    void* cell_range = malloc(cell_range_size);
    T* cell_range_T = static_cast<T*>(cell_range);
    for(int i=0; i<dim_num; ++i) {
      cell_range_T[i] = global_overlap_range[2*i];
      cell_range_T[dim_num + i] = global_overlap_range[2*i+1];
    }
    fragment_cell_ranges.push_back(
        FragmentCellRange(fragment_info, cell_range));
  } else { // Non-contiguous cells, multiple ranges
    // Initialize the coordinates at the beginning of the global range
    T* coords = new T[dim_num];
    for(int i=0; i<dim_num; ++i)
      coords[i] = global_overlap_range[2*i];

    // Handle the different cell orders
    int i;
    if(cell_order == TILEDB_ROW_MAJOR) {           // ROW
      while(coords[0] <= global_overlap_range[1]) {
        // Make a cell range representing a slab       
        void* cell_range = malloc(cell_range_size);
        T* cell_range_T = static_cast<T*>(cell_range);
        for(int i=0; i<dim_num-1; ++i) { 
          cell_range_T[i] = coords[i];
          cell_range_T[dim_num+i] = coords[i];
        }
        cell_range_T[dim_num-1] = global_overlap_range[2*(dim_num-1)];
        cell_range_T[2*dim_num-1] = global_overlap_range[2*(dim_num-1)+1];

        // Insert the new range into the result vector
        fragment_cell_ranges.push_back(
            FragmentCellRange(fragment_info, cell_range));
 
        // Advance coordinates
        i=dim_num-2;
        ++coords[i];
        while(i > 0 && coords[i] > global_overlap_range[2*i+1]) {
          coords[i] = global_overlap_range[2*i];
          ++coords[--i];
        } 
      }
    } else if(cell_order == TILEDB_COL_MAJOR) { // COLUMN
      while(coords[dim_num-1] <= global_overlap_range[2*(dim_num-1)+1]) {
        // Make a cell range representing a slab       
        void* cell_range = malloc(cell_range_size);
        T* cell_range_T = static_cast<T*>(cell_range);
        for(int i=dim_num-1; i>0; --i) { 
          cell_range_T[i] = coords[i];
          cell_range_T[dim_num+i] = coords[i];
        }
        cell_range_T[0] = global_overlap_range[0];
        cell_range_T[dim_num] = global_overlap_range[1];

        // Insert the new range into the result vector
        fragment_cell_ranges.push_back(
            FragmentCellRange(fragment_info, cell_range));
 
        // Advance coordinates
        i=1;
        ++coords[i];
        while(i <dim_num-1 && coords[i] > global_overlap_range[2*i+1]) {
          coords[i] = global_overlap_range[2*i];
          ++coords[++i];
        } 
      }
    } else {
      assert(0);
    }

    delete [] coords;
  }

  // Clean up
  delete [] global_overlap_range;
}

template<class T>
void ReadState::compute_fragment_cell_ranges_sparse(
    const FragmentInfo& fragment_info,
    FragmentCellRanges& fragment_cell_ranges) const {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int dim_num = array_schema->dim_num();
  size_t coords_size = array_schema->coords_size();
  const T* tile_extents = static_cast<const T*>(array_schema->tile_extents());
  const T* global_domain = static_cast<const T*>(array_schema->domain());
  const T* global_tile_coords = 
      static_cast<const T*>(overlapping_tiles_.back().global_coords_);
  const T* overlap_range = 
      static_cast<const T*>(overlapping_tiles_.back().overlap_range_);

  // Compute overlap range coordinates inside the tile
  T* tile_overlap_range = new T[2*dim_num]; 
  for(int i=0; i<dim_num; ++i) {
    tile_overlap_range[2*i] = 
        std::max(
            global_tile_coords[i] * tile_extents[i] + global_domain[2*i],
            overlap_range[2*i]);
    tile_overlap_range[2*i+1] = 
        std::min(
            (global_tile_coords[i]+1) * tile_extents[i] - 1 + global_domain[2*i],
            overlap_range[2*i+1]);
  }
  
/* TODO: remove
  void* cell_range = malloc(2*coords_size);
  memcpy(cell_range, bounding_coords, 2*coords_size);
  fragment_cell_ranges.push_back(
      FragmentCellRange(fragment_info, cell_range));
*/

  void* cell_range = malloc(2*coords_size);
  T* cell_range_T = static_cast<T*>(cell_range);
  for(int i=0; i<dim_num; ++i) {
    cell_range_T[i] = tile_overlap_range[2*i];
    cell_range_T[dim_num + i] = tile_overlap_range[2*i+1];
  }
  fragment_cell_ranges.push_back(
      FragmentCellRange(fragment_info, cell_range));
 

// TODO: remove
/*
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int dim_num = array_schema->dim_num();
  size_t coords_size = array_schema->coords_size();
  ArraySchema::CellOrder cell_order = array_schema->cell_order();
  size_t cell_range_size = 2*coords_size;
  const T* tile_extents = static_cast<const T*>(array_schema->tile_extents());
  const T* global_domain = static_cast<const T*>(array_schema->domain());
  const T* global_tile_coords = 
      static_cast<const T*>(overlapping_tiles_.back().global_coords_);
  const T* overlap_range = 
      static_cast<const T*>(overlapping_tiles_.back().overlap_range_);

  // Compute overlap range coordinates inside the tile
  T* tile_overlap_range = new T[2*dim_num]; 
  for(int i=0; i<dim_num; ++i) {
    tile_overlap_range[2*i] = 
        std::max(
            global_tile_coords[i] * tile_extents[i] + global_domain[2*i],
            overlap_range[2*i]);
    tile_overlap_range[2*i+1] = 
        std::min(
            (global_tile_coords[i]+1) * tile_extents[i] - 1 + global_domain[2*i],
            overlap_range[2*i+1]);
  }

  // Compute overlap
  Overlap overlap = FULL;
  for(int i=0; i<dim_num; ++i) {
    if(tile_overlap_range[2*i] - global_domain[2*i] - 
       ((tile_overlap_range[2*i] - global_domain[2*i]) / tile_extents[i]) * 
       tile_extents[i] != 0 ||
       tile_overlap_range[2*i+1] - global_domain[2*i] - 
       ((tile_overlap_range[2*i+1] - global_domain[2*i]) / tile_extents[i]) * tile_extents[i] 
            != tile_extents[i] - 1) {
      overlap = PARTIAL_NON_CONTIG;
      break;
    }
  }

  if(overlap == PARTIAL_NON_CONTIG) {
    overlap = PARTIAL_CONTIG;
    if(cell_order == ArraySchema::TILEDB_AS_CO_ROW_MAJOR) {   // Row major
      for(int i=1; i<dim_num; ++i) {
       if(tile_overlap_range[2*i] - global_domain[2*i] - 
          ((tile_overlap_range[2*i] - global_domain[2*i]) / tile_extents[i]) * 
          tile_extents[i] != 0 ||
          tile_overlap_range[2*i+1] - global_domain[2*i] - 
          ((tile_overlap_range[2*i+1] - global_domain[2*i]) / tile_extents[i]) * tile_extents[i] 
               != tile_extents[i] - 1) {
          overlap = PARTIAL_NON_CONTIG;
          break;
        }
      }
    } else if(cell_order == ArraySchema::TILEDB_AS_CO_COLUMN_MAJOR) { 
      // Column major
      for(int i=dim_num-2; i>=0; --i) {
        if(tile_overlap_range[2*i] - global_domain[2*i] - 
          ((tile_overlap_range[2*i] - global_domain[2*i]) / tile_extents[i]) * 
          tile_extents[i] != 0 ||
          tile_overlap_range[2*i+1] - global_domain[2*i] - 
          ((tile_overlap_range[2*i+1] - global_domain[2*i]) / tile_extents[i]) * tile_extents[i] 
               != tile_extents[i] - 1) {
          overlap = PARTIAL_NON_CONTIG;
          break;
        }
      }
    }
  }

  // Contiguous cells, single cell range
  if(overlap == FULL || overlap == PARTIAL_CONTIG) {
    void* cell_range = malloc(cell_range_size);
    T* cell_range_T = static_cast<T*>(cell_range);
    for(int i=0; i<dim_num; ++i) {
      cell_range_T[i] = tile_overlap_range[2*i];
      cell_range_T[dim_num + i] = tile_overlap_range[2*i+1];
    }
    fragment_cell_ranges.push_back(
        FragmentCellRange(fragment_info, cell_range));
  } else { // Non-contiguous cells, multiple ranges
    // Initialize the coordinates at the beginning of the global range
    T* coords = new T[dim_num];
    for(int i=0; i<dim_num; ++i)
      coords[i] = tile_overlap_range[2*i];

    // Handle the different cell orders
    int i;
    if(cell_order == ArraySchema::TILEDB_AS_CO_ROW_MAJOR) {           // ROW
      while(coords[0] <= tile_overlap_range[1]) {
        // Make a cell range representing a slab       
        void* cell_range = malloc(cell_range_size);
        T* cell_range_T = static_cast<T*>(cell_range);
        for(int i=0; i<dim_num-1; ++i) { 
          cell_range_T[i] = coords[i];
          cell_range_T[dim_num+i] = coords[i];
        }
        cell_range_T[dim_num-1] = tile_overlap_range[2*(dim_num-1)];
        cell_range_T[2*dim_num-1] = tile_overlap_range[2*(dim_num-1)+1];

        // Insert the new range into the result vector
        fragment_cell_ranges.push_back(
            FragmentCellRange(fragment_info, cell_range));
 
        // Advance coordinates
        i=dim_num-2;
        ++coords[i];
        while(i > 0 && coords[i] > tile_overlap_range[2*i+1]) {
          coords[i] = tile_overlap_range[2*i];
          ++coords[--i];
        } 
      }
    } else if(cell_order == ArraySchema::TILEDB_AS_CO_COLUMN_MAJOR) { // COLUMN
      while(coords[dim_num-1] <= tile_overlap_range[2*(dim_num-1)+1]) {
        // Make a cell range representing a slab       
        void* cell_range = malloc(cell_range_size);
        T* cell_range_T = static_cast<T*>(cell_range);
        for(int i=dim_num-1; i>0; --i) { 
          cell_range_T[i] = coords[i];
          cell_range_T[dim_num+i] = coords[i];
        }
        cell_range_T[0] = tile_overlap_range[0];
        cell_range_T[dim_num] = tile_overlap_range[1];

        // Insert the new range into the result vector
        fragment_cell_ranges.push_back(
            FragmentCellRange(fragment_info, cell_range));
 
        // Advance coordinates
        i=1;
        ++coords[i];
        while(i <dim_num-1 && coords[i] > tile_overlap_range[2*i+1]) {
          coords[i] = tile_overlap_range[2*i];
          ++coords[++i];
        } 
      }
    } else {
      assert(0);
    }

    delete [] coords;
  }

  // Clean up
  delete [] tile_overlap_range;

*/
}

const void* ReadState::get_global_tile_coords() const {
  return overlapping_tiles_.back().global_coords_;
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

void ReadState::clean_up_processed_overlapping_tiles() {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();

  // Find the minimum overlapping tile position across all attributes
  const std::vector<int>& attribute_ids = fragment_->array()->attribute_ids();
  int attribute_id_num = attribute_ids.size(); 
  int min_pos = overlapping_tiles_pos_[0];
  for(int i=1; i<attribute_id_num; ++i) 
    if(overlapping_tiles_pos_[attribute_ids[i]] < min_pos) 
      min_pos = overlapping_tiles_pos_[attribute_ids[i]];

  // Clean up processed overlapping tiles
  if(min_pos != 0) {
    // Clear memory
    for(int i=0; i<min_pos; ++i) {
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

    // Remove overlapping tile elements from the vector
    std::vector<OverlappingTile>::iterator it_first = 
         overlapping_tiles_.begin(); 
    std::vector<OverlappingTile>::iterator it_last = it_first + min_pos; 
    overlapping_tiles_.erase(it_first, it_last); 

    // Update the positions
    for(int i=0; i<attribute_num+1; ++i)
      if(overlapping_tiles_pos_[i] != 0)
        overlapping_tiles_pos_[i] -= min_pos;
  }
}

void ReadState::compute_bytes_to_copy(
    int attribute_id,
    int64_t start_cell_pos,
    int64_t& end_cell_pos,
    size_t buffer_free_space,
    size_t buffer_var_free_space,
    size_t& bytes_to_copy,
    size_t& bytes_var_to_copy) const {
  // Handle trivial case
  if(buffer_free_space == 0 || buffer_var_free_space == 0) {
    bytes_to_copy = 0;
    bytes_var_to_copy = 0;
    return;
  }

  // For easy reference
  int64_t cell_num = 
      overlapping_tiles_[overlapping_tiles_pos_[attribute_id]].cell_num_; 
  const size_t* tile = static_cast<const size_t*>(tiles_[attribute_id]);

  // Calculate bytes to copy from the variable tile
  if(end_cell_pos + 1 < cell_num) 
    bytes_var_to_copy = tile[end_cell_pos + 1] - tile[start_cell_pos];
  else 
    bytes_var_to_copy = tiles_var_sizes_[attribute_id] - tile[start_cell_pos];

  // If bytes do not fit in variable buffer, we need to adjust
  if(bytes_var_to_copy > buffer_var_free_space) {
    // Perform binary search
    int64_t min = start_cell_pos;
    int64_t max = end_cell_pos+1;
    int64_t med;
    //Invariant: all data upto min cell (not including min cell) fits in buffer
    //all data upto max (not including max cell) does not fit into buffer
    while(min < max-1) {
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

    // Determine the start position of the range
    if(min == max-1)  
      end_cell_pos = min - 1;     
    else 
      end_cell_pos = med - 1;  

    // Update variable bytes to copy
    bytes_var_to_copy = tile[end_cell_pos + 1] - tile[start_cell_pos];
  }

  // Update bytes to copy
  bytes_to_copy = 
      (end_cell_pos - start_cell_pos + 1) * TILEDB_CELL_VAR_OFFSET_SIZE;
}

template<class T>
void ReadState::compute_cell_pos_ranges() {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();
  int dim_num = array_schema->dim_num();
  const T* range = static_cast<const T*>(fragment_->array()->subarray());
  int compression = array_schema->compression(attribute_num);

  // Bring coordinates tile in main memory
  if(compression == TILEDB_GZIP)
    get_tile_from_disk_cmp_gzip(attribute_num);
  else
    get_tile_from_disk_cmp_none(attribute_num);

  // Invoke the proper function based on the type of range and overlap
  if(is_unary_subarray(range, dim_num))
    compute_cell_pos_ranges_unary<T>();
  else if(overlapping_tiles_.back().overlap_ == PARTIAL_CONTIG)
    compute_cell_pos_ranges_contig<T>();
  else if(overlapping_tiles_.back().overlap_ == PARTIAL_NON_CONTIG)
    compute_cell_pos_ranges_non_contig<T>();
}

template<class T>
void ReadState::compute_cell_pos_ranges_contig() {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int cell_order = array_schema->cell_order();

  // Invoke the proper function based on the cell order
  if(cell_order == TILEDB_ROW_MAJOR)
    compute_cell_pos_ranges_contig_row<T>();
  else if(cell_order == TILEDB_COL_MAJOR)
    compute_cell_pos_ranges_contig_col<T>();
}

template<class T>
void ReadState::compute_cell_pos_ranges_contig_col() {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();
  int dim_num = array_schema->dim_num();
  OverlappingTile& overlapping_tile = overlapping_tiles_.back();
  const T* range = static_cast<const T*>(overlapping_tile.overlap_range_);
  int64_t cell_num = overlapping_tile.cell_num_;
  const T* tile = static_cast<const T*>(tiles_[attribute_num]);
  const T* cell_coords;
  int64_t start_cell_pos, end_cell_pos;

  // Calculate range coordinates
  T* range_min_coords = new T[dim_num];
  for(int i=0; i<dim_num; ++i)
    memcpy(&range_min_coords[i], &range[2*i], sizeof(T)); 
  T* range_max_coords = new T[dim_num];
  for(int i=0; i<dim_num; ++i)
    memcpy(&range_max_coords[i], &range[2*i+1], sizeof(T)); 

  // -- Compute the start cell position

  // Perform binary search in the coordinates tile
  int64_t min = 0;
  int64_t max = cell_num - 1;
  int64_t med;
  while(min <= max) {
    med = min + ((max - min) / 2);

    // Get cell
    cell_coords = &tile[med*dim_num];

    // Calculate precedence
    if(cmp_col_order(
           range_min_coords,
           cell_coords,
           dim_num) < 0) {   // Range min precedes cell
      max = med-1;
    } else if(cmp_col_order(
           range_min_coords,
           cell_coords,
           dim_num) > 0) {   // Range min succeeds cell
      min = med+1;
    } else {                 // Range min is cell
      break; 
    }
  }

  // Determine the start position of the range
  if(max < min)    // Range min precedes the cell at position min  
    start_cell_pos = min;     
  else             // Range min is a cell
    start_cell_pos = med;

  // -- Compute the end cell position

  // Perform binary search in the coordinates tile
  min = 0;
  max = cell_num - 1;
  while(min <= max) {
    med = min + ((max - min) / 2);

    // Get cell
    cell_coords = &tile[med*dim_num];
     
    // Calculate precedence
    if(cmp_col_order(
           range_max_coords,
           cell_coords,
           dim_num) < 0) {   // Range max precedes cell
      max = med-1;
    } else if(cmp_col_order(
           range_max_coords,
           cell_coords,
           dim_num) > 0) {   // Range max succeeds cell
      min = med+1;
    } else {                 // Range max is cell
      break; 
    }
  }

  // Determine the end position of the range
  if(max < min)    // Range max succeeds the cell at position max  
    end_cell_pos = max;     
  else             // Range min is a cell
    end_cell_pos = med;

  // Add the cell position range
  if(start_cell_pos <= end_cell_pos)
    overlapping_tile.cell_pos_ranges_.push_back(
        std::pair<int64_t, int64_t>(start_cell_pos, end_cell_pos));

  // Clean up
  delete [] range_min_coords;
  delete [] range_max_coords;
}

template<class T>
void ReadState::compute_cell_pos_ranges_contig_row() {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();
  int dim_num = array_schema->dim_num();
  OverlappingTile& overlapping_tile = overlapping_tiles_.back();
  const T* range = static_cast<const T*>(overlapping_tile.overlap_range_);
  int64_t cell_num = overlapping_tile.cell_num_;
  const T* tile = static_cast<const T*>(tiles_[attribute_num]);
  const T* cell_coords;
  int64_t start_cell_pos, end_cell_pos;

  // Calculate range coordinates
  T* range_min_coords = new T[dim_num];
  for(int i=0; i<dim_num; ++i)
    memcpy(&range_min_coords[i], &range[2*i], sizeof(T)); 
  T* range_max_coords = new T[dim_num];
  for(int i=0; i<dim_num; ++i)
    memcpy(&range_max_coords[i], &range[2*i+1], sizeof(T)); 

  // -- Compute the start cell position

  // Perform binary search in the coordinates tile
  int64_t min = 0;
  int64_t max = cell_num - 1;
  int64_t med;
  while(min <= max) {
    med = min + ((max - min) / 2);

    // Get cell
    cell_coords = &tile[med*dim_num];

    // Calculate precedence
    if(cmp_row_order(
           range_min_coords,
           cell_coords,
           dim_num) < 0) {   // Range min precedes cell
      max = med-1;
    } else if(cmp_row_order(
           range_min_coords,
           cell_coords,
           dim_num) > 0) {   // Range min succeeds cell
      min = med+1;
    } else {                 // Range min is cell
      break; 
    }
  }

  // Determine the start position of the range
  if(max < min)    // Range min precedes the cell at position min  
    start_cell_pos = min;     
  else             // Range min is a cell
    start_cell_pos = med;

  // -- Compute the end cell position

  // Perform binary search in the coordinates tile
  min = 0;
  max = cell_num - 1;
  while(min <= max) {
    med = min + ((max - min) / 2);

    // Get cell
    cell_coords = &tile[med*dim_num];
     
    // Calculate precedence
    if(cmp_row_order(
           range_max_coords,
           cell_coords,
           dim_num) < 0) {   // Range max precedes cell
      max = med-1;
    } else if(cmp_row_order(
           range_max_coords,
           cell_coords,
           dim_num) > 0) {   // Range max succeeds cell
      min = med+1;
    } else {                 // Range max is cell
      break; 
    }
  }

  // Determine the end position of the range
  if(max < min)    // Range max succeeds the cell at position max  
    end_cell_pos = max;     
  else             // Range min is a cell
    end_cell_pos = med;

  // Add the cell position range
  if(start_cell_pos <= end_cell_pos)
    overlapping_tile.cell_pos_ranges_.push_back(
        std::pair<int64_t, int64_t>(start_cell_pos, end_cell_pos));

  // Clean up
  delete [] range_min_coords;
  delete [] range_max_coords;
}

template<class T>
void ReadState::compute_cell_pos_ranges_non_contig() {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int cell_order = array_schema->cell_order();

  // Invoke the proper function based on the cell order
  if(cell_order == TILEDB_ROW_MAJOR ||
     cell_order == TILEDB_COL_MAJOR) {
    // Find the largest range in which there are results
    compute_cell_pos_ranges_contig<T>();
    if(overlapping_tiles_.back().cell_pos_ranges_.size() == 0) // No results
      return; 
    int64_t start_pos = overlapping_tiles_.back().cell_pos_ranges_[0].first;
    int64_t end_pos = overlapping_tiles_.back().cell_pos_ranges_[0].second;
    overlapping_tiles_.back().cell_pos_ranges_.clear();

    // Scan each cell in the cell range and check if it is in the query range
    compute_cell_pos_ranges_scan<T>(start_pos, end_pos);
  } else if(cell_order == TILEDB_HILBERT) {
    // Scan each cell in the entire tile and check if it is inside the range
    int64_t cell_num = array_schema->capacity();
    compute_cell_pos_ranges_scan<T>(0, cell_num-1);
  }
}

template<class T>
void ReadState::compute_cell_pos_ranges_scan(
    int64_t start_pos,
    int64_t end_pos) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();
  int dim_num = array_schema->dim_num();
  const T* range = static_cast<const T*>(fragment_->array()->subarray());
  const T* tile = static_cast<const T*>(tiles_[attribute_num]);
  const T* cell;
  int64_t current_start_pos, current_end_pos = -2; 
  OverlappingTile& overlapping_tile = overlapping_tiles_.back();

  // Compute the cell position ranges
  for(int64_t i=start_pos; i<=end_pos; ++i) {
    cell = &tile[i*dim_num];
    if(cell_in_subarray<T>(cell, range, dim_num)) {
      if(i-1 == current_end_pos) { // The range is expanded
       ++current_end_pos;
      } else {                     // A new range starts
        current_start_pos = i;
        current_end_pos = i;
      } 
    } else {
      if(i-1 == current_end_pos) { // The range need to be added to the list
        overlapping_tile.cell_pos_ranges_.push_back(
            std::pair<int64_t, int64_t>(current_start_pos, current_end_pos));
        current_end_pos = -2; // This indicates that there is no active range
      }
    }
  }

  // Add last cell range
  if(current_end_pos != -2)
    overlapping_tile.cell_pos_ranges_.push_back(
      std::pair<int64_t, int64_t>(current_start_pos, current_end_pos));
}

template<class T>
void ReadState::compute_cell_pos_ranges_unary() {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int cell_order = array_schema->cell_order();

  // Invoke the proper function based on the cell order
  if(cell_order == TILEDB_ROW_MAJOR)
    compute_cell_pos_ranges_unary_row<T>();
  else if(cell_order == TILEDB_COL_MAJOR)
    compute_cell_pos_ranges_unary_col<T>();
  else if(cell_order == TILEDB_HILBERT)
    compute_cell_pos_ranges_unary_hil<T>();
}

template<class T>
void ReadState::compute_cell_pos_ranges_unary_col() {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();
  int dim_num = array_schema->dim_num();
  OverlappingTile& overlapping_tile = overlapping_tiles_.back();
  const T* range = static_cast<const T*>(overlapping_tile.overlap_range_);
  int64_t cell_num = overlapping_tile.cell_num_;
  const T* tile = static_cast<const T*>(tiles_[attribute_num]);
  const T* cell_coords;

  // Calculate range coordinates
  T* range_coords = new T[dim_num];
  for(int i=0; i<dim_num; ++i)
    memcpy(&range_coords[i], &range[2*i], sizeof(T)); 

  // Perform binary search in the coordinates tile
  int64_t min = 0;
  int64_t max = cell_num - 1;
  int64_t med;
  while(min <= max) {
    med = min + ((max - min) / 2);

    // Get cell
    cell_coords = &tile[med*dim_num];
     
    // Calculate precedence
    if(cmp_col_order(
           range_coords,
           cell_coords,
           dim_num) < 0) {   // Range precedes cell
      max = med-1;
    } else if(cmp_col_order(
           range_coords,
           cell_coords,
           dim_num) > 0) {   // Range succeeds cell
      min = med+1;
    } else {                 // Range is cell
      break; 
    }
  }

  // Determine the unary cell range
  if(max >= min)      
    overlapping_tile.cell_pos_ranges_.push_back(
        std::pair<int64_t, int64_t>(med, med));     

  // Clean up
  delete [] range_coords;
}

template<class T>
void ReadState::compute_cell_pos_ranges_unary_hil() {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();
  int dim_num = array_schema->dim_num();
  OverlappingTile& overlapping_tile = overlapping_tiles_.back();
  const T* range = static_cast<const T*>(overlapping_tile.overlap_range_);
  int64_t cell_num = overlapping_tile.cell_num_;
  const T* tile = static_cast<const T*>(tiles_[attribute_num]);
  const T* cell_coords;
  int64_t cell_coords_id;

  // Calculate range coordinates and Hilbert id
  T* range_coords = new T[dim_num];
  for(int i=0; i<dim_num; ++i)
    memcpy(&range_coords[i], &range[2*i], sizeof(T)); 
  int64_t range_coords_id = array_schema->hilbert_id(range_coords);

  // Perform binary search in the coordinates tile
  int64_t min = 0;
  int64_t max = cell_num - 1;
  int64_t med;
  while(min <= max) {
    med = min + ((max - min) / 2);

    // Get cell
    cell_coords = &tile[med*dim_num];
    cell_coords_id = array_schema->hilbert_id(cell_coords);
     
    // Calculate precedence
    if(cmp_row_order(
           range_coords_id,
           range_coords,
           cell_coords_id,
           cell_coords,
           dim_num) < 0) {   // Range precedes cell
      max = med-1;
    } else if(cmp_row_order(
           range_coords_id,
           range_coords,
           cell_coords_id,
           cell_coords,
           dim_num) > 0) {   // Range succeeds cell
      min = med+1;
    } else {                 // Range is cell
      break; 
    }
  }

  // Determine the unary cell range
  if(max >= min)      
    overlapping_tile.cell_pos_ranges_.push_back(
        std::pair<int64_t, int64_t>(med, med));     

  // Clean up
  delete [] range_coords;

}

template<class T>
void ReadState::compute_cell_pos_ranges_unary_row() {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();
  int dim_num = array_schema->dim_num();
  OverlappingTile& overlapping_tile = overlapping_tiles_.back();
  const T* range = static_cast<const T*>(overlapping_tile.overlap_range_);
  int64_t cell_num = overlapping_tile.cell_num_;
  const T* tile = static_cast<const T*>(tiles_[attribute_num]);
  const T* cell_coords;

  // Calculate range coordinates
  T* range_coords = new T[dim_num];
  for(int i=0; i<dim_num; ++i)
    memcpy(&range_coords[i], &range[2*i], sizeof(T)); 

  // Perform binary search in the coordinates tile
  int64_t min = 0;
  int64_t max = cell_num - 1;
  int64_t med;
  while(min <= max) {
    med = min + ((max - min) / 2);

    // Get cell
    cell_coords = &tile[med*dim_num];

    // Calculate precedence
    if(cmp_row_order(
           range_coords,
           cell_coords,
           dim_num) < 0) {   // Range precedes cell
      max = med-1;
    } else if(cmp_row_order(
           range_coords,
           cell_coords,
           dim_num) > 0) {   // Range succeeds cell
      min = med+1;
    } else {                 // Range is cell
      break; 
    }
  }

  // Determine the unary cell range
  if(max >= min)      
    overlapping_tile.cell_pos_ranges_.push_back(
        std::pair<int64_t, int64_t>(med, med));     

  // Clean up
  delete [] range_coords;
}

int ReadState::compute_tile_var_size(
    int attribute_id, 
    int tile_pos, 
    size_t& tile_var_size) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();

  // =========== Compression case =========== // 
  if(array_schema->compression(attribute_id) == TILEDB_GZIP) {
    tile_var_size = book_keeping_->tile_var_sizes()[attribute_id][tile_pos]; 
    return TILEDB_RS_OK;
  }

  // ========= Non-Compression case ========= // 

  // For easy reference
  size_t full_tile_size = fragment_->tile_size(attribute_id);
  int64_t tile_num = book_keeping_->tile_num();

  // Prepare attribute file name
  std::string filename = fragment_->fragment_name() + "/" +
                         array_schema->attribute(attribute_id) +
                         TILEDB_FILE_SUFFIX;

  // Open file
  int fd = open(filename.c_str(), O_RDONLY);
  if(fd == -1) {
    PRINT_ERROR("Cannot compute variable tile size; File opening error");
    return TILEDB_RS_ERR;
  }

  // Find file offset where the tile begins
  off_t file_offset = tile_pos * full_tile_size;

  // Read start variable tile offset
  size_t start_tile_var_offset;
  lseek(fd, file_offset, SEEK_SET); 
  size_t bytes_read = ::read(
                            fd, 
                            &start_tile_var_offset, 
                            TILEDB_CELL_VAR_OFFSET_SIZE);
  if(bytes_read != TILEDB_CELL_VAR_OFFSET_SIZE) {
    PRINT_ERROR("Cannot compute variable tile size; File reading error");
    return TILEDB_RS_ERR;
  }

  // Compute the end of the variable tile
  size_t end_tile_var_offset;
  if(tile_pos != tile_num - 1) { // Not the last tile
    lseek(fd, file_offset + full_tile_size, SEEK_SET); 
    bytes_read = ::read(fd, &end_tile_var_offset, TILEDB_CELL_VAR_OFFSET_SIZE);
    if(bytes_read != TILEDB_CELL_VAR_OFFSET_SIZE) {
      PRINT_ERROR("Cannot compute variable tile size; File reading error");
      return TILEDB_RS_ERR;
    }
  } else { // Last tile
    // Prepare variable attribute file name
    filename = fragment_->fragment_name() + "/" +
               array_schema->attribute(attribute_id) + "_var" +
               TILEDB_FILE_SUFFIX;
    
    // The end of the tile is the end of the file
    end_tile_var_offset = file_size(filename);
  }
  
  // Close file
  if(close(fd)) {
    PRINT_ERROR("Cannot compute variable tile size; File closing error");
    return TILEDB_RS_ERR;
  }

  // Set the returned tile size
  tile_var_size = end_tile_var_offset - start_tile_var_offset;

  // Success
  return TILEDB_RS_OK;
}

template<class T>
void ReadState::copy_from_tile_buffer_dense(
    int attribute_id,
    void* buffer,
    size_t buffer_size,
    size_t& buffer_offset) {
  // Invoke the proper function based on the overlap type
  Overlap overlap = 
      overlapping_tiles_[overlapping_tiles_pos_[attribute_id]].overlap_;
  if(overlap == FULL)
    copy_from_tile_buffer_full(
        attribute_id, 
        buffer, 
        buffer_size, 
        buffer_offset);
  else if(overlap == PARTIAL_NON_CONTIG)
    copy_from_tile_buffer_partial_non_contig_dense<T>(
        attribute_id, 
        buffer, 
        buffer_size, 
        buffer_offset);
  else if(overlap == PARTIAL_CONTIG)
    copy_from_tile_buffer_partial_contig_dense<T>(
        attribute_id, 
        buffer, 
        buffer_size, 
        buffer_offset);
}

template<class T>
void ReadState::copy_from_tile_buffer_dense_var(
    int attribute_id,
    void* buffer,
    size_t buffer_size,
    size_t& buffer_offset,
    void* buffer_var,
    size_t buffer_var_size,
    size_t& buffer_var_offset) {
  // Invoke the proper function based on the overlap type
  Overlap overlap = 
      overlapping_tiles_[overlapping_tiles_pos_[attribute_id]].overlap_;
  if(overlap == FULL)
    copy_from_tile_buffer_full_var(
        attribute_id, 
        buffer, 
        buffer_size, 
        buffer_offset,
        buffer_var, 
        buffer_var_size,
        buffer_var_offset);
  else if(overlap == PARTIAL_NON_CONTIG)
    copy_from_tile_buffer_partial_non_contig_dense_var<T>(
        attribute_id, 
        buffer, 
        buffer_size, 
        buffer_offset,
        buffer_var, 
        buffer_var_size,
        buffer_var_offset);
  else if(overlap == PARTIAL_CONTIG)
    copy_from_tile_buffer_partial_contig_dense_var<T>(
        attribute_id, 
        buffer, 
        buffer_size, 
        buffer_offset,
        buffer_var, 
        buffer_var_size,
        buffer_var_offset);
}

void ReadState::copy_from_tile_buffer_full(
    int attribute_id,
    void* buffer,
    size_t buffer_size,
    size_t& buffer_offset) {
  // Calculate the appropriate sizes
  size_t bytes_left_to_copy = 
      tiles_sizes_[attribute_id] - tiles_offsets_[attribute_id];
  assert(bytes_left_to_copy != 0);
  size_t buffer_free_space = buffer_size - buffer_offset;
  size_t bytes_to_copy = std::min(bytes_left_to_copy, buffer_free_space); 

  // If there is something to copy
  if(bytes_to_copy != 0) {
    // Copy
    char* buffer_c = static_cast<char*>(buffer);
    char* tile = static_cast<char*>(tiles_[attribute_id]);
    memcpy(
        buffer_c + buffer_offset, 
        tile + tiles_offsets_[attribute_id], 
        bytes_to_copy);
    tiles_offsets_[attribute_id] += bytes_to_copy;
    buffer_offset += bytes_to_copy;
    bytes_left_to_copy -= bytes_to_copy;
  }

  // Check overflow
  if(bytes_left_to_copy > 0) {             // Buffer overflow
    assert(buffer_offset == buffer_size);
    overflow_[attribute_id] = true;
  } else {                                 // Finished copying this tile
    assert(tiles_offsets_[attribute_id] == tiles_sizes_[attribute_id]);
    ++overlapping_tiles_pos_[attribute_id];
  }
}

void ReadState::copy_from_tile_buffer_full_var(
    int attribute_id,
    void* buffer,
    size_t buffer_size,
    size_t& buffer_offset,
    void* buffer_var,
    size_t buffer_var_size,
    size_t& buffer_var_offset) {
  // For easy reference
  char* buffer_c = static_cast<char*>(buffer);
  void* buffer_start = buffer_c + buffer_offset;
  char* buffer_var_c = static_cast<char*>(buffer_var);
  void* buffer_var_start = buffer_var_c + buffer_var_offset;
  char* tile = static_cast<char*>(tiles_[attribute_id]);
  void* tile_start = tile + tiles_offsets_[attribute_id];
  char* tile_var = static_cast<char*>(tiles_var_[attribute_id]);
  void* tile_var_start = tile_var + tiles_var_offsets_[attribute_id];
  size_t bytes_left_to_copy = 
      tiles_sizes_[attribute_id] - tiles_offsets_[attribute_id];
  size_t bytes_var_left_to_copy = 
      tiles_var_sizes_[attribute_id] - tiles_var_offsets_[attribute_id];

  // Compute actual bytes to copy 
  size_t buffer_free_space = buffer_size - buffer_offset;
  size_t buffer_var_free_space = buffer_var_size - buffer_var_offset;
  size_t bytes_to_copy, bytes_var_to_copy;
  int64_t start_cell_pos = 
      tiles_offsets_[attribute_id] / TILEDB_CELL_VAR_OFFSET_SIZE;
  int64_t end_cell_pos = 
      start_cell_pos - 1 + 
      (std::min(bytes_left_to_copy, buffer_free_space) / 
       sizeof(TILEDB_CELL_VAR_OFFSET_SIZE));
  compute_bytes_to_copy(
      attribute_id,
      start_cell_pos,
      end_cell_pos,
      buffer_free_space,
      buffer_var_free_space,
      bytes_to_copy,
      bytes_var_to_copy);

  // If there is something to copy
  if(bytes_to_copy != 0) {
    // Copy from fixed tile
    memcpy(buffer_start, tile_start, bytes_to_copy);
    tiles_offsets_[attribute_id] += bytes_to_copy;
    buffer_offset += bytes_to_copy;
    bytes_left_to_copy -= bytes_to_copy;

    // Shift offsets
    shift_var_offsets(
        buffer_start, 
        end_cell_pos - start_cell_pos + 1, 
        buffer_var_offset); 

    // Copy from variable tile
    memcpy(buffer_var_start, tile_var_start, bytes_var_to_copy);
    tiles_var_offsets_[attribute_id] += bytes_var_to_copy;
    buffer_var_offset += bytes_var_to_copy;
    bytes_var_left_to_copy -= bytes_var_to_copy;
  }

  // Check if finished copying this tile
  if(bytes_left_to_copy == 0) {               // Tile is done 
    assert(tiles_offsets_[attribute_id] == tiles_sizes_[attribute_id]);
    assert(tiles_var_offsets_[attribute_id] == tiles_var_sizes_[attribute_id]);
    ++overlapping_tiles_pos_[attribute_id];
  } else {                                    // Overflow
    overflow_[attribute_id] = true;
  }
}

template<class T>
void ReadState::copy_from_tile_buffer_sparse(
    int attribute_id,
    void* buffer,
    size_t buffer_size,
    size_t& buffer_offset) {
  // Invoke the proper function based on the overlap type
  int64_t pos = overlapping_tiles_pos_[attribute_id];
  Overlap overlap = overlapping_tiles_[pos].overlap_;
  if(overlap == FULL)
    copy_from_tile_buffer_full(
        attribute_id, 
        buffer, 
        buffer_size, 
        buffer_offset);
  else if(overlap == PARTIAL_NON_CONTIG)
    copy_from_tile_buffer_partial_non_contig_sparse<T>(
        attribute_id, 
        buffer, 
        buffer_size, 
        buffer_offset);
  else if(overlap == PARTIAL_CONTIG)
    copy_from_tile_buffer_partial_contig_sparse<T>(
        attribute_id, 
        buffer, 
        buffer_size, 
        buffer_offset);
}

template<class T>
void ReadState::copy_from_tile_buffer_sparse_var(
    int attribute_id,
    void* buffer,
    size_t buffer_size,
    size_t& buffer_offset,
    void* buffer_var,
    size_t buffer_var_size,
    size_t& buffer_var_offset) {
  if(overlapping_tiles_[overlapping_tiles_pos_[attribute_id]].overlap_
     == FULL)
    copy_from_tile_buffer_full_var(
        attribute_id, 
        buffer, 
        buffer_size, 
        buffer_offset,
        buffer_var, 
        buffer_var_size,
        buffer_var_offset);
  else if(overlapping_tiles_[overlapping_tiles_pos_[attribute_id]].overlap_
          == PARTIAL_NON_CONTIG)
    copy_from_tile_buffer_partial_non_contig_sparse_var<T>(
        attribute_id, 
        buffer, 
        buffer_size, 
        buffer_offset,
        buffer_var, 
        buffer_var_size,
        buffer_var_offset);
  else if(overlapping_tiles_[overlapping_tiles_pos_[attribute_id]].overlap_
          == PARTIAL_CONTIG)
    copy_from_tile_buffer_partial_contig_sparse_var<T>(
        attribute_id, 
        buffer, 
        buffer_size, 
        buffer_offset,
        buffer_var, 
        buffer_var_size,
        buffer_var_offset);
}

template<class T>
void ReadState::copy_from_tile_buffer_partial_contig_dense(
    int attribute_id,
    void* buffer,
    size_t buffer_size,
    size_t& buffer_offset) {
  // Calculate free space in buffer
  size_t buffer_free_space = buffer_size - buffer_offset;
  if(buffer_free_space == 0) { // Overflow
    overflow_[attribute_id] = true;
    return;
  }
 
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  OverlappingTile& overlapping_tile = 
      overlapping_tiles_[overlapping_tiles_pos_[attribute_id]];
  const T* overlap_range = 
      static_cast<const T*>(overlapping_tile.overlap_range_);
  int dim_num = array_schema->dim_num();
  T* start_coords = new T[dim_num]; 
  for(int i=0; i<dim_num; ++i)
    start_coords[i] = overlap_range[2*i];
  char* buffer_c = static_cast<char*>(buffer);
  char* tile = static_cast<char*>(tiles_[attribute_id]);

  // Sanity check
  assert(!array_schema->var_size(attribute_id));

  // Find the offset at the beginning and end of the overlap range
  size_t cell_size = array_schema->cell_size(attribute_id);
  int64_t start_cell_pos = array_schema->get_cell_pos<T>(start_coords);
  size_t start_offset = start_cell_pos * cell_size; 
  size_t range_size = cell_num_in_subarray(overlap_range, dim_num) * cell_size;
  size_t end_offset = start_offset + range_size - 1;

  // If current tile offset is 0, set it to the beginning of the overlap range
  if(tiles_offsets_[attribute_id] == 0) 
    tiles_offsets_[attribute_id] = start_offset;

  // Calculate the total size to copy
  size_t bytes_left_to_copy = end_offset - tiles_offsets_[attribute_id] + 1;
  size_t bytes_to_copy = std::min(bytes_left_to_copy, buffer_free_space); 

  // Copy and update current buffer and tile offsets
  memcpy(
      buffer_c + buffer_offset, 
      tile + tiles_offsets_[attribute_id], 
      bytes_to_copy);
  buffer_offset += bytes_to_copy;
  tiles_offsets_[attribute_id] += bytes_to_copy; 

  // Update overlapping tile position if necessary
  if(tiles_offsets_[attribute_id] == end_offset + 1) { // This tile is done
    tiles_offsets_[attribute_id] = tiles_sizes_[attribute_id]; 
    ++overlapping_tiles_pos_[attribute_id];
  } else {                                             // Buffer overflow
    assert(buffer_offset == buffer_size); // Buffer full
    overflow_[attribute_id] = true;
  }

  // Clean up
  delete [] start_coords;
}

template<class T>
void ReadState::copy_from_tile_buffer_partial_contig_dense_var(
    int attribute_id,
    void* buffer,
    size_t buffer_size,
    size_t& buffer_offset,
    void* buffer_var,
    size_t buffer_var_size,
    size_t& buffer_var_offset) {
  // Calculate free space in buffers
  size_t buffer_free_space = buffer_size - buffer_offset;
  size_t buffer_var_free_space = buffer_var_size - buffer_var_offset;

  // Handle overflow
  if(buffer_free_space == 0 || buffer_var_free_space == 0) { // Overflow
    overflow_[attribute_id] = true; 
    return;
  }
 
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  OverlappingTile& overlapping_tile = 
      overlapping_tiles_[overlapping_tiles_pos_[attribute_id]];
  const T* overlap_range = 
      static_cast<const T*>(overlapping_tile.overlap_range_);
  int dim_num = array_schema->dim_num();
  T* start_coords = new T[dim_num]; 
  for(int i=0; i<dim_num; ++i)
    start_coords[i] = overlap_range[2*i];

  // Sanity check
  assert(array_schema->var_size(attribute_id));

  // Find the offset at the beginning of the overlap range in the fixed tile
  int64_t start_cell_pos = array_schema->get_cell_pos<T>(start_coords);
  size_t start_offset = start_cell_pos * TILEDB_CELL_VAR_OFFSET_SIZE; 
  int64_t cell_num_in_range = ::cell_num_in_subarray(overlap_range, dim_num);
  size_t range_size = cell_num_in_range * TILEDB_CELL_VAR_OFFSET_SIZE;
  int64_t end_cell_pos = start_cell_pos + cell_num_in_range - 1;
  size_t end_offset = start_offset + range_size - 1;

  // Compute actual bytes to copy 
  size_t bytes_to_copy, bytes_var_to_copy;
  compute_bytes_to_copy(
      attribute_id,
      start_cell_pos,
      end_cell_pos,
      buffer_free_space,
      buffer_var_free_space,
      bytes_to_copy,
      bytes_var_to_copy);

  // Overflow
  if(bytes_to_copy == 0) {
    overflow_[attribute_id] = true;
    delete [] start_coords;
    return;
  }

  // Set properly the tile offsets
  if(tiles_offsets_[attribute_id] == 0) {
    tiles_offsets_[attribute_id] = start_offset;
    tiles_var_offsets_[attribute_id] = 
        static_cast<const size_t*>(tiles_[attribute_id])[start_cell_pos];
  }

  // For easy reference
  char* buffer_c = static_cast<char*>(buffer);
  char* buffer_var_c = static_cast<char*>(buffer_var);
  void* buffer_start = buffer_c + buffer_offset;
  void* buffer_var_start = buffer_var_c + buffer_var_offset;
  char* tile = static_cast<char*>(tiles_[attribute_id]);
  char* tile_var = static_cast<char*>(tiles_var_[attribute_id]);
  const void* tile_start = tile + tiles_offsets_[attribute_id];
  const void* tile_var_start = tile_var + tiles_var_offsets_[attribute_id];

  // Copy and update current buffer and tile offsets
  memcpy(buffer_start, tile_start, bytes_to_copy);
  buffer_offset += bytes_to_copy;
  tiles_offsets_[attribute_id] += bytes_to_copy; 

  // Shift variable offsets
  shift_var_offsets(
      buffer_start, 
      end_cell_pos - start_cell_pos + 1, 
      buffer_var_offset); 

  // Copy and update current variable buffer and variable tile offsets
  memcpy(buffer_var_start, tile_var_start, bytes_var_to_copy);
  buffer_var_offset += bytes_var_to_copy;
  tiles_var_offsets_[attribute_id] += bytes_var_to_copy; 

  // Update overlapping tile position if necessary
  if(tiles_offsets_[attribute_id] == end_offset + 1) {      // Tile is done
    tiles_offsets_[attribute_id] = tiles_sizes_[attribute_id]; 
    tiles_var_offsets_[attribute_id] = tiles_var_sizes_[attribute_id]; 
    ++overlapping_tiles_pos_[attribute_id];
  } else {                                                  // Overflow
    overflow_[attribute_id] = true;
  }

  // Clean up
  delete [] start_coords;
}

template<class T>
void ReadState::copy_from_tile_buffer_partial_contig_sparse(
    int attribute_id,
    void* buffer,
    size_t buffer_size,
    size_t& buffer_offset) {
  // Calculate free space in buffer
  size_t buffer_free_space = buffer_size - buffer_offset;
  if(buffer_free_space == 0) { // Overflow
    overflow_[attribute_id] = true;
    return;
  }
 
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  size_t cell_size = array_schema->cell_size(attribute_id);
  OverlappingTile& overlapping_tile = 
      overlapping_tiles_[overlapping_tiles_pos_[attribute_id]];
  char* buffer_c = static_cast<char*>(buffer);
  char* tile = static_cast<char*>(tiles_[attribute_id]);
  assert(overlapping_tile.cell_pos_ranges_.size() <= 1);

  // If there are no qualifying cells in this tile, return
  if(overlapping_tile.cell_pos_ranges_.size() == 0)
    return;

  // Calculate start and end offset in the tile
  size_t start_offset = overlapping_tile.cell_pos_ranges_[0].first * cell_size; 
  size_t end_offset = 
      (overlapping_tile.cell_pos_ranges_[0].second + 1) * cell_size - 1;

  // Sanity check
  assert(!array_schema->var_size(attribute_id));

  // Potentially set the tile offset to the beginning of the current range
  if(tiles_offsets_[attribute_id] < start_offset) 
    tiles_offsets_[attribute_id] = start_offset;

  // Calculate the total size to copy
  size_t bytes_left_to_copy = end_offset - tiles_offsets_[attribute_id] + 1;
  size_t bytes_to_copy = std::min(bytes_left_to_copy, buffer_free_space); 

  // Copy and update current buffer and tile offsets
  memcpy(
      buffer_c + buffer_offset, 
      tile + tiles_offsets_[attribute_id], 
      bytes_to_copy);
  buffer_offset += bytes_to_copy;
  tiles_offsets_[attribute_id] += bytes_to_copy; 

  // Update overlapping tile position if necessary
  if(tiles_offsets_[attribute_id] == end_offset + 1) { // This tile is done
    tiles_offsets_[attribute_id] = tiles_sizes_[attribute_id]; 
    ++overlapping_tiles_pos_[attribute_id];
  } else { // Check for overflow
    assert(buffer_offset == buffer_size);              // Buffer overflow
    overflow_[attribute_id] = true;
  }
}

template<class T>
void ReadState::copy_from_tile_buffer_partial_contig_sparse_var(
    int attribute_id,
    void* buffer,
    size_t buffer_size,
    size_t& buffer_offset,
    void* buffer_var,
    size_t buffer_var_size,
    size_t& buffer_var_offset) {
  // Calculate free space in buffers
  size_t buffer_free_space = buffer_size - buffer_offset;
  size_t buffer_var_free_space = buffer_var_size - buffer_var_offset;

  // Handle overflow
  if(buffer_free_space == 0 || buffer_var_free_space == 0) { // Overflow
    overflow_[attribute_id] = true; 
    return;
  }

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  size_t cell_size = TILEDB_CELL_VAR_OFFSET_SIZE;
  OverlappingTile& overlapping_tile = 
      overlapping_tiles_[overlapping_tiles_pos_[attribute_id]];
  char* buffer_c = static_cast<char*>(buffer);
  char* buffer_var_c = static_cast<char*>(buffer_var);
  char* tile = static_cast<char*>(tiles_[attribute_id]);
  char* tile_var = static_cast<char*>(tiles_var_[attribute_id]);

  // Sanity checks
  assert(overlapping_tile.cell_pos_ranges_.size() <= 1);
  assert(array_schema->var_size(attribute_id));

  // If there are no qualifying cells in this tile, return
  if(overlapping_tile.cell_pos_ranges_.size() == 0)
    return;

  // Calculate start and end offset in the tile
  size_t start_offset = overlapping_tile.cell_pos_ranges_[0].first * cell_size; 
  size_t end_offset = 
      (overlapping_tile.cell_pos_ranges_[0].second + 1) * cell_size - 1;

  // Compute actual bytes to copy 
  size_t bytes_to_copy, bytes_var_to_copy;
  int64_t start_cell_pos = overlapping_tile.cell_pos_ranges_[0].first;
  int64_t end_cell_pos = overlapping_tile.cell_pos_ranges_[0].second;
  compute_bytes_to_copy(
      attribute_id,
      start_cell_pos,
      end_cell_pos,
      buffer_free_space,
      buffer_var_free_space,
      bytes_to_copy,
      bytes_var_to_copy);

  // Overflow
  if(bytes_to_copy == 0) {
    overflow_[attribute_id] = true;
    return;
  }

  // Set properly the tile offsets
  if(tiles_offsets_[attribute_id] == 0) {
    tiles_offsets_[attribute_id] = start_offset;
    tiles_var_offsets_[attribute_id] = 
        static_cast<const size_t*>(tiles_[attribute_id])[start_cell_pos];
  }

  // Copy and update current buffer and tile offsets
  memcpy(
      buffer_c + buffer_offset, 
      tile + tiles_offsets_[attribute_id], 
      bytes_to_copy);

  // Shift variable offsets
  shift_var_offsets(
      buffer_c + buffer_offset,
      end_cell_pos - start_cell_pos + 1, 
      buffer_var_offset);
  buffer_offset += bytes_to_copy;
  tiles_offsets_[attribute_id] += bytes_to_copy; 

  // Copy and update current buffer and tile offsets
  memcpy(
      buffer_var_c + buffer_var_offset, 
      tile_var + tiles_var_offsets_[attribute_id], 
      bytes_var_to_copy);
  buffer_var_offset += bytes_var_to_copy;
  tiles_var_offsets_[attribute_id] += bytes_var_to_copy;

  // Update overlapping tile position if necessary
  if(tiles_offsets_[attribute_id] == end_offset + 1) { // This tile is done
    tiles_offsets_[attribute_id] = tiles_sizes_[attribute_id]; 
    tiles_var_offsets_[attribute_id] = tiles_var_sizes_[attribute_id]; 
    ++overlapping_tiles_pos_[attribute_id];
  } else {                                             // Overflow
    overflow_[attribute_id] = true;
  }
}

template<class T>
void ReadState::copy_from_tile_buffer_partial_non_contig_dense(
    int attribute_id,
    void* buffer,
    size_t buffer_size,
    size_t& buffer_offset) {
  // Calculate free space in buffer
  size_t buffer_free_space = buffer_size - buffer_offset;
  if(buffer_free_space == 0) { // Overflow
    overflow_[attribute_id] = true;
    return;
  }

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  OverlappingTile& overlapping_tile = 
      overlapping_tiles_[overlapping_tiles_pos_[attribute_id]];
  const T* overlap_range = 
      static_cast<const T*>(overlapping_tile.overlap_range_);
  int dim_num = array_schema->dim_num();
  T* range_start_coords = new T[dim_num]; 
  T* range_end_coords = new T[dim_num]; 
  for(int i=0; i<dim_num; ++i) {
    range_start_coords[i] = overlap_range[2*i];
    range_end_coords[i] = overlap_range[2*i+1];
  }
  char* buffer_c = static_cast<char*>(buffer);
  char* tile = static_cast<char*>(tiles_[attribute_id]);
  size_t cell_size = array_schema->cell_size(attribute_id);

  // Sanity check
  assert(!array_schema->var_size(attribute_id));

  // Find the offset at the beginning and end of the overlap range
  int64_t range_start_cell_pos = 
      array_schema->get_cell_pos<T>(range_start_coords);
  int64_t range_end_cell_pos = 
      array_schema->get_cell_pos<T>(range_end_coords);
  size_t range_start_offset = range_start_cell_pos * cell_size; 
  size_t range_end_offset = (range_end_cell_pos + 1) * cell_size - 1; 

  // If current tile offset is 0, set it to the beginning of the overlap range
  if(tiles_offsets_[attribute_id] < range_start_offset) 
    tiles_offsets_[attribute_id] = range_start_offset;

  // Compute slab information
  T cell_num_in_range_slab = 
      array_schema->cell_num_in_range_slab<T>(overlap_range);
  size_t range_slab_size = cell_num_in_range_slab * cell_size;
  T cell_num_in_tile_slab = array_schema->cell_num_in_tile_slab<T>();
  size_t tile_slab_size = cell_num_in_tile_slab * cell_size;
  size_t current_slab_start_offset = 
      ((tiles_offsets_[attribute_id] - range_start_offset) / tile_slab_size) *
      tile_slab_size + range_start_offset;
  size_t current_slab_end_offset = 
      current_slab_start_offset + range_slab_size - 1;

  // Copy rest of the current slab
  size_t bytes_in_current_slab_left_to_copy = 
      current_slab_end_offset - tiles_offsets_[attribute_id] + 1;
  size_t bytes_to_copy = 
      std::min(bytes_in_current_slab_left_to_copy, buffer_size);
  memcpy(
      buffer_c + buffer_offset, 
      tile + tiles_offsets_[attribute_id],
      bytes_to_copy);
  buffer_offset += bytes_to_copy;
  tiles_offsets_[attribute_id] += bytes_to_copy;

  if(bytes_to_copy == bytes_in_current_slab_left_to_copy &&
     tiles_offsets_[attribute_id] != range_end_offset + 1)
    tiles_offsets_[attribute_id] += tile_slab_size - range_slab_size;

  // Copy rest of the slabs
  while(buffer_offset != buffer_size &&
        tiles_offsets_[attribute_id] != range_end_offset + 1) {
    buffer_free_space = buffer_size - buffer_offset;
    bytes_to_copy = std::min(range_slab_size, buffer_free_space);

    memcpy(
        buffer_c + buffer_offset, 
        tile + tiles_offsets_[attribute_id],
        bytes_to_copy);
    
    buffer_offset += bytes_to_copy;
    tiles_offsets_[attribute_id] += bytes_to_copy;

    if(bytes_to_copy == range_slab_size &&
       tiles_offsets_[attribute_id] != range_end_offset + 1) { 
      // Go to the start of the next slab
      tiles_offsets_[attribute_id] += tile_slab_size - bytes_to_copy;
    }
  } 

  // Update overlapping tile position if necessary
  if(tiles_offsets_[attribute_id] == range_end_offset + 1) { // Tile done
    tiles_offsets_[attribute_id] = tiles_sizes_[attribute_id]; 
    ++overlapping_tiles_pos_[attribute_id];
  } else {                                                   // Overflow
    assert(buffer_offset == buffer_size); 
    overflow_[attribute_id] = true;
  }

  // Clean up
  delete [] range_start_coords;
  delete [] range_end_coords;
}

template<class T>
void ReadState::copy_from_tile_buffer_partial_non_contig_dense_var(
    int attribute_id,
    void* buffer,
    size_t buffer_size,
    size_t& buffer_offset,
    void* buffer_var,
    size_t buffer_var_size,
    size_t& buffer_var_offset) {
  // Calculate free space in buffers
  size_t buffer_free_space = buffer_size - buffer_offset;
  size_t buffer_var_free_space = buffer_var_size - buffer_var_offset;

  // Handle trivial cases
  if(buffer_free_space == 0 || buffer_var_free_space == 0) { // Overflow
    overflow_[attribute_id] = true;
    return;
  }

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  OverlappingTile& overlapping_tile = 
      overlapping_tiles_[overlapping_tiles_pos_[attribute_id]];
  const T* overlap_range = 
      static_cast<const T*>(overlapping_tile.overlap_range_);
  int dim_num = array_schema->dim_num();
  T* range_start_coords = new T[dim_num]; 
  T* range_end_coords = new T[dim_num]; 
  for(int i=0; i<dim_num; ++i) {
    range_start_coords[i] = overlap_range[2*i];
    range_end_coords[i] = overlap_range[2*i+1];
  }
  char* buffer_c = static_cast<char*>(buffer);
  char* buffer_var_c = static_cast<char*>(buffer_var);
  char* tile = static_cast<char*>(tiles_[attribute_id]);
  char* tile_var = static_cast<char*>(tiles_var_[attribute_id]);
  size_t* tile_s = static_cast<size_t*>(tiles_[attribute_id]);

  // Sanity check
  assert(array_schema->var_size(attribute_id));

  // Find the offset at the beginning and end of the overlap range
  int64_t range_start_cell_pos = 
      array_schema->get_cell_pos<T>(range_start_coords);
  int64_t range_end_cell_pos = 
      array_schema->get_cell_pos<T>(range_end_coords);
  size_t range_start_offset = 
      range_start_cell_pos * TILEDB_CELL_VAR_OFFSET_SIZE; 
  size_t range_end_offset = 
      (range_end_cell_pos + 1) * TILEDB_CELL_VAR_OFFSET_SIZE - 1; 

  // If current tile offset is 0, set it to the beginning of the overlap range
  if(tiles_offsets_[attribute_id] < range_start_offset) { 
    tiles_offsets_[attribute_id] = range_start_offset;
    tiles_var_offsets_[attribute_id] = tile_s[range_start_cell_pos];
  }

  // Compute slab information
  T cell_num_in_range_slab = 
      array_schema->cell_num_in_range_slab<T>(overlap_range);
  size_t range_slab_size = cell_num_in_range_slab * TILEDB_CELL_VAR_OFFSET_SIZE;
  T cell_num_in_tile_slab = array_schema->cell_num_in_tile_slab<T>();
  size_t tile_slab_size = cell_num_in_tile_slab * TILEDB_CELL_VAR_OFFSET_SIZE;
  size_t current_slab_start_offset = 
      ((tiles_offsets_[attribute_id] - range_start_offset) / tile_slab_size) *
      tile_slab_size + range_start_offset;
  size_t current_slab_end_offset = 
      current_slab_start_offset + range_slab_size - 1;
  size_t bytes_in_current_slab_left_to_copy = 
      current_slab_end_offset - tiles_offsets_[attribute_id] + 1;

  // Compute actual bytes to copy 
  int64_t start_cell_pos = 
      current_slab_start_offset / TILEDB_CELL_VAR_OFFSET_SIZE;
  int64_t end_cell_pos = 
      ((current_slab_end_offset + 1) / TILEDB_CELL_VAR_OFFSET_SIZE) - 1;
  size_t bytes_to_copy, bytes_var_to_copy;
  compute_bytes_to_copy(
      attribute_id,
      start_cell_pos,
      end_cell_pos,
      buffer_free_space,
      buffer_var_free_space,
      bytes_to_copy,
      bytes_var_to_copy);

  if(bytes_to_copy == 0) {
    overflow_[attribute_id] = true;
    delete [] range_start_coords;
    delete [] range_end_coords;
    return;
  }

  // Copy the cells from the current slab
  void* buffer_start = buffer_c + buffer_offset;
  void* buffer_var_start = buffer_var_c + buffer_var_offset;
  void* tile_start = tile + tiles_offsets_[attribute_id];
  void* tile_var_start = tile_var + tiles_var_offsets_[attribute_id];

  memcpy(buffer_start, tile_start, bytes_to_copy);
  buffer_offset += bytes_to_copy;
  tiles_offsets_[attribute_id] += bytes_to_copy;

  // Shift variable offsets
  shift_var_offsets(
      buffer_start, 
      end_cell_pos - start_cell_pos + 1, 
      buffer_var_offset); 

  memcpy(buffer_var_start, tile_var_start, bytes_var_to_copy);
  buffer_var_offset += bytes_var_to_copy;
  tiles_var_offsets_[attribute_id] += bytes_var_to_copy;

  if(bytes_to_copy == bytes_in_current_slab_left_to_copy &&
     tiles_offsets_[attribute_id] != range_end_offset + 1) {
    // Go to the start of the next slab
    tiles_offsets_[attribute_id] += tile_slab_size - range_slab_size;
    if(tiles_offsets_[attribute_id] != tiles_sizes_[attribute_id])
      tiles_var_offsets_[attribute_id] = 
          tile_s[tiles_offsets_[attribute_id] / TILEDB_CELL_VAR_OFFSET_SIZE];
    else 
      tiles_var_offsets_[attribute_id] = tiles_var_sizes_[attribute_id];
  }

  // Copy rest of the slabs
  while(tiles_offsets_[attribute_id] != range_end_offset + 1) {
    // Update variables
    buffer_free_space = buffer_size - buffer_offset;
    buffer_var_free_space = buffer_var_size - buffer_var_offset;
    buffer_start = buffer_c + buffer_offset;
    buffer_var_start = buffer_var_c + buffer_var_offset;
    tile_start = tile + tiles_offsets_[attribute_id];
    tile_var_start = tile_var + tiles_var_offsets_[attribute_id];
    start_cell_pos = tiles_offsets_[attribute_id] / TILEDB_CELL_VAR_OFFSET_SIZE;
    end_cell_pos = start_cell_pos + cell_num_in_range_slab - 1;

    // Compute actual bytes to copy 
    compute_bytes_to_copy(
        attribute_id,
        start_cell_pos,
        end_cell_pos,
        buffer_free_space,
        buffer_var_free_space,
        bytes_to_copy,
        bytes_var_to_copy);

    if(bytes_to_copy == 0) 
      break;

    memcpy(buffer_start, tile_start, bytes_to_copy);
    buffer_offset += bytes_to_copy;
    tiles_offsets_[attribute_id] += bytes_to_copy;

    // Shift variable offsets
    shift_var_offsets(
        buffer_start, 
        end_cell_pos - start_cell_pos + 1, 
        buffer_var_offset); 

    memcpy(buffer_var_start, tile_var_start, bytes_var_to_copy);
    buffer_var_offset += bytes_var_to_copy;
    tiles_var_offsets_[attribute_id] += bytes_var_to_copy;

    if(bytes_to_copy == range_slab_size &&
       tiles_offsets_[attribute_id] != range_end_offset + 1) { 
      // Go to the start of the next slab
      tiles_offsets_[attribute_id] += tile_slab_size - bytes_to_copy;
      if(tiles_offsets_[attribute_id] != tiles_sizes_[attribute_id])
        tiles_var_offsets_[attribute_id] = 
            tile_s[tiles_offsets_[attribute_id] / TILEDB_CELL_VAR_OFFSET_SIZE];
      else 
        tiles_var_offsets_[attribute_id] = tiles_var_sizes_[attribute_id];
    } else {
      break;
    }
  } 

  // Update overlapping tile position if necessary
  if(tiles_offsets_[attribute_id] == range_end_offset + 1) {   // Tile done
    tiles_offsets_[attribute_id] = tiles_sizes_[attribute_id]; 
    tiles_var_offsets_[attribute_id] = tiles_var_sizes_[attribute_id]; 
    ++overlapping_tiles_pos_[attribute_id];
  } else {                                                     // Overflow
    overflow_[attribute_id] = true;
  } 

  // Clean up
  delete [] range_start_coords;
  delete [] range_end_coords;
}

template<class T>
void ReadState::copy_from_tile_buffer_partial_non_contig_sparse(
    int attribute_id,
    void* buffer,
    size_t buffer_size,
    size_t& buffer_offset) {
  // Calculate free space in buffer
  size_t buffer_free_space = buffer_size - buffer_offset;
  if(buffer_free_space == 0) { // Overflow
    overflow_[attribute_id] = true;
    return;
  }

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  size_t cell_size = array_schema->cell_size(attribute_id);
  OverlappingTile& overlapping_tile = 
      overlapping_tiles_[overlapping_tiles_pos_[attribute_id]];
  char* buffer_c = static_cast<char*>(buffer);
  char* tile = static_cast<char*>(tiles_[attribute_id]);

  // Sanity check
  assert(!array_schema->var_size(attribute_id));

  // If there are no qualifying cells in this tile, return
  if(overlapping_tile.cell_pos_ranges_.size() == 0) {
    tiles_offsets_[attribute_id] = tiles_sizes_[attribute_id]; 
    ++overlapping_tiles_pos_[attribute_id];
    return;
  }

  // For each cell position range, copy the respective cells to the buffer
  size_t start_offset, end_offset;
  size_t bytes_left_to_copy, bytes_to_copy;
  int64_t cell_pos_ranges_num = overlapping_tile.cell_pos_ranges_.size();

  for(int64_t i=cell_pos_range_pos_[attribute_id]; i<cell_pos_ranges_num; ++i) {
    // Calculate start and end offset in the tile
    start_offset = overlapping_tile.cell_pos_ranges_[i].first * cell_size; 
    end_offset = 
        (overlapping_tile.cell_pos_ranges_[i].second + 1) * cell_size - 1;

    // Potentially set the tile offset to the beginning of the current range
    if(tiles_offsets_[attribute_id] < start_offset) 
      tiles_offsets_[attribute_id] = start_offset;

    // Calculate the total size to copy
    bytes_left_to_copy = end_offset - tiles_offsets_[attribute_id] + 1;
    bytes_to_copy = std::min(bytes_left_to_copy, buffer_free_space); 

    // Copy and update current buffer and tile offsets
    if(bytes_to_copy != 0) {
      memcpy(
          buffer_c + buffer_offset, 
          tile + tiles_offsets_[attribute_id], 
          bytes_to_copy);
      buffer_offset += bytes_to_copy;
      tiles_offsets_[attribute_id] += bytes_to_copy; 
      buffer_free_space = buffer_size - buffer_offset;
    }

    // Update overlapping tile position if necessary
    if(i == cell_pos_ranges_num - 1 &&
       tiles_offsets_[attribute_id] == end_offset + 1) { // This tile is done
      tiles_offsets_[attribute_id] = tiles_sizes_[attribute_id]; 
      ++overlapping_tiles_pos_[attribute_id];
      cell_pos_range_pos_[attribute_id] = 0; // Update the new range pos 
    } else if(tiles_offsets_[attribute_id] != end_offset + 1) {
      assert(buffer_offset == buffer_size);              // Buffer overflow
      overflow_[attribute_id] = true;
      cell_pos_range_pos_[attribute_id] = i; // Update the new range pos 
    }
  }
}

template<class T>
void ReadState::copy_from_tile_buffer_partial_non_contig_sparse_var(
    int attribute_id,
    void* buffer,
    size_t buffer_size,
    size_t& buffer_offset,
    void* buffer_var,
    size_t buffer_var_size,
    size_t& buffer_var_offset) {
  // Calculate free space in buffer
  size_t buffer_free_space = buffer_size - buffer_offset;
  size_t buffer_var_free_space = buffer_var_size - buffer_var_offset;

  // Handle overflow
  if(buffer_free_space == 0 || buffer_var_free_space == 0) { // Overflow
    overflow_[attribute_id] = true; 
    return;
  }

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  size_t cell_size = sizeof(size_t);
  OverlappingTile& overlapping_tile = 
      overlapping_tiles_[overlapping_tiles_pos_[attribute_id]];
  char* buffer_c = static_cast<char*>(buffer);
  void* buffer_start = buffer_c + buffer_offset;
  char* buffer_var_c = static_cast<char*>(buffer_var);
  char* tile = static_cast<char*>(tiles_[attribute_id]);
  size_t* tile_s = static_cast<size_t*>(tiles_[attribute_id]);
  char* tile_var = static_cast<char*>(tiles_var_[attribute_id]);

  // Sanity check
  assert(array_schema->var_size(attribute_id));

  // If there are no qualifying cells in this tile, return
  if(overlapping_tile.cell_pos_ranges_.size() == 0) {
    tiles_offsets_[attribute_id] = tiles_sizes_[attribute_id]; 
    tiles_var_offsets_[attribute_id] = tiles_var_sizes_[attribute_id]; 
    ++overlapping_tiles_pos_[attribute_id];
    return;
  }

  // For each cell position range, copy the respective cells to the buffer
  size_t start_offset, end_offset;
  int64_t start_cell_pos, end_cell_pos;
  size_t bytes_left_to_copy, bytes_to_copy, bytes_var_to_copy;
  int64_t cell_pos_ranges_num = overlapping_tile.cell_pos_ranges_.size();
  for(int64_t i=cell_pos_range_pos_[attribute_id]; i<cell_pos_ranges_num; ++i) {
    // Calculate start and end offset in the tile
    start_offset = overlapping_tile.cell_pos_ranges_[i].first * cell_size; 
    end_offset = 
        (overlapping_tile.cell_pos_ranges_[i].second + 1) * cell_size - 1;

    // Potentially set the tile offset to the beginning of the current range
    if(tiles_offsets_[attribute_id] < start_offset) 
      tiles_offsets_[attribute_id] = start_offset;

    // Calculate the total size to copy
    bytes_left_to_copy = end_offset - tiles_offsets_[attribute_id] + 1;
    bytes_to_copy = std::min(bytes_left_to_copy, buffer_free_space); 

    // Compute actual bytes to copy
    start_cell_pos = tiles_offsets_[attribute_id] / cell_size;
    end_cell_pos = overlapping_tile.cell_pos_ranges_[i].second;
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

    // Update overlapping tile position if necessary
    if(i == cell_pos_ranges_num - 1 &&
       tiles_offsets_[attribute_id] == end_offset + 1) { // This tile is done
      tiles_offsets_[attribute_id] = tiles_sizes_[attribute_id]; 
      ++overlapping_tiles_pos_[attribute_id];
      cell_pos_range_pos_[attribute_id] = 0; // Update the new range pos 
    } else if(tiles_offsets_[attribute_id] != end_offset + 1) {
      // Check for overflow
      assert(buffer_offset == buffer_size);              // Buffer overflow
      overflow_[attribute_id] = true;
      cell_pos_range_pos_[attribute_id] = i; // Update the new range pos 
    }
  }
}

int ReadState::copy_tile_full(
    int attribute_id,
    void* buffer,
    size_t buffer_size,
    size_t& buffer_offset) {
  // Sanity check
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  assert(!array_schema->var_size(attribute_id));
 
  // Trivial case
  size_t buffer_free_space = buffer_size - buffer_offset;
  if(buffer_free_space == 0) {
    overflow_[attribute_id] = true;
    return TILEDB_RS_OK;
  }

  // For easy reference
  size_t cell_size = array_schema->cell_size(attribute_id);
  OverlappingTile& overlapping_tile = 
      overlapping_tiles_[overlapping_tiles_pos_[attribute_id]];
  size_t tile_size = overlapping_tile.cell_num_ * cell_size;

  if(tile_size <= buffer_free_space) { // Direct copy into buffer
    return copy_tile_full_direct(
               attribute_id, 
               buffer, 
               buffer_size, 
               tile_size,
               buffer_offset);
  } else {                             // Two-step copy
    // Get tile from the disk to the local buffer
    if(get_tile_from_disk_cmp_none(attribute_id) != TILEDB_RS_OK)
      return TILEDB_RS_ERR;
    // Copy as much data as it fits from local buffer into input buffer
    copy_from_tile_buffer_full(
        attribute_id, 
        buffer, 
        buffer_size, 
        buffer_offset);
    return TILEDB_RS_OK;
  }
}

int ReadState::copy_tile_full_var(
    int attribute_id,
    void* buffer,
    size_t buffer_size,
    size_t& buffer_offset,
    void* buffer_var,
    size_t buffer_var_size,
    size_t& buffer_var_offset) {
  // For easy reference
  size_t buffer_free_space = buffer_size - buffer_offset;
  size_t buffer_var_free_space = buffer_var_size - buffer_var_offset;
  OverlappingTile& overlapping_tile = 
      overlapping_tiles_[overlapping_tiles_pos_[attribute_id]];
  size_t tile_size = overlapping_tile.cell_num_ * TILEDB_CELL_VAR_OFFSET_SIZE;

  // Sanity check
  assert(fragment_->array()->array_schema()->var_size(attribute_id));

  // Compute variable tile size
  size_t tile_var_size;
  if(compute_tile_var_size(
         attribute_id, 
         overlapping_tile.pos_, 
         tile_var_size) != TILEDB_RS_OK) 
    return TILEDB_RS_ERR;

  if(tile_size <= buffer_free_space &&
     tile_var_size <= buffer_var_free_space) { // Direct copy into buffer
    return copy_tile_full_direct_var(
               attribute_id, 
               buffer, 
               buffer_size, 
               tile_size,
               buffer_offset,
               buffer_var, 
               buffer_var_size,
               tile_var_size,
               buffer_var_offset); 
  } else {                                     // Two-step copy
    // Get tile from the disk to the local buffer
    if(get_tile_from_disk_var_cmp_none(attribute_id) != TILEDB_RS_OK)
      return TILEDB_RS_ERR;
    // Copy as much data as it fits from local buffer into input buffer
    copy_from_tile_buffer_full_var(
        attribute_id, 
        buffer, 
        buffer_size, 
        buffer_offset,
        buffer_var, 
        buffer_var_size,
        buffer_var_offset); 
    return TILEDB_RS_OK;
  }
}

int ReadState::copy_tile_full_direct(
    int attribute_id,
    void* buffer,
    size_t buffer_size,
    size_t tile_size,
    size_t& buffer_offset) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  char* buffer_c = static_cast<char*>(buffer);

  // Sanity check
  assert(tile_size <= buffer_size - buffer_offset);
  
  // Prepare attribute file name
  std::string filename = fragment_->fragment_name() + "/" +
                         array_schema->attribute(attribute_id) +
                         TILEDB_FILE_SUFFIX;

  // Find file offset where the tile begins
  int64_t pos = overlapping_tiles_[overlapping_tiles_pos_[attribute_id]].pos_;
  off_t file_offset = pos * fragment_->tile_size(attribute_id);

  // Read from file
  if(READ_FROM_FILE(
         filename, 
         file_offset, 
         buffer_c + buffer_offset, 
         tile_size) != TILEDB_UT_OK)
    return TILEDB_RS_ERR;

  // Update offset and overlapping tile pos
  buffer_offset += tile_size;
  ++overlapping_tiles_pos_[attribute_id];
  
  // Success
  return TILEDB_RS_OK;
}

int ReadState::copy_tile_full_direct_var(
    int attribute_id,
    void* buffer,
    size_t buffer_size,
    size_t tile_size,
    size_t& buffer_offset,
    void* buffer_var,
    size_t buffer_var_size,
    size_t tile_var_size,
    size_t& buffer_var_offset) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  char* buffer_c = static_cast<char*>(buffer);
  char* buffer_var_c = static_cast<char*>(buffer_var);
  void* buffer_start = buffer_c + buffer_offset; 
  void* buffer_var_start = buffer_var_c + buffer_var_offset; 
  size_t full_tile_size = fragment_->tile_size(attribute_id);

  // Sanity check
  assert(tile_size <= buffer_size - buffer_offset);

  // ========== Copy variable cell offsets ========== //
  
  // Prepare attribute file name
  std::string filename = fragment_->fragment_name() + "/" +
                         array_schema->attribute(attribute_id) +
                         TILEDB_FILE_SUFFIX;

  // Find file offset where the tile begins
  int64_t pos = overlapping_tiles_[overlapping_tiles_pos_[attribute_id]].pos_;
  off_t file_offset = pos * full_tile_size;

  // Read from file
  if(READ_FROM_FILE(
         filename, 
         file_offset, 
         buffer_start, 
         tile_size) != TILEDB_UT_OK)
    return TILEDB_RS_ERR;


  // ========== Copy variable cells ========== // 

  // Prepare variable attribute file name
  filename = fragment_->fragment_name() + "/" +
                         array_schema->attribute(attribute_id) + "_var" +
                         TILEDB_FILE_SUFFIX;

  // Find file offset where the tile begins
  file_offset = static_cast<size_t*>(buffer_start)[0]; 

  // Read from file
  if(READ_FROM_FILE(
         filename, 
         file_offset, 
         buffer_var_start, 
         tile_var_size) != TILEDB_UT_OK)
    return TILEDB_RS_ERR;

  // Shift variable cell offsets
  shift_var_offsets(
      buffer_start, 
      tile_size / TILEDB_CELL_VAR_OFFSET_SIZE,
      buffer_var_offset);

  // Update buffer offset and overlapping tile position
  buffer_offset += tile_size;
  buffer_var_offset += tile_var_size;
  ++overlapping_tiles_pos_[attribute_id];
  
  // Success
  return TILEDB_RS_OK;
}

template<class T>
int ReadState::copy_tile_partial_contig_direct_dense(
    int attribute_id,
    void* buffer,
    size_t buffer_size,
    size_t result_size,
    size_t& buffer_offset) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  OverlappingTile& overlapping_tile = 
      overlapping_tiles_[overlapping_tiles_pos_[attribute_id]];
  const T* overlap_range = 
      static_cast<const T*>(overlapping_tile.overlap_range_);
  int dim_num = array_schema->dim_num();
  T* start_coords = new T[dim_num]; 
  for(int i=0; i<dim_num; ++i)
    start_coords[i] = overlap_range[2*i];
  char* buffer_c = static_cast<char*>(buffer);

  // Sanity check
  assert(!array_schema->var_size(attribute_id));

  // Find the offset at the beginning and end of the overlap range
  size_t tile_size = fragment_->tile_size(attribute_id);
  size_t cell_size = array_schema->cell_size(attribute_id);
  int64_t start_cell_pos = array_schema->get_cell_pos<T>(start_coords);
  size_t start_offset = start_cell_pos * cell_size; 

  // Find file offset where the tile begins
  int64_t pos = overlapping_tile.pos_;
  off_t file_offset = pos * tile_size + start_offset;
  
  // Prepare attribute file name
  std::string filename = fragment_->fragment_name() + "/" +
                         array_schema->attribute(attribute_id) +
                         TILEDB_FILE_SUFFIX;

  // Read from file
  if(READ_FROM_FILE(
         filename, 
         file_offset, 
         buffer_c + buffer_offset, 
         result_size) != TILEDB_UT_OK)
    return TILEDB_RS_ERR;

  // Update offset and overlapping tile pos
  buffer_offset += result_size;
  ++overlapping_tiles_pos_[attribute_id];
  
  // Success
  return TILEDB_RS_OK;
}

template<class T>
int ReadState::copy_tile_partial_contig_direct_sparse(
    int attribute_id,
    void* buffer,
    size_t buffer_size,
    size_t result_size,
    size_t& buffer_offset) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  OverlappingTile& overlapping_tile = 
      overlapping_tiles_[overlapping_tiles_pos_[attribute_id]];
  char* buffer_c = static_cast<char*>(buffer);

  // Sanity check
  assert(!array_schema->var_size(attribute_id));

  // Find the offset at the beginning and end of the overlap range
  size_t tile_size = fragment_->tile_size(attribute_id);
  size_t cell_size = array_schema->cell_size(attribute_id);
  const std::pair<int64_t, int64_t>& cell_pos_range = 
      overlapping_tile.cell_pos_ranges_[0];
  size_t start_offset = cell_pos_range.first * cell_size; 

  // Find file offset where the tile begins
  int64_t pos = overlapping_tile.pos_;
  off_t file_offset = pos * tile_size + start_offset;
  
  // Prepare attribute file name
  std::string filename = fragment_->fragment_name() + "/" +
                         array_schema->attribute(attribute_id) +
                         TILEDB_FILE_SUFFIX;

  // Read from file
  if(READ_FROM_FILE(
         filename, 
         file_offset, 
         buffer_c + buffer_offset, 
         result_size) != TILEDB_UT_OK)
    return TILEDB_RS_ERR;

  // Update offset and overlapping tile pos
  buffer_offset += result_size;
  ++overlapping_tiles_pos_[attribute_id];
  
  // Success
  return TILEDB_RS_OK;
}

template<class T>
int ReadState::copy_tile_partial_contig_dense(
    int attribute_id,
    void* buffer,
    size_t buffer_size,
    size_t& buffer_offset) {
  // Trivial case
  size_t buffer_free_space = buffer_size - buffer_offset;
  if(buffer_free_space == 0) {
    overflow_[attribute_id] = true;
    return TILEDB_RS_OK;
  }

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int dim_num = array_schema->dim_num();
  OverlappingTile& overlapping_tile = 
      overlapping_tiles_[overlapping_tiles_pos_[attribute_id]];
  const T* overlap_range = 
      static_cast<const T*>(overlapping_tile.overlap_range_);

  // Check if the partial read results fit in the buffer
  size_t cell_size = array_schema->cell_size(attribute_id);
  size_t result_size = cell_num_in_subarray(overlap_range, dim_num) * cell_size;

  if(result_size <= buffer_free_space) { // Direct disk to buffer access
    copy_tile_partial_contig_direct_dense<T>(
        attribute_id, 
        buffer, 
        buffer_size, 
        result_size,
        buffer_offset);
  } else {                               // Need to buffer the tile locally
    // Get tile from the disk into the local buffer
    if(get_tile_from_disk_cmp_none(attribute_id) != TILEDB_RS_OK) 
      return TILEDB_RS_ERR;
    
    // Copy the relevant data to the input buffer
    copy_from_tile_buffer_partial_contig_dense<T>(
        attribute_id, 
        buffer, 
        buffer_size, 
        buffer_offset);
  }

  // Success
  return TILEDB_RS_OK;
}

template<class T>
int ReadState::copy_tile_partial_contig_dense_var(
    int attribute_id,
    void* buffer,
    size_t buffer_size,
    size_t& buffer_offset,
    void* buffer_var,
    size_t buffer_var_size,
    size_t& buffer_var_offset) {
  // Get tile from the disk into the local buffer
  if(get_tile_from_disk_var_cmp_none(attribute_id) != TILEDB_RS_OK) 
    return TILEDB_RS_ERR;

  // Copy the relevant data to the input buffer
  copy_from_tile_buffer_partial_contig_dense_var<T>(
      attribute_id, 
      buffer, 
      buffer_size, 
      buffer_offset,
      buffer_var, 
      buffer_var_size, 
      buffer_var_offset);

  // Success
  return TILEDB_RS_OK;
}

template<class T>
int ReadState::copy_tile_partial_contig_sparse(
    int attribute_id,
    void* buffer,
    size_t buffer_size,
    size_t& buffer_offset) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  OverlappingTile& overlapping_tile = 
      overlapping_tiles_[overlapping_tiles_pos_[attribute_id]];
  assert(overlapping_tile.cell_pos_ranges_.size() <= 1); 

  // If there are no qualifying cells, return
  if(overlapping_tile.cell_pos_ranges_.size() == 0)
    return TILEDB_RS_OK;

  // For easy reference
  const std::pair<int64_t, int64_t>& cell_pos_range = 
      overlapping_tile.cell_pos_ranges_[0];

  // Check if the partial read results fit in the buffer
  size_t buffer_free_space = buffer_size - buffer_offset;
  size_t cell_size = array_schema->cell_size(attribute_id);
  size_t result_size = 
      (cell_pos_range.second - cell_pos_range.first + 1) * cell_size;

  if(result_size <= buffer_free_space) { // Direct disk to buffer access
    copy_tile_partial_contig_direct_sparse<T>(
        attribute_id, 
        buffer, 
        buffer_size, 
        result_size,
        buffer_offset);
  } else {                               // Need to buffer the tile locally
    // Get tile from the disk into the local buffer
    if(get_tile_from_disk_cmp_none(attribute_id) != TILEDB_RS_OK) 
      return TILEDB_RS_ERR;
 
    // Copy the relevant data to the input buffer
    copy_from_tile_buffer_partial_contig_sparse<T>(
        attribute_id, 
        buffer, 
        buffer_size, 
        buffer_offset);
  }

  // Success
  return TILEDB_RS_OK;
}

template<class T>
int ReadState::copy_tile_partial_contig_sparse_var(
    int attribute_id,
    void* buffer,
    size_t buffer_size,
    size_t& buffer_offset,
    void* buffer_var,
    size_t buffer_var_size,
    size_t& buffer_var_offset) {
  // Get tile from the disk into the local buffer
  if(get_tile_from_disk_var_cmp_none(attribute_id) != TILEDB_RS_OK) 
    return TILEDB_RS_ERR;
    
  // Copy the relevant data to the input buffer
  copy_from_tile_buffer_partial_contig_sparse_var<T>(
      attribute_id, 
      buffer, 
      buffer_size, 
      buffer_offset,
      buffer_var, 
      buffer_var_size, 
      buffer_var_offset);

  // Success
  return TILEDB_RS_OK;
}

template<class T>
int ReadState::copy_tile_partial_non_contig_dense(
    int attribute_id,
    void* buffer,
    size_t buffer_size,
    size_t& buffer_offset) {
  // Get tile from the disk into the local buffer
  if(get_tile_from_disk_cmp_none(attribute_id) != TILEDB_RS_OK) 
    return TILEDB_RS_ERR;

  // Copy as much data as it fits from local buffer into input buffer
  copy_from_tile_buffer_partial_non_contig_dense<T>(
      attribute_id, 
      buffer, 
      buffer_size, 
      buffer_offset);

  // Success
  return TILEDB_RS_OK;
}

template<class T>
int ReadState::copy_tile_partial_non_contig_dense_var(
    int attribute_id,
    void* buffer,
    size_t buffer_size,
    size_t& buffer_offset,
    void* buffer_var,
    size_t buffer_var_size,
    size_t& buffer_var_offset) {
  // Get tile from the disk into the local buffer
  if(get_tile_from_disk_var_cmp_none(attribute_id) != TILEDB_RS_OK) 
    return TILEDB_RS_ERR;

  // Copy as much data as it fits from local buffer into input buffer
  copy_from_tile_buffer_partial_non_contig_dense_var<T>(
      attribute_id, 
      buffer, 
      buffer_size, 
      buffer_offset,
      buffer_var, 
      buffer_var_size, 
      buffer_var_offset);

  // Success
  return TILEDB_RS_OK;
}

template<class T>
int ReadState::copy_tile_partial_non_contig_sparse(
    int attribute_id,
    void* buffer,
    size_t buffer_size,
    size_t& buffer_offset) {
  // Get tile from the disk into the local buffer
  if(get_tile_from_disk_cmp_none(attribute_id) != TILEDB_RS_OK) 
    return TILEDB_RS_ERR;

  // Copy as much data as it fits from local buffer into input buffer
  copy_from_tile_buffer_partial_non_contig_sparse<T>(
      attribute_id, 
      buffer, 
      buffer_size, 
      buffer_offset);

  // Success
  return TILEDB_RS_OK;
}

template<class T>
int ReadState::copy_tile_partial_non_contig_sparse_var(
    int attribute_id,
    void* buffer,
    size_t buffer_size,
    size_t& buffer_offset,
    void* buffer_var,
    size_t buffer_var_size,
    size_t& buffer_var_offset) {
  // Get tile from the disk into the local buffer
  if(get_tile_from_disk_var_cmp_none(attribute_id) != TILEDB_RS_OK) 
    return TILEDB_RS_ERR;

  // Copy as much data as it fits from local buffer into input buffer
  copy_from_tile_buffer_partial_non_contig_sparse_var<T>(
      attribute_id, 
      buffer, 
      buffer_size, 
      buffer_offset,
      buffer_var, 
      buffer_var_size, 
      buffer_var_offset);

  // Success
  return TILEDB_RS_OK;
}

template<class T>
int ReadState::get_first_coords_after(
    int tile_i,
    T* start_coords_before,
    T* first_coords) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  const void* tile_extents = array_schema->tile_extents();
  int attribute_num = array_schema->attribute_num();
  int dim_num = array_schema->dim_num();
  size_t coords_size = array_schema->coords_size();
  int compression = array_schema->compression(attribute_num);
  T empty_coord = 0;
  int rc;

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
}

template<class T>
int ReadState::get_first_two_coords(
    int tile_i,
    T* start_coords,
    T* first_coords,
    T* second_coords) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();
  const void* tile_extents = array_schema->tile_extents();
  int dim_num = array_schema->dim_num();
  size_t coords_size = array_schema->coords_size();
  int compression = array_schema->compression(attribute_num);
  T empty_coord = 0;
  int rc;

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
      start_coords_tile_id = array_schema->tile_id(start_coords);
      tile_coords_tile_id = array_schema->tile_id(&tile[med*dim_num]);
    } else { 
      start_coords_tile_id = 0;
      tile_coords_tile_id = 0;
    }

    // Calculate precedence of cells
    if(start_coords_tile_id == tile_coords_tile_id) {
      cmp = array_schema->cell_order_cmp<T>(
                start_coords,
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
    first_coords_pos = med;

  // Copy the first coordinates
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

  // Copy the second coordinates
  if(first_coords_pos+1 < cell_num) {
    memcpy(second_coords, &tile[(first_coords_pos+1) * dim_num], coords_size);
  } else {
    // Initialize empty value
    if(empty_coord == 0) { // If empty value not set
      if(&typeid(T) == &typeid(int))
        empty_coord = T(TILEDB_EMPTY_INT32);
      else if(&typeid(T) == &typeid(int64_t))
        empty_coord = T(TILEDB_EMPTY_INT64);
      else if(&typeid(T) == &typeid(float))
        empty_coord = T(TILEDB_EMPTY_FLOAT64);
      else if(&typeid(T) == &typeid(double))
        empty_coord = T(TILEDB_EMPTY_FLOAT64);
    }

    // Copy empty value
    memcpy(second_coords, &empty_coord, sizeof(T));
  }

  // Success
  return TILEDB_RS_OK;
}

template<class T>
int ReadState::get_cell_pos_ranges_sparse(
    const FragmentInfo& fragment_info,
    const T* subarray,
    const T* cell_range, 
    FragmentCellPosRanges& fragment_cell_pos_ranges) {
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
    fragment_cell_pos_ranges.push_back(fragment_cell_pos_range);
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

// TODO: This can be improved by checking if the query subarray includes
// results that appear contiguous on the disk, by checking the MBR overalp

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
        fragment_cell_pos_ranges.push_back(fragment_cell_pos_range);
        current_end_pos = -2; // This indicates that there is no active range
      }
    }
  }

  // Add last cell range
  if(current_end_pos != -2) {
    FragmentCellPosRange fragment_cell_pos_range;
    fragment_cell_pos_range.first = fragment_info;
    fragment_cell_pos_range.second = CellPosRange(current_start_pos, current_end_pos);
    fragment_cell_pos_ranges.push_back(fragment_cell_pos_range);
  }

  // Return 
  return TILEDB_RS_OK;
}

void ReadState::get_next_overlapping_tile_mult() {
  if(fragment_->dense())
    get_next_overlapping_tile_dense_mult();
  else
    get_next_overlapping_tile_sparse_mult();
}

void ReadState::get_next_overlapping_tile_dense() {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int coords_type = array_schema->coords_type();

  // Invoke the proper templated function 
  if(coords_type == TILEDB_INT32)
    get_next_overlapping_tile_dense<int>();
  else if(coords_type == TILEDB_INT64)
    get_next_overlapping_tile_dense<int64_t>();
  else
    assert(0); // The code should never reach this point
}

template<class T>
void ReadState::get_next_overlapping_tile_dense() {
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
  array_schema->compute_tile_range_overlap<T>(
      domain,          // fragment domain
      non_empty_domain,// fragment non-empty domain
      range,           // query range in array domain
      coords,          // tile coordinates in range in tile domain
      overlap_range,   // returned overlap in terms of cell coordinates in tile
      overlap);        // returned type of overlap

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
}

void ReadState::get_bounding_coords(void* bounding_coords) const {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  size_t coords_size = array_schema->coords_size();
  int64_t pos = overlapping_tiles_.back().pos_;
  memcpy(bounding_coords, book_keeping_->bounding_coords()[pos], 2*coords_size);
}

void ReadState::get_next_overlapping_tile_dense_mult() {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int coords_type = array_schema->coords_type();

  // Invoke the proper templated function 
  if(coords_type == TILEDB_INT32)
    get_next_overlapping_tile_dense_mult<int>();
  else if(coords_type == TILEDB_INT64)
    get_next_overlapping_tile_dense_mult<int64_t>();
  else
    assert(0); // The code should never reach this point
}

template<class T>
void ReadState::get_next_overlapping_tile_dense_mult() {
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
//      overlapping_tile.global_pos_ = -1;
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
  array_schema->compute_tile_range_overlap<T>(
      domain,          // fragment domain
      non_empty_domain,// fragment non-empty domain
      range,           // query range in array domain
      coords,          // tile coordinates in range in fragment tile domain
      overlap_range,   // returned overlap in terms of cell coordinates in tile
      overlap);        // returned type of overlap

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

/* TODO
  // Calculate global tile position
  if(overlapping_tile.overlap_ == NONE) {
    overlapping_tile.global_pos_ = -1;
  } else {
    const T* global_domain = static_cast<const T*>(array_schema->domain());
    const T* tile_extents = static_cast<const T*>(array_schema->tile_extents());
    if(memcmp(domain, global_domain, 2*coords_size) == 0) {
      // Fragment domain coincides with global domain
      overlapping_tile.global_pos_ = overlapping_tile.pos_;
    } else { // Fragment domain does not coincide with global domain
      // Calculate global tile coordinates
      overlapping_tile.global_coords_ = malloc(coords_size);
      T* global_coords = static_cast<T*>(overlapping_tile.global_coords_);
      const T* coords = static_cast<const T*>(overlapping_tile.coords_);
      for(int i=0; i<dim_num; ++i) {
        global_coords[i] = 
            (domain[2*i] - global_domain[2*i]) / tile_extents[i] + coords[i];
      } 
   
      // Find the global position
      overlapping_tile.global_pos_ = 
          array_schema->get_tile_pos<T>(global_domain, global_coords);
    }
  }
*/ 

  // Calculate global coordinates
  if(overlapping_tile.overlap_ != NONE) {
    const T* global_domain = static_cast<const T*>(array_schema->domain());
    const T* tile_extents = static_cast<const T*>(array_schema->tile_extents());
    overlapping_tile.global_coords_ = malloc(coords_size);
    T* global_coords = static_cast<T*>(overlapping_tile.global_coords_);
    const T* coords = static_cast<const T*>(overlapping_tile.coords_);
    for(int i=0; i<dim_num; ++i) {
      global_coords[i] = 
          (domain[2*i] - global_domain[2*i]) / tile_extents[i] + coords[i];
    } 
  }

  // Store the new tile
  overlapping_tiles_.push_back(overlapping_tile); 

  // Clean up processed overlapping tiles
// TODO:  clean_up_processed_overlapping_tiles();
}

void ReadState::get_next_overlapping_tile_sparse() {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int coords_type = array_schema->coords_type();

  // Invoke the proper templated function 
  if(coords_type == TILEDB_INT32)
    get_next_overlapping_tile_sparse<int>();
  else if(coords_type == TILEDB_INT64)
    get_next_overlapping_tile_sparse<int64_t>();
  else if(coords_type == TILEDB_FLOAT32)
    get_next_overlapping_tile_sparse<float>();
  else if(coords_type == TILEDB_FLOAT64)
    get_next_overlapping_tile_sparse<double>();
  else
    assert(0); // The code should never reach this point
}

template<class T>
void ReadState::get_next_overlapping_tile_sparse() {
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

      array_schema->compute_mbr_range_overlap<T>(
          range,         // query range
          mbr,           // tile mbr
          overlap_range, // returned overlap in terms of absolute coordinates
          overlap);      // returned type of overlap

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
}

void ReadState::get_next_overlapping_tile_sparse_mult() {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int coords_type = array_schema->coords_type();

  // Invoke the proper templated function 
  if(coords_type == TILEDB_INT32)
    get_next_overlapping_tile_sparse_mult<int>();
  else if(coords_type == TILEDB_INT64)
    get_next_overlapping_tile_sparse_mult<int64_t>();
  else if(coords_type == TILEDB_FLOAT32)
    get_next_overlapping_tile_sparse_mult<float>();
  else if(coords_type == TILEDB_FLOAT64)
    get_next_overlapping_tile_sparse_mult<double>();
  else
    assert(0); // The code should never reach this point
}

template<class T>
void ReadState::get_next_overlapping_tile_sparse_mult() {
  // Trivial case
  if(overlapping_tiles_.size() != 0 && 
     overlapping_tiles_.back().overlap_ == NONE)
    return;

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int dim_num = array_schema->dim_num();
  int attribute_num = array_schema->attribute_num();
  size_t coords_size = array_schema->coords_size();
  const T* range = static_cast<const T*>(fragment_->array()->subarray());
  int64_t tile_num = book_keeping_->mbrs().size();
  const T* global_domain = static_cast<const T*>(array_schema->domain());
  const T* tile_extents = static_cast<const T*>(array_schema->tile_extents());

  // Attempt to advance only the global position
  if(overlapping_tiles_.size() != 0 && 
     overlapping_tiles_.back().mbr_in_tile_domain_ != NULL) {
    // Get next tile coordinates inside the MBR
    const T* mbr_in_tile_domain = 
        static_cast<const T*>(overlapping_tiles_.back().mbr_in_tile_domain_);
    T* mbr_coords = 
        static_cast<T*>(overlapping_tiles_.back().mbr_coords_);
    array_schema->get_next_tile_coords<T>(mbr_in_tile_domain, mbr_coords);

    // Check if the new coordinates exceeded the MBR tile domain
    bool inside_mbr = true;
    for(int i=0; i<dim_num; ++i) {
      if(mbr_coords[i] > mbr_in_tile_domain[2*i+1] ||
         mbr_coords[i] < mbr_in_tile_domain[2*i]) {
        inside_mbr = false;
        break;
      }
    }

    // If the next tile coordinates are insinde the MBR, calculate new global
    // position and return (i.e., do not get the next overlapping tile)
    if(inside_mbr) {
      T* global_coords = 
          static_cast<T*>(overlapping_tiles_.back().global_coords_);
      const T* mbr = 
          static_cast<const T*>(
              book_keeping_->mbrs()[overlapping_tiles_.back().pos_]);
      for(int i=0; i<dim_num; ++i) {
        global_coords[i] = 
            (mbr[2*i] - global_domain[2*i]) / tile_extents[i] + mbr_coords[i];
      }

// TODO      overlapping_tiles_.back().global_pos_ = 
// TODO          array_schema->get_tile_pos<T>(global_domain, global_coords);
  
      // Return here
      return;
    }
  }

  // The next overlapping tile
  OverlappingTile overlapping_tile = {};
  // TODO: overlapping_tile.coords_ = NULL; // Applicable only to dense arrays
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
  const T* mbr;
  if(tile_search_range_[0] >= 0 &&
     tile_search_range_[0] < tile_num) { 
    while(overlapping_tile.overlap_ == NONE && 
          tile_pos <= tile_search_range_[1]) {
      // Set tile position
      overlapping_tile.pos_ = tile_pos;

      // Get tile overlap
      int overlap;
      mbr = static_cast<const T*>(book_keeping_->mbrs()[tile_pos]);

      array_schema->compute_mbr_range_overlap<T>(
          range,         // query range
          mbr,           // tile mbr
          overlap_range, // returned overlap in terms of absolute coordinates
          overlap);      // returned type of overlap

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
   
/* TODO
  // Find the global position
  if(overlapping_tile.overlap_ == NONE) {
    overlapping_tile.global_pos_ = -1;
  } else {
    // Calculate the initial global tile position based on the MBR
    overlapping_tile.global_coords_ = malloc(coords_size);
    T* global_coords = static_cast<T*>(overlapping_tile.global_coords_);
    for(int i=0; i<dim_num; ++i) {
      global_coords[i] = (mbr[2*i] - global_domain[2*i]) / tile_extents[i]; 
    }

    // Find the global position
    overlapping_tile.global_pos_ = 
        array_schema->get_tile_pos<T>(global_domain, global_coords);
  }
*/
  // Find the global coordinates
  if(overlapping_tile.overlap_ != NONE) {
    // Calculate the initial global tile position based on the MBR
    overlapping_tile.global_coords_ = malloc(coords_size);
    T* global_coords = static_cast<T*>(overlapping_tile.global_coords_);
    for(int i=0; i<dim_num; ++i) {
      global_coords[i] = (mbr[2*i] - global_domain[2*i]) / tile_extents[i]; 
    }

    // Calculate the tile domain of the MBR and the initial coordinates
    overlapping_tile.mbr_in_tile_domain_ = malloc(2*dim_num*sizeof(T));
    overlapping_tile.mbr_coords_ = malloc(dim_num*sizeof(T));
    T* mbr_in_tile_domain = static_cast<T*>(overlapping_tile.mbr_in_tile_domain_);
    T* mbr_coords = static_cast<T*>(overlapping_tile.mbr_coords_);

    for(int i=0; i<dim_num; ++i) {
      mbr_in_tile_domain[2*i] = (mbr[2*i] - global_domain[2*i]) / tile_extents[i]; 
      mbr_in_tile_domain[2*i+1] = (mbr[2*i+1] - global_domain[2*i]) / tile_extents[i];
      mbr_in_tile_domain[2*i+1] -= mbr_in_tile_domain[2*i];
      mbr_in_tile_domain[2*i] = 0;
      mbr_coords[i] = 0;
    }
  }

  // Store the new tile
  overlapping_tiles_.push_back(overlapping_tile); 

  // Clean up processed overlapping tiles
// TODO  clean_up_processed_overlapping_tiles();
}

int ReadState::get_tile_from_disk_cmp_gzip(int attribute_id) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();
  OverlappingTile& overlapping_tile = 
      overlapping_tiles_[overlapping_tiles_pos_[attribute_id]];

  // Check if the coodinates tile is already in memory
  if(overlapping_tile.tile_fetched_[attribute_id])
    return TILEDB_RS_OK;

  // For easy reference
  size_t cell_size = array_schema->cell_size(attribute_id);
  size_t full_tile_size = fragment_->tile_size(attribute_id);
  size_t tile_size = overlapping_tile.cell_num_ * cell_size;

  const std::vector<std::vector<off_t> >& tile_offsets = 
      book_keeping_->tile_offsets(); 
  int64_t tile_num = book_keeping_->tile_num();

  // Allocate space for the tile if needed
  if(tiles_[attribute_id] == NULL) 
    tiles_[attribute_id] = malloc(full_tile_size);

  // Set the actual tile size
  tiles_sizes_[attribute_id] = tile_size;

  // Prepare attribute file name
  std::string filename = fragment_->fragment_name() + "/" +
                         array_schema->attribute(attribute_id) +
                         TILEDB_FILE_SUFFIX;

  // Find file offset where the tile begins
  int64_t pos = overlapping_tile.pos_;
  off_t file_offset = tile_offsets[attribute_id][pos];
  off_t file_size = ::file_size(filename);
  size_t tile_compressed_size = 
      (pos == tile_num-1) ? file_size - tile_offsets[attribute_id][pos] 
                          : tile_offsets[attribute_id][pos+1] - 
                            tile_offsets[attribute_id][pos];

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

  // Update tile offset
  tiles_offsets_[attribute_id] = 0;

  // Set flag
  overlapping_tile.tile_fetched_[attribute_id] = true;
  
  // Success
  return TILEDB_RS_OK;
}

int ReadState::get_tile_from_disk_cmp_none(int attribute_id) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();
  OverlappingTile& overlapping_tile = 
      overlapping_tiles_[overlapping_tiles_pos_[attribute_id]];

  // Check if the coodinates tile is already in memory
  if(overlapping_tile.tile_fetched_[attribute_id]) 
    return TILEDB_RS_OK;

  // For easy reference
  size_t cell_size = array_schema->cell_size(attribute_id);
  size_t full_tile_size = fragment_->tile_size(attribute_id);
  size_t tile_size = overlapping_tile.cell_num_ * cell_size;

  // Find file offset where the tile begins
  int64_t pos = overlapping_tile.pos_;
  off_t file_offset = pos * full_tile_size;

  if(READ_TILE_FROM_FILE_CMP_NONE(
         attribute_id, 
         file_offset, 
         tile_size) != TILEDB_RS_OK)
    return TILEDB_RS_ERR;

  // Update tile offset
  tiles_offsets_[attribute_id] = 0;

  // Set flag
  overlapping_tile.tile_fetched_[attribute_id] = true;
  
  // Success
  return TILEDB_RS_OK;
}

int ReadState::get_tile_from_disk_var_cmp_gzip(int attribute_id) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  OverlappingTile& overlapping_tile = 
      overlapping_tiles_[overlapping_tiles_pos_[attribute_id]];

  // Sanity check
  assert(array_schema->var_size(attribute_id));

  // Check if the coodinates tile is already in memory
  if(overlapping_tile.tile_fetched_[attribute_id]) 
    return TILEDB_RS_OK;

  // For easy reference
  size_t cell_size = TILEDB_CELL_VAR_OFFSET_SIZE;
  size_t full_tile_size = fragment_->tile_size(attribute_id);
  size_t tile_size = overlapping_tile.cell_num_ * cell_size;
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
  int64_t pos = overlapping_tile.pos_;
  off_t file_offset = tile_offsets[attribute_id][pos];
  off_t file_size = ::file_size(filename);
  size_t tile_compressed_size = 
      (pos == tile_num-1) ? file_size - tile_offsets[attribute_id][pos] 
                          : tile_offsets[attribute_id][pos+1] - 
                            tile_offsets[attribute_id][pos];

  // Allocate space for the tile if needed
  if(tiles_[attribute_id] == NULL) 
    tiles_[attribute_id] = malloc(full_tile_size);

  // Set the actual tile size
  tiles_sizes_[attribute_id] = tile_size;

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

  // Update tile offset
  tiles_offsets_[attribute_id] = 0;

  // ========== Get variable tile ========== //

  // Prepare variable attribute file name
  filename = fragment_->fragment_name() + "/" +
             array_schema->attribute(attribute_id) + "_var" +
             TILEDB_FILE_SUFFIX;

  // Calculate offset and compressed tile size
  file_offset = tile_var_offsets[attribute_id][pos];
  file_size = ::file_size(filename);
  tile_compressed_size = 
      (pos == tile_num-1) ? file_size - tile_var_offsets[attribute_id][pos] 
                          : tile_var_offsets[attribute_id][pos+1] - 
                            tile_var_offsets[attribute_id][pos];

  // Get size of decompressed tile
  size_t tile_var_size = book_keeping_->tile_var_sizes()[attribute_id][pos];

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

  // Set the actual variable tile size
  tiles_var_sizes_[attribute_id] = tile_var_size; 

  // Update tile offset
  tiles_var_offsets_[attribute_id] = 0;

  // Shift variable cell offsets
  shift_var_offsets(attribute_id);

  // Set flag
  overlapping_tile.tile_fetched_[attribute_id] = true;

  // Success
  return TILEDB_RS_OK;
}

int ReadState::get_tile_from_disk_var_cmp_none(int attribute_id) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  OverlappingTile& overlapping_tile = 
      overlapping_tiles_[overlapping_tiles_pos_[attribute_id]];

  // Sanity check
  assert(array_schema->var_size(attribute_id));

  // Check if the coodinates tile is already in memory
  if(overlapping_tile.tile_fetched_[attribute_id]) 
    return TILEDB_RS_OK;

  // For easy reference
  size_t full_tile_size = fragment_->tile_size(attribute_id);
  size_t tile_size = overlapping_tile.cell_num_ * TILEDB_CELL_VAR_OFFSET_SIZE;
  int64_t tile_num = book_keeping_->tile_num();
  int64_t pos = overlapping_tile.pos_;
  off_t file_offset = pos * full_tile_size;

  // Read tile from file
  if(READ_TILE_FROM_FILE_CMP_NONE(
         attribute_id, 
         file_offset, 
         tile_size) != TILEDB_RS_OK)
    return TILEDB_RS_ERR;

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

  if(pos != tile_num - 1) { // Not the last tile
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

  // Update offsets
  tiles_offsets_[attribute_id] = 0;
  tiles_var_offsets_[attribute_id] = 0;

  // Shift starting offsets of variable-sized cells
  shift_var_offsets(attribute_id);

  // Set flag
  overlapping_tile.tile_fetched_[attribute_id] = true;

  // Success
  return TILEDB_RS_OK;
}

void ReadState::init_range_in_tile_domain() {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int coords_type = array_schema->coords_type();

  // Invoke the proper templated function
  if(coords_type == TILEDB_INT32) {
    init_range_in_tile_domain<int>();
  } else if(coords_type == TILEDB_INT64) {
    init_range_in_tile_domain<int64_t>();
  } else {
    // The code should never reach here
    assert(0);
  } 
}

template<class T>
void ReadState::init_range_in_tile_domain() {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int dim_num = array_schema->dim_num();
  const T* domain = static_cast<const T*>(book_keeping_->domain());
  const T* tile_extents = static_cast<const T*>(array_schema->tile_extents());
  const T* range = static_cast<const T*>(fragment_->array()->subarray());

  // Sanity check
  assert(tile_extents != NULL);
  assert(range != NULL);

  // Compute tile domain
  T* tile_domain = new T[2*dim_num];
  T tile_num; // Per dimension
  for(int i=0; i<dim_num; ++i) {
    tile_num = ceil(double(domain[2*i+1] - domain[2*i] + 1) / tile_extents[i]);
    tile_domain[2*i] = 0;
    tile_domain[2*i+1] = tile_num - 1;
  }

  // Allocate space for the range in tile domain
  assert(range_in_tile_domain_ == NULL);
  range_in_tile_domain_ = malloc(2*dim_num*sizeof(T));

  // For easy reference
  T* range_in_tile_domain = static_cast<T*>(range_in_tile_domain_);

  // Calculate range in tile domain
  for(int i=0; i<dim_num; ++i) {
    range_in_tile_domain[2*i] = 
        std::max((range[2*i] - domain[2*i]) / tile_extents[i],
            tile_domain[2*i]); 
    range_in_tile_domain[2*i+1] = 
        std::min((range[2*i+1] - domain[2*i]) / tile_extents[i],
            tile_domain[2*i+1]); 
  }

  // Check if there is any overlap between the range and the tile domain
  overlap_ = true;
  for(int i=0; i<dim_num; ++i) {
    if(range_in_tile_domain[2*i] > tile_domain[2*i+1] ||
       range_in_tile_domain[2*i+1] < tile_domain[2*i]) {
      overlap_ = false;
      break;
    }
  }

  // Clean up
  delete [] tile_domain;
}

void ReadState::init_tile_search_range() {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int coords_type = array_schema->coords_type();

  // Invoke the proper templated function
  if(coords_type == TILEDB_INT32) {
    init_tile_search_range<int>();
  } else if(coords_type == TILEDB_INT64) {
    init_tile_search_range<int64_t>();
  } else if(coords_type == TILEDB_FLOAT32) {
    init_tile_search_range<float>();
  } else if(coords_type == TILEDB_FLOAT64) {
    init_tile_search_range<double>();
  } else {
    // The code should never reach here
    assert(0);
  } 
}

template<class T>
void ReadState::init_tile_search_range() {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int cell_order = array_schema->cell_order();

  // Initialize the tile search range
  if(array_schema->tile_extents() == NULL) { // Null tile extents
    if(cell_order == TILEDB_HILBERT) {      
      // HILBERT
      init_tile_search_range_hil<T>();
    } else if(cell_order == TILEDB_ROW_MAJOR) { 
      // ROW MAJOR
      init_tile_search_range_row<T>();
    } else if(cell_order ==TILEDB_COL_MAJOR) { 
      // COLUMN MAJOR
      init_tile_search_range_col<T>();
    } else {
      assert(0);
    }
  } else {                                   // Non-null tile extents
    if(cell_order == TILEDB_ROW_MAJOR) {    
      // ROW MAJOR
      init_tile_search_range_id_row<T>();
    } else if(cell_order == TILEDB_COL_MAJOR) { 
      // COLUMN MAJOR
      init_tile_search_range_id_col<T>();
    } else {
      assert(0);
    }
  }

  // Get the first overlapping tile in case of no overlap
  if(tile_search_range_[1] < tile_search_range_[0]) {
    OverlappingTile overlapping_tile = {};
    overlapping_tile.overlap_ = NONE;
    //overlapping_tile.overlap_range_ = NULL; 
    //overlapping_tile.coords_ = NULL;
    overlapping_tiles_.push_back(overlapping_tile);
  }
}

template<class T>
void ReadState::init_tile_search_range_col() {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int dim_num = array_schema->dim_num();
  const T* range = static_cast<const T*>(fragment_->array()->subarray());
  int64_t tile_num = book_keeping_->tile_num();
  const std::vector<void*>& bounding_coords = 
      book_keeping_->bounding_coords();

  // Calculate range coordinates
  T* range_min_coords = new T[dim_num];
  T* range_max_coords = new T[dim_num];
  for(int i=0; i<dim_num; ++i) {
    memcpy(&range_min_coords[i], &range[2*i], sizeof(T)); 
    memcpy(&range_max_coords[i], &range[2*i+1], sizeof(T)); 
  }

  // For the bounding coordinates of each investigated tile
  const T* tile_start_coords;
  const T* tile_end_coords;

  // --- Finding the start tile in search range

  // Perform binary search
  int64_t min = 0;
  int64_t max = tile_num - 1;
  int64_t med;
  while(min <= max) {
    med = min + ((max - min) / 2);

    // Get info for bounding coordinates
    tile_start_coords = static_cast<const T*>(bounding_coords[med]);
    tile_end_coords = &(static_cast<const T*>(bounding_coords[med])[dim_num]);

    // Calculate precedence
    if(cmp_col_order(
           range_min_coords,
           tile_start_coords,
           dim_num) < 0) {   // Range min precedes MBR
      max = med-1;
    } else if(cmp_col_order(
           range_min_coords,
           tile_end_coords,
           dim_num) > 0) {   // Range min succeeds MBR
      min = med+1;
    } else {                 // Range min in MBR
      break; 
    }
  }

  // Determine the start position of the range
  if(max < min)    // Range min precedes the tile at position min  
    tile_search_range_[0] = min;     
  else             // Range min included in a tile
    tile_search_range_[0] = med;

  if(is_unary_subarray(range, dim_num)) {  // Unary range
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
      if(cmp_col_order(
             range_max_coords,
             tile_start_coords,
             dim_num) < 0) {   // Range max precedes MBR
        max = med-1;
      } else if(cmp_col_order(
             range_max_coords,
             tile_end_coords,
             dim_num) > 0) {   // Range max succeeds MBR
        min = med+1;
      } else {                 // Range max in MBR
        break; 
      }
    }

    // Determine the start position of the range
    if(max < min)    // Range max succeeds the tile at position max 
      tile_search_range_[1] = max;     
    else             // Range max included in a tile
      tile_search_range_[1] = med;
  }

  // Clean up
  delete [] range_min_coords;
  delete [] range_max_coords;
}

template<class T>
void ReadState::init_tile_search_range_hil() {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int dim_num = array_schema->dim_num();
  const T* range = static_cast<const T*>(fragment_->array()->subarray());
  int64_t tile_num = book_keeping_->tile_num();

  if(is_unary_subarray(range, dim_num)) {  // Unary range
    // For easy reference
    const std::vector<void*>& bounding_coords = 
        book_keeping_->bounding_coords();

    // Calculate range coordinates
    T* range_coords = new T[dim_num];
    for(int i=0; i<dim_num; ++i)
      memcpy(&range_coords[i], &range[2*i], sizeof(T)); 
    int64_t range_coords_id = array_schema->hilbert_id(range_coords);

    // For the bounding coordinates of each investigated tile
    const T* tile_start_coords;
    const T* tile_end_coords;
    int64_t tile_start_coords_id;
    int64_t tile_end_coords_id;

    // Perform binary search
    int64_t min = 0;
    int64_t max = tile_num - 1;
    int64_t med;
    while(min <= max) {
      med = min + ((max - min) / 2);

      // Get info for bounding coordinates
      tile_start_coords = static_cast<const T*>(bounding_coords[med]);
      tile_end_coords = &(static_cast<const T*>(bounding_coords[med])[dim_num]);
      tile_start_coords_id = array_schema->hilbert_id(tile_start_coords);
      tile_end_coords_id = array_schema->hilbert_id(tile_end_coords);
     
      // Calculate precedence
      if(cmp_row_order(
             range_coords_id,
             range_coords,
             tile_start_coords_id,
             tile_start_coords,
             dim_num) < 0) {   // Range precedes MBR
        max = med-1;
      } else if(cmp_row_order(
             range_coords_id,
             range_coords,
             tile_end_coords_id,
             tile_end_coords,
             dim_num) > 0) {   // Range succeeds MBR
        min = med+1;
      } else {                 // Range in MBR
        break; 
      }
    }

    // Determine the start position of the range
    if(max < min)    // Range precedes the tile at position min  
      tile_search_range_[0] = min;     
    else             // Range included in a tile
      tile_search_range_[0] = med;

    // For unary ranges, start and end positions are the same
    tile_search_range_[1] = tile_search_range_[0];

    // Clean up
    delete [] range_coords;
  }  else {                            // Non-unary range
    tile_search_range_[0] = 0;
    tile_search_range_[1] = book_keeping_->tile_num() - 1;
  }
}

template<class T>
void ReadState::init_tile_search_range_row() {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int dim_num = array_schema->dim_num();
  const T* range = static_cast<const T*>(fragment_->array()->subarray());

  // Trivial case
  if(range == NULL)
    return;

  int64_t tile_num = book_keeping_->tile_num();
  const std::vector<void*>& bounding_coords = 
      book_keeping_->bounding_coords();

  // Calculate range coordinates
  T* range_min_coords = new T[dim_num];
  T* range_max_coords = new T[dim_num];
  for(int i=0; i<dim_num; ++i) {
    memcpy(&range_min_coords[i], &range[2*i], sizeof(T)); 
    memcpy(&range_max_coords[i], &range[2*i+1], sizeof(T)); 
  }

  // For the bounding coordinates of each investigated tile
  const T* tile_start_coords;
  const T* tile_end_coords;

  // --- Finding the start tile in search range

  // Perform binary search
  int64_t min = 0;
  int64_t max = tile_num - 1;
  int64_t med;
  while(min <= max) {
    med = min + ((max - min) / 2);

    // Get info for bounding coordinates
    tile_start_coords = static_cast<const T*>(bounding_coords[med]);
    tile_end_coords = &(static_cast<const T*>(bounding_coords[med])[dim_num]);
     
    // Calculate precedence
    if(cmp_row_order(
           range_min_coords,
           tile_start_coords,
           dim_num) < 0) {   // Range min precedes MBR
      max = med-1;
    } else if(cmp_row_order(
           range_min_coords,
           tile_end_coords,
           dim_num) > 0) {   // Range min succeeds MBR
      min = med+1;
    } else {                 // Range min in MBR
      break; 
    }
  }

  // Determine the start position of the range
  if(max < min)    // Range min precedes the tile at position min  
    tile_search_range_[0] = min;     
  else             // Range min included in a tile
    tile_search_range_[0] = med;

  if(is_unary_subarray(range, dim_num)) {  // Unary range
    // The end positions is the same as the start
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
      if(cmp_row_order(
             range_max_coords,
             tile_start_coords,
             dim_num) < 0) {   // Range max precedes MBR
        max = med-1;
      } else if(cmp_row_order(
             range_max_coords,
             tile_end_coords,
             dim_num) > 0) {   // Range max succeeds MBR
        min = med+1;
      } else {                 // Range max in MBR
        break; 
      }
    }

    // Determine the start position of the range
    if(max < min)    // Range max succeeds the tile at position max 
      tile_search_range_[1] = max;     
    else             // Range max included in a tile
      tile_search_range_[1] = med;
  }

  // Clean up
  delete [] range_min_coords;
  delete [] range_max_coords;
}

template<class T>
void ReadState::init_tile_search_range_id_col() {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int dim_num = array_schema->dim_num();
  const T* range = static_cast<const T*>(fragment_->array()->subarray());
  int64_t tile_num = book_keeping_->tile_num();
  const std::vector<void*>& bounding_coords = 
      book_keeping_->bounding_coords();

  // Calculate range coordinates
  T* range_min_coords = new T[dim_num];
  T* range_max_coords = new T[dim_num];
  for(int i=0; i<dim_num; ++i) {
    memcpy(&range_min_coords[i], &range[2*i], sizeof(T)); 
    memcpy(&range_max_coords[i], &range[2*i+1], sizeof(T)); 
  }
  int64_t range_min_coords_id = array_schema->tile_id(range_min_coords);
  int64_t range_max_coords_id = array_schema->tile_id(range_max_coords);

  // For the bounding coordinates of each investigated tile
  const T* tile_start_coords;
  const T* tile_end_coords;
  int64_t tile_start_coords_id;
  int64_t tile_end_coords_id;

  // --- Finding the start tile in search range

  // Perform binary search
  int64_t min = 0;
  int64_t max = tile_num - 1;
  int64_t med;
  while(min <= max) {
    med = min + ((max - min) / 2);

    // Get info for bounding coordinates
    tile_start_coords = static_cast<const T*>(bounding_coords[med]);
    tile_end_coords = &(static_cast<const T*>(bounding_coords[med])[dim_num]);
    tile_start_coords_id = array_schema->tile_id(tile_start_coords);
    tile_end_coords_id = array_schema->tile_id(tile_end_coords);

    // Calculate precedence
    if(cmp_col_order(
           range_min_coords_id,
           range_min_coords,
           tile_start_coords_id,
           tile_start_coords,
           dim_num) < 0) {   // Range min precedes MBR
      max = med-1;
    } else if(cmp_col_order(
           range_min_coords_id,
           range_min_coords,
           tile_end_coords_id,
           tile_end_coords,
           dim_num) > 0) {   // Range min succeeds MBR
      min = med+1;
    } else {                 // Range min in MBR
      break; 
    }
  }

  // Determine the start position of the range
  if(max < min)    // Range min precedes the tile at position min  
    tile_search_range_[0] = min;     
  else             // Range min included in a tile
    tile_search_range_[0] = med;

  if(is_unary_subarray(range, dim_num)) {  // Unary range
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
      tile_start_coords_id = array_schema->tile_id(tile_start_coords);
      tile_end_coords_id = array_schema->tile_id(tile_end_coords);
     
      // Calculate precedence
      if(cmp_col_order(
             range_max_coords_id,
             range_max_coords,
             tile_start_coords_id,
             tile_start_coords,
             dim_num) < 0) {   // Range max precedes MBR
        max = med-1;
      } else if(cmp_col_order(
             range_max_coords_id,
             range_max_coords,
             tile_end_coords_id,
             tile_end_coords,
             dim_num) > 0) {   // Range max succeeds MBR
        min = med+1;
      } else {                 // Range max in MBR
        break; 
      }
    }

    // Determine the start position of the range
    if(max < min)    // Range max succeeds the tile at position max 
      tile_search_range_[1] = max;     
    else             // Range max included in a tile
      tile_search_range_[1] = med;
  }

  // Clean up
  delete [] range_min_coords;
  delete [] range_max_coords;
}

template<class T>
void ReadState::init_tile_search_range_id_row() {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int dim_num = array_schema->dim_num();
  const T* range = static_cast<const T*>(fragment_->array()->subarray());
  int64_t tile_num = book_keeping_->tile_num();
  const std::vector<void*>& bounding_coords = 
      book_keeping_->bounding_coords();

  // Calculate range coordinates
  T* range_min_coords = new T[dim_num];
  T* range_max_coords = new T[dim_num];
  for(int i=0; i<dim_num; ++i) {
    memcpy(&range_min_coords[i], &range[2*i], sizeof(T)); 
    memcpy(&range_max_coords[i], &range[2*i+1], sizeof(T)); 
  }
  int64_t range_min_coords_id = array_schema->tile_id(range_min_coords);
  int64_t range_max_coords_id = array_schema->tile_id(range_max_coords);

  // For the bounding coordinates of each investigated tile
  const T* tile_start_coords;
  const T* tile_end_coords;
  int64_t tile_start_coords_id;
  int64_t tile_end_coords_id;

  // --- Finding the start tile in search range

  // Perform binary search
  int64_t min = 0;
  int64_t max = tile_num - 1;
  int64_t med;
  while(min <= max) {
    med = min + ((max - min) / 2);

    // Get info for bounding coordinates
    tile_start_coords = static_cast<const T*>(bounding_coords[med]);
    tile_end_coords = &(static_cast<const T*>(bounding_coords[med])[dim_num]);
    tile_start_coords_id = array_schema->tile_id(tile_start_coords);
    tile_end_coords_id = array_schema->tile_id(tile_end_coords);
     
    // Calculate precedence
    if(cmp_row_order(
           range_min_coords_id,
           range_min_coords,
           tile_start_coords_id,
           tile_start_coords,
           dim_num) < 0) {   // Range min precedes MBR
      max = med-1;
    } else if(cmp_row_order(
           range_min_coords_id,
           range_min_coords,
           tile_end_coords_id,
           tile_end_coords,
           dim_num) > 0) {   // Range min succeeds MBR
      min = med+1;
    } else {                 // Range min in MBR
      break; 
    }
  }

  // Determine the start position of the range
  if(max < min)    // Range min precedes the tile at position min  
    tile_search_range_[0] = min;     
  else             // Range min included in a tile
    tile_search_range_[0] = med;

  if(is_unary_subarray(range, dim_num)) {  // Unary range
    // The end positions is the same as the start
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
      tile_start_coords_id = array_schema->tile_id(tile_start_coords);
      tile_end_coords_id = array_schema->tile_id(tile_end_coords);
     
      // Calculate precedence
      if(cmp_row_order(
             range_max_coords_id,
             range_max_coords,
             tile_start_coords_id,
             tile_start_coords,
             dim_num) < 0) {   // Range max precedes MBR
        max = med-1;
      } else if(cmp_row_order(
             range_max_coords_id,
             range_max_coords,
             tile_end_coords_id,
             tile_end_coords,
             dim_num) > 0) {   // Range max succeeds MBR
        min = med+1;
      } else {                 // Range max in MBR
        break; 
      }
    }

    // Determine the start position of the range
    if(max < min)    // Range max succeeds the tile at position max 
      tile_search_range_[1] = max;     
    else             // Range max included in a tile
      tile_search_range_[1] = med;
  }

  // Clean up
  delete [] range_min_coords;
  delete [] range_max_coords;
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

int ReadState::read_dense(
    void** buffers,
    size_t* buffer_sizes) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  const std::vector<int>& attribute_ids = fragment_->array()->attribute_ids();
  int attribute_id_num = attribute_ids.size(); 

  // Write each attribute individually
  int buffer_i = 0;
  int rc;
  for(int i=0; i<attribute_id_num; ++i) {
    if(!array_schema->var_size(attribute_ids[i])) { // FIXED CELLS
      rc = read_dense_attr(
               attribute_ids[i], 
               buffers[buffer_i], 
               buffer_sizes[buffer_i]);

      if(rc != TILEDB_WS_OK)
        break;
      ++buffer_i;
    } else {                                        // VARIABLE-SIZED CELLS
      rc = read_dense_attr_var(
               attribute_ids[i], 
               buffers[buffer_i],       // offsets 
               buffer_sizes[buffer_i],
               buffers[buffer_i+1],     // actual values
               buffer_sizes[buffer_i+1]);

      if(rc != TILEDB_WS_OK)
        break;
      buffer_i += 2;
    }
  }

  return rc;
}

int ReadState::read_dense_attr(
    int attribute_id,
    void* buffer,
    size_t& buffer_size) {
  // Trivial case
  if(buffer_size == 0) {
    overflow_[attribute_id] = true;
    return TILEDB_RS_OK;
  }

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int compression = array_schema->compression(attribute_id);

  // No compression
  if(compression == TILEDB_NO_COMPRESSION) {
    return read_dense_attr_cmp_none(attribute_id, buffer, buffer_size);
  } else { // GZIP
    return read_dense_attr_cmp_gzip(attribute_id, buffer, buffer_size);
  }
}

int ReadState::read_dense_attr_cmp_gzip(
    int attribute_id,
    void* buffer,
    size_t& buffer_size) {
  // Hanlde empty attributes
  if(is_empty_attribute(attribute_id)) {
    buffer_size = 0;
    return TILEDB_RS_OK;
  }

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int coords_type = array_schema->coords_type();

  // Invoke the proper templated function
  if(coords_type == TILEDB_INT32) {
    return read_dense_attr_cmp_gzip<int>(attribute_id, buffer, buffer_size);
  } else if(coords_type == TILEDB_INT64) {
    return read_dense_attr_cmp_gzip<int64_t>(attribute_id, buffer, buffer_size);
  } else {
    PRINT_ERROR("Cannot read from fragment; Invalid coordinates type");
    return TILEDB_RS_ERR;
  }
}

template<class T>
int ReadState::read_dense_attr_cmp_gzip(
    int attribute_id,
    void* buffer,
    size_t& buffer_size) {
  // Auxiliary variables
  size_t buffer_offset = 0;

  // The following loop should break somewhere inside
  for(;;) {
    // There are still data pending inside the local tile buffers
    if(tiles_offsets_[attribute_id] < tiles_sizes_[attribute_id]) 
      copy_from_tile_buffer_dense<T>(
          attribute_id, 
          buffer, 
          buffer_size, 
          buffer_offset); 

    // If the buffer overflows, return
    if(overflow_[attribute_id]) { 
      buffer_size = buffer_offset; 
      return TILEDB_RS_OK; 
    }

    // Compute the next overlapping tile
    if(overlapping_tiles_pos_[attribute_id] >= overlapping_tiles_.size())
      get_next_overlapping_tile_dense<T>();

    // Fectch and decompress tile
    if(overlapping_tiles_[overlapping_tiles_pos_[attribute_id]].overlap_
       != NONE) {
      if(get_tile_from_disk_cmp_gzip(attribute_id) != TILEDB_RS_OK)
        return TILEDB_RS_ERR;
    }

    // Invoke proper copy command
    if(overlapping_tiles_[overlapping_tiles_pos_[attribute_id]].overlap_
       == NONE) {                 // No more tiles
      buffer_size = buffer_offset;
      return TILEDB_RS_OK; 
    } else if(overlapping_tiles_[overlapping_tiles_pos_[attribute_id]].overlap_ 
              == FULL) {          // Full tile
      copy_from_tile_buffer_full(
          attribute_id, 
          buffer, 
          buffer_size, 
          buffer_offset);
    } else if(overlapping_tiles_[overlapping_tiles_pos_[attribute_id]].overlap_ 
              == PARTIAL_CONTIG) { // Partial tile, contig
      copy_from_tile_buffer_partial_contig_dense<T>(
          attribute_id, 
          buffer, 
          buffer_size, 
          buffer_offset);
    } else {                       // Partial tile, non-contig
      copy_from_tile_buffer_partial_non_contig_dense<T>(
          attribute_id, 
          buffer, 
          buffer_size, 
          buffer_offset);
    }

    // If the buffer overflows, return
    if(overflow_[attribute_id]) { 
      buffer_size = buffer_offset; 
      return TILEDB_RS_OK; 
    }
  }
}

int ReadState::read_dense_attr_cmp_none(
    int attribute_id,
    void* buffer,
    size_t& buffer_size) {
  // Hanlde empty attributes
  if(is_empty_attribute(attribute_id)) {
    buffer_size = 0;
    return TILEDB_RS_OK;
  }

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int coords_type = array_schema->coords_type();

  // Invoke the proper templated function
  if(coords_type == TILEDB_INT32) {
    return read_dense_attr_cmp_none<int>(attribute_id, buffer, buffer_size);
  } else if(coords_type == TILEDB_INT64) {
    return read_dense_attr_cmp_none<int64_t>(attribute_id, buffer, buffer_size);
  } else {
    PRINT_ERROR("Cannot read from fragment; Invalid coordinates type");
    return TILEDB_RS_ERR;
  }
}

template<class T>
int ReadState::read_dense_attr_cmp_none(
    int attribute_id,
    void* buffer,
    size_t& buffer_size) {
  // Auxiliary variables
  size_t buffer_offset = 0;

  // The following loop should break somewhere inside
  for(;;) {
    // There are still data pending inside the local tile buffers
    if(tiles_offsets_[attribute_id] < tiles_sizes_[attribute_id]) 
      copy_from_tile_buffer_dense<T>(
          attribute_id, 
          buffer, 
          buffer_size, 
          buffer_offset); 

    // If the buffer overflows, return
    if(overflow_[attribute_id]) { 
      buffer_size = buffer_offset; 
      return TILEDB_RS_OK; 
    }

    // Compute the next overlapping tile
    if(overlapping_tiles_pos_[attribute_id] >= overlapping_tiles_.size())
      get_next_overlapping_tile_dense<T>();

    // Invoke proper copy command based ont he overlap type
    Overlap overlap = 
        overlapping_tiles_[overlapping_tiles_pos_[attribute_id]].overlap_;
    if(overlap == NONE) {                 // No more tiles
      buffer_size = buffer_offset;
      return TILEDB_RS_OK; 
    } else if(overlap == FULL) {          // Full tile
      if(copy_tile_full(
             attribute_id, 
             buffer, 
             buffer_size, 
             buffer_offset) != TILEDB_RS_OK)
        return TILEDB_RS_ERR;
    } else if(overlap == PARTIAL_CONTIG) { // Partial tile, contig
      if(copy_tile_partial_contig_dense<T>(
             attribute_id, 
             buffer, 
             buffer_size, 
             buffer_offset) != TILEDB_RS_OK)
        return TILEDB_RS_ERR;
    } else {                              // Partial tile, non-contig
      if(copy_tile_partial_non_contig_dense<T>(
             attribute_id, 
             buffer, 
             buffer_size, 
             buffer_offset) != TILEDB_RS_OK)
        return TILEDB_RS_ERR;
    }

    // If the buffer overflows, return
    if(overflow_[attribute_id]) { 
      buffer_size = buffer_offset; 
      return TILEDB_RS_OK; 
    }
  }
}

int ReadState::read_dense_attr_var(
    int attribute_id,
    void* buffer,
    size_t& buffer_size,
    void* buffer_var,
    size_t& buffer_var_size) {
  // Trivial case
  if(buffer_size == 0 || buffer_var_size == 0) {
    overflow_[attribute_id] = true;
    buffer_size = 0;
    buffer_var_size = 0;
    return TILEDB_RS_OK;
  }

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int compression = array_schema->compression(attribute_id);

  // No compression
  if(compression == TILEDB_NO_COMPRESSION) {
    return read_dense_attr_var_cmp_none(
               attribute_id, 
               buffer, 
               buffer_size,
               buffer_var, 
               buffer_var_size);
  } else { // GZIP
    return read_dense_attr_var_cmp_gzip(
               attribute_id, 
               buffer, 
               buffer_size,
               buffer_var, 
               buffer_var_size);
  }
}

int ReadState::read_dense_attr_var_cmp_gzip(
    int attribute_id,
    void* buffer,
    size_t& buffer_size,
    void* buffer_var,
    size_t& buffer_var_size) {
  // Hanlde empty attributes
  if(is_empty_attribute(attribute_id)) {
    buffer_size = 0;
    buffer_var_size = 0;
    return TILEDB_RS_OK;
  }

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int coords_type = array_schema->coords_type();

  // Invoke the proper templated function
  if(coords_type == TILEDB_INT32) {
    return read_dense_attr_var_cmp_gzip<int>(
               attribute_id, 
               buffer, 
               buffer_size,
               buffer_var,
               buffer_var_size);
  } else if(coords_type == TILEDB_INT64) {
    return read_dense_attr_var_cmp_gzip<int64_t>(
               attribute_id, 
               buffer, 
               buffer_size,
               buffer_var,
               buffer_var_size);
  } else {
    PRINT_ERROR("Cannot read from fragment; Invalid coordinates type");
    return TILEDB_RS_ERR;
  }
}

template<class T>
int ReadState::read_dense_attr_var_cmp_gzip(
    int attribute_id,
    void* buffer,
    size_t& buffer_size,
    void* buffer_var,
    size_t& buffer_var_size) {
  // Auxiliary variables
  size_t buffer_offset = 0;
  size_t buffer_var_offset = 0;

  // The following loop should break somewhere inside
  for(;;) {
    // There are still data pending inside the local tile buffers
    if(tiles_offsets_[attribute_id] < tiles_sizes_[attribute_id]) 
      copy_from_tile_buffer_dense_var<T>(
          attribute_id, 
          buffer, 
          buffer_size, 
          buffer_offset,
          buffer_var,
          buffer_var_size,
          buffer_var_offset); 

    // If the buffer overflows, return
    if(overflow_[attribute_id]) { 
      buffer_size = buffer_offset;
      buffer_var_size = buffer_var_offset;
      return TILEDB_RS_OK; 
    }

    // Compute the next overlapping tile
    if(overlapping_tiles_pos_[attribute_id] >= overlapping_tiles_.size())
      get_next_overlapping_tile_dense<T>();

    // Fectch and decompress tile
    if(overlapping_tiles_[overlapping_tiles_pos_[attribute_id]].overlap_
       != NONE) {
      if(get_tile_from_disk_var_cmp_gzip(attribute_id) != TILEDB_RS_OK)
        return TILEDB_RS_ERR;
    }

    // Invoke proper copy command
    Overlap overlap = 
        overlapping_tiles_[overlapping_tiles_pos_[attribute_id]].overlap_;
    if(overlap == NONE) {                 // No more tiles
      buffer_size = buffer_offset;
      buffer_var_size = buffer_var_offset;
      return TILEDB_RS_OK; 
    } else if(overlap == FULL) {          // Full tile
      copy_from_tile_buffer_full_var(
          attribute_id, 
          buffer, 
          buffer_size, 
          buffer_offset,
          buffer_var,
          buffer_var_size,
          buffer_var_offset);
    } else if(overlap == PARTIAL_CONTIG) { // Partial tile, contig
      copy_from_tile_buffer_partial_contig_dense_var<T>(
          attribute_id, 
          buffer, 
          buffer_size, 
          buffer_offset,
          buffer_var,
          buffer_var_size,
          buffer_var_offset);
    } else {                       // Partial tile, non-contig
      copy_from_tile_buffer_partial_non_contig_dense_var<T>(
          attribute_id, 
          buffer, 
          buffer_size, 
          buffer_offset,
          buffer_var,
          buffer_var_size,
          buffer_var_offset);
    }

    // If the buffer overflows, return
    if(overflow_[attribute_id]) { 
      buffer_size = buffer_offset;
      buffer_var_size = buffer_var_offset;
      return TILEDB_RS_OK; 
    }
  }
}

int ReadState::read_dense_attr_var_cmp_none(
    int attribute_id,
    void* buffer,
    size_t& buffer_size,
    void* buffer_var,
    size_t& buffer_var_size) {
  // Hanlde empty attributes
  if(is_empty_attribute(attribute_id)) {
    buffer_size = 0;
    buffer_var_size = 0;
    return TILEDB_RS_OK;
  }

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int coords_type = array_schema->coords_type();

  // Invoke the proper templated function
  if(coords_type == TILEDB_INT32) {
    return read_dense_attr_var_cmp_none<int>(
               attribute_id, 
               buffer, 
               buffer_size,
               buffer_var,
               buffer_var_size);
  } else if(coords_type == TILEDB_INT64) {
    return read_dense_attr_var_cmp_none<int64_t>(
               attribute_id, 
               buffer, 
               buffer_size,
               buffer_var,
               buffer_var_size);
  } else {
    PRINT_ERROR("Cannot read from fragment; Invalid coordinates type");
    return TILEDB_RS_ERR;
  }
}

template<class T>
int ReadState::read_dense_attr_var_cmp_none(
    int attribute_id,
    void* buffer,
    size_t& buffer_size,
    void* buffer_var,
    size_t& buffer_var_size) {
  // Auxiliary variables
  size_t buffer_offset = 0;
  size_t buffer_var_offset = 0;

  // The following loop should break somewhere inside
  for(;;) {
    // There are still data pending inside the local tile buffers
    if(tiles_offsets_[attribute_id] < tiles_sizes_[attribute_id]) 
      copy_from_tile_buffer_dense_var<T>(
          attribute_id, 
          buffer, 
          buffer_size, 
          buffer_offset,
          buffer_var,
          buffer_var_size,
          buffer_var_offset); 

    // If the buffer overflows, return
    if(overflow_[attribute_id]) { 
      buffer_size = buffer_offset;
      buffer_var_size = buffer_var_offset;
      return TILEDB_RS_OK; 
    }

    // Compute the next overlapping tile
    if(overlapping_tiles_pos_[attribute_id] >= overlapping_tiles_.size())
      get_next_overlapping_tile_dense<T>();

    // Invoke proper copy command
    Overlap overlap = 
        overlapping_tiles_[overlapping_tiles_pos_[attribute_id]].overlap_;
    if(overlap == NONE) {                 // No more tiles
      buffer_size = buffer_offset;
      buffer_var_size = buffer_var_offset;
      return TILEDB_RS_OK; 
    } else if(overlap == FULL) {          // Full tile
      if(copy_tile_full_var(
             attribute_id, 
             buffer, 
             buffer_size, 
             buffer_offset,
             buffer_var,
             buffer_var_size,
             buffer_var_offset) != TILEDB_RS_OK)
        return TILEDB_RS_ERR;
    } else if(overlap == PARTIAL_CONTIG) { // Partial tile, contig
      if(copy_tile_partial_contig_dense_var<T>(
             attribute_id, 
             buffer, 
             buffer_size, 
             buffer_offset, 
             buffer_var, 
             buffer_var_size,
             buffer_var_offset) != TILEDB_RS_OK)
        return TILEDB_RS_ERR;
    } else {                       // Partial tile, non-contig
      if(copy_tile_partial_non_contig_dense_var<T>(
             attribute_id, 
             buffer, 
             buffer_size, 
             buffer_offset,
             buffer_var,
             buffer_var_size,
             buffer_var_offset) != TILEDB_RS_OK)
        return TILEDB_RS_ERR;
    }

    // If the buffer overflows, return
    if(overflow_[attribute_id]) { 
      buffer_size = buffer_offset;
      buffer_var_size = buffer_var_offset;
      return TILEDB_RS_OK; 
    }
  }
}

int ReadState::read_sparse(
    void** buffers,
    size_t* buffer_sizes) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num(); 
  const std::vector<int>& attribute_ids = fragment_->array()->attribute_ids();
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
    else
      buffer_i +=2;
  }

  // Process coordinates first, if they are in the selected attributes
  if(coords_buffer_i != -1)
    if(read_sparse_attr(
           attribute_num, 
           buffers[coords_buffer_i], 
           buffer_sizes[coords_buffer_i]) != TILEDB_RS_OK)
      return TILEDB_RS_ERR;

  // Process each other attribute individually
  buffer_i = 0;
  int rc;
  for(int i=0; i<attribute_id_num; ++i) {
    // Skip coordinates 
    if(attribute_ids[i] == attribute_num) {
      ++buffer_i;
      continue;
    }

    if(!array_schema->var_size(attribute_ids[i])) { // FIXED CELLS
      rc = read_sparse_attr(
               attribute_ids[i], 
               buffers[buffer_i], 
               buffer_sizes[buffer_i]);

      if(rc != TILEDB_WS_OK)
        break;
      ++buffer_i;
    } else {                                        // VARIABLE-SIZED CELLS
      rc = read_sparse_attr_var(
               attribute_ids[i], 
               buffers[buffer_i],       // offsets 
               buffer_sizes[buffer_i],
               buffers[buffer_i+1],     // actual values
               buffer_sizes[buffer_i+1]);

      if(rc != TILEDB_WS_OK)
        break;
      buffer_i += 2;
    }
  }

  return rc;
} 

int ReadState::read_sparse_attr(
    int attribute_id,
    void* buffer,
    size_t& buffer_size) {
  // Trivial case
  if(buffer_size == 0) {
    overflow_[attribute_id] = true;
    return TILEDB_RS_OK;
  }

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int compression = array_schema->compression(attribute_id);

  // No compression
  if(compression == TILEDB_NO_COMPRESSION) {
    return read_sparse_attr_cmp_none(attribute_id, buffer, buffer_size);
  } else { // GZIP
    return read_sparse_attr_cmp_gzip(attribute_id, buffer, buffer_size);
  }
}

int ReadState::read_sparse_attr_cmp_gzip(
    int attribute_id,
    void* buffer,
    size_t& buffer_size) {
  // Hanlde empty attributes
  if(is_empty_attribute(attribute_id)) {
    buffer_size = 0;
    return TILEDB_RS_OK;
  }

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int coords_type = array_schema->coords_type();

  // Invoke the proper templated function
  if(coords_type == TILEDB_INT32) {
    return read_sparse_attr_cmp_gzip<int>(
               attribute_id, 
               buffer, 
               buffer_size);
  } else if(coords_type == TILEDB_INT64) {
    return read_sparse_attr_cmp_gzip<int64_t>(
               attribute_id, 
               buffer, 
               buffer_size);
  } else if(coords_type == TILEDB_FLOAT32) {
    return read_sparse_attr_cmp_gzip<float>(
               attribute_id, 
               buffer, 
               buffer_size);
  } else if(coords_type == TILEDB_FLOAT64) {
    return read_sparse_attr_cmp_gzip<double>(
               attribute_id, 
               buffer, 
               buffer_size);
  } else {
    PRINT_ERROR("Cannot read from fragment; Invalid coordinates type");
    return TILEDB_RS_ERR;
  }
}

template<class T>
int ReadState::read_sparse_attr_cmp_gzip(
    int attribute_id,
    void* buffer,
    size_t& buffer_size) {
  // Auxiliary variables
  size_t buffer_offset = 0;

  // The following loop should break somewhere inside
  for(;;) {
    // There are still data pending inside the local tile buffers
    if(tiles_offsets_[attribute_id] < tiles_sizes_[attribute_id]) 
      copy_from_tile_buffer_sparse<T>(
          attribute_id, 
          buffer, 
          buffer_size, 
          buffer_offset); 

    // If the buffer overflows, return
    if(overflow_[attribute_id]) { 
      buffer_size = buffer_offset; 
      return TILEDB_RS_OK; 
    }

    // Compute the next overlapping tile
    if(overlapping_tiles_pos_[attribute_id] >= overlapping_tiles_.size()) 
      get_next_overlapping_tile_sparse<T>();

    // Fectch and decompress tile
    int64_t pos = overlapping_tiles_pos_[attribute_id];
    Overlap overlap = overlapping_tiles_[pos].overlap_;

    if(overlap != NONE) {
      if(get_tile_from_disk_cmp_gzip(attribute_id) != TILEDB_RS_OK)
        return TILEDB_RS_ERR;
    }

    // Invoke proper copy command
    if(overlap == NONE) {                 // No more tiles
      buffer_size = buffer_offset;
      return TILEDB_RS_OK; 
    } else if(overlap == FULL) {          // Full tile
      copy_from_tile_buffer_full(
          attribute_id, 
          buffer, 
          buffer_size, 
          buffer_offset);
    } else if(overlap == PARTIAL_CONTIG) { // Partial tile, contig
      copy_from_tile_buffer_partial_contig_sparse<T>(
          attribute_id, 
          buffer, 
          buffer_size, 
          buffer_offset);
    } else {                               // Partial tile, non-contig
      copy_from_tile_buffer_partial_non_contig_sparse<T>(
          attribute_id, 
          buffer, 
          buffer_size, 
          buffer_offset);
    }

    // If the buffer overflows, return
    if(overflow_[attribute_id]) { 
      buffer_size = buffer_offset; 
      return TILEDB_RS_OK; 
    }
  }
}

int ReadState::read_sparse_attr_cmp_none(
    int attribute_id,
    void* buffer,
    size_t& buffer_size) {
  // Hanlde empty attributes
  if(is_empty_attribute(attribute_id)) {
    buffer_size = 0;
    return TILEDB_RS_OK;
  }

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int coords_type = array_schema->coords_type();

  // Invoke the proper templated function
  if(coords_type == TILEDB_INT32) {
    return read_sparse_attr_cmp_none<int>(
               attribute_id, 
               buffer, 
               buffer_size);
  } else if(coords_type == TILEDB_INT64) {
    return read_sparse_attr_cmp_none<int64_t>(
               attribute_id, 
               buffer, 
               buffer_size);
  } else if(coords_type == TILEDB_FLOAT32) {
    return read_sparse_attr_cmp_none<float>(
               attribute_id, 
               buffer, 
               buffer_size);
  } else if(coords_type == TILEDB_FLOAT64) {
    return read_sparse_attr_cmp_none<double>(
               attribute_id, 
               buffer, 
               buffer_size);
  } else {
    PRINT_ERROR("Cannot read from fragment; Invalid coordinates type");
    return TILEDB_RS_ERR;
  }
}

template<class T>
int ReadState::read_sparse_attr_cmp_none(
    int attribute_id,
    void* buffer,
    size_t& buffer_size) {
  // For easy refernece
  int attribute_num = fragment_->array()->array_schema()->attribute_num();

  // Auxiliary variables
  size_t buffer_offset = 0;

  // The following loop should break somewhere inside
  for(;;) {
    // There are still data pending inside the local tile buffers
    if(tiles_offsets_[attribute_id] < tiles_sizes_[attribute_id]) { 
      copy_from_tile_buffer_sparse<T>(
          attribute_id, 
          buffer, 
          buffer_size, 
          buffer_offset); 
    }

    // If the buffer overflows, return
    if(overflow_[attribute_id]) { 
      buffer_size = buffer_offset; 
      return TILEDB_RS_OK; 
    }

    // Compute the next overlapping tile
    if(overlapping_tiles_pos_[attribute_id] >= overlapping_tiles_.size())
      get_next_overlapping_tile_sparse<T>();

    // Invoke proper copy command
    int64_t pos = overlapping_tiles_pos_[attribute_id];
    Overlap overlap = overlapping_tiles_[pos].overlap_;
    if(attribute_id == fragment_->array()->array_schema()->attribute_num() &&
       overlapping_tiles_[pos].tile_fetched_[attribute_num]) {
      // The coordinates tile is already in main memory
      copy_from_tile_buffer_sparse<T>(
          attribute_id, 
          buffer, 
          buffer_size, 
          buffer_offset);
    } else if(overlap == NONE) {                 // No more tiles
      buffer_size = buffer_offset;
      return TILEDB_RS_OK; 
    } else if(overlap == FULL) {                 // Full tile
      if(copy_tile_full(
             attribute_id, 
             buffer, 
             buffer_size, 
             buffer_offset) != TILEDB_RS_OK)
        return TILEDB_RS_ERR;
    } else if(overlap == PARTIAL_CONTIG) {       // Partial tile, contig
      if(copy_tile_partial_contig_sparse<T>(
             attribute_id, 
             buffer, 
             buffer_size, 
             buffer_offset) != TILEDB_RS_OK)
        return TILEDB_RS_ERR;
    } else {                                     // Partial tile, non-contig
      if(copy_tile_partial_non_contig_sparse<T>(
             attribute_id, 
             buffer, 
             buffer_size, 
             buffer_offset) != TILEDB_RS_OK)
        return TILEDB_RS_ERR;
    }

    // If the buffer overflows, return
    if(overflow_[attribute_id]) { 
      buffer_size = buffer_offset; 
      return TILEDB_RS_OK; 
    }
  }
}

int ReadState::read_sparse_attr_var(
    int attribute_id,
    void* buffer,
    size_t& buffer_size,
    void* buffer_var,
    size_t& buffer_var_size) {
  // Trivial case
  if(buffer_size == 0 || buffer_var_size == 0) {
    overflow_[attribute_id] = true;
    buffer_size = 0;
    buffer_var_size = 0;
    return TILEDB_RS_OK;
  }

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int compression = array_schema->compression(attribute_id);

  // No compression
  if(compression == TILEDB_NO_COMPRESSION) {
    return read_sparse_attr_var_cmp_none(
               attribute_id, 
               buffer, 
               buffer_size,
               buffer_var, 
               buffer_var_size);
  } else { // GZIP
    return read_sparse_attr_var_cmp_gzip(
               attribute_id, 
               buffer, 
               buffer_size,
               buffer_var, 
               buffer_var_size);
  }
}

int ReadState::read_sparse_attr_var_cmp_gzip(
    int attribute_id,
    void* buffer,
    size_t& buffer_size,
    void* buffer_var,
    size_t& buffer_var_size) {
  // Hanlde empty attributes
  if(is_empty_attribute(attribute_id)) {
    buffer_size = 0;
    buffer_var_size = 0;
    return TILEDB_RS_OK;
  }

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int coords_type = array_schema->coords_type();

  // Invoke the proper templated function
  if(coords_type == TILEDB_INT32) {
    return read_sparse_attr_var_cmp_gzip<int>(
               attribute_id, 
               buffer, 
               buffer_size,
               buffer_var, 
               buffer_var_size);
  } else if(coords_type == TILEDB_INT64) {
    return read_sparse_attr_var_cmp_gzip<int64_t>(
               attribute_id, 
               buffer, 
               buffer_size,
               buffer_var, 
               buffer_var_size);
  } else if(coords_type == TILEDB_FLOAT32) {
    return read_sparse_attr_var_cmp_gzip<float>(
               attribute_id, 
               buffer, 
               buffer_size,
               buffer_var, 
               buffer_var_size);
  } else if(coords_type == TILEDB_FLOAT64) {
    return read_sparse_attr_var_cmp_gzip<double>(
               attribute_id, 
               buffer, 
               buffer_size,
               buffer_var, 
               buffer_var_size);
  } else {
    PRINT_ERROR("Cannot read from fragment; Invalid coordinates type");
    return TILEDB_RS_ERR;
  }
}

template<class T>
int ReadState::read_sparse_attr_var_cmp_gzip(
    int attribute_id,
    void* buffer,
    size_t& buffer_size,
    void* buffer_var,
    size_t& buffer_var_size) {
  // Auxiliary variables
  size_t buffer_offset = 0;
  size_t buffer_var_offset = 0;

  // The following loop should break somewhere inside
  for(;;) {
    // There are still data pending inside the local tile buffers
    if(tiles_offsets_[attribute_id] < tiles_sizes_[attribute_id]) 
      copy_from_tile_buffer_sparse_var<T>(
          attribute_id, 
          buffer, 
          buffer_size, 
          buffer_offset, 
          buffer_var, 
          buffer_var_size, 
          buffer_var_offset); 

    // If the buffer overflows, return
    if(overflow_[attribute_id]) { 
      buffer_size = buffer_offset;
      buffer_var_size = buffer_var_offset;
      return TILEDB_RS_OK; 
    }

    // Compute the next overlapping tile
    if(overlapping_tiles_pos_[attribute_id] >= overlapping_tiles_.size())
      get_next_overlapping_tile_sparse<T>();

    // Fectch and decompress tile
    int64_t pos = overlapping_tiles_pos_[attribute_id];
    Overlap overlap = overlapping_tiles_[pos].overlap_;
    if(overlap != NONE) {
      if(get_tile_from_disk_var_cmp_gzip(attribute_id) != TILEDB_RS_OK)
        return TILEDB_RS_ERR;
    }

    // Invoke proper copy command
    if(overlap == NONE) {                 // No more tiles
      buffer_size = buffer_offset;
      buffer_var_size = buffer_var_offset;
      return TILEDB_RS_OK; 
    } else if(overlap == FULL) {          // Full tile
      copy_from_tile_buffer_full_var(
          attribute_id, 
          buffer, 
          buffer_size, 
          buffer_offset, 
          buffer_var, 
          buffer_var_size, 
          buffer_var_offset);
    } else if(overlap == PARTIAL_CONTIG) { // Partial tile, contig
      copy_from_tile_buffer_partial_contig_sparse_var<T>(
          attribute_id, 
          buffer, 
          buffer_size, 
          buffer_offset, 
          buffer_var, 
          buffer_var_size, 
          buffer_var_offset);
    } else {                               // Partial tile, non-contig
      copy_from_tile_buffer_partial_non_contig_sparse_var<T>(
          attribute_id, 
          buffer, 
          buffer_size, 
          buffer_offset, 
          buffer_var, 
          buffer_var_size, 
          buffer_var_offset);
    }

    // If the buffer overflows, return
    if(overflow_[attribute_id]) { 
      buffer_size = buffer_offset;
      buffer_var_size = buffer_var_offset;
      return TILEDB_RS_OK; 
    }
  }
}

int ReadState::read_sparse_attr_var_cmp_none(
    int attribute_id,
    void* buffer,
    size_t& buffer_size,
    void* buffer_var,
    size_t& buffer_var_size) {
  // Hanlde empty attributes
  if(is_empty_attribute(attribute_id)) {
    buffer_size = 0;
    buffer_var_size = 0;
    return TILEDB_RS_OK;
  }

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int coords_type = array_schema->coords_type();

  // Invoke the proper templated function
  if(coords_type == TILEDB_INT32) {
    return read_sparse_attr_var_cmp_none<int>(
               attribute_id, 
               buffer, 
               buffer_size,
               buffer_var, 
               buffer_var_size);
  } else if(coords_type == TILEDB_INT64) {
    return read_sparse_attr_var_cmp_none<int64_t>(
               attribute_id, 
               buffer, 
               buffer_size,
               buffer_var, 
               buffer_var_size);
  } else if(coords_type == TILEDB_FLOAT32) {
    return read_sparse_attr_var_cmp_none<float>(
               attribute_id, 
               buffer, 
               buffer_size,
               buffer_var, 
               buffer_var_size);
  } else if(coords_type == TILEDB_FLOAT64) {
    return read_sparse_attr_var_cmp_none<double>(
               attribute_id, 
               buffer, 
               buffer_size,
               buffer_var, 
               buffer_var_size);
  } else {
    PRINT_ERROR("Cannot read from fragment; Invalid coordinates type");
    return TILEDB_RS_ERR;
  }
}

template<class T>
int ReadState::read_sparse_attr_var_cmp_none(
    int attribute_id,
    void* buffer,
    size_t& buffer_size,
    void* buffer_var,
    size_t& buffer_var_size) {
  // Auxiliary variables
  size_t buffer_offset = 0;
  size_t buffer_var_offset = 0;

  // The following loop should break somewhere inside
  for(;;) {
    // There are still data pending inside the local tile buffers
    if(tiles_offsets_[attribute_id] < tiles_sizes_[attribute_id]) 
      copy_from_tile_buffer_sparse_var<T>(
          attribute_id, 
          buffer, 
          buffer_size, 
          buffer_offset, 
          buffer_var, 
          buffer_var_size, 
          buffer_var_offset); 

    // If the buffer overflows, return
    if(overflow_[attribute_id]) { 
      buffer_size = buffer_offset;
      buffer_var_size = buffer_var_offset;
      return TILEDB_RS_OK; 
    }

    // Compute the next overlapping tile
    if(overlapping_tiles_pos_[attribute_id] >= overlapping_tiles_.size())
      get_next_overlapping_tile_sparse<T>();

    // Invoke proper copy command
    int64_t pos = overlapping_tiles_pos_[attribute_id];
    Overlap overlap = overlapping_tiles_[pos].overlap_;
    if(overlap == NONE) {                 // No more tiles
      buffer_size = buffer_offset;
      buffer_var_size = buffer_var_offset;
      return TILEDB_RS_OK; 
    } else if(overlap == FULL) {          // Full tile
      if(copy_tile_full_var(
             attribute_id, 
             buffer, 
             buffer_size, 
             buffer_offset,
             buffer_var, 
             buffer_var_size, 
             buffer_var_offset) != TILEDB_RS_OK)
        return TILEDB_RS_ERR;
    } else if(overlap == PARTIAL_CONTIG) { // Partial tile, contig
      if(copy_tile_partial_contig_sparse_var<T>(
             attribute_id, 
             buffer, 
             buffer_size, 
             buffer_offset,
             buffer_var, 
             buffer_var_size, 
             buffer_var_offset) != TILEDB_RS_OK)
        return TILEDB_RS_ERR;
    } else {                               // Partial tile, non-contig
      if(copy_tile_partial_non_contig_sparse_var<T>(
             attribute_id, 
             buffer, 
             buffer_size, 
             buffer_offset,
             buffer_var, 
             buffer_var_size, 
             buffer_var_offset) != TILEDB_RS_OK)
        return TILEDB_RS_ERR;
    }

    // If the buffer overflows, return
    if(overflow_[attribute_id]) { 
      buffer_size = buffer_offset;
      buffer_var_size = buffer_var_offset;
      return TILEDB_RS_OK; 
    }
  }
}

int ReadState::read_tile_from_file_cmp_gzip(
    int attribute_id,
    off_t offset,
    size_t tile_size) {
  // Potentially allocate compressed tile buffer
  if(tile_compressed_ == NULL) {
    const ArraySchema* array_schema = fragment_->array()->array_schema();
    size_t full_tile_size = fragment_->tile_size(attribute_id);
    size_t tile_max_size = 
        full_tile_size + 6 + 5*(ceil(full_tile_size/16834.0));
    tile_compressed_ = malloc(tile_max_size); 
    tile_compressed_allocated_size_ = tile_max_size;
  }

  // Prepare attribute file name
  std::string filename = 
      fragment_->fragment_name() + "/" +
      fragment_->array()->array_schema()->attribute(attribute_id) +
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
  // Allocate space for the tile if needed
  if(tiles_[attribute_id] == NULL) {
    const ArraySchema* array_schema = fragment_->array()->array_schema();
    size_t full_tile_size = fragment_->tile_size(attribute_id);
    tiles_[attribute_id] = malloc(full_tile_size);
  }

  // Set the actual tile size
  tiles_sizes_[attribute_id] = tile_size; 

  // Prepare attribute file name
  std::string filename = 
      fragment_->fragment_name() + "/" +
      fragment_->array()->array_schema()->attribute(attribute_id) +
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
      fragment_->array()->array_schema()->attribute(attribute_id) +
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
      fragment_->array()->array_schema()->attribute(attribute_id) +
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
  bool var_size = fragment_->array()->array_schema()->var_size(attribute_id);
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
  tiles_sizes_[attribute_id] = tile_size; 

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

void ReadState::reset_overflow() {
  for(int i=0; i<int(overflow_.size()); ++i)
    overflow_[i] = false;
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
template void ReadState::compute_fragment_cell_ranges<int>(
    const FragmentInfo& fragment_info,
    FragmentCellRanges& fragment_cell_ranges) const;
template void ReadState::compute_fragment_cell_ranges<int64_t>(
    const FragmentInfo& fragment_info,
    FragmentCellRanges& fragment_cell_ranges) const;
template void ReadState::compute_fragment_cell_ranges<float>(
    const FragmentInfo& fragment_info,
    FragmentCellRanges& fragment_cell_ranges) const;
template void ReadState::compute_fragment_cell_ranges<double>(
    const FragmentInfo& fragment_info,
    FragmentCellRanges& fragment_cell_ranges) const;

template bool ReadState::max_overlap<int>(
    const int* max_overlap_range) const;
template bool ReadState::max_overlap<int64_t>(
    const int64_t* max_overlap_range) const;
template bool ReadState::max_overlap<float>(
    const float* max_overlap_range) const;
template bool ReadState::max_overlap<double>(
    const double* max_overlap_range) const;

template int ReadState::get_first_two_coords<int>(
    int tile_i,
    int* start_coords,
    int* first_coords,
    int* second_coords);
template int ReadState::get_first_two_coords<int64_t>(
    int tile_i,
    int64_t* start_coords,
    int64_t* first_coords,
    int64_t* second_coords);
template int ReadState::get_first_two_coords<float>(
    int tile_i,
    float* start_coords,
    float* first_coords,
    float* second_coords);
template int ReadState::get_first_two_coords<double>(
    int tile_i,
    double* start_coords,
    double* first_coords,
    double* second_coords);

template int ReadState::get_first_coords_after<int>(
    int tile_i,
    int* start_coords_before,
    int* first_coords);
template int ReadState::get_first_coords_after<int64_t>(
    int tile_i,
    int64_t* start_coords_before,
    int64_t* first_coords);
template int ReadState::get_first_coords_after<float>(
    int tile_i,
    float* start_coords_before,
    float* first_coords);
template int ReadState::get_first_coords_after<double>(
    int tile_i,
    double* start_coords_before,
    double* first_coords);

template int ReadState::get_cell_pos_ranges_sparse<int>(
    const FragmentInfo& fragment_info,
    const int* tile_domain,
    const int* cell_range,
    FragmentCellPosRanges& fragment_cell_pos_ranges);
template int ReadState::get_cell_pos_ranges_sparse<int64_t>(
    const FragmentInfo& fragment_info,
    const int64_t* tile_domain,
    const int64_t* cell_range,
    FragmentCellPosRanges& fragment_cell_pos_ranges);
template int ReadState::get_cell_pos_ranges_sparse<float>(
    const FragmentInfo& fragment_info,
    const float* tile_domain,
    const float* cell_range,
    FragmentCellPosRanges& fragment_cell_pos_ranges);
template int ReadState::get_cell_pos_ranges_sparse<double>(
    const FragmentInfo& fragment_info,
    const double* tile_domain,
    const double* cell_range,
    FragmentCellPosRanges& fragment_cell_pos_ranges);

template int ReadState::copy_cell_range<int>(
    int attribute_id,
    int tile_i,
    void* buffer,  
    size_t buffer_size,
    size_t& buffer_offset,
    const CellPosRange& cell_pos_range);
template int ReadState::copy_cell_range<int64_t>(
    int attribute_id,
    int tile_i,
    void* buffer,  
    size_t buffer_size,
    size_t& buffer_offset,
    const CellPosRange& cell_pos_range);
template int ReadState::copy_cell_range<float>(
    int attribute_id,
    int tile_i,
    void* buffer,  
    size_t buffer_size,
    size_t& buffer_offset,
    const CellPosRange& cell_pos_range);
template int ReadState::copy_cell_range<double>(
    int attribute_id,
    int tile_i,
    void* buffer,  
    size_t buffer_size,
    size_t& buffer_offset,
    const CellPosRange& cell_pos_range);

template bool ReadState::coords_exist<int>(
    int64_t tile_i,
    const int* coords);
template bool ReadState::coords_exist<int64_t>(
    int64_t tile_i,
    const int64_t* coords);
template bool ReadState::coords_exist<float>(
    int64_t tile_i,
    const float* coords);
template bool ReadState::coords_exist<double>(
    int64_t tile_i,
    const double* coords);

template int ReadState::copy_cell_range_var<int>(
    int attribute_id,
    int tile_i,
    void* buffer,  
    size_t buffer_size,
    size_t& buffer_offset,
    void* buffer_var,  
    size_t buffer_var_size,
    size_t& buffer_var_offset,
    const CellPosRange& cell_pos_range);
template int ReadState::copy_cell_range_var<int64_t>(
    int attribute_id,
    int tile_i,
    void* buffer,  
    size_t buffer_size,
    size_t& buffer_offset,
    void* buffer_var,  
    size_t buffer_var_size,
    size_t& buffer_var_offset,
    const CellPosRange& cell_pos_range);
template int ReadState::copy_cell_range_var<float>(
    int attribute_id,
    int tile_i,
    void* buffer,  
    size_t buffer_size,
    size_t& buffer_offset,
    void* buffer_var,  
    size_t buffer_var_size,
    size_t& buffer_var_offset,
    const CellPosRange& cell_pos_range);
template int ReadState::copy_cell_range_var<double>(
    int attribute_id,
    int tile_i,
    void* buffer,  
    size_t buffer_size,
    size_t& buffer_offset,
    void* buffer_var,  
    size_t buffer_var_size,
    size_t& buffer_var_offset,
    const CellPosRange& cell_pos_range);

template void ReadState::tile_done<int>(int attribute_id);
template void ReadState::tile_done<int64_t>(int attribute_id);
template void ReadState::tile_done<float>(int attribute_id);
template void ReadState::tile_done<double>(int attribute_id);
