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

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

ReadState::ReadState(
    const Fragment* fragment, 
    BookKeeping* book_keeping)
    : fragment_(fragment),
      book_keeping_(book_keeping) {
  // For easy reference 
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();
  const std::type_info* coords_type = array_schema->coords_type();
  bool dense = array_schema->dense();

  // Initializations
  range_in_tile_domain_ = NULL;
  overlapping_tile_pos_.resize(attribute_num+1); 
  tiles_.resize(attribute_num+1);
  tile_offsets_.resize(attribute_num+1);
  tile_sizes_.resize(attribute_num+1);

  for(int i=0; i<attribute_num+1; ++i) {
    overlapping_tile_pos_[i] = 0;
    tiles_[i] = NULL;
    tile_offsets_[i] = 0;
    tile_sizes_[i] = 0;
  }

  // More initializations only for the dense case
  if(dense) {
    if(coords_type == &typeid(int)) {
      init_range_in_tile_domain<int>();
    } else if(coords_type == &typeid(int64_t)) {
      init_range_in_tile_domain<int64_t>();
    } else {
      // The code should never reach here
      assert(0);
    } 
  }
}

ReadState::~ReadState() { 
  // For easy reference 
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();

  for(int i=0; i<overlapping_tiles_.size(); ++i) { 
    if(overlapping_tiles_[i].overlap_range_ != NULL)
      free(overlapping_tiles_[i].overlap_range_);

    if(overlapping_tiles_[i].coords_ != NULL)
      free(overlapping_tiles_[i].coords_);
   }

  for(int i=0; i<attribute_num+1; ++i) {
    if(tiles_[i] != NULL)
      free(tiles_[i]);
  }

  if(range_in_tile_domain_ != NULL)
    free(range_in_tile_domain_);
}

/* ****************************** */
/*          READ FUNCTIONS        */
/* ****************************** */

int ReadState::read(void** buffers, size_t* buffer_sizes) {
  // For easy reference
  const Array* array = fragment_->array();
  const ArraySchema* array_schema = fragment_->array()->array_schema();

  // Dispatch the proper write command
  if(array->mode() == TILEDB_READ) {                 // NORMAL
    if(array->array_schema()->dense()) {                   // DENSE
      return read_dense(buffers, buffer_sizes);       
    } else {                                               // SPARSE ARRAY
      // TODO
    }
  } else if (array->mode() == TILEDB_READ_REVERSE) { // REVERSE
    // TODO
  } else {
    PRINT_ERROR("Cannot read from fragment; Invalid mode");
    return TILEDB_RS_ERR;
  } 
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

template<class T>
void ReadState::get_next_overlapping_tile() {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int dim_num = array_schema->dim_num();
  size_t coords_size = array_schema->coords_size();

  // The next overlapping tile
  OverlappingTile overlapping_tile;
  overlapping_tile.coords_ = malloc(coords_size);
  overlapping_tile.overlap_range_ = malloc(2*coords_size);

  // For easy reference
  const T* range_in_tile_domain = static_cast<const T*>(range_in_tile_domain_);
  T* coords = static_cast<T*>(overlapping_tile.coords_);
  T* overlap_range = static_cast<T*>(overlapping_tile.overlap_range_);
  const T* range = static_cast<const T*>(fragment_->array()->range());

  // Get coordinates
  if(overlapping_tiles_.size() == 0) {  // First tile
    for(int i=0; i<dim_num; ++i)
      coords[i] = range_in_tile_domain[2*i]; 
  } else {                              // Every other tile
      array_schema->get_next_tile_coords<T>(range_in_tile_domain, coords);
  }

  // Get the position
  overlapping_tile.pos_ = array_schema->get_tile_pos<T>(coords);

  // Get tile overlap
  int overlap;
  array_schema->get_tile_range_overlap<T>(
      range,           // query range
      coords,          // tile coordinates in tile domain
      overlap_range,   // returned overlap in terms of cell coordinates in tile
      overlap);        // returned type of overlap
  if(overlap == 0) {
    overlapping_tile.overlap_ = NONE;
  } else if(overlap == 1) {
    overlapping_tile.overlap_ = FULL;
  } else if(overlap == 2) {
    overlapping_tile.overlap_ = PARTIAL;
  } else if(overlap == 3) {
    overlapping_tile.overlap_ = PARTIAL_SPECIAL;
  }
 
  // Store the new tile
  overlapping_tiles_.push_back(overlapping_tile); 
}

template<class T>
void ReadState::copy_from_tile_buffer(
    int attribute_id,
    void* buffer,
    size_t buffer_size,
    size_t& buffer_offset) {
  if(overlapping_tiles_[overlapping_tile_pos_[attribute_id]].overlap_
     == FULL)
    copy_from_tile_buffer_full(
        attribute_id, 
        buffer, 
        buffer_size, 
        buffer_offset);
  else if(overlapping_tiles_[overlapping_tile_pos_[attribute_id]].overlap_
          == PARTIAL)
    copy_from_tile_buffer_partial<T>(
        attribute_id, 
        buffer, 
        buffer_size, 
        buffer_offset);
  else if(overlapping_tiles_[overlapping_tile_pos_[attribute_id]].overlap_
          == PARTIAL_SPECIAL)
    copy_from_tile_buffer_partial_special<T>(
        attribute_id, 
        buffer, 
        buffer_size, 
        buffer_offset);
}

void ReadState::copy_from_tile_buffer_full(
    int attribute_id,
    void* buffer,
    size_t buffer_size,
    size_t& buffer_offset) {
  // Calculate the appropriate sizes
  size_t bytes_left_to_copy = 
      tile_sizes_[attribute_id] - tile_offsets_[attribute_id];
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
        tile + tile_offsets_[attribute_id], 
        bytes_to_copy);
    tile_offsets_[attribute_id] += bytes_to_copy;
    buffer_offset += bytes_to_copy;
    bytes_left_to_copy -= bytes_to_copy;
  }

  // Check overflow
  if(bytes_left_to_copy > 0) {
    assert(buffer_offset == buffer_size);
    buffer_offset = buffer_size + 1; // This will indicate the overflow
  } else {  // Finished copying this tile
    assert(tile_offsets_[attribute_id] == tile_sizes_[attribute_id]);
    ++overlapping_tile_pos_[attribute_id];
  }
}

template<class T>
void ReadState::copy_from_tile_buffer_partial(
    int attribute_id,
    void* buffer,
    size_t buffer_size,
    size_t& buffer_offset) {
  // Calculate free space in buffer
  size_t buffer_free_space = buffer_size - buffer_offset;
  if(buffer_free_space == 0) { // Overflow
    ++buffer_offset; // This will indicate the overflow
    return;
  }

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  OverlappingTile& overlapping_tile = 
      overlapping_tiles_[overlapping_tile_pos_[attribute_id]];
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
  size_t range_end_offset = range_end_cell_pos * cell_size; 

  // If current tile offset is 0, set it to the beginning of the overlap range
  if(tile_offsets_[attribute_id] == 0) 
    tile_offsets_[attribute_id] = range_start_offset;

  // Compute slab information
  T cell_num_in_range_slab = 
      array_schema->cell_num_in_range_slab<T>(overlap_range);
  size_t range_slab_size = cell_num_in_range_slab * cell_size;
  T cell_num_in_tile_slab = array_schema->cell_num_in_tile_slab<T>();
  size_t tile_slab_size = cell_num_in_tile_slab * cell_size;
  size_t current_slab_start_offset = 
      ((tile_offsets_[attribute_id] - range_start_offset) / tile_slab_size) *
      tile_slab_size + range_start_offset;
  size_t current_slab_end_offset = 
      current_slab_start_offset + range_slab_size - 1;

  // Copy rest of the current slab
  size_t bytes_in_current_slab_left_to_copy = 
      current_slab_end_offset - tile_offsets_[attribute_id] + 1;
  size_t bytes_to_copy = 
      std::min(bytes_in_current_slab_left_to_copy, buffer_size);
  memcpy(
      buffer_c + buffer_offset, 
      tile + tile_offsets_[attribute_id],
      bytes_to_copy);
  buffer_offset += bytes_to_copy;
  tile_offsets_[attribute_id] += bytes_to_copy;
 
  // Copy rest of the slabs
  while(buffer_offset != buffer_size &&
        tile_offsets_[attribute_id] != range_end_offset) {
    buffer_free_space = buffer_size - buffer_offset;
    bytes_to_copy = std::min(range_slab_size, buffer_free_space);

    memcpy(
        buffer_c + buffer_offset, 
        tile + tile_offsets_[attribute_id],
        bytes_to_copy);
    
    buffer_offset += bytes_to_copy;
    if(bytes_to_copy == range_slab_size) // Go to the start of the next slab
      tile_offsets_[attribute_id] += tile_slab_size;
    else // The copy stops in the middles of a slab - the loop will exit
      tile_offsets_[attribute_id] += bytes_to_copy;
  } 

  // Update overlapping tile position if necessary
  if(tile_offsets_[attribute_id] == range_end_offset) { // This tile is done
    tile_offsets_[attribute_id] = tile_sizes_[attribute_id]; 
    ++overlapping_tile.pos_;
  }

  // Check for overflow
  if(tile_offsets_[attribute_id] != range_end_offset) { // This tile is NOT done
    assert(buffer_offset == buffer_size); // Buffer full
    ++buffer_offset;  // This will indicate the overflow
  }

  // Clean up
  delete [] range_start_coords;
  delete [] range_end_coords;
}

template<class T>
void ReadState::copy_from_tile_buffer_partial_special(
    int attribute_id,
    void* buffer,
    size_t buffer_size,
    size_t& buffer_offset) {
  // Calculate free space in buffer
  size_t buffer_free_space = buffer_size - buffer_offset;
  if(buffer_free_space == 0) { // Overflow
    ++buffer_offset; // This will indicate the overflow
    return;
  }
 
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  OverlappingTile& overlapping_tile = 
      overlapping_tiles_[overlapping_tile_pos_[attribute_id]];
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
  size_t range_size = cell_num_in_range(overlap_range, dim_num) * cell_size;
  size_t end_offset = start_offset + range_size - 1;
  
  // If current tile offset is 0, set it to the beginning of the overlap range
  if(tile_offsets_[attribute_id] == 0) 
    tile_offsets_[attribute_id] = start_offset;

  // Calculate the total size to copy
  size_t bytes_left_to_copy = end_offset - tile_offsets_[attribute_id] + 1;
  size_t bytes_to_copy = std::min(bytes_left_to_copy, buffer_free_space); 

  // Copy and update current buffer and tile offsets
  memcpy(
      buffer_c + buffer_offset, 
      tile + tile_offsets_[attribute_id], 
      bytes_to_copy);
  buffer_offset += bytes_to_copy;
  tile_offsets_[attribute_id] += bytes_to_copy; 
  
  // Update overlapping tile position if necessary
  if(tile_offsets_[attribute_id] == end_offset) { // This tile is done
    tile_offsets_[attribute_id] = tile_sizes_[attribute_id]; 
    ++overlapping_tile.pos_;
  }

  // Check for overflow
  if(tile_offsets_[attribute_id] != end_offset) { // This tile is NOT done
    assert(buffer_offset == buffer_size); // Buffer full
    ++buffer_offset;  // This will indicate the overflow
  }

  // Clean up
  delete [] start_coords;
}

int ReadState::copy_tile_full(
    int attribute_id,
    void* buffer,
    size_t buffer_size,
    size_t& buffer_offset) {
  // For easy reference
  size_t buffer_free_space = buffer_size - buffer_offset;
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  assert(!array_schema->var_size(attribute_id));
  size_t tile_size = array_schema->tile_size(attribute_id);

  if(tile_size <= buffer_free_space) { // Direct copy into buffer
    return copy_tile_full_direct(
        attribute_id, 
        buffer, 
        buffer_size, 
        buffer_offset);
  } else {                             // Two-step copy
    // Get tile from the disk to the local buffer
    if(get_tile_from_disk(attribute_id) != TILEDB_RS_OK)
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

int ReadState::copy_tile_full_direct(
    int attribute_id,
    void* buffer,
    size_t buffer_size,
    size_t& buffer_offset) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  size_t tile_size = array_schema->tile_size(attribute_id);
  char* buffer_c = static_cast<char*>(buffer);

  // Sanity check
  assert(tile_size <= buffer_size - buffer_offset);
  
  // Prepare attribute file name
  std::string filename = fragment_->fragment_name() + "/" +
                         array_schema->attribute(attribute_id) +
                         TILEDB_FILE_SUFFIX;

  // Open file
  int fd = open(filename.c_str(), O_RDONLY);
  if(fd == -1) {
    PRINT_ERROR("Cannot copy full tile to buffer; File opening error");
    return TILEDB_RS_ERR;
  }

  // Find file offset where the tile begins
  int64_t pos = overlapping_tiles_[overlapping_tile_pos_[attribute_id]].pos_;
  size_t file_offset = pos * tile_size;

  // Read tile
  lseek(fd, file_offset, SEEK_SET); 
  ssize_t bytes_read = ::read(fd, buffer_c + buffer_offset, tile_size);
  if(bytes_read != tile_size) {
    PRINT_ERROR("Cannot copy full tile to buffer; File reading error");
    return TILEDB_RS_ERR;
  }
  
  // Close file
  if(close(fd)) {
    PRINT_ERROR("Cannot copy full tile to buffer; File closing error");
    return TILEDB_RS_ERR;
  }

  // Update offset and overlapping tile pos
  buffer_offset += tile_size;
  ++overlapping_tile_pos_[attribute_id];
  
  // Success
  return TILEDB_RS_OK;
}

template<class T>
int ReadState::copy_tile_partial(
    int attribute_id,
    void* buffer,
    size_t buffer_size,
    size_t& buffer_offset) {
  // Get tile from the disk into the local buffer
  if(get_tile_from_disk(attribute_id) != TILEDB_RS_OK)
    return TILEDB_RS_ERR;

  // Copy as much data as it fits from local buffer into input buffer
  if(overlapping_tiles_[attribute_id].overlap_ == PARTIAL)
    copy_from_tile_buffer_partial<T>(
        attribute_id, 
        buffer, 
        buffer_size, 
        buffer_offset);
  else if(overlapping_tiles_[attribute_id].overlap_ == PARTIAL_SPECIAL)
    copy_from_tile_buffer_partial_special<T>(
        attribute_id, 
        buffer, 
        buffer_size, 
        buffer_offset);
 

  // Success
  return TILEDB_RS_OK;
}

int ReadState::get_tile_from_disk(int attribute_id) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  size_t tile_size = array_schema->tile_size(attribute_id);

  // Allocate space for the tile if needed
  if(tiles_[attribute_id] == NULL) {
    tiles_[attribute_id] = malloc(tile_size);
    tile_sizes_[attribute_id] = tile_size;
  } 

  // Prepare attribute file name
  std::string filename = fragment_->fragment_name() + "/" +
                         array_schema->attribute(attribute_id) +
                         TILEDB_FILE_SUFFIX;

  // Open file
  int fd = open(filename.c_str(), O_RDONLY);
  if(fd == -1) {
    PRINT_ERROR("Cannot get tile from disk; File opening error");
    return TILEDB_RS_ERR;
  }

  // Find file offset where the tile begins
  int64_t pos = overlapping_tiles_[overlapping_tile_pos_[attribute_id]].pos_;
  size_t file_offset = pos * tile_size;

  // Read tile
  lseek(fd, file_offset, SEEK_SET); 
  ssize_t bytes_read = ::read(fd, tiles_[attribute_id], tile_size);
  if(bytes_read != tile_size) {
    PRINT_ERROR("Cannot get tile from disk; File reading error");
    return TILEDB_RS_ERR;
  }
  
  // Close file
  if(close(fd)) {
    PRINT_ERROR("Cannot get tile from disk; File closing error");
    return TILEDB_RS_ERR;
  }

  // Update tile offset
  tile_offsets_[attribute_id] = 0;
  
  // Success
  return TILEDB_RS_OK;

}

template<class T>
void ReadState::init_range_in_tile_domain() {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int dim_num = array_schema->dim_num();
  const T* domain = static_cast<const T*>(array_schema->domain());
  const T* tile_extents = static_cast<const T*>(array_schema->tile_extents());
  const T* range = static_cast<const T*>(fragment_->array()->range());
  const T* tile_domain = static_cast<const T*>(array_schema->tile_domain());

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
  bool overlap = true;
  for(int i=0; i<dim_num; ++i) {
    if(range_in_tile_domain[2*i] > tile_domain[2*i+1] ||
       range_in_tile_domain[2*i+1] < tile_domain[2*i]) {
      overlap = false;
      break;
    }
  }

  // Get the first overlapping tile in case of no overlap
  if(!overlap) {
    OverlappingTile overlapping_tile;
    overlapping_tile.overlap_ = NONE;
    overlapping_tile.overlap_range_ = NULL; 
    overlapping_tile.coords_ = NULL;
    overlapping_tiles_.push_back(overlapping_tile);
  }
}

bool ReadState::is_empty_attribute(int attribute_id) const {
  // Prepare attribute file name
  std::string filename = 
      fragment_->fragment_name() + "/" +
      fragment_->array()->array_schema()->attribute(attribute_id) + 
      TILEDB_FILE_SUFFIX;

  // Check if the attribute file exists
  return is_file(filename);
}

int ReadState::read_dense(
    void** buffers,
    size_t* buffer_sizes) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  const std::vector<int>& attribute_ids = fragment_->array()->attribute_ids();
  int attribute_id_num = attribute_ids.size(); 

  // Read from each attribute individually
  int i=0; 
  int rc;
  while(i<attribute_id_num) {
    if(!array_schema->var_size(attribute_ids[i])) { // FIXED CELLS
      rc = read_dense_attr(attribute_ids[i], buffers[i], buffer_sizes[i]);
      if(rc != TILEDB_RS_OK)
        break;
      ++i;
    } else {                                        // VARIABLE-SIZED CELLS
      // TODO
    }
  }

  // Return
  return rc;
}

int ReadState::read_dense_attr(
    int attribute_id,
    void* buffer,
    size_t& buffer_size) {
  // Trivial case
  if(buffer_size == 0)
    return TILEDB_RS_OK;

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  ArraySchema::Compression compression = 
      array_schema->compression(attribute_id);

  // No compression
  if(compression == ArraySchema::TILEDB_AS_CMP_NONE) {
    return read_dense_attr_cmp_none(attribute_id, buffer, buffer_size);
  } else { // GZIP
    // TODO
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
  const std::type_info* coords_type = array_schema->coords_type();

  // Invoke the proper templated function
  if(coords_type == &typeid(int)) {
    return read_dense_attr_cmp_none<int>(attribute_id, buffer, buffer_size);
  } else if(coords_type == &typeid(int64_t)) {
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
  int rc;

  // The following loop should break somewhere inside
  while(1) {
    // There are still data pending inside the local tile buffers
    if(tile_offsets_[attribute_id] < tile_sizes_[attribute_id]) 
      copy_from_tile_buffer<T>(
          attribute_id, 
          buffer, 
          buffer_size, 
          buffer_offset); 

    // If the buffer overflows, return
    if(buffer_offset > buffer_size) {
      ++buffer_size; // This will indicate the overflow
      return TILEDB_RS_OK; 
    }

    // Compute the next overlapping tile
    if(overlapping_tile_pos_[attribute_id] >= overlapping_tiles_.size())
      get_next_overlapping_tile<T>();

    // Invoke proper copy command
    if(overlapping_tiles_[overlapping_tile_pos_[attribute_id]].overlap_
       == NONE) {                 // No more tiles
      buffer_size = buffer_offset;
      return TILEDB_RS_OK; 
    } else if(overlapping_tiles_[overlapping_tile_pos_[attribute_id]].overlap_ 
              == FULL) {          // Full tile
      if(copy_tile_full(
             attribute_id, 
             buffer, 
             buffer_size, 
             buffer_offset) != TILEDB_RS_OK)
        return TILEDB_RS_ERR;
    } else {                      // Partial tile
      if(copy_tile_partial<T>(
             attribute_id, 
             buffer, 
             buffer_size, 
             buffer_offset) != TILEDB_RS_OK)
        return TILEDB_RS_ERR;
    }

    // If the buffer overflows, return
    if(buffer_offset > buffer_size) {
      ++buffer_size; // This will indicate the overflow
      return TILEDB_RS_OK; 
    }
  }
}

