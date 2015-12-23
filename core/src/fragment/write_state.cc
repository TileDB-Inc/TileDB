/**
 * @file   write_state.cc
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
 * This file implements the WriteState class.
 */

#include "utils.h"
#include "write_state.h"
#include <cassert>
#include <cmath>
#include <cstring>
#include <iostream>

/* ****************************** */
/*             MACROS             */
/* ****************************** */

#if VERBOSE == 1
#  define PRINT_ERROR(x) std::cerr << "[TileDB] Error: " << x << ".\n" 
#  define PRINT_WARNING(x) std::cerr << "[TileDB] Warning: " \
                                     << x << ".\n"
#elif VERBOSE == 2
#  define PRINT_ERROR(x) std::cerr << "[TileDB::WriteState] Error: " \
                                   << x << ".\n" 
#  define PRINT_WARNING(x) std::cerr << "[TileDB::WriteState] Warning: " \
                                     << x << ".\n"
#else
#  define PRINT_ERROR(x) do { } while(0) 
#  define PRINT_WARNING(x) do { } while(0) 
#endif

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

WriteState::WriteState(
    const Fragment* fragment, 
    BookKeeping* book_keeping)
    : fragment_(fragment),
      book_keeping_(book_keeping) {
  // For easy reference
  const ArraySchema* array_schema = fragment->array()->array_schema();
  int attribute_num = array_schema->attribute_num();

  // Initialize the number of cells written in the current tile
  current_tile_cell_num_.resize(attribute_num+1);
  for(int i=0; i<attribute_num+1; ++i)
    current_tile_cell_num_[i] = 0;

  // Initialize total number of cells written
  total_cell_num_.resize(attribute_num+1);
  for(int i=0; i<attribute_num+1; ++i)
    total_cell_num_[i] = 0;

  // Initialize segments
  segments_.resize(attribute_num+1);
  for(int i=0; i<attribute_num+1; ++i)
    segments_[i] = NULL;

  // Initialize current tiles
  current_tiles_.resize(attribute_num+1);
  for(int i=0; i<attribute_num+1; ++i)
    current_tiles_[i] = NULL;

  // Initialize current tiles used in compression
  current_tiles_compressed_.resize(attribute_num+1);
  for(int i=0; i<attribute_num+1; ++i)
    current_tiles_compressed_[i] = NULL;

  // Initialize current tile offsets
  current_tile_offsets_.resize(attribute_num+1);
  for(int i=0; i<attribute_num+1; ++i)
    current_tile_offsets_[i] = 0;
}

WriteState::~WriteState() { 
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();

  // Free current tiles, current compressed tiles and segments
  for(int i=0; i<attribute_num+1; ++i) {
    if(current_tiles_[i] != NULL)
      free(current_tiles_[i]);
    if(current_tiles_compressed_[i] != NULL)
      free(current_tiles_compressed_[i]);
    if(segments_[i] != NULL)
      free(segments_[i]);
  }
}

/* ****************************** */
/*         WRITE FUNCTIONS        */
/* ****************************** */

void WriteState::flush() {
  // TODO: Write empty cells if needed
  // TODO: Write rest of the segments to disk
}

int WriteState::write(
    const void** buffers,
    const size_t* buffer_sizes) {
  // For easy reference
  const Array* array = fragment_->array();

  // Dispatch the proper write command
  if(array->mode() == TILEDB_WRITE) {    // SORTED
    if(array->array_schema()->dense()) {            // DENSE
      if(has_coords())                                // SPARSE
        return write_sparse(buffers, buffer_sizes);
      else {                                          // DENSE 
        if(array->range() == NULL)
          return write_dense(buffers, buffer_sizes);          // DENSE
        else 
          return write_dense_in_range(buffers, buffer_sizes); // DENSE IN RANGE
      }
    } else {                                // SPARSE
      return write_sparse(buffers, buffer_sizes);
    }
  } else {                       // UNSORTED
    return write_sparse_unsorted(buffers, buffer_sizes);
  } 
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

ssize_t WriteState::compress_tile_into_segment(
    int attribute_id,
    void* tile,
    size_t tile_size) {
  // Compress tile
  ssize_t current_tile_compressed_size = 
      gzip(
          static_cast<unsigned char*>(tile), 
          tile_size, 
          static_cast<unsigned char*>(current_tiles_compressed_[attribute_id]), 
          TILEDB_Z_SEGMENT_SIZE);
  if(current_tile_compressed_size == -1) 
    return TILEDB_WS_ERR;

  // Write segment to disk if full
  if(segment_offsets_[attribute_id] + current_tile_compressed_size > 
     TILEDB_SEGMENT_SIZE) 
    if(write_segment_to_file(attribute_id))
      return TILEDB_WS_ERR;

  // Copy compressed tile to segment
  memcpy((char*) segments_[attribute_id] + segment_offsets_[attribute_id],
         current_tiles_compressed_[attribute_id],
         current_tile_compressed_size);
  segment_offsets_[attribute_id] += current_tile_compressed_size;

  return TILEDB_WS_OK;
}

bool WriteState::has_coords() const {
  const Array* array = fragment_->array();
  const std::vector<int>& attribute_ids = array->attribute_ids();
  int id_num = attribute_ids.size();
  int attribute_num = array->array_schema()->attribute_num();
  for(int i=0; i<id_num; ++i) {
    if(attribute_ids[i] == attribute_num) 
      return true;
  }

  return false;
}

void WriteState::init_current_tile(int attribute_id) {
  if(current_tiles_[attribute_id] == NULL)
    current_tiles_[attribute_id] = malloc(TILEDB_SEGMENT_SIZE);
}

void WriteState::init_current_tile_compressed(int attribute_id) {
  // We take into account also the zlib's maximum expansion factor
  if(current_tiles_compressed_[attribute_id] == NULL)
    current_tiles_compressed_[attribute_id] = malloc(TILEDB_Z_SEGMENT_SIZE);
}

void WriteState::init_segment(int attribute_id) {
  if(segments_[attribute_id] == NULL) {
    segments_[attribute_id] = malloc(TILEDB_SEGMENT_SIZE);
    segment_offsets_[attribute_id] = 0;
  }
}

int WriteState::write_dense(
    const void** buffers,
    const size_t* buffer_sizes) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  const std::vector<int>& attribute_ids = fragment_->array()->attribute_ids();
  int attribute_id_num = attribute_ids.size(); 

  // Write each attribute individually
  int i=0; 
  int rc;
  while(i<attribute_id_num) {
    // Fixed-sized cells
    if(!array_schema->var_size(attribute_ids[i])) { 
      rc = write_dense_attr(attribute_ids[i], buffers[i], buffer_sizes[i]);
      ++i;
      if(rc != TILEDB_WS_OK)
        break;
    } else { // Variable-sized cells
      rc = write_dense_attr_var(
          attribute_ids[i], 
          buffers[i], 
          buffer_sizes[i],
          buffers[i+1], 
          buffer_sizes[i+1]);
      i+=2;
      if(rc != TILEDB_WS_OK)
        break;
    }
  }

  return rc;
}

int WriteState::write_dense_attr(
    int attribute_id,
    const void* buffer,
    size_t buffer_size) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  ArraySchema::Compression compression = 
      array_schema->compression(attribute_id);

  // No compression
  if(compression == ArraySchema::TILEDB_AS_CMP_NONE)
    return write_dense_attr_cmp_none(attribute_id, buffer, buffer_size);
  else // GZIP
    return write_dense_attr_cmp_gzip(attribute_id, buffer, buffer_size);
}

int WriteState::write_dense_attr_cmp_none(
    int attribute_id,
    const void* buffer,
    size_t buffer_size) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int64_t cell_num_per_tile = array_schema->cell_num_per_tile();
  size_t cell_size = array_schema->cell_size(attribute_id);
  size_t tile_size = cell_num_per_tile * cell_size; 
  int64_t buffer_cell_num = buffer_size / cell_size;

  // Update total number of cells
  total_cell_num_[attribute_id] += buffer_cell_num;

  // Calculate number of cells needed to fill the current tile
  int64_t current_tile_cells_to_fill =
      (cell_num_per_tile - current_tile_cell_num_[attribute_id]);

  // The buffer has enough cells to fill at least one tile
  if(buffer_cell_num >= current_tile_cells_to_fill) {
    // Fill up current tile, and append offset to book-keeping
    buffer_cell_num -= current_tile_cells_to_fill;
    book_keeping_->append_tile_offset(attribute_id, tile_size);
    current_tile_cell_num_[attribute_id] = 0;
  }

  // Continue filling up entire tiles
  while(buffer_cell_num >= cell_num_per_tile) {
    book_keeping_->append_tile_offset(attribute_id, tile_size);
    buffer_cell_num -= cell_num_per_tile;
  }

  // Partially fill the (new) current tile
  current_tile_cell_num_[attribute_id] += buffer_cell_num;

  // Write buffer to file 
  std::string filename = 
      fragment_->array()->array_schema()->attribute(attribute_id) + 
      TILEDB_FILE_SUFFIX;
  int rc = write_to_file(filename.c_str(), buffer, buffer_size);

  // Return
  if(rc == TILEDB_UT_OK)
    return TILEDB_WS_OK;
  else
    return TILEDB_WS_ERR;
}

int WriteState::write_dense_attr_cmp_gzip(
    int attribute_id,
    const void* buffer,
    size_t buffer_size) {
  // Initialize segments and current tiles
  init_current_tile(attribute_id);
  init_current_tile_compressed(attribute_id);
  init_segment(attribute_id);

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int64_t cell_num_per_tile = array_schema->cell_num_per_tile();
  size_t cell_size = array_schema->cell_size(attribute_id);
  size_t tile_size = cell_num_per_tile * cell_size; 
  char* current_tile = static_cast<char*>(current_tiles_[attribute_id]);
  int64_t& current_tile_cell_num = current_tile_cell_num_[attribute_id];
  size_t& current_tile_offset = current_tile_offsets_[attribute_id];
  size_t buffer_offset = 0;
  int64_t buffer_cell_num = buffer_size / cell_size;
  ssize_t compressed_tile_size;
  size_t current_tile_size_to_fill;
  int rc;

  // Update total number of cells
  total_cell_num_[attribute_id] += buffer_cell_num;

  // Calculate number of cells needed to fill the current tile
  int64_t current_tile_cells_to_fill =
      (cell_num_per_tile - current_tile_cell_num_[attribute_id]);

  // The buffer has enough cells to fill at least one tile
  if(buffer_cell_num >= current_tile_cells_to_fill) {
    // Fill up current tile, and append offset to book-keeping
    current_tile_size_to_fill = current_tile_cells_to_fill * cell_size;
    memcpy(
        current_tile + current_tile_offset, 
        static_cast<const char*>(buffer) + buffer_offset,
        current_tile_size_to_fill); 
    buffer_offset += current_tile_size_to_fill;
    buffer_cell_num -= current_tile_cells_to_fill;
    current_tile_offset = 0;
    current_tile_cell_num = 0;

    // Compress current tile, append to segment.
    compressed_tile_size = 
          compress_tile_into_segment(
              attribute_id,
              current_tile, 
              tile_size);
    if(compressed_tile_size == TILEDB_WS_ERR)
      return TILEDB_WS_ERR;

    // Append offset
    book_keeping_->append_tile_offset(attribute_id, compressed_tile_size);
  }
      
  // Continue to fill and compress entire tiles
  while(buffer_cell_num >= cell_num_per_tile) {
    // Prepare tle
    memcpy(
        current_tile, 
        static_cast<const char*>(buffer) + buffer_offset,
        tile_size); 
    buffer_offset += tile_size;
    buffer_cell_num -= cell_num_per_tile;

    // Compress the tile and write it to segment
    compressed_tile_size = 
        compress_tile_into_segment(
            attribute_id,
            current_tile, 
            tile_size);
    if(compressed_tile_size == TILEDB_WS_ERR)
      return TILEDB_WS_ERR;
      
    // Append offset
    book_keeping_->append_tile_offset(attribute_id, compressed_tile_size);
  }

  // Partially fill the (new) current tile
  current_tile_size_to_fill = buffer_cell_num * cell_size;
  if(current_tile_size_to_fill != 0) {
    memcpy(
        current_tile + current_tile_offset, 
        static_cast<const char*>(buffer) + buffer_offset,
        current_tile_size_to_fill); 
    buffer_offset += current_tile_size_to_fill;
    assert(buffer_offset == buffer_size);
    current_tile_offset += current_tile_size_to_fill;
    current_tile_cell_num = buffer_cell_num;
  }

  return TILEDB_WS_OK;
}

int WriteState::write_dense_attr_var(
    int attribute_id,
    const void* buffer_val,
    size_t buffer_val_size,
    const void* buffer_val_num,
    size_t buffer_val_num_size) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  ArraySchema::Compression compression = 
      array_schema->compression(attribute_id);

  // No compression
  if(compression == ArraySchema::TILEDB_AS_CMP_NONE)
    return write_dense_attr_var_cmp_none(
               attribute_id, 
               buffer_val,  
               buffer_val_size,
               buffer_val_num,  
               buffer_val_num_size);
  else // GZIP
    return write_dense_attr_var_cmp_gzip(
               attribute_id, 
               buffer_val,  
               buffer_val_size,
               buffer_val_num,  
               buffer_val_num_size);
}

int WriteState::write_dense_in_range(
    const void** buffers,
    const size_t* buffer_sizes) {
  // TODO
}

int WriteState::write_sparse(
    const void** buffers,
    const size_t* buffer_sizes) {
  // TODO
}

int WriteState::write_sparse_unsorted(
    const void** buffers,
    const size_t* buffer_sizes) {
  // TODO
}

int WriteState::write_segment_to_file(int attribute_id) {
  // Get the attribute file name
  std::string filename = 
      fragment_->array()->array_schema()->attribute(attribute_id) + 
      TILEDB_FILE_SUFFIX;

  // Write segment to file
  int rc = write_to_file(
         filename.c_str(),
         segments_[attribute_id],
         segment_offsets_[attribute_id]);

  // Reset segment offset
  segment_offsets_[attribute_id] = 0;

  // Return
  if(rc == TILEDB_UT_OK) 
    return TILEDB_WS_OK;
  else
    return TILEDB_WS_ERR;
}
