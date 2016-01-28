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

#include "constants.h"
#include "utils.h"
#include "write_state.h"
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
#  define PRINT_ERROR(x) std::cerr << "[TileDB::WriteState] Error: " \
                                   << x << ".\n" 
#  define PRINT_WARNING(x) std::cerr << "[TileDB::WriteState] Warning: " \
                                     << x << ".\n"
#else
#  define PRINT_ERROR(x) do { } while(0) 
#  define PRINT_WARNING(x) do { } while(0) 
#endif

#ifdef GNU_PARALLEL
  #include <parallel/algorithm>
  #define SORT(first, last, comp) __gnu_parallel::sort((first), (last), (comp))
#else
  #include <algorithm>
  #define SORT(first, last, comp) std::sort((first), (last), (comp))
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
  size_t coords_size = array_schema->coords_size();

  // Initialize the number of cells written in the current tile
  tile_cell_num_ = 0;

  // Initialize total number of cells written
  total_cell_num_.resize(attribute_num+1);
  for(int i=0; i<attribute_num+1; ++i)
    total_cell_num_[i] = 0;

  // Initialize current tiles
  tiles_.resize(attribute_num+1);
  for(int i=0; i<attribute_num+1; ++i)
    tiles_[i] = NULL;

  // Initialize tile buffer used in compression
  tile_compressed_ = NULL;
  tile_compressed_allocated_size_ = 0;

  // Initialize current tile offsets
  tile_offsets_.resize(attribute_num+1);
  for(int i=0; i<attribute_num+1; ++i)
    tile_offsets_[i] = 0;

  // Initialize current MBR
  mbr_ = malloc(2*coords_size);

  // Initialize current bounding coordinates
  bounding_coords_ = malloc(2*coords_size);
}

WriteState::~WriteState() { 
  // Free current tiles
  for(int i=0; i<tiles_.size(); ++i) 
    if(tiles_[i] != NULL)
      free(tiles_[i]);

  // Free current compressed tile buffer
  if(tile_compressed_ != NULL)
    free(tile_compressed_);

  // Free current MBR
  free(mbr_);

  // Free current bounding coordinates
  free(bounding_coords_);
}

/* ****************************** */
/*         WRITE FUNCTIONS        */
/* ****************************** */

int WriteState::write( const void** buffers, const size_t* buffer_sizes) {
  // Create fragment directory if it does not exist
  std::string fragment_name = fragment_->fragment_name();
  if(!is_dir(fragment_name)) {
    if(create_dir(fragment_name) != TILEDB_UT_OK)
      return TILEDB_WS_ERR;
  }

  // For easy reference
  const Array* array = fragment_->array();

  // Dispatch the proper write command
  if(array->mode() == TILEDB_WRITE) {    // SORTED
    if(array->array_schema()->dense()) {            // DENSE ARRAY
      if(has_coords()) {                                // SPARSE CELLS
        return write_sparse(buffers, buffer_sizes);
      } else {                                          // DENSE ARRAY 
        if(book_keeping_->range() == NULL)
          return write_dense(buffers, buffer_sizes);          // DENSE CELLS
        else 
          return write_dense_in_range(buffers, buffer_sizes); // DENSE IN RANGE
      }
    } else {                                // SPARSE ARRAY
      return write_sparse(buffers, buffer_sizes);
    }
  } else if (array->mode() == TILEDB_WRITE_UNSORTED) { // UNSORTED
    return write_sparse_unsorted(buffers, buffer_sizes);
  } else {
    PRINT_ERROR("Cannot write to fragment; Invalid mode");
    return TILEDB_WS_ERR;
  } 
}

/* ****************************** */
/*              MISC              */
/* ****************************** */

int WriteState::finalize() {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();

  // Write last tile (applicable only to the sparse case)
  if(tile_cell_num_ != 0) { 
    if(write_last_tile() != TILEDB_RS_OK)
      return TILEDB_RS_ERR;
    tile_cell_num_ = 0;
  }

  // Success
  return TILEDB_WS_OK;
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

int WriteState::compress_and_write_tile(int attribute_id) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  unsigned char* tile = static_cast<unsigned char*>(tiles_[attribute_id]);
  size_t tile_size = tile_offsets_[attribute_id];

  // Allocate space to store the compressed tile
  if(tile_compressed_ == NULL) {
    tile_compressed_allocated_size_ = 
        tile_size + 6 + 5*(ceil(tile_size/16834.0));
    tile_compressed_ = malloc(tile_compressed_allocated_size_); 
  }

  // Expand comnpressed tile if necessary
  while(tile_size + 6 + 5*(ceil(tile_size/16834.0)) > 
        tile_compressed_allocated_size_) 
    expand_buffer(tile_compressed_, tile_compressed_allocated_size_);

  // For easy reference
  unsigned char* tile_compressed = 
      static_cast<unsigned char*>(tile_compressed_);

  // Compress tile
  ssize_t tile_compressed_size = 
      gzip(tile, tile_size, tile_compressed, tile_compressed_allocated_size_);
  if(tile_compressed_size == TILEDB_UT_ERR) 
    return TILEDB_WS_ERR;

  // Get the attribute file name
  std::string filename = fragment_->fragment_name() + "/" + 
      array_schema->attribute(attribute_id) + 
      TILEDB_FILE_SUFFIX;

  // Write segment to file
  if(write_to_file(
         filename.c_str(),
         tile_compressed_,
         tile_compressed_size) != TILEDB_WS_OK)
    return TILEDB_WS_ERR;

  // Append offset to book-keeping
  book_keeping_->append_tile_offset(attribute_id, tile_compressed_size);

  // Success
  return TILEDB_WS_OK;
}

template<class T>
void WriteState::expand_mbr(const T* coords) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();
  int dim_num = array_schema->dim_num();
  T* mbr = static_cast<T*>(mbr_);

  // Initialize MBR 
  if(tile_cell_num_ == 0) {
    for(int i=0; i<dim_num; ++i) {
      mbr[2*i] = coords[i];
      mbr[2*i+1] = coords[i];
    }
  } else { // Expand MBR 
    ::expand_mbr(mbr, coords, dim_num);
  }
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

void WriteState::sort_cell_pos(
    const void* buffer,
    size_t buffer_size,
    std::vector<int64_t>& cell_pos) const {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  const std::type_info* coords_type = array_schema->coords_type();

  // Invoke the proper templated function
  if(coords_type == &typeid(int))
    sort_cell_pos<int>(buffer, buffer_size, cell_pos);
  else if(coords_type == &typeid(int64_t))
    sort_cell_pos<int64_t>(buffer, buffer_size, cell_pos);
  else if(coords_type == &typeid(float))
    sort_cell_pos<float>(buffer, buffer_size, cell_pos);
  else if(coords_type == &typeid(double))
    sort_cell_pos<double>(buffer, buffer_size, cell_pos);
}

template<class T>
void WriteState::sort_cell_pos(
    const void* buffer,
    size_t buffer_size,
    std::vector<int64_t>& cell_pos) const {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int dim_num = array_schema->dim_num();
  size_t coords_size = array_schema->coords_size();
  int64_t buffer_cell_num = buffer_size / coords_size;
  ArraySchema::CellOrder cell_order = array_schema->cell_order();
  const T* buffer_T = static_cast<const T*>(buffer);

  // Populate cell_pos
  cell_pos.resize(buffer_cell_num);
  for(int i=0; i<buffer_cell_num; ++i)
    cell_pos[i] = i;

  // Invoke the proper sort function, based on the cell order
  if(cell_order == ArraySchema::TILEDB_AS_CO_ROW_MAJOR) {
    // Sort cell positions
    SORT(
        cell_pos.begin(), 
        cell_pos.end(), 
        SmallerRow<T>(buffer_T, dim_num)); 
  } else if(cell_order == ArraySchema::TILEDB_AS_CO_COLUMN_MAJOR) {
    // Sort cell positions
    SORT(
        cell_pos.begin(), 
        cell_pos.end(), 
        SmallerCol<T>(buffer_T, dim_num)); 
  } else if(cell_order == ArraySchema::TILEDB_AS_CO_HILBERT) {
    // Get hilbert ids
    std::vector<int64_t> ids;
    ids.resize(buffer_cell_num);
    for(int i=0; i<buffer_cell_num; ++i) 
      ids[i] = array_schema->hilbert_id<T>(&buffer_T[i * dim_num]); 

    // Sort cell positions
    SORT(
        cell_pos.begin(), 
        cell_pos.end(), 
        SmallerIdRow<T>(buffer_T, dim_num, ids)); 
  } else {
    assert(0); // The code should never reach here
  }
}

void WriteState::update_book_keeping(
    const void* buffer,
    size_t buffer_size) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  const std::type_info* coords_type = array_schema->coords_type();

  // Invoke the proper templated function
  if(coords_type == &typeid(int))
    update_book_keeping<int>(buffer, buffer_size);
  else if(coords_type == &typeid(int64_t))
    update_book_keeping<int64_t>(buffer, buffer_size);
  else if(coords_type == &typeid(float))
    update_book_keeping<float>(buffer, buffer_size);
  else if(coords_type == &typeid(double))
    update_book_keeping<double>(buffer, buffer_size);
}

template<class T>
void WriteState::update_book_keeping(
    const void* buffer,
    size_t buffer_size) {
  // Trivial case
  if(buffer_size == 0)
    return;

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();
  int dim_num = array_schema->dim_num();
  int64_t capacity = array_schema->capacity();
  size_t coords_size = array_schema->coords_size();
  int64_t buffer_cell_num = buffer_size / coords_size;
  const T* buffer_T = static_cast<const T*>(buffer); 

  // Update bounding coordinates and MBRs
  for(int64_t i = 0; i<buffer_cell_num; ++i) {
    // Set first bounding coordinates
    if(tile_cell_num_ == 0) 
      memcpy(bounding_coords_, &buffer_T[i*dim_num], coords_size);

    // Set second bounding coordinates
    memcpy(
        static_cast<char*>(bounding_coords_) + coords_size,
        &buffer_T[i*dim_num], 
        coords_size);

    // Expand MBR
    expand_mbr(&buffer_T[i*dim_num]);

    // Advance a cell
    ++tile_cell_num_;

    // Send MBR and bounding coordinates to book-keeping
    if(tile_cell_num_ == capacity) {
      book_keeping_->append_mbr(mbr_);
      book_keeping_->append_bounding_coords(bounding_coords_);
      tile_cell_num_ = 0; 
    }
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
    if(!array_schema->var_size(attribute_ids[i])) { // FIXED CELLS
      rc = write_dense_attr(attribute_ids[i], buffers[i], buffer_sizes[i]);
      if(rc != TILEDB_WS_OK)
        break;
      ++i;
    } else {                                        // VARIABLE-SIZED CELLS
      rc = write_dense_attr_var(
          attribute_ids[i], 
          buffers[i], 
          buffer_sizes[i],
          buffers[i+1], 
          buffer_sizes[i+1]);
      if(rc != TILEDB_WS_OK)
        break;
      i+=2;
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

  // Write buffer to file 
  std::string filename = fragment_->fragment_name() + "/" + 
      array_schema->attribute(attribute_id) + 
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
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  size_t cell_size = array_schema->cell_size(attribute_id); 
  size_t tile_size = array_schema->tile_size(attribute_id); 

  // Initialize local tile buffer if needed
  if(tiles_[attribute_id] == NULL)
    tiles_[attribute_id] = malloc(tile_size);

  // For easy reference
  char* tile = static_cast<char*>(tiles_[attribute_id]);
  size_t& tile_offset = tile_offsets_[attribute_id];
  const char* buffer_c = static_cast<const char*>(buffer);
  size_t buffer_offset = 0;

  // Update total number of cells
  total_cell_num_[attribute_id] += buffer_size / cell_size;

  // Bytes to fill the potentially partially buffered tile
  size_t bytes_to_fill = tile_size - tile_offset;

  // The buffer has enough cells to fill at least one tile
  if(bytes_to_fill <= buffer_size) {
    // Fill up current tile, and append offset to book-keeping
    memcpy(
        tile + tile_offset, 
        buffer_c + buffer_offset,
        bytes_to_fill); 
    buffer_offset += bytes_to_fill;
    tile_offset += bytes_to_fill;

    // Compress current tile and write it to disk
    if(compress_and_write_tile(attribute_id) != TILEDB_WS_OK)
      return TILEDB_WS_ERR;

    // Update local tile buffer offset
    tile_offset = 0;
  }
      
  // Continue to fill and compress entire tiles
  while(buffer_offset + tile_size <= buffer_size) {
    // Prepare tile
    memcpy(tile, buffer_c + buffer_offset, tile_size); 
    buffer_offset += tile_size;
    tile_offset += tile_size;

    // Compress current tile, append to segment.
    if(compress_and_write_tile(attribute_id) != TILEDB_WS_OK)
      return TILEDB_WS_ERR;

    // Update local tile buffer offset
    tile_offset = 0;
  }

  // Partially fill the (new) current tile
  bytes_to_fill = buffer_size - buffer_offset;
  if(bytes_to_fill != 0) {
    memcpy(tile, buffer_c + buffer_offset, bytes_to_fill); 
    buffer_offset += bytes_to_fill;
    assert(buffer_offset == buffer_size);
    tile_offset += bytes_to_fill;
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

int WriteState::write_dense_attr_var_cmp_none(
    int attribute_id,
    const void* buffer_val,
    size_t buffer_val_size,
    const void* buffer_val_num,
    size_t buffer_val_num_size) {
  // TODO
}

int WriteState::write_dense_attr_var_cmp_gzip(
    int attribute_id,
    const void* buffer_val,
    size_t buffer_val_size,
    const void* buffer_val_num,
    size_t buffer_val_num_size) {
  // TODO
}

int WriteState::write_dense_in_range(
    const void** buffers,
    const size_t* buffer_sizes) {
  // TODO
}

int WriteState::write_sparse(
    const void** buffers,
    const size_t* buffer_sizes) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();
  const std::vector<int>& attribute_ids = fragment_->array()->attribute_ids();
  int attribute_id_num = attribute_ids.size(); 

  // Write each attribute individually
  int i=0; 
  int rc;
  while(i<attribute_id_num) {
    if(!array_schema->var_size(attribute_ids[i])) { // FIXED CELLS
      rc = write_sparse_attr(attribute_ids[i], buffers[i], buffer_sizes[i]);
      if(rc != TILEDB_WS_OK)
        break;
      ++i;
    } else {                                        // VARIABLE-SIZED CELLS
      // TODO
    }
  }

  return rc;
}

int WriteState::write_sparse_attr(
    int attribute_id,
    const void* buffer,
    size_t buffer_size) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  ArraySchema::Compression compression = 
      array_schema->compression(attribute_id);

  // No compression
  if(compression == ArraySchema::TILEDB_AS_CMP_NONE)
    return write_sparse_attr_cmp_none(attribute_id, buffer, buffer_size);
  else // GZIP
    return write_sparse_attr_cmp_gzip(attribute_id, buffer, buffer_size);
}

int WriteState::write_sparse_attr_cmp_none(
    int attribute_id,
    const void* buffer,
    size_t buffer_size) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();

  // Update book-keeping
  if(attribute_id == attribute_num)
    update_book_keeping(buffer, buffer_size);

  // Write buffer to file 
  std::string filename = fragment_->fragment_name() + "/" + 
      array_schema->attribute(attribute_id) + 
      TILEDB_FILE_SUFFIX;
  int rc = write_to_file(filename.c_str(), buffer, buffer_size);

  // Return
  if(rc == TILEDB_UT_OK)
    return TILEDB_WS_OK;
  else
    return TILEDB_WS_ERR;
}

int WriteState::write_sparse_attr_cmp_gzip(
    int attribute_id,
    const void* buffer,
    size_t buffer_size) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();
  size_t cell_size = array_schema->cell_size(attribute_id); 
  size_t tile_size = array_schema->tile_size(attribute_id); 

  // Update book-keeping
  if(attribute_id == attribute_num)
    update_book_keeping(buffer, buffer_size);

  // Initialize local tile buffer if needed
  if(tiles_[attribute_id] == NULL)
    tiles_[attribute_id] = malloc(tile_size);

  // For easy reference
  char* tile = static_cast<char*>(tiles_[attribute_id]);
  size_t& tile_offset = tile_offsets_[attribute_id];
  const char* buffer_c = static_cast<const char*>(buffer);
  size_t buffer_offset = 0;

  // Update total number of cells
  total_cell_num_[attribute_id] += buffer_size / cell_size;

  // Bytes to fill the potentially partially buffered tile
  size_t bytes_to_fill = tile_size - tile_offset;

  // The buffer has enough cells to fill at least one tile
  if(bytes_to_fill <= buffer_size) {
    // Fill up current tile, and append offset to book-keeping
    memcpy(
        tile + tile_offset, 
        buffer_c + buffer_offset,
        bytes_to_fill); 
    buffer_offset += bytes_to_fill;
    tile_offset += bytes_to_fill;

    // Compress current tile and write it to disk
    if(compress_and_write_tile(attribute_id) != TILEDB_WS_OK)
      return TILEDB_WS_ERR;

    // Update local tile buffer offset
    tile_offset = 0;
  }
      
  // Continue to fill and compress entire tiles
  while(buffer_offset + tile_size <= buffer_size) {
    // Prepare tile
    memcpy(tile, buffer_c + buffer_offset, tile_size); 
    buffer_offset += tile_size;
    tile_offset += tile_size;

    // Compress current tile, append to segment.
    if(compress_and_write_tile(attribute_id) != TILEDB_WS_OK)
      return TILEDB_WS_ERR;

    // Update local tile buffer offset
    tile_offset = 0;
  }

  // Partially fill the (new) current tile
  bytes_to_fill = buffer_size - buffer_offset;
  if(bytes_to_fill != 0) {
    memcpy(tile, buffer_c + buffer_offset, bytes_to_fill); 
    buffer_offset += bytes_to_fill;
    assert(buffer_offset == buffer_size);
    tile_offset += bytes_to_fill;
  }

  return TILEDB_WS_OK;
}

int WriteState::write_sparse_unsorted(
    const void** buffers,
    const size_t* buffer_sizes) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();
  const std::vector<int>& attribute_ids = fragment_->array()->attribute_ids();
  int attribute_id_num = attribute_ids.size(); 

  // Find the coordinates buffer
  int coords_id = -1;
  for(int i=0; i<attribute_id_num; ++i) { 
    if(attribute_ids[i] == attribute_num) {
      coords_id = i;
      break;
    }
  }
  
  // Coordinates are missing
  if(coords_id == -1) {
    PRINT_ERROR("Cannot write sparse unsorted; Coordinates missing");
    return TILEDB_WS_ERR;
  }

  // Sort cell positions
  std::vector<int64_t> cell_pos;
  sort_cell_pos(buffers[coords_id], buffer_sizes[coords_id], cell_pos);

  // Write each attribute individually
  int i=0; 
  int rc;
  while(i<attribute_id_num) {
    if(!array_schema->var_size(attribute_ids[i])) { // FIXED CELLS
      rc = write_sparse_unsorted_attr(
               attribute_ids[i], 
               buffers[i], 
               buffer_sizes[i],
               cell_pos);
      if(rc != TILEDB_WS_OK)
        break;
      ++i;
    } else {                                        // VARIABLE-SIZED CELLS
      // TODO
    }
  }

  return rc;
}

int WriteState::write_sparse_unsorted_attr(
    int attribute_id,
    const void* buffer,
    size_t buffer_size,
    const std::vector<int64_t>& cell_pos) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  ArraySchema::Compression compression = 
      array_schema->compression(attribute_id);

  // No compression
  if(compression == ArraySchema::TILEDB_AS_CMP_NONE)
    return write_sparse_unsorted_attr_cmp_none(
               attribute_id, 
               buffer, 
               buffer_size,
               cell_pos);
  else // GZIP
    return write_sparse_unsorted_attr_cmp_gzip(
               attribute_id,
               buffer, 
               buffer_size,
               cell_pos);
}

int WriteState::write_sparse_unsorted_attr_cmp_none(
    int attribute_id,
    const void* buffer,
    size_t buffer_size,
    const std::vector<int64_t>& cell_pos) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  size_t cell_size = array_schema->cell_size(attribute_id); 
  const char* buffer_c = static_cast<const char*>(buffer); 

  // Check number of cells in buffer
  int64_t buffer_cell_num = buffer_size / cell_size;
  if(buffer_cell_num != cell_pos.size()) {
    PRINT_ERROR(std::string("Cannot write sparse unsorted; Invalid number of "
                "cells in attribute '") + 
                array_schema->attribute(attribute_id) + "'");
    return TILEDB_WS_ERR;
  }

  // Allocate a local buffer to hold the sorted cells
  char* sorted_buffer = new char[TILEDB_SORTED_BUFFER_SIZE]; 
  size_t sorted_buffer_size = 0;

  // Sort and write attribute values in batches
  for(int64_t i=0; i<buffer_cell_num; ++i) {
    // Write batch
    if(sorted_buffer_size + cell_size > TILEDB_SORTED_BUFFER_SIZE) {
      if(write_sparse_attr_cmp_none(
             attribute_id,
             sorted_buffer, 
             sorted_buffer_size) != TILEDB_WS_OK) {
        delete [] sorted_buffer;
        return TILEDB_WS_ERR;
      } else {
        sorted_buffer_size = 0;
      }
    }

    // Keep on copying the cells in the sorted order in the sorted buffer
    memcpy(
        sorted_buffer + sorted_buffer_size, 
        buffer_c + cell_pos[i] * cell_size, 
        cell_size);
    sorted_buffer_size += cell_size;
  }

  // Write final batch
  if(sorted_buffer_size != 0) {
    if(write_sparse_attr_cmp_none(
           attribute_id, 
           sorted_buffer, 
           sorted_buffer_size) != TILEDB_WS_OK) {
      delete [] sorted_buffer;
      return TILEDB_WS_ERR;
    }
  }

  // Clean up
  delete [] sorted_buffer;

  // Success
  return TILEDB_WS_OK;
}

int WriteState::write_sparse_unsorted_attr_cmp_gzip(
    int attribute_id,
    const void* buffer,
    size_t buffer_size,
    const std::vector<int64_t>& cell_pos) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  size_t cell_size = array_schema->cell_size(attribute_id); 
  const char* buffer_c = static_cast<const char*>(buffer); 

  // Check number of cells in buffer
  int64_t buffer_cell_num = buffer_size / cell_size;
  if(buffer_cell_num != cell_pos.size()) {
    PRINT_ERROR(std::string("Cannot write sparse unsorted; Invalid number of "
                "cells in attribute '") + 
                array_schema->attribute(attribute_id) + "'");
    return TILEDB_WS_ERR;
  }

  // Allocate a local buffer to hold the sorted cells
  char* sorted_buffer = new char[TILEDB_SORTED_BUFFER_SIZE]; 
  size_t sorted_buffer_size = 0;

  // Sort and write attribute values in batches
  for(int64_t i=0; i<buffer_cell_num; ++i) {
    // Write batch
    if(sorted_buffer_size + cell_size > TILEDB_SORTED_BUFFER_SIZE) {
      if(write_sparse_attr_cmp_gzip(
             attribute_id,
             sorted_buffer, 
             sorted_buffer_size) != TILEDB_WS_OK) {
        delete [] sorted_buffer;
        return TILEDB_WS_ERR;
      } else {
        sorted_buffer_size = 0;
      }
    }

    // Keep on copying the cells in the sorted order in the sorted buffer
    memcpy(
        sorted_buffer + sorted_buffer_size, 
        buffer_c + cell_pos[i] * cell_size, 
        cell_size);
    sorted_buffer_size += cell_size;
  }

  // Write final batch
  if(sorted_buffer_size != 0) {
    if(write_sparse_attr_cmp_gzip(
           attribute_id, 
           sorted_buffer, 
           sorted_buffer_size) != TILEDB_WS_OK) {
      delete [] sorted_buffer;
      return TILEDB_WS_ERR;
    }
  }

  // Clean up
  delete [] sorted_buffer;

  // Success
  return TILEDB_WS_OK;
} 

int WriteState::write_last_tile() {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();

  // Send last MBR, bounding coordinates and tile cell number to book-keeping
  book_keeping_->append_mbr(mbr_);
  book_keeping_->append_bounding_coords(bounding_coords_);
  book_keeping_->set_last_tile_cell_num(tile_cell_num_);

  // Flush the last tile for each compressed attribute (it is still in main
  // memory
  for(int i=0; i<attribute_num+1; ++i) {
    if(array_schema->compression(i) == ArraySchema::TILEDB_AS_CMP_GZIP) 
      if(compress_and_write_tile(i) != TILEDB_WS_OK)
        return TILEDB_WS_ERR;
  } 

  // Success
  return TILEDB_WS_OK;
}
