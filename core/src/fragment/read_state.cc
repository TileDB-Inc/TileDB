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
  bool dense = array_schema->dense();

  // Initializations
  cell_pos_range_pos_.resize(attribute_num+1);
  range_in_tile_domain_ = NULL;
  overlapping_tile_pos_.resize(attribute_num+1); 
  tiles_.resize(attribute_num+1);
  tiles_var_.resize(attribute_num);
  tile_offsets_.resize(attribute_num+1);
  tile_var_offsets_.resize(attribute_num);
  tile_sizes_.resize(attribute_num+1);
  tile_var_sizes_.resize(attribute_num);
  coords_tile_fetched_ = false;
  tile_compressed_ = NULL;
  tile_compressed_allocated_size_ = 0;
  tiles_var_allocated_size_.resize(attribute_num);

  for(int i=0; i<attribute_num+1; ++i) {
    overlapping_tile_pos_[i] = 0;
    cell_pos_range_pos_[i] = 0;
    tiles_[i] = NULL;
    tiles_var_[i] = NULL;
    tile_offsets_[i] = 0;
    tile_sizes_[i] = 0;
  }

  for(int i=0; i<attribute_num; ++i) {
    tile_var_offsets_[i] = 0;
    tile_var_sizes_[i] = 0;
    tiles_var_allocated_size_[i] = 0;
  }

  // More initializations
  if(dense)         // DENSE
    init_range_in_tile_domain();
  else              // SPARSE
    init_tile_search_range();
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

  for(int i=0; i<tiles_.size(); ++i) {
    if(tiles_[i] != NULL)
      free(tiles_[i]);
  }

  for(int i=0; i<tiles_var_.size(); ++i) {
    if(tiles_var_[i] != NULL)
      free(tiles_var_[i]);
  }

  if(range_in_tile_domain_ != NULL)
    free(range_in_tile_domain_);

  if(tile_compressed_ != NULL)
    free(tile_compressed_);
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
      return read_sparse(buffers, buffer_sizes);       
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
void ReadState::get_next_overlapping_tile_dense() {
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
    const T* previous_coords = 
        static_cast<const T*>(overlapping_tiles_.back().coords_);
    for(int i=0; i<dim_num; ++i)
      coords[i] = previous_coords[i]; 
    array_schema->get_next_tile_coords<T>(range_in_tile_domain, coords);
  }

  // Get the position
  overlapping_tile.pos_ = array_schema->get_tile_pos<T>(coords);

  // Get tile overlap
  int overlap;
  array_schema->compute_tile_range_overlap<T>(
      range,           // query range
      coords,          // tile coordinates in tile domain
      overlap_range,   // returned overlap in terms of cell coordinates in tile
      overlap);        // returned type of overlap

  if(overlap == 0) {
    overlapping_tile.overlap_ = NONE;
  } else if(overlap == 1) {
    overlapping_tile.overlap_ = FULL;
  } else if(overlap == 2) {
    overlapping_tile.overlap_ = PARTIAL_NON_CONTIG;
  } else if(overlap == 3) {
    overlapping_tile.overlap_ = PARTIAL_CONTIG;
  }

  // Set the number of cells in the tile
  overlapping_tile.cell_num_ = array_schema->cell_num_per_tile();
 
  // Store the new tile
  overlapping_tiles_.push_back(overlapping_tile); 

  // Clean up processed overlapping tiles
  clean_up_processed_overlapping_tiles();
}

template<class T>
void ReadState::get_next_overlapping_tile_sparse() {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int dim_num = array_schema->dim_num();
  int attribute_num = array_schema->attribute_num();
  size_t coords_size = array_schema->coords_size();
  const T* range = static_cast<const T*>(fragment_->array()->range());
  int64_t tile_num = book_keeping_->mbrs().size();
  size_t full_tile_size = array_schema->capacity() * coords_size;

  // Reset flag
  coords_tile_fetched_ = false;

  // The next overlapping tile
  OverlappingTile overlapping_tile;
  overlapping_tile.coords_ = NULL; // Applicable only to dense arrays
  overlapping_tile.overlap_range_ = malloc(2*coords_size);

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

  // In case of partial overlap, find the ranges of qualifying cells
  if(overlapping_tile.overlap_ == PARTIAL_CONTIG ||
     overlapping_tile.overlap_ == PARTIAL_NON_CONTIG)
    compute_cell_pos_ranges<T>();

  // Clean up processed overlapping tiles
  clean_up_processed_overlapping_tiles();
}

void ReadState::clean_up_processed_overlapping_tiles() {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();

  // Find the minimum overlapping tile position across all attributes
  int min_pos = overlapping_tile_pos_[0];
  for(int i=1; i<attribute_num+1; ++i)
    if(overlapping_tile_pos_[i] < min_pos)
      min_pos = overlapping_tile_pos_[i];

  // Clean up processed overlapping tiles
  if(min_pos != 0) {
    // Clear memory
    for(int i=0; i<min_pos; ++i) {
      if(overlapping_tiles_[i].overlap_range_ != NULL)
        free(overlapping_tiles_[i].overlap_range_);
      if(overlapping_tiles_[i].coords_ != NULL)
        free(overlapping_tiles_[i].coords_);
    }

    // Remove overlapping tile elements from the vector
    std::vector<OverlappingTile>::iterator it_first = 
         overlapping_tiles_.begin(); 
    std::vector<OverlappingTile>::iterator it_last = it_first + min_pos; 
    overlapping_tiles_.erase(it_first, it_last); 

    // Update the positions
    for(int i=0; i<attribute_num+1; ++i)
      overlapping_tile_pos_[i] -= min_pos;
  }
}

int ReadState::compute_tile_var_size(
    int attribute_id, 
    int tile_pos, 
    size_t& tile_size) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();

  // =========== Compression case =========== // 
  if(array_schema->compression(attribute_id) ==  
     ArraySchema::TILEDB_AS_CMP_GZIP) {
    tile_size = book_keeping_->tile_var_sizes()[attribute_id][tile_pos]; 
    return TILEDB_RS_OK;
  }

  // ========= Non-Compression case ========= // 

  // For easy reference
  size_t cell_size = sizeof(size_t);
  size_t full_tile_size = array_schema->cell_num_per_tile() * cell_size;
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
  size_t file_offset = tile_pos * full_tile_size;

  // Read start variable tile offset
  size_t start_tile_var_offset;
  lseek(fd, file_offset, SEEK_SET); 

  size_t bytes_read = ::read(fd, &start_tile_var_offset, cell_size);
  if(bytes_read != cell_size) {
    PRINT_ERROR("Cannot compute variable tile size; File reading error");
    return TILEDB_RS_ERR;
  }

  // Compute the end of the variable tile
  size_t end_tile_var_offset;
  if(tile_pos != tile_num - 1) { // Not the last tile
    lseek(fd, file_offset + full_tile_size, SEEK_SET); 
    bytes_read = ::read(fd, &end_tile_var_offset, cell_size);
    if(bytes_read != cell_size) {
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
  tile_size = end_tile_var_offset - start_tile_var_offset;

  // Success
  return TILEDB_RS_OK;
}

void ReadState::compute_bytes_to_copy(
    int attribute_id,
    int64_t start_cell_pos,
    int64_t end_cell_pos,
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
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int64_t cell_num = 
      overlapping_tiles_[overlapping_tile_pos_[attribute_id]].cell_num_; 
  const size_t* tile = static_cast<const size_t*>(tiles_[attribute_id]);

  // Calculate bytes to copy from the variable tile
  if(end_cell_pos + 1 < cell_num) 
    bytes_var_to_copy = tile[end_cell_pos + 1] - tile[start_cell_pos];
  else 
    bytes_var_to_copy = tile_var_sizes_[attribute_id] - tile[start_cell_pos];

  // If bytes do not fit in variable buffer, we need to adjust
  if(bytes_var_to_copy > buffer_var_free_space) {
    // Perform binary search
    int64_t min = start_cell_pos;
    int64_t max = end_cell_pos;
    int64_t med;
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

    // Determine the start position of the range
    if(max < min)  
      end_cell_pos = max - 1;     
    else 
      end_cell_pos = med;  

    // Update variable bytes to copy
    bytes_var_to_copy = tile[end_cell_pos + 1] - tile[start_cell_pos];
  }

  // Update bytes to copy
  bytes_to_copy = (end_cell_pos - start_cell_pos + 1) * sizeof(size_t);
}

template<class T>
void ReadState::compute_cell_pos_ranges() {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();
  int dim_num = array_schema->dim_num();
  const T* range = static_cast<const T*>(fragment_->array()->range());

  // Bring coordinates tile in main memory
  get_tile_from_disk_cmp_none(attribute_num);

  // Invoke the proper function based on the type of range and overlap
  if(is_unary_range(range, dim_num))
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
  ArraySchema::CellOrder cell_order = array_schema->cell_order();

  // Invoke the proper function based on the cell order
  if(cell_order == ArraySchema::TILEDB_AS_CO_ROW_MAJOR)
    compute_cell_pos_ranges_contig_row<T>();
  else if(cell_order == ArraySchema::TILEDB_AS_CO_COLUMN_MAJOR)
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
  med;
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
  med;
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
  ArraySchema::CellOrder cell_order = array_schema->cell_order();

  // Invoke the proper function based on the cell order
  if(cell_order == ArraySchema::TILEDB_AS_CO_ROW_MAJOR ||
     cell_order == ArraySchema::TILEDB_AS_CO_COLUMN_MAJOR) {
    // Find the largest range in which there are results
    compute_cell_pos_ranges_contig<T>();
    if(overlapping_tiles_.back().cell_pos_ranges_.size() == 0) // No results
      return; 
    int64_t start_pos = overlapping_tiles_.back().cell_pos_ranges_[0].first;
    int64_t end_pos = overlapping_tiles_.back().cell_pos_ranges_[0].second;
    overlapping_tiles_.back().cell_pos_ranges_.clear();

    // Scan each cell in the cell range and check if it is in the query range
    compute_cell_pos_ranges_scan<T>(start_pos, end_pos);
  } else if(cell_order == ArraySchema::TILEDB_AS_CO_HILBERT) {
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
  const T* range = static_cast<const T*>(fragment_->array()->range());
  const T* tile = static_cast<const T*>(tiles_[attribute_num]);
  const T* cell;
  int64_t current_start_pos, current_end_pos = -2; 
  OverlappingTile& overlapping_tile = overlapping_tiles_.back();

  // Compute the cell position ranges
  for(int64_t i=start_pos; i<=end_pos; ++i) {
    cell = &tile[i*dim_num];
    if(cell_in_range<T>(cell, range, dim_num)) {
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
  ArraySchema::CellOrder cell_order = array_schema->cell_order();

  // Invoke the proper function based on the cell order
  if(cell_order == ArraySchema::TILEDB_AS_CO_ROW_MAJOR)
    compute_cell_pos_ranges_unary_row<T>();
  else if(cell_order == ArraySchema::TILEDB_AS_CO_COLUMN_MAJOR)
    compute_cell_pos_ranges_unary_col<T>();
  else if(cell_order == ArraySchema::TILEDB_AS_CO_HILBERT)
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

template<class T>
void ReadState::copy_from_tile_buffer_dense(
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
          == PARTIAL_NON_CONTIG)
    copy_from_tile_buffer_partial_non_contig_dense<T>(
        attribute_id, 
        buffer, 
        buffer_size, 
        buffer_offset);
  else if(overlapping_tiles_[overlapping_tile_pos_[attribute_id]].overlap_
          == PARTIAL_CONTIG)
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
  if(overlapping_tiles_[overlapping_tile_pos_[attribute_id]].overlap_
     == FULL)
    copy_from_tile_buffer_full_var(
        attribute_id, 
        buffer, 
        buffer_size, 
        buffer_offset,
        buffer_var, 
        buffer_var_size,
        buffer_var_offset);
  else if(overlapping_tiles_[overlapping_tile_pos_[attribute_id]].overlap_
          == PARTIAL_NON_CONTIG)
    copy_from_tile_buffer_partial_non_contig_dense_var<T>(
        attribute_id, 
        buffer, 
        buffer_size, 
        buffer_offset,
        buffer_var, 
        buffer_var_size,
        buffer_var_offset);
  else if(overlapping_tiles_[overlapping_tile_pos_[attribute_id]].overlap_
          == PARTIAL_CONTIG)
    copy_from_tile_buffer_partial_contig_dense_var<T>(
        attribute_id, 
        buffer, 
        buffer_size, 
        buffer_offset,
        buffer_var, 
        buffer_var_size,
        buffer_var_offset);
}

template<class T>
void ReadState::copy_from_tile_buffer_sparse(
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
          == PARTIAL_NON_CONTIG)
    copy_from_tile_buffer_partial_non_contig_sparse<T>(
        attribute_id, 
        buffer, 
        buffer_size, 
        buffer_offset);
  else if(overlapping_tiles_[overlapping_tile_pos_[attribute_id]].overlap_
          == PARTIAL_CONTIG)
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
  if(overlapping_tiles_[overlapping_tile_pos_[attribute_id]].overlap_
     == FULL)
    copy_from_tile_buffer_full_var(
        attribute_id, 
        buffer, 
        buffer_size, 
        buffer_offset,
        buffer_var, 
        buffer_var_size,
        buffer_var_offset);
  else if(overlapping_tiles_[overlapping_tile_pos_[attribute_id]].overlap_
          == PARTIAL_NON_CONTIG)
    copy_from_tile_buffer_partial_non_contig_sparse_var<T>(
        attribute_id, 
        buffer, 
        buffer_size, 
        buffer_offset,
        buffer_var, 
        buffer_var_size,
        buffer_var_offset);
  else if(overlapping_tiles_[overlapping_tile_pos_[attribute_id]].overlap_
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
    ++buffer_offset; // This will indicate the overflow
  } else {  // Finished copying this tile
    assert(tile_offsets_[attribute_id] == tile_sizes_[attribute_id]);
    ++overlapping_tile_pos_[attribute_id];
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
  char* tile = static_cast<char*>(tiles_[attribute_id]);
  void* tile_start = tile + tile_offsets_[attribute_id];
  char* buffer_var_c = static_cast<char*>(buffer_var);
  void* buffer_var_start = buffer_var_c + buffer_var_offset;
  char* tile_var = static_cast<char*>(tiles_var_[attribute_id]);
  void* tile_var_start = tile_var + tile_var_offsets_[attribute_id];
  size_t bytes_left_to_copy = 
      tile_sizes_[attribute_id] - tile_offsets_[attribute_id];
  size_t bytes_var_left_to_copy = 
      tile_var_sizes_[attribute_id] - tile_var_offsets_[attribute_id];

  // Compute actual bytes to copy 
  size_t buffer_free_space = buffer_size - buffer_offset;
  size_t buffer_var_free_space = buffer_var_size - buffer_var_offset;
  size_t bytes_to_copy, bytes_var_to_copy;
  int64_t start_cell_pos = 0;
  int64_t end_cell_pos = 
      std::min(bytes_left_to_copy, buffer_free_space) / sizeof(size_t) - 1;
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
    tile_offsets_[attribute_id] += bytes_to_copy;
    buffer_offset += bytes_to_copy;
    bytes_left_to_copy -= bytes_to_copy;

    // Shift offsets
    shift_var_offsets(
        buffer_start, 
        end_cell_pos - start_cell_pos + 1, 
        buffer_var_offset); 

    // Copy from variable tile
    memcpy(buffer_var_start, tile_var_start, bytes_var_to_copy);
    tile_var_offsets_[attribute_id] += bytes_var_to_copy;
    buffer_var_offset += bytes_var_to_copy;
    bytes_var_left_to_copy -= bytes_var_to_copy;
  }

  // Check if finished copying this tile
  if(bytes_left_to_copy == 0) { 
    assert(tile_offsets_[attribute_id] == tile_sizes_[attribute_id]);
    assert(tile_var_offsets_[attribute_id] == tile_var_sizes_[attribute_id]);
    ++overlapping_tile_pos_[attribute_id];
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
  size_t range_end_offset = (range_end_cell_pos + 1) * cell_size - 1; 

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

  if(bytes_to_copy == bytes_in_current_slab_left_to_copy &&
     tile_offsets_[attribute_id] != range_end_offset + 1)
    tile_offsets_[attribute_id] += tile_slab_size - range_slab_size;

  // Copy rest of the slabs
  while(buffer_offset != buffer_size &&
        tile_offsets_[attribute_id] != range_end_offset + 1) {
    buffer_free_space = buffer_size - buffer_offset;
    bytes_to_copy = std::min(range_slab_size, buffer_free_space);

    memcpy(
        buffer_c + buffer_offset, 
        tile + tile_offsets_[attribute_id],
        bytes_to_copy);
    
    buffer_offset += bytes_to_copy;
    tile_offsets_[attribute_id] += bytes_to_copy;

    if(bytes_to_copy == range_slab_size &&
       tile_offsets_[attribute_id] != range_end_offset + 1) { 
      // Go to the start of the next slab
      tile_offsets_[attribute_id] += tile_slab_size - bytes_to_copy;
    }
  } 

  // Update overlapping tile position if necessary
  if(tile_offsets_[attribute_id] == range_end_offset + 1) { // This tile is done
    tile_offsets_[attribute_id] = tile_sizes_[attribute_id]; 
    ++overlapping_tile_pos_[attribute_id];
  } else { // Check for overflow
    // This tile is NOT done
    assert(buffer_offset == buffer_size); // Buffer full
    ++buffer_offset;  // This will indicate the overflow
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
  if(buffer_free_space == 0) { // Overflow
    ++buffer_offset; // This will indicate the overflow
    return;
  }

  // Handle trivial cases
  if(buffer_var_free_space == 0) { // Overflow
    ++buffer_var_offset; // This will indicate the overflow
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
  void* buffer_start = buffer_c + buffer_offset;
  char* buffer_var_c = static_cast<char*>(buffer_var);
  char* tile = static_cast<char*>(tiles_[attribute_id]);
  char* tile_var = static_cast<char*>(tiles_var_[attribute_id]);
  size_t* tile_s = static_cast<size_t*>(tiles_[attribute_id]);
  size_t cell_size = sizeof(size_t);

  // Sanity check
  assert(array_schema->var_size(attribute_id));

  // Find the offset at the beginning and end of the overlap range
  int64_t range_start_cell_pos = 
      array_schema->get_cell_pos<T>(range_start_coords);
  int64_t range_end_cell_pos = 
      array_schema->get_cell_pos<T>(range_end_coords);
  size_t range_start_offset = range_start_cell_pos * cell_size; 
  size_t range_end_offset = (range_end_cell_pos + 1) * cell_size - 1; 

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
  size_t bytes_in_current_slab_left_to_copy = 
      current_slab_end_offset - tile_offsets_[attribute_id] + 1;
  int64_t start_cell_pos = current_slab_start_offset / sizeof(size_t);
  int64_t end_cell_pos = ((current_slab_end_offset + 1) / sizeof(size_t)) - 1;

  // If current tile offset is 0, set it to the beginning of the overlap range
  if(tile_var_offsets_[attribute_id] == 0) 
    tile_var_offsets_[attribute_id] = tile_s[range_start_cell_pos];

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

  if(bytes_to_copy == 0) {
    buffer_offset = buffer_size + 1; // This will indicate overflow
    buffer_var_offset = buffer_var_size + 1; // This will indicate overflow
    return;
  }

  memcpy(
      buffer_c + buffer_offset, 
      tile + tile_offsets_[attribute_id],
      bytes_to_copy);
  buffer_offset += bytes_to_copy;
  tile_offsets_[attribute_id] += bytes_to_copy;

  // Shift variable offsets
  shift_var_offsets(
      buffer_start, 
      end_cell_pos - start_cell_pos + 1, 
      buffer_var_offset); 

  memcpy(
      buffer_var_c + buffer_var_offset, 
      tile_var + tile_var_offsets_[attribute_id],
      bytes_var_to_copy);
  buffer_var_offset += bytes_var_to_copy;
  tile_var_offsets_[attribute_id] += bytes_var_to_copy;

  if(bytes_to_copy == bytes_in_current_slab_left_to_copy &&
     tile_offsets_[attribute_id] != range_end_offset + 1) {
    tile_offsets_[attribute_id] += tile_slab_size - range_slab_size;
    if(tile_offsets_[attribute_id] != tile_sizes_[attribute_id])
      tile_var_offsets_[attribute_id] = 
          tile_s[tile_offsets_[attribute_id] / cell_size];
    else 
      tile_var_offsets_[attribute_id] = tile_var_sizes_[attribute_id];
  }

  // Copy rest of the slabs
  while(tile_offsets_[attribute_id] != range_end_offset + 1) {
    buffer_free_space = buffer_size - buffer_offset;
    buffer_var_free_space = buffer_var_size - buffer_var_offset;

    buffer_start = buffer_c + buffer_offset;
    start_cell_pos = tile_offsets_[attribute_id] / sizeof(size_t);
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

    memcpy(
        buffer_c + buffer_offset, 
        tile + tile_offsets_[attribute_id],
        bytes_to_copy);
    buffer_offset += bytes_to_copy;
    tile_offsets_[attribute_id] += bytes_to_copy;

    // Shift variable offsets
    shift_var_offsets(
        buffer_start, 
        end_cell_pos - start_cell_pos + 1, 
        buffer_var_offset); 

    memcpy(
        buffer_var_c + buffer_var_offset, 
        tile_var + tile_var_offsets_[attribute_id],
        bytes_var_to_copy);
    buffer_var_offset += bytes_var_to_copy;
    tile_var_offsets_[attribute_id] += bytes_var_to_copy;

    if(bytes_to_copy == range_slab_size &&
       tile_offsets_[attribute_id] != range_end_offset + 1) { 
      // Go to the start of the next slab
      tile_offsets_[attribute_id] += tile_slab_size - bytes_to_copy;
      if(tile_offsets_[attribute_id] != tile_sizes_[attribute_id])
        tile_var_offsets_[attribute_id] = 
            tile_s[tile_offsets_[attribute_id] / cell_size];
      else 
        tile_var_offsets_[attribute_id] = tile_var_sizes_[attribute_id];
    } else {
      break;
    }
  } 

  // Update overlapping tile position if necessary
  if(tile_offsets_[attribute_id] == range_end_offset + 1) { // This tile is done
    tile_offsets_[attribute_id] = tile_sizes_[attribute_id]; 
    tile_var_offsets_[attribute_id] = tile_var_sizes_[attribute_id]; 
    ++overlapping_tile_pos_[attribute_id];
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
    ++buffer_offset; // This will indicate the overflow
    return;
  }

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  size_t cell_size = array_schema->cell_size(attribute_id);
  OverlappingTile& overlapping_tile = 
      overlapping_tiles_[overlapping_tile_pos_[attribute_id]];
  char* buffer_c = static_cast<char*>(buffer);
  char* tile = static_cast<char*>(tiles_[attribute_id]);

  // Sanity check
  assert(!array_schema->var_size(attribute_id));

  // If there are no qualifying cells in this tile, return
  if(overlapping_tile.cell_pos_ranges_.size() == 0) {
    tile_offsets_[attribute_id] = tile_sizes_[attribute_id]; 
    ++overlapping_tile_pos_[attribute_id];
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
    if(tile_offsets_[attribute_id] < start_offset) 
      tile_offsets_[attribute_id] = start_offset;

    // Calculate the total size to copy
    bytes_left_to_copy = end_offset - tile_offsets_[attribute_id] + 1;
    bytes_to_copy = std::min(bytes_left_to_copy, buffer_free_space); 

    // Copy and update current buffer and tile offsets
    memcpy(
        buffer_c + buffer_offset, 
        tile + tile_offsets_[attribute_id], 
        bytes_to_copy);
    buffer_offset += bytes_to_copy;
    tile_offsets_[attribute_id] += bytes_to_copy; 
    buffer_free_space = buffer_size - buffer_offset;

    // Update overlapping tile position if necessary
    if(i == cell_pos_ranges_num - 1 &&
       tile_offsets_[attribute_id] == end_offset + 1) { // This tile is done
      tile_offsets_[attribute_id] = tile_sizes_[attribute_id]; 
      ++overlapping_tile_pos_[attribute_id];
      cell_pos_range_pos_[attribute_id] = 0; // Update the new range pos 
    } else if(tile_offsets_[attribute_id] != end_offset + 1) {
      // Check for overflow
      assert(buffer_offset == buffer_size); // Buffer full
      ++buffer_offset;  // This will indicate the overflow
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

  // Handle trivial cases
  if(buffer_free_space == 0) { // Overflow
    ++buffer_offset; // This will indicate the overflow
    return;
  }
  if(buffer_var_free_space == 0) { // Overflow
    ++buffer_var_offset; // This will indicate the overflow
    return;
  }

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  size_t cell_size = sizeof(size_t);
  OverlappingTile& overlapping_tile = 
      overlapping_tiles_[overlapping_tile_pos_[attribute_id]];
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
    tile_offsets_[attribute_id] = tile_sizes_[attribute_id]; 
    tile_var_offsets_[attribute_id] = tile_var_sizes_[attribute_id]; 
    ++overlapping_tile_pos_[attribute_id];
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
    if(tile_offsets_[attribute_id] < start_offset) 
      tile_offsets_[attribute_id] = start_offset;

    // Calculate the total size to copy
    bytes_left_to_copy = end_offset - tile_offsets_[attribute_id] + 1;
    bytes_to_copy = std::min(bytes_left_to_copy, buffer_free_space); 

    // Compute actual bytes to copy
    start_cell_pos = tile_offsets_[attribute_id] / cell_size;
    end_cell_pos = overlapping_tile.cell_pos_ranges_[i].second;
    compute_bytes_to_copy(
        attribute_id,
        start_cell_pos,
        end_cell_pos,
        buffer_free_space,
        buffer_var_free_space,
        bytes_to_copy,
        bytes_var_to_copy);

    if(bytes_to_copy == 0) {
      buffer_offset = buffer_size + 1; // This will indicate overflow
      buffer_var_offset = buffer_var_size + 1; // This will indicate overflow
      return;
    }

  // If current tile offset is 0, set it to the beginning of the overlap range
  if(tile_var_offsets_[attribute_id] == 0) 
    tile_var_offsets_[attribute_id] = tile_s[start_cell_pos];

   buffer_start = buffer_c + buffer_offset;

    // Copy and update current buffer and tile offsets
    memcpy(
        buffer_start, 
        tile + tile_offsets_[attribute_id], 
        bytes_to_copy);
    buffer_offset += bytes_to_copy;
    tile_offsets_[attribute_id] += bytes_to_copy; 
    buffer_free_space = buffer_size - buffer_offset;

    // Shift variable offsets
    shift_var_offsets(
        buffer_start, 
        end_cell_pos - start_cell_pos + 1, 
        buffer_var_offset); 

    // Copy and update current variable buffer and tile offsets
    memcpy(
        buffer_var_c + buffer_var_offset, 
        tile_var + tile_var_offsets_[attribute_id], 
        bytes_var_to_copy);
    buffer_var_offset += bytes_var_to_copy;
    tile_var_offsets_[attribute_id] += bytes_var_to_copy; 
    buffer_var_free_space = buffer_var_size - buffer_var_offset;

    // Update overlapping tile position if necessary
    if(i == cell_pos_ranges_num - 1 &&
       tile_offsets_[attribute_id] == end_offset + 1) { // This tile is done
      tile_offsets_[attribute_id] = tile_sizes_[attribute_id]; 
      ++overlapping_tile_pos_[attribute_id];
      cell_pos_range_pos_[attribute_id] = 0; // Update the new range pos 
    } else if(tile_offsets_[attribute_id] != end_offset + 1) {
      // Check for overflow
      assert(buffer_offset == buffer_size); // Buffer full
      ++buffer_offset;  // This will indicate the overflow
      cell_pos_range_pos_[attribute_id] = i; // Update the new range pos 
    }
  }
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
  if(tile_offsets_[attribute_id] == end_offset + 1) { // This tile is done
    tile_offsets_[attribute_id] = tile_sizes_[attribute_id]; 
    ++overlapping_tile_pos_[attribute_id];
  } else { // Check for overflow
    assert(buffer_offset == buffer_size); // Buffer full
    ++buffer_offset;  // This will indicate the overflow
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
  if(buffer_free_space == 0) { // Overflow
    ++buffer_offset; // This will indicate the overflow
    return;
  }
  if(buffer_var_free_space == 0) { // Overflow
    ++buffer_var_offset; // This will indicate the overflow
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

  // Sanity check
  assert(array_schema->var_size(attribute_id));

  // Find the offset at the beginning of the overlap range in the fixed tile
  size_t cell_size = sizeof(size_t);
  int64_t start_cell_pos = array_schema->get_cell_pos<T>(start_coords);
  size_t start_offset = start_cell_pos * cell_size; 
  int64_t cell_num_in_range = ::cell_num_in_range(overlap_range, dim_num);
  size_t range_size = cell_num_in_range * cell_size;
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
    buffer_offset = buffer_size + 1; // This will indicate the overflow
    buffer_var_offset = buffer_var_size + 1; // This will indicate the overflow
    return;
  }

  // Set properly the tile offsets
  tile_offsets_[attribute_id] = start_cell_pos * cell_size;
  tile_var_offsets_[attribute_id] = 
      static_cast<const size_t*>(tiles_[attribute_id])[start_cell_pos];

  // For easy reference
  char* buffer_c = static_cast<char*>(buffer);
  void* buffer_start = buffer_c + buffer_offset;
  char* tile = static_cast<char*>(tiles_[attribute_id]);
  const void* tile_start = tile + tile_offsets_[attribute_id];
  char* buffer_var_c = static_cast<char*>(buffer_var);
  void* buffer_var_start = buffer_var_c + buffer_var_offset;
  char* tile_var = static_cast<char*>(tiles_var_[attribute_id]);
  const void* tile_var_start = tile_var + tile_var_offsets_[attribute_id];

  // Copy and update current buffer and tile offsets
  memcpy(buffer_start, tile_start, bytes_to_copy);
  buffer_offset += bytes_to_copy;
  tile_offsets_[attribute_id] += bytes_to_copy; 

  // Shift variable offsets
  shift_var_offsets(
      buffer_start, 
      end_cell_pos - start_cell_pos + 1, 
      buffer_var_offset); 

  // Copy and update current variable buffer and variable tile offsets
  memcpy(buffer_var_start, tile_var_start, bytes_var_to_copy);
  buffer_var_offset += bytes_var_to_copy;
  tile_var_offsets_[attribute_id] += bytes_var_to_copy; 

  // Update overlapping tile position if necessary
  if(tile_offsets_[attribute_id] == end_offset + 1) { // This tile is done
    tile_offsets_[attribute_id] = tile_sizes_[attribute_id]; 
    tile_var_offsets_[attribute_id] = tile_var_sizes_[attribute_id]; 
    ++overlapping_tile_pos_[attribute_id];
  } // else there is an overflow

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
    ++buffer_offset; // This will indicate the overflow
    return;
  }
 
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  size_t cell_size = array_schema->cell_size(attribute_id);
  OverlappingTile& overlapping_tile = 
      overlapping_tiles_[overlapping_tile_pos_[attribute_id]];
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
  if(tile_offsets_[attribute_id] < start_offset) 
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
  if(tile_offsets_[attribute_id] == end_offset + 1) { // This tile is done
    tile_offsets_[attribute_id] = tile_sizes_[attribute_id]; 
    ++overlapping_tile_pos_[attribute_id];
  } else { // Check for overflow
    assert(buffer_offset == buffer_size); // Buffer full
    ++buffer_offset;  // This will indicate the overflow
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

  // Handle trivial cases
  if(buffer_free_space == 0) { // Overflow
    ++buffer_offset; // This will indicate the overflow
    return;
  }
  if(buffer_var_free_space == 0) { // Overflow
    ++buffer_var_offset; // This will indicate the overflow
    return;
  }

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  size_t cell_size = sizeof(size_t);
  OverlappingTile& overlapping_tile = 
      overlapping_tiles_[overlapping_tile_pos_[attribute_id]];
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
    buffer_offset = buffer_size + 1; // This will indicate the overflow
    buffer_var_offset = buffer_var_size + 1; // This will indicate the overflow
    return;
  }

  // Set properly the tile offsets
  tile_offsets_[attribute_id] = start_cell_pos * cell_size;
  tile_var_offsets_[attribute_id] = 
      static_cast<const size_t*>(tiles_[attribute_id])[start_cell_pos];

  // Copy and update current buffer and tile offsets
  memcpy(
      buffer_c + buffer_offset, 
      tile + tile_offsets_[attribute_id], 
      bytes_to_copy);
  buffer_offset += bytes_to_copy;
  tile_offsets_[attribute_id] += bytes_to_copy; 

  // Shift variable offsets
  shift_var_offsets(
      buffer_c + buffer_offset, 
      end_cell_pos - start_cell_pos + 1, 
      buffer_var_offset);

  // Copy and update current buffer and tile offsets
  memcpy(
      buffer_var_c + buffer_var_offset, 
      tile_var + tile_var_offsets_[attribute_id], 
      bytes_var_to_copy);
  buffer_var_offset += bytes_var_to_copy;
  tile_var_offsets_[attribute_id] += bytes_var_to_copy;

  // Update overlapping tile position if necessary
  if(tile_offsets_[attribute_id] == end_offset + 1) { // This tile is done
    tile_offsets_[attribute_id] = tile_sizes_[attribute_id]; 
    tile_var_offsets_[attribute_id] = tile_var_sizes_[attribute_id]; 
    ++overlapping_tile_pos_[attribute_id];
  } 
}

int ReadState::copy_tile_full(
    int attribute_id,
    void* buffer,
    size_t buffer_size,
    size_t& buffer_offset) {
  // For easy reference
  size_t buffer_free_space = buffer_size - buffer_offset;
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  size_t cell_size = array_schema->cell_size(attribute_id);
  assert(!array_schema->var_size(attribute_id));
  OverlappingTile& overlapping_tile = 
      overlapping_tiles_[overlapping_tile_pos_[attribute_id]];
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
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  size_t cell_size = sizeof(size_t);
  assert(array_schema->var_size(attribute_id));
  OverlappingTile& overlapping_tile = 
      overlapping_tiles_[overlapping_tile_pos_[attribute_id]];
  size_t tile_size = overlapping_tile.cell_num_ * cell_size;

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
  } else {                             // Two-step copy
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

  // Open file
  int fd = open(filename.c_str(), O_RDONLY);
  if(fd == -1) {
    PRINT_ERROR("Cannot copy full tile to buffer; File opening error");
    return TILEDB_RS_ERR;
  }

  // Find file offset where the tile begins
  int64_t pos = overlapping_tiles_[overlapping_tile_pos_[attribute_id]].pos_;
  size_t file_offset = pos * array_schema->tile_size(attribute_id);

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
  size_t cell_size = sizeof(size_t);
  size_t full_tile_size = array_schema->cell_num_per_tile() * cell_size;

  // Sanity check
  assert(tile_size <= buffer_size - buffer_offset);

  // ========== Copy variable cell offsets ========== //
  
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
  size_t file_offset = pos * full_tile_size;

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

  // ========== Copy variable cells ========== // 

  // For easy reference
  char* buffer_var_c = static_cast<char*>(buffer_var);

  // Prepare variable attribute file name
  filename = fragment_->fragment_name() + "/" +
                         array_schema->attribute(attribute_id) + "_var" +
                         TILEDB_FILE_SUFFIX;

  // Open file
  fd = open(filename.c_str(), O_RDONLY);
  if(fd == -1) {
    PRINT_ERROR("Cannot copy full tile to buffer; File opening error");
    return TILEDB_RS_ERR;
  }

  // Find file offset where the tile begins
  void* buffer_at_tile_start = buffer_c + buffer_offset; 
  file_offset = static_cast<size_t*>(buffer_at_tile_start)[0]; 

  // Read tile
  lseek(fd, file_offset, SEEK_SET); 
  bytes_read = ::read(fd, buffer_var_c + buffer_var_offset, tile_var_size);
  if(bytes_read != tile_var_size) {
    PRINT_ERROR("Cannot copy full tile to buffer; File reading error");
    return TILEDB_RS_ERR;
  }

  // Close file
  if(close(fd)) {
    PRINT_ERROR("Cannot copy full tile to buffer; File closing error");
    return TILEDB_RS_ERR;
  }

  // Shift variable cell offsets
  shift_var_offsets(
      buffer_at_tile_start, 
      tile_size / cell_size,
      buffer_var_offset);

  // Update buffer offset and overlapping tile position
  buffer_offset += tile_size;
  buffer_var_offset += tile_var_size;
  ++overlapping_tile_pos_[attribute_id];
  
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
      overlapping_tiles_[overlapping_tile_pos_[attribute_id]];
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
  size_t tile_size = array_schema->tile_size(attribute_id);
  size_t cell_size = array_schema->cell_size(attribute_id);
  int64_t start_cell_pos = array_schema->get_cell_pos<T>(start_coords);
  size_t start_offset = start_cell_pos * cell_size; 
  
  // Prepare attribute file name
  std::string filename = fragment_->fragment_name() + "/" +
                         array_schema->attribute(attribute_id) +
                         TILEDB_FILE_SUFFIX;

  // Open file
  int fd = open(filename.c_str(), O_RDONLY);
  if(fd == -1) {
    PRINT_ERROR("Cannot copy partial contig tile to buffer; "
                "File opening error");
    return TILEDB_RS_ERR;
  }

  // Find file offset where the tile begins
  int64_t pos = overlapping_tile.pos_;
  size_t file_offset = pos * tile_size + start_offset;

  // Read tile
  lseek(fd, file_offset, SEEK_SET); 
  ssize_t bytes_read = ::read(fd, buffer_c + buffer_offset, result_size);
  if(bytes_read != result_size) {
    PRINT_ERROR("Cannot copy partial contig tile to buffer; "
                "File reading error");
    return TILEDB_RS_ERR;
  }
  
  // Close file
  if(close(fd)) {
    PRINT_ERROR("Cannot copy partial contig tile to buffer; "
                "File closing error");
    return TILEDB_RS_ERR;
  }

  // Update offset and overlapping tile pos
  buffer_offset += result_size;
  ++overlapping_tile_pos_[attribute_id];
  
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
      overlapping_tiles_[overlapping_tile_pos_[attribute_id]];
  char* buffer_c = static_cast<char*>(buffer);

  // Sanity check
  assert(!array_schema->var_size(attribute_id));

  // Find the offset at the beginning and end of the overlap range
  size_t tile_size = array_schema->tile_size(attribute_id);
  size_t cell_size = array_schema->cell_size(attribute_id);
  const std::pair<int64_t, int64_t>& cell_pos_range = 
      overlapping_tile.cell_pos_ranges_[0];
  size_t start_offset = cell_pos_range.first * cell_size; 
  
  // Prepare attribute file name
  std::string filename = fragment_->fragment_name() + "/" +
                         array_schema->attribute(attribute_id) +
                         TILEDB_FILE_SUFFIX;

  // Open file
  int fd = open(filename.c_str(), O_RDONLY);
  if(fd == -1) {
    PRINT_ERROR("Cannot copy partial contig tile to buffer; "
                "File opening error");
    return TILEDB_RS_ERR;
  }

  // Find file offset where the tile begins
  int64_t pos = overlapping_tile.pos_;
  size_t file_offset = pos * tile_size + start_offset;

  // Read tile
  lseek(fd, file_offset, SEEK_SET); 
  ssize_t bytes_read = ::read(fd, buffer_c + buffer_offset, result_size);
  if(bytes_read != result_size) {
    PRINT_ERROR("Cannot copy partial contig tile to buffer; "
                "File reading error");
    return TILEDB_RS_ERR;
  }
  
  // Close file
  if(close(fd)) {
    PRINT_ERROR("Cannot copy partial contig tile to buffer; "
                "File closing error");
    return TILEDB_RS_ERR;
  }

  // Update offset and overlapping tile pos
  buffer_offset += result_size;
  ++overlapping_tile_pos_[attribute_id];
  
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
int ReadState::copy_tile_partial_contig_dense(
    int attribute_id,
    void* buffer,
    size_t buffer_size,
    size_t& buffer_offset) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int dim_num = array_schema->dim_num();
  OverlappingTile& overlapping_tile = 
      overlapping_tiles_[overlapping_tile_pos_[attribute_id]];
  const T* overlap_range = 
      static_cast<const T*>(overlapping_tile.overlap_range_);

  // Check if the partial read results fit in the buffer
  size_t buffer_free_space = buffer_size - buffer_offset;
  size_t cell_size = array_schema->cell_size(attribute_id);
  size_t result_size = cell_num_in_range(overlap_range, dim_num) * cell_size;

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
  int dim_num = array_schema->dim_num();
  OverlappingTile& overlapping_tile = 
      overlapping_tiles_[overlapping_tile_pos_[attribute_id]];
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

int ReadState::get_tile_from_disk_cmp_none(int attribute_id) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();

  // Check if the coodinates tile is already in memory
  if(attribute_id == attribute_num && coords_tile_fetched_)
    return TILEDB_RS_OK;

  // For easy reference
  size_t cell_size = array_schema->cell_size(attribute_id);
  OverlappingTile& overlapping_tile = 
      overlapping_tiles_[overlapping_tile_pos_[attribute_id]];
  size_t full_tile_size = array_schema->tile_size(attribute_id);
  size_t tile_size = overlapping_tile.cell_num_ * cell_size;

  // Allocate space for the tile if needed
  if(tiles_[attribute_id] == NULL) 
    tiles_[attribute_id] = malloc(full_tile_size);

  // Set the actual tile size
  tile_sizes_[attribute_id] = tile_size; 

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
  int64_t pos = overlapping_tile.pos_;
  size_t file_offset = pos * full_tile_size;

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

  // Set flag
  if(attribute_id == attribute_num)
    coords_tile_fetched_ = true;
  
  // Success
  return TILEDB_RS_OK;
}

int ReadState::get_tile_from_disk_var_cmp_none(int attribute_id) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();

  // Sanity check
  assert(array_schema->var_size(attribute_id));

  // For easy reference
  size_t cell_size = sizeof(size_t);
  OverlappingTile& overlapping_tile = 
      overlapping_tiles_[overlapping_tile_pos_[attribute_id]];
  int64_t cell_num_per_tile = array_schema->cell_num_per_tile();
  size_t full_tile_size = cell_num_per_tile * cell_size;
  size_t tile_size = overlapping_tile.cell_num_ * cell_size;
  int64_t tile_num = book_keeping_->tile_num();

  // ========== Get tile with variable cell offsets ========== //

  // Allocate space for the tile if needed
  if(tiles_[attribute_id] == NULL)  
    tiles_[attribute_id] = malloc(full_tile_size);

  // Set the actual tile size
  tile_sizes_[attribute_id] = tile_size; 

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
  int64_t pos = overlapping_tile.pos_;
  size_t file_offset = pos * full_tile_size;

  // Read tile
  lseek(fd, file_offset, SEEK_SET); 
  ssize_t bytes_read = ::read(fd, tiles_[attribute_id], tile_size);
  if(bytes_read != tile_size) {
    PRINT_ERROR("Cannot get tile from disk; File reading error");
    return TILEDB_RS_ERR;
  }

  // Get file size
  struct stat st;
  fstat(fd, &st);
  ssize_t file_size = st.st_size;

  // Get last variable cell offset
  size_t end_tile_var_offset; 
  if(pos != tile_num - 1) { // Not the last tile
    bytes_read = ::read(fd, &end_tile_var_offset, sizeof(size_t));
    if(bytes_read != sizeof(size_t)) {
      PRINT_ERROR("Cannot get tile from disk; File reading error");
      return TILEDB_RS_ERR;
    }
  } 
  
  // Close file
  if(close(fd)) {
    PRINT_ERROR("Cannot get tile from disk; File closing error");
    return TILEDB_RS_ERR;
  }

  // Update tile offset
  tile_offsets_[attribute_id] = 0;

  // ========== Get variable tile ========== //

  // For easy reference
  const size_t* tile_s = static_cast<const size_t*>(tiles_[attribute_id]);

  // Prepare variable attribute file name
  filename = fragment_->fragment_name() + "/" +
             array_schema->attribute(attribute_id) + "_var" +
             TILEDB_FILE_SUFFIX;

  // Open file
  fd = open(filename.c_str(), O_RDONLY);
  if(fd == -1) {
    PRINT_ERROR("Cannot get tile from disk; File opening error");
    return TILEDB_RS_ERR;
  }

  // Calculate tile size
  if(pos != tile_num - 1) { // Last tile
    tile_size = end_tile_var_offset - tile_s[0];
  } else {
    struct stat st;
    fstat(fd, &st);
    tile_size = st.st_size - tile_s[0];
  }

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
  tile_var_sizes_[attribute_id] = tile_size; 

  // Find file offset where the tile begins
  file_offset = tile_s[0];

  // Read tile
  lseek(fd, file_offset, SEEK_SET); 
  bytes_read = ::read(fd, tiles_var_[attribute_id], tile_size);
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
  tile_var_offsets_[attribute_id] = 0;

  // Shift variable cell offsets
  shift_var_offsets(attribute_id);

  // Success
  return TILEDB_RS_OK;
}

int ReadState::get_tile_from_disk_cmp_gzip(int attribute_id) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();

  // Check if the coodinates tile is already in memory
  if(attribute_id == attribute_num && coords_tile_fetched_)
    return TILEDB_RS_OK;

  // For easy reference
  size_t cell_size = array_schema->cell_size(attribute_id);
  OverlappingTile& overlapping_tile = 
      overlapping_tiles_[overlapping_tile_pos_[attribute_id]];
  size_t full_tile_size = array_schema->tile_size(attribute_id);
  size_t tile_size = overlapping_tile.cell_num_ * cell_size;
  const std::vector<std::vector<size_t> >& tile_offsets = 
      book_keeping_->tile_offsets(); 
  int64_t tile_num = book_keeping_->tile_num();

  // Potentially allocate or expand compressed tile buffer
  if(tile_compressed_ == NULL) {
    tile_compressed_allocated_size_ = 
        tile_size + 6 + 5*(ceil(tile_size/16834.0));
    tile_compressed_ = malloc(tile_compressed_allocated_size_); 
  }

  // Expand comnpressed tile if necessary
  if(tile_size + 6 + 5*(ceil(tile_size/16834.0)) > 
     tile_compressed_allocated_size_) { 
    tile_compressed_allocated_size_ = 
        tile_size + 6 + 5*(ceil(tile_size/16834.0));
    tile_compressed_ = 
        realloc(tile_compressed_, tile_compressed_allocated_size_);
  }

  // Allocate space for the tile if needed
  if(tiles_[attribute_id] == NULL) 
    tiles_[attribute_id] = malloc(full_tile_size);

  // Set the actual tile size
  tile_sizes_[attribute_id] = tile_size;

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

  // Get file size
  struct stat st;
  fstat(fd, &st);
  ssize_t file_size = st.st_size;

  // Find file offset where the tile begins
  int64_t pos = overlapping_tile.pos_;
  size_t file_offset = tile_offsets[attribute_id][pos];
  size_t tile_compressed_size = 
      (pos == tile_num-1) ? file_size - tile_offsets[attribute_id][pos] 
                          : tile_offsets[attribute_id][pos+1] - 
                            tile_offsets[attribute_id][pos];

  // Read tile
  lseek(fd, file_offset, SEEK_SET); 
  ssize_t bytes_read = ::read(fd, tile_compressed_, tile_compressed_size);

  if(bytes_read != tile_compressed_size) {
    PRINT_ERROR("Cannot get tile from disk; File reading error");
    return TILEDB_RS_ERR;
  }
 
  // Close file
  if(close(fd)) {
    PRINT_ERROR("Cannot get tile from disk; File closing error");
    return TILEDB_RS_ERR;
  }

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
  tile_offsets_[attribute_id] = 0;

  // Set flag
  if(attribute_id == attribute_num)
    coords_tile_fetched_ = true;
  
  // Success
  return TILEDB_RS_OK;
}

int ReadState::get_tile_from_disk_var_cmp_gzip(int attribute_id) {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();

  // Sanity check
  assert(array_schema->var_size(attribute_id));

  // ========== Get tile with variable cell offsets ========== //

  // For easy reference
  size_t cell_size = sizeof(size_t);
  OverlappingTile& overlapping_tile = 
      overlapping_tiles_[overlapping_tile_pos_[attribute_id]];
  size_t full_tile_size = array_schema->cell_num_per_tile() * cell_size;
  size_t tile_size = overlapping_tile.cell_num_ * cell_size;
  const std::vector<std::vector<size_t> >& tile_offsets = 
      book_keeping_->tile_offsets(); 
  int64_t tile_num = book_keeping_->tile_num();

  // Potentially allocate or expand compressed tile buffer
  if(tile_compressed_ == NULL) {
    tile_compressed_allocated_size_ = 
        tile_size + 6 + 5*(ceil(tile_size/16834.0));
    tile_compressed_ = malloc(tile_compressed_allocated_size_); 
  }

  // Expand comnpressed tile if necessary
  if(tile_size + 6 + 5*(ceil(tile_size/16834.0)) > 
        tile_compressed_allocated_size_) {
    tile_compressed_allocated_size_ = 
        tile_size + 6 + 5*(ceil(tile_size/16834.0));
    tile_compressed_ = 
        realloc(tile_compressed_, tile_compressed_allocated_size_);
  }

  // Allocate space for the tile if needed
  if(tiles_[attribute_id] == NULL) 
    tiles_[attribute_id] = malloc(full_tile_size);

  // Set the actual tile size
  tile_sizes_[attribute_id] = tile_size;

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

  // Get file size
  struct stat st;
  fstat(fd, &st);
  ssize_t file_size = st.st_size;

  // Find file offset where the tile begins
  int64_t pos = overlapping_tile.pos_;
  size_t file_offset = tile_offsets[attribute_id][pos];
  size_t tile_compressed_size = 
      (pos == tile_num-1) ? file_size - tile_offsets[attribute_id][pos] 
                          : tile_offsets[attribute_id][pos+1] - 
                            tile_offsets[attribute_id][pos];

  // Read tile
  lseek(fd, file_offset, SEEK_SET); 
  ssize_t bytes_read = ::read(fd, tile_compressed_, tile_compressed_size);
  if(bytes_read != tile_compressed_size) {
    PRINT_ERROR("Cannot get tile from disk; File reading error");
    return TILEDB_RS_ERR;
  }
 
  // Close file
  if(close(fd)) {
    PRINT_ERROR("Cannot get tile from disk; File closing error");
    return TILEDB_RS_ERR;
  }

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
  tile_offsets_[attribute_id] = 0;

  // ========== Get variable tile ========== //

  // For easy reference
  const std::vector<std::vector<size_t> >& tile_var_offsets = 
      book_keeping_->tile_var_offsets(); 

  // Prepare variable attribute file name
  filename = fragment_->fragment_name() + "/" +
             array_schema->attribute(attribute_id) + "_var" +
             TILEDB_FILE_SUFFIX;

  // Open file
  fd = open(filename.c_str(), O_RDONLY);
  if(fd == -1) {
    PRINT_ERROR("Cannot get tile from disk; File opening error");
    return TILEDB_RS_ERR;
  }

  // Calculate offset and compressed tile size
  file_offset = tile_var_offsets[attribute_id][pos];
  fstat(fd, &st);
  tile_compressed_size = 
      (pos == tile_num-1) ? st.st_size - tile_var_offsets[attribute_id][pos] 
                          : tile_var_offsets[attribute_id][pos+1] - 
                            tile_var_offsets[attribute_id][pos];

  // Expand comnpressed tile if necessary
  if(tile_compressed_size > tile_compressed_allocated_size_) {
    tile_compressed_ = realloc(tile_compressed_, tile_compressed_size);
    tile_compressed_allocated_size_ = tile_compressed_size;
  }

  // Read tile
  lseek(fd, file_offset, SEEK_SET); 
  bytes_read = ::read(fd, tile_compressed_, tile_compressed_size);
  if(bytes_read != tile_compressed_size) {
    PRINT_ERROR("Cannot get tile from disk; File reading error");
    return TILEDB_RS_ERR;
  }
  
  // Close file
  if(close(fd)) {
    PRINT_ERROR("Cannot get tile from disk; File closing error");
    return TILEDB_RS_ERR;
  }

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
  tile_var_sizes_[attribute_id] = tile_var_size; 

  // Update tile offset
  tile_var_offsets_[attribute_id] = 0;

  // Shift variable cell offsets
  shift_var_offsets(attribute_id);

  // Success
  return TILEDB_RS_OK;
}

void ReadState::init_range_in_tile_domain() {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  const std::type_info* coords_type = array_schema->coords_type();

  // Invoke the proper templated function
  if(coords_type == &typeid(int)) {
    init_range_in_tile_domain<int>();
  } else if(coords_type == &typeid(int64_t)) {
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

void ReadState::init_tile_search_range() {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  const std::type_info* coords_type = array_schema->coords_type();

  // Invoke the proper templated function
  if(coords_type == &typeid(int)) {
    init_tile_search_range<int>();
  } else if(coords_type == &typeid(int64_t)) {
    init_tile_search_range<int64_t>();
  } else if(coords_type == &typeid(float)) {
    init_tile_search_range<float>();
  } else if(coords_type == &typeid(double)) {
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
  ArraySchema::CellOrder cell_order = array_schema->cell_order();

  // Initialize the tile search range
  if(cell_order == ArraySchema::TILEDB_AS_CO_HILBERT) { // HILBERT
    init_tile_search_range_hil<T>();
  } else if(cell_order == ArraySchema::TILEDB_AS_CO_ROW_MAJOR) { // ROW MAJOR
    init_tile_search_range_row<T>();
  } else { // COLUMN MAJOR
    init_tile_search_range_col<T>();
  } 

  // Get the first overlapping tile in case of no overlap
  if(tile_search_range_[1] < tile_search_range_[0]) {
    OverlappingTile overlapping_tile;
    overlapping_tile.overlap_ = NONE;
    overlapping_tile.overlap_range_ = NULL; 
    overlapping_tile.coords_ = NULL;
    overlapping_tiles_.push_back(overlapping_tile);
  }
}

template<class T>
void ReadState::init_tile_search_range_col() {
  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  int dim_num = array_schema->dim_num();
  const T* range = static_cast<const T*>(fragment_->array()->range());
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

  if(is_unary_range(range, dim_num)) {  // Unary range
    // The end positions is the same as the start
    tile_search_range_[1] = tile_search_range_[0];
  } else { // Need to find the end position
    // --- Finding the end tile in search range

    // Perform binary search
    min = 0;
    max = tile_num - 1;
    med;
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
  const T* range = static_cast<const T*>(fragment_->array()->range());
  int64_t tile_num = book_keeping_->tile_num();

  if(is_unary_range(range, dim_num)) {  // Unary range
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
  const T* range = static_cast<const T*>(fragment_->array()->range());
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

  if(is_unary_range(range, dim_num)) {  // Unary range
    // The end positions is the same as the start
    tile_search_range_[1] = tile_search_range_[0];
  } else { // Need to find the end position
    // --- Finding the end tile in search range

    // Perform binary search
    min = 0;
    max = tile_num - 1;
    med;
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
    return read_dense_attr_cmp_gzip(attribute_id, buffer, buffer_size);
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

  // The following loop should break somewhere inside
  for(;;) {
    // There are still data pending inside the local tile buffers
    if(tile_offsets_[attribute_id] < tile_sizes_[attribute_id]) 
      copy_from_tile_buffer_dense<T>(
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
      get_next_overlapping_tile_dense<T>();

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
    } else if(overlapping_tiles_[overlapping_tile_pos_[attribute_id]].overlap_ 
              == PARTIAL_CONTIG) { // Partial tile, contig
      if(copy_tile_partial_contig_dense<T>(
             attribute_id, 
             buffer, 
             buffer_size, 
             buffer_offset) != TILEDB_RS_OK)
        return TILEDB_RS_ERR;
    } else {                       // Partial tile, non-contig
      if(copy_tile_partial_non_contig_dense<T>(
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
  const std::type_info* coords_type = array_schema->coords_type();

  // Invoke the proper templated function
  if(coords_type == &typeid(int)) {
    return read_dense_attr_cmp_gzip<int>(attribute_id, buffer, buffer_size);
  } else if(coords_type == &typeid(int64_t)) {
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
    if(tile_offsets_[attribute_id] < tile_sizes_[attribute_id]) 
      copy_from_tile_buffer_dense<T>(
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
      get_next_overlapping_tile_dense<T>();

    // Fectch and decompress tile
    if(overlapping_tiles_[overlapping_tile_pos_[attribute_id]].overlap_
       != NONE) {
      if(get_tile_from_disk_cmp_gzip(attribute_id) != TILEDB_RS_OK)
        return TILEDB_RS_ERR;
    }

    // Invoke proper copy command
    if(overlapping_tiles_[overlapping_tile_pos_[attribute_id]].overlap_
       == NONE) {                 // No more tiles
      buffer_size = buffer_offset;
      return TILEDB_RS_OK; 
    } else if(overlapping_tiles_[overlapping_tile_pos_[attribute_id]].overlap_ 
              == FULL) {          // Full tile
      copy_from_tile_buffer_full(
          attribute_id, 
          buffer, 
          buffer_size, 
          buffer_offset);
    } else if(overlapping_tiles_[overlapping_tile_pos_[attribute_id]].overlap_ 
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
    if(buffer_offset > buffer_size) {
      ++buffer_size; // This will indicate the overflow
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
  if(buffer_size == 0 || buffer_var_size == 0)
    return TILEDB_RS_OK;

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  ArraySchema::Compression compression = 
      array_schema->compression(attribute_id);

  // No compression
  if(compression == ArraySchema::TILEDB_AS_CMP_NONE) {
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
  const std::type_info* coords_type = array_schema->coords_type();

  // Invoke the proper templated function
  if(coords_type == &typeid(int)) {
    return read_dense_attr_var_cmp_none<int>(
               attribute_id, 
               buffer, 
               buffer_size,
               buffer_var,
               buffer_var_size);
  } else if(coords_type == &typeid(int64_t)) {
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
    if(tile_offsets_[attribute_id] < tile_sizes_[attribute_id]) 
      copy_from_tile_buffer_dense_var<T>(
          attribute_id, 
          buffer, 
          buffer_size, 
          buffer_offset,
          buffer_var,
          buffer_var_size,
          buffer_var_offset); 

    // If the buffer overflows, return
    if(buffer_offset > buffer_size) {
      ++buffer_size; // This will indicate the overflow
      return TILEDB_RS_OK; 
    }

    // If the variable buffer overflows, return
    if(buffer_var_offset > buffer_var_size) {
      ++buffer_var_size; // This will indicate the overflow
      return TILEDB_RS_OK; 
    }

    // Compute the next overlapping tile
    if(overlapping_tile_pos_[attribute_id] >= overlapping_tiles_.size())
      get_next_overlapping_tile_dense<T>();

    // Invoke proper copy command
    if(overlapping_tiles_[overlapping_tile_pos_[attribute_id]].overlap_
       == NONE) {                 // No more tiles
      buffer_size = buffer_offset;
      return TILEDB_RS_OK; 
    } else if(overlapping_tiles_[overlapping_tile_pos_[attribute_id]].overlap_ 
              == FULL) {          // Full tile
      if(copy_tile_full_var(
             attribute_id, 
             buffer, 
             buffer_size, 
             buffer_offset,
             buffer_var,
             buffer_var_size,
             buffer_var_offset) != TILEDB_RS_OK)
        return TILEDB_RS_ERR;
    } else if(overlapping_tiles_[overlapping_tile_pos_[attribute_id]].overlap_ 
              == PARTIAL_CONTIG) { // Partial tile, contig
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
    if(buffer_offset > buffer_size) {
      ++buffer_size; // This will indicate the overflow
      return TILEDB_RS_OK; 
    }

    // If the variable buffer overflows, return
    if(buffer_var_offset > buffer_var_size) {
      ++buffer_var_size; // This will indicate the overflow
      return TILEDB_RS_OK; 
    }
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
  const std::type_info* coords_type = array_schema->coords_type();

  // Invoke the proper templated function
  if(coords_type == &typeid(int)) {
    return read_dense_attr_var_cmp_gzip<int>(
               attribute_id, 
               buffer, 
               buffer_size,
               buffer_var,
               buffer_var_size);
  } else if(coords_type == &typeid(int64_t)) {
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
    if(tile_offsets_[attribute_id] < tile_sizes_[attribute_id]) 
      copy_from_tile_buffer_dense_var<T>(
          attribute_id, 
          buffer, 
          buffer_size, 
          buffer_offset,
          buffer_var,
          buffer_var_size,
          buffer_var_offset); 

    // If the buffer overflows, return
    if(buffer_offset > buffer_size) {
      ++buffer_size; // This will indicate the overflow
      return TILEDB_RS_OK; 
    }

    // If the variable buffer overflows, return
    if(buffer_var_offset > buffer_var_size) {
      ++buffer_var_size; // This will indicate the overflow
      return TILEDB_RS_OK; 
    }

    // Compute the next overlapping tile
    if(overlapping_tile_pos_[attribute_id] >= overlapping_tiles_.size())
      get_next_overlapping_tile_dense<T>();

    // Fectch and decompress tile
    if(overlapping_tiles_[overlapping_tile_pos_[attribute_id]].overlap_
       != NONE) {
      if(get_tile_from_disk_var_cmp_gzip(attribute_id) != TILEDB_RS_OK)
        return TILEDB_RS_ERR;
    }

    // Invoke proper copy command
    if(overlapping_tiles_[overlapping_tile_pos_[attribute_id]].overlap_
       == NONE) {                 // No more tiles
      buffer_size = buffer_offset;
      return TILEDB_RS_OK; 
    } else if(overlapping_tiles_[overlapping_tile_pos_[attribute_id]].overlap_ 
              == FULL) {          // Full tile
      copy_from_tile_buffer_full_var(
          attribute_id, 
          buffer, 
          buffer_size, 
          buffer_offset,
          buffer_var,
          buffer_var_size,
          buffer_var_offset);
    } else if(overlapping_tiles_[overlapping_tile_pos_[attribute_id]].overlap_ 
              == PARTIAL_CONTIG) { // Partial tile, contig
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
    if(buffer_offset > buffer_size) {
      ++buffer_size; // This will indicate the overflow
      return TILEDB_RS_OK; 
    }

    // If the variable buffer overflows, return
    if(buffer_var_offset > buffer_var_size) {
      ++buffer_var_size; // This will indicate the overflow
      return TILEDB_RS_OK; 
    }
  }
}

int ReadState::read_sparse(
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
  if(buffer_size == 0)
    return TILEDB_RS_OK;

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  ArraySchema::Compression compression = 
      array_schema->compression(attribute_id);

  // No compression
  if(compression == ArraySchema::TILEDB_AS_CMP_NONE) {
    return read_sparse_attr_cmp_none(attribute_id, buffer, buffer_size);
  } else { // GZIP
    return read_sparse_attr_cmp_gzip(attribute_id, buffer, buffer_size);
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
  const std::type_info* coords_type = array_schema->coords_type();

  // Invoke the proper templated function
  if(coords_type == &typeid(int)) {
    return read_sparse_attr_cmp_none<int>(
               attribute_id, 
               buffer, 
               buffer_size);
  } else if(coords_type == &typeid(int64_t)) {
    return read_sparse_attr_cmp_none<int64_t>(
               attribute_id, 
               buffer, 
               buffer_size);
  } else if(coords_type == &typeid(float)) {
    return read_sparse_attr_cmp_none<float>(
               attribute_id, 
               buffer, 
               buffer_size);
  } else if(coords_type == &typeid(double)) {
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
  // Auxiliary variables
  size_t buffer_offset = 0;

  // The following loop should break somewhere inside
  for(;;) {
    // There are still data pending inside the local tile buffers
    if(tile_offsets_[attribute_id] < tile_sizes_[attribute_id]) { 
      copy_from_tile_buffer_sparse<T>(
          attribute_id, 
          buffer, 
          buffer_size, 
          buffer_offset); 
    }

    // If the buffer overflows, return
    if(buffer_offset > buffer_size) {
      ++buffer_size; // This will indicate the overflow
      return TILEDB_RS_OK; 
    }

    // Compute the next overlapping tile
    if(overlapping_tile_pos_[attribute_id] >= overlapping_tiles_.size())
      get_next_overlapping_tile_sparse<T>();

    // Invoke proper copy command
    if(attribute_id == fragment_->array()->array_schema()->attribute_num() &&
       coords_tile_fetched_) {
      // The coordinates tile is already in main memory
      copy_from_tile_buffer_sparse<T>(
          attribute_id, 
          buffer, 
          buffer_size, 
          buffer_offset);
    } else if(overlapping_tiles_[overlapping_tile_pos_[attribute_id]].overlap_
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
    } else if(overlapping_tiles_[overlapping_tile_pos_[attribute_id]].overlap_ 
              == PARTIAL_CONTIG) { // Partial tile, contig
      if(copy_tile_partial_contig_sparse<T>(
             attribute_id, 
             buffer, 
             buffer_size, 
             buffer_offset) != TILEDB_RS_OK)
        return TILEDB_RS_ERR;
    } else {                       // Partial tile, non-contig
      if(copy_tile_partial_non_contig_sparse<T>(
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
  const std::type_info* coords_type = array_schema->coords_type();

  // Invoke the proper templated function
  if(coords_type == &typeid(int)) {
    return read_sparse_attr_cmp_gzip<int>(
               attribute_id, 
               buffer, 
               buffer_size);
  } else if(coords_type == &typeid(int64_t)) {
    return read_sparse_attr_cmp_gzip<int64_t>(
               attribute_id, 
               buffer, 
               buffer_size);
  } else if(coords_type == &typeid(float)) {
    return read_sparse_attr_cmp_gzip<float>(
               attribute_id, 
               buffer, 
               buffer_size);
  } else if(coords_type == &typeid(double)) {
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
    if(tile_offsets_[attribute_id] < tile_sizes_[attribute_id]) 
      copy_from_tile_buffer_sparse<T>(
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
      get_next_overlapping_tile_sparse<T>();

    // Fectch and decompress tile
    if(overlapping_tiles_[overlapping_tile_pos_[attribute_id]].overlap_
       != NONE) {
      if(get_tile_from_disk_cmp_gzip(attribute_id) != TILEDB_RS_OK)
        return TILEDB_RS_ERR;
    }

    // Invoke proper copy command
    if(overlapping_tiles_[overlapping_tile_pos_[attribute_id]].overlap_
       == NONE) {                 // No more tiles
      buffer_size = buffer_offset;
      return TILEDB_RS_OK; 
    } else if(overlapping_tiles_[overlapping_tile_pos_[attribute_id]].overlap_ 
              == FULL) {          // Full tile
      copy_from_tile_buffer_full(
          attribute_id, 
          buffer, 
          buffer_size, 
          buffer_offset);
    } else if(overlapping_tiles_[overlapping_tile_pos_[attribute_id]].overlap_ 
              == PARTIAL_CONTIG) { // Partial tile, contig
      copy_from_tile_buffer_partial_contig_sparse<T>(
          attribute_id, 
          buffer, 
          buffer_size, 
          buffer_offset);
    } else {                       // Partial tile, non-contig
      copy_from_tile_buffer_partial_non_contig_sparse<T>(
          attribute_id, 
          buffer, 
          buffer_size, 
          buffer_offset);
    }

    // If the buffer overflows, return
    if(buffer_offset > buffer_size) {
      ++buffer_size; // This will indicate the overflow
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
  if(buffer_size == 0 || buffer_var_size == 0)
    return TILEDB_RS_OK;

  // For easy reference
  const ArraySchema* array_schema = fragment_->array()->array_schema();
  ArraySchema::Compression compression = 
      array_schema->compression(attribute_id);

  // No compression
  if(compression == ArraySchema::TILEDB_AS_CMP_NONE) {
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
  const std::type_info* coords_type = array_schema->coords_type();

  // Invoke the proper templated function
  if(coords_type == &typeid(int)) {
    return read_sparse_attr_var_cmp_none<int>(
               attribute_id, 
               buffer, 
               buffer_size,
               buffer_var, 
               buffer_var_size);
  } else if(coords_type == &typeid(int64_t)) {
    return read_sparse_attr_var_cmp_none<int64_t>(
               attribute_id, 
               buffer, 
               buffer_size,
               buffer_var, 
               buffer_var_size);
  } else if(coords_type == &typeid(float)) {
    return read_sparse_attr_var_cmp_none<float>(
               attribute_id, 
               buffer, 
               buffer_size,
               buffer_var, 
               buffer_var_size);
  } else if(coords_type == &typeid(double)) {
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
    if(tile_offsets_[attribute_id] < tile_sizes_[attribute_id]) 
      copy_from_tile_buffer_sparse_var<T>(
          attribute_id, 
          buffer, 
          buffer_size, 
          buffer_offset, 
          buffer_var, 
          buffer_var_size, 
          buffer_var_offset); 

    // If the buffer overflows, return
    if(buffer_offset > buffer_size) {
      ++buffer_size; // This will indicate the overflow
      return TILEDB_RS_OK; 
    }

    // If the buffer overflows, return
    if(buffer_var_offset > buffer_var_size) {
      ++buffer_var_size; // This will indicate the overflow
      return TILEDB_RS_OK; 
    }

    // Compute the next overlapping tile
    if(overlapping_tile_pos_[attribute_id] >= overlapping_tiles_.size())
      get_next_overlapping_tile_sparse<T>();

    // Invoke proper copy command
    if(overlapping_tiles_[overlapping_tile_pos_[attribute_id]].overlap_
       == NONE) {                 // No more tiles
      buffer_size = buffer_offset;
      return TILEDB_RS_OK; 
    } else if(overlapping_tiles_[overlapping_tile_pos_[attribute_id]].overlap_ 
              == FULL) {          // Full tile
      if(copy_tile_full_var(
             attribute_id, 
             buffer, 
             buffer_size, 
             buffer_offset,
             buffer_var, 
             buffer_var_size, 
             buffer_var_offset) != TILEDB_RS_OK)
        return TILEDB_RS_ERR;
    } else if(overlapping_tiles_[overlapping_tile_pos_[attribute_id]].overlap_ 
              == PARTIAL_CONTIG) { // Partial tile, contig
      if(copy_tile_partial_contig_sparse_var<T>(
             attribute_id, 
             buffer, 
             buffer_size, 
             buffer_offset,
             buffer_var, 
             buffer_var_size, 
             buffer_var_offset) != TILEDB_RS_OK)
        return TILEDB_RS_ERR;
    } else {                       // Partial tile, non-contig
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
    if(buffer_offset > buffer_size) {
      ++buffer_size; // This will indicate the overflow
      return TILEDB_RS_OK; 
    }

    // If the buffer overflows, return
    if(buffer_var_offset > buffer_var_size) {
      ++buffer_var_size; // This will indicate the overflow
      return TILEDB_RS_OK; 
    }
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
  const std::type_info* coords_type = array_schema->coords_type();

  // Invoke the proper templated function
  if(coords_type == &typeid(int)) {
    return read_sparse_attr_var_cmp_gzip<int>(
               attribute_id, 
               buffer, 
               buffer_size,
               buffer_var, 
               buffer_var_size);
  } else if(coords_type == &typeid(int64_t)) {
    return read_sparse_attr_var_cmp_gzip<int64_t>(
               attribute_id, 
               buffer, 
               buffer_size,
               buffer_var, 
               buffer_var_size);
  } else if(coords_type == &typeid(float)) {
    return read_sparse_attr_var_cmp_gzip<float>(
               attribute_id, 
               buffer, 
               buffer_size,
               buffer_var, 
               buffer_var_size);
  } else if(coords_type == &typeid(double)) {
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
    if(tile_offsets_[attribute_id] < tile_sizes_[attribute_id]) 
      copy_from_tile_buffer_sparse_var<T>(
          attribute_id, 
          buffer, 
          buffer_size, 
          buffer_offset, 
          buffer_var, 
          buffer_var_size, 
          buffer_var_offset); 

    // If the buffer overflows, return
    if(buffer_offset > buffer_size) {
      ++buffer_size; // This will indicate the overflow
      return TILEDB_RS_OK; 
    }

    // If the buffer overflows, return
    if(buffer_var_offset > buffer_var_size) {
      ++buffer_var_size; // This will indicate the overflow
      return TILEDB_RS_OK; 
    }

    // Compute the next overlapping tile
    if(overlapping_tile_pos_[attribute_id] >= overlapping_tiles_.size())
      get_next_overlapping_tile_sparse<T>();

    // Fectch and decompress tile
    if(overlapping_tiles_[overlapping_tile_pos_[attribute_id]].overlap_
       != NONE) {
      if(get_tile_from_disk_var_cmp_gzip(attribute_id) != TILEDB_RS_OK)
        return TILEDB_RS_ERR;
    }

    // Invoke proper copy command
    if(overlapping_tiles_[overlapping_tile_pos_[attribute_id]].overlap_
       == NONE) {                 // No more tiles
      buffer_size = buffer_offset;
      return TILEDB_RS_OK; 
    } else if(overlapping_tiles_[overlapping_tile_pos_[attribute_id]].overlap_ 
              == FULL) {          // Full tile
      copy_from_tile_buffer_full_var(
          attribute_id, 
          buffer, 
          buffer_size, 
          buffer_offset, 
          buffer_var, 
          buffer_var_size, 
          buffer_var_offset);
    } else if(overlapping_tiles_[overlapping_tile_pos_[attribute_id]].overlap_ 
              == PARTIAL_CONTIG) { // Partial tile, contig
      copy_from_tile_buffer_partial_contig_sparse_var<T>(
          attribute_id, 
          buffer, 
          buffer_size, 
          buffer_offset, 
          buffer_var, 
          buffer_var_size, 
          buffer_var_offset);
    } else {                       // Partial tile, non-contig
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
    if(buffer_offset > buffer_size) {
      ++buffer_size; // This will indicate the overflow
      return TILEDB_RS_OK; 
    }

    // If the buffer overflows, return
    if(buffer_var_offset > buffer_var_size) {
      ++buffer_var_size; // This will indicate the overflow
      return TILEDB_RS_OK; 
    }
  }
}

void ReadState::shift_var_offsets(int attribute_id) {
  // For easy reference
  int64_t cell_num = tile_sizes_[attribute_id] / sizeof(size_t);
  size_t* tile_s = static_cast<size_t*>(tiles_[attribute_id]);
  size_t first_offset = tile_s[0];

  // Shift offsets
  for(int64_t i=0; i<cell_num; ++i) 
    tile_s[i] -= first_offset;
}

void ReadState::shift_var_offsets(
    void* buffer, 
    int64_t cell_num,
    size_t new_start_offset) {
  // For easy reference
  size_t* buffer_s = static_cast<size_t*>(buffer);
  size_t start_offset = buffer_s[0];

  // Shift offsets
  for(int64_t i=0; i<cell_num; ++i) 
    buffer_s[i] = buffer_s[i] - start_offset + new_start_offset;
}
