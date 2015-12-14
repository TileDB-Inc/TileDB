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

#include "bin_file.h"
#include "bin_file_collection.h"
#include "cell.h"
#include "global.h"
#include "special_values.h"
#include "utils.h"
#include "write_state.h"
#include <assert.h>
#include <fcntl.h>
#include <math.h>
#include <sstream>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#ifdef GNU_PARALLEL
  #include <parallel/algorithm>
  #define SORT(first, last, comp) __gnu_parallel::sort((first), (last), (comp))
#else
  #include <algorithm>
  #define SORT(first, last, comp) std::sort((first), (last), (comp))
#endif

/******************************************************
************* CONSTRUCTORS & DESTRUCTORS **************
******************************************************/

WriteState::WriteState(
    const ArraySchema* array_schema,
    const std::string* fragment_name,
    const std::string* temp_dirname,
    const std::string* dirname,
    BookKeeping* book_keeping,
    bool dense)
    : array_schema_(array_schema),
      fragment_name_(fragment_name),
      temp_dirname_(temp_dirname),
      dirname_(dirname),
      book_keeping_(book_keeping),
      dense_(dense) {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();

  segment_size_ = SEGMENT_SIZE;
  write_state_max_size_ = WRITE_STATE_MAX_SIZE;

  current_coords_ = NULL;
  zero_cell_ = NULL;

  tile_id_ = INVALID_TILE_ID;
  cell_num_ = 0;
  run_offset_ = 0;
  run_size_ = 0;
  runs_num_ = 0;

  mbr_ = NULL;
  bounding_coordinates_.first = NULL;
  bounding_coordinates_.second = NULL;

  segments_.resize(attribute_num+1);
  segment_utilization_.resize(attribute_num+1);
  gz_segments_.resize(attribute_num+1);
  gz_segment_utilization_.resize(attribute_num+1);
  file_offsets_.resize(attribute_num+1);
  for(int i=0; i<= attribute_num; ++i) {
    segments_[i] = malloc(segment_size_);
    segment_utilization_[i] = 0;
    if(array_schema_->compression(i) == CMP_GZIP) 
      gz_segments_[i] = malloc(segment_size_);
    else if(array_schema_->compression(i) == CMP_NONE) 
      gz_segments_[i] = NULL;
    gz_segment_utilization_[i] = 0;
    file_offsets_[i] = 0;
  }

  // Prepare compression buffer
  gz_buffer_ = malloc(segment_size_);
}

WriteState::~WriteState() {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  int64_t cells_num = cells_.size();
  int64_t cells_with_id_num = cells_with_id_.size();
  int64_t cells_with_2_ids_num = cells_with_2_ids_.size();

  if(cells_num > 0) {
    for(int64_t i=0; i<cells_num; ++i) 
      free(cells_[i].cell_);
  }
  if(cells_with_id_num > 0) {
    for(int64_t i=0; i<cells_with_id_num; ++i)
      free(cells_with_id_[i].cell_);
  }
  if(cells_with_2_ids_num > 0) {
    for(int64_t i=0; i<cells_with_2_ids_num; ++i)
      free(cells_with_2_ids_[i].cell_);
  }

  // Clear segments
  for(int i=0; i<=attribute_num; ++i) {
    if(segments_[i] != NULL)
      free(segments_[i]);
    if(array_schema_->compression(i) == CMP_GZIP) 
      free(gz_segments_[i]);
  }

  // Clear current_coords_ and zero_cell_
  if(current_coords_ != NULL) 
    free(current_coords_);
  if(zero_cell_ != NULL) 
    free(zero_cell_);

  // Clear MBR and bounding coordinates
  if(mbr_ != NULL)
    free(mbr_);
  if(bounding_coordinates_.first != NULL)
    free(bounding_coordinates_.first);
  if(bounding_coordinates_.second != NULL)
    free(bounding_coordinates_.second);

  // Clear compression buffer
  if(gz_buffer_ != NULL)
    free(gz_buffer_);
}

/******************************************************
******************** ACCESSORS ************************
******************************************************/

int64_t WriteState::cell_num() const {
  return cell_num_;
}

/******************************************************
********************* MUTATORS ************************
******************************************************/

void WriteState::flush() {
  // Make tiles, after finalizing the last run and merging the runs
  finalize_last_run();

  // Make tiles
  if(runs_num_ > 0)
    make_tiles(*temp_dirname_);

  flush_segments();
}

template<class T>
int WriteState::cell_write(const void* input_cell) {
  // Find cell size
  size_t cell_size = ::Cell(input_cell, array_schema_).cell_size();

  // Write each logical cell to the array
  if(array_schema_->has_irregular_tiles()) { // Irregular tiles
    if(array_schema_->cell_order() == ArraySchema::CO_ROW_MAJOR ||
       array_schema_->cell_order() == ArraySchema::CO_COLUMN_MAJOR) {
      // Check if run must be flushed
      size_t size_cost = sizeof(Cell) + cell_size;
      if(run_size_ + size_cost > write_state_max_size_) {
        sort_run();
        flush_sorted_run();
      }
      // Create cell
      Cell new_cell;
      new_cell.cell_ = copy_cell(input_cell, cell_size);
      cells_.push_back(new_cell);
      run_size_ += size_cost;
    } else { // array_schema->cell_order() == ArraySchema::CO_HILBERT
      // Check if run must be flushed
      size_t size_cost = sizeof(CellWithId) + cell_size;

      if(run_size_ + size_cost > write_state_max_size_) {
        sort_run_with_id();
        flush_sorted_run_with_id();
      }
      // Create cell
      CellWithId new_cell;
      new_cell.cell_ = copy_cell(input_cell, cell_size);
      new_cell.id_ =
          array_schema_->cell_id_hilbert<T>
              (static_cast<const T*>(new_cell.cell_));

      cells_with_id_.push_back(new_cell);
      run_size_ += size_cost;
    }
  } else { // Regular tiles
    if(array_schema_->tile_order() == ArraySchema::TO_ROW_MAJOR) {
      if(array_schema_->cell_order() == ArraySchema::CO_ROW_MAJOR ||
         array_schema_->cell_order() == ArraySchema::CO_COLUMN_MAJOR) {
        // Check if run must be flushed
        size_t size_cost = sizeof(CellWithId) + cell_size;
        if(run_size_ + size_cost > write_state_max_size_) {
          sort_run_with_id();
          flush_sorted_run_with_id();
        }

        // Create cell
        CellWithId new_cell;
        new_cell.cell_ = copy_cell(input_cell, cell_size);
        new_cell.id_ = array_schema_->tile_id_row_major(new_cell.cell_);
        cells_with_id_.push_back(new_cell);
        run_size_ += size_cost;
      } else { // array_schema->cell_order() == ArraySchema::CO_HILBERT) {
        // Check if run must be flushed
        size_t size_cost = sizeof(CellWith2Ids) + cell_size;
        if(run_size_ + size_cost > write_state_max_size_) {
          sort_run_with_2_ids();
          flush_sorted_run_with_2_ids();
        }
        // Create cell
        CellWith2Ids new_cell;
        new_cell.cell_ = copy_cell(input_cell, cell_size);
        new_cell.tile_id_ = array_schema_->tile_id_row_major(new_cell.cell_);
        new_cell.cell_id_ =
          array_schema_->cell_id_hilbert<T>
            (static_cast<const T*>(new_cell.cell_));
        cells_with_2_ids_.push_back(new_cell);
        run_size_ += size_cost;
      }
    } else if(array_schema_->tile_order() == ArraySchema::TO_COLUMN_MAJOR) {
      if(array_schema_->cell_order() == ArraySchema::CO_ROW_MAJOR ||
         array_schema_->cell_order() == ArraySchema::CO_COLUMN_MAJOR) {
        // Check if run must be flushed
        size_t size_cost = sizeof(CellWithId) + cell_size;
        if(run_size_ + size_cost > write_state_max_size_) {
          sort_run_with_id();
          flush_sorted_run_with_id();
        }
        // Create cell
        CellWithId new_cell;
        new_cell.cell_ = copy_cell(input_cell, cell_size);
        new_cell.id_ = array_schema_->tile_id_column_major(new_cell.cell_);
        cells_with_id_.push_back(new_cell);
        run_size_ += size_cost;
      } else { // array_schema->cell_order() == ArraySchema::CO_HILBERT) {
        // Check if run must be flushed
        size_t size_cost = sizeof(CellWith2Ids) + cell_size;
        if(run_size_ + size_cost > write_state_max_size_) {
          sort_run_with_2_ids();
          flush_sorted_run_with_2_ids();
        }
        // Create cell
        CellWith2Ids new_cell;
        new_cell.cell_ = copy_cell(input_cell, cell_size);
        new_cell.tile_id_ = array_schema_->tile_id_column_major(new_cell.cell_);
        new_cell.cell_id_ =
              array_schema_->cell_id_hilbert<T>
                (static_cast<const T*>(new_cell.cell_));
        cells_with_2_ids_.push_back(new_cell);
        run_size_ += size_cost;
      }
    } else if(array_schema_->tile_order() == ArraySchema::TO_HILBERT) {
      if(array_schema_->cell_order() == ArraySchema::CO_ROW_MAJOR ||
         array_schema_->cell_order() == ArraySchema::CO_COLUMN_MAJOR) {
        // Check if run must be flushed
        size_t size_cost = sizeof(CellWithId) + cell_size;
        if(run_size_ + size_cost > write_state_max_size_) {
          sort_run_with_id();
          flush_sorted_run_with_id();
        }
        // Create cell
        CellWithId new_cell;
        new_cell.cell_ = copy_cell(input_cell, cell_size);
        new_cell.id_ = array_schema_->tile_id_hilbert(new_cell.cell_);
        cells_with_id_.push_back(new_cell);
        run_size_ += size_cost;
      } else { // array_schema->cell_order() == ArraySchema::CO_HILBERT) {
        // Check if run must be flushed
        size_t size_cost = sizeof(CellWith2Ids) + cell_size;
        if(run_size_ + size_cost > write_state_max_size_) {
          sort_run_with_2_ids();
          flush_sorted_run_with_2_ids();
        }
        // Create cell
        CellWith2Ids new_cell;
        new_cell.cell_ = copy_cell(input_cell, cell_size);
        new_cell.tile_id_ = array_schema_->tile_id_hilbert(new_cell.cell_);
        new_cell.cell_id_ =
          array_schema_->cell_id_hilbert<T>
            (static_cast<const T*>(new_cell.cell_));
        cells_with_2_ids_.push_back(new_cell);
        run_size_ += size_cost;
      }
    }
  }

  return 0;
}

template<class T>
int WriteState::cell_write_sorted(
    const void* cell, 
    bool without_coords) {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  size_t coords_size = array_schema_->coords_size();
  bool regular = array_schema_->has_regular_tiles();
  const char* c_cell = static_cast<const char*>(cell);
  int64_t tile_id;
  size_t cell_offset, attr_size;
  int64_t capacity; // only for irregular tiles
  std::vector<size_t> attr_sizes;

  if(dense_ && (current_coords_ == NULL)) { 
    current_coords_ = malloc(coords_size);
    array_schema_->get_domain_start<T>(
        static_cast<T*>(current_coords_),  
        NULL); 
    in_domain_ = true;
    array_schema_->init_zero_cell(zero_cell_, false);
  }
  const char* z_cell = static_cast<const char*>(zero_cell_);

  if(dense_) { // DENSE
    if(!without_coords) { // WITH COORDS
      // Store zero cells
      while(in_domain_ && !array_schema_->coords_match(
                 static_cast<const T*>(current_coords_),
                 static_cast<const T*>(cell))) {
        tile_id = array_schema_->tile_id(static_cast<const T*>(current_coords_));

        if(tile_id != tile_id_)
          flush_tile_info_to_book_keeping();

        cell_offset = 0;
        if(array_schema_->cell_size() == VAR_SIZE)
          cell_offset += sizeof(size_t);

        // Append attribute values to the respective segments
        attr_sizes.clear();
        for(int i=0; i<attribute_num; ++i) {
          append_attribute_value(z_cell + cell_offset, i, attr_size);
          cell_offset += attr_size;
          attr_sizes.push_back(attr_size);
        }
        attr_sizes.push_back(coords_size);

        update_tile_info<T>( 
            static_cast<const T*>(current_coords_), 
            tile_id, attr_sizes);

        in_domain_ = array_schema_->advance_coords<T>(
            static_cast<T*>(current_coords_), 
            NULL);
      }
      // Store the input cell
      // Initialization
      tile_id = array_schema_->tile_id(static_cast<const T*>(cell));

      // Flush tile info to book-keeping if a new tile must be created
      if(tile_id != tile_id_)
        flush_tile_info_to_book_keeping();

      // Skip coordinates
      cell_offset = coords_size;

      if(array_schema_->cell_size() == VAR_SIZE)
        cell_offset += sizeof(size_t);

      // Append attribute values to the respective segments
      attr_sizes.clear();
      for(int i=0; i<attribute_num; ++i) {
        append_attribute_value(c_cell + cell_offset, i, attr_size);
        cell_offset += attr_size;
        attr_sizes.push_back(attr_size);
      }
      attr_sizes.push_back(coords_size);

      // Update the info of the currently populated tile
      update_tile_info<T>(static_cast<const T*>(cell), tile_id, attr_sizes);

      in_domain_ = array_schema_->advance_coords<T>(
          static_cast<T*>(current_coords_), 
          NULL);
    } else { // WITHOUT COORDS
      tile_id = array_schema_->tile_id(static_cast<const T*>(current_coords_));
      if(tile_id != tile_id_)
        flush_tile_info_to_book_keeping();

      cell_offset = 0;
      if(array_schema_->cell_size() == VAR_SIZE)
        cell_offset += sizeof(size_t);

      // Append attribute values to the respective segments
      attr_sizes.clear();
      for(int i=0; i<attribute_num; ++i) {
        append_attribute_value(c_cell + cell_offset, i, attr_size);
        cell_offset += attr_size;
        attr_sizes.push_back(attr_size);
      }
      attr_sizes.push_back(coords_size);

      update_tile_info<T>( 
          static_cast<const T*>(current_coords_), 
          tile_id, attr_sizes);

      in_domain_ = array_schema_->advance_coords<T>(
          static_cast<T*>(current_coords_), 
          NULL);

      if(!in_domain_)
        flush_tile_info_to_book_keeping();
    }
  } else { // SPARSE
    // Initialization
    if(regular)
      tile_id = array_schema_->tile_id(static_cast<const T*>(cell));
    else
      capacity = array_schema_->capacity();

    // Flush tile info to book-keeping if a new tile must be created
    if((regular && tile_id != tile_id_) ||
       (!regular && (cell_num_ == capacity))) 
      flush_tile_info_to_book_keeping();

    // Append coordinates to segment
    append_coordinates(c_cell);

    cell_offset = coords_size;
    if(array_schema_->cell_size() == VAR_SIZE)
      cell_offset += sizeof(size_t);

    // Append attribute values to the respective segments
    for(int i=0; i<attribute_num; ++i) {
      append_attribute_value(c_cell + cell_offset, i, attr_size);
      cell_offset += attr_size;
      attr_sizes.push_back(attr_size);
    }
    attr_sizes.push_back(coords_size);

    // Update the info of the currently populated tile
    update_tile_info<T>(static_cast<const T*>(cell), tile_id, attr_sizes);
  }

  return 0;
}

template<class T>
int WriteState::cell_write_sorted_with_id(
    const void* cell) {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  size_t coords_size = array_schema_->cell_size(attribute_num);
  bool regular = array_schema_->has_regular_tiles();
  int64_t id = *(static_cast<const int64_t*>(cell));
  const void* coords = static_cast<const char*>(cell) + sizeof(int64_t);
  const char* c_cell = static_cast<const char*>(coords);
  size_t cell_offset, attr_size;
  std::vector<size_t> attr_sizes;

  if(dense_ && (current_coords_ == NULL)) { 
    current_coords_ = malloc(coords_size);
    array_schema_->get_domain_start<T>(
        static_cast<T*>(current_coords_),  
        NULL); 
    in_domain_ = true;
    array_schema_->init_zero_cell(zero_cell_, false);
  }
  const char* z_cell = static_cast<const char*>(zero_cell_);

  if(dense_) { // DENSE
    // Store zero cells
    while(!array_schema_->coords_match(
               static_cast<const T*>(current_coords_),
               static_cast<const T*>(coords))) {
      int64_t tile_id = array_schema_->tile_id(static_cast<const T*>(current_coords_));
      if(tile_id != tile_id_)
        flush_tile_info_to_book_keeping();

      cell_offset = 0;
      if(array_schema_->cell_size() == VAR_SIZE)
        cell_offset += sizeof(size_t);

      // Append attribute values to the respective segments
      attr_sizes.clear();
      for(int i=0; i<attribute_num; ++i) {
        append_attribute_value(z_cell + cell_offset, i, attr_size);
        cell_offset += attr_size;
        attr_sizes.push_back(attr_size);
      }
      attr_sizes.push_back(coords_size);

      update_tile_info<T>( 
          static_cast<const T*>(current_coords_), 
          tile_id, attr_sizes);

      in_domain_ = array_schema_->advance_coords<T>(
          static_cast<T*>(current_coords_), 
          NULL);
    }
    // Store the input cell

    // Flush tile info to book-keeping if a new tile must be created
    if(id != tile_id_)
      flush_tile_info_to_book_keeping();

    // Skip coordinates
    cell_offset = coords_size;

    if(array_schema_->cell_size() == VAR_SIZE)
      cell_offset += sizeof(size_t);

    // Append attribute values to the respective segments
    attr_sizes.clear();
    for(int i=0; i<attribute_num; ++i) {
      append_attribute_value(c_cell + cell_offset, i, attr_size);
      cell_offset += attr_size;
      attr_sizes.push_back(attr_size);
    }
    attr_sizes.push_back(coords_size);

    // Update the info of the currently populated tile
    update_tile_info<T>(static_cast<const T*>(coords), id, attr_sizes);

    in_domain_ = array_schema_->advance_coords<T>(
        static_cast<T*>(current_coords_), 
        NULL);

  } else { // SPARSE
    // Flush tile info to book-keeping if a new tile must be created
    if((regular && id != tile_id_) ||
       (!regular && (cell_num_ == array_schema_->capacity()))) 
      flush_tile_info_to_book_keeping();

    // Append coordinates to segment
    append_coordinates(c_cell);
    cell_offset = coords_size;
    if(array_schema_->cell_size() == VAR_SIZE)
      cell_offset += sizeof(size_t);

    // Append attribute values to the respective segments
    for(int i=0; i<attribute_num; ++i) {
      append_attribute_value(c_cell + cell_offset, i, attr_size);
      cell_offset += attr_size;
      attr_sizes.push_back(attr_size);
    }
    attr_sizes.push_back(coords_size);

    // Update the info of the currently populated tile
    update_tile_info<T>(static_cast<const T*>(coords), id, attr_sizes);
  }

  return 0;
}

template<class T>
int WriteState::cell_write_sorted_with_2_ids(
    const void* cell) {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  size_t coords_size = array_schema_->cell_size(attribute_num);
  int64_t id = *(static_cast<const int64_t*>(cell));
  const void* coords = static_cast<const char*>(cell) + 2*sizeof(int64_t);
  const char* c_cell = static_cast<const char*>(coords);
  size_t cell_offset, attr_size;
  std::vector<size_t> attr_sizes;

  if(dense_ && (current_coords_ == NULL)) { 
    current_coords_ = malloc(coords_size);
    array_schema_->get_domain_start<T>(
        static_cast<T*>(current_coords_),  
        NULL); 
    in_domain_ = true;
    array_schema_->init_zero_cell(zero_cell_, false);
  }
  const char* z_cell = static_cast<const char*>(zero_cell_);

  if(dense_) { // DENSE
    // Store zero cells
    while(!array_schema_->coords_match(
               static_cast<const T*>(current_coords_),
               static_cast<const T*>(coords))) {
      int64_t tile_id = array_schema_->tile_id(static_cast<const T*>(current_coords_));
      if(tile_id != tile_id_)
        flush_tile_info_to_book_keeping();

      cell_offset = 0;
      if(array_schema_->cell_size() == VAR_SIZE)
        cell_offset += sizeof(size_t);

      // Append attribute values to the respective segments
      attr_sizes.clear();
      for(int i=0; i<attribute_num; ++i) {
        append_attribute_value(z_cell + cell_offset, i, attr_size);
        cell_offset += attr_size;
        attr_sizes.push_back(attr_size);
      }
      attr_sizes.push_back(coords_size);

      update_tile_info<T>( 
          static_cast<const T*>(current_coords_), 
          tile_id, attr_sizes);

      in_domain_ = array_schema_->advance_coords<T>(
          static_cast<T*>(current_coords_), 
          NULL);
    }
    // Store the input cell

    // Flush tile info to book-keeping if a new tile must be created
    if(id != tile_id_)
      flush_tile_info_to_book_keeping();

    // Skip coordinates
    cell_offset = coords_size;

    if(array_schema_->cell_size() == VAR_SIZE)
      cell_offset += sizeof(size_t);

    // Append attribute values to the respective segments
    attr_sizes.clear();
    for(int i=0; i<attribute_num; ++i) {
      append_attribute_value(c_cell + cell_offset, i, attr_size);
      cell_offset += attr_size;
      attr_sizes.push_back(attr_size);
    }
    attr_sizes.push_back(coords_size);

    // Update the info of the currently populated tile
    update_tile_info<T>(static_cast<const T*>(coords), id, attr_sizes);

    in_domain_ = array_schema_->advance_coords<T>(
        static_cast<T*>(current_coords_), 
        NULL);

  } else { // SPARSE
    // Flush tile info to book-keeping if a new tile must be created
    if(id != tile_id_)
      flush_tile_info_to_book_keeping();

    // Append coordinates to segment
    append_coordinates(c_cell);
    cell_offset = coords_size;
    if(array_schema_->cell_size() == VAR_SIZE)
      cell_offset += sizeof(size_t);

    // Append attribute values to the respective segments
    for(int i=0; i<attribute_num; ++i) {
      append_attribute_value(c_cell + cell_offset, i, attr_size);
      cell_offset += attr_size;
      attr_sizes.push_back(attr_size);
    }
    attr_sizes.push_back(coords_size);

    // Update the info of the currently populated tile
    update_tile_info<T>(static_cast<const T*>(coords), id, attr_sizes);
  }

  return 0;
}

/******************************************************
****************** PRIVATE METHODS ********************
******************************************************/

void WriteState::append_attribute_value(
    const char* attr, int attribute_id, size_t& attr_size) {
  // For easy reference
  CompressionType compression = 
      array_schema_->compression(attribute_id);

  if(compression == CMP_NONE) 
    append_attribute_value_to_segment(attr, attribute_id, attr_size);
  else if(compression == CMP_GZIP) 
    append_attribute_value_to_gz_segment(attr, attribute_id, attr_size);
  else
    assert(0); // The code must never reach this point 
}

void WriteState::append_attribute_value_to_gz_segment(
    const char* attr, int attribute_id, size_t& attr_size) {
  // For easy reference
  bool var_size = (array_schema_->cell_size(attribute_id) == VAR_SIZE);

  if(!var_size) {
    attr_size = array_schema_->cell_size(attribute_id);
  } else {
    int val_num;
    memcpy(&val_num, attr, sizeof(int));
    attr_size = val_num * array_schema_->type_size(attribute_id) + sizeof(int);
  }

  // Recall that a gz_segment holds only the cells of a single tile, and
  // we assume that a tile can fit in a single gz_segment
  assert(gz_segment_utilization_[attribute_id] + attr_size <= segment_size_);

  // Append cell to the gz_segment
  memcpy(static_cast<char*>(gz_segments_[attribute_id]) +
         gz_segment_utilization_[attribute_id], attr, attr_size);
  gz_segment_utilization_[attribute_id] += attr_size;
}

void WriteState::append_attribute_value_to_segment(
    const char* attr, int attribute_id, size_t& attr_size) {
  // For easy reference
  bool var_size = (array_schema_->cell_size(attribute_id) == VAR_SIZE);

  if(!var_size) {
    attr_size = array_schema_->cell_size(attribute_id);
  } else {
    int val_num;
    memcpy(&val_num, attr, sizeof(int));
    attr_size = val_num * array_schema_->type_size(attribute_id) + sizeof(int);
  }

  // Check if the segment is full
  if(segment_utilization_[attribute_id] + attr_size > segment_size_)
    flush_segment(attribute_id);

  // Append cell to the segment
  memcpy(static_cast<char*>(segments_[attribute_id]) +
         segment_utilization_[attribute_id], attr, attr_size);
  segment_utilization_[attribute_id] += attr_size;
}

void WriteState::append_coordinates(const char* cell) {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  CompressionType compression = 
      array_schema_->compression(attribute_num);

  if(compression == CMP_NONE) 
    append_coordinates_to_segment(cell);
  else if(compression == CMP_GZIP)
    append_coordinates_to_gz_segment(cell);
  else
    assert(0); // The code must never reach this point 
}

void WriteState::append_coordinates_to_gz_segment(
    const char* cell) {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  bool var_size = (array_schema_->cell_size() == VAR_SIZE);
  size_t coords_size = array_schema_->cell_size(attribute_num);

  // Recall that a gz_segment holds only the cells of a single tile, and
  // we assume that a tile can fit in a single gz_segment
  assert(gz_segment_utilization_[attribute_num] + coords_size <= segment_size_);

  // Append the coordinates
  memcpy(static_cast<char*>(gz_segments_[attribute_num]) +
         gz_segment_utilization_[attribute_num], cell, coords_size);
  gz_segment_utilization_[attribute_num] += coords_size;
}

void WriteState::append_coordinates_to_segment(
    const char* cell) {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  bool var_size = (array_schema_->cell_size() == VAR_SIZE);
  size_t coords_size = array_schema_->cell_size(attribute_num);

  // Check if the segment is full
  if(segment_utilization_[attribute_num] + coords_size > segment_size_)
    flush_segment(attribute_num);

  // Append cell to the segment
  memcpy(static_cast<char*>(segments_[attribute_num]) +
         segment_utilization_[attribute_num], cell, coords_size);
  segment_utilization_[attribute_num] += coords_size;
}

void* WriteState::copy_cell(const void* cell, size_t cell_size) {
  int buffers_num = buffers_.size();

  // First copy
  if(buffers_num == 0) {
    size_t new_size = BUFFER_INITIAL_SIZE;
    while(cell_size > new_size)
      new_size *= 2;
    if(new_size > write_state_max_size_)
      new_size = write_state_max_size_;
    assert(cell_size < write_state_max_size_);
    void* new_buffer = malloc(new_size);
    buffers_.push_back(new_buffer);
    ++buffers_num;
    buffers_sizes_.push_back(new_size);
    buffers_utilization_.push_back(0);
  }

  size_t total_buffer_size = 0;
  for(int i=0; i<buffers_num; ++i)
    total_buffer_size += buffers_sizes_[i];

  // Buffer cannot hold the cell - new buffer allocation
  if(buffers_utilization_.back() + cell_size > buffers_sizes_.back()) {
    size_t new_size = buffers_sizes_.back() * 2;
    while(cell_size > new_size)
      new_size *= 2;
    if(new_size > write_state_max_size_ - total_buffer_size)
      new_size = write_state_max_size_ - total_buffer_size;
    assert(cell_size < new_size);
    void* new_buffer = malloc(new_size);
    buffers_.push_back(new_buffer);
    ++buffers_num;
    buffers_sizes_.push_back(new_size);
    buffers_utilization_.push_back(0);
  }

  // Copy cell to buffer
  void* pointer_in_buffer = static_cast<char*>(buffers_.back()) +
                            buffers_utilization_.back();
  memcpy(pointer_in_buffer, cell, cell_size);
  buffers_utilization_.back() += cell_size;

  return pointer_in_buffer;
}

void WriteState::finalize_last_run() {
  if(cells_.size() > 0) {
    sort_run();
    flush_sorted_run();
  } else if(cells_with_id_.size() > 0) {
    sort_run_with_id();
    flush_sorted_run_with_id();
  } else if(cells_with_2_ids_.size() > 0) {
    sort_run_with_2_ids();
    flush_sorted_run_with_2_ids();
  }

  cells_.clear();
  cells_with_id_.clear();
  cells_with_2_ids_.clear();
}

void WriteState::flush_gz_segment_to_segment(
    int attribute_id, size_t& flushed) {
  assert(array_schema_->compression(attribute_id) == CMP_GZIP);

  // Compress
  size_t compressed_data_size;
  gzip(static_cast<unsigned char*>(gz_segments_[attribute_id]), 
       gz_segment_utilization_[attribute_id], 
       static_cast<unsigned char*>(gz_buffer_),
       segment_size_, 
       compressed_data_size);

  // Check if the segment is full
  if(segment_utilization_[attribute_id] + compressed_data_size + sizeof(size_t)
         > segment_size_)
    flush_segment(attribute_id);

  // Append compressed data to the segment
  memcpy(static_cast<char*>(segments_[attribute_id]) +
             segment_utilization_[attribute_id], 
         &gz_segment_utilization_[attribute_id],
         sizeof(size_t));
  memcpy(static_cast<char*>(segments_[attribute_id]) +
             segment_utilization_[attribute_id] + sizeof(size_t), 
         gz_buffer_, 
         compressed_data_size);
  flushed = compressed_data_size + sizeof(size_t); // Return value
  segment_utilization_[attribute_id] += flushed;

  // Reset utilization
  gz_segment_utilization_[attribute_id] = 0;
}

void WriteState::flush_segment(int attribute_id) {
  // Exit if the segment has no useful data
  if(segment_utilization_[attribute_id] == 0)
    return;

  // Open file
  std::string filename = *dirname_ + "/" +
                         array_schema_->attribute_name(attribute_id) +
                         TILE_DATA_FILE_SUFFIX;

  int fd = open(filename.c_str(), O_WRONLY | O_APPEND | O_CREAT | O_SYNC,
                S_IRWXU);
  assert(fd != -1);

  // Retrieve the current file offset (equal to the file size)
  struct stat st;
  fstat(fd, &st);
  int64_t offset = st.st_size;

  // Append the segment to the file
  write(fd, segments_[attribute_id], segment_utilization_[attribute_id]);

  // Update the write state
  segment_utilization_[attribute_id] = 0;

  // Clean up
  close(fd);
}

void WriteState::flush_segments() {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();

  // Store the info of the lastly populated tile
  flush_tile_info_to_book_keeping();

  // Flush the segments
  for(int i=0; i<=attribute_num; ++i) {
    flush_segment(i);
    free(segments_[i]);
    segments_[i] = NULL;
  }
}

void WriteState::flush_sorted_run() {

  // Prepare BIN file
  std::stringstream filename;
  filename << *temp_dirname_ << "/" << runs_num_;
  // TODO: The runs could be gzipped. This could go in the config file
  // filename << *temp_dirname_ << runs_num_ << GZIP_SUFFIX;

  BINFile file(array_schema_, CMP_NONE, 0);
  file.open(filename.str(), "w", segment_size_);

  // Prepare cell
  ::Cell cell(array_schema_);

  // Write the cells into the file
  int64_t cell_num = cells_.size();

  for(int64_t i=0; i<cell_num; ++i) {
    cell.set_cell(cells_[i].cell_);
    file << cell;
  }

  // Clean up
  file.close();

  // Update write state
  for(int i=0; i<buffers_.size(); ++i)
    free(buffers_[i]);
  buffers_.clear();
  buffers_sizes_.clear();
  buffers_utilization_.clear();
  cells_.clear();
  cells_.reserve(cell_num);
  run_size_ = 0;
  ++runs_num_;
}

void WriteState::flush_sorted_run_with_id() {
  // Prepare BIN file
  std::stringstream filename;
  filename << *temp_dirname_ << "/" << runs_num_;
  // TODO: The runs could be gzipped. This could go in the config file
  // filename << *temp_dirname_ << runs_num_ << GZIP_SUFFIX;

  BINFile file(array_schema_, CMP_NONE, 1);
  file.open(filename.str(), "w", segment_size_);

  // Prepare cell
  ::Cell cell(array_schema_);

  // Write the cells into the file
  int64_t cell_num = cells_with_id_.size();
  for(int64_t i=0; i<cell_num; ++i) {
    file.write(&cells_with_id_[i].id_, sizeof(int64_t));
    cell.set_cell(cells_with_id_[i].cell_);
    file << cell;
  }

  // Clean up
  file.close();

  // Update write state
  for(int i=0; i<buffers_.size(); ++i)
    free(buffers_[i]);
  buffers_.clear();
  buffers_sizes_.clear();
  buffers_utilization_.clear();
  cells_with_id_.clear();
  cells_with_id_.reserve(cell_num);
  run_size_ = 0;
  ++runs_num_;
}

void WriteState::flush_sorted_run_with_2_ids() {
  // Prepare BIN file
  std::stringstream filename;
  filename << *temp_dirname_ << "/" << runs_num_;
  // TODO: The runs could be gzipped. This could go in the config file
  // filename << *temp_dirname_ << runs_num_ << GZIP_SUFFIX;

  BINFile file(array_schema_, CMP_NONE, 2);
  file.open(filename.str(), "w", segment_size_);

  // Prepare cell
  ::Cell cell(array_schema_);

  // Write the cells into the file
  int64_t cell_num = cells_with_2_ids_.size();
  for(int64_t i=0; i<cell_num; ++i) {
    file.write(&cells_with_2_ids_[i].tile_id_, sizeof(int64_t));
    file.write(&cells_with_2_ids_[i].cell_id_, sizeof(int64_t));
    cell.set_cell(cells_with_2_ids_[i].cell_);
    file << cell;
  }

  // Clean up
  file.close();

  // Update write state
  for(int i=0; i<buffers_.size(); ++i)
    free(buffers_[i]);
  buffers_.clear();
  buffers_sizes_.clear();
  buffers_utilization_.clear();
  cells_with_2_ids_.clear();
  cells_with_2_ids_.reserve(cell_num);
  run_size_ = 0;
  ++runs_num_;
}

void WriteState::flush_tile_info_to_book_keeping() {
  // Exit if there are no cells in the current tile
  if(cell_num_ == 0)
    return;

  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  size_t coords_size = array_schema_->cell_size(attribute_num);

  // Flush info
  for(int i=0; i<=attribute_num; ++i) {
    if(array_schema_->compression(i) == CMP_NONE) {
      book_keeping_->offsets_[i].push_back(file_offsets_[i]);
    } else { // Compress tile, flush to segment and update offset 
      size_t flushed;
      flush_gz_segment_to_segment(i, flushed);
      file_offsets_[i] += flushed;
      book_keeping_->offsets_[i].push_back(file_offsets_[i]);
    }
  }

  book_keeping_->bounding_coordinates_.push_back(bounding_coordinates_);
  book_keeping_->mbrs_.push_back(mbr_);
  cell_num_ = 0;

  // Nullify MBR and bounding coordinates
  mbr_ = NULL;
  bounding_coordinates_.first = NULL;
  bounding_coordinates_.second = NULL;
}

void WriteState::make_tiles(const std::string& dirname) {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  const std::type_info* coords_type = array_schema_->type(attribute_num);
  ArraySchema::TileOrder tile_order = array_schema_->tile_order();
  ArraySchema::CellOrder cell_order = array_schema_->cell_order();
  bool regular_tiles = array_schema_->has_regular_tiles();

  if(!regular_tiles && (cell_order == ArraySchema::CO_ROW_MAJOR ||
                        cell_order == ArraySchema::CO_COLUMN_MAJOR)) {
    // Cell
    if(*coords_type == typeid(int))
      make_tiles<int>(dirname);
    else if(*coords_type == typeid(int64_t))
      make_tiles<int64_t>(dirname);
    else if(*coords_type == typeid(float))
      make_tiles<float>(dirname);
    else if(*coords_type == typeid(double))
      make_tiles<double>(dirname);
  } else if((regular_tiles && (cell_order == ArraySchema::CO_ROW_MAJOR ||
                               cell_order == ArraySchema::CO_COLUMN_MAJOR)) ||
            (!regular_tiles && cell_order == ArraySchema::CO_HILBERT)) {
    // CellWithId
    if(*coords_type == typeid(int))
      make_tiles_with_id<int>(dirname);
    else if(*coords_type == typeid(int64_t))
      make_tiles_with_id<int64_t>(dirname);
    else if(*coords_type == typeid(float))
      make_tiles_with_id<float>(dirname);
    else if(*coords_type == typeid(double))
      make_tiles_with_id<double>(dirname);
  } else if(regular_tiles && cell_order == ArraySchema::CO_HILBERT) {
    // CellWith2Ids
    if(*coords_type == typeid(int))
      make_tiles_with_2_ids<int>(dirname);
    else if(*coords_type == typeid(int64_t))
      make_tiles_with_2_ids<int64_t>(dirname);
    else if(*coords_type == typeid(float))
      make_tiles_with_2_ids<float>(dirname);
    else if(*coords_type == typeid(double))
      make_tiles_with_2_ids<double>(dirname);
  }
}

// NOTE: This function applies only to irregular tiles
template<class T>
void WriteState::make_tiles(const std::string& dirname) {
  int id_num = 0;
  bool sorted = true;

  // Create a cell
  ::Cell cell(array_schema_, id_num);

  // Create a file collection
  BINFileCollection<T> bin_file_collection(CMP_NONE);
  bin_file_collection.open(array_schema_, id_num, dirname, sorted);

  // Loop over the cells
  while(bin_file_collection >> cell)
    // TODO: this should return a value
    cell_write_sorted<T>(cell.cell());

  // Store zero cells
  size_t cell_offset, attr_size;
  std::vector<size_t> attr_sizes;
  int attribute_num = array_schema_->attribute_num();
  size_t coords_size = array_schema_->coords_size();
  const char* z_cell = static_cast<const char*>(zero_cell_);
  while(in_domain_) {
    int64_t tile_id = array_schema_->tile_id(static_cast<const T*>(current_coords_));
    if(tile_id != tile_id_)
      flush_tile_info_to_book_keeping();

    cell_offset = 0;
      if(array_schema_->cell_size() == VAR_SIZE)
        cell_offset += sizeof(size_t);

    // Append attribute values to the respective segments
    attr_sizes.clear();
    for(int i=0; i<attribute_num; ++i) {
      append_attribute_value(z_cell + cell_offset, i, attr_size);
      cell_offset += attr_size;
      attr_sizes.push_back(attr_size);
    }
    attr_sizes.push_back(coords_size);

    update_tile_info<T>( 
        static_cast<const T*>(current_coords_), 
        tile_id, attr_sizes);

    in_domain_ = array_schema_->advance_coords<T>(
        static_cast<T*>(current_coords_), 
         NULL);
  }
}

// This function applies either to regular tiles with row- or column-major
// order, or irregular tiles with Hilbert order
template<class T>
void WriteState::make_tiles_with_id(const std::string& dirname) {
  int id_num = 1;
  bool sorted = true;

  // Create a cell
  ::Cell cell(array_schema_, id_num);

  // Create a file collection
  BINFileCollection<T> bin_file_collection(CMP_NONE);
  bin_file_collection.open(array_schema_, id_num, dirname, sorted);

  // Loop over the cells
  // TODO this should return a value
  if(array_schema_->has_regular_tiles()) {
    while(bin_file_collection >> cell)
      cell_write_sorted_with_id<T>(cell.cell());
  } else {
    while(bin_file_collection >> cell)
      cell_write_sorted<T>(static_cast<const char*>(cell.cell()) +
                           sizeof(int64_t));
  }

  // Store zero cells
  size_t cell_offset, attr_size;
  std::vector<size_t> attr_sizes;
  int attribute_num = array_schema_->attribute_num();
  size_t coords_size = array_schema_->coords_size();
  const char* z_cell = static_cast<const char*>(zero_cell_);
  while(in_domain_) {
    int64_t tile_id = array_schema_->tile_id(static_cast<const T*>(current_coords_));
    if(tile_id != tile_id_)
      flush_tile_info_to_book_keeping();

    cell_offset = 0;
      if(array_schema_->cell_size() == VAR_SIZE)
        cell_offset += sizeof(size_t);

    // Append attribute values to the respective segments
    attr_sizes.clear();
    for(int i=0; i<attribute_num; ++i) {
      append_attribute_value(z_cell + cell_offset, i, attr_size);
      cell_offset += attr_size;
      attr_sizes.push_back(attr_size);
    }
    attr_sizes.push_back(coords_size);

    update_tile_info<T>( 
        static_cast<const T*>(current_coords_), 
        tile_id, attr_sizes);

    in_domain_ = array_schema_->advance_coords<T>(
        static_cast<T*>(current_coords_), 
         NULL);
  }

}

// NOTE: This function applies only to regular tiles
template<class T>
void WriteState::make_tiles_with_2_ids(const std::string& dirname) {
  int id_num = 2;
  bool sorted = true;

  // Create a cell
  ::Cell cell(array_schema_, id_num);

  // Create a file collection
  BINFileCollection<T> bin_file_collection(CMP_NONE);
  bin_file_collection.open(array_schema_, id_num, dirname, sorted);

  // Loop over the cells
  // TODO: this should return a value
  while(bin_file_collection >> cell)
    cell_write_sorted<T>(cell.cell());

  // Store zero cells
  size_t cell_offset, attr_size;
  std::vector<size_t> attr_sizes;
  int attribute_num = array_schema_->attribute_num();
  size_t coords_size = array_schema_->coords_size();
  const char* z_cell = static_cast<const char*>(zero_cell_);

  while(in_domain_) {
    int64_t tile_id = array_schema_->tile_id(static_cast<const T*>(current_coords_));
    if(tile_id != tile_id_)
      flush_tile_info_to_book_keeping();

    cell_offset = 0;
      if(array_schema_->cell_size() == VAR_SIZE)
        cell_offset += sizeof(size_t);

    // Append attribute values to the respective segments
    attr_sizes.clear();
    for(int i=0; i<attribute_num; ++i) {
      append_attribute_value(z_cell + cell_offset, i, attr_size);
      cell_offset += attr_size;
      attr_sizes.push_back(attr_size);
    }
    attr_sizes.push_back(coords_size);

    update_tile_info<T>( 
        static_cast<const T*>(current_coords_), 
        tile_id, attr_sizes);

    in_domain_ = array_schema_->advance_coords<T>(
        static_cast<T*>(current_coords_), 
         NULL);
  }
}

void WriteState::sort_run() {
  // For easy reference
  int dim_num = array_schema_->dim_num();
  int attribute_num = array_schema_->attribute_num();
  const std::type_info* coords_type = array_schema_->type(attribute_num);
  ArraySchema::CellOrder cell_order = array_schema_->cell_order();

  // Sort the cells
  if(cell_order == ArraySchema::CO_ROW_MAJOR) {
    if(*coords_type == typeid(int)) {
      SORT(cells_.begin(), cells_.end(), SmallerRow<int>(dim_num));
    } else if(*coords_type == typeid(int64_t)) {
      SORT(cells_.begin(), cells_.end(), SmallerRow<int64_t>(dim_num));
    } else if(*coords_type == typeid(float)) {
      SORT(cells_.begin(), cells_.end(), SmallerRow<float>(dim_num));
    } else if(*coords_type == typeid(double)) {
      SORT(cells_.begin(), cells_.end(), SmallerRow<double>(dim_num));
    }
  } else if(cell_order == ArraySchema::CO_COLUMN_MAJOR) {
    if(*coords_type == typeid(int)) {
      SORT(cells_.begin(), cells_.end(), SmallerCol<int>(dim_num));
    } else if(*coords_type == typeid(int64_t)) {
      SORT(cells_.begin(), cells_.end(), SmallerCol<int64_t>(dim_num));
    } else if(*coords_type == typeid(float)) {
      SORT(cells_.begin(), cells_.end(), SmallerCol<float>(dim_num));
    } else if(*coords_type == typeid(double)) {
      SORT(cells_.begin(), cells_.end(), SmallerCol<double>(dim_num));
    }
  }
}

void WriteState::sort_run_with_id() {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  int dim_num = array_schema_->dim_num();
  const std::type_info* coords_type = array_schema_->type(attribute_num);
  ArraySchema::TileOrder tile_order = array_schema_->tile_order();
  ArraySchema::CellOrder cell_order = array_schema_->cell_order();

  if(tile_order == ArraySchema::TO_NONE || // Irregular + Hilbert co
     cell_order == ArraySchema::CO_ROW_MAJOR) { // Regular + row co
    if(*coords_type == typeid(int)) {
      SORT(cells_with_id_.begin(), cells_with_id_.end(),
                    SmallerRowWithId<int>(dim_num));
    } else if(*coords_type == typeid(int64_t)) {
      SORT(cells_with_id_.begin(), cells_with_id_.end(),
                    SmallerRowWithId<int64_t>(dim_num));
    } else if(*coords_type == typeid(float)) {
      SORT(cells_with_id_.begin(), cells_with_id_.end(),
                    SmallerRowWithId<float>(dim_num));
    } else if(*coords_type == typeid(double)) {
      SORT(cells_with_id_.begin(), cells_with_id_.end(),
                    SmallerRowWithId<double>(dim_num));
    }
  } else if(cell_order == ArraySchema::CO_COLUMN_MAJOR) { // Regular + col co
    if(*coords_type == typeid(int)) {
      SORT(cells_with_id_.begin(), cells_with_id_.end(),
                    SmallerColWithId<int>(dim_num));
    } else if(*coords_type == typeid(int64_t)) {
      SORT(cells_with_id_.begin(), cells_with_id_.end(),
                    SmallerColWithId<int64_t>(dim_num));
    } else if(*coords_type == typeid(float)) {
      SORT(cells_with_id_.begin(), cells_with_id_.end(),
                    SmallerColWithId<float>(dim_num));
    } else if(*coords_type == typeid(double)) {
      SORT(cells_with_id_.begin(), cells_with_id_.end(),
                    SmallerColWithId<double>(dim_num));
    }
  }
}

void WriteState::sort_run_with_2_ids() {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  int dim_num = array_schema_->dim_num();
  const std::type_info* coords_type = array_schema_->type(attribute_num);

  if(*coords_type == typeid(int)) {
    SORT(cells_with_2_ids_.begin(), cells_with_2_ids_.end(),
                  SmallerWith2Ids<int>(dim_num));
  } else if(*coords_type == typeid(int64_t)) {
    SORT(cells_with_2_ids_.begin(), cells_with_2_ids_.end(),
                  SmallerWith2Ids<int64_t>(dim_num));
  } else if(*coords_type == typeid(float)) {
    SORT(cells_with_2_ids_.begin(), cells_with_2_ids_.end(),
                  SmallerWith2Ids<float>(dim_num));
  } else if(*coords_type == typeid(double)) {
    SORT(cells_with_2_ids_.begin(), cells_with_2_ids_.end(),
                  SmallerWith2Ids<double>(dim_num));
  }
}

template<class T>
void WriteState::update_tile_info(
    const T* coords, int64_t tile_id,
    const std::vector<size_t>& attr_sizes) {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  int dim_num = array_schema_->dim_num();
  size_t coords_size = array_schema_->cell_size(attribute_num);

  // Update MBR and (potentially) the first bounding coordinate
  if(cell_num_ == 0) {
    // Allocate space for MBR and bounding coordinates
    mbr_ = malloc(2*array_schema_->cell_size(attribute_num));
    bounding_coordinates_.first =
        malloc(array_schema_->cell_size(attribute_num));
    bounding_coordinates_.second =
        malloc(array_schema_->cell_size(attribute_num));

    // Init MBR first bounding coordinate
    init_mbr(coords, static_cast<T*>(mbr_), dim_num);
    memcpy(bounding_coordinates_.first, coords, coords_size);
  } else {
    expand_mbr(coords, static_cast<T*>(mbr_), dim_num);
  }

  // Update the second bounding coordinate, tile id, and cell number
  memcpy(bounding_coordinates_.second, coords, coords_size);
  tile_id_ = tile_id;
  ++cell_num_;

  // Update file offsets
  for(int i=0; i<=attribute_num; ++i)
    if(array_schema_->compression(i) == CMP_NONE)
      file_offsets_[i] += attr_sizes[i];
}

// Explicit template instantiations
template int WriteState::cell_write<int>(const void* cell);
template int WriteState::cell_write<int64_t>(const void* cell);
template int WriteState::cell_write<float>(const void* cell);
template int WriteState::cell_write<double>(const void* cell);

