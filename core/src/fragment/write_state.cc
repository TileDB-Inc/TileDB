/**
 * @file   write_state.cc
 * @author Stavros Papadopoulos <stavrosp@csail.mit.edu>
 *
 * @section LICENSE
 *
 * The MIT License
 * 
 * @copyright Copyright (c) 2014 Stavros Papadopoulos <stavrosp@csail.mit.edu>
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
#include "write_state.h"
#include "utils.h"
#include <assert.h>
#include <fcntl.h>
#include <math.h>
#include <parallel/algorithm>
#include <sstream>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

/******************************************************
************* CONSTRUCTORS & DESTRUCTORS **************
******************************************************/

WriteState::WriteState(
    const ArraySchema* array_schema, 
    const std::string* fragment_name,
    const std::string* temp_dirname,
    const std::string* workspace,
    BookKeeping* book_keeping,
    size_t segment_size,
    size_t write_state_max_size) 
    : array_schema_(array_schema),
      fragment_name_(fragment_name),
      temp_dirname_(temp_dirname),
      workspace_(workspace),
      book_keeping_(book_keeping),
      segment_size_(segment_size),
      write_state_max_size_(write_state_max_size) {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();

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
  file_offsets_.resize(attribute_num+1);
  for(int i=0; i<= attribute_num; ++i) {
    segments_[i] = malloc(segment_size_); 
    segment_utilization_[i] = 0;
    file_offsets_[i] = 0;
  } 
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
  for(int i=0; i<=attribute_num; ++i) 
    free(segments_[i]); 

  // Clear MBR and bounding coordinates
  if(mbr_ != NULL) 
    free(mbr_);
  if(bounding_coordinates_.first != NULL) 
    free(bounding_coordinates_.first);
  if(bounding_coordinates_.second != NULL) 
    free(bounding_coordinates_.second);

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
void WriteState::write_cell(const void* input_cell) {
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
}

template<class T>
void WriteState::write_cell_sorted(const void* cell) {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  size_t coords_size = array_schema_->cell_size(attribute_num);
  bool regular = array_schema_->has_regular_tiles();
  const char* c_cell = static_cast<const char*>(cell);
  int64_t tile_id; 
  size_t cell_offset, attr_size;
  int64_t capacity; // only for irregular tiles
  std::vector<size_t> attr_sizes;

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
  append_coordinates_to_segment(c_cell);
  cell_offset = coords_size; 
  if(array_schema_->cell_size() == VAR_SIZE)
    cell_offset += sizeof(size_t);

  // Append attribute values to the respective segments
  for(int i=0; i<attribute_num; ++i) {
    append_attribute_to_segment(c_cell + cell_offset, i, attr_size);
    cell_offset += attr_size;
    attr_sizes.push_back(attr_size);
  }
  attr_sizes.push_back(coords_size);
    
  // Update the info of the currently populated tile
  update_tile_info<T>(static_cast<const T*>(cell), tile_id, attr_sizes);
}

template<class T>
void WriteState::write_cell_sorted_with_id(const void* cell) {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  size_t coords_size = array_schema_->cell_size(attribute_num);
  bool regular = array_schema_->has_regular_tiles();
  int64_t id = *(static_cast<const int64_t*>(cell));
  const void* coords = static_cast<const char*>(cell) + sizeof(int64_t);
  const char* c_cell = static_cast<const char*>(coords);
  size_t cell_offset, attr_size;
  std::vector<size_t> attr_sizes;

  // Flush tile info to book-keeping if a new tile must be created
  if((regular && id != tile_id_) || 
     (!regular && (cell_num_ == array_schema_->capacity())))
    flush_tile_info_to_book_keeping();

  // Append coordinates to segment  
  append_coordinates_to_segment(c_cell);
  cell_offset = coords_size; 
  if(array_schema_->cell_size() == VAR_SIZE)
    cell_offset += sizeof(size_t);

  // Append attribute values to the respective segments
  for(int i=0; i<attribute_num; ++i) {
    append_attribute_to_segment(c_cell + cell_offset, i, attr_size);
    cell_offset += attr_size;
    attr_sizes.push_back(attr_size);
  }
  attr_sizes.push_back(coords_size);
  
  // Update the info of the currently populated tile
  update_tile_info<T>(static_cast<const T*>(coords), id, attr_sizes);
}

template<class T>
void WriteState::write_cell_sorted_with_2_ids(const void* cell) {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  size_t coords_size = array_schema_->cell_size(attribute_num);
  int64_t id = *(static_cast<const int64_t*>(cell));
  const void* coords = static_cast<const char*>(cell) + 2*sizeof(int64_t);
  const char* c_cell = static_cast<const char*>(coords);
  size_t cell_offset, attr_size;
  std::vector<size_t> attr_sizes;

  // Flush tile info to book-keeping if a new tile must be created
  if(id != tile_id_)    
    flush_tile_info_to_book_keeping();

  // Append coordinates to segment  
  append_coordinates_to_segment(c_cell);
  cell_offset = coords_size; 
  if(array_schema_->cell_size() == VAR_SIZE)
    cell_offset += sizeof(size_t);

  // Append attribute values to the respective segments
  for(int i=0; i<attribute_num; ++i) {
    append_attribute_to_segment(c_cell + cell_offset, i, attr_size);
    cell_offset += attr_size;
    attr_sizes.push_back(attr_size);
  }
  attr_sizes.push_back(coords_size);

  // Update the info of the currently populated tile
  update_tile_info<T>(static_cast<const T*>(coords), id, attr_sizes);
}

/******************************************************
****************** PRIVATE METHODS ********************
******************************************************/

void WriteState::append_attribute_to_segment(
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

void WriteState::flush_segment(int attribute_id) {
  // Exit if the segment has no useful data
  if(segment_utilization_[attribute_id] == 0)
    return;

  // Open file
  std::string filename = *workspace_ + "/" + 
                         array_schema_->array_name() + "/" + 
                         *fragment_name_ + "/" + 
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
  filename << *temp_dirname_ << runs_num_;

  BINFile file(array_schema_, 0);
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
  BINFile file(array_schema_, 1);
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
  BINFile file(array_schema_, 2);
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
  for(int i=0; i<=attribute_num; ++i) 
    book_keeping_->offsets_[i].push_back(file_offsets_[i]);

  book_keeping_->bounding_coordinates_.push_back(bounding_coordinates_);
  book_keeping_->mbrs_.push_back(mbr_);
  book_keeping_->tile_ids_.push_back(tile_id_);
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
  BINFileCollection<T> bin_file_collection;
  bin_file_collection.open(array_schema_, id_num, dirname, sorted);

  // Loop over the cells
  while(bin_file_collection >> cell) 
    write_cell_sorted<T>(cell.cell());
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
  BINFileCollection<T> bin_file_collection;
  bin_file_collection.open(array_schema_, id_num, dirname, sorted);

  // Loop over the cells
  if(array_schema_->has_regular_tiles()) {
    while(bin_file_collection >> cell) 
      write_cell_sorted_with_id<T>(cell.cell());
  } else {
    while(bin_file_collection >> cell) 
      write_cell_sorted<T>(static_cast<const char*>(cell.cell()) + 
                           sizeof(int64_t));
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
  BINFileCollection<T> bin_file_collection;
  bin_file_collection.open(array_schema_, id_num, dirname, sorted);

  // Loop over the cells
  while(bin_file_collection >> cell) 
    write_cell_sorted<T>(cell.cell());
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
      std::sort(cells_.begin(), cells_.end(), SmallerRow<int>(dim_num));
    } else if(*coords_type == typeid(int64_t)) {
      std::sort(cells_.begin(), cells_.end(), SmallerRow<int64_t>(dim_num));
    } else if(*coords_type == typeid(float)) {
      std::sort(cells_.begin(), cells_.end(), SmallerRow<float>(dim_num));
    } else if(*coords_type == typeid(double)) {
      std::sort(cells_.begin(), cells_.end(), SmallerRow<double>(dim_num));
    }
  } else if(cell_order == ArraySchema::CO_COLUMN_MAJOR) {
    if(*coords_type == typeid(int)) {
      __gnu_parallel::sort(cells_.begin(), cells_.end(), SmallerCol<int>(dim_num));
    } else if(*coords_type == typeid(int64_t)) {
      __gnu_parallel::sort(cells_.begin(), cells_.end(), SmallerCol<int64_t>(dim_num));
    } else if(*coords_type == typeid(float)) {
      __gnu_parallel::sort(cells_.begin(), cells_.end(), SmallerCol<float>(dim_num));
    } else if(*coords_type == typeid(double)) {
      __gnu_parallel::sort(cells_.begin(), cells_.end(), SmallerCol<double>(dim_num));
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
      __gnu_parallel::sort(cells_with_id_.begin(), cells_with_id_.end(), 
                SmallerRowWithId<int>(dim_num));
    } else if(*coords_type == typeid(int64_t)) {
      __gnu_parallel::sort(cells_with_id_.begin(), 
                cells_with_id_.end(), 
                SmallerRowWithId<int64_t>(dim_num));
    } else if(*coords_type == typeid(float)) {
      __gnu_parallel::sort(cells_with_id_.begin(), 
                cells_with_id_.end(), 
                SmallerRowWithId<float>(dim_num));
    } else if(*coords_type == typeid(double)) {
      __gnu_parallel::sort(cells_with_id_.begin(), 
                cells_with_id_.end(), 
                SmallerRowWithId<double>(dim_num));
    }
  } else if(cell_order == ArraySchema::CO_COLUMN_MAJOR) { // Regular + col co
    if(*coords_type == typeid(int)) {
      __gnu_parallel::sort(cells_with_id_.begin(), 
                cells_with_id_.end(), 
                SmallerColWithId<int>(dim_num));
    } else if(*coords_type == typeid(int64_t)) {
      __gnu_parallel::sort(cells_with_id_.begin(), 
                cells_with_id_.end(), 
                SmallerColWithId<int64_t>(dim_num));
    } else if(*coords_type == typeid(float)) {
      __gnu_parallel::sort(cells_with_id_.begin(), 
                cells_with_id_.end(), 
                SmallerColWithId<float>(dim_num));
    } else if(*coords_type == typeid(double)) {
      __gnu_parallel::sort(cells_with_id_.begin(), 
                cells_with_id_.end(), 
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
    __gnu_parallel::sort(cells_with_2_ids_.begin(), 
              cells_with_2_ids_.end(), 
              SmallerWith2Ids<int>(dim_num));
  } else if(*coords_type == typeid(int64_t)) {
    __gnu_parallel::sort(cells_with_2_ids_.begin(), 
              cells_with_2_ids_.end(), 
              SmallerWith2Ids<int64_t>(dim_num));
  } else if(*coords_type == typeid(float)) {
    __gnu_parallel::sort(cells_with_2_ids_.begin(), 
              cells_with_2_ids_.end(), 
              SmallerWith2Ids<float>(dim_num));
  } else if(*coords_type == typeid(double)) {
    __gnu_parallel::sort(cells_with_2_ids_.begin(), 
              cells_with_2_ids_.end(), 
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
    file_offsets_[i] += attr_sizes[i];
}

// Explicit template instantiations
template void WriteState::write_cell<int>(const void* cell);
template void WriteState::write_cell<int64_t>(const void* cell);
template void WriteState::write_cell<float>(const void* cell);
template void WriteState::write_cell<double>(const void* cell);

