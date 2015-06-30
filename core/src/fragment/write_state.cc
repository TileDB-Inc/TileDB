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

#include "cell.h"
#include "write_state.h"
#include "utils.h"
#include <algorithm>
#include <assert.h>
#include <fcntl.h>
#include <math.h>
#include <sstream>
#include <string.h>
#include <sys/stat.h>
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

  // Merge runs
  merge_sorted_runs(*temp_dirname_);
  make_tiles(*temp_dirname_);
  flush_segments();
}

void WriteState::load_sorted_bin(const std::string& dirname) {
  assert(is_dir(dirname));

  // Merge sorted files
  bool merged = merge_sorted_runs(dirname);
  
  // Make tiles
  if(merged)
    make_tiles(*temp_dirname_);
  else 
    make_tiles(dirname);
}

template<class T>
void WriteState::write_cell(const void* input_cell) {
  // For easy reference
  int dim_num = array_schema_->dim_num();

  // Find cell size
  size_t cell_size = ::Cell(input_cell, array_schema_).cell_size();

  // Copy the input cell
  void* cell = malloc(cell_size);
  memcpy(cell, input_cell, cell_size);

  // Write each logical cell to the array
  if(array_schema_->has_irregular_tiles()) { // Irregular tiles
    if(array_schema_->cell_order() == ArraySchema::CO_ROW_MAJOR ||
       array_schema_->cell_order() == ArraySchema::CO_COLUMN_MAJOR) {
      Cell new_cell;
      new_cell.cell_ = cell;
      write_cell(new_cell, cell_size); 
    } else { // array_schema->cell_order() == ArraySchema::CO_HILBERT
      CellWithId new_cell;
      new_cell.cell_ = cell;
      new_cell.id_ = 
          array_schema_->cell_id_hilbert<T>(static_cast<const T*>(cell));
      write_cell(new_cell, cell_size); 
    }
  } else { // Regular tiles
    if(array_schema_->tile_order() == ArraySchema::TO_ROW_MAJOR) { 
      if(array_schema_->cell_order() == ArraySchema::CO_ROW_MAJOR ||
         array_schema_->cell_order() == ArraySchema::CO_COLUMN_MAJOR) {
        CellWithId new_cell;
        new_cell.cell_ = cell;
        new_cell.id_ = array_schema_->tile_id_row_major(cell);
        write_cell(new_cell, cell_size); 
      } else { // array_schema->cell_order() == ArraySchema::CO_HILBERT) {
        CellWith2Ids new_cell;
        new_cell.cell_ = cell;
        new_cell.tile_id_ = array_schema_->tile_id_row_major(cell);
        new_cell.cell_id_ = 
          array_schema_->cell_id_hilbert<T>(static_cast<const T*>(cell));
        write_cell(new_cell, cell_size); 
      }
    } else if(array_schema_->tile_order() == ArraySchema::TO_COLUMN_MAJOR) { 
      if(array_schema_->cell_order() == ArraySchema::CO_ROW_MAJOR ||
         array_schema_->cell_order() == ArraySchema::CO_COLUMN_MAJOR) {
        CellWithId new_cell;
        new_cell.cell_ = cell;
        new_cell.id_ = array_schema_->tile_id_column_major(cell);
        write_cell(new_cell, cell_size); 
      } else { // array_schema->cell_order() == ArraySchema::CO_HILBERT) {
        CellWith2Ids new_cell;
        new_cell.cell_ = cell;
        new_cell.tile_id_ = array_schema_->tile_id_column_major(cell);
        new_cell.cell_id_ = 
          array_schema_->cell_id_hilbert<T>(static_cast<const T*>(cell));
        write_cell(new_cell, cell_size); 
      }
    } else if(array_schema_->tile_order() == ArraySchema::TO_HILBERT) { 
      if(array_schema_->cell_order() == ArraySchema::CO_ROW_MAJOR ||
         array_schema_->cell_order() == ArraySchema::CO_COLUMN_MAJOR) {
        CellWithId new_cell;
        new_cell.cell_ = cell;
        new_cell.id_ = array_schema_->tile_id_hilbert(cell);
        write_cell(new_cell, cell_size); 
      } else { // array_schema->cell_order() == ArraySchema::CO_HILBERT) {
        CellWith2Ids new_cell;
        new_cell.cell_ = cell;
        new_cell.tile_id_ = array_schema_->tile_id_hilbert(cell);
        new_cell.cell_id_ = 
          array_schema_->cell_id_hilbert<T>(static_cast<const T*>(cell));
        write_cell(new_cell, cell_size); 
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
  if(!regular)
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
  // For easy reference
  size_t cell_size = array_schema_->cell_size();
  bool var_size = (cell_size == VAR_SIZE);
  int attribute_num = array_schema_->attribute_num();
  size_t coords_size = array_schema_->cell_size(attribute_num);

  // Prepare file
  std::stringstream filename;
  filename << *temp_dirname_ << runs_num_;
  remove(filename.str().c_str());
  int file = open(filename.str().c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  assert(file != -1);

  // Write the cells into the file 
  char* segment = new char[segment_size_];
  size_t offset = 0; 
  int64_t cell_num = cells_.size();
  char* cell;

  for(int64_t i=0; i<cell_num; ++i) {
    // For easy reference
    cell = static_cast<char*>(cells_[i].cell_);

    // If variable-sized, calculate size
    if(var_size) 
      memcpy(&cell_size, cell + coords_size, sizeof(size_t));

    // Flush the segment to the file
    if(offset + cell_size > segment_size_) { 
      write(file, segment, offset);
      offset = 0;
    }

    // Write cell to segment
    memcpy(segment + offset, cell, cell_size);
    offset += cell_size;
  }

  if(offset != 0) {
    write(file, segment, offset);
    offset = 0;
  }

  // Clean up
  close(file);
  delete [] segment;

  // Update write state
  for(int64_t i=0; i<cell_num; ++i)
    free(cells_[i].cell_);
  cells_.clear();
  run_size_ = 0;
  ++runs_num_;
}

void WriteState::flush_sorted_run_with_id() {
  // For easy reference
  size_t cell_size = array_schema_->cell_size();
  bool var_size = (cell_size == VAR_SIZE);
  int attribute_num = array_schema_->attribute_num();
  size_t coords_size = array_schema_->cell_size(attribute_num);

  // Prepare file
  std::stringstream filename;
  filename << *temp_dirname_ << runs_num_;
  remove(filename.str().c_str());
  int file = open(filename.str().c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  assert(file != -1);

  // Write the cells into the file 
  char* buffer = new char[segment_size_];
  size_t buffer_offset = 0; 
  int64_t cell_num = cells_with_id_.size();
  char* cell;

  for(int64_t i=0; i<cell_num; ++i) {
    // For easy reference
    cell = static_cast<char*>(cells_with_id_[i].cell_);

    // If variable-sized, calculate size
    if(var_size) 
      memcpy(&cell_size, cell + coords_size, sizeof(size_t));

    // Flush the segment to the file
    if(buffer_offset + sizeof(int64_t) + cell_size > segment_size_) { 
      write(file, buffer, buffer_offset);
      buffer_offset = 0;
    }

    // Write id to segment
    memcpy(buffer + buffer_offset, &cells_with_id_[i].id_, 
           sizeof(int64_t));
    buffer_offset += sizeof(int64_t);
    // Write cell to segment
    memcpy(buffer + buffer_offset, cell, cell_size);
    buffer_offset += cell_size;
  }

  if(buffer_offset != 0) {
    write(file, buffer, buffer_offset);
    buffer_offset = 0;
  }

  // Clean up
  delete [] buffer;
  close(file);

  // Update write state
  for(int64_t i=0; i<cell_num; ++i)
    free(cells_with_id_[i].cell_);
  cells_with_id_.clear();
  run_size_ = 0;
  ++runs_num_;
}

void WriteState::flush_sorted_run_with_2_ids() {
  // For easy reference
  size_t cell_size = array_schema_->cell_size();
  bool var_size = (cell_size == VAR_SIZE);
  int attribute_num = array_schema_->attribute_num();
  size_t coords_size = array_schema_->cell_size(attribute_num);

  // Prepare file
  std::stringstream filename;
  filename << *temp_dirname_ << runs_num_;
  remove(filename.str().c_str());
  int file = open(filename.str().c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  assert(file != -1);

  // Write the cells into the file 
  char* buffer = new char[segment_size_];
  size_t buffer_offset = 0; 
  int64_t cell_num = cells_with_2_ids_.size();
  char* cell;

  for(int64_t i=0; i<cell_num; ++i) {
    // For easy reference
    cell = static_cast<char*>(cells_with_2_ids_[i].cell_);

    // If variable-sized, calculate size
    if(var_size) 
      memcpy(&cell_size, cell + coords_size, sizeof(size_t));

    // Flush the segment to the file
    if(buffer_offset + 2*sizeof(int64_t) + cell_size > segment_size_) { 
      write(file, buffer, buffer_offset);
      buffer_offset = 0;
    }

    // Write tile id to segment
    memcpy(buffer + buffer_offset, &cells_with_2_ids_[i].tile_id_, 
           sizeof(int64_t));
    buffer_offset += sizeof(int64_t);
    // Write cell id to segment
    memcpy(buffer + buffer_offset, &cells_with_2_ids_[i].cell_id_, 
           sizeof(int64_t));
    buffer_offset += sizeof(int64_t);
    // Write cell to segment
    memcpy(buffer + buffer_offset, cell, cell_size);
    buffer_offset += cell_size;
  }

  if(buffer_offset != 0) {
    write(file, buffer, buffer_offset);
    buffer_offset = 0;
  }

  // Clean up
  delete [] buffer;
  close(file);

  // Update write state
  for(int64_t i=0; i<cell_num; ++i)
    free(cells_with_2_ids_[i].cell_);
  cells_with_2_ids_.clear();
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

template<class T>
void* WriteState::get_next_cell(
    SortedRun** runs, int runs_num, size_t& cell_size) const {
  assert(runs_num > 0);
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  size_t coords_size = array_schema_->cell_size(attribute_num);

  // Get the first non-NULL cell
  void *next_cell, *cell;
  int next_run;
  int r = 0;
  do {
    next_cell = runs[r]->current_cell(); 
    ++r;
  } while((next_cell == NULL) && (r < runs_num)); 

  next_run = r-1;

  // Get the next cell in the global cell order
  for(int i=r; i<runs_num; ++i) {
    cell = runs[i]->current_cell();
    if(cell != NULL && 
       array_schema_->precedes(static_cast<const T*>(cell), 
                               static_cast<const T*>(next_cell))) { 
      next_cell = cell;
      next_run = i;
    }
  }

  if(next_cell != NULL) {
    if(runs[next_run]->var_size()) {
      memcpy(&cell_size, static_cast<char*>(next_cell) + coords_size,
             sizeof(size_t));
    }

    runs[next_run]->advance_cell(cell_size);
  }

  return next_cell;
}

template<class T>
void* WriteState::get_next_cell_with_id(
    SortedRun** runs, int runs_num, size_t& cell_size) const {
  assert(runs_num > 0);
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  size_t coords_size = array_schema_->cell_size(attribute_num);

  // Get the first non-NULL cell
  void *next_cell, *cell;
  int next_run;
  int r = 0;
  do {
    next_cell = runs[r]->current_cell(); 
    ++r;
  } while((next_cell == NULL) && (r < runs_num)); 

  next_run = r-1;
  int64_t* next_cell_id = static_cast<int64_t*>(next_cell);
  void* next_cell_coords = next_cell_id + 1; 
  int64_t* cell_id;
  void* cell_coords;

  // Get the next cell in the global cell order
  for(int i=r; i<runs_num; ++i) {
    cell = runs[i]->current_cell();
    if(cell == NULL)
      continue;

    cell_id = static_cast<int64_t*>(cell);
    cell_coords = cell_id + 1;

    if(*cell_id < *next_cell_id || 
       (*cell_id == *next_cell_id && 
         array_schema_->precedes(static_cast<T*>(cell_coords), 
                                 static_cast<T*>(next_cell_coords)))) { 
      next_cell = cell;
      next_cell_id = static_cast<int64_t*>(next_cell);
      next_cell_coords = next_cell_id + 1; 
      next_run = i;
    }
  }

  if(next_cell != NULL) {
    if(runs[next_run]->var_size()) {
      memcpy(&cell_size, 
             static_cast<char*>(next_cell) + sizeof(int64_t) + coords_size, 
             sizeof(size_t));
    }

    runs[next_run]->advance_cell(cell_size + sizeof(int64_t));
  }

  return next_cell;
}


template<class T>
void* WriteState::get_next_cell_with_2_ids(
    SortedRun** runs, int runs_num, size_t& cell_size) const {
  assert(runs_num > 0);
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  size_t coords_size = array_schema_->cell_size(attribute_num);

  // Get the first non-NULL cell
  void *next_cell, *cell;
  int next_run;
  int r = 0;
  do {
    next_cell = runs[r]->current_cell(); 
    ++r;
  } while((next_cell == NULL) && (r < runs_num)); 

  next_run = r-1;
  int64_t* next_cell_tile_id = static_cast<int64_t*>(next_cell);
  int64_t* next_cell_cell_id = next_cell_tile_id + 1;
  void* next_cell_coords = next_cell_cell_id + 1; 
  int64_t* cell_tile_id;
  int64_t* cell_cell_id;
  void* cell_coords; 

  // Get the next cell in the global cell order
  for(int i=r; i<runs_num; ++i) {
    cell = runs[i]->current_cell();
    if(cell == NULL)
      continue;

    cell_tile_id = static_cast<int64_t*>(cell);
    cell_cell_id = cell_tile_id + 1;
    cell_coords = cell_cell_id + 1;

    if(*cell_tile_id < *next_cell_tile_id       ||
       (*cell_tile_id == *next_cell_tile_id && 
        *cell_cell_id < *next_cell_cell_id)     ||
       (*cell_tile_id == *next_cell_tile_id && 
        *cell_cell_id == *next_cell_cell_id &&
         array_schema_->precedes(static_cast<T*>(cell_coords), 
                                 static_cast<T*>(next_cell_coords)))) { 
      next_cell = cell;
      next_cell_tile_id = static_cast<int64_t*>(next_cell);
      next_cell_cell_id = next_cell_tile_id;
      next_cell_coords = next_cell_cell_id + 1; 
      next_run = i;
    }
  }

  if(next_cell != NULL) {
    if(runs[next_run]->var_size()) {
      memcpy(&cell_size, 
             static_cast<char*>(next_cell) + 2*sizeof(int64_t) + coords_size, 
             sizeof(size_t));
    }

    runs[next_run]->advance_cell(cell_size + 2*sizeof(int64_t));
  }

  return next_cell;
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
  // For easy reference
  size_t cell_size = array_schema_->cell_size();
  std::vector<std::string> filenames = get_filenames(dirname);
  int runs_num = filenames.size();

  if(runs_num == 0)
    return;

  // Information about the runs to be merged 
  SortedRun** runs = new SortedRun*[runs_num];
  for(int i=0; i<runs_num; ++i) {
    runs[i] = new SortedRun(dirname + filenames[i], 
                            cell_size == VAR_SIZE, 
                            segment_size_);
  }

  // Loop over the cells
  void* cell;
  while((cell = get_next_cell<T>(runs, runs_num, cell_size)) != NULL) 
    write_cell_sorted<T>(cell);

  // Clean up
  for(int i=0; i<runs_num; ++i) 
    delete runs[i];
  delete [] runs;
}

// This function applies either to regular tiles with row- or column-major
// order, or irregular tiles with Hilbert order
template<class T>
void WriteState::make_tiles_with_id(const std::string& dirname) {
  // For easy reference
  size_t cell_size = array_schema_->cell_size();
  std::vector<std::string> filenames = get_filenames(dirname);
  int runs_num = filenames.size();

  if(runs_num == 0)
    return;

  // Information about the runs to be merged 
  SortedRun** runs = new SortedRun*[runs_num];
  for(int i=0; i<runs_num; ++i) {
    runs[i] = new SortedRun(dirname + filenames[i], 
                            cell_size == VAR_SIZE, 
                            segment_size_);
  }

  // Loop over the cells
  void* cell;
  if(array_schema_->has_regular_tiles()) {
    while((cell = get_next_cell_with_id<T>(runs, runs_num, cell_size)) != NULL) 
      write_cell_sorted_with_id<T>(cell);
  } else { // Irregular + Hilbert cell order --> Skip the Hilber id
    while((cell = get_next_cell_with_id<T>(runs, runs_num, cell_size)) != NULL) 
      write_cell_sorted<T>(static_cast<char*>(cell) + sizeof(int64_t));
  }

  // Clean up
  for(int i=0; i<runs_num; ++i) 
    delete runs[i];
  delete [] runs;
}

// NOTE: This function applies only to regular tiles
template<class T>
void WriteState::make_tiles_with_2_ids(const std::string& dirname) {
  // For easy reference
  size_t cell_size = array_schema_->cell_size();
  std::vector<std::string> filenames = get_filenames(dirname);
  int runs_num = filenames.size();

  if(runs_num == 0)
    return;

  // Information about the runs to be merged 
  SortedRun** runs = new SortedRun*[runs_num];
  for(int i=0; i<runs_num; ++i) {
    runs[i] = new SortedRun(dirname + filenames[i], 
                            cell_size == VAR_SIZE, 
                            segment_size_);
  }

  // Loop over the cells
  void* cell;
  while((cell = get_next_cell_with_2_ids<T>(runs, runs_num, cell_size)) != NULL)
    write_cell_sorted_with_2_ids<T>(cell);

  // Clean up
  for(int i=0; i<runs_num; ++i) 
    delete runs[i];
  delete [] runs;
}

bool WriteState::merge_sorted_runs(const std::string& dirname) {
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
      return merge_sorted_runs<int>(dirname); 
    else if(*coords_type == typeid(int64_t)) 
      return merge_sorted_runs<int64_t>(dirname); 
    else if(*coords_type == typeid(float)) 
      return merge_sorted_runs<float>(dirname); 
    else if(*coords_type == typeid(double)) 
      return merge_sorted_runs<double>(dirname); 
  } else if((regular_tiles && (cell_order == ArraySchema::CO_ROW_MAJOR ||
                               cell_order == ArraySchema::CO_COLUMN_MAJOR)) ||
            (!regular_tiles && cell_order == ArraySchema::CO_HILBERT)) {
    // CellWithId
    if(*coords_type == typeid(int)) 
      return merge_sorted_runs_with_id<int>(dirname); 
    else if(*coords_type == typeid(int64_t)) 
      return merge_sorted_runs_with_id<int64_t>(dirname); 
    else if(*coords_type == typeid(float)) 
      return merge_sorted_runs_with_id<float>(dirname); 
    else if(*coords_type == typeid(double)) 
      return merge_sorted_runs_with_id<double>(dirname); 
  } else if(regular_tiles && cell_order == ArraySchema::CO_HILBERT) {
    // CellWith2Ids
    if(*coords_type == typeid(int)) 
      return merge_sorted_runs_with_2_ids<int>(dirname); 
    else if(*coords_type == typeid(int64_t)) 
      return merge_sorted_runs_with_2_ids<int64_t>(dirname); 
    else if(*coords_type == typeid(float)) 
      return merge_sorted_runs_with_2_ids<float>(dirname); 
    else if(*coords_type == typeid(double)) 
      return merge_sorted_runs_with_2_ids<double>(dirname); 
  }
}

template<class T>
bool WriteState::merge_sorted_runs(const std::string& dirname) {
  int runs_per_merge = double(write_state_max_size_)/segment_size_-1;
  int merges, first_run, last_run;
  std::vector<std::string> filenames = get_filenames(dirname);
  int initial_runs_num = filenames.size();
  int new_run = initial_runs_num;

  // No merge
  if(initial_runs_num <= runs_per_merge)
    return false;

  // Merge hierarchically
  bool first_merge = true;
  while(filenames.size() > runs_per_merge) {
    merges = ceil(double(filenames.size()) / runs_per_merge);

    for(int i=0; i<merges; ++i) {
      first_run = i*runs_per_merge;
      last_run = std::min((i+1)*runs_per_merge-1, int(filenames.size()-1));
      merge_sorted_runs<T>((first_merge) ? dirname : *temp_dirname_, 
                           filenames, first_run, last_run, new_run);
      ++new_run;
    }

    filenames = get_filenames(*temp_dirname_);
    first_merge = false;
  }

  return true;
}

template<class T>
void WriteState::merge_sorted_runs(
    const std::string& dirname, const std::vector<std::string>& filenames,
    int first_run, int last_run, int new_run) {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  size_t cell_size = array_schema_->cell_size();
  struct stat st;

  // Information about the runs to be merged 
  int runs_num = last_run-first_run+1;
  SortedRun** runs = new SortedRun*[runs_num];
  for(int i=0; i<runs_num; ++i) {
    runs[i] = new SortedRun(dirname + filenames[first_run + i], 
                            cell_size == VAR_SIZE, 
                            segment_size_);
  }

  // Information about the output merged run
  void* cell;
  char* segment = new char[segment_size_];
  size_t offset = 0;
  std::stringstream new_filename;
  new_filename << temp_dirname_ << new_run;
  int new_run_fd = open(new_filename.str().c_str(), 
                        O_WRONLY | O_CREAT | O_TRUNC | O_SYNC, S_IRWXU);
  assert(new_run_fd != -1);
 
  // Loop over the cells
  while((cell = get_next_cell<T>(runs, runs_num, cell_size)) != NULL) {
    // If the output segment is full, flush it into the output file
    if(offset + cell_size > segment_size_) {
      write(new_run_fd, segment, offset);
      offset = 0;
    }

    // Write cell to segment 
    memcpy(segment + offset, cell, cell_size);
    offset += cell_size;
  }

  // Flush the remaining data of the output segment into the output file
  if(offset > 0) 
    write(new_run_fd, segment, offset);

  // Delete the files of the merged runs
  if(dirname == *temp_dirname_)
    for(int i=0; i<runs_num; ++i) 
      remove(runs[i]->filename().c_str());

  // Clean up
  delete [] segment;
  for(int i=0; i<runs_num; ++i)
    delete runs[i];
  delete [] runs;
  close(new_run_fd);
}

template<class T>
bool WriteState::merge_sorted_runs_with_id(const std::string& dirname) {
  int runs_per_merge = double(write_state_max_size_)/segment_size_-1;
  int merges, first_run, last_run;
  std::vector<std::string> filenames = get_filenames(dirname);
  int initial_runs_num = filenames.size();
  int new_run = initial_runs_num;

  // No merge
  if(initial_runs_num <= runs_per_merge)
    return false;

  // Merge hierarchically
  bool first_merge = true;
  while(filenames.size() > runs_per_merge) {
    merges = ceil(double(filenames.size()) / runs_per_merge);

    for(int i=0; i<merges; ++i) {
      first_run = i*runs_per_merge;
      last_run = std::min((i+1)*runs_per_merge-1, int(filenames.size()-1));
      merge_sorted_runs_with_id<T>((first_merge) ? dirname : *temp_dirname_, 
                                   filenames, first_run, last_run, new_run);
      ++new_run;
    }

    filenames = get_filenames(*temp_dirname_);
    first_merge = false;
  }

  return true;
}

template<class T>
void WriteState::merge_sorted_runs_with_id(
    const std::string& dirname, const std::vector<std::string>& filenames,
    int first_run, int last_run, int new_run) {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  size_t cell_size = array_schema_->cell_size();
  struct stat st;

  // Information about the runs to be merged 
  int runs_num = last_run-first_run+1;
  SortedRun** runs = new SortedRun*[runs_num];
  for(int i=0; i<runs_num; ++i) {
    std::stringstream filename;
    filename << dirname << (first_run+i);
    runs[i] = new SortedRun(dirname + filenames[first_run + i], 
                            cell_size == VAR_SIZE, 
                            segment_size_);
  }

  // Information about the output merged run
  void* cell;
  char* segment = new char[segment_size_];
  size_t offset = 0;
  std::stringstream new_filename;
  new_filename << *temp_dirname_ << new_run;
  int new_run_fd = open(new_filename.str().c_str(), 
                        O_WRONLY | O_CREAT | O_TRUNC | O_SYNC, S_IRWXU);
  assert(new_run_fd != -1);
 
  // Loop over the cells
  while((cell = get_next_cell_with_id<T>(runs, runs_num, cell_size)) != NULL) {
    // If the output segment is full, flush it into the output file
    if(offset + cell_size + sizeof(int64_t) > segment_size_) {
      write(new_run_fd, segment, offset);
      offset = 0;
    }

    // Write cell (along with its id) to segment 
    memcpy(segment + offset, cell, cell_size + sizeof(int64_t));
    offset += cell_size;
  }

  // Flush the remaining data of the output segment into the output file
  if(offset > 0) 
    write(new_run_fd, segment, offset);

  // Delete the files of the merged runs
  if(dirname == *temp_dirname_)
    for(int i=0; i<runs_num; ++i) 
      remove(runs[i]->filename().c_str());

  // Clean up
  delete [] segment;
  for(int i=0; i<runs_num; ++i)
    delete runs[i];
  delete [] runs;
  close(new_run_fd);
}

template<class T>
bool WriteState::merge_sorted_runs_with_2_ids(
  const std::string& dirname) {
  int runs_per_merge = double(write_state_max_size_)/segment_size_-1;
  int merges, first_run, last_run;
  std::vector<std::string> filenames = get_filenames(dirname);
  int initial_runs_num = filenames.size();
  int new_run = initial_runs_num;

  // No merge
  if(initial_runs_num <= runs_per_merge)
    return false;

  // Merge hierarchically
  bool first_merge = true;
  while(filenames.size() > runs_per_merge) {
    merges = ceil(double(filenames.size()) / runs_per_merge);

    for(int i=0; i<merges; ++i) {
      first_run = i*runs_per_merge;
      last_run = std::min((i+1)*runs_per_merge-1, int(filenames.size()-1));
      merge_sorted_runs_with_2_ids<T>((first_merge) ? dirname : *temp_dirname_, 
                                      filenames, first_run, last_run, new_run);
      ++new_run;
    }

    filenames = get_filenames(*temp_dirname_);
    first_merge = false;
  }

  return true;
}

template<class T>
void WriteState::merge_sorted_runs_with_2_ids(
    const std::string& dirname, const std::vector<std::string>& filenames,
    int first_run, int last_run, int new_run) {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  size_t cell_size = array_schema_->cell_size();
  struct stat st;

  // Information about the runs to be merged 
  int runs_num = last_run-first_run+1;
  SortedRun** runs = new SortedRun*[runs_num];
  for(int i=0; i<runs_num; ++i) {
    std::stringstream filename;
    filename << dirname << (first_run+i);
    runs[i] = new SortedRun(dirname + filenames[first_run + i], 
                            cell_size == VAR_SIZE, 
                            segment_size_);
  }

  // Information about the output merged run
  void* cell;
  char* segment = new char[segment_size_];
  size_t offset = 0;
  std::stringstream new_filename;
  new_filename << *temp_dirname_ << new_run;
  int new_run_fd = open(new_filename.str().c_str(), 
                        O_WRONLY | O_CREAT | O_TRUNC | O_SYNC, S_IRWXU);
  assert(new_run_fd != -1);

  // Loop over the cells
  while((cell = get_next_cell_with_2_ids<T>(runs, runs_num, cell_size)) != 
        NULL) {
    // If the output segment is full, flush it into the output file
    if(offset + cell_size + 2*sizeof(int64_t) > segment_size_) {
      write(new_run_fd, segment, offset);
      offset = 0;
    }

    // Write cell (along with its 2 ids) to segment 
    memcpy(segment + offset, cell, cell_size + 2*sizeof(int64_t));
    offset += cell_size;
  }

  // Flush the remaining data of the output segment into the output file
  if(offset > 0) 
    write(new_run_fd, segment, offset);

  // Delete the files of the merged runs
  if(dirname == *temp_dirname_)
    for(int i=0; i<runs_num; ++i) 
      remove(runs[i]->filename().c_str());

  // Clean up
  delete [] segment;
  for(int i=0; i<runs_num; ++i)
    delete runs[i];
  delete [] runs;
  close(new_run_fd);
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
      std::sort(cells_.begin(), cells_.end(), SmallerCol<int>(dim_num));
    } else if(*coords_type == typeid(int64_t)) {
      std::sort(cells_.begin(), cells_.end(), SmallerCol<int64_t>(dim_num));
    } else if(*coords_type == typeid(float)) {
      std::sort(cells_.begin(), cells_.end(), SmallerCol<float>(dim_num));
    } else if(*coords_type == typeid(double)) {
      std::sort(cells_.begin(), cells_.end(), SmallerCol<double>(dim_num));
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
      std::sort(cells_with_id_.begin(), cells_with_id_.end(), 
                SmallerRowWithId<int>(dim_num));
    } else if(*coords_type == typeid(int64_t)) {
      std::sort(cells_with_id_.begin(), 
                cells_with_id_.end(), 
                SmallerRowWithId<int64_t>(dim_num));
    } else if(*coords_type == typeid(float)) {
      std::sort(cells_with_id_.begin(), 
                cells_with_id_.end(), 
                SmallerRowWithId<float>(dim_num));
    } else if(*coords_type == typeid(double)) {
      std::sort(cells_with_id_.begin(), 
                cells_with_id_.end(), 
                SmallerRowWithId<double>(dim_num));
    }
  } else if(cell_order == ArraySchema::CO_COLUMN_MAJOR) { // Rregular + col co
    if(*coords_type == typeid(int)) {
      std::sort(cells_with_id_.begin(), 
                cells_with_id_.end(), 
                SmallerColWithId<int>(dim_num));
    } else if(*coords_type == typeid(int64_t)) {
      std::sort(cells_with_id_.begin(), 
                cells_with_id_.end(), 
                SmallerColWithId<int64_t>(dim_num));
    } else if(*coords_type == typeid(float)) {
      std::sort(cells_with_id_.begin(), 
                cells_with_id_.end(), 
                SmallerColWithId<float>(dim_num));
    } else if(*coords_type == typeid(double)) {
      std::sort(cells_with_id_.begin(), 
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
    std::sort(cells_with_2_ids_.begin(), 
              cells_with_2_ids_.end(), 
              SmallerWith2Ids<int>(dim_num));
  } else if(*coords_type == typeid(int64_t)) {
    std::sort(cells_with_2_ids_.begin(), 
              cells_with_2_ids_.end(), 
              SmallerWith2Ids<int64_t>(dim_num));
  } else if(*coords_type == typeid(float)) {
    std::sort(cells_with_2_ids_.begin(), 
              cells_with_2_ids_.end(), 
              SmallerWith2Ids<float>(dim_num));
  } else if(*coords_type == typeid(double)) {
    std::sort(cells_with_2_ids_.begin(), 
              cells_with_2_ids_.end(), 
              SmallerWith2Ids<double>(dim_num));
  }
}

void WriteState::write_cell(const Cell& cell, size_t cell_size) {

  size_t size_cost = sizeof(Cell) + cell_size;

  if(run_size_ + size_cost > write_state_max_size_) {
    sort_run();
    flush_sorted_run();
  }

  cells_.push_back(cell);
  run_size_ += size_cost;
}

void WriteState::write_cell(const CellWithId& cell, size_t cell_size) {
  size_t size_cost = sizeof(CellWithId) + cell_size;

  if(run_size_ + size_cost > write_state_max_size_) {
    sort_run_with_id();
    flush_sorted_run_with_id();
  }

  cells_with_id_.push_back(cell);
  run_size_ += size_cost;
}

void WriteState::write_cell(const CellWith2Ids& cell, size_t cell_size) {
  size_t size_cost = sizeof(CellWith2Ids) + cell_size;

  if(run_size_ + size_cost > write_state_max_size_) {
    sort_run_with_2_ids();
    flush_sorted_run_with_2_ids();
  }

  cells_with_2_ids_.push_back(cell);
  run_size_ += size_cost;
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

