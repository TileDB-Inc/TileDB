/**
 * @file   storage_manager.cc
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
 * This file implements the StorageManager class.
 */

#include "storage_manager.h"
#include "utils.h"
#include <algorithm>
#include <dirent.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <math.h>
#include <typeinfo>
#include <iostream>
#include <sstream>

/******************************************************
********************* FRAGMENT ************************
******************************************************/

StorageManager::Fragment::Fragment(
    const std::string& workspace, size_t segment_size,
    size_t write_state_max_size,
    const ArraySchema* array_schema, const std::string& fragment_name)
    : workspace_(workspace), array_schema_(array_schema),
      fragment_name_(fragment_name), segment_size_(segment_size), 
      write_state_max_size_(write_state_max_size) {
  read_state_ = NULL;
  write_state_ = NULL;

  // If the fragment folder exists (read mode), load the book-keeping structures
  std::string fragment_dir = workspace_ + 
                             array_schema->array_name() + "/" + 
                             fragment_name;
  if(path_exists(fragment_dir)) {
    load_book_keeping();
    init_read_state();
  }
  else { // Create the folder (write mode) 
    create_directory(fragment_dir);
    init_write_state();
    init_book_keeping();
  }
}

StorageManager::Fragment::~Fragment() {
  // Clean-up states
  if(read_state_ != NULL) 
    flush_read_state();

  if(write_state_ != NULL) {
    flush_write_state();
    flush_book_keeping();
  }
}

void StorageManager::Fragment::append_cell_to_segment(
    const void* cell, int attribute_id) {
  // For easy reference
  size_t cell_size = array_schema_->cell_size(attribute_id);

  // Check if the segment is full
  if(write_state_->segment_utilization_[attribute_id] + cell_size > 
     segment_size_)
    flush_segment(attribute_id);

  // Append cell to the segment
  memcpy(static_cast<char*>(write_state_->segments_[attribute_id]) + 
         write_state_->segment_utilization_[attribute_id], cell, cell_size); 
  write_state_->segment_utilization_[attribute_id] += cell_size;
}

void StorageManager::Fragment::delete_tiles(int attribute_id) {
  for(int i=0; i<read_state_->tiles_[attribute_id].size(); ++i)
    delete read_state_->tiles_[attribute_id][i];
}

void StorageManager::Fragment::finalize_last_run() {
  if(write_state_->cells_.size() > 0) {
    sort_run();
    flush_sorted_run();
  } else if(write_state_->cells_with_id_.size() > 0) {
    sort_run_with_id();
    flush_sorted_run_with_id();
  } else if(write_state_->cells_with_2_ids_.size() > 0) {
    sort_run_with_2_ids();
    flush_sorted_run_with_2_ids();
  }
}

void StorageManager::Fragment::flush_book_keeping() {
  flush_bounding_coordinates();
  flush_mbrs();
  flush_offsets();
  flush_tile_ids();
}

// FILE FORMAT:
// tile#1_lower_dim#1(T) tile#1_lower_dim#2(T) ... 
// tile#1_upper_dim#1(T) tile#1_upper_dim#2(T) ... 
// tile#2_lower_dim#1(T) tile#2_lower_dim#2(T) ... 
// tile#2_upper_dim#1(T) tile#2_upper_dim#2(T) ...
// ... 
// NOTE: T is the type of the dimensions of this array
void StorageManager::Fragment::flush_bounding_coordinates() {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  int64_t tile_num = book_keeping_.tile_ids_.size();
  size_t cell_size = array_schema_->cell_size(attribute_num);

  // Prepare filename
  std::string filename = workspace_ + "/" + array_schema_->array_name() + "/" + 
                         fragment_name_ + "/" +
                         SM_BOUNDING_COORDINATES_FILENAME + 
                         SM_BOOK_KEEPING_FILE_SUFFIX;

  // Delete file if it exists
  remove(filename.c_str());

  // Open file
  int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  assert(fd != -1);

  if(tile_num != 0) { // If the array is not empty
    // Prepare the buffer
    size_t buffer_size = 2 * tile_num * cell_size;
    char* buffer = new char[buffer_size];
 
    // Populate buffer
    size_t offset = 0;
    for(int64_t i=0; i<tile_num; ++i) {
      // Lower bounding coordinates
      memcpy(buffer+offset, 
             book_keeping_.bounding_coordinates_[i].first, cell_size);
      free(book_keeping_.bounding_coordinates_[i].first);
      offset += cell_size;
      // Upper bounding coordinates
      memcpy(buffer+offset, 
             book_keeping_.bounding_coordinates_[i].second, cell_size);
      free(book_keeping_.bounding_coordinates_[i].second);
      offset += cell_size;
    }

    // Write buffer and clean up
    write(fd, buffer, buffer_size);
    delete [] buffer;
  }  

  close(fd);
  book_keeping_.bounding_coordinates_.clear();
}

// FILE FORMAT:
// MBR#1_dim#1_low(T) MBR#1_dim#1_high(T) ...
// MBR#1_dim#2_low(T) MBR#1_dim#2_high(T) ...
// ...
// MBR#2_dim#1_low(T) MBR#2_dim#1_high(T) ...
// ...
// NOTE: T is the type of the dimensions of this array
void StorageManager::Fragment::flush_mbrs() {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  int64_t tile_num = book_keeping_.tile_ids_.size();
  size_t cell_size = array_schema_->cell_size(attribute_num);

  // prepare file name
  std::string filename = workspace_ + "/" + array_schema_->array_name() + "/" + 
                         fragment_name_ + "/" +
                         SM_MBRS_FILENAME + SM_BOOK_KEEPING_FILE_SUFFIX;

  // Delete file if it exists
  remove(filename.c_str());

  // Open file
  int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  assert(fd != -1);

  if(tile_num != 0) { // If the array is not empty
    // Prepare the buffer
    size_t buffer_size = tile_num * 2 * cell_size;
    char* buffer = new char[buffer_size];
 
    // Populate buffer
    size_t offset = 0;
    for(int64_t i=0; i<tile_num; ++i) {
      memcpy(buffer+offset, book_keeping_.mbrs_[i], 2 * cell_size);
      free(book_keeping_.mbrs_[i]);
      offset += 2 * cell_size;
    }

    // Write buffer and clean up
    write(fd, buffer, buffer_size);
    delete [] buffer;
  }

  close(fd);
  book_keeping_.mbrs_.clear();
}

// FILE FORMAT:
// tile#1_of_attribute#1_offset(int64_t)
// tile#2_of_attribute#1_offset(int64_t)
// ...
// tile#1_of_attribute#2_offset(int64_t)
// tile#2_of_attribute#2_offset(int64_t)
// ...
// NOTE: Do not forget the extra coordinate attribute
void StorageManager::Fragment::flush_offsets() {
  // For easy reference
  int64_t tile_num = book_keeping_.tile_ids_.size();

  // Prepare file name
  std::string filename = workspace_ + "/" + array_schema_->array_name() + "/" + 
                         fragment_name_ + "/" +
                         SM_OFFSETS_FILENAME + SM_BOOK_KEEPING_FILE_SUFFIX;

  // Delete file if it exists
  remove(filename.c_str());

  // Open file
  int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  assert(fd != -1);

  if(tile_num != 0) { // If the array is not empty
    // Prepare the buffer
    int attribute_num = array_schema_->attribute_num();
    size_t buffer_size = (attribute_num+1) * tile_num * sizeof(int64_t);
    char* buffer = new char[buffer_size];
 
    // Populate buffer
    int64_t offset = 0;
    for(int i=0; i<=attribute_num; ++i) {
      memcpy(buffer+offset, 
             &book_keeping_.offsets_[i][0], tile_num * sizeof(int64_t));
      offset += tile_num * sizeof(int64_t);
    }

    // Write buffer and clean up
    write(fd, buffer, buffer_size);
    delete [] buffer;
  }
  
  close(fd);
  book_keeping_.offsets_.clear();
}

void StorageManager::Fragment::flush_read_state() {
  // Clean-up tiles
  for(int i=0; i<read_state_->tiles_.size(); ++i)
    delete_tiles(i);
  read_state_->tiles_.clear();

  // Clean-up segments
  for(int i=0; i<read_state_->segments_.size(); ++i)
    free(read_state_->segments_[i]);
  read_state_->segments_.clear();

  // Clean-up pos_ranges_
  read_state_->pos_ranges_.clear();

  delete read_state_;
  read_state_ = NULL;
}


void StorageManager::Fragment::flush_segment(int attribute_id) {
  // Exit if the segment has no useful data
  if(write_state_->segment_utilization_[attribute_id] == 0)
    return;

  // Open file
  std::string filename = workspace_ + "/" + 
                         array_schema_->array_name() + "/" + 
                         fragment_name_ + "/" + 
                         array_schema_->attribute_name(attribute_id) +
                         SM_TILE_DATA_FILE_SUFFIX;		
  int fd = open(filename.c_str(), O_WRONLY | O_APPEND | O_CREAT | O_SYNC,  
                S_IRWXU);
  assert(fd != -1);

  // Retrieve the current file offset (equal to the file size)
  struct stat st;
  fstat(fd, &st);
  int64_t offset = st.st_size;

  // Append the segment to the file
  write(fd, write_state_->segments_[attribute_id], 
         write_state_->segment_utilization_[attribute_id]);
 
  // Update the write state 
  write_state_->segment_utilization_[attribute_id] = 0;

  // Clean up 
  close(fd);
}

void StorageManager::Fragment::flush_segments() {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();

  // Store the info of the lastly populated tile
  flush_tile_info_to_book_keeping();

  // Flush the segments
  for(int i=0; i<=attribute_num; ++i) {
    flush_segment(i);
    free(write_state_->segments_[i]); 
  }
}

void StorageManager::Fragment::flush_sorted_run() {
  // For easy reference
  size_t cell_size = array_schema_->cell_size();

  // Prepare file
  std::stringstream filename;
  std::string dirname = workspace_ + "/" + SM_TEMP + "/" + 
                        array_schema_->array_name() + "_" +
                        fragment_name_ + "/";
  create_directory(dirname);
  filename << dirname << write_state_->runs_num_;
  remove(filename.str().c_str());
  int file = open(filename.str().c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  assert(file != -1);

  // Write the cells into the file 
  char* segment = new char[segment_size_];
  size_t offset = 0; 
  int64_t cell_num = write_state_->cells_.size();

  for(int64_t i=0; i<cell_num; ++i) {
    // Flush the segment to the file
    if(offset + cell_size > segment_size_) { 
      write(file, segment, offset);
      offset = 0;
    }

    memcpy(segment + offset, write_state_->cells_[i].cell_, cell_size);
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
    free(write_state_->cells_[i].cell_);
  write_state_->cells_.clear();
  write_state_->run_size_ = 0;
  ++write_state_->runs_num_;
}

void StorageManager::Fragment::flush_sorted_run_with_id() {
  // For easy reference
  size_t cell_size = array_schema_->cell_size();

  // Prepare file
  std::stringstream filename;
  std::string dirname = workspace_ + "/" + SM_TEMP + "/" + 
                        array_schema_->array_name() + "_" +
                        fragment_name_ + "/";
  create_directory(dirname);
  filename << dirname << write_state_->runs_num_;
  remove(filename.str().c_str());
  int file = open(filename.str().c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  assert(file != -1);

  // Write the cells into the file 
  char* buffer = new char[segment_size_];
  size_t buffer_offset = 0; 
  int64_t cell_num = write_state_->cells_with_id_.size();

  for(int64_t i=0; i<cell_num; ++i) {
    // Flush the segment to the file
    if(buffer_offset + sizeof(int64_t) + cell_size > segment_size_) { 
      write(file, buffer, buffer_offset);
      buffer_offset = 0;
    }

    memcpy(buffer + buffer_offset, 
           &write_state_->cells_with_id_[i].id_, 
           sizeof(int64_t));
    buffer_offset += sizeof(int64_t);
    memcpy(buffer + buffer_offset, 
           write_state_->cells_with_id_[i].cell_, 
           cell_size);
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
    free(write_state_->cells_with_id_[i].cell_);
  write_state_->cells_with_id_.clear();
  write_state_->run_size_ = 0;
  ++write_state_->runs_num_;
}

void StorageManager::Fragment::flush_sorted_run_with_2_ids() {
  // For easy reference
  size_t cell_size = array_schema_->cell_size();

  // Prepare file
  std::stringstream filename;
  std::string dirname = workspace_ + "/" + SM_TEMP + "/" + 
                        array_schema_->array_name() + "_" +
                        fragment_name_ + "/";
  create_directory(dirname);
  filename << dirname << write_state_->runs_num_;
  remove(filename.str().c_str());
  int file = open(filename.str().c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  assert(file != -1);

  // Write the cells into the file 
  char* buffer = new char[segment_size_];
  size_t buffer_offset = 0; 
  int64_t cell_num = write_state_->cells_with_2_ids_.size();

  for(int64_t i=0; i<cell_num; ++i) {
    // Flush the segment to the file
    if(buffer_offset + 2*sizeof(int64_t) + cell_size > segment_size_) { 
      write(file, buffer, buffer_offset);
      buffer_offset = 0;
    }

    memcpy(buffer + buffer_offset, 
           &write_state_->cells_with_2_ids_[i].tile_id_, 
           sizeof(int64_t));
    buffer_offset += sizeof(int64_t);
    memcpy(buffer + buffer_offset, 
           &write_state_->cells_with_2_ids_[i].cell_id_, 
           sizeof(int64_t));
    buffer_offset += sizeof(int64_t);
    memcpy(buffer + buffer_offset,  
           &write_state_->cells_with_2_ids_[i].cell_, 
           cell_size);
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
    free(write_state_->cells_with_2_ids_[i].cell_);
  write_state_->cells_with_2_ids_.clear();
  write_state_->run_size_ = 0;
  ++write_state_->runs_num_;
}


// FILE FORMAT:
// tile_num(int64_t)
//   tile_id#1(int64_t) tile_id#2(int64_t)  ...
void StorageManager::Fragment::flush_tile_ids() {
  // For easy reference
  int64_t tile_num = book_keeping_.tile_ids_.size();

  // Prepare file name
  std::string filename = workspace_ + "/" + array_schema_->array_name() + "/" +
                         fragment_name_ + "/" +
                         SM_TILE_IDS_FILENAME + SM_BOOK_KEEPING_FILE_SUFFIX;

  // Delete file if it exists
  remove(filename.c_str());

  // Open file
  int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  assert(fd != -1);
 
  if(tile_num != 0) { // If the array is not empty
    // Prepare the buffer
    size_t buffer_size = (tile_num+1) * sizeof(int64_t);
    char* buffer = new char[buffer_size];
 
    // Populate buffer
    memcpy(buffer, &tile_num, sizeof(int64_t));
    memcpy(buffer+sizeof(int64_t), 
           &book_keeping_.tile_ids_[0], tile_num * sizeof(int64_t));

    // Write buffer and clean up
    write(fd, buffer, buffer_size);  
    delete [] buffer;
  }

  close(fd);
  book_keeping_.tile_ids_.clear();
}

void StorageManager::Fragment::flush_tile_info_to_book_keeping() {
  // Exit if there are no cells in the current tile
  if(write_state_->cell_num_ == 0)
    return;

  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  size_t coords_size = array_schema_->cell_size(attribute_num);

  // Flush info
  for(int i=0; i<=attribute_num; ++i) 
    book_keeping_.offsets_[i].push_back(write_state_->file_offsets_[i]);

  book_keeping_.bounding_coordinates_.push_back(
      write_state_->bounding_coordinates_);
  book_keeping_.mbrs_.push_back(write_state_->mbr_);
  book_keeping_.tile_ids_.push_back(write_state_->tile_id_);
  write_state_->cell_num_ = 0;

  // Allocate new memory space for MBR and bounding coordinates
  write_state_->mbr_ = malloc(2*coords_size);
  write_state_->bounding_coordinates_.first = malloc(coords_size);
  write_state_->bounding_coordinates_.second = malloc(coords_size);
}

void StorageManager::Fragment::flush_write_state() {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();

  // Make tiles, after finalizing the last run and merging the runs
  finalize_last_run();
  merge_sorted_runs();
  make_tiles();
  flush_segments();

  delete write_state_;
  write_state_ = NULL;
}

// NOTE: The format of a cell is <coords, attributes>
template<class T>
void* StorageManager::Fragment::get_next_cell(
    SortedRun** runs, int runs_num) const {
  assert(runs_num > 0);

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

  if(next_cell != NULL)
    runs[next_run]->advance_cell();

  return next_cell;
}

// NOTE: The format of a cell is <id, coords, attributes>
template<class T>
void* StorageManager::Fragment::get_next_cell_with_id(
    SortedRun** runs, int runs_num) const {
  assert(runs_num > 0);

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

  if(next_cell != NULL)
    runs[next_run]->advance_cell();

  return next_cell;
}


// NOTE: The format of a cell is <tile_id, cell_id, coords, attributes>
template<class T>
void* StorageManager::Fragment::get_next_cell_with_2_ids(
    SortedRun** runs, int runs_num) const {
  assert(runs_num > 0);

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

  if(next_cell != NULL)
    runs[next_run]->advance_cell();

  return next_cell;
}

const Tile* StorageManager::Fragment::get_tile_by_pos(
    int attribute_id, int64_t pos) {
  // For easy reference
  const int64_t& pos_lower = read_state_->pos_ranges_[attribute_id].first;
  const int64_t& pos_upper = read_state_->pos_ranges_[attribute_id].second;

  // Fetch from the disk if the tile is not in main memory
  if(read_state_->tiles_[attribute_id].size() == 0 ||
     (pos < pos_lower || pos > pos_upper)) 
    // The following updates pos_lower and pos_upper
    load_tiles_from_disk(attribute_id, pos);

  assert(pos >= pos_lower && pos <= pos_upper);
  assert(pos - pos_lower <= read_state_->tiles_[attribute_id].size());

  return read_state_->tiles_[attribute_id][pos-pos_lower];
}

void StorageManager::Fragment::init_book_keeping() {
  int attribute_num = array_schema_->attribute_num();

  book_keeping_.offsets_.resize(attribute_num+1);
  for(int i=0; i<=attribute_num; ++i)
    book_keeping_.offsets_[i].push_back(0);
}

void StorageManager::Fragment::init_read_state() {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();

  read_state_ = new ReadState();

  // Initialize segments
  read_state_->segments_.resize(attribute_num+1);
  for(int i=0; i<=attribute_num; ++i)
    read_state_->segments_[i] = malloc(segment_size_);

  // Initialize tiles
  read_state_->tiles_.resize(attribute_num+1);

  // Initialize pos_ranges
  read_state_->pos_ranges_.resize(attribute_num+1);
}

void StorageManager::Fragment::init_write_state() {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();

  write_state_ = new WriteState();

  write_state_->tile_id_ = SM_INVALID_TILE_ID;
  write_state_->cell_num_ = 0;
  write_state_->run_buffer_ = NULL;
  write_state_->run_buffer_size_ = 0;
  write_state_->run_offset_ = 0;
  write_state_->run_size_ = 0;
  write_state_->runs_num_ = 0;

  write_state_->mbr_ = malloc(2*array_schema_->cell_size(attribute_num));
  write_state_->bounding_coordinates_.first = 
      malloc(array_schema_->cell_size(attribute_num));
  write_state_->bounding_coordinates_.second = 
      malloc(array_schema_->cell_size(attribute_num));

  write_state_->segments_.resize(attribute_num+1);
  write_state_->segment_utilization_.resize(attribute_num+1);
  write_state_->file_offsets_.resize(attribute_num+1);
  for(int i=0; i<= attribute_num; ++i) {
    write_state_->segments_[i] = malloc(segment_size_); 
    write_state_->segment_utilization_[i] = 0;
    write_state_->file_offsets_[i] = 0;
  } 
}

void StorageManager::Fragment::load_book_keeping() {
  load_tile_ids();
  load_bounding_coordinates();
  load_mbrs();
  load_offsets();
}

// FILE FORMAT:
// tile#1_lower_dim#1(T) tile#1_lower_dim#2(T) ... 
// tile#1_upper_dim#1(T) tile#1_upper_dim#2(T) ... 
// tile#2_lower_dim#1(T) tile#2_lower_dim#2(T) ... 
// tile#2_upper_dim#1(T) tile#2_upper_dim#2(T) ...
// ... 
// NOTE: T is the type of the dimensions of this array
void StorageManager::Fragment::load_bounding_coordinates() {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  int64_t tile_num = book_keeping_.tile_ids_.size();
  assert(tile_num != 0);
  size_t cell_size = array_schema_->cell_size(attribute_num);
 
  // Open file
  std::string filename = workspace_ + "/" + array_schema_->array_name() + "/" +
                         fragment_name_ + "/" +
                         SM_BOUNDING_COORDINATES_FILENAME + 
                         SM_BOOK_KEEPING_FILE_SUFFIX;
  int fd = open(filename.c_str(), O_RDONLY);
  assert(fd != -1);

  // Initializations
  struct stat st;
  fstat(fd, &st);
  size_t buffer_size = st.st_size;
  assert(buffer_size == tile_num * 2 * cell_size);
  char* buffer = new char[buffer_size];
  read(fd, buffer, buffer_size);
  int64_t offset = 0;
  book_keeping_.bounding_coordinates_.resize(tile_num);
  void* coord;

  // Load bounding coordinates
  for(int64_t i=0; i<tile_num; ++i) {
    coord = malloc(cell_size);
    memcpy(coord, buffer + offset, cell_size);
    book_keeping_.bounding_coordinates_[i].first = coord; 
    offset += cell_size;
    coord = malloc(cell_size);
    memcpy(coord, buffer + offset, cell_size);
    book_keeping_.bounding_coordinates_[i].second = coord; 
    offset += cell_size;
  }

  // Clean up
  delete [] buffer;
  close(fd);
}

// FILE FORMAT:
// MBR#1_dim#1_low(T) MBR#1_dim#1_high(T) ...
// MBR#1_dim#2_low(T) MBR#1_dim#2_high(T) ...
// ...
// MBR#2_dim#1_low(T) MBR#2_dim#1_high(T) ...
// ...
// NOTE: T is the type of the dimensions of this array
void StorageManager::Fragment::load_mbrs() {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  size_t cell_size = array_schema_->cell_size(attribute_num);
  int64_t tile_num = book_keeping_.tile_ids_.size();
  assert(tile_num != 0);
 
  // Open file
  std::string filename = workspace_ + "/" + array_schema_->array_name() + "/" +
                         fragment_name_ + "/" +
                         SM_MBRS_FILENAME + SM_BOOK_KEEPING_FILE_SUFFIX;
  int fd = open(filename.c_str(), O_RDONLY);
  assert(fd != -1);

  // Initializations
  struct stat st;
  fstat(fd, &st);
  size_t buffer_size = st.st_size;
  assert(buffer_size == tile_num * 2 * cell_size);
  char* buffer = new char[buffer_size];
  read(fd, buffer, buffer_size);
  size_t offset = 0;
  book_keeping_.mbrs_.resize(tile_num);
  void* mbr;

  // Load MBRs
  for(int64_t i=0; i<tile_num; ++i) {
    mbr = malloc(2 * cell_size);
    memcpy(mbr, buffer + offset, 2 * cell_size);
    book_keeping_.mbrs_[i] = mbr;
    offset += 2 * cell_size;
  }

  // Clean up
  delete [] buffer;
  close(fd);
}

// FILE FORMAT:
// tile#1_of_attribute#1_offset(int64_t)
// tile#2_of_attribute#1_offset(int64_t)
// ...
// tile#1_of_attribute#2_offset(int64_t)
// tile#2_of_attribute#2_offset(int64_t)
// ...
// NOTE: Do not forget the extra coordinate attribute
void StorageManager::Fragment::load_offsets() {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  int64_t tile_num = book_keeping_.tile_ids_.size();
  assert(tile_num != 0);
 
  // Open file
  std::string filename = workspace_ + "/" + array_schema_->array_name() + "/" +
                         fragment_name_ + "/" +
                         SM_OFFSETS_FILENAME + SM_BOOK_KEEPING_FILE_SUFFIX;
  int fd = open(filename.c_str(), O_RDONLY);
  assert(fd != -1);

  // Initializations
  struct stat st;
  fstat(fd, &st);
  size_t buffer_size = st.st_size;
  assert(buffer_size == (attribute_num+1)*tile_num*sizeof(int64_t));
  char* buffer = new char[buffer_size];
  read(fd, buffer, buffer_size);
  int64_t offset = 0;

  // Load offsets
  book_keeping_.offsets_.resize(attribute_num+1);
  for(int i=0; i<=attribute_num; ++i) {
    book_keeping_.offsets_[i].resize(tile_num);
    memcpy(&book_keeping_.offsets_[i][0], buffer + offset, 
           tile_num*sizeof(int64_t));
    offset += tile_num * sizeof(int64_t);
  }

  // Clean up
  delete [] buffer;
  close(fd);
}

inline
std::pair<size_t, int64_t> StorageManager::Fragment::load_payloads_into_segment(
    int attribute_id, int64_t start_pos) {
  // For easy reference
  const std::string& array_name = array_schema_->array_name();
  const std::string& attribute_name = 
      array_schema_->attribute_name(attribute_id);
  const OffsetList& offsets = book_keeping_.offsets_[attribute_id];
  int64_t tile_num = offsets.size();
  assert(tile_num == book_keeping_.tile_ids_.size());

  // Open file
  std::string filename = workspace_ + "/" + array_name + "/" +
                         fragment_name_ + "/" +
                         attribute_name + 
                         SM_TILE_DATA_FILE_SUFFIX;
  int fd = open(filename.c_str(), O_RDONLY);
  assert(fd != -1);

  // Initilizations
  struct stat st;
  fstat(fd, &st);
  size_t segment_utilization = 0;
  int64_t tiles_in_segment = 0;
  int64_t pos = start_pos;

  // Calculate buffer size (largest size smaller than the segment_size_)
  while(pos < tile_num && segment_utilization < segment_size_) {
    if(pos == tile_num-1)
      segment_utilization += st.st_size - offsets[pos];
    else
      segment_utilization += offsets[pos+1] - offsets[pos];
    pos++;
    tiles_in_segment++;
  }

  assert(segment_utilization != 0);
  assert(offsets[start_pos] + segment_utilization <= st.st_size);

  // Read payloads into buffer
  lseek(fd, offsets[start_pos], SEEK_SET);
  read(fd, read_state_->segments_[attribute_id], segment_utilization); 
  close(fd);

  return std::pair<size_t, int64_t>(segment_utilization, tiles_in_segment);
}

// FILE FORMAT:
// tile_num(int64_t)
//   tile_id#1(int64_t) tile_id#2(int64_t)  ...
void StorageManager::Fragment::load_tile_ids() {
  // Open file
  std::string filename = workspace_ + "/" + array_schema_->array_name() + "/" +
                         fragment_name_ + "/" +
                         SM_TILE_IDS_FILENAME + SM_BOOK_KEEPING_FILE_SUFFIX;
  int fd = open(filename.c_str(), O_RDONLY);
  assert(fd != -1);

  // Initializations
  struct stat st;
  fstat(fd, &st);
  size_t buffer_size = st.st_size;

  if(buffer_size == 0) // Empty array
    return;

  assert(buffer_size > sizeof(int64_t));
  char* buffer = new char[buffer_size];
  read(fd, buffer, buffer_size);
  int64_t tile_num;
  memcpy(&tile_num, buffer, sizeof(int64_t));
  assert(buffer_size == (tile_num+1)*sizeof(int64_t));
  book_keeping_.tile_ids_.resize(tile_num);

  // Load tile_ids
  memcpy(&book_keeping_.tile_ids_[0], 
         buffer+sizeof(int64_t), tile_num*sizeof(int64_t));

  // Clean up
  delete [] buffer;
  close(fd);
}

void StorageManager::Fragment::load_tiles_from_disk(
    int attribute_id, int64_t start_pos) { 
  char* buffer;

  // Load the tile payloads from the disk into a segment, starting
  // from the tile at position start_pos
  std::pair<size_t, int64_t> ret = 
      load_payloads_into_segment(attribute_id, start_pos); 
  size_t segment_utilization = ret.first;
  int64_t tiles_in_segment = ret.second;

  // Delete previous tiles from main memory
  delete_tiles(attribute_id);
  
  // Create the tiles from the payloads in the segment
  load_tiles_from_segment(attribute_id, start_pos, 
                          segment_utilization, tiles_in_segment);         
 
  // Update pos range in main memory
  read_state_->pos_ranges_[attribute_id].first = start_pos;
  read_state_->pos_ranges_[attribute_id].second = 
      start_pos + tiles_in_segment - 1;
}

void StorageManager::Fragment::load_tiles_from_segment(
    int attribute_id, int64_t start_pos, 
    size_t segment_utilization, int64_t tiles_in_segment) {
  // For easy reference
  const OffsetList& offsets = book_keeping_.offsets_[attribute_id];
  const TileIds& tile_ids = book_keeping_.tile_ids_;
  const MBRs& mbrs = book_keeping_.mbrs_;
  const BoundingCoordinates& bounding_coordinates = 
      book_keeping_.bounding_coordinates_;
  TileList& tiles = read_state_->tiles_[attribute_id];
  int attribute_num = array_schema_->attribute_num();
  int dim_num = (attribute_id != attribute_num) ? 0 : array_schema_->dim_num();
  char* segment = static_cast<char*>(read_state_->segments_[attribute_id]);
  const std::type_info* cell_type = array_schema_->type(attribute_id);
  size_t cell_size = array_schema_->cell_size(attribute_id);
  assert(offsets.size() == tile_ids.size());

  // Initializations
  size_t segment_offset = 0, payload_size;
  int64_t tile_id;
  int64_t pos = start_pos;
  tiles.resize(tiles_in_segment);
  void* payload;
  
  // Load tiles
  for(int64_t i=0; i<tiles_in_segment; ++i, ++pos) {
    assert(pos < tile_ids.size());
    tile_id = tile_ids[pos];

    if(pos == offsets.size()-1)
      payload_size = segment_utilization - segment_offset;
    else
      payload_size = offsets[pos+1] - offsets[pos];

    payload = segment + segment_offset;
    
    Tile* tile = new Tile(tile_id, dim_num, cell_type, 0);
    tile->set_payload(payload, payload_size);
    if(tile->tile_type() == Tile::COORDINATE) 
      tile->set_mbr(mbrs[pos]);
    
    tiles[i] = tile;
    segment_offset += payload_size;
  }
}

void StorageManager::Fragment::make_tiles() {
  // Exit if there are no runs
  if(write_state_->runs_num_ == 0)
    return;

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
      make_tiles<int>(); 
    else if(*coords_type == typeid(int64_t)) 
      make_tiles<int64_t>(); 
    else if(*coords_type == typeid(float)) 
      make_tiles<float>(); 
    else if(*coords_type == typeid(double)) 
      make_tiles<double>(); 
  } else if((regular_tiles && (cell_order == ArraySchema::CO_ROW_MAJOR ||
                               cell_order == ArraySchema::CO_COLUMN_MAJOR)) ||
            (!regular_tiles && cell_order == ArraySchema::CO_HILBERT)) {
    // CellWithId
    if(*coords_type == typeid(int)) 
      make_tiles_with_id<int>(); 
    else if(*coords_type == typeid(int64_t)) 
      make_tiles_with_id<int64_t>(); 
    else if(*coords_type == typeid(float)) 
      make_tiles_with_id<float>(); 
    else if(*coords_type == typeid(double)) 
      make_tiles_with_id<double>(); 
  } else if(regular_tiles && cell_order == ArraySchema::CO_HILBERT) {
    // CellWith2Ids
    if(*coords_type == typeid(int)) 
      make_tiles_with_2_ids<int>(); 
    else if(*coords_type == typeid(int64_t)) 
      make_tiles_with_2_ids<int64_t>(); 
    else if(*coords_type == typeid(float)) 
      make_tiles_with_2_ids<float>(); 
    else if(*coords_type == typeid(double)) 
      make_tiles_with_2_ids<double>(); 
  }
}

// NOTE: This function applies only to irregular tiles
template<class T>
void StorageManager::Fragment::make_tiles() {
  // For easy reference
  size_t cell_size = array_schema_->cell_size();
  std::string dirname = workspace_ + "/" + SM_TEMP + "/" + 
                        array_schema_->array_name() + "_" + 
                        fragment_name_ + "/";
  int runs_num = write_state_->runs_num_;

  // Information about the runs to be merged 
  SortedRun** runs = new SortedRun*[runs_num];
  for(int i=0; i<runs_num; ++i) {
    std::stringstream filename;
    filename << dirname << i;
    runs[i] = new SortedRun(filename.str(), cell_size, segment_size_);
  }

  // Loop over the cells
  void* cell = malloc(cell_size);
  while((cell = get_next_cell<T>(runs, runs_num)) != NULL) 
    write_cell_sorted<T>(cell);

  // Clean up
  for(int i=0; i<runs_num; ++i) {
    remove(runs[i]->filename_.c_str());
    delete runs[i];
  }
  delete [] runs;
  free(cell);
}

// This function applies either to regular tiles with row- or column-major
// order, or irregular tiles with Hilbert order
template<class T>
void StorageManager::Fragment::make_tiles_with_id() {
  // For easy reference
  size_t cell_size = sizeof(int64_t) + array_schema_->cell_size();
  std::string dirname = workspace_ + "/" + SM_TEMP + "/" + 
                        array_schema_->array_name() + "_" + 
                        fragment_name_ + "/";
  int runs_num = write_state_->runs_num_;

  // Information about the runs to be merged 
  SortedRun** runs = new SortedRun*[runs_num];
  for(int i=0; i<runs_num; ++i) {
    std::stringstream filename;
    filename << dirname << i;
    runs[i] = new SortedRun(filename.str(), cell_size, segment_size_);
  }

  // Loop over the cells
  void* cell = malloc(cell_size);
  if(array_schema_->has_regular_tiles()) {
    while((cell = get_next_cell_with_id<T>(runs, runs_num)) != NULL) 
      write_cell_sorted_with_id<T>(cell);
  } else { // Irregular + Hilbert cell order --> Skip the Hilber id
    while((cell = get_next_cell_with_id<T>(runs, runs_num)) != NULL) 
      write_cell_sorted<T>(static_cast<char*>(cell) + sizeof(int64_t));
  }

  // Clean up
  for(int i=0; i<runs_num; ++i) {
    remove(runs[i]->filename_.c_str());
    delete runs[i];
  }
  delete [] runs;
  free(cell);
}

// NOTE: This function applies only to regular tiles
template<class T>
void StorageManager::Fragment::make_tiles_with_2_ids() {
  // For easy reference
  size_t cell_size = 2*sizeof(int64_t) + array_schema_->cell_size();
  std::string dirname = workspace_ + "/" + SM_TEMP + "/" + 
                        array_schema_->array_name() + "_" + 
                        fragment_name_ + "/";
  int runs_num = write_state_->runs_num_;

  // Information about the runs to be merged 
  SortedRun** runs = new SortedRun*[runs_num];
  for(int i=0; i<runs_num; ++i) {
    std::stringstream filename;
    filename << dirname << i;
    runs[i] = new SortedRun(filename.str(), cell_size, segment_size_);
  }

  // Loop over the cells
  void* cell = malloc(cell_size);
  while((cell = get_next_cell_with_2_ids<T>(runs, runs_num)) != NULL) 
    write_cell_sorted_with_2_ids<T>(cell);

  // Clean up
  for(int i=0; i<runs_num; ++i) {
    remove(runs[i]->filename_.c_str());
    delete runs[i];
  }
  delete [] runs;
  free(cell);
}

void StorageManager::Fragment::merge_sorted_runs() {
  // Exit if there are no runs
  if(write_state_->runs_num_ == 0)
    return;

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
      merge_sorted_runs<int>(); 
    else if(*coords_type == typeid(int64_t)) 
      merge_sorted_runs<int64_t>(); 
    else if(*coords_type == typeid(float)) 
      merge_sorted_runs<float>(); 
    else if(*coords_type == typeid(double)) 
      merge_sorted_runs<double>(); 
  } else if((regular_tiles && (cell_order == ArraySchema::CO_ROW_MAJOR ||
                               cell_order == ArraySchema::CO_COLUMN_MAJOR)) ||
            (!regular_tiles && cell_order == ArraySchema::CO_HILBERT)) {
    // CellWithId
    if(*coords_type == typeid(int)) 
      merge_sorted_runs_with_id<int>(); 
    else if(*coords_type == typeid(int64_t)) 
      merge_sorted_runs_with_id<int64_t>(); 
    else if(*coords_type == typeid(float)) 
      merge_sorted_runs_with_id<float>(); 
    else if(*coords_type == typeid(double)) 
      merge_sorted_runs_with_id<double>(); 
  } else if(regular_tiles && cell_order == ArraySchema::CO_HILBERT) {
    // CellWith2Ids
    if(*coords_type == typeid(int)) 
      merge_sorted_runs_with_2_ids<int>(); 
    else if(*coords_type == typeid(int64_t)) 
      merge_sorted_runs_with_2_ids<int64_t>(); 
    else if(*coords_type == typeid(float)) 
      merge_sorted_runs_with_2_ids<float>(); 
    else if(*coords_type == typeid(double)) 
      merge_sorted_runs_with_2_ids<double>(); 
  }
}

template<class T>
void StorageManager::Fragment::merge_sorted_runs() {
  int runs_per_merge = double(write_state_max_size_)/segment_size_-1;
  int merges;

  while(write_state_->runs_num_ > runs_per_merge) {
    merges = ceil(double(write_state_->runs_num_) / runs_per_merge);

    for(int i=0; i<merges; ++i) 
      merge_sorted_runs<T>(
          i*runs_per_merge, 
          std::min((i+1)*runs_per_merge-1, write_state_->runs_num_-1), 
          i);

    write_state_->runs_num_ = merges; 
  }
}

template<class T>
void StorageManager::Fragment::merge_sorted_runs(
    int first_run, int last_run, int new_run) {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  size_t cell_size = array_schema_->cell_size();
  struct stat st;
  std::string dirname = workspace_ + "/" + SM_TEMP + "/" + 
                        array_schema_->array_name() + "_" + 
                        fragment_name_ + "/";

  // Information about the runs to be merged 
  int runs_num = last_run-first_run+1;
  SortedRun** runs = new SortedRun*[runs_num];
  for(int i=0; i<runs_num; ++i) {
    std::stringstream filename;
    filename << dirname << (first_run+i);
    runs[i] = new SortedRun(filename.str(), cell_size, segment_size_);
  }

  // Information about the output merged run
  void* cell;
  char* segment = new char[segment_size_];
  size_t offset = 0;
  std::stringstream new_filename;
  new_filename << dirname << new_run;
  int new_run_fd = open(new_filename.str().c_str(), 
                        O_WRONLY | O_CREAT | O_TRUNC | O_SYNC, S_IRWXU);
  assert(new_run_fd != -1);
 
  // Loop over the cells
  while((cell = get_next_cell<T>(runs, runs_num)) != NULL) {
    // If the output segment is full, flush it into the output file
    if(offset + cell_size > segment_size_) {
      write(new_run_fd, segment, offset);
      offset = 0;
    }

    // Put the new cell into the segment 
    memcpy(segment + offset, cell, cell_size);
    offset += cell_size;
  }

  // Flush the remaining data of the output segment into the output file
  if(offset > 0) 
    write(new_run_fd, segment, offset);

  // Delete the files of the merged runs
  for(int i=0; i<runs_num; ++i) 
    remove(runs[i]->filename_.c_str());

  // Clean up
  delete [] segment;
  for(int i=0; i<runs_num; ++i)
    delete runs[i];
  delete [] runs;
  close(new_run_fd);
}

template<class T>
void StorageManager::Fragment::merge_sorted_runs_with_id(
    int first_run, int last_run, int new_run) {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  size_t cell_size = sizeof(int64_t) + array_schema_->cell_size();
  struct stat st;
  std::string dirname = workspace_ + "/" + SM_TEMP + "/" + 
                        array_schema_->array_name() + "_" + 
                        fragment_name_ + "/";

  // Information about the runs to be merged 
  int runs_num = last_run-first_run+1;
  SortedRun** runs = new SortedRun*[runs_num];
  for(int i=0; i<runs_num; ++i) {
    std::stringstream filename;
    filename << dirname << (first_run+i);
    runs[i] = new SortedRun(filename.str(), cell_size, segment_size_);
  }

  // Information about the output merged run
  void* cell;
  char* segment = new char[segment_size_];
  size_t offset = 0;
  std::stringstream new_filename;
  new_filename << dirname << new_run;
  int new_run_fd = open(new_filename.str().c_str(), 
                        O_WRONLY | O_CREAT | O_TRUNC | O_SYNC, S_IRWXU);
  assert(new_run_fd != -1);
 
  // Loop over the cells
  while((cell = get_next_cell_with_id<T>(runs, runs_num)) != NULL) {
    // If the output segment is full, flush it into the output file
    if(offset + cell_size > segment_size_) {
      write(new_run_fd, segment, offset);
      offset = 0;
    }

    // Put the new cell into the segment 
    memcpy(segment + offset, cell, cell_size);
    offset += cell_size;
  }

  // Flush the remaining data of the output segment into the output file
  if(offset > 0) 
    write(new_run_fd, segment, offset);

  // Delete the files of the merged runs
  for(int i=0; i<runs_num; ++i) 
    remove(runs[i]->filename_.c_str());

  // Clean up
  delete [] segment;
  for(int i=0; i<runs_num; ++i)
    delete runs[i];
  delete [] runs;
  close(new_run_fd);
}


template<class T>
void StorageManager::Fragment::merge_sorted_runs_with_2_ids(
    int first_run, int last_run, int new_run) {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  size_t cell_size = 2*sizeof(int64_t) + array_schema_->cell_size();
  struct stat st;
  std::string dirname = workspace_ + "/" + SM_TEMP + "/" + 
                        array_schema_->array_name() + "_" + 
                        fragment_name_ + "/";

  // Information about the runs to be merged 
  int runs_num = last_run-first_run+1;
  SortedRun** runs = new SortedRun*[runs_num];
  for(int i=0; i<runs_num; ++i) {
    std::stringstream filename;
    filename << dirname << (first_run+i);
    runs[i] = new SortedRun(filename.str(), cell_size, segment_size_);
  }

  // Information about the output merged run
  void* cell;
  char* segment = new char[segment_size_];
  size_t offset = 0;
  std::stringstream new_filename;
  new_filename << dirname << new_run;
  int new_run_fd = open(new_filename.str().c_str(), 
                        O_WRONLY | O_CREAT | O_TRUNC | O_SYNC, S_IRWXU);
  assert(new_run_fd != -1);

  // Loop over the cells
  while((cell = get_next_cell_with_2_ids<T>(runs, runs_num)) != NULL) {
    // If the output segment is full, flush it into the output file
    if(offset + cell_size > segment_size_) {
      write(new_run_fd, segment, offset);
      offset = 0;
    }

    // Put the new cell into the segment 
    memcpy(segment + offset, cell, cell_size);
    offset += cell_size;
  }

  // Flush the remaining data of the output segment into the output file
  if(offset > 0) 
    write(new_run_fd, segment, offset);

  // Delete the files of the merged runs
  for(int i=0; i<runs_num; ++i) 
    remove(runs[i]->filename_.c_str());

  // Clean up
  delete [] segment;
  for(int i=0; i<runs_num; ++i)
    delete runs[i];
  delete [] runs;
  close(new_run_fd);
}

template<class T>
void StorageManager::Fragment::merge_sorted_runs_with_id() {
  int runs_per_merge = double(write_state_max_size_)/segment_size_-1;
  int merges;

  while(write_state_->runs_num_ > runs_per_merge) {
    merges = ceil(double(write_state_->runs_num_) / runs_per_merge);

    for(int i=0; i<merges; ++i) 
      merge_sorted_runs_with_id<T>(
          i*runs_per_merge, 
          std::min((i+1)*runs_per_merge-1, write_state_->runs_num_-1), 
          i);

    write_state_->runs_num_ = merges; 
  }
}

template<class T>
void StorageManager::Fragment::merge_sorted_runs_with_2_ids() {
  int runs_per_merge = double(write_state_max_size_)/segment_size_-1;
  int merges;

  while(write_state_->runs_num_ > runs_per_merge) {
    merges = ceil(double(write_state_->runs_num_) / runs_per_merge);

    for(int i=0; i<merges; ++i) 
      merge_sorted_runs_with_2_ids<T>(
          i*runs_per_merge, 
          std::min((i+1)*runs_per_merge-1, write_state_->runs_num_-1), 
          i);

    write_state_->runs_num_ = merges; 
  }
}

void StorageManager::Fragment::sort_run() {
  // For easy reference
  int dim_num = array_schema_->dim_num();
  int attribute_num = array_schema_->attribute_num();
  const std::type_info* coords_type = array_schema_->type(attribute_num);
  ArraySchema::CellOrder cell_order = array_schema_->cell_order();

  // Sort the cells
  if(cell_order == ArraySchema::CO_ROW_MAJOR) {
    if(*coords_type == typeid(int)) {
      std::sort(write_state_->cells_.begin(), write_state_->cells_.end(), 
                SmallerRow<int>(dim_num));
    } else if(*coords_type == typeid(int64_t)) {
      std::sort(write_state_->cells_.begin(), write_state_->cells_.end(), 
                SmallerRow<int64_t>(dim_num));
    } else if(*coords_type == typeid(float)) {
      std::sort(write_state_->cells_.begin(), write_state_->cells_.end(), 
                SmallerRow<float>(dim_num));
    } else if(*coords_type == typeid(double)) {
      std::sort(write_state_->cells_.begin(), write_state_->cells_.end(), 
                SmallerRow<double>(dim_num));
    }
  } else if(cell_order == ArraySchema::CO_COLUMN_MAJOR) {
    if(*coords_type == typeid(int)) {
      std::sort(write_state_->cells_.begin(), write_state_->cells_.end(), 
                SmallerCol<int>(dim_num));
    } else if(*coords_type == typeid(int64_t)) {
      std::sort(write_state_->cells_.begin(), write_state_->cells_.end(), 
                SmallerCol<int64_t>(dim_num));
    } else if(*coords_type == typeid(float)) {
      std::sort(write_state_->cells_.begin(), write_state_->cells_.end(), 
                SmallerCol<float>(dim_num));
    } else if(*coords_type == typeid(double)) {
      std::sort(write_state_->cells_.begin(), write_state_->cells_.end(), 
                SmallerCol<double>(dim_num));
    }
  }
}

void StorageManager::Fragment::sort_run_with_id() {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  int dim_num = array_schema_->dim_num();
  const std::type_info* coords_type = array_schema_->type(attribute_num);
  ArraySchema::TileOrder tile_order = array_schema_->tile_order();
  ArraySchema::CellOrder cell_order = array_schema_->cell_order();

  if(tile_order == ArraySchema::TO_NONE || // Irregular + Hilbert co
     cell_order == ArraySchema::CO_ROW_MAJOR) { // Regular + row co
    if(*coords_type == typeid(int)) {
      std::sort(write_state_->cells_with_id_.begin(), 
                write_state_->cells_with_id_.end(), 
                SmallerRowWithId<int>(dim_num));
    } else if(*coords_type == typeid(int64_t)) {
      std::sort(write_state_->cells_with_id_.begin(), 
                write_state_->cells_with_id_.end(), 
                SmallerRowWithId<int64_t>(dim_num));
    } else if(*coords_type == typeid(float)) {
      std::sort(write_state_->cells_with_id_.begin(), 
                write_state_->cells_with_id_.end(), 
                SmallerRowWithId<float>(dim_num));
    } else if(*coords_type == typeid(double)) {
      std::sort(write_state_->cells_with_id_.begin(), 
                write_state_->cells_with_id_.end(), 
                SmallerRowWithId<double>(dim_num));
    }
  } else if(cell_order == ArraySchema::CO_COLUMN_MAJOR) { // Rregular + col co
    if(*coords_type == typeid(int)) {
      std::sort(write_state_->cells_with_id_.begin(), 
                write_state_->cells_with_id_.end(), 
                SmallerColWithId<int>(dim_num));
    } else if(*coords_type == typeid(int64_t)) {
      std::sort(write_state_->cells_with_id_.begin(), 
                write_state_->cells_with_id_.end(), 
                SmallerColWithId<int64_t>(dim_num));
    } else if(*coords_type == typeid(float)) {
      std::sort(write_state_->cells_with_id_.begin(), 
                write_state_->cells_with_id_.end(), 
                SmallerColWithId<float>(dim_num));
    } else if(*coords_type == typeid(double)) {
      std::sort(write_state_->cells_with_id_.begin(), 
                write_state_->cells_with_id_.end(), 
                SmallerColWithId<double>(dim_num));
    }
  }
}

void StorageManager::Fragment::sort_run_with_2_ids() {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  int dim_num = array_schema_->dim_num();
  const std::type_info* coords_type = array_schema_->type(attribute_num);

  if(*coords_type == typeid(int)) {
    std::sort(write_state_->cells_with_2_ids_.begin(), 
              write_state_->cells_with_2_ids_.end(), 
              SmallerWith2Ids<int>(dim_num));
  } else if(*coords_type == typeid(int64_t)) {
    std::sort(write_state_->cells_with_2_ids_.begin(), 
              write_state_->cells_with_2_ids_.end(), 
              SmallerWith2Ids<int64_t>(dim_num));
  } else if(*coords_type == typeid(float)) {
    std::sort(write_state_->cells_with_2_ids_.begin(), 
              write_state_->cells_with_2_ids_.end(), 
              SmallerWith2Ids<float>(dim_num));
  } else if(*coords_type == typeid(double)) {
    std::sort(write_state_->cells_with_2_ids_.begin(), 
              write_state_->cells_with_2_ids_.end(), 
              SmallerWith2Ids<double>(dim_num));
  }
}

template<class T>
void StorageManager::Fragment::update_tile_info(
    const T* coords, int64_t tile_id) {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  int dim_num = array_schema_->dim_num();
  size_t coords_size = array_schema_->cell_size(attribute_num);

  // Update MBR and (potentially) the first bounding coordinate
  if(write_state_->cell_num_ == 0) {
    init_mbr(coords, static_cast<T*>(write_state_->mbr_), dim_num);
    memcpy(write_state_->bounding_coordinates_.first, coords, coords_size);
  } else {
    expand_mbr(coords, static_cast<T*>(write_state_->mbr_), dim_num);
  }

  // Update the second bounding coordinate, tile id, and cell number
  memcpy(write_state_->bounding_coordinates_.second, coords, coords_size);
  write_state_->tile_id_ = tile_id;
  ++write_state_->cell_num_;

  // Update file offsets
  for(int i=0; i<=attribute_num; ++i)
    write_state_->file_offsets_[i] += array_schema_->cell_size(i);
}

void StorageManager::Fragment::write_cell(
    const StorageManager::Cell& cell) {

  size_t cell_size = sizeof(Cell) + array_schema_->cell_size();

  if(write_state_->run_size_ + cell_size > write_state_max_size_) {
    sort_run();
    flush_sorted_run();
  }

  write_state_->cells_.push_back(cell);
  write_state_->run_size_ += cell_size;
}

void StorageManager::Fragment::write_cell(
    const StorageManager::CellWithId& cell) {
  size_t size_cost = sizeof(CellWithId) + array_schema_->cell_size();

  if(write_state_->run_size_ + size_cost > write_state_max_size_) {
    sort_run_with_id();
    flush_sorted_run_with_id();
  }

  write_state_->cells_with_id_.push_back(cell);
  write_state_->run_size_ += size_cost;
}

void StorageManager::Fragment::write_cell(
    const StorageManager::CellWith2Ids& cell) {
  size_t size_cost = sizeof(CellWith2Ids) + array_schema_->cell_size();

  if(write_state_->run_size_ + size_cost > write_state_max_size_) {
    sort_run_with_2_ids();
    flush_sorted_run_with_2_ids();
  }

  write_state_->cells_with_2_ids_.push_back(cell);
  write_state_->run_size_ += size_cost;
}

// Note: This function applies only to irregular tiles
template<class T>
void StorageManager::Fragment::write_cell_sorted(const void* cell) {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();

  // Flush tile info to book-keeping if a new tile must be created
  if(write_state_->cell_num_ == array_schema_->capacity())
    flush_tile_info_to_book_keeping();

  // Append coordinates to segment  
  append_cell_to_segment(cell, attribute_num);
  size_t cell_offset = array_schema_->cell_size(attribute_num);

  // Append attribute values to the respective segments
  for(int i=0; i<attribute_num; ++i) {
    append_cell_to_segment(static_cast<const char*>(cell) + cell_offset, i);
    cell_offset += array_schema_->cell_size(i);
  }
    
  // Update the info of the currently populated tile
  int64_t tile_id = (write_state_->cell_num_) ? write_state_->tile_id_ 
                                              : write_state_->tile_id_ + 1;
  update_tile_info(static_cast<const T*>(cell), tile_id);
}

template<class T>
void StorageManager::Fragment::write_cell_sorted_with_id(const void* cell) {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  bool regular = array_schema_->has_regular_tiles();
  int64_t id = *(static_cast<const int64_t*>(cell));
  const void* coords = static_cast<const char*>(cell) + sizeof(int64_t);

  // Flush tile info to book-keeping if a new tile must be created
  if((regular && id != write_state_->tile_id_) || 
     (!regular && (write_state_->cell_num_ == array_schema_->capacity())))
    flush_tile_info_to_book_keeping();

  // Append coordinates to segment  
  append_cell_to_segment(coords, attribute_num);

  // Append attribute values to the respective segments
  size_t cell_offset = array_schema_->cell_size(attribute_num);
  for(int i=0; i<attribute_num; ++i) {
    append_cell_to_segment(static_cast<const char*>(coords) + cell_offset, i);
    cell_offset += array_schema_->cell_size(i);
  }
    
  // Update the info of the currently populated tile
  update_tile_info<T>(static_cast<const T*>(coords), id);
}

template<class T>
void StorageManager::Fragment::write_cell_sorted_with_2_ids(const void* cell) {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  int64_t id = *(static_cast<const int64_t*>(cell));
  const void* coords = static_cast<const char*>(cell) + 2*sizeof(int64_t);

  // Flush tile info to book-keeping if a new tile must be created
  if(id != write_state_->tile_id_)    
    flush_tile_info_to_book_keeping();

  // Append coordinates to segment  
  append_cell_to_segment(coords, attribute_num);

  // Append attribute values to the respective segments
  size_t cell_offset = array_schema_->cell_size(attribute_num); 
  for(int i=0; i<attribute_num; ++i) {
    append_cell_to_segment(static_cast<const char*>(coords) + cell_offset, i);
    cell_offset += array_schema_->cell_size(i);
  }
    
  // Update the info of the currently populated tile
  update_tile_info<T>(static_cast<const T*>(coords), id);
}

/*----------------- TILE ITERATORS ------------------*/

StorageManager::Fragment::const_tile_iterator::const_tile_iterator() 
    : fragment_(NULL), end_(true) {
}

StorageManager::Fragment::const_tile_iterator::const_tile_iterator(
    Fragment* fragment, int attribute_id, int64_t pos)
    : fragment_(fragment), attribute_id_(attribute_id), pos_(pos) {
  if(pos_ >= 0 && pos_ < fragment_->book_keeping_.tile_ids_.size())
    end_ = false;
  else
    end_ = true;
}

void StorageManager::Fragment::const_tile_iterator::operator=(
    const Fragment::const_tile_iterator& rhs) {
  fragment_ = rhs.fragment_;
  attribute_id_ = rhs.attribute_id_;
  end_ = rhs.end_;
  pos_ = rhs.pos_;
}

StorageManager::Fragment::const_tile_iterator 
StorageManager::Fragment::const_tile_iterator::operator+(int64_t step) {
  const_tile_iterator it = *this;
  it.pos_ += step;
  if(it.pos_ >= 0 && it.pos_ < fragment_->book_keeping_.tile_ids_.size())
    it.end_ = false;
  else
    it.end_ = true;
  return it;
}

void StorageManager::Fragment::const_tile_iterator::operator+=(int64_t step) {
  pos_ += step;
  if(pos_ >= 0 && pos_ < fragment_->book_keeping_.tile_ids_.size())
    end_ = false;
  else
    end_ = true;
}

StorageManager::Fragment::const_tile_iterator 
StorageManager::Fragment::const_tile_iterator::operator++() {
  ++pos_;
  
  if(pos_ >= 0 && pos_ < fragment_->book_keeping_.tile_ids_.size())
    end_ = false;
  else
    end_ = true;

  return *this;
}

StorageManager::Fragment::const_tile_iterator 
StorageManager::Fragment::const_tile_iterator::operator++(int junk) {
  const_tile_iterator it = *this;
   ++pos_;
  
  if(pos_ >= 0 && pos_ < fragment_->book_keeping_.tile_ids_.size())
    end_ = false;
  else
    end_ = true;

  return it;
}

bool StorageManager::Fragment::const_tile_iterator::operator==(
    const Fragment::const_tile_iterator& rhs) const {
  return (pos_ == rhs.pos_ && attribute_id_ == rhs.attribute_id_ &&
          fragment_ == rhs.fragment_); 
}

bool StorageManager::Fragment::const_tile_iterator::operator!=(
    const Fragment::const_tile_iterator& rhs) const {
  return (!(pos_ == rhs.pos_ && attribute_id_ == rhs.attribute_id_ &&
          fragment_ == rhs.fragment_)); 
}

const Tile* StorageManager::Fragment::const_tile_iterator::operator*() const {
  assert(pos_ >= 0 && pos_ < fragment_->book_keeping_.tile_ids_.size());

  return fragment_->get_tile_by_pos(attribute_id_, pos_);
}

const ArraySchema* 
StorageManager::Fragment::const_tile_iterator::array_schema() const {
  return fragment_->array_schema_;
}

StorageManager::BoundingCoordinatesPair 
StorageManager::Fragment::const_tile_iterator::bounding_coordinates() const {
  return fragment_->book_keeping_.bounding_coordinates_[pos_];
}

StorageManager::MBR 
StorageManager::Fragment::const_tile_iterator::mbr() const {
  return fragment_->book_keeping_.mbrs_[pos_];
}

int64_t StorageManager::Fragment::const_tile_iterator::tile_id() const {
  return fragment_->book_keeping_.tile_ids_[pos_];
}

/******************************************************
*********************** ARRAY *************************
******************************************************/

StorageManager::Array::Array(
    const std::string& workspace, size_t segment_size,
    size_t write_state_max_size,
    const ArraySchema* array_schema, const char* mode) 
    : array_schema_(array_schema), workspace_(workspace),
      segment_size_(segment_size),
      write_state_max_size_(write_state_max_size) {
  strcpy(mode_, mode);
  load_fragment_tree();
  open_fragments();
}

StorageManager::Array::~Array() {
  if(strcmp(mode_, "w") == 0 || strcmp(mode_, "a"))
    flush_fragment_tree();

  close_fragments();

  delete array_schema_;
}

void StorageManager::Array::close_fragments() {
  for(int i=0; i<fragments_.size(); ++i)
    delete fragments_[i];

  fragments_.clear();
}

void StorageManager::Array::flush_fragment_tree() {
  // For easy reference
  int level_num = fragment_tree_.size(); 

  // Do nothing if there are not fragments
  if(level_num == 0) 
    return;

  // Initialize buffer
  size_t buffer_size = level_num * 2 * sizeof(int);
  char* buffer = new char[buffer_size];

  // Populate buffer
  for(int i=0; i<level_num; ++i) {
    memcpy(buffer+2*i*sizeof(int), &(fragment_tree_[i].first), sizeof(int));
    memcpy(buffer+(2*i+1)*sizeof(int), 
           &(fragment_tree_[i].second), sizeof(int));
  }

  // Name of the fragment tree file
  std::string filename = workspace_ + array_schema_->array_name() + "/" +
                         SM_FRAGMENT_TREE_FILENAME + 
                         SM_BOOK_KEEPING_FILE_SUFFIX;

  // Delete fragment tree file if it exists
  remove(filename.c_str());

  // Open file
  int fd = open(filename.c_str(), 
                O_WRONLY | O_CREAT | O_TRUNC | O_SYNC, S_IRWXU);
  assert(fd != -1);

  // Flush the buffer into the file
  write(fd, buffer, buffer_size);

  // Clean up
  close(fd);
  delete [] buffer;
  fragment_tree_.clear();
}

std::vector<std::string> StorageManager::Array::get_fragment_names() const {
  std::vector<std::string> fragment_names;

  // Return nothing if there are no fragments
  if(fragment_tree_.size() == 0)
    return fragment_names;

  // For easy reference
  int level;
  int64_t start_seq = 0, end_seq, subtree_size;
  std::stringstream ss;
  int consolidation_step = array_schema_->consolidation_step();

  // Eager consolidation
  if(consolidation_step == 1) {
    ss << "0_" << next_fragment_seq_-1;
    fragment_names.push_back(ss.str());
  } else { // Lazy consolidation
    for(int i=0; i<fragment_tree_.size(); ++i) {
      level = fragment_tree_[i].first;
      for(int j=0; j < fragment_tree_[i].second; ++j) {
        subtree_size = pow(double(consolidation_step), level); 
        end_seq = start_seq + subtree_size - 1;
        ss.str(std::string());
        ss << start_seq << "_" << end_seq;
        fragment_names.push_back(ss.str());
        start_seq += subtree_size;
      } 
    }
  }

  return fragment_names;
}

void StorageManager::Array::load_fragment_tree() {
  // For easy reference
  const std::string& array_name = array_schema_->array_name();
  int consolidation_step = array_schema_->consolidation_step();

  // Load the fragment book-keeping info from the disk into a buffer
  char* buffer = NULL;
  size_t buffer_size = 0;
  next_fragment_seq_ = 0;

  // Name of the fragment tree file
  std::string filename = workspace_ + array_schema_->array_name() + "/" +
                         SM_FRAGMENT_TREE_FILENAME + 
                         SM_BOOK_KEEPING_FILE_SUFFIX;

  // Open file
  int fd = open(filename.c_str(), O_RDONLY);

  // If the file does not exist, the tree is empty
  if(fd == -1)
    return;

  // Initialize buffer
  struct stat st;
  fstat(fd, &st);
  buffer_size = st.st_size;
  assert(buffer_size != 0);
  buffer = new char[buffer_size];
 
  // Load contents of the file into the buffer
  read(fd, buffer, buffer_size);
 
  // Create the tree from the buffer and calculate next fragment sequence number
  int level_num = buffer_size / (2 * sizeof(int));
  int level, node_num;
  for(int i=0; i<level_num; ++i) {
    memcpy(&level, buffer+2*i*sizeof(int), sizeof(int));
    memcpy(&node_num, buffer+(2*i+1)*sizeof(int), sizeof(int));
    fragment_tree_.push_back(FragmentTreeLevel(level, node_num));
    next_fragment_seq_ += pow(double(consolidation_step), level) * node_num;
  }

  // Clean up
  close(fd);
  delete [] buffer;
}

void StorageManager::Array::new_fragment() {
  // Create a new Fragment object
  std::stringstream fragment_name;
  fragment_name << next_fragment_seq_ << "_" << next_fragment_seq_; 
  fragments_.push_back(new Fragment(workspace_, segment_size_,
                                    write_state_max_size_,
                                    array_schema_, fragment_name.str()));

  // Add fragment to tree
  int level_num = fragment_tree_.size();
  if(level_num == 0 || fragment_tree_[level_num-1].first > 0) {
    fragment_tree_.push_back(FragmentTreeLevel(0,1));
  } else {
    ++(fragment_tree_[level_num-1].second);
  }

  // Update the next fragment sequence
  ++next_fragment_seq_;
}

void StorageManager::Array::open_fragments() {
  // Get fragment names
  std::vector<std::string> fragment_names = get_fragment_names();

  // Create a new Fragment object
  for(int i=0; i<fragment_names.size(); ++i) 
    fragments_.push_back(new Fragment(workspace_, segment_size_, 
                                      write_state_max_size_,
                                      array_schema_, fragment_names[i]));
}

void StorageManager::Array::write_cell(
    const StorageManager::Cell& cell) {
  assert(strcmp(mode_, "w") == 0 || strcmp(mode_, "a") == 0);

  fragments_.back()->write_cell(cell);
}

void StorageManager::Array::write_cell(
    const StorageManager::CellWithId& cell) {
  assert(strcmp(mode_, "w") == 0 || strcmp(mode_, "a") == 0);

  fragments_.back()->write_cell(cell);
}

void StorageManager::Array::write_cell(
    const StorageManager::CellWith2Ids& cell) {
  assert(strcmp(mode_, "w") == 0 || strcmp(mode_, "a") == 0);

  fragments_.back()->write_cell(cell);
}

template<class T>
void StorageManager::Array::write_cell_sorted(const void* cell) {
  fragments_.back()->write_cell_sorted<T>(cell);
}

/*----------------- TILE ITERATORS ------------------*/

StorageManager::Fragment::const_tile_iterator StorageManager::Array::begin(
    Fragment* fragment, int attribute_id) const {
  // Check attribute id
  assert(attribute_id <= array_schema_->attribute_num());

  if(fragment->book_keeping_.tile_ids_.size() > 0) 
    return Fragment::const_tile_iterator(fragment, attribute_id, 0);
  else
    return Fragment::const_tile_iterator();

}

/*----------------- CELL ITERATORS ------------------*/

template<class T>
StorageManager::Array::const_cell_iterator<T>::const_cell_iterator() {
  array_ = NULL;
  cell_ = NULL;
  cell_its_ = NULL;
  end_ = false;
  range_ = NULL;
  tile_its_ = NULL;
  full_overlap_ = NULL;
}

template<class T>
StorageManager::Array::const_cell_iterator<T>::const_cell_iterator(
    Array* array) : array_(array) {
  // Initialize private attributes
  attribute_num_ = array_->array_schema_->attribute_num();
  dim_num_ = array_->array_schema_->dim_num();
  fragment_num_ = array_->fragments_.size();
  end_ = false;
  cell_ = malloc(array_->array_schema_->cell_size());
  range_ = NULL;
  full_overlap_ = NULL;

  // Create tile and cell iterators
  tile_its_ = new Fragment::const_tile_iterator*[fragment_num_];
  cell_its_ = new Tile::const_cell_iterator*[fragment_num_];

  for(int i=0; i<fragment_num_; ++i) {
    tile_its_[i] = new Fragment::const_tile_iterator[attribute_num_+1];
   cell_its_[i] = new Tile::const_cell_iterator[attribute_num_+1];
  }

  // Get first cell
  init_iterators();
  int fragment_id = get_next_cell(); 
  if(fragment_id != -1)
    advance_cell(fragment_id); 
}

template<class T>
StorageManager::Array::const_cell_iterator<T>::const_cell_iterator(
    Array* array, const T* range) : array_(array) {
  // Initialize private attributes
  attribute_num_ = array_->array_schema_->attribute_num();
  dim_num_ = array_->array_schema_->dim_num();
  fragment_num_ = array_->fragments_.size();
  end_ = false;
  cell_ = malloc(array_->array_schema_->cell_size());
  range_ = new T[2*dim_num_];
  full_overlap_ = new bool[fragment_num_];
  memcpy(range_, range, 2*dim_num_*sizeof(T));

  // Create tile and cell iterators
  tile_its_ = new Fragment::const_tile_iterator*[fragment_num_];
  cell_its_ = new Tile::const_cell_iterator*[fragment_num_];

  for(int i=0; i<fragment_num_; ++i) {
    tile_its_[i] = new Fragment::const_tile_iterator[attribute_num_+1];
    cell_its_[i] = new Tile::const_cell_iterator[attribute_num_+1];
  }

  // Get first cell
  init_iterators_in_range();
  for(int i=0; i<fragment_num_; ++i)
    find_next_cell_in_range(i);
  int fragment_id = get_next_cell(); 
  if(fragment_id != -1)
    advance_cell_in_range(fragment_id); 
}

template<class T>
StorageManager::Array::const_cell_iterator<T>::~const_cell_iterator() {
  if(cell_ != NULL) 
    free(cell_);

  if(cell_its_ != NULL) {
    for(int i=0; i<fragment_num_; ++i)
      delete [] cell_its_[i];
    delete [] cell_its_; 
  }

  if(tile_its_ != NULL) {
    for(int i=0; i<fragment_num_; ++i)
      delete [] tile_its_[i];
    delete [] tile_its_; 
  }

  if(range_ != NULL)
    delete [] range_;

  if(full_overlap_ != NULL)
    delete [] full_overlap_;
}

template<class T>
void StorageManager::Array::const_cell_iterator<T>::advance_cell(
    int fragment_id) {
  // Advance cell iterators
  for(int j=0; j<=attribute_num_; ++j)
    ++cell_its_[fragment_id][j];

  // Potentially advance also tile iterators
  if(cell_its_[fragment_id][attribute_num_].end()) {
    // Advance tile iterators
    for(int j=0; j<=attribute_num_; ++j) 
      ++tile_its_[fragment_id][j];

    // Initialize cell iterators
    if(!tile_its_[fragment_id][attribute_num_].end()) {
      for(int j=0; j<=attribute_num_; ++j) 
        cell_its_[fragment_id][j] = (*tile_its_[fragment_id][j])->begin();
    }
  }
}

template<class T>
void StorageManager::Array::const_cell_iterator<T>::advance_cell_in_range(
    int fragment_id) {
  // Advance cell iterators
  for(int j=0; j<=attribute_num_; ++j)
    ++cell_its_[fragment_id][j];

  find_next_cell_in_range(fragment_id);
}

template<class T>
bool StorageManager::Array::const_cell_iterator<T>::end() const {
  return end_;
}

template<class T>
void StorageManager::Array::const_cell_iterator<T>::find_next_cell_in_range(
    int fragment_id) {

  // The loop will be broken when a cell in range is found, or
  // all cells are exhausted
  while(1) { 
    // If not in the end of the tile
    if(!cell_its_[fragment_id][attribute_num_].end() &&
       !full_overlap_[fragment_id]) {
      const void* coords;
      const T* point;
      while(!cell_its_[fragment_id][attribute_num_].end()) {
        coords = *cell_its_[fragment_id][attribute_num_];
        point = static_cast<const T*>(coords);
        if(inside_range(point, range_, dim_num_)) 
          break; // cell found
        ++cell_its_[fragment_id][attribute_num_];
      }

    }



    // If the end of the tile is reached (cell not found yet)
    if(cell_its_[fragment_id][attribute_num_].end()) {
      // Advance coordinate tile iterator
      ++tile_its_[fragment_id][attribute_num_];

      // Find the first coordinate tile that overlaps with the range
      const T* mbr;
      std::pair<bool, bool> tile_overlap;
      while(!tile_its_[fragment_id][attribute_num_].end()) {
        mbr = static_cast<const T*>(
                  tile_its_[fragment_id][attribute_num_].mbr());
        tile_overlap = overlap(mbr, range_, dim_num_); 
        if(tile_overlap.first) { 
          full_overlap_[fragment_id] = tile_overlap.second;
          break;  // next tile found
        }
        ++tile_its_[fragment_id][attribute_num_];
      } 

      if(tile_its_[fragment_id][attribute_num_].end())
        break; // cell cannot be found
      else // initialize coordinates cell iterator
        cell_its_[fragment_id][attribute_num_] = 
            (*tile_its_[fragment_id][attribute_num_])->begin();

    } else { // Not the end of the cells in this tile
      break; // cell found
    }
  }

  // Syncronize attribute cell and tile iterators
  for(int j=0; j<attribute_num_; ++j) {
    tile_its_[fragment_id][j] = 
        array_->begin(array_->fragments_[fragment_id], j);
    tile_its_[fragment_id][j] +=  
        tile_its_[fragment_id][attribute_num_].pos();
    if(!tile_its_[fragment_id][j].end()) {
      cell_its_[fragment_id][j] = 
          (*tile_its_[fragment_id][j])->begin();
      cell_its_[fragment_id][j] +=  
          cell_its_[fragment_id][attribute_num_].pos();
    } else {
      cell_its_[fragment_id][j] = Tile::end(); 
    }
  }    
}

template<class T>
int StorageManager::Array::const_cell_iterator<T>::get_next_cell() {
  // Get the first non-NULL coordinates
  const void *next_coords, *coords;
  int fragment_id;
  int f = 0;
  do {
    next_coords = *cell_its_[f][attribute_num_]; 
    ++f;
  } while((next_coords == NULL) && (f < fragment_num_)); 

  fragment_id = f-1;

  // Get the next coordinates in the global cell order
  for(int i=f; i<fragment_num_; ++i) {
    coords = *cell_its_[i][attribute_num_]; 
    if(coords != NULL && 
       array_->array_schema_->precedes(static_cast<const T*>(coords), 
                                       static_cast<const T*>(next_coords))) { 
      next_coords = coords;
      fragment_id = i;
    }
  }

  if(next_coords != NULL) { // There are cells
    char* cell = static_cast<char*>(cell_);
    int attribute_num = array_->array_schema_->attribute_num();
    size_t attr_size;
    size_t coords_size = array_->array_schema_->cell_size(attribute_num);
    // Copy coordinates to cell
    memcpy(cell, *(cell_its_[fragment_id][attribute_num]), coords_size);
    // Copy attributes to cell
    size_t offset = coords_size;
    for(int j=0; j<attribute_num_; ++j) { 
      attr_size = array_->array_schema_->cell_size(j);
      memcpy(cell + offset, *(cell_its_[fragment_id][j]), attr_size);
      offset += attr_size;
    }
    return fragment_id;
  } else { // No more cells
    cell_ = NULL;
    end_ = true;
    return -1;
  }
}

template<class T>
void StorageManager::Array::const_cell_iterator<T>::init_iterators() {
  for(int i=0; i<fragment_num_; ++i) {
    for(int j=0; j<=attribute_num_; ++j) {
      tile_its_[i][j] = array_->begin(array_->fragments_[i], j);
      cell_its_[i][j] = (*tile_its_[i][j])->begin();
    }
  }
}

template<class T>
void StorageManager::Array::const_cell_iterator<T>::init_iterators_in_range() {
  // Initialize tile and cell iterators 
  for(int i=0; i<fragment_num_; ++i) { 
    // Initialize coordinate tile iterator
    tile_its_[i][attribute_num_] =  
        array_->begin(array_->fragments_[i], attribute_num_);

    // Find the first coordinate tile that overlaps with the range
    const T* mbr;
    std::pair<bool, bool> tile_overlap;
    while(!tile_its_[i][attribute_num_].end()) {
      mbr = static_cast<const T*>(tile_its_[i][attribute_num_].mbr());
      tile_overlap = overlap(mbr, range_, dim_num_); 

      if(tile_overlap.first) { 
        full_overlap_[i] = tile_overlap.second;
        break;
      }
      ++tile_its_[i][attribute_num_];
    } 

    // Syncronize attribute tile iterators
    for(int j=0; j<attribute_num_; ++j) {
      tile_its_[i][j] = array_->begin(array_->fragments_[i], j);
      tile_its_[i][j] += tile_its_[i][attribute_num_].pos();
    }    

    // Initialize cell iterators
    for(int j=0; j<=attribute_num_; ++j) {
      if(!tile_its_[i][j].end())
        cell_its_[i][j] = (*tile_its_[i][j])->begin();
      else
        cell_its_[i][j] = Tile::end();
    }
  }
}

template<class T>
void StorageManager::Array::const_cell_iterator<T>::operator++() {
  int fragment_id = get_next_cell();

  // Advance cell
  if(fragment_id != -1) {
    if(range_ != NULL) 
      advance_cell_in_range(fragment_id);
    else 
      advance_cell(fragment_id);
  } 
}

template<class T>
const void* StorageManager::Array::const_cell_iterator<T>::operator*() {
  return cell_;
}

/******************************************************
******************** SORTED RUN ***********************
******************************************************/

StorageManager::SortedRun::SortedRun(
    const std::string& filename, size_t cell_size, size_t segment_size) 
    : cell_size_(cell_size), 
      segment_size_ (segment_size), 
      filename_(filename) {
  // Load the first segment
  offset_in_file_ = 0; 
  segment_ = new char[segment_size_];
  load_next_segment();
  assert(segment_ != NULL);
}

StorageManager::SortedRun::~SortedRun() {
  delete [] segment_;
}

void StorageManager::SortedRun::advance_cell() {
  assert(offset_in_segment_ < segment_utilization_);

  // Advance offset in segment
  offset_in_segment_ += cell_size_;

  // Potentially fetch a new segment from the file
  if(offset_in_segment_ >= segment_utilization_)
     load_next_segment();
}

void* StorageManager::SortedRun::current_cell() const {
  // End of file
  if(segment_utilization_ == 0)
    return NULL;

  assert(offset_in_segment_ + cell_size_ <= segment_utilization_);

  return segment_ + offset_in_segment_;
}

void StorageManager::SortedRun::load_next_segment() {
  assert(segment_ != NULL);

  // Read the segment
  int fd = open(filename_.c_str(), O_RDONLY);
  assert(fd != -1);
  lseek(fd, offset_in_file_, SEEK_SET);
  segment_utilization_ = read(fd, segment_, segment_size_);
  assert(segment_utilization_ != -1);

  // Update offsets
  offset_in_file_ += segment_utilization_;
  offset_in_segment_ = 0; 

  // Close file
  close(fd);
}

/******************************************************
****************** STORAGE MANAGER ********************
******************************************************/

StorageManager::StorageManager(const std::string& path, 
                               const MPIHandler* mpi_handler,
                               size_t segment_size)
    : segment_size_(segment_size), mpi_handler_(mpi_handler) {
  set_workspace(path);
  create_directory(workspace_);
  create_directory(workspace_ + "/" + SM_TEMP + "/"); 

  write_state_max_size_ = SM_WRITE_STATE_MAX_SIZE;

  arrays_ = new Array*[SM_MAX_OPEN_ARRAYS]; 
  for(int i=0; i<SM_MAX_OPEN_ARRAYS; ++i)
    arrays_[i] = NULL;
}

StorageManager::~StorageManager() {
  for(int i=0; i<SM_MAX_OPEN_ARRAYS; ++i) {
    if(arrays_[i] != NULL)
      close_array(i);
  }

  delete [] arrays_;
}

bool StorageManager::array_defined(const std::string& array_name) const {
  std::string filename = workspace_ + "/" + array_name + "/" + 
                         SM_ARRAY_SCHEMA_FILENAME + 
                         SM_BOOK_KEEPING_FILE_SUFFIX;

  int fd = open(filename.c_str(), O_RDONLY);

  if(fd == -1) {
    return false;
  } else {
    close(fd);
    return true;
  }
}

/*  substitute with array_empty

bool StorageManager::array_loaded(const std::string& array_name) const {
  std::string filename = workspace_ + "/" + array_name + "/" + 
                         SM_FRAGMENTS_FILENAME + 
                         SM_BOOK_KEEPING_FILE_SUFFIX;

  int fd = open(filename.c_str(), O_RDONLY);

  if(fd == -1) {
    return false;
  } else {
    close(fd);
    return true;
  }
}

*/

template<class T>
StorageManager::Array::const_cell_iterator<T> StorageManager::begin(
    int ad) const {
  assert(ad >= 0 && ad < SM_MAX_OPEN_ARRAYS);
  assert(arrays_[ad] != NULL);
  assert(strcmp(arrays_[ad]->mode_, "r") == 0);
  assert(!arrays_[ad]->empty());

  return Array::const_cell_iterator<T>(arrays_[ad]);
}

template<class T>
StorageManager::Array::const_cell_iterator<T> StorageManager::begin(
    int ad, const T* range) const {
  assert(ad >= 0 && ad < SM_MAX_OPEN_ARRAYS);
  assert(arrays_[ad] != NULL);
  assert(strcmp(arrays_[ad]->mode_, "r") == 0);
  assert(!arrays_[ad]->empty());

  return Array::const_cell_iterator<T>(arrays_[ad], range);
}

void StorageManager::clear_array(const std::string& array_name) {
  // Close the array if it is open 
  OpenArrays::iterator it = open_arrays_.find(array_name);
  if(it != open_arrays_.end())
    close_array(it->second);

  // Delete the entire array directory
  std::string dirname = workspace_ + "/" + array_name + "/";
  std::string filename; 

  struct dirent *next_file;
  DIR* dir = opendir(dirname.c_str());
  std::string fragments_filename = std::string(SM_FRAGMENT_TREE_FILENAME) + 
                                   SM_BOOK_KEEPING_FILE_SUFFIX;
  std::string array_schema_filename = std::string(SM_ARRAY_SCHEMA_FILENAME) + 
                                      SM_BOOK_KEEPING_FILE_SUFFIX;
  
  // If the directory does not exist, exit
  if(dir == NULL)
    return;

  while(next_file = readdir(dir)) {
    if(strcmp(next_file->d_name, ".") == 0 ||
       strcmp(next_file->d_name, "..") == 0 ||
       strcmp(next_file->d_name, array_schema_filename.c_str()) == 0)
      continue;
    if(strcmp(next_file->d_name, fragments_filename.c_str()) == 0)  {
      filename = dirname + next_file->d_name;
      remove(filename.c_str());
    }
    else {  // It is a fragment directory
      delete_directory(dirname + next_file->d_name);
    }
  } 
  
  closedir(dir);
}

void StorageManager::close_array(int ad) {
  if(arrays_[ad] == NULL)
    return;

  open_arrays_.erase(arrays_[ad]->array_schema_->array_name());
  delete arrays_[ad];
  arrays_[ad] = NULL;
}

void StorageManager::define_array(const ArraySchema* array_schema) const {
  // For easy reference
  const std::string& array_name = array_schema->array_name();

  // Create array directory
  std::string dir_name = workspace_ + "/" + array_name + "/"; 
  create_directory(dir_name);

  // Open file
  std::string filename = dir_name + 
                         SM_ARRAY_SCHEMA_FILENAME + 
                         SM_BOOK_KEEPING_FILE_SUFFIX;
  remove(filename.c_str());
  int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  assert(fd != -1);

  // Serialize array schema
  std::pair<const char*, size_t> ret = array_schema->serialize();
  const char* buffer = ret.first;
  size_t buffer_size = ret.second; 

  // Store the array schema
  write(fd, buffer, buffer_size); 

  delete [] buffer;
  close(fd);
}

void StorageManager::delete_array(const std::string& array_name) {
  // Close the array if it is open 
  OpenArrays::iterator it = open_arrays_.find(array_name);
  if(it != open_arrays_.end())
    close_array(it->second);

  // Delete the entire array directory
  std::string dirname = workspace_ + "/" + array_name + "/";
  std::string filename; 
  std::string fragments_filename = std::string(SM_FRAGMENT_TREE_FILENAME) + 
                                   SM_BOOK_KEEPING_FILE_SUFFIX;
  std::string array_schema_filename = std::string(SM_ARRAY_SCHEMA_FILENAME) + 
                                      SM_BOOK_KEEPING_FILE_SUFFIX;

  struct dirent *next_file;
  DIR* dir = opendir(dirname.c_str());
  
  // If the directory does not exist, exit
  if(dir == NULL)
    return;

  while(next_file = readdir(dir)) {
    if(strcmp(next_file->d_name, ".") == 0 ||
       strcmp(next_file->d_name, "..") == 0)
      continue;
    if(strcmp(next_file->d_name, fragments_filename.c_str()) == 0 ||
       strcmp(next_file->d_name, array_schema_filename.c_str()) == 0) {
      filename = dirname + next_file->d_name;
      remove(filename.c_str());
    }
    else {  // It is a fragment directory
      delete_directory(dirname + next_file->d_name);
    }
  } 
  
  closedir(dir);
  rmdir(dirname.c_str());
}


/*

void StorageManager::modify_array_schema(
    const ArraySchema* array_schema) const {
  // For easy reference
  const std::string& array_name = array_schema->array_name();


  // Open file
  std::string filename = workspace_ + "/" + array_name + "/" + 
                         SM_ARRAY_SCHEMA_FILENAME + 
                         SM_BOOK_KEEPING_FILE_SUFFIX;
  int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  assert(fd != -1);

  // Serialize array schema
  std::pair<const char*, size_t> ret = array_schema->serialize();
  const char* buffer = ret.first;
  size_t buffer_size = ret.second; 

  // Store the array schema
  write(fd, buffer, buffer_size); 

  delete [] buffer;
  close(fd);
}

void StorageManager::modify_fragment_bkp(
    const FragmentDescriptor* fd, int64_t capacity) const {
  assert(capacity > 0);

  // For easy reference
  const ArraySchema* array_schema = fd->fragment_info_->array_schema_;
  const std::string& array_name = array_schema->array_name();
  const std::string& fragment_name = fd->fragment_info_->fragment_name_;
  int attribute_num = array_schema->attribute_num();
  int64_t old_capacity = array_schema->capacity();
  size_t coords_cell_size = array_schema->cell_size(attribute_num);

  // New book-keeping structures
  FragmentInfo fragment_info;
  fragment_info.array_schema_ = array_schema;
  fragment_info.fragment_name_ = fragment_name;
  BoundingCoordinates& new_bounding_coordinates = 
      fragment_info.bounding_coordinates_;
  MBRs& new_mbrs = fragment_info.mbrs_;
  Offsets& new_offsets = fragment_info.offsets_;
  TileIds& new_tile_ids = fragment_info.tile_ids_;

  // Reserve proper space for the new book-keeping structures
  int64_t old_tile_num = fd->fragment_info_->tile_ids_.size();
  int64_t expected_new_tile_num = 
      ceil(double(old_tile_num) * old_capacity / capacity);
  new_bounding_coordinates.reserve(expected_new_tile_num);
  new_mbrs.reserve(expected_new_tile_num);
  new_offsets.resize(attribute_num+1);
  for(int i=0; i<=attribute_num; ++i)
    new_offsets[i].reserve(expected_new_tile_num);
  new_tile_ids.reserve(expected_new_tile_num);

  // Compute new book-keeping info
  int64_t new_tile_id = 0;
  int64_t cell_num = 0;
  const_tile_iterator tile_it = begin(fd, attribute_num); 
  const_tile_iterator tile_it_end = end(fd, attribute_num);
  Tile::const_cell_iterator cell_it, cell_it_end;
  BoundingCoordinatesPair bounding_coordinates;
  void* mbr = NULL;
  const void* coords;
  for(; tile_it != tile_it_end; ++tile_it) {
    cell_it = (*tile_it)->begin();
    cell_it_end = (*tile_it)->end();
    for(; cell_it != cell_it_end; ++cell_it) {
      coords = *cell_it;
      if(cell_num % capacity == 0) { // New tile
        if(cell_num != 0) { 
          new_mbrs.push_back(mbr); 
          new_bounding_coordinates.push_back(bounding_coordinates);
          new_tile_ids.push_back(new_tile_id++);
          for(int i=0; i<=attribute_num; ++i) {
            new_offsets[i].push_back((cell_num - capacity) * 
                                     array_schema->cell_size(i)); 
          }
          mbr = NULL;
        }
        bounding_coordinates.first = malloc(coords_cell_size);
        bounding_coordinates.second = malloc(coords_cell_size);
        memcpy(bounding_coordinates.first, coords, coords_cell_size);
      }
      expand_mbr(array_schema, coords, mbr);
      memcpy(bounding_coordinates.second, coords, coords_cell_size);
      ++cell_num;
    }
  }

  // Take into account the book-keeping info of the last new tile
  if(cell_num % capacity != 0) {
    new_mbrs.push_back(mbr); 
    new_bounding_coordinates.push_back(bounding_coordinates);
    new_tile_ids.push_back(new_tile_id++);
    for(int i=0; i<=attribute_num; ++i) {
      new_offsets[i].push_back((cell_num - (cell_num % capacity)) * 
                               array_schema->cell_size(i));  
    } 
  }

  // Flush the new book-keeping structures to disk
  flush_bounding_coordinates(fragment_info);
  flush_MBRs(fragment_info);   
  flush_offsets(fragment_info);
  flush_tile_ids(fragment_info);
}

*/

int StorageManager::open_array(
    const std::string& array_name, const char* mode) {
  // Proper checks
  check_on_open_array(array_name, mode);

  // If in write mode, delete the array if it exists
  if(strcmp(mode, "w") == 0) 
    clear_array(array_name); 

  // Initialize an Array object
  Array* array = new Array(workspace_, segment_size_,
                           write_state_max_size_,
                           get_array_schema(array_name), mode);

  // If the array is in write or append mode, initialize a new fragment
  if(strcmp(mode, "w") == 0 || strcmp(mode, "a") == 0)
    array->new_fragment();

  // Stores the Array object and returns an array descriptor
  int ad = store_array(array);

  // Maximum open arrays reached
  if(ad == -1) {
    delete array; 
    return -1;
  }

  // Keep track of the opened array
  open_arrays_[array_name] = ad;

  return ad;
}

void StorageManager::read_cells(int ad, const void* range, 
                                void*& cells, int64_t& cell_num) const {
  // For easy reference
  int attribute_num = arrays_[ad]->array_schema_->attribute_num();
  const std::type_info& coords_type = 
      *(arrays_[ad]->array_schema_->type(attribute_num));

  if(coords_type == typeid(int))
    read_cells<int>(ad, static_cast<const int*>(range), 
                    cells, cell_num);
  else if(coords_type == typeid(int64_t))
    read_cells<int64_t>(ad, static_cast<const int64_t*>(range), 
                        cells, cell_num);
  else if(coords_type == typeid(float))
    read_cells<float>(ad, static_cast<const float*>(range), 
                      cells, cell_num);
  else if(coords_type == typeid(double))
    read_cells<double>(ad, static_cast<const double*>(range), 
                       cells, cell_num);
}


void StorageManager::read_cells(int ad, const void* range,
                                void*& cells, int64_t& cell_num,
                                int rcv_rank) const {
  // For easy reference
  int attribute_num = arrays_[ad]->array_schema_->attribute_num();
  const std::type_info& coords_type = 
      *(arrays_[ad]->array_schema_->type(attribute_num));

  if(coords_type == typeid(int))
    read_cells<int>(ad, static_cast<const int*>(range), 
                    cells, cell_num, rcv_rank);
  else if(coords_type == typeid(int64_t))
    read_cells<int64_t>(ad, static_cast<const int64_t*>(range),
                        cells, cell_num, rcv_rank);
  else if(coords_type == typeid(float))
    read_cells<float>(ad, static_cast<const float*>(range),
                      cells, cell_num, rcv_rank);
  else if(coords_type == typeid(double))
    read_cells<double>(ad, static_cast<const double*>(range),
                       cells, cell_num, rcv_rank);
}

template<class T>
void StorageManager::read_cells(int ad, const T* range, 
                                void*& cells, int64_t& cell_num) const {
  // For easy reference
  const ArraySchema* array_schema = arrays_[ad]->array_schema_;
  size_t cell_size = array_schema->cell_size();

  // Initialize the cells buffer and cell num
  size_t buffer_size = (segment_size_ / cell_size) * cell_size;
  cells = malloc(buffer_size);
  cell_num = 0;
  size_t offset = 0;
 
  // Prepare cell iterator
  Array::const_cell_iterator<T> cell_it = begin<T>(ad, range);

  // Write cells into the CSV file
  for(; !cell_it.end(); ++cell_it) { 
    // Expand buffer
    if(offset == buffer_size) {
      expand_buffer(cells, buffer_size);
      buffer_size *= 2;
    } 
    memcpy(static_cast<char*>(cells) + offset, *cell_it, cell_size);
    offset += cell_size;
    ++cell_num;
  }
}

template<class T>
void StorageManager::read_cells(int ad, const T* range,
                                void*& cells, int64_t& cell_num,
                                int rcv_rank) const {
  assert(mpi_handler_ != NULL);

  // For easy reference
  size_t cell_size = arrays_[ad]->array_schema_->cell_size();

  // Read local cells in the range
  void* local_cells;
  int64_t local_cell_num;
  int all_cells_size;
  read_cells(ad, range, local_cells, local_cell_num);

  // Collect all cells from all processes
  void* all_cells;
  mpi_handler_->gather(local_cells, local_cell_num * cell_size,
                       all_cells, all_cells_size, rcv_rank); 

  if(rcv_rank == mpi_handler_->rank()) {
    cells = all_cells;
    cell_num = all_cells_size / cell_size; 
  }
}

void StorageManager::write_cell(int ad, const void* input_cell) const {
  assert(ad >= 0 && ad < SM_MAX_OPEN_ARRAYS);
  assert(arrays_[ad] != NULL);

  // For easy reference
  const ArraySchema* array_schema = arrays_[ad]->array_schema_;
  int dim_num = array_schema->dim_num();
  size_t cell_size = array_schema->cell_size();

  // Copy the input cell
  void* cell = malloc(cell_size);
  memcpy(cell, input_cell, cell_size);

  // Write each logical cell to the array
  if(array_schema->has_irregular_tiles()) { // Irregular tiles
    if(array_schema->cell_order() == ArraySchema::CO_ROW_MAJOR ||
       array_schema->cell_order() == ArraySchema::CO_COLUMN_MAJOR) {
      Cell new_cell;
      new_cell.cell_ = cell;
      arrays_[ad]->write_cell(new_cell); 
    } else { // array_schema->cell_order() == ArraySchema::CO_HILBERT
      CellWithId new_cell;
      new_cell.cell_ = cell;
      new_cell.id_ = array_schema->cell_id_hilbert(cell);
      arrays_[ad]->write_cell(new_cell); 
    }
  } else { // Regular tiles
    if(array_schema->tile_order() == ArraySchema::TO_ROW_MAJOR) { 
      if(array_schema->cell_order() == ArraySchema::CO_ROW_MAJOR ||
         array_schema->cell_order() == ArraySchema::CO_COLUMN_MAJOR) {
        CellWithId new_cell;
        new_cell.cell_ = cell;
        new_cell.id_ = array_schema->tile_id_row_major(cell);
        arrays_[ad]->write_cell(new_cell); 
      } else { // array_schema->cell_order() == ArraySchema::CO_HILBERT) {
        CellWith2Ids new_cell;
        new_cell.cell_ = cell;
        new_cell.tile_id_ = array_schema->tile_id_row_major(cell);
        new_cell.cell_id_ = array_schema->cell_id_hilbert(cell);
        arrays_[ad]->write_cell(new_cell); 
      }
    } else if(array_schema->tile_order() == ArraySchema::TO_COLUMN_MAJOR) { 
      if(array_schema->cell_order() == ArraySchema::CO_ROW_MAJOR ||
         array_schema->cell_order() == ArraySchema::CO_COLUMN_MAJOR) {
        CellWithId new_cell;
        new_cell.cell_ = cell;
        new_cell.id_ = array_schema->tile_id_column_major(cell);
        arrays_[ad]->write_cell(new_cell); 
      } else { // array_schema->cell_order() == ArraySchema::CO_HILBERT) {
        CellWith2Ids new_cell;
        new_cell.cell_ = cell;
        new_cell.tile_id_ = array_schema->tile_id_column_major(cell);
        new_cell.cell_id_ = array_schema->cell_id_hilbert(cell);
        arrays_[ad]->write_cell(new_cell); 
      }
    } else if(array_schema->tile_order() == ArraySchema::TO_HILBERT) { 
      if(array_schema->cell_order() == ArraySchema::CO_ROW_MAJOR ||
         array_schema->cell_order() == ArraySchema::CO_COLUMN_MAJOR) {
        CellWithId new_cell;
        new_cell.cell_ = cell;
        new_cell.id_ = array_schema->tile_id_hilbert(cell);
        arrays_[ad]->write_cell(new_cell); 
      } else { // array_schema->cell_order() == ArraySchema::CO_HILBERT) {
        CellWith2Ids new_cell;
        new_cell.cell_ = cell;
        new_cell.tile_id_ = array_schema->tile_id_hilbert(cell);
        new_cell.cell_id_ = array_schema->cell_id_hilbert(cell);
        arrays_[ad]->write_cell(new_cell); 
      }
    } 
  }
}

void StorageManager::write_cells(
    int ad, const void* cells, int64_t cell_num) const {
  // For easy reference
  size_t cell_size = arrays_[ad]->array_schema_->cell_size();
  size_t offset = 0;

  for(int64_t i=0; i<cell_num; ++i) {
    write_cell(ad, static_cast<const char*>(cells) + offset);
    offset += cell_size;
  }
}

template<class T>
void StorageManager::write_cell_sorted(int ad, const void* cell) const {
  arrays_[ad]->write_cell_sorted<T>(cell); 
}

void StorageManager::write_cells_sorted(
    int ad, const void* cells, int64_t cell_num) const {
  // For easy reference
  int attribute_num = arrays_[ad]->array_schema_->attribute_num();
  const std::type_info& coords_type = 
      *(arrays_[ad]->array_schema_->type(attribute_num));

  if(coords_type == typeid(int))
    write_cells_sorted<int>(ad, cells, cell_num);
  else if(coords_type == typeid(int64_t))
    write_cells_sorted<int64_t>(ad, cells, cell_num);
  else if(coords_type == typeid(float))
    write_cells_sorted<float>(ad, cells, cell_num);
  else if(coords_type == typeid(double))
    write_cells_sorted<double>(ad, cells, cell_num);
}

template<class T>
void StorageManager::write_cells_sorted(
    int ad, const void* cells, int64_t cell_num) const {
  // For easy reference
  size_t cell_size = arrays_[ad]->array_schema_->cell_size();
  size_t offset = 0;

  for(int64_t i=0; i<cell_num; ++i) {
    arrays_[ad]->write_cell_sorted<T>(static_cast<const char*>(cells) + offset);
    offset += cell_size;
  }
}

/******************************************************
*********************** MISC **************************
******************************************************/

/*
StorageManager::MBRs::const_iterator StorageManager::MBR_begin(
    const FragmentDescriptor* fd) const {
  // Check array descriptor
  assert(check_fragment_descriptor(fd)); 

  // For easy reference
  const FragmentInfo& fragment_info = *(fd->fragment_info_);

  // The fragment must be open in READ mode
  assert(fragment_info.fragment_mode_ == READ);

  return fragment_info.mbrs_.begin(); 
}

StorageManager::MBRs::const_iterator StorageManager::MBR_end(
    const FragmentDescriptor* fd) const {
  // Check fragment descriptor
  assert(check_fragment_descriptor(fd)); 
  
  // For easy reference
  const FragmentInfo& fragment_info = *(fd->fragment_info_);

  // The fragment must be open in READ mode
  assert(fragment_info.fragment_mode_ == READ);

  return fragment_info.mbrs_.end(); 
}

*/

/******************************************************
***************** PRIVATE FUNCTIONS *******************
******************************************************/

void StorageManager::check_on_open_array(
    const std::string& array_name, const char* mode) const {
  // Check if the array is defined
  if(!array_defined(array_name))
    throw StorageManagerException(std::string("Array ") + array_name + 
                                  " not defined.");
  // Check mode
  if(invalid_array_mode(mode))
    throw StorageManagerException(std::string("Invalid mode ") + mode + "."); 

  // Check if array is already open
  if(open_arrays_.find(array_name) != open_arrays_.end())
    throw StorageManagerException(std::string("Array ") + array_name + 
                                              " already open."); 
}

const ArraySchema* StorageManager::get_array_schema(int ad) const {
  assert(arrays_[ad] != NULL);

  return arrays_[ad]->array_schema_;
}

const ArraySchema* StorageManager::get_array_schema(
    const std::string& array_name) const {
  // The schema to be returned
  ArraySchema* array_schema = new ArraySchema();

  // Open file
  std::string filename = workspace_ + "/" + array_name + "/" + 
                         SM_ARRAY_SCHEMA_FILENAME + 
                         SM_BOOK_KEEPING_FILE_SUFFIX;
  int fd = open(filename.c_str(), O_RDONLY);
  assert(fd != -1);

  // Initialize buffer
  struct stat st;
  fstat(fd, &st);
  size_t buffer_size = st.st_size;
  char* buffer = new char[buffer_size];
 
  // Load array schema
  read(fd, buffer, buffer_size);
  array_schema->deserialize(buffer, buffer_size);

  // Clean up
  close(fd);
  delete [] buffer;

  return array_schema;
}

bool StorageManager::invalid_array_mode(const char* mode) const {
  if(strcmp(mode, "r") && strcmp(mode, "w") && strcmp(mode, "a"))
    return true;
  else 
    return false;
}

inline
void StorageManager::set_workspace(const std::string& path) {
  workspace_ = absolute_path(path);
  assert(path_exists(workspace_));

  if(mpi_handler_ == NULL) {
    workspace_ += "/StorageManager/";
  } else {
    std::stringstream ss;
    ss << workspace_<< "/StorageManager_"  
       << mpi_handler_->rank() << "/"; 
    workspace_ = ss.str();
  }
}

int StorageManager::store_array(Array* array) {
  int ad = -1;

  for(int i=0; i<SM_MAX_OPEN_ARRAYS; ++i) {
    if(arrays_[i] == NULL) {
      ad = i; 
      break;
    }
  }

  if(ad != -1) 
    arrays_[ad] = array;

  return ad;
}

/*
int64_t StorageManager::tile_pos(const FragmentInfo& fragment_info, 
                                  int64_t tile_id) const {
  // Check of the array has not tiles
  if(fragment_info.tile_ids_.size() == 0 || tile_id  < 0)
    return SM_INVALID_POS;

  // Perform binary search
  int64_t min = 0;
  int64_t max = fragment_info.tile_ids_.size()-1;
  int64_t mid;
  while(min <= max) {
    mid = min + ((max - min) / 2);
    if(fragment_info.tile_ids_[mid] == tile_id)  // Tile found!
      return mid;
    else if(fragment_info.tile_ids_[mid] > tile_id) {
      if(mid == 0)
        return SM_INVALID_POS;
      max = mid - 1;
    }
    else // (fragment_info.tile_ids_[mid] < tile_id)
      min = mid + 1;
  }

  // Tile id not found
  return SM_INVALID_POS;
}

*/


// Explicit template instantiations
template class StorageManager::Array::const_cell_iterator<int>;
template class StorageManager::Array::const_cell_iterator<int64_t>;
template class StorageManager::Array::const_cell_iterator<float>;
template class StorageManager::Array::const_cell_iterator<double>;

template StorageManager::Array::const_cell_iterator<int>
StorageManager::begin<int>(int ad) const;
template StorageManager::Array::const_cell_iterator<int64_t>
StorageManager::begin<int64_t>(int ad) const;
template StorageManager::Array::const_cell_iterator<float>
StorageManager::begin<float>(int ad) const;
template StorageManager::Array::const_cell_iterator<double>
StorageManager::begin<double>(int ad) const;

template StorageManager::Array::const_cell_iterator<int>
StorageManager::begin<int>(int ad, const int* range) const;
template StorageManager::Array::const_cell_iterator<int64_t>
StorageManager::begin<int64_t>(int ad, const int64_t* range) const;
template StorageManager::Array::const_cell_iterator<float>
StorageManager::begin<float>(int ad, const float* range) const;
template StorageManager::Array::const_cell_iterator<double>
StorageManager::begin<double>(int ad, const double* range) const;

template void StorageManager::write_cell_sorted<int>(
    int ad, const void* range) const;
template void StorageManager::write_cell_sorted<int64_t>(
    int ad, const void* range) const;
template void StorageManager::write_cell_sorted<float>(
    int ad, const void* range) const;
template void StorageManager::write_cell_sorted<double>(
    int ad, const void* range) const;

template void StorageManager::read_cells<int>(
    int ad, const int* range, void*& cells,
    int64_t& cell_num, int rcv_rank) const;
template void StorageManager::read_cells<int64_t>(
    int ad, const int64_t* range, void*& cells,
    int64_t& cell_num, int rcv_rank) const;
template void StorageManager::read_cells<float>(
    int ad, const float* range, void*& cells,
    int64_t& cell_num, int rcv_rank) const;
template void StorageManager::read_cells<double>(
    int ad, const double* range, void*& cells,
    int64_t& cell_num, int rcv_rank) const;
