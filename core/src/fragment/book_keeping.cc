/**
 * @file   book_keeping.cc
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
 * This file implements the BookKeeping class.
 */

#include "book_keeping.h"
#include "utils.h"
#include <assert.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

/******************************************************
************ CONSTRUCTORS & DESTRUCTORS ***************
******************************************************/

BookKeeping::BookKeeping(
    const ArraySchema* array_schema,
    const std::string* fragment_name,
    const std::string* workspace) 
    : array_schema_(array_schema), 
      fragment_name_(fragment_name),
      workspace_(workspace) {
}

BookKeeping::~BookKeeping() {
  // For easy reference
  int64_t tile_num = tile_ids_.size();
 
  for(int64_t i=0; i<tile_num; ++i) {
    if(bounding_coordinates_[i].first != NULL) {
      free(bounding_coordinates_[i].first);
      bounding_coordinates_[i].first = NULL;
    }

    if(bounding_coordinates_[i].second != NULL) {
      free(bounding_coordinates_[i].second);
      bounding_coordinates_[i].second = NULL;
    }

    if(mbrs_[i] != NULL) {
      free(mbrs_[i]);
      mbrs_[i] = NULL;
    }
  }

  bounding_coordinates_.clear();
  mbrs_.clear();
  offsets_.clear(); 
  tile_ids_.clear(); 
}

/******************************************************
******************** ACCESSORS ************************
******************************************************/

Tile::BoundingCoordinatesPair 
BookKeeping::bounding_coordinates(int64_t pos) const {
  assert(pos >= 0 && pos < bounding_coordinates_.size());

  return bounding_coordinates_[pos];
}

Tile::MBR BookKeeping::mbr(int64_t pos) const {
  assert(pos >= 0 && pos < mbrs_.size());

  return mbrs_[pos];
}

int64_t BookKeeping::offset(int attribute_id, int64_t pos) const {
  return offsets_[attribute_id][pos];
}

int64_t BookKeeping::tile_id(int64_t pos) const {
  assert(pos >= 0 && pos < tile_ids_.size());

  return tile_ids_[pos];
}

int64_t BookKeeping::tile_num() const {
  return tile_ids_.size();
}

size_t BookKeeping::tile_size(int attribute_id, int64_t pos) const {
  assert(tile_num() > 0);
  assert(pos >= 0 && pos < tile_ids_.size());

  if(pos == tile_num() - 1) {
    const std::string& array_name = array_schema_->array_name();
    const std::string& attribute_name = 
        array_schema_->attribute_name(attribute_id);
    std::string filename = *workspace_ + "/" + array_name + "/" +
                           *fragment_name_ + "/" +
                           attribute_name + TILE_DATA_FILE_SUFFIX;
    return file_size(filename) - offsets_[attribute_id][pos]; 
  } else {
    return offsets_[attribute_id][pos+1] - offsets_[attribute_id][pos]; 
  } 
}

/******************************************************
******************** MUTATORS *************************
******************************************************/

void BookKeeping::commit() {
  // Do nothing if the book keeping files exist
  std::string filename = *workspace_ + "/" + array_schema_->array_name() + "/" +
                         *fragment_name_ + "/" +
                         BOUNDING_COORDINATES_FILENAME + 
                         BOOK_KEEPING_FILE_SUFFIX;

  if(is_file(filename)) 
    return;

  commit_bounding_coordinates();
  commit_mbrs();
  commit_offsets();
  commit_tile_ids();

}

void BookKeeping::init() {
  int attribute_num = array_schema_->attribute_num();

  offsets_.resize(attribute_num+1);
  for(int i=0; i<=attribute_num; ++i)
    offsets_[i].push_back(0);
}

void BookKeeping::load() {
  load_tile_ids();
  load_bounding_coordinates();
  load_mbrs();
  load_offsets();
}

/******************************************************
****************** PRIVATE METHODS ********************
******************************************************/

// FILE FORMAT:
// tile#1_lower_dim#1(T) tile#1_lower_dim#2(T) ... 
// tile#1_upper_dim#1(T) tile#1_upper_dim#2(T) ... 
// tile#2_lower_dim#1(T) tile#2_lower_dim#2(T) ... 
// tile#2_upper_dim#1(T) tile#2_upper_dim#2(T) ...
// ... 
// NOTE: T is the type of the dimensions of this array
void BookKeeping::commit_bounding_coordinates() {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  int64_t tile_num = tile_ids_.size();

  if(tile_num == 0)
    return;

  size_t cell_size = array_schema_->cell_size(attribute_num);

  // Prepare filename
  std::string filename = *workspace_ + "/" + array_schema_->array_name() + "/" +
                         *fragment_name_ + "/" +
                         BOUNDING_COORDINATES_FILENAME + 
                         BOOK_KEEPING_FILE_SUFFIX;

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
      memcpy(buffer+offset, bounding_coordinates_[i].first, cell_size);
      offset += cell_size;
      // Upper bounding coordinates
      memcpy(buffer+offset, bounding_coordinates_[i].second, cell_size);
      offset += cell_size;
    }

    // Write buffer and clean up
    write(fd, buffer, buffer_size);
    delete [] buffer;
  }  

  close(fd);
}

// FILE FORMAT:
// MBR#1_dim#1_low(T) MBR#1_dim#1_high(T) ...
// MBR#1_dim#2_low(T) MBR#1_dim#2_high(T) ...
// ...
// MBR#2_dim#1_low(T) MBR#2_dim#1_high(T) ...
// ...
// NOTE: T is the type of the dimensions of this array
void BookKeeping::commit_mbrs() {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  int64_t tile_num = tile_ids_.size();

  if(tile_num == 0)
    return;

  size_t cell_size = array_schema_->cell_size(attribute_num);

  // prepare file name
  std::string filename = *workspace_ + "/" + array_schema_->array_name() + "/" + 
                         *fragment_name_ + "/" +
                         MBRS_FILENAME + BOOK_KEEPING_FILE_SUFFIX;

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
      memcpy(buffer+offset, mbrs_[i], 2 * cell_size);
      offset += 2 * cell_size;
    }

    // Write buffer and clean up
    write(fd, buffer, buffer_size);
    delete [] buffer;
  }

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
void BookKeeping::commit_offsets() {
  // For easy reference
  int64_t tile_num = tile_ids_.size();

  if(tile_num == 0)
    return;

  // Prepare file name
  std::string filename = *workspace_ + "/" + array_schema_->array_name() + "/" + 
                         *fragment_name_ + "/" +
                         OFFSETS_FILENAME + BOOK_KEEPING_FILE_SUFFIX;

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
      memcpy(buffer+offset, &offsets_[i][0], tile_num * sizeof(int64_t));
      offset += tile_num * sizeof(int64_t);
    }

    // Write buffer and clean up
    write(fd, buffer, buffer_size);
    delete [] buffer;
  }
  
  close(fd);
}

// FILE FORMAT:
// tile_num(int64_t)
//   tile_id#1(int64_t) tile_id#2(int64_t)  ...
void BookKeeping::commit_tile_ids() {
  // For easy reference
  int64_t tile_num = tile_ids_.size();

  if(tile_num == 0)
    return;

  // Prepare file name
  std::string filename = *workspace_ + "/" + array_schema_->array_name() + "/" +
                         *fragment_name_ + "/" +
                         TILE_IDS_FILENAME + BOOK_KEEPING_FILE_SUFFIX;

  // Open file
  int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  assert(fd != -1);
 
  if(tile_num != 0) { // If the array is not empty
    // Prepare the buffer
    size_t buffer_size = (tile_num+1) * sizeof(int64_t);
    char* buffer = new char[buffer_size];
 
    // Populate buffer
    memcpy(buffer, &tile_num, sizeof(int64_t));
    memcpy(buffer+sizeof(int64_t), &tile_ids_[0], tile_num * sizeof(int64_t));

    // Write buffer and clean up
    write(fd, buffer, buffer_size);  
    delete [] buffer;
  }

  close(fd);
}

// FILE FORMAT:
// tile#1_lower_dim#1(T) tile#1_lower_dim#2(T) ... 
// tile#1_upper_dim#1(T) tile#1_upper_dim#2(T) ... 
// tile#2_lower_dim#1(T) tile#2_lower_dim#2(T) ... 
// tile#2_upper_dim#1(T) tile#2_upper_dim#2(T) ...
// ... 
// NOTE: T is the type of the dimensions of this array
void BookKeeping::load_bounding_coordinates() {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  int64_t tile_num = tile_ids_.size();
  assert(tile_num != 0);
  size_t cell_size = array_schema_->cell_size(attribute_num);
 
  // Open file
  std::string filename = *workspace_ + "/" + array_schema_->array_name() + "/" +
                         *fragment_name_ + "/" +
                         BOUNDING_COORDINATES_FILENAME + 
                         BOOK_KEEPING_FILE_SUFFIX;
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
  bounding_coordinates_.resize(tile_num);
  void* coord;

  // Load bounding coordinates
  for(int64_t i=0; i<tile_num; ++i) {
    coord = malloc(cell_size);
    memcpy(coord, buffer + offset, cell_size);
    bounding_coordinates_[i].first = coord; 
    offset += cell_size;
    coord = malloc(cell_size);
    memcpy(coord, buffer + offset, cell_size);
    bounding_coordinates_[i].second = coord; 
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
void BookKeeping::load_mbrs() {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  size_t cell_size = array_schema_->cell_size(attribute_num);
  int64_t tile_num = tile_ids_.size();
  assert(tile_num != 0);
 
  // Open file
  std::string filename = *workspace_ + "/" + array_schema_->array_name() + "/" +
                         *fragment_name_ + "/" +
                         MBRS_FILENAME + BOOK_KEEPING_FILE_SUFFIX;
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
  mbrs_.resize(tile_num);
  void* mbr;

  // Load MBRs
  for(int64_t i=0; i<tile_num; ++i) {
    mbr = malloc(2 * cell_size);
    memcpy(mbr, buffer + offset, 2 * cell_size);
    mbrs_[i] = mbr;
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
void BookKeeping::load_offsets() {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();
  int64_t tile_num = tile_ids_.size();
  assert(tile_num != 0);
 
  // Open file
  std::string filename = *workspace_ + "/" + array_schema_->array_name() + "/" +
                         *fragment_name_ + "/" +
                         OFFSETS_FILENAME + BOOK_KEEPING_FILE_SUFFIX;
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
  offsets_.resize(attribute_num+1);
  for(int i=0; i<=attribute_num; ++i) {
    offsets_[i].resize(tile_num);
    memcpy(&offsets_[i][0], buffer + offset, tile_num*sizeof(int64_t));
    offset += tile_num * sizeof(int64_t);
  }

  // Clean up
  delete [] buffer;
  close(fd);
}

// FILE FORMAT:
// tile_num(int64_t)
//   tile_id#1(int64_t) tile_id#2(int64_t)  ...
void BookKeeping::load_tile_ids() {
  // Open file
  std::string filename = *workspace_ + "/" + array_schema_->array_name() + "/" +
                         *fragment_name_ + "/" +
                         TILE_IDS_FILENAME + BOOK_KEEPING_FILE_SUFFIX;
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
  tile_ids_.resize(tile_num);

  // Load tile_ids
  memcpy(&tile_ids_[0], buffer+sizeof(int64_t), tile_num*sizeof(int64_t));

  // Clean up
  delete [] buffer;
  close(fd);
}

