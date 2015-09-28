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

#include "bin_file.h"
#include "book_keeping.h"
#include "utils.h"
#include <assert.h>
#include <fcntl.h>
#include <iostream>
#include <sys/stat.h>
#include <unistd.h>
#include <zlib.h>

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
  int64_t tile_num = mbrs_.size();
 
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
  assert(pos >= 0 && pos < mbrs_.size());

  if(array_schema_->has_irregular_tiles())
    return pos;
  else 
    return array_schema_->tile_id(bounding_coordinates_[pos].first);
}

int64_t BookKeeping::tile_num() const {
  return mbrs_.size();
}

size_t BookKeeping::tile_size(int attribute_id, int64_t pos) const {
  assert(tile_num() > 0);
  assert(pos >= 0 && pos < tile_num());

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

  commit_offsets();
  commit_bounding_coordinates();
  commit_mbrs();
}

void BookKeeping::init() {
  int attribute_num = array_schema_->attribute_num();

  offsets_.resize(attribute_num+1);
  for(int i=0; i<=attribute_num; ++i)
    offsets_[i].push_back(0);
}

void BookKeeping::load() {
  load_mbrs();
  load_bounding_coordinates();
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
  int64_t tile_num = mbrs_.size();

  if(tile_num == 0)
    return;

  size_t coords_size = array_schema_->cell_size(attribute_num);

  // Prepare filename
  std::string filename = *workspace_ + "/" + array_schema_->array_name() + "/" +
                         *fragment_name_ + "/" +
                         BOUNDING_COORDINATES_FILENAME + 
                         BOOK_KEEPING_FILE_SUFFIX;

  // Open file
  BINFile* bin_file = new BINFile(filename, "w"); 

  // Write to file
  for(int64_t i=0; i<tile_num; ++i) {
    // Lower bounding coordinates
    bin_file->write(bounding_coordinates_[i].first, coords_size);
    // Upper bounding coordinates
    bin_file->write(bounding_coordinates_[i].second, coords_size);
  }

  delete bin_file;
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
  int64_t tile_num = mbrs_.size();
  int attribute_num = array_schema_->attribute_num();

  if(tile_num == 0)
    return;

  size_t coords_size = array_schema_->cell_size(attribute_num);

  // prepare file name
  std::string filename = *workspace_ + "/" + array_schema_->array_name() + "/" +
                         *fragment_name_ + "/" +
                         MBRS_FILENAME + BOOK_KEEPING_FILE_SUFFIX;

  // Open file
  BINFile* bin_file = new BINFile(filename, "w"); 

  // Write to file
  for(int64_t i=0; i<tile_num; ++i) 
    bin_file->write(mbrs_[i], 2 * coords_size);

  delete bin_file;
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
  int64_t tile_num = mbrs_.size();
  int attribute_num = array_schema_->attribute_num();

  if(tile_num == 0)
    return;

  // Prepare file name
  std::string filename = *workspace_ + "/" + array_schema_->array_name() + "/" +
                         *fragment_name_ + "/" +
                         OFFSETS_FILENAME + BOOK_KEEPING_FILE_SUFFIX;

  // Open file
  BINFile* bin_file = new BINFile(filename, "w"); 

  // Write to the file
  // TODO: This could be optimized a bit
  for(int i=0; i<=attribute_num; ++i) {
    for(int j=0; j<tile_num; ++j) 
      bin_file->write(&offsets_[i][j], sizeof(int64_t));
  }
  
  delete bin_file;
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
  int64_t tile_num = mbrs_.size();
  assert(tile_num != 0);
  size_t coords_size = array_schema_->cell_size(attribute_num);
 
  // Prepare file name
  std::string filename = *workspace_ + "/" + array_schema_->array_name() + "/" +
                         *fragment_name_ + "/" +
                         BOUNDING_COORDINATES_FILENAME + 
                         BOOK_KEEPING_FILE_SUFFIX;

  // Open file
  BINFile* bin_file = new BINFile(filename, "r"); 

  // Initializations
  bounding_coordinates_.resize(tile_num);
  void* coords;
  ssize_t bytes_read;

  // Load bounding coordinates
  for(int64_t i=0; i<tile_num; ++i) {
    coords = malloc(coords_size);
    bytes_read = bin_file->read(coords, coords_size);
    assert(bytes_read == coords_size);
    bounding_coordinates_[i].first = coords; 
    coords = malloc(coords_size);
    bytes_read = bin_file->read(coords, coords_size);
    assert(bytes_read == coords_size);
    bounding_coordinates_[i].second = coords; 
  }

  // Correctness check
  void* dummy = malloc(1); 
  bytes_read = bin_file->read(dummy, 1);
  assert(bytes_read == 0);
  free(dummy);

  // Clean up
  delete bin_file;
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
  size_t coords_size = array_schema_->cell_size(attribute_num);
 
  // Prepare file name
  std::string filename = *workspace_ + "/" + array_schema_->array_name() + "/" +
                         *fragment_name_ + "/" +
                         MBRS_FILENAME + BOOK_KEEPING_FILE_SUFFIX;

  // Open file
  BINFile* bin_file = new BINFile(filename, "r"); 

  // Load MBRs
  void* mbr;
  ssize_t bytes_read;
  do {
    mbr = malloc(2 * coords_size);
    bytes_read = bin_file->read(mbr, 2 * coords_size);

    if(bytes_read == 0) { 
      free(mbr);
    } else {
      assert(bytes_read == 2 * coords_size);
      mbrs_.push_back(mbr);
    }
  } while (bytes_read != 0);


  // Clean up
  delete bin_file;
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
  int64_t tile_num = mbrs_.size();
  assert(tile_num != 0);
 
  // Prepare file name
  std::string filename = *workspace_ + "/" + array_schema_->array_name() + "/" +
                         *fragment_name_ + "/" +
                         OFFSETS_FILENAME + BOOK_KEEPING_FILE_SUFFIX;

  // Open file
  BINFile* bin_file = new BINFile(filename, "r"); 

  // Load offsets
  offsets_.resize(attribute_num+1);
  ssize_t bytes_read;
  for(int i=0; i<=attribute_num; ++i) {
    offsets_[i].resize(tile_num);
    for(int j=0; j<tile_num; ++j) {
      bytes_read = bin_file->read(&offsets_[i][j], sizeof(int64_t));
      assert(bytes_read == sizeof(int64_t));
    }
  }

  // Correctness check
  void* dummy = malloc(1); 
  bytes_read = bin_file->read(dummy, 1);
  assert(bytes_read == 0);
  free(dummy);

  // Clean up
  delete bin_file;
}

