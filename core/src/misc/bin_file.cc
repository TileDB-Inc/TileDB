/**
 * @file   bin_file.cc
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
 * This file implements the BINFile class.
 */

#include "bin_file.h"
#include "utils.h"
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <iostream>

/******************************************************
************* CONSTRUCTORS & DESTRUCTORS **************
******************************************************/

BINFile::BINFile() { 
  init();
}

BINFile::BINFile(const ArraySchema* array_schema, int id_num) 
    : id_num_(id_num) { 
  init();
  array_schema_ = array_schema;
  cell_size_ = array_schema_->cell_size();
  var_size_ = (cell_size_ == VAR_SIZE);

  if(id_num_ != 0) 
    ids_ = malloc(id_num_ * sizeof(int64_t));

  if(!var_size_) {
      cell_ = malloc(cell_size_ + id_num_ * sizeof(int64_t));
      allocated_cell_size_ = cell_size_ + id_num_ * sizeof(int64_t);
  } else {
    coords_size_ = array_schema_->coords_size();
    coords_ = malloc(coords_size_);
    cell_ = malloc(BIN_INITIAL_VAR_CELL_SIZE);
    allocated_cell_size_ = BIN_INITIAL_VAR_CELL_SIZE;
  } 
}

BINFile::BINFile(const std::string& filename, const char* mode) { 
  init();
  open(filename, mode);
}

BINFile::~BINFile() {
  close();

  if(ids_ != NULL)
    free(ids_);

  if(coords_ != NULL) 
    free(coords_);

  if(cell_ != NULL) 
    free(cell_);
}

/******************************************************
*********************** MUTATORS **********************
******************************************************/

int BINFile::close() {
  if(buffer_ != NULL) {
    // If there are data in the buffer pending to be written in the file,
    // flush the buffer.
    if(((strcmp(mode_, "a") == 0) || (strcmp(mode_, "w") == 0)) && 
       buffer_offset_ != 0) {
      ssize_t bytes_flushed = flush_buffer();
      if(bytes_flushed == -1) //TODO: delete buffer_
        return -1;
    }
    delete buffer_;
    buffer_ = NULL;
  }

  return 0;
}

int BINFile::open(const std::string& filename, 
                  const char* mode,
                  size_t segment_size) {
  filename_ = absolute_path(filename);

  if(strcmp(mode_, "r") == 0 && !is_file(filename_))
    return -1;

  segment_size_ = segment_size;
  strcpy(mode_, mode);

  // If mode is "w", delete the previous file in order to overwrite it.
  // After initialization and for as long as the BINFile object is alive,
  // it will be in "a" mode.
  if(strcmp(mode_, "w") == 0) {
    remove(filename_.c_str());
    strcpy(mode_, "a");
  }

  buffer_ = NULL;
  buffer_end_ = 0;
  buffer_offset_ = 0;
  file_offset_ = 0;

  return 0;
}

/******************************************************
********************* READ/WRITE **********************
******************************************************/

ssize_t BINFile::read(void* destination, size_t size) {
  assert(strcmp(mode_, "r") == 0);
 
  // If this is the first read
  if(buffer_ == NULL) { 
     ssize_t bytes_read = read_segment();  
     if(bytes_read == -1)
       return -1;
  }

  // Determine what should be read from the buffer and 
  // what from the file (in case a new segment must be
  // retrieved.
  size_t destination_offset = 0;
  size_t remaining_bytes_in_buffer = buffer_end_ - buffer_offset_;
  size_t bytes_to_be_read_from_buffer;
  size_t bytes_to_be_read_from_file;
  if(remaining_bytes_in_buffer < size) {
    bytes_to_be_read_from_buffer = remaining_bytes_in_buffer;
    bytes_to_be_read_from_file = size - remaining_bytes_in_buffer;
  } else {
    bytes_to_be_read_from_buffer = size;
    bytes_to_be_read_from_file = 0;
  }

  // Read from buffer
  memcpy(destination, buffer_ + buffer_offset_,  
         bytes_to_be_read_from_buffer);
  buffer_offset_ += bytes_to_be_read_from_buffer;
  destination_offset += bytes_to_be_read_from_buffer;

  // (Potentially) read from file
  if(bytes_to_be_read_from_file > 0) {
     ssize_t bytes_read = read_segment();  
     if(bytes_read == 0)
       return 0;
     else if(bytes_read == -1)
       return -1;
     memcpy(static_cast<char*>(destination) + destination_offset,
            buffer_ + buffer_offset_, bytes_to_be_read_from_file);
    buffer_offset_ += bytes_to_be_read_from_file;
  }
  
  return size;
}

ssize_t BINFile::write(const void* source, size_t size) {
  assert(strcmp(mode_, "w") == 0 || strcmp(mode_, "a") == 0);
  assert(size <= segment_size_);

  // Initialize the buffer
  if(buffer_ == NULL) 
    buffer_ = new char[segment_size_];
  
  // Flush the buffer to the file if its stored data size plus the size of the
  // new data exceed BINFile::segment_size_. 
  if(buffer_offset_ + size > segment_size_) {	
    ssize_t bytes_flushed = flush_buffer();
    if(bytes_flushed == -1)
      return -1;
    buffer_offset_ = 0;
  }

  // Write the input data in the buffer
  memcpy(buffer_ + buffer_offset_, source, size);
  buffer_offset_ += size;

  return size;
}

/******************************************************
********************** OPERATORS **********************
******************************************************/

bool BINFile::operator>>(Cell& cell) {

  // TODO: Handle errors

  assert(strcmp(mode_, "r") == 0);
  assert(array_schema_ != NULL);

  ssize_t bytes_read;

  // If this is the first read
  if(buffer_ == NULL) { 
     ssize_t bytes_read = read_segment();  
     assert(bytes_read != -1);
  }
 
  if(!var_size_) { // Fixed-sized cells
    bytes_read = read(cell_, cell_size_ + id_num_ * sizeof(int64_t) );

    if(bytes_read == 0) {
      cell.set_cell(NULL);
      return false;
    }
    assert(bytes_read != -1);
  } else {        // Variable-sized cells
    // Read ids
    if(ids_ != NULL) {
      bytes_read = read(ids_, id_num_ * sizeof(int64_t));
      if(bytes_read == 0) {
        cell.set_cell(NULL);
        return false;
      }
    }

    // Read coordinates
    bytes_read = read(coords_, coords_size_);
    assert(bytes_read != -1);
    if(bytes_read == 0) {
      cell.set_cell(NULL);
      return false;
    }

    // Read cell size
    bytes_read = read(&cell_size_, sizeof(size_t));
    assert(bytes_read > 0);

    // Prepare a cell
    if(allocated_cell_size_ < cell_size_ + id_num_ * sizeof(int64_t)) {
      expand_buffer(cell_, allocated_cell_size_);
      allocated_cell_size_ *= 2;
    }

    // Copy ids, coordinates and cell size to cell
    memcpy(cell_, ids_, id_num_ * sizeof(int64_t));
    size_t cell_offset = id_num_ * sizeof(int64_t);
    memcpy(static_cast<char*>(cell_) + cell_offset, coords_, coords_size_);
    cell_offset += coords_size_;
    memcpy(static_cast<char*>(cell_) + cell_offset, 
           &cell_size_, sizeof(size_t));
    cell_offset += sizeof(size_t);

    // Read rest of attribute values into the appropriate offset in cell
    bytes_read = read(static_cast<char*>(cell_) + cell_offset, 
                      cell_size_ - coords_size_ - sizeof(size_t) - 
                      id_num_ * sizeof(int64_t));
  }

  if(bytes_read == 0) {
    cell.set_cell(NULL);
    return false;
  }

  cell.set_cell(cell_);
  return true;
}

bool BINFile::operator<<(const Cell& cell) {
  write(cell.cell(), cell.cell_size());
}

/******************************************************
******************** PRIVATE METHODS ******************
******************************************************/

ssize_t BINFile::flush_buffer() {
  // Open file
  int fd = ::open(filename_.c_str(), O_WRONLY | O_APPEND | O_CREAT | O_SYNC, 
                  S_IRWXU);
  assert(fd != -1);

  ssize_t bytes_flushed = ::write(fd, buffer_, buffer_offset_);
  ::close(fd);	

  return bytes_flushed;
}

void BINFile::init() {
  array_schema_ = NULL;
  cell_ = NULL;
  coords_ = NULL;
  buffer_ = NULL;
  buffer_end_ = 0;
  buffer_offset_ = 0;
  file_offset_ = 0;
  ids_ = NULL;
}

ssize_t BINFile::read_segment() {
  int fd = ::open(filename_.c_str(), O_RDONLY);
  assert(fd != -1);
  struct stat st;
  fstat(fd, &st);

  // Handle end of the file
  if(file_offset_ >= st.st_size) {
    ::close(fd);
    return 0;
  }

  // Initialize the buffer
  if(buffer_ == NULL)
    buffer_ = new char[segment_size_];

  // Read the new lines
  lseek(fd, file_offset_, SEEK_SET);
  ssize_t bytes_read = ::read(fd, buffer_, segment_size_);

  if(bytes_read == -1) {
    ::close(fd);
    return -1;
  }

  buffer_offset_ = 0;
  buffer_end_ = bytes_read;
  file_offset_ += buffer_end_;

  ::close(fd);
  return true;
}
