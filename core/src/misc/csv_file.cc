/**
 * @file   csv_file.cc
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
 * This file implements the CSVFile class.
 */

#include "csv_file.h"
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

CSVFile::CSVFile() { 
  array_schema_ = NULL;
  buffer_ = NULL;
  buffer_end_ = 0;
  buffer_offset_ = 0;
  cell_ = NULL;
  file_offset_ = 0;
}

CSVFile::CSVFile(const ArraySchema* array_schema) 
    : array_schema_(array_schema) { 
  buffer_ = NULL;
  buffer_end_ = 0;
  buffer_offset_ = 0;
  if(array_schema_->cell_size() != VAR_SIZE) {
    cell_ = malloc(array_schema_->cell_size());
    allocated_cell_size_ = array_schema_->cell_size();
  } else {
    cell_ = malloc(CSV_INITIAL_VAR_CELL_SIZE);
    allocated_cell_size_ = CSV_INITIAL_VAR_CELL_SIZE;
  }
  file_offset_ = 0;
}

CSVFile::CSVFile(const std::string& filename, const char* mode) { 
  array_schema_ = NULL;
  buffer_ = NULL;
  buffer_end_ = 0;
  buffer_offset_ = 0;
  cell_ = NULL;
  file_offset_ = 0;

  open(filename, mode);
}

CSVFile::~CSVFile() {
  close();
}

/******************************************************
********************** ACCESSORS **********************
******************************************************/

ssize_t CSVFile::bytes_read() const {
  if(file_offset_ == 0)
    return 0;
  else 
    return file_offset_ - segment_size_ + buffer_offset_;
}

ssize_t CSVFile::size() const {
  int fd = ::open(filename_.c_str(), O_RDONLY);
  struct stat st;
  fstat(fd, &st);
  ::close(fd);

  return st.st_size;
}

/******************************************************
*********************** MUTATORS **********************
******************************************************/

void CSVFile::close() {
  if(buffer_ != NULL) {
    // If there are data in the buffer pending to be written in the file,
    // flush the buffer.
    if(((strcmp(mode_, "a") == 0) || (strcmp(mode_, "w") == 0)) && 
       buffer_offset_ != 0)
      flush_buffer();
    delete buffer_;
    buffer_ = NULL;
  }

  if(cell_ != NULL) {
    free(cell_);
    cell_ = NULL;
  }
}

bool CSVFile::open(const std::string& filename, 
                   const char* mode,
                   size_t segment_size) {
// TODO: return error codes
  filename_ = absolute_path(filename);

  if(strcmp(mode, "r") == 0 && !is_file(filename_)) 
    return false;

  segment_size_ = segment_size;
  strcpy(mode_, mode);

  // If mode is "w", delete the previous file in order to overwrite it.
  // After initialization and for as long as the CSVFile object is alive,
  // it will be in "a" mode.
  if(strcmp(mode_, "w") == 0) {
    remove(filename_.c_str());
    strcpy(mode_, "a");
  }

  buffer_ = NULL;
  buffer_end_ = 0;
  buffer_offset_ = 0;
  file_offset_ = 0;

  return true;
}

/******************************************************
********************** OPERATORS **********************
******************************************************/

void CSVFile::operator<<(const CSVLine& csv_line) {
  assert(strcmp(mode_, "w") == 0 || strcmp(mode_, "a") == 0);

  size_t line_size = csv_line.strlen();
  assert(line_size < segment_size_);

  // Initialize the buffer.
  if(buffer_ == NULL) 
    buffer_ = new char[segment_size_];
  
  // Flush the buffer to the file if its stored data size plus the size of the
  // new line exceed CSVFile::segment_size_. The +1 is for the '\n' appended
  // after the line.
  if(buffer_offset_ + line_size + 1 > segment_size_) {	
    flush_buffer();
    buffer_offset_ = 0;
  }

  // Write the line in the buffer
  memcpy(buffer_ + buffer_offset_, csv_line.c_str(), line_size);
  buffer_offset_ += line_size;

  // Write the '\n' character in the end
  char c = '\n';
  memcpy(buffer_ + buffer_offset_, &c, sizeof(c));
  buffer_offset_ += sizeof(c);
}

bool CSVFile::operator>>(CSVLine& csv_line) {
  assert(strcmp(mode_, "r") == 0);

  // NOTE: buffer_end_ always points to a '\0' character.

  // If the buffer is NULL or the buffer offset exceeds the useful
  // data in the buffer, we must read a new segment. If a new segment
  // cannot be read, then there are no more lines. 
  if((buffer_ == NULL || buffer_offset_ == buffer_end_) 
     && !read_segment())  
    return false;

  // Read next line
  char* line;
  do { // Loop when a CSV line starts with '#' (i.e., ignore comment lines)
    line = strtok(buffer_+buffer_offset_, "\n");
    if(line == NULL) {
      // Read the next segment of lines
      if(!read_segment()) // We reached the end of the file
        return false;
      else // New lines were loaded
        line = strtok(buffer_+buffer_offset_, "\n");
    } 

    buffer_offset_ += strlen(line) + 1; // +1 for the '\n' character

    // If the last line of the file does not contain the '\n' character
    // (all other lines do), buffer_offset_ > buffer_end_ and we missed 
    // the '\0' character
    if(buffer_offset_ > buffer_end_)
      // Now buffer_offset_ points to '\0'
      buffer_offset_ = buffer_end_;
  } while(line[0] == '#');

  // Return line
  csv_line = line;

  return true;
}

bool CSVFile::operator>>(Cell& cell) {
  assert(strcmp(mode_, "r") == 0);
  assert(array_schema_ != NULL);

  CSVLine csv_line;
  if(!(*this >> csv_line)) {
    cell.set_cell(NULL);
    return false;
  }

  array_schema_->csv_line_to_cell(csv_line, cell_, allocated_cell_size_);
  cell.set_cell(cell_);

  return true;
}

/******************************************************
******************** PRIVATE METHODS ******************
******************************************************/

void CSVFile::flush_buffer() {
  // Open file
  int fd = ::open(filename_.c_str(), O_WRONLY | O_APPEND | O_CREAT | O_SYNC, 
                  S_IRWXU);
  assert(fd != -1);

  write(fd, buffer_, buffer_offset_);
  ::close(fd);	
}

bool CSVFile::read_segment() {
  int fd = ::open(filename_.c_str(), O_RDONLY);

  assert(fd != -1);
  struct stat st;
  fstat(fd, &st);

  // Handle end of the file
  if(file_offset_ >= st.st_size) {
    ::close(fd);
    return false;
  }

  size_t bytes_to_be_read = (st.st_size-file_offset_ >= segment_size_) 
                              ? segment_size_ : st.st_size-file_offset_;

  // Initialize the buffer
  if(buffer_ != NULL)
    delete buffer_;
  buffer_ = new char[bytes_to_be_read + 1];

  // Read the new lines
  lseek(fd, file_offset_, SEEK_SET);
  read(fd, buffer_, bytes_to_be_read);

  buffer_offset_ = 0;
  buffer_end_ = bytes_to_be_read;
   
  // We may need to backtrack until we find the last '\n' character,
  // so that we do not split lines in the middle at the boundaries of two
  // segments, unless we reached the end of the file.
  if(file_offset_ + bytes_to_be_read != st.st_size) { 
    while(buffer_[buffer_end_-1] != '\n') {
      --buffer_end_;
      assert(buffer_end_ != 0); // Make sure line fits in the buffer.
    }
  }

  file_offset_ += buffer_end_;
  buffer_[buffer_end_] = '\0';

  ::close(fd);
  return true;
}
