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
#include "special_values.h"
#include "utils.h"
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <iostream>

// Macro for printing error message in VERBOSE mode
#ifdef VERBOSE
  #define PRINT_ERROR(x) std::cerr << "[TileDB::CSVFile] Error: " << x \
                                   << ".\n" 
#else
  #define PRINT_ERROR(x) do { } while(0) 
#endif


/******************************************************
************* CONSTRUCTORS & DESTRUCTORS **************
******************************************************/

CSVFile::CSVFile(
    CompressionType compression,
    char delimiter,
    bool metadata)
    : compression_(compression),
      delimiter_(delimiter), 
      metadata_(metadata) { 
  array_schema_ = NULL;
  buffer_ = NULL;
  buffer_end_ = 0;
  buffer_offset_ = 0;
  cell_ = NULL;
  eof_ = false;
  fd_ = -1;
  fdz_ = NULL;
  mode_[0] = '\0';
}

CSVFile::CSVFile(
    const ArraySchema* array_schema,
    CompressionType compression,
    char delimiter,
    bool dense,
    bool metadata)
    : array_schema_(array_schema), 
      compression_(compression),
      delimiter_(delimiter),
      dense_(dense),
      metadata_(metadata) { 
  buffer_ = NULL;
  buffer_end_ = 0;
  buffer_offset_ = 0;
  buffer_utilization_ = 0;
  if(array_schema_->cell_size() != VAR_SIZE) {
    cell_ = malloc(array_schema_->cell_size());
    allocated_cell_size_ = array_schema_->cell_size();
  } else {
    cell_ = malloc(CSV_INITIAL_VAR_CELL_SIZE);
    allocated_cell_size_ = CSV_INITIAL_VAR_CELL_SIZE;
  }
  eof_ = false;
  fd_ = -1;
  fdz_ = NULL;
  mode_[0] = '\0';
}

CSVFile::CSVFile(
    const std::string& filename, 
    const char* mode,
    CompressionType compression,
    char delimiter,
    bool metadata) 
    : compression_(compression),
      delimiter_(delimiter),
      metadata_(metadata) { 
  array_schema_ = NULL;
  buffer_ = NULL;
  buffer_end_ = 0;
  buffer_offset_ = 0;
  buffer_utilization_ = 0;
  cell_ = NULL;
  eof_ = false;
  fd_ = -1;
  fdz_ = NULL;
  mode_[0] = '\0';

  open(filename, mode);
}

CSVFile::~CSVFile() {
  close();
}

/******************************************************
********************** ACCESSORS **********************
******************************************************/

ssize_t CSVFile::size() const {
  assert(strcmp(mode_, "r") == 0);

  return file_size_;
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

  if(fd_ != -1) {
    ::close(fd_);
    fd_ = -1;
  }	

  if(fdz_ != NULL) { 
    gzclose(fdz_); 
    fdz_ = NULL;
  }
}

int CSVFile::open(const std::string& filename, 
                   const char* mode,
                   size_t segment_size) {
  filename_ = real_path(filename);
  if(filename_ == "") {
    PRINT_ERROR("Invalid filename");
    return -1;
  }

  if(!strcmp(mode, "r") && !is_file(filename_)) {
    PRINT_ERROR(std::string("File '") + filename_ + "' does not exist");
    return -1;
  }

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

  // Calculate file size
  int fd = ::open(filename_.c_str(), O_RDONLY);
  struct stat st;
  fstat(fd, &st);
  file_size_ = st.st_size;
  ::close(fd);

  if(filename != "/dev/stdin" && file_size_ == 0)
    eof_ = true;

  // Open the file, depending on the compression
  open_file(filename);

  return 0;
}

/******************************************************
********************** OPERATORS **********************
******************************************************/

int CSVFile::operator<<(const CSVLine& csv_line) {

  // TODO: -1 for error

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

  return 0;
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

// TODO: change this to int and return proper codes...
bool CSVFile::operator>>(Cell& cell) {
  assert(strcmp(mode_, "r") == 0);
  assert(array_schema_ != NULL);

  // TODO: do not create a new CSV line all the time
  CSVLine csv_line(delimiter_);
  if(!(*this >> csv_line)) {
    cell.set_cell(NULL);
    return false;
  }

  int rc;
  while((rc = array_schema_->csv_line_to_cell(
                  csv_line, cell_, 
                  allocated_cell_size_, 
                  metadata_, dense_)) == TILEDB_AS_ERR_BUFFER_OVERFLOW) {
    expand_buffer(cell_, allocated_cell_size_);
    allocated_cell_size_ *= 2;
  }

  if(rc == TILEDB_AS_OK) {
    cell.set_cell(cell_);
    return true;
  } else {
    // TODO: return another error code here
    cell.set_cell(NULL);
    return false;
  }
}

/******************************************************
******************** PRIVATE METHODS ******************
******************************************************/

void CSVFile::flush_buffer() {
  assert(fd_ != -1 || fdz_ != NULL);

  ssize_t bytes_written;
  if(compression_ == CMP_NONE) 
    bytes_written = write(fd_, buffer_, buffer_offset_);
  else if(compression_ == CMP_GZIP) 
    bytes_written = gzwrite(fdz_, buffer_, buffer_offset_);

  // TODO: Error messages here
}

void CSVFile::open_file(const std::string& filename) {
  // No compression
  if(compression_ == CMP_NONE) {
    if(strcmp(mode_, "r") == 0) 
      fd_ = ::open(filename_.c_str(), O_RDONLY);
    else if(strcmp(mode_, "a") == 0) 
      fd_ = ::open(filename_.c_str(), O_WRONLY | O_APPEND | O_CREAT | O_SYNC, 
                   S_IRWXU);
  } else if(compression_ == CMP_GZIP) {
    if(strcmp(mode_, "r") == 0) 
      fdz_ = gzopen(filename_.c_str(), "rb");
    else if(strcmp(mode_, "a") == 0) 
      fdz_ = gzopen(filename_.c_str(), "wb");
  }
}

bool CSVFile::read_segment() {
  assert(fd_ != -1 || fdz_ != NULL);

  // Handle end of the file
  if(eof_) 
    return false;

  // First read
  if(buffer_ == NULL) { 
    buffer_ = new char[segment_size_ + 1];
    buffer_[segment_size_] = '\0';
  }

  // Bytes to copy from buffer, not processed yet
  size_t bytes_to_copy = buffer_utilization_ - buffer_end_; 
  // New bytes to read from the file
  size_t bytes_to_be_read = segment_size_ - bytes_to_copy;

  // Copy useful bytes
  if(bytes_to_copy != 0) {
    // Easy copy in-place
    if(buffer_end_ < segment_size_ / 2) {
      memcpy(buffer_, buffer_+buffer_end_, bytes_to_copy);
    } else { // We need a new buffer (not in-place copy)
      char* temp = buffer_;
      buffer_ = new char[segment_size_ + 1];
      buffer_[segment_size_] = '\0';
      memcpy(buffer_, temp + buffer_end_, bytes_to_copy);
      delete [] temp;
    }
  }

  // Read the new lines
  size_t bytes_read;

  if(compression_ == CMP_NONE) 
    bytes_read = read(fd_, buffer_+bytes_to_copy, bytes_to_be_read);
  else if(compression_ == CMP_GZIP)
    bytes_read = gzread(fdz_, buffer_+bytes_to_copy, bytes_to_be_read);

  if(bytes_read == 0) {
    // TODO: Better error messages
    eof_ = true;
    return false;
  }

  buffer_offset_ = 0;
  buffer_utilization_ = bytes_to_copy + bytes_read;
  buffer_end_ = buffer_utilization_;

  // We may need to backtrack until we find the last '\n' character,
  // so that we do not split lines in the middle at the boundaries of two
  // segments, unless we reached the end of the file.
  while(buffer_end_ != 0 && buffer_[buffer_end_-1] != '\n') 
    --buffer_end_;

  return true;
}
