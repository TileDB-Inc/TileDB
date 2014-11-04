/**
 * @file   csv_file.cc
 * @author Stavros Papadopoulos <stavrosp@csail.mit.edu>
 *
 * @section LICENSE
 *
 * The MIT License
 * 
 * Copyright (c) 2014 Stavros Papadopoulos <stavrosp@csail.mit.edu>
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
 * This file implements the CSVLine and CSVFile classes.
 */

#include "csv_file.h"
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <iostream>

/* ----------------- CSVLine functions ------------- */

/******************************************************
************ CONSTRUCTORS & DESTRUCTORS ***************
******************************************************/

CSVLine::CSVLine() {
  pos_ = 0;
}

CSVLine::CSVLine(const std::string& line) {
  pos_ = 0;
  tokenize(line);
}

CSVLine::~CSVLine() {
}

/******************************************************
*********************** ACCESSORS *********************
******************************************************/

std::string CSVLine::str() const {
  if(values_.size() == 0) 
    return "";

  std::string ret = values_[0];
  for(unsigned int i=1; i<values_.size(); i++)
    ret += "," + values_[i];
  return ret;
}

/******************************************************
*********************** MUTATORS **********************
******************************************************/

void CSVLine::clear() {
  values_.clear();
  pos_ = 0;
}

/******************************************************
*********************** OPERATORS *********************
******************************************************/

template<>
void CSVLine::operator<<(const std::string& value) {
  tokenize(value);
}

template<>
void CSVLine::operator<<(const char* value) {
  tokenize(value);
}

template<>
void CSVLine::operator<<(int value) {
  char s[50]; // maximum 50 digits
  sprintf(s, "%d", value); 
  values_.push_back(s);
}

template<>
void CSVLine::operator<<(unsigned int value) {
  char s[50]; // maximum 50 digits
  sprintf(s, "%u", value); 
  values_.push_back(s);
}

template<>
void CSVLine::operator<<(int64_t value) {
  char s[50]; // maximum 50 digits
  sprintf(s, "%lld", value); 
  values_.push_back(s);
}

template<>
void CSVLine::operator<<(uint64_t value) {
  char s[50]; // maximum 50 digits
  sprintf(s, "%llu", value); 
  values_.push_back(s);
}

template<>
void CSVLine::operator<<(float value) {
  char s[50]; // maximum 50 digits
  sprintf(s, "%g", value); 
  values_.push_back(s);
}

template<>
void CSVLine::operator<<(double value) {
  char s[50]; // maximum 50 digits
  sprintf(s, "%lg", value); 
  values_.push_back(s);
}

template<>
void CSVLine::operator<<(const CSVLine csv_line) {
  values_.insert(values_.end(), csv_line.begin(), csv_line.end());
}

template<>
bool CSVLine::operator>>(std::string& value) {
  if(pos_ == values_.size()) {
    return false;
  } else { 
    value = values_[pos_++];
    return true;
  }
}

template<>
bool CSVLine::operator>>(int& value) {
  if(pos_ == values_.size()) {
    return false;
  } else { 
    sscanf(values_[pos_++].c_str(), "%d", &value);
    return true;
  }
}

template<>
bool CSVLine::operator>>(int64_t& value) {
  if(pos_ == values_.size()) {
    return false;
  } else { 
    sscanf(values_[pos_++].c_str(), "%lld", &value);
    return true;
  }
}

template<>
bool CSVLine::operator>>(uint64_t& value) {
  if(pos_ == values_.size()) {
    return false;
  } else { 
    sscanf(values_[pos_++].c_str(), "%llu", &value);
    return true;
  }
}

template<>
bool CSVLine::operator>>(float& value) {
  if(pos_ == values_.size()) {
    return false;
  } else { 
    sscanf(values_[pos_++].c_str(), "%f", &value);
    return true;
  }
}

template<>
bool CSVLine::operator>>(double& value) {
  if(pos_ == values_.size()) {
    return false;
  } else { 
    sscanf(values_[pos_++].c_str(), "%lf", &value);
    return true;
  }
}

template<>
void CSVLine::operator=(const std::string& value) {
  pos_ = 0;
  values_.clear();
  tokenize(value);
}

template<>
void CSVLine::operator=(char* value) {
  pos_ = 0;
  values_.clear();
  tokenize(value);
}

template<>
void CSVLine::operator=(const char* value) {
  pos_ = 0;
  values_.clear();
  tokenize(value);
}

template<>
void CSVLine::operator=(int value) {
  pos_ = 0;
  values_.clear();
  (*this) << value;
}

template<>
void CSVLine::operator=(unsigned int value) {
  pos_ = 0;
  values_.clear();
  (*this) << value;
}

template<>
void CSVLine::operator=(int64_t value) {
  pos_ = 0;
  values_.clear();
  (*this) << value;
}

template<>
void CSVLine::operator=(uint64_t value) {
  pos_ = 0;
  values_.clear();
  (*this) << value;
}

template<>
void CSVLine::operator=(float value) {
  pos_ = 0;
  values_.clear();
  (*this) << value;
}

template<>
void CSVLine::operator=(double value) {
  pos_ = 0;
  values_.clear();
  (*this) << value;
}

template<>
void CSVLine::operator=(const CSVLine csv_line) {
  pos_ = 0;
  values_.clear();
  (*this) << csv_line;
}

/******************************************************
******************* PRIVATE METHODS *******************
******************************************************/

void CSVLine::tokenize(std::string line) {
  char* token = strtok(&line[0], ",");
  while(token != NULL) {
    values_.push_back(token);
    token = strtok(NULL, ",");
  }
}

/* ----------------- CSVFile functions ------------- */

/******************************************************
********************* CONSTRUCTORS ********************
******************************************************/

CSVFile::CSVFile(const std::string& filename, Mode mode, 
                 uint64_t segment_size) : filename_(filename),
                                          mode_(mode), 
                                          segment_size_(segment_size) {
  // Replace '~' with the absolute path
  if(filename_[0] == '~') 
    filename_ = std::string(getenv("HOME")) +
                 filename_.substr(1, filename_.size()-1);

  // If the mode is WRITE, delete the previous file in order to overwrite it.
  // After initialization and for as long as the CSVFile object is alive,
  // it will be in APPEND mode.
  if(mode_ == WRITE) {
    remove(filename_.c_str());
    mode_ = APPEND;
  }

  buffer_ = NULL;
  buffer_end_ = 0;
  buffer_offset_ = 0;
  file_offset_ = 0;
}

CSVFile::~CSVFile() {
  if(buffer_ != NULL) {
    // If there are data in the buffer pending to be written in the file,
    // flush the buffer.
    if(mode_ == APPEND && buffer_offset_ != 0)
      flush_buffer();
    delete buffer_;
  }
}

/******************************************************
********************** OPERATORS **********************
******************************************************/

void CSVFile::operator<<(const CSVLine& csv_line) {
  if(mode_ == READ)
    throw CSVFileException("Cannot append line: the CSV file is in READ mode.", 
                           filename_);

  const std::string& line = csv_line.str(); 

  // Initialize the buffer.
  if(buffer_ == NULL) 
    buffer_ = new char[segment_size_ + 1]; // +1 for the extra '\n' character
  
  // Throw an exception if the line is bigger than CSVFile::segment_size_. 
  if(line.size() > segment_size_)
    throw CSVFileException("Cannot append line: line cannot fit in the buffer.", 
                           filename_);
 
  // Flush the buffer to the file if its stored data size plus the size of the
  // new line exceed CSVFile::segment_size._
  if(buffer_offset_ + line.size() > segment_size_) {	
    flush_buffer();
    buffer_offset_ = 0;
  }

  // Write the line in the buffer
  memcpy(buffer_ + buffer_offset_, line.c_str(), line.size());
  buffer_offset_ += line.size();

  // Write the '\n' character in the end
  char c = '\n';
  memcpy(buffer_ + buffer_offset_, &c, sizeof(c));
  buffer_offset_ += sizeof(c);
}

bool CSVFile::operator>>(CSVLine& csv_line) {
  if(mode_ == APPEND)
    throw CSVFileException("Cannot get line: the CSV file is in APPEND mode.", 
                           filename_);

  // If the buffer is NULL or the buffer offset exceeds the useful
  // data in the buffer, we must read a new segment. If a new segment
  // cannot be read, then there are no more lines. 
  if((buffer_ == NULL || buffer_offset_ >= buffer_end_) 
     && !read_segment())  
    return false;

  // Read next line
  char* line = strtok(buffer_+buffer_offset_, "\n");
  if(line == NULL) {
    // Read the next segment of lines
    if(!read_segment()) // We reached the end of the file
      return false;
    else // New lines were loaded
      line = strtok(buffer_+buffer_offset_, "\n");
  } 

  // Return line
  csv_line = line;
  buffer_offset_ += strlen(line) + 1; // +1 to skip '\n'
 
  return true;
}

/******************************************************
******************** PRIVATE METHODS ******************
******************************************************/

void CSVFile::flush_buffer() {
  // Open file
  int fd = open(filename_.c_str(), O_WRONLY | O_APPEND | O_CREAT | O_SYNC, 
                S_IRWXU);
  if(fd == -1)
    throw CSVFileException("Cannot open CSV file.", filename_);

  write(fd, buffer_, buffer_offset_);
  close(fd);	
}

bool CSVFile::read_segment() {
  int fd = open(filename_.c_str(), O_RDONLY);
  if(fd == -1)
    throw CSVFileException("Cannot open CSV file.", filename_);
  struct stat st;
  fstat(fd, &st);

  // Handle end of the file
  if(file_offset_ == st.st_size) {
    close(fd);
    return false;
  }

  // Handle empty files 
  if(st.st_size == 0) {
    close(fd);
    return false;
  }

  uint64_t bytes_to_be_read = (st.st_size >= segment_size_) 
                              ? segment_size_ : st.st_size;

  // This is the first time a segment is read.
  // We reserve one extra byte for the potential '\n' that must be 
  // injected in the end of the last line to be read from the file.
  if(buffer_ == NULL) { 
    buffer_ = new char[bytes_to_be_read + 1];
  }
 
  // Read the new lines
  lseek(fd, file_offset_, SEEK_SET);
  read(fd, buffer_, bytes_to_be_read);

  // Update the offsets
  buffer_offset_ = 0;
  buffer_end_ = bytes_to_be_read;
  
  // We reached the end of the file
  // Inject '\n' to the end of the last line read from the file
  if(file_offset_ == st.st_size) { 
     if(buffer_[bytes_to_be_read-1] != '\n')
       buffer_[bytes_to_be_read] = '\n';
  } else {
  // We may need to backtrack until we find the last '\n' character,
  // so that we do not split lines in the middle at the boundaries of two
  // segments
    while(buffer_[buffer_end_-1] != '\n') {
      if(--buffer_end_ == 0)
        throw CSVFileException("Cannot read segment: line cannot fit in the "
                             "buffer.", filename_);
    }
  }
  
  // Update file offset
  file_offset_ += buffer_end_;

  close(fd);
  return true;
}
