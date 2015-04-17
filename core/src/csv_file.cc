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
 * This file implements the CSVLine and CSVFile classes.
 */

#include "csv_file.h"
#include "utils.h"
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <iostream>

/* ----------------- CSVLine functions ------------- */

/******************************************************
************ CONSTRUCTORS & DESTRUCTORS ***************
******************************************************/

CSVLine::CSVLine(const std::string& line) {
  pos_ = 0;
  tokenize(line);
}

/******************************************************
*********************** ACCESSORS *********************
******************************************************/

std::string CSVLine::str() const {
  if(values_.size() == 0) 
    return "";

  std::string ret = values_[0];
  for(int i=1; i<values_.size(); i++)
    ret += "," + values_[i];
  return ret;
}

/******************************************************
************************** MISC ***********************
******************************************************/

template<>
bool CSVLine::is_del(char v) {
  return v == CSV_DEL_CHAR;
}

template<>
bool CSVLine::is_del(int v) {
  return v == CSV_DEL_INT;
}

template<>
bool CSVLine::is_del(int64_t v) {
  return v == CSV_DEL_INT64_T;
}

template<>
bool CSVLine::is_del(float v) {
  return v == CSV_DEL_FLOAT;
}

template<>
bool CSVLine::is_del(double v) {
  return v == CSV_DEL_DOUBLE;
}

template<>
bool CSVLine::is_null(char v) {
  return v == CSV_NULL_CHAR;
}

template<>
bool CSVLine::is_null(int v) {
  return v == CSV_NULL_INT;
}

template<>
bool CSVLine::is_null(int64_t v) {
  return v == CSV_NULL_INT64_T;
}

template<>
bool CSVLine::is_null(float v) {
  return v == CSV_NULL_FLOAT;
}

template<>
bool CSVLine::is_null(double v) {
  return v == CSV_NULL_DOUBLE;
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

void CSVLine::operator<<(const std::string& value) {
  tokenize(value);
}

void CSVLine::operator<<(const CSVLine& csv_line) {
  values_.insert(values_.end(), 
                 csv_line.values_.begin(), csv_line.values_.end());
}

template<>
void CSVLine::operator<<(const char* value) {
  tokenize(value);
}

template<>
void CSVLine::operator<<(char* value) {
  tokenize(value);
}

template<>
void CSVLine::operator<<(char value) {
  char s[2];
  sprintf(s, "%c", value); 
  values_.push_back(s);
}

template<>
void CSVLine::operator<<(int value) {
  char s[CSV_MAX_DIGITS];
  sprintf(s, "%d", value); 
  values_.push_back(s);
}

template<>
void CSVLine::operator<<(int64_t value) {
  char s[CSV_MAX_DIGITS];
  sprintf(s, "%lld", value); 
  values_.push_back(s);
}

template<>
void CSVLine::operator<<(float value) {
  char s[CSV_MAX_DIGITS];
  sprintf(s, "%g", value); 
  values_.push_back(s);
}

template<>
void CSVLine::operator<<(double value) {
  char s[CSV_MAX_DIGITS];
  sprintf(s, "%lg", value); 
  values_.push_back(s);
}

template<>
void CSVLine::operator<<(const std::vector<char>& values) {
  char s[2];
  for(int i=0; i<values.size(); i++) {
    sprintf(s, "%c", values[i]); 
    values_.push_back(s);
  }
}

template<>
void CSVLine::operator<<(const std::vector<int>& values) {
  char s[CSV_MAX_DIGITS];
  for(int i=0; i<values.size(); i++) {
    sprintf(s, "%d", values[i]); 
    values_.push_back(s);
  }
}

template<>
void CSVLine::operator<<(const std::vector<int64_t>& values) {
  char s[CSV_MAX_DIGITS];
  for(int i=0; i<values.size(); i++) {
    sprintf(s, "%lld", values[i]); 
    values_.push_back(s);
  }
}

template<>
void CSVLine::operator<<(const std::vector<float>& values) {
  char s[CSV_MAX_DIGITS];
  for(int i=0; i<values.size(); i++) {
    sprintf(s, "%g", values[i]); 
    values_.push_back(s);
  }
}

template<>
void CSVLine::operator<<(const std::vector<double>& values) {
  char s[CSV_MAX_DIGITS];
  for(int i=0; i<values.size(); i++) {
    sprintf(s, "%lg", values[i]); 
    values_.push_back(s);
  }
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
bool CSVLine::operator>>(char& value) {
  if(pos_ == values_.size()) {
    return false;
  } else { 
    sscanf(values_[pos_++].c_str(), "%c", &value);
    return true;
  }
}

template<>
bool CSVLine::operator>>(int& value) {
  if(pos_ == values_.size()) {
    return false;
  } else {
    if(values_[pos_] == CSV_NULL_VALUE) // Value missing
      value = CSV_NULL_INT;
    else 
      sscanf(values_[pos_].c_str(), "%d", &value);
    ++pos_;
    return true;
  }
}

template<>
bool CSVLine::operator>>(int64_t& value) {
  if(pos_ == values_.size()) {
    return false;
  } else { 
    if(values_[pos_] == CSV_NULL_VALUE) // Value missing
      value = CSV_NULL_INT64_T;
    else 
      sscanf(values_[pos_].c_str(), "%lld", &value);
    ++pos_;
    return true;
  }
}

template<>
bool CSVLine::operator>>(float& value) {
  if(pos_ == values_.size()) {
    return false;
  } else { 
    if(values_[pos_] == CSV_NULL_VALUE) // Value missing
      value = CSV_NULL_FLOAT;
    else 
      sscanf(values_[pos_].c_str(), "%f", &value);
    ++pos_;
    return true;
  }
}

template<>
bool CSVLine::operator>>(double& value) {
  if(pos_ == values_.size()) {
    return false;
  } else { 
    if(values_[pos_] == CSV_NULL_VALUE) // Value missing
      value = CSV_NULL_DOUBLE;
    else 
      sscanf(values_[pos_].c_str(), "%lf", &value);
    ++pos_;
    return true;
  }
}

void CSVLine::operator=(const std::string& value) {
  pos_ = 0;
  values_.clear();
  tokenize(value);
}

void CSVLine::operator=(const CSVLine& csv_line) {
  pos_ = 0;
  values_.clear();
  values_.insert(values_.end(), 
                 csv_line.values_.begin(), csv_line.values_.end());
}

template<>
void CSVLine::operator=(const char* value) {
  pos_ = 0;
  values_.clear();
  tokenize(value);
}

template<class T>
void CSVLine::operator=(T value) {
  pos_ = 0;
  values_.clear();
  (*this) << value;
}

template<class T>
void CSVLine::operator=(const std::vector<T>& values) {
  pos_ = 0;
  values_.clear();
  (*this) << values;
}

/******************************************************
******************* PRIVATE METHODS *******************
******************************************************/

void CSVLine::tokenize(std::string line) {
  // Do nothing if the string is empty
  if(line.size() == 0)
    return;

  // If the CSV line is empty and the input line is a comment line 
  // (starting with '#'), do not tokenize the input line
  if(values_.size() == 0 && line[0] == '#') {
    values_.push_back(line);
  } else { // Tokenize
    char* token = strtok(&line[0], ",");
    while(token != NULL) {
      values_.push_back(token);
      token = strtok(NULL, ",");
    }
  }
}

/* ----------------- CSVFile functions ------------- */

/******************************************************
************* CONSTRUCTORS & DESTRUCTORS **************
******************************************************/

CSVFile::CSVFile() { 
  buffer_ = NULL;
  buffer_end_ = 0;
  buffer_offset_ = 0;
  file_offset_ = 0;
}

CSVFile::~CSVFile() {
  close();
}

/******************************************************
********************** OPERATORS **********************
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
}

bool CSVFile::open(const std::string& filename, 
                   const char* mode,
                   size_t segment_size) {
  filename_ = absolute_path(filename);

  if(strcmp(mode_, "r") == 0 && !file_exists(filename_))
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

  const std::string& line = csv_line.str(); 
  assert(line.size() <= segment_size_);

  // Initialize the buffer.
  if(buffer_ == NULL) 
    buffer_ = new char[segment_size_];
  
  // Flush the buffer to the file if its stored data size plus the size of the
  // new line exceed CSVFile::segment_size_. The +1 is for the '\n' appended
  // after the line.
  if(buffer_offset_ + line.size() + 1 > segment_size_) {	
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

// Explicit template instantiations
template void CSVLine::operator= <char> (char value);
template void CSVLine::operator= <int> (int value);
template void CSVLine::operator= <int64_t> (int64_t value);
template void CSVLine::operator= <float> (float value);
template void CSVLine::operator= <double> (double value);
template void CSVLine::operator= <char> 
    (const std::vector<char>& values);
template void CSVLine::operator= <int> 
    (const std::vector<int>& values);
template void CSVLine::operator= <int64_t> 
    (const std::vector<int64_t>& values);
template void CSVLine::operator= <float> 
    (const std::vector<float>& values);
template void CSVLine::operator= <double> 
    (const std::vector<double>& values);
