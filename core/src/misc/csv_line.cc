/**
 * @file   csv_line.cc
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
 * This file implements the CSVLine class.
 */

#include "csv_line.h"
#include "special_values.h"
#include "utils.h"
#include <assert.h>
#include <string.h>
#include <time.h>

/******************************************************
************ CONSTRUCTORS & DESTRUCTORS ***************
******************************************************/

CSVLine::CSVLine() {
  line_ = NULL;
  line_size_ = 0;
  line_allocated_size_ = 0;
  mode_ = NONE;
  offsets_allocated_size_ = 0;
  pos_ = 0;
  val_num_ = 0;
}

CSVLine::CSVLine(char* line) {
  line_ = line;
  line_allocated_size_ = 0;
  mode_ = READ;
  offsets_.resize(CSV_INITIAL_VAL_NUM);
  offsets_allocated_size_ = CSV_INITIAL_VAL_NUM;
  pos_ = 0;

  tokenize();
}

CSVLine::~CSVLine() {
  if(mode_ == WRITE && line_ != NULL)
    free(line_);
}

/******************************************************
*********************** ACCESSORS *********************
******************************************************/

const char* CSVLine::c_str() const {
  assert(mode_ == WRITE);

  return static_cast<const char*>(line_);
}

int CSVLine::val_num() const {
  return val_num_;
}

std::vector<std::string> CSVLine::values_str_vec() const {
  std::vector<std::string> ret;

  char* line = static_cast<char*>(line_);

  for(int i=0; i<val_num_; ++i) 
    ret.push_back(line + offsets_[i]);

  return ret;
}

/******************************************************
*********************** MUTATORS **********************
******************************************************/

void CSVLine::clear() {
  if(mode_ == WRITE && line_ != NULL)
    free(line_);

  line_ = NULL;
  line_allocated_size_ = 0;
  line_size_ = 0;
  mode_ = NONE;
  offsets_allocated_size_ = 0;
  pos_ = 0;
  val_num_ = 0;
}

void CSVLine::reset() {
  pos_ = 0;
}

/******************************************************
*********************** OPERATORS *********************
******************************************************/

const char* CSVLine::operator*() const {
  assert(mode_ == READ);

  return static_cast<const char*>(line_) + offsets_[pos_];
}

void CSVLine::operator++() {
  assert(mode_ == READ);

  ++pos_;
}

void CSVLine::operator+=(int step) {
  assert(mode_ == READ);

  pos_ += step;
}

void CSVLine::operator<<(const CSVLine& csv_line) {
  append(csv_line.c_str(), csv_line.strlen() + 1);
}

void CSVLine::operator<<(const std::string& value) {
  append(value.c_str(), value.size() + 1);
}

void CSVLine::operator<<(const char* value) {
  append(value, ::strlen(value) + 1);
}

template<>
void CSVLine::operator<<(char* value) {
  append(value, ::strlen(value) + 1);
}

template<>
void CSVLine::operator<<(char value) {
  char s[2];
  sprintf(s, "%c", value); 
  append(s, ::strlen(s) + 1);
}

template<>
void CSVLine::operator<<(size_t value) {
  char s[CSV_MAX_DIGITS];
  sprintf(s, "%u", value); 
  append(s, ::strlen(s) + 1);
}

template<>
void CSVLine::operator<<(int value) {
  char s[CSV_MAX_DIGITS];
  sprintf(s, "%d", value); 
  append(s, ::strlen(s) + 1);
}

template<>
void CSVLine::operator<<(int64_t value) {
  char s[CSV_MAX_DIGITS];
  sprintf(s, "%lld", value); 
  append(s, ::strlen(s) + 1);
}

template<>
void CSVLine::operator<<(float value) {
  char s[CSV_MAX_DIGITS];
  sprintf(s, "%g", value); 
  append(s, ::strlen(s) + 1);
}

template<>
void CSVLine::operator<<(double value) {
  char s[CSV_MAX_DIGITS];
  sprintf(s, "%lg", value); 
  append(s, ::strlen(s) + 1);
}

template<>
bool CSVLine::operator>>(std::string& value) {
  if(mode_ != READ || pos_ == val_num_) {
    return false;
  } else {

    char* value_ = static_cast<char*>(line_) + offsets_[pos_];
    value = value_;

    ++pos_;
    return true;
  }
}

template<>
bool CSVLine::operator>>(char& value) {
  if(mode_ != READ || pos_ == val_num_) {
    return false;
  } else {
    char* value_ = static_cast<char*>(line_) + offsets_[pos_];

    if(*value_ == NULL_VALUE) // Value missing
      value = NULL_CHAR;
    else if(*value_ == DEL_VALUE) // Value deleted
      value = DEL_CHAR;
    else 
      value = *value_;

    ++pos_;
    return true;
  }
}

template<>
bool CSVLine::operator>>(int& value) {
  if(mode_ != READ || pos_ == val_num_) {
    return false;
  } else {
    char* value_ = static_cast<char*>(line_) + offsets_[pos_];

    if(*value_ == NULL_VALUE) // Value missing
      value = NULL_INT;
    else if(*value_ == DEL_VALUE) // Value deleted
      value = DEL_INT;
    else 
      sscanf(value_, "%d", &value);

    ++pos_;
    return true;
  }
}

template<>
bool CSVLine::operator>>(size_t& value) {
  if(mode_ != READ || pos_ == val_num_) {
    return false;
  } else {
    char* value_ = static_cast<char*>(line_) + offsets_[pos_];

    if(*value_ == NULL_VALUE) // Value missing
      value = NULL_SIZE_T;
    else if(*value_ == DEL_VALUE) // Value deleted
      value = DEL_SIZE_T;
    else 
      sscanf(value_, "%u", &value);

    ++pos_;
    return true;
  }
}

template<>
bool CSVLine::operator>>(int64_t& value) {
  if(mode_ != READ || pos_ == val_num_) {
    return false;
  } else { 
    char* value_ = static_cast<char*>(line_) + offsets_[pos_];

    if(*value_ == NULL_VALUE) // Value missing
      value = NULL_INT64_T;
    else if(*value_ == DEL_VALUE) // Value deleted
      value = DEL_INT64_T;
    else 
      sscanf(value_, "%lld", &value);

    ++pos_;
    return true;
  }
}

template<>
bool CSVLine::operator>>(float& value) {
  if(mode_ != READ || pos_ == val_num_) {
    return false;
  } else { 
    char* value_ = static_cast<char*>(line_) + offsets_[pos_];

    if(*value_ == NULL_VALUE) // Value missing
      value = NULL_FLOAT;
    else if(*value_ == DEL_VALUE) // Value deleted
      value = DEL_FLOAT;
    else 
      sscanf(value_, "%f", &value);

    ++pos_;
    return true;
  }
}

template<>
bool CSVLine::operator>>(double& value) {
  if(mode_ != READ || pos_ == val_num_) {
    return false;
  } else { 
    char* value_ = static_cast<char*>(line_) + offsets_[pos_];

    if(*value_ == NULL_VALUE) // Value missing
      value = NULL_DOUBLE;
    else if(*value_ == DEL_VALUE) // Value deleted
      value = DEL_DOUBLE;
    else 
      sscanf(value_, "%lf", &value);

    ++pos_;
    return true;
  }
}

void CSVLine::operator=(char* line) {
  if(mode_ == WRITE) {
    if(line_ != NULL)
      free(line_);

    line_allocated_size_ = 0;
  }

  line_ = line;
  pos_ = 0;

  if(mode_ != READ) {
    mode_ = READ;
    offsets_.resize(CSV_INITIAL_VAL_NUM);
    offsets_allocated_size_ = CSV_INITIAL_VAL_NUM;
  }

  tokenize();
}

/******************************************************
******************* PRIVATE METHODS *******************
******************************************************/

void CSVLine::append(const char* value, size_t size) {
  assert(mode_ != READ);
  assert(size > 0);

  if(mode_ != WRITE) {
    mode_ = WRITE;
    line_ = malloc(CSV_INITIAL_LINE_SIZE);
    line_allocated_size_ = CSV_INITIAL_LINE_SIZE;
  }

  while(line_size_ + size > line_allocated_size_) {
    expand_buffer(line_, line_allocated_size_);
    line_allocated_size_ *= 2;
  } 

  char* line = static_cast<char*>(line_);

  // Substitute last '\0' with a separator
  if(line_size_ != 0) {
    char c = CSV_SEPARATOR;
    memcpy(line + line_size_ - 1, &c, sizeof(char));
  }

  // Append the value
  memcpy(line + line_size_, value, size);
  line_size_ += size;

  // Increment number of values
  ++val_num_; // One is for sure
  for(int i=0; i<size; ++i) {
    // In case the string contains more separators
    if(value[i] == CSV_SEPARATOR)
      ++val_num_;
  }
}

size_t CSVLine::strlen() const {
  assert(mode_ == WRITE);

  if(line_size_ == 0)
    return 0;
  else 
    return line_size_ - 1; // Exclude the last '\0'
}

void CSVLine::tokenize() {
  assert(mode_ == READ);
  assert(offsets_allocated_size_ > 0);

  line_size_ = 0;
  val_num_ = 0;
  size_t offset = 0, next_value_offset = 0;
  size_t value_size = 0;
  char* line = static_cast<char*>(line_);

  // Empty line
  if(line == NULL) 
    return;

  // Comment line - treat is as a single string
  if(line[0] == CSV_COMMENT) {
    val_num_ = 1;
    line_size_ = ::strlen(line) + 1;
  }

  // Tokenize 
  while(line[offset] != '\0') {
    if(line[offset] == CSV_SEPARATOR) {
      line[offset] = '\0';
      if(val_num_ == offsets_allocated_size_) {
        offsets_allocated_size_ *= 2;
        offsets_.resize(offsets_allocated_size_);
      } 
      offsets_[val_num_] = next_value_offset;
      ++val_num_;
      next_value_offset = offset + 1;
    }

    ++offset;
  }

  line_size_ = offset;

  // Handle last value
  offsets_[val_num_] = next_value_offset;
  ++val_num_;
}

