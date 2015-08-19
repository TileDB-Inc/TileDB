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
#include <string.h>
#include <time.h>

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

size_t CSVLine::size() const {
  return values_.size();
}

std::string CSVLine::str() const {
  if(values_.size() == 0) 
    return "";

  std::string ret = values_[0];
  for(size_t i=1; i<values_.size(); i++)
    ret += "," + values_[i];
  return ret;
}

const std::vector<std::string>& CSVLine::values() const {
  return values_;
}

/******************************************************
*********************** MUTATORS **********************
******************************************************/

void CSVLine::clear() {
  values_.clear();
  pos_ = 0;
}

void CSVLine::reset() {
  pos_ = 0;
}

/******************************************************
*********************** OPERATORS *********************
******************************************************/

const std::string& CSVLine::operator*() const {
  return values_[pos_];
}

void CSVLine::operator++() {
  ++pos_;
}

void CSVLine::operator+=(int step) {
  pos_ += step;
}

void CSVLine::operator<<(const std::string& value) {
  if(value == "")
    values_.push_back(value);
  else
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
void CSVLine::operator<<(size_t value) {
  char s[CSV_MAX_DIGITS];
  sprintf(s, "%u", value); 
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
  for(size_t i=0; i<values.size(); i++) {
    sprintf(s, "%c", values[i]); 
    values_.push_back(s);
  }
}

template<>
void CSVLine::operator<<(const std::vector<size_t>& values) {
  char s[CSV_MAX_DIGITS];
  for(size_t i=0; i<values.size(); i++) {
    sprintf(s, "%u", values[i]); 
    values_.push_back(s);
  }
}

template<>
void CSVLine::operator<<(const std::vector<int>& values) {
  char s[CSV_MAX_DIGITS];
  for(size_t i=0; i<values.size(); i++) {
    sprintf(s, "%d", values[i]); 
    values_.push_back(s);
  }
}

template<>
void CSVLine::operator<<(const std::vector<int64_t>& values) {
  char s[CSV_MAX_DIGITS];
  for(size_t i=0; i<values.size(); i++) {
    sprintf(s, "%lld", values[i]); 
    values_.push_back(s);
  }
}

template<>
void CSVLine::operator<<(const std::vector<float>& values) {
  char s[CSV_MAX_DIGITS];
  for(size_t i=0; i<values.size(); i++) {
    sprintf(s, "%g", values[i]); 
    values_.push_back(s);
  }
}

template<>
void CSVLine::operator<<(const std::vector<double>& values) {
  char s[CSV_MAX_DIGITS];
  for(size_t i=0; i<values.size(); i++) {
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
    if(values_[pos_] == NULL_VALUE) // Value missing
      value = NULL_CHAR;
    else if(values_[pos_] == DEL_VALUE) // Value deleted
      value = DEL_CHAR;
    else 
      sscanf(values_[pos_].c_str(), "%c", &value);
    ++pos_;
    return true;
  }
}

template<>
bool CSVLine::operator>>(int& value) {
  if(pos_ == values_.size()) {
    return false;
  } else {
    if(values_[pos_] == NULL_VALUE) // Value missing
      value = NULL_INT;
    else if(values_[pos_] == DEL_VALUE) // Value deleted
      value = DEL_INT;
    else 
      sscanf(values_[pos_].c_str(), "%d", &value);
    ++pos_;
    return true;
  }
}

template<>
bool CSVLine::operator>>(size_t& value) {
  if(pos_ == values_.size()) {
    return false;
  } else {
    if(values_[pos_] == NULL_VALUE) // Value missing
      value = NULL_SIZE_T;
    else if(values_[pos_] == DEL_VALUE) // Value deleted
      value = DEL_SIZE_T;
    else 
      sscanf(values_[pos_].c_str(), "%u", &value);
    ++pos_;
    return true;
  }
}

template<>
bool CSVLine::operator>>(int64_t& value) {
  if(pos_ == values_.size()) {
    return false;
  } else { 
    if(values_[pos_] == NULL_VALUE) // Value missing
      value = NULL_INT64_T;
    else if(values_[pos_] == DEL_VALUE) // Value deleted
      value = DEL_INT64_T;
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
    if(values_[pos_] == NULL_VALUE) // Value missing
      value = NULL_FLOAT;
    else if(values_[pos_] == DEL_VALUE) // Value deleted
      value = DEL_FLOAT;
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
    if(values_[pos_] == NULL_VALUE) // Value missing
      value = NULL_DOUBLE;
    else if(values_[pos_] == DEL_VALUE) // Value deleted
      value = DEL_DOUBLE;
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

template<>
void CSVLine::operator=(char* value) {
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
