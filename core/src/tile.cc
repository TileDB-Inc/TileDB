/**
 * @file   tile.cc
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
 * This file implements the Tile class.
 */

#include "tile.h"
#include "utils.h"
#include <assert.h>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/******************************************************
************* CONSTRUCTORS & DESTRUCTORS **************
******************************************************/

Tile::Tile(int64_t tile_id, int dim_num, const std::type_info* cell_type,
           int64_t payload_capacity) 
    : tile_id_(tile_id), dim_num_(dim_num), cell_type_(cell_type), 
      payload_capacity_(payload_capacity) {
  assert(dim_num >= 0 && payload_capacity >= 0);

  if(dim_num == 0) 
    tile_type_ = ATTRIBUTE;
  else
    tile_type_ = COORDINATE;

  mbr_ = NULL;
  cell_num_ = 0;
  tile_size_ = 0;

  if(*cell_type == typeid(char)) {
    assert(tile_type_ == ATTRIBUTE); // Char not supported for coordinate cells 
    cell_size_ = sizeof(char);
  } else if(*cell_type == typeid(int)) {
    cell_size_ = sizeof(int) * ((tile_type_ == ATTRIBUTE) ? 1 : dim_num_);
  } else if(*cell_type == typeid(int64_t)) {
    cell_size_ = sizeof(int64_t) * ((tile_type_ == ATTRIBUTE) ? 1 : dim_num_);
  } else if(*cell_type == typeid(float)) {
    cell_size_ = sizeof(float) * ((tile_type_ == ATTRIBUTE) ? 1 : dim_num_);
  } else if(*cell_type == typeid(double)) {
    cell_size_ = sizeof(double) * ((tile_type_ == ATTRIBUTE) ? 1 : dim_num_);
  } else {
    assert(0); // No other cell type is supported
  }

  if(payload_capacity_ == 0) {
    payload_ = NULL;
    payload_alloc_ = false;
  } else { 
    payload_ = malloc(payload_capacity_*cell_size_);
    payload_alloc_ = true;
  }
}

Tile::~Tile() {
  clear();
}

/******************************************************
********************* ACCESSORS ***********************
******************************************************/

std::pair<const void*, const void*> Tile::bounding_coordinates() const {
  // Attribute tile or empty tile
  if(tile_type_ == ATTRIBUTE || cell_num_ == 0)
    return std::pair<const void*, const void*>(NULL, NULL);

  // Coordinate tile
  char* lower = static_cast<char*>(payload_);
  char* upper = lower + (cell_num_-1)*cell_size_;

  return std::pair<const void*, const void*>(lower, upper);
}

const void* Tile::cell(int64_t pos) const {
  assert(pos >= 0 && pos < cell_num_);

  return static_cast<char*>(payload_) + pos*cell_size_; 
}

void Tile::copy_payload(void* buffer) const {
  memcpy(buffer, payload_, tile_size_);
}

/******************************************************
********************** MUTATORS ***********************
******************************************************/

void Tile::clear() {
  clear_mbr();
  clear_payload();
}

void Tile::set_mbr(const void* mbr) {
  assert(tile_type_ == COORDINATE);

  clear_mbr();

  mbr_ = malloc(2*cell_size_); 
  memcpy(mbr_, mbr, 2*cell_size_);
}

void Tile::set_payload(void* payload, size_t payload_size) {
  assert(payload_size % cell_size_ == 0);

  clear_payload();
  payload_ = payload;
  cell_num_ = payload_size / cell_size_;
  tile_size_ = payload_size;
}

/******************************************************
********************** OPERATORS **********************
******************************************************/

void Tile::operator<<(void* value) {
  if(*cell_type_ == typeid(char))
    *this << static_cast<char*>(value);
  else if(*cell_type_ == typeid(int))
    *this << static_cast<int*>(value);
  else if(*cell_type_ == typeid(int64_t))
    *this << static_cast<int64_t*>(value);
  else if(*cell_type_ == typeid(float))
    *this << static_cast<float*>(value);
  else if(*cell_type_ == typeid(double))
    *this << static_cast<double*>(value);
}

void Tile::operator<<(const void* value) {
  if(*cell_type_ == typeid(char))
    *this << static_cast<const char*>(value);
  else if(*cell_type_ == typeid(int))
    *this << static_cast<const int*>(value);
  else if(*cell_type_ == typeid(int64_t))
    *this << static_cast<const int64_t*>(value);
  else if(*cell_type_ == typeid(float))
    *this << static_cast<const float*>(value);
  else if(*cell_type_ == typeid(double))
    *this << static_cast<const double*>(value);
}

template<class T>
void Tile::operator<<(T* value) {
  assert(*cell_type_ == typeid(T));
  assert(payload_capacity_ > 0);

  if(cell_num_ == payload_capacity_)
    expand_payload();

  char* payload_pos = static_cast<char*>(payload_) + tile_size_;
  memcpy(payload_pos, value, cell_size_);

  if(tile_type_ == COORDINATE)
    expand_mbr(value);

  ++cell_num_;
  tile_size_ += cell_size_;
}

template<class T>
void Tile::operator<<(const T* value) {
  assert(*cell_type_ == typeid(T));
  assert(payload_capacity_ > 0);

  if(cell_num_ == payload_capacity_)
    expand_payload();

  char* payload_pos = static_cast<char*>(payload_) + tile_size_;
  memcpy(payload_pos, value, cell_size_);

  if(tile_type_ == COORDINATE)
    expand_mbr(value);

  ++cell_num_;
  tile_size_ += cell_size_;
}

template<class T>
void Tile::operator<<(const T& value) {
  *this << &value;
}

/******************************************************
************************ MISC *************************
******************************************************/

template<class T> 
bool Tile::cell_inside_range(int64_t pos, const T* range) const {
  assert(*cell_type_ == typeid(T));
  assert(tile_type_ == COORDINATE);

  const T* coords = static_cast<const T*>(cell(pos));

  for(int i=0; i<dim_num_; ++i) {
    if(coords[i] < range[2*i] || coords[i] > range[2*i+1])
      return false;
  }
  return true;
}

void Tile::print() const {
  std::cout << "=========== Tile info ==========\n";
  std::cout << "Tile id: " << tile_id_ << "\n";
  std:: cout << "Dim num: " << dim_num_ << "\n";
  std::cout << "Cell type: ";
  if(*cell_type_ == typeid(char))
    std::cout << "char\n";
  else if(*cell_type_ == typeid(int))
    std::cout << "int\n";
  else if(*cell_type_ == typeid(int64_t))
    std::cout << "int64_t\n";
  else if(*cell_type_ == typeid(float))
    std::cout << "float\n";
  else if(*cell_type_ == typeid(double))
    std::cout << "double\n";

  std::cout << "Tile type: "
            << (tile_type_ == ATTRIBUTE ? "ATTRIBUTE" : "COORDINATE") << "\n";

  std::cout << "Cell size: " << cell_size_ << "\n";
  std::cout << "Cell num: " << cell_num_ << "\n";
  std::cout << "Tile size: " << tile_size_ << "\n";

  if(*cell_type_ == typeid(char)) {
    assert(tile_type_ == ATTRIBUTE);
    print_payload<char>();
    print_mbr<char>();
    print_bounding_coordinates<char>();
  } else if(*cell_type_ == typeid(int)) {
    print_payload<int>();
    print_mbr<int>();
    print_bounding_coordinates<int>();
  } else if(*cell_type_ == typeid(int64_t)) {
    print_payload<int64_t>();
    print_mbr<int64_t>();
    print_bounding_coordinates<int64_t>();
  } else if(*cell_type_ == typeid(float)) {
    print_payload<float>();
    print_mbr<float>();
    print_bounding_coordinates<float>();
  } else if(*cell_type_ == typeid(double)) {
    print_payload<double>();
    print_mbr<double>();
    print_bounding_coordinates<double>();
  }

  std::cout << "Payload capacity: " << payload_capacity_ << "\n";
  std::cout << "Payload allocated: " << ((payload_alloc_) ? "true" : "no") << "\n";

  std::cout << "========== End of Tile info ========== \n\n";
}

/******************************************************
********************* CELL ITERATOR *******************
******************************************************/

Tile::const_cell_iterator::const_cell_iterator() 
    : tile_(NULL), pos_(-1), cell_(NULL), end_(true) {
}

Tile::const_cell_iterator::const_cell_iterator(const Tile* tile, int64_t pos)
    : tile_(tile), pos_(pos) {
  if(pos_ >= 0 && pos_ < tile_->cell_num()) {
    cell_ = tile_->cell(pos_);
    end_ = false;
  } else {
    cell_ = NULL;
    end_ = true;
  }
}

bool Tile::const_cell_iterator::is_del() const {
  // Applies only to attribute values
  assert(tile_->tile_type() == ATTRIBUTE);

  if(*tile_->cell_type() == typeid(char)) { 
    const char* v = static_cast<const char*>(**this);
    return (*v  == TL_DEL_CHAR);
  } else if(*tile_->cell_type() == typeid(int)) {
    const int* v = static_cast<const int*>(**this);
    return (*v  == TL_DEL_INT);
  } else if(*tile_->cell_type() == typeid(int64_t)) {
    const int64_t* v = static_cast<const int64_t*>(**this);
    return (*v  == TL_DEL_INT64_T);
  } else if(*tile_->cell_type() == typeid(float)) {
    const float* v = static_cast<const float*>(**this);
    return (*v  == TL_DEL_FLOAT);
  } else if(*tile_->cell_type() == typeid(double)) {
    const double* v = static_cast<const double*>(**this);
    return (*v  == TL_DEL_DOUBLE);
  }
}

bool Tile::const_cell_iterator::is_null() const {
  // Applies only to attribute values
  assert(tile_->tile_type() == ATTRIBUTE);

  if(*tile_->cell_type() == typeid(char)) { 
    const char* v = static_cast<const char*>(**this);
    return (*v  == TL_NULL_CHAR);
  } else if(*tile_->cell_type() == typeid(int)) {
    const int* v = static_cast<const int*>(**this);
    return (*v  == TL_NULL_INT);
  } else if(*tile_->cell_type() == typeid(int64_t)) {
    const int64_t* v = static_cast<const int64_t*>(**this);
    return (*v  == TL_NULL_INT64_T);
  } else if(*tile_->cell_type() == typeid(float)) {
    const float* v = static_cast<const float*>(**this);
    return (*v  == TL_NULL_FLOAT);
  } else if(*tile_->cell_type() == typeid(double)) {
    const double* v = static_cast<const double*>(**this);
    return (*v  == TL_NULL_DOUBLE);
  }
}

void Tile::const_cell_iterator::operator=(const const_cell_iterator& rhs) {
  pos_ = rhs.pos_;
  cell_ = rhs.cell_;
  end_ = rhs.end_;
  tile_ = rhs.tile_;
}

Tile::const_cell_iterator Tile::const_cell_iterator::operator+(int64_t step) {
  const_cell_iterator it = *this;
  it.pos_ += step;
  if(it.pos_ >= 0 && it.pos_ < tile_->cell_num()) {
    it.cell_ = tile_->cell(pos_);
    it.end_ = false;
  } else {
    it.cell_ = NULL;
    it.end_ = true;
  }
  return it;
}

void Tile::const_cell_iterator::operator+=(int64_t step) {
  pos_ += step;
  if(pos_ >= 0 && pos_ < tile_->cell_num()) {
    cell_ = tile_->cell(pos_);
    end_ = false;
  } else {  
    cell_ = NULL;
    end_ = true;
  }
}

Tile::const_cell_iterator Tile::const_cell_iterator::operator++() {
  ++pos_;
  if(pos_ >= 0 && pos_ < tile_->cell_num()) {
    cell_ = tile_->cell(pos_);
    end_ = false;
  } else { // end of the iterator 
    cell_ = NULL;
    end_ = true;
  }
  return *this;
}

Tile::const_cell_iterator Tile::const_cell_iterator::operator++(int junk) {
  const_cell_iterator it = *this;
  pos_++;
  if(pos_ >= 0 && pos_ < tile_->cell_num()) {
    cell_ = tile_->cell(pos_);
    end_ = false;
  } else {
    cell_ = NULL;
    end_ = true;
  }
  return it;
}

bool Tile::const_cell_iterator::operator==(
    const const_cell_iterator& rhs) const {
  return (tile_ == rhs.tile_ && pos_ == rhs.pos_);
}

bool Tile::const_cell_iterator::operator!=(
  const const_cell_iterator& rhs) const {
  return (tile_ != rhs.tile_ || pos_ != rhs.pos_);
}

const void* Tile::const_cell_iterator::operator*() const {
  return cell_;
}

Tile::const_cell_iterator Tile::begin() const {
  return const_cell_iterator(this, 0);
}

Tile::const_cell_iterator Tile::end() {
  return const_cell_iterator();
}

template<class T>
bool Tile::const_cell_iterator::cell_inside_range(
    const T* range) const {
  return tile_->cell_inside_range(pos_, range); 
}
/******************************************************
******************** PRIVATE METHODS ******************
******************************************************/

void Tile::clear_mbr() {
  if(mbr_ != NULL) 
    free(mbr_);

  mbr_ = NULL;
}

void Tile::clear_payload() {
  if(payload_ != NULL && payload_alloc_) 
    free(payload_);

  payload_ = NULL;
  payload_alloc_ = false;
  cell_num_ = 0;
  tile_size_ = 0;
  payload_capacity_ = 0;
}

template<class T>
inline 
void Tile::expand_mbr(const T* coords) {
  assert(*cell_type_ == typeid(T));
  assert(tile_type_ == COORDINATE);

  if(mbr_ == NULL) 
    mbr_ = malloc(2*cell_size_);

  T* mbr = static_cast<T*>(mbr_);

  if(cell_num_ == 0) {
    for(int i=0; i<dim_num_; ++i) {
      mbr[2*i] = coords[i]; 
      mbr[2*i+1] = coords[i]; 
    }
  } else {
    for(int i=0; i<dim_num_; ++i) {
      // Update lower bound on dimension i
      if(mbr[2*i] > coords[i])
        mbr[2*i] = coords[i];

      // Update upper bound on dimension i
      if(mbr[2*i+1] < coords[i])
        mbr[2*i+1] = coords[i];   
    }	
  }
}

void Tile::expand_payload() {
  assert(payload_alloc_);
  assert(payload_capacity_ > 0);

  expand_buffer(payload_, payload_capacity_*cell_size_);
  payload_capacity_ *= 2;
}

template<class T>
void Tile::print_bounding_coordinates() const {
  assert(*cell_type_ == typeid(T));

  // Applies only to coordinate tiles
  if(tile_type_ != COORDINATE)
    return;

  std::cout << "Bounding coordinates: \n";

  T* lower = static_cast<T*>(payload_);
  T* upper = lower + dim_num_*(cell_num_-1);

  if(cell_num_ != 0) {
    std::cout << "\t";
    for(int j=0; j<dim_num_; ++j)
      std::cout << lower[j] << "\t";
    std::cout << "\n\t";
    for(int j=0; j<dim_num_; ++j)
      std::cout << upper[j] << "\t";
    std::cout << "\n";
  }
}

template<class T>
void Tile::print_mbr() const {
  assert(*cell_type_ == typeid(T));

  // Applies only to coordinate tiles
  if(tile_type_ != COORDINATE)
    return;

  T* mbr = static_cast<T*>(mbr_);

  std::cout << "MBR: \n";
  if(cell_num_ != 0) {
    assert(mbr_ != NULL);
    for(int i=0; i<dim_num_; ++i)
      std::cout << "\t dim " << i << ": [" << mbr[2*i] << "," 
                                           << mbr[2*i+1] << "]\n";
  }
}

template<class T>
void Tile::print_payload() const {
  assert(*cell_type_ == typeid(T));

  T* payload = static_cast<T*>(payload_);

  std::cout << "Payload contents:\n";

  if(tile_type_ == ATTRIBUTE) {
    for(int64_t i=0; i < cell_num_; ++i)
      std::cout << "\t" << payload[i] << "\n";
  } else { // tile_type_ == COORDINATE
    for(int64_t i=0; i < cell_num_; ++i) {
      std::cout << "\t";
      for(int j=0; j<dim_num_; ++j)
        std::cout << payload[i*dim_num_ + j] << "\t";
      std::cout << "\n";
    }
  }
}

// Explicit template instantiations
template void Tile::operator<< <char>
    (char* value);
template void Tile::operator<< <int>
    (int* value);
template void Tile::operator<< <int64_t>
    (int64_t* value);
template void Tile::operator<< <float>
    (float* value);
template void Tile::operator<< <double>
    (double* value);
template void Tile::operator<< <char>
    (const char* value);
template void Tile::operator<< <int>
    (const int* value);
template void Tile::operator<< <int64_t>
    (const int64_t* value);
template void Tile::operator<< <float>
    (const float* value);
template void Tile::operator<< <double>
    (const double* value);
template void Tile::operator<< <char>
    (const char& value);
template void Tile::operator<< <int>
    (const int& value);
template void Tile::operator<< <int64_t>
    (const int64_t& value);
template void Tile::operator<< <float>
    (const float& value);
template void Tile::operator<< <double>
    (const double& value);
template bool Tile::cell_inside_range<int>
    (int64_t pos, const int* range) const;
template bool Tile::cell_inside_range<int64_t>
    (int64_t pos, const int64_t* range) const;
template bool Tile::cell_inside_range<float>
    (int64_t pos, const float* range) const;
template bool Tile::cell_inside_range<double>
    (int64_t pos, const double* range) const;
template bool Tile::const_cell_iterator::cell_inside_range<int>
    (const int* range) const;
template bool Tile::const_cell_iterator::cell_inside_range<int64_t>
    (const int64_t* range) const;
template bool Tile::const_cell_iterator::cell_inside_range<float>
    (const float* range) const;
template bool Tile::const_cell_iterator::cell_inside_range<double>
    (const double* range) const;
template void Tile::expand_mbr<int>(const int* coords);
template void Tile::expand_mbr<int64_t>(const int64_t* coords);
template void Tile::expand_mbr<float>(const float* coords);
template void Tile::expand_mbr<double>(const double* coords);
template void Tile::print_bounding_coordinates<int>() const;
template void Tile::print_bounding_coordinates<int64_t>() const;
template void Tile::print_bounding_coordinates<float>() const;
template void Tile::print_bounding_coordinates<double>() const;
template void Tile::print_mbr<int>() const;
template void Tile::print_mbr<int64_t>() const;
template void Tile::print_mbr<float>() const;
template void Tile::print_mbr<double>() const;
template void Tile::print_payload<int>() const;
template void Tile::print_payload<int64_t>() const;
template void Tile::print_payload<float>() const;
template void Tile::print_payload<double>() const;
