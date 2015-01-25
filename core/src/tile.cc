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
 * This file implements the Tile, AttributeTile and CoordinateTile classes.
 */

#include "tile.h"
#include <iostream>
#include <stdio.h>
#include <string.h>
#include <assert.h>

/* ----------------- Tile functions ---------------- */

/******************************************************
********************** OPERATORS **********************
******************************************************/

template<class T> 
void Tile::operator<<(const T& value) {
  append_cell(value);
}

bool Tile::operator<<(CSVLine& csv_line) {
  if(tile_type_ == ATTRIBUTE) {
    if(*cell_type_ == typeid(char)) 
     return append_attribute_value_from_csv_line<char>(csv_line);
    else if(*cell_type_ == typeid(int)) 
     return append_attribute_value_from_csv_line<int>(csv_line);
    else if(*cell_type_ == typeid(int64_t)) 
     return append_attribute_value_from_csv_line<int64_t>(csv_line);
    else if(*cell_type_ == typeid(float)) 
     return append_attribute_value_from_csv_line<float>(csv_line);
    else if(*cell_type_ == typeid(double)) 
     return append_attribute_value_from_csv_line<double>(csv_line);
  } else {
    if(*cell_type_ == typeid(int)) 
      return append_coordinates_from_csv_line<int>(csv_line);
    else if(*cell_type_ == typeid(int64_t)) 
      return append_coordinates_from_csv_line<int64_t>(csv_line);
    else if(*cell_type_ == typeid(float)) 
      return append_coordinates_from_csv_line<float>(csv_line);
    else if(*cell_type_ == typeid(double)) 
      return append_coordinates_from_csv_line<double>(csv_line);
  }
}

void Tile::operator<<(const Tile::const_iterator& cell_it) {
  if(tile_type_ == ATTRIBUTE) {
    if(*cell_type_ == typeid(char)) 
     append_attribute_value_from_cell_it<char>(cell_it);
    else if(*cell_type_ == typeid(int)) 
     append_attribute_value_from_cell_it<int>(cell_it);
    else if(*cell_type_ == typeid(int64_t)) 
     append_attribute_value_from_cell_it<int64_t>(cell_it);
    else if(*cell_type_ == typeid(float)) 
     append_attribute_value_from_cell_it<float>(cell_it);
    else if(*cell_type_ == typeid(double)) 
     append_attribute_value_from_cell_it<double>(cell_it);
  } else {
    if(*cell_type_ == typeid(int)) 
      append_coordinates_from_cell_it<int>(cell_it);
    else if(*cell_type_ == typeid(int64_t)) 
      append_coordinates_from_cell_it<int64_t>(cell_it);
    else if(*cell_type_ == typeid(float)) 
      append_coordinates_from_cell_it<float>(cell_it);
    else if(*cell_type_ == typeid(double)) 
      append_coordinates_from_cell_it<double>(cell_it);
  }
}

/******************************************************
******************** CELL ITERATORS *******************
******************************************************/

// ------------------ //
// const_iterator_ret //
// ------------------ //

template<>
Tile::const_iterator::const_iterator_ret::operator char () {
  return tile_->cell_char(pos_);
}

template<>
Tile::const_iterator::const_iterator_ret::operator int () {
  return tile_->cell_int(pos_);
}

template<>
Tile::const_iterator::const_iterator_ret::operator int64_t () {
  return tile_->cell_int64_t(pos_);
}

template<>
Tile::const_iterator::const_iterator_ret::operator float () {
  return tile_->cell_float(pos_);
}

template<>
Tile::const_iterator::const_iterator_ret::operator double () {
  return tile_->cell_double(pos_);
}

template<>
Tile::const_iterator::const_iterator_ret::
                      operator std::vector<int> () {
  return tile_->cell_v_int(pos_);
}

template<>
Tile::const_iterator::const_iterator_ret::
                      operator std::vector<int64_t> () {
  return tile_->cell_v_int64_t(pos_);
}

template<>
Tile::const_iterator::const_iterator_ret::
                      operator std::vector<float> () {
  return tile_->cell_v_float(pos_);
}

template<>
Tile::const_iterator::const_iterator_ret::
                      operator std::vector<double> () {
  return tile_->cell_v_double(pos_);
}

template<>
Tile::const_iterator::const_iterator_ret::operator CSVLine () {
  CSVLine csv_line;

  if(tile_->tile_type() == ATTRIBUTE) {
    if(*tile_->cell_type() == typeid(char))
      csv_line = tile_->cell_char(pos_);
    else if(*tile_->cell_type() == typeid(int))
      csv_line = tile_->cell_int(pos_);
    else if(*tile_->cell_type() == typeid(int64_t))
      csv_line = tile_->cell_int64_t(pos_);
    else if(*tile_->cell_type() == typeid(float))
      csv_line = tile_->cell_float(pos_);
    else if(*tile_->cell_type() == typeid(double))
      csv_line = tile_->cell_double(pos_);
  } else {
    if(*tile_->cell_type() == typeid(int))
      csv_line = tile_->cell_v_int(pos_);
    else if(*tile_->cell_type() == typeid(int64_t))
      csv_line = tile_->cell_v_int64_t(pos_);
    else if(*tile_->cell_type() == typeid(float))
      csv_line = tile_->cell_v_float(pos_);
    else if(*tile_->cell_type() == typeid(double))
      csv_line = tile_->cell_v_double(pos_);
  }

  return csv_line;
}

// -------------- //
// const_iterator //
// -------------- //

Tile::const_iterator::const_iterator() 
    : tile_(NULL), pos_(0) {
}

Tile::const_iterator::const_iterator(const Tile* tile, uint64_t pos) 
    : tile_(tile), pos_(pos) {
}

void Tile::const_iterator::operator=(const const_iterator& rhs) {
  pos_ = rhs.pos_;
  tile_ = rhs.tile_;
}

Tile::const_iterator Tile::const_iterator::operator+(int64_t step) {
  const_iterator it = *this;
  it.pos_ += step;
  return it;
}

void Tile::const_iterator::operator+=(int64_t step) {
  pos_ += step;
}

Tile::const_iterator Tile::const_iterator::operator++() {
  ++pos_;
  return *this;
}

Tile::const_iterator Tile::const_iterator::operator++(int junk) {
  const_iterator it = *this;
  pos_++;
  return it;
}

bool Tile::const_iterator::operator==(const const_iterator& rhs) const {
  // --- If both operands correspond to the same tile
  if(tile_ == rhs.tile_)
    return pos_ == rhs.pos_;

  // --- The operands correspond to different tiles - check their cell values
  // Currently works only with tiles of the same type. If the tiles are
  // coordinate tiles, they must have the same number of dimensions
  assert(tile_->tile_type() == rhs.tile_->tile_type());
  assert(*(tile_->cell_type()) == *(rhs.tile_->cell_type()));
  assert(tile_->tile_type() == ATTRIBUTE || 
         tile_->dim_num() == rhs.tile_->dim_num());

  // For easy reference
  const std::type_info* cell_type = tile_->cell_type();
  TileType tile_type = tile_->tile_type();

  // Attribute tiles
  if(tile_type == ATTRIBUTE) {
    if(*cell_type == typeid(char))
      return static_cast<const AttributeTile<char>*>(tile_)->cell(pos_) ==
             static_cast<const AttributeTile<char>*>(rhs.tile_)->cell(rhs.pos_);
    else if(*cell_type == typeid(int))
      return static_cast<const AttributeTile<int>*>(tile_)->cell(pos_) ==
             static_cast<const AttributeTile<int>*>(rhs.tile_)->cell(rhs.pos_);
    else if(*cell_type == typeid(int64_t))
      return static_cast<const AttributeTile<int64_t>*>(tile_)->cell(pos_) ==
             static_cast<const AttributeTile<int64_t>*>
                 (rhs.tile_)->cell(rhs.pos_);
    else if(*cell_type == typeid(float))
      return static_cast<const AttributeTile<float>*>(tile_)->cell(pos_) ==
             static_cast<const AttributeTile<float>*>
                 (rhs.tile_)->cell(rhs.pos_);
    else if(*cell_type == typeid(double))
      return static_cast<const AttributeTile<double>*>(tile_)->cell(pos_) ==
             static_cast<const AttributeTile<double>*>
                 (rhs.tile_)->cell(rhs.pos_);
  // Coordinate tiles
  } else { // (tile_type == COORDINATE)
    if(*cell_type == typeid(int)) {
      const std::vector<int>& coord_lhs = 
          static_cast<const CoordinateTile<int>*>(tile_)->cell(pos_);
      const std::vector<int>& coord_rhs = 
          static_cast<const CoordinateTile<int>*>(rhs.tile_)->cell(rhs.pos_);
      return std::equal(coord_lhs.begin(), coord_lhs.end(), coord_rhs.begin());
    } else if(*cell_type == typeid(int64_t)) {
      const std::vector<int64_t>& coord_lhs = 
          static_cast<const CoordinateTile<int64_t>*>(tile_)->cell(pos_);
      const std::vector<int64_t>& coord_rhs = 
          static_cast<const CoordinateTile<int64_t>*> 
              (rhs.tile_)->cell(rhs.pos_);
      return std::equal(coord_lhs.begin(), coord_lhs.end(), coord_rhs.begin());
    } else if(*cell_type == typeid(float)) {
      const std::vector<float>& coord_lhs = 
          static_cast<const CoordinateTile<float>*>(tile_)->cell(pos_);
      const std::vector<float>& coord_rhs = 
          static_cast<const CoordinateTile<float>*>(rhs.tile_)->cell(rhs.pos_);
      return std::equal(coord_lhs.begin(), coord_lhs.end(), coord_rhs.begin());
    } else if(*cell_type == typeid(double)) {
      const std::vector<double>& coord_lhs = 
          static_cast<const CoordinateTile<double>*>(tile_)->cell(pos_);
      const std::vector<double>& coord_rhs = 
          static_cast<const CoordinateTile<double>*>(rhs.tile_)->cell(rhs.pos_);
      return std::equal(coord_lhs.begin(), coord_lhs.end(), coord_rhs.begin());
    }
  }
}

bool Tile::const_iterator::operator!=(const const_iterator& rhs) const {
  return !(*this == rhs);
}

void Tile::const_iterator::operator>>(CSVLine& csv_line) const {
  tile_->append_cell_to_csv_line(pos_, csv_line);
}

Tile::const_iterator Tile::begin() const {
  return const_iterator(this, 0);
}

Tile::const_iterator Tile::end() const {
  return const_iterator(this, cell_num_);
}

bool Tile::const_iterator::cell_inside_range(const Tile::Range& range) const {
  return tile_->cell_inside_range(pos_, range); 
}

/******************************************************
******************* PRIVATE METHODS *******************
******************************************************/

template<class T>
bool Tile::append_attribute_value_from_csv_line(CSVLine& csv_line) {
  assert(tile_type_ == ATTRIBUTE);

  T v;
  if(!(csv_line >> v)) 
    return false;

  append_cell(v);

  return true;
} 

template<class T>
void Tile::append_attribute_value_from_cell_it(
    const Tile::const_iterator& cell_it) {
  assert(tile_type_ == ATTRIBUTE);

  T v = *cell_it;
  *this << v;
}

template<class T>
bool Tile::append_coordinates_from_csv_line(CSVLine& csv_line) {
  assert(tile_type_ == COORDINATE);

  unsigned int dim_num = this->dim_num();
  std::vector<T> coordinates;
  coordinates.resize(dim_num);
  T c;
 
  for(unsigned int i=0; i<dim_num; i++) {
    if(!(csv_line >> c)) 
      return false;
    coordinates[i] = c;
  }

  append_cell(coordinates);
 
  return true;
}

template<class T>
void Tile::append_coordinates_from_cell_it(
    const Tile::const_iterator& cell_it) {
  assert(tile_type_ == COORDINATE);

  std::vector<T> coordinates = *cell_it; 
  *this << coordinates;
}

/* ------------- AttributeTile functions ----------- */

/******************************************************
************** CONSTRUCTORS & DESTRUCTORS *************
******************************************************/

template<class T> 
AttributeTile<T>::AttributeTile(uint64_t tile_id) 
    : Tile(ATTRIBUTE, &typeid(T), tile_id, 0, sizeof(T)) {
}

template<class T> 
AttributeTile<T>::AttributeTile(uint64_t tile_id, uint64_t capacity) 
    : Tile(ATTRIBUTE, &typeid(T), tile_id, 0, sizeof(T)) {
  payload_.reserve(capacity);
}

/******************************************************
********************** ACCESSORS **********************
******************************************************/

template<class T> 
const T& AttributeTile<T>::cell(uint64_t i) const {
  assert(i<cell_num_);

  return payload_[i];
}

template<class T> 
void AttributeTile<T>::copy_payload(char* buffer) const {
  memcpy(buffer, &payload_[0], tile_size());
}

/******************************************************
*********************** MUTATORS **********************
******************************************************/

template<class T>
void AttributeTile<T>::set_mbr(const std::vector<double>& mbr) {
  assert(0); // Inapplicable to attribute tiles 
}

template<class T> 
void AttributeTile<T>::set_payload(const char* payload, 
    uint64_t payload_size) {
  assert(payload_size % cell_size_ == 0);

  cell_num_ = payload_size / cell_size_;
  payload_.resize(cell_num_);
  memcpy(&payload_[0], payload, payload_size);
}

/******************************************************
********************** OPERATORS **********************
******************************************************/

template<class T>
template<class T2> 
void AttributeTile<T>::operator<<(const T2& value) {
  payload_.push_back(static_cast<T>(value));	
  cell_num_++;
}

/******************************************************
************************* MISC ************************
******************************************************/

template<class T> 
void AttributeTile<T>::append_cell_to_csv_line(uint64_t pos, 
                                               CSVLine& csv_line) const {
  csv_line << cell(pos);
}

template<class T> 
bool AttributeTile<T>::cell_inside_range(uint64_t pos, 
                                         const Range& range) const {
  assert(0); // Inapplicable to attribute tiles
}

template<class T> 
void AttributeTile<T>::print() const {
  std::cout << "=========== Tile info ==========\n";
  std::cout << "Cell num: " << cell_num_ << "\n";
  std::cout << "Cell size: " << cell_size_ << "\n";
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
  std::cout << "Tile id: " << tile_id_ << "\n";
  std::cout << "Tile type: "
            << (tile_type_ == ATTRIBUTE ? "ATTRIBUTE" : "COORDINATE") << "\n";
  std::cout << "Tile size: " << tile_size() << "\n";
  std::cout << "Payload contents:\n";
  for(uint64_t i=0; i < cell_num_; i++)
    std::cout << "\t" << payload_[i] << "\n";
  std::cout << "========== End of Tile info ========== \n\n";
}

/******************************************************
******************** PRIVATE METHODS ******************
******************************************************/

template<class T> 
void AttributeTile<T>::append_cell(const char& value) { 
  *this << value; 
}

template<class T> 
void AttributeTile<T>::append_cell(const int& value) { 
  *this << value; 
}

template<class T> 
void AttributeTile<T>::append_cell(const int64_t& value) { 
  *this << value; 
}

template<class T> 
void AttributeTile<T>::append_cell(const float& value) { 
  *this << value; 
}

template<class T> 
void AttributeTile<T>::append_cell(const double& value) { 
  *this << value; 
}

template<class T> 
void AttributeTile<T>::append_cell(const std::vector<int>& coordinates) {
  assert(0); // Inapplicable
} 

template<class T> 
void AttributeTile<T>::append_cell(const std::vector<int64_t>& coordinates) {
  assert(0); // Inapplicable
} 

template<class T> 
void AttributeTile<T>::append_cell(const std::vector<float>& coordinates) {
  assert(0); // Inapplicable
} 

template<class T> 
void AttributeTile<T>::append_cell(const std::vector<double>& coordinates) {
  assert(0); // Inapplicable
} 

template<class T> 
std::pair<std::vector<double>, std::vector<double> > 
    AttributeTile<T>::bounding_coordinates() const {
  assert(0); // Inapplicable
}

template<class T> 
char AttributeTile<T>::cell_char(uint64_t pos) const { 
  return static_cast<T>(cell(pos));
}

template<class T> 
int AttributeTile<T>::cell_int(uint64_t pos) const { 
  return static_cast<T>(cell(pos));
}

template<class T> 
int64_t AttributeTile<T>::cell_int64_t(uint64_t pos) const { 
  return static_cast<T>(cell(pos));
}

template<class T> 
float AttributeTile<T>::cell_float(uint64_t pos) const { 
  return static_cast<T>(cell(pos));
}

template<class T> 
double AttributeTile<T>::cell_double(uint64_t pos) const { 
  return static_cast<T>(cell(pos));
}

template<class T> 
std::vector<int> AttributeTile<T>::cell_v_int(uint64_t pos) const { 
  assert(0); // Inapplicable
}

template<class T> 
std::vector<int64_t> AttributeTile<T>::cell_v_int64_t(
    uint64_t pos) const { 
  assert(0); // Inapplicable
}

template<class T> 
std::vector<float> AttributeTile<T>::cell_v_float(
    uint64_t pos) const { 
  assert(0); // Inapplicable
}

template<class T> 
std::vector<double> AttributeTile<T>::cell_v_double(
    uint64_t pos) const { 
  assert(0); // Inapplicable
}

template<class T> 
unsigned int AttributeTile<T>::dim_num() const {
  assert(0); // Inapplicable
}

template<class T> 
std::vector<double>& AttributeTile<T>::mbr() const {
  assert(0); // Inapplicable
}

/* ----------- CoordinateTile functions ------------ */

/******************************************************
********************* CONSTRUCTORS ********************
******************************************************/

template<class T>
CoordinateTile<T>::CoordinateTile(uint64_t tile_id, unsigned int dim_num)  
    : Tile(COORDINATE, &typeid(T), tile_id, 0, sizeof(T) * dim_num) {
  assert(dim_num > 0);

  dim_num_ = dim_num;
  mbr_.resize(2*dim_num);
}

template<class T>
CoordinateTile<T>::CoordinateTile(uint64_t tile_id, 
                                  unsigned int dim_num,
                                  uint64_t capacity)   
    : Tile(COORDINATE, &typeid(T), tile_id, 0, sizeof(T) * dim_num) {
  assert(dim_num > 0);

  dim_num_ = dim_num;
  mbr_.resize(2*dim_num);
  payload_.reserve(capacity);
}

/******************************************************
********************** ACCESSORS **********************
******************************************************/

template<class T> 
std::pair<std::vector<double>, std::vector<double> > 
    CoordinateTile<T>::bounding_coordinates() const {
  std::vector<double> lower(payload_.front().begin(), payload_.front().end());
  std::vector<double> upper(payload_.back().begin(), payload_.back().end()); 
  return std::pair<std::vector<double>, std::vector<double> >(lower, upper);
}

template<class T> 
const std::vector<T>& CoordinateTile<T>::cell(uint64_t i) const {
  assert(i<cell_num_);

  return payload_[i];
}

template<class T> 
void CoordinateTile<T>::copy_payload(char* buffer) const {
  uint64_t offset = 0;
  for(uint64_t i=0; i<cell_num_; i++) {
    memcpy(buffer + offset, &payload_[i][0], cell_size_);
    offset += cell_size_;
  }
}

/******************************************************
*********************** MUTATORS **********************
******************************************************/

template<class T> 
void CoordinateTile<T>::set_mbr(const std::vector<double>& mbr) {
  assert(mbr.size() == 2*dim_num_);
  mbr_ = mbr;
}

template<class T> 
void CoordinateTile<T>::set_payload(const char* payload, 
    uint64_t payload_size) {
  assert(payload_size % cell_size_ == 0);

  cell_num_ = payload_size / cell_size_;
  payload_.resize(cell_num_);
  uint64_t offset = 0;

  for(uint64_t i=0; i<cell_num_; i++) {
    payload_[i].resize(dim_num_);
    memcpy(&payload_[i][0], payload + offset, cell_size_);
    offset += cell_size_;
  }
}

/******************************************************
********************** OPERATORS **********************
******************************************************/

template<class T>
template<class T2> 
void CoordinateTile<T>::operator<<(const std::vector<T2>& coordinates) {
  assert(coordinates.size() == dim_num_);

  std::vector<T> cast_coordinates(coordinates.begin(), coordinates.end());
  update_mbr(cast_coordinates);
  payload_.push_back(cast_coordinates);	
  cell_num_++;
}

/******************************************************
************************* MISC ************************
******************************************************/

template<class T> 
void CoordinateTile<T>::append_cell_to_csv_line(uint64_t pos, 
                                                CSVLine& csv_line) const {
  csv_line << cell(pos);
}

template<class T> 
bool CoordinateTile<T>::cell_inside_range(uint64_t pos, const Range& range) const {
  assert(range.size() == 2*dim_num_);

  for(unsigned int i=0; i<dim_num_; i++)
    if(payload_[pos][i] < range[2*i] || payload_[pos][i] > range[2*i+1])
      return false;
  return true;
}

template<class T> 
void CoordinateTile<T>::print() const {
  std::cout << "=========== Tile info ==========\n";
  std::cout << "Cell num: " << cell_num_ << "\n";
  std::cout << "Cell size: " << cell_size_ << "\n";
  std::cout << "Cell type: ";
  if(*cell_type_ == typeid(int))
    std::cout << "int\n";
  else if(*cell_type_ == typeid(int64_t))
    std::cout << "int64_t\n";
  else if(*cell_type_ == typeid(float))
    std::cout << "float\n";
  else if(*cell_type_ == typeid(double))
    std::cout << "double\n";

  std::cout << "Tile id: " << tile_id_ << "\n";
  std::cout << "Tile type: "
            << (tile_type_ == ATTRIBUTE ? "ATTRIBUTE" : "COORDINATE") << "\n";
  std::cout << "Tile size: " << tile_size() << "\n";
  std:: cout << "Dim num: " << dim_num_ << "\n";
  std::cout << "MBR: \n";
  if(cell_num_ != 0) {
    for(unsigned int i=0; i<dim_num_; i++)
      std::cout << "\t dim " << i << ": [" << mbr_[2*i] << "," << mbr_[2*i+1] 
                << "]\n";
  }
  std::cout << "Bounding coordinates: \n";
  if(cell_num_ != 0) {
    std::cout << "\t";
    for(unsigned int j=0; j<dim_num_; j++)
      std::cout << payload_[0][j] << "\t";
    std::cout << "\n\t";
    for(unsigned int j=0; j<dim_num_; j++)
      std::cout << payload_[cell_num_-1][j] << "\t";
    std::cout << "\n";
  }
  std::cout << "Payload contents:\n";
  for(uint64_t i=0; i < cell_num_; i++) {
    std::cout << "\t";
    for(unsigned int j=0; j<dim_num_; j++)
      std::cout << payload_[i][j] << "\t";
    std::cout << "\n";
  }
  std::cout << "========== End of Tile info ========== \n\n";
}

/******************************************************
****************** PRIVATE METHODS ********************
******************************************************/

template<class T> 
void CoordinateTile<T>::append_cell(const char& value) {
  assert(0); // Inapplicable
}

template<class T> 
void CoordinateTile<T>::append_cell(const int& value) {
  assert(0); // Inapplicable
}

template<class T> 
void CoordinateTile<T>::append_cell(const int64_t& value) {
  assert(0); // Inapplicable
}

template<class T> 
void CoordinateTile<T>::append_cell(const float& value) {
  assert(0); // Inapplicable
}

template<class T> 
void CoordinateTile<T>::append_cell(const double& value) {
  assert(0); // Inapplicable
}

template<class T> 
void CoordinateTile<T>::append_cell(const std::vector<int>& coordinates) {
  *this << coordinates; 
}

template<class T> 
void CoordinateTile<T>::append_cell(const std::vector<int64_t>& coordinates) {
  *this << coordinates; 
}

template<class T> 
void CoordinateTile<T>::append_cell(const std::vector<float>& coordinates) {
  *this << coordinates; 
}

template<class T> 
void CoordinateTile<T>::append_cell(const std::vector<double>& coordinates) {
  *this << coordinates; 
}

template<class T> 
char CoordinateTile<T>::cell_char(uint64_t pos) const { 
  assert(0); // Inapplicable
}

template<class T> 
int CoordinateTile<T>::cell_int(uint64_t pos) const { 
  assert(0); // inapplicable
}

template<class T> 
int64_t CoordinateTile<T>::cell_int64_t(uint64_t pos) const { 
  assert(0); // Inapplicable
}

template<class T> 
float CoordinateTile<T>::cell_float(uint64_t pos) const { 
  assert(0); // Inapplicable
}

template<class T> 
double CoordinateTile<T>::cell_double(uint64_t pos) const { 
  assert(0); // Type mismatch
}

template<class T> 
std::vector<int> CoordinateTile<T>::cell_v_int(
    uint64_t pos) const {
  std::vector<int> coord(cell(pos).begin(), cell(pos).end()); 
  return coord;
}

template<class T> 
std::vector<int64_t> CoordinateTile<T>::cell_v_int64_t(
    uint64_t pos) const {
  std::vector<int64_t> coord(cell(pos).begin(), cell(pos).end()); 
  return coord;
}

template<class T> 
std::vector<float> CoordinateTile<T>::cell_v_float(
    uint64_t pos) const {
  std::vector<float> coord(cell(pos).begin(), cell(pos).end()); 
  return coord;
}

template<class T> 
std::vector<double> CoordinateTile<T>::cell_v_double(
    uint64_t pos) const {
  std::vector<double> coord(cell(pos).begin(), cell(pos).end()); 
  return coord;
}

template<class T> 
void CoordinateTile<T>::update_mbr(const std::vector<T>& coordinates) {
  if(cell_num_ == 0) {
    for(unsigned int i=0; i<dim_num_; i++) {
      mbr_[2*i] = coordinates[i]; 
      mbr_[2*i+1] = coordinates[i]; 
    }
  } else {
    for(unsigned int i=0; i<dim_num_; i++) {
      // Update lower bound on dimension i
      if(mbr_[2*i] > coordinates[i])
        mbr_[2*i] = coordinates[i];

      // Update upper bound on dimension i
      if(mbr_[2*i+1] < coordinates[i])
        mbr_[2*i+1] = coordinates[i];   
    }	
  }
}

// Explicit template instantiations
template class AttributeTile<char>;
template class AttributeTile<int>;
template class AttributeTile<int64_t>;
template class AttributeTile<float>;
template class AttributeTile<double>;
template class CoordinateTile<int>;
template class CoordinateTile<int64_t>;
template class CoordinateTile<float>;
template class CoordinateTile<double>;
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
template void Tile::operator<< <std::vector<int> >
    (const std::vector<int>& value);
template void Tile::operator<< <std::vector<int64_t> >
    (const std::vector<int64_t>& value);
template void Tile::operator<< <std::vector<float> >
    (const std::vector<float>& value);
template void Tile::operator<< <std::vector<double> >
    (const std::vector<double>& value);
template bool Tile::append_attribute_value_from_csv_line <char>
    (CSVLine& csv_line);
template bool Tile::append_attribute_value_from_csv_line <int>
    (CSVLine& csv_line);
template bool Tile::append_attribute_value_from_csv_line <int64_t>
    (CSVLine& csv_line);
template bool Tile::append_attribute_value_from_csv_line <float>
    (CSVLine& csv_line);
template bool Tile::append_attribute_value_from_csv_line <double>
    (CSVLine& csv_line);
template bool Tile::append_coordinates_from_csv_line <int>
    (CSVLine& csv_line);
template bool Tile::append_coordinates_from_csv_line <int64_t>
    (CSVLine& csv_line);
template bool Tile::append_coordinates_from_csv_line <float>
    (CSVLine& csv_line);
template bool Tile::append_coordinates_from_csv_line <double>
    (CSVLine& csv_line);
template void AttributeTile<int>::operator<< <char>
    (const char& value);
template void AttributeTile<int>::operator<< <int>
    (const int& value);
template void AttributeTile<int>::operator<< <int64_t>
    (const int64_t& value);
template void AttributeTile<int>::operator<< <float>
    (const float& value);
template void AttributeTile<int>::operator<< <double>
    (const double& value);
template void AttributeTile<int64_t>::operator<< <char>
    (const char& value);
template void AttributeTile<int64_t>::operator<< <int>
    (const int& value);
template void AttributeTile<int64_t>::operator<< <int64_t>
    (const int64_t& value);
template void AttributeTile<int64_t>::operator<< <float>
    (const float& value);
template void AttributeTile<int64_t>::operator<< <double>
    (const double& value);
template void AttributeTile<float>::operator<< <char>
    (const char& value);
template void AttributeTile<float>::operator<< <int>
    (const int& value);
template void AttributeTile<float>::operator<< <int64_t>
    (const int64_t& value);
template void AttributeTile<float>::operator<< <float>
    (const float& value);
template void AttributeTile<float>::operator<< <double>
    (const double& value);
template void AttributeTile<double>::operator<< <char>
    (const char& value);
template void AttributeTile<double>::operator<< <int>
    (const int& value);
template void AttributeTile<double>::operator<< <int64_t>
    (const int64_t& value);
template void AttributeTile<double>::operator<< <float>
    (const float& value);
template void AttributeTile<double>::operator<< <double>
    (const double& value);
template void CoordinateTile<int>::operator<< <int>
    (const std::vector<int>& coordinates);
template void CoordinateTile<int>::operator<< <int64_t>
    (const std::vector<int64_t>& coordinates);
template void CoordinateTile<int>::operator<< <float>
    (const std::vector<float>& coordinates);
template void CoordinateTile<int>::operator<< <double>
    (const std::vector<double>& coordinates);
template void CoordinateTile<int64_t>::operator<< <int>
    (const std::vector<int>& coordinates);
template void CoordinateTile<int64_t>::operator<< <int64_t>
    (const std::vector<int64_t>& coordinates);
template void CoordinateTile<int64_t>::operator<< <float>
    (const std::vector<float>& coordinates);
template void CoordinateTile<int64_t>::operator<< <double>
    (const std::vector<double>& coordinates);
template void CoordinateTile<float>::operator<< <int>
    (const std::vector<int>& coordinates);
template void CoordinateTile<float>::operator<< <int64_t>
    (const std::vector<int64_t>& coordinates);
template void CoordinateTile<float>::operator<< <float>
    (const std::vector<float>& coordinates);
template void CoordinateTile<float>::operator<< <double>
    (const std::vector<double>& coordinates);
template void CoordinateTile<double>::operator<< <int>
    (const std::vector<int>& coordinates);
template void CoordinateTile<double>::operator<< <int64_t>
    (const std::vector<int64_t>& coordinates);
template void CoordinateTile<double>::operator<< <float>
    (const std::vector<float>& coordinates);
template void CoordinateTile<double>::operator<< <double>
    (const std::vector<double>& coordinates);
