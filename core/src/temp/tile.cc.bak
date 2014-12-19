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
 * This file implements the AttributeTile and CoordinateTile classes.
 */

#include "tile.h"
#include <iostream>
#include <stdio.h>
#include <string.h>
#include <typeinfo>

/* ----------------- Tile functions ---------------- */

/******************************************************
********************** OPERATORS **********************
******************************************************/

template<class T2> 
void Tile::operator<<(const T2& value) {
  append_cell(value);
}

/******************************************************
******************** CELL ITERATORS *******************
******************************************************/

template<>
Tile::const_iterator::const_iterator_ret::operator int () {
  return tile_->get_cell_int(pos_);
}

template<>
Tile::const_iterator::const_iterator_ret::operator int64_t () {
  return tile_->get_cell_int64_t(pos_);
}

template<>
Tile::const_iterator::const_iterator_ret::operator float () {
  return tile_->get_cell_float(pos_);
}

template<>
Tile::const_iterator::const_iterator_ret::operator double () {
  return tile_->get_cell_double(pos_);
}

template<>
Tile::const_iterator::const_iterator_ret::
                      operator std::vector<int> () {
  return tile_->get_cell_v_int(pos_);
}

template<>
Tile::const_iterator::const_iterator_ret::
                      operator std::vector<int64_t> () {
  return tile_->get_cell_v_int64_t(pos_);
}

template<>
Tile::const_iterator::const_iterator_ret::
                      operator std::vector<float> () {
  return tile_->get_cell_v_float(pos_);
}

template<>
Tile::const_iterator::const_iterator_ret::
                      operator std::vector<double> () {
  return tile_->get_cell_v_double(pos_);
}

Tile::const_iterator::const_iterator() 
    : tile_(NULL), pos_(0) {
}

void Tile::const_iterator::operator=(const const_iterator& rhs) {
  pos_ = rhs.pos_;
  tile_ = rhs.tile_;
}

Tile::const_iterator::const_iterator(const Tile* tile, uint64_t pos) 
    : tile_(tile), pos_(pos) {
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
  return (pos_ == rhs.pos_);
}

bool Tile::const_iterator::operator!=(const const_iterator& rhs) const {
  return (pos_ != rhs.pos_);
}

Tile::const_iterator Tile::begin() const {
  return const_iterator(this, 0);
}

Tile::const_iterator Tile::end() const {
  return const_iterator(this, cell_num_);
}

/* ------------- AttributeTile functions ----------- */

/******************************************************
************** CONSTRUCTORS & DESTRUCTORS *************
******************************************************/

template<class T> 
AttributeTile<T>::AttributeTile(uint64_t tile_id) {
  tile_id_ = tile_id;
  cell_num_ = 0;
  cell_size_ = sizeof(T);
}

/******************************************************
********************** ACCESSORS **********************
******************************************************/

template<class T> 
std::pair<std::vector<double>, std::vector<double> > 
    AttributeTile<T>::bounding_coordinates() const {
  throw TileException("Attribute tiles do not have bounding coordinates", 
                      tile_id_);
}

template<class T> 
const T& AttributeTile<T>::cell(uint64_t i) const {
  if(i < 0 || i >= cell_num_)
    throw TileException("Cannot get cell, cell index out of bounds.", tile_id_);

  return payload_[i];
}

template<class T> 
void AttributeTile<T>::copy_payload(char* buffer) const {
  memcpy(buffer, &payload_[0], tile_size());
}

template<class T> 
unsigned int AttributeTile<T>::dim_num() const {
  throw TileException("Attribute tiles do not have dimensions.", tile_id_);
}

template<class T> 
std::vector<double>& AttributeTile<T>::mbr() const {
  throw TileException("Attribute tiles do not store MBRs.", tile_id_);
}

/******************************************************
*********************** MUTATORS **********************
******************************************************/

template<class T> 
void AttributeTile<T>::set_payload(const std::vector<T>& payload) {
  payload_ = payload;
  cell_num_ = payload.size();
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
void AttributeTile<T>::print() const {
  std::cout << "\n==== Tile info ====\n\n";
  std:: cout << "This is an attribute tile \n";	
  std:: cout << "Tile id: " << tile_id_ << "\n";
  std:: cout << "Cell num: " << cell_num_ << "\n";
  std:: cout << "Cell size: " << cell_size_ << "\n";
	
  std::cout << "\nPayload elements num: " << payload_.size()  << "\n";
  if(payload_.size() != 0) {
    std::cout << "Payload contents:\n";
    for(uint64_t i=0; i < payload_.size(); i++)
      std::cout << "\t" << payload_[i] << "\n";
  }				

  std::cout << "\n==== End of tile info ==== \n\n";
}

/******************************************************
******************** PRIVATE METHODS ******************
******************************************************/

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
  throw TileException("Cannot append coordinates to an "
                      "attribute tile.", tile_id_); 
} 

template<class T> 
void AttributeTile<T>::append_cell(const std::vector<int64_t>& coordinates) {
  throw TileException("Cannot append coordinates to an "
                      "attribute tile.", tile_id_); 
} 

template<class T> 
void AttributeTile<T>::append_cell(const std::vector<float>& coordinates) {
  throw TileException("Cannot append coordinates to an "
                      "attribute tile.", tile_id_); 
} 

template<class T> 
void AttributeTile<T>::append_cell(const std::vector<double>& coordinates) {
  throw TileException("Cannot append coordinates to an "
                      "attribute tile.", tile_id_); 
} 

template<> 
const int& AttributeTile<int>::get_cell_int(uint64_t pos) const { 
  return cell(pos);
}

template<> 
const int& AttributeTile<int64_t>::get_cell_int(uint64_t pos) const { 
  throw TileException("Cannot get cell: type mismatch.", tile_id_);
}

template<> 
const int& AttributeTile<float>::get_cell_int(uint64_t pos) const { 
  throw TileException("Cannot get cell: type mismatch.", tile_id_);
}

template<> 
const int& AttributeTile<double>::get_cell_int(uint64_t pos) const { 
  throw TileException("Cannot get cell: type mismatch.", tile_id_);
}

template<> 
const int64_t& AttributeTile<int>::get_cell_int64_t(uint64_t pos) const { 
  throw TileException("Cannot get cell: type mismatch.", tile_id_);
}

template<> 
const int64_t& AttributeTile<int64_t>::get_cell_int64_t(uint64_t pos) const { 
  return cell(pos);
}

template<> 
const int64_t& AttributeTile<float>::get_cell_int64_t(uint64_t pos) const { 
  throw TileException("Cannot get cell: type mismatch.", tile_id_);
}

template<> 
const int64_t& AttributeTile<double>::get_cell_int64_t(uint64_t pos) const { 
  throw TileException("Cannot get cell: type mismatch.", tile_id_);
}

template<> 
const float& AttributeTile<int>::get_cell_float(uint64_t pos) const { 
  throw TileException("Cannot get cell: type mismatch.", tile_id_);
}

template<> 
const float& AttributeTile<int64_t>::get_cell_float(uint64_t pos) const { 
  throw TileException("Cannot get cell: type mismatch.", tile_id_);
}

template<> 
const float& AttributeTile<float>::get_cell_float(uint64_t pos) const { 
  return cell(pos);
}

template<> 
const float& AttributeTile<double>::get_cell_float(uint64_t pos) const { 
  throw TileException("Cannot get cell: type mismatch.", tile_id_);
}

template<> 
const double& AttributeTile<int>::get_cell_double(uint64_t pos) const { 
  throw TileException("Cannot get cell: type mismatch.", tile_id_);
}

template<> 
const double& AttributeTile<int64_t>::get_cell_double(uint64_t pos) const { 
  throw TileException("Cannot get cell: type mismatch.", tile_id_);
}

template<> 
const double& AttributeTile<float>::get_cell_double(uint64_t pos) const { 
  throw TileException("Cannot get cell: type mismatch.", tile_id_);
}

template<> 
const double& AttributeTile<double>::get_cell_double(uint64_t pos) const { 
  return cell(pos);
}

template<class T> 
const std::vector<int>& AttributeTile<T>::get_cell_v_int(uint64_t pos) const { 
  throw TileException("Cannot get coordinates from an "
                      "attribute tile.", tile_id_); 
}

template<class T> 
const std::vector<int64_t>& AttributeTile<T>::get_cell_v_int64_t(
    uint64_t pos) const { 
  throw TileException("Cannot get coordinates from an "
                      "attribute tile.", tile_id_); 
}

template<class T> 
const std::vector<float>& AttributeTile<T>::get_cell_v_float(
    uint64_t pos) const { 
  throw TileException("Cannot get coordinates from an "
                      "attribute tile.", tile_id_); 
}

template<class T> 
const std::vector<double>& AttributeTile<T>::get_cell_v_double(
    uint64_t pos) const { 
  throw TileException("Cannot get coordinates from an "
                      "attribute tile.", tile_id_); 
}

/* ----------- CoordinateTile functions ------------ */

/******************************************************
********************* CONSTRUCTORS ********************
******************************************************/

template<class T>
CoordinateTile<T>::CoordinateTile(uint64_t tile_id, unsigned int dim_num) {
  tile_id_ = tile_id;
  
  if(dim_num == 0)
    throw TileException("The number of dimensions must not be zero.", tile_id_);
  
  dim_num_ = dim_num;
  cell_num_ = 0;
  cell_size_ = sizeof(T)*dim_num_;
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
  if(i < 0 || i >= cell_num_)
    throw TileException("Cannot get cell, cell index out of bounds.", tile_id_);

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
void CoordinateTile<T>::set_payload(
    const std::vector<std::vector<T> >& payload) {
  // Check if each coordinate cell has the right number of values
  for(uint64_t i=0; i<payload.size(); i++) {
    if(payload[i].size() != dim_num_)
      throw TileException(
          "Cannot set payload: the input coordinates do not agree "
          "with the number of dimensions.", tile_id_);
  }

  payload_ = payload;
  cell_num_ = payload.size();
}

/******************************************************
********************** OPERATORS **********************
******************************************************/

template<class T>
template<class T2> 
void CoordinateTile<T>::operator<<(const std::vector<T2>& coordinates) {
  if(coordinates.size() != dim_num_)
    throw TileException(
        "Cannot append coordinates: the input coordinates do not agree "
        "with the tile dimensions.", tile_id_);

  std::vector<T> cast_coordinates(coordinates.begin(), coordinates.end());

  update_mbr(cast_coordinates);
  payload_.push_back(cast_coordinates);	
  cell_num_++;
}

/******************************************************
************************* MISC ************************
******************************************************/

template<class T> 
void CoordinateTile<T>::print() const {
  std::cout << "\n==== Tile info ====\n\n";
  std:: cout << "This is a coordinate tile \n";	
  std:: cout << "Tile id: " << tile_id_ << "\n";
  std:: cout << "Cell num: " << cell_num_ << "\n";
  std:: cout << "Cell size: " << cell_size_ << "\n";
  std:: cout << "Dim num: " << dim_num_ << "\n";	
	
  std::cout << "\nPayload elements num: " << payload_.size()  << "\n";
  if(payload_.size() != 0) {
    std::cout << "Payload contents:\n";
    for(uint64_t i=0; i < payload_.size(); i++) {
        std::cout << "\t";
        for(unsigned int j=0; j<dim_num_; j++)
          std::cout << payload_[i][j] << "\t";
        std::cout << "\n";
    }
  }

  std::cout << "\nMBR: \n";
  if(mbr_.size() == 2*dim_num_) {
    for(unsigned int i=0; i<dim_num_; i++)
      std::cout << "\t dim " << i << ": [" << mbr_[2*i] << "," << mbr_[2*i+1] 
                << "]\n";
  } else {
    std::cout << "\t Warning: MBR not set properly!\n";
  }

  std::cout << "\n==== End of tile info ==== \n\n";
}

/******************************************************
****************** PRIVATE METHODS ********************
******************************************************/

template<class T> 
void CoordinateTile<T>::append_cell(const int& value) {
  throw TileException("Cannot append a cell value to a "
                      "coordinate tile (coordinates needed).", tile_id_); 
}

template<class T> 
void CoordinateTile<T>::append_cell(const int64_t& value) {
  throw TileException("Cannot append a cell value to a "
                      "coordinate tile (coordinates needed).", tile_id_); 
}

template<class T> 
void CoordinateTile<T>::append_cell(const float& value) {
  throw TileException("Cannot append a cell value to a "
                      "coordinate tile (coordinates needed).", tile_id_); 
}

template<class T> 
void CoordinateTile<T>::append_cell(const double& value) {
  throw TileException("Cannot append a cell value to a "
                      "coordinate tile (coordinates needed).", tile_id_); 
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
const int& CoordinateTile<T>::get_cell_int(uint64_t pos) const { 
  throw TileException("Cannot get a cell value from a "
                      "coordinate tile.", tile_id_); 
}

template<class T> 
const int64_t& CoordinateTile<T>::get_cell_int64_t(uint64_t pos) const { 
  throw TileException("Cannot get a cell value from a "
                      "coordinate tile.", tile_id_); 
}

template<class T> 
const float& CoordinateTile<T>::get_cell_float(uint64_t pos) const { 
  throw TileException("Cannot get a cell value from a "
                      "coordinate tile.", tile_id_); 
}

template<class T> 
const double& CoordinateTile<T>::get_cell_double(uint64_t pos) const { 
  throw TileException("Cannot get a cell value from a "
                      "coordinate tile.", tile_id_); 
}

template<> 
const std::vector<int>& CoordinateTile<int>::get_cell_v_int(
    uint64_t pos) const { 
  return cell(pos);
}

template<> 
const std::vector<int>& CoordinateTile<int64_t>::get_cell_v_int(
    uint64_t pos) const { 
  throw TileException("Cannot get coordinates: type mismatch.", tile_id_);
}

template<> 
const std::vector<int>& CoordinateTile<float>::get_cell_v_int(
    uint64_t pos) const { 
  throw TileException("Cannot get coordinates: type mismatch.", tile_id_);
}

template<> 
const std::vector<int>& CoordinateTile<double>::get_cell_v_int(
    uint64_t pos) const { 
  throw TileException("Cannot get coordinates: type mismatch.", tile_id_);
}

template<> 
const std::vector<int64_t>& CoordinateTile<int>::get_cell_v_int64_t(
    uint64_t pos) const { 
  throw TileException("Cannot get coordinates: type mismatch.", tile_id_);
}

template<> 
const std::vector<int64_t>& CoordinateTile<int64_t>::get_cell_v_int64_t(
    uint64_t pos) const { 
  return cell(pos);
}

template<> 
const std::vector<int64_t>& CoordinateTile<float>::get_cell_v_int64_t(
    uint64_t pos) const { 
  throw TileException("Cannot get coordinates: type mismatch.", tile_id_);
}

template<> 
const std::vector<int64_t>& CoordinateTile<double>::get_cell_v_int64_t(
    uint64_t pos) const { 
  throw TileException("Cannot get coordinates: type mismatch.", tile_id_);
}

template<> 
const std::vector<float>& CoordinateTile<int>::get_cell_v_float(
    uint64_t pos) const { 
  throw TileException("Cannot get coordinates: type mismatch.", tile_id_);
}

template<> 
const std::vector<float>& CoordinateTile<int64_t>::get_cell_v_float(
    uint64_t pos) const { 
  throw TileException("Cannot get coordinates: type mismatch.", tile_id_);
}

template<> 
const std::vector<float>& CoordinateTile<float>::get_cell_v_float(
    uint64_t pos) const { 
  return cell(pos);
}

template<> 
const std::vector<float>& CoordinateTile<double>::get_cell_v_float(
    uint64_t pos) const { 
  throw TileException("Cannot get coordinates: type mismatch.", tile_id_);
}

template<> 
const std::vector<double>& CoordinateTile<int>::get_cell_v_double(
    uint64_t pos) const { 
  throw TileException("Cannot get coordinates: type mismatch.", tile_id_);
}

template<> 
const std::vector<double>& CoordinateTile<int64_t>::get_cell_v_double(
    uint64_t pos) const { 
  throw TileException("Cannot get coordinates: type mismatch.", tile_id_);
}

template<> 
const std::vector<double>& CoordinateTile<float>::get_cell_v_double(
    uint64_t pos) const { 
  throw TileException("Cannot get coordinates: type mismatch.", tile_id_);
}

template<> 
const std::vector<double>& CoordinateTile<double>::get_cell_v_double(
    uint64_t pos) const { 
  return cell(pos);
}

template<class T> 
void CoordinateTile<T>::update_mbr(const std::vector<T>& coordinates) {
  if(cell_num_ == 0) {
    for(unsigned int i=0; i<dim_num_; i++) {
      mbr_.push_back(coordinates[i]);
      mbr_.push_back(coordinates[i]);
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
template class AttributeTile<int>;
template class AttributeTile<int64_t>;
template class AttributeTile<float>;
template class AttributeTile<double>;
template class CoordinateTile<int>;
template class CoordinateTile<int64_t>;
template class CoordinateTile<float>;
template class CoordinateTile<double>;
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
template void AttributeTile<int>::operator<< <int>
    (const int& value);
template void AttributeTile<int>::operator<< <int64_t>
    (const int64_t& value);
template void AttributeTile<int>::operator<< <float>
    (const float& value);
template void AttributeTile<int>::operator<< <double>
    (const double& value);
template void AttributeTile<int64_t>::operator<< <int>
    (const int& value);
template void AttributeTile<int64_t>::operator<< <int64_t>
    (const int64_t& value);
template void AttributeTile<int64_t>::operator<< <float>
    (const float& value);
template void AttributeTile<int64_t>::operator<< <double>
    (const double& value);
template void AttributeTile<float>::operator<< <int>
    (const int& value);
template void AttributeTile<float>::operator<< <int64_t>
    (const int64_t& value);
template void AttributeTile<float>::operator<< <float>
    (const float& value);
template void AttributeTile<float>::operator<< <double>
    (const double& value);
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
