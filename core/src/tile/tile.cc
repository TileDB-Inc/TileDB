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

Tile::Tile(int64_t tile_id, int dim_num, 
           const std::type_info* cell_type, int val_num) 
    : tile_id_(tile_id), dim_num_(dim_num), cell_type_(cell_type), 
      val_num_(val_num) {
  assert(dim_num >= 0);

  if(dim_num == 0) 
    tile_type_ = ATTRIBUTE;
  else
    tile_type_ = COORDINATE;

  mbr_ = NULL;
  cell_num_ = 0;
  tile_size_ = 0;
  payload_ = NULL;

  // Set type size
  if(*cell_type == typeid(char)) {
    assert(tile_type_ == ATTRIBUTE); // Char not supported for coordinate cells 
    type_size_ = sizeof(char);
  } else if(*cell_type == typeid(int)) {
    type_size_ = sizeof(int);
  } else if(*cell_type == typeid(int64_t)) {
    type_size_ = sizeof(int64_t);
  } else if(*cell_type == typeid(float)) {
    type_size_ = sizeof(float);
  } else if(*cell_type == typeid(double)) {
    type_size_ = sizeof(double);
  } else {
    assert(0); // No other cell type is supported
  }

  if(val_num != VAR_SIZE) {
    cell_size_ = val_num_ * type_size_;
    if(tile_type_ == COORDINATE)
      cell_size_ *= dim_num_;
  } else {
    cell_size_ = VAR_SIZE;
  }
}

Tile::~Tile() {
  clear();
}

/******************************************************
********************* ACCESSORS ***********************
******************************************************/

TileConstCellIterator Tile::begin() const {
  return TileConstCellIterator(this, 0);
}

Tile::BoundingCoordinatesPair Tile::bounding_coordinates() const {
  // Attribute tile or empty tile
  if(tile_type_ == ATTRIBUTE || cell_num_ == 0)
    return BoundingCoordinatesPair((void *)NULL, (void *)NULL);

  // Coordinate tile
  char* lower = static_cast<char*>(payload_);
  char* upper = lower + (cell_num_-1)*cell_size_;

  return BoundingCoordinatesPair(lower, upper);
}

const void* Tile::cell(int64_t pos) const {
  // Fixed-sized cells
  if(val_num_ != VAR_SIZE) 
    return  static_cast<char*>(payload_) + pos*cell_size_; 
  else 
    return static_cast<char*>(payload_) + offsets_[pos];
}

size_t Tile::cell_size() const {
  assert(val_num_ != VAR_SIZE);

  return cell_size_;
}

int64_t Tile::cell_num() const {
  return cell_num_;
}

const std::type_info* Tile::cell_type() const {
  return cell_type_;
}

void Tile::copy_payload(void* buffer) const {
  memcpy(buffer, payload_, tile_size_);
}

int Tile::dim_num() const {
  return dim_num_;
}

TileConstCellIterator Tile::end() {
  return TileConstCellIterator();
}

bool Tile::is_del(int64_t pos) const {
  // Applies only to attribute values
  assert(tile_type_ == ATTRIBUTE);

  const void* cell;
  if(val_num_ != VAR_SIZE)
    cell = this->cell(pos);
  else
    cell = static_cast<const char*>(this->cell(pos)) + sizeof(int);

  if(cell_type_ == &typeid(char)) { 
    const char* v = static_cast<const char*>(cell);
    return (*v  == DEL_CHAR);
  } else if(cell_type_ == &typeid(int)) {
    const int* v = static_cast<const int*>(cell);
    return (*v  == DEL_INT);
  } else if(cell_type_ == &typeid(int64_t)) {
    const int64_t* v = static_cast<const int64_t*>(cell);
    return (*v  == DEL_INT64_T);
  } else if(cell_type_ == &typeid(float)) {
    const float* v = static_cast<const float*>(cell);
    return (*v  == DEL_FLOAT);
  } else if(cell_type_ == &typeid(double)) {
    const double* v = static_cast<const double*>(cell);
    return (*v  == DEL_DOUBLE);
  }
}

bool Tile::is_null(int64_t pos) const {
  // Applies only to attribute values
  assert(tile_type_ == ATTRIBUTE);

  const void* cell;
  if(val_num_ != VAR_SIZE)
    cell = this->cell(pos);
  else
    cell = static_cast<const char*>(this->cell(pos)) + sizeof(int);

  if(cell_type_ == &typeid(char)) { 
    const char* v = static_cast<const char*>(cell);
    return (*v  == NULL_CHAR);
  } else if(cell_type_ == &typeid(int)) {
    const int* v = static_cast<const int*>(cell);
    return (*v  == NULL_INT);
  } else if(cell_type_ == &typeid(int64_t)) {
    const int64_t* v = static_cast<const int64_t*>(cell);
    return (*v  == NULL_INT64_T);
  } else if(cell_type_ == &typeid(float)) {
    const float* v = static_cast<const float*>(cell);
    return (*v  == NULL_FLOAT);
  } else if(cell_type_ == &typeid(double)) {
    const double* v = static_cast<const double*>(cell);
    return (*v  == NULL_DOUBLE);
  }
}

Tile::MBR Tile::mbr() const {
  return mbr_;
}

int64_t Tile::tile_id() const {
  return tile_id_;
}

size_t Tile::tile_size() const {
  return tile_size_;
}

Tile::TileType Tile::tile_type() const {
  return tile_type_;
}

size_t Tile::type_size() const {
  return type_size_;
}

bool Tile::var_size() const {
  return val_num_ == VAR_SIZE;
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

  size_t mbr_size = 2*cell_size_;

  mbr_ = malloc(mbr_size); 
  memcpy(mbr_, mbr, mbr_size);
}

void Tile::set_payload(void* payload, size_t payload_size) {
  clear_payload();
  payload_ = payload;
  tile_size_ = payload_size;

  // Fixed-sized cells
  if(val_num_ != VAR_SIZE) {
    assert(payload_size % cell_size_ == 0);
    cell_num_ = payload_size / cell_size_;
  } else { // Variable-sized cells 
    size_t offset = 0;
    int val_num;
    char* c_payload = static_cast<char*>(payload);

    while(offset < payload_size) {
      offsets_.push_back(offset); 
      assert(offset + sizeof(int) < payload_size);
      memcpy(&val_num, c_payload + offset, sizeof(int)); 
      offset += sizeof(int) + val_num * type_size_;
    }
    assert(offset == payload_size);

    cell_num_ = offsets_.size();
  }
}

/******************************************************
************************ MISC *************************
******************************************************/

TileConstReverseCellIterator Tile::rbegin() const {
  return TileConstReverseCellIterator(this, cell_num()-1);
}

TileConstReverseCellIterator Tile::rend() {
  return TileConstReverseCellIterator();
}

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
  if(*cell_type_ == typeid(char)) {
    std::cout << "char[";
    if(val_num_ == VAR_SIZE)
      std::cout << "var]\n";
    else 
      std::cout << val_num_ << "]\n";
  } else if(*cell_type_ == typeid(int)) {
    std::cout << "int[";
    if(val_num_ == VAR_SIZE)
      std::cout << "var]\n";
    else 
      std::cout << val_num_ << "]\n";
  } else if(*cell_type_ == typeid(int64_t)) {
    std::cout << "int64_t[";
    if(val_num_ == VAR_SIZE)
      std::cout << "var]\n";
    else 
      std::cout << val_num_ << "]\n";
  } else if(*cell_type_ == typeid(float)) {
    std::cout << "float[";
    if(val_num_ == VAR_SIZE)
      std::cout << "var]\n";
    else 
      std::cout << val_num_ << "]\n";
  } else if(*cell_type_ == typeid(double)) {
    std::cout << "double[";
    if(val_num_ == VAR_SIZE)
      std::cout << "var]\n";
    else 
      std::cout << val_num_ << "]\n";
  }

  std::cout << "Tile type: "
            << (tile_type_ == ATTRIBUTE ? "ATTRIBUTE" : "COORDINATE") << "\n";

  std::cout << "Cell size: ";
  if(val_num_ == VAR_SIZE)
    std::cout << "var\n";
  else 
    std::cout << cell_size_ << "\n";
  std::cout << "Cell num: " << cell_num_ << "\n";
  std::cout << "Tile size: " << tile_size_ << "\n";
  std::cout << "Cell type size: " << type_size_ << "\n";

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

  std::cout << "========== End of Tile info ========== \n\n";
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
  payload_ = NULL;
  cell_num_ = 0;
  tile_size_ = 0;
  offsets_.clear();
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
  assert(cell_type_ == &typeid(T));

  std::cout << "Payload contents:\n";

  // Attribute tiles
  if(tile_type_ == ATTRIBUTE) {
    // Fixed-sized cells
    if(val_num_ != VAR_SIZE) {
      T* payload = static_cast<T*>(payload_);
      for(int64_t i=0; i < cell_num_; ++i) {
        std::cout << "\t";
        for(int j=0; j<val_num_; ++j) {
          T v = payload[i*val_num_ + j];
         if(is_null(v))
            std::cout << NULL_VALUE << "\t";
          else if(is_del(v))
            std::cout << DEL_VALUE << "\t";
          else
            std::cout << v << "\t";
        }
        std::cout << "\n";
      }
    } else { // Variable-sized cells
      char* payload = static_cast<char*>(payload_);
      size_t offset = 0;
      int val_num; 

      while(offset < tile_size_) {
        memcpy(&val_num, payload + offset, sizeof(int));
        offset += sizeof(int);
        std::cout << "\t";

        for(int j=0; j<val_num; ++j) {
          void* payload_tmp = payload + offset;
          T v = *(static_cast<T*>(payload_tmp));
          if(is_null(v))
            std::cout << NULL_VALUE << "\t";
          else if(is_del(v))
            std::cout << DEL_VALUE << "\t";
          else
            std::cout << v << "\t";
          offset += type_size_;
        }

        std::cout << "\n";
      }

      assert(offset == tile_size_);
    }
  } else { // Coordinate tiles
    T* payload = static_cast<T*>(payload_);
    for(int64_t i=0; i < cell_num_; ++i) {
      std::cout << "\t";
      for(int j=0; j<dim_num_; ++j)
        std::cout << payload[i*dim_num_ + j] << "\t";
      std::cout << "\n";
    }
  }
}

// Explicit template instantiations
template bool Tile::cell_inside_range<int>
    (int64_t pos, const int* range) const;
template bool Tile::cell_inside_range<int64_t>
    (int64_t pos, const int64_t* range) const;
template bool Tile::cell_inside_range<float>
    (int64_t pos, const float* range) const;
template bool Tile::cell_inside_range<double>
    (int64_t pos, const double* range) const;

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
