/**
 * @file   tile_const_cell_iterator.cc
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
 * This file implements the TileConstCellIterator class.
 */

#include "tile_const_cell_iterator.h"

/******************************************************
************* CONSTRUCTORS & DESTRUCTORS **************
******************************************************/

TileConstCellIterator::TileConstCellIterator() 
    : tile_(NULL), pos_(-1), cell_(NULL), end_(true) {
}

TileConstCellIterator::TileConstCellIterator(const Tile* tile, int64_t pos)
    : tile_(tile), pos_(pos) {
  if(pos_ >= 0 && pos_ < tile_->cell_num()) {
    cell_ = tile_->cell(pos_);
    end_ = false;
  } else {
    cell_ = NULL;
    end_ = true;
  }
}

/******************************************************
******************** ACCESSORS ************************
******************************************************/

template<class T>
bool TileConstCellIterator::cell_inside_range(
    const T* range) const {
  return tile_->cell_inside_range(pos_, range); 
}

int64_t TileConstCellIterator::cell_num() const {
  return tile_->cell_num();
}

size_t TileConstCellIterator::cell_size() const {
  // Fixed-sized cells
  if(!tile_->var_size()) {
    return tile_->cell_size();
  } else { // Variable-sized cells
    int val_num;
    memcpy(&val_num, cell_, sizeof(int));
    return sizeof(int) + val_num * tile_->type_size(); 
  }
}

const std::type_info* TileConstCellIterator::cell_type() const {
  return tile_->cell_type();
}

int TileConstCellIterator::dim_num() const {
  return tile_->dim_num();
}

bool TileConstCellIterator::end() const {
  return end_;
}

bool TileConstCellIterator::is_del() const {
  return tile_->is_del(pos_);
}

bool TileConstCellIterator::is_null() const {
  return tile_->is_null(pos_);
}

int64_t TileConstCellIterator::pos() const {
  return pos_;
}

const Tile* TileConstCellIterator::tile() const {
  return tile_;
}

int64_t TileConstCellIterator::tile_id() const {
  return tile_->tile_id();
}

/******************************************************
******************** OPERATORS ************************
******************************************************/

void TileConstCellIterator::operator=(const TileConstCellIterator& rhs) {
  pos_ = rhs.pos_;
  cell_ = rhs.cell_;
  end_ = rhs.end_;
  tile_ = rhs.tile_;
}

TileConstCellIterator TileConstCellIterator::operator+(int64_t step) {
  TileConstCellIterator it = *this;
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

void TileConstCellIterator::operator+=(int64_t step) {
  pos_ += step;
  if(pos_ >= 0 && pos_ < tile_->cell_num()) {
    cell_ = tile_->cell(pos_);
    end_ = false;
  } else {  
    cell_ = NULL;
    end_ = true;
  }
}

TileConstCellIterator TileConstCellIterator::operator++() {
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

TileConstCellIterator TileConstCellIterator::operator++(int junk) {
  TileConstCellIterator it = *this;
  ++pos_;
  if(pos_ >= 0 && pos_ < tile_->cell_num()) {
    cell_ = tile_->cell(pos_);
    end_ = false;
  } else {
    cell_ = NULL;
    end_ = true;
  }
  return it;
}

bool TileConstCellIterator::operator==(
    const TileConstCellIterator& rhs) const {
  return (tile_ == rhs.tile_ && pos_ == rhs.pos_);
}

bool TileConstCellIterator::operator!=(
  const TileConstCellIterator& rhs) const {
  return (tile_ != rhs.tile_ || pos_ != rhs.pos_);
}

const void* TileConstCellIterator::operator*() const {
  return cell_;
}

// Explicit template instantiations
template bool TileConstCellIterator::cell_inside_range<int>
    (const int* range) const;
template bool TileConstCellIterator::cell_inside_range<int64_t>
    (const int64_t* range) const;
template bool TileConstCellIterator::cell_inside_range<float>
    (const float* range) const;
template bool TileConstCellIterator::cell_inside_range<double>
    (const double* range) const;

