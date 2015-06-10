/**
 * @file   fragment_const_tile_iterator.cc
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
 * This file implements the FragmentConstTileIterator class.
 */

#include "fragment_const_tile_iterator.h"
#include <assert.h>

/******************************************************
************* CONSTRUCTORS & DESTRUCTORS **************
******************************************************/

FragmentConstTileIterator::FragmentConstTileIterator() 
    : fragment_(NULL), end_(true) {
}

FragmentConstTileIterator::FragmentConstTileIterator(
    const Fragment* fragment, int attribute_id, int64_t pos)
    : fragment_(fragment), attribute_id_(attribute_id), pos_(pos) {
  if(pos_ >= 0 && pos_ < fragment_->tile_num())
    end_ = false;
  else
    end_ = true;
}

/******************************************************
********************* ACCESSORS ***********************
******************************************************/

const ArraySchema* FragmentConstTileIterator::array_schema() const {
  return fragment_->array_schema();
}

Tile::BoundingCoordinatesPair 
FragmentConstTileIterator::bounding_coordinates() const {
  return fragment_->bounding_coordinates(pos_);
}

bool FragmentConstTileIterator::end() const {
  return end_;
}

Tile::MBR FragmentConstTileIterator::mbr() const {
  return fragment_->mbr(pos_);
}

int64_t FragmentConstTileIterator::pos() const {
  return pos_;
}

int64_t FragmentConstTileIterator::tile_id() const {
  return fragment_->tile_id(pos_);
}

int64_t FragmentConstTileIterator::tile_num() const {
  return fragment_->tile_num();
}

/******************************************************
********************* OPERATORS ***********************
******************************************************/

void FragmentConstTileIterator::operator=(
    const FragmentConstTileIterator& rhs) {
  fragment_ = rhs.fragment_;
  attribute_id_ = rhs.attribute_id_;
  end_ = rhs.end_;
  pos_ = rhs.pos_;
}

FragmentConstTileIterator FragmentConstTileIterator::operator+(int64_t step) {
  FragmentConstTileIterator it = *this;
  it.pos_ += step;
  if(it.pos_ >= 0 && it.pos_ < fragment_->tile_num())
    it.end_ = false;
  else
    it.end_ = true;
  return it;
}

void FragmentConstTileIterator::operator+=(int64_t step) {
  pos_ += step;
  if(pos_ >= 0 && pos_ < fragment_->tile_num())
    end_ = false;
  else
    end_ = true;
}

FragmentConstTileIterator FragmentConstTileIterator::operator++() {
  ++pos_;
  
  if(pos_ >= 0 && pos_ < fragment_->tile_num())
    end_ = false;
  else
    end_ = true;

  return *this;
}

FragmentConstTileIterator FragmentConstTileIterator::operator++(int junk) {
  FragmentConstTileIterator it = *this;
   ++pos_;
  
  if(pos_ >= 0 && pos_ < fragment_->tile_num())
    end_ = false;
  else
    end_ = true;

  return it;
}

bool FragmentConstTileIterator::operator==(
    const FragmentConstTileIterator& rhs) const {
  return (pos_ == rhs.pos_ && attribute_id_ == rhs.attribute_id_ &&
          fragment_ == rhs.fragment_); 
}

bool FragmentConstTileIterator::operator!=(
    const FragmentConstTileIterator& rhs) const {
  return (!(pos_ == rhs.pos_ && attribute_id_ == rhs.attribute_id_ &&
          fragment_ == rhs.fragment_)); 
}

const Tile* FragmentConstTileIterator::operator*() const {
  assert(pos_ >= 0 && pos_ < fragment_->tile_num());

  return fragment_->get_tile_by_pos(attribute_id_, pos_);
}

