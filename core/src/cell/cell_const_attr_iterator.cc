/**
 * @file   cell_const_attr_iterator.cc
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
 * This file implements the class CellConstAttrIterator.
 */

#include "cell_const_attr_iterator.h"
#include <string.h>

/******************************************************
************* CONSTRUCTORS & DESTRUCTORS **************
******************************************************/

CellConstAttrIterator::CellConstAttrIterator(
    const Cell* cell, int pos) : cell_(cell), pos_(pos) {
  // For easy reference
  int attribute_num = cell_->array_schema()->attribute_num();

  end_ = false;
  offset_ = cell_->array_schema()->cell_size(attribute_num) + 
            cell_->ids_size();
  if(cell_->var_size())
    offset_ += sizeof(size_t);
}

CellConstAttrIterator::~CellConstAttrIterator() {
}

/******************************************************
********************* ACCESSORS ***********************
******************************************************/

int CellConstAttrIterator::attribute_id() const {
  return cell_->attribute_id(pos_);
}

bool CellConstAttrIterator::end() const {
  return end_;
}

size_t CellConstAttrIterator::offset() const {
  return offset_;
}

/******************************************************
********************* OPERATORS ***********************
******************************************************/

void CellConstAttrIterator::operator++() {
  // Calculate new offset
  int attribute_id = cell_->attribute_id(pos_);
  int val_num = cell_->array_schema()->val_num(attribute_id);
  if(val_num == VAR_SIZE) {
    memcpy(&val_num, static_cast<const char*>(cell_->cell()) + offset_,
           sizeof(int));
    offset_ += sizeof(int);
  }
  offset_ += val_num * cell_->array_schema()->type_size(attribute_id);

  // Advance pos_
  ++pos_;

  // The iterator iterates over the attributes. If it reaches
  // the last element of cell_->attribute_ids_ (i.e., the
  // coordinates), then it means that it has reached the end.
  if(pos_ < 0 || pos_ >= cell_->attribute_num()-1) {
    end_ = true;
  }
}

TypeConverter CellConstAttrIterator::operator*() const {
  return TypeConverter(static_cast<const char*>(cell_->cell()) + offset_);
}
