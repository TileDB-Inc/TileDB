/**
 * @file   cell_iterator.cc
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
 * This file implements the class CellIterator.
 */

#include "cell_iterator.h"
#include "utils.h"

/******************************************************
************* CONSTRUCTORS & DESTRUCTORS **************
******************************************************/

CellIterator::CellIterator() 
    : cells_(NULL), cells_size_(0), array_schema_(NULL) {
  end_ = true;
  offset_ = 0;
}

CellIterator::CellIterator(
    void* cells, size_t cells_size, 
    const ArraySchema* array_schema) 
    : cells_(cells), cells_size_(cells_size), array_schema_(array_schema) {
  attribute_ids_ = array_schema_->attribute_ids();
  cell_size_ = array_schema_->cell_size();
  end_ = false;
  offset_ = 0;
}

CellIterator::CellIterator(
    void* cells, size_t cells_size, 
    const ArraySchema* array_schema,
    const std::vector<int>& attribute_ids)
    : cells_(cells), cells_size_(cells_size), array_schema_(array_schema) {
  attribute_ids_ = attribute_ids;
  cell_size_ = array_schema_->cell_size(attribute_ids_);
  end_ = false;
  offset_ = 0;
} 


CellIterator::~CellIterator() {
}

/******************************************************
********************* ACCESSORS ***********************
******************************************************/

bool CellIterator::end() const {
  return end_;
}

/******************************************************
********************* OPERATORS ***********************
******************************************************/

void CellIterator::operator++() {
  if(cells_ == NULL || end_) 
    return; 

  if(cell_size_ == VAR_SIZE) { // Variable-sized cells
    // For easy reference
    int attribute_num = array_schema_->attribute_num(); 
    size_t coords_size = array_schema_->cell_size(attribute_num);
    size_t cell_size;  
 
    memcpy(&cell_size, 
           static_cast<const char*>(cells_) + offset_ + coords_size, 
           sizeof(size_t));
    offset_ += cell_size;
  } else {                     // Fixed-sized cells
    offset_ += cell_size_;
  }

  if(offset_ == cells_size_)
    end_ = true;
}

void* CellIterator::operator*() const {
  if(cells_ == NULL || end_)
    return NULL;
  else
    return static_cast<char*>(cells_) + offset_;
}
