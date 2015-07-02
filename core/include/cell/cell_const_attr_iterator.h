/**
 * @file   cell_const_attr_iterator.h
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
 * This file defines class CellConstAttrIterator.
 */

#ifndef CELL_CONST_ATTR_ITERATOR_H
#define CELL_CONST_ATTR_ITERATOR_H

#include "cell.h"
#include "type_converter.h"
#include <string.h>

class Cell;

/** Constant iterator over the attribute values of the cell. */
class CellConstAttrIterator {
 public:
  // CONSTRUCTORS & DESTRUCTORS
  /** 
   * Constructor. The inputs are a cell and a position. The latter is a
   * position in the attribute_ids_ list of the input cell, i.e., it
   * indicates which attribute the iterator starts on. 
   */
  CellConstAttrIterator(const Cell* cell, int pos);
  /** Destructor. */
  ~CellConstAttrIterator();

  // ACCESSORS
  /** Returns the id of the attribute the iterator is currently on. */
  int attribute_id() const;
  /** True if the iterator has reached the end of the attributes. */
  bool end() const;
  /** Returns the offset in the cell payload the iterator is currently on. */
  size_t offset() const;

  // OPERATORS
  /** Advances the iterator to the next attribute. */
  void operator++();
  /** Returns the attribute value(s). */
  TypeConverter operator*() const;

 private:
  // PRIVATE ATTRIBUTES
  /** The cell the iterator iterates over. */
  const Cell* cell_;
  /** True if the iterator has reached the end of the attributes. */
  bool end_;
  /** The offset in the cell payload the iterator is currently on. */
  size_t offset_;
  /** The position of the iterator on the attributes of the cell. */
  int pos_;
};

#endif
