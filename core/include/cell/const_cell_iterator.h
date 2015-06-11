/**
 * @file   const_cell_iterator.h
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
 * This file defines class ConstCellIterator.
 */

#ifndef CONST_CELL_ITERATOR_H
#define CONST_CELL_ITERATOR_H

#include "array_schema.h"
#include <vector>

/** 
 * Objects of this class enable iterating over binary cell payloads serialized
 * in main memory.
 */
class ConstCellIterator {
 public:
  // CONSTRUCTORS & DESTRUCTORS
  /** Constructor. */
  ConstCellIterator();
  /** Constructor. */
  ConstCellIterator(
      const void* cells, size_t cells_size, 
      const ArraySchema* array_schema);
  /** Constructor. */
  ConstCellIterator(
      const void* cells, size_t cells_size, 
      const ArraySchema* array_schema,
      const std::vector<int>& attribute_ids);
  /** Destructor. */
  ~ConstCellIterator();

  // ACCESSORS
  /** True if the end of cells has been reached. */
  bool end() const;

  // OPERATORS
  /** Advances the iterator to the next cell. */
  void operator++();
  /** Returns the memory location of the current cell. */
  const void* operator*() const;

 private:
  // PRIVATE ATTRIBUTES
  /** The array schema. */
  const ArraySchema* array_schema_;
  /** The attributes that each cell contains. */
  std::vector<int> attribute_ids_;
  /** Size of a single cell (could be VAR_SIZE). */
  size_t cell_size_;
  /** The serialized cell payloads. */
  const void* cells_;
  /** The collective size of all cells. */
  size_t cells_size_;
  /** True if the iterator has reached the end of the cells. */
  bool end_;
  /** The current offset of the iterator on the cell payloads. */
  size_t offset_;
};

#endif
