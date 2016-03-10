/**
 * @file   array_iterator.h
 * @author Stavros Papadopoulos <stavrosp@csail.mit.edu>
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2015 Stavros Papadopoulos <stavrosp@csail.mit.edu>
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
 * This file defines class ArrayIterator. 
 */

#ifndef __ARRAY_ITERATOR_H__
#define __ARRAY_ITERATOR_H__

#include "array.h"

/* ********************************* */
/*             CONSTANTS             */
/* ********************************* */

#define TILEDB_AIT_OK     0
#define TILEDB_AIT_ERR   -1

// TODO
class ArrayIterator {
 public:
  // CONSTRUCTORS & DESTRUCTORS
  
  /** Constructor. */
  ArrayIterator();

  /** Destructor. */
  ~ArrayIterator();

  // ACCESSORS

  // TODO
  bool end() const;

  // TODO
  int get_value(int attribute_id, const void** value, size_t* value_size) const;

  // MUTATORS 

  // TODO
  int init(
      Array* array, 
      void** buffers, 
      size_t* buffer_sizes);

  // TODO
  int finalize();

  // TODO
  int next();

 private:
  // PRIVATE ATTRIBUTES

  // TODO
  Array* array_;

  // TODO
  void** buffers_;

  // TODO
  size_t* buffer_sizes_;

  std::vector<size_t> buffer_allocated_sizes_;

  // TODO
  bool end_;

  // TODO
  std::vector<int64_t> pos_;

  // TODO
  std::vector<int64_t> cell_num_;

  // TODO
  std::vector<size_t> cell_sizes_;

  // TODO
  std::vector<int> buffer_i_;
  
  // TODO
  int var_attribute_num_;
};

#endif
