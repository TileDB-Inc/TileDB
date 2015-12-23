/**
 * @file   book_keeping.h
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
 * This file defines class BookKeeping. 
 */

#ifndef __BOOK_KEEPING_H__
#define __BOOK_KEEPING_H__

#include "fragment.h"
#include <vector>

class Fragment;

/** Stores the book-keeping structures of a fragment. */
class BookKeeping {
 public:
  // CONSTRUCTORS & DESTRUCTORS

  /** 
   * Constructor. 
   *
   * @param fragment The fragment the book-keeping structure belongs to.
   */
  BookKeeping(const Fragment* fragment);

  /** Destructor. */
  ~BookKeeping();

  // MUTATORS 
 
  /** Appends a tile offset for the input attribute. */
  void append_tile_offset(int attribute_id, size_t offset);

  // MISC
  void flush();

 private:
  // PRIVATE ATTRIBUTES

  /** The fragment the book-keeping belongs to. */
  const Fragment* fragment_;
  /** The tile offsets in their corresponding attribute files. */
  std::vector<std::vector<size_t> > tile_offsets_;
};

#endif
