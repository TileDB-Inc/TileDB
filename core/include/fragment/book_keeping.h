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

/* ********************************* */
/*             CONSTANTS             */
/* ********************************* */

#define TILEDB_BK_OK     0
#define TILEDB_BK_ERR   -1

#define TILEDB_BK_BUFFER_SIZE 10000

class Fragment;

/** Stores the book-keeping structures of a fragment. */
class BookKeeping {
 public:
  // CONSTRUCTORS & DESTRUCTORS

  /** 
   * Constructor. 
   *
   * @param fragment The fragment the book-keeping structure belongs to.
   * @param range The range in which the array read/write will be constrained.
   */
  BookKeeping(const Fragment* fragment, const void* range);

  /** Destructor. */
  ~BookKeeping();

  // ACCESSORS

  /** Returns the range in which the fragment is constrained. */
  const void* range() const;

  // MUTATORS 
 
  /** Appends a tile offset for the input attribute. */
  void append_tile_offset(int attribute_id, size_t offset);

  // MISC

  // TODO
  int finalize();

 private:
  // PRIVATE ATTRIBUTES

  /** The fragment the book-keeping belongs to. */
  const Fragment* fragment_;
  /** The offsets of the next tile to be appended for each attribute. */
  std::vector<size_t> next_tile_offsets_;
  /**
   * The range in which the fragment is constrained. Note that the type of the
   * range must be the same as the type of the array coordinates.
   */
  void* range_;
  /** The tile offsets in their corresponding attribute files. */
  std::vector<std::vector<size_t> > tile_offsets_;

  // PRIVATE METHODS

  /** 
   * Serializes the book-keeping object into a buffer.
   *
   * @param buffer The buffer where the book-keeping object is copied.
   * @param buffer_size The size of the buffer after serialization.
   * @return TILEDB_BK_OK for succes, and TILEDB_BK_ERR for error.
   */ 
  int serialize(void*& buffer, size_t& buffer_size) const;

  /** 
   * Serializes the range into the buffer.
   *
   * @param buffer The buffer where the range is copied.
   * @param buffer_allocated_size The allocated size of the buffer. Note that 
   *     this may change if the buffer gets full, in which case the buffer is
   *     expanded and the allocated size gets doubled.
   * @param buffer_size The current (useful) size of the buffer.
   * @return TILEDB_BK_OK for succes, and TILEDB_BK_ERR for error.
   */ 
  int serialize_range(
      void*& buffer, 
      size_t& buffer_allocated_size, 
      size_t& buffer_size) const;

  /** 
   * Serializes the tile offsets into the buffer.
   *
   * @param buffer The buffer where the tile offsets are copied.
   * @param buffer_allocated_size The allocated size of the buffer. Note that 
   *     this may change if the buffer gets full, in which case the buffer is
   *     expanded and the allocated size gets doubled.
   * @param buffer_size The current (useful) size of the buffer.
   * @return TILEDB_BK_OK for succes, and TILEDB_BK_ERR for error.
   */ 
  int serialize_tile_offsets(
      void*& buffer, 
      size_t& buffer_allocated_size, 
      size_t& buffer_size) const;

};

#endif
