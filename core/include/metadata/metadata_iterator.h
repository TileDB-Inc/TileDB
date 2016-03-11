/**
 * @file   metadata_iterator.h
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
 * This file defines class MetadataIterator. 
 */

#ifndef __METADATA_ITERATOR_H__
#define __METADATA_ITERATOR_H__

#include "array_iterator.h"
#include "metadata.h"

/* ********************************* */
/*             CONSTANTS             */
/* ********************************* */

#define TILEDB_MIT_OK     0
#define TILEDB_MIT_ERR   -1

// TODO
class MetadataIterator {
 public:
  // CONSTRUCTORS & DESTRUCTORS
  
  /** Constructor. */
  MetadataIterator();

  /** Destructor. */
  ~MetadataIterator();

  // ACCESSORS

  // TODO
  bool end() const;

  // TODO
  int get_value(int attribute_id, const void** value, size_t* value_size) const;

  // MUTATORS 

  // TODO
  int init(
      Metadata* metadata, 
      void** buffers, 
      size_t* buffer_sizes);

  // TODO
  int finalize();

  // TODO
  int next();

 private:
  // PRIVATE ATTRIBUTES

  // TODO
  ArrayIterator* array_it_;
};

#endif
