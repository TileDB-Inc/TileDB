/**
 * @file   fragment.h
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
 * This file defines class Fragment. 
 */

#ifndef __FRAGMENT_H__
#define __FRAGMENT_H__

#include "array.h"
#include "array_schema.h"
#include "book_keeping.h"
#include "constants.h"
#include "read_state.h"
#include "write_state.h"
#include <vector>

/* ********************************* */
/*             CONSTANTS             */
/* ********************************* */

#define TILEDB_FG_OK     0
#define TILEDB_FG_ERR   -1

class Array;
class BookKeeping;
class ReadState;
class WriteState;

/** Contains information about an array fragment. */
class Fragment {
 public:
  // CONSTRUCTORS & DESTRUCTORS
  
  /** 
   * Constructor. 
   *
   * @param array The array the fragment belongs to.
   */
  Fragment(const Array* array);
  /** Destructor. */
  ~Fragment();

  // ACCESSORS
  
  /** Returns the array the fragment belongs to. */
  const Array* array() const;

  /** Returns the fragment name. */
  const std::string& fragment_name() const;

  // TODO
  int read(void** buffers, size_t* buffer_sizes);

  //MUTATORS
  
  // TODO
  int init(const std::string& fragment_name, const void* range);

  // TODO
  int write(const void** buffers, const size_t* buffer_sizes);

  // MISC
  
  // TODO
  int finalize();
 
 private:
  // PRIVATE ATTRIBUTES
  /** The array the fragment belongs to. */
  const Array* array_;
  /** A book-keeping structure. */
  BookKeeping* book_keeping_;
  /** The fragment name. */
  std::string fragment_name_;
  /** A read state structure. */
  ReadState* read_state_;
  /** A write state structure. */
  WriteState* write_state_;

  // PRIVATE METHODS
  /** 
   * Changes the temporary fragment name into a stable one.
   *
   * @return TILEDB_FG_OK for success, and TILEDB_FG_ERR for error.
   */
  int rename_fragment();
};

#endif
