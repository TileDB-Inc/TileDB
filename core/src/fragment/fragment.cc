/**
 * @file   fragment.cc
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
 * This file implements the Fragment class.
 */

#include "fragment.h"

/* ****************************** */
/*             MACROS             */
/* ****************************** */

#if VERBOSE == 1
#  define PRINT_ERROR(x) std::cerr << "[TileDB] Error: " << x << ".\n" 
#  define PRINT_WARNING(x) std::cerr << "[TileDB] Warning: " \
                                     << x << ".\n"
#elif VERBOSE == 2
#  define PRINT_ERROR(x) std::cerr << "[TileDB::Fragment] Error: " \
                                   << x << ".\n" 
#  define PRINT_WARNING(x) std::cerr << "[TileDB::Fragment] Warning: " \
                                     << x << ".\n"
#else
#  define PRINT_ERROR(x) do { } while(0) 
#  define PRINT_WARNING(x) do { } while(0) 
#endif

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Fragment::Fragment(
    const std::string& fragment_name,
    const Array* array)
    : fragment_name_(fragment_name),
      array_(array) {
  book_keeping_ = NULL;
  write_state_ = NULL;
}

Fragment::~Fragment() {
  // The fragment was opened for writing
  if(write_state_ != NULL) {
    write_state_->flush();
    book_keeping_->flush();
    delete write_state_;
    delete book_keeping_;
  }
}

/* ****************************** */
/*            ACCESSORS           */
/* ****************************** */

const Array* Fragment::array() const {
  return array_;
}

/* ****************************** */
/*         WRITE FUNCTIONS        */
/* ****************************** */

int Fragment::write(const void** buffers, const size_t* buffer_sizes) {
  // Initialize book-keeping and write state
  if(write_state_ == NULL) {
    book_keeping_ = new BookKeeping(this);
    write_state_ = new WriteState(this, book_keeping_);
  }

  // Forward the write command to the write state
  int rc = write_state_->write(buffers, buffer_sizes);

  if(rc == TILEDB_WS_OK)
    return TILEDB_FG_OK;
  else
    return TILEDB_FG_ERR;
}
