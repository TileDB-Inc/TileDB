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
#include "utils.h"
#include <cassert>
#include <cstdio>
#include <cstring>
#include <iostream>

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

Fragment::Fragment(const Array* array)
    : array_(array) {
  read_state_ = NULL;
  write_state_ = NULL;
  book_keeping_ = NULL;
}

Fragment::~Fragment() {
  if(write_state_ != NULL)
    delete write_state_;

  if(read_state_ != NULL)
    delete read_state_;

  if(book_keeping_ != NULL)
    delete book_keeping_;
}

/* ****************************** */
/*            ACCESSORS           */
/* ****************************** */

const std::string& Fragment::fragment_name() const {
  return fragment_name_;
}

const Array* Fragment::array() const {
  return array_;
}

int Fragment::read(void** buffers, size_t* buffer_sizes) {
  // Sanity check
  if(array_->range() == NULL) {
    PRINT_ERROR("Cannot read from fragment; Invalid range");
    return TILEDB_BK_ERR;
  }

  // Forward the read command to the read state
  int rc = read_state_->read(buffers, buffer_sizes);

  if(rc == TILEDB_RS_OK)
    return TILEDB_FG_OK;
  else
    return TILEDB_FG_ERR;
}

/* ****************************** */
/*            MUTATORS            */
/* ****************************** */

int Fragment::init(const std::string& fragment_name, const void* range) {
  // Set fragment name
  fragment_name_ = fragment_name;

  // For easy referece
  int mode = array_->mode();

  // Initialize book-keeping and write state
  book_keeping_ = new BookKeeping(this);
  if(mode == TILEDB_WRITE || mode == TILEDB_WRITE_UNSORTED) {
    read_state_ = NULL;
    if(book_keeping_->init(range) != TILEDB_BK_OK) {
      delete book_keeping_;
      book_keeping_ = NULL;
      write_state_ = NULL;
      return TILEDB_FG_ERR;
    }
    write_state_ = new WriteState(this, book_keeping_);
  } else if(mode == TILEDB_READ || mode == TILEDB_READ_REVERSE) {
    write_state_ = NULL;
    if(book_keeping_->load() != TILEDB_BK_OK) {
      delete book_keeping_;
      book_keeping_ = NULL;
      return TILEDB_FG_ERR;
    }
    read_state_ = new ReadState(this, book_keeping_);
  }

  // Success
  return TILEDB_FG_OK;
}

int Fragment::write(const void** buffers, const size_t* buffer_sizes) {
  // Forward the write command to the write state
  int rc = write_state_->write(buffers, buffer_sizes);

  if(rc == TILEDB_WS_OK)
    return TILEDB_FG_OK;
  else
    return TILEDB_FG_ERR;
}

/* ****************************** */
/*               MISC             */
/* ****************************** */

int Fragment::finalize() {
  // The fragment was opened for writing
  if(write_state_ != NULL) {
    assert(book_keeping_ != NULL);  
    int rc_ws = write_state_->finalize();
    int rc_bk = book_keeping_->finalize();
    int rc_rn = rename_fragment();
    int rc_cf = create_fragment_file(fragment_name_);
      return TILEDB_WS_ERR;
    if(rc_ws != TILEDB_WS_OK || rc_bk != TILEDB_BK_OK || 
       rc_rn != TILEDB_FG_OK || rc_cf != TILEDB_UT_OK)
      return TILEDB_FG_ERR;
    else 
      return TILEDB_FG_OK;
  } else { // The fragment was opened for reading
    // Nothing to be done
    return TILEDB_FG_OK;
  } 
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

int Fragment::rename_fragment() {
  // Do nothing in READ mode
  if(array_->mode() == TILEDB_READ || array_->mode() == TILEDB_READ_REVERSE)
    return TILEDB_FG_OK;

  std::string parent_dir = ::parent_dir(fragment_name_);
  std::string new_fragment_name = parent_dir + "/" +
                                  fragment_name_.substr(parent_dir.size() + 2);

  if(rename(fragment_name_.c_str(), new_fragment_name.c_str())) {
    PRINT_ERROR(std::string("Cannot rename fragment directory; ") +
                strerror(errno));
    return TILEDB_FG_ERR;
  } 

  fragment_name_ = new_fragment_name;

  return TILEDB_FG_OK;
}
