/**
 * @file   fragment.cc
 *
 * @section LICENSE
 *
 * The MIT License
 * 
 * @copyright Copyright (c) 2016 MIT and Intel Corporation
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
#include "tiledb_constants.h"
#include "utils.h"
#include <cassert>
#include <cstdio>
#include <cstring>
#include <iostream>




/* ****************************** */
/*             MACROS             */
/* ****************************** */

#ifdef TILEDB_VERBOSE
#  define PRINT_ERROR(x) std::cerr << TILEDB_FG_ERRMSG << x << ".\n" 
#else
#  define PRINT_ERROR(x) do { } while(0) 
#endif





/* ****************************** */
/*        GLOBAL VARIABLES        */
/* ****************************** */

std::string tiledb_fg_errmsg = "";




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

  if(book_keeping_ != NULL && !read_mode())
    delete book_keeping_;
}




/* ****************************** */
/*            ACCESSORS           */
/* ****************************** */

const Array* Fragment::array() const {
  return array_;
}

int64_t Fragment::cell_num_per_tile() const {
  return (dense_) ? array_->array_schema()->cell_num_per_tile() : 
                    array_->array_schema()->capacity(); 
}

bool Fragment::dense() const {
  return dense_;
}

const std::string& Fragment::fragment_name() const {
  return fragment_name_;
}

int Fragment::mode() const {
  return mode_;
}

inline
bool Fragment::read_mode() const {
  return array_read_mode(mode_);
}

ReadState* Fragment::read_state() const {
  return read_state_;
}

size_t Fragment::tile_size(int attribute_id) const {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  bool var_size = array_schema->var_size(attribute_id);

  int64_t cell_num_per_tile = (dense_) ? 
              array_schema->cell_num_per_tile() : 
              array_schema->capacity(); 
 
  return (var_size) ? 
             cell_num_per_tile * TILEDB_CELL_VAR_OFFSET_SIZE :
             cell_num_per_tile * array_schema->cell_size(attribute_id);
}

inline
bool Fragment::write_mode() const {
  return array_write_mode(mode_);
}




/* ****************************** */
/*            MUTATORS            */
/* ****************************** */

int Fragment::finalize() {
  if(write_state_ != NULL) {  // WRITE
    assert(book_keeping_ != NULL);  
    int rc_ws = write_state_->finalize();
    int rc_bk = book_keeping_->finalize();
    int rc_rn = TILEDB_FG_OK;
    int rc_cf = TILEDB_UT_OK;
    if(is_dir(fragment_name_)) {
      rc_rn = rename_fragment();
      rc_cf = create_fragment_file(fragment_name_);
    }
    // Errors
    if(rc_ws != TILEDB_WS_OK) {
      tiledb_fg_errmsg = tiledb_ws_errmsg;
      return TILEDB_FG_ERR;
    } 
    if(rc_bk != TILEDB_BK_OK) {
      tiledb_fg_errmsg = tiledb_bk_errmsg;
      return TILEDB_FG_ERR;
    } 
    if(rc_cf != TILEDB_UT_OK) {
      tiledb_fg_errmsg = tiledb_ut_errmsg;
      return TILEDB_FG_ERR;
    }
    if(rc_rn != TILEDB_FG_OK)
      return TILEDB_FG_ERR;

    // Success
    return TILEDB_FG_OK;
  } else {                    // READ
    // Nothing to be done
    return TILEDB_FG_OK;
  } 
}

int Fragment::init(
    const std::string& fragment_name, 
    int mode,
    const void* subarray) {
  // Set fragment name and mode
  fragment_name_ = fragment_name;
  mode_ = mode;

  // Sanity check
  if(!write_mode()) {
    std::string errmsg = "Cannot initialize fragment;  Invalid mode";
    PRINT_ERROR(errmsg);
    tiledb_fg_errmsg = TILEDB_FG_ERRMSG + errmsg;
    return TILEDB_FG_ERR;
  }

  // Check if the fragment is dense or not
  dense_ = true;
  const std::vector<int>& attribute_ids = array_->attribute_ids();
  int id_num = attribute_ids.size();
  int attribute_num = array_->array_schema()->attribute_num();
  for(int i=0; i<id_num; ++i) {
    if(attribute_ids[i] == attribute_num) {
      dense_ = false;
      break;
    }
  }

  // Initialize book-keeping and read/write state
  book_keeping_ = 
      new BookKeeping(
          array_->array_schema(),
          dense_,
          fragment_name,
          mode_);
  read_state_ = NULL;
  if(book_keeping_->init(subarray) != TILEDB_BK_OK) {
    delete book_keeping_;
    book_keeping_ = NULL;
    write_state_ = NULL;
    tiledb_fg_errmsg = tiledb_bk_errmsg;
    return TILEDB_FG_ERR;
  }
  write_state_ = new WriteState(this, book_keeping_);

  // Success
  return TILEDB_FG_OK;
}

int Fragment::init(
    const std::string& fragment_name, 
    BookKeeping* book_keeping) {
  // Set member attributes
  fragment_name_ = fragment_name;
  mode_ = array_->mode();
  book_keeping_ = book_keeping;
  dense_ = book_keeping_->dense();
  write_state_ = NULL;
  read_state_ = new ReadState(this, book_keeping_);

  // Success
  return TILEDB_FG_OK;
}

void Fragment::reset_read_state() {
  read_state_->reset();
}

int Fragment::sync() {
  // Sanity check
  assert(write_state_ != NULL);

  // Sync
  if(write_state_->sync() != TILEDB_WS_OK) {
    tiledb_fg_errmsg = tiledb_ws_errmsg;
    return TILEDB_FG_ERR;
  } else {
    return TILEDB_FG_OK;
  }
}

int Fragment::sync_attribute(const std::string& attribute) {
  // Sanity check
  assert(write_state_ != NULL);

  // Sync attribute
  if(write_state_->sync_attribute(attribute) != TILEDB_WS_OK) {
    tiledb_fg_errmsg = tiledb_ws_errmsg;
    return TILEDB_FG_ERR;
  } else {
    return TILEDB_FG_OK;
  }
}

int Fragment::write(const void** buffers, const size_t* buffer_sizes) {
  // Forward the write command to the write state
  int rc = write_state_->write(buffers, buffer_sizes);

  // Error
  if(rc != TILEDB_WS_OK) {
    tiledb_fg_errmsg = tiledb_ws_errmsg;
    return TILEDB_FG_ERR;
  }

  // Success
  return TILEDB_FG_OK;
}




/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

int Fragment::rename_fragment() {
  // Do nothing in READ mode
  if(read_mode())
    return TILEDB_FG_OK;

  std::string parent_dir = ::parent_dir(fragment_name_);
  std::string new_fragment_name = parent_dir + "/" +
                                  fragment_name_.substr(parent_dir.size() + 2);

  if(rename(fragment_name_.c_str(), new_fragment_name.c_str())) {
    std::string errmsg = 
        std::string("Cannot rename fragment directory; ") + strerror(errno);
    PRINT_ERROR(errmsg);
    tiledb_fg_errmsg = TILEDB_FG_ERRMSG + errmsg;
    return TILEDB_FG_ERR;
  } 

  fragment_name_ = new_fragment_name;

  return TILEDB_FG_OK;
}

