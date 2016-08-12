/**
 * @file   metadata_iterator.cc
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
 * This file implements the MetadataIterator class.
 */

#include "metadata_iterator.h"




/* ****************************** */
/*             MACROS             */
/* ****************************** */

#ifdef VERBOSE
#  define PRINT_ERROR(x) std::cerr << TILEDB_MIT_ERRMSG << x << ".\n" 
#else
#  define PRINT_ERROR(x) do { } while(0) 
#endif




/* ****************************** */
/*        GLOBAL VARIABLES        */
/* ****************************** */

std::string tiledb_mit_errmsg = "";




/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

MetadataIterator::MetadataIterator() {
  array_it_ = NULL;
}

MetadataIterator::~MetadataIterator() {
}




/* ****************************** */
/*           ACCESSORS            */
/* ****************************** */

const std::string& MetadataIterator::metadata_name() const {
  return array_it_->array_name();
}

bool MetadataIterator::end() const {
  return array_it_->end();
}

int MetadataIterator::get_value(
    int attribute_id,
    const void** value,
    size_t* value_size) const {
  if(array_it_->get_value(attribute_id, value, value_size) != TILEDB_AIT_OK) {
    tiledb_mit_errmsg = tiledb_ait_errmsg; 
    return TILEDB_MIT_ERR;
  }

  // Success
  return TILEDB_MIT_OK; 
}




/* ****************************** */
/*            MUTATORS            */
/* ****************************** */

int MetadataIterator::finalize() {
  int rc = array_it_->finalize();
  delete array_it_;
  array_it_ = NULL;
  delete metadata_;
  metadata_ = NULL;

  // Error
  if(rc != TILEDB_AIT_OK) {
    tiledb_mit_errmsg = tiledb_ait_errmsg; 
    return TILEDB_MIT_ERR;
  }

  // Success
  return TILEDB_MIT_OK;
}

int MetadataIterator::init(
    Metadata* metadata,
    void** buffers,
    size_t* buffer_sizes) {
  // Initialize an array iterator
  metadata_ = metadata;
  array_it_ = new ArrayIterator();
  if(array_it_->init(metadata->array(), buffers, buffer_sizes) != 
     TILEDB_AIT_OK) {
    delete array_it_;
    array_it_ = NULL;
    tiledb_mit_errmsg = tiledb_ait_errmsg; 
    return TILEDB_MIT_ERR;
  } 
  
  // Return
  return TILEDB_MIT_OK;
}

int MetadataIterator::next() {
  if(array_it_->next() != TILEDB_AIT_OK) {
    tiledb_mit_errmsg = tiledb_ait_errmsg; 
    return TILEDB_MIT_ERR;
  }

  // Success
  return TILEDB_MIT_OK;
}
