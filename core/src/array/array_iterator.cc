/**
 * @file   array_iterator.cc
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
 * This file implements the ArrayIterator class.
 */

#include "array_iterator.h"




/* ****************************** */
/*             MACROS             */
/* ****************************** */

#ifdef VERBOSE
#  define PRINT_ERROR(x) std::cerr << TILEDB_AIT_ERRMSG << x << ".\n" 
#else
#  define PRINT_ERROR(x) do { } while(0) 
#endif




/* ****************************** */
/*        GLOBAL VARIABLES        */
/* ****************************** */

std::string tiledb_ait_errmsg = "";




/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

ArrayIterator::ArrayIterator() {
  array_ = NULL;
  buffers_ = NULL;
  buffer_sizes_ = NULL;
  end_ = false;
  var_attribute_num_ = 0;
}

ArrayIterator::~ArrayIterator() {
}




/* ****************************** */
/*           ACCESSORS            */
/* ****************************** */

const std::string& ArrayIterator::array_name() const {
  return array_->array_schema()->array_name();
}

bool ArrayIterator::end() const {
  return end_;
}

int ArrayIterator::get_value(
    int attribute_id,
    const void** value,
    size_t* value_size) const {
  // Trivial case
  if(end_) {
    *value = NULL;
    *value_size = 0;
    std::string errmsg = "Cannot get value; Iterator end reached";
    PRINT_ERROR(errmsg);
    tiledb_ait_errmsg = TILEDB_AIT_ERRMSG + errmsg; 
    return TILEDB_AIT_ERR;
  }

  // Get the value
  int buffer_i = buffer_i_[attribute_id];
  int64_t pos = pos_[attribute_id];
  size_t cell_size = cell_sizes_[attribute_id];
  if(cell_size != TILEDB_VAR_SIZE) { // FIXED
    *value = static_cast<const char*>(buffers_[buffer_i]) + pos*cell_size;
    *value_size = cell_size;
  } else {                           // VARIABLE
    size_t offset = static_cast<size_t*>(buffers_[buffer_i])[pos];
    *value = static_cast<const char*>(buffers_[buffer_i+1]) + offset;
    if(pos < cell_num_[attribute_id] - 1) 
      *value_size = static_cast<size_t*>(buffers_[buffer_i])[pos+1] - offset;
    else 
      *value_size = buffer_sizes_[buffer_i+1] - offset;
  }

  // Success
  return TILEDB_AIT_OK; 
}




/* ****************************** */
/*            MUTATORS            */
/* ****************************** */

int ArrayIterator::init(
    Array* array,
    void** buffers,
    size_t* buffer_sizes) {
  // Initial assignments
  array_ = array;
  buffers_ = buffers;
  buffer_sizes_ = buffer_sizes;
  end_ = false;
  var_attribute_num_ = 0;

  // Initialize next, cell num, cell sizes, buffer_i and var_attribute_num
  const ArraySchema* array_schema = array_->array_schema();
  const std::vector<int> attribute_ids = array_->attribute_ids();
  int attribute_id_num = attribute_ids.size();
  pos_.resize(attribute_id_num);
  cell_num_.resize(attribute_id_num);
  cell_sizes_.resize(attribute_id_num);
  buffer_i_.resize(attribute_id_num);

  for(int i=0, buffer_i=0; i<attribute_id_num; ++i) {
    pos_[i] = 0;
    cell_num_[i] = 0;
    cell_sizes_[i] = array_schema->cell_size(attribute_ids[i]);
    buffer_i_[i] = buffer_i;
    buffer_allocated_sizes_.push_back(buffer_sizes[buffer_i]);
    if(cell_sizes_[i] != TILEDB_VAR_SIZE) {
      ++buffer_i;
    } else {
      buffer_allocated_sizes_.push_back(buffer_sizes[buffer_i+1]);
      buffer_i += 2;
      ++var_attribute_num_;
    }
  }

  // Perform first read
  if(array_->read(buffers, buffer_sizes) != TILEDB_AR_OK) {
    tiledb_ait_errmsg = tiledb_ar_errmsg; 
    return TILEDB_AIT_ERR;
  }

  // Check if initialization went well and update internal state
  for(int i=0; i<attribute_id_num; ++i) {
    // End
    if(buffer_sizes_[buffer_i_[i]] == 0 && 
       !array_->overflow(attribute_ids[i])) {
      end_ = true;
      return TILEDB_AIT_OK;
    } 

    // Error
    if(buffer_sizes_[buffer_i_[i]] == 0 && 
       array_->overflow(attribute_ids[i])) {
      std::string errmsg = 
          "Array iterator initialization failed; Buffer overflow";
      PRINT_ERROR(errmsg);
      tiledb_ait_errmsg = TILEDB_AIT_ERRMSG + errmsg; 
      return TILEDB_AIT_ERR;
    }

    // Update cell num
    if(cell_sizes_[i] == TILEDB_VAR_SIZE)  // VARIABLE
      cell_num_[i] = buffer_sizes[buffer_i_[i]] / TILEDB_CELL_VAR_OFFSET_SIZE;
    else                                   // FIXED 
      cell_num_[i] = buffer_sizes[buffer_i_[i]] / cell_sizes_[i]; 
  }

  // Return
  return TILEDB_AIT_OK;
}

int ArrayIterator::finalize() {
  // Finalize
  int rc = array_->finalize();
  delete array_;
  array_ = NULL;

  // Error
  if(rc != TILEDB_AR_OK) {
    tiledb_ait_errmsg = tiledb_ar_errmsg;
    return TILEDB_AIT_ERR;
  }

  // Success
  return TILEDB_AIT_OK;
}

int ArrayIterator::next() {
  // Trivial case
  if(end_) {
    std::string errmsg = "Cannot advance iterator; Iterator end reached";
    PRINT_ERROR(errmsg);
    tiledb_ait_errmsg = TILEDB_AIT_ERRMSG + errmsg; 
    return TILEDB_AIT_ERR;
  }

  // Advance iterator
  std::vector<int> needs_new_read;
  const std::vector<int> attribute_ids = array_->attribute_ids();
  int attribute_id_num = attribute_ids.size();
  for(int i=0; i<attribute_id_num; ++i) {
    // Advance position
    ++pos_[i];

    // Record the attributes that need a new read
    if(pos_[i] == cell_num_[i]) 
      needs_new_read.push_back(i);
  }

  // Perform a new read
  if(needs_new_read.size() > 0) {
    // Need to copy buffer_sizes_ and restore at the end.
    // buffer_sizes_ must be set to 0 for array->read() to work correctly, i.e.,
    // do not fetch new data for fields which still have pending data in
    // buffers_. However, the correct value of buffer_sizes_ for such fields is
    // required for correct operation of the iterator in subsequent calls
    std::vector<size_t> copy_buffer_sizes(attribute_id_num+var_attribute_num_);
    // Properly set the buffer sizes
    for(int i=0; i<attribute_id_num + var_attribute_num_; ++i) {
      copy_buffer_sizes[i] = buffer_sizes_[i];
      buffer_sizes_[i] = 0;
    }
    int buffer_i;
    int needs_new_read_num = needs_new_read.size();
    for(int i=0; i<needs_new_read_num; ++i) {
      buffer_i = buffer_i_[needs_new_read[i]];
      buffer_sizes_[buffer_i] = buffer_allocated_sizes_[buffer_i]; 
      if(cell_sizes_[needs_new_read[i]] == TILEDB_VAR_SIZE) 
        buffer_sizes_[buffer_i+1] = buffer_allocated_sizes_[buffer_i+1]; 
    }

    // Perform first read
    if(array_->read(buffers_, buffer_sizes_) != TILEDB_AR_OK) {
      tiledb_ait_errmsg = tiledb_ar_errmsg;
      return TILEDB_AIT_ERR;
    }

    // Check if read went well and update internal state
    for(int i=0; i<needs_new_read_num; ++i) {
      buffer_i = buffer_i_[needs_new_read[i]];

      // End
      if(buffer_sizes_[buffer_i] == 0 && 
         !array_->overflow(attribute_ids[needs_new_read[i]])) {
         end_ = true;
        return TILEDB_AIT_OK;
      } 

      // Error
      if(buffer_sizes_[buffer_i] == 0 && 
         array_->overflow(attribute_ids[needs_new_read[i]])) {
        std::string errmsg = "Cannot advance iterator; Buffer overflow";
        PRINT_ERROR(errmsg);
        tiledb_ait_errmsg = TILEDB_AIT_ERRMSG + errmsg; 
        return TILEDB_AIT_ERR;
      }

      // Update cell num
      if(cell_sizes_[needs_new_read[i]] == TILEDB_VAR_SIZE)  // VARIABLE
        cell_num_[needs_new_read[i]] = buffer_sizes_[buffer_i] / sizeof(size_t);
      else                                   // FIXED 
        cell_num_[needs_new_read[i]] = 
            buffer_sizes_[buffer_i] / cell_sizes_[needs_new_read[i]]; 

      // Update pos
      pos_[needs_new_read[i]] = 0;
    }

    // Restore buffer sizes for attributes which still had pending data
    for(int i=0, needs_new_read_idx=0; i<attribute_id_num; ++i) {
      if(static_cast<size_t>(needs_new_read_idx) < needs_new_read.size() && 
         i == needs_new_read[needs_new_read_idx]) // buffer_size would have been
        ++needs_new_read_idx;                     // set by array->read()
      else { //restore buffer size from copy
        buffer_i = buffer_i_[i];
        buffer_sizes_[buffer_i] = copy_buffer_sizes[buffer_i];
        if(cell_sizes_[i] == TILEDB_VAR_SIZE) 
          buffer_sizes_[buffer_i+1] = copy_buffer_sizes[buffer_i+1]; 
      }
    }
  }

  // Success
  return TILEDB_AIT_OK;
}
