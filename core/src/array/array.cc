/**
 * @file   array.cc
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
 * This file implements the Array class.
 */

#include "array.h"
#include "utils.h"
#include <cassert>
#include <cstring>
#include <iostream>
#include <sstream>
#include <sys/time.h>
#include <unistd.h>

/* ****************************** */
/*             MACROS             */
/* ****************************** */

#if VERBOSE == 1
#  define PRINT_ERROR(x) std::cerr << "[TileDB] Error: " << x << ".\n" 
#  define PRINT_WARNING(x) std::cerr << "[TileDB] Warning: " \
                                     << x << ".\n"
#elif VERBOSE == 2
#  define PRINT_ERROR(x) std::cerr << "[TileDB::Array] Error: " \
                                   << x << ".\n" 
#  define PRINT_WARNING(x) std::cerr << "[TileDB::Array] Warning: " \
                                     << x << ".\n"
#else
#  define PRINT_ERROR(x) do { } while(0) 
#  define PRINT_WARNING(x) do { } while(0) 
#endif

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Array::Array() {
  array_schema_ = NULL;
  range_ = NULL;
}

Array::~Array() {
  if(array_schema_ != NULL)
    delete array_schema_;

  if(range_ != NULL)
    free(range_);
}

/* ****************************** */
/*            MUTATORS            */
/* ****************************** */

const ArraySchema* Array::array_schema() const {
  return array_schema_;
}

const std::vector<int>& Array::attribute_ids() const {
  return attribute_ids_;
}

int Array::mode() const {
  return mode_;
}

const void* Array::range() const {
  return range_;
}

/* ****************************** */
/*            MUTATORS            */
/* ****************************** */

int Array::init(
    const ArraySchema* array_schema,
    int mode,
    const char** attributes,
    int attribute_num,
    const void* range) {
  // Sanity check on mode
  if(mode != TILEDB_READ &&
     mode != TILEDB_READ_REVERSE &&
     mode != TILEDB_WRITE &&
     mode != TILEDB_WRITE_UNSORTED) {
    PRINT_ERROR("Cannot initialize array; Invalid array mode");
    return TILEDB_AR_ERR;
  }

  // Get attributes
  std::vector<std::string> attributes_vec;
  if(attributes == NULL) { // Default: all attributes
    attributes_vec = array_schema->attributes();
  } else {
    for(int i=0; i<attribute_num; ++i) {
      attributes_vec.push_back(attributes[i]);
    }
    // Sanity check on duplicates 
    if(has_duplicates(attributes_vec)) {
      PRINT_ERROR("Cannot initialize array; Duplicate attributes");
      return TILEDB_AR_ERR;
    }
  }
  
  // Set array schema
  array_schema_ = array_schema;

  // Set attribute ids
  if(array_schema->get_attribute_ids(attributes_vec, attribute_ids_) 
         == TILEDB_AS_ERR)
    return TILEDB_AR_ERR;

  // Set mode
  mode_ = mode;

  // Set range
  if(range == NULL) {
    range_ = NULL;
  } else {
    size_t range_size = 2*array_schema->coords_size();
    range_ = malloc(range_size);
    memcpy(range_, range, range_size);
  }
}

int Array::finalize() {
  return TILEDB_AR_OK; 
}

int Array::write(const void** buffers, const size_t* buffer_sizes) {
  // Sanity checks
  assert(mode_ == TILEDB_WRITE || mode_ == TILEDB_WRITE_UNSORTED);
  assert(fragments_.size() <= 1);

  // Initialize new fragment if needed
  if(fragments_.size() == 0)
    fragments_.push_back(new Fragment(new_fragment_name(), this));

  // Dispatch the write command to the new fragment
  int rc = fragments_[0]->write(buffers, buffer_sizes);

  // Return
  if(rc == TILEDB_FG_OK)
    return TILEDB_AR_OK;
  else
    return TILEDB_AR_ERR;
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

std::string Array::new_fragment_name() const {
  std::stringstream fragment_name;
  struct timeval tp;
  gettimeofday(&tp, NULL);
  uint64_t ms = tp.tv_sec * 1000 + tp.tv_usec / 1000;
  fragment_name << array_schema_->array_name() << "/.__" 
                << getpid() << "_" << ms;

  return fragment_name.str();
}
