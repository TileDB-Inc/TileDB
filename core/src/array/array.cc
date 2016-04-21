/**
 * @file   array.cc
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
 * This file implements the Array class.
 */

#include "array.h"
#include "utils.h"
#include <algorithm>
#include <cassert>
#include <cstring>
#include <iostream>
#include <omp.h>
#include <sys/time.h>
#include <sys/syscall.h>
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
  array_read_state_ = NULL;
  array_schema_ = NULL;
  subarray_ = NULL;
}

Array::~Array() {
  std::vector<Fragment*>::iterator it = fragments_.begin();
  for(; it != fragments_.end(); ++it)
    if(*it != NULL)
       delete *it;

  if(array_schema_ != NULL)
    delete array_schema_;

  if(subarray_ != NULL)
    free(subarray_);

  if(array_read_state_ != NULL)
    delete array_read_state_;
}




/* ****************************** */
/*           ACCESSORS            */
/* ****************************** */

const ArraySchema* Array::array_schema() const {
  return array_schema_;
}

const std::vector<int>& Array::attribute_ids() const {
  return attribute_ids_;
}

int Array::fragment_num() const {
  return fragments_.size();
}

std::vector<Fragment*> Array::fragments() const {
  return fragments_;
}

int Array::mode() const {
  return mode_;
}

bool Array::overflow(int attribute_id) const {
  assert(mode_ == TILEDB_ARRAY_READ);

  // Trivial case
  if(fragments_.size() == 0)
    return false;
 
  // Check the array read state
  return array_read_state_->overflow(attribute_id);
}

int Array::read(void** buffers, size_t* buffer_sizes) {
  // Sanity checks
  if(mode_ != TILEDB_ARRAY_READ) {
    PRINT_ERROR("Cannot read from array; Invalid mode");
    return TILEDB_AR_ERR;
  }

  int buffer_i = 0;
  int attribute_id_num = attribute_ids_.size();
  bool success = false;
  if(fragments_.size() == 0) {             // No fragments - read nothing
    for(int i=0; i<attribute_id_num; ++i) {
      buffer_sizes[buffer_i] = 0; 
      if(!array_schema_->var_size(attribute_ids_[i])) 
        ++buffer_i;
      else 
        buffer_i += 2;
    }
    success = true;
    } else {
    if(array_read_state_->read(buffers, buffer_sizes) == 
       TILEDB_ARS_OK)
      success = true;
  }

  // Return
  if(success)
    return TILEDB_AR_OK;
  else
    return TILEDB_AR_ERR;
}

const void* Array::subarray() const {
  return subarray_;
}




/* ****************************** */
/*            MUTATORS            */
/* ****************************** */

int Array::consolidate(
    Fragment*& new_fragment,
    std::vector<std::string>& old_fragment_names) {
  // Trivial case
  if(fragments_.size() == 1)
    return TILEDB_AS_OK;

  // Get new fragment name
  std::string new_fragment_name = this->new_fragment_name();
  if(new_fragment_name == "")
    return TILEDB_AS_ERR;

  // Create new fragment
  new_fragment = new Fragment(this);
  if(new_fragment->init(new_fragment_name, TILEDB_ARRAY_WRITE, subarray_) != 
     TILEDB_FG_OK)
    return TILEDB_AR_ERR;

  // Consolidate on a per-attribute basis
  for(int i=0; i<array_schema_->attribute_num()+1; ++i) {
    if(consolidate(new_fragment, i) != TILEDB_AR_OK) {
      delete_dir(new_fragment->fragment_name());
      delete new_fragment;
      return TILEDB_AR_ERR;
    }
  }

  // Get old fragment names
  int fragment_num = fragments_.size();
  for(int i=0; i<fragment_num; ++i) 
    old_fragment_names.push_back(fragments_[i]->fragment_name());

  // Success
  return TILEDB_AR_OK;
}

int Array::consolidate(
    Fragment* new_fragment,
    int attribute_id) {
  // For easy reference
  int attribute_num = array_schema_->attribute_num();

  // Do nothing if the array is dense for the coordinates attribute
  if(array_schema_->dense() && attribute_id == attribute_num)
    return TILEDB_AR_OK;

  // Prepare buffers
  void** buffers;
  size_t* buffer_sizes;

  // Count the number of variable attributes
  int var_attribute_num = array_schema_->var_attribute_num();

  // Populate the buffers
  int buffer_num = attribute_num + 1 + var_attribute_num;
  buffers = (void**) malloc(buffer_num * sizeof(void*));
  buffer_sizes = (size_t*) malloc(buffer_num * sizeof(size_t));
  int buffer_i = 0;
  for(int i=0; i<attribute_num+1; ++i) {
    if(i == attribute_id) {
      buffers[buffer_i] = malloc(TILEDB_CONSOLIDATION_BUFFER_SIZE);
      buffer_sizes[buffer_i] = TILEDB_CONSOLIDATION_BUFFER_SIZE;
      ++buffer_i;
      if(array_schema_->var_size(i)) {
        buffers[buffer_i] = malloc(TILEDB_CONSOLIDATION_BUFFER_SIZE);
        buffer_sizes[buffer_i] = TILEDB_CONSOLIDATION_BUFFER_SIZE;
        ++buffer_i;
      }
    } else {
      buffers[buffer_i] = NULL;
      buffer_sizes[buffer_i] = 0;
      ++buffer_i;
      if(array_schema_->var_size(i)) {
        buffers[buffer_i] = NULL;
        buffer_sizes[buffer_i] = 0;
        ++buffer_i;
      }
    }
  }

  // Read and write attribute until there is no overflow
  int rc_write = TILEDB_FG_OK; 
  int rc_read = TILEDB_FG_OK; 
  do {
    // Read
    rc_read = read(buffers, buffer_sizes);
    if(rc_read != TILEDB_FG_OK)
      break;

    // Write
    rc_write = new_fragment->write(
                   (const void**) buffers, 
                   (const size_t*) buffer_sizes);
    if(rc_write != TILEDB_FG_OK)
      break;
  } while(overflow(attribute_id));

  // Clean up
  for(int i=0; i<buffer_num; ++i) {
    if(buffers[i] != NULL)
      free(buffers[i]);
  } 
  free(buffers);
  free(buffer_sizes);

  // Return
  if(rc_write == TILEDB_FG_OK && rc_read == TILEDB_FG_OK)
    return TILEDB_AR_OK;
  else
    return TILEDB_AR_ERR;
}

int Array::finalize() {
  int rc = TILEDB_FG_OK;
  int fragment_num =  fragments_.size();
  for(int i=0; i<fragment_num; ++i) {
    rc = fragments_[i]->finalize();
    if(rc != TILEDB_FG_OK)
      break;
    delete fragments_[i];
  }
  fragments_.clear();

  if(array_read_state_ != NULL) {
    delete array_read_state_;
    array_read_state_ = NULL;
  }

  if(rc == TILEDB_FG_OK)
    return TILEDB_AR_OK; 
  else
    return TILEDB_AR_ERR; 
}

int Array::init(
    const ArraySchema* array_schema,
    const std::vector<std::string>& fragment_names,
    const std::vector<BookKeeping*>& book_keeping,
    int mode,
    const char** attributes,
    int attribute_num,
    const void* subarray) {
  // Sanity check on mode
  if(mode != TILEDB_ARRAY_READ &&
     mode != TILEDB_ARRAY_WRITE &&
     mode != TILEDB_ARRAY_WRITE_UNSORTED) {
    PRINT_ERROR("Cannot initialize array; Invalid array mode");
    return TILEDB_AR_ERR;
  }

  // Set subarray
  size_t subarray_size = 2*array_schema->coords_size();
  subarray_ = malloc(subarray_size);
  if(subarray == NULL) 
    memcpy(subarray_, array_schema->domain(), subarray_size);
  else 
    memcpy(subarray_, subarray, subarray_size);

  // Get attributes
  std::vector<std::string> attributes_vec;
  if(attributes == NULL) { // Default: all attributes
    attributes_vec = array_schema->attributes();
    if(array_schema->dense() && mode != TILEDB_ARRAY_WRITE_UNSORTED) 
      // Remove coordinates attribute for dense arrays, 
      // unless in TILEDB_WRITE_UNSORTED mode
      attributes_vec.pop_back(); 
  } else {                 // Custom attributes
    // Get attributes
    for(int i=0; i<attribute_num; ++i) {
      // Check attribute name length
      if(attributes[i] == NULL || strlen(attributes[i]) > TILEDB_NAME_MAX_LEN) {
        PRINT_ERROR("Invalid attribute name length");
        return TILEDB_AR_ERR;
      }
      attributes_vec.push_back(attributes[i]);
    }

    // Sanity check on duplicates 
    if(has_duplicates(attributes_vec)) {
      PRINT_ERROR("Cannot initialize array; Duplicate attributes");
      return TILEDB_AR_ERR;
    }
  }
  
  // Set attribute ids
  if(array_schema->get_attribute_ids(attributes_vec, attribute_ids_) 
         == TILEDB_AS_ERR)
    return TILEDB_AR_ERR;

  // Set mode
  mode_ = mode;

  // Set array schema
  array_schema_ = array_schema;

  // Initialize new fragment if needed
  if(mode_ == TILEDB_ARRAY_WRITE || 
     mode_ == TILEDB_ARRAY_WRITE_UNSORTED) {
    // Get new fragment name
    std::string new_fragment_name = this->new_fragment_name();
    if(new_fragment_name == "")
      return TILEDB_AS_ERR;

    // Create new fragment
    Fragment* fragment = new Fragment(this);
    fragments_.push_back(fragment);
    if(fragment->init(new_fragment_name, mode_, subarray) != TILEDB_FG_OK) {
      array_schema_ = NULL;
      return TILEDB_AR_ERR;
    }
  } else if(mode_ == TILEDB_ARRAY_READ) {
    if(open_fragments(fragment_names, book_keeping) != TILEDB_AR_OK) {
      array_schema_ = NULL;
      return TILEDB_AR_ERR;
    }
    array_read_state_ = new ArrayReadState(this);
  }

  // Return
  return TILEDB_AR_OK;
}

int Array::reset_attributes(
    const char** attributes,
    int attribute_num) {
  // Get attributes
  std::vector<std::string> attributes_vec;
  if(attributes == NULL) { // Default: all attributes
    attributes_vec = array_schema_->attributes();
    if(array_schema_->dense()) // Remove coordinates attribute for dense
      attributes_vec.pop_back(); 
  } else {                 //  Custom attributes
    // Copy attribute names
    for(int i=0; i<attribute_num; ++i) {
      // Check attribute name length
      if(attributes[i] == NULL || strlen(attributes[i]) > TILEDB_NAME_MAX_LEN) {
        PRINT_ERROR("Invalid attribute name length");
        return TILEDB_AR_ERR;
      }
      attributes_vec.push_back(attributes[i]);
    }

    // Sanity check on duplicates 
    if(has_duplicates(attributes_vec)) {
      PRINT_ERROR("Cannot reset attributes; Duplicate attributes");
      return TILEDB_AR_ERR;
    }
  }

  // Set attribute ids
  if(array_schema_->get_attribute_ids(attributes_vec, attribute_ids_) 
         == TILEDB_AS_ERR)
    return TILEDB_AR_ERR;

  // Success
  return TILEDB_AR_OK;
}

int Array::reset_subarray(const void* subarray) {
  // Sanity check on mode
  if(mode_ != TILEDB_ARRAY_READ) {
    PRINT_ERROR("Cannot reset subarray; Invalid array mode");
    return TILEDB_AR_ERR;
  }

  // Set subarray
  size_t subarray_size = 2*array_schema_->coords_size();
  if(subarray_ == NULL) 
    subarray_ = malloc(subarray_size);
  if(subarray == NULL) 
    memcpy(subarray_, array_schema_->domain(), subarray_size);
  else 
    memcpy(subarray_, subarray, subarray_size);

  // Re-initialize the read state of the fragments
  int fragment_num =  fragments_.size();
  for(int i=0; i<fragment_num; ++i) 
    fragments_[i]->reset_read_state();

  // Re-initialize array read state
  if(array_read_state_ != NULL) {
    delete array_read_state_;
    array_read_state_ = NULL;
  }
  array_read_state_ = new ArrayReadState(this);

  // Success
  return TILEDB_AR_OK;
}

int Array::write(const void** buffers, const size_t* buffer_sizes) {
  // Sanity checks
  if(mode_ != TILEDB_ARRAY_WRITE && 
     mode_ != TILEDB_ARRAY_WRITE_UNSORTED) {
    PRINT_ERROR("Cannot write to array; Invalid mode");
    return TILEDB_AR_ERR;
  }

  // Create and initialize a new fragment 
  if(fragments_.size() == 0) {
    // Get new fragment name
    std::string new_fragment_name = this->new_fragment_name();
    if(new_fragment_name == "")
      return TILEDB_AS_ERR;
   
    // Create new fragment 
    Fragment* fragment = new Fragment(this);
    fragments_.push_back(fragment);
    if(fragment->init(new_fragment_name, mode_, subarray_) != TILEDB_FG_OK)
      return TILEDB_AR_ERR;
  }
 
  // Dispatch the write command to the new fragment
  if(fragments_[0]->write(buffers, buffer_sizes) != TILEDB_FG_OK)
    return TILEDB_AR_ERR;

  // In WRITE_UNSORTED mode, the fragment must be finalized
  if(mode_ == TILEDB_ARRAY_WRITE_UNSORTED) {
    if(fragments_[0]->finalize() != TILEDB_FG_OK)
      return TILEDB_AR_ERR;
    delete fragments_[0];
    fragments_.clear();
  }

  // Success
  return TILEDB_AR_OK;
}




/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

std::string Array::new_fragment_name() const {
  struct timeval tp;
  gettimeofday(&tp, NULL);
  uint64_t ms = (uint64_t) tp.tv_sec * 1000L + tp.tv_usec / 1000;
  pthread_t self = pthread_self();
  uint64_t tid = 0;
  memcpy(&tid, &self, std::min(sizeof(self), sizeof(tid)));
  char fragment_name[TILEDB_NAME_MAX_LEN];

  int n = sprintf(
              fragment_name, 
              "%s/.__%llu_%llu", 
              array_schema_->array_name().c_str(), 
              tid, 
              ms);
  if(n <0) 
    return "";

  return fragment_name;
}

int Array::open_fragments(
    const std::vector<std::string>& fragment_names,
    const std::vector<BookKeeping*>& book_keeping) {
  // Sanity check
  assert(fragment_names.size() == book_keeping.size());


  // Create a fragment object for each fragment directory
  int fragment_num = fragment_names.size();
  for(int i=0; i<fragment_num; ++i) {
    Fragment* fragment = new Fragment(this);
    fragments_.push_back(fragment);

    if(fragment->init(fragment_names[i], book_keeping[i]) != TILEDB_FG_OK)
      return TILEDB_AR_ERR;
  } 

  // Success
  return TILEDB_AR_OK;
}

