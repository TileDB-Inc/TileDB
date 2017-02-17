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
#include <sys/time.h>
#include <sys/syscall.h>
#include <unistd.h>

/* ****************************** */
/*             MACROS             */
/* ****************************** */

#ifdef TILEDB_VERBOSE
#  define PRINT_ERROR(x) std::cerr << TILEDB_AR_ERRMSG << x << ".\n" 
#else
#  define PRINT_ERROR(x) do { } while(0) 
#endif




/* ****************************** */
/*        GLOBAL VARIABLES        */
/* ****************************** */

std::string tiledb_ar_errmsg = "";




/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Array::Array() {
  array_read_state_ = NULL;
  array_sorted_read_state_ = NULL;
  array_sorted_write_state_ = NULL;
  array_schema_ = NULL;
  subarray_ = NULL;
  aio_thread_created_ = false;
  array_clone_ = NULL;
}

Array::~Array() {
  // Applicable to both arrays and array clones
  std::vector<Fragment*>::iterator it = fragments_.begin();
  for(; it != fragments_.end(); ++it)
    if(*it != NULL)
       delete *it;
  if(array_read_state_ != NULL)
    delete array_read_state_;
  if(array_sorted_read_state_ != NULL)
    delete array_sorted_read_state_;
  if(array_sorted_write_state_ != NULL)
    delete array_sorted_write_state_;

  // Applicable only to non-clones
  if(array_clone_ != NULL) {
    delete array_clone_;
    if(array_schema_ != NULL)
      delete array_schema_;
    if(subarray_ != NULL)
      free(subarray_);
  }
}




/* ****************************** */
/*           ACCESSORS            */
/* ****************************** */

void Array::aio_handle_requests() {
  // Holds the next AIO request
  AIO_Request* aio_next_request;

  // Initiate infinite loop
  for(;;) {
    // Lock AIO mutext
    if(pthread_mutex_lock(&aio_mtx_)) {
      std::string errmsg = "Cannot lock AIO mutex";
      PRINT_ERROR(errmsg);
      tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
      return;
    } 

    // If the thread is canceled, unblock and exit
    if(aio_thread_canceled_) {
      if(pthread_mutex_unlock(&aio_mtx_)) 
        PRINT_ERROR("Cannot unlock AIO mutex while canceling AIO thread");
      else 
        aio_thread_created_ = false;
      return;
    }

    // Wait for AIO requests
    while(aio_queue_.size() == 0) {
      // Wait to be signaled
      if(pthread_cond_wait(&aio_cond_, &aio_mtx_)) {
        PRINT_ERROR("Cannot wait on AIO mutex condition");
        return;
      }

      // If the thread is canceled, unblock and exit
      if(aio_thread_canceled_) {
        if(pthread_mutex_unlock(&aio_mtx_)) { 
          std::string errmsg = 
              "Cannot unlock AIO mutex while canceling AIO thread";
          PRINT_ERROR(errmsg);
          tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
        } else { 
          aio_thread_created_ = false;
        }
        return;
      }
    }
    
    // Pop the next AIO request 
    aio_next_request = aio_queue_.front(); 
    aio_queue_.pop();

    // Unlock AIO mutext
    if(pthread_mutex_unlock(&aio_mtx_)) {
      std::string errmsg = "Cannot unlock AIO mutex";
      PRINT_ERROR(errmsg);
      tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
      return;
    }

    // Handle the next AIO request
    aio_handle_next_request(aio_next_request);

    // Set last handled AIO request
    aio_last_handled_request_ = aio_next_request->id_;
  }
}

int Array::aio_read(AIO_Request* aio_request) {
  // Sanity checks
  if(!read_mode()) {
    std::string errmsg = "Cannot (async) read from array; Invalid mode";
    PRINT_ERROR(errmsg);
    tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
    return TILEDB_AR_ERR;
  }

  // Create the AIO thread if not already done
  if(!aio_thread_created_)
    if(aio_thread_create() != TILEDB_AR_OK) 
      return TILEDB_ERR;

  // Push the AIO request in the queue
  if(aio_push_request(aio_request) != TILEDB_AR_OK)
    return TILEDB_AR_ERR; 

  // Success
  return TILEDB_AR_OK;
}

int Array::aio_write(AIO_Request* aio_request) {
  // Sanity checks
  if(!write_mode()) {
    std::string errmsg = "Cannot (async) write to array; Invalid mode";
    PRINT_ERROR(errmsg);
    tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
    return TILEDB_AR_ERR;
  }

  // Create the AIO thread if not already done
  if(!aio_thread_created_)
    if(aio_thread_create() != TILEDB_AR_OK) 
      return TILEDB_ERR;

  // Push the AIO request in the queue
  if(aio_push_request(aio_request) != TILEDB_AR_OK)
    return TILEDB_AR_ERR; 

  // Success
  return TILEDB_AR_OK;
}

Array* Array::array_clone() const {
  return array_clone_;
}

const ArraySchema* Array::array_schema() const {
  return array_schema_;
}

const std::vector<int>& Array::attribute_ids() const {
  return attribute_ids_;
}

const StorageManagerConfig* Array::config() const {
  return config_;
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

bool Array::overflow() const {
  // Not applicable to writes
  if(!read_mode()) 
    return false;

  // Check overflow
  if(array_sorted_read_state_ != NULL)
     return array_sorted_read_state_->overflow();
  else
    return array_read_state_->overflow();
}

bool Array::overflow(int attribute_id) const {
  assert(read_mode());

  // Trivial case
  if(fragments_.size() == 0)
    return false;

  // Check overflow
  if(array_sorted_read_state_ != NULL)
    return array_sorted_read_state_->overflow(attribute_id);
  else
    return array_read_state_->overflow(attribute_id);
}

int Array::read(void** buffers, size_t* buffer_sizes) {
  // Sanity checks
  if(!read_mode()) {
    std::string errmsg = "Cannot read from array; Invalid mode";
    PRINT_ERROR(errmsg);
    tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
    return TILEDB_AR_ERR;
  }

  // Check if there are no fragments 
  int buffer_i = 0;
  int attribute_id_num = attribute_ids_.size();
  if(fragments_.size() == 0) {             
    for(int i=0; i<attribute_id_num; ++i) {
      // Update all sizes to 0
      buffer_sizes[buffer_i] = 0; 
      if(!array_schema_->var_size(attribute_ids_[i])) 
        ++buffer_i;
      else 
        buffer_i += 2;
    }
    return TILEDB_AR_OK;
  }

  // Handle sorted modes
  if(mode_ == TILEDB_ARRAY_READ_SORTED_COL ||
     mode_ == TILEDB_ARRAY_READ_SORTED_ROW) { 
      if(array_sorted_read_state_->read(buffers, buffer_sizes) == 
         TILEDB_ASRS_OK) {
        return TILEDB_AR_OK;
      } else {
        tiledb_ar_errmsg = tiledb_asrs_errmsg;
        return TILEDB_AR_ERR;
      }
  } else { // mode_ == TILDB_ARRAY_READ 
    return read_default(buffers, buffer_sizes);
  }
}

int Array::read_default(void** buffers, size_t* buffer_sizes) {
  if(array_read_state_->read(buffers, buffer_sizes) != TILEDB_ARS_OK) {
    tiledb_ar_errmsg = tiledb_ars_errmsg;
    return TILEDB_AR_ERR;
  }

  // Success
  return TILEDB_AR_OK;
}

bool Array::read_mode() const {
  return array_read_mode(mode_);
}

const void* Array::subarray() const {
  return subarray_;
}

bool Array::write_mode() const {
  return array_write_mode(mode_);
}



/* ****************************** */
/*            MUTATORS            */
/* ****************************** */

int Array::consolidate(
    Fragment*& new_fragment,
    std::vector<std::string>& old_fragment_names) {
  // Trivial case
  if(fragments_.size() == 1)
    return TILEDB_AR_OK;

  // Get new fragment name
  std::string new_fragment_name = this->new_fragment_name();
  if(new_fragment_name == "") {
    std::string errmsg = "Cannot produce new fragment name";
    PRINT_ERROR(errmsg);
    tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
    return TILEDB_AR_ERR;
  }

  // Create new fragment
  new_fragment = new Fragment(this);
  if(new_fragment->init(new_fragment_name, TILEDB_ARRAY_WRITE, subarray_) != 
     TILEDB_FG_OK) {
    tiledb_ar_errmsg = tiledb_fg_errmsg;
    return TILEDB_AR_ERR;
  }

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

  // Error
  if(rc_write != TILEDB_FG_OK || rc_read != TILEDB_FG_OK) {
    tiledb_ar_errmsg = tiledb_fg_errmsg;
    return TILEDB_AR_ERR;
  }

  // Success
    return TILEDB_AR_OK;
}

int Array::finalize() {
  // Initializations
  int rc = TILEDB_FG_OK;
  int fragment_num =  fragments_.size();
  bool fg_error = false;
  for(int i=0; i<fragment_num; ++i) {
    rc = fragments_[i]->finalize();
    if(rc != TILEDB_FG_OK)  
      fg_error = true;
    delete fragments_[i];
  }
  fragments_.clear();

  // Clean the array read state
  if(array_read_state_ != NULL) {
    delete array_read_state_;
    array_read_state_ = NULL;
  }

  // Clean the array sorted read state
  if(array_sorted_read_state_ != NULL) {
    delete array_sorted_read_state_;
    array_sorted_read_state_ = NULL;
  }

  // Clean the array sorted write state
  if(array_sorted_write_state_ != NULL) {
    delete array_sorted_write_state_;
    array_sorted_write_state_ = NULL;
  }

  // Clean the AIO-related members
  int rc_aio_thread = aio_thread_destroy(); 
  int rc_aio_cond = TILEDB_AR_OK, rc_aio_mtx = TILEDB_AR_OK;
  if(pthread_cond_destroy(&aio_cond_))
    rc_aio_cond = TILEDB_AR_ERR;
  if(pthread_mutex_destroy(&aio_mtx_))
    rc_aio_mtx = TILEDB_AR_ERR;
  while(aio_queue_.size() != 0) {
    free(aio_queue_.front());
    aio_queue_.pop();
  }

  // Finalize the clone
  int rc_clone = TILEDB_AR_OK; 
  if(array_clone_ != NULL) 
    rc_clone = array_clone_->finalize();

  // Errors
  if(rc != TILEDB_FG_OK) {
    tiledb_ar_errmsg = tiledb_fg_errmsg;
    return TILEDB_AR_ERR;
  }
  if(rc_aio_thread != TILEDB_AR_OK) 
    return TILEDB_AR_ERR;
  if(rc_aio_cond != TILEDB_AR_OK) {
    std::string errmsg = "Cannot destroy AIO mutex condition";
    PRINT_ERROR(errmsg);
    tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
    return TILEDB_AR_ERR;
  }
  if(rc_aio_mtx != TILEDB_AR_OK) {
    std::string errmsg = "Cannot destroy AIO mutex";
    PRINT_ERROR(errmsg);
    tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
    return TILEDB_AR_ERR;
  }
  if(rc_clone != TILEDB_AR_OK)
    return TILEDB_AR_ERR; 
  if(fg_error) 
    return TILEDB_AR_ERR; 

  // Success
  return TILEDB_AR_OK; 
}

int Array::init(
    const ArraySchema* array_schema,
    const std::vector<std::string>& fragment_names,
    const std::vector<BookKeeping*>& book_keeping,
    int mode,
    const char** attributes,
    int attribute_num,
    const void* subarray,
    const StorageManagerConfig* config,
    Array* array_clone) {
  // Set mode
  mode_ = mode;

  // Set array clone
  array_clone_ = array_clone;

  // Sanity check on mode
  if(!read_mode() && !write_mode()) {
    std::string errmsg = "Cannot initialize array; Invalid array mode";
    PRINT_ERROR(errmsg);
    tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
    return TILEDB_AR_ERR;
  }

  // Set config
  config_ = config;

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
    bool coords_found = false;
    bool sparse = !array_schema->dense();
    for(int i=0; i<attribute_num; ++i) {
      // Check attribute name length
      if(attributes[i] == NULL || strlen(attributes[i]) > TILEDB_NAME_MAX_LEN) {
        std::string errmsg = "Invalid attribute name length";
        PRINT_ERROR(errmsg);
        tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
        return TILEDB_AR_ERR;
      }
      attributes_vec.push_back(attributes[i]);
      if(!strcmp(attributes[i], TILEDB_COORDS))
        coords_found = true;
    }

    // Sanity check on duplicates 
    if(has_duplicates(attributes_vec)) {
      std::string errmsg = "Cannot initialize array; Duplicate attributes";
      PRINT_ERROR(errmsg);
      tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
      return TILEDB_AR_ERR;
    }

    // For the case of the clone sparse array, append coordinates if they do
    // not exist already
    if(sparse                   && 
       array_clone == NULL      && 
       !coords_found            && 
       !is_metadata(array_schema->array_name()))
      attributes_vec.push_back(TILEDB_COORDS);
  }

  // Set attribute ids
  if(array_schema->get_attribute_ids(attributes_vec, attribute_ids_) 
         != TILEDB_AS_OK) {
    tiledb_ar_errmsg = tiledb_as_errmsg;
    return TILEDB_AR_ERR;
  }

  // Set array schema
  array_schema_ = array_schema;

  // Initialize new fragment if needed
  if(write_mode()) { // WRITE MODE
    // Get new fragment name
    std::string new_fragment_name = this->new_fragment_name();
    if(new_fragment_name == "") {
      std::string errmsg = "Cannot produce new fragment name";
      PRINT_ERROR(errmsg);
      tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
      return TILEDB_AR_ERR;
    }

    // Create new fragment
    Fragment* fragment = new Fragment(this);
    fragments_.push_back(fragment);
    if(fragment->init(new_fragment_name, mode_, subarray) != TILEDB_FG_OK) {
      array_schema_ = NULL;
      tiledb_ar_errmsg = tiledb_fg_errmsg;
      return TILEDB_AR_ERR;
    }

    // Create ArraySortedWriteState
    if(mode_ == TILEDB_ARRAY_WRITE_SORTED_COL ||
       mode_ == TILEDB_ARRAY_WRITE_SORTED_ROW) { 
      array_sorted_write_state_ = new ArraySortedWriteState(this);
      if(array_sorted_write_state_->init() != TILEDB_ASWS_OK) {
        tiledb_ar_errmsg = tiledb_asws_errmsg;
        delete array_sorted_write_state_;
        array_sorted_write_state_ = NULL;
        return TILEDB_AR_ERR; 
      }
    } else {
      array_sorted_write_state_ = NULL;
    }
  } else {           // READ MODE
    // Open fragments
    if(open_fragments(fragment_names, book_keeping) != TILEDB_AR_OK) {
      array_schema_ = NULL;
      return TILEDB_AR_ERR;
    }
    
    // Create ArrayReadState
    array_read_state_ = new ArrayReadState(this);

    // Create ArraySortedReadState
    if(mode_ != TILEDB_ARRAY_READ) { 
      array_sorted_read_state_ = new ArraySortedReadState(this);
      if(array_sorted_read_state_->init() != TILEDB_ASRS_OK) {
        tiledb_ar_errmsg = tiledb_asrs_errmsg;
        delete array_sorted_read_state_;
        array_sorted_read_state_ = NULL;
        return TILEDB_AR_ERR; 
      }
    } else {
      array_sorted_read_state_ = NULL;
    }
  } 

  // Initialize the AIO-related members
  aio_cond_ = PTHREAD_COND_INITIALIZER; 
  if(pthread_mutex_init(&aio_mtx_, NULL)) {
    std::string errmsg = "Cannot initialize AIO mutex";
    PRINT_ERROR(errmsg);
    tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
    return TILEDB_AR_ERR;
  }
  if(pthread_cond_init(&aio_cond_, NULL)) {
    std::string errmsg = "Cannot initialize AIO mutex condition";
    PRINT_ERROR(errmsg);
    tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
    return TILEDB_AR_ERR;
  }
  aio_thread_canceled_ = false;
  aio_thread_created_ = false;
  aio_last_handled_request_ = -1;

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
        std::string errmsg = "Invalid attribute name length";
        PRINT_ERROR(errmsg);
        tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
        return TILEDB_AR_ERR;
      }
      attributes_vec.push_back(attributes[i]);
    }

    // Sanity check on duplicates 
    if(has_duplicates(attributes_vec)) {
      std::string errmsg = "Cannot reset attributes; Duplicate attributes";
      PRINT_ERROR(errmsg);
      tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
      return TILEDB_AR_ERR;
    }
  }

  // Set attribute ids
  if(array_schema_->get_attribute_ids(attributes_vec, attribute_ids_) 
         != TILEDB_AS_OK)
    tiledb_ar_errmsg = tiledb_as_errmsg;
    return TILEDB_AR_ERR;

  // Reset subarray so that the read/write states are flushed
  if(reset_subarray(subarray_) != TILEDB_AR_OK) 
    return TILEDB_AR_ERR;

  // Success
  return TILEDB_AR_OK;
}

int Array::reset_subarray(const void* subarray) {
  // Sanity check
  assert(read_mode() || write_mode());

  // For easy referencd
  int fragment_num =  fragments_.size();

  // Finalize fragments if in write mode
  if(write_mode()) {  
    // Finalize and delete fragments     
    for(int i=0; i<fragment_num; ++i) { 
      fragments_[i]->finalize();
      delete fragments_[i];
    }
    fragments_.clear();
  } 

  // Set subarray
  size_t subarray_size = 2*array_schema_->coords_size();
  if(subarray_ == NULL) 
    subarray_ = malloc(subarray_size);
  if(subarray == NULL) 
    memcpy(subarray_, array_schema_->domain(), subarray_size);
  else 
    memcpy(subarray_, subarray, subarray_size);

  // Re-set or re-initialize fragments
  if(write_mode()) {  // WRITE MODE 
    // Finalize last fragment
    if(fragments_.size() != 0) {
      assert(fragments_.size() == 1);
      if(fragments_[0]->finalize() != TILEDB_FG_OK)
        tiledb_ar_errmsg = tiledb_fg_errmsg;
        return TILEDB_AR_ERR;
      delete fragments_[0];
      fragments_.clear();
    }

    // Re-initialize ArraySortedWriteState
    if(array_sorted_write_state_ != NULL) 
      delete array_sorted_write_state_;
    if(mode_ == TILEDB_ARRAY_WRITE_SORTED_COL ||
       mode_ == TILEDB_ARRAY_WRITE_SORTED_ROW) { 
      array_sorted_write_state_ = new ArraySortedWriteState(this);
      if(array_sorted_write_state_->init() != TILEDB_ASWS_OK) {
        tiledb_ar_errmsg = tiledb_asws_errmsg;
        delete array_sorted_write_state_;
        array_sorted_write_state_ = NULL;
        return TILEDB_AR_ERR; 
      }
    } else {
      array_sorted_write_state_ = NULL;
    }

    // Get new fragment name
    std::string new_fragment_name = this->new_fragment_name();
    if(new_fragment_name == "") {
      std::string errmsg = "Cannot generate new fragment name";
      PRINT_ERROR(errmsg);
      tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
      return TILEDB_AR_ERR;
    }

    // Create new fragment
    Fragment* fragment = new Fragment(this);
    fragments_.push_back(fragment);
    if(fragment->init(new_fragment_name, mode_, subarray) != TILEDB_FG_OK) { 
      tiledb_ar_errmsg = tiledb_fg_errmsg;
      return TILEDB_AR_ERR;
    }
  } else {           // READ MODE
    // Re-initialize the read state of the fragments
    for(int i=0; i<fragment_num; ++i) 
      fragments_[i]->reset_read_state();

    // Re-initialize array read state
    if(array_read_state_ != NULL) {
      delete array_read_state_;
      array_read_state_ = NULL;
    }
    array_read_state_ = new ArrayReadState(this);

    // Re-initialize ArraySortedReadState
    if(array_sorted_read_state_ != NULL) 
      delete array_sorted_read_state_;
    if(mode_ != TILEDB_ARRAY_READ) { 
      array_sorted_read_state_ = new ArraySortedReadState(this);
      if(array_sorted_read_state_->init() != TILEDB_ASRS_OK) {
      tiledb_ar_errmsg = tiledb_asrs_errmsg;
        delete array_sorted_read_state_;
        array_sorted_read_state_ = NULL;
        return TILEDB_AR_ERR; 
      }
    } else {
      array_sorted_read_state_ = NULL;
    }
  }

  // Success
  return TILEDB_AR_OK;
}

int Array::reset_subarray_soft(const void* subarray) {
  // Sanity check
  assert(read_mode() || write_mode());

  // For easy referencd
  int fragment_num =  fragments_.size();

  // Finalize fragments if in write mode
  if(write_mode()) {  
    // Finalize and delete fragments     
    for(int i=0; i<fragment_num; ++i) { 
      fragments_[i]->finalize();
      delete fragments_[i];
    }
    fragments_.clear();
  } 

  // Set subarray
  size_t subarray_size = 2*array_schema_->coords_size();
  if(subarray_ == NULL) 
    subarray_ = malloc(subarray_size);
  if(subarray == NULL) 
    memcpy(subarray_, array_schema_->domain(), subarray_size);
  else 
    memcpy(subarray_, subarray, subarray_size);

  // Re-set or re-initialize fragments
  if(write_mode()) {  // WRITE MODE 
    // Do nothing
  } else {            // READ MODE
    // Re-initialize the read state of the fragments
    for(int i=0; i<fragment_num; ++i) 
      fragments_[i]->reset_read_state();

    // Re-initialize array read state
    if(array_read_state_ != NULL) {
      delete array_read_state_;
      array_read_state_ = NULL;
    }
    array_read_state_ = new ArrayReadState(this);
  }

  // Success
  return TILEDB_AR_OK;
}

int Array::sync() {
  // Sanity check
  if(!write_mode()) {
    std::string errmsg = "Cannot sync array; Invalid mode";
    PRINT_ERROR(errmsg);
    tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
    return TILEDB_AR_ERR;
  }

  // Sanity check
  assert(fragments_.size() == 1);

  // Sync fragment
  if(fragments_[0]->sync() != TILEDB_FG_OK) {
    tiledb_ar_errmsg = tiledb_fg_errmsg;
    return TILEDB_AR_ERR;
  } else {
    return TILEDB_AR_OK;
  }
}

int Array::sync_attribute(const std::string& attribute) {
  // Sanity checks
  if(!write_mode()) {
    std::string errmsg = "Cannot sync attribute; Invalid mode";
    PRINT_ERROR(errmsg);
    tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
    return TILEDB_AR_ERR;
  }

  // Sanity check
  assert(fragments_.size() == 1);

  // Sync fragment
  if(fragments_[0]->sync_attribute(attribute) != TILEDB_FG_OK) {
    tiledb_ar_errmsg = tiledb_fg_errmsg;
    return TILEDB_AR_ERR;
  } else {
    return TILEDB_AR_OK;
  }
}

int Array::write(const void** buffers, const size_t* buffer_sizes) {
  // Sanity checks
  if(!write_mode()) {
    std::string errmsg = "Cannot write to array; Invalid mode";
    PRINT_ERROR(errmsg);
    tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
    return TILEDB_AR_ERR;
  }

  // Write based on mode
  int rc;
  if(mode_ == TILEDB_ARRAY_WRITE_SORTED_COL ||
     mode_ == TILEDB_ARRAY_WRITE_SORTED_ROW) { 
    rc = array_sorted_write_state_->write(buffers, buffer_sizes); 
  } else if(mode_ == TILEDB_ARRAY_WRITE ||
            mode_ == TILEDB_ARRAY_WRITE_UNSORTED) { 
    rc = write_default(buffers, buffer_sizes);
  } else {
    assert(0);
  }

  // Handle error
  if(rc != TILEDB_ASWS_OK) {
    tiledb_ar_errmsg = tiledb_asws_errmsg;
    return TILEDB_AR_ERR;
  }

  // In all modes except TILEDB_ARRAY_WRITE, the fragment must be finalized
  if(mode_ != TILEDB_ARRAY_WRITE) {
    if(fragments_[0]->finalize() != TILEDB_FG_OK) {
      tiledb_ar_errmsg = tiledb_fg_errmsg;
      return TILEDB_AR_ERR;
    }
    delete fragments_[0];
    fragments_.clear();
  }

  // Success
  return TILEDB_AR_OK;
}

int Array::write_default(const void** buffers, const size_t* buffer_sizes) {
  // Sanity checks
  if(!write_mode()) {
    std::string errmsg = "Cannot write to array; Invalid mode";
    PRINT_ERROR(errmsg);
    tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
    return TILEDB_AR_ERR;
  }

  // Create and initialize a new fragment 
  if(fragments_.size() == 0) {
    // Get new fragment name
    std::string new_fragment_name = this->new_fragment_name();
    if(new_fragment_name == "") {
      std::string errmsg = "Cannot produce new fragment name";
      PRINT_ERROR(errmsg);
      tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
      return TILEDB_AR_ERR;
    }
   
    // Create new fragment 
    Fragment* fragment = new Fragment(this);
    fragments_.push_back(fragment);
    if(fragment->init(new_fragment_name, mode_, subarray_) != TILEDB_FG_OK) {
      tiledb_ar_errmsg = tiledb_fg_errmsg;
      return TILEDB_AR_ERR;
    }
  }
 
  // Dispatch the write command to the new fragment
  if(fragments_[0]->write(buffers, buffer_sizes) != TILEDB_FG_OK) {
    tiledb_ar_errmsg = tiledb_fg_errmsg;
    return TILEDB_AR_ERR; 
  }

  // Success
  return TILEDB_AR_OK;
}




/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

void Array::aio_handle_next_request(AIO_Request* aio_request) {
  int rc = TILEDB_AR_OK;
  if(read_mode()) {   // READ MODE
    // Invoke the read
    if(aio_request->mode_ == TILEDB_ARRAY_READ) { 
      // Reset the subarray only if this request does not continue from the last
      if(aio_last_handled_request_ != aio_request->id_)
        reset_subarray_soft(aio_request->subarray_);

      // Read 
      rc = read_default(aio_request->buffers_, aio_request->buffer_sizes_);
    } else {  
      // This may initiate a series of new AIO requests
      // Reset the subarray hard this time (updating also the subarray
      // of the ArraySortedReadState object.
      if(aio_last_handled_request_ != aio_request->id_)
        reset_subarray(aio_request->subarray_);

      // Read 
      rc = read(aio_request->buffers_, aio_request->buffer_sizes_);
    }
  } else {            // WRITE MODE
    // Invoke the write
    if(aio_request->mode_ == TILEDB_ARRAY_WRITE ||
       aio_request->mode_ == TILEDB_ARRAY_WRITE_UNSORTED) { 
      // Reset the subarray only if this request does not continue from the last
      if(aio_last_handled_request_ != aio_request->id_)
        reset_subarray_soft(aio_request->subarray_);

      // Write
      rc = write_default(
               (const void**) aio_request->buffers_, 
               (const size_t*) aio_request->buffer_sizes_);
    } else {  
      // This may initiate a series of new AIO requests
      // Reset the subarray hard this time (updating also the subarray
      // of the ArraySortedWriteState object.
      if(aio_last_handled_request_ != aio_request->id_)
        reset_subarray(aio_request->subarray_);
     
      // Write
      rc = write(
               (const void**) aio_request->buffers_, 
               (const size_t*) aio_request->buffer_sizes_);
    }
  }

  if(rc == TILEDB_AR_OK) {      // Success
    // Check for overflow (applicable only to reads)
    if(aio_request->mode_ == TILEDB_ARRAY_READ && 
       array_read_state_->overflow()) {
      *aio_request->status_= TILEDB_AIO_OVERFLOW;
      if(aio_request->overflow_ != NULL) {
        for(int i=0; i<int(attribute_ids_.size()); ++i) 
          aio_request->overflow_[i] = 
            array_read_state_->overflow(attribute_ids_[i]);
      }
    } else if((aio_request->mode_ == TILEDB_ARRAY_READ_SORTED_COL ||
               aio_request->mode_ == TILEDB_ARRAY_READ_SORTED_ROW ) && 
              array_sorted_read_state_->overflow()) {
      *aio_request->status_= TILEDB_AIO_OVERFLOW;
      if(aio_request->overflow_ != NULL) {
        for(int i=0; i<int(attribute_ids_.size()); ++i) 
          aio_request->overflow_[i] = 
              array_sorted_read_state_->overflow(attribute_ids_[i]);
      }
    } else { // Completion
      *aio_request->status_= TILEDB_AIO_COMPLETED;
    }

    // Invoke the callback
    if(aio_request->completion_handle_ != NULL) 
      (*(aio_request->completion_handle_))(aio_request->completion_data_);
  } else {                      // Error
    *aio_request->status_= TILEDB_AIO_ERR;
  }
}

void *Array::aio_handler(void* context) {
  // This will enter an indefinite loop that will handle all incoming AIO 
  // requests
  ((Array*) context)->aio_handle_requests();

  // Return
  return NULL;
}

int Array::aio_push_request(AIO_Request* aio_request) {
  // Set the request status
  *aio_request->status_ = TILEDB_AIO_INPROGRESS;

  // Lock AIO mutex
  if(pthread_mutex_lock(&aio_mtx_)) {
    std::string errmsg = "Cannot lock AIO mutex";
    PRINT_ERROR(errmsg);
    tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
    return TILEDB_AR_ERR;
  }

  // Push request
  aio_queue_.push(aio_request);

  // Signal AIO thread
  if(pthread_cond_signal(&aio_cond_)) { 
    std::string errmsg = "Cannot signal AIO thread";
    PRINT_ERROR(errmsg);
    tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
    return TILEDB_AR_ERR;
  }

  // Unlock AIO mutext
  if(pthread_mutex_unlock(&aio_mtx_)) {
    std::string errmsg = "Cannot unlock AIO mutex";
    PRINT_ERROR(errmsg);
    tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
    return TILEDB_AR_ERR;
  }

  // Success
  return TILEDB_AR_OK;
}

int Array::aio_thread_create() {
  // Trivial case
  if(aio_thread_created_)
    return TILEDB_AR_OK;

  // Create the thread that will be handling all AIO requests
  int rc;
  if((rc=pthread_create(&aio_thread_, NULL, Array::aio_handler, this))) {
    std::string errmsg = "Cannot create AIO thread";
    PRINT_ERROR(errmsg);
    tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
    return TILEDB_AR_ERR;
  }

  aio_thread_created_ = true;

  // Success
  return TILEDB_AR_OK;
}

int Array::aio_thread_destroy() {
  // Trivial case
  if(!aio_thread_created_)
    return TILEDB_AR_OK;

  // Lock AIO mutext
  if(pthread_mutex_lock(&aio_mtx_)) {
    std::string errmsg = "Cannot lock AIO mutex while destroying AIO thread";
    PRINT_ERROR(errmsg);
    tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
    return TILEDB_AR_ERR;
  }

  // Signal the cancelation so that the thread unblocks
  aio_thread_canceled_ = true;
  if(pthread_cond_signal(&aio_cond_)) { 
    std::string errmsg = "Cannot signal AIO thread while destroying AIO thread";
    PRINT_ERROR(errmsg);
    tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
    return TILEDB_AR_ERR;
  }

  // Unlock AIO mutext
  if(pthread_mutex_unlock(&aio_mtx_)) {
    std::string errmsg = "Cannot unlock AIO mutex while destroying AIO thread";
    PRINT_ERROR(errmsg);
    tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
    return TILEDB_AR_ERR;
  }

  // Wait for cancelation to take place
  while(aio_thread_created_);

  // Join with the terminated thread
  if(pthread_join(aio_thread_, NULL)) {
    std::string errmsg = "Cannot join AIO thread";
    PRINT_ERROR(errmsg);
    tiledb_ar_errmsg = TILEDB_AR_ERRMSG + errmsg;
    return TILEDB_AR_ERR;
  }

  // Success
  return TILEDB_AR_OK;
}

std::string Array::new_fragment_name() const {
  struct timeval tp;
  gettimeofday(&tp, NULL);
  uint64_t ms = (uint64_t) tp.tv_sec * 1000L + tp.tv_usec / 1000;
  pthread_t self = pthread_self();
  uint64_t tid = 0;
  memcpy(&tid, &self, std::min(sizeof(self), sizeof(tid)));
  char fragment_name[TILEDB_NAME_MAX_LEN];

  // Get MAC address
  std::string mac = get_mac_addr();
  if(mac == "")
    return "";

  // Generate fragment name
  int n = sprintf(
              fragment_name, 
              "%s/.__%s%llu_%llu", 
              array_schema_->array_name().c_str(), 
              mac.c_str(),
              tid, 
              ms);

  // Handle error
  if(n<0) 
    return "";

  // Return
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

    if(fragment->init(fragment_names[i], book_keeping[i]) != TILEDB_FG_OK) {
      tiledb_ar_errmsg = tiledb_fg_errmsg;
      return TILEDB_AR_ERR;
    }
  } 

  // Success
  return TILEDB_AR_OK;
}

