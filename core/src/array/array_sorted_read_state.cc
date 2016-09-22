/**
 * @file   array_sorted_read_state.cc
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
 * This file implements the ArraySortedReadState class.
 */

#include "array_sorted_read_state.h"
#include <cassert>




/* ****************************** */
/*             MACROS             */
/* ****************************** */

#ifdef VERBOSE
#  define PRINT_ERROR(x) std::cerr << TILEDB_ASRS_ERRMSG << x << ".\n" 
#else
#  define PRINT_ERROR(x) do { } while(0) 
#endif




/* ****************************** */
/*         GLOBAL VARIABLES       */
/* ****************************** */

std::string tiledb_asrs_errmsg = "";




/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

ArraySortedReadState::ArraySortedReadState(
    Array* array)
    : array_(array),
      attribute_ids_(array->attribute_ids()),
      subarray_(array->subarray()) {
  // Initializations
  copy_id_ = 0;
  aio_id_ = 0;
  done_ = false;
  resume_copy_ = false;
  resume_aio_ = false;
  for(int i=0; i<2; ++i) {
    buffer_sizes_[i] = NULL;
    buffers_[i] = NULL;
    tile_slab_[i] = NULL;
    wait_copy_[i] = false;
    wait_aio_[i] = true;
  }
  overflow_.resize(attribute_ids_.size());
  for(int i=0; (int) attribute_ids_.size(); ++i)
    overflow_[i] = false;

  // Calculate number of buffers
  calculate_buffer_num();

  // Calculate buffer sizes
  calculate_buffer_sizes();

  // Initialize tile slab info and state
  init_tile_slab_info();
  init_tile_slab_state();
}

ArraySortedReadState::~ArraySortedReadState() { 
  // Clean up
  for(int i=0; i<2; ++i) {
    if(buffer_sizes_[i] != NULL)
      delete [] buffer_sizes_[i];
    if(buffers_[i] != NULL) {
      for(int b=0; b<buffer_num_; ++b)  
        free(buffers_[i][b]);
      free(buffers_[i]);
    }
    if(tile_slab_[i] != NULL)
      free(tile_slab_[i]);
  }

  // Destroy thread, conditions and mutexes
  for(int i=0; i<2; ++i) {
    if(pthread_cond_destroy(&(aio_cond_[i]))) {
      std::string errmsg = "Cannot destroy AIO mutex condition";
      PRINT_ERROR(errmsg);
      tiledb_asrs_errmsg = TILEDB_ASRS_ERRMSG + errmsg;
    }
    if(pthread_cond_destroy(&(copy_cond_[i]))) {
      std::string errmsg = "Cannot destroy copy mutex condition";
      PRINT_ERROR(errmsg);
      tiledb_asrs_errmsg = TILEDB_ASRS_ERRMSG + errmsg;
    }
  }
  if(pthread_cond_destroy(&overflow_cond_)) {
    std::string errmsg = "Cannot destroy overflow mutex condition";
    PRINT_ERROR(errmsg);
    tiledb_asrs_errmsg = TILEDB_ASRS_ERRMSG + errmsg;
  }
  if(pthread_mutex_destroy(&aio_mtx_)) {
    std::string errmsg = "Cannot destroy AIO mutex";
    PRINT_ERROR(errmsg);
    tiledb_asrs_errmsg = TILEDB_ASRS_ERRMSG + errmsg;
  }
  if(pthread_mutex_destroy(&copy_mtx_)) {
    std::string errmsg = "Cannot destroy copy mutex";
    PRINT_ERROR(errmsg);
    tiledb_asrs_errmsg = TILEDB_ASRS_ERRMSG + errmsg;
  }
  if(pthread_mutex_destroy(&overflow_mtx_)) {
    std::string errmsg = "Cannot destroy overflow mutex";
    PRINT_ERROR(errmsg);
    tiledb_asrs_errmsg = TILEDB_ASRS_ERRMSG + errmsg;
  }
  if(pthread_cancel(copy_thread_)) {
    std::string errmsg = "Cannot destroy AIO thread";
    PRINT_ERROR(errmsg);
    tiledb_asrs_errmsg = TILEDB_ASRS_ERRMSG + errmsg;
  }

  // Free tile slab info and state
  free_tile_slab_info();
  free_tile_slab_state();
}




/* ****************************** */
/*           ACCESSORS            */
/* ****************************** */

bool ArraySortedReadState::overflow() const {
  for(int i=0; (int) attribute_ids_.size(); ++i) {
    if(overflow_[i])
      return true;
  }

  return false;
}

int ArraySortedReadState::read(void** buffers, size_t* buffer_sizes) {
  // Trivial case
  if(done_) {
    for(int i=0; i<buffer_num_; ++i)
      buffer_sizes[i] = 0;
    return TILEDB_ASRS_OK;
  }

  // Reset overflow
  reset_overflow();
  
  // Resume the copy request handling
  if(resume_copy_)
    release_overflow();

  // Set local user buffers
  user_buffers_ = buffers;
  user_buffer_sizes_ = buffer_sizes;

  // Call the appropriate templated read
  int type = array_->array_schema()->coords_type();
  if(type == TILEDB_INT32)
    return read<int>();
  else if(type == TILEDB_INT64)
    return read<int64_t>();
  else if(type == TILEDB_FLOAT32)
    return read<float>();
  else if(type == TILEDB_FLOAT64)
    return read<double>();
  else 
    assert(0);
} 




/* ****************************** */
/*            MUTATORS            */
/* ****************************** */

int ArraySortedReadState::init() {
  // Create buffers
  if(create_buffers() != TILEDB_ASRS_OK)
    return TILEDB_ASRS_ERR;

  // Create the thread that will be handling all the copying
  if(pthread_create(
         &copy_thread_, 
         NULL, 
         ArraySortedReadState::copy_handler, 
         this)) {
    std::string errmsg = "Cannot create AIO thread";
    PRINT_ERROR(errmsg);
    tiledb_asrs_errmsg = TILEDB_ASRS_ERRMSG + errmsg; 
    return TILEDB_ASRS_ERR;
  }

  // Initialize the mutexes and conditions
  if(pthread_mutex_init(&aio_mtx_, NULL)) {
       std::string errmsg = "Cannot initialize IO mutex";
      PRINT_ERROR(errmsg);
      tiledb_asrs_errmsg = TILEDB_ASRS_ERRMSG + errmsg; 
      return TILEDB_ASRS_ERR;
  }
  if(pthread_mutex_init(&copy_mtx_, NULL)) {
    std::string errmsg = "Cannot initialize copy mutex";
    PRINT_ERROR(errmsg);
    tiledb_asrs_errmsg = TILEDB_ASRS_ERRMSG + errmsg; 
    return TILEDB_ASRS_ERR;
  }
  if(pthread_mutex_init(&overflow_mtx_, NULL)) {
       std::string errmsg = "Cannot initialize overflow mutex";
      PRINT_ERROR(errmsg);
      tiledb_asrs_errmsg = TILEDB_ASRS_ERRMSG + errmsg; 
      return TILEDB_ASRS_ERR;
  }
  for(int i=0; i<2; ++i) {
    aio_cond_[i] = PTHREAD_COND_INITIALIZER; 
    if(pthread_cond_init(&(aio_cond_[i]), NULL)) {
      std::string errmsg = "Cannot initialize IO mutex condition";
      PRINT_ERROR(errmsg);
      tiledb_asrs_errmsg = TILEDB_ASRS_ERRMSG + errmsg; 
      return TILEDB_ASRS_ERR;
    }
    copy_cond_[i] = PTHREAD_COND_INITIALIZER; 
    if(pthread_cond_init(&(copy_cond_[i]), NULL)) {
      std::string errmsg = "Cannot initialize copy mutex condition";
      PRINT_ERROR(errmsg);
      tiledb_asrs_errmsg = TILEDB_ASRS_ERRMSG + errmsg; 
      return TILEDB_ASRS_ERR;
    }
  }
  overflow_cond_ = PTHREAD_COND_INITIALIZER; 
  if(pthread_cond_init(&overflow_cond_, NULL)) {
    std::string errmsg = "Cannot initialize overflow mutex condition";
    PRINT_ERROR(errmsg);
    tiledb_asrs_errmsg = TILEDB_ASRS_ERRMSG + errmsg; 
    return TILEDB_ASRS_ERR;
  }

  // Success
  return TILEDB_ASRS_OK;
}


/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

void *ArraySortedReadState::aio_done(void* data) {
  ArraySortedReadState* asrs = ((ASRS_Request*) data)->asrs_;
  int id = ((ASRS_Request*) data)->id_;
  asrs->block_copy(id);
  asrs->release_aio(id);
 
  return NULL;
}

void ArraySortedReadState::block_aio(int id) {
  lock_copy_mtx();
  wait_copy_[id] = true; 
  unlock_copy_mtx();
}

void ArraySortedReadState::block_copy(int id) {
  lock_aio_mtx();
  wait_aio_[id] = true; 
  unlock_aio_mtx();
}

void ArraySortedReadState::block_overflow() {
  lock_overflow_mtx();
  resume_copy_ = true; 
  unlock_overflow_mtx();
}

void ArraySortedReadState::calculate_buffer_num() {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();

  // Calculate number of buffers
  buffer_num_ = 0;
  int attribute_id_num = (int) attribute_ids_.size();
  for(int i=0; i<attribute_id_num; ++i) {
    // Fix-sized attribute
    if(!array_schema->var_size(attribute_ids_[i])) 
      ++buffer_num_;
    else // Variable-sized attribute
      buffer_num_ += 2;
  }
}

void ArraySortedReadState::calculate_buffer_sizes() {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();

  // Get cell number in a (full) tile slab
  int64_t tile_slab_cell_num; 
  if(array_->mode() == TILEDB_ARRAY_READ_SORTED_ROW)
    tile_slab_cell_num = array_schema->tile_slab_row_cell_num(subarray_);
  else // TILEDB_ARRAY_READ_SORTED_COL
    tile_slab_cell_num = array_schema->tile_slab_col_cell_num(subarray_);

  // Calculate buffer sizes
  int attribute_id_num = (int) attribute_ids_.size();
  for(int j=0; j<2; ++j) {
    buffer_sizes_[j] = new size_t[buffer_num_]; 
    for(int i=0, b=0; i<attribute_id_num; ++i) {
      // Fix-sized attribute
      if(!array_schema->var_size(attribute_ids_[i])) { 
        buffer_sizes_[j][b] = 
            tile_slab_cell_num * array_schema->cell_size(attribute_ids_[i]);
        ++b;
      } else { // Variable-sized attribute
        buffer_sizes_[j][b] = tile_slab_cell_num * sizeof(size_t);
        ++b;
        buffer_sizes_[j][b] = 2 * tile_slab_cell_num * sizeof(size_t);
        ++b;
      }
    }
  }
}

template<class T>
void ArraySortedReadState::calculate_tile_slab_info_col(int id) {
  // TODO
}

template<class T>
void ArraySortedReadState::calculate_tile_slab_info_row(int id) {
  // TODO
}

void *ArraySortedReadState::copy_handler(void* context) {
  // This will enter an indefinite loop that will handle all incoming copy 
  // requests
  ((ArraySortedReadState*) context)->handle_copy_requests();

  // Return
  return NULL;
}

void ArraySortedReadState::copy_tile_slab() {
  // TODO
}

int ArraySortedReadState::create_buffers() {
  for(int j=0; j<2; ++j) {
    buffers_[j] = (void**) malloc(buffer_num_ * sizeof(void*));
    if(buffers_[j] == NULL) {
      std::string errmsg = "Cannot create local buffers";
      PRINT_ERROR(errmsg);
      tiledb_asrs_errmsg = TILEDB_ASRS_ERRMSG + errmsg;
      return TILEDB_ASRS_ERR;
    }

    for(int b=0; b < buffer_num_; ++b) { 
      buffers_[j][b] = aligned_alloc(ALIGNMENT, buffer_sizes_[j][b]);
      if(buffers_[j][b] == NULL) {
        std::string errmsg = "Cannot allocate local buffer";
        PRINT_ERROR(errmsg);
        tiledb_asrs_errmsg = TILEDB_ASRS_ERRMSG + errmsg;
        return TILEDB_ASRS_ERR;
      }
    }
  }

  // Success
  return TILEDB_ASRS_OK; 
}

void ArraySortedReadState::free_tile_slab_info() {
  // TODO
}

void ArraySortedReadState::free_tile_slab_state() {
  // TODO
}

void ArraySortedReadState::handle_copy_requests() {
  // Handle copy requests indefinitely
  for(;;) {
    // Wait for AIO
    wait_aio(copy_id_); 

    // Start the copy
    copy_tile_slab();
 
    // Wait in case of overflow
    if(overflow()) {
      block_overflow();
      block_aio(copy_id_);
      release_copy(copy_id_); 
      wait_overflow();
      continue;
    }

    // Copy is done 
    block_aio(copy_id_);
    release_copy(copy_id_); 
    copy_id_ = (copy_id_ + 1) % 2;
  }
}

void ArraySortedReadState::init_tile_slab_info() {
  // TODO
}

void ArraySortedReadState::init_tile_slab_state() {
  // TODO
}

int ArraySortedReadState::lock_aio_mtx() {
  if(pthread_mutex_lock(&aio_mtx_)) {
    std::string errmsg = "Cannot lock AIO mutex";
    PRINT_ERROR(errmsg);
    tiledb_asrs_errmsg = TILEDB_ASRS_ERRMSG + errmsg;
    return TILEDB_ASRS_ERR;
  }

  // Success
  return TILEDB_ASRS_OK;
}

int ArraySortedReadState::lock_copy_mtx() {
  if(pthread_mutex_lock(&copy_mtx_)) {
    std::string errmsg = "Cannot lock copy mutex";
    PRINT_ERROR(errmsg);
    tiledb_asrs_errmsg = TILEDB_ASRS_ERRMSG + errmsg;
    return TILEDB_ASRS_ERR;
  }

  // Success
  return TILEDB_ASRS_OK;
}

int ArraySortedReadState::lock_overflow_mtx() {
  if(pthread_mutex_lock(&overflow_mtx_)) {
    std::string errmsg = "Cannot lock overflow mutex";
    PRINT_ERROR(errmsg);
    tiledb_asrs_errmsg = TILEDB_ASRS_ERRMSG + errmsg;
    return TILEDB_ASRS_ERR;
  }

  // Success
  return TILEDB_ASRS_OK;
}

template<class T>
bool ArraySortedReadState::next_tile_slab_col() {
  // Quick check if done
  if(done_)
    return false;

  // If the AIO needs to be resumed, exit (no need for a new tile slab)
  if(resume_aio_) {
    resume_aio_ = false;
    return true;
  }

  // Allocate space for the tile slab if necessary
  if(tile_slab_[aio_id_] == NULL) 
    tile_slab_[aio_id_] = malloc(2*array_->array_schema()->coords_size());

  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  const T* subarray = static_cast<const T*>(subarray_);
  int dim_num = array_schema->dim_num();
  const T* domain = static_cast<const T*>(array_schema->domain());
  const T* tile_extents = static_cast<const T*>(array_schema->tile_extents());
  T* tile_slab[2];
  for(int i=0; i<2; ++i)
    tile_slab[i] = static_cast<T*>(tile_slab_[i]);
  int prev_id = (aio_id_-1)%2;

  // Check again if done, this time based on the tile slab and subarray
  if(tile_slab[prev_id] != NULL && 
     tile_slab[prev_id][2*(dim_num-1) + 1] == subarray[2*(dim_num-1) + 1]) {
    done_ = true;
    return false;
  }

  // If this is the first time this function is called, initialize
  if(tile_slab[prev_id] == NULL) {
    // Crop the subarray extent along the first axis to fit in the first tile
    tile_slab[aio_id_][2*(dim_num-1)] = subarray[2*(dim_num-1)]; 
    T upper = subarray[2*(dim_num-1)] + tile_extents[dim_num-1];
    T cropped_upper = 
        (upper - domain[2*(dim_num-1)]) / tile_extents[dim_num-1] * 
        tile_extents[dim_num-1] + domain[2*(dim_num-1)];
    tile_slab[aio_id_][2*(dim_num-1)+1] = 
        std::min(cropped_upper - 1, subarray[2*(dim_num-1)+1]); 

    // Leave the rest of the subarray extents intact
    for(int i=0; i<dim_num-1; ++i) {
      tile_slab[aio_id_][2*i] = subarray[2*i]; 
      tile_slab[aio_id_][2*i+1] = subarray[2*i+1]; 
    } 
  } else { // Calculate a new slab based on the previous
    // Copy previous tile slab
    memcpy(
        tile_slab[aio_id_], 
        tile_slab[prev_id], 
        2*array_->array_schema()->coords_size());

    // Advance tile slab
    tile_slab[aio_id_][2*(dim_num-1)] = tile_slab[aio_id_][2*(dim_num-1)+1] + 1;
    tile_slab[aio_id_][2*(dim_num-1)+1] = 
        std::min(
            tile_slab[aio_id_][2*(dim_num-1)+1] + tile_extents[dim_num-1] - 1,
            subarray[2*(dim_num-1)+1]);
  }

  // Calculate tile slab info and reset tile slab state
  calculate_tile_slab_info_col<T>(aio_id_);
  reset_tile_slab_state(aio_id_);

  // Success
  return true;
}

template<class T>
bool ArraySortedReadState::next_tile_slab_row() {
  // Quick check if done
  if(done_)
    return false;

  // If the AIO needs to be resumed, exit (no need for a new tile slab)
  if(resume_aio_) {
    resume_aio_ = false;
    return true;
  }

  // Allocate space for the tile slab if necessary
  if(tile_slab_[aio_id_] == NULL) 
    tile_slab_[aio_id_] = malloc(2*array_->array_schema()->coords_size());

  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  const T* subarray = static_cast<const T*>(subarray_);
  int dim_num = array_schema->dim_num();
  const T* domain = static_cast<const T*>(array_schema->domain());
  const T* tile_extents = static_cast<const T*>(array_schema->tile_extents());
  T* tile_slab[2];
  for(int i=0; i<2; ++i)
    tile_slab[i] = static_cast<T*>(tile_slab_[i]);
  int prev_id = (aio_id_-1)%2;

  // Check again if done, this time based on the tile slab and subarray
  if(tile_slab[prev_id] != NULL && 
     tile_slab[prev_id][1] == subarray[1]) {
    done_ = true;
    return false;
  }

  // If this is the first time this function is called, initialize
  if(tile_slab[prev_id] == NULL) {
    // Crop the subarray extent along the first axis to fit in the first tile
    tile_slab[aio_id_][0] = subarray[0]; 
    T upper = subarray[0] + tile_extents[0];
    T cropped_upper = 
        (upper - domain[0]) / tile_extents[0] * tile_extents[0] + domain[0];
    tile_slab[aio_id_][1] = std::min(cropped_upper - 1, subarray[1]); 

    // Leave the rest of the subarray extents intact
    for(int i=1; i<dim_num; ++i) {
      tile_slab[aio_id_][2*i] = subarray[2*i]; 
      tile_slab[aio_id_][2*i+1] = subarray[2*i+1]; 
    } 
  } else { // Calculate a new slab based on the previous
    // Copy previous tile slab
    memcpy(
        tile_slab[aio_id_], 
        tile_slab[prev_id], 
        2*array_->array_schema()->coords_size());

    // Advance tile slab
    tile_slab[aio_id_][0] = tile_slab[aio_id_][1] + 1; 
    tile_slab[aio_id_][1] = std::min(
                                tile_slab[aio_id_][1] + tile_extents[0] - 1,
                                subarray[1]);
  }

  // Calculate tile slab info and reset tile slab state
  calculate_tile_slab_info_row<T>(aio_id_);
  reset_tile_slab_state(aio_id_);

  // Success
  return true;
}

template<class T>
int ArraySortedReadState::read() {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  int mode = array_->mode(); 

  if(mode == TILEDB_ARRAY_READ_SORTED_COL) {
    if(array_schema->dense()) 
      return read_dense_sorted_col<T>();
    else
      return read_sparse_sorted_col<T>();
  } else if(mode == TILEDB_ARRAY_READ_SORTED_ROW) {
    if(array_schema->dense()) 
      return read_dense_sorted_row<T>();
    else
      return read_sparse_sorted_row<T>();
  } else {
    assert(0); // The code should never reach here
  }
}

template<class T>
int ArraySortedReadState::read_dense_sorted_col() {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  const T* subarray = static_cast<const T*>(subarray_);

  // Check if this can be satisfied with a default read
  if(array_schema->cell_order() == TILEDB_COL_MAJOR &&
     array_schema->is_contained_in_tile_slab_row<T>(subarray))
    return array_->read_default(user_buffers_, user_buffer_sizes_);

  // Iterate over each tile slab
  while(next_tile_slab_col<T>()) {
    // Read the next tile slab with the default cell order
    if(read_tile_slab() != TILEDB_ASRS_OK)
      return TILEDB_ASRS_ERR;

    // Handle overflow
    if(resume_aio_)
      break;
  }

  // Wait for copy to finish
  for(int i=0; i<2; ++i)
    wait_copy(i);

  // Success
  return TILEDB_ASRS_OK;
}

template<class T>
int ArraySortedReadState::read_dense_sorted_row() {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  const T* subarray = static_cast<const T*>(subarray_);

  // Check if this can be satisfied with a default read
  if(array_schema->cell_order() == TILEDB_ROW_MAJOR &&
     array_schema->is_contained_in_tile_slab_col<T>(subarray))
    return array_->read_default(user_buffers_, user_buffer_sizes_);

  // Iterate over each tile slab
  while(next_tile_slab_row<T>()) {
    // Read the next tile slab with the default cell order
    if(read_tile_slab() != TILEDB_ASRS_OK)
      return TILEDB_ASRS_ERR;

    // Handle overflow
    if(resume_aio_)
      break;
  }

  // Wait for copy to finish
  for(int i=0; i<2; ++i)
    wait_copy(i);

  // Success
  return TILEDB_ASRS_OK;
}

template<class T>
int ArraySortedReadState::read_sparse_sorted_col() {
  // TODO

  // Success
  return TILEDB_ASRS_OK;
}

template<class T>
int ArraySortedReadState::read_sparse_sorted_row() {
  // TODO


  // Success
  return TILEDB_ASRS_OK;
}

int ArraySortedReadState::read_tile_slab() {
  // Wait for the previous copy on aio_id_ buffer to be consumed
  wait_copy(aio_id_);

  // We need to exit if the copy did no complete (due to overflow)
  if(resume_copy_) {
    resume_aio_ = true;
    return TILEDB_ASRS_OK;
  }
 
  // TODO: This has to go in a loop to capture the case a variable-length
  // TODO: attribute overflows 

  // Prepare AIO request
  ASRS_Request asrs_request = { aio_id_, this };
  AIO_Request aio_request = {};
  aio_request.buffers_ = buffers_[aio_id_];
  aio_request.buffer_sizes_ = buffer_sizes_[aio_id_];
  aio_request.subarray_ = tile_slab_[aio_id_];
  aio_request.completion_handle_ = aio_done;
  aio_request.completion_data_ = &asrs_request; 

  // Send the AIO request
  if(array_->aio_read(&aio_request) != TILEDB_AR_OK) {
    // TODO: get error message: tiledb_asrs_errmsg = tiledb_ar_msg;
    return TILEDB_ASRS_ERR;
  } 

  // Change aio_id_
  aio_id_ = (aio_id_ + 1) % 2;

  // Success
  return TILEDB_ASRS_OK;
}

int ArraySortedReadState::release_aio(int id) {
  // Lock the AIO mutex
  if(lock_aio_mtx() != TILEDB_ASRS_OK)
    return TILEDB_ASRS_ERR;    

  // Set AIO flag
  wait_aio_[id] = false; 

  // Signal condition
  if(pthread_cond_signal(&(aio_cond_[id]))) { 
    std::string errmsg = "Cannot signal AIO condition";
    PRINT_ERROR(errmsg);
    tiledb_asrs_errmsg = TILEDB_ASRS_ERRMSG + errmsg;
    return TILEDB_ASRS_ERR;
  }

  // Unlock the AIO mutex
  if(unlock_aio_mtx() != TILEDB_ASRS_OK)
    return TILEDB_ASRS_ERR;    

  // Success
  return TILEDB_ASRS_OK;
}

int ArraySortedReadState::release_copy(int id) {
  // Lock the copy mutex
  if(lock_copy_mtx() != TILEDB_ASRS_OK)
    return TILEDB_ASRS_ERR;    

  // Set copy flag
  wait_copy_[id] = false;

  // Signal condition
  if(pthread_cond_signal(&copy_cond_[id])) { 
    std::string errmsg = "Cannot signal copy condition";
    PRINT_ERROR(errmsg);
    tiledb_asrs_errmsg = TILEDB_ASRS_ERRMSG + errmsg;
    return TILEDB_ASRS_ERR;    
  }

  // Unlock the copy mutex
  if(unlock_copy_mtx() != TILEDB_ASRS_OK)
    return TILEDB_ASRS_ERR;    

  // Success
  return TILEDB_ASRS_OK;
}

int ArraySortedReadState::release_overflow() {
  // Lock the overflow mutex
  if(lock_overflow_mtx() != TILEDB_ASRS_OK)
    return TILEDB_ASRS_ERR;    

  // Set copy flag
  resume_copy_ = false;

  // Signal condition
  if(pthread_cond_signal(&overflow_cond_)) { 
    std::string errmsg = "Cannot signal overflow condition";
    PRINT_ERROR(errmsg);
    tiledb_asrs_errmsg = TILEDB_ASRS_ERRMSG + errmsg;
    return TILEDB_ASRS_ERR;
  }

  // Unlock the overflow mutex
  if(unlock_overflow_mtx() != TILEDB_ASRS_OK)
    return TILEDB_ASRS_ERR;    

  // Success
  return TILEDB_ASRS_OK;
}

void ArraySortedReadState::reset_overflow() {
  for(int i=0; (int) attribute_ids_.size(); ++i)
    overflow_[i] = false;
}

int ArraySortedReadState::unlock_aio_mtx() {
  if(pthread_mutex_unlock(&aio_mtx_)) {
    std::string errmsg = "Cannot unlock AIO mutex";
    PRINT_ERROR(errmsg);
    tiledb_asrs_errmsg = TILEDB_ASRS_ERRMSG + errmsg;
    return TILEDB_ASRS_ERR;
  }

  // Success
  return TILEDB_ASRS_OK;
}

void ArraySortedReadState::reset_tile_slab_state(int id) {
  // TODO
}

int ArraySortedReadState::unlock_copy_mtx() {
  if(pthread_mutex_unlock(&copy_mtx_)) {
    std::string errmsg = "Cannot unlock copy mutex";
    PRINT_ERROR(errmsg);
    tiledb_asrs_errmsg = TILEDB_ASRS_ERRMSG + errmsg;
    return TILEDB_ASRS_ERR;
  }

  // Success
  return TILEDB_ASRS_OK;
}

int ArraySortedReadState::unlock_overflow_mtx() {
  if(pthread_mutex_unlock(&overflow_mtx_)) {
    std::string errmsg = "Cannot unlock overflow mutex";
    PRINT_ERROR(errmsg);
    tiledb_asrs_errmsg = TILEDB_ASRS_ERRMSG + errmsg;
    return TILEDB_ASRS_ERR;
  }

  // Success
  return TILEDB_ASRS_OK;
}

int ArraySortedReadState::wait_aio(int id) {
  // Lock AIO mutex
  if(lock_aio_mtx() != TILEDB_ASRS_OK)
    return TILEDB_ASRS_ERR; 

  // Wait to be signaled
  while(wait_aio_[id]) {
    if(pthread_cond_wait(&(aio_cond_[id]), &aio_mtx_)) {
      std::string errmsg = "Cannot wait on IO mutex condition";
      PRINT_ERROR(errmsg);
      tiledb_asrs_errmsg = TILEDB_ASRS_ERRMSG + errmsg;
      return TILEDB_ASRS_ERR;
    }
  }

  // Unlock AIO mutex
  if(unlock_aio_mtx() != TILEDB_ASRS_OK)
    return TILEDB_ASRS_ERR;

  // Success
  return TILEDB_ASRS_OK;
}

int ArraySortedReadState::wait_copy(int id) {
  // Lock copy mutex
  if(lock_copy_mtx() != TILEDB_ASRS_OK)
    return TILEDB_ASRS_ERR; 

  // Wait to be signaled
  while(wait_copy_[id]) {
    if(pthread_cond_wait(&(copy_cond_[id]), &copy_mtx_)) {
      std::string errmsg = "Cannot wait on copy mutex condition";
      PRINT_ERROR(errmsg);
      tiledb_asrs_errmsg = TILEDB_ASRS_ERRMSG + errmsg;
      return TILEDB_ASRS_ERR;
    }
  }

  // Unlock copy mutex
  if(unlock_copy_mtx() != TILEDB_ASRS_OK)
    return TILEDB_ASRS_ERR;

  // Success
  return TILEDB_ASRS_OK;
}

int ArraySortedReadState::wait_overflow() {
  // Wait to be signaled
  while(overflow()) {
    if(pthread_cond_wait(&overflow_cond_, &overflow_mtx_)) {
      std::string errmsg = "Cannot wait on IO mutex condition";
      PRINT_ERROR(errmsg);
      tiledb_asrs_errmsg = TILEDB_ASRS_ERRMSG + errmsg;
      return TILEDB_ASRS_ERR;
    }
  }

  // Success
  return TILEDB_ASRS_OK;
}
