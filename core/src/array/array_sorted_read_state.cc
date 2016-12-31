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
#include "comparators.h"
#include "math.h"
#include "utils.h"
#include <cassert>




/* ****************************** */
/*             MACROS             */
/* ****************************** */

#ifdef VERBOSE
#  define PRINT_ERROR(x) std::cerr << TILEDB_ASRS_ERRMSG << x << ".\n" 
#else
#  define PRINT_ERROR(x) do { } while(0) 
#endif

#if defined HAVE_OPENMP && defined USE_PARALLEL_SORT
  #include <parallel/algorithm>
  #define SORT(first, last, comp) __gnu_parallel::sort((first), (last), (comp))
#else
  #include <algorithm>
  #define SORT(first, last, comp) std::sort((first), (last), (comp))
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
    : array_(array) {
  // Calculate the attribute ids
  calculate_attribute_ids();

  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  int anum = (int) attribute_ids_.size();

  // Initializations
  aio_id_ = 0;
  aio_cnt_ = 0;
  coords_size_ = array_schema->coords_size();
  copy_id_ = 0;
  dim_num_ = array_schema->dim_num();
  copy_thread_running_ = false;
  copy_thread_canceled_ = false;
  read_tile_slabs_done_ = false;
  resume_copy_ = false;
  resume_aio_ = false;
  tile_coords_ = NULL;
  tile_domain_ = NULL;
  for(int i=0; i<2; ++i) {
    aio_overflow_[i] = new bool[anum];
    buffer_sizes_[i] = NULL;
    buffer_sizes_tmp_[i] = NULL;
    buffer_sizes_tmp_bak_[i] = NULL;
    buffers_[i] = NULL;
    tile_slab_[i] = malloc(2*coords_size_);
    tile_slab_norm_[i] = malloc(2*coords_size_);
    tile_slab_init_[i] = false;
    wait_copy_[i] = false;
    wait_aio_[i] = true;
  }
  overflow_ = new bool[anum];
  overflow_still_ = new bool[anum];
  for(int i=0; i<anum; ++i) {
    overflow_[i] = false;
    overflow_still_[i] = true;
    if(array_schema->var_size(attribute_ids_[i]))
      attribute_sizes_.push_back(sizeof(size_t));
    else
      attribute_sizes_.push_back(array_schema->cell_size(attribute_ids_[i]));
  }

  subarray_ = malloc(2*coords_size_);
  memcpy(subarray_, array_->subarray(), 2*coords_size_);

  // Calculate number of buffers
  calculate_buffer_num();

  // Calculate buffer sizes
  calculate_buffer_sizes();

  // Initialize tile slab info and state, and copy state
  init_tile_slab_info();
  init_tile_slab_state();
  init_copy_state();
}

ArraySortedReadState::~ArraySortedReadState() { 
  // Cancel copy thread
  copy_thread_canceled_ = true;
  for(int i=0; i<2; ++i)
    release_aio(i);
  // Wait for thread to be destroyed
  while(copy_thread_running_);

  // Join with the terminated thread
  pthread_join(copy_thread_, NULL);

  // Clean up
  free(subarray_);
  free(tile_coords_);
  free(tile_domain_);
  delete [] overflow_;

  for(int i=0; i<2; ++i) {
    delete [] aio_overflow_[i];

    if(buffer_sizes_[i] != NULL)
      delete [] buffer_sizes_[i];
    if(buffer_sizes_tmp_[i] != NULL)
      delete [] buffer_sizes_tmp_[i];
    if(buffer_sizes_tmp_bak_[i] != NULL)
      delete [] buffer_sizes_tmp_bak_[i];
    if(buffers_[i] != NULL) {
      for(int b=0; b<buffer_num_; ++b)  
        free(buffers_[i][b]);
      free(buffers_[i]);
    }

    free(tile_slab_[i]);
    free(tile_slab_norm_[i]);
  } 
  
  // Free tile slab info and state, and copy state
  free_copy_state();
  free_tile_slab_state();
  free_tile_slab_info();

  // Destroy conditions and mutexes
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
}




/* ****************************** */
/*           ACCESSORS            */
/* ****************************** */

bool ArraySortedReadState::copy_tile_slab_done() const {
  for(int i=0; i < (int) attribute_ids_.size(); ++i) {
    // Special case for sparse arrays with extra coordinates attribute    
    if(i == coords_attr_i_ && extra_coords_)
      continue;

    // Check
    if(!tile_slab_state_.copy_tile_slab_done_[i])
      return false;
  }

  return true;
}

bool ArraySortedReadState::done() const {
  if(!read_tile_slabs_done_)
    return false;
  else
    return copy_tile_slab_done();
} 

bool ArraySortedReadState::overflow() const {
  for(int i=0; i < (int) attribute_ids_.size(); ++i) {
    if(overflow_[i]) 
      return true;
  }

  return false;
}

bool ArraySortedReadState::overflow(int attribute_id) const {
  for(int i=0; i < (int) attribute_ids_.size(); ++i) {
    if(attribute_ids_[i] == attribute_id)
      return overflow_[i];
  }

  return false;
}

int ArraySortedReadState::read(void** buffers, size_t* buffer_sizes) {
  // Trivial case
  if(done()) {
    for(int i=0; i<buffer_num_; ++i)
      buffer_sizes[i] = 0;
    return TILEDB_ASRS_OK;
  }

  // Reset copy state
  reset_copy_state(buffers, buffer_sizes);

  // Reset overflow
  reset_overflow();
  
  // Resume the copy request handling
  if(resume_copy_) {
    block_copy(1); 
    block_copy(0); 
    release_aio(copy_id_);
    release_overflow();
  }

  // Call the appropriate templated read
  int type = array_->array_schema()->coords_type();
  if(type == TILEDB_INT32) {
    return read<int>();
  } else if(type == TILEDB_INT64) {
    return read<int64_t>();
  } else if(type == TILEDB_FLOAT32) {
    return read<float>();
  } else if(type == TILEDB_FLOAT64) {
    return read<double>();
  } else {
    assert(0);
    return TILEDB_ASRS_ERR;
  }
} 




/* ****************************** */
/*            MUTATORS            */
/* ****************************** */

int ArraySortedReadState::init() {
  // Create buffers
  if(create_buffers() != TILEDB_ASRS_OK)
    return TILEDB_ASRS_ERR;

  // Create AIO requests
  init_aio_requests();

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

  // Initialize functors
  const ArraySchema* array_schema = array_->array_schema();
  int mode = array_->mode();
  int cell_order = array_schema->cell_order();
  int tile_order = array_schema->tile_order();
  int coords_type = array_schema->coords_type();
  if(mode == TILEDB_ARRAY_READ_SORTED_ROW) { 
    if(coords_type == TILEDB_INT32) {
      advance_cell_slab_ = advance_cell_slab_row_s<int>;
      calculate_cell_slab_info_ = 
          (cell_order == TILEDB_ROW_MAJOR) ?
           calculate_cell_slab_info_row_row_s<int> :
           calculate_cell_slab_info_row_col_s<int>;
    } else if(coords_type == TILEDB_INT64) {
      advance_cell_slab_ = advance_cell_slab_row_s<int64_t>;
      calculate_cell_slab_info_ = 
          (cell_order == TILEDB_ROW_MAJOR) ?
           calculate_cell_slab_info_row_row_s<int64_t> :
           calculate_cell_slab_info_row_col_s<int64_t>;
    } else if(coords_type == TILEDB_FLOAT32) {
      advance_cell_slab_ = advance_cell_slab_row_s<float>;
      calculate_cell_slab_info_ = 
          (cell_order == TILEDB_ROW_MAJOR) ?
           calculate_cell_slab_info_row_row_s<float> :
           calculate_cell_slab_info_row_col_s<float>;
    } else if(coords_type == TILEDB_FLOAT64) {
      advance_cell_slab_ = advance_cell_slab_row_s<double>;
      calculate_cell_slab_info_ = 
          (cell_order == TILEDB_ROW_MAJOR) ?
           calculate_cell_slab_info_row_row_s<double> :
           calculate_cell_slab_info_row_col_s<double>;
    } else {
      assert(0);
    }
  } else { // mode == TILEDB_ARRAY_READ_SORTED_COL 
    if(coords_type == TILEDB_INT32) {
      advance_cell_slab_ = advance_cell_slab_col_s<int>;
      calculate_cell_slab_info_ = 
          (cell_order == TILEDB_ROW_MAJOR) ?
           calculate_cell_slab_info_col_row_s<int> :
           calculate_cell_slab_info_col_col_s<int>;
    } else if(coords_type == TILEDB_INT64) {
      advance_cell_slab_ = advance_cell_slab_col_s<int64_t>;
      calculate_cell_slab_info_ = 
          (cell_order == TILEDB_ROW_MAJOR) ?
           calculate_cell_slab_info_col_row_s<int64_t> :
           calculate_cell_slab_info_col_col_s<int64_t>;
    } else if(coords_type == TILEDB_FLOAT32) {
      advance_cell_slab_ = advance_cell_slab_col_s<float>;
      calculate_cell_slab_info_ = 
          (cell_order == TILEDB_ROW_MAJOR) ?
           calculate_cell_slab_info_col_row_s<float> :
           calculate_cell_slab_info_col_col_s<float>;
    } else if(coords_type == TILEDB_FLOAT64) {
      advance_cell_slab_ = advance_cell_slab_col_s<double>;
      calculate_cell_slab_info_ = 
          (cell_order == TILEDB_ROW_MAJOR) ?
           calculate_cell_slab_info_col_row_s<double> :
           calculate_cell_slab_info_col_col_s<double>;
    } else {
      assert(0);
    }
  }
  if(tile_order == TILEDB_ROW_MAJOR) { 
    if(coords_type == TILEDB_INT32) 
      calculate_tile_slab_info_ = calculate_tile_slab_info_row<int>;
    else if(coords_type == TILEDB_INT64) 
      calculate_tile_slab_info_ = calculate_tile_slab_info_row<int64_t>;
    else if(coords_type == TILEDB_FLOAT32) 
      calculate_tile_slab_info_ = calculate_tile_slab_info_row<float>;
    else if(coords_type == TILEDB_FLOAT64) 
      calculate_tile_slab_info_ = calculate_tile_slab_info_row<double>;
    else 
      assert(0);
  } else { // tile_order == TILEDB_COL_MAJOR
    if(coords_type == TILEDB_INT32) 
      calculate_tile_slab_info_ = calculate_tile_slab_info_col<int>;
    else if(coords_type == TILEDB_INT64) 
      calculate_tile_slab_info_ = calculate_tile_slab_info_col<int64_t>;
    else if(coords_type == TILEDB_FLOAT32) 
      calculate_tile_slab_info_ = calculate_tile_slab_info_col<float>;
    else if(coords_type == TILEDB_FLOAT64) 
      calculate_tile_slab_info_ = calculate_tile_slab_info_col<double>;
    else 
      assert(0);
  }

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
  copy_thread_running_ = true;

  // Success
  return TILEDB_ASRS_OK;
}


/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

template<class T>
void *ArraySortedReadState::advance_cell_slab_col_s(void* data) {
  ArraySortedReadState* asrs = ((ASRS_Data*) data)->asrs_;
  int aid = ((ASRS_Data*) data)->id_;
  asrs->advance_cell_slab_col<T>(aid);
  return NULL;
}

template<class T>
void *ArraySortedReadState::advance_cell_slab_row_s(void* data) {
  ArraySortedReadState* asrs = ((ASRS_Data*) data)->asrs_;
  int aid = ((ASRS_Data*) data)->id_;
  asrs->advance_cell_slab_row<T>(aid);
  return NULL;
}

template<class T>
void ArraySortedReadState::advance_cell_slab_col(int aid) {
  // For easy reference
  int64_t& tid = tile_slab_state_.current_tile_[aid];  // Tile id
  int64_t cell_slab_num = tile_slab_info_[copy_id_].cell_slab_num_[tid];
  T* current_coords = (T*) tile_slab_state_.current_coords_[aid]; 
  const T* tile_slab = (const T*) tile_slab_norm_[copy_id_];

  // Advance cell slab coordinates
  int d = 0;
  current_coords[d] += cell_slab_num;
  int64_t dim_overflow;
  for(int i=0; i<dim_num_-1; ++i) {
    dim_overflow = 
        (current_coords[i] - tile_slab[2*i]) / 
        (tile_slab[2*i+1]-tile_slab[2*i]+1);
    current_coords[i+1] += dim_overflow;
    current_coords[i] -= dim_overflow * (tile_slab[2*i+1]-tile_slab[2*i]+1);
  }

  // Check if done
  if(current_coords[dim_num_-1] > tile_slab[2*(dim_num_-1)+1]) {
    tile_slab_state_.copy_tile_slab_done_[aid] = true;
    return;
  }

  // Calculate new tile and offset for the current coords
  update_current_tile_and_offset<T>(aid);  
}

template<class T>
void ArraySortedReadState::advance_cell_slab_row(int aid) {
  // For easy reference
  int64_t& tid = tile_slab_state_.current_tile_[aid];  // Tile id
  int64_t cell_slab_num = tile_slab_info_[copy_id_].cell_slab_num_[tid];
  T* current_coords = (T*) tile_slab_state_.current_coords_[aid]; 
  const T* tile_slab = (const T*) tile_slab_norm_[copy_id_];

  // Advance cell slab coordinates
  int d = dim_num_-1;
  current_coords[d] += cell_slab_num;
  int64_t dim_overflow;
  for(int i=d; i>0; --i) {
    dim_overflow = 
        (current_coords[i] - tile_slab[2*i]) / 
        (tile_slab[2*i+1]-tile_slab[2*i]+1);
    current_coords[i-1] += dim_overflow;
    current_coords[i] -= dim_overflow * (tile_slab[2*i+1]-tile_slab[2*i]+1);
  }

  // Check if done
  if(current_coords[0] > tile_slab[1]) {
    tile_slab_state_.copy_tile_slab_done_[aid] = true;
    return;
  }

  // Calculate new tile and offset for the current coords
  update_current_tile_and_offset<T>(aid);  
}

void *ArraySortedReadState::aio_done(void* data) {
  // Retrieve data
  ArraySortedReadState* asrs = ((ASRS_Data*) data)->asrs_;
  int id = ((ASRS_Data*) data)->id_;

  // For easy reference
  int anum = (int) asrs->attribute_ids_.size();
  const ArraySchema* array_schema = asrs->array_->array_schema(); 
  
  // Check for overflow 
  bool overflow = false;
  for(int i=0; i<anum; ++i) {
    if(asrs->overflow_still_[i] && asrs->aio_overflow_[id][i]) {
      overflow = true;
      break;
    }
  }

  // Handle overflow
  bool sparse = array_schema->dense();
  if(overflow) {                // OVERFLOW
    // Update buffer sizes
    for(int i=0, b=0; i<anum; ++i) {
      if(!array_schema->var_size(asrs->attribute_ids_[i])) { // FIXED 
        if(asrs->aio_overflow_[id][i]) { 
          // Expand buffer
          expand_buffer(asrs->buffers_[id][b], asrs->buffer_sizes_[id][b]);
          // Re-assign the buffer size for the fixed-sized offsets
          asrs->buffer_sizes_tmp_[id][b] = asrs->buffer_sizes_[id][b];
        } else {
          // Backup sizes and zero them
          asrs->buffer_sizes_tmp_bak_[id][b] = asrs->buffer_sizes_tmp_[id][b];
          asrs->buffer_sizes_tmp_[id][b] = 0;
          // Does not overflow any more
          asrs->overflow_still_[i] = false;
        }
        ++b;
      } else {                                               // VAR
        if(asrs->aio_overflow_[id][i]) { 
          // Expand offset buffer only in the case of sparse arrays
          if(sparse)
            expand_buffer(asrs->buffers_[id][b], asrs->buffer_sizes_[id][b]);
          // Re-assign the buffer size for the fixed-sized offsets
          asrs->buffer_sizes_tmp_[id][b] = asrs->buffer_sizes_[id][b];
          ++b;
          // Expand variable-length cell buffers for both dense and sparse
          expand_buffer(asrs->buffers_[id][b], asrs->buffer_sizes_[id][b]);
          // Assign the new buffer size for the variable-sized values
          asrs->buffer_sizes_tmp_[id][b] = asrs->buffer_sizes_[id][b];
          ++b;
        } else {
          // Backup sizes and zero them (fixed-sized offsets)
          asrs->buffer_sizes_tmp_bak_[id][b] = asrs->buffer_sizes_tmp_[id][b];
          asrs->buffer_sizes_tmp_[id][b] = 0;
          ++b;
          // Backup sizes and zero them (variable-sized values)
          asrs->buffer_sizes_tmp_bak_[id][b] = asrs->buffer_sizes_tmp_[id][b];
          asrs->buffer_sizes_tmp_[id][b] = 0;
          ++b;
          // Does not overflow any more
          asrs->overflow_still_[i] = false;
        }
      }
    }

    // Send the request again
    asrs->send_aio_request(id);
  } else {                      // NO OVERFLOW
    // Restore backup temporary buffer sizes
    for(int b=0; b<asrs->buffer_num_; ++b) {
      if(asrs->buffer_sizes_tmp_bak_[id][b] != 0)
        asrs->buffer_sizes_tmp_[id][b] = asrs->buffer_sizes_tmp_bak_[id][b];
    }

    // Manage the mutexes and conditions
    asrs->release_aio(id);
  }

  return NULL;
}

bool ArraySortedReadState::aio_overflow(int aio_id) {
  // For easy reference
  int anum = (int) attribute_ids_.size();

  for(int i=0; i<anum; ++i) {
    if(aio_overflow_[aio_id][i])
      return true;
  }

  return false;
}

void ArraySortedReadState::block_aio(int id) {
  lock_aio_mtx();
  wait_aio_[id] = true; 
  unlock_aio_mtx();
}

void ArraySortedReadState::block_copy(int id) {
  lock_copy_mtx();
  wait_copy_[id] = true; 
  unlock_copy_mtx();
}

void ArraySortedReadState::block_overflow() {
  lock_overflow_mtx();
  resume_copy_ = true; 
  unlock_overflow_mtx();
}

void ArraySortedReadState::calculate_attribute_ids() {
  // Initialization
  attribute_ids_ = array_->attribute_ids();
  coords_attr_i_ = -1;

  // For ease reference
  const ArraySchema* array_schema = array_->array_schema();
  int attribute_num = array_schema->attribute_num();

  // No need to do anything else in case the array is dense
  if(array_schema->dense())
    return;

  // Find the coordinates index
  for(int i=0; i<(int)attribute_ids_.size(); ++i) {
    if(attribute_ids_[i] == attribute_num) {
      coords_attr_i_ = i;
      break;
    }
  }  

  // If the coordinates index is not found, append coordinates attribute
  // to attribute ids.
  if(coords_attr_i_ == -1) {
    attribute_ids_.push_back(attribute_num);
    coords_attr_i_ = attribute_ids_.size() - 1;
    extra_coords_ = true;
  } else { // No extra coordinates appended
    extra_coords_ = false;
  } 
}

void ArraySortedReadState::calculate_buffer_num() {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  int attribute_num = array_schema->attribute_num();

  // Calculate number of buffers
  buffer_num_ = 0;
  int attribute_id_num = (int) attribute_ids_.size();
  for(int i=0; i<attribute_id_num; ++i) {
    // Fix-sized attribute
    if(!array_schema->var_size(attribute_ids_[i])) { 
      if(attribute_ids_[i] == attribute_num)
        coords_buf_i_ = i; // Buffer that holds the coordinates
      ++buffer_num_;
    } else  { // Variable-sized attribute
      buffer_num_ += 2;
    }
  }
}

void ArraySortedReadState::calculate_buffer_sizes() {
  if(array_->array_schema()->dense())
    calculate_buffer_sizes_dense();
  else
    calculate_buffer_sizes_sparse();
}

void ArraySortedReadState::calculate_buffer_sizes_dense() {
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
    buffer_sizes_tmp_[j] = new size_t[buffer_num_]; 
    buffer_sizes_tmp_bak_[j] = new size_t[buffer_num_]; 
    for(int i=0, b=0; i<attribute_id_num; ++i) {
      // Fix-sized attribute
      if(!array_schema->var_size(attribute_ids_[i])) { 
        buffer_sizes_[j][b] = 
            tile_slab_cell_num * array_schema->cell_size(attribute_ids_[i]);
        buffer_sizes_tmp_bak_[j][b] = 0; 
        ++b;
      } else { // Variable-sized attribute
        buffer_sizes_[j][b] = tile_slab_cell_num * sizeof(size_t);
        buffer_sizes_tmp_bak_[j][b] = 0; 
        ++b;
        buffer_sizes_[j][b] = 2 * tile_slab_cell_num * sizeof(size_t);
        buffer_sizes_tmp_bak_[j][b] = 0; 
        ++b;
      }
    }
  }
}

void ArraySortedReadState::calculate_buffer_sizes_sparse() {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();

  // Calculate buffer sizes
  int attribute_id_num = (int) attribute_ids_.size();
  for(int j=0; j<2; ++j) {
    buffer_sizes_[j] = new size_t[buffer_num_]; 
    buffer_sizes_tmp_[j] = new size_t[buffer_num_]; 
    buffer_sizes_tmp_bak_[j] = new size_t[buffer_num_]; 
    for(int i=0, b=0; i<attribute_id_num; ++i) {
      // Fix-sized buffer
      buffer_sizes_[j][b] = TILEDB_ASRS_INIT_BUFFER_SIZE; 
      buffer_sizes_tmp_bak_[j][b] = 0; 
      ++b;
      if(array_schema->var_size(attribute_ids_[i])) { // Variable-sized buffer 
        buffer_sizes_[j][b] = 2*TILEDB_ASRS_INIT_BUFFER_SIZE; 
        buffer_sizes_tmp_bak_[j][b] = 0; 
        ++b;
      }
    }
  }
}

template<class T>
void *ArraySortedReadState::calculate_cell_slab_info_col_col_s(void* data) {
  ArraySortedReadState* asrs = ((ASRS_Data*) data)->asrs_;
  int id = ((ASRS_Data*) data)->id_;
  int tid = ((ASRS_Data*) data)->id_2_;
  asrs->calculate_cell_slab_info_col_col<T>(id, tid);
  return NULL;
}

template<class T>
void *ArraySortedReadState::calculate_cell_slab_info_col_row_s(void* data) {
  ArraySortedReadState* asrs = ((ASRS_Data*) data)->asrs_;
  int id = ((ASRS_Data*) data)->id_;
  int tid = ((ASRS_Data*) data)->id_2_;
  asrs->calculate_cell_slab_info_col_row<T>(id, tid);
  return NULL;
}

template<class T>
void *ArraySortedReadState::calculate_cell_slab_info_row_col_s(void* data) {
  ArraySortedReadState* asrs = ((ASRS_Data*) data)->asrs_;
  int id = ((ASRS_Data*) data)->id_;
  int tid = ((ASRS_Data*) data)->id_2_;
  asrs->calculate_cell_slab_info_row_col<T>(id, tid);
  return NULL;
}

template<class T>
void *ArraySortedReadState::calculate_cell_slab_info_row_row_s(void* data) {
  ArraySortedReadState* asrs = ((ASRS_Data*) data)->asrs_;
  int id = ((ASRS_Data*) data)->id_;
  int tid = ((ASRS_Data*) data)->id_2_;
  asrs->calculate_cell_slab_info_row_row<T>(id, tid);
  return NULL;
}

template<class T>
void ArraySortedReadState::calculate_cell_slab_info_col_col(
    int id, 
    int64_t tid) {
  // For easy reference
  int anum = (int) attribute_ids_.size();
  const T* range_overlap = (const T*) tile_slab_info_[id].range_overlap_[tid];
  const T* tile_domain = (const T*) tile_domain_;
  int64_t tile_num, cell_num; 
  
  // Calculate number of cells in cell slab
  cell_num = range_overlap[1] - range_overlap[0] + 1;
  for(int i=0; i<dim_num_-1; ++i) {
    tile_num = tile_domain[2*i+1] - tile_domain[2*i] + 1;
    if(tile_num == 1)
      cell_num *= range_overlap[2*(i+1)+1] - range_overlap[2*(i+1)] + 1;
    else
      break;
  }
  tile_slab_info_[id].cell_slab_num_[tid] = cell_num;

  // Calculate size of a cell slab per attribute
  for(int aid=0; aid<anum; ++aid)
    tile_slab_info_[id].cell_slab_size_[aid][tid] = 
        tile_slab_info_[id].cell_slab_num_[tid] * attribute_sizes_[aid];

  // Calculate cell offset per dimension 
  int64_t cell_offset = 1;
  tile_slab_info_[id].cell_offset_per_dim_[tid][0] = cell_offset;
  for(int i=1; i<dim_num_; ++i) {
    cell_offset *= (range_overlap[2*(i-1)+1] - range_overlap[2*(i-1)] + 1); 
    tile_slab_info_[id].cell_offset_per_dim_[tid][i] = cell_offset;
  }
}

template<class T>
void ArraySortedReadState::calculate_cell_slab_info_row_row(
    int id, 
    int64_t tid) {
  // For easy reference
  int anum = (int) attribute_ids_.size();
  const T* range_overlap = (const T*) tile_slab_info_[id].range_overlap_[tid];
  const T* tile_domain = (const T*) tile_domain_;
  int64_t tile_num, cell_num; 

  // Calculate number of cells in cell slab
  cell_num = range_overlap[2*(dim_num_-1)+1] - range_overlap[2*(dim_num_-1)] +1;
  for(int i=dim_num_-1; i>0; --i) {
    tile_num = tile_domain[2*i+1] - tile_domain[2*i] + 1;
    if(tile_num == 1)
      cell_num *= range_overlap[2*(i-1)+1] - range_overlap[2*(i-1)] + 1;
    else
      break;
  }
  tile_slab_info_[id].cell_slab_num_[tid] = cell_num;

  // Calculate size of a cell slab per attribute
  for(int aid=0; aid<anum; ++aid)
    tile_slab_info_[id].cell_slab_size_[aid][tid] = 
        tile_slab_info_[id].cell_slab_num_[tid] * attribute_sizes_[aid];

  // Calculate cell offset per dimension 
  int64_t cell_offset = 1;
  tile_slab_info_[id].cell_offset_per_dim_[tid][dim_num_-1] = cell_offset;
  for(int i=dim_num_-2; i>=0; --i) {
    cell_offset *= (range_overlap[2*(i+1)+1] - range_overlap[2*(i+1)] + 1); 
    tile_slab_info_[id].cell_offset_per_dim_[tid][i] = cell_offset;
  }
}

template<class T>
void ArraySortedReadState::calculate_cell_slab_info_col_row(
    int id, 
    int64_t tid) {
  // For easy reference
  int anum = (int) attribute_ids_.size();
  const T* range_overlap = (const T*) tile_slab_info_[id].range_overlap_[tid];
  
  // Calculate number of cells in cell slab
  tile_slab_info_[id].cell_slab_num_[tid] = 1;

  // Calculate size of a cell slab per attribute
  for(int aid=0; aid<anum; ++aid)
    tile_slab_info_[id].cell_slab_size_[aid][tid] = 
        tile_slab_info_[id].cell_slab_num_[tid] * attribute_sizes_[aid];

  // Calculate cell offset per dimension 
  int64_t cell_offset = 1;
  tile_slab_info_[id].cell_offset_per_dim_[tid][dim_num_-1] = cell_offset;
  for(int i=dim_num_-2; i>=0; --i) {
    cell_offset *= (range_overlap[2*(i+1)+1] - range_overlap[2*(i+1)] + 1); 
    tile_slab_info_[id].cell_offset_per_dim_[tid][i] = cell_offset;
  } 
}

template<class T>
void ArraySortedReadState::calculate_cell_slab_info_row_col(
    int id, 
    int64_t tid) {
  // For easy reference
  int anum = (int) attribute_ids_.size();
  const T* range_overlap = (const T*) tile_slab_info_[id].range_overlap_[tid];
  
  // Calculate number of cells in cell slab
  tile_slab_info_[id].cell_slab_num_[tid] = 1;

  // Calculate size of a cell slab per attribute
  for(int aid=0; aid<anum; ++aid)
    tile_slab_info_[id].cell_slab_size_[aid][tid] = 
        tile_slab_info_[id].cell_slab_num_[tid] * attribute_sizes_[aid];

  // Calculate cell offset per dimension 
  int64_t cell_offset = 1;
  tile_slab_info_[id].cell_offset_per_dim_[tid][0] = cell_offset;
  for(int i=1; i<dim_num_; ++i) {
    cell_offset *= (range_overlap[2*(i-1)+1] - range_overlap[2*(i-1)] + 1); 
    tile_slab_info_[id].cell_offset_per_dim_[tid][i] = cell_offset;
  }
}

template<class T>
void ArraySortedReadState::calculate_tile_domain(int id) {
  // Initializations
  tile_coords_ = malloc(coords_size_);
  tile_domain_ = malloc(2*coords_size_);

  // For easy reference
  const T* tile_slab = (const T*) tile_slab_norm_[id];
  const T* tile_extents = (const T*) array_->array_schema()->tile_extents();
  T* tile_coords = (T*) tile_coords_;
  T* tile_domain = (T*) tile_domain_;

  // Calculate tile domain and initial tile coordinates
  for(int i=0; i<dim_num_; ++i) {
    tile_coords[i] = 0;
    tile_domain[2*i] = tile_slab[2*i] / tile_extents[i];
    tile_domain[2*i+1] = tile_slab[2*i+1] / tile_extents[i];
  }
}

template<class T>
void ArraySortedReadState::calculate_tile_slab_info(int id) {
  // Calculate number of tiles, if they are not already calculated
  if(tile_slab_info_[id].tile_num_ == -1) 
    init_tile_slab_info<T>(id); 

  // Calculate tile domain, if not calculated yet
  if(tile_domain_ == NULL)
    calculate_tile_domain<T>(id);

  // Reset tile coordinates
  reset_tile_coords<T>();

  // Calculate tile slab info
  ASRS_Data asrs_data = { id, 0, this };
  (*calculate_tile_slab_info_)(&asrs_data);
}

template<class T>
void *ArraySortedReadState::calculate_tile_slab_info_col(void* data) {
  ArraySortedReadState* asrs = ((ASRS_Data*) data)->asrs_;
  int id = ((ASRS_Data*) data)->id_;
  asrs->calculate_tile_slab_info_col<T>(id);
  return NULL;
}

template<class T>
void ArraySortedReadState::calculate_tile_slab_info_col(int id) {
  // For easy reference
  const T* tile_domain = (const T*) tile_domain_;
  T* tile_coords = (T*) tile_coords_;
  const T* tile_extents = (const T*) array_->array_schema()->tile_extents();
  T** range_overlap = (T**) tile_slab_info_[id].range_overlap_;
  const T* tile_slab = (const T*) tile_slab_norm_[id];
  int64_t tile_offset, tile_cell_num, total_cell_num = 0;
  int anum = (int) attribute_ids_.size();
  int d;

  // Iterate over all tiles in the tile domain
  int64_t tid=0; // Tile id
  while(tile_coords[dim_num_-1] <= tile_domain[2*(dim_num_-1)+1]) {
    // Calculate range overlap, number of cells in the tile 
    tile_cell_num = 1;
    for(int i=0; i<dim_num_; ++i) {
      // Range overlap
      range_overlap[tid][2*i] = 
          std::max(tile_coords[i] * tile_extents[i], tile_slab[2*i]);
      range_overlap[tid][2*i+1] = 
          std::min((tile_coords[i]+1) * tile_extents[i] - 1, tile_slab[2*i+1]);

      // Number of cells in this tile
      tile_cell_num *= range_overlap[tid][2*i+1] - range_overlap[tid][2*i] + 1;
     }

    // Calculate tile offsets per dimension
    tile_offset = 1;
    tile_slab_info_[id].tile_offset_per_dim_[0] = tile_offset;
    for(int i=1; i<dim_num_; ++i) {
      tile_offset *= (tile_domain[2*(i-1)+1] - tile_domain[2*(i-1)] + 1);
      tile_slab_info_[id].tile_offset_per_dim_[i] = tile_offset;
    }

    // Calculate cell slab info
    ASRS_Data asrs_data = { id, tid, this };
    (*calculate_cell_slab_info_)(&asrs_data);

    // Calculate start offsets 
    for(int aid=0; aid<anum; ++aid) {
      tile_slab_info_[id].start_offsets_[aid][tid] =
          total_cell_num * attribute_sizes_[aid];
    }
    total_cell_num += tile_cell_num;

    // Advance tile coordinates
    d=0;
    ++tile_coords[d];
    while(d < dim_num_-1 && tile_coords[d] > tile_domain[2*d+1]) {
      tile_coords[d] = tile_domain[2*d];
      ++tile_coords[++d];
    }

    // Advance tile id
    ++tid;
  }
}

template<class T>
void *ArraySortedReadState::calculate_tile_slab_info_row(void* data) {
  ArraySortedReadState* asrs = ((ASRS_Data*) data)->asrs_;
  int id = ((ASRS_Data*) data)->id_;
  asrs->calculate_tile_slab_info_row<T>(id);
  return NULL;
}

template<class T>
void ArraySortedReadState::calculate_tile_slab_info_row(int id) {
  // For easy reference
  const T* tile_domain = (const T*) tile_domain_;
  T* tile_coords = (T*) tile_coords_;
  const T* tile_extents = (const T*) array_->array_schema()->tile_extents();
  T** range_overlap = (T**) tile_slab_info_[id].range_overlap_;
  const T* tile_slab = (const T*) tile_slab_norm_[id];
  int64_t tile_offset, tile_cell_num, total_cell_num = 0;
  int anum = (int) attribute_ids_.size();
  int d;

  // Iterate over all tiles in the tile domain
  int64_t tid=0; // Tile id
  while(tile_coords[0] <= tile_domain[1]) {
    // Calculate range overlap, number of cells in the tile 
    tile_cell_num = 1;
    for(int i=0; i<dim_num_; ++i) {
      // Range overlap
      range_overlap[tid][2*i] = 
          std::max(tile_coords[i] * tile_extents[i], tile_slab[2*i]);
      range_overlap[tid][2*i+1] = 
          std::min((tile_coords[i]+1) * tile_extents[i] - 1, tile_slab[2*i+1]);

      // Number of cells in this tile
      tile_cell_num *= range_overlap[tid][2*i+1] - range_overlap[tid][2*i] + 1;
     }

    // Calculate tile offsets per dimension
    tile_offset = 1;
    tile_slab_info_[id].tile_offset_per_dim_[dim_num_-1] = tile_offset;
    for(int i=dim_num_-2; i>=0; --i) {
      tile_offset *= (tile_domain[2*(i+1)+1] - tile_domain[2*(i+1)] + 1); 
      tile_slab_info_[id].tile_offset_per_dim_[i] = tile_offset;
    }

    // Calculate cell slab info
    ASRS_Data asrs_data = { id, tid, this };
    (*calculate_cell_slab_info_)(&asrs_data);

    // Calculate start offsets 
    for(int aid=0; aid<anum; ++aid) {
      tile_slab_info_[id].start_offsets_[aid][tid] =
          total_cell_num * attribute_sizes_[aid];
    }
    total_cell_num += tile_cell_num;

    // Advance tile coordinates
    d=dim_num_-1;
    ++tile_coords[d];
    while(d > 0 && tile_coords[d] > tile_domain[2*d+1]) {
      tile_coords[d] = tile_domain[2*d];
      ++tile_coords[--d];
    }

    // Advance tile id
    ++tid;
  }
}

void *ArraySortedReadState::copy_handler(void* context) {
  // For easy reference
  ArraySortedReadState* asrs = (ArraySortedReadState*) context;

  // This will enter an indefinite loop that will handle all incoming copy 
  // requests
  int coords_type = asrs->array_->array_schema()->coords_type();
  if(asrs->array_->array_schema()->dense()) {     // DENSE
    if(coords_type == TILEDB_INT32)
      asrs->handle_copy_requests_dense<int>();
    else if(coords_type == TILEDB_INT64)
      asrs->handle_copy_requests_dense<int64_t>();
    else if(coords_type == TILEDB_FLOAT32)
      asrs->handle_copy_requests_dense<float>();
    else if(coords_type == TILEDB_FLOAT64)
      asrs->handle_copy_requests_dense<double>();
    else
      assert(0);
  } else {                                       // SPARSE
    if(coords_type == TILEDB_INT32)
      asrs->handle_copy_requests_sparse<int>();
    else if(coords_type == TILEDB_INT64)
      asrs->handle_copy_requests_sparse<int64_t>();
    else if(coords_type == TILEDB_FLOAT32)
      asrs->handle_copy_requests_sparse<float>();
    else if(coords_type == TILEDB_FLOAT64)
      asrs->handle_copy_requests_sparse<double>();
    else
      assert(0);
  }

  // Return
  return NULL;
}

void ArraySortedReadState::copy_tile_slab_dense() {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();

  // Copy tile slab for each attribute separately
  for(int i=0, b=0; i<(int)attribute_ids_.size(); ++i) {
    if(!array_schema->var_size(attribute_ids_[i])) {
      copy_tile_slab_dense(i, b); 
      ++b;
    } else {
      copy_tile_slab_dense_var(i, b); 
      b += 2;
    }
  }
}

void ArraySortedReadState::copy_tile_slab_dense(int aid, int bid) {
  // Exit if copy is done for this attribute
  if(tile_slab_state_.copy_tile_slab_done_[aid]) {
    copy_state_.buffer_sizes_[bid] = 0; // Nothing written
    return;
  }

  // For easy reference
  int64_t& tid = tile_slab_state_.current_tile_[aid]; 
  size_t& buffer_offset = copy_state_.buffer_offsets_[bid];
  size_t buffer_size = copy_state_.buffer_sizes_[bid];
  char* buffer = (char*) copy_state_.buffers_[bid];
  char* local_buffer = (char*) buffers_[copy_id_][bid];
  ASRS_Data asrs_data = { aid, 0, this };

  // Iterate over the tile slab cells
  for(;;) {
    // For easy reference
    size_t cell_slab_size = tile_slab_info_[copy_id_].cell_slab_size_[aid][tid];
    size_t& local_buffer_offset = tile_slab_state_.current_offsets_[aid]; 

    // Handle overflow
    if(buffer_offset + cell_slab_size > buffer_size) {
      overflow_[aid] = true;
      break;
    }
      
    // Copy cell slab
    memcpy(
        buffer + buffer_offset,
        local_buffer + local_buffer_offset, 
        cell_slab_size);

    // Update buffer offset
    buffer_offset += cell_slab_size;
   
    // Prepare for new cell slab
    (*advance_cell_slab_)(&asrs_data);

    // Terminating condition 
    if(tile_slab_state_.copy_tile_slab_done_[aid])
      break;
  }

  // Set user buffer size
  buffer_size = buffer_offset;  
}

void ArraySortedReadState::copy_tile_slab_dense_var(int aid, int bid) {
  // Exit if copy is done for this attribute
  if(tile_slab_state_.copy_tile_slab_done_[aid]) {
    copy_state_.buffer_sizes_[bid] = 0; // Nothing written
    copy_state_.buffer_sizes_[bid+1] = 0; // Nothing written
    return;
  }

  // For easy reference
  int64_t& tid = tile_slab_state_.current_tile_[aid]; 
  size_t cell_slab_size_var;
  size_t& buffer_offset = copy_state_.buffer_offsets_[bid];
  size_t& buffer_offset_var = copy_state_.buffer_offsets_[bid+1];
  size_t buffer_size = copy_state_.buffer_sizes_[bid];
  size_t buffer_size_var = copy_state_.buffer_sizes_[bid+1];
  char* buffer = (char*) copy_state_.buffers_[bid];
  char* buffer_var = (char*) copy_state_.buffers_[bid+1];
  char* local_buffer_var = (char*) buffers_[copy_id_][bid+1];
  size_t local_buffer_size = buffer_sizes_tmp_[copy_id_][bid];
  size_t local_buffer_var_size = buffer_sizes_tmp_[copy_id_][bid+1];
  size_t* local_buffer_s = (size_t*) buffers_[copy_id_][bid];
  int64_t cell_num_in_buffer = local_buffer_size / sizeof(size_t);
  size_t var_offset = buffer_offset_var;
  ASRS_Data asrs_data = { aid, 0, this };

  // For all overlapping tiles, in a round-robin fashion
  for(;;) {
    // For easy reference
    size_t cell_slab_size = 
        tile_slab_info_[copy_id_].cell_slab_size_[aid][tid];
    int64_t cell_num_in_slab = cell_slab_size / sizeof(size_t);
    size_t& local_buffer_offset = tile_slab_state_.current_offsets_[aid]; 

    // Handle overflow
    if(buffer_offset + cell_slab_size > buffer_size) {
      overflow_[aid] = true;
      break;
    }

    // Calculate variable cell slab size
    int64_t cell_start = local_buffer_offset / sizeof(size_t);
    int64_t cell_end = cell_start + cell_num_in_slab;
    cell_slab_size_var = 
         (cell_end == cell_num_in_buffer) ?
         local_buffer_var_size - local_buffer_s[cell_start] :
         local_buffer_s[cell_end] - local_buffer_s[cell_start];

    // Handle overflow for the the variable-length buffer
    if(buffer_offset_var + cell_slab_size_var > buffer_size_var) {
      overflow_[aid] = true;
      break;
    }
      
    // Copy fixed-sized offsets
    for(int64_t i=cell_start; i<cell_end; ++i) {
      memcpy(
          buffer + buffer_offset, 
          &var_offset, 
          sizeof(size_t));
      buffer_offset += sizeof(size_t);
      var_offset += (i == cell_num_in_buffer-1) ? 
                 local_buffer_var_size - local_buffer_s[i] :
                 local_buffer_s[i+1] - local_buffer_s[i];
    }

    // Copy variable-sized values
    memcpy(
        buffer_var + buffer_offset_var, 
        local_buffer_var + local_buffer_s[cell_start], 
        cell_slab_size_var);
    buffer_offset_var += cell_slab_size_var;
   
    // Prepare for new cell slab
    (*advance_cell_slab_)(&asrs_data);

    // Terminating condition 
    if(tile_slab_state_.copy_tile_slab_done_[aid])
      break;
  }

  // Set user buffer sizes
  buffer_size = buffer_offset;  
  buffer_size_var = buffer_offset_var;  
}

void ArraySortedReadState::copy_tile_slab_sparse() {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
 
  // Copy tile slab for each attribute separately
  for(int i=0, b=0; i<(int)attribute_ids_.size(); ++i) {
    if(!array_schema->var_size(attribute_ids_[i])) {  // FIXED
      // Make sure not to copy coordinates if the user has not requested them
      if(i != coords_attr_i_ || !extra_coords_)
        copy_tile_slab_sparse(i, b); 
      ++b;
    } else {                                          // VAR
      copy_tile_slab_sparse_var(i, b); 
      b += 2;
    }
  }
}

void ArraySortedReadState::copy_tile_slab_sparse(int aid, int bid) {
  // Exit if copy is done for this attribute
  if(tile_slab_state_.copy_tile_slab_done_[aid]) {
    copy_state_.buffer_sizes_[bid] = 0; // Nothing written
    return;
  }

  // For easy reference
  size_t cell_size = array_->array_schema()->cell_size(attribute_ids_[aid]);
  size_t& buffer_offset = copy_state_.buffer_offsets_[bid];
  size_t buffer_size = copy_state_.buffer_sizes_[bid];
  char* buffer = (char*) copy_state_.buffers_[bid];
  char* local_buffer = (char*) buffers_[copy_id_][bid];
  size_t local_buffer_offset;
  int64_t cell_num = buffer_sizes_tmp_[copy_id_][coords_buf_i_] / coords_size_; 
  int64_t& current_cell_pos = tile_slab_state_.current_cell_pos_[aid];

  // Iterate over the remaining tile slab cells in a sorted order
  for(; current_cell_pos<cell_num; ++current_cell_pos) {
    // Handle overflow
    if(buffer_offset + cell_size > buffer_size) {
      overflow_[aid] = true;
      break;
    }

    // Calculate new local buffer offset
    local_buffer_offset = cell_pos_[current_cell_pos] * cell_size;

    // Copy cell slab
    memcpy(
        buffer + buffer_offset,
        local_buffer + local_buffer_offset, 
        cell_size);

    // Update buffer offset
    buffer_offset += cell_size;
  }

  // Mark tile slab as done
  if(current_cell_pos == cell_num)
    tile_slab_state_.copy_tile_slab_done_[aid] = true;

  // Set user buffer size
  buffer_size = buffer_offset;  
}


void ArraySortedReadState::copy_tile_slab_sparse_var(int aid, int bid) {
  // Exit if copy is done for this attribute
  if(tile_slab_state_.copy_tile_slab_done_[aid]) {
    copy_state_.buffer_sizes_[bid] = 0; // Nothing written
    copy_state_.buffer_sizes_[bid+1] = 0; // Nothing written
    return;
  }

  // For easy reference
  size_t cell_size = sizeof(size_t);
  size_t cell_size_var;
  size_t& buffer_offset = copy_state_.buffer_offsets_[bid];
  size_t& buffer_offset_var = copy_state_.buffer_offsets_[bid+1];
  size_t buffer_size = copy_state_.buffer_sizes_[bid];
  size_t buffer_size_var = copy_state_.buffer_sizes_[bid+1];
  char* buffer = (char*) copy_state_.buffers_[bid];
  char* buffer_var = (char*) copy_state_.buffers_[bid+1];
  char* local_buffer_var = (char*) buffers_[copy_id_][bid+1];
  size_t local_buffer_var_size = buffer_sizes_tmp_[copy_id_][bid+1];
  size_t* local_buffer_s = (size_t*) buffers_[copy_id_][bid];
  int64_t cell_num = buffer_sizes_tmp_[copy_id_][coords_buf_i_] / coords_size_; 
  int64_t& current_cell_pos = tile_slab_state_.current_cell_pos_[aid];

  // Iterate over the remaining tile slab cells in a sorted order
  for(; current_cell_pos<cell_num; ++current_cell_pos) {
    // Handle overflow
    if(buffer_offset + cell_size > buffer_size) {
      overflow_[aid] = true;
      break;
    }

    // Calculate variable cell size
    int64_t cell_start = cell_pos_[current_cell_pos];
    int64_t cell_end = cell_start + 1;
    cell_size_var = 
         (cell_end == cell_num) ?
         local_buffer_var_size - local_buffer_s[cell_start] :
         local_buffer_s[cell_end] - local_buffer_s[cell_start];

    // Handle overflow for the the variable-length buffer
    if(buffer_offset_var + cell_size_var > buffer_size_var) {
      overflow_[aid] = true;
      break;
    }
      
    // Copy fixed-sized offset
    memcpy(
        buffer + buffer_offset, 
        &buffer_offset_var, 
        sizeof(size_t));
    buffer_offset += sizeof(size_t);

    // Copy variable-sized values
    memcpy(
        buffer_var + buffer_offset_var, 
        local_buffer_var + local_buffer_s[cell_start], 
        cell_size_var);
    buffer_offset_var += cell_size_var;
  }

  // Mark tile slab as done
  if(current_cell_pos == cell_num)
    tile_slab_state_.copy_tile_slab_done_[aid] = true;

  // Set user buffer sizes
  buffer_size = buffer_offset;  
  buffer_size_var = buffer_offset_var;  
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
      buffers_[j][b] = malloc(buffer_sizes_[j][b]);
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

void ArraySortedReadState::free_copy_state() {
  if(copy_state_.buffer_offsets_ != NULL)
    delete [] copy_state_.buffer_offsets_;
}

void ArraySortedReadState::free_tile_slab_info() {
  // Do nothing in the case of sparse arrays
  if(!array_->array_schema()->dense())
    return;

  // For easy reference
  int anum = (int) attribute_ids_.size();

  // Free
  for(int i=0; i<2; ++i) {
    int64_t tile_num = tile_slab_info_[i].tile_num_;

    if(tile_slab_info_[i].cell_offset_per_dim_ != NULL) {
      for(int j=0; j<tile_num; ++j)
        delete [] tile_slab_info_[i].cell_offset_per_dim_[j];
      delete [] tile_slab_info_[i].cell_offset_per_dim_;
    }

    for(int j=0; j<anum; ++j) {  
      if(tile_slab_info_[i].cell_slab_size_[j] != NULL)
        delete [] tile_slab_info_[i].cell_slab_size_[j];
    }
    delete [] tile_slab_info_[i].cell_slab_size_;

    if(tile_slab_info_[i].cell_slab_num_ != NULL)
      delete [] tile_slab_info_[i].cell_slab_num_;

    if(tile_slab_info_[i].range_overlap_ != NULL) {
      for(int j=0; j<tile_num; ++j)
        free(tile_slab_info_[i].range_overlap_[j]);
      delete [] tile_slab_info_[i].range_overlap_;
    }

    for(int j=0; j<anum; ++j) {  
      if(tile_slab_info_[i].start_offsets_[j] != NULL)
        delete [] tile_slab_info_[i].start_offsets_[j];
    }
    delete [] tile_slab_info_[i].start_offsets_;

    delete [] tile_slab_info_[i].tile_offset_per_dim_; 
  }
}

void ArraySortedReadState::free_tile_slab_state() {
  // For easy reference
  int anum = (int) attribute_ids_.size();

  // Clean up
  if(tile_slab_state_.current_coords_ != NULL) {
    for(int i=0; i<anum; ++i) 
      free(tile_slab_state_.current_coords_[i]);
    delete [] tile_slab_state_.current_coords_;
  }

  if(tile_slab_state_.copy_tile_slab_done_ != NULL)
    delete [] tile_slab_state_.copy_tile_slab_done_;

  if(tile_slab_state_.current_offsets_ != NULL)
    delete [] tile_slab_state_.current_offsets_;

  if(tile_slab_state_.current_tile_ != NULL)
    delete [] tile_slab_state_.current_tile_;

  if(tile_slab_state_.current_cell_pos_ != NULL)
    delete [] tile_slab_state_.current_cell_pos_;
}

template<class T>
int64_t ArraySortedReadState::get_cell_id(int aid) {
  // For easy reference
  const T* current_coords = (const T*) tile_slab_state_.current_coords_[aid];
  int64_t tid = tile_slab_state_.current_tile_[aid];
  const T* range_overlap = 
      (const T*) tile_slab_info_[copy_id_].range_overlap_[tid];
  int64_t* cell_offset_per_dim = 
      tile_slab_info_[copy_id_].cell_offset_per_dim_[tid];

  // Calculate cell id
  int64_t cid = 0;
  for(int i=0; i<dim_num_; ++i)
    cid += (current_coords[i] - range_overlap[2*i]) * cell_offset_per_dim[i];

  // Return tile id
  return cid;

}

template<class T>
int64_t ArraySortedReadState::get_tile_id(int aid) {
  // For easy reference
  const T* current_coords = (const T*) tile_slab_state_.current_coords_[aid];
  const T* tile_extents = (const T*) array_->array_schema()->tile_extents();
  int64_t* tile_offset_per_dim = tile_slab_info_[copy_id_].tile_offset_per_dim_;

  // Calculate tile id
  int64_t tid = 0;
  for(int i=0; i<dim_num_; ++i)
    tid += (current_coords[i] / tile_extents[i]) * tile_offset_per_dim[i];

  // Return tile id
  return tid;
}

template<class T>
void ArraySortedReadState::handle_copy_requests_dense() {
  // Handle copy requests indefinitely
  for(;;) {
    // Wait for AIO
    wait_aio(copy_id_); 

    // Kill thread, after releasing any blocked resources
    if(copy_thread_canceled_) {
      copy_thread_running_ = false;
      return;
    }

    // Reset the tile slab state
    if(copy_tile_slab_done())
      reset_tile_slab_state<T>();

    // Start the copy
    copy_tile_slab_dense();
 
    // Wait in case of overflow
    if(overflow()) {
      block_overflow();
      block_aio(copy_id_);
      release_copy(0); 
      release_copy(1); 
      wait_overflow();
      continue;
    }

    // Copy is done 
    block_aio(copy_id_);
    release_copy(copy_id_); 
    copy_id_ = (copy_id_ + 1) % 2;
  }
}

template<class T>
void ArraySortedReadState::handle_copy_requests_sparse() {
  // Handle copy requests indefinitely
  for(;;) {
    // Wait for AIO
    wait_aio(copy_id_); 

    // Kill thread, after releasing any blocked resources
    if(copy_thread_canceled_) {
      copy_thread_running_ = false;
      return;
    }

    // Sort the cell positions
    if(copy_tile_slab_done()) {
      reset_tile_slab_state<T>();
      sort_cell_pos<T>();
    }

    // Start the copy
    copy_tile_slab_sparse();
 
    // Wait in case of overflow
    if(overflow()) {
      block_overflow();
      block_aio(copy_id_);
      release_copy(0); 
      release_copy(1); 
      wait_overflow();
      continue;
    }

    // Copy is done 
    block_aio(copy_id_);
    release_copy(copy_id_); 
    copy_id_ = (copy_id_ + 1) % 2;
  }
}

void ArraySortedReadState::init_aio_requests() {
  for(int i=0; i<2; ++i) {
    aio_data_[i] = { i, 0, this };
    aio_request_[i] = {};
    aio_request_[i].buffer_sizes_ = buffer_sizes_tmp_[i];
    aio_request_[i].buffers_ = buffers_[i];
    aio_request_[i].mode_ = TILEDB_ARRAY_READ;
    aio_request_[i].subarray_ = tile_slab_[i];
    aio_request_[i].completion_handle_ = aio_done;
    aio_request_[i].completion_data_ = &(aio_data_[i]); 
    aio_request_[i].overflow_ = aio_overflow_[i];
    aio_request_[i].status_ = &(aio_status_[i]);
  }
}

void ArraySortedReadState::init_copy_state() {
  copy_state_.buffer_sizes_ = NULL;
  copy_state_.buffers_ = NULL;
  copy_state_.buffer_offsets_ = new size_t[buffer_num_];
  for(int i=0; i<buffer_num_; ++i)
    copy_state_.buffer_offsets_[i] = 0;
}

void ArraySortedReadState::init_tile_slab_info() {
  // Do nothing in the case of sparse arrays
  if(!array_->array_schema()->dense())
    return;

  // For easy reference
  int anum = (int) attribute_ids_.size();

  // Initialize
  for(int i=0; i<2; ++i) {
    tile_slab_info_[i].cell_offset_per_dim_ = NULL;
    tile_slab_info_[i].cell_slab_size_ = new size_t*[anum];
    tile_slab_info_[i].cell_slab_num_ = NULL;
    tile_slab_info_[i].range_overlap_ = NULL;
    tile_slab_info_[i].start_offsets_ = new size_t*[anum];
    tile_slab_info_[i].tile_offset_per_dim_ = new int64_t[dim_num_];

    for(int j=0; j<anum; ++j) {
      tile_slab_info_[i].cell_slab_size_[j] = NULL;
      tile_slab_info_[i].start_offsets_[j] = NULL;
    }

    tile_slab_info_[i].tile_num_ = -1;
  }
}

template<class T>
void ArraySortedReadState::init_tile_slab_info(int id) {
  // Sanity check
  assert(array_->array_schema()->dense());

  // For easy reference
  int anum = (int) attribute_ids_.size();

  // Calculate tile number
  int64_t tile_num = array_->array_schema()->tile_num(tile_slab_[id]);

  // Initializations
  tile_slab_info_[id].cell_offset_per_dim_ = new int64_t*[tile_num];
  tile_slab_info_[id].cell_slab_num_ = new int64_t[tile_num];
  tile_slab_info_[id].range_overlap_ = new void*[tile_num];
  for(int64_t i=0; i<tile_num; ++i) {
    tile_slab_info_[id].range_overlap_[i] = malloc(2*coords_size_);
    tile_slab_info_[id].cell_offset_per_dim_[i] = new int64_t[dim_num_];
  }

  for(int i=0; i<anum; ++i) {
    tile_slab_info_[id].cell_slab_size_[i] = new size_t[tile_num];
    tile_slab_info_[id].start_offsets_[i] = new size_t[tile_num];
  }

  tile_slab_info_[id].tile_num_ = tile_num;
}

void ArraySortedReadState::init_tile_slab_state() {
  // For easy reference
  int anum = (int) attribute_ids_.size();
  bool dense = array_->array_schema()->dense();

  // Both for dense and sparse
  tile_slab_state_.copy_tile_slab_done_ = new bool[anum];
  for(int i=0; i<anum; ++i) 
    tile_slab_state_.copy_tile_slab_done_[i] = true; // Important!

  // Allocations and initializations
  if(dense) { // DENSE
    tile_slab_state_.current_offsets_ = new size_t[anum];
    tile_slab_state_.current_coords_ = new void*[anum];
    tile_slab_state_.current_tile_ = new int64_t[anum];
    tile_slab_state_.current_cell_pos_ = NULL;

    for(int i=0; i<anum; ++i) {
      tile_slab_state_.current_coords_[i] = malloc(coords_size_);
      tile_slab_state_.current_offsets_[i] = 0;
      tile_slab_state_.current_tile_[i] = 0;
    }
  } else {   // SPARSE
    tile_slab_state_.current_offsets_ = NULL;
    tile_slab_state_.current_coords_ = NULL;
    tile_slab_state_.current_tile_ = NULL;
    tile_slab_state_.current_cell_pos_ = new int64_t[anum];

    for(int i=0; i<anum; ++i) 
      tile_slab_state_.current_cell_pos_[i] = 0;
  }
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
bool ArraySortedReadState::next_tile_slab_dense_col() {
  // Wait for the previous copy on aio_id_ buffer to be consumed
  wait_copy(aio_id_);

  // Block copy
  block_copy(aio_id_);

  // Quick check if done
  if(read_tile_slabs_done_)
    return false;

  // If the AIO needs to be resumed, exit (no need for a new tile slab)
  if(resume_aio_) {
    resume_aio_ = false;
    return true;
  }

  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  const T* subarray = static_cast<const T*>(subarray_);
  const T* domain = static_cast<const T*>(array_schema->domain());
  const T* tile_extents = static_cast<const T*>(array_schema->tile_extents());
  T* tile_slab[2];
  T* tile_slab_norm = static_cast<T*>(tile_slab_norm_[aio_id_]);
  for(int i=0; i<2; ++i)
    tile_slab[i] = static_cast<T*>(tile_slab_[i]);
  int prev_id = (aio_id_+1)%2;
  T tile_start;

  // Check again if done, this time based on the tile slab and subarray
  if(tile_slab_init_[prev_id] && 
     tile_slab[prev_id][2*(dim_num_-1) + 1] == subarray[2*(dim_num_-1) + 1]) {
    read_tile_slabs_done_ = true;
    return false;
  }

  // If this is the first time this function is called, initialize
  if(!tile_slab_init_[prev_id]) {
    // Crop the subarray extent along the first axis to fit in the first tile
    tile_slab[aio_id_][2*(dim_num_-1)] = subarray[2*(dim_num_-1)]; 
    T upper = subarray[2*(dim_num_-1)] + tile_extents[dim_num_-1];
    T cropped_upper = 
        (upper - domain[2*(dim_num_-1)]) / tile_extents[dim_num_-1] * 
        tile_extents[dim_num_-1] + domain[2*(dim_num_-1)];
    tile_slab[aio_id_][2*(dim_num_-1)+1] = 
        std::min(cropped_upper - 1, subarray[2*(dim_num_-1)+1]); 

    // Leave the rest of the subarray extents intact
    for(int i=0; i<dim_num_-1; ++i) {
      tile_slab[aio_id_][2*i] = subarray[2*i]; 
      tile_slab[aio_id_][2*i+1] = subarray[2*i+1]; 
    } 
  } else { // Calculate a new slab based on the previous
    // Copy previous tile slab
    memcpy(
        tile_slab[aio_id_], 
        tile_slab[prev_id], 
        2*coords_size_);

    // Advance tile slab
    tile_slab[aio_id_][2*(dim_num_-1)] = 
        tile_slab[aio_id_][2*(dim_num_-1)+1] + 1;
    tile_slab[aio_id_][2*(dim_num_-1)+1] = 
        std::min(
            tile_slab[aio_id_][2*(dim_num_-1)] + tile_extents[dim_num_-1] - 1,
            subarray[2*(dim_num_-1)+1]);
  }

  // Calculate normalized tile slab
  for(int i=0; i<dim_num_; ++i) {
    tile_start = 
        ((tile_slab[aio_id_][2*i] - domain[2*i]) / tile_extents[i]) * 
        tile_extents[i] + domain[2*i]; 
    tile_slab_norm[2*i] = tile_slab[aio_id_][2*i] - tile_start; 
    tile_slab_norm[2*i+1] = tile_slab[aio_id_][2*i+1] - tile_start; 
  }

  // Calculate tile slab info and reset tile slab state
  calculate_tile_slab_info<T>(aio_id_);

  // Mark this tile slab as initialized
  tile_slab_init_[aio_id_] = true;

  // Success
  return true;
}

template<class T>
bool ArraySortedReadState::next_tile_slab_dense_row() {
  // Wait for the previous copy on aio_id_ buffer to be consumed
  wait_copy(aio_id_);

  // Block copy
  block_copy(aio_id_);

  // Quick check if done
  if(read_tile_slabs_done_)
    return false;

  // If the AIO needs to be resumed, exit (no need for a new tile slab)
  if(resume_aio_) {
    resume_aio_ = false;
    return true;
  }

  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  const T* subarray = static_cast<const T*>(subarray_);
  const T* domain = static_cast<const T*>(array_schema->domain());
  const T* tile_extents = static_cast<const T*>(array_schema->tile_extents());
  T* tile_slab[2];
  T* tile_slab_norm = static_cast<T*>(tile_slab_norm_[aio_id_]);
  for(int i=0; i<2; ++i)
    tile_slab[i] = static_cast<T*>(tile_slab_[i]);
  int prev_id = (aio_id_+1)%2;
  T tile_start;

  // Check again if done, this time based on the tile slab and subarray
  if(tile_slab_init_[prev_id] && 
     tile_slab[prev_id][1] == subarray[1]) {
    read_tile_slabs_done_ = true;
    return false;
  }

  // If this is the first time this function is called, initialize
  if(!tile_slab_init_[prev_id]) {
    // Crop the subarray extent along the first axis to fit in the first tile
    tile_slab[aio_id_][0] = subarray[0]; 
    T upper = subarray[0] + tile_extents[0];
    T cropped_upper = 
        (upper - domain[0]) / tile_extents[0] * tile_extents[0] + domain[0];
    tile_slab[aio_id_][1] = std::min(cropped_upper - 1, subarray[1]); 

    // Leave the rest of the subarray extents intact
    for(int i=1; i<dim_num_; ++i) {
      tile_slab[aio_id_][2*i] = subarray[2*i]; 
      tile_slab[aio_id_][2*i+1] = subarray[2*i+1]; 
    } 
  } else { // Calculate a new slab based on the previous
    // Copy previous tile slab
    memcpy(
        tile_slab[aio_id_], 
        tile_slab[prev_id], 
        2*coords_size_);

    // Advance tile slab
    tile_slab[aio_id_][0] = tile_slab[aio_id_][1] + 1; 
    tile_slab[aio_id_][1] = std::min(
                                tile_slab[aio_id_][0] + tile_extents[0] - 1,
                                subarray[1]);
  }

  // Calculate normalized tile slab
  for(int i=0; i<dim_num_; ++i) {
    tile_start = 
        ((tile_slab[aio_id_][2*i] - domain[2*i]) / tile_extents[i]) * 
        tile_extents[i] + domain[2*i]; 
    tile_slab_norm[2*i] = tile_slab[aio_id_][2*i] - tile_start; 
    tile_slab_norm[2*i+1] = tile_slab[aio_id_][2*i+1] - tile_start; 
  }

  // Calculate tile slab info and reset tile slab state
  calculate_tile_slab_info<T>(aio_id_);

  // Mark this tile slab as initialized
  tile_slab_init_[aio_id_] = true;

  // Success
  return true;
}

template<class T>
bool ArraySortedReadState::next_tile_slab_sparse_col() {
  // Wait for the previous copy on aio_id_ buffer to be consumed
  wait_copy(aio_id_);

  // Block copy
  block_copy(aio_id_);

  // Quick check if done
  if(read_tile_slabs_done_)
    return false;

  // If the AIO needs to be resumed, exit (no need for a new tile slab)
  if(resume_aio_) {
    resume_aio_ = false;
    return true;
  }

  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  const T* subarray = static_cast<const T*>(subarray_);
  const T* domain = static_cast<const T*>(array_schema->domain());
  const T* tile_extents = static_cast<const T*>(array_schema->tile_extents());
  T* tile_slab[2];
  for(int i=0; i<2; ++i)
    tile_slab[i] = static_cast<T*>(tile_slab_[i]);
  int prev_id = (aio_id_+1)%2;

  // Check again if done, this time based on the tile slab and subarray
  if(tile_slab_init_[prev_id] && 
     tile_slab[prev_id][2*(dim_num_-1) + 1] == subarray[2*(dim_num_-1) + 1]) {
    read_tile_slabs_done_ = true;
    return false;
  }

  // If this is the first time this function is called, initialize
  if(!tile_slab_init_[prev_id]) {
    // Crop the subarray extent along the first axis to fit in the first tile
    tile_slab[aio_id_][2*(dim_num_-1)] = subarray[2*(dim_num_-1)]; 
    T upper = subarray[2*(dim_num_-1)] + tile_extents[dim_num_-1];
    T cropped_upper = 
        (upper - domain[2*(dim_num_-1)]) / tile_extents[dim_num_-1] * 
        tile_extents[dim_num_-1] + domain[2*(dim_num_-1)];
    tile_slab[aio_id_][2*(dim_num_-1)+1] = 
        std::min(cropped_upper - 1, subarray[2*(dim_num_-1)+1]); 

    // Leave the rest of the subarray extents intact
    for(int i=0; i<dim_num_-1; ++i) {
      tile_slab[aio_id_][2*i] = subarray[2*i]; 
      tile_slab[aio_id_][2*i+1] = subarray[2*i+1]; 
    } 
  } else { // Calculate a new slab based on the previous
    // Copy previous tile slab
    memcpy(
        tile_slab[aio_id_], 
        tile_slab[prev_id], 
        2*coords_size_);

    // Advance tile slab
    tile_slab[aio_id_][2*(dim_num_-1)] = 
        tile_slab[aio_id_][2*(dim_num_-1)+1] + 1;
    tile_slab[aio_id_][2*(dim_num_-1)+1] = 
        std::min(
            tile_slab[aio_id_][2*(dim_num_-1)] + tile_extents[dim_num_-1] - 1,
            subarray[2*(dim_num_-1)+1]);
  }

  // Mark this tile slab as initialized
  tile_slab_init_[aio_id_] = true;

  // Success
  return true;
}

template<>
bool ArraySortedReadState::next_tile_slab_sparse_col<float>() {
  // Wait for the previous copy on aio_id_ buffer to be consumed
  wait_copy(aio_id_);

  // Block copy
  block_copy(aio_id_);

  // Quick check if done
  if(read_tile_slabs_done_)
    return false;

  // If the AIO needs to be resumed, exit (no need for a new tile slab)
  if(resume_aio_) {
    resume_aio_ = false;
    return true;
  }

  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  const float* subarray = (const float*) subarray_;
  const float* domain = (const float*) array_schema->domain();
  const float* tile_extents = (const float*) array_schema->tile_extents();
  float* tile_slab[2];
  for(int i=0; i<2; ++i)
    tile_slab[i] = (float*) tile_slab_[i];
  int prev_id = (aio_id_+1)%2;

  // Check again if done, this time based on the tile slab and subarray
  if(tile_slab_init_[prev_id] && 
     tile_slab[prev_id][2*(dim_num_-1) + 1] == subarray[2*(dim_num_-1) + 1]) {
    read_tile_slabs_done_ = true;
    return false;
  }

  // If this is the first time this function is called, initialize
  if(!tile_slab_init_[prev_id]) {
    // Crop the subarray extent along the first axis to fit in the first tile
    tile_slab[aio_id_][2*(dim_num_-1)] = subarray[2*(dim_num_-1)]; 
    float upper = subarray[2*(dim_num_-1)] + tile_extents[dim_num_-1];
    float cropped_upper = 
        floor((upper - domain[2*(dim_num_-1)]) / tile_extents[dim_num_-1]) * 
        tile_extents[dim_num_-1] + domain[2*(dim_num_-1)];
    tile_slab[aio_id_][2*(dim_num_-1)+1] = 
        std::min(cropped_upper - FLT_MIN, subarray[2*(dim_num_-1)+1]); 

    // Leave the rest of the subarray extents intact
    for(int i=0; i<dim_num_-1; ++i) {
      tile_slab[aio_id_][2*i] = subarray[2*i]; 
      tile_slab[aio_id_][2*i+1] = subarray[2*i+1]; 
    } 
  } else { // Calculate a new slab based on the previous
    // Copy previous tile slab
    memcpy(
        tile_slab[aio_id_], 
        tile_slab[prev_id], 
        2*coords_size_);

    // Advance tile slab
    tile_slab[aio_id_][2*(dim_num_-1)] = 
        tile_slab[aio_id_][2*(dim_num_-1)+1] + FLT_MIN;
    tile_slab[aio_id_][2*(dim_num_-1)+1] = 
        std::min(
            tile_slab[aio_id_][2*(dim_num_-1)] + 
                tile_extents[dim_num_-1] - FLT_MIN,
            subarray[2*(dim_num_-1)+1]);
  }

  // Mark this tile slab as initialized
  tile_slab_init_[aio_id_] = true;

  // Success
  return true;
}

template<>
bool ArraySortedReadState::next_tile_slab_sparse_col<double>() {
  // Wait for the previous copy on aio_id_ buffer to be consumed
  wait_copy(aio_id_);

  // Block copy
  block_copy(aio_id_);

  // Quick check if done
  if(read_tile_slabs_done_)
    return false;

  // If the AIO needs to be resumed, exit (no need for a new tile slab)
  if(resume_aio_) {
    resume_aio_ = false;
    return true;
  }

  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  const double* subarray = (const double*) subarray_;
  const double* domain = (const double*) array_schema->domain();
  const double* tile_extents = (const double*) array_schema->tile_extents();
  double* tile_slab[2];
  for(int i=0; i<2; ++i)
    tile_slab[i] = (double*) tile_slab_[i];
  int prev_id = (aio_id_+1)%2;

  // Check again if done, this time based on the tile slab and subarray
  if(tile_slab_init_[prev_id] && 
     tile_slab[prev_id][2*(dim_num_-1) + 1] == subarray[2*(dim_num_-1) + 1]) {
    read_tile_slabs_done_ = true;
    return false;
  }

  // If this is the first time this function is called, initialize
  if(!tile_slab_init_[prev_id]) {
    // Crop the subarray extent along the first axis to fit in the first tile
    tile_slab[aio_id_][2*(dim_num_-1)] = subarray[2*(dim_num_-1)]; 
    double upper = subarray[2*(dim_num_-1)] + tile_extents[dim_num_-1];
    double cropped_upper = 
        floor((upper - domain[2*(dim_num_-1)]) / tile_extents[dim_num_-1]) * 
        tile_extents[dim_num_-1] + domain[2*(dim_num_-1)];
    tile_slab[aio_id_][2*(dim_num_-1)+1] = 
        std::min(cropped_upper - DBL_MIN, subarray[2*(dim_num_-1)+1]); 

    // Leave the rest of the subarray extents intact
    for(int i=0; i<dim_num_-1; ++i) {
      tile_slab[aio_id_][2*i] = subarray[2*i]; 
      tile_slab[aio_id_][2*i+1] = subarray[2*i+1]; 
    } 
  } else { // Calculate a new slab based on the previous
    // Copy previous tile slab
    memcpy(
        tile_slab[aio_id_], 
        tile_slab[prev_id], 
        2*coords_size_);

    // Advance tile slab
    tile_slab[aio_id_][2*(dim_num_-1)] = 
        tile_slab[aio_id_][2*(dim_num_-1)+1] + DBL_MIN;
    tile_slab[aio_id_][2*(dim_num_-1)+1] = 
        std::min(
            tile_slab[aio_id_][2*(dim_num_-1)] + 
                tile_extents[dim_num_-1] - DBL_MIN,
            subarray[2*(dim_num_-1)+1]);
  }

  // Mark this tile slab as initialized
  tile_slab_init_[aio_id_] = true;

  // Success
  return true;
}

template<class T>
bool ArraySortedReadState::next_tile_slab_sparse_row() {
  // Wait for the previous copy on aio_id_ buffer to be consumed
  wait_copy(aio_id_);

  // Block copy
  block_copy(aio_id_);

  // Quick check if done
  if(read_tile_slabs_done_)
    return false;

  // If the AIO needs to be resumed, exit (no need for a new tile slab)
  if(resume_aio_) {
    resume_aio_ = false;
    return true;
  }

  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  const T* subarray = static_cast<const T*>(subarray_);
  const T* domain = static_cast<const T*>(array_schema->domain());
  const T* tile_extents = static_cast<const T*>(array_schema->tile_extents());
  T* tile_slab[2];
  for(int i=0; i<2; ++i)
    tile_slab[i] = static_cast<T*>(tile_slab_[i]);
  int prev_id = (aio_id_+1)%2;

  // Check again if done, this time based on the tile slab and subarray
  if(tile_slab_init_[prev_id] && 
     tile_slab[prev_id][1] == subarray[1]) {
    read_tile_slabs_done_ = true;
    return false;
  }

  // If this is the first time this function is called, initialize
  if(!tile_slab_init_[prev_id]) {
    // Crop the subarray extent along the first axis to fit in the first tile
    tile_slab[aio_id_][0] = subarray[0]; 
    T upper = subarray[0] + tile_extents[0];
    T cropped_upper = 
        (upper - domain[0]) / tile_extents[0] * tile_extents[0] + domain[0];
    tile_slab[aio_id_][1] = std::min(cropped_upper - 1, subarray[1]); 

    // Leave the rest of the subarray extents intact
    for(int i=1; i<dim_num_; ++i) {
      tile_slab[aio_id_][2*i] = subarray[2*i]; 
      tile_slab[aio_id_][2*i+1] = subarray[2*i+1]; 
    } 
  } else { // Calculate a new slab based on the previous
    // Copy previous tile slab
    memcpy(
        tile_slab[aio_id_], 
        tile_slab[prev_id], 
        2*coords_size_);

    // Advance tile slab
    tile_slab[aio_id_][0] = tile_slab[aio_id_][1] + 1; 
    tile_slab[aio_id_][1] = std::min(
                                tile_slab[aio_id_][0] + tile_extents[0] - 1,
                                subarray[1]);
  }

  // Mark this tile slab as initialized
  tile_slab_init_[aio_id_] = true;

  // Success
  return true;
}

template<>
bool ArraySortedReadState::next_tile_slab_sparse_row<float>() {
  // Wait for the previous copy on aio_id_ buffer to be consumed
  wait_copy(aio_id_);

  // Block copy
  block_copy(aio_id_);

  // Quick check if done
  if(read_tile_slabs_done_)
    return false;

  // If the AIO needs to be resumed, exit (no need for a new tile slab)
  if(resume_aio_) {
    resume_aio_ = false;
    return true;
  }

  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  const float* subarray = (const float*) subarray_;
  const float* domain = (const float*) array_schema->domain();
  const float* tile_extents = (const float*) array_schema->tile_extents();
  float* tile_slab[2];
  for(int i=0; i<2; ++i)
    tile_slab[i] = (float*) tile_slab_[i];
  int prev_id = (aio_id_+1)%2;

  // Check again if done, this time based on the tile slab and subarray
  if(tile_slab_init_[prev_id] && 
     tile_slab[prev_id][1] == subarray[1]) {
    read_tile_slabs_done_ = true;
    return false;
  }

  // If this is the first time this function is called, initialize
  if(!tile_slab_init_[prev_id]) {
    // Crop the subarray extent along the first axis to fit in the first tile
    tile_slab[aio_id_][0] = subarray[0]; 
    float upper = subarray[0] + tile_extents[0];
    float cropped_upper = 
        floor((upper - domain[0]) / tile_extents[0]) * tile_extents[0] + 
        domain[0];
    tile_slab[aio_id_][1] = std::min(cropped_upper - FLT_MIN, subarray[1]); 

    // Leave the rest of the subarray extents intact
    for(int i=1; i<dim_num_; ++i) {
      tile_slab[aio_id_][2*i] = subarray[2*i]; 
      tile_slab[aio_id_][2*i+1] = subarray[2*i+1]; 
    } 
  } else { // Calculate a new slab based on the previous
    // Copy previous tile slab
    memcpy(
        tile_slab[aio_id_], 
        tile_slab[prev_id], 
        2*coords_size_);

    // Advance tile slab
    tile_slab[aio_id_][0] = tile_slab[aio_id_][1] + FLT_MIN; 
    tile_slab[aio_id_][1] = 
        std::min(
            tile_slab[aio_id_][0] + tile_extents[0] - FLT_MIN,
            subarray[1]);
  }

  // Mark this tile slab as initialized
  tile_slab_init_[aio_id_] = true;

  // Success
  return true;
}

template<>
bool ArraySortedReadState::next_tile_slab_sparse_row<double>() {
  // Wait for the previous copy on aio_id_ buffer to be consumed
  wait_copy(aio_id_);

  // Block copy
  block_copy(aio_id_);

  // Quick check if done
  if(read_tile_slabs_done_)
    return false;

  // If the AIO needs to be resumed, exit (no need for a new tile slab)
  if(resume_aio_) {
    resume_aio_ = false;
    return true;
  }

  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  const double* subarray = (const double*) subarray_;
  const double* domain = (const double*) array_schema->domain();
  const double* tile_extents = (const double*) array_schema->tile_extents();
  double* tile_slab[2];
  for(int i=0; i<2; ++i)
    tile_slab[i] = (double*) tile_slab_[i];
  int prev_id = (aio_id_+1)%2;

  // Check again if done, this time based on the tile slab and subarray
  if(tile_slab_init_[prev_id] && 
     tile_slab[prev_id][1] == subarray[1]) {
    read_tile_slabs_done_ = true;
    return false;
  }

  // If this is the first time this function is called, initialize
  if(!tile_slab_init_[prev_id]) {
    // Crop the subarray extent along the first axis to fit in the first tile
    tile_slab[aio_id_][0] = subarray[0]; 
    double upper = subarray[0] + tile_extents[0];
    double cropped_upper = 
        floor((upper - domain[0]) / tile_extents[0]) * tile_extents[0] + 
        domain[0];
    tile_slab[aio_id_][1] = std::min(cropped_upper - DBL_MIN, subarray[1]); 

    // Leave the rest of the subarray extents intact
    for(int i=1; i<dim_num_; ++i) {
      tile_slab[aio_id_][2*i] = subarray[2*i]; 
      tile_slab[aio_id_][2*i+1] = subarray[2*i+1]; 
    } 
  } else { // Calculate a new slab based on the previous
    // Copy previous tile slab
    memcpy(
        tile_slab[aio_id_], 
        tile_slab[prev_id], 
        2*coords_size_);

    // Advance tile slab
    tile_slab[aio_id_][0] = tile_slab[aio_id_][1] + DBL_MIN; 
    tile_slab[aio_id_][1] = 
        std::min(
            tile_slab[aio_id_][0] + tile_extents[0] - DBL_MIN,
            subarray[1]);
  }

  // Mark this tile slab as initialized
  tile_slab_init_[aio_id_] = true;

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
    return TILEDB_ASRS_ERR;
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
    return array_->read_default(
               copy_state_.buffers_, 
               copy_state_.buffer_sizes_);

  // Iterate over each tile slab
  while(next_tile_slab_dense_col<T>()) {
    // Read the next tile slab with the default cell order
    if(read_tile_slab() != TILEDB_ASRS_OK) 
      return TILEDB_ASRS_ERR;

    // Handle overflow
    if(resume_aio_)
      break;
  }

  // Wait for copy to finish
  int copy_id = (resume_aio_) ? aio_id_ : (aio_id_ + 1) % 2;
  wait_copy(copy_id);

  // Assign the true buffer sizes
  for(int i=0; i<buffer_num_; ++i) 
    copy_state_.buffer_sizes_[i] = copy_state_.buffer_offsets_[i];

  // The following will make the copy thread terminate
  if(done()) { 
    copy_thread_canceled_ = true;
    release_aio(aio_id_);
  }

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
    return array_->read_default(
               copy_state_.buffers_, 
               copy_state_.buffer_sizes_);

  // Iterate over each tile slab
  while(next_tile_slab_dense_row<T>()) {
    // Read the next tile slab with the default cell order
    if(read_tile_slab() != TILEDB_ASRS_OK)
      return TILEDB_ASRS_ERR;

    // Handle overflow
    if(resume_aio_)
      break;
  }

  // Wait for copy and AIO to finish
  int copy_id = (resume_aio_) ? aio_id_ : (aio_id_ + 1) % 2;
  wait_copy(copy_id);

  // Assign the true buffer sizes
  for(int i=0; i<buffer_num_; ++i) 
    copy_state_.buffer_sizes_[i] = copy_state_.buffer_offsets_[i];

  // The following will make the copy thread terminate
  if(done()) { 
    copy_thread_canceled_ = true;
    release_aio(aio_id_);
  }

  // Success
  return TILEDB_ASRS_OK;
}

template<class T>
int ArraySortedReadState::read_sparse_sorted_col() {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  const T* subarray = static_cast<const T*>(subarray_);

  // Check if this can be satisfied with a default read
  if(array_schema->cell_order() == TILEDB_COL_MAJOR &&
     array_schema->is_contained_in_tile_slab_row<T>(subarray))
    return array_->read_default(
               copy_state_.buffers_, 
               copy_state_.buffer_sizes_);

  // Iterate over each tile slab
  while(next_tile_slab_sparse_col<T>()) {
    // Read the next tile slab with the default cell order
    if(read_tile_slab() != TILEDB_ASRS_OK) 
      return TILEDB_ASRS_ERR;

    // Handle overflow
    if(resume_aio_)
      break;
  }

  // Wait for copy to finish
  int copy_id = (resume_aio_) ? aio_id_ : (aio_id_ + 1) % 2;
  wait_copy(copy_id);

  // Assign the true buffer sizes
  int buffer_num = buffer_num_ - (int) extra_coords_;
  for(int i=0; i<buffer_num; ++i) 
    copy_state_.buffer_sizes_[i] = copy_state_.buffer_offsets_[i];

  // The following will make the copy thread terminate
  if(done()) { 
    copy_thread_canceled_ = true;
    release_aio(aio_id_);
  }

  // Success
  return TILEDB_ASRS_OK;
}

template<class T>
int ArraySortedReadState::read_sparse_sorted_row() {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  const T* subarray = static_cast<const T*>(subarray_);

  // Check if this can be satisfied with a default read
  if(array_schema->cell_order() == TILEDB_ROW_MAJOR &&
     array_schema->is_contained_in_tile_slab_col<T>(subarray))
    return array_->read_default(
               copy_state_.buffers_, 
               copy_state_.buffer_sizes_);

  // Iterate over each tile slab
  while(next_tile_slab_sparse_row<T>()) {
    // Read the next tile slab with the default cell order
    if(read_tile_slab() != TILEDB_ASRS_OK)
      return TILEDB_ASRS_ERR;

    // Handle overflow
    if(resume_aio_)
      break;
  }

  // Wait for copy and AIO to finish
  int copy_id = (resume_aio_) ? aio_id_ : (aio_id_ + 1) % 2;
  wait_copy(copy_id);

  // Assign the true buffer sizes
  int buffer_num = buffer_num_ - (int) extra_coords_;
  for(int i=0; i<buffer_num; ++i) 
    copy_state_.buffer_sizes_[i] = copy_state_.buffer_offsets_[i];

  // The following will make the copy thread terminate
  if(done()) { 
    copy_thread_canceled_ = true;
    release_aio(aio_id_);
  }

  // Success
  return TILEDB_ASRS_OK; 
}

int ArraySortedReadState::read_tile_slab() {
  // We need to exit if the copy did no complete (due to overflow)
  if(resume_copy_) {
    resume_aio_ = true;
    return TILEDB_ASRS_OK;
  }

  // Reset AIO overflow flags
  reset_aio_overflow(aio_id_);

  // Reset temporary buffer sizes
  reset_buffer_sizes_tmp(aio_id_);

  // Send AIO request
  if(send_aio_request(aio_id_) != TILEDB_ASRS_OK)
    return TILEDB_ASRS_ERR;

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

void ArraySortedReadState::reset_aio_overflow(int aio_id) {
  // For easy reference
  int anum = (int) attribute_ids_.size();

  // Reset aio_overflow_
  for(int i=0; i<anum; ++i)
    aio_overflow_[aio_id][i] = false;
}

void ArraySortedReadState::reset_buffer_sizes_tmp(int id) {
  for(int i=0; i<buffer_num_; ++i)
    buffer_sizes_tmp_[id][i] = buffer_sizes_[id][i];
}

void ArraySortedReadState::reset_copy_state(
    void** buffers,
    size_t* buffer_sizes) {
  copy_state_.buffers_ = buffers;
  copy_state_.buffer_sizes_ = buffer_sizes;
  for(int i=0; i<buffer_num_; ++i)
    copy_state_.buffer_offsets_[i] = 0; 
}

void ArraySortedReadState::reset_overflow() {
  for(int i=0; i<(int) attribute_ids_.size(); ++i)
    overflow_[i] = false;
}

template<class T>
void ArraySortedReadState::reset_tile_coords() {
  T* tile_coords = (T*) tile_coords_;
  for(int i=0; i<dim_num_; ++i)
    tile_coords[i] = 0;
}

template<class T>
void ArraySortedReadState::reset_tile_slab_state() {
  // For easy reference
  int anum = (int) attribute_ids_.size();
  bool dense = array_->array_schema()->dense();

  // Both dense and sparse
  for(int i=0; i<anum; ++i) 
    tile_slab_state_.copy_tile_slab_done_[i] = false;

  if(dense) { // DENSE
    T** current_coords = (T**) tile_slab_state_.current_coords_;
    const T* tile_slab = (const T*) tile_slab_norm_[copy_id_];

    // Reset values
    for(int i=0; i<anum; ++i) {
      tile_slab_state_.current_offsets_[i] = 0; 
      tile_slab_state_.current_tile_[i] = 0;
      for(int j=0; j<dim_num_; ++j)
        current_coords[i][j] = tile_slab[2*j];
    }
  } else {   // SPARSE
    for(int i=0; i<anum; ++i) 
      tile_slab_state_.current_cell_pos_[i] = 0;
  }
}

int ArraySortedReadState::send_aio_request(int aio_id) { 
  // Important!!
  aio_request_[aio_id].id_ = aio_cnt_++;

  // For easy reference
  Array* array_clone = array_->array_clone();

  // Sanity check
  assert(array_clone != NULL);

  // Send the AIO request to the clone array
  if(array_clone->aio_read(&(aio_request_[aio_id])) != TILEDB_AR_OK) {
    tiledb_asrs_errmsg = tiledb_ar_errmsg;
    return TILEDB_ASRS_ERR;
  }

  // Success
  return TILEDB_ASRS_OK;
}

template<class T>
void ArraySortedReadState::sort_cell_pos() {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  int dim_num = array_schema->dim_num();
  int64_t cell_num = buffer_sizes_tmp_[copy_id_][coords_buf_i_] / coords_size_; 
  int mode = array_->mode();
  const T* buffer = static_cast<const T*>(buffers_[copy_id_][coords_buf_i_]);

  // Populate cell_pos
  cell_pos_.resize(cell_num);
  for(int i=0; i<cell_num; ++i) 
    cell_pos_[i] = i;

  // Invoke the proper sort function, based on the mode
  if(mode == TILEDB_ARRAY_READ_SORTED_ROW) {
    // Sort cell positions
    SORT(
        cell_pos_.begin(), 
        cell_pos_.end(), 
        SmallerRow<T>(buffer, dim_num)); 
  } else { // mode == TILEDB_ARRAY_READ_SORTED_COL
    // Sort cell positions
    SORT(
        cell_pos_.begin(), 
        cell_pos_.end(), 
        SmallerCol<T>(buffer, dim_num)); 
  } 
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

template<class T>
void ArraySortedReadState::update_current_tile_and_offset(int aid) {
  // For easy reference
  int64_t& tid = tile_slab_state_.current_tile_[aid];
  size_t& current_offset = tile_slab_state_.current_offsets_[aid];
  int64_t cid;

  // Calculate the new tile id
  tid = get_tile_id<T>(aid);

  // Calculate the cell id
  cid = get_cell_id<T>(aid); 

  // Calculate new offset
  current_offset = 
      tile_slab_info_[copy_id_].start_offsets_[aid][tid] + 
      cid * attribute_sizes_[aid];
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
  // Lock overflow mutex
  if(lock_overflow_mtx() != TILEDB_ASRS_OK)
    return TILEDB_ASRS_ERR; 

  // Wait to be signaled
  while(overflow()) {
    if(pthread_cond_wait(&overflow_cond_, &overflow_mtx_)) {
      std::string errmsg = "Cannot wait on IO mutex condition";
      PRINT_ERROR(errmsg);
      tiledb_asrs_errmsg = TILEDB_ASRS_ERRMSG + errmsg;
      return TILEDB_ASRS_ERR;
    }
  }

  // Unlock overflow mutex
  if(unlock_overflow_mtx() != TILEDB_ASRS_OK)
    return TILEDB_ASRS_ERR; 

  // Success
  return TILEDB_ASRS_OK;
}

// Explicit template instantiations

template int ArraySortedReadState::read_dense_sorted_col<int>();
template int ArraySortedReadState::read_dense_sorted_col<int64_t>();
template int ArraySortedReadState::read_dense_sorted_col<float>();
template int ArraySortedReadState::read_dense_sorted_col<double>();

template int ArraySortedReadState::read_dense_sorted_row<int>();
template int ArraySortedReadState::read_dense_sorted_row<int64_t>();
template int ArraySortedReadState::read_dense_sorted_row<float>();
template int ArraySortedReadState::read_dense_sorted_row<double>();
