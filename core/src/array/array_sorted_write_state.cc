/**
 * @file   array_sorted_write_state.cc
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
 * This file implements the ArraySortedWriteState class.
 */

#include "array_sorted_write_state.h"
#include "math.h"
#include "utils.h"
#include <cassert>




/* ****************************** */
/*             MACROS             */
/* ****************************** */

#ifdef VERBOSE
#  define PRINT_ERROR(x) std::cerr << TILEDB_ASWS_ERRMSG << x << ".\n" 
#else
#  define PRINT_ERROR(x) do { } while(0) 
#endif




/* ****************************** */
/*         GLOBAL VARIABLES       */
/* ****************************** */

std::string tiledb_asws_errmsg = "";




/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

ArraySortedWriteState::ArraySortedWriteState(
    Array* array)
    : array_(array),
      attribute_ids_(array->attribute_ids()) {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  int anum = (int) attribute_ids_.size();

  // Initializations
  aio_cnt_ = 0;
  aio_id_ = 0;
  aio_thread_running_ = false;
  aio_thread_canceled_ = false;
  coords_size_ = array_schema->coords_size();
  copy_id_ = 0;
  dim_num_ = array_schema->dim_num();
  tile_coords_ = NULL;
  tile_domain_ = NULL;
  buffer_sizes_ = NULL;
  buffers_ = NULL;
  for(int i=0; i<2; ++i) {
    tile_slab_[i] = malloc(2*coords_size_);
    tile_slab_norm_[i] = malloc(2*coords_size_);
    tile_slab_init_[i] = false;
    wait_copy_[i] = true;
    wait_aio_[i] = false;
  }
  for(int i=0; i<anum; ++i) {
    if(array_schema->var_size(attribute_ids_[i]))
      attribute_sizes_.push_back(sizeof(size_t));
    else
      attribute_sizes_.push_back(array_schema->cell_size(attribute_ids_[i]));
  }

  subarray_ = malloc(2*coords_size_);
  memcpy(subarray_, array_->subarray(), 2*coords_size_);

  // Calculate expanded subarray
  expanded_subarray_ = malloc(2*coords_size_);
  memcpy(expanded_subarray_, subarray_, 2*coords_size_);
  array_schema->expand_domain(expanded_subarray_);

  // Calculate number of buffers
  calculate_buffer_num();

  // Initialize tile slab info and state, and copy state
  init_tile_slab_info();
  init_tile_slab_state();
  init_copy_state();
}

ArraySortedWriteState::~ArraySortedWriteState() { 
  // Clean up
  free(subarray_);
  free(expanded_subarray_);
  free(tile_coords_);
  free(tile_domain_);

  for(int i=0; i<2; ++i) {
    free(tile_slab_[i]);
    free(tile_slab_norm_[i]);
  } 
  
  // Free tile slab info and state, and copy state
  free_copy_state();
  free_tile_slab_state();
  free_tile_slab_info();

  // Cancel AIO thread
  aio_thread_canceled_ = true;
  for(int i=0; i<2; ++i)
    release_copy(i);

  // Wait for thread to be destroyed
  while(aio_thread_running_);

  // Join with the terminated thread
  pthread_join(aio_thread_, NULL);

  // Destroy conditions and mutexes
  for(int i=0; i<2; ++i) {
    if(pthread_cond_destroy(&(aio_cond_[i]))) {
      std::string errmsg = "Cannot destroy AIO mutex condition";
      PRINT_ERROR(errmsg);
      tiledb_asws_errmsg = TILEDB_ASWS_ERRMSG + errmsg;
    }
    if(pthread_cond_destroy(&(copy_cond_[i]))) {
      std::string errmsg = "Cannot destroy copy mutex condition";
      PRINT_ERROR(errmsg);
      tiledb_asws_errmsg = TILEDB_ASWS_ERRMSG + errmsg;
    }
  }
  if(pthread_mutex_destroy(&aio_mtx_)) {
    std::string errmsg = "Cannot destroy AIO mutex";
    PRINT_ERROR(errmsg);
    tiledb_asws_errmsg = TILEDB_ASWS_ERRMSG + errmsg;
  }
  if(pthread_mutex_destroy(&copy_mtx_)) {
    std::string errmsg = "Cannot destroy copy mutex";
    PRINT_ERROR(errmsg);
    tiledb_asws_errmsg = TILEDB_ASWS_ERRMSG + errmsg;
  }
}




/* ****************************** */
/*            MUTATORS            */
/* ****************************** */

int ArraySortedWriteState::init() {
  // Initialize the mutexes and conditions
  if(pthread_mutex_init(&aio_mtx_, NULL)) {
       std::string errmsg = "Cannot initialize IO mutex";
      PRINT_ERROR(errmsg);
      tiledb_asws_errmsg = TILEDB_ASWS_ERRMSG + errmsg; 
      return TILEDB_ASWS_ERR;
  }
  if(pthread_mutex_init(&copy_mtx_, NULL)) {
    std::string errmsg = "Cannot initialize copy mutex";
    PRINT_ERROR(errmsg);
    tiledb_asws_errmsg = TILEDB_ASWS_ERRMSG + errmsg; 
    return TILEDB_ASWS_ERR;
  }
  for(int i=0; i<2; ++i) {
    aio_cond_[i] = PTHREAD_COND_INITIALIZER; 
    if(pthread_cond_init(&(aio_cond_[i]), NULL)) {
      std::string errmsg = "Cannot initialize IO mutex condition";
      PRINT_ERROR(errmsg);
      tiledb_asws_errmsg = TILEDB_ASWS_ERRMSG + errmsg; 
      return TILEDB_ASWS_ERR;
    }
    copy_cond_[i] = PTHREAD_COND_INITIALIZER; 
    if(pthread_cond_init(&(copy_cond_[i]), NULL)) {
      std::string errmsg = "Cannot initialize copy mutex condition";
      PRINT_ERROR(errmsg);
      tiledb_asws_errmsg = TILEDB_ASWS_ERRMSG + errmsg; 
      return TILEDB_ASWS_ERR;
    }
  }

  // Initialize functors
  const ArraySchema* array_schema = array_->array_schema();
  int mode = array_->mode();
  int cell_order = array_schema->cell_order();
  int tile_order = array_schema->tile_order();
  int coords_type = array_schema->coords_type();
  if(mode == TILEDB_ARRAY_WRITE_SORTED_ROW) { 
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
    } else {
      assert(0);
    }
  } else { // mode == TILEDB_ARRAY_WRITE_SORTED_COL 
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
    } else {
      assert(0);
    }
  }
  if(tile_order == TILEDB_ROW_MAJOR) { 
    if(coords_type == TILEDB_INT32) 
      calculate_tile_slab_info_ = calculate_tile_slab_info_row<int>;
    else if(coords_type == TILEDB_INT64) 
      calculate_tile_slab_info_ = calculate_tile_slab_info_row<int64_t>;
    else 
      assert(0);
  } else { // tile_order == TILEDB_COL_MAJOR
    if(coords_type == TILEDB_INT32) 
      calculate_tile_slab_info_ = calculate_tile_slab_info_col<int>;
    else if(coords_type == TILEDB_INT64) 
      calculate_tile_slab_info_ = calculate_tile_slab_info_col<int64_t>;
    else 
      assert(0);
  }

  // Create the thread that will be handling all the asynchronous IOs
  if(pthread_create(
         &aio_thread_, 
         NULL, 
         ArraySortedWriteState::aio_handler, 
         this)) {
    std::string errmsg = "Cannot create AIO thread";
    PRINT_ERROR(errmsg);
    tiledb_asws_errmsg = TILEDB_ASWS_ERRMSG + errmsg; 
    return TILEDB_ASWS_ERR;
  }
  aio_thread_running_ = true;

  // Success
  return TILEDB_ASWS_OK;
}

int ArraySortedWriteState::write(
    const void** buffers, 
    const size_t* buffer_sizes) {
  // Locally store user buffer information
  create_user_buffers(buffers, buffer_sizes);

  // Create buffers
  if(create_copy_state_buffers() != TILEDB_ASWS_OK)
    return TILEDB_ASWS_ERR;

  // Create AIO requests
  init_aio_requests();

  // Call the appropriate templated read
  int type = array_->array_schema()->coords_type();
  if(type == TILEDB_INT32) {
    return write<int>();
  } else if(type == TILEDB_INT64) {
    return write<int64_t>();
  } else {
    assert(0);
    return TILEDB_ASWS_ERR;
  }

  // Success
  return TILEDB_ASWS_OK;
} 


/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */


template<class T>
void *ArraySortedWriteState::advance_cell_slab_col_s(void* data) {
  ArraySortedWriteState* asws = ((ASWS_Data*) data)->asws_;
  int aid = ((ASWS_Data*) data)->id_;
  asws->advance_cell_slab_col<T>(aid);
  return NULL;
}

template<class T>
void *ArraySortedWriteState::advance_cell_slab_row_s(void* data) {
  ArraySortedWriteState* asws = ((ASWS_Data*) data)->asws_;
  int aid = ((ASWS_Data*) data)->id_;
  asws->advance_cell_slab_row<T>(aid);
  return NULL;
}


template<class T>
void ArraySortedWriteState::advance_cell_slab_col(int aid) {
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
void ArraySortedWriteState::advance_cell_slab_row(int aid) {
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

void *ArraySortedWriteState::aio_done(void* data) {
  // Retrieve data
  ArraySortedWriteState* asws = ((ASWS_Data*) data)->asws_;
  int id = ((ASWS_Data*) data)->id_;

  // Manage the mutexes and conditions
  asws->release_aio(id); 
 
  return NULL;
}

void ArraySortedWriteState::block_aio(int id) {
  lock_aio_mtx();
  wait_aio_[id] = true; 
  unlock_aio_mtx();
}

void ArraySortedWriteState::block_copy(int id) {
  lock_copy_mtx();
  wait_copy_[id] = true; 
  unlock_copy_mtx();
}

void ArraySortedWriteState::calculate_buffer_num() {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();

  // Calculate number of buffers
  buffer_num_ = 0;
  int attribute_id_num = (int) attribute_ids_.size();
  for(int i=0; i<attribute_id_num; ++i) {
    // Fix-sized attribute
    if(!array_schema->var_size(attribute_ids_[i])) 
      ++buffer_num_;
    else   // Variable-sized attribute
      buffer_num_ += 2;
  }
}

template<class T>
void *ArraySortedWriteState::calculate_cell_slab_info_col_col_s(void* data) {
  ArraySortedWriteState* asws = ((ASWS_Data*) data)->asws_;
  int id = ((ASWS_Data*) data)->id_;
  int tid = ((ASWS_Data*) data)->id_2_;
  asws->calculate_cell_slab_info_col_col<T>(id, tid);
  return NULL;
}

template<class T>
void *ArraySortedWriteState::calculate_cell_slab_info_col_row_s(void* data) {
  ArraySortedWriteState* asws = ((ASWS_Data*) data)->asws_;
  int id = ((ASWS_Data*) data)->id_;
  int tid = ((ASWS_Data*) data)->id_2_;
  asws->calculate_cell_slab_info_col_row<T>(id, tid);
  return NULL;
}

template<class T>
void *ArraySortedWriteState::calculate_cell_slab_info_row_col_s(void* data) {
  ArraySortedWriteState* asws = ((ASWS_Data*) data)->asws_;
  int id = ((ASWS_Data*) data)->id_;
  int tid = ((ASWS_Data*) data)->id_2_;
  asws->calculate_cell_slab_info_row_col<T>(id, tid);
  return NULL;
}

template<class T>
void *ArraySortedWriteState::calculate_cell_slab_info_row_row_s(void* data) {
  ArraySortedWriteState* asws = ((ASWS_Data*) data)->asws_;
  int id = ((ASWS_Data*) data)->id_;
  int tid = ((ASWS_Data*) data)->id_2_;
  asws->calculate_cell_slab_info_row_row<T>(id, tid);
  return NULL;
}

template<class T>
void ArraySortedWriteState::calculate_cell_slab_info_col_col(
    int id, 
    int64_t tid) {
  // For easy reference
  int anum = (int) attribute_ids_.size();
  const T* range_overlap = (const T*) tile_slab_info_[id].range_overlap_[tid];
  const T* tile_extents = (const T*) array_->array_schema()->tile_extents();
  int64_t cell_num; 
  
  // Calculate number of cells in cell slab
  cell_num = range_overlap[1] - range_overlap[0] + 1;
  tile_slab_info_[id].cell_slab_num_[tid] = cell_num;

  // Calculate size of a cell slab per attribute
  for(int aid=0; aid<anum; ++aid)
    tile_slab_info_[id].cell_slab_size_[aid][tid] = 
        tile_slab_info_[id].cell_slab_num_[tid] * attribute_sizes_[aid];

  // Calculate cell offset per dimension 
  int64_t cell_offset = 1;
  tile_slab_info_[id].cell_offset_per_dim_[tid][0] = cell_offset;
  for(int i=1; i<dim_num_; ++i) {
    cell_offset *= tile_extents[i-1]; 
    tile_slab_info_[id].cell_offset_per_dim_[tid][i] = cell_offset;
  }
}

template<class T>
void ArraySortedWriteState::calculate_cell_slab_info_row_row(
    int id, 
    int64_t tid) {
  // For easy reference
  int anum = (int) attribute_ids_.size();
  const T* range_overlap = (const T*) tile_slab_info_[id].range_overlap_[tid];
  const T* tile_extents = (const T*) array_->array_schema()->tile_extents();
  int64_t cell_num; 

  // Calculate number of cells in cell slab
  cell_num = range_overlap[2*(dim_num_-1)+1] - range_overlap[2*(dim_num_-1)] +1;
  tile_slab_info_[id].cell_slab_num_[tid] = cell_num;

  // Calculate size of a cell slab per attribute
  for(int aid=0; aid<anum; ++aid)
    tile_slab_info_[id].cell_slab_size_[aid][tid] = 
        tile_slab_info_[id].cell_slab_num_[tid] * attribute_sizes_[aid];

  // Calculate cell offset per dimension 
  int64_t cell_offset = 1;
  tile_slab_info_[id].cell_offset_per_dim_[tid][dim_num_-1] = cell_offset;
  for(int i=dim_num_-2; i>=0; --i) {
    cell_offset *= tile_extents[i+1]; 
    tile_slab_info_[id].cell_offset_per_dim_[tid][i] = cell_offset;
  }
}

template<class T>
void ArraySortedWriteState::calculate_cell_slab_info_col_row(
    int id, 
    int64_t tid) {
  // For easy reference
  int anum = (int) attribute_ids_.size();
  const T* tile_extents = (const T*) array_->array_schema()->tile_extents();
  
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
    cell_offset *= tile_extents[i+1]; 
    tile_slab_info_[id].cell_offset_per_dim_[tid][i] = cell_offset;
  } 
}

template<class T>
void ArraySortedWriteState::calculate_cell_slab_info_row_col(
    int id, 
    int64_t tid) {
  // For easy reference
  int anum = (int) attribute_ids_.size();
  const T* tile_extents = (const T*) array_->array_schema()->tile_extents();
  
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
    cell_offset *= tile_extents[i-1]; 
    tile_slab_info_[id].cell_offset_per_dim_[tid][i] = cell_offset;
  }
}

template<class T>
void ArraySortedWriteState::calculate_tile_domain(int id) {
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
void ArraySortedWriteState::calculate_tile_slab_info(int id) {
  // Calculate number of tiles, if they are not already calculated
  if(tile_slab_info_[id].tile_num_ == -1) 
    init_tile_slab_info<T>(id); 

  // Calculate tile domain, if not calculated yet
  if(tile_domain_ == NULL)
    calculate_tile_domain<T>(id);

  // Reset tile coordinates
  reset_tile_coords<T>();

  // Calculate tile slab info
  ASWS_Data asws_data = { id, 0, this };
  (*calculate_tile_slab_info_)(&asws_data);
}

template<class T>
void *ArraySortedWriteState::calculate_tile_slab_info_col(void* data) {
  ArraySortedWriteState* asws = ((ASWS_Data*) data)->asws_;
  int id = ((ASWS_Data*) data)->id_;
  asws->calculate_tile_slab_info_col<T>(id);
  return NULL;
}

template<class T>
void ArraySortedWriteState::calculate_tile_slab_info_col(int id) {
  // For easy reference
  const T* tile_domain = (const T*) tile_domain_;
  T* tile_coords = (T*) tile_coords_;
  const T* tile_extents = (const T*) array_->array_schema()->tile_extents();
  T** range_overlap = (T**) tile_slab_info_[id].range_overlap_;
  const T* tile_slab = (const T*) tile_slab_norm_[id];
  int64_t tile_offset, tile_cell_num;
  int64_t total_cell_num = 0;
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
      tile_cell_num *= tile_extents[i];
     }

    // Calculate tile offsets per dimension
    tile_offset = 1;
    tile_slab_info_[id].tile_offset_per_dim_[0] = tile_offset;
    for(int i=1; i<dim_num_; ++i) {
      tile_offset *= (tile_domain[2*(i-1)+1] - tile_domain[2*(i-1)] + 1);
      tile_slab_info_[id].tile_offset_per_dim_[i] = tile_offset;
    }

    // Calculate cell slab info
    ASWS_Data asws_data = { id, tid, this };
    (*calculate_cell_slab_info_)(&asws_data);

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
void *ArraySortedWriteState::calculate_tile_slab_info_row(void* data) {
  ArraySortedWriteState* asws = ((ASWS_Data*) data)->asws_;
  int id = ((ASWS_Data*) data)->id_;
  asws->calculate_tile_slab_info_row<T>(id);
  return NULL;
}

template<class T>
void ArraySortedWriteState::calculate_tile_slab_info_row(int id) {
  // For easy reference
  const T* tile_domain = (const T*) tile_domain_;
  T* tile_coords = (T*) tile_coords_;
  const T* tile_extents = (const T*) array_->array_schema()->tile_extents();
  T** range_overlap = (T**) tile_slab_info_[id].range_overlap_;
  const T* tile_slab = (const T*) tile_slab_norm_[id];
  int64_t tile_offset, tile_cell_num;
  int64_t total_cell_num = 0;
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
      tile_cell_num *= tile_extents[i];
     }

    // Calculate tile offsets per dimension
    tile_offset = 1;
    tile_slab_info_[id].tile_offset_per_dim_[dim_num_-1] = tile_offset;
    for(int i=dim_num_-2; i>=0; --i) {
      tile_offset *= (tile_domain[2*(i+1)+1] - tile_domain[2*(i+1)] + 1); 
      tile_slab_info_[id].tile_offset_per_dim_[i] = tile_offset;
    }

    // Calculate cell slab info
    ASWS_Data asws_data = { id, tid, this };
    (*calculate_cell_slab_info_)(&asws_data);

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

void *ArraySortedWriteState::aio_handler(void* context) {
  // For easy reference
  ArraySortedWriteState* asws = (ArraySortedWriteState*) context;

  // This will enter an indefinite loop that will handle all incoming copy 
  // requests
  int coords_type = asws->array_->array_schema()->coords_type();
  if(coords_type == TILEDB_INT32)
    asws->handle_aio_requests<int>();
  else if(coords_type == TILEDB_INT64)
    asws->handle_aio_requests<int64_t>();
  else
    assert(0);

  // Return
  return NULL;
}

void ArraySortedWriteState::copy_tile_slab() {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();

  // Copy tile slab for each attribute separately
  for(int i=0, b=0; i<(int)attribute_ids_.size(); ++i) {
    int type = array_schema->type(attribute_ids_[i]);
    if(!array_schema->var_size(attribute_ids_[i])) {
      if(type == TILEDB_INT32)
        copy_tile_slab<int>(i, b); 
      else if(type == TILEDB_INT64)
        copy_tile_slab<int64_t>(i, b); 
      else if(type == TILEDB_FLOAT32)
        copy_tile_slab<float>(i, b); 
      else if(type == TILEDB_FLOAT64)
        copy_tile_slab<double>(i, b); 
      else if(type == TILEDB_CHAR)
        copy_tile_slab<char>(i, b); 
      ++b;
    } else {
      if(type == TILEDB_INT32)
        copy_tile_slab_var<int>(i, b); 
      else if(type == TILEDB_INT64)
        copy_tile_slab_var<int64_t>(i, b); 
      else if(type == TILEDB_FLOAT32)
        copy_tile_slab_var<float>(i, b); 
      else if(type == TILEDB_FLOAT64)
        copy_tile_slab_var<double>(i, b); 
      else if(type == TILEDB_CHAR)
        copy_tile_slab_var<char>(i, b); 
      b += 2;
    }
  }
}

template<class T>
void ArraySortedWriteState::copy_tile_slab(int aid, int bid) {
  // For easy reference
  int64_t& tid = tile_slab_state_.current_tile_[aid]; 
  size_t& buffer_offset = buffer_offsets_[bid];
  char* buffer = (char*) buffers_[bid];
  char* local_buffer = (char*) copy_state_.buffers_[copy_id_][bid];
  size_t& local_buffer_offset = copy_state_.buffer_offsets_[copy_id_][bid];
  size_t local_buffer_size = copy_state_.buffer_sizes_[copy_id_][bid];
  ASWS_Data asws_data = { aid, bid, this };

  // Fill with empty
  fill_with_empty<T>(bid);

  // Important for initializing the current tile and offsets! 
  update_current_tile_and_offset(aid);

  // Iterate over the tile slab cells
  for(;;) {
    // For easy reference
    size_t cell_slab_size = tile_slab_info_[copy_id_].cell_slab_size_[aid][tid];
    size_t& local_buffer_offset_cur = tile_slab_state_.current_offsets_[aid]; 

    // Copy cell slab from user to local buffer
    memcpy(
        local_buffer + local_buffer_offset_cur, 
        buffer + buffer_offset,
        cell_slab_size);

    // Update user buffer offset
    buffer_offset += cell_slab_size;
   
    // Prepare for new slab
    (*advance_cell_slab_)(&asws_data);

    // Terminating condition 
    if(tile_slab_state_.copy_tile_slab_done_[aid])
      break;
  }

  // Set local buffer offset
  local_buffer_offset = local_buffer_size;
}

template<class T>
void ArraySortedWriteState::copy_tile_slab_var(int aid, int bid) {
  // For easy reference
  int64_t& tid = tile_slab_state_.current_tile_[aid]; 
  size_t& buffer_offset = buffer_offsets_[bid];
  size_t buffer_size = buffer_sizes_[bid];
  size_t buffer_var_size = buffer_sizes_[bid+1];
  char* buffer_var = (char*) buffers_[bid+1];
  size_t* buffer_s = (size_t*) buffers_[bid];
  char* local_buffer = (char*) copy_state_.buffers_[copy_id_][bid];
  size_t* local_buffer_s = (size_t*) copy_state_.buffers_[copy_id_][bid];
  size_t local_buffer_size = copy_state_.buffer_sizes_[copy_id_][bid];
  size_t& local_buffer_offset = copy_state_.buffer_offsets_[copy_id_][bid];
  char* local_buffer_var = (char*) copy_state_.buffers_[copy_id_][bid+1];
  size_t& local_buffer_offset_var = 
      copy_state_.buffer_offsets_[copy_id_][bid+1];
  size_t& local_buffer_var_size = copy_state_.buffer_sizes_[copy_id_][bid+1];
  int64_t cell_num_in_buffer = buffer_size / sizeof(size_t);
  int64_t cell_num_in_tile_slab = local_buffer_size / sizeof(size_t);
  ASWS_Data asws_data = { aid, 0, this };

  // Important for initializing the current tile and offsets! 
  update_current_tile_and_offset(aid);

  // Fill the local buffer offsets with zeros
  bzero(local_buffer, local_buffer_size);

  // Handle offsets first
  for(;;) {
    // For easy reference
    size_t cell_slab_size = 
        tile_slab_info_[copy_id_].cell_slab_size_[aid][tid];
    int64_t cell_num_in_slab = cell_slab_size / sizeof(size_t);
    size_t local_buffer_offset_cur = tile_slab_state_.current_offsets_[aid]; 

    // Calculate variable cell slab size
    int64_t cell_start = buffer_offset / sizeof(size_t);
    int64_t cell_end = cell_start + cell_num_in_slab;

    // Keep track of where each variable-sized cell is.
    // Note that cell ids start with 1 here!
    for(int64_t i=cell_start+1; i<=cell_end; ++i) {
      memcpy(
          local_buffer + local_buffer_offset_cur, 
          &i, 
          sizeof(size_t));
      local_buffer_offset_cur += sizeof(size_t);
      buffer_offset += sizeof(size_t);
    }

    // Prepare for new slab
    (*advance_cell_slab_)(&asws_data);

    // Terminating condition 
    if(tile_slab_state_.copy_tile_slab_done_[aid])
      break;
  }

  // Rectify offsets and copy variable-sized cells
  int64_t cell;
  size_t cell_size_var;
  for(int i=0; i<cell_num_in_tile_slab; ++i) {
    // Handle empties 
    if(local_buffer_s[i] == 0) {
      local_buffer_s[i] = local_buffer_offset_var;
      fill_with_empty_var<T>(bid);
      local_buffer_offset_var += sizeof(T);
      continue;
    }

    // Find size of variable-sized cell
    cell = local_buffer_s[i]-1; // So that cell ids start from 0
    cell_size_var = 
         (cell == cell_num_in_buffer-1) ?
         buffer_var_size - buffer_s[cell] :
         buffer_s[cell+1] - buffer_s[cell];

    // Rectify offset
    local_buffer_s[i] = local_buffer_offset_var;

    // Expand the variable-sized buffer if necessary
    while(local_buffer_offset_var + cell_size_var > local_buffer_var_size) {
      expand_buffer(
          copy_state_.buffers_[copy_id_][bid+1], 
          copy_state_.buffer_sizes_[copy_id_][bid+1]);
      local_buffer_var = (char*) copy_state_.buffers_[copy_id_][bid+1];
    }

    // Copy variable-sized cell
    memcpy(
        local_buffer_var + local_buffer_offset_var, 
        buffer_var + buffer_s[cell], 
        cell_size_var);
    local_buffer_offset_var += cell_size_var;
  }

  // Set local buffer offset
  local_buffer_offset = local_buffer_size;
}

int ArraySortedWriteState::create_copy_state_buffers() {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();

  // Get cell number in a (full) tile slab
  int64_t tile_slab_cell_num; 
  if(array_->mode() == TILEDB_ARRAY_WRITE_SORTED_ROW)
    tile_slab_cell_num = 
        array_schema->tile_slab_row_cell_num(expanded_subarray_);
  else // TILEDB_ARRAY_WRITE_SORTED_COL
    tile_slab_cell_num = 
        array_schema->tile_slab_col_cell_num(expanded_subarray_);

  // Calculate buffer sizes
  int attribute_id_num = (int) attribute_ids_.size();
  for(int j=0; j<2; ++j) {
    copy_state_.buffer_sizes_[j] = new size_t[buffer_num_]; 
    for(int i=0, b=0; i<attribute_id_num; ++i) {
      // Fix-sized attribute
      if(!array_schema->var_size(attribute_ids_[i])) { 
        copy_state_.buffer_sizes_[j][b++] = 
            tile_slab_cell_num * array_schema->cell_size(attribute_ids_[i]);
      } else { // Variable-sized attribute
        copy_state_.buffer_sizes_[j][b++] = tile_slab_cell_num * sizeof(size_t);
        copy_state_.buffer_sizes_[j][b++] = 2*tile_slab_cell_num*sizeof(size_t);
      }
    }
  }

  // Allocate buffers
  for(int j=0; j<2; ++j) {
    copy_state_.buffers_[j] = (void**) malloc(buffer_num_ * sizeof(void*));
    if(copy_state_.buffers_[j] == NULL) {
      std::string errmsg = "Cannot create local buffers";
      PRINT_ERROR(errmsg);
      tiledb_asws_errmsg = TILEDB_ASWS_ERRMSG + errmsg;
      return TILEDB_ASWS_ERR;
    }

    for(int b=0; b < buffer_num_; ++b) { 
      copy_state_.buffers_[j][b] = malloc(copy_state_.buffer_sizes_[j][b]);
      if(copy_state_.buffers_[j][b] == NULL) {
        std::string errmsg = "Cannot allocate local buffer";
        PRINT_ERROR(errmsg);
        tiledb_asws_errmsg = TILEDB_ASWS_ERRMSG + errmsg;
        return TILEDB_ASWS_ERR;
      }
    }
  } 

  // Success
  return TILEDB_ASWS_OK; 
}

void ArraySortedWriteState::create_user_buffers(
    const void** buffers, 
    const size_t* buffer_sizes) {
  buffers_ = buffers;
  buffer_sizes_ = buffer_sizes;
  buffer_offsets_ = new size_t[buffer_num_];
  for(int i=0; i<buffer_num_; ++i)
    buffer_offsets_[i] = 0;
}

template<>
void ArraySortedWriteState::fill_with_empty<int>(int bid) {
  // For easy reference
  int* local_buffer = (int*) copy_state_.buffers_[copy_id_][bid];
  size_t local_buffer_size = copy_state_.buffer_sizes_[copy_id_][bid];
  int empty = TILEDB_EMPTY_INT32;

  // Fill with empty values
  size_t offset = 0;
  for(int64_t i=0; offset < local_buffer_size; offset += sizeof(int), ++i) 
    local_buffer[i] = empty;
}

template<>
void ArraySortedWriteState::fill_with_empty<int64_t>(int bid) {
  // For easy reference
  int64_t* local_buffer = (int64_t*) copy_state_.buffers_[copy_id_][bid];
  size_t local_buffer_size = copy_state_.buffer_sizes_[copy_id_][bid];
  int64_t empty = TILEDB_EMPTY_INT64;

  // Fill with empty values
  size_t offset = 0;
  for(int64_t i=0; offset < local_buffer_size; offset += sizeof(int64_t), ++i) 
    local_buffer[i] = empty;
}

template<>
void ArraySortedWriteState::fill_with_empty<float>(int bid) {
  // For easy reference
  float* local_buffer = (float*) copy_state_.buffers_[copy_id_][bid];
  size_t local_buffer_size = copy_state_.buffer_sizes_[copy_id_][bid];
  float empty = TILEDB_EMPTY_FLOAT32;

  // Fill with empty values
  size_t offset = 0;
  for(int64_t i=0; offset < local_buffer_size; offset += sizeof(float), ++i) 
    local_buffer[i] = empty;
}

template<>
void ArraySortedWriteState::fill_with_empty<double>(int bid) {
  // For easy reference
  double* local_buffer = (double*) copy_state_.buffers_[copy_id_][bid];
  size_t local_buffer_size = copy_state_.buffer_sizes_[copy_id_][bid];
  double empty = TILEDB_EMPTY_FLOAT64;

  // Fill with empty values
  size_t offset = 0;
  for(int64_t i=0; offset < local_buffer_size; offset += sizeof(double), ++i) 
    local_buffer[i] = empty;
}

template<>
void ArraySortedWriteState::fill_with_empty<char>(int bid) {
  // For easy reference
  char* local_buffer = (char*) copy_state_.buffers_[copy_id_][bid];
  size_t local_buffer_size = copy_state_.buffer_sizes_[copy_id_][bid];
  char empty = TILEDB_EMPTY_CHAR;

  // Fill with empty values
  size_t offset = 0;
  for(int64_t i=0; offset < local_buffer_size; offset += sizeof(char), ++i) 
    local_buffer[i] = empty;
}

template<>
void ArraySortedWriteState::fill_with_empty_var<int>(int bid) {
  // For easy reference
  char* local_buffer_var = (char*) copy_state_.buffers_[copy_id_][bid+1];
  size_t local_buffer_offset_var = copy_state_.buffer_offsets_[copy_id_][bid+1];
  int empty = TILEDB_EMPTY_INT32;

  // Fill an empty value
  memcpy(local_buffer_var + local_buffer_offset_var, &empty, sizeof(int));
}

template<>
void ArraySortedWriteState::fill_with_empty_var<int64_t>(int bid) {
  // For easy reference
  char* local_buffer_var = (char*) copy_state_.buffers_[copy_id_][bid+1];
  size_t local_buffer_offset_var = copy_state_.buffer_offsets_[copy_id_][bid+1];
  int64_t empty = TILEDB_EMPTY_INT64;

  // Fill an empty value
  memcpy(local_buffer_var + local_buffer_offset_var, &empty, sizeof(int64_t));
}

template<>
void ArraySortedWriteState::fill_with_empty_var<float>(int bid) {
  // For easy reference
  char* local_buffer_var = (char*) copy_state_.buffers_[copy_id_][bid+1];
  size_t local_buffer_offset_var = copy_state_.buffer_offsets_[copy_id_][bid+1];
  float empty = TILEDB_EMPTY_FLOAT32;

  // Fill an empty value
  memcpy(local_buffer_var + local_buffer_offset_var, &empty, sizeof(float));
}

template<>
void ArraySortedWriteState::fill_with_empty_var<double>(int bid) {
  // For easy reference
  char* local_buffer_var = (char*) copy_state_.buffers_[copy_id_][bid+1];
  size_t local_buffer_offset_var = copy_state_.buffer_offsets_[copy_id_][bid+1];
  double empty = TILEDB_EMPTY_FLOAT64;

  // Fill an empty value
  memcpy(local_buffer_var + local_buffer_offset_var, &empty, sizeof(double));
}

template<>
void ArraySortedWriteState::fill_with_empty_var<char>(int bid) {
  // For easy reference
  char* local_buffer_var = (char*) copy_state_.buffers_[copy_id_][bid+1];
  size_t local_buffer_offset_var = copy_state_.buffer_offsets_[copy_id_][bid+1];
  char empty = TILEDB_EMPTY_CHAR;

  // Fill an empty value
  memcpy(local_buffer_var + local_buffer_offset_var, &empty, sizeof(char));
}

void ArraySortedWriteState::free_copy_state() {
  for(int i=0; i<2; ++i) {
    if(copy_state_.buffer_sizes_[i] != NULL)
      delete [] copy_state_.buffer_sizes_[i];
    if(copy_state_.buffers_[i] != NULL) {
      for(int b=0; b<buffer_num_; ++b)  
        free(copy_state_.buffers_[i][b]);
      free(copy_state_.buffers_[i]);
    }
  }
}

void ArraySortedWriteState::free_tile_slab_info() {
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

void ArraySortedWriteState::free_tile_slab_state() {
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
}

template<class T>
int64_t ArraySortedWriteState::get_cell_id(int aid) {
  // For easy reference
  const T* current_coords = (const T*) tile_slab_state_.current_coords_[aid];
  const T* tile_extents = (const T*) array_->array_schema()->tile_extents();
  int64_t tid = tile_slab_state_.current_tile_[aid];
  int64_t* cell_offset_per_dim = 
      tile_slab_info_[copy_id_].cell_offset_per_dim_[tid];

  // Calculate cell id
  int64_t cid = 0;
  for(int i=0; i<dim_num_; ++i)
    cid += (current_coords[i] - 
           (current_coords[i] / tile_extents[i]) * tile_extents[i])  * 
           cell_offset_per_dim[i];

  // Return tile id
  return cid;

}

template<class T>
int64_t ArraySortedWriteState::get_tile_id(int aid) {
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
void ArraySortedWriteState::handle_aio_requests() {
  // Handle AIO requests indefinitely
  for(;;) {
    // Wait for AIO
    wait_copy(aio_id_); 

    // Kill thread
    if(aio_thread_canceled_) {
      aio_thread_running_ = false;
      return;
    }

    // Block copy
    block_copy(aio_id_);

    // Send AIO request
    send_aio_request(aio_id_);

    // Advance AIO id
    aio_id_ = (aio_id_ + 1) % 2;
  }
}

void ArraySortedWriteState::init_aio_requests() {
  // For easy reference
  int mode = array_->mode();
  int tile_order = array_->array_schema()->tile_order();
  const void* subarray = array_->subarray();
  bool separate_fragments = 
    (mode == TILEDB_ARRAY_WRITE_SORTED_COL && tile_order == TILEDB_ROW_MAJOR) ||
    (mode == TILEDB_ARRAY_WRITE_SORTED_ROW && tile_order == TILEDB_COL_MAJOR);

  // Initialize AIO requests
  for(int i=0; i<2; ++i) {
    aio_data_[i] = { i, 0, this };
    aio_request_[i] = {};
    aio_request_[i].id_ = (separate_fragments) ? aio_cnt_++ : 0;
    aio_request_[i].buffer_sizes_ = copy_state_.buffer_offsets_[i];
    aio_request_[i].buffers_ = copy_state_.buffers_[i];
    aio_request_[i].mode_ = TILEDB_ARRAY_WRITE;
    aio_request_[i].subarray_ = (separate_fragments) ? tile_slab_[i] : subarray;
    aio_request_[i].completion_handle_ = aio_done;
    aio_request_[i].completion_data_ = &(aio_data_[i]); 
    aio_request_[i].overflow_ = NULL;
    aio_request_[i].status_ = &(aio_status_[i]);
  }
}

void ArraySortedWriteState::init_copy_state() {
  for(int j=0; j<2; ++j) {
    copy_state_.buffer_offsets_[j] = new size_t[buffer_num_];
    copy_state_.buffer_sizes_[j] = new size_t[buffer_num_];
    copy_state_.buffers_[j] = new void*[buffer_num_];
    for(int i=0; i<buffer_num_; ++i) {
      copy_state_.buffer_offsets_[j][i] = 0;
      copy_state_.buffer_sizes_[j][i] = 0;
      copy_state_.buffers_[j][i] = NULL;
    }
  }
}

void ArraySortedWriteState::init_tile_slab_info() {
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
void ArraySortedWriteState::init_tile_slab_info(int id) {
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
    for(int j=0; j<tile_num; ++j)
      tile_slab_info_[id].start_offsets_[i][j] = 0;
  }

  tile_slab_info_[id].tile_num_ = tile_num;
}

void ArraySortedWriteState::init_tile_slab_state() {
  // For easy reference
  int anum = (int) attribute_ids_.size();

  // Allocations and initializations
  tile_slab_state_.copy_tile_slab_done_ = new bool[anum];
  for(int i=0; i<anum; ++i) 
    tile_slab_state_.copy_tile_slab_done_[i] = true; // Important!

  tile_slab_state_.current_offsets_ = new size_t[anum];
  tile_slab_state_.current_coords_ = new void*[anum];
  tile_slab_state_.current_tile_ = new int64_t[anum];

  for(int i=0; i<anum; ++i) {
    tile_slab_state_.current_coords_[i] = malloc(coords_size_);
    tile_slab_state_.current_offsets_[i] = 0;
    tile_slab_state_.current_tile_[i] = 0;
  }
}

int ArraySortedWriteState::lock_aio_mtx() {
  if(pthread_mutex_lock(&aio_mtx_)) {
    std::string errmsg = "Cannot lock AIO mutex";
    PRINT_ERROR(errmsg);
    tiledb_asws_errmsg = TILEDB_ASWS_ERRMSG + errmsg;
    return TILEDB_ASWS_ERR;
  }

  // Success
  return TILEDB_ASWS_OK;
}

int ArraySortedWriteState::lock_copy_mtx() {
  if(pthread_mutex_lock(&copy_mtx_)) {
    std::string errmsg = "Cannot lock copy mutex";
    PRINT_ERROR(errmsg);
    tiledb_asws_errmsg = TILEDB_ASWS_ERRMSG + errmsg;
    return TILEDB_ASWS_ERR;
  }

  // Success
  return TILEDB_ASWS_OK;
}


template<class T>
bool ArraySortedWriteState::next_tile_slab_col() {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  const T* subarray = static_cast<const T*>(subarray_);
  const T* domain = static_cast<const T*>(array_schema->domain());
  const T* tile_extents = static_cast<const T*>(array_schema->tile_extents());
  T* tile_slab[2];
  T* tile_slab_norm = static_cast<T*>(tile_slab_norm_[copy_id_]);
  for(int i=0; i<2; ++i)
    tile_slab[i] = static_cast<T*>(tile_slab_[i]);
  int prev_id = (copy_id_+1)%2;
  T tile_start;

  // Check again if done, this time based on the tile slab and subarray
  if(tile_slab_init_[prev_id] && 
     tile_slab[prev_id][2*(dim_num_-1) + 1] == subarray[2*(dim_num_-1) + 1]) {
    return false;
  }

  // If this is the first time this function is called, initialize
  if(!tile_slab_init_[prev_id]) {
    // Crop the subarray extent along the first axis to fit in the first tile
    tile_slab[copy_id_][2*(dim_num_-1)] = subarray[2*(dim_num_-1)]; 
    T upper = subarray[2*(dim_num_-1)] + tile_extents[dim_num_-1];
    T cropped_upper = 
        (upper - domain[2*(dim_num_-1)]) / tile_extents[dim_num_-1] * 
        tile_extents[dim_num_-1] + domain[2*(dim_num_-1)];
    tile_slab[copy_id_][2*(dim_num_-1)+1] = 
        std::min(cropped_upper - 1, subarray[2*(dim_num_-1)+1]); 

    // Leave the rest of the subarray extents intact
    for(int i=0; i<dim_num_-1; ++i) {
      tile_slab[copy_id_][2*i] = subarray[2*i]; 
      tile_slab[copy_id_][2*i+1] = subarray[2*i+1]; 
    } 
  } else { // Calculate a new slab based on the previous
    // Copy previous tile slab
    memcpy(
        tile_slab[copy_id_], 
        tile_slab[prev_id], 
        2*coords_size_);

    // Advance tile slab
    tile_slab[copy_id_][2*(dim_num_-1)] = 
        tile_slab[copy_id_][2*(dim_num_-1)+1] + 1;
    tile_slab[copy_id_][2*(dim_num_-1)+1] = 
        std::min(
            tile_slab[copy_id_][2*(dim_num_-1)] + tile_extents[dim_num_-1] - 1,
            subarray[2*(dim_num_-1)+1]);
  }

  // Calculate normalized tile slab
  for(int i=0; i<dim_num_; ++i) {
    tile_start = 
        ((tile_slab[copy_id_][2*i] - domain[2*i]) / tile_extents[i]) * 
        tile_extents[i] + domain[2*i]; 
    tile_slab_norm[2*i] = tile_slab[copy_id_][2*i] - tile_start; 
    tile_slab_norm[2*i+1] = tile_slab[copy_id_][2*i+1] - tile_start; 
  }

  // Calculate tile slab info and reset tile slab state
  calculate_tile_slab_info<T>(copy_id_);

  // Mark this tile slab as initialized
  tile_slab_init_[copy_id_] = true;

  // Success
  return true;
}

template<class T>
bool ArraySortedWriteState::next_tile_slab_row() {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  const T* subarray = static_cast<const T*>(subarray_);
  const T* domain = static_cast<const T*>(array_schema->domain());
  const T* tile_extents = static_cast<const T*>(array_schema->tile_extents());
  T* tile_slab[2];
  T* tile_slab_norm = static_cast<T*>(tile_slab_norm_[copy_id_]);
  for(int i=0; i<2; ++i)
    tile_slab[i] = static_cast<T*>(tile_slab_[i]);
  int prev_id = (copy_id_+1)%2;
  T tile_start;

  // Check again if done, this time based on the tile slab and subarray
  if(tile_slab_init_[prev_id] && 
     tile_slab[prev_id][1] == subarray[1]) {
    return false;
  }

  // If this is the first time this function is called, initialize
  if(!tile_slab_init_[prev_id]) {
    // Crop the subarray extent along the first axis to fit in the first tile
    tile_slab[copy_id_][0] = subarray[0]; 
    T upper = subarray[0] + tile_extents[0];
    T cropped_upper = 
        (upper - domain[0]) / tile_extents[0] * tile_extents[0] + domain[0];
    tile_slab[copy_id_][1] = std::min(cropped_upper - 1, subarray[1]); 

    // Leave the rest of the subarray extents intact
    for(int i=1; i<dim_num_; ++i) {
      tile_slab[copy_id_][2*i] = subarray[2*i]; 
      tile_slab[copy_id_][2*i+1] = subarray[2*i+1]; 
    } 
  } else { // Calculate a new slab based on the previous
    // Copy previous tile slab
    memcpy(
        tile_slab[copy_id_], 
        tile_slab[prev_id], 
        2*coords_size_);

    // Advance tile slab
    tile_slab[copy_id_][0] = tile_slab[copy_id_][1] + 1; 
    tile_slab[copy_id_][1] = std::min(
                                tile_slab[copy_id_][0] + tile_extents[0] - 1,
                                subarray[1]);
  }

  // Calculate normalized tile slab
  for(int i=0; i<dim_num_; ++i) {
    tile_start = 
        ((tile_slab[copy_id_][2*i] - domain[2*i]) / tile_extents[i]) * 
        tile_extents[i] + domain[2*i]; 
    tile_slab_norm[2*i] = tile_slab[copy_id_][2*i] - tile_start; 
    tile_slab_norm[2*i+1] = tile_slab[copy_id_][2*i+1] - tile_start; 
  }

  // Calculate tile slab info and reset tile slab state
  calculate_tile_slab_info<T>(copy_id_);

  // Mark this tile slab as initialized
  tile_slab_init_[copy_id_] = true;

  // Success
  return true;
}

template<class T>
int ArraySortedWriteState::write() {
  // For easy reference
  int mode = array_->mode(); 

  if(mode == TILEDB_ARRAY_WRITE_SORTED_COL) { 
    return write_sorted_col<T>();
  } else if(mode == TILEDB_ARRAY_WRITE_SORTED_ROW) {
    return write_sorted_row<T>();
  } else {
    assert(0); // The code should never reach here
    return TILEDB_ASWS_ERR;
  }
}

template<class T>
int ArraySortedWriteState::write_sorted_col() {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  const T* subarray = static_cast<const T*>(subarray_);

  // Check if this can be satisfied with a default write
  if(array_schema->cell_order() == TILEDB_COL_MAJOR &&
     !memcmp(subarray_, expanded_subarray_, 2*coords_size_) &&
     array_schema->is_contained_in_tile_slab_row<T>(subarray))
    return array_->write_default(buffers_, buffer_sizes_);

  // Iterate over each tile slab
  while(next_tile_slab_col<T>()) {
    // Wait for AIO
    wait_aio(copy_id_);

    // Block AIO
    block_aio(copy_id_);

    // Reset the tile slab state and copy state
    reset_tile_slab_state<T>();
    reset_copy_state();

    // Copy tile slab
    copy_tile_slab();

    // Release copy
    release_copy(copy_id_); 

    // Advance copy id
    copy_id_ = (copy_id_ + 1) % 2;
  }

  // Wait for last AIO to finish
  wait_aio((copy_id_ + 1) % 2);

  // The following will make the AIO thread terminate
  aio_thread_canceled_ = true;
  release_copy(copy_id_);

  // Success
  return TILEDB_ASWS_OK;
}

template<class T>
int ArraySortedWriteState::write_sorted_row() {
  // For easy reference
  const ArraySchema* array_schema = array_->array_schema();
  const T* subarray = static_cast<const T*>(subarray_);

  // Check if this can be satisfied with a default write
  if(array_schema->cell_order() == TILEDB_ROW_MAJOR &&
     !memcmp(subarray_, expanded_subarray_, 2*coords_size_) &&
     array_schema->is_contained_in_tile_slab_col<T>(subarray))
    return array_->write_default(buffers_, buffer_sizes_);

  // Iterate over each tile slab
  while(next_tile_slab_row<T>()) {
    // Wait for AIO
    wait_aio(copy_id_);

    // Block AIO
    block_aio(copy_id_);

    // Reset the tile slab state
    reset_tile_slab_state<T>();
    reset_copy_state();

    // Copy tile slab
    copy_tile_slab();

    // Release copy
    release_copy(copy_id_); 

    // Advance copy id
    copy_id_ = (copy_id_ + 1) % 2;
  }

  // Wait for last AIO to finish
  wait_aio((copy_id_ + 1) % 2);

  // The following will make the AIO thread terminate
  aio_thread_canceled_ = true;
  release_copy(copy_id_);

  // Success
  return TILEDB_ASWS_OK;
}

int ArraySortedWriteState::release_aio(int id) {
  // Lock the AIO mutex
  if(lock_aio_mtx() != TILEDB_ASWS_OK)
    return TILEDB_ASWS_ERR;    

  // Set AIO flag
  wait_aio_[id] = false; 

  // Signal condition
  if(pthread_cond_signal(&(aio_cond_[id]))) { 
    std::string errmsg = "Cannot signal AIO condition";
    PRINT_ERROR(errmsg);
    tiledb_asws_errmsg = TILEDB_ASWS_ERRMSG + errmsg;
    return TILEDB_ASWS_ERR;
  }

  // Unlock the AIO mutex
  if(unlock_aio_mtx() != TILEDB_ASWS_OK)
    return TILEDB_ASWS_ERR;    

  // Success
  return TILEDB_ASWS_OK;
}

int ArraySortedWriteState::release_copy(int id) {
  // Lock the copy mutex
  if(lock_copy_mtx() != TILEDB_ASWS_OK)
    return TILEDB_ASWS_ERR;    

  // Set copy flag
  wait_copy_[id] = false;

  // Signal condition
  if(pthread_cond_signal(&copy_cond_[id])) { 
    std::string errmsg = "Cannot signal copy condition";
    PRINT_ERROR(errmsg);
    tiledb_asws_errmsg = TILEDB_ASWS_ERRMSG + errmsg;
    return TILEDB_ASWS_ERR;    
  }

  // Unlock the copy mutex
  if(unlock_copy_mtx() != TILEDB_ASWS_OK)
    return TILEDB_ASWS_ERR;    

  // Success
  return TILEDB_ASWS_OK;
}

void ArraySortedWriteState::reset_copy_state() {
  for(int i=0; i<buffer_num_; ++i)
    copy_state_.buffer_offsets_[copy_id_][i] = 0;
}

template<class T>
void ArraySortedWriteState::reset_tile_coords() {
  T* tile_coords = (T*) tile_coords_;
  for(int i=0; i<dim_num_; ++i)
    tile_coords[i] = 0;
}

template<class T>
void ArraySortedWriteState::reset_tile_slab_state() {
  // For easy reference
  int anum = (int) attribute_ids_.size();
  T** current_coords = (T**) tile_slab_state_.current_coords_;
  const T* tile_slab = (const T*) tile_slab_norm_[copy_id_];

  // Reset values
  for(int i=0; i<anum; ++i) {
    tile_slab_state_.copy_tile_slab_done_[i] = false;
    tile_slab_state_.current_tile_[i] = 0;
    for(int j=0; j<dim_num_; ++j)
      current_coords[i][j] = tile_slab[2*j];
  }
}

int ArraySortedWriteState::send_aio_request(int aio_id) { 
  // For easy reference
  Array* array_clone = array_->array_clone();

  // Sanity check
  assert(array_clone != NULL);

  // Send the AIO request to the clone array
  if(array_clone->aio_write(&(aio_request_[aio_id])) != TILEDB_AR_OK) {
    tiledb_asws_errmsg = tiledb_ar_errmsg;
    return TILEDB_ASWS_ERR;
  }

  // Success
  return TILEDB_ASWS_OK;
}

int ArraySortedWriteState::unlock_aio_mtx() {
  if(pthread_mutex_unlock(&aio_mtx_)) {
    std::string errmsg = "Cannot unlock AIO mutex";
    PRINT_ERROR(errmsg);
    tiledb_asws_errmsg = TILEDB_ASWS_ERRMSG + errmsg;
    return TILEDB_ASWS_ERR;
  }

  // Success
  return TILEDB_ASWS_OK;
}

int ArraySortedWriteState::unlock_copy_mtx() {
  if(pthread_mutex_unlock(&copy_mtx_)) {
    std::string errmsg = "Cannot unlock copy mutex";
    PRINT_ERROR(errmsg);
    tiledb_asws_errmsg = TILEDB_ASWS_ERRMSG + errmsg;
    return TILEDB_ASWS_ERR;
  }

  // Success
  return TILEDB_ASWS_OK;
}

void ArraySortedWriteState::update_current_tile_and_offset(int aid) {
  // For easy reference
  int coords_type = array_->array_schema()->coords_type();
  
  // Invoke the proper templated function
  if(coords_type == TILEDB_INT32)
    update_current_tile_and_offset<int>(aid);
  else if(coords_type == TILEDB_INT64)
    update_current_tile_and_offset<int64_t>(aid);
  else if(coords_type == TILEDB_FLOAT32)
    update_current_tile_and_offset<float>(aid);
  else if(coords_type == TILEDB_FLOAT64)
    update_current_tile_and_offset<double>(aid);
  else
    assert(0);
}

template<class T>
void ArraySortedWriteState::update_current_tile_and_offset(int aid) {
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


int ArraySortedWriteState::wait_aio(int id) {
  // Lock AIO mutex
  if(lock_aio_mtx() != TILEDB_ASWS_OK)
    return TILEDB_ASWS_ERR; 

  // Wait to be signaled
  while(wait_aio_[id]) {
    if(pthread_cond_wait(&(aio_cond_[id]), &aio_mtx_)) {
      std::string errmsg = "Cannot wait on IO mutex condition";
      PRINT_ERROR(errmsg);
      tiledb_asws_errmsg = TILEDB_ASWS_ERRMSG + errmsg;
      return TILEDB_ASWS_ERR;
    }
  }

  // Unlock AIO mutex
  if(unlock_aio_mtx() != TILEDB_ASWS_OK)
    return TILEDB_ASWS_ERR;

  // Success
  return TILEDB_ASWS_OK;
}

int ArraySortedWriteState::wait_copy(int id) {
  // Lock copy mutex
  if(lock_copy_mtx() != TILEDB_ASWS_OK)
    return TILEDB_ASWS_ERR; 

  // Wait to be signaled
  while(wait_copy_[id]) {
    if(pthread_cond_wait(&(copy_cond_[id]), &copy_mtx_)) {
      std::string errmsg = "Cannot wait on copy mutex condition";
      PRINT_ERROR(errmsg);
      tiledb_asws_errmsg = TILEDB_ASWS_ERRMSG + errmsg;
      return TILEDB_ASWS_ERR;
    }
  }

  // Unlock copy mutex
  if(unlock_copy_mtx() != TILEDB_ASWS_OK)
    return TILEDB_ASWS_ERR;

  // Success
  return TILEDB_ASWS_OK;
}

// Explicit template instantiations

template int ArraySortedWriteState::write_sorted_col<int>();
template int ArraySortedWriteState::write_sorted_col<int64_t>();

template int ArraySortedWriteState::write_sorted_row<int>();
template int ArraySortedWriteState::write_sorted_row<int64_t>();

