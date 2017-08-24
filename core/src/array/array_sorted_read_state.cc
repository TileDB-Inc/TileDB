/**
 * @file   array_sorted_read_state.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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

#include <algorithm>
#include <cassert>
#include <cmath>

#include "array_sorted_read_state.h"
#include "comparators.h"
#include "logger.h"
#include "storage_manager.h"
#include "utils.h"

/* ****************************** */
/*             MACROS             */
/* ****************************** */

#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))

namespace tiledb {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

ArraySortedReadState::ArraySortedReadState(Query* query)
    : query_(query) {
  // Calculate the attribute ids
  calculate_attribute_ids();

  // For easy reference
  const ArraySchema* array_schema = query->array()->array_schema();
  int anum = (int)attribute_ids_.size();

  // Initializations
  coords_size_ = array_schema->coords_size();
  copy_id_ = 0;
  dim_num_ = array_schema->dim_num();
  read_tile_slabs_done_ = false;
  resume_copy_ = false;
  resume_copy_2_ = false;
  tile_coords_ = nullptr;
  tile_domain_ = nullptr;
  for (int i = 0; i < 2; ++i) {
    aio_overflow_[i] = new bool[anum];
    aio_query_[i] = nullptr;
    buffer_sizes_[i] = nullptr;
    buffer_sizes_tmp_[i] = nullptr;
    buffer_sizes_tmp_bak_[i] = nullptr;
    buffers_[i] = nullptr;
    tile_slab_[i] = malloc(2 * coords_size_);
    tile_slab_norm_[i] = malloc(2 * coords_size_);
    tile_slab_init_[i] = false;
    wait_aio_[i] = true;
  }
  overflow_ = new bool[anum];
  overflow_still_ = new bool[anum];
  for (int i = 0; i < anum; ++i) {
    overflow_[i] = false;
    overflow_still_[i] = true;
    if (array_schema->var_size(attribute_ids_[i]))
      attribute_sizes_.push_back(sizeof(size_t));
    else
      attribute_sizes_.push_back(array_schema->cell_size(attribute_ids_[i]));
  }

  subarray_ = malloc(2 * coords_size_);
  memcpy(subarray_, query_->subarray(), 2 * coords_size_);

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
  // Clean up
  free(subarray_);
  free(tile_coords_);
  free(tile_domain_);
  delete[] overflow_;

  for (int i = 0; i < 2; ++i) {
    delete[] aio_overflow_[i];
    delete aio_query_[i];

    if (buffer_sizes_[i] != nullptr)
      delete[] buffer_sizes_[i];
    if (buffer_sizes_tmp_[i] != nullptr)
      delete[] buffer_sizes_tmp_[i];
    if (buffer_sizes_tmp_bak_[i] != nullptr)
      delete[] buffer_sizes_tmp_bak_[i];
    if (buffers_[i] != nullptr) {
      for (int b = 0; b < buffer_num_; ++b)
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
}

/* ****************************** */
/*           ACCESSORS            */
/* ****************************** */

bool ArraySortedReadState::copy_tile_slab_done() const {
  for (int i = 0; i < (int)attribute_ids_.size(); ++i) {
    // Special case for sparse arrays with extra coordinates attribute
    if (i == coords_attr_i_ && extra_coords_)
      continue;

    // Check
    if (!tile_slab_state_.copy_tile_slab_done_[i])
      return false;
  }

  return true;
}

bool ArraySortedReadState::done() const {
  if (!read_tile_slabs_done_)
    return false;
  else
    return copy_tile_slab_done();
}

bool ArraySortedReadState::overflow() const {
  for (int i = 0; i < (int)attribute_ids_.size(); ++i) {
    if (overflow_[i])
      return true;
  }

  return false;
}

bool ArraySortedReadState::overflow(int attribute_id) const {
  for (int i = 0; i < (int)attribute_ids_.size(); ++i) {
    if (attribute_ids_[i] == attribute_id)
      return overflow_[i];
  }

  return false;
}

Status ArraySortedReadState::read(void** buffers, size_t* buffer_sizes) {
  // Trivial case
  if (done()) {
    for (int i = 0; i < buffer_num_; ++i)
      buffer_sizes[i] = 0;
    return Status::Ok();
  }

  // Reset copy state and overflow
  reset_copy_state(buffers, buffer_sizes);
  reset_overflow();

  // Call the appropriate templated read
  Datatype type = query_->array()->array_schema()->coords_type();
  if (type == Datatype::INT32) {
    return read<int>();
  } else if (type == Datatype::INT64) {
    return read<int64_t>();
  } else if (type == Datatype::FLOAT32) {
    return read<float>();
  } else if (type == Datatype::FLOAT64) {
    return read<double>();
  } else if (type == Datatype::INT8) {
    return read<int8_t>();
  } else if (type == Datatype::UINT8) {
    return read<uint8_t>();
  } else if (type == Datatype::INT16) {
    return read<int16_t>();
  } else if (type == Datatype::UINT16) {
    return read<uint16_t>();
  } else if (type == Datatype::UINT32) {
    return read<uint32_t>();
  } else if (type == Datatype::UINT64) {
    return read<uint64_t>();
  } else {
    assert(0);
  }

  // Code should never reach here
  return LOG_STATUS(Status::ASRSError("Invalid datatype when reading"));
}

/* ****************************** */
/*            MUTATORS            */
/* ****************************** */

Status ArraySortedReadState::init() {
  // Create buffers
  RETURN_NOT_OK(create_buffers());

  // Create AIO requests
  init_aio_requests();

  // Initialize functors
  const ArraySchema* array_schema = query_->array()->array_schema();
  QueryMode mode = query_->mode();
  Layout cell_order = array_schema->cell_order();
  Layout tile_order = array_schema->tile_order();
  Datatype coords_type = array_schema->coords_type();
  if (mode == QueryMode::READ_SORTED_ROW) {
    if (coords_type == Datatype::INT32) {
      advance_cell_slab_ = advance_cell_slab_row_s<int>;
      calculate_cell_slab_info_ = (cell_order == Layout::ROW_MAJOR) ?
                                      calculate_cell_slab_info_row_row_s<int> :
                                      calculate_cell_slab_info_row_col_s<int>;
    } else if (coords_type == Datatype::INT64) {
      advance_cell_slab_ = advance_cell_slab_row_s<int64_t>;
      calculate_cell_slab_info_ =
          (cell_order == Layout::ROW_MAJOR) ?
              calculate_cell_slab_info_row_row_s<int64_t> :
              calculate_cell_slab_info_row_col_s<int64_t>;
    } else if (coords_type == Datatype::FLOAT32) {
      advance_cell_slab_ = advance_cell_slab_row_s<float>;
      calculate_cell_slab_info_ =
          (cell_order == Layout::ROW_MAJOR) ?
              calculate_cell_slab_info_row_row_s<float> :
              calculate_cell_slab_info_row_col_s<float>;
    } else if (coords_type == Datatype::FLOAT64) {
      advance_cell_slab_ = advance_cell_slab_row_s<double>;
      calculate_cell_slab_info_ =
          (cell_order == Layout::ROW_MAJOR) ?
              calculate_cell_slab_info_row_row_s<double> :
              calculate_cell_slab_info_row_col_s<double>;
    } else if (coords_type == Datatype::INT8) {
      advance_cell_slab_ = advance_cell_slab_row_s<int8_t>;
      calculate_cell_slab_info_ =
          (cell_order == Layout::ROW_MAJOR) ?
              calculate_cell_slab_info_row_row_s<int8_t> :
              calculate_cell_slab_info_row_col_s<int8_t>;
    } else if (coords_type == Datatype::UINT8) {
      advance_cell_slab_ = advance_cell_slab_row_s<uint8_t>;
      calculate_cell_slab_info_ =
          (cell_order == Layout::ROW_MAJOR) ?
              calculate_cell_slab_info_row_row_s<uint8_t> :
              calculate_cell_slab_info_row_col_s<uint8_t>;
    } else if (coords_type == Datatype::INT16) {
      advance_cell_slab_ = advance_cell_slab_row_s<int16_t>;
      calculate_cell_slab_info_ =
          (cell_order == Layout::ROW_MAJOR) ?
              calculate_cell_slab_info_row_row_s<int16_t> :
              calculate_cell_slab_info_row_col_s<int16_t>;
    } else if (coords_type == Datatype::UINT16) {
      advance_cell_slab_ = advance_cell_slab_row_s<uint16_t>;
      calculate_cell_slab_info_ =
          (cell_order == Layout::ROW_MAJOR) ?
              calculate_cell_slab_info_row_row_s<uint16_t> :
              calculate_cell_slab_info_row_col_s<uint16_t>;
    } else if (coords_type == Datatype::UINT32) {
      advance_cell_slab_ = advance_cell_slab_row_s<uint32_t>;
      calculate_cell_slab_info_ =
          (cell_order == Layout::ROW_MAJOR) ?
              calculate_cell_slab_info_row_row_s<uint32_t> :
              calculate_cell_slab_info_row_col_s<uint32_t>;
    } else if (coords_type == Datatype::UINT64) {
      advance_cell_slab_ = advance_cell_slab_row_s<uint64_t>;
      calculate_cell_slab_info_ =
          (cell_order == Layout::ROW_MAJOR) ?
              calculate_cell_slab_info_row_row_s<uint64_t> :
              calculate_cell_slab_info_row_col_s<uint64_t>;
    } else {
      assert(0);
    }
  } else {  // mode == TILEDB_ARRAY_READ_SORTED_COL
    if (coords_type == Datatype::INT32) {
      advance_cell_slab_ = advance_cell_slab_col_s<int>;
      calculate_cell_slab_info_ = (cell_order == Layout::ROW_MAJOR) ?
                                      calculate_cell_slab_info_col_row_s<int> :
                                      calculate_cell_slab_info_col_col_s<int>;
    } else if (coords_type == Datatype::INT64) {
      advance_cell_slab_ = advance_cell_slab_col_s<int64_t>;
      calculate_cell_slab_info_ =
          (cell_order == Layout::ROW_MAJOR) ?
              calculate_cell_slab_info_col_row_s<int64_t> :
              calculate_cell_slab_info_col_col_s<int64_t>;
    } else if (coords_type == Datatype::FLOAT32) {
      advance_cell_slab_ = advance_cell_slab_col_s<float>;
      calculate_cell_slab_info_ =
          (cell_order == Layout::ROW_MAJOR) ?
              calculate_cell_slab_info_col_row_s<float> :
              calculate_cell_slab_info_col_col_s<float>;
    } else if (coords_type == Datatype::FLOAT64) {
      advance_cell_slab_ = advance_cell_slab_col_s<double>;
      calculate_cell_slab_info_ =
          (cell_order == Layout::ROW_MAJOR) ?
              calculate_cell_slab_info_col_row_s<double> :
              calculate_cell_slab_info_col_col_s<double>;
    } else if (coords_type == Datatype::INT8) {
      advance_cell_slab_ = advance_cell_slab_col_s<int8_t>;
      calculate_cell_slab_info_ =
          (cell_order == Layout::ROW_MAJOR) ?
              calculate_cell_slab_info_col_row_s<int8_t> :
              calculate_cell_slab_info_col_col_s<int8_t>;
    } else if (coords_type == Datatype::UINT8) {
      advance_cell_slab_ = advance_cell_slab_col_s<uint8_t>;
      calculate_cell_slab_info_ =
          (cell_order == Layout::ROW_MAJOR) ?
              calculate_cell_slab_info_col_row_s<uint8_t> :
              calculate_cell_slab_info_col_col_s<uint8_t>;
    } else if (coords_type == Datatype::INT16) {
      advance_cell_slab_ = advance_cell_slab_col_s<int16_t>;
      calculate_cell_slab_info_ =
          (cell_order == Layout::ROW_MAJOR) ?
              calculate_cell_slab_info_col_row_s<int16_t> :
              calculate_cell_slab_info_col_col_s<int16_t>;
    } else if (coords_type == Datatype::UINT16) {
      advance_cell_slab_ = advance_cell_slab_col_s<uint16_t>;
      calculate_cell_slab_info_ =
          (cell_order == Layout::ROW_MAJOR) ?
              calculate_cell_slab_info_col_row_s<uint16_t> :
              calculate_cell_slab_info_col_col_s<uint16_t>;
    } else if (coords_type == Datatype::UINT32) {
      advance_cell_slab_ = advance_cell_slab_col_s<uint32_t>;
      calculate_cell_slab_info_ =
          (cell_order == Layout::ROW_MAJOR) ?
              calculate_cell_slab_info_col_row_s<uint32_t> :
              calculate_cell_slab_info_col_col_s<uint32_t>;
    } else if (coords_type == Datatype::UINT64) {
      advance_cell_slab_ = advance_cell_slab_col_s<uint64_t>;
      calculate_cell_slab_info_ =
          (cell_order == Layout::ROW_MAJOR) ?
              calculate_cell_slab_info_col_row_s<uint64_t> :
              calculate_cell_slab_info_col_col_s<uint64_t>;
    } else {
      assert(0);
    }
  }
  if (tile_order == Layout::ROW_MAJOR) {
    if (coords_type == Datatype::INT32)
      calculate_tile_slab_info_ = calculate_tile_slab_info_row<int>;
    else if (coords_type == Datatype::INT64)
      calculate_tile_slab_info_ = calculate_tile_slab_info_row<int64_t>;
    else if (coords_type == Datatype::FLOAT32)
      calculate_tile_slab_info_ = calculate_tile_slab_info_row<float>;
    else if (coords_type == Datatype::FLOAT64)
      calculate_tile_slab_info_ = calculate_tile_slab_info_row<double>;
    else if (coords_type == Datatype::INT8)
      calculate_tile_slab_info_ = calculate_tile_slab_info_row<int8_t>;
    else if (coords_type == Datatype::UINT8)
      calculate_tile_slab_info_ = calculate_tile_slab_info_row<uint8_t>;
    else if (coords_type == Datatype::INT16)
      calculate_tile_slab_info_ = calculate_tile_slab_info_row<int16_t>;
    else if (coords_type == Datatype::UINT16)
      calculate_tile_slab_info_ = calculate_tile_slab_info_row<uint16_t>;
    else if (coords_type == Datatype::UINT32)
      calculate_tile_slab_info_ = calculate_tile_slab_info_row<uint32_t>;
    else if (coords_type == Datatype::UINT64)
      calculate_tile_slab_info_ = calculate_tile_slab_info_row<uint64_t>;
    else
      assert(0);
  } else {  // tile_order == Layout::COL_MAJOR
    if (coords_type == Datatype::INT32)
      calculate_tile_slab_info_ = calculate_tile_slab_info_col<int>;
    else if (coords_type == Datatype::INT64)
      calculate_tile_slab_info_ = calculate_tile_slab_info_col<int64_t>;
    else if (coords_type == Datatype::FLOAT32)
      calculate_tile_slab_info_ = calculate_tile_slab_info_col<float>;
    else if (coords_type == Datatype::FLOAT64)
      calculate_tile_slab_info_ = calculate_tile_slab_info_col<double>;
    else if (coords_type == Datatype::INT8)
      calculate_tile_slab_info_ = calculate_tile_slab_info_col<int8_t>;
    else if (coords_type == Datatype::UINT8)
      calculate_tile_slab_info_ = calculate_tile_slab_info_col<uint8_t>;
    else if (coords_type == Datatype::INT16)
      calculate_tile_slab_info_ = calculate_tile_slab_info_col<int16_t>;
    else if (coords_type == Datatype::UINT16)
      calculate_tile_slab_info_ = calculate_tile_slab_info_col<uint16_t>;
    else if (coords_type == Datatype::UINT32)
      calculate_tile_slab_info_ = calculate_tile_slab_info_col<uint32_t>;
    else if (coords_type == Datatype::UINT64)
      calculate_tile_slab_info_ = calculate_tile_slab_info_col<uint64_t>;
    else
      assert(0);
  }

  return Status::Ok();
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

template <class T>
void* ArraySortedReadState::advance_cell_slab_col_s(void* data) {
  ArraySortedReadState* asrs = ((ASRS_Data*)data)->asrs_;
  int aid = ((ASRS_Data*)data)->id_;
  asrs->advance_cell_slab_col<T>(aid);
  return nullptr;
}

template <class T>
void* ArraySortedReadState::advance_cell_slab_row_s(void* data) {
  ArraySortedReadState* asrs = ((ASRS_Data*)data)->asrs_;
  int aid = ((ASRS_Data*)data)->id_;
  asrs->advance_cell_slab_row<T>(aid);
  return nullptr;
}

template <class T>
void ArraySortedReadState::advance_cell_slab_col(int aid) {
  // For easy reference
  int64_t& tid = tile_slab_state_.current_tile_[aid];  // Tile id
  int64_t cell_slab_num = tile_slab_info_[copy_id_].cell_slab_num_[tid];
  T* current_coords = (T*)tile_slab_state_.current_coords_[aid];
  const T* tile_slab = (const T*)tile_slab_norm_[copy_id_];

  // Advance cell slab coordinates
  int d = 0;
  current_coords[d] += cell_slab_num;
  int64_t dim_overflow;
  for (int i = 0; i < dim_num_ - 1; ++i) {
    dim_overflow = (current_coords[i] - tile_slab[2 * i]) /
                   (tile_slab[2 * i + 1] - tile_slab[2 * i] + 1);
    current_coords[i + 1] += dim_overflow;
    current_coords[i] -=
        dim_overflow * (tile_slab[2 * i + 1] - tile_slab[2 * i] + 1);
  }

  // Check if done
  if (current_coords[dim_num_ - 1] > tile_slab[2 * (dim_num_ - 1) + 1]) {
    tile_slab_state_.copy_tile_slab_done_[aid] = true;
    return;
  }

  // Calculate new tile and offset for the current coords
  update_current_tile_and_offset<T>(aid);
}

template <class T>
void ArraySortedReadState::advance_cell_slab_row(int aid) {
  // For easy reference
  int64_t& tid = tile_slab_state_.current_tile_[aid];  // Tile id
  int64_t cell_slab_num = tile_slab_info_[copy_id_].cell_slab_num_[tid];
  T* current_coords = (T*)tile_slab_state_.current_coords_[aid];
  const T* tile_slab = (const T*)tile_slab_norm_[copy_id_];

  // Advance cell slab coordinates
  int d = dim_num_ - 1;
  current_coords[d] += cell_slab_num;
  int64_t dim_overflow;
  for (int i = d; i > 0; --i) {
    dim_overflow = (current_coords[i] - tile_slab[2 * i]) /
                   (tile_slab[2 * i + 1] - tile_slab[2 * i] + 1);
    current_coords[i - 1] += dim_overflow;
    current_coords[i] -=
        dim_overflow * (tile_slab[2 * i + 1] - tile_slab[2 * i] + 1);
  }

  // Check if done
  if (current_coords[0] > tile_slab[1]) {
    tile_slab_state_.copy_tile_slab_done_[aid] = true;
    return;
  }

  // Calculate new tile and offset for the current coords
  update_current_tile_and_offset<T>(aid);
}

void* ArraySortedReadState::aio_done(void* data) {
  // Retrieve data
  ArraySortedReadState* asrs = ((ASRS_Data*)data)->asrs_;
  int id = ((ASRS_Data*)data)->id_;

  // For easy reference
  int anum = (int)asrs->attribute_ids_.size();
  const ArraySchema* array_schema = asrs->query_->array()->array_schema();

  // Check for overflow
  bool overflow = false;
  for (int i = 0; i < anum; ++i) {
    if (asrs->overflow_still_[i] && asrs->aio_overflow_[id][i]) {
      overflow = true;
      break;
    }
  }

  // Handle overflow
  bool sparse = array_schema->dense();
  if (overflow) {  // OVERFLOW
    // Update buffer sizes
    for (int i = 0, b = 0; i < anum; ++i) {
      if (!array_schema->var_size(asrs->attribute_ids_[i])) {  // FIXED
        if (asrs->aio_overflow_[id][i]) {
          // Expand buffer
          utils::expand_buffer(
              asrs->buffers_[id][b], asrs->buffer_sizes_[id][b]);
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
      } else {  // VAR
        if (asrs->aio_overflow_[id][i]) {
          // Expand offset buffer only in the case of sparse arrays
          if (sparse)
            utils::expand_buffer(
                asrs->buffers_[id][b], asrs->buffer_sizes_[id][b]);
          // Re-assign the buffer size for the fixed-sized offsets
          asrs->buffer_sizes_tmp_[id][b] = asrs->buffer_sizes_[id][b];
          ++b;
          // Expand variable-length cell buffers for both dense and sparse
          utils::expand_buffer(
              asrs->buffers_[id][b], asrs->buffer_sizes_[id][b]);
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
  } else {  // NO OVERFLOW
    // Restore backup temporary buffer sizes
    for (int b = 0; b < asrs->buffer_num_; ++b) {
      if (asrs->buffer_sizes_tmp_bak_[id][b] != 0)
        asrs->buffer_sizes_tmp_[id][b] = asrs->buffer_sizes_tmp_bak_[id][b];
    }

    // Manage the mutexes and conditions
    asrs->aio_notify(id);
  }

  return nullptr;
}

void ArraySortedReadState::aio_notify(int id) {
  {
    std::lock_guard<std::mutex> lk(aio_mtx_[id]);
    wait_aio_[id] = false;
  }
  aio_cv_[id].notify_one();
}

void ArraySortedReadState::aio_wait(int id) {
  std::unique_lock<std::mutex> lk(aio_mtx_[id]);
  aio_cv_[id].wait(lk, [id, this] { return !wait_aio_[id]; });
  lk.unlock();
}

void ArraySortedReadState::calculate_attribute_ids() {
  // Initialization
  attribute_ids_ = query_->attribute_ids();
  coords_attr_i_ = -1;

  // For ease reference
  const ArraySchema* array_schema = query_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();

  // No need to do anything else in case the array is dense
  if (array_schema->dense())
    return;

  // Find the coordinates index
  for (int i = 0; i < (int)attribute_ids_.size(); ++i) {
    if (attribute_ids_[i] == attribute_num) {
      coords_attr_i_ = i;
      break;
    }
  }

  // If the coordinates index is not found, append coordinates attribute
  // to attribute ids.
  if (coords_attr_i_ == -1) {
    attribute_ids_.push_back(attribute_num);
    coords_attr_i_ = attribute_ids_.size() - 1;
    extra_coords_ = true;
  } else {  // No extra coordinates appended
    extra_coords_ = false;
  }
}

void ArraySortedReadState::calculate_buffer_num() {
  // For easy reference
  const ArraySchema* array_schema = query_->array()->array_schema();
  int attribute_num = array_schema->attribute_num();

  // Calculate number of buffers
  buffer_num_ = 0;
  int attribute_id_num = (int)attribute_ids_.size();
  for (int i = 0; i < attribute_id_num; ++i) {
    // Fix-sized attribute
    if (!array_schema->var_size(attribute_ids_[i])) {
      if (attribute_ids_[i] == attribute_num)
        coords_buf_i_ = i;  // Buffer that holds the coordinates
      ++buffer_num_;
    } else {  // Variable-sized attribute
      buffer_num_ += 2;
    }
  }
}

void ArraySortedReadState::calculate_buffer_sizes() {
  if (query_->array()->array_schema()->dense())
    calculate_buffer_sizes_dense();
  else
    calculate_buffer_sizes_sparse();
}

void ArraySortedReadState::calculate_buffer_sizes_dense() {
  // For easy reference
  const ArraySchema* array_schema = query_->array()->array_schema();

  // Get cell number in a (full) tile slab
  int64_t tile_slab_cell_num;
  if (query_->mode() == QueryMode::READ_SORTED_ROW)
    tile_slab_cell_num = array_schema->tile_slab_row_cell_num(subarray_);
  else  // TILEDB_ARRAY_READ_SORTED_COL
    tile_slab_cell_num = array_schema->tile_slab_col_cell_num(subarray_);

  // Calculate buffer sizes
  int attribute_id_num = (int)attribute_ids_.size();
  for (int j = 0; j < 2; ++j) {
    buffer_sizes_[j] = new size_t[buffer_num_];
    buffer_sizes_tmp_[j] = new size_t[buffer_num_];
    buffer_sizes_tmp_bak_[j] = new size_t[buffer_num_];
    for (int i = 0, b = 0; i < attribute_id_num; ++i) {
      // Fix-sized attribute
      if (!array_schema->var_size(attribute_ids_[i])) {
        buffer_sizes_[j][b] =
            tile_slab_cell_num * array_schema->cell_size(attribute_ids_[i]);
        buffer_sizes_tmp_bak_[j][b] = 0;
        ++b;
      } else {  // Variable-sized attribute
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
  const ArraySchema* array_schema = query_->array()->array_schema();

  // Calculate buffer sizes
  int attribute_id_num = (int)attribute_ids_.size();
  for (int j = 0; j < 2; ++j) {
    buffer_sizes_[j] = new size_t[buffer_num_];
    buffer_sizes_tmp_[j] = new size_t[buffer_num_];
    buffer_sizes_tmp_bak_[j] = new size_t[buffer_num_];
    for (int i = 0, b = 0; i < attribute_id_num; ++i) {
      // Fix-sized buffer
      buffer_sizes_[j][b] = constants::internal_buffer_size;
      buffer_sizes_tmp_bak_[j][b] = 0;
      ++b;
      if (array_schema->var_size(attribute_ids_[i])) {  // Variable-sized buffer
        buffer_sizes_[j][b] = 2 * constants::internal_buffer_size;
        buffer_sizes_tmp_bak_[j][b] = 0;
        ++b;
      }
    }
  }
}

template <class T>
void* ArraySortedReadState::calculate_cell_slab_info_col_col_s(void* data) {
  ArraySortedReadState* asrs = ((ASRS_Data*)data)->asrs_;
  int id = ((ASRS_Data*)data)->id_;
  int64_t tid = ((ASRS_Data*)data)->id_2_;
  asrs->calculate_cell_slab_info_col_col<T>(id, tid);
  return nullptr;
}

template <class T>
void* ArraySortedReadState::calculate_cell_slab_info_col_row_s(void* data) {
  ArraySortedReadState* asrs = ((ASRS_Data*)data)->asrs_;
  int id = ((ASRS_Data*)data)->id_;
  int64_t tid = ((ASRS_Data*)data)->id_2_;
  asrs->calculate_cell_slab_info_col_row<T>(id, tid);
  return nullptr;
}

template <class T>
void* ArraySortedReadState::calculate_cell_slab_info_row_col_s(void* data) {
  ArraySortedReadState* asrs = ((ASRS_Data*)data)->asrs_;
  int id = ((ASRS_Data*)data)->id_;
  int64_t tid = ((ASRS_Data*)data)->id_2_;
  asrs->calculate_cell_slab_info_row_col<T>(id, tid);
  return nullptr;
}

template <class T>
void* ArraySortedReadState::calculate_cell_slab_info_row_row_s(void* data) {
  ArraySortedReadState* asrs = ((ASRS_Data*)data)->asrs_;
  int id = ((ASRS_Data*)data)->id_;
  int64_t tid = ((ASRS_Data*)data)->id_2_;
  asrs->calculate_cell_slab_info_row_row<T>(id, tid);
  return nullptr;
}

template <class T>
void ArraySortedReadState::calculate_cell_slab_info_col_col(
    int id, int64_t tid) {
  // For easy reference
  int anum = (int)attribute_ids_.size();
  const T* range_overlap = (const T*)tile_slab_info_[id].range_overlap_[tid];
  const T* tile_domain = (const T*)tile_domain_;
  int64_t tile_num, cell_num;

  // Calculate number of cells in cell slab
  cell_num = range_overlap[1] - range_overlap[0] + 1;
  for (int i = 0; i < dim_num_ - 1; ++i) {
    tile_num = tile_domain[2 * i + 1] - tile_domain[2 * i] + 1;
    if (tile_num == 1)
      cell_num *=
          range_overlap[2 * (i + 1) + 1] - range_overlap[2 * (i + 1)] + 1;
    else
      break;
  }
  tile_slab_info_[id].cell_slab_num_[tid] = cell_num;

  // Calculate size of a cell slab per attribute
  for (int aid = 0; aid < anum; ++aid)
    tile_slab_info_[id].cell_slab_size_[aid][tid] =
        tile_slab_info_[id].cell_slab_num_[tid] * attribute_sizes_[aid];

  // Calculate cell offset per dimension
  int64_t cell_offset = 1;
  tile_slab_info_[id].cell_offset_per_dim_[tid][0] = cell_offset;
  for (int i = 1; i < dim_num_; ++i) {
    cell_offset *=
        (range_overlap[2 * (i - 1) + 1] - range_overlap[2 * (i - 1)] + 1);
    tile_slab_info_[id].cell_offset_per_dim_[tid][i] = cell_offset;
  }
}

template <class T>
void ArraySortedReadState::calculate_cell_slab_info_row_row(
    int id, int64_t tid) {
  // For easy reference
  int anum = (int)attribute_ids_.size();
  const T* range_overlap = (const T*)tile_slab_info_[id].range_overlap_[tid];
  const T* tile_domain = (const T*)tile_domain_;
  int64_t tile_num, cell_num;

  // Calculate number of cells in cell slab
  cell_num = range_overlap[2 * (dim_num_ - 1) + 1] -
             range_overlap[2 * (dim_num_ - 1)] + 1;
  for (int i = dim_num_ - 1; i > 0; --i) {
    tile_num = tile_domain[2 * i + 1] - tile_domain[2 * i] + 1;
    if (tile_num == 1)
      cell_num *=
          range_overlap[2 * (i - 1) + 1] - range_overlap[2 * (i - 1)] + 1;
    else
      break;
  }
  tile_slab_info_[id].cell_slab_num_[tid] = cell_num;

  // Calculate size of a cell slab per attribute
  for (int aid = 0; aid < anum; ++aid)
    tile_slab_info_[id].cell_slab_size_[aid][tid] =
        tile_slab_info_[id].cell_slab_num_[tid] * attribute_sizes_[aid];

  // Calculate cell offset per dimension
  int64_t cell_offset = 1;
  tile_slab_info_[id].cell_offset_per_dim_[tid][dim_num_ - 1] = cell_offset;
  for (int i = dim_num_ - 2; i >= 0; --i) {
    cell_offset *=
        (range_overlap[2 * (i + 1) + 1] - range_overlap[2 * (i + 1)] + 1);
    tile_slab_info_[id].cell_offset_per_dim_[tid][i] = cell_offset;
  }
}

template <class T>
void ArraySortedReadState::calculate_cell_slab_info_col_row(
    int id, int64_t tid) {
  // For easy reference
  int anum = (int)attribute_ids_.size();
  const T* range_overlap = (const T*)tile_slab_info_[id].range_overlap_[tid];

  // Calculate number of cells in cell slab
  tile_slab_info_[id].cell_slab_num_[tid] = 1;

  // Calculate size of a cell slab per attribute
  for (int aid = 0; aid < anum; ++aid)
    tile_slab_info_[id].cell_slab_size_[aid][tid] =
        tile_slab_info_[id].cell_slab_num_[tid] * attribute_sizes_[aid];

  // Calculate cell offset per dimension
  int64_t cell_offset = 1;
  tile_slab_info_[id].cell_offset_per_dim_[tid][dim_num_ - 1] = cell_offset;
  for (int i = dim_num_ - 2; i >= 0; --i) {
    cell_offset *=
        (range_overlap[2 * (i + 1) + 1] - range_overlap[2 * (i + 1)] + 1);
    tile_slab_info_[id].cell_offset_per_dim_[tid][i] = cell_offset;
  }
}

template <class T>
void ArraySortedReadState::calculate_cell_slab_info_row_col(
    int id, int64_t tid) {
  // For easy reference
  int anum = (int)attribute_ids_.size();
  const T* range_overlap = (const T*)tile_slab_info_[id].range_overlap_[tid];

  // Calculate number of cells in cell slab
  tile_slab_info_[id].cell_slab_num_[tid] = 1;

  // Calculate size of a cell slab per attribute
  for (int aid = 0; aid < anum; ++aid)
    tile_slab_info_[id].cell_slab_size_[aid][tid] =
        tile_slab_info_[id].cell_slab_num_[tid] * attribute_sizes_[aid];

  // Calculate cell offset per dimension
  int64_t cell_offset = 1;
  tile_slab_info_[id].cell_offset_per_dim_[tid][0] = cell_offset;
  for (int i = 1; i < dim_num_; ++i) {
    cell_offset *=
        (range_overlap[2 * (i - 1) + 1] - range_overlap[2 * (i - 1)] + 1);
    tile_slab_info_[id].cell_offset_per_dim_[tid][i] = cell_offset;
  }
}

template <class T>
void ArraySortedReadState::calculate_tile_domain(int id) {
  // Initializations
  tile_coords_ = malloc(coords_size_);
  tile_domain_ = malloc(2 * coords_size_);

  // For easy reference
  const T* tile_slab = (const T*)tile_slab_norm_[id];
  const T* tile_extents =
      (const T*)query_->array()->array_schema()->tile_extents();
  T* tile_coords = (T*)tile_coords_;
  T* tile_domain = (T*)tile_domain_;

  // Calculate tile domain and initial tile coordinates
  for (int i = 0; i < dim_num_; ++i) {
    tile_coords[i] = 0;
    tile_domain[2 * i] = tile_slab[2 * i] / tile_extents[i];
    tile_domain[2 * i + 1] = tile_slab[2 * i + 1] / tile_extents[i];
  }
}

template <class T>
void ArraySortedReadState::calculate_tile_slab_info(int id) {
  // Calculate number of tiles, if they are not already calculated
  if (tile_slab_info_[id].tile_num_ == -1)
    init_tile_slab_info<T>(id);

  // Calculate tile domain, if not calculated yet
  if (tile_domain_ == nullptr)
    calculate_tile_domain<T>(id);

  // Reset tile coordinates
  reset_tile_coords<T>();

  // Calculate tile slab info
  ASRS_Data asrs_data = {id, 0, this};
  (*calculate_tile_slab_info_)(&asrs_data);
}

template <class T>
void* ArraySortedReadState::calculate_tile_slab_info_col(void* data) {
  ArraySortedReadState* asrs = ((ASRS_Data*)data)->asrs_;
  int id = ((ASRS_Data*)data)->id_;
  asrs->calculate_tile_slab_info_col<T>(id);
  return nullptr;
}

template <class T>
void ArraySortedReadState::calculate_tile_slab_info_col(int id) {
  // For easy reference
  const T* tile_domain = (const T*)tile_domain_;
  T* tile_coords = (T*)tile_coords_;
  const T* tile_extents =
      (const T*)query_->array()->array_schema()->tile_extents();
  T** range_overlap = (T**)tile_slab_info_[id].range_overlap_;
  const T* tile_slab = (const T*)tile_slab_norm_[id];
  int64_t tile_offset, tile_cell_num, total_cell_num = 0;
  int anum = (int)attribute_ids_.size();
  int d;

  // Iterate over all tiles in the tile domain
  int64_t tid = 0;  // Tile id
  while (tile_coords[dim_num_ - 1] <= tile_domain[2 * (dim_num_ - 1) + 1]) {
    // Calculate range overlap, number of cells in the tile
    tile_cell_num = 1;
    for (int i = 0; i < dim_num_; ++i) {
      // Range overlap
      range_overlap[tid][2 * i] =
          MAX(tile_coords[i] * tile_extents[i], tile_slab[2 * i]);
      range_overlap[tid][2 * i + 1] =
          MIN((tile_coords[i] + 1) * tile_extents[i] - 1, tile_slab[2 * i + 1]);

      // Number of cells in this tile
      tile_cell_num *=
          range_overlap[tid][2 * i + 1] - range_overlap[tid][2 * i] + 1;
    }

    // Calculate tile offsets per dimension
    tile_offset = 1;
    tile_slab_info_[id].tile_offset_per_dim_[0] = tile_offset;
    for (int i = 1; i < dim_num_; ++i) {
      tile_offset *=
          (tile_domain[2 * (i - 1) + 1] - tile_domain[2 * (i - 1)] + 1);
      tile_slab_info_[id].tile_offset_per_dim_[i] = tile_offset;
    }

    // Calculate cell slab info
    ASRS_Data asrs_data = {id, tid, this};
    (*calculate_cell_slab_info_)(&asrs_data);

    // Calculate start offsets
    for (int aid = 0; aid < anum; ++aid) {
      tile_slab_info_[id].start_offsets_[aid][tid] =
          total_cell_num * attribute_sizes_[aid];
    }
    total_cell_num += tile_cell_num;

    // Advance tile coordinates
    d = 0;
    ++tile_coords[d];
    while (d < dim_num_ - 1 && tile_coords[d] > tile_domain[2 * d + 1]) {
      tile_coords[d] = tile_domain[2 * d];
      ++tile_coords[++d];
    }

    // Advance tile id
    ++tid;
  }
}

template <class T>
void* ArraySortedReadState::calculate_tile_slab_info_row(void* data) {
  ArraySortedReadState* asrs = ((ASRS_Data*)data)->asrs_;
  int id = ((ASRS_Data*)data)->id_;
  asrs->calculate_tile_slab_info_row<T>(id);
  return nullptr;
}

template <class T>
void ArraySortedReadState::calculate_tile_slab_info_row(int id) {
  // For easy reference
  const T* tile_domain = (const T*)tile_domain_;
  T* tile_coords = (T*)tile_coords_;
  const T* tile_extents =
      (const T*)query_->array()->array_schema()->tile_extents();
  T** range_overlap = (T**)tile_slab_info_[id].range_overlap_;
  const T* tile_slab = (const T*)tile_slab_norm_[id];
  int64_t tile_offset, tile_cell_num, total_cell_num = 0;
  int anum = (int)attribute_ids_.size();
  int d;

  // Iterate over all tiles in the tile domain
  int64_t tid = 0;  // Tile id
  while (tile_coords[0] <= tile_domain[1]) {
    // Calculate range overlap, number of cells in the tile
    tile_cell_num = 1;
    for (int i = 0; i < dim_num_; ++i) {
      // Range overlap
      range_overlap[tid][2 * i] =
          MAX(tile_coords[i] * tile_extents[i], tile_slab[2 * i]);
      range_overlap[tid][2 * i + 1] =
          MIN((tile_coords[i] + 1) * tile_extents[i] - 1, tile_slab[2 * i + 1]);

      // Number of cells in this tile
      tile_cell_num *=
          range_overlap[tid][2 * i + 1] - range_overlap[tid][2 * i] + 1;
    }

    // Calculate tile offsets per dimension
    tile_offset = 1;
    tile_slab_info_[id].tile_offset_per_dim_[dim_num_ - 1] = tile_offset;
    for (int i = dim_num_ - 2; i >= 0; --i) {
      tile_offset *=
          (tile_domain[2 * (i + 1) + 1] - tile_domain[2 * (i + 1)] + 1);
      tile_slab_info_[id].tile_offset_per_dim_[i] = tile_offset;
    }

    // Calculate cell slab info
    ASRS_Data asrs_data = {id, tid, this};
    (*calculate_cell_slab_info_)(&asrs_data);

    // Calculate start offsets
    for (int aid = 0; aid < anum; ++aid) {
      tile_slab_info_[id].start_offsets_[aid][tid] =
          total_cell_num * attribute_sizes_[aid];
    }
    total_cell_num += tile_cell_num;

    // Advance tile coordinates
    d = dim_num_ - 1;
    ++tile_coords[d];
    while (d > 0 && tile_coords[d] > tile_domain[2 * d + 1]) {
      tile_coords[d] = tile_domain[2 * d];
      ++tile_coords[--d];
    }

    // Advance tile id
    ++tid;
  }
}

void ArraySortedReadState::copy_tile_slab_dense() {
  // For easy reference
  const ArraySchema* array_schema = query_->array()->array_schema();

  // Copy tile slab for each attribute separately
  for (int i = 0, b = 0; i < (int)attribute_ids_.size(); ++i) {
    if (!array_schema->var_size(attribute_ids_[i])) {
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
  if (tile_slab_state_.copy_tile_slab_done_[aid]) {
    copy_state_.buffer_sizes_[bid] = 0;  // Nothing written
    return;
  }

  // For easy reference
  int64_t& tid = tile_slab_state_.current_tile_[aid];
  size_t& buffer_offset = copy_state_.buffer_offsets_[bid];
  size_t buffer_size = copy_state_.buffer_sizes_[bid];
  char* buffer = (char*)copy_state_.buffers_[bid];
  char* local_buffer = (char*)buffers_[copy_id_][bid];
  ASRS_Data asrs_data = {aid, 0, this};

  // Iterate over the tile slab cells
  for (;;) {
    // For easy reference
    size_t cell_slab_size = tile_slab_info_[copy_id_].cell_slab_size_[aid][tid];
    size_t& local_buffer_offset = tile_slab_state_.current_offsets_[aid];

    // Handle overflow
    if (buffer_offset + cell_slab_size > buffer_size) {
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
    if (tile_slab_state_.copy_tile_slab_done_[aid])
      break;
  }

  // Set user buffer size
  buffer_size = buffer_offset;
}

void ArraySortedReadState::copy_tile_slab_dense_var(int aid, int bid) {
  // Exit if copy is done for this attribute
  if (tile_slab_state_.copy_tile_slab_done_[aid]) {
    copy_state_.buffer_sizes_[bid] = 0;      // Nothing written
    copy_state_.buffer_sizes_[bid + 1] = 0;  // Nothing written
    return;
  }

  // For easy reference
  int64_t& tid = tile_slab_state_.current_tile_[aid];
  size_t cell_slab_size_var;
  size_t& buffer_offset = copy_state_.buffer_offsets_[bid];
  size_t& buffer_offset_var = copy_state_.buffer_offsets_[bid + 1];
  size_t buffer_size = copy_state_.buffer_sizes_[bid];
  size_t buffer_size_var = copy_state_.buffer_sizes_[bid + 1];
  char* buffer = (char*)copy_state_.buffers_[bid];
  char* buffer_var = (char*)copy_state_.buffers_[bid + 1];
  char* local_buffer_var = (char*)buffers_[copy_id_][bid + 1];
  size_t local_buffer_size = buffer_sizes_tmp_[copy_id_][bid];
  size_t local_buffer_var_size = buffer_sizes_tmp_[copy_id_][bid + 1];
  size_t* local_buffer_s = (size_t*)buffers_[copy_id_][bid];
  int64_t cell_num_in_buffer = local_buffer_size / sizeof(size_t);
  size_t var_offset = buffer_offset_var;
  ASRS_Data asrs_data = {aid, 0, this};

  // For all overlapping tiles, in a round-robin fashion
  for (;;) {
    // For easy reference
    size_t cell_slab_size = tile_slab_info_[copy_id_].cell_slab_size_[aid][tid];
    int64_t cell_num_in_slab = cell_slab_size / sizeof(size_t);
    size_t& local_buffer_offset = tile_slab_state_.current_offsets_[aid];

    // Handle overflow
    if (buffer_offset + cell_slab_size > buffer_size) {
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
    if (buffer_offset_var + cell_slab_size_var > buffer_size_var) {
      overflow_[aid] = true;
      break;
    }

    // Copy fixed-sized offsets
    for (int64_t i = cell_start; i < cell_end; ++i) {
      memcpy(buffer + buffer_offset, &var_offset, sizeof(size_t));
      buffer_offset += sizeof(size_t);
      var_offset += (i == cell_num_in_buffer - 1) ?
                        local_buffer_var_size - local_buffer_s[i] :
                        local_buffer_s[i + 1] - local_buffer_s[i];
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
    if (tile_slab_state_.copy_tile_slab_done_[aid])
      break;
  }

  // Set user buffer sizes
  buffer_size = buffer_offset;
  buffer_size_var = buffer_offset_var;
}

void ArraySortedReadState::copy_tile_slab_sparse() {
  // For easy reference
  const ArraySchema* array_schema = query_->array()->array_schema();

  // Copy tile slab for each attribute separately
  for (int i = 0, b = 0; i < (int)attribute_ids_.size(); ++i) {
    if (!array_schema->var_size(attribute_ids_[i])) {  // FIXED
      // Make sure not to copy coordinates if the user has not requested them
      if (i != coords_attr_i_ || !extra_coords_)
        copy_tile_slab_sparse(i, b);
      ++b;
    } else {  // VAR
      copy_tile_slab_sparse_var(i, b);
      b += 2;
    }
  }
}

void ArraySortedReadState::copy_tile_slab_sparse(int aid, int bid) {
  // Exit if copy is done for this attribute
  if (tile_slab_state_.copy_tile_slab_done_[aid]) {
    copy_state_.buffer_sizes_[bid] = 0;  // Nothing written
    return;
  }

  // For easy reference
  size_t cell_size =
      query_->array()->array_schema()->cell_size(attribute_ids_[aid]);
  size_t& buffer_offset = copy_state_.buffer_offsets_[bid];
  size_t buffer_size = copy_state_.buffer_sizes_[bid];
  char* buffer = (char*)copy_state_.buffers_[bid];
  char* local_buffer = (char*)buffers_[copy_id_][bid];
  size_t local_buffer_offset;
  int64_t cell_num = buffer_sizes_tmp_[copy_id_][coords_buf_i_] / coords_size_;
  int64_t& current_cell_pos = tile_slab_state_.current_cell_pos_[aid];

  // Iterate over the remaining tile slab cells in a sorted order
  for (; current_cell_pos < cell_num; ++current_cell_pos) {
    // Handle overflow
    if (buffer_offset + cell_size > buffer_size) {
      overflow_[aid] = true;
      break;
    }

    // Calculate new local buffer offset
    local_buffer_offset = cell_pos_[current_cell_pos] * cell_size;

    // Copy cell slab
    memcpy(
        buffer + buffer_offset, local_buffer + local_buffer_offset, cell_size);

    // Update buffer offset
    buffer_offset += cell_size;
  }

  // Mark tile slab as done
  if (current_cell_pos == cell_num)
    tile_slab_state_.copy_tile_slab_done_[aid] = true;

  // Set user buffer size
  buffer_size = buffer_offset;
}

void ArraySortedReadState::copy_tile_slab_sparse_var(int aid, int bid) {
  // Exit if copy is done for this attribute
  if (tile_slab_state_.copy_tile_slab_done_[aid]) {
    copy_state_.buffer_sizes_[bid] = 0;      // Nothing written
    copy_state_.buffer_sizes_[bid + 1] = 0;  // Nothing written
    return;
  }

  // For easy reference
  size_t cell_size = sizeof(size_t);
  size_t cell_size_var;
  size_t& buffer_offset = copy_state_.buffer_offsets_[bid];
  size_t& buffer_offset_var = copy_state_.buffer_offsets_[bid + 1];
  size_t buffer_size = copy_state_.buffer_sizes_[bid];
  size_t buffer_size_var = copy_state_.buffer_sizes_[bid + 1];
  char* buffer = (char*)copy_state_.buffers_[bid];
  char* buffer_var = (char*)copy_state_.buffers_[bid + 1];
  char* local_buffer_var = (char*)buffers_[copy_id_][bid + 1];
  size_t local_buffer_var_size = buffer_sizes_tmp_[copy_id_][bid + 1];
  size_t* local_buffer_s = (size_t*)buffers_[copy_id_][bid];
  int64_t cell_num = buffer_sizes_tmp_[copy_id_][coords_buf_i_] / coords_size_;
  int64_t& current_cell_pos = tile_slab_state_.current_cell_pos_[aid];

  // Iterate over the remaining tile slab cells in a sorted order
  for (; current_cell_pos < cell_num; ++current_cell_pos) {
    // Handle overflow
    if (buffer_offset + cell_size > buffer_size) {
      overflow_[aid] = true;
      break;
    }

    // Calculate variable cell size
    int64_t cell_start = cell_pos_[current_cell_pos];
    int64_t cell_end = cell_start + 1;
    cell_size_var = (cell_end == cell_num) ?
                        local_buffer_var_size - local_buffer_s[cell_start] :
                        local_buffer_s[cell_end] - local_buffer_s[cell_start];

    // Handle overflow for the the variable-length buffer
    if (buffer_offset_var + cell_size_var > buffer_size_var) {
      overflow_[aid] = true;
      break;
    }

    // Copy fixed-sized offset
    memcpy(buffer + buffer_offset, &buffer_offset_var, sizeof(size_t));
    buffer_offset += sizeof(size_t);

    // Copy variable-sized values
    memcpy(
        buffer_var + buffer_offset_var,
        local_buffer_var + local_buffer_s[cell_start],
        cell_size_var);
    buffer_offset_var += cell_size_var;
  }

  // Mark tile slab as done
  if (current_cell_pos == cell_num)
    tile_slab_state_.copy_tile_slab_done_[aid] = true;

  // Set user buffer sizes
  buffer_size = buffer_offset;
  buffer_size_var = buffer_offset_var;
}

Status ArraySortedReadState::create_buffers() {
  for (int j = 0; j < 2; ++j) {
    buffers_[j] = (void**)malloc(buffer_num_ * sizeof(void*));
    if (buffers_[j] == nullptr) {
      return LOG_STATUS(Status::ASRSError("Cannot create local buffers"));
    }

    for (int b = 0; b < buffer_num_; ++b) {
      buffers_[j][b] = malloc(buffer_sizes_[j][b]);
      if (buffers_[j][b] == nullptr) {
        return LOG_STATUS(Status::ASRSError("Cannot allocate local buffer"));
      }
    }
  }
  return Status::Ok();
}

void ArraySortedReadState::free_copy_state() {
  if (copy_state_.buffer_offsets_ != nullptr)
    delete[] copy_state_.buffer_offsets_;
}

void ArraySortedReadState::free_tile_slab_info() {
  // Do nothing in the case of sparse arrays
  if (!query_->array()->array_schema()->dense())
    return;

  // For easy reference
  int anum = (int)attribute_ids_.size();

  // Free
  for (auto& info : tile_slab_info_) {
    int64_t tile_num = info.tile_num_;

    if (info.cell_offset_per_dim_ != nullptr) {
      for (int tile = 0; tile < tile_num; ++tile)
        delete[] info.cell_offset_per_dim_[tile];
      delete[] info.cell_offset_per_dim_;
    }

    for (int attr = 0; attr < anum; ++attr) {
      if (info.cell_slab_size_[attr] != nullptr)
        delete[] info.cell_slab_size_[attr];
    }
    delete[] info.cell_slab_size_;

    if (info.cell_slab_num_ != nullptr)
      delete[] info.cell_slab_num_;

    if (info.range_overlap_ != nullptr) {
      for (int tile = 0; tile < tile_num; ++tile)
        free(info.range_overlap_[tile]);
      delete[] info.range_overlap_;
    }

    for (int attr = 0; attr < anum; ++attr) {
      if (info.start_offsets_[attr] != nullptr)
        delete[] info.start_offsets_[attr];
    }
    delete[] info.start_offsets_;

    delete[] info.tile_offset_per_dim_;
  }
}

void ArraySortedReadState::free_tile_slab_state() {
  // For easy reference
  int anum = (int)attribute_ids_.size();

  // Clean up
  if (tile_slab_state_.current_coords_ != nullptr) {
    for (int i = 0; i < anum; ++i)
      free(tile_slab_state_.current_coords_[i]);
    delete[] tile_slab_state_.current_coords_;
  }

  if (tile_slab_state_.copy_tile_slab_done_ != nullptr)
    delete[] tile_slab_state_.copy_tile_slab_done_;

  if (tile_slab_state_.current_offsets_ != nullptr)
    delete[] tile_slab_state_.current_offsets_;

  if (tile_slab_state_.current_tile_ != nullptr)
    delete[] tile_slab_state_.current_tile_;

  if (tile_slab_state_.current_cell_pos_ != nullptr)
    delete[] tile_slab_state_.current_cell_pos_;
}

template <class T>
int64_t ArraySortedReadState::get_cell_id(int aid) {
  // For easy reference
  const T* current_coords = (const T*)tile_slab_state_.current_coords_[aid];
  int64_t tid = tile_slab_state_.current_tile_[aid];
  const T* range_overlap =
      (const T*)tile_slab_info_[copy_id_].range_overlap_[tid];
  int64_t* cell_offset_per_dim =
      tile_slab_info_[copy_id_].cell_offset_per_dim_[tid];

  // Calculate cell id
  int64_t cid = 0;
  for (int i = 0; i < dim_num_; ++i)
    cid += (current_coords[i] - range_overlap[2 * i]) * cell_offset_per_dim[i];

  // Return tile id
  return cid;
}

template <class T>
int64_t ArraySortedReadState::get_tile_id(int aid) {
  // For easy reference
  const T* current_coords = (const T*)tile_slab_state_.current_coords_[aid];
  const T* tile_extents =
      (const T*)query_->array()->array_schema()->tile_extents();
  int64_t* tile_offset_per_dim = tile_slab_info_[copy_id_].tile_offset_per_dim_;

  // Calculate tile id
  int64_t tid = 0;
  for (int i = 0; i < dim_num_; ++i)
    tid += (current_coords[i] / tile_extents[i]) * tile_offset_per_dim[i];

  // Return tile id
  return tid;
}

void ArraySortedReadState::init_aio_requests() {
  for (int i = 0; i < 2; ++i) {
    aio_data_[i] = {i, 0, this};
    aio_request_[i].set_query(nullptr);
    aio_request_[i].set_buffer_sizes(buffer_sizes_tmp_[i]);
    aio_request_[i].set_buffers(buffers_[i]);
    aio_request_[i].set_mode(QueryMode::READ);
    aio_request_[i].set_subarray(tile_slab_[i]);
    aio_request_[i].set_callback(aio_done, &(aio_data_[i]));
    aio_request_[i].set_overflow(aio_overflow_[i]);
    aio_request_[i].set_status(&(aio_status_[i]));
  }
}

void ArraySortedReadState::init_copy_state() {
  copy_state_.buffer_sizes_ = nullptr;
  copy_state_.buffers_ = nullptr;
  copy_state_.buffer_offsets_ = new size_t[buffer_num_];
  for (int i = 0; i < buffer_num_; ++i)
    copy_state_.buffer_offsets_[i] = 0;
}

void ArraySortedReadState::init_tile_slab_info() {
  // Do nothing in the case of sparse arrays
  if (!query_->array()->array_schema()->dense())
    return;

  // For easy reference
  int anum = (int)attribute_ids_.size();

  // Initialize
  for (auto& info : tile_slab_info_) {
    info.cell_offset_per_dim_ = nullptr;
    info.cell_slab_size_ = new size_t*[anum];
    info.cell_slab_num_ = nullptr;
    info.range_overlap_ = nullptr;
    info.start_offsets_ = new size_t*[anum];
    info.tile_offset_per_dim_ = new int64_t[dim_num_];

    for (int attr = 0; attr < anum; ++attr) {
      info.cell_slab_size_[attr] = nullptr;
      info.start_offsets_[attr] = nullptr;
    }

    info.tile_num_ = -1;
  }
}

template <class T>
void ArraySortedReadState::init_tile_slab_info(int id) {
  // Sanity check
  assert(query_->array()->array_schema()->dense());

  // For easy reference
  int anum = (int)attribute_ids_.size();

  // Calculate tile number
  int64_t tile_num = query_->array()->array_schema()->tile_num(tile_slab_[id]);

  // Initializations
  tile_slab_info_[id].cell_offset_per_dim_ = new int64_t*[tile_num];
  tile_slab_info_[id].cell_slab_num_ = new int64_t[tile_num];
  tile_slab_info_[id].range_overlap_ = new void*[tile_num];
  for (int64_t i = 0; i < tile_num; ++i) {
    tile_slab_info_[id].range_overlap_[i] = malloc(2 * coords_size_);
    tile_slab_info_[id].cell_offset_per_dim_[i] = new int64_t[dim_num_];
  }

  for (int i = 0; i < anum; ++i) {
    tile_slab_info_[id].cell_slab_size_[i] = new size_t[tile_num];
    tile_slab_info_[id].start_offsets_[i] = new size_t[tile_num];
  }

  tile_slab_info_[id].tile_num_ = tile_num;
}

void ArraySortedReadState::init_tile_slab_state() {
  // For easy reference
  int anum = (int)attribute_ids_.size();
  bool dense = query_->array()->array_schema()->dense();

  // Both for dense and sparse
  tile_slab_state_.copy_tile_slab_done_ = new bool[anum];
  for (int i = 0; i < anum; ++i)
    tile_slab_state_.copy_tile_slab_done_[i] = true;  // Important!

  // Allocations and initializations
  if (dense) {  // DENSE
    tile_slab_state_.current_offsets_ = new size_t[anum];
    tile_slab_state_.current_coords_ = new void*[anum];
    tile_slab_state_.current_tile_ = new int64_t[anum];
    tile_slab_state_.current_cell_pos_ = nullptr;

    for (int i = 0; i < anum; ++i) {
      tile_slab_state_.current_coords_[i] = malloc(coords_size_);
      tile_slab_state_.current_offsets_[i] = 0;
      tile_slab_state_.current_tile_[i] = 0;
    }
  } else {  // SPARSE
    tile_slab_state_.current_offsets_ = nullptr;
    tile_slab_state_.current_coords_ = nullptr;
    tile_slab_state_.current_tile_ = nullptr;
    tile_slab_state_.current_cell_pos_ = new int64_t[anum];

    for (int i = 0; i < anum; ++i)
      tile_slab_state_.current_cell_pos_[i] = 0;
  }
}

template <class T>
bool ArraySortedReadState::next_tile_slab_dense_col() {
  // Quick check if done
  if (read_tile_slabs_done_)
    return false;

  // For easy reference
  const ArraySchema* array_schema = query_->array()->array_schema();
  const T* subarray = static_cast<const T*>(subarray_);
  const T* domain = static_cast<const T*>(array_schema->domain());
  const T* tile_extents = static_cast<const T*>(array_schema->tile_extents());
  T* tile_slab[2];
  T* tile_slab_norm = static_cast<T*>(tile_slab_norm_[copy_id_]);
  for (int i = 0; i < 2; ++i)
    tile_slab[i] = static_cast<T*>(tile_slab_[i]);
  int prev_id = (copy_id_ + 1) % 2;
  T tile_start;

  // Check again if done, this time based on the tile slab and subarray
  if (tile_slab_init_[prev_id] && tile_slab[prev_id][2 * (dim_num_ - 1) + 1] ==
                                      subarray[2 * (dim_num_ - 1) + 1]) {
    read_tile_slabs_done_ = true;
    return false;
  }

  // If this is the first time this function is called, initialize
  if (!tile_slab_init_[prev_id]) {
    // Crop the subarray extent along the first axis to fit in the first tile
    tile_slab[copy_id_][2 * (dim_num_ - 1)] = subarray[2 * (dim_num_ - 1)];
    T upper = subarray[2 * (dim_num_ - 1)] + tile_extents[dim_num_ - 1];
    T cropped_upper = (upper - domain[2 * (dim_num_ - 1)]) /
                          tile_extents[dim_num_ - 1] *
                          tile_extents[dim_num_ - 1] +
                      domain[2 * (dim_num_ - 1)];
    tile_slab[copy_id_][2 * (dim_num_ - 1) + 1] =
        MIN(cropped_upper - 1, subarray[2 * (dim_num_ - 1) + 1]);

    // Leave the rest of the subarray extents intact
    for (int i = 0; i < dim_num_ - 1; ++i) {
      tile_slab[copy_id_][2 * i] = subarray[2 * i];
      tile_slab[copy_id_][2 * i + 1] = subarray[2 * i + 1];
    }
  } else {  // Calculate a new slab based on the previous
    // Copy previous tile slab
    memcpy(tile_slab[copy_id_], tile_slab[prev_id], 2 * coords_size_);

    // Advance tile slab
    tile_slab[copy_id_][2 * (dim_num_ - 1)] =
        tile_slab[copy_id_][2 * (dim_num_ - 1) + 1] + 1;
    tile_slab[copy_id_][2 * (dim_num_ - 1) + 1] =
        MIN(tile_slab[copy_id_][2 * (dim_num_ - 1)] +
                tile_extents[dim_num_ - 1] - 1,
            subarray[2 * (dim_num_ - 1) + 1]);
  }

  // Calculate normalized tile slab
  for (int i = 0; i < dim_num_; ++i) {
    tile_start =
        ((tile_slab[copy_id_][2 * i] - domain[2 * i]) / tile_extents[i]) *
            tile_extents[i] +
        domain[2 * i];
    tile_slab_norm[2 * i] = tile_slab[copy_id_][2 * i] - tile_start;
    tile_slab_norm[2 * i + 1] = tile_slab[copy_id_][2 * i + 1] - tile_start;
  }

  // Calculate tile slab info and reset tile slab state
  calculate_tile_slab_info<T>(copy_id_);

  // Mark this tile slab as initialized
  tile_slab_init_[copy_id_] = true;

  // Success
  return true;
}

template <class T>
bool ArraySortedReadState::next_tile_slab_dense_row() {
  // Quick check if done
  if (read_tile_slabs_done_)
    return false;

  // For easy reference
  const ArraySchema* array_schema = query_->array()->array_schema();
  const T* subarray = static_cast<const T*>(subarray_);
  const T* domain = static_cast<const T*>(array_schema->domain());
  const T* tile_extents = static_cast<const T*>(array_schema->tile_extents());
  T* tile_slab[2];
  T* tile_slab_norm = static_cast<T*>(tile_slab_norm_[copy_id_]);
  for (int i = 0; i < 2; ++i)
    tile_slab[i] = static_cast<T*>(tile_slab_[i]);
  int prev_id = (copy_id_ + 1) % 2;
  T tile_start;

  // Check again if done, this time based on the tile slab and subarray
  if (tile_slab_init_[prev_id] && tile_slab[prev_id][1] == subarray[1]) {
    read_tile_slabs_done_ = true;
    return false;
  }

  // If this is the first time this function is called, initialize
  if (!tile_slab_init_[prev_id]) {
    // Crop the subarray extent along the first axis to fit in the first tile
    tile_slab[copy_id_][0] = subarray[0];
    T upper = subarray[0] + tile_extents[0];
    T cropped_upper =
        (upper - domain[0]) / tile_extents[0] * tile_extents[0] + domain[0];
    tile_slab[copy_id_][1] = MIN(cropped_upper - 1, subarray[1]);

    // Leave the rest of the subarray extents intact
    for (int i = 1; i < dim_num_; ++i) {
      tile_slab[copy_id_][2 * i] = subarray[2 * i];
      tile_slab[copy_id_][2 * i + 1] = subarray[2 * i + 1];
    }
  } else {  // Calculate a new slab based on the previous
    // Copy previous tile slab
    memcpy(tile_slab[copy_id_], tile_slab[prev_id], 2 * coords_size_);

    // Advance tile slab
    tile_slab[copy_id_][0] = tile_slab[copy_id_][1] + 1;
    tile_slab[copy_id_][1] =
        MIN(tile_slab[copy_id_][0] + tile_extents[0] - 1, subarray[1]);
  }

  // Calculate normalized tile slab
  for (int i = 0; i < dim_num_; ++i) {
    tile_start =
        ((tile_slab[copy_id_][2 * i] - domain[2 * i]) / tile_extents[i]) *
            tile_extents[i] +
        domain[2 * i];
    tile_slab_norm[2 * i] = tile_slab[copy_id_][2 * i] - tile_start;
    tile_slab_norm[2 * i + 1] = tile_slab[copy_id_][2 * i + 1] - tile_start;
  }

  // Calculate tile slab info and reset tile slab state
  calculate_tile_slab_info<T>(copy_id_);

  // Mark this tile slab as initialized
  tile_slab_init_[copy_id_] = true;

  // Success
  return true;
}

template <class T>
bool ArraySortedReadState::next_tile_slab_sparse_col() {
  // Quick check if done
  if (read_tile_slabs_done_)
    return false;

  // For easy reference
  const ArraySchema* array_schema = query_->array()->array_schema();
  const T* subarray = static_cast<const T*>(subarray_);
  const T* domain = static_cast<const T*>(array_schema->domain());
  const T* tile_extents = static_cast<const T*>(array_schema->tile_extents());
  T* tile_slab[2];
  for (int i = 0; i < 2; ++i)
    tile_slab[i] = static_cast<T*>(tile_slab_[i]);
  int prev_id = (copy_id_ + 1) % 2;

  // Check again if done, this time based on the tile slab and subarray
  if (tile_slab_init_[prev_id] && tile_slab[prev_id][2 * (dim_num_ - 1) + 1] ==
                                      subarray[2 * (dim_num_ - 1) + 1]) {
    read_tile_slabs_done_ = true;
    return false;
  }

  // If this is the first time this function is called, initialize
  if (!tile_slab_init_[prev_id]) {
    // Crop the subarray extent along the first axis to fit in the first tile
    tile_slab[copy_id_][2 * (dim_num_ - 1)] = subarray[2 * (dim_num_ - 1)];
    T upper = subarray[2 * (dim_num_ - 1)] + tile_extents[dim_num_ - 1];
    T cropped_upper = (upper - domain[2 * (dim_num_ - 1)]) /
                          tile_extents[dim_num_ - 1] *
                          tile_extents[dim_num_ - 1] +
                      domain[2 * (dim_num_ - 1)];
    tile_slab[copy_id_][2 * (dim_num_ - 1) + 1] =
        MIN(cropped_upper - 1, subarray[2 * (dim_num_ - 1) + 1]);

    // Leave the rest of the subarray extents intact
    for (int i = 0; i < dim_num_ - 1; ++i) {
      tile_slab[copy_id_][2 * i] = subarray[2 * i];
      tile_slab[copy_id_][2 * i + 1] = subarray[2 * i + 1];
    }
  } else {  // Calculate a new slab based on the previous
    // Copy previous tile slab
    memcpy(tile_slab[copy_id_], tile_slab[prev_id], 2 * coords_size_);

    // Advance tile slab
    tile_slab[copy_id_][2 * (dim_num_ - 1)] =
        tile_slab[copy_id_][2 * (dim_num_ - 1) + 1] + 1;
    tile_slab[copy_id_][2 * (dim_num_ - 1) + 1] =
        MIN(tile_slab[copy_id_][2 * (dim_num_ - 1)] +
                tile_extents[dim_num_ - 1] - 1,
            subarray[2 * (dim_num_ - 1) + 1]);
  }

  // Mark this tile slab as initialized
  tile_slab_init_[copy_id_] = true;

  // Success
  return true;
}

template <>
bool ArraySortedReadState::next_tile_slab_sparse_col<float>() {
  // Quick check if done
  if (read_tile_slabs_done_)
    return false;

  // For easy reference
  const ArraySchema* array_schema = query_->array()->array_schema();
  const float* subarray = (const float*)subarray_;
  const float* domain = (const float*)array_schema->domain();
  const float* tile_extents = (const float*)array_schema->tile_extents();
  float* tile_slab[2];
  for (int i = 0; i < 2; ++i)
    tile_slab[i] = (float*)tile_slab_[i];
  int prev_id = (copy_id_ + 1) % 2;

  // Check again if done, this time based on the tile slab and subarray
  if (tile_slab_init_[prev_id] && tile_slab[prev_id][2 * (dim_num_ - 1) + 1] ==
                                      subarray[2 * (dim_num_ - 1) + 1]) {
    read_tile_slabs_done_ = true;
    return false;
  }

  // If this is the first time this function is called, initialize
  if (!tile_slab_init_[prev_id]) {
    // Crop the subarray extent along the first axis to fit in the first tile
    tile_slab[copy_id_][2 * (dim_num_ - 1)] = subarray[2 * (dim_num_ - 1)];
    float upper = subarray[2 * (dim_num_ - 1)] + tile_extents[dim_num_ - 1];
    float cropped_upper =
        floor(
            (upper - domain[2 * (dim_num_ - 1)]) / tile_extents[dim_num_ - 1]) *
            tile_extents[dim_num_ - 1] +
        domain[2 * (dim_num_ - 1)];
    tile_slab[copy_id_][2 * (dim_num_ - 1) + 1] =
        MIN(cropped_upper - FLT_MIN, subarray[2 * (dim_num_ - 1) + 1]);

    // Leave the rest of the subarray extents intact
    for (int i = 0; i < dim_num_ - 1; ++i) {
      tile_slab[copy_id_][2 * i] = subarray[2 * i];
      tile_slab[copy_id_][2 * i + 1] = subarray[2 * i + 1];
    }
  } else {  // Calculate a new slab based on the previous
    // Copy previous tile slab
    memcpy(tile_slab[copy_id_], tile_slab[prev_id], 2 * coords_size_);

    // Advance tile slab
    tile_slab[copy_id_][2 * (dim_num_ - 1)] =
        tile_slab[copy_id_][2 * (dim_num_ - 1) + 1] + FLT_MIN;
    tile_slab[copy_id_][2 * (dim_num_ - 1) + 1] =
        MIN(tile_slab[copy_id_][2 * (dim_num_ - 1)] +
                tile_extents[dim_num_ - 1] - FLT_MIN,
            subarray[2 * (dim_num_ - 1) + 1]);
  }

  // Mark this tile slab as initialized
  tile_slab_init_[copy_id_] = true;

  // Success
  return true;
}

template <>
bool ArraySortedReadState::next_tile_slab_sparse_col<double>() {
  // Quick check if done
  if (read_tile_slabs_done_)
    return false;

  // For easy reference
  const ArraySchema* array_schema = query_->array()->array_schema();
  const double* subarray = (const double*)subarray_;
  const double* domain = (const double*)array_schema->domain();
  const double* tile_extents = (const double*)array_schema->tile_extents();
  double* tile_slab[2];
  for (int i = 0; i < 2; ++i)
    tile_slab[i] = (double*)tile_slab_[i];
  int prev_id = (copy_id_ + 1) % 2;

  // Check again if done, this time based on the tile slab and subarray
  if (tile_slab_init_[prev_id] && tile_slab[prev_id][2 * (dim_num_ - 1) + 1] ==
                                      subarray[2 * (dim_num_ - 1) + 1]) {
    read_tile_slabs_done_ = true;
    return false;
  }

  // If this is the first time this function is called, initialize
  if (!tile_slab_init_[prev_id]) {
    // Crop the subarray extent along the first axis to fit in the first tile
    tile_slab[copy_id_][2 * (dim_num_ - 1)] = subarray[2 * (dim_num_ - 1)];
    double upper = subarray[2 * (dim_num_ - 1)] + tile_extents[dim_num_ - 1];
    double cropped_upper =
        floor(
            (upper - domain[2 * (dim_num_ - 1)]) / tile_extents[dim_num_ - 1]) *
            tile_extents[dim_num_ - 1] +
        domain[2 * (dim_num_ - 1)];
    tile_slab[copy_id_][2 * (dim_num_ - 1) + 1] =
        MIN(cropped_upper - DBL_MIN, subarray[2 * (dim_num_ - 1) + 1]);

    // Leave the rest of the subarray extents intact
    for (int i = 0; i < dim_num_ - 1; ++i) {
      tile_slab[copy_id_][2 * i] = subarray[2 * i];
      tile_slab[copy_id_][2 * i + 1] = subarray[2 * i + 1];
    }
  } else {  // Calculate a new slab based on the previous
    // Copy previous tile slab
    memcpy(tile_slab[copy_id_], tile_slab[prev_id], 2 * coords_size_);

    // Advance tile slab
    tile_slab[copy_id_][2 * (dim_num_ - 1)] =
        tile_slab[copy_id_][2 * (dim_num_ - 1) + 1] + DBL_MIN;
    tile_slab[copy_id_][2 * (dim_num_ - 1) + 1] =
        MIN(tile_slab[copy_id_][2 * (dim_num_ - 1)] +
                tile_extents[dim_num_ - 1] - DBL_MIN,
            subarray[2 * (dim_num_ - 1) + 1]);
  }

  // Mark this tile slab as initialized
  tile_slab_init_[copy_id_] = true;

  // Success
  return true;
}

template <class T>
bool ArraySortedReadState::next_tile_slab_sparse_row() {
  // Quick check if done
  if (read_tile_slabs_done_)
    return false;

  // For easy reference
  const ArraySchema* array_schema = query_->array()->array_schema();
  const T* subarray = static_cast<const T*>(subarray_);
  const T* domain = static_cast<const T*>(array_schema->domain());
  const T* tile_extents = static_cast<const T*>(array_schema->tile_extents());
  T* tile_slab[2];
  for (int i = 0; i < 2; ++i)
    tile_slab[i] = static_cast<T*>(tile_slab_[i]);
  int prev_id = (copy_id_ + 1) % 2;

  // Check again if done, this time based on the tile slab and subarray
  if (tile_slab_init_[prev_id] && tile_slab[prev_id][1] == subarray[1]) {
    read_tile_slabs_done_ = true;
    return false;
  }

  // If this is the first time this function is called, initialize
  if (!tile_slab_init_[prev_id]) {
    // Crop the subarray extent along the first axis to fit in the first tile
    tile_slab[copy_id_][0] = subarray[0];
    T upper = subarray[0] + tile_extents[0];
    T cropped_upper =
        (upper - domain[0]) / tile_extents[0] * tile_extents[0] + domain[0];
    tile_slab[copy_id_][1] = MIN(cropped_upper - 1, subarray[1]);

    // Leave the rest of the subarray extents intact
    for (int i = 1; i < dim_num_; ++i) {
      tile_slab[copy_id_][2 * i] = subarray[2 * i];
      tile_slab[copy_id_][2 * i + 1] = subarray[2 * i + 1];
    }
  } else {  // Calculate a new slab based on the previous
    // Copy previous tile slab
    memcpy(tile_slab[copy_id_], tile_slab[prev_id], 2 * coords_size_);

    // Advance tile slab
    tile_slab[copy_id_][0] = tile_slab[copy_id_][1] + 1;
    tile_slab[copy_id_][1] =
        MIN(tile_slab[copy_id_][0] + tile_extents[0] - 1, subarray[1]);
  }

  // Mark this tile slab as initialized
  tile_slab_init_[copy_id_] = true;

  // Success
  return true;
}

template <>
bool ArraySortedReadState::next_tile_slab_sparse_row<float>() {
  // Quick check if done
  if (read_tile_slabs_done_)
    return false;

  // For easy reference
  const ArraySchema* array_schema = query_->array()->array_schema();
  const float* subarray = (const float*)subarray_;
  const float* domain = (const float*)array_schema->domain();
  const float* tile_extents = (const float*)array_schema->tile_extents();
  float* tile_slab[2];
  for (int i = 0; i < 2; ++i)
    tile_slab[i] = (float*)tile_slab_[i];
  int prev_id = (copy_id_ + 1) % 2;

  // Check again if done, this time based on the tile slab and subarray
  if (tile_slab_init_[prev_id] && tile_slab[prev_id][1] == subarray[1]) {
    read_tile_slabs_done_ = true;
    return false;
  }

  // If this is the first time this function is called, initialize
  if (!tile_slab_init_[prev_id]) {
    // Crop the subarray extent along the first axis to fit in the first tile
    tile_slab[copy_id_][0] = subarray[0];
    float upper = subarray[0] + tile_extents[0];
    float cropped_upper =
        floor((upper - domain[0]) / tile_extents[0]) * tile_extents[0] +
        domain[0];
    tile_slab[copy_id_][1] = MIN(cropped_upper - FLT_MIN, subarray[1]);

    // Leave the rest of the subarray extents intact
    for (int i = 1; i < dim_num_; ++i) {
      tile_slab[copy_id_][2 * i] = subarray[2 * i];
      tile_slab[copy_id_][2 * i + 1] = subarray[2 * i + 1];
    }
  } else {  // Calculate a new slab based on the previous
    // Copy previous tile slab
    memcpy(tile_slab[copy_id_], tile_slab[prev_id], 2 * coords_size_);

    // Advance tile slab
    tile_slab[copy_id_][0] = tile_slab[copy_id_][1] + FLT_MIN;
    tile_slab[copy_id_][1] =
        MIN(tile_slab[copy_id_][0] + tile_extents[0] - FLT_MIN, subarray[1]);
  }

  // Mark this tile slab as initialized
  tile_slab_init_[copy_id_] = true;

  // Success
  return true;
}

template <>
bool ArraySortedReadState::next_tile_slab_sparse_row<double>() {
  // Quick check if done
  if (read_tile_slabs_done_)
    return false;

  // For easy reference
  const ArraySchema* array_schema = query_->array()->array_schema();
  const double* subarray = (const double*)subarray_;
  const double* domain = (const double*)array_schema->domain();
  const double* tile_extents = (const double*)array_schema->tile_extents();
  double* tile_slab[2];
  for (int i = 0; i < 2; ++i)
    tile_slab[i] = (double*)tile_slab_[i];
  int prev_id = (copy_id_ + 1) % 2;

  // Check again if done, this time based on the tile slab and subarray
  if (tile_slab_init_[prev_id] && tile_slab[prev_id][1] == subarray[1]) {
    read_tile_slabs_done_ = true;
    return false;
  }

  // If this is the first time this function is called, initialize
  if (!tile_slab_init_[prev_id]) {
    // Crop the subarray extent along the first axis to fit in the first tile
    tile_slab[copy_id_][0] = subarray[0];
    double upper = subarray[0] + tile_extents[0];
    double cropped_upper =
        floor((upper - domain[0]) / tile_extents[0]) * tile_extents[0] +
        domain[0];
    tile_slab[copy_id_][1] = MIN(cropped_upper - DBL_MIN, subarray[1]);

    // Leave the rest of the subarray extents intact
    for (int i = 1; i < dim_num_; ++i) {
      tile_slab[copy_id_][2 * i] = subarray[2 * i];
      tile_slab[copy_id_][2 * i + 1] = subarray[2 * i + 1];
    }
  } else {  // Calculate a new slab based on the previous
    // Copy previous tile slab
    memcpy(tile_slab[copy_id_], tile_slab[prev_id], 2 * coords_size_);

    // Advance tile slab
    tile_slab[copy_id_][0] = tile_slab[copy_id_][1] + DBL_MIN;
    tile_slab[copy_id_][1] =
        MIN(tile_slab[copy_id_][0] + tile_extents[0] - DBL_MIN, subarray[1]);
  }

  // Mark this tile slab as initialized
  tile_slab_init_[copy_id_] = true;

  // Success
  return true;
}

template <class T>
Status ArraySortedReadState::read() {
  // For easy reference
  const ArraySchema* array_schema = query_->array()->array_schema();
  QueryMode mode = query_->mode();

  if (mode == QueryMode::READ_SORTED_COL) {
    if (array_schema->dense())
      return read_dense_sorted_col<T>();
    else
      return read_sparse_sorted_col<T>();
  } else if (mode == QueryMode::READ_SORTED_ROW) {
    if (array_schema->dense())
      return read_dense_sorted_row<T>();
    else
      return read_sparse_sorted_row<T>();
  } else {
    assert(0);  // The code should never reach here
  }

  // Code should never reach here
  return LOG_STATUS(Status::ASRSError("Invalid array mode when reading"));
}

template <class T>
Status ArraySortedReadState::read_dense_sorted_col() {
  // For easy reference
  const ArraySchema* array_schema = query_->array()->array_schema();
  const T* subarray = static_cast<const T*>(subarray_);

  // Check if this can be satisfied with a default read
  if (array_schema->cell_order() == Layout::COL_MAJOR &&
      array_schema->is_contained_in_tile_slab_row<T>(subarray))
    return query_->array()->read_default(
        query_, copy_state_.buffers_, copy_state_.buffer_sizes_);

  // These gotos SIGNIFICANTLY simplify the resume from overflow logic
  if (resume_copy_)
    goto copy_label_1;
  if (resume_copy_2_)
    goto copy_label_2;

  // First AIO
  if (next_tile_slab_dense_col<T>()) {
    reset_buffer_sizes_tmp(copy_id_);
    wait_aio_[copy_id_] = true;
    reset_aio_overflow(copy_id_);
    RETURN_NOT_OK(send_aio_request(copy_id_));
    copy_id_ = (copy_id_ + 1) % 2;
  }

  // Iterate over tile slabs
  while (next_tile_slab_dense_col<T>()) {
    // Submit AIO
    reset_buffer_sizes_tmp(copy_id_);
    wait_aio_[copy_id_] = true;
    reset_aio_overflow(copy_id_);
    RETURN_NOT_OK(send_aio_request(copy_id_));
    copy_id_ = (copy_id_ + 1) % 2;

    aio_wait(copy_id_);

    // Copy tile slab
    if (copy_tile_slab_done())
      reset_tile_slab_state<T>();

  copy_label_1:  // Resume from the point the copy led to overflow
    resume_copy_ = false;
    copy_tile_slab_dense();

    if (overflow()) {
      resume_copy_ = true;
      break;
    }
  }

  if (!resume_copy_) {
    copy_id_ = (copy_id_ + 1) % 2;
    aio_wait(copy_id_);

    if (copy_tile_slab_done())
      reset_tile_slab_state<T>();

  copy_label_2:  // Resume from the point the copy led to overflow
    resume_copy_2_ = false;
    copy_tile_slab_dense();

    if (overflow())
      resume_copy_2_ = true;
  }

  // Assign the true buffer sizes
  for (int i = 0; i < buffer_num_; ++i)
    copy_state_.buffer_sizes_[i] = copy_state_.buffer_offsets_[i];

  return Status::Ok();
}

template <class T>
Status ArraySortedReadState::read_dense_sorted_row() {
  // For easy reference
  const ArraySchema* array_schema = query_->array()->array_schema();
  const T* subarray = static_cast<const T*>(subarray_);

  // Check if this can be satisfied with a default read
  if (array_schema->cell_order() == Layout::ROW_MAJOR &&
      array_schema->is_contained_in_tile_slab_col<T>(subarray))
    return query_->array()->read_default(
        query_, copy_state_.buffers_, copy_state_.buffer_sizes_);

  // These gotos SIGNIFICANTLY simplify the resume from overflow logic
  if (resume_copy_)
    goto copy_label_1;
  if (resume_copy_2_)
    goto copy_label_2;

  // First AIO
  if (next_tile_slab_dense_row<T>()) {
    reset_buffer_sizes_tmp(copy_id_);
    wait_aio_[copy_id_] = true;
    reset_aio_overflow(copy_id_);
    RETURN_NOT_OK(send_aio_request(copy_id_));
    copy_id_ = (copy_id_ + 1) % 2;
  }

  // Iterate over each tile slab
  while (next_tile_slab_dense_row<T>()) {
    // Submit AIO
    reset_buffer_sizes_tmp(copy_id_);
    wait_aio_[copy_id_] = true;
    reset_aio_overflow(copy_id_);
    RETURN_NOT_OK(send_aio_request(copy_id_));
    copy_id_ = (copy_id_ + 1) % 2;

    aio_wait(copy_id_);

    // Copy tile slab
    if (copy_tile_slab_done())
      reset_tile_slab_state<T>();

  copy_label_1:  // Resume from the point the copy led to overflow
    resume_copy_ = false;
    copy_tile_slab_dense();

    // Handle overflow here
    if (overflow()) {
      resume_copy_ = true;
      break;
    }
  }

  if (!resume_copy_) {
    copy_id_ = (copy_id_ + 1) % 2;
    aio_wait(copy_id_);
    if (copy_tile_slab_done())
      reset_tile_slab_state<T>();

  copy_label_2:  // Resume from the point the copy led to overflow
    resume_copy_2_ = false;
    copy_tile_slab_dense();

    if (overflow())
      resume_copy_2_ = true;
  }

  // Assign the true buffer sizes
  for (int i = 0; i < buffer_num_; ++i)
    copy_state_.buffer_sizes_[i] = copy_state_.buffer_offsets_[i];

  return Status::Ok();
}

template <class T>
Status ArraySortedReadState::read_sparse_sorted_col() {
  // For easy reference
  const ArraySchema* array_schema = query_->array()->array_schema();
  const T* subarray = static_cast<const T*>(subarray_);

  // Check if this can be satisfied with a default read
  if (array_schema->cell_order() == Layout::COL_MAJOR &&
      array_schema->is_contained_in_tile_slab_row<T>(subarray))
    return query_->array()->read_default(
        query_, copy_state_.buffers_, copy_state_.buffer_sizes_);

  // These gotos SIGNIFICANTLY simplify the resume from overflow logic
  if (resume_copy_)
    goto copy_label_1;
  if (resume_copy_2_)
    goto copy_label_2;

  // First AIO
  if (next_tile_slab_sparse_col<T>()) {
    reset_buffer_sizes_tmp(copy_id_);
    wait_aio_[copy_id_] = true;
    reset_aio_overflow(copy_id_);
    RETURN_NOT_OK(send_aio_request(copy_id_));
    copy_id_ = (copy_id_ + 1) % 2;
  }

  // Iterate over tile slabs
  while (next_tile_slab_sparse_col<T>()) {
    // Submit AIO
    reset_buffer_sizes_tmp(copy_id_);
    wait_aio_[copy_id_] = true;
    reset_aio_overflow(copy_id_);
    RETURN_NOT_OK(send_aio_request(copy_id_));
    copy_id_ = (copy_id_ + 1) % 2;

    aio_wait(copy_id_);

    // Copy tile slab
    if (copy_tile_slab_done()) {
      reset_tile_slab_state<T>();
      sort_cell_pos<T>();
    }

  copy_label_1:  // Resume from the point the copy led to overflow
    resume_copy_ = false;
    copy_tile_slab_sparse();

    // Handle overflow here
    if (overflow()) {
      resume_copy_ = true;
      break;
    }
  }

  if (!resume_copy_) {
    copy_id_ = (copy_id_ + 1) % 2;
    aio_wait(copy_id_);
    if (copy_tile_slab_done()) {
      reset_tile_slab_state<T>();
      sort_cell_pos<T>();
    }

  copy_label_2:  // Resume from the point the copy led to overflow
    resume_copy_2_ = false;
    copy_tile_slab_sparse();

    if (overflow())
      resume_copy_2_ = true;
  }

  // Assign the true buffer sizes
  for (int i = 0; i < buffer_num_; ++i)
    copy_state_.buffer_sizes_[i] = copy_state_.buffer_offsets_[i];

  return Status::Ok();
}

template <class T>
Status ArraySortedReadState::read_sparse_sorted_row() {
  // For easy reference
  const ArraySchema* array_schema = query_->array()->array_schema();
  const T* subarray = static_cast<const T*>(subarray_);

  // Check if this can be satisfied with a default read
  if (array_schema->cell_order() == Layout::ROW_MAJOR &&
      array_schema->is_contained_in_tile_slab_col<T>(subarray))
    return query_->array()->read_default(
        query_, copy_state_.buffers_, copy_state_.buffer_sizes_);

  // These gotos SIGNIFICANTLY simplify the resume from overflow logic
  if (resume_copy_)
    goto copy_label_1;
  if (resume_copy_2_)
    goto copy_label_2;

  // First AIO
  if (next_tile_slab_sparse_row<T>()) {
    reset_buffer_sizes_tmp(copy_id_);
    wait_aio_[copy_id_] = true;
    reset_aio_overflow(copy_id_);
    RETURN_NOT_OK(send_aio_request(copy_id_));
    copy_id_ = (copy_id_ + 1) % 2;
  }

  // Iterate over tile slabs
  while (next_tile_slab_sparse_row<T>()) {
    // Submit AIO
    reset_buffer_sizes_tmp(copy_id_);
    wait_aio_[copy_id_] = true;
    reset_aio_overflow(copy_id_);
    RETURN_NOT_OK(send_aio_request(copy_id_));
    copy_id_ = (copy_id_ + 1) % 2;

    aio_wait(copy_id_);

    // Copy tile slab
    if (copy_tile_slab_done()) {
      reset_tile_slab_state<T>();
      sort_cell_pos<T>();
    }

  copy_label_1:  // Resume from the point the copy led to overflow
    resume_copy_ = false;

    copy_tile_slab_sparse();

    // Handle overflow here
    if (overflow()) {
      resume_copy_ = true;
      break;
    }
  }

  if (!resume_copy_) {
    copy_id_ = (copy_id_ + 1) % 2;
    aio_wait(copy_id_);
    if (copy_tile_slab_done()) {
      reset_tile_slab_state<T>();
      sort_cell_pos<T>();
    }

  copy_label_2:  // Resume from the point the copy led to overflow
    resume_copy_2_ = false;

    copy_tile_slab_sparse();

    if (overflow())
      resume_copy_2_ = true;
  }

  // Assign the true buffer sizes
  for (int i = 0; i < buffer_num_; ++i)
    copy_state_.buffer_sizes_[i] = copy_state_.buffer_offsets_[i];

  return Status::Ok();
}

void ArraySortedReadState::reset_aio_overflow(int id) {
  // For easy reference
  int anum = (int)attribute_ids_.size();

  // Reset aio_overflow_
  for (int i = 0; i < anum; ++i)
    aio_overflow_[id][i] = false;
}

void ArraySortedReadState::reset_buffer_sizes_tmp(int id) {
  for (int i = 0; i < buffer_num_; ++i)
    buffer_sizes_tmp_[id][i] = buffer_sizes_[id][i];
}

void ArraySortedReadState::reset_copy_state(
    void** buffers, size_t* buffer_sizes) {
  copy_state_.buffers_ = buffers;
  copy_state_.buffer_sizes_ = buffer_sizes;
  for (int i = 0; i < buffer_num_; ++i)
    copy_state_.buffer_offsets_[i] = 0;
}

void ArraySortedReadState::reset_overflow() {
  for (int i = 0; i < (int)attribute_ids_.size(); ++i)
    overflow_[i] = false;
}

template <class T>
void ArraySortedReadState::reset_tile_coords() {
  T* tile_coords = (T*)tile_coords_;
  for (int i = 0; i < dim_num_; ++i)
    tile_coords[i] = 0;
}

template <class T>
void ArraySortedReadState::reset_tile_slab_state() {
  // For easy reference
  int anum = (int)attribute_ids_.size();
  bool dense = query_->array()->array_schema()->dense();

  // Both dense and sparse
  for (int i = 0; i < anum; ++i)
    tile_slab_state_.copy_tile_slab_done_[i] = false;

  if (dense) {  // DENSE
    T** current_coords = (T**)tile_slab_state_.current_coords_;
    const T* tile_slab = (const T*)tile_slab_norm_[copy_id_];

    // Reset values
    for (int i = 0; i < anum; ++i) {
      tile_slab_state_.current_offsets_[i] = 0;
      tile_slab_state_.current_tile_[i] = 0;
      for (int j = 0; j < dim_num_; ++j)
        current_coords[i][j] = tile_slab[2 * j];
    }
  } else {  // SPARSE
    for (int i = 0; i < anum; ++i)
      tile_slab_state_.current_cell_pos_[i] = 0;
  }
}

Status ArraySortedReadState::send_aio_request(int id) {
  // For easy reference
  StorageManager* storage_manager = query_->array()->storage_manager();

  // Sanity check
  assert(storage_manager != NULL);

  if (aio_query_[id] != nullptr)
    delete aio_query_[id];
  aio_query_[id] =
      new Query(query_, aio_request_[id].mode(), aio_request_[id].subarray());

  aio_request_[id].set_query(aio_query_[id]);

  // Send the AIO request to the clone array
  RETURN_NOT_OK(storage_manager->aio_submit(&(aio_request_[id]), 1));

  // Success
  return Status::Ok();
}

template <class T>
void ArraySortedReadState::sort_cell_pos() {
  // For easy reference
  const ArraySchema* array_schema = query_->array()->array_schema();
  int dim_num = array_schema->dim_num();
  uint64_t cell_num = buffer_sizes_tmp_[copy_id_][coords_buf_i_] / coords_size_;
  QueryMode mode = query_->mode();
  auto buffer = static_cast<const T*>(buffers_[copy_id_][coords_buf_i_]);

  // Populate cell_pos
  cell_pos_.resize(cell_num);
  for (int i = 0; i < cell_num; ++i)
    cell_pos_[i] = i;

  // Invoke the proper sort function, based on the mode
  if (mode == QueryMode::READ_SORTED_ROW) {
    // Sort cell positions
    std::sort(
        cell_pos_.begin(), cell_pos_.end(), SmallerRow<T>(buffer, dim_num));
  } else {  // mode == TILEDB_ARRAY_READ_SORTED_COL
    // Sort cell positions
    std::sort(
        cell_pos_.begin(), cell_pos_.end(), SmallerCol<T>(buffer, dim_num));
  }
}

template <class T>
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
  current_offset = tile_slab_info_[copy_id_].start_offsets_[aid][tid] +
                   cid * attribute_sizes_[aid];
}

// Explicit template instantiations

template Status ArraySortedReadState::read_dense_sorted_col<int>();
template Status ArraySortedReadState::read_dense_sorted_col<int64_t>();
template Status ArraySortedReadState::read_dense_sorted_col<float>();
template Status ArraySortedReadState::read_dense_sorted_col<double>();
template Status ArraySortedReadState::read_dense_sorted_col<int8_t>();
template Status ArraySortedReadState::read_dense_sorted_col<uint8_t>();
template Status ArraySortedReadState::read_dense_sorted_col<int16_t>();
template Status ArraySortedReadState::read_dense_sorted_col<uint16_t>();
template Status ArraySortedReadState::read_dense_sorted_col<uint32_t>();
template Status ArraySortedReadState::read_dense_sorted_col<uint64_t>();

template Status ArraySortedReadState::read_dense_sorted_row<int>();
template Status ArraySortedReadState::read_dense_sorted_row<int64_t>();
template Status ArraySortedReadState::read_dense_sorted_row<float>();
template Status ArraySortedReadState::read_dense_sorted_row<double>();
template Status ArraySortedReadState::read_dense_sorted_row<int8_t>();
template Status ArraySortedReadState::read_dense_sorted_row<uint8_t>();
template Status ArraySortedReadState::read_dense_sorted_row<int16_t>();
template Status ArraySortedReadState::read_dense_sorted_row<uint16_t>();
template Status ArraySortedReadState::read_dense_sorted_row<uint32_t>();
template Status ArraySortedReadState::read_dense_sorted_row<uint64_t>();

};  // namespace tiledb
