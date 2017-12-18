/**
 * @file   array_ordered_read_state.cc
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
 * This file implements the ArrayOrderedReadState class.
 */

#include <algorithm>
#include <cassert>
#include <cfloat>
#include <cmath>

#include "array_ordered_read_state.h"
#include "comparators.h"
#include "logger.h"
#include "utils.h"

/* ****************************** */
/*             MACROS             */
/* ****************************** */

#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))

namespace tiledb {

/* ****************************** */
/*        STATIC CONSTANTS        */
/* ****************************** */

const uint64_t ArrayOrderedReadState::INVALID_UINT64 = UINT64_MAX;

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

ArrayOrderedReadState::ArrayOrderedReadState(Query* query)
    : query_(query) {
  // Calculate the attribute ids
  calculate_attribute_ids();

  // For easy reference
  const ArrayMetadata* array_metadata = query->array_metadata();
  auto anum = (unsigned int)attribute_ids_.size();

  // Initializations
  coords_size_ = array_metadata->coords_size();
  copy_id_ = 0;
  dim_num_ = array_metadata->dim_num();
  read_tile_slabs_done_ = false;
  resume_copy_ = false;
  resume_copy_2_ = false;
  tile_coords_ = nullptr;
  tile_domain_ = nullptr;
  for (unsigned int i = 0; i < 2; ++i) {
    async_query_[i] = nullptr;
    buffer_sizes_[i] = nullptr;
    buffer_sizes_tmp_[i] = nullptr;
    buffer_sizes_tmp_bak_[i] = nullptr;
    buffers_[i] = nullptr;
    tile_slab_[i] = std::malloc(2 * coords_size_);
    tile_slab_norm_[i] = std::malloc(2 * coords_size_);
    tile_slab_init_[i] = false;
    async_wait_[i] = true;
  }
  overflow_ = new bool[anum];
  overflow_still_ = new bool[anum];
  for (unsigned int i = 0; i < anum; ++i) {
    overflow_[i] = false;
    overflow_still_[i] = true;
    if (array_metadata->var_size(attribute_ids_[i]))
      attribute_sizes_.push_back(sizeof(uint64_t));
    else
      attribute_sizes_.push_back(array_metadata->cell_size(attribute_ids_[i]));
  }

  subarray_ = std::malloc(2 * coords_size_);
  std::memcpy(subarray_, query_->subarray(), 2 * coords_size_);

  // Calculate number of buffers
  calculate_buffer_num();

  // Calculate buffer sizes
  calculate_buffer_sizes();

  // Initialize tile slab info and state, and copy state
  init_tile_slab_info();
  init_tile_slab_state();
  init_copy_state();
}

ArrayOrderedReadState::~ArrayOrderedReadState() {
  // Clean up
  if (subarray_ != nullptr)
    std::free(subarray_);
  if (tile_coords_ != nullptr)
    std::free(tile_coords_);
  if (tile_domain_ != nullptr)
    std::free(tile_domain_);
  delete[] overflow_;

  for (unsigned int i = 0; i < 2; ++i) {
    if (async_query_[i] != nullptr)
      async_query_[i]->finalize();
    delete async_query_[i];

    if (buffer_sizes_[i] != nullptr)
      delete[] buffer_sizes_[i];
    if (buffer_sizes_tmp_[i] != nullptr)
      delete[] buffer_sizes_tmp_[i];
    if (buffer_sizes_tmp_bak_[i] != nullptr)
      delete[] buffer_sizes_tmp_bak_[i];
    if (buffers_[i] != nullptr) {
      for (unsigned int b = 0; b < buffer_num_; ++b)
        std::free(buffers_[i][b]);
      free(buffers_[i]);
    }

    std::free(tile_slab_[i]);
    std::free(tile_slab_norm_[i]);
  }

  // Free tile slab info and state, and copy state
  free_copy_state();
  free_tile_slab_state();
  free_tile_slab_info();
}

/* ****************************** */
/*           ACCESSORS            */
/* ****************************** */

bool ArrayOrderedReadState::copy_tile_slab_done() const {
  auto anum = (unsigned int)attribute_ids_.size();
  for (unsigned int i = 0; i < anum; ++i) {
    // Special case for sparse arrays with extra coordinates attribute
    if (i == coords_attr_i_ && extra_coords_)
      continue;

    // Check
    if (!tile_slab_state_.copy_tile_slab_done_[i])
      return false;
  }

  return true;
}

bool ArrayOrderedReadState::done() const {
  if (!read_tile_slabs_done_)
    return false;

  return copy_tile_slab_done();
}

Status ArrayOrderedReadState::finalize() {
  for (auto& aq : async_query_) {
    if (aq != nullptr)
      RETURN_NOT_OK(aq->finalize());
    delete aq;
    aq = nullptr;
  }

  return Status::Ok();
}

bool ArrayOrderedReadState::overflow() const {
  for (int i = 0; i < (int)attribute_ids_.size(); ++i) {
    if (overflow_[i])
      return true;
  }

  return false;
}

bool ArrayOrderedReadState::overflow(unsigned int attribute_id) const {
  auto anum = (unsigned int)attribute_ids_.size();
  for (unsigned int i = 0; i < anum; ++i) {
    if (attribute_ids_[i] == attribute_id)
      return overflow_[i];
  }

  return false;
}

Status ArrayOrderedReadState::read(void** buffers, uint64_t* buffer_sizes) {
  // Trivial case
  if (done()) {
    for (unsigned int i = 0; i < buffer_num_; ++i)
      buffer_sizes[i] = 0;
    return Status::Ok();
  }

  // Reset copy state and overflow
  reset_copy_state(buffers, buffer_sizes);
  reset_overflow();

  // Call the appropriate templated read
  Datatype type = query_->array_metadata()->coords_type();
  if (type == Datatype::INT32)
    return read<int>();
  if (type == Datatype::INT64)
    return read<int64_t>();
  if (type == Datatype::FLOAT32)
    return read<float>();
  if (type == Datatype::FLOAT64)
    return read<double>();
  if (type == Datatype::INT8)
    return read<int8_t>();
  if (type == Datatype::UINT8)
    return read<uint8_t>();
  if (type == Datatype::INT16)
    return read<int16_t>();
  if (type == Datatype::UINT16)
    return read<uint16_t>();
  if (type == Datatype::UINT32)
    return read<uint32_t>();
  if (type == Datatype::UINT64)
    return read<uint64_t>();

  // Code should never reach here
  assert(0);
  return LOG_STATUS(Status::ASRSError("Invalid datatype when reading"));
}

/* ****************************** */
/*            MUTATORS            */
/* ****************************** */

Status ArrayOrderedReadState::init() {
  // Create buffers
  RETURN_NOT_OK(create_buffers());

  for (unsigned int i = 0; i < 2; ++i)
    async_data_[i] = {i, 0, this};

  // Initialize functors
  auto array_metadata = query_->array_metadata();
  Layout query_layout = query_->layout();
  Layout cell_order = array_metadata->cell_order();
  Layout tile_order = array_metadata->tile_order();
  Datatype coords_type = array_metadata->coords_type();
  if (query_layout == Layout::ROW_MAJOR) {
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
  } else if (query_layout == Layout::COL_MAJOR) {
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
  } else {
    assert(0);
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
  } else if (tile_order == Layout::COL_MAJOR) {
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
  } else {
    assert(0);
  }

  return Status::Ok();
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

template <class T>
void* ArrayOrderedReadState::advance_cell_slab_col_s(void* data) {
  ArrayOrderedReadState* asrs = ((ASRS_Data*)data)->asrs_;
  unsigned int aid = ((ASRS_Data*)data)->id_;
  asrs->advance_cell_slab_col<T>(aid);
  return nullptr;
}

template <class T>
void* ArrayOrderedReadState::advance_cell_slab_row_s(void* data) {
  ArrayOrderedReadState* asrs = ((ASRS_Data*)data)->asrs_;
  unsigned int aid = ((ASRS_Data*)data)->id_;
  asrs->advance_cell_slab_row<T>(aid);
  return nullptr;
}

template <class T>
void ArrayOrderedReadState::advance_cell_slab_col(unsigned int aid) {
  // For easy reference
  uint64_t& tid = tile_slab_state_.current_tile_[aid];  // Tile id
  uint64_t cell_slab_num = tile_slab_info_[copy_id_].cell_slab_num_[tid];
  auto current_coords = (T*)tile_slab_state_.current_coords_[aid];
  auto tile_slab = (const T*)tile_slab_norm_[copy_id_];

  // Advance cell slab coordinates
  unsigned int d = 0;
  current_coords[d] += cell_slab_num;
  for (unsigned int i = 0; i < dim_num_ - 1; ++i) {
    int64_t dim_overflow = (current_coords[i] - tile_slab[2 * i]) /
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
void ArrayOrderedReadState::advance_cell_slab_row(unsigned int aid) {
  // For easy reference
  uint64_t& tid = tile_slab_state_.current_tile_[aid];  // Tile id
  uint64_t cell_slab_num = tile_slab_info_[copy_id_].cell_slab_num_[tid];
  auto current_coords = (T*)tile_slab_state_.current_coords_[aid];
  auto tile_slab = (const T*)tile_slab_norm_[copy_id_];

  // Advance cell slab coordinates
  unsigned int d = dim_num_ - 1;
  current_coords[d] += cell_slab_num;
  for (unsigned int i = d; i > 0; --i) {
    int64_t dim_overflow = (current_coords[i] - tile_slab[2 * i]) /
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

void* ArrayOrderedReadState::async_done(void* data) {
  // Retrieve data
  ArrayOrderedReadState* asrs = ((ASRS_Data*)data)->asrs_;
  unsigned int id = ((ASRS_Data*)data)->id_;
  auto query = asrs->query_;

  // For easy reference
  auto anum = (unsigned int)asrs->attribute_ids_.size();
  const ArrayMetadata* array_metadata = query->array_metadata();

  // Check for overflow
  bool overflow = false;
  for (unsigned int i = 0; i < anum; ++i) {
    if (asrs->overflow_still_[i] && query->overflow(i)) {
      overflow = true;
      break;
    }
  }

  // Handle overflow
  bool sparse = array_metadata->dense();
  if (overflow) {  // OVERFLOW
    // Update buffer sizes
    for (unsigned int i = 0, b = 0; i < anum; ++i) {
      if (!array_metadata->var_size(asrs->attribute_ids_[i])) {  // FIXED
        if (query->overflow(i)) {
          // Expand buffer
          utils::expand_buffer(
              asrs->buffers_[id][b], &(asrs->buffer_sizes_[id][b]));
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
        if (query->overflow(i)) {
          // Expand offset buffer only in the case of sparse arrays
          if (sparse)
            utils::expand_buffer(
                asrs->buffers_[id][b], &(asrs->buffer_sizes_[id][b]));
          // Re-assign the buffer size for the fixed-sized offsets
          asrs->buffer_sizes_tmp_[id][b] = asrs->buffer_sizes_[id][b];
          ++b;
          // Expand variable-length cell buffers for both dense and sparse
          utils::expand_buffer(
              asrs->buffers_[id][b], &(asrs->buffer_sizes_[id][b]));
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
    asrs->async_submit_query(id);
  } else {  // NO OVERFLOW
    // Restore backup temporary buffer sizes
    for (unsigned int b = 0; b < asrs->buffer_num_; ++b) {
      if (asrs->buffer_sizes_tmp_bak_[id][b] != 0)
        asrs->buffer_sizes_tmp_[id][b] = asrs->buffer_sizes_tmp_bak_[id][b];
    }

    // Manage the mutexes and conditions
    asrs->async_notify(id);
  }

  return nullptr;
}

void ArrayOrderedReadState::async_notify(unsigned int id) {
  {
    std::lock_guard<std::mutex> lk(async_mtx_[id]);
    async_wait_[id] = false;
  }
  async_cv_[id].notify_one();
}

Status ArrayOrderedReadState::async_submit_query(unsigned int id) {
  // For easy reference
  auto storage_manager = query_->storage_manager();

  // Sanity check
  assert(storage_manager != nullptr);

  // Prepare a new query to be submitted asynchronously
  if (async_query_[id] != nullptr)
    RETURN_NOT_OK(async_query_[id]->finalize());
  delete async_query_[id];
  async_query_[id] = new Query();
  RETURN_NOT_OK(async_query_[id]->init(
      query_->storage_manager(),
      query_->array_metadata(),
      query_->fragment_metadata(),
      QueryType::READ,
      Layout::GLOBAL_ORDER,
      tile_slab_[id],
      query_->attribute_ids(),
      buffers_[id],
      buffer_sizes_tmp_[id],
      true));
  async_query_[id]->set_callback(async_done, &(async_data_[id]));

  // Send the async query
  RETURN_NOT_OK(storage_manager->async_push_query(async_query_[id], 1));

  // Success
  return Status::Ok();
}

void ArrayOrderedReadState::async_wait(unsigned int id) {
  std::unique_lock<std::mutex> lk(async_mtx_[id]);
  async_cv_[id].wait(lk, [id, this] { return !async_wait_[id]; });
  lk.unlock();
}

void ArrayOrderedReadState::calculate_attribute_ids() {
  // Initialization
  attribute_ids_ = query_->attribute_ids();
  bool coords_found = false;

  // For ease reference
  auto array_metadata = query_->array_metadata();
  auto attribute_num = array_metadata->attribute_num();

  // No need to do anything else in case the array_metadata is dense
  if (array_metadata->dense())
    return;

  // Find the coordinates index
  auto anum = (unsigned int)attribute_ids_.size();
  for (unsigned int i = 0; i < anum; ++i) {
    if (attribute_ids_[i] == attribute_num) {
      coords_attr_i_ = i;
      coords_found = true;
      break;
    }
  }

  // If the coordinates index is not found, append coordinates attribute
  // to attribute ids.
  if (!coords_found) {
    attribute_ids_.push_back(attribute_num);
    coords_attr_i_ = (unsigned int)attribute_ids_.size() - 1;
    extra_coords_ = true;
  } else {  // No extra coordinates appended
    extra_coords_ = false;
  }
}

void ArrayOrderedReadState::calculate_buffer_num() {
  // For easy reference
  auto array_metadata = query_->array_metadata();
  auto attribute_num = array_metadata->attribute_num();

  // Calculate number of buffers
  buffer_num_ = 0;
  auto attribute_id_num = (unsigned int)attribute_ids_.size();
  for (unsigned int i = 0; i < attribute_id_num; ++i) {
    // Fix-sized attribute
    if (!array_metadata->var_size(attribute_ids_[i])) {
      if (attribute_ids_[i] == attribute_num)
        coords_buf_i_ = buffer_num_;  // Buffer that holds the coordinates
      ++buffer_num_;
    } else {  // Variable-sized attribute
      buffer_num_ += 2;
    }
  }
}

void ArrayOrderedReadState::calculate_buffer_sizes() {
  if (query_->array_metadata()->dense())
    calculate_buffer_sizes_dense();
  else
    calculate_buffer_sizes_sparse();
}

void ArrayOrderedReadState::calculate_buffer_sizes_dense() {
  // For easy reference
  auto array_metadata = query_->array_metadata();
  auto domain = array_metadata->domain();

  // Get cell number in a (full) tile slab
  uint64_t tile_slab_cell_num = 0;
  Layout query_layout = query_->layout();
  if (query_layout == Layout::ROW_MAJOR)
    tile_slab_cell_num = domain->tile_slab_row_cell_num(subarray_);
  else if (query_layout == Layout::COL_MAJOR)
    tile_slab_cell_num = domain->tile_slab_col_cell_num(subarray_);
  else
    assert(0);

  // Calculate buffer sizes
  auto attribute_id_num = (unsigned int)attribute_ids_.size();
  for (unsigned int j = 0; j < 2; ++j) {
    buffer_sizes_[j] = new uint64_t[buffer_num_];
    buffer_sizes_tmp_[j] = new uint64_t[buffer_num_];
    buffer_sizes_tmp_bak_[j] = new uint64_t[buffer_num_];
    for (unsigned int i = 0, b = 0; i < attribute_id_num; ++i) {
      // Fix-sized attribute
      if (!array_metadata->var_size(attribute_ids_[i])) {
        buffer_sizes_[j][b] =
            tile_slab_cell_num * array_metadata->cell_size(attribute_ids_[i]);
        buffer_sizes_tmp_bak_[j][b] = 0;
        ++b;
      } else {  // Variable-sized attribute
        buffer_sizes_[j][b] = tile_slab_cell_num * sizeof(uint64_t);
        buffer_sizes_tmp_bak_[j][b] = 0;
        ++b;
        buffer_sizes_[j][b] = 2 * tile_slab_cell_num * sizeof(uint64_t);
        buffer_sizes_tmp_bak_[j][b] = 0;
        ++b;
      }
    }
  }
}

void ArrayOrderedReadState::calculate_buffer_sizes_sparse() {
  // For easy reference
  const ArrayMetadata* array_metadata = query_->array_metadata();

  // Calculate buffer sizes
  auto attribute_id_num = (unsigned int)attribute_ids_.size();
  for (unsigned int j = 0; j < 2; ++j) {
    buffer_sizes_[j] = new uint64_t[buffer_num_];
    buffer_sizes_tmp_[j] = new uint64_t[buffer_num_];
    buffer_sizes_tmp_bak_[j] = new uint64_t[buffer_num_];
    for (unsigned int i = 0, b = 0; i < attribute_id_num; ++i) {
      // Fix-sized buffer
      buffer_sizes_[j][b] = constants::internal_buffer_size;
      buffer_sizes_tmp_bak_[j][b] = 0;
      ++b;
      if (array_metadata->var_size(
              attribute_ids_[i])) {  // Variable-sized buffer
        buffer_sizes_[j][b] = 2 * constants::internal_buffer_size;
        buffer_sizes_tmp_bak_[j][b] = 0;
        ++b;
      }
    }
  }
}

template <class T>
void* ArrayOrderedReadState::calculate_cell_slab_info_col_col_s(void* data) {
  ArrayOrderedReadState* asrs = ((ASRS_Data*)data)->asrs_;
  unsigned int id = ((ASRS_Data*)data)->id_;
  uint64_t tid = ((ASRS_Data*)data)->id_2_;
  asrs->calculate_cell_slab_info_col_col<T>(id, tid);
  return nullptr;
}

template <class T>
void* ArrayOrderedReadState::calculate_cell_slab_info_col_row_s(void* data) {
  ArrayOrderedReadState* asrs = ((ASRS_Data*)data)->asrs_;
  unsigned int id = ((ASRS_Data*)data)->id_;
  uint64_t tid = ((ASRS_Data*)data)->id_2_;
  asrs->calculate_cell_slab_info_col_row<T>(id, tid);
  return nullptr;
}

template <class T>
void* ArrayOrderedReadState::calculate_cell_slab_info_row_col_s(void* data) {
  ArrayOrderedReadState* asrs = ((ASRS_Data*)data)->asrs_;
  unsigned int id = ((ASRS_Data*)data)->id_;
  uint64_t tid = ((ASRS_Data*)data)->id_2_;
  asrs->calculate_cell_slab_info_row_col<T>(id, tid);
  return nullptr;
}

template <class T>
void* ArrayOrderedReadState::calculate_cell_slab_info_row_row_s(void* data) {
  ArrayOrderedReadState* asrs = ((ASRS_Data*)data)->asrs_;
  unsigned int id = ((ASRS_Data*)data)->id_;
  uint64_t tid = ((ASRS_Data*)data)->id_2_;
  asrs->calculate_cell_slab_info_row_row<T>(id, tid);
  return nullptr;
}

template <class T>
void ArrayOrderedReadState::calculate_cell_slab_info_col_col(
    unsigned int id, uint64_t tid) {
  // For easy reference
  auto anum = (unsigned int)attribute_ids_.size();
  auto range_overlap = (const T*)tile_slab_info_[id].range_overlap_[tid];
  auto tile_domain = (const T*)tile_domain_;

  // Calculate number of cells in cell slab
  uint64_t cell_num = range_overlap[1] - range_overlap[0] + 1;
  for (unsigned int i = 0; i < dim_num_ - 1; ++i) {
    uint64_t tile_num = tile_domain[2 * i + 1] - tile_domain[2 * i] + 1;
    if (tile_num == 1)
      cell_num *=
          range_overlap[2 * (i + 1) + 1] - range_overlap[2 * (i + 1)] + 1;
    else
      break;
  }
  tile_slab_info_[id].cell_slab_num_[tid] = cell_num;

  // Calculate size of a cell slab per attribute
  for (unsigned int aid = 0; aid < anum; ++aid)
    tile_slab_info_[id].cell_slab_size_[aid][tid] =
        tile_slab_info_[id].cell_slab_num_[tid] * attribute_sizes_[aid];

  // Calculate cell offset per dimension
  uint64_t cell_offset = 1;
  tile_slab_info_[id].cell_offset_per_dim_[tid][0] = cell_offset;
  for (unsigned int i = 1; i < dim_num_; ++i) {
    cell_offset *=
        (range_overlap[2 * (i - 1) + 1] - range_overlap[2 * (i - 1)] + 1);
    tile_slab_info_[id].cell_offset_per_dim_[tid][i] = cell_offset;
  }
}

template <class T>
void ArrayOrderedReadState::calculate_cell_slab_info_row_row(
    unsigned int id, uint64_t tid) {
  // For easy reference
  auto anum = (unsigned int)attribute_ids_.size();
  auto range_overlap = (const T*)tile_slab_info_[id].range_overlap_[tid];
  auto tile_domain = (const T*)tile_domain_;

  // Calculate number of cells in cell slab
  uint64_t cell_num = range_overlap[2 * (dim_num_ - 1) + 1] -
                      range_overlap[2 * (dim_num_ - 1)] + 1;
  for (unsigned int i = dim_num_ - 1; i > 0; --i) {
    uint64_t tile_num = tile_domain[2 * i + 1] - tile_domain[2 * i] + 1;
    if (tile_num == 1)
      cell_num *=
          range_overlap[2 * (i - 1) + 1] - range_overlap[2 * (i - 1)] + 1;
    else
      break;
  }
  tile_slab_info_[id].cell_slab_num_[tid] = cell_num;

  // Calculate size of a cell slab per attribute
  for (unsigned int aid = 0; aid < anum; ++aid)
    tile_slab_info_[id].cell_slab_size_[aid][tid] =
        tile_slab_info_[id].cell_slab_num_[tid] * attribute_sizes_[aid];

  // Calculate cell offset per dimension
  uint64_t cell_offset = 1;
  tile_slab_info_[id].cell_offset_per_dim_[tid][dim_num_ - 1] = cell_offset;
  if (dim_num_ > 1) {
    for (unsigned int i = dim_num_ - 2;; --i) {
      cell_offset *=
          (range_overlap[2 * (i + 1) + 1] - range_overlap[2 * (i + 1)] + 1);
      tile_slab_info_[id].cell_offset_per_dim_[tid][i] = cell_offset;

      if (i == 0)
        break;
    }
  }
}

template <class T>
void ArrayOrderedReadState::calculate_cell_slab_info_col_row(
    unsigned int id, uint64_t tid) {
  // For easy reference
  auto anum = (unsigned int)attribute_ids_.size();
  auto range_overlap = (const T*)tile_slab_info_[id].range_overlap_[tid];

  // Calculate number of cells in cell slab
  tile_slab_info_[id].cell_slab_num_[tid] = 1;

  // Calculate size of a cell slab per attribute
  for (unsigned int aid = 0; aid < anum; ++aid)
    tile_slab_info_[id].cell_slab_size_[aid][tid] =
        tile_slab_info_[id].cell_slab_num_[tid] * attribute_sizes_[aid];

  // Calculate cell offset per dimension
  uint64_t cell_offset = 1;
  tile_slab_info_[id].cell_offset_per_dim_[tid][dim_num_ - 1] = cell_offset;
  if (dim_num_ > 1) {
    for (unsigned int i = dim_num_ - 2;; --i) {
      cell_offset *=
          (range_overlap[2 * (i + 1) + 1] - range_overlap[2 * (i + 1)] + 1);
      tile_slab_info_[id].cell_offset_per_dim_[tid][i] = cell_offset;

      if (i == 0)
        break;
    }
  }
}

template <class T>
void ArrayOrderedReadState::calculate_cell_slab_info_row_col(
    unsigned int id, uint64_t tid) {
  // For easy reference
  auto anum = (unsigned int)attribute_ids_.size();
  auto range_overlap = (const T*)tile_slab_info_[id].range_overlap_[tid];

  // Calculate number of cells in cell slab
  tile_slab_info_[id].cell_slab_num_[tid] = 1;

  // Calculate size of a cell slab per attribute
  for (unsigned int aid = 0; aid < anum; ++aid)
    tile_slab_info_[id].cell_slab_size_[aid][tid] =
        tile_slab_info_[id].cell_slab_num_[tid] * attribute_sizes_[aid];

  // Calculate cell offset per dimension
  uint64_t cell_offset = 1;
  tile_slab_info_[id].cell_offset_per_dim_[tid][0] = cell_offset;
  for (unsigned int i = 1; i < dim_num_; ++i) {
    cell_offset *=
        (range_overlap[2 * (i - 1) + 1] - range_overlap[2 * (i - 1)] + 1);
    tile_slab_info_[id].cell_offset_per_dim_[tid][i] = cell_offset;
  }
}

template <class T>
void ArrayOrderedReadState::calculate_tile_domain(unsigned int id) {
  // Initializations
  tile_coords_ = std::malloc(coords_size_);
  tile_domain_ = std::malloc(2 * coords_size_);

  // For easy reference
  auto tile_slab = (const T*)tile_slab_norm_[id];
  auto tile_extents =
      (const T*)query_->array_metadata()->domain()->tile_extents();
  auto tile_coords = (T*)tile_coords_;
  auto tile_domain = (T*)tile_domain_;

  // Calculate tile domain and initial tile coordinates
  for (unsigned int i = 0; i < dim_num_; ++i) {
    tile_coords[i] = 0;
    tile_domain[2 * i] = tile_slab[2 * i] / tile_extents[i];
    tile_domain[2 * i + 1] = tile_slab[2 * i + 1] / tile_extents[i];
  }
}

template <class T>
void ArrayOrderedReadState::calculate_tile_slab_info(unsigned int id) {
  // Calculate number of tiles, if they are not already calculated
  if (tile_slab_info_[id].tile_num_ == INVALID_UINT64)
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
void* ArrayOrderedReadState::calculate_tile_slab_info_col(void* data) {
  ArrayOrderedReadState* asrs = ((ASRS_Data*)data)->asrs_;
  unsigned int id = ((ASRS_Data*)data)->id_;
  asrs->calculate_tile_slab_info_col<T>(id);
  return nullptr;
}

template <class T>
void ArrayOrderedReadState::calculate_tile_slab_info_col(unsigned int id) {
  // For easy reference
  auto tile_domain = (const T*)tile_domain_;
  auto tile_coords = (T*)tile_coords_;
  auto tile_extents =
      (const T*)query_->array_metadata()->domain()->tile_extents();
  auto range_overlap = (T**)tile_slab_info_[id].range_overlap_;
  auto tile_slab = (const T*)tile_slab_norm_[id];
  uint64_t total_cell_num = 0;
  auto anum = static_cast<unsigned int>(attribute_ids_.size());

  // Iterate over all tiles in the tile domain
  uint64_t tid = 0;  // Tile id
  while (tile_coords[dim_num_ - 1] <= tile_domain[2 * (dim_num_ - 1) + 1]) {
    // Calculate range overlap, number of cells in the tile
    uint64_t tile_cell_num = 1;
    for (unsigned int i = 0; i < dim_num_; ++i) {
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
    uint64_t tile_offset = 1;
    tile_slab_info_[id].tile_offset_per_dim_[0] = tile_offset;
    for (unsigned int i = 1; i < dim_num_; ++i) {
      tile_offset *=
          (tile_domain[2 * (i - 1) + 1] - tile_domain[2 * (i - 1)] + 1);
      tile_slab_info_[id].tile_offset_per_dim_[i] = tile_offset;
    }

    // Calculate cell slab info
    ASRS_Data asrs_data = {id, tid, this};
    (*calculate_cell_slab_info_)(&asrs_data);

    // Calculate start offsets
    for (unsigned int aid = 0; aid < anum; ++aid) {
      tile_slab_info_[id].start_offsets_[aid][tid] =
          total_cell_num * attribute_sizes_[aid];
    }
    total_cell_num += tile_cell_num;

    // Advance tile coordinates
    unsigned int d = 0;
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
void* ArrayOrderedReadState::calculate_tile_slab_info_row(void* data) {
  ArrayOrderedReadState* asrs = ((ASRS_Data*)data)->asrs_;
  unsigned int id = ((ASRS_Data*)data)->id_;
  asrs->calculate_tile_slab_info_row<T>(id);
  return nullptr;
}

template <class T>
void ArrayOrderedReadState::calculate_tile_slab_info_row(unsigned int id) {
  // For easy reference
  auto tile_domain = (const T*)tile_domain_;
  auto tile_coords = (T*)tile_coords_;
  auto tile_extents =
      (const T*)query_->array_metadata()->domain()->tile_extents();
  auto range_overlap = (T**)tile_slab_info_[id].range_overlap_;
  auto tile_slab = (const T*)tile_slab_norm_[id];
  uint64_t total_cell_num = 0;
  auto anum = (unsigned int)attribute_ids_.size();

  // Iterate over all tiles in the tile domain
  uint64_t tid = 0;  // Tile id
  while (tile_coords[0] <= tile_domain[1]) {
    // Calculate range overlap, number of cells in the tile
    uint64_t tile_cell_num = 1;
    for (unsigned int i = 0; i < dim_num_; ++i) {
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
    uint64_t tile_offset = 1;
    tile_slab_info_[id].tile_offset_per_dim_[dim_num_ - 1] = tile_offset;
    if (dim_num_ > 1) {
      for (int i = dim_num_ - 2; i >= 0; --i) {
        tile_offset *=
            (tile_domain[2 * (i + 1) + 1] - tile_domain[2 * (i + 1)] + 1);
        tile_slab_info_[id].tile_offset_per_dim_[i] = tile_offset;

        if (i == 0)
          break;
      }
    }

    // Calculate cell slab info
    ASRS_Data asrs_data = {id, tid, this};
    (*calculate_cell_slab_info_)(&asrs_data);

    // Calculate start offsets
    for (unsigned int aid = 0; aid < anum; ++aid) {
      tile_slab_info_[id].start_offsets_[aid][tid] =
          total_cell_num * attribute_sizes_[aid];
    }
    total_cell_num += tile_cell_num;

    // Advance tile coordinates
    unsigned int d = dim_num_ - 1;
    ++tile_coords[d];
    while (d > 0 && tile_coords[d] > tile_domain[2 * d + 1]) {
      tile_coords[d] = tile_domain[2 * d];
      ++tile_coords[--d];
    }

    // Advance tile id
    ++tid;
  }
}

void ArrayOrderedReadState::copy_tile_slab_dense() {
  // For easy reference
  auto array_metadata = query_->array_metadata();

  // Copy tile slab for each attribute separately
  for (unsigned int i = 0, b = 0; i < attribute_ids_.size(); ++i) {
    if (!array_metadata->var_size(attribute_ids_[i])) {
      copy_tile_slab_dense(i, b);
      ++b;
    } else {
      copy_tile_slab_dense_var(i, b);
      b += 2;
    }
  }
}

void ArrayOrderedReadState::copy_tile_slab_dense(
    unsigned int aid, unsigned int bid) {
  // Exit if copy is done for this attribute
  if (tile_slab_state_.copy_tile_slab_done_[aid]) {
    copy_state_.buffer_sizes_[bid] = 0;  // Nothing written
    return;
  }

  // For easy reference
  uint64_t& tid = tile_slab_state_.current_tile_[aid];
  uint64_t& buffer_offset = copy_state_.buffer_offsets_[bid];
  uint64_t buffer_size = copy_state_.buffer_sizes_[bid];
  auto buffer = (char*)copy_state_.buffers_[bid];
  auto local_buffer = (char*)buffers_[copy_id_][bid];
  ASRS_Data asrs_data = {aid, 0, this};

  // Iterate over the tile slab cells
  for (;;) {
    // For easy reference
    uint64_t cell_slab_size =
        tile_slab_info_[copy_id_].cell_slab_size_[aid][tid];
    uint64_t& local_buffer_offset = tile_slab_state_.current_offsets_[aid];

    // Handle overflow
    if (buffer_offset + cell_slab_size > buffer_size) {
      overflow_[aid] = true;
      break;
    }

    // Copy cell slab
    std::memcpy(
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
}

void ArrayOrderedReadState::copy_tile_slab_dense_var(
    unsigned int aid, unsigned int bid) {
  // Exit if copy is done for this attribute
  if (tile_slab_state_.copy_tile_slab_done_[aid]) {
    copy_state_.buffer_sizes_[bid] = 0;      // Nothing written
    copy_state_.buffer_sizes_[bid + 1] = 0;  // Nothing written
    return;
  }

  // For easy reference
  uint64_t& tid = tile_slab_state_.current_tile_[aid];
  uint64_t& buffer_offset = copy_state_.buffer_offsets_[bid];
  uint64_t& buffer_offset_var = copy_state_.buffer_offsets_[bid + 1];
  uint64_t buffer_size = copy_state_.buffer_sizes_[bid];
  uint64_t buffer_size_var = copy_state_.buffer_sizes_[bid + 1];
  auto buffer = (char*)copy_state_.buffers_[bid];
  auto buffer_var = (char*)copy_state_.buffers_[bid + 1];
  auto local_buffer_var = (char*)buffers_[copy_id_][bid + 1];
  uint64_t local_buffer_size = buffer_sizes_tmp_[copy_id_][bid];
  uint64_t local_buffer_var_size = buffer_sizes_tmp_[copy_id_][bid + 1];
  auto local_buffer_s = (uint64_t*)buffers_[copy_id_][bid];
  uint64_t cell_num_in_buffer = local_buffer_size / sizeof(uint64_t);
  uint64_t var_offset = buffer_offset_var;
  ASRS_Data asrs_data = {aid, 0, this};

  // For all overlapping tiles, in a round-robin fashion
  for (;;) {
    // For easy reference
    uint64_t cell_slab_size =
        tile_slab_info_[copy_id_].cell_slab_size_[aid][tid];
    uint64_t cell_num_in_slab = cell_slab_size / sizeof(uint64_t);
    uint64_t& local_buffer_offset = tile_slab_state_.current_offsets_[aid];

    // Handle overflow
    if (buffer_offset + cell_slab_size > buffer_size) {
      overflow_[aid] = true;
      break;
    }

    // Calculate variable cell slab size
    uint64_t cell_start = local_buffer_offset / sizeof(uint64_t);
    uint64_t cell_end = cell_start + cell_num_in_slab;
    uint64_t cell_slab_size_var =
        (cell_end == cell_num_in_buffer) ?
            local_buffer_var_size - local_buffer_s[cell_start] :
            local_buffer_s[cell_end] - local_buffer_s[cell_start];

    // Handle overflow for the the variable-length buffer
    if (buffer_offset_var + cell_slab_size_var > buffer_size_var) {
      overflow_[aid] = true;
      break;
    }

    // Copy fixed-sized offsets
    for (uint64_t i = cell_start; i < cell_end; ++i) {
      std::memcpy(buffer + buffer_offset, &var_offset, sizeof(uint64_t));
      buffer_offset += sizeof(uint64_t);
      var_offset += (i == cell_num_in_buffer - 1) ?
                        local_buffer_var_size - local_buffer_s[i] :
                        local_buffer_s[i + 1] - local_buffer_s[i];
    }

    // Copy variable-sized values
    std::memcpy(
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
}

void ArrayOrderedReadState::copy_tile_slab_sparse() {
  // For easy reference
  auto array_metadata = query_->array_metadata();

  // Copy tile slab for each attribute separately
  auto anum = (unsigned int)attribute_ids_.size();
  for (unsigned int i = 0, b = 0; i < anum; ++i) {
    if (!array_metadata->var_size(attribute_ids_[i])) {  // FIXED
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

void ArrayOrderedReadState::copy_tile_slab_sparse(
    unsigned int aid, unsigned int bid) {
  // Exit if copy is done for this attribute
  if (tile_slab_state_.copy_tile_slab_done_[aid]) {
    copy_state_.buffer_sizes_[bid] = 0;  // Nothing written
    return;
  }

  // For easy reference
  uint64_t cell_size = query_->array_metadata()->cell_size(attribute_ids_[aid]);
  uint64_t& buffer_offset = copy_state_.buffer_offsets_[bid];
  uint64_t buffer_size = copy_state_.buffer_sizes_[bid];
  auto buffer = (char*)copy_state_.buffers_[bid];
  auto local_buffer = (char*)buffers_[copy_id_][bid];
  uint64_t cell_num = buffer_sizes_tmp_[copy_id_][coords_buf_i_] / coords_size_;
  uint64_t& current_cell_pos = tile_slab_state_.current_cell_pos_[aid];

  // Iterate over the remaining tile slab cells in a sorted order
  for (; current_cell_pos < cell_num; ++current_cell_pos) {
    // Handle overflow
    if (buffer_offset + cell_size > buffer_size) {
      overflow_[aid] = true;
      break;
    }

    // Calculate new local buffer offset
    uint64_t local_buffer_offset = cell_pos_[current_cell_pos] * cell_size;

    // Copy cell slab
    std::memcpy(
        buffer + buffer_offset, local_buffer + local_buffer_offset, cell_size);

    // Update buffer offset
    buffer_offset += cell_size;
  }

  // Mark tile slab as done
  if (current_cell_pos == cell_num)
    tile_slab_state_.copy_tile_slab_done_[aid] = true;
}

void ArrayOrderedReadState::copy_tile_slab_sparse_var(
    unsigned int aid, unsigned int bid) {
  // Exit if copy is done for this attribute
  if (tile_slab_state_.copy_tile_slab_done_[aid]) {
    copy_state_.buffer_sizes_[bid] = 0;      // Nothing written
    copy_state_.buffer_sizes_[bid + 1] = 0;  // Nothing written
    return;
  }

  // For easy reference
  uint64_t cell_size = sizeof(uint64_t);
  uint64_t& buffer_offset = copy_state_.buffer_offsets_[bid];
  uint64_t& buffer_offset_var = copy_state_.buffer_offsets_[bid + 1];
  uint64_t buffer_size = copy_state_.buffer_sizes_[bid];
  uint64_t buffer_size_var = copy_state_.buffer_sizes_[bid + 1];
  auto buffer = (char*)copy_state_.buffers_[bid];
  auto buffer_var = (char*)copy_state_.buffers_[bid + 1];
  auto local_buffer_var = (char*)buffers_[copy_id_][bid + 1];
  uint64_t local_buffer_var_size = buffer_sizes_tmp_[copy_id_][bid + 1];
  auto local_buffer_s = (uint64_t*)buffers_[copy_id_][bid];
  uint64_t cell_num = buffer_sizes_tmp_[copy_id_][coords_buf_i_] / coords_size_;
  uint64_t& current_cell_pos = tile_slab_state_.current_cell_pos_[aid];

  // Iterate over the remaining tile slab cells in a sorted order
  for (; current_cell_pos < cell_num; ++current_cell_pos) {
    // Handle overflow
    if (buffer_offset + cell_size > buffer_size) {
      overflow_[aid] = true;
      break;
    }

    // Calculate variable cell size
    uint64_t cell_start = cell_pos_[current_cell_pos];
    uint64_t cell_end = cell_start + 1;
    uint64_t cell_size_var =
        (cell_end == cell_num) ?
            local_buffer_var_size - local_buffer_s[cell_start] :
            local_buffer_s[cell_end] - local_buffer_s[cell_start];

    // Handle overflow for the the variable-length buffer
    if (buffer_offset_var + cell_size_var > buffer_size_var) {
      overflow_[aid] = true;
      break;
    }

    // Copy fixed-sized offset
    std::memcpy(buffer + buffer_offset, &buffer_offset_var, sizeof(uint64_t));
    buffer_offset += sizeof(uint64_t);

    // Copy variable-sized values
    std::memcpy(
        buffer_var + buffer_offset_var,
        local_buffer_var + local_buffer_s[cell_start],
        cell_size_var);
    buffer_offset_var += cell_size_var;
  }

  // Mark tile slab as done
  if (current_cell_pos == cell_num)
    tile_slab_state_.copy_tile_slab_done_[aid] = true;
}

Status ArrayOrderedReadState::create_buffers() {
  for (unsigned int j = 0; j < 2; ++j) {
    buffers_[j] = (void**)std::malloc(buffer_num_ * sizeof(void*));
    if (buffers_[j] == nullptr) {
      return LOG_STATUS(Status::ASRSError("Cannot create local buffers"));
    }

    for (unsigned int b = 0; b < buffer_num_; ++b) {
      buffers_[j][b] = std::malloc(buffer_sizes_[j][b]);
      if (buffers_[j][b] == nullptr) {
        return LOG_STATUS(Status::ASRSError("Cannot allocate local buffer"));
      }
    }
  }
  return Status::Ok();
}

void ArrayOrderedReadState::free_copy_state() {
  delete[] copy_state_.buffer_offsets_;
}

void ArrayOrderedReadState::free_tile_slab_info() {
  // Do nothing in the case of sparse arrays
  if (!query_->array_metadata()->dense())
    return;

  // For easy reference
  auto anum = (unsigned int)attribute_ids_.size();

  // Free
  for (auto& info : tile_slab_info_) {
    uint64_t tile_num = info.tile_num_;

    if (info.cell_offset_per_dim_ != nullptr) {
      for (uint64_t tile = 0; tile < tile_num; ++tile)
        delete[] info.cell_offset_per_dim_[tile];
      delete[] info.cell_offset_per_dim_;
    }

    for (unsigned int attr = 0; attr < anum; ++attr) {
      if (info.cell_slab_size_[attr] != nullptr)
        delete[] info.cell_slab_size_[attr];
    }

    delete[] info.cell_slab_size_;
    delete[] info.cell_slab_num_;

    if (info.range_overlap_ != nullptr) {
      for (uint64_t tile = 0; tile < tile_num; ++tile)
        std::free(info.range_overlap_[tile]);
      delete[] info.range_overlap_;
    }

    for (unsigned int attr = 0; attr < anum; ++attr) {
      if (info.start_offsets_[attr] != nullptr)
        delete[] info.start_offsets_[attr];
    }
    delete[] info.start_offsets_;

    delete[] info.tile_offset_per_dim_;
  }
}

void ArrayOrderedReadState::free_tile_slab_state() {
  // For easy reference
  auto anum = (unsigned int)attribute_ids_.size();

  // Clean up
  if (tile_slab_state_.current_coords_ != nullptr) {
    for (unsigned int i = 0; i < anum; ++i)
      std::free(tile_slab_state_.current_coords_[i]);
    delete[] tile_slab_state_.current_coords_;
  }

  delete[] tile_slab_state_.copy_tile_slab_done_;
  delete[] tile_slab_state_.current_offsets_;
  delete[] tile_slab_state_.current_tile_;
  delete[] tile_slab_state_.current_cell_pos_;
}

template <class T>
uint64_t ArrayOrderedReadState::get_cell_id(unsigned int aid) {
  // For easy reference
  auto current_coords = (const T*)tile_slab_state_.current_coords_[aid];
  uint64_t tid = tile_slab_state_.current_tile_[aid];
  auto range_overlap = (const T*)tile_slab_info_[copy_id_].range_overlap_[tid];
  uint64_t* cell_offset_per_dim =
      tile_slab_info_[copy_id_].cell_offset_per_dim_[tid];

  // Calculate cell id
  uint64_t cid = 0;
  for (unsigned int i = 0; i < dim_num_; ++i)
    cid += (current_coords[i] - range_overlap[2 * i]) * cell_offset_per_dim[i];

  // Return tile id
  return cid;
}

template <class T>
uint64_t ArrayOrderedReadState::get_tile_id(unsigned int aid) {
  // For easy reference
  auto current_coords = (const T*)tile_slab_state_.current_coords_[aid];
  auto tile_extents =
      (const T*)query_->array_metadata()->domain()->tile_extents();
  uint64_t* tile_offset_per_dim =
      tile_slab_info_[copy_id_].tile_offset_per_dim_;

  // Calculate tile id
  uint64_t tid = 0;
  for (unsigned int i = 0; i < dim_num_; ++i)
    tid += (current_coords[i] / tile_extents[i]) * tile_offset_per_dim[i];

  // Return tile id
  return tid;
}

void ArrayOrderedReadState::init_copy_state() {
  copy_state_.buffer_sizes_ = nullptr;
  copy_state_.buffers_ = nullptr;
  copy_state_.buffer_offsets_ = new uint64_t[buffer_num_];
  for (unsigned int i = 0; i < buffer_num_; ++i)
    copy_state_.buffer_offsets_[i] = 0;
}

void ArrayOrderedReadState::init_tile_slab_info() {
  // Do nothing in the case of sparse arrays
  if (!query_->array_metadata()->dense())
    return;

  // For easy reference
  auto anum = (unsigned int)attribute_ids_.size();

  // Initialize
  for (auto& info : tile_slab_info_) {
    info.cell_offset_per_dim_ = nullptr;
    info.cell_slab_size_ = new uint64_t*[anum];
    info.cell_slab_num_ = nullptr;
    info.range_overlap_ = nullptr;
    info.start_offsets_ = new uint64_t*[anum];
    info.tile_offset_per_dim_ = new uint64_t[dim_num_];

    for (unsigned int attr = 0; attr < anum; ++attr) {
      info.cell_slab_size_[attr] = nullptr;
      info.start_offsets_[attr] = nullptr;
    }

    info.tile_num_ = INVALID_UINT64;
  }
}

template <class T>
void ArrayOrderedReadState::init_tile_slab_info(unsigned int id) {
  // Sanity check
  assert(query_->array_metadata()->dense());

  // For easy reference
  auto anum = (unsigned int)attribute_ids_.size();

  // Calculate tile number
  uint64_t tile_num =
      query_->array_metadata()->domain()->tile_num(tile_slab_[id]);

  // Initializations
  tile_slab_info_[id].cell_offset_per_dim_ = new uint64_t*[tile_num];
  tile_slab_info_[id].cell_slab_num_ = new uint64_t[tile_num];
  tile_slab_info_[id].range_overlap_ = new void*[tile_num];
  for (uint64_t i = 0; i < tile_num; ++i) {
    tile_slab_info_[id].range_overlap_[i] = std::malloc(2 * coords_size_);
    tile_slab_info_[id].cell_offset_per_dim_[i] = new uint64_t[dim_num_];
  }

  for (unsigned int i = 0; i < anum; ++i) {
    tile_slab_info_[id].cell_slab_size_[i] = new uint64_t[tile_num];
    tile_slab_info_[id].start_offsets_[i] = new uint64_t[tile_num];
  }

  tile_slab_info_[id].tile_num_ = tile_num;
}

void ArrayOrderedReadState::init_tile_slab_state() {
  // For easy reference
  auto anum = (unsigned int)attribute_ids_.size();
  bool dense = query_->array_metadata()->dense();

  // Both for dense and sparse
  tile_slab_state_.copy_tile_slab_done_ = new bool[anum];
  for (unsigned int i = 0; i < anum; ++i)
    tile_slab_state_.copy_tile_slab_done_[i] = true;  // Important!

  // Allocations and initializations
  if (dense) {  // DENSE
    tile_slab_state_.current_offsets_ = new uint64_t[anum];
    tile_slab_state_.current_coords_ = new void*[anum];
    tile_slab_state_.current_tile_ = new uint64_t[anum];
    tile_slab_state_.current_cell_pos_ = nullptr;

    for (unsigned int i = 0; i < anum; ++i) {
      tile_slab_state_.current_coords_[i] = std::malloc(coords_size_);
      tile_slab_state_.current_offsets_[i] = 0;
      tile_slab_state_.current_tile_[i] = 0;
    }
  } else {  // SPARSE
    tile_slab_state_.current_offsets_ = nullptr;
    tile_slab_state_.current_coords_ = nullptr;
    tile_slab_state_.current_tile_ = nullptr;
    tile_slab_state_.current_cell_pos_ = new uint64_t[anum];

    for (unsigned int i = 0; i < anum; ++i)
      tile_slab_state_.current_cell_pos_[i] = 0;
  }
}

template <class T>
bool ArrayOrderedReadState::next_tile_slab_dense_col() {
  // Quick check if done
  if (read_tile_slabs_done_)
    return false;

  // For easy reference
  auto array_metadata = query_->array_metadata();
  auto subarray = static_cast<const T*>(subarray_);
  auto domain = static_cast<const T*>(array_metadata->domain()->domain());
  auto tile_extents =
      static_cast<const T*>(array_metadata->domain()->tile_extents());
  T* tile_slab[2];
  auto tile_slab_norm = static_cast<T*>(tile_slab_norm_[copy_id_]);
  for (unsigned int i = 0; i < 2; ++i)
    tile_slab[i] = static_cast<T*>(tile_slab_[i]);
  unsigned int prev_id = (copy_id_ + 1) % 2;
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
    for (unsigned int i = 0; i < dim_num_ - 1; ++i) {
      tile_slab[copy_id_][2 * i] = subarray[2 * i];
      tile_slab[copy_id_][2 * i + 1] = subarray[2 * i + 1];
    }
  } else {  // Calculate a new slab based on the previous
    // Copy previous tile slab
    std::memcpy(tile_slab[copy_id_], tile_slab[prev_id], 2 * coords_size_);

    // Advance tile slab
    tile_slab[copy_id_][2 * (dim_num_ - 1)] =
        tile_slab[copy_id_][2 * (dim_num_ - 1) + 1] + 1;
    tile_slab[copy_id_][2 * (dim_num_ - 1) + 1] =
        MIN(tile_slab[copy_id_][2 * (dim_num_ - 1)] +
                tile_extents[dim_num_ - 1] - 1,
            subarray[2 * (dim_num_ - 1) + 1]);
  }

  // Calculate normalized tile slab
  for (unsigned int i = 0; i < dim_num_; ++i) {
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
bool ArrayOrderedReadState::next_tile_slab_dense_row() {
  // Quick check if done
  if (read_tile_slabs_done_)
    return false;

  // For easy reference
  auto array_metadata = query_->array_metadata();
  auto subarray = static_cast<const T*>(subarray_);
  auto domain = static_cast<const T*>(array_metadata->domain()->domain());
  auto tile_extents =
      static_cast<const T*>(array_metadata->domain()->tile_extents());
  T* tile_slab[2];
  auto tile_slab_norm = static_cast<T*>(tile_slab_norm_[copy_id_]);
  for (unsigned int i = 0; i < 2; ++i)
    tile_slab[i] = static_cast<T*>(tile_slab_[i]);
  unsigned int prev_id = (copy_id_ + 1) % 2;
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
    for (unsigned int i = 1; i < dim_num_; ++i) {
      tile_slab[copy_id_][2 * i] = subarray[2 * i];
      tile_slab[copy_id_][2 * i + 1] = subarray[2 * i + 1];
    }
  } else {  // Calculate a new slab based on the previous
    // Copy previous tile slab
    std::memcpy(tile_slab[copy_id_], tile_slab[prev_id], 2 * coords_size_);

    // Advance tile slab
    tile_slab[copy_id_][0] = tile_slab[copy_id_][1] + 1;
    tile_slab[copy_id_][1] =
        MIN(tile_slab[copy_id_][0] + tile_extents[0] - 1, subarray[1]);
  }

  // Calculate normalized tile slab
  for (unsigned int i = 0; i < dim_num_; ++i) {
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
bool ArrayOrderedReadState::next_tile_slab_sparse_col() {
  // Quick check if done
  if (read_tile_slabs_done_)
    return false;

  // For easy reference
  auto array_metadata = query_->array_metadata();
  auto subarray = static_cast<const T*>(subarray_);
  auto domain = static_cast<const T*>(array_metadata->domain()->domain());
  auto tile_extents =
      static_cast<const T*>(array_metadata->domain()->tile_extents());
  T* tile_slab[2];
  for (unsigned int i = 0; i < 2; ++i)
    tile_slab[i] = static_cast<T*>(tile_slab_[i]);
  unsigned int prev_id = (copy_id_ + 1) % 2;

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
    for (unsigned int i = 0; i < dim_num_ - 1; ++i) {
      tile_slab[copy_id_][2 * i] = subarray[2 * i];
      tile_slab[copy_id_][2 * i + 1] = subarray[2 * i + 1];
    }
  } else {  // Calculate a new slab based on the previous
    // Copy previous tile slab
    std::memcpy(tile_slab[copy_id_], tile_slab[prev_id], 2 * coords_size_);

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
bool ArrayOrderedReadState::next_tile_slab_sparse_col<float>() {
  // Quick check if done
  if (read_tile_slabs_done_)
    return false;

  // For easy reference
  auto array_metadata = query_->array_metadata();
  auto subarray = (const float*)subarray_;
  auto domain = (const float*)array_metadata->domain()->domain();
  auto tile_extents = (const float*)array_metadata->domain()->tile_extents();
  float* tile_slab[2];
  for (unsigned int i = 0; i < 2; ++i)
    tile_slab[i] = (float*)tile_slab_[i];
  unsigned int prev_id = (copy_id_ + 1) % 2;

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
        (float)floor(
            (upper - domain[2 * (dim_num_ - 1)]) / tile_extents[dim_num_ - 1]) *
            tile_extents[dim_num_ - 1] +
        domain[2 * (dim_num_ - 1)];
    tile_slab[copy_id_][2 * (dim_num_ - 1) + 1] =
        MIN(cropped_upper - FLT_MIN, subarray[2 * (dim_num_ - 1) + 1]);

    // Leave the rest of the subarray extents intact
    for (unsigned int i = 0; i < dim_num_ - 1; ++i) {
      tile_slab[copy_id_][2 * i] = subarray[2 * i];
      tile_slab[copy_id_][2 * i + 1] = subarray[2 * i + 1];
    }
  } else {  // Calculate a new slab based on the previous
    // Copy previous tile slab
    std::memcpy(tile_slab[copy_id_], tile_slab[prev_id], 2 * coords_size_);

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
bool ArrayOrderedReadState::next_tile_slab_sparse_col<double>() {
  // Quick check if done
  if (read_tile_slabs_done_)
    return false;

  // For easy reference
  auto array_metadata = query_->array_metadata();
  auto subarray = (const double*)subarray_;
  auto domain = (const double*)array_metadata->domain()->domain();
  auto tile_extents = (const double*)array_metadata->domain()->tile_extents();
  double* tile_slab[2];
  for (unsigned int i = 0; i < 2; ++i)
    tile_slab[i] = (double*)tile_slab_[i];
  unsigned int prev_id = (copy_id_ + 1) % 2;

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
    for (unsigned int i = 0; i < dim_num_ - 1; ++i) {
      tile_slab[copy_id_][2 * i] = subarray[2 * i];
      tile_slab[copy_id_][2 * i + 1] = subarray[2 * i + 1];
    }
  } else {  // Calculate a new slab based on the previous
    // Copy previous tile slab
    std::memcpy(tile_slab[copy_id_], tile_slab[prev_id], 2 * coords_size_);

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
bool ArrayOrderedReadState::next_tile_slab_sparse_row() {
  // Quick check if done
  if (read_tile_slabs_done_)
    return false;

  // For easy reference
  auto array_metadata = query_->array_metadata();
  auto subarray = static_cast<const T*>(subarray_);
  auto domain = static_cast<const T*>(array_metadata->domain()->domain());
  auto tile_extents =
      static_cast<const T*>(array_metadata->domain()->tile_extents());
  T* tile_slab[2];
  for (unsigned int i = 0; i < 2; ++i)
    tile_slab[i] = static_cast<T*>(tile_slab_[i]);
  unsigned int prev_id = (copy_id_ + 1) % 2;

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
    for (unsigned int i = 1; i < dim_num_; ++i) {
      tile_slab[copy_id_][2 * i] = subarray[2 * i];
      tile_slab[copy_id_][2 * i + 1] = subarray[2 * i + 1];
    }
  } else {  // Calculate a new slab based on the previous
    // Copy previous tile slab
    std::memcpy(tile_slab[copy_id_], tile_slab[prev_id], 2 * coords_size_);

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
bool ArrayOrderedReadState::next_tile_slab_sparse_row<float>() {
  // Quick check if done
  if (read_tile_slabs_done_)
    return false;

  // For easy reference
  auto array_metadata = query_->array_metadata();
  auto subarray = (const float*)subarray_;
  auto domain = (const float*)array_metadata->domain()->domain();
  auto tile_extents = (const float*)array_metadata->domain()->tile_extents();
  float* tile_slab[2];
  for (unsigned int i = 0; i < 2; ++i)
    tile_slab[i] = (float*)tile_slab_[i];
  unsigned int prev_id = (copy_id_ + 1) % 2;

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
        (float)floor((upper - domain[0]) / tile_extents[0]) * tile_extents[0] +
        domain[0];
    tile_slab[copy_id_][1] = MIN(cropped_upper - FLT_MIN, subarray[1]);

    // Leave the rest of the subarray extents intact
    for (unsigned int i = 1; i < dim_num_; ++i) {
      tile_slab[copy_id_][2 * i] = subarray[2 * i];
      tile_slab[copy_id_][2 * i + 1] = subarray[2 * i + 1];
    }
  } else {  // Calculate a new slab based on the previous
    // Copy previous tile slab
    std::memcpy(tile_slab[copy_id_], tile_slab[prev_id], 2 * coords_size_);

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
bool ArrayOrderedReadState::next_tile_slab_sparse_row<double>() {
  // Quick check if done
  if (read_tile_slabs_done_)
    return false;

  // For easy reference
  auto array_metadata = query_->array_metadata();
  auto subarray = (const double*)subarray_;
  auto domain = (const double*)array_metadata->domain()->domain();
  auto tile_extents = (const double*)array_metadata->domain()->tile_extents();
  double* tile_slab[2];
  for (unsigned int i = 0; i < 2; ++i)
    tile_slab[i] = (double*)tile_slab_[i];
  unsigned int prev_id = (copy_id_ + 1) % 2;

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
    for (unsigned int i = 1; i < dim_num_; ++i) {
      tile_slab[copy_id_][2 * i] = subarray[2 * i];
      tile_slab[copy_id_][2 * i + 1] = subarray[2 * i + 1];
    }
  } else {  // Calculate a new slab based on the previous
    // Copy previous tile slab
    std::memcpy(tile_slab[copy_id_], tile_slab[prev_id], 2 * coords_size_);

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
Status ArrayOrderedReadState::read() {
  // For easy reference
  auto array_metadata = query_->array_metadata();
  Layout layout = query_->layout();

  if (layout == Layout::COL_MAJOR) {
    if (array_metadata->dense())
      return read_dense_sorted_col<T>();

    return read_sparse_sorted_col<T>();
  }

  if (layout == Layout::ROW_MAJOR) {
    if (array_metadata->dense())
      return read_dense_sorted_row<T>();

    return read_sparse_sorted_row<T>();
  }

  assert(0);  // The code should never reach here
  return LOG_STATUS(Status::ASRSError("Invalid query layout when reading"));
}

template <class T>
Status ArrayOrderedReadState::read_dense_sorted_col() {
  // For easy reference
  auto array_metadata = query_->array_metadata();
  auto subarray = static_cast<const T*>(subarray_);

  // Check if this can be satisfied with a default read
  if (array_metadata->cell_order() == Layout::COL_MAJOR &&
      array_metadata->domain()->is_contained_in_tile_slab_row<T>(subarray))
    return query_->read(copy_state_.buffers_, copy_state_.buffer_sizes_);

  // These gotos SIGNIFICANTLY simplify the resume from overflow logic
  if (resume_copy_)
    goto copy_label_1;
  if (resume_copy_2_)
    goto copy_label_2;

  // First AIO
  if (next_tile_slab_dense_col<T>()) {
    reset_buffer_sizes_tmp(copy_id_);
    async_wait_[copy_id_] = true;
    RETURN_NOT_OK(async_submit_query(copy_id_));
    copy_id_ = (copy_id_ + 1) % 2;
  }

  // Iterate over tile slabs
  while (next_tile_slab_dense_col<T>()) {
    // Submit AIO
    reset_buffer_sizes_tmp(copy_id_);
    async_wait_[copy_id_] = true;
    RETURN_NOT_OK(async_submit_query(copy_id_));
    copy_id_ = (copy_id_ + 1) % 2;

    async_wait(copy_id_);

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
    async_wait(copy_id_);

    if (copy_tile_slab_done())
      reset_tile_slab_state<T>();

  copy_label_2:  // Resume from the point the copy led to overflow
    resume_copy_2_ = false;
    copy_tile_slab_dense();

    if (overflow())
      resume_copy_2_ = true;
  }

  // Assign the true buffer sizes
  for (unsigned int i = 0; i < buffer_num_; ++i)
    copy_state_.buffer_sizes_[i] = copy_state_.buffer_offsets_[i];

  return Status::Ok();
}

template <class T>
Status ArrayOrderedReadState::read_dense_sorted_row() {
  // For easy reference
  auto array_metadata = query_->array_metadata();
  auto subarray = static_cast<const T*>(subarray_);

  // Check if this can be satisfied with a default read
  if (array_metadata->cell_order() == Layout::ROW_MAJOR &&
      array_metadata->domain()->is_contained_in_tile_slab_col<T>(subarray))
    return query_->read(copy_state_.buffers_, copy_state_.buffer_sizes_);

  // These gotos SIGNIFICANTLY simplify the resume from overflow logic
  if (resume_copy_)
    goto copy_label_1;
  if (resume_copy_2_)
    goto copy_label_2;

  // First AIO
  if (next_tile_slab_dense_row<T>()) {
    reset_buffer_sizes_tmp(copy_id_);
    async_wait_[copy_id_] = true;
    RETURN_NOT_OK(async_submit_query(copy_id_));
    copy_id_ = (copy_id_ + 1) % 2;
  }

  // Iterate over each tile slab
  while (next_tile_slab_dense_row<T>()) {
    // Submit AIO
    reset_buffer_sizes_tmp(copy_id_);
    async_wait_[copy_id_] = true;
    RETURN_NOT_OK(async_submit_query(copy_id_));
    copy_id_ = (copy_id_ + 1) % 2;

    async_wait(copy_id_);

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
    async_wait(copy_id_);
    if (copy_tile_slab_done())
      reset_tile_slab_state<T>();

  copy_label_2:  // Resume from the point the copy led to overflow
    resume_copy_2_ = false;
    copy_tile_slab_dense();

    if (overflow())
      resume_copy_2_ = true;
  }

  // Assign the true buffer sizes
  for (unsigned int i = 0; i < buffer_num_; ++i)
    copy_state_.buffer_sizes_[i] = copy_state_.buffer_offsets_[i];

  return Status::Ok();
}

template <class T>
Status ArrayOrderedReadState::read_sparse_sorted_col() {
  // For easy reference
  auto array_metadata = query_->array_metadata();
  auto subarray = static_cast<const T*>(subarray_);

  // Check if this can be satisfied with a default read
  if (array_metadata->cell_order() == Layout::COL_MAJOR &&
      array_metadata->domain()->is_contained_in_tile_slab_row<T>(subarray))
    return query_->read(copy_state_.buffers_, copy_state_.buffer_sizes_);

  // These gotos SIGNIFICANTLY simplify the resume from overflow logic
  if (resume_copy_)
    goto copy_label_1;
  if (resume_copy_2_)
    goto copy_label_2;

  // First AIO
  if (next_tile_slab_sparse_col<T>()) {
    reset_buffer_sizes_tmp(copy_id_);
    async_wait_[copy_id_] = true;
    RETURN_NOT_OK(async_submit_query(copy_id_));
    copy_id_ = (copy_id_ + 1) % 2;
  }

  // Iterate over tile slabs
  while (next_tile_slab_sparse_col<T>()) {
    // Submit AIO
    reset_buffer_sizes_tmp(copy_id_);
    async_wait_[copy_id_] = true;
    RETURN_NOT_OK(async_submit_query(copy_id_));
    copy_id_ = (copy_id_ + 1) % 2;

    async_wait(copy_id_);

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
    async_wait(copy_id_);
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
  for (unsigned int i = 0; i < buffer_num_ - extra_coords_; ++i)
    copy_state_.buffer_sizes_[i] = copy_state_.buffer_offsets_[i];

  return Status::Ok();
}

template <class T>
Status ArrayOrderedReadState::read_sparse_sorted_row() {
  // For easy reference
  auto array_metadata = query_->array_metadata();
  auto subarray = static_cast<const T*>(subarray_);

  // Check if this can be satisfied with a default read
  if (array_metadata->cell_order() == Layout::ROW_MAJOR &&
      array_metadata->domain()->is_contained_in_tile_slab_col<T>(subarray))
    return query_->read(copy_state_.buffers_, copy_state_.buffer_sizes_);

  // These gotos SIGNIFICANTLY simplify the resume from overflow logic
  if (resume_copy_)
    goto copy_label_1;
  if (resume_copy_2_)
    goto copy_label_2;

  // First async query
  if (next_tile_slab_sparse_row<T>()) {
    reset_buffer_sizes_tmp(copy_id_);
    async_wait_[copy_id_] = true;
    RETURN_NOT_OK(async_submit_query(copy_id_));
    copy_id_ = (copy_id_ + 1) % 2;
  }

  // Iterate over tile slabs
  while (next_tile_slab_sparse_row<T>()) {
    // Submit async query
    reset_buffer_sizes_tmp(copy_id_);
    async_wait_[copy_id_] = true;
    RETURN_NOT_OK(async_submit_query(copy_id_));
    copy_id_ = (copy_id_ + 1) % 2;

    async_wait(copy_id_);

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
    async_wait(copy_id_);
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
  for (unsigned int i = 0; i < buffer_num_ - extra_coords_; ++i)
    copy_state_.buffer_sizes_[i] = copy_state_.buffer_offsets_[i];

  return Status::Ok();
}

void ArrayOrderedReadState::reset_buffer_sizes_tmp(unsigned int id) {
  for (unsigned int i = 0; i < buffer_num_; ++i)
    buffer_sizes_tmp_[id][i] = buffer_sizes_[id][i];
}

void ArrayOrderedReadState::reset_copy_state(
    void** buffers, uint64_t* buffer_sizes) {
  copy_state_.buffers_ = buffers;
  copy_state_.buffer_sizes_ = buffer_sizes;
  for (unsigned int i = 0; i < buffer_num_; ++i)
    copy_state_.buffer_offsets_[i] = 0;
}

void ArrayOrderedReadState::reset_overflow() {
  auto anum = (unsigned int)attribute_ids_.size();
  for (unsigned int i = 0; i < anum; ++i)
    overflow_[i] = false;
}

template <class T>
void ArrayOrderedReadState::reset_tile_coords() {
  auto tile_coords = (T*)tile_coords_;
  for (unsigned int i = 0; i < dim_num_; ++i)
    tile_coords[i] = 0;
}

template <class T>
void ArrayOrderedReadState::reset_tile_slab_state() {
  // For easy reference
  auto anum = (unsigned int)attribute_ids_.size();
  bool dense = query_->array_metadata()->dense();

  // Both dense and sparse
  for (unsigned int i = 0; i < anum; ++i)
    tile_slab_state_.copy_tile_slab_done_[i] = false;

  if (dense) {  // DENSE
    auto current_coords = (T**)tile_slab_state_.current_coords_;
    auto tile_slab = (const T*)tile_slab_norm_[copy_id_];

    // Reset values
    for (unsigned int i = 0; i < anum; ++i) {
      tile_slab_state_.current_offsets_[i] = 0;
      tile_slab_state_.current_tile_[i] = 0;
      for (unsigned int j = 0; j < dim_num_; ++j)
        current_coords[i][j] = tile_slab[2 * j];
    }
  } else {  // SPARSE
    for (unsigned int i = 0; i < anum; ++i)
      tile_slab_state_.current_cell_pos_[i] = 0;
  }
}

template <class T>
void ArrayOrderedReadState::sort_cell_pos() {
  // For easy reference
  auto array_metadata = query_->array_metadata();
  auto dim_num = array_metadata->dim_num();
  uint64_t cell_num = buffer_sizes_tmp_[copy_id_][coords_buf_i_] / coords_size_;
  Layout layout = query_->layout();
  auto buffer = static_cast<const T*>(buffers_[copy_id_][coords_buf_i_]);

  // Populate cell_pos
  cell_pos_.resize(cell_num);
  for (uint64_t i = 0; i < cell_num; ++i)
    cell_pos_[i] = i;

  // Invoke the proper sort function, based on the mode
  if (layout == Layout::ROW_MAJOR) {
    // Sort cell positions
    std::sort(
        cell_pos_.begin(), cell_pos_.end(), SmallerRow<T>(buffer, dim_num));
  } else if (layout == Layout::COL_MAJOR) {
    // Sort cell positions
    std::sort(
        cell_pos_.begin(), cell_pos_.end(), SmallerCol<T>(buffer, dim_num));
  } else {
    assert(0);
  }
}

template <class T>
void ArrayOrderedReadState::update_current_tile_and_offset(unsigned int aid) {
  // For easy reference
  uint64_t& tid = tile_slab_state_.current_tile_[aid];
  uint64_t& current_offset = tile_slab_state_.current_offsets_[aid];
  uint64_t cid;

  // Calculate the new tile id
  tid = get_tile_id<T>(aid);

  // Calculate the cell id
  cid = get_cell_id<T>(aid);

  // Calculate new offset
  current_offset = tile_slab_info_[copy_id_].start_offsets_[aid][tid] +
                   cid * attribute_sizes_[aid];
}

// Explicit template instantiations

template Status ArrayOrderedReadState::read_dense_sorted_col<int>();
template Status ArrayOrderedReadState::read_dense_sorted_col<int64_t>();
template Status ArrayOrderedReadState::read_dense_sorted_col<float>();
template Status ArrayOrderedReadState::read_dense_sorted_col<double>();
template Status ArrayOrderedReadState::read_dense_sorted_col<int8_t>();
template Status ArrayOrderedReadState::read_dense_sorted_col<uint8_t>();
template Status ArrayOrderedReadState::read_dense_sorted_col<int16_t>();
template Status ArrayOrderedReadState::read_dense_sorted_col<uint16_t>();
template Status ArrayOrderedReadState::read_dense_sorted_col<uint32_t>();
template Status ArrayOrderedReadState::read_dense_sorted_col<uint64_t>();

template Status ArrayOrderedReadState::read_dense_sorted_row<int>();
template Status ArrayOrderedReadState::read_dense_sorted_row<int64_t>();
template Status ArrayOrderedReadState::read_dense_sorted_row<float>();
template Status ArrayOrderedReadState::read_dense_sorted_row<double>();
template Status ArrayOrderedReadState::read_dense_sorted_row<int8_t>();
template Status ArrayOrderedReadState::read_dense_sorted_row<uint8_t>();
template Status ArrayOrderedReadState::read_dense_sorted_row<int16_t>();
template Status ArrayOrderedReadState::read_dense_sorted_row<uint16_t>();
template Status ArrayOrderedReadState::read_dense_sorted_row<uint32_t>();
template Status ArrayOrderedReadState::read_dense_sorted_row<uint64_t>();

};  // namespace tiledb
