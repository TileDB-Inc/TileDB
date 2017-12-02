/**
 * @file   array_ordered_write_state.cc
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
 * This file implements the ArrayOrderedWriteState class.
 */

#include "array_ordered_write_state.h"
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

const uint64_t ArrayOrderedWriteState::INVALID_UINT64 = UINT64_MAX;

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

ArrayOrderedWriteState::ArrayOrderedWriteState(Query* query)
    : attribute_ids_(query->attribute_ids())
    , query_(query) {
  // For easy reference.
  auto array_metadata = query_->array_metadata();
  auto anum = attribute_ids_.size();

  // Initializations
  coords_size_ = array_metadata->coords_size();
  copy_id_ = 0;
  dim_num_ = array_metadata->dim_num();
  tile_coords_ = nullptr;
  tile_domain_ = nullptr;
  buffer_sizes_ = nullptr;
  buffers_ = nullptr;
  for (unsigned int i = 0; i < 2; ++i) {
    async_query_[i] = nullptr;
    tile_slab_[i] = std::malloc(2 * coords_size_);
    tile_slab_norm_[i] = std::malloc(2 * coords_size_);
    tile_slab_init_[i] = false;
    async_wait_[i] = false;
  }
  for (unsigned int i = 0; i < anum; ++i) {
    if (array_metadata->var_size(attribute_ids_[i]))
      attribute_sizes_.push_back(sizeof(uint64_t));
    else
      attribute_sizes_.push_back(array_metadata->cell_size(attribute_ids_[i]));
  }

  subarray_ = std::malloc(2 * coords_size_);
  std::memcpy(subarray_, query_->subarray(), 2 * coords_size_);

  // Calculate expanded subarray
  expanded_subarray_ = std::malloc(2 * coords_size_);
  std::memcpy(expanded_subarray_, subarray_, 2 * coords_size_);
  array_metadata->domain()->expand_domain(expanded_subarray_);

  // Calculate number of buffers
  calculate_buffer_num();

  // Initialize tile slab info and state, and copy state
  init_tile_slab_info();
  init_tile_slab_state();
  init_copy_state();
}

ArrayOrderedWriteState::~ArrayOrderedWriteState() {
  // Clean up
  std::free(subarray_);
  std::free(expanded_subarray_);
  std::free(tile_coords_);
  std::free(tile_domain_);

  for (unsigned int i = 0; i < 2; ++i) {
    if (async_query_[i] != nullptr)
      async_query_[i]->finalize();
    delete async_query_[i];
    std::free(tile_slab_[i]);
    std::free(tile_slab_norm_[i]);
  }

  // Free tile slab info and state, and copy state
  free_copy_state();
  free_tile_slab_state();
  free_tile_slab_info();
}

/* ****************************** */
/*            MUTATORS            */
/* ****************************** */

Status ArrayOrderedWriteState::finalize() {
  for (auto& aq : async_query_) {
    if (aq != nullptr)
      RETURN_NOT_OK(aq->finalize());
    delete aq;
    aq = nullptr;
  }

  return Status::Ok();
}

Status ArrayOrderedWriteState::init() {
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

  // Success
  return Status::Ok();
}

Status ArrayOrderedWriteState::write(void** buffers, uint64_t* buffer_sizes) {
  // Locally store user buffer information
  create_user_buffers(buffers, buffer_sizes);

  // Create buffers
  RETURN_NOT_OK(create_copy_state_buffers());

  for (unsigned int i = 0; i < 2; ++i)
    async_data_[i] = {i, 0, this};

  // Call the appropriate templated write
  Datatype type = query_->array_metadata()->coords_type();
  if (type == Datatype::INT32)
    return write<int>();
  if (type == Datatype::INT64)
    return write<int64_t>();
  if (type == Datatype::INT8)
    return write<int8_t>();
  if (type == Datatype::UINT8)
    return write<uint8_t>();
  if (type == Datatype::INT16)
    return write<int16_t>();
  if (type == Datatype::UINT16)
    return write<uint16_t>();
  if (type == Datatype::UINT32)
    return write<uint32_t>();
  if (type == Datatype::UINT64)
    return write<uint64_t>();

  // Error
  assert(0);
  return Status::ASWSError("Cannot write; Invalid datatype");
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

template <class T>
void* ArrayOrderedWriteState::advance_cell_slab_col_s(void* data) {
  ArrayOrderedWriteState* asws = ((ASWS_Data*)data)->asws_;
  unsigned int aid = ((ASWS_Data*)data)->id_;
  asws->advance_cell_slab_col<T>(aid);
  return nullptr;
}

template <class T>
void* ArrayOrderedWriteState::advance_cell_slab_row_s(void* data) {
  ArrayOrderedWriteState* asws = ((ASWS_Data*)data)->asws_;
  unsigned int aid = ((ASWS_Data*)data)->id_;
  asws->advance_cell_slab_row<T>(aid);
  return nullptr;
}

template <class T>
void ArrayOrderedWriteState::advance_cell_slab_col(unsigned int aid) {
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
void ArrayOrderedWriteState::advance_cell_slab_row(unsigned int aid) {
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

void* ArrayOrderedWriteState::async_done(void* data) {
  // Retrieve data
  ArrayOrderedWriteState* asws = ((ASWS_Data*)data)->asws_;
  unsigned int id = ((ASWS_Data*)data)->id_;

  asws->async_notify(id);

  return nullptr;
}

void ArrayOrderedWriteState::async_notify(unsigned int id) {
  {
    std::lock_guard<std::mutex> lk(async_mtx_[id]);
    async_wait_[id] = false;
  }
  async_cv_[id].notify_one();
}

Status ArrayOrderedWriteState::async_submit_query(unsigned int id) {
  // For easy reference
  auto storage_manager = query_->storage_manager();
  bool separate_fragments = this->separate_fragments();

  // Sanity check
  assert(storage_manager != nullptr);
  if (separate_fragments) {
    if (async_query_[id] != nullptr)
      RETURN_NOT_OK(async_query_[id]->finalize());
    delete async_query_[id];
    async_query_[id] = new Query();
    RETURN_NOT_OK(async_query_[id]->init(
        query_->storage_manager(),
        query_->array_metadata(),
        query_->fragment_metadata(),
        QueryType::WRITE,
        Layout::GLOBAL_ORDER,
        tile_slab_[id],
        query_->attribute_ids(),
        copy_state_.buffers_[id],
        copy_state_.buffer_offsets_[id]));
    async_query_[id]->set_callback(async_done, &(async_data_[id]));
  } else {
    if (id == 0) {
      if (async_query_[id] == nullptr) {
        async_query_[id] = new Query();
        RETURN_NOT_OK(async_query_[id]->init(
            query_->storage_manager(),
            query_->array_metadata(),
            query_->fragment_metadata(),
            QueryType::WRITE,
            Layout::GLOBAL_ORDER,
            query_->subarray(),
            query_->attribute_ids(),
            copy_state_.buffers_[id],
            copy_state_.buffer_offsets_[id]));
        async_query_[id]->set_callback(async_done, &(async_data_[id]));
      }
    } else {  // id == 1
      if (async_query_[id] == nullptr) {
        async_query_[id] = new Query(async_query_[0]);
        async_query_[id]->set_buffers(
            copy_state_.buffers_[id], copy_state_.buffer_offsets_[id]);
        async_query_[id]->set_callback(async_done, &(async_data_[id]));
      }
    }
  }

  // Send the async query
  RETURN_NOT_OK(storage_manager->async_push_query(async_query_[id], 1));

  // Success
  return Status::Ok();
}

void ArrayOrderedWriteState::async_wait(unsigned int id) {
  std::unique_lock<std::mutex> lk(async_mtx_[id]);
  async_cv_[id].wait(lk, [id, this] { return !async_wait_[id]; });
  lk.unlock();
}

void ArrayOrderedWriteState::calculate_buffer_num() {
  // For easy reference
  const ArrayMetadata* array_metadata = query_->array_metadata();

  // Calculate number of buffers
  buffer_num_ = 0;
  auto attribute_id_num = attribute_ids_.size();
  for (unsigned int i = 0; i < attribute_id_num; ++i) {
    // Fix-sized attribute
    if (!array_metadata->var_size(attribute_ids_[i]))
      ++buffer_num_;
    else  // Variable-sized attribute
      buffer_num_ += 2;
  }
}

template <class T>
void* ArrayOrderedWriteState::calculate_cell_slab_info_col_col_s(void* data) {
  ArrayOrderedWriteState* asws = ((ASWS_Data*)data)->asws_;
  unsigned int id = ((ASWS_Data*)data)->id_;
  uint64_t tid = ((ASWS_Data*)data)->id_2_;
  asws->calculate_cell_slab_info_col_col<T>(id, tid);
  return nullptr;
}

template <class T>
void* ArrayOrderedWriteState::calculate_cell_slab_info_col_row_s(void* data) {
  ArrayOrderedWriteState* asws = ((ASWS_Data*)data)->asws_;
  unsigned int id = ((ASWS_Data*)data)->id_;
  uint64_t tid = ((ASWS_Data*)data)->id_2_;
  asws->calculate_cell_slab_info_col_row<T>(id, tid);
  return nullptr;
}

template <class T>
void* ArrayOrderedWriteState::calculate_cell_slab_info_row_col_s(void* data) {
  ArrayOrderedWriteState* asws = ((ASWS_Data*)data)->asws_;
  unsigned int id = ((ASWS_Data*)data)->id_;
  uint64_t tid = ((ASWS_Data*)data)->id_2_;
  asws->calculate_cell_slab_info_row_col<T>(id, tid);
  return nullptr;
}

template <class T>
void* ArrayOrderedWriteState::calculate_cell_slab_info_row_row_s(void* data) {
  ArrayOrderedWriteState* asws = ((ASWS_Data*)data)->asws_;
  unsigned int id = ((ASWS_Data*)data)->id_;
  uint64_t tid = ((ASWS_Data*)data)->id_2_;
  asws->calculate_cell_slab_info_row_row<T>(id, tid);
  return nullptr;
}

template <class T>
void ArrayOrderedWriteState::calculate_cell_slab_info_col_col(
    unsigned int id, uint64_t tid) {
  // For easy reference
  auto anum = attribute_ids_.size();
  auto range_overlap = (const T*)tile_slab_info_[id].range_overlap_[tid];
  auto tile_extents =
      (const T*)query_->array_metadata()->domain()->tile_extents();
  uint64_t cell_num;

  // Calculate number of cells in cell slab
  cell_num = range_overlap[1] - range_overlap[0] + 1;
  tile_slab_info_[id].cell_slab_num_[tid] = cell_num;

  // Calculate size of a cell slab per attribute
  for (unsigned int aid = 0; aid < anum; ++aid)
    tile_slab_info_[id].cell_slab_size_[aid][tid] =
        tile_slab_info_[id].cell_slab_num_[tid] * attribute_sizes_[aid];

  // Calculate cell offset per dimension
  uint64_t cell_offset = 1;
  tile_slab_info_[id].cell_offset_per_dim_[tid][0] = cell_offset;
  for (unsigned int i = 1; i < dim_num_; ++i) {
    cell_offset *= tile_extents[i - 1];
    tile_slab_info_[id].cell_offset_per_dim_[tid][i] = cell_offset;
  }
}

template <class T>
void ArrayOrderedWriteState::calculate_cell_slab_info_row_row(
    unsigned int id, uint64_t tid) {
  // For easy reference
  auto anum = attribute_ids_.size();
  auto range_overlap =
      static_cast<const T*>(tile_slab_info_[id].range_overlap_[tid]);
  auto tile_extents =
      (const T*)query_->array_metadata()->domain()->tile_extents();
  uint64_t cell_num;

  // Calculate number of cells in cell slab
  cell_num = range_overlap[2 * (dim_num_ - 1) + 1] -
             range_overlap[2 * (dim_num_ - 1)] + 1;
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
      cell_offset *= tile_extents[i + 1];
      tile_slab_info_[id].cell_offset_per_dim_[tid][i] = cell_offset;
      if (i == 0)
        break;
    }
  }
}

template <class T>
void ArrayOrderedWriteState::calculate_cell_slab_info_col_row(
    unsigned int id, uint64_t tid) {
  // For easy reference
  auto anum = attribute_ids_.size();
  auto tile_extents =
      (const T*)query_->array_metadata()->domain()->tile_extents();

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
      cell_offset *= tile_extents[i + 1];
      tile_slab_info_[id].cell_offset_per_dim_[tid][i] = cell_offset;
      if (i == 0)
        break;
    }
  }
}

template <class T>
void ArrayOrderedWriteState::calculate_cell_slab_info_row_col(
    unsigned int id, uint64_t tid) {
  // For easy reference
  auto anum = attribute_ids_.size();
  auto tile_extents =
      (const T*)query_->array_metadata()->domain()->tile_extents();

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
    cell_offset *= tile_extents[i - 1];
    tile_slab_info_[id].cell_offset_per_dim_[tid][i] = cell_offset;
  }
}

template <class T>
void ArrayOrderedWriteState::calculate_tile_domain(unsigned int id) {
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
void ArrayOrderedWriteState::calculate_tile_slab_info(unsigned int id) {
  // Calculate number of tiles, if they are not already calculated
  if (tile_slab_info_[id].tile_num_ == INVALID_UINT64)
    init_tile_slab_info<T>(id);

  // Calculate tile domain, if not calculated yet
  if (tile_domain_ == nullptr)
    calculate_tile_domain<T>(id);

  // Reset tile coordinates
  reset_tile_coords<T>();

  // Calculate tile slab info
  ASWS_Data asws_data = {id, 0, this};
  (*calculate_tile_slab_info_)(&asws_data);
}

template <class T>
void* ArrayOrderedWriteState::calculate_tile_slab_info_col(void* data) {
  ArrayOrderedWriteState* asws = ((ASWS_Data*)data)->asws_;
  unsigned int id = ((ASWS_Data*)data)->id_;
  asws->calculate_tile_slab_info_col<T>(id);
  return nullptr;
}

template <class T>
void ArrayOrderedWriteState::calculate_tile_slab_info_col(unsigned int id) {
  // For easy reference
  auto tile_domain = (const T*)tile_domain_;
  auto tile_coords = (T*)tile_coords_;
  auto tile_extents =
      (const T*)query_->array_metadata()->domain()->tile_extents();
  auto range_overlap = (T**)tile_slab_info_[id].range_overlap_;
  auto tile_slab = (const T*)tile_slab_norm_[id];
  uint64_t total_cell_num = 0;
  auto anum = attribute_ids_.size();

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
      tile_cell_num *= tile_extents[i];
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
    ASWS_Data asws_data = {id, tid, this};
    (*calculate_cell_slab_info_)(&asws_data);

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
void* ArrayOrderedWriteState::calculate_tile_slab_info_row(void* data) {
  ArrayOrderedWriteState* asws = ((ASWS_Data*)data)->asws_;
  unsigned int id = ((ASWS_Data*)data)->id_;
  asws->calculate_tile_slab_info_row<T>(id);
  return nullptr;
}

template <class T>
void ArrayOrderedWriteState::calculate_tile_slab_info_row(unsigned int id) {
  // For easy reference
  auto tile_domain = (const T*)tile_domain_;
  auto tile_coords = (T*)tile_coords_;
  auto tile_extents =
      (const T*)query_->array_metadata()->domain()->tile_extents();
  auto range_overlap = (T**)tile_slab_info_[id].range_overlap_;
  auto tile_slab = (const T*)tile_slab_norm_[id];
  uint64_t total_cell_num = 0;
  auto anum = attribute_ids_.size();

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
      tile_cell_num *= tile_extents[i];
    }

    // Calculate tile offsets per dimension
    uint64_t tile_offset = 1;
    tile_slab_info_[id].tile_offset_per_dim_[dim_num_ - 1] = tile_offset;
    if (dim_num_ > 1) {
      for (unsigned int i = dim_num_ - 2;; --i) {
        tile_offset *=
            (tile_domain[2 * (i + 1) + 1] - tile_domain[2 * (i + 1)] + 1);
        tile_slab_info_[id].tile_offset_per_dim_[i] = tile_offset;
        if (i == 0)
          break;
      }
    }

    // Calculate cell slab info
    ASWS_Data asws_data = {id, tid, this};
    (*calculate_cell_slab_info_)(&asws_data);

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

void ArrayOrderedWriteState::copy_tile_slab() {
  // For easy reference
  auto array_metadata = query_->array_metadata();
  auto nattributes = attribute_ids_.size();

  // Copy tile slab for each attribute separately
  for (unsigned int i = 0, b = 0; i < nattributes; ++i) {
    Datatype type = array_metadata->type(attribute_ids_.at(i));
    if (!array_metadata->var_size(attribute_ids_[i])) {
      if (type == Datatype::INT32)
        copy_tile_slab<int>(i, b);
      else if (type == Datatype::INT64)
        copy_tile_slab<int64_t>(i, b);
      else if (type == Datatype::FLOAT32)
        copy_tile_slab<float>(i, b);
      else if (type == Datatype::FLOAT64)
        copy_tile_slab<double>(i, b);
      else if (type == Datatype::CHAR)
        copy_tile_slab<char>(i, b);
      else if (type == Datatype::INT8)
        copy_tile_slab<int8_t>(i, b);
      else if (type == Datatype::UINT8)
        copy_tile_slab<uint8_t>(i, b);
      else if (type == Datatype::INT16)
        copy_tile_slab<int16_t>(i, b);
      else if (type == Datatype::UINT16)
        copy_tile_slab<uint16_t>(i, b);
      else if (type == Datatype::UINT32)
        copy_tile_slab<uint32_t>(i, b);
      else if (type == Datatype::UINT64)
        copy_tile_slab<uint64_t>(i, b);
      ++b;
    } else {
      if (type == Datatype::INT32)
        copy_tile_slab_var<int>(i, b);
      else if (type == Datatype::INT64)
        copy_tile_slab_var<int64_t>(i, b);
      else if (type == Datatype::FLOAT32)
        copy_tile_slab_var<float>(i, b);
      else if (type == Datatype::FLOAT64)
        copy_tile_slab_var<double>(i, b);
      else if (type == Datatype::CHAR)
        copy_tile_slab_var<char>(i, b);
      else if (type == Datatype::INT8)
        copy_tile_slab_var<int8_t>(i, b);
      else if (type == Datatype::UINT8)
        copy_tile_slab_var<uint8_t>(i, b);
      else if (type == Datatype::INT16)
        copy_tile_slab_var<int16_t>(i, b);
      else if (type == Datatype::UINT16)
        copy_tile_slab_var<uint16_t>(i, b);
      else if (type == Datatype::UINT32)
        copy_tile_slab_var<uint32_t>(i, b);
      else if (type == Datatype::UINT64)
        copy_tile_slab_var<uint64_t>(i, b);
      b += 2;
    }
  }
}

template <class T>
void ArrayOrderedWriteState::copy_tile_slab(
    unsigned int aid, unsigned int bid) {
  // For easy reference
  uint64_t& tid = tile_slab_state_.current_tile_[aid];
  uint64_t& buffer_offset = buffer_offsets_[bid];
  auto buffer = (char*)buffers_[bid];
  auto local_buffer = (char*)copy_state_.buffers_[copy_id_][bid];
  uint64_t& local_buffer_offset = copy_state_.buffer_offsets_[copy_id_][bid];
  uint64_t local_buffer_size = copy_state_.buffer_sizes_[copy_id_][bid];
  ASWS_Data asws_data = {aid, bid, this};

  // Fill with empty
  fill_with_empty<T>(bid);

  // Important for initializing the current tile and offsets!
  update_current_tile_and_offset(aid);

  // Iterate over the tile slab cells
  for (;;) {
    // For easy reference
    uint64_t cell_slab_size =
        tile_slab_info_[copy_id_].cell_slab_size_[aid][tid];
    uint64_t& local_buffer_offset_cur = tile_slab_state_.current_offsets_[aid];

    // Copy cell slab from user to local buffer
    std::memcpy(
        local_buffer + local_buffer_offset_cur,
        buffer + buffer_offset,
        cell_slab_size);

    // Update user buffer offset
    buffer_offset += cell_slab_size;

    // Prepare for new slab
    (*advance_cell_slab_)(&asws_data);

    // Terminating condition
    if (tile_slab_state_.copy_tile_slab_done_[aid])
      break;
  }

  // Set local buffer offset
  local_buffer_offset = local_buffer_size;
}

template <class T>
void ArrayOrderedWriteState::copy_tile_slab_var(
    unsigned int aid, unsigned int bid) {
  // For easy reference
  uint64_t& tid = tile_slab_state_.current_tile_[aid];
  uint64_t& buffer_offset = buffer_offsets_[bid];
  uint64_t buffer_size = buffer_sizes_[bid];
  uint64_t buffer_var_size = buffer_sizes_[bid + 1];
  auto buffer_var = (char*)buffers_[bid + 1];
  auto buffer_s = (uint64_t*)buffers_[bid];
  auto local_buffer = (char*)copy_state_.buffers_[copy_id_][bid];
  auto local_buffer_s = (uint64_t*)copy_state_.buffers_[copy_id_][bid];
  uint64_t local_buffer_size = copy_state_.buffer_sizes_[copy_id_][bid];
  uint64_t& local_buffer_offset = copy_state_.buffer_offsets_[copy_id_][bid];
  auto local_buffer_var = (char*)copy_state_.buffers_[copy_id_][bid + 1];
  uint64_t& local_buffer_offset_var =
      copy_state_.buffer_offsets_[copy_id_][bid + 1];
  uint64_t& local_buffer_var_size =
      copy_state_.buffer_sizes_[copy_id_][bid + 1];
  uint64_t cell_num_in_buffer = buffer_size / sizeof(uint64_t);
  uint64_t cell_num_in_tile_slab = local_buffer_size / sizeof(uint64_t);
  ASWS_Data asws_data = {aid, 0, this};

  // Important for initializing the current tile and offsets!
  update_current_tile_and_offset(aid);

  // Fill the local buffer offsets with zeros
  bzero(local_buffer, local_buffer_size);

  // Handle offsets first
  for (;;) {
    // For easy reference
    uint64_t cell_slab_size =
        tile_slab_info_[copy_id_].cell_slab_size_[aid][tid];
    int64_t cell_num_in_slab = cell_slab_size / sizeof(uint64_t);
    uint64_t local_buffer_offset_cur = tile_slab_state_.current_offsets_[aid];

    // Calculate variable cell slab size
    uint64_t cell_start = buffer_offset / sizeof(uint64_t);
    uint64_t cell_end = cell_start + cell_num_in_slab;

    // Keep track of where each variable-sized cell is.
    // Note that cell ids start with 1 here!
    for (uint64_t i = cell_start + 1; i <= cell_end; ++i) {
      std::memcpy(local_buffer + local_buffer_offset_cur, &i, sizeof(uint64_t));
      local_buffer_offset_cur += sizeof(uint64_t);
      buffer_offset += sizeof(uint64_t);
    }

    // Prepare for new slab
    (*advance_cell_slab_)(&asws_data);

    // Terminating condition
    if (tile_slab_state_.copy_tile_slab_done_[aid])
      break;
  }

  // Rectify offsets and copy variable-sized cells
  uint64_t cell;
  uint64_t cell_size_var;
  for (uint64_t i = 0; i < cell_num_in_tile_slab; ++i) {
    // Handle empties
    if (local_buffer_s[i] == 0) {
      local_buffer_s[i] = local_buffer_offset_var;
      fill_with_empty_var<T>(bid);
      local_buffer_offset_var += sizeof(T);
      continue;
    }

    // Find size of variable-sized cell
    cell = local_buffer_s[i] - 1;  // So that cell ids start from 0
    cell_size_var = (cell == cell_num_in_buffer - 1) ?
                        buffer_var_size - buffer_s[cell] :
                        buffer_s[cell + 1] - buffer_s[cell];

    // Rectify offset
    local_buffer_s[i] = local_buffer_offset_var;

    // Expand the variable-sized buffer if necessary
    while (local_buffer_offset_var + cell_size_var > local_buffer_var_size) {
      utils::expand_buffer(
          copy_state_.buffers_[copy_id_][bid + 1],
          &(copy_state_.buffer_sizes_[copy_id_][bid + 1]));
      local_buffer_var = (char*)copy_state_.buffers_[copy_id_][bid + 1];
    }

    // Copy variable-sized cell
    std::memcpy(
        local_buffer_var + local_buffer_offset_var,
        buffer_var + buffer_s[cell],
        cell_size_var);
    local_buffer_offset_var += cell_size_var;
  }

  // Set local buffer offset
  local_buffer_offset = local_buffer_size;
}

Status ArrayOrderedWriteState::create_copy_state_buffers() {
  // For easy reference
  const ArrayMetadata* array_metadata = query_->array_metadata();

  // Get cell number in a (full) tile slab
  uint64_t tile_slab_cell_num = 0;
  Layout layout = query_->layout();
  if (layout == Layout::ROW_MAJOR)
    tile_slab_cell_num =
        array_metadata->domain()->tile_slab_row_cell_num(expanded_subarray_);
  else if (layout == Layout::COL_MAJOR)
    tile_slab_cell_num =
        array_metadata->domain()->tile_slab_col_cell_num(expanded_subarray_);
  else
    assert(false);

  // Calculate buffer sizes
  auto attribute_id_num = attribute_ids_.size();
  for (auto& buffer_size : copy_state_.buffer_sizes_) {
    buffer_size = new uint64_t[buffer_num_];
    for (unsigned int i = 0, b = 0; i < attribute_id_num; ++i) {
      // Fix-sized attribute
      if (!array_metadata->var_size(attribute_ids_[i])) {
        buffer_size[b++] =
            tile_slab_cell_num * array_metadata->cell_size(attribute_ids_[i]);
      } else {  // Variable-sized attribute
        buffer_size[b++] = tile_slab_cell_num * sizeof(uint64_t);
        buffer_size[b++] = 2 * tile_slab_cell_num * sizeof(uint64_t);
      }
    }
  }

  // Allocate buffers
  for (unsigned int j = 0; j < 2; ++j) {
    copy_state_.buffers_[j] = (void**)std::malloc(buffer_num_ * sizeof(void*));
    if (copy_state_.buffers_[j] == nullptr) {
      return LOG_STATUS(Status::ASWSError("Cannot create local buffers"));
    }

    for (unsigned int b = 0; b < buffer_num_; ++b) {
      copy_state_.buffers_[j][b] = std::malloc(copy_state_.buffer_sizes_[j][b]);
      if (copy_state_.buffers_[j][b] == nullptr) {
        return LOG_STATUS(Status::ASWSError("Cannot allocate local buffer"));
      }
    }
  }

  // Success
  return Status::Ok();
}

void ArrayOrderedWriteState::create_user_buffers(
    void** buffers, uint64_t* buffer_sizes) {
  buffers_ = buffers;
  buffer_sizes_ = buffer_sizes;
  buffer_offsets_ = new uint64_t[buffer_num_];
  for (unsigned int i = 0; i < buffer_num_; ++i)
    buffer_offsets_[i] = 0;
}

template <>
void ArrayOrderedWriteState::fill_with_empty<int>(unsigned int bid) {
  // For easy reference
  auto local_buffer = (int*)copy_state_.buffers_[copy_id_][bid];
  uint64_t local_buffer_size = copy_state_.buffer_sizes_[copy_id_][bid];
  int empty = constants::empty_int32;

  // Fill with empty values
  uint64_t offset = 0;
  for (uint64_t i = 0; offset < local_buffer_size; offset += sizeof(int), ++i)
    local_buffer[i] = empty;
}

template <>
void ArrayOrderedWriteState::fill_with_empty<int64_t>(unsigned int bid) {
  // For easy reference
  auto local_buffer = (int64_t*)copy_state_.buffers_[copy_id_][bid];
  uint64_t local_buffer_size = copy_state_.buffer_sizes_[copy_id_][bid];
  int64_t empty = constants::empty_int64;

  // Fill with empty values
  uint64_t offset = 0;
  for (uint64_t i = 0; offset < local_buffer_size;
       offset += sizeof(int64_t), ++i)
    local_buffer[i] = empty;
}

template <>
void ArrayOrderedWriteState::fill_with_empty<float>(unsigned int bid) {
  // For easy reference
  auto local_buffer = (float*)copy_state_.buffers_[copy_id_][bid];
  uint64_t local_buffer_size = copy_state_.buffer_sizes_[copy_id_][bid];
  float empty = constants::empty_float32;

  // Fill with empty values
  uint64_t offset = 0;
  for (uint64_t i = 0; offset < local_buffer_size; offset += sizeof(float), ++i)
    local_buffer[i] = empty;
}

template <>
void ArrayOrderedWriteState::fill_with_empty<double>(unsigned int bid) {
  // For easy reference
  auto local_buffer = (double*)copy_state_.buffers_[copy_id_][bid];
  uint64_t local_buffer_size = copy_state_.buffer_sizes_[copy_id_][bid];
  double empty = constants::empty_float64;

  // Fill with empty values
  uint64_t offset = 0;
  for (uint64_t i = 0; offset < local_buffer_size;
       offset += sizeof(double), ++i)
    local_buffer[i] = empty;
}

template <>
void ArrayOrderedWriteState::fill_with_empty<char>(unsigned int bid) {
  // For easy reference
  auto local_buffer = (char*)copy_state_.buffers_[copy_id_][bid];
  uint64_t local_buffer_size = copy_state_.buffer_sizes_[copy_id_][bid];
  char empty = constants::empty_char;

  // Fill with empty values
  uint64_t offset = 0;
  for (uint64_t i = 0; offset < local_buffer_size; offset += sizeof(char), ++i)
    local_buffer[i] = empty;
}

template <>
void ArrayOrderedWriteState::fill_with_empty<int8_t>(unsigned int bid) {
  // For easy reference
  auto local_buffer = (int8_t*)copy_state_.buffers_[copy_id_][bid];
  uint64_t local_buffer_size = copy_state_.buffer_sizes_[copy_id_][bid];
  int8_t empty = constants::empty_int8;

  // Fill with empty values
  uint64_t offset = 0;
  for (uint64_t i = 0; offset < local_buffer_size;
       offset += sizeof(int8_t), ++i)
    local_buffer[i] = empty;
}

template <>
void ArrayOrderedWriteState::fill_with_empty<uint8_t>(unsigned int bid) {
  // For easy reference
  auto local_buffer = (uint8_t*)copy_state_.buffers_[copy_id_][bid];
  uint64_t local_buffer_size = copy_state_.buffer_sizes_[copy_id_][bid];
  uint8_t empty = constants::empty_uint8;

  // Fill with empty values
  uint64_t offset = 0;
  for (uint64_t i = 0; offset < local_buffer_size;
       offset += sizeof(uint8_t), ++i)
    local_buffer[i] = empty;
}

template <>
void ArrayOrderedWriteState::fill_with_empty<int16_t>(unsigned int bid) {
  // For easy reference
  auto local_buffer = (int16_t*)copy_state_.buffers_[copy_id_][bid];
  uint64_t local_buffer_size = copy_state_.buffer_sizes_[copy_id_][bid];
  int16_t empty = constants::empty_int16;

  // Fill with empty values
  uint64_t offset = 0;
  for (uint64_t i = 0; offset < local_buffer_size;
       offset += sizeof(int16_t), ++i)
    local_buffer[i] = empty;
}

template <>
void ArrayOrderedWriteState::fill_with_empty<uint16_t>(unsigned int bid) {
  // For easy reference
  auto local_buffer = (uint16_t*)copy_state_.buffers_[copy_id_][bid];
  uint64_t local_buffer_size = copy_state_.buffer_sizes_[copy_id_][bid];
  uint16_t empty = constants::empty_uint16;

  // Fill with empty values
  uint64_t offset = 0;
  for (uint64_t i = 0; offset < local_buffer_size;
       offset += sizeof(uint16_t), ++i)
    local_buffer[i] = empty;
}

template <>
void ArrayOrderedWriteState::fill_with_empty<uint32_t>(unsigned int bid) {
  // For easy reference
  auto local_buffer = (uint32_t*)copy_state_.buffers_[copy_id_][bid];
  uint64_t local_buffer_size = copy_state_.buffer_sizes_[copy_id_][bid];
  uint32_t empty = constants::empty_uint32;

  // Fill with empty values
  uint64_t offset = 0;
  for (uint64_t i = 0; offset < local_buffer_size;
       offset += sizeof(uint32_t), ++i)
    local_buffer[i] = empty;
}

template <>
void ArrayOrderedWriteState::fill_with_empty<uint64_t>(unsigned int bid) {
  // For easy reference
  auto local_buffer = (uint64_t*)copy_state_.buffers_[copy_id_][bid];
  uint64_t local_buffer_size = copy_state_.buffer_sizes_[copy_id_][bid];
  uint64_t empty = constants::empty_uint64;

  // Fill with empty values
  uint64_t offset = 0;
  for (uint64_t i = 0; offset < local_buffer_size;
       offset += sizeof(uint64_t), ++i)
    local_buffer[i] = empty;
}

template <>
void ArrayOrderedWriteState::fill_with_empty_var<int>(unsigned int bid) {
  // For easy reference
  auto local_buffer_var = (char*)copy_state_.buffers_[copy_id_][bid + 1];
  uint64_t local_buffer_offset_var =
      copy_state_.buffer_offsets_[copy_id_][bid + 1];
  int empty = constants::empty_int32;

  // Fill an empty value
  std::memcpy(local_buffer_var + local_buffer_offset_var, &empty, sizeof(int));
}

template <>
void ArrayOrderedWriteState::fill_with_empty_var<int64_t>(unsigned int bid) {
  // For easy reference
  auto local_buffer_var = (char*)copy_state_.buffers_[copy_id_][bid + 1];
  uint64_t local_buffer_offset_var =
      copy_state_.buffer_offsets_[copy_id_][bid + 1];
  int64_t empty = constants::empty_int64;

  // Fill an empty value
  std::memcpy(
      local_buffer_var + local_buffer_offset_var, &empty, sizeof(int64_t));
}

template <>
void ArrayOrderedWriteState::fill_with_empty_var<float>(unsigned int bid) {
  // For easy reference
  auto local_buffer_var = (char*)copy_state_.buffers_[copy_id_][bid + 1];
  uint64_t local_buffer_offset_var =
      copy_state_.buffer_offsets_[copy_id_][bid + 1];
  float empty = constants::empty_float32;

  // Fill an empty value
  std::memcpy(
      local_buffer_var + local_buffer_offset_var, &empty, sizeof(float));
}

template <>
void ArrayOrderedWriteState::fill_with_empty_var<double>(unsigned int bid) {
  // For easy reference
  auto local_buffer_var = (char*)copy_state_.buffers_[copy_id_][bid + 1];
  uint64_t local_buffer_offset_var =
      copy_state_.buffer_offsets_[copy_id_][bid + 1];
  double empty = constants::empty_float64;

  // Fill an empty value
  std::memcpy(
      local_buffer_var + local_buffer_offset_var, &empty, sizeof(double));
}

template <>
void ArrayOrderedWriteState::fill_with_empty_var<char>(unsigned int bid) {
  // For easy reference
  auto local_buffer_var = (char*)copy_state_.buffers_[copy_id_][bid + 1];
  uint64_t local_buffer_offset_var =
      copy_state_.buffer_offsets_[copy_id_][bid + 1];
  char empty = constants::empty_char;

  // Fill an empty value
  std::memcpy(local_buffer_var + local_buffer_offset_var, &empty, sizeof(char));
}

template <>
void ArrayOrderedWriteState::fill_with_empty_var<int8_t>(unsigned int bid) {
  // For easy reference
  auto local_buffer_var = (char*)copy_state_.buffers_[copy_id_][bid + 1];
  uint64_t local_buffer_offset_var =
      copy_state_.buffer_offsets_[copy_id_][bid + 1];
  int8_t empty = constants::empty_int8;

  // Fill an empty value
  std::memcpy(
      local_buffer_var + local_buffer_offset_var, &empty, sizeof(int8_t));
}

template <>
void ArrayOrderedWriteState::fill_with_empty_var<uint8_t>(unsigned int bid) {
  // For easy reference
  auto local_buffer_var = (char*)copy_state_.buffers_[copy_id_][bid + 1];
  uint64_t local_buffer_offset_var =
      copy_state_.buffer_offsets_[copy_id_][bid + 1];
  uint8_t empty = constants::empty_uint8;

  // Fill an empty value
  std::memcpy(
      local_buffer_var + local_buffer_offset_var, &empty, sizeof(uint8_t));
}

template <>
void ArrayOrderedWriteState::fill_with_empty_var<int16_t>(unsigned int bid) {
  // For easy reference
  auto local_buffer_var = (char*)copy_state_.buffers_[copy_id_][bid + 1];
  uint64_t local_buffer_offset_var =
      copy_state_.buffer_offsets_[copy_id_][bid + 1];
  int16_t empty = constants::empty_int16;

  // Fill an empty value
  std::memcpy(
      local_buffer_var + local_buffer_offset_var, &empty, sizeof(int16_t));
}

template <>
void ArrayOrderedWriteState::fill_with_empty_var<uint16_t>(unsigned int bid) {
  // For easy reference
  auto local_buffer_var = (char*)copy_state_.buffers_[copy_id_][bid + 1];
  uint64_t local_buffer_offset_var =
      copy_state_.buffer_offsets_[copy_id_][bid + 1];
  uint16_t empty = constants::empty_uint16;

  // Fill an empty value
  std::memcpy(
      local_buffer_var + local_buffer_offset_var, &empty, sizeof(uint16_t));
}

template <>
void ArrayOrderedWriteState::fill_with_empty_var<uint32_t>(unsigned int bid) {
  // For easy reference
  auto local_buffer_var = (char*)copy_state_.buffers_[copy_id_][bid + 1];
  uint64_t local_buffer_offset_var =
      copy_state_.buffer_offsets_[copy_id_][bid + 1];
  uint32_t empty = constants::empty_uint32;

  // Fill an empty value
  std::memcpy(
      local_buffer_var + local_buffer_offset_var, &empty, sizeof(uint32_t));
}

template <>
void ArrayOrderedWriteState::fill_with_empty_var<uint64_t>(unsigned int bid) {
  // For easy reference
  auto local_buffer_var = (char*)copy_state_.buffers_[copy_id_][bid + 1];
  uint64_t local_buffer_offset_var =
      copy_state_.buffer_offsets_[copy_id_][bid + 1];
  uint64_t empty = constants::empty_uint64;

  // Fill an empty value
  std::memcpy(
      local_buffer_var + local_buffer_offset_var, &empty, sizeof(uint64_t));
}

void ArrayOrderedWriteState::free_copy_state() {
  for (unsigned int i = 0; i < 2; ++i) {
    if (copy_state_.buffer_sizes_[i] != nullptr)
      delete[] copy_state_.buffer_sizes_[i];
    if (copy_state_.buffers_[i] != nullptr) {
      for (unsigned int b = 0; b < buffer_num_; ++b)
        std::free(copy_state_.buffers_[i][b]);
      std::free(copy_state_.buffers_[i]);
    }
  }
}

void ArrayOrderedWriteState::free_tile_slab_info() {
  // For easy reference
  auto anum = attribute_ids_.size();

  // Free
  for (auto& info : tile_slab_info_) {
    uint64_t tile_num = info.tile_num_;

    if (info.cell_offset_per_dim_ != nullptr) {
      for (uint64_t tile = 0; tile < tile_num; ++tile)
        delete[] info.cell_offset_per_dim_[tile];
      delete[] info.cell_offset_per_dim_;
    }

    for (size_t attr = 0; attr < anum; ++attr) {
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

    for (size_t attr = 0; attr < anum; ++attr) {
      if (info.start_offsets_[attr] != nullptr)
        delete[] info.start_offsets_[attr];
    }
    delete[] info.start_offsets_;

    delete[] info.tile_offset_per_dim_;
  }
}

void ArrayOrderedWriteState::free_tile_slab_state() {
  // For easy reference
  auto anum = attribute_ids_.size();

  // Clean up
  if (tile_slab_state_.current_coords_ != nullptr) {
    for (unsigned int i = 0; i < anum; ++i)
      std::free(tile_slab_state_.current_coords_[i]);
    delete[] tile_slab_state_.current_coords_;
  }

  delete[] tile_slab_state_.copy_tile_slab_done_;
  delete[] tile_slab_state_.current_offsets_;
  delete[] tile_slab_state_.current_tile_;
}

template <class T>
uint64_t ArrayOrderedWriteState::get_cell_id(unsigned int aid) {
  // For easy reference
  auto current_coords = (const T*)tile_slab_state_.current_coords_[aid];
  auto tile_extents =
      (const T*)query_->array_metadata()->domain()->tile_extents();
  uint64_t tid = tile_slab_state_.current_tile_[aid];
  uint64_t* cell_offset_per_dim =
      tile_slab_info_[copy_id_].cell_offset_per_dim_[tid];

  // Calculate cell id
  uint64_t cid = 0;
  for (unsigned int i = 0; i < dim_num_; ++i)
    cid += (current_coords[i] -
            (current_coords[i] / tile_extents[i]) * tile_extents[i]) *
           cell_offset_per_dim[i];

  // Return tile id
  return cid;
}

template <class T>
uint64_t ArrayOrderedWriteState::get_tile_id(unsigned int aid) {
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

bool ArrayOrderedWriteState::separate_fragments() const {
  Layout query_layout = query_->layout();
  Layout tile_order = query_->array_metadata()->tile_order();
  return (query_layout == Layout::COL_MAJOR &&
          tile_order == Layout::ROW_MAJOR) ||
         (query_layout == Layout::ROW_MAJOR && tile_order == Layout::COL_MAJOR);
}

void ArrayOrderedWriteState::init_copy_state() {
  for (unsigned int j = 0; j < 2; ++j) {
    copy_state_.buffer_offsets_[j] = new uint64_t[buffer_num_];
    copy_state_.buffer_sizes_[j] = new uint64_t[buffer_num_];
    copy_state_.buffers_[j] = new void*[buffer_num_];
    for (unsigned i = 0; i < buffer_num_; ++i) {
      copy_state_.buffer_offsets_[j][i] = 0;
      copy_state_.buffer_sizes_[j][i] = 0;
      copy_state_.buffers_[j][i] = nullptr;
    }
  }
}

void ArrayOrderedWriteState::init_tile_slab_info() {
  // For easy reference
  auto anum = attribute_ids_.size();

  // Initialize
  for (auto& info : tile_slab_info_) {
    info.cell_offset_per_dim_ = nullptr;
    info.cell_slab_size_ = new uint64_t*[anum];
    info.cell_slab_num_ = nullptr;
    info.range_overlap_ = nullptr;
    info.start_offsets_ = new uint64_t*[anum];
    info.tile_offset_per_dim_ = new uint64_t[dim_num_];

    for (size_t attr = 0; attr < anum; ++attr) {
      info.cell_slab_size_[attr] = nullptr;
      info.start_offsets_[attr] = nullptr;
    }
    info.tile_num_ = INVALID_UINT64;
  }
}

template <class T>
void ArrayOrderedWriteState::init_tile_slab_info(unsigned int id) {
  // Sanity check
  assert(query_->array_metadata()->dense());

  // For easy reference
  auto anum = attribute_ids_.size();

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
    for (uint64_t j = 0; j < tile_num; ++j)
      tile_slab_info_[id].start_offsets_[i][j] = 0;
  }

  tile_slab_info_[id].tile_num_ = tile_num;
}

void ArrayOrderedWriteState::init_tile_slab_state() {
  // For easy reference
  auto anum = attribute_ids_.size();

  // Allocations and initializations
  tile_slab_state_.copy_tile_slab_done_ = new bool[anum];
  for (unsigned int i = 0; i < anum; ++i)
    tile_slab_state_.copy_tile_slab_done_[i] = true;  // Important!

  tile_slab_state_.current_offsets_ = new uint64_t[anum];
  tile_slab_state_.current_coords_ = new void*[anum];
  tile_slab_state_.current_tile_ = new uint64_t[anum];

  for (unsigned int i = 0; i < anum; ++i) {
    tile_slab_state_.current_coords_[i] = std::malloc(coords_size_);
    tile_slab_state_.current_offsets_[i] = 0;
    tile_slab_state_.current_tile_[i] = 0;
  }
}

template <class T>
bool ArrayOrderedWriteState::next_tile_slab_col() {
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
bool ArrayOrderedWriteState::next_tile_slab_row() {
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
Status ArrayOrderedWriteState::write() {
  // For easy reference
  switch (query_->layout()) {
    case Layout::COL_MAJOR:
      return write_sorted_col<T>();
    case Layout::ROW_MAJOR:
      return write_sorted_row<T>();
    default:
      assert(false);
      return LOG_STATUS(Status::Error("Cannot write; Invalid query layout"));
  }
}

template <class T>
Status ArrayOrderedWriteState::write_sorted_col() {
  // For easy reference
  auto array_metadata = query_->array_metadata();
  auto subarray = static_cast<const T*>(subarray_);

  // Check if this can be satisfied with a default write
  if (array_metadata->cell_order() == Layout::COL_MAJOR &&
      !std::memcmp(subarray_, expanded_subarray_, 2 * coords_size_) &&
      array_metadata->domain()->is_contained_in_tile_slab_row<T>(subarray))
    return query_->write(buffers_, buffer_sizes_);

  // Iterate over each tile slab
  while (next_tile_slab_col<T>()) {
    async_wait(copy_id_);
    reset_tile_slab_state<T>();
    reset_copy_state();
    copy_tile_slab();
    async_wait_[copy_id_] = true;
    async_submit_query(copy_id_);
    copy_id_ = (copy_id_ + 1) % 2;
  }

  // Wait for last async query to finish
  async_wait((copy_id_ + 1) % 2);

  // Success
  return Status::Ok();
}

template <class T>
Status ArrayOrderedWriteState::write_sorted_row() {
  // For easy reference
  auto array_metadata = query_->array_metadata();
  auto subarray = static_cast<const T*>(subarray_);

  // Check if this can be satisfied with a default write
  if (array_metadata->cell_order() == Layout::ROW_MAJOR &&
      !std::memcmp(subarray_, expanded_subarray_, 2 * coords_size_) &&
      array_metadata->domain()->is_contained_in_tile_slab_col<T>(subarray))
    return query_->write(buffers_, buffer_sizes_);

  // Iterate over each tile slab
  while (next_tile_slab_row<T>()) {
    async_wait(copy_id_);
    reset_tile_slab_state<T>();
    reset_copy_state();
    copy_tile_slab();
    async_wait_[copy_id_] = true;
    async_submit_query(copy_id_);
    copy_id_ = (copy_id_ + 1) % 2;
  }

  // Wait for last async query to finish
  async_wait((copy_id_ + 1) % 2);

  // Success
  return Status::Ok();
}

void ArrayOrderedWriteState::reset_copy_state() {
  for (unsigned int i = 0; i < buffer_num_; ++i)
    copy_state_.buffer_offsets_[copy_id_][i] = 0;
}

template <class T>
void ArrayOrderedWriteState::reset_tile_coords() {
  auto tile_coords = (T*)tile_coords_;
  for (unsigned int i = 0; i < dim_num_; ++i)
    tile_coords[i] = 0;
}

template <class T>
void ArrayOrderedWriteState::reset_tile_slab_state() {
  // For easy reference
  auto anum = attribute_ids_.size();
  auto current_coords = (T**)tile_slab_state_.current_coords_;
  auto tile_slab = (const T*)tile_slab_norm_[copy_id_];

  // Reset values
  for (unsigned int i = 0; i < anum; ++i) {
    tile_slab_state_.copy_tile_slab_done_[i] = false;
    tile_slab_state_.current_tile_[i] = 0;
    for (unsigned int j = 0; j < dim_num_; ++j)
      current_coords[i][j] = tile_slab[2 * j];
  }
}

void ArrayOrderedWriteState::update_current_tile_and_offset(unsigned int aid) {
  // For easy reference
  Datatype coords_type = query_->array_metadata()->coords_type();

  // Invoke the proper templated function
  if (coords_type == Datatype::INT32)
    update_current_tile_and_offset<int>(aid);
  else if (coords_type == Datatype::INT64)
    update_current_tile_and_offset<int64_t>(aid);
  else if (coords_type == Datatype::FLOAT32)
    update_current_tile_and_offset<float>(aid);
  else if (coords_type == Datatype::FLOAT64)
    update_current_tile_and_offset<double>(aid);
  else if (coords_type == Datatype::INT8)
    update_current_tile_and_offset<int8_t>(aid);
  else if (coords_type == Datatype::UINT8)
    update_current_tile_and_offset<uint8_t>(aid);
  else if (coords_type == Datatype::INT16)
    update_current_tile_and_offset<int16_t>(aid);
  else if (coords_type == Datatype::UINT16)
    update_current_tile_and_offset<uint16_t>(aid);
  else if (coords_type == Datatype::UINT32)
    update_current_tile_and_offset<uint32_t>(aid);
  else if (coords_type == Datatype::UINT64)
    update_current_tile_and_offset<uint64_t>(aid);
  else
    assert(false);
}

template <class T>
void ArrayOrderedWriteState::update_current_tile_and_offset(unsigned int aid) {
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

template Status ArrayOrderedWriteState::write_sorted_col<int>();
template Status ArrayOrderedWriteState::write_sorted_col<int64_t>();
template Status ArrayOrderedWriteState::write_sorted_col<int8_t>();
template Status ArrayOrderedWriteState::write_sorted_col<uint8_t>();
template Status ArrayOrderedWriteState::write_sorted_col<int16_t>();
template Status ArrayOrderedWriteState::write_sorted_col<uint16_t>();
template Status ArrayOrderedWriteState::write_sorted_col<uint32_t>();
template Status ArrayOrderedWriteState::write_sorted_col<uint64_t>();

template Status ArrayOrderedWriteState::write_sorted_row<int>();
template Status ArrayOrderedWriteState::write_sorted_row<int64_t>();
template Status ArrayOrderedWriteState::write_sorted_row<int8_t>();
template Status ArrayOrderedWriteState::write_sorted_row<uint8_t>();
template Status ArrayOrderedWriteState::write_sorted_row<int16_t>();
template Status ArrayOrderedWriteState::write_sorted_row<uint16_t>();
template Status ArrayOrderedWriteState::write_sorted_row<uint32_t>();
template Status ArrayOrderedWriteState::write_sorted_row<uint64_t>();

}  // namespace tiledb
