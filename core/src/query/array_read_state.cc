/**
 * @file   array_read_state.cc
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
 * This file implements the ArrayReadState class.
 */

#include "array_read_state.h"
#include "logger.h"
#include "pq_fragment_cell_range.h"
#include "smaller_pq_fragment_cell_range.h"

#include <cassert>

/* ****************************** */
/*             MACROS             */
/* ****************************** */

#define MIN(a, b) ((a) < (b) ? (a) : (b))
#define MAX(a, b) ((a) > (b) ? (a) : (b))

namespace tiledb {

/* ****************************** */
/*        STATIC CONSTANTS        */
/* ****************************** */

const uint64_t ArrayReadState::INVALID_UINT64 = UINT64_MAX;
const unsigned int ArrayReadState::INVALID_UINT = UINT_MAX;

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

ArrayReadState::ArrayReadState(Query* query)
    : query_(query) {
  // For easy reference
  array_metadata_ = query->array_metadata();
  attribute_num_ = array_metadata_->attribute_num();
  coords_size_ = array_metadata_->coords_size();

  // Initializations
  done_ = false;
  empty_cells_written_.resize(attribute_num_ + 1);
  fragment_cell_pos_ranges_vec_pos_.resize(attribute_num_ + 1);
  min_bounding_coords_end_ = nullptr;
  read_round_done_.resize(attribute_num_);
  subarray_tile_coords_ = nullptr;
  subarray_tile_domain_ = nullptr;

  for (unsigned int i = 0; i < attribute_num_ + 1; ++i) {
    empty_cells_written_[i] = 0;
    fragment_cell_pos_ranges_vec_pos_[i] = 0;
    read_round_done_[i] = true;
  }

  // Get fragment read states
  std::vector<Fragment*> fragments = query_->fragments();
  fragment_num_ = (unsigned int)fragments.size();
  fragment_read_states_.resize(fragment_num_);
  for (unsigned int i = 0; i < fragment_num_; ++i)
    fragment_read_states_[i] = fragments[i]->read_state();

  tile_coords_aux_ = std::malloc(coords_size_);
}

ArrayReadState::~ArrayReadState() {
  if (min_bounding_coords_end_ != nullptr)
    std::free(min_bounding_coords_end_);

  if (subarray_tile_coords_ != nullptr)
    std::free(subarray_tile_coords_);

  if (subarray_tile_domain_ != nullptr)
    std::free(subarray_tile_domain_);

  auto fragment_bounding_coords_num =
      (unsigned int)fragment_bounding_coords_.size();
  for (unsigned int i = 0; i < fragment_bounding_coords_num; ++i)
    if (fragment_bounding_coords_[i] != nullptr)
      std::free(fragment_bounding_coords_[i]);

  uint64_t fragment_cell_pos_ranges_vec_size =
      fragment_cell_pos_ranges_vec_.size();
  for (uint64_t i = 0; i < fragment_cell_pos_ranges_vec_size; ++i)
    delete fragment_cell_pos_ranges_vec_[i];

  if (tile_coords_aux_ != nullptr)
    std::free(tile_coords_aux_);
}

/* ****************************** */
/*              API               */
/* ****************************** */

bool ArrayReadState::overflow() const {
  auto attribute_num = (unsigned int)query_->attribute_ids().size();
  for (unsigned int i = 0; i < attribute_num; ++i)
    if (overflow_[i])
      return true;

  return false;
}

bool ArrayReadState::overflow(unsigned int attribute_id) const {
  return overflow_[attribute_id];
}

Status ArrayReadState::read(void** buffers, uint64_t* buffer_sizes) {
  // Sanity check
  assert(fragment_num_);

  // Reset overflow
  overflow_.resize(attribute_num_ + 1);
  for (unsigned int i = 0; i < attribute_num_ + 1; ++i)
    overflow_[i] = false;

  for (unsigned int i = 0; i < fragment_num_; ++i)
    fragment_read_states_[i]->reset_overflow();

  if (array_metadata_->dense())  // DENSE
    return read_dense(buffers, buffer_sizes);
  return read_sparse(buffers, buffer_sizes);  // SPARSE
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

void ArrayReadState::clean_up_processed_fragment_cell_pos_ranges() {
  // Find the minimum overlapping tile position across all attributes
  auto& attribute_ids = query_->attribute_ids();
  auto attribute_id_num = (unsigned int)attribute_ids.size();
  uint64_t min_pos = fragment_cell_pos_ranges_vec_pos_[0];
  for (unsigned int i = 1; i < attribute_id_num; ++i)
    if (fragment_cell_pos_ranges_vec_pos_[attribute_ids[i]] < min_pos)
      min_pos = fragment_cell_pos_ranges_vec_pos_[attribute_ids[i]];

  // Clean up processed overlapping tiles
  if (min_pos != 0) {
    // Remove overlapping tile elements from the vector
    for (uint64_t i = 0; i < min_pos; ++i)
      delete fragment_cell_pos_ranges_vec_[i];
    auto it_first = fragment_cell_pos_ranges_vec_.begin();
    auto it_last = it_first + min_pos;
    fragment_cell_pos_ranges_vec_.erase(it_first, it_last);

    // Update the positions
    for (unsigned int i = 0; i < attribute_num_ + 1; ++i)
      if (fragment_cell_pos_ranges_vec_pos_[i] != 0)
        fragment_cell_pos_ranges_vec_pos_[i] -= min_pos;
  }
}

template <class T>
Status ArrayReadState::compute_fragment_cell_pos_ranges(
    FragmentCellRanges* fragment_cell_ranges,
    FragmentCellPosRanges* fragment_cell_pos_ranges) const {
  // For easy reference
  auto dim_num = array_metadata_->dim_num();
  auto fragment_cell_ranges_num = (uint64_t)fragment_cell_ranges->size();
  Status st = Status::Ok();
  auto domain = array_metadata_->domain();

  // Compute fragment cell position ranges
  for (uint64_t i = 0; i < fragment_cell_ranges_num; ++i) {
    unsigned int fragment_id = (*fragment_cell_ranges)[i].first.first;
    if (fragment_id == INVALID_UINT ||
        fragment_read_states_[fragment_id]->dense()) {  // DENSE
      // Create a new fragment cell position range
      FragmentCellPosRange fragment_cell_pos_range;
      fragment_cell_pos_range.first = (*fragment_cell_ranges)[i].first;
      CellPosRange& cell_pos_range = fragment_cell_pos_range.second;
      auto cell_range = static_cast<T*>((*fragment_cell_ranges)[i].second);
      st = domain->get_cell_pos(cell_range, &cell_pos_range.first);
      if (!st.ok())
        break;
      st = domain->get_cell_pos(&cell_range[dim_num], &cell_pos_range.second);
      if (!st.ok())
        break;

      // Insert into the result
      fragment_cell_pos_ranges->push_back(fragment_cell_pos_range);
    } else {  // SPARSE
      // Create a new fragment cell position range
      FragmentCellPosRange fragment_cell_pos_range;
      st = fragment_read_states_[(*fragment_cell_ranges)[i].first.first]
               ->get_fragment_cell_pos_range_sparse<T>(
                   (*fragment_cell_ranges)[i].first,
                   static_cast<T*>((*fragment_cell_ranges)[i].second),
                   &fragment_cell_pos_range);
      if (!st.ok())
        break;

      // Insert into the result only valid fragment cell position ranges
      if (fragment_cell_pos_range.second.first != INVALID_UINT64)
        fragment_cell_pos_ranges->push_back(fragment_cell_pos_range);
    }

    // Clean up corresponding input cell range
    std::free((*fragment_cell_ranges)[i].second);
    (*fragment_cell_ranges)[i].second = nullptr;
  }

  // Clean up
  for (uint64_t i = 0; i < fragment_cell_ranges_num; ++i)
    std::free((*fragment_cell_ranges)[i].second);
  fragment_cell_ranges->clear();
  if (!st.ok())
    fragment_cell_pos_ranges->clear();

  // Return
  return st;
}

template <class T>
void ArrayReadState::compute_min_bounding_coords_end() {
  // For easy reference
  auto dim_num = array_metadata_->dim_num();

  // Allocate memory
  if (min_bounding_coords_end_ == nullptr)
    min_bounding_coords_end_ = std::malloc(coords_size_);
  auto min_bounding_coords_end = static_cast<T*>(min_bounding_coords_end_);

  // Compute min bounding coords end
  bool first = true;
  for (unsigned int i = 0; i < fragment_num_; ++i) {
    auto fragment_bounding_coords =
        static_cast<T*>(fragment_bounding_coords_[i]);
    if (fragment_bounding_coords != nullptr) {
      if (first) {
        std::memcpy(
            min_bounding_coords_end,
            &fragment_bounding_coords[dim_num],
            coords_size_);
        first = false;
      } else if (
          array_metadata_->domain()->tile_cell_order_cmp(
              &fragment_bounding_coords[dim_num],
              min_bounding_coords_end,
              (T*)tile_coords_aux_) < 0) {
        std::memcpy(
            min_bounding_coords_end,
            &fragment_bounding_coords[dim_num],
            coords_size_);
      }
    }
  }
}

template <class T>
Status ArrayReadState::compute_unsorted_fragment_cell_ranges_dense(
    std::vector<FragmentCellRanges>* unsorted_fragment_cell_ranges) {
  // Compute cell ranges for all fragments
  for (unsigned int i = 0; i < fragment_num_; ++i) {
    if (!fragment_read_states_[i]->done()) {
      if (fragment_read_states_[i]->dense()) {  // DENSE
        // Get fragment cell ranges
        FragmentCellRanges fragment_cell_ranges;
        RETURN_NOT_OK(
            fragment_read_states_[i]->get_fragment_cell_ranges_dense<T>(
                i, &fragment_cell_ranges));
        // Insert fragment cell ranges to the result
        unsorted_fragment_cell_ranges->push_back(fragment_cell_ranges);
      } else {  // SPARSE
        FragmentCellRanges fragment_cell_ranges;
        FragmentCellRanges fragment_cell_ranges_tmp;
        do {
          // Get next overlapping tiles
          fragment_read_states_[i]->get_next_overlapping_tile_sparse<T>(
              static_cast<const T*>(subarray_tile_coords_));
          // Get fragment cell ranges
          fragment_cell_ranges_tmp.clear();
          RETURN_NOT_OK(
              fragment_read_states_[i]->get_fragment_cell_ranges_sparse<T>(
                  i, &fragment_cell_ranges_tmp));
          // Insert fragment cell ranges to temporary ranges
          fragment_cell_ranges.insert(
              fragment_cell_ranges.end(),
              fragment_cell_ranges_tmp.begin(),
              fragment_cell_ranges_tmp.end());
        } while (!fragment_read_states_[i]->done() &&
                 fragment_read_states_[i]->mbr_overlaps_tile());
        unsorted_fragment_cell_ranges->push_back(fragment_cell_ranges);
      }
    } else {
      // Append an empty list
      unsorted_fragment_cell_ranges->emplace_back();
    }
  }

  // Check if some dense fragment completely covers the subarray
  bool subarray_area_covered = false;
  for (unsigned int i = 0; i < fragment_num_; ++i) {
    if (!fragment_read_states_[i]->done() &&
        fragment_read_states_[i]->dense() &&
        fragment_read_states_[i]->subarray_area_covered()) {
      subarray_area_covered = true;
      break;
    }
  }

  // Add a fragment that accounts for the empty areas of the array
  if (!subarray_area_covered)
    unsorted_fragment_cell_ranges->push_back(empty_fragment_cell_ranges<T>());

  // Success
  return Status::Ok();
}

template <class T>
Status ArrayReadState::compute_unsorted_fragment_cell_ranges_sparse(
    std::vector<FragmentCellRanges>* unsorted_fragment_cell_ranges) {
  // For easy reference
  auto dim_num = array_metadata_->dim_num();
  auto min_bounding_coords_end = static_cast<T*>(min_bounding_coords_end_);

  // Compute the relevant fragment cell ranges
  for (unsigned int i = 0; i < fragment_num_; ++i) {
    auto fragment_bounding_coords =
        static_cast<T*>(fragment_bounding_coords_[i]);

    // Compute new fragment cell ranges
    if (fragment_bounding_coords != nullptr &&
        array_metadata_->domain()->tile_cell_order_cmp(
            fragment_bounding_coords,
            min_bounding_coords_end,
            (T*)tile_coords_aux_) <= 0) {
      FragmentCellRanges fragment_cell_ranges;
      RETURN_NOT_OK(
          fragment_read_states_[i]->get_fragment_cell_ranges_sparse<T>(
              i,
              fragment_bounding_coords,
              min_bounding_coords_end,
              &fragment_cell_ranges));
      unsorted_fragment_cell_ranges->push_back(fragment_cell_ranges);

      // If the end bounding coordinate is not the same as the smallest one,
      // update the start bounding coordinate to exceed the smallest
      // end bounding coordinates
      if (std::memcmp(
              &fragment_bounding_coords[dim_num],
              min_bounding_coords_end,
              coords_size_) != 0) {
        // Get the first coordinates AFTER the min bounding coords end
        bool coords_retrieved;
        RETURN_NOT_OK(fragment_read_states_[i]->get_coords_after<T>(
            min_bounding_coords_end,
            fragment_bounding_coords,
            &coords_retrieved));
        // Sanity check for the sparse case
        assert(coords_retrieved);
      }
    } else {
      // Append an empty list
      unsorted_fragment_cell_ranges->emplace_back();
    }
  }

  // Success
  return Status::Ok();
}

Status ArrayReadState::copy_cells(
    unsigned int attribute_id,
    void* buffer,
    uint64_t buffer_size,
    uint64_t* buffer_offset) {
  // For easy reference
  Datatype type = array_metadata_->type(attribute_id);

  if (type == Datatype::INT32) {
    int32_t val = constants::empty_int32;
    return copy_cells_generic(
        attribute_id,
        buffer,
        buffer_size,
        buffer_offset,
        &val,
        sizeof(int32_t));
  }

  if (type == Datatype::INT64) {
    int64_t val = constants::empty_int64;
    return copy_cells_generic(
        attribute_id,
        buffer,
        buffer_size,
        buffer_offset,
        &val,
        sizeof(int64_t));
  }

  if (type == Datatype::FLOAT32) {
    float_t val = constants::empty_float32;
    return copy_cells_generic(
        attribute_id,
        buffer,
        buffer_size,
        buffer_offset,
        &val,
        sizeof(float_t));
  }

  if (type == Datatype::FLOAT64) {
    double_t val = constants::empty_float64;
    return copy_cells_generic(
        attribute_id,
        buffer,
        buffer_size,
        buffer_offset,
        &val,
        sizeof(double_t));
  }

  if (type == Datatype::CHAR) {
    char val = constants::empty_char;
    return copy_cells_generic(
        attribute_id, buffer, buffer_size, buffer_offset, &val, sizeof(char));
  }

  if (type == Datatype::INT8) {
    int8_t val = constants::empty_int8;
    return copy_cells_generic(
        attribute_id, buffer, buffer_size, buffer_offset, &val, sizeof(int8_t));
  }

  if (type == Datatype::UINT8) {
    uint8_t val = constants::empty_uint8;
    return copy_cells_generic(
        attribute_id,
        buffer,
        buffer_size,
        buffer_offset,
        &val,
        sizeof(uint8_t));
  }

  if (type == Datatype::INT16) {
    int16_t val = constants::empty_int16;
    return copy_cells_generic(
        attribute_id,
        buffer,
        buffer_size,
        buffer_offset,
        &val,
        sizeof(int16_t));
  }

  if (type == Datatype::UINT16) {
    uint16_t val = constants::empty_uint16;
    return copy_cells_generic(
        attribute_id,
        buffer,
        buffer_size,
        buffer_offset,
        &val,
        sizeof(uint16_t));
  }

  if (type == Datatype::UINT32) {
    uint32_t val = constants::empty_uint32;
    return copy_cells_generic(
        attribute_id,
        buffer,
        buffer_size,
        buffer_offset,
        &val,
        sizeof(uint32_t));
  }

  if (type == Datatype::UINT64) {
    uint64_t val = constants::empty_uint64;
    return copy_cells_generic(
        attribute_id,
        buffer,
        buffer_size,
        buffer_offset,
        &val,
        sizeof(uint64_t));
  }

  // Error
  return LOG_STATUS(Status::ARSError("Invalid datatype when copying cells"));
}

Status ArrayReadState::copy_cells_generic(
    unsigned int attribute_id,
    void* buffer,
    uint64_t buffer_size,
    uint64_t* buffer_offset,
    const void* empty_type_value,
    const uint64_t empty_type_size) {
  // For easy reference
  uint64_t pos = fragment_cell_pos_ranges_vec_pos_[attribute_id];
  FragmentCellPosRanges& fragment_cell_pos_ranges =
      *fragment_cell_pos_ranges_vec_[pos];
  auto fragment_cell_pos_ranges_num = (uint64_t)fragment_cell_pos_ranges.size();

  // Sanity check
  assert(!array_metadata_->var_size(attribute_id));

  // Copy the cell ranges one by one
  for (uint64_t i = 0; i < fragment_cell_pos_ranges_num; ++i) {
    unsigned int fragment_id = fragment_cell_pos_ranges[i].first.first;
    uint64_t tile_pos = fragment_cell_pos_ranges[i].first.second;
    CellPosRange& cell_pos_range = fragment_cell_pos_ranges[i].second;

    // Handle empty fragment
    if (fragment_id == INVALID_UINT) {
      copy_cells_with_empty_generic(
          attribute_id,
          buffer,
          buffer_size,
          buffer_offset,
          cell_pos_range,
          empty_type_value,
          empty_type_size);
      if (overflow_[attribute_id])
        break;

      continue;
    }

    // Handle non-empty fragment
    RETURN_NOT_OK(fragment_read_states_[fragment_id]->copy_cells(
        attribute_id,
        tile_pos,
        buffer,
        buffer_size,
        buffer_offset,
        cell_pos_range));

    // Handle overflow
    if (fragment_read_states_[fragment_id]->overflow(attribute_id)) {
      overflow_[attribute_id] = true;
      break;
    }
  }

  // Handle the case the read round is done for this attribute
  if (!overflow_[attribute_id]) {
    ++fragment_cell_pos_ranges_vec_pos_[attribute_id];
    read_round_done_[attribute_id] = true;
  } else {
    read_round_done_[attribute_id] = false;
  }

  // Success
  return Status::Ok();
}

Status ArrayReadState::copy_cells_var(
    unsigned int attribute_id,
    void* buffer,
    uint64_t buffer_size,
    uint64_t* buffer_offset,
    void* buffer_var,
    uint64_t buffer_var_size,
    uint64_t* buffer_var_offset) {
  // For easy reference
  Datatype type = array_metadata_->type(attribute_id);

  if (type == Datatype::INT32) {
    int32_t val = constants::empty_int32;
    return copy_cells_var_generic(
        attribute_id,
        buffer,
        buffer_size,
        buffer_offset,
        buffer_var,
        buffer_var_size,
        buffer_var_offset,
        &val,
        sizeof(int32_t));
  }

  if (type == Datatype::INT64) {
    int64_t val = constants::empty_int64;
    return copy_cells_var_generic(
        attribute_id,
        buffer,
        buffer_size,
        buffer_offset,
        buffer_var,
        buffer_var_size,
        buffer_var_offset,
        &val,
        sizeof(int64_t));
  }

  if (type == Datatype::FLOAT32) {
    float_t val = constants::empty_float32;
    return copy_cells_var_generic(
        attribute_id,
        buffer,
        buffer_size,
        buffer_offset,
        buffer_var,
        buffer_var_size,
        buffer_var_offset,
        &val,
        sizeof(float_t));
  }

  if (type == Datatype::FLOAT64) {
    double_t val = constants::empty_float64;
    return copy_cells_var_generic(
        attribute_id,
        buffer,
        buffer_size,
        buffer_offset,
        buffer_var,
        buffer_var_size,
        buffer_var_offset,
        &val,
        sizeof(double_t));
  }

  if (type == Datatype::CHAR) {
    char val = constants::empty_char;
    return copy_cells_var_generic(
        attribute_id,
        buffer,
        buffer_size,
        buffer_offset,
        buffer_var,
        buffer_var_size,
        buffer_var_offset,
        &val,
        sizeof(char));
  }

  if (type == Datatype::INT8) {
    int8_t val = constants::empty_int8;
    return copy_cells_var_generic(
        attribute_id,
        buffer,
        buffer_size,
        buffer_offset,
        buffer_var,
        buffer_var_size,
        buffer_var_offset,
        &val,
        sizeof(int8_t));
  }

  if (type == Datatype::UINT8) {
    uint8_t val = constants::empty_uint8;
    return copy_cells_var_generic(
        attribute_id,
        buffer,
        buffer_size,
        buffer_offset,
        buffer_var,
        buffer_var_size,
        buffer_var_offset,
        &val,
        sizeof(uint8_t));
  }

  if (type == Datatype::INT16) {
    int16_t val = constants::empty_int16;
    return copy_cells_var_generic(
        attribute_id,
        buffer,
        buffer_size,
        buffer_offset,
        buffer_var,
        buffer_var_size,
        buffer_var_offset,
        &val,
        sizeof(int16_t));
  }

  if (type == Datatype::UINT16) {
    uint16_t val = constants::empty_uint16;
    return copy_cells_var_generic(
        attribute_id,
        buffer,
        buffer_size,
        buffer_offset,
        buffer_var,
        buffer_var_size,
        buffer_var_offset,
        &val,
        sizeof(uint16_t));
  }

  if (type == Datatype::UINT32) {
    uint32_t val = constants::empty_uint32;
    return copy_cells_var_generic(
        attribute_id,
        buffer,
        buffer_size,
        buffer_offset,
        buffer_var,
        buffer_var_size,
        buffer_var_offset,
        &val,
        sizeof(uint32_t));
  }

  if (type == Datatype::UINT64) {
    uint64_t val = constants::empty_uint64;
    return copy_cells_var_generic(
        attribute_id,
        buffer,
        buffer_size,
        buffer_offset,
        buffer_var,
        buffer_var_size,
        buffer_var_offset,
        &val,
        sizeof(uint64_t));
  }

  // Error
  return LOG_STATUS(Status::ARSError("Invalid datatype when copying cells"));
}

Status ArrayReadState::copy_cells_var_generic(
    unsigned int attribute_id,
    void* buffer,
    uint64_t buffer_size,
    uint64_t* buffer_offset,
    void* buffer_var,
    uint64_t buffer_var_size,
    uint64_t* buffer_var_offset,
    const void* empty_type_value,
    uint64_t empty_type_size) {
  // For easy reference
  uint64_t pos = fragment_cell_pos_ranges_vec_pos_[attribute_id];
  FragmentCellPosRanges& fragment_cell_pos_ranges =
      *fragment_cell_pos_ranges_vec_[pos];
  auto fragment_cell_pos_ranges_num = (uint64_t)fragment_cell_pos_ranges.size();

  // Sanity check
  assert(array_metadata_->var_size(attribute_id));

  // Copy the cell ranges one by one
  for (uint64_t i = 0; i < fragment_cell_pos_ranges_num; ++i) {
    unsigned int fragment_id = fragment_cell_pos_ranges[i].first.first;
    uint64_t tile_pos = fragment_cell_pos_ranges[i].first.second;
    CellPosRange& cell_pos_range = fragment_cell_pos_ranges[i].second;

    // Handle empty fragment
    if (fragment_id == INVALID_UINT) {
      copy_cells_with_empty_var_generic(
          attribute_id,
          buffer,
          buffer_size,
          buffer_offset,
          buffer_var,
          buffer_var_size,
          buffer_var_offset,
          cell_pos_range,
          empty_type_value,
          empty_type_size);
      if (overflow_[attribute_id])
        break;

      continue;
    }

    // Handle non-empty fragment
    RETURN_NOT_OK(fragment_read_states_[fragment_id]->copy_cells_var(
        attribute_id,
        tile_pos,
        buffer,
        buffer_size,
        buffer_offset,
        buffer_var,
        buffer_var_size,
        buffer_var_offset,
        cell_pos_range));

    // Handle overflow
    if (fragment_read_states_[fragment_id]->overflow(attribute_id)) {
      overflow_[attribute_id] = true;
      break;
    }
  }

  // Handle the case the read round is done for this attribute
  if (!overflow_[attribute_id]) {
    ++fragment_cell_pos_ranges_vec_pos_[attribute_id];
    read_round_done_[attribute_id] = true;
  } else {
    read_round_done_[attribute_id] = false;
  }

  // Success
  return Status::Ok();
}

void ArrayReadState::copy_cells_with_empty_generic(
    unsigned int attribute_id,
    void* buffer,
    uint64_t buffer_size,
    uint64_t* buffer_offset,
    const CellPosRange& cell_pos_range,
    const void* empty_type_value,
    const uint64_t empty_type_size) {
  // For easy reference
  uint64_t cell_size = array_metadata_->cell_size(attribute_id);
  auto buffer_c = static_cast<char*>(buffer);
  auto cell_val_num = array_metadata_->cell_val_num(attribute_id);

  // Calculate free space in buffer
  uint64_t buffer_free_space = buffer_size - *buffer_offset;
  buffer_free_space = (buffer_free_space / cell_size) * cell_size;
  if (buffer_free_space == 0) {  // Overflow
    overflow_[attribute_id] = true;
    return;
  }

  // Sanity check
  assert(!array_metadata_->var_size(attribute_id));

  // Calculate number of empty cells to write
  uint64_t cell_num_in_range = cell_pos_range.second - cell_pos_range.first + 1;
  uint64_t cell_num_left_to_copy =
      cell_num_in_range - empty_cells_written_[attribute_id];
  uint64_t bytes_left_to_copy = cell_num_left_to_copy * cell_size;
  uint64_t bytes_to_copy = MIN(bytes_left_to_copy, buffer_free_space);
  uint64_t cell_num_to_copy = bytes_to_copy / cell_size;

  // Copy empty cells to buffer
  for (uint64_t i = 0; i < cell_num_to_copy; ++i) {
    for (unsigned int j = 0; j < cell_val_num; ++j) {
      std::memcpy(buffer_c + *buffer_offset, empty_type_value, empty_type_size);
      *buffer_offset += empty_type_size;
    }
  }
  empty_cells_written_[attribute_id] += cell_num_to_copy;

  // Handle buffer overflow
  if (empty_cells_written_[attribute_id] != cell_num_in_range) {
    overflow_[attribute_id] = true;
  } else {  // Done copying this range
    empty_cells_written_[attribute_id] = 0;
  }
}

void ArrayReadState::copy_cells_with_empty_var_generic(
    unsigned int attribute_id,
    void* buffer,
    uint64_t buffer_size,
    uint64_t* buffer_offset,
    void* buffer_var,
    uint64_t buffer_var_size,
    uint64_t* buffer_var_offset,
    const CellPosRange& cell_pos_range,
    const void* empty_type_value,
    uint64_t empty_type_size) {
  // For easy reference
  uint64_t cell_size = constants::cell_var_offset_size;
  uint64_t cell_size_var = sizeof(int);
  auto buffer_c = static_cast<char*>(buffer);
  auto buffer_var_c = static_cast<char*>(buffer_var);

  // Calculate free space in buffer
  uint64_t buffer_free_space = buffer_size - *buffer_offset;
  buffer_free_space = (buffer_free_space / cell_size) * cell_size;
  uint64_t buffer_var_free_space = buffer_var_size - *buffer_var_offset;
  buffer_var_free_space =
      (buffer_var_free_space / cell_size_var) * cell_size_var;

  // Handle overflow
  if (buffer_free_space == 0 || buffer_var_free_space == 0) {  // Overflow
    overflow_[attribute_id] = true;
    return;
  }

  // Sanity check
  assert(array_metadata_->var_size(attribute_id));

  // Calculate cell number to copy
  uint64_t cell_num_in_range = cell_pos_range.second - cell_pos_range.first + 1;
  uint64_t cell_num_left_to_copy =
      cell_num_in_range - empty_cells_written_[attribute_id];
  uint64_t bytes_left_to_copy = cell_num_left_to_copy * cell_size;
  uint64_t bytes_left_to_copy_var = cell_num_left_to_copy * cell_size_var;
  uint64_t bytes_to_copy = MIN(bytes_left_to_copy, buffer_free_space);
  uint64_t bytes_to_copy_var =
      MIN(bytes_left_to_copy_var, buffer_var_free_space);
  uint64_t cell_num_to_copy = bytes_to_copy / cell_size;
  uint64_t cell_num_to_copy_var = bytes_to_copy_var / cell_size_var;
  cell_num_to_copy = MIN(cell_num_to_copy, cell_num_to_copy_var);

  // Copy empty cells to buffers
  for (uint64_t i = 0; i < cell_num_to_copy; ++i) {
    std::memcpy(buffer_c + *buffer_offset, buffer_var_offset, cell_size);
    *buffer_offset += cell_size;
    std::memcpy(
        buffer_var_c + *buffer_var_offset, empty_type_value, empty_type_size);
    *buffer_var_offset += empty_type_size;
  }
  empty_cells_written_[attribute_id] += cell_num_to_copy;

  // Handle buffer overflow
  if (empty_cells_written_[attribute_id] != cell_num_in_range) {
    overflow_[attribute_id] = true;
  } else {  // Done copying this range
    empty_cells_written_[attribute_id] = 0;
  }
}

template <class T>
ArrayReadState::FragmentCellRanges ArrayReadState::empty_fragment_cell_ranges()
    const {
  // For easy reference
  auto dim_num = array_metadata_->dim_num();
  Layout cell_order = array_metadata_->cell_order();
  uint64_t cell_range_size = 2 * coords_size_;
  auto subarray = static_cast<const T*>(query_->subarray());
  auto tile_coords = (const T*)subarray_tile_coords_;
  auto domain = array_metadata_->domain();

  // To return
  FragmentInfo fragment_info = FragmentInfo(INVALID_UINT, INVALID_UINT64);
  FragmentCellRanges fragment_cell_ranges;

  // Compute the tile subarray
  auto tile_subarray = new T[2 * dim_num];
  domain->get_tile_subarray(tile_coords, tile_subarray);

  // Compute overlap of tile subarray with non-empty fragment domain
  auto query_tile_overlap_subarray = new T[2 * dim_num];
  auto overlap = domain->subarray_overlap(
      subarray, tile_subarray, query_tile_overlap_subarray);

  // Contiguous cells, single cell range
  if (overlap == 1 || overlap == 3) {
    void* cell_range = std::malloc(cell_range_size);
    auto cell_range_T = static_cast<T*>(cell_range);
    for (unsigned int i = 0; i < dim_num; ++i) {
      cell_range_T[i] = query_tile_overlap_subarray[2 * i];
      cell_range_T[dim_num + i] = query_tile_overlap_subarray[2 * i + 1];
    }

    // Insert the new range into the result vector
    fragment_cell_ranges.emplace_back(fragment_info, cell_range);
  } else {  // Non-contiguous cells, multiple ranges
    // Initialize the coordinates at the beginning of the global range
    auto coords = new T[dim_num];
    for (unsigned int i = 0; i < dim_num; ++i)
      coords[i] = query_tile_overlap_subarray[2 * i];

    // Handle the different cell orders
    unsigned int i;
    if (cell_order == Layout::ROW_MAJOR) {  // ROW
      while (coords[0] <= query_tile_overlap_subarray[1]) {
        // Make a cell range representing a slab
        void* cell_range = std::malloc(cell_range_size);
        auto cell_range_T = static_cast<T*>(cell_range);
        for (unsigned int j = 0; j < dim_num - 1; ++j) {
          cell_range_T[j] = coords[j];
          cell_range_T[dim_num + j] = coords[j];
        }
        cell_range_T[dim_num - 1] =
            query_tile_overlap_subarray[2 * (dim_num - 1)];
        cell_range_T[2 * dim_num - 1] =
            query_tile_overlap_subarray[2 * (dim_num - 1) + 1];

        // Insert the new range into the result vector
        fragment_cell_ranges.emplace_back(fragment_info, cell_range);

        // Advance coordinates
        i = dim_num - 2;
        ++coords[i];
        while (i > 0 && coords[i] > query_tile_overlap_subarray[2 * i + 1]) {
          coords[i] = query_tile_overlap_subarray[2 * i];
          ++coords[--i];
        }
      }
    } else if (cell_order == Layout::COL_MAJOR) {  // COLUMN
      while (coords[dim_num - 1] <=
             query_tile_overlap_subarray[2 * (dim_num - 1) + 1]) {
        // Make a cell range representing a slab
        void* cell_range = std::malloc(cell_range_size);
        auto cell_range_T = static_cast<T*>(cell_range);
        for (int j = dim_num - 1; j > 0; --j) {
          cell_range_T[j] = coords[j];
          cell_range_T[dim_num + j] = coords[j];
        }
        cell_range_T[0] = query_tile_overlap_subarray[0];
        cell_range_T[dim_num] = query_tile_overlap_subarray[1];

        // Insert the new range into the result vector
        fragment_cell_ranges.emplace_back(fragment_info, cell_range);

        // Advance coordinates
        i = 1;
        ++coords[i];
        while (i < dim_num - 1 &&
               coords[i] > query_tile_overlap_subarray[2 * i + 1]) {
          coords[i] = query_tile_overlap_subarray[2 * i];
          ++coords[++i];
        }
      }
    } else {
      assert(0);
    }

    // Clean up
    delete[] coords;
  }

  // Clean up
  delete[] tile_subarray;
  delete[] query_tile_overlap_subarray;

  // Return
  return fragment_cell_ranges;
}

template <class T>
Status ArrayReadState::get_next_fragment_cell_ranges_dense() {
  // Trivial case
  if (done_)
    return Status::Ok();

  // Get the next overlapping tile for each fragment
  get_next_overlapping_tiles_dense<T>();

  // Return if there are no more overlapping tiles
  if (done_)
    return Status::Ok();

  // Compute the unsorted fragment cell ranges needed for this read run
  std::vector<FragmentCellRanges> unsorted_fragment_cell_ranges;
  RETURN_NOT_OK(compute_unsorted_fragment_cell_ranges_dense<T>(
      &unsorted_fragment_cell_ranges));

  // Sort fragment cell ranges
  FragmentCellRanges fragment_cell_ranges;
  RETURN_NOT_OK(sort_fragment_cell_ranges<T>(
      &unsorted_fragment_cell_ranges, &fragment_cell_ranges));

  // Compute the fragment cell position ranges
  auto fragment_cell_pos_ranges = new FragmentCellPosRanges();
  RETURN_NOT_OK(compute_fragment_cell_pos_ranges<T>(
      &fragment_cell_ranges, fragment_cell_pos_ranges));

  // Insert cell pos ranges in the state
  fragment_cell_pos_ranges_vec_.push_back(fragment_cell_pos_ranges);

  // Clean up processed overlapping tiles
  clean_up_processed_fragment_cell_pos_ranges();

  // Success
  return Status::Ok();
}

template <class T>
Status ArrayReadState::get_next_fragment_cell_ranges_sparse() {
  // Trivial case
  if (done_)
    return Status::Ok();

  // Gets the next overlapping tiles in the fragment read states
  get_next_overlapping_tiles_sparse<T>();

  // Return if there are no more overlapping tiles
  if (done_)
    return Status::Ok();

  // Compute smallest end bounding coordinates
  compute_min_bounding_coords_end<T>();

  // Compute the unsorted fragment cell ranges needed for this read run
  std::vector<FragmentCellRanges> unsorted_fragment_cell_ranges;
  RETURN_NOT_OK(compute_unsorted_fragment_cell_ranges_sparse<T>(
      &unsorted_fragment_cell_ranges));

  // Sort fragment cell ranges
  FragmentCellRanges fragment_cell_ranges;
  RETURN_NOT_OK(sort_fragment_cell_ranges<T>(
      &unsorted_fragment_cell_ranges, &fragment_cell_ranges));

  // Compute the fragment cell position ranges
  auto fragment_cell_pos_ranges = new FragmentCellPosRanges();
  RETURN_NOT_OK(compute_fragment_cell_pos_ranges<T>(
      &fragment_cell_ranges, fragment_cell_pos_ranges));

  // Insert cell pos ranges in the state
  fragment_cell_pos_ranges_vec_.push_back(fragment_cell_pos_ranges);

  // Clean up processed overlapping tiles
  clean_up_processed_fragment_cell_pos_ranges();

  return Status::Ok();
}

template <class T>
void ArrayReadState::get_next_overlapping_tiles_dense() {
  // For easy reference
  auto dim_num = array_metadata_->dim_num();

  // Get the first overlapping tile for each fragment
  if (fragment_cell_pos_ranges_vec_.empty()) {
    // Initialize subarray tile coordinates
    init_subarray_tile_coords<T>();

    // Return if there are no more overlapping tiles
    if (subarray_tile_coords_ == nullptr) {
      done_ = true;
      return;
    }

    // Get next overlapping tile
    for (unsigned int i = 0; i < fragment_num_; ++i) {
      if (fragment_read_states_[i]->dense())
        fragment_read_states_[i]->get_next_overlapping_tile_dense<T>(
            static_cast<const T*>(subarray_tile_coords_));
      // else, it is handled in compute_unsorted_fragment_cell_ranges
    }
  } else {
    // Temporarily store the current subarray tile coordinates
    assert(subarray_tile_coords_ != NULL);
    auto previous_subarray_tile_coords = new T[dim_num];
    std::memcpy(
        previous_subarray_tile_coords, subarray_tile_coords_, coords_size_);

    // Advance range coordinates
    get_next_subarray_tile_coords<T>();

    // Return if there are no more overlapping tiles
    if (subarray_tile_coords_ == nullptr) {
      done_ = true;
      delete[] previous_subarray_tile_coords;
      return;
    }

    // Get next overlapping tiles for the processed fragments
    for (unsigned int i = 0; i < fragment_num_; ++i) {
      if (!fragment_read_states_[i]->done()) {
        if (fragment_read_states_[i]->dense())
          fragment_read_states_[i]->get_next_overlapping_tile_dense<T>(
              static_cast<const T*>(subarray_tile_coords_));
        // else, it is handled in compute_unsorted_fragment_cell_ranges
      }
    }

    // Clean up
    delete[] previous_subarray_tile_coords;
  }
}

template <class T>
void ArrayReadState::get_next_overlapping_tiles_sparse() {
  // For easy reference
  auto dim_num = array_metadata_->dim_num();

  // Get the first overlapping tile for each fragment
  if (fragment_cell_pos_ranges_vec_.empty()) {
    // Initializations
    assert(fragment_bounding_coords_.size() == 0);
    fragment_bounding_coords_.resize(fragment_num_);

    // Get next overlapping tile and bounding coordinates
    done_ = true;
    for (unsigned int i = 0; i < fragment_num_; ++i) {
      fragment_read_states_[i]->get_next_overlapping_tile_sparse<T>();
      if (!fragment_read_states_[i]->done()) {
        fragment_bounding_coords_[i] = std::malloc(2 * coords_size_);
        assert(fragment_bounding_coords_[i] != NULL);
        fragment_read_states_[i]->get_bounding_coords(
            fragment_bounding_coords_[i]);
        done_ = false;
      } else {
        fragment_bounding_coords_[i] = nullptr;
      }
    }
  } else {
    // Get the next overlapping tile for the appropriate fragments
    for (unsigned int i = 0; i < fragment_num_; ++i) {
      // Get next overlapping tile
      auto fragment_bounding_coords =
          static_cast<T*>(fragment_bounding_coords_[i]);
      if (fragment_bounding_coords_[i] != nullptr &&
          !std::memcmp(  // Coinciding end bounding coords
              &fragment_bounding_coords[dim_num],
              min_bounding_coords_end_,
              coords_size_)) {
        fragment_read_states_[i]->get_next_overlapping_tile_sparse<T>();
        if (!fragment_read_states_[i]->done()) {
          fragment_read_states_[i]->get_bounding_coords(
              fragment_bounding_coords_[i]);
        } else {
          if (fragment_bounding_coords_[i])
            std::free(fragment_bounding_coords_[i]);
          fragment_bounding_coords_[i] = nullptr;
        }
      }
    }

    // Check if done
    done_ = true;
    for (unsigned int i = 0; i < fragment_num_; ++i) {
      if (fragment_bounding_coords_[i] != nullptr) {
        done_ = false;
        break;
      }
    }
  }
}

template <class T>
void ArrayReadState::init_subarray_tile_coords() {
  // For easy reference
  auto dim_num = array_metadata_->dim_num();
  auto domain = array_metadata_->domain();
  auto subarray = static_cast<const T*>(query_->subarray());

  // Sanity checks
  assert(subarray_tile_domain_ == NULL);

  // Allocate space for tile domain and subarray tile domain
  auto tile_domain = new T[2 * dim_num];
  subarray_tile_domain_ = std::malloc(2 * dim_num * sizeof(T));
  auto subarray_tile_domain = static_cast<T*>(subarray_tile_domain_);

  // Get subarray in tile domain
  domain->get_subarray_tile_domain<T>(
      subarray, tile_domain, subarray_tile_domain);

  // Check if there is any overlap between the subarray tile domain and the
  // array_metadata tile domain
  bool overlap = true;
  for (unsigned int i = 0; i < dim_num; ++i) {
    if (subarray_tile_domain[2 * i] > tile_domain[2 * i + 1] ||
        subarray_tile_domain[2 * i + 1] < tile_domain[2 * i]) {
      overlap = false;
      break;
    }
  }

  // Calculate subarray tile coordinates
  if (!overlap) {  // No overlap
    std::free(subarray_tile_domain_);
    subarray_tile_domain_ = nullptr;
    assert(subarray_tile_coords_ == NULL);
  } else {  // Overlap
    subarray_tile_coords_ = std::malloc(coords_size_);
    auto subarray_tile_coords = static_cast<T*>(subarray_tile_coords_);
    for (unsigned int i = 0; i < dim_num; ++i)
      subarray_tile_coords[i] = subarray_tile_domain[2 * i];
  }

  // Clean up
  delete[] tile_domain;
}

template <class T>
void ArrayReadState::get_next_subarray_tile_coords() {
  // For easy reference
  auto dim_num = array_metadata_->dim_num();
  auto subarray_tile_domain = static_cast<T*>(subarray_tile_domain_);
  auto subarray_tile_coords = static_cast<T*>(subarray_tile_coords_);

  // Advance subarray tile coordinates
  array_metadata_->domain()->get_next_tile_coords<T>(
      subarray_tile_domain, subarray_tile_coords);

  // Check if the new subarray coordinates fall out of the range domain
  bool inside_domain = true;
  for (unsigned int i = 0; i < dim_num; ++i) {
    if (subarray_tile_coords[i] < subarray_tile_domain[2 * i] ||
        subarray_tile_coords[i] > subarray_tile_domain[2 * i + 1]) {
      inside_domain = false;
      break;
    }
  }

  // The coordinates fall outside the domain
  if (!inside_domain) {
    std::free(subarray_tile_domain_);
    subarray_tile_domain_ = nullptr;
    std::free(subarray_tile_coords_);
    subarray_tile_coords_ = nullptr;
  }
}

Status ArrayReadState::read_dense(void** buffers, uint64_t* buffer_sizes) {
  // For easy reference
  auto& attribute_ids = query_->attribute_ids();
  auto attribute_id_num = (int)attribute_ids.size();

  // Read each attribute individually
  unsigned int buffer_i = 0;
  for (int i = 0; i < attribute_id_num; ++i) {
    if (!array_metadata_->var_size(attribute_ids[i])) {  // FIXED CELLS
      RETURN_NOT_OK(read_dense_attr(
          attribute_ids[i], buffers[buffer_i], &(buffer_sizes[buffer_i])));
      ++buffer_i;
    } else {  // VARIABLE-SIZED CELLS
      RETURN_NOT_OK(read_dense_attr_var(
          attribute_ids[i],
          buffers[buffer_i],  // offsets
          &(buffer_sizes[buffer_i]),
          buffers[buffer_i + 1],  // actual values
          &(buffer_sizes[buffer_i + 1])));
      buffer_i += 2;
    }
  }
  return Status::Ok();
}

Status ArrayReadState::read_dense_attr(
    unsigned int attribute_id, void* buffer, uint64_t* buffer_size) {
  // For easy referenceD
  Datatype coords_type = array_metadata_->coords_type();

  // Invoke the proper templated function
  if (coords_type == Datatype::INT32)
    return read_dense_attr<int>(attribute_id, buffer, buffer_size);
  if (coords_type == Datatype::INT64)
    return read_dense_attr<int64_t>(attribute_id, buffer, buffer_size);
  if (coords_type == Datatype::INT8)
    return read_dense_attr<int8_t>(attribute_id, buffer, buffer_size);
  if (coords_type == Datatype::UINT8)
    return read_dense_attr<uint8_t>(attribute_id, buffer, buffer_size);
  if (coords_type == Datatype::INT16)
    return read_dense_attr<int16_t>(attribute_id, buffer, buffer_size);
  if (coords_type == Datatype::UINT16)
    return read_dense_attr<uint16_t>(attribute_id, buffer, buffer_size);
  if (coords_type == Datatype::UINT32)
    return read_dense_attr<uint32_t>(attribute_id, buffer, buffer_size);
  if (coords_type == Datatype::UINT64)
    return read_dense_attr<uint64_t>(attribute_id, buffer, buffer_size);

  // Code should never reach here
  assert(0);
  return LOG_STATUS(
      Status::ARSError("Invalid datatype when reading dense attribute"));
}

template <class T>
Status ArrayReadState::read_dense_attr(
    unsigned int attribute_id, void* buffer, uint64_t* buffer_size) {
  // Auxiliary variables
  uint64_t buffer_offset = 0;

  // Until read is done or there is a buffer overflow
  for (;;) {
    // Continue copying from the previous unfinished read round
    if (!read_round_done_[attribute_id])
      RETURN_NOT_OK(
          copy_cells(attribute_id, buffer, *buffer_size, &buffer_offset));

    // Check for overflow
    if (overflow_[attribute_id]) {
      *buffer_size = buffer_offset;
      return Status::Ok();
    }

    // Prepare the cell ranges for the next read round
    if (fragment_cell_pos_ranges_vec_pos_[attribute_id] >=
        uint64_t(fragment_cell_pos_ranges_vec_.size())) {
      // Get next cell ranges
      RETURN_NOT_OK(get_next_fragment_cell_ranges_dense<T>());
    }

    // Check if read is done
    if (done_ && fragment_cell_pos_ranges_vec_pos_[attribute_id] ==
                     uint64_t(fragment_cell_pos_ranges_vec_.size())) {
      *buffer_size = buffer_offset;
      return Status::Ok();
    }

    // Copy cells to buffers
    RETURN_NOT_OK(
        copy_cells(attribute_id, buffer, *buffer_size, &buffer_offset));

    // Check for buffer overflow
    if (overflow_[attribute_id]) {
      *buffer_size = buffer_offset;
      return Status::Ok();
    }
  }

  return Status::Ok();
}

Status ArrayReadState::read_dense_attr_var(
    unsigned int attribute_id,
    void* buffer,
    uint64_t* buffer_size,
    void* buffer_var,
    uint64_t* buffer_var_size) {
  // For easy reference
  Datatype coords_type = array_metadata_->coords_type();

  // Invoke the proper templated function
  if (coords_type == Datatype::INT32)
    return read_dense_attr_var<int>(
        attribute_id, buffer, buffer_size, buffer_var, buffer_var_size);
  if (coords_type == Datatype::INT64)
    return read_dense_attr_var<int64_t>(
        attribute_id, buffer, buffer_size, buffer_var, buffer_var_size);
  if (coords_type == Datatype::INT8)
    return read_dense_attr_var<int8_t>(
        attribute_id, buffer, buffer_size, buffer_var, buffer_var_size);
  if (coords_type == Datatype::UINT8)
    return read_dense_attr_var<uint8_t>(
        attribute_id, buffer, buffer_size, buffer_var, buffer_var_size);
  if (coords_type == Datatype::INT16)
    return read_dense_attr_var<int16_t>(
        attribute_id, buffer, buffer_size, buffer_var, buffer_var_size);
  if (coords_type == Datatype::UINT16)
    return read_dense_attr_var<uint16_t>(
        attribute_id, buffer, buffer_size, buffer_var, buffer_var_size);
  if (coords_type == Datatype::UINT32)
    return read_dense_attr_var<uint32_t>(
        attribute_id, buffer, buffer_size, buffer_var, buffer_var_size);
  if (coords_type == Datatype::UINT64)
    return read_dense_attr_var<uint64_t>(
        attribute_id, buffer, buffer_size, buffer_var, buffer_var_size);

  // Code should never reach here
  assert(0);
  return LOG_STATUS(Status::ARSError(
      "Invalid datatype when reading dense variable attribute"));
}

template <class T>
Status ArrayReadState::read_dense_attr_var(
    unsigned int attribute_id,
    void* buffer,
    uint64_t* buffer_size,
    void* buffer_var,
    uint64_t* buffer_var_size) {
  // Auxiliary variables
  uint64_t buffer_offset = 0;
  uint64_t buffer_var_offset = 0;

  // Until read is done or there is a buffer overflow
  for (;;) {
    // Continue copying from the previous unfinished read round
    if (!read_round_done_[attribute_id])
      RETURN_NOT_OK(copy_cells_var(
          attribute_id,
          buffer,
          *buffer_size,
          &buffer_offset,
          buffer_var,
          *buffer_var_size,
          &buffer_var_offset));

    // Check for overflow
    if (overflow_[attribute_id]) {
      *buffer_size = buffer_offset;
      *buffer_var_size = buffer_var_offset;
      return Status::Ok();
    }

    // Prepare the cell ranges for the next read round
    if (fragment_cell_pos_ranges_vec_pos_[attribute_id] >=
        uint64_t(fragment_cell_pos_ranges_vec_.size())) {
      // Get next cell ranges
      RETURN_NOT_OK(get_next_fragment_cell_ranges_dense<T>());
    }

    // Check if read is done
    if (done_ && fragment_cell_pos_ranges_vec_pos_[attribute_id] ==
                     uint64_t(fragment_cell_pos_ranges_vec_.size())) {
      *buffer_size = buffer_offset;
      *buffer_var_size = buffer_var_offset;
      return Status::Ok();
    }

    // Copy cells to buffers
    RETURN_NOT_OK(copy_cells_var(
        attribute_id,
        buffer,
        *buffer_size,
        &buffer_offset,
        buffer_var,
        *buffer_var_size,
        &buffer_var_offset));

    // Check for buffer overflow
    if (overflow_[attribute_id]) {
      *buffer_size = buffer_offset;
      *buffer_var_size = buffer_var_offset;
      return Status::Ok();
    }
  }
  return Status::Ok();
}

Status ArrayReadState::read_sparse(void** buffers, uint64_t* buffer_sizes) {
  // For easy reference
  auto attribute_ids = query_->attribute_ids();
  auto attribute_id_num = attribute_ids.size();

  // Find the coordinates buffer
  unsigned int coords_buffer_i = INVALID_UINT;
  unsigned int buffer_i = 0;
  for (size_t i = 0; i < attribute_id_num; ++i) {
    if (attribute_ids[i] == attribute_num_) {
      coords_buffer_i = buffer_i;
      break;
    }
    if (!array_metadata_->var_size(attribute_ids[i]))  // FIXED CELLS
      ++buffer_i;
    else  // VARIABLE-SIZED CELLS
      buffer_i += 2;
  }

  // Read coordinates attribute first
  if (coords_buffer_i != INVALID_UINT) {
    RETURN_NOT_OK(read_sparse_attr(
        attribute_num_,
        buffers[coords_buffer_i],
        &buffer_sizes[coords_buffer_i]));
  }

  // Read each attribute individually
  buffer_i = 0;
  for (unsigned int i = 0; i < attribute_id_num; ++i) {
    // Skip coordinates attribute (already read)
    if (attribute_ids[i] == attribute_num_) {
      ++buffer_i;
      continue;
    }

    if (!array_metadata_->var_size(attribute_ids[i])) {  // FIXED CELLS
      RETURN_NOT_OK(read_sparse_attr(
          attribute_ids[i], buffers[buffer_i], &buffer_sizes[buffer_i]));
      ++buffer_i;
    } else {  // VARIABLE-SIZED CELLS
      RETURN_NOT_OK(read_sparse_attr_var(
          attribute_ids[i],
          buffers[buffer_i],  // offsets
          &buffer_sizes[buffer_i],
          buffers[buffer_i + 1],  // actual values
          &buffer_sizes[buffer_i + 1]));
      buffer_i += 2;
    }
  }
  return Status::Ok();
}

Status ArrayReadState::read_sparse_attr(
    unsigned int attribute_id, void* buffer, uint64_t* buffer_size) {
  // For easy reference
  Datatype coords_type = array_metadata_->coords_type();

  // Invoke the proper templated function
  if (coords_type == Datatype::INT32)
    return read_sparse_attr<int>(attribute_id, buffer, buffer_size);
  if (coords_type == Datatype::INT64)
    return read_sparse_attr<int64_t>(attribute_id, buffer, buffer_size);
  if (coords_type == Datatype::FLOAT32)
    return read_sparse_attr<float>(attribute_id, buffer, buffer_size);
  if (coords_type == Datatype::FLOAT64)
    return read_sparse_attr<double>(attribute_id, buffer, buffer_size);
  if (coords_type == Datatype::INT8)
    return read_sparse_attr<int8_t>(attribute_id, buffer, buffer_size);
  if (coords_type == Datatype::UINT8)
    return read_sparse_attr<uint8_t>(attribute_id, buffer, buffer_size);
  if (coords_type == Datatype::INT16)
    return read_sparse_attr<int16_t>(attribute_id, buffer, buffer_size);
  if (coords_type == Datatype::UINT16)
    return read_sparse_attr<uint16_t>(attribute_id, buffer, buffer_size);
  if (coords_type == Datatype::UINT32)
    return read_sparse_attr<uint32_t>(attribute_id, buffer, buffer_size);
  if (coords_type == Datatype::UINT64)
    return read_sparse_attr<uint64_t>(attribute_id, buffer, buffer_size);

  // Code should never reach here
  assert(0);
  return LOG_STATUS(
      Status::ARSError("Invalid datatype when reading sparse attribute"));
}

template <class T>
Status ArrayReadState::read_sparse_attr(
    unsigned int attribute_id, void* buffer, uint64_t* buffer_size) {
  // Auxiliary variables
  uint64_t buffer_offset = 0;

  // Until read is done or there is a buffer overflow
  for (;;) {
    // Continue copying from the previous unfinished read round
    if (!read_round_done_[attribute_id])
      RETURN_NOT_OK(
          copy_cells(attribute_id, buffer, *buffer_size, &buffer_offset));

    // Check for overflow
    if (overflow_[attribute_id]) {
      *buffer_size = buffer_offset;
      return Status::Ok();
    }

    // Prepare the cell ranges for the next read round
    if (fragment_cell_pos_ranges_vec_pos_[attribute_id] >=
        uint64_t(fragment_cell_pos_ranges_vec_.size())) {
      // Get next cell ranges
      RETURN_NOT_OK(get_next_fragment_cell_ranges_sparse<T>());
    }

    // Check if read is done
    if (done_ && fragment_cell_pos_ranges_vec_pos_[attribute_id] ==
                     uint64_t(fragment_cell_pos_ranges_vec_.size())) {
      *buffer_size = buffer_offset;
      return Status::Ok();
    }

    // Copy cells to buffers
    RETURN_NOT_OK(
        copy_cells(attribute_id, buffer, *buffer_size, &buffer_offset));

    // Check for buffer overflow
    if (overflow_[attribute_id]) {
      *buffer_size = buffer_offset;
      return Status::Ok();
    }
  }
}

Status ArrayReadState::read_sparse_attr_var(
    unsigned int attribute_id,
    void* buffer,
    uint64_t* buffer_size,
    void* buffer_var,
    uint64_t* buffer_var_size) {
  // For easy reference
  Datatype coords_type = array_metadata_->coords_type();

  // Invoke the proper templated function
  if (coords_type == Datatype::INT32)
    return read_sparse_attr_var<int>(
        attribute_id, buffer, buffer_size, buffer_var, buffer_var_size);
  if (coords_type == Datatype::INT64)
    return read_sparse_attr_var<int64_t>(
        attribute_id, buffer, buffer_size, buffer_var, buffer_var_size);
  if (coords_type == Datatype::FLOAT32)
    return read_sparse_attr_var<float>(
        attribute_id, buffer, buffer_size, buffer_var, buffer_var_size);
  if (coords_type == Datatype::FLOAT64)
    return read_sparse_attr_var<double>(
        attribute_id, buffer, buffer_size, buffer_var, buffer_var_size);
  if (coords_type == Datatype::INT8)
    return read_sparse_attr_var<int8_t>(
        attribute_id, buffer, buffer_size, buffer_var, buffer_var_size);
  if (coords_type == Datatype::UINT8)
    return read_sparse_attr_var<uint8_t>(
        attribute_id, buffer, buffer_size, buffer_var, buffer_var_size);
  if (coords_type == Datatype::INT16)
    return read_sparse_attr_var<int16_t>(
        attribute_id, buffer, buffer_size, buffer_var, buffer_var_size);
  if (coords_type == Datatype::UINT16)
    return read_sparse_attr_var<uint16_t>(
        attribute_id, buffer, buffer_size, buffer_var, buffer_var_size);
  if (coords_type == Datatype::UINT32)
    return read_sparse_attr_var<uint32_t>(
        attribute_id, buffer, buffer_size, buffer_var, buffer_var_size);
  if (coords_type == Datatype::UINT64)
    return read_sparse_attr_var<uint64_t>(
        attribute_id, buffer, buffer_size, buffer_var, buffer_var_size);

  // Code should never reach here
  assert(0);
  return LOG_STATUS(Status::ARSError(
      "Invalid datatype when reading sparse variable attribute"));
}

template <class T>
Status ArrayReadState::read_sparse_attr_var(
    unsigned int attribute_id,
    void* buffer,
    uint64_t* buffer_size,
    void* buffer_var,
    uint64_t* buffer_var_size) {
  // Auxiliary variables
  uint64_t buffer_offset = 0;
  uint64_t buffer_var_offset = 0;

  // Until read is done or there is a buffer overflow
  for (;;) {
    // Continue copying from the previous unfinished read round
    if (!read_round_done_[attribute_id])
      RETURN_NOT_OK(copy_cells_var(
          attribute_id,
          buffer,
          *buffer_size,
          &buffer_offset,
          buffer_var,
          *buffer_var_size,
          &buffer_var_offset));

    // Check for overflow
    if (overflow_[attribute_id]) {
      *buffer_size = buffer_offset;
      *buffer_var_size = buffer_var_offset;
      return Status::Ok();
    }

    // Prepare the cell ranges for the next read round
    if (fragment_cell_pos_ranges_vec_pos_[attribute_id] >=
        uint64_t(fragment_cell_pos_ranges_vec_.size())) {
      // Get next overlapping tiles
      RETURN_NOT_OK(get_next_fragment_cell_ranges_sparse<T>());
    }

    // Check if read is done
    if (done_ && fragment_cell_pos_ranges_vec_pos_[attribute_id] ==
                     uint64_t(fragment_cell_pos_ranges_vec_.size())) {
      *buffer_size = buffer_offset;
      *buffer_var_size = buffer_var_offset;
      return Status::Ok();
    }

    // Copy cells to buffers
    RETURN_NOT_OK(copy_cells_var(
        attribute_id,
        buffer,
        *buffer_size,
        &buffer_offset,
        buffer_var,
        *buffer_var_size,
        &buffer_var_offset));

    // Check for buffer overflow
    if (overflow_[attribute_id]) {
      *buffer_size = buffer_offset;
      *buffer_var_size = buffer_var_offset;
      return Status::Ok();
    }
  }
}

template <class T>
Status ArrayReadState::sort_fragment_cell_ranges(
    std::vector<FragmentCellRanges>* unsorted_fragment_cell_ranges,
    FragmentCellRanges* fragment_cell_ranges) const {
  // For easy reference
  auto fragment_num = (unsigned int)unsorted_fragment_cell_ranges->size();

  // Calculate the number of non-empty unsorted fragment range lists
  unsigned int non_empty = 0;
  unsigned int first_non_empty = INVALID_UINT;
  for (unsigned int i = 0; i < fragment_num; ++i) {
    if (!(*unsorted_fragment_cell_ranges)[i].empty()) {
      ++non_empty;
      if (first_non_empty == INVALID_UINT)
        first_non_empty = i;
    }
  }

  // Sanity check
  if (non_empty == 0)
    return Status::Ok();

  // Trivial case - single fragment
  if (fragment_num == 1) {
    *fragment_cell_ranges = (*unsorted_fragment_cell_ranges)[first_non_empty];
    unsorted_fragment_cell_ranges->clear();
    return Status::Ok();
  }

  // For easy reference
  auto dim_num = array_metadata_->dim_num();
  auto domain = static_cast<const T*>(array_metadata_->domain()->domain());
  auto tile_extents =
      static_cast<const T*>(array_metadata_->domain()->tile_extents());
  auto tile_coords = static_cast<const T*>(subarray_tile_coords_);

  // Compute tile domain
  // This is non-NULL only in the dense array_metadata case
  T* tile_domain = nullptr;
  if (tile_coords != nullptr) {
    tile_domain = new T[2 * dim_num];
    for (unsigned int i = 0; i < dim_num; ++i) {
      tile_domain[2 * i] = domain[2 * i] + tile_coords[i] * tile_extents[i];
      tile_domain[2 * i + 1] = tile_domain[2 * i] + tile_extents[i] - 1;
    }
  }

  // Initialization of metadata for unsorted ranges
  auto rlen = new uint64_t[fragment_num];
  auto rid = new uint64_t[fragment_num];
  unsigned int fid = 0;
  for (unsigned int i = 0; i < fragment_num; ++i) {
    rlen[i] = (uint64_t)((*unsorted_fragment_cell_ranges)[i].size());
    rid[i] = 0;
  }

  // Initializations
  PQFragmentCellRange<T>* pq_fragment_cell_range = nullptr;
  PQFragmentCellRange<T>* popped;
  PQFragmentCellRange<T>* top;
  PQFragmentCellRange<T>* trimmed_top;
  PQFragmentCellRange<T>* extra_popped;
  PQFragmentCellRange<T>* left;
  PQFragmentCellRange<T>* unary;
  FragmentCellRange result;

  // Populate queue
  std::priority_queue<
      PQFragmentCellRange<T>*,
      std::vector<PQFragmentCellRange<T>*>,
      SmallerPQFragmentCellRange<T>>
      pq(array_metadata_);

  for (unsigned int i = 0; i < fragment_num; ++i) {
    if (rlen[i] != 0) {
      pq_fragment_cell_range = new PQFragmentCellRange<T>(
          array_metadata_, &fragment_read_states_, (T*)tile_coords_aux_);
      pq_fragment_cell_range->import_from(
          (*unsorted_fragment_cell_ranges)[i][0]);
      pq.push(pq_fragment_cell_range);
      ++rid[i];
    }
  }

  // Start processing the queue
  while (!pq.empty()) {
    // Pop the first entry and mark it as popped
    popped = pq.top();
    pq.pop();

    // Last range - insert it into the results and get the next range
    // for that fragment
    if (pq.empty()) {
      popped->export_to(&result);
      fragment_cell_ranges->push_back(result);
      fid = (popped->fragment_id_ != INVALID_UINT) ? popped->fragment_id_ :
                                                     fragment_num - 1;
      delete popped;

      if (rid[fid] == rlen[fid])
        break;

      pq_fragment_cell_range = new PQFragmentCellRange<T>(
          array_metadata_, &fragment_read_states_, (T*)tile_coords_aux_);
      pq_fragment_cell_range->import_from(
          (*unsorted_fragment_cell_ranges)[fid][rid[fid]]);
      pq.push(pq_fragment_cell_range);
      ++rid[fid];
      continue;
    }

    // Mark the second entry (now top) as top
    top = pq.top();

    // Dinstinguish two cases
    if (popped->dense() || popped->unary()) {  // DENSE OR UNARY POPPED
      // Keep on trimming ranges from the queue
      while (!pq.empty() && popped->must_trim(top)) {
        // Cut the top range and re-insert, only if there is partial overlap
        if (top->ends_after(popped)) {
          // Create the new trimmed top range
          trimmed_top = new PQFragmentCellRange<T>(
              array_metadata_, &fragment_read_states_, (T*)tile_coords_aux_);
          popped->trim(top, trimmed_top, tile_domain);

          // Discard top
          std::free(top->cell_range_);
          delete top;
          pq.pop();

          if (trimmed_top->cell_range_ != nullptr) {
            // Re-insert the trimmed range in pq
            pq.push(trimmed_top);
          } else {
            // Get the next range from the top fragment
            fid = (trimmed_top->fragment_id_ != INVALID_UINT) ?
                      trimmed_top->fragment_id_ :
                      fragment_num - 1;
            if (rid[fid] != rlen[fid]) {
              pq_fragment_cell_range = new PQFragmentCellRange<T>(
                  array_metadata_,
                  &fragment_read_states_,
                  (T*)tile_coords_aux_);
              pq_fragment_cell_range->import_from(
                  (*unsorted_fragment_cell_ranges)[fid][rid[fid]]);
              pq.push(pq_fragment_cell_range);
              ++rid[fid];
            }
            // Clear trimmed top
            delete trimmed_top;
          }
        } else {
          // Get the next range from the top fragment
          fid = (top->fragment_id_ != INVALID_UINT) ? top->fragment_id_ :
                                                      fragment_num - 1;
          if (rid[fid] != rlen[fid]) {
            pq_fragment_cell_range = new PQFragmentCellRange<T>(
                array_metadata_, &fragment_read_states_, (T*)tile_coords_aux_);
            pq_fragment_cell_range->import_from(
                (*unsorted_fragment_cell_ranges)[fid][rid[fid]]);
          }

          // Discard top
          std::free(top->cell_range_);
          delete top;
          pq.pop();

          if (rid[fid] != rlen[fid]) {
            pq.push(pq_fragment_cell_range);
            ++rid[fid];
          }
        }

        // Get a new top
        if (!pq.empty())
          top = pq.top();
      }

      // Potentially split the popped range
      if (!pq.empty() && popped->must_be_split(top)) {
        // Split the popped range
        extra_popped = new PQFragmentCellRange<T>(
            array_metadata_, &fragment_read_states_, (T*)tile_coords_aux_);
        popped->split(top, extra_popped, tile_domain);
        // Re-instert the extra popped range into the queue
        pq.push(extra_popped);
      } else {
        // Get the next range from popped fragment
        fid = (popped->fragment_id_ != INVALID_UINT) ? popped->fragment_id_ :
                                                       fragment_num - 1;
        if (rid[fid] != rlen[fid]) {
          pq_fragment_cell_range = new PQFragmentCellRange<T>(
              array_metadata_, &fragment_read_states_, (T*)tile_coords_aux_);
          pq_fragment_cell_range->import_from(
              (*unsorted_fragment_cell_ranges)[fid][rid[fid]]);
          pq.push(pq_fragment_cell_range);
          ++rid[fid];
        }
      }

      // Insert the final popped range into the results
      popped->export_to(&result);
      fragment_cell_ranges->push_back(result);
      delete popped;
    } else {  // SPARSE POPPED
      // If popped does not overlap with top, insert popped into results
      if (!pq.empty() && top->begins_after(popped)) {
        popped->export_to(&result);
        fragment_cell_ranges->push_back(result);
        // Get the next range from the popped fragment
        fid = popped->fragment_id_;
        if (rid[fid] != rlen[fid]) {
          pq_fragment_cell_range = new PQFragmentCellRange<T>(
              array_metadata_, &fragment_read_states_, (T*)tile_coords_aux_);
          pq_fragment_cell_range->import_from(
              (*unsorted_fragment_cell_ranges)[fid][rid[fid]]);
          pq.push(pq_fragment_cell_range);
          ++rid[fid];
        }
        delete popped;
      } else {
        // Create up to 3 more ranges (left, unary, new popped/right)
        left = new PQFragmentCellRange<T>(
            array_metadata_, &fragment_read_states_, (T*)tile_coords_aux_);
        unary = new PQFragmentCellRange<T>(
            array_metadata_, &fragment_read_states_, (T*)tile_coords_aux_);
        popped->split_to_3(top, left, unary);

        // Get the next range from the popped fragment
        if (unary->cell_range_ == nullptr && popped->cell_range_ == nullptr) {
          fid = popped->fragment_id_;
          if (rid[fid] != rlen[fid]) {
            pq_fragment_cell_range = new PQFragmentCellRange<T>(
                array_metadata_, &fragment_read_states_, (T*)tile_coords_aux_);
            pq_fragment_cell_range->import_from(
                (*unsorted_fragment_cell_ranges)[fid][rid[fid]]);
            pq.push(pq_fragment_cell_range);
            ++rid[fid];
          }
        }

        // Insert left to results or discard it
        if (left->cell_range_ != nullptr) {
          left->export_to(&result);
          fragment_cell_ranges->push_back(result);
        }
        delete left;

        // Insert unary to the priority queue
        if (unary->cell_range_ != nullptr)
          pq.push(unary);
        else
          delete unary;

        // Re-insert new popped (right) range to the priority queue
        if (popped->cell_range_ != nullptr)
          pq.push(popped);
        else
          delete popped;
      }
    }
  }

  // Clean up
  unsorted_fragment_cell_ranges->clear();
  delete[] tile_domain;
  delete[] rlen;
  delete[] rid;

  assert(pq.empty());  // Sanity check

  // Return
  return Status::Ok();
}

};  // namespace tiledb
