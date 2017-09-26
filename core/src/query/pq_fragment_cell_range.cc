/**
 * @file   pq_fragment_cell_range.cc
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
 * This file implements the PQFragmentCellRange class.
 */

#include "pq_fragment_cell_range.h"

#include <cassert>

namespace tiledb {

/* ****************************** */
/*        STATIC CONSTANTS        */
/* ****************************** */

template <typename T>
const uint64_t PQFragmentCellRange<T>::INVALID_UINT64 = UINT64_MAX;

template <typename T>
const unsigned int PQFragmentCellRange<T>::INVALID_UINT = UINT_MAX;

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

template <class T>
PQFragmentCellRange<T>::PQFragmentCellRange(
    const ArrayMetadata* array_metadata,
    const std::vector<ReadState*>* fragment_read_states,
    T* tile_coords_aux) {
  array_metadata_ = array_metadata;
  fragment_read_states_ = fragment_read_states;
  tile_coords_aux_ = tile_coords_aux;

  cell_range_ = nullptr;
  fragment_id_ = INVALID_UINT;
  tile_id_l_ = INVALID_UINT64;
  tile_id_r_ = INVALID_UINT64;
  tile_pos_ = INVALID_UINT64;

  coords_size_ = array_metadata_->coords_size();
  dim_num_ = array_metadata_->dim_num();
}

/* ****************************** */
/*             API                */
/* ****************************** */

template <class T>
bool PQFragmentCellRange<T>::begins_after(
    const PQFragmentCellRange* fcr) const {
  return tile_id_l_ > fcr->tile_id_r_ ||
         (tile_id_l_ == fcr->tile_id_r_ &&
          array_metadata_->domain()->cell_order_cmp(
              cell_range_, &(fcr->cell_range_[dim_num_])) > 0);
}

template <class T>
bool PQFragmentCellRange<T>::dense() const {
  return fragment_id_ == INVALID_UINT ||
         (*fragment_read_states_)[fragment_id_]->dense();
}

template <class T>
bool PQFragmentCellRange<T>::ends_after(const PQFragmentCellRange* fcr) const {
  return tile_id_r_ > fcr->tile_id_r_ ||
         (tile_id_r_ == fcr->tile_id_r_ &&
          array_metadata_->domain()->cell_order_cmp(
              &cell_range_[dim_num_], &fcr->cell_range_[dim_num_]) > 0);
}

template <class T>
void PQFragmentCellRange<T>::export_to(
    ReadState::FragmentCellRange* fragment_cell_range) {
  // Copy members
  fragment_cell_range->second = cell_range_;
  fragment_cell_range->first.first = fragment_id_;
  fragment_cell_range->first.second = tile_pos_;
}

template <class T>
void PQFragmentCellRange<T>::import_from(
    const ReadState::FragmentCellRange& fragment_cell_range) {
  // Copy members
  cell_range_ = static_cast<T*>(fragment_cell_range.second);
  fragment_id_ = fragment_cell_range.first.first;
  tile_pos_ = fragment_cell_range.first.second;

  // Compute tile ids
  tile_id_l_ =
      array_metadata_->domain()->tile_id<T>(cell_range_, tile_coords_aux_);
  tile_id_r_ = array_metadata_->domain()->tile_id<T>(
      &cell_range_[dim_num_], tile_coords_aux_);
}

template <class T>
bool PQFragmentCellRange<T>::must_be_split(
    const PQFragmentCellRange* fcr) const {
  return fcr->fragment_id_ != INVALID_UINT &&
         (fragment_id_ == INVALID_UINT || fcr->fragment_id_ > fragment_id_) &&
         (fcr->tile_id_l_ < tile_id_r_ ||
          (fcr->tile_id_l_ == tile_id_r_ &&
           array_metadata_->domain()->cell_order_cmp(
               fcr->cell_range_, &cell_range_[dim_num_]) <= 0));
}

template <class T>
bool PQFragmentCellRange<T>::must_trim(const PQFragmentCellRange* fcr) const {
  return fragment_id_ != INVALID_UINT &&
         (fcr->fragment_id_ == INVALID_UINT ||
          fcr->fragment_id_ < fragment_id_) &&
         (fcr->tile_id_l_ > tile_id_l_ ||
          (fcr->tile_id_l_ == tile_id_l_ &&
           array_metadata_->domain()->cell_order_cmp(
               fcr->cell_range_, cell_range_) >= 0)) &&
         (fcr->tile_id_l_ < tile_id_r_ ||
          (fcr->tile_id_l_ == tile_id_r_ &&
           array_metadata_->domain()->cell_order_cmp(
               fcr->cell_range_, &cell_range_[dim_num_]) <= 0));
}

template <class T>
void PQFragmentCellRange<T>::split(
    const PQFragmentCellRange* fcr,
    PQFragmentCellRange* fcr_new,
    const T* tile_domain) {
  // Create the new range
  fcr_new->fragment_id_ = fragment_id_;
  fcr_new->tile_pos_ = tile_pos_;
  fcr_new->cell_range_ = (T*)std::malloc(2 * coords_size_);
  fcr_new->tile_id_l_ = fcr->tile_id_l_;
  std::memcpy(fcr_new->cell_range_, fcr->cell_range_, coords_size_);
  fcr_new->tile_id_r_ = tile_id_r_;
  std::memcpy(
      &(fcr_new->cell_range_[dim_num_]), &cell_range_[dim_num_], coords_size_);

  // Trim the calling object range
  std::memcpy(&cell_range_[dim_num_], fcr->cell_range_, coords_size_);
  array_metadata_->domain()->get_previous_cell_coords<T>(
      tile_domain, &cell_range_[dim_num_]);
  tile_id_r_ = array_metadata_->domain()->tile_id<T>(
      &cell_range_[dim_num_], tile_coords_aux_);
}

template <class T>
void PQFragmentCellRange<T>::split_to_3(
    const PQFragmentCellRange* fcr,
    PQFragmentCellRange* fcr_left,
    PQFragmentCellRange* fcr_unary) {
  // Initialize fcr_left
  fcr_left->fragment_id_ = fragment_id_;
  fcr_left->tile_pos_ = tile_pos_;
  fcr_left->cell_range_ = (T*)malloc(2 * coords_size_);
  fcr_left->tile_id_l_ = tile_id_l_;
  std::memcpy(fcr_left->cell_range_, cell_range_, coords_size_);

  // Get enclosing coordinates
  bool left_retrieved, right_retrieved, target_exists;
  Status st =
      (*fragment_read_states_)[fragment_id_]->template get_enclosing_coords<T>(
          tile_pos_,                         // Tile
          fcr->cell_range_,                  // Target coords
          cell_range_,                       // Start coords
          &cell_range_[dim_num_],            // End coords
          &fcr_left->cell_range_[dim_num_],  // Left coords
          cell_range_,                       // Right coords
          &left_retrieved,                   // Left retrieved
          &right_retrieved,                  // Right retrieved
          &target_exists);                   // Target exists
  assert(st.ok());

  // Clean up if necessary
  if (left_retrieved) {
    fcr_left->tile_id_r_ = array_metadata_->domain()->tile_id<T>(
        &fcr_left->cell_range_[dim_num_], tile_coords_aux_);
  } else {
    std::free(fcr_left->cell_range_);
    fcr_left->cell_range_ = nullptr;
  }

  if (right_retrieved) {
    tile_id_l_ =
        array_metadata_->domain()->tile_id<T>(cell_range_, tile_coords_aux_);
  } else {
    std::free(cell_range_);
    cell_range_ = nullptr;
  }

  // Create unary range
  if (target_exists) {
    fcr_unary->fragment_id_ = fragment_id_;
    fcr_unary->tile_pos_ = tile_pos_;
    fcr_unary->cell_range_ = (T*)malloc(2 * coords_size_);
    fcr_unary->tile_id_l_ = fcr->tile_id_l_;
    std::memcpy(fcr_unary->cell_range_, fcr->cell_range_, coords_size_);
    fcr_unary->tile_id_r_ = fcr->tile_id_l_;
    std::memcpy(
        &(fcr_unary->cell_range_[dim_num_]), fcr->cell_range_, coords_size_);
  } else {
    fcr_unary->cell_range_ = nullptr;
  }
}

template <class T>
void PQFragmentCellRange<T>::trim(
    const PQFragmentCellRange* fcr,
    PQFragmentCellRange* fcr_trimmed,
    const T* tile_domain) const {
  // Construct trimmed range
  fcr_trimmed->fragment_id_ = fcr->fragment_id_;
  fcr_trimmed->tile_pos_ = fcr->tile_pos_;
  fcr_trimmed->cell_range_ = (T*)malloc(2 * coords_size_);
  memcpy(fcr_trimmed->cell_range_, &cell_range_[dim_num_], coords_size_);
  fcr_trimmed->tile_id_l_ = tile_id_r_;
  memcpy(
      &(fcr_trimmed->cell_range_[dim_num_]),
      &(fcr->cell_range_[dim_num_]),
      coords_size_);
  fcr_trimmed->tile_id_r_ = fcr->tile_id_r_;

  // Advance the left endpoint of the trimmed range
  bool coords_retrieved;
  if (fcr_trimmed->dense()) {
    array_metadata_->domain()->get_next_cell_coords<T>(  // fcr is DENSE
        tile_domain,
        fcr_trimmed->cell_range_,
        &coords_retrieved);
  } else {  // fcr is SPARSE
    Status st = (*fragment_read_states_)[fcr->fragment_id_]->get_coords_after(
        &(cell_range_[dim_num_]), fcr_trimmed->cell_range_, &coords_retrieved);
    assert(st.ok());
  }

  if (!coords_retrieved) {
    free(fcr_trimmed->cell_range_);
    fcr_trimmed->cell_range_ = nullptr;
  }
}

template <class T>
bool PQFragmentCellRange<T>::unary() const {
  return !std::memcmp(cell_range_, &cell_range_[dim_num_], coords_size_);
}

// Explicit template instantiations
template class PQFragmentCellRange<int>;
template class PQFragmentCellRange<int64_t>;
template class PQFragmentCellRange<float>;
template class PQFragmentCellRange<double>;
template class PQFragmentCellRange<int8_t>;
template class PQFragmentCellRange<uint8_t>;
template class PQFragmentCellRange<int16_t>;
template class PQFragmentCellRange<uint16_t>;
template class PQFragmentCellRange<uint32_t>;
template class PQFragmentCellRange<uint64_t>;

}  // namespace tiledb
