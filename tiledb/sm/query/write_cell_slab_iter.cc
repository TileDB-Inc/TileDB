/**
 * @file   write_cell_slab_iter.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * This file implements class WriteCellSlabIter.
 */

#include "tiledb/sm/query/write_cell_slab_iter.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/misc/utils.h"

#include <cassert>
#include <iostream>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

template <class T>
WriteCellSlabIter<T>::WriteCellSlabIter() {
  domain_ = nullptr;
  end_ = true;
  tile_overlap_ = false;
}

template <class T>
WriteCellSlabIter<T>::WriteCellSlabIter(
    const Domain* domain, const std::vector<T>& subarray, Layout layout)
    : domain_(domain)
    , subarray_(subarray)
    , layout_(layout) {
  end_ = true;
  tile_overlap_ = false;
}

/* ****************************** */
/*               API              */
/* ****************************** */

template <class T>
Status WriteCellSlabIter<T>::begin() {
  // No domain, do not begin
  if (domain_ == nullptr)
    return Status::Ok();

  RETURN_NOT_OK(sanity_check());

  end_ = false;
  auto dim_num = domain_->dim_num();
  coords_start_.resize(dim_num);
  coords_end_.resize(dim_num);
  tile_coords_.resize(dim_num);
  tile_coords_in_subarray_.resize(dim_num);
  tile_subarray_.resize(2 * dim_num);
  subarray_in_tile_.resize(2 * dim_num);
  tile_domain_.resize(2 * domain_->dim_num());
  for (unsigned i = 0; i < dim_num; ++i)
    coords_start_[i] = subarray_[2 * i];

  compute_current_tile_info();
  compute_current_end_coords();
  RETURN_NOT_OK(compute_current_slab());

  return Status::Ok();
}

template <class T>
const T* WriteCellSlabIter<T>::coords_start() const {
  return &coords_start_[0];
}

template <class T>
const T* WriteCellSlabIter<T>::coords_end() const {
  return &coords_end_[0];
}

template <class T>
bool WriteCellSlabIter<T>::end() const {
  return end_;
}

template <class T>
uint64_t WriteCellSlabIter<T>::tile_idx() const {
  return tile_idx_;
}

template <class T>
uint64_t WriteCellSlabIter<T>::slab_start() const {
  return slab_start_;
}

template <class T>
uint64_t WriteCellSlabIter<T>::slab_end() const {
  return slab_end_;
}

template <class T>
const T* WriteCellSlabIter<T>::tile_coords() const {
  return &tile_coords_[0];
}

template <class T>
void WriteCellSlabIter<T>::operator++() {
  // If at the end, do nothing
  if (end_)
    return;

  // Get next start coordinates, which must follow the end coordinates
  bool in_subarray = false;
  coords_start_ = coords_end_;
  compute_next_start_coords(&in_subarray);

  // Iterator done if coordinates not retrieved
  if (!in_subarray) {
    end_ = true;
    return;
  }

  // For global order, `compute_next_start_coords` computes all tile info,
  // so compute tile info only for the other layouts
  if (layout_ != Layout::GLOBAL_ORDER)
    compute_current_tile_info();

  compute_current_end_coords();
  auto st = compute_current_slab();
  assert(st.ok());
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

template <class T>
void WriteCellSlabIter<T>::compute_current_end_coords() {
  domain_->get_end_of_cell_slab(
      &subarray_[0], &coords_start_[0], layout_, &coords_end_[0]);
}

template <class T>
Status WriteCellSlabIter<T>::compute_current_slab() {
  RETURN_NOT_OK(domain_->get_cell_pos<T>(&coords_start_[0], &slab_start_));
  RETURN_NOT_OK(domain_->get_cell_pos<T>(&coords_end_[0], &slab_end_));
  assert(slab_start_ <= slab_end_);
  return Status::Ok();
}

template <class T>
void WriteCellSlabIter<T>::compute_current_tile_info() {
  auto dim_num = domain_->dim_num();
  domain_->get_tile_coords(&coords_start_[0], &tile_coords_[0]);
  domain_->get_tile_subarray(&tile_coords_[0], &tile_subarray_[0]);
  utils::geometry::overlap(
      &subarray_[0],
      &tile_subarray_[0],
      dim_num,
      &subarray_in_tile_[0],
      &tile_overlap_);
  domain_->get_tile_domain(&subarray_[0], &tile_domain_[0]);
  tile_idx_ = domain_->get_tile_pos(&tile_coords_[0]);
}

template <class T>
void WriteCellSlabIter<T>::compute_next_start_coords(bool* in_subarray) {
  switch (layout_) {
    case Layout::ROW_MAJOR:
      domain_->get_next_cell_coords_row<T>(
          &subarray_[0], &coords_start_[0], in_subarray);
      break;
    case Layout::COL_MAJOR:
      domain_->get_next_cell_coords_col<T>(
          &subarray_[0], &coords_start_[0], in_subarray);
      break;
    case Layout::GLOBAL_ORDER:
      compute_next_start_coords_global(in_subarray);
      break;
    default:
      *in_subarray = false;
      assert(0);
  }
}

template <class T>
void WriteCellSlabIter<T>::compute_next_start_coords_global(bool* in_subarray) {
  if (domain_->cell_order() == Layout::ROW_MAJOR)
    domain_->get_next_cell_coords_row<T>(
        &subarray_in_tile_[0], &coords_start_[0], in_subarray);
  else if (domain_->cell_order() == Layout::COL_MAJOR)
    domain_->get_next_cell_coords_col<T>(
        &subarray_in_tile_[0], &coords_start_[0], in_subarray);
  else
    assert(0);

  // Move to the next tile
  auto dim_num = domain_->dim_num();
  if (!*in_subarray) {
    domain_->get_next_tile_coords(
        &tile_domain_[0], &tile_coords_[0], in_subarray);
    if (*in_subarray) {
      tile_idx_ = domain_->get_tile_pos(&tile_coords_[0]);
      domain_->get_tile_subarray(&tile_coords_[0], &tile_subarray_[0]);
      utils::geometry::overlap(
          &subarray_[0],
          &tile_subarray_[0],
          dim_num,
          &subarray_in_tile_[0],
          &tile_overlap_);
      for (unsigned i = 0; i < domain_->dim_num(); ++i)
        coords_start_[i] = subarray_in_tile_[2 * i];
    }
  }
}

template <class T>
Status WriteCellSlabIter<T>::sanity_check() const {
  // The layout should not be unordered
  if (layout_ == Layout::UNORDERED)
    return LOG_STATUS(Status::WriteCellSlabIterError(
        "Sanity check failed; Unordered layout is invalid"));

  // For easy reference
  auto dim_num = domain_->dim_num();
  auto domain = domain_->domain();

  // Check subarray length
  if (subarray_.size() != 2 * dim_num)
    return LOG_STATUS(Status::WriteCellSlabIterError(
        "Sanity check failed; Invalid subarray length"));

  // Check subarray bounds
  for (unsigned d = 0; d < dim_num; ++d) {
    if (subarray_[2 * d] > subarray_[2 * d + 1])
      return LOG_STATUS(Status::WriteCellSlabIterError(
          "Sanity check failed; Invalid subarray bounds"));
  }

  // Check if subarray is contained in the domain
  for (unsigned d = 0; d < dim_num; ++d) {
    auto dim_dom = (const T*)domain[d].data();
    if (subarray_[2 * d] < dim_dom[0] || subarray_[2 * d] > dim_dom[1] ||
        subarray_[2 * d + 1] < dim_dom[0] || subarray_[2 * d + 1] > dim_dom[1])
      return LOG_STATUS(Status::WriteCellSlabIterError(
          "Sanity check failed; Subarray not contained in domain"));
  }

  return Status::Ok();
}

// Explicit template instantiations
template class WriteCellSlabIter<int8_t>;
template class WriteCellSlabIter<uint8_t>;
template class WriteCellSlabIter<int16_t>;
template class WriteCellSlabIter<uint16_t>;
template class WriteCellSlabIter<int>;
template class WriteCellSlabIter<unsigned>;
template class WriteCellSlabIter<int64_t>;
template class WriteCellSlabIter<uint64_t>;

}  // namespace sm
}  // namespace tiledb
