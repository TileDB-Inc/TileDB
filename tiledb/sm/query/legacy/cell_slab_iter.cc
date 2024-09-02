/**
 * @file   cell_slab_iter.cc
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
 * This file implements class CellSlabIter.
 */

#include "tiledb/sm/query/legacy/cell_slab_iter.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/type/apply_with_type.h"
#include "tiledb/type/range/range.h"

#include <cassert>
#include <iostream>

using namespace tiledb::common;
using namespace tiledb::type;

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

template <class T>
CellSlabIter<T>::CellSlabIter() {
  subarray_ = nullptr;
  end_ = true;
}

template <class T>
CellSlabIter<T>::CellSlabIter(const Subarray* subarray)
    : subarray_(subarray) {
  end_ = true;
  if (subarray != nullptr) {
    const auto& array_schema = subarray->array()->array_schema_latest();
    auto dim_num = array_schema.dim_num();
    auto coord_size{array_schema.dimension_ptr(0)->coord_size()};
    aux_tile_coords_.resize(dim_num);
    aux_tile_coords_2_.resize(dim_num * coord_size);
  }
}

/* ****************************** */
/*               API              */
/* ****************************** */

template <class T>
Status CellSlabIter<T>::begin() {
  if (subarray_ == nullptr)
    return Status::Ok();

  RETURN_NOT_OK(sanity_check());
  cell_slab_.init(subarray_->dim_num());
  RETURN_NOT_OK(init_ranges());
  init_coords();
  init_cell_slab_lengths();
  update_cell_slab();

  end_ = false;

  return Status::Ok();
}

template <class T>
CellSlab<T> CellSlabIter<T>::cell_slab() const {
  return cell_slab_;
}

template <class T>
bool CellSlabIter<T>::end() const {
  return end_;
}

template <class T>
void CellSlabIter<T>::operator++() {
  // If at the end, do nothing
  if (end_)
    return;

  // Advance the iterator
  if (subarray_->layout() == Layout::ROW_MAJOR)
    advance_row();
  else
    advance_col();

  if (end_) {
    cell_slab_.reset();
    return;
  }

  update_cell_slab();
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

template <class T>
void CellSlabIter<T>::advance_col() {
  auto dim_num = (int)subarray_->dim_num();

  for (int i = 0; i < dim_num; ++i) {
    cell_slab_coords_[i] += (i == 0) ? cell_slab_lengths_[range_coords_[i]] : 1;
    if (cell_slab_coords_[i] > ranges_[i][range_coords_[i]].end_) {
      ++range_coords_[i];
      if (range_coords_[i] < (T)ranges_[i].size())
        cell_slab_coords_[i] = ranges_[i][range_coords_[i]].start_;
    }

    if (range_coords_[i] < (T)ranges_[i].size()) {
      break;
    } else {
      // The iterator has reached the end
      if (i == dim_num - 1) {
        end_ = true;
        return;
      }

      range_coords_[i] = 0;
      cell_slab_coords_[i] = ranges_[i][0].start_;
    }
  }
}

template <class T>
void CellSlabIter<T>::advance_row() {
  auto dim_num = (int)subarray_->dim_num();

  for (int i = dim_num - 1; i >= 0; --i) {
    cell_slab_coords_[i] +=
        (i == dim_num - 1) ? cell_slab_lengths_[range_coords_[i]] : 1;
    if (cell_slab_coords_[i] > ranges_[i][range_coords_[i]].end_) {
      ++range_coords_[i];
      if (range_coords_[i] < (T)ranges_[i].size())
        cell_slab_coords_[i] = ranges_[i][range_coords_[i]].start_;
    }

    if (range_coords_[i] < (T)ranges_[i].size()) {
      break;
    } else {
      // The iterator has reached the end
      if (i == 0) {
        end_ = true;
        return;
      }

      range_coords_[i] = 0;
      cell_slab_coords_[i] = ranges_[i][0].start_;
    }
  }
}

template <class T>
void CellSlabIter<T>::create_ranges(
    const T* range,
    T tile_extent,
    T dim_domain_start,
    std::vector<Range>* ranges) {
  T tile_start = Dimension::tile_idx(range[0], dim_domain_start, tile_extent);
  T tile_end = Dimension::tile_idx(range[1], dim_domain_start, tile_extent);

  // The range falls in the same tile
  if (tile_start == tile_end) {
    ranges->emplace_back(range[0], range[1], tile_start);
  } else {  // We need to split the range
    T start = range[0];
    T end;
    for (T i = tile_start; i < tile_end; ++i) {
      end = Dimension::tile_coord_high(i, dim_domain_start, tile_extent);
      ranges->emplace_back(start, end, i);
      start = end + 1;
    }
    ranges->emplace_back(start, range[1], tile_end);
  }
}

template <class T>
void CellSlabIter<T>::init_cell_slab_lengths() {
  auto layout = subarray_->layout();
  auto dim_num = subarray_->dim_num();

  if (layout == Layout::ROW_MAJOR) {
    auto range_num = ranges_[dim_num - 1].size();
    cell_slab_lengths_.resize(range_num);
    for (size_t i = 0; i < range_num; ++i)
      cell_slab_lengths_[i] =
          ranges_[dim_num - 1][i].end_ - ranges_[dim_num - 1][i].start_ + 1;
  } else {
    assert(layout == Layout::COL_MAJOR);
    auto range_num = ranges_[0].size();
    cell_slab_lengths_.resize(range_num);
    for (size_t i = 0; i < range_num; ++i)
      cell_slab_lengths_[i] = ranges_[0][i].end_ - ranges_[0][i].start_ + 1;
  }
}

template <class T>
void CellSlabIter<T>::init_coords() {
  auto dim_num = subarray_->dim_num();

  range_coords_.resize(dim_num);
  cell_slab_coords_.resize(dim_num);
  for (unsigned i = 0; i < dim_num; ++i) {
    range_coords_[i] = 0;
    cell_slab_coords_[i] = ranges_[i][0].start_;
  }
}

template <class T>
Status CellSlabIter<T>::init_ranges() {
  // For easy reference
  auto dim_num = subarray_->dim_num();
  const auto& array_schema = subarray_->array()->array_schema_latest();
  auto array_domain = array_schema.domain().domain();
  uint64_t range_num;
  T tile_extent, dim_domain_start;
  const tiledb::type::Range* r;

  ranges_.resize(dim_num);
  for (unsigned d = 0; d < dim_num; ++d) {
    auto dim_dom = (const T*)array_domain[d].data();
    subarray_->get_range_num(d, &range_num);
    ranges_[d].reserve(range_num);
    tile_extent = *(const T*)array_schema.domain().tile_extent(d).data();
    dim_domain_start = dim_dom[0];
    for (uint64_t j = 0; j < range_num; ++j) {
      subarray_->get_range(d, j, &r);
      create_ranges(
          (const T*)(*r).data(), tile_extent, dim_domain_start, &ranges_[d]);
    }
  }

  return Status::Ok();
}

template <class T>
Status CellSlabIter<T>::sanity_check() const {
  assert(subarray_ != nullptr);

  // Check layout
  auto layout = subarray_->layout();
  if (layout != Layout::ROW_MAJOR && layout != Layout::COL_MAJOR)
    return LOG_STATUS(Status_CellSlabIterError(
        "Unsupported subarray layout; the iterator supports only row-major and "
        "column-major layouts"));

  // Check type
  bool error;
  const auto& array_schema = subarray_->array()->array_schema_latest();
  auto type = array_schema.domain().dimension_ptr(0)->type();

  auto g = [&](auto Arg) {
    if constexpr (tiledb::type::TileDBIntegral<decltype(Arg)>) {
      error = !std::is_same_v<T, decltype(Arg)>;
    } else {
      throw std::logic_error("Unsupported datatype");
    }
  };
  apply_with_type(g, type);

  if (error) {
    return LOG_STATUS(Status_CellSlabIterError(
        "Datatype mismatch between cell slab iterator and subarray"));
  }

  return Status::Ok();
}

template <class T>
void CellSlabIter<T>::update_cell_slab() {
  auto dim_num = subarray_->dim_num();
  auto layout = subarray_->layout();

  for (unsigned i = 0; i < dim_num; ++i) {
    aux_tile_coords_[i] = ranges_[i][range_coords_[i]].tile_coord_;
    cell_slab_.coords_[i] = cell_slab_coords_[i];
  }
  cell_slab_.tile_coords_ =
      subarray_->tile_coords_ptr(aux_tile_coords_, &aux_tile_coords_2_);
  cell_slab_.length_ = (layout == Layout::ROW_MAJOR) ?
                           cell_slab_lengths_[range_coords_[dim_num - 1]] :
                           cell_slab_lengths_[range_coords_[0]];
}

// Explicit template instantiations
template class CellSlabIter<int8_t>;
template class CellSlabIter<uint8_t>;
template class CellSlabIter<int16_t>;
template class CellSlabIter<uint16_t>;
template class CellSlabIter<int32_t>;
template class CellSlabIter<uint32_t>;
template class CellSlabIter<int64_t>;
template class CellSlabIter<uint64_t>;
template class CellSlabIter<float>;
template class CellSlabIter<double>;

}  // namespace sm
}  // namespace tiledb
