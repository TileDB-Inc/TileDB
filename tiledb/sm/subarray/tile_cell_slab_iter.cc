/**
 * @file   tile_cell_slab_iter.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * This file implements class TileCellSlabIter.
 */

#include "tiledb/sm/subarray/tile_cell_slab_iter.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/array/array.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/misc/types.h"
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
TileCellSlabIter<T>::TileCellSlabIter(
    const uint64_t range_thread_idx,
    const uint64_t num_range_threads,
    const Subarray& root_subarray,
    const DenseTileSubarray<T>& subarray,
    const std::vector<T>& tile_extents,
    const std::vector<T>& start_coords,
    const std::vector<RangeInfo<T>>& range_info,
    const Layout cell_order)
    : num_ranges_(root_subarray.range_num())
    , original_range_idx_(subarray.original_range_idx())
    , range_info_(range_info)
    , layout_(
          root_subarray.layout() == Layout::GLOBAL_ORDER ?
              cell_order :
              root_subarray.layout())
    , dim_num_(root_subarray.dim_num())
    , global_offset_(0)
    , pos_in_tile_(0)
    , dest_offset_row_col_(0)
    , num_(std::numeric_limits<uint64_t>::max())
    , end_(false)
    , last_(true)
    , global_order_(root_subarray.layout() == Layout::GLOBAL_ORDER)
    , ranges_(subarray.ranges())
    , mult_extents_(root_subarray.dim_num())
    , start_coords_(start_coords) {
  init_coords();
  init_cell_slab_lengths();

  if (num_range_threads != 1) {
    // Compute cells per dimensions.
    std::vector<uint64_t> cell_idx(dim_num_);
    for (int d = 0; d < dim_num_; ++d) {
      for (auto& range : ranges_[d]) {
        cell_idx[d] += range.end_ - range.start_ + 1;
      }
    }

    // Compute total number of slabs not including the last dimension as we
    // do not split on the last dimension.
    uint64_t num_slabs = 1;
    if (layout_ == Layout::ROW_MAJOR) {
      for (int d = 0; d < dim_num_ - 1; ++d) {
        num_slabs *= cell_idx[d];
      }
    } else {  // COL_MAJOR
      for (int d = dim_num_ - 1; d > 0; --d) {
        num_slabs *= cell_idx[d];
      }
    }

    // Prevent processing past the end in case there are more threads than
    // slabs.
    if (num_slabs != 0 && range_thread_idx < num_slabs) {
      // Compute the cells to process.
      auto part_num = std::min(num_slabs, num_range_threads);
      uint64_t min = (range_thread_idx * num_slabs + part_num - 1) / part_num;
      uint64_t max = std::min(
          ((range_thread_idx + 1) * num_slabs + part_num - 1) / part_num,
          num_slabs);

      last_ = max == num_slabs;
      num_ = max - min;

      // Compute range coords and global offset.
      if (layout_ == Layout::ROW_MAJOR) {
        global_offset_ = min * cell_idx[dim_num_ - 1];
        cell_idx[dim_num_ - 1] = 0;
        for (int d = dim_num_ - 2; d >= 0; --d) {
          uint64_t temp = min % cell_idx[d];
          min /= cell_idx[d];
          cell_idx[d] = temp;
        }
      } else {  // COL_MAJOR
        global_offset_ = min * cell_idx[0];
        cell_idx[0] = 0;
        for (int d = 1; d < dim_num_; ++d) {
          uint64_t temp = min % cell_idx[d];
          min /= cell_idx[d];
          cell_idx[d] = temp;
        }
      }

      for (unsigned d = 0; d < cell_idx.size(); ++d) {
        range_coords_[d] = 0;
        for (auto& range : ranges_[d]) {
          auto length = static_cast<uint64_t>(range.end_ - range.start_ + 1);
          if (cell_idx[d] < length) {
            cell_slab_coords_[d] = range.start_ + static_cast<T>(cell_idx[d]);
            break;
          } else {
            range_coords_[d]++;
            cell_idx[d] -= length;
          }
        }
      }

      if (layout_ == Layout::ROW_MAJOR) {
        num_ *= ranges_[dim_num_ - 1].size();
      } else {  // COL_MAJOR
        num_ *= ranges_[0].size();
      }
    } else {
      num_ = 0;
    }
  }

  uint64_t mult = 1;
  if (cell_order == Layout::COL_MAJOR) {
    for (int32_t d = 0; d < dim_num_; d++) {
      mult_extents_[d] = mult;
      mult *= tile_extents[d];
    }
  } else {
    for (auto d = dim_num_ - 1; d >= 0; d--) {
      mult_extents_[d] = mult;
      mult *= tile_extents[d];
    }
  }

  update_cell_slab();
}

/* ****************************** */
/*               API              */
/* ****************************** */

template <class T>
void TileCellSlabIter<T>::operator++() {
  num_--;
  // If at the end, do nothing
  if (num_ == 0 || end_) {
    return;
  }

  // Advance the iterator
  if (layout_ == Layout::ROW_MAJOR) {
    advance_row();
  } else {
    advance_col();
  }

  if (!end_) {
    update_cell_slab();
  }
}

/* ****************************** */
/*          PRIVATE METHODS       */
/* ****************************** */

template <class T>
void TileCellSlabIter<T>::advance_col() {
  for (int i = 0; i < dim_num_; ++i) {
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
      if (i == dim_num_ - 1) {
        end_ = true;
        return;
      }

      range_coords_[i] = 0;
      cell_slab_coords_[i] = ranges_[i][0].start_;
    }
  }
}

template <class T>
void TileCellSlabIter<T>::advance_row() {
  for (int i = dim_num_ - 1; i >= 0; --i) {
    cell_slab_coords_[i] +=
        (i == dim_num_ - 1) ? cell_slab_lengths_[range_coords_[i]] : 1;
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
void TileCellSlabIter<T>::init_cell_slab_lengths() {
  if (layout_ == Layout::ROW_MAJOR || layout_ == Layout::UNORDERED) {
    auto range_num = ranges_[dim_num_ - 1].size();
    cell_slab_lengths_.resize(range_num);
    for (size_t i = 0; i < range_num; ++i) {
      cell_slab_lengths_[i] =
          ranges_[dim_num_ - 1][i].end_ - ranges_[dim_num_ - 1][i].start_ + 1;
    }
  } else {
    assert(layout_ == Layout::COL_MAJOR);
    auto range_num = ranges_[0].size();
    cell_slab_lengths_.resize(range_num);
    for (size_t i = 0; i < range_num; ++i) {
      cell_slab_lengths_[i] = ranges_[0][i].end_ - ranges_[0][i].start_ + 1;
    }
  }
}

template <class T>
void TileCellSlabIter<T>::init_coords() {
  range_coords_.resize(dim_num_);
  cell_slab_coords_.resize(dim_num_);
  for (int i = 0; i < dim_num_; ++i) {
    range_coords_[i] = 0;
    cell_slab_coords_[i] = ranges_[i][0].start_;
  }
}

template <class T>
void TileCellSlabIter<T>::update_cell_slab() {
  // Compute the cell slab length.
  cell_slab_length_ = (layout_ == Layout::ROW_MAJOR) ?
                          cell_slab_lengths_[range_coords_[dim_num_ - 1]] :
                          cell_slab_lengths_[range_coords_[0]];

  // Compute position in tile.
  pos_in_tile_ = 0;
  for (int32_t d = 0; d < dim_num_; d++) {
    pos_in_tile_ +=
        mult_extents_[d] * (cell_slab_coords_[d] - start_coords_[d]);
  }

  // Compute destination offset for row/col orders.
  if (!global_order_) {
    dest_offset_row_col_ = 0;
    if (num_ranges_ == 1) {
      if (layout_ == Layout::COL_MAJOR) {
        for (int32_t d = 0; d < dim_num_; d++) {
          const auto& ri = range_info_[d];
          dest_offset_row_col_ +=
              ri.multiplier_ * (cell_slab_coords_[d] - ri.mins_[0]);
        }
      } else {
        for (int32_t d = dim_num_ - 1; d >= 0; d--) {
          const auto& ri = range_info_[d];
          dest_offset_row_col_ +=
              ri.multiplier_ * (cell_slab_coords_[d] - ri.mins_[0]);
        }
      }
    } else {
      if (layout_ == Layout::COL_MAJOR) {
        for (int32_t d = 0; d < dim_num_; d++) {
          const auto& ri = range_info_[d];
          auto r = original_range_idx_[d][range_coords_[d]];
          dest_offset_row_col_ +=
              ri.multiplier_ *
              (cell_slab_coords_[d] - ri.mins_[r] + ri.cell_offsets_[r]);
        }
      } else {
        for (int32_t d = dim_num_ - 1; d >= 0; d--) {
          const auto& ri = range_info_[d];
          auto r = original_range_idx_[d][range_coords_[d]];
          dest_offset_row_col_ +=
              ri.multiplier_ *
              (cell_slab_coords_[d] - ri.mins_[r] + ri.cell_offsets_[r]);
        }
      }
    }
  }
}

// Explicit template instantiations
template class TileCellSlabIter<int8_t>;
template class TileCellSlabIter<uint8_t>;
template class TileCellSlabIter<int16_t>;
template class TileCellSlabIter<uint16_t>;
template class TileCellSlabIter<int32_t>;
template class TileCellSlabIter<uint32_t>;
template class TileCellSlabIter<int64_t>;
template class TileCellSlabIter<uint64_t>;

}  // namespace sm
}  // namespace tiledb
