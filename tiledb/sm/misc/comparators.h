/**
 * @file   comparators.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
 * Defines custom comparators to be used in cell position sorting in the case
 * of sparse arrays.
 */

#ifndef TILEDB_COMPARATORS_H
#define TILEDB_COMPARATORS_H

#include <algorithm>
#include <cinttypes>
#include <iostream>
#include <vector>

#include "tiledb/sm/query/reader.h"

namespace tiledb {
namespace sm {

/** Wrapper of comparison function for sorting coords on row-major order. */
template <class T>
class RowCmp {
 public:
  /**
   * Constructor.
   *
   * @param dim_num The number of dimensions of the coords.
   */
  RowCmp(unsigned dim_num)
      : dim_num_(dim_num) {
  }

  /**
   * Comparison operator.
   *
   * @param a The first coordinate.
   * @param b The second coordinate.
   * @return `true` if `a` precedes `b` and `false` otherwise.
   */
  bool operator()(
      const Reader::OverlappingCoords<T>& a,
      const Reader::OverlappingCoords<T>& b) const {
    for (unsigned int i = 0; i < dim_num_; ++i) {
      if (a.coords_[i] < b.coords_[i])
        return true;
      if (a.coords_[i] > b.coords_[i])
        return false;
      // else a.coords_[i] == b.coords_[i] --> continue
    }

    return false;
  }

 private:
  /** The number of dimensions. */
  unsigned dim_num_;
};

/** Wrapper of comparison function for sorting coords on col-major order. */
template <class T>
class ColCmp {
 public:
  /**
   * Constructor.
   *
   * @param dim_num The number of dimensions of the coords.
   */
  ColCmp(unsigned dim_num)
      : dim_num_(dim_num) {
  }

  /**
   * Comparison operator.
   *
   * @param a The first coordinate.
   * @param b The second coordinate.
   * @return `true` if `a` precedes `b` and `false` otherwise.
   */
  bool operator()(
      const Reader::OverlappingCoords<T>& a,
      const Reader::OverlappingCoords<T>& b) const {
    for (unsigned int i = dim_num_ - 1;; --i) {
      if (a.coords_[i] < b.coords_[i])
        return true;
      if (a.coords_[i] > b.coords_[i])
        return false;
      // else a.coords_[i] == b.coords_[i] --> continue

      if (i == 0)
        break;
    }

    return false;
  }

 private:
  /** The number of dimensions. */
  unsigned dim_num_;
};

/**
 * Wrapper of comparison function for sorting coords on the global order
 * of some domain.
 */
template <class T>
class GlobalCmp {
 public:
  /**
   * Constructor.
   *
   * @param dim_num The number of dimensions of the coords.
   */
  GlobalCmp(const Domain* domain, const T* buff = nullptr)
      : domain_(domain)
      , buff_(buff) {
    dim_num_ = domain->dim_num();
  }

  /**
   * Comparison operator for a vector of `OverlappingCoords`.
   *
   * @param a The first coordinate.
   * @param b The second coordinate.
   * @return `true` if `a` precedes `b` and `false` otherwise.
   */
  bool operator()(
      const Reader::OverlappingCoords<T>& a,
      const Reader::OverlappingCoords<T>& b) const {
    // Compare tile order first
    auto tile_cmp =
        domain_->tile_order_cmp_tile_coords<T>(a.tile_coords_, b.tile_coords_);

    if (tile_cmp == -1)
      return true;
    if (tile_cmp == 1)
      return false;
    // else tile_cmp == 0 --> continue

    // Compare cell order
    auto cell_cmp = domain_->cell_order_cmp(a.coords_, b.coords_);
    return cell_cmp == -1;
  }

  /**
   * Comparison operator for a vector of integer positions.
   * Here `buff_` is **not** `nullptr` and a position corresponds to
   * coordinates in `buff_`.
   *
   * @param a The first coordinate position.
   * @param b The second coordinate position.
   * @return `true` if coordinates at `a` precedes coordinates at `b`,
   *     and `false` otherwise.
   */
  bool operator()(uint64_t a, uint64_t b) const {
    // Get coordinates
    const T* coords_a = &buff_[a * dim_num_];
    const T* coords_b = &buff_[b * dim_num_];
    // Compare tile order first
    auto tile_cmp = domain_->tile_order_cmp<T>(coords_a, coords_b);

    if (tile_cmp == -1)
      return true;
    if (tile_cmp == 1)
      return false;
    // else tile_cmp == 0 --> continue

    // Compare cell order
    auto cell_cmp = domain_->cell_order_cmp(coords_a, coords_b);
    return cell_cmp == -1;
  }

 private:
  /** The domain. */
  const Domain* domain_;
  /** A buffer - not applicable to sorting `OverlappingCoords`. */
  const T* buff_;
  /** The number of dimensions. */
  unsigned dim_num_;
};

/** Wrapper of comparison function for sorting dense cell ranges. */
template <class T>
class DenseCellRangeCmp {
 public:
  /** Enumerator specifying comparing on range start or end. */
  enum CmpOn { RANGE_START, RANGE_END };

  /** Constructor. */
  DenseCellRangeCmp(const Domain* domain, Layout layout)
      : cell_order_(domain->cell_order())
      , dim_num_(domain->dim_num())
      , domain_((T*)domain->domain())
      , layout_(layout)
      , tile_extents_((T*)domain->tile_extents()) {
    assert(layout_ == Layout::ROW_MAJOR || layout_ == Layout::COL_MAJOR);

    pos_coords_.resize(dim_num_);

    // Compute cell offsets
    uint64_t cell_num;
    if (cell_order_ == Layout::COL_MAJOR) {
      cell_offsets_.push_back(1);
      if (dim_num_ > 1) {
        for (unsigned int i = 1; i < dim_num_; ++i) {
          cell_num = tile_extents_[i - 1];
          cell_offsets_.push_back(cell_offsets_.back() * cell_num);
        }
      }
    } else {
      cell_offsets_.push_back(1);
      if (dim_num_ > 1) {
        for (unsigned int i = dim_num_ - 2;; --i) {
          cell_num = tile_extents_[i + 1];
          cell_offsets_.push_back(cell_offsets_.back() * cell_num);
          if (i == 0)
            break;
        }
      }
      std::reverse(cell_offsets_.begin(), cell_offsets_.end());
    }
  }

  /**
   * Comparison operator.
   *
   * @param a The first dense cell range.
   * @param b The second dense cell range.
   * @return `true` if `a` succeeds `b` and `false` otherwise.
   */
  bool operator()(
      const Reader::DenseCellRange<T>& a, const Reader::DenseCellRange<T>& b) {
    if (a.coords_start_ == nullptr || b.coords_start_ == nullptr) {
      if (a.start_ < b.start_)
        return false;
      if (a.start_ > b.start_)
        return true;
    } else {  // Need to compare the coords
      if (layout_ == Layout::ROW_MAJOR) {
        for (unsigned i = 0; i < dim_num_; ++i) {
          if (a.coords_start_[i] < b.coords_start_[i])
            return false;
          if (a.coords_start_[i] > b.coords_start_[i])
            return true;
        }
      } else {  // layout_ == Layout::COL_MAJOR
        for (unsigned int i = dim_num_ - 1;; --i) {
          if (a.coords_start_[i] < b.coords_start_[i])
            return false;
          if (a.coords_start_[i] > b.coords_start_[i])
            return true;

          if (i == 0)
            break;
        }
      }
    }
    return a.fragment_idx_ < b.fragment_idx_;
  }

  /**
   * Returns `true` if `a` precedes range position `pos` on the range
   * start or end specified by `cmp_on`.
   */
  bool precedes(
      const Reader::DenseCellRange<T>& a, uint64_t pos, CmpOn cmp_on) {
    auto a_pos = (cmp_on == RANGE_START) ? a.start_ : a.end_;
    auto a_coords = (cmp_on == RANGE_START) ? a.coords_start_ : a.coords_end_;

    if (a_coords == nullptr)
      return a_pos < pos;

    // Compute coordinates from `pos`
    uint64_t pos_rem = pos;
    if (cell_order_ == Layout::ROW_MAJOR) {
      for (unsigned int i = 0; i < dim_num_; ++i) {
        pos_coords_[i] = pos_rem / cell_offsets_[i];
        pos_rem -= pos_coords_[i] * cell_offsets_[i];
      }
    } else {
      for (unsigned int i = dim_num_ - 1;; --i) {
        pos_coords_[i] = pos_rem / cell_offsets_[i];
        pos_rem -= pos_coords_[i] * cell_offsets_[i];
        if (i == 0)
          break;
      }
    }

    // Compare the coords
    if (layout_ == Layout::ROW_MAJOR) {
      for (unsigned int i = 0; i < dim_num_; ++i) {
        T a_norm_coords = (a_coords[i] - domain_[2 * i]) % tile_extents_[i];
        if (a_norm_coords < pos_coords_[i])
          return true;
        if (a_norm_coords > pos_coords_[i])
          return false;
      }
    } else {  // layout_ == Layout::COL_MAJOR
      for (unsigned int i = dim_num_ - 1;; --i) {
        T a_norm_coords = (a_coords[i] - domain_[2 * i]) % tile_extents_[i];
        if (a_norm_coords < pos_coords_[i])
          return true;
        if (a_norm_coords > pos_coords_[i])
          return false;
        if (i == 0)
          break;
      }
    }

    return false;
  }

  /**
   * Returns `true` if `a` succeeds range position `pos` on the range
   * start or end specified by `cmp_on`.
   */
  bool succeeds(
      const Reader::DenseCellRange<T>& a, uint64_t pos, CmpOn cmp_on) {
    auto a_pos = (cmp_on == RANGE_START) ? a.start_ : a.end_;
    auto a_coords = (cmp_on == RANGE_START) ? a.coords_start_ : a.coords_end_;

    if (a_coords == nullptr)
      return a_pos > pos;

    // Compute coordinates from `pos`
    uint64_t pos_rem = pos;
    if (cell_order_ == Layout::ROW_MAJOR) {
      for (unsigned int i = 0; i < dim_num_; ++i) {
        pos_coords_[i] = pos_rem / cell_offsets_[i];
        pos_rem -= pos_coords_[i] * cell_offsets_[i];
      }
    } else {
      for (unsigned int i = dim_num_ - 1;; --i) {
        pos_coords_[i] = pos_rem / cell_offsets_[i];
        pos_rem -= pos_coords_[i] * cell_offsets_[i];
        if (i == 0)
          break;
      }
    }

    // Compare the coords
    if (layout_ == Layout::ROW_MAJOR) {
      for (unsigned int i = 0; i < dim_num_; ++i) {
        T a_norm_coords = (a_coords[i] - domain_[2 * i]) % tile_extents_[i];
        if (a_norm_coords > pos_coords_[i])
          return true;
        if (a_norm_coords < pos_coords_[i])
          return false;
      }
    } else {  // layout_ == Layout::COL_MAJOR
      for (unsigned int i = dim_num_ - 1;; --i) {
        T a_norm_coords = (a_coords[i] - domain_[2 * i]) % tile_extents_[i];
        if (a_norm_coords > pos_coords_[i])
          return true;
        if (a_norm_coords < pos_coords_[i])
          return false;
        if (i == 0)
          break;
      }
    }

    return false;
  }

 private:
  /** The array cell order. */
  Layout cell_order_;
  /** The number of dimensions. */
  unsigned dim_num_;
  /** The array domain. */
  const T* domain_;
  /** The query cell layout. */
  Layout layout_;
  /** Needed for comparisons with cell positions. */
  std::vector<uint64_t> cell_offsets_;
  /** Auxiliary coordinates used in `precedes`/`succeeds` functions. */
  std::vector<T> pos_coords_;
  /** The tile extents. */
  const T* tile_extents_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_COMPARATORS_H
