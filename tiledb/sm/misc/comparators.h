/**
 * @file   comparators.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2019 TileDB, Inc.
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
  bool operator()(const ResultCoords<T>& a, const ResultCoords<T>& b) const {
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
  bool operator()(const ResultCoords<T>& a, const ResultCoords<T>& b) const {
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
   * @param domain The array domain.
   * @param buff The buffer containing the actual values, used
   *     in positional comparisons.
   */
  GlobalCmp(const Domain* domain, const T* buff = nullptr)
      : domain_(domain)
      , buff_(buff) {
    dim_num_ = domain->dim_num();
  }

  /**
   * Comparison operator for a vector of `ResultCoords`.
   *
   * @param a The first coordinate.
   * @param b The second coordinate.
   * @return `true` if `a` precedes `b` and `false` otherwise.
   */
  bool operator()(const ResultCoords<T>& a, const ResultCoords<T>& b) const {
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
  /** A buffer - not applicable to sorting `ResultCoords`. */
  const T* buff_;
  /** The number of dimensions. */
  unsigned dim_num_;
};

/**
 * Wrapper of comparison function for sorting coords on the global order
 * of some domain.
 */
class GlobalCmp2 {
 public:
  /**
   * Constructor.
   *
   * @param domain The array domain.
   * @param coord_buffs The coordinate buffers, one per dimension, containing
   *     the actual values, used in positional comparisons.
   */
  GlobalCmp2(const Domain* domain, const std::vector<const void*>& coord_buffs)
      : domain_(domain)
      , coord_buffs_(coord_buffs) {
  }

  /**
   * Positional comparison operator.
   *
   * @param a The first cell position.
   * @param b The second cell position.
   * @return `true` if cell at `a` across all coordinate buffers precedes
   *     cell at `b`, and `false` otherwise.
   */
  bool operator()(uint64_t a, uint64_t b) const {
    // Compare tile order first
    auto tile_cmp = domain_->tile_order_cmp(coord_buffs_, a, b);

    if (tile_cmp == -1)
      return true;
    if (tile_cmp == 1)
      return false;
    // else tile_cmp == 0 --> continue

    // Compare cell order
    auto cell_cmp = domain_->cell_order_cmp(coord_buffs_, a, b);
    return cell_cmp == -1;
  }

 private:
  /** The domain. */
  const Domain* domain_;
  /**
   * The coordinate buffers, one per dimension, sorted in the order the
   * dimensions are defined in the array schema.
   */
  const std::vector<const void*>& coord_buffs_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_COMPARATORS_H
