/**
 * @file   comparators.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB, Inc.
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
class RowCmp {
 public:
  /** Constructor. */
  RowCmp(const Domain* domain)
      : domain_(domain)
      , dim_num_(domain->dim_num()) {
  }

  /**
   * Comparison operator.
   *
   * @param a The first coordinate.
   * @param b The second coordinate.
   * @return `true` if `a` precedes `b` and `false` otherwise.
   */
  bool operator()(const ResultCoords& a, const ResultCoords& b) const {
    for (unsigned int d = 0; d < dim_num_; ++d) {
      auto res = domain_->cell_order_cmp(d, a.coord(d), b.coord(d));

      if (res == -1)
        return true;
      if (res == 1)
        return false;
      // else same coordinate on dimension d --> continue
    }

    return false;
  }

 private:
  /** The domain. */
  const Domain* domain_;
  /** The number of dimensions. */
  unsigned dim_num_;
};

/** Wrapper of comparison function for sorting coords on col-major order. */
class ColCmp {
 public:
  /** Constructor. */
  ColCmp(const Domain* domain)
      : domain_(domain)
      , dim_num_(domain->dim_num()) {
  }

  /**
   * Comparison operator.
   *
   * @param a The first coordinate.
   * @param b The second coordinate.
   * @return `true` if `a` precedes `b` and `false` otherwise.
   */
  bool operator()(const ResultCoords& a, const ResultCoords& b) const {
    for (unsigned int d = dim_num_ - 1;; --d) {
      auto res = domain_->cell_order_cmp(d, a.coord(d), b.coord(d));

      if (res == -1)
        return true;
      if (res == 1)
        return false;
      // else same coordinate on dimension d --> continue

      if (d == 0)
        break;
    }

    return false;
  }

 private:
  /** The domain. */
  const Domain* domain_;
  /** The number of dimensions. */
  unsigned dim_num_;
};

/**
 * Wrapper of comparison function for sorting coords on the global order
 * of some domain.
 */
class GlobalCmp {
 public:
  /**
   * Constructor.
   *
   * @param domain The array domain.
   * @param buff The buffer containing the actual values, used
   *     in positional comparisons.
   */
  GlobalCmp(const Domain* domain)
      : domain_(domain) {
    dim_num_ = domain->dim_num();
    tile_order_ = domain->tile_order();
    cell_order_ = domain->cell_order();
    coord_buffs_ = nullptr;
  }

  /**
   * Constructor.
   *
   * @param domain The array domain.
   * @param coord_buffs The coordinate buffers, one per dimension, containing
   *     the actual values, used in positional comparisons.
   */
  GlobalCmp(const Domain* domain, const std::vector<const void*>* coord_buffs)
      : domain_(domain)
      , coord_buffs_(coord_buffs) {
  }

  /**
   * Comparison operator for a vector of `ResultCoords`.
   *
   * @param a The first coordinate.
   * @param b The second coordinate.
   * @return `true` if `a` precedes `b` and `false` otherwise.
   */
  bool operator()(const ResultCoords& a, const ResultCoords& b) const {
    // Compare tile order first
    if (tile_order_ == Layout::ROW_MAJOR) {
      for (unsigned d = 0; d < dim_num_; ++d) {
        auto res = domain_->tile_order_cmp(d, a.coord(d), b.coord(d));

        if (res == -1)
          return true;
        if (res == 1)
          return false;
        // else same tile on dimension d --> continue
      }
    } else {  // COL_MAJOR
      assert(tile_order_ == Layout::COL_MAJOR);
      for (unsigned d = dim_num_ - 1;; --d) {
        auto res = domain_->tile_order_cmp(d, a.coord(d), b.coord(d));

        if (res == -1)
          return true;
        if (res == 1)
          return false;
        // else same tile on dimension d --> continue

        if (d == 0)
          break;
      }
    }

    // Compare cell order
    if (cell_order_ == Layout::ROW_MAJOR) {
      for (unsigned d = 0; d < dim_num_; ++d) {
        auto res = domain_->cell_order_cmp(d, a.coord(d), b.coord(d));

        if (res == -1)
          return true;
        if (res == 1)
          return false;
        // else same tile on dimension d --> continue
      }
    } else {  // COL_MAJOR
      assert(cell_order_ == Layout::COL_MAJOR);
      for (unsigned d = dim_num_ - 1;; --d) {
        auto res = domain_->cell_order_cmp(d, a.coord(d), b.coord(d));

        if (res == -1)
          return true;
        if (res == 1)
          return false;
        // else same tile on dimension d --> continue

        if (d == 0)
          break;
      }
    }

    return false;
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
    assert(coord_buffs_ != nullptr);
    auto tile_cmp = domain_->tile_order_cmp(*coord_buffs_, a, b);

    if (tile_cmp == -1)
      return true;
    if (tile_cmp == 1)
      return false;
    // else tile_cmp == 0 --> continue

    // Compare cell order
    auto cell_cmp = domain_->cell_order_cmp(*coord_buffs_, a, b);
    return cell_cmp == -1;
  }

 private:
  /** The domain. */
  const Domain* domain_;
  /** The number of dimensions. */
  unsigned dim_num_;
  /** The tile order. */
  Layout tile_order_;
  /** The cell order. */
  Layout cell_order_;
  /**
   * The coordinate buffers, one per dimension, sorted in the order the
   * dimensions are defined in the array schema.
   */
  const std::vector<const void*>* coord_buffs_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_COMPARATORS_H
