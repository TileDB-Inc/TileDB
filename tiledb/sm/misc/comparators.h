/**
 * @file   comparators.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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

#include <cinttypes>
#include <vector>

#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/query/result_coords.h"
#include "tiledb/sm/query/sparse_index_reader_base.h"

using namespace tiledb::common;

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
      auto res = domain_->cell_order_cmp(d, a, b);

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
      auto res = domain_->cell_order_cmp(d, a, b);

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

/** Wrapper of comparison function for sorting coords on Hilbert values. */
class HilbertCmp {
 public:
  /** Constructor. */
  HilbertCmp(
      const Domain* domain,
      const std::vector<const QueryBuffer*>* buffs,
      const std::vector<uint64_t>* hilbert_values)
      : buffs_(buffs)
      , domain_(domain)
      , hilbert_values_(hilbert_values) {
    dim_num_ = domain->dim_num();
  }

  /** Constructor. */
  HilbertCmp(
      const Domain* domain, std::vector<ResultCoords>::iterator iter_begin)
      : domain_(domain)
      , iter_begin_(iter_begin) {
    dim_num_ = domain->dim_num();
  }

  /** Constructor. */
  HilbertCmp(const Domain* domain)
      : domain_(domain) {
    dim_num_ = domain->dim_num();
  }

  /**
   * Positional comparison operator.
   *
   * @param a The first cell position.
   * @param b The second cell position.
   * @return `true` if cell at `a` precedes
   *     cell at `b` on the hilbert value, and `false` otherwise.
   */
  bool operator()(uint64_t a, uint64_t b) const {
    assert(hilbert_values_ != nullptr);
    if ((*hilbert_values_)[a] < (*hilbert_values_)[b])
      return true;
    else if ((*hilbert_values_)[a] > (*hilbert_values_)[b])
      return false;
    // else the hilbert values are equal

    // Compare cell order
    auto cell_cmp = domain_->cell_order_cmp(*buffs_, a, b);
    return cell_cmp == -1;
  }

  /**
   * (Hilbert, iterator offset) comparison operator.
   *
   * @param a The first (Hilbert, iterator offset).
   * @param b The second (Hilbert, iterator offset).
   * @return `true` if cell represented by `a` across precedes
   *     cell at `b` on the hilbert value, and `false` otherwise.
   */
  bool operator()(
      const std::pair<uint64_t, uint64_t>& a,
      const std::pair<uint64_t, uint64_t>& b) const {
    assert(hilbert_values_ != nullptr);
    if (a.first < b.first)
      return true;
    else if (a.first > b.first)
      return false;
    // else the hilbert values are equal

    // Compare cell order on row-major to break the tie
    const auto& a_coord = *(iter_begin_ + a.second);
    const auto& b_coord = *(iter_begin_ + b.second);
    for (unsigned d = 0; d < dim_num_; ++d) {
      auto res = domain_->cell_order_cmp(d, a_coord, b_coord);
      if (res == -1)
        return true;
      if (res == 1)
        return false;
      // else same tile on dimension d --> continue
    }

    return false;
  }

  /**
   * Positional comparison operator.
   *
   * @param a The first coordinate.
   * @param b The second coordinate.
   * @return `true` if `a` precedes `b` and `false` otherwise.
   */
  bool operator()(const ResultCoords& a, const ResultCoords& b) const {
    auto hilbert_a =
        ((ResultTileWithBitmap<uint8_t>*)a.tile_)->hilbert_values_[a.pos_];
    auto hilbert_b =
        ((ResultTileWithBitmap<uint8_t>*)b.tile_)->hilbert_values_[b.pos_];
    if (hilbert_a < hilbert_b)
      return true;
    else if (hilbert_a > hilbert_b)
      return false;
    // else the hilbert values are equal

    // Compare cell order on row-major to break the tie
    for (unsigned d = 0; d < dim_num_; ++d) {
      auto res = domain_->cell_order_cmp(d, a, b);
      if (res == -1)
        return true;
      if (res == 1)
        return false;
      // else same tile on dimension d --> continue
    }

    return false;
  }

 private:
  /**
   * The coordinate buffers, one per dimension, sorted in the order the
   * dimensions are defined in the array schema.
   */
  const std::vector<const QueryBuffer*>* buffs_;
  /** The array domain. */
  const Domain* domain_;
  /** The number of dimensions. */
  unsigned dim_num_;
  /** Start iterator of result coords vector. */
  std::vector<ResultCoords>::iterator iter_begin_;
  /** The Hilbert values vector. */
  const std::vector<uint64_t>* hilbert_values_;
};

/**
 * Wrapper of comparison function for sorting coords on Hilbert values
 * in reverse order.
 */
class HilbertCmpReverse {
 public:
  /**
   * Constructor.
   *
   * @param domain The array domain.
   */
  HilbertCmpReverse(const Domain* domain)
      : cmp_(domain) {
  }

  /**
   * Comparison operator for a vector of `ResultCoords`.
   *
   * @param a The first coordinate.
   * @param b The second coordinate.
   * @return `true` if `a` precedes `b` and `false` otherwise.
   */
  bool operator()(const ResultCoords& a, const ResultCoords& b) const {
    return !cmp_.operator()(a, b);
  }

 private:
  /** HilbertCmp. */
  HilbertCmp cmp_;
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
    buffs_ = nullptr;
  }

  /**
   * Constructor.
   *
   * @param domain The array domain.
   * @param buffs The coordinate query buffers, one per dimension.
   */
  GlobalCmp(const Domain* domain, const std::vector<const QueryBuffer*>* buffs)
      : domain_(domain)
      , buffs_(buffs) {
  }

  /**
   * Comparison operator for a vector of `ResultCoords`.
   *
   * @param a The first coordinate.
   * @param b The second coordinate.
   * @return `true` if `a` precedes `b` and `false` otherwise.
   */
  bool operator()(const ResultCoords& a, const ResultCoords& b) const {
    if (tile_order_ == Layout::ROW_MAJOR) {
      for (unsigned d = 0; d < dim_num_; ++d) {
        // Not applicable to var-sized dimensions
        if (domain_->dimension(d)->var_size())
          continue;

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
        // Not applicable to var-sized dimensions
        if (domain_->dimension(d)->var_size())
          continue;

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
        auto res = domain_->cell_order_cmp(d, a, b);

        if (res == -1)
          return true;
        if (res == 1)
          return false;
        // else same tile on dimension d --> continue
      }
    } else {  // COL_MAJOR
      assert(cell_order_ == Layout::COL_MAJOR);
      for (unsigned d = dim_num_ - 1;; --d) {
        auto res = domain_->cell_order_cmp(d, a, b);

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
    assert(buffs_ != nullptr);
    auto tile_cmp = domain_->tile_order_cmp(*buffs_, a, b);

    if (tile_cmp == -1)
      return true;
    if (tile_cmp == 1)
      return false;
    // else tile_cmp == 0 --> continue

    // Compare cell order
    auto cell_cmp = domain_->cell_order_cmp(*buffs_, a, b);
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
  const std::vector<const QueryBuffer*>* buffs_;
};

/**
 * Wrapper of comparison function for sorting coords on the global order
 * of some domain in reverse order.
 */
class GlobalCmpReverse {
 public:
  /**
   * Constructor.
   *
   * @param domain The array domain.
   * @param buff The buffer containing the actual values, used
   *     in positional comparisons.
   */
  GlobalCmpReverse(const Domain* domain)
      : cmp_(domain) {
  }

  /**
   * Comparison operator for a vector of `ResultCoords`.
   *
   * @param a The first coordinate.
   * @param b The second coordinate.
   * @return `true` if `a` precedes `b` and `false` otherwise.
   */
  bool operator()(const ResultCoords& a, const ResultCoords& b) const {
    return !cmp_.operator()(a, b);
  }

 private:
  /** GlobalCmp. */
  GlobalCmp cmp_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_COMPARATORS_H
