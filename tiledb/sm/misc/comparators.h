/**
 * @file   comparators.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2022 TileDB, Inc.
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
#include "tiledb/sm/query/domain_buffer.h"
#include "tiledb/sm/query/result_coords.h"
#include "tiledb/sm/query/sparse_index_reader_base.h"

using namespace tiledb::common;

namespace tiledb::sm {

class CellCmpBase {
 protected:
  /** The domain. */
  const Domain& domain_;

  /** The number of dimensions. */
  unsigned dim_num_;

 public:
  explicit CellCmpBase(const Domain& domain)
      : domain_(domain)
      , dim_num_(domain.dim_num()) {
  }

  [[nodiscard]] int cell_order_cmp_RC(
      unsigned int d, const ResultCoords& a, const ResultCoords& b) const {
    const auto& dim{*(domain_.dimension_ptr(d))};
    auto v1{a.dimension_datum(dim, d)};
    auto v2{b.dimension_datum(dim, d)};
    return domain_.cell_order_cmp(d, v1, v2);
  }
};

/** Wrapper of comparison function for sorting coords on row-major order. */
class RowCmp : CellCmpBase {
 public:
  /** Constructor. */
  explicit RowCmp(const Domain& domain)
      : CellCmpBase(domain) {
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
      auto res = cell_order_cmp_RC(d, a, b);

      if (res == -1)
        return true;
      if (res == 1)
        return false;
      // else same coordinate on dimension d --> continue
    }

    return false;
  }
};

/** Wrapper of comparison function for sorting coords on col-major order. */
class ColCmp : CellCmpBase {
 public:
  /** Constructor. */
  explicit ColCmp(const Domain& domain)
      : CellCmpBase(domain) {
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
      auto res = cell_order_cmp_RC(d, a, b);

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
};

/** Wrapper of comparison function for sorting coords on Hilbert values. */
class HilbertCmp : protected CellCmpBase {
 public:
  /** Constructor. */
  HilbertCmp(const Domain& domain)
      : CellCmpBase(domain) {
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
      auto res = cell_order_cmp_RC(d, a, b);
      if (res == -1)
        return true;
      if (res == 1)
        return false;
      // else same tile on dimension d --> continue
    }

    return false;
  }
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
  HilbertCmpReverse(const Domain& domain)
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
 * (Hilbert) comparison (Cmp) function class on domain values retrieved with a
 * `ResultCoords` iterator (RCI).
 */
class HilbertCmpRCI : protected CellCmpBase {
  /**
   * Start iterator of result coords vector.
   */
  const std::vector<ResultCoords>::iterator& iter_begin_;

 public:
  /** Constructor. */
  HilbertCmpRCI(
      const Domain& domain,
      const std::vector<ResultCoords>::iterator& iter_begin)
      : CellCmpBase(domain)
      , iter_begin_(iter_begin) {
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
    if (a.first < b.first)
      return true;
    else if (a.first > b.first)
      return false;
    // else the hilbert values are equal

    // Compare cell order on row-major to break the tie
    const auto& a_coord = *(iter_begin_ + a.second);
    const auto& b_coord = *(iter_begin_ + b.second);
    for (unsigned d = 0; d < dim_num_; ++d) {
      auto res = cell_order_cmp_RC(d, a_coord, b_coord);
      if (res == -1)
        return true;
      if (res == 1)
        return false;
      // else same tile on dimension d --> continue
    }

    return false;
  }
};

/**
 * Wrapper of comparison function for sorting coords on the global order
 * of some domain.
 */
class GlobalCmp : protected CellCmpBase {
 public:
  /**
   * Constructor.
   *
   * @param domain The array domain.
   * @param buff The buffer containing the actual values, used
   *     in positional comparisons.
   */
  explicit GlobalCmp(const Domain& domain)
      : CellCmpBase(domain) {
    tile_order_ = domain.tile_order();
    cell_order_ = domain.cell_order();
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
        if (domain_.dimension_ptr(d)->var_size())
          continue;

        auto res = domain_.tile_order_cmp(d, a.coord(d), b.coord(d));

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
        if (domain_.dimension_ptr(d)->var_size())
          continue;

        auto res = domain_.tile_order_cmp(d, a.coord(d), b.coord(d));

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
        auto res = cell_order_cmp_RC(d, a, b);

        if (res == -1)
          return true;
        if (res == 1)
          return false;
        // else same tile on dimension d --> continue
      }
    } else {  // COL_MAJOR
      assert(cell_order_ == Layout::COL_MAJOR);
      for (unsigned d = dim_num_ - 1;; --d) {
        auto res = cell_order_cmp_RC(d, a, b);

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

 private:
  /** The tile order. */
  Layout tile_order_;
  /** The cell order. */
  Layout cell_order_;
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
  explicit GlobalCmpReverse(const Domain& domain)
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

/**
 * Base class for comparison function classes whose operands are domain values
 * residing in QueryBuffer objects.
 */
class DomainValueCmpBaseQB {
 protected:
  /**
   * The type of a domain, currently accessed through Domain object.
   */
  const Domain& domain_;

  /**
   * A view into a set of buffers for the domain.
   */
  const DomainBuffersView& db_;

  /**
   * Constructor.
   *
   * @param domain A domain of an array
   * @param buffs Coordinate query buffers, one per dimension of the array.
   */
  DomainValueCmpBaseQB(const Domain& domain, const DomainBuffersView& db)
      : domain_(domain)
      , db_(db) {
  }

  [[nodiscard]] DomainBufferDataRef domain_ref_at(size_t k) const {
    return db_.domain_ref_at(domain_, k);
  }
};

/**
 * (Global) Comparsion (Cmp) function class that operates on values that
 * reside in QueryBuffers (QB) for a domain.
 */
class GlobalCmpQB : protected DomainValueCmpBaseQB {
 public:
  /// Default constructor is prohibited.
  GlobalCmpQB() = delete;

  /**
   * Constructor.
   *
   * @param domain The array domain.
   * @param buffs The coordinate query buffers, one per dimension.
   */
  GlobalCmpQB(const Domain& domain, const DomainBuffersView& db)
      : DomainValueCmpBaseQB(domain, db) {
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
    auto left{domain_ref_at(a)};
    auto right{domain_ref_at(b)};
    auto tile_cmp = domain_.tile_order_cmp(left, right);

    if (tile_cmp == -1)
      return true;
    if (tile_cmp == 1)
      return false;
    // else tile_cmp == 0 --> continue

    // Compare cell order
    auto cell_cmp = domain_.cell_order_cmp(left, right);
    return cell_cmp == -1;
  }
};

/**
 * (Hilbert) Comparsion (Cmp) function class that operates on values that
 * reside in QueryBuffers (QB) for a domain.
 */
class HilbertCmpQB : protected DomainValueCmpBaseQB {
  /**
   * The Hilbert values vector.
   */
  const std::vector<uint64_t>& hilbert_values_;

 public:
  /// Default constructor is prohibited.
  HilbertCmpQB() = delete;

  /**
   * Constructor.
   */
  HilbertCmpQB(
      const Domain& domain,
      const DomainBuffersView& domain_buffers,
      const std::vector<uint64_t>& hilbert_values)
      : DomainValueCmpBaseQB(domain, domain_buffers)
      , hilbert_values_(hilbert_values) {
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
    if (hilbert_values_[a] < hilbert_values_[b]) {
      return true;
    } else if (hilbert_values_[a] > hilbert_values_[b]) {
      return false;
    }
    // Assert: The hilbert values are equal

    // Compare cell order
    auto left{domain_ref_at(a)};
    auto right{domain_ref_at(b)};
    auto cell_cmp = domain_.cell_order_cmp(left, right);
    return cell_cmp == -1;
  }
};

}  // namespace tiledb::sm

#endif  // TILEDB_COMPARATORS_H
