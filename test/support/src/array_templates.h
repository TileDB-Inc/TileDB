/**
 * @file test/support/src/array_templates.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022-2024 TileDB, Inc.
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
 * This file provides templates for generic programming with respect
 * to array schema, data types, etc.
 */

#ifndef TILEDB_ARRAY_TEMPLATES_H
#define TILEDB_ARRAY_TEMPLATES_H

#include "tiledb/type/datatype_traits.h"
#include "tiledb/type/range/range.h"

#include <algorithm>
#include <concepts>
#include <type_traits>

namespace tiledb::sm {
class Dimension;
}

namespace tiledb::test::templates {

/**
 * Adapts a `std::tuple` whose fields are all `GlobalCellCmp`
 * to itself be `GlobalCellCmp`.
 */
template <typename StdTuple>
struct global_cell_cmp_std_tuple {
  global_cell_cmp_std_tuple(const StdTuple& tup)
      : tup_(tup) {
  }

  tiledb::common::UntypedDatumView dimension_datum(
      const tiledb::sm::Dimension&, unsigned dim_idx) const {
    return std::apply(
        [&](const auto&... field) {
          size_t sizes[] = {sizeof(std::decay_t<decltype(field)>)...};
          const void* const ptrs[] = {
              static_cast<const void*>(std::addressof(field))...};
          return UntypedDatumView(ptrs[dim_idx], sizes[dim_idx]);
        },
        tup_);
  }

  const void* coord(unsigned dim) const {
    return std::apply(
        [&](const auto&... field) {
          const void* const ptrs[] = {
              static_cast<const void*>(std::addressof(field))...};
          return ptrs[dim];
        },
        tup_);
  }

  const StdTuple& tup_;
};

/**
 * Constrains types which can be used as the physical type of a dimension.
 */
template <typename D>
concept DimensionType = requires(const D& coord) {
  typename std::is_signed<D>;
  { coord < coord } -> std::same_as<bool>;
  { D(int64_t(coord)) } -> std::same_as<D>;
};

/**
 * Constrains types which can be used as the physical type of an attribute.
 *
 * Right now this doesn't constrain anything, it is just a marker for
 * readability, and someday we might want it do require something.
 */
template <typename T>
concept AttributeType = true;

/**
 * Constrains types which can be used as columnar data fragment input.
 *
 * Methods `dimensions` and `attributes` return tuples whose fields are each
 * `std::vector<DimensionType>` and `std::vector<AttributeType>`
 * respectively.
 */
template <typename T>
concept FragmentType = requires(const T& fragment) {
  { fragment.size() } -> std::convertible_to<uint64_t>;

  // not sure how to specify "returns any tuple whose elements decay to
  // std::vector"
  fragment.dimensions();
  fragment.attributes();
} and requires(T& fragment) {
  // non-const versions
  fragment.dimensions();
  fragment.attributes();
};

/**
 * A generic, statically-typed range which is inclusive on both ends.
 */
template <DimensionType D>
struct Domain {
  D lower_bound;
  D upper_bound;

  Domain() {
  }

  Domain(D d1, D d2)
      : lower_bound(std::min(d1, d2))
      , upper_bound(std::max(d1, d2)) {
  }

  bool contains(D point) const {
    return lower_bound <= point && point <= upper_bound;
  }

  bool intersects(const Domain<D>& other) const {
    return (other.lower_bound <= lower_bound &&
            lower_bound <= other.upper_bound) ||
           (other.lower_bound <= upper_bound &&
            upper_bound <= other.upper_bound) ||
           (lower_bound <= other.lower_bound &&
            other.lower_bound <= upper_bound) ||
           (lower_bound <= other.upper_bound &&
            other.upper_bound <= upper_bound);
  }

  tiledb::type::Range range() const {
    return tiledb::type::Range(lower_bound, upper_bound);
  }
};

/**
 * A description of a dimension as it pertains to its datatype.
 */
template <tiledb::sm::Datatype DATATYPE>
struct Dimension {
  using value_type = tiledb::type::datatype_traits<DATATYPE>::value_type;

  Domain<value_type> domain;
  value_type extent;
};

/**
 * Data for a one-dimensional array
 */
template <DimensionType D, AttributeType... Att>
struct Fragment1D {
  std::vector<D> dim_;
  std::tuple<std::vector<Att>...> atts_;

  uint64_t size() const {
    return dim_.size();
  }

  std::tuple<const std::vector<D>&> dimensions() const {
    return std::tuple<const std::vector<D>&>(dim_);
  }

  std::tuple<const std::vector<Att>&...> attributes() const {
    return std::apply(
        [](const std::vector<Att>&... attribute) {
          return std::tuple<const std::vector<Att>&...>(attribute...);
        },
        atts_);
  }

  std::tuple<std::vector<D>&> dimensions() {
    return std::tuple<std::vector<D>&>(dim_);
  }

  std::tuple<std::vector<Att>&...> attributes() {
    return std::apply(
        [](std::vector<Att>&... attribute) {
          return std::tuple<std::vector<Att>&...>(attribute...);
        },
        atts_);
  }
};

/**
 * Data for a two-dimensional array
 */
template <DimensionType D1, DimensionType D2, AttributeType... Att>
struct Fragment2D {
  std::vector<D1> d1_;
  std::vector<D2> d2_;
  std::tuple<std::vector<Att>...> atts_;

  uint64_t size() const {
    return d1_.size();
  }

  std::tuple<const std::vector<D1>&, const std::vector<D2>&> dimensions()
      const {
    return std::tuple<const std::vector<D1>&, const std::vector<D2>&>(d1_, d2_);
  }

  std::tuple<std::vector<D1>&, std::vector<D2>&> dimensions() {
    return std::tuple<std::vector<D1>&, std::vector<D2>&>(d1_, d2_);
  }

  std::tuple<const std::vector<Att>&...> attributes() const {
    return std::apply(
        [](const std::vector<Att>&... attribute) {
          return std::tuple<const std::vector<Att>&...>(attribute...);
        },
        atts_);
  }

  std::tuple<std::vector<Att>&...> attributes() {
    return std::apply(
        [](std::vector<Att>&... attribute) {
          return std::tuple<std::vector<Att>&...>(attribute...);
        },
        atts_);
  }
};

}  // namespace tiledb::test::templates

#endif
