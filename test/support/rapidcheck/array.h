/**
 * @file test/support/rapidcheck/array.h
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
 * This file defines structures and rapidcheck generators which
 * can be commonly useful for writing rapidcheck properties
 * about tiledb array.
 */

#ifndef TILEDB_RAPIDCHECK_ARRAY_H
#define TILEDB_RAPIDCHECK_ARRAY_H

#include <test/support/tdb_rapidcheck.h>

#include <algorithm>
#include <concepts>
#include <type_traits>

namespace tiledb::test::tdbrc {

/**
 * Describes types which can be used for rapidcheck-generated dimensions.
 */
template <typename D>
concept DimensionType = requires(const D& coord) {
  typename ::rc::Arbitrary<D>;
  typename std::is_signed<D>;
  { coord < coord } -> std::same_as<bool>;
  { D(int64_t(coord)) } -> std::same_as<D>;
};

/**
 * Describes types which can be used for rapidcheck-generated attributes.
 */
template <typename T>
concept AttributeType =
    requires(const T& coord) { typename ::rc::Arbitrary<T>; };

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
};

/**
 * A description of a dimension as it pertains to its datatype.
 */
template <DimensionType D>
struct Dimension {
  Domain<D> domain;
  D extent;
};

/**
 * Data for a one-dimensional array
 */
template <DimensionType D, AttributeType... Att>
struct Fragment1D {
  std::vector<D> dim;
  std::tuple<std::vector<Att>...> atts;
};

/**
 * Data for a two-dimensional array
 */
template <DimensionType D1, DimensionType D2, AttributeType... Att>
struct Fragment2D {
  std::vector<D1> d1;
  std::vector<D2> d2;
  std::tuple<std::vector<Att>...> atts;
};

}  // namespace tiledb::test::tdbrc

namespace stdx {

/**
 * @return the transposition of row-oriented tuples into column-oriented tuples
 */
template <typename... Ts>
std::tuple<std::vector<Ts>...> transpose(std::vector<std::tuple<Ts...>> rows) {
  std::tuple<std::vector<Ts>...> cols;

  std::apply(
      [&](std::vector<Ts>&... col) { (col.reserve(rows.size()), ...); }, cols);

  for (size_t i = 0; i < rows.size(); i++) {
    std::apply(
        [&](std::vector<Ts>&... col) {
          std::apply([&](Ts... cell) { (col.push_back(cell), ...); }, rows[i]);
        },
        cols);
  }

  return cols;
}

}  // namespace stdx

namespace rc {

using namespace tiledb::test::tdbrc;

template <DimensionType D>
struct Arbitrary<Domain<D>> {
  static Gen<Domain<D>> arbitrary() {
    // NB: `gen::inRange` is exclusive at the upper end but tiledb domain is
    // inclusive. So we have to use `int64_t` to avoid overflow.
    auto bounds = gen::mapcat(gen::arbitrary<D>(), [](D lb) {
      if (std::is_same<D, int64_t>::value) {
        return gen::pair(
            gen::just(lb), gen::inRange(lb, std::numeric_limits<D>::max()));
      } else if (std::is_same<D, uint64_t>::value) {
        return gen::pair(
            gen::just(lb), gen::inRange(lb, std::numeric_limits<D>::max()));
      } else {
        auto ub_limit = int64_t(std::numeric_limits<D>::max()) + 1;
        return gen::pair(
            gen::just(lb), gen::cast<D>(gen::inRange(int64_t(lb), ub_limit)));
      }
    });

    return gen::map(bounds, [](std::pair<D, D> bounds) {
      return Domain<D>(bounds.first, bounds.second);
    });
  }
};

template <DimensionType D>
Gen<D> make_extent(const Domain<D>& domain) {
  // upper bound on all possible extents to avoid unreasonably
  // huge tile sizes
  static constexpr D extent_limit = static_cast<D>(
      std::is_signed<D>::value ?
          std::min(
              static_cast<int64_t>(std::numeric_limits<D>::max()),
              static_cast<int64_t>(1024 * 16)) :
          std::min(
              static_cast<uint64_t>(std::numeric_limits<D>::max()),
              static_cast<uint64_t>(1024 * 16)));

  // NB: `gen::inRange` is exclusive at the upper end but tiledb domain is
  // inclusive. So we have to be careful to avoid overflow.

  D extent_lower_bound = 1;
  D extent_upper_bound;
  if (domain.lower_bound + domain.upper_bound + 1 < domain.lower_bound) {
    // overflow
    extent_upper_bound = extent_limit;
  } else {
    extent_upper_bound =
        std::min(extent_limit, domain.lower_bound + domain.upper_bound + 1);
  }

  return gen::inRange(extent_lower_bound, extent_upper_bound + 1);
}

template <DimensionType D>
struct Arbitrary<Dimension<D>> {
  static Gen<Dimension<D>> arbitrary() {
    auto tup = gen::mapcat(gen::arbitrary<Domain<D>>(), [](Domain<D> domain) {
      return gen::pair(gen::just(domain), make_extent(domain));
    });

    return gen::map(tup, [](std::pair<Domain<D>, D> tup) {
      return Dimension{.domain = tup.first, .extent = tup.second};
    });
  }
};

template <DimensionType D>
Gen<D> make_coordinate(const Domain<D>& domain) {
  // `gen::inRange` does an exclusive upper bound,
  // whereas the domain upper bound is inclusive.
  // As a result some contortion is required to deal
  // with numeric_limits.
  if (std::is_signed<D>::value) {
    if (domain.upper_bound < std::numeric_limits<int64_t>::max()) {
      return gen::cast<D>(gen::inRange(
          int64_t(domain.lower_bound), int64_t(domain.upper_bound + 1)));
    } else {
      return gen::inRange(domain.lower_bound, domain.upper_bound);
    }
  } else {
    if (domain.upper_bound < std::numeric_limits<uint64_t>::max()) {
      return gen::cast<D>(gen::inRange(
          uint64_t(domain.lower_bound), uint64_t(domain.upper_bound + 1)));
    } else {
      return gen::inRange(domain.lower_bound, domain.upper_bound);
    }
  }
}

template <DimensionType D>
Gen<Domain<D>> make_range(const Domain<D>& domain) {
  return gen::apply(
      [](D p1, D p2) { return Domain<D>(p1, p2); },
      make_coordinate<D>(domain),
      make_coordinate<D>(domain));
}

template <DimensionType D, AttributeType... Att>
Gen<Fragment1D<D, Att...>> make_fragment_1d(const Domain<D>& d) {
  auto coord = make_coordinate(d);

  auto cell = gen::tuple(coord, gen::arbitrary<Att>()...);

  using Cell = std::tuple<D, Att...>;

  auto cells = gen::nonEmpty(gen::container<std::vector<Cell>>(cell));

  return gen::map(cells, [](std::vector<Cell> cells) {
    std::vector<D> coords;
    std::tuple<std::vector<Att>...> atts;

    std::apply(
        [&](std::vector<D> tup_d1, auto... tup_atts) {
          coords = tup_d1;
          atts = std::make_tuple(tup_atts...);
        },
        stdx::transpose(cells));

    return Fragment1D<D, Att...>{.dim = coords, .atts = atts};
  });
}

template <DimensionType D1, DimensionType D2, AttributeType... Att>
Gen<Fragment2D<D1, D2, Att...>> make_fragment_2d(
    const Domain<D1>& d1, const Domain<D2>& d2) {
  auto coord_d1 = make_coordinate(d1);
  auto coord_d2 = make_coordinate(d2);

  using Cell = std::tuple<D1, D2, Att...>;

  auto cell = gen::tuple(coord_d1, coord_d2, gen::arbitrary<Att>()...);

  auto cells = gen::nonEmpty(gen::container<std::vector<Cell>>(cell));

  return gen::map(cells, [](std::vector<Cell> cells) {
    /*
  std::vector<D1> coords_d1;
  std::vector<D2> coords_d2;
  std::tuple<std::vector<Att>...> atts;

  coords_d1.reserve(cells.size());
  coords_d2.reserve(cells.size());
  atts.reserve(cells.size())...;

  for (uint64_t i = 0; i < cells.size(); i++) {
    D1 coord_d1;
    D2 coord_d2;
    Att... att;
    std::tie(coord_d1, coord_d2, att...) = cells[i];
    coords_d1.push_back(coord_d1);
    coords_d2.push_back(coord_d2);
    atts.push_back(att)...;
  }
  */

    std::vector<D1> coords_d1;
    std::vector<D2> coords_d2;
    std::tuple<std::vector<Att>...> atts;

    std::apply(
        [&](std::vector<D1> tup_d1, std::vector<D2> tup_d2, auto... tup_atts) {
          coords_d1 = tup_d1;
          coords_d2 = tup_d2;
          atts = std::make_tuple(tup_atts...);
        },
        stdx::transpose(cells));

    //    std::tie(coords_d1, coords_d2, atts...) = stdx::transpose(cells);

    return Fragment2D<D1, D2, Att...>{
        .d1 = coords_d1, .d2 = coords_d2, .atts = atts};
  });
}

}  // namespace rc

#endif
