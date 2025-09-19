/**
 * @file test/support/rapidcheck/array_templates.h
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
 * This file defines rapidcheck generators for the structures
 * defined in test/support/src/array_templates.h.
 */

#ifndef TILEDB_RAPIDCHECK_ARRAY_H
#define TILEDB_RAPIDCHECK_ARRAY_H

#include <test/support/src/array_templates.h>
#include <test/support/stdx/tuple.h>
#include <test/support/tdb_rapidcheck.h>

namespace rc {

using namespace tiledb::test;
using namespace tiledb::test::templates;

template <DimensionType D>
struct Arbitrary<templates::Domain<D>> {
  static Gen<templates::Domain<D>> arbitrary() {
    // NB: `gen::inRange` is exclusive at the upper end but tiledb domain is
    // inclusive. So we have to use `int64_t` to avoid overflow.
    auto bounds = gen::mapcat(gen::arbitrary<D>(), [](D lb) {
      if constexpr (std::is_same<D, int64_t>::value) {
        return gen::pair(
            gen::just(lb), gen::inRange(lb, std::numeric_limits<D>::max()));
      } else if constexpr (std::is_same<D, uint64_t>::value) {
        return gen::pair(
            gen::just(lb), gen::inRange(lb, std::numeric_limits<D>::max()));
      } else {
        auto ub_limit = int64_t(std::numeric_limits<D>::max()) + 1;
        return gen::pair(
            gen::just(lb), gen::cast<D>(gen::inRange(int64_t(lb), ub_limit)));
      }
    });

    return gen::map(bounds, [](std::pair<D, D> bounds) {
      return templates::Domain<D>(bounds.first, bounds.second);
    });
  }
};

/**
 * @return `a - b` if it does not overflow, `std::nullopt` if it does
 */
template <std::integral T>
std::optional<T> checked_sub(T a, T b) {
  if (!std::is_signed<T>::value) {
    return (b > a ? std::nullopt : std::optional(a - b));
  } else if (b < 0) {
    return (
        std::numeric_limits<T>::max() + b < a ? std::nullopt :
                                                std::optional(a - b));
  } else {
    return (
        std::numeric_limits<T>::min() - b > a ? std::nullopt :
                                                std::optional(a - b));
  }
}

template <DimensionType D>
Gen<D> make_extent(const templates::Domain<D>& domain) {
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

  const auto bound_distance =
      checked_sub(domain.upper_bound, domain.lower_bound);
  if (bound_distance.has_value()) {
    extent_upper_bound =
        (bound_distance.value() < extent_limit ? bound_distance.value() + 1 :
                                                 extent_limit);
  } else {
    extent_upper_bound = extent_limit;
  }

  return gen::inRange(extent_lower_bound, extent_upper_bound + 1);
}

template <tiledb::sm::Datatype D>
struct Arbitrary<templates::Dimension<D>> {
  static Gen<templates::Dimension<D>> arbitrary() {
    using CoordType = templates::Dimension<D>::value_type;
    auto tup = gen::mapcat(
        gen::arbitrary<Domain<CoordType>>(), [](Domain<CoordType> domain) {
          return gen::pair(gen::just(domain), make_extent(domain));
        });

    return gen::map(tup, [](std::pair<Domain<CoordType>, CoordType> tup) {
      return templates::Dimension<D>(tup.first, tup.second);
    });
  }
};

template <DimensionType D>
Gen<D> make_coordinate(const templates::Domain<D>& domain) {
  // `gen::inRange` does an exclusive upper bound,
  // whereas the domain upper bound is inclusive.
  // As a result some contortion is required to deal
  // with numeric_limits.
  if constexpr (std::is_same_v<D, StringDimensionCoordType>) {
    // NB: poor performance with small domains for sure
    return gen::suchThat(
        gen::map(
            gen::string<std::string>(),
            [](std::string s) {
              StringDimensionCoordType v(s.begin(), s.end());
              return v;
            }),
        [domain](const StringDimensionCoordType& s) {
          return domain.lower_bound <= s && s <= domain.upper_bound;
        });
  } else if constexpr (std::is_signed<D>::value) {
    if (int64_t(domain.upper_bound) < std::numeric_limits<int64_t>::max()) {
      return gen::cast<D>(gen::inRange(
          int64_t(domain.lower_bound), int64_t(domain.upper_bound + 1)));
    } else {
      return gen::inRange(domain.lower_bound, domain.upper_bound);
    }
  } else {
    if (uint64_t(domain.upper_bound) < std::numeric_limits<uint64_t>::max()) {
      return gen::cast<D>(gen::inRange(
          uint64_t(domain.lower_bound), uint64_t(domain.upper_bound + 1)));
    } else {
      return gen::inRange(domain.lower_bound, domain.upper_bound);
    }
  }
}

template <DimensionType D>
Gen<templates::Domain<D>> make_range(const templates::Domain<D>& domain) {
  return gen::apply(
      [](D p1, D p2) { return templates::Domain<D>(p1, p2); },
      make_coordinate<D>(domain),
      make_coordinate<D>(domain));
}

template <DimensionType D, typename... Att>
Gen<Fragment1D<D, typename Att::cell_type...>> make_fragment_1d(
    bool allow_duplicates, const Domain<D>& d) {
  auto coord = make_coordinate(d);

  auto cell = gen::tuple(coord, gen::arbitrary<typename Att::cell_type>()...);

  using Cell = std::tuple<D, typename Att::cell_type...>;

  auto uniqueCoords = [](const Cell& cell) { return std::get<0>(cell); };

  auto cells = gen::nonEmpty(
      allow_duplicates ? gen::container<std::vector<Cell>>(cell) :
                         gen::uniqueBy<std::vector<Cell>>(cell, uniqueCoords));

  return gen::map(cells, [](std::vector<Cell> cells) {
    query_buffers<D> coords;
    std::tuple<query_buffers<typename Att::cell_type>...> atts;

    std::apply(
        [&](std::vector<D> tup_d1, auto... tup_atts) {
          if constexpr (std::is_same_v<D, StringDimensionCoordType>) {
            coords = query_buffers<D>(tup_d1);
          } else {
            coords.values_ = tup_d1;
          }
          atts = std::apply(
              [&]<typename... Ts>(std::vector<Ts>... att) {
                return std::make_tuple(query_buffers<Ts>(att)...);
              },
              std::forward_as_tuple(tup_atts...));
        },
        stdx::transpose(cells));

    return Fragment1D<D, typename Att::cell_type...>{
        std::make_tuple(coords), atts};
  });
}

template <DimensionType D1, DimensionType D2, AttributeType... Att>
Gen<Fragment2D<D1, D2, Att...>> make_fragment_2d(
    bool allow_duplicates,
    const Domain<D1>& d1,
    const templates::Domain<D2>& d2) {
  auto coord_d1 = make_coordinate(d1);
  auto coord_d2 = make_coordinate(d2);

  using Cell = std::tuple<D1, D2, Att...>;

  auto cell = gen::tuple(coord_d1, coord_d2, gen::arbitrary<Att>()...);

  auto uniqueCoords = [](const Cell& cell) {
    return std::make_pair(std::get<0>(cell), std::get<1>(cell));
  };

  auto cells = gen::nonEmpty(
      allow_duplicates ? gen::container<std::vector<Cell>>(cell) :
                         gen::uniqueBy<std::vector<Cell>>(cell, uniqueCoords));

  return gen::map(cells, [](std::vector<Cell> cells) {
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

    return Fragment2D<D1, D2, Att...>{
        std::make_tuple(coords_d1, coords_d2), atts};
  });
}

void showValue(const templates::Domain<int>& domain, std::ostream& os);
void showValue(const templates::Domain<int64_t>& domain, std::ostream& os);
void showValue(const templates::Domain<uint64_t>& domain, std::ostream& os);

namespace detail {

template <stdx::is_fundamental T, bool A, bool B>
struct ShowDefault<templates::query_buffers<T>, A, B> {
  static void show(const query_buffers<T>& value, std::ostream& os) {
    ::rc::show<decltype(value.values_)>(value.values_, os);
  }
};

}  // namespace detail

template <typename DimensionTuple, typename AttributeTuple>
void showFragment(
    const templates::Fragment<DimensionTuple, AttributeTuple>& value,
    std::ostream& os) {
  auto showField = [&]<typename T>(const query_buffers<T>& field) {
    os << "\t\t";
    show(field, os);
    os << std::endl;
  };
  os << "{" << std::endl << "\t\"dimensions\": [" << std::endl;
  std::apply(
      [&](const auto&... dimension) { (showField(dimension), ...); },
      value.dimensions());
  os << "\t]" << std::endl;
  os << "\t\"attributes\": [" << std::endl;
  std::apply(
      [&](const auto&... attribute) { (showField(attribute), ...); },
      value.attributes());
  os << "\t]" << std::endl << "}" << std::endl;
}

}  // namespace rc

#endif
