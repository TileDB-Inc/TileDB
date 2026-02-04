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

#include <test/support/rapidcheck/array_schema_templates.h>
#include <test/support/src/array_templates.h>
#include <test/support/stdx/tuple.h>
#include <test/support/tdb_rapidcheck.h>

namespace rc {

using namespace tiledb::test;
using namespace tiledb::test::templates;

template <DimensionType D, typename... Att>
Gen<Fragment1D<D, typename Att::cell_type...>> make_fragment_1d(
    bool allow_duplicates, Gen<D> coord) {
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

template <DimensionType D, typename... Att>
Gen<Fragment1D<D, typename Att::cell_type...>> make_fragment_1d(
    bool allow_duplicates) {
  return make_fragment_1d<D, Att...>(allow_duplicates, gen::arbitrary<D>());
}

template <DimensionType D, typename... Att>
Gen<Fragment1D<D, typename Att::cell_type...>> make_fragment_1d(
    bool allow_duplicates, const Domain<D>& d) {
  auto coord = make_coordinate(d);
  return make_fragment_1d<D, Att...>(allow_duplicates, coord);
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

template <
    DimensionType D1,
    DimensionType D2,
    DimensionType D3,
    AttributeType... Att>
Gen<Fragment3D<D1, D2, D3, Att...>> make_fragment_3d(
    bool allow_duplicates,
    std::optional<Domain<D1>> d1,
    std::optional<Domain<D2>> d2,
    std::optional<Domain<D3>> d3) {
  auto coord_d1 =
      (d1.has_value() ? make_coordinate(d1.value()) : gen::arbitrary<D1>());
  auto coord_d2 =
      (d2.has_value() ? make_coordinate(d2.value()) : gen::arbitrary<D2>());
  auto coord_d3 =
      (d3.has_value() ? make_coordinate(d3.value()) : gen::arbitrary<D3>());

  using Cell = std::tuple<D1, D2, D3, Att...>;

  auto cell =
      gen::tuple(coord_d1, coord_d2, coord_d3, gen::arbitrary<Att>()...);

  auto uniqueCoords = [](const Cell& cell) {
    return std::make_tuple(
        std::get<0>(cell), std::get<1>(cell), std::get<2>(cell));
  };

  auto cells = gen::nonEmpty(
      allow_duplicates ? gen::container<std::vector<Cell>>(cell) :
                         gen::uniqueBy<std::vector<Cell>>(cell, uniqueCoords));

  return gen::map(cells, [](std::vector<Cell> cells) {
    std::vector<D1> coords_d1;
    std::vector<D2> coords_d2;
    std::vector<D3> coords_d3;
    std::tuple<std::vector<Att>...> atts;

    std::apply(
        [&](std::vector<D1> tup_d1,
            std::vector<D2> tup_d2,
            std::vector<D3> tup_d3,
            auto... tup_atts) {
          coords_d1 = tup_d1;
          coords_d2 = tup_d2;
          coords_d3 = tup_d3;
          atts = std::make_tuple(tup_atts...);
        },
        stdx::transpose(cells));

    return Fragment3D<D1, D2, D3, Att...>{
        std::make_tuple(coords_d1, coords_d2, coords_d3), atts};
  });
}

namespace detail {

/**
 * Specialization of `rc::detail::ShowDefault` for `query_buffers` of
 * fundamental cell type.
 *
 * Parameters `A` and `B` are SFINAE which in principle allow less verbose
 * alternative paths to providing this custom functionality.
 */
template <stdx::is_fundamental T, bool A, bool B>
struct ShowDefault<templates::query_buffers<T>, A, B> {
  static void show(const query_buffers<T>& value, std::ostream& os) {
    ::rc::show<decltype(value.values_)>(value.values_, os);
  }
};

/**
 * Specialization of `rc::detail::ShowDefault` for
 * `query_buffers<std::vector<char>>`.
 *
 * Parameters `A` and `B` are SFINAE which in principle allow less verbose
 * alternative paths to providing this custom functionality.
 */
template <bool A, bool B>
struct ShowDefault<templates::query_buffers<std::vector<char>>, A, B> {
  static void show(
      const query_buffers<std::vector<char>>& value, std::ostream& os) {
    std::vector<std::string> values;
    for (uint64_t c = 0; c < value.num_cells(); c++) {
      values.push_back(std::string(value[c].begin(), value[c].end()));
    }
    ::rc::show<decltype(values)>(values, os);
  }
};

}  // namespace detail

/**
 * Generic logic to for showing a `templates::FragmentType`.
 */
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
