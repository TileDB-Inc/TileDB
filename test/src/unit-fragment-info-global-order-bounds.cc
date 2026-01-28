/**
 * @file unit-sparse-global-order-reader.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2025 TileDB, Inc.
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
 * Tests populating fragment metadata with the global order lower/upper bound
 * coordinates per tile.
 */

#include "tiledb/api/c_api/array_schema/array_schema_api_internal.h"
#include "tiledb/sm/misc/comparators.h"

#include <test/support/rapidcheck/array_templates.h>
#include <test/support/src/array_helpers.h>
#include <test/support/src/array_templates.h>
#include <test/support/src/vfs_helpers.h>

using namespace tiledb;
using namespace tiledb::test;

using Fragment1DFixed = templates::Fragment1D<uint64_t>;
using Fragment2DFixed = templates::Fragment2D<int32_t, int32_t>;

using Fragment1DVar =
    templates::Fragment1D<templates::StringDimensionCoordType>;

using FragmentVcf2025 = templates::Fragment3D<
    templates::StringDimensionCoordType,
    uint32_t,
    templates::StringDimensionCoordType>;

void showValue(const Fragment1DFixed& value, std::ostream& os) {
  rc::showFragment(value, os);
}

void showValue(const Fragment1DVar& value, std::ostream& os) {
  rc::showFragment(value, os);
}

void showValue(const Fragment2DFixed& value, std::ostream& os) {
  rc::showFragment(value, os);
}

namespace rc::detail {

template <bool A, bool B>
struct ShowDefault<Fragment1DFixed, A, B> {
  static void show(const Fragment1DFixed& value, std::ostream& os) {
    rc::showFragment(value, os);
  }
};

template <bool A, bool B>
struct ShowDefault<Fragment1DVar, A, B> {
  static void show(const Fragment1DVar& value, std::ostream& os) {
    rc::showFragment(value, os);
  }
};

}  // namespace rc::detail

/**
 * @return another fragment containing the contents of the argument sorted in
 * global order based on `array.schema()`.
 */
template <templates::FragmentType F>
static F make_global_order(
    const Array& array, const F& fragment, sm::Layout layout) {
  if (layout == sm::Layout::GLOBAL_ORDER) {
    return fragment;
  }

  std::vector<uint64_t> idxs(fragment.size(), 0);
  std::iota(idxs.begin(), idxs.end(), 0);

  // sort in global order
  auto array_schema = array.schema().ptr()->array_schema();
  sm::GlobalCellCmp globalcmp(array_schema->domain());

  auto icmp = [&](uint64_t ia, uint64_t ib) -> bool {
    return std::apply(
        [&globalcmp, ia, ib]<typename... Ts>(
            const templates::query_buffers<Ts>&... dims) {
          const auto l = std::make_tuple(dims[ia]...);
          const auto r = std::make_tuple(dims[ib]...);
          return globalcmp(
              templates::global_cell_cmp_std_tuple<decltype(l)>(l),
              templates::global_cell_cmp_std_tuple<decltype(r)>(r));
        },
        fragment.dimensions());
  };
  std::sort(idxs.begin(), idxs.end(), icmp);

  F sorted = fragment;
  sorted.dimensions() =
      stdx::select(stdx::reference_tuple(sorted.dimensions()), std::span(idxs));
  if constexpr (!std::is_same_v<decltype(sorted.attributes()), std::tuple<>>) {
    sorted.attributes() = stdx::select(
        stdx::reference_tuple(sorted.attributes()), std::span(idxs));
  }

  return sorted;
}

template <templates::FragmentType F>
using DimensionTuple =
    stdx::decay_tuple<decltype(std::declval<F>().dimensions())>;

/**
 * @return something equivalent to `value` which owns all of its own memory.
 */
template <stdx::is_fundamental T>
T to_owned(T value) {
  return value;
}

template <stdx::is_fundamental T>
std::vector<T> to_owned(std::span<const T> value) {
  return std::vector<T>(value.begin(), value.end());
}

/**
 * @return a tuple containing the `idx`th value of each field of the argument
 * tuple
 */
template <typename T>
auto tuple_index(const T& tuple, uint64_t idx) {
  return std::apply(
      [&](const auto&... field) {
        return std::make_tuple(to_owned(field[idx])...);
      },
      tuple);
}

/**
 * A `std::tuple` whose fields are the coordinate types of each dimension of
 * `F`.
 */
template <templates::FragmentType F>
using CoordsTuple = decltype(tuple_index(
    std::declval<F>().dimensions(), std::declval<uint64_t>()));

template <templates::FragmentType F>
using Bounds = std::pair<CoordsTuple<F>, CoordsTuple<F>>;

template <templates::FragmentType F>
using FragmentBounds = std::vector<Bounds<F>>;

template <templates::FragmentType F>
using ArrayBounds = std::vector<FragmentBounds<F>>;

/**
 * @return the lower and upper bounds of tile `(f, t)` in the fragment info
 */
template <templates::FragmentType F>
static Bounds<F> global_order_bounds(
    const FragmentInfo& finfo, uint64_t fragment, uint64_t tile) {
  DimensionTuple<F> lb, ub;

  auto bound_vec_to_tuple = [](DimensionTuple<F>& out,
                               std::vector<std::vector<uint8_t>> bounds) {
    auto handle_field = [&]<typename T>(
                            templates::query_buffers<T>& qb, uint64_t d) {
      static_assert(
          stdx::is_fundamental<T> ||
          std::is_same_v<T, templates::StringDimensionCoordType>);
      if constexpr (stdx::is_fundamental<T>) {
        qb.resize(1);
        qb[0] = *reinterpret_cast<T*>(bounds[d].data());
      } else {
        static_assert(std::is_same_v<T, std::vector<char>>);
        qb.values_.resize(bounds[d].size());
        memcpy(qb.values_.data(), bounds[d].data(), bounds[d].size());
        qb.offsets_ = {0};
      }
    };
    uint64_t d = 0;
    std::apply(
        [&]<typename... Ts>(templates::query_buffers<Ts>&... qb) {
          (handle_field(qb, d++), ...);
        },
        out);
  };

  auto lbvec = finfo.global_order_lower_bound(fragment, tile);
  bound_vec_to_tuple(lb, lbvec);

  auto ubvec = finfo.global_order_upper_bound(fragment, tile);
  bound_vec_to_tuple(ub, ubvec);

  return std::make_pair(tuple_index(lb, 0), tuple_index(ub, 0));
}

/**
 * @return the global order bounds of all tiles in all fragments; [f][t]
 * contains the bounds of the t'th tile of fragment f
 */
template <templates::FragmentType F>
std::vector<std::vector<Bounds<F>>> get_all_bounds(
    const Context& ctx, const std::string& array_uri) {
  FragmentInfo finfo(ctx, array_uri);
  finfo.load();

  std::vector<std::vector<Bounds<F>>> bounds;
  bounds.reserve(finfo.fragment_num());

  for (uint64_t f = 0; f < finfo.fragment_num(); f++) {
    const uint64_t num_tiles = finfo.mbr_num(f);

    std::vector<Bounds<F>> this_fragment_bounds;
    this_fragment_bounds.reserve(num_tiles);
    for (uint64_t t = 0; t < num_tiles; t++) {
      this_fragment_bounds.push_back(global_order_bounds<F>(finfo, f, t));
    }

    bounds.emplace_back(std::move(this_fragment_bounds));
  }

  return bounds;
}

/**
 * Asserts that when a set of fragments are written, the fragment metadata
 * accurately reflects the expected global order bounds of the input.
 *
 * "Accurately reflects" means that:
 * 1) there is data for each tile in the fragment;
 * 2) the lower bound for tile `(f, t)` is indeed the first coordinate in
 *    global order of the `t`th tile of the `f`th fragment
 * 3) the upper bound for tile `(f, t)` is indeed the last coordinate in
 *    global order of the `t`th tile of the `f`th fragment
 *
 * @return the global order bounds for each tile per fragment
 */
template <typename Asserter, templates::FragmentType F>
std::vector<std::vector<Bounds<F>>> assert_written_bounds(
    const Context& ctx,
    const std::string& array_uri,
    const std::vector<F>& fragments,
    sm::Layout layout = sm::Layout::GLOBAL_ORDER) {
  // write each fragment
  {
    Array forwrite(ctx, array_uri, TILEDB_WRITE);
    for (const auto& fragment : fragments) {
      templates::query::write_fragment<Asserter, F>(
          fragment, forwrite, static_cast<tiledb_layout_t>(layout));
    }
  }

  // check bounds
  std::vector<std::vector<Bounds<F>>> bounds =
      get_all_bounds<F>(ctx, array_uri);

  Array forread(ctx, array_uri, TILEDB_READ);
  const uint64_t tile_stride = forread.schema().capacity();

  ASSERTER(bounds.size() == fragments.size());
  for (size_t f = 0; f < fragments.size(); f++) {
    const auto fragment = make_global_order(forread, fragments[f], layout);

    const uint64_t num_tiles = bounds[f].size();
    ASSERTER(num_tiles == (fragment.size() + tile_stride - 1) / tile_stride);

    for (size_t t = 0; t < num_tiles; t++) {
      const uint64_t lbi = t * tile_stride;
      const uint64_t ubi =
          std::min<uint64_t>((t + 1) * tile_stride, fragment.size()) - 1;

      const auto lbexpect = tuple_index(fragment.dimensions(), lbi);
      const auto ubexpect = tuple_index(fragment.dimensions(), ubi);

      const auto [lbactual, ubactual] = bounds[f][t];
      ASSERTER(lbexpect == lbactual);
      ASSERTER(ubexpect == ubactual);
    }
  }

  return bounds;
}

TEST_CASE(
    "Fragment metadata global order bounds: 1D fixed",
    "[fragment_info][global-order]") {
  VFSTestSetup vfs_test_setup;
  const auto array_uri = vfs_test_setup.array_uri(
      "fragment_metadata_global_order_bounds_1d_fixed");

  const templates::Dimension<Datatype::UINT64> dimension(
      templates::Domain<uint64_t>(0, 1024 * 8), 16);

  const bool allow_dups = GENERATE(true, false);

  templates::ddl::create_array<Datatype::UINT64>(
      array_uri,
      vfs_test_setup.ctx(),
      std::tuple<const templates::Dimension<Datatype::UINT64>&>{dimension},
      std::vector<std::tuple<Datatype, uint32_t, bool>>{},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      8,
      allow_dups);

  DeleteArrayGuard delarray(
      vfs_test_setup.ctx().ptr().get(), array_uri.c_str());

  using Fragment = templates::Fragment1D<uint64_t>;

  auto make_expect =
      [](std::initializer_list<std::pair<uint64_t, uint64_t>> tile_bounds) {
        std::vector<std::pair<std::tuple<uint64_t>, std::tuple<uint64_t>>>
            out_bounds;
        for (const auto& tile : tile_bounds) {
          out_bounds.push_back(std::make_pair(
              std::make_tuple(tile.first), std::make_tuple(tile.second)));
        }
        return out_bounds;
      };

  SECTION("Minimum write") {
    Fragment f;
    f.resize(1);
    f.dimension()[0] = 1;

    std::vector<std::vector<Bounds<Fragment>>> fragment_bounds;
    SECTION("Global Order") {
      fragment_bounds =
          assert_written_bounds<tiledb::test::AsserterCatch, Fragment1DFixed>(
              vfs_test_setup.ctx(), array_uri, std::vector<Fragment1DFixed>{f});
    }

    SECTION("Unordered") {
      fragment_bounds =
          assert_written_bounds<tiledb::test::AsserterCatch, Fragment1DFixed>(
              vfs_test_setup.ctx(),
              array_uri,
              std::vector<Fragment1DFixed>{f},
              sm::Layout::UNORDERED);
    }
    REQUIRE(fragment_bounds.size() == 1);
    CHECK(
        fragment_bounds[0] == std::vector<Bounds<Fragment>>{std::make_pair(
                                  std::make_tuple(1), std::make_tuple(1))});
  }

  SECTION("Ascending fragment") {
    Fragment f;
    f.resize(64);

    const auto expect = make_expect({
        {1, 8},
        {9, 16},
        {17, 24},
        {25, 32},
        {33, 40},
        {41, 48},
        {49, 56},
        {57, 64},
    });

    SECTION("Global Order") {
      std::iota(f.dimension().begin(), f.dimension().end(), 1);
      const auto fragment_bounds =
          assert_written_bounds<tiledb::test::AsserterCatch, Fragment1DFixed>(
              vfs_test_setup.ctx(), array_uri, std::vector<Fragment1DFixed>{f});
      REQUIRE(fragment_bounds.size() == 1);
      CHECK(fragment_bounds[0] == expect);
    }

    SECTION("Unordered") {
      for (uint64_t i = 0; i < f.size(); i++) {
        f.dimension()[i] = f.size() - i;
      }

      const auto fragment_bounds =
          assert_written_bounds<tiledb::test::AsserterCatch, Fragment1DFixed>(
              vfs_test_setup.ctx(),
              array_uri,
              std::vector<Fragment1DFixed>{f},
              sm::Layout::UNORDERED);
      REQUIRE(fragment_bounds.size() == 1);
      CHECK(fragment_bounds[0] == expect);
    }
  }

  if (allow_dups) {
    SECTION("Duplicates") {
      const auto expect = make_expect({{0, 0}, {1, 1}});

      SECTION("Global Order") {
        Fragment f;
        f.dimension() = {0, 0, 0, 0, 0, 0, 0, 0, 1};

        const auto fragment_bounds =
            assert_written_bounds<tiledb::test::AsserterCatch, Fragment1DFixed>(
                vfs_test_setup.ctx(),
                array_uri,
                std::vector<Fragment1DFixed>{f},
                sm::Layout::GLOBAL_ORDER);
        REQUIRE(fragment_bounds.size() == 1);
        CHECK(fragment_bounds[0] == expect);
      }

      SECTION("Unordered") {
        Fragment f;
        f.dimension() = {0, 0, 0, 1, 0, 0, 0, 0, 0};

        const auto fragment_bounds =
            assert_written_bounds<tiledb::test::AsserterCatch, Fragment1DFixed>(
                vfs_test_setup.ctx(),
                array_uri,
                std::vector<Fragment1DFixed>{f},
                sm::Layout::UNORDERED);
        REQUIRE(fragment_bounds.size() == 1);
        CHECK(fragment_bounds[0] == expect);
      }
    }
  }
}

TEST_CASE(
    "Fragment metadata global order bounds: 1D fixed rapidcheck",
    "[fragment_info][global-order][rapidcheck]") {
  VFSTestSetup vfs_test_setup;
  const auto array_uri = vfs_test_setup.array_uri(
      "fragment_metadata_global_order_bounds_1d_fixed_rapidcheck");

  static constexpr uint64_t LB = 0;
  static constexpr uint64_t UB = 1024 * 8;
  const templates::Domain<uint64_t> domain(LB, UB);
  const templates::Dimension<Datatype::UINT64> dimension(domain, 16);

  Context ctx = vfs_test_setup.ctx();

  auto temp_array = [&](bool allow_dups) {
    templates::ddl::create_array<Datatype::UINT64>(
        array_uri,
        ctx,
        std::tuple<const templates::Dimension<Datatype::UINT64>&>{dimension},
        std::vector<std::tuple<Datatype, uint32_t, bool>>{},
        TILEDB_ROW_MAJOR,
        TILEDB_ROW_MAJOR,
        8,
        allow_dups);

    return DeleteArrayGuard(ctx.ptr().get(), array_uri.c_str());
  };

  rc::prop("global order", [&](bool allow_dups) {
    auto fragments = *rc::gen::container<std::vector<Fragment1DFixed>>(
        rc::make_fragment_1d<uint64_t>(allow_dups, domain));
    auto arrayguard = temp_array(allow_dups);
    Array forread(ctx, array_uri, TILEDB_READ);
    std::vector<Fragment1DFixed> global_order_fragments;
    for (const auto& fragment : fragments) {
      global_order_fragments.push_back(make_global_order<Fragment1DFixed>(
          forread, fragment, sm::Layout::UNORDERED));
    }

    assert_written_bounds<tiledb::test::AsserterRapidcheck, Fragment1DFixed>(
        vfs_test_setup.ctx(),
        array_uri,
        global_order_fragments,
        sm::Layout::GLOBAL_ORDER);
  });

  rc::prop("unordered", [&]() {
    const bool allow_dups = false;  // FIXME: not working correctly
    auto fragments = *rc::gen::container<std::vector<Fragment1DFixed>>(
        rc::make_fragment_1d<uint64_t>(allow_dups, domain));
    auto arrayguard = temp_array(allow_dups);
    Array forread(ctx, array_uri, TILEDB_READ);

    assert_written_bounds<tiledb::test::AsserterRapidcheck, Fragment1DFixed>(
        vfs_test_setup.ctx(), array_uri, fragments, sm::Layout::UNORDERED);
  });
}

TEST_CASE(
    "Fragment metadata global order bounds: 2D fixed",
    "[fragment_info][global-order]") {
  VFSTestSetup vfs_test_setup;
  const auto array_uri = vfs_test_setup.array_uri(
      "fragment_metadata_global_order_bounds_2d_fixed");

  const bool allow_dups = false;

  const templates::Dimension<Datatype::INT32> d1(
      templates::Domain<int32_t>(-256, 256), 4);
  const templates::Dimension<Datatype::INT32> d2(
      templates::Domain<int32_t>(-256, 256), 4);

  Context ctx = vfs_test_setup.ctx();

  templates::ddl::create_array<Datatype::INT32, Datatype::INT32>(
      array_uri,
      ctx,
      std::tie(d1, d2),
      std::vector<std::tuple<Datatype, uint32_t, bool>>{},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      8,
      allow_dups);

  DeleteArrayGuard delarray(ctx.ptr().get(), array_uri.c_str());

  using Fragment = templates::Fragment2D<int32_t, int32_t>;
  using TileBounds =
      std::pair<std::tuple<int32_t, int32_t>, std::tuple<int32_t, int32_t>>;

  Fragment minimum;
  {
    minimum.resize(1);
    minimum.d1()[0] = 0;
    minimum.d2()[0] = 0;
  }

  constexpr size_t row_num_cells = 64, col_num_cells = 64,
                   square_num_cells = 64;

  Fragment row, col, square, square_offset;

  row.resize(row_num_cells);
  col.resize(col_num_cells);
  square.resize(square_num_cells);
  square_offset.resize(square_num_cells);

  const sm::Layout layout =
      GENERATE(sm::Layout::UNORDERED, sm::Layout::GLOBAL_ORDER);

  for (uint64_t i = 0; i < row_num_cells; i++) {
    row.d1()[i] = 0;
    row.d2()[i] = i;
  }

  for (uint64_t i = 0; i < col_num_cells; i++) {
    col.d1()[i] = i;
    col.d2()[i] = 0;
  }

  const uint64_t square_row_length = std::sqrt(square_num_cells);
  for (uint64_t i = 0; i < square_num_cells; i++) {
    square.d1()[i] = i / square_row_length;
    square.d2()[i] = i % square_row_length;
  }
  for (uint64_t i = 0; i < square_num_cells; i++) {
    square_offset.d1()[i] = 2 + (i / square_row_length);
    square_offset.d2()[i] = 2 + (i % square_row_length);
  }

  if (layout == sm::Layout::GLOBAL_ORDER) {
    Array forread(ctx, array_uri, TILEDB_READ);

    // row, col are in global order already
    square = make_global_order(forread, square, tiledb::sm::Layout::UNORDERED);
    REQUIRE(
        square.d1() == std::vector<int32_t>{
                           0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3,
                           0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3,
                           4, 4, 4, 4, 5, 5, 5, 5, 6, 6, 6, 6, 7, 7, 7, 7,
                           4, 4, 4, 4, 5, 5, 5, 5, 6, 6, 6, 6, 7, 7, 7, 7,
                       });
    REQUIRE(
        square.d2() == std::vector<int32_t>{
                           0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3,
                           4, 5, 6, 7, 4, 5, 6, 7, 4, 5, 6, 7, 4, 5, 6, 7,
                           0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3,
                           4, 5, 6, 7, 4, 5, 6, 7, 4, 5, 6, 7, 4, 5, 6, 7,
                       });

    square_offset = make_global_order(
        forread, square_offset, tiledb::sm::Layout::UNORDERED);
    REQUIRE(
        square_offset.d1().values_ ==
        std::vector<int32_t>{2, 2, 3, 3, 2, 2, 2, 2, 3, 3, 3, 3, 2, 2, 3, 3,
                             4, 4, 5, 5, 6, 6, 7, 7, 4, 4, 4, 4, 5, 5, 5, 5,
                             6, 6, 6, 6, 7, 7, 7, 7, 4, 4, 5, 5, 6, 6, 7, 7,
                             8, 8, 9, 9, 8, 8, 8, 8, 9, 9, 9, 9, 8, 8, 9, 9});
    REQUIRE(
        square_offset.d2().values_ ==
        std::vector<int32_t>{2, 3, 2, 3, 4, 5, 6, 7, 4, 5, 6, 7, 8, 9, 8, 9,
                             2, 3, 2, 3, 2, 3, 2, 3, 4, 5, 6, 7, 4, 5, 6, 7,
                             4, 5, 6, 7, 4, 5, 6, 7, 8, 9, 8, 9, 8, 9, 8, 9,
                             2, 3, 2, 3, 4, 5, 6, 7, 4, 5, 6, 7, 8, 9, 8, 9});
  }

  const std::vector<TileBounds> expect_row_bounds = {
      {{0, 0}, {0, 7}},
      {{0, 8}, {0, 15}},
      {{0, 16}, {0, 23}},
      {{0, 24}, {0, 31}},
      {{0, 32}, {0, 39}},
      {{0, 40}, {0, 47}},
      {{0, 48}, {0, 55}},
      {{0, 56}, {0, 63}}};

  const std::vector<TileBounds> expect_col_bounds = {
      {{0, 0}, {7, 0}},
      {{8, 0}, {15, 0}},
      {{16, 0}, {23, 0}},
      {{24, 0}, {31, 0}},
      {{32, 0}, {39, 0}},
      {{40, 0}, {47, 0}},
      {{48, 0}, {55, 0}},
      {{56, 0}, {63, 0}}};

  const std::vector<TileBounds> expect_square_bounds = {
      {{0, 0}, {1, 3}},
      {{2, 0}, {3, 3}},
      {{0, 4}, {1, 7}},
      {{2, 4}, {3, 7}},
      {{4, 0}, {5, 3}},
      {{6, 0}, {7, 3}},
      {{4, 4}, {5, 7}},
      {{6, 4}, {7, 7}},
  };

  const std::vector<TileBounds> expect_square_offset_bounds = {
      {{2, 2}, {2, 7}},
      {{3, 4}, {3, 9}},
      {{4, 2}, {7, 3}},
      {{4, 4}, {5, 7}},
      {{6, 4}, {7, 7}},
      {{4, 8}, {7, 9}},
      {{8, 2}, {8, 7}},
      {{9, 4}, {9, 9}}};

  SECTION("Minimum write") {
    Fragment f;
    f.resize(1);
    f.d1()[0] = 0;
    f.d2()[0] = 0;

    std::vector<std::vector<Bounds<Fragment>>> fragment_bounds;
    SECTION("Global Order") {
      fragment_bounds =
          assert_written_bounds<tiledb::test::AsserterCatch, Fragment>(
              ctx, array_uri, std::vector<Fragment>{f});
    }

    SECTION("Unordered") {
      fragment_bounds =
          assert_written_bounds<tiledb::test::AsserterCatch, Fragment>(
              ctx, array_uri, std::vector<Fragment>{f}, sm::Layout::UNORDERED);
    }

    std::vector<TileBounds> expect_bounds = {{{0, 0}, {0, 0}}};
    REQUIRE(fragment_bounds.size() == 1);
    CHECK(fragment_bounds[0] == expect_bounds);
  }

  DYNAMIC_SECTION("Row (layout = " + sm::layout_str(layout) + ")") {
    const auto fragment_bounds =
        assert_written_bounds<tiledb::test::AsserterCatch, Fragment>(
            ctx, array_uri, std::vector<Fragment>{row}, layout);
    REQUIRE(fragment_bounds.size() == 1);
    CHECK(fragment_bounds[0] == expect_row_bounds);
  }

  DYNAMIC_SECTION("Column (layout = " + sm::layout_str(layout) + ")") {
    const auto fragment_bounds =
        assert_written_bounds<tiledb::test::AsserterCatch, Fragment>(
            ctx, array_uri, std::vector<Fragment>{col}, layout);
    REQUIRE(fragment_bounds.size() == 1);
    CHECK(fragment_bounds[0] == expect_col_bounds);
  }

  DYNAMIC_SECTION("Square (layout = " + sm::layout_str(layout) + ")") {
    const auto fragment_bounds =
        assert_written_bounds<tiledb::test::AsserterCatch, Fragment>(
            ctx, array_uri, std::vector<Fragment>{square}, layout);
    REQUIRE(fragment_bounds.size() == 1);
    CHECK(fragment_bounds[0] == expect_square_bounds);
  }

  DYNAMIC_SECTION("Square offset (layout = " + sm::layout_str(layout) + ")") {
    const auto fragment_bounds =
        assert_written_bounds<tiledb::test::AsserterCatch, Fragment>(
            ctx, array_uri, std::vector<Fragment>{square_offset}, layout);
    REQUIRE(fragment_bounds.size() == 1);
    CHECK(fragment_bounds[0] == expect_square_offset_bounds);
  }

  DYNAMIC_SECTION("Multi-fragment (layout = " + sm::layout_str(layout) + ")") {
    const std::vector<Fragment> fragments = {col, square_offset, row, square};
    const auto fragment_bounds =
        assert_written_bounds<tiledb::test::AsserterCatch, Fragment>(
            ctx, array_uri, fragments, layout);
    CHECK(fragment_bounds.size() >= 1);
    if (fragment_bounds.size() >= 1) {
      CHECK(fragment_bounds[0] == expect_col_bounds);
    }
    CHECK(fragment_bounds.size() >= 2);
    if (fragment_bounds.size() >= 2) {
      CHECK(fragment_bounds[1] == expect_square_offset_bounds);
    }
    CHECK(fragment_bounds.size() >= 3);
    if (fragment_bounds.size() >= 3) {
      CHECK(fragment_bounds[2] == expect_row_bounds);
    }
    CHECK(fragment_bounds.size() >= 4);
    if (fragment_bounds.size() >= 4) {
      CHECK(fragment_bounds[3] == expect_square_bounds);
    }
    REQUIRE(fragment_bounds.size() == 4);
  }
}

TEST_CASE(
    "Fragment metadata global order bounds: 2D fixed rapidcheck",
    "[fragment_info][global-order][rapidcheck]") {
  VFSTestSetup vfs_test_setup;
  const auto array_uri = vfs_test_setup.array_uri(
      "fragment_metadata_global_order_bounds_1d_fixed_rapidcheck");

  static constexpr int32_t LB = -256;
  static constexpr int32_t UB = 256;

  const templates::Domain<int32_t> domain(LB, UB);
  const templates::Dimension<Datatype::INT32> d1(domain, 4);
  const templates::Dimension<Datatype::INT32> d2(domain, 4);

  Context ctx = vfs_test_setup.ctx();

  auto temp_array = [&](bool allow_dups) {
    templates::ddl::create_array<Datatype::INT32, Datatype::INT32>(
        array_uri,
        ctx,
        std::tuple<
            const templates::Dimension<Datatype::INT32>&,
            const templates::Dimension<Datatype::INT32>&>{d1, d2},
        std::vector<std::tuple<Datatype, uint32_t, bool>>{},
        TILEDB_ROW_MAJOR,
        TILEDB_ROW_MAJOR,
        8,
        allow_dups);

    return DeleteArrayGuard(ctx.ptr().get(), array_uri.c_str());
  };

  rc::prop("global order", [&](bool allow_dups) {
    auto fragments = *rc::gen::container<std::vector<Fragment2DFixed>>(
        rc::make_fragment_2d<int32_t, int32_t>(allow_dups, domain, domain));
    auto arrayguard = temp_array(allow_dups);
    Array forread(ctx, array_uri, TILEDB_READ);
    std::vector<Fragment2DFixed> global_order_fragments;
    for (const auto& fragment : fragments) {
      global_order_fragments.push_back(make_global_order<Fragment2DFixed>(
          forread, fragment, sm::Layout::UNORDERED));
    }

    assert_written_bounds<tiledb::test::AsserterRapidcheck, Fragment2DFixed>(
        vfs_test_setup.ctx(),
        array_uri,
        global_order_fragments,
        sm::Layout::GLOBAL_ORDER);
  });

  rc::prop("unordered", [&]() {
    const bool allow_dups = false;  // FIXME: not working correctly
    auto fragments = *rc::gen::container<std::vector<Fragment2DFixed>>(
        rc::make_fragment_2d<int32_t, int32_t>(allow_dups, domain, domain));
    auto arrayguard = temp_array(allow_dups);
    Array forread(ctx, array_uri, TILEDB_READ);

    assert_written_bounds<tiledb::test::AsserterRapidcheck, Fragment2DFixed>(
        vfs_test_setup.ctx(), array_uri, fragments, sm::Layout::UNORDERED);
  });
}

TEST_CASE(
    "Fragment metadata global order bounds: 1D var",
    "[fragment_info][global-order]") {
  VFSTestSetup vfs_test_setup;
  const auto array_uri =
      vfs_test_setup.array_uri("fragment_metadata_global_order_bounds_1d_var");

  const bool allow_dups = GENERATE(true, false);

  const templates::Dimension<Datatype::STRING_ASCII> dimension;

  Context ctx = vfs_test_setup.ctx();

  templates::ddl::create_array<Datatype::STRING_ASCII>(
      array_uri,
      ctx,
      std::tuple<const templates::Dimension<Datatype::STRING_ASCII>&>{
          dimension},
      std::vector<std::tuple<Datatype, uint32_t, bool>>{},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      32,
      allow_dups);

  DeleteArrayGuard delarray(ctx.ptr().get(), array_uri.c_str());

  using Fragment = Fragment1DVar;

  SECTION("Minimum write") {
    std::string svalue = GENERATE("foo", "", "long-ish string");
    const templates::StringDimensionCoordType value(
        svalue.begin(), svalue.end());

    Fragment f;
    f.dimension().push_back(value);

    std::vector<std::vector<Bounds<Fragment>>> fragment_bounds;
    SECTION("Global Order") {
      fragment_bounds =
          assert_written_bounds<tiledb::test::AsserterCatch, Fragment>(
              ctx, array_uri, std::vector<Fragment>{f});
    }

    SECTION("Unordered") {
      fragment_bounds =
          assert_written_bounds<tiledb::test::AsserterCatch, Fragment>(
              ctx, array_uri, std::vector<Fragment>{f}, sm::Layout::UNORDERED);
    }
    REQUIRE(fragment_bounds.size() == 1);

    CHECK(
        fragment_bounds[0] ==
        std::vector<Bounds<Fragment>>{
            std::make_pair(std::make_tuple(value), std::make_tuple(value))});
  }

  SECTION("Single fragment") {
    const std::vector<std::string> words = {
        "foo", "bar", "baz", "quux", "corge", "grault", "gub"};

    Fragment f;
    for (const auto& s1 : words) {
      for (const auto& s2 : words) {
        for (const auto& s3 : words) {
          templates::StringDimensionCoordType coord(s1.begin(), s1.end());
          coord.insert(coord.end(), s2.begin(), s2.end());
          coord.insert(coord.end(), s3.begin(), s3.end());

          f.dimension().push_back(coord);
        }
      }
    }

    const sm::Layout layout =
        GENERATE(sm::Layout::UNORDERED, sm::Layout::GLOBAL_ORDER);

    if (layout == sm::Layout::GLOBAL_ORDER) {
      Array forread(ctx, array_uri, TILEDB_READ);
      f = make_global_order(forread, f, tiledb::sm::Layout::UNORDERED);
    }

    DYNAMIC_SECTION(
        "allow_dups = " + std::to_string(allow_dups) +
        ", layout = " + sm::layout_str(layout)) {
      const auto fragment_bounds =
          assert_written_bounds<tiledb::test::AsserterCatch, Fragment>(
              ctx, array_uri, std::vector<Fragment>{f}, layout);
      REQUIRE(fragment_bounds.size() == 1);
      CHECK(fragment_bounds[0].size() == 11);

      auto lbstr = [](const Bounds<Fragment>& bound) {
        const auto& value = std::get<0>(std::get<0>(bound));
        return std::string(value.begin(), value.end());
      };
      auto ubstr = [](const Bounds<Fragment>& bound) {
        const auto& value = std::get<0>(std::get<1>(bound));
        return std::string(value.begin(), value.end());
      };

      CHECK(fragment_bounds[0].size() == 11);
      if (fragment_bounds[0].size() > 0) {
        CHECK(lbstr(fragment_bounds[0][0]) == "barbarbar");
        CHECK(ubstr(fragment_bounds[0][0]) == "bargraultfoo");
      }
      if (fragment_bounds[0].size() > 1) {
        CHECK(lbstr(fragment_bounds[0][1]) == "bargraultgrault");
        CHECK(ubstr(fragment_bounds[0][1]) == "bazcorgebar");
      }
      if (fragment_bounds[0].size() > 2) {
        CHECK(lbstr(fragment_bounds[0][2]) == "bazcorgebaz");
        CHECK(ubstr(fragment_bounds[0][2]) == "bazquuxgrault");
      }
      if (fragment_bounds[0].size() > 3) {
        CHECK(lbstr(fragment_bounds[0][3]) == "bazquuxgub");
        CHECK(ubstr(fragment_bounds[0][3]) == "corgegraultbaz");
      }
      if (fragment_bounds[0].size() > 4) {
        CHECK(lbstr(fragment_bounds[0][4]) == "corgegraultcorge");
        CHECK(ubstr(fragment_bounds[0][4]) == "foobazgub");
      }
      if (fragment_bounds[0].size() > 5) {
        CHECK(lbstr(fragment_bounds[0][5]) == "foobazquux");
        CHECK(ubstr(fragment_bounds[0][5]) == "fooquuxcorge");
      }
      if (fragment_bounds[0].size() > 6) {
        CHECK(lbstr(fragment_bounds[0][6]) == "fooquuxfoo");
        CHECK(ubstr(fragment_bounds[0][6]) == "graultfooquux");
      }
      if (fragment_bounds[0].size() > 7) {
        CHECK(lbstr(fragment_bounds[0][7]) == "graultgraultbar");
        CHECK(ubstr(fragment_bounds[0][7]) == "gubbazfoo");
      }
      if (fragment_bounds[0].size() > 8) {
        CHECK(lbstr(fragment_bounds[0][8]) == "gubbazgrault");
        CHECK(ubstr(fragment_bounds[0][8]) == "gubquuxbar");
      }
      if (fragment_bounds[0].size() > 9) {
        CHECK(lbstr(fragment_bounds[0][9]) == "gubquuxbaz");
        CHECK(ubstr(fragment_bounds[0][9]) == "quuxfoograult");
      }
      if (fragment_bounds[0].size() > 10) {
        CHECK(lbstr(fragment_bounds[0][10]) == "quuxfoogub");
        CHECK(ubstr(fragment_bounds[0][10]) == "quuxquuxquux");
      }
    }
  }
}

TEST_CASE(
    "Fragment metadata global order bounds: 1D var rapidcheck",
    "[fragment_info][global-order][rapidcheck]") {
  VFSTestSetup vfs_test_setup;
  const auto array_uri = vfs_test_setup.array_uri(
      "fragment_metadata_global_order_bounds_1d_var_rapidcheck");

  const templates::Dimension<Datatype::STRING_ASCII> dimension;

  Context ctx = vfs_test_setup.ctx();

  auto temp_array = [&](bool allow_dups) {
    templates::ddl::create_array<Datatype::STRING_ASCII>(
        array_uri,
        ctx,
        std::tuple<const templates::Dimension<Datatype::STRING_ASCII>&>{
            dimension},
        std::vector<std::tuple<Datatype, uint32_t, bool>>{},
        TILEDB_ROW_MAJOR,
        TILEDB_ROW_MAJOR,
        8,
        allow_dups);

    return DeleteArrayGuard(ctx.ptr().get(), array_uri.c_str());
  };

  using F = Fragment1DVar;

  auto instance = [&]<typename Asserter = tiledb::test::AsserterRapidcheck>(
                      bool allow_dups, const std::vector<F>& fragments) {
    auto arrayguard = temp_array(allow_dups);
    Array forread(ctx, array_uri, TILEDB_READ);
    std::vector<F> global_order_fragments;
    for (const auto& fragment : fragments) {
      global_order_fragments.push_back(
          make_global_order<F>(forread, fragment, sm::Layout::UNORDERED));
    }

    assert_written_bounds<Asserter, F>(
        vfs_test_setup.ctx(),
        array_uri,
        global_order_fragments,
        sm::Layout::GLOBAL_ORDER);
  };

  rc::prop("1D var rapidcheck", [&](bool allow_dups) {
    auto fragments = *rc::gen::container<std::vector<F>>(
        rc::make_fragment_1d<templates::StringDimensionCoordType>(allow_dups));

    instance(allow_dups, fragments);
  });

  SECTION("Shrinking") {
    F f;
    f.dimension().push_back(templates::StringDimensionCoordType{'a'});
    f.dimension().push_back(templates::StringDimensionCoordType{'b'});
    f.dimension().push_back(templates::StringDimensionCoordType{'c'});
    f.dimension().push_back(templates::StringDimensionCoordType{'w'});
    f.dimension().push_back(templates::StringDimensionCoordType{'n'});
    f.dimension().push_back(templates::StringDimensionCoordType{'a', 'a'});
    f.dimension().push_back(templates::StringDimensionCoordType{'d'});
    f.dimension().push_back(templates::StringDimensionCoordType{'g'});
    f.dimension().push_back(templates::StringDimensionCoordType{'v'});

    instance.operator()<tiledb::test::AsserterCatch>(false, std::vector<F>{f});
  }
}

TEST_CASE(
    "Fragment metadata global order bounds: 3D vcf",
    "[fragment_info][global-order]") {
  VFSTestSetup vfs_test_setup;
  const auto array_uri =
      vfs_test_setup.array_uri("fragment_metadata_global_order_bounds_3d_vcf");

  const templates::Domain<uint32_t> domain_sample(0, 10000);

  const templates::Dimension<Datatype::STRING_ASCII> d_chromosome;
  const templates::Dimension<Datatype::UINT32> d_position(domain_sample, 100);
  const templates::Dimension<Datatype::STRING_ASCII> d_sample;

  Context ctx = vfs_test_setup.ctx();

  constexpr uint64_t num_chromosomes = 40;
  constexpr uint64_t num_positions = 1000;
  constexpr uint64_t num_samples = 20;

  constexpr uint64_t tile_capacity = num_positions * num_samples / 4;

  templates::ddl::create_array<
      Datatype::STRING_ASCII,
      Datatype::UINT32,
      Datatype::STRING_ASCII>(
      array_uri,
      ctx,
      std::tie(d_chromosome, d_position, d_sample),
      std::vector<std::tuple<Datatype, uint32_t, bool>>{},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      tile_capacity,
      false);

  auto arrayguard = DeleteArrayGuard(ctx.ptr().get(), array_uri.c_str());

  using F = FragmentVcf2025;

  F input;
  for (uint64_t c = 0; c < num_chromosomes; c++) {
    std::stringstream cell_chromosome_ss;
    cell_chromosome_ss << "chr" << std::setfill('0') << std::setw(2) << c;
    const std::string cell_chromosome = cell_chromosome_ss.str();
    for (uint64_t sample = 0; sample < num_samples; sample++) {
      std::stringstream cell_sample_ss;
      cell_sample_ss << "HG" << std::setfill('0') << std::setw(5) << sample;
      const std::string cell_sample = cell_sample_ss.str();
      for (uint64_t pos = 0; pos < num_positions; pos++) {
        input.d1().push_back(cell_chromosome);
        input.d2().push_back(pos);
        input.d3().push_back(cell_sample);
      }
    }
  }

  using Asserter = AsserterCatch;
  const auto all_fragment_bounds = assert_written_bounds<Asserter, F>(
      ctx, array_uri, {input}, sm::Layout::UNORDERED);
  REQUIRE(all_fragment_bounds.size() == 1);

  const auto& tile_bounds = all_fragment_bounds[0];

  // the global order tile order skips variable-length dimensions.
  // this means that the tile order is effectively determined by the
  // position dimension, and then the cells within the tile are ordered
  // in the expected way.
  // arithmetically this happens to work out where we have cycles
  // which are 2 chromosomes long.

  auto to_bound_tuple = [](std::string_view chr,
                           uint64_t p,
                           std::string_view sample) -> CoordsTuple<F> {
    return std::make_tuple(
        std::vector<char>(chr.begin(), chr.end()),
        p,
        std::vector<char>(sample.begin(), sample.end()));
  };
  // (c00, [0, 99], [0, 19]), (c01, [0, 99], [0, 19]), (c02, [0, 49], [0, 19])]
  const CoordsTuple<F> cycle_0_lower = to_bound_tuple("chr00", 0, "HG00000");
  const CoordsTuple<F> cycle_0_upper = to_bound_tuple("chr02", 49, "HG00019");
  // (c02, [49, 99], ..), (c03, [0, 99], ..), (c04, [0, 99], ..)
  const CoordsTuple<F> cycle_1_lower = to_bound_tuple("chr02", 50, "HG00000");
  const CoordsTuple<F> cycle_1_upper = to_bound_tuple("chr04", 99, "HG00019");
  // (c05, [0, 99], ..), (c06, [0, 99], ..), (c07, [0, 49], ..)
  const CoordsTuple<F> cycle_2_lower = to_bound_tuple("chr05", 0, "HG00000");
  const CoordsTuple<F> cycle_2_upper = to_bound_tuple("chr07", 49, "HG00019");
  // (c07, [49, 99], ..), (c08, [0, 99], ..), (c09, [0, 99], ..)
  const CoordsTuple<F> cycle_3_lower = to_bound_tuple("chr07", 50, "HG00000");
  const CoordsTuple<F> cycle_3_upper = to_bound_tuple("chr09", 99, "HG00019");

  const CoordsTuple<F> cycle_lower[] = {
      cycle_0_lower, cycle_1_lower, cycle_2_lower, cycle_3_lower};
  const CoordsTuple<F> cycle_upper[] = {
      cycle_0_upper, cycle_1_upper, cycle_2_upper, cycle_3_upper};

  auto update_chr = [](std::vector<char>& chr, uint64_t offset) {
    std::string_view chrview(chr.begin(), chr.end());
    REQUIRE(chrview.starts_with("chr"));

    std::string i(&chrview.data()[3], 2);

    std::stringstream ss;
    ss << "chr" << std::setfill('0') << std::setw(2)
       << (std::stoll(i) + offset);
    i = ss.str();
    chr = std::vector<char>(i.begin(), i.end());
  };

  for (uint64_t t = 0; t < tile_bounds.size(); t++) {
    const uint64_t chr_offset = (10 * (t / 4)) % num_chromosomes;
    const uint64_t position_offset =
        100 *
        ((t * tile_capacity) / (tile_capacity * 4 * (num_chromosomes / 10)));

    CoordsTuple<F> expect_lower = cycle_lower[t % 4];
    update_chr(std::get<0>(expect_lower), chr_offset);
    std::get<1>(expect_lower) += position_offset;

    CoordsTuple<F> expect_upper = cycle_upper[t % 4];
    update_chr(std::get<0>(expect_upper), chr_offset);
    std::get<1>(expect_upper) += position_offset;

    CHECK(tile_bounds[t].first == expect_lower);
    CHECK(tile_bounds[t].second == expect_upper);
  }
  CHECK(tile_bounds.size() == num_chromosomes * 4);
}

/**
 * Rapidcheck bounds test using the VCF 2025 data model
 * (3D sparse array with chromosome/position/sample dimensions)
 */
TEST_CASE(
    "Fragment metadata global order bounds: 3D vcf rapidcheck",
    "[fragment_info][global-order][rapidcheck]") {
  VFSTestSetup vfs_test_setup;
  const auto array_uri = vfs_test_setup.array_uri(
      "fragment_metadata_global_order_bounds_3d_vcf_rapidcheck");

  const templates::Domain<uint32_t> domain_sample(0, 10000);

  const templates::Dimension<Datatype::STRING_ASCII> d_chromosome;
  const templates::Dimension<Datatype::UINT32> d_position(domain_sample, 32);
  const templates::Dimension<Datatype::STRING_ASCII> d_sample;

  Context ctx = vfs_test_setup.ctx();

  auto temp_array = [&](bool allow_dups) {
    templates::ddl::create_array<
        Datatype::STRING_ASCII,
        Datatype::UINT32,
        Datatype::STRING_ASCII>(
        array_uri,
        ctx,
        std::tie(d_chromosome, d_position, d_sample),
        std::vector<std::tuple<Datatype, uint32_t, bool>>{},
        TILEDB_ROW_MAJOR,
        TILEDB_ROW_MAJOR,
        8,
        allow_dups);

    return DeleteArrayGuard(ctx.ptr().get(), array_uri.c_str());
  };

  using F = FragmentVcf2025;

  auto instance = [&]<typename Asserter = tiledb::test::AsserterRapidcheck>(
                      bool allow_dups, const std::vector<F>& fragments) {
    auto arrayguard = temp_array(allow_dups);
    Array forread(ctx, array_uri, TILEDB_READ);
    std::vector<F> global_order_fragments;
    for (const auto& fragment : fragments) {
      global_order_fragments.push_back(
          make_global_order<F>(forread, fragment, sm::Layout::UNORDERED));
    }

    assert_written_bounds<Asserter, F>(
        vfs_test_setup.ctx(),
        array_uri,
        global_order_fragments,
        sm::Layout::GLOBAL_ORDER);
  };

  rc::prop("3D vcf2025 rapidcheck", [&](bool allow_dups) {
    auto fragments = *rc::gen::container<std::vector<F>>(
        rc::make_fragment_3d<
            templates::StringDimensionCoordType,
            uint32_t,
            templates::StringDimensionCoordType>(
            allow_dups, std::nullopt, domain_sample, std::nullopt));

    instance(allow_dups, fragments);
  });
}

template <templates::FragmentType F>
std::vector<std::vector<Bounds<F>>> consolidate_n_wise(
    const Context& ctx, const std::string& uri, uint64_t fan_in) {
  // step 0: consolidation config
  // NB: this ideally would not be needed but in debug builds
  // a huge amount of memory is allocated and initialized which is very slow
  Config cfg;
  cfg["sm.mem.total_budget"] = std::to_string(128 * 1024 * 1024);

  // step 1: n-wise consolidate
  std::vector<std::string> s_fragment_uris;
  {
    FragmentInfo fi(ctx, uri);
    fi.load();

    for (uint32_t f = 0; f < fi.fragment_num(); f++) {
      s_fragment_uris.push_back(fi.fragment_uri(f));
    }
  }
  for (uint32_t f = 0; f < s_fragment_uris.size(); f += fan_in) {
    std::vector<const char*> fragment_uris;
    for (uint64_t ff = f;
         ff < std::min<uint64_t>(f + fan_in, s_fragment_uris.size());
         ff++) {
      fragment_uris.push_back(s_fragment_uris[ff].c_str());
    }

    Array::consolidate(
        ctx, uri, fragment_uris.data(), fragment_uris.size(), &cfg);
  }

  // step 2: retrieve bounds of new fragments
  return get_all_bounds<F>(ctx, uri);
}

template <templates::FragmentType F>
struct ConsolidateOutput {
  std::vector<F> fragment_data_;
  std::vector<std::vector<Bounds<F>>> bounds_;
};

template <typename Asserter, templates::FragmentType F>
ConsolidateOutput<F> assert_consolidate_n_wise_bounds(
    const Context& ctx,
    const std::string& array_uri,
    const std::vector<F>& input_fragment_data,
    uint64_t fan_in) {
  const auto actual_bounds = consolidate_n_wise<F>(ctx, array_uri, fan_in);

  Array forread(ctx, array_uri, TILEDB_READ);
  const uint64_t tile_stride = forread.schema().capacity();

  std::vector<F> output_fragments;
  for (uint64_t f = 0; f < input_fragment_data.size(); f += fan_in) {
    F output_fragment;
    for (uint64_t ff = f;
         ff < std::min<uint64_t>(f + fan_in, input_fragment_data.size());
         ff++) {
      output_fragment.extend(input_fragment_data[ff]);
    }

    output_fragments.push_back(
        make_global_order<F>(forread, output_fragment, sm::Layout::UNORDERED));
  }

  ASSERTER(output_fragments.size() == actual_bounds.size());
  for (uint64_t f = 0; f < output_fragments.size(); f++) {
    const uint64_t num_tiles = actual_bounds[f].size();
    ASSERTER(
        num_tiles ==
        (output_fragments[f].size() + tile_stride - 1) / tile_stride);

    for (size_t t = 0; t < num_tiles; t++) {
      const uint64_t lbi = t * tile_stride;
      const uint64_t ubi =
          std::min<uint64_t>(
              (t + 1) * tile_stride, output_fragments[f].size()) -
          1;

      const auto lbexpect = tuple_index(output_fragments[f].dimensions(), lbi);
      const auto ubexpect = tuple_index(output_fragments[f].dimensions(), ubi);

      const auto [lbactual, ubactual] = actual_bounds[f][t];
      ASSERTER(lbexpect == lbactual);
      ASSERTER(ubexpect == ubactual);
    }
  }

  return ConsolidateOutput<F>{
      .fragment_data_ = output_fragments, .bounds_ = actual_bounds};
}

TEST_CASE(
    "Fragment metadata global order bounds: 1D fixed consolidation",
    "[fragment_info][global-order]") {
  VFSTestSetup vfs_test_setup;
  const auto array_uri = vfs_test_setup.array_uri(
      "fragment_metadata_global_order_bounds_1d_fixed_consolidation");

  const templates::Dimension<Datatype::UINT64> dimension(
      templates::Domain<uint64_t>(0, 1024 * 8), 16);

  Context ctx = vfs_test_setup.ctx();

  templates::ddl::create_array<Datatype::UINT64>(
      array_uri,
      ctx,
      std::tuple<const templates::Dimension<Datatype::UINT64>&>{dimension},
      std::vector<std::tuple<Datatype, uint32_t, bool>>{},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      8,
      false);

  DeleteArrayGuard delarray(ctx.ptr().get(), array_uri.c_str());

  using Fragment = templates::Fragment1D<uint64_t>;

  SECTION("Non-overlapping") {
    std::vector<Fragment> fs;
    for (uint64_t f = 0; f < 8; f++) {
      Fragment input;
      input.resize(8);
      std::iota(input.dimension().begin(), input.dimension().end(), 1 + f * 8);
      fs.push_back(input);
    }

    const auto fragment_bounds =
        assert_written_bounds<tiledb::test::AsserterCatch, Fragment>(
            vfs_test_setup.ctx(), array_uri, fs);
    REQUIRE(fragment_bounds.size() == fs.size());

    SECTION("Pairs") {
      const auto pairwise = assert_consolidate_n_wise_bounds<
          tiledb::test::AsserterCatch,
          Fragment>(ctx, array_uri, fs, 2);
      CHECK(pairwise.bounds_.size() == fs.size() / 2);

      // each new fragment should have two tiles each,
      // and since they are ascending they should just be a concatenation
      CHECK(
          pairwise.bounds_[0] ==
          std::vector<Bounds<Fragment>>{
              fragment_bounds[0][0], fragment_bounds[1][0]});
      CHECK(
          pairwise.bounds_[1] ==
          std::vector<Bounds<Fragment>>{
              fragment_bounds[2][0], fragment_bounds[3][0]});
      CHECK(
          pairwise.bounds_[2] ==
          std::vector<Bounds<Fragment>>{
              fragment_bounds[4][0], fragment_bounds[5][0]});
      CHECK(
          pairwise.bounds_[3] ==
          std::vector<Bounds<Fragment>>{
              fragment_bounds[6][0], fragment_bounds[7][0]});

      // run another round, now each should have four tiles
      const auto quadwise = assert_consolidate_n_wise_bounds<
          tiledb::test::AsserterCatch,
          Fragment>(ctx, array_uri, pairwise.fragment_data_, 2);
      CHECK(quadwise.bounds_.size() == 2);
      CHECK(
          quadwise.bounds_[0] == std::vector<Bounds<Fragment>>{
                                     fragment_bounds[0][0],
                                     fragment_bounds[1][0],
                                     fragment_bounds[2][0],
                                     fragment_bounds[3][0]});
      CHECK(
          quadwise.bounds_[1] == std::vector<Bounds<Fragment>>{
                                     fragment_bounds[4][0],
                                     fragment_bounds[5][0],
                                     fragment_bounds[6][0],
                                     fragment_bounds[7][0]});

      // run final round
      const auto octwise = assert_consolidate_n_wise_bounds<
          tiledb::test::AsserterCatch,
          Fragment>(ctx, array_uri, quadwise.fragment_data_, 2);
      CHECK(octwise.bounds_.size() == 1);
      CHECK(
          octwise.bounds_[0] == std::vector<Bounds<Fragment>>{
                                    fragment_bounds[0][0],
                                    fragment_bounds[1][0],
                                    fragment_bounds[2][0],
                                    fragment_bounds[3][0],
                                    fragment_bounds[4][0],
                                    fragment_bounds[5][0],
                                    fragment_bounds[6][0],
                                    fragment_bounds[7][0]});
    }

    SECTION("Triples") {
      const auto triwise = assert_consolidate_n_wise_bounds<
          tiledb::test::AsserterCatch,
          Fragment>(ctx, array_uri, fs, 3);
      CHECK(triwise.bounds_.size() == 3);

      // see notes above
      CHECK(
          triwise.bounds_[0] == std::vector<Bounds<Fragment>>{
                                    fragment_bounds[0][0],
                                    fragment_bounds[1][0],
                                    fragment_bounds[2][0]});
      CHECK(
          triwise.bounds_[1] == std::vector<Bounds<Fragment>>{
                                    fragment_bounds[3][0],
                                    fragment_bounds[4][0],
                                    fragment_bounds[5][0]});
      CHECK(
          triwise.bounds_[2] ==
          std::vector<Bounds<Fragment>>{
              fragment_bounds[6][0], fragment_bounds[7][0]});

      const auto ninewise = assert_consolidate_n_wise_bounds<
          tiledb::test::AsserterCatch,
          Fragment>(ctx, array_uri, triwise.fragment_data_, 3);

      CHECK(ninewise.bounds_.size() == 1);
      CHECK(
          ninewise.bounds_[0] == std::vector<Bounds<Fragment>>{
                                     fragment_bounds[0][0],
                                     fragment_bounds[1][0],
                                     fragment_bounds[2][0],
                                     fragment_bounds[3][0],
                                     fragment_bounds[4][0],
                                     fragment_bounds[5][0],
                                     fragment_bounds[6][0],
                                     fragment_bounds[7][0]});
    }
  }

  auto tile = [](uint64_t lb, uint64_t ub) -> Bounds<Fragment> {
    return std::make_pair(std::make_tuple(lb), std::make_tuple(ub));
  };

  SECTION("Interleaving") {
    std::vector<Fragment> fs;
    for (uint64_t f = 0; f < 8; f++) {
      Fragment input;
      input.resize(8);
      for (uint64_t c = 0; c < 8; c++) {
        input.dimension()[c] = (8 * c + 1 + f);
      }
      fs.push_back(input);
    }

    const auto fragment_bounds =
        assert_written_bounds<tiledb::test::AsserterCatch, Fragment>(
            vfs_test_setup.ctx(), array_uri, fs);
    REQUIRE(fragment_bounds.size() == fs.size());

    CHECK(
        fragment_bounds == std::vector<std::vector<Bounds<Fragment>>>{
                               {tile(1, 57)},
                               {tile(2, 58)},
                               {tile(3, 59)},
                               {tile(4, 60)},
                               {tile(5, 61)},
                               {tile(6, 62)},
                               {tile(7, 63)},
                               {tile(8, 64)}});

    SECTION("Pairs") {
      const auto pairwise =
          assert_consolidate_n_wise_bounds<test::AsserterCatch, Fragment>(
              ctx, array_uri, fs, 2);

      CHECK(
          pairwise.bounds_ == std::vector<std::vector<Bounds<Fragment>>>{
                                  {tile(1, 26), tile(33, 58)},
                                  {tile(3, 28), tile(35, 60)},
                                  {tile(5, 30), tile(37, 62)},
                                  {tile(7, 32), tile(39, 64)}});

      const auto quadwise =
          assert_consolidate_n_wise_bounds<test::AsserterCatch, Fragment>(
              ctx, array_uri, pairwise.fragment_data_, 2);
      CHECK(
          quadwise.bounds_ ==
          std::vector<std::vector<Bounds<Fragment>>>{
              {tile(1, 12), tile(17, 28), tile(33, 44), tile(49, 60)},
              {tile(5, 16), tile(21, 32), tile(37, 48), tile(53, 64)},
          });

      const auto octwise =
          assert_consolidate_n_wise_bounds<test::AsserterCatch, Fragment>(
              ctx, array_uri, quadwise.fragment_data_, 2);
      CHECK(
          octwise.bounds_ == std::vector<std::vector<Bounds<Fragment>>>{
                                 {tile(1, 8),
                                  tile(9, 16),
                                  tile(17, 24),
                                  tile(25, 32),
                                  tile(33, 40),
                                  tile(41, 48),
                                  tile(49, 56),
                                  tile(57, 64)}});
    }

    SECTION("Triples") {
      const auto triwise = assert_consolidate_n_wise_bounds<
          tiledb::test::AsserterCatch,
          Fragment>(ctx, array_uri, fs, 3);

      CHECK(
          triwise.bounds_ == ArrayBounds<Fragment>{
                                 {tile(1, 18), tile(19, 41), tile(42, 59)},
                                 {tile(4, 21), tile(22, 44), tile(45, 62)},
                                 {tile(7, 32), tile(39, 64)}});

      const auto ninewise = assert_consolidate_n_wise_bounds<
          tiledb::test::AsserterCatch,
          Fragment>(ctx, array_uri, triwise.fragment_data_, 3);

      CHECK(
          ninewise.bounds_ == std::vector<std::vector<Bounds<Fragment>>>{
                                  {tile(1, 8),
                                   tile(9, 16),
                                   tile(17, 24),
                                   tile(25, 32),
                                   tile(33, 40),
                                   tile(41, 48),
                                   tile(49, 56),
                                   tile(57, 64)}});
    }
  }
}

template <templates::FragmentType F>
void rapidcheck_instance_consolidation(
    const Context& ctx,
    const std::string& array_uri,
    uint64_t fan_in,
    const std::vector<F>& input) {
  Array forread(ctx, array_uri, TILEDB_READ);
  std::vector<F> global_order_fragments;
  for (const auto& fragment : input) {
    global_order_fragments.push_back(
        make_global_order<F>(forread, fragment, sm::Layout::UNORDERED));
  }

  ConsolidateOutput<F> state;
  state.fragment_data_ = global_order_fragments;
  state.bounds_ = assert_written_bounds<tiledb::test::AsserterRapidcheck, F>(
      ctx, array_uri, global_order_fragments, sm::Layout::GLOBAL_ORDER);

  while (state.bounds_.size() > 1) {
    state =
        assert_consolidate_n_wise_bounds<tiledb::test::AsserterRapidcheck, F>(
            ctx, array_uri, state.fragment_data_, fan_in);
  }
}

TEST_CASE(
    "Fragment metadata global order bounds: 1D var consolidation",
    "[fragment_info][global-order]") {
  VFSTestSetup vfs_test_setup;
  const auto array_uri = vfs_test_setup.array_uri(
      "fragment_metadata_global_order_bounds_1d_var_consolidation");

  const bool allow_dups = GENERATE(true, false);

  const templates::Dimension<Datatype::STRING_ASCII> dimension;

  Context ctx = vfs_test_setup.ctx();

  templates::ddl::create_array<Datatype::STRING_ASCII>(
      array_uri,
      ctx,
      std::tuple<const templates::Dimension<Datatype::STRING_ASCII>&>{
          dimension},
      std::vector<std::tuple<Datatype, uint32_t, bool>>{},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      8,
      allow_dups);

  DeleteArrayGuard delarray(ctx.ptr().get(), array_uri.c_str());

  using Fragment = Fragment1DVar;

  const uint64_t num_cells_per_fragment = 16;
  std::vector<Fragment> input;
  for (uint64_t f = 0; f < 8; f++) {
    Fragment fdata;
    for (uint64_t c = 0; c < num_cells_per_fragment; c++) {
      const std::string value = std::to_string(c + f * num_cells_per_fragment);
      fdata.dimension().push_back(
          templates::StringDimensionCoordView(value.data(), value.size()));
    }
    input.push_back(fdata);
  }

  auto tile = [](std::string_view lb, std::string_view ub) -> Bounds<Fragment> {
    return std::make_pair(
        std::make_tuple(
            templates::StringDimensionCoordType(lb.begin(), lb.end())),
        std::make_tuple(
            templates::StringDimensionCoordType(ub.begin(), ub.end())));
  };

  const auto fragment_bounds =
      assert_written_bounds<tiledb::test::AsserterCatch, Fragment>(
          ctx, array_uri, input, sm::Layout::UNORDERED);

  REQUIRE(
      fragment_bounds == ArrayBounds<Fragment>{
                             {tile("0", "15"), tile("2", "9")},
                             {tile("16", "23"), tile("24", "31")},
                             {tile("32", "39"), tile("40", "47")},
                             {tile("48", "55"), tile("56", "63")},
                             {tile("64", "71"), tile("72", "79")},
                             {tile("80", "87"), tile("88", "95")},
                             {tile("100", "107"), tile("108", "99")},
                             {tile("112", "119"), tile("120", "127")}});

  SECTION("Pairs") {
    const auto pairwise =
        assert_consolidate_n_wise_bounds<tiledb::test::AsserterCatch, Fragment>(
            ctx, array_uri, input, 2);

    CHECK(
        pairwise.bounds_ == ArrayBounds<Fragment>{
                                {tile("0", "15"),
                                 tile("16", "22"),
                                 tile("23", "3"),
                                 tile("30", "9")},
                                {tile("32", "39"),
                                 tile("40", "47"),
                                 tile("48", "55"),
                                 tile("56", "63")},
                                {tile("64", "71"),
                                 tile("72", "79"),
                                 tile("80", "87"),
                                 tile("88", "95")},
                                {tile("100", "107"),
                                 tile("108", "115"),
                                 tile("116", "123"),
                                 tile("124", "99")}});

    const auto quadwise =
        assert_consolidate_n_wise_bounds<tiledb::test::AsserterCatch, Fragment>(
            ctx, array_uri, pairwise.fragment_data_, 2);

    CHECK(
        quadwise.bounds_ == ArrayBounds<Fragment>{
                                {tile("0", "15"),
                                 tile("16", "22"),
                                 tile("23", "3"),
                                 tile("30", "37"),
                                 tile("38", "44"),
                                 tile("45", "51"),
                                 tile("52", "59"),
                                 tile("6", "9")},
                                {tile("100", "107"),
                                 tile("108", "115"),
                                 tile("116", "123"),
                                 tile("124", "67"),
                                 tile("68", "75"),
                                 tile("76", "83"),
                                 tile("84", "91"),
                                 tile("92", "99")}});

    const auto octwise =
        assert_consolidate_n_wise_bounds<tiledb::test::AsserterCatch, Fragment>(
            ctx, array_uri, quadwise.fragment_data_, 2);

    CHECK(
        octwise.bounds_ == ArrayBounds<Fragment>{
                               {tile("0", "104"),
                                tile("105", "111"),
                                tile("112", "119"),
                                tile("12", "126"),
                                tile("127", "19"),
                                tile("2", "26"),
                                tile("27", "33"),
                                tile("34", "40"),
                                tile("41", "48"),
                                tile("49", "55"),
                                tile("56", "62"),
                                tile("63", "7"),
                                tile("70", "77"),
                                tile("78", "84"),
                                tile("85", "91"),
                                tile("92", "99")}});
  }

  SECTION("Triples") {
    const auto triwise =
        assert_consolidate_n_wise_bounds<tiledb::test::AsserterCatch, Fragment>(
            ctx, array_uri, input, 3);

    CHECK(
        triwise.bounds_ == ArrayBounds<Fragment>{
                               {tile("0", "15"),
                                tile("16", "22"),
                                tile("23", "3"),
                                tile("30", "37"),
                                tile("38", "44"),
                                tile("45", "9")},
                               {tile("48", "55"),
                                tile("56", "63"),
                                tile("64", "71"),
                                tile("72", "79"),
                                tile("80", "87"),
                                tile("88", "95")},
                               {tile("100", "107"),
                                tile("108", "115"),
                                tile("116", "123"),
                                tile("124", "99")}});

    const auto ninewise =
        assert_consolidate_n_wise_bounds<tiledb::test::AsserterCatch, Fragment>(
            ctx, array_uri, triwise.fragment_data_, 3);

    CHECK(
        ninewise.bounds_ == ArrayBounds<Fragment>{
                                {tile("0", "104"),
                                 tile("105", "111"),
                                 tile("112", "119"),
                                 tile("12", "126"),
                                 tile("127", "19"),
                                 tile("2", "26"),
                                 tile("27", "33"),
                                 tile("34", "40"),
                                 tile("41", "48"),
                                 tile("49", "55"),
                                 tile("56", "62"),
                                 tile("63", "7"),
                                 tile("70", "77"),
                                 tile("78", "84"),
                                 tile("85", "91"),
                                 tile("92", "99")}});
  }
}

TEST_CASE(
    "Fragment metadata global order bounds: 1D fixed consolidation rapidcheck",
    "[fragment_info][global-order][rapidcheck]") {
  VFSTestSetup vfs_test_setup;
  const auto array_uri = vfs_test_setup.array_uri(
      "fragment_metadata_global_order_bounds_1d_fixed_consolidation");

  const templates::Dimension<Datatype::UINT64> dimension(
      templates::Domain<uint64_t>(0, 1024 * 8), 16);

  Context ctx = vfs_test_setup.ctx();

  auto temp_array = [&](bool allow_dups) {
    templates::ddl::create_array<Datatype::UINT64>(
        array_uri,
        ctx,
        std::tuple<const templates::Dimension<Datatype::UINT64>&>{dimension},
        std::vector<std::tuple<Datatype, uint32_t, bool>>{},
        TILEDB_ROW_MAJOR,
        TILEDB_ROW_MAJOR,
        8,
        allow_dups);

    return DeleteArrayGuard(ctx.ptr().get(), array_uri.c_str());
  };

  rc::prop("1D fixed consolidation", [&](bool allow_dups) {
    uint64_t fan_in = *rc::gen::inRange(2, 8);
    auto fragments = *rc::gen::suchThat(
        rc::gen::container<std::vector<Fragment1DFixed>>(
            rc::make_fragment_1d<uint64_t>(allow_dups, dimension.domain)),
        [](auto value) { return value.size() > 1; });

    auto arrayguard = temp_array(allow_dups);
    rapidcheck_instance_consolidation<Fragment1DFixed>(
        ctx, array_uri, fan_in, fragments);
  });
}

TEST_CASE(
    "Fragment metadata global order bounds: 1D var consolidation rapidcheck",
    "[fragment_info][global-order][rapidcheck]") {
  VFSTestSetup vfs_test_setup;
  const auto array_uri = vfs_test_setup.array_uri(
      "fragment_metadata_global_order_bounds_1d_fixed_consolidation");

  const templates::Dimension<Datatype::STRING_ASCII> dimension;

  Context ctx = vfs_test_setup.ctx();

  auto temp_array = [&](bool allow_dups) {
    templates::ddl::create_array<Datatype::STRING_ASCII>(
        array_uri,
        ctx,
        std::tuple<const templates::Dimension<Datatype::STRING_ASCII>&>{
            dimension},
        std::vector<std::tuple<Datatype, uint32_t, bool>>{},
        TILEDB_ROW_MAJOR,
        TILEDB_ROW_MAJOR,
        8,
        allow_dups);

    return DeleteArrayGuard(ctx.ptr().get(), array_uri.c_str());
  };

  using F = Fragment1DVar;

  rc::prop("1D var consolidation", [&](bool allow_dups) {
    uint64_t fan_in = *rc::gen::inRange(2, 8);
    auto fragments = *rc::gen::suchThat(
        rc::gen::container<std::vector<F>>(
            rc::make_fragment_1d<templates::StringDimensionCoordType>(
                allow_dups)),
        [](auto value) { return value.size() > 1; });

    auto arrayguard = temp_array(allow_dups);
    rapidcheck_instance_consolidation<F>(ctx, array_uri, fan_in, fragments);
  });
}

/**
 * Rapidcheck bounds consolidation test using the VCF 2025 data model
 * (3D sparse array with chromosome/position/sample dimensions)
 */
TEST_CASE(
    "Fragment metadata global order bounds: 3D vcf consolidation rapidcheck",
    "[fragment_info][global-order][rapidcheck]") {
  VFSTestSetup vfs_test_setup;
  const auto array_uri = vfs_test_setup.array_uri(
      "fragment_metadata_global_order_bounds_3d_vcf_consolidation");

  const templates::Domain<uint32_t> domain_sample(0, 10000);

  const templates::Dimension<Datatype::STRING_ASCII> d_chromosome;
  const templates::Dimension<Datatype::UINT32> d_position(domain_sample, 32);
  const templates::Dimension<Datatype::STRING_ASCII> d_sample;

  Context ctx = vfs_test_setup.ctx();

  auto temp_array = [&](bool allow_dups) {
    templates::ddl::create_array<
        Datatype::STRING_ASCII,
        Datatype::UINT32,
        Datatype::STRING_ASCII>(
        array_uri,
        ctx,
        std::tie(d_chromosome, d_position, d_sample),
        std::vector<std::tuple<Datatype, uint32_t, bool>>{},
        TILEDB_ROW_MAJOR,
        TILEDB_ROW_MAJOR,
        8,
        allow_dups);

    return DeleteArrayGuard(ctx.ptr().get(), array_uri.c_str());
  };

  using F = FragmentVcf2025;

  rc::prop("3D vcf2025 consolidation", [&](bool allow_dups) {
    uint64_t fan_in = *rc::gen::inRange(2, 8);
    auto fragments = *rc::gen::suchThat(
        rc::gen::container<std::vector<F>>(rc::make_fragment_3d<
                                           templates::StringDimensionCoordType,
                                           uint32_t,
                                           templates::StringDimensionCoordType>(
            allow_dups, std::nullopt, domain_sample, std::nullopt)),
        [](auto value) { return value.size() > 1; });

    auto arrayguard = temp_array(allow_dups);
    rapidcheck_instance_consolidation<F>(ctx, array_uri, fan_in, fragments);
  });
}
