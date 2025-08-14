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

void showValue(const Fragment1DFixed& value, std::ostream& os) {
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
  sm::GlobalCellCmp globalcmp(array.schema().ptr()->array_schema()->domain());

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

template <typename T>
auto tuple_index(const T& tuple, uint64_t idx) {
  return std::apply(
      [&](const auto&... field) { return std::make_tuple(field[idx]...); },
      tuple);
}

template <templates::FragmentType F>
using CoordsTuple = decltype(tuple_index(
    std::declval<F>().dimensions(), std::declval<uint64_t>()));

template <templates::FragmentType F>
static void prepare_bound_buffers(
    DimensionTuple<F>& bufs, std::array<void*, F::NUM_DIMENSIONS>& ptrs) {
  uint64_t i = 0;
  std::apply(
      [&]<typename... Ts>(templates::query_buffers<Ts>&... qbufs) {
        ([&]() { ptrs[i++] = static_cast<void*>(&qbufs[0]); }(), ...);
      },
      bufs);
}

template <templates::FragmentType F>
using Bounds = std::pair<CoordsTuple<F>, CoordsTuple<F>>;

/**
 * @return the lower and upper bounds of tile `(f, t)` in the fragment info
 */
template <templates::FragmentType F>
static Bounds<F> global_order_bounds(
    const FragmentInfo& finfo, uint64_t fragment, uint64_t tile) {
  constexpr size_t num_fields = std::tuple_size<DimensionTuple<F>>::value;

  // FIXME: there needs to be another API to ask about maximum variable-length.
  // Otherwise it is unsafe to call this API with variable-length dimensions

  DimensionTuple<F> lb, ub;
  std::apply(
      []<typename... Ts>(templates::query_buffers<Ts>&... field) {
        (field.resize(1), ...);
      },
      lb);
  std::apply(
      []<typename... Ts>(templates::query_buffers<Ts>&... field) {
        (field.resize(1), ...);
      },
      ub);

  size_t lb_sizes[num_fields];
  std::array<void*, num_fields> lb_dimensions;
  prepare_bound_buffers<F>(lb, lb_dimensions);

  size_t ub_sizes[num_fields];
  std::array<void*, num_fields> ub_dimensions;
  prepare_bound_buffers<F>(ub, ub_dimensions);

  auto ctx_c = finfo.context().ptr().get();

  // FIXME: add C++ API
  auto rc = tiledb_fragment_info_get_global_order_lower_bound(
      ctx_c,
      finfo.ptr().get(),
      fragment,
      tile,
      &lb_sizes[0],
      &lb_dimensions[0]);
  throw_if_error(ctx_c, rc);

  rc = tiledb_fragment_info_get_global_order_upper_bound(
      ctx_c,
      finfo.ptr().get(),
      fragment,
      tile,
      &ub_sizes[0],
      &ub_dimensions[0]);
  throw_if_error(ctx_c, rc);

  return std::make_pair(tuple_index(lb, 0), tuple_index(ub, 0));
}

/**
 * Asserts that when a set of fragments are written, the fragment metadata
 * accurately reflects the expected global order bounds of the input.
 *
 * "Accurately reflects" means that:
 * 1) the lower bound is indeed the first coordinate in global order in the
 * fragment 2) the upper bound is indeed the last coordinate in global order in
 * the fragment
 *
 * @return the global order bounds for each tile per fragment
 */
template <typename Asserter, templates::FragmentType F>
std::vector<std::vector<Bounds<F>>> instance(
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
  std::vector<std::vector<std::pair<CoordsTuple<F>, CoordsTuple<F>>>> bounds;
  Array forread(ctx, array_uri, TILEDB_READ);

  const uint64_t tile_stride = forread.schema().capacity();

  FragmentInfo finfo(ctx, array_uri);
  finfo.load();

  for (size_t f = 0; f < fragments.size(); f++) {
    const auto fragment = make_global_order(forread, fragments[f], layout);

    std::decay_t<decltype(bounds[0])> fragment_bounds;

    const uint64_t num_tiles = finfo.mbr_num(f);
    for (size_t t = 0; t < num_tiles; t++) {
      const uint64_t lbi = t * tile_stride;
      const uint64_t ubi = std::min((t + 1) * tile_stride, fragment.size()) - 1;

      const auto lbexpect = tuple_index(fragment.dimensions(), lbi);
      const auto ubexpect = tuple_index(fragment.dimensions(), ubi);

      const auto [lbactual, ubactual] = global_order_bounds<F>(finfo, f, t);
      ASSERTER(lbexpect == lbactual);
      ASSERTER(ubexpect == ubactual);

      fragment_bounds.push_back(std::make_pair(lbactual, ubactual));
    }

    bounds.push_back(fragment_bounds);
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
      fragment_bounds = instance<tiledb::test::AsserterCatch, Fragment1DFixed>(
          vfs_test_setup.ctx(), array_uri, std::vector<Fragment1DFixed>{f});
    }

    SECTION("Unordered") {
      fragment_bounds = instance<tiledb::test::AsserterCatch, Fragment1DFixed>(
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
          instance<tiledb::test::AsserterCatch, Fragment1DFixed>(
              vfs_test_setup.ctx(), array_uri, std::vector<Fragment1DFixed>{f});
      REQUIRE(fragment_bounds.size() == 1);
      CHECK(fragment_bounds[0] == expect);
    }

    SECTION("Unordered") {
      for (uint64_t i = 0; i < f.size(); i++) {
        f.dimension()[i] = f.size() - i;
      }

      const auto fragment_bounds =
          instance<tiledb::test::AsserterCatch, Fragment1DFixed>(
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
            instance<tiledb::test::AsserterCatch, Fragment1DFixed>(
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
            instance<tiledb::test::AsserterCatch, Fragment1DFixed>(
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

    instance<tiledb::test::AsserterRapidcheck, Fragment1DFixed>(
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

    instance<tiledb::test::AsserterRapidcheck, Fragment1DFixed>(
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

  templates::ddl::create_array<Datatype::INT32, Datatype::INT32>(
      array_uri,
      vfs_test_setup.ctx(),
      std::tie(d1, d2),
      std::vector<std::tuple<Datatype, uint32_t, bool>>{},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      8,
      allow_dups);

  DeleteArrayGuard delarray(
      vfs_test_setup.ctx().ptr().get(), array_uri.c_str());

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

  if (layout == sm::Layout::UNORDERED) {
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
  } else {
    SKIP("TODO");
    // TODO
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
      fragment_bounds = instance<tiledb::test::AsserterCatch, Fragment>(
          vfs_test_setup.ctx(), array_uri, std::vector<Fragment>{f});
    }

    SECTION("Unordered") {
      fragment_bounds = instance<tiledb::test::AsserterCatch, Fragment>(
          vfs_test_setup.ctx(),
          array_uri,
          std::vector<Fragment>{f},
          sm::Layout::UNORDERED);
    }

    std::vector<TileBounds> expect_bounds = {{{0, 0}, {0, 0}}};
    REQUIRE(fragment_bounds.size() == 1);
    CHECK(fragment_bounds[0] == expect_bounds);
  }

  DYNAMIC_SECTION("Row (layout = " + sm::layout_str(layout) + ")") {
    const auto fragment_bounds =
        instance<tiledb::test::AsserterCatch, Fragment>(
            vfs_test_setup.ctx(),
            array_uri,
            std::vector<Fragment>{row},
            layout);
    REQUIRE(fragment_bounds.size() == 1);
    CHECK(fragment_bounds[0] == expect_row_bounds);
  }

  DYNAMIC_SECTION("Column (layout = " + sm::layout_str(layout) + ")") {
    const auto fragment_bounds =
        instance<tiledb::test::AsserterCatch, Fragment>(
            vfs_test_setup.ctx(),
            array_uri,
            std::vector<Fragment>{col},
            layout);
    REQUIRE(fragment_bounds.size() == 1);
    CHECK(fragment_bounds[0] == expect_col_bounds);
  }

  DYNAMIC_SECTION("Square (layout = " + sm::layout_str(layout) + ")") {
    const auto fragment_bounds =
        instance<tiledb::test::AsserterCatch, Fragment>(
            vfs_test_setup.ctx(),
            array_uri,
            std::vector<Fragment>{square},
            layout);
    REQUIRE(fragment_bounds.size() == 1);
    CHECK(fragment_bounds[0] == expect_square_bounds);
  }

  DYNAMIC_SECTION("Square offset (layout = " + sm::layout_str(layout) + ")") {
    const auto fragment_bounds =
        instance<tiledb::test::AsserterCatch, Fragment>(
            vfs_test_setup.ctx(),
            array_uri,
            std::vector<Fragment>{square_offset},
            layout);
    REQUIRE(fragment_bounds.size() == 1);
    CHECK(fragment_bounds[0] == expect_square_offset_bounds);
  }

  DYNAMIC_SECTION("Multi-fragment (layout = " + sm::layout_str(layout) + ")") {
    const std::vector<Fragment> fragments = {col, square_offset, row, square};
    const auto fragment_bounds =
        instance<tiledb::test::AsserterCatch, Fragment>(
            vfs_test_setup.ctx(), array_uri, fragments, layout);
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
