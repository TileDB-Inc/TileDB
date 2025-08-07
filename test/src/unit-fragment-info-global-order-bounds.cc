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

#include <test/support/src/array_templates.h>
#include <test/support/src/vfs_helpers.h>

using namespace tiledb;
using namespace tiledb::test;

/**
 * @return another fragment containing the contents of the argument sorted in
 * global order based on `array.schema()`.
 */
template <templates::FragmentType F>
static F make_global_order(
    const Array& array, const F& fragment, tiledb_layout_t layout) {
  if (layout == TILEDB_GLOBAL_ORDER) {
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

/**
 * @return the lower and upper bounds of tile `(f, t)` in the fragment info
 */
template <templates::FragmentType F>
static std::pair<CoordsTuple<F>, CoordsTuple<F>> global_order_bounds(
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
 * @return the global order bounds for each tile per fragment
 */
template <typename Asserter, templates::FragmentType F>
std::vector<std::vector<std::pair<CoordsTuple<F>, CoordsTuple<F>>>> instance(
    Context ctx,
    const std::string& array_uri,
    const std::vector<F>& fragments,
    tiledb_layout_t layout = TILEDB_GLOBAL_ORDER) {
  // write each fragment
  {
    Array forwrite(ctx, array_uri, TILEDB_WRITE);
    for (const auto& fragment : fragments) {
      templates::query::write_fragment<Asserter, F>(fragment, forwrite);
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
      templates::Domain<uint64_t>(1, 1024), 16);
  templates::ddl::create_array<Datatype::UINT64>(
      array_uri,
      vfs_test_setup.ctx(),
      std::tuple<const templates::Dimension<Datatype::UINT64>&>{dimension},
      std::vector<std::tuple<Datatype, uint32_t, bool>>{},
      TILEDB_ROW_MAJOR,
      TILEDB_ROW_MAJOR,
      8,
      false);

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

  SECTION("Example 1") {
    Fragment f;
    f.resize(64);
    std::iota(f.dimension().begin(), f.dimension().end(), 1);

    const auto fragment_bounds =
        instance<tiledb::test::AsserterCatch, Fragment>(
            vfs_test_setup.ctx(), array_uri, std::vector<Fragment>{f});
    REQUIRE(fragment_bounds.size() == 1);

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
    CHECK(fragment_bounds[0] == expect);
  }
}
