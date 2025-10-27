#include <test/support/assert_helpers.h>
#include <test/support/tdb_catch.h>
#include "test/support/rapidcheck/array_schema_templates.h"
#include "test/support/src/array_schema_templates.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/tile/arithmetic.h"
#include "tiledb/type/range/range.h"

#include <span>

using namespace tiledb;
using namespace sm;
using namespace tiledb::test;

template <typename T>
static bool is_rectangular_domain(
    std::span<const T> tile_extents,
    T lower_bound,
    T upper_bound,
    uint64_t start_tile,
    uint64_t num_tiles,
    Layout tile_order = Layout::ROW_MAJOR) {
  sm::NDRange r;
  r.push_back(Range(lower_bound, upper_bound));
  return is_rectangular_domain(
      tile_order, tile_extents, r, start_tile, num_tiles);
}

template <typename T>
static bool is_rectangular_domain(
    std::span<const T> tile_extents,
    T d1_lower_bound,
    T d1_upper_bound,
    T d2_lower_bound,
    T d2_upper_bound,
    uint64_t start_tile,
    uint64_t num_tiles,
    Layout tile_order = Layout::ROW_MAJOR) {
  sm::NDRange r;
  r.push_back(Range(d1_lower_bound, d1_upper_bound));
  r.push_back(Range(d2_lower_bound, d2_upper_bound));
  return is_rectangular_domain(
      tile_order, tile_extents, r, start_tile, num_tiles);
}

template <sm::Datatype DT>
static bool is_rectangular_domain(
    const templates::Dimension<DT>& d1,
    const templates::Dimension<DT>& d2,
    uint64_t start_tile,
    uint64_t num_tiles,
    Layout tile_order = Layout::ROW_MAJOR) {
  using Coord = templates::Dimension<DT>::value_type;
  const std::vector<Coord> extents = {d1.extent, d2.extent};
  return is_rectangular_domain<Coord>(
      extents,
      d1.domain.lower_bound,
      d1.domain.upper_bound,
      d2.domain.lower_bound,
      d2.domain.upper_bound,
      start_tile,
      num_tiles,
      tile_order);
}

template <sm::Datatype DT>
static bool is_rectangular_domain(
    const templates::Dimension<DT>& d1,
    const templates::Dimension<DT>& d2,
    const templates::Dimension<DT>& d3,
    uint64_t start_tile,
    uint64_t num_tiles,
    Layout tile_order = Layout::ROW_MAJOR) {
  using Coord = templates::Dimension<DT>::value_type;
  const std::vector<Coord> extents = {d1.extent, d2.extent, d3.extent};
  sm::NDRange r;
  r.push_back(Range(d1.domain.lower_bound, d1.domain.upper_bound));
  r.push_back(Range(d2.domain.lower_bound, d2.domain.upper_bound));
  r.push_back(Range(d3.domain.lower_bound, d3.domain.upper_bound));
  return is_rectangular_domain<Coord>(
      tile_order, extents, r, start_tile, num_tiles);
}

// in one dimension all domains are rectangles
TEST_CASE("is_rectangular_domain 1d", "[arithmetic]") {
  rc::prop(
      "is_rectangular_domain 1d",
      [](templates::Dimension<sm::Datatype::UINT64> dimension) {
        const uint64_t start_tile =
            *rc::gen::inRange<uint64_t>(0, dimension.num_tiles());
        const uint64_t num_tiles =
            *rc::gen::inRange<uint64_t>(1, dimension.num_tiles() - start_tile);

        const std::vector<uint64_t> extents = {dimension.extent};
        RC_ASSERT(is_rectangular_domain<uint64_t>(
            extents,
            dimension.domain.lower_bound,
            dimension.domain.upper_bound,
            start_tile,
            num_tiles));
      });
}

TEST_CASE("is_rectangular_domain 2d", "[arithmetic]") {
  /*
   * Domain is a 16x16 square
   */
  SECTION("Square") {
    const uint64_t d1_lower = GENERATE(0, 3);
    const uint64_t d1_upper = d1_lower + 16 - 1;
    const uint64_t d2_lower = GENERATE(0, 3);
    const uint64_t d2_upper = d2_lower + 16 - 1;

    SECTION("Row tiles") {
      const std::vector<uint64_t> extents = {1, 16};
      for (uint64_t start_tile = 0; start_tile < 15; start_tile++) {
        for (uint64_t num_tiles = 1; start_tile + num_tiles <= 16;
             num_tiles++) {
          CAPTURE(start_tile, num_tiles);
          CHECK(is_rectangular_domain<uint64_t>(
              extents,
              d1_lower,
              d1_upper,
              d2_lower,
              d2_upper,
              start_tile,
              num_tiles));
        }
      }
    }

    SECTION("Square tiles") {
      // 7x7 tiles will subdivide the 16x16 square into 3x3 tiles
      const std::vector<uint64_t> extents = {7, 7};

      auto tt = [&](uint64_t start_tile, uint64_t num_tiles) -> bool {
        return is_rectangular_domain<uint64_t>(
            extents,
            d1_lower,
            d1_upper,
            d2_lower,
            d2_upper,
            start_tile,
            num_tiles);
      };

      // tiles aligned with the start: rectangle formed if less than one row, or
      // integral number of rows
      for (uint64_t start_tile : {0, 3, 6}) {
        for (uint64_t num_tiles = 1; start_tile + num_tiles <= 9; num_tiles++) {
          CAPTURE(start_tile, num_tiles);
          if (num_tiles < 3 || num_tiles % 3 == 0) {
            CHECK(tt(start_tile, num_tiles));
          } else {
            CHECK(!tt(start_tile, num_tiles));
          }
        }
      }

      // otherwise a rectangle is only formed within the same row
      for (uint64_t start_tile : {1, 2, 4, 5, 7, 8}) {
        for (uint64_t num_tiles = 1; start_tile + num_tiles <= 9; num_tiles++) {
          CAPTURE(start_tile, num_tiles);
          if ((start_tile % 3) + num_tiles <= 3) {
            CHECK(tt(start_tile, num_tiles));
          } else {
            CHECK(!tt(start_tile, num_tiles));
          }
        }
      }
    }
  }

  using Dim64 = templates::Dimension<sm::Datatype::UINT64>;

  auto instance_is_rectangular_domain_2d =
      []<typename Asserter = AsserterCatch>(Dim64 d1, Dim64 d2) {
        const std::vector<uint64_t> extents = {d1.extent, d2.extent};
        auto tt = [&](uint64_t start_tile, uint64_t num_tiles) -> bool {
          return is_rectangular_domain(d1, d2, start_tile, num_tiles);
        };

        const uint64_t total_tiles = d1.num_tiles() * d2.num_tiles();

        for (uint64_t t = 0; t < d1.num_tiles(); t += d2.num_tiles()) {
          // row-aligned tiles
          for (uint64_t num_tiles = 1; t + num_tiles <= total_tiles;
               num_tiles++) {
            if (num_tiles <= d2.num_tiles() ||
                num_tiles % d2.num_tiles() == 0) {
              ASSERTER(tt(t, num_tiles));
            } else {
              ASSERTER(!tt(t, num_tiles));
            }
          }
          // other tiles
          for (uint64_t o = 1; t + o < d2.num_tiles(); o++) {
            for (uint64_t num_tiles = 1; t + o + num_tiles <= total_tiles;
                 num_tiles++) {
              if (((t + o) % d2.num_tiles()) + num_tiles <= d2.num_tiles()) {
                ASSERTER(tt(t + o, num_tiles));
              } else {
                ASSERTER(!tt(t + o, num_tiles));
              }
            }
          }
        }
      };

  SECTION("Shrinking") {
    instance_is_rectangular_domain_2d(Dim64(0, 2, 1), Dim64(0, 0, 1));
    instance_is_rectangular_domain_2d(Dim64(0, 2, 1), Dim64(0, 1, 1));
  }

  rc::prop("is_rectangular_domain 2d", [&]() {
    Dim64 d1 = *rc::make_dimension<sm::Datatype::UINT64>(std::nullopt, {64});
    Dim64 d2 = *rc::make_dimension<sm::Datatype::UINT64>(std::nullopt, {64});
    instance_is_rectangular_domain_2d.operator()<AsserterRapidcheck>(d1, d2);
  });
}

TEST_CASE("is_rectangular_domain 3d", "[arithmetic]") {
  using Dim64 = templates::Dimension<sm::Datatype::UINT64>;

  /**
   * 3D plane tiles (where the outermost dimension has extent 1)
   * should produce the same results as rectangular tiles in the plane
   */
  rc::prop("plane tiles", [&]() {
    Dim64 d1 = *rc::make_dimension<sm::Datatype::UINT64>(std::nullopt, {1});
    Dim64 d2 = *rc::make_dimension<sm::Datatype::UINT64>(std::nullopt, {32});
    Dim64 d3 = *rc::make_dimension<sm::Datatype::UINT64>(std::nullopt, {32});

    const uint64_t total_tiles =
        d1.num_tiles() * d2.num_tiles() * d3.num_tiles();
    for (uint64_t start_tile = 0; start_tile < total_tiles; start_tile++) {
      for (uint64_t num_tiles = 1; start_tile + num_tiles <= total_tiles;
           num_tiles++) {
        const bool rectangle =
            is_rectangular_domain(d2, d3, start_tile, num_tiles);
        const bool plane =
            is_rectangular_domain(d1, d2, d3, start_tile, num_tiles);

        RC_ASSERT(rectangle == plane);
      }
    }
  });

  /**
   * Runs over the possible `(start_tiles, num_tiles)` pairs for dimensions
   * `{d1, d2, d3}` and asserts that `is_rectangular_domain` returns true if and
   * only if the pair represents an expected rectangle.
   */
  auto instance_is_rectangular_domain_3d =
      []<typename Asserter = AsserterCatch>(Dim64 d1, Dim64 d2, Dim64 d3) {
        auto tt = [&](uint64_t start_tile, uint64_t num_tiles) -> bool {
          return is_rectangular_domain(d1, d2, d3, start_tile, num_tiles);
        };

        const uint64_t total_tiles =
            d1.num_tiles() * d2.num_tiles() * d3.num_tiles();
        const uint64_t plane_tiles = d2.num_tiles() * d3.num_tiles();

        for (uint64_t start_tile = 0; start_tile < total_tiles; start_tile++) {
          for (uint64_t num_tiles = 1; start_tile + num_tiles <= total_tiles;
               num_tiles++) {
            if (start_tile % plane_tiles == 0) {
              // aligned to a plane, several options to be a rectangle
              if (num_tiles <= d3.num_tiles()) {
                ASSERTER(tt(start_tile, num_tiles));
              } else if (
                  num_tiles <= plane_tiles && num_tiles % d3.num_tiles() == 0) {
                ASSERTER(tt(start_tile, num_tiles));
              } else if (num_tiles % (plane_tiles) == 0) {
                ASSERTER(tt(start_tile, num_tiles));
              } else {
                ASSERTER(!tt(start_tile, num_tiles));
              }
            } else if (start_tile % d3.num_tiles() == 0) {
              // aligned to a row within a plane, but not aligned to the plane
              // this is a rectangle if it is an integral number of rows, or
              // fits within a row
              if (num_tiles <= d3.num_tiles()) {
                ASSERTER(tt(start_tile, num_tiles));
              } else if (
                  num_tiles % d3.num_tiles() == 0 &&
                  (start_tile % plane_tiles) + num_tiles <= plane_tiles) {
                ASSERTER(tt(start_tile, num_tiles));
              } else {
                ASSERTER(!tt(start_tile, num_tiles));
              }
            } else {
              // unaligned, only a rectangle if it doesn't advance rows
              if (start_tile % d3.num_tiles() + num_tiles <= d3.num_tiles()) {
                ASSERTER(tt(start_tile, num_tiles));
              } else {
                ASSERTER(!tt(start_tile, num_tiles));
              }
            }
          }
        }
      };

  SECTION("Shrinking") {
    instance_is_rectangular_domain_3d(
        Dim64(0, 1, 1), Dim64(0, 0, 1), Dim64(0, 1, 1));
    instance_is_rectangular_domain_3d(
        Dim64(0, 1, 1), Dim64(0, 2, 1), Dim64(0, 0, 1));
  }

  rc::prop("any tiles", [&]() {
    const Dim64 d1 =
        *rc::make_dimension<sm::Datatype::UINT64>(std::nullopt, {16});
    const Dim64 d2 =
        *rc::make_dimension<sm::Datatype::UINT64>(std::nullopt, {16});
    const Dim64 d3 =
        *rc::make_dimension<sm::Datatype::UINT64>(std::nullopt, {16});

    instance_is_rectangular_domain_3d.operator()<AsserterRapidcheck>(
        d1, d2, d3);
  });
}

template <typename T, typename Asserter = AsserterCatch>
std::optional<sm::NDRange> instance_domain_tile_offset(
    std::span<const T> tile_extents,
    const sm::NDRange& domain,
    uint64_t start_tile,
    uint64_t num_tiles,
    Layout tile_order = Layout::ROW_MAJOR) {
  const bool expect_rectangle = is_rectangular_domain(
      tile_order, tile_extents, domain, start_tile, num_tiles);
  const std::optional<sm::NDRange> adjusted_domain = domain_tile_offset(
      tile_order, tile_extents, domain, start_tile, num_tiles);
  if (!expect_rectangle) {
    ASSERTER(!adjusted_domain.has_value());
    return std::nullopt;
  }

  ASSERTER(adjusted_domain.has_value());

  uint64_t num_tiles_result = 1;
  for (uint64_t d = 0; d < tile_extents.size(); d++) {
    const uint64_t num_tiles_this_dimension =
        sm::Dimension::tile_idx<T>(
            adjusted_domain.value()[d].end_as<T>(),
            adjusted_domain.value()[d].start_as<T>(),
            tile_extents[d]) +
        1;
    num_tiles_result *= num_tiles_this_dimension;
  }
  ASSERTER(num_tiles_result == num_tiles);

  const std::vector<uint64_t> hyperrow_sizes =
      compute_hyperrow_sizes(tile_order, tile_extents, domain);

  uint64_t start_tile_result = 0;
  for (uint64_t di = 0; di < tile_extents.size(); di++) {
    const uint64_t d =
        (tile_order == Layout::ROW_MAJOR ? di : tile_extents.size() - di - 1);
    const uint64_t start_tile_this_dimension = sm::Dimension::tile_idx<T>(
        adjusted_domain.value()[d].start_as<T>(),
        domain[d].start_as<T>(),
        tile_extents[d]);
    start_tile_result += start_tile_this_dimension * hyperrow_sizes[di + 1];
  }
  ASSERTER(start_tile_result == start_tile);

  return adjusted_domain;
}

template <typename T, typename Asserter = AsserterCatch>
void instance_domain_tile_offset(
    std::span<const T> tile_extents,
    const sm::NDRange& domain,
    Layout tile_order = Layout::ROW_MAJOR) {
  uint64_t total_tiles = 1;
  for (uint64_t d = 0; d < tile_extents.size(); d++) {
    const uint64_t num_tiles_this_dimension =
        sm::Dimension::tile_idx<T>(
            domain[d].end_as<T>(), domain[d].start_as<T>(), tile_extents[d]) +
        1;
    total_tiles *= num_tiles_this_dimension;
  }
  for (uint64_t start_tile = 0; start_tile < total_tiles; start_tile++) {
    for (uint64_t num_tiles = 1; start_tile + num_tiles <= total_tiles;
         num_tiles++) {
      instance_domain_tile_offset<T, Asserter>(
          tile_extents, domain, start_tile, num_tiles, tile_order);
    }
  }
}

template <sm::Datatype DT, typename Asserter = AsserterCatch>
std::optional<std::vector<
    templates::Domain<typename templates::Dimension<DT>::value_type>>>
instance_domain_tile_offset(
    const std::vector<templates::Dimension<DT>>& dims,
    uint64_t start_tile,
    uint64_t num_tiles,
    Layout tile_order = Layout::ROW_MAJOR) {
  using Coord = typename templates::Dimension<DT>::value_type;

  std::vector<Coord> tile_extents;
  for (const auto& dim : dims) {
    tile_extents.push_back(dim.extent);
  }

  sm::NDRange domain;
  for (const auto& dim : dims) {
    domain.push_back(Range(dim.domain.lower_bound, dim.domain.upper_bound));
  }

  const auto range = instance_domain_tile_offset<Coord, Asserter>(
      tile_extents, domain, start_tile, num_tiles, tile_order);
  if (!range.has_value()) {
    return std::nullopt;
  }

  std::vector<templates::Domain<Coord>> typed_range;
  for (const auto& r : range.value()) {
    typed_range.emplace_back(
        r.template start_as<Coord>(), r.template end_as<Coord>());
  }
  return typed_range;
}

template <sm::Datatype DT, typename Asserter = AsserterCatch>
void instance_domain_tile_offset(
    const std::vector<templates::Dimension<DT>>& dims,
    Layout tile_order = Layout::ROW_MAJOR) {
  using Coord = templates::Dimension<DT>::value_type;

  std::vector<Coord> tile_extents;
  for (const auto& dim : dims) {
    tile_extents.push_back(dim.extent);
  }

  sm::NDRange domain;
  for (const auto& dim : dims) {
    domain.push_back(Range(dim.domain.lower_bound, dim.domain.upper_bound));
  }

  instance_domain_tile_offset<Coord, Asserter>(
      tile_extents, domain, tile_order);
}

TEST_CASE("domain_tile_offset 1d", "[arithmetic]") {
  using Dim64 = templates::Dimension<sm::Datatype::UINT64>;

  SECTION("Shrinking") {
    instance_domain_tile_offset<Dim64::DATATYPE>({Dim64(0, 18, 5)});
  }

  rc::prop("any tiles", []() {
    const Dim64 d1 = *rc::make_dimension<Dim64::DATATYPE>(std::nullopt, {128});

    instance_domain_tile_offset<Dim64::DATATYPE, AsserterRapidcheck>({d1});
  });
}

TEST_CASE("domain_tile_offset 2d", "[arithmetic]") {
  using Dim64 = templates::Dimension<sm::Datatype::UINT64>;
  using Dom64 = Dim64::domain_type;

  SECTION("Rectangle examples") {
    const uint64_t d1_lower_bound = GENERATE(0, 3);
    const uint64_t d1_extent = GENERATE(1, 4);
    const uint64_t d2_lower_bound = GENERATE(0, 3);
    const uint64_t d2_extent = GENERATE(1, 4);

    const Dim64 d1(
        d1_lower_bound, d1_lower_bound + (5 * d1_extent) - 1, d1_extent);
    const Dim64 d2(
        d2_lower_bound, d2_lower_bound + (4 * d2_extent) - 1, d2_extent);

    auto make_d1 = [&](uint64_t r_start, uint64_t r_end) {
      return Dom64(
          d1_lower_bound + r_start * d1_extent,
          d1_lower_bound + r_end * d1_extent + d1_extent - 1);
    };
    auto make_d2 = [&](uint64_t c_start, uint64_t c_end) {
      return Dom64(
          d2_lower_bound + c_start * d2_extent,
          d2_lower_bound + c_end * d2_extent + d2_extent - 1);
    };

    SECTION("Whole domain") {
      const Layout tile_order = GENERATE(Layout::ROW_MAJOR, Layout::COL_MAJOR);
      const auto r = instance_domain_tile_offset<Dim64::DATATYPE>(
          {d1, d2}, 0, 20, tile_order);
      CHECK(r == std::vector<Dom64>{d1.domain, d2.domain});
    }

    SECTION("Sub-rectangle") {
      const auto r1 =
          instance_domain_tile_offset<Dim64::DATATYPE>({d1, d2}, 4, 8);
      CHECK(r1 == std::vector<Dom64>{make_d1(1, 2), d2.domain});

      const auto r2 =
          instance_domain_tile_offset<Dim64::DATATYPE>({d1, d2}, 8, 4);
      CHECK(r2 == std::vector<Dom64>{make_d1(2, 2), d2.domain});

      const auto r3 =
          instance_domain_tile_offset<Dim64::DATATYPE>({d1, d2}, 8, 12);
      CHECK(r3 == std::vector<Dom64>{make_d1(2, 4), d2.domain});
    }

    SECTION("Line") {
      const auto r1 =
          instance_domain_tile_offset<Dim64::DATATYPE>({d1, d2}, 0, 2);
      CHECK(r1 == std::vector<Dom64>{make_d1(0, 0), make_d2(0, 1)});

      const auto r2 =
          instance_domain_tile_offset<Dim64::DATATYPE>({d1, d2}, 1, 2);
      CHECK(
          r2 == std::vector<Dom64>{
                    make_d1(0, 0),
                    make_d2(1, 2),
                });

      const auto r3 =
          instance_domain_tile_offset<Dim64::DATATYPE>({d1, d2}, 9, 3);
      CHECK(r3 == std::vector<Dom64>{make_d1(2, 2), make_d2(1, 3)});
    }

    SECTION("Align start but not end") {
      const auto r1 =
          instance_domain_tile_offset<Dim64::DATATYPE>({d1, d2}, 0, 5);
      CHECK(r1 == std::optional<std::vector<Dom64>>{});

      const auto r2 =
          instance_domain_tile_offset<Dim64::DATATYPE>({d1, d2}, 4, 11);
      CHECK(r2 == std::optional<std::vector<Dom64>>{});
    }

    SECTION("Cross row") {
      const auto r1 =
          instance_domain_tile_offset<Dim64::DATATYPE>({d1, d2}, 7, 2);
      CHECK(r1 == std::optional<std::vector<Dom64>>{});

      const auto r2 =
          instance_domain_tile_offset<Dim64::DATATYPE>({d1, d2}, 5, 4);
      CHECK(r2 == std::optional<std::vector<Dom64>>{});

      const auto r3 =
          instance_domain_tile_offset<Dim64::DATATYPE>({d1, d2}, 5, 8);
      CHECK(r3 == std::optional<std::vector<Dom64>>{});
    }

    SECTION("Column major") {
      const auto r1 = instance_domain_tile_offset<Dim64::DATATYPE>(
          {d1, d2}, 0, 10, Layout::COL_MAJOR);
      CHECK(r1 == std::vector<Dom64>{d1.domain, make_d2(0, 1)});

      const auto r2 = instance_domain_tile_offset<Dim64::DATATYPE>(
          {d1, d2}, 11, 4, Layout::COL_MAJOR);
      CHECK(r2 == std::vector<Dom64>{make_d1(1, 4), make_d2(2, 2)});

      const auto r3 = instance_domain_tile_offset<Dim64::DATATYPE>(
          {d1, d2}, 11, 5, Layout::COL_MAJOR);
      CHECK(r3 == std::optional<std::vector<Dom64>>{});
    }
  }

  rc::prop("any tiles", []() {
    const Dim64 d1 = *rc::make_dimension<Dim64::DATATYPE>(std::nullopt, {64});
    const Dim64 d2 = *rc::make_dimension<Dim64::DATATYPE>(std::nullopt, {64});
    const Layout tile_order =
        *rc::gen::element(Layout::ROW_MAJOR, Layout::COL_MAJOR);

    instance_domain_tile_offset<Dim64::DATATYPE, AsserterRapidcheck>(
        {d1, d2}, tile_order);
  });
}

TEST_CASE("domain_tile_offset 3d", "[arithmetic]") {
  using Dim64 = templates::Dimension<sm::Datatype::UINT64>;
  using Dom64 = Dim64::domain_type;

  SECTION("Rectangular prism examples") {
    const uint64_t d1_lower_bound = GENERATE(0, 3);
    const uint64_t d1_extent = GENERATE(1, 4);
    const uint64_t d2_lower_bound = GENERATE(0, 3);
    const uint64_t d2_extent = GENERATE(1, 4);
    const uint64_t d3_lower_bound = GENERATE(0, 3);
    const uint64_t d3_extent = GENERATE(1, 4);

    const Dim64 d1(
        d1_lower_bound, d1_lower_bound + (3 * d1_extent) - 1, d1_extent);
    const Dim64 d2(
        d2_lower_bound, d2_lower_bound + (6 * d2_extent) - 1, d2_extent);
    const Dim64 d3(
        d3_lower_bound, d3_lower_bound + (7 * d3_extent) - 1, d3_extent);

    auto make_d1 = [&](uint64_t h_start, uint64_t h_end) {
      return Dom64(
          d1_lower_bound + h_start * d1_extent,
          d1_lower_bound + h_end * d1_extent + d1_extent - 1);
    };
    auto make_d2 = [&](uint64_t w_start, uint64_t w_end) {
      return Dom64(
          d2_lower_bound + w_start * d2_extent,
          d2_lower_bound + w_end * d2_extent + d2_extent - 1);
    };
    auto make_d3 = [&](uint64_t l_start, uint64_t l_end) {
      return Dom64(
          d3_lower_bound + l_start * d3_extent,
          d3_lower_bound + l_end * d3_extent + d3_extent - 1);
    };

    SECTION("Whole domain") {
      const Layout tile_order = GENERATE(Layout::ROW_MAJOR, Layout::COL_MAJOR);
      const auto r = instance_domain_tile_offset<Dim64::DATATYPE>(
          {d1, d2, d3},
          0,
          d1.num_tiles() * d2.num_tiles() * d3.num_tiles(),
          tile_order);
      CHECK(r == std::vector<Dom64>{d1.domain, d2.domain, d3.domain});
    }

    SECTION("Plane") {
      const auto r1 =
          instance_domain_tile_offset<Dim64::DATATYPE>({d1, d2, d3}, 0, 42);
      CHECK(r1 == std::vector<Dom64>{make_d1(0, 0), d2.domain, d3.domain});

      const auto r2 =
          instance_domain_tile_offset<Dim64::DATATYPE>({d1, d2, d3}, 42, 42);
      CHECK(r2 == std::vector<Dom64>{make_d1(1, 1), d2.domain, d3.domain});

      const auto r3 =
          instance_domain_tile_offset<Dim64::DATATYPE>({d1, d2, d3}, 84, 42);
      CHECK(r3 == std::vector<Dom64>{make_d1(2, 2), d2.domain, d3.domain});
    }

    SECTION("Rectangle") {
      const auto r1 =
          instance_domain_tile_offset<Dim64::DATATYPE>({d1, d2, d3}, 0, 14);
      CHECK(r1 == std::vector<Dom64>{make_d1(0, 0), make_d2(0, 1), d3.domain});

      const auto r2 =
          instance_domain_tile_offset<Dim64::DATATYPE>({d1, d2, d3}, 70, 14);
      CHECK(r2 == std::vector<Dom64>{make_d1(1, 1), make_d2(4, 5), d3.domain});
    }

    SECTION("Line") {
      const auto r1 =
          instance_domain_tile_offset<Dim64::DATATYPE>({d1, d2, d3}, 0, 4);
      CHECK(
          r1 ==
          std::vector<Dom64>{make_d1(0, 0), make_d2(0, 0), make_d3(0, 3)});

      const auto r2 =
          instance_domain_tile_offset<Dim64::DATATYPE>({d1, d2, d3}, 8, 2);
      CHECK(
          r2 ==
          std::vector<Dom64>{make_d1(0, 0), make_d2(1, 1), make_d3(1, 2)});

      const auto r3 =
          instance_domain_tile_offset<Dim64::DATATYPE>({d1, d2, d3}, 109, 3);
      CHECK(
          r3 ==
          std::vector<Dom64>{make_d1(2, 2), make_d2(3, 3), make_d3(4, 6)});
    }

    SECTION("Align start but not end") {
      const auto r1 =
          instance_domain_tile_offset<Dim64::DATATYPE>({d1, d2, d3}, 0, 43);
      CHECK(r1 == std::optional<std::vector<Dom64>>{});

      const auto r2 =
          instance_domain_tile_offset<Dim64::DATATYPE>({d1, d2, d3}, 42, 125);
      CHECK(r2 == std::optional<std::vector<Dom64>>{});
    }

    SECTION("Cross row") {
      const auto r1 =
          instance_domain_tile_offset<Dim64::DATATYPE>({d1, d2, d3}, 0, 8);
      CHECK(r1 == std::optional<std::vector<Dom64>>{});

      const auto r2 =
          instance_domain_tile_offset<Dim64::DATATYPE>({d1, d2, d3}, 23, 6);
      CHECK(r2 == std::optional<std::vector<Dom64>>{});
    }

    SECTION("Cross plane") {
      const auto r1 =
          instance_domain_tile_offset<Dim64::DATATYPE>({d1, d2, d3}, 40, 3);
      CHECK(r1 == std::optional<std::vector<Dom64>>{});

      const auto r2 =
          instance_domain_tile_offset<Dim64::DATATYPE>({d1, d2, d3}, 77, 8);
      CHECK(r2 == std::optional<std::vector<Dom64>>{});
    }

    SECTION("Column major") {
      const auto r1 = instance_domain_tile_offset<Dim64::DATATYPE>(
          {d1, d2, d3}, 54, 36, Layout::COL_MAJOR);
      CHECK(r1 == std::vector<Dom64>{d1.domain, d2.domain, make_d3(3, 4)});

      const auto r2 = instance_domain_tile_offset<Dim64::DATATYPE>(
          {d1, d2, d3}, 78, 12, Layout::COL_MAJOR);
      CHECK(r2 == std::vector<Dom64>{d1.domain, make_d2(2, 5), make_d3(4, 4)});
    }
  }

  rc::prop("any tiles", []() {
    const Dim64 d1 =
        *rc::make_dimension<sm::Datatype::UINT64>(std::nullopt, {16});
    const Dim64 d2 =
        *rc::make_dimension<sm::Datatype::UINT64>(std::nullopt, {16});
    const Dim64 d3 =
        *rc::make_dimension<sm::Datatype::UINT64>(std::nullopt, {16});
    const Layout tile_order =
        *rc::gen::element(Layout::ROW_MAJOR, Layout::COL_MAJOR);

    instance_domain_tile_offset<Dim64::DATATYPE, AsserterRapidcheck>(
        {d1, d2, d3}, tile_order);
  });
}
