#include <test/support/assert_helpers.h>
#include <test/support/tdb_catch.h>
#include "test/support/rapidcheck/array_schema_templates.h"
#include "test/support/src/array_schema_templates.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/type/range/range.h"

#include <span>

using namespace tiledb;
using namespace tiledb::test;

template <typename T>
static bool is_rectangular_domain(
    std::span<const T> tile_extents,
    const sm::NDRange& domain,
    uint64_t start_tile,
    uint64_t num_tiles) {
  for (uint64_t d_outer = 0; d_outer < tile_extents.size(); d_outer++) {
    uint64_t hyperrow_num_tiles = 1;
    for (uint64_t d_inner = d_outer + 1; d_inner < tile_extents.size();
         d_inner++) {
      const uint64_t d_inner_num_tiles = sm::Dimension::tile_idx<T>(
                                             domain[d_inner].end_as<T>(),
                                             domain[d_inner].start_as<T>(),
                                             tile_extents[d_inner]) +
                                         1;
      hyperrow_num_tiles *= d_inner_num_tiles;
    }

    const uint64_t hyperrow_offset = start_tile % hyperrow_num_tiles;
    if (hyperrow_offset + num_tiles > hyperrow_num_tiles) {
      if (hyperrow_offset != 0) {
        return false;
      } else if (num_tiles % hyperrow_num_tiles != 0) {
        return false;
      }
    }
  }
  return true;
}

template <typename T>
static bool is_rectangular_domain(
    std::span<const T> tile_extents,
    T lower_bound,
    T upper_bound,
    uint64_t start_tile,
    uint64_t num_tiles) {
  sm::NDRange r;
  r.push_back(Range(lower_bound, upper_bound));
  return is_rectangular_domain(tile_extents, r, start_tile, num_tiles);
}

template <typename T>
static bool is_rectangular_domain(
    std::span<const T> tile_extents,
    T d1_lower_bound,
    T d1_upper_bound,
    T d2_lower_bound,
    T d2_upper_bound,
    uint64_t start_tile,
    uint64_t num_tiles) {
  sm::NDRange r;
  r.push_back(Range(d1_lower_bound, d1_upper_bound));
  r.push_back(Range(d2_lower_bound, d2_upper_bound));
  return is_rectangular_domain(tile_extents, r, start_tile, num_tiles);
}

template <sm::Datatype DT>
static bool is_rectangular_domain(
    const templates::Dimension<DT>& d1,
    const templates::Dimension<DT>& d2,
    uint64_t start_tile,
    uint64_t num_tiles) {
  using Coord = templates::Dimension<DT>::value_type;
  const std::vector<Coord> extents = {d1.extent, d2.extent};
  return is_rectangular_domain<Coord>(
      extents,
      d1.domain.lower_bound,
      d1.domain.upper_bound,
      d2.domain.lower_bound,
      d2.domain.upper_bound,
      start_tile,
      num_tiles);
}

template <sm::Datatype DT>
static bool is_rectangular_domain(
    const templates::Dimension<DT>& d1,
    const templates::Dimension<DT>& d2,
    const templates::Dimension<DT>& d3,
    uint64_t start_tile,
    uint64_t num_tiles) {
  using Coord = templates::Dimension<DT>::value_type;
  const std::vector<Coord> extents = {d1.extent, d2.extent, d3.extent};
  sm::NDRange r;
  r.push_back(Range(d1.domain.lower_bound, d1.domain.upper_bound));
  r.push_back(Range(d2.domain.lower_bound, d2.domain.upper_bound));
  r.push_back(Range(d3.domain.lower_bound, d3.domain.upper_bound));
  return is_rectangular_domain<Coord>(extents, r, start_tile, num_tiles);
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
