/**
 * @file   tiledb/sm/tile/arithmetic.h
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
 * This file provides template definitions for doing tile arithmetic,
 * e.g. computing new domains based on offsets and such.
 */
#ifndef TILEDB_TILE_ARITHMETIC_H
#define TILEDB_TILE_ARITHMETIC_H

#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/type/range/range.h"

namespace tiledb::sm {

/**
 * @return true if the range `[start_tile, start_tile + num_tiles)` represents
 * a hyper-rectangle inside `domain` with tile sizes given by `tile_extents`
 */
template <typename T>
static bool is_rectangular_domain(
    Layout tile_order,
    std::span<const T> tile_extents,
    const sm::NDRange& domain,
    uint64_t start_tile,
    uint64_t num_tiles) {
  for (uint64_t d_outer = 0; d_outer < tile_extents.size(); d_outer++) {
    uint64_t hyperrow_num_tiles = 1;
    for (uint64_t d_inner = d_outer + 1; d_inner < tile_extents.size();
         d_inner++) {
      const uint64_t d =
          (tile_order == Layout::ROW_MAJOR ? d_inner :
                                             tile_extents.size() - d_inner - 1);
      const uint64_t d_inner_num_tiles =
          sm::Dimension::tile_idx<T>(
              domain[d].end_as<T>(), domain[d].start_as<T>(), tile_extents[d]) +
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

/**
 * Compute the number of tiles per hyper-row for the given `domain` with tiles
 * given by `tile_extents`.
 *
 * For D dimensions, the returned vector contains `D+1` elements.
 * Position 0 is the number of tiles in `domain`.
 * For dimension `d`, position `d + 1` is the number of tiles in a hyper-row of
 * dimension `d` (and is thus always 1 for the final dimension).
 */
template <typename T>
std::vector<uint64_t> compute_hyperrow_sizes(
    Layout tile_order,
    std::span<const T> tile_extents,
    const sm::NDRange& domain) {
  std::vector<uint64_t> hyperrow_sizes(tile_extents.size() + 1, 1);
  for (uint64_t di = 0; di < tile_extents.size(); di++) {
    const uint64_t d =
        (tile_order == Layout::ROW_MAJOR ? di : tile_extents.size() - di - 1);
    const uint64_t d_num_tiles =
        sm::Dimension::tile_idx<T>(
            domain[d].end_as<T>(), domain[d].start_as<T>(), tile_extents[d]) +
        1;
    hyperrow_sizes[di] = d_num_tiles;
  }
  for (uint64_t d = tile_extents.size(); d > 0; d--) {
    hyperrow_sizes[d - 1] = hyperrow_sizes[d - 1] * hyperrow_sizes[d];
  }

  return hyperrow_sizes;
}

/**
 * @return a new range which is contained the rectangle within `domain` defined
 * by `[start_tile, start_tile + num_tiles)` for the tile sizes given by
 * `tile_extents`. If this does not represent a valid rectangle then
 * `std::nullopt` is returned instead.
 */
template <typename T>
static std::optional<sm::NDRange> domain_tile_offset(
    Layout tile_order,
    std::span<const T> tile_extents,
    const sm::NDRange& domain,
    uint64_t start_tile,
    uint64_t num_tiles) {
  sm::NDRange r;
  r.resize(tile_extents.size());

  const std::vector<uint64_t> dimension_sizes =
      compute_hyperrow_sizes(tile_order, tile_extents, domain);

  for (uint64_t di = 0; di < tile_extents.size(); di++) {
    const uint64_t d =
        (tile_order == Layout::ROW_MAJOR ? di : tile_extents.size() - di - 1);

    const uint64_t outer_num_tiles = dimension_sizes[di];
    const uint64_t hyperrow_num_tiles = dimension_sizes[di + 1];

    const T this_dimension_start_tile = (start_tile / hyperrow_num_tiles) %
                                        (outer_num_tiles / hyperrow_num_tiles);
    const T this_dimension_end_tile =
        ((start_tile + num_tiles - 1) / hyperrow_num_tiles) %
        (outer_num_tiles / hyperrow_num_tiles);

    if (start_tile % hyperrow_num_tiles == 0) {
      // aligned to the start of the hyperrow
      if (num_tiles > hyperrow_num_tiles &&
          num_tiles % hyperrow_num_tiles != 0) {
        return std::nullopt;
      }
    } else {
      // begins in the middle of the hyperrow
      const uint64_t offset = start_tile % hyperrow_num_tiles;
      if (offset + num_tiles > hyperrow_num_tiles) {
        return std::nullopt;
      }
    }

    const T start =
        domain[d].start_as<T>() + (this_dimension_start_tile * tile_extents[d]);
    const T end = domain[d].start_as<T>() +
                  (this_dimension_end_tile * tile_extents[d]) +
                  tile_extents[d] - 1;
    r[d] = Range(
        std::max<T>(domain[d].start_as<T>(), start),
        std::min<T>(domain[d].end_as<T>(), end));
  }

  return r;
}

}  // namespace tiledb::sm

#endif
