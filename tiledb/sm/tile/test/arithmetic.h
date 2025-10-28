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
 * This file provides template definitions for functions which are
 * used to test tile arithmetic.
 */
#ifndef TILEDB_TILE_TEST_ARITHMETIC_H
#define TILEDB_TILE_TEST_ARITHMETIC_H

#include "tiledb/sm/tile/arithmetic.h"

namespace tiledb::test {

/**
 * @return the number of tiles in `subrectangle` based on the tile sizes in
 * `tile_extents`
 */
template <typename T>
uint64_t compute_num_tiles(
    std::span<const T> tile_extents, const sm::NDRange& subrectangle) {
  uint64_t num_tiles_result = 1;
  for (uint64_t d = 0; d < tile_extents.size(); d++) {
    const uint64_t num_tiles_this_dimension = sm::Dimension::tile_idx<T>(
                                                  subrectangle[d].end_as<T>(),
                                                  subrectangle[d].start_as<T>(),
                                                  tile_extents[d]) +
                                              1;
    num_tiles_result *= num_tiles_this_dimension;
  }

  return num_tiles_result;
}

/**
 * @return the tile offset of `subrectangle` within `domain` based on the tile
 * sizes in `tile_extents`
 */
template <typename T>
uint64_t compute_start_tile(
    sm::Layout tile_order,
    std::span<const T> tile_extents,
    const sm::NDRange& domain,
    const sm::NDRange& subrectangle) {
  const std::vector<uint64_t> hyperrow_sizes =
      sm::compute_hyperrow_sizes(tile_order, tile_extents, domain);

  uint64_t start_tile_result = 0;
  for (uint64_t di = 0; di < tile_extents.size(); di++) {
    const uint64_t d =
        (tile_order == sm::Layout::ROW_MAJOR ? di :
                                               tile_extents.size() - di - 1);
    const uint64_t start_tile_this_dimension = sm::Dimension::tile_idx<T>(
        subrectangle[d].start_as<T>(),
        domain[d].start_as<T>(),
        tile_extents[d]);
    start_tile_result += start_tile_this_dimension * hyperrow_sizes[di + 1];
  }

  return start_tile_result;
}

}  // namespace tiledb::test

#endif
