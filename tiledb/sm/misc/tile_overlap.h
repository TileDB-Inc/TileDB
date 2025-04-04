/**
 * @file   tile_overlap.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 * This file defines struct TileOverlap.
 */

#ifndef TILEDB_TILE_OVERLAP_H
#define TILEDB_TILE_OVERLAP_H

#include <cinttypes>
#include <vector>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/**
 * A simple struct that stores tile overlap info from a single
 * fragment with a single range within a subarray.
 */
struct TileOverlap {
  /**
   * A vector of pairs ``(overlapping tile id, ratio).``
   * The ``ratio`` is defined as the ratio of the volume of the overlap over
   * the total tile volume. This is a number in [0.0, 1.0]. An overlap of 1.0
   * indicates full overlap and 0.0 no overlap at all.
   *
   * The ratio is used for result size estimation.
   */
  std::vector<std::pair<uint64_t, double>> tiles_;

  /** Ranges of tile ids that lie completely inside the subarray range. */
  std::vector<std::pair<uint64_t, uint64_t>> tile_ranges_;

  /** Returns the current byte size of an instance. */
  size_t byte_size() const {
    static const uint64_t tiles_element_size =
        sizeof(std::pair<uint64_t, double>);
    static const uint64_t tile_ranges_element_size =
        sizeof(std::pair<uint64_t, uint64_t>);
    return sizeof(TileOverlap) + (tiles_.size() * tiles_element_size) +
           (tile_ranges_.size() * tile_ranges_element_size);
  }
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_TILE_OVERLAP_H
