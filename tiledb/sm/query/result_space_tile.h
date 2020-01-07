/**
 * @file   result_space_tile.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB, Inc.
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
 * This file defines struct ResultSpaceTile.
 */

#ifndef TILEDB_RESULT_SPACE_TILE_H
#define TILEDB_RESULT_SPACE_TILE_H

#include <iostream>
#include <map>
#include <vector>

#include "tiledb/sm/query/result_tile.h"

namespace tiledb {
namespace sm {

/**
 * Stores information about a space tile covered by a subarray query.
 *
 * @tparam The datatype of the array domain.
 */
template <class T>
struct ResultSpaceTile {
  /** The (global) coordinates of the first cell in the space tile. */
  std::vector<T> start_coords_;

  /**
   * A vector of pairs `(fragment id, fragment domain)`, sorted on
   * fragment id in descending order. Note that only fragments
   * with domains that intersect this space tile will be included
   * in this vector.
   */
  std::vector<std::pair<unsigned, const T*>> frag_domains_;

  /**
   * The (dense) result tiles for this space tile, as a map
   * `(fragment id) -> (result tile)`.
   */
  std::map<unsigned, ResultTile> result_tiles_;

  /** Default constructor. */
  ResultSpaceTile() = default;

  /** Default destructor. */
  ~ResultSpaceTile() = default;

  /** Default copy constructor. */
  ResultSpaceTile(const ResultSpaceTile& result_space_tile) = default;

  /** Default move constructor. */
  ResultSpaceTile(ResultSpaceTile&& result_space_tile) = default;

  /** Default copy-assign operator. */
  ResultSpaceTile& operator=(const ResultSpaceTile& result_space_tile) =
      default;

  /** Default move-assign operator. */
  ResultSpaceTile& operator=(ResultSpaceTile&& result_space_tile) = default;

  /** Equality operator (mainly for debugging purposes). */
  bool operator==(const ResultSpaceTile& rst) const {
    if (frag_domains_.size() != rst.frag_domains_.size())
      return false;
    for (size_t i = 0; i < frag_domains_.size(); ++i) {
      if (!(frag_domains_[i].first == rst.frag_domains_[i].first &&
            frag_domains_[i].second == rst.frag_domains_[i].second))
        return false;
    }

    return start_coords_ == rst.start_coords_ &&
           result_tiles_ == rst.result_tiles_;
  }
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_RESULT_SPACE_TILE_H
