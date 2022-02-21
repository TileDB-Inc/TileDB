/**
 * @file   result_space_tile.h
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
 * This file defines struct ResultSpaceTile.
 */

#ifndef TILEDB_RESULT_SPACE_TILE_H
#define TILEDB_RESULT_SPACE_TILE_H

#include <cassert>
#include <functional>
#include <iostream>
#include <map>
#include <vector>

#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/query/result_tile.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/**
 * Stores information about a space tile covered by a subarray query.
 *
 * @tparam The datatype of the array domain.
 */
template <class T>
class ResultSpaceTile {
 public:
  /** Default constructor. */
  ResultSpaceTile() = default;

  /** Default destructor. */
  ~ResultSpaceTile() = default;

  /** Default copy constructor. */
  ResultSpaceTile(const ResultSpaceTile&) = default;

  /** Default move constructor. */
  ResultSpaceTile(ResultSpaceTile&&) = default;

  /** Default copy-assign operator. */
  ResultSpaceTile& operator=(const ResultSpaceTile&) = default;

  /** Default move-assign operator. */
  ResultSpaceTile& operator=(ResultSpaceTile&&) = default;

  /** Returns the fragment domains. */
  const std::vector<std::pair<unsigned, NDRange>>& frag_domains() const {
    return frag_domains_;
  }

  /** Returns the result tiles. */
  const std::map<unsigned, ResultTile>& result_tiles() const {
    return result_tiles_;
  }

  /** Returns the start coordinates. */
  const std::vector<T>& start_coords() const {
    return start_coords_;
  }

  /** Sets the start coordinates. */
  void set_start_coords(const std::vector<T>& start_coords) {
    start_coords_ = start_coords;
  }

  /** Appends a fragment domain. */
  void append_frag_domain(
      unsigned frag_idx, const std::reference_wrapper<const NDRange>& dom) {
    frag_domains_.emplace_back(frag_idx, dom);
  }

  /** Sets the input result tile for the given fragment. */
  void set_result_tile(unsigned frag_idx, ResultTile& result_tile) {
    assert(result_tiles_.count(frag_idx) == 0);
    result_tiles_[frag_idx] = std::move(result_tile);
  }

  /** Returns the result tile for the input fragment. */
  ResultTile* result_tile(unsigned frag_idx) {
    auto it = result_tiles_.find(frag_idx);
    assert(it != result_tiles_.end());
    return &(it->second);
  }

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

 private:
  /** The (global) coordinates of the first cell in the space tile. */
  std::vector<T> start_coords_;

  /**
   * A vector of pairs `(fragment id, fragment domain)`, sorted on
   * fragment id in descending order. Note that only fragments
   * with domains that intersect this space tile will be included
   * in this vector.
   */
  std::vector<std::pair<unsigned, NDRange>> frag_domains_;

  /**
   * The (dense) result tiles for this space tile, as a map
   * `(fragment id) -> (result tile)`.
   */
  std::map<unsigned, ResultTile> result_tiles_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_RESULT_SPACE_TILE_H
