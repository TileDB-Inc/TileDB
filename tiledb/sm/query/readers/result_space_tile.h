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
#include "tiledb/sm/query/readers/result_tile.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class MemoryTracker;

/** Fragment domain structure (fragment id, fragment domain). */
struct FragmentDomain {
 public:
  /** Delete default constructor. */
  FragmentDomain() = delete;

  /** Constructor. */
  FragmentDomain(
      unsigned fragment_id, const std::reference_wrapper<const NDRange>& domain)
      : fragment_id_(fragment_id)
      , domain_(domain) {
  }

  inline unsigned fid() const {
    return fragment_id_;
  }

  inline const NDRange& domain() const {
    return domain_;
  }

 private:
  unsigned fragment_id_;
  NDRange domain_;
};

/**
 * Stores information about a space tile covered by a subarray query.
 *
 * @tparam The datatype of the array domain.
 */
template <class T>
class ResultSpaceTile {
 public:
  /** No default constructor. */
  ResultSpaceTile() = delete;

  ResultSpaceTile(shared_ptr<MemoryTracker> memory_tracker)
      : memory_tracker_(memory_tracker) {
  }

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
  const std::vector<FragmentDomain>& frag_domains() const {
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
  void set_result_tile(
      unsigned frag_idx, uint64_t tile_idx, FragmentMetadata& frag_md) {
    assert(result_tiles_.count(frag_idx) == 0);
    result_tiles_.emplace(
        std::piecewise_construct,
        std::forward_as_tuple(frag_idx),
        std::forward_as_tuple(frag_idx, tile_idx, frag_md, memory_tracker_));
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
      if (!(frag_domains_[i].fid() == rst.frag_domains_[i].fid() &&
            frag_domains_[i].domain() == rst.frag_domains_[i].domain()))
        return false;
    }

    return start_coords_ == rst.start_coords_ &&
           result_tiles_ == rst.result_tiles_;
  }

  /** The query condition filtered a result for this tile. */
  inline void set_qc_filtered_results() {
    qc_filtered_results_ = true;
  }

  /**
   * Returns if the query condition filtered any results for this tile or not.
   */
  inline bool qc_filtered_results() const {
    return qc_filtered_results_;
  }

  /**
   * Returns the only result tile in this space tile or throws if there are more
   * than one.
   */
  inline ResultTile& single_result_tile() {
    if (result_tiles_.size() != 1) {
      throw std::runtime_error(
          "Shouldn't call single_result_tile on tiles with more than one "
          "fragment domain.");
    }

    auto iter = result_tiles_.find(frag_domains_[0].fid());
    if (iter == result_tiles_.end()) {
      throw std::runtime_error(
          "Invalid call to single_result_tile with unknown tile.");
    }

    return iter->second;
  }

 private:
  /** The memory tracker to use. */
  shared_ptr<MemoryTracker> memory_tracker_;

  /** The (global) coordinates of the first cell in the space tile. */
  std::vector<T> start_coords_;

  /**
   * A vector of fragment domains, sorted on fragment id in descending order.
   * Note that only fragments with domains that intersect this space tile will
   * be included in this vector.
   */
  std::vector<FragmentDomain> frag_domains_;

  /**
   * The (dense) result tiles for this space tile, as a map
   * `(fragment id) -> (result tile)`.
   */
  std::map<unsigned, ResultTile> result_tiles_;

  /** Did the query condition filter any result for this space tile. */
  bool qc_filtered_results_ = false;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_RESULT_SPACE_TILE_H
