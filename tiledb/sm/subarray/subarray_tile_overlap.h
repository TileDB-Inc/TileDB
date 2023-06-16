/**
 * @file   subarray_tile_overlap.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB, Inc.
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
 * This file defines the SubarrayTileOverlap class.
 *
 * The purpose of this class is to:
 *   1. Provide abstract views of shared `TileOverlap` instances between
 *      instances of `Subarray`.
 *   2. Store partitioning indices that the `SubarrayPartitioner` can
 *      use to determine the ranges that `TileOverlap` instances have
 *      been computed for.
 *
 * Copies of this class share the same `TileOverlap` instances but have
 * their own individual offsets that provide a logical view of the `TileOverlap`
 * instances for their individual ranges.
 *
 * The range indices are unused by this class, but can be used by the caller
 * (e.g. a `SubarrayPartitioner`) to determine which ranges in a `Subarray`
 * the `TileOverlap` instances correspond to.
 */

#ifndef TILEDB_SUBARRAY_TILE_OVERLAP_H
#define TILEDB_SUBARRAY_TILE_OVERLAP_H

#include "tiledb/common/common.h"
#include "tiledb/common/heap_memory.h"
#include "tiledb/common/macros.h"
#include "tiledb/sm/misc/tile_overlap.h"

#include <iostream>

namespace tiledb {
namespace sm {

class SubarrayTileOverlap final {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Default constructor. */
  SubarrayTileOverlap();

  /** Copy constructor. */
  SubarrayTileOverlap(const SubarrayTileOverlap& rhs);

  /** Move constructor. */
  SubarrayTileOverlap(SubarrayTileOverlap&& rhs);

  /**
   * Value constructor.
   *
   * @param fragment_num The number of relevant fragments.
   * @param range_idx_start The inclusive starting range index.
   * @param range_idx_end The inclusive ending range index.
   */
  SubarrayTileOverlap(
      uint64_t fragment_num, uint64_t range_idx_start, uint64_t range_idx_end);

  /** Destructor. */
  ~SubarrayTileOverlap();

  /* ********************************* */
  /*            OPERATORS              */
  /* ********************************* */

  /** Copy-assignment operator. */
  SubarrayTileOverlap& operator=(const SubarrayTileOverlap&);

  /** Move-assignment operator. */
  SubarrayTileOverlap& operator=(SubarrayTileOverlap&&);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  std::string to_string();

  /**
   * Returns a const-pointer to the internal `TileOverlap` instance. The
   * caller is responsible for ensuring that a `TileOverlap` instance has
   * been allocated for the input indexes.
   *
   * @param fragment_idx The fragment index.
   * @param range_idx The range index, relative to `range_idx_start()`.
   * @return The internal `TileOverlap` instance.
   */
  inline const TileOverlap* at(
      uint64_t fragment_idx, uint64_t range_idx) const {
    const uint64_t translated_range_idx = range_idx_start_offset_ + range_idx;
    return &(*tile_overlap_idx_)[fragment_idx][translated_range_idx];
  }

  /**
   * Returns a mutable-pointer to the internal `TileOverlap` instance. The
   * caller is responsible for ensuring that a `TileOverlap` instance has
   * been allocated for the input indexes.
   *
   * @param fragment_idx The fragment index.
   * @param range_idx The range index, relative to `range_idx_start()`.
   * @return The internal `TileOverlap` instance.
   */
  inline TileOverlap* at(uint64_t fragment_idx, uint64_t range_idx) {
    const uint64_t translated_range_idx = range_idx_start_offset_ + range_idx;
    return &(*tile_overlap_idx_)[fragment_idx][translated_range_idx];
  }

  /** The logical inclusive start range index in the subarray. */
  uint64_t range_idx_start() const;

  /** The logical inclusive end range index in the subarray. */
  uint64_t range_idx_end() const;

  /** The number of ranges in each fragment. */
  uint64_t range_num() const;

  /**
   * Returns true if ranges have been allocated between the given
   * input range.
   *
   * @param range_idx_start The inclusive starting range index.
   * @param range_idx_end The inclusive ending range index.
   * @return bool
   */
  bool contains_range(uint64_t range_idx_start, uint64_t range_idx_end) const;

  /**
   * Updates the logical range for this instance. The caller is responsible
   * for ensuring that this instance contains this range. After calling
   * this routine, the indexes in the `at()` routines will be relative to
   * the `range_idx_start` parameter.
   *
   * @param range_idx_start The inclusive starting range index.
   * @param range_idx_end The inclusive ending range index.
   */
  void update_range(uint64_t range_idx_start, uint64_t range_idx_end);

  /**
   * Expands the tile overlap by the end index, allocating empty
   * TileOverlap objects for all the new ranges.
   *
   * @param range_idx_end The inclusive ending range index.
   */
  void expand(uint64_t range_idx_end);

  /**
   * Resets all state.
   */
  void clear();

  /**
   * Returns the total byte size of all stored `TileOverlap` instances.
   */
  uint64_t byte_size() const;

 private:
  /* ********************************* */
  /*      PRIVATE DATA STRUCTURES      */
  /* ********************************* */

  /** Indexes instances of TileOverlap by a fragment-idx and range-idx. */
  typedef std::vector<std::vector<TileOverlap>> TileOverlapIndex;

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /**
   * The indexed TileOverlap instances. This is a shared pointer because
   * it is shared between copies.
   */
  shared_ptr<TileOverlapIndex> tile_overlap_idx_;

  /** The real inclusive start range index. */
  uint64_t range_idx_start_;

  /** The real inclusive end range index. */
  uint64_t range_idx_end_;

  /** Positive-offset from `range_idx_start_` to the logical inclusive start
   * range index. */
  uint64_t range_idx_start_offset_;

  /** Negative-offset from `range_idx_end_` to the logical inclusive start range
   * index. */
  uint64_t range_idx_end_offset_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_SUBARRAY_TILE_OVERLAP_H
