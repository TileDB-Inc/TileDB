/**
 * @file   cell_slab_iter.h
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
 * This file defines class CellSlabIter.
 */

#ifndef TILEDB_CELL_SLAB_ITER_H
#define TILEDB_CELL_SLAB_ITER_H

#include "tiledb/sm/subarray/cell_slab.h"
#include "tiledb/sm/subarray/subarray.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/**
 * Given a subarray (containing potentially multiple ranges per dimension),
 * this iterator produces cell slabs, i.e., cell ranges in the logical
 * coordinate space that do not cross tiles (i.e., each cell slab is
 * contained in a single tile). The iterator produces the cell slabs
 * respecting the subarray layout (row- or col-major are the only
 * layouts supported - the other layouts are not needed to realize
 * the read algorithm using this iterator).
 *
 * A cell slab is identified by the starting global coordinates,
 * the tile coordinates (in the tile global tile domain) and the
 * length. This information suffices for the read algorithm to carry
 * out the dense multi-range subarray reads.
 *
 * **1D example:**
 *
 * Suppose the array domain is [1,100] and the tile extent is 10.
 * If a subarray contains two ranges {[2, 5], [6, 26]}, the iterator
 * will produce [2, 5] from the first range (it is included in tile
 * [1, 10]) and {[6, 10], [11, 20], [21, 16]} from the second range,
 * since it has to be split in the boundaries of tile [1, 10] and
 * [11, 20].
 *
 * **2D example:**
 *
 * Suppose the array domain is {[1, 4], [1, 4]} and the tile extent
 * is 2 in both dimensions. Let the subarray contain ranges
 * {[2, 3]} on the first dimension and {[1, 2], [1, 4]} in the
 * second dimension (multiplicities are supported).
 *
 * The iterator will break the ranges of the first dimension
 * to {[2, 2], [3, 3]} and those of the second to
 * {[1, 2], [1, 2], [3, 4]} (notice the multiplicities - this is how
 * it should be done). Then the iterator iterates over the 2D
 * "range domain" properly producing the cell slabs in the
 * subarray layout.
 *
 * For *row-major*, the result cell slabs are the following in the
 * form <tile coords, cell coords, length>:
 * - (0, 0), (2, 1), 2
 * - (0, 0), (2, 1), 2
 * - (0, 1), (2, 3), 2
 * - (1, 0), (3, 1), 2
 * - (1, 0), (3, 1), 2
 * - (1, 1), (3, 3), 2
 *
 * For *col-major*, the result cell slabs are the following in the
 * form <tile coords, cell coords, length>:
 * - (0, 0), (2, 1), 1
 * - (1, 0), (3, 1), 1
 * - (0, 0), (2, 2), 1
 * - (1, 0), (3, 2), 1
 * - (0, 0), (2, 1), 1
 * - (1, 0), (3, 1), 1
 * - (0, 0), (2, 2), 1
 * - (1, 0), (3, 2), 1
 * - (0, 1), (2, 3), 1
 * - (1, 1), (3, 3), 1
 * - (0, 1), (2, 4), 1
 * - (1, 1), (3, 4), 1
 *
 * @tparam T The datatype of the subarray domain.
 */
template <class T>
class CellSlabIter {
 public:
  /* ********************************* */
  /*          TYPE DEFINITIONS         */
  /* ********************************* */

  /**
   * Stores information about a range along a single dimension. The
   * whole range resides in a single tile.
   */
  struct Range {
    /** The start of the range in global coordinates. */
    T start_;
    /** The end of the range in global coordinates. */
    T end_;
    /** The global coordinate of the tile the range resides in. */
    T tile_coord_;

    /** Constructor. */
    Range(T start, T end, T tile_coord)
        : start_(start)
        , end_(end)
        , tile_coord_(tile_coord) {
    }

    /** Equality operator. */
    bool operator==(const Range& r) const {
      return (
          r.tile_coord_ == tile_coord_ && r.start_ == start_ && r.end_ == end_);
    }
  };

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  CellSlabIter();

  /** Constructor taking a subarray as input. */
  explicit CellSlabIter(const Subarray* subarray);

  /** Destructor. */
  ~CellSlabIter() = default;

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /** Initializes the iterator. */
  Status begin();

  /** Returns the current cell slab. */
  CellSlab<T> cell_slab() const;

  /** Checks if the iterator has reached the end. */
  bool end() const;

  /** Advances the iterator to the next cell slab. */
  void operator++();

  /**
   * Returns the ranges per dimension used to produce the
   * cell slabs. Mainly for debugging purposes.
   */
  const std::vector<std::vector<Range>>& ranges() const {
    return ranges_;
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The current cell slab. */
  CellSlab<T> cell_slab_;

  /**
   * The coordinates of the current range that the next
   * cell slab will be retrieved from.
   */
  std::vector<T> range_coords_;

  /** The starting (global) coordinates of the current cell slab. */
  std::vector<T> cell_slab_coords_;

  /** The length of a cell slab, one per range along the minor dimension. */
  std::vector<uint64_t> cell_slab_lengths_;

  /** `True` if the iterator has reached its end. */
  bool end_;

  /**
   * A list of ranges per dimension. This is derived from the `subarray_`
   * ranges, after appropriately splitting them so that no range crosses
   * more that one tile.
   */
  std::vector<std::vector<Range>> ranges_;

  /** The subarray the cell slab iterator will work on. */
  const Subarray* subarray_;

  /** Auxiliary tile coordinates to avoid repeated allocations. */
  std::vector<T> aux_tile_coords_;

  /** Auxiliary tile coordinates to avoid repeated allocations. */
  std::vector<uint8_t> aux_tile_coords_2_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /** Advances to the next cell slab when the layout is col-major. */
  void advance_col();

  /** Advances to the next cell slab when the layout is row-major. */
  void advance_row();

  /**
   * Given an input 1D range (corresponding to a single dimension),
   * it potentially splits it into ranges at the tile boundaries, and
   * adds them at the end of `ranges`. For each added range, the function
   * also calculates the global coordinate of the tile the range falls into.
   *
   * @param range The input range.
   * @param tile_extent The tile extent along the dimension.
   * @param dim_domain_start The start of the global domain of the dimension.
   * @param ranges The results will be added at the back of this vector.
   */
  void create_ranges(
      const T* range,
      T tile_extent,
      T dim_domain_start,
      std::vector<Range>* ranges);

  /**
   * Initializes the cell slab length for each range along the minor
   * dimension.
   */
  void init_cell_slab_lengths();

  /** Initializes the range coords and the cell slab coords. */
  void init_coords();

  /**
   * Initializes the ranges per dimension, splitting appropriately the
   * subarray ranges on tile boundaries. Eventually the produced `ranges_`
   * should not never cross more than one tile.
   */
  Status init_ranges();

  /** Performs sanity checks. */
  Status sanity_check() const;

  /**
   * Updates the current cell slab, based on the current state of
   * the iterator.
   */
  void update_cell_slab();
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_CELL_SLAB_ITER_H
