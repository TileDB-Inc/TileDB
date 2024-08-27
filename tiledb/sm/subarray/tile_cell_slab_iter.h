/**
 * @file   tile_cell_slab_iter.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * This file defines class TileCellSlabIter.
 */

#ifndef TILEDB_TILE_CELL_SLAB_ITER_H
#define TILEDB_TILE_CELL_SLAB_ITER_H

#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/subarray/subarray.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/**
 * Stores range information, for a dimension, used for row/col reads. This is
 * used to cache information used to compute where data will be copied in the
 * output subarray.
 */
template <class DimType>
struct RangeInfo {
  /** Cell offset, per range for this dimension. */
  std::vector<uint64_t> cell_offsets_;

  /** Min, per range for this dimension. */
  std::vector<DimType> mins_;

  /**
   * Multiplier that contains the product of the total range length for
   * previous dimensions.
   */
  uint64_t multiplier_;
};

template <class T>
class TileCellSlabIter {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  explicit TileCellSlabIter(
      const uint64_t range_thread_idx,
      const uint64_t num_range_threads,
      const Subarray& root_subarray,
      const DenseTileSubarray<T>& subarray,
      const std::vector<T>& tile_extents,
      const std::vector<T>& start_coords,
      const std::vector<RangeInfo<T>>& range_info,
      const Layout cell_order);

  /** Destructor. */
  ~TileCellSlabIter() = default;

  DISABLE_COPY_AND_COPY_ASSIGN(TileCellSlabIter);
  DISABLE_MOVE_AND_MOVE_ASSIGN(TileCellSlabIter);

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /** Returns the current cell slab coords. */
  inline const std::vector<T>& cell_slab_coords() const {
    return cell_slab_coords_;
  }

  /** Returns the input cell position in the tile for the current cell slab. */
  inline uint64_t pos_in_tile() const {
    return pos_in_tile_;
  }

  /**
   * Returns the destination cell position for the current cell slab for
   * row/col major reads.
   */
  inline uint64_t dest_offset_row_col() const {
    return dest_offset_row_col_;
  }

  /** Returns the global offset. */
  inline uint64_t global_offset() const {
    return global_offset_;
  };

  /** Returns the current cell slab length. */
  inline uint64_t cell_slab_length() const {
    return cell_slab_length_;
  }

  /** Checks if the iterator has reached the end. */
  inline bool end() const {
    return end_ || num_ == 0;
  }

  /** Checks if the iterator has reached the last slab. */
  inline bool last_slab() const {
    return last_ && end();
  }

  /** Advances the iterator to the next cell slab. */
  void operator++();

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** Number of ranges. */
  uint64_t num_ranges_;

  /** Original range indexes. */
  const tdb::pmr::vector<tdb::pmr::vector<uint64_t>>& original_range_idx_;

  /** Range info. */
  const std::vector<RangeInfo<T>>& range_info_;

  /** The current cell slab length. */
  uint64_t cell_slab_length_;

  /** The layout of the tile. */
  Layout layout_;

  /** Number of dimensions. */
  int dim_num_;

  /** Global offset. */
  uint64_t global_offset_;

  /** Current cell position in tile. */
  uint64_t pos_in_tile_;

  /** Current destination cell offset for row/col orders. */
  uint64_t dest_offset_row_col_;

  /** Number of slabs left. */
  uint64_t num_;

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

  /** `True` if the iterator is the last of the range threads. */
  bool last_;

  /** `True` if the request is in global order. */
  bool global_order_;

  /**
   * A list of ranges per dimension. This is derived from the `subarray_`
   * ranges, after appropriately splitting them so that no range crosses
   * more that one tile.
   */
  const tdb::pmr::vector<
      tdb::pmr::vector<typename DenseTileSubarray<T>::DenseTileRange>>& ranges_;

  /** Saved multiplication of tile extents in cell order. */
  std::vector<uint64_t> mult_extents_;

  /** start coordinates of the tile. */
  const std::vector<T>& start_coords_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /** Advances to the next cell slab when the layout is col-major. */
  void advance_col();

  /** Advances to the next cell slab when the layout is row-major. */
  void advance_row();

  /**
   * Initializes the cell slab length for each range along the minor
   * dimension.
   */
  void init_cell_slab_lengths();

  /** Initializes the range coords and the cell slab coords. */
  void init_coords();

  /**
   * Updates the current cell slab, based on the current state of
   * the iterator.
   */
  void update_cell_slab();
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_TILE_CELL_SLAB_ITER_H
