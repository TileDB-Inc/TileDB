/**
 * @file   dense_cell_range_iter.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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
 * This file defines class DenseCellRangeIter.
 */

#ifndef TILEDB_DENSE_CELL_RANGE_ITER_H
#define TILEDB_DENSE_CELL_RANGE_ITER_H

#include "tiledb/sm/array_schema/domain.h"

namespace tiledb {
namespace sm {

/**
 * Iterator over cell ranges inside a particular subarray over a domain.
 * The iterator takes into account the layout of the cells in the global
 * order, as well as the query layout. It serves the next range of
 * contiguous cells (along the global order) that can satisfy the query
 * layout in the query subarray.
 *
 * @tparam T The domain type.
 */
template <class T>
class DenseCellRangeIter {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  DenseCellRangeIter();

  /**
   * Constructor.
   *
   * @param domain The array domain.
   * @param subarray The subarray the iterator will focuse on.
   * @param layout The layout in which the cell ranges will be iterated on.
   */
  DenseCellRangeIter(
      const Domain* domain, const std::vector<T>& subarray, Layout layout);

  /** Destructor. */
  ~DenseCellRangeIter() = default;

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /** Initializes the iterator, computing the very first cell range. */
  Status begin();

  /** Returns the coordinates of the start of the range. */
  const T* coords_start() const;

  /** Returns the coordinates of the end of the range. */
  const T* coords_end() const;

  /** Checks if the iterator has reached the end. */
  bool end() const;

  /**
   * Returns the tile index of the current range. The tile index is in the
   * global order of the domain.
   */
  uint64_t tile_idx() const;

  /**
   * Returns the start position of the current range. This is the
   * position of the start cell of the range **within** the current tile.
   */
  uint64_t range_start() const;

  /**
   * Returns the end position of the current range. This is the position
   * position of the ending cell of the range **within** the current tile.
   */
  uint64_t range_end() const;

  /** Returns the current tile coordinates. */
  const T* tile_coords() const;

  /** Advances the iterator to the next range. */
  void operator++();

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** `True` if the iterator has reached its end. */
  bool end_;

  /** The array domain. */
  const Domain* domain_;

  /** The query subarray. */
  std::vector<T> subarray_;

  /** The intersection between `subarray_` and the current tile. */
  std::vector<T> subarray_in_tile_;

  /** The current global tile position. */
  uint64_t tile_idx_;

  /** The tile domain of `subarray_`. */
  std::vector<T> tile_domain_;

  /** The subarray oriented by the current tile. */
  std::vector<T> tile_subarray_;

  /** `true` if `subarray_` overlaps with the current tile. */
  bool tile_overlap_;

  /** Current tile coords in the global tile domain. */
  std::vector<T> tile_coords_;

  /** Current tile coords in the subarray tile domain. */
  std::vector<T> tile_coords_in_subarray_;

  /** The start coordinates of the range. */
  std::vector<T> coords_start_;

  /** The end coordinates of the range. */
  std::vector<T> coords_end_;

  /** The start position of the range. */
  uint64_t range_start_;

  /** The end position of the range. */
  uint64_t range_end_;

  /** The query layout. */
  Layout layout_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /**
   * Based on the current start coords, it computes the current tile
   * info (e.g., coordinates, index/position, etc.).
   */
  void compute_current_tile_info();

  /** Computes the current start/end range positions. */
  Status compute_current_range();

  /** Computes the end coordinates based on the current start coordinates. */
  void compute_current_end_coords();

  /**
   * Computes the next start coordinates.
   *
   * @param in_subarray This will be set to `true` if the next coordinates
   *     are inside `subarray_`, and `false` otherwise.
   */
  void compute_next_start_coords(bool* in_subarray);

  /**
   * Computes the next start coordinates specifically when the query layout
   * is the global order layout.
   *
   * @param in_subarray This will be set to `true` if the next coordinates
   *     are inside `subarray_`, and `false` otherwise.
   */
  void compute_next_start_coords_global(bool* in_subarray);

  /** Sanity check on the private attributes of the iterator. */
  Status sanity_check() const;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_DENSE_CELL_RANGE_ITER_H
