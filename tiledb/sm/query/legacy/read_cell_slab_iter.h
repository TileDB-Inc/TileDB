/**
 * @file   read_cell_slab_iter.h
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
 * This file defines class ResultCellSlabIter.
 */

#ifndef TILEDB_READ_CELL_SLAB_ITER_H
#define TILEDB_READ_CELL_SLAB_ITER_H

#include "tiledb/common/common.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/query/legacy/cell_slab_iter.h"
#include "tiledb/sm/query/readers/result_cell_slab.h"
#include "tiledb/sm/query/readers/result_coords.h"
#include "tiledb/sm/query/readers/result_space_tile.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/**
 * Given a subarray (containing potentially multiple ranges per dimension),
 * this iterator produces result cell slabs, i.e., cell ranges in the
 * physical tiles that will be used by the dense read algorithm
 * to copy the appropriate results values into the user buffers.
 *
 * It essentially uses a `CellSlabIter` iterator and, for every cell
 * slab (which corresponded to the logical array space), it produces
 * the appropriate cell slab partitions (`ResultCellSlab` objects)
 * that now map to the physical tiles of the fragments.
 *
 * @tparam T The datatype of the subarray domain.
 */
template <class T>
class ReadCellSlabIter {
 public:
  /* ********************************* */
  /*          TYPE DEFINITIONS         */
  /* ********************************* */

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor.
   *
   * @param subarray The subarray the iterator will produce result cell
   *     slabs for.
   * @param result_space_tiles This is an auxiliary structure that helps
   *     constructing the result cell slabs. It contains pre-computed
   *     information that prevents expensive repeated computations.
   * @param result_coords These are definite sparse fragment results,
   *     which will be used to appropriately "break" dense cell slabs
   *     when producing the final result cell slabs.
   * @param result_coords_pos The position in `result_coords` the
   *     iterator will start iterating on.
   */
  ReadCellSlabIter(
      const Subarray* subarray,
      std::map<const T*, ResultSpaceTile<T>>* result_space_tiles,
      std::vector<ResultCoords>* result_coords,
      uint64_t result_coords_pos = 0);

  /** Destructor. */
  ~ReadCellSlabIter() = default;

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /** Initializes the iterator. */
  Status begin();

  /** Returns the current result cell slab. */
  ResultCellSlab result_cell_slab() const;

  /** Checks if the iterator has reached the end. */
  bool end() const {
    return end_;
  }

  /**
   * Returns the current result coordinates position.
   * Useful when multiple iterators are used (e.g., one per tile)
   * and we need to pass the current result coordinates position from
   * one iterator as the starting result coordinates position to the
   * next iterator.
   */
  uint64_t result_coords_pos() const {
    return result_coords_pos_;
  }

  /** Advances the iterator to the next cell slab. */
  void operator++();

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The array domain. */
  const Domain* domain_;

  /** The subarray layout. */
  Layout layout_;

  /** `True` if the iterator has reached its end. */
  bool end_;

  /**
   * Auxiliary cell offsets used for computing cell positions in a tile
   * given cell coordinates.
   */
  std::vector<T> cell_offsets_;

  /** A cell slab iterator. */
  CellSlabIter<T> cell_slab_iter_;

  /**
   * The result cell slabs buffer. Note that this is always
   * sorted on starting position of the result cell slab.
   * The iterator will first serve all slabs in this vector,
   * before proceeding to get the next cell slab from `CellSlabIter`.
   */
  std::vector<ResultCellSlab> result_cell_slabs_;

  /**
   * Position in `result_cell_slabs_` indicating the next
   * result cell slab to be served in this iteration.
   */
  size_t result_cell_slabs_pos_;

  /**
   * The map to the result space tiles, given a pointer to the
   * tile coordinates.
   * */
  std::map<const T*, ResultSpaceTile<T>>* result_space_tiles_;

  /** The result sparse fragment coordinates. */
  std::vector<ResultCoords>* result_coords_;

  /** Current position to be explored in `result_coords_`. */
  size_t result_coords_pos_;

  /**
   * The initial position in the result coordinates the
   * iterator was constructed with. When invoking `begin`,
   * `result_coords_pos_` will be reset to this value.
   */
  size_t init_result_coords_pos_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /**
   * Computes auxiliary cell offsets used in calculating cell positions
   * given cell coordinates.
   */
  void compute_cell_offsets();

  /**
   * Computes auxiliary cell offsets used in calculating cell positions
   * given cell coordinates. This function assumes a column-major layout
   * for the cells in a tile.
   */
  void compute_cell_offsets_col();

  /**
   * Computes auxiliary cell offsets used in calculating cell positions
   * given cell coordinates. This function assumes a row-major layout
   * for the cells in a tile.
   */
  void compute_cell_offsets_row();

  /**
   * Given a cell slab determined by `cell_slab_coords` and `cell_slab_length`,
   * as well as the starting global coordinates of the tile the slab belongs
   * to, it computes the start cell position of the slab in the tile.
   *
   * @param cell_slab_coords The starting coordinates of the cell slab.
   * @param tile_start_coords The global position of the first cell in the
   *     tile the cell slab belongs to.
   * @param start The computed start cell position of the slab.
   */
  void compute_cell_slab_start(
      const T* cell_slab_coords,
      std::span<const T> tile_start_coords,
      uint64_t* start);

  /**
   * Computes the overlap of the input cell slab with the input fragment
   * domain.
   *
   * @param cell_slab The input cell slab.
   * @param frag_domain The input fragment domain.
   * @param slab_overlap The computed starting coordinates of the cell
   *     slab overlap.
   * @param overlap_length The length of the overlap (note that the overlap is
   *     a subset of the input cell slab).
   * @param overlap_type The type of overlap, where `0` means no overlap,
   *     `1` means full overlap, and `2` means partial overlap.
   */
  void compute_cell_slab_overlap(
      const CellSlab<T>& cell_slab,
      const NDRange& frag_domain,
      std::vector<T>* slab_overlap,
      uint64_t* overlap_length,
      unsigned* overlap_type);

  /**
   * Given the input cell slab, it creates the result cell slabs
   * using `result_space_tiles_` and `result_coords` (which may
   * partition the cell slab into potentially multiple result
   * cell slabs).
   *
   * In other words, this function creates result cell slabs based
   * on both sparse and dense fragments in the dense array.
   */
  void compute_result_cell_slabs(const CellSlab<T>& cell_slab);

  /**
   * Given the input cell slab and result space tile,
   * it creates result cell slabs based on dense fragments in the dense array.
   */
  void compute_result_cell_slabs_dense(
      const CellSlab<T>& cell_slab, ResultSpaceTile<T>* result_space_tile);

  /**
   * Given the input result space tile and list of cell slabs to process,
   * it creates result cell slabs that correspond to "empty" cells
   * (i.e., to cells without any overlap with any fragments), and appends
   * them to `result_cell_slabs`.
   */
  void compute_result_cell_slabs_empty(
      const ResultSpaceTile<T>& result_space_tile,
      const std::list<CellSlab<T>>& to_process,
      std::vector<ResultCellSlab>& result_cell_slabs);

  /**
   * Splits the input cell slab into up to two new cell slabs based on the
   * input slab overlap (i.e., it computes the set difference).
   *
   * @param cell_slab The input cell slab.
   * @param slab_overlap The input slab overlap starting coordinates. This
   *     will be used to split `cell_slab`. The result will essentially
   *     be `cell_slab - (slab_overlap, overlap_length)`, which may consist
   *     of one or two cell slabs (left and right of the slab overlap).
   * @param overlap_length The overlap slab length.
   * @param p1 The first new cell slab to be produced from the split.
   * @param p2 The second (optional) new cell slab to be produced form the
   * split.
   * @param two_slabs Indicates whether one or two cell slabs got produced.
   */
  void split_cell_slab(
      const CellSlab<T>& cell_slab,
      const std::vector<T>& slab_overlap,
      uint64_t overlap_length,
      CellSlab<T>* p1,
      CellSlab<T>* p2,
      bool* two_slabs);

  /**
   * Updates the current result cell slab, based on the next cell slab
   * retrieved from `cell_slab_iter_`.
   */
  void update_result_cell_slab();
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_READ_CELL_SLAB_ITER_H
