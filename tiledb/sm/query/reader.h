/**
 * @file   reader.h
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
 * This file defines class Reader.
 */

#ifndef TILEDB_READER_H
#define TILEDB_READER_H

#include "tiledb/common/status.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/tile_domain.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/query/iquery_strategy.h"
#include "tiledb/sm/query/query_buffer.h"
#include "tiledb/sm/query/query_condition.h"
#include "tiledb/sm/query/reader_base.h"
#include "tiledb/sm/query/result_cell_slab.h"
#include "tiledb/sm/query/result_coords.h"
#include "tiledb/sm/query/result_space_tile.h"
#include "tiledb/sm/subarray/subarray_partitioner.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class Array;
class StorageManager;
class Tile;

/** Processes read queries. */
class Reader : public ReaderBase, public IQueryStrategy {
 public:
  /* ********************************* */
  /*          TYPE DEFINITIONS         */
  /* ********************************* */

  /** The state for a read query. */
  struct ReadState {
    /**
     * True if the query led to a result that does not fit in
     * the user buffers.
     */
    bool overflowed_ = false;

    /** The subarray partitioner. */
    SubarrayPartitioner partitioner_;

    /**
     * ``true`` if the next partition cannot be retrieved from the
     * partitioner, because it reaches a partition that is unsplittable.
     */
    bool unsplittable_ = false;

    /** True if the reader has been initialized. */
    bool initialized_ = false;

    /** ``true`` if there are no more partitions. */
    bool done() const {
      return partitioner_.done();
    }

    /** Retrieves the next partition from the partitioner. */
    Status next() {
      return partitioner_.next(&unsplittable_);
    }

    /**
     * Splits the current partition and updates the state, retrieving
     * a new current partition. This function is typically called
     * by the reader when the current partition was estimated to fit
     * the results, but that was not eventually true.
     */
    Status split_current() {
      return partitioner_.split_current(&unsplittable_);
    }
  };

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  Reader(
      stats::Stats* stats,
      StorageManager* storage_manager,
      Array* array,
      Config& config,
      std::unordered_map<std::string, QueryBuffer>& buffers,
      Subarray& subarray,
      Layout layout,
      QueryCondition& condition,
      bool sparse_mode);

  /** Destructor. */
  ~Reader() = default;

  DISABLE_COPY_AND_COPY_ASSIGN(Reader);
  DISABLE_MOVE_AND_MOVE_ASSIGN(Reader);

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /** Finalizes the reader. */
  Status finalize();

  /**
   * Returns `true` if the query was incomplete, i.e., if all subarray
   * partitions in the read state have not been processed or there
   * was some buffer overflow.
   */
  bool incomplete() const;

  /** Initializes the reader. */
  Status init();

  /** Returns the current read state. */
  const ReadState* read_state() const;

  /** Returns the current read state. */
  ReadState* read_state();

  /** Performs a read query using its set members. */
  Status dowork();

  /** Resets the reader object. */
  void reset();

  /* ********************************* */
  /*          STATIC FUNCTIONS         */
  /* ********************************* */

  /**
   * Computes a mapping (tile coordinates) -> (result space tile).
   * The produced result space tiles will contain information only
   * about fragments that will contribute results. Specifically, if
   * a fragment is completely covered by a more recent fragment
   * in a particular space tile, then it will certainly not contribute
   * results and, thus, no information about that fragment is included
   * in the space tile.
   *
   * @tparam T The datatype of the tile domains.
   * @param domain The array domain
   * @param tile_coords The unique coordinates of the tiles that intersect
   *     a subarray.
   * @param array_tile_domain The array tile domain.
   * @param frag_tile_domains The tile domains of each fragment. These
   *     are assumed to be ordered from the most recent to the oldest
   *     fragment.
   * @param result_space_tiles The result space tiles to be produced
   *     by the function.
   */
  template <class T>
  static void compute_result_space_tiles(
      const Domain* domain,
      const std::vector<std::vector<uint8_t>>& tile_coords,
      const TileDomain<T>& array_tile_domain,
      const std::vector<TileDomain<T>>& frag_tile_domains,
      std::map<const T*, ResultSpaceTile<T>>* result_space_tiles);

  /**
   * Computes the result cell slabs for the input subarray, given the
   * input result coordinates (retrieved from the sparse fragments).
   * The function also computes and stores the results space tiles
   * in `result_space_tiles`. This needs to be preserved throughout
   * the cell copying operations, since this structure stores all
   * the relevant result tiles for the dense fragments.
   *
   * @tparam T The domain datatype.
   * @param subarray The input subarray.
   * @param result_space_tiles The result space tiles computed by the
   *     function, which store the result tiles from the dense fragments.
   * @param result_coords The result coordinates produced by the sparse
   *     fragments.
   * @param result_tiles This will store pointers to the result tiles
   *     of both dense and sparse fragments.
   * @param result_cell_slabs The returned result cell slabs.
   * @return Status
   */
  template <class T>
  Status compute_result_cell_slabs(
      const Subarray& subarray,
      std::map<const T*, ResultSpaceTile<T>>* result_space_tiles,
      std::vector<ResultCoords>* result_coords,
      std::vector<ResultTile*>* result_tiles,
      std::vector<ResultCellSlab>* result_cell_slabs) const;

  /**
   * Computes the result cell slabs for the input subarray, given the
   * input result coordinates (retrieved from the sparse fragments).
   * The function also computes and stores the results space tiles
   * in `result_space_tiles`. This needs to be preserved throughout
   * the cell copying operations, since this structure stores all
   * the relevant result tiles for the dense fragments. Applicable
   * only to row-/col-major subarray layouts.
   *
   * @tparam T The domain datatype.
   * @param subarray The input subarray.
   * @param result_coords The result coordinates produced by the sparse
   *     fragments.
   * @param result_coords_pos The position in `result_coords` to be
   *     passed to the result cell slab iterator in the function.
   *     This practically keeps track of the sparse coordinate results
   *     already processed in successive calls of this function.
   *     The function updates this value with the current position
   *     returned by the iterator at the end of its process.
   * @param result_space_tiles The result space tiles computed by the
   *     function, which store the result tiles from the dense fragments.
   * @param result_tiles This will store pointers to the result tiles
   *     of both dense and sparse fragments.
   * @param frag_tile_set Stores the unique pairs (frag_idx, tile_idx)
   *     for all result tiles.
   * @param result_cell_slabs The returned result cell slabs.
   * @return Status
   */
  template <class T>
  Status compute_result_cell_slabs_row_col(
      const Subarray& subarray,
      std::map<const T*, ResultSpaceTile<T>>* result_space_tiles,
      std::vector<ResultCoords>* result_coords,
      uint64_t* result_coords_pos,
      std::vector<ResultTile*>* result_tiles,
      std::set<std::pair<unsigned, uint64_t>>* frag_tile_set,
      std::vector<ResultCellSlab>* result_cell_slabs) const;

  /**
   * Computes the result cell slabs for the input subarray, given the
   * input result coordinates (retrieved from the sparse fragments).
   * The function also computes and stores the results space tiles
   * in `result_space_tiles`. This needs to be preserved throughout
   * the cell copying operations, since this structure stores all
   * the relevant result tiles for the dense fragments. Applicable
   * only to global order subarray layouts.
   *
   * @tparam T The domain datatype.
   * @param subarray The input subarray.
   * @param result_coords The result coordinates produced by the sparse
   *     fragments.
   * @param result_space_tiles The result space tiles computed by the
   *     function, which store the result tiles from the dense fragments.
   * @param result_tiles This will store pointers to the result tiles
   *     of both dense and sparse fragments.
   * @param result_cell_slabs The returned result cell slabs.
   * @return Status
   */
  template <class T>
  Status compute_result_cell_slabs_global(
      const Subarray& subarray,
      std::map<const T*, ResultSpaceTile<T>>* result_space_tiles,
      std::vector<ResultCoords>* result_coords,
      std::vector<ResultTile*>* result_tiles,
      std::vector<ResultCellSlab>* result_cell_slabs) const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** Read state. */
  ReadState read_state_;

  /**
   * If `true`, then the dense array will be read in "sparse mode", i.e.,
   * the sparse read algorithm will be executing, returning results only
   * for the non-empty cells.
   */
  bool sparse_mode_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /**
   * Compute the maximal cell slabs of contiguous sparse coordinates.
   *
   * @param coords The coordinates to compute the slabs from.
   * @param result_cell_slabs The result cell slabs to compute.
   * @return Status
   */
  Status compute_result_cell_slabs(
      const std::vector<ResultCoords>& result_coords,
      std::vector<ResultCellSlab>* result_cell_slabs) const;

  /**
   * Retrieves the coordinates that overlap the input N-dimensional range
   * from the input result tile.
   *
   * @param subarray The subarray to operate on.
   * @param frag_idx The id of the fragment that the result tile belongs to.
   * @param tile The result tile.
   * @param range_idx The range id.
   * @param result_coords The overlapping coordinates to retrieve.
   * @return Status
   */
  Status compute_range_result_coords(
      Subarray* subarray,
      unsigned frag_idx,
      ResultTile* tile,
      uint64_t range_idx,
      std::vector<ResultCoords>* result_coords);

  /**
   * Computes the result coordinates for each range of the query
   * subarray.
   *
   * @param subarray The subarray to operate on.
   * @param single_fragment For each range, it indicates whether all
   *     result coordinates come from a single fragment.
   * @param result_tile_map This is an auxialiary map that helps finding the
   *     result tiles of each range.
   * @param result_tiles The result tiles to read the coordinates from.
   * @param range_result_coords The result coordinates to be retrieved.
   *     It contains a vector for each range of the subarray.
   * @return Status
   */
  Status compute_range_result_coords(
      Subarray* subarray,
      const std::vector<bool>& single_fragment,
      const std::map<std::pair<unsigned, uint64_t>, size_t>& result_tile_map,
      std::vector<ResultTile>* result_tiles,
      std::vector<std::vector<ResultCoords>>* range_result_coords);

  /**
   * Computes the result coordinates of a given range of the query
   * subarray.
   *
   * @param subarray The subarray to operate on.
   * @param range_idx The range to focus on.
   * @param result_tile_map This is an auxialiary map that helps finding the
   *     result_tiles overlapping with each range.
   * @param result_tiles The result tiles to read the coordinates from.
   * @param range_result_coords The result coordinates to be retrieved.
   *     It contains a vector for each range of the subarray.
   * @return Status
   */
  Status compute_range_result_coords(
      Subarray* subarray,
      uint64_t range_idx,
      const std::map<std::pair<unsigned, uint64_t>, size_t>& result_tile_map,
      std::vector<ResultTile>* result_tiles,
      std::vector<ResultCoords>* range_result_coords);

  /**
   * Computes the result coordinates of a given range of the query
   * subarray.
   *
   * @param subarray The subarray to operate on.
   * @param range_idx The range to focus on.
   * @param fragment_idx The fragment to focus on.
   * @param result_tile_map This is an auxialiary map that helps finding the
   *     result_tiles overlapping with each range.
   * @param result_tiles The result tiles to read the coordinates from.
   * @param range_result_coords The result coordinates to be retrieved.
   *     It contains a vector for each range of the subarray.
   * @return Status
   */
  Status compute_range_result_coords(
      Subarray* subarray,
      uint64_t range_idx,
      uint32_t fragment_idx,
      const std::map<std::pair<unsigned, uint64_t>, size_t>& result_tile_map,
      std::vector<ResultTile>* result_tiles,
      std::vector<ResultCoords>* range_result_coords);

  /**
   * Computes the final subarray result coordinates, which will be
   * deduplicated and sorted on the specified subarray layout.
   *
   * @param range_result_coords The result coordinates for each subarray range.
   * @param result_coords The final (subarray) result coordinates to be
   *     retrieved.
   * @return Status
   *
   * @note the function will try to gradually clean up ``range_result_coords``
   *     as it is done processing its elements to quickly reclaim memory.
   */
  Status compute_subarray_coords(
      std::vector<std::vector<ResultCoords>>* range_result_coords,
      std::vector<ResultCoords>* result_coords);

  /**
   * Computes info about the sparse result tiles, such as which fragment they
   * belong to, the tile index and the type of overlap. The tile vector
   * contains unique info about the tiles. The function also computes
   * a map from fragment index and tile id to a result tile to keep
   * track of the unique result  tile info for subarray ranges that overlap
   * with common tiles.
   *
   * @param result_tiles The result tiles to be computed.
   * @param result_tile_map The result tile map to be computed.
   * @param single_fragment Each element corresponds to a range of the
   *     subarray and is set to ``true`` if all the overlapping
   *     tiles come from a single fragment for that range.
   * @return Status
   */
  Status compute_sparse_result_tiles(
      std::vector<ResultTile>* result_tiles,
      std::map<std::pair<unsigned, uint64_t>, size_t>* result_tile_map,
      std::vector<bool>* single_fragment);

  /**
   * Computes the result space tiles based on the input subarray.
   *
   * @tparam T The domain datatype.
   * @param subarray The input subarray.
   * @param result_space_tiles The result space tiles to be computed.
   */
  template <class T>
  void compute_result_space_tiles(
      const Subarray& subarray,
      std::map<const T*, ResultSpaceTile<T>>* result_space_tiles) const;

  /**
   * Computes the result coordinates from the sparse fragments.
   *
   * @param result_tiles This will store the unique result tiles.
   * @param result_coords This will store the result coordinates.
   */
  Status compute_result_coords(
      std::vector<ResultTile>* result_tiles,
      std::vector<ResultCoords>* result_coords);

  /**
   * Deduplicates the input result coordinates, breaking ties giving preference
   * to the largest fragment index (i.e., it prefers more recent fragments).
   *
   * @param result_coords The result coordinates to dedup.
   * @return Status
   */
  Status dedup_result_coords(std::vector<ResultCoords>* result_coords) const;

  /** Performs a read on a dense array. */
  Status dense_read();

  /**
   * Performs a read on a dense array.
   *
   * @tparam The domain type.
   * @return Status
   */
  template <class T>
  Status dense_read();

  /**
   * Fills the coordinate buffer with coordinates. Applicable only to dense
   * arrays when the user explicitly requests the coordinates to be
   * materialized.
   *
   * @tparam T The domain type.
   * @param subarray The input subarray.
   * @return Status
   */
  template <class T>
  Status fill_dense_coords(const Subarray& subarray);

  /**
   * Fills the coordinate buffers with coordinates. Applicable only to dense
   * arrays when the user explicitly requests the coordinates to be
   * materialized. Also applicable only to global order.
   *
   * @tparam T The domain type.
   * @param subarray The input subarray.
   * @param dim_idx The dimension indices of the corresponding `buffers`.
   *     For the special zipped coordinates, `dim_idx`, `buffers` and `offsets`
   *     contain a single element and `dim_idx` contains `dim_num` as
   *     the dimension index.
   * @param buffers The buffers to copy from. It could be the special
   *     zipped coordinates or separate coordinate buffers.
   * @param offsets The offsets that will be used eventually to update
   *     the buffer sizes, determining the useful results written in
   *     the buffers.
   * @return Status
   */
  template <class T>
  Status fill_dense_coords_global(
      const Subarray& subarray,
      const std::vector<unsigned>& dim_idx,
      const std::vector<QueryBuffer*>& buffers,
      std::vector<uint64_t>* offsets);

  /**
   * Fills the coordinate buffers with coordinates. Applicable only to dense
   * arrays when the user explicitly requests the coordinates to be
   * materialized. Also applicable only to row-/col-major order.
   *
   * @tparam T The domain type.
   * @param subarray The input subarray.
   * @param dim_idx The dimension indices of the corresponding `buffers`.
   *     For the special zipped coordinates, `dim_idx`, `buffers` and `offsets`
   *     contain a single element and `dim_idx` contains `dim_num` as
   *     the dimension index.
   * @param buffers The buffers to copy from. It could be the special
   *     zipped coordinates or separate coordinate buffers.
   * @param offsets The offsets that will be used eventually to update
   *     the buffer sizes, determining the useful results written in
   *     the buffers.
   * @return Status
   */
  template <class T>
  Status fill_dense_coords_row_col(
      const Subarray& subarray,
      const std::vector<unsigned>& dim_idx,
      const std::vector<QueryBuffer*>& buffers,
      std::vector<uint64_t>* offsets);

  /**
   * Fills coordinates in the input buffers for a particular cell slab,
   * following a row-major layout. For instance, if the starting coordinate are
   * [3, 1] and the number of coords to be written is 3, this function will
   * write to the input buffer (starting at the input offset) coordinates
   * [3, 1], [3, 2], and [3, 3].
   *
   * @tparam T The domain type.
   * @param start The starting coordinates in the slab.
   * @param num The number of coords to be written.
   * @param dim_idx The dimension indices of the corresponding `buffers`.
   *     For the special zipped coordinates, `dim_idx`, `buffers` and `offsets`
   *     contain a single element and `dim_idx` contains `dim_num` as
   *     the dimension index.
   * @param buffers The buffers to copy from. It could be the special
   *     zipped coordinates or separate coordinate buffers.
   * @param offsets The offsets that will be used eventually to update
   *     the buffer sizes, determining the useful results written in
   *     the buffers.
   */
  template <class T>
  void fill_dense_coords_row_slab(
      const T* start,
      uint64_t num,
      const std::vector<unsigned>& dim_idx,
      const std::vector<QueryBuffer*>& buffers,
      std::vector<uint64_t>* offsets) const;

  /**
   * Fills coordinates in the input buffers for a particular cell slab,
   * following a col-major layout. For instance, if the starting coordinate are
   * [3, 1] and the number of coords to be written is 3, this function will
   * write to the input buffer (starting at the input offset) coordinates
   * [4, 1], [5, 1], and [6, 1].
   *
   * @tparam T The domain type.
   * @param start The starting coordinates in the slab.
   * @param num The number of coords to be written.
   * @param dim_idx The dimension indices of the corresponding `buffers`.
   *     For the special zipped coordinates, `dim_idx`, `buffers` and `offsets`
   *     contain a single element and `dim_idx` contains `dim_num` as
   *     the dimension index.
   * @param buffers The buffers to copy from. It could be the special
   *     zipped coordinates or separate coordinate buffers.
   * @param offsets The offsets that will be used eventually to update
   *     the buffer sizes, determining the useful results written in
   *     the buffers.
   */
  template <class T>
  void fill_dense_coords_col_slab(
      const T* start,
      uint64_t num,
      const std::vector<unsigned>& dim_idx,
      const std::vector<QueryBuffer*>& buffers,
      std::vector<uint64_t>* offsets) const;

  /**
   * Gets all the result coordinates of the input tile into `result_coords`.
   *
   * @param result_tile The result tile to read the coordinates from.
   * @param result_coords The result coordinates to copy into.
   * @return Status
   */
  Status get_all_result_coords(
      ResultTile* tile, std::vector<ResultCoords>* result_coords) const;

  /** Returns `true` if the coordinates are included in the attributes. */
  bool has_coords() const;

  /**
   * Returns `true` if a coordinate buffer for a separate dimension
   * has been set.
   */
  bool has_separate_coords() const;

  /** Initializes the read state. */
  Status init_read_state();

  /**
   * Sorts the input result coordinates according to the subarray layout.
   *
   * @param iter_begin The start position of the coordinates to sort.
   * @param iter_end The end position of the coordinates to sort.
   * @param coords_num The number of coordinates to be sorted.
   * @param layout The layout to sort into.
   * @return Status
   */
  Status sort_result_coords(
      std::vector<ResultCoords>::iterator iter_begin,
      std::vector<ResultCoords>::iterator iter_end,
      size_t coords_num,
      Layout layout) const;

  /** Performs a read on a sparse array. */
  Status sparse_read();

  /**
   * Adds an extra offset in the end of the offsets buffer indicating the
   * returned data size if an attribute is var-sized.
   */
  Status add_extra_offset();

  /**
   * Returns true if the input tile's MBR of the input fragment is fully
   * covered by the non-empty domain of a more recent fragment.
   */
  bool sparse_tile_overwritten(unsigned frag_idx, uint64_t tile_idx) const;

  /**
   * Erases the coordinate tiles (zipped or separate) from the input result
   * tiles.
   */
  void erase_coord_tiles(std::vector<ResultTile>* result_tiles) const;

  /** Gets statistics about the result cells. */
  void get_result_cell_stats(
      const std::vector<ResultCellSlab>& result_cell_slabs) const;

  /** Gets statistics about the result tiles. */
  void get_result_tile_stats(
      const std::vector<ResultTile*>& result_tiles) const;

  /**
   * Calculates the hilbert values of the result coordinates between
   * `iter_begin` and `iter_begin + hilbert_values.size()`.
   * The hilbert values are stored
   * in `hilbert_values`, where the first pair value is the hilbert value
   * and the second is the position of the result coords after the
   * input iterator.
   */
  Status calculate_hilbert_values(
      std::vector<ResultCoords>::iterator iter_begin,
      std::vector<std::pair<uint64_t, uint64_t>>* hilbert_values) const;

  /**
   * It reorganizes the result coords given the iterator offsets in
   * `hilbert_values` (second values in the pair). This essentially
   * sorts the result coordinates starting at `iter_begin` based
   * on the already sorted hilbert values.
   *
   * The algorithm is in-place, operates with O(1) memory and
   * in O(coords_num) time, but modifies the offsets/positions in
   * `hilbert_values`.
   */
  Status reorganize_result_coords(
      std::vector<ResultCoords>::iterator iter_begin,
      std::vector<std::pair<uint64_t, uint64_t>>* hilbert_values) const;

  /**
   * Returns true if the result coordinates between the two iterators
   * belong to the same fragment.
   */
  bool belong_to_single_fragment(
      std::vector<ResultCoords>::iterator iter_begin,
      std::vector<ResultCoords>::iterator iter_end) const;

  /** Perform necessary checks before exiting a read loop */
  Status complete_read_loop();
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_READER_H
