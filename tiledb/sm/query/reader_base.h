/**
 * @file   reader_base.h
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
 * This file defines class ReaderBase.
 */

#ifndef TILEDB_READER_BASE_H
#define TILEDB_READER_BASE_H

#include <queue>
#include "strategy_base.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/tile_domain.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/query/query_condition.h"
#include "tiledb/sm/query/result_cell_slab.h"
#include "tiledb/sm/query/result_space_tile.h"
#include "tiledb/sm/subarray/subarray_partitioner.h"

namespace tiledb {
namespace sm {

class Array;
class ArraySchema;
class StorageManager;
class Subarray;

/** Processes read queries. */
class ReaderBase : public StrategyBase {
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
  ReaderBase(
      stats::Stats* stats,
      tdb_shared_ptr<Logger> logger,
      StorageManager* storage_manager,
      Array* array,
      Config& config,
      std::unordered_map<std::string, QueryBuffer>& buffers,
      Subarray& subarray,
      Layout layout,
      QueryCondition& condition);

  /** Destructor. */
  ~ReaderBase() = default;

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
      const std::vector<tdb_shared_ptr<FragmentMetadata>>& fragment_metadata,
      const std::vector<std::vector<uint8_t>>& tile_coords,
      const TileDomain<T>& array_tile_domain,
      const std::vector<TileDomain<T>>& frag_tile_domains,
      std::map<const T*, ResultSpaceTile<T>>& result_space_tiles);

 protected:
  /* ********************************* */
  /*       PROTECTED ATTRIBUTES        */
  /* ********************************* */

  /** The query condition. */
  QueryCondition& condition_;

  /** The fragment metadata that the reader will focus on. */
  std::vector<tdb_shared_ptr<FragmentMetadata>> fragment_metadata_;

  /* ********************************* */
  /*         PROTECTED METHODS         */
  /* ********************************* */

  /**
   * Deletes the tiles on the input attribute/dimension from the result tiles.
   *
   * @param name The attribute/dimension name.
   * @param result_tiles The result tiles to delete from.
   * @param min_result_tile The minimum index to start clearing tiles at.
   * @return void
   */
  void clear_tiles(
      const std::string& name,
      const std::vector<ResultTile*>& result_tiles,
      const uint64_t min_result_tile = 0) const;

  /**
   * Resets the buffer sizes to the original buffer sizes. This is because
   * the read query may alter the buffer sizes to reflect the size of
   * the useful data (results) written in the buffers.
   */
  void reset_buffer_sizes();

  /** Zeroes out the user buffer sizes, indicating an empty result. */
  void zero_out_buffer_sizes();

  /** Correctness checks for `subarray_`. */
  Status check_subarray() const;

  /** Correctness checks validity buffer sizes in `buffers_`. */
  Status check_validity_buffer_sizes() const;

  /**
   * Loads tile offsets for each attribute/dimension name into
   * their associated element in `fragment_metadata_`.
   *
   * @param subarray The subarray to load tiles for.
   * @param names The attribute/dimension names.
   * @return Status
   */
  Status load_tile_offsets(
      Subarray& subarray, const std::vector<std::string>& names);

  /**
   * Loads tile var sizes for each attribute/dimension name into
   * their associated element in `fragment_metadata_`.
   *
   * @param subarray The subarray to load tiles for.
   * @param names The attribute/dimension names.
   * @return Status
   */
  Status load_tile_var_sizes(
      Subarray& subarray, const std::vector<std::string>& names);

  /**
   * Initializes a fixed-sized tile.
   *
   * @param format_version The format version of the tile.
   * @param name The attribute/dimension the tile belongs to.
   * @param tile The tile to be initialized.
   * @return Status
   */
  Status init_tile(
      uint32_t format_version, const std::string& name, Tile* tile) const;

  /**
   * Initializes a var-sized tile.
   *
   * @param format_version The format version of the tile.
   * @param name The attribute/dimension the tile belongs to.
   * @param tile The offsets tile to be initialized.
   * @param tile_var The var-sized data tile to be initialized.
   * @return Status
   */
  Status init_tile(
      uint32_t format_version,
      const std::string& name,
      Tile* tile,
      Tile* tile_var) const;

  /**
   * Initializes a fixed-sized tile.
   *
   * @param format_version The format version of the tile.
   * @param name The attribute/dimension the tile belongs to.
   * @param tile The tile to be initialized.
   * @param tile_validity The validity tile to be initialized.
   * @return Status
   */
  Status init_tile_nullable(
      uint32_t format_version,
      const std::string& name,
      Tile* tile,
      Tile* tile_validity) const;

  /**
   * Initializes a var-sized tile.
   *
   * @param format_version The format version of the tile.
   * @param name The attribute/dimension the tile belongs to.
   * @param tile The offsets tile to be initialized.
   * @param tile_var The var-sized data tile to be initialized.
   * @param tile_validity The validity tile to be initialized.
   * @return Status
   */
  Status init_tile_nullable(
      uint32_t format_version,
      const std::string& name,
      Tile* tile,
      Tile* tile_var,
      Tile* tile_validity) const;

  /**
   * Concurrently executes across each name in `names` and each result tile
   * in 'result_tiles'.
   *
   * This must be the entry point for reading attribute tiles because it
   * generates stats for reading attributes.
   *
   * @param names The attribute names.
   * @param result_tiles The retrieved tiles will be stored inside the
   *     `ResultTile` instances in this vector.
   * @param disable_cache Disable the tile cache or not.
   * @return Status
   */
  Status read_attribute_tiles(
      const std::vector<std::string>& names,
      const std::vector<ResultTile*>& result_tiles,
      const bool disable_cache = false) const;

  /**
   * Concurrently executes across each name in `names` and each result tile
   * in 'result_tiles'.
   *
   * This must be the entry point for reading coordinate tiles because it
   * generates stats for reading coordinates.
   *
   * @param names The coordinate/dimension names.
   * @param result_tiles The retrieved tiles will be stored inside the
   *     `ResultTile` instances in this vector.
   * @param disable_cache Disable the tile cache or not.
   * @return Status
   */
  Status read_coordinate_tiles(
      const std::vector<std::string>& names,
      const std::vector<ResultTile*>& result_tiles,
      const bool disable_cache = false) const;

  /**
   * Retrieves the tiles on a list of attribute or dimension and stores it
   * in the appropriate result tile.
   *
   * Concurrently executes across each name in `names` and each result tile
   * in 'result_tiles'.
   *
   * @param names The attribute/dimension names.
   * @param result_tiles The retrieved tiles will be stored inside the
   *     `ResultTile` instances in this vector.
   * @param disable_cache Disable the tile cache or not.
   * @return Status
   */
  Status read_tiles(
      const std::vector<std::string>& names,
      const std::vector<ResultTile*>& result_tiles,
      const bool disable_cache = false) const;

  /**
   * Filters the tiles on a particular attribute/dimension from all input
   * fragments based on the tile info in `result_tiles`. Used only by new
   * readers that parallelize on chunk ranges.
   *
   * @param name Attribute/dimension whose tiles will be unfiltered.
   * @param result_tiles Vector containing the tiles to be unfiltered.
   * @return Status
   */
  Status unfilter_tiles_chunk_range(
      const std::string& name,
      const std::vector<ResultTile*>& result_tiles) const;

  /**
   * Runs the input fixed-sized tile for the input attribute or dimension
   * through the filter pipeline. The tile buffer is modified to contain the
   * output of the pipeline. Used only by new readers that parallelize on chunk
   * ranges.
   *
   * @param num_range_threads Total number of range threads.
   * @param range_thread_idx Current range thread index.
   * @param name Attribute/dimension the tile belong to.
   * @param tile Tile to be unfiltered.
   * @param tile_chunk_data Tile chunk info, buffers and offsets
   * @return Status
   */
  Status unfilter_tile_chunk_range(
      uint64_t num_range_threads,
      uint64_t thread_idx,
      const std::string& name,
      Tile* tile,
      const ChunkData& tile_chunk_data) const;

  /**
   * Runs the input var-sized tile for the input attribute or dimension through
   * the filter pipeline. The tile buffer is modified to contain the output of
   * the pipeline. Used only by new readers that parallelize on chunk ranges.
   *
   * @param num_range_threads Total number of range threads.
   * @param range_thread_idx Current range thread index.
   * @param name Attribute/dimension the tile belong to.
   * @param tile Offsets tile to be unfiltered.
   * @param tile_chunk_data Offsets tile chunk info, buffers and offsets
   * @param tile_var Value tile to be unfiltered.
   * @param tile_var_chunk_data Value tile chunk info, buffers and offsets
   * @return Status
   */
  Status unfilter_tile_chunk_range(
      uint64_t num_range_threads,
      uint64_t thread_idx,
      const std::string& name,
      Tile* tile,
      const ChunkData& tile_chunk_data,
      Tile* tile_var,
      const ChunkData& tile_var_chunk_data) const;

  /**
   * Runs the input fixed-sized tile for the input nullable attribute
   * through the filter pipeline. The tile buffer is modified to contain the
   * output of the pipeline. Used only by new readers that parallelize on chunk
   * ranges.
   *
   * @param num_range_threads Total number of range threads.
   * @param range_thread_idx Current range thread index.
   * @param name Attribute/dimension the tile belong to.
   * @param tile Tile to be unfiltered.
   * @param tile_chunk_data Tile chunk info, buffers and offsets
   * @param tile_validity Validity tile to be unfiltered.
   * @param tile_validity_chunk_data Validity tile chunk info, buffers and
   * offsets
   * @return Status
   */
  Status unfilter_tile_chunk_range_nullable(
      uint64_t num_range_threads,
      uint64_t thread_idx,
      const std::string& name,
      Tile* tile,
      const ChunkData& tile_chunk_data,
      Tile* tile_validity,
      const ChunkData& tile_validity_chunk_data) const;

  /**
   * Runs the input var-sized tile for the input nullable attribute through
   * the filter pipeline. The tile buffer is modified to contain the output of
   * the pipeline. Used only by new readers that parallelize on chunk ranges.
   *
   * @param num_range_threads Total number of range threads.
   * @param range_thread_idx Current range thread index.
   * @param name Attribute/dimension the tile belong to.
   * @param tile Offsets tile to be unfiltered.
   * @param tile_chunk_data Offsets tile chunk info, buffers and offsets
   * @param tile_var Value tile to be unfiltered.
   * @param tile_var_chunk_data Value tile chunk info, buffers and offsets
   * @param tile_validity Validity tile to be unfiltered.
   * @param tile_validity_chunk_data Validity tile chunk info, buffers and
   * offsets
   * @return Status
   */
  Status unfilter_tile_chunk_range_nullable(
      uint64_t num_range_threads,
      uint64_t thread_idx,
      const std::string& name,
      Tile* tile,
      const ChunkData& tile_chunk_data,
      Tile* tile_var,
      const ChunkData& tile_var_chunk_data,
      Tile* tile_validity,
      const ChunkData& tile_validity_chunk_data) const;

  /**
   * Filters the tiles on a particular attribute/dimension from all input
   * fragments based on the tile info in `result_tiles`.
   *
   * @param name Attribute/dimension whose tiles will be unfiltered.
   * @param result_tiles Vector containing the tiles to be unfiltered.
   * @param disable_cache Disable the filtered buffers cache or not.
   * @return Status
   */
  Status unfilter_tiles(
      const std::string& name,
      const std::vector<ResultTile*>& result_tiles,
      const bool disable_cache = false) const;

  /**
   * Runs the input fixed-sized tile for the input attribute or dimension
   * through the filter pipeline. The tile buffer is modified to contain the
   * output of the pipeline.
   *
   * @param name The attribute/dimension the tile belong to.
   * @param tile The tile to be unfiltered.
   * @return Status
   */
  Status unfilter_tile(const std::string& name, Tile* tile) const;

  /**
   * Runs the input var-sized tile for the input attribute or dimension through
   * the filter pipeline. The tile buffer is modified to contain the output of
   * the pipeline.
   *
   * @param name The attribute/dimension the tile belong to.
   * @param tile The offsets tile to be unfiltered.
   * @param tile_var The value tile to be unfiltered.
   * @return Status
   */
  Status unfilter_tile(
      const std::string& name, Tile* tile, Tile* tile_var) const;

  /**
   * Runs the input fixed-sized tile for the input nullable attribute
   * through the filter pipeline. The tile buffer is modified to contain the
   * output of the pipeline.
   *
   * @param name The attribute/dimension the tile belong to.
   * @param tile The tile to be unfiltered.
   * @param tile_validity The validity tile to be unfiltered.
   * @return Status
   */
  Status unfilter_tile_nullable(
      const std::string& name, Tile* tile, Tile* tile_validity) const;

  /**
   * Runs the input var-sized tile for the input nullable attribute through
   * the filter pipeline. The tile buffer is modified to contain the output of
   * the pipeline.
   *
   * @param name The attribute/dimension the tile belong to.
   * @param tile The offsets tile to be unfiltered.
   * @param tile_var The value tile to be unfiltered.
   * @param tile_validity The validity tile to be unfiltered.
   * @return Status
   */
  Status unfilter_tile_nullable(
      const std::string& name,
      Tile* tile,
      Tile* tile_var,
      Tile* tile_validity) const;

  /**
   * Returns the configured bytesize for var-sized attribute offsets
   */
  uint64_t offsets_bytesize() const;

  /**
   * Get the size of an attribute tile.
   *
   * @param name The attribute name.
   * @param f The fragment idx.
   * @param t The tile idx.
   * @return Status, tile size.
   */
  tuple<Status, optional<uint64_t>> get_attribute_tile_size(
      const std::string& name, unsigned f, uint64_t t);

  /**
   * Computes the result space tiles based on the current partition.
   *
   * @tparam T The domain datatype.
   * @param subarray The input subarray.
   * @param partitioner_subarray The partitioner subarray.
   * @param result_space_tiles The result space tiles to be computed.
   */
  template <class T>
  void compute_result_space_tiles(
      const Subarray& subarray,
      const Subarray& partitioner_subarray,
      std::map<const T*, ResultSpaceTile<T>>& result_space_tiles) const;

  /** Returns `true` if the coordinates are included in the attributes. */
  bool has_coords() const;

  /**
   * Fills the coordinate buffer with coordinates. Applicable only to dense
   * arrays when the user explicitly requests the coordinates to be
   * materialized.
   *
   * @tparam T The domain type.
   * @param subarray The input subarray.
   * @param overflowed Returns true if the method overflowed.
   * @return Status, overflowed
   */
  template <class T>
  tuple<Status, optional<bool>> fill_dense_coords(const Subarray& subarray);

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
   * @return Status, overflowed.
   */
  template <class T>
  tuple<Status, optional<bool>> fill_dense_coords_global(
      const Subarray& subarray,
      const std::vector<unsigned>& dim_idx,
      const std::vector<QueryBuffer*>& buffers,
      std::vector<uint64_t>& offsets);

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
   * @return Status, overflowed.
   */
  template <class T>
  tuple<Status, optional<bool>> fill_dense_coords_row_col(
      const Subarray& subarray,
      const std::vector<unsigned>& dim_idx,
      const std::vector<QueryBuffer*>& buffers,
      std::vector<uint64_t>& offsets);

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
      std::vector<uint64_t>& offsets) const;

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
      std::vector<uint64_t>& offsets) const;

  /**
   * If the tile stores coordinates, zip them. Note that format version < 2 only
   * split the coordinates when compression was used. See
   * https://github.com/TileDB-Inc/TileDB/issues/1053. For format version > 4,
   * a tile never stores coordinates
   *
   * @param name Attribute/dimension the tile belongs to.
   * @param tile Tile to zip the coordinates if needed.
   * @return Status
   */
  Status zip_tile_coordinates(const std::string& name, Tile* tile) const;

  /**
   * Computes the minimum and maximum indexes of tile chunks to process based on
   * the available threads.
   *
   * @param num_chunks Total number of chunks in a tile
   * @param num_range_threads Total number of range threads.
   * @param range_thread_idx Current range thread index.
   * @return {min, max}
   */
  tuple<uint64_t, uint64_t> compute_chunk_min_max(
      const uint64_t num_chunks,
      const uint64_t num_range_threads,
      const uint64_t thread_idx) const;

  /**
   * Reads the chunk data of all tile buffers and stores them in a data
   * structure together with the offsets between them
   *
   * @param name Attribute/dimension the tile belong to.
   * @param tile Offsets tile to be unfiltered.
   * @param var_size True if the attribute/dimension is var-sized, false
   * otherwise
   * @param nullable True if the attribute/dimension is nullable, false
   * otherwise
   * @param tile_chunk_data Tile/offsets tile chunk info, buffers and
   * offsets
   * @param tile_var_chunk_data Value tile chunk info, buffers and offsets
   * @param tile_validity_chunk_data Validity tile chunk info, buffers and
   * offsets
   * @return {Status, size of the unfiltered tile buffer, size of the unfiltered
   * tile_var buffer, size of the unfiltered tile validity buffer}
   */
  tuple<Status, optional<uint64_t>, optional<uint64_t>, optional<uint64_t>>
  load_tile_chunk_data(
      const std::string& name,
      ResultTile* const tile,
      const bool var_size,
      const bool nullable,
      ChunkData* const tile_chunk_data,
      ChunkData* const tile_chunk_var_data,
      ChunkData* const tile_chunk_validity_data) const;

  /**
   * Reads the chunk data of a tile buffer and populates a chunk data structure
   *
   * @param tile Offsets tile to be unfiltered.
   * @param tile_chunk_data Tile chunk info, buffers and offsets
   * @return Status
   */
  tuple<Status, optional<uint64_t>> load_chunk_data(
      Tile* const tile, ChunkData* chunk_data) const;

  /**
   * Unfilter a specific range of chunks in tile
   *
   * @param name Attribute/dimension the tile belong to.
   * @param tile Offsets tile to be unfiltered.
   * @param var_size True if the attribute/dimension is var-sized, false
   * otherwise
   * @param nullable True if the attribute/dimension is nullable, false
   * otherwise
   * @param range_thread_idx Current range thread index.
   * @param num_range_threads Total number of range threads.
   * @param tile_chunk_data Tile chunk info, buffers and offsets
   * @param tile_var_chunk_data Value tile chunk info, buffers and offsets
   * @param tile_validity_chunk_data Validity tile chunk info, buffers and
   * offsets
   * @return Status
   */
  Status unfilter_tile_chunk_range(
      const std::string& name,
      ResultTile* const tile,
      const bool var_size,
      const bool nullable,
      uint64_t range_thread_idx,
      uint64_t num_range_threads,
      const ChunkData& tile_chunk_data,
      const ChunkData& tile_chunk_var_data,
      const ChunkData& tile_chunk_validity_data) const;

  /**
   * Perform some necessary post-processing on a tile that was just unfiiltered
   *
   * @param name Attribute/dimension the tile belong to.
   * @param tile Offsets tile that was just unfiltered.
   * @param var_size True if the attribute/dimension is var-sized, false
   * otherwise
   * @param nullable True if the attribute/dimension is nullable, false
   * otherwise
   * @param unfiltered_tile_size Size of the unfiltered tile buffer
   * @param unfiltered_tile_var_size Size of the unfiltered tile_var buffer
   * @param unfiltered_tile_validity_size Size of the unfiltered tile
   * validity buffer
   * @return Status
   */
  Status post_process_unfiltered_tile(
      const std::string& name,
      ResultTile* const tile,
      const bool var_size,
      const bool nullable) const;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_READER_BASE_H