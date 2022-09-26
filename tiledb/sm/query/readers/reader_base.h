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
#include "../strategy_base.h"
#include "tiledb/common/common.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/tile_domain.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/query/query_condition.h"
#include "tiledb/sm/query/readers/result_cell_slab.h"
#include "tiledb/sm/query/readers/result_space_tile.h"
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
      shared_ptr<Logger> logger,
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
      const std::vector<shared_ptr<FragmentMetadata>>& fragment_metadata,
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

  /**
   * The delete and update conditions.
   *
   * Note: These will be ordered by timestamps.
   */
  std::vector<QueryCondition> delete_and_update_conditions_;

  /**
   * Timestamped delete and update conditions. This the same as
   * delete_and_update_conditions_ but adds a conditional in the condition with
   * the timestamp of the condition. It will be used to process fragments with
   * timestamps when a delete or update condition timestamp falls within the
   * fragment timestamps.
   *
   * Note: These should have the same order as in the
   * `delete_and_update_conditions_` vector.
   */
  std::vector<QueryCondition> timestamped_delete_and_update_conditions_;

  /** The fragment metadata that the reader will focus on. */
  std::vector<shared_ptr<FragmentMetadata>> fragment_metadata_;

  /** Disable the tile cache or not. */
  bool disable_cache_;

  /** Read directly from storage without batching. */
  bool disable_batching_;

  /**
   * The condition to apply on results when there is partial time overlap
   * with at least one fragment
   */
  QueryCondition partial_overlap_condition_;

  /**
   * The condition to apply on results when there is a delete timestamps
   * column for a fragment
   */
  QueryCondition delete_timestamps_condition_;

  /** If the user requested timestamps attribute in the query */
  bool user_requested_timestamps_;

  /**
   * If the special timestamps attribute should be loaded to memory for
   * this query
   */
  bool use_timestamps_;

  /**
   * Boolean, per fragment, to specify that we need to load timestamps for
   * deletes. This matches the fragments in 'fragment_metadata_'
   */
  std::vector<bool> timestamps_needed_for_deletes_and_updates_;

  /** Names of dim/attr loaded for query condition. */
  std::unordered_set<std::string> qc_loaded_attr_names_set_;

  /** Have we loaded the initial data. */
  bool initial_data_loaded_;

  /* ********************************* */
  /*         PROTECTED METHODS         */
  /* ********************************* */

  /**
   * Returns if we need to process partial timestamp condition for this
   * fragment.
   *
   * @param frag_meta Fragment metadata.
   * @return true if the condition need to be processed.
   */
  bool process_partial_timestamps(FragmentMetadata& frag_meta) const;

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
   * Is there a need to build timestamped conditions for deletes.
   *
   * @return true if the conditions need to be generated.
   */
  bool need_timestamped_conditions();

  /**
   * Generates timestamped conditions for deletes.
   *
   * @return Status.
   */
  Status generate_timestamped_conditions();

  /**
   * Resets the buffer sizes to the original buffer sizes. This is because
   * the read query may alter the buffer sizes to reflect the size of
   * the useful data (results) written in the buffers.
   */
  void reset_buffer_sizes();

  /** Zeroes out the user buffer sizes, indicating an empty result. */
  void zero_out_buffer_sizes();

  /** Correctness checks for `subarray_`. */
  void check_subarray() const;

  /** Correctness checks validity buffer sizes in `buffers_`. */
  void check_validity_buffer_sizes() const;

  /**
   * Skip read/unfilter operations for timestamps attribute and fragments
   * without timestamps.
   */
  inline bool timestamps_not_present(
      const std::string& name, const unsigned f) const {
    return name == constants::timestamps && !include_timestamps(f);
  }

  /**
   * Skip read/unfilter operations for timestamps attribute and fragments
   * without delete metadata.
   */
  inline bool delete_meta_not_present(
      const std::string& name, const unsigned f) const {
    return (name == constants::delete_timestamps ||
            name == constants::delete_condition_index) &&
           !fragment_metadata_[f]->has_delete_meta();
  }

  /**
   * Checks if timestamps should be loaded for a fragment.
   *
   * @param f Fragment index.
   * @return True if timestamps should be included, false if they are not.
   * needed.
   */
  bool include_timestamps(const unsigned f) const;

  /**
   * Returns the fragment timestamp for a result tile.
   *
   * @param rt Result tile.
   * @return fragment timestamp.
   */
  inline uint64_t fragment_timestamp(ResultTile* rt) const {
    return fragment_metadata_[rt->frag_idx()]->timestamp_range().first;
  }

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
   * Checks if at least one fragment overlaps partially with the
   * time at which the read is taking place.
   *
   * @return True if at least one fragment partially overlaps.
   */
  bool partial_consolidated_fragment_overlap() const;

  /**
   * Add a condition for partial time overlap based on array open and
   * end times, to be used to filter out results on fragments that have
   * been consolidated with timestamps.
   */
  Status add_partial_overlap_condition();

  /**
   * Add a condition for delete timestamps.
   */
  Status add_delete_timestamps_condition();

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
   * Loads processed conditions from fragment metadata.
   *
   * @param subarray The subarray to load processed conditions for.
   * @return Status
   */
  Status load_processed_conditions();

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
   * @return Status
   */
  Status read_attribute_tiles(
      const std::vector<std::string>& names,
      const std::vector<ResultTile*>& result_tiles) const;

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
   * @return Status
   */
  Status read_coordinate_tiles(
      const std::vector<std::string>& names,
      const std::vector<ResultTile*>& result_tiles) const;

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
   * @return Status
   */
  Status read_tiles(
      const std::vector<std::string>& names,
      const std::vector<ResultTile*>& result_tiles) const;

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
   * @return Status
   */
  Status unfilter_tiles(
      const std::string& name,
      const std::vector<ResultTile*>& result_tiles) const;

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