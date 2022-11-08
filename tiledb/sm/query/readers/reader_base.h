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

#include "../strategy_base.h"
#include "tiledb/common/common.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/tile_domain.h"
#include "tiledb/sm/filter/bitsort_filter_type.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/query/query_condition.h"
#include "tiledb/sm/query/readers/result_cell_slab.h"
#include "tiledb/sm/query/readers/result_space_tile.h"
#include "tiledb/sm/query/writers/domain_buffer.h"
#include "tiledb/sm/storage_manager/storage_manager_declaration.h"
#include "tiledb/sm/subarray/subarray_partitioner.h"

namespace tiledb {
namespace sm {

class Array;
class ArraySchema;
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

  class AttributeOrderValidationData {
   public:
    /* ********************************* */
    /*     CONSTRUCTORS & DESTRUCTORS    */
    /* ********************************* */
    AttributeOrderValidationData() = delete;

    /**
     * Construct a new Order Validation Data object.
     *
     * @param num_frags Number of fragments.
     */
    AttributeOrderValidationData(uint64_t num_frags)
        : result_tiles_to_load_(num_frags)
        , value_validated_(num_frags)
        , tile_to_compare_against_(num_frags) {
    }

    /* ********************************* */
    /*                 API               */
    /* ********************************* */

    /**
     * Add a tile to compare against when running the order validation against
     * tile data.
     *
     * @param f Current fragment index.
     * @param is_lower_bound Is this for the lower bound or upper bound.
     * @param f_to_compare Fragment index of the tile to compare against.
     * @param t_to_compare Tile index of the tile to compare against.
     * @param schema Array schema.
     */
    inline void add_tile_to_load(
        unsigned f,
        bool is_lower_bound,
        uint64_t f_to_compare,
        uint64_t t_to_compare,
        const ArraySchema& schema) {
      auto it = result_tiles_to_load_[f].find(t_to_compare);
      if (it == result_tiles_to_load_[f].end()) {
        result_tiles_to_load_[f].emplace(
            std::piecewise_construct,
            std::forward_as_tuple(t_to_compare),
            std::forward_as_tuple(f_to_compare, t_to_compare, schema));
      }

      if (is_lower_bound) {
        tile_to_compare_against_[f].first = t_to_compare;
      } else {
        tile_to_compare_against_[f].second = t_to_compare;
      }
    }

    /**
     * Return the min validated value for a fragment.
     *
     * @param f Fragment index.
     * @return reference to the min validated value.
     */
    inline bool& min_validated(unsigned f) {
      return value_validated_[f].first;
    }

    /**
     * Return the max validated value for a fragment.
     *
     * @param f Fragment index.
     * @return reference to the max validated value.
     */
    inline bool& max_validated(unsigned f) {
      return value_validated_[f].second;
    }

    /**
     * Return the tile, for the fragment min, to compare against.
     *
     * @param f Fragment index.
     * @return Tile to compare against.
     */
    inline ResultTile* min_tile_to_compare_against(unsigned f) {
      return &result_tiles_to_load_[f][tile_to_compare_against_[f].first];
    }

    /**
     * Return the tile, for the fragment max, to compare against.
     *
     * @param f Fragment index.
     * @return Tile to compare against.
     */
    inline ResultTile* max_tile_to_compare_against(unsigned f) {
      return &result_tiles_to_load_[f][tile_to_compare_against_[f].second];
    }

    /** Returns 'true' if tiles need to be loaded. */
    inline bool need_to_load_tiles() {
      for (auto& rt_map : result_tiles_to_load_) {
        if (!rt_map.empty()) {
          return true;
        }
      }
      return false;
    }

    /** Returns a vector of pointers to tiles to load. */
    std::vector<ResultTile*> tiles_to_load() {
      std::vector<ResultTile*> ret;
      uint64_t size = 0;
      for (auto& rt_map : result_tiles_to_load_) {
        size += rt_map.size();
      }

      ret.reserve(size);
      for (auto& rt_map : result_tiles_to_load_) {
        for (auto& rt : rt_map) {
          ret.emplace_back(&rt.second);
        }
      }

      return ret;
    }

   private:
    /* ********************************* */
    /*         PRIVATE ATTRIBUTES        */
    /* ********************************* */

    /** Map of result tiles to load, per fragments. */
    std::vector<std::unordered_map<uint64_t, ResultTile>> result_tiles_to_load_;

    /**
     * Vector of pairs to store, per fragment, which min/max has been validated
     * already.
     */
    std::vector<std::pair<bool, bool>> value_validated_;

    /**
     * Vector of pairs to store, per fragment, which tile we should compate data
     * against for the min/max. The value is an index into
     * `result_tiles_to_load_`.
     */
    std::vector<std::pair<uint64_t, uint64_t>> tile_to_compare_against_;
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
   * @param support_data Support data for the filter
   * @return Status
   */
  Status unfilter_tile_chunk_range(
      uint64_t num_range_threads,
      uint64_t thread_idx,
      const std::string& name,
      Tile* tile,
      const ChunkData& tile_chunk_data,
      void* support_data = nullptr) const;

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
   * @param support_data Support data for the filter.
   * @return Status
   */
  Status unfilter_tile(
      const std::string& name, Tile* tile, void* support_data = nullptr) const;

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
   * @return Tile size.
   */
  uint64_t get_attribute_tile_size(
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

  /**
   * Cache data to be used by dimension label code.
   *
   * @tparam Index type.
   * @return non empty domain, non empty domains, fragment first array tile
   * indexes.
   */
  template <typename IndexType>
  tuple<Range, std::vector<const void*>, std::vector<uint64_t>>
  cache_dimension_label_data();

  /**
   * Ensures a continuous (with no holes) domain is written. Compute non empty
   * domain at the same time.
   *
   * @tparam Index type.
   * @param non_empty_domains Vector of pointers to the non empty domains for
   * each fragments.
   */
  template <typename IndexType>
  void ensure_continuous_domain_written(
      std::vector<const void*>& non_empty_domains);

  /**
   * Computes, for attribute ordering check, for a fragment if the non empty
   * domain bounds are already validated by previous fragments.
   *
   * @tparam Index type.
   * @param array_min_idx Minimum index value for the array.
   * @param array_max_idx Maximum index value for the array.
   * @param f Fragment index.
   * @param non_empty_domains Vector of pointers to the non empty domains for
   * each fragments.
   * @return Min validated, max validated.
   */
  template <typename IndexType>
  std::pair<bool, bool> attribute_order_ned_bounds_already_validated(
      IndexType array_min_idx,
      IndexType array_max_idx,
      uint64_t f,
      std::vector<const void*>& non_empty_domains);

  /**
   * Validate the attribute order using the tile min/max. The list of tiles to
   * load to process the remaining bounds is returned in
   * AttributeOrderValidationData with the list of bounds that are already
   * validated.
   *
   * @tparam Index type
   * @tparam Attribute type
   * @param attribute_name Name of the attribute to validate.
   * @param increasing_data Is the order of the data increasing?
   * @param array_non_empty_domain Range storing the array non empty domain.
   * @param non_empty_domains Pointer, per fragment, to the non empty domains.
   * @param frag_first_array_tile_idx First tile index (in full domain), per
   * fragment.
   * @return Order validation data.
   */
  template <typename IndexType, typename AttributeType>
  void validate_attribute_order(
      std::string& attribute_name,
      bool increasing_data,
      Range& array_non_empty_domain,
      std::vector<const void*>& non_empty_domains,
      std::vector<uint64_t>& frag_first_array_tile_idx);

  /**
   * Validate the attribute order using the tile min/max. The list of tiles to
   * load to process the remaining bounds is returned in
   * AttributeOrderValidationData with the list of bounds that are already
   * validated.
   *
   * @tparam Index type
   * @param attribute_type Type of the attribute to validate.
   * @param attribute_name Name of the attribute to validate.
   * @param increasing_data Is the order of the data increasing?
   * @param array_non_empty_domain Range storing the array non empty domain.
   * @param non_empty_domains Pointer, per fragment, to the non empty domains.
   * @param frag_first_array_tile_idx First tile index (in full domain), per
   * fragment.
   */
  template <typename IndexType>
  void validate_attribute_order(
      Datatype attribute_type,
      std::string& attribute_name,
      bool increasing_data,
      Range& array_non_empty_domain,
      std::vector<const void*>& non_empty_domains,
      std::vector<uint64_t>& frag_first_array_tile_idx);

  /**
   * Complete order validation after required tiles have been loaded by the
   * reader.
   *
   * @tparam Index type
   * @tparam Attribute type
   * @param attribute_name Name of the attribute to validate.
   * @param increasing_data Is the order of the data increasing?
   * @param non_empty_domains Pointer, per fragment, to the non empty domains.
   * @param frag_first_array_tile_idx First tile index (in full domain), per
   * fragment.
   * @param order_validation_data Order validation data.
   */
  template <typename IndexType, typename AttributeType>
  void validate_attribute_order_with_tile_data(
      std::string& attribute_name,
      bool increasing_data,
      std::vector<const void*>& non_empty_domains,
      std::vector<uint64_t>& frag_first_array_tile_idx,
      AttributeOrderValidationData& order_validation_data);

 private:
  /**
   * @brief Class that stores all the storage needed to keep bitsort
   * metadata.
   * @tparam CmpObject The comparator object being stored.
   */
  template <
      typename CmpObject,
      typename std::enable_if_t<
          std::is_same_v<CmpObject, HilbertCmpQB> ||
          std::is_same_v<CmpObject, GlobalCmpQB>>* = nullptr>
  struct BitSortFilterMetadataStorage;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /**
   * Calculate Hilbert values. Used to pass in a Hilbert
   * comparator to the read-reverse path.
   *
   * @param domain_buffers
   * @param hilbert_values
   */
  void calculate_hilbert_values(
      const DomainBuffersView& domain_buffers,
      std::vector<uint64_t>& hilbert_values) const;

  /**
   * Constructs the bitsort metadata object.
   *
   * @tparam CmpObject The comparator object being constructed.
   * @param tile Fixed tile that is being unfiltered.
   * @param bitsort_storage Storage for all the vectors needed to construct
   * the bitsort filter.
   * @return BitSortFilterMetadataType the constructed argument.
   */

  template <
      typename CmpObject,
      typename std::enable_if_t<
          std::is_same_v<CmpObject, HilbertCmpQB> ||
          std::is_same_v<CmpObject, GlobalCmpQB>>* = nullptr>
  BitSortFilterMetadataType construct_bitsort_filter_argument(
      ResultTile* const tile,
      BitSortFilterMetadataStorage<CmpObject>& bitsort_storage) const;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_READER_BASE_H