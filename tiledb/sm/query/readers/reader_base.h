/**
 * @file   reader_base.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2024 TileDB, Inc.
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
#include "tiledb/common/memory_tracker.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/tile_domain.h"
#include "tiledb/sm/fragment/fragment_metadata.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/query/query_condition.h"
#include "tiledb/sm/query/readers/aggregators/aggregate_buffer.h"
#include "tiledb/sm/query/readers/aggregators/iaggregator.h"
#include "tiledb/sm/query/readers/result_cell_slab.h"
#include "tiledb/sm/query/readers/result_space_tile.h"
#include "tiledb/sm/query/writers/domain_buffer.h"
#include "tiledb/sm/subarray/subarray_partitioner.h"

namespace tiledb::sm {

class Array;
class ArraySchema;
class FilteredData;
class Subarray;

/** Processes read queries. */
class ReaderBase : public StrategyBase {
 public:
  /* ********************************* */
  /*          TYPE DEFINITIONS         */
  /* ********************************* */

  class NameToLoad {
   public:
    /* ********************************* */
    /*          STATIC FUNCTIONS         */
    /* ********************************* */

    /** Return a NameToLoad vector from a string vector. */
    static std::vector<ReaderBase::NameToLoad> from_string_vec(
        const std::vector<std::string>& names_to_load) {
      std::vector<ReaderBase::NameToLoad> ret;
      ret.reserve(names_to_load.size());
      for (auto& name : names_to_load) {
        ret.emplace_back(name);
      }

      return ret;
    }

    /* ********************************* */
    /*     CONSTRUCTORS & DESTRUCTORS    */
    /* ********************************* */

    /** Constructor. */
    NameToLoad(std::string name, bool validity_only = false)
        : name_(name)
        , validity_only_(validity_only) {
    }

    /* ********************************* */
    /*          PUBLIC METHODS           */
    /* ********************************* */

    /** @returns the name to load. */
    const std::string& name() const {
      return name_;
    }

    /** @returns if the field needs to be loaded for validity only. */
    bool validity_only() const {
      return validity_only_;
    }

   private:
    /* ********************************* */
    /*        PRIVATE ATTRIBUTES         */
    /* ********************************* */

    /** Field name to load. */
    const std::string name_;

    /** Load validity only for the field. */
    const bool validity_only_;
  };

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
      stats::Stats* stats, shared_ptr<Logger> logger, StrategyParams& params);

  /** Destructor. */
  ~ReaderBase() = default;

  /* ********************************* */
  /*          STATIC FUNCTIONS         */
  /* ********************************* */

  /**
   * Computes the minimum and maximum indexes of tile chunks to process based on
   * the available threads.
   *
   * @param num_chunks Total number of chunks in a tile
   * @param num_range_threads Total number of range threads.
   * @param range_thread_idx Current range thread index.
   * @return {min, max}
   */
  static tuple<uint64_t, uint64_t> compute_chunk_min_max(
      const uint64_t num_chunks,
      const uint64_t num_range_threads,
      const uint64_t thread_idx) {
    if (num_range_threads == 0) {
      throw std::runtime_error("Number of range thread value is 0");
    }

    if (thread_idx > num_range_threads - 1) {
      throw std::runtime_error(
          "Range thread index is greater than number of range threads");
    }

    if (num_chunks == 0) {
      return {0, 0};
    }

    auto t_part_num = std::min(num_chunks, num_range_threads);
    auto t_min = (thread_idx * num_chunks + t_part_num - 1) / t_part_num;
    auto t_max = std::min(
        ((thread_idx + 1) * num_chunks + t_part_num - 1) / t_part_num,
        num_chunks);

    return {t_min, t_max};
  }

  /* ********************************* */
  /*          PUBLIC METHODS           */
  /* ********************************* */

  /**
   * Check if a field should be skipped for a certain fragment.
   *
   * @param frag_idx Fragment index.
   * @param name Name of the dimension/attribute.
   * @return True if the field should be skipped, false if they shouldn't.
   */
  bool skip_field(const unsigned frag_idx, const std::string& name) const;

 protected:
  /* ********************************* */
  /*       PROTECTED ATTRIBUTES        */
  /* ********************************* */

  /** The query's memory tracker. */
  shared_ptr<MemoryTracker> memory_tracker_;

  /** The query condition. */
  std::optional<QueryCondition>& condition_;

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

  /** Are we doing deletes consolidation (without purge option). */
  bool deletes_consolidation_no_purge_;

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

  /** Are dimensions var sized. */
  std::vector<bool> is_dim_var_size_;

  /** Names of dim/attr loaded for query condition. */
  std::vector<std::string> qc_loaded_attr_names_;

  /** Names of dim/attr loaded for query condition. */
  std::unordered_set<std::string> qc_loaded_attr_names_set_;

  /** Have we loaded the initial data. */
  bool initial_data_loaded_;

  /** The maximum number of bytes in a batched read operation. */
  uint64_t max_batch_size_;

  /** The minimum number of bytes between two read batches. */
  uint64_t min_batch_gap_;

  /** The minimum number of bytes in a batched read operation. */
  uint64_t min_batch_size_;

  /** Default channel aggregates, stored by field name. */
  std::unordered_map<std::string, std::vector<shared_ptr<IAggregator>>>
      aggregates_;

  /**
   * Maps aggregate names to their buffers.
   * */
  std::unordered_map<std::string, QueryBuffer>& aggregate_buffers_;

  /** Per fragment tile offsets memory usage. */
  std::vector<uint64_t> per_frag_tile_offsets_usage_;

  /* ********************************* */
  /*         PROTECTED METHODS         */
  /* ********************************* */

  /** Returns wether the field is for aggregation only or not. */
  bool aggregate_only(const std::string& name) const {
    if (qc_loaded_attr_names_set_.count(name) != 0) {
      return false;
    }

    if (buffers_.count(name) != 0) {
      return false;
    }

    return true;
  }

  /** Returns wether the field is for null count aggregation only or not. */
  bool null_count_aggregate_only(const std::string& name) const {
    if (!aggregate_only(name)) {
      return false;
    }

    auto& aggregates = aggregates_.at(name);
    for (auto& aggregate : aggregates) {
      if (!aggregate->aggregation_validity_only()) {
        return false;
      }
    }

    return true;
  }

  /**
   * Computes the required size for loading tile offsets, per fragments.
   *
   * @return Required memory for loading tile offsets, per fragments.
   */
  std::vector<uint64_t> tile_offset_sizes();

  /**
   * Returns if we need to process partial timestamp condition for this
   * fragment.
   *
   * @param frag_meta Fragment metadata.
   * @return true if the condition need to be processed.
   */
  bool process_partial_timestamps(FragmentMetadata& frag_meta) const;

  /**
   * Deletes the tiles on the input field from the result tiles.
   *
   * @param name The field name.
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

  /**
   * Correctness checks for `subarray_`.
   *
   * @param check_ranges_oob If true, checks subarray ranges are within domain
   * bounds for the array. If false, only basic checks are performed.
   */
  void check_subarray(bool check_ranges_oob = false) const;

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
   * @return True if timestamps should be included, false if they are not
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
   * Loads tile offsets for each field name into
   * their associated element in `fragment_metadata_`.
   *
   * @param relevant_fragments List of relevant fragments.
   * @param names The field names.
   * @return Status
   */
  void load_tile_offsets(
      const RelevantFragments& relevant_fragments,
      const std::vector<std::string>& names);

  /**
   * Checks if at least one fragment overlaps partially with the
   * time at which the read is taking place.
   *
   * @param subarray Subarray to use.
   * @return True if at least one fragment partially overlaps.
   */
  bool partial_consolidated_fragment_overlap(Subarray& subarray) const;

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
   * Loads tile var sizes for each field name into
   * their associated element in `fragment_metadata_`.
   *
   * @param relevant_fragments List of relevant fragments.
   * @param names The field names.
   */
  void load_tile_var_sizes(
      const RelevantFragments& relevant_fragments,
      const std::vector<std::string>& names);

  /*
   * Loads tile metadata for each field name into
   * their associated element in `fragment_metadata_`. This is done for
   * attributes with aggregates.
   *
   * @param relevant_fragments List of relevant fragments.
   * @param names The field names.
   */
  void load_tile_metadata(
      const RelevantFragments& relevant_fragments,
      const std::vector<std::string>& names);

  /**
   * Loads processed conditions from fragment metadata.
   *
   * @param subarray The subarray to load processed conditions for.
   * @return Status
   */
  void load_processed_conditions();

  /**
   * Read and unfilter attribute tiles.
   *
   * @param names The attribute names.
   * @param result_tiles The retrieved tiles will be stored inside the
   *     `ResultTile` instances in this vector.
   * @return Status.
   */
  Status read_and_unfilter_attribute_tiles(
      const std::vector<NameToLoad>& names,
      const std::vector<ResultTile*>& result_tiles);

  /**
   * Read and unfilter coordinate tiles.
   *
   * @param names The coordinate/dimension names.
   * @param result_tiles The retrieved tiles will be stored inside the
   *     `ResultTile` instances in this vector.
   * @return Status.
   */
  Status read_and_unfilter_coordinate_tiles(
      const std::vector<std::string>& names,
      const std::vector<ResultTile*>& result_tiles);

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
   * @return Filtered data blocks.
   */
  void read_attribute_tiles(
      const std::vector<NameToLoad>& names,
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
   * @return Filtered data blocks.
   */
  void read_coordinate_tiles(
      const std::vector<std::string>& names,
      const std::vector<ResultTile*>& result_tiles) const;

  /**
   * Retrieves the tiles on a list of attribute or dimension and stores it
   * in the appropriate result tile.
   *
   * Concurrently executes across each name in `names` and each result tile
   * in 'result_tiles'.
   *
   * @param names The field names.
   * @param result_tiles The retrieved tiles will be stored inside the
   *     `ResultTile` instances in this vector.
   * @param validity_only Is the field read for validity only.
   * @return Filtered data blocks.
   */
  void read_tiles(
      const std::vector<NameToLoad>& names,
      const std::vector<ResultTile*>& result_tiles) const;

  /**
   * Filters the tiles on a particular field from all input
   * fragments based on the tile info in `result_tiles`.
   *
   * @param name Field whose tiles will be unfiltered.
   * @param validity_only Unfilter for the validity tile only?
   * @param result_tiles Vector containing the tiles to be unfiltered.
   * @return Status
   */
  Status unfilter_tiles(
      const std::string& name,
      const bool validity_only,
      const std::vector<ResultTile*>& result_tiles);

  /**
   * Unfilter a specific range of chunks in tile
   *
   * @param name field the tile belong to.
   * @param validity_only Unfilter the validity tile only.
   * @param tile Offsets tile to be unfiltered.
   * @param var_size True if the field is var-sized, false
   * otherwise
   * @param nullable True if the field is nullable, false
   * otherwise
   * @param range_thread_idx Current range thread index.
   * @param num_range_threads Total number of range threads.
   * @param tile_chunk_data Tile chunk info, buffers and offsets
   * @param tile_var_chunk_data Value tile chunk info, buffers and offsets
   * @param tile_validity_chunk_data Validity tile chunk info, buffers and
   * offsets
   * @return Status
   */
  Status unfilter_tile(
      const std::string& name,
      const bool validity_only,
      ResultTile* const tile,
      const bool var_size,
      const bool nullable,
      uint64_t range_thread_idx,
      uint64_t num_range_threads,
      const ChunkData& tile_chunk_data,
      const ChunkData& tile_chunk_var_data,
      const ChunkData& tile_chunk_validity_data) const;

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
      const std::string& name, unsigned f, uint64_t t) const;

  /**
   * Get the on disk size of an attribute tile.
   *
   * @param name The attribute name.
   * @param f The fragment idx.
   * @param t The tile idx.
   * @return Tile size.
   */
  uint64_t get_attribute_persisted_tile_size(
      const std::string& name, unsigned f, uint64_t t) const;

  /**
   * Computes the tile domains based on the current partition.
   *
   * @tparam T The domain datatype.
   * @param partitioner_subarray The partitioner subarray.
   * @return array tile domain, fragments tile domains.
   */
  template <class T>
  std::pair<TileDomain<T>, std::vector<TileDomain<T>>> compute_tile_domains(
      const Subarray& partitioner_subarray) const;

  /** Returns `true` if the coordinates are included in the attributes. */
  bool has_coords() const;

  /**
   * If the tile stores coordinates, zip them. Note that format version < 2 only
   * split the coordinates when compression was used. See
   * https://github.com/TileDB-Inc/TileDB/issues/1053. For format version > 4,
   * a tile never stores coordinates
   *
   * @param name field the tile belongs to.
   * @param tile Tile to zip the coordinates if needed.
   * @return Status
   */
  Status zip_tile_coordinates(const std::string& name, Tile* tile) const;

  /**
   * Reads the chunk data of all tile buffers and stores them in a data
   * structure together with the offsets between them
   *
   * @param name field the tile belong to.
   * @param validity_only Is the field read for validity only.
   * @param tile Offsets tile to be unfiltered.
   * @param var_size True if the field is var-sized, false
   * otherwise
   * @param nullable True if the field is nullable, false
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
      const bool validity_only,
      ResultTile* const tile,
      const bool var_size,
      const bool nullable,
      ChunkData& tile_chunk_data,
      ChunkData& tile_chunk_var_data,
      ChunkData& tile_chunk_validity_data) const;

  /**
   * Perform some necessary post-processing on a tile that was just unfiiltered
   *
   * @param name field the tile belong to.
   * @param validity_only Is the field read for validity only.
   * @param tile Offsets tile that was just unfiltered.
   * @param var_size True if the field is var-sized, false
   * otherwise
   * @param nullable True if the field is nullable, false
   * otherwise
   * @param unfiltered_tile_size Size of the unfiltered tile buffer
   * @param unfiltered_tile_var_size Size of the unfiltered tile_var buffer
   * @param unfiltered_tile_validity_size Size of the unfiltered tile
   * validity buffer
   * @return Status
   */
  Status post_process_unfiltered_tile(
      const std::string& name,
      const bool validity_only,
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
   * Validates the attribute order for all loaded fragments.
   *
   * Throws an error if the there is a gap between fragments or the attribute
   * order between fragments is not maintained.
   *
   * @tparam Index type
   * @tparam Attribute type
   * @param attribute_name Name of the attribute to validate.
   * @param increasing_data Is the order of the data increasing?
   * @param array_non_empty_domain Range storing the array non empty domain.
   * @param non_empty_domains Pointer, per fragment, to the non empty domains.
   * @param frag_first_array_tile_idx First tile index (in full domain), per
   * fragment.
   */
  template <typename IndexType, typename AttributeType>
  void validate_attribute_order(
      std::string& attribute_name,
      bool increasing_data,
      Range& array_non_empty_domain,
      std::vector<const void*>& non_empty_domains,
      std::vector<uint64_t>& frag_first_array_tile_idx);

  /**
   * Validates the attribute order for all loaded fragments.
   *
   * Throws an error if the there is a gap between fragments or the attribute
   * order between fragments is not maintained.
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
};

}  // namespace tiledb::sm

#endif  // TILEDB_READER_BASE_H
