/**
 * @file   ordered_dim_label_reader.h
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
 * This file defines class OrderedDimLabelReader.
 */

#ifndef TILEDB_ORDERED_DIM_LABEL_READER
#define TILEDB_ORDERED_DIM_LABEL_READER

#include <atomic>

#include "tiledb/common/common.h"
#include "tiledb/common/logger_public.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/query/iquery_strategy.h"
#include "tiledb/sm/query/query_buffer.h"
#include "tiledb/sm/query/readers/reader_base.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class Array;
class StorageManager;

/** Processes dense read queries. */
class OrderedDimLabelReader : public ReaderBase, public IQueryStrategy {
 public:
  /**
   * Enum used to qualify the tile index found for a range. Tne index could be
   * contained, meaning that the searched value was contained in the found tile
   * index, greater than, meaning that the searched value should be found after
   * the tile, or less than meaning the value should be found before the tile.
   */
  enum class IndexValueType : uint8_t {
    CONTAINED,
    GT,
    LT,
  };

  /**
   * Class to store range data for a range inside of a particular fragment.
   * This will store the index of the start value, with the index value type
   * qualifier and the same for the end value.
   */
  class FragmentRangeTileIndexes {
   public:
    /* ********************************* */
    /*     CONSTRUCTORS & DESTRUCTORS    */
    /* ********************************* */
    FragmentRangeTileIndexes() = default;

    FragmentRangeTileIndexes(
        uint64_t start_idx,
        IndexValueType start_value_type,
        uint64_t end_idx,
        IndexValueType end_value_type)
        : indexes_({start_idx, end_idx})
        , value_types_({start_value_type, end_value_type}) {
    }

    /* ********************************* */
    /*                 API               */
    /* ********************************* */

    /** Returns the start tile index. */
    inline uint64_t idx(uint8_t range_index) {
      return indexes_[range_index];
    }

    /** Index value type for the start index. */
    inline IndexValueType val_type(uint8_t range_index) {
      return value_types_[range_index];
    }

   private:
    /* ********************************* */
    /*         PRIVATE ATTRIBUTES        */
    /* ********************************* */

    /** Stores the tile indexes. */
    std::vector<uint64_t> indexes_;

    /**
     * Stores if the indexes values are an equal, greater than or less than
     * values.
     */
    std::vector<IndexValueType> value_types_;
  };

  /**
   * Class storing the min/max tile indexes required to find a specific values
   * of a range. This contains the min/max for the start and end value of the
   * range.
   */
  class RangeTileIndexes {
   public:
    /* ********************************* */
    /*     CONSTRUCTORS & DESTRUCTORS    */
    /* ********************************* */
    RangeTileIndexes() = default;

    RangeTileIndexes(
        uint64_t tile_idx_min,
        uint64_t tile_idx_max,
        std::vector<FragmentRangeTileIndexes>& per_range_array_tile_indexes) {
      // Uses the values computed per fragments to compute the min/max.
      std::array<uint64_t, 2> min = {std::numeric_limits<uint64_t>::max(),
                                     std::numeric_limits<uint64_t>::max()};
      std::array<uint64_t, 2> max = {std::numeric_limits<uint64_t>::min(),
                                     std::numeric_limits<uint64_t>::min()};

      // While processing the tiles that we know to possibly contain the
      // searched values (IndexValueType::EQUAL), also compute the minimum
      // and maximum values that we see that hint where we can find the values
      // (IndexValueType::GT or IndexValueType::LT). When a searched value is
      // contained between two tiles, that is all we will have to find the
      // values.
      std::array<uint64_t, 2> lt = {tile_idx_max, tile_idx_max};
      std::array<uint64_t, 2> gt = {tile_idx_min, tile_idx_min};

      // Go through all fragments.
      for (unsigned f = 0; f < per_range_array_tile_indexes.size(); f++) {
        for (uint8_t range_index = 0; range_index < 2; range_index++) {
          auto idx = per_range_array_tile_indexes[f].idx(range_index);
          auto val_type = per_range_array_tile_indexes[f].val_type(range_index);

          // Expand the range of min/max when we see a tile that we know
          // contains the value.
          if (val_type == IndexValueType::CONTAINED) {
            min[range_index] = std::min(idx, min[range_index]);
            max[range_index] = std::max(idx, max[range_index]);
          } else if (val_type == IndexValueType::GT) {
            // Save the last tile that we know to be greater then the start.
            gt[range_index] = std::max(idx, gt[range_index]);
          } else {
            // Save the first tile that we know to be less than the start.
            lt[range_index] = std::min(idx, lt[range_index]);
          }
        }
      }

      for (uint8_t range_index = 0; range_index < 2; range_index++) {
        // No tiles had an equal value type, use the hints to set a min/max.
        if (min[range_index] == std::numeric_limits<uint64_t>::max()) {
          min[range_index] = gt[range_index];
          max[range_index] = lt[range_index];
        }
      }

      per_range_array_tile_indexes_ = {{{min[0], max[0]}, {min[1], max[1]}}};
    }

    /* ********************************* */
    /*                 API               */
    /* ********************************* */

    /** Returns the minimum tile index. */
    inline uint64_t min(uint8_t range_index) {
      return per_range_array_tile_indexes_[range_index].first;
    }

    /** Returns the maximum tile index. */
    inline uint64_t max(uint8_t range_index) {
      return per_range_array_tile_indexes_[range_index].second;
    }

   private:
    /* ********************************* */
    /*         PRIVATE ATTRIBUTES        */
    /* ********************************* */

    /**
     * Tile indexes vector. Stores the start/end ranges as a pair of min/max.
     */
    std::array<std::pair<uint64_t, uint64_t>, 2> per_range_array_tile_indexes_;
  };

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  OrderedDimLabelReader(
      stats::Stats* stats,
      shared_ptr<Logger> logger,
      StorageManager* storage_manager,
      Array* array,
      Config& config,
      std::unordered_map<std::string, QueryBuffer>& buffers,
      Subarray& subarray,
      Layout layout,
      QueryCondition& condition,
      bool increasing_order,
      bool skip_checks_serialization);

  /** Destructor. */
  ~OrderedDimLabelReader() = default;

  DISABLE_COPY_AND_COPY_ASSIGN(OrderedDimLabelReader);
  DISABLE_MOVE_AND_MOVE_ASSIGN(OrderedDimLabelReader);

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /** Finalizes the reader. */
  Status finalize() {
    return Status::Ok();
  }

  /**
   * Returns `true` if the query was incomplete, i.e., if all subarray
   * partitions in the read state have not been processed or there
   * was some buffer overflow.
   */
  bool incomplete() const;

  /** Returns the status details reason. */
  QueryStatusDetailsReason status_incomplete_reason() const;

  /** Initialize the memory budget variables. */
  Status initialize_memory_budget();

  /** Performs a read query using its set members. */
  Status dowork();

  /** Resets the reader object. */
  void reset();

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** UID of the logger instance */
  inline static std::atomic<uint64_t> logger_id_ = 0;

  /** Ranges to be retreived for the attribute. */
  std::vector<Range> ranges_;

  /** Stores the label attribute name. */
  std::string label_name_;

  /** Stores the dimension label type. */
  Datatype label_type_;

  /** Stores if the dimension label is var sized. */
  bool label_var_size_;

  /** Are the labels increasing? */
  bool increasing_labels_;

  /** Stores a pointer to the index dimension. */
  const Dimension* index_dim_;

  /** Save pointers to the non empty domain for each fragments. */
  std::vector<const void*> non_empty_domains_;

  /** Non empty domain for the array. */
  Range non_empty_domain_;

  /** Minimum tile index in the full domain. */
  uint64_t tile_idx_min_;

  /** Maximum tile index in the full domain. */
  uint64_t tile_idx_max_;

  /**
   * Per fragment map to hold the result tiles. The key for the maps is the
   * tile index.
   */
  std::vector<std::unordered_map<uint64_t, OrderedDimLabelResultTile>>
      result_tiles_;

  /**
   * Tile index (inside of the domain) of the first tile of each fragments.
   */
  std::vector<uint64_t> domain_tile_idx_;

  /**
   * Stores the tile indexes (min/max) that can potentially contain the label
   * value for each range start/end.
   */
  std::vector<RangeTileIndexes> per_range_array_tile_indexes_;

  /** Total memory budget. */
  uint64_t memory_budget_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /** Performs a read on an ordered label array. */
  void label_read();

  /**
   * Performs a read on an ordered label array.
   *
   * @tparam The index type.
   */
  template <typename IndexType>
  void label_read();

  /**
   * Compute the non empty domain for the index dimension. Also caches the non
   * empty domain of each fragments for future use and saves the non empty
   * domain in tile indexes in `tile_idx_min_` and `tile_idx_max_`.
   *
   * @tparam The index type.
   */
  template <typename IndexType>
  void compute_non_empty_domain();

  /**
   * Compute the tile indexes (min/max) that can potentially contain thev label
   * value for each range start/end.
   *
   * Also compute the tile index (inside of the full dimension domain) of the
   * first tile of each fragments.
   *
   * @tparam The index type.
   */
  template <typename IndexType>
  void compute_array_tile_indexes_for_ranges();

  /** Load the tile min max values for the index attribute. */
  void load_label_min_max_values();

  /**
   * Template specialization of the 'get_array_tile_indexes_for_range' function.
   *
   * @tparam The label type.
   * @param f Fragment to get the tile indexes for.
   * @param r Range to get the tile indexes for.
   *
   * @return Pair of the min/max index ranges.
   */
  template <typename LabelType>
  FragmentRangeTileIndexes get_array_tile_indexes_for_range(
      unsigned f, uint64_t r);

  /**
   * Get tile indexes (which tile should contain the desired range) for a
   * specific range/fragment.
   *
   * @param f Fragment to get the tile indexes for.
   * @param r Range to get the tile indexes for.
   *
   * @return FragmentRangeTileIndexes.
   */
  FragmentRangeTileIndexes get_array_tile_indexes_for_range(
      unsigned f, uint64_t r);

  /**
   * Returns the tile size of a particular label tile.
   *
   * @param f Fragment index.
   * @param t Tile index.
   * @return Tile size.
   */
  uint64_t label_tile_size(unsigned f, uint64_t t) const;

  /**
   * Returns if a particular tile is covered by a later fragment.
   *
   * @tparam Index type.
   * @param frag_idx Fragment index.
   * @param tile_idx Tile index.
   * @param domain_low Lowest possible value for the index domain.
   * @param tile_extent Tile extent.
   * @return 'true' if the tile is covered by a later fragment.
   */
  template <typename IndexType>
  bool tile_overwritten(
      unsigned frag_idx,
      uint64_t tile_idx,
      const IndexType& domain_low,
      const IndexType& tile_extent) const;

  /**
   * Creates the result tiles required to do the range to index search.
   *
   * @tparam Index type.
   * @return Max range to process.
   */
  template <typename IndexType>
  uint64_t create_result_tiles();

  /**
   * Get the fixed label value in a specific fragment/tile/cell.
   *
   * @tparam Label type.
   * @param f Fragment to get the value from.
   * @param tile_idx Tile index.
   * @param cell_idx Cell index.
   * @return Label value.
   */
  template <typename LabelType>
  LabelType get_label_value(
      const unsigned f, const uint64_t tile_idx, const uint64_t cell_idx);

  /**
   * Get the fixed label value at a specific index.
   *
   * @tparam Index type.
   * @tparam Label type.
   * @param index Index value.
   * @param domain_low Lowest possible value for the index domain.
   * @param tile_extent Tile extent.
   * @return Label value.
   */
  template <typename IndexType, typename LabelType>
  LabelType get_value_at(
      const IndexType& index,
      const IndexType& domain_low,
      const IndexType& tile_extent);

  /**
   * Get a range value.
   *
   * @tparam Label type.
   * @param r Range to get the value for.
   * @param range_index Range index (start/end).
   * @return Label value.
   */
  template <typename LabelType>
  LabelType get_range_as(uint64_t r, uint8_t range_index);

  /**
   * Search for a label defined in the specified range for fixed size labels.
   *
   * @tparam Index type.
   * @tparam Label type.
   * @tparam Op type (std::greater for increasing and std::less for
   * decreasing).
   * @param r Range to process.
   * @param range_index Range index (start or end).
   * @param domain_low Lowest possible value for the index domain.
   * @param tile_extent Tile extent.
   * @return Index for the searched label.
   */
  template <typename IndexType, typename LabelType, typename Op>
  IndexType search_for_range_fixed(
      uint64_t r,
      uint8_t range_index,
      const IndexType& domain_low,
      const IndexType& tile_extent);

  /**
   * Compute the indexes value for a fixed label range.
   *
   * @tparam Index type.
   * @tparam Label type.
   * @param dest Destination inside of the user buffers.
   * @param r Range to compute indexes for.
   */
  template <typename IndexType, typename LabelType>
  void compute_and_copy_range_indexes(IndexType* dest, uint64_t r);

  /**
   * Compute and copy the range indexes for a specific range.
   *
   * @tparam Index type.
   * @param buffer_offset Current offset into the user buffer.
   * @param r Range index to compute/copy.
   */
  template <typename IndexType>
  void compute_and_copy_range_indexes(uint64_t buffer_offset, uint64_t r);
};  // namespace sm

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_ORDERED_DIM_LABEL_READER
