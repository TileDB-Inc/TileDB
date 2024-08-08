/**
 * @file   dense_reader.h
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
 * This file defines class DenseReader.
 */

#ifndef TILEDB_DENSE_READER
#define TILEDB_DENSE_READER

#include <atomic>

#include "tiledb/common/common.h"
#include "tiledb/common/pmr.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/query/iquery_strategy.h"
#include "tiledb/sm/query/query_buffer.h"
#include "tiledb/sm/query/readers/reader_base.h"
#include "tiledb/sm/subarray/tile_cell_slab_iter.h"

using namespace tiledb::common;

namespace tiledb::sm {

class Array;

/** Processes dense read queries. */
class DenseReader : public ReaderBase, public IQueryStrategy {
  /**
   * Class that stores the tile data for an internal loop of the reader.
   */
  template <class DimType>
  class IterationTileData {
   public:
    /* ********************************* */
    /*     CONSTRUCTORS & DESTRUCTORS    */
    /* ********************************* */
    IterationTileData() = delete;

    IterationTileData(
        ThreadPool& compute_tp,
        Subarray& subarray,
        uint64_t t_start,
        uint64_t t_end,
        std::map<const DimType*, ResultSpaceTile<DimType>>&& result_space_tiles,
        shared_ptr<MemoryTracker> memory_tracker)
        : t_start_(t_start)
        , t_end_(t_end)
        , memory_tracker_(memory_tracker)
        , tile_subarrays_(
              t_end - t_start,
              subarray.dim_num(),
              memory_tracker_->get_resource(MemoryType::DENSE_TILE_SUBARRAY))
        , result_space_tiles_(std::move(result_space_tiles)) {
      auto& tile_coords = subarray.tile_coords();
      throw_if_not_ok(
          parallel_for(&compute_tp, 0, tile_subarrays_.size(), [&](uint64_t t) {
            subarray.crop_to_tile(
                tile_subarrays_[t],
                (const DimType*)&tile_coords[t + t_start][0]);
            return Status::Ok();
          }));
    };

    DISABLE_COPY_AND_COPY_ASSIGN(IterationTileData);
    DISABLE_MOVE_AND_MOVE_ASSIGN(IterationTileData);

    /* ********************************* */
    /*          PUBLIC METHODS           */
    /* ********************************* */

    /** Returns the tile start index. */
    inline uint64_t t_start() {
      return t_start_;
    }

    /** Returns the tile end index. */
    inline uint64_t& t_end() {
      return t_end_;
    }

    /** Returns the tile subarrays. */
    inline DenseTileSubarray<DimType>& tile_subarray(uint64_t t) {
      return tile_subarrays_[t - t_start_];
    }

    /** Returns the result space tiles. */
    inline std::map<const DimType*, ResultSpaceTile<DimType>>&
    result_space_tiles() {
      return result_space_tiles_;
    }

   private:
    /* ********************************* */
    /*         PRIVATE ATTRIBUTES        */
    /* ********************************* */

    /** Start tile to process. */
    uint64_t t_start_;

    /** End tile to process. */
    uint64_t t_end_;

    /** Memory tracker. */
    shared_ptr<MemoryTracker> memory_tracker_;

    /** Tile subarrays. */
    tdb::pmr::vector<DenseTileSubarray<DimType>> tile_subarrays_;

    /** Result space tiles. */
    std::map<const DimType*, ResultSpaceTile<DimType>> result_space_tiles_;
  };

 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  DenseReader(
      stats::Stats* stats,
      shared_ptr<Logger> logger,
      StrategyParams& params,
      bool remote_query = false);

  /** Destructor. */
  ~DenseReader() = default;

  DISABLE_COPY_AND_COPY_ASSIGN(DenseReader);
  DISABLE_MOVE_AND_MOVE_ASSIGN(DenseReader);

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
  void refresh_config();

  /** Returns the current read state. */
  const ReadState* read_state() const;

  /** Returns the current read state. */
  ReadState* read_state();

  /** Performs a read query using its set members. */
  Status dowork();

  /** Resets the reader object. */
  void reset();

  /** Returns the name of the strategy */
  std::string name();

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** UID of the logger instance */
  inline static std::atomic<uint64_t> logger_id_ = 0;

  /** Read state. */
  ReadState read_state_;

  /** Are we in elements mode. */
  bool elements_mode_;

  /**
   * Reading mode that only sets the coordinates of the cells that match the
   * query condition.
   */
  bool qc_coords_mode_;

  /** Total memory budget. */
  uint64_t memory_budget_;

  /** Total memory budget if overridden by the query. */
  optional<uint64_t> memory_budget_from_query_;

  /** Target upper memory limit for tiles. */
  uint64_t tile_upper_memory_limit_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /** Performs a read on a dense array. */
  template <class OffType>
  Status dense_read();

  /** Performs a read on a dense array. */
  template <class DimType, class OffType>
  Status dense_read();

  /** Initializes the read state. */
  void init_read_state();

  /**
   * Returns the field name to process.
   *
   * @param condition_names Set of query condition names.
   * @return {names, var names}.
   */
  tuple<std::vector<std::string>, std::vector<std::string>>
  field_names_to_process(
      const std::unordered_set<std::string>& condition_names);

  /**
   * Computes the result space tiles based on the current partition.
   *
   * @tparam T The domain datatype.
   * @param t_start The start tile index in the tile coords.
   * @param names The fields to process.
   * @param condition_names The fields in the query condition.
   * @param subarray The input subarray.
   * @param tiles_cell_num The cell num for all tiles.
   * @param array_tile_domain The array tile domain.
   * @param frag_tile_domains The relevant fragments tile domains.
   * @param iteration_tile_data The iteration data.
   *
   * @return wait_compute_task_before_read, result_space_tiles.
   */
  template <class DimType>
  tuple<bool, std::map<const DimType*, ResultSpaceTile<DimType>>>
  compute_result_space_tiles(
      const uint64_t t_start,
      const std::vector<std::string>& names,
      const std::unordered_set<std::string>& condition_names,
      const Subarray& subarray,
      const std::vector<uint64_t>& tiles_cell_num,
      const TileDomain<DimType>& array_tile_domain,
      const std::vector<TileDomain<DimType>>& frag_tile_domains) const;

  /** Compute the result tiles to load for a name. */
  template <class DimType>
  std::vector<ResultTile*> result_tiles_to_load(
      const optional<std::string> name,
      const std::unordered_set<std::string>& condition_names,
      const Subarray& subarray,
      shared_ptr<IterationTileData<DimType>> iteration_tile_data,
      const std::vector<uint64_t>& tiles_cell_num) const;

  /** Apply the query condition. */
  template <class DimType, class OffType>
  Status apply_query_condition(
      ThreadPool::Task& compute_task,
      Subarray& subarray,
      const std::unordered_set<std::string>& condition_names,
      const std::vector<DimType>& tile_extents,
      const std::vector<uint64_t>& tiles_cell_num,
      std::vector<uint64_t>& tile_offsets,
      const std::vector<RangeInfo<DimType>>& range_info,
      shared_ptr<IterationTileData<DimType>> iteration_tile_data,
      const uint64_t num_range_threads,
      std::vector<uint8_t>& qc_result);

  /** Fix offsets buffer after reading all offsets. */
  template <class OffType>
  void fix_offsets_buffer(
      const std::string& name,
      const bool nullable,
      const uint64_t subarray_start_cell,
      const uint64_t subarray_end_cell,
      uint64_t& var_buffer_size,
      std::vector<void*>& var_data);

  /** Copy attribute into the users buffers. */
  template <class DimType, class OffType>
  Status copy_attribute(
      const std::string& name,
      const std::vector<DimType>& tile_extents,
      const Subarray& subarray,
      const uint64_t subarray_start_cell,
      const uint64_t subarray_end_cell,
      const std::vector<uint64_t>& tile_offsets,
      uint64_t& var_buffer_size,
      const std::vector<RangeInfo<DimType>>& range_info,
      shared_ptr<IterationTileData<DimType>> iteration_tile_data,
      const std::vector<uint8_t>& qc_result,
      const uint64_t num_range_threads);

  /** Make an aggregate buffer. */
  template <class DimType>
  AggregateBuffer make_aggregate_buffer(
      const bool var_sized,
      const bool nullable,
      const bool is_dim,
      DimType& dim_val,
      const uint64_t cell_size,
      const uint64_t min_cell,
      const uint64_t max_cell,
      ResultTile::TileTuple* tile_tuple,
      optional<void*> bitmap_data);

  /**
   * Returns wether or not we can aggregate the tile with only the fragment
   * metadata.
   *
   * @param name Name of the field to process.
   * @param rst Result space tile.
   * @param tile_cell_num Tile cell num.
   * @return If we can do the aggregation with the frag md or not.
   */
  template <class DimType>
  inline bool can_aggregate_tile_with_frag_md(
      const std::string& name,
      ResultSpaceTile<DimType>& rst,
      const uint64_t tile_cell_num) const {
    if (array_schema_.is_dim(name)) {
      return false;
    }

    // Make sure there are no filtered results by the query condition and that
    // there are only one fragment domain for this tile. Having more fragment
    // domains for a tile means we'll have to merge data for many sources so we
    // cannot aggregate the full tile.
    if (rst.qc_filtered_results() || rst.frag_domains().size() != 1) {
      return false;
    }

    // Now we can get the fragment metadata of the only result tile in this
    // space tile.
    const auto& rt = rst.single_result_tile();
    const auto frag_md = fragment_metadata_[rt.frag_idx()];

    // Make sure this tile isn't cropped by ranges and the fragment metadata has
    // tile metadata.
    if (tile_cell_num != rt.cell_num() || !frag_md->has_tile_metadata()) {
      return false;
    }

    // Fixed size nullable strings had incorrect min/max metadata for dense
    // until version 20.
    const auto type = array_schema_.type(name);
    if ((type == Datatype::STRING_ASCII || type == Datatype::CHAR) &&
        array_schema_.cell_val_num(name) != constants::var_num &&
        array_schema_.is_nullable(name)) {
      if (frag_md->version() <= 20) {
        return false;
      }
    }

    return true;
  }

  /** Process aggregates for a given field. */
  template <class DimType, class OffType>
  Status process_aggregates(
      const std::string& name,
      const std::vector<DimType>& tile_extents,
      const Subarray& subarray,
      const std::vector<uint64_t>& tiles_cell_num,
      const std::vector<uint64_t>& tile_offsets,
      const std::vector<RangeInfo<DimType>>& range_info,
      shared_ptr<IterationTileData<DimType>> iteration_tile_data,
      const std::vector<uint8_t>& qc_result,
      const uint64_t num_range_threads);

  /**
   * Checks if a cell slab overlaps a fragment domain range and returns the
   * start and end of the overlap.
   */
  template <class DimType>
  tuple<bool, uint64_t, uint64_t> cell_slab_overlaps_range(
      const unsigned dim_num,
      const NDRange& ndrange,
      const std::vector<DimType>& coords,
      const uint64_t length);

  /** Copy fixed tiles to the output buffers. */
  template <class DimType>
  Status copy_fixed_tiles(
      const std::string& name,
      const std::vector<DimType>& tile_extents,
      ResultSpaceTile<DimType>& result_space_tile,
      const Subarray& subarray,
      const DenseTileSubarray<DimType>& tile_subarray,
      const uint64_t global_cell_offset,
      const std::vector<RangeInfo<DimType>>& range_info,
      const std::vector<uint8_t>& qc_result,
      const uint64_t range_thread_idx,
      const uint64_t num_range_threads);

  /** Copy a tile var offsets to the output buffers. */
  template <class DimType, class OffType>
  Status copy_offset_tiles(
      const std::string& name,
      const std::vector<DimType>& tile_extents,
      ResultSpaceTile<DimType>& result_space_tile,
      const Subarray& subarray,
      const DenseTileSubarray<DimType>& tile_subarray,
      const uint64_t subarray_start_cell,
      const uint64_t global_cell_offset,
      std::vector<void*>& var_data,
      const std::vector<RangeInfo<DimType>>& range_info,
      const std::vector<uint8_t>& qc_result,
      const uint64_t range_thread_idx,
      const uint64_t num_range_threads);

  /** Copy a var tile to the output buffers. */
  template <class DimType, class OffType>
  Status copy_var_tiles(
      const std::string& name,
      const std::vector<DimType>& tile_extents,
      ResultSpaceTile<DimType>& result_space_tile,
      const Subarray& subarray,
      const DenseTileSubarray<DimType>& tile_subarray,
      const uint64_t subarray_start_cell,
      const uint64_t global_cell_offset,
      std::vector<void*>& var_data,
      const std::vector<RangeInfo<DimType>>& range_info,
      bool last_tile,
      uint64_t var_buffer_sizes,
      const uint64_t range_thread_idx,
      const uint64_t num_range_threads);

  /** Aggregate tiles. */
  template <class DimType>
  Status aggregate_tiles(
      const std::string& name,
      const std::vector<DimType>& tile_extents,
      ResultSpaceTile<DimType>& result_space_tile,
      const Subarray& subarray,
      const DenseTileSubarray<DimType>& tile_subarray,
      const uint64_t global_cell_offset,
      const std::vector<RangeInfo<DimType>>& range_info,
      const std::vector<uint8_t>& qc_result,
      const uint64_t range_thread_idx,
      const uint64_t num_range_threads);

  /**
   * Adds an extra offset in the end of the offsets buffer indicating the
   * returned data size if an attribute is var-sized.
   */
  Status add_extra_offset();

  /** Perform necessary checks before exiting a read loop */
  Status complete_read_loop();

  /**
   * Fills the coordinate buffer with coordinates. Applicable only to dense
   * arrays when the user explicitly requests the coordinates to be
   * materialized.
   *
   * @tparam T Domain type.
   * @param subarray Input subarray.
   * @param qc_results Results from the query condition.
   */
  template <class T>
  void fill_dense_coords(
      const Subarray& subarray,
      const optional<std::vector<uint8_t>> qc_results);

  /**
   * Fills the coordinate buffers with coordinates. Applicable only to dense
   * arrays when the user explicitly requests the coordinates to be
   * materialized. Also applicable only to global order.
   *
   * @tparam T Domain type.
   * @param subarray Input subarray.
   * @param qc_results Results from the query condition.
   * @param qc_results_index Current index in the `qc_results` buffer.
   * @param dim_idx Dimension indices of the corresponding `buffers`.
   *     For the special zipped coordinates, `dim_idx`, `buffers` and `offsets`
   *     contain a single element and `dim_idx` contains `dim_num` as the
   *     dimension index.
   * @param buffers Buffers to copy from. It could be the special zipped
   *     coordinates or separate coordinate buffers.
   * @param offsets Offsets that will be used eventually to update the buffer
   *     sizes, determining the useful results written in the buffers.
   */
  template <class T>
  void fill_dense_coords_global(
      const Subarray& subarray,
      const optional<std::vector<uint8_t>> qc_results,
      uint64_t& qc_results_index,
      const std::vector<unsigned>& dim_idx,
      const std::vector<QueryBuffer*>& buffers,
      std::vector<uint64_t>& offsets);

  /**
   * Fills the coordinate buffers with coordinates. Applicable only to dense
   * arrays when the user explicitly requests the coordinates to be
   * materialized. Also applicable only to row-/col-major order.
   *
   * @tparam T Domain type.
   * @param subarray Input subarray.
   * @param qc_results Results from the query condition.
   * @param qc_results_index Current index in the `qc_results` buffer.
   * @param dim_idx Dimension indices of the corresponding `buffers`.
   *     For the special zipped coordinates, `dim_idx`, `buffers` and `offsets`
   *     contain a single element and `dim_idx` contains `dim_num` as
   *     the dimension index.
   * @param buffers Buffers to copy from. It could be the special zipped
   *     coordinates or separate coordinate buffers.
   * @param offsets Offsets that will be used eventually to update the buffer
   *     sizes, determining the useful results written in the buffers.
   */
  template <class T>
  void fill_dense_coords_row_col(
      const Subarray& subarray,
      const optional<std::vector<uint8_t>> qc_results,
      uint64_t& qc_results_index,
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
   * @tparam T Domain type.
   * @param start Starting coordinates in the slab.
   * @param qc_results Results from the query condition.
   * @param qc_results_index Current index in the `qc_results` buffer.
   * @param num Number of coords to be written.
   * @param dim_idx Dimension indices of the corresponding `buffers`.
   *     For the special zipped coordinates, `dim_idx`, `buffers` and `offsets`
   *     contain a single element and `dim_idx` contains `dim_num` as
   *     the dimension index.
   * @param buffers Buffers to copy from. It could be the special zipped
   *     coordinates or separate coordinate buffers.
   * @param offsets Offsets that will be used eventually to update the buffer
   *     sizes, determining the useful results written in the buffers.
   */
  template <class T>
  void fill_dense_coords_row_slab(
      const T* start,
      const optional<std::vector<uint8_t>> qc_results,
      uint64_t& qc_results_index,
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
   * @tparam T Domain type.
   * @param start Starting coordinates in the slab.
   * @param qc_results Results from the query condition.
   * @param qc_results_index Current index in the `qc_results` buffer.
   * @param num Number of coords to be written.
   * @param dim_idx Dimension indices of the corresponding `buffers`.
   *     For the special zipped coordinates, `dim_idx`, `buffers` and `offsets`
   *     contain a single element and `dim_idx` contains `dim_num` as
   *     the dimension index.
   * @param buffers Buffers to copy from. It could be the special zipped
   *     coordinates or separate coordinate buffers.
   * @param offsets Offsets that will be used eventually to update the buffer
   *     sizes, determining the useful results written in the buffers.
   */
  template <class T>
  void fill_dense_coords_col_slab(
      const T* start,
      const optional<std::vector<uint8_t>> qc_results,
      uint64_t& qc_results_index,
      uint64_t num,
      const std::vector<unsigned>& dim_idx,
      const std::vector<QueryBuffer*>& buffers,
      std::vector<uint64_t>& offsets) const;
};

}  // namespace tiledb::sm

#endif  // TILEDB_DENSE_READER
