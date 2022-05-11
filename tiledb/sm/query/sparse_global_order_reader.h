/**
 * @file   sparse_global_order_reader.h
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
 * This file defines class SparseGlobalOrderReader.
 */

#ifndef TILEDB_SPARSE_GLOBAL_ORDER_READER
#define TILEDB_SPARSE_GLOBAL_ORDER_READER

#include <atomic>

#include "tiledb/common/common.h"
#include "tiledb/common/logger_public.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/query/iquery_strategy.h"
#include "tiledb/sm/query/query_buffer.h"
#include "tiledb/sm/query/query_condition.h"
#include "tiledb/sm/query/reader_base.h"
#include "tiledb/sm/query/result_cell_slab.h"
#include "tiledb/sm/query/result_coords.h"
#include "tiledb/sm/query/sparse_index_reader_base.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class Array;
class StorageManager;

/** Processes sparse global order read queries. */
class SparseGlobalOrderReader : public SparseIndexReaderBase,
                                public IQueryStrategy {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  SparseGlobalOrderReader(
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
  ~SparseGlobalOrderReader() = default;

  DISABLE_COPY_AND_COPY_ASSIGN(SparseGlobalOrderReader);
  DISABLE_MOVE_AND_MOVE_ASSIGN(SparseGlobalOrderReader);

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /**
   * Finalizes the reader.
   *
   * @return Status.
   */
  Status finalize() {
    return Status::Ok();
  }

  /**
   * Returns `true` if the query was incomplete.
   *
   * @return The query status.
   */
  bool incomplete() const;

  /**
   * Returns `true` if the query was incomplete.
   *
   * @return The query status.
   */
  QueryStatusDetailsReason status_incomplete_reason() const;

  /**
   * Initializes the reader.
   *
   * @return Status.
   */
  Status init();

  /**
   * Initialize the memory budget variables.
   *
   * @return Status.
   */
  Status initialize_memory_budget();

  /**
   * Performs a read query using its set members.
   *
   * @return Status.
   */
  Status dowork();

  /** Resets the reader object. */
  void reset();

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** UID of the logger instance */
  inline static std::atomic<uint64_t> logger_id_ = 0;

  /** The result tiles currently loaded. */
  ResultTileListPerFragment<uint8_t> result_tiles_;

  /** Memory used for coordinates tiles per fragment. */
  std::vector<uint64_t> memory_used_for_coords_;

  /** Memory budget per fragment. */
  double per_fragment_memory_;

  /** Memory used for qc tiles per fragment. */
  std::vector<uint64_t> memory_used_for_qc_tiles_;

  /** Memory budget per fragment for qc tiles. */
  double per_fragment_qc_memory_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /**
   * Add a result tile to process, making sure maximum budget is respected.
   *
   * @param dim_num Number of dimensions.
   * @param memory_budget_coords_tiles Memory budget for coordinate tiles.
   * @param memory_budget_qc_tiles Memory budget for query condition tiles.
   * @param f Fragment index.
   * @param t Tile index.
   * @param array_schema Array schema.
   *
   * @return buffers_full, new_var_buffer_size, new_result_tiles_size.
   */
  tuple<Status, optional<bool>> add_result_tile(
      const unsigned dim_num,
      const uint64_t memory_budget_coords_tiles,
      const uint64_t memory_budget_qc_tiles,
      const unsigned f,
      const uint64_t t,
      const ArraySchema& array_schema);

  /**
   * Create the result tiles.
   *
   * @return Status, tiles_found.
   */
  tuple<Status, optional<bool>> create_result_tiles();

  /**
   * Populate a result cell slab to process.
   *
   * @return Status, result_cell_slab.
   */
  tuple<Status, optional<std::vector<ResultCellSlab>>>
  compute_result_cell_slab();

  /**
   * Add a new tile to the queue of tiles currently being processed
   * for a specific fragment.
   *
   * @param frag_idx Fragment index.
   * @param cell_idx Cell index.
   * @param result_tiles_it Iterator, per frag, in the list of retult tiles.
   * @param result_tile_used Boolean, per frag, to know if a tile was used.
   * @param tile_queue Queue of one result coords, per fragment, sorted.
   * @param tile_queue_mutex Protects tile_queue.
   *
   * @return Status, more_tiles.
   */
  template <class T>
  tuple<Status, optional<bool>> add_next_tile_to_queue(
      unsigned int frag_idx,
      uint64_t cell_idx,
      std::vector<std::list<ResultTileWithBitmap<uint8_t>>::iterator>&
          result_tiles_it,
      std::vector<uint8_t>& result_tile_used,
      std::priority_queue<ResultCoords, std::vector<ResultCoords>, T>&
          tile_queue,
      std::mutex& tile_queue_mutex);

  /**
   * Computes a tile's Hilbert values for a tile.
   *
   * @param result_tiles Result tiles to process.
   *
   * @return Status.
   */
  Status compute_hilbert_values(std::vector<ResultTile*>& result_tiles);

  /**
   * Compute the result cell slabs once tiles are loaded.
   *
   * @param num_cells Number of cells that can be copied in the user buffer.
   * @param cmp Comparator used to merge cells.
   *
   * @return Status, result_cell_slabs.
   */
  template <class T>
  tuple<Status, optional<std::vector<ResultCellSlab>>> merge_result_cell_slabs(
      uint64_t num_cells, T cmp);

  /**
   * Compute parallelization parameters for a tile copy operation.
   *
   * @param range_thread_idx Current range thread index.
   * @param num_range_threads Total number of range threads.
   * @param start Start cell position to process.
   * @param lenth Number of cells to process.
   * @param cell_offsets Cell offset per result tile.
   *
   * @return min_pos, max_pos, dest_cell_offset, skip_copy.
   */
  tuple<uint64_t, uint64_t, uint64_t, bool> compute_parallelization_parameters(
      const uint64_t range_thread_idx,
      const uint64_t num_range_threads,
      const uint64_t start,
      const uint64_t length,
      const uint64_t cell_offset);

  /**
   * Copy offsets tiles.
   *
   * @param name Name of the dimension/attribute.
   * @param num_range_threads Total number of range threads.
   * @param nullable Is this field nullable.
   * @param offset_div Divisor used to convert offsets into element mode.
   * @param result_cell_slabs Result cell slabs to process.
   * @param cell_offsets Cell offset per result tile.
   * @param query_buffer Query buffer to operate on.
   * @param var_data Stores pointers to var data cell values.
   *
   * @return Status.
   */
  template <class OffType>
  Status copy_offsets_tiles(
      const std::string& name,
      const uint64_t num_range_threads,
      const bool nullable,
      const OffType offset_div,
      const std::vector<ResultCellSlab>& result_cell_slabs,
      const std::vector<uint64_t>& cell_offsets,
      QueryBuffer& query_buffer,
      std::vector<void*>& var_data);

  /**
   * Copy var data tiles.
   *
   * @param num_range_threads Total number of range threads.
   * @param offset_div Divisor used to convert offsets into element mode.
   * @param var_buffer_size Size of the var data buffer.
   * @param result_cell_slabs Result cell slabs to process.
   * @param cell_offsets Cell offset per result tile.
   * @param query_buffer Query buffer to operate on.
   * @param var_data Stores pointers to var data cell values.
   *
   * @return Status.
   */
  template <class OffType>
  Status copy_var_data_tiles(
      const uint64_t num_range_threads,
      const OffType offset_div,
      const uint64_t var_buffer_size,
      const std::vector<ResultCellSlab>& result_cell_slabs,
      const std::vector<uint64_t>& cell_offsets,
      QueryBuffer& query_buffer,
      const std::vector<void*>& var_data);

  /**
   * Copy fixed size data tiles.
   *
   * @param name Name of the dimension/attribute.
   * @param num_range_threads Total number of range threads.
   * @param is_dim Is this field a dimension.
   * @param nullable Is this field nullable.
   * @param dim_idx Dimention index, used for zipped coords.
   * @param cell_size Cell size.
   * @param result_cell_slabs Result cell slabs to process.
   * @param cell_offsets Cell offset per result tile.
   * @param query_buffer Query buffer to operate on.
   *
   * @return Status.
   */
  Status copy_fixed_data_tiles(
      const std::string& name,
      const uint64_t num_range_threads,
      const bool is_dim,
      const bool nullable,
      const unsigned dim_idx,
      const uint64_t cell_size,
      const std::vector<ResultCellSlab>& result_cell_slabs,
      const std::vector<uint64_t>& cell_offsets,
      QueryBuffer& query_buffer);

  /**
   * Copy timestamps tiles.
   *
   * @param num_range_threads Total number of range threads.
   * @param result_cell_slabs Result cell slabs to process.
   * @param cell_offsets Cell offset per result tile.
   * @param query_buffer Query buffer to operate on.
   *
   * @return Status.
   */
  Status copy_timestamps_tiles(
      const uint64_t num_range_threads,
      const std::vector<ResultCellSlab>& result_cell_slabs,
      const std::vector<uint64_t>& cell_offsets,
      QueryBuffer& query_buffer);

  /**
   * Make sure we respect memory budget for copy operation by making sure that,
   * for all attributes to be copied, the size of tiles in memory can fit into
   * the budget.
   *
   * @param names Attribute/dimensions to compute for.
   * @param memory_budget Memory budget allowed for copy operation.
   * @param result_cell_slabs Result cell slabs to process, might be truncated.
   *
   * @return Status, total_mem_usage_per_attr.
   */
  tuple<Status, optional<std::vector<uint64_t>>> respect_copy_memory_budget(
      const std::vector<std::string>& names,
      const uint64_t memory_budget,
      std::vector<ResultCellSlab>& result_cell_slabs);

  /**
   * Compute the var size offsets and make sure all the data can fit in the
   * user buffer.
   *
   * @param stats Stats.
   * @param result_cell_slabs Result cell slabs to process, might be truncated.
   * @param cell_offsets Cell offset per result tile.
   * @param query_buffer Query buffer to operate on.
   *
   * @return new_var_buffer_size.
   */
  template <class OffType>
  uint64_t compute_var_size_offsets(
      stats::Stats* stats,
      std::vector<ResultCellSlab>& result_cell_slabs,
      std::vector<uint64_t>& cell_offsets,
      QueryBuffer& query_buffer);

  /**
   * Copy cell slabs.
   *
   * @param names Attribute/dimensions to compute for.
   * @param result_cell_slabs The result cell slabs to process.
   *
   * @return Status.
   */
  template <class OffType>
  Status process_slabs(
      std::vector<std::string>& names,
      std::vector<ResultCellSlab>& result_cell_slabs);

  /**
   * Remove a result tile from memory
   *
   * @param frag_idx Fragment index.
   * @param rt Iterator to the result tile to remove.
   *
   * @return Status.
   */
  Status remove_result_tile(
      const unsigned frag_idx,
      std::list<ResultTileWithBitmap<uint8_t>>::iterator rt);

  /**
   * Clean up processed data after copying and get ready for the next
   * iteration.
   *
   * @return Status.
   */
  Status end_iteration();
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_SPARSE_GLOBAL_ORDER_READER
