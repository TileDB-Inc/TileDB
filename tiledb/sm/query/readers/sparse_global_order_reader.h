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
#include "tiledb/sm/query/readers/reader_base.h"
#include "tiledb/sm/query/readers/result_cell_slab.h"
#include "tiledb/sm/query/readers/result_coords.h"
#include "tiledb/sm/query/readers/sparse_index_reader_base.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class Array;
class StorageManager;

/** Processes sparse global order read queries. */

template <class BitmapType>
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
      QueryCondition& condition,
      bool consolidation_with_timestamps);

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
  std::vector<std::list<GlobalOrderResultTile<BitmapType>>> result_tiles_;

  /** Memory used for coordinates tiles per fragment. */
  std::vector<uint64_t> memory_used_for_coords_;

  /** Memory budget per fragment. */
  double per_fragment_memory_;

  /** Memory used for qc tiles per fragment. */
  std::vector<uint64_t> memory_used_for_qc_tiles_;

  /** Memory budget per fragment for qc tiles. */
  double per_fragment_qc_memory_;

  /** Enables consolidation with timestamps or not. */
  bool consolidation_with_timestamps_;

  /** Mutex to protect the tile queue. */
  std::mutex tile_queue_mutex_;

  /** Stores last cell for fragments consolidated with timestamps. */
  std::vector<FragIdx> last_cells_;

  /* ********************************* */
  /*       PRIVATE DECLARATIONS        */
  /* ********************************* */

  /** Tile min heap. */
  template <typename CompType>
  using TileMinHeap = std::priority_queue<
      GlobalOrderResultCoords<BitmapType>,
      std::vector<GlobalOrderResultCoords<BitmapType>>,
      CompType>;

  /** Tile list iterator. */
  using TileListIt =
      typename std::list<GlobalOrderResultTile<BitmapType>>::iterator;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /**
   * Get the coordinate tiles size for a dimension.
   *
   * @param dim_num Number of dimensions.
   * @param f Fragment index.
   * @param t Tile index.
   *
   * @return Status, tiles_size, tiles_size_qc.
   */
  tuple<Status, optional<std::pair<uint64_t, uint64_t>>> get_coord_tiles_size(
      unsigned dim_num, unsigned f, uint64_t t);

  /**
   * Add a result tile to process, making sure maximum budget is respected.
   *
   * @param dim_num Number of dimensions.
   * @param memory_budget_coords_tiles Memory budget for coordinate tiles.
   * @param memory_budget_qc_tiles Memory budget for query condition tiles.
   * @param f Fragment index.
   * @param t Tile index.
   * @param frag_md Fragment metadata.
   *
   * @return buffers_full, new_var_buffer_size, new_result_tiles_size.
   */
  tuple<Status, optional<bool>> add_result_tile(
      const unsigned dim_num,
      const uint64_t memory_budget_coords_tiles,
      const uint64_t memory_budget_qc_tiles,
      const unsigned f,
      const uint64_t t,
      const FragmentMetadata& frag_md);

  /**
   * Create the result tiles.
   *
   * @return Status, tiles_found.
   */
  tuple<Status, optional<bool>> create_result_tiles();

  /**
   * Process tiles with timestamps to deduplicate entries.
   *
   * @param result_tiles Result tiles to process.
   *
   * @return Status.
   */
  Status dedup_tiles_with_timestamps(std::vector<ResultTile*>& result_tiles);

  /**
   * Process fragments with timestamps to deduplicate entries.
   * This removes cells across tiles.
   *
   * @param result_tiles Result tiles to process.
   *
   * @return Status.
   */
  Status dedup_fragments_with_timestamps();

  /**
   * Populate a result cell slab to process.
   *
   * @return Status, result_cell_slab.
   */
  tuple<Status, optional<std::vector<ResultCellSlab>>>
  compute_result_cell_slab();

  /**
   * Is the result coord the last cell of a consolidated fragment with
   * timestamps.
   *
   * @param frag_idx Fragment index for the result coords.
   * @param rc Result coords.
   *
   * @return true if the result coords is the last cell of a consolidated
   * fragment with timestamps.
   */
  inline bool last_in_memory_cell_of_consolidated_fragment(
      const unsigned int frag_idx,
      const GlobalOrderResultCoords<BitmapType>& rc) const {
    return !all_tiles_loaded_[frag_idx] &&
           fragment_metadata_[frag_idx]->has_timestamps() &&
           rc.tile_->tile_idx() == last_cells_[frag_idx].tile_idx_ &&
           rc.pos_ == last_cells_[frag_idx].cell_idx_;
  }

  /**
   * Add a cell (for a specific fragment) to the queue of cells currently being
   * processed.
   *
   * @param dups Are we returning dups or not.
   * @param rc Current result coords for the fragment.
   * @param result_tiles_it Iterator, per frag, in the list of retult tiles.
   * @param tile_queue Queue of one result coords, per fragment, sorted.
   *
   * @return Status, more_tiles.
   */
  template <class CompType>
  tuple<Status, optional<bool>> add_next_cell_to_queue(
      bool dups,
      GlobalOrderResultCoords<BitmapType>& rc,
      std::vector<TileListIt>& result_tiles_it,
      TileMinHeap<CompType>& tile_queue);

  /**
   * Computes a tile's Hilbert values for a tile.
   *
   * @param result_tiles Result tiles to process.
   *
   * @return Status.
   */
  Status compute_hilbert_values(std::vector<ResultTile*>& result_tiles);

  /**
   * Get the timestamp value for a result coords.
   *
   * @param rc Result coords.
   *
   * @return timestamp.
   */
  uint64_t get_timestamp(const GlobalOrderResultCoords<BitmapType>& rc) const;

  /**
   * Compute the result cell slabs once tiles are loaded.
   *
   * @param num_cells Number of cells that can be copied in the user buffer.
   * @param cmp Comparator used to merge cells.
   *
   * @return Status, result_cell_slabs.
   */
  template <class CompType>
  tuple<Status, optional<std::vector<ResultCellSlab>>> merge_result_cell_slabs(
      uint64_t num_cells, CompType cmp);

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
  Status remove_result_tile(const unsigned frag_idx, TileListIt rt);

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
