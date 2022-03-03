/**
 * @file   sparse_unordered_with_dups_reader.h
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
 * This file defines class SparseUnorderedWithDupsReader.
 */

#ifndef TILEDB_SPARSE_UNORDERED_WITH_DUPS_READER
#define TILEDB_SPARSE_UNORDERED_WITH_DUPS_READER

#include <atomic>

#include "tiledb/common/logger_public.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/misc/types.h"
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

/** Processes sparse unordered with duplicates read queries. */
template <class BitmapType>
class SparseUnorderedWithDupsReader : public SparseIndexReaderBase,
                                      public IQueryStrategy {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  SparseUnorderedWithDupsReader(
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
  ~SparseUnorderedWithDupsReader() = default;

  DISABLE_COPY_AND_COPY_ASSIGN(SparseUnorderedWithDupsReader);
  DISABLE_MOVE_AND_MOVE_ASSIGN(SparseUnorderedWithDupsReader);

  /* ********************************* */
  /*          STATIC FUNCTIONS         */
  /* ********************************* */

  /**
   * Compute the var size offsets and make sure all the data can fit in the
   * user buffer.
   *
   * @param stats Stats.
   * @param fragment_metadata Fragment metadata.
   * @param result_tiles Result tiles to process, might be truncated.
   * @param first_tile_min_pos Cell progress of the first tile.
   * @param cell_offsets Cell offset per result tile.
   * @param query_buffer Query buffer to operate on.
   *
   * @return buffers_full, new_var_buffer_size, new_result_tiles_size.
   */
  template <class OffType>
  static tuple<bool, uint64_t, uint64_t> compute_var_size_offsets(
      stats::Stats* stats,
      const std::vector<tdb_shared_ptr<FragmentMetadata>>& fragment_metadata,
      const std::vector<ResultTile*>& result_tiles,
      const uint64_t first_tile_min_pos,
      std::vector<uint64_t>& cell_offsets,
      QueryBuffer& query_buffer);

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

  /** Result tiles currently loaded. */
  ResultTileListPerFragment<BitmapType> result_tiles_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /**
   * Add a result tile to process, making sure maximum budget is respected.
   *
   * @param dim_num Number of dimensions.
   * @param memory_budget_qc_tiles Memory budget for query condition tiles.
   * @param memory_budget_coords_tiles Memory budget for coordinate tiles.
   * @param f Fragment index.
   * @param t Tile index.
   * @param last_t Last tile index.
   * @param array_schema Array schema.
   *
   * @return buffers_full, new_var_buffer_size, new_result_tiles_size.
   */
  tuple<Status, optional<bool>> add_result_tile(
      const unsigned dim_num,
      const uint64_t memory_budget_qc_tiles,
      const uint64_t memory_budget_coords_tiles,
      const unsigned f,
      const uint64_t t,
      const uint64_t last_t,
      const ArraySchema& array_schema);

  /**
   * Create the result tiles.
   *
   * @return Status.
   */
  Status create_result_tiles();

  /**
   * Compute parallelization parameters for a tile copy operation.
   *
   * @param range_thread_idx Current range thread index.
   * @param num_range_threads Total number of range threads.
   * @param min_pos_tile Minimum cell position to process.
   * @param max_pos_tile Maximum cell postiion to process.
   * @param cell_offsets Cell offset per result tile.
   * @param rt Result tile currently in process.
   *
   * @return min_pos, max_pos, dest_cell_offset, skip_copy.
   */
  tuple<bool, uint64_t, uint64_t, uint64_t> compute_parallelization_parameters(
      const uint64_t range_thread_idx,
      const uint64_t num_range_threads,
      const uint64_t min_pos_tile,
      const uint64_t max_pos_tile,
      const uint64_t cell_offset,
      const ResultTileWithBitmap<BitmapType>* rt);

  /**
   * Copy offsets tile.
   *
   * @param name Name of the dimension/attribute.
   * @param nullable Is this field nullable.
   * @param offset_div Divisor used to convert offsets into element mode.
   * @param rt Result tile currently in process.
   * @param src_min_pos Minimum cell position to copy.
   * @param src_max_pos Maximum cell position to copy.
   * @param buffer Offsets buffer.
   * @param val_buffer Validity buffer.
   * @param var_data Stores pointers to var data cell values.
   *
   * @return Status.
   */
  template <class OffType>
  Status copy_offsets_tile(
      const std::string& name,
      const bool nullable,
      const OffType offset_div,
      ResultTileWithBitmap<BitmapType>* rt,
      const uint64_t src_min_pos,
      const uint64_t src_max_pos,
      OffType* buffer,
      uint8_t* val_buffer,
      void** var_data);

  /**
   * Copy offsets tiles.
   *
   * @param name Name of the dimension/attribute.
   * @param num_range_threads Total number of range threads.
   * @param nullable Is this field nullable.
   * @param offset_div Divisor used to convert offsets into element mode.
   * @param result_tiles Result tiles to process.
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
      const std::vector<ResultTile*>& result_tiles,
      const std::vector<uint64_t>& cell_offsets,
      QueryBuffer& query_buffer,
      std::vector<void*>& var_data);

  /**
   * Copy var data tile.
   *
   * @param last_partition Is this the last partition in process.
   * @param var_data_offset First offset into var data values for this tile.
   * @param offset_div Divisor used to convert offsets into element mode.
   * @param var_buffer_size Size of the var data buffer.
   * @param src_min_pos Minimum cell position to copy.
   * @param src_max_pos Maximum cell position to copy.
   * @param var_data Stores pointers to var data cell values.
   * @param offsets_buffer Offsets buffer.
   * @param var_data_buffer Var data buffer.
   *
   * @return Status.
   */
  template <class OffType>
  Status copy_var_data_tile(
      const bool last_partition,
      const uint64_t var_data_offset,
      const uint64_t offset_div,
      const uint64_t var_buffer_size,
      const uint64_t src_min_pos,
      const uint64_t src_max_pos,
      const void** var_data,
      const OffType* offsets_buffer,
      uint8_t* var_data_buffer);

  /**
   * Copy var data tiles.
   *
   * @param num_range_threads Total number of range threads.
   * @param offset_div Divisor used to convert offsets into element mode.
   * @param var_buffer_size Size of the var data buffer.
   * @param result_tiles Result tiles to process.
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
      const std::vector<ResultTile*>& result_tiles,
      const std::vector<uint64_t>& cell_offsets,
      QueryBuffer& query_buffer,
      std::vector<void*>& var_data);

  /**
   * Copy fixed size data tile.
   *
   * @param name Name of the dimension/attribute.
   * @param is_dim Is this field a dimension.
   * @param nullable Is this field nullable.
   * @param dim_idx Dimention index, used for zipped coords.
   * @param cell_size Cell size.
   * @param rt Result tile currently in process.
   * @param src_min_pos Minimum cell position to copy.
   * @param src_max_pos Maximum cell position to copy.
   * @param buffer Offsets buffer.
   * @param val_buffer Validity buffer.
   *
   * @return Status.
   */
  Status copy_fixed_data_tile(
      const std::string& name,
      const bool is_dim,
      const bool nullable,
      const unsigned dim_idx,
      const uint64_t cell_size,
      ResultTileWithBitmap<BitmapType>* rt,
      const uint64_t src_min_pos,
      const uint64_t src_max_pos,
      uint8_t* buffer,
      uint8_t* val_buffer);

  /**
   * Copy fixed size data tiles.
   *
   * @param name Name of the dimension/attribute.
   * @param num_range_threads Total number of range threads.
   * @param is_dim Is this field a dimension.
   * @param nullable Is this field nullable.
   * @param dim_idx Dimention index, used for zipped coords.
   * @param cell_size Cell size.
   * @param result_tiles Result tiles to process.
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
      const uint64_t dim_idx,
      const uint64_t cell_size,
      const std::vector<ResultTile*>& result_tiles,
      const std::vector<uint64_t>& cell_offsets,
      QueryBuffer& query_buffer);

  /**
   * Compute the maximum vector of result tiles to process and cell offsets for
   * each tiles using the fixed size buffers from the user.
   *
   * @param names Attribute/dimensions to compute for.
   * @param result_tiles The result tiles to process.
   *
   * @return Cell o
   */
  std::vector<uint64_t> compute_fixed_results_to_copy(
      const std::vector<std::string>& names,
      std::vector<ResultTile*>& result_tiles);

  /**
   * Make sure we respect memory budget for copy operation by making sure that,
   * for all attributes to be copied, the size of tiles in memory can fit into
   * the budget.
   *
   * @param names Attribute/dimensions to compute for.
   * @param memory_budget Memory budget allowed for copy operation.
   * @param result_tiles Result tiles to process, might be truncated.
   *
   * @return Status, total_mem_usage_per_attr.
   */
  tuple<Status, optional<std::vector<uint64_t>>> respect_copy_memory_budget(
      const std::vector<std::string>& names,
      const uint64_t memory_budget,
      std::vector<ResultTile*>& result_tiles);

  /**
   * Copy tiles.
   *
   * @param names Attribute/dimensions to compute for.
   * @param result_tiles The result tiles to process.
   *
   * @return Status.
   */
  template <class OffType>
  Status process_tiles(
      std::vector<std::string>& names, std::vector<ResultTile*>& result_tiles);

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
      typename std::list<ResultTileWithBitmap<BitmapType>>::iterator rt);

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

#endif  // TILEDB_SPARSE_UNORDERED_WITH_DUPS_READER
