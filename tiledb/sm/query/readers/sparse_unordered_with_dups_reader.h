/**
 * @file   sparse_unordered_with_dups_reader.h
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
 * This file defines class SparseUnorderedWithDupsReader.
 */

#ifndef TILEDB_SPARSE_UNORDERED_WITH_DUPS_READER
#define TILEDB_SPARSE_UNORDERED_WITH_DUPS_READER

#include <atomic>

#include "tiledb/common/common.h"
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

namespace tiledb::sm {

class Array;

/** Processes sparse unordered with duplicates read queries. */
template <class BitmapType>
class SparseUnorderedWithDupsReader : public SparseIndexReaderBase,
                                      public IQueryStrategy {
 public:
  typedef std::list<UnorderedWithDupsResultTile<BitmapType>> ResultTilesList;

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  SparseUnorderedWithDupsReader(
      stats::Stats* stats, shared_ptr<Logger> logger, StrategyParams& params);

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
   * @param result_tiles Result tiles to process, might be truncated.
   * @param first_tile_min_pos Cell progress of the first tile.
   * @param cell_offsets Cell offset per result tile.
   * @param query_buffer Query buffer to operate on.
   *
   * @return caused_overflow, new_var_buffer_size, new_result_tiles_size.
   */
  template <class OffType>
  static tuple<bool, uint64_t, uint64_t> compute_var_size_offsets(
      stats::Stats* stats,
      const std::vector<ResultTile*>& result_tiles,
      const uint64_t first_tile_min_pos,
      std::vector<uint64_t>& cell_offsets,
      QueryBuffer& query_buffer);

  /**
   * Compute the fixed result tiles to copy using the list of result tiles. This
   * will resize the list of result tiles to the actual size we can copy.
   *
   * @param max_num_cells Max number of cells we can fit in the fixed size
   * buffers.
   * @param initial_cell_offset Initial cell offset in the user buffers.
   * @param first_tile_min_pos Cell progress of the first tile.
   * @param result_tiles Result tiles to process, might be truncated.
   *
   * @return user_buffers_full, cell_offsets.
   */
  static tuple<bool, std::vector<uint64_t>> resize_fixed_result_tiles_to_copy(
      uint64_t max_num_cells,
      uint64_t initial_cell_offset,
      uint64_t first_tile_min_pos,
      std::vector<ResultTile*>& result_tiles);

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
   * Initialize the memory budget variables.
   *
   * @return Status.
   */
  void refresh_config();

  /**
   * Performs a read query using its set members.
   *
   * @return Status.
   */
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

  /**
   * Result tiles currently for which we loaded coordinates but couldn't
   * process in the previous iteration.
   */
  std::list<UnorderedWithDupsResultTile<BitmapType>> result_tiles_leftover_;
  /** Minimum fragment index for loaded tile offsets data. */
  unsigned tile_offsets_min_frag_idx_;

  /** Maximum fragment index for loaded tile offsets data. */
  unsigned tile_offsets_max_frag_idx_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /** Loads as much tile offset data in memory as possible. */
  void load_tile_offsets_data();

  /**
   * Get the coordinate tiles size for a dimension.
   *
   * @param dim_num Number of dimensions.
   * @param f Fragment index.
   * @param t Tile index.
   *
   * @return Tiles size.
   */
  uint64_t get_coord_tiles_size(unsigned dim_num, unsigned f, uint64_t t);

  /**
   * Add a result tile to process, making sure maximum budget is respected.
   *
   * @param dim_num Number of dimensions.
   * @param f Fragment index.
   * @param t Tile index.
   * @param last_t Last tile index.
   * @param frag_md Fragment metadata.
   * @param result_tiles Result tile list to add to.
   *
   * @return user_buffers_full.
   */
  bool add_result_tile(
      const unsigned dim_num,
      const unsigned f,
      const uint64_t t,
      const uint64_t last_t,
      const FragmentMetadata& frag_md,
      ResultTilesList& result_tiles);

  /** Create the result tiles. */
  ResultTilesList create_result_tiles();

  /**
   * Clean tiles that have 0 results from the tile lists.
   *
   * @param result_tiles Result tiles list.
   * @param result_tiles_ptr Result tile pointers vectors.
   */
  void clean_tile_list(
      ResultTilesList& result_tiles,
      std::vector<ResultTile*>& result_tiles_ptr);

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
      const UnorderedWithDupsResultTile<BitmapType>* rt);

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
   */
  template <class OffType>
  void copy_offsets_tile(
      const std::string& name,
      const bool nullable,
      const OffType offset_div,
      UnorderedWithDupsResultTile<BitmapType>* rt,
      const uint64_t src_min_pos,
      const uint64_t src_max_pos,
      OffType* buffer,
      uint8_t* val_buffer,
      const void** var_data);

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
   */
  template <class OffType>
  void copy_offsets_tiles(
      const std::string& name,
      const uint64_t num_range_threads,
      const bool nullable,
      const OffType offset_div,
      const std::vector<ResultTile*>& result_tiles,
      const std::vector<uint64_t>& cell_offsets,
      QueryBuffer& query_buffer,
      std::vector<const void*>& var_data);

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
   */
  template <class OffType>
  void copy_var_data_tile(
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
   */
  template <class OffType>
  void copy_var_data_tiles(
      const uint64_t num_range_threads,
      const OffType offset_div,
      const uint64_t var_buffer_size,
      const std::vector<ResultTile*>& result_tiles,
      const std::vector<uint64_t>& cell_offsets,
      QueryBuffer& query_buffer,
      std::vector<const void*>& var_data);

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
   */
  void copy_fixed_data_tile(
      const std::string& name,
      const bool is_dim,
      const bool nullable,
      const unsigned dim_idx,
      const uint64_t cell_size,
      UnorderedWithDupsResultTile<BitmapType>* rt,
      const uint64_t src_min_pos,
      const uint64_t src_max_pos,
      uint8_t* buffer,
      uint8_t* val_buffer);

  /**
   * Copy timestamp data tile.
   *
   * @param rt Result tile currently in process.
   * @param src_min_pos Minimum cell position to copy.
   * @param src_max_pos Maximum cell position to copy.
   * @param buffer Offsets buffer.
   */
  void copy_timestamp_data_tile(
      UnorderedWithDupsResultTile<BitmapType>* rt,
      const uint64_t src_min_pos,
      const uint64_t src_max_pos,
      uint8_t* buffer);

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
   */
  void copy_fixed_data_tiles(
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
   * Compute the maximum vector of result tiles to process and cell offsets
   * for each tiles using the fixed size buffers from the user.
   *
   * @param names Attribute/dimensions to compute for.
   * @param result_tiles The result tiles to process.
   *
   * @return user_buffers_full, cell_offsets.
   */
  tuple<bool, std::vector<uint64_t>> resize_fixed_results_to_copy(
      const std::vector<std::string>& names,
      std::vector<ResultTile*>& result_tiles);

  /**
   * Make sure we respect memory budget for copy operation by making sure
   * that, for all attributes to be copied, the size of tiles in memory can
   * fit into the budget.
   *
   * @param names Attribute/dimensions to compute for.
   * @param result_tiles Result tiles to process, might be truncated.
   * @param user_buffers_full Boolean that indicates if the user buffers are
   * full or not. If this comes in as `true`, it might be reset to `false` if
   * the results were truncated.
   *
   * @return total_mem_usage_per_attr.
   */
  std::vector<uint64_t> respect_copy_memory_budget(
      const std::vector<std::string>& names,
      std::vector<ResultTile*>& result_tiles,
      bool& user_buffers_full);

  /**
   * Process tiles.
   *
   * @param names Fields to process.
   * @param result_tiles The result tiles to process.
   *
   * @return user_buffers_full.
   */
  template <class OffType>
  bool process_tiles(
      std::vector<std::string>& names, std::vector<ResultTile*>& result_tiles);

  /**
   * Copy tiles.
   *
   * @param num_range_threads Total number of range threads.
   * @param name Field to copy.
   * @param names_to_copy All names processed for this copy batch.
   * @param is_dim Is the field a dimension.
   * @param cell_offsets Cell offset per result tile.
   * @param result_tiles The result tiles to process.
   * @param last_field_to_overflow Last field that caused an overflow.
   *
   * @return user_buffers_full.
   */
  template <class OffType>
  bool copy_tiles(
      const uint64_t num_range_threads,
      const std::string name,
      const std::vector<std::string>& names_to_copy,
      const bool is_dim,
      std::vector<uint64_t>& cell_offsets,
      std::vector<ResultTile*>& result_tiles,
      std::optional<std::string>& last_field_to_overflow);

  /**
   * Make an aggregate buffer.
   *
   * @param name Field to aggregate.
   * @param var_sized Is the field var sized?
   * @param nullable Is the field nullable?
   * @param cell_size Cell size for the field.
   * @param count_bitmap Is the bitmap a count bitmap?
   * @param min_cell Min cell to aggregate.
   * @param min_cell Max cell to aggregate.
   * @param rt Result tile.
   */
  AggregateBuffer make_aggregate_buffer(
      const std::string name,
      const bool var_sized,
      const bool nullable,
      const uint64_t cell_size,
      const bool count_bitmap,
      const uint64_t min_cell,
      const uint64_t max_cell,
      UnorderedWithDupsResultTile<BitmapType>& rt);

  /**
   * Returns wether or not we can aggregate the tile with only the fragment
   * metadata.
   *
   * @param rt Result tile.
   * @return If we can do the aggregation with the frag md or not.
   */
  inline bool can_aggregate_tile_with_frag_md(
      UnorderedWithDupsResultTile<BitmapType>* rt) {
    auto& frag_md = fragment_metadata_[rt->frag_idx()];

    // Here we only aggregate a full tile if first of all there are no missing
    // cells in the bitmap. This can be validated with 'copy_full_tile'.
    // Finally, we check the fragment metadata has indeed tile metadata.
    return rt->copy_full_tile() && frag_md->has_tile_metadata();
  }

  /**
   * Process aggregates.
   *
   * @param num_range_threads Total number of range threads.
   * @param name Field to aggregate.
   * @param cell_offsets Cell offset per result tile.
   * @param result_tiles The result tiles to process.
   */
  void process_aggregates(
      const uint64_t num_range_threads,
      const std::string name,
      std::vector<uint64_t>& cell_offsets,
      std::vector<ResultTile*>& result_tiles);

  /**
   * Remove a result tile from memory.
   *
   * @param frag_idx Fragment index.
   * @param result_tiles List of result tiles.
   * @param rt Iterator to the result tile to remove.
   */
  void remove_result_tile(
      const unsigned frag_idx,
      ResultTilesList& result_tiles,
      typename ResultTilesList::iterator rt);

  /**
   * Clean up processed data after copying and get ready for the next
   * iteration.
   *
   * @param result_tiles List of result tiles.
   */
  void end_iteration(ResultTilesList& result_tiles);
};

}  // namespace tiledb::sm

#endif  // TILEDB_SPARSE_UNORDERED_WITH_DUPS_READER
