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
   */
  template <class OffType>
  static Status compute_var_size_offsets(
      stats::Stats* stats,
      const std::vector<ResultTile*>* result_tiles,
      std::vector<uint64_t>* cell_offsets,
      QueryBuffer* query_buffer,
      uint64_t* new_result_tiles_size,
      uint64_t* var_buffer_size);

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

  /** Initializes the reader. */
  Status init();

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

  /** The result tiles currently loaded. */
  ResultTileListPerFragment<BitmapType> result_tiles_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /** Add a result tile to process, making sure maximum budget is respected. */
  Status add_result_tile(
      const unsigned dim_num,
      const uint64_t memory_budget_qc_tiles,
      const uint64_t memory_budget_coords_tiles,
      const unsigned f,
      const uint64_t t,
      const uint64_t last_t,
      const ArraySchema* const array_schema,
      bool* budget_exceeded);

  /** Create the result tiles. */
  Status create_result_tiles();

  /** Compute parallelization parameters for a tile copy operation. */
  void compute_parallelization_parameters(
      const uint64_t range_thread_idx,
      const uint64_t num_range_threads,
      const uint64_t min_pos_tile,
      const uint64_t max_pos_tile,
      const ResultTileWithBitmap<BitmapType>* rt,
      uint64_t* src_min_pos,
      uint64_t* src_max_pos,
      uint64_t* dest_cell_offset,
      bool* skip_copy);

  /** Copy offsets tile. */
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

  /** Copy offsets tiles. */
  template <class OffType>
  Status copy_offsets_tiles(
      const std::string& name,
      const uint64_t num_range_threads,
      const bool nullable,
      const OffType offset_div,
      const std::vector<ResultTile*>* result_tiles,
      const std::vector<uint64_t>* cell_offsets,
      QueryBuffer* query_buffer,
      void** var_data);

  /** Copy var data tile. */
  template <class OffType>
  Status copy_var_data_tile(
      const bool last_partition,
      const uint64_t cell_offset,
      const uint64_t offset_div,
      const uint64_t var_buffer_size,
      const uint64_t src_min_pos,
      const uint64_t src_max_pos,
      const void** var_data,
      const OffType* offsets_buffer,
      uint8_t* var_data_buffer);

  /** Copy var data tiles. */
  template <class OffType>
  Status copy_var_data_tiles(
      const uint64_t num_range_threads,
      const OffType offset_div,
      const uint64_t var_buffer_size,
      const uint64_t result_tiles_size,
      const std::vector<ResultTile*>* result_tiles,
      const std::vector<uint64_t>* cell_offsets,
      QueryBuffer* query_buffer,
      const void** var_data);

  /** Copy fixed size data tile. */
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

  /** Copy fixed size data tiles. */
  Status copy_fixed_data_tiles(
      const std::string& name,
      const uint64_t num_range_threads,
      const bool is_dim,
      const bool nullable,
      const uint64_t dim_idx,
      const uint64_t cell_size,
      const std::vector<ResultTile*>* result_tiles,
      const std::vector<uint64_t>* cell_offsets,
      QueryBuffer* query_buffer);

  /**
   * Compute the maximum vector of result tiles to process amd cell offsets for
   * each tiles using the fixed size buffers from the user.
   */
  Status compute_fixed_results_to_copy(
      std::vector<ResultTile*>* result_tiles,
      std::vector<uint64_t>* cell_offsets);

  /**
   * Make sure we respect memory budget for copy operation by making sure that,
   * for all attributes to be copied, the size of tiles in memory can fit into
   * the budget.
   */
  Status respect_copy_memory_budget(
      const std::vector<std::string>& names,
      const uint64_t memory_budget,
      const std::vector<uint64_t>* cell_offsets,
      std::vector<ResultTile*>* result_tiles,
      std::vector<uint64_t>* total_mem_usage_per_attr);

  /**
   * Read and unfilter as many attributes as can fit in the memory budget and
   * return the names loaded in 'names_to_copy'. Also keep the 'buffer_idx'
   * updated to keep track of progress.
   */
  Status read_and_unfilter_attributes(
      const uint64_t memory_budget,
      const std::vector<std::string>* names,
      const std::vector<uint64_t>* mem_usage_per_attr,
      uint64_t* buffer_idx,
      std::vector<std::string>* names_to_copy,
      std::vector<ResultTile*>* result_tiles);

  /** Copy tiles. */
  template <class OffType>
  Status process_tiles(std::vector<ResultTile*>* result_tiles);

  /** Remove a result tile from memory */
  Status remove_result_tile(
      const unsigned frag_idx,
      typename std::list<ResultTileWithBitmap<BitmapType>>::iterator rt);

  /** Clean up processed data after copying and get ready for the next
   * iteration. */
  Status end_iteration();
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_SPARSE_UNORDERED_WITH_DUPS_READER
