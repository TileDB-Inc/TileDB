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
      tdb_shared_ptr<Logger> logger,
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

  /** Finalizes the reader. */
  Status finalize() {
    return Status::Ok();
  }

  /** Returns `true` if the query was incomplete. */
  bool incomplete() const;

  /** Returns the status details reason. */
  QueryStatusDetailsReason status_incomplete_reason() const;

  /** Initializes the reader. */
  Status init();

  /** Initialize the memory budget variables. */
  Status initialize_memory_budget();

  /** Performs a read query using its set members. */
  Status dowork();

  /** Resets the reader object. */
  void reset();

  /** Clears the result tiles. Used by serialization. */
  Status clear_result_tiles();

  /** Add a result tile with no memory budget checks. Used by serialization. */
  ResultTile* add_result_tile_unsafe(
      unsigned f, uint64_t t, const ArraySchema* array_schema);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** UID of the logger instance */
  inline static std::atomic<uint64_t> logger_id_ = 0;

  /** The result tiles currently loaded. */
  ResultTileListPerFragment<uint8_t> result_tiles_;

  /** Is the result tile list empty. */
  bool empty_result_tiles_;

  /** Memory used for coordinates tiles per fragment. */
  std::vector<uint64_t> memory_used_for_coords_;

  /** Memory budget per fragment. */
  double per_fragment_memory_;

  /** Memory used for qc tiles per fragment. */
  std::vector<uint64_t> memory_used_for_qc_tiles_;

  /** Memory budget per fragment for qc tiles. */
  double per_fragment_qc_memory_;

  /** Memory used for result cell slabs. */
  uint64_t memory_used_rcs_;

  /** How much of the memory budget is reserved for result cell slabs. */
  double memory_budget_ratio_rcs_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /** Add a result tile to process, making sure maximum budget is respected. */
  Status add_result_tile(
      const unsigned dim_num,
      const uint64_t memory_budget_coords_tiles,
      const uint64_t memory_budget_qc_tiles,
      const unsigned f,
      const uint64_t t,
      const ArraySchema* const array_schema,
      bool* budget_exceeded);

  /** corrects memory usage after de-serialization. */
  Status fix_memory_usage_after_serialization();

  /** Create the result tiles. */
  Status create_result_tiles(bool* tiles_found);

  /** Populate a result cell slab to process. */
  Status compute_result_cell_slab();

  /**
   * Add a new tile to the queue of tiles currently being processed
   *  for a specific fragment.
   */
  template <class T>
  Status add_next_tile_to_queue(
      bool subarray_set,
      unsigned int frag_idx,
      uint64_t cell_idx,
      std::vector<std::list<ResultTileWithBitmap<uint8_t>>::iterator>&
          result_tiles_it,
      std::vector<bool>& result_tile_used,
      std::priority_queue<ResultCoords, std::vector<ResultCoords>, T>&
          tile_queue,
      std::mutex& tile_queue_mutex,
      T& cmp,
      bool* need_more_tiles);

  /** Computes a tile's Hilbert values, stores them in the comparator. */
  template <class T>
  Status calculate_hilbert_values(
      bool subarray_set, ResultTileWithBitmap<uint8_t>* tile, T& cmp);

  /** Compute the result cell slabs once tiles are loaded. */
  template <class T>
  Status merge_result_cell_slabs(uint64_t memory_budget, T cmp);

  /** Remove a result tile from memory */
  Status remove_result_tile(
      const unsigned frag_idx,
      std::list<ResultTileWithBitmap<uint8_t>>::iterator rt);

  /** Clean up processed data after copying and get ready for the next
   * iteration. */
  Status end_iteration();
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_SPARSE_GLOBAL_ORDER_READER
