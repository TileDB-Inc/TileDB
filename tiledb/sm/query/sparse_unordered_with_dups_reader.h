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
class SparseUnorderedWithDupsReader : public SparseIndexReaderBase,
                                      public IQueryStrategy {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  SparseUnorderedWithDupsReader(
      stats::Stats* stats,
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

  /** Performs a read query using its set members. */
  Status dowork();

  /** Resets the reader object. */
  void reset();

  /** Clears the result tiles. Used by serialization. */
  Status clear_result_tiles();

  /** Add a result tile with no memory budget checks. Used by serialization. */
  ResultTile* add_result_tile_unsafe(
      unsigned dim_num, unsigned f, uint64_t t, const Domain* domain);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The result tiles currently loaded. */
  std::list<ResultTile> result_tiles_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /** Load a coordinate tile, making sure maximum budget is respected. */
  Status add_result_tile(
      unsigned dim_num,
      uint64_t memory_budget_result_tiles,
      uint64_t memory_budget_qc_tiles,
      uint64_t memory_budget_coords_tiles,
      unsigned f,
      uint64_t t,
      const Domain* domain,
      bool* budget_exceeded);

  /** Create the result tiles. */
  Status create_result_tiles(bool* tiles_found);

  /** Populate a result cell slab to process. */
  Status compute_result_cell_slab();

  /** Create the result cell slabs once tiles are loaded. */
  Status create_result_cell_slabs(uint64_t memory_budget);

  /** Remove a result tile from memory */
  Status remove_result_tile(
      unsigned frag_idx, std::list<ResultTile>::iterator rt);

  /** Clean up processed data after copying and get ready for the next
   * iteration. */
  Status end_iteration();
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_SPARSE_UNORDERED_WITH_DUPS_READER
