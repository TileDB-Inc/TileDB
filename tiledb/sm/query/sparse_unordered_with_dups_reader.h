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
      const Domain* const domain,
      bool* budget_exceeded);

  /** Create the result tiles. */
  Status create_result_tiles();

  /** Copy offsets tile. */
  template <class OffType>
  Status copy_offsets_tile(
      const std::string& name,
      const bool nullable,
      const OffType offset_div,
      ResultTileWithBitmap<BitmapType>* rt,
      OffType* buffer,
      uint8_t* val_buffer,
      void** var_data);

  /** Copy offsets tiles. */
  template <class OffType>
  Status copy_offsets_tiles(
      const std::string& name,
      const bool nullable,
      const OffType offset_div,
      const uint64_t max_rt_idx,
      OffType* buffer,
      uint8_t* val_buffer,
      void** var_data,
      uint64_t* global_cell_offset,
      typename std::list<ResultTileWithBitmap<BitmapType>>::iterator*
          result_tiles_it);

  /** Copy var data tile. */
  template <class OffType>
  Status copy_var_data_tile(
      const bool last_tile,
      const uint64_t cell_offset,
      const uint64_t offset_div,
      const uint64_t last_offset,
      const ResultTileWithBitmap<BitmapType>* rt,
      const void** var_data,
      const OffType* offsets_buffer,
      uint8_t* var_data_buffer);

  /** Copy var data tiles. */
  template <class OffType>
  Status copy_var_data_tiles(
      const OffType offset_div,
      const uint64_t last_offset,
      const uint64_t max_rt_idx,
      OffType* offsets_buffer,
      uint8_t* var_data_buffer,
      const void** var_data,
      uint64_t* global_cell_offset);

  /** Copy fixed size data tile. */
  Status copy_fixed_data_tile(
      const std::string& name,
      const bool is_dim,
      const bool nullable,
      const unsigned dim_idx,
      const uint64_t cell_size,
      ResultTileWithBitmap<BitmapType>* rt,
      uint8_t* buffer,
      uint8_t* val_buffer);

  /** Copy fixed size data tiles. */
  Status copy_fixed_data_tiles(
      const std::string& name,
      const bool is_dim,
      const bool nullable,
      const uint64_t dim_idx,
      const uint64_t max_rt_idx,
      const uint64_t cell_size,
      uint8_t* buffer,
      uint8_t* val_buffer,
      uint64_t* global_cell_offset,
      typename std::list<ResultTileWithBitmap<BitmapType>>::iterator*
          result_tiles_it);

  /**
   * Compute initial max rt index for the copy and approximate memory usage
   * per attribute.
   */
  Status compute_initial_copy_bound(
      uint64_t memory_budget,
      uint64_t* max_rt_idx,
      std::vector<uint64_t>* total_mem_usage_per_attr);

  /** Read and unfilter attributes. */
  Status read_and_unfilter_attributes(
      const std::vector<std::string>* names,
      std::vector<ResultTile*>* result_tiles);

  /** Copy tiles. */
  template <class OffType>
  Status process_tiles();

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
