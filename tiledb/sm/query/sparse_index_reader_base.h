/**
 * @file   sparse_index_reader_base.h
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
 * This file defines class SparseIndexReaderBase.
 */

#ifndef TILEDB_SPARSE_INDEX_READER_BASE_H
#define TILEDB_SPARSE_INDEX_READER_BASE_H

#include <queue>
#include "reader_base.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/query/query_condition.h"
#include "tiledb/sm/query/result_cell_slab.h"

namespace tiledb {
namespace sm {

class Array;
class ArraySchema;
class OpenArrayMemoryTracker;
class StorageManager;
class Subarray;

/** Processes read queries. */
class SparseIndexReaderBase : public ReaderBase {
 public:
  /* ********************************* */
  /*          TYPE DEFINITIONS         */
  /* ********************************* */

  /** The state for a read sparse global order query. */
  struct ReadState {
    /** The result cell slabs currently in process. */
    std::vector<ResultCellSlab> result_cell_slabs_;

    /** The tile index inside of each fragments. */
    std::vector<std::pair<uint64_t, uint64_t>> frag_tile_idx_;
  };

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  SparseIndexReaderBase(
      stats::Stats* stats,
      StorageManager* storage_manager,
      Array* array,
      Config& config,
      std::unordered_map<std::string, QueryBuffer>& buffers,
      Subarray& subarray,
      Layout layout,
      QueryCondition& condition);

  /** Destructor. */
  ~SparseIndexReaderBase() = default;

  /* ********************************* */
  /*          PUBLIC METHODS           */
  /* ********************************* */

  /** Returns the current read state. */
  const ReadState* read_state() const;

  /** Returns the current read state. */
  ReadState* read_state();

  /** Clears the result tiles. Used by serialization. */
  virtual Status clear_result_tiles() = 0;

  /** Add a result tile with no memory budget checks. Used by serialization. */
  virtual ResultTile* add_result_tile_unsafe(
      unsigned dim_num, unsigned f, uint64_t t, const Domain* domain) = 0;

 protected:
  /* ********************************* */
  /*       PROTECTED ATTRIBUTES        */
  /* ********************************* */

  /** Read state. */
  ReadState read_state_;

  /** Is the reader done with the query. */
  bool done_adding_result_tiles_;

  /** Have we loaded all thiles for this fragment. */
  std::vector<bool> all_tiles_loaded_;

  /** Dimension names. */
  std::vector<std::string> dim_names_;

  /** Are dimensions var sized. */
  std::vector<bool> is_dim_var_size_;

  /** Sorted list, per fragments of tiles ranges in the subarray, if set. */
  std::vector<std::list<std::pair<uint64_t, uint64_t>>> result_tile_ranges_;

  /** Have ve loaded the initial data. */
  bool initial_data_loaded_;

  /** Total memory budget. */
  uint64_t memory_budget_;

  /** Mutex protecting memory budget variables. */
  std::mutex mem_budget_mtx_;

  /** Memory tracker object for the array. */
  OpenArrayMemoryTracker* array_memory_tracker_;

  /** Memory used for coordinates tiles. */
  uint64_t memory_used_for_coords_total_;

  /** Memory used for query condition tiles. */
  uint64_t memory_used_qc_tiles_;

  /** Memory used for result cell slabs. */
  uint64_t memory_used_rcs_;

  /** Memory used for result tiles. */
  uint64_t memory_used_result_tiles_;

  /** Memory used for result tile ranges. */
  uint64_t memory_used_result_tile_ranges_;

  /** How much of the memory budget is reserved for coords. */
  double memory_budget_ratio_coords_;

  /** How much of the memory budget is reserved for query condition. */
  double memory_budget_ratio_query_condition_;

  /** How much of the memory budget is reserved for tile ranges. */
  double memory_budget_ratio_tile_ranges_;

  /** How much of the memory budget is reserved for array data. */
  double memory_budget_ratio_array_data_;

  /** How much of the memory budget is reserved for result tiles. */
  double memory_budget_ratio_result_tiles_;

  /** How much of the memory budget is reserved for result cell slabs. */
  double memory_budget_ratio_rcs_;

  /** Indicate if the coordinates are loaded for the result tiles. */
  bool coords_loaded_;

  /* ********************************* */
  /*         PROTECTED METHODS         */
  /* ********************************* */

  /** Get the coordinate tiles size for a dimension. */
  Status get_coord_tiles_size(
      unsigned dim_num, unsigned f, uint64_t t, uint64_t* tiles_size);

  /** Load tile offsets and result tile ranges. */
  Status load_initial_data();

  /**
   * Computes info about the which tiles are in subarray,
   * making sure maximum budget is respected.
   */
  Status compute_result_tiles_ranges(uint64_t memory_budget);

  /** Compute the result bitmap for a tile. */
  Status compute_coord_tiles_result_bitmap(
      bool subarray_set,
      ResultTile* tile,
      std::vector<uint8_t>* coord_tiles_result_bitmap);

  /** Resize the output buffers to the correct size after copying. */
  Status resize_output_buffers();
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_SPARSE_INDEX_READER_BASE_H