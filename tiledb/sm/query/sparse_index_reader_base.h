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
#include "tiledb/sm/misc/utils.h"
#include "tiledb/sm/query/query_condition.h"
#include "tiledb/sm/query/result_cell_slab.h"

namespace tiledb {
namespace sm {

class Array;
class ArraySchema;
class OpenArrayMemoryTracker;
class StorageManager;
class Subarray;

/** Result tile with bitmap. */
template <class BitmapType>
class ResultTileWithBitmap : public ResultTile {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */
  ResultTileWithBitmap(
      unsigned frag_idx, uint64_t tile_idx, const Domain* domain)
      : ResultTile(frag_idx, tile_idx, domain)
      , bitmap_num_cells(std::numeric_limits<uint64_t>::max())
      , qc_processed(false) {
  }

  /* ********************************* */
  /*         PUBLIC ATTRIBUTES         */
  /* ********************************* */

  /** Bitmap for this tile. */
  std::vector<BitmapType> bitmap;

  /** Number of cells in this bitmap. */
  uint64_t bitmap_num_cells;

  /** Was the query condition processed for this tile. */
  bool qc_processed;
};

/**
 * Result tile list per fragments. For sparse global order reader, this will
 * be the list of tiles loaded per fragments. For the unordered with duplicates
 * reader, all tiles will be in fragment 0.
 */
template <typename BitmapType>
using ResultTileListPerFragment =
    std::vector<std::list<ResultTileWithBitmap<BitmapType>>>;

/**
 * Iterates in a thread safe manner over a ResultTileListPerFragment. This will
 * be used where we have parallel_for's the need to iterate over all tiles. As
 * we have a vector of lists, we cannot retreive the ResultTile at a certain
 * index easily so this helper gives us one ResultTile at a time until we are
 * done.
 */
template <class BitmapType>
class ResultTileIt {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */
  ResultTileIt(ResultTileListPerFragment<BitmapType>* result_tiles)
      : result_tiles_(result_tiles)
      , it_(result_tiles->at(0).begin())
      , f_(0) {
  }

  /* ********************************* */
  /*          PUBLIC METHODS           */
  /* ********************************* */

  /** Returns the size. */
  uint64_t size() {
    uint64_t size = 0;
    for (auto& rt_list : *result_tiles_)
      size += rt_list.size();

    return size;
  }

  /** Returns the next result tile iterator. */
  typename std::list<ResultTileWithBitmap<BitmapType>>::iterator get_next() {
    std::unique_lock<std::mutex> ul(mtx_);
    ensure_not_end();
    return it_++;
  }

 private:
  /* ********************************* */
  /*         PRIVATE METHODS           */
  /* ********************************* */

  /**
   * Make sure that we are not at the end of a fragment list by moving to the
   * next fragment that still has tiles available.
   */
  void ensure_not_end() {
    while (it_ == result_tiles_->at(f_).end()) {
      if (f_ == result_tiles_->size() - 1) {
        break;
      }
      f_++;
      it_ = result_tiles_->at(f_).begin();
    }
  }

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /**
   * List of tiles, per fragments. For unordered with dups reader, everything
   * will be in fragment 0.
   */
  ResultTileListPerFragment<BitmapType>* result_tiles_;

  /** Iterator to the current result tile. */
  typename std::list<ResultTileWithBitmap<BitmapType>>::iterator it_;

  /** Fragment index. */
  unsigned f_;

  /** Mutex to make the iterator thread safe. */
  std::mutex mtx_;
};

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

    /** Is the reader done with the query. */
    bool done_adding_result_tiles_;
  };

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  SparseIndexReaderBase(
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
  ~SparseIndexReaderBase() = default;

  /* ********************************* */
  /*          PUBLIC METHODS           */
  /* ********************************* */

  /** Returns the current read state. */
  const ReadState* read_state() const;

  /** Initializes the reader. */
  Status init();

  /** Resize the output buffers to the correct size after copying. */
  Status resize_output_buffers(uint64_t cells_copied);

  /** Returns the current read state. */
  ReadState* read_state();

 protected:
  /* ********************************* */
  /*       PROTECTED ATTRIBUTES        */
  /* ********************************* */

  /** Read state. */
  ReadState read_state_;

  /** Have we loaded all thiles for this fragment. */
  std::vector<bool> all_tiles_loaded_;

  /** Dimension names. */
  std::vector<std::string> dim_names_;

  /** Are dimensions var sized. */
  std::vector<bool> is_dim_var_size_;

  /** Reverse sorted vector, per fragments, of tiles ranges in the subarray, if
   * set. */
  std::vector<std::vector<std::pair<uint64_t, uint64_t>>> result_tile_ranges_;

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
  uint64_t memory_used_qc_tiles_total_;

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

  /** Indicate if the coordinates are loaded for the result tiles. */
  bool coords_loaded_;

  /** Are we in elements mode. */
  bool elements_mode_;

  /** Names of dim/attr loaded for query condition. */
  std::vector<std::string> qc_loaded_names_;

  /* ********************************* */
  /*         PROTECTED METHODS         */
  /* ********************************* */

  /** Get the coordinate tiles size for a dimension. */
  template <class BitmapType>
  Status get_coord_tiles_size(
      bool include_coords,
      unsigned dim_num,
      unsigned f,
      uint64_t t,
      uint64_t* tiles_size,
      uint64_t* tiles_size_qc);

  /** Load tile offsets and result tile ranges. */
  Status load_initial_data();

  /** Read and unfilter coord tiles. */
  Status read_and_unfilter_coords(
      bool include_coords, const std::vector<ResultTile*>* result_tiles);

  /** Compute tile bitmaps. */
  template <class BitmapType>
  Status compute_tile_bitmaps(
      ResultTileListPerFragment<BitmapType>* result_tiles);

  /** Apply query condition. */
  template <class BitmapType>
  Status apply_query_condition(
      ResultTileListPerFragment<BitmapType>* result_tiles);

  /**
   * Adds an extra offset in the end of the offsets buffer indicating the
   * returned data size if an attribute is var-sized.
   */
  Status add_extra_offset();

  /** Remove a result tile range for a specific fragment */
  void remove_result_tile_range(uint64_t f);

  /** Remove a result tile range for a specific range */
  void remove_range_result_tile_range(uint64_t r);
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_SPARSE_INDEX_READER_BASE_H