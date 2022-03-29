/**
 * @file   dense_reader.h
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
 * This file defines class DenseReader.
 */

#ifndef TILEDB_DENSE_READER
#define TILEDB_DENSE_READER

#include <atomic>

#include "tiledb/common/logger_public.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/query/iquery_strategy.h"
#include "tiledb/sm/query/query_buffer.h"
#include "tiledb/sm/query/reader_base.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class Array;
class StorageManager;

/** Processes dense read queries. */
class DenseReader : public ReaderBase, public IQueryStrategy {
 public:
  /* ********************************* */
  /*          TYPE DEFINITIONS         */
  /* ********************************* */

  /** Range information, for a dimension, used for row/col reads. */
  struct RangeInfo {
    /** Cell offset, per range for this dimension. */
    std::vector<uint64_t> cell_offsets_;

    /** Multiplier to be used in offset computation. */
    uint64_t multiplier_;
  };

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  DenseReader(
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
  ~DenseReader() = default;

  DISABLE_COPY_AND_COPY_ASSIGN(DenseReader);
  DISABLE_MOVE_AND_MOVE_ASSIGN(DenseReader);

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

  /** Returns the status details reason. */
  QueryStatusDetailsReason status_incomplete_reason() const;

  /** Initializes the reader. */
  Status init();

  /** Initialize the memory budget variables. */
  Status initialize_memory_budget();

  /** Returns the current read state. */
  const ReadState* read_state() const;

  /** Returns the current read state. */
  ReadState* read_state();

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

  /** Read state. */
  ReadState read_state_;

  /** Are we in elements mode. */
  bool elements_mode_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /** Performs a read on a dense array. */
  template <class OffType>
  Status dense_read();

  /** Performs a read on a dense array. */
  template <class DimType, class OffType>
  Status dense_read();

  /** Initializes the read state. */
  Status init_read_state();

  /** Apply the query condition. */
  template <class DimType, class OffType>
  tuple<Status, optional<std::vector<uint8_t>>> apply_query_condition(
      Subarray& subarray,
      std::vector<Subarray>& tile_subarrays,
      std::vector<uint64_t>& tile_offsets,
      const std::vector<RangeInfo>& range_info,
      std::map<const DimType*, ResultSpaceTile<DimType>>& result_space_tiles);

  /** Fix offsets buffer after reading all offsets. */
  template <class OffType>
  uint64_t fix_offsets_buffer(
      const std::string& name,
      const bool nullable,
      const uint64_t cell_num,
      std::vector<void*>& var_data);

  /** Read attributes into the users buffers. */
  template <class DimType, class OffType>
  Status read_attributes(
      const std::vector<std::string>& fixed_names,
      const std::vector<std::string>& var_names,
      const Subarray& subarray,
      const std::vector<Subarray>& tile_subarrays,
      const std::vector<uint64_t>& tile_offsets,
      const std::vector<RangeInfo>& range_info,
      std::map<const DimType*, ResultSpaceTile<DimType>>& result_space_tiles,
      const std::vector<uint8_t>& qc_result);

  /** Get the cell position within a tile. */
  template <class DimType>
  uint64_t get_cell_pos_in_tile(
      const Layout& cell_order,
      const int32_t dim_num,
      const Domain* const domain,
      const ResultSpaceTile<DimType>& result_space_tile,
      const DimType* const coords);

  /**
   * Checks if a cell slab overlaps a fragment domain range and returns the
   * start and end of the overlap.
   */
  template <class DimType>
  tuple<bool, uint64_t, uint64_t> cell_slab_overlaps_range(
      const unsigned dim_num,
      const NDRange& ndrange,
      const DimType* const coords,
      const uint64_t length);

  /** Get the cell offset in the output buffers to copy data to. */
  template <class DimType>
  uint64_t get_dest_cell_offset_row_col(
      const int32_t dim_num,
      const Subarray& subarray,
      const Subarray& tile_subarray,
      const DimType* const coords,
      const DimType* const range_coords,
      const std::vector<RangeInfo>& range_info);

  /** Copy fixed tiles to the output buffers. */
  template <class DimType>
  Status copy_fixed_tiles(
      const std::vector<std::string>& names,
      const std::vector<uint8_t*>& dst_bufs,
      const std::vector<uint8_t*>& dst_val_bufs,
      const std::vector<const Attribute*>& attributes,
      const std::vector<uint64_t>& cell_sizes,
      ResultSpaceTile<DimType>& result_space_tile,
      const Subarray& subarray,
      const Subarray& tile_subarray,
      const uint64_t global_cell_offset,
      const std::vector<RangeInfo>& range_info,
      const std::vector<uint8_t>& qc_result);

  /** Copy a tile var offsets to the output buffers. */
  template <class DimType, class OffType>
  Status copy_offset_tiles(
      const std::vector<std::string>& names,
      const std::vector<uint8_t*>& dst_bufs,
      const std::vector<uint8_t*>& dst_val_bufs,
      const std::vector<const Attribute*>& attributes,
      const std::vector<uint64_t>& data_type_sizes,
      ResultSpaceTile<DimType>& result_space_tile,
      const Subarray& subarray,
      const Subarray& tile_subarray,
      const uint64_t global_cell_offset,
      std::vector<std::vector<void*>>& var_data,
      const std::vector<RangeInfo>& range_info,
      const std::vector<uint8_t>& qc_result);

  /** Copy a var tile to the output buffers. */
  template <class DimType, class OffType>
  Status copy_var_tiles(
      const std::vector<std::string>& names,
      const std::vector<uint8_t*>& dst_bufs,
      const std::vector<uint8_t*>& offsets_bufs,
      const std::vector<uint64_t>& data_type_sizes,
      const Subarray& subarray,
      const Subarray& tile_subarray,
      const uint64_t global_cell_offset,
      std::vector<std::vector<void*>>& var_data,
      const std::vector<RangeInfo>& range_info,
      bool last_tile,
      std::vector<uint64_t>& var_buffer_sizes);

  /**
   * Adds an extra offset in the end of the offsets buffer indicating the
   * returned data size if an attribute is var-sized.
   */
  Status add_extra_offset();

  /** Perform necessary checks before exiting a read loop */
  Status complete_read_loop();
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_DENSE_READER
