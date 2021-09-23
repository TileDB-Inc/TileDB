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
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  DenseReader(
      stats::Stats* stats,
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

  /** Initializes the reader. */
  Status init();

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
  Status apply_query_condition(
      Subarray* subarray,
      std::vector<Subarray>* tile_subarrays,
      std::vector<uint64_t>* tile_offsets,
      std::map<const DimType*, ResultSpaceTile<DimType>>* result_space_tiles,
      std::vector<uint8_t>* qc_result);

  /** Fix offsets buffer after reading all offsets. */
  template <class OffType>
  void fix_offsets_buffer(
      const std::string& name,
      const bool nullable,
      const uint64_t cell_num,
      std::vector<void*>* var_data,
      uint64_t* var_buffer_size);

  /** Read attributes into the users buffers. */
  template <class DimType, class OffType>
  Status read_attributes(
      const std::vector<std::string>* fixed_names,
      const std::vector<std::string>* var_names,
      const Subarray* const subarray,
      const std::vector<Subarray>* const tile_subarrays,
      const std::vector<uint64_t>* const tile_offsets,
      std::map<const DimType*, ResultSpaceTile<DimType>>* result_space_tiles,
      const std::vector<uint8_t>* const qc_result);

  /** Get the cell position within a tile. */
  template <class DimType>
  Status get_cell_pos_in_tile(
      const Layout& cell_order,
      const int32_t dim_num,
      const Domain* const domain,
      const ResultSpaceTile<DimType>* const result_space_tile,
      const DimType* const coords,
      uint64_t* cell_pos);

  /**
   * Checks if a cell slab overlaps a fragment domain range and returns the
   * start and end of the overlap.
   */
  template <class DimType>
  bool cell_slab_overlaps_range(
      const unsigned dim_num,
      const NDRange& ndrange,
      const DimType* const coords,
      const uint64_t length,
      uint64_t* start,
      uint64_t* end);

  /** Get the cell offset in the output buffers to copy data to. */
  template <class DimType>
  Status get_dest_cell_offset_row_col(
      const int32_t dim_num,
      const Subarray* const subarray,
      const DimType* const coords,
      uint64_t* cell_offset);

  /** Copy fixed tiles to the output buffers. */
  template <class DimType>
  Status copy_fixed_tiles(
      const std::vector<std::string>* names,
      const std::vector<uint8_t*>* const dst_bufs,
      const std::vector<uint8_t*>* const dst_val_bufs,
      const std::vector<const Attribute*>* const attributes,
      const std::vector<uint64_t>* const cell_sizes,
      ResultSpaceTile<DimType>* result_space_tile,
      const Subarray* const subarray,
      const Subarray* const tile_subarray,
      const uint64_t global_cell_offset,
      const std::vector<uint8_t>* const qc_result);

  /** Copy a tile var offsets to the output buffers. */
  template <class DimType, class OffType>
  Status copy_offset_tiles(
      const std::vector<std::string>* names,
      const std::vector<uint8_t*>* const dst_bufs,
      const std::vector<uint8_t*>* const dst_val_bufs,
      const std::vector<const Attribute*>* const attributes,
      const std::vector<uint64_t>* const data_type_sizes,
      ResultSpaceTile<DimType>* result_space_tile,
      const Subarray* const subarray,
      const Subarray* const tile_subarray,
      const uint64_t global_cell_offset,
      std::vector<std::vector<void*>>* var_data,
      const std::vector<uint8_t>* const qc_result);

  /** Copy a var tile to the output buffers. */
  template <class DimType, class OffType>
  Status copy_var_tiles(
      const std::vector<std::string>* names,
      const std::vector<uint8_t*>* const dst_bufs,
      const std::vector<uint8_t*>* const offsets_bufs,
      const std::vector<uint64_t>* const data_type_sizes,
      const Subarray* const subarray,
      const Subarray* const tile_subarray,
      const uint64_t global_cell_offset,
      std::vector<std::vector<void*>>* var_data,
      bool last_tile,
      std::vector<uint64_t>* var_buffer_sizes);

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
