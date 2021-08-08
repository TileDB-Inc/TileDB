/**
 * @file   reader_base.h
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
 * This file defines class ReaderBase.
 */

#ifndef TILEDB_READER_BASE_H
#define TILEDB_READER_BASE_H

#include <queue>
#include "strategy_base.h"
#include "tiledb/common/status.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/query/query_condition.h"
#include "tiledb/sm/query/result_cell_slab.h"

namespace tiledb {
namespace sm {

class Array;
class ArraySchema;
class StorageManager;
class Subarray;

/** Processes read queries. */
class ReaderBase : public StrategyBase {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  ReaderBase(
      stats::Stats* stats,
      StorageManager* storage_manager,
      Array* array,
      Config& config,
      std::unordered_map<std::string, QueryBuffer>& buffers,
      Subarray& subarray,
      Layout layout,
      QueryCondition& condition);

  /** Destructor. */
  ~ReaderBase() = default;

 protected:
  /* ********************************* */
  /*        PROTECTED DATATYPES        */
  /* ********************************* */

  /** Bitflags for individual dimension/attributes in `process_tiles()`. */
  typedef uint8_t ProcessTileFlags;

  /** Bitflag values applicable to `ProcessTileFlags`. */
  enum ProcessTileFlag { READ = 1, COPY = 2 };

  typedef std::
      unordered_map<ResultTile*, std::vector<std::pair<uint64_t, uint64_t>>>
          ResultCellSlabsIndex;

  /* ********************************* */
  /*       PROTECTED ATTRIBUTES        */
  /* ********************************* */

  /** The query condition. */
  QueryCondition& condition_;

  /** The fragment metadata that the reader will focus on. */
  std::vector<FragmentMetadata*> fragment_metadata_;

  /** Protects result tiles. */
  mutable std::mutex result_tiles_mutex_;

  /** Try to fix overflows on var sized copies. */
  bool fix_var_sized_overflows_;

  /** Clear the coordinates tiles after copies. */
  bool clear_coords_tiles_on_copy_;

  /** Was there an overflow during copying tiles. */
  bool copy_overflowed_;

  /**
   * Used to specify where in the result cell slabs to end the copy
   * operations. First is the size of the result cell slabs, second is
   * the length of the last result cell slab.
   */
  std::pair<uint64_t, uint64_t> copy_end_;

  /* ********************************* */
  /*         PROTECTED METHODS         */
  /* ********************************* */

  /**
   * Deletes the tiles on the input attribute/dimension from the result tiles.
   *
   * @param name The attribute/dimension name.
   * @param result_tiles The result tiles to delete from.
   * @return void
   */
  void clear_tiles(
      const std::string& name,
      const std::vector<ResultTile*>* result_tiles) const;

  /**
   * Resets the buffer sizes to the original buffer sizes. This is because
   * the read query may alter the buffer sizes to reflect the size of
   * the useful data (results) written in the buffers.
   */
  void reset_buffer_sizes();

  /** Zeroes out the user buffer sizes, indicating an empty result. */
  void zero_out_buffer_sizes();

  /** Correctness checks for `subarray_`. */
  Status check_subarray() const;

  /** Correctness checks validity buffer sizes in `buffers_`. */
  Status check_validity_buffer_sizes() const;

  /**
   * Loads tile offsets for each attribute/dimension name into
   * their associated element in `fragment_metadata_`.
   *
   * @param subarray The subarray to load tiles for.
   * @param names The attribute/dimension names.
   * @return Status
   */
  Status load_tile_offsets(
      Subarray* subarray, const std::vector<std::string>* names);

  /**
   * Initializes a fixed-sized tile.
   *
   * @param format_version The format version of the tile.
   * @param name The attribute/dimension the tile belongs to.
   * @param tile The tile to be initialized.
   * @return Status
   */
  Status init_tile(
      uint32_t format_version, const std::string& name, Tile* tile) const;

  /**
   * Initializes a var-sized tile.
   *
   * @param format_version The format version of the tile.
   * @param name The attribute/dimension the tile belongs to.
   * @param tile The offsets tile to be initialized.
   * @param tile_var The var-sized data tile to be initialized.
   * @return Status
   */
  Status init_tile(
      uint32_t format_version,
      const std::string& name,
      Tile* tile,
      Tile* tile_var) const;

  /**
   * Initializes a fixed-sized tile.
   *
   * @param format_version The format version of the tile.
   * @param name The attribute/dimension the tile belongs to.
   * @param tile The tile to be initialized.
   * @param tile_validity The validity tile to be initialized.
   * @return Status
   */
  Status init_tile_nullable(
      uint32_t format_version,
      const std::string& name,
      Tile* tile,
      Tile* tile_validity) const;

  /**
   * Initializes a var-sized tile.
   *
   * @param format_version The format version of the tile.
   * @param name The attribute/dimension the tile belongs to.
   * @param tile The offsets tile to be initialized.
   * @param tile_var The var-sized data tile to be initialized.
   * @param tile_validity The validity tile to be initialized.
   * @return Status
   */
  Status init_tile_nullable(
      uint32_t format_version,
      const std::string& name,
      Tile* tile,
      Tile* tile_var,
      Tile* tile_validity) const;

  /**
   * Concurrently executes `read_tiles` for each name in `names`. This
   * must be the entry point for reading attribute tiles because it
   * generates stats for reading attributes.
   *
   * @param names The attribute names.
   * @param result_tiles The retrieved tiles will be stored inside the
   *     `ResultTile` instances in this vector.
   * @return Status
   */
  Status read_attribute_tiles(
      const std::vector<std::string>* names,
      const std::vector<ResultTile*>* result_tiles) const;

  /**
   * Concurrently executes `read_tiles` for each name in `names`. This
   * must be the entry point for reading coordinate tiles because it
   * generates stats for reading coordinates.
   *
   * @param names The coordinate/dimension names.
   * @param result_tiles The retrieved tiles will be stored inside the
   *     `ResultTile` instances in this vector.
   * @return Status
   */
  Status read_coordinate_tiles(
      const std::vector<std::string>* names,
      const std::vector<ResultTile*>* result_tiles) const;

  /**
   * Concurrently executes `read_tiles` for each name in `names`.
   *
   * @param names The attribute/dimension names.
   * @param result_tiles The retrieved tiles will be stored inside the
   *     `ResultTile` instances in this vector.
   * @return Status
   */
  Status read_tiles(
      const std::vector<std::string>* names,
      const std::vector<ResultTile*>* result_tiles) const;

  /**
   * Retrieves the tiles on a particular attribute or dimension and stores it
   * in the appropriate result tile.
   *
   * @param name The attribute/dimension name.
   * @param result_tiles The retrieved tiles will be stored inside the
   *     `ResultTile` instances in this vector.
   * @return Status
   */
  Status read_tiles(
      const std::string& name,
      const std::vector<ResultTile*>* result_tiles) const;

  /**
   * Retrieves the tiles on a particular attribute or dimension and stores it
   * in the appropriate result tile.
   *
   * The reads are done asynchronously, and futures for each read operation are
   * added to the output parameter.
   *
   * @param name The attribute/dimension name.
   * @param result_tiles The retrieved tiles will be stored inside the
   *     `ResultTile` instances in this vector.
   * @param tasks Vector to hold futures for the read tasks.
   * @return Status
   */
  Status read_tiles(
      const std::string& name,
      const std::vector<ResultTile*>* result_tiles,
      std::vector<ThreadPool::Task>* tasks) const;

  /**
   * Filters the tiles on a particular attribute/dimension from all input
   * fragments based on the tile info in `result_tiles`.
   *
   * @param name Attribute/dimension whose tiles will be unfiltered.
   * @param result_tiles Vector containing the tiles to be unfiltered.
   * @return Status
   */
  Status unfilter_tiles(
      const std::string& name,
      const std::vector<ResultTile*>* result_tiles) const;

  /**
   * Runs the input fixed-sized tile for the input attribute or dimension
   * through the filter pipeline. The tile buffer is modified to contain the
   * output of the pipeline.
   *
   * @param name The attribute/dimension the tile belong to.
   * @param tile The tile to be unfiltered.
   * @return Status
   */
  Status unfilter_tile(const std::string& name, Tile* tile) const;

  /**
   * Runs the input var-sized tile for the input attribute or dimension through
   * the filter pipeline. The tile buffer is modified to contain the output of
   * the pipeline.
   *
   * @param name The attribute/dimension the tile belong to.
   * @param tile The offsets tile to be unfiltered.
   * @param tile_var The value tile to be unfiltered.
   * @return Status
   */
  Status unfilter_tile(
      const std::string& name, Tile* tile, Tile* tile_var) const;

  /**
   * Runs the input fixed-sized tile for the input nullable attribute
   * through the filter pipeline. The tile buffer is modified to contain the
   * output of the pipeline.
   *
   * @param name The attribute/dimension the tile belong to.
   * @param tile The tile to be unfiltered.
   * @param tile_validity The validity tile to be unfiltered.
   * @return Status
   */
  Status unfilter_tile_nullable(
      const std::string& name, Tile* tile, Tile* tile_validity) const;

  /**
   * Runs the input var-sized tile for the input nullable attribute through
   * the filter pipeline. The tile buffer is modified to contain the output of
   * the pipeline.
   *
   * @param name The attribute/dimension the tile belong to.
   * @param tile The offsets tile to be unfiltered.
   * @param tile_var The value tile to be unfiltered.
   * @param tile_validity The validity tile to be unfiltered.
   * @return Status
   */
  Status unfilter_tile_nullable(
      const std::string& name,
      Tile* tile,
      Tile* tile_var,
      Tile* tile_validity) const;

  /**
   * Copies the result coordinates to the user buffers.
   * It also appropriately cleans up the used result tiles.
   */
  Status copy_coordinates(
      const std::vector<ResultTile*>* result_tiles,
      std::vector<ResultCellSlab>* result_cell_slabs);

  /**
   * Copies the result attribute values to the user buffers.
   * It also appropriately cleans up the used result tiles.
   */
  Status copy_attribute_values(
      uint64_t stride,
      std::vector<ResultTile*>* result_tiles,
      std::vector<ResultCellSlab>* result_cell_slabs,
      Subarray& subarray,
      uint64_t memory_budget = UINT64_MAX);

  /**
   * Copies the cells for the input **fixed-sized** attribute/dimension and
   * result cell slabs into the corresponding result buffers.
   *
   * @param name The targeted attribute/dimension.
   * @param stride If it is `UINT64_MAX`, then the cells in the result
   *     cell slabs are all contiguous. Otherwise, each cell in the
   *     result cell slabs are `stride` cells apart from each other.
   * @param result_cell_slabs The result cell slabs to copy cells for.
   * @param fixed_cs_partitions The cell slab partitions.
   * @return Status
   */
  Status copy_fixed_cells(
      const std::string& name,
      uint64_t stride,
      const std::vector<ResultCellSlab>* result_cell_slabs,
      std::vector<size_t>* fixed_cs_partitions);

  /**
   * Compute cs partitions for fixed-sized cell copying.
   *
   * @param result_cell_slabs The result cell slabs to copy cells for.
   * @param fixed_cs_partitions The output partitions.
   */
  void compute_fixed_cs_partitions(
      const std::vector<ResultCellSlab>* result_cell_slabs,
      std::vector<size_t>* fixed_cs_partitions);

  /**
   * Returns the configured bytesize for var-sized attribute offsets
   */
  uint64_t offsets_bytesize() const;

  /**
   * Copies the cells for the input **fixed-sized** attribute/dimension and
   * result cell slabs into the corresponding result buffers for the
   * partition in `ctx_cache` at index `partition_idx`.
   *
   * @param name The partition index.
   * @param name The targeted attribute/dimension.
   * @param stride If it is `UINT64_MAX`, then the cells in the result
   *     cell slabs are all contiguous. Otherwise, each cell in the
   *     result cell slabs are `stride` cells apart from each other.
   * @param result_cell_slabs The result cell slabs to copy cells for.
   * @param cs_offsets The cell slab offsets.
   * @param cs_partitions The cell slab partitions to operate on.
   * @return Status
   */
  Status copy_partitioned_fixed_cells(
      size_t partition_idx,
      const std::string* name,
      uint64_t stride,
      const std::vector<ResultCellSlab>* result_cell_slabs,
      const std::vector<uint64_t>* cs_offsets,
      const std::vector<size_t>* cs_partitions);

  /**
   * Copies the cells for the input **var-sized** attribute/dimension and result
   * cell slabs into the corresponding result buffers.
   *
   * @param name The targeted attribute/dimension.
   * @param stride If it is `UINT64_MAX`, then the cells in the result
   *     cell slabs are all contiguous. Otherwise, each cell in the
   *     result cell slabs are `stride` cells apart from each other.
   * @param result_cell_slabs The result cell slabs to copy cells for.
   * @param var_cs_partitions The cell slab partitions.
   * @param total_cs_length The total cell slab length.
   * @return Status
   */
  Status copy_var_cells(
      const std::string& name,
      uint64_t stride,
      std::vector<ResultCellSlab>* result_cell_slabs,
      std::vector<std::pair<size_t, size_t>>* var_cs_partitions,
      size_t total_var_cs_length);

  /**
   * Compute cs partitions for var-sized cell copying.
   *
   * @param result_cell_slabs The result cell slabs to copy cells for.
   * @param var_cs_partitions The output partitions.
   * @param total_var_cs_length The total cell slab length.
   */
  void compute_var_cs_partitions(
      const std::vector<ResultCellSlab>* result_cell_slabs,
      std::vector<std::pair<size_t, size_t>>* var_cs_partitions,
      size_t* total_var_cs_length);

  /**
   * Computes offsets into destination buffers for the given
   * attribute/dimensions's offset and variable-length data, for the given list
   * of result cell slabs.
   *
   * @param name The variable-length attribute/dimension.
   * @param stride If it is `UINT64_MAX`, then the cells in the result
   *     cell slabs are all contiguous. Otherwise, each cell in the
   *     result cell slabs are `stride` cells apart from each other.
   * @param result_cell_slabs The result cell slabs to compute destinations for.
   * @param offset_offsets_per_cs Output to hold one vector per result cell
   *    slab, and one element per cell in the slab. The elements are the
   *    destination offsets for the attribute's offsets.
   * @param var_offsets_per_cs Output to hold one vector per result cell slab,
   *    and one element per cell in the slab. The elements are the destination
   *    offsets for the attribute's variable-length data.
   * @param total_offset_size Output set to the total size in bytes of the
   *    offsets in the given list of result cell slabs.
   * @param total_var_size Output set to the total size in bytes of the
   *    attribute's variable-length in the given list of result cell slabs.
   * @param total_validity_size Output set to the total size in bytes of the
   *    attribute's validity vector in the given list of result cell slabs.
   * @return Status
   */
  Status compute_var_cell_destinations(
      const std::string& name,
      uint64_t stride,
      std::vector<ResultCellSlab>* result_cell_slabs,
      std::vector<uint64_t>* offset_offsets_per_cs,
      std::vector<uint64_t>* var_offsets_per_cs,
      uint64_t* total_offset_size,
      uint64_t* total_var_size,
      uint64_t* total_validity_size);

  /**
   * Copies the cells for the input **var-sized** attribute/dimension and result
   * cell slabs into the corresponding result buffers for the
   * partition in `cs_partitions` at index `partition_idx`.
   *
   * @param name The partition index.
   * @param name The targeted attribute/dimension.
   * @param stride If it is `UINT64_MAX`, then the cells in the result
   *     cell slabs are all contiguous. Otherwise, each cell in the
   *     result cell slabs are `stride` cells apart from each other.
   * @param result_cell_slabs The result cell slabs to copy cells for.
   * @param offset_offsets_per_cs Maps each cell slab to its offset
   *     for its attribute offsets.
   * @param var_offsets_per_cs Maps each cell slab to its offset
   *     for its variable-length data.
   * @param cs_partitions The cell slab partitions to operate on.
   * @return Status
   */
  Status copy_partitioned_var_cells(
      size_t partition_idx,
      const std::string* name,
      uint64_t stride,
      const std::vector<ResultCellSlab>* result_cell_slabs,
      const std::vector<uint64_t>* offset_offsets_per_cs,
      const std::vector<uint64_t>* var_offsets_per_cs,
      const std::vector<std::pair<size_t, size_t>>* cs_partitions);

  /**
   * For each dimension/attribute in `names`, performs the actions
   * defined in the `ProcessTileFlags`.
   *
   * @param names The dimension/attribute names to process.
   * @param result_tiles The retrieved tiles will be stored inside the
   *   `ResultTile` instances in this vector.
   * @param result_cell_slabs The cell slabs to process.
   * @param subarray Specifies the current subarray.
   * @param stride The stride between cells, UINT64_MAX for contiguous.
   * @param memory_budget The memory budget, UINT64_MAX for unlimited.
   * @param memory_used_for_tiles The memory used for tiles that will not be
   * unloaded.
   */
  Status process_tiles(
      const std::unordered_map<std::string, ProcessTileFlags>* names,
      std::vector<ResultTile*>* result_tiles,
      std::vector<ResultCellSlab>* result_cell_slabs,
      Subarray* subarray,
      uint64_t stride,
      uint64_t memory_budget,
      uint64_t* memory_used_for_tiles);

  /**
   * Builds and returns an association from each tile in `result_cell_slabs`
   * to the cell slabs it contains.
   */
  tdb_unique_ptr<ResultCellSlabsIndex> compute_rcs_index(
      const std::vector<ResultCellSlab>* result_cell_slabs) const;

  /**
   * Applies the query condition, `condition_`, to filter cell indexes
   * within `result_cell_slabs`. This mutates `result_cell_slabs`.
   *
   * @param result_cell_slabs The unfiltered cell slabs.
   * @param result_tiles The result tiles that must contain values for
   *   attributes within `condition_`.
   * @param subarray Specifies the current subarray.
   * @param stride The stride between cells, defaulting to UINT64_MAX
   *   for contiguous cells.
   * @param memory_budget_rcs The memory budget for tiles, defaulting
   *   to UINT64_MAX for unlimited budget.
   * @param memory_budget_tiles The memory budget for result cell slabs,
   *   defaulting to UINT64_MAX for unlimited budget.
   * @param memory_used_for_tiles The memory used for tiles that will
   *   not be unloaded.
   * @return Status
   */
  Status apply_query_condition(
      std::vector<ResultCellSlab>* result_cell_slabs,
      std::vector<ResultTile*>* result_tiles,
      Subarray* subarray,
      uint64_t stride = UINT64_MAX,
      uint64_t memory_budget_rcs = UINT64_MAX,
      uint64_t memory_budget_tiles = UINT64_MAX,
      uint64_t* memory_used_for_tiles = nullptr);

  /**
   * Get the size of an attribute tile.
   *
   * @param name The attribute name.
   * @param result_tile The result tile.
   * @param tile_size The return tile size.
   * @return Status
   */
  Status get_attribute_tile_size(
      const std::string& name, ResultTile* result_tile, uint64_t* tile_size);
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_READER_BASE_H