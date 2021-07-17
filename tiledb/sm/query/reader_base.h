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

  // TODO Remove selective unfiltering.
  /** Bitflags for individual dimension/attributes in `process_tiles()`. */
  typedef uint8_t ProcessTileFlags;

  /** Bitflag values applicable to `ProcessTileFlags`. */
  enum ProcessTileFlag { READ = 1, COPY = 2, SELECTIVE_UNFILTERING = 4 };

  typedef std::
      unordered_map<ResultTile*, std::vector<std::pair<uint64_t, uint64_t>>>
          ResultCellSlabsIndex;

  class CopyFixedCellsContextCache {
   public:
    /** Constructor. */
    CopyFixedCellsContextCache()
        : initialized_(false)
        , num_cs_(0) {
    }

    /** Destructor. */
    ~CopyFixedCellsContextCache() = default;

    DISABLE_COPY_AND_COPY_ASSIGN(CopyFixedCellsContextCache);
    DISABLE_MOVE_AND_MOVE_ASSIGN(CopyFixedCellsContextCache);

    /**
     * Initializes this instance on the first invocation, otherwise
     * this is a no-op.
     *
     * @param result_cell_slabs The cell slabs.
     * @param num_copy_threads The number of threads used in the copy path.
     */
    void initialize(
        const std::vector<ResultCellSlab>& result_cell_slabs,
        const int num_copy_threads) {
      // Without locking the `mutex_`, check if this instance
      // has been initialized.
      if (initialized_)
        return;

      std::lock_guard<std::mutex> lg(mutex_);

      // Re-check if this instance has been initialized.
      if (initialized_)
        return;

      // Store the number of cell slabs.
      num_cs_ = result_cell_slabs.size();

      // Calculate the partition sizes.
      const uint64_t num_cs_partitions =
          std::min<uint64_t>(num_copy_threads, num_cs_);
      const uint64_t cs_per_partition = num_cs_ / num_cs_partitions;
      const uint64_t cs_per_partition_carry = num_cs_ % num_cs_partitions;

      // Calculate the partition offsets.
      uint64_t num_cs_partitioned = 0;
      cs_partitions_.reserve(num_cs_partitions);
      for (uint64_t i = 0; i < num_cs_partitions; ++i) {
        const uint64_t num_cs_in_partition =
            cs_per_partition + ((i < cs_per_partition_carry) ? 1 : 0);
        num_cs_partitioned += num_cs_in_partition;
        cs_partitions_.emplace_back(num_cs_partitioned);
      }

      initialized_ = true;
    }

    /** Returns the `cs_partitions_`. */
    const std::vector<size_t>* cs_partitions() const {
      // We do protect this with `mutex_` because it is only
      // mutated on initialization.
      return &cs_partitions_;
    }

    /** Returns a pre-sized vector to store cell slab offsets. */
    tdb_unique_ptr<std::vector<uint64_t>> get_cs_offsets() {
      std::lock_guard<std::mutex> lg(mutex_);

      // Re-use a vector in the `cs_offsets_cache_` if possible,
      // otherwise create a new vector of size `num_cs_`.
      tdb_unique_ptr<std::vector<uint64_t>> cs_offsets;
      if (!cs_offsets_cache_.empty()) {
        cs_offsets = std::move(cs_offsets_cache_.front());
        assert(cs_offsets->size() == num_cs_);
        cs_offsets_cache_.pop();
      } else {
        cs_offsets =
            tdb_unique_ptr<std::vector<uint64_t>>(new std::vector<uint64_t>());
        cs_offsets->resize(num_cs_);
      }

      return cs_offsets;
    }

    /** Returns a vector fetched from `get_cs_offsets`. */
    void cache_cs_offsets(tdb_unique_ptr<std::vector<uint64_t>>&& cs_offsets) {
      assert(cs_offsets->size() == num_cs_);
      std::lock_guard<std::mutex> lg(mutex_);
      cs_offsets_cache_.push(std::move(cs_offsets));
    }

   private:
    /** Protects all member variables. */
    std::mutex mutex_;

    /** True if the context cache has been initialized. */
    bool initialized_;

    /**
     * Logical partitions of `cs_offsets`. Each element is the
     * partition's starting index.
     */
    std::vector<size_t> cs_partitions_;

    /** The number of cell slabs. */
    size_t num_cs_;

    /**
     * A pool of vectors that maps the index of each cell slab
     * to its offset in the output buffer.
     */
    std::queue<tdb_unique_ptr<std::vector<uint64_t>>> cs_offsets_cache_;
  };

  class CopyVarCellsContextCache {
   public:
    /** Constructor. */
    CopyVarCellsContextCache()
        : initialized_(false)
        , total_cs_length_(0) {
    }

    /** Destructor. */
    ~CopyVarCellsContextCache() = default;

    DISABLE_COPY_AND_COPY_ASSIGN(CopyVarCellsContextCache);
    DISABLE_MOVE_AND_MOVE_ASSIGN(CopyVarCellsContextCache);

    /**
     * Initializes this instance on the first invocation, otherwise
     * this is a no-op.
     *
     * @param result_cell_slabs The cell slabs.
     * @param num_copy_threads The number of threads used in the copy path.
     */
    void initialize(
        const std::vector<ResultCellSlab>& result_cell_slabs,
        const int num_copy_threads) {
      // Without locking the `mutex_`, check if this instance
      // has been initialized.
      if (initialized_)
        return;

      std::lock_guard<std::mutex> lg(mutex_);

      // Re-check if this instance has been initialized.
      if (initialized_)
        return;

      // Calculate the partition range.
      const uint64_t num_cs = result_cell_slabs.size();
      const uint64_t num_cs_partitions =
          std::min<uint64_t>(num_copy_threads, num_cs);
      const uint64_t cs_per_partition = num_cs / num_cs_partitions;
      const uint64_t cs_per_partition_carry = num_cs % num_cs_partitions;

      // Compute the boundary between each partition. Each boundary
      // is represented by an `std::pair` that contains the total
      // length of each cell slab in the leading partition and an
      // exclusive cell slab index that ends the partition.
      uint64_t next_partition_idx = cs_per_partition;
      if (cs_per_partition_carry > 0)
        ++next_partition_idx;

      total_cs_length_ = 0;
      cs_partitions_.reserve(num_cs_partitions);
      for (uint64_t cs_idx = 0; cs_idx < num_cs; cs_idx++) {
        if (cs_idx == next_partition_idx) {
          cs_partitions_.emplace_back(total_cs_length_, cs_idx);

          // The final partition may contain extra cell slabs that did
          // not evenly divide into the partition range. Set the
          // `next_partition_idx` to zero and build the last boundary
          // after this for-loop.
          if (cs_partitions_.size() == num_cs_partitions) {
            next_partition_idx = 0;
          } else {
            next_partition_idx += cs_per_partition;
            if (cs_idx < (cs_per_partition_carry - 1))
              ++next_partition_idx;
          }
        }

        total_cs_length_ += result_cell_slabs[cs_idx].length_;
      }

      // Store the final boundary.
      cs_partitions_.emplace_back(total_cs_length_, num_cs);
    }

    /** Returns the `cs_partitions_`. */
    const std::vector<std::pair<size_t, size_t>>* cs_partitions() const {
      // We do protect this with `mutex_` because it is only
      // mutated on initialization.
      return &cs_partitions_;
    }

    /** Returns a pre-sized vector to store offset-offsets per cell slab. */
    tdb_unique_ptr<std::vector<uint64_t>> get_offset_offsets_per_cs() {
      std::lock_guard<std::mutex> lg(mutex_);

      // Re-use a vector in the `offset_offsets_per_cs_cache_` if possible,
      // otherwise create a new vector of size `total_cs_length_`.
      tdb_unique_ptr<std::vector<uint64_t>> offset_offsets_per_cs;
      if (!offset_offsets_per_cs_cache_.empty()) {
        offset_offsets_per_cs = std::move(offset_offsets_per_cs_cache_.front());
        assert(offset_offsets_per_cs->size() == total_cs_length_);
        offset_offsets_per_cs_cache_.pop();
      } else {
        offset_offsets_per_cs =
            tdb_unique_ptr<std::vector<uint64_t>>(new std::vector<uint64_t>());
        offset_offsets_per_cs->resize(total_cs_length_);
      }

      return offset_offsets_per_cs;
    }

    /** Returns a vector fetched from `get_offset_offsets_per_cs`. */
    void cache_offset_offsets_per_cs(
        tdb_unique_ptr<std::vector<uint64_t>>&& offset_offsets_per_cs) {
      assert(offset_offsets_per_cs->size() == total_cs_length_);
      std::lock_guard<std::mutex> lg(mutex_);
      offset_offsets_per_cs_cache_.push(std::move(offset_offsets_per_cs));
    }

    /** Returns a pre-sized vector to store var-offsets per cell slab. */
    tdb_unique_ptr<std::vector<uint64_t>> get_var_offsets_per_cs() {
      std::lock_guard<std::mutex> lg(mutex_);

      // Re-use a vector in the `var_offsets_per_cs_cache_` if possible,
      // otherwise create a new vector of size `total_cs_length_`.
      tdb_unique_ptr<std::vector<uint64_t>> var_offsets_per_cs;
      if (!var_offsets_per_cs_cache_.empty()) {
        var_offsets_per_cs = std::move(var_offsets_per_cs_cache_.front());
        assert(var_offsets_per_cs->size() == total_cs_length_);
        var_offsets_per_cs_cache_.pop();
      } else {
        var_offsets_per_cs =
            tdb_unique_ptr<std::vector<uint64_t>>(new std::vector<uint64_t>());
        var_offsets_per_cs->resize(total_cs_length_);
      }

      return var_offsets_per_cs;
    }

    /** Returns a vector fetched from `get_var_offsets_per_cs`. */
    void cache_var_offsets_per_cs(
        tdb_unique_ptr<std::vector<uint64_t>>&& var_offsets_per_cs) {
      assert(var_offsets_per_cs->size() == total_cs_length_);
      std::lock_guard<std::mutex> lg(mutex_);
      var_offsets_per_cs_cache_.push(std::move(var_offsets_per_cs));
    }

   private:
    /** Protects all member variables. */
    std::mutex mutex_;

    /** True if the context cache has been initialized. */
    bool initialized_;

    /**
     * Logical partitions for both `offset_offsets_per_cs` and
     * `var_offsets_per_cs`. Each element contains a pair, where the
     * first pair-element is the partition's starting index and the
     * second pair-element is the number of cell slabs in the partition.
     */
    std::vector<std::pair<size_t, size_t>> cs_partitions_;

    /** The total size of all cell slabs. */
    size_t total_cs_length_;

    /**
     * A pool of vectors that maps each cell slab to its offset
     * for its attribute offsets.
     */
    std::queue<tdb_unique_ptr<std::vector<uint64_t>>>
        offset_offsets_per_cs_cache_;

    /**
     * A pool of vectors that maps each cell slab to its offset
     * for its variable-length data.
     */
    std::queue<tdb_unique_ptr<std::vector<uint64_t>>> var_offsets_per_cs_cache_;
  };

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
      const std::vector<ResultTile*>& result_tiles) const;

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
      Subarray& subarray, const std::vector<std::string>& names);

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
      const std::vector<std::string>& names,
      const std::vector<ResultTile*>& result_tiles) const;

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
      const std::vector<std::string>& names,
      const std::vector<ResultTile*>& result_tiles) const;

  /**
   * Concurrently executes `read_tiles` for each name in `names`.
   *
   * @param names The attribute/dimension names.
   * @param result_tiles The retrieved tiles will be stored inside the
   *     `ResultTile` instances in this vector.
   * @return Status
   */
  Status read_tiles(
      const std::vector<std::string>& names,
      const std::vector<ResultTile*>& result_tiles) const;

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
      const std::vector<ResultTile*>& result_tiles) const;

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
      const std::vector<ResultTile*>& result_tiles,
      std::vector<ThreadPool::Task>* tasks) const;

  /**
   * Filters the tiles on a particular attribute/dimension from all input
   * fragments based on the tile info in `result_tiles`.
   *
   * @param name Attribute/dimension whose tiles will be unfiltered.
   * @param result_tiles Vector containing the tiles to be unfiltered.
   * @param cs_ranges An optional association from the result tile to
   *   the cell slab ranges that it contains. If given, this will be
   *   used for selective unfiltering.
   * @return Status
   */
  Status unfilter_tiles(
      const std::string& name,
      const std::vector<ResultTile*>& result_tiles,
      const ResultCellSlabsIndex* const rcs_index) const;

  /**
   * Runs the input fixed-sized tile for the input attribute or dimension
   * through the filter pipeline. The tile buffer is modified to contain the
   * output of the pipeline.
   *
   * @param name The attribute/dimension the tile belong to.
   * @param tile The tile to be unfiltered.
   * @param result_cell_slab_ranges Result cell slab ranges sorted in ascending
   *    order.
   * @return Status
   */
  Status unfilter_tile(
      const std::string& name,
      Tile* tile,
      const std::vector<std::pair<uint64_t, uint64_t>>* result_cell_slab_ranges)
      const;

  /**
   * Runs the input var-sized tile for the input attribute or dimension through
   * the filter pipeline. The tile buffer is modified to contain the output of
   * the pipeline.
   *
   * @param name The attribute/dimension the tile belong to.
   * @param tile The offsets tile to be unfiltered.
   * @param tile_var The value tile to be unfiltered.
   * @param result_cell_slab_ranges Result cell slab ranges sorted in ascending
   *    order.
   * @return Status
   */
  Status unfilter_tile(
      const std::string& name,
      Tile* tile,
      Tile* tile_var,
      const std::vector<std::pair<uint64_t, uint64_t>>* result_cell_slab_ranges)
      const;

  /**
   * Runs the input fixed-sized tile for the input nullable attribute
   * through the filter pipeline. The tile buffer is modified to contain the
   * output of the pipeline.
   *
   * @param name The attribute/dimension the tile belong to.
   * @param tile The tile to be unfiltered.
   * @param tile_validity The validity tile to be unfiltered.
   * @param result_cell_slab_ranges Result cell slab ranges sorted in ascending
   *    order.
   * @return Status
   */
  Status unfilter_tile_nullable(
      const std::string& name,
      Tile* tile,
      Tile* tile_validity,
      const std::vector<std::pair<uint64_t, uint64_t>>* result_cell_slab_ranges)
      const;

  /**
   * Runs the input var-sized tile for the input nullable attribute through
   * the filter pipeline. The tile buffer is modified to contain the output of
   * the pipeline.
   *
   * @param name The attribute/dimension the tile belong to.
   * @param tile The offsets tile to be unfiltered.
   * @param tile_var The value tile to be unfiltered.
   * @param tile_validity The validity tile to be unfiltered.
   * @param result_cell_slab_ranges Result cell slab ranges sorted in ascending
   *    order.
   * @return Status
   */
  Status unfilter_tile_nullable(
      const std::string& name,
      Tile* tile,
      Tile* tile_var,
      Tile* tile_validity,
      const std::vector<std::pair<uint64_t, uint64_t>>* result_cell_slab_ranges)
      const;

  /**
   * Copies the result coordinates to the user buffers.
   * It also appropriately cleans up the used result tiles.
   */
  Status copy_coordinates(
      const std::vector<ResultTile*>& result_tiles,
      std::vector<ResultCellSlab>& result_cell_slabs);

  /**
   * Copies the result attribute values to the user buffers.
   * It also appropriately cleans up the used result tiles.
   */
  Status copy_attribute_values(
      uint64_t stride,
      const std::vector<ResultTile*>& result_tiles,
      std::vector<ResultCellSlab>& result_cell_slabs,
      Subarray& subarray);

  /**
   * Copies the cells for the input **fixed-sized** attribute/dimension and
   * result cell slabs into the corresponding result buffers.
   *
   * @param name The targeted attribute/dimension.
   * @param stride If it is `UINT64_MAX`, then the cells in the result
   *     cell slabs are all contiguous. Otherwise, each cell in the
   *     result cell slabs are `stride` cells apart from each other.
   * @param result_cell_slabs The result cell slabs to copy cells for.
   * @param ctx_cache An opaque context cache that may be shared between
   *     calls to improve performance.
   * @return Status
   */
  Status copy_fixed_cells(
      const std::string& name,
      uint64_t stride,
      const std::vector<ResultCellSlab>& result_cell_slabs,
      CopyFixedCellsContextCache* ctx_cache);

  /**
   * Populates 'ctx_cache' for fixed-sized cell copying.
   *
   * @param result_cell_slabs The result cell slabs to copy cells for.
   * @param ctx_cache The context cache to populate.
   */
  void populate_cfc_ctx_cache(
      const std::vector<ResultCellSlab>& result_cell_slabs,
      CopyFixedCellsContextCache* ctx_cache);

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
      const std::vector<uint64_t>& cs_offsets,
      const std::vector<size_t>& cs_partitions);

  /**
   * Copies the cells for the input **var-sized** attribute/dimension and result
   * cell slabs into the corresponding result buffers.
   *
   * @param name The targeted attribute/dimension.
   * @param stride If it is `UINT64_MAX`, then the cells in the result
   *     cell slabs are all contiguous. Otherwise, each cell in the
   *     result cell slabs are `stride` cells apart from each other.
   * @param result_cell_slabs The result cell slabs to copy cells for.
   * @param ctx_cache An opaque context cache that may be shared between
   *     calls to improve performance.
   * @return Status
   */
  Status copy_var_cells(
      const std::string& name,
      uint64_t stride,
      std::vector<ResultCellSlab>& result_cell_slabs,
      CopyVarCellsContextCache* ctx_cache);

  /**
   * Populates 'ctx_cache' for var-sized cell copying.
   *
   * @param result_cell_slabs The result cell slabs to copy cells for.
   * @param ctx_cache The context cache to populate.
   */
  void populate_cvc_ctx_cache(
      const std::vector<ResultCellSlab>& result_cell_slabs,
      CopyVarCellsContextCache* ctx_cache);

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
      std::vector<ResultCellSlab>& result_cell_slabs,
      std::vector<uint64_t>* offset_offsets_per_cs,
      std::vector<uint64_t>* var_offsets_per_cs,
      uint64_t* total_offset_size,
      uint64_t* total_var_size,
      uint64_t* total_validity_size);

  /**
   * Copies the cells for the input **var-sized** attribute/dimension and result
   * cell slabs into the corresponding result buffers for the
   * partition in `ctx_cache` at index `partition_idx`.
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
   */
  Status process_tiles(
      const std::unordered_map<std::string, ProcessTileFlags>& names,
      const std::vector<ResultTile*>& result_tiles,
      std::vector<ResultCellSlab>& result_cell_slabs,
      Subarray& subarray,
      uint64_t stride);

  /**
   * Builds and returns an association from each tile in `result_cell_slabs`
   * to the cell slabs it contains.
   */
  tdb_unique_ptr<ResultCellSlabsIndex> compute_rcs_index(
      const std::vector<ResultCellSlab>& result_cell_slabs,
      Subarray& subarray) const;

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
   * @return Status
   */
  Status apply_query_condition(
      std::vector<ResultCellSlab>* result_cell_slabs,
      const std::vector<ResultTile*>& result_tiles,
      Subarray& subarray,
      uint64_t stride = UINT64_MAX);
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_READER_BASE_H