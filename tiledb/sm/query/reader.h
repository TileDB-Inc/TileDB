/**
 * @file   reader.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB, Inc.
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
 * This file defines class Reader.
 */

#ifndef TILEDB_READER_H
#define TILEDB_READER_H

#include <future>
#include <list>
#include <map>
#include <memory>
#include <queue>
#include <vector>

#include "tiledb/common/status.h"
#include "tiledb/sm/array_schema/tile_domain.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/misc/uri.h"
#include "tiledb/sm/query/query_buffer.h"
#include "tiledb/sm/query/result_cell_slab.h"
#include "tiledb/sm/query/result_coords.h"
#include "tiledb/sm/query/result_space_tile.h"
#include "tiledb/sm/query/validity_vector.h"
#include "tiledb/sm/query/write_cell_slab_iter.h"
#include "tiledb/sm/subarray/subarray_partitioner.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class Array;
class ArraySchema;
class FragmentMetadata;
class StorageManager;
class Tile;

/** Processes read queries. */
class Reader {
 public:
  /* ********************************* */
  /*          TYPE DEFINITIONS         */
  /* ********************************* */

  /** The state for a read query. */
  struct ReadState {
    /**
     * True if the query led to a result that does not fit in
     * the user buffers.
     */
    bool overflowed_ = false;
    /** The subarray partitioner. */
    SubarrayPartitioner partitioner_;
    /**
     * ``true`` if the next partition cannot be retrieved from the
     * partitioner, because it reaches a partition that is unsplittable.
     */
    bool unsplittable_ = false;
    /** True if the reader has been initialized. */
    bool initialized_ = false;

    /** ``true`` if there are no more partitions. */
    bool done() const {
      return partitioner_.done();
    }

    /** Retrieves the next partition from the partitioner. */
    Status next() {
      return partitioner_.next(&unsplittable_);
    }

    /**
     * Splits the current partition and updates the state, retrieving
     * a new current partition. This function is typically called
     * by the reader when the current partition was estimated to fit
     * the results, but that was not eventually true.
     */
    Status split_current() {
      return partitioner_.split_current(&unsplittable_);
    }
  };

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor.
   *
   * @param array_schema The array schema.
   * @param fragment_metadata The fragment metadata.
   */
  Reader();

  /** Destructor. */
  ~Reader();

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /** Returns the array. */
  const Array* array() const;

  /** Adds a range to the subarray on the input dimension. */
  Status add_range(unsigned dim_idx, const Range& range);

  /** Retrieves the number of ranges of the subarray for the given dimension. */
  Status get_range_num(unsigned dim_idx, uint64_t* range_num) const;

  /**
   * Retrieves a range from a dimension in the form (start, end, stride).
   *
   * @param dim_idx The dimension to retrieve the range from.
   * @param range_idx The id of the range to retrieve.
   * @param start The range start to retrieve.
   * @param end The range end to retrieve.
   * @param stride The range stride to retrieve.
   * @return Status
   */
  Status get_range(
      unsigned dim_idx,
      uint64_t range_idx,
      const void** start,
      const void** end,
      const void** stride) const;

  /**
   * Retrieves a range's sizes for a variable-length dimension
   *
   * @param dim_idx The dimension to retrieve the range from.
   * @param range_idx The id of the range to retrieve.
   * @param start_size range start size in bytes
   * @param end_size range end size in bytes
   * @return Status
   */
  Status get_range_var_size(
      unsigned dim_idx,
      uint64_t range_idx,
      uint64_t* start_size,
      uint64_t* end_size) const;

  /**
   * Gets the estimated result size (in bytes) for the input fixed-sized
   * attribute/dimension.
   */
  Status get_est_result_size(const char* name, uint64_t* size);

  /**
   * Gets the estimated result size (in bytes) for the input var-sized
   * attribute/dimension.
   */
  Status get_est_result_size(
      const char* name, uint64_t* size_off, uint64_t* size_val);

  /**
   * Used by serialization to get the map of result sizes
   * @return
   */
  std::unordered_map<std::string, Subarray::ResultSize>
  get_est_result_size_map();

  /**
   * Used by serialization to get the map of max mem sizes
   * @return
   */
  std::unordered_map<std::string, Subarray::MemorySize> get_max_mem_size_map();

  /**
   * Used by serialization to set the estimated result size
   *
   * @param est_result_size map to set
   * @param max_mem_size map to set
   * @return Status
   */
  Status set_est_result_size(
      std::unordered_map<std::string, Subarray::ResultSize>& est_result_size,
      std::unordered_map<std::string, Subarray::MemorySize>& max_mem_size);

  /** returns whether the estimated result size has been computed or not */
  bool est_result_size_computed();

  /** Returns the array schema. */
  const ArraySchema* array_schema() const;

  /** Returns the names of the buffers set by the user for the read query. */
  std::vector<std::string> buffer_names() const;

  /** Fetch QueryBuffer for the input attribute/dimension. */
  QueryBuffer buffer(const std::string& name) const;

  /**
   * Returns `true` if the query was incomplete, i.e., if all subarray
   * partitions in the read state have not been processed or there
   * was some buffer overflow.
   */
  bool incomplete() const;

  /**
   * Retrieves the buffer of a fixed-sized attribute/dimension.
   *
   * @param name The attribute/dimension name.
   * @param buffer The buffer to be retrieved.
   * @param buffer_size A pointer to the buffer size to be retrieved.
   * @return Status
   */
  Status get_buffer(
      const std::string& name, void** buffer, uint64_t** buffer_size) const;

  /**
   * Retrieves the offsets and values buffers of a var-sized
   * attribute/dimension.
   *
   * @param name The attribute/dimension name.
   * @param buffer_off The offsets buffer to be retrieved.
   * @param buffer_off_size A pointer to the offsets buffer size to be
   *     retrieved.
   * @param buffer_val The values buffer to be retrieved.
   * @param buffer_val_size A pointer to the values buffer size to be retrieved.
   * @return Status
   */
  Status get_buffer(
      const std::string& name,
      uint64_t** buffer_off,
      uint64_t** buffer_off_size,
      void** buffer_val,
      uint64_t** buffer_val_size) const;

  /**
   * Retrieves the buffer of a fixed-sized, nullable attribute.
   *
   * @param name The attribute name.
   * @param buffer The buffer to be retrieved.
   * @param buffer_size A pointer to the buffer size to be retrieved.
   * @param ValidityVector The validity vector to be retrieved.
   * @return Status
   */
  Status get_buffer_nullable(
      const std::string& name,
      void** buffer,
      uint64_t** buffer_size,
      const ValidityVector** validity_vector) const;

  /**
   * Retrieves the offsets, values, and validity buffers of a var-sized,
   * nullable attribute.
   *
   * @param name The attribute/dimension name.
   * @param buffer_off The offsets buffer to be retrieved.
   * @param buffer_off_size A pointer to the offsets buffer size to be
   *     retrieved.
   * @param buffer_val The values buffer to be retrieved.
   * @param buffer_val_size A pointer to the values buffer size to be retrieved.
   * @param ValidityVector The validity vector to be retrieved.
   * @return Status
   */
  Status get_buffer_nullable(
      const std::string& name,
      uint64_t** buffer_off,
      uint64_t** buffer_off_size,
      void** buffer_val,
      uint64_t** buffer_val_size,
      const ValidityVector** validity_vector) const;

  /** Returns the first fragment uri. */
  URI first_fragment_uri() const;

  /** Returns the last fragment uri. */
  URI last_fragment_uri() const;

  /** Initializes the reader with the subarray layout. */
  Status init(const Layout& layout);

  /** Returns the cell layout. */
  Layout layout() const;

  /** Returns `true` if no results were retrieved after a query. */
  bool no_results() const;

  /** Returns the current read state. */
  const ReadState* read_state() const;

  /** Returns the current read state. */
  ReadState* read_state();

  /** Performs a read query using its set members. */
  Status read();

  /** Sets the array. */
  void set_array(const Array* array);

  /**
   * Sets the array schema. If the array is a kv store, then this
   * function also sets global order as the default layout.
   */
  void set_array_schema(const ArraySchema* array_schema);

  /**
   * Sets the buffer for a fixed-sized attribute/dimension.
   *
   * @param name The attribute/dimension to set the buffer for.
   * @param buffer The buffer that will hold the data to be read.
   * @param buffer_size This initially contains the allocated
   *     size of `buffer`, but after the termination of the function
   *     it will contain the size of the useful (read) data in `buffer`.
   * @param check_null_buffers If true (default), null buffers are not allowed.
   * @return Status
   */
  Status set_buffer(
      const std::string& name,
      void* buffer,
      uint64_t* buffer_size,
      bool check_null_buffers = true);

  /**
   * Sets the buffer for a var-sized attribute/dimension.
   *
   * @param name The name to set the buffer for.
   * @param buffer_off The buffer that will hold the data to be read.
   *     This buffer holds the starting offsets of each cell value in
   *     `buffer_val`.
   * @param buffer_off_size This initially contains
   *     the allocated size of `buffer_off`, but after the termination of the
   *     function it will contain the size of the useful (read) data in
   *     `buffer_off`.
   * @param buffer_val The buffer that will hold the data to be read.
   *     This buffer holds the actual var-sized cell values.
   * @param buffer_val_size This initially contains
   *     the allocated size of `buffer_val`, but after the termination of the
   *     function it will contain the size of the useful (read) data in
   *     `buffer_val`.
   * @param check_null_buffers If true (default), null buffers are not allowed.
   * @return Status
   */
  Status set_buffer(
      const std::string& name,
      uint64_t* buffer_off,
      uint64_t* buffer_off_size,
      void* buffer_val,
      uint64_t* buffer_val_size,
      bool check_null_buffers = true);

  /**
   * Sets the buffer for a fixed-sized, nullable attribute.
   *
   * @param name The attribute to set the buffer for.
   * @param buffer The buffer that will hold the data to be read.
   * @param buffer_size This initially contains the allocated
   *     size of `buffer`, but after the termination of the function
   *     it will contain the size of the useful (read) data in `buffer`.
   * @param check_null_buffers If true (default), null buffers are not allowed.
   * @param validity_vector The validity vector associated with values in
   * `buffer`.
   * @return Status
   */
  Status set_buffer(
      const std::string& name,
      void* buffer,
      uint64_t* buffer_size,
      ValidityVector&& validity_vector,
      bool check_null_buffers = true);

  /**
   * Sets the buffer for a var-sized, nullable attribute.
   *
   * @param name The name to set the buffer for.
   * @param buffer_off The buffer that will hold the data to be read.
   *     This buffer holds the starting offsets of each cell value in
   *     `buffer_val`.
   * @param buffer_off_size This initially contains
   *     the allocated size of `buffer_off`, but after the termination of the
   *     function it will contain the size of the useful (read) data in
   *     `buffer_off`.
   * @param buffer_val The buffer that will hold the data to be read.
   *     This buffer holds the actual var-sized cell values.
   * @param buffer_val_size This initially contains
   *     the allocated size of `buffer_val`, but after the termination of the
   *     function it will contain the size of the useful (read) data in
   *     `buffer_val`.
   * @param validity_vector The validity vector associated with values in
   * `buffer_val`.
   * @param check_null_buffers If true (default), null buffers are not allowed.
   * @return Status
   */
  Status set_buffer(
      const std::string& name,
      uint64_t* buffer_off,
      uint64_t* buffer_off_size,
      void* buffer_val,
      uint64_t* buffer_val_size,
      ValidityVector&& validity_vector,
      bool check_null_buffers = true);

  /** Sets the fragment metadata. */
  void set_fragment_metadata(
      const std::vector<FragmentMetadata*>& fragment_metadata);

  /**
   * Sets the cell layout of the query. The function will return an error
   * if the queried array is a key-value store (because it has its default
   * layout for both reads and writes.
   */
  Status set_layout(Layout layout);

  /**
   * This is applicable only to dense arrays (errors out for sparse arrays),
   * and only in the case where the array is opened in a way that all its
   * fragments are sparse. If the input is `true`, then the dense array
   * will be read in "sparse mode", i.e., the sparse read algorithm will
   * be executing, returning results only for the non-empty cells.
   * The sparse mode is useful particularly in consolidation, when the
   * algorithm determines that the subset of fragments to consolidate
   * in the next step are all sparse. In that case, the consolidator
   * needs to read only the non-empty cells of those fragments, which
   * can be achieved by opening the dense array in the sparse mode
   * (otherwise the dense array would return special values for all
   * empty cells).
   *
   * @param sparse_mode This sets the sparse mode.
   * @return Status
   */
  Status set_sparse_mode(bool sparse_mode);

  /** Sets the storage manager. */
  void set_storage_manager(StorageManager* storage_manager);

  /** Sets the query subarray. */
  Status set_subarray(const Subarray& subarray);

  /** Returns the query subarray. */
  const Subarray* subarray() const;

  /* ********************************* */
  /*          STATIC FUNCTIONS         */
  /* ********************************* */

  /**
   * Computes a mapping (tile coordinates) -> (result space tile).
   * The produced result space tiles will contain information only
   * about fragments that will contribute results. Specifically, if
   * a fragment is completely covered by a more recent fragment
   * in a particular space tile, then it will certainly not contribute
   * results and, thus, no information about that fragment is included
   * in the space tile.
   *
   * @tparam T The datatype of the tile domains.
   * @param domain The array domain
   * @param tile_coords The unique coordinates of the tiles that intersect
   *     a subarray.
   * @param array_tile_domain The array tile domain.
   * @param frag_tile_domains The tile domains of each fragment. These
   *     are assumed to be ordered from the most recent to the oldest
   *     fragment.
   * @param result_space_tiles The result space tiles to be produced
   *     by the function.
   */
  template <class T>
  static void compute_result_space_tiles(
      const Domain* domain,
      const std::vector<std::vector<uint8_t>>& tile_coords,
      const TileDomain<T>& array_tile_domain,
      const std::vector<TileDomain<T>>& frag_tile_domains,
      std::map<const T*, ResultSpaceTile<T>>* result_space_tiles);

  /**
   * Computes the result cell slabs for the input subarray, given the
   * input result coordinates (retrieved from the sparse fragments).
   * The function also computes and stores the results space tiles
   * in `result_space_tiles`. This needs to be preserved throughout
   * the cell copying operations, since this structure stores all
   * the relevant result tiles for the dense fragments.
   *
   * @tparam T The domain datatype.
   * @param subarray The input subarray.
   * @param result_space_tiles The result space tiles computed by the
   *     function, which store the result tiles from the dense fragments.
   * @param result_coords The result coordinates produced by the sparse
   *     fragments.
   * @param result_tiles This will store pointers to the result tiles
   *     of both dense and sparse fragments.
   * @param result_cell_slabs The returned result cell slabs.
   * @return Status
   */
  template <class T>
  Status compute_result_cell_slabs(
      const Subarray& subarray,
      std::map<const T*, ResultSpaceTile<T>>* result_space_tiles,
      std::vector<ResultCoords>* result_coords,
      std::vector<ResultTile*>* result_tiles,
      std::vector<ResultCellSlab>* result_cell_slabs) const;

  /**
   * Computes the result cell slabs for the input subarray, given the
   * input result coordinates (retrieved from the sparse fragments).
   * The function also computes and stores the results space tiles
   * in `result_space_tiles`. This needs to be preserved throughout
   * the cell copying operations, since this structure stores all
   * the relevant result tiles for the dense fragments. Applicable
   * only to row-/col-major subarray layouts.
   *
   * @tparam T The domain datatype.
   * @param subarray The input subarray.
   * @param result_coords The result coordinates produced by the sparse
   *     fragments.
   * @param result_coords_pos The position in `result_coords` to be
   *     passed to the result cell slab iterator in the function.
   *     This practically keeps track of the sparse coordinate results
   *     already processed in successive calls of this function.
   *     The function updates this value with the current position
   *     returned by the iterator at the end of its process.
   * @param result_space_tiles The result space tiles computed by the
   *     function, which store the result tiles from the dense fragments.
   * @param result_tiles This will store pointers to the result tiles
   *     of both dense and sparse fragments.
   * @param frag_tile_set Stores the unique pairs (frag_idx, tile_idx)
   *     for all result tiles.
   * @param result_cell_slabs The returned result cell slabs.
   * @return Status
   */
  template <class T>
  Status compute_result_cell_slabs_row_col(
      const Subarray& subarray,
      std::map<const T*, ResultSpaceTile<T>>* result_space_tiles,
      std::vector<ResultCoords>* result_coords,
      uint64_t* result_coords_pos,
      std::vector<ResultTile*>* result_tiles,
      std::set<std::pair<unsigned, uint64_t>>* frag_tile_set,
      std::vector<ResultCellSlab>* result_cell_slabs) const;

  /**
   * Computes the result cell slabs for the input subarray, given the
   * input result coordinates (retrieved from the sparse fragments).
   * The function also computes and stores the results space tiles
   * in `result_space_tiles`. This needs to be preserved throughout
   * the cell copying operations, since this structure stores all
   * the relevant result tiles for the dense fragments. Applicable
   * only to global order subarray layouts.
   *
   * @tparam T The domain datatype.
   * @param subarray The input subarray.
   * @param result_coords The result coordinates produced by the sparse
   *     fragments.
   * @param result_space_tiles The result space tiles computed by the
   *     function, which store the result tiles from the dense fragments.
   * @param result_tiles This will store pointers to the result tiles
   *     of both dense and sparse fragments.
   * @param result_cell_slabs The returned result cell slabs.
   * @return Status
   */
  template <class T>
  Status compute_result_cell_slabs_global(
      const Subarray& subarray,
      std::map<const T*, ResultSpaceTile<T>>* result_space_tiles,
      std::vector<ResultCoords>* result_coords,
      std::vector<ResultTile*>* result_tiles,
      std::vector<ResultCellSlab>* result_cell_slabs) const;

 private:
  /* ********************************* */
  /*         PRIVATE DATATYPES         */
  /* ********************************* */

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
    std::unique_ptr<std::vector<uint64_t>> get_cs_offsets() {
      std::lock_guard<std::mutex> lg(mutex_);

      // Re-use a vector in the `cs_offsets_cache_` if possible,
      // otherwise create a new vector of size `num_cs_`.
      std::unique_ptr<std::vector<uint64_t>> cs_offsets;
      if (!cs_offsets_cache_.empty()) {
        cs_offsets = std::move(cs_offsets_cache_.front());
        assert(cs_offsets->size() == num_cs_);
        cs_offsets_cache_.pop();
      } else {
        cs_offsets =
            std::unique_ptr<std::vector<uint64_t>>(new std::vector<uint64_t>());
        cs_offsets->resize(num_cs_);
      }

      return cs_offsets;
    }

    /** Returns a vector fetched from `get_cs_offsets`. */
    void cache_cs_offsets(std::unique_ptr<std::vector<uint64_t>>&& cs_offsets) {
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
    std::queue<std::unique_ptr<std::vector<uint64_t>>> cs_offsets_cache_;
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
    std::unique_ptr<std::vector<uint64_t>> get_offset_offsets_per_cs() {
      std::lock_guard<std::mutex> lg(mutex_);

      // Re-use a vector in the `offset_offsets_per_cs_cache_` if possible,
      // otherwise create a new vector of size `total_cs_length_`.
      std::unique_ptr<std::vector<uint64_t>> offset_offsets_per_cs;
      if (!offset_offsets_per_cs_cache_.empty()) {
        offset_offsets_per_cs = std::move(offset_offsets_per_cs_cache_.front());
        assert(offset_offsets_per_cs->size() == total_cs_length_);
        offset_offsets_per_cs_cache_.pop();
      } else {
        offset_offsets_per_cs =
            std::unique_ptr<std::vector<uint64_t>>(new std::vector<uint64_t>());
        offset_offsets_per_cs->resize(total_cs_length_);
      }

      return offset_offsets_per_cs;
    }

    /** Returns a vector fetched from `get_offset_offsets_per_cs`. */
    void cache_offset_offsets_per_cs(
        std::unique_ptr<std::vector<uint64_t>>&& offset_offsets_per_cs) {
      assert(offset_offsets_per_cs->size() == total_cs_length_);
      std::lock_guard<std::mutex> lg(mutex_);
      offset_offsets_per_cs_cache_.push(std::move(offset_offsets_per_cs));
    }

    /** Returns a pre-sized vector to store var-offsets per cell slab. */
    std::unique_ptr<std::vector<uint64_t>> get_var_offsets_per_cs() {
      std::lock_guard<std::mutex> lg(mutex_);

      // Re-use a vector in the `var_offsets_per_cs_cache_` if possible,
      // otherwise create a new vector of size `total_cs_length_`.
      std::unique_ptr<std::vector<uint64_t>> var_offsets_per_cs;
      if (!var_offsets_per_cs_cache_.empty()) {
        var_offsets_per_cs = std::move(var_offsets_per_cs_cache_.front());
        assert(var_offsets_per_cs->size() == total_cs_length_);
        var_offsets_per_cs_cache_.pop();
      } else {
        var_offsets_per_cs =
            std::unique_ptr<std::vector<uint64_t>>(new std::vector<uint64_t>());
        var_offsets_per_cs->resize(total_cs_length_);
      }

      return var_offsets_per_cs;
    }

    /** Returns a vector fetched from `get_var_offsets_per_cs`. */
    void cache_var_offsets_per_cs(
        std::unique_ptr<std::vector<uint64_t>>&& var_offsets_per_cs) {
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
    std::queue<std::unique_ptr<std::vector<uint64_t>>>
        offset_offsets_per_cs_cache_;

    /**
     * A pool of vectors that maps each cell slab to its offset
     * for its variable-length data.
     */
    std::queue<std::unique_ptr<std::vector<uint64_t>>>
        var_offsets_per_cs_cache_;
  };

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The array. */
  const Array* array_;

  /** The array schema. */
  const ArraySchema* array_schema_;

  /**
   * Maps attribute/dimension names to their buffers.
   * `TILEDB_COORDS` may be used for the special zipped coordinates
   * buffer.
   * */
  std::unordered_map<std::string, QueryBuffer> buffers_;

  /** The fragment metadata. */
  std::vector<FragmentMetadata*> fragment_metadata_;

  /** The layout of the cells in the result of the subarray. */
  Layout layout_;

  /** Read state. */
  ReadState read_state_;

  /**
   * If `true`, then the dense array will be read in "sparse mode", i.e.,
   * the sparse read algorithm will be executing, returning results only
   * for the non-empty cells.
   */
  bool sparse_mode_;

  /** The storage manager. */
  StorageManager* storage_manager_;

  /** The query subarray (initially the whole domain by default). */
  Subarray subarray_;

  /** The offset format used for variable-sized attributes. */
  std::string offsets_format_mode_;

  /**
   * If `true`, an extra element that points to the end of the values buffer
   * will be added in the end of the offsets buffer of var-sized attributes
   */
  bool offsets_extra_element_;

  /** Protects result tiles. */
  mutable std::mutex result_tiles_mutex_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /** Correctness checks for `subarray_`. */
  Status check_subarray() const;

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
   * Compute the maximal cell slabs of contiguous sparse coordinates.
   *
   * @param coords The coordinates to compute the slabs from.
   * @param result_cell_slabs The result cell slabs to compute.
   * @return Status
   */
  Status compute_result_cell_slabs(
      const std::vector<ResultCoords>& result_coords,
      std::vector<ResultCellSlab>* result_cell_slabs) const;

  /**
   * Retrieves the coordinates that overlap the input N-dimensional range
   * from the input result tile.
   *
   * @param subarray The subarray to operate on.
   * @param frag_idx The id of the fragment that the result tile belongs to.
   * @param tile The result tile.
   * @param range_idx The range id.
   * @param result_coords The overlapping coordinates to retrieve.
   * @return Status
   */
  Status compute_range_result_coords(
      Subarray* subarray,
      unsigned frag_idx,
      ResultTile* tile,
      uint64_t range_idx,
      std::vector<ResultCoords>* result_coords);

  /**
   * Computes the result coordinates for each range of the query
   * subarray.
   *
   * @param subarray The subarray to operate on.
   * @param single_fragment For each range, it indicates whether all
   *     result coordinates come from a single fragment.
   * @param result_tile_map This is an auxialiary map that helps finding the
   *     result tiles of each range.
   * @param result_tiles The result tiles to read the coordinates from.
   * @param range_result_coords The result coordinates to be retrieved.
   *     It contains a vector for each range of the subarray.
   * @return Status
   */
  Status compute_range_result_coords(
      Subarray* subarray,
      const std::vector<bool>& single_fragment,
      const std::map<std::pair<unsigned, uint64_t>, size_t>& result_tile_map,
      std::vector<ResultTile>* result_tiles,
      std::vector<std::vector<ResultCoords>>* range_result_coords);

  /**
   * Computes the result coordinates of a given range of the query
   * subarray.
   *
   * @param subarray The subarray to operate on.
   * @param range_idx The range to focus on.
   * @param result_tile_map This is an auxialiary map that helps finding the
   *     result_tiles overlapping with each range.
   * @param result_tiles The result tiles to read the coordinates from.
   * @param range_result_coords The result coordinates to be retrieved.
   *     It contains a vector for each range of the subarray.
   * @return Status
   */
  Status compute_range_result_coords(
      Subarray* subarray,
      uint64_t range_idx,
      const std::map<std::pair<unsigned, uint64_t>, size_t>& result_tile_map,
      std::vector<ResultTile>* result_tiles,
      std::vector<ResultCoords>* range_result_coords);

  /**
   * Computes the result coordinates of a given range of the query
   * subarray.
   *
   * @param subarray The subarray to operate on.
   * @param range_idx The range to focus on.
   * @param fragment_idx The fragment to focus on.
   * @param result_tile_map This is an auxialiary map that helps finding the
   *     result_tiles overlapping with each range.
   * @param result_tiles The result tiles to read the coordinates from.
   * @param range_result_coords The result coordinates to be retrieved.
   *     It contains a vector for each range of the subarray.
   * @return Status
   */
  Status compute_range_result_coords(
      Subarray* subarray,
      uint64_t range_idx,
      uint32_t fragment_idx,
      const std::map<std::pair<unsigned, uint64_t>, size_t>& result_tile_map,
      std::vector<ResultTile>* result_tiles,
      std::vector<ResultCoords>* range_result_coords);

  /**
   * Computes the final subarray result coordinates, which will be
   * deduplicated and sorted on the specified subarray layout.
   *
   * @param range_result_coords The result coordinates for each subarray range.
   * @param result_coords The final (subarray) result coordinates to be
   *     retrieved.
   * @return Status
   *
   * @note the function will try to gradually clean up ``range_result_coords``
   *     as it is done processing its elements to quickly reclaim memory.
   */
  Status compute_subarray_coords(
      std::vector<std::vector<ResultCoords>>* range_result_coords,
      std::vector<ResultCoords>* result_coords);

  /**
   * Computes info about the sparse result tiles, such as which fragment they
   * belong to, the tile index and the type of overlap. The tile vector
   * contains unique info about the tiles. The function also computes
   * a map from fragment index and tile id to a result tile to keep
   * track of the unique result  tile info for subarray ranges that overlap
   * with common tiles.
   *
   * @param result_tiles The result tiles to be computed.
   * @param result_tile_map The result tile map to be computed.
   * @param single_fragment Each element corresponds to a range of the
   *     subarray and is set to ``true`` if all the overlapping
   *     tiles come from a single fragment for that range.
   * @return Status
   */
  Status compute_sparse_result_tiles(
      std::vector<ResultTile>* result_tiles,
      std::map<std::pair<unsigned, uint64_t>, size_t>* result_tile_map,
      std::vector<bool>* single_fragment);

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
      const std::vector<ResultCellSlab>& result_cell_slabs,
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
   * @return Status
   */
  Status compute_var_cell_destinations(
      const std::string& name,
      uint64_t stride,
      const std::vector<ResultCellSlab>& result_cell_slabs,
      std::vector<uint64_t>* offset_offsets_per_cs,
      std::vector<uint64_t>* var_offsets_per_cs,
      uint64_t* total_offset_size,
      uint64_t* total_var_size,
      uint64_t* total_validity_size) const;

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
   * Computes the result space tiles based on the input subarray.
   *
   * @tparam T The domain datatype.
   * @param subarray The input subarray.
   * @param result_space_tiles The result space tiles to be computed.
   */
  template <class T>
  void compute_result_space_tiles(
      const Subarray& subarray,
      std::map<const T*, ResultSpaceTile<T>>* result_space_tiles) const;

  /**
   * Computes the result coordinates from the sparse fragments.
   *
   * @param result_tiles This will store the unique result tiles.
   * @param result_coords This will store the result coordinates.
   */
  Status compute_result_coords(
      std::vector<ResultTile>* result_tiles,
      std::vector<ResultCoords>* result_coords);

  /**
   * Deduplicates the input result coordinates, breaking ties giving preference
   * to the largest fragment index (i.e., it prefers more recent fragments).
   *
   * @param result_coords The result coordinates to dedup.
   * @return Status
   */
  Status dedup_result_coords(std::vector<ResultCoords>* result_coords) const;

  /** Performs a read on a dense array. */
  Status dense_read();

  /**
   * Performs a read on a dense array.
   *
   * @tparam The domain type.
   * @return Status
   */
  template <class T>
  Status dense_read();

  /**
   * Fills the coordinate buffer with coordinates. Applicable only to dense
   * arrays when the user explicitly requests the coordinates to be
   * materialized.
   *
   * @tparam T The domain type.
   * @param subarray The input subarray.
   * @return Status
   */
  template <class T>
  Status fill_dense_coords(const Subarray& subarray);

  /**
   * Fills the coordinate buffers with coordinates. Applicable only to dense
   * arrays when the user explicitly requests the coordinates to be
   * materialized. Also applicable only to global order.
   *
   * @tparam T The domain type.
   * @param subarray The input subarray.
   * @param dim_idx The dimension indices of the corresponding `buffers`.
   *     For the special zipped coordinates, `dim_idx`, `buffers` and `offsets`
   *     contain a single element and `dim_idx` contains `dim_num` as
   *     the dimension index.
   * @param buffers The buffers to copy from. It could be the special
   *     zipped coordinates or separate coordinate buffers.
   * @param offsets The offsets that will be used eventually to update
   *     the buffer sizes, determining the useful results written in
   *     the buffers.
   * @return Status
   */
  template <class T>
  Status fill_dense_coords_global(
      const Subarray& subarray,
      const std::vector<unsigned>& dim_idx,
      const std::vector<QueryBuffer*>& buffers,
      std::vector<uint64_t>* offsets);

  /**
   * Fills the coordinate buffers with coordinates. Applicable only to dense
   * arrays when the user explicitly requests the coordinates to be
   * materialized. Also applicable only to row-/col-major order.
   *
   * @tparam T The domain type.
   * @param subarray The input subarray.
   * @param dim_idx The dimension indices of the corresponding `buffers`.
   *     For the special zipped coordinates, `dim_idx`, `buffers` and `offsets`
   *     contain a single element and `dim_idx` contains `dim_num` as
   *     the dimension index.
   * @param buffers The buffers to copy from. It could be the special
   *     zipped coordinates or separate coordinate buffers.
   * @param offsets The offsets that will be used eventually to update
   *     the buffer sizes, determining the useful results written in
   *     the buffers.
   * @return Status
   */
  template <class T>
  Status fill_dense_coords_row_col(
      const Subarray& subarray,
      const std::vector<unsigned>& dim_idx,
      const std::vector<QueryBuffer*>& buffers,
      std::vector<uint64_t>* offsets);

  /**
   * Fills coordinates in the input buffers for a particular cell slab,
   * following a row-major layout. For instance, if the starting coordinate are
   * [3, 1] and the number of coords to be written is 3, this function will
   * write to the input buffer (starting at the input offset) coordinates
   * [3, 1], [3, 2], and [3, 3].
   *
   * @tparam T The domain type.
   * @param start The starting coordinates in the slab.
   * @param num The number of coords to be written.
   * @param dim_idx The dimension indices of the corresponding `buffers`.
   *     For the special zipped coordinates, `dim_idx`, `buffers` and `offsets`
   *     contain a single element and `dim_idx` contains `dim_num` as
   *     the dimension index.
   * @param buffers The buffers to copy from. It could be the special
   *     zipped coordinates or separate coordinate buffers.
   * @param offsets The offsets that will be used eventually to update
   *     the buffer sizes, determining the useful results written in
   *     the buffers.
   */
  template <class T>
  void fill_dense_coords_row_slab(
      const T* start,
      uint64_t num,
      const std::vector<unsigned>& dim_idx,
      const std::vector<QueryBuffer*>& buffers,
      std::vector<uint64_t>* offsets) const;

  /**
   * Fills coordinates in the input buffers for a particular cell slab,
   * following a col-major layout. For instance, if the starting coordinate are
   * [3, 1] and the number of coords to be written is 3, this function will
   * write to the input buffer (starting at the input offset) coordinates
   * [4, 1], [5, 1], and [6, 1].
   *
   * @tparam T The domain type.
   * @param start The starting coordinates in the slab.
   * @param num The number of coords to be written.
   * @param dim_idx The dimension indices of the corresponding `buffers`.
   *     For the special zipped coordinates, `dim_idx`, `buffers` and `offsets`
   *     contain a single element and `dim_idx` contains `dim_num` as
   *     the dimension index.
   * @param buffers The buffers to copy from. It could be the special
   *     zipped coordinates or separate coordinate buffers.
   * @param offsets The offsets that will be used eventually to update
   *     the buffer sizes, determining the useful results written in
   *     the buffers.
   */
  template <class T>
  void fill_dense_coords_col_slab(
      const T* start,
      uint64_t num,
      const std::vector<unsigned>& dim_idx,
      const std::vector<QueryBuffer*>& buffers,
      std::vector<uint64_t>* offsets) const;

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
      const std::unordered_map<
          ResultTile*,
          std::vector<std::pair<uint64_t, uint64_t>>>* const cs_ranges =
          nullptr) const;

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
   * Gets all the result coordinates of the input tile into `result_coords`.
   *
   * @param result_tile The result tile to read the coordinates from.
   * @param result_coords The result coordinates to copy into.
   * @return Status
   */
  Status get_all_result_coords(
      ResultTile* tile, std::vector<ResultCoords>* result_coords) const;

  /** Returns `true` if the coordinates are included in the attributes. */
  bool has_coords() const;

  /**
   * Returns `true` if a coordinate buffer for a separate dimension
   * has been set.
   */
  bool has_separate_coords() const;

  /** Initializes the read state. */
  Status init_read_state();

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
   * Loads tile offsets for each attribute/dimension name into
   * their associated element in `fragment_metadata_`.
   *
   * @param names The attribute/dimension names.
   * @return Status
   */
  Status load_tile_offsets(const std::vector<std::string>& names);

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
   * Resets the buffer sizes to the original buffer sizes. This is because
   * the read query may alter the buffer sizes to reflect the size of
   * the useful data (results) written in the buffers.
   */
  void reset_buffer_sizes();

  /**
   * Sorts the input result coordinates according to the subarray layout.
   *
   * @param iter_begin The start position of the coordinates to sort.
   * @param iter_end The end position of the coordinates to sort.
   * @param coords_num The number of coordinates to be sorted.
   * @param layout The layout to sort into.
   * @return Status
   */
  Status sort_result_coords(
      std::vector<ResultCoords>::iterator iter_begin,
      std::vector<ResultCoords>::iterator iter_end,
      size_t coords_num,
      Layout layout) const;

  /** Performs a read on a sparse array. */
  Status sparse_read();

  /**
   * Copies the result coordinates to the user buffers.
   * It also appropriately cleans up the used result tiles.
   */
  Status copy_coordinates(
      const std::vector<ResultTile*>& result_tiles,
      const std::vector<ResultCellSlab>& result_cell_slabs);

  // TODO: add docstring
  Status add_extra_offset();

  /**
   * Copies the result attribute values to the user buffers.
   * It also appropriately cleans up the used result tiles.
   */
  Status copy_attribute_values(
      uint64_t stride,
      const std::vector<ResultTile*>& result_tiles,
      const std::vector<ResultCellSlab>& result_cell_slabs);

  /** Zeroes out the user buffer sizes, indicating an empty result. */
  void zero_out_buffer_sizes();

  /**
   * Returns true if the input tile's MBR of the input fragment is fully
   * covered by the non-empty domain of a more recent fragment.
   */
  bool sparse_tile_overwritten(unsigned frag_idx, uint64_t tile_idx) const;

  /**
   * Erases the coordinate tiles (zipped or separate) from the input result
   * tiles.
   */
  void erase_coord_tiles(std::vector<ResultTile>* result_tiles) const;

  /**
   * Gets statistics about the number of attributes and dimensions in
   * the read query.
   */
  void get_dim_attr_stats() const;

  /** Gets statistics about the result cells. */
  void get_result_cell_stats(
      const std::vector<ResultCellSlab>& result_cell_slabs) const;

  /** Gets statistics about the result tiles. */
  void get_result_tile_stats(
      const std::vector<ResultTile*>& result_tiles) const;

  /**
   * Calculates the hilbert values of the result coordinates between
   * `iter_begin` and `iter_begin + hilbert_values.size()`.
   * The hilbert values are stored
   * in `hilbert_values`, where the first pair value is the hilbert value
   * and the second is the position of the result coords after the
   * input iterator.
   */
  Status calculate_hilbert_values(
      std::vector<ResultCoords>::iterator iter_begin,
      std::vector<std::pair<uint64_t, uint64_t>>* hilbert_values) const;

  /**
   * It reorganizes the result coords given the iterator offsets in
   * `hilbert_values` (second values in the pair). This essentially
   * sorts the result coordinates starting at `iter_begin` based
   * on the already sorted hilbert values.
   *
   * The algorithm is in-place, operates with O(1) memory and
   * in O(coords_num) time, but modifies the offsets/positions in
   * `hilbert_values`.
   */
  Status reorganize_result_coords(
      std::vector<ResultCoords>::iterator iter_begin,
      std::vector<std::pair<uint64_t, uint64_t>>* hilbert_values) const;

  /**
   * Returns true if the result coordinates between the two iterators
   * belong to the same fragment.
   */
  bool belong_to_single_fragment(
      std::vector<ResultCoords>::iterator iter_begin,
      std::vector<ResultCoords>::iterator iter_end) const;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_READER_H
