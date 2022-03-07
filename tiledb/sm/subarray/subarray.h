/**
 * @file   subarray.h
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
 * This file defines class Subarray.
 */

#ifndef TILEDB_SUBARRAY_H
#define TILEDB_SUBARRAY_H

#include <atomic>

#include "tiledb/common/common.h"
#include "tiledb/common/logger_public.h"
#include "tiledb/common/thread_pool.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/config/config.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/misc/tile_overlap.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/stats/stats.h"
#include "tiledb/sm/subarray/range_subset.h"
#include "tiledb/sm/subarray/subarray_tile_overlap.h"

#include <cmath>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <vector>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class Array;
class ArraySchema;
class EncryptionKey;
class FragmentMetadata;
class StorageManager;

enum class Layout : uint8_t;
enum class QueryType : uint8_t;

/**
 * A Subarray object is associated with an array, and
 * is oriented by a set of 1D ranges per dimension over the array
 * domain. The ranges may overlap. The subarray essentially represents
 * a new logical domain constructed by choosing cell slabs from
 * the array domain. This is applicable to both dense and sparse
 * array domains.
 *
 * **Example:**
 *
 * Consider the following array with domain [1,3] x [1,3]:
 *
 * 1 2 3
 * 4 5 6
 * 7 8 9
 *
 * A subarray will be defined as an ordered list of 1D ranges per dimension,
 * e.g., for
 *
 * rows dim: ([2,3], [1,1])
 * cols dim: ([1,2])
 *
 * we get a 3x2 array:
 *
 * 4 5
 * 7 8
 * 1 2
 *
 * The 1D ranges may overlap. For instance:
 *
 * rows dim: ([2,3], [1,1])
 * cols dim: ([1,2], [2,3])
 *
 * produces:
 *
 * 4 5 5 6
 * 7 8 8 9
 * 1 2 2 3
 *
 * @note The subarray will certainly have the same type and number of
 *     dimensions as the array domain it is constructed from, but it may
 *     have a different shape.
 *
 * @note If no 1D ranges are set for some dimension, then the subarray
 *     by default will cover the entire domain of that dimension.
 */
class Subarray {
 public:
  /* ********************************* */
  /*         PUBLIC DATA TYPES         */
  /* ********************************* */

  /**
   * Result size (in bytes) for an attribute/dimension used for
   * partitioning.
   */
  struct ResultSize {
    /** Size for fixed-sized attributes/dimensions or offsets of var-sized
     * attributes/dimensions.
     */
    double size_fixed_;

    /** Size of values for var-sized attributes/dimensions. */
    double size_var_;

    /** Size of validity values for nullable attributes. */
    double size_validity_;
  };

  /**
   * Maximum memory size (in bytes) for an attribute/dimension used for
   * partitioning.
   */
  struct MemorySize {
    /**
     * Maximum size of overlapping tiles fetched into memory for
     * fixed-sized attributes/dimensions or offsets of var-sized
     * attributes/dimensions.
     */
    uint64_t size_fixed_;

    /**
     * Maximum size of overlapping tiles fetched into memory for
     * var-sized attributes/dimensions.
     */
    uint64_t size_var_;

    /**
     * Maximum size of overlapping validity tiles fetched into memory for
     * nullable attributes.
     */
    uint64_t size_validity_;
  };

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  Subarray();

  /**
   * Constructor.
   *
   * @param array The array the subarray is associated with.
   * @param parent_stats The parent stats to inherit from.
   * @param logger The parent logger to clone and use for logging
   * @param coalesce_ranges When enabled, ranges will attempt to coalesce
   *     with existing ranges as they are added.
   */
  Subarray(
      const Array* array,
      stats::Stats* parent_stats,
      tdb_shared_ptr<Logger> logger,
      bool coalesce_ranges = true,
      StorageManager* storage_manager = nullptr);

  /**
   * Constructor.
   *
   * @param array The array the subarray is associated with.
   * @param layout The layout of the values of the subarray (of the results
   *     if the subarray is used for reads, or of the values provided
   *     by the user for writes).
   * @param parent_stats The parent stats to inherit from.
   * @param logger The parent logger to clone and use for logging
   * @param coalesce_ranges When enabled, ranges will attempt to coalesce
   *     with existing ranges as they are added.
   */
  Subarray(
      const Array* array,
      Layout layout,
      stats::Stats* parent_stats,
      tdb_shared_ptr<Logger> logger,
      bool coalesce_ranges = true,
      StorageManager* storage_manager = nullptr);

  /**
   * Copy constructor. This performs a deep copy (including memcpy of
   * underlying buffers).
   */
  Subarray(const Subarray& subarray);

  /** Move constructor. */
  Subarray(Subarray&& subarray) noexcept;

  /** Destructor. */
  ~Subarray() = default;

  /**
   * Copy-assign operator. This performs a deep copy (including memcpy
   * of underlying buffers).
   */
  Subarray& operator=(const Subarray& subarray);

  /** Move-assign operator. */
  Subarray& operator=(Subarray&& subarray) noexcept;

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /** Sets config for query-level parameters only. */
  Status set_config(const Config& config);

  /**
   * Get the config of the writer
   * @return Config
   */
  const Config* config() const;

  /** equivalent for older Query::set_subarray(const void *subarray); */
  Status set_subarray(const void* subarray);

  /** Adds a range along the dimension with the given index. */
  Status add_range(
      uint32_t dim_idx, Range&& range, const bool read_range_oob_error = true);

  /**
   * Adds a range to the subarray on the input dimension by index,
   * in the form of (start, end, stride).
   * The range components must be of the same type as the domain type of the
   * underlying array.
   */
  Status add_range(
      unsigned dim_idx, const void* start, const void* end, const void* stride);

  /**
   * @brief Set point ranges from an array
   *
   * @param dim_idx Dimension index
   * @param start Pointer to start of the array
   * @param count Number of elements to add
   * @return Status
   */
  Status add_point_ranges(unsigned dim_idx, const void* start, uint64_t count);

  /**
   * Adds a variable-sized range to the (read/write) query on the input
   * dimension by index, in the form of (start, end).
   */
  Status add_range_var(
      unsigned dim_idx,
      const void* start,
      uint64_t start_size,
      const void* end,
      uint64_t end_size);

  /**
   * Adds a range along the dimension with the given index, without
   * performing any error checks.
   */
  Status add_range_unsafe(uint32_t dim_idx, const Range& range);

  /**
   * Adds a range to the (read/write) query on the input dimension by name,
   * in the form of (start, end, stride).
   * The range components must be of the same type as the domain type of the
   * underlying array.
   */
  Status add_range_by_name(
      const std::string& dim_name,
      const void* start,
      const void* end,
      const void* stride);

  /**
   * Adds a variable-sized range to the (read/write) query on the input
   * dimension by name, in the form of (start, end).
   */
  Status add_range_var_by_name(
      const std::string& dim_name,
      const void* start,
      uint64_t start_size,
      const void* end,
      uint64_t end_size);

  /**
   * Retrieves the number of ranges of the subarray for the given dimension
   * name.
   */
  Status get_range_num_from_name(
      const std::string& dim_name, uint64_t* range_num) const;

  /**
   * Retrieves a range from a dimension name in the form (start, end, stride).
   *
   * @param dim_name The dimension to retrieve the range from.
   * @param range_idx The id of the range to retrieve.
   * @param start The range start to retrieve.
   * @param end The range end to retrieve.
   * @param stride The range stride to retrieve.
   * @return Status
   */
  Status get_range_from_name(
      const std::string& dim_name,
      uint64_t range_idx,
      const void** start,
      const void** end,
      const void** stride) const;

  /**
   * Retrieves a range's sizes for a variable-length dimension name
   *
   * @param dim_name The dimension name to retrieve the range from.
   * @param range_idx The id of the range to retrieve.
   * @param start_size range start size in bytes
   * @param end_size range end size in bytes
   * @return Status
   */
  Status get_range_var_size_from_name(
      const std::string& dim_name,
      uint64_t range_idx,
      uint64_t* start_size,
      uint64_t* end_size) const;

  /**
   * Retrieves a range from a variable-length dimension name in the form (start,
   * end).
   *
   * @param dim_name The dimension name to retrieve the range from.
   * @param range_idx The id of the range to retrieve.
   * @param start The range start to retrieve.
   * @param end The range end to retrieve.
   * @return Status
   */
  Status get_range_var_from_name(
      const std::string& dim_name,
      uint64_t range_idx,
      void* start,
      void* end) const;

  /** Returns the array the subarray is associated with. */
  const Array* array() const;

  /** Returns the number of cells in the subarray. */
  uint64_t cell_num() const;

  /** Returns the number of cells in the input ND range. */
  uint64_t cell_num(uint64_t range_idx) const;

  /** Returns the number of cells in the input ND range. */
  uint64_t cell_num(const std::vector<uint64_t>& range_coords) const;

  /** Clears the contents of the subarray. */
  void clear();

  /** Clears the contents of the tile overlap. */
  void clear_tile_overlap();

  /**
   * Returns the size of the tile overlap data.
   *
   * @return Size of the tile overlap data.
   */
  uint64_t tile_overlap_byte_size() const;

  /**
   * Returns true if the subarray is unary and it coincides with
   * tile boundaries.
   */
  bool coincides_with_tiles() const;

  /**
   * Computes the range offsets which are important for getting
   * an ND range index from a flat serialized index.
   */
  void compute_range_offsets();

  /**
   * Precomputes the tile overlap with all subarray ranges for
   * all fragments. The state is cached internally and accessible
   * through the `get_subarray_tile_overlap` API.
   *
   * This routine may not compute tile overlap for the entire range.
   * This only guarantees that:
   *   1. Tile overlap will be computed for at least one range.
   *   2. Tile overlap is computed starting from `start_range_idx`.
   *
   * The caller is responsible for checking the range indexes that
   * were computed through the `get_subarray_tile_overlap` API.
   *
   * @param start_range_idx The start range index.
   * @param end_range_idx The target end range index.
   * @param config The config object.
   * @param compute_tp The compute thread pool.
   * @param override_memory_constraint When true, this forces the
   *    routine to compute tile overlap for all ranges.
   */
  Status precompute_tile_overlap(
      uint64_t start_range_idx,
      uint64_t end_range_idx,
      const Config* config,
      ThreadPool* compute_tp,
      bool override_memory_constraint = false);

  /**
   * Precomputes the tile overlap with all subarray ranges for all fragments.
   *
   * @param compute_tp The compute thread pool.
   * @param frag_tile_idx The current tile index, per fragment.
   * @param result_tile_ranges The resulting tile ranges.
   */
  Status precompute_all_ranges_tile_overlap(
      ThreadPool* const compute_tp,
      std::vector<std::pair<uint64_t, uint64_t>>& frag_tile_idx,
      std::vector<std::vector<std::pair<uint64_t, uint64_t>>>*
          result_tile_ranges);

  /**
   * Computes the estimated result size (calibrated using the maximum size)
   * for a vector of given attributes/dimensions and range id, for all
   * fragments. The function focuses only on fragments relevant to the
   * subarray query.
   *
   * @param encryption_key The encryption key of the array.
   * @param array_schema The array schema.
   * @param all_dims_same_type Whether or not all dimensions have the
   *     same type.
   * @param all_dims_fixed Whether or not all dimensions are fixed-sized.
   * @param fragment_meta The fragment metadata of the array.
   * @param names The name vector of the attributes/dimensions to focus on.
   * @param var_sizes A vector indicating which attribute/dimension is
   *     var-sized.
   * @param nullable A vector indicating which attribute is nullable.
   * @param range_idx The id of the subarray range to focus on.
   * @param range_coords The coordinates of the subarray range to focus on.
   * @param result_sizes The result sizes to be retrieved for all given names.
   * @param frag_tiles The set of unique (fragment id, tile id) pairs across
   *   all ranges, which is update by this function in a thread-safe manner.
   * @return Status
   */
  Status compute_relevant_fragment_est_result_sizes(
      const ArraySchema& array_schema,
      bool all_dims_same_type,
      bool all_dims_fixed,
      const std::vector<tdb_shared_ptr<FragmentMetadata>>& fragment_meta,
      const std::vector<std::string>& name,
      const std::vector<bool>& var_sizes,
      const std::vector<bool>& nullable,
      uint64_t range_idx,
      const std::vector<uint64_t>& range_coords,
      std::vector<ResultSize>* result_sizes,
      std::set<std::pair<unsigned, uint64_t>>* frag_tiles);

  /**
   * Returns a cropped version of the subarray, constrained in the
   * tile with the input coordinates. The new subarray will have
   * the input `layout`.
   */
  template <class T>
  Subarray crop_to_tile(const T* tile_coords, Layout layout) const;

  /** Returns the number of dimensions of the subarray. */
  uint32_t dim_num() const;

  /** Returns the domain the subarray is constructed from. */
  NDRange domain() const;

  /** ``True`` if the subarray does not contain any ranges. */
  bool empty() const;

  /**
   * Retrieves a range of a given dimension at a given range index.
   *
   * @note Note that the retrieved range may be invalid if
   *     Subarray::set_range() is called after this function. In that case,
   *     make sure to make a copy in the caller function.
   */
  Status get_range(
      uint32_t dim_idx, uint64_t range_idx, const Range** range) const;

  /**
   * Retrieves a range of a given dimension at a given range index.
   * The range is in the form (start, end).
   *
   * @note Note that the retrieved range may be invalid if
   *     Subarray::set_range() is called after this function. In that case,
   *     make sure to make a copy in the caller function.
   */
  Status get_range(
      uint32_t dim_idx,
      uint64_t range_idx,
      const void** start,
      const void** end) const;

  /**
   * Retrieves a range's start and end size for a given variable-length
   * dimensions at a given range index.
   *
   *
   * @param dim_idx The dimension to retrieve the range from.
   * @param range_idx The id of the range to retrieve.
   * @param start_size range start size in bytes
   * @param end_size range end size in bytes
   * @return Status
   */
  Status get_range_var_size(
      uint32_t dim_idx,
      uint64_t range_idx,
      uint64_t* start_size,
      uint64_t* end_size) const;

  /**
   * Retrieves a range from a variable-length dimension index in the form
   * (start, end).
   *
   * @param dim_idx The dimension to retrieve the range from.
   * @param range_idx The id of the range to retrieve.
   * @param start The range start to retrieve.
   * @param end The range end to retrieve.
   * @return Status
   */
  Status get_range_var(
      unsigned dim_idx, uint64_t range_idx, void* start, void* end) const;

  /** Retrieves the number of ranges on the given dimension index. */
  Status get_range_num(uint32_t dim_idx, uint64_t* range_num) const;

  /**
   * Retrieves a range from a dimension index in the form (start, end, stride).
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
   *
   * @param dim_index
   * @return returns true if the specified dimension is set to default subarray
   */
  bool is_default(uint32_t dim_index) const;

  /** Returns `true` if at least one dimension has non-default ranges set. */
  bool is_set() const;

  /** Returns number of non-default (set) ranges */
  int32_t count_set_ranges() const;

  /** Returns `true` if the input dimension has non-default range set. */
  bool is_set(unsigned dim_idx) const;

  /**
   * Returns ``true`` if the subarray is unary, which happens when it consists
   * of a single ND range **and** each 1D range is unary (i.e., consisting of
   * a single point in the 1D domain).
   */
  bool is_unary() const;

  /**
   * Returns ``true`` if the subarray range with the input id is unary
   * (i.e., consisting of a single point in the 1D domain).
   */
  bool is_unary(uint64_t range_idx) const;

  /**
   * Gets the estimated result size (in bytes) for the input fixed-sized
   * attribute/dimension.
   */
  Status get_est_result_size_internal(
      const char* name,
      uint64_t* size,
      const Config* config,
      ThreadPool* compute_tp);

  /**
   * Gets the estimated result size (in bytes) for the input fixed-sized
   * attribute/dimension.
   */
  Status get_est_result_size(
      const char* name, uint64_t* size, StorageManager* storage_manager);

  /**
   * Gets the estimated result size (in bytes) for the input var-sized
   * attribute/dimension.
   */
  Status get_est_result_size(
      const char* name,
      uint64_t* size_off,
      uint64_t* size_val,
      const Config* config,
      ThreadPool* compute_tp);

  /**
   * Gets the estimated result size (in bytes) for the input fixed-sized,
   * nullable attribute.
   */
  Status get_est_result_size_nullable(
      const char* name,
      uint64_t* size,
      uint64_t* size_validity,
      const Config* config,
      ThreadPool* compute_tp);

  /**
   * Gets the estimated result size (in bytes) for the input var-sized,
   * nullable attribute.
   */
  Status get_est_result_size_nullable(
      const char* name,
      uint64_t* size_off,
      uint64_t* size_val,
      uint64_t* size_validity,
      const Config* config,
      ThreadPool* compute_tp);

  /** returns whether the estimated result size has been computed or not */
  bool est_result_size_computed();

  /*
   * Gets the maximum memory required to produce the result (in bytes)
   * for the input fixed-sized attribute/dimensiom.
   */
  Status get_max_memory_size(
      const char* name,
      uint64_t* size,
      const Config* config,
      ThreadPool* compute_tp);

  /**
   * Gets the maximum memory required to produce the result (in bytes)
   * for the input var-sized attribute/dimension.
   */
  Status get_max_memory_size(
      const char* name,
      uint64_t* size_off,
      uint64_t* size_val,
      const Config* config,
      ThreadPool* compute_tp);

  /*
   * Gets the maximum memory required to produce the result (in bytes)
   * for the input fixed-sized, nullable attribute.
   */
  Status get_max_memory_size_nullable(
      const char* name,
      uint64_t* size,
      uint64_t* size_validity,
      const Config* config,
      ThreadPool* compute_tp);

  /**
   * Gets the maximum memory required to produce the result (in bytes)
   * for the input var-sized, nullable attribute.
   */
  Status get_max_memory_size_nullable(
      const char* name,
      uint64_t* size_off,
      uint64_t* size_val,
      uint64_t* size_validity,
      const Config* config,
      ThreadPool* compute_tp);

  /** Retrieves the query type of the subarray's array. */
  Status get_query_type(QueryType* type) const;

  /**
   * Returns the range coordinates (for all dimensions) given a flattened
   * 1D range id. The range coordinates is a tuple with an index
   * per dimension that uniquely identify a multi-dimensional
   * subarray range.
   */
  std::vector<uint64_t> get_range_coords(uint64_t range_idx) const;

  /**
   * Advances the input range coords to the next coords along the
   * subarray range layout.
   */
  void get_next_range_coords(std::vector<uint64_t>* range_coords) const;

  /**
   * Returns a subarray consisting of the ranges specified by
   * the input.
   *
   * @param start The subarray will be constructed from ranges in
   *     interval ``[start, end]`` in the flattened range order.
   * @param end The subarray will be constructed from ranges in
   *     interval ``[start, end]`` in the flattened range order.
   */
  Subarray get_subarray(uint64_t start, uint64_t end) const;

  /**
   * Set default indicator for dimension subarray. Used by serialization only
   * @param dim_index
   * @param is_default
   */
  void set_is_default(uint32_t dim_index, bool is_default);

  /** Sets the subarray layout. */
  void set_layout(Layout layout);

  /** Sets coalesce_ranges flag, intended for use by CAPI, to alloc matching
   * default coalesc-ranges=true semantics of internal class constructor, but
   * giving capi clients ability to turn off if desired.
   */
  Status set_coalesce_ranges(bool coalesce_ranges = true);

  /**
   * Flattens the subarray ranges in a byte vector. Errors out
   * if the subarray is not unary.
   */
  Status to_byte_vec(std::vector<uint8_t>* byte_vec) const;

  /** Returns the subarray layout. */
  Layout layout() const;

  /** Returns the flattened 1D id of the range with the input coordinates. */
  uint64_t range_idx(const std::vector<uint64_t>& range_coords) const;

  /** Returns the flattened 1D id of the range with the input coordinates for
   * the original subarray. */
  template <class T>
  void get_original_range_coords(
      const T* const range_coords,
      std::vector<uint64_t>* original_range_coords) const;

  /** The total number of multi-dimensional ranges in the subarray. */
  uint64_t range_num() const;

  /**
   * Returns the multi-dimensional range with the input id, based on the
   * order imposed on the the subarray ranges by the layout. If ``layout_``
   * is UNORDERED, then the range layout will be the same as the array's
   * cell order, since this will lead to more beneficial tile access
   * patterns upon a read query.
   */
  NDRange ndrange(uint64_t range_idx) const;

  /** Returns the NDRange corresponding to the input range coordinates. */
  NDRange ndrange(const std::vector<uint64_t>& range_coords) const;

  /**
   * Returns the `Range` vector for the given dimension index.
   * @note Intended for serialization only
   */
  const std::vector<Range>& ranges_for_dim(uint32_t dim_idx) const;

  /**
   * Directly sets the `Range` vector for the given dimension index, making
   * a deep copy.
   *
   * @param dim_idx Index of dimension to set
   * @param ranges `Range` vector that will be copied and set
   * @return Status
   *
   * @note Intended for serialization only
   */
  Status set_ranges_for_dim(uint32_t dim_idx, const std::vector<Range>& ranges);

  /**
   * Splits the subarray along the splitting dimension and value into
   * two new subarrays `r1` and `r2`.
   */
  Status split(
      unsigned splitting_dim,
      const ByteVecValue& splitting_value,
      Subarray* r1,
      Subarray* r2) const;

  /**
   * Splits the subarray along the splitting range, dimension and value
   * into two new subarrays `r1` and `r2`.
   */
  Status split(
      uint64_t splitting_range,
      unsigned splitting_dim,
      const ByteVecValue& splitting_value,
      Subarray* r1,
      Subarray* r2) const;

  /**
   * Returns the (unique) coordinates of all the tiles that the subarray
   * ranges intersect with.
   */
  const std::vector<std::vector<uint8_t>>& tile_coords() const;

  /**
   * Given the input tile coordinates, it returns a pointer to a tile
   * coords vector in `tile_coords_` (casted to `T`). This is typically
   * to avoid storing a tile coords vector in numerous cell slabs (and
   * instead store only a pointer to the tile coordinates vector
   * stored once in the subarray instance).
   *
   * `aux` should be of the same byte size as `tile_coords`. It is used
   * to avoid repeated allocations of the auxiliary vector need for
   * converting the `tile_coords` vector of type `T` to a vector of
   * type `uint8_t` before searching for `tile_coords` in `tile_coords_`.
   */
  template <class T>
  const T* tile_coords_ptr(
      const std::vector<T>& tile_coords, std::vector<uint8_t>* aux) const;

  /**
   * Returns the internal `TileOverlap` instance. The caller is responsible
   * for ensuring that the input indexes are valid.
   *
   * @param fragment_idx The fragment index.
   * @param range_idx The range index.
   */
  inline const TileOverlap* tile_overlap(
      uint64_t fragment_idx, uint64_t range_idx) const {
    return tile_overlap_.at(fragment_idx, range_idx);
  }

  /**
   * Returns the precomputed tile overlap. This may not contain
   * tile overlap information for each range in this subarray. The
   * caller is responsible for inspecting the returned object to
   * determine the ranges it has information for.
   */
  const SubarrayTileOverlap* subarray_tile_overlap() const;

  /**
   * Compute `tile_coords_` and `tile_coords_map_`. The coordinates will
   * be sorted on the array tile order.
   *
   * @tparam T The subarray datatype.
   * @return Status
   */
  template <class T>
  Status compute_tile_coords();

  /**
   * Computes the estimated result sizes for the input attribute/dimension
   * names, assuming the tile overlap is already computed. This function focuses
   * only on fragments relevant to the subarray and the input range ids.
   * The result retrieved contains the estimated result sizes per
   * dimension/attribute per range.
   *
   * The function also retrieves a `mem_sizes` vector of vectors, which
   * holds values for each range and each attribute. Each value represents
   * the **unique** bytes that the correpsonding range contributes to
   * the maximum memory size for all ranges (i.e., based on whether
   * it overlaps a unique tile versus all previous ranges in the vector).
   */
  Status compute_relevant_fragment_est_result_sizes(
      const std::vector<std::string>& names,
      uint64_t range_start,
      uint64_t range_end,
      std::vector<std::vector<ResultSize>>* result_sizes,
      std::vector<std::vector<MemorySize>>* mem_sizes,
      ThreadPool* compute_tp);

  /**
   * Used by serialization to set the estimated result size
   *
   * @param est_result_size map to set
   * @param max_mem_size map to set
   * @return Status
   */
  Status set_est_result_size(
      std::unordered_map<std::string, ResultSize>& est_result_size,
      std::unordered_map<std::string, MemorySize>& max_mem_size);

  /**
   * Used by serialization to get the map of result sizes
   * @return
   */
  std::unordered_map<std::string, ResultSize> get_est_result_size_map(
      const Config* config, ThreadPool* compute_tp);

  /**
   * Used by serialization to get the map of max mem sizes
   * @return
   */
  std::unordered_map<std::string, MemorySize> get_max_mem_size_map(
      const Config* config, ThreadPool* compute_tp);

  /**
   * Return relevant fragments as computed
   */
  const std::vector<unsigned>* relevant_fragments() const;

  /**
   * Return relevant fragments as computed
   */
  std::vector<unsigned>* relevant_fragments();

  /**
   * For flattened ("total order") start/end range indexes,
   * return the starting and ending ND-range coordinates that
   * contain the minimum space to contain all flattened ranges.
   *
   * @param range_idx_start flattened range starting index, inclusive.
   * @param range_idx_end flattened range ending index, inclusive.
   * @param start_coords mutated to contain the nd-range start coordinates.
   * @param end_coords mutated to contain the nd-range end coordinates.
   */
  void get_expanded_coordinates(
      uint64_t range_idx_start,
      uint64_t range_idx_end,
      std::vector<uint64_t>* start_coords,
      std::vector<uint64_t>* end_coords) const;

  /** Returns `stats_`. */
  stats::Stats* stats() const;

  /** Stores a vector of 1D ranges per dimension. */
  std::vector<std::vector<uint64_t>> original_range_idx_;

  /** Returns if ranges are sorted. */
  bool ranges_sorted() {
    return ranges_sorted_;
  }

  /** Sort ranges per dimension. */
  Status sort_ranges(ThreadPool* const compute_tp);

  /** Returns if all ranges for this subarray are non overlapping. */
  tuple<Status, optional<bool>> non_overlapping_ranges(
      ThreadPool* const compute_tp);

 private:
  /* ********************************* */
  /*        PRIVATE DATA TYPES         */
  /* ********************************* */

  /**
   * An opaque context to be used between successive calls
   * to `compute_relevant_fragments`.
   */
  struct ComputeRelevantFragmentsCtx {
    ComputeRelevantFragmentsCtx()
        : initialized_(false) {
    }

    /**
     * This context cache is lazy initialized. This will be
     * set to `true` when initialized in `compute_relevant_fragments()`.
     */
    bool initialized_;

    /**
     * The last calibrated start coordinates.
     */
    std::vector<uint64_t> last_start_coords_;

    /**
     * The last calibrated end coordinates.
     */
    std::vector<uint64_t> last_end_coords_;

    /**
     * The fragment bytemaps for each dimension. The inner
     * vector is the fragment bytemap that has a byte element
     * for each fragment. Non-zero bytes represent relevant
     * fragments for a specific dimension. Each dimension
     * has its own fragment bytemap (the outer vector).
     */
    std::vector<std::vector<uint8_t>> frag_bytemaps_;
  };

  /**
   * An opaque context to be used between successive calls
   * to `compute_relevant_fragment_tile_overlap`.
   */
  struct ComputeRelevantTileOverlapCtx {
    ComputeRelevantTileOverlapCtx()
        : range_idx_offset_(0)
        , range_len_(0) {
    }

    /**
     * The current range index offset. This points to the ending
     * index from the last invocation of
     * `compute_relevant_fragment_tile_overlap`.
     */
    uint64_t range_idx_offset_;

    /** The number of ranges. */
    uint64_t range_len_;
  };

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The class stats. */
  stats::Stats* stats_;

  /** UID of the logger instance */
  inline static std::atomic<uint64_t> logger_id_ = 0;

  /** The class logger. */
  tdb_shared_ptr<Logger> logger_;

  /** The array the subarray object is associated with. */
  const Array* array_;

  /** Stores the estimated result size for each array attribute/dimension. */
  std::unordered_map<std::string, ResultSize> est_result_size_;

  /** Stores the maximum memory size for each array attribute/dimension. */
  std::unordered_map<std::string, MemorySize> max_mem_size_;

  /**
   * The layout of the subarray (i.e., of the results
   * if the subarray is used for reads, or of the values provided
   * by the user for writes).
   */
  Layout layout_;

  /**
   * The array cell order. We explicitly store it as it may be used
   * numerous times and we'd better have it readily available.
   */
  Layout cell_order_;

  /**
   * Stores a vector of RangeSetAndSuperset objects, one per dimension, for
   * handling operations on ranges.
   */
  std::vector<RangeSetAndSuperset> range_subset_;

  /**
   * Flag storing if each dimension is a default value or not.
   *
   * TODO: Remove this variable and store `is_default` directly with NDRange.
   */
  std::vector<bool> is_default_;

  /** Important for computed an ND range index from a flat serialized index. */
  std::vector<uint64_t> range_offsets_;

  /**
   * ``True`` if the estimated result size for all attributes/dimensions has
   * been computed.
   */
  bool est_result_size_computed_;

  /**
   * The array fragment ids whose non-empty domain intersects at
   * least one subarray range.
   */
  std::vector<unsigned> relevant_fragments_;

  /**
   * The precomputed tile overlap state. Is not guaranteed to be
   * computed for all ranges in this subarray.
   */
  SubarrayTileOverlap tile_overlap_;

  /**
   * ``True`` if ranges should attempt to be coalesced as they are added.
   */
  bool coalesce_ranges_;

  /**
   * The tile coordinates that the subarray overlaps with. Note that
   * the datatype must be casted to the datatype of the subarray upon use.
   */
  std::vector<std::vector<uint8_t>> tile_coords_;

  /** A map (tile coords) -> (vector element position in `tile_coords_`). */
  std::map<std::vector<uint8_t>, size_t> tile_coords_map_;

  /** The config for query-level parameters only. */
  Config config_;

  /** State of specific Config item needed from multiple locations. */
  bool err_on_range_oob_ = true;

  /** Indicate if ranges are sorted. */
  bool ranges_sorted_;

  /** Mutext to protect sorting ranges. */
  std::mutex ranges_sort_mtx_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /**
   * This function adds a range that covers the whole domain along
   * each dimension. When ``add_range()`` is called for the first
   * time along a dimension, the corresponding default range is
   * replaced.
   */
  void add_default_ranges();

  /** Computes the estimated result size for all attributes/dimensions. */
  Status compute_est_result_size(const Config* config, ThreadPool* compute_tp);

  /**
   * Compute `tile_coords_` and `tile_coords_map_`. The coordinates will
   * be sorted on col-major tile order.
   *
   * @tparam T The subarray datatype.
   * @return Status
   */
  template <class T>
  Status compute_tile_coords_col();

  /**
   * Compute `tile_coords_` and `tile_coords_map_`. The coordinates will
   * be sorted on row-major tile order.
   *
   * @tparam T The subarray datatype.
   * @return Status
   */
  template <class T>
  Status compute_tile_coords_row();

  /** Returns a deep copy of this Subarray. */
  Subarray clone() const;

  /**
   * Compute the tile overlap between ``range`` and the non-empty domain
   * of the input fragment. Applicable only to dense fragments.
   *
   * @param range_idx The id of the range to compute the overlap with.
   * @param fid The id of the fragment to focus on.
   * @return The tile overlap.
   */
  TileOverlap compute_tile_overlap(uint64_t range_idx, unsigned fid) const;

  /**
   * Compute the tile overlap between ``range`` and the non-empty domain
   * of the input fragment. Applicable only to dense fragments.
   *
   * @tparam T The domain data type.
   * @param range_idx The id of the range to compute the overlap with.
   * @param fid The id of the fragment to focus on.
   * @return The tile overlap.
   */
  template <class T>
  TileOverlap compute_tile_overlap(uint64_t range_idx, unsigned fid) const;

  /**
   * Swaps the contents (all field values) of this subarray with the
   * given subarray.
   */
  void swap(Subarray& subarray);

  /**
   * Computes the indexes of the fragments that are relevant to the query,
   * that is those whose non-empty domain intersects with at least one
   * range.
   *
   * @param compute_tp The thread pool for compute-bound tasks.
   * @param tile_overlap Mutated to store the computed tile overlap.
   * @param fn_ctx An opaque context object to be used between successive
   * invocations.
   */
  Status compute_relevant_fragments(
      ThreadPool* compute_tp,
      const SubarrayTileOverlap* tile_overlap,
      ComputeRelevantFragmentsCtx* fn_ctx);

  /**
   * Computes the relevant fragment bytemap for a specific dimension.
   *
   * @param compute_tp The thread pool for compute-bound tasks.
   * @param dim_idx The index of the dimension to compute on.
   * @param fragment_num The number of fragments to compute on.
   * @param start_coords The starting range coordinates to compute between.
   * @param end_coords The ending range coordinates to compute between.
   * @param frag_bytemap The fragment bytemap to mutate.
   */
  Status compute_relevant_fragments_for_dim(
      ThreadPool* compute_tp,
      uint32_t dim_idx,
      uint64_t fragment_num,
      const std::vector<uint64_t>& start_coords,
      const std::vector<uint64_t>& end_coords,
      std::vector<uint8_t>* frag_bytemap) const;

  /** Loads the R-Trees of all relevant fragments in parallel. */
  Status load_relevant_fragment_rtrees(ThreadPool* compute_tp) const;

  /**
   * Computes the tile overlap for each range and relevant fragment.
   *
   * @param compute_tp The thread pool for compute-bound tasks.
   * @param tile_overlap Mutated to store the computed tile overlap.
   * @param fn_ctx An opaque context object to be used between successive
   * invocations.
   */
  Status compute_relevant_fragment_tile_overlap(
      ThreadPool* compute_tp,
      SubarrayTileOverlap* tile_overlap,
      ComputeRelevantTileOverlapCtx* fn_ctx);

  /**
   * Computes the tile overlap for all ranges on the given relevant fragment.
   *
   * @param meta The fragment metadat to focus on.
   * @param frag_idx The fragment id.
   * @param dense Whether the fragment is dense or sparse.
   * @param compute_tp The thread pool for compute-bound tasks.
   * @param tile_overlap Mutated to store the computed tile overlap.
   * @param fn_ctx An opaque context object to be used between successive
   * invocations.
   * @return Status
   */
  Status compute_relevant_fragment_tile_overlap(
      tdb_shared_ptr<FragmentMetadata> meta,
      unsigned frag_idx,
      bool dense,
      ThreadPool* compute_tp,
      SubarrayTileOverlap* tile_overlap,
      ComputeRelevantTileOverlapCtx* fn_ctx);

  /**
   * Load the var-sized tile sizes for the input names and from the
   * relevant fragments.
   */
  Status load_relevant_fragment_tile_var_sizes(
      const std::vector<std::string>& names, ThreadPool* compute_tp) const;

  /**
   * Sort ranges for a particular dimension
   *
   * @tparam T dimension type
   * @param compute_tp threadpool for parallel_sort
   * @param dim_idx dimension index to sort
   * @return Status
   */
  template <typename T>
  Status sort_ranges_for_dim(
      ThreadPool* const compute_tp, const uint64_t& dim_idx);

  /**
   * Sort ranges for a particular dimension
   *
   * @param compute_tp threadpool for parallel_sort
   * @param dim_idx dimension index to sort
   * @return Status
   */
  Status sort_ranges_for_dim(
      ThreadPool* const compute_tp, const uint64_t& dim_idx);

  /**
   * Determine if ranges for a dimension are non overlapping.
   *
   * @param dim_idx dimension index.
   * @return true if the ranges are non overlapping, false otherwise.
   */
  template <typename T>
  tuple<Status, optional<bool>> non_overlapping_ranges_for_dim(
      const uint64_t dim_idx);

  /**
   * Determine if ranges for a dimension are non overlapping.
   *
   * @param dim_idx dimension index.
   * @return true if the ranges are non overlapping, false otherwise.
   */
  tuple<Status, optional<bool>> non_overlapping_ranges_for_dim(
      const uint64_t dim_idx);
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_SUBARRAY_H
