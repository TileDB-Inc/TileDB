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

#include "tiledb/common/logger.h"
#include "tiledb/common/thread_pool.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/misc/tile_overlap.h"
#include "tiledb/sm/misc/types.h"

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
  /*           TYPE DEFINITIONS        */
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
   * @param coalesce_ranges When enabled, ranges will attempt to coalesce
   *     with existing ranges as they are added.
   */
  Subarray(const Array* array, bool coalesce_ranges = true);

  /**
   * Constructor.
   *
   * @param array The array the subarray is associated with.
   * @param layout The layout of the values of the subarray (of the results
   *     if the subarray is used for reads, or of the values provided
   *     by the user for writes).
   * @param coalesce_ranges When enabled, ranges will attempt to coalesce
   *     with existing ranges as they are added.
   */
  Subarray(const Array* array, Layout layout, bool coalesce_ranges = true);

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

  /** Adds a range along the dimension with the given index. */
  Status add_range(uint32_t dim_idx, const Range& range);

  /**
   * Adds a range along the dimension with the given index, without
   * performing any error checks.
   */
  Status add_range_unsafe(uint32_t dim_idx, const Range& range);

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
   * Computes the tile overlap with all subarray ranges for
   * all fragments.
   */
  Status compute_tile_overlap(ThreadPool* compute_tp);

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
      const EncryptionKey* encryption_key,
      const ArraySchema* array_schema,
      bool all_dims_same_type,
      bool all_dims_fixed,
      const std::vector<FragmentMetadata*>& fragment_meta,
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

  /** Retrieves the number of ranges on the given dimension index. */
  Status get_range_num(uint32_t dim_idx, uint64_t* range_num) const;

  /**
   *
   * @param dim_index
   * @return returns true if the specified dimension is set to default subarray
   */
  bool is_default(uint32_t dim_index) const;

  /** Returns `true` if at least one dimension has non-default ranges set. */
  bool is_set() const;

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
  Status get_est_result_size(
      const char* name, uint64_t* size, ThreadPool* compute_tp);

  /**
   * Gets the estimated result size (in bytes) for the input var-sized
   * attribute/dimension.
   */
  Status get_est_result_size(
      const char* name,
      uint64_t* size_off,
      uint64_t* size_val,
      ThreadPool* compute_tp);

  /**
   * Gets the estimated result size (in bytes) for the input fixed-sized,
   * nullable attribute.
   */
  Status get_est_result_size_nullable(
      const char* name,
      uint64_t* size,
      uint64_t* size_validity,
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
      ThreadPool* compute_tp);

  /** returns whether the estimated result size has been computed or not */
  bool est_result_size_computed();

  /*
   * Gets the maximum memory required to produce the result (in bytes)
   * for the input fixed-sized attribute/dimensiom.
   */
  Status get_max_memory_size(
      const char* name, uint64_t* size, ThreadPool* compute_tp);

  /**
   * Gets the maximum memory required to produce the result (in bytes)
   * for the input var-sized attribute/dimension.
   */
  Status get_max_memory_size(
      const char* name,
      uint64_t* size_off,
      uint64_t* size_val,
      ThreadPool* compute_tp);

  /*
   * Gets the maximum memory required to produce the result (in bytes)
   * for the input fixed-sized, nullable attribute.
   */
  Status get_max_memory_size_nullable(
      const char* name,
      uint64_t* size,
      uint64_t* size_validity,
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

  /** Sets the array layout. */
  void set_layout(Layout layout);

  /**
   * Flattens the subarray ranges in a byte vector. Errors out
   * if the subarray is not unary.
   */
  Status to_byte_vec(std::vector<uint8_t>* byte_vec) const;

  /** Returns the subarray layout. */
  Layout layout() const;

  /** Returns the flattened 1D id of the range with the input coordinates. */
  uint64_t range_idx(const std::vector<uint64_t>& range_coords) const;

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
   * Returns the tile overlap of the subarray.
   *
   * The outer vector is indexed by fragment ids and the inner vector is
   * indexed by range indexes.
   *
   * As an optimization, the underlying data structure may be shared between
   * `Subarray` instances and their partitioned `Subarray` instances created
   * by the `SubarrayPartitioner`. The caller must use the
   * `Subarray::overlap_range_offset` to determine the range index that points
   * to the starting range of this `Subarray` instance.
   */
  const std::vector<std::vector<TileOverlap>>& tile_overlap() const;

  /**
   * The `Subarray::tile_overlap` returns a data structure where the
   * outter vector is indexed by fragment ids and the inner vector
   * is indexed by range ids. This API returns the offset into the
   * inner vector that points to the first range used by this
   * `Subarray` instance.
   */
  uint64_t overlap_range_offset() const;

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
      ThreadPool* compute_tp);

  /**
   * Used by serialization to get the map of max mem sizes
   * @return
   */
  std::unordered_map<std::string, MemorySize> get_max_mem_size_map(
      ThreadPool* compute_tp);

  /**
   * Return relevant fragments as computed
   */
  std::vector<unsigned> relevant_fragments() const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

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

  /** Stores a vector of 1D ranges per dimension. */
  std::vector<std::vector<Range>> ranges_;

  /**
   * One value per dimension indicating whether the (single) range set in
   * `ranges_` is the default range.
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
   * Stores info about the overlap of the subarray with tiles
   * of all array fragments. Each element is a vector corresponding
   * to a single range of the subarray. These vectors/ranges are sorted
   * according to ``layout_``.
   *
   * This is shared between a `Subarray` and all of its `Subarray` partitions
   * created with the `Subarray::get_subarray` API.
   */
  tdb_shared_ptr<std::vector<std::vector<TileOverlap>>> tile_overlap_;

  /**
   * The first index in `tile_overlap_` corresponds to a fragment
   * index. The second index corresponds to a range index. This
   * variable stores the range index for the first range in
   * this instance.
   */
  uint64_t tile_overlap_range_offset_;

  /**
   * ``True`` if ranges should attempt to be coalesced as they are added.
   */
  bool coalesce_ranges_;

  /**
   * Each element on the vector contains a template-bound variant of
   * `add_or_coalesce_range()` for the respective dimension's data
   * type. Dimensions with data types that we do not attempt to
   * coalesce (e.g. floats and var-sized data types), this will be
   * bound to `add_range_without_coalesce` as an optimization.
   */
  std::vector<std::function<void(Subarray*, uint32_t, const Range&)>>
      add_or_coalesce_range_func_;

  /**
   * The tile coordinates that the subarray overlaps with. Note that
   * the datatype must be casted to the datatype of the subarray upon use.
   */
  std::vector<std::vector<uint8_t>> tile_coords_;

  /** A map (tile coords) -> (vector element poistion in `tile_coords_`). */
  std::map<std::vector<uint8_t>, size_t> tile_coords_map_;

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
  Status compute_est_result_size(ThreadPool* compute_tp);

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
  TileOverlap get_tile_overlap(uint64_t range_idx, unsigned fid) const;

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
  TileOverlap get_tile_overlap(uint64_t range_idx, unsigned fid) const;

  /**
   * Swaps the contents (all field values) of this subarray with the
   * given subarray.
   */
  void swap(Subarray& subarray);

  /**
   * Computes the indexes of the fragments that are relevant to the query,
   * that is those whose non-empty domain intersects with at least one
   * range.
   */
  Status compute_relevant_fragments(ThreadPool* compute_tp);

  /** Loads the R-Trees of all relevant fragments in parallel. */
  Status load_relevant_fragment_rtrees(ThreadPool* compute_tp) const;

  /** Computes the tile overlap for each range and relevant fragment. */
  Status compute_relevant_fragment_tile_overlap(
      ThreadPool* compute_tp,
      std::vector<std::vector<TileOverlap>>* tile_overlap);

  /**
   * Computes the tile overlap for all ranges on the given relevant fragment.
   *
   * @param meta The fragment metadat to focus on.
   * @param frag_idx The fragment id.
   * @param dense Whether the fragment is dense or sparse.
   * @param range_num The number of ranges.
   * @param compute_tp The thread pool for compute-bound tasks.
   * @param tile_overlap Mutated to store the computed tile overlap.
   * @return Status
   */
  Status compute_relevant_fragment_tile_overlap(
      FragmentMetadata* meta,
      unsigned frag_idx,
      bool dense,
      uint64_t range_num,
      ThreadPool* compute_tp,
      std::vector<std::vector<TileOverlap>>* tile_overlap);

  /**
   * Load the var-sized tile sizes for the input names and from the
   * relevant fragments.
   */
  Status load_relevant_fragment_tile_var_sizes(
      const std::vector<std::string>& names, ThreadPool* compute_tp) const;

  /**
   * Constructs `add_or_coalesce_range_func_` for all dimensions
   * in `array_->array_schema()`.
   */
  void set_add_or_coalesce_range_func();

  /**
   * Appends `range` onto `ranges_[dim_idx]` without attempting to
   * coalesce `range` with any existing ranges.
   */
  void add_range_without_coalesce(uint32_t dim_idx, const Range& range);

  /**
   * Coalesces `range` with the last element on `ranges_[dim_idx]`
   * if they are contiguous. Otherwise, `range` will be appended to
   * `ranges_[dim_idx]`.
   *
   * @tparam T The range data type.
   * @param dim_idx The dimension index into `ranges_`.
   * @param range The range to add or coalesce.
   */
  template <class T>
  void add_or_coalesce_range(uint32_t dim_idx, const Range& range);
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_SUBARRAY_H
