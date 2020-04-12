/**
 * @file   subarray.h
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
 * This file defines class Subarray.
 */

#ifndef TILEDB_SUBARRAY_H
#define TILEDB_SUBARRAY_H

#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/tile_overlap.h"
#include "tiledb/sm/misc/types.h"

#include <cmath>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <vector>

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
    /**
     * Maximum size of overlapping tiles fetched into memory for
     * fixed-sized attributes/dimensions or offsets of var-sized
     * attributes/dimensions.
     */
    uint64_t mem_size_fixed_;
    /**
     * Maximum size of overlapping tiles fetched into memory for
     * var-sized attributes/dimensions.
     */
    uint64_t mem_size_var_;
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
   */
  Subarray(const Array* array);

  /**
   * Constructor.
   *
   * @param array The array the subarray is associated with.
   * @param layout The layout of the values of the subarray (of the results
   *     if the subarray is used for reads, or of the values provided
   *     by the user for writes).
   */
  Subarray(const Array* array, Layout layout);

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

  /** Returns the number of cells in the input ND range. */
  uint64_t cell_num(uint64_t range_idx) const;

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
  Status compute_tile_overlap();

  /**
   * Computes the estimated result size (calibrated using the maximum size)
   * for a given attribute/dimension and range id, for all fragments.
   *
   * @param encryption_key The encryption key of the array.
   * @param array_schema The array schema.
   * @param fragment_meta The fragment metadata of the array.
   * @param name The name of the attribute/dimension to focus on.
   * @param range_idx The id of the subarray range to focus on.
   * @param var_size Whether the attribute/dimension is var-sized or not.
   * @param result_size The result size to be retrieved.
   * @return Status
   */
  Status compute_est_result_size(
      const EncryptionKey* encryption_key,
      const ArraySchema* array_schema,
      const std::vector<FragmentMetadata*>& fragment_meta,
      const std::string& name,
      uint64_t range_idx,
      bool var_size,
      ResultSize* result_size) const;

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
  Status get_est_result_size(const char* name, uint64_t* size);

  /**
   * Gets the estimated result size (in bytes) for the input var-sized
   * attribute/dimension.
   */
  Status get_est_result_size(
      const char* name, uint64_t* size_off, uint64_t* size_val);

  /*
   * Gets the maximum memory required to produce the result (in bytes)
   * for the input fixed-sized attribute/dimensiom.
   */
  Status get_max_memory_size(const char* name, uint64_t* size);

  /**
   * Gets the maximum memory required to produce the result (in bytes)
   * for the input var-sized attribute/dimension.
   */
  Status get_max_memory_size(
      const char* name, uint64_t* size_off, uint64_t* size_val);

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

  /** Returns the tile overlap of the subarray. */
  const std::vector<std::vector<TileOverlap>>& tile_overlap() const;

  /**
   * Compute `tile_coords_` and `tile_coords_map_`. The coordinates will
   * be sorted on the array tile order.
   *
   * @tparam T The subarray datatype.
   */
  template <class T>
  void compute_tile_coords();

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The array the subarray object is associated with. */
  const Array* array_;

  /** Stores the estimated result size for each array attribute/dimension. */
  std::unordered_map<std::string, ResultSize> est_result_size_;

  /**
   * The layout of the subarray (i.e., of the results
   * if the subarray is used for reads, or of the values provided
   * by the user for writes).
   */
  Layout layout_;

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
   * Stores info about the overlap of the subarray with tiles
   * of all array fragments. Each element is a vector corresponding
   * to a single range of the subarray. These vectors/ranges are sorted
   * according to ``layout_``.
   */
  std::vector<std::vector<TileOverlap>> tile_overlap_;

  /**
   * ``True`` if the tile overlap for the ranges of the subarray has
   *  been computed.
   */
  bool tile_overlap_computed_;

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
  Status compute_est_result_size();

  /**
   * Compute `tile_coords_` and `tile_coords_map_`. The coordinates will
   * be sorted on col-major tile order.
   *
   * @tparam T The subarray datatype.
   */
  template <class T>
  void compute_tile_coords_col();

  /**
   * Compute `tile_coords_` and `tile_coords_map_`. The coordinates will
   * be sorted on row-major tile order.
   *
   * @tparam T The subarray datatype.
   */
  template <class T>
  void compute_tile_coords_row();

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
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_SUBARRAY_H
