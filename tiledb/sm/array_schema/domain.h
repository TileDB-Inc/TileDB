/**
 * @file   domain.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2023 TileDB, Inc.
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
 * This file defines class Domain.
 */

#ifndef TILEDB_DOMAIN_H
#define TILEDB_DOMAIN_H

#include "tiledb/common/common.h"
#include "tiledb/common/macros.h"
#include "tiledb/common/pmr.h"
#include "tiledb/common/status.h"
#include "tiledb/common/types/dynamic_typed_datum.h"
#include "tiledb/common/types/untyped_datum.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/storage_format/serialization/serializers.h"

#include <vector>

using namespace tiledb::common;
using namespace tiledb::type;

namespace tiledb::type {
class DomainDataRef;
class Range;
}  // namespace tiledb::type

namespace tiledb::sm {

class Buffer;
class ConstBuffer;
class Dimension;
class DomainTypedDataView;
class CurrentDomain;
class NDRectangle;
class FilterPipeline;
class MemoryTracker;
enum class Datatype : uint8_t;
enum class Layout : uint8_t;

/** Defines an array domain, which consists of dimensions. */
class Domain {
 public:
  /**
   * Size type for the number of dimensions of an array and for dimension
   * indices.
   */
  using dimension_size_type = unsigned int;

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Deleted default constructor */
  Domain() = delete;

  /** Constructor. */
  Domain(shared_ptr<MemoryTracker> memory_tracker);

  /** Constructor.*/
  Domain(
      Layout cell_order,
      const std::vector<shared_ptr<Dimension>> dimensions,
      Layout tile_order,
      shared_ptr<MemoryTracker> memory_tracker);

  /** Destructor. */
  ~Domain() = default;

  /* ********************************* */
  /*             OPERATORS             */
  /* ********************************* */

  DISABLE_COPY_AND_COPY_ASSIGN(Domain);
  DISABLE_MOVE_AND_MOVE_ASSIGN(Domain);

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /**
   * Adds a dimension to the domain.
   *
   * @param dim The dimension to be added.
   */
  void add_dimension(shared_ptr<Dimension> dim);

  /** Returns true if all dimensions have fixed-sized domain datatypes. */
  bool all_dims_fixed() const;

  /** Returns true if all dimensions have integer domain types. */
  bool all_dims_int() const;

  /** Returns true if all dimensions have string domain types. */
  bool all_dims_string() const;

  /** Returns true if all dimensions have real domain types. */
  bool all_dims_real() const;

  /** Returns true if all dimensions have the same type. */
  bool all_dims_same_type() const;

  /** Returns the number of cells per tile (only for the dense case). */
  uint64_t cell_num_per_tile() const;

  /**
   * Checks the cell order of the input coordinates. Note that, in the presence
   * of a regular tile grid, this function assumes that the cells are in the
   * same regular tile.
   *
   * @param dim_idx The dimension to compare the coordinates on.
   * @param a The first input coordinates.
   * @param b The second input coordinates.
   * @return One of the following:
   *    - -1 if the first coordinate precedes the second
   *    -  0 if the two coordinates are identical
   *    - +1 if the first coordinate succeeds the second
   */
  int cell_order_cmp(
      unsigned dim_idx,
      const UntypedDatumView a,
      const UntypedDatumView b) const;

  /**
   * Checks the cell order of the input coordinates. Since the coordinates
   * are given for a single dimension, this function simply checks which
   * coordinate is larger.
   *
   * @param coord_a The first coordinate.
   * @param coord_b The second coordinate.
   * @return One of the following:
   *    - -1 if the first coordinate is smaller than the second
   *    -  0 if the two coordinates have the same cell order
   *    - +1 if the first coordinate is larger than the second
   */
  template <class T>
  static int cell_order_cmp_2(const void* coord_a, const void* coord_b);

  /**
   * Checks the cell order of the input coordinates.
   *
   * @param left Left operand
   * @param right Right operand
   * @return One of the following:
   *    - -1 if the left coordinates precede the right on the cell order
   *    -  0 if the two coordinates have the same cell order
   *    - +1 if the left coordinates succeed the right on the cell order
   */
  int cell_order_cmp(
      const type::DomainDataRef& left, const type::DomainDataRef& right) const;

  /**
   * Populates the object members from the data in the input binary buffer.
   *
   * @param deserializer The deserializer to deserialize from.
   * @param version The array schema version.
   * @param cell_order Cell order.
   * @param tile_order Tile order.
   * @param coords_filters Coords filters to replace empty coords pipelines.
   * @param memory_tracker The memory tracker to use.
   * @return Status and Domain
   */
  static shared_ptr<Domain> deserialize(
      Deserializer& deserializer,
      uint32_t version,
      Layout cell_order,
      Layout tile_order,
      FilterPipeline& coords_filters,
      shared_ptr<MemoryTracker> memory_tracker);

  /** Returns the cell order. */
  Layout cell_order() const;

  /**
   * Crops the input ND range such that it does not exceed the array domain.
   */
  void crop_ndrange(NDRange* ndrange) const;

  /** Returns the tile order. */
  Layout tile_order() const;

  /** Returns the number of dimensions. */
  inline dimension_size_type dim_num() const {
    return dim_num_;
  }

  /** Returns the domain along the i-th dimension. */
  const Range& domain(unsigned i) const;

  /** Returns the domain as a N-dimensional range object. */
  NDRange domain() const;

  /**
   * Return a pointer to the dimension given by the argument index
   *
   * This function does not return `nullptr`. It throws if the argument is
   * invalid. It relies on the class invariant that absent dimensions (as
   * manifested by a null pointer) are not allowed.
   *
   * @param i index of the dimension within the domain
   * @return non-null pointer to the dimension
   */
  inline const Dimension* dimension_ptr(dimension_size_type i) const {
    if (i >= dim_num_) {
      throw std::invalid_argument("invalid dimension index");
    }
    return dimension_ptrs_[i];
  }

  /**
   * Return a copy of the shared pointer to the dimension given by the argument
   * index.
   *
   * This function does not return null pointers.
   *
   * This function is intended for use with the C API for initializing handles,
   * and in life cycle management generally. Ordinary functions within the
   * library should use `dimension_ptr`.
   *
   * @param i index of the dimension within the domain
   * @return non-null pointer to the dimension
   */
  inline shared_ptr<Dimension> shared_dimension(dimension_size_type i) const {
    if (i >= dim_num_) {
      throw std::invalid_argument("invalid dimension index");
    }
    return dimensions_[i];
  }

  /**
   * @return a view over the dimensions
   */
  inline std::span<const shared_ptr<Dimension>> dimensions() const {
    return std::span(dimensions_.begin(), dimensions_.end());
  }

  /** Returns the dimension given a name (nullptr upon error). */
  const Dimension* dimension_ptr(const std::string& name) const;

  /**
   * A copy of the storage pointer to a dimension given a name.
   *
   * This function is intended for use with the C API for initializing handles,
   * and in life cycle management generally. Ordinary functions within the
   * library should use `dimension_ptr`.
   *
   * @param name candidate name of a dimension
   * @return copy of the storage pointer to the dimension with matching name,
   * a null pointer otherwise.
   */
  shared_ptr<Dimension> shared_dimension(const std::string& name) const;

  /** Expands ND range `r2` using ND range `r1`. */
  void expand_ndrange(const NDRange& r1, NDRange* r2) const;

  /**
   * Expands the input domain such that it coincides with the boundaries of
   * the array's regular tiles (i.e., it maps it on the regular tile grid).
   * If the array has no regular tile grid or real domain, the function
   * does not do anything.
   *
   * @param query_ndrange The query domain to be expanded.
   */
  void expand_to_tiles_when_no_current_domain(NDRange& query_ndrange) const;

  /**
   * Retrieves the tile coordinates of the input cell coordinates.
   *
   * @tparam T The domain type.
   * @param coords The cell coordinates.
   * @param tile_coords The tile coordinates of the cell coordinates to
   *     be retrieved.
   */
  template <class T>
  void get_tile_coords(const T* coords, T* tile_coords) const;

  /**
   * Retrieves the next tile coordinates along the array tile order within a
   * given tile domain. Applicable only to **dense** arrays.
   *
   * @tparam T The coordinates type.
   * @param domain The targeted domain.
   * @param tile_coords The input tile coordinates, which the function modifies
   *     to store the next tile coordinates at termination.
   * @return void
   */
  template <class T>
  void get_next_tile_coords(const T* domain, T* tile_coords) const;

  /**
   * Retrieves the next tile coordinates along the array tile order within a
   * given tile domain. Applicable only to **dense** arrays.
   *
   * @tparam T The coordinates type.
   * @param domain The targeted domain.
   * @param tile_coords The input tile coordinates, which the function modifies
   *     to store the next tile coordinates at termination.
   * @param in Set to `true` if the retrieve coords are inside the domain.
   * @return void
   */
  template <class T>
  void get_next_tile_coords(const T* domain, T* tile_coords, bool* in) const;

  /**
   * Gets the tile domain of the input cell `subarray`.
   *
   * @tparam T The domain type.
   * @param subarray The input (cell) subarray.
   * @param tile_subarray The tile subarray in the tile domain to be retrieved.
   * @return void
   */
  template <class T>
  void get_tile_domain(const T* subarray, T* tile_subarray) const;

  /**
   * Returns the tile position along the array tile order within the input
   * domain. Applicable only to **dense** arrays.
   *
   * @tparam T The domain type.
   * @param domain The input domain, which is a cell domain partitioned into
   *     regular tiles in the same manner as that of the array domain (however
   *     *domain* may be a sub-domain of the array domain).
   * @param tile_coords The tile coordinates, normalized inside the tile
   *     domain of cell `domain`.
   * @return The tile position of *tile_coords* along the tile order of the
   *     array inside the input domain.
   */
  template <class T>
  uint64_t get_tile_pos(const T* domain, const T* tile_coords) const;

  /**
   * Gets the tile subarray for the input tile coordinates.
   *
   * @tparam T The coordinates type.
   * @param tile_coords The input tile coordinates.
   * @param tile_subarray The output tile subarray.
   * @return void.
   */
  template <class T>
  void get_tile_subarray(const T* tile_coords, T* tile_subarray) const;

  /**
   * Gets the tile subarray for the input tile coordinates. The tile
   * coordinates are with respect to the input `domain`.
   *
   * @tparam T The coordinates type.
   * @param domain The domain `tile_coords` are in reference to.
   * @param tile_coords The input tile coordinates.
   * @param tile_subarray The output tile subarray.
   * @return void.
   */
  template <class T>
  void get_tile_subarray(
      const T* domain, const T* tile_coords, T* tile_subarray) const;

  /**
   * Checks if the domain has a dimension of the given name.
   *
   * @param name Name of dimension to check for
   * @return true if the domain has a dimension of the given name, false
   * otherwise
   */
  bool has_dimension(const std::string& name) const;

  /**
   * Gets the index in the domain of a given dimension name
   *
   * @param name Name of dimension to check for
   * @return Dimension index
   */
  unsigned get_dimension_index(const std::string& name) const;

  /** Returns true if at least one dimension has null tile extent. */
  bool null_tile_extents() const;

  /**
   * Serializes the object members into a binary buffer.
   *
   * @param serializer The object the array schema is serialized into.
   * @param version The array schema version.
   */
  void serialize(Serializer& serializer, uint32_t version) const;

  /**
   * For every dimension that has a null tile extent, it sets
   * the tile extent to that dimension domain range.
   */
  void set_null_tile_extents_to_range();

  /**
   * Based on the input subarray layout and the domain's cell layout
   * and tile extents, this returns the number of cells that each
   * pair of adjacent cells in a result cell slab (produced during the
   * read algorithm for that subarray) are apart. If it is
   * `UINT64_MAX`, then the cells in the result cell slabs are contiguous.
   */
  template <class T>
  uint64_t stride(Layout subarray_layout) const;

  /** Returns the tile extent along the i-th dimension. */
  const ByteVecValue& tile_extent(unsigned i) const;

  /** Returns the tile extents. */
  std::vector<ByteVecValue> tile_extents() const;

  /**
   * Returns the number of tiles intersecting the input ND range.
   * Returns 0 if even a single dimension has non-integral type.
   */
  uint64_t tile_num(const NDRange& ndrange) const;

  /**
   * Returns the number of cells in the input range.
   * If there is an overflow, then the function returns MAX_UINT64.
   * If at least one dimension had a non-integer domain, the
   * function returns MAX_UINT64.
   */
  uint64_t cell_num(const NDRange& ndrange) const;

  /** Returns true if r1 is fully covered by r2. */
  bool covered(const NDRange& r1, const NDRange& r2) const;

  /** Returns true if the two ND ranges overlap. */
  bool overlap(const NDRange& r1, const NDRange& r2) const;

  /**
   * Return ratio of the overalp of the two input ND ranges over
   * the volume of `r2`.
   */
  double overlap_ratio(
      const NDRange& r1,
      const std::vector<bool>& r1_default,
      const NDRange& r2) const;

  /**
   * Checks the tile order of the input coordinates.
   *
   * @param left
   * @param right
   * @return One of the following:
   *    - -1 if the first coordinates precede the second on the tile order
   *    -  0 if the two coordinates have the same tile order
   *    - +1 if the first coordinates succeed the second on the tile order
   */
  int tile_order_cmp(
      const tiledb::type::DomainDataRef& left,
      const tiledb::type::DomainDataRef& right) const;

  /**
   * Checks the tile order of the input coordinates for a given dimension.
   *
   * @param dim_idx The index of the dimension to focus on.
   * @param coord_a The first coordinate on the given dimension.
   * @param coord_b The second coordinate on the given dimension.
   * @return One of the following:
   *    - -1 if the first coordinate precedes the second on the tile order
   *    -  0 if the two coordinates have the same tile order
   *    - +1 if the first coordinate succeeds the second on the tile order
   */
  int tile_order_cmp(
      unsigned dim_idx, const void* coord_a, const void* coord_b) const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The memory tracker for this Domain. */
  shared_ptr<MemoryTracker> memory_tracker_;

  /** The number of cells per tile. Meaningful only for the **dense** case. */
  std::optional<uint64_t> cell_num_per_tile_;

  /** The cell order of the array the domain belongs to. */
  Layout cell_order_;

  /**
   * Allocation for the dimensions of this domain.
   *
   * This array keeps the memory resource for the dimensions. This is its only
   * responsibility, and as such only appears in initialization code. Pointers
   * to the stored `Dimension` objects are mirrored in `dimension_ptrs_`.
   *
   * @invariant All pointers in the vector are non-null.
   */
  tdb::pmr::vector<shared_ptr<Dimension>> dimensions_;

  /**
   * Non-allocating mirror of the dimensions vector.
   *
   * This vector holds pointers to the dimensions for ordinary, efficient use.
   * The `shared_ptr` of `dimensions_` is only used for resource management, so
   * this variable mirrors just the pointers, avoiding all overhead of using
   * `shared_ptr` ordinarily.
   *
   * @invariant All pointers in the vector are non-null.
   */
  tdb::pmr::vector<const Dimension*> dimension_ptrs_;

  /** The number of dimensions. */
  unsigned dim_num_;

  /** The tile order of the array the domain belongs to. */
  Layout tile_order_;

  /**
   * Vector of functions, one per dimension, for comparing the cell order of
   * two coordinates. The inputs to the function are:
   *
   * - dim: The dimension to check the coordinates on.
   * - buff: The buffer that stores all coorinates;
   * - a, b: The positions of the two coordinates in the buffer to compare.
   */
  tdb::pmr::vector<int (*)(
      const Dimension* dim, const UntypedDatumView a, const UntypedDatumView b)>
      cell_order_cmp_func_;

  /**
   * Vector of functions, one per dimension, for comparing the cell order of
   * two coordinates. The inputs to the function are:
   *
   * - coord_a, coord_b: The two coordinates to compare.
   */
  tdb::pmr::vector<int (*)(const void* coord_a, const void* coord_b)>
      cell_order_cmp_func_2_;

  /**
   * Vector of functions, one per dimension, for comparing the tile order of
   * two coordinates. The inputs to the function are:
   *
   * - dim: The dimension to compare on.
   * - coord_a, coord_b: The two coordinates to compare.
   */
  tdb::pmr::vector<int (*)(
      const Dimension* dim, const void* coord_a, const void* coord_b)>
      tile_order_cmp_func_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /**
   * Checks the cell order of the input coordinates. Since the coordinates
   * are given for a single dimension, this function simply checks which
   * coordinate is larger.
   *
   * @param dim The dimension to check the coordinates on.
   * @param a The position of the first coordinate in the buffer to check.
   * @param b The position of the second coordinate in the buffer to check.
   * @return One of the following:
   *    - -1 if the first coordinate is smaller than the second
   *    -  0 if the two coordinates have the same cell order
   *    - +1 if the first coordinate is larger than the second
   */
  template <class T>
  static int cell_order_cmp_impl(
      const Dimension* dim, UntypedDatumView a, UntypedDatumView b);

  /**
   * Checks the tile order of the input coordinates on the given dimension.
   *
   * @param dim The dimension to compare on.
   * @param coord_a The first coordinate.
   * @param coord_b The second coordinate.
   * @return One of the following:
   *    - -1 if the first coordinate precedes the second on the tile order
   *    -  0 if the two coordinates have the same tile order
   *    - +1 if the first coordinate succeeds the second on the tile order
   */
  template <class T>
  static int tile_order_cmp_impl(
      const Dimension* dim, const void* coord_a, const void* coord_b);

  /** Compute the number of cells per tile. */
  std::optional<uint64_t> compute_cell_num_per_tile() const;

  /**
   * Compute the number of cells per tile.
   *
   * @tparam T The coordinates type.
   * @return void
   */
  template <class T>
  std::optional<uint64_t> compute_cell_num_per_tile() const;

  /**
   * Computes and updates the number of cells per tile.
   */
  void update_cell_num_per_tile();

  /** Prepares the comparator functions for each dimension. */
  void set_tile_cell_order_cmp_funcs();

  /**
   * Retrieves the next tile coordinates along the array tile order within a
   * given tile domain. Applicable only to **dense** arrays, and focusing on
   * the **column-major** tile order.
   *
   * @tparam T The coordinates type.
   * @param domain The targeted domain.
   * @param tile_coords The input tile coordinates, which the function modifies
   *     to store the next tile coordinates at termination.
   * @return void
   */
  template <class T>
  void get_next_tile_coords_col(const T* domain, T* tile_coords) const;

  /**
   * Retrieves the next tile coordinates along the array tile order within a
   * given tile domain. Applicable only to **dense** arrays, and focusing on
   * the **column-major** tile order.
   *
   * @tparam T The coordinates type.
   * @param domain The targeted domain.
   * @param tile_coords The input tile coordinates, which the function modifies
   *     to store the next tile coordinates at termination.
   * @param in Set to `true` if the retrieved coords are inside the domain.
   * @return void
   */
  template <class T>
  void get_next_tile_coords_col(
      const T* domain, T* tile_coords, bool* in) const;

  /**
   * Retrieves the next tile coordinates along the array tile order within a
   * given tile domain. Applicable only to **dense** arrays, and focusing on
   * the **row-major** tile order.
   *
   * @tparam T The coordinates type.
   * @param domain The targeted domain.
   * @param tile_coords The input tile coordinates, which the function modifies
   *     to store the next tile coordinates at termination.
   * @return void
   */
  template <class T>
  void get_next_tile_coords_row(const T* domain, T* tile_coords) const;

  /**
   * Retrieves the next tile coordinates along the array tile order within a
   * given tile domain. Applicable only to **dense** arrays, and focusing on
   * the **row-major** tile order.
   *
   * @tparam T The coordinates type.
   * @param domain The targeted domain.
   * @param tile_coords The input tile coordinates, which the function modifies
   *     to store the next tile coordinates at termination.
   * @param in Set to `true` if the retrieved coords are inside the domain.
   * @return void
   */
  template <class T>
  void get_next_tile_coords_row(
      const T* domain, T* tile_coords, bool* in) const;

  /**
   * Returns the tile position along the array tile order within the input
   * domain. Applicable only to **dense** arrays, and focusing on the
   * **column-major** tile order.
   *
   * @tparam T The domain type.
   * @param domain The input domain, which is a cell domain partitioned into
   *     regular tiles in the same manner as that of the array domain
   *     (however *domain* may be a sub-domain of the array domain).
   * @param tile_coords The tile coordinates.
   * @return The tile position of *tile_coords* along the tile order of the
   *     array inside the input domain.
   */
  template <class T>
  uint64_t get_tile_pos_col(const T* domain, const T* tile_coords) const;

  /**
   * Returns the tile position along the array tile order within the input
   * domain. Applicable only to **dense** arrays, and focusing on the
   * **row-major** tile order.
   *
   * @tparam T The domain type.
   * @param domain The input domain, which is a cell domain partitioned into
   *     regular tiles in the same manner as that of the array domain (however
   *     *domain* may be a sub-domain of the array domain).
   * @param tile_coords The tile coordinates.
   * @return The tile position of *tile_coords* along the tile order of the
   *     array inside the input domain.
   */
  template <class T>
  uint64_t get_tile_pos_row(const T* domain, const T* tile_coords) const;
};

}  // namespace tiledb::sm

/** Converts the filter into a string representation. */
std::ostream& operator<<(std::ostream& os, const tiledb::sm::Domain& domain);

#endif  // TILEDB_DOMAIN_H
