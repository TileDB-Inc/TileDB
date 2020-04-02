/**
 * @file   domain.h
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
 * This file defines class Domain.
 */

#ifndef TILEDB_DOMAIN_H
#define TILEDB_DOMAIN_H

#include "tiledb/sm/misc/status.h"
#include "tiledb/sm/misc/types.h"

#include <vector>

namespace tiledb {
namespace sm {

class Buffer;
class ConstBuffer;
class Dimension;

enum class Datatype : uint8_t;
enum class Layout : uint8_t;

/** Defines an array domain, which consists of dimensions. */
class Domain {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Empty constructor. */
  Domain();

  /**
   * Constructor that clones the input domain.
   *
   * @param domain The object to clone.
   */
  explicit Domain(const Domain* domain);

  /** Destructor. */
  ~Domain();

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /**
   * Adds a dimension to the domain.
   *
   * @param dim The dimension to be added.
   * @return Status
   */
  Status add_dimension(const Dimension* dim);

  /** Returns true if all dimensions have fixed-sized domain datatypes. */
  bool all_dims_fixed() const;

  /** Returns true if all dimensions have integer domain types. */
  bool all_dims_int() const;

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
   * @tparam T The coordinates type.
   * @param dim_idx The dimension to compare the coordinates on.
   * @param coord_a The first input coordinates.
   * @param coord_b The second input coordinates.
   * @return One of the following:
   *    - -1 if the first coordinate precedes the second
   *    -  0 if the two coordinates are identical
   *    - +1 if the first coordinate succeeds the second
   */
  int cell_order_cmp(
      unsigned dim_idx, const void* coord_a, const void* coord_b) const;

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
  static int cell_order_cmp(const void* coord_a, const void* coord_b);

  /**
   * Checks the cell order of the input coordinates.
   *
   * @param coord_buffs The input coordinates, given n separate buffers,
   *     one per dimension. The buffers are sorted in the same order of the
   *     dimensions as defined in the array schema.
   * @param a The position of the first coordinate tuple across all buffers.
   * @param b The position of the second coordinate tuple across all buffers.
   * @return One of the following:
   *    - -1 if the first coordinates precede the second on the cell order
   *    -  0 if the two coordinates have the same cell order
   *    - +1 if the first coordinates succeed the second on the cell order
   */
  int cell_order_cmp(
      const std::vector<const void*>& coord_buffs,
      uint64_t a,
      uint64_t b) const;

  /**
   * Populates the object members from the data in the input binary buffer.
   *
   * @param buff The buffer to deserialize from.
   * @param version The array schema version.
   * @return Status
   */
  Status deserialize(ConstBuffer* buff, uint32_t version);

  /** Returns the cell order. */
  Layout cell_order() const;

  /**
   * Crops the input ND range such that it does not exceed the array domain.
   */
  void crop_ndrange(NDRange* ndrange) const;

  /** Returns the tile order. */
  Layout tile_order() const;

  /** Returns the number of dimensions. */
  unsigned int dim_num() const;

  /** Returns the domain along the i-th dimension. */
  const Range& domain(unsigned i) const;

  /** Returns the domain as a N-dimensional range object. */
  NDRange domain() const;

  /** Returns the i-th dimensions (nullptr upon error). */
  const Dimension* dimension(unsigned int i) const;

  /** Returns the dimension given a name (nullptr upon error). */
  const Dimension* dimension(const std::string& name) const;

  /** Dumps the domain in ASCII format in the selected output. */
  void dump(FILE* out) const;

  /** Expands ND range `r2` using ND range `r1`. */
  void expand_ndrange(const NDRange& r1, NDRange* r2) const;

  /**
   * Expands the input domain such that it coincides with the boundaries of
   * the array's regular tiles (i.e., it maps it on the regular tile grid).
   * If the array has no regular tile grid or real domain, the function
   * does not do anything.
   */
  void expand_to_tiles(NDRange* ndrange) const;

  /**
   * Returns the position of the input coordinates inside its corresponding
   * tile, based on the array cell order. Applicable only to **dense** arrays.
   *
   * @tparam T The coordinates type.
   * @param coords The input coordindates, which are expressed as global
   *     coordinates in the array domain.
   * @pos The position of the cell coordinates in the array cell order
   *     within its corresponding tile.
   * @return Status
   *
   */
  template <class T>
  Status get_cell_pos(const T* coords, uint64_t* pos) const;

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
   * Retrieves the end of a cell slab starting at the `start` input
   * coordinates. The cell slab is determined based on the domain
   * tile/cell order and the input query `layout`. Essentially a
   * cell slab is a contiguous (in the global cell order) range of
   * cells that follow the query layout.
   *
   * @tparam T The domain type.
   * @param subarray The subarray in which the end of the cell slab must
   *     be contained.
   * @param start The start coordinates.
   * @param layout The query layout.
   * @param end The cell slab end coordinates to be retrieved.
   */
  template <class T>
  void get_end_of_cell_slab(T* subarray, T* start, Layout layout, T* end) const;

  /**
   * Retrieves the next coordinates along the array cell order within a given
   * domain (desregarding whether the domain is split into tiles or not).
   * Applicable only to **dense** arrays, and focusing on **column-major**
   * cell order.
   *
   * @tparam T The coordinates type.
   * @param domain The targeted domain.
   * @param cell_coords The input cell coordinates, which the function modifies
   *     to store the next coordinates at termination.
   * @param coords_retrieved Will store true if the retrieved coordinates are
   *     inside the domain, and false otherwise.
   * @return void
   */
  template <class T>
  void get_next_cell_coords_col(
      const T* domain, T* cell_coords, bool* coords_retrieved) const;

  /**
   * Retrieves the next coordinates along the array cell order within a given
   * domain (desregarding whether the domain is split into tiles or not).
   * Applicable only to **dense** arrays, and focusing on **row-major**
   * cell order.
   *
   * @tparam T The coordinates type.
   * @param domain The targeted domain.
   * @param cell_coords The input cell coordinates, which the function modifies
   *     to store the next coordinates at termination.
   * @param coords_retrieved Will store true if the retrieved coordinates are
   *     inside the domain, and false otherwise.
   * @return void
   */
  template <class T>
  void get_next_cell_coords_row(
      const T* domain, T* cell_coords, bool* coords_retrieved) const;

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
   * @param tile_coords The tile coordinates.
   * @return The tile position of *tile_coords* along the tile order of the
   *     array inside the array domain.
   */
  template <class T>
  uint64_t get_tile_pos(const T* tile_coords) const;

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
   * @param has_dim Set to true if the domain has a dimension of the given name.
   * @return Status
   */
  Status has_dimension(const std::string& name, bool* has_dim) const;

  /**
   * Initializes the domain.
   *
   * @param cell_order The cell order of the array the domain belongs to.
   * @param tile_order The cell order of the array the domain belongs to.
   * @return Status
   */
  Status init(Layout cell_order, Layout tile_order);

  /** Returns true if at least one dimension has null tile extent. */
  bool null_tile_extents() const;

  /**
   * Serializes the object members into a binary buffer.
   *
   * @param buff The buffer to serialize the data into.
   * @param version The array schema version.
   * @return Status
   */
  Status serialize(Buffer* buff, uint32_t version);

  /**
   * For every dimension that has a null tile extent, it sets
   * the tile extent to that dimension domain range.
   */
  Status set_null_tile_extents_to_range();

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
   * Returns the number of tiles contained in the input ND range.
   * Returns 0 if even a single dimension has non-integral type.
   */
  uint64_t tile_num(const NDRange& ndrange) const;

  /**
   * Returns the number of cells in the input range.
   * If there is an overflow, then the function returns MAX_UINT64.
   * If at least one dimension had a non-integer domain, the
   * functuon returns MAX_UINT64.
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
  double overlap_ratio(const NDRange& r1, const NDRange& r2) const;

  /**
   * Checks the tile order of the input coordinates on the given dimension.
   *
   * @param The dimension to compare on.
   * @param coord_a The first coordinate.
   * @param coord_b The second coordinate.
   * @return One of the following:
   *    - -1 if the first coordinate precedes the second on the tile order
   *    -  0 if the two coordinates have the same tile order
   *    - +1 if the first coordinate succeeds the second on the tile order
   */
  template <class T>
  static int tile_order_cmp(
      const Dimension* dim, const void* coord_a, const void* coord_b);

  /**
   * Checks the tile order of the input coordinates.
   *
   * @param coord_buffs The input coordinates, given n separate buffers,
   *     one per dimension. The buffers are sorted in the same order of the
   *     dimensions as defined in the array schema.
   * @param a The position of the first coordinate tuple across all buffers.
   * @param b The position of the second coordinate tuple across all buffers.
   * @return One of the following:
   *    - -1 if the first coordinates precede the second on the tile order
   *    -  0 if the two coordinates have the same tile order
   *    - +1 if the first coordinates succeed the second on the tile order
   */
  int tile_order_cmp(
      const std::vector<const void*>& coord_buffs,
      uint64_t a,
      uint64_t b) const;

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

  /**
   * Returns the position of the input coordinates inside its corresponding
   * tile, based on the array cell order. Applicable only to **dense** arrays,
   * and focusing on the **column-major** cell order.
   *
   * @tparam T The coordinates type.
   * @param coords The input coordindates, which are expressed as global
   *     coordinates in the array domain.
   * @return The position of the cell coordinates in the array cell order
   *     within its corresponding tile.
   */
  template <class T>
  uint64_t get_cell_pos_col(const T* coords) const;

  /**
   * Returns the position of the input coordinates inside the input subarray.
   * Applicable only to **dense** arrays,
   * and focusing on the **column-major** cell order.
   *
   * @tparam T The coordinates type.
   * @param subarray The input subarray, expressed in global coordinates.
   * @param coords The input coordindates, expressed in global coordinates.
   * @return The position of the cell coordinates in the subarray.
   */
  template <class T>
  uint64_t get_cell_pos_col(const T* subarray, const T* coords) const;

  /**
   * Returns the position of the input coordinates inside its corresponding
   * tile, based on the array cell order. Applicable only to **dense** arrays,
   * and focusing on the **row-major** cell order.
   *
   * @tparam T The coordinates type.
   * @param coords The input coordindates, which are expressed as global
   *     coordinates in the array domain.
   * @return The position of the cell coordinates in the array cell order
   *     within its corresponding tile.
   */
  template <class T>
  uint64_t get_cell_pos_row(const T* coords) const;

  /**
   * Returns the position of the input coordinates inside the input subarray.
   * Applicable only to **dense** arrays,
   * and focusing on the **row-major** cell order.
   *
   * @tparam T The coordinates type.
   * @param subarray The input subarray, expressed in global coordinates.
   * @param coords The input coordindates, expressed in global coordinates.
   * @return The position of the cell coordinates in the subarray.
   */
  template <class T>
  uint64_t get_cell_pos_row(const T* subarray, const T* coords) const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The number of cells per tile. Meaningful only for the **dense** case. */
  uint64_t cell_num_per_tile_;

  /** The cell order of the array the domain belongs to. */
  Layout cell_order_;

  /** The domain dimensions. */
  std::vector<Dimension*> dimensions_;

  /** The number of dimensions. */
  unsigned dim_num_;

  /**
   * Offsets for calculating tile positions and ids for the column-major
   * tile order.
   */
  std::vector<uint64_t> tile_offsets_col_;

  /**
   * Offsets for calculating tile positions and ids for the row-major
   * tile order.
   */
  std::vector<uint64_t> tile_offsets_row_;

  /** The tile order of the array the domain belongs to. */
  Layout tile_order_;

  /**
   * Vector of functions, one per dimension, for comparing the cell order of
   * two coordinates. The inputs to the function are:
   *
   * - coord_a, coord_b: The two coordinates to compare.
   */
  std::vector<int (*)(const void* coord_a, const void* coord_b)>
      cell_order_cmp_func_;

  /**
   * Vector of functions, one per dimension, for comparing the tile order of
   * two coordinates. The inputs to the function are:
   *
   * - dim: The dimension to compare on.
   * - coord_a, coord_b: The two coordinates to compare.
   */
  std::vector<int (*)(
      const Dimension* dim, const void* coord_a, const void* coord_b)>
      tile_order_cmp_func_;

  /* ********************************* */
  /*           PRIVATE METHODS         */
  /* ********************************* */

  /** Compute the number of cells per tile. */
  void compute_cell_num_per_tile();

  /**
   * Compute the number of cells per tile.
   *
   * @tparam T The coordinates type.
   * @return void
   */
  template <class T>
  void compute_cell_num_per_tile();

  /** Prepares the comparator functions for each dimension. */
  void set_tile_cell_order_cmp_funcs();

  /** Computes tile offsets neccessary when computing tile positions and ids. */
  void compute_tile_offsets();

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
   * @param tile_coords The tile coordinates.
   * @return The tile position of *tile_coords* along the tile order of the
   *     array inside the array domain.
   */
  template <class T>
  uint64_t get_tile_pos_col(const T* tile_coords) const;

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
   * @param tile_coords The tile coordinates.
   * @return The tile position of *tile_coords* along the tile order of the
   *     array inside the array domain.
   */
  template <class T>
  uint64_t get_tile_pos_row(const T* tile_coords) const;

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

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_DOMAIN_H
