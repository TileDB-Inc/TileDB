/**
 * @file   domain.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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

#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/enums/array_type.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/misc/status.h"

#include <iostream>
#include <vector>

namespace tiledb {
namespace sm {

/** Defines an array domain, which consists of dimensions. */
class Domain {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Empty constructor. */
  Domain();

  /**
   * Constructor.
   *
   * @param type The type of dimensions.
   */
  explicit Domain(Datatype type);

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
   * Splits the input subarray in half, in a way that the input layout is
   * respected. This means that if the two resulting subarrays were to
   * be issued as consecutive queries with the input layout, the retrieved
   * result would be correct (i.e., the resulting cells would respect the
   * input layout).
   *
   * @param subarray The input subarray.
   * @param layout The query layout.
   * @param subarray_1 The first subarray resulting from the split.
   * @param subarray_2 The second subarray resulting from the split.
   * @return Status
   */
  Status split_subarray(
      void* subarray,
      Layout layout,
      void** subarray_1,
      void** subarray_2) const;

  /**
   * Splits the input subarray in half, in a way that the input layout is
   * respected. This means that if the two resulting subarrays were to
   * be issued as consecutive queries with the input layout, the retrieved
   * result would be correct (i.e., the resulting cells would respect the
   * input layout).
   *
   * @tparam T The domain type.
   * @param subarray The input subarray.
   * @param layout The query layout.
   * @param subarray_1 The first subarray resulting from the split.
   * @param subarray_2 The second subarray resulting from the split.
   * @return Status
   */
  template <class T>
  Status split_subarray(
      void* subarray,
      Layout layout,
      void** subarray_1,
      void** subarray_2) const;

  /**
   * Splits the input subarray in half, in a way that the global layout is
   * respected. This means that if the two resulting subarrays were to
   * be issued as consecutive queries with the input layout, the retrieved
   * result would be correct (i.e., the resulting cells would respect the
   * global layout).
   *
   * @tparam T The domain type.
   * @param subarray The input subarray.
   * @param layout The query layout.
   * @param subarray_1 The first subarray resulting from the split.
   * @param subarray_2 The second subarray resulting from the split.
   * @return Status
   */
  template <class T>
  Status split_subarray_global(
      void* subarray, void** subarray_1, void** subarray_2) const;

  /**
   * Splits the input subarray in half, in a way that the input cell layout is
   * respected. This means that if the two resulting subarrays were to
   * be issued as consecutive queries with the cell layout, the retrieved
   * result would be correct (i.e., the resulting cells would respect the
   * input layout).
   *
   * @tparam T The domain type.
   * @param subarray The input subarray.
   * @param cell_layout The cell layout.
   * @param subarray_1 The first subarray resulting from the split.
   * @param subarray_2 The second subarray resulting from the split.
   * @return Status
   */
  template <class T>
  Status split_subarray_cell(
      void* subarray,
      Layout cell_layout,
      void** subarray_1,
      void** subarray_2) const;

  /**
   * Adds a dimension to the domain.
   *
   * @param dim The dimension to be added.
   * @return Status
   */
  Status add_dimension(Dimension* dim);

  /**
   * Returns the number of cells in the input domain. Note that this is
   * applicable only to integer array domains (otherwise the output is 0).
   * Also note that it is assummed that the input domain is expanded
   * such that it aligns with the tile extents.
   *
   * @param domain The domain to be checked.
   * @return The number of cells in the domain.
   *
   * @note The function returns 0 in case `domain` is huge, leading to more
   *      cells than `uint64_t` can hold.
   */
  uint64_t cell_num(const void* domain) const;

  /**
   * Returns the number of cells in the input domain. Note that this is
   * applicable only to integer array domains (otherwise the output is 0).
   * Also note that it is assummed that the input domain is expanded
   * such that it aligns with the tile extents.
   *
   * @tparam T The domain type.
   * @param domain The domain to be checked.
   * @return The number of cells in the domain.
   */
  template <class T>
  uint64_t cell_num(const T* domain) const;

  /** Returns the number of cells per tile (only for the dense case). */
  uint64_t cell_num_per_tile() const;

  /**
   * Checks the cell order of the input coordinates. Note that, in the presence
   * of a regular tile grid, this function assumes that the cells are in the
   * same regular tile.
   *
   * @tparam T The coordinates type.
   * @param coords_a The first input coordinates.
   * @param coords_b The second input coordinates.
   * @return One of the following:
   *    - -1 if the first coordinates precede the second
   *    -  0 if the two coordinates are identical
   *    - +1 if the first coordinates succeed the second
   */
  template <class T>
  int cell_order_cmp(const T* coords_a, const T* coords_b) const;

  /**
   * Populates the object members from the data in the input binary buffer.
   *
   * @param buff The buffer to deserialize from.
   * @return Status
   */
  Status deserialize(ConstBuffer* buff);

  /** Returns the cell order. */
  Layout cell_order() const;

  /** Returns the tile order. */
  Layout tile_order() const;

  /** Returns the number of dimensions. */
  unsigned int dim_num() const;

  /** Returns the domain (serialized dimension domains). */
  const void* domain() const;

  /** returns the domain along the i-th dimension (nullptr upon error). */
  const void* domain(unsigned int i) const;

  /** Returns the i-th dimensions (nullptr upon error). */
  const Dimension* dimension(unsigned int i) const;

  /** Returns the dimension given a name (nullptr upon error). */
  const Dimension* dimension(std::string name) const;

  /** Dumps the domain in ASCII format in the selected output. */
  void dump(FILE* out) const;

  /**
   * Expands the input domain such that it coincides with the boundaries of
   * the array's regular tiles (i.e., it maps it on the regular tile grid).
   * If the array has no regular tile grid, the function does not do anything.
   *
   * @param domain The domain to be expanded.
   * @return void
   */
  void expand_domain(void* domain) const;

  /**
   * Expands the input domain such that it coincides with the boundaries of
   * the array's regular tiles (i.e., it maps it on the regular tile grid).
   * If the array has no regular tile grid, the function does not do anything.
   *
   * @tparam The domain type.
   * @param domain The domain to be expanded.
   * @return void
   */
  template <class T>
  void expand_domain(T* domain) const;

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
   * @return Status
   */
  Status serialize(Buffer* buff);

  /**
   * For every dimension that has a null tile extent, it sets
   * the tile extent to that dimension domain range.
   */
  Status set_null_tile_extents_to_range();

  /** Returns the tile extents. */
  const void* tile_extents() const;

  /** returns the tile extent along the i-th dimension (nullptr upon error). */
  const void* tile_extent(unsigned int i) const;

  /**
   * Returns the number of tiles contained in the input range.
   *
   * @note Applicable only to integer domains.
   */
  uint64_t tile_num(const void* range) const;

  /**
   * Returns the number of tiles contained in the input range.
   *
   * @note Applicable only to integer domains.
   */
  template <
      class T,
      class = typename std::enable_if<std::is_integral<T>::value>::type>
  uint64_t tile_num(const T* range) const {
    // For easy reference
    auto tile_extents = static_cast<const T*>(tile_extents_);
    auto domain = static_cast<const T*>(domain_);

    uint64_t ret = 1;
    for (unsigned int i = 0; i < dim_num_; ++i) {
      uint64_t start = (range[2 * i] - domain[2 * i]) / tile_extents[i];
      uint64_t end = (range[2 * i + 1] - domain[2 * i]) / tile_extents[i];
      ret *= (end - start + 1);
    }
    return ret;
  }

  /**
   * Checks the tile order of the input coordinates.
   *
   * @tparam T The coordinates type.
   * @param coords_a The first input coordinates.
   * @param coords_b The second input coordinates.
   * @return One of the following:
   *    - -1 if the first coordinates precede the second on the tile order
   *    -  0 if the two coordinates have the same tile order
   *    - +1 if the first coordinates succeed the second on the tile order
   */
  template <class T>
  int tile_order_cmp(const T* coords_a, const T* coords_b) const;

  /**
   * Checks the tile order of the input tile coordinates.
   *
   * @tparam T The coordinates type.
   * @param coords_a The first tile's coordinates.
   * @param coords_b The second tile's coordinates.
   * @return One of the following:
   *    - -1 if the first coordinates precede the second on the tile order
   *    -  0 if the two coordinates have the same tile order
   *    - +1 if the first coordinates succeed the second on the tile order
   */
  template <class T>
  int tile_order_cmp_tile_coords(
      const T* tile_coords_a, const T* tile_coords_b) const;

  /** Returns the dimensions type. */
  Datatype type() const;

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
  unsigned int dim_num_;

  /**
   * The array domain, represented by serializing the dimension domains.
   * It should contain one [lower, upper] pair per dimension.
   * The type of the values stored in this buffer should match the dimensions
   * type.
   */
  void* domain_;

  /**
   * The array domain. It should contain one [lower, upper] pair per dimension.
   * The type of the values stored in this buffer should match the dimensions
   * type.
   */
  void* tile_domain_;

  /**
   * The tile extents. There should be one value for each dimension. The type
   * of the values stored in this buffer should match the dimensions type. If
   * it is NULL, then it means that the array is sparse.
   */
  void* tile_extents_;

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

  /** The type of dimensions. */
  Datatype type_;

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

  /** Computes the tile domain. */
  void compute_tile_domain();

  /**
   * Computes the tile domain.
   *
   * @tparam T The domain type.
   * @return void
   */
  template <class T>
  void compute_tile_domain();

  /**
   * Computes tile offsets neccessary when computing tile positions and ids.
   *
   * @return void
   */
  void compute_tile_offsets();

  /**
   * Computes tile offsets neccessary when computing tile positions and ids.
   *
   * @tparam T The coordinates type.
   * @return void
   */
  template <class T>
  void compute_tile_offsets();

  /** Returns the default name constructed for the i-th dimension. */
  std::string default_dimension_name(unsigned int i) const;

  /**
   * Floors the value such that it coincides with the largest start of a tile
   * that is smaller than value, on a given dimension. If there are no tile
   * extents, then the returned value is the start of the domain on the input
   * dimension.
   *
   * @tparam T The domain type.
   * @param value The value to be floored.
   * @param dim_idx The targeted dimension.
   * @return The floored value.
   */
  template <class T>
  T floor_to_tile(T value, unsigned dim_idx) const;

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
