/**
 * @file   domain.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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

#include "buffer.h"
#include "datatype.h"
#include "dimension.h"
#include "layout.h"
#include "status.h"

#include <vector>

namespace tiledb {

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
   * Adds a dimension to the domain.
   *
   * @param dim The dimension to be added.
   * @return Status
   */
  Status add_dimension(Dimension* dim);

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
   * Retrieves the next coordinates along the array cell order within a given
   * domain (desregarding whether the domain is split into tiles or not).
   * Applicable only to **dense** arrays.
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
  void get_next_cell_coords(
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
   * Retrieves the previous coordinates along the array cell order within a
   * given domain (desregarding whether the domain is split into tiles or not).
   * Applicable only to **dense** arrays.
   *
   * @tparam T The coordinates type.
   * @param domain The targeted domain.
   * @param cell_coords The input cell coordinates, which the function modifies
   *     to store the previous coordinates at termination.
   * @return void
   */
  template <class T>
  void get_previous_cell_coords(const T* domain, T* cell_coords) const;

  /**
   * Gets a subarray of tile coordinates for the input (cell) subarray
   * over the input array domain. Retrieves also the tile domain of
   * the array..
   *
   * @tparam T The domain type.
   * @param subarray The input (cell) subarray.
   * @param tile_domain The array tile domain to be retrieved.
   * @param subarray_in_tile_domain The output (tile) subarray.
   * @return void
   */
  template <class T>
  void get_subarray_tile_domain(
      const T* subarray, T* tile_domain, T* subarray_in_tile_domain) const;

  /**
   * Returns the tile position along the array tile order within the input
   * domain. Applicable only to **dense** arrays.
   *
   * @tparam T The domain type.
   * @param tile_coords The tile coordinates.
   * @return The tile position of *tile_coords* along the tile order of the
   *     array inside the array domain, or TILEDB_AS_ERR on error.
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
   * @param tile_coords The tile coordinates.
   * @return The tile position of *tile_coords* along the tile order of the
   *     array inside the input domain, or TILEDB_AS_ERR on error.
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
   * Initializes the domain.
   *
   * @param cell_order The cell order of the array the domain belongs to.
   * @param tile_order The cell order of the array the domain belongs to.
   * @return Status
   */
  Status init(Layout cell_order, Layout tile_order);

  /**
   * Returns true if the input range is contained fully in a single
   * column of tiles.
   */
  bool is_contained_in_tile_slab_col(const void* range) const;

  /**
   * Returns true if the input range is contained fully in a single
   * column of tiles.
   *
   * @tparam T The coordinates type.
   * @param range The input range.
   * @return True if the input range is contained fully in a single
   *     column of tiles.
   */
  template <class T>
  bool is_contained_in_tile_slab_col(const T* range) const;

  /**
   * Returns true if the input range is contained fully in a single
   * row of tiles.
   */
  bool is_contained_in_tile_slab_row(const void* range) const;

  /**
   * Returns true if the input range is contained fully in a single
   * row of tiles.
   *
   * @tparam T The coordinates type.
   * @param range The input range.
   * @return True if the input range is contained fully in a single
   *     row of tiles.
   */
  template <class T>
  bool is_contained_in_tile_slab_row(const T* range) const;

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
   * Returns the type of overlap of the input subarrays.
   *
   * @tparam T The types of the subarrays.
   * @param subarray_a The first input subarray.
   * @param subarray_b The second input subarray.
   * @param overlap_subarray The overlap area between *subarray_a* and
   *     *subarray_b*.
   * @return The type of overlap, which can be one of the following:
   *    - 0: No overlap
   *    - 1: *subarray_a* fully covers *subarray_b*
   *    - 2: Partial overlap (non-contig)
   *    - 3: Partial overlap (contig)
   */
  template <class T>
  unsigned int subarray_overlap(
      const T* subarray_a, const T* subarray_b, T* overlap_subarray) const;

  /**
   * Checks the order of the input coordinates. First the tile order is checked
   * (which, in case of non-regular tiles, is always the same), breaking the
   * tie by checking the cell order.
   *
   * @tparam T The coordinates type.
   * @param coords_a The first input coordinates.
   * @param coords_b The second input coordinates.
   * @param tile_coords This is an auxiliary input that helps in calculating the
   *     tile id. It is strongly advised that the caller initializes this
   *     parameter once and calls the function many times with the same
   *     auxiliary input to improve performance.
   * @return One of the following:
   *    - -1 if the first coordinates precede the second
   *    -  0 if the two coordinates are identical
   *    - +1 if the first coordinates succeed the second
   */
  template <class T>
  int tile_cell_order_cmp(
      const T* coords_a, const T* coords_b, T* tile_coords) const;

  /** Returns the tile extents. */
  const void* tile_extents() const;

  /** returns the tile extent along the i-th dimension (nullptr upon error). */
  const void* tile_extent(unsigned int i) const;

  /**
   * Returns the id of the tile the input coordinates fall into.
   *
   * @tparam T The coordinates type.
   * @param cell_coords The input coordinates.
   * @param tile_coords This is an auxiliary input that helps in calculating the
   *     tile id. It is strongly advised that the caller initializes this
   *     parameter once and calls the function many times with the same
   *     auxiliary input to improve performance.
   * @return The computed tile id.
   */
  template <class T>
  uint64_t tile_id(const T* cell_coords, T* tile_coords) const;

  /**
   * Returns the number of tiles in the array domain (applicable only to dense
   * arrays).
   */
  uint64_t tile_num() const;

  /**
   * Returns the number of tiles in the array domain (applicable only to dense
   * arrays).
   *
   * @tparam T The coordinates type.
   * @return The number of tiles.
   */
  template <class T>
  uint64_t tile_num() const;

  /**
   * Returns the number of tiles overlapping with the input range
   * (applicable only to dense arrays).
   */
  uint64_t tile_num(const void* range) const;

  /**
   * Returns the number of tiles in the input domain (applicable only to dense
   * arrays).
   *
   * @tparam T The coordinates type.
   * @param domain The input domain.
   * @return The number of tiles.
   */
  template <class T>
  uint64_t tile_num(const T* domain) const;

  /**
   * Checks the tile order of the input coordinates.
   *
   * @tparam T The coordinates type.
   * @param coords_a The first input coordinates.
   * @param coords_b The second input coordinates.
   * @param tile_coords This is an auxiliary input that helps in calculating the
   *     tile id. It is strongly advised that the caller initializes this
   *     parameter once and calls the function many times with the same
   *     auxiliary input to improve performance.
   * @return One of the following:
   *    - -1 if the first coordinates precede the second on the tile order
   *    -  0 if the two coordinates have the same tile order
   *    - +1 if the first coordinates succeed the second on the tile order
   */
  template <class T>
  int tile_order_cmp(
      const T* coords_a, const T* coords_b, T* tile_coords) const;

  /** Return the number of cells in a column tile slab of an input subarray. */
  uint64_t tile_slab_col_cell_num(const void* subarray) const;

  /** Return the number of cells in a row tile slab of an input subarray. */
  uint64_t tile_slab_row_cell_num(const void* subarray) const;

  /** Returns the dimensions type. */
  Datatype type() const;

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
   * The array domain, represented by serializes the dimensions domains.
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
   * Retrieves the previous coordinates along the array cell order within a
   * given domain (desregarding whether the domain is split into tiles or not).
   * Applicable only to **dense** arrays, and focusing on the **column-major**
   * cell order.
   *
   * @tparam T The coordinates type.
   * @param domain The targeted domain.
   * @param cell_coords The input cell coordinates, which the function modifies
   *     to store the previous coordinates at termination.
   * @return void
   */
  template <class T>
  void get_previous_cell_coords_col(const T* domain, T* cell_coords) const;

  /**
   * Retrieves the previous coordinates along the array cell order within a
   * given domain (desregarding whether the domain is split into tiles or not).
   * Applicable only to **dense** arrays, and focusing on the **row-major**
   * cell order.
   *
   * @tparam T The coordinates type.
   * @param domain The targeted domain.
   * @param cell_coords The input cell coordinates, which the function modifies
   *     to store the previous coordinates at termination.
   * @return void
   */
  template <class T>
  void get_previous_cell_coords_row(const T* domain, T* cell_coords) const;

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

  /** Return the number of cells in a column tile slab of an input subarray. */
  template <class T>
  uint64_t tile_slab_col_cell_num(const T* subarray) const;

  /** Return the number of cells in a row tile slab of an input subarray. */
  template <class T>
  uint64_t tile_slab_row_cell_num(const T* subarray) const;
};

}  // namespace tiledb

#endif  // TILEDB_DOMAIN_H
