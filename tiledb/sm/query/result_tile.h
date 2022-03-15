/**
 * @file   result_tile.h
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
 * This file defines class ResultTile.
 */

#ifndef TILEDB_RESULT_TILE_H
#define TILEDB_RESULT_TILE_H

#include <functional>
#include <iostream>
#include <unordered_map>
#include <vector>

#include "tiledb/common/common.h"
#include "tiledb/sm/array_schema/array_schema.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/misc/constants.h"
#include "tiledb/sm/misc/types.h"
#include "tiledb/sm/subarray/subarray.h"
#include "tiledb/sm/tile/tile.h"

using namespace tiledb::common;
using namespace tiledb::type;

namespace tiledb::type {
class Range;
}

namespace tiledb {
namespace sm {

class Domain;
class FragmentMetadata;
class Subarray;

/**
 * Stores information about a logical dense or sparse result tile. Note that it
 * may store the physical tiles across more than one attributes for the same
 * logical tile (space/data tile for dense, data tile oriented by an MBR for
 * sparse).
 */
class ResultTile {
 public:
  /**
   * For each fixed-sized attributes, the second tile in the tuple is ignored.
   * For var-sized attributes, the first tile is the offsets tile and the second
   * tile is the var-sized values tile. If the attribute is nullable, the third
   * tile contains the validity vector.
   */
  typedef tuple<Tile, Tile, Tile> TileTuple;

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Default constructor. */
  ResultTile() = default;

  /**
   * Constructor. The number of dimensions `dim_num` is used to allocate
   * the separate coordinate tiles.
   */
  ResultTile(unsigned frag_idx, uint64_t tile_idx, const ArraySchema& schema);

  DISABLE_COPY_AND_COPY_ASSIGN(ResultTile);

  /** Default destructor. */
  ~ResultTile() = default;

  /** Move constructor. */
  ResultTile(ResultTile&& tile);

  /** Move-assign operator. */
  ResultTile& operator=(ResultTile&& tile);

  /** Swaps the contents (all field values) of this tile with the given tile. */
  void swap(ResultTile& tile);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Equality operator (mainly for debugging purposes). */
  bool operator==(const ResultTile& rt) const;

  /**
   * Returns the number of cells in the result tile.
   * Should be the same across all attributes.
   */
  uint64_t cell_num() const;

  /** Returns true if it stores zipped coordinates. */
  bool stores_zipped_coords() const;

  /** Returns the zipped coordinates tile. */
  const Tile& zipped_coords_tile() const;

  /** Returns the coordinate tile for the input dimension. */
  const TileTuple& coord_tile(unsigned dim_idx) const;

  /** Returns the stored domain. */
  const Domain* domain() const {
    return domain_;
  }

  /** Erases the tile for the input attribute/dimension. */
  void erase_tile(const std::string& name);

  /** Initializes the result tile for the given attribute. */
  void init_attr_tile(const std::string& name);

  /** Initializes the result tile for the given dimension name and index. */
  void init_coord_tile(const std::string& name, unsigned dim_idx);

  /** Returns the tile pair for the input attribute or dimension. */
  TileTuple* tile_tuple(const std::string& name);

  /**
   * Returns a constant pointer to the coordinate at position `pos` for
   * dimension `dim_idx`. This will fetch from the zipped `coord_tile_`
   * unless at least one `init_coord_tile()` has been invoked on this
   * instance. The caller must be certain that a coordinate exists at
   * the requested position and dimension.
   */
  inline const void* coord(uint64_t pos, unsigned dim_idx) const {
    return (this->*coord_func_)(pos, dim_idx);
  }

  /**
   * Returns the string coordinate at position `pos` for
   * dimension `dim_idx`. Applicable only to string dimensions.
   */
  std::string_view coord_string(uint64_t pos, unsigned dim_idx) const;

  /** Returns the coordinate size on the input dimension. */
  uint64_t coord_size(unsigned dim_idx) const;

  /**
   * Returns true if the coordinates at position `pos_a` and `pos_b` are
   * the same.
   */
  bool same_coords(const ResultTile& rt, uint64_t pos_a, uint64_t pos_b) const;

  /** Returns the fragment id that this result tile belongs to. */
  unsigned frag_idx() const;

  /** Returns the tile index of this tile in the fragment. */
  uint64_t tile_idx() const;

  /**
   * Reads `len` coordinates the from the tile, starting at the beginning of
   * the coordinates at position `pos`.
   */
  Status read(
      const std::string& name,
      void* buffer,
      uint64_t buffer_offset,
      uint64_t pos,
      uint64_t len);

  /**
   * Reads `len` coordinates the from the tile, starting at the beginning of
   * the coordinates at position `pos`. The associated validity values are
   * stored in the `buffer_validity`.
   */
  Status read_nullable(
      const std::string& name,
      void* buffer,
      uint64_t buffer_offset,
      uint64_t pos,
      uint64_t len,
      void* buffer_validity);

  /**
   * Applicable only to sparse tiles of dense arrays.
   *
   * Accummulates to a result bitmap for the coordinates that
   * fall in the input range, checking only dimensions `dim_idx`.
   * It also accummulates to an `overwritten_bitmap` that
   * checks if the coordinate is overwritten on that dimension by a
   * future fragment (one with index greater than `frag_idx`). That
   * is computed only for `dim_idx == dim_num -1`, as it needs
   * to know if the the coordinates to be checked are already results.
   */
  template <class T>
  static void compute_results_dense(
      const ResultTile* result_tile,
      unsigned dim_idx,
      const Range& range,
      const std::vector<shared_ptr<FragmentMetadata>> fragment_metadata,
      unsigned frag_idx,
      std::vector<uint8_t>* result_bitmap,
      std::vector<uint8_t>* overwritten_bitmap);

  /**
   * Applicable only to sparse arrays.
   *
   * Computes a result bitmap for the input dimension for the coordinates that
   * fall in the input range.
   */
  template <class T>
  static void compute_results_sparse(
      const ResultTile* result_tile,
      unsigned dim_idx,
      const Range& range,
      std::vector<uint8_t>* result_bitmap,
      const Layout& cell_order);

  /**
   * Applicable only to sparse arrays.
   *
   * Computes a result count for the input dimension for the coordinates that
   * fall in the input ranges and multiply with the previous count.
   *
   * This only processes cells from min_cell to max_cell as we might
   * parallelize on cells.
   *
   * When called over multiple ranges, this follows the formula:
   * total_count = d1_count * d2_count ... dN_count.
   */
  template <class BitmapType, class T>
  static void compute_results_count_sparse(
      const ResultTile* result_tile,
      unsigned dim_idx,
      const NDRange& ranges,
      const std::vector<uint64_t>& range_indexes,
      std::vector<BitmapType>& result_count,
      const Layout& cell_order,
      const uint64_t min_cell,
      const uint64_t max_cell);

  /**
   * Applicable only to sparse arrays.
   *
   * Computes a result count for the input string dimension for the coordinates
   * that fall in the input ranges and multiply with the previous count.
   *
   * This only processes cells from min_cell to max_cell as we might
   * parallelize on cells.
   *
   * When called over multiple ranges, this follows the formula:
   * total_count = d1_count * d2_count ... dN_count.
   */
  template <class BitmapType>
  static void compute_results_count_sparse_string(
      const ResultTile* result_tile,
      unsigned dim_idx,
      const NDRange& ranges,
      const std::vector<uint64_t>& range_indexes,
      std::vector<BitmapType>& result_count,
      const Layout& cell_order,
      const uint64_t min_cell,
      const uint64_t max_cell);

  /**
   * Applicable only to sparse tiles of dense arrays.
   *
   * Accummulates to a result bitmap for the coordinates that
   * fall in the input range, checking only dimensions `dim_idx`.
   * It also accummulates to an `overwritten_bitmap` that
   * checks if the coordinate is overwritten on that dimension by a
   * future fragment (one with index greater than `frag_idx`). That
   * is computed only for `dim_idx == dim_num -1`, as it needs
   * to know if the the coordinates to be checked are already results.
   */
  Status compute_results_dense(
      unsigned dim_idx,
      const Range& range,
      const std::vector<shared_ptr<FragmentMetadata>> fragment_metadata,
      unsigned frag_idx,
      std::vector<uint8_t>* result_bitmap,
      std::vector<uint8_t>* overwritten_bitmap) const;

  /**
   * Applicable only to sparse arrays.
   *
   * Accummulates to a result bitmap for the coordinates that
   * fall in the input range, checking only dimensions `dim_idx`.
   */
  Status compute_results_sparse(
      unsigned dim_idx,
      const Range& range,
      std::vector<uint8_t>* result_bitmap,
      const Layout& cell_order) const;

  /**
   * Applicable only to sparse arrays.
   *
   * Computes a result count for the input dimension for the coordinates that
   * fall in the input ranges and multiply with the previous count.
   *
   * This only processes cells from min_cell to max_cell as we might
   * parallelize on cells.
   *
   * When called over multiple ranges, this follows the formula:
   * total_count = d1_count * d2_count ... dN_count.
   */
  template <class BitmapType>
  Status compute_results_count_sparse(
      unsigned dim_idx,
      const NDRange& ranges,
      const std::vector<uint64_t>& range_indexes,
      std::vector<BitmapType>& result_count,
      const Layout& cell_order,
      const uint64_t min_cell,
      const uint64_t max_cell) const;

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The array domain. */
  const Domain* domain_;

  /** The id of the fragment this tile belongs to. */
  unsigned frag_idx_ = UINT32_MAX;

  /** The id of the tile (which helps locating the physical attribute tiles). */
  uint64_t tile_idx_ = UINT64_MAX;

  /** Attribute names to tiles based on attribute ordering from array schema. */
  std::vector<std::pair<std::string, optional<TileTuple>>> attr_tiles_;

  /** The zipped coordinates tile. */
  TileTuple coords_tile_;

  /**
   * The separate coordinate tiles along with their names, sorted on the
   * dimension order.
   */
  std::vector<std::pair<std::string, TileTuple>> coord_tiles_;

  /**
   * Stores the appropriate templated compute_results_dense() function based for
   * each dimension, based on the dimension datatype.
   */
  std::vector<std::function<void(
      const ResultTile*,
      unsigned,
      const Range&,
      const std::vector<shared_ptr<FragmentMetadata>>,
      unsigned,
      std::vector<uint8_t>*,
      std::vector<uint8_t>*)>>
      compute_results_dense_func_;

  /**
   * Implements coord() with either zipped or unzipped coordinates. This
   * is invoked in a critical path and is experimentally faster as a c-style
   * function pointer than a bound `std::function`.
   */
  const void* (ResultTile::*coord_func_)(uint64_t, unsigned)const;

  /**
   * Stores the appropriate templated compute_results_sparse() function based
   * for each dimension, based on the dimension datatype.
   */
  std::vector<std::function<void(
      const ResultTile*,
      unsigned,
      const Range&,
      std::vector<uint8_t>*,
      const Layout&)>>
      compute_results_sparse_func_;

  /**
   * Stores the appropriate templated compute_results_count_sparse() function
   * for each dimension, based on the dimension datatype.
   */
  std::vector<std::function<void(
      const ResultTile*,
      unsigned,
      const NDRange&,
      const std::vector<uint64_t>&,
      std::vector<uint64_t>&,
      const Layout&,
      const uint64_t,
      const uint64_t)>>
      compute_results_count_sparse_uint64_t_func_;

  /**
   * Stores the appropriate templated compute_results_count_sparse() function
   * for each dimension, based on the dimension datatype.
   */
  std::vector<std::function<void(
      const ResultTile*,
      unsigned,
      const NDRange&,
      const std::vector<uint64_t>&,
      std::vector<uint8_t>&,
      const Layout&,
      const uint64_t,
      const uint64_t)>>
      compute_results_count_sparse_uint8_t_func_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /** Sets the templated compute_results() function. */
  void set_compute_results_func();

  /** Implements coord() for zipped coordinates. */
  const void* zipped_coord(uint64_t pos, unsigned dim_idx) const;

  /** Implements coord() for unzipped coordinates. */
  const void* unzipped_coord(uint64_t pos, unsigned dim_idx) const;

  /**
   * A helper routine used in `compute_results_sparse<char>` to
   * determine if a given string-valued coordinate intersects
   * the given start and end range.
   *
   * @param c_offset The offset of the coordinate value
   *    within `buff_str`.
   * @param c_cize The size of the coordinate value.
   * @param buff_str The buffer containing the coordinate value
   *    at `c_offset`.
   * @param range_start The starting range value.
   * @param range_end The ending range value.
   * @return false for no intersection, true for instersection.
   */
  static bool str_coord_intersects(
      const uint64_t c_offset,
      const uint64_t c_size,
      const char* const buff_str,
      const std::string_view& range_start,
      const std::string_view& range_end);
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_RESULT_TILE_H
