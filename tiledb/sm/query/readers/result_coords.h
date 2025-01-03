/**
 * @file   result_coords.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2022 TileDB, Inc.
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
 * This file defines struct ResultCoords.
 */

#ifndef TILEDB_RESULT_COORDS_H
#define TILEDB_RESULT_COORDS_H

#include <iostream>
#include <vector>

#include "tiledb/common/types/dynamic_typed_datum.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/misc/type_traits.h"
#include "tiledb/sm/query/readers/result_tile.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

template <class BitmapType>
class GlobalOrderResultTile;

/**
 * Stores information about cell coordinates of a sparse fragment
 * that are in the result of a subarray query.
 */
template <class ResultTileType>
struct ResultCoordsBase {
  /**
   * The result tile the coords belong to.
   *
   * Note that the tile this points to is allocated and freed in
   * sparse_read/dense_read, so the lifetime of this struct must not exceed
   * the scope of those functions.
   */
  ResultTileType* tile_;
  /** The position of the coordinates in the tile. */
  uint64_t pos_;

  /** Constructor. */
  ResultCoordsBase()
      : tile_(nullptr)
      , pos_(0) {
  }

  /** Constructor. */
  ResultCoordsBase(ResultTileType* tile, uint64_t pos)
      : tile_(tile)
      , pos_(pos) {
  }

  unsigned fragment_idx() const {
    return tile_->frag_idx();
  }

  uint64_t tile_idx() const {
    return tile_->tile_idx();
  }

  /**
   * Returns a string coordinate. Applicable only to string
   * dimensions.
   */
  inline std::string_view coord_string(unsigned dim_idx) const {
    return tile_->coord_string(pos_, dim_idx);
  }

  /**
   * Returns the coordinate at the object's position `pos_` from the object's
   * tile `tile_` on the given dimension.
   *
   * @param dim_idx The index of the dimension to retrieve the coordinate for.
   * @return A constant pointer to the requested coordinate.
   */
  inline const void* coord(unsigned dim_idx) const {
    return tile_->coord(pos_, dim_idx);
  }

  inline UntypedDatumView dimension_datum(
      const Dimension& dim, unsigned dim_idx) const {
    if (dim.var_size()) {
      auto x{tile_->coord_string(pos_, dim_idx)};
      return tdb::UntypedDatumView{x.data(), x.size()};
    } else {
      return tdb::UntypedDatumView{coord(dim_idx), dim.coord_size()};
    }
  }

  /**
   * Returns true if the coordinates (at the current position) of the
   * calling ResultCoords object and the input are the same across all
   * dimensions.
   */
  bool same_coords(const ResultCoordsBase& rc) const {
    return tile_->same_coords(*(rc.tile_), pos_, rc.pos_);
  }
};

struct ResultCoords : public ResultCoordsBase<ResultTile> {
  /** Constructor. */
  ResultCoords()
      : ResultCoordsBase()
      , valid_(false) {
  }

  /** Constructor. */
  ResultCoords(ResultTile* tile, uint64_t pos)
      : ResultCoordsBase(tile, pos)
      , valid_(true) {
  }

  /** Invalidate this instance. */
  inline void invalidate() {
    valid_ = false;
  }

  /** Return true if this instance is valid. */
  inline bool valid() const {
    return valid_;
  }

  /** Whether this instance is "valid". */
  bool valid_;
};

template <class BitmapType>
struct GlobalOrderResultCoords
    : public ResultCoordsBase<GlobalOrderResultTile<BitmapType>> {
  using base = ResultCoordsBase<GlobalOrderResultTile<BitmapType>>;

  /**
   * Set to false when a duplicate was found in the cell following this cell
   * in the same fragment and added to the tile queue.
   */
  bool has_next_;

  /** Constructor. */
  GlobalOrderResultCoords(GlobalOrderResultTile<BitmapType>* tile, uint64_t pos)
      : ResultCoordsBase<GlobalOrderResultTile<BitmapType>>(tile, pos)
      , has_next_(true)
      , init_(false) {
  }

  /** Constructor. */
  GlobalOrderResultCoords(
      GlobalOrderResultTile<BitmapType>* tile, uint64_t pos, bool has_next)
      : ResultCoordsBase<GlobalOrderResultTile<BitmapType>>(tile, pos)
      , has_next_(has_next)
      , init_(false) {
  }

  /** Advance to the next available cell in the tile. */
  bool advance_to_next_cell() {
    base::pos_ += init_;
    init_ = true;
    uint64_t cell_num = base::tile_->cell_num();
    if (base::pos_ != cell_num) {
      if (base::tile_->has_bmp()) {
        while (base::pos_ < cell_num) {
          if (base::tile_->bitmap()[base::pos_]) {
            return true;
          }
          base::pos_++;
        }
      } else {
        return true;
      }
    }

    return false;
  }

  /** See if the next cell has the same coordinates. */
  bool next_cell_same_coords() {
    auto next_pos = base::pos_ + 1;
    uint64_t cell_num = base::tile_->cell_num();
    if (next_pos != cell_num) {
      if (base::tile_->has_bmp()) {
        while (next_pos < cell_num) {
          if (base::tile_->bitmap()[next_pos]) {
            break;
          }
          next_pos++;
        }
      }
    }

    if (next_pos == cell_num) {
      return false;
    }

    return base::tile_->same_coords(*base::tile_, base::pos_, next_pos);
  }

  /**
   * Get the maximum slab length that can be created (when there's no other
   * fragments left).
   *
   * @return Max slab length that can be merged for this tile.
   */
  uint64_t max_slab_length() {
    uint64_t ret = 1;
    uint64_t cell_num = base::tile_->cell_num();
    uint64_t next_pos = base::pos_ + 1;
    if (base::tile_->has_post_dedup_bmp()) {
      auto& bitmap = base::tile_->post_dedup_bitmap();

      // Current cell is not in the bitmap.
      if (!bitmap[base::pos_]) {
        return 0;
      }

      // For overlapping ranges, if there's more than one cell in the bitmap,
      // return 1.
      const bool overlapping_ranges = std::is_same<BitmapType, uint64_t>::value;
      if constexpr (overlapping_ranges) {
        if (bitmap[base::pos_] != 1) {
          return 1;
        }
      }

      // With bitmap, find the longest contiguous set of bits in the bitmap
      // from the current position.
      while (next_pos < cell_num && bitmap[next_pos] == 1) {
        next_pos++;
        ret++;
      }
    } else {
      // No bitmap, add all cells from current position.
      ret = cell_num - base::pos_;
    }

    return ret;
  }

  /**
   * Get the maximum slab length that can be created using the next result
   * coords in the queue.
   *
   * @param next The next coordinate, which is the smallest coordinate, in
   * Global order or Hilbert order, from all the other fragments current
   * indexes. Mostly, this is the maximum coordinate value that can be merged
   * before we need to run our top level algorithm again.
   * @param cmp The comparator class. Calling cmp(current, next) should tell us
   * if current is bigger or equal than next in the order of the comparator.
   *
   * @return Max slab length that can be merged for this tile.
   */
  template <GlobalCellCmpable GlobalOrderLowerBound, class CompType>
  uint64_t max_slab_length(
      const GlobalOrderLowerBound& next, const CompType& cmp) {
    uint64_t cell_num = base::tile_->cell_num();

    // Store the original position.
    uint64_t original_pos = base::pos_;

    // Max posssible position in the tile. Defaults to the last cell in the
    // tile, it might get updated if we have a bitmap below.
    uint64_t max_pos = cell_num - 1;

    // If there is a bitmap, update the maximum position. Mostly, this looks at
    // the current cell (if it's not in the bitmap, return 0), then will go
    // until we find a cell that isn't in the bitmap. This will tell us the
    // maximum slab that can be merged for this bitmap, next we'll look at
    // next.
    if (base::tile_->has_post_dedup_bmp()) {
      auto& bitmap = base::tile_->post_dedup_bitmap();
      // Current cell is not in the bitmap.
      if (!bitmap[base::pos_]) {
        return 0;
      }

      // For overlapping ranges, if there's more than one cell in the bitmap,
      // return 1.
      const bool overlapping_ranges = std::is_same<BitmapType, uint64_t>::value;
      if constexpr (overlapping_ranges) {
        if (bitmap[base::pos_] != 1) {
          return 1;
        }
      }

      // Compute max position.
      max_pos = base::pos_;
      while (max_pos < cell_num && bitmap[max_pos] == 1) {
        max_pos++;
      }
      max_pos--;
    }

    // Now use cmp to find the last value in this tile smaller than next.
    // But, calling cmp can be expensive. So to minimize how many times it is
    // called, we first call cmp at every power of 2 indexes from the current
    // cell, until we find a value that is bigger than next. This will give us
    // an upper bound for searching. We know that the previous power of two is
    // smaller than next as we already called cmp on it, so this will be a
    // lower bound for the search.
    // This ensures the algorithm works equally well for small slabs vs large
    // ones. It will never take more comparisons that a linear search.
    uint64_t power_of_two = 1;
    bool return_max = true;
    while (return_max && base::pos_ != max_pos) {
      base::pos_ = std::min(original_pos + power_of_two, max_pos);
      if (cmp(*this, next)) {
        return_max = false;

        // If we exit on first comprarison, return 1.
        if (power_of_two == 1) {
          base::pos_ = original_pos;
          return 1;
        }
      } else {
        power_of_two *= 2;
      }
    }

    // If we reached the end without cmp being true once, we know that every
    // cell until max_pos is smaller than next. So return the maximum cell
    // slab.
    if (return_max) {
      base::pos_ = original_pos;
      return max_pos - original_pos + 1;
    }

    // We have an upper bound and a lower bound for our search with our power
    // of twos found above. Run a bisection search in between to find the exact
    // cell.
    uint64_t left = power_of_two / 2;
    uint64_t right = base::pos_;
    while (left != right - 1) {
      // Check against mid.
      base::pos_ = left + (right - left) / 2;
      if (!cmp(*this, next)) {
        left = base::pos_;
      } else {
        right = base::pos_;
      }
    }

    // Restore the original position and return.
    base::pos_ = original_pos;
    return left - original_pos + 1;
  }

 private:
  /** Initially set to false on first call to next_cell. */
  bool init_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_RESULT_COORDS_H
