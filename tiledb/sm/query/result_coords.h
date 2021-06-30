/**
 * @file   result_coords.h
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
 * This file defines struct ResultCoords.
 */

#ifndef TILEDB_RESULT_COORDS_H
#define TILEDB_RESULT_COORDS_H

#include <iostream>
#include <vector>

#include "tiledb/sm/query/result_tile.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/**
 * Stores information about cell coordinates of a sparse fragment
 * that are in the result of a subarray query.
 */
struct ResultCoords {
  /**
   * The result tile the coords belong to.
   *
   * Note that the tile this points to is allocated and freed in
   * sparse_read/dense_read, so the lifetime of this struct must not exceed
   * the scope of those functions.
   */
  ResultTile* tile_;
  /** The position of the coordinates in the tile. */
  uint64_t pos_;
  /** Whether this instance is "valid". */
  bool valid_;

  /** Constructor. */
  ResultCoords()
      : tile_(nullptr)
      , pos_(0)
      , valid_(false) {
  }

  /** Constructor. */
  ResultCoords(ResultTile* tile, uint64_t pos)
      : tile_(tile)
      , pos_(pos)
      , valid_(true) {
  }

  /** Moves to the next cell */
  bool next() {
    if (pos_ == tile_->cell_num() - 1)
      return false;

    pos_++;
    return true;
  }

  /** Invalidate this instance. */
  void invalidate() {
    valid_ = false;
  }

  /** Return true if this instance is valid. */
  bool valid() const {
    return valid_;
  }

  /**
   * Returns a string coordinate. Applicable only to string
   * dimensions.
   */
  std::string coord_string(unsigned dim_idx) const {
    return tile_->coord_string(pos_, dim_idx);
  }

  /**
   * Returns the coordinate at the object's position `pos_` from the object's
   * tile `tile_` on the given dimension.
   *
   * @param dim_idx The index of the dimension to retrieve the coordinate for.
   * @return A constant pointer to the requested coordinate.
   */
  const void* coord(unsigned dim_idx) const {
    return tile_->coord(pos_, dim_idx);
  }

  /**
   * Returns true if the coordinates (at the current position) of the
   * calling ResultCoords object and the input are the same across all
   * dimensions.
   */
  bool same_coords(const ResultCoords& rc) const {
    return tile_->same_coords(*(rc.tile_), pos_, rc.pos_);
  }
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_RESULT_COORDS_H
