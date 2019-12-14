/**
 * @file   result_coords.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2019 TileDB, Inc.
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

#include "tiledb/sm/query/result_tile.h"

#include <iostream>
#include <vector>

namespace tiledb {
namespace sm {

/**
 * Stores information about cell coordinates of a sparse fragment
 * that are result of a subarray query.
 *
 * @tparam T The coords type
 */
template <class T>
struct ResultCoords {
  /**
   * The result tile the coords belong to.
   *
   * Note that the tile this points to is allocated and freed in
   * sparse_read/dense_read, so the lifetime of this struct must not exceed
   * the scope of those functions.
   */
  ResultTile* tile_;
  /** The coordinates. */
  const T* coords_;
  /** The coordinates of the tile in the global logical space. */
  const T* tile_coords_;
  /** The position of the coordinates in the tile. */
  uint64_t pos_;
  /** Whether this instance is "valid". */
  bool valid_;

  /** Constructor. */
  ResultCoords(ResultTile* tile, const T* coords, uint64_t pos)
      : tile_(tile)
      , coords_(coords)
      , tile_coords_(nullptr)
      , pos_(pos)
      , valid_(true) {
  }

  /** Invalidate this instance. */
  void invalidate() {
    valid_ = false;
  }

  /** Return true if this instance is valid. */
  bool valid() const {
    return valid_;
  }

  /** Mainly for debugging. */
  void print(size_t dim_num) const {
    if (tile_ == nullptr) {
      std::cout << "null tile\n";
    } else {
      std::cout << "frag_idx: " << tile_->frag_idx_ << "\n";
      std::cout << "tile_idx: " << tile_->tile_idx_ << "\n";
    }
    std::cout << "pos: " << pos_ << "\n";
    std::cout << "valid: " << valid_ << "\n";
    if (coords_ != nullptr) {
      for (size_t i = 0; i < dim_num; ++i) {
        std::cout << "coord[" << i << "]: " << coords_[i] << "\n";
      }
    }
    if (tile_coords_ != nullptr)
      std::cout << "first tile coord: " << tile_coords_[0] << "\n";
  }
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_RESULT_COORDS_H
