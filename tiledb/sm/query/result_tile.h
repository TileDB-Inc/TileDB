/**
 * @file   result_tile.h
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
 * This file defines struct ResultTile.
 */

#ifndef TILEDB_RESULT_TILE_H
#define TILEDB_RESULT_TILE_H

#include "tiledb/sm/tile/tile.h"

#include <iostream>
#include <vector>

namespace tiledb {
namespace sm {

/**
 * Stores information about a logical dense or sparse result tile. Note that it
 * may store the physical tiles across more than one attributes for the same
 * logical tile (space/data tile for dense, data tile oriented by an MBR for
 * sparse).
 */
struct ResultTile {
  /**
   * For each fixed-sized attributes, the second tile in the pair is ignored.
   * For var-sized attributes, the first is the offsets tile and the second is
   * the var-sized values tile.
   */
  typedef std::pair<Tile, Tile> TilePair;

  /** The id of the fragment this tile belongs to. */
  unsigned frag_idx_ = UINT32_MAX;
  /** The id of the tile (which helps locating the physical attribute tiles). */
  uint64_t tile_idx_ = UINT64_MAX;
  /**
   * Maps attribute names to attribute tiles. Note that the coordinates
   * are a special attribute as well.
   */
  std::unordered_map<std::string, TilePair> attr_tiles_;

  /** Default constructor. */
  ResultTile() = default;

  ResultTile(unsigned frag_idx, uint64_t tile_idx)
      : frag_idx_(frag_idx)
      , tile_idx_(tile_idx) {
  }

  /** Default destructor. */
  ~ResultTile() = default;

  /** Default copy constructor. */
  ResultTile(const ResultTile& result_tile) = default;

  /** Default move constructor. */
  ResultTile(ResultTile&& result_tile) = default;

  /** Default copy-assign operator. */
  ResultTile& operator=(const ResultTile& result_tile) = default;

  /** Default move-assign operator. */
  ResultTile& operator=(ResultTile&& result_tile) = default;

  /** Equality operator (mainly for debugging purposes). */
  bool operator==(const ResultTile& rt) const {
    return frag_idx_ == rt.frag_idx_ && tile_idx_ == rt.tile_idx_;
  }
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_RESULT_TILE_H
