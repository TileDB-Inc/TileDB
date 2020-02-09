/**
 * @file   result_tile.h
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
 * This file defines class ResultTile.
 */

#ifndef TILEDB_RESULT_TILE_H
#define TILEDB_RESULT_TILE_H

#include <iostream>
#include <unordered_map>
#include <vector>

#include "tiledb/sm/tile/tile.h"

#include "tiledb/sm/tile/tile.h"

namespace tiledb {
namespace sm {

class Domain;

/**
 * Stores information about a logical dense or sparse result tile. Note that it
 * may store the physical tiles across more than one attributes for the same
 * logical tile (space/data tile for dense, data tile oriented by an MBR for
 * sparse).
 */
class ResultTile {
 public:
  /**
   * For each fixed-sized attributes, the second tile in the pair is ignored.
   * For var-sized attributes, the first is the offsets tile and the second is
   * the var-sized values tile.
   */
  typedef std::pair<Tile, Tile> TilePair;

  /** Default constructor. */
  ResultTile() = default;

  /**
   * Constructor. The number of dimensions `dim_num` is used to allocate
   * the separate coordinate tiles.
   */
  ResultTile(unsigned frag_idx, uint64_t tile_idx, const Domain* domain);

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
  bool operator==(const ResultTile& rt) const;

  /**
   * Returns the number of cells in the result tile.
   * Should be the same across all attributes.
   */
  uint64_t cell_num() const;

  /** Erases the tile for the input attribute/dimension. */
  void erase_tile(const std::string& name);

  /** Initializes the result tile for the given attribute. */
  void init_attr_tile(const std::string& name);

  /** Initializes the result tile for the given dimension name and index. */
  void init_coord_tile(const std::string& name, unsigned dim_idx);

  /** Returns the tile pair for the input attribute or dimension. */
  TilePair* tile_pair(const std::string& name);

  /**
   * Returns a constant pointer to the coordinate at position `pos` for
   * dimension `dim_idx`.
   */
  const void* coord(uint64_t pos, unsigned dim_idx) const;

  /**
   * Returns true if the coordinates at position `pos` are inside
   * the input multi-dimensional rectangle.
   */
  bool coord_in_rect(uint64_t pos, const std::vector<const void*>& rect) const;

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
      const std::string& name, void* buffer, uint64_t pos, uint64_t len);

 private:
  /** The array domain. */
  const Domain* domain_;
  /** The id of the fragment this tile belongs to. */
  unsigned frag_idx_ = UINT32_MAX;
  /** The id of the tile (which helps locating the physical attribute tiles). */
  uint64_t tile_idx_ = UINT64_MAX;
  /** Maps attribute names to tiles. */
  std::unordered_map<std::string, TilePair> attr_tiles_;
  /** The zipped coordinates tile. */
  TilePair coords_tile_;
  /**
   * The separate coordinate tiles along with their names, sorted on the
   * dimension order.
   */
  std::vector<std::pair<std::string, TilePair>> coord_tiles_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_RESULT_TILE_H
