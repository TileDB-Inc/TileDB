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
 * This file defines struct ResultTile.
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

  /** Default constructor. */
  ResultTile() = default;

  /**
   * Constructor. The number of dimensions `dim_num` is used to allocate
   * the separate coordinate tiles.
   */
  ResultTile(unsigned frag_idx, uint64_t tile_idx, unsigned dim_num)
      : frag_idx_(frag_idx)
      , tile_idx_(tile_idx) {
    coord_tiles_.resize(dim_num);
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

  /** Erases the tile for the input attribute/dimension. */
  void erase_tile(const std::string& name) {
    // Handle zipped coordinates tiles
    if (name == constants::coords) {
      coords_tile_ = TilePair(Tile(), Tile());
      return;
    }

    // Handle dimension tile
    for (auto& ct : coord_tiles_) {
      if (ct.first == name) {
        ct.second = TilePair(Tile(), Tile());
        return;
      }
    }

    // Handle attribute tile
    attr_tiles_.erase(name);
  }

  /** Initializes the result tile for the given attribute. */
  void init_attr_tile(const std::string& name) {
    // Nothing to do for the special zipped coordinates tile
    if (name == constants::coords)
      return;

    // Handle attributes
    if (attr_tiles_.find(name) == attr_tiles_.end())
      attr_tiles_.emplace(name, TilePair(Tile(), Tile()));
  }

  /** Initializes the result tile for the given dimension name and index. */
  void init_coord_tile(const std::string& name, unsigned dim_idx) {
    coord_tiles_[dim_idx] =
        std::pair<std::string, TilePair>(name, TilePair(Tile(), Tile()));
  }

  /** Returns the tile pair for the input attribute or dimension. */
  TilePair* tile_pair(const std::string& name) {
    // Handle zipped coordinates tile
    if (name == constants::coords)
      return &coords_tile_;

    // Handle attribute tile
    auto it = attr_tiles_.find(name);
    if (it != attr_tiles_.end())
      return &(it->second);

    // Handle separate coordinates tile
    for (auto& ct : coord_tiles_) {
      if (ct.first == name)
        return &(ct.second);
    }

    return nullptr;
  }

  /**
   * Returns a constant pointer to the coordinate at position `pos` for
   * dimension `dim_idx`.
   */
  const void* coord(uint64_t pos, unsigned dim_idx) const {
    // Handle separate coordinate tiles
    const auto& coord_tile = coord_tiles_[dim_idx].second.first;
    if (!coord_tile.empty()) {
      auto coord_buff = (const unsigned char*)coord_tile.internal_data();
      return &coord_buff[pos * coord_tile.cell_size()];
    }

    // Handle zipped coordinates tile
    if (!coords_tile_.first.empty()) {
      auto coords_size = coords_tile_.first.cell_size();
      auto coord_size = coords_size / coords_tile_.first.dim_num();
      auto coords_buff =
          (const unsigned char*)coords_tile_.first.internal_data();
      return &coords_buff[pos * coords_size + dim_idx * coord_size];
    }

    return nullptr;
  }

  /** Returns the coordinate size on the input dimension. */
  uint64_t coord_size(unsigned dim_idx) const {
    // Handle zipped coordinate tiles
    if (!coords_tile_.first.empty())
      return coords_tile_.first.cell_size() / coords_tile_.first.dim_num();

    // Handle separate coordinate tiles
    return coord_tiles_[dim_idx].second.first.cell_size();
  }

  /**
   * Returns true if the coordinates of the calling object at position `pos_a`
   * and the coordinates of the input result tile at `pos_b` are the same
   * across all dimensions.
   */
  bool same_coords(const ResultTile& rt, uint64_t pos_a, uint64_t pos_b) const {
    auto dim_num = coord_tiles_.size();
    for (unsigned d = 0; d < dim_num; ++d) {
      if (std::memcmp(coord(pos_a, d), rt.coord(pos_b, d), coord_size(d)) != 0)
        return false;
    }

    return true;
  }

  /** Returns the zipped coordinates tile. */
  const Tile& coords_tile() const {
    return coords_tile_.first;
  }

  /** Returns the fragment index. */
  unsigned frag_idx() const {
    return frag_idx_;
  }

  /** Returns the tile index. */
  uint64_t tile_idx() const {
    return tile_idx_;
  }

 private:
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
