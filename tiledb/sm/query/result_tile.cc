/**
 * @file   result_tile.cc
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
 * This file implements class ResultTile.
 */

#include "tiledb/sm/query/result_tile.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"

#include <cassert>
#include <iostream>
#include <list>

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

ResultTile::ResultTile(
    unsigned frag_idx, uint64_t tile_idx, const Domain* domain)
    : domain_(domain)
    , frag_idx_(frag_idx)
    , tile_idx_(tile_idx) {
  assert(domain != nullptr);
  coord_tiles_.resize(domain->dim_num());
}

/* ****************************** */
/*               API              */
/* ****************************** */

bool ResultTile::operator==(const ResultTile& rt) const {
  return frag_idx_ == rt.frag_idx_ && tile_idx_ == rt.tile_idx_;
}

uint64_t ResultTile::cell_num() const {
  if (!coord_tiles_[0].second.first.empty())
    return coord_tiles_[0].second.first.cell_num();

  if (!coords_tile_.first.empty())
    return coords_tile_.first.cell_num();

  if (!attr_tiles_.empty())
    return attr_tiles_.begin()->second.first.cell_num();

  return 0;
}

void ResultTile::erase_tile(const std::string& name) {
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

void ResultTile::init_attr_tile(const std::string& name) {
  // Nothing to do for the special zipped coordinates tile
  if (name == constants::coords)
    return;

  // Handle attributes
  if (attr_tiles_.find(name) == attr_tiles_.end())
    attr_tiles_.emplace(name, TilePair(Tile(), Tile()));
}

void ResultTile::init_coord_tile(const std::string& name, unsigned dim_idx) {
  coord_tiles_[dim_idx] =
      std::pair<std::string, TilePair>(name, TilePair(Tile(), Tile()));
}

ResultTile::TilePair* ResultTile::tile_pair(const std::string& name) {
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

const void* ResultTile::coord(uint64_t pos, unsigned dim_idx) const {
  // Handle separate coordinate tiles
  const auto& coord_tile = coord_tiles_[dim_idx].second.first;
  if (!coord_tile.empty()) {
    const uint64_t offset = pos * coord_tile.cell_size();
    void* buffer = nullptr;
    const Status st = coord_tile.chunked_buffer()->internal_buffer_from_offset(
        offset, &buffer);
    assert(st.ok());
    return buffer;
  }

  // Handle zipped coordinates tile
  if (!coords_tile_.first.empty()) {
    auto coords_size = coords_tile_.first.cell_size();
    auto coord_size = coords_size / coords_tile_.first.dim_num();
    const uint64_t offset = pos * coords_size + dim_idx * coord_size;
    void* buffer = nullptr;
    const Status st =
        coords_tile_.first.chunked_buffer()->internal_buffer_from_offset(
            offset, &buffer);
    assert(st.ok());
    return buffer;
  }

  return nullptr;
}

std::string ResultTile::coord_string(uint64_t pos, unsigned dim_idx) const {
  const auto& coord_tile_off = coord_tiles_[dim_idx].second.first;
  const auto& coord_tile_val = coord_tiles_[dim_idx].second.second;
  assert(!coord_tile_off.empty());
  assert(!coord_tile_val.empty());
  auto cell_num = coord_tile_off.cell_num();
  auto val_size = coord_tile_val.size();

  uint64_t offset = 0;
  Status st = coord_tile_off.chunked_buffer()->read(
      &offset, sizeof(uint64_t), pos * sizeof(uint64_t));
  assert(st.ok());

  uint64_t next_offset = 0;
  if (pos == cell_num - 1) {
    next_offset = val_size;
  } else {
    st = coord_tile_off.chunked_buffer()->read(
        &next_offset, sizeof(uint64_t), (pos + 1) * sizeof(uint64_t));
    assert(st.ok());
  }

  auto size = next_offset - offset;

  void* buffer = nullptr;
  st = coord_tile_val.chunked_buffer()->internal_buffer_from_offset(
      offset, &buffer);
  assert(st.ok());

  return std::string(static_cast<char*>(buffer), size);
}

bool ResultTile::coord_in_rect(uint64_t pos, const NDRange& rect) const {
  auto dim_num = domain_->dim_num();
  for (unsigned d = 0; d < dim_num; ++d) {
    if (!domain_->dimension(d)->var_size()) {  // Fixed-sized
      if (!domain_->dimension(d)->value_in_range(coord(pos, d), rect[d]))
        return false;
    } else {  // Var-sized
      if (!domain_->dimension(d)->value_in_range(coord_string(pos, d), rect[d]))
        return false;
    }
  }

  return true;
}

uint64_t ResultTile::coord_size(unsigned dim_idx) const {
  // Handle zipped coordinate tiles
  if (!coords_tile_.first.empty())
    return coords_tile_.first.cell_size() / coords_tile_.first.dim_num();

  // Handle separate coordinate tiles
  assert(dim_idx < coord_tiles_.size());
  return coord_tiles_[dim_idx].second.first.cell_size();
}

bool ResultTile::same_coords(
    const ResultTile& rt, uint64_t pos_a, uint64_t pos_b) const {
  auto dim_num = coord_tiles_.size();
  for (unsigned d = 0; d < dim_num; ++d) {
    if (!domain_->dimension(d)->var_size()) {  // Fixed-sized
      if (std::memcmp(coord(pos_a, d), rt.coord(pos_b, d), coord_size(d)) != 0)
        return false;
    } else {  // Var-sized
      if (coord_string(pos_a, d) != rt.coord_string(pos_b, d))
        return false;
    }
  }

  return true;
}

unsigned ResultTile::frag_idx() const {
  return frag_idx_;
}

uint64_t ResultTile::tile_idx() const {
  return tile_idx_;
}

Status ResultTile::read(
    const std::string& name, void* buffer, uint64_t pos, uint64_t len) {
  // Typical case
  if (name != constants::coords || coord_tiles_[0].first.empty()) {
    const auto& tile = this->tile_pair(name)->first;
    auto cell_size = tile.cell_size();
    auto nbytes = len * cell_size;
    auto offset = pos * cell_size;
    return tile.read(buffer, nbytes, offset);
  }

  // Special case where zipped coordinates are requested, but the
  // result tile stores separate coordinates
  auto dim_num = coord_tiles_.size();
  assert(coords_tile_.first.empty());
  auto buff = (unsigned char*)buffer;
  auto buff_offset = 0;
  for (uint64_t c = 0; c < len; ++c) {
    for (unsigned d = 0; d < dim_num; ++d) {
      auto coord_tile = coord_tiles_[d].second.first;
      auto cell_size = coord_tile.cell_size();
      auto tile_offset = (pos + c) * cell_size;
      RETURN_NOT_OK(
          coord_tile.read(buff + buff_offset, cell_size, tile_offset));
      buff_offset += cell_size;
    }
  }

  return Status::Ok();
}

}  // namespace sm
}  // namespace tiledb
