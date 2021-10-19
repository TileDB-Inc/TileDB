/**
 * @file   result_tile.cc
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
 * This file implements class ResultTile.
 */

#include "tiledb/sm/query/result_tile.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/array_schema/domain.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/sm/fragment/fragment_metadata.h"

#include <cassert>
#include <iostream>
#include <list>

using namespace tiledb::common;

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
  set_compute_results_func();

  // Default `coord_func_` to fetch from `coord_tile_` until at least
  // one unzipped coordinate has been initialized.
  coord_func_ = &ResultTile::zipped_coord;
}

/* ****************************** */
/*               API              */
/* ****************************** */

bool ResultTile::operator==(const ResultTile& rt) const {
  return frag_idx_ == rt.frag_idx_ && tile_idx_ == rt.tile_idx_;
}

uint64_t ResultTile::cell_num() const {
  if (!std::get<0>(coord_tiles_[0].second).empty())
    return std::get<0>(coord_tiles_[0].second).cell_num();

  if (!std::get<0>(coords_tile_).empty())
    return std::get<0>(coords_tile_).cell_num();

  if (!attr_tiles_.empty())
    return std::get<0>(attr_tiles_.begin()->second).cell_num();

  return 0;
}

const Domain* ResultTile::domain() const {
  return domain_;
}

void ResultTile::erase_tile(const std::string& name) {
  // Handle zipped coordinates tiles
  if (name == constants::coords) {
    coords_tile_ = TileTuple(Tile(), Tile(), Tile());
    return;
  }

  // Handle dimension tile
  for (auto& ct : coord_tiles_) {
    if (ct.first == name) {
      ct.second = TileTuple(Tile(), Tile(), Tile());
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
    attr_tiles_.emplace(name, TileTuple(Tile(), Tile(), Tile()));
}

void ResultTile::init_coord_tile(const std::string& name, unsigned dim_idx) {
  coord_tiles_[dim_idx] = std::pair<std::string, TileTuple>(
      name, TileTuple(Tile(), Tile(), Tile()));

  // When at least one unzipped coordinate has been initialized, we will
  // use the unzipped `coord()` implementation.
  coord_func_ = &ResultTile::unzipped_coord;
}

ResultTile::TileTuple* ResultTile::tile_tuple(const std::string& name) {
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

const void* ResultTile::unzipped_coord(uint64_t pos, unsigned dim_idx) const {
  const auto& coord_tile = std::get<0>(coord_tiles_[dim_idx].second);
  const uint64_t offset = pos * coord_tile.cell_size();
  Buffer* const buffer = coord_tile.buffer();
  void* const ret = static_cast<char*>(buffer->data()) + offset;
  return ret;
}

const void* ResultTile::zipped_coord(uint64_t pos, unsigned dim_idx) const {
  auto coords_size = std::get<0>(coords_tile_).cell_size();
  auto coord_size = coords_size / std::get<0>(coords_tile_).dim_num();
  const uint64_t offset = pos * coords_size + dim_idx * coord_size;
  Buffer* const buffer = std::get<0>(coords_tile_).buffer();
  void* const ret = static_cast<char*>(buffer->data()) + offset;
  return ret;
}

std::string ResultTile::coord_string(uint64_t pos, unsigned dim_idx) const {
  const auto& coord_tile_off = std::get<0>(coord_tiles_[dim_idx].second);
  const auto& coord_tile_val = std::get<1>(coord_tiles_[dim_idx].second);
  auto cell_num = coord_tile_off.cell_num();
  auto val_size = coord_tile_val.size();

  uint64_t offset = 0;
  Status st = coord_tile_off.buffer()->read(
      &offset, pos * sizeof(uint64_t), sizeof(uint64_t));
  assert(st.ok());

  uint64_t next_offset = 0;
  if (pos == cell_num - 1) {
    next_offset = val_size;
  } else {
    st = coord_tile_off.buffer()->read(
        &next_offset, (pos + 1) * sizeof(uint64_t), sizeof(uint64_t));
    assert(st.ok());
  }

  auto size = next_offset - offset;

  auto* buffer = static_cast<char*>(coord_tile_val.buffer()->data()) + offset;
  return std::string(buffer, size);
}

uint64_t ResultTile::coord_size(unsigned dim_idx) const {
  // Handle zipped coordinate tiles
  if (!std::get<0>(coords_tile_).empty())
    return std::get<0>(coords_tile_).cell_size() /
           std::get<0>(coords_tile_).dim_num();

  // Handle separate coordinate tiles
  assert(dim_idx < coord_tiles_.size());
  return std::get<0>(coord_tiles_[dim_idx].second).cell_size();
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
    const std::string& name,
    void* buffer,
    uint64_t buffer_offset,
    uint64_t pos,
    uint64_t len) {
  buffer = static_cast<char*>(buffer) + buffer_offset;

  bool is_dim = false;
  RETURN_NOT_OK(domain_->has_dimension(name, &is_dim));

  // Typical case
  // If asking for an attribute, or split dim buffers with split coordinates
  // or coordinates have been fetched as zipped
  if ((!is_dim && name != constants::coords) ||
      (is_dim && !coord_tiles_[0].first.empty()) ||
      (name == constants::coords && !std::get<0>(coords_tile_).empty())) {
    const auto& tile = std::get<0>(*this->tile_tuple(name));
    auto cell_size = tile.cell_size();
    auto nbytes = len * cell_size;
    auto offset = pos * cell_size;
    return tile.read(buffer, nbytes, offset);
  } else if (
      name == constants::coords && !coord_tiles_[0].first.empty() &&
      std::get<0>(coords_tile_).empty()) {
    // Special case where zipped coordinates are requested, but the
    // result tile stores separate coordinates
    auto dim_num = coord_tiles_.size();
    assert(std::get<0>(coords_tile_).empty());
    auto buff = static_cast<unsigned char*>(buffer);
    auto buff_offset = 0;
    for (uint64_t c = 0; c < len; ++c) {
      for (unsigned d = 0; d < dim_num; ++d) {
        auto coord_tile = std::get<0>(coord_tiles_[d].second);
        auto cell_size = coord_tile.cell_size();
        auto tile_offset = (pos + c) * cell_size;
        RETURN_NOT_OK(
            coord_tile.read(buff + buff_offset, cell_size, tile_offset));
        buff_offset += cell_size;
      }
    }
  } else {
    // Last case which is zipped coordinates but split buffers
    // This is only for backwards compatibility of pre format 5 (v2.0) arrays
    assert(!std::get<0>(coords_tile_).empty());
    assert(name != constants::coords);
    int dim_offset = 0;
    for (uint32_t i = 0; i < domain_->dim_num(); ++i) {
      if (domain_->dimension(i)->name() == name) {
        dim_offset = i;
        break;
      }
    }
    auto buff = static_cast<unsigned char*>(buffer);
    auto cell_size = std::get<0>(coords_tile_).cell_size();
    auto dim_size = cell_size / domain_->dim_num();
    uint64_t offset = pos * cell_size + dim_size * dim_offset;
    for (uint64_t c = 0; c < len; ++c) {
      RETURN_NOT_OK(std::get<0>(coords_tile_)
                        .read(buff + (c * dim_size), dim_size, offset));
      offset += cell_size;
    }
  };

  return Status::Ok();
}

Status ResultTile::read_nullable(
    const std::string& name,
    void* buffer,
    uint64_t buffer_offset,
    uint64_t pos,
    uint64_t len,
    void* buffer_validity) {
  const auto& tile = std::get<0>(*this->tile_tuple(name));
  const auto& tile_validity = std::get<2>(*this->tile_tuple(name));

  auto cell_size = tile.cell_size();
  auto validity_cell_size = tile_validity.cell_size();

  buffer = static_cast<char*>(buffer) + buffer_offset;
  buffer_validity = static_cast<char*>(buffer_validity) +
                    (buffer_offset / cell_size * validity_cell_size);

  auto nbytes = len * cell_size;
  auto offset = pos * cell_size;
  auto validity_nbytes = len * validity_cell_size;
  auto validity_offset = pos * validity_cell_size;

  RETURN_NOT_OK(tile.read(buffer, nbytes, offset));
  RETURN_NOT_OK(
      tile_validity.read(buffer_validity, validity_nbytes, validity_offset));

  return Status::Ok();
}

bool ResultTile::stores_zipped_coords() const {
  return !std::get<0>(coords_tile_).empty();
}

const Tile& ResultTile::zipped_coords_tile() const {
  assert(stores_zipped_coords());
  return std::get<0>(coords_tile_);
}

const ResultTile::TileTuple& ResultTile::coord_tile(unsigned dim_idx) const {
  assert(!stores_zipped_coords());
  assert(!coord_tiles_.empty());
  return coord_tiles_[dim_idx].second;
}

template <class T>
void ResultTile::compute_results_dense(
    const ResultTile* result_tile,
    unsigned dim_idx,
    const Range& range,
    const std::vector<tdb_shared_ptr<FragmentMetadata>> fragment_metadata,
    unsigned frag_idx,
    std::vector<uint8_t>* result_bitmap,
    std::vector<uint8_t>* overwritten_bitmap) {
  auto coords_num = result_tile->cell_num();
  auto frag_num = fragment_metadata.size();
  auto r = (const T*)range.data();
  auto stores_zipped_coords = result_tile->stores_zipped_coords();
  auto dim_num = result_tile->domain()->dim_num();
  auto& r_bitmap = (*result_bitmap);
  auto& o_bitmap = (*overwritten_bitmap);

  // Handle separate coordinate tiles
  if (!stores_zipped_coords) {
    const auto& coord_tile = std::get<0>(result_tile->coord_tile(dim_idx));

    Buffer* const buffer = coord_tile.buffer();
    auto coords = (const T*)buffer->data();
    T c;

    if (dim_idx == dim_num - 1) {
      // Last dimension, need to check for overwrites
      for (uint64_t pos = 0; pos < coords_num; ++pos) {
        c = coords[pos];
        r_bitmap[pos] &= (uint8_t)(c >= r[0] && c <= r[1]);

        // Check if overwritten only if it is a result
        if (r_bitmap[pos] == 1) {
          auto overwritten = false;
          for (auto f = frag_idx + 1; f < frag_num && !overwritten; ++f) {
            auto meta = fragment_metadata[f];
            if (meta->dense()) {
              overwritten = true;
              for (unsigned d = 0; d < dim_num; ++d) {
                const auto& coord_tile =
                    std::get<0>(result_tile->coord_tile(dim_idx));
                Buffer* const buffer = coord_tile.buffer();
                const T* const coords = static_cast<const T*>(buffer->data());
                auto c_d = coords[pos];
                auto dom = (const T*)meta->non_empty_domain()[d].data();
                if (c_d < dom[0] || c_d > dom[1]) {
                  overwritten = false;
                  break;
                }
              }
            }
          }
          o_bitmap[pos] = overwritten;
        }
      }
    } else {
      for (uint64_t pos = 0; pos < coords_num; ++pos) {
        c = coords[pos];
        r_bitmap[pos] &= (uint8_t)(c >= r[0] && c <= r[1]);
      }
    }

    return;
  }

  // Handle zipped coordinates tile
  assert(stores_zipped_coords);
  const auto& coords_tile = result_tile->zipped_coords_tile();
  Buffer* const buffer = coords_tile.buffer();
  auto coords = (const T*)buffer->data();
  T c;

  if (dim_idx == dim_num - 1) {
    for (uint64_t pos = 0; pos < coords_num; ++pos) {
      // Check if result
      c = coords[pos * dim_num + dim_idx];
      r_bitmap[pos] &= (uint8_t)(c >= r[0] && c <= r[1]);

      // Check if overwritten
      if ((*result_bitmap)[pos] == 1) {
        auto overwritten = false;
        for (auto f = frag_idx + 1; f < frag_num && !overwritten; ++f) {
          auto meta = fragment_metadata[f];
          if (meta->dense()) {
            overwritten = true;
            for (unsigned d = 0; d < dim_num; ++d) {
              auto c_d = coords[pos * dim_num + d];
              auto dom = (const T*)meta->non_empty_domain()[d].data();
              if (c_d < dom[0] || c_d > dom[1]) {
                overwritten = false;
                break;
              }
            }
          }
        }
        o_bitmap[pos] = overwritten;
      }
    }
  } else {
    for (uint64_t pos = 0; pos < coords_num; ++pos) {
      c = coords[pos * dim_num + dim_idx];
      r_bitmap[pos] &= (uint8_t)(c >= r[0] && c <= r[1]);
    }
  }
}

inline uint8_t ResultTile::str_coord_intersects(
    const uint64_t c_offset,
    const uint64_t c_size,
    const char* const buff_str,
    const std::string& range_start,
    const std::string& range_end) {
  // Test against start
  bool geq_start = true;
  bool all_chars_match = true;
  uint64_t min_size = std::min<uint64_t>(c_size, range_start.size());
  for (uint64_t i = 0; i < min_size; ++i) {
    if (buff_str[c_offset + i] < range_start[i]) {
      geq_start = false;
      all_chars_match = false;
      break;
    } else if (buff_str[c_offset + i] > range_start[i]) {
      all_chars_match = false;
      break;
    }  // Else characters match
  }
  if (geq_start && all_chars_match && c_size < range_start.size()) {
    geq_start = false;
  }

  // Test against end
  bool seq_end = false;
  if (geq_start) {
    seq_end = true;
    all_chars_match = true;
    min_size = std::min<uint64_t>(c_size, range_end.size());
    for (uint64_t i = 0; i < min_size; ++i) {
      if (buff_str[c_offset + i] > range_end[i]) {
        geq_start = false;
        break;
      } else if (buff_str[c_offset + i] < range_end[i]) {
        all_chars_match = false;
        break;
      }  // Else characters match
    }
    if (seq_end && all_chars_match && c_size > range_end.size()) {
      seq_end = false;
    }
  }

  return geq_start && seq_end;
}

template <>
void ResultTile::compute_results_sparse<char>(
    const ResultTile* result_tile,
    unsigned dim_idx,
    const Range& range,
    std::vector<uint8_t>* result_bitmap,
    const Layout& cell_order) {
  auto coords_num = result_tile->cell_num();
  auto dim_num = result_tile->domain()->dim_num();
  auto range_start = range.start_str();
  auto range_end = range.end_str();
  auto& r_bitmap = (*result_bitmap);

  // Sanity check.
  assert(coords_num != 0);
  if (coords_num == 0)
    return;

  // Get coordinate tile
  const auto& coord_tile = result_tile->coord_tile(dim_idx);

  // Get offset buffer
  const auto& coord_tile_off = std::get<0>(coord_tile);
  auto buff_off = static_cast<const uint64_t*>(coord_tile_off.buffer()->data());

  // Get string buffer
  const auto& coord_tile_str = std::get<1>(coord_tile);
  auto buff_str = static_cast<const char*>(coord_tile_str.buffer()->data());
  auto buff_str_size = coord_tile_str.size();

  // For row-major cell orders, the first dimension is sorted.
  // For col-major cell orders, the last dimension is sorted.
  // If the coordinate value at index 0 is equivalent to the
  // coordinate value at index 100, we know that  all coordinates
  // between them are also equivalent. In this scenario where
  // coordinate value i=0 matches the coordinate value at i=100,
  // we can calculate `r_bitmap[0]` and apply assign its value to
  // `r_bitmap[i]` where i in the range [1, 100].
  //
  // Calculating `r_bitmap[i]` is expensive for strings because
  // it invokes a string comparison. We partition the coordinates
  // into a fixed number of ranges. For each partition, we will
  // compare the start and ending coordinate values to see if
  // they are equivalent. If so, we save the expense of computing
  // `r_bitmap` for each corresponding coordinate. If they do
  // not match, we must compare each coordinate in the partition.
  static const uint64_t c_partition_num = 6;
  // We calculate the size of each partition by dividing the total
  // number of coordinates by the number of partitions. If this
  // does not evenly divide, we will append the remaining coordinates
  // onto the last partition.
  const uint64_t c_partition_size_div = coords_num / c_partition_num;
  const uint64_t c_partition_size_rem = coords_num % c_partition_num;
  const bool is_sorted_dim =
      ((cell_order == Layout::ROW_MAJOR && dim_idx == 0) ||
       (cell_order == Layout::COL_MAJOR && dim_idx == dim_num - 1));
  if (is_sorted_dim && c_partition_size_div > 1 &&
      coords_num > c_partition_num) {
    // Loop over each coordinate partition.
    for (uint64_t p = 0; p < c_partition_num; ++p) {
      // Calculate the size of this partition.
      const uint64_t c_partition_size =
          c_partition_size_div +
          (p == (c_partition_num - 1) ? c_partition_size_rem : 0);

      // Calculate the position of the first and last coordinates
      // in this partition.
      const uint64_t first_c_pos = p * c_partition_size_div;
      const uint64_t last_c_pos = first_c_pos + c_partition_size - 1;
      assert(first_c_pos < last_c_pos);

      // The coordinate values are determined by their offset and size
      // within `buff_str`. Calculate the offset and size for the
      // first and last coordinates in this partition.
      const uint64_t first_c_offset = buff_off[first_c_pos];
      const uint64_t last_c_offset = buff_off[last_c_pos];
      const uint64_t first_c_size = buff_off[first_c_pos + 1] - first_c_offset;
      const uint64_t last_c_size = (last_c_pos == coords_num - 1) ?
                                       buff_str_size - last_c_offset :
                                       buff_off[last_c_pos + 1] - last_c_offset;

      // Fetch the coordinate values for the first and last coordinates
      // in this partition.
      const char* const first_c_coord = &buff_str[first_c_offset];
      const char* const second_coord = &buff_str[last_c_offset];

      // Check if the first and last coordinates have an identical
      // string value.
      if (first_c_size == last_c_size &&
          strncmp(first_c_coord, second_coord, first_c_size) == 0) {
        assert(r_bitmap[first_c_pos] == 1);
        const uint8_t intersects = str_coord_intersects(
            first_c_offset, first_c_size, buff_str, range_start, range_end);
        memset(&r_bitmap[first_c_pos], intersects, c_partition_size);
      } else {
        // Compute results
        uint64_t c_offset = 0, c_size = 0;
        for (uint64_t pos = first_c_pos; pos <= last_c_pos; ++pos) {
          c_offset = buff_off[pos];
          c_size = (pos < coords_num - 1) ? buff_off[pos + 1] - c_offset :
                                            buff_str_size - c_offset;
          r_bitmap[pos] = str_coord_intersects(
              c_offset, c_size, buff_str, range_start, range_end);
        }
      }
    }

    // We've calculated `r_bitmap` for all coordinate values in
    // the sorted dimension.
    return;
  }

  // Here, we're computing on all other dimensions. After the first
  // dimension, it is possible that coordinates have already been determined
  // to not intersect with the range in an earlier dimension. For example,
  // if `dim_idx` is 2 for a row-major cell order, we have already checked
  // this coordinate for an intersection in dimension-0 and dimension-1. If
  // either did not intersect a coordindate, its associated value in
  // `r_bitmap` will be zero. To optimize for the scenario where there are
  // many contiguous zeroes, we can `memcmp` a large range of `r_bitmap`
  // values to avoid a comparison on each individual element.
  //
  // Often, a memory comparison for one byte is as quick as comparing
  // 4 or 8 bytes. We will only get a benefit if we successfully
  // find a `memcmp` on a much larger range.
  const uint64_t zeroed_size = coords_num < 256 ? coords_num : 256;
  for (uint64_t i = 0; i < coords_num; i += zeroed_size) {
    const uint64_t partition_size =
        (i < coords_num - zeroed_size) ? zeroed_size : coords_num - i;

    // Check if all `r_bitmap` values are zero between `i` and
    // `partition_size`.
    if (r_bitmap[i] == 0 &&
        memcmp(&r_bitmap[i], &r_bitmap[i + 1], partition_size - 1) == 0)
      continue;

    // Here, we know that there is at least one `1` value within
    // the `r_bitmap` values within this partition. We must check
    // each value for an intersection.
    uint64_t c_offset = 0, c_size = 0;
    for (uint64_t pos = i; pos < i + partition_size; ++pos) {
      if (r_bitmap[pos] == 0)
        continue;

      c_offset = buff_off[pos];
      c_size = (pos < coords_num - 1) ? buff_off[pos + 1] - c_offset :
                                        buff_str_size - c_offset;
      r_bitmap[pos] = str_coord_intersects(
          c_offset, c_size, buff_str, range_start, range_end);
    }
  }
}

template <class T>
void ResultTile::compute_results_sparse(
    const ResultTile* result_tile,
    unsigned dim_idx,
    const Range& range,
    std::vector<uint8_t>* result_bitmap,
    const Layout& cell_order) {
  // We do not use `cell_order` for this template type.
  (void)cell_order;

  // For easy reference.
  auto coords_num = result_tile->cell_num();
  auto r = (const T*)range.data();
  auto stores_zipped_coords = result_tile->stores_zipped_coords();
  auto dim_num = result_tile->domain()->dim_num();
  auto& r_bitmap = (*result_bitmap);

  // Variables for the loop over `coords_num`.
  T c;
  const T& r0 = r[0];
  const T& r1 = r[1];

  // Handle separate coordinate tiles
  if (!stores_zipped_coords) {
    const auto& coord_tile = std::get<0>(result_tile->coord_tile(dim_idx));
    Buffer* const buffer = coord_tile.buffer();
    const T* const coords = static_cast<const T*>(buffer->data());
    for (uint64_t pos = 0; pos < coords_num; ++pos) {
      c = coords[pos];
      r_bitmap[pos] &= (uint8_t)(c >= r0 && c <= r1);
    }

    return;
  }

  // Handle zipped coordinates tile
  assert(stores_zipped_coords);
  const auto& coords_tile = result_tile->zipped_coords_tile();
  Buffer* const buffer = coords_tile.buffer();
  const T* const coords = static_cast<const T*>(buffer->data());
  for (uint64_t pos = 0; pos < coords_num; ++pos) {
    c = coords[pos * dim_num + dim_idx];
    r_bitmap[pos] &= (uint8_t)(c >= r0 && c <= r1);
  }
}

Status ResultTile::compute_results_dense(
    unsigned dim_idx,
    const Range& range,
    const std::vector<tdb_shared_ptr<FragmentMetadata>> fragment_metadata,
    unsigned frag_idx,
    std::vector<uint8_t>* result_bitmap,
    std::vector<uint8_t>* overwritten_bitmap) const {
  assert(compute_results_dense_func_[dim_idx] != nullptr);
  compute_results_dense_func_[dim_idx](
      this,
      dim_idx,
      range,
      fragment_metadata,
      frag_idx,
      result_bitmap,
      overwritten_bitmap);
  return Status::Ok();
}

Status ResultTile::compute_results_sparse(
    unsigned dim_idx,
    const Range& range,
    std::vector<uint8_t>* result_bitmap,
    const Layout& cell_order) const {
  assert(compute_results_sparse_func_[dim_idx] != nullptr);
  compute_results_sparse_func_[dim_idx](
      this, dim_idx, range, result_bitmap, cell_order);
  return Status::Ok();
}

/* ****************************** */
/*         PRIVATE METHODS        */
/* ****************************** */

void ResultTile::set_compute_results_func() {
  auto dim_num = domain_->dim_num();
  compute_results_dense_func_.resize(dim_num);
  compute_results_sparse_func_.resize(dim_num);
  for (unsigned d = 0; d < dim_num; ++d) {
    auto dim = domain_->dimension(d);
    switch (dim->type()) {
      case Datatype::INT32:
        compute_results_dense_func_[d] = compute_results_dense<int32_t>;
        compute_results_sparse_func_[d] = compute_results_sparse<int32_t>;
        break;
      case Datatype::INT64:
        compute_results_dense_func_[d] = compute_results_dense<int64_t>;
        compute_results_sparse_func_[d] = compute_results_sparse<int64_t>;
        break;
      case Datatype::INT8:
        compute_results_dense_func_[d] = compute_results_dense<int8_t>;
        compute_results_sparse_func_[d] = compute_results_sparse<int8_t>;
        break;
      case Datatype::UINT8:
        compute_results_dense_func_[d] = compute_results_dense<uint8_t>;
        compute_results_sparse_func_[d] = compute_results_sparse<uint8_t>;
        break;
      case Datatype::INT16:
        compute_results_dense_func_[d] = compute_results_dense<int16_t>;
        compute_results_sparse_func_[d] = compute_results_sparse<int16_t>;
        break;
      case Datatype::UINT16:
        compute_results_dense_func_[d] = compute_results_dense<uint16_t>;
        compute_results_sparse_func_[d] = compute_results_sparse<uint16_t>;
        break;
      case Datatype::UINT32:
        compute_results_dense_func_[d] = compute_results_dense<uint32_t>;
        compute_results_sparse_func_[d] = compute_results_sparse<uint32_t>;
        break;
      case Datatype::UINT64:
        compute_results_dense_func_[d] = compute_results_dense<uint64_t>;
        compute_results_sparse_func_[d] = compute_results_sparse<uint64_t>;
        break;
      case Datatype::FLOAT32:
        compute_results_dense_func_[d] = compute_results_dense<float>;
        compute_results_sparse_func_[d] = compute_results_sparse<float>;
        break;
      case Datatype::FLOAT64:
        compute_results_dense_func_[d] = compute_results_dense<double>;
        compute_results_sparse_func_[d] = compute_results_sparse<double>;
        break;
      case Datatype::DATETIME_YEAR:
      case Datatype::DATETIME_MONTH:
      case Datatype::DATETIME_WEEK:
      case Datatype::DATETIME_DAY:
      case Datatype::DATETIME_HR:
      case Datatype::DATETIME_MIN:
      case Datatype::DATETIME_SEC:
      case Datatype::DATETIME_MS:
      case Datatype::DATETIME_US:
      case Datatype::DATETIME_NS:
      case Datatype::DATETIME_PS:
      case Datatype::DATETIME_FS:
      case Datatype::DATETIME_AS:
      case Datatype::TIME_HR:
      case Datatype::TIME_MIN:
      case Datatype::TIME_SEC:
      case Datatype::TIME_MS:
      case Datatype::TIME_US:
      case Datatype::TIME_NS:
      case Datatype::TIME_PS:
      case Datatype::TIME_FS:
      case Datatype::TIME_AS:
        compute_results_dense_func_[d] = compute_results_dense<int64_t>;
        compute_results_sparse_func_[d] = compute_results_sparse<int64_t>;
        break;
      case Datatype::STRING_ASCII:
        compute_results_dense_func_[d] = nullptr;
        compute_results_sparse_func_[d] = compute_results_sparse<char>;
        break;
      default:
        compute_results_dense_func_[d] = nullptr;
        compute_results_sparse_func_[d] = nullptr;
        break;
    }
  }
}

}  // namespace sm
}  // namespace tiledb
