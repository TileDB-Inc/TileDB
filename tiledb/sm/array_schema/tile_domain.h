/**
 * @file   tile_domain.h
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
 * This file defines struct TileDomain.
 */

#ifndef TILEDB_TILE_DOMAIN_H
#define TILEDB_TILE_DOMAIN_H

#include <algorithm>
#include <vector>

#include "tiledb/common/assert.h"
#include "tiledb/common/pmr.h"
#include "tiledb/sm/array_schema/dimension.h"
#include "tiledb/sm/enums/layout.h"
#include "tiledb/sm/misc/types.h"

namespace tiledb {
namespace sm {

/**
 * The tile domain of some ND domain (single-range) slice, with
 * respect to another domain, is a logical space for tiles, where
 * each tile is identified by unique coordinates. This class offers
 * functionality around creating tile domains based on array domain
 * slices and computing 1D-mapped tile positions.
 *
 * For instance, assume 2D `domain` `[1,4], [1,4]` with tile extent 2
 * along both dimensions, and `domain_slice` `[1,2], [1,4]`. The tile
 * domain of `domain` is `[0,1], [0,1]` (since there are two tiles
 * along each dimension), whereas the tile domain of `domain_slice`
 * is `[0,0], [0,1]`, since it covers only the first two rows, i.e.,
 * only one tile slab along the row dimension.
 */
template <class T>
class TileDomain {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Default constructor. */
  TileDomain() = default;

  /**
   * Constructor.
   *
   * @param id An identifier given to this tile domain.
   * @param domain The domain.
   * @param domain_slice The domain slice (included in `domain`).
   * @param tile_extents The tile extents of the domains.
   * @param layout The layout of the tiles in the tile domain.
   *     Only row-major and col-major are supported.
   */
  TileDomain(
      unsigned id,
      const NDRange& domain,
      const NDRange& domain_slice,
      std::span<const ByteVecValue> tile_extents,
      Layout layout)
      : id_(id)
      , dim_num_((unsigned)domain.size())
      , domain_(domain)
      , domain_slice_(domain_slice)
      , tile_extents_(tile_extents.begin(), tile_extents.end())
      , layout_(layout) {
    iassert(
        layout == Layout::ROW_MAJOR || layout == Layout::COL_MAJOR,
        "layout = {}",
        layout_str(layout));
    compute_tile_domain(domain, domain_slice, tile_extents);
    if (layout == Layout::ROW_MAJOR)
      compute_tile_offsets_row();
    else
      compute_tile_offsets_col();
  }

  /** Default destructor. */
  ~TileDomain() = default;

  /** Default copy constructor. */
  TileDomain(const TileDomain&) = default;

  /** Default move constructor. */
  TileDomain(TileDomain&&) = default;

  /** Default copy-assign operator. */
  TileDomain& operator=(const TileDomain&) = default;

  /** Default move-assign operator. */
  TileDomain& operator=(TileDomain&&) = default;

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

  /** Returns the number of dimensions. */
  unsigned dim_num() const {
    return dim_num_;
  }

  /** Returns the id of the tile domain. */
  unsigned id() const {
    return id_;
  }

  /**
   * Returns the global coordinates of the first cell in the tile
   * with the input coordinates.
   */
  vector_ndim<T> start_coords(const T* tile_coords) const {
    vector_ndim<T> ret;
    ret.resize(dim_num_);
    for (unsigned d = 0; d < dim_num_; ++d) {
      auto dim_dom = (const T*)domain_[d].data();
      auto tile_extent = *(const T*)tile_extents_[d].data();
      ret[d] =
          Dimension::tile_coord_low(tile_coords[d], dim_dom[0], tile_extent);
    }

    return ret;
  }

  /**
   * Returns `true` if the input tile coordinates reside in the
   * tile domain of the instance.
   */
  bool in_tile_domain(const T* tile_coords) const {
    for (unsigned i = 0; i < dim_num_; ++i) {
      if (tile_coords[i] < tile_domain_[2 * i] ||
          tile_coords[i] > tile_domain_[2 * i + 1])
        return false;
    }

    return true;
  }

  /**
   * Returns the global subarray corresponding to the tile with
   * the input coordinates.
   */
  vector_2ndim<T> tile_subarray(const T* tile_coords) const {
    vector_2ndim<T> ret;
    ret.resize(2 * dim_num_);

    for (unsigned d = 0; d < dim_num_; ++d) {
      auto dim_dom = (const T*)domain_[d].data();
      auto tile_extent = *(const T*)tile_extents_[d].data();
      ret[2 * d] =
          Dimension::tile_coord_low(tile_coords[d], dim_dom[0], tile_extent);
      ret[2 * d + 1] =
          Dimension::tile_coord_high(tile_coords[d], dim_dom[0], tile_extent);
    }

    return ret;
  }

  /**
   * Returns the tile overlap (as a global subarray slice) between the
   * domain slice of the instance and the tile identified by `tile_coords`.
   *
   * If there is no overlap, then the returned vector is empty.
   */
  vector_2ndim<T> tile_overlap(const T* tile_coords) const {
    vector_2ndim<T> ret;

    // Return empty if the tile is not in the tile domain
    if (!in_tile_domain(tile_coords))
      return ret;

    // Get overlap
    ret.resize(2 * dim_num_);
    auto tile_subarray = this->tile_subarray(tile_coords);
    for (unsigned d = 0; d < dim_num_; ++d) {
      auto ds = (const T*)domain_slice_[d].data();
      ret[2 * d] = std::max(tile_subarray[2 * d], ds[0]);
      ret[2 * d + 1] = std::min(tile_subarray[2 * d + 1], ds[1]);
    }

    return ret;
  }

  /**
   * Returns `true` if the instance's domain slice covers completely
   * that of `tile_domain` for the tile identified by `tile_coords`.
   */
  bool covers(const T* tile_coords, const TileDomain& tile_domain) const {
    if (!in_tile_domain(tile_coords) ||
        !tile_domain.in_tile_domain(tile_coords))
      return false;

    auto tile_overlap_1 = this->tile_overlap(tile_coords);
    auto tile_overlap_2 = tile_domain.tile_overlap(tile_coords);
    iassert(tile_overlap_1.size() == tile_overlap_2.size());
    iassert(tile_overlap_1.size() == 2 * dim_num_);
    for (unsigned i = 0; i < dim_num_; ++i) {
      if (tile_overlap_2[2 * i] < tile_overlap_1[2 * i] ||
          tile_overlap_2[2 * i + 1] > tile_overlap_1[2 * i + 1])
        return false;
    }

    return true;
  }

  /**
   * Given the input tile coordinates, this function produces an
   * 1D-mapped tile position, based on the layout of the tile
   * domain. Note that the returned tile position will be normalized
   * to reflect the position within the tile domain. That is,
   * if the tile (say 1D)domain is `[5,8]`, and the tile coords
   * are `6`, then the returned position will be `1` (starting
   * always from 0).
   *
   * If the input tile coordinates are not inside the tile domain,
   * the function returns `UINT64_MAX` (for invalid position).
   */
  uint64_t tile_pos(const T* tile_coords) const {
    uint64_t pos = 0;
    for (unsigned int i = 0; i < dim_num_; ++i) {
      if (tile_coords[i] < tile_domain_[2 * i] ||
          tile_coords[i] > tile_domain_[2 * i + 1]) {
        return UINT64_MAX;
      }
      pos += (tile_coords[i] - tile_domain_[2 * i]) * tile_offsets_[i];
    }
    return pos;
  }

  /** Returns the tile domain. */
  std::span<const T> tile_domain() const {
    return tile_domain_;
  }

  /** Returns the domain slice. */
  const NDRange& domain_slice() const {
    return domain_slice_;
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /**
   * A unique identifier (usually the fragment id - the larger,
   * the more recent the fragment).
   */
  unsigned id_;

  /** The number of dimensions of the tile domain. */
  unsigned dim_num_ = 0;

  /** The global domain the tiles are defined over. */
  NDRange domain_;

  /** The domain slice from which the tile domain is constructed. */
  NDRange domain_slice_;

  /** The tile extents. */
  vector_ndim<ByteVecValue> tile_extents_;

  /** The layout used to compute 1D-mapped tile positions. */
  Layout layout_ = Layout::GLOBAL_ORDER;

  /** The tile domain. */
  vector_2ndim<T> tile_domain_;

  /**
   * Auxiliary offsets for efficiently computing 1D-mapped tile
   * positions from tile coordinates.
   */
  vector_ndim<T> tile_offsets_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /**
   * Computes the tile domain given `domain`, `domain_slice` and
   * `tile_extents`.
   */
  void compute_tile_domain(
      const NDRange& domain,
      const NDRange& domain_slice,
      std::span<const ByteVecValue> tile_extents) {
    tile_domain_.resize(2 * dim_num_);
    for (unsigned d = 0; d < dim_num_; ++d) {
      auto ds = (const T*)domain_slice[d].data();
      auto dim_dom = (const T*)domain[d].data();
      auto tile_extent = *(const T*)tile_extents[d].data();
      iassert(ds[0] <= ds[1]);
      iassert(ds[0] >= dim_dom[0] && ds[1] <= dim_dom[1]);
      tile_domain_[2 * d] = Dimension::tile_idx(ds[0], dim_dom[0], tile_extent);
      tile_domain_[2 * d + 1] =
          Dimension::tile_idx(ds[1], dim_dom[0], tile_extent);
    }
  }

  /**
   * Computes the auxiliary tile offsets given the tile domain, assuming
   * a col-major order for the tiles in the tile domain.
   */
  void compute_tile_offsets_col() {
    iassert(std::is_integral<T>::value);

    tile_offsets_.reserve(dim_num_);
    tile_offsets_.push_back(1);
    for (unsigned int i = 1; i < dim_num_; ++i) {
      auto tile_num =
          tile_domain_[2 * (i - 1) + 1] - tile_domain_[2 * (i - 1)] + 1;
      tile_offsets_.push_back(tile_offsets_.back() * tile_num);
    }
  }

  /**
   * Computes the auxiliary tile offsets given the tile domain, assuming
   * a row-major order for the tiles in the tile domain.
   */
  void compute_tile_offsets_row() {
    iassert(std::is_integral<T>::value);

    tile_offsets_.reserve(dim_num_);
    tile_offsets_.push_back(1);
    if (dim_num_ > 1) {
      for (unsigned int i = dim_num_ - 2;; --i) {
        auto tile_num =
            tile_domain_[2 * (i + 1) + 1] - tile_domain_[2 * (i + 1)] + 1;
        tile_offsets_.push_back(tile_offsets_.back() * tile_num);
        if (i == 0)
          break;
      }
    }

    std::reverse(tile_offsets_.begin(), tile_offsets_.end());
  }
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_TILE_DOMAIN_H
