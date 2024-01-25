/**
 * @file   cell_slab.h
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
 * This file defines struct CellSlab.
 */

#ifndef TILEDB_CELL_SLAB_H
#define TILEDB_CELL_SLAB_H

#include <cstdint>
#include <cstring>
#include <iostream>
#include <vector>

namespace tiledb {
namespace sm {

/** The `CellSlabIter` iterator returns cell slabs of this form. */
template <class T>
struct CellSlab {
  /**
   * The (global) coordinates of the tile the cell slab belongs to.
   * Note that, for efficiency purporses, all tile coordinates are computed
   * once and maintained in a single place. Instances of this struct
   * only store constant pointers to those tile coordinates.
   */
  const T* tile_coords_ = nullptr;
  /** The (global) coordinates of the cell slab start. */
  std::vector<T> coords_;
  /** The cell slab length. */
  uint64_t length_ = UINT64_MAX;

  /** Default constructor. */
  CellSlab() = default;

  /** Default destructor. */
  ~CellSlab() = default;

  /** Constructor. */
  CellSlab(const T* tile_coords, const std::vector<T>& coords, uint64_t length)
      : tile_coords_(tile_coords)
      , coords_(coords)
      , length_(length) {
  }

  /** Default copy constructor. */
  CellSlab(const CellSlab&) = default;

  /** Default move constructor. */
  CellSlab(CellSlab&&) = default;

  /** Default copy-assign operator. */
  CellSlab& operator=(const CellSlab&) = default;

  /** Default move-assign operator. */
  CellSlab& operator=(CellSlab&&) = default;

  /** Simple initializer. */
  void init(unsigned int dim_num) {
    tile_coords_ = nullptr;
    coords_.resize(dim_num);
    length_ = UINT64_MAX;
  }

  /** Resets the cell sab. */
  void reset() {
    tile_coords_ = nullptr;
    coords_.clear();
    length_ = UINT64_MAX;
  }

  /** Equality operator. */
  bool operator==(const CellSlab& cell_slab) const {
    auto coords_size = cell_slab.coords_.size() * sizeof(T);
    bool tile_coords_match =
        (cell_slab.tile_coords_ == nullptr && tile_coords_ == nullptr) ||
        (cell_slab.tile_coords_ != nullptr && tile_coords_ != nullptr &&
         !std::memcmp(cell_slab.tile_coords_, tile_coords_, coords_size));
    return (
        tile_coords_match && cell_slab.coords_ == coords_ &&
        cell_slab.length_ == length_);
  }

  /** For debugging. */
  void print() const {
    auto dim_num = coords_.size();
    std::cout << "tile coords: ";
    if (tile_coords_ == nullptr) {
      std::cout << "null";
    } else {
      for (unsigned i = 0; i < dim_num; ++i)
        std::cout << tile_coords_[i] << " ";
    }
    std::cout << "\ncell coords: ";
    for (const auto& c : coords_)
      std::cout << c << " ";
    std::cout << "\nlength: " << length_ << "\n";
  }
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_CELL_SLAB_H
