/**
 * @file   result_cell_slab.h
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

#ifndef TILEDB_RESULT_CELL_SLAB_H
#define TILEDB_RESULT_CELL_SLAB_H

#include <iostream>
#include <vector>

#include "tiledb/sm/query/readers/result_tile.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/** The `ResultCellSlabIter` iterator returns cell slabs of this form. */
// TODO: YEET (EDIT THIS TO TEMPLATE IT)
struct ResultCellSlab {
  /**
   * The result tile the cell slab belongs to. If `nullptr`, then this is
   * an "empty" cell range, to be filled with the default empty
   * values.
   *
   * Note that the tile this points to is allocated and freed in
   * sparse_read/dense_read, so the lifetime of this struct must not exceed
   * the scope of those functions.
   */
  ResultTile<ContextResources::resource_manager_type>* tile_;
  /** The cell position where the slab starts. */
  uint64_t start_;
  /** The length of the slab (i.e., the number of cells in the slab). */
  uint64_t length_;

  /** Constructor. */
  ResultCellSlab() {
    tile_ = nullptr;
    start_ = UINT64_MAX;
    length_ = UINT64_MAX;
  }

  /** Constructor. */
  ResultCellSlab(ResultTile<ContextResources::resource_manager_type>* tile, uint64_t start, uint64_t length)
      : tile_(tile)
      , start_(start)
      , length_(length) {
  }

  /** Default destructor. */
  ~ResultCellSlab() = default;

  /** Copy constructor. */
  ResultCellSlab(const ResultCellSlab& result_cell_slab)
      : ResultCellSlab() {
    auto clone = result_cell_slab.clone();
    swap(clone);
  }

  /** Move constructor. */
  ResultCellSlab(ResultCellSlab&& result_cell_slab) noexcept
      : ResultCellSlab() {
    swap(result_cell_slab);
  }

  /** Copy-assign operator. */
  ResultCellSlab& operator=(const ResultCellSlab& result_cell_slab) {
    auto clone = result_cell_slab.clone();
    swap(clone);
    return *this;
  }

  /** Move-assign operator. */
  ResultCellSlab& operator=(ResultCellSlab&& result_cell_slab) noexcept {
    swap(result_cell_slab);
    return *this;
  }

  /** Comparator. Returns `true` if the instance precedes `a`. */
  bool operator<(const ResultCellSlab& a) const {
    return start_ < a.start_;
  }

  /** Clones the instance. */
  ResultCellSlab clone() const {
    ResultCellSlab clone;
    clone.tile_ = tile_;
    clone.start_ = start_;
    clone.length_ = length_;
    return clone;
  }

  /** Swaps members with another instance. */
  void swap(ResultCellSlab& result_cell_slab) {
    std::swap(tile_, result_cell_slab.tile_);
    std::swap(start_, result_cell_slab.start_);
    std::swap(length_, result_cell_slab.length_);
  }
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_RESULT_CELL_SLAB_H
