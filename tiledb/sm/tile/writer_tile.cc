/**
 * @file   writer_tile.cc
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
 * This file implements class WriterTile.
 */

#include "tiledb/sm/tile/writer_tile.h"
#include "tiledb/sm/array_schema/domain.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

WriterTile::WriterTile(
    const ArraySchema& array_schema,
    const uint64_t cell_num_per_tile,
    const bool var_size,
    const bool nullable,
    const uint64_t cell_size,
    const Datatype type)
    : fixed_tile_(
          var_size ? Tile(
                         array_schema.write_version(),
                         constants::cell_var_offset_type,
                         constants::cell_var_offset_size,
                         0,
                         cell_num_per_tile * constants::cell_var_offset_size,
                         0) :
                     Tile(
                         array_schema.write_version(),
                         type,
                         cell_size,
                         0,
                         cell_num_per_tile * cell_size,
                         0))
    , var_tile_(
          var_size ? std::optional<Tile>(Tile(
                         array_schema.write_version(),
                         type,
                         datatype_size(type),
                         0,
                         cell_num_per_tile * constants::cell_var_offset_size,
                         0)) :
                     std::nullopt)
    , validity_tile_(
          nullable ? std::optional<Tile>(Tile(
                         array_schema.write_version(),
                         constants::cell_validity_type,
                         constants::cell_validity_size,
                         0,
                         cell_num_per_tile * constants::cell_validity_size,
                         0)) :
                     std::nullopt)
    , cell_size_(cell_size)
    , var_pre_filtered_size_(0)
    , min_size_(0)
    , max_size_(0)
    , null_count_(0) {
}

WriterTile::WriterTile(WriterTile&& tile)
    : fixed_tile_(std::move(tile.fixed_tile_))
    , var_tile_(std::move(tile.var_tile_))
    , validity_tile_(std::move(tile.validity_tile_))
    , cell_size_(std::move(tile.cell_size_))
    , var_pre_filtered_size_(std::move(tile.var_pre_filtered_size_))
    , min_(std::move(tile.min_))
    , min_size_(std::move(tile.min_size_))
    , max_(std::move(tile.max_))
    , max_size_(std::move(tile.max_size_))
    , sum_(std::move(tile.sum_))
    , null_count_(std::move(tile.null_count_)) {
}

WriterTile& WriterTile::operator=(WriterTile&& tile) {
  // Swap with the argument
  swap(tile);

  return *this;
}

/* ****************************** */
/*               API              */
/* ****************************** */

void WriterTile::set_metadata(
    const void* min,
    const uint64_t min_size,
    const void* max,
    const uint64_t max_size,
    const ByteVec& sum,
    const uint64_t null_count) {
  min_.resize(min_size);
  min_size_ = min_size;
  if (min != nullptr) {
    memcpy(min_.data(), min, min_size);
  }

  max_.resize(max_size);
  max_size_ = max_size;
  if (max != nullptr) {
    memcpy(max_.data(), max, max_size);
  }

  sum_ = sum;
  null_count_ = null_count;

  if (var_tile_.has_value()) {
    var_pre_filtered_size_ = var_tile_->size();
  }
}

void WriterTile::swap(WriterTile& tile) {
  std::swap(fixed_tile_, tile.fixed_tile_);
  std::swap(var_tile_, tile.var_tile_);
  std::swap(validity_tile_, tile.validity_tile_);
  std::swap(cell_size_, tile.cell_size_);
  std::swap(var_pre_filtered_size_, tile.var_pre_filtered_size_);
  std::swap(min_, tile.min_);
  std::swap(min_size_, tile.min_size_);
  std::swap(max_, tile.max_);
  std::swap(max_size_, tile.max_size_);
  std::swap(sum_, tile.sum_);
  std::swap(null_count_, tile.null_count_);
}

}  // namespace sm
}  // namespace tiledb
