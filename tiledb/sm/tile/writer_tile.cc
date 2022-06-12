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
    const bool has_coords,
    const bool var_size,
    const bool nullable,
    const uint64_t cell_size,
    const Datatype type)
    : var_tile_(var_size ? std::optional<Tile>(Tile()) : std::nullopt)
    , validity_tile_(nullable ? std::optional<Tile>(Tile()) : std::nullopt)
    , cell_size_(cell_size)
    , var_pre_filtered_size_(0)
    , min_size_(0)
    , max_size_(0)
    , null_count_(0) {
  auto& domain{array_schema.domain()};
  auto capacity = array_schema.capacity();
  auto cell_num_per_tile = has_coords ? capacity : domain.cell_num_per_tile();

  if (var_size) {
    auto tile_size = cell_num_per_tile * constants::cell_var_offset_size;

    // Initialize
    if (!fixed_tile_
             .init_unfiltered(
                 array_schema.write_version(),
                 constants::cell_var_offset_type,
                 tile_size,
                 constants::cell_var_offset_size,
                 0)
             .ok()) {
      throw std::logic_error(
          "Could not initialize offset tile in WriterTile constructor");
    }
    if (!var_tile_
             ->init_unfiltered(
                 array_schema.write_version(),
                 type,
                 tile_size,
                 datatype_size(type),
                 0)
             .ok()) {
      throw std::logic_error(
          "Could not initialize var tile in WriterTile constructor");
    }
  } else {
    auto tile_size = cell_num_per_tile * cell_size;

    // Initialize
    if (!fixed_tile_
             .init_unfiltered(
                 array_schema.write_version(), type, tile_size, cell_size, 0)
             .ok()) {
      throw std::logic_error(
          "Could not initialize fixed tile in WriterTile constructor");
    }
  }

  if (nullable) {
    if (!validity_tile_
             ->init_unfiltered(
                 array_schema.write_version(),
                 constants::cell_validity_type,
                 cell_num_per_tile * constants::cell_validity_size,
                 constants::cell_validity_size,
                 0)
             .ok()) {
      throw std::logic_error(
          "Could not initialize validity tile in WriterTile constructor");
    }
  }
}

WriterTile::WriterTile(WriterTile&& tile)
    : WriterTile() {
  // Swap with the argument
  swap(tile);
}

WriterTile& WriterTile::operator=(WriterTile&& tile) {
  // Swap with the argument
  swap(tile);

  return *this;
}

/* ****************************** */
/*               API              */
/* ****************************** */

uint64_t WriterTile::var_pre_filtered_size() const {
  return var_pre_filtered_size_;
}

void* WriterTile::min() const {
  return (void*)min_.data();
}

void* WriterTile::max() const {
  return (void*)max_.data();
}

tuple<const void*, uint64_t, const void*, uint64_t, const ByteVec*, uint64_t>
WriterTile::metadata() const {
  return {min_.data(), min_size_, max_.data(), max_size_, &sum_, null_count_};
}

void WriterTile::set_metadata(const tuple<
                              const void*,
                              uint64_t,
                              const void*,
                              uint64_t,
                              const ByteVec*,
                              uint64_t>& md) {
  const auto& [min, min_size, max, max_size, sum, null_count] = md;
  assert(sum != nullptr);

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

  sum_ = *sum;
  null_count_ = null_count;

  if (var_tile_.has_value()) {
    var_pre_filtered_size_ = var_tile_->size();
  }
}

void WriterTile::swap(WriterTile& tile) {
  fixed_tile_.swap(tile.fixed_tile_);
  var_tile_.swap(tile.var_tile_);
  validity_tile_.swap(tile.validity_tile_);
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
