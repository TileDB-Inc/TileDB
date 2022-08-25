/**
 * @file   tile.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2022 TileDB, Inc.
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
 * This file implements class Tile.
 */

#include "tiledb/sm/tile/tile.h"
#include "tiledb/common/heap_memory.h"
#include "tiledb/common/logger.h"
#include "tiledb/sm/enums/datatype.h"

#include <iostream>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class TileStatusException : public StatusException {
 public:
  explicit TileStatusException(const std::string& message)
      : StatusException("Tile", message) {
  }
};

void nop_free(void* const) {
}

/* ****************************** */
/*           STATIC INIT          */
/* ****************************** */
uint64_t Tile::max_tile_chunk_size_ = constants::max_tile_chunk_size;

/* ****************************** */
/*           STATIC API           */
/* ****************************** */

Status Tile::compute_chunk_size(
    const uint64_t tile_size,
    const uint32_t tile_dim_num,
    const uint64_t tile_cell_size,
    uint32_t* const chunk_size) {
  const uint32_t dim_num = tile_dim_num > 0 ? tile_dim_num : 1;
  const uint64_t dim_tile_size = tile_size / dim_num;
  const uint64_t dim_cell_size = tile_cell_size / dim_num;

  uint64_t chunk_size64 = std::min(max_tile_chunk_size_, dim_tile_size);
  chunk_size64 = chunk_size64 / dim_cell_size * dim_cell_size;
  chunk_size64 = std::max(chunk_size64, dim_cell_size);
  if (chunk_size64 > std::numeric_limits<uint32_t>::max()) {
    return LOG_STATUS(Status_TileError("Chunk size exceeds uint32_t"));
  }

  *chunk_size = chunk_size64;
  return Status::Ok();
}

void Tile::set_max_tile_chunk_size(uint64_t max_tile_chunk_size) {
  max_tile_chunk_size_ = max_tile_chunk_size;
}

Tile Tile::from_generic(storage_size_t tile_size) {
  return {0,
          constants::generic_tile_datatype,
          constants::generic_tile_cell_size,
          0,
          tile_size,
          0};
}

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

Tile::Tile(
    const uint32_t format_version,
    const Datatype type,
    const uint64_t cell_size,
    const unsigned int zipped_coords_dim_num,
    const uint64_t size,
    const uint64_t filtered_size)
    : data_(static_cast<char*>(tdb_malloc(size)), tiledb_free)
    , size_(size)
    , cell_size_(cell_size)
    , zipped_coords_dim_num_(zipped_coords_dim_num)
    , format_version_(format_version)
    , type_(type)
    , filtered_buffer_(filtered_size) {
}

Tile::Tile(
    const Datatype type,
    const uint64_t cell_size,
    const unsigned int zipped_coords_dim_num,
    void* const buffer,
    uint64_t size)
    : data_(static_cast<char*>(buffer), nop_free)
    , size_(size)
    , cell_size_(cell_size)
    , zipped_coords_dim_num_(zipped_coords_dim_num)
    , format_version_(0)
    , type_(type)
    , filtered_buffer_(0) {
}

Tile::Tile(Tile&& tile)
    : data_(std::move(tile.data_))
    , size_(std::move(tile.size_))
    , cell_size_(std::move(tile.cell_size_))
    , zipped_coords_dim_num_(std::move(tile.zipped_coords_dim_num_))
    , format_version_(std::move(tile.format_version_))
    , type_(std::move(tile.type_))
    , filtered_buffer_(std::move(tile.filtered_buffer_)) {
}

Tile& Tile::operator=(Tile&& tile) {
  // Swap with the argument
  swap(tile);

  return *this;
}

/* ****************************** */
/*               API              */
/* ****************************** */

uint64_t Tile::cell_num() const {
  return size() / cell_size_;
}

void Tile::clear_data() {
  data_ = nullptr;
  size_ = 0;
}

Status Tile::read(
    void* const buffer, const uint64_t offset, const uint64_t nbytes) const {
  assert(!filtered());
  if (nbytes > size_ - offset) {
    return LOG_STATUS(Status_TileError(
        "Read tile overflow; may not read beyond buffer size"));
  }
  std::memcpy(buffer, data_.get() + offset, nbytes);
  return Status::Ok();
}

Status Tile::write(const void* data, uint64_t offset, uint64_t nbytes) {
  assert(!filtered());
  if (nbytes > size_ - offset) {
    return LOG_STATUS(
        Status_TileError("Write tile overflow; would write out of bounds"));
  }

  std::memcpy(data_.get() + offset, data, nbytes);
  size_ = std::max(offset + nbytes, size_);

  return Status::Ok();
}

Status Tile::write_var(const void* data, uint64_t offset, uint64_t nbytes) {
  if (size_ - offset < nbytes) {
    auto new_alloc_size = size_ == 0 ? offset + nbytes : size_;
    while (new_alloc_size < offset + nbytes)
      new_alloc_size *= 2;

    auto new_data =
        static_cast<char*>(tdb_realloc(data_.release(), new_alloc_size));
    if (new_data == nullptr) {
      return LOG_STATUS(Status_TileError(
          "Cannot reallocate buffer; Memory allocation failed"));
    }
    data_.reset(new_data);
    size_ = new_alloc_size;
  }

  return write(data, offset, nbytes);
}

Status Tile::zip_coordinates() {
  assert(zipped_coords_dim_num_ > 0);

  // For easy reference
  const uint64_t tile_size = size_;
  const uint64_t coord_size = cell_size_ / zipped_coords_dim_num_;
  const uint64_t cell_num = tile_size / cell_size_;

  // Create a tile clone
  char* const tile_tmp = static_cast<char*>(tdb_malloc(tile_size));
  assert(tile_tmp);
  std::memcpy(tile_tmp, data_.get(), tile_size);

  // Zip coordinates
  uint64_t ptr_tmp = 0;
  for (unsigned int j = 0; j < zipped_coords_dim_num_; ++j) {
    uint64_t ptr = j * coord_size;
    for (uint64_t i = 0; i < cell_num; ++i) {
      std::memcpy(data_.get() + ptr, tile_tmp + ptr_tmp, coord_size);
      ptr += cell_size_;
      ptr_tmp += coord_size;
    }
  }

  // Clean up
  tdb_free((void*)tile_tmp);

  return Status::Ok();
}

void Tile::swap(Tile& tile) {
  // Note swapping buffer pointers here.
  std::swap(filtered_buffer_, tile.filtered_buffer_);
  std::swap(size_, tile.size_);
  std::swap(data_, tile.data_);
  std::swap(cell_size_, tile.cell_size_);
  std::swap(zipped_coords_dim_num_, tile.zipped_coords_dim_num_);
  std::swap(format_version_, tile.format_version_);
  std::swap(type_, tile.type_);
}

}  // namespace sm
}  // namespace tiledb
