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

#include <utility>
#include "tiledb/common/exception/exception.h"
#include "tiledb/common/heap_memory.h"
#include "tiledb/common/memory_tracker.h"
#include "tiledb/sm/enums/datatype.h"
#include "tiledb/storage_format/serialization/serializers.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/** Class for locally generated status exceptions. */
class TileException : public StatusException {
 public:
  explicit TileException(const std::string& msg)
      : StatusException("Tile", msg) {
  }
};
/* ****************************** */
/*           STATIC INIT          */
/* ****************************** */
uint64_t WriterTile::max_tile_chunk_size_ = constants::max_tile_chunk_size;

/* ****************************** */
/*           STATIC API           */
/* ****************************** */

shared_ptr<Tile> Tile::from_generic(
    storage_size_t tile_size, shared_ptr<MemoryTracker> memory_tracker) {
  return make_shared<Tile>(
      HERE(),
      0,
      constants::generic_tile_datatype,
      constants::generic_tile_cell_size,
      0,
      tile_size,
      nullptr,
      0,
      memory_tracker->get_resource(MemoryType::GENERIC_TILE_IO),
      ThreadPool::SharedTask(),
      nullptr);
}

shared_ptr<WriterTile> WriterTile::from_generic(
    storage_size_t tile_size, shared_ptr<MemoryTracker> memory_tracker) {
  return make_shared<WriterTile>(
      HERE(),
      0,
      constants::generic_tile_datatype,
      constants::generic_tile_cell_size,
      tile_size,
      memory_tracker->get_resource(MemoryType::GENERIC_TILE_IO));
}

uint32_t WriterTile::compute_chunk_size(
    const uint64_t tile_size, const uint64_t tile_cell_size) {
  const uint64_t dim_tile_size = tile_size;
  const uint64_t dim_cell_size = tile_cell_size;

  uint64_t chunk_size64 = std::min(max_tile_chunk_size_, dim_tile_size);
  chunk_size64 = chunk_size64 / dim_cell_size * dim_cell_size;
  chunk_size64 = std::max(chunk_size64, dim_cell_size);
  if (chunk_size64 > std::numeric_limits<uint32_t>::max()) {
    throw TileException("Chunk size exceeds uint32_t");
  }

  return static_cast<uint32_t>(chunk_size64);
}

/* ****************************** */
/*   CONSTRUCTORS & DESTRUCTORS   */
/* ****************************** */

TileBase::TileBase(
    const format_version_t format_version,
    const Datatype type,
    const uint64_t cell_size,
    const uint64_t size,
    tdb::pmr::memory_resource* resource,
    const bool ignore_tasks)
    : resource_(resource)
    , data_(tdb::pmr::make_unique<std::byte>(resource_, size))
    , size_(size)
    , cell_size_(cell_size)
    , format_version_(format_version)
    , type_(type)
    , ignore_tasks_(ignore_tasks) {
  /*
   * We can check for a bad allocation after initialization without risk
   * because none of the other member variables use its value for their own
   * initialization.
   */
  if (!data_) {
    throw std::bad_alloc();
  }
}

Tile::Tile(
    const format_version_t format_version,
    const Datatype type,
    const uint64_t cell_size,
    const unsigned int zipped_coords_dim_num,
    const uint64_t size,
    void* filtered_data,
    uint64_t filtered_size,
    shared_ptr<MemoryTracker> memory_tracker,
    ThreadPool::SharedTask data_io_task,
    shared_ptr<FilteredData> filtered_data_block)
    : Tile(
          format_version,
          type,
          cell_size,
          zipped_coords_dim_num,
          size,
          filtered_data,
          filtered_size,
          memory_tracker->get_resource(MemoryType::TILE_DATA),
          std::move(data_io_task),
          std::move(filtered_data_block)) {
}

Tile::Tile(
    const format_version_t format_version,
    const Datatype type,
    const uint64_t cell_size,
    const unsigned int zipped_coords_dim_num,
    const uint64_t size,
    void* filtered_data,
    uint64_t filtered_size,
    tdb::pmr::memory_resource* resource,
    ThreadPool::SharedTask filtered_data_io_task,
    shared_ptr<FilteredData> filtered_data_block)
    : TileBase(
          format_version,
          type,
          cell_size,
          size,
          resource,
          filtered_data_block == nullptr)
    , zipped_coords_dim_num_(zipped_coords_dim_num)
    , filtered_data_(filtered_data)
    , filtered_size_(filtered_size)
    , filtered_data_io_task_(std::move(filtered_data_io_task))
    , filtered_data_block_(std::move(filtered_data_block)) {
}

WriterTile::WriterTile(
    const format_version_t format_version,
    const Datatype type,
    const uint64_t cell_size,
    const uint64_t size,
    shared_ptr<MemoryTracker> memory_tracker)
    : TileBase(
          format_version,
          type,
          cell_size,
          size,
          memory_tracker->get_resource(MemoryType::WRITER_TILE_DATA),
          true)
    , filtered_buffer_(0) {
}

WriterTile::WriterTile(
    const format_version_t format_version,
    const Datatype type,
    const uint64_t cell_size,
    const uint64_t size,
    tdb::pmr::memory_resource* resource)
    : TileBase(format_version, type, cell_size, size, resource, true)
    , filtered_buffer_(0) {
}

/* ****************************** */
/*               API              */
/* ****************************** */

void TileBase::read(
    void* const buffer, const uint64_t offset, const uint64_t nbytes) const {
  if (!ignore_tasks_) {
    if (unfilter_data_compute_task_.valid()) {
      throw_if_not_ok(unfilter_data_compute_task_.wait());
    } else {
      throw std::future_error(std::future_errc::no_state);
    }
  }

  if (nbytes > size_ - offset) {
    throw TileException("Read tile overflow; may not read beyond buffer size");
  }
  std::memcpy(buffer, data_.get() + offset, nbytes);
}

void TileBase::write(const void* data, uint64_t offset, uint64_t nbytes) {
  if (nbytes > size_ - offset) {
    throw TileException("Write tile overflow; would write out of bounds");
  }

  std::memcpy(data_.get() + offset, data, nbytes);
  size_ = std::max(offset + nbytes, size_);
}

void Tile::zip_coordinates_unsafe() {
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
}

uint64_t Tile::load_chunk_data(ChunkData& chunk_data) {
  return load_chunk_data(chunk_data, size());
}

uint64_t Tile::load_offsets_chunk_data(ChunkData& chunk_data) {
  auto s = size();
  if (s < 8) {
    throw TileException("Offsets tile should at least be 8 bytes.");
  }

  return load_chunk_data(chunk_data, s - 8);
}

void WriterTile::clear_data() {
  data_ = nullptr;
  size_ = 0;
}

void Tile::set_unfilter_data_compute_task(
    ThreadPool::SharedTask unfilter_data_compute_task) {
  unfilter_data_compute_task_ = std::move(unfilter_data_compute_task);
}

void WriterTile::write_var(const void* data, uint64_t offset, uint64_t nbytes) {
  if (size_ - offset < nbytes) {
    auto new_alloc_size = size_ == 0 ? offset + nbytes : size_;
    while (new_alloc_size < offset + nbytes)
      new_alloc_size *= 2;

    auto new_data = tdb::pmr::make_unique<std::byte>(resource_, new_alloc_size);

    if (new_data == nullptr) {
      throw TileException("Cannot reallocate buffer; Memory allocation failed");
    }

    std::memcpy(new_data.get(), data_.get(), std::min(size_, new_alloc_size));

    data_ = std::move(new_data);
    size_ = new_alloc_size;
  }

  write(data, offset, nbytes);
}

/* ********************************* */
/*         PRIVATE FUNCTIONS         */
/* ********************************* */

uint64_t Tile::load_chunk_data(
    ChunkData& unfiltered_tile, uint64_t expected_original_size) {
  // std::scoped_lock<std::recursive_mutex> lock{filtered_data_io_task_mtx_};
  assert(filtered());

  Deserializer deserializer(filtered_data(), filtered_size());

  // Make a pass over the tile to get the chunk information.
  uint64_t num_chunks = deserializer.read<uint64_t>();

  auto& filtered_chunks = unfiltered_tile.filtered_chunks_;
  auto& chunk_offsets = unfiltered_tile.chunk_offsets_;
  filtered_chunks.resize(num_chunks);
  chunk_offsets.resize(num_chunks);
  uint64_t total_orig_size = 0;
  for (uint64_t i = 0; i < num_chunks; i++) {
    auto& chunk = filtered_chunks[i];
    chunk.unfiltered_data_size_ = deserializer.read<uint32_t>();
    chunk.filtered_data_size_ = deserializer.read<uint32_t>();
    chunk.filtered_metadata_size_ = deserializer.read<uint32_t>();
    chunk.filtered_metadata_ = const_cast<void*>(
        deserializer.get_ptr<void>(chunk.filtered_metadata_size_));
    chunk.filtered_data_ = const_cast<void*>(
        deserializer.get_ptr<void>(chunk.filtered_data_size_));

    chunk_offsets[i] = total_orig_size;
    total_orig_size += chunk.unfiltered_data_size_;
  }

  if (total_orig_size != expected_original_size) {
    throw TileException("Incorrect unfiltered tile size allocated.");
  }

  return total_orig_size;
}

}  // namespace sm
}  // namespace tiledb
