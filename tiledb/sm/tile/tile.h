/**
 * @file   tile.h
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
 * This file defines class Tile.
 */

#ifndef TILEDB_TILE_H
#define TILEDB_TILE_H

#include "tiledb/common/common.h"
#include "tiledb/common/pmr.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/storage_manager/context_resources.h"
#include "tiledb/sm/tile/filtered_buffer.h"
#include "tiledb/storage_format/serialization/serializers.h"

#include <cinttypes>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class FilteredData;
class MemoryTracker;

/**
 * Base class for common code between Tile and WriterTile objects.
 */
class TileBase {
 public:
  /**
   * Constructor.
   *
   * @param format_version The format version.
   * @param type The data type.
   * @param cell_size The cell size.
   * @param size The size of the tile.
   * @param resource The memory resource to use.
   * @param data_io_task The I/O task to wait on for data to be valid.
   */
  TileBase(
      const format_version_t format_version,
      const Datatype type,
      const uint64_t cell_size,
      const uint64_t size,
      tdb::pmr::memory_resource* resource,
      ContextResources* resources,
      const bool ignore_tasks = false);

  DISABLE_COPY_AND_COPY_ASSIGN(TileBase);
  DISABLE_MOVE_AND_MOVE_ASSIGN(TileBase);

  ~TileBase() {
    // TODO: destructor should not throw, catch any exceptions
    std::scoped_lock<std::recursive_mutex> lock{
        unfilter_data_compute_task_mtx_};
    if (unfilter_data_compute_task_.valid()) {
      auto st = resources_->compute_tp().wait(&unfilter_data_compute_task_);
    }
  }

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the tile data type. */
  inline Datatype type() const {
    return type_;
  }

  /** Gets the format version number of the data in this Tile. */
  inline uint32_t format_version() const {
    return format_version_;
  }

  /** Converts the data pointer to a specific type. */
  template <class T>
  inline T* data_as() const {
    return static_cast<T*>(data());
  }

  /** Converts the data pointer to a specific type with no check on compute
   * task. This is used for getting thte data from inside the compute thread
   * itself for unfiltering. */
  template <class T>
  inline T* data_as_unsafe() const {
    return static_cast<T*>(data_unsafe());
  }

  /** Gets the size, considering the data as a specific type. */
  template <class T>
  inline size_t size_as() const {
    return size() / sizeof(T);
  }

  /** Returns the internal buffer. */
  inline void* data() const {
    if (!ignore_tasks_) {
      std::scoped_lock<std::recursive_mutex> lock{
          unfilter_data_compute_task_mtx_};
      if (unfilter_data_compute_task_.valid()) {
        throw_if_not_ok(
            resources_->compute_tp().wait(&unfilter_data_compute_task_));
      } else {
        throw std::future_error(
            std::make_error_code(std::future_errc::no_state));
      }
    }

    return data_.get();
  }

  /** Returns the internal buffer. This is used for getting thte data from
   * inside the compute thread itself for unfiltering. */
  inline void* data_unsafe() const {
    return data_.get();
  }

  /**
   * Reads from the tile at the given offset into the input
   * buffer of size nbytes. Does not mutate the internal offset.
   * Thread-safe among readers.
   */
  void read(void* buffer, uint64_t offset, uint64_t nbytes) const;

  /** Returns the tile size. */
  inline uint64_t size() const {
    return (data_ == nullptr) ? 0 : size_;
  }

  /** Returns the cell size. */
  inline uint64_t cell_size() const {
    return cell_size_;
  }

  /**
   * Writes `nbytes` from `data` to the tile at `offset`.
   *
   * @note This function assumes that the tile buffer has already been
   *     properly allocated. It does not alter the tile offset and size.
   */
  void write(const void* data, uint64_t offset, uint64_t nbytes);

  /**
   * Adds an extra offset at the end of this tile representing the size of the
   * var tile.
   *
   * @param var_tile Var tile.
   */
  void add_extra_offset_unsafe(TileBase& var_tile) {
    data_as_unsafe<uint64_t>()[size_ / cell_size_ - 1] = var_tile.size();
  }

 protected:
  /* ********************************* */
  /*       PROTECTED ATTRIBUTES        */
  /* ********************************* */

  /** The memory resource to use. */
  tdb::pmr::memory_resource* resource_;

  /** The ContextResources object. */
  ContextResources* resources_;

  /** The buffer backing the tile data. */
  tdb::pmr::unique_ptr<std::byte> data_;

  /** Size of the data. */
  uint64_t size_;

  /** The cell size. */
  uint64_t cell_size_;

  /** The format version of the data in this tile. */
  format_version_t format_version_;

  /** The tile data type. */
  Datatype type_;

  const bool ignore_tasks_;

  /** Compute task to check and block on if unfiltered data is ready. */
  mutable ThreadPool::SharedTask unfilter_data_compute_task_;

  /**
   * Lock for checking task, since this tile can be used by multiple threads.
   * The ThreadPool::SharedTask lets multiple threads copy the task, but it
   * doesn't let multiple threads access a single task itself. Due to this we
   * need a mutext since the tile will be accessed by multiple threads.
   */
  mutable std::recursive_mutex unfilter_data_compute_task_mtx_;
};

/**
 * Tile object used for read operations.
 */
class Tile : public TileBase {
 public:
  /**
   * returns a Tile initialized with parameters commonly used for
   * generic data storage.
   *
   * @param tile_size to be provided to init_unfiltered call
   */
  static shared_ptr<Tile> from_generic(
      storage_size_t tile_size, shared_ptr<MemoryTracker> memory_tracker);

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor.
   *
   * @param format_version The format version.
   * @param type The data type.
   * @param cell_size The cell size.
   * @param zipped_coords_dim_num The number of dimensions in case the tile
   *      stores coordinates.
   * @param size The size of the tile.
   * @param filtered_data Pointer to the external filtered data.
   * @param filtered_size The filtered size to allocate.
   * @param memory_tracker The memory resource to use.
   * @param resources The context resources.
   * @param filtered_data_io_task The I/O task to wait on for data to be valid.
   * @param filtered_data_block The FilteredData block class which backs the
   * memory for this filtered tile.
   */
  Tile(
      const format_version_t format_version,
      const Datatype type,
      const uint64_t cell_size,
      const unsigned int zipped_coords_dim_num,
      const uint64_t size,
      void* filtered_data,
      uint64_t filtered_size,
      shared_ptr<MemoryTracker> memory_tracker,
      ContextResources* resources,
      ThreadPool::SharedTask filtered_data_io_task,
      shared_ptr<FilteredData> filtered_data_block);

  /**
   * Constructor.
   *
   * @param format_version The format version.
   * @param type The data type.
   * @param cell_size The cell size.
   * @param zipped_coords_dim_num The number of dimensions in case the tile
   *      stores coordinates.
   * @param size The size of the tile.
   * @param filtered_data Pointer to the external filtered data.
   * @param filtered_size The filtered size to allocate.
   * @param resource The memory resource to use.
   * @param resources The context resources.
   * @param filtered_data_io_task The I/O task to wait on for data to be valid.
   * @param filtered_data_block The FilteredData block class which backs the
   * memory for this filtered tile.
   */
  Tile(
      const format_version_t format_version,
      const Datatype type,
      const uint64_t cell_size,
      const unsigned int zipped_coords_dim_num,
      const uint64_t size,
      void* filtered_data,
      uint64_t filtered_size,
      tdb::pmr::memory_resource* resource,
      ContextResources* resources,
      ThreadPool::SharedTask filtered_data_io_task,
      shared_ptr<FilteredData> filtered_data_block);

  DISABLE_MOVE_AND_MOVE_ASSIGN(Tile);
  DISABLE_COPY_AND_COPY_ASSIGN(Tile);

  ~Tile() {
    // TODO: destructor should not throw, catch any exceptions
    std::scoped_lock<std::recursive_mutex> lock{
        unfilter_data_compute_task_mtx_};
    if (unfilter_data_compute_task_.valid()) {
      auto st = resources_->compute_tp().wait(&unfilter_data_compute_task_);
    }
  }

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns *true* if the tile stores zipped coordinates. */
  inline bool stores_coords() const {
    return zipped_coords_dim_num_ > 0;
  }

  /** Returns the number of zipped coordinates (0 if this is an attribute tile).
   */
  inline unsigned int zipped_coords_dim_num() const {
    return zipped_coords_dim_num_;
  }

  /**
   * Returns the current filtered state of the tile data in the buffer. When
   * `true`, the buffer contains the filtered, on-disk format of the tile.
   */
  inline bool filtered() const {
    return filtered_size_ > 0;
  }

  /** Returns the buffer that contains the filtered, on-disk format. */
  inline char* filtered_data() {
    // if an i/o task has been launched
    if (filtered_data_block_ != nullptr) {
      std::scoped_lock<std::recursive_mutex> lock{filtered_data_io_task_mtx_};
      if (filtered_data_io_task_.valid()) {
        throw_if_not_ok(resources_->compute_tp().wait(&filtered_data_io_task_));
      } else {
        throw std::future_error(
            std::make_error_code(std::future_errc::no_state));
      }
    }

    return static_cast<char*>(filtered_data_);
  }

  /** Returns the data casted as a type. */
  template <class T>
  inline T* filtered_data_as() {
    // if an i/o task has been launched
    if (filtered_data_block_ != nullptr) {
      std::scoped_lock<std::recursive_mutex> lock{filtered_data_io_task_mtx_};
      if (filtered_data_io_task_.valid()) {
        throw_if_not_ok(resources_->compute_tp().wait(&filtered_data_io_task_));
      } else {
        throw std::future_error(
            std::make_error_code(std::future_errc::no_state));
      }
    }

    return static_cast<T*>(filtered_data_);
  }

  /** Clears the filtered buffer. */
  void clear_filtered_buffer() {
    if (filtered_data_block_ != nullptr) {
      std::scoped_lock<std::recursive_mutex> lock{filtered_data_io_task_mtx_};
      if (filtered_data_io_task_.valid()) {
        throw_if_not_ok(resources_->compute_tp().wait(&filtered_data_io_task_));
      } else {
        throw std::future_error(
            std::make_error_code(std::future_errc::no_state));
      }
    }

    filtered_data_ = nullptr;
    filtered_size_ = 0;
    filtered_data_block_ = nullptr;
  }

  /**
   * Returns the buffer size.
   */
  inline uint64_t filtered_size() {
    return filtered_size_;
  }

  /**
   * Zips the coordinate values such that a cell's coordinates across
   * all dimensions appear contiguously in the buffer.
   *
   * This is marked unsafe because we don't check for unfiltering to be
   * completed since this function is used by the unfiltering task itself as
   * part of post processing.
   */
  void zip_coordinates_unsafe();

  /**
   * Reads the chunk data of a tile buffer and populates a chunk data structure.
   *
   * @param chunk_data Tile chunk info, buffers and offsets.
   * @return Original size.
   */
  uint64_t load_chunk_data(ChunkData& chunk_data);

  /**
   * Reads the chunk data of a tile offsets buffer and populates a chunk data
   * structure.
   *
   * @param chunk_data Tile chunk info, buffers and offsets.
   * @return Original size.
   */
  uint64_t load_offsets_chunk_data(ChunkData& chunk_data);

  /**
   * Set task for filter pipeline unfiltering to allow async monitoring
   *
   * @param unfilter_data_compute_task task for unfiltering
   */
  void set_unfilter_data_compute_task(
      ThreadPool::SharedTask unfilter_data_compute_task);

 private:
  /* ********************************* */
  /*         PRIVATE FUNCTIONS         */
  /* ********************************* */

  /**
   * Reads the chunk data of a tile buffer and populates a chunk data structure.
   *
   * This expects as input a Tile in the following byte format:
   *
   *   number_of_chunks (uint64_t)
   *   chunk0_orig_len (uint32_t)
   *   chunk0_data_len (uint32_t)
   *   chunk0_metadata_len (uint32_t)
   *   chunk0_metadata (uint8_t[])
   *   chunk0_data (uint8_t[])
   *   chunk1_orig_len (uint32_t)
   *   chunk1_data_len (uint32_t)
   *   chunk1_metadata_len (uint32_t)
   *   chunk1_metadata (uint8_t[])
   *   chunk1_data (uint8_t[])
   *   ...
   *   chunkN_orig_len (uint32_t)
   *   chunkN_data_len (uint32_t)
   *   chunkN_metadata_len (uint32_t)
   *   chunkN_metadata (uint8_t[])
   *   chunkN_data (uint8_t[])
   *
   * @param chunk_data Tile chunk info, buffers and offsets.
   * @param expected_original_size Expected size for the tile.
   * @return Original size.
   */
  uint64_t load_chunk_data(
      ChunkData& chunk_data, uint64_t expected_original_size);

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /**
   * The number of dimensions, in case the tile stores zipped coordinates.
   * It is 0 in case the tile stores attributes or other type of dimensions
   */
  unsigned int zipped_coords_dim_num_;

  /**
   * The buffer that contains the filtered, on-disk bytes. This buffer is
   * exclusively used in the I/O path between the disk and the filter
   * pipeline. Note that this buffer is _only_ accessed in the filtered()
   * public API, all other public API routines operate on 'buffer_'.
   */
  void* filtered_data_;

  /** The size of the filtered data. */
  uint64_t filtered_size_;

  /** I/O task to check and block on if filtered data is ready. */
  mutable ThreadPool::SharedTask filtered_data_io_task_;

  /**
   * Lock for checking task, since this tile can be used by multiple threads.
   * The ThreadPool::SharedTask lets multiple threads copy the task, but it
   * doesn't let multiple threads access a single task itself. Due to this we
   * need a mutex since the tile will be accessed by multiple threads.
   */
  mutable std::recursive_mutex filtered_data_io_task_mtx_;

  /**
   * shared_ptr to the FilteredData class that backs this tile. We keep a shared
   * pointer to maintain the lifetime.
   */
  shared_ptr<FilteredData> filtered_data_block_;
};

/**
 * Tile object for write operations.
 */
class WriterTile : public TileBase {
  /**
   * Allow access to max_tile_chunk_size_ for testing.
   */
  friend class WhiteboxWriterTile;

 public:
  /**
   * returns a Tile initialized with parameters commonly used for
   * generic data storage.
   *
   * @param tile_size to be provided to init_unfiltered call
   * @param memory_tracker The memory tracker to use.
   */
  static shared_ptr<WriterTile> from_generic(
      storage_size_t tile_size, shared_ptr<MemoryTracker> memory_tracker);

  /**
   * Computes the chunk size for a tile.
   *
   * @param tile_size The total size of the tile.
   * @param tile_cell_size The cell size.
   * @return Chunk size.
   */
  static uint32_t compute_chunk_size(
      const uint64_t tile_size, const uint64_t tile_cell_size);

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor.
   *
   * @param format_version The format version.
   * @param type The data type.
   * @param cell_size The cell size.
   * @param size The size of the tile.
   * @param memory_tracker The memory tracker to use.
   * @param resources The context resources.
   */
  WriterTile(
      const format_version_t format_version,
      const Datatype type,
      const uint64_t cell_size,
      const uint64_t size,
      shared_ptr<MemoryTracker> memory_tracker,
      ContextResources* resources);

  /**
   * Constructor.
   *
   * @param format_version The format version.
   * @param type The data type.
   * @param cell_size The cell size.
   * @param size The size of the tile.
   * @param resource The memory resource to use.
   * @param resources The context resources.
   */
  WriterTile(
      const format_version_t format_version,
      const Datatype type,
      const uint64_t cell_size,
      const uint64_t size,
      tdb::pmr::memory_resource* resource,
      ContextResources* resources);

  DISABLE_COPY_AND_COPY_ASSIGN(WriterTile);
  DISABLE_MOVE_AND_MOVE_ASSIGN(WriterTile);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the number of cells stored in the tile. */
  uint64_t cell_num() const {
    return size() / cell_size_;
  }

  /** Clears the internal buffer. */
  void clear_data();

  /**
   * Returns the current filtered state of the tile data in the buffer. When
   * `true`, the buffer contains the filtered, on-disk format of the tile.
   */
  inline bool filtered() const {
    return filtered_buffer_.size() > 0;
  }

  /**
   * Returns the buffer that contains the filtered, on-disk format.
   */
  inline FilteredBuffer& filtered_buffer() {
    return filtered_buffer_;
  }

  /**
   * Write method used for var data. Resizes the internal buffer if needed.
   *
   * @param data Pointer to the data to write.
   * @param offset Offset to write into the tile buffer.
   * @param nbytes Number of bytes to write.
   */
  void write_var(const void* data, uint64_t offset, uint64_t nbytes);

  /**
   * Sets the size of the tile.
   *
   * @param size The new size.
   */
  inline void set_size(uint64_t size) {
    size_ = size;
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /**
   * The buffer that contains the filtered, on-disk bytes. This buffer is
   * exclusively used in the I/O path between the disk and the filter
   * pipeline. Note that this buffer is _only_ accessed in the filtered()
   * public API, all other public API routines operate on 'buffer_'.
   */
  FilteredBuffer filtered_buffer_;

  /**
   * Static variable to store constants::max_tile_chunk_size. This will be used
   * to override the value in tests.
   */
  static uint64_t max_tile_chunk_size_;
};

/**
 * A deserializer that owns a Tile.
 */
class TileDeserializer : public Deserializer {
 public:
  explicit TileDeserializer(shared_ptr<Tile> tile)
      : Deserializer(tile->data(), tile->size())
      , tile_(tile) {
  }

 private:
  shared_ptr<Tile> tile_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_TILE_H
