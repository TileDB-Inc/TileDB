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
#include "tiledb/common/status.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/tile/filtered_buffer.h"

#include <cinttypes>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class WhiteboxTile;

/**
 * Handles tile information. A tile can be in main memory if it has been
 * fetched from the disk or has been mmap-ed from a file. However, a tile
 * can be solely on the disk, in which case the Tile object stores the
 * offset in the file where the tile data begin, plus the tile size.
 */
class Tile {
  friend class WhiteboxTile;

 public:
  /**
   * Computes the chunk size for a tile.
   *
   * @param tile_size The total size of the tile.
   * @param tile_dim_num The number of coordinate dimensions.
   * @param tile_cell_size The cell size.
   * @param chunk_size Mutates to the calculated chunk size.
   * @return Status
   */
  static Status compute_chunk_size(
      const uint64_t tile_size,
      const uint32_t tile_dim_num,
      const uint64_t tile_cell_size,
      uint32_t* const chunk_size);

  /**
   * Override max_tile_chunk_size_. Used in tests only.
   *
   * @param max_tile_chunk_size The maximum chunk size.
   */
  static void set_max_tile_chunk_size(uint64_t max_tile_chunk_size);

  /**
   * returns a Tile initialized with parameters commonly used for
   * generic data storage.
   *
   * @param tile_size to be provided to init_unfiltered call
   */
  static Tile from_generic(storage_size_t tile_size);

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
   * @param filtered_size The filtered size to allocate.
   */
  Tile(
      const uint32_t format_version,
      const Datatype type,
      const uint64_t cell_size,
      const unsigned int zipped_coords_dim_num,
      const uint64_t size,
      const uint64_t filtered_size);

  /**
   * Constructor.
   *
   * @param type The type of the data to be stored.
   * @param cell_size The cell size.
   * @param dim_num The number of dimensions in case the tile stores
   *      coordinates.
   * @param buffer The buffer to be encapsulated by the tile object. The
   * tile will not take ownership of the buffer.
   * @param size The buffer size.
   */
  Tile(
      Datatype type,
      uint64_t cell_size,
      unsigned int dim_num,
      void* buffer,
      uint64_t size);

  /** Move constructor. */
  Tile(Tile&& tile);

  /** Move-assign operator. */
  Tile& operator=(Tile&& tile);

  DISABLE_COPY_AND_COPY_ASSIGN(Tile);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Converts the data pointer to a specific type. */
  template <class T>
  inline T* data_as() const {
    return static_cast<T*>(data());
  }

  /** Gets the size, considering the data as a specific type. */
  template <class T>
  inline size_t size_as() const {
    return size() / sizeof(T);
  }

  /** Returns the number of cells stored in the tile. */
  uint64_t cell_num() const;

  /** Returns the internal buffer. */
  inline void* data() const {
    return data_.get();
  }

  /** Clears the internal buffer. */
  void clear_data();

  /** Returns the cell size. */
  inline uint64_t cell_size() const {
    return cell_size_;
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
    return filtered_buffer_.size() > 0;
  }

  /**
   * Returns the buffer that contains the filtered, on-disk format.
   */
  inline FilteredBuffer& filtered_buffer() {
    return filtered_buffer_;
  }

  /** Gets the format version number of the data in this Tile. */
  inline uint32_t format_version() const {
    return format_version_;
  }

  /**
   * Reads from the tile at the given offset into the input
   * buffer of size nbytes. Does not mutate the internal offset.
   * Thread-safe among readers.
   */
  Status read(void* buffer, uint64_t offset, uint64_t nbytes) const;

  /** Returns the tile size. */
  inline uint64_t size() const {
    return (data_ == nullptr) ? 0 : size_;
  }

  /** Returns *true* if the tile stores zipped coordinates. */
  inline bool stores_coords() const {
    return zipped_coords_dim_num_ > 0;
  }

  /** Returns the tile data type. */
  inline Datatype type() const {
    return type_;
  }

  /**
   * Writes `nbytes` from `data` to the tile at `offset`.
   *
   * @note This function assumes that the tile buffer has already been
   *     properly allocated. It does not alter the tile offset and size.
   */
  Status write(const void* data, uint64_t offset, uint64_t nbytes);

  /**
   * Write method used for var data. Resizes the internal buffer if needed.
   *
   * @param data Pointer to the data to write.
   * @param offset Offset to write into the tile buffer.
   * @param nbytes Number of bytes to write.
   * @return Status.
   */
  Status write_var(const void* data, uint64_t offset, uint64_t nbytes);

  /**
   * Zips the coordinate values such that a cell's coordinates across
   * all dimensions appear contiguously in the buffer.
   */
  Status zip_coordinates();

  /**
   * Sets the size of the tile.
   *
   * @param size The new size.
   */
  inline void set_size(uint64_t size) {
    size_ = size;
  }

  /** Swaps the contents (all field values) of this tile with the given tile. */
  void swap(Tile& tile);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /**
   * The buffer backing the tile data.
   *
   * TODO: Convert to regular allocations once tdb_realloc is not used for var
   * size data anymore and remove custom deleter.
   */
  std::unique_ptr<char, void (*)(void*)> data_;

  /** Size of the data. */
  uint64_t size_;

  /** The cell size. */
  uint64_t cell_size_;

  /**
   * The number of dimensions, in case the tile stores zipped coordinates.
   * It is 0 in case the tile stores attributes or other type of dimensions
   */
  unsigned int zipped_coords_dim_num_;

  /** The format version of the data in this tile. */
  uint32_t format_version_;

  /** The tile data type. */
  Datatype type_;

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

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_TILE_H
