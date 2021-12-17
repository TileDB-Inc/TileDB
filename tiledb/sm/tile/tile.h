/**
 * @file   tile.h
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
 * This file defines class Tile.
 */

#ifndef TILEDB_TILE_H
#define TILEDB_TILE_H

#include "tiledb/common/status.h"
#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/buffer/buffer.h"

#include <cinttypes>

using namespace tiledb::common;

namespace tiledb {
namespace sm {

/**
 * Handles tile information. A tile can be in main memory if it has been
 * fetched from the disk or has been mmap-ed from a file. However, a tile
 * can be solely on the disk, in which case the Tile object stores the
 * offset in the file where the tile data begin, plus the tile size.
 */
class Tile {
 public:
  enum class TileStatus : char { NotReady, Ready, Error };

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

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  Tile();

  /**
   * Constructor.
   *
   * @param type The type of the data to be stored.
   * @param cell_size The cell size.
   * @param dim_num The number of dimensions in case the tile stores
   *      coordinates.
   * @param Buffer The buffer to be encapsulated by the tile object. This means
   *     that the tile will not create a new Buffer object, but operate
   *     on the input.
   * @param owns_buff If *true* the tile object will delete *buffer_* upon
   *     destruction, otherwise it will not delete it.
   */
  Tile(
      Datatype type,
      uint64_t cell_size,
      unsigned int dim_num,
      Buffer* buffer,
      bool owns_buff);

  /**
   * Constructor.
   *
   * @param format_version The format version of the internal buffer.
   * @param type The type of the data to be stored.
   * @param cell_size The cell size.
   * @param dim_num The number of dimensions in case the tile stores
   *      coordinates.
   * @param Buffer The buffer to be encapsulated by the tile object. This means
   *     that the tile will not create a new Buffer object, but operate
   *     directly on the input.
   * @param owns_buff If *true* the tile object will delete *buffer_* upon
   *     destruction, otherwise it will not delete it.
   */
  Tile(
      uint32_t format_version,
      Datatype type,
      uint64_t cell_size,
      unsigned int dim_num,
      Buffer* buffer,
      bool owns_buff);

  /**
   * Copy constructor. This performs a deep copy (including potential memcpy of
   * underlying buffers).
   */
  Tile(const Tile& tile);

  /** Move constructor. */
  Tile(Tile&& tile);

  /** Destructor. */
  ~Tile();

  /**
   * Copy-assign operator. This performs a deep copy (including potential memcpy
   * of underlying buffers).
   */
  Tile& operator=(const Tile& tile);

  /** Move-assign operator. */
  Tile& operator=(Tile&& tile);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the number of cells stored in the tile. */
  uint64_t cell_num() const;

  /**
   * Tile initializer for storing unfiltered bytes.
   *
   * @param format_version The format version of the data in this tile.
   * @param type The type of the data to be stored.
   * @param tile_size The tile size. The internal buffer will be allocated
   *     that much space upon construction.
   * @param cell_size The cell size.
   * @param dim_num The number of dimensions in case the tile stores
   *      coordinates.
   * @param fill_with_zeros If true, it fills the entire tile with zeros,
   *      allocating memory and setting the size.
   * @return Status
   */
  Status init_unfiltered(
      uint32_t format_version,
      Datatype type,
      uint64_t tile_size,
      uint64_t cell_size,
      unsigned int dim_num,
      bool fill_with_zeros = false);

  /**
   * Tile initializer for storing filtered bytes.
   *
   * @param format_version The format version of the data in this tile.
   * @param type The type of the data to be stored.
   * @param tile_size The tile size. The internal buffer will be allocated
   *     that much space upon construction.
   * @param cell_size The cell size.
   * @param dim_num The number of dimensions in case the tile stores
   *      coordinates.
   * @return Status
   */
  Status init_filtered(
      uint32_t format_version,
      Datatype type,
      uint64_t cell_size,
      unsigned int dim_num);

  /** Advances the buffer offset. */
  void advance_offset(uint64_t nbytes);

  /** Returns the internal buffer. */
  inline Buffer* buffer() const {
    return buffer_;
  }

  /** Returns the cell size. */
  inline uint64_t cell_size() const {
    return cell_size_;
  }

  /**
   * Returns a shallow or deep copy of this Tile.
   *
   * @param deep_copy If true, a deep copy is performed, including potentially
   *    memcpying the underlying Buffer. If false, a shallow copy is performed,
   *    which sets the clone's Buffer equal to Tile's buffer pointer.
   * @return New Tile
   */
  Tile clone(bool deep_copy) const;

  /**
   * Sets `owns_buffer_` to `false` and thus will not destroy the buffer
   * in the destructor.
   */
  void disown_buff();

  bool owns_buff() const;

  /** Returns the number of dimensions (0 if this is an attribute tile). */
  inline unsigned int dim_num() const {
    return dim_num_;
  }

  /** Checks if the tile is empty. */
  bool empty() const;

  /**
   * Returns the current filtered state of the tile data in the buffer. When
   * `true`, the buffer contains the filtered, on-disk format of the tile.
   */
  inline bool filtered() const {
    assert(!(filtered_buffer_.alloced_size() > 0 && buffer_->size() > 0));
    return filtered_buffer_.alloced_size() > 0;
  }

  /**
   * Returns the buffer that contains the filtered, on-disk format.
   */
  inline Buffer* filtered_buffer() {
    return &filtered_buffer_;
  }

  /** Gets the format version number of the data in this Tile. */
  inline uint32_t format_version() const {
    return format_version_;
  }

  /** Checks if the tile is full. */
  bool full() const;

  /** The current offset in the tile. */
  uint64_t offset() const;

  /**
   * Returns the pre-filtered size of the tile data in the buffer.
   *
   * On writes, the pre-filtered size is the uncompressed size.
   *
   * On reads, the pre-filtered size is the persisted (compressed) size.
   */
  uint64_t pre_filtered_size() const;

  /** Reads from the tile into the input buffer *nbytes*. */
  Status read(void* buffer, uint64_t nbytes);

  /**
   * Reads from the tile at the given offset into the input
   * buffer of size nbytes. Does not mutate the internal offset.
   * Thread-safe among readers.
   */
  Status read(void* buffer, uint64_t nbytes, uint64_t offset) const;

  /** Resets the size and offset of the tile. */
  void reset();

  /** Resets the tile offset. */
  void reset_offset();

  /** Resets the tile size. */
  void reset_size();

  /** Sets the tile offset. */
  void set_offset(uint64_t offset);

  /** Sets the pre-filtered size value to the given value. */
  void set_pre_filtered_size(uint64_t pre_filtered_size);

  /** Returns the tile size. */
  inline uint64_t size() const {
    assert(!filtered());
    return (buffer_ == nullptr) ? 0 : buffer_->size();
  }

  /** Returns *true* if the tile stores coordinates. */
  inline bool stores_coords() const {
    return dim_num_ > 0;
  }

  /** Returns the tile data type. */
  inline Datatype type() const {
    return type_;
  }

  /** Writes as much data as possibly can be read from the input buffer. */
  Status write(ConstBuffer* buf);

  /**
   * Writes exactly `nbytes` from the input buffer to the local buffer.
   * The local buffer can be potentially expanded to fit these bytes.
   */
  Status write(ConstBuffer* buf, uint64_t nbytes);

  /** Writes `nbytes` from `data` to the tile. */
  Status write(const void* data, uint64_t nbytes);

  /**
   * Writes `nbytes` from `data` to the tile at `offset`.
   *
   * @note This function assumes that the tile buffer has already been
   *     properly allocated. It does not alter the tile offset and size.
   */
  Status write(const void* data, uint64_t offset, uint64_t nbytes);

  /**
   * Zips the coordinate values such that a cell's coordinates across
   * all dimensions appear contiguously in the buffer.
   */
  Status zip_coordinates();

  /** Wait for a tile to become ready. */
  Status wait_ready();

  /** Signal a tile is not ready to be unfiltered. */
  void signal_ready();

  /** Signal a tile had an error while reading. */
  void signal_error();

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The buffer backing the tile data. */
  Buffer* buffer_;

  /** The cell size. */
  uint64_t cell_size_;

  /**
   * The number of dimensions, in case the tile stores coordinates. It is 0
   * in case the tile stores attributes.
   */
  unsigned int dim_num_;

  /** The format version of the data in this tile. */
  uint32_t format_version_;

  /**
   * If *true* the tile object will free *buffer_* upon destruction, otherwise
   * it will not delete it.
   */
  bool owns_buffer_;

  /** The size in bytes of the tile data before it has been filtered. */
  uint64_t pre_filtered_size_;

  /** The tile data type. */
  Datatype type_;

  /**
   * The buffer that contains the filtered, on-disk bytes. This buffer is
   * exclusively used in the I/O path between the disk and the filter
   * pipeline. Note that this buffer is _only_ accessed in the filtered()
   * public API, all other public API routines operate on 'buffer_'.
   */
  Buffer filtered_buffer_;

  /** Condition variable to wait on the tile being ready. */
  std::condition_variable tile_status_ready_cv_;

  /** The tile status, not ready, ready or error. */
  TileStatus tile_status_;

  /** Mutex protecting tile_status_ready_cv_ and tile_status_. */
  std::mutex tile_status_mtx_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /** Swaps the contents (all field values) of this tile with the given tile. */
  void swap(Tile& tile);
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_TILE_H
