/**
 * @file   tile.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2019 TileDB, Inc.
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

#include "tiledb/sm/array_schema/attribute.h"
#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/buffer/const_buffer.h"
#include "tiledb/sm/misc/status.h"

#include <cinttypes>

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
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  Tile();

  /**
   * Constructor.
   *
   * @param dim_num The number of dimensions in case the tile stores
   *      coordinates.
   */
  explicit Tile(unsigned int dim_num);

  /**
   * Constructor.
   *
   * @param type The type of the data to be stored.
   * @param cell_size The cell size.
   * @param dim_num The number of dimensions in case the tile stores
   *      coordinates.
   * @param buff  The buffer to be encapsulated by the tile object. This
   *     means that the tile will not create a new buffer object, but
   *     operate directly on the input.
   * @param owns_buff If *true* the tile object will delete *buff* upon
   *     destruction, otherwise it will not delete it.
   */
  Tile(
      Datatype type,
      uint64_t cell_size,
      unsigned int dim_num,
      Buffer* buff,
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
   * Tile initializer.
   *
   * @param format_version The format version of the data in this tile.
   * @param type The type of the data to be stored.
   * @param cell_size The cell size.
   * @param dim_num The number of dimensions in case the tile stores
   *      coordinates.
   * @return Status
   */
  Status init(
      uint32_t format_version,
      Datatype type,
      uint64_t cell_size,
      unsigned int dim_num);

  /**
   * Tile initializer.
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
  Status init(
      uint32_t format_version,
      Datatype type,
      uint64_t tile_size,
      uint64_t cell_size,
      unsigned int dim_num);

  /** Advances the buffer offset. */
  void advance_offset(uint64_t nbytes);

  /** Returns the internal buffer. */
  Buffer* buffer() const;

  /** Returns the cell size. */
  uint64_t cell_size() const;

  /**
   * Returns a shallow or deep copy of this Tile.
   *
   * @param deep_copy If true, a deep copy is performed, including potentially
   *    memcpying the underlying Buffer. If false, a shallow copy is performed,
   *    which sets the clone's Buffer equal to Tile's buffer pointer.
   * @return New Tile
   */
  Tile clone(bool deep_copy) const;

  /** Returns the internal tile data. */
  void* internal_data() const;

  /**
   * Sets `owns_buff_` to `false` and thus will not destroy the buffer
   * in the destructor.
   */
  void disown_buff();

  /** Returns the number of dimensions (0 if this is an attribute tile). */
  unsigned int dim_num() const;

  /** Checks if the tile is empty. */
  bool empty() const;

  /**
   * Returns the current filtered state of the tile data in the buffer.
   *
   * On writes, this returns true once the tile buffer is ready to be written
   * (compressed, etc).
   *
   * On reads, this returns true once the tile buffer is ready to be copied to
   * user buffers (decompressed, etc).
   */
  bool filtered() const;

  /** Gets the format version number of the data in this Tile. */
  uint32_t format_version() const;

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

  /** Reallocates nbytes for the internal tile buffer. */
  Status realloc(uint64_t nbytes);

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

  /** Set the filtered state of the tile. */
  void set_filtered(bool filtered);

  /** Sets the tile offset. */
  void set_offset(uint64_t offset);

  /** Sets the pre-filtered size value to the given value. */
  void set_pre_filtered_size(uint64_t pre_filtered_size);

  /** Sets the internal buffer size. */
  void set_size(uint64_t size);

  /** Returns the tile size. */
  uint64_t size() const;

  /**
   * Splits the coordinates such that all the values of each dimension
   * appear contiguously in the buffer.
   */
  void split_coordinates();

  /** Returns *true* if the tile stores coordinates. */
  bool stores_coords() const;

  /** Returns the tile data type. */
  Datatype type() const;

  /** Returns the value of type T in the tile at the input offset. */
  template <class T>
  inline T value(uint64_t offset) const {
    return buffer_->value<T>(offset);
  }

  /** Returns the value of type T in the tile at the current offset. */
  template <class T>
  inline T value() const {
    return buffer_->value<T>();
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

  /** Writes the entire internal buffer of 'rhs' into the tile. */
  Status write(const Tile& rhs);

  /**
   * Writes as much data as possibly can be read from the input buffer.
   * Note that this is a special function where each read value (of type
   * uint64_t) is added to the input offset prior to being written to
   * the tile local buffer.
   */
  Status write_with_shift(ConstBuffer* buf, uint64_t offset);

  /**
   * Zips the coordinate values such that a cell's coordinates across
   * all dimensions appear contiguously in the buffer.
   */
  void zip_coordinates();

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** Local buffer that stores the tile data. */
  Buffer* buffer_ = nullptr;

  /** The cell size. */
  uint64_t cell_size_;

  /**
   * The number of dimensions, in case the tile stores coordinates. It is 0
   * in case the tile stores attributes.
   */
  unsigned int dim_num_;

  /** The current state of the in-memory tile data with respect to filtering. */
  bool filtered_;

  /** The format version of the data in this tile. */
  uint32_t format_version_;

  /**
   * If *true* the tile object will delete *buff* upon
   * destruction, otherwise it will not delete it.
   */
  bool owns_buff_;

  /** The size in bytes of the tile data before it has been filtered. */
  uint64_t pre_filtered_size_;

  /** The tile data type. */
  Datatype type_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /** Swaps the contents (all field values) of this tile with the given tile. */
  void swap(Tile& tile);
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_TILE_H
