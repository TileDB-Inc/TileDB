/**
 * @file   tile.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017 TileDB, Inc.
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

#include "attribute.h"
#include "buffer.h"
#include "const_buffer.h"
#include "status.h"

#include <cinttypes>

namespace tiledb {

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
   * @param compression The compression type.
   * @param compression_level The compression level.
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
      Compressor compression,
      int compression_level,
      uint64_t cell_size,
      unsigned int dim_num,
      Buffer* buff,
      bool owns_buff);

  /**
   * Constructor.
   *
   * @param type The type of the data to be stored.
   * @param compression The compression type.
   * @param compression_level The compression level.
   * @param tile_size The tile size. The internal buffer will be allocated
   *     that much space upon construction.
   * @param cell_size The cell size.
   * @param dim_num The number of dimensions in case the tile stores
   *      coordinates.
   */
  Tile(
      Datatype type,
      Compressor compression,
      int compression_level,
      uint64_t tile_size,
      uint64_t cell_size,
      unsigned int dim_num);

  /**
   * Constructor.
   *
   * @param type The type of the data to be stored.
   * @param compression The compression type.
   * @param cell_size The cell size.
   * @param dim_num The number of dimensions in case the tile stores
   *      coordinates.
   */
  Tile(
      Datatype type,
      Compressor compression,
      uint64_t cell_size,
      unsigned int dim_num);

  /** Destructor. */
  ~Tile();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Advances the buffer offset. */
  void advance_offset(uint64_t nbytes);

  /** Returns the internal buffer. */
  Buffer* buffer() const;

  /** Returns the cell size. */
  uint64_t cell_size() const;

  /** Returns the tile compressor. */
  Compressor compressor() const;

  /** Returns the tile compression level. */
  int compression_level() const;

  /** Returns the buffer data pointer at the current offset. */
  void* cur_data() const;

  /** Returns the tile data. */
  void* data() const;

  /**
   * Sets `owns_buff_` to `false` and thus will not destroy the buffer
   * in the destructor.
   */
  void disown_buff();

  /** Returns the number of dimensions (0 if this is an attribute tile). */
  unsigned int dim_num() const;

  /** Checks if the tile is empty. */
  bool empty() const;

  /** Checks if the tile is full. */
  bool full() const;

  /** The current offset in the tile. */
  uint64_t offset() const;

  /** Reallocates nbytes for the internal tile buffer. */
  Status realloc(uint64_t nbytes);

  /** Reads from the tile into the input buffer *nbytes*. */
  Status read(void* buffer, uint64_t nbytes);

  /** Resets the tile offset. */
  void reset_offset();

  /** Resets the tile size. */
  void reset_size();

  /** Sets the tile offset. */
  void set_offset(uint64_t offset);

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
   * Writes exactly *nbytes* from the input buffer to the local buffer.
   * The local buffer can be potentially expanded to fit these bytes.
   */
  Status write(ConstBuffer* buf, uint64_t nbytes);

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
  Buffer* buffer_;

  /** The cell size. */
  uint64_t cell_size_;

  /** The compression type. */
  Compressor compressor_;

  /** The compression level. */
  int compression_level_;

  /**
   * The number of dimensions, in case the tile stores coordinates. It is 0
   * in case the tile stores attributes.
   */
  unsigned int dim_num_;

  /**
   * If *true* the tile object will delete *buff* upon
   * destruction, otherwise it will not delete it.
   */
  bool owns_buff_;

  /** The tile data type. */
  Datatype type_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */
};

}  // namespace tiledb

#endif  // TILEDB_TILE_H
