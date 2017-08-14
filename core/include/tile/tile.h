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

  /** Constructor. */
  Tile();

  /**
   * Constructor.
   *
   * @param type The type of the data to be stored.
   * @param compression The compression type.
   * @param compression_level The compression level.
   * @param tile_size The tile size.
   * @param cell_size The cell size.
   * @param stores_offsets Indicates whether the tile stores offsets
   *     (of type uint64_t) of variable-length cells (stored in another
   *     tile).
   */
  Tile(
      Datatype type,
      Compressor compression,
      int compression_level,
      uint64_t tile_size,
      uint64_t cell_size,
      bool stores_offsets = false);

  /**
   * Constructor.
   *
   * @param type The type of the data to be stored.
   * @param compression The compression type.
   * @param cell_size The cell size.
   * @param stores_offsets Indicates whether the tile stores offsets
   *     (of type uint64_t) of variable-length cells (stored in another
   *     tile).
   */
  Tile(
      Datatype type,
      Compressor compression,
      uint64_t cell_size,
      bool stores_offsets = false);

  /** Destructor. */
  ~Tile();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Adbances the tile offset by the input bytes. */
  void advance_offset(uint64_t nbytes);

  /** Allocates memory of the input size. */
  Status alloc(uint64_t size);

  inline Buffer* buffer() const {
    return buffer_;
  }

  /** Returns the tile data. */
  inline void* data() const {
    if (buffer_ == nullptr)
      return nullptr;

    return buffer_->data();
  }

  /** Returns the cell size. */
  inline uint64_t cell_size() const {
    return cell_size_;
  }

  /** Returns the tile compressor. */
  inline Compressor compressor() const {
    return compressor_;
  }

  /** Returns the tile compression level. */
  inline int compression_level() const {
    return compression_level_;
  }

  /** Checks if the tile is empty. */
  inline bool empty() const {
    return buffer_ == nullptr || buffer_->offset() == 0;
  }

  /** Returns the file offset. */
  inline uint64_t file_offset() const {
    return file_offset_;
  }

  /** Checks if the tile is full. */
  inline bool full() const {
    if (buffer_ == nullptr)
      return false;

    return buffer_->offset() == buffer_->size();
  }

  /** Checks if the tile is in main memory. */
  inline bool in_mem() const {
    return buffer_ != nullptr;
  }

  /** The current offset in the tile. */
  inline uint64_t offset() const {
    return offset_;
  }

  /**
   * Memory-maps a region in a file.
   *
   * @param filename The file to be mmap-ed.
   * @param tile_size The tile size.
   * @param offset The offset in the file where the mmap begins.
   * @return Status.
   */
  Status mmap(const uri::URI& filename, uint64_t tile_size, uint64_t offset);

  /** Reads from the tile into the input buffer *nbytes*. */
  Status read(void* buffer, uint64_t nbytes);

  /** Resets the tile offset. */
  inline void reset_offset() {
    if (buffer_ != nullptr)
      buffer_->reset_offset();
    offset_ = 0;
  }

  /** Sets the file offset. */
  inline void set_file_offset(uint64_t file_offset) {
    file_offset_ = file_offset;
  }

  /** Sets the tile offset. */
  inline void set_offset(uint64_t offset) {
    if (buffer_ != nullptr)
      buffer_->set_offset(offset);
    offset_ = offset;
  }

  /** Sets the tile size. */
  inline void set_size(uint64_t size) {
    tile_size_ = size;
  }

  /** Returns the tile size. */
  inline uint64_t size() const {
    return tile_size_;
  }

  /** Checks if the tile stores offsets. */
  inline bool stores_offsets() const {
    return stores_offsets_;
  }

  /** Returns the tile data type. */
  inline Datatype type() const {
    return type_;
  }

  /** Returns the value of type T in the tile at the input offset. */
  template <class T>
  inline T value(uint64_t offset) {
    return buffer_->value<T>(offset);
  }

  /** Returns the value of type T in the tile at the current offset. */
  template <class T>
  inline T value() {
    return buffer_->value<T>();
  }

  /** Writes as much data as possibly can be read from the input buffer. */
  Status write(ConstBuffer* buf);

  /**
   * Writes as much data as possibly can be read from the input buffer.
   * Note that this is a special function where each read value (of type
   * uint64_t) is added to the input offset prior to being written to
   * the tile local buffer.
   */
  Status write_with_shift(ConstBuffer* buf, uint64_t offset);

  /**
   * Writes exactly *nbytes* from the input buffer to the local buffer.
   * The local buffer can be potentially expanded to fit these bytes.
   */
  Status write(ConstBuffer* buf, uint64_t bytes);

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

  /** The file offset where the tile begins. */
  uint64_t file_offset_;

  /** The current offset in the tile. */
  uint64_t offset_;

  /** Indicates if the tile stores offsets. */
  bool stores_offsets_;

  /** The tile size. */
  uint64_t tile_size_;

  /** The tile data type. */
  Datatype type_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */
};

}  // namespace tiledb

#endif  // TILEDB_TILE_H
