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

class Tile {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  Tile();

  Tile(
      Datatype type,
      Compressor compression,
      int compression_level,
      uint64_t tile_size,
      uint64_t cell_size,
      bool stores_offsets = false);

  Tile(
      Datatype type,
      Compressor compression,
      uint64_t cell_size,
      bool stores_offsets = false);

  ~Tile();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  void advance_offset(uint64_t bytes);

  void alloc(uint64_t);

  inline void* data() const {
    if (buffer_ == nullptr)
      return nullptr;

    return buffer_->data();
  }

  inline uint64_t cell_size() const {
    return cell_size_;
  }

  inline Compressor compressor() const {
    return compressor_;
  }

  inline int compression_level() const {
    return compression_level_;
  }

  inline bool empty() const {
    return buffer_ == nullptr || buffer_->offset() == 0;
  }

  inline uint64_t file_offset() const {
    return file_offset_;
  }

  inline bool full() const {
    if (buffer_ == nullptr)
      return false;

    return buffer_->offset() == buffer_->size();
  }

  inline bool in_mem() const {
    return buffer_ != nullptr;
  }

  inline uint64_t offset() const {
    return offset_;
  }

  Status mmap(const uri::URI& filename, uint64_t tile_size, uint64_t offset);

  Status read(void* buffer, uint64_t bytes);

  inline void reset() {
    if (buffer_ != nullptr)
      buffer_->reset_offset();
    offset_ = 0;
  }

  inline void set_file_offset(uint64_t file_offset) {
    file_offset_ = file_offset;
  }

  inline void set_offset(uint64_t offset) {
    if (buffer_ != nullptr)
      buffer_->set_offset(offset);
    offset_ = offset;
  }

  inline void set_size(uint64_t size) {
    tile_size_ = size;
  }

  inline uint64_t size() const {
    return tile_size_;
  }

  inline bool stores_offsets() const {
    return stores_offsets_;
  }

  inline Datatype type() const {
    return type_;
  }

  template <class T>
  inline T value(uint64_t offset) {
    return buffer_->value<T>(offset);
  }

  template <class T>
  inline T value() {
    return buffer_->value<T>();
  }

  Status write(ConstBuffer* buf);

  Status write_with_shift(ConstBuffer* buf, uint64_t offset);

  Status write(ConstBuffer* buf, uint64_t bytes);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  Buffer* buffer_;

  uint64_t cell_size_;

  Compressor compressor_;

  int compression_level_;

  uint64_t file_offset_;

  // TODO: perhaps find a better way to implement this
  uint64_t offset_;

  bool stores_offsets_;

  uint64_t tile_size_;

  Datatype type_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */
};

}  // namespace tiledb

#endif  // TILEDB_TILE_H
