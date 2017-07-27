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
      uint64_t cell_size);

  ~Tile();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  inline void* buffer() const {
    return buffer_->data();
  }

  inline uint64_t buffer_size() const {
    return buffer_->size();
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

  inline bool full() const {
    return buffer_->offset() == buffer_->size();
  }

  void reset();

  inline uint64_t tile_size() const {
    return tile_size_;
  }

  inline Datatype type() const {
    return type_;
  }

  Status write(ConstBuffer* buf);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  Buffer* buffer_;

  Buffer* buffer_var_;

  uint64_t cell_size_;

  Compressor compressor_;

  int compression_level_;

  uint64_t tile_size_;

  Datatype type_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */
};

}  // namespace tiledb

#endif  // TILEDB_TILE_H
