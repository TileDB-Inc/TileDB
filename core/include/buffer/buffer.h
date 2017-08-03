/**
 * @file   buffer.h
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
 * This file defines class Buffer.
 */

#ifndef TILEDB_BUFFER_H
#define TILEDB_BUFFER_H

#include <cinttypes>

#include "const_buffer.h"

namespace tiledb {

class Buffer {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  Buffer();

  Buffer(uint64_t size);

  ~Buffer();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  inline void* data() const {
    return data_;
  }

  inline uint64_t offset() const {
    return offset_;
  }

  void realloc(uint64_t size);

  inline void reset() {
    offset_ = 0;
    size_ = 0;
  }

  inline void reset_offset() {
    offset_ = 0;
  }

  // TODO: this will be removed soon
  inline void set_offset(uint64_t offset) {
    offset_ = offset;
  }

  // TODO: this will be removed soon
  inline void set_size(uint64_t size) {
    size_ = size;
  }

  inline uint64_t size() const {
    return size_;
  }

  inline uint64_t size_alloced() const {
    return size_alloced_;
  }

  template <class T>
  inline T value(uint64_t offset) {
    return ((T*)(((char*)data_) + offset))[0];
  }

  template <class T>
  inline T value() {
    return ((T*)(((char*)data_) + offset_))[0];
  }

  void write(ConstBuffer* buf);

  void write(ConstBuffer* buf, uint64_t bytes);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  void* data_;

  uint64_t offset_;

  uint64_t size_;

  uint64_t size_alloced_;
};

}  // namespace tiledb

#endif  // TILEDB_BUFFER_H
