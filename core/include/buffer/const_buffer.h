/**
 * @file   const_buffer.h
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
 * This file defines class ConstBuffer.
 */

#ifndef TILEDB_CONST_BUFFER_H
#define TILEDB_CONST_BUFFER_H

#include <cinttypes>

namespace tiledb {

class ConstBuffer {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  ConstBuffer(const void* data, uint64_t size);

  ~ConstBuffer();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  inline uint64_t bytes_left_to_read() const {
    return size_ - offset_;
  }

  inline const void* data() const {
    return data_;
  }

  inline uint64_t offset() const {
    return offset_;
  }

  inline bool end() const {
    return offset_ == size_;
  }

  void read(void* buffer, uint64_t bytes);

  template <class T>
  inline T value(uint64_t offset) {
    return ((const T*)(((const char*)data_) + offset))[0];
  }

  template <class T>
  inline T value() {
    return ((const T*)(((const char*)data_) + offset_))[0];
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  const void* data_;

  uint64_t offset_;

  uint64_t size_;
};

}  // namespace tiledb

#endif  // TILEDB_CONST_BUFFER_H
