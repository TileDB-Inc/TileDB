/**
 * @file   const_buffer.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2018 TileDB, Inc.
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

#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/misc/status.h"

namespace tiledb {
namespace sm {

/** Enables reading from a constant buffer. */
class ConstBuffer {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor (initializer).
   *
   * @param data The data of the buffer.
   * @param size The size of the buffer.
   */
  ConstBuffer(const void* data, uint64_t size);

  /**
   * Constructor.
   *
   * @param buff The buffer the object will encapsulate, working on its
   *     data and size, but using a separate local offset, without affecting
   *     the input buffer.
   */
  explicit ConstBuffer(Buffer* buff);

  /** Destructor. */
  ~ConstBuffer() = default;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Advances the offset by *nbytes*. */
  void advance_offset(uint64_t nbytes);

  /** Returns pointer to data at the current offset. */
  const void* cur_data() const;

  /** Returns the buffer data. */
  const void* data() const;

  /** Checks if reading has reached the end of the buffer. */
  bool end() const;

  /** Returns the number of bytes left for reading. */
  uint64_t nbytes_left_to_read() const;

  /** Sets the buffer offset. */
  void set_offset(uint64_t offset);

  /** Returns the buffer offset. */
  uint64_t offset() const;

  /**
   * Reads from the internal buffer into the input buffer.
   *
   * @param buffer The buffer to write to when reading from the local buffer.
   * @param nbytes The number of bytes to read.
   * @return Status.
   */
  Status read(void* buffer, uint64_t nbytes);

  /**
   * This is a special function used for reading from a buffer that stores
   * uint64_t values. It reads a number of bytes from the local buffer and
   * writes them to the input buffer, after adding *offset* to each read
   * uint64_t value.
   *
   * @param buf The buffer to write to when reading from the local buffer.
   * @param nbytes The number of bytes to read.
   * @param offset The offset to add to each uint64_t value.
   * @return void.
   */
  void read_with_shift(uint64_t* buf, uint64_t nbytes, uint64_t offset);

  /** Returns the size of the buffer. */
  uint64_t size() const;

  /**
   * Returns a value from the buffer of type T.
   *
   * @tparam T The type of the value to be read.
   * @param offset The offset in the local buffer to read from.
   * @return The desired value of type T.
   */
  template <class T>
  inline T value(uint64_t offset) {
    return ((const T*)(((const char*)data_) + offset))[0];
  }

  /**
   * Returns the value at the current offset of the buffer of type T.
   *
   * @tparam T The type of the value to be returned.
   * @return The value to be returned of type T.
   */
  template <class T>
  inline T value() {
    return ((const T*)(((const char*)data_) + offset_))[0];
  }

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The (read-only) buffer data. */
  const void* data_;

  /** The current offset in the buffer to read from. */
  uint64_t offset_;

  /** The size of the buffer. */
  uint64_t size_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_CONST_BUFFER_H
