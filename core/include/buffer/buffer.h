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
#include "status.h"

namespace tiledb {

/** Enables reading from and writing to a buffer. */
class Buffer {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  Buffer();

  /** Constructor that allocates memory with the input size. */
  explicit Buffer(uint64_t size);

  /** Destructor. */
  ~Buffer();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Advances the buffer offset. */
  void advance_offset(uint64_t nbytes);

  /** Clears the buffer, deallocating memory. */
  void clear();

  /** Returns the buffer data. */
  void* data() const;

  /** Returns the current offset in the buffer. */
  uint64_t offset() const;

  /**
   * Reads from the local data into the input buffer.
   *
   * @param buffer The buffer to read the data into.
   * @param nbytes The number of bytes to read.
   * @return Status
   */
  Status read(void* buffer, uint64_t nbytes);

  /**
   * Reallocates memory for the buffer with the input size.
   *
   * @param nbytes Number of bytes to allocate.
   * @return Status.
   */
  Status realloc(uint64_t nbytes);

  /** Resets the buffer offset to 0. */
  void reset_offset();

  /** Sets the buffer offset to the input offset. */
  void set_offset(uint64_t offset);

  /** Sets the size of the buffer. */
  void set_size(uint64_t size);

  /** Returns the buffer size. */
  uint64_t size() const;

  /**
   * Returns the value of type T at the input offset.
   *
   * @tparam T The type of the value to return.
   * @param offset The offset from which to retrieve the value.
   * @return The requested value.
   */
  template <class T>
  T value(uint64_t offset) const {
    return ((T*)(((char*)data_) + offset))[0];
  }

  /** Returns the value of type T at the current offset. */
  template <class T>
  T value() const {
    return ((T*)(((char*)data_) + offset_))[0];
  }

  /**
   * Writes into the local buffer by reading as much data as possible from
   * the input buffer. No new memory is allocated for the local buffer.
   *
   * @param buff The buffer to read from.
   * @return void
   */
  void write(ConstBuffer* buff);

  /**
   * Writes exactly *nbytes* into the local buffer by reading from the
   * input buffer *buf*.
   *
   * @param buff The buffer to read from.
   * @param nbytes Number of bytes to write.
   * @return Status.
   */
  Status write(ConstBuffer* buff, uint64_t nbytes);

  /**
   * Writes exactly *nbytes* into the local buffer by reading from the
   * input buffer *buf*.
   *
   * @param buffer The buffer to read from.
   * @param nbytes Number of bytes to write.
   * @return Status.
   */
  Status write(const void* buffer, uint64_t nbytes);

  /**
   * Writes as much data as possible read from the input buffer *buff*, but
   * then adds the *offset* value to the read data. The type of each data
   * value read is uint64_t. This is an auxiliary function used when
   * reading variable-sized attribute offsets from the disk.
   *
   * @param buff The buffer to read from.
   * @param offset The offset value to be added to the read values.
   */
  void write_with_shift(ConstBuffer* buff, uint64_t offset);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The buffer data. */
  void* data_;

  /** The current buffer offset. */
  uint64_t offset_;

  /** Buffer allocated size. */
  uint64_t size_;
};

}  // namespace tiledb

#endif  // TILEDB_BUFFER_H
