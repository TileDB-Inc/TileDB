/**
 * @file   buffer.h
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
 * This file defines class Buffer.
 */

#ifndef TILEDB_BUFFER_H
#define TILEDB_BUFFER_H

#include <cinttypes>

#include "tiledb/sm/misc/status.h"

namespace tiledb {
namespace sm {

class ConstBuffer;

/** Enables reading from and writing to a buffer. */
class Buffer {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  Buffer();

  /**
   * Constructor. Initializes a buffer with the input data and size.
   *
   * @param data The internal data of the buffer.
   * @param size The size of the data.
   * @param owns_data Indicates whether the object will own the data,
   *     i.e., if it has permission to reallocate the data and
   *     is responsible for freeing it.
   */
  Buffer(void* data, uint64_t size, bool owns_data);

  /** Copy constructor. */
  Buffer(const Buffer& buff);

  /** Destructor. */
  ~Buffer();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Advances the offset by *nbytes*. */
  void advance_offset(uint64_t nbytes);

  /** Advances the size by *nbytes*. */
  void advance_size(uint64_t nbytes);

  /** Returns the allocated buffer size. */
  uint64_t alloced_size() const;

  /** Clears the buffer, deallocating memory. */
  void clear();

  /** Returns the buffer data pointer at the current offset. */
  void* cur_data() const;

  /** Returns the buffer data. */
  void* data() const;

  /** Returns the buffer data pointer at the input offset. */
  void* data(uint64_t offset) const;

  /**
   * Sets `owns_data_` to `false` and thus will not destroy the data
   * in the destructor.
   */
  void disown_data();

  /** Returns the number of byte of free space in the buffer. */
  uint64_t free_space() const;

  /** Returns the current offset in the buffer. */
  uint64_t offset() const;

  /** Returns `true` if the buffer owns its data buffer. */
  bool owns_data() const;

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

  /** Resets the buffer size. */
  void reset_size();

  /** Sets the buffer offset to the input offset. */
  void set_offset(uint64_t offset);

  /** Sets the buffer size. */
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

  /**
   * Returns the pointer to the value the input offset.
   *
   * @param offset The offset from which to retrieve the value pointer.
   * @return The requested pointer.
   */
  void* value_ptr(uint64_t offset) const {
    return ((void*)(((char*)data_) + offset));
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
   * @return Status
   */
  Status write(ConstBuffer* buff);

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
   * @return Status
   */
  Status write_with_shift(ConstBuffer* buff, uint64_t offset);

  /** Copy operator. */
  Buffer& operator=(const Buffer& buff);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The allocated buffer size. */
  uint64_t alloced_size_;

  /** The buffer data. */
  void* data_;

  /** The current buffer offset. */
  uint64_t offset_;

  /**
   * True if the object owns the data buffer, which means that it is
   * responsible for allocating and freeing it.
   */
  bool owns_data_;

  /** Size of the buffer useful data. */
  uint64_t size_;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_BUFFER_H
