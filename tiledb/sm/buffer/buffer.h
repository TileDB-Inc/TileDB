/**
 * @file   buffer.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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

#include "tiledb/common/status.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class ConstBuffer;

/**
 * Base class for `Buffer` and `ConstBuffer`
 *
 * Responsible for maintaining an read offset in the range [0..size_]. Not
 * responsible for memory management.
 */
class BufferBase {
 public:
  /** Returns the buffer size. */
  uint64_t size() const;

  /** Returns the current read position, in bytes. */
  uint64_t offset() const;

  /** Resets the buffer offset to 0. */
  void reset_offset();

  /** Sets the buffer offset to the input offset. */
  void set_offset(uint64_t offset);

  /** Advances the offset by *nbytes*. */
  void advance_offset(uint64_t nbytes);

  /** Checks if reading has reached the end of the buffer. */
  bool end() const;

  /**
   * Reads from the local data into the input buffer.
   *
   * @param destination The buffer to read the data into.
   * @param nbytes The number of bytes to read.
   * @return Status
   */
  Status read(void* destination, uint64_t nbytes);

 protected:
  BufferBase();
  BufferBase(void* data, uint64_t size);
  BufferBase(const void* data, uint64_t size);

  /** Returns the buffer data. */
  void* nonconst_data() const;

  /** Returns the buffer data as a pointer to constant. */
  const void* const_data() const;

  /** Returns the buffer at the current read offset */
  void* nonconst_unread_data() const;

  /** Returns the buffer at the current read offset as a pointer to constant */
  const void* const_unread_data() const;

  /** The buffer data. */
  /**
   * @invariant If data_ does not change across a class method, then neither
   * does data_[0..size_). In other words, the data is treated as constant.
   */
  void* data_;

  /** Size of the buffer data. */
  uint64_t size_;

  /** The current buffer read position in bytes, i.e. sizeof(char). */
  /**
   * @invariant offset_ <= size_
   */
  uint64_t offset_;
};

/** Enables reading from and writing to a buffer. */
class Buffer : public BufferBase {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  Buffer();

  /**
   * Constructor.
   *
   * Initializes the buffer to "wrap" the input data and size. The buffer being
   * constructed does not make a copy of the input data, and thus does not own
   * it.
   *
   * @param data The data for the buffer to wrap.
   * @param size The size (in bytes) of the data.
   */
  Buffer(void* data, uint64_t size);

  /**
   * Copy constructor.
   *
   * If the given buffer owns its data, this new buffer will make its own copy
   * of the given buffer's allocation. If the given buffer does not own its
   * data, this new buffer will wrap the allocation without owning or
   * copying it.
   */
  Buffer(const Buffer& buff);

  /** Move constructor. */
  Buffer(Buffer&& buff);

  /** Destructor. */
  ~Buffer();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the buffer data. */
  void* data() const;

  /** Advances the size by *nbytes*. */
  void advance_size(uint64_t nbytes);

  /** Returns the allocated buffer size. */
  uint64_t alloced_size() const;

  /** Clears the buffer, deallocating memory. */
  void clear();

  /** Returns the buffer data pointer at the current offset. */
  void* cur_data() const;

  /** Returns the buffer data pointer at the input offset. */
  void* data(uint64_t offset) const;

  /**
   * Sets `owns_data_` to `false` and thus will not destroy the data
   * in the destructor.
   */
  void disown_data();

  /** Returns the number of byte of free space in the buffer. */
  uint64_t free_space() const;

  /** Returns `true` if the buffer owns its data buffer. */
  bool owns_data() const;

  /**
   * Reallocates memory for the buffer with the input size.
   *
   * @param nbytes Number of bytes to allocate.
   * @return Status.
   */
  Status realloc(uint64_t nbytes);

  /** Resets the buffer size. */
  void reset_size();

  /** Sets the buffer size. */
  void set_size(uint64_t size);

  /**
   * Swaps this buffer with the other one. After swapping, this buffer's
   * allocation will point to the other buffer's allocation (including size,
   * offset, data ownership, etc).
   *
   * @param other Buffer to swap with.
   * @return Status
   */
  Status swap(Buffer& other);

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

  /** Copy-assign operator. */
  Buffer& operator=(const Buffer& buff);

  /** Move-assign operator. */
  Buffer& operator=(Buffer&& buff);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /**
   * True if the object owns the data buffer, which means that it is
   * responsible for allocating and freeing it.
   */
  bool owns_data_;

  /** The allocated buffer size. */
  uint64_t alloced_size_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /**
   * Ensure that the allocation is equal to or larger than the given number of
   * bytes.
   *
   * @param nbytes Minimum number of bytes to be ensured in the allocation.
   * @return Status
   */
  Status ensure_alloced_size(uint64_t nbytes);
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_BUFFER_H
