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
#include "uri.h"

namespace tiledb {

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
  void advance_offset(uint64_t nbytes) {
    offset_ += nbytes;
  }

  /** Clears the buffer. */
  Status clear();

  /** Returns the buffer data. */
  inline void* data() const {
    return data_;
  }

  /** Checks if the buffer is full. */
  inline bool full() const {
    return offset_ == size_;
  }

  /**
   * Maps a region of a file to the buffer.
   *
   * @param filename The name of the file to map.
   * @param size The size of the region to map.
   * @param offset The offset in the file where the mapping will start.
   * @param read_only Indicates if the buffer will be read-only.
   * @return Status.
   */
  Status mmap(
      const uri::URI& filename,
      uint64_t size,
      uint64_t offset,
      bool read_only = true);

  /** Unmaps the region from a file. */
  Status munmap();

  /** Returns the current offset in the buffer. */
  inline uint64_t offset() const {
    return offset_;
  }

  /** Reads from the local data into the input buffer. */
  Status read(void* buffer, uint64_t nbytes);

  /**
   * Reallocates memory for the buffer with the input size.
   *
   * @param size Size to allocate.
   * @return Status.
   */
  Status realloc(uint64_t size);

  /** Resets the buffer offset. */
  inline void reset_offset() {
    offset_ = 0;
  }

  /** Sets the buffer offset. */
  inline void set_offset(uint64_t offset) {
    offset_ = offset;
  }

  /** Sets the buffer size. */
  inline void set_size(uint64_t size) {
    size_ = size;
  }

  /** Returns the buffer size. */
  inline uint64_t size() const {
    return size_;
  }

  /** Returns the value of type T at the input offset. */
  template <class T>
  inline T value(uint64_t offset) {
    return ((T*)(((char*)data_) + offset))[0];
  }

  /** Returns the value of type T at the current offset. */
  template <class T>
  inline T value() {
    return ((T*)(((char*)data_) + offset_))[0];
  }

  /**
   * Writes into the local buffer by reading as much data as possible from
   * the input buffer. No new memory is allocated for the local buffer.
   *
   * @param buf The buffer to read from.
   * @return void.
   */
  void write(ConstBuffer* buf);

  /**
   * Writes exactly *nbytes* into the local buffer by reading from the
   * input buffer *buf*.
   *
   * @param buf The buffer to read from.
   * @param nbytes Number of bytes to write.
   * @return Status.
   */
  Status write(ConstBuffer* buf, uint64_t nbytes);

  /**
   * Writes exactly *nbytes* into the local buffer by reading from the
   * input buffer *buf*.
   *
   * @param buf The buffer to read from.
   * @param nbytes Number of bytes to write.
   * @return Status.
   */
  Status write(const void* buf, uint64_t nbytes);

  void write_with_shift(ConstBuffer* buf, uint64_t offset);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The buffer data. */
  void* data_;

  /** The current buffer offset. */
  uint64_t offset_;

  /** Pointer where the file region is mapped. */
  void* mmap_data_;

  /** Size of the mapped region. */
  uint64_t mmap_size_;

  /** Buffer allocated size. */
  uint64_t size_;
};

}  // namespace tiledb

#endif  // TILEDB_BUFFER_H
