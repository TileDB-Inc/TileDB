/**
 * @file   buffer_list.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2019-2021 TileDB, Inc.
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
 * This file defines class BufferList.
 */

#ifndef TILEDB_BUFFER_LIST_H
#define TILEDB_BUFFER_LIST_H

#include <vector>

#include "tiledb/common/status.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class Buffer;

/**
 * A simple flat list of Buffers. This class also offers some convenience
 * functions for reading from the list of buffers as if it was a contiguous
 * buffer.
 */
class BufferList {
 public:
  /** Constructor. */
  BufferList();

  /**
   * Adds the given buffer to the list.
   *
   * This BufferList takes ownership of the given Buffer instance (which is why
   * it takes an rvalue reference). This is to support efficient appends without
   * having to make potentially large memcpy calls.
   *
   * @param buffer The buffer to add
   * @return Status
   */
  Status add_buffer(Buffer&& buffer);

  /**
   * Gets the Buffer in the list at the given index.
   *
   * @param index Index of buffer to get
   * @param buffer Set to point to the buffer instance
   * @return Status
   */
  Status get_buffer(uint64_t index, const Buffer** buffer) const;

  /** Returns the number of buffers in the list. */
  uint64_t num_buffers() const;

  /**
   * Reads from the current offset into the given destination.
   *
   * Returns an error if the buffers contain less than the requested number of
   * bytes starting from the current offset.
   *
   * @param dest The buffer to read the data into. If null then will perform
   * seek.
   * @param nbytes The number of bytes to read.
   * @return Status
   */
  Status read(void* dest, uint64_t nbytes);

  /**
   * Similar to `Status read(void* dest, uint64_t nbytes)` but does not return
   * an error if more bytes are requested than exist in the buffers.
   *
   * @param dest The buffer to read the data into.
   * @param nbytes The maximum number of bytes to read.
   * @param bytes_read Will be set to the number of bytes actually read.
   * @return Status
   */
  Status read_at_most(void* dest, uint64_t nbytes, uint64_t* bytes_read);

  /**
   * Seek to an offset, similar to lseek or fseek
   *
   * Whence options are:
   *
   *  SEEK_SET
   *    The offset is set to offset bytes.
   *
   *  SEEK_CUR
   *    The offset is set to its current location plus offset bytes
   *
   *  SEEK_END
   *    This is not supported. Its purpose in lseek would be to set
   *    the offset to the size of the BufferList plus offset bytes.
   *
   * @param offset Offset to seek to.
   * @param whence Location to seek from.
   * @return Status
   */
  Status seek(off_t offset, int whence);

  /** Resets the current offset for reading. */
  void reset_offset();

  /** Returns the sum of sizes of all buffers in the list. */
  uint64_t total_size() const;

  /**
   * Sets the current offsets for reading.
   *
   * @param current_buffer_index The index of the current buffer in the list.
   * @param current_relative_offset The current relative offset within the
   * current buffer.
   *
   * */
  void set_offset(
      const size_t current_buffer_index,
      const uint64_t current_relative_offset);

  /**
   * Returns the current offsets.
   *
   * @return The index of the current buffer in the list.
   * @return The current relative offset within the current buffer.
   */
  std::tuple<size_t, uint64_t> get_offset() const;

 private:
  /** The underlying list of Buffers. */
  std::vector<Buffer> buffers_;

  /** The index of the buffer containing the current global offset. */
  size_t current_buffer_index_;

  /** The current relative offset within the current buffer. */
  uint64_t current_relative_offset_;

  /** The current global offset. */
  uint64_t offset_;

  /**
   * Reads from the current offset into the given destination.
   *
   * @param dest The buffer to read the data into. If null will perform seek.
   * @param nbytes The number of bytes to read.
   * @param bytes_read Set to the number of bytes actually read.
   * @return Status
   */
  Status read(void* dest, uint64_t nbytes, uint64_t* bytes_read);
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_BUFFER_LIST_H
