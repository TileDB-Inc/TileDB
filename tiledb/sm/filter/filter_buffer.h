/**
 * @file   filter_buffer.h
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
 * This file declares class FilterBuffer.
 */

#ifndef TILEDB_FILTER_BUFFER_H
#define TILEDB_FILTER_BUFFER_H

#include "tiledb/sm/buffer/buffer.h"
#include "tiledb/sm/buffer/const_buffer.h"
#include "tiledb/sm/filter/filter_storage.h"
#include "tiledb/sm/misc/status.h"

#include <vector>

namespace tiledb {
namespace sm {

/**
 * Class that manages an ordered list of multiple separate buffers and offers as
 * a single contiguous view on the underlying bytes.
 */
class FilterBuffer {
 public:
  /** Constructor. */
  FilterBuffer();

  /**
   * Constructor.
   *
   * @param storage FilterStorage instance to use for buffer
   *    allocation/management.
   */
  explicit FilterBuffer(FilterStorage* storage);

  /**
   * Advance the offset (global across buffers) by the given number of bytes.
   */
  void advance_offset(uint64_t nbytes);

  /**
   * Append a new buffer "view" to the end of the ordered list of buffers and
   * reset the offset.
   *
   * A buffer "view" is just a pointer into an existing buffer and associated
   * length. No bytes are copied by this function, but the underlying bytes
   * behind the view are treated as a part of this FilterBuffer for reads and
   * writes.
   *
   * @param other Buffer to "view" into
   * @param offset Offset into other
   * @param nbytes Length of the view, starting at offset.
   * @return Status
   */
  Status append_view(
      const FilterBuffer* other, uint64_t offset, uint64_t nbytes);

  /**
   * Return a list of ConstBuffers representing the multiple separate buffers
   * making up this FilterBuffer.
   */
  std::vector<ConstBuffer> buffers() const;

  /**
   * Return a pointer to the underlying buffer at the given index. It is allowed
   * to modify the contents, and offset of the returned buffer.
   *
   * Note the returned buffer is not guaranteed to "own" its data. If the
   * returned buffer does own its data, it is allowed to realloc and resize it.
   *
   * @param index Index into the underlying list of buffers.
   * @return Pointer to buffer (or nullptr for invalid index).
   */
  Buffer* buffer_ptr(unsigned index) const;

  /**
   * Reset the global offset and removes all underlying buffers.
   * @return Status
   */
  Status clear();

  /**
   * Copy the underlying bytes contiguously to the given buffer.
   *
   * @param dest Buffer to copy to
   * @return Status
   */
  Status copy_to(Buffer* dest) const;

  /**
   * Copy the underlying bytes contiguously to the given memory region.
   *
   * @param dest Memory region to copy to
   * @return Status
   */
  Status copy_to(void* dest) const;

  /** Return a pointer to the data at the current global offset. */
  void* cur_data() const;

  /**
   * Initialize this FilterBuffer with a preallocated buffer view.
   *
   * @param data Allocation to view into
   * @param nbytes Length of the view
   * @return Status
   */
  Status init(void* data, uint64_t nbytes);

  /** Return the number of underlying buffers. */
  uint64_t num_buffers() const;

  /** Return the current offset, global across all buffers. */
  uint64_t offset() const;

  /**
   * Prepend a new buffer to the front of the list of underlying buffers, and
   * reset the offset.
   *
   * The new buffer is guaranteed to have at least nbytes allocated, but note
   * the buffer is not guaranteed to own its data (i.e. you may not be able to
   * realloc buffers prepended with this function).
   *
   * @param nbytes Allocation size of the new buffer.
   * @return Status
   */
  Status prepend_buffer(uint64_t nbytes);

  /**
   * Read a number of bytes from the current global offset into the given
   * buffer.
   *
   * @param buffer Buffer to read into
   * @param nbytes Number of bytes to read
   * @return Status
   */
  Status read(void* buffer, uint64_t nbytes);

  /** Return whether this buffer is read-only. */
  bool read_only() const;

  /** Reset the global offset to 0. */
  void reset_offset();

  /**
   * Sets this buffer to a "fixed allocation." This can only happen when there
   * are no buffers already being managed.
   *
   * If a FilterBuffer is a fixed allocation, only a single prepend or
   * append_view operation is allowed on it. Prepending will return a buffer
   * encapsulating the fixed allocation. Appending a view will copy the viewed
   * data into the fixed allocation.
   *
   * Reset to the normal state by calling clear().
   *
   * @param buffer Pointer to fixed allocation
   * @param nbytes Size of fixed allocation
   * @return Status
   */
  Status set_fixed_allocation(void* buffer, uint64_t nbytes);

  /** Set the global offset to the given value. */
  void set_offset(uint64_t offset);

  /** Set the read-only state to the given value. */
  void set_read_only(bool read_only);

  /** Return the total size of all underlying buffers. */
  uint64_t size() const;

  /**
   * Swap (no copying) the contents of this FilterBuffer with the other.
   *
   * @param other Buffer to swap with
   * @return Status
   */
  Status swap(FilterBuffer& other);

  /**
   * Return a typed pointer to the value at the current global offset.
   *
   * @tparam T Type of value to point to
   * @return Pointer to value
   */
  template <typename T>
  T* value_ptr() const {
    return (T*)current_buffer_->buffer()->value_ptr(current_relative_offset_);
  }

  /**
   * Write a number of bytes from the given buffer at the current global offset.
   *
   * @param buffer Buffer to copy from
   * @param nbytes Number of bytes to copy
   * @return Status
   */
  Status write(const void* buffer, uint64_t nbytes);

 private:
  /**
   * Helper class that represents a Buffer or a "view" on an underlying Buffer.
   * In either case a shared_ptr to the underlying Buffer is maintained, which
   * prevents FilterStorage from marking a buffer as available as long as there
   * is still an active view on it.
   */
  class BufferOrView {
   public:
    /**
     * Constructor. Initializes a non-view on the given Buffer.
     */
    explicit BufferOrView(const std::shared_ptr<Buffer>& buffer);

    /**
     * Constructor. Initializes a view on the given Buffer.
     *
     * @param buffer Buffer to view
     * @param offset Offset in buffer where view will start
     * @param nbytes Length of view in bytes.
     */
    BufferOrView(
        const std::shared_ptr<Buffer>& buffer,
        uint64_t offset,
        uint64_t nbytes);

    /** Move constructor. */
    BufferOrView(BufferOrView&& other);

    /** Deleted copy constructor (due to the unique_ptr). */
    BufferOrView(const BufferOrView& other) = delete;

    /**
     * Return a Buffer instance used to access the underlying data, which will
     * either be the underlying buffer itself, or a view on it.
     */
    Buffer* buffer() const;

    /**
     * Constructs and returns a new view on this instance.
     *
     * @param offset Offset in this buffer where view will start
     * @param nbytes Length of view
     * @return The new view
     */
    BufferOrView get_view(uint64_t offset, uint64_t nbytes) const;

    /** Return true if this instance is a view. */
    bool is_view() const;

    /** Return a pointer to the underlying buffer. */
    std::shared_ptr<Buffer> underlying_buffer() const;

   private:
    /**
     * Pointer to the underlying buffer, regardless of whether this instance is
     * a view or not.
     */
    std::shared_ptr<Buffer> underlying_buffer_;

    /** True if this instance is a view on the underlying buffer. */
    bool is_view_;

    /**
     * If this instance is a view, the view Buffer (which does not own its
     * data). Otherwise nullptr.
     */
    std::unique_ptr<Buffer> view_;
  };

  /**
   * Ordered list of underlying Buffers (not all of which may own their
   * allocations).
   */
  std::list<BufferOrView> buffers_;

  /**
   * Linked list node (from buffers_) of the buffer containing the current
   * global offset.
   */
  std::list<BufferOrView>::const_iterator current_buffer_;

  /**
   * Relative offset into the current buffer, corresponding to the current
   * global offset.
   */
  uint64_t current_relative_offset_;

  /** Pointer to the fixed allocation, if any. */
  void* fixed_allocation_data_;

  /**
   * If true, a prepend or append operation is allowed when a fixed allocation
   * is set.
   */
  bool fixed_allocation_op_allowed_;

  /** Current global offset. */
  uint64_t offset_;

  /**
   * If true, the buffer can be read from, and the offset can be modified, but
   * not otherwise modified.
   */
  bool read_only_;

  /** Buffer pool used for prepend operations. */
  FilterStorage* storage_;

  /**
   * Convert a global offset into a buffer, relative offset pair.
   *
   * @param offset Global offset
   * @param list_node Set to the list node of the buffer containing the given
   *    global offset.
   * @param relative_offset Set to the relative offset in the buffer containing
   *    the given global offset.
   * @return Status
   */
  Status get_relative_offset(
      uint64_t offset,
      std::list<BufferOrView>::const_iterator* list_node,
      uint64_t* relative_offset) const;
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_FILTER_BUFFER_H
