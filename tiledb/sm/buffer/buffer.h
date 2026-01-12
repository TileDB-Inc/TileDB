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
 * This file defines classes BufferBase, Buffer, ConstBuffer, and
 * PreallocatedBuffer.
 */

#ifndef TILEDB_BUFFER_H
#define TILEDB_BUFFER_H

#include <cinttypes>
#include "tiledb/common/pmr.h"
#include "tiledb/common/status.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

class BufferStatusException : public StatusException {
 public:
  explicit BufferStatusException(const std::string& msg)
      : StatusException("Buffer", msg) {
  }
};

class ConstBuffer;

/* ****************************** */
/*          BufferBase            */
/* ****************************** */
/**
 * Base class for `Buffer`, `ConstBuffer`, and `PreallocatedBuffer`
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

  /** Predicate "offset is at the end of the buffer". */
  bool end() const;

  /** Returns the buffer data as a pointer to constant. */
  const void* data() const;

  /** Returns pointer to data at the current offset. */
  const void* cur_data() const;

  /** Returns the data pointer as a specific type. */
  template <typename T>
  inline T* data_as() const {
    return static_cast<T*>(data_);
  }

  /** Returns the buffer data as bytes (this declaration is seemingly redundant
   * but helps with Rust FFI declarations) */
  const uint8_t* bytes() const {
    return data_as<uint8_t>();
  }

  /**
   * Reads from the local data into the input buffer.
   *
   * @param destination The buffer to read the data into.
   * @param nbytes The number of bytes to read.
   * @return Status
   */
  Status read(void* destination, uint64_t nbytes);

  /**
   * Wraps the read function, converting Status error to a runtime exception
   * using the name of the requested variable.
   *
   * If read failed, a runtime exception will be throw with the message:
   *
   *   "Failed to load " + variable_description + "."
   *
   * @param destination The buffer to read the data into.
   * @param nbytes The number of bytes to read.
   * @param variable_description Name of the variable to use if read fails.
   */
  inline void read(
      void* destination,
      uint64_t nbytes,
      const std::string& variable_description) {
    auto st = read(destination, nbytes);
    if (!st.ok())
      throw std::runtime_error("Failed to load " + variable_description + ".");
  }

  /**
   * Reads from the local data at an offset into the input buffer.
   *
   * @param destination The buffer to read the data into.
   * @param offset The input offset.
   * @param nbytes The number of bytes to read.
   * @return Status
   */
  Status read(void* destination, uint64_t offset, uint64_t nbytes);

  /**
   * Implicit conversion operator to span.
   *
   * @return A span to the buffer's whole data.
   */
  operator span<const char>() const&;

  /**
   * Returns a span to the buffer's data after the offset.
   */
  span<const char> cur_span() const&;

 protected:
  BufferBase();
  BufferBase(void* data, uint64_t size);
  BufferBase(const void* data, uint64_t size);

  /** Returns the buffer data. */
  void* nonconst_data() const;

  /** Returns the buffer at the current offset */
  void* nonconst_unread_data() const;

  /** Throws range_error if offset is not valid. Does nothing otherwise. */
  void assert_offset_is_valid(uint64_t offset) const;

  /** The buffer data. */
  /**
   * @invariant If data_ does not change across a class method, then neither
   * does data_[0..size_). In other words, the data is treated as constant.
   */
  void* data_;

  /** Size of the buffer data. */
  uint64_t size_;

  /** The current buffer position in bytes, i.e. sizeof(char). */
  /**
   * @invariant offset_ <= size_
   */
  uint64_t offset_;
};

/* ****************************** */
/*            Buffer              */
/* ****************************** */
/**
 * General-purpose buffer. Manages own memory. Writeable.
 */
class Buffer : public BufferBase {
 public:
  /** Default constructor. */
  Buffer();

  /**
   * Owning constructor for preallocated fixed size Buffer.
   * The resulting Buffer object will have ownership of it's data.
   * Buffers using this constructor will never reallocate.
   *
   * @param size The size in bytes to preallocate.
   */
  Buffer(uint64_t size);

  /**
   * Non-owning constructor.
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
  Buffer(Buffer&& buff) noexcept;

  /** Copy-assign operator. */
  Buffer& operator=(const Buffer& buff);

  /** Move-assign operator. */
  Buffer& operator=(Buffer&& buff);

  /** Destructor. */
  ~Buffer();

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

  template <typename T>
  inline T* cur_data_as() const {
    return static_cast<T*>(nonconst_unread_data());
  }

  /** Returns the buffer data pointer at the input offset. */
  void* data(uint64_t offset) const;

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
   */
  void swap(Buffer& other);

  /**
   * Returns the value of type T at the input offset.
   *
   * @tparam T The type of the value to return.
   * @param offset The offset from which to retrieve the value.
   * @return The requested value.
   */
  template <class T>
  T value(uint64_t offset) const {
    assert_offset_is_valid(offset);
    return ((T*)(((char*)data_) + offset))[0];
  }

  /**
   * Returns the pointer to the value the input offset.
   *
   * @param offset The offset from which to retrieve the value pointer.
   * @return The requested pointer.
   */
  void* value_ptr(uint64_t offset) const {
    assert_offset_is_valid(offset);
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
   * input buffer *buff*.
   *
   * @param buff The buffer to read from.
   * @param nbytes Number of bytes to write.
   * @return Status.
   */
  Status write(ConstBuffer* buff, uint64_t nbytes);

  /**
   * Writes exactly *nbytes* into the local buffer by reading from the
   * input buffer *buffer*.
   *
   * @param buffer The buffer to read from.
   * @param nbytes Number of bytes to write.
   * @return Status.
   */
  Status write(const void* buffer, uint64_t nbytes);

  /**
   * Writes exactly *nbytes* into the local buffer at offset 'offset'
   * by reading from the input buffer *buffer*.
   *
   * @param buffer The buffer to read from.
   * @param offset The offset to write in the local buffer.
   * @param nbytes Number of bytes to write.
   * @return Status.
   */
  Status write(const void* buffer, uint64_t offset, uint64_t nbytes);

 private:
  /**
   * True if the object owns the data buffer, which means that it is
   * responsible for allocating and freeing it.
   */
  bool owns_data_;

  /**
   * True if the buffer is preallocated to a fixed size.
   * If this flag is set an error will be thrown when trying to reallocate.
   */
  bool preallocated_ = false;

  /** The allocated buffer size. */
  uint64_t alloced_size_;

  /**
   * Ensure that the allocation is equal to or larger than the given number of
   * bytes.
   *
   * @param nbytes Minimum number of bytes to be ensured in the allocation.
   * @return Status
   */
  Status ensure_alloced_size(uint64_t nbytes);
};

/* ****************************** */
/*          ConstBuffer           */
/* ****************************** */
/**
 * A read-only buffer fully-initialized at construction. It does not manage
 * memory; its storage is subordinate to some other object.
 */
class ConstBuffer : public BufferBase {
 public:
  /** Default constructor deleted. */
  ConstBuffer() = delete;

  /**
   * Ordinary constructor.
   *
   * @param data The data of the buffer.
   * @param size The size of the buffer.
   */
  ConstBuffer(const void* data, uint64_t size);

  /**
   * Constructor from a general-purpose buffer.
   *
   * @param buff The buffer the object will encapsulate, working on its
   *     data and size, but using a separate local offset, without affecting
   *     the input buffer.
   */
  explicit ConstBuffer(Buffer* buff);

  /** Destructor. */
  ~ConstBuffer() = default;

  /** Returns the number of bytes left for reading. */
  uint64_t nbytes_left_to_read() const;

  /**
   * Returns a value from the buffer of type T.
   *
   * @tparam T The type of the value to be read.
   * @param offset The offset in the local buffer to read from.
   * @return The desired value of type T.
   */
  template <class T>
  inline T value(uint64_t offset) const {
    assert_offset_is_valid(offset);
    return ((const T*)(((const char*)data_) + offset))[0];
  }

  /**
   * Returns the value at the current offset of the buffer of type T.
   *
   * @tparam T The type of the value to be returned.
   * @return The value to be returned of type T.
   */
  template <class T>
  inline T value() const {
    return ((const T*)(((const char*)data_) + offset_))[0];
  }
};

/**
 * Manages a byte buffer that is used for capnp (de)serialization.
 *
 * The buffer can be either owned by this class (happens in cases of
 * serialization) or not (typically user-managed, happens in cases of
 * deserialization).
 */
class SerializationBuffer {
 private:
  /**
   * Manages the memory of the buffer, if it is owned by this class.
   *
   * In the case of non-owned buffers, this vector is empty. The vector is not
   * wrapped in an std::optional to avoid losing the allocator when reassigning
   * between owned and non-owned buffers.
   *
   * @invariant Unless buffer_owner_ gets cleared to assign a non-owned buffer,
   * after updating the content of buffer_owner_, buffer_ must be set to the
   * memory pointed by buffer_owner_.
   */
  tdb::pmr::vector<char> buffer_owner_;

  /**
   * The buffer's content. In the case of owned buffers, this points to the
   * same memory as buffer_owner_.
   */
  span<const char> buffer_;

 public:
  /** Marker type for non-owned buffer assign methods. */
  class NonOwnedMarker {};

  /** Singleton instance of NonOwnedMarker. */
  static constexpr NonOwnedMarker NonOwned{};

  /**
   * The type of the allocator used by the buffer. This is required to make the
   * type allocator-aware.
   */
  using allocator_type = decltype(buffer_owner_)::allocator_type;

  /**
   * Constructor.
   *
   * @param alloc Allocator for owned buffers.
   */
  SerializationBuffer(const allocator_type& alloc)
      : buffer_owner_(alloc)
      , buffer_(buffer_owner_) {
  }

  DISABLE_COPY_AND_COPY_ASSIGN(SerializationBuffer);
  DISABLE_MOVE_AND_MOVE_ASSIGN(SerializationBuffer);

  /**
   * Constructor for an owned buffer of a given size.
   *
   * The data is intended to be modified later with the owned_mutable_span()
   * function.
   *
   * @param size The size of the buffer.
   * @param alloc Allocator for the buffer.
   */
  SerializationBuffer(size_t size, const allocator_type& alloc)
      : buffer_owner_(size, alloc)
      , buffer_(buffer_owner_) {
  }

  /**
   * Shorthand constructor for a non-owned buffer to a given pointer-size pair.
   *
   * @param data The data of the buffer.
   * @param size The size of the buffer.
   */
  SerializationBuffer(
      NonOwnedMarker,
      const void* data,
      size_t size,
      const allocator_type& alloc)
      : buffer_owner_(alloc)
      , buffer_(static_cast<const char*>(data), size) {
  }

  /**
   * Returns whether this class owns the underlying memory buffer and is
   * responsible for freeing it.
   */
  bool is_owned() const {
    return buffer_.data() == buffer_owner_.data();
  }

  /**
   * Assigns a new owned buffer to the object.
   *
   * @param iter The iterator range to copy to the serialization buffer.
   */
  template <class Iter>
  inline void assign(const Iter& iter) {
    // Clear vector and deallocate its buffer.
    buffer_owner_.clear();
    buffer_owner_.shrink_to_fit();
    buffer_owner_.assign(std::cbegin(iter), std::cend(iter));
    buffer_ = buffer_owner_;
  }

  /**
   * Assigns a new non-owned buffer to the object.
   *
   * @param iter The iterator range to wrap in the serialization buffer.
   */
  template <class Iter>
  inline void assign(NonOwnedMarker, const Iter& iter) {
    // Clear vector and deallocate its buffer.
    buffer_owner_.clear();
    buffer_owner_.shrink_to_fit();
    buffer_ = iter;
  }

  /**
   * Gets the number of bytes in the buffer.
   */
  inline size_t size() const {
    return buffer_.size();
  }

  /**
   * Returns a mutable span to the buffer's whole data.
   *
   * This function must be called only on owned buffers, otherwise it will
   * throw.
   *
   * The span returned by this function must not be used after one of the assign
   * methods is called.
   *
   * @return A mutable span to the buffer's whole data.
   */
  inline span<char> owned_mutable_span() & {
    if (!is_owned())
      throw BufferStatusException(
          "Cannot get a mutable span of a non-owned buffer.");
    return buffer_owner_;
  }

  /**
   * Implicit conversion operator to span.
   *
   * @return A span to the buffer's whole data.
   */
  inline operator span<const char>() const& {
    return buffer_;
  }
};

/* ****************************** */
/*       PreallocatedBuffer       */
/* ****************************** */
/**
 * Writeable buffer that uses pre-allocated storage provided externally. Does
 * not expand storage on write.
 */
class PreallocatedBuffer : public BufferBase {
 public:
  /** Default constructor deleted. */
  PreallocatedBuffer() = delete;

  /**
   * Constructor.
   *
   * @param data The data of the buffer.
   * @param size The size of the buffer.
   */
  PreallocatedBuffer(const void* data, uint64_t size);

  /** Returns the buffer data pointer at the current offset. */
  void* cur_data() const;

  /** Returns the "free space" in the buffer, which is the size minus the
   * current offset. */
  uint64_t free_space() const;

  /**
   * Returns a value from the buffer of type T.
   *
   * @tparam T The type of the value to be read.
   * @param offset The offset in the local buffer to read from.
   * @return The desired value of type T.
   */
  template <class T>
  inline T value(uint64_t offset) const {
    assert_offset_is_valid(offset);
    return ((const T*)(((const char*)data_) + offset))[0];
  }

  /**
   * Returns the value at the current offset of the buffer of type T.
   *
   * @tparam T The type of the value to be returned.
   * @return The value to be returned of type T.
   */
  template <class T>
  inline T value() const {
    return ((const T*)(((const char*)data_) + offset_))[0];
  }

  /**
   * Writes exactly *nbytes* into the local buffer by reading from the
   * input buffer *buf*.
   *
   * @param buffer The buffer to read from.
   * @param nbytes Number of bytes to write.
   * @return Status.
   */
  Status write(const void* buffer, uint64_t nbytes);
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_BUFFER_H
