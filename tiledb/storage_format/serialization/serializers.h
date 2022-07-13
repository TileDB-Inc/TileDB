/**
 * @file serializers.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * This file contains classes used for serialization/deserialization.
 */

#ifndef TILEDB_SERIALIZERS_H
#define TILEDB_SERIALIZERS_H

#include "tiledb/common/common.h"
#include "tiledb/common/status.h"

namespace tiledb::sm {

/**
 * A serializer that allows to write data to a preallocated buffer. Once the
 * serialization is completed, the user should call ensure_full_buffer_written
 * to make sure the right buffer was allocated for serialization. The class can
 * also be created with no arguments to run serialization without copying the
 * data, which will be used to compute the required buffer size.
 */
class Serializer {
 public:
  /**
   * Constructor with no buffer, used to compute required size.
   */
  Serializer()
      : ptr_(nullptr)
      , size_(0) {
  }

  /**
   * Constructor using a preallocated buffer.
   *
   * @param data Preallocated buffer.
   * @param size Size of the buffer.
   */
  Serializer(void* data, storage_size_t size)
      : ptr_(static_cast<uint8_t*>(data))
      , size_(size) {
  }

  DISABLE_COPY_AND_COPY_ASSIGN(Serializer);
  DISABLE_MOVE_AND_MOVE_ASSIGN(Serializer);

  /**
   * Serialize fixed size data.
   *
   * @tparam T Type of the data to write.
   * @param v data to write.
   */
  template <class T>
  void write(const T& v) {
    if (!ptr_) {
      size_ += sizeof(T);
      return;
    }

    if (sizeof(T) > size_) {
      throw std::logic_error(
          "Writting serialized data past end of allocated size.");
    }

    memcpy(ptr_, &v, sizeof(T));
    ptr_ += sizeof(T);
    size_ -= sizeof(T);
  }

  /**
   * Serialize a buffer.
   *
   * @param data data to write.
   * @param size size of the data.
   */
  void write(const void* data, storage_size_t size) {
    if (!ptr_) {
      size_ += size;
      return;
    }

    if (size > size_) {
      throw std::logic_error(
          "Writting serialized data past end of allocated size.");
    }

    memcpy(ptr_, data, size);
    ptr_ += size;
    size_ -= size;
  }

  /**
   * Ensures the full data buffer was writter to. Should be called at the end
   * of serialization.
   */
  void ensure_full_buffer_written() {
    if (ptr_ && size_ != 0) {
      throw std::logic_error("Didn't write full buffer.");
    }
  }

  /**
   * Returns the size.
   */
  inline storage_size_t size() {
    return size_;
  }

 private:
  /**
   * Pointer to the current data to be written. Will be nullptr when the
   * serializer is used to compute total size.
   */
  uint8_t* ptr_;

  /* Size left to be written or total size. */
  storage_size_t size_;
};

/**
 * A deserializer implementation using a preallocated buffer.
 */
class Deserializer {
 public:
  /**
   * Deleted default constructor.
   */
  Deserializer() = delete;

  /**
   * Constructor using a preallocated buffer.
   *
   * @param data Preallocated buffer.
   * @param size Size of the buffer.
   */
  Deserializer(const void* data, storage_size_t size)
      : ptr_(static_cast<const uint8_t*>(data))
      , size_(size) {
  }

  DISABLE_COPY_AND_COPY_ASSIGN(Deserializer);
  DISABLE_MOVE_AND_MOVE_ASSIGN(Deserializer);

  /**
   * Deserialize fixed size data.
   *
   * @tparam T Type of the data to write.
   * @return Data read.
   */
  template <class T>
  T read() {
    if (sizeof(T) > size_) {
      throw std::logic_error("Reading data past end of serialized data size.");
    }

    T ret;
    memcpy(&ret, ptr_, sizeof(T));
    ptr_ += sizeof(T);
    size_ -= sizeof(T);

    return ret;
  }

  /**
   * Return a pointer to serialized data.
   *
   * @tparam T Type of the data.
   * @param size Size of the data.
   * @return Pointer to the data.
   */
  template <class T>
  const T* get_ptr(storage_size_t size) {
    if (size > size_) {
      throw std::logic_error("Reading data past end of serialized data size.");
    }

    auto ptr = static_cast<const T*>(static_cast<const void*>(ptr_));
    ptr_ += size;
    size_ -= size;

    return ptr;
  }

  /**
   * Return the size left to be read.
   *
   * @return Size left to be read.
   */
  inline storage_size_t size() {
    return size_;
  }

 private:
  /* Pointer to the current data to be read. */
  const uint8_t* ptr_;

  /* Size left to be read. */
  storage_size_t size_;
};

}  // namespace tiledb::sm

#endif  // TILEDB_SERIALIZERS_H
