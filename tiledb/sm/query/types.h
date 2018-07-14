/**
 * @file   types.h
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
 * This file defines common types for Query/Write/Read class usage
 */

#ifndef TILEDB_TYPES_H
#define TILEDB_TYPES_H
namespace tiledb {
namespace sm {

/* ********************************* */
/*          TYPE DEFINITIONS         */
/* ********************************* */

/** Contains the buffer(s) and buffer size(s) for some attribute. */
struct AttributeBuffer {
  /**
   * The attribute buffer. In case the attribute is var-sized, this is
   * the offsets buffer.
   */
  void* buffer_;
  /**
   * For a var-sized attribute, this is the data buffer. It is `nullptr`
   * for fixed-sized attributes.
   */
  void* buffer_var_;
  /**
   * The size (in bytes) of `buffer_`. Note that this size may be altered by
   * a read query to reflect the useful data written in the buffer.
   */
  uint64_t* buffer_size_;
  /**
   * The size (in bytes) of `buffer_var_`. Note that this size may be altered
   * by a read query to reflect the useful data written in the buffer.
   */
  uint64_t* buffer_var_size_;
  /**
   * This is the original size (in bytes) of `buffer_` (before
   * potentially altered by the query).
   */
  uint64_t original_buffer_size_;
  /**
   * This is the original size (in bytes) of `buffer_var_` (before
   * potentially altered by the query).
   */
  uint64_t original_buffer_var_size_;

  /** Constructor. */
  AttributeBuffer() {
    buffer_ = nullptr;
    buffer_var_ = nullptr;
    buffer_size_ = nullptr;
    buffer_var_size_ = nullptr;
    original_buffer_size_ = 0;
    original_buffer_var_size_ = 0;
  }

  /** Constructor. */
  AttributeBuffer(
      void* buffer,
      void* buffer_var,
      uint64_t* buffer_size,
      uint64_t* buffer_var_size)
      : buffer_(buffer)
      , buffer_var_(buffer_var)
      , buffer_size_(buffer_size)
      , buffer_var_size_(buffer_var_size) {
    original_buffer_size_ = *buffer_size;
    original_buffer_var_size_ =
        (buffer_var_size_ != nullptr) ? *buffer_var_size : 0;
  }
};
}  // namespace sm
}  // namespace tiledb
#endif  // TILEDB_TYPES_H
