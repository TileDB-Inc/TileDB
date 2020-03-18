/**
 * @file   types.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB, Inc.
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

#include <cassert>
#include <cstring>
#include <vector>

namespace tiledb {
namespace sm {

/* ********************************* */
/*          TYPE DEFINITIONS         */
/* ********************************* */

/**
 * Defines a 1D range (low, high), flattened in a sequence of bytes.
 * If the range consists of var-sized values (e.g., strings), then
 * the format is:
 *
 * low_nbytes (uint32) | low | high_nbytes (uint32) | high
 */
class Range {
 public:
  /** Default constructor. */
  Range() = default;

  /** Constructor setting a range. */
  Range(const void* range, uint64_t range_size) {
    set_range(range, range_size);
  }

  /** Copy constructor. */
  Range(const Range& range) = default;

  /** Move constructor. */
  Range(Range&& range) = default;

  /** Destructor. */
  ~Range() = default;

  /** Copy-assign operator.*/
  Range& operator=(const Range& range) = default;

  /** Move-assign operator. */
  Range& operator=(Range&& range) = default;

  /** Sets a range. */
  void set_range(const void* range, uint64_t range_size) {
    range_.resize(range_size);
    std::memcpy(&range_[0], range, range_size);
  }

  /** Returns the pointer to the range flattened bytes. */
  const void* data() const {
    return &range_[0];
  }

  /** Returns a pointer to the start of the range. */
  const void* start() const {
    return &range_[0];
  }

  const void* end() const {
    return &range_[range_.size() / 2];
  }

  /** Returns true if the range is empty. */
  bool empty() const {
    return range_.empty();
  }

  /** Clears the range. */
  void clear() {
    range_.clear();
  }

  /** Returns the range size in bytes. */
  uint64_t size() const {
    return range_.size();
  }

  /** Equality operator. */
  bool operator==(const Range& r) const {
    return range_ == r.range_;
  }

  /** Returns true if the range start is the same as its end. */
  bool unary() const {
    assert(!range_.empty());
    return !std::memcmp(
        &range_[0], &range_[range_.size() / 2], range_.size() / 2);
  }

 private:
  /** The range as a flat byte vector.*/
  std::vector<uint8_t> range_;
};

/** An N-dimensional range, consisting of a vector of 1D ranges. */
typedef std::vector<Range> NDRange;

/** A value as a vector of bytes. */
typedef std::vector<uint8_t> ByteVecValue;

/** A byte vector. */
typedef std::vector<uint8_t> ByteVec;

/** Contains the buffer(s) and buffer size(s) for some attribute/dimension. */
struct QueryBuffer {
  /**
   * The attribute/dimension buffer. In case the attribute/dimension is
   * var-sized, this is the offsets buffer.
   */
  void* buffer_;
  /**
   * For a var-sized attribute/dimension, this is the data buffer. It is
   * `nullptr` for fixed-sized attributes/dimensions.
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
  QueryBuffer() {
    buffer_ = nullptr;
    buffer_var_ = nullptr;
    buffer_size_ = nullptr;
    buffer_var_size_ = nullptr;
    original_buffer_size_ = 0;
    original_buffer_var_size_ = 0;
  }

  /** Constructor. */
  QueryBuffer(
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
