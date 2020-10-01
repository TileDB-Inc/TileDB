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

#include "tiledb/common/logger.h"

using namespace tiledb::common;

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
  Range()
      : range_start_size_(0) {
  }

  /** Constructor setting a range. */
  Range(const void* range, uint64_t range_size)
      : Range() {
    set_range(range, range_size);
  }

  Range(const void* range, uint64_t range_size, uint64_t range_start_size)
      : Range() {
    set_range(range, range_size, range_start_size);
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

  /** Sets a fixed-sized range serialized in `r`. */
  void set_range(const void* r, uint64_t r_size) {
    range_.resize(r_size);
    std::memcpy(&range_[0], r, r_size);
  }

  /** Sets a var-sized range serialized in `r`. */
  void set_range(const void* r, uint64_t r_size, uint64_t range_start_size) {
    range_.resize(r_size);
    std::memcpy(&range_[0], r, r_size);
    range_start_size_ = range_start_size;
  }

  /** Sets a var-sized range `[r1, r2]`. */
  void set_range_var(
      const void* r1, uint64_t r1_size, const void* r2, uint64_t r2_size) {
    range_.resize(r1_size + r2_size);
    std::memcpy(&range_[0], r1, r1_size);
    auto c = (char*)(&range_[0]);
    std::memcpy(c + r1_size, r2, r2_size);
    range_start_size_ = r1_size;
  }

  /** Sets a string range. */
  void set_str_range(const std::string& s1, const std::string& s2) {
    auto size = s1.size() + s2.size();
    if (size == 0) {
      range_.clear();
      range_start_size_ = 0;
      return;
    }

    set_range_var(s1.data(), s1.size(), s2.data(), s2.size());
  }

  /** Returns the pointer to the range flattened bytes. */
  const void* data() const {
    return range_.empty() ? nullptr : &range_[0];
  }

  /** Returns a pointer to the start of the range. */
  const void* start() const {
    return &range_[0];
  }

  /** Copies 'start' into this range's start bytes for fixed-size ranges. */
  void set_start(const void* const start) {
    if (range_start_size_ != 0)
      LOG_FATAL("Unexpected var-sized range; cannot set end range.");
    const size_t fixed_size = range_.size() / 2;
    std::memcpy(&range_[0], start, fixed_size);
  }

  /** Returns the start as a string. */
  std::string start_str() const {
    if (start_size() == 0)
      return std::string();
    return std::string((const char*)start(), start_size());
  }

  /** Returns the end as a string. */
  std::string end_str() const {
    if (end_size() == 0)
      return std::string();
    return std::string((const char*)end(), end_size());
  }

  /**
   * Returns the size of the start of the range.
   * Non-zero only for var-sized ranges.
   */
  uint64_t start_size() const {
    return range_start_size_;
  }

  /**
   * Returns the size of the end of the range.
   * Non-zero only for var-sized ranges.
   */
  uint64_t end_size() const {
    if (range_start_size_ == 0)
      return 0;
    return range_.size() - range_start_size_;
  }

  /** Returns a pointer to the end of the range. */
  const void* end() const {
    auto end_pos =
        (range_start_size_ == 0) ? range_.size() / 2 : range_start_size_;
    return range_.empty() ? nullptr : &range_[end_pos];
  }

  /** Copies 'end' into this range's end bytes for fixed-size ranges. */
  void set_end(const void* const end) {
    if (range_start_size_ != 0)
      LOG_FATAL("Unexpected var-sized range; cannot set end range.");
    const size_t fixed_size = range_.size() / 2;
    std::memcpy(&range_[fixed_size], end, fixed_size);
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
    return range_ == r.range_ && range_start_size_ == r.range_start_size_;
  }

  /** Returns true if the range start is the same as its end. */
  bool unary() const {
    assert(!range_.empty());
    bool same_size =
        range_start_size_ == 0 || 2 * range_start_size_ == range_.size();
    return same_size &&
           !std::memcmp(
               &range_[0], &range_[range_.size() / 2], range_.size() / 2);
  }

 private:
  /** The range as a flat byte vector.*/
  std::vector<uint8_t> range_;
  /**
   * Non-zero only for var-sized ranges. It is the size of the
   * start of `range_`.
   */
  uint64_t range_start_size_;
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
