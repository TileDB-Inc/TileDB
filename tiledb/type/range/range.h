/**
 * @file tiledb/type/range/range.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2022 TileDB, Inc.
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
 * This file defines the Range type for storing [low, high] closed intervals.
 */

#ifndef TILEDB_RANGE_H
#define TILEDB_RANGE_H

#include "tiledb/common/common.h"
#include "tiledb/common/logger_public.h"
#include "tiledb/sm/enums/datatype.h"

#include <cmath>
#include <cstring>
#include <sstream>
#include <string>
#include <vector>

using namespace tiledb::common;

namespace tiledb::type {

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
      : range_start_size_(0)
      , var_size_(false)
      , partition_depth_(0) {
  }

  /** Constructor setting a range. */
  Range(const void* range, uint64_t range_size)
      : Range() {
    set_range(range, range_size);
  }

  /** Constructor setting a range. */
  Range(const void* range, uint64_t range_size, uint64_t range_start_size)
      : Range() {
    set_range(range, range_size, range_start_size);
  }

  /** Constructs a range and sets fixed data. */
  Range(const void* start, const void* end, uint64_t type_size)
      : Range() {
    set_range_fixed(start, end, type_size);
  }

  /** Constructs a range and sets variable data. */
  Range(
      const void* start,
      uint64_t start_size,
      const void* end,
      uint64_t end_size)
      : Range() {
    set_range_var(start, start_size, end, end_size);
  }
  /** Copy constructor. */
  Range(const Range&) = default;

  /** Move constructor. */
  Range(Range&&) = default;

  /** Destructor. */
  ~Range() = default;

  /** Copy-assign operator.*/
  Range& operator=(const Range&) = default;

  /** Move-assign operator. */
  Range& operator=(Range&&) = default;

  /** Sets a fixed-sized range serialized in `r`. */
  void set_range(const void* r, uint64_t r_size) {
    range_.resize(r_size);
    std::memcpy(range_.data(), r, r_size);
    var_size_ = false;
  }

  /** Sets a var-sized range serialized in `r`. */
  void set_range(const void* r, uint64_t r_size, uint64_t range_start_size) {
    range_.resize(r_size);
    std::memcpy(range_.data(), r, r_size);
    range_start_size_ = range_start_size;
    var_size_ = true;
  }

  /**
   * Sets a fixed-sixed range with start and end serialized separately.
   *
   * @param start Location of serialized starting element
   * @param end Location of serialixed ending element
   * @param type_size The size of a single element of the deserialized data.
   */
  void set_range_fixed(const void* start, const void* end, uint64_t type_size) {
    range_.resize(2 * type_size);
    std::memcpy(&range_[0], start, type_size);
    std::memcpy(&range_[type_size], end, type_size);
    range_start_size_ = type_size;
    var_size_ = false;
  }

  /** Sets a var-sized range `[r1, r2]`. */
  void set_range_var(
      const void* r1, uint64_t r1_size, const void* r2, uint64_t r2_size) {
    range_.resize(r1_size + r2_size);
    std::memcpy(range_.data(), r1, r1_size);
    auto c = (char*)(range_.data());
    std::memcpy(c + r1_size, r2, r2_size);
    range_start_size_ = r1_size;
    var_size_ = true;
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
  inline const void* data() const {
    return range_.empty() ? nullptr : range_.data();
  }

  /**
   * Returns the pointer to the range as flattened bytes of the requested type.
   */
  template <typename T>
  inline const T* typed_data() const {
    assert(!var_size_);
    assert(range_.empty() || (range_.size() == 2 * sizeof(T)));
    return range_.empty() ? nullptr : (T*)range_.data();
  }

  /** Returns a pointer to the start of the range. */
  inline const void* start_fixed() const {
    assert(!var_size_);
    assert(range_.size() != 0);
    return range_.data();
  }

  /** Copies 'start' into this range's start bytes for fixed-size ranges. */
  void set_start_fixed(const void* const start) {
    if (var_size_) {
      auto msg = "Unexpected var-sized range; cannot set start range.";
      LOG_ERROR(msg);
      throw std::runtime_error(msg);
    }

    const size_t fixed_size = range_.size() / 2;
    std::memcpy(range_.data(), start, fixed_size);
  }

  /** Returns the start as a string view. */
  std::string_view start_str() const {
    if (range_start_size_ == 0) {
      return {nullptr, 0};
    }
    return std::string_view(
        reinterpret_cast<const char*>(range_.data()), range_start_size_);
  }

  /** Returns the end as a string view. */
  std::string_view end_str() const {
    // Must be variable
    // or have no data if 'fixed' (primarily default construction)
    assert(var_size_ || !range_.size());
    auto size = range_.size() - range_start_size_;
    if (size == 0) {
      return {nullptr, 0};
    }

    return std::string_view(
        reinterpret_cast<const char*>(range_.data()) + range_start_size_, size);
  }

  /**
   * Returns the size of the start of the range.
   * Non-zero only for var-sized ranges.
   */
  uint64_t start_size() const {
    if (!var_size_)
      return 0;
    return range_start_size_;
  }

  /**
   * Returns the size of the end of the range.
   * Non-zero only for var-sized ranges.
   */
  uint64_t end_size() const {
    if (!var_size_)
      return 0;
    return range_.size() - range_start_size_;
  }

  /** Returns a pointer to the end of the range. */
  const void* end_fixed() const {
    assert(!var_size_);
    assert(range_.size() != 0);
    auto end_pos = range_.size() / 2;
    return &range_[end_pos];
  }

  /** Copies 'end' into this range's end bytes for fixed-size ranges. */
  void set_end_fixed(const void* const end) {
    if (var_size_) {
      auto msg = "Unexpected var-sized range; cannot set end range.";
      LOG_ERROR(msg);
      throw std::runtime_error(msg);
    }
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
    // If the range is empty, then it corresponds to strings
    // covering the whole domain (so it is not unary)
    if (range_.empty())
      return false;

    bool same_size = !var_size_ || 2 * range_start_size_ == range_.size();
    return same_size &&
           !std::memcmp(
               range_.data(), &range_[range_.size() / 2], range_.size() / 2);
  }

  /** True if the range is variable sized. */
  bool var_size() const {
    return var_size_;
  }

  /** Sets the partition depth. */
  void set_partition_depth(uint64_t partition_depth) {
    partition_depth_ = partition_depth;
  }

  /** Returns the partition depth. */
  uint64_t partition_depth() const {
    return partition_depth_;
  }

 private:
  /** The range as a flat byte vector. */
  std::vector<uint8_t> range_;

  /** The size of the start of `range_`. */
  uint64_t range_start_size_;

  /** Is the range var sized. */
  bool var_size_;

  /**
   * The ranges in a query's initial subarray have a depth of 0.
   * When a range is split, the depth on the split ranges are
   * set to +1 the depth of the original range.
   */
  uint64_t partition_depth_;
};

/**
 * Performs checks to verify a range is a subset of the superset.
 *
 * Both the superset and the range must be valid ranges of the appropriate type
 * as checked by ``check_range_is_valid``.
 *
 * @param superset The range that is the assumed superset. Must be a valid range
 * with data of type T.
 * @param range The range that is the assumed subset. Must be a valid
 * range with data of type T.
 * @return Status of the checks.
 */
template <
    typename T,
    typename std::enable_if<std::is_arithmetic<T>::value>::type* = nullptr>
Status check_range_is_subset(const Range& superset, const Range& range) {
  auto domain = (const T*)superset.data();
  auto r = (const T*)range.data();
  if (r[0] < domain[0] || r[1] > domain[1]) {
    std::stringstream ss;
    ss << "Range [" << r[0] << ", " << r[1] << "] is out of domain bounds ["
       << domain[0] << ", " << domain[1] << "]";
    return Status_RangeError(ss.str());
  }
  return Status::Ok();
};

/**
 * Performs correctness checks for a valid range. If any validity checks fail
 * an exception is thrown.
 *
 * @param range The range to check.
 */
template <
    typename T,
    typename std::enable_if<std::is_arithmetic<T>::value>::type* = nullptr>
void check_range_is_valid(const Range& range) {
  // Check has data.
  if (range.empty())
    throw std::invalid_argument("Range is empty");
  auto r = (const T*)range.data();
  // Check for NaN
  if constexpr (std::is_floating_point_v<T>) {
    if (std::isnan(r[0]) || std::isnan(r[1]))
      throw std::invalid_argument("Range contains NaN");
  }
  // Check range bounds
  if (r[0] > r[1])
    throw std::invalid_argument(
        "Lower range bound " + std::to_string(r[0]) +
        " cannot be larger than the higher bound " + std::to_string(r[1]));
};

/**
 * Crop a range to the requested bounds.
 *
 * @param bounds Bounds to crop the range to. Must be a valid range with data of
 * type T.
 * @param range The range to crop. Must be a valid range with data of type T.
 */
template <
    typename T,
    typename std::enable_if<std::is_arithmetic<T>::value>::type* = nullptr>
void crop_range(const Range& bounds, Range& range) {
  auto bounds_data = (const T*)bounds.data();
  auto range_data = (T*)range.data();
  range_data[0] = std::max(bounds_data[0], range_data[0]);
  range_data[1] = std::min(bounds_data[1], range_data[1]);
};

/**
 * Returns a string representation of the range.
 *
 * @param range The range to get a string representation of.
 */
std::string range_str(const Range& range, const tiledb::sm::Datatype type);

}  // namespace tiledb::type

#endif
