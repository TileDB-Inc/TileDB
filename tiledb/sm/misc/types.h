/**
 * @file   types.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2023 TileDB, Inc.
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

#include <vector>
#include "tiledb/type/range/range.h"

using namespace tiledb::type;

namespace tiledb::sm {

/* ********************************* */
/*          TYPE DEFINITIONS         */
/* ********************************* */

/** An N-dimensional range, consisting of a vector of 1D ranges. */
using NDRange = std::vector<Range>;
using NDRangeView = std::span<const Range>;

/** An untyped value, barely more than raw storage. This class is only
 * transitional. All uses should be rewritten to use ordinary types. Consider
 * it deprecated at creation.
 *
 * This class started off as a typedef for a byte vector. In its current state,
 * it provides methods that capture common patterns of usage, avoiding bleeding
 * all its abstraction into calling code. It's not perfect, and never will be.
 *
 * A minimal number of vector methods are forwarded outside the class to allow
 * not-yet-converted code its legacy behavior. The incremental goal is to remove
 * such functions as the code base evolves away from untyped variables entirely.
 */

class ByteVecValue {
  using Base = std::vector<uint8_t>;
  Base x_;

  template <class T>
  Base convert_to_bytes(std::initializer_list<T> y) {
    Base x{};
    auto from{reinterpret_cast<const uint8_t*>(y.begin())};
    Base::size_type length{sizeof(T) * y.size()};
    x.resize(length);
    auto to{x.data()};
    for (decltype(length) i{0}; i < length; ++i) {
      *to++ = *from++;
    }
    return x;
  }

 public:
  using size_type = Base::size_type;
  using reference = Base::reference;
  /** Default constructor */
  ByteVecValue()
      : x_() {
  }
  /** Fixed-size constructor */
  explicit ByteVecValue(Base::size_type n)
      : x_(n) {
  }

  /**
   * Constructor from initializer list
   */
  template <class T>
  ByteVecValue(std::initializer_list<T> y)
      : x_(convert_to_bytes(y)) {
  }

  /** Move constructor from underlying vector type */
  explicit ByteVecValue(std::vector<uint8_t>&& y)
      : x_(std::move(y)) {
  }

  /**
   * Performs an assignment as if a variable of type T were located at the
   * beginning of storage.
   *
   * @post size() >= sizeof(T)
   *
   * @tparam T
   * @return A reference to the phantom variable which was assigned.
   */
  template <class T>
  T& assign_as(T val = T()) {
    if (size() < sizeof(T))
      x_.resize(sizeof(T));
    T& a = *reinterpret_cast<T*>(data());
    a = val;
    return a;
  }

  /// Remove any existing value.
  void assign_as_void() noexcept {
    x_.clear();
  }

  /**
   * Returns the value of a variable of type T as if it were located at the
   * beginning of storage.
   *
   * Intentionally unimplemented in general and only certain specializations
   * are available.
   *
   * @tparam T
   * @return
   */
  template <class T>
  T rvalue_as() const;

  /// Forwarded from vector
  void resize(size_type count) {
    x_.resize(count);
  }
  /// Forwarded from vector
  void shrink_to_fit() {
    x_.shrink_to_fit();
  }
  /// Forwarded from vector
  uint8_t* data() noexcept {
    return x_.data();
  }
  /// Forwarded from vector
  [[nodiscard]] const uint8_t* data() const noexcept {
    return x_.data();
  }
  /// Forwarded from vector
  [[nodiscard]] Base::size_type size() const noexcept {
    return x_.size();
  }
  /**
   * Conversion to boolean in the style of std::optional.
   * @return True if a value is present, false otherwise.
   */
  explicit operator bool() const noexcept {
    return !x_.empty();
  }
};

/** A byte vector. */
using ByteVec = std::vector<uint8_t>;

/** The chunk info, buffers and offsets */
struct ChunkData {
  struct DiskLayout {
    uint32_t unfiltered_data_size_;
    uint32_t filtered_data_size_;
    uint32_t filtered_metadata_size_;
    void* filtered_metadata_;
    void* filtered_data_;
  };

  std::vector<uint64_t> chunk_offsets_;
  std::vector<DiskLayout> filtered_chunks_;
};

}  // namespace tiledb::sm

#endif  // TILEDB_TYPES_H
