/**
 * @file   tiledb/common/vec_view.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * A centralized definition of the polymorphic resource types used by
 * TileDB.
 */

#ifndef TILEDB_COMMON_VEC_VIEW_H
#define TILEDB_COMMON_VEC_VIEW_H

namespace tiledb::stdx {

template <typename T>
struct vec_view {
  using const_reference = const T&;
  using const_iterator = const T*;
  using const_reverse_iterator = std::vector<T>::reverse_iterator;
  using size_type = std::vector<T>::size_type;

  template <typename... Args>
  vec_view(const std::vector<T, Args...>& source)
      : sourcebegin_(source.data())
      , sourceend_(sourcebegin_ + source.size()) {
    static_assert(
        !std::is_same_v<bool, T>,
        "vec_view cannot be used with bool due to specialization");
  }

  const_iterator begin() const {
    return sourcebegin_;
  }

  const_iterator end() const {
    return sourceend_;
  }

  bool empty() const {
    return (begin() == end());
  }

  size_t size() const {
    return (end() - begin());
  }

  const_reference operator[](size_type pos) const {
    return sourcebegin_[pos];
  }

  const_reference at(size_type pos) const {
    if (pos >= size()) {
      throw std::out_of_range(
          "vec_view::at: __n (which is " + std::to_string(pos) +
          ") >= this->size() (which is " + std::to_string(size()) + ")");
    }
    return sourcebegin_[pos];
  }

  const_reference front() const {
    return sourcebegin_[0];
  }

  const_reference back() const {
    return sourceend_[-1];
  }

  constexpr const T* data() const {
    return sourcebegin_;
  }

  /**
   * Implicit conversion to vector (for assignment)
   */
  template <typename... Args>
  operator std::vector<T, Args...>() const {
    return std::vector<T, Args...>(begin(), end());
  }

 private:
  const T* sourcebegin_;
  const T* sourceend_;
};

}  // namespace tiledb::stdx

#endif
