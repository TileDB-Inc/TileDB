/**
 * @file vector_sso.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB, Inc.
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
 * This file provides a template for `vector_sso` which provides the same
 * interface as `std::vector` but uses "short string optimization" to
 * avoid allocating memory when the number of elements is below a certain
 * (configurable) threshold.
 *
 * A common use case for this is something like handling Dimensions, where
 * there is a need to use `std::vector` to store a datum per Dimension,
 * but the number of Dimensions is most commonly one, two, or three.
 */

#ifndef TILEDB_COMMON_STDX_VECTOR_SSO_H
#define TILEDB_COMMON_STDX_VECTOR_SSO_H

#include <cstring>
#include <memory>
#include <optional>
#include <span>
#include <utility>
#include <vector>

namespace tiledb::common {

template <typename T>
concept sso_enabled =
    std::is_trivially_copyable_v<T> || std::is_move_constructible_v<T>;

/**
 * A sequence container that encapsulates dynamic size array.
 *
 * This provides the same interface as `std::vector` but also implements "short
 * string optimization". That is, if the number of elements in the container
 * does not exceed `N`, the values are stored inline inside this object instead
 * of on the heap.
 *
 * Correspondingly the size of this object is at least `N *
 * sizeof(T)`.
 */
template <typename T, uint64_t N, class Allocator = std::allocator<T>>
class vector_sso {
  uint64_t size_;
  union {
    uint64_t capacity_;
    uint64_t capacity_hint_;
  };
  union {
    T* buf_;
    std::array<uint8_t, N * sizeof(T)> sso_;
  };

  Allocator alloc_;

  static void move_all(T* dst, T* src, uint64_t n) {
    if constexpr (std::is_trivially_copyable_v<T>) {
      std::memcpy(dst, src, sizeof(T) * n);
    } else {
      static_assert(std::is_move_constructible_v<T>);
      for (uint64_t i = 0; i < n; i++) {
        new (&dst[i]) T(std::move(src[i]));
      }
    }
  }

  /**
   * Copies or moves contents from the internal buffer to the heap.
   */
  void transfer() {
    const uint64_t capacity = capacity_hint_;
    T* buf = alloc_.allocate(capacity);

    move_all(buf, reinterpret_cast<T*>(sso_.data()), size_);

    capacity_ = capacity;
    buf_ = buf;
  }

  void grow() {
    const uint64_t new_capacity = capacity_ * 2;
    T* new_buf = alloc_.allocate(new_capacity);

    move_all(new_buf, buf_, size_);

    alloc_.deallocate(buf_, capacity_);

    capacity_ = new_capacity;
    buf_ = new_buf;
  }

  /**
   * Allocates space for one additional element and returns a pointer to the new
   * space where that element should be constructed.
   */
  T* next() {
    if (size_ < N) {
      return &data()[size_++];
    } else if (size_ == N) {
      transfer();
      return &buf_[size_++];
    } else if (size_ < capacity_) {
      return &buf_[size_++];
    } else {
      grow();
      return &buf_[size_++];
    }
  }

 public:
  using self_type = vector_sso<T, N, Allocator>;
  using value_type = T;
  using allocator_type = Allocator;
  using size_type = uint64_t;
  using difference_type = std::ptrdiff_t;
  using reference = T&;
  using const_reference = const T&;
  using iterator = T*;
  using const_iterator = const T*;

  vector_sso()
      : size_(0)
      , capacity_hint_(2 * N)
      , alloc_(Allocator()) {
  }

  vector_sso(const Allocator& alloc)
      : size_(0)
      , capacity_hint_(2 * N)
      , alloc_(alloc) {
  }

  vector_sso(size_type count, const Allocator& alloc = Allocator())
      : vector_sso(alloc) {
    reserve(count);
    for (uint64_t i = 0; i < count; i++) {
      emplace_back();
    }
  }

  template <std::input_iterator InputIt>
  vector_sso(InputIt first, InputIt last, const Allocator& alloc = Allocator())
      : vector_sso(alloc) {
    assign(first, last);
  }

  vector_sso(
      std::initializer_list<T> init, const Allocator& alloc = Allocator())
      : vector_sso(alloc) {
    assign(init.begin(), init.end());
  }

  vector_sso(const self_type& copyfrom)
      : vector_sso(std::allocator_traits<Allocator>::
                       select_on_container_copy_construction(
                           copyfrom.get_allocator())) {
    assign(copyfrom.begin(), copyfrom.end());
  }

  vector_sso(self_type&& movefrom)
      : vector_sso(std::move(movefrom.get_allocator())) {
    *this = std::move(movefrom);
  }

  ~vector_sso() {
    clear();
  }

  /**
   * @return the number of elements currently in the container
   */
  size_type size() const {
    return size_;
  }

  /**
   * @return the number of elements that can be held in currently allocated
   * storage
   */
  size_type capacity() const {
    if (size_ <= N) {
      return N;
    } else {
      return capacity_;
    }
  }

  /**
   * @return the number of elements that can be held in allocated storage after
   * the next reallocation
   */
  size_type next_capacity() const {
    if (size_ <= N) {
      return capacity_hint_;
    } else {
      return capacity_ * 2;
    }
  }

  /**
   * @return true if the internal buffer is used for elements, false if they are
   * on the heap
   */
  bool is_inline() const {
    return size() <= N;
  }

  /**
   * @return true if this container contains zero elements.
   */
  bool empty() const {
    return size() == 0;
  }

  /**
   * @return a pointer to the contiguous elements of this container.
   */
  const T* data() const {
    if (size_ <= N) {
      return reinterpret_cast<const T*>(sso_.data());
    } else {
      return buf_;
    }
  }

  /**
   * @return a pointer to the contiguous elements of this container.
   */
  T* data() {
    if (size_ <= N) {
      return reinterpret_cast<T*>(sso_.data());
    } else {
      return buf_;
    }
  }

  const Allocator& get_allocator() const {
    return alloc_;
  }

  /**
   * @return a reference to the last element of this container. If the
   * container is empty then the behavior is undefined.
   */
  reference back() {
    return data()[size() - 1];
  }

  /**
   * @return a reference to the last element of this container. If the
   * container is empty then the behavior is undefined.
   */
  const_reference back() const {
    return data()[size() - 1];
  }

  /**
   * @return a reference to the element at specified location `pos`, with
   * bounds checking.
   * @throws `std::out_of_range` if `pos >= size()`.
   */
  const_reference at(size_type pos) const {
    if (pos < size_) {
      return data()[pos];
    } else {
      std::__throw_out_of_range_fmt(
          "vector_sso::at: __n (which is %zu) >= this->size() (which "
          "is %zu)",
          pos,
          size_);
    }
  }

  /**
   * Increases the capacity of the container to ensure that at least
   * `newcapacity` elements can fit without needing reallocation.
   *
   * If the vector's current size is at most `N`, then this instead ensures
   * that an initial allocation if one is needed will fit at least
   * `newcapacity` elements without needing reallocation.
   */
  void reserve(size_type newcapacity) {
    if (size_ <= N) {
      capacity_hint_ = std::max(capacity_hint_, newcapacity);
    } else {
      T* newbuf = alloc_.allocate(newcapacity);
      move_all(newbuf, buf_, size_);
      alloc_.deallocate(buf_, capacity_);

      capacity_ = newcapacity;
      buf_ = newbuf;
    }
  }

  void resize(size_type newsize) {
    // FIXME: this can be more efficient
    while (size_ < newsize) {
      emplace_back();
    }
    while (newsize < size_) {
      pop_back();
    }
  }

  void shrink_to_fit() {
    if (N < size_ && size_ < capacity_) {
      T* newbuf = alloc_.allocate(size_);
      move_all(newbuf, buf_, size_);
      alloc_.deallocate(buf_, capacity_);

      capacity_ = size_;
      buf_ = newbuf;
    }
  }

  void clear() {
    // destruct elements in reverse order
    for (auto it = rbegin(); it != rend(); ++it) {
      it->~T();
    }

    // then free storage if needed
    if (size_ > N) {
      alloc_.deallocate(buf_, capacity_);
    }

    size_ = 0;
  }

  reference push_back(const T& value) {
    T* ptr = next();
    new (ptr) T(value);
    return *ptr;
  }

  reference push_back(T&& value) {
    T* ptr = next();
    new (ptr) T(std::move(value));
    return *ptr;
  }

  template <typename... Args>
  reference emplace_back(Args&&... args) {
    T* ptr = next();
    new (ptr) T(std::forward<Args>(args)...);
    return *ptr;
  }

  void pop_back() {
    back().~T();
    if (size_ == N + 1) {
      // stash as locals since they will get clobbered
      T* buf = buf_;
      uint64_t capacity = capacity_;

      // move to sso buffer
      move_all(reinterpret_cast<T*>(sso_.data()), buf, N);

      alloc_.deallocate(buf, capacity);

      capacity_hint_ = 2 * N;
    }
    --size_;

    // FIXME: this should probably shrink capacity if it goes below a certain
    // scale factor
  }

  template <std::input_iterator InputIt>
  void assign(InputIt first, InputIt last) {
    clear();

    if constexpr (std::random_access_iterator<InputIt>) {
      reserve(last - first);
    }

    InputIt it = first;
    while (it != last) {
      push_back(*it);
      ++it;
    }
  }

  /**
   * Exchanges the contents and capacity of the container with those of `rhs`.
   *
   * If both `this` and `rhs` have size greater than `N`, then this avoids
   * moving or copying elements of both containers. Otherwise elements must
   * be moved or copied into the inline buffer of either side.
   */
  void swap(self_type& rhs) {
    auto& lhs = *this;
    if (lhs.size() <= N && rhs.size() <= N) {
      std::swap(lhs.capacity_hint_, rhs.capacity_hint_);
      std::swap(lhs.sso_, rhs.sso_);
    } else if (lhs.size() <= N) {
      T* rbuf = rhs.buf_;
      move_all(
          reinterpret_cast<T*>(rhs.sso_.data()),
          reinterpret_cast<T*>(lhs.sso_.data()),
          lhs.size());
      if (lhs.alloc_ == rhs.alloc_) {
        lhs.buf_ = rbuf;
      } else {
        // TODO: we'll do it if we need it
        throw std::logic_error(
            "vector_sso::swap: not implemented with un-equal allocators");
      }
    } else if (rhs.size() <= N) {
      rhs.swap(*this);
      return;
    } else {
      if (lhs.alloc_ == rhs.alloc_) {
        std::swap(lhs.capacity_, rhs.capacity_);
        std::swap(lhs.buf_, rhs.buf_);
      } else {
        // TODO: we'll do it if we need it
        throw std::logic_error(
            "vector_sso::swap: not implemented with un-equal allocators");
      }
    }
    std::swap(lhs.size_, rhs.size_);
  }

  self_type& operator=(const self_type& copyfrom) {
    alloc_ = copyfrom.alloc_;
    assign(copyfrom.begin(), copyfrom.end());
    return *this;
  }

  self_type& operator=(self_type&& movefrom) {
    clear();
    if (movefrom.is_inline()) {
      move_all(
          reinterpret_cast<T*>(sso_.data()),
          reinterpret_cast<T*>(movefrom.sso_.data()),
          movefrom.size());
      size_ = movefrom.size();
      capacity_hint_ = movefrom.capacity_hint_;
      movefrom.clear();
    } else {
      buf_ = movefrom.buf_;
      size_ = movefrom.size_;
      capacity_ = movefrom.capacity_;

      movefrom.buf_ = nullptr;
      movefrom.size_ = 0;
      movefrom.capacity_hint_ = 2 * N;
    }
    return *this;
  }

  const T& operator[](size_type pos) const {
    return data()[pos];
  }

  T& operator[](size_type pos) {
    return data()[pos];
  }

  iterator begin() {
    return data();
  }

  iterator end() {
    return data() + size();
  }

  const_iterator begin() const {
    return data();
  }

  const_iterator end() const {
    return data() + size();
  }

  std::reverse_iterator<iterator> rbegin() {
    return std::reverse_iterator<iterator>{end()};
  }

  std::reverse_iterator<iterator> rend() {
    return std::reverse_iterator<iterator>{begin()};
  }

  std::reverse_iterator<const_iterator> rbegin() const {
    return std::reverse_iterator<const_iterator>{end()};
  }

  std::reverse_iterator<const_iterator> rend() const {
    return std::reverse_iterator<const_iterator>{begin()};
  }

  /*
  bool operator==(const self_type& other) const {
    return std::equal(begin(), end(), other.begin(), other.end());
  }
  */

  /*
  template <Contiguous<const T> S>
  bool operator==(const S& other) const {
    return std::equal(begin(), end(), other.begin(), other.end())
  }
  */

  // template <typename A2>
  bool operator==(const std::vector<T>& other) const {
    return std::equal(begin(), end(), other.begin(), other.end());
  }

  bool operator==(const std::span<const T>& other) const {
    return std::equal(begin(), end(), other.begin(), other.end());
  }

  bool operator==(const std::initializer_list<T>& other) const {
    return std::equal(begin(), end(), other.begin(), other.end());
  }
};

/*
template <typename T, typename A1, typename A2>
bool operator==(const vector_sso<T, A1>& left, const vector_sso<T, A2>& right)
{ return std::equal(begin(), end(), other.begin(), other.end());
}

template <typename T, typename A1, typename A2>
bool operator==(const vector_sso<T, A1>& left, const vector<T, A2>& right) {
  return std::equal(begin(), end(), other.begin(), other.end());
}

template <typename T, typename A1, typename A2>
bool operator==(const vector<T, A1>& left, const vector_sso<T, A2>& right) {
  return std::equal(begin(), end(), other.begin(), other.end());
}

template <typename T, typename A>
bool operator==(
    const vector_sso<T, A>& left, const std::initializer_list<T>& right) {
  return std::equal(begin(), end(), other.begin(), other.end());
}

template <typename T, typename A>
bool operator==(
    const std::initializer_list<T>& left, const vector_sso<T>& right) {
  return std::equal(begin(), end(), other.begin(), other.end());
}
*/

}  // namespace tiledb::common

namespace std {

template <class T, uint64_t N, class Alloc>
void swap(
    tiledb::common::vector_sso<T, N, Alloc>& lhs,
    tiledb::common::vector_sso<T, N, Alloc>& rhs) {
  lhs.swap(rhs);
}

}  // namespace std

#endif  // TILEDB_COMMON_STDX_VECTOR_SSO_H
