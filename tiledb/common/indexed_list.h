/**
 * @file   indexed_list.h
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
 * This file defines and implements the `IndexedList` class.
 */

#ifndef TILEDB_INDEXED_LIST_H
#define TILEDB_INDEXED_LIST_H

#include "memory_tracker.h"
#include "pmr.h"

#include <list>
#include <vector>

namespace tiledb::common {

namespace detail {
template <typename T>
class WhiteboxIndexedList;
}

/**
 * Container class for data that cannot be moved but that we want to access by
 * an index.
 *
 * @tparam T The type of the element used in the container
 */
template <class T>
class IndexedList {
  using Self = IndexedList<T>;
  using Elements = tdb::pmr::list<T>;
  using Indexes = std::vector<T*>;

  friend class detail::WhiteboxIndexedList<T>;

 public:
  /**
   * Iterator handle to a mutable element of an `IndexedList`.
   */
  class iterator {
    Elements::iterator elt_;
    Indexes::iterator idx_;

    friend class IndexedList<T>;

    iterator(Elements::iterator elt, Indexes::iterator idx)
        : elt_(elt)
        , idx_(idx) {
    }

   public:
    using difference_type = int64_t;
    using value_type = T;

    T& operator*() const {
      return *elt_;
    }

    T* operator->() const {
      return &*elt_;
    }

    iterator& operator++() {
      ++elt_;
      ++idx_;
      return *this;
    }

    iterator& operator++(int) {
      elt_++;
      idx_++;
      return *this;
    }

    iterator& operator--() {
      --elt_;
      --idx_;
      return *this;
    }

    iterator& operator--(int) {
      elt_--;
      idx_--;
      return *this;
    }

    bool operator==(const iterator& other) const {
      return elt_ == other.elt_;
    }

    bool operator!=(const iterator& other) const {
      return !(*this == other);
    }
  };

  /**
   * Iterator handle to an immutable element of an `IndexedList`.
   */
  class const_iterator {
    Elements::const_iterator elt_;
    Indexes::const_iterator idx_;

    friend class IndexedList<T>;

    const_iterator(Elements::const_iterator elt, Indexes::const_iterator idx)
        : elt_(elt)
        , idx_(idx) {
    }

   public:
    using difference_type = int64_t;
    using value_type = T;

    const_iterator(IndexedList<T>::iterator ii)
        : elt_(ii.elt_)
        , idx_(ii.idx_) {
    }

    const T& operator*() const {
      return *elt_;
    }

    const T* operator->() const {
      return &*elt_;
    }

    const_iterator& operator++() {
      ++elt_;
      ++idx_;
      return *this;
    }

    const_iterator& operator++(int) {
      elt_++;
      idx_++;
      return *this;
    }

    const_iterator& operator--() {
      --elt_;
      --idx_;
      return *this;
    }

    const_iterator& operator--(int) {
      elt_--;
      idx_--;
      return *this;
    }

    bool operator==(const const_iterator& other) const {
      return elt_ == other.elt_;
    }

    bool operator!=(const const_iterator& other) const {
      return !(*this == other);
    }
  };

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Deleted Default constructor. */
  IndexedList() = delete;

  /**
   * Constructor.
   *
   * @param memory_tracker The memory tracker for the underlying containers.
   */
  explicit IndexedList(
      shared_ptr<sm::MemoryTracker> memory_tracker, sm::MemoryType mem_type)
      : memory_tracker_(memory_tracker)
      , list_(memory_tracker->get_resource(mem_type)) {
  }

  DISABLE_COPY_AND_COPY_ASSIGN(IndexedList);
  DISABLE_MOVE_AND_MOVE_ASSIGN(IndexedList);

  /* ********************************* */
  /*                 API               */
  /* ********************************* */
  shared_ptr<sm::MemoryTracker> memory_tracker() const {
    return memory_tracker_;
  }

  /**
   * Emplace an item to the end of the container.
   *
   * @param args Arguments forwarded to the initialization function.
   *
   */
  template <class... Args>
  T& emplace_back(Args&&... args) {
    auto& v = list_.emplace_back(std::forward<Args>(args)...);
    vec_.emplace_back(&v);
    return v;
  }

  /** Returns the allocator. */
  tdb::pmr::polymorphic_allocator<T> get_allocator() const {
    return list_.get_allocator();
  }

  iterator begin() {
    return iterator(list_.begin(), vec_.begin());
  }

  const_iterator begin() const {
    return const_iterator(list_.begin(), vec_.begin());
  }

  iterator end() {
    return iterator(list_.end(), vec_.end());
  }

  const_iterator end() const {
    return const_iterator(list_.end(), vec_.end());
  }

  /** Returns wether the container is empty or not. */
  bool empty() const {
    return list_.empty();
  }

  /** Clears the container. */
  void clear() {
    list_.clear();
    vec_.clear();
  }

  /** Returns the number of items in the container. */
  size_t size() const {
    return list_.size();
  }

  /**
   * Reserve space for a number of items.
   *
   * @param num Number of items to reserve for.
   */
  void reserve(size_t num) {
    vec_.reserve(num);
  }

  /**
   * Resize the container with default constructed items.
   *
   * Note: Only allowed on an empty container.
   *
   * @param num Number of items to add.
   */
  template <std::copyable... Args>
  void resize(size_t num, Args... args) {
    if (list_.size() != 0 || vec_.size() != 0) {
      throw std::logic_error(
          "Resize should only be called on empty container.");
    }

    vec_.reserve(num);
    for (uint64_t n = 0; n < num; n++) {
      std::tuple<std::decay_t<Args>...> copied_args(args...);
      std::apply(
          [this](auto&&... copied_arg) {
            emplace_back<Args...>(std::move(copied_arg)...);
          },
          copied_args);
    }
  }

  /**
   * Returns a reference to the item at an index.
   *
   * Note: This API will not throw if an item out of bounds is asked for.
   *
   * @param index Index of the item to return.
   * @return The item.
   */
  T& operator[](size_t index) {
    return *vec_[index];
  }

  /**
   * Returns a reference to the item at an index.
   *
   * Note: This API will throw if an item out of bounds is asked for.
   *
   * @param index Index of the item to return.
   * @return The item.
   */
  T& at(size_t index) {
    return *(vec_.at(index));
  }

  /**
   * Returns a const reference to the item at an index.
   *
   * Note: This API will not throw if an item out of bounds is asked for.
   *
   * @param index Index of the item to return.
   * @return The item.
   */
  const T& operator[](size_t index) const {
    return *vec_[index];
  }

  /**
   * Returns a const reference to the item at an index.
   *
   * Note: This API will throw if an item out of bounds is asked for.
   *
   * @param index Index of the item to return.
   * @return The item.
   */
  const T& at(size_t index) const {
    return *(vec_.at(index));
  }

  /**
   * @return a pointer to the last element of the list
   */
  T* back() {
    if (empty()) {
      return nullptr;
    } else {
      return vec_.back();
    }
  }

  /**
   * @return a pointer to the last element of the list
   */
  const T* back() const {
    if (empty()) {
      return nullptr;
    } else {
      return vec_.back();
    }
  }

  /**
   * Transfers elements from `other` to `*this`. The elements are inserted at
   * `pos`.
   */
  void splice(
      const_iterator pos,
      Self& other,
      const_iterator first,
      const_iterator last) {
    list_.splice(pos.elt_, other.list_, first.elt_, last.elt_);
    vec_.insert(pos.idx_, first.idx_, last.idx_);
    other.vec_.erase(first.idx_, last.idx_);
  }

 private:
  /** The memory tracker for the underlying list. */
  shared_ptr<sm::MemoryTracker> memory_tracker_;

  /** List that contains all the elements. */
  Elements list_;

  /** Vector that contains a pointer to the elements allowing indexed access. */
  Indexes vec_;
};

}  // namespace tiledb::common

#endif  // TILEDB_INDEXED_LIST_H
