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

#include "pmr.h"

#include <list>
#include <vector>

namespace tiledb::sm {
class MemoryTracker;
}

namespace tiledb::common {

/**
 * Container class for data that cannot be moved but that we want to access by
 * an index.
 *
 * @tparam T The type of the element used in the container
 */
template <class T>
class IndexedList {
 public:
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
  explicit IndexedList(shared_ptr<sm::MemoryTracker> memory_tracker);

  DISABLE_COPY_AND_COPY_ASSIGN(IndexedList);
  DISABLE_MOVE_AND_MOVE_ASSIGN(IndexedList);

  /* ********************************* */
  /*                 API               */
  /* ********************************* */

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

  /** Returns an iterator to the beginning of the items. */
  typename std::list<T>::iterator begin() {
    return list_.begin();
  }

  /** Returns an iterator to the beginning of the items. */
  typename std::list<T>::const_iterator begin() const {
    return list_.begin();
  }

  /** Returns an iterator to the end of the items. */
  typename std::list<T>::iterator end() {
    return list_.end();
  }

  /** Returns an iterator to the end of the items. */
  typename std::list<T>::const_iterator end() const {
    return list_.end();
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
  void resize(size_t num) {
    if (list_.size() != 0 || vec_.size() != 0) {
      throw std::logic_error(
          "Resize should only be called on empty container.");
    }

    vec_.reserve(num);
    for (uint64_t n = 0; n < num; n++) {
      emplace_back(memory_tracker_);
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

 private:
  /** The memory tracker for the underlying list. */
  shared_ptr<sm::MemoryTracker> memory_tracker_;

  /** List that contains all the elements. */
  tdb::pmr::list<T> list_;

  /** Vector that contains a pointer to the elements allowing indexed access. */
  std::vector<T*> vec_;
};

}  // namespace tiledb::common

#endif  // TILEDB_INDEXED_LIST_H
