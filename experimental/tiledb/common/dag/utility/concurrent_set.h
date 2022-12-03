/**
 * @file   concurrent_set.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021-2022 TileDB, Inc.
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
 * This file declares a classic/basic generic bounded-buffer producer-consumer
 * queue. The class supports a purely unbounded option.
 */

#ifndef TILEDB_CONCURRENT_SET_H
#define TILEDB_CONCURRENT_SET_H

#include <mutex>
#include <set>

#include "experimental/tiledb/common/dag/utility/print_types.h"

namespace tiledb::common {

/**
 * Minimal implementation of a concurrent set class.  This currently only has
 * implementations of member functions required for the tiledb task graph
 * library.
 *
 * @todo Add complete set of member functions.
 */
template <
    class Key,
    class Compare = std::less<Key>,
    class Allocator = std::allocator<Key>>
class ConcurrentSet : public std::set<Key, Compare, Allocator> {
  using Base = std::set<Key, Compare, Allocator>;

  using Base::Base;

  mutable std::mutex mutex_;

 public:
  using key_type = typename Base::key_type;
  using value_type = typename Base::value_type;
  using size_type = typename Base::size_type;
  using difference_type = typename Base::difference_type;
  using key_compare = typename Base::key_compare;
  using value_compare = typename Base::value_compare;
  using allocator_type = typename Base::allocator_type;
  using reference = typename Base::reference;
  using const_reference = typename Base::const_reference;
  using pointer = typename Base::pointer;
  using const_pointer = typename Base::const_pointer;
  using iterator = typename Base::iterator;
  using const_iterator = typename Base::const_iterator;
  using reverse_iterator = typename Base::reverse_iterator;
  using const_reverse_iterator = typename Base::const_reverse_iterator;
  using node_type = typename Base::node_type;
  using insert_return_type = typename Base::insert_return_type;

  bool empty() const {
    std::scoped_lock lock(mutex_);
    return Base::empty();
  }

  size_type size() const {
    std::lock_guard lock(mutex_);
    return Base::size();
  }

  void clear() {
    std::lock_guard lock(mutex_);
    return Base::clear();
  }

  std::pair<iterator, bool> insert(const value_type& value) {
    std::lock_guard lock(mutex_);
    return Base::insert(value);
  }

  std::pair<iterator, bool> insert(value_type&& value) {
    std::lock_guard lock(mutex_);
    return Base::insert(std::forward<value_type>(value));
  }

  insert_return_type insert(node_type&& nh) {
    std::lock_guard lock(mutex_);
    return Base::insert(std::forward<node_type>(nh));
  }

  template <class... Args>
  std::pair<iterator, bool> emplace(Args&&... args) {
    std::lock_guard lock(mutex_);
    return Base::emplace(std::forward<Args>(args)...);
  }

  iterator erase(iterator pos) {
    std::lock_guard lock(mutex_);
    return Base::erase(pos);
  }

  template <class U = void>
  iterator erase(
      const_iterator pos,
      std::enable_if<std::is_same_v<iterator, const_iterator>, void*> =
          nullptr) {
    std::lock_guard lock(mutex_);
    return Base::erase(pos);
  }

  size_type erase(const Key& key) {
    std::lock_guard lock(mutex_);
    return Base::erase(key);
  }

  void swap(ConcurrentSet& other) {
    std::lock_guard lock(mutex_);
    Base::swap(other);
  }

  node_type extract(const Key& k) {
    std::lock_guard lock(mutex_);
    return Base::extract(k);
  }

  iterator find(const Key& key) {
    std::lock_guard lock(mutex_);
    return Base::find(key);
  }

  const_iterator find(const Key& key) const {
    std::lock_guard lock(mutex_);
    return Base::find(key);
  }

  template <class K>
  iterator find(const K& x) {
    std::lock_guard lock(mutex_);
    return Base::find(x);
  }

  template <class K>
  const_iterator find(const K& x) const {
    std::lock_guard lock(mutex_);
    return Base::find(x);
  }
};

}  // namespace tiledb::common
#endif  // TILEDB_CONCURRENT_SET_H
