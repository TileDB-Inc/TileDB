/**
 * @file   concurrent_map.h
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
 */

#ifndef TILEDB_CONCURRENT_MAP_H
#define TILEDB_CONCURRENT_MAP_H

#include <map>
#include <mutex>

#include "experimental/tiledb/common/dag/utility/print_types.h"

namespace tiledb::common {

/**
 * Rudimentary implementation of a concurrent mapclass.  This currently only has
 * implementations of member functions necessarty for the TileDB task graph
 * library.
 *
 * @todo Add complete set of member functions.
 */

#include <map>
#include <mutex>

template <
    class Key,
    class T,
    class Compare = std::less<Key>,
    class Allocator = std::allocator<std::pair<const Key, T>>>
class ConcurrentMap : public std::map<Key, T, Compare, Allocator> {
  using Base = std::map<Key, T, Compare, Allocator>;
  using Base::Base;

  std::mutex mutex_;

 public:
  using key_type = typename Base::key_type;
  using mapped_type = typename Base::mapped_type;
  using value_type = typename Base::value_type;
  using size_type = typename Base::size_type;
  using difference_type = typename Base::difference_type;
  using key_compare = typename Base::key_compare;
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

  T& operator[](const Key& key) {
    std::lock_guard _(mutex_);
    return Base::operator[](key);
  }

  T& operator[](Key&& key) {
    std::lock_guard _(mutex_);
    return Base::operator[](std::forward<Key>(key));
  }
};
}  // namespace tiledb::common
#endif  // TILEDB_CONCURRENT_MAP_H
