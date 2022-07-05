/**
 * @file   data_block.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * This file declares the DataBlock class for dag.
 */

#ifndef TILEDB_DAG_DATA_BLOCK_H
#define TILEDB_DAG_DATA_BLOCK_H

#include <cstddef>
#include <vector>
#include "external/include/span/span.hpp"
#include "pool_allocator.h"

namespace tiledb::common {

namespace {
constexpr static const size_t chunk_size_ = 4'194'304;  // 4M
}

/**
 * A fixed size block, an untyped carrier for data to be interpreted by its
 * users.
 *
 * Intended to be allocated from a pool with a bitmap allocation strategy.
 *
 * Implemented internally as a span.
 *
 * Implements standard library interface for random access container.
 */
template <class Allocator>
class DataBlockImpl {
  static Allocator allocator_;

  using storage_t = std::shared_ptr<std::byte>;
  using span_t = tcb::span<std::byte>;
  using pointer_t = std::byte*;

  size_t capacity_;
  size_t size_;
  storage_t storage_;
  pointer_t data_;

 public:
  template <class R = Allocator>
  DataBlockImpl(
      size_t init_size = 0UL,
      typename std::enable_if<
          std::is_same_v<R, std::allocator<std::byte>>>::type* = 0)
      : capacity_{chunk_size_}
      , size_{init_size}
      , storage_{allocator_.allocate(chunk_size_),
                 [&](auto px) { allocator_.deallocate(px, chunk_size_); }}
      , data_{storage_.get()} {
  }

  template <class R = Allocator>
  DataBlockImpl(
      size_t init_size = 0UL,
      typename std::enable_if<
          std::is_same_v<R, PoolAllocator<chunk_size_>>>::type* = 0)
      : capacity_{chunk_size_}
      , size_{init_size}
      , storage_{allocator_.allocate(),
                 [&](auto px) { allocator_.deallocate(px); }}
      , data_{storage_.get()} {
  }

  using DataBlockIterator = span_t::iterator;
  using DataBlockConstIterator = span_t::iterator;

  using value_type = span_t::value_type;
  using allocator_type = SingletonPoolAllocator<chunk_size_>;
  using size_type = std::size_t;
  using difference_type = std::ptrdiff_t;
  using reference = value_type&;
  using const_reference = const value_type&;
  using pointer = span_t::pointer;
  using const_pointer = span_t::const_pointer;
  using iterator = DataBlockIterator;
  using const_iterator = DataBlockConstIterator;
  using reverse_iterator = std::reverse_iterator<iterator>;
  using const_reverse_iterator = std::reverse_iterator<const_iterator>;

  reference operator[](size_type idx) {
    return data_[idx];
  }
  const_reference operator[](size_type idx) const {
    return data_[idx];
  }

  pointer data() {
    return data_;
  }
  const_pointer data() const {
    return data_;
  }

  span_t entire_span() const {
    return {data_, capacity_};
  }

  span_t span() const {
    return {data_, size_};
  }

  iterator begin() {
    return data_;
  }
  const_iterator begin() const {
    return data_;
  }
  const_iterator cbegin() const {
    return data_;
  }
  reverse_iterator rbegin() {
    return span_t(data_, size_).rbegin();
  }
  const_reverse_iterator rbegin() const {
    return data_t(data_, size_).rbegin();
  }
  const_reverse_iterator crbegin() const {
    return data_t(data_, size_).rbegin();
  }

  iterator end() {
    return data_ + size_;
  }
  const_iterator end() const {
    return data_ + size_;
  }
  const_iterator cend() const {
    return data_ + size_;
  }
  reverse_iterator rend() {
    return data_t(data_, size_).rend();
  }
  const_reverse_iterator rend() const {
    return data_t(data_, size_).rend();
  }
  const_reverse_iterator crend() const {
    return data_t(data_, size_).rend();
  }

  bool resize(size_t count) {
    if (count > capacity_) {
      return false;
    }
    size_ = count;
    return true;
  }

  bool empty() const {
    return size_ == 0;
  }

  size_t size() const {
    return size_;
  }
  size_t capacity() const {
    return capacity_;
  }
};

template <class Allocator>
Allocator DataBlockImpl<Allocator>::allocator_;

using DataBlock = DataBlockImpl<PoolAllocator<chunk_size_>>;

}  // namespace tiledb::common

#endif  // TILEDB_DAG_DATA_BLOCK_H
