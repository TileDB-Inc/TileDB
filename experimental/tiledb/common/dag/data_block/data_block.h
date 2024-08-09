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
 *
 * A `DataBlock` is a managed fixed-size container of `std::byte`.  The actual
 * data is a fixed-size "chunk" of bytes, which are managed by a shared pointer.
 * As a result (and by design), copy and assignment semantics are shallow.
 *
 * @todo For hygiene, might want to clean up a moved-from `DataBlock`.
 */

#ifndef TILEDB_DAG_DATA_BLOCK_H
#define TILEDB_DAG_DATA_BLOCK_H

#include <cstddef>
#include <memory>
#include <span>
#include <vector>
#include "pool_allocator.h"

namespace tiledb::common {

/**
 * The fixed size (in bytes) of each `DataBlock`.
 */

/**
 * A fixed size block, an untyped carrier (of `std::byte`) for data to
 * be interpreted by its users.
 *
 * The actual storage associated with the `DataBlock` is a fixed-size "chunk",
 * allocated by an allocator specified by the template parameter `Allocator`.
 * The `DataBlock` can use `std::allocator<std::byte>` as well as the fixed-size
 * `PoolAllocator`.
 *
 * The `DataBlock` itself implements a standard library interface for random
 * access container.  It can also present a `span` for its dat chunk.
 *
 * The interface to the `DataBlock` has some similarities to `std::vector` in
 * that it has a `size` indicating how much of the `DataBlock` contains valid
 * data as well as a `capacity` indicating maximum possible size (i.e., the size
 * of the underlying chunk).  A `DataBlock` can be resized up its capacity.
 * Unlike `std::vector` a `DataBlock` will not reallocate space if a resize
 * request is larger than the capacity.
 *
 * The `DataBlockImpl` class provides the interface for `DataBlock`, but is also
 * parameterized by`Allocator`.
 */
template <size_t chunk_size = 64 * 1'024>
class DataBlockImpl {
  constexpr static const size_t chunk_size_ = chunk_size;

  /**
   * Type aliases for `DataBlock` storage.  The underlying storage for the
   * `DataBlock` is a chunk of `std::byte`.  The `DataBlock` maintains this
   * storage as a `shared_ptr<std::byte>`.
   */
  using allocator_t = PoolAllocator<chunk_size_>;
  using storage_t = std::shared_ptr<std::byte>;
  using span_t = std::span<std::byte>;
  using pointer_t = std::byte*;

  /**
   * Internal accounting to maintain the current size (the extent of valid data)
   * as well as the total capacity of the underlying data chunk.  Both size and
   * capacity are in units of bytes.
   */
  size_t capacity_{0};
  size_t size_{0};

  /**
   * The actual stored chunk.  The `storage_` is the `shared_ptr` to the chunk,
   * while `data_` is a pointer to the actual bytes.
   */

  storage_t storage_;
  pointer_t data_{nullptr};

 public:
  // @todo: Do some initialization here.
  // @todo: Default initialize?
  // Although there is a "size" argument, we don't actually do anything with it.
  // We always return a single datablock of size chunk_size_.
  void* operator new(size_t) {
    return reinterpret_cast<void*>(allocator_t{}.allocate());
  }

  void operator delete(void* ptr) {
    allocator_t{}.deallocate(reinterpret_cast<std::byte*>(ptr));
  }

  /**
   * Constructor used if the `Allocator` is the fixed-size pool allocator.  Note
   * that we pass the deleter associated with the `PoolAllocator` to to the
   * `shared_ptr` constructor.  It is expected that his deleter will simply
   * return the chunk to the pool.  See pool_allocator.h for details.
   */
  DataBlockImpl(size_t init_size = 0UL)
              : capacity_{chunk_size_}
      , size_{init_size}
      , storage_{allocator_t{}.allocate(),
                 [&](auto px) { allocator_t{}.deallocate(px); }}
      , data_{storage_.get()} {
  }

  /**
   * Copy constructors and assignment operators.
   * NOTE: Copies are shallow.
   */
  DataBlockImpl(const DataBlockImpl&) = default;
  DataBlockImpl(DataBlockImpl&&) noexcept = default;
  //  DataBlockImpl(DataBlockImpl&& rhs) {
  //    if (storage_.use_count() == 0) {
  //      rhs.size_ = 0;
  //      rhs.capacity_ = 0;
  //      rhs.data_ = nullptr;
  //    }
  //  }
  DataBlockImpl& operator=(const DataBlockImpl&) = default;
  DataBlockImpl& operator=(DataBlockImpl&&) noexcept = default;

  /**
   * Various type aliases expected for a random-access range.
   */
  using DataBlockIterator = span_t::iterator;
  using DataBlockConstIterator = span_t::iterator;

  using value_type = span_t::value_type;
  using allocator_type = allocator_t;
  using size_type = std::size_t;
  using difference_type = std::ptrdiff_t;
  using reference = value_type&;
  using const_reference = const value_type&;
  using pointer = span_t::pointer;
  using const_pointer = span_t::const_pointer;
  using iterator = DataBlockIterator;
  using iterator_category = std::random_access_iterator_tag;
  using const_iterator = DataBlockConstIterator;
  using reverse_iterator = std::reverse_iterator<iterator>;
  using const_reverse_iterator = std::reverse_iterator<const_iterator>;

  /*
   * Various functions expected for a random-access range.
   */
  reference operator[](size_type idx) {
    return data_[idx];
  }
  const_reference operator[](size_type idx) const {
    return data_[idx];
  }

  pointer data() {
    return data_;
  }
  [[nodiscard]] const_pointer data() const {
    return data_;
  }

  [[nodiscard]] span_t entire_span() const {
    return {data_, capacity_};
  }

  [[nodiscard]] span_t span() const {
    return {data_, size_};
  }

  iterator begin() {
    return data_;
  }
  [[nodiscard]] const_iterator begin() const {
    return data_;
  }
  [[nodiscard]] const_iterator cbegin() const {
    return data_;
  }
  reverse_iterator rbegin() {
    return span_t(data_, size_).rbegin();
  }
  [[nodiscard]] const_reverse_iterator rbegin() const {
    return span_t(data_, size_).rbegin();
  }
  [[nodiscard]] const_reverse_iterator crbegin() const {
    return span_t(data_, size_).rbegin();
  }

  iterator end() {
    return data_ + size_;
  }
  [[nodiscard]] const_iterator end() const {
    return data_ + size_;
  }
  [[nodiscard]] const_iterator cend() const {
    return data_ + size_;
  }
  reverse_iterator rend() {
    return span_t(data_, size_).rend();
  }
  [[nodiscard]] const_reverse_iterator rend() const {
    return span_t(data_, size_).rend();
  }
  [[nodiscard]] const_reverse_iterator crend() const {
    return span_t(data_, size_).rend();
  }

  reference back() {
    return data_[size_ - 1];
  }

  [[nodiscard]] const_reference back() const {
    return data_[size_ - 1];
  }

  bool resize(size_t count) {
    if (count > capacity_) {
      return false;
    }
    size_ = count;
    return true;
  }

  [[nodiscard]] bool empty() const {
    return size_ == 0;
  }

  [[nodiscard]] size_t size() const {
    return size_;
  }
  [[nodiscard]] size_t capacity() const {
    return capacity_;
  }

  /**
   * Class variable to access max possible size of the `DataBlock`
   */
  constexpr static inline size_t max_size() {
    return chunk_size_;
  }

  /**
   * Get shared pointer use count -- needed for diagnostics / testing
   */
  [[nodiscard]] size_t use_count() const noexcept {
    return storage_.use_count();
  }
};

/**
 * The `DataBlock` class is the `DataBlockImpl` above, specialized to the
 * PoolAllocator with chunk size specified by our defined `chunk_size_`.
 */
template <size_t block_size = 64 * 1'024>
class DataBlock : public DataBlockImpl<block_size> {
  using Base = DataBlockImpl<block_size>;
  using Base::Base;
};

}  // namespace tiledb::common

#endif  // TILEDB_DAG_DATA_BLOCK_H
