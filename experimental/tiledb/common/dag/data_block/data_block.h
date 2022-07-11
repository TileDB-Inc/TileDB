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

namespace tiledb::common {

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
class DataBlock {
  constexpr static const size_t N_ = 4'194'304;  // 4M

  using storage_t = std::vector<std::byte>;  // For prototyping
  using data_t = tcb::span<std::byte>;
  storage_t storage_;
  data_t data_;

 public:
  DataBlock()
      : storage_(N_)
      , data_(storage_.data(), storage_.size()) {
  }

  using DataBlockIterator = data_t::iterator;
  using DataBlockConstIterator = data_t::iterator;

  using value_type = data_t::value_type;
  // using  allocator_type = ??
  using size_type = std::size_t;
  using difference_type = std::ptrdiff_t;
  using reference = value_type&;
  using const_reference = const value_type&;
  using pointer = data_t::pointer;
  using const_pointer = data_t::const_pointer;
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
    return data_.data();
  }
  const_pointer data() const {
    return data_.data();
  }

  iterator begin() {
    return data_.begin();
  }
  const_iterator begin() const {
    return data_.begin();
  }
  const_iterator cbegin() const {
    return data_.begin();
  }
  reverse_iterator rbegin() {
    return data_.rbegin();
  }
  const_reverse_iterator rbegin() const {
    return data_.rbegin();
  }
  const_reverse_iterator crbegin() const {
    return data_.rbegin();
  }

  iterator end() {
    return data_.end();
  }
  const_iterator end() const {
    return data_.end();
  }
  const_iterator cend() const {
    return data_.end();
  }
  reverse_iterator rend() {
    return data_.rend();
  }
  const_reverse_iterator rend() const {
    return data_.rend();
  }
  const_reverse_iterator crend() const {
    return data_.rend();
  }

  bool empty() const {
    return data_.empty();
  }

  size_t size() const {
    return data_.size();
  }
  size_t capacity() const {
    return storage_.size();
  }
};

}  // namespace tiledb::common

#endif  // TILEDB_DAG_DATA_BLOCK_H
