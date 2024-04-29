/**
 * @file   var_length_view.h
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
 * This file contains the definition of the var_length_view class, which splits
 * a given view into subranges of variable length, as delimited by adjacent
 * pairs of values in an index range.
 *
 * Usage example:
 * ```
 * std::vector<int> x {1, 2, 3, 4, 5, 6, 7, 8, 9}; // Data range
 * std::vector<size_t> indices {0, 4, 7, 9};       // Index range
 * var_length_view v(x, indices);
 * CHECK(std::ranges::equal(v[0], std::vector<int>{1, 2, 3, 4}));
 * CHECK(std::ranges::equal(v[1], std::vector<int>{5, 6, 7}));
 * CHECK(std::ranges::equal(v[2], std::vector<int>{8, 9}));
 * ```
 */

#ifndef TILEDB_VAR_LENGTH_VIEW_H
#define TILEDB_VAR_LENGTH_VIEW_H

#include <ranges>

#include "iterator_facade.h"

/**
 * A view that splits a view into subranges of variable length, as delimited by
 * a range of indices. The resulting view is a range of subranges, each of which
 * is a view into the original data range. The iterator over the var_length_view
 * is a random_access_iterator that dereferences to a subrange of the data
 * range.
 *
 * @tparam R Type of the data range, assumed to be a random access range.
 * @tparam I Type of the index range, assumed to be a random access range.
 *
 * @todo R could be a view rather than a range.
 */
template <
    std::ranges::random_access_range R,
    std::ranges::random_access_range I>
class var_length_view : public std::ranges::view_base {
  // Forward reference of the iterator over the range of variable length data
  template <class Value>
  struct private_iterator;

  // The type of the iterator over the var length data range, and the type that
  // can index into it
  using data_iterator_type = std::ranges::iterator_t<R>;
  using data_index_type = std::iter_difference_t<data_iterator_type>;

  // The type of the iterator over the index range -- It should derference to
  // something that can index into the data range (e.g., the data_index_type)
  using index_iterator_type = std::ranges::iterator_t<const I>;

  // They type dereferenced by the iterator is a subrange
  using var_length_type = std::ranges::subrange<data_iterator_type>;
  using var_length_iterator = private_iterator<var_length_type>;
  using var_length_const_iterator = private_iterator<var_length_type const>;

  std::ranges::iterator_t<R> data_begin_;
  std::ranges::iterator_t<R> data_end_;

  // const_iterator is c++23.  For now we just use an iterator to const
  std::ranges::iterator_t<const I> index_begin_;
  std::ranges::iterator_t<const I> index_end_;

 public:
  var_length_view(R& data, const I& index)
      : data_begin_(std::ranges::begin(data))
      , data_end_(std::ranges::end(data))
      , index_begin_(std::ranges::cbegin(index))
      , index_end_(std::ranges::cend(index) - 1) {
  }

  auto begin() {
    return var_length_iterator(data_begin_, index_begin_, 0);
  }

  auto end() {
    return var_length_iterator(
        data_begin_, index_begin_, index_end_ - index_begin_);
  }

  auto begin() const {
    return var_length_const_iterator(data_begin_, index_begin_, 0);
  }

  auto end() const {
    return var_length_const_iterator(
        data_begin_, index_begin_, index_end_ - index_begin_);
  }

  auto cbegin() const {
    return var_length_const_iterator(data_begin_, index_begin_, 0);
  }

  auto cend() const {
    return var_length_const_iterator(
        data_begin_, index_begin_, index_end_ - index_begin_);
  }

  auto size() const {
    return index_end_ - index_begin_;
  }

 private:
  template <class Value>
  struct private_iterator : public iterator_facade<private_iterator<Value>> {
    using value_type_ = Value;

    data_index_type index_;
    data_iterator_type data_begin_;
    index_iterator_type offsets_begin_;

    private_iterator() = default;
    private_iterator(
        data_iterator_type data_begin,
        index_iterator_type offsets_begin,
        data_index_type index = 0)
        : index_(index)
        , data_begin_(data_begin)
        , offsets_begin_(offsets_begin) {
      // ...
    }

    /*************************************************************************
     * Functions needed for iterator_facade
     * Here we just supply the minimum needed to make the iterator work
     *************************************************************************/

    /*
     * The critical function for defining the iterator as it determines the
     * value type, etc and also does the dereference.  In the case of the
     * var_length_view, the value type is a subrange of the data range, and
     * the subrage return value must be a proxy object, which we return by
     * value.  This is fine, since we can't assign a subrange to another.
     * We do, however, want to be able to modify the contents of the subrange.
     */
    value_type_ dereference() const {
      return {
          data_begin_ + offsets_begin_[index_],
          data_begin_ + offsets_begin_[index_ + 1]};
    }

    auto advance(data_index_type n) {
      index_ += n;
      return *this;
    }

    auto distance_to(const private_iterator& other) const {
      return other.index_ - index_;
    }

    bool operator==(const private_iterator& other) const {
      return data_begin_ == other.data_begin_ &&
             offsets_begin_ == other.offsets_begin_ && index_ == other.index_;
    }

    static const bool single_pass_iterator = false;
  };

  // using iterator = private_iterator;
};

#endif  // TILEDB_VAR_LENGTH_VIEW_H
