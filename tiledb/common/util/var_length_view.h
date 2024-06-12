/**
 * @file   tiledb/common/util/var_length_view.h
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

#include "tiledb/common/util/detail/iterator_facade.h"

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
 * @todo: Should we use `view_interface` instead of view_base?
 */
template <
    std::ranges::random_access_range R,
    std::ranges::random_access_range I>
class var_length_view : public std::ranges::view_base {
  // Forward reference of the iterator over the range of variable length data
  template <class Value>
  struct private_iterator;

  /** The type of the iterator over the var length data range */
  using data_iterator_type = std::ranges::iterator_t<R>;

  /** The type that can index into the var length data range */
  using data_index_type = std::ranges::range_difference_t<R>;

  /**
   * The type of the iterator over the index range -- It should dereference to
   * something that can index into the data range (e.g., the data_index_type)
   */
  using index_iterator_type = std::ranges::iterator_t<const I>;

  /** The type dereferenced by the iterator is a subrange */
  using var_length_type = std::ranges::subrange<data_iterator_type>;

  /** The type of the iterator over the var length view */
  using var_length_iterator = private_iterator<var_length_type>;

  /** The type of the const iterator over the var length view */
  using var_length_const_iterator = private_iterator<var_length_type const>;

 public:
  /*****************************************************************************
   * Constructors
   * The full litany of constructors (for each input range):
   * var_l_view(data_begin, data_end, index_begin, index_end);
   * var_l_view(data_begin, data_end, n_data, index_begin, index_end, n_index);
   * var_l_view(data, index);
   * var_l_view(data, n_data, index, n_index);
   ****************************************************************************/

  /** Constructor taking iterator pairs for the data and index ranges */
  var_length_view(
      std::ranges::iterator_t<R> data_begin,
      std::ranges::iterator_t<R> data_end,
      std::ranges::iterator_t<const I> index_begin,
      std::ranges::iterator_t<const I> index_end)
      : data_begin_(data_begin)
      , data_end_(data_end)
      , index_begin_(index_begin)
      , index_end_(index_end)
      , num_subranges_(index_end - index_begin - 1) {
  }

  /**
   * Constructor taking iterator pairs for the data and index ranges, along
   * with sizes
   */
  var_length_view(
      std::ranges::iterator_t<R> data_begin,
      [[maybe_unused]] std::ranges::iterator_t<R> data_end,
      std::ranges::range_difference_t<R> n_data,
      std::ranges::iterator_t<const I> index_begin,
      [[maybe_unused]] std::ranges::iterator_t<const I> index_end,
      std::ranges::range_difference_t<I> n_index)
      : data_begin_(data_begin)
      , data_end_(data_begin + n_data)
      , index_begin_(index_begin)
      , index_end_(index_begin + n_index)
      , num_subranges_(index_end_ - index_begin_ - 1) {
    assert(data_end - data_begin >= n_data);
    assert(index_end - index_begin >= n_index);
  }

  /** Constructor taking ranges for the data and index ranges */
  var_length_view(R& data, const I& index)
      : data_begin_(std::ranges::begin(data))
      , data_end_(std::ranges::end(data))
      , index_begin_(std::ranges::cbegin(index))
      , index_end_(std::ranges::cend(index))
      , num_subranges_(std::ranges::size(index) - 1) {
  }

  /**
   * Constructor taking ranges for the data and index ranges, along with sizes
   */
  var_length_view(
      R& data,
      std::ranges::range_difference_t<R> n_data,
      const I& index,
      std::ranges::range_difference_t<I> n_index)
      : data_begin_(std::ranges::begin(data))
      , data_end_(std::ranges::begin(data) + n_data)
      , index_begin_(std::ranges::cbegin(index))
      , index_end_(std::ranges::cbegin(index) + n_index)
      , num_subranges_(n_index - 1) {
    assert(data_end_ - data_begin_ >= n_data);
    assert(index_end_ - index_begin_ >= n_index);
  }

  /** Return iterator to the beginning of the var length view */
  auto begin() {
    return var_length_iterator(data_begin_, index_begin_, 0);
  }

  /** Return iterator to the end of the var length view */
  auto end() {
    return var_length_iterator(data_begin_, index_begin_, num_subranges_);
  }

  /** Return const iterator to the beginning of the var length view */
  auto begin() const {
    return var_length_const_iterator(data_begin_, index_begin_, 0);
  }

  /** Return const iterator to the end of the var length view */
  auto end() const {
    return var_length_const_iterator(data_begin_, index_begin_, num_subranges_);
  }

  /** Return const iterator to the beginning of the var length view */
  auto cbegin() const {
    return var_length_const_iterator(data_begin_, index_begin_, 0);
  }

  /** Return const iterator to the end of the var length view */
  auto cend() const {
    return var_length_const_iterator(data_begin_, index_begin_, num_subranges_);
  }

  /** Return the number of subranges in the var length view */
  auto size() const {
    return num_subranges_;
  }

 private:
  template <class Value>
  struct private_iterator : public iterator_facade<private_iterator<Value>> {
    using value_type_ = Value;

    /** Default constructor */
    private_iterator() = default;

    /** Primary constructor */
    private_iterator(
        data_iterator_type data_begin,
        index_iterator_type offsets_begin,
        data_index_type index = 0)
        : index_(index)
        , data_begin_(data_begin)
        , offsets_begin_(offsets_begin) {
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

    /** Advance the iterator by n */
    auto advance(data_index_type n) {
      index_ += n;
      return *this;
    }

    /** Return the distance to another iterator */
    auto distance_to(const private_iterator& other) const {
      return other.index_ - index_;
    }

    /** Compare two iterators for equality */
    bool operator==(const private_iterator& other) const {
      return data_begin_ == other.data_begin_ &&
             offsets_begin_ == other.offsets_begin_ && index_ == other.index_;
    }

    /** Flag to indicate that the iterator is not a single pass iterator */
    static const bool single_pass_iterator = false;

    /** The index to the current location of the iterator */
    data_index_type index_;

    /** Iterator to the beginning of the data range */
    data_iterator_type data_begin_;

    /** Iterator to the beginning of the index range */
    index_iterator_type offsets_begin_;
  };

  /** The beginning of the data range */
  std::ranges::iterator_t<R> data_begin_;

  /** The end of the data range */
  std::ranges::iterator_t<R> data_end_;

  // const_iterator is c++23.  For now we just use an iterator to const
  /** The beginning of the index range */
  std::ranges::iterator_t<const I> index_begin_;

  /**
   * The end of the index range.  This the actual end, not the element that
   * points to the end of the data.
   */
  std::ranges::iterator_t<const I> index_end_;

  /**
   * Length of the active index range (the number of subranges).  The number of
   * subranges is one less than size of the (arrow format) index range.
   */
  std::ranges::range_difference_t<const I> num_subranges_;
};

/** Deduction guide for var_length_view */
template <class R, class I>
var_length_view(R, R, I, I)
    -> var_length_view<std::ranges::subrange<R>, std::ranges::subrange<I>>;

/** Deduction guide for var_length_view */
template <class R, class I, class J, class K>
var_length_view(R, R, J, I, I, K)
    -> var_length_view<std::ranges::subrange<R>, std::ranges::subrange<I>>;

#endif  // TILEDB_VAR_LENGTH_VIEW_H
