/**
 * @file   tiledb/common/util/alt_var_length_view.h
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
 * This file contains the definition of the alt_var_length_view class, which
 * splits a given view into subranges of variable length, as delimited by
 * adjacent pairs of values in an index range.
 *
 * The difference between `alt_var_length_view` and `var_length_view` is that
 * `alt_var_length_view` maintains a materialized range of subranges, whereas
 * `var_length_view` creates subrange views on the fly as proxy objects.  As a
 * result:
 *   -  An `alt_var_length_view` does not need to refer to the offsets array
 * after it is constructed
 *   -  An `alt_var_length_view` can be sorted
 *
 *
 * Usage example:
 * ```
 * std::vector<int> x {1, 2, 3, 4, 5, 6, 7, 8, 9}; // Data range
 * std::vector<size_t> indices {0, 4, 7, 9};       // Index range
 * alt_var_length_view v(x, indices);
 * CHECK(std::ranges::equal(v[0], std::vector<int>{1, 2, 3, 4}));
 * CHECK(std::ranges::equal(v[1], std::vector<int>{5, 6, 7}));
 * CHECK(std::ranges::equal(v[2], std::vector<int>{8, 9}));
 * ```
 */

#ifndef TILEDB_ALT_VAR_LENGTH_VIEW_H
#define TILEDB_ALT_VAR_LENGTH_VIEW_H

#include <ranges>

#include "tiledb/common/util/detail/iterator_facade.h"

/**
 * A view that splits a view into subranges of variable length, as delimited by
 * a range of indices. The resulting view is a range of subranges, each of which
 * is a view into the original data range. The iterator over the
 * alt_var_length_view is a random_access_iterator that dereferences to a
 * subrange of the data range.
 *
 * @tparam R Type of the data range, required to be viewable_range
 * @tparam I Type of the index range, required to be viewable_range
 *
 * @note We use view_interface instead of view_base to get operator[] (among
 * other things).
 */

/** @todo This should go in some common place */
#define GCC_VERSION \
  (__GNUC__ * 10000 + __GNUC_MINOR__ * 100 + __GNUC_PATCHLEVEL__)

#if GCC_VERSION > 120000
template <std::ranges::viewable_range R, std::ranges::viewable_range I>
  requires std::ranges::random_access_range<R> &&
           std::ranges::random_access_range<I>
#else
template <
    std::ranges::random_access_range R,
    std::ranges::random_access_range I>
#endif
class alt_var_length_view
    : public std::ranges::view_interface<alt_var_length_view<R, I>> {
  /**
   * Forward reference of the iterator over the range of variable length data
   */
  template <class Value>
  struct private_iterator;

  /** The type of the iterator over the var length data range */
  using data_iterator_type = std::ranges::iterator_t<R>;

  /** The type that can index into the var length data range */
  using data_index_type = std::iter_difference_t<data_iterator_type>;

  /** The type dereferenced by the iterator is a subrange */
  using var_length_type = std::ranges::subrange<data_iterator_type>;

  /** The type of the iterator over the var length view */
  using var_length_iterator = private_iterator<var_length_type>;

  /** The type of the const iterator over the var length view */
  using var_length_const_iterator = private_iterator<var_length_type const>;

 public:
  /*****************************************************************************
   * Constructors
   * @note: Since this view has ownership of the subranges, we don't allow
   * copying, but do allow moves.
   ****************************************************************************/

  /** Default constructor */
  alt_var_length_view() = default;

  /** Copy constructor is deleted */
  alt_var_length_view(const alt_var_length_view&) = delete;

  /** Move constructor */
  alt_var_length_view(alt_var_length_view&&) = default;

  /** Copy assignment is deleted */
  alt_var_length_view& operator=(const alt_var_length_view&) = delete;

  /** Move assignment */
  alt_var_length_view& operator=(alt_var_length_view&&) = default;

  /**
   * Constructor taking iterator pairs for the data and index ranges, arrow
   * format
   */
  alt_var_length_view(
      std::ranges::iterator_t<R> data_begin,
      [[maybe_unused]] std::ranges::iterator_t<R> data_end,
      std::ranges::iterator_t<const I> begin_index,
      std::ranges::iterator_t<const I> index_end) {
    auto num_subranges = index_end - begin_index - 1;

    subranges_.reserve(num_subranges);
    for (long i = 0; i < num_subranges; ++i) {
      subranges_.emplace_back(
          data_begin + begin_index[i], data_begin + begin_index[i + 1]);
    }
  }

  /**
   * Constructor taking iterator pairs for the data and index ranges, tiledb
   * format
   */
  alt_var_length_view(
      std::ranges::iterator_t<R> data_begin,
      [[maybe_unused]] std::ranges::iterator_t<R> data_end,
      std::ranges::iterator_t<const I> begin_index,
      std::ranges::iterator_t<const I> index_end,
      data_index_type missing_index) {
    auto num_subranges = index_end - begin_index;

    subranges_.reserve(num_subranges);
    for (long i = 0; i < num_subranges - 1; ++i) {
      subranges_.emplace_back(
          data_begin + begin_index[i], data_begin + begin_index[i + 1]);
    }
    subranges_.emplace_back(
        data_begin + begin_index[num_subranges - 1],
        data_begin + missing_index);
  }

  /**
   * Constructor taking iterator pairs for the data and index ranges, with
   * sizes, arrow format
   */
  alt_var_length_view(
      std::ranges::iterator_t<R> data_begin,
      [[maybe_unused]] std::ranges::iterator_t<R> data_end,
      [[maybe_unused]] std::ranges::range_difference_t<R> n_data,
      std::ranges::iterator_t<const I> begin_index,
      [[maybe_unused]] std::ranges::iterator_t<const I> index_end,
      std::ranges::range_difference_t<I> n_index) {
    auto num_subranges = n_index - 1;

    subranges_.reserve(num_subranges);
    for (long i = 0; i < num_subranges; ++i) {
      subranges_.emplace_back(
          data_begin + begin_index[i], data_begin + begin_index[i + 1]);
    }
  }

  /**
   * Constructor taking iterator pairs for the data and index ranges, with
   * sizes, tiledb format
   */
  alt_var_length_view(
      std::ranges::iterator_t<R> data_begin,
      [[maybe_unused]] std::ranges::iterator_t<R> data_end,
      [[maybe_unused]] std::ranges::range_difference_t<R> n_data,
      std::ranges::iterator_t<const I> begin_index,
      [[maybe_unused]] std::ranges::iterator_t<const I> index_end,
      std::ranges::range_difference_t<I> n_index,
      data_index_type missing_index) {
    auto num_subranges = n_index;

    subranges_.reserve(num_subranges);
    for (long i = 0; i < num_subranges - 1; ++i) {
      subranges_.emplace_back(
          data_begin + begin_index[i], data_begin + begin_index[i + 1]);
    }
    subranges_.emplace_back(
        data_begin + begin_index[num_subranges - 1],
        data_begin + missing_index);
  }

  /**
   * Constructor taking ranges for the data and index ranges arrow
   * format
   */
  alt_var_length_view(R& data, const I& index) {
    auto num_subranges = std::ranges::size(index) - 1;
    auto data_begin(std::ranges::begin(data));
    auto index_begin(std::ranges::begin(index));

    subranges_.reserve(num_subranges);

    for (size_t i = 0; i < num_subranges; ++i) {
      subranges_.emplace_back(
          data_begin + index_begin[i], data_begin + index_begin[i + 1]);
    }
  }

  /**
   * Constructor taking ranges for the data and index ranges
   * tiledb format
   */
  alt_var_length_view(R& data, const I& index, data_index_type missing_index) {
    auto num_subranges = std::ranges::size(index);
    auto data_begin(std::ranges::begin(data));
    [[maybe_unused]] auto index_begin(std::ranges::begin(index));

    subranges_.reserve(num_subranges);

    for (size_t i = 0; i < num_subranges - 1; ++i) {
      subranges_.emplace_back(data_begin + index[i], data_begin + index[i + 1]);
    }
    subranges_.emplace_back(
        data_begin + index.back(), data_begin + missing_index);
  }

  /**
   * Constructor taking ranges for the data and index ranges, with sizes, arrow
   * format
   */
  alt_var_length_view(
      R& data,
      [[maybe_unused]] std::ranges::range_difference_t<R> n_data,
      const I& index,
      std::ranges::range_difference_t<I> n_index) {
    auto num_subranges = n_index - 1;
    auto data_begin(std::ranges::begin(data));
    auto index_begin(std::ranges::begin(index));

    subranges_.reserve(num_subranges);

    for (long i = 0; i < num_subranges; ++i) {
      subranges_.emplace_back(
          data_begin + index_begin[i], data_begin + index_begin[i + 1]);
    }
  }

  /**
   * Constructor taking ranges for the data and index ranges, with sizes,
   * tiledb format
   */
  alt_var_length_view(
      R& data,
      [[maybe_unused]] std::ranges::range_difference_t<R> n_data,
      const I& index,
      std::ranges::range_difference_t<I> n_index,
      data_index_type missing_index) {
    auto num_subranges = n_index;
    auto data_begin(std::ranges::begin(data));
    [[maybe_unused]] auto index_begin(std::ranges::begin(index));

    subranges_.reserve(num_subranges);

    for (long i = 0; i < num_subranges - 1; ++i) {
      subranges_.emplace_back(data_begin + index[i], data_begin + index[i + 1]);
    }
    subranges_.emplace_back(
        data_begin + index[num_subranges - 1], data_begin + missing_index);
  }

  /** Return iterator to the beginning of the var length view */
  auto begin() {
    return subranges_.begin();
  }

  /** Return iterator to the end of the var length view */
  auto end() {
    return subranges_.end();
  }

  /** Return const iterator to the beginning of the var length view */
  auto begin() const {
    return subranges_.cbegin();
  }

  /** Return const iterator to the end of the var length view */
  auto end() const {
    return subranges_.cend();
  }

  /** Return const iterator to the beginning of the var length view */
  auto cbegin() const {
    return subranges_.cbegin();
  }

  /** Return const iterator to the end of the var length view */
  auto cend() const {
    return subranges_.cend();
  }

  /** Return the number of subranges in the var length view */
  auto size() const {
    return std::ranges::size(subranges_);
  }

 private:
  std::vector<var_length_type> subranges_;
};

/** Deduction guide for alt_var_length_view */
template <class R, class I>
alt_var_length_view(R, R, I, I)
    -> alt_var_length_view<std::ranges::subrange<R>, std::ranges::subrange<I>>;

/** Deduction guide for alt_var_length_view */
template <class R, class I, class J>
alt_var_length_view(R, R, I, I, J)
    -> alt_var_length_view<std::ranges::subrange<R>, std::ranges::subrange<I>>;

/** Deduction guide for alt_var_length_view */
template <class R, class I, class J, class K>
alt_var_length_view(R, R, J, I, I, K)
    -> alt_var_length_view<std::ranges::subrange<R>, std::ranges::subrange<I>>;

/** Deduction guide for alt_var_length_view */
template <class R, class I, class J, class K, class L>
alt_var_length_view(R, R, J, I, I, K, L)
    -> alt_var_length_view<std::ranges::subrange<R>, std::ranges::subrange<I>>;

/**
 * Actually reorder the data underlying an alt_var_length_view (un-virtualize
 * the permutation).  Upon return, data will have the sorted data, offsets will
 * have the SIZES of each subrange, and the subranges will be "in order"
 * @tparam S Type of the subranges
 * @tparam R Type of the data range
 * @tparam I Type of the index / offset / size range
 * @tparam B Type of the scratch buffer
 * @param subranges The alt_var_length_view
 * @param data The data range underlying the alt_var_length_view
 * @param offsets The offsets range to be written to
 * @param buffer Scratch space used for reordering, must have enough space to
 * hold all of the data
 * @return
 *
 * @todo Make this a member of alt_var_length_view
 */
template <
    std::ranges::forward_range S,
    std::ranges::forward_range R,
    std::ranges::forward_range I,
    std::ranges::random_access_range B>
auto actualize(S& subranges, R& data, I& offsets, B& buffer) {
  auto x = buffer.begin();
  auto o = offsets.begin();
  for (auto& s : subranges) {
    std::ranges::copy(s.begin(), s.end(), x);
    auto n = s.size();
    s = std::ranges::subrange(x, x + n);
    x += n;
    *o++ = n;  // x - buffer.begin();
  }
  std::ranges::copy(buffer.begin(), buffer.begin() + data.size(), data.begin());
}
#endif  // TILEDB_ALT_VAR_LENGTH_VIEW_H
