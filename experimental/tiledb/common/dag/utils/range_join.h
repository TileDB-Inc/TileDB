/**
 * @file   range_join.h
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
 * This file declares an adaptor for joining a range of containers into
 * a view of a single container.  Modeled after std::ranges::join (and
 * derived from NWGraph hierarchical graph container).
 *
 * Todo: Add variadic constructor / initializer list constructor so
 * that a range_join view can be constructed from a collection of
 * containers, rather than having to explicitly form a container
 * of containers (which could be expensive to construct).
 *
 * Todo: Create a random_access_range view if the constituent inner
 * ranges are random_access.
 */

#ifndef TILEDB_RANGE_JOIN_H
#define TILEDB_RANGE_JOIN_H

#include <iterator>
#include <tuple>
#include <utility>
#include "arrow_proxy.hpp"

#include "print_types.h"

namespace tiledb::common {

/*
 * Some useful type aliases
 */
template <class T>
using const_iterator_t = typename T::const_iterator;

template <class T>
using iterator_t = typename T::iterator;

template <typename G>
using inner_range_t = typename G::value_type;

template <typename G>
using inner_iterator_t = iterator_t<inner_range_t<G>>;

template <typename G>
using inner_const_iterator_t = const_iterator_t<inner_range_t<G>>;

template <typename G>
using inner_value_t = typename inner_range_t<G>::value_type;

template <typename G>
using inner_reference_t = typename inner_range_t<G>::reference;

/**
 * A joined range view class.  Creates a single view of
 * a range of ranges.  Currently, creates an input_range
 * view.
 */
template <class RangeOfRanges>
class join {
  using range_of_ranges_iterator = std::conditional_t<
      std::is_const_v<RangeOfRanges>,
      const_iterator_t<const RangeOfRanges>,
      iterator_t<RangeOfRanges>>;

  using inner_range_iterator = std::conditional_t<
      std::is_const_v<RangeOfRanges>,
      inner_iterator_t<RangeOfRanges>,
      inner_iterator_t<RangeOfRanges>>;

  range_of_ranges_iterator outer_begin_;
  range_of_ranges_iterator outer_end_;

 public:
  /**
   * Construct view from a range of ranges.  The
   * resulting view will appear as a single range, equal
   * to the concatenation of the inner ranges.
   */
  explicit join(RangeOfRanges& g)
      : outer_begin_(g.begin())
      , outer_end_(g.end()) {
  }

  // Default copy constructor and assignment operators are fine.
  join(const join&) = default;
  join(join&&) = default;
  join& operator=(const join&) = default;
  join& operator=(join&&) = default;

  template <bool is_const>
  class join_iterator {
   public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = inner_value_t<RangeOfRanges>;
    using difference_type = std::ptrdiff_t;
    using reference = inner_reference_t<RangeOfRanges>;
    using pointer = arrow_proxy<reference>;

   private:
    range_of_ranges_iterator first_;     //!<
    range_of_ranges_iterator last_;      //!<
    inner_range_iterator u_begin_ = {};  //!<
    inner_range_iterator u_end_ = {};    //!<

    join_iterator(const join_iterator& b, unsigned long step)
        : join_iterator(b) {
      first_ += step;
    }

    void check() {
      while (u_begin_ == u_end_ &&
             first_ !=
                 last_) {  // make sure we start at a valid dereference point
        if (++first_ != last_) {
          u_begin_ = (*first_).begin();
          u_end_ = (*first_).end();
        } else
          break;
      }
    }

   public:
    friend join_iterator<!is_const>;

    join_iterator(range_of_ranges_iterator begin, range_of_ranges_iterator end)
        : first_(begin)
        , last_(end) {
      if (first_ != last_) {
        u_begin_ = (*first_).begin();
        u_end_ = (*first_).end();
        check();
      }
    }

    join_iterator() = default;
    join_iterator(const join_iterator&) = default;
    join_iterator(join_iterator&&) = default;

    template <typename = std::enable_if<is_const>>
    join_iterator(const join_iterator<false>& rhs)
        : first_(rhs.first_)
        , last_(rhs.last_)
        , u_begin_(rhs.u_begin_)
        , u_end_(rhs.u_end_) {
    }

    join_iterator& operator=(const join_iterator&) = default;
    join_iterator& operator=(join_iterator&&) = default;

    template <typename = std::enable_if<is_const>>
    join_iterator& operator=(
        const join_iterator<false>& rhs)  // requires(is_const)
    {
      first_ = rhs.first_;
      last_ = rhs.last_;
      u_begin_ = rhs.u_begin_;
      u_end_ = rhs.u_end_;
      return *this;
    }

    friend bool operator==(const join_iterator& a, const join_iterator& b) {
      return a.first_ == b.first_;
    }
    friend bool operator!=(const join_iterator& a, const join_iterator& b) {
      return a.first_ != b.first_;
    }
    friend auto operator<(const join_iterator& a, const join_iterator& b) {
      return a.first_ < b.first_;
    }
    friend auto operator<=(const join_iterator& a, const join_iterator& b) {
      return a.first_ <= b.first_;
    }
    friend auto operator>(const join_iterator& a, const join_iterator& b) {
      return a.first_ > b.first_;
    }
    friend auto operator>=(const join_iterator& a, const join_iterator& b) {
      return a.first_ >= b.first_;
    }

    join_iterator& operator++() {
      ++u_begin_;

      check();
      return *this;
    }

    join_iterator operator++(int) const {
      join_iterator it = *this;
      ++(*this);
      return it;
    }

    reference operator*() const {
      return reference(*u_begin_);
    }

    pointer operator->() const {
      return {**this};
    }

    //    difference_type operator-(const join_iterator& b) const {
    //      return first_ - b.first_;
    //    }
    //    join_iterator operator+(difference_type step) const {
    //      return join_iterator(*this, step);
    //    }
  };

  using iterator = join_iterator<false>;
  using const_iterator = join_iterator<true>;

  using value_type = typename iterator::value_type;
  using reference = typename iterator::reference;

  iterator begin() {
    return {outer_begin_, outer_end_};
  }
  const_iterator begin() const {
    return {outer_begin_, outer_end_};
  }
  const_iterator cbegin() const {
    return {outer_begin_, outer_end_};
  }

  iterator end() {
    return {outer_end_, outer_end_};
  }
  const_iterator end() const {
    return {outer_end_, outer_end_};
  }
  const_iterator cend() const {
    return {outer_end_, outer_end_};
  }

  std::size_t size() const {
    size_t n = 0;

    auto tmp = outer_begin_;
    //    print_types(outer_begin_, outer_end_, *outer_begin_, *outer_end_);

    while (tmp != outer_end_) {
      n += (*tmp).size();
      ++tmp;
    }
    return n;
  }
  bool empty() const {
    return begin() == end();
  }
};

template <class RangeOfRanges>
static inline join<RangeOfRanges> make_join(RangeOfRanges& g) {
  return {g};
}

}  // namespace tiledb::common

#endif  // TILEDB_RANGE_JOIN_H
