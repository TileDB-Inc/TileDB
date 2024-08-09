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
 * The view takes a container of containers (a range of ranges) and presents a
 * view that is a single contiguous range, with elements in the same order as
 * determined by the order of the inner ranges within the outer range.  As a
 * view, the joined view object refers to the actual ranges in the range of
 * ranges.  That is, changing an element in one of the inner ranges will be seen
 * in the joined range -- and vice versa.
 *
 * @example Suppose we have three vectors that we wish to join.
 *
 *   std::vector<int> u { 3, 1, 4 };
 *   std::vector<int> v { 1, 5, 9, 2 };
 *   std::vector<int> w { 6, 5 } ;
 *
 *   std::list<std::vector<int>> a { u, v, w };
 *
 *   auto x = join(a);
 *
 * The joined view `x` will now be a single view of the elements in `u`, `v`,
 * and `w`.  We can use that view as if it were a single container, for example:
 *
 *   for (auto&& j : x) {
 *     std::cout << j << " ";
 *   }
 *
 * Will print
 *
 *   3 1 4 1 5 9 2 6 5
 *
 * @todo Implement random-access iterator if inner and outer containers have
 * random access iterators.
 */

#ifndef TILEDB_RANGE_JOIN_H
#define TILEDB_RANGE_JOIN_H

#include <algorithm>
#include <iterator>
#include <numeric>
#include <span>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>
#include "tiledb/common/util/detail/arrow_proxy.h"

namespace tiledb::common {

/*
 * Some useful type aliases
 */

template <typename T, typename = void>
struct last_t_impl {
  using type = T;
};
template <typename T>
struct last_t_impl<T, std::enable_if_t<!std::is_same_v<typename T::Next, T>>> {
  using type = typename last_t_impl<typename T::Next>::type;
};
template <typename T>
using last_t = typename last_t_impl<T>::type;

template <class T>
using iterator_t = typename T::iterator;

template <class T>
struct const_iterator_traits {
  using type = typename T::const_iterator;
};

template <class T>
struct const_iterator_traits<std::span<T>> {
  using type = typename std::span<T>::iterator;
};

template <class T>
using const_iterator_t = typename const_iterator_traits<T>::type;

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

template <typename G>
using inner_const_reference_t = typename inner_range_t<G>::const_reference;

/**
 * A joined range view class.  Creates a single view of a range of ranges.
 * Currently, creates an input_range view, meaning the iterator associated with
 * the joined view are forward iterators.  Unlike `std::ranges::join`, which is
 * a variadic template, this view is constructed from a number of containers
 * whose cardinality is determined at run time.  As a result, also unlike
 * `std::ranges::view`, all of the inner containers must be of the same type
 * (which is fine for our purposes).
 *
 * NB: As with other containers in the C++ standard library, this view is
 * not thread-safe. Users of a joined container are responsible for protecting
 * access to it.
 */
template <class RangeOfRanges>
class join {
  using range_of_ranges_iterator = std::conditional_t<
      std::is_const_v<RangeOfRanges>,
      const_iterator_t<const RangeOfRanges>,
      iterator_t<RangeOfRanges>>;

  using inner_range_iterator = std::conditional_t<
      std::is_const_v<RangeOfRanges>,
      inner_const_iterator_t<RangeOfRanges>,
      inner_iterator_t<RangeOfRanges>>;

  range_of_ranges_iterator outer_begin_;
  range_of_ranges_iterator outer_end_;

  std::vector<size_t> offsets_;

 public:
  /**
   * Construct view from a range of ranges.  The
   * resulting view will appear as a single range, equal
   * to the concatenation of the inner ranges.
   */
  explicit join(RangeOfRanges& g)
      : outer_begin_(g.begin())
      , outer_end_(g.end())
      , offsets_(g.size() + 1) {
    /*
     * If both the outer container and inner container are random access, we can
     * support `operator[]` in the joined view.  Compute an `offsets` array to
     * support `operator[]`.
     *
     * NB: The offsets array is computed based on the sizes of the underlying
     * containers.  The offsets array will be invalid if any of the underlying
     * containers resize themselves.  This is only an issue with respect to
     * `operator[]`, other aspects of the view will work as expected if any
     * underlying container changes its size.
     *
     * @todo Add a `resize` function to the view to regenerate the offset array.
     */
    if constexpr (std::is_same_v<
                      typename std::iterator_traits<
                          typename RangeOfRanges::iterator>::iterator_category,
                      std::random_access_iterator_tag>) {
      for (size_t i = 0; i < g.size(); ++i) {
        offsets_[i] = g[i].size();
      }
      std::exclusive_scan(
          offsets_.begin(), offsets_.end(), offsets_.begin(), 0UL);
    }
  }

  // Default copy constructor and assignment operators are fine.
  join(const join&) = default;
  join(join&&) noexcept = default;
  join& operator=(const join&) = default;
  join& operator=(join&&) noexcept = default;

  /**
   * Iterator class for the joined data structure.  Currently it is a (legacy)
   * forward iterator.
   *
   * @tparam a `bool` indicating whether the definition of the iterator is for a
   * const or non-const iterator.  This hack avoids having to duplicate all of
   * the boilerplate required for separately implementing the iterator class and
   * the const iterator class.
   *
   * @todo Implement to support legacy random access iterator.  Operator[]
   * should be fairly straightforward using the `offsets` array from the
   * view the iterator belongs to.
   */
  template <bool is_const>
  class join_iterator {
   public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = inner_value_t<RangeOfRanges>;
    using difference_type = std::ptrdiff_t;
    using reference = inner_reference_t<RangeOfRanges>;
    using const_reference = inner_const_reference_t<RangeOfRanges>;
    using pointer = arrow_proxy<reference>;

   private:
    /*
     * Iterators over the outer "backbone" container. `first` is essentially a
     * cursor.
     */
    range_of_ranges_iterator first_;  //!<
    range_of_ranges_iterator last_;   //!<

    /*
     * Iterators over the inner containers.  The current element being pointed
     * to by the joined view is a combination of the outer iterator (which
     * points to the current inner container and the inner container, which
     * points to the current element in the inner container pointed to by the
     * outer iterator.
     */
    inner_range_iterator inner_begin_ = {};  //!<
    inner_range_iterator inner_end_ = {};    //!<

    join_iterator(const join_iterator& b, unsigned long step)
        : join_iterator(b) {
      first_ += step;
    }

    /*
     * Function that increments the inner iterator and properly updates the
     * outer iterator if the inner iterator crosses container boundaries.  Works
     * for empty inner ranges, including the case where an inner empty range is
     * at the beginning or end of the viewed range of ranges.
     */
    void check() {
      while (inner_begin_ == inner_end_ &&
             first_ !=
                 last_) {  // make sure we start at a valid dereference point
        if (++first_ != last_) {
          inner_begin_ = (*first_).begin();
          inner_end_ = (*first_).end();
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
        inner_begin_ = (*first_).begin();
        inner_end_ = (*first_).end();
        check();
      }
    }

    join_iterator() = default;
    join_iterator(const join_iterator&) = default;
    join_iterator(join_iterator&&) noexcept = default;

    template <bool is_const_ = is_const, class = std::enable_if<is_const_>>
    explicit join_iterator(const join_iterator<false>& rhs)
        : first_(rhs.first_)
        , last_(rhs.last_)
        , inner_begin_(rhs.inner_begin_)
        , inner_end_(rhs.inner_end_) {
    }

    join_iterator& operator=(const join_iterator&) = default;
    join_iterator& operator=(join_iterator&&) noexcept = default;

    template <bool is_const_ = is_const, class = std::enable_if<is_const_>>
    join_iterator& operator=(
        const join_iterator<false>& rhs)  // requires(is_const)
    {
      first_ = rhs.first_;
      last_ = rhs.last_;
      inner_begin_ = rhs.inner_begin_;
      inner_end_ = rhs.inner_end_;
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
      ++inner_begin_;

      check();
      return *this;
    }

    join_iterator operator++(int) {
      join_iterator it = *this;
      ++(*this);
      return it;
    }

    const_reference operator*() const {
      return reference(*inner_begin_);
    }

    typename std::conditional_t<is_const, const_reference, reference>
    operator*() {
      return reference(*inner_begin_);
    }

    pointer operator->() const {
      return {**this};
    }

    //  Functions appropriate for a random access iterator (a todo)
    //
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
  using const_reference = typename iterator::const_reference;

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

  /**
   * operator[] for the joined container.
   *
   * We can implement operator[] for the view itself, without requiring that the
   * view also provide a random access iterator, using the `offsets` array
   * created when the joined view was constructed.  Note that having
   * `operator[]` does not make the view a `random_access_range`, as the
   * requirement for `random_access_range` is that its iterator be a random
   * access iterator.
   *
   * @param i The index into the joined container.
   * @return A reference to the value in the joined container at index i.
   *
   * @todo Implement random access iterator
   */

  template <typename Dummy = value_type&>
  constexpr typename std::enable_if_t<
      std::is_same_v<
          typename std::iterator_traits<
              typename RangeOfRanges::iterator>::iterator_category,
          std::random_access_iterator_tag>,
      Dummy>
  // value_type&
  operator[](size_t i) {
    auto it_j = std::upper_bound(offsets_.begin(), offsets_.end(), i) - 1;
    auto idx_j = it_j - offsets_.begin();
    auto j = *it_j;
    auto k = i - j;
    //    auto block = outer_begin_[idx_j];
    //    return block[k];
    return outer_begin_[idx_j][k];
  }

  /**
   * Return the size of the joined view.  We do this dynamically by adding up
   * the sizes of the inner containers every time `size` is invoked in case
   * one of the underlying containers has changed its size.  (Note, however,
   * that the offsets array will probably be incorrect if an underlying
   * containers does change its size.)
   *
   * @invariant Equal to the sum of the sizes of the inner containers.
   */
  [[nodiscard]] std::size_t size() const {
    size_t n = 0;

    auto tmp = outer_begin_;

    while (tmp != outer_end_) {
      n += (*tmp).size();
      ++tmp;
    }
    return n;
  }

  /**
   * Return whether the joined view is empty.
   */
  [[nodiscard]] bool empty() const {
    return begin() == end();
  }
};

/**
 * Function to generate joined views from a container of containers (a range of
 * ranges).
 */
template <class RangeOfRanges>
static inline join<RangeOfRanges> make_join(RangeOfRanges& g) {
  return {g};
}

}  // namespace tiledb::common

#endif  // TILEDB_RANGE_JOIN_H
