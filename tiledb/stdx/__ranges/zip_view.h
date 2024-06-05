/**
 * @file tiledb/stdx/__ranges/zip_view.h
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
 * This file implements a zip view for zipping together a set of ranges.
 * It is intended to implement the zip view as defined for C++23.  From
 * https://en.cppreference.com/w/cpp/ranges/zip_view:
 *  1) A zip_view is a range adaptor that takes one or more views, and produces
 * a view whose ith element is a tuple-like value consisting of the ith elements
 * of all views. The size of produced view is the minimum of sizes of all
 * adapted views. 2) zip is a customization point object that constructs a
 * zip_view.
 *
 * @note This implementation is specialized to random access iterators.  In the
 * implementation, we keep begin iterators for all of the constituents and the
 * iterator keeps and index and indexes into the begin iterators.  There are
 * two alternatives:
 *
 * 1) Use a tuple of iterators that we advance, rather than keeping an index.
 * This would work for all ranges (down to input ranges), but would be slower
 * on advancing.  It might be faster on dereferencing, since we wouldn't have
 * to index into the tuple.
 *
 * 2) Maintain a pointer back to the container from which the iterator came.
 * In that case the iterator would not need to maintain its own set of
 * iterators.  However, this would still only work for random access ranges and
 * there would be an additional level of indirection.
 *
 * @todo Implement and evaluate the above alternatives
 */

#ifndef TILEDB_ZIP_VIEW_H
#define TILEDB_ZIP_VIEW_H

#include <ranges>
#include "tiledb/common/util/detail/iterator_facade.h"
namespace stdx::ranges {
/*
 * @todo Should this take viewable ranges?  E.g.,
 * template <std::ranges::viewable_range... Rngs>
 *
 * See discussion of randeom access above
 */
template <std::ranges::random_access_range... Rngs>
class zip_view : public std::ranges::view_interface<zip_view<Rngs...>> {
  /**
   * Forward declaration to private (random access) iterator class
   * @todo Generalize to non-random access ranges
   */
  template <std::ranges::random_access_range... Rs>
  struct zip_iterator;

  template <class... Ts>
  struct zip_reference;

  using iterator_type = zip_iterator<Rngs...>;
  using const_iterator_type = zip_iterator<const Rngs...>;

 public:
  /****************************************************************************
   * Constructors
   ****************************************************************************/

  /** Default constructor */
  zip_view() = default;

  /**
   * Construct a zip view from a set of ranges.  The ranges are stored in a
   * tuple.
   * @param rngs The ranges to zip
   *
   * @tparam Ranges The types of the ranges to zip
   * @param rngs The ranges to zip
   */
  template <class... Ranges>
  constexpr explicit zip_view(Ranges&&... rngs)
      : ranges_{std::forward<Ranges>(rngs)...} {
  }

  /****************************************************************************
   *
   * Iterator accessors.  If all the ranges are random access ranges (which
   * we assume is the case for now), an iterator is a tuple of begin iterators
   * for each of
   * the ranges being zipped, plus an index.
   *
   * @note The end iterator is only defined if all the ranges are sized ranges,
   * in which case the size of the zipped view is the minimum of the sizes of
   * the ranges being zipped, and the end iterator is the begin iterator plus
   * the size of the zipped view.
   *
   * @note Some of these should be automatically generated via CRTP with
   * view_interface, but they don't seem to be.
   *
   * @todo Investigate defined behavior of CRTP with view_interface
   *
   ****************************************************************************/

  /** Return an iterator to the beginning of the zipped view. */
  auto begin() {
    return std::apply(
        [](auto&&... rngs) {
          return iterator_type(std::ranges::begin(rngs)...);
        },
        ranges_);
  }

  /**
   * Return an iterator to the end of the zipped view.  See above for what we
   * mean by "end"
   */
  auto end()
    requires(std::ranges::sized_range<Rngs> && ...)
  {
    return std::apply(
        [this](auto&&... rngs) {
          return iterator_type(std::ranges::begin(rngs)..., this->size());
        },
        ranges_);
  }

  /** Return an iterator to the beginning of a const zipped view. */
  auto begin() const {
    return std::apply(
        [](auto&&... rngs) {
          return const_iterator_type(std::ranges::begin(rngs)...);
        },
        ranges_);
  }

  /**
   * Return an iterator to the end of a const zipped view.  See above for what
   * we mean by "end"
   */
  auto end() const
    requires(std::ranges::sized_range<Rngs> && ...)
  {
    return std::apply(
        [this](auto&&... rngs) {
          return const_iterator_type(std::ranges::begin(rngs)..., this->size());
        },
        ranges_);
  }

  /** Return an iterator to the beginning of a const zipped view. */
  auto cbegin() const {
    return std::apply(
        [](auto&&... rngs) {
          return const_iterator_type(std::ranges::cbegin(rngs)...);
        },
        ranges_);
  }

  /**
   * Return an iterator to the end of a const zipped view.  See above for what
   * we mean by "end"
   */
  auto cend() const {
    return std::apply(
        [this](auto&&... rngs) {
          return const_iterator_type(
              std::ranges::cbegin(rngs)..., this->size());
        },
        ranges_);
  }

  /**
   * @brief The size of the zipped view is the size of the smallest range in the
   * view.  Requires that all the ranges are sized ranges.
   * @return The size of the smallest range in the zip view
   */
  auto size() const
    requires(std::ranges::sized_range<Rngs> && ...)
  {
    return std::apply(
        [](auto&&... rngs) { return std::min({std::ranges::size(rngs)...}); },
        ranges_);
  }

 private:
  /**
   * This is a very straightforward iterator for a zip view over a set of
   * random access ranges.  It keeps an iterator for each of the ranges being
   * zipped, along with an index into them.
   * @tparam Rs
   */
  template <std::ranges::random_access_range... Rs>
  struct zip_iterator : public iterator_facade<zip_iterator<Rs...>> {
    using index_type = std::common_type_t<std::remove_cvref_t<
        std::ranges::range_difference_t<std::remove_cvref_t<Rs>>>...>;

    /**
     * The reference and value types for the iterator.  Since the iterator
     * returns a proxy -- tuple of references -- it is very important to get
     * these exactly right, particularly for things like std::swap and
     * std::sort.
     */
    using reference = std::tuple<std::ranges::range_reference_t<Rs>...>;
    using value_type = std::tuple<std::ranges::range_value_t<Rs>...>;

    /** Default constructor */
    zip_iterator() = default;

    /** Construct an iterator from a set of begin iterators */
    zip_iterator(std::ranges::iterator_t<Rs>... begins, index_type index = 0)
        : index_(index)
        , begins_(begins...) {
    }

    /*************************************************************************
     * Functions needed for iterator_facade
     * Here we just supply the minimum needed to make the iterator work
     *************************************************************************/

    /**
     * Dereference the iterator -- the critical function for defining the
     * iterator since the facade bases many type aliases and other functions
     * based on it and its signature
     */
    reference dereference() const {
      return std::apply(
          [this]<typename... Is>(Is&&... iters) {
            return reference(std::forward<Is>(iters)[index_]...);
          },
          begins_);
    }

    /** Advance the iterator by n */
    auto advance(index_type n) {
      index_ += n;
      return *this;
    }

    /** Return the distance to another iterator */
    auto distance_to(const zip_iterator& other) const {
      return other.index_ - index_;
    }

    /** Compare two iterators for equality */
    bool operator==(const zip_iterator& other) const {
      return begins_ == other.begins_ && index_ == other.index_;
    }

    /*************************************************************************
     * Data members
     *************************************************************************/

    /** Current index of iterator */
    index_type index_;

    /** Begin iterators for each of the ranges being zipped */
    std::tuple<std::ranges::iterator_t<Rs>...> begins_;

    /*
     * Should we decide to start using std::ranges for containers and
     * algorithms, we will need to implement iter_move and iter_swap for proxy
     * iterators.
     */
#if 0
    friend constexpr auto iter_move(const zip_iterator& rhs) {
      return std::apply(
          [&]<typename... _Ts>(_Ts&&... __elts) {
            return std::tuple<
                std::invoke_result_t<decltype(std::ranges::iter_move), _Ts>...>(
                std::ranges::iter_move(
                    (std::forward<_Ts>(__elts) + rhs.index_))...);
          },
          rhs.begins_);
    }

    friend constexpr void iter_swap(
        const zip_iterator& lhs, const zip_iterator& rhs) {
      [&]<size_t... _Is>(std::index_sequence<_Is...>) {
        (std::ranges::iter_swap(
             std::get<_Is>(lhs.begins_) + lhs.index_,
             std::get<_Is>(rhs.begins_) + rhs.index_),
         ...);
      }(std::make_index_sequence<sizeof...(Rs)>{});
    }

#else
    /** Function to swap values pointed to by two iterators */
    friend constexpr void iter_swap(
        const zip_iterator& lhs, const zip_iterator& rhs) {
      [&]<size_t... _Is>(std::index_sequence<_Is...>) {
        (std::swap(
             std::get<_Is>(lhs.begins_)[lhs.index_],
             std::get<_Is>(rhs.begins_)[rhs.index_]),
         ...);
      }(std::make_index_sequence<sizeof...(Rs)>{});
    }
#endif

    friend class zip_view;
  };

 private:
  /** The ranges being zipped */
  std::tuple<Rngs...> ranges_;
};
}  // namespace stdx::ranges
/**
 * Define "zip()" cpo for creating zip views
 */
namespace _zip {
struct _fn {
  // @todo Should this take viewable ranges?
  // template <std::ranges::viewable_range... T>
  template <std::ranges::random_access_range... T>
  auto constexpr operator()(T&&... t) const {
    return stdx::ranges::zip_view<T...>{std::forward<T>(t)...};
  }
};
}  // namespace _zip

inline namespace _cpo {
inline constexpr auto zip = _zip::_fn{};
}  // namespace _cpo

namespace stdx::views {
using _cpo::zip;
}  // namespace stdx::views

namespace stdx::ranges::views {
using _cpo::zip;
}  // namespace stdx::ranges::views

/**
 * Define "swap()" for tuples of references
 *
 * Based on nwgraph implementation
 *
 * Also, Eric Niebler can "live with this"
 * template< class T, class U >
 * void swap( pair< T&, U& > && a, pair< T&, U& > && b )
 * {
 *   swap(a.first, b.first);
 *   swap(a.second, b.second);
 * }
 *
 * We generalize Eric's example to tuples.
 *
 * @note Different C++ libraries implement std::sort differently.  Some use
 * std::swap at all levels of the algorithm.  Others use insertion sort for
 * small length containers -- which will not invoke std::swap.  (In that case
 * it is very important to get the reference/value types of the iterator right.)
 */
namespace std {
template <class... Ts>
  requires(std::is_swappable<Ts>::value && ...)
void swap(std::tuple<Ts&...>&& x, std::tuple<Ts&...>&& y) {
  using std::get;
  using std::swap;

  [&]<std::size_t... i>(std::index_sequence<i...>) {
    (swap(get<i>(x), get<i>(y)), ...);
  }(std::make_index_sequence<sizeof...(Ts)>());
}
}  // namespace std

#endif  // TILEDB_ZIP_VIEW_H
