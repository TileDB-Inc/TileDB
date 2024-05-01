/**
 * @file   zip_view.h
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
 */

#ifndef TILEDB_ZIP_VIEW_H
#define TILEDB_ZIP_VIEW_H

#include <ranges>
#include "iterator_facade.h"

// @todo Should this take viewable ranges?
// template <std::ranges::viewable_range... Rngs>
template <std::ranges::random_access_range... Rngs>
class zip_view {
  /**
   * Forward declaration to private (random access) iterator class
   * @todo gneralize to non-random access ranges
   */
  template <std::ranges::random_access_range... Rs>
  struct private_iterator;

  using iterator_type = private_iterator<Rngs...>;
  using const_iterator_type = private_iterator<const Rngs...>;

 public:
  /****************************************************************************
   * Constructors
   ****************************************************************************/

  /**
   * Construct a zip view from a set of ranges.  The ranges are stored in a
   * tuple.
   * @param rngs The ranges to zip
   *
   * @tparam Ranges The types of the ranges to zip
   * @param rngs The ranges to zip
   */
  template <class... Ranges>
  zip_view(Ranges&&... rngs)
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
          return const_iterator_type(std::ranges::cbegin(rngs)...);
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
          return const_iterator_type(
              std::ranges::cbegin(rngs)..., this->size());
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
  struct private_iterator : public iterator_facade<private_iterator<Rs...>> {
    using value_type_ = std::tuple<std::ranges::range_reference_t<Rs>...>;
    using index_type =
        std::common_type_t<std::ranges::range_difference_t<Rs>...>;

    /** Default constructor */
    private_iterator() = default;

    /** Construct an iterator from a set of begin iterators */
    private_iterator(
        std::ranges::iterator_t<Rs>... begins, index_type index = 0)
        : index_(index)
        , begins_(begins...) {
    }

    /*************************************************************************
     * Functions needed for iterator_facade
     * Here we just supply the minimum needed to make the iterator work
     *************************************************************************/

    /**
     * Dereference the iterator -- the critical function for defining the
     * iterator sinc the facade bases many type aliases and other functions
     * based on it and its signature
     */
    value_type_ dereference() const {
      return std::apply(
          [this](auto&&... iters) { return value_type_(iters[index_]...); },
          begins_);
    }

    /** Advance the iterator by n */
    auto advance(index_type n) {
      index_ += n;
      return *this;
    }

    /** Return the distance to another iterator */
    auto distance_to(const private_iterator& other) const {
      return other.index_ - index_;
    }

    /** Compare two iterators for equality */
    bool operator==(const private_iterator& other) const {
      return begins_ == other.begins_ && index_ == other.index_;
    }

    /*************************************************************************
     * Data members
     *************************************************************************/

    /** Current index of iterator */
    index_type index_;

    /** Begin iterators for each of the ranges being zipped */
    std::tuple<std::ranges::iterator_t<Rs>...> begins_;
  };

 private:
  /** The ranges being zipped */
  std::tuple<Rngs...> ranges_;
};

/**
 * Define "zip()" cpo for creating zip views
 */
namespace _zip {
struct _fn {
  // @todo Should this take viewable ranges?
  // template <std::ranges::viewable_range... T>
  template <std::ranges::random_access_range... T>
  auto constexpr operator()(T&&... t) const {
    return zip_view<T...>{std::forward<T>(t)...};
  }
};
}  // namespace _zip
inline namespace _cpo {
inline constexpr auto zip = _zip::_fn{};
}  // namespace _cpo

#endif  // TILEDB_ZIP_VIEW_H
