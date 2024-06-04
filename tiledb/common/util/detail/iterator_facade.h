/**
 * @file   tiledb/common/util/iterator_facade.h
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
 * This file implements an `iterator_facade` that will be used as part of TileDB
 * external sort.  The implementation is a modified version of
 * `iterator_facade` from the `vectorofbool/neo-fun` github repository,
 * (https://github.com/vector-of-bool/neo-fun/commit/b6c38c8 Mar 21, 2024). It
 * is used here under the terms of the Boost Software License 1.0 and is
 * Copyright (c) the author(s).
 */

#ifndef TILEDB_ITERATOR_FACADE_H
#define TILEDB_ITERATOR_FACADE_H

#include <concepts>
#include <iterator>
#include <type_traits>

#include "tiledb/common/util/detail/arrow_proxy.h"

namespace detail {

// clang-format off
template <typename Sentinel, typename Iter>
concept sized_sentinel_of =
    requires(const Iter& it, const Sentinel& sentinel) {
        it.distance_to(sentinel);
    };

template <typename>
struct infer_difference_type {
    using type = std::ptrdiff_t;
};

template <typename T>
    requires sized_sentinel_of<T, T>
struct infer_difference_type<T> {
    static const T& _it;
    using type = decltype(_it.distance_to(_it));
};

template <typename T>
using infer_difference_type_t = typename infer_difference_type<T>::type;

template <typename T>
struct infer_value_type {
    static const T& _it;
    using type = std::remove_cvref_t<decltype(*_it)>;
};

template <typename T>
    requires requires { typename T::value_type; }
struct infer_value_type<T> {
    using type = typename T::value_type;
};

template <typename T>
using infer_value_type_t = typename infer_value_type<T>::type;

template <typename T>
concept can_increment =
    requires(T& t) {
        t.increment();
    };

template <typename T>
concept can_decrement =
    requires(T& t) {
        t.decrement();
    };

template <typename T>
concept can_advance =
    requires(T& t, const infer_difference_type_t<T> d) {
        t.advance(d);
    };

template <typename T>
concept can_to_address =
    requires (const T& t) {
        { t.to_address() } -> std::contiguous_iterator;
    };

template <typename T>
concept iter_is_random_access =
    sized_sentinel_of<T, T> &&
    can_advance<T>;

template <typename T>
concept iter_is_contiguous = iter_is_random_access<T> and can_to_address<T>;

template <typename T>
concept iter_is_bidirectional =
    iter_is_random_access<T> ||
    can_decrement<T>;

template <typename T>
concept iter_is_single_pass = requires {
    requires bool(T::single_pass_iterator);
};

template <typename T>
concept iter_is_forward = !iter_is_single_pass<T> && requires(const T& item) {
    { item == item };
};

// clang-format on

// This causes internalASTError in cppcheck 2.13.0_1 (Homebrew) on macOS 14.3.1
template <typename T>
concept noexcept_incrementable = requires(T& iter) {
  { iter.increment() } noexcept;
} || requires(T& item) {
  { item += 1 } noexcept;
};

template <typename T, typename Iter>
concept iter_diff = std::convertible_to<T, infer_difference_type_t<Iter>>;

struct iterator_facade_base;

template <typename T>
concept iter_facade_type =
    std::is_base_of_v<iterator_facade_base, std::remove_cvref_t<T>>;

template <typename T>
concept iter_self = iter_facade_type<T>;

template <typename T>
concept random_access_iter_self = iter_self<T> && iter_is_random_access<T>;

template <typename T>
concept bidirectional_iter_self = iter_self<T> && iter_is_bidirectional<T>;

struct iterator_facade_base {
  /**
   * If this is a random_access_iterator, returns the distance from the right
   * to the left, i.e. how many times to apply ++right to reach `left`.
   */
  template <iter_self Self, sized_sentinel_of<Self> Sent>
  [[nodiscard]] constexpr friend auto operator-(
      const Sent& sent, const Self& self) noexcept {
    return self.distance_to(sent);
  }

  template <random_access_iter_self Self, iter_diff<Self> Diff>
  [[nodiscard]] constexpr friend Self operator-(
      const Self& self, Diff off) noexcept {
    using diff_type = infer_difference_type_t<Self>;
    using signed_diff_type = std::make_signed_t<diff_type>;
    return self + -static_cast<signed_diff_type>(off);
  }

  template <random_access_iter_self Self, iter_diff<Self> Diff>
  [[nodiscard]] constexpr friend Self operator+(
      const Self& self, Diff off) noexcept {
    auto cp = self;
    return cp += off;
  }

  template <random_access_iter_self Self, iter_diff<Self> Diff>
  [[nodiscard]] constexpr friend Self operator+(
      Diff off, const Self& self) noexcept {
    return self + off;
  }

  template <random_access_iter_self Self, iter_diff<Self> Diff>
  constexpr friend Self& operator+=(Self& self, Diff off) noexcept {
    self.advance(static_cast<detail::infer_difference_type_t<Self>>(off));
    return self;
  }

  template <random_access_iter_self Self, iter_diff<Self> Diff>
  constexpr friend Self& operator-=(Self& self, Diff off) noexcept {
    return self = self - off;
  }

  /**
   * Advance the iterator one position forward. Implemented as a call to
   * `.increment()`, if present, otherwise `*this += 1`
   *
   * @todo If `increment()` is not present, but advance() is, we should use
   * advance(1)
   */
  template <iter_self Self>
  constexpr friend Self& operator++(Self& s) noexcept(
      noexcept_incrementable<Self>) {
    if constexpr (can_increment<Self>) {
      // If there is an increment(), assume it is the most efficient way to
      // advance, even if we have an advance()
      s.increment();
    } else if constexpr (iter_is_random_access<Self>) {
      // Just offset by one
      s += 1;
    } else {
      static_assert(
          iter_is_random_access<Self> || can_increment<Self>,
          "Iterator subclass must provide an `increment` or `advance(n)` "
          "method");
    }
    return s;
  }

  // clang-format off
    template <iter_self Self>
    constexpr friend
    std::conditional_t<
        detail::iter_is_single_pass<Self>,
            void,
            std::remove_cvref_t<Self>>
    operator++(Self& self, int) noexcept(noexcept_incrementable<Self>) {
    // clang-format on
    if constexpr (detail::iter_is_single_pass<Self>) {
      // The iterator is a single-pass iterator. It isn't safe to make and
      // return an old copy.
      ++self;
    } else {
      auto cp = self;
      ++self;
      return cp;
    }
  }

  template <bidirectional_iter_self Self>
  constexpr friend Self& operator--(Self& self) noexcept {
    if constexpr (can_decrement<Self>) {
      self.decrement();
    } else {
      self -= 1;
    }
    return self;
  }

  template <bidirectional_iter_self Self>
  constexpr friend std::remove_cvref_t<Self> operator--(
      Self& self, int) noexcept {
    auto cp = self;
    --self;
    return cp;
  }

  /**
   * With three-way comparison, we can get away with much simpler
   * comparison/equality operators, since we can also rely on synthesized
   * rewrites
   */
  template <random_access_iter_self Self, detail::sized_sentinel_of<Self> S>
  [[nodiscard]] constexpr friend std::strong_ordering operator<=>(
      const Self& self, const S& right) noexcept {
    auto dist = self - right;
    auto rel = dist <=> 0;
    return rel;
  }

  /**
   * With three-way comparison, we can get away with much simpler
   * comparison/equality operators, since we can also rely on synthesized
   * rewrites
   */
  template <random_access_iter_self Self, detail::sized_sentinel_of<Self> S>
  [[nodiscard]] constexpr friend bool operator<(
      const Self& self, const S& right) noexcept {
    auto dist = self - right;
    auto rel = dist < 0;
    return rel;
  }
};

}  // namespace detail

/**
 * An iterator_facade fills-out the interface of an iterator based on just a few
 * methods be present on the derived class (provided as the CRTP parameter).
 *
 * The following methods MUST be provided:
 *
 * - Derived::dereference() - Return value of the operator*(). Need not be an
 *   actual reference.
 * - Derived::increment() OR Derived::advance(ptrdiff_t) - (Or both) Used to
 *   implement operator++() and operator++(int). If both methods are provided,
 *   `increment()` will be prefered for single increment operations.
 *
 * With these two methods defined, the iterator is a forward_iterator. If
 * `advance(ptrdiff_t)` is provided, the iterator is also bidirectional.
 *
 *
 * ====== Iterator Equality
 *
 * The generated iterator type is equality-comparible with any object of type
 * S if the derived class implements `distance_to(S)`. This includes sentinel
 * types and other instances of the iterator.
 *
 *
 * ====== Single-pass Input
 *
 * If the class provides a static member `single_pass_iterator` that is `true`,
 * then the iterator will be an input_iterator, and the operator++(int)
 * (postfix decrement) will return `void`.
 *
 *
 * ====== Bidirectional
 *
 * If the following are provided, the iterator is bidirectional:
 *
 * - Derived::decrement() OR Derived::advance(ptrdiff_t) - Used to implement
 *   operator--() and operator--(int).
 *
 * Note that unless the requirements for Random Access are met, `advance()`
 * will only be called with `1` or `-1`.
 *
 *
 * ====== Random Access
 *
 * If the following are provided, the iterator is random_access_iterator:
 *
 * - Derived::advance(ptrdiff_t p) - Move the iterator by `p` (may be negative!)
 * - Derived::distance_to(Derived other) - Return the "distance" to `other`,
 *   that is: The number of types *this must be incremented to be equal to
 *   `other`, or the numner of times *this must be decremented to reach `other`
 *   (which should yield a negative number).
 *
 * These two methods are used to implement the remainder of the iterator
 * functionality.
 *
 *
 * NOTE: A specialization of std::iterator_traits<Derived> is provided that
 * provides the required iterator typedefs. You cannot access e.g.
 * `iterator_facade<...>::value_type` directly, and must instead go through
 * `iterator_traits<Derived>::value_type`.
 */
template <typename Derived>
class iterator_facade : public detail::iterator_facade_base {
 public:
  using self_type = Derived;

 private:
  constexpr self_type& _self() noexcept {
    return static_cast<self_type&>(*this);
  }
  constexpr const self_type& _self() const noexcept {
    return static_cast<const self_type&>(*this);
  }

 public:
  /**
   * Implement operator* in terms of `.dereference()`
   */
  [[nodiscard]] constexpr decltype(auto) operator*() const
      noexcept(noexcept(_self().dereference())) {
    return _self().dereference();
  }

  /**
   * Implement arrow in terms of `operator*`, but:
   *
   * If the return type of operator* is a non-reference type, returns an
   * arrow_proxy that wraps the returned value.
   *
   * If the return type is a reference type, returns a pointer to the returned
   * object.
   */
  constexpr decltype(auto) operator->() const
      noexcept(noexcept(_self().dereference())) {
    if constexpr (detail::can_to_address<self_type>) {
      return _self().to_address();
    } else if constexpr (std::is_reference_v<
                             std::iter_reference_t<self_type>>) {
      // If operator*() returns a reference, just return that address
      return std::addressof(**this);
    } else {
      // It returned a value, so we need to wrap it in an arrow_proxy for the
      // caller
      return arrow_proxy{**this};
    }
  }

  template <detail::iter_diff<self_type> D>
  [[nodiscard]] constexpr decltype(auto) operator[](D pos) const noexcept {
    return *(_self() + pos);
  }
};

template <typename Derived, typename InnerIterator>
class iterator_wrapper_facade : public iterator_facade<Derived> {
  using base = typename iterator_wrapper_facade::iterator_facade;
  using self_type = Derived;

 protected:
  InnerIterator wrapped_iterator;

 public:
  iterator_wrapper_facade() = default;
  explicit iterator_wrapper_facade(InnerIterator it)
      : wrapped_iterator(it) {
  }

  template <detail::iter_diff<self_type> D>
  constexpr void advance(D off) noexcept {
    std::advance(wrapped_iterator, off);
  }

  // clang-format off
    constexpr auto distance_to(const self_type& other) const noexcept
        requires std::derived_from<
            std::random_access_iterator_tag,
            typename std::iterator_traits<InnerIterator>::iterator_category
            > {
        return other.wrapped_iterator - wrapped_iterator;
    }
  // clang-format on

  constexpr friend bool operator==(
      const self_type& left, const self_type& other) noexcept {
    return left.wrapped_iterator == other.wrapped_iterator;
  }
};

// Putting things in std:: namespace is a bad idea, but it is necessary for
// `iterator_traits` to work correctly with `iterator_facade`.
namespace std {

template <typename Derived>
  requires std::is_base_of_v<detail::iterator_facade_base, Derived>
struct iterator_traits<Derived> {
  static const Derived& _const_it;
  using value_type = detail::infer_value_type_t<Derived>;
  using reference = decltype(*_const_it);
  using pointer = decltype(_const_it.operator->());
  using difference_type = detail::infer_difference_type_t<Derived>;

  // Pick the iterator category based on the interfaces that it provides
  // @todo Add support for output iterators and for contiguous iterators
  using iterator_category = std::conditional_t<
      // Contiguous?
      detail::iter_is_contiguous<Derived>,
      std::contiguous_iterator_tag,
      // Not
      std::conditional_t<
          // Random access?
          detail::iter_is_random_access<Derived>,
          std::random_access_iterator_tag,
          // Nope
          std::conditional_t<
              // Bidirectional?
              detail::iter_is_bidirectional<Derived>,
              std::bidirectional_iterator_tag,
              // Noh
              std::conditional_t<
                  // Is it single-pass?
                  detail::iter_is_forward<Derived>,
                  // Otherwise it is a forward iterator
                  std::forward_iterator_tag,
                  // Than means it is an input iterator
                  std::input_iterator_tag>>>>;

  using iterator_concept = iterator_category;
};

template <typename Derived>
  requires detail::can_to_address<Derived> and
           std::derived_from<Derived, iterator_facade<Derived>>
struct pointer_traits<Derived> {
  using pointer = decltype(std::addressof(*std::declval<Derived>()));
  using element_type = std::remove_pointer_t<pointer>;
  using difference_type = std::ptrdiff_t;
};

}  // namespace std
#endif
