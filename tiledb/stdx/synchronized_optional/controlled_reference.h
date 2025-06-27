/**
 * @file tiledb/stdx/synchronized_optional/controlled_reference.h
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
 * This file implements `constrolled_reference` and `controlled_const_reference`
 *
 * A controlled reference provides a scope-oriented access to another object. In
 * these cases the reference is a resource that needs to be "opened" and
 * "closed", whatever that might mean for the resource in question.
 *
 * A controlled reference is a wrapper around a handle supplied by the referent
 * class. A referent may supply more than one handle if needed. A traits
 * template argument selects which handle is used. These classes may be
 * considered boilerplate for a scoped access class in a way that's common to
 * the pattern as a whole.
 *
 * Controlled references come in both `const` and non-`const` varieties. These
 * may or may not use the same handle class.
 *
 * The handle interface is asymmetric. The referent presents an `attach`
 * function that supplies a handle. The corresponding `detach` function is
 * called on the handle itself.
 */

#ifndef TILEDB_CONTROLLED_REFERENCE_H
#define TILEDB_CONTROLLED_REFERENCE_H

#include "tiledb/common/common.h"

namespace stdx {
namespace detail {

/**
 * Default traits class for controlled reference classes. Both `const` and
 * non-`const` varieties use the same traits.
 *
 * @tparam H The holder class that contains a referent object.
 */
template <class H>
class [[maybe_unused]] controlled_reference_default_traits {
  /**
   * The handle type is supplied by the holder class.
   */
  using handle_type = typename H::handle_type;

  /**
   * Ordinary factory function for handles.
   *
   * @param source
   * @return
   */
  handle_type attach(H& source) {
    return source.attach();
  }

  /**
   * Factory function for handles, `adopt_lock` variant.
   *
   * @param source
   * @return
   */
  handle_type attach(H& source, std::adopt_lock_t) {
    return source.attach(std::adopt_lock);
  }
};

/**
 * Base class for reference objects.
 *
 * @tparam T The type that this class is a reference to, as if it were `T&`
 * @tparam H A class that holds an object of type `T`
 * @tparam Tr Traits for H<T> allows different kinds of references to a holder
 */
template <
    class T,
    template <class> class H,
    class Tr = detail::controlled_reference_default_traits<H<T>>>
class controlled_reference_base {
 protected:
  using source_type = H<T>;
  using handle_type = typename Tr::handle_type;
  using const_handle_type = const handle_type;

  handle_type handle_;

  explicit controlled_reference_base(source_type& s)
      : handle_{Tr::attach(s)} {};

  explicit controlled_reference_base(source_type& s, std::adopt_lock_t)
      : handle_{Tr::attach(s, std::adopt_lock)} {};

  inline handle_type& handle() {
    return handle_;
  }

  inline const_handle_type& const_handle() const {
    return const_cast<const_handle_type&>(handle_);
  }

  static constexpr bool star_is_noexcept =
      std::is_nothrow_invocable_v<decltype(handle_.operator*())>;
  static constexpr bool const_star_is_noexcept{std::is_nothrow_invocable_v<
      decltype(const_cast<const_handle_type*>(&handle_)->operator*())>};
};

}  // namespace detail

/**
 * A reference object whose lifecycle is subordinate to an object that holds
 * the referent object.
 *
 * This class is a wrapper around a handle class provided by the source. This
 * class acts a standard interface to underlying dereference functions `operator
 * *` and `operator->` in the handle class.
 *
 * @tparam T The type that this class is a reference to, as if it were `T&`
 * @tparam H A class that holds an object of type `T`
 * @tparam Tr Traits for H<T> allows different kinds of references to a holder
 */
template <
    class T,
    template <class> class H,
    class Tr = detail::controlled_reference_default_traits<H<T>>>
class controlled_reference
    : public detail::controlled_reference_base<T, H, Tr> {
  using base_type = detail::controlled_reference_base<T, H, Tr>;

 public:
  controlled_reference() = delete;

  explicit controlled_reference(typename base_type::source_type& s)
      : base_type{s} {};

  explicit controlled_reference(
      typename base_type::source_type& s, std::adopt_lock_t)
      : base_type{s, std::adopt_lock} {};

  controlled_reference(const controlled_reference&) = delete;
  controlled_reference(controlled_reference&&) = delete;
  controlled_reference& operator=(const controlled_reference&) = delete;
  controlled_reference& operator=(controlled_reference&&) = delete;
  ~controlled_reference() = default;

  inline const T& operator*() const& noexcept(
      base_type::const_star_is_noexcept) {
    return base_type::const_handle().operator*();
  }
  inline T& operator*() & noexcept(base_type::star_is_noexcept) {
    return base_type::handle().operator*();
  }

  inline const T&& operator*() const&& noexcept(
      base_type::const_star_is_noexcept) {
    return move(base_type::const_handle().operator*());
  }
  inline T&& operator*() && noexcept(base_type::star_is_noexcept) {
    return move(base_type::handle().operator*());
  }

  /**
   * Implicit conversion operator to reference
   */
  // NOLINTNEXTLINE(google-explicit-constructor)
  inline operator T&() noexcept(base_type::star_is_noexcept) {
    return operator*();
  }

  /**
   * Implicit conversion operator to constant reference
   */
  // NOLINTNEXTLINE(google-explicit-constructor)
  inline operator const T&() const noexcept(base_type::const_star_is_noexcept) {
    return operator*();
  }
};

/**
 * A version of `controlled_reference` for `const` reference. See that class for
 * more.
 *
 * @tparam T The type that this class is a reference to, as if it were `T&`
 * @tparam H A class that holds an object of type `T`
 * @tparam Tr Traits for H<T> allows different kinds of references to a holder
 */
template <
    class T,
    template <class> class H,
    class Tr = detail::controlled_reference_default_traits<H<T>>>
class controlled_const_reference
    : public detail::controlled_reference_base<T, H, Tr> {
  using base_type = detail::controlled_reference_base<T, H, Tr>;

 public:
  controlled_const_reference() = delete;

  explicit controlled_const_reference(const typename base_type::source_type& s)
      : base_type{const_cast<typename base_type::source_type&>(s)} {};
  controlled_const_reference(const controlled_const_reference&) = delete;
  controlled_const_reference(controlled_const_reference&&) = delete;
  controlled_const_reference& operator=(const controlled_const_reference&) =
      delete;
  controlled_const_reference& operator=(controlled_const_reference&&) = delete;
  ~controlled_const_reference() = default;
  inline const T* operator->() const
      noexcept(base_type::const_arrow_is_noexcept) {
    return base_type::const_handle().operator->();
  }
  inline const T& operator*() const& noexcept(
      base_type::const_star_is_noexcept) {
    return base_type::const_handle().operator*();
  }
  inline const T&& operator*() const&& noexcept(
      base_type::const_star_is_noexcept) {
    return move(base_type::const_handle().operator*());
  }
  /**
   * Implicit conversion operator to constant reference
   */
  // NOLINTNEXTLINE(google-explicit-constructor)
  inline operator const T&() const noexcept(base_type::const_star_is_noexcept) {
    return operator*();
  }
};

}  // namespace stdx

#endif  // TILEDB_CONTROLLED_REFERENCE_H
