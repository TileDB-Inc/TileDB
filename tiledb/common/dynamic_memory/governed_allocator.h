/**
 * @file   tiledb/common/dynamic_memory/governed_allocator.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021 TileDB, Inc.
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
 * `GovernedAllocator` is an allocator under the supervision of a resource
 * governor. This simple version assumes that the governor always grants
 * permission to allocate. The only responsibility of the allocator is to notify
 * the governor when at out of memory condition occurs.
 *
 */

#ifndef TILEDB_COMMON_DYNAMIC_MEMORY_GOVERNED_ALLOCATOR_H
#define TILEDB_COMMON_DYNAMIC_MEMORY_GOVERNED_ALLOCATOR_H

namespace tiledb::common {

namespace detail {
/*
 * Forward declaration for testing class
 */
template <class T, template <class> class Alloc, class Governor>
class WhiteboxGovernedAllocator;

}  // namespace detail

/**
 * An allocator transformer that puts an allocator under governor control.
 *
 * At present the governor tacitly grants full permission to allocate by not
 * having an interface to grant anything less than full permission. When that
 * changes the constructor of this class will have to change.
 *
 * @tparam Alloc
 * @tparam Governor
 */
template <class T, template <class U> class Alloc, class Governor>
class GovernedAllocator : public Alloc<T> {
  friend class detail::WhiteboxGovernedAllocator<T, Alloc, Governor>;

  using self = GovernedAllocator<T, Alloc, Governor>;
  using inner = Alloc<T>;
  using inner_traits = std::allocator_traits<inner>;

 public:
  using value_type = typename inner_traits::value_type;
  using pointer = typename inner_traits::pointer;
  using const_pointer = typename inner_traits::const_pointer;
  using void_pointer = typename inner_traits::void_pointer;
  using const_void_pointer = typename inner_traits::const_void_pointer;
  using difference_type = typename inner_traits::difference_type;
  using size_type = typename inner_traits::size_type;
  using propagate_on_container_copy_assignment =
      typename inner_traits::propagate_on_container_copy_assignment;
  using propagate_on_container_move_assignment =
      typename inner_traits::propagate_on_container_move_assignment;
  using propagate_on_container_swap =
      typename inner_traits::propagate_on_container_swap;

  using is_always_equal = typename inner_traits::is_always_equal;

  /**
   * Ordinary constructor merely forwards to base allocator.
   *
   * @param args Arguments for the base allocator
   */
  template <typename... Args>
  GovernedAllocator(Args&&... args)
      : Alloc<T>(std::forward<Args>(args)...) {
  }

  /*
   * Doing something more sophisticated that declaring default all the copy/move
   * constructors/assignments might be necessary for supporting allocators
   * other than `std::allocator`.
   */
  GovernedAllocator(const GovernedAllocator& x) noexcept = default;
  GovernedAllocator(GovernedAllocator&& x) noexcept = default;
  GovernedAllocator& operator=(const GovernedAllocator& x) noexcept = default;
  GovernedAllocator& operator=(GovernedAllocator&& x) noexcept = default;

  /**
   * Rebind the current allocator for class T into one for class V.  Resolution
   * of this type selects a conversion constructor to invoke.
   *
   * @tparam V Allocation type of a converted allocator
   */
  template <class V>
  struct rebind {
    using other = GovernedAllocator<V, Alloc, Governor>;
  };

  /**
   * Conversion constructor creates an allocator for V from this allocator.
   * The conversion type is given by `rebind::other`.
   *
   * @tparam V Allocation type of the converted allocator
   */
  template <class V>
  GovernedAllocator(const GovernedAllocator<V, Alloc, Governor>& x) noexcept
      : inner(inner(x)) {
  }

  /**
   * The responsibility of this class is to notify the governor when an
   * out-of-memory condition occurs. It's not our responsibility to throw
   * exceptions.
   */
  pointer allocate(std::size_t n) {
    try {
      Alloc<T>& a{*this};
      return inner_traits::allocate(a, n);
    } catch (const std::bad_alloc&) {
      Governor::memory_panic();
      throw;
    }
  }
};

}  // namespace tiledb::common

#endif  // TILEDB_COMMON_DYNAMIC_MEMORY_GOVERNED_ALLOCATOR_H
