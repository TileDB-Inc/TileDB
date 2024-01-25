/** * @file   tiledb/common/dynamic_memory/dynamic_memory.h
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
 * `TracedAllocator` is an allocator that traces each allocation and
 * deallocation it makes. It calls out to an external tracing facility specified
 * as a template argument.
 */

#ifndef TILEDB_TRACED_ALLOCATOR_H
#define TILEDB_TRACED_ALLOCATOR_H

#include <atomic>
#include <string>

#include "../common-std.h"

namespace tiledb::common {
namespace detail {

/*
 * White-box declaration for testing classes.
 */
template <class T, template <class U> class Alloc, class Tracer>
class WhiteboxTracedAllocator;

}  // namespace detail

struct TracingLabel {
  /**
   * The origin origin to affix to traces of allocation and deallocation events.
   * The origin is (at least) a source location string identifying a line of
   * code where the allocation arose from.
   *
   * This member intentionally is a string_view and not a string in order to
   * efficiently support static char[] labels. If a non-static string is needed,
   * this class may be the base class of another that holds that string.
   */
  const std::string_view origin_;

  /**
   * The serial number is an arbitrary identifier. It identifies the allocator
   * object. When a new allocator is created for each container, it also
   * identifies the container.
   */
  unsigned long serial_number_{};

  /**
   * The cause label identifies the promixate operation that caused this origin
   * to come into existence. These are all internally-generated.
   */
  const std::string_view cause_;

  /**
   * If the cause refers to another allocator, this is the serial number of that
   * allocator. For example, if an allocator is copied, this is the allocator it
   * was copied from.
   */
  unsigned long referent_number_{};

  /**
   * The master serial number is incremented each time a new origin is created.
   */
  static /*constinit*/ std::atomic<unsigned long> master_serial_number_;

  TracingLabel(
      const std::string_view& origin,
      const std::string_view& cause,
      unsigned long referent_number)
      : origin_(origin)
      , serial_number_(master_serial_number_.fetch_add(1))
      , cause_(cause)
      , referent_number_(referent_number) {
  }

  explicit TracingLabel(const std::string_view& origin)
      : TracingLabel(origin, std::string_view("construct", 9), 0) {
  }
};

/**
 * An allocator transformer that adds tracing to an allocator.
 *
 * The default tracer uses facilities defined in `heap_memory.h`.
 *
 * @tparam Alloc An allocator that we're tracing
 */
template <class T, template <class U> class Alloc, class Tracer>
class TracedAllocator : public Alloc<T> {
  friend class detail::WhiteboxTracedAllocator<T, Alloc, Tracer>;

  using self = TracedAllocator<T, Alloc, Tracer>;
  using inner = Alloc<T>;
  using inner_traits = std::allocator_traits<inner>;

 public:
  /**
   * The origin to affix to traces of allocation and deallocation events.
   */
  const TracingLabel label_;

  using value_type = typename inner_traits::value_type;
  using pointer = typename inner_traits::pointer;
  using const_pointer = typename inner_traits::const_pointer;
  using void_pointer = typename inner_traits::void_pointer;
  using const_void_pointer = typename inner_traits::const_void_pointer;
  using difference_type = typename inner_traits::difference_type;
  using size_type = typename inner_traits::size_type;

  /*
   * Allocator propagation
   *
   * One container is used to create another container during the operations
   * of copy, move, and swap. When this occurs, the new container needs an
   * allocator. The `propagate_*` traits specify whether the allocator of the
   * original container should be propagated to the new one. The default
   * behavior is that these operations construct a new allocator instance
   * unrelated to the original, hence the default value of the traits is false.
   *
   * This allocator, however, is labelled, and we want the label to propagate
   * to a new container. We must thus override the propagation traits to obtain
   * this behavior.
   */
  using propagate_on_container_copy_assignment = std::true_type;
  using propagate_on_container_move_assignment = std::true_type;
  using propagate_on_container_swap = std::true_type;

  using is_always_equal = typename inner_traits::is_always_equal;

  /**
   * Ordinary constructor labels each allocation.
   *
   * @param label A fixed origin for this allocator
   * @param args Arguments for the allocator from which this one is derived
   */
  template <typename... Args>
  TracedAllocator(const TracingLabel& label, Args&&... args)
      : inner(std::forward<Args>(args)...)
      , label_(label) {
  }

  /*
   * Doing something more sophisticated that declaring default all the copy/move
   * constructors/assignments might be necessary for supporting allocators
   * other than `std::allocator`.
   */
  TracedAllocator(const TracedAllocator& x) noexcept = default;
  TracedAllocator(TracedAllocator&& x) noexcept = default;
  TracedAllocator& operator=(const TracedAllocator& x) noexcept = default;
  TracedAllocator& operator=(TracedAllocator&& x) noexcept = default;

  /**
   * Rebind the current allocator for class T into one for class V.  Resolution
   * of this type selects a conversion constructor to invoke.
   *
   * @tparam V Allocation type of a converted allocator
   */
  template <class V>
  struct rebind {
    using other = TracedAllocator<V, Alloc, Tracer>;
  };

  /**
   * Conversion constructor creates an allocator for V from this allocator.
   * The conversion type is given by `rebind::other`.
   *
   * @tparam V Allocation type of the converted allocator
   */
  template <class V>
  TracedAllocator(const TracedAllocator<V, Alloc, Tracer>& x) noexcept
      : inner(inner(x))
      , label_(x.label_) {
  }

  pointer allocate(std::size_t n) {
    Alloc<T>& a{*this};
    try {
      pointer p = inner_traits::allocate(a, n);
      /*
       * The Tracer requires that `pointer` be convertible to `void *`. There's
       * no issue with the standard allocator. If, however, `pointer` is not
       * `T*`, its class may need `operator void *()`.
       */
      typedef void* voidp;
      Tracer::allocate(voidp{p}, sizeof(T), n, label_);
      return p;
    } catch (const std::bad_alloc&) {
      Tracer::allocate(nullptr, sizeof(T), n, label_);
      throw;
    }
  }

  void deallocate(pointer p, std::size_t n) noexcept {
    Alloc<T>& a{*this};
    auto p_orig = std::launder(reinterpret_cast<char*>(p));
    inner_traits::deallocate(a, p, n);
    Tracer::deallocate(p_orig, sizeof(T), n, label_);
  }

  TracedAllocator select_on_container_copy_construction() const {
    return *this;
  }

  /*
   * Equality operators
   *
   * This class does not impose any new restriction on equality tests, so we
   * simply use whatever is defined in the base class.
   *
   * Allocators are always considered equal because any instance may deallocate
   * the result of any other allocator. If the heap profiler required a label
   * for deallocation events, it would require our own test for equality here.
   * As it is, we don't.
   *
   * The upshot is that we use these elements from the base class:
   * `is_always_equal`, `operator==`, and `operator!=`.
   */
};

}  // namespace tiledb::common

#endif  // TILEDB_TRACED_ALLOCATOR_H
