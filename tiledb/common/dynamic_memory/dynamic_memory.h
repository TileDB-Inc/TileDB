/**
 * @file   tiledb/common/dynamic_memory/dynamic_memory.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2021-2022 TileDB, Inc.
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
 * At the top level, this file defines custom dynamic memory facilities,
 * particularly versions of `make_shared` and `make_unique`. The standard
 * versions are augmented with an extra argument used for tracing. The smart
 * pointers that result are standard smart pointers with custom allocators. They
 * can be used just as any other smart pointer.
 *
 * At the bottom level, this file defines custom allocators for dynamic memory
 * allocation, or rather, it defines allocator-transformers that augment an
 * allocator class with extra behavior. At present these transformers are
 * defined in the same source file because they're not very complicated yet.
 */

#ifndef TILEDB_COMMON_DYNAMIC_MEMORY_H
#define TILEDB_COMMON_DYNAMIC_MEMORY_H

#include <cstddef>
#include <memory>
#include <string>

#include "../common-std.h"
#define HERE() __FILE__ ":" TILEDB_TO_STRING(__LINE__)

#include "allocate_unique.h"
#include "governed_allocator.h"
#include "tiledb/common/governor/governor.h"
#include "tiledb/common/heap_profiler.h"
#include "traced_allocator.h"

namespace tiledb::common {

namespace detail {
/**
 * The library-default tracer uses heap_profiler.* to record allocation
 * activity.
 */
struct HeapProfilerTracer {
  static void allocate(
      void* p, size_t type_size, size_t n, const TracingLabel& label) {
    heap_profiler.record_alloc(p, n * type_size, std::string(label.origin_));
  }

  static void deallocate(void* p, size_t, size_t, const TracingLabel&) {
    heap_profiler.record_dealloc(p);
  }
};

/**
 * Enabling policy class for tracing within `common::tiledb::allocator`
 *
 * @tparam M nonce class
 */
template <class M>
struct global_tracing {
  using enabled = std::false_type;
};

/*
 * As part of the documentation for `global_tracing`, the specialization below,
 * if present when `TiledbTracedAllocator` is compiled, will incorporate
 * tracing into `common::tiledb::allocator`.
 */
#if defined(TILEDB_MEMTRACE)
template <>
struct global_tracing<void> {
  using enabled = std::true_type;
};
#endif

}  // namespace detail
namespace /*anonymous*/ {

/**
 * Traced allocator type is the innermost allocator transform that constitutes
 * the common allocator.
 *
 * This allocator may be compiled with or without tracing. What determines
 * whether it is or not is the value of the `enabled` type in
 * `global_tracing<void>`. If the specialization exists, it will use whatever
 * its value is. If no such specialization exists, it will fall back to the
 * value in the non-specialized class, which is false.
 *
 * This class is in an anonymous namespace and used to define
 * tiledb::common::allocator. This makes tracing an all-or-nothing activity,
 * chosen "behind the scenes", as it were. This is a policy choice that ordinary
 * code be ignorant about whether its allocations are being traced. Individual
 * classes and functions do not have the choice to turn on tracing selectively.
 *
 * It's important to note that the constructors change depending on tracing
 * policy. Without tracing, the allocator does not take a tracing argument,
 * so functions that use the allocator must themselves be conditional on the
 * tracing policy. This is a minor syntactic burden with some repetition, but
 * it yields prompt truncation of extraneous labels when not tracing.
 *
 * @param T The allocator's `value_type`
 * @param tracing_enabled Change the default value of this argument to compile
 * in tracing calls to the TileDB allocator.
 */
template <class T>
using TiledbTracedAllocator = typename std::conditional<
    // The condition for tracing
    detail::global_tracing<void>::enabled::value,
    // The allocator used when tracing
    TracedAllocator<T, std::allocator, detail::HeapProfilerTracer>,
    // The allocator used when not tracing
    std::allocator<T>>::type;

}  // namespace

/**
 * The common allocator for the TileDB library.
 *
 * Note that "governed" is specified outside "traced". This means that tracing
 * for the return value of `allocate` happens before governing. Thus when
 * `allocate` fails, the failure gets traced before the governor can do
 * anything drastic.
 *
 * @param T The allocator's `value_type`
 */
template <class T>
using allocator = GovernedAllocator<T, TiledbTracedAllocator, Governor>;

/**
 * Predicate class for use with std::enable_if
 *
 * The required template is ignored. If weren't an argument, there couldn't be a
 * substitution for an argument and SFINAE would never apply.
 */
template <class>
struct is_tracing_enabled {
  constexpr static bool value = detail::global_tracing<void>::enabled::value;
};

template <class T = void>
constexpr bool is_tracing_enabled_v = is_tracing_enabled<T>::value;

namespace /* anonymous */ {

/**
 * This class contains implementations of allocation functions, both traced and
 * untraced version, that are then aliased outside the class for ordinary use.
 * This class has one specific but important use, which is to hide the untraced
 * fundamental implementations of functions that don't have any label argument.
 * Such functions, if exposed in any way, would be source of annoyance because
 * the would compile when tracing was off but break as soon as it was turned.
 * Thus these functions are hidden as private member functions, only available
 * to implement untraced behavior, and not aliased for general use.
 *
 * This class also acts a workaround for odd-but-persistent problems with
 * compilers during development, where sometimes unqualified `make_shared` would
 * be interpreted as `std::make_shared` even without any obvious declaration
 * that would admit it into the namespace `std` for resolution.
 *
 * Note that these functions assume that `allocator` is default-constructible.
 * This is the case for `std::allocator`, but if another allocator is used,
 * these function may have to be altered to pass any allocator arguments.
 *
 * @tparam T allocator::value_type
 */
template <class T>
class AllocationFunctions {
  /**
   * This is the fundamental implementation of `make_shared` when not tracing.
   *
   * @tparam Args Argument types of a constructor of T
   * @param args Arguments for a constructor of T
   */
  template <class... Args>
  static std::shared_ptr<T> make_shared_notrace(Args&&... args) {
    return std::allocate_shared<T>(allocator<T>(), std::forward<Args>(args)...);
  }

 public:
  /**
   * This function contains the fundamental implementation of `make_shared` when
   * tracing, and forwards when not tracing.
   *
   * @tparam Args Argument types of a constructor of T
   * @param x Identifier for this allocator in trace logs
   * @param args Arguments for a constructor of T
   */
  template <class... Args>
  static std::shared_ptr<T> make_shared(
      [[maybe_unused]] const TracingLabel& x, Args&&... args) {
    if constexpr (detail::global_tracing<void>::enabled::value) {
      return std::allocate_shared<T>(
          allocator<T>(x), std::forward<Args>(args)...);
    } else {
      return make_shared_notrace(std::forward<Args>(args)...);
    }
  }

  template <class... Args>
  static std::shared_ptr<T> make_shared(
      [[maybe_unused]] const std::string_view& origin, Args&&... args) {
    if constexpr (detail::global_tracing<void>::enabled::value) {
      return make_shared(TracingLabel(origin), std::forward<Args>(args)...);
    } else {
      return make_shared_notrace(std::forward<Args>(args)...);
    }
  }

  template <int n, class... Args>
  static std::shared_ptr<T> make_shared(
      [[maybe_unused]] const char (&origin)[n], Args&&... args) {
    if constexpr (detail::global_tracing<void>::enabled::value) {
      return make_shared(
          std::string_view(origin, n - 1), std::forward<Args>(args)...);
    } else {
      return make_shared_notrace(std::forward<Args>(args)...);
    }
  }
};

}  // namespace

/**
 *
 * @tparam T The type of the shared pointer
 * @tparam Args Argument types for the T constructor
 * @param x A TracingLabel that identifies the allocator
 * @param args Arguments for the T constructor
 * @return A standard shared_ptr allocated with tiledb::common::allocator
 */
template <class T, class... Args>
std::shared_ptr<T> make_shared(const TracingLabel& x, Args&&... args) {
  return AllocationFunctions<T>::make_shared(x, std::forward<Args>(args)...);
}

template <class T, class... Args>
std::shared_ptr<T> make_shared(const std::string_view& origin, Args&&... args) {
  return AllocationFunctions<T>::make_shared(
      origin, std::forward<Args>(args)...);
}

template <class T, int n, class... Args>
std::shared_ptr<T> make_shared(const char (&origin)[n], Args&&... args) {
  return AllocationFunctions<T>::make_shared(
      origin, std::forward<Args>(args)...);
}

template <typename RequireMakeSharedClass>
class require_make_shared {
 public:
  template <int n, class... Args>
  static std::shared_ptr<RequireMakeSharedClass> make_shared(
      const char (&origin)[n], Args&&... args) {
    class make_shared_adapter : public RequireMakeSharedClass {
     public:
      make_shared_adapter(Args&&... args)
          : RequireMakeSharedClass(std::forward<Args>(args)...) {
        // Make sure any called constructor is not public.
        static_assert(
            !std::is_constructible_v<RequireMakeSharedClass, Args...>);
      }
    };

    return tiledb::common::make_shared<make_shared_adapter>(
        origin, std::forward<Args>(args)...);
  }
};

}  // namespace tiledb::common

/**
 * `tdb_shared_ptr` was formerly a macro. Now it's an alias in the global
 * namespace. It's here for legacy compatibility only; it's not recommended for
 * new code.
 */
template <class T>
using tdb_shared_ptr = std::shared_ptr<T>;

#endif  // TILEDB_COMMON_DYNAMIC_MEMORY_H
