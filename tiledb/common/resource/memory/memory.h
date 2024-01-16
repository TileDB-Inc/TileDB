/**
 * @file   tiledb/common/resource/memory/memory.h
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
 */

#ifndef TILEDB_COMMON_RESOURCE_MEMORY_H
#define TILEDB_COMMON_RESOURCE_MEMORY_H

#include "../resource-internal.h"

#include <concepts>
#include <memory>
#include <utility>

#include "tiledb/stdx/named_requirement/allocator.h"

/*
 * header inclusion needs to be outside any namespace
 */
#if __has_include(<memory_resource>)
namespace tiledb::common {
static constexpr bool using_memory_resources = true;
}
#include <memory_resource>
#else
namespace tiledb::common {
static constexpr bool using_memory_resources = false;
}
#endif

namespace tiledb::common {
/*
 * -------------------------------------------------------
 * Workaround for missing <memory_resource>
 *
 * The C++17 header <memory_resource> is not available on all platforms at of
 * this writing. A substitution for it is in the works, but a delay waiting for
 * it lengthens the critical path. The immediate need is to specify allocators
 * other than `std::allocator` in user code. It is not the necessary for the
 * next stage to populate a single `pmr` allocator type with different
 * allocation resources; eventually it will be.
 *
 * Thus we have the following hack:
 * - In all cases we pass allocation through a interim class that specifies
 *   allocator types and has factories for them.
 * - For systems with <memory_resource>, we are using it.
 * - For systems without <memory_resource>, we are using `std::allocator`.
 * -------------------------------------------------------
 */
/**
 * Template that specifies the common allocator type and how to use it. This
 * class is for specialization only.
 *
 * This is an interim class will be removed once a substitution for
 * <memory_resource> is available. The template argument `using_PMR` will be
 * considered always true, which means that there will no effect of the
 * compile-time polymorphics, and the definitions can all be inlined.
 *
 * @tparam using_PMR whether or not we have <memory_resources> available
 */
template <bool using_PMR>
struct IterimAllocators;

#if __has_include(<memory_resource>)
/**
 * Selection class when we have <memory_resource>
 *
 * This class is only compiled when <memory_resource> is available, because it
 * requires a definition in `std::pmr`.
 */
template <>
struct IterimAllocators<true> {
  /**
   * The first version of the polymorphic allocator is the standard one. This
   * will later be replaced with a subclass that ties in with the resource
   * management system.
   */
  template <class T = std::byte>
  using pmr_allocator = std::pmr::polymorphic_allocator<T>;
};
#endif

/**
 * Selection class when we don't have <memory_resource>.
 *
 * This class is always compiled, whether or not we have the header.
 */
template <>
struct IterimAllocators<false> {
  /*
   * The hack version is pretty much any allocator that's available, and that's
   * `std::allocator`.
   */
  template <class T = std::byte>
  using pmr_allocator = std::allocator<T>;
};

namespace detail {
/**
 * The allocator type used for polymorphic allocation.
 *
 * The allocator is always an allocator for `std::byte`. The user must convert
 * this into an allocator of appropriate type. This usually does not require any
 * explicit syntax.
 *
 * This allocator is declared in a `detail` namespace to deter direct reference
 * in user code. User code should always use the allocator type available
 * through its resource manager class. While there is no plan at present to use
 * anything but a single polymorphic allocator class everywhere in the code, we
 * are avoiding doing so with a global declaration. This choice retains at
 * option to manipulate this type in the future. It's unlikely that this will
 * ever be needed for production code, but it may find utility in test,
 * measurement, or experimental code.
 *
 * @section Maturity
 *
 * This class is using the iterim allocators at present.
 */
template <class T>
using pmr_allocator_base = IterimAllocators<tiledb::common::using_memory_resources>::pmr_allocator<T>;
}

namespace resource_manager {
/* Forward declaration */
template <bool>
struct MMPolicyUnbudgeted;
}

/** Marker class */
class OriginalAllocatorT {};
/** Marker instance */
static constexpr OriginalAllocatorT OriginalAllocator{};

/**
 * C++ allocator for memory under control of a resource manager
 *
 * @section Design
 *
 * This class does not have public constructors. Adding public constructors
 * subverts the resource management system. All allocators of this type must
 * originate in a resource manager, which thus acts as a factory for allocator
 * objects.
 *
 * @tparam T type of allocated memory
 */
template <class T = std::byte>
class pmr_allocator : public detail::pmr_allocator_base<T> {
  /*
   * pmr_allocator<T> is friends with pmr_allocator<U>
   */
  template <class U>
  friend class pmr_allocator;

  using base_type = detail::pmr_allocator_base<T>;
  friend struct resource_manager::MMPolicyUnbudgeted<false>;
  friend struct resource_manager::MMPolicyUnbudgeted<true>;

  /**
   * Default allocator is private; it is only avaliable through friend declarations.
   */
  pmr_allocator(OriginalAllocatorT, base_type x)
      : base_type(x) {
  }

 public:
  /** Copy constructor */
  pmr_allocator(const pmr_allocator&) = default;

  /** Move constuctor */
  pmr_allocator(pmr_allocator&&) = default;

  /** Copy conversion */
  template <class U>
  pmr_allocator(const pmr_allocator<U>& x)
      : pmr_allocator(
            OriginalAllocator,
            static_cast<base_type>(
                static_cast<pmr_allocator<U>::base_type>(x))){};

  /** Copy assignment */
  pmr_allocator& operator=(const pmr_allocator&) = default;

  /** Move assignment */
  pmr_allocator& operator=(pmr_allocator&&) = default;
};
static_assert(stdx::named_requirement::allocator<pmr_allocator>);

namespace resource_manager {
/**
 * Memory management policy is a template argument for both managers and containers.
 *
 * @tparam P a potential policy class
 */
template <class P>
concept MemoryManagementPolicy = requires() {
  typename P::template allocator_type<int>;
  //stdx::named_requirement::allocator<P::template allocator_type>;
} && requires(P x) {
  // At this time only checking for the symbol, not its type
  x.construct_allocator;
};

/**
 * Unbudgeted memory management policy
 *
 * @tparam UsingPMR temporary parameter
 */
template <bool UsingPMR = tiledb::common::using_memory_resources>
struct MMPolicyUnbudgeted {
  template <class T>
  using allocator_type = pmr_allocator<T>;

  static allocator_type<std::byte> construct_allocator() {
    if constexpr (UsingPMR) {
      return allocator_type<std::byte>(OriginalAllocator, std::pmr::new_delete_resource());
    } else {
      return {};
    }
  }
};
static_assert(MemoryManagementPolicy<MMPolicyUnbudgeted<true>>);
static_assert(MemoryManagementPolicy<MMPolicyUnbudgeted<false>>);

/*
 * forward declaration for friend of `class Memory`
 */
template <ResourceManagementPolicy>
class ResourceManager;

namespace detail {
template <MemoryManagementPolicy>
class MemoryBase {};

/**
 *
 * @tparam P Policy class for memory allocation
 */
template <MemoryManagementPolicy P>
class Memory : public MemoryBase<P> {
  /**
   * Each memory manager contains an allocator that draws from its own budget.
   */
  P::template allocator_type<std::byte> allocator_;

 protected:
  /**
   * Generic constructor.
   */
  template <class... Args>
  explicit Memory(Args&&... args)
      : allocator_(P::construct_allocator(std::forward<Args>(args)...)) {
  }

 public:
  Memory(const Memory&) = delete;
  Memory(Memory&&) = delete;

  /**
   * Access for the allocator of this manager
   */
  template <class T>
  P::template allocator_type<T> allocator() const {
    return P::template allocator_type<T>(allocator_);
  }
};

}  // namespace detail

template <ResourceManagementPolicy P>
class MemoryManager : public detail::Memory<typename P::memory_management_policy> {
  /**
   * Constructors only visible to top-level resource manager.
   */
  friend class ResourceManager<P>;

  template <class... Args>
  explicit MemoryManager(Args&&... args)
      : detail::Memory<typename P::memory_management_policy>(std::forward<Args>(args)...) {
  }

 public:
  MemoryManager(const MemoryManager&) = delete;
  MemoryManager(MemoryManager&&) = delete;
};

}  // namespace resource_manager

}  // namespace tiledb::common

#endif
