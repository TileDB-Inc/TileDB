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
/*
 * header inclusion needs to be outside any namespace
 */
#if __has_include(<memory_resource>)
#include <memory_resource>
#endif

namespace tiledb::common {

/*
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
 *
 * The interim class will be removed once the substitution is available.
 */
#if __has_include(<memory_resource>)
static constexpr bool using_memory_resources = true;
#include <memory_resource>
struct IterimAllocators {
  /**
   * The first version of the polymorphic allocator is the standard one. This
   * will later be replaced with a subclass that ties in with the resource
   * management system.
   */
  using pmr_allocator = ::std::pmr::polymorphic_allocator<::std::byte>;
};
#else
static constexpr bool using_memory_resources = false;
struct IterimAllocators {
  /*
   * The hack version is pretty much any allocator that's available, and that's
   * `std::allocator`.
   */
  using pmr_allocator = ::std::allocator<::std::byte>;
};
#endif

namespace resource_manager {
/*
 * forward declaration for friend of `class Memory`
 */
template <ResourceManagementPolicy P>
class ResourceManager;

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
using pmr_allocator = IterimAllocators::pmr_allocator;
}

/**
 * Memory management policy is a template argument for both managers and containers.
 *
 * @tparam P a potential policy class
 */
template <class P>
concept MemoryManagementPolicy = requires() {
  typename P::allocator_type;
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
  using allocator_type = detail::pmr_allocator;

  static allocator_type construct_allocator() {
    if constexpr (UsingPMR) {
      return {std::pmr::new_delete_resource()};
    } else {
      throw 0;
    }
  }
};
static_assert(MemoryManagementPolicy<MMPolicyUnbudgeted<>>);

namespace detail {
/**
 *
 * @tparam P Policy class for memory allocation
 */
template <MemoryManagementPolicy P>
class Memory {
  /**
   * Each memory manager contains an allocator that draws from its own budget.
   */
  P::allocator_type allocator_;

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
  P::allocator_type& allocator() {
    return allocator_;
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

/**
 * Manageable `vector` with mandatory and polymorphic allocator.
 */
#if false
/*
 * pmr::vector not available on all platforms at present
 */
template <class T>
class vector : public std::pmr::vector<T> {
  using base = std::pmr::vector<T>;

 public:
  /**
   * Default constructor is deleted.
   *
   * It would violate policy for this class to have a default constructor. All
   * `tdb` containers require an allocator for construction; they do not
   * allocate on the global heap.
   */
  vector() = delete;

  /**
   * Allocator-only constructor is the closest thing to a default constructor.
   */
  explicit vector(const pmr_allocator& a)
      : base(a) {
  }
};
#endif

}  // namespace tiledb::common

#endif