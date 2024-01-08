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

#include <memory>
#include <memory_resource>
#include "../resource-internal.h"
namespace tiledb::common {

/**
 * The allocator type used for polymorphic allocation.
 *
 * The allocator is always an allocator for `std::byte`. The user must
 * convert this into an allocator of appropriate type.
 *
 * @section Maturity
 *
 * This class is currently an alias. It will be replaced with a class derived
 * from `std::pmr::polymorphic_allocator` which works with the budgeting system.
 */
using pmr_allocator = std::pmr::polymorphic_allocator<std::byte>;

namespace resource_manager {
/*
 * forward declaration for friend of `class Memory`
 */
template <ResourceManagementPolicy P>
class ResourceManager;

namespace detail {

template <class T>
concept MemoryManagementPolicy = true;

struct UnbudgetedMemoryPolicy {
  using allocator_type = pmr_allocator;

  static allocator_type construct_allocator() {
    return {std::pmr::new_delete_resource()};
  }
};

/**
 * Traits for memory management policies.
 *
 * Specializations of this class select which memory management policy is used
 * with each resource management policy.
 */
template <ResourceManagementPolicy P>
struct MMPTraits {};

template <>
struct MMPTraits<RMPolicyUnbudgeted> : public UnbudgetedMemoryPolicy {};

/**
 * Memory manager traits for production.
 *
 * Currently the production policy is unbudgeted. This is a transitional
 * policy which all the budgeting infrastructure is being built out.
 */
template <>
struct MMPTraits<RMPolicyProduction> : public UnbudgetedMemoryPolicy {};

/**
 *
 * @tparam P Policy class for memory allocation
 */
template <MemoryManagementPolicy P>
class Memory {
  /**
   * Each memory manager contains an allocator that draws from its own budget.
   *
   * At present this allocator is pointer to the default pmr allocator in the
   * standard library.
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
   * The allocator is always a byte allocator. The user is responsible for
   * converting it into an allocator of another type.
   */
  pmr_allocator& allocator() {
    return allocator_;
  }
};

}  // namespace detail

template <ResourceManagementPolicy P>
class MemoryManager : public detail::Memory<detail::MMPTraits<P>> {
  /**
   * Constructors only visible to top-level resource manager.
   */
  friend class ResourceManager<P>;

  template <class... Args>
  explicit MemoryManager(Args&&... args)
      : detail::Memory<detail::MMPTraits<P>>(std::forward<Args>(args)...) {
  }

 public:
  MemoryManager(const MemoryManager&) = delete;
  MemoryManager(MemoryManager&&) = delete;
};

}  // namespace resource_manager

/**
 * Manageable `vector` with mandatory and polymorphic allocator.
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

}  // namespace tiledb::common

#endif