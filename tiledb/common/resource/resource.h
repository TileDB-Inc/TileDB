/**
 * @file tiledb/common/resource/resource.h
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

#ifndef TILEDB_COMMON_RESOURCE_H
#define TILEDB_COMMON_RESOURCE_H

#include "memory/memory.h"

namespace tiledb::sm {
/*
 * Forward declaration for `friend` in `class ResourceManager`.
 */
class Context;
}  // namespace tiledb::sm

namespace tiledb::common {
namespace resource_manager {

/**
 * The budget for the top-level resource manager.
 *
 * This contains a budget object for each managed resource.
 *
 * @section Maturity
 *
 * At present there's only a memory budget.
 */
class AllResourcesBudget {
  Budget memory_budget_{};
 public:
  AllResourcesBudget() = default;
};

/**
 * The unbudgeted resource management policy.
 *
 * This policy creates all resource managers as unbudgeted.
 */
struct RMPolicyUnbudgeted {
  using memory_management_policy = MMPolicyUnbudgeted<>;
};
static_assert(ResourceManagementPolicy<RMPolicyUnbudgeted>);

/**
 * The production resource management policy.
 *
 * @section Maturity
 *
 * This policy is a partially a stub, since <memory_resources> is not yet fully
 * available.
 */
struct RMPolicyProduction {
  using memory_management_policy = MMPolicyUnbudgeted<>;
};
static_assert(ResourceManagementPolicy<RMPolicyProduction>);

namespace test {
/*
 * Forward declaration for `friend` in `class ResourceManager`.
 */
template <ResourceManagementPolicy P>
class WhiteboxResourceManager;
}  // namespace test

namespace detail {
/**
 * Non-specialized implementation class for `ResourceManager`
 *
 * These are base classes from which different versions of `ResourceManager` derive.

 */
template <ResourceManagementPolicy P>
class ResourceManagerInternal {
 public:
  ResourceManagerInternal() = delete;
};

template <>
class ResourceManagerInternal<RMPolicyUnbudgeted> {
 public:
  ResourceManagerInternal() = default;
};

template <>
class ResourceManagerInternal<RMPolicyProduction> {
  AllResourcesBudget budget_;
 public:
  ResourceManagerInternal() = delete;
  explicit ResourceManagerInternal(const AllResourcesBudget& b) : budget_{b} {
  }
};

}

/**
 * The aggregate resource manager contains an individual manager for each
 * managed resource.
 *
 * The management policy P determines which specific individual resource
 * managers are compiled. Simple policies may be "everything in production" or
 * "everything unbudgeted", but any combination of these can be specified as a
 * policy.
 *
 * @tparam P The total resource management policy
 */
template <ResourceManagementPolicy P>
class ResourceManager : public detail::ResourceManagerInternal<P> {
  /**
   * Friend declaration for the `class Context`.
   *
   * The top-level division of resources happens at the C API context. The
   * actual resources are held in `class ContextResources`. This class provides
   * the original factory for creating instances of this class. Subdivisions
   * of resources occur in factory methods implemented in this class.
   */
  friend class tiledb::sm::Context;

  /**
   * Friend declaration for `class WhiteboxResourceManager`.
   *
   * This class uses private constructors in order to enforce construction
   * solely through factories. A whitebox derived class allows independent
   * testing of the constructors of this class.
   */
  friend class test::WhiteboxResourceManager<P>;

  /**
   * The resource manager for process memory.
   */
  MemoryManager<P> memory_;

  /**
   * Unbudgeted constructor is hidden and available only to friends.
   *
   * This class is the nexus for resource budgeting. There are use cases for
   * unbudgeted use of resources, principally testing.
   */
  explicit ResourceManager()
      : memory_() {
  }
 public:
  /**
   * Default constructor is deleted.
   *
   * This class is the nexus for resource budgeting. While unbudgeted use is
   * allowed technically, it's explicitly marked as such. The default
   * constructor does not provide any adequate distinction for such marks.
   */
  template <class... Args>
  explicit ResourceManager(Args&&... args)
      : detail::ResourceManagerInternal<P>(std::forward<Args>(args)...) {
  }

  /**
   * Copy constructor is deleted because of semantic necessity.
   *
   * A resource manager may contain a budget. Budgets may not be duplicated.
   */
  ResourceManager(const ResourceManager&) = delete;

  /*
   * Move constructor is deleted because of convenience.
   */
  ResourceManager(ResourceManager&&) = delete;

  /**
   * Accessor for memory resource manager.
   */
  [[nodiscard]] MemoryManager<P>& memory() {
    return memory_;
  }
};

}  // namespace resource_manager

/**
 * Vernacular name for the top-level resource manager
 */
// using RM = resource_manager::ResourceManager;

// Temporarily a particular class
// Everything using this class will need a template argument
using RM =
    resource_manager::ResourceManager<resource_manager::RMPolicyUnbudgeted>;

}  // namespace tiledb::common

#endif