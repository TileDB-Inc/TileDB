/**
 * @file tiledb/sm/storage_manager/job.h
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
 * This file declares the mixin for using the job system in the TileDB library.
 */

#ifndef TILEDB_CONTEXT_JOB_H
#define TILEDB_CONTEXT_JOB_H

#include "tiledb/sm/storage_manager/job_system.h"

#include "cancellation_source.h"
#include "storage_manager_declaration.h"

//-------------------------------------------------------
// Library-specific configuration
//-------------------------------------------------------
namespace tiledb::sm {
// Forward declaration
class ContextResources;

namespace job {

namespace tcj = tiledb::common::job;

/**
 * The library mixin for the job system focuses on resources.
 *
 * This mixin focuses on two entities:
 *   - The legacy storage manager. `StorageManager` is only used to handle
 *     query cancellation; this will be replaced with a cancellation system
 *     intrinsic to the job system.
 *   - `ContextResources`. Resources are now consistently accessed through a job
 *     parent, rather than going back to `class Context` or being passed as a
 *     function argument.
 *
 * Future uses of this mixin might include the following. All of them share in
 * common that they benefit from a hierarchy.
 *   - Performance measurement. This is currently in `class Stats`. Gathering
 *     performance metrics through the job system will allow hierarchical
 *     reporting.
 *   - Resource budgets, including memory. Subdivision of budget is only
 *     possible when the division is explicitly modeled.
 */
struct JobResourceMixin {
  using Self = JobResourceMixin;
  struct ParentMixin;

  /**
   * Each activity holds a reference to a storage manager, which is currently
   * the means used to implement cancellation.
   */
  class ActivityMixin : public tcj::ActivityBase<Self> {
    StorageManager& sm_;

   public:
    ActivityMixin() = delete;
    explicit ActivityMixin(StorageManager& sm)
        : ActivityBase<Self>()
        , sm_(sm){};

    /**
     * Accessor for the storage manager associated with this activity.
     */
    [[nodiscard]] StorageManager* storage_manager() {
      return &sm_;
    }
  };

  /**
   * The child activity uses the storage manager of its parent.
   */
  class ChildMixin : public tcj::ChildBase<Self> {
   public:
    ChildMixin() = delete;
    explicit ChildMixin(ParentMixin& parent)
        : ChildBase<Self>{parent, parent.parent_storage_manager()} {
    }
  };

  /**
   * The nonchild activity uses the storage manager from its constructor
   * argument.
   */
  struct NonchildMixin : public tcj::NonchildBase<Self> {
    NonchildMixin() = delete;
    explicit NonchildMixin(StorageManager& sm)
        : NonchildBase<Self>(sm) {
    }
  };

  /**
   * The supervision mixin adds nothing over the default.
   */
  struct SupervisionMixin : public tcj::SupervisionBase<Self> {
    SupervisionMixin() = delete;
    explicit SupervisionMixin(ActivityMixin& activity)
        : SupervisionBase<Self>(activity){};
  };

  /**
   * The parent (lower) propagates the storage manager of its activity (upper).
   */
  struct ParentMixin : public tcj::ParentBase<Self> {
    friend class ChildMixin;
    activity_type& activity_;

    StorageManager& parent_storage_manager() const {
      return *activity_.storage_manager();
    }

   public:
    ParentMixin() = delete;

    explicit ParentMixin(activity_type& activity)
        : tcj::ParentBase<Self>(activity)
        , activity_(activity) {
    }

    virtual ~ParentMixin() = default;

    /**
     * Accessor for the resources of this Parent
     *
     * @section Design
     *
     * This function is virtual to anticipate a future subdivision of resources.
     * At present `class ContextResources` is only an accessor to resource
     * objects; it does not carry any limitations or budget with it. In such
     * course as operations are more tightly budgeted, the resources they have
     * may be accordingly tracked and possibly limited.
     *
     * Each class derived from `Parent` implements its own resources accessor.
     * Typically this accessor will simply return its own member variable.
     */
    [[nodiscard]] virtual ContextResources& resources() const = 0;

    /**
     * Factory for cancellation source objects that are tied to the cancellation
     * state of this parent.
     */
    sm::LegacyCancellationSource make_cancellation_source() {
      return sm::LegacyCancellationSource(parent_storage_manager());
    }
  };

  /**
   * The nonparent mixin adds nothing over the default.
   */
  struct NonparentMixin : public tcj::NonparentBase<Self> {
    NonparentMixin() = delete;
    explicit NonparentMixin(ActivityMixin& activity)
        : tcj::NonparentBase<Self>(activity) {
    }
  };
};

using system_type = tcj::JobSystem<JobResourceMixin>;
}  // namespace job

//-------------------------------------------------------
// Exports
//-------------------------------------------------------
/*
 * The namespace `tiledb::sm::job` is an internal namespace, not to be used
 * externally. Below are the type declarations exported from this header.
 */
/**
 * `JobParent` should be used only as an interface, not as a base class.
 */
using JobParent = job::system_type::JobParent;

/**
 * The root class of a job tree.
 *
 * The only class derived from this is `class Context`.
 */
using JobRoot = job::system_type::JobRoot;

/**
 * The branch class of a job tree.
 *
 * Branches are both parent and child. Any composite activity is a branch.
 */
using JobBranch = job::system_type::JobBranch;

/**
 * The leaf class of a job tree.
 *
 * Leaves are non-composite activities. For example, a single long-lived I/O
 * operation could be a leaf.
 */
using JobLeaf = job::system_type::JobLeaf;

/**
 * A degenerate tree, with exactly one element.
 */
using JobIsolate = job::system_type::JobIsolate;

}  // namespace tiledb::sm

#endif  // TILEDB_CONTEXT_JOB_H
