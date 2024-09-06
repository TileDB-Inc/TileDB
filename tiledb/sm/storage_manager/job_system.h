/**
 * @file tiledb/sm/storage_manager/job_system.h
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
 * This file declares class Job.
 *
 * @section Overview
 *
 * A job is a supervised activity. Jobs may supervise other jobs, forming a
 * tree. At the TileDB library uses this job system, the root of a job tree is a
 * `Context`. Major user-visible operations, such as query and consolidation,
 * are branches in the tree.
 *
 * @section Design
 *
 * Each job object is the composition of an activity object and a supervision
 * object. At the leaf of the job tree, the supervision object is trivial, since
 * a leaf does not supervise anything else. At the root of the job tree, the
 * activity object is special, since it's a unique activity that must support
 * the need to act as the root of supervision.
 *
 * Each job object is a composition between two halves, and each half comes in
 * two variants depending on its position in the tree. The halves are denoted as
 * "upper" and "lower", with the convention that the root of the tree is at the
 * top and the tree grows downward. The upper half is an `Activity`; its two
 * variants are `Child` and `Nonchild`. The lower half is a `Supervision`; its
 * two variants are `Parent` and `Nonparent`. To construct a `Child`, a
 * reference to a `Parent` in some other object is needed. To construct a
 * `Parent`, on the other hand, a reference to the `Child` in the same object is
 * needed. The `Parent` reference in the `Child` comes from outside the system;
 * it's the responsibility of the user. Conversely, the `Child` reference in the
 * `Parent` is handled entirely with the jobs system; it's not visible from the
 * outside.
 *
 * `class Activity` is a simple base class for classes that do something. That
 * "something" is specified with a mixin class. Each of the six structural
 * classes have their own mixin in order to allow appropriate application
 * behavior coherent with the job tree. The job system itself, however, has no
 * specific knowledge of what that something is.
 *
 * For example, as the job system is used within this library, `class Context`
 * is a job root but does not directly see queries. Instead, it sees them only
 * indirectly as job branches. That's because queries are not the only kind of
 * branch. This principle is vital to keeping cyclic dependencies out of the
 * library. If it were otherwise, say, by `class Context` being able to list
 * queries and their `QueryStatus` reporting values, it would mean that `class
 * Context` and everything that references it would need to link most all of the
 * library. This would be fatal to the policy that code be testable with only
 * minimal dependencies.
 *
 * @section Life Cycle
 *
 * Supervision in job tree does not extend to creating the job objects; the job
 * system does not supply factories for making specific jobs. We thus use the
 * word "supervision" instead of "control". Supervision watches what's happening
 * and is able to perform certain operations on generic jobs, but doesn't
 * control everthing that's part of the tree.
 *
 * The lifespan of a job parent must be strictly larger than that of a job
 * child. This is because the child holds both a direct reference to its parent
 * as well as a handle to a registry entry within the parent. (The handle
 * assumes existence of the registry to work correctly.) It would be technically
 * possible to weaken this requirement at the cost of greater internal
 * complexity; the design decision here was that this lifespan requirement was
 * naturally satisfied in the typical case where an operation has suboperations
 * that must finish before the operation itself does.
 *
 * @section Job States
 *
 * The activity of a job is one with some definite life span that can end.
 * Because of the life cycle requirement that the life span of a parent must
 * encompass that of a child, long-lived jobs are only appropriate near the top
 * of the tree.
 *
 * Jobs exist in one of three states: quiescent, active, halted. The initial
 * state is quiescent. The final state is halted. A job cannot be halted while
 * it still has active operations. When a job is ordered to halt, the transition
 * to the halted state does not happen immediately but only after all its active
 * operations have stopped.
 *
 * @section Maturity
 *
 * The mature part of the library are the structural elements around
 * construction of a job tree. This is a necessary first step that allows the
 * job system to be integrated with the rest of the library.
 *
 * State management is a stub at present. There are some functions defined but
 * not used. There's a tree-lock system that needs to be fully implemented, as
 * well as a tree-walk function the requires locking the tree.
 *
 * The use of job control system of this library is nascent at present. The only
 * activity that's currently tracked all centers around queries. As yet there's
 * plenty of activity that's not tracked as a `class Activity` through some job,
 * in addition to supervisors of such activity. Operations throughout the code
 * will have to be packaged as job classes for them to fall under the
 * supervision of a parent, and the parents will have to become job branches.
 * The work remaining to finish building out the jobs system can be estimated by
 * the amount of activity that's not implemented within some operation class.
 */

#ifndef TILEDB_COMMON_JOB_SYSTEM_H
#define TILEDB_COMMON_JOB_SYSTEM_H

#include <mutex>
#include "tiledb/stdx/stop_token"  // substitutes for <stop_token>

#include "cancellation_source.h"
#include "tiledb/common/registry/registry.h"

namespace tiledb::common::job {

//----------------------------------
// Supervision: Parent and Nonparent
//----------------------------------
namespace test {
template <class>
class WhiteboxSupervisionBase;
template <class>
class WhiteboxParentBase;
template <class>
class WhiteboxNonparentBase;
}  // namespace test

/**
 * Base class for the supervision (lower) half of a job.
 *
 * This class is the ultimate base of both the parent class and the not-parent
 * class. Parent objects supervise other jobs (either a branch or leaf).
 * Nonparent objects do not supervise anything.
 */
template <class Mixin>
class SupervisionBase {
  template <class>
  friend class WhiteboxSupervisionBase;

 public:
  using activity_type = typename Mixin::ActivityMixin;

 private:
  /**
   * Reference to the upper half of a job object.
   */
  activity_type& activity_;

 public:
  SupervisionBase() = delete;

  explicit SupervisionBase(activity_type& activity)
      : activity_(activity) {
  }

  /**
   * Accessor to the upper half of a job object.
   *
   * Supervision objects are the lower half, constructed second.
   */
  activity_type activity() {
    return activity_;
  }
};

/*
 * Forward declaration
 */
template <class>
class ChildBase;

/**
 * Base for classes with subordinate jobs: `JobRoot` and `JobBranch`. Aliased as
 * `JobParent`
 *
 * The overall responsibility of a job parent is to subdivide resources. In
 * order to fulfill this responsibility, this base class does two things:
 *   - Has a registry of all subordinate jobs
 *   - Provides resources to its subordinate jobs.
 */
template <class Mixin>
class ParentBase : public Mixin::SupervisionMixin,
                   private Registry<ChildBase<Mixin>> {
  template <class>
  friend class WhiteboxParentBase;

  using Base = Registry<ChildBase<Mixin>>;

  friend class ChildBase<Mixin>;

 public:
  /**
   * The type of the job handle to be held by an instance of `class Job`
   */
  using job_handle_type = typename Base::registry_handle_type;

 private:
  /**
   * Register a job as one to be governed by this context.
   *
   * @return A handle that refers to its argument
   */
  job_handle_type register_job(ChildBase<Mixin>& job) {
    return Base::register_item(job);
  }

 public:
  ParentBase() = delete;

  template <class... Args>
  explicit ParentBase(typename Mixin::ActivityMixin& activity, Args&&... args)
      : Mixin::SupervisionMixin(activity, std::forward<Args>(args)...) {
  }

  static constexpr bool is_parent = true;

  using job_size_type = typename Base::size_type;

  /**
   * The current number of jobs in this registry.
   */
  [[nodiscard]] job_size_type number_of_jobs() const {
    return Base::size();
  }

  /*
   * Stub state predicates
   *
   * We need a for_each to distinguish between active and halted.
   */
  bool is_active() {
    return Base::size() > 0;
  }
  bool is_quiescent() {
    return Base::size() == 0;
  }
  bool is_halted() {
    if (is_active()) {
      return true;
    }
    return false;
  }
};

/**
 * Base class for `Nonparent`
 */
template <class Mixin>
class NonparentBase : public Mixin::SupervisionMixin {
  template <class>
  friend class WhiteboxNonparentBase;

 public:
  explicit NonparentBase(typename Mixin::ActivityMixin& activity)
      : Mixin::SupervisionMixin(activity) {
  }

  static constexpr bool is_parent = false;

  constexpr bool is_active() {
    return false;
  }
  constexpr bool is_quiescent() {
    return true;
  }
  constexpr bool is_halted() {
    return false;
  }
};

//-------------------------------------------------------
// Activity: Child and Nonchild
//-------------------------------------------------------
/*
 * The core activity of a job is the upper half of the composite. If a job
 * is supervised by a parent, the activity is the target of supervision.
 */

//----------------------------------
// Activity
//----------------------------------
namespace test {
template <class>
class WhiteboxActivityBase;
template <class>
class WhiteboxChildBase;
template <class>
class WhiteboxNonchildBase;
}  // namespace test

/**
 * Base class for job activities
 *
 * An activity is the part of a job that's worth being supervised and monitored.
 *
 * Supervised:
 *   - Has a `cancel()` method
 *   - (maybe later) `start()` method. Currently it's start at construction.
 *   - (maybe later) `suspend()`, `resume()` methods. Would need to based on
 *     `std::jthread` to be meaningful.
 * Monitored:
 *   - A node in the job tree, visible during tree traversal
 *   - (later) A nexus for performance measurement, a.k.a. "stats"
 *
 * `class Activity` is the base class for both `class Child` and `class
 * Nonchild`, depending on whether the instance is registered with a parent.
 *
 */
template <class Mixin>
class ActivityBase {
  template <class>
  friend class test::WhiteboxActivityBase;

  NewCancellationSource new_cancellation_source_;

 protected:
  /**
   * Constructor is protected, because this is always a base class.
   */
  ActivityBase()
      : new_cancellation_source_(cancellation_origin) {
  }

 public:
  /**
   * Predicate for the "quiescent" state
   */
  bool is_quiescent() {
    if (is_active()) {
      return true;
    }
    return !new_cancellation_source_.cancellation_requested();
  }

  /**
   * Predicate for the "halted" state
   */
  bool is_halted() {
    if (is_active()) {
      return true;
    }
    return new_cancellation_source_.cancellation_requested();
  };

  /**
   * Predicate for the "active" state
   *
   * This function is virtual to allow subclasses to define their own sense
   * of `active`.
   *
   * @section Design
   *
   * This method is a concession to the maturity of the code base as a whole.
   * Ideally each activity class knows when it's active and when it's not, but
   * at present that information is not always explicit.
   */
  virtual bool is_active() {
    return true;
  }

  /**
   * Lock an activity against state change
   *
   * The base class does not itself own a mutex, so the function here is
   * trivial. It's the responsibility of each activity class to implement
   * locking in coordination with its own state changes.
   */
  virtual void lock() {
  }

  /**
   * Release the lock obtained by `lock()`
   *
   * The base class does not itself own a mutex, so the function here is
   * trivial. It's the responsibility of each activity class to implement
   * locking in coordination with its own state changes.
   */
  virtual void unlock() {
  }

  /**
   * Returns whether the lock is integrated into state change of the activity
   * class.
   *
   * The default is `true`, since this base class does no locking of its own.
   * Activity classes that do not have explicit state change governed by a mutex
   * will also have nothing to lock, and thus don't need to override this class.
   *
   * @section Design
   *
   * This method is a concession to the maturity of the code base as a whole.
   * Ideally every activity class has non-trivial locking, meaning that an
   * activity is expected to be cancellable, even for long-running I/O. Once
   * that's accomplished, `lock()` and `unlock()` can be made pure-virtual and
   * this method removed.
   */
  [[nodiscard]] virtual bool has_trivial_locking() const {
    return true;
  }
};

//----------------------------------
// Child
//----------------------------------
/**
 * Base for classes with a supervisor: `JobBranch` and `JobLeaf`
 */
template <class Mixin>
class ChildBase : public Mixin::ActivityMixin {
  template <class>
  friend class test::WhiteboxChildBase;

 public:
  using parent_type = typename Mixin::ParentMixin;

 private:
  /**
   * The parent supervises this activity.
   */
  parent_type& parent_;

  /*
   * This alias definition repeats the one in ParentBase. Were it not to be
   * repeated,
   */
  using job_handle_type =
      typename Registry<ChildBase<Mixin>>::registry_handle_type;

  /**
   * The job handle for this job, as provided by its parent
   */
  job_handle_type job_handle_;

 public:
  /**
   * Default constructor is deleted. There no such thing as an child job that
   * does not have a parent.
   */
  ChildBase() = delete;

  /**
   * Ordinary constructor
   *
   * The constructor registers the existence of this activity with its parent.
   * The caller of this constructor is responsible for doing the second phase of
   * registration.
   *
   * @param parent The parent of this job
   */
  template <class... Args>
  explicit ChildBase(parent_type& parent, Args&&... args)
      : Mixin::ActivityMixin(std::forward<Args>(args)...)
      , parent_(parent)
      , job_handle_(parent.register_job(*this)) {
  }

  /**
   * Destructor
   */
  ~ChildBase() = default;

  /**
   * Accessor to parent.
   */
  [[nodiscard]] parent_type& parent() const {
    return parent_;
  }

  /**
   * Property self-declaration. For testing.
   */
  static constexpr bool is_child{true};

  /**
   * Register this object in the parent registry with the `shared_ptr` which
   * holds this object. This function should be called immediately after the
   * object is constructed.
   *
   * @param ptr `shared_ptr<Job>` that points to this object
   */
  void register_shared_ptr(std::shared_ptr<ChildBase> ptr) {
    job_handle_.register_shared_ptr(ptr);
  }
};

//----------------------------------
// Nonchild
//----------------------------------
/**
 * Base for classes without a supervisor: `JobRoot` and `JobIsolate`
 */
template <class Mixin>
class NonchildBase : public Mixin::ActivityMixin {
  template <class>
  friend class test::WhiteboxNonchildBase;

 public:
  template <class... Args>
  explicit NonchildBase(Args&&... args)
      : Mixin::ActivityMixin(std::forward<Args>(args)...) {
  }

  static constexpr bool is_child{false};
};

//-------------------------------------------------------
// Mixin
//-------------------------------------------------------
/*
 * The job system uses a mix-in class to separate two concerns:
 *   - What supervision and activity do generically
 *   - Specific requirements needed for how the job system is used
 *
 * All specific functionality is present in the mix-in class. The overall
 * structure looks this example inheritance hierarchy:
 *   - `template <class Mixin> class ${part}Base`
 *   - `Mixin::${part}Mixin: public ${part}Base<Mixin>`
 *   - `template <class Mixin> class ${subpart}Base : public ${part}Mixin`
 *   - `Mixin::{subpart}Mixin: public ${subpart}Base<Mixin>`
 */

/**
 * Null mixin class for the job system.
 *
 * This is a do-nothing mixin class used to define the job system. It's not
 * suitable for a complete job system, because it doesn't hook into any
 * particular application mechanisms. Instead, this class illustrates how to
 * define a mixin class for actual use. The class structure and inheritance
 * illustrates how a production mixin system should look.
 *
 * Note that extraneous default constructors are deleted. This is so that any
 * defects in the inheritance hierarchy are exposed promptly.
 */
class NullMixin {
  /**
   * The `Self` alias is used for clarity when the mixin classes refer to the
   * mixed versions of the job classes. The system structure is more important
   * than the specific name of this class.
   */
  using Self = NullMixin;

 public:
  struct ParentMixin;

  /**
   * Mixin class for Activity
   */
  struct ActivityMixin : public ActivityBase<Self> {
    explicit ActivityMixin()
        : ActivityBase<Self>(){};
  };

  /**
   * Mixin class for Child
   */
  struct ChildMixin : public ChildBase<Self> {
    ChildMixin() = delete;
    explicit ChildMixin(ParentMixin& parent)
        : ChildBase<Self>(parent){};
  };

  /**
   * Mixin class for Nonchild
   */
  struct NonchildMixin : public NonchildBase<Self> {
    explicit NonchildMixin()
        : NonchildBase<Self>() {
    }
  };

  /**
   * Mixin class for Supervision
   */
  struct SupervisionMixin : public SupervisionBase<Self> {
    SupervisionMixin() = delete;
    explicit SupervisionMixin(ActivityMixin& activity)
        : SupervisionBase<Self>(activity){};
  };

  /**
   * Mixin class for Parent
   */
  struct ParentMixin : public ParentBase<Self> {
    ParentMixin() = delete;
    explicit ParentMixin(ActivityMixin& activity)
        : ParentBase<Self>(activity) {
    }
  };

  /**
   * Mixin class for Nonparent
   */
  struct NonparentMixin : public NonparentBase<Self> {
    NonparentMixin() = delete;
    explicit NonparentMixin(ActivityMixin& activity)
        : NonparentBase<Self>(activity) {
    }
  };
};

//-------------------------------------------------------
// Job and JobSystem
//-------------------------------------------------------
/**
 * Integration template unifying all of the root, branch, and leaf aspects of
 * the job system
 *
 * Note that we use public inheritance so that, for example, we can declare
 * `JobParent` arguments in constructors.
 *
 * @section States
 *
 * There are three states a job can be in:
 * - Quiescent. Nothing is active and new activity is possible
 * - Active. Something is active, either itself or some descendant.
 * - Halted. Nothing is active and new activity will not occur.
 *
 * There is no associated state machine that represents the state of a job.
 * Instead of an explicit state variable there are instead three predicate
 * functions, one for each possible state.
 *
 * There's a pseudostate "halting" state that could be made detectable, but it's
 * not implemented as a predicate function. "Halting" means it's been ordered to
 * halt but it's operations have not concluded yet. As such "halting" is a
 * sub-state of "active". It's not exposed because there's nothing further to do
 * once the job has been told to stop.
 *
 * @tparam SupervisionT Base class for parent/nonparent behavior
 * @tparam ActivityT Base class for child/nonchild behavior
 */
template <class ActivityT, class SupervisionT, class Mixin>
class Job : public ActivityT, public SupervisionT {
  using upper_type = ActivityT;
  using lower_type = SupervisionT;

 private:
  /**
   * A scope-based lock guard that manages locks on a subtree.
   *
   * We lock the child first and unlock it last. This ensures that the activity
   * in a node is locked before the activities of any children are.
   */
  class SubtreeLockGuard {
    SubtreeLockGuard() {
      upper_type::lock();
      lower_type::lock();
    }
    ~SubtreeLockGuard() {
      lower_type::unlock();
      upper_type::unlock();
    }
  };

 public:
  /**
   * Constructor.
   *
   * We forward any arguments to the upper half (the activity). Supervision
   * classes have fixed constructors.
   */
  template <class... Args>
  explicit Job(Args&&... args)
      : upper_type(std::forward<Args>(args)...)
      , lower_type(*static_cast<upper_type*>(this)) {
  }

  bool quiescent() {
    SubtreeLockGuard lg;
    if (lower_type::is_active() || upper_type::is_active()) {
      return false;
    }
    return lower_type::is_quiescent() || upper_type::is_quiescent();
  }
  bool active() {
    return lower_type::is_active() || upper_type::is_active();
  }
  bool halted() {
    if (lower_type::is_active() || upper_type::is_active()) {
      return false;
    }
    return lower_type::is_halted() && upper_type::is_halted();
  };
};

/**
 * The whole job system, consistently instantiated with the same mixin template
 *
 * Note that this is the only place that `NullMixin` is used as the default
 * for a template argument. This singular occurrence ensures that each of the
 * job class is instantiated consistently with the others.
 */
template <class Mixin = NullMixin>
struct JobSystem {
  /**
   * `JobParent` should be used only as an interface, not as a base class.
   */
  using JobParent = typename Mixin::ParentMixin;

  using parent_type = typename Mixin::ParentMixin;
  using child_type = typename Mixin::ChildMixin;
  using nonparent_type = typename Mixin::NonparentMixin;
  using nonchild_type = typename Mixin::NonchildMixin;
  /**
   * The root class of a job tree.
   */
  class JobRoot : public Job<nonchild_type, parent_type, Mixin> {
   public:
    template <class... Args>
    explicit JobRoot(Args&&... args)
        : Job<nonchild_type, parent_type, Mixin>(std::forward<Args>(args)...) {
    }
  };

  /**
   * The branch class of a job tree.
   */
  class JobBranch : public Job<child_type, parent_type, Mixin> {
   public:
    template <class... Args>
    explicit JobBranch(JobParent& parent, Args&&... args)
        : Job<child_type, parent_type, Mixin>(
              parent, std::forward<Args>(args)...) {
    }
  };

  /**
   * The leaf class of a job tree.
   */
  class JobLeaf : public Job<child_type, nonparent_type, Mixin> {
   public:
    template <class... Args>
    explicit JobLeaf(JobParent& parent, Args&&... args)
        : Job<child_type, nonparent_type, Mixin>(
              parent, std::forward<Args>(args)...) {
    }
  };

  /**
   * A degenerate tree, with exactly one element.
   *
   * This class is essentially a `job::Activity` but with the same interface as
   * the other job classes.
   */
  class JobIsolate : public Job<nonchild_type, nonparent_type, Mixin> {
   public:
    template <class... Args>
    explicit JobIsolate(Args&&... args)
        : Job<nonchild_type, nonparent_type, Mixin>(
              std::forward<Args>(args)...) {
    }
  };
};

}  // namespace tiledb::common::job

#endif  // TILEDB_CONTEXT_JOB_H
