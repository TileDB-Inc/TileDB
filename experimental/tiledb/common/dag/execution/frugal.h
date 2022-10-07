/**
 * @file   frugal.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 * This file declares a frugal (static threadpool) scheduler for dag.
 *
 * This scheduler has a fixed number of threads (determined when the scheduler
 * is constructed).  Each thread runs the `worker` method of the scheduler.
 * The `worker` method implements the scheduling of tasks.  A task is an
 * executable entity with a `resume` method.  The `worker` manages the state
 * of each task, in conjunction with a scheduler policy task and the scheduler
 * state machine (defined in scheduler.h).
 *
 * Tasks are submitted to the scheduler with the `submit` method.  The
 * `submit` method is variadic, so an arbitray number of tasks can be
 * submitted.  Task execution is lazy; tasks do not start executing when
 * submit is called.  Rather, after `submit` has been called, a "wait"
 * scheduler function is called, which will begin execution of the submitted
 * tasks.  In the case of `sync_wait_all`, the scheduler will start execution
 * of all tasks and block until they are all complete.
 *
 * Tasks are maintained on a "runnable" queue and are executed in order from
 * the queue.  When a task is executing, it is placed in the running set and
 * when it is waiting, it is placed in the waiting set.  When a task yields,
 * it is moved from the running set to the back of the runnable queue.  When a
 * task is notified, it is moved from the waiting set to the runnable queue.
 *
 * When a task has completed execution, it is moved to the finished queue.
 *
 * @todo Factor scheduler, task, and policy so they are more orthogonal and
 * can be mixed and matched.
 */

#ifndef TILEDB_DAG_FRUGAL_H
#define TILEDB_DAG_FRUGAL_H

#include <condition_variable>
#include <functional>
#include <future>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include "experimental/tiledb/common/dag/execution/scheduler.h"
#include "experimental/tiledb/common/dag/state_machine/fsm.h"
#include "experimental/tiledb/common/dag/state_machine/item_mover.h"
#include "experimental/tiledb/common/dag/utils/bounded_buffer.h"
#include "experimental/tiledb/common/dag/utils/concurrent_map.h"
#include "experimental/tiledb/common/dag/utils/concurrent_set.h"
#include "experimental/tiledb/common/dag/utils/print_types.h"

namespace tiledb::common {

namespace detail {
class frugal_exception {};

class frugal_exit {};

class frugal_wait {};

class frugal_notify {};
}  // namespace detail

constexpr const detail::frugal_exit frugal_exit;
constexpr const detail::frugal_wait frugal_wait;
constexpr const detail::frugal_notify frugal_notify;

template <class Mover, class PortState = typename Mover::PortState>
class FrugalPortPolicy : public PortFiniteStateMachine<
                             FrugalPortPolicy<Mover, PortState>,
                             PortState> {
  using state_machine_type =
      PortFiniteStateMachine<FrugalPortPolicy<Mover, PortState>, PortState>;
  using lock_type = typename state_machine_type::lock_type;

  using mover_type = Mover;

 public:
  FrugalPortPolicy() {
    if constexpr (std::is_same_v<PortState, two_stage>) {
      CHECK(str(static_cast<Mover*>(this)->state()) == "st_00");
    } else if constexpr (std::is_same_v<PortState, three_stage>) {
      CHECK(str(static_cast<Mover*>(this)->state()) == "st_000");
    }
  }

  inline void on_ac_return(lock_type&, std::atomic<int>&) {
    debug_msg("ScheduledPolicy Action return");
  }

  inline void on_source_move(lock_type&, std::atomic<int>& event) {
    static_cast<Mover*>(this)->on_move(event);
  }

  inline void on_sink_move(lock_type&, std::atomic<int>& event) {
    static_cast<Mover*>(this)->on_move(event);
  }

  inline void on_notify_source(lock_type&, std::atomic<int>&) {
    debug_msg("ScheduledPolicy Action notify source");
    throw(frugal_notify);
  }

  inline void on_notify_sink(lock_type&, std::atomic<int>&) {
    debug_msg("ScheduledPolicy Action notify sink");
    throw(frugal_notify);
  }

  inline void on_source_wait(lock_type&, std::atomic<int>&) {
    debug_msg("ScheduledPolicy Action source wait");
    throw(frugal_wait);
  }

  inline void on_sink_wait(lock_type&, std::atomic<int>&) {
    debug_msg("ScheduledPolicy Action sink wait");
    throw(frugal_wait);
  }

  inline void on_term_source(lock_type&, std::atomic<int>&) {
    debug_msg("ScheduledPolicy Action source done");
    throw(frugal_exit);
  }

  inline void on_term_sink(lock_type&, std::atomic<int>&) {
    debug_msg("ScheduledPolicy Action sink done");
    throw(frugal_exit);
  }

 private:
  void debug_msg(const std::string& msg) {
    if (static_cast<Mover*>(this)->debug_enabled()) {
      std::cout << msg << "\n";
    }
  }
};

template <class T>
using FrugalMover3 = ItemMover<FrugalPortPolicy, three_stage, T>;
template <class T>
using FrugalMover2 = ItemMover<FrugalPortPolicy, two_stage, T>;

template <class Node>
class FrugalScheduler;

template <class Task>
class FrugalSchedulerPolicy;

template <typename T>
struct SchedulerTraits<FrugalSchedulerPolicy<T>> {
  using task_type = T;
  using task_handle_type = T;
};

template <class Task>
class FrugalSchedulerPolicy
    : public SchedulerStateMachine<FrugalSchedulerPolicy<Task>> {
  using state_machine_type = SchedulerStateMachine<FrugalSchedulerPolicy<Task>>;
  using lock_type = typename state_machine_type::lock_type;

 public:
  using task_type =
      typename SchedulerTraits<FrugalSchedulerPolicy<Task>>::task_type;
  using task_handle_type =
      typename SchedulerTraits<FrugalSchedulerPolicy<Task>>::task_handle_type;

  ~FrugalSchedulerPolicy() {
    if (this->debug())
      std::cout << "policy destructor\n";

    waiting_set_.clear();
    runnable_queue_.drain();
    running_set_.clear();
    finished_queue_.drain();
  }

  void on_create(task_handle_type& task) {
    if (this->debug())
      std::cout << "calling on_create"
                << "\n";
    this->submission_queue_.push(task);
  }

  void on_stop_create(task_handle_type&) {
    if (this->debug())
      std::cout << "calling on_stop_create"
                << "\n";
  }

  void on_make_runnable(task_handle_type& task) {
    if (this->debug())
      std::cout << "calling on_make_runnable"
                << "\n";
    this->runnable_queue_.push(task);
  }

  void on_stop_runnable(task_handle_type&) {
    if (this->debug())
      std::cout << "calling on_stop_runnable"
                << "\n";
  }

  void on_make_running(task_handle_type& task) {
    if (this->debug())
      std::cout << "calling on_make_runninge"
                << "\n";
    this->running_set_.insert(task);
  }

  void on_stop_running(task_handle_type& task) {
    if (this->debug())
      std::cout << "calling on_stop_running"
                << "\n";
    auto n = this->running_set_.extract(task);

    assert(!n.empty());
  }

  void on_make_waiting(task_handle_type& task) {
    if (this->debug())
      std::cout << "calling on_make_waiting"
                << "\n";
    this->waiting_set_.insert(task);
  }

  void on_stop_waiting(task_handle_type& task) {
    if (this->debug())
      std::cout << "calling on_stop_waiting"
                << "\n";

    auto n = this->waiting_set_.extract(task);
    assert(!n.empty());

    // Try waited-on condition again
    // @todo Is this the best place to be doing this?
    n.value()->decrement_program_counter();
  }

  void on_terminate(task_handle_type& task) {
    if (this->debug())
      std::cout << "calling on_terminate"
                << "\n";
    this->finished_queue_.push(task);
  }

  void launch() {
    if (this->debug()) {
      this->dump_queue_state("Launching start");
    }
    while (true) {
      auto s = submission_queue_.try_pop();
      if (!s)
        break;
      if (this->debug())
        (*s)->dump_task_state("Admitting");

      this->task_admit(*s);
    }
    if (this->debug()) {
      this->dump_queue_state("Launching end");
    }
  }

  auto get_runnable_task() {
#if 1
    auto val = runnable_queue_.try_pop();
    while (val && ((*val)->task_state() == TaskState::terminated)) {
      val = runnable_queue_.try_pop();
    }
    return val;
#else
    auto val = runnable_queue_.pop();
#endif
    return val;
  }

  void p_shutdown() {
#if 0
    if (this->debug())
      std::cout << "policy shutdown\n";

    size_t loops = waiting_set_.size();

    for (size_t i = 0; i < loops; ++i) {
      std::vector<Task> notified_waiters_;
      notified_waiters_.reserve(waiting_set_.size());

      for (auto t : waiting_set_) {

	// Notify can change the waiting_set_
	// But it should be empty by here?
        this->task_notify(t);
      }

      for (auto&& t : notified_waiters_) {
        this->task_dispatch(t);
        waiting_set_.erase(t);
        runnable_queue_.push(t);
      }
      notified_waiters_.clear();
    }

    // all waiters must have been notified
    assert(waiting_set_.empty());

    runnable_queue_.drain();
    finished_queue_.drain();

    waiting_set_.clear();
    running_set_.clear();
#endif
  }

  void dump_queue_state(const std::string& msg = "") {
    std::string preface = (msg != "" ? msg + "\n" : "");

    std::cout << preface + "    runnable_queue_.size() = " +
                     std::to_string(runnable_queue_.size()) + "\n" +
                     "    running_set_.size() = " +
                     std::to_string(running_set_.size()) + "\n" +
                     "    waiting_set_.size() = " +
                     std::to_string(waiting_set_.size()) + "\n" +
                     "    finished_queue_.size() = " +
                     std::to_string(finished_queue_.size()) + "\n" + "\n";
  }

 private:
#if 1
  ConcurrentSet<Task> waiting_set_;
  ConcurrentSet<Task> running_set_;
  BoundedBufferQ<Task, std::queue<Task>, false> submission_queue_;
  BoundedBufferQ<Task, std::queue<Task>, false> runnable_queue_;
  BoundedBufferQ<Task, std::queue<Task>, false> finished_queue_;
#else
  ConcurrentSet<std::shared_ptr<Task>> waiting_set_;
  ConcurrentSet<std::shared_ptr<Task>> running_set_;
  BoundedBufferQ<
      std::shared_ptr<Task>,
      std::queue<std::shared_ptr<Task>>,
      false>
      submission_queue_;
  BoundedBufferQ<
      std::shared_ptr<Task>,
      std::queue<std::shared_ptr<Task>>,
      false>
      runnable_queue_;
  BoundedBufferQ<
      std::shared_ptr<Task>,
      std::queue<std::shared_ptr<Task>>,
      false>
      finished_queue_;
#endif
};

template <class Node>
class FrugalTaskImpl {
  using scheduler = FrugalScheduler<Node>;
  scheduler* scheduler_{nullptr};
  TaskState state_{TaskState::created};

  Node node_;

 public:
  FrugalTaskImpl(const Node& n)
      : node_{n} {
  }

  FrugalTaskImpl() = default;
  FrugalTaskImpl(const FrugalTaskImpl&) = default;
  FrugalTaskImpl(FrugalTaskImpl&&) = default;
  FrugalTaskImpl& operator=(const FrugalTaskImpl&) = default;
  FrugalTaskImpl& operator=(FrugalTaskImpl&&) = default;

  void set_scheduler(scheduler* sched) {
    scheduler_ = sched;
  }

  TaskState& task_state() {
    return state_;
  }

  void resume() {
    node_->resume();
  }

  void decrement_program_counter() {
    node_->decrement_program_counter();
  }

  Node correspondent() {
    return node_->correspondent_;
  }

  std::string name() {
    return node_->name() + " task";
  }

  auto id() {
    return node_->id();
  }

  virtual ~FrugalTaskImpl() = default;

  void dump_task_state(const std::string msg = "") {
    std::string preface = (msg != "" ? msg + "\n" : "");

    std::cout << preface + "    " + this->name() + " with id " +
                     std::to_string(this->id()) + "\n" +
                     "    state = " + str(this->task_state()) + "\n";
  }
};

template <class Node>
class FrugalTask : public std::shared_ptr<FrugalTaskImpl<Node>> {
  using Base = std::shared_ptr<FrugalTaskImpl<Node>>;

 public:
  FrugalTask(const Node& n)
      : Base{std::make_shared<FrugalTaskImpl<Node>>(n)} {
  }

  FrugalTask() = default;

  FrugalTask(const FrugalTask& rhs) = default;
  FrugalTask(FrugalTask&& rhs) = default;
  FrugalTask& operator=(const FrugalTask& rhs) = default;
  FrugalTask& operator=(FrugalTask&& rhs) = default;

  TaskState& task_state() {
    return (*this)->task_state();
  }
};

template <class Node>
class FrugalScheduler : public FrugalSchedulerPolicy<FrugalTask<Node>> {
  using Scheduler = FrugalScheduler<Node>;
  using Policy = FrugalSchedulerPolicy<FrugalTask<Node>>;

 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor.
   *
   * @param n The number of threads to be spawned for the thread pool.  This
   * should be a value between 1 and 256 * hardware_concurrency.  A value of
   * zero will construct the thread pool in its shutdown state--constructed
   * but not accepting nor executing any tasks.  A value of
   * 256*hardware_concurrency or larger is an error.
   */
  FrugalScheduler(size_t n)
      : concurrency_level_(n) {
    // If concurrency_level_ is set to zero, construct the thread pool in
    // shutdown state.  Explicitly shut down the task queue as well.
    if (concurrency_level_ == 0) {
      return;
    }

    // Set an upper limit on number of threads per core.  One use for this is
    // in testing error conditions in creating a context.
    if (concurrency_level_ >= 256 * std::thread::hardware_concurrency()) {
      std::string msg =
          "Error initializing frugal scheduler of concurrency level " +
          std::to_string(concurrency_level_) + "; Requested size too large";
      throw std::runtime_error(msg);
    }

    threads_.reserve(concurrency_level_);

    for (size_t i = 0; i < concurrency_level_; ++i) {
      std::thread tmp;

      // Try to launch a thread running the worker() function. If we get
      // resources_unvailable_try_again error, then try again. Three shall be
      // the maximum number of retries and the maximum number of retries shall
      // be three.
      size_t tries = 3;
      while (tries--) {
        try {
          tmp = std::thread(&FrugalScheduler::worker, this);
        } catch (const std::system_error& e) {
          if (e.code() != std::errc::resource_unavailable_try_again ||
              tries == 0) {
            shutdown();
            throw std::runtime_error(
                "Error initializing thread pool of concurrency level " +
                std::to_string(concurrency_level_) + "; " + e.what());
          }
          continue;
        }
        break;
      }

      try {
        threads_.emplace_back(std::move(tmp));
      } catch (...) {
        shutdown();
        throw;
      }
    }
  }

  /** Deleted default constructor */
  FrugalScheduler() = delete;
  FrugalScheduler(const FrugalScheduler&) = delete;

  /** Destructor. */
  ~FrugalScheduler() {
    if (this->debug())
      std::cout << "scheduler destructor\n";
    shutdown();
  }

  /* ********************************* */
  /*                API                */
  /* ********************************* */

 public:
  size_t concurrency_level() {
    return concurrency_level_;
  }

  std::atomic<bool> debug_{false};

  void enable_debug() {
    debug_.store(true);
  }
  void disable_debug() {
    debug_.store(false);
  }

  bool debug() {
    return debug_.load();
  }

 private:
  /** @todo Need to make ConcurrentMap */
  ConcurrentMap<Node, FrugalTask<Node>> node_to_task;

 public:
  void submit(Node&& n) {
    ++num_submitted_tasks_;
    ++num_tasks_;

    auto t = FrugalTask{n};

    t->set_scheduler(this);

    node_to_task[n] = t;

    this->task_create(t);

    if (this->debug()) {
      t->dump_task_state("Submitting");
      std::cout << "    with correspondent " + t->correspondent()->name() +
                       " node with id " +
                       std::to_string(t->correspondent()->id()) + "\n";
    }
  }

  /**
   * Wait on all the given tasks to complete. Since tasks are started lazily,
   * they are not actually started on submit().  So, we first start all the
   * submitted jobs and then wait on them to all be started before
   * transferring them from the submitted queue to the runnable queue.
   */
  void sync_wait_all() {
    std::cout << "Starting sync_wait_all()\n";

    /*
     * Swap the submission queue (where all the submitted tasks are) with the
     * runnable queue, making all the tasks runnable.
     */
    Policy::launch();

    this->make_ready_to_run();

    std::cout << "About to release worker threads\n";

    /*
     * Once we have put all of tasks into the runnable queue, we release the
     * worker threads.
     */
    {
      std::unique_lock _(mutex_);
      start_cv_.notify_all();
    }

    /*
     * Then we wait for the worker threads to complete.
     *
     * @todo switch to using tasks with `std::async` so we can catch
     * exceptions.
     */
    for (auto&& t : threads_) {
      t.join();
    }
    threads_.clear();
  }

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

 private:
  std::atomic<bool> ready_to_run_{false};
  void make_ready_to_run() {
    ready_to_run_.store(true);
  }

  bool ready_to_run() {
    return ready_to_run_.load();
  }

  /** The worker thread routine */
  void worker() {
    [[maybe_unused]] thread_local static auto scheduler = this;
    thread_local FrugalTask<Node> this_task{nullptr};

    /*
     * The worker threads wait on a condition variable until they are released
     * by a call to `sync_wait_all` (e.g.)
     */
    {
      std::unique_lock _(mutex_);
      start_cv_.wait(_, [this]() { return this->ready_to_run(); });
    }

    if (num_submitted_tasks_ == 0) {
      if (this->debug())
        std::cout << "No submissions, returning\n";
      return;
    }

    while (true) {
      {
        std::unique_lock _(mutex_);

        if (num_exited_tasks_ == num_submitted_tasks_) {
          if (this->debug()) {
            std::cout << "Breaking at top of while true with " +
                             std::to_string(num_exited_tasks_) +
                             " exited tasks and " +
                             std::to_string(num_submitted_tasks_) +
                             " submitted tasks\n";
            this->dump_queue_state("Breaking at top of while true");
          }
          break;
        }

        if (this->debug())
          this->dump_queue_state("About to get task");

        /*
         * Get a runnable task.
         * This is a blocking call, unless the queue is finished
         */
        auto val = this->get_runnable_task();

        if (!val) {
          if (this->debug())
            this->dump_queue_state("breaking with empty val");
          break;
        }

        auto task_to_run = *val;

        if (this->debug()) {
          this->dump_queue_state("Pre dispatch");
          task_to_run->dump_task_state();
        }

        /*
         * Transition task from runnable to running
         */
        this->task_dispatch(task_to_run);

        if (this->debug()) {
          this->dump_queue_state("Post dispatch");
          task_to_run->dump_task_state();
        }

        /*
         * Experimental throw-catch mechanism for cooperative multitasking.
         *
         * Executing tasks throw on
         */

        try {
          if (this->debug())
            task_to_run->dump_task_state("About to resume");

          _.unlock();
          task_to_run->resume();
          _.lock();

          if (this->debug())
            task_to_run->dump_task_state("Returning from resume");

        } catch (detail::frugal_wait) {
          _.lock();

          if (this->debug())
            task_to_run->dump_task_state("Caught wait");

          this->task_wait(task_to_run);

          if (this->debug())
            task_to_run->dump_task_state("Post wait");

        } catch (detail::frugal_notify) {
          _.lock();

          if (this->debug())
            task_to_run->dump_task_state("Caught notify");

          /*
           * @note Notification goes to correspondent task of task_to_run
           */
          auto task_to_notify = node_to_task[task_to_run->correspondent()];

          this->task_notify(task_to_notify);  // Need to retry what is being
                                              // notified, e.g., pull

          if (this->debug()) {
            task_to_run->dump_task_state("Post notify");
            task_to_notify->dump_task_state("Correspondent task");
          }

        } catch (detail::frugal_exit) {
          _.lock();

          if (this->debug()) {
            task_to_run->dump_task_state("Caught exit");
            this->dump_queue_state("Caught exit");
          }

          //          auto other_task =
          //          node_to_task[task_to_run->correspondent()]; if
          //          (other_task->task_state() == TaskState::waiting) {
          //            this->task_notify(other_task);
          //          }
          //          this->task_exit(other_task);
          //          this->task_notify(other_task);

          --num_tasks_;
          ++num_exited_tasks_;
          this->task_exit(task_to_run);

          if (this->debug()) {
            task_to_run->dump_task_state("Post exit");
            this->dump_queue_state("Post exit");
          }
          // break;
          // continue;

        } catch (...) {
          throw;
        }

        if (this->debug()) {
          this->dump_queue_state("Pre yield");
          task_to_run->dump_task_state();
        }

        /*
         * Perhaps deal with everything in yield?  Or dispatch?  Or both?
         */
        this->task_yield(task_to_run);

        if (this->debug()) {
          this->dump_queue_state("Post yield");
          task_to_run->dump_task_state();
        }
        if (num_exited_tasks_ == num_submitted_tasks_) {
          if (this->debug()) {
            std::cout << "Breaking at bottom of while true with " +
                             std::to_string(num_exited_tasks_) +
                             " exited tasks and " +
                             std::to_string(num_submitted_tasks_) +
                             " submitted tasks\n";
            this->dump_queue_state("Breaking at bottom of while true");
            task_to_run->dump_task_state();
          }

          break;
        }
      }  // End lock
    }    // end while(true);
    if (this->debug())
      std::cout << "Finished while (true)\n";

  }  // worker()

    /** Terminate threads in the thread pool */
    void shutdown() {
      if (this->debug())
        std::cout << "scheduler shutdown\n";

      /** Clear out any submitted tasks that haven't been put into the scheduler
       */
      this->make_ready_to_run();
      start_cv_.notify_all();

      // This doesn't seem to be necessary
      // All queues should be empty by the time we come to shutdown

      // Policy::p_shutdown();

      concurrency_level_.store(0);

      for (auto&& t : threads_) {
        t.join();
      }
      threads_.clear();
    }

    /** The worker threads */
    std::vector<std::thread> threads_;

    /** The maximum level of concurrency among all of the worker threads */
    std::atomic<size_t> concurrency_level_;

    /** Track number of tasks submited to scheduler */
    std::atomic<size_t> num_submitted_tasks_{0};

    /** Track number of tasks in the scheduler */
    std::atomic<size_t> num_tasks_{0};

    /** Track number of tasks that have exited the scheduler */
    std::atomic<size_t> num_exited_tasks_{0};

    /** Synchronization variables */
    std::mutex mutex_;
    std::condition_variable start_cv_;
  };

}  // namespace tiledb::common

#endif  // TILEDB_DAG_FRUGAL_H
