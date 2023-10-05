/**
 * @file   duffs.h
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
 * This file declares a duffs (static threadpool) scheduler for dag.
 *
 * This scheduler has a fixed number of threads (determined when the scheduler
 * is constructed).  Each thread runs the `worker` method of the scheduler.
 * The `worker` method implements the scheduling of tasks.  A task is an
 * executable entity with a `resume` method.  The `worker` manages the state
 * of each task, in conjunction with a scheduler policy task and the scheduler
 * state machine (defined in task_state_machine.h).
 *
 * Tasks are submitted to the scheduler with the `submit` method.  The
 * `submit` method is variadic, so an arbitrary number of tasks can be
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
 * Some very basic thread-safe data structures were required for this scheduler
 * and implemented in utils subdirectory.  These are not intended to be general
 * purpose, but rather to provide just enough functionality to support the
 * scheduler.
 *
 * More complete documentation about the generic interaction between schedulers
 * and item movers can can be found in the docs subdirectory.
 *
 * @todo Factor scheduler, task, and policy so they are more orthogonal and
 * can be mixed and matched.
 */

#ifndef TILEDB_DAG_duffs_H
#define TILEDB_DAG_duffs_H

#include <condition_variable>
#include <functional>
#include <future>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

#include <tiledb/stdx/stop_token>
#include <tiledb/stdx/thread>

#include "experimental/tiledb/common/dag/execution/task.h"
#include "experimental/tiledb/common/dag/execution/task_state_machine.h"
#include "experimental/tiledb/common/dag/state_machine/fsm.h"
#include "experimental/tiledb/common/dag/state_machine/item_mover.h"
#include "experimental/tiledb/common/dag/utility/bounded_buffer.h"
#include "experimental/tiledb/common/dag/utility/concurrent_map.h"
#include "experimental/tiledb/common/dag/utility/concurrent_set.h"
#include "experimental/tiledb/common/dag/utility/print_types.h"
#include "experimental/tiledb/common/dag/utility/spinlock.h"

using namespace std::placeholders;

namespace tiledb::common {

/**
 * @brief A scheduler that uses a fixed number of threads to execute tasks and
 * an experimental "throw-catch" mechanism for signalling from port to
 * scheduler.
 *
 * @tparam Mover The type of data mover used to move data between nodes ports.
 * @tparam PortState The type of port state, either `PortState::two_stage` or
 * `PortState::three_stage`.
 */
template <class Mover, class PortState = typename Mover::PortState>
class DuffsPortPolicy : public PortFiniteStateMachine<
                            DuffsPortPolicy<Mover, PortState>,
                            PortState> {
  using state_machine_type =
      PortFiniteStateMachine<DuffsPortPolicy<Mover, PortState>, PortState>;
  using lock_type = typename state_machine_type::lock_type;

  using mover_type = Mover;

  using scheduler_event_type = SchedulerAction;

 public:
  constexpr static bool wait_returns_{false};
  /**
   * @brief Constructs a port policy.  Initializes the port state to empty.
   * Uses `enable_if` to select between two-stage and three-stage port state for
   * initialization values.
   */
  DuffsPortPolicy() {
    if constexpr (std::is_same_v<PortState, two_stage>) {
      assert(static_cast<Mover*>(this)->state() == PortState::st_00);
    } else if constexpr (std::is_same_v<PortState, three_stage>) {
      assert(static_cast<Mover*>(this)->state() == PortState::st_000);
    }
  }

  /**
   * Policy action called on the port `ac_return` action.
   */
  inline scheduler_event_type on_ac_return(lock_type&, std::atomic<int>&) {
    return scheduler_event_type::noop;
  }

  /**
   * Policy action called on the port `on_source_move` action.
   */
  inline scheduler_event_type on_source_move(
      lock_type&, std::atomic<int>& event) {
    static_cast<Mover*>(this)->on_move(event);
    return scheduler_event_type::noop;
  }

  /**
   * Policy action called on the port `on_sink_move` action.
   */
  inline scheduler_event_type on_sink_move(
      lock_type&, std::atomic<int>& event) {
    static_cast<Mover*>(this)->on_move(event);
    return scheduler_event_type::noop;
  }

  /**
   * Policy action called on the port `on_notify_source` action.
   */
  inline scheduler_event_type on_notify_source(lock_type&, std::atomic<int>&) {
    return scheduler_event_type::notify_source;
  }

  /**
   * Policy action called on the port `on_notify_sink` action.
   */
  inline scheduler_event_type on_notify_sink(lock_type&, std::atomic<int>&) {
    return scheduler_event_type::notify_sink;
  }

  /**
   * Policy action called on the port `on_source_wait` action.
   */
  inline scheduler_event_type on_source_wait(lock_type&, std::atomic<int>&) {
    // @todo Should wait predicate be checked here?  (It is currently checked in
    // the scheduler body.)
    return scheduler_event_type::source_wait;
  }

  /**
   * Policy action called on the port `on_sink_wait` action.
   */
  inline scheduler_event_type on_sink_wait(lock_type&, std::atomic<int>&) {
    // @todo Should wait predicate be checked here?  (It is currently checked in
    // the scheduler body.)
    // Predicate: source is full?
    return scheduler_event_type::sink_wait;
  }

  /**
   * Policy action called on the port `on_term_source` action.
   */
  inline scheduler_event_type on_term_source(lock_type&, std::atomic<int>&) {
    // throw(duffs_source_exit);
    return scheduler_event_type::source_exit;
  }

  /**
   * Policy action called on the port `on_term_sink` action.
   */
  inline scheduler_event_type on_term_sink(lock_type&, std::atomic<int>&) {
    // @todo There might be a better way of integrating `term_sink` with
    // `term_source`.  For now, `term_sink` just returns.
    // return scheduler_event_type::sink_exit; ?? noop ?? yield ??
    return scheduler_event_type::noop;
  }

 private:
  void debug_msg(const std::string& msg) {
    if (static_cast<Mover*>(this)->debug_enabled()) {
      std::cout << msg << "@" << str(this->state()) << "\n";
    }
  }
};

/**
 * Alias to define the three-stage data mover for the duffs device scheduler.
 */
template <class T>
using DuffsMover3 = ItemMover<DuffsPortPolicy, three_stage, T>;

/**
 * Alias to define the two-stage data mover for the duffs device scheduler.
 */
template <class T>
using DuffsMover2 = ItemMover<DuffsPortPolicy, two_stage, T>;

template <class Task, template <class, class> class Base>
class DuffsSchedulerImpl;

template <class Task, class Policy>
class DuffsSchedulerPolicy;

template <class Node>
using DuffsScheduler = DuffsSchedulerImpl<Task<Node>, DuffsSchedulerPolicy>;

// We'll see if we still need this.
template <class Task, class Scheduler>
struct SchedulerTraits<DuffsSchedulerPolicy<Task, Scheduler>> {
  using task_handle_type = Task;
  using task_type = typename task_handle_type::element_type;
};

/**
 * @brief Defines actions for scheduler state transitions.
 *
 * @tparam Task The type of task to be scheduled.
 */

template <class Task, class Scheduler>
class DuffsSchedulerPolicy
    : public SchedulerStateMachine<DuffsSchedulerPolicy<Task, Scheduler>> {
  // using policy_type = DuffsSchedulerPolicy<Task, Scheduler>;
  // using lock_type = typename state_machine_type::lock_type;
  //  using state_machine_type = SchedulerStateMachine<policy_type>;
  using scheduler_type = Scheduler;

 public:
  using task_type = task_t<Task>;
  using task_handle_type = task_handle_t<Task>;

 private:
  /**
   * @brief A thread pool for use by the state machine.
   * @todo This should be a resouce parameter to the policy, not a member.
   * @todo Use work stealing thread pool.
   */
  class thread_pool {
    scheduler_type* scheduler_;
    // std::atomic<size_t> concurrency_level_{0};
    size_t concurrency_level_{0};

   public:
    /**
     * @brief Very simple static thread pool.  The purpose of this thread pool
     * is to launch the scheduler `worker` tasks, one per thread.
     */
    explicit thread_pool(scheduler_type* sched, size_t n)
        : scheduler_(sched)
        , concurrency_level_(n) {
      // If concurrency_level_ is set to zero, construct the thread pool in
      // shutdown state.  Explicitly shut down the task queue as well.
      if (concurrency_level_ == 0) {
        return;
      }

      // Set an upper limit on number of threads per core.  One use for this is
      // in testing error conditions in creating a context.
      if (concurrency_level_ >= 256 * std::thread::hardware_concurrency()) {
        std::string msg =
            "Error initializing duffs scheduler of concurrency level " +
            std::to_string(concurrency_level_) + "; Requested size too large";
        throw std::runtime_error(msg);
      }

      threads_.reserve(concurrency_level_);

      for (size_t i = 0; i < concurrency_level_; ++i) {
        std::thread tmp;

        // Try to launch a thread running the worker() function. If we get
        // resources_unavailable_try_again error, then try again. Three shall be
        // the maximum number of retries and the maximum number of retries shall
        // be three.
        size_t tries = 3;
        while (tries--) {
          try {
            // @todo switch to using tasks with `std::async` so we can catch
            // exceptions.
            auto tmp_ = std::bind(&scheduler_type::worker, scheduler_, _1, _2);
            threads_.emplace_back(tmp_, i);
          } catch (const std::system_error& e) {
            if (e.code() != std::errc::resource_unavailable_try_again ||
                tries == 0) {
              join_all_threads();
              throw std::runtime_error(
                  "Error initializing thread pool of concurrency level " +
                  std::to_string(concurrency_level_) + "; " + e.what());
            }
            continue;
          }
          break;
        }
      }
    }

    /**
     * @brief Join all of the threads in the thread pool.
     * @todo Use async and futures to be able to handle exceptions.
     */
    void join_all_threads() {
      for (auto&& t : threads_) {
        if (t.joinable()) {
          t.join();
        }
      }
      threads_.clear();
      concurrency_level_ = 0;
    }

    ~thread_pool() {
      join_all_threads();
    }

   private:
    /** The worker threads */
    std::vector<std::jthread> threads_;
  };

  /** Synchronization variables */
  std::mutex mutex_;
  std::condition_variable start_cv_;

  /************************************************************************
   *
   * The following are the actions for the scheduler state transitions.
   *
   ***********************************************************************/
 public:
  /**
   * @brief Initial action for task creation transition.  Moves `task` to the
   * task submission queue.
   *
   * @param task The task to be transitioned.
   */
  void on_create(const task_handle_type& task) {
    this->submission_queue_.push(task);
  }

  /**
   * @brief Action for task submission transition.
   *
   * @param task The task to be transitioned.
   */
  void on_stop_create(const task_handle_type&) {
  }

  /**
   * @brief Action for transitioning a task to the `runnable` state.  Puts the
   * task on the runnable queue.
   *
   * @param task The task to be transitioned.
   */
  void on_make_runnable(const task_handle_type& task) {
    this->global_runnable_queue_.push(task);
  }

  /**
   *  @brief Action for transitioning a task out of the `runnable` state.  Note
   * that this does not remove task from the runnable queue.  Tasks are removed
   * from the runnable queue by the scheduler when they are to be executed.
   *
   * @param task The task to be transitioned.
   */
  void on_stop_runnable(const task_handle_type&) {
  }

  /**
   * @brief Action for transitioning a task to the `running` state.  Puts task
   * into the running set.
   *
   * @param task The task to be transitioned.
   */
  void on_make_running(const task_handle_type& task) {
    this->running_set_.insert(task);
  }

  /**
   * @brief Action for transitioning a task out of the `running` state.  Removes
   * task from the running set.
   *
   * @param task The task to be transitioned.
   */
  void on_stop_running(const task_handle_type& task) {
    auto n = this->running_set_.extract(task);
    assert(!n.empty());
  }

  /**
   * @brief Action for transitioning a task to the `waiting` state.
   *
   * @param task The task to be transitioned.
   */
  void on_make_waiting(const task_handle_type& task) {
    this->waiting_set_.insert(task);
  }

  /**
   * @brief Action for transitioning a task out of the `waiting` state.
   *
   * @param task The task to be transitioned.
   */
  void on_stop_waiting(const task_handle_type& task) {
    auto n = this->waiting_set_.extract(task);
  }

  /**
   * @brief Action for transitioning a task to the `done` state.  Puts task on
   * the finished queue.
   *
   * @param task The task to be transitioned.
   */
  void on_terminate(const task_handle_type& task) {
    this->finished_queue_.push(task);
  }

  /************************************************************************
   *
   * End policy API functions
   *
   ***********************************************************************/

 public:
  explicit DuffsSchedulerPolicy(size_t num_threads)
      : tp_{static_cast<scheduler_type*>(this), num_threads} {
  }

  /**
   * @brief Destructor.
   */
  ~DuffsSchedulerPolicy() {
    shutdown();
  }

 public:
  void block_worker() {
    /*
     * The worker threads wait on a condition variable until they are released
     * by a call to `sync_wait_all` (e.g.)
     */
    std::unique_lock lock(mutex_);
    start_cv_.wait(lock, [this]() { return this->ready_to_run(); });
  }

  /**
   * @brief Gets a task from the runnable queue.  Blocking unless job is
   * finished and queue is shut down.
   */
  auto get_runnable_task() {
    auto val = global_runnable_queue_.pop();
    return val;
  }

  void sync_wait_all() {
    /*
     * Swap the submission queue (where all the submitted tasks are) with the
     * runnable queue, making all the tasks runnable.
     */
    make_submitted_runnable();
    sync_wait_all_no_launch();
  }

  void sync_wait_all_no_launch() {
    this->make_ready_to_run();

    /*
     * Release the worker threads.
     */
    {
      std::unique_lock lock(mutex_);
      start_cv_.notify_all();
    }

    /*
     * Then we wait for the worker threads to complete.
     */
    tp_.join_all_threads();
  }

  /**
   * @brief Terminate threads in the thread pool
   */
  void shutdown() {
    /*
     * Clear out any submitted tasks that haven't been put into the scheduler
     */
    this->finish_queues();
    sync_wait_all_no_launch();
  }

  /**
   * @brief Cleans up the scheduler policy.  This is called when the scheduler
   * is done. All queues are shut down.  All queues and sets should be empty at
   * this point.
   */
  void finish_queues([[maybe_unused]] const std::string& msg = "") {
    waiting_set_.clear();
    global_runnable_queue_.drain();
    for (auto& q : worker_queues_) {
      q.drain();
    }
    running_set_.clear();
    // finished_queue_.drain();
  }
  /* ********************************* */
  /*         PRIVATE FUNCTIONS         */
  /* ********************************* */

 private:
  std::atomic<bool> ready_to_run_{false};

  /**
   * @brief Set the ready_to_run_ flag to true.
   */
  void make_ready_to_run() {
    ready_to_run_.store(true);
  }

  /**
   * @brief Get the values of the ready_to_run_ flag.
   * @returns The value of the ready_to_run_ flag.
   */
  bool ready_to_run() {
    return ready_to_run_.load();
  }

  /**
   * @brief Transitions all tasks from submission queue to runnable queue.
   */
  void make_submitted_runnable() {
    while (!submission_queue_.empty()) {
      auto s = submission_queue_.front();
      submission_queue_.pop();
      if (this->debug_enabled())
        s->dump_task_state("Admitting");
      this->task_admit(s);
    }
  }

  /**
   * @brief Debug helper function.
   */
  void dump_queue_state(const std::string& msg = "") {
    if (this->debug_enabled()) {
      std::string preface = (!msg.empty() ? msg + "\n" : "");

      std::cout << preface + "    global_runnable_queue_.size() = " +
                       std::to_string(global_runnable_queue_.size()) + "\n" +
                       "    running_set_.size() = " +
                       std::to_string(running_set_.size()) + "\n" +
                       "    waiting_set_.size() = " +
                       std::to_string(waiting_set_.size()) + "\n" +
                       "    finished_queue_.size() = " +
                       std::to_string(finished_queue_.size()) + "\n" + "\n";
    }
  }

  /**
   * @brief Debug helper function.
   */
  void debug_msg(const std::string& msg) {
    if (this->debug_enabled()) {
      std::cout << msg + "\n";
    }
  }

 private:
  /**
   * @brief Data structures to hold tasks in various states of execution.
   * Since accesses to these are made under the scheduler lock, we don't need
   * to use thread-safe data structures.
   */
  std::set<Task> waiting_set_;
  std::set<Task> running_set_;
  std::queue<Task> submission_queue_;
  std::queue<Task> finished_queue_;

  /**
   * @brief Queue of runnable tasks.
   *
   * @todo make private
   * @todo Use thread-stealing scheduling
   */
 protected:
  BoundedBufferQ<Task, std::queue<Task>, false> global_runnable_queue_;

  /**
   * @brief Local queues for each worker thread.
   */
 private:
  std::atomic<size_t> counter_{0};
  size_t num_workers_{0};
  std::vector<BoundedBufferQ<Task, std::queue<Task>, false>> worker_queues_;

  thread_pool tp_;
};  // namespace tiledb::common

/**
 * @brief A scheduler that uses a policy to manage tasks.  Task graph nodes are
 * submitted to the scheduler, which wraps them up as tasks.  The tasks are
 * maintain execution state (rather than having nodes do it).  Tasks are what
 * are actually scheduled.
 *
 * @tparam Node The type of the task graph node to be scheduled as a task.
 */

template <class Task, template <class, class> class Base>
class DuffsSchedulerImpl : public Base<Task, DuffsSchedulerImpl<Task, Base>> {
  using Scheduler = DuffsSchedulerImpl<Task, Base>;
  using Policy = Base<Task, DuffsSchedulerImpl<Task, Base>>;

  using task_type = task_t<Task>;
  using task_handle_type = task_handle_t<Task>;

  using node_handle_type = node_handle_t<Task>;
  using node_type = node_t<Task>;

  // using node_handle_type = typename task_type::node_handle_type;
  // using node_type = typename task_type::node_type;

  using Policy::Policy;

 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * @brief Constructor.
   *
   * @param num_threads The number of threads to use for the scheduler.
   */
  explicit DuffsSchedulerImpl(size_t num_threads)
      : Policy(num_threads) {
  }

  /** Deleted default constructor */
  DuffsSchedulerImpl() = delete;

  /** Deleted copy constructor */
  DuffsSchedulerImpl(const DuffsSchedulerImpl&) = delete;

  /** Deleted copy assignment operator */
  DuffsSchedulerImpl(DuffsSchedulerImpl&&) = delete;

  /** Deleted move constructor */
  DuffsSchedulerImpl& operator=(const DuffsSchedulerImpl&) = delete;

  /** Deleted move assignment operator */
  DuffsSchedulerImpl& operator=(DuffsSchedulerImpl&&) = delete;

  /** Destructor. */
  ~DuffsSchedulerImpl() = default;

  /* ********************************* */
  /*         PUBLIC API                */
  /* ********************************* */

 public:
  /**
   * @brief Submit a task graph node to the scheduler.  The task create action
   * is invoked, which results in the wrapped node being put into the
   * submission queue.
   *
   * @param n The task graph node to be submitted.
   */
  void submit(node_handle_type&& n) {
    ++num_submitted_tasks_;
    ++num_tasks_;

    // task_handle_type aa;
    // print_types(n, aa);
    // std::shared_ptr<tiledb::common::node_base>
    // tiledb::common::Task<std::shared_ptr<tiledb::common::node_base>>

    auto t = task_handle_type{std::move(n)};

    node_to_task[n] = t;

    this->task_create(t);
  }

  /**
   * @brief The worker thread routine, which is the body of the scheduler and
   * the main loop of the thread pool (each thread runs this function).
   *
   * The primary operation of the worker thread is to get a task and execute it.
   * Task actions will be invoked in response to port events as used by
   * execution of the `resume` function in the node.
   *
   * Task actions throw exceptions when they are invoked.  The worker function
   * catches these exceptions and reacts accordingly.  Events handled by the
   * scheduler are: wait, notify, and exit.
   *
   * @param id The id of the thread.  These are assigned on thread creation,
   * to be used for debugging purposes.  The id is in the range [0,
   * concurrency_level).
   */
  void worker(std::stop_token stop_token, size_t id = 0) {
    thread_local size_t my_id{id};

    this->block_worker();

    if (num_submitted_tasks_ == 0) {
      return;
    }

    while (!stop_token.stop_requested()) {
      {
        /*
         * If all of our tasks are done, then we are done.
         */
        if (num_exited_tasks_ == num_submitted_tasks_) {
          break;
        }

        /*
         * Get a runnable task.
         * This is a blocking call, unless the queue is finished.
         * We don't want to call this under the lock, because
         * `get_runnable_task` may block, causing deadlock.  So we release lock.
         */
        // lock.unlock();
        auto val = this->get_runnable_task();
        // lock.lock();

        /*
         * An empty `val` means that the queue is finished and that the task
         * graph task is finished.  We can exit the worker thread.
         */
        if (!val) {
          break;
        }

        auto task_to_run = *val;
        auto node = (*(task_to_run->node()));

        std::unique_lock lock(worker_mutex_);

        /*
         * Transition task from runnable to running
         */
        this->task_dispatch(task_to_run);

        if (task_to_run->task_state() != TaskState::running) {
          throw std::runtime_error("Task is not in running state");
        }

      retry:

        /*
         * Invoke the node's `resume` function.
         */
        lock.unlock();
        auto evt = task_to_run->resume();
        lock.lock();
        {
          if (task_to_run->task_state() != TaskState::running) {
            throw std::runtime_error("Task is not in running state");
          }

          switch (evt) {
            case SchedulerAction::source_wait: {
              /*
               * These steps must be atomic to avoid lost wakeup
               * @todo perhaps unify with sink_wait via predicate argument?
               * @todo use actual state instead of is_*?
               */
              if (node->is_source_state_full() && !node->is_source_done()) {
                this->task_wait(task_to_run);
              }
            } break;

            case SchedulerAction::sink_wait: {
              /*
               * These steps must be atomic to avoid lost wakeup
               * @todo perhaps unify with source_wait via predicate argument?
               * @todo use actual state instead of is_*?
               */
              if (node->is_sink_state_empty() && !node->is_sink_done() &&
                  !node->is_sink_terminated()) {
                this->task_wait(task_to_run);
              }
            } break;

            case SchedulerAction::notify_source: {
              auto task_to_notify =
                  node_to_task[task_to_run->source_correspondent()];
              this->task_notify(task_to_notify);
              goto retry;
              break;
            }

            case SchedulerAction::notify_sink: {
              auto task_to_notify =
                  node_to_task[task_to_run->sink_correspondent()];
              this->task_notify(task_to_notify);
              goto retry;
              break;
            }

            case SchedulerAction::source_exit: {
              if (task_to_run->sink_correspondent() != nullptr) {
                auto task_to_notify =
                    node_to_task[task_to_run->sink_correspondent()];

                if (task_to_notify == nullptr) {
                  throw std::runtime_error("task_to_notify is null");
                }
                this->task_notify(task_to_notify);
              }
            }
              [[fallthrough]];

            case SchedulerAction::sink_exit: {
              /*
               * Transition task to finished state.
               */
              this->task_exit(task_to_run);
              --num_tasks_;
              ++num_exited_tasks_;

              if (num_tasks_ + num_exited_tasks_ != num_submitted_tasks_) {
                throw std::runtime_error(
                    "num_tasks_ + num_exited_tasks_ != "
                    "num_submitted_tasks_");
              }

              /*
               * The task graph is finished when all submitted tasks have
               * exited.
               */
              if (num_exited_tasks_ == num_submitted_tasks_) {
                break;
              }
              break;
            }

            case SchedulerAction::yield: {
              // We yield at the end of the loop, so do nothing here.
              // this->task_yield(task_to_run);
              if (this->global_runnable_queue_.size() ==
                  0) {  // @todo Abstraction violation!
                goto retry;
              }
              goto retry;
              break;
            }

            case SchedulerAction::noop: {
              break;
            }

            case SchedulerAction::error: {
              throw std::runtime_error("Error condition in scheduler");
            }

            default: {
              throw std::runtime_error("Unknown event");
            }
          }
          /*
           * Yield this task.
           * @todo Should this be done here or at the end of a task?
           */
          { this->task_yield(task_to_run); }
        }
      }  // End lock
    }    // end while(true);

    /*
     * Shutdown the queues, which will release any threads waiting on the
     * runnable queue.
     */
    this->finish_queues(std::to_string(my_id));
  }  // worker()

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

 private:
  /**
   * @brief A map to convert node ids to tasks.
   */
  std::map<node_handle_type, task_handle_type> node_to_task;

  /** Track number of tasks submitted to scheduler */
  std::atomic<size_t> num_submitted_tasks_{0};

  /** Track number of tasks in the scheduler */
  std::atomic<size_t> num_tasks_{0};

  /** Track number of tasks that have exited the scheduler */
  std::atomic<size_t> num_exited_tasks_{0};

  /** Synchronization variables */
  std::mutex worker_mutex_;
};

}  // namespace tiledb::common

#endif  // TILEDB_DAG_duffs_H
