/**
 * @file   throw_catch.h
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
 * This file declares a throw_catch (static threadpool) scheduler for dag.
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
 * The throw_catch scheduler introduces some challenges for the port state
 * machine in particular. Since calls to notify and wait don't return, we can't
 * invoke the two together in response to the same event.  Thus, we need to
 * decrement the program counter for a waiting task rather than letting the
 * event handler do the retry.
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

#ifndef TILEDB_DAG_THROW_CATCH_H
#define TILEDB_DAG_THROW_CATCH_H

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
#include "experimental/tiledb/common/dag/execution/task.h"
#include "experimental/tiledb/common/dag/execution/task_state_machine.h"
#include "experimental/tiledb/common/dag/execution/throw_catch_types.h"
#include "experimental/tiledb/common/dag/state_machine/fsm.h"
#include "experimental/tiledb/common/dag/state_machine/item_mover.h"
#include "experimental/tiledb/common/dag/utility/bounded_buffer.h"
#include "experimental/tiledb/common/dag/utility/concurrent_map.h"
#include "experimental/tiledb/common/dag/utility/concurrent_set.h"
#include "experimental/tiledb/common/dag/utility/print_types.h"

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
class ThrowCatchPortPolicy : public PortFiniteStateMachine<
                                 ThrowCatchPortPolicy<Mover, PortState>,
                                 PortState> {
  using state_machine_type =
      PortFiniteStateMachine<ThrowCatchPortPolicy<Mover, PortState>, PortState>;
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
  ThrowCatchPortPolicy() {
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
    throw(throw_catch_notify_source);
    return scheduler_event_type::notify_source;
  }

  /**
   * Policy action called on the port `on_notify_sink` action.
   */
  inline scheduler_event_type on_notify_sink(lock_type&, std::atomic<int>&) {
    throw(throw_catch_notify_sink);
    return scheduler_event_type::notify_sink;
  }

  /**
   * Policy action called on the port `on_source_wait` action.
   */
  inline scheduler_event_type on_source_wait(lock_type&, std::atomic<int>&) {
    // @todo Should wait predicate be checked here?  (It is currently checked in
    // the scheduler body.)
    throw(throw_catch_source_wait);
    return scheduler_event_type::source_wait;
  }

  /**
   * Policy action called on the port `on_sink_wait` action.
   */
  inline scheduler_event_type on_sink_wait(lock_type&, std::atomic<int>&) {
    // @todo Should wait predicate be checked here?  (It is currently checked in
    // the scheduler body.)
    throw(throw_catch_sink_wait);  // Predicate: source is full?
    return scheduler_event_type::sink_wait;
  }

  /**
   * Policy action called on the port `on_term_source` action.
   */
  inline scheduler_event_type on_term_source(lock_type&, std::atomic<int>&) {
    throw(throw_catch_source_exit);
    return scheduler_event_type::source_exit;
  }

  /**
   * Policy action called on the port `on_term_sink` action.
   */
  inline scheduler_event_type on_term_sink(lock_type&, std::atomic<int>&) {
    // @todo There might be a better way of integrating `term_sink` with
    // `term_source`.  For now, `term_sink` just returns.
    // throw(throw_catch_sink_exit);
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
 * Alias to define the three-stage data mover for the throw-catch scheduler.
 */
template <class T>
using ThrowCatchMover3 = ItemMover<ThrowCatchPortPolicy, three_stage, T>;

/**
 * Alias to define the two-stage data mover for the throw-catch scheduler.
 */
template <class T>
using ThrowCatchMover2 = ItemMover<ThrowCatchPortPolicy, two_stage, T>;

/*
 * The following types trait mechanism is necessary to get the
 * `ThrowCatchSchedulerPolicy` to compile.
 */
template <class Node>
class ThrowCatchScheduler;

template <class Task>
class ThrowCatchSchedulerPolicy;

template <typename T>
struct SchedulerTraits<ThrowCatchSchedulerPolicy<T>> {
  using task_type = T;
  using task_handle_type = T;
};

/**
 * @brief Defines actions for scheduler state transitions.
 *
 * @tparam Task The type of task to be scheduled.
 */
template <class Task>
class ThrowCatchSchedulerPolicy
    : public SchedulerStateMachine<ThrowCatchSchedulerPolicy<Task>> {
  using state_machine_type =
      SchedulerStateMachine<ThrowCatchSchedulerPolicy<Task>>;
  using lock_type = typename state_machine_type::lock_type;

 public:
  using task_type =
      typename SchedulerTraits<ThrowCatchSchedulerPolicy<Task>>::task_type;
  using task_handle_type = typename SchedulerTraits<
      ThrowCatchSchedulerPolicy<Task>>::task_handle_type;

  /**
   * @brief Destructor.
   */
  ~ThrowCatchSchedulerPolicy() {
    done();
  }

  /**
   * @brief Initial action for task creation transition.  Moves `task` to the
   * task submission queue.
   *
   * @param task The task to be transitioned.
   */
  void on_create(task_handle_type& task) {
    debug_msg("calling on_create");
    this->submission_queue_.push(task);
  }

  /**
   * @brief Action for task submission transition.
   *
   * @param task The task to be transitioned.
   */
  void on_stop_create(task_handle_type&) {
  }

  /**
   * @brief Action for transitioning a task to the `runnable` state.  Puts the
   * task on the runnable queue.
   *
   * @param task The task to be transitioned.
   */
  void on_make_runnable(task_handle_type& task) {
    debug_msg("calling on_make_runnable");
    this->runnable_queue_.push(task);
  }

  /**
   *  @brief Action for transitioning a task out of the `runnable` state.  Note
   * that this does not remove task from the runnable queue.  Tasks are removed
   * from the runnable queue by the scheduler when they are to be executed.
   *
   * @param task The task to be transitioned.
   */
  void on_stop_runnable(task_handle_type&) {
  }

  /**
   * @brief Action for transitioning a task to the `running` state.  Puts task
   * into the running set.
   *
   * @param task The task to be transitioned.
   */
  void on_make_running(task_handle_type& task) {
    this->running_set_.insert(task);
  }

  /**
   * @brief Action for transitioning a task out of the `running` state.  Removes
   * task from the running set.
   *
   * @param task The task to be transitioned.
   */
  void on_stop_running(task_handle_type& task) {
    auto n = this->running_set_.extract(task);

    assert(!n.empty());
  }

  /**
   * @brief Action for transitioning a task to the `waiting` state.
   * Note that a task in the waiting state must have its program counter
   * decremented so that when it resumes it will resume before the action that
   * caused it to wait. (This similar in behavior to a cv wait.) Currently the
   * decrementing is done in the scheduler body. But we might want to move it
   * here.
   *
   * @param task The task to be transitioned.
   */
  void on_make_waiting(task_handle_type& task) {
    // @todo: try decrementing here?
    auto node = (*(task->node()));
    node->decrement_program_counter();
    this->waiting_set_.insert(task);
  }

  /**
   * @brief Action for transitioning a task out of the `waiting` state.  Removes
   * task from the waiting set. As described in `on_make_waiting`, the program
   * counter must be decremented. This is another possible location for doing
   * the decrement.
   *
   * @param task The task to be transitioned.
   */
  void on_stop_waiting(task_handle_type& task) {
    auto n = this->waiting_set_.extract(task);
    // @todo: Should this never be empty?
    // if (n.empty()) {
    //   throw std::runtime_error("on_stop_waiting: task not in waiting set");
    // }

    // @todo: try decrementing program counter here?
  }

  /**
   * @brief Action for transitioning a task to the `done` state.  Puts task on
   * the finished queue.
   *
   * @param task The task to be transitioned.
   */
  void on_terminate(task_handle_type& task) {
    this->finished_queue_.push(task);
  }

  /**
   * @brief Transitions all tasks from submission queue to runnable queue.
   */
  void launch() {
    while (true) {
      auto s = submission_queue_.try_pop();
      if (!s)
        break;
      if (this->debug_enabled())
        (*s)->dump_task_state("Admitting");

      this->task_admit(*s);
    }
  }

  /**
   * @brief Gets a task from the runnable queue.  Blocking unless job is
   * finished and queue is shut down.
   */
  auto get_runnable_task() {
    auto val = runnable_queue_.pop();
    return val;
  }

  /**
   * @brief Cleans up the scheduler policy.  This is called when the scheduler
   * is done. All queues are shut down.  All queues and sets should be empty at
   * this point.
   */
  void done([[maybe_unused]] const std::string& msg = "") {
    waiting_set_.clear();
    runnable_queue_.drain();
    running_set_.clear();
    finished_queue_.drain();
  }

  /**
   * @brief Debug helper function.
   */
  void dump_queue_state(const std::string& msg = "") {
    if (this->debug_enabled()) {
      std::string preface = (!msg.empty() ? msg + "\n" : "");

      std::cout << preface + "    runnable_queue_.size() = " +
                       std::to_string(runnable_queue_.size()) + "\n" +
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
   */
  ConcurrentSet<Task> waiting_set_;
  ConcurrentSet<Task> running_set_;
  BoundedBufferQ<Task, std::queue<Task>, false> submission_queue_;
  BoundedBufferQ<Task, std::queue<Task>, false> runnable_queue_;
  BoundedBufferQ<Task, std::queue<Task>, false> finished_queue_;
};

/**
 * @brief A scheduler that uses a policy to manage tasks.  Task graph nodes are
 * submitted to the scheduler, which wraps them up as tasks.  The tasks are
 * maintain execution state (rather than having nodes do it).  Tasks are what
 * are actually scheduled.
 *
 * @tparam Node The type of the task graph node to be scheduled as a task.
 */
template <class Node>
class ThrowCatchScheduler : public ThrowCatchSchedulerPolicy<Task<Node>> {
  using Scheduler = ThrowCatchScheduler<Task<Node>>;
  using Policy = ThrowCatchSchedulerPolicy<Task<Node>>;

  using task_type = Task<Node>;
  using task_handle_type = typename task_type::task_handle_type;
  using node_handle_type = typename task_type::node_handle_type;
  using node_type =
      typename node_handle_type::element_type;  // @todo abstraction violation?

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
  explicit ThrowCatchScheduler(size_t n)
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
          "Error initializing throw_catch scheduler of concurrency level " +
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
          tmp = std::thread(&ThrowCatchScheduler::worker, this, i);
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
  ThrowCatchScheduler() = delete;

  /** Deleted copy constructor */
  ThrowCatchScheduler(const ThrowCatchScheduler&) = delete;

  /** Destructor. */
  ~ThrowCatchScheduler() {
    shutdown();
  }

  /* ********************************* */
  /*                API                */
  /* ********************************* */

 public:
  /**
   * @brief Get the concurrency level (number of threads in the thread pool) of
   * the scheduler.
   *
   * @returns The concurrency level of the scheduler.
   */
  size_t concurrency_level() {
    return concurrency_level_;
  }

 private:
  /**
   * @brief A map to convert node ids to tasks.
   */
  std::map<node_handle_type, task_handle_type> node_to_task;

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

    auto t = task_handle_type{std::move(n)};

    node_to_task[n] = t;

    this->task_create(t);
  }

  /**
   * Wait on all the given tasks to complete. Since tasks are started lazily,
   * they are not actually started on submit().  So, we first start all the
   * submitted jobs and then wait on them to all be started before
   * transferring them from the submitted queue to the runnable queue.
   */
  void sync_wait_all() {
    /*
     * Swap the submission queue (where all the submitted tasks are) with the
     * runnable queue, making all the tasks runnable.
     */
    Policy::launch();

    this->make_ready_to_run();

    /*
     * Once we have put all tasks into the runnable queue, we release the
     * worker threads.
     */
    {
      std::unique_lock lock(mutex_);
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
  void worker(size_t id = 0) {
    thread_local size_t my_id{id};

    /*
     * The worker threads wait on a condition variable until they are released
     * by a call to `sync_wait_all` (e.g.)
     */
    {
      std::unique_lock lock(mutex_);
      start_cv_.wait(lock, [this]() { return this->ready_to_run(); });
    }

    if (num_submitted_tasks_ == 0) {
      return;
    }

    while (true) {
      {
        /*
         * Lock the scheduler mutex.
         * @todo We might be able to make this more fine grained.
         */
        std::unique_lock lock(mutex_);

        /*
         * If all of our tasks are done, then we are done.
         */
        if (num_exited_tasks_ == num_submitted_tasks_) {
          this->done(std::to_string(my_id));
          break;
        }

        /*
         * Get a runnable task.
         * This is a blocking call, unless the queue is finished.
         * We don't want to call this under the lock, because
         * `get_runnable_task` may block, causing deadlock.  So we release lock.
         */
        lock.unlock();
        auto val = this->get_runnable_task();
        lock.lock();

        /*
         * An empty `val` means that the queue is finished and that the task
         * graph task is finished.  We can exit the worker thread.
         */
        if (!val) {
          break;
        }

        auto task_to_run = *val;
        auto node = (*(task_to_run->node()));

        // @todo Do we need to do this?
        // if (task_to_run->task_state() == TaskState::terminated) {
        //   this->task_exit(task_to_run);
        // }

        /*
         * Transition task from runnable to running
         */
        this->task_dispatch(task_to_run);

        /*
         * Invoke the node's `resume` function.  This is done in a try-catch.
         * Events invoked by the node are caught and handled by the scheduler.
         */
        try {
          lock.unlock();
          task_to_run->resume();
          lock.lock();
        }
        /*
         * Handle the `wait` event.  The scheduler transitions the task to the
         * waiting state (and puts it on the waiting queue).
         */
        catch (const detail::throw_catch_wait& w) {
          lock.lock();

          if (!(w.target() == detail::throw_catch_target::sink ||
                w.target() == detail::throw_catch_target::source)) {
            throw std::runtime_error("Unknown throw catch target");
          }

          /*
           * Check predicates for the wait event, in order to avoid lost
           * wakeups.
           */
          if (((w.target() == detail::throw_catch_target::sink) &&
               // !is_sink_state_full
               node->is_sink_state_empty() && !node->is_sink_done() &&
               !node->is_sink_terminated()) ||
              ((w.target() == detail::throw_catch_target::source) &&
               // ! is_source_state_empty
               node->is_source_state_full() &&
               !node->is_source_done() /*&& !node->is_source_terminated()*/)) {
            this->task_wait(task_to_run);
          } else {
            node->decrement_program_counter();
          }
        }
        /*
         * Handle the `notify` event.  A notification is invoked on the
         * corresponding task in the task graph (where a corresponding task is
         * the one connected to the current task via an edge).
         */
        catch (const detail::throw_catch_notify& n) {
          lock.lock();

          assert(
              n.target() == detail::throw_catch_target::sink ||
              n.target() == detail::throw_catch_target::source);

          /*
           * @note Notification goes to correspondent task of task_to_run
           *  @todo This isn't being done under a lock -- race condition?
           */
          if (n.target() == detail::throw_catch_target::sink) {
            auto task_to_notify =
                node_to_task[task_to_run->sink_correspondent()];

            this->task_notify(task_to_notify);
          } else {
            auto task_to_notify =
                node_to_task[task_to_run->source_correspondent()];
            this->task_notify(task_to_notify);
          }
        }

        /*
         * Handle the `exit` event.  The scheduler transitions the task to the
         * finished state. If a source is exiting (due to a `term_source` event,
         * the corresponding sink is notified).
         */
        catch (const detail::throw_catch_exit& ex) {
          lock.lock();

          /*
           * Term source needs to notify sink of exit
           */
          if (ex.target() == detail::throw_catch_target::source) {
            if (task_to_run->sink_correspondent() != nullptr) {
              auto task_to_notify =
                  node_to_task[task_to_run->sink_correspondent()];

              if (task_to_notify == nullptr) {
                throw std::runtime_error("task_to_notify is null");
              }
              this->task_notify(task_to_notify);
            }
          }

          /*
           * Transition task to finished state.
           */
          this->task_exit(task_to_run);
          --num_tasks_;
          ++num_exited_tasks_;

          if (num_tasks_ + num_exited_tasks_ != num_submitted_tasks_) {
            throw std::runtime_error("Impossible task numbers");
          }

          /*
           * The task graph is finished when all submitted tasks have exited.
           */
          if (num_exited_tasks_ == num_submitted_tasks_) {
            this->done(std::to_string(my_id));
            break;
          }

          /* Slight optimization to skip call to yield when exiting */
          continue;

        }

        /*
         * Catch anything else.  Any other exception is a bug.
         */
        catch (const std::exception& e) {
          std::cout << e.what() << std::endl;
          throw;
        }

        /*
         * @todo Perhaps deal with everything in yield?  Or dispatch?  Or both?
         */
        this->task_yield(task_to_run);

        /*
         * @todo Is this check necessary here?
         */
        if (num_exited_tasks_ == num_submitted_tasks_) {
          this->done(std::to_string(my_id));
          break;
        }
      }  // End lock
    }    // end while(true);
  }      // worker()

  /**
   * @brief Terminate threads in the thread pool
   */
  void shutdown() {
    /*
     * Clear out any submitted tasks that haven't been put into the scheduler
     */
    this->make_ready_to_run();
    start_cv_.notify_all();

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

  /** Track number of tasks submitted to scheduler */
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

#endif  // TILEDB_DAG_THROW_CATCH_H
