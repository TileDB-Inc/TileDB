/**
 * @file   random.h
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
 * This file declares a random (static threadpool) scheduler for dag.
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
 * Tasks are maintained on a "runnable" vector.  Tasks are executed in order
 * from the vector.  When the last task in the vector is executed, the vector
 * is shuffled and the process repeats.  This is a simple way to ensure that
 * tasks are executed fairly, but in a random order.
 *
 * @todo Factor scheduler, task, and policy so they are more orthogonal and
 * can be mixed and matched.
 */

#ifndef TILEDB_DAG_RANDOM_H
#define TILEDB_DAG_RANDOM_H

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
#include "experimental/tiledb/common/dag/execution/jthread/jthread.hpp"
#include "experimental/tiledb/common/dag/execution/task.h"
#include "experimental/tiledb/common/dag/execution/task_state_machine.h"
#include "experimental/tiledb/common/dag/state_machine/fsm.h"
#include "experimental/tiledb/common/dag/state_machine/item_mover.h"
#include "experimental/tiledb/common/dag/utility/print_types.h"

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
class RandomPortPolicy : public PortFiniteStateMachine<
                            RandomPortPolicy<Mover, PortState>,
                            PortState> {
  using state_machine_type =
      PortFiniteStateMachine<RandomPortPolicy<Mover, PortState>, PortState>;
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
  RandomPortPolicy() {
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

};

/**
 * Alias to define the three-stage data mover for the random scheduler.
 */
template <class T>
using RandomMover3 = ItemMover<RandomPortPolicy, three_stage, T>;

/**
 * Alias to define the two-stage data mover for the random scheduler.
 */
template <class T>
using RandomMover2 = ItemMover<RandomPortPolicy, two_stage, T>;

template <class Task, template <class, class> class Base>
class RandomSchedulerImpl;

template <class Task, class Policy>
class RandomSchedulerPolicy;

template <class Node>
using RandomScheduler = RandomSchedulerImpl<Task<Node>, RandomSchedulerPolicy>;

// We'll see if we still need this.
template <class Task, class Scheduler>
struct SchedulerTraits<RandomSchedulerPolicy<Task, Scheduler>> {
  using task_handle_type = Task;
  using task_type = typename task_handle_type::element_type;
};


/**
 * @brief Defines actions for scheduler state transitions.
 *
 * @tparam Task The type of task to be scheduled.
 */
template <class Task, class Scheduler>
class RandomSchedulerPolicy {
  using scheduler_type = Scheduler;

 public:
  using task_type = task_t<Task>;
  using task_handle_type = task_handle_t<Task>;

 private:

  class thread_pool {
    scheduler_type* scheduler_;
    size_t concurrency_level_{0};

   public:

    explicit thread_pool(scheduler_type* sched, size_t n)
        : scheduler_(sched)
        , concurrency_level_(n) {
      if (concurrency_level_ == 0) {
        return;
      }

      if (concurrency_level_ >= 256 * std::thread::hardware_concurrency()) {
        std::string msg =
            "Error initializing random scheduler of concurrency level " +
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

 public:
  explicit RandomSchedulerPolicy(size_t num_threads)
      : tp_{static_cast<scheduler_type*>(this), num_threads} {
  }

  ~RandomSchedulerPolicy() = default;

  void block_worker() {
    std::unique_lock lock(mutex_);
    start_cv_.wait(lock, [this]() { return this->ready_to_run(); });
  }

  auto get_runnable_task() {
    if (runnable_vector_.empty()) {
      // Returning an empty optional indicates that there are no tasks to run,
      // and the worker thread should exit.
      return std::optional<task_handle_type>{};
    }

    static std::atomic<size_t> next_task {0};
    size_t idx = next_task++ % runnable_vector_.size();

    static std::mutex m;
    if (idx == 0) {
      std::lock_guard lock(m);
      shuffle_runnable_vector();
    }

    auto val = runnable_vector_[idx];

    return std::optional<task_handle_type>{val};
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
     std::lock_guard lock(mutex_);
    runnable_vector_.clear();
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
   * @brief Transitions all tasks from submission queue to runnable vector.
   */
  void make_submitted_runnable() {

    assert(runnable_vector_.size() == 0);

    runnable_vector_.clear();
    runnable_vector_.reserve(submission_queue_.size());
    while (!submission_queue_.empty()) {
      runnable_vector_.push_back(submission_queue_.front());
      submission_queue_.pop();
    }
    shuffle_runnable_vector();
  }


  void shuffle_runnable_vector() {
    std::shuffle(std::begin(runnable_vector_), std::end(runnable_vector_),
                 std::mt19937{std::random_device{}()});
  }


   private:
   protected:
    /**
   * @brief Data structures to hold tasks in various states of execution.
   * Since accesses to these are made under the scheduler lock, we don't need
   * to use thread-safe data structures.
   */
  // std::set<Task> waiting_set_;
  // std::set<Task> running_set_;
  std::queue<Task> submission_queue_;
  // std::queue<Task> finished_queue_;

  /**
   * @brief Vector of runnable tasks.
   *
   * @todo make private
   * @todo Use thread-stealing scheduling
   */

  // BoundedBufferQ<Task, std::queue<Task>, false> global_runnable_queue_;
  std::vector<Task> runnable_vector_;

  /**
   * @brief Local queues for each worker thread.
   */
 private:
  std::atomic<size_t> counter_{0};
  size_t num_workers_{0};
  // std::vector<BoundedBufferQ<Task, std::queue<Task>, false>> worker_queues_;

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
class RandomSchedulerImpl : public Base<Task, RandomSchedulerImpl<Task, Base>> {
  using Scheduler = RandomSchedulerImpl<Task, Base>;
  using Policy = Base<Task, RandomSchedulerImpl<Task, Base>>;

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
  explicit RandomSchedulerImpl(size_t num_threads)
      : Policy(num_threads) {
  }

  /** Deleted default constructor */
  RandomSchedulerImpl() = delete;

  /** Deleted copy constructor */
  RandomSchedulerImpl(const RandomSchedulerImpl&) = delete;

  /** Deleted copy assignment operator */
  RandomSchedulerImpl(RandomSchedulerImpl&&) = delete;

  /** Deleted move constructor */
  RandomSchedulerImpl& operator=(const RandomSchedulerImpl&) = delete;

  /** Deleted move assignment operator */
  RandomSchedulerImpl& operator=(RandomSchedulerImpl&&) = delete;

  /** Destructor. */
  ~RandomSchedulerImpl() //= default;
  {
    this->shutdown();
  }

  /* ********************************* */
  /*         PUBLIC API                */
  /* ********************************* */

 public:
  void submit(node_handle_type&& n) {
    ++num_submitted_tasks_;
    ++num_tasks_;

    auto t = task_handle_type{std::move(n)};
    node_to_task[n] = t;
    this->submission_queue_.push(t);
  }

  void worker(std::stop_token stop_token, size_t id = 0) {
    thread_local size_t my_id{id};

    this->block_worker();

    if (num_submitted_tasks_ == 0) {
      return;
    }

    while (!stop_token.stop_requested()) {
      {

          std::unique_lock lock(worker_mutex_);


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



        if (!val) {
          break;
        }
        auto task_to_run = *val;
        auto node = (*(task_to_run->node()));


      // retry:

        /*
         * Invoke the node's `resume` function.
         */
        lock.unlock();
        [[ maybe_unused ]] auto evt = task_to_run->resume();
        lock.lock();
        if (evt == SchedulerAction::done) {
          ++num_exited_tasks_;
          task_to_run->task_state() = TaskState::terminated;
        }
      }
    }

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

#endif  // TILEDB_DAG_RANDOM_H
