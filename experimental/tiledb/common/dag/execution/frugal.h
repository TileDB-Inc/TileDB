/**
 * @file   frugal.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * This file declares a throw-catch scheduler for dag.
 */

#ifndef TILEDB_DAG_FRUGAL_H
#define TILEDB_DAG_FRUGAL_H

#include <condition_variable>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include "experimental/tiledb/common/dag/utils/bounded_buffer.h"
#include "experimental/tiledb/common/dag/utils/concurrent_set.h"
#include "experimental/tiledb/common/dag/utils/print_types.h"

namespace tiledb::common {

/*
 * Below we just use standard OS scheduling terminology.  For our purposes a
 * "Task" is a node.
 */

enum class TaskState { created, ready, running, waiting, terminated, last };

constexpr unsigned short to_index(TaskState x) {
  return static_cast<unsigned short>(x);
}

namespace {
std::vector<std::string> task_state_strings{
    "created", "ready", "running", "waiting", "terminated", "last"};
}

static inline auto str(TaskState st) {
  return task_state_strings[to_index(st)];
}

enum class TaskEvent { admitted, dispatch, wait, notify, exit, yield, last };

constexpr unsigned short to_index(TaskEvent x) {
  return static_cast<unsigned short>(x);
}

namespace {
std::vector<std::string> task_event_strings{
    "admitted", "dispatch", "wait", "notify", "exit", "yield", "last"};
}
static inline auto str(TaskEvent st) {
  return task_event_strings[to_index(st)];
}

template <class Task>
class SchedulerBase {
  virtual void submit(Task*) = 0;
  virtual void wait(Task*) = 0;
  virtual void notify(Task*) = 0;
  virtual void yield(Task*) = 0;
};

template <class Task>
class ThrowCatchScheduler /* : public SchedulerBase<Task> */ {
  using Scheduler = ThrowCatchScheduler<Task>;

 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /**
   * Constructor.
   *
   * @param n The number of threads to be spawned for the thread pool.  This
   * should be a value between 1 and 256 * hardware_concurrency.  A value of
   * zero will construct the thread pool in its shutdown state--constructed but
   * not accepting nor executing any tasks.  A value of 256*hardware_concurrency
   * or larger is an error.
   */
  ThrowCatchScheduler(size_t n)
      : concurrency_level_(n) {
    // If concurrency_level_ is set to zero, construct the thread pool in
    // shutdown state.  Explicitly shut down the task queue as well.
    if (concurrency_level_ == 0) {
      waiting_set_.clear();
      runnable_queue_.drain();
      running_set_.clear();
      finished_queue_.drain();
      return;
    }

    // Set an upper limit on number of threads per core.  One use for this is in
    // testing error conditions in creating a context.
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
          tmp = std::thread(&ThrowCatchScheduler::worker, this);
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

  /** Destructor. */
  ~ThrowCatchScheduler() {
    shutdown();
  }

  /* ********************************* */
  /*                API                */
  /* ********************************* */

 public:
  size_t concurrency_level() {
    return concurrency_level_;
  }

#if 0
  /**
   * Schedule a new task to be executed. If the returned future object
   * is valid, `f` is execute asynchronously. To avoid deadlock, `f`
   * should not acquire non-recursive locks held by the calling thread.
   *
   * @param f Callable object to call
   * @param args... Parameters to pass to f
   * @return std::future referring to the shared state created by this call
   */

  template <class Fn, class... Args>
  auto async(Fn&& f, Args&&... args) {
    if (concurrency_level_ == 0) {
      Task invalid_future;
      return invalid_future;
    }

    using R = std::invoke_result_t<std::decay_t<Fn>, std::decay_t<Args>...>;

    auto task = std::make_shared<std::packaged_task<R()>>(
        [f = std::forward<Fn>(f),
         args = std::make_tuple(std::forward<Args>(args)...)]() mutable {
          return std::apply(std::move(f), std::move(args));
        });

    std::future<R> future = task->get_future();

    runnable_queue_.push(task);

    return future;
  }

  /**
   * Alias for async()
   *
   * @param f Callable object to call
   * @param args... Parameters to pass to f
   * @return std::future referring to the shared state created by this call
   */
  template <class Fn, class... Args>
  auto execute(Fn&& f, Args&&... args) {
    return async(std::forward<Fn>(f), std::forward<Args>(args)...);
  }
#endif

  void wait(Task*) {
  }
  void notify(Task*) {
  }
  void yield(Task*) {
  }

  void submit(Task* t) {
    t->set_node_state(TaskState::created);

    if (debug_)
      std::cout << "Submitting " << t->name() << " node " << t->id()
                << " with correspondent_ " << t->correspondent_->name()
                << " node "
                << " node " << t->correspondent_->id() << std::endl;

    ++num_submissions_;
    ++num_tasks_;
    submission_queue_.push(t);
  }

  /**
   * Wait on all the given tasks to complete. Since tasks are started lazily,
   * they are not actually started on submit().  So, we first start all the
   * submitted jobs and then wait on them.
   *
   * @param tasks Task list to wait on.
   * @return Status::Ok if all tasks returned Status::Ok, otherwise the first
   * error status is returned
   */
  void sync_wait_all() {
    submission_queue_.swap_data(runnable_queue_);

    start_cv_.notify_all();

    for (auto&& t : threads_) {
      t.join();
    }
    threads_.clear();
  }

  void enable_debug() {
    debug_ = true;
  }

  void disable_debug() {
    debug_ = false;
  }

  bool debug() {
    return debug_;
  }

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

 private:
  /** The worker thread routine */
  void worker() {
    //    thread_local static Scheduler* scheduler = this;
    [[maybe_unused]] thread_local static auto scheduler = this;

    if (num_submissions_ == 0) {
      std::cout << "No submissions, returning" << std::endl;
      return;
    }

    {
      std::unique_lock _(mutex_);
      start_cv_.wait(_, [this]() { return runnable_queue_.size() != 0; });
    }

    while (true) {
      // Make any waiting tasks runnable
      // @todo this doesn't seem very efficient, but we
      // need to avoid doing a throw for every notify?
      // nodes should maybe have more access to scheduler itself

      if (waiting_set_.size() == 0 && runnable_queue_.size() == 0 &&
          running_set_.size() == 0) {
        if (debug_)
          std::cout << "breaking with waiting_set_.size() = "
                    << waiting_set_.size()
                    << ", runnable_queue_.size() == " << runnable_queue_.size()
                    << ", running_set_.size() == " << running_set_.size()
                    << ", finished_queue_.size() == " << finished_queue_.size()
                    << std::endl;

        break;
      }

      std::vector<Task*> notified_waiters_;
      notified_waiters_.reserve(waiting_set_.size());
      for (auto&& t : waiting_set_) {
        if (debug_)
          std::cout << "Waiter found " << t->name() << " node " << t->id()
                    << " with state " << str(t->node_state()) << " and event "
                    << str(t->task_event()) << std::endl;

        if (t->task_event() == TaskEvent::notify) {
          notified_waiters_.push_back(t);
        }
      }

      for (auto&& t : notified_waiters_) {
        t->set_node_state(TaskState::ready);
        waiting_set_.erase(t);
        runnable_queue_.push(t);
      }
      notified_waiters_.clear();

      auto val = runnable_queue_.try_pop();

      if (!val) {
        if (debug_)
          std::cout << "Nothing found in runnable_queue_, but " << std::endl;
        if (debug_)
          std::cout << "waiting_set_.size() == " << waiting_set_.size()
                    << ", runnable_queue_.size() == " << runnable_queue_.size()
                    << ", running_set_.size() == " << running_set_.size()
                    << ", finished_queue_.size() == " << finished_queue_.size()
                    << std::endl;
        continue;
      }

      if (val) {
        auto task_to_run = *val;

        if (debug_)
          std::cout << "Worker popped " << task_to_run->name() << " node "
                    << task_to_run->id() << " from runnable_queue_"
                    << std::endl;

        if (task_to_run->node_state() == TaskState::waiting) {
          if (debug_)
            std::cout << "Worker found " << task_to_run->name() << " node "
                      << task_to_run->id() << " with waiting state"
                      << std::endl;
          waiting_set_.insert(task_to_run);

          // Don't have time to figure out nesting of ifs for big blocks
          goto if_val_bottom;
        }

        running_set_.insert(task_to_run);

        try {
          task_to_run->resume();

        } catch (Task* t) {
          task_to_run = t;

        } catch (...) {
          throw;
        }

        switch (task_to_run->task_event()) {
          case TaskEvent::yield: {
            auto n = running_set_.extract(task_to_run);
            n.value()->set_node_state(TaskState::ready);
            runnable_queue_.push(n.value());
            break;
          }

          case TaskEvent::wait: {
            auto n = running_set_.extract(task_to_run);

            if (n) {
              if (n.value()->task_event() == TaskEvent::notify) {
                n.value()->set_node_state(TaskState::ready);
                runnable_queue_.push(n.value());
                break;
              }
              n.value()->set_node_state(TaskState::waiting);
              if (debug_)
                std::cout << n.value()->name() << " node " << n.value()->id()
                          << " found in running with "
                          << str(n.value()->node_state()) << " and "
                          << str(n.value()->task_event()) << std::endl;
              assert(n.value()->node_state() == TaskState::waiting);
            } else {
              if (debug_)
                std::cout << "n not found in running set -- n has no value"
                          << std::endl;
              break;
            }

            assert(n);

            if (debug_)
              std::cout << "Putting " << n.value()->name() << " node "
                        << n.value()->id() << " onto waiting_set_" << std::endl;
            waiting_set_.insert(n.value());
            break;
          }
          case TaskEvent::notify: {
            auto n = waiting_set_.extract(task_to_run);
            n.value()->set_node_state(TaskState::ready);
            runnable_queue_.push(n.value());
            break;
          }
          case TaskEvent::exit: {
            auto n = running_set_.extract(task_to_run);

            if (debug_)
              std::cout << "Putting " << n.value()->name() << " node "
                        << n.value()->id() << " finished_queue_" << std::endl;

            finished_queue_.push(n.value());

            if (debug_)
              std::cout << "waiting_set_.size() == " << waiting_set_.size()
                        << ", runnable_queue_.size() == "
                        << runnable_queue_.size()
                        << ", running_set_.size() == " << running_set_.size()
                        << ", finished_queue_.size() == "
                        << finished_queue_.size() << std::endl;

            break;
          }
          default: {
          }
        }  // switch
        if (debug_)
          std::cout << "Finished switch" << std::endl;

      if_val_bottom:;
      } else {
        break;
      }  // if (val)
      if (debug_)
        std::cout << "Finished if (val)" << std::endl;

      if (!running_set_.empty()) {
        if (debug_)
          std::cout << "Running_set_ not empty, continuing" << std::endl;
        continue;
      }
      if (!waiting_set_.empty()) {
        if (debug_)
          std::cout << "Waiting_set_ not empty, continuing" << std::endl;
        continue;
      }

    }  // while(true);
    if (debug_)
      std::cout << "Finished while (true)" << std::endl;

  }  // worker()

  /** Terminate threads in the thread pool */
  void shutdown() {
    size_t loops = waiting_set_.size();

    for (size_t i = 0; i < loops; ++i) {
      std::vector<Task*> notified_waiters_;
      notified_waiters_.reserve(waiting_set_.size());
      for (auto&& t : waiting_set_) {
        if (t->task_event() == TaskEvent::notify) {
          notified_waiters_.push_back(t);
        }
      }

      for (auto&& t : notified_waiters_) {
        t->set_node_state(TaskState::ready);
        waiting_set_.erase(t);
        runnable_queue_.push(t);
      }
      notified_waiters_.clear();
    }

    // all waiters must have been notified
    assert(waiting_set_.empty());

    concurrency_level_.store(0);

    runnable_queue_.drain();
    finished_queue_.drain();

    waiting_set_.clear();
    running_set_.clear();

    for (auto&& t : threads_) {
      t.join();
    }
    threads_.clear();
  }

  /** Producer-consumer queue where functions to be executed are kept */
  ConcurrentSet<Task*> waiting_set_;
  ConcurrentSet<Task*> running_set_;
  BoundedBufferQ<Task*, std::queue<Task*>, false> submission_queue_;
  BoundedBufferQ<Task*, std::queue<Task*>, false> runnable_queue_;
  BoundedBufferQ<Task*, std::queue<Task*>, false> finished_queue_;

  /** The worker threads */
  std::vector<std::thread> threads_;

  /** The maximum level of concurrency among all of the worker threads */
  std::atomic<size_t> concurrency_level_;

  /** Debug flag */
  bool debug_;

  /** Track number of submissions */
  std::atomic<size_t> num_submissions_;

  /** Track number of tasks */
  std::atomic<size_t> num_tasks_;

  /** Synchronization variables */
  std::mutex mutex_;
  std::condition_variable start_cv_;
};

}  // namespace tiledb::common

#endif  // TILEDB_DAG_FRUGAL_H
