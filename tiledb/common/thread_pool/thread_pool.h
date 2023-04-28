/**
 * @file   thread_pool.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2021 TileDB, Inc.
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
 * This file declares the ThreadPool class.
 */

#ifndef TILEDB_THREAD_POOL_H
#define TILEDB_THREAD_POOL_H

#include "producer_consumer_queue.h"

#include <functional>
#include <future>
#include <sstream>

#include "tiledb/common/common.h"
#include "tiledb/common/logger_public.h"
#include "tiledb/common/macros.h"
#include "tiledb/common/status.h"

namespace tiledb::common {

namespace threadpool_impl {
/**
 * Evaluates std::future<T> values and accumulates their result into a vector.
 */
template <class T>
struct ResultAccumulator {
  using collection_type = std::vector<T>;
  ResultAccumulator(size_t capacity)
      : results_(capacity) {
  }
  void add(size_t index, std::future<T>& task) {
    results_[index] = task.get();
  }
  collection_type results() {
    return results_;
  }

 private:
  collection_type results_;
};

/**
 * Specialization of ResultAccumulator for void, which does not accumulate
 * anything.
 */
template <>
struct ResultAccumulator<void> {
  using collection_type = void;
  ResultAccumulator(size_t) {
  }
  void add(size_t, std::future<void>& task) {
    task.get();
  }
  collection_type results() {
  }
};
}  // namespace threadpool_impl

class ThreadPool {
 public:
  using Task = std::future<Status>;

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
  explicit ThreadPool(size_t n);

  /** Deleted default constructor */
  ThreadPool() = delete;

  /** Destructor. */
  ~ThreadPool() {
    shutdown();
  }

  /* ********************************* */
  /*                API                */
  /* ********************************* */

 public:
  size_t concurrency_level() {
    return concurrency_level_;
  }

  /**
   * Schedule a new task to be executed. If the returned future object
   * is valid, `f` is execute asynchronously. To avoid deadlock, `f`
   * should not acquire non-recursive locks held by the calling thread.
   *
   * @param f Callable object to call
   * @param args... Parameters to pass to f
   * @return std::future referring to the shared state created by this call
   */

  template <
      class Fn,
      class... Args,
      class R = std::invoke_result_t<std::decay_t<Fn>, std::decay_t<Args>...>>
  std::future<R> async(Fn&& f, Args&&... args) {
    if (concurrency_level_ == 0) {
      LOG_ERROR("Cannot execute task; thread pool uninitialized.");
      return {};
    }

    auto task =
        make_shared<std::packaged_task<R()>>(HERE(), std::bind(f, args...));
    task_queue_.push([task]() { (*task)(); });

    return task->get_future();
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

  /**
   * Wait on all the given tasks to complete, returning a vector of their
   * results.  Exceptions caught while waiting are aggregated and rethrown as a
   * StatusException. Results are saved at the same index in the return vector
   * as the corresponding task in the input vector.
   *
   * This function is safe to call recursively and may execute pending tasks
   * with the calling thread while waiting.
   *
   * @param tasks Task list to wait on
   * @return Vector of each task's result or void if the tasks return void.
   */
  template <class R>
  typename threadpool_impl::ResultAccumulator<R>::collection_type wait_all(
      std::vector<std::future<R>>& tasks) {
    threadpool_impl::ResultAccumulator<R> results(tasks.size());
    std::stringstream exceptions;

    auto log_exception = [&exceptions](const char* message) {
      // If this is the first exception, add an intro message.
      if (exceptions.tellp() <= 0) {
        exceptions << "One or more errors occurred";
      }
      exceptions << "\n * " << message;
    };

    std::queue<size_t> pending_tasks;

    // Create queue of ids of all the pending tasks for processing
    for (size_t i = 0; i < tasks.size(); ++i) {
      pending_tasks.push(i);
    }

    // Process tasks in the pending queue
    while (!pending_tasks.empty()) {
      auto task_id = pending_tasks.front();
      pending_tasks.pop();
      auto& task = tasks[task_id];

      if (!task.valid()) {
        log_exception("Invalid task future");
      } else if (
          task.wait_for(std::chrono::milliseconds(0)) !=
          std::future_status::timeout) {
        // Task is completed, get result, handling possible exceptions
        try {
          results.add(task_id, task);
        } catch (const std::exception& e) {
          log_exception(e.what());
        } catch (const std::string& msg) {
          log_exception(msg.c_str());
        } catch (...) {
          log_exception("Unknown exception");
        }
      } else {
        // If the task is not completed, try again later
        pending_tasks.push(task_id);

        // In the meantime, try to do something useful to make progress (and
        // avoid deadlock)
        if (!pump()) {
          // If nothing useful to do, yield so we don't burn cycles
          // going through the task list over and over (thereby slowing down
          // other threads).
          std::this_thread::yield();

          // (An alternative would be to wait some amount of time)
          // task.wait_for(std::chrono::milliseconds(10));
        }
      }
    }

    // If exceptions is not empty, at least one of the tasks has failed. We
    // throw an exception containing all exception messages.
    if (exceptions.tellp() > 0) {
      throw StatusException(Status_ThreadPoolError(exceptions.str()));
    }
    return results.results();
  }

  /**
   * Wait on all the given tasks to complete. This function is safe to call
   * recursively and may execute pending tasks on the calling thread while
   * waiting.
   *
   * @param tasks Task list to wait on.
   * @return Status::Ok if all tasks returned Status::Ok, otherwise the first
   * error status is returned
   */
  Status wait_all(std::vector<Task>& tasks);

  /**
   * Wait on all the given tasks to complete, returning a vector of their return
   * Status.  Exceptions caught while waiting are returned as Status_TaskError.
   * Status are saved at the same index in the return vector as the
   * corresponding task in the input vector.  The status vector may contain more
   * than one error Status.
   *
   * This function is safe to call recursively and may execute pending tasks
   * with the calling thread while waiting.
   *
   * @param tasks Task list to wait on
   * @return Vector of each task's Status.
   */
  std::vector<Status> wait_all_status(std::vector<Task>& tasks);

  /**
   * Wait on a single task to complete. This function is safe to call
   * recursively and may execute pending tasks on the calling thread while
   * waiting. This method will throw the future's exception if it fails.
   *
   * @param task Task to wait on.
   * @return The task's result.
   */
  template <class R>
  R wait(std::future<R>& task) {
    if (!task.valid()) {
      throw std::runtime_error("Invalid task future");
    }
    while (task.wait_for(std::chrono::milliseconds(0)) ==
           std::future_status::timeout) {
      // Until the task completes, try to do something useful to make progress
      // (and avoid deadlock)
      if (!pump()) {
        std::this_thread::yield();
      }
    }
    // Task is completed, get result, throwing possible exceptions
    return task.get();
  }

  /**
   * Wait on a single task returning Status to complete. This function is safe
   * to call recursively and may execute pending tasks on the calling thread
   * while waiting.
   *
   * @param task Task to wait on.
   * @return Status::Ok if the task returned Status::Ok, otherwise the error
   * status is returned
   */
  Status wait(Task& task);

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

 private:
  /** Tries to run a queued task. Returns whether such task was found. */
  bool pump();

  /** The worker thread routine */
  void worker();

  /** Terminate threads in the thread pool */
  void shutdown();

  /** Producer-consumer queue where functions to be executed are kept */
  ProducerConsumerQueue<
      std::function<void()>,
      std::deque<std::function<void()>>>
      task_queue_;

  /** The worker threads */
  std::vector<std::thread> threads_;

  /** The maximum level of concurrency among all of the worker threads */
  std::atomic<size_t> concurrency_level_;
};
}  // namespace tiledb::common

#endif  // TILEDB_THREAD_POOL_H
