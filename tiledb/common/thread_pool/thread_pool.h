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

#include "tiledb/common/logger.h"
#include "tiledb/common/macros.h"
#include "tiledb/common/status.h"

namespace tiledb::common {

class ThreadPool {
 public:
  using Task = std::shared_future<Status>;

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  ThreadPool()
      : concurrency_level_(0) {
  }

  /** Constructor. */
  explicit ThreadPool(
      size_t n) {  // There shouldn't be an uninitalized threadpool (IMO)
    init(n);
  }

  /** Destructor. */
  ~ThreadPool() {
    shutdown();
  }

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Initialize the thread pool.
   *
   * @param concurrency_level Maximum level of concurrency. Defaults to
   * available hardware concurrency.
   * @return Status
   */


  Status init(size_t concurrency_level = std::thread::hardware_concurrency());

public:

  size_t concurrency_level() {
    return concurrency_level_;
  }

  /**
   * Schedule a new task to be executed. If the returned future object
   * is valid, `f` is execute asynchronously. To avoid deadlock, `f`
   * should not acquire non-recursive locks held by the calling thread.
   * A std::shared_future is returned instead of a std::future so that get() can
   * be called multiple times.
   *
   * @param task Callable object to call
   * @param args... Parameters to pass to f
   * @return std::future referring to the shared state created by this call
   */
  template <
      class Fn,
      class... Args,
      class R = std::invoke_result_t<std::decay_t<Fn>, std::decay_t<Args>...>>
  std::shared_future<R> async(const Fn& f, const Args&... args) {
    if (concurrency_level_ == 0) {
      Task invalid_future;
      LOG_ERROR("Cannot execute task; thread pool uninitialized.");
      return invalid_future;
    }

    std::shared_ptr<std::promise<R>> task_promise(new std::promise<R>);
    std::shared_future<R> future = task_promise->get_future();

    task_queue_.push([f, args..., task_promise] {
      try {
        // Separately handle the case of future<void>
        if constexpr (std::is_void_v<R>) {
          f(args...);
          task_promise->set_value();
        } else {
          task_promise->set_value(f(args...));
        }
      } catch (...) {
        try {
          task_promise->set_exception(std::current_exception());
        } catch (...) {
        }
      }
    });

    return future;
  }

  /**
   * Alias for async()
   */
  template <
      class Fn,
      class... Args,
      class R = std::invoke_result_t<std::decay_t<Fn>, std::decay_t<Args>...>>
  std::shared_future<R> execute(const Fn& task, const Args&... args) {
    return async(task, args...);
  }

  /**
   * Wait on all the given tasks to complete. This is safe to call recursively
   * and may execute pending tasks on the calling thread while waiting.
   *
   * @param tasks Task list to wait on.
   * @return Status::Ok if all tasks returned Status::Ok, otherwise the first
   * error status is returned
   */
  Status wait_all(std::vector<Task>& tasks);

  /**
   * Wait on all the given tasks to complete, return a vector of their return
   * Status. This is safe to call recursively and may execute pending tasks
   * on the calling thread while waiting.
   *
   * @param tasks Task list to wait on
   * @return Vector of each task's Status.
   */
  std::vector<Status> wait_all_status(std::vector<Task>& tasks);

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

 private:
  /** The worker thread routine */
  void worker();

  /** Terminate threads in the thread pool */
  void shutdown();

  /** Producer-consumer queue where functions to be executed are kept */
  ProducerConsumerQueue<std::function<void()>> task_queue_;

  /** The worker threads */
  std::vector<std::thread> threads_;

  /** The maximum level of concurrency among all of the worker threads */
  std::atomic<size_t> concurrency_level_;
};

}  // namespace tiledb::common

#endif  // TILEDB_THREAD_POOL_H
