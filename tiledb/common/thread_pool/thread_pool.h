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

#include "tiledb/common/common.h"
#include "tiledb/common/logger_public.h"
#include "tiledb/common/macros.h"
#include "tiledb/common/status.h"

namespace tiledb::common {

class ThreadPool {
 public:
  class ThreadPoolTask {
   public:
    ThreadPoolTask() = default;
    ThreadPoolTask(ThreadPool* tp)
        : tp_(tp) {};
    virtual ~ThreadPoolTask() {};

   protected:
    friend class ThreadPool;

    ThreadPool* tp_;
    virtual std::future_status wait_for(
        const std::chrono::milliseconds timeout_duration) const = 0;
    virtual bool valid() const noexcept = 0;
    virtual Status get() = 0;
  };

  class Task : public ThreadPoolTask {
   public:
    Task() = default;

    // Constructor, std::future can only be moved
    Task(std::future<Status>&& f, ThreadPool* tp)
        : ThreadPoolTask(tp)
        , f_(std::move(f)) {};

    // Move constructor
    Task(Task&& t) noexcept
        : ThreadPoolTask(t.tp_)
        , f_(std::move(t.f_)) {};

    // Disable copying
    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;

    Status wait() {
      return tp_->wait(this);
    }

    bool valid() const noexcept {
      return f_.valid();
    }

   private:
    friend class ThreadPool;

    std::future_status wait_for(
        const std::chrono::milliseconds timeout_duration) const {
      return f_.wait_for(timeout_duration);
    }

    Status get() {
      return f_.get();
    }

    std::future<Status> f_;
  };

  class SharedTask : public ThreadPoolTask {
   public:
    SharedTask() = default;

    SharedTask(std::shared_future<Status>&& f, ThreadPool* tp)
        : ThreadPoolTask(tp)
        , f_(std::move(f)) {};

    SharedTask(std::future<Status>&& f, ThreadPool* tp) noexcept
        : ThreadPoolTask(tp)
        , f_(std::move(f)) {};

    SharedTask(Task&& t) noexcept
        : ThreadPoolTask(t.tp_)
        , f_(std::move(t.f_)) {};

    SharedTask(SharedTask&& t) noexcept
        : ThreadPoolTask(t.tp_)
        , f_(std::move(t.f_)) {};

    SharedTask(const SharedTask& t) noexcept
        : ThreadPoolTask(t.tp_)
        , f_(t.f_) {};

    SharedTask& operator=(const SharedTask& t) noexcept {
      tp_ = t.tp_;
      f_ = t.f_;
      return *this;
    }

    SharedTask& operator=(SharedTask&& t) noexcept {
      tp_ = t.tp_;
      f_ = std::move(t.f_);
      return *this;
    }

    SharedTask& operator=(Task&& t) noexcept {
      tp_ = t.tp_;
      f_ = std::move(t.f_);
      return *this;
    }

    Status wait() {
      return tp_->wait(this);
    }

    bool valid() const noexcept {
      return f_.valid();
    }

   private:
    friend class ThreadPool;

    std::future_status wait_for(
        const std::chrono::milliseconds timeout_duration) const {
      return f_.wait_for(timeout_duration);
    }

    Status get() {
      return f_.get();
    }

    std::shared_future<Status> f_;
  };

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

  template <class Fn, class... Args>
  auto async(Fn&& f, Args&&... args) {
    if (concurrency_level_ == 0) {
      Task invalid_future;
      LOG_ERROR("Cannot execute task; thread pool uninitialized.");
      return invalid_future;
    }

    using R = std::invoke_result_t<std::decay_t<Fn>, std::decay_t<Args>...>;

    auto task = make_shared<std::packaged_task<R()>>(
        HERE(),
        [f = std::forward<Fn>(f),
         args = std::make_tuple(std::forward<Args>(args)...)]() mutable {
          return std::apply(std::move(f), std::move(args));
        });

    Task future(task->get_future(), this);

    task_queue_.push(task);

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

  /**
   * Wait on all the given tasks to complete. This function is safe to call
   * recursively and may execute pending tasks on the calling thread while
   * waiting.
   *
   * @param tasks Task list to wait on.
   * @return Status::Ok if all tasks returned Status::Ok, otherwise the first
   * error status is returned
   */
  Status wait_all(std::vector<ThreadPoolTask*>& tasks);

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
  std::vector<Status> wait_all_status(std::vector<ThreadPoolTask*>& tasks);

  /* Helper functions for lists that consists purely of Tasks */
  Status wait_all(std::vector<Task>& tasks);
  std::vector<Status> wait_all_status(std::vector<Task>& tasks);

  /* Helper functions for lists that consists purely of SharedTasks */
  Status wait_all(std::vector<SharedTask>& tasks);
  std::vector<Status> wait_all_status(std::vector<SharedTask>& tasks);

  /**
   * Wait on a single tasks to complete. This function is safe to call
   * recursively and may execute pending tasks on the calling thread while
   * waiting.
   *
   * @param task Task to wait on.
   * @return Status::Ok if the task returned Status::Ok, otherwise the error
   * status is returned
   */
  Status wait(ThreadPoolTask* task);

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

 private:
  /** The worker thread routine */
  void worker();

  /** Terminate threads in the thread pool */
  void shutdown();

  /** Producer-consumer queue where functions to be executed are kept */
  ProducerConsumerQueue<
      shared_ptr<std::packaged_task<Status()>>,
      std::deque<shared_ptr<std::packaged_task<Status()>>>>
      task_queue_;

  /** The worker threads */
  std::vector<std::thread> threads_;

  /** The maximum level of concurrency among all of the worker threads */
  std::atomic<size_t> concurrency_level_;
};
}  // namespace tiledb::common

#endif  // TILEDB_THREAD_POOL_H
