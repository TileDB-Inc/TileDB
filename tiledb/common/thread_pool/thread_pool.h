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
  template <typename T>
  class TaskBase : public std::future<T> {
   public:
    TaskBase() noexcept
        : std::future<T>()
        , tp_(nullptr){};

    TaskBase(std::future<T>&& f) noexcept
        : std::future<T>(std::move(f))
        , tp_(nullptr){};

    TaskBase(std::future<T>&& f, ThreadPool* tp) noexcept
        : std::future<T>(std::move(f))
        , tp_(tp){};

    TaskBase(TaskBase&& tb) noexcept
        : std::future<T>(std::move(tb))
        , tp_(tb.tp_){};

    // Disable copying
    TaskBase(const TaskBase&) = delete;
    TaskBase& operator=(const TaskBase&) = delete;
    /*    TaskBase(const TaskBase& tb) : std::future<T>(tb),  tp_(tb.tp_) {
    //      std::future<T>(std::move(tb))._M_swap(*this);
        };
        TaskBase& operator=(const TaskBase& tb) {
          tp_ = tb.tp_;
          std::future<T>(std::move(tb))._M_swap(*this);
        }*/
    //
    //    TaskBase& operator=(TaskBase&& __fut) noexcept {
    //      this->tp_ = __fut.tp_;
    //      std::future<T>::operator=(__fut);
    //      return *this;
    //    }

    ~TaskBase() {
      if (tp_ != nullptr && this->valid()) {
        std::ignore = tp_->wait(*this);
      }
    };

    void wait() {
      if (tp_ != nullptr) {
        throw_if_not_ok(tp_->wait(*this));
      } else {
        return std::future<T>::wait();
      }
    };

    T get(const bool wait_thread_pool = true) {
      if (wait_thread_pool && tp_ != nullptr) {
        throw_if_not_ok(tp_->wait(*this));
      }
      return std::future<T>::get();
    }

    ThreadPool* tp() const {
      return tp_;
    }

   private:
    ThreadPool* tp_;
  };

  template <typename T>
  class SharedTaskBase : public std::shared_future<T> {
   public:
    SharedTaskBase() noexcept
        : std::shared_future<T>()
        , tp_(nullptr){};

    SharedTaskBase(std::shared_future<T>&& f) noexcept
        : std::shared_future<T>(std::move(f))
        , tp_(nullptr){};

    SharedTaskBase(std::shared_future<T>&& f, ThreadPool* tp) noexcept
        : std::shared_future<T>(std::move(f))
        , tp_(tp){};

    SharedTaskBase(std::future<T>&& f) noexcept
        : std::shared_future<T>(std::move(f))
        , tp_(nullptr){};

    SharedTaskBase(std::future<T>&& f, ThreadPool* tp) noexcept
        : std::shared_future<T>(std::move(f))
        , tp_(tp){};

    SharedTaskBase(TaskBase<T>&& f) noexcept
        : std::shared_future<T>(std::move(f))
        , tp_(f.tp()){};

    SharedTaskBase(SharedTaskBase&& f) noexcept
        : std::shared_future<T>(std::move(f))
        , tp_(f.tp_){};

    SharedTaskBase(const SharedTaskBase& __sf) noexcept
        : std::shared_future<T>(__sf) {
      tp_ = __sf.tp_;
    }

    SharedTaskBase& operator=(const SharedTaskBase& __sf) noexcept {
      this->tp_ = __sf.tp_;
      std::shared_future<T>::operator=(__sf);
      return *this;
    }

    SharedTaskBase& operator=(SharedTaskBase&& __sf) noexcept {
      this->tp_ = __sf.tp_;
      std::shared_future<T>::operator=(__sf);
      return *this;
    }

    ~SharedTaskBase() {
      if (tp_ != nullptr && this->valid()) {
        std::ignore = tp_->wait(*this);
      }
    };

    void wait() {
      if (tp_ != nullptr) {
        throw_if_not_ok(tp_->wait(*this));
      } else {
        return std::shared_future<T>::wait();
      }
    }

    T get(const bool wait_thread_pool = true) {
      if (wait_thread_pool && tp_ != nullptr) {
        throw_if_not_ok(tp_->wait(*this));
      }
      return std::shared_future<T>::get();
    }

    ThreadPool* tp() const {
      return tp_;
    }

   private:
    ThreadPool* tp_;
  };

 public:
  using Task = TaskBase<Status>;
  using SharedTask = SharedTaskBase<Status>;

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
   * Wait on a single tasks to complete. This function is safe to call
   * recursively and may execute pending tasks on the calling thread while
   * waiting.
   *
   * @param task Task to wait on.
   * @return Status::Ok if the task returned Status::Ok, otherwise the error
   * status is returned
   */
  Status wait(Task& task);

  /**
   * Wait on all the given tasks to complete. This function is safe to call
   * recursively and may execute pending tasks on the calling thread while
   * waiting.
   *
   * @param tasks SharedTask list to wait on.
   * @return Status::Ok if all tasks returned Status::Ok, otherwise the first
   * error status is returned
   */
  Status wait_all(std::vector<SharedTask>& tasks);

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
   * @param tasks SharedTask list to wait on
   * @return Vector of each task's Status.
   */
  std::vector<Status> wait_all_status(std::vector<SharedTask>& tasks);

  /**
   * Wait on a single tasks to complete. This function is safe to call
   * recursively and may execute pending tasks on the calling thread while
   * waiting.
   *
   * @param task SharedTask to wait on.
   * @return Status::Ok if the task returned Status::Ok, otherwise the error
   * status is returned
   */
  Status wait(SharedTask& task);

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
