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
#include <iostream>
#include <sstream>

#include "tiledb/common/common.h"
#include "tiledb/common/logger_public.h"
#include "tiledb/common/macros.h"
#include "tiledb/common/status.h"

namespace tiledb::common {

class RunnableBase {
 public:
  virtual ~RunnableBase() {}
  virtual void run() = 0;
  virtual std::shared_future<Status> get_future() = 0;
};

template<typename FuncT>
class RunnableImpl : public RunnableBase {
 public:
  RunnableImpl(FuncT func) : func_(func) {
    static_assert(std::is_invocable_r_v<Status, FuncT>);
    promise_ = make_shared<std::promise<Status>>(HERE());
  }

  void run() override {
    try {
      promise_->set_value(func_());
    } catch (...) {
      promise_->set_exception(std::current_exception());
    }
  }

  std::shared_future<Status> get_future() override {
    return promise_->get_future();
  }

 private:
  FuncT func_;
  std::shared_ptr<std::promise<Status>> promise_;
};

class Runnable {
  public:
    Runnable(std::shared_ptr<RunnableBase> runner) : runner_(runner) {
    }

    template<typename FuncT>
    static Runnable create(FuncT func) {
      auto runner = make_shared<RunnableImpl<FuncT>>(HERE(), func);
      return Runnable(runner);
    }

    void operator()() {
      runner_->run();
    }

    std::shared_future<Status> get_future() {
      return runner_->get_future();
    }

  private:
    std::shared_ptr<RunnableBase> runner_;
};


class ThreadPool {
 public:
  using Task = std::shared_future<Status>;
  using TdbRunner = std::packaged_task<Status()>;

  class DepthToken {
    public:
      DepthToken() {
        depth_++;
      }

      ~DepthToken() {
        assert(depth_ >= 1 && "Invalid task depth.");
        depth_--;
      }

      uint64_t get() {
        return depth_;
      }

    private:
      /** The task depth of the current thread. */
      static thread_local uint64_t depth_;
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

  size_t size() {
    return task_queue_.size();
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
 Task async(Fn&& f, Args&&... args) {
    static_assert(std::is_invocable_r_v<Status, Fn>);
    if (concurrency_level_ == 0) {
      Task invalid_future;
      LOG_ERROR("Cannot execute task; thread pool uninitialized.");
      return invalid_future;
    }

    auto task = make_shared<TdbRunner>(
    HERE(),
    [f = std::forward<Fn>(f),
     args = std::make_tuple(std::forward<Args>(args)...)]() mutable {
      return std::apply(std::move(f), std::move(args));
    });

    auto dt = DepthToken();
    task_queue_.push(task, dt.get() + 1);
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
  Task execute(Fn&& f, Args&&... args) {
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

  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

 private:
  /** The worker thread routine */
  void worker();

  /** Terminate threads in the thread pool */
  void shutdown();

  /** Producer-consumer queue where functions to be executed are kept */
  ProducerConsumerQueue<shared_ptr<TdbRunner>> task_queue_;

  /** The worker threads */
  std::vector<std::thread> threads_;

  /** The maximum level of concurrency among all of the worker threads */
  std::atomic<size_t> concurrency_level_;
};

class CallOnce {
 public:
  CallOnce();

  template <class Fn, class... Args>
  void call(ThreadPool* tp, Fn&& f, Args&&... args) {
    {
      std::unique_lock lock(mtx_);

      if (state_ == State::CALLED) {
        return;
      }

      if (state_ == State::FAILED) {
        throw std::invalid_argument("Something went boom");
      }

      if (state_ == State::UNCALLED) {
        task_ = tp->async(std::forward<Fn>(f), std::forward<Args>(args)...);
        state_ = State::CALLING;
      }
    }

    auto my_task = std::shared_future{task_};
    auto st = tp->wait(my_task);

    {
      std::unique_lock lock{mtx_};
      if (st.ok()) {
        state_ = State::CALLED;
      } else {
        state_ = State::FAILED;
      }
    }

    throw_if_not_ok(st);
  }

 private:
  enum class State { UNCALLED = 0, CALLING = 1, CALLED = 2, FAILED = 3};

  std::mutex mtx_;
  State state_;
  ThreadPool::Task task_;
};

}  // namespace tiledb::common

#endif  // TILEDB_THREAD_POOL_H
