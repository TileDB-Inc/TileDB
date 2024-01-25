/**
 * @file   threadpool.h
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
 * This file declares a threadpool with various parameterized capabilities.
 */

#ifndef TILEDB_THREADPOOL_H
#define TILEDB_THREADPOOL_H

#include "../utility/bounded_buffer.h"

#include <functional>
#include <future>
#include <iostream>
#include <type_traits>
#include <vector>

namespace tiledb::common {

template <bool MultipleQueues>
struct QueueBase;
template <>
struct QueueBase<true> {
  QueueBase(size_t num_threads)
      : task_queues_(num_threads) {
  }

  std::atomic<size_t> index_;
  const size_t rounds_{3};
  std::vector<ProducerConsumerQueue<std::shared_ptr<std::function<void()>>>>
      task_queues_;
};

template <>
struct QueueBase<false> {
  QueueBase(size_t) {
  }
  ProducerConsumerQueue<std::shared_ptr<std::function<void()>>> task_queue_;
};

/**
 * Experimental threadpool class.
 *
 * @tparam WorkStealing Whether the threadpool implements a work-stealing
 * scheme.  Only applicable when there are multiple queues.
 * @tparam MultipleQueues Whether the threadpool uses one queue per thread (plus
 * a global queue).
 * @tparam RecursivePush Whether the threadpool queues tasks from threads in the
 * threadpool.  If not, the worker thread trying to submit the job will just run
 * the job.  This is not necessarily a bad scheme, since the job being enqueued
 * would go at the top of the stack and would be the next job to be run by a
 * worker thread anyway.
 */
template <
    bool WorkStealing = true,
    bool MultipleQueues = false,
    bool RecursivePush = true>
class ThreadPool : public QueueBase<MultipleQueues> {
 public:
  using QBase = QueueBase<MultipleQueues>;

  explicit ThreadPool(size_t concurrency = std::thread::hardware_concurrency())
      : QueueBase<MultipleQueues>(concurrency)
      , num_threads_(concurrency) {
    threads_.reserve(num_threads_);

    for (size_t i = 0; i < num_threads_; ++i) {
      std::thread tmp = std::thread(&ThreadPool::worker, this, i);
      threads_.emplace_back(std::move(tmp));
    }
  }

  ~ThreadPool() {
    if constexpr (MultipleQueues) {
      for (auto& t : QBase::task_queues_) {
        t.shutdown();
      }

    } else {
      QBase::task_queue_.shutdown();
    }

    for (auto&& t : threads_) {
      t.join();
    }
    threads_.clear();
  }

 public:
  template <class Fn, class... Args>
  auto async(Fn&& f, Args&&... args) {
    using R = std::invoke_result_t<std::decay_t<Fn>, std::decay_t<Args>...>;

    std::shared_ptr<std::promise<R>> task_promise(new std::promise<R>);
    std::future<R> future = task_promise->get_future();

#if 1
    auto task = std::make_shared<std::function<void()>>(
        [f = std::forward<Fn>(f),
         args = std::make_tuple(std::forward<Args>(args)...),
         task_promise]() mutable {
          try {
            if constexpr (std::is_void_v<R>) {
              std::apply(std::move(f), std::move(args));
              task_promise->set_value();
            } else {
              task_promise->set_value(
                  std::apply(std::move(f), std::move(args)));
            }
          } catch (...) {
            task_promise->set_exception(std::current_exception());
          }
        });
#else
    auto task = std::make_shared<std::packaged_task<R()>>(
        [f = std::forward<Fn>(f),
         args = std::make_tuple(std::forward<Args>(args)...)]() mutable {
          return std::apply(std::move(f), std::move(args));
        });
#endif
    if constexpr (RecursivePush) {
      if constexpr (MultipleQueues) {
        size_t i = QBase::index_++;
        QBase::task_queues_[i % num_threads_].push(task);
      } else {
        QBase::task_queue_.push(task);
      }
    } else {
      bool found = false;
      for (auto& j : threads_) {
        if (j.get_id() == std::this_thread::get_id()) {
          found = true;
          break;
        }
      }

      if (found) {
        (*task)();
      } else {
        if constexpr (MultipleQueues) {
          size_t i = QBase::index_++;
          QBase::task_queues_[i % num_threads_].push(task);
        } else {
          QBase::task_queue_.push(task);
        }
      }
    }

    return future;
  }

  template <class R, bool U = WorkStealing>
  auto wait(std::future<R>&& task, std::enable_if_t<U, bool> = true) {
    while (true) {
      if (task.wait_for(std::chrono::milliseconds(0)) ==
          std::future_status::ready) {
        if constexpr (std::is_void_v<R>) {
          task.wait();
          return;
        } else {
          auto ret = task.get();
          return ret;
        }
      } else {
        std::optional<std::shared_ptr<std::function<void()>>> val;

        if constexpr (MultipleQueues) {
          size_t i = QBase::index_++;
          for (size_t j = 0; j < num_threads_ * QBase::rounds_; ++j) {
            val = QBase::task_queues_[(i + j) % num_threads_].try_pop();

            if (val) {
              break;
            }
          }
        } else {
          val = QBase::task_queue_.try_pop();
        }

        if (val) {
          (*(*val))();
        } else {
          std::this_thread::yield();
        }
      }
    }
  }

  template <class R, bool U = WorkStealing>
  auto wait(std::future<R>&& task, std::enable_if_t<!U, bool> = true) {
    return task.wait();
  }

  size_t num_threads() {
    return num_threads_;
  }

 private:
  void worker(size_t i) {
    while (true) {
      std::optional<std::shared_ptr<std::function<void()>>> val;

      if constexpr (MultipleQueues) {
        for (size_t j = 0; j < num_threads_ * QBase::rounds_; ++j) {
          val = QBase::task_queues_[(i + j) % num_threads_].try_pop();
          if (val) {
            break;
          }
        }
      } else {
        val = QBase::task_queue_.try_pop();
      }

      if (!val) {
        if constexpr (MultipleQueues) {
          val = QBase::task_queues_[i].pop();
        } else {
          val = QBase::task_queue_.pop();
        }
      }
      if (val) {
        (*(*val))();
      } else {
        break;
      }
    }
  }

  const size_t num_threads_;
  std::vector<std::thread> threads_;
};

}  // namespace tiledb::common
#endif  // TILEDB_THREADPOOL_H
