/**
 * @file tiledb/common/coroutine/when_all.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2025 TileDB, Inc.
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
 * Awaitable combinator that waits for all tasks in a collection to complete.
 */

#ifndef TILEDB_COROUTINE_WHEN_ALL_H
#define TILEDB_COROUTINE_WHEN_ALL_H

#include <atomic>
#include <coroutine>
#include <exception>
#include <mutex>
#include <vector>

#include "tiledb/common/coroutine/task.h"

namespace tiledb::common {

namespace detail {

/**
 * Counter that tracks how many tasks are still outstanding. When the last
 * task completes, the awaiting coroutine is resumed.
 */
class WhenAllCounter {
 public:
  WhenAllCounter(size_t count, std::coroutine_handle<> continuation) noexcept
      : count_(count + 1)
      , continuation_(continuation) {
  }

  /**
   * Called when a child task completes. Returns true if this was the last
   * task (and the continuation should be resumed).
   */
  bool notify_complete() noexcept {
    return count_.fetch_sub(1, std::memory_order_acq_rel) == 1;
  }

  std::coroutine_handle<> continuation() const noexcept {
    return continuation_;
  }

 private:
  std::atomic<size_t> count_;
  std::coroutine_handle<> continuation_;
};

/**
 * Fire-and-forget coroutine return type for when_all wrapper tasks.
 *
 * Unlike Task<void>, this type:
 * - Starts eagerly (initial_suspend = suspend_never)
 * - Self-destructs at completion (final_suspend = suspend_never)
 *
 * This avoids the memory leak where detached Task<void> wrappers stay
 * suspended at final_suspend forever, and avoids the use-after-free from
 * trying to destroy them externally while the last wrapper is still running.
 *
 * NOTE: unhandled_exception calls std::terminate(). Exceptions from child
 * tasks must be caught INSIDE when_all_task (they are — the try/catch around
 * co_await captures them). Do NOT let exceptions escape when_all_task.
 */
struct WhenAllTaskHandle {
  struct promise_type {
    WhenAllTaskHandle get_return_object() noexcept {
      return {};
    }
    std::suspend_never initial_suspend() noexcept {
      return {};
    }
    std::suspend_never final_suspend() noexcept {
      return {};
    }
    void return_void() noexcept {
    }
    void unhandled_exception() noexcept {
      std::terminate();
    }
  };
};

/**
 * A wrapper coroutine that starts a child task and then notifies the
 * WhenAllCounter when it completes. Captures the first exception.
 *
 * Uses WhenAllTaskHandle (fire-and-forget): the coroutine frame is
 * automatically destroyed when the coroutine completes. No external
 * cleanup is needed.
 */
inline WhenAllTaskHandle when_all_task(
    Task<void> task,
    WhenAllCounter* counter,
    std::exception_ptr* first_exception,
    std::mutex* exception_mutex) {
  try {
    co_await std::move(task);
  } catch (...) {
    std::lock_guard lock(*exception_mutex);
    if (!*first_exception) {
      *first_exception = std::current_exception();
    }
  }
  // Copy the continuation handle before notify_complete(). If this is the
  // last task, resume() will run the parent coroutine which destroys the
  // WhenAllAwaitable (and its counter_ unique_ptr) before resume() returns.
  // Copying the handle first ensures we never touch `counter` after it may
  // have been freed.
  auto cont = counter->continuation();
  if (counter->notify_complete()) {
    cont.resume();
  }
}

}  // namespace detail

/**
 * Awaitable that completes when all tasks in the vector have completed.
 *
 * If any task throws an exception, the first exception is rethrown after
 * all tasks have completed.
 *
 * Usage:
 *   std::vector<Task<void>> tasks;
 *   tasks.push_back(do_work_a());
 *   tasks.push_back(do_work_b());
 *   co_await when_all(std::move(tasks));
 */
class WhenAllAwaitable {
 public:
  explicit WhenAllAwaitable(std::vector<Task<void>> tasks) noexcept
      : tasks_(std::move(tasks)) {
  }

  bool await_ready() const noexcept {
    return tasks_.empty();
  }

  // Symmetric transfer: returns the coroutine to resume next (or
  // noop_coroutine if tasks are still running). This avoids calling
  // continuation.resume() from inside await_suspend — which is valid per
  // C++20 but fragile and can cause stack depth issues when many tasks
  // complete synchronously. The compiler performs the resume as a tail-call
  // after await_suspend returns, so WhenAllAwaitable members are no longer
  // reachable when the continuation executes.
  std::coroutine_handle<> await_suspend(std::coroutine_handle<> continuation) {
    counter_ =
        std::make_unique<detail::WhenAllCounter>(tasks_.size(), continuation);

    // Wrap each task in a when_all_task that notifies the counter.
    // WhenAllTaskHandle is fire-and-forget: starts eagerly (suspend_never
    // initial_suspend) and self-destructs (suspend_never final_suspend).
    // No handle tracking needed.
    for (auto& task : tasks_) {
      // when_all_task starts immediately (eager) and runs until it
      // suspends on co_await of the child task or completes.
      detail::when_all_task(
          std::move(task), counter_.get(), &first_exception_, &mutex_);
    }
    tasks_.clear();

    // Account for the +1 in the counter constructor. If all tasks already
    // completed synchronously during the when_all_task calls above, the
    // counter hits zero here and we tail-call the continuation directly.
    if (counter_->notify_complete()) {
      return continuation;
    }
    return std::noop_coroutine();
  }

  void await_resume() {
    if (first_exception_) {
      std::rethrow_exception(first_exception_);
    }
  }

 private:
  std::vector<Task<void>> tasks_;
  std::unique_ptr<detail::WhenAllCounter> counter_;
  std::exception_ptr first_exception_;
  std::mutex mutex_;
};

inline WhenAllAwaitable when_all(std::vector<Task<void>> tasks) {
  return WhenAllAwaitable{std::move(tasks)};
}

}  // namespace tiledb::common

#endif  // TILEDB_COROUTINE_WHEN_ALL_H
