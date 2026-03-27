/**
 * @file tiledb/common/coroutine/async_semaphore.h
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
 * A counting semaphore for coroutines. Coroutines that co_await acquire()
 * when the count is zero will suspend until another coroutine calls release().
 */

#ifndef TILEDB_COROUTINE_ASYNC_SEMAPHORE_H
#define TILEDB_COROUTINE_ASYNC_SEMAPHORE_H

#include <cassert>
#include <coroutine>
#include <cstddef>
#include <mutex>

namespace tiledb::common {

class AsyncSemaphore {
 public:
  explicit AsyncSemaphore(size_t initial_count) noexcept
      : count_(initial_count) {
  }

  // Non-copyable, non-movable.
  AsyncSemaphore(const AsyncSemaphore&) = delete;
  AsyncSemaphore& operator=(const AsyncSemaphore&) = delete;

  ~AsyncSemaphore() {
    assert(waiters_ == nullptr && "Destroying semaphore with pending waiters");
  }

  /**
   * Awaitable returned by acquire(). Suspends if the semaphore count is zero.
   */
  struct AcquireAwaiter {
    AsyncSemaphore& sem;
    AcquireAwaiter* next{nullptr};
    std::coroutine_handle<> handle;

    bool await_ready() noexcept {
      std::lock_guard lock(sem.mutex_);
      if (sem.count_ > 0) {
        --sem.count_;
        return true;
      }
      return false;
    }

    bool await_suspend(std::coroutine_handle<> h) noexcept {
      handle = h;
      std::lock_guard lock(sem.mutex_);
      // Double-check: count may have increased between await_ready and
      // await_suspend.
      if (sem.count_ > 0) {
        --sem.count_;
        return false;  // Don't suspend, resume immediately.
      }
      // Enqueue this waiter.
      next = sem.waiters_;
      sem.waiters_ = this;
      return true;
    }

    void await_resume() noexcept {
    }
  };

  /**
   * co_await sem.acquire() to decrement the semaphore. If the count is
   * zero, the coroutine suspends until release() is called.
   */
  [[nodiscard]] AcquireAwaiter acquire() noexcept {
    return AcquireAwaiter{*this, nullptr, {}};
  }

  /**
   * Increment the semaphore count. If there are suspended waiters,
   * resumes one of them instead.
   *
   * NOTE: The waiter is resumed inline on the calling thread (direct
   * handle.resume()). This is acceptable when release() is called from
   * within a thread-pool worker that has a shallow stack. If release() could
   * be called from a deep call stack (e.g., an I/O completion callback),
   * consider posting the resumption to a thread pool instead.
   */
  void release() {
    AcquireAwaiter* to_resume = nullptr;
    {
      std::lock_guard lock(mutex_);
      if (waiters_ != nullptr) {
        // Resume one waiter instead of incrementing count.
        to_resume = waiters_;
        waiters_ = waiters_->next;
      } else {
        ++count_;
      }
    }
    if (to_resume) {
      to_resume->handle.resume();
    }
  }

 private:
  std::mutex mutex_;
  size_t count_;
  AcquireAwaiter* waiters_{nullptr};
};

}  // namespace tiledb::common

#endif  // TILEDB_COROUTINE_ASYNC_SEMAPHORE_H
