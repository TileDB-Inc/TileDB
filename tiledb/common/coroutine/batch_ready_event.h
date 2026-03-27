/**
 * @file tiledb/common/coroutine/batch_ready_event.h
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
 * A one-shot event for coroutines. Multiple coroutines can co_await the event.
 * When signal() is called, all waiters are resumed. If the event is already
 * signaled when a coroutine awaits, it resumes immediately.
 *
 * This is used to notify per-tile coroutines when their I/O batch completes.
 *
 * IMPORTANT: Do not use temporary lambda coroutines that capture the event by
 * reference. The closure object of a temporary lambda is destroyed after the
 * Task is created, leaving a dangling reference in the coroutine frame. Use
 * free functions or named lambdas instead. See unit_coroutine.cc for examples.
 */

#ifndef TILEDB_COROUTINE_BATCH_READY_EVENT_H
#define TILEDB_COROUTINE_BATCH_READY_EVENT_H

#include <atomic>
#include <coroutine>
#include <exception>
#include <mutex>

namespace tiledb::common {

class BatchReadyEvent {
 public:
  BatchReadyEvent() noexcept = default;

  // Non-copyable, non-movable.
  BatchReadyEvent(const BatchReadyEvent&) = delete;
  BatchReadyEvent& operator=(const BatchReadyEvent&) = delete;

  /**
   * Signal the event, resuming all waiting coroutines.
   * Can only be called once.
   */
  void signal() noexcept {
    Waiter* waiters = nullptr;
    {
      std::lock_guard lock(mutex_);
      signaled_ = true;
      waiters = waiters_;
      waiters_ = nullptr;
    }
    // Resume all waiters outside the lock.
    while (waiters) {
      auto* next = waiters->next;
      waiters->handle.resume();
      waiters = next;
    }
  }

  /**
   * Signal the event with an error. Waiting coroutines will have the
   * exception rethrown when they resume.
   */
  void signal_error(std::exception_ptr e) noexcept {
    Waiter* waiters = nullptr;
    {
      std::lock_guard lock(mutex_);
      signaled_ = true;
      exception_ = e;
      waiters = waiters_;
      waiters_ = nullptr;
    }
    while (waiters) {
      auto* next = waiters->next;
      waiters->handle.resume();
      waiters = next;
    }
  }

  /**
   * Returns true if the event has been signaled.
   */
  bool is_signaled() const noexcept {
    std::lock_guard lock(mutex_);
    return signaled_;
  }

  struct Waiter {
    BatchReadyEvent& event;
    Waiter* next{nullptr};
    std::coroutine_handle<> handle;

    bool await_ready() noexcept {
      std::lock_guard lock(event.mutex_);
      return event.signaled_;
    }

    bool await_suspend(std::coroutine_handle<> h) noexcept {
      handle = h;
      std::lock_guard lock(event.mutex_);
      if (event.signaled_) {
        return false;  // Already signaled, don't suspend.
      }
      next = event.waiters_;
      event.waiters_ = this;
      return true;
    }

    void await_resume() {
      // If the event was signaled with an error, rethrow it.
      if (event.exception_) {
        std::rethrow_exception(event.exception_);
      }
    }
  };

  /**
   * co_await event to wait for the signal.
   */
  [[nodiscard]] Waiter operator co_await() noexcept {
    return Waiter{*this, nullptr, {}};
  }

 private:
  mutable std::mutex mutex_;
  bool signaled_{false};
  std::exception_ptr exception_;
  Waiter* waiters_{nullptr};
};

}  // namespace tiledb::common

#endif  // TILEDB_COROUTINE_BATCH_READY_EVENT_H
