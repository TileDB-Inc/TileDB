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

#include "tiledb/common/thread_pool/thread_pool.h"

namespace tiledb::common {

class BatchReadyEvent {
 public:
  /**
   * @param tp Optional thread pool. When non-null, signal() resumes waiters
   *   via tp->schedule_resume() — posted as tasks on the pool, not inline on
   *   the calling thread. This prevents unbounded stack growth when signal()
   *   is called from a deep call stack (e.g., an I/O completion callback) and
   *   many tiles are waiting on the same block.
   *
   *   When null, signal() resumes waiters inline on the calling thread. Only
   *   safe when the caller has a shallow stack and few waiters per event.
   */
  explicit BatchReadyEvent(ThreadPool* tp = nullptr) noexcept
      : tp_(tp) {
  }

  // Non-copyable, non-movable.
  BatchReadyEvent(const BatchReadyEvent&) = delete;
  BatchReadyEvent& operator=(const BatchReadyEvent&) = delete;

  /**
   * Signal the event, resuming all waiting coroutines.
   * Can only be called once.
   *
   * If a ThreadPool was provided at construction, waiters are resumed via
   * schedule_resume (posted to the pool). Otherwise they are resumed inline
   * on the calling thread.
   */
  void signal() noexcept {
    Waiter* waiters = nullptr;
    {
      std::lock_guard lock(mutex_);
      signaled_ = true;
      waiters = waiters_;
      waiters_ = nullptr;
    }
    resume_waiters(waiters);
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
    resume_waiters(waiters);
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
  void resume_waiters(Waiter* waiters) noexcept {
    while (waiters) {
      auto* next = waiters->next;
      if (tp_) {
        tp_->schedule_resume(waiters->handle);
      } else {
        waiters->handle.resume();
      }
      waiters = next;
    }
  }

  mutable std::mutex mutex_;
  bool signaled_{false};
  std::exception_ptr exception_;
  Waiter* waiters_{nullptr};
  ThreadPool* tp_{nullptr};
};

}  // namespace tiledb::common

#endif  // TILEDB_COROUTINE_BATCH_READY_EVENT_H
