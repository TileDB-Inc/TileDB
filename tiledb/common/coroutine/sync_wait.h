/**
 * @file tiledb/common/coroutine/sync_wait.h
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
 * Bridges coroutine world to synchronous world. sync_wait(task) blocks the
 * calling thread until the given Task completes, then returns its result
 * (or rethrows its exception).
 *
 * Key safety property: The SyncWaitTask's final_suspend notifies the waiting
 * thread AFTER the coroutine is fully suspended. This prevents a data race
 * where the waiting thread destroys the coroutine frame while the worker
 * thread is still inside the coroutine body.
 */

#ifndef TILEDB_COROUTINE_SYNC_WAIT_H
#define TILEDB_COROUTINE_SYNC_WAIT_H

#include <cassert>
#include <condition_variable>
#include <coroutine>
#include <exception>
#include <mutex>
#include <type_traits>
#include <variant>

#include "tiledb/common/coroutine/task.h"
#include "tiledb/common/thread_pool/thread_pool.h"

namespace tiledb::common {

namespace detail {

/**
 * State shared between the sync_wait coroutine and the blocking caller.
 */
struct SyncWaitState {
  std::mutex mutex;
  std::condition_variable cv;
  bool done{false};

  void notify() {
    {
      std::lock_guard lock(mutex);
      done = true;
    }
    cv.notify_one();
  }

  void wait() {
    std::unique_lock lock(mutex);
    cv.wait(lock, [this] { return done; });
  }
};

// Forward declarations.
template <typename T>
class SyncWaitTask;

template <typename T>
class SyncWaitPromise;

/**
 * Promise type for SyncWaitTask<T> (non-void).
 *
 * Notification happens in final_suspend's await_suspend, AFTER the
 * coroutine is fully suspended — safe for the waiting thread to then
 * destroy the frame.
 */
template <typename T>
class SyncWaitPromise {
 public:
  SyncWaitPromise() noexcept = default;

  SyncWaitTask<T> get_return_object() noexcept;

  std::suspend_always initial_suspend() noexcept {
    return {};
  }

  struct FinalAwaiter {
    bool await_ready() noexcept {
      return false;
    }

    void await_suspend(std::coroutine_handle<SyncWaitPromise<T>> h) noexcept {
      h.promise().state_->notify();
    }

    void await_resume() noexcept {
    }
  };

  FinalAwaiter final_suspend() noexcept {
    return {};
  }

  void unhandled_exception() noexcept {
    result_.template emplace<2>(std::current_exception());
  }

  template <typename U>
    requires std::convertible_to<U, T>
  void return_value(U&& value) noexcept(
      std::is_nothrow_constructible_v<T, U&&>) {
    result_.template emplace<1>(std::forward<U>(value));
  }

  T&& result() && {
    if (result_.index() == 2) {
      std::rethrow_exception(std::get<2>(result_));
    }
    assert(result_.index() == 1);
    return std::get<1>(std::move(result_));
  }

  SyncWaitState* state_{nullptr};

 private:
  // Index 0: not yet set (monostate), 1: value, 2: exception.
  std::variant<std::monostate, T, std::exception_ptr> result_;
};

/**
 * Promise type for SyncWaitTask<void>.
 */
template <>
class SyncWaitPromise<void> {
 public:
  SyncWaitPromise() noexcept = default;

  SyncWaitTask<void> get_return_object() noexcept;

  std::suspend_always initial_suspend() noexcept {
    return {};
  }

  struct FinalAwaiter {
    bool await_ready() noexcept {
      return false;
    }

    void await_suspend(
        std::coroutine_handle<SyncWaitPromise<void>> h) noexcept {
      // The coroutine is fully suspended at this point. After we call
      // notify(), the waiting thread may destroy the coroutine frame.
      // Since await_suspend returns void, the current thread simply
      // returns to its caller (the thread pool worker loop) without
      // touching the frame again.
      h.promise().state_->notify();
    }

    void await_resume() noexcept {
    }
  };

  FinalAwaiter final_suspend() noexcept {
    return {};
  }

  void unhandled_exception() noexcept {
    exception_ = std::current_exception();
  }

  void return_void() noexcept {
  }

  void result() {
    if (exception_) {
      std::rethrow_exception(exception_);
    }
  }

  SyncWaitState* state_{nullptr};

 private:
  std::exception_ptr exception_;
};

/**
 * Coroutine return type for sync_wait's wrapper coroutine.
 *
 * Unlike Task<T>, SyncWaitTask's final_suspend notifies a SyncWaitState
 * after the coroutine is fully suspended. This eliminates the race between
 * the worker thread (still inside the coroutine body) and the waiting
 * thread (destroying the coroutine frame after being notified).
 */
template <typename T>
class SyncWaitTask {
 public:
  using promise_type = SyncWaitPromise<T>;
  using handle_type = std::coroutine_handle<promise_type>;

  explicit SyncWaitTask(handle_type handle) noexcept
      : handle_(handle) {
  }

  ~SyncWaitTask() {
    if (handle_) {
      handle_.destroy();
    }
  }

  SyncWaitTask(SyncWaitTask&& other) noexcept
      : handle_(std::exchange(other.handle_, nullptr)) {
  }

  SyncWaitTask(const SyncWaitTask&) = delete;
  SyncWaitTask& operator=(const SyncWaitTask&) = delete;
  SyncWaitTask& operator=(SyncWaitTask&&) = delete;

  handle_type handle() const noexcept {
    return handle_;
  }

 private:
  handle_type handle_;
};

// Implement get_return_object after SyncWaitTask is fully defined.
template <typename T>
SyncWaitTask<T> SyncWaitPromise<T>::get_return_object() noexcept {
  return SyncWaitTask<T>{
      std::coroutine_handle<SyncWaitPromise<T>>::from_promise(*this)};
}

inline SyncWaitTask<void> SyncWaitPromise<void>::get_return_object() noexcept {
  return SyncWaitTask<void>{
      std::coroutine_handle<SyncWaitPromise<void>>::from_promise(*this)};
}

/**
 * Wrapper coroutine that awaits a Task<T>. The SyncWaitTask's
 * final_suspend handles notification — no Guard/scope-guard needed.
 */
template <typename T>
SyncWaitTask<T> sync_wait_coro(Task<T> task) {
  if constexpr (std::is_void_v<T>) {
    co_await std::move(task);
  } else {
    co_return co_await std::move(task);
  }
}

}  // namespace detail

/**
 * Block the calling thread until the given Task<void> completes.
 * Rethrows any exception from the task.
 *
 * @param task The task to wait on.
 * @param tp Optional thread pool. When provided, the calling thread
 *   participates in cooperative scheduling (processes queued tasks while
 *   waiting), preventing deadlocks when called from a pool thread. When
 *   nullptr, uses a bare condition variable wait (safe only from non-pool
 *   threads).
 */
inline void sync_wait(Task<void> task, ThreadPool* tp = nullptr) {
  detail::SyncWaitState state;
  auto wrapper = detail::sync_wait_coro(std::move(task));
  wrapper.handle().promise().state_ = &state;
  wrapper.handle().resume();  // Start the wrapper coroutine.

  if (tp) {
    // Pool-safe: process tasks while waiting (same as parallel_for).
    tp->wait_until([&state] {
      std::lock_guard lock(state.mutex);
      return state.done;
    });
  } else {
    // Non-pool thread: plain block.
    state.wait();
  }

  wrapper.handle().promise().result();  // Rethrow exception if any.
}

/**
 * Block the calling thread until the given Task<T> completes.
 * Returns the result or rethrows any exception.
 *
 * @param task The task to wait on.
 * @param tp Optional thread pool for cooperative scheduling (see above).
 */
template <typename T>
  requires(!std::is_void_v<T>)
T sync_wait(Task<T> task, ThreadPool* tp = nullptr) {
  detail::SyncWaitState state;
  auto wrapper = detail::sync_wait_coro<T>(std::move(task));
  wrapper.handle().promise().state_ = &state;
  wrapper.handle().resume();

  if (tp) {
    tp->wait_until([&state] {
      std::lock_guard lock(state.mutex);
      return state.done;
    });
  } else {
    state.wait();
  }

  return std::move(wrapper.handle().promise()).result();
}

}  // namespace tiledb::common

#endif  // TILEDB_COROUTINE_SYNC_WAIT_H
