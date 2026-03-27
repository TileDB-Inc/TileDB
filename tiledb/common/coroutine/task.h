/**
 * @file tiledb/common/coroutine/task.h
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
 * This file defines a lazy coroutine Task<T> type. A Task represents an
 * asynchronous computation that produces a value of type T. It is lazy: the
 * coroutine body does not start executing until the Task is co_awaited.
 */

#ifndef TILEDB_COROUTINE_TASK_H
#define TILEDB_COROUTINE_TASK_H

#include <cassert>
#include <coroutine>
#include <exception>
#include <type_traits>
#include <utility>
#include <variant>

namespace tiledb::common {

template <typename T = void>
class Task;

namespace detail {

/**
 * Promise type for Task<T> where T is non-void.
 */
template <typename T>
class TaskPromise {
 public:
  TaskPromise() noexcept = default;

  Task<T> get_return_object() noexcept;

  std::suspend_always initial_suspend() noexcept {
    return {};
  }

  /**
   * Final suspend awaiter that resumes the continuation (the coroutine that
   * co_awaited this task).
   */
  struct FinalAwaiter {
    bool await_ready() noexcept {
      return false;
    }

    std::coroutine_handle<> await_suspend(
        std::coroutine_handle<TaskPromise> h) noexcept {
      auto& promise = h.promise();
      if (promise.continuation_) {
        return promise.continuation_;
      }
      return std::noop_coroutine();
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

  T& result() & {
    if (result_.index() == 2) {
      std::rethrow_exception(std::get<2>(result_));
    }
    assert(result_.index() == 1);
    return std::get<1>(result_);
  }

  T&& result() && {
    if (result_.index() == 2) {
      std::rethrow_exception(std::get<2>(result_));
    }
    assert(result_.index() == 1);
    return std::get<1>(std::move(result_));
  }

  void set_continuation(std::coroutine_handle<> continuation) noexcept {
    continuation_ = continuation;
  }

 private:
  std::coroutine_handle<> continuation_;

  // Index 0: not yet set (monostate), 1: value, 2: exception.
  std::variant<std::monostate, T, std::exception_ptr> result_;
};

/**
 * Promise type specialization for Task<void>.
 */
template <>
class TaskPromise<void> {
 public:
  TaskPromise() noexcept = default;

  Task<void> get_return_object() noexcept;

  std::suspend_always initial_suspend() noexcept {
    return {};
  }

  struct FinalAwaiter {
    bool await_ready() noexcept {
      return false;
    }

    std::coroutine_handle<> await_suspend(
        std::coroutine_handle<TaskPromise<void>> h) noexcept {
      auto& promise = h.promise();
      if (promise.continuation_) {
        return promise.continuation_;
      }
      return std::noop_coroutine();
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

  void set_continuation(std::coroutine_handle<> continuation) noexcept {
    continuation_ = continuation;
  }

 private:
  std::coroutine_handle<> continuation_;
  std::exception_ptr exception_;
};

}  // namespace detail

/**
 * A lazy coroutine task that produces a value of type T.
 *
 * The coroutine body does not start executing until the Task is co_awaited.
 * This allows composing coroutines without eager execution, and ensures the
 * caller controls when work begins.
 *
 * Usage:
 *   Task<int> compute_value() {
 *     co_return 42;
 *   }
 *
 *   Task<void> caller() {
 *     int v = co_await compute_value();
 *   }
 */
template <typename T>
class [[nodiscard]] Task {
 public:
  using promise_type = detail::TaskPromise<T>;
  using handle_type = std::coroutine_handle<promise_type>;

  Task() noexcept = default;

  explicit Task(handle_type handle) noexcept
      : handle_(handle) {
  }

  ~Task() {
    if (handle_) {
      handle_.destroy();
    }
  }

  // Move-only.
  Task(Task&& other) noexcept
      : handle_(std::exchange(other.handle_, nullptr)) {
  }

  Task& operator=(Task&& other) noexcept {
    if (this != &other) {
      if (handle_) {
        handle_.destroy();
      }
      handle_ = std::exchange(other.handle_, nullptr);
    }
    return *this;
  }

  Task(const Task&) = delete;
  Task& operator=(const Task&) = delete;

  /**
   * Returns whether this task has a valid coroutine handle.
   */
  bool valid() const noexcept {
    return handle_ != nullptr;
  }

  /**
   * Awaiter returned by operator co_await.
   */
  struct Awaiter {
    handle_type handle;

    bool await_ready() noexcept {
      return false;
    }

    std::coroutine_handle<> await_suspend(
        std::coroutine_handle<> continuation) noexcept {
      handle.promise().set_continuation(continuation);
      return handle;
    }

    decltype(auto) await_resume() {
      if constexpr (std::is_void_v<T>) {
        handle.promise().result();
      } else {
        return std::move(handle.promise()).result();
      }
    }
  };

  auto operator co_await() && noexcept {
    assert(handle_);
    return Awaiter{handle_};
  }

  /**
   * Get the coroutine handle. For use by sync_wait and when_all.
   */
  handle_type handle() const noexcept {
    return handle_;
  }

  /**
   * Detach the handle from this Task (caller takes ownership).
   */
  handle_type detach() noexcept {
    return std::exchange(handle_, nullptr);
  }

 private:
  handle_type handle_{nullptr};
};

namespace detail {

template <typename T>
Task<T> TaskPromise<T>::get_return_object() noexcept {
  return Task<T>{std::coroutine_handle<TaskPromise<T>>::from_promise(*this)};
}

inline Task<void> TaskPromise<void>::get_return_object() noexcept {
  return Task<void>{
      std::coroutine_handle<TaskPromise<void>>::from_promise(*this)};
}

}  // namespace detail

}  // namespace tiledb::common

#endif  // TILEDB_COROUTINE_TASK_H
