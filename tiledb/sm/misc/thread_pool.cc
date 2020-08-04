/**
 * @file   thread_pool.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2020 TileDB, Inc.
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
 * This file defines the ThreadPool class.
 */

#include <cassert>

#include "tiledb/sm/misc/logger.h"
#include "tiledb/sm/misc/thread_pool.h"

namespace tiledb {
namespace sm {

ThreadPool::ThreadPool()
    : concurrency_level_(0)
    , should_terminate_(false) {
}

ThreadPool::~ThreadPool() {
  terminate();
}

Status ThreadPool::init(const uint64_t concurrency_level) {
  if (concurrency_level == 0) {
    return Status::ThreadPoolError(
        "Unable to initialize a thread pool with a concurrency level of 0.");
  }

  Status st = Status::Ok();

  // We allocate one less thread than `concurrency_level` because
  // the `wait_all*()` routines may service tasks concurrently with
  // the worker threads.
  const uint64_t num_threads = concurrency_level - 1;
  for (uint64_t i = 0; i < num_threads; i++) {
    try {
      threads_.emplace_back([this]() { worker(*this); });
    } catch (const std::exception& e) {
      st = Status::ThreadPoolError(
          "Error initializing thread pool of concurrencylevel " +
          std::to_string(concurrency_level) + "; " + e.what());
      LOG_STATUS(st);
      break;
    }
  }

  if (st.ok()) {
    concurrency_level_ = concurrency_level;
  } else {
    // Join any created threads on error.
    terminate();
  }

  return st;
}

std::future<Status> ThreadPool::execute(std::function<Status()>&& function) {
  if (concurrency_level_ == 0) {
    std::future<Status> invalid_future;
    LOG_ERROR("Cannot execute task; thread pool uninitialized.");
    return invalid_future;
  }

  std::unique_lock<std::mutex> lck(task_stack_mutex_);

  if (should_terminate_) {
    std::future<Status> invalid_future;
    LOG_ERROR("Cannot execute task; thread pool has terminated.");
    return invalid_future;
  }

  std::packaged_task<Status()> task(std::move(function));
  auto future = task.get_future();

  // When we have a concurrency level > 1, we will have at least
  // one thread available to pick up the task. For a concurrency
  // level == 1, we have no worker threads available. When no
  // worker threads are available, execute the task on this
  // thread.
  if (concurrency_level_ > 1) {
    task_stack_.push(std::move(task));
    task_stack_cv_.notify_one();
    lck.unlock();
  } else {
    lck.unlock();
    task();
  }

  assert(future.valid());
  return future;
}

uint64_t ThreadPool::concurrency_level() const {
  return concurrency_level_;
}

Status ThreadPool::wait_all(std::vector<std::future<Status>>& tasks) {
  auto statuses = wait_all_status(tasks);
  for (auto& st : statuses) {
    if (!st.ok()) {
      return st;
    }
  }
  return Status::Ok();
}

std::vector<Status> ThreadPool::wait_all_status(
    std::vector<std::future<Status>>& tasks) {
  std::vector<Status> statuses;
  for (auto& task : tasks) {
    if (!task.valid()) {
      LOG_ERROR("Waiting on invalid task future.");
      statuses.push_back(Status::ThreadPoolError("Invalid task future"));
    } else {
      Status status = wait_or_work(std::move(task));
      if (!status.ok()) {
        LOG_STATUS(status);
      }
      statuses.push_back(status);
    }
  }
  return statuses;
}

Status ThreadPool::wait_or_work(std::future<Status>&& task) {
  do {
    // If `task` has completed, we're done.
    const std::future_status task_status =
        task.wait_for(std::chrono::seconds(0));
    if (task_status == std::future_status::ready) {
      break;
    }

    // The task has not completed.
    assert(task_status != std::future_status::deferred);
    assert(task_status == std::future_status::timeout);

    // Lock the `task_stack_` to receive the next task to work on.
    std::unique_lock<std::mutex> lck(task_stack_mutex_);

    // If there are no pending tasks, we will wait for `task` to complete.
    if (task_stack_.empty())
      break;

    // Pull the next task off of the task stack. We specifically use a LIFO
    // ordering to prevent overflowing the call stack.
    std::packaged_task<Status()> inner_task = std::move(task_stack_.top());
    task_stack_.pop();

    // We're done mutating `task_stack_`.
    lck.unlock();

    // Execute the inner task.
    if (inner_task.valid())
      inner_task();
  } while (true);

  return task.get();
}

void ThreadPool::terminate() {
  {
    std::unique_lock<std::mutex> lck(task_stack_mutex_);
    should_terminate_ = true;
    task_stack_cv_.notify_all();
  }

  for (auto& t : threads_) {
    t.join();
  }

  threads_.clear();
}

void ThreadPool::worker(ThreadPool& pool) {
  while (true) {
    std::packaged_task<Status()> task;

    {
      // Wait until there's work to do.
      std::unique_lock<std::mutex> lck(pool.task_stack_mutex_);
      pool.task_stack_cv_.wait(lck, [&pool]() {
        return pool.should_terminate_ || !pool.task_stack_.empty();
      });

      if (!pool.task_stack_.empty()) {
        task = std::move(pool.task_stack_.top());
        pool.task_stack_.pop();
      }
    }

    if (task.valid())
      task();

    if (pool.should_terminate_)
      break;
  }
}

}  // namespace sm
}  // namespace tiledb
