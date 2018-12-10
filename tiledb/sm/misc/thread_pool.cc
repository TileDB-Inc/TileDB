/**
 * @file   thread_pool.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018 TileDB, Inc.
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

#include "tiledb/sm/misc/thread_pool.h"
#include "tiledb/sm/misc/logger.h"

namespace tiledb {
namespace sm {

ThreadPool::ThreadPool() {
  should_cancel_ = false;
  should_terminate_ = false;
}

ThreadPool::~ThreadPool() {
  terminate();
}

Status ThreadPool::init(uint64_t num_threads) {
  Status st = Status::Ok();

  for (uint64_t i = 0; i < num_threads; i++) {
    try {
      threads_.emplace_back([this]() { worker(*this); });
    } catch (const std::exception& e) {
      st = Status::Error(
          "Error allocating thread pool of " + std::to_string(num_threads) +
          " threads; " + e.what());
      LOG_STATUS(st);
      break;
    }
  }

  // Join any created threads on error.
  if (!st.ok()) {
    terminate();
  }

  return st;
}

void ThreadPool::cancel_all_tasks() {
  // Notify workers to dequeue and cancel all tasks.
  {
    std::unique_lock<std::mutex> lck(queue_mutex_);
    should_cancel_ = true;
    queue_cv_.notify_all();
  }

  // Wait for the queue to empty and reset the flag.
  {
    std::unique_lock<std::mutex> lck(queue_mutex_);
    queue_cv_.wait(lck, [this]() { return task_queue_.empty(); });
    should_cancel_ = false;
  }
}

std::future<Status> ThreadPool::enqueue(
    const std::function<Status()>& function) {
  return enqueue(function, []() {});
}

std::future<Status> ThreadPool::enqueue(
    const std::function<Status()>& function,
    const std::function<void()>& on_cancel) {
  if (threads_.empty()) {
    std::promise<Status> error_task;
    error_task.set_value(
        Status::Error("Cannot enqueue task; thread pool has no threads."));
    return error_task.get_future();
  }

  std::packaged_task<Status(bool)> task(
      [function, on_cancel](bool should_cancel) {
        if (should_cancel) {
          on_cancel();
          return Status::Error("Task cancelled before execution.");
        } else {
          return function();
        }
      });
  auto future = task.get_future();

  {
    std::unique_lock<std::mutex> lck(queue_mutex_);
    task_queue_.push(std::move(task));
    queue_cv_.notify_one();
  }

  return future;
}

uint64_t ThreadPool::num_threads() const {
  return threads_.size();
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
  for (auto& future : tasks) {
    if (!future.valid()) {
      LOG_ERROR("Waiting on invalid future.");
      statuses.push_back(Status::Error("Invalid future"));
    } else {
      Status status = future.get();
      if (!status.ok()) {
        LOG_STATUS(status);
      }
      statuses.push_back(status);
    }
  }
  return statuses;
}

void ThreadPool::terminate() {
  {
    std::unique_lock<std::mutex> lck(queue_mutex_);
    if (!task_queue_.empty()) {
      LOG_ERROR("Destroying ThreadPool with outstanding tasks.");
    }
    should_terminate_ = true;
    queue_cv_.notify_all();
  }

  for (auto& t : threads_) {
    t.join();
  }

  threads_.clear();
}

void ThreadPool::worker(ThreadPool& pool) {
  while (true) {
    std::packaged_task<Status(bool)> task;
    bool should_cancel = false;

    {
      // Wait until there's work to do or a message is received.
      std::unique_lock<std::mutex> lck(pool.queue_mutex_);
      pool.queue_cv_.wait(lck, [&pool]() {
        return pool.should_terminate_ || pool.should_cancel_ ||
               !pool.task_queue_.empty();
      });

      if (pool.should_terminate_) {
        break;
      } else if (!pool.task_queue_.empty()) {
        task = std::move(pool.task_queue_.front());
        pool.task_queue_.pop();
      }

      // Keep waking up threads until the cancel flag is reset. This also wakes
      // up the thread that will reset the cancel flag when appropriate.
      if (pool.should_cancel_) {
        pool.queue_cv_.notify_all();
      }

      // Save the cancellation flag.
      should_cancel = pool.should_cancel_;
    }

    if (task.valid()) {
      task(should_cancel);
    }
  }
}

}  // namespace sm
}  // namespace tiledb