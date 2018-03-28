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

ThreadPool::ThreadPool(uint64_t num_threads) {
  should_terminate_ = false;
  for (uint64_t i = 0; i < num_threads; i++) {
    threads_.emplace_back([this]() { worker(*this); });
  }
}

ThreadPool::~ThreadPool() {
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
}

std::future<Status> ThreadPool::enqueue(
    const std::function<Status()>& function) {
  std::packaged_task<Status()> task(function);
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

bool ThreadPool::wait_all(std::vector<std::future<Status>>& tasks) {
  bool all_ok = true;
  for (auto& future : tasks) {
    if (!future.valid()) {
      LOG_ERROR("Waiting on invalid future.");
      all_ok = false;
    } else {
      Status status = future.get();
      all_ok &= status.ok();
      if (!status.ok()) {
        LOG_STATUS(status);
      }
    }
  }
  return all_ok;
}

void ThreadPool::worker(ThreadPool& pool) {
  while (true) {
    std::packaged_task<Status()> task;
    {
      std::unique_lock<std::mutex> lck(pool.queue_mutex_);
      pool.queue_cv_.wait(lck, [&pool]() {
        return pool.should_terminate_ || !pool.task_queue_.empty();
      });

      if (pool.should_terminate_) {
        break;
      } else {
        task = std::move(pool.task_queue_.front());
        pool.task_queue_.pop();
      }
    }

    task();
  }
}

}  // namespace sm
}  // namespace tiledb