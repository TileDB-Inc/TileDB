/**
 * @file   s3_thread_pool_executor.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2019 TileDB, Inc.
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
 * This file defines the S3ThreadPoolExecutor class.
 */

#ifdef HAVE_S3

#include <cassert>

#include "tiledb/sm/filesystem/s3_thread_pool_executor.h"
#include "tiledb/sm/misc/status.h"

namespace tiledb {
namespace sm {

S3ThreadPoolExecutor::S3ThreadPoolExecutor(ThreadPool* const thread_pool)
    : thread_pool_(thread_pool) {
  assert(thread_pool_);
}

S3ThreadPoolExecutor::~S3ThreadPoolExecutor() {
  std::unique_lock<std::mutex> lock_guard(tasks_lock_);
  std::unordered_set<std::shared_ptr<std::future<Status>>> tasks =
      std::move(tasks_);
  tasks_.clear();
  lock_guard.unlock();

  // Wait for all outstanding tasks to complete.
  for (auto& task : tasks) {
    task->wait();
    assert(task->get().ok());
  }
}

bool S3ThreadPoolExecutor::SubmitToThread(std::function<void()>&& fn) {
  std::shared_ptr<std::future<Status>> task_ptr =
      std::make_shared<std::future<Status>>();
  auto wrapped_fn = [this, fn, task_ptr]() -> Status {
    fn();

    std::unique_lock<std::mutex> lock_guard(tasks_lock_);
    // If 'tasks_' is empty, the S3ThreadPoolExecutor instance is destructing.
    if (!tasks_.empty()) {
      tasks_.erase(task_ptr);
    }
    lock_guard.unlock();
    return Status::Ok();
  };

  *task_ptr = thread_pool_->enqueue(wrapped_fn);
  std::unique_lock<std::mutex> lock_guard(tasks_lock_);
  tasks_.emplace(std::move(task_ptr));
  lock_guard.unlock();

  return true;
}

}  // namespace sm
}  // namespace tiledb

#endif  // HAVE_S3
