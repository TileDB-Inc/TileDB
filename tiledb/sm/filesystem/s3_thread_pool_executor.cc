/**
 * @file   s3_thread_pool_executor.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2019-2020 TileDB, Inc.
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
    : thread_pool_(thread_pool)
    , state_(State::RUNNING) {
  assert(thread_pool_);
}

S3ThreadPoolExecutor::~S3ThreadPoolExecutor() {
  assert(state_ == State::STOPPED);
  assert(tasks_.empty());
}

Status S3ThreadPoolExecutor::Stop() {
  if (state_ == State::STOPPED)
    return Status::Ok();

  std::unique_lock<std::recursive_mutex> lock_guard(lock_);
  assert(state_ == State::RUNNING);
  state_ = State::STOPPING;
  std::vector<ThreadPool::Task<Status>> tasks;
  tasks.reserve(tasks_->size());
  for (auto& task : tasks_)
    tasks.emplace_back(std::move(*task));
  tasks_.clear();
  lock_guard.unlock();

  // Wait for all outstanding tasks to complete.
  const Status st = thread_pool_->wait_all(tasks);

  lock_guard.lock();
  state_ = State::STOPPED;
  lock_guard.unlock();

  return st;
}

bool S3ThreadPoolExecutor::SubmitToThread(std::function<void()>&& fn) {
  std::shared_ptr<ThreadPool::Task<Status>> task_ptr =
      std::make_shared<ThreadPool::Task<Status>>();
  auto wrapped_fn = [this, fn, task_ptr]() -> Status {
    fn();

    std::unique_lock<std::recursive_mutex> lock_guard(lock_);
    assert(state_ != State::STOPPED);
    if (state_ == State::RUNNING) {
      tasks_.erase(task_ptr);
    }
    lock_guard.unlock();
    return Status::Ok();
  };

  std::unique_lock<std::recursive_mutex> lock_guard(lock_);
  if (state_ != State::RUNNING) {
    return false;
  }

  // 'ThreadPool::execute' may invoke 'wrapped_fn' on this thread.
  // Although we are holding 'lock_', it is safe because it is a
  // recursive mutex.
  *task_ptr = thread_pool_->execute(wrapped_fn);
  if (!task_ptr->valid()) {
    return false;
  }

  tasks_.emplace(std::move(task_ptr));
  lock_guard.unlock();

  return true;
}

}  // namespace sm
}  // namespace tiledb

#endif  // HAVE_S3
