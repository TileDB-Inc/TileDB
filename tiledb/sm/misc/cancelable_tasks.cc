/**
 * @file   thread_pool.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2018-2024 TileDB, Inc.
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

#include "tiledb/sm/misc/cancelable_tasks.h"
#include "tiledb/common/thread_pool/thread_pool.h"

using namespace tiledb::common;

namespace tiledb::sm {

CancelableTasks::CancelableTasks()
    : outstanding_tasks_(0)
    , should_cancel_(false) {
}

ThreadPool::Task CancelableTasks::execute(
    ThreadPool* const thread_pool,
    std::function<void()>&& fn,
    std::function<void()>&& on_cancel) {
  ThreadPool::Task task =
      thread_pool->execute(&CancelableTasks::fn_wrapper, this, fn, on_cancel);
  if (task.valid()) {
    std::unique_lock<std::mutex> lck(outstanding_tasks_mutex_);
    ++outstanding_tasks_;
  }

  return task;
}

void CancelableTasks::cancel_all_tasks() {
  {
    std::unique_lock<std::mutex> lck(outstanding_tasks_mutex_);
    should_cancel_ = true;
  }

  // Wait for all outstanding tasks to cancel.
  {
    std::unique_lock<std::mutex> lck(outstanding_tasks_mutex_);
    outstanding_tasks_cv_.wait(
        lck, [this]() { return outstanding_tasks_ == 0; });
    should_cancel_ = false;
  }
}

void CancelableTasks::fn_wrapper(
    const std::function<void()>& fn, const std::function<void()>& on_cancel) {
  std::unique_lock<std::mutex> lck(outstanding_tasks_mutex_);
  if (should_cancel_) {
    if (on_cancel) {
      lck.unlock();
      on_cancel();
      lck.lock();
    }
    if (--outstanding_tasks_ == 0) {
      outstanding_tasks_cv_.notify_all();
    }
    throw TaskException("Task cancelled before execution.");
  } else {
    lck.unlock();
    fn();
    lck.lock();
    --outstanding_tasks_;
    // If 'should_cancel_' became true when the lock was released to execute
    // 'fn', we need to signal the CV.
    if (should_cancel_ && outstanding_tasks_ == 0) {
      outstanding_tasks_cv_.notify_all();
    }
  }
}

}  // namespace tiledb::sm
