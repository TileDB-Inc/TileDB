/**
 * @file   s3_thread_pool_executor.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2019-2021 TileDB, Inc.
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

#include "tiledb/common/status.h"
#include "tiledb/sm/filesystem/s3_thread_pool_executor.h"

using namespace tiledb::common;

namespace tiledb {
namespace sm {

S3ThreadPoolExecutor::S3ThreadPoolExecutor(ThreadPool* const thread_pool)
    : thread_pool_(thread_pool)
    , state_(State::RUNNING)
    , outstanding_tasks_(0) {
  assert(thread_pool_);
}

S3ThreadPoolExecutor::~S3ThreadPoolExecutor() {
  assert(state_ == State::STOPPED);
  assert(outstanding_tasks_ == 0);
}

void S3ThreadPoolExecutor::Stop() {
  std::unique_lock<std::mutex> lock_guard(lock_);

  if (state_ != State::RUNNING)
    return;

  state_ = State::STOPPING;
  while (outstanding_tasks_ != 0)
    cv_.wait(lock_guard);
  state_ = State::STOPPED;
}

bool S3ThreadPoolExecutor::SubmitToThread(std::function<void()>&& fn) {
  auto wrapped_fn = [this, fn]() {
    fn();

    std::unique_lock<std::mutex> lock_guard(lock_);
    if (--outstanding_tasks_ == 0)
      cv_.notify_all();
  };

  std::unique_lock<std::mutex> lock_guard(lock_);
  if (state_ != State::RUNNING) {
    return false;
  }
  ++outstanding_tasks_;
  lock_guard.unlock();

  ThreadPool::Task task = thread_pool_->execute(std::move(wrapped_fn));

  return task.valid();
}

}  // namespace sm
}  // namespace tiledb

#endif  // HAVE_S3
