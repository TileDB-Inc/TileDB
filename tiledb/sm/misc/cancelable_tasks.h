/**
 * @file   cancelable_tasks.h
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
 * This file declares the CancelableTasks class.
 */

#ifndef TILEDB_CANCELABLE_TASKS_H
#define TILEDB_CANCELABLE_TASKS_H

#include <condition_variable>
#include <functional>
#include <mutex>

#include "tiledb/common/status.h"
#include "tiledb/common/thread_pool/thread_pool.h"

using namespace tiledb::common;

namespace tiledb::sm {

class CancelableTasks {
 public:
  /**
   * Constructor.
   */
  CancelableTasks();

  /**
   * Destructor.
   */
  ~CancelableTasks() = default;

  /**
   * Execute a task on the specified thread pool.
   *
   * @param function Task to be executed.
   * @param function Optional routine to execute on cancelation.
   * @return Task for the return value of the task.
   */
  ThreadPool::Task execute(
      ThreadPool* thread_pool,
      std::function<Status()>&& fn,
      std::function<void()>&& on_cancel = nullptr);

  /**
   * Waits for all pending tasks to cancel. If a task is already running, it
   * will run to completion.
   */
  void cancel_all_tasks();

 private:
  /**
   * The wrapped task decorator. If all tasks have been cancelled, they will
   * short-circuit here with an appropriate return value.
   *
   * @param function Task to be executed.
   * @param function Optional routine to execute on cancelation.
   * @return Status The returned status from 'fn', or a non-OK status if tasks
   * were cancelled.
   */
  Status fn_wrapper(
      const std::function<Status()>& fn,
      const std::function<void()>& on_cancel);

  /** The number of outstanding tasks */
  uint32_t outstanding_tasks_;

  /** Protects `outstanding_tasks_` */
  std::mutex outstanding_tasks_mutex_;

  /** For signal-and-waiting on `outstanding_tasks_` */
  std::condition_variable outstanding_tasks_cv_;

  /** True when all outstanding tasks should be cancelled */
  bool should_cancel_;
};

}  // namespace tiledb::sm

#endif  // TILEDB_CANCELABLE_TASKS_H
