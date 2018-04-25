/**
 * @file   thread_pool.h
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
 * This file declares the ThreadPool class.
 */

#ifndef TILEDB_THREAD_POOL_H
#define TILEDB_THREAD_POOL_H

#include <condition_variable>
#include <future>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

#include "tiledb/sm/misc/status.h"

namespace tiledb {
namespace sm {

/**
 * Thread pool class.
 */
class ThreadPool {
 public:
  /** Constructor. */
  ThreadPool();

  /** Destructor. */
  ~ThreadPool();

  /**
   * Initialize the thread pool.
   *
   * @param num_threads Number of threads to create (default 1).
   * @return Status
   */
  Status init(uint64_t num_threads = 1);

  /**
   * Cancel all queued tasks, causing them to return error statuses.
   * This does not terminate the threads. Cancelled tasks are guaranteed not to
   * begin execution of their tasks, as currently running tasks are not able to
   * be cancelled (only queued tasks).
   */
  void cancel_all_tasks();

  /**
   * Enqueue a new task to be executed by a thread.
   *
   * @param function Task function to execute.
   * @return Future for the return value of the task.
   */
  std::future<Status> enqueue(const std::function<Status()>& function);

  /**
   * Enqueue a new task to be executed by a thread with a custom callback
   * made if the task is cancelled before it can execute.
   *
   * Note: the on_cancel callback is made from a thread in the pool.
   *
   * @param function Task function to execute.
   * @param on_cancel Cancellation callback function to make on cancel.
   * @return Future for the return value of the task.
   */
  std::future<Status> enqueue(
      const std::function<Status()>& function,
      const std::function<void()>& on_cancel);

  /** Return the number of threads in this pool. */
  uint64_t num_threads() const;

  /**
   * Wait on all the given tasks to complete.
   *
   * @param tasks Task list to wait on.
   * @return True if all tasks returned Status::Ok, false otherwise.
   */
  bool wait_all(std::vector<std::future<Status>>& tasks);

  /**
   * Wait on all the given tasks to complete, return a vector of their return
   * Status.
   *
   * @param tasks Task list to wait on
   * @return Vector of each task's Status.
   */
  std::vector<Status> wait_all_status(std::vector<std::future<Status>>& tasks);

 private:
  std::mutex queue_mutex_;

  std::condition_variable queue_cv_;

  bool should_cancel_;

  bool should_terminate_;

  std::queue<std::packaged_task<Status(bool)>> task_queue_;

  std::vector<std::thread> threads_;

  /** Terminate the threads in the thread pool. */
  void terminate();

  static void worker(ThreadPool& pool);
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_THREAD_POOL_H