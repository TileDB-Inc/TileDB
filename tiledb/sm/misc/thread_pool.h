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
  /**
   * Constructor.
   *
   * @param num_threads Number of threads to create (default 1).
   */
  explicit ThreadPool(uint64_t num_threads = 1);

  /** Destructor. */
  ~ThreadPool();

  /**
   * Enqueue a new task to be executed by a thread.
   *
   * @param function Task function to execute.
   * @return Future for the return value of the task.
   */
  std::future<Status> enqueue(const std::function<Status()>& function);

  /** Return the number of threads in this pool. */
  uint64_t num_threads() const;

  /**
   * Wait on all the given tasks to complete.
   *
   * @param tasks Task list to wait on.
   * @return True if all tasks returned Status::Ok, false otherwise.
   */
  bool wait_all(std::vector<std::future<Status>>& tasks);

 private:
  std::mutex queue_mutex_;

  std::condition_variable queue_cv_;

  bool should_terminate_;

  std::queue<std::packaged_task<Status()>> task_queue_;

  std::vector<std::thread> threads_;

  static void worker(ThreadPool& pool);
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_THREAD_POOL_H