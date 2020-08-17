/**
 * @file   thread_pool.h
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
 * This file declares the ThreadPool class.
 */

#ifndef TILEDB_THREAD_POOL_H
#define TILEDB_THREAD_POOL_H

#include <condition_variable>
#include <future>
#include <mutex>
#include <stack>
#include <thread>
#include <unordered_map>
#include <vector>

#include "tiledb/sm/misc/status.h"

namespace tiledb {
namespace sm {

/**
 * A recusive-safe thread pool.
 */
class ThreadPool {
 public:
  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  ThreadPool();

  /** Destructor. */
  ~ThreadPool();

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Initialize the thread pool.
   *
   * @param concurrency_level Maximum level of concurrency.
   * @return Status
   */
  Status init(uint64_t concurrency_level = 1);

  /**
   * Schedule a new task to be executed. If the returned `future` object
   * is valid, `function` is guaranteed to execute. The 'function' may
   * execute immediately on the calling thread. To avoid deadlock, `function`
   * should not aquire non-recursive locks held by the calling thread.
   *
   * @param function Task function to execute.
   * @return Future for the return value of the task.
   */
  std::future<Status> execute(std::function<Status()>&& function);

  /** Return the maximum level of concurrency. */
  uint64_t concurrency_level() const;

  /**
   * Wait on all the given tasks to complete. This is safe to call recusively
   * and may execute pending tasks on the calling thread while waiting.
   *
   * @param tasks Task list to wait on.
   * @return Status::Ok if all tasks returned Status::Ok, otherwise the first
   * error status is returned
   */
  Status wait_all(std::vector<std::future<Status>>& tasks);

  /**
   * Wait on all the given tasks to complete, return a vector of their return
   * Status. This is safe to call recusively and may execute pending tasks
   * on the calling thread while waiting.
   *
   * @param tasks Task list to wait on
   * @return Vector of each task's Status.
   */
  std::vector<Status> wait_all_status(std::vector<std::future<Status>>& tasks);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  uint64_t concurrency_level_;

  /** Protects `task_stack_` and `idle_threads_`. */
  std::mutex task_stack_mutex_;

  /** Notifies work threads to check `task_stack_` for work. */
  std::condition_variable task_stack_cv_;

  /** Pending tasks in LIFO ordering. */
  std::stack<std::packaged_task<Status()>> task_stack_;

  /**
   * The number of threads waiting for the `task_stack_` to
   * become non-empty.
   */
  uint64_t idle_threads_;

  /** The worker threads. */
  std::vector<std::thread> threads_;

  /** When true, all pending tasks will remain unscheduled. */
  bool should_terminate_;

  /** Indexes thread ids to the ThreadPool instance they belong to. */
  static std::unordered_map<std::thread::id, ThreadPool*> tp_index_;

  /** Protects 'tp_index_'. */
  static std::mutex tp_index_lock_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /**
   * Waits for `task`, but will execute other tasks from `task_stack_`
   * while waiting. While this may be an performance optimization
   * to perform work on this thread rather than waiting, the primary
   * motiviation is to prevent deadlock when tasks are enqueued recursively.
   */
  Status wait_or_work(std::future<Status>&& task);

  /** Terminate the threads in the thread pool. */
  void terminate();

  /** The worker thread routine. */
  static void worker(ThreadPool& pool);

  // Add indexes from each thread to this instance.
  void add_tp_index();

  // Remove indexes from each thread to this instance.
  void remove_tp_index();

  // Lookup the thread pool instance from the calling thread.
  ThreadPool* lookup_tp();
};

}  // namespace sm
}  // namespace tiledb

#endif  // TILEDB_THREAD_POOL_H
