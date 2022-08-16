/**
 * @file bountiful.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 * This file defines the "bountiful" thread pool for dag. It launches every
 * scheduled job on its own thread, using `std::async`.  The retuen futures are
 * saved.  When the scheduler destructor is called, all tasks are waited on
 * (using future::get until they complete).
 *
 * This scheduler is assumed to be used in conjunction with an AsyncPolicy (a
 * policy that does its own synchronization).
 */

#ifndef TILEDB_DAG_BOUNTIFUL_H
#define TILEDB_DAG_BOUNTIFUL_H

#include <future>
#include <thread>
#include <vector>

namespace tiledb::common {

class BountifulScheduler {
  std::vector<std::shared_ptr<std::function<void()>>> tasks_;
  std::vector<std::thread>;

  /**
   * Destructor.  Waits for all tasks to finish.
   */
 public:
  BountifulScheduler() {
  }

  /**
   * Schedule a given container of tasks to run.
   *
   * Operates in conjunction with an asynchronous state machine policy (i.e., a
   * policy that does its own wait and notify).
   *
   * The tasks do **not** start running when they are scheduled.  Rather, they
   * begin execution when the `wait` member function is called.
   *
   * The tasks will be scheduled (and executed) in the order they are given.  It
   * is up to the caller to provide the tasks in a sensible order.
   */
  auto schedule(std::vector<std::future<void>>& tasks) {
    for (auto&& t : tasks) {
      tasks_.emplace_back(t);
    }
  }

  /**
   * The `sync_wait` function initiates execution of the tasks in `tasks_` and
   * waits for their completion. The tasks will be run in the order they were
   * originally given.
   *
   * With the bountiful scheduler, every task gets a thread.
   * (You get a thread and you get a thread!  Every task gets a thread!)
   */
  void sync_wait() {
    for (auto&& t : tasks_) {
      threads_.emplace_back(t);
    }
    for (auto&& t : tasks_) {
      t.join();
    }
  }
};

}  // namespace tiledb::common

#endif  // TILEDB_DAG_BOUNTIFUL_H
