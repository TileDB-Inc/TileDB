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
 * scheduled job on its own thread, using `std::async`.  The return futures are
 * saved.  When the scheduler destructor is called, all tasks are waited on
 * (using future::get until they complete).
 *
 * Notes: 
 *
 *   - The bountiful scheduler does not wrap nodes up as tasks and does not manage their
 *      state of execution.  Rather, nodes are wrapped up in a lambda that invokes the
 *     `run` method of the node.
 *
 *   - The bountiful scheduler runs nodes eagerly -- as soon as they are submitted,
 *     rather than lazily.  This differs from behavior of other schedulers.
 *
 *   - The bountiful scheduler is assumed to be used in conjunction with an
 *     AsyncPolicy (a policy that does its own synchronization).
 *
 * @todo Implement lazy submission of nodes submitted to the bountiful scheduler.
 * @todo Refactor policies in policies.h to be specific to bountiful scheduler.
 * @todo Implement with Task wrapper ?
 *
 */

#ifndef TILEDB_DAG_BOUNTIFUL_H
#define TILEDB_DAG_BOUNTIFUL_H

#include <future>
#include <iostream>
#include <thread>
#include <vector>
#include "experimental/tiledb/common/dag/execution/task_state_machine.h"
#include "experimental/tiledb/common/dag/state_machine/fsm.h"
#include "experimental/tiledb/common/dag/state_machine/item_mover.h"
#include "experimental/tiledb/common/dag/state_machine/policies.h"

namespace tiledb::common {

template <class T>
using BountifulMover3 = ItemMover<AsyncPolicy, three_stage, T>;
template <class T>
using BountifulMover2 = ItemMover<AsyncPolicy, two_stage, T>;
template <class T>
using UnifiedBountifulMover3 = ItemMover<UnifiedAsyncPolicy, three_stage, T>;
template <class T>
using UnifiedBountifulMover2 = ItemMover<UnifiedAsyncPolicy, two_stage, T>;

template <class Node>
class BountifulScheduler;

template <class Task>
class BountifulSchedulerPolicy;

template <typename T>
struct SchedulerTraits<BountifulSchedulerPolicy<T>> {
  using task_type = T;
  using task_handle_type = T;
};

template <class Node>
class BountifulScheduler {
  std::vector<std::future<void>> futures_;

  /* ********************************* */
  /*                API                */
  /* ********************************* */

 public:
  std::atomic<bool> debug_{false};

  BountifulScheduler([[maybe_unused]] size_t num_threads = 0) {
  }

  /**
   * @brief Turn on debug mode.
   */
  void enable_debug() {
    debug_.store(true);
  }

  /**
   * @brief Turn off debug mode.
   */
  void disable_debug() {
    debug_.store(false);
  }

  /**
   * @brief Get state of debug mode.
   */
  bool debug() {
    return debug_.load();
  }

  /**
   * @brief Submit a task to the scheduler.
   *
   * @param node The task to submit.
   */
  void submit(Node&& n) {
    if (debug()) {
      std::cout << "Submitting node " << n->id() << std::endl;
    }
    auto f = std::async(std::launch::async, [this, n]() mutable {
      if (debug()) {
        std::cout << "Running node " << n->id() << std::endl;
      }

      n->run();

      if (debug()) {
        std::cout << "Completed node " << n->id() << std::endl;
      }
    });
    futures_.push_back(std::move(f));
  }

  /**
   * @brief Wait for all running tasks to complete
   */
  void sync_wait_all() {
    for (auto&& f : futures_) {
      f.get();
    }
  }
};

}  // namespace tiledb::common
#endif  // TILEDB_DAG_BOUNTIFUL_H
