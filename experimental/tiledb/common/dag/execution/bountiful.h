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
 *   - The bountiful scheduler does not wrap nodes up as tasks and does not
 * manage their state of execution.  Rather, nodes are wrapped up in a lambda
 * that invokes the `run` method of the node.
 *
 *   - The bountiful scheduler runs nodes lazily.  They are not launched until
 * `sync_wait_all` is invoked.
 *
 *   - The bountiful scheduler is assumed to be used in conjunction with an
 *     AsyncPolicy (a policy that does its own synchronization).
 *
 * More complete documentation about the generic interaction between schedulers
 * and item movers can can be found in the docs subdirectory.
 *
 * @todo Refactor policies in policies.h to be specific to bountiful scheduler.
 * @todo Implement with Task wrapper ?
 *
 */

#ifndef TILEDB_DAG_BOUNTIFUL_H
#define TILEDB_DAG_BOUNTIFUL_H

#include <condition_variable>
#include <future>
#include <iostream>
#include <mutex>
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

/**
 * @brief A scheduler that launches every task on its own thread and uses
 * standard library mechanisms for synchronization (i.e.,
 * std::condition_variable) The corresponding functionality is provided in
 * policies.h
 *
 * @tparam Node The type of node being scheduled.
 */
template <class Node>
class BountifulScheduler {
  /* Flag to indicate to enable debugging in the scheduler. */
  std::atomic<bool> debug_{false};

  /* ********************************* */
  /*                API                */
  /* ********************************* */

 public:
  /**
   * @brief Constructor.
   *
   * @param num_threads
   */
  explicit BountifulScheduler([[maybe_unused]] size_t num_threads = 0) {
  }

  ~BountifulScheduler() {
    // Wait for all tasks to complete.
    for (auto&& f : futures_) {
      f.get();
    }
    futures_.clear();
    nodes_.clear();
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
  bool debug_enabled() {
    return debug_.load();
  }

  /**
   * @brief Submit a task to the scheduler.
   *
   * @param node The task to submit.
   */
  void submit(Node&& n) {
    if (debug_enabled()) {
      std::cout << "Submitting node " << n->id() << std::endl;
    }
    nodes_.emplace_back(std::move(n));
  }

  /**
   * @brief Wait for all running tasks to complete.  Since the bountiful
   * scheduler lazily launches its task, they are launched here.
   */
  void sync_wait_all() {
    if (debug_enabled()) {
      std::cout << "Starting sync_wait_all()\n";
      std::cout << "About to launch all tasks\n";
    }

    /*
     * Launch all tasks.
     */
    for (auto&& n : nodes_) {
      auto f = std::async(std::launch::async, [this, n]() mutable {
        if (debug_enabled()) {
          std::cout << "Running node " << n->id() << std::endl;
        }

        n->run();

        if (debug_enabled()) {
          std::cout << "Completed node " << n->id() << std::endl;
        }
      });
      futures_.emplace_back(std::move(f));
    }

    /*
     * Then we wait for the worker threads to complete.
     *
     */
    for (auto&& f : futures_) {
      try {
        f.get();
      } catch (const std::exception& e) {
        std::cout << "Exception caught in sync_wait_all: " << e.what()
                  << std::endl;
      }
    }
    futures_.clear();
  }

 private:
  /* Container for all nodes of the submitted task graph. */
  std::vector<Node> nodes_;

  /* Container of all futures corresponding to the launched tasks. */
  std::vector<std::future<void>> futures_;
};

}  // namespace tiledb::common
#endif  // TILEDB_DAG_BOUNTIFUL_H
