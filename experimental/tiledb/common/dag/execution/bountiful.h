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
 * This scheduler is assumed to be used in conjunction with an AsyncPolicy (a
 * policy that does its own synchronization).
 */

#ifndef TILEDB_DAG_BOUNTIFUL_H
#define TILEDB_DAG_BOUNTIFUL_H

#include <future>
#include <iostream>
#include <vector>
#include <thread>
#include "experimental/tiledb/common/dag/state_machine/fsm.h"
#include "experimental/tiledb/common/dag/state_machine/item_mover.h"
#include "experimental/tiledb/common/dag/state_machine/policies.h"
#include "experimental/tiledb/common/dag/execution/task_state_machine.h"

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
  void submit(Node&& n)  {
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
    for (auto&& f: futures_) {
      f.get();
    }
  }

};

#if 0

template <class Task>
class BountifulSchedulerPolicy
    : public SchedulerStateMachine<BountifulSchedulerPolicy<Task>> {
  using state_machine_type = SchedulerStateMachine<BountifulSchedulerPolicy<Task>>;
  using lock_type = typename state_machine_type::lock_type;

private:
  std::vector<Task> submitted_tasks_;
  std::vector<Task> futures_;

 public:
  using task_type =
      typename SchedulerTraits<BountifulSchedulerPolicy<Task>>::task_type;
  using task_handle_type =
      typename SchedulerTraits<BountifulSchedulerPolicy<Task>>::task_handle_type;

  ~BountifulSchedulerPolicy() {
    if (this->debug())
      std::cout << "policy destructor\n";
  }

  void on_create(task_handle_type& task) {
    if (this->debug())
      std::cout << "calling on_create"
                << "\n";

  }

  void on_stop_create(task_handle_type&) {
    if (this->debug())
      std::cout << "calling on_stop_create"
                << "\n";
  }

  void on_make_runnable(task_handle_type& task) {
    if (this->debug())
      std::cout << "calling on_make_runnable"
                << "\n";

  }

  void on_stop_runnable(task_handle_type&) {
    if (this->debug())
      std::cout << "calling on_stop_runnable"
                << "\n";
  }

  void on_make_running(task_handle_type& task) {
    if (this->debug())
      std::cout << "calling on_make_runninge"
                << "\n";

  }

  void on_stop_running(task_handle_type& task) {
    if (this->debug())
      std::cout << "calling on_stop_running"
                << "\n";

  }

  void on_make_waiting(task_handle_type& task) {
    if (this->debug())
      std::cout << "calling on_make_waiting"
                << "\n";

  }

  void on_stop_waiting(task_handle_type& task) {
    if (this->debug())
      std::cout << "calling on_stop_waiting"
                << "\n";
  }

  void on_terminate(task_handle_type& task) {
    if (this->debug())
      std::cout << "calling on_terminate"
                << "\n";
  }

  void launch() {
    if (this->debug()) {
      this->dump_queue_state("Launching start");
    }

    while (true) {
      auto s = submission_queue_.try_pop();
      if (!s)
        break;
      if (this->debug())
        (*s)->dump_task_state("Admitting");

      this->task_admit(*s);
    }
    if (this->debug()) {
      this->dump_queue_state("Launching end");
    }
  }

  auto get_runnable_task() {

    auto val = runnable_queue_.try_pop();
    while (val && ((*val)->task_state() == TaskState::terminated)) {
      val = runnable_queue_.try_pop();
    }
    return val;
  }

  void dump_queue_state(const std::string& msg = "") {
    std::string preface = (msg != "" ? msg + "\n" : "");

    std::cout << preface + "    runnable_queue_.size() = " +
                     std::to_string(runnable_queue_.size()) + "\n" +
                     "    running_set_.size() = " +
                     std::to_string(running_set_.size()) + "\n" +
                     "    waiting_set_.size() = " +
                     std::to_string(waiting_set_.size()) + "\n" +
                     "    finished_queue_.size() = " +
                     std::to_string(finished_queue_.size()) + "\n" + "\n";
  }
};
#endif

}



#if 0
namespace tiledb::common {

template <class Node>
class BountifulScheduler {
 public:
  BountifulScheduler() = default;
  ~BountifulScheduler() = default;

  std::vector<std::future<void>> futures_{};
  std::vector<std::function<void()>> tasks_{};

  template <class C = Node>
  void submit(Node& n, std::enable_if_t<std::is_same_v<decltype(n.resume()), void>, void*> = nullptr)  {
    tasks_.emplace_back([&n]() {

    auto mover = n.get_mover();
    if (mover->debug_enabled()) {
      std::cout << "producer starting run on " << n.get_mover()
                << std::endl;
    }

    while (!mover->is_stopping()) {
      n.resume();
    }
    });
  }

  template <class C = Node>
  void submit(Node&& n, std::enable_if_t<std::is_same_v<decltype(n.resume()), void>, void*> = nullptr) {
    futures_.emplace_back(std::async(std::launch::async, [&n]() { n.resume(); }));    
  }

  template <class C = Node>
  void submit(C& n, std::enable_if_t<(!std::is_same_v<decltype(n.resume()), void> && std::is_same_v<decltype(n.run()), void>), void*> = nullptr)  {
    futures_.emplace_back(std::async(std::launch::async, [&n]() { n.run(); }));
  }

  template <class C = Node>
  void submit(C& n, std::enable_if_t<(std::is_same_v<decltype(n.resume()), void> && std::is_same_v<decltype(n.run()), void>), void*> = nullptr)  {
    futures_.emplace_back(std::async(std::launch::async, [&n]() { n.run(); }));
  }


  auto sync_wait_all() {
    for (auto&& t : tasks_) {
      futures_.emplace_back(std::async(std::launch::async, t));
    }
    for (auto&& t : futures_) {
       t.get();
    }
  }
};



template <class Node>
class ThrowCatchScheduler : public ThrowCatchSchedulerPolicy<ThrowCatchTask<Node>> {
  using Scheduler = ThrowCatchScheduler<Node>;
  using Policy = ThrowCatchSchedulerPolicy<ThrowCatchTask<Node>>;


  /** Deleted default constructor */
  // ThrowCatchScheduler() = delete;
  ThrowCatchScheduler(const ThrowCatchScheduler&) = delete;

  /** Destructor. */
  ~ThrowCatchScheduler() {
    if (this->debug())
      std::cout << "scheduler destructor\n";
  }


  /* ********************************* */
  /*                API                */
  /* ********************************* */


  std::atomic<bool> debug_{false};

  void enable_debug() {
    debug_.store(true);
  }
  void disable_debug() {
    debug_.store(false);
  }

  bool debug() {
    return debug_.load();
  }

  void submit(Node&& n) {
  }

  void sync_wait_all() {

  };
}  // namespace tiledb::common
#endif


#endif  // TILEDB_DAG_BOUNTIFUL_H
