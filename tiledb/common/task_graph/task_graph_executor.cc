/**
 * @file   task_graph_executor.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2020 TileDB, Inc.
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
 * This file implements class TaskGraphExecutor.
 */

#include "tiledb/common/task_graph/task_graph_executor.h"
#include "tiledb/common/logger.h"

#include <iostream>

using namespace tiledb::common;

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

TaskGraphExecutor::TaskGraphExecutor()
    : tp_(nullptr)
    , task_graph_(nullptr)
    , terminated_(false)
    , task_done_(false)
    , done_(true)
    , last_task_st_(Status::Ok()) {
}

TaskGraphExecutor::TaskGraphExecutor(
    ThreadPool* tp, const tdb_shared_ptr<TaskGraph>& task_graph)
    : tp_(tp)
    , task_graph_(task_graph)
    , terminated_(false)
    , task_done_(false)
    , done_(true)
    , last_task_st_(Status::Ok()) {
}

TaskGraphExecutor::~TaskGraphExecutor() {
}

TaskGraphExecutor::TaskGraphExecutor(const TaskGraphExecutor& tge)
    : TaskGraphExecutor() {
  auto clone = tge.clone();
  swap(clone);
}

TaskGraphExecutor::TaskGraphExecutor(TaskGraphExecutor&& tge)
    : TaskGraphExecutor() {
  swap(tge);
}

TaskGraphExecutor& TaskGraphExecutor::operator=(const TaskGraphExecutor& tge) {
  auto clone = tge.clone();
  swap(clone);
  return *this;
}

TaskGraphExecutor& TaskGraphExecutor::operator=(TaskGraphExecutor&& tge) {
  swap(tge);
  return *this;
}

/* ********************************* */
/*                API                */
/* ********************************* */

Status TaskGraphExecutor::execute() {
  // Error messages
  if (tp_ == nullptr) {
    return LOG_STATUS(Status::TaskGraphExecutorError(
        "Cannot execute task graph; Thread pool is null"));
  }
  if (task_graph_ == nullptr) {
    return LOG_STATUS(Status::TaskGraphExecutorError(
        "Cannot execute task graph; Task graph is null"));
  }
  if (!done_) {
    return LOG_STATUS(Status::TaskGraphExecutorError(
        "Cannot execute task graph; Task graph is already being executed"));
  }
  if (task_graph_->is_cyclic()) {
    return LOG_STATUS(Status::TaskGraphExecutorError(
        "Cannot execute task graph; Task graph is cyclic (unsupported)"));
  }

  // Get roots
  auto roots = task_graph_->roots();

  // Get the lock
  std::unique_lock<std::mutex> lk(mtx_);

  // Initializations
  terminated_ = false;
  task_done_ = false;
  done_ = roots.empty();
  last_task_st_ = Status::Ok();
  running_tasks_.clear();
  predecessors_done_.clear();

  // Add roots to map <id, task> and dispatch to thread pool
  for (const auto& task : roots) {
    running_tasks_.insert({task->id(), task});
    tp_->execute([this, task]() { return execute_task(task); });
  }

  return Status::Ok();
}

Status TaskGraphExecutor::set_thread_pool(ThreadPool* tp) {
  std::unique_lock<std::mutex> lk(mtx_);

  if (!done_) {
    return LOG_STATUS(Status::TaskGraphExecutorError(
        "Cannot set thread pool; A task graph is being executed"));
  }

  tp_ = tp;

  return Status::Ok();
}

Status TaskGraphExecutor::set_task_graph(
    const tdb_shared_ptr<TaskGraph>& task_graph) {
  std::unique_lock<std::mutex> lk(mtx_);

  if (!done_) {
    return LOG_STATUS(Status::TaskGraphExecutorError(
        "Cannot set task graph; Another task graph is being executed"));
  }

  task_graph_ = task_graph;

  return Status::Ok();
}

Status TaskGraphExecutor::wait() {
  // Wait for all tasks to complete
  while (!terminated_ && !done_) {
    std::unique_lock<std::mutex> lk(mtx_);
    cv_.wait(lk, [this] { return task_done_; });

    if (!last_task_st_.ok()) {
      terminated_ = true;
    }

    if (running_tasks_.empty()) {
      done_ = true;
    }

    task_done_ = false;
  };

  return last_task_st_;
}

Status TaskGraphExecutor::execute_task(const tdb_shared_ptr<Task>& task) {
  // Execute the task;
  auto st = task->execute();

  // Lock
  std::unique_lock<std::mutex> lk(mtx_);

  // Handle task done
  task_done_ = true;
  running_tasks_.erase(task->id());
  last_task_st_ = st;

  // Handle failed task
  if (!last_task_st_.ok()) {
    terminated_ = true;

    // Signal the waiting function
    lk.unlock();
    cv_.notify_one();

    return last_task_st_;
  }

  // Merge the generated task graph with `task`.
  // Note that the lock is already taken here, this is safe.
  // Also note that we are always merging nodes forward in
  // the task flow and always adding new nodes, therefore
  // this does not interrup the execution of the rest of the graph.
  last_task_st_ = task_graph_->merge_generated_task_graph(task->id());
  if (!last_task_st_.ok()) {
    terminated_ = true;

    // Signal the waiting function
    lk.unlock();
    cv_.notify_one();

    return last_task_st_;
  }

  // Update the done counters of the predecessors of the
  // successors of this task
  auto successors = task->successors();
  for (const auto& t : successors) {
    auto it = predecessors_done_.find(t->id());
    if (it == predecessors_done_.end()) {
      predecessors_done_.insert({t->id(), 1});
    } else {
      it->second++;
    }
  }

  // Add successors to the thread pool and running tasks.
  // Only successors with all their predecessors done will be added.
  for (const auto& t : successors) {
    auto it = predecessors_done_.find(t->id());
    if (it != predecessors_done_.end() && it->second == t->predecessors_num()) {
      running_tasks_.insert({t->id(), t});
      predecessors_done_.erase(t->id());
      tp_->execute([this, t]() { return execute_task(t); });
    }
  }

  // Signal the waiting function
  lk.unlock();
  cv_.notify_one();

  return last_task_st_;
}

/* ********************************* */
/*          PRIVATE METHODS          */
/* ********************************* */

TaskGraphExecutor TaskGraphExecutor::clone() const {
  TaskGraphExecutor clone;

  clone.tp_ = tp_;
  clone.task_graph_ = task_graph_;
  clone.running_tasks_ = running_tasks_;
  clone.predecessors_done_ = predecessors_done_;
  clone.terminated_ = terminated_;
  clone.task_done_ = task_done_;
  clone.done_ = done_;
  clone.last_task_st_ = last_task_st_;

  return clone;
}

void TaskGraphExecutor::swap(TaskGraphExecutor& tge) {
  std::swap(tp_, tge.tp_);
  std::swap(task_graph_, tge.task_graph_);
  std::swap(running_tasks_, tge.running_tasks_);
  std::swap(predecessors_done_, tge.predecessors_done_);
  std::swap(terminated_, tge.terminated_);
  std::swap(task_done_, tge.task_done_);
  std::swap(done_, tge.done_);
  std::swap(last_task_st_, tge.last_task_st_);
}