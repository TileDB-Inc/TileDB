/**
 * @file   task_graph_executor.h
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
 This file defines class TaskGraphExecutor.
 */

#ifndef TILEDB_TASK_GRAPH_EXECUTOR_H
#define TILEDB_TASK_GRAPH_EXECUTOR_H

#include "tiledb/common/status.h"
#include "tiledb/common/task_graph/task_graph.h"
#include "tiledb/common/thread_pool.h"

#include <condition_variable>
#include <mutex>
#include <unordered_map>

namespace tiledb {
namespace common {

/**
 * This class implements a task graph executor, with its own
 * separate thread pool for dispatching the tasks to the threads.
 */
class TaskGraphExecutor {
 public:
  /* ********************************* */
  /*         TYPE DEFINITIONS          */
  /* ********************************* */

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Consturctor. */
  TaskGraphExecutor();

  /** Constructor. */
  TaskGraphExecutor(
      ThreadPool* tp, const tdb_shared_ptr<TaskGraph>& task_graph);

  /** Destructor. */
  ~TaskGraphExecutor();

  TaskGraphExecutor(const TaskGraphExecutor&);
  TaskGraphExecutor(TaskGraphExecutor&&);
  TaskGraphExecutor& operator=(const TaskGraphExecutor&);
  TaskGraphExecutor& operator=(TaskGraphExecutor&&);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Execute the task graph. This function is non-blocking. */
  Status execute();

  /** Executes the input task. */
  Status execute_task(const tdb_shared_ptr<Task>& task);

  /** Sets the thread pool */
  Status set_thread_pool(ThreadPool* tp);

  /** Sets the task graph. */
  Status set_task_graph(const tdb_shared_ptr<TaskGraph>& task_graph);

  /** Waits for the task graph to be executed to completion. */
  Status wait();

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /**
   * A pointer to the thread pool used by the task graph
   * executor.
   */
  ThreadPool* tp_;

  /** The task graph to execute. */
  tdb_shared_ptr<TaskGraph> task_graph_;

  /** A mutex. */
  std::mutex mtx_;

  /** A condition variable for the waiting function. */
  std::condition_variable cv_;

  /** The running tasks, indexed on task id. */
  std::unordered_map<uint64_t, tdb_shared_ptr<Task>> running_tasks_;

  /**
   * Maps task ids to a counter that indicates how many of the
   * predecessors of the task are done executing.
   */
  std::unordered_map<uint64_t, uint64_t> predecessors_done_;

  /** Whether the task graph has been terminated. */
  bool terminated_;

  /** Whether a task is done, to signal the waiting function. */
  bool task_done_;

  /** Whether the execution is done, to break the waiting function. */
  bool done_;

  /**
   * The last status to be reported after the completion of the
   * task graph execution.
   */
  Status last_task_st_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /** Returns a clone of this object. */
  TaskGraphExecutor clone() const;

  /**
   * Swaps the contents (all field values) of this object with the given
   * object.
   */
  void swap(TaskGraphExecutor& tge);
};

}  // namespace common
}  // namespace tiledb

#endif  // TILEDB_TASK_GRAPH_EXECUTOR_H
