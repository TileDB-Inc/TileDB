/**
 * @file   task_graph.h
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
 This file defines class TaskGraph.
 */

#ifndef TILEDB_TASK_GRAPH_H
#define TILEDB_TASK_GRAPH_H

#include "tiledb/common/heap_memory.h"
#include "tiledb/common/logger.h"
#include "tiledb/common/status.h"
#include "tiledb/common/task_graph/task.h"

#include <mutex>
#include <set>
#include <unordered_map>
#include <vector>

namespace tiledb {
namespace common {

/**
 * This class implements a task graph.
 *
 * @note Currently, we constrain the task graph as being directed
 *     acyclic (DAG).
 *
 * TODO: Currently the task graph is static. We may have to make
 *     it be dynamic in the future (i.e., tasks may be able to generate
 *     new tasks).
 */
class TaskGraph {
 public:
  /* ********************************* */
  /*         TYPE DEFINITIONS          */
  /* ********************************* */

  /** To implement variadic functions `succeeds` and `precedes`. */
  template <typename... Tt>
  using AllTasks = typename std::enable_if<std::conjunction<
      std::is_convertible<Tt, tdb_shared_ptr<Task>>...>::value>::type;

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Consturctor. */
  TaskGraph();

  /** Destructor. */
  ~TaskGraph();

  TaskGraph(const TaskGraph&);
  TaskGraph(TaskGraph&&);
  TaskGraph& operator=(const TaskGraph&);
  TaskGraph& operator=(TaskGraph&&);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /**
   * Creates a new task and adds it to the graph.
   *
   * @param func The function to be stored in the task.
   * @param name The task name.
   */
  tdb_shared_ptr<Task> emplace(
      const std::function<Status(void)>& func,
      const std::string& name = std::string());

  /**
   * Creates a new task and adds it to the graph.
   * This is for tasks that generate task graphs.
   *
   * @param func The function to be stored in the task.
   * @param name The task name.
   */
  tdb_shared_ptr<Task> emplace(
      const std::function<std::pair<Status, tdb_shared_ptr<TaskGraph>>(void)>&
          func,
      const std::string& name = std::string());

  /** Makes `pred` a predecessor of `task` and `task` a successor of `pred`. */
  Status succeeds(tdb_shared_ptr<Task> task, tdb_shared_ptr<Task> pred) {
    if (tasks_.find(task) == tasks_.end()) {
      return LOG_STATUS(Status::TaskGraphError(
          "Cannot add predecessors; invalid successor task"));
    }
    if (tasks_.find(pred) == tasks_.end()) {
      return LOG_STATUS(Status::TaskGraphError(
          "Cannot add predecessors; invalid predecessor task"));
    }

    RETURN_NOT_OK(task->add_predecessor(pred));
    RETURN_NOT_OK(pred->add_successor(task));
    return Status::Ok();
  }

  /** Variadic version of `succeeds`. */
  template <typename... Tt, typename = AllTasks<Tt...>>
  Status succeeds(
      tdb_shared_ptr<Task> task, tdb_shared_ptr<Task> pred, Tt... preds) {
    RETURN_NOT_OK(succeeds(task, pred));
    RETURN_NOT_OK(succeeds(task, preds...));
    return Status::Ok();
  }

  /** Makes `succ` a successor of `task` and `task` a predecessor of `succ`. */
  Status precedes(tdb_shared_ptr<Task> task, tdb_shared_ptr<Task> succ) {
    if (tasks_.find(task) == tasks_.end()) {
      return LOG_STATUS(Status::TaskGraphError(
          "Cannot add predecessors; invalid predecessor task"));
    }
    if (tasks_.find(succ) == tasks_.end()) {
      return LOG_STATUS(Status::TaskGraphError(
          "Cannot add predecessors; invalid successor task"));
    }

    RETURN_NOT_OK(task->add_successor(succ));
    RETURN_NOT_OK(succ->add_predecessor(task));
    return Status::Ok();
  }

  /** Variadic version of `succeeds`. */
  template <typename... Tt, typename = AllTasks<Tt...>>
  Status precedes(
      tdb_shared_ptr<Task> task, tdb_shared_ptr<Task> succ, Tt... succs) {
    RETURN_NOT_OK(precedes(task, succ));
    RETURN_NOT_OK(precedes(task, succs...));
    return Status::Ok();
  }

  /** Returns true if the graph is cyclic. */
  bool is_cyclic() const;

  /** Returns the root tasks, i.e., tasks without predecessors. */
  std::vector<tdb_shared_ptr<Task>> roots() const;

  /** Returns as a string the TaskGraph in DOT language. */
  std::string to_dot() const;

  /** Returns the tasks. */
  const std::set<tdb_shared_ptr<Task>>& tasks() const;

  /** Returns the tasks map. */
  const std::unordered_map<uint64_t, tdb_shared_ptr<Task>>& tasks_map() const;

  /**
   * It merges the generated task graph of the task with
   * input id by adding its roots as successors to the
   * task and chaning the generated id of the tasks
   * in that task graph.
   */
  Status merge_generated_task_graph(uint64_t task_id);

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /**
   * The tasks that comprise the task graph. This is a set
   * because we need to test quick task existence in the
   * graph.
   */
  std::set<tdb_shared_ptr<Task>> tasks_;

  /** A map from task id to task pointer for easy search. */
  std::unordered_map<uint64_t, tdb_shared_ptr<Task>> tasks_map_;

  /** Mutex for thread-safety. */
  std::mutex mtx_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /** Utility function used in `is_cyclic`. */
  bool is_cyclic_util(
      size_t v, std::vector<bool>& visited, std::vector<bool>& rec_stack) const;

  /** Returns a clone of this object. */
  TaskGraph clone() const;

  /**
   * Swaps the contents (all field values) of this object with the given
   * object.
   */
  void swap(TaskGraph& task_graph);

  /**
   * This adds an already created task to the graph, which was
   * generated by the task with id `generated_by`. This does not
   * acquire a lock and is used only in `merge_generated_task_graph`.
   */
  void emplace(const tdb_shared_ptr<Task>& task, uint64_t generated_by);
};

}  // namespace common
}  // namespace tiledb

#endif  // TILEDB_TASK_GRAPH_H
