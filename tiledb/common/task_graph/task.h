/**
 * @file   task.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2017-2021 TileDB, Inc.
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
 This file defines class Task.
 */

#ifndef TILEDB_TASK_H
#define TILEDB_TASK_H

#include <functional>
#include <string>
#include <vector>

#include "tiledb/common/status.h"
#include "tiledb/common/task_graph/task_log.h"
#include "tiledb/common/task_graph/task_stats.h"
#include "tiledb/common/task_graph/task_status.h"

namespace tiledb {
namespace common {

class TaskGraph;

/** Represents a node in the task graph. */
class Task {
 public:
  /* ********************************* */
  /*         TYPE DEFINITIONS          */
  /* ********************************* */

  /* ********************************* */
  /*     CONSTRUCTORS & DESTRUCTORS    */
  /* ********************************* */

  /** Constructor. */
  Task();

  /**
   * Constructor.
   *
   * @param id The task id (unique in a task graph).
   * @param func The function that this task will call.
   * @param name The name of the task
   */
  Task(
      uint64_t id,
      const std::function<Status(void)>& func,
      const std::string& name = std::string());

  /**
   * Constructor for a task that generates a task graph.
   *
   * @param id The task id (unique in a task graph).
   * @param func The function that this task will call.
   * @param name The name of the task
   */
  Task(
      uint64_t id,
      const std::function<std::pair<Status, tdb_shared_ptr<TaskGraph>>(void)>&
          func,
      const std::string& name = std::string());

  /** Destructor. */
  ~Task();

  /** Copy constructor. */
  Task(const Task&);

  /** Move constructor. */
  Task(Task&&);

  /** Copy assignment. */
  Task& operator=(const Task&);

  /** Move assignment. */
  Task& operator=(Task&&);

  /* ********************************* */
  /*                API                */
  /* ********************************* */

  /** Returns the task id. */
  uint64_t id() const;

  /** Sets the task id. */
  void set_id(uint64_t id);

  /** Returns the task name. */
  const std::string& name() const;

  /** Returns the ids of the tasks that precede this task. */
  const std::vector<tdb_shared_ptr<Task>>& predecessors() const;

  /** Returns the number of predecessors. */
  size_t predecessors_num() const;

  /** Returns the ids of the tasks that succeed this task. */
  const std::vector<tdb_shared_ptr<Task>>& successors() const;

  /** Returns the number of successors. */
  size_t successors_num() const;

  /** Adds `succ_task` to the list of  successors. */
  Status add_successor(const tdb_shared_ptr<Task>& succ_task);

  /** Adds `pred_task` to the list of predecessors. */
  Status add_predecessor(const tdb_shared_ptr<Task>& pred_task);

  /** Executes the function in the task. */
  Status execute();

  /** Return the generated task graph. */
  const tdb_shared_ptr<TaskGraph>& generated_task_graph() const;

  /** Returns the id of the task that generated it. */
  uint64_t generated_by() const;

  /** Sets the `generated_by_` value. */
  void set_generated_by(uint64_t task_id);

  /** Clears the generated task graph. */
  void clear_generated_task_graph();

 private:
  /* ********************************* */
  /*         PRIVATE ATTRIBUTES        */
  /* ********************************* */

  /** The task id. */
  uint64_t id_;

  /** The function to be called for this task. */
  std::function<Status(void)> func_;

  /**
   * The function to be called for this task. Generates
   * a taks graph.
   */
  std::function<std::pair<Status, tdb_shared_ptr<TaskGraph>>(void)> func_tg_;

  /** The task name. */
  std::string name_;

  /** The predecessors. */
  std::vector<tdb_shared_ptr<Task>> predecessors_;

  /** The successors. */
  std::vector<tdb_shared_ptr<Task>> successors_;

  /** The task status. */
  TaskStatus status_;

  /** The task statistics. */
  TaskStats stats_;

  /** The task logs. */
  TaskLog log_;

  /** The task graph generated by executing func_tg_. */
  tdb_shared_ptr<TaskGraph> generated_tg_;

  /**
   * If the task is generated by another task this value will
   * store the id of the task that generated. This is valuable
   * for debugging and inspecting closely an algorithm design.
   */
  uint64_t generated_by_;

  /* ********************************* */
  /*          PRIVATE METHODS          */
  /* ********************************* */

  /** Returns a clone of this object. */
  Task clone() const;

  /**
   * Swaps the contents (all field values) of this object with the given
   * object.
   */
  void swap(Task& task);
};

}  // namespace common
}  // namespace tiledb

#endif  // TILEDB_EX_NODE_H
