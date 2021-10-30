/**
 * @file   task_graph.cc
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
 * This file implements class TaskGraph.
 */

#include "tiledb/common/task_graph/task_graph.h"

#include <iostream>
#include <sstream>

using namespace tiledb::common;

/* ********************************* */
/*     CONSTRUCTORS & DESTRUCTORS    */
/* ********************************* */

TaskGraph::TaskGraph() {
}

TaskGraph::~TaskGraph() {
}

TaskGraph::TaskGraph(const TaskGraph& task_graph)
    : TaskGraph() {
  auto clone = task_graph.clone();
  swap(clone);
}

TaskGraph::TaskGraph(TaskGraph&& task_graph)
    : TaskGraph() {
  swap(task_graph);
}

TaskGraph& TaskGraph::operator=(const TaskGraph& task_graph) {
  auto clone = task_graph.clone();
  swap(clone);
  return *this;
}

TaskGraph& TaskGraph::operator=(TaskGraph&& task_graph) {
  swap(task_graph);
  return *this;
}

/* ********************************* */
/*                API                */
/* ********************************* */

tdb_shared_ptr<Task> TaskGraph::emplace(
    const std::function<Status(void)>& func, const std::string& name) {
  std::unique_lock<std::mutex> lck(mtx_);

  auto task_id = tasks_.size();
  auto new_task = tdb_make_shared(Task, task_id, func, name);
  tasks_.insert(new_task);
  tasks_map_[task_id] = new_task;

  return new_task;
}

tdb_shared_ptr<Task> TaskGraph::emplace(
    const std::function<std::pair<Status, tdb_shared_ptr<TaskGraph>>(void)>&
        func,
    const std::string& name) {
  std::unique_lock<std::mutex> lck(mtx_);

  auto task_id = tasks_.size();
  auto new_task = tdb_make_shared(Task, task_id, func, name);
  tasks_.insert(new_task);
  tasks_map_[task_id] = new_task;

  return new_task;
}

const std::set<tdb_shared_ptr<Task>>& TaskGraph::tasks() const {
  return tasks_;
}

const std::unordered_map<uint64_t, tdb_shared_ptr<Task>>& TaskGraph::tasks_map()
    const {
  return tasks_map_;
}

std::string TaskGraph::to_dot() const {
  std::stringstream ss;

  ss << "digraph TaskGraph {\n";

  for (const auto& task : tasks_) {
    const auto& name = task->name();
    auto id = task->id();

    // Add label for visited task
    ss << "    " << id << " [label=\"id: " << id;
    if (name.empty())
      ss << "\"];\n";
    else
      ss << "\\nname: " << name << "\"];\n";

    // Add edges for successors
    for (const auto& succ : task->successors())
      ss << "    " << id << " -> " << succ->id() << ";\n";
  }

  ss << "}\n";

  return ss.str();
}

std::vector<tdb_shared_ptr<Task>> TaskGraph::roots() const {
  std::vector<tdb_shared_ptr<Task>> ret;

  for (const auto& task : tasks_) {
    if (task->predecessors().empty())
      ret.emplace_back(task);
  }

  return ret;
}

bool TaskGraph::is_cyclic() const {
  std::vector<bool> visited(tasks_.size(), false);
  std::vector<bool> rec_stack(tasks_.size(), false);

  // Recursive depth-first traversal
  for (size_t i = 0; i < tasks_.size(); i++) {
    if (is_cyclic_util(i, visited, rec_stack))
      return true;
  }

  return false;
}

Status TaskGraph::merge_generated_task_graph(uint64_t task_id) {
  // Acquire the lock
  std::unique_lock<std::mutex> lk(mtx_);

  // Check existence
  auto it = tasks_map_.find(task_id);
  if (it == tasks_map_.end()) {
    return LOG_STATUS(Status::TaskGraphError(
        "Cannot merge generated task graph; invalid task id"));
  }

  // Get generated task graph
  auto task = it->second;
  auto gen_tg = task->generated_task_graph();

  // Nothing to do if there is no generated task graph
  if (gen_tg == nullptr)
    return Status::Ok();

  // Add all the tasks of the generated task graph.
  // Note that this will properly update all the ids
  // of the tasks, as well as their `generated_by` field.
  const auto& gen_tg_tasks = gen_tg->tasks();
  for (const auto& t : gen_tg_tasks)
    emplace(t, task_id);

  // Set the roots as successors of the task
  const auto& roots = gen_tg->roots();
  for (const auto& t : roots)
    precedes(task, t);

  // Set generated_tg_ to nullptr (clear)
  task->clear_generated_task_graph();

  return Status::Ok();
}

/* ********************************* */
/*          PRIVATE METHODS          */
/* ********************************* */

bool TaskGraph::is_cyclic_util(
    size_t v, std::vector<bool>& visited, std::vector<bool>& rec_stack) const {
  if (visited[v] == false) {
    visited[v] = true;
    rec_stack[v] = true;

    // Recur for all the successors of v
    auto it = tasks_map_.find(v);
    assert(it != tasks_map_.end());
    auto task = it->second;
    for (const auto& s : task->successors()) {
      auto id = s->id();
      if (!visited[id] && is_cyclic_util(id, visited, rec_stack))
        return true;
      else if (rec_stack[id])
        return true;
    }
  }

  rec_stack[v] = false;
  return false;
}

TaskGraph TaskGraph::clone() const {
  TaskGraph clone;
  clone.tasks_ = tasks_;

  return clone;
}

void TaskGraph::swap(TaskGraph& task_graph) {
  std::swap(tasks_, task_graph.tasks_);
}

void TaskGraph::emplace(
    const tdb_shared_ptr<Task>& task, uint64_t generated_by) {
  auto task_id = tasks_.size();
  tasks_.insert(task);
  tasks_map_[task_id] = task;
  task->set_id(task_id);
  task->set_generated_by(generated_by);
}