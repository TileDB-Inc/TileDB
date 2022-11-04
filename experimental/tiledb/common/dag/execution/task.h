/**
 * @file   task.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 *
 * @section DESCRIPTION
 *
 */

#ifndef TILEDB_DAG_TASK_H
#define TILEDB_DAG_TASK_H

#include "experimental/tiledb/common/dag/execution/task_state_machine.h"

namespace tiledb::common {

template <class Node>
class TaskImpl : Node {
  TaskState state_{TaskState::created};

  /** @todo Is there a way to derive from Node to be able to use statements like
   * `using Node::resume` even though it is a `shared_ptr`? */
  //  Node node_;
  using node_ = Node;

 public:
  explicit TaskImpl(const Node& n)
      : node_{n} {
  }

  TaskImpl() = default;

  TaskImpl(const TaskImpl&) = default;

  TaskImpl(TaskImpl&&) noexcept = default;

  TaskImpl& operator=(const TaskImpl&) = default;

  TaskImpl& operator=(TaskImpl&&) noexcept = default;

  TaskState& task_state() {
    return state_;
  }

  void resume() {
    (*this)->resume();
  }

  void decrement_program_counter() {
    (*this)->decrement_program_counter();
  }

  Node& sink_correspondent() const {
    return (*this)->sink_correspondent();
  }

  Node& source_correspondent() const {
    return (*this)->source_correspondent();
  }

  [[nodiscard]] std::string name() const {
    return (*this)->name() + " task";
  }

  [[nodiscard]] size_t id() const {
    return (*this)->id();
  }

  virtual ~TaskImpl() = default;

  void dump_task_state(const std::string& msg = "") {
    std::string preface = (!msg.empty() ? msg + "\n" : "");

    std::cout << preface + "    " + this->name() + " with id " +
                     std::to_string(this->id()) + "\n" +
                     "    state = " + str(this->task_state()) + "\n";
  }
};

template <class Node>
class Task : public std::shared_ptr<TaskImpl<Node>> {
  using Base = std::shared_ptr<TaskImpl<Node>>;

 public:
  explicit Task(const Node& n)
      : Base{std::make_shared<TaskImpl<Node>>(n)} {
  }

  Task() = default;

  Task(const Task& rhs) = default;

  Task(Task&& rhs) noexcept = default;

  Task& operator=(const Task& rhs) = default;

  Task& operator=(Task&& rhs) noexcept = default;

  [[nodiscard]] TaskState& task_state() const {
    return (*this)->task_state();
  }
};

}  // namespace tiledb::common
#endif  // TILEDB_DAG_TASK_H
