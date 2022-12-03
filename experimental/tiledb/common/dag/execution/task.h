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
 * A `Task` wraps up a task graph node for the purposes of being able to be
 * scheduled for execution. It is a simple wrapper around a `Node` and maintains
 * the current state of the task in the scheduler.
 *
 */

#ifndef TILEDB_DAG_TASK_H
#define TILEDB_DAG_TASK_H

#include "experimental/tiledb/common/dag/execution/task_state_machine.h"
#include "experimental/tiledb/common/dag/execution/task_traits.h"
#include "experimental/tiledb/common/dag/nodes/node_traits.h"

#include "experimental/tiledb/common/dag/utility/print_types.h"

namespace tiledb::common {

/**
 * @brief The actual implementation of a task.
 *
 * @tparam Node The type of the node.
 */
template <class Node>
class TaskImpl : Node {
  using NodeBase = Node;
  using scheduler_event_type = SchedulerAction;

  TaskState state_{TaskState::created};

 public:
  using node_type = node_t<Node>;
  using node_handle_type = node_handle_t<Node>;
  using task_type = TaskImpl<Node>;
  using task_handle_type = std::shared_ptr<TaskImpl<Node>>;

  explicit TaskImpl(const node_type& n)
      : NodeBase{
            std::make_shared<node_type>(n)} {  // @todo abstraction violation
  }

  explicit TaskImpl(node_type&& n)
      : NodeBase{n} {
  }

  explicit TaskImpl(const node_handle_type& n)
      : NodeBase{n} {
  }

  explicit TaskImpl(node_handle_type&& n)
      : NodeBase{n} {
  }

  /** Default constructor. */
  TaskImpl() = default;

  /** Default copy constructor. */
  TaskImpl(const TaskImpl&) = default;

  /** Default move constructor. */
  TaskImpl(TaskImpl&&) noexcept = default;

  /** Default assignment operator. */
  TaskImpl& operator=(const TaskImpl&) = default;

  /** Default move assignment operator. */
  TaskImpl& operator=(TaskImpl&&) noexcept = default;

  /** Get the current state of the task. */
  TaskState& task_state() {
    return state_;
  }

  /** Get the underlying node */
  auto node() {
    return dynamic_cast<NodeBase*>(this);
  }

  /** Resume the underlying node computation. */
  scheduler_event_type resume() {
    return (*this)->resume();
  }

  /** Decrement program counter of underlying node */
  void decrement_program_counter() {
    (*this)->decrement_program_counter();
  }

  /**
   * Get correspondent of underlying node, a `Sink`.
   * @return The corresponding node.
   */
  node_handle_type& sink_correspondent() {
    return (*this)->sink_correspondent();
  }

  /**
   * Get correspondent of underlying node, a `Source`.
   * @return The corresponding node.
   */
  node_handle_type& source_correspondent() {
    return (*this)->source_correspondent();
  }

  /** Get name of underlying node.  Useful for testing and debugging. */
  [[nodiscard]] std::string virtual name() const {
    return (*this)->name() + " task";
  }

  /** Get id of underlying node.  Useful for testing and debugging. */
  [[nodiscard]] size_t id() const {
    return (*this)->id();
  }

  // @todo virtual ??
  virtual ~TaskImpl() = default;

  /** Dump some debugging information about the task. */
  void dump_task_state(const std::string& msg = "") {
    std::string preface = (!msg.empty() ? msg + "\n" : "");

    std::cout << preface + "    " + this->name() + " with id " +
                     std::to_string(this->id()) + "\n" +
                     "    state = " + str(this->task_state()) + "\n";
  }
};

/** Wrapper around `TaskImpl` to make it a `shared_ptr`. */
template <class Node>
class Task : public std::shared_ptr<TaskImpl<Node>> {
  using Base = std::shared_ptr<TaskImpl<Node>>;

 public:
  using node_type = node_t<Node>;
  using node_handle_type = node_handle_t<Node>;
  using task_type = TaskImpl<node_handle_type>;
  using task_handle_type = Task<Node>;

  explicit Task(const node_handle_type& n)
      : Base{std::make_shared<TaskImpl<node_handle_type>>(n)} {
  }

  template <class U = Node>
  explicit Task(
      const node_type& n,
      std::enable_if_t<!std::is_same_v<node_t<U>, node_handle_t<U>>, void**> =
          nullptr)
      : Base{std::make_shared<TaskImpl<node_handle_type>>(n)} {
  }

  explicit Task(node_handle_type&& n)
      : Base{std::make_shared<TaskImpl<node_handle_type>>(n)} {
  }

  template <class U = Node>
  explicit Task(
      node_type&& n,
      std::enable_if_t<!std::is_same_v<node_t<U>, node_handle_t<U>>, void**> =
          nullptr)
      : Base{std::make_shared<TaskImpl<node_handle_type>>(n)} {
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
