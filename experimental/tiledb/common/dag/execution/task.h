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

namespace tiledb::common {

/**
 * @brief The actual implementation of a task.
 *
 * @tparam Node The type of the node.
 */
template <class Node>
class TaskImpl : Node {
  TaskState state_{TaskState::created};

  /** @todo Is there a way to derive from Node to be able to use statements like
   * `using Node::resume` even though it is a `shared_ptr`? */
  //  Node node_;
  using node_ = Node;

  using scheduler_event_type = SchedulerAction;

 public:
  using node_handle_type = Node;  // @todo abstraction violation?
  using node_type =
      typename Node::element_type;  // @todo abstraction violation?

  explicit TaskImpl(const Node& n)
      : node_{n} {
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
    return dynamic_cast<node_*>(this);
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
  Node& sink_correspondent() const {
    return (*this)->sink_correspondent();
  }

  /**
   * Get correspondent of underlying node, a `Source`.
   * @return The corresponding node.
   */
  Node& source_correspondent() const {
    return (*this)->source_correspondent();
  }

  /** Get name of underlying node.  Useful for testing and debugging. */
  [[nodiscard]] std::string name() const {
    return (*this)->name() + " task";
  }

  /** Get id of underlying node.  Useful for testing and debugging. */
  [[nodiscard]] size_t id() const {
    return (*this)->id();
  }

  // @todo virtual ??
  ~TaskImpl() = default;

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
  using node_type = typename Base::element_type::node_type;
  using node_handle_type = typename Base::element_type::node_handle_type;
  using task_handle_type = Task<Node>;

  explicit Task(node_handle_type&& n)
      : Base{std::make_shared<TaskImpl<node_handle_type>>(n)} {
    // print_types(n);
  }

  explicit Task(node_type&& n)
      : Base{std::make_shared<TaskImpl<node_handle_type>>(
            std::make_shared<node_type>(n))} {
    // print_types(n);
  }

  explicit Task(const node_handle_type& n)
      : Base{std::make_shared<TaskImpl<node_handle_type>>(n)} {
    // print_types(n);
  }

  explicit Task(const node_type& n)
      : Base{std::make_shared<TaskImpl<node_handle_type>>(
            std::make_shared<node_type>(n))} {
    // print_types(n);
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
