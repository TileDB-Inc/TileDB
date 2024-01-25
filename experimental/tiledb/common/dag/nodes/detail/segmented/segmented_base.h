/**
 * @file experimental/tiledb/common/dag/nodes/detail/segmented/segmented_base.h
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
 */

#ifndef TILEDB_DAG_NODES_DETAIL_BASE_H
#define TILEDB_DAG_NODES_DETAIL_BASE_H

#include <atomic>
#include <iostream>
#include <memory>
#include <string>

#include "experimental/tiledb/common/dag/execution/task_state_machine.h"

#include "experimental/tiledb/common/dag/nodes/node_traits.h"

namespace tiledb::common {

/**
 * Base class for all segmented nodes.  Maintains a program counter (for the
 * Duff's device) and a link to other nodes with which it communicates.  For
 * testing and debugging purposes, the node also maintains a name and an id.
 */
class node_base {
  /**
   * An atomic counter used to assign unique ids to nodes.
   */
  inline static std::atomic<size_t> id_counter{0};

 protected:
  using scheduler_event_type = SchedulerAction;

  bool debug_{false};

  size_t id_{0UL};
  size_t program_counter_{0};

 public:
  using node_type = node_base;
  using node_handle_type = std::shared_ptr<node_base>;

 private:
  node_handle_type sink_correspondent_{nullptr};
  node_handle_type source_correspondent_{nullptr};

 public:
  [[nodiscard]] size_t get_program_counter() const {
    return program_counter_;
  }

  virtual node_handle_type& sink_correspondent() {
    return sink_correspondent_;
  }

  virtual node_handle_type& source_correspondent() {
    return source_correspondent_;
  }

  /** Move constructor */
  node_base(node_base&&) = default;

  /** Nonsensical constructor, provided so that node_base will meet movable
   * concept requirements */
  node_base(const node_base&) {
    std::cout << "Nonsense copy constructor" << std::endl;
  }

  /** Default destructor
   * @todo Is virtual necessary?
   */
  virtual ~node_base() = default;

  /** Return the id of the node (const) */
  [[nodiscard]] inline size_t id() const {
    return id_;
  }

  /** Return a reference to the id of the node (non const) */
  inline size_t& id() {
    return id_;
  }

  /** Default constructor */
  explicit node_base()
      : id_{id_counter++} {
  }

  /** Utility functions for indicating what kind of node and state of the ports
   * being used.
   *
   * @todo Are these used anywhere?  This is an abstraction violation, so we
   * should try not to use them.
   * */
  [[nodiscard]] virtual bool is_producer_node() const {
    return false;
  }

  [[nodiscard]] virtual bool is_consumer_node() const {
    return false;
  }

  [[nodiscard]] virtual bool is_function_node() const {
    return false;
  }

  [[nodiscard]] virtual bool is_source_empty() const {
    return false;
  }

  [[nodiscard]] virtual bool is_sink_full() const {
    return false;
  }

  [[nodiscard]] virtual bool is_sink_state_empty() const {
    return false;
  }

  [[nodiscard]] virtual bool is_sink_state_full() const {
    return false;
  }

  [[nodiscard]] virtual bool is_source_state_empty() const {
    return false;
  }

  [[nodiscard]] virtual bool is_source_state_full() const {
    return false;
  }

  [[nodiscard]] virtual bool is_source_terminating() const {
    return false;
  }

  [[nodiscard]] virtual bool is_sink_terminating() const {
    return false;
  }

  [[nodiscard]] virtual bool is_source_terminated() const {
    return false;
  }

  [[nodiscard]] virtual bool is_sink_terminated() const {
    return false;
  }

  [[nodiscard]] virtual bool is_source_done() const {
    return false;
  }

  [[nodiscard]] virtual bool is_sink_done() const {
    return false;
  }

  /**
   * The resume function.  Primary entry point for execution of the node.
   */
  virtual scheduler_event_type resume() = 0;

  /**
   * The run function.  Executes resume in loop until the node is done.
   */
  virtual void run() = 0;

  /** Decrement the program counter */
  void decrement_program_counter() {
    assert(program_counter_ > 0);
    --program_counter_;
  }

  /** Function for getting name of node.  As used in this library, the name
   * is just a string that specifies the type of node.
   *
   * @return Name of the node
   */
  virtual std::string name() const {
    return {"abstract base"};
  }

  virtual void enable_debug() {
    debug_ = true;
  }

  void disable_debug() {
    debug_ = false;
  }

  [[nodiscard]] bool debug() const {
    return debug_;
  }

  /** Function useful for debugging.  */
  virtual void dump_node_state() = 0;
};

/**
 * Connect two nodes.
 * @tparam From Source node type.
 * @tparam To Sink node type.
 * @param from Source node.
 * @param to Sink node.
 */
template <class From, class To>
void connect(From& from, To& to) {
  (*from).sink_correspondent() = to;
  (*to).source_correspondent() = from;
}

}  // namespace tiledb::common
#endif  // TILEDB_DAG_NODES_DETAIL_BASE_H
