/**
 * @file resumable_base.h
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

#ifndef TILEDB_DAG_NODES_DETAIL_RESUMABLE_BASE_H
#define TILEDB_DAG_NODES_DETAIL_RESUMABLE_BASE_H

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
class resumable_node_base {

 protected:
  using scheduler_event_type = SchedulerAction;

  size_t program_counter_{0};

 public:
  using node_type = resumable_node_base;
  using node_handle_type = std::shared_ptr<resumable_node_base>;

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
  resumable_node_base(resumable_node_base&&) = default;

  /** Nonsensical constructor, provided so that node_base will meet movable
   * concept requirements */
  resumable_node_base(const resumable_node_base&) {
    std::cout << "Nonsense copy constructor" << std::endl;
  }

  /** Default destructor
   * @todo Is virtual necessary?
   */
  virtual ~resumable_node_base() = default;


  /** Default constructor */
  explicit resumable_node_base() = default;


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
#endif  // TILEDB_DAG_NODES_DETAIL_RESUMABLE_BASE_H
