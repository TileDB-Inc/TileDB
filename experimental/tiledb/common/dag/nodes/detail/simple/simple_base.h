/**
 * @file   experimental/tiledb/common/dag/nodes/detail/simple/simple_base.h
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
 *
 * This file declares  a virtual base class for dag task graph nodes, to be used
 * by both simple and general graph nodes.
 */

#ifndef TILEDB_DAG_NODE_DETAIL_SIMPLE_SIMPLE_BASE_H
#define TILEDB_DAG_NODE_DETAIL_SIMPLE_SIMPLE_BASE_H

#include <functional>
#include <type_traits>
#include <variant>

#include "experimental/tiledb/common/dag/ports/ports.h"
#include "experimental/tiledb/common/dag/state_machine/test/helpers.h"

namespace tiledb::common {

/**
 * A virtual base class for graph nodes.
 */
template <class CalculationState>
class GeneralGraphNode {
 protected:
  CalculationState current_calculation_state_;
  CalculationState new_calculation_state_;

 public:
  // including swap
  void update_state();

  /**
   * Pure virtual function for running graph nodes.
   */
  virtual void resume() = 0;
  virtual void run() = 0;
  virtual void run_for(size_t iterations = 1) = 0;
  //  virtual void stop();
};

template <>
class GeneralGraphNode<void> : GeneralGraphNode<std::monostate> {};

using GraphNode = GeneralGraphNode<void>;
}  // namespace tiledb::common

#endif  // TILEDB_DAG_NODE_DETAIL_SIMPLE_SIMPLE_BASE_H
