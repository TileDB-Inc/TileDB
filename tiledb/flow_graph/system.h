/**
 * @file flow_graph/flow_graph.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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

#ifndef TILEDB_FLOW_GRAPH_FLOW_GRAPH_H
#define TILEDB_FLOW_GRAPH_FLOW_GRAPH_H

#include "library/dynamic/to_dynamic_reference.h"
#include "library/graph/flow_graph_reference.h"
#include "library/platform/basic_execution_platform.h"
#include "system/flow_graph_system.h"

namespace tiledb::flow_graph {

/**
 * A reference system that consists of all the reference implementations.
 */
struct ReferenceSystem {
  static constexpr struct invariant_type {
    static constexpr bool i_am_flow_graph_system{true};
  } invariants;

  template <graph_static_specification GSS, execution_platform EP>
  using static_to_dynamic_transformer = ToDynamicReference<GSS, EP>;

  using execution_platform = BasicExecutionPlatform;

  template <graph_dynamic_specification GDS>
  using dynamic_to_execution_transformer = FlowGraphReference<GDS>;
};
static_assert(flow_graph_system<ReferenceSystem>);

}  // namespace tiledb::flow_graph

#endif  // TILEDB_FLOW_GRAPH_FLOW_GRAPH_H
