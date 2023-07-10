/**
 * @file flow_graph/system/flow_graph_system.h
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

#ifndef TILEDB_FLOW_GRAPH_SYSTEM_FLOW_GRAPH_SYSTEM_H
#define TILEDB_FLOW_GRAPH_SYSTEM_FLOW_GRAPH_SYSTEM_H

#include "../library/dynamic/dynamic_zero_graph.h"
#include "../library/platform/minimal_execution_platform.h"
#include "../library/static/static_zero_graph.h"
#include "execution_platform.h"
#include "graph_static_specification.h"

namespace tiledb::flow_graph {

namespace {
using SZG = static_specification::ZeroGraph;
using DZG = dynamic_specification::ZeroGraph;
}  // namespace

/**
 * Class declares itself as a flow graph system.
 *
 * @tparam T a potential flow graph system
 */
template <class T>
concept self_declared_as_flow_graph_system =
    requires(T x) { x.invariants.i_am_flow_graph_system; };

/**
 * Class declares a transformer from a graph static specification to a graph
 * dynamic specification with a given execution platform.
 *
 * @tparam T a potential flow graph system
 */
template <class T>
concept declares_static_to_dynamic_transformer = requires(T) {
  typename T::
      template static_to_dynamic_transformer<SZG, MinimalExecutionPlatform>;
};

/**
 * Class declares an execution platform.
 *
 * @tparam T a potential flow graph system
 */
template <class T>
concept declares_execution_platform = requires(T) {
  typename T::execution_platform;
} && execution_platform<typename T::execution_platform>;

/**
 * Class declares a transformer from a dynamic specification to an execution
 * graph.
 *
 * @tparam T a potential flow graph system
 */
template <class T>
concept declares_dynamic_to_execution_graph_transformer =
    requires(T) { typename T::template dynamic_to_execution_transformer<DZG>; };

/**
 * A flow graph system is a selection of particular implementations of the top-
 * level concepts.
 *
 * @tparam T a potential flow graph system
 */
template <class T>
concept flow_graph_system = self_declared_as_flow_graph_system<T> &&
                            declares_static_to_dynamic_transformer<T> &&
                            declares_execution_platform<T> &&
                            declares_dynamic_to_execution_graph_transformer<T>;

}  // namespace tiledb::flow_graph
#endif  // TILEDB_FLOW_GRAPH_SYSTEM_FLOW_GRAPH_SYSTEM_H
