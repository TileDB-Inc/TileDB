/**
 * @file flow_graph/execution_graph.h
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

#ifndef TILEDB_FLOW_GRAPH_SYSTEM_EXECUTION_GRAPH_H
#define TILEDB_FLOW_GRAPH_SYSTEM_EXECUTION_GRAPH_H

namespace tiledb::flow_graph::execution {
/**
 * Execution graphs must declare themselves as such.
 *
 * @tparam T a potential specification graph class
 */
template <class T>
concept self_declared_as_graph =
    requires(T x) { x.invariants.i_am_execution_graph; };

template <class T>
concept graph = self_declared_as_graph<T>;

}  // namespace tiledb::flow_graph::execution

namespace tiledb::flow_graph {

/**
 * This concept is the interface between a scheduler and a flow graph.Any
 * scheduler can run an execution graph.
 *
 * Maturity Note: This is a stub. There are no schedulers in the `flow_graph`
 * directory yet. This concept should be modeled on `class FlowGraph`.
 *
 * @tparam T a potential specification graph class
 */
template <class T>
concept execution_graph = execution::graph<T>;

}  // namespace tiledb::flow_graph

#endif  // TILEDB_FLOW_GRAPH_SYSTEM_EXECUTION_GRAPH_H
