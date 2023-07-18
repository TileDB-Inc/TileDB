/**
 * @file flow_graph/system/graph_static_specification.h
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

#ifndef TILEDB_FLOW_GRAPH_GRAPH_STATIC_SPECIFICATION_H
#define TILEDB_FLOW_GRAPH_GRAPH_STATIC_SPECIFICATION_H

#include <concepts>
#include <tuple>

#include "graph_type.h"
#include "tiledb/flow_graph/system/edge_static_specification.h"
#include "tiledb/flow_graph/system/node_static_specification.h"
#include "tiledb/flow_graph/system/port_static_specification.h"

namespace tiledb::flow_graph::static_specification {

/**
 * A class declares itself as a graph static specification.
 *
 * @tparam T a potential graph static specification class
 */
template <class T>
concept self_declared_as_graph =
    requires(T x) { x.invariants.i_am_graph_static_specification; };

/**
 * Class declares a member variable `nodes`.
 *
 * @tparam T a potential graph static specification class
 */
template <class T>
concept declares_node_list = requires(T) { T::nodes; };

/**
 * Class member variable `nodes` is tuple-like
 *
 * @tparam T a potential graph static specification class
 */
template <class T>
concept node_list_is_tuple =
    declares_node_list<T> && applicable<decltype(T::nodes)>;

/**
 * Each element of class member variable 'nodes' is an lvalue reference.
 *
 * The requirement for lvalue references is to implement the equality operator
 * as address equality instead of some other mechanism that would not have
 * compiler support.
 *
 * @tparam T a potential graph static specification class
 */
template <class T>
concept node_list_contains_lvalue_references =
    node_list_is_tuple<T> &&
    requires(T x) { contains_lvalue_references_v(x.nodes); };

/**
 * Each element of class member variable 'nodes' is an lvalue reference to
 * a node static specification.
 *
 * @tparam T a potential graph static specification class
 */
template <class T>
concept has_node_list =
    node_list_contains_lvalue_references<T> && requires(T x) {
      []<class... U>(std::tuple<U...>)
        requires(node<std::remove_reference_t<U>> && ...)
      {}
      (x.nodes);
    };

/**
 * Class declares a member variable `edges`.
 *
 * @tparam T a potential graph static specification class
 */
template <class T>
concept declares_edge_list = requires(T) { T::edges; };

/**
 * Class member variable `edges` is tuple-like.
 *
 * @tparam T a potential graph static specification class
 */
template <class T>
concept edge_list_is_tuple =
    declares_edge_list<T> && requires(T x) { applicable<decltype(x.edges)>; };

/**
 * Each element of class member variable 'edges' is an edge static
 * specification.
 *
 * @tparam T a potential graph static specification class
 */
template <class T>
concept has_edge_list = edge_list_is_tuple<T> && requires(T x) {
  []<class... U>(std::tuple<U...>)
    requires(edge<U> && ...)
  {}
  (x.edges);
};

/**
 * Stub.
 *
 * @tparam T a potential graph static specification class
 */
template <class T>
concept edge_tails_are_in_graph = true;

/**
 * Stub.
 *
 * @tparam T a potential graph static specification class
 */
template <class T>
concept edge_heads_are_in_graph = true;

/**
 * A specification graph is self-declared. It has a node list and an edge list.
 * The heads and tails of all the edges are within the nodes of the graph.
 *
 * Maturity Note: There is as yet no requirement that each port be connected to
 * something. This will need to be added before leaving the initial development
 * phase. There are two ordinary possibilities that needs to be accounted for:
 *   1. The graph is sealed. There are no inflows or outflows to the graph.
 *   2. The graph is ported. The graph has input ports and/or output ports.
 * Only a sealed graph can be scheduled. A ported graph can be used as a
 * subcomponent of another graph through a standard transformation to a node. At
 * present there are no concepts for ported graphs; all graphs are considered
 * sealed.
 *
 * @tparam T a potential graph static specification class
 */
template <class T>
concept graph =
    self_declared_as_graph<T> && has_node_list<T> && has_edge_list<T> &&
    edge_tails_are_in_graph<T> && edge_heads_are_in_graph<T>;

}  // namespace tiledb::flow_graph::static_specification

namespace tiledb::flow_graph {
/**
 * A graph static specification provides the essential information for
 * constructing a flow graph: nodes and edge connections.
 *
 * @tparam T a potential graph static specification class
 */
template <class T>
concept graph_static_specification = static_specification::graph<T>;
}  // namespace tiledb::flow_graph

#endif  // TILEDB_FLOW_GRAPH_GRAPH_STATIC_SPECIFICATION_H
