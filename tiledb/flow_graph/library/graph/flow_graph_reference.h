/**
 * @file flow_graph/basic/basic_graph.h
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

#ifndef TILEDB_FLOW_GRAPH_LIBRARY_GRAPH_FLOW_GRAPH_H
#define TILEDB_FLOW_GRAPH_LIBRARY_GRAPH_FLOW_GRAPH_H

#include <memory>
#include <vector>

#include "../../system/graph_dynamic_specification.h"

namespace tiledb::flow_graph {
/**
 * The reference flow graph class.
 *
 * As a reference implementation, it's not meant to be optimal in any particular
 * way. For example, it does not use allocated memory particularly efficiently.
 * Because graphs are of variable size, we need to make at least one allocation.
 * This class makes no effort to make effort to make exactly one allocation,
 * which would require precomputing all the object sizes and offsets, allocating
 * an internal pool with the total size, and constructing objects within the
 * pool. Instead, each object is allocated separately.
 *
 * The constructor of this class relies on an execution platform with a
 * particular construction and connection policy, specifically (a) to make nodes
 * and edges first, and (b) to connect them after construction. If a reference
 * implementation is desired for execution platforms with other policies, this
 * can gain a constraint on its execution platform as derived from its template
 * argument `G`.
 *
 * @tparam EP Execution platform class
 */
template <graph_dynamic_specification G>
class FlowGraphReference {
  using EP = G::execution_platform_type;

 public:
  static constexpr struct invariant_type {
    static constexpr bool i_am_execution_graph{true};
  } invariants;

  /**
   * The node type of this execution graph is that of its platform parameter.
   */
  using node_type = EP::node_type;
  /**
   * The edge type of this execution graph is that of its platform parameter.
   */
  using edge_type = EP::edge_type;
  /**
   * node_list_type must satisfy (future) concept `ExecutionNodeListType`
   *
   * `ExecutionNodeListType` means `size()` returns `nodes_size_type`
   * `ExecutionNodeListType` means `operator[]` returns `ExecutionNodeType`
   */
  using node_list_type = std::vector<std::reference_wrapper<node_type>>;
  /**
   * edge_list_type must satisfy (future) concept `ExecutionEdgeListType`
   *
   * `ExecutionEdgeListType` means `size()` returns `edges_size_type`
   * `ExecutionEdgeListType` means `operator[]` returns `ExecutionEdgeType`
   */
  using edge_list_type = std::vector<std::reference_wrapper<edge_type>>;

 private:
  std::vector<std::shared_ptr<node_type>> node_storage_{};
  std::vector<std::reference_wrapper<node_type>> nodes_{};

  std::vector<std::shared_ptr<edge_type>> edge_storage_{};
  std::vector<std::reference_wrapper<edge_type>> edges_{};

 public:
  /**
   * Ordinary constructor builds from a dynamic specification graph. Upon
   * return, all nodes are in their initial state and all edges are empty.
   *
   * @tparam G A dynamic graph specification type
   * @param g a graph specification
   */
  explicit FlowGraphReference(const G& g) {
    // Note that all containers are initialized empty in their declarations.
    const auto edges{g.edges()};
    const typename G::nodes_size_type nn{g.nodes_size()};
    const typename G::edges_size_type ne{g.edges_size()};
    /*
     * Create nodes. The node type has the responsibility for constructing a
     * full execution node from its specification.
     */
    nodes_.reserve(nn);
    for (typename G::nodes_size_type i = 0; i < nn; ++i) {
      auto node_ptr{node_storage_.emplace_back(
          std::make_shared<node_type>(g.make_node(i)))};
      nodes_.emplace_back(std::ref(*node_ptr));
    }
    /*
     * Create and connect edges. The edge type has the responsibility for
     * constructing an execution edge from a specification. The platform has the
     * responsibility for connecting it to ports according to its specification.
     */
    edges_.reserve(ne);
    for (typename G::edges_size_type i = 0; i < ne; ++i) {
      const auto e_spec{edges[i]};
      const auto edge_ptr{edge_storage_.emplace_back(
          std::make_shared<edge_type>(g.make_edge(i)))};
      auto edge{edges_.emplace_back(std::ref(*edge_ptr))};

      /*
       * Needs edge data initialization for this not to crash. Can't index
       * correctly into port lists without it.
       */
      /*
      EP::connect_tail(
          edge, nodes_[e_spec.tail_node()].get().outputs()[e_spec.tail_port()]);
      EP::connect_head(
          edge, nodes_[e_spec.head_node()].get().inputs()[e_spec.head_port()]);
       */
      /*
       * Not doing it by passing containers and indices as arguments:
       * ```
       *   EP::connect_tail(??, i, e_spec.tail_node(), e_spec.tail_port());
       *   EP::connect_head(??, i, e_spec.head_node(), e_spec.head_port());
       * ```
       * The problem is how to get a node list and an edge list into a
       * platform-defined function. Regardless of how we might do it, it's
       * injecting a type dependency into a platform function, which sets up a
       * form of cyclic dependency. We're avoiding that issue entirely by
       * passing object references.
       */
    }
  }

  [[nodiscard]] const node_list_type& nodes() const {
    return nodes_;
  }
  [[nodiscard]] const edge_list_type& edges() const {
    return edges_;
  }
};

}  // namespace tiledb::flow_graph

#endif  // TILEDB_FLOW_GRAPH_LIBRARY_GRAPH_FLOW_GRAPH_H
