/**
 * @file flow_graph/library/dynamic/to_dynamic_reference.h
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
 * @section DESCRIPTION Static and dynamic graph specification. Conversion of
 * static specifications to dynamic ones.
 */

/**
 * @section Static vs. Dynamic Specification
 *
 * Common Graph Specification
 * - A graph class may optionally specify a global state type.
 *   (Not yet implemented)
 *   - If specified, the graph requires a constructor that takes either a
 *     global state value or a reference to a global state object.
 *   - If absent, the graph requires a default constructor.
 * - Specification nodes and edges are factories for execution nodes and edges.
 * - Specification ports are accessors into execution ports.
 *   - The means of access change
 *
 * Static Graph Specification
 * - Each graph class specifies a graph with a fixed set of nodes and edges.
 *   - All graph objects have the same underlying graph.
 * - Each node may be of a different type.
 * - Ports on nodes are referenced by a member pointer of the node.
 * - Each edge may be of a different type.
 * - Each node is a specification node member variable of the graph.
 *   - A specification node has a static tuple of its input and output ports.
 * - The graph contains a static tuple of nodes references.
 * - Edges are anonymous by default
 *   - Nothing in the framework requires a named edge.
 * - The graph contains a static tuple of edges.
 *
 * Dynamic Graph Specification
 * - Each graph object specifies a graph with a fixed set of nodes and edges.
 *   - Each constructor must supply the parameters needed to fully specify the
 *     underling graph.
 * - Each node is of the same type, as defined by the graph class
 *   - Since node implementation class are all different, this requires some
 *     kind of type erasure mechanism with derivation from a common base
 *     class and possibly adapter classes or an adapter class template.
 * - Ports on nodes are referenced by index.
 *   - The index type is an unsigned integral type defined by the graph class.
 *   - Indices need not be contiguous.
 * - Each edge is of the same type, as defined by the graph class
 *   - Edges can be the same type based on typed-erased nodes, or they might
 *     use type erasure analogously as the nodes do.
 * - Nodes and edges are anonymous.
 *   - The dynamic framework uses neither node names nor edge names.
 * - Each graph object has two forward-iterable containers: one for nodes and
 *   one for edges
 * - Edge tails and heads are specified as a pair: an iterator reference to a
 *   node and a port index.
 *
 * Canonical conversion of static specification to dynamic specification
 * - The framework provides adapter class templates for static specification
 *   nodes and edges.
 * - The node and edge lists generate an array of adapter objects.
 * - The port lists of nodes are already accessible by index.
 */

#ifndef TILEDB_FLOW_GRAPH_LIBRARY_DYNAMIC_TO_DYNAMIC_REFERENCE_H
#define TILEDB_FLOW_GRAPH_LIBRARY_DYNAMIC_TO_DYNAMIC_REFERENCE_H

#include <concepts>
#include <tuple>
#include <vector>

#include "../../general_factory.h"
#include "../../system/execution_platform.h"
#include "../../system/graph_dynamic_specification.h"
#include "../../system/graph_static_specification.h"
#include "../../system/graph_type.h"

namespace tiledb::flow_graph {
//-------------------------------------------------------
// ToDynamicReference
//-------------------------------------------------------
/*
 * The class template `ToDynamic` is a particular way of creating dynamic
 * specifications from static specifications. Static specifications are complete
 * about the topology of their graphs but are incomplete about the specific
 * types involved. There are two particular ways in which static specifications
 * are incomplete:
 *   1. A node body in a static specification has an abstract I/O interface
 *     to the graph as a whole. A node body requires a node services template
 *     argument and itself is a template argument to a full node class.
 *   2. An edge in a static specification has its connectivity information and
 *     perhaps certain parameters, but not a specific edge class. The edge
 *     information provides arguments to the edge constructor and to the graph
 *     builder.
 *
 * Both kinds of information are left out of static specifications in service of
 * a consistent system of data flow within the graph. This structure allows the
 * separation of two important concerns:
 *   1. What the graph does. This includes the behaviors of nodes, the graph
 *     topology, and what data flows across edges.
 *   2. How the data moves. This includes the classes for edges, classes for
 *     ports, and how node bodies perform I/O on their ports.
 *
 * In order to accomplish this separation, the template argument of `ToDynamic`
 * is a policy class that specifies all the parameters needed to construct an
 * executable flow graph. This template argument is called an execution
 * platform.
 */
namespace detail::to_dynamic {

using nodes_size_type = size_t;
using edges_size_type = size_t;
using ports_size_type = size_t;

/**
 * The only required datum for an input port is the size of its flow type.
 */
class TDRInputPortSpecification {
  size_t flow_type_size_;

 public:
  explicit TDRInputPortSpecification(size_t flow_type_size)
      : flow_type_size_(flow_type_size) {
  }
};

/*
 * The only required datum for an output port is the size of its flow type.
 */
class TDROutputPortSpecification {
  size_t flow_type_size_;

 public:
  explicit TDROutputPortSpecification(size_t flow_type_size)
      : flow_type_size_(flow_type_size) {
  }
};

/**
 * A function declared to be used as a template argument for `GenericFactory`.
 * This function has no arguments, so the factory uses the default constructor.
 *
 * The function is not defined and should not be referenced other than as a
 * template argument to generic factories. In those templates it's only used
 * for template argument deduction and not called.
 */
void node_factory_prototype();

/**
 * Node specification within ToDynamic.
 *
 * @tparam EP
 */
template <execution_platform EP>
class TDRNodeSpecification {
  std::vector<TDRInputPortSpecification> inputs_{};
  std::vector<TDROutputPortSpecification> outputs_{};
  ClassFactory<node_factory_prototype> factory_;
  const ports_size_type inputs_size_;
  const ports_size_type outputs_size_;
  const size_t size_of_node_;

 public:
  template <class U>
  using node_body_type =
      EP::template node_body_adapter<U::template node_body_template>;

  template <node_static_specification V>
  explicit constexpr TDRNodeSpecification(const V& static_spec)
      : inputs_size_{number_of_input_ports<V>}
      , outputs_size_{number_of_output_ports<V>}
      , factory_{ForClass<node_body_type<V>>}
      , size_of_node_{sizeof(node_body_type<V>)} {
    // Node factory

    // Input ports
    if constexpr (number_of_input_ports<V> > 0) {
      [&static_spec]<std::size_t... I>(
          std::vector<TDRInputPortSpecification>& inputs,
          std::index_sequence<I...>) {
        (inputs.emplace_back(TDRInputPortSpecification{
             flow_size(std::get<I>(static_spec.input_ports))}),
         ...);
      }(inputs_, std::make_index_sequence<number_of_input_ports<V>>{});
    }

    // Output ports
    if constexpr (number_of_output_ports<V> > 0) {
      [&static_spec]<std::size_t... I>(
          std::vector<TDROutputPortSpecification>& outputs,
          std::index_sequence<I...>) {
        (outputs.emplace_back(TDROutputPortSpecification{
             flow_size(std::get<I>(static_spec.output_ports))}),
         ...);
      }(outputs_, std::make_index_sequence<number_of_output_ports<V>>{});
    }
  }

  [[nodiscard]] constexpr ports_size_type inputs_size() const {
    return inputs_size_;
  }
  [[nodiscard]] constexpr ports_size_type outputs_size() const {
    return outputs_size_;
  }
  [[nodiscard]] const TDRInputPortSpecification* inputs() const {
    return inputs_.data();
  }
  [[nodiscard]] const TDROutputPortSpecification* outputs() const {
    return outputs_.data();
  }
  [[nodiscard]] constexpr size_t size_of_node() const {
    return size_of_node_;
  }
  /**
   * Precondition:
   */
  void make(void* p) const {
    factory_.make(p);
  }
};

template <class EP>
class TDREdgeSpecification {
  /*
   * Obviously these initializations are wrong.
   */
  nodes_size_type tail_node_{0};
  nodes_size_type head_node_{0};
  ports_size_type tail_port_{0};
  ports_size_type head_port_{0};

 public:
  static constexpr struct invariant_type {
    static constexpr bool i_am_edge_dynamic_specification{true};
  } invariants;
  template <edge_static_specification T>
  TDREdgeSpecification(size_t, const T&) {
  }
  [[nodiscard]] nodes_size_type tail_node() const {
    return tail_node_;
  }
  [[nodiscard]] nodes_size_type head_node() const {
    return head_node_;
  }
  [[nodiscard]] ports_size_type tail_port() const {
    return tail_port_;
  }
  [[nodiscard]] ports_size_type head_port() const {
    return head_port_;
  }
};

}  // namespace detail::to_dynamic

/**
 * Adapter that presents a static specification graph as a dynamic one.
 *
 * Invariant: For all `graph_static_specification T`, `ToDynamic<T>` satisfies
 * the concept `graph_dynamic_specification`.
 *
 * This is an early implementation. It does not attempt to avoid allocating and
 * copying. For simplicity it uses `std::vector`. The main goal at the outset
 * is to ensure that we have a graph construction discipline that passes through
 * a dynamic specification that uses integral indices. Other goals are secondary
 * at this stage.
 *
 * That said, all the data (indices, sizes, etc.) needed for a dynamic
 * specification could be generated as `constexpr` and then presented in a way
 * that satisfies the dynamic specification concept.
 *
 * @tparam G a static graph specification
 * @tparam EP an execution platform
 */
template <graph_static_specification G, class EP>
class ToDynamicReference {
 public:
  static constexpr struct invariant_type {
    static constexpr bool i_am_graph_dynamic_specification{true};
  } invariants;

  using execution_platform_type = EP;
  using nodes_size_type = detail::to_dynamic::nodes_size_type;
  using edges_size_type = detail::to_dynamic::edges_size_type;
  using ports_size_type = detail::to_dynamic::ports_size_type;

 private:
  /*
   * The user only references the components of a dynamic specification through
   * the concept and types them using `auto`. Component classes don't
   * need public-facing names. These aliases are private.
   */
  using node_type = detail::to_dynamic::TDRNodeSpecification<EP>;
  using edge_spec = detail::to_dynamic::TDREdgeSpecification<EP>;

  constexpr static auto nodes_size_{std::tuple_size_v<decltype(G::nodes)>};
  constexpr static auto edges_size_{std::tuple_size_v<decltype(G::edges)>};
  std::vector<node_type> nodes_{};
  std::vector<edge_spec> edges_{};

 public:
  template <class... Args>
  explicit constexpr ToDynamicReference(Args&&...) {
    []<std::size_t... I>(
        std::vector<node_type>& nodes, std::index_sequence<I...>) {
      (nodes.emplace_back(node_type{std::get<I>(G::nodes)}), ...);
    }(nodes_, std::make_index_sequence<nodes_size_>{});
    []<std::size_t... I>(
        std::vector<edge_spec>& edges, std::index_sequence<I...>) {
      (edges.emplace_back(edge_spec{I, std::get<I>(G::edges)}), ...);
    }(edges_, std::make_index_sequence<edges_size_>{});
  }
  [[nodiscard]] nodes_size_type nodes_size() const {
    return nodes_size_;
  }
  [[nodiscard]] edges_size_type edges_size() const {
    return edges_size_;
  }
  [[nodiscard]] const node_type* nodes() const {
    return nodes_.data();
  }
  [[nodiscard]] const edge_spec* edges() const {
    return edges_.data();
  }

  [[nodiscard]] EP::node_type make_node(nodes_size_type i) const {
    return typename EP::node_type{i, nodes_[i]};
  }

  EP::edge_type make_edge(edges_size_type i) const {
    return typename EP::edge_type{i, edges_[i]};
  }
};

}  // namespace tiledb::flow_graph
#endif  // TILEDB_FLOW_GRAPH_LIBRARY_DYNAMIC_TO_DYNAMIC_REFERENCE_H
