/**
 * @file flow_graph/library/basic/basic_execution_platform.h
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

#ifndef TILEDB_FLOW_GRAPH_BASIC_EXECUTION_PLATFORM_H
#define TILEDB_FLOW_GRAPH_BASIC_EXECUTION_PLATFORM_H

#include <memory>
#include "../../system/edge_dynamic_specification.h"
#include "../../system/node_body.h"
#include "../../system/node_dynamic_specification.h"

namespace tiledb::flow_graph {

//-------------------------------------------------------
// Type erasure in the graph construction process
//-------------------------------------------------------
/*
 * Flow graphs have multiple types of data flowing across edges to multiple
 * kinds of nodes. The behavior of a scheduler, however, does not depend upon
 * these types. The scheduler depends upon the coroutine state of each node and
 * and it depends on the I/O states of the nodes and edges. These states are
 * independent of the flow types and individual nodes.
 *
 * Somewhere between the initial specification and final execution objects, all
 * the types irrelevant to scheduling must be encapsulated and erased in the
 * final objects. This flow graph system uses a few fundamental conventions:
 *
 * (1) Each specification object holds all the variation needed to construct an
 * underlying execution object. It does this by providing factories for these
 * objects.
 *
 * (2) Underlying objects are uniformly adaptable. The manifestation of this
 * principle is that each underlying component (port, node, edge, etc.) satisfy
 * an appropriate concept. Each adapter has a template argument of this concept
 * type. A set of thin adapter classes provide a uniform interface to execution
 * classes.
 *
 * (3) Constructors for execution objects take a specification object as an
 * argument. Internally, these constructor call the specification factory,
 * wrap it in an adapter, and store the wrapped underlying object as a member
 * variable.
 *
 * The end result of this is that a single scheduler class can control multiple
 * different graphs. If this were not the case, schedulers would have to be
 * compiled with at least some of the type variation present in the
 * specification. This would lead to object code bloat, as there would be a
 * compiled scheduler for each graph.
 */

//-------------------------------------------------------
// Execution Platform
//-------------------------------------------------------
/*
 * A execution platform consists of all elements necessary to instantiate nodes
 * and graphs from their specifications.
 *
 * The essence of a graph are its nodes and the topology of their connections.
 * This skeleton, however, needs to be fleshed out with all the supporting
 * classes that perform the generic parts of flow graph execution. The design
 * decision is to define these parts uniformly across a graph. Thus, for
 * example, all edges derive from the same class template.
 *
 * An alternative might allow variation in platform classes, say, having
 * different edges of different types. While this is technically possible, it
 * makes writing schedulers to deal with such non-uniformity far more difficult.
 * Furthermore, allowing such variety has a strong tendency toward object code
 * bloat as scheduling code is duplicated for each graph that's executed.
 *
 * The node and edge classes in an execution platform cannot all be C.41
 * compliant. Consider a node and an edge that are connected. Either the node
 * constructor or edge constructor must run first. An object cannot be fully
 * initialized before it's connected, and it can't be connected before the
 * object it's being connected to exists. Thus at least one of these classes
 * can't be C.41 compliant.
 *
 * Given this situation, there are three kinds of platform conventions around
 * construction.
 *   1. Construct nodes and edges independently and connect them with explicit
 *     function calls.
 *   2. Construct nodes first. Pass a pair of node references or identifiers to
 *     edge constructors.
 *   3. Construct edges first. Pass a list of edge references or identifiers to
 *     node constructors.
 * The graph constructor and the platform must use the same convention.
 *
 * The initial versions of the  classes `BasicExecutionPlatform` and `FlowGraph`
 * implement the first convention. This design selection is somewhat arbitrary;
 * any of these could be made to work. The decision was to pick the one with the
 * clearest code and the simplest declarations.
 */

//-------------------------------------------------------
// BasicExecutionPlatform
//-------------------------------------------------------
namespace basic_execution_parameter {
using nodes_size_type = size_t;
using edges_size_type = size_t;
using ports_size_type = size_t;
};  // namespace basic_execution_parameter

// Forward declaration enables friend class declarations.
class BasicExecutionPlatform;

/**
 * Maturity Note: This class does not execute. It's only function at present is
 * to exercise the graph construction regime. It captures topological
 * information in order to test that graph topology passes faithfully from
 * specification to execution.
 */
class BasicExecutionInputPort {
  friend class BasicExecutionPlatform;

  /**
   * The index of the node containing this port.
   *
   * The index is into the node list within the execution graph that contains
   * the node that contains this port. It's initialized at construction by the
   * node constructor.
   */
  basic_execution_parameter::nodes_size_type node_index_;

  /**
   * The index of this port within its node.
   *
   * The index is into the port list within the execution node that contains
   * this port. It's initialized at construction by the node constructor.
   */
  basic_execution_parameter::ports_size_type port_index_;

  /**
   * The index of the edge whose head is attached to this port.
   *
   * The index is into the edge list within the execution graph that contains
   * the node that contains this port. It's initialized during graph
   * construction immediately upon construction of the edge.
   */
  basic_execution_parameter::edges_size_type edge_index_;

 public:
  template <input_port_dynamic_specification T>
  explicit BasicExecutionInputPort(
      basic_execution_parameter::nodes_size_type node_index,
      basic_execution_parameter::ports_size_type port_index,
      const T&)
      : node_index_(node_index)
      , port_index_(port_index)
      , edge_index_(std::numeric_limits<decltype(edge_index_)>::max()){};

  [[nodiscard]] decltype(node_index_) node_index() const {
    return node_index_;
  }
  [[nodiscard]] decltype(edge_index_) edge_index() const {
    return edge_index_;
  }
};

/**
 * Maturity Note: This class does not execute. It's only function at present is
 * to exercise the graph construction regime. It captures topological
 * information in order to test that graph topology passes faithfully from
 * specification to execution.
 */
class BasicExecutionOutputPort {
  friend class BasicExecutionPlatform;

  /**
   * The index of the node containing this port.
   *
   * The index is into the node list within the execution graph that contains
   * the node that contains this port. It's initialized at construction by the
   * node constructor.
   */
  basic_execution_parameter::nodes_size_type node_index_;

  /**
   * The index of this port within its node.
   *
   * The index is into the port list within the execution node that contains
   * this port. It's initialized at construction by the node constructor.
   */
  basic_execution_parameter::ports_size_type port_index_;

  /**
   * The index of the edge whose head is attached to this port.
   *
   * The index is into the edge list within the execution graph that contains
   * the node that contains this port. It's initialized during graph
   * construction immediately upon construction of the edge.
   */
  basic_execution_parameter::edges_size_type edge_index_;

 public:
  template <output_port_dynamic_specification T>
  explicit BasicExecutionOutputPort(
      basic_execution_parameter::nodes_size_type node_index,
      basic_execution_parameter::ports_size_type port_index,
      const T&)
      : node_index_(node_index)
      , port_index_(port_index)
      , edge_index_(std::numeric_limits<decltype(edge_index_)>::max()){};

  [[nodiscard]] decltype(node_index_) node_index() const {
    return node_index_;
  }
  [[nodiscard]] decltype(edge_index_) edge_index() const {
    return edge_index_;
  }
};

/**
 * This nody body class is the nexus for type erasure of node specifications.
 *
 * All node bodies need to derive from this base or be adapted to it with some
 * standard adapter. It's the responsibility of a dynamic specification to use
 * an adapter if needed.
 */
class BasicExecutionNodeBody {
 public:
  virtual ~BasicExecutionNodeBody() = default;
  /**
   * Coroutine-style body function. This signature is a placeholder for the
   * actual one we'll use in the future.
   */
  virtual void operator()() {
  }
};

template <template <class> class T>
  requires node_body<T>
class BasicExecutionNodeBodyAdapter;

/**
 * Adapter for trivially destructible node bodies.
 *
 * @tparam T
 */
template <template <class> class T>
  requires execution::node_body_trivially_destructible<T>
class BasicExecutionNodeBodyAdapter<T> : public BasicExecutionNodeBody {
 public:
  BasicExecutionNodeBodyAdapter() = default;
  /**
   * Destructor doesn't need to do anything because it's adapting a class with
   * a trivial destructor.
   */
  ~BasicExecutionNodeBodyAdapter() = default;
};

/**
 * Adapter for node bodies with a virtual destructor.
 * NOT YET IMPLEMENTED
 *
 * @tparam T
 */
template <template <class> class T>
  requires execution::node_body_with_virtual_destructor<T>
class BasicExecutionNodeBodyAdapter<T> : public BasicExecutionNodeBody {};

/**
 * The execution node within `BasicExecutionPlatform`.
 *
 * This class is not C.41-compliant in isolation. It's a component class of
 * `BasicExecutionPlatform` and needs to be constructed accordingly.
 * Specifically, an instance of this class has unconnected ports upon
 * construction. Unconnected ports are inoperable and not fully initialized.
 * Connecting an edge to a port completes the initialization of that port
 * object. Initializing all ports within an instance of this class completes the
 * initialization of the node object.
 *
 * Maturity Note: This version only deals with construction order. It does not
 * even begin to implement anything about the actual I/O system.
 */
class BasicExecutionNode {
  friend class BasicExecutionPlatform;

  /**
   * The index of the node within its graph.
   *
   * The index is into the node list within the execution graph that contains
   * this node. It's initialized at construction by the node constructor.
   */
  basic_execution_parameter::nodes_size_type node_index_;

 public:
  using input_list_type =
      std::vector<std::reference_wrapper<BasicExecutionInputPort>>;
  using output_list_type =
      std::vector<std::reference_wrapper<BasicExecutionOutputPort>>;

 private:
  input_list_type inputs_{};
  output_list_type outputs_{};

  /**
   * Storage for a node body is typed as a byte array that can be default
   * initialized for overwrite by the actual object.
   */
  std::shared_ptr<std::byte[]> node_body_ptr_;

  /**
   * Reference to the node body is an interpretation of the raw storage in
   * `node_body_ptr` as a typed object.
   */
  BasicExecutionNodeBody& node_body_;

 public:
  template <node_dynamic_specification T>
  explicit BasicExecutionNode(
      basic_execution_parameter::nodes_size_type, const T& node_spec)
      : node_body_ptr_{std::make_shared_for_overwrite<std::byte[]>(
            node_spec.size_of_node())}
      // Caveat: node_body_ is not valid until after the factory returns
      , node_body_(
            *reinterpret_cast<BasicExecutionNodeBody*>(node_body_ptr_.get())) {
    /*
     * Construct the node body in the place allocated for it.
     */
    node_spec.make(&node_body_);
  }

  ~BasicExecutionNode() {
    /*
     * We must explicitly call the `node_body_` destructor because there's
     * nothing declared that will call it implicitly.
     */
    node_body_.~BasicExecutionNodeBody();
  }

  [[nodiscard]] const input_list_type& inputs() const {
    return inputs_;
  }
  [[nodiscard]] const output_list_type& outputs() const {
    return outputs_;
  }
};

/**
 * Node services class of the basic execution platform
 *
 * Maturity Note: This class does not have anything. All it needs to do for the
 * moment is exist.
 */
class BasicExecutionNodeServices {};

/**
 * The execution edge within `BasicExecutionPlatform`.
 *
 * This class is not C.41-compliant in isolation. It's a component class of
 * `BasicExecutionPlatform` and needs to be constructed accordingly.
 * Specifically, an instance of this class has an unconnected head and an
 * unconnected tail upon construction. The head and tail are inoperable and the
 * edge is not fully initialized. Connecting the head to an input port and the
 * tail to an output port completes the initialization of the edge.
 */
class BasicExecutionEdge {
  friend class BasicExecutionPlatform;

  /**
   * The index of the edge within its graph.
   *
   * The index is into the edge list within the execution graph that this edge.
   * It's initialized at construction.
   */
  basic_execution_parameter::edges_size_type edge_index_;

  /**
   * The index of the node containing the port connected to the tail of this
   * edge.
   *
   * The index is into the node list within the execution graph that contains
   * this edge. It's initialized after construction.
   */
  basic_execution_parameter::nodes_size_type tail_node_index_;

  /**
   * The index of the port within the tail node.
   *
   * The index is into the port list of the tail node. It's initialized after
   * construction.
   */
  basic_execution_parameter::ports_size_type tail_port_index_;

  /**
   * The index of the node containing the port connected to the head of this
   * edge.
   *
   * The index is into the node list within the execution graph that contains
   * this edge. It's initialized after construction.
   */
  basic_execution_parameter::nodes_size_type head_node_index_;

  /**
   * The index of the port within the head node.
   *
   * The index is into the port list of the head node. It's initialized after
   * construction.
   */
  basic_execution_parameter::ports_size_type head_port_index_;

 public:
  template <edge_dynamic_specification T>
  explicit BasicExecutionEdge(
      basic_execution_parameter::edges_size_type edge_index, const T&)
      : edge_index_(edge_index) {
  }

  [[nodiscard]] basic_execution_parameter::edges_size_type edge_index() const {
    return edge_index_;
  }
};

/**
 * The basic execution platform is a reference implementation of the execution
 * platform concept.
 *
 * The `connect_head` and `connect_tail` functions are defined in the platform,
 * rather than in a graph component, in order to allow a platform to remain
 * agnostic about how the connection is made. It might be a member function
 * either on the edge or the port, or it might operate directly on the edge and
 * port simultaneously.
 */
class BasicExecutionPlatform {
 public:
  using node_type = BasicExecutionNode;
  template <template <class> class T>
    requires node_body<T>
  using node_body_adapter = BasicExecutionNodeBodyAdapter<T>;
  using node_services_type = BasicExecutionNodeServices;
  using edge_type = BasicExecutionEdge;
  template <class Edge, class Port>
  static void connect_head(Edge&, Port&) {
  }
  template <class Edge, class Port>
  static void connect_tail(Edge&, Port&) {
  }
};

}  // namespace tiledb::flow_graph

#endif  // TILEDB_FLOW_GRAPH_BASIC_EXECUTION_PLATFORM_H
