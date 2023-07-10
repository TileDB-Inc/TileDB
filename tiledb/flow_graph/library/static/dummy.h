/**
 * @file flow_graph/library/dummy/dummy.cc
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

#ifndef TILEDB_FLOW_GRAPH_DUMMY_H
#define TILEDB_FLOW_GRAPH_DUMMY_H

#include "../../node.h"

namespace tiledb::flow_graph {

//----------------------------------
// Ports
//----------------------------------
template <class T>
class DummyOutputPortSpecification {
 public:
  static constexpr struct invariant_type {
    static constexpr bool i_am_output_port_static_specification{true};
  } invariants;
  using flow_type = T;
};

template <class T>
class DummyInputPortSpecification {
 public:
  static constexpr struct invariant_type {
    static constexpr bool i_am_input_port_static_specification{true};
  } invariants;
  using flow_type = T;
};

//----------------------------------
// Node State
//----------------------------------
/**
 * Dummy specification nodes would ordinarily have no storage, because they
 * don't do anything and thus don't have internal state. Static specification
 * nodes may be `constexpr` and need something to distinguish them in order to
 * implement `operator==`. This class is a placeholder with "state" that does
 * nothing, but requires objects of this class to have different addresses. This
 * allows implementing `operator==` with an address equality.
 */
class DummyNodeState {
  [[maybe_unused]] bool useless_{false};

 public:
  DummyNodeState() = default;
};

//----------------------------------
// Output Node
//----------------------------------
/**
 * The dummy output node has a single output node of the designated type.
 *
 * @tparam T flow type of the output port
 */
template <class T, node_services NS>
class DummyOutputNode {
 public:
  static constexpr struct invariant_type {
    static constexpr bool i_am_node_body{true};
  } invariants;
  void operator()(){};
};

/**
 * Regular specification for the dummy output Node.
 *
 * @tparam T flow type of the output port
 */
template <class T>
class DummyOutputNodeSpecification {
  using self = DummyOutputNodeSpecification;
  [[maybe_unused]] DummyNodeState unused_;

 public:
  static constexpr struct invariant_type {
    static constexpr bool i_am_node_static_specification{true};
  } invariants;
  static constexpr DummyOutputPortSpecification<T> output{};
  static constexpr reference_tuple input_ports{};
  static constexpr reference_tuple output_ports{output};
  template <node_services NS>
  using node_body_template = DummyOutputNode<T, NS>;
};

//----------------------------------
// Input Node
//----------------------------------
/**
 * The dummy output node has a single output node of the designated type.
 *
 * @tparam T flow type of the output port
 */
template <class T, node_services NS>
class DummyInputNode {
 public:
  static constexpr struct invariant_type {
    static constexpr bool i_am_node_body{true};
  } invariants;
  void operator()(){};
};

/**
 * Regular specification for the dummy output Node.
 *
 * @tparam T flow type of the output port
 */
template <class T>
class DummyInputNodeSpecification {
  using self = DummyInputNodeSpecification;
  [[maybe_unused]] DummyNodeState unused_;

 public:
  static constexpr struct invariant_type {
    static constexpr bool i_am_node_static_specification{true};
  } invariants;
  static constexpr DummyInputPortSpecification<T> input{};
  static constexpr reference_tuple input_ports{input};
  static constexpr reference_tuple output_ports{};
  template <node_services NS>
  using node_body_template = DummyInputNode<T, NS>;
};

//----------------------------------
// Edge
//----------------------------------
/**
 * Edge from node reference and port reference
 *
 * @tparam TailNode The type of the tail node
 * @tparam TailPort The type of an output port
 * @tparam HeadNode The type of the head node
 * @tparam HeadPort The type of an input port
 */
template <class TailNode, class TailPort, class HeadNode, class HeadPort>
class DummyEdgeSpecification {
  using self = DummyEdgeSpecification;

 public:
  static constexpr struct invariant_type {
    static constexpr bool i_am_edge_static_specification{true};
  } invariants;
  const TailNode& tail_node;
  const TailPort& tail_port;
  const HeadNode& head_node;
  const HeadPort& head_port;
  using tail_node_type = TailNode;
  using tail_port_type = TailPort;
  using head_node_type = HeadNode;
  using head_port_type = HeadPort;
  constexpr DummyEdgeSpecification(
      const TailNode& tn,
      const TailPort& tp,
      const HeadNode& hn,
      const HeadPort& hp)
      : tail_node{tn}
      , tail_port{tp}
      , head_node{hn}
      , head_port{hp} {
  }
};
template <
    class TailNode,
    class TailPortPointer,
    class HeadNode,
    class HeadPortPointer>
DummyEdgeSpecification(
    const TailNode&,
    const TailPortPointer&,
    const HeadNode&,
    const HeadPortPointer&)
    -> DummyEdgeSpecification<
        TailNode,
        TailPortPointer,
        HeadNode,
        HeadPortPointer>;

}  // namespace tiledb::flow_graph
#endif  // TILEDB_FLOW_GRAPH_DUMMY_H
