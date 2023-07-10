/**
 * @file flow_graph/system/edge_static_specification.h
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

#ifndef TILEDB_FLOW_GRAPH_EDGE_STATIC_SPECIFICATION_H
#define TILEDB_FLOW_GRAPH_EDGE_STATIC_SPECIFICATION_H

#include "graph_type.h"
#include "tiledb/flow_graph/system/node_static_specification.h"
#include "tiledb/flow_graph/system/port_static_specification.h"

namespace tiledb::flow_graph::static_specification {

/**
 * The class declares itself as an edge static specification.
 *
 * @tparam T a potential edge static specification class
 */
template <class T>
concept self_declared_as_edge_static_specification =
    requires(T x) { x.invariants.i_am_edge_static_specification; };

/**
 * The class declares a type of a tail node.
 *
 * @tparam T a potential edge static specification class
 */
template <class T>
concept declares_tail_node_type = requires(T) { typename T::tail_node_type; };

/**
 * The tail node type is a node specification.
 *
 * @tparam T a potential edge static specification class
 */
template <class T>
concept tail_node_type_is_node =
    declares_tail_node_type<T> && node<typename T::tail_node_type>;

/**
 * The class defines a tail node.
 *
 * @tparam T a potential edge static specification class
 */
template <class T>
concept defines_tail_node = requires(T x) { x.tail_node; };

/**
 * The tail node class matches its declared type.
 *
 * @tparam T a potential edge static specification class
 */
template <class T>
concept has_tail_node =
    tail_node_type_is_node<T> && defines_tail_node<T> &&
    std::same_as<decltype(T::tail_node), const typename T::tail_node_type&>;

/**
 * The class declares a type for its tail port.
 *
 * @tparam T a potential edge static specification class
 */
template <class T>
concept declares_tail_port_type = requires(T) { typename T::tail_port_type; };

/**
 * The type of the tail port is an output port.
 *
 * @tparam T a potential edge static specification class
 */
template <class T>
concept tail_port_type_is_port =
    declares_tail_port_type<T> && output_port<typename T::tail_port_type>;

/**
 * The class defines a tail port.
 *
 * @tparam T a potential edge static specification class
 */
template <class T>
concept defines_tail_port = requires(T x) { x.tail_port; };

/**
 * The tail port class matches its declared type.
 *
 * @tparam T a potential edge static specification class
 */
template <class T>
concept has_tail_port =
    defines_tail_port<T> &&
    std::same_as<decltype(T::tail_port), const typename T::tail_port_type&>;

/**
 * The tail port is an output port of the tail node.
 *
 * @tparam T a potential edge static specification class
 */
template <class T>
concept has_tail = has_tail_node<T> && has_tail_port<T> && requires(T x) {
  is_reference_element_of(x.tail_port, x.tail_node.output_ports);
};

/**
 * The class declares a type of a head node.
 *
 * @tparam T a potential edge static specification class
 */
template <class T>
concept declares_head_node_type = requires(T) { typename T::head_node_type; };

/**
 * The head node type is a node specification.
 *
 * @tparam T a potential edge static specification class
 */
template <class T>
concept head_node_type_is_node =
    declares_head_node_type<T> && node<typename T::head_node_type>;

/**
 * The class defines a head node.
 *
 * @tparam T a potential edge static specification class
 */
template <class T>
concept defines_head_node = requires(T x) { x.head_node; };

/**
 * The head node class matches its declared type.
 *
 * @tparam T a potential edge static specification class
 */
template <class T>
concept has_head_node =
    head_node_type_is_node<T> && defines_head_node<T> &&
    std::same_as<decltype(T::head_node), const typename T::head_node_type&>;

/**
 * The class declares a type for its head port.
 *
 * @tparam T a potential edge static specification class
 */
template <class T>
concept declares_head_port_type = requires(T) { typename T::head_port_type; };

/**
 * The type of the head port is an output port.
 *
 * @tparam T a potential edge static specification class
 */
template <class T>
concept head_port_type_is_port =
    declares_head_port_type<T> &&
    input_port_static_specification<typename T::head_port_type>;

/**
 * The class defines a head port.
 *
 * @tparam T a potential edge static specification class
 */
template <class T>
concept defines_head_port = requires(T x) { x.head_port; };

/**
 * The head port class matches its declared type.
 *
 * @tparam T a potential edge static specification class
 */
template <class T>
concept has_head_port =
    defines_tail_port<T> &&
    std::same_as<decltype(T::head_port), const typename T::head_port_type&>;

/**
 * The head port is an output port of the tail node.
 *
 * @tparam T a potential edge static specification class
 */
template <class T>
concept has_head = has_head_node<T> && has_head_port<T> && requires(T x) {
  is_reference_element_of(x.head_port, x.head_node.input_ports);
};

/**
 * An edge static specification is self-declared. It has a well-defined head and
 * tail whose flow types match.
 *
 * @tparam T a potential edge static specification class
 */
template <class T>
concept edge = self_declared_as_edge_static_specification<T> && has_tail<T> &&
               has_head<T> &&
               std::same_as<
                   typename T::tail_port_type::flow_type,
                   typename T::head_port_type::flow_type>;

/**
 * Compile-time class function validates that class is an edge static
 * specification.
 *
 * This function contains a static assertion for each concept that feeds into
 * `edge`. If the concept fails, this validator will also fail, but with a line
 * number that identifies which elements are failing. Such errors messages may
 * be more legible than those from a concept failure.
 */
template <class T>
  requires self_declared_as_edge_static_specification<T>
void validator() {
  // tail
  static_assert(declares_tail_node_type<T>);
  static_assert(tail_node_type_is_node<T>);
  static_assert(defines_tail_node<T>);
  static_assert(has_tail_node<T>);
  static_assert(declares_tail_port_type<T>);
  static_assert(tail_port_type_is_port<T>);
  static_assert(defines_tail_port<T>);
  static_assert(has_tail_port<T>);
  static_assert(has_tail<T>);
  // head
  static_assert(declares_head_node_type<T>);
  static_assert(head_node_type_is_node<T>);
  static_assert(defines_head_node<T>);
  static_assert(has_head_node<T>);
  static_assert(declares_head_port_type<T>);
  static_assert(head_port_type_is_port<T>);
  static_assert(defines_head_port<T>);
  static_assert(has_head_port<T>);
  static_assert(has_head<T>);
  // edge
  static_assert(edge<T>);
}

}  // namespace tiledb::flow_graph::static_specification

namespace tiledb::flow_graph {

/**
 * An edge static specification.
 *
 * @tparam T a potential edge static specification class
 */
template <class T>
concept edge_static_specification = static_specification::edge<T>;

}  // namespace tiledb::flow_graph
#endif  // TILEDB_FLOW_GRAPH_SPECIFICATION_EDGE_H
