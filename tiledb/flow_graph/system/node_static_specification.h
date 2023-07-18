/**
 * @file flow_graph/system/node_static_specification.h
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

#ifndef TILEDB_FLOW_GRAPH_NODE_STATIC_SPECIFICATION_H
#define TILEDB_FLOW_GRAPH_NODE_STATIC_SPECIFICATION_H

#include "graph_type.h"
#include "node_body.h"
#include "port_static_specification.h"
#include "tiledb/stdx/type_traits.h"

namespace tiledb::flow_graph::static_specification {
namespace {
using MNS = tiledb::flow_graph::execution::MinimalNodeServices;
}  // namespace

//-------------------------------------------------------
// Node
//-------------------------------------------------------
/**
 * Specification nodes must declare themselves as such.
 *
 * @tparam T a potential node specification class
 */
template <class T>
concept self_declared_as_node_static_specification =
    requires(T x) { x.invariants.i_am_node_static_specification; };

/**
 * 'T' has a member class template `node_body_template`
 *
 * @tparam T a potential node specification class
 */
template <class T>
concept declares_node_body_template =
    requires(T) { typename T::template node_body_template<MNS>; };

template <class T>
concept node_body_template_satisfies_node_body =
    node_body<T::template node_body_template>;

template <class T>
concept has_nody_body =
    declares_node_body_template<T> && node_body_template_satisfies_node_body<T>;

/**
 * Class declares a member variable `input_ports`.
 *
 * @tparam T a potential node specification class
 */
template <class T>
concept declares_input_port_list = requires(T x) {
  { T::input_ports };
};

/**
 * Member variable `input_ports` is tuple-like for `std::apply`.
 *
 * @tparam T a potential node specification class
 */
template <class T>
concept input_port_list_is_tuplelike =
    declares_input_port_list<T> &&
    requires(T x) { applicable<decltype(x.input_ports)>; };

/**
 * Member variable `input_ports` contains only lvalue references.
 *
 * @tparam T a potential node specification class
 */
template <class T>
concept input_port_list_contains_lvalue_references =
    input_port_list_is_tuplelike<T> &&
    requires(T x) { contains_lvalue_references_v(x.input_ports); };

/**
 * Member variable `input_ports` is a tuple-like list of lvalue references to
 * input port objects.
 *
 * @tparam T a potential node specification class
 */
template <class T>
concept has_input_port_list =
    input_port_list_contains_lvalue_references<T> && requires(T x) {
      std::apply(
          []<class... U>(U...) {
            return (input_port<std::remove_reference<U>> && ...);
          },
          x.input_ports);
    };

/**
 * Class declares a member variable `output_ports`.
 *
 * @tparam T a potential node specification class
 */
template <class T>
concept declares_output_port_list = requires(T x) {
  { T::output_ports };
};

/**
 * Member variable `output_ports` is tuple-like for `std::apply`.
 *
 * @tparam T a potential node specification class
 */
template <class T>
concept output_port_list_is_tuplelike =
    declares_output_port_list<T> &&
    requires(T x) { applicable<decltype(x.output_ports)>; };

/**
 * Member variable `output_ports` contains only lvalue references.
 *
 * @tparam T a potential node specification class
 */
template <class T>
concept output_port_list_contains_lvalue_references =
    output_port_list_is_tuplelike<T> &&
    requires(T x) { contains_lvalue_references_v(x.output_ports); };

/**
 * Member variable `output_ports` is a tuple-like list of lvalue references to
 * output port objects.
 *
 * @tparam T a potential node specification class
 */
template <class T>
concept has_output_port_list =
    output_port_list_contains_lvalue_references<T> && requires(T x) {
      std::apply(
          []<class... U>(U...) {
            return (output_port<std::remove_reference<U>> && ...);
          },
          x.output_ports);
    };

/**
 * A node static specification is self-declared as such. It declares a node body
 * and annotates its input and output ports.
 *
 * @tparam T a potential node static specification class
 */
template <class T>
concept node =
    self_declared_as_node_static_specification<T> && has_input_port_list<T> &&
    has_output_port_list<T> && has_nody_body<T>;

}  // namespace tiledb::flow_graph::static_specification

namespace tiledb::flow_graph {
/**
 * A node specification to be used as part of a larger graph static
 * specification.
 *
 * @tparam T a potential node static specification class
 */
template <class T>
concept node_static_specification = static_specification::node<T>;

/**
 * Class function on a node static specification for the number of its input
 * ports.
 *
 * @tparam T a potential node static specification class
 */
template <node_static_specification T>
constexpr size_t number_of_input_ports{
    std::tuple_size_v<decltype(T::input_ports)>};

/**
 * Class function on a node static specification for the number of its output
 * ports.
 *
 * @tparam T a potential node static specification class
 */
template <node_static_specification T>
constexpr size_t number_of_output_ports{
    std::tuple_size_v<decltype(T::output_ports)>};

}  // namespace tiledb::flow_graph
#endif  // TILEDB_FLOW_GRAPH_NODE_STATIC_SPECIFICATION_H
