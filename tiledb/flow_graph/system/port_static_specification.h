/**
 * @file flow_graph/system/port_static_specification.h
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

#ifndef TILEDB_FLOW_GRAPH_PORT_STATIC_SPECIFICATION_H
#define TILEDB_FLOW_GRAPH_PORT_STATIC_SPECIFICATION_H

/*
 * TODO: split into static and dynamic headers
 */

namespace tiledb::flow_graph::static_specification {
/**
 * The class declares itself as an output port static specification.
 *
 * @tparam T a potential output port static specification
 */
template <class T>
concept self_declared_as_output_port =
    requires(T x) { x.invariants.i_am_output_port_static_specification; };

/**
 * The class declares itself as an input port static specification.
 *
 * @tparam T a potential input port static specification
 */
template <class T>
concept self_declared_as_input_port =
    requires(T x) { x.invariants.i_am_input_port_static_specification; };

/**
 * The class declares a flow type.
 *
 * @tparam T a potential port static specification
 */
template <class T>
concept port_has_typed_flow = requires(T) { typename T::flow_type; };

/**
 * An output port is self-declared and has a flow type.
 *
 * @tparam T a potential output port static specification
 */
template <class T>
concept output_port = self_declared_as_output_port<T> && port_has_typed_flow<T>;

/**
 * An input port is self-declared and has a flow type.
 *
 * @tparam T a potential input port static specification
 */
template <class T>
concept input_port = self_declared_as_input_port<T> && port_has_typed_flow<T>;

}  // namespace tiledb::flow_graph::static_specification

namespace tiledb::flow_graph {

/**
 * An output port static specification is part of a node static specification.
 * Its primary purpose is to specify a flow type that matches an edge.
 *
 * @tparam T a potential output port static specification
 */
template <class T>
concept output_port_static_specification = static_specification::output_port<T>;

/**
 * An input port static specification is part of a node static specification.
 * Its primary purpose is to specify a flow type that matches an edge.
 *
 * @tparam T a potential input port static specification
 */
template <class T>
concept input_port_static_specification = static_specification::input_port<T>;

/**
 * Extended sizeof. Operates on types. Extension returns 0 for void instead of
 * a syntax error.
 *
 * @tparam T
 */
template <class T>
constexpr size_t sizeof_type{sizeof(T)};
/**
 * Specialization that yields zero for void.
 */
template <>
constexpr size_t sizeof_type<void>{0};

/**
 * The size of the flow type of `T`.
 *
 * @tparam T either an input or an output port specification
 */
template <static_specification::port_has_typed_flow T>
constexpr size_t flow_size_type{sizeof_type<typename T::flow_type>};

/**
 * The size of the flow type of an object of type `T`.
 *
 * @tparam T either an input or an output port specification
 */
template <class T>
constexpr size_t flow_size(const T&) {
  return flow_size_type<T>;
};

}  // namespace tiledb::flow_graph

#endif  // TILEDB_FLOW_GRAPH_PORT_STATIC_SPECIFICATION_H
