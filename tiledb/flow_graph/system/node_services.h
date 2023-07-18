/**
 * @file flow_graph/system/node_services.h
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
 * @section DESCRIPTION Node services concept
 */

#ifndef TILEDB_FLOW_GRAPH_NODE_SERVICES_H
#define TILEDB_FLOW_GRAPH_NODE_SERVICES_H

namespace tiledb::flow_graph::execution {

/**
 * Node services are self-declared as such.
 *
 * @tparam T a potential node services class
 */
template <class T>
concept self_declared_as_node_services =
    requires(T x) { x.invariants.i_am_node_services; };

/**
 * Node services are self-declared.
 *
 * Maturity Note: This concept is not yet implemented. It needs an I/O
 * implementation.
 *
 * @tparam T a potential node services class
 */
template <class T>
concept node_services_impl = self_declared_as_node_services<T>;

//-------------------------------------------------------
// MinimalNodeServices
//-------------------------------------------------------
/**
 * A minimal node services class, acting as a stub to instantiate node body
 * templates so the resulting class can have concepts evaluated against it.
 * This technique substitutes for some way of evaluating universal quantifiers
 * on concepts. In this case we want a quantifier "for all node services class
 * arguments NS ...".
 */
class MinimalNodeServices {
 public:
  static constexpr struct invariant_type {
    static constexpr bool i_am_node_services{true};
  } invariants;
};
static_assert(node_services_impl<MinimalNodeServices>);

}  // namespace tiledb::flow_graph::execution

namespace tiledb::flow_graph {

/**
 * Node services are the means by which a node body accesses services from an
 * execution platform.
 *
 * The most basic service is I/O along edges, the service that puts the "flow"
 * in "flow graph".
 *
 * @tparam T a potential node services class
 */
template <class T>
concept node_services = execution::node_services_impl<T>;

}  // namespace tiledb::flow_graph

#endif  // TILEDB_FLOW_GRAPH_NODE_SERVICES_H
