/**
 * @file flow_graph/node_body.h
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

#ifndef TILEDB_FLOW_GRAPH_SYSTEM_NODE_BODY_H
#define TILEDB_FLOW_GRAPH_SYSTEM_NODE_BODY_H

#include <type_traits>

#include "discrete_coroutine.h"
#include "node_services.h"

namespace tiledb::flow_graph::execution {

// Anonymous namespace hides the alias to the minimal node services class
namespace {
using MNS = MinimalNodeServices;
}  // namespace

//-------------------------------------------------------
// Node Body
//-------------------------------------------------------
/**
 * Node bodies must declare themselves as such. By design we do not admit
 * outside traits classes that might declare node properties. Self-declaration
 * is the first and simplest requirement.
 *
 * @tparam N a potential node body class
 */
template <template <class> class T>
concept self_declared_as_node_body =
    requires(T<MNS> x) { x.invariants.i_am_node_body; };

/**
 * A node body has a discrete coroutine when instantiated with node services.
 *
 * @tparam T a potential node body class
 */
template <template <class> class T>
concept node_body_base =
    self_declared_as_node_body<T> && discrete_coroutine<T<MNS>>;

/**
 * A node body with a virtual destructor.
 *
 * @tparam T a potential node body class
 */
template <template <class> class T>
concept node_body_with_virtual_destructor =
    node_body_base<T> && std::has_virtual_destructor_v<T<MNS>>;

/**
 * A node body with a trivial destructor
 *
 * @tparam T
 */
template <template <class> class T>
concept node_body_trivially_destructible =
    node_body_base<T> && std::is_trivially_destructible_v<T<MNS>>;

/**
 * A node body is a discrete coroutine with destruction that can be type-erased.
 *
 * @tparam T
 */
template <template <class> class T>
concept node_body_impl =
    node_body_trivially_destructible<T> || node_body_with_virtual_destructor<T>;

}  // namespace tiledb::flow_graph::execution

namespace tiledb::flow_graph {

/**
 * A node body is essential part of an execution node. It's supported from below
 * by node services class and from about by node class, both provided by an
 * execution platform.
 *
 * @tparam T a potential node body class
 */
template <template <class> class T>
concept node_body = execution::node_body_impl<T>;

}  // namespace tiledb::flow_graph

#endif  // TILEDB_FLOW_GRAPH_NODE_BODY_H
