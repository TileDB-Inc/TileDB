/**
 * @file flow_graph/system/node_dynamic_specification.h
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

#ifndef TILEDB_FLOW_GRAPH_NODE_DYNAMIC_SPECIFICATION_H
#define TILEDB_FLOW_GRAPH_NODE_DYNAMIC_SPECIFICATION_H

#include "tiledb/flow_graph/system/port_dynamic_specification.h"

namespace tiledb::flow_graph::dynamic_specification {
/**
 * Specification nodes must declare themselves as such.
 *
 * @tparam T a potential node specification class
 */
template <class T>
concept self_declared_as_node_dynamic_specification =
    requires(T x) { x.invariants.i_am_node_dynamic_specification; };

/**
 * A dynamic node specification has a type-erased factory for its node body.
 *
 * @tparam T a potential node dynamic specification class
 */
template <class T>
concept declares_node_size = requires(T x) { x.size_of_node(); };

/**
 * Class has a factory function with placement signature.
 *
 * @tparam T a potential node dynamic specification class
 */
template <class T>
concept has_factory_function = requires { [](T xx, void* p) { xx.make(p); }; };

/**
 * A type-erased factory requires both a size function to determine allocation
 * size and a factory function with a placement argument to accept the address
 * of the allocation.
 *
 * @tparam T a potential node dynamic specification class
 */
template <class T>
concept has_node_body_factory =
    declares_node_size<T> && has_factory_function<T>;

/**
 * A node dynamic specification has a factory for its node body.
 *
 * At present there's no requirement for self-declaration. The only dynamic
 * specification is the class transformer `ToDynamic`. Should there be many
 * other dynamic specifications, it might be prudent to add an analogous
 * requirement.
 *
 * @tparam T a potential node dynamic specification class
 */
template <class T>
concept node = has_node_body_factory<T>;
}  // namespace tiledb::flow_graph::dynamic_specification

namespace tiledb::flow_graph {
/**
 * A node specification to be used as part of a larger dynamic graph
 * specification.
 *
 * @tparam T a potential node dynamic specification class
 */
template <class T>
concept node_dynamic_specification = dynamic_specification::node<T>;

}  // namespace tiledb::flow_graph
#endif  // TILEDB_FLOW_GRAPH_NODE_DYNAMIC_SPECIFICATION_H
