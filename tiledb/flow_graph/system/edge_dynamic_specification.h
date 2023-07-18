/**
 * @file flow_graph/system/edge_dynamic_specification.h
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

#ifndef TILEDB_FLOW_GRAPH_EDGE_DYNAMIC_SPECIFICATION_H
#define TILEDB_FLOW_GRAPH_EDGE_DYNAMIC_SPECIFICATION_H

#include "graph_type.h"
#include "tiledb/flow_graph/system/node_dynamic_specification.h"
#include "tiledb/flow_graph/system/port_dynamic_specification.h"

namespace tiledb::flow_graph::dynamic_specification {
/**
 * The class declares itself as an edge static specification.
 *
 * @tparam T a potential edge static specification class
 */
template <class T>
concept self_declared_as_edge_dynamic_specification =
    requires(T x) { x.invariants.i_am_edge_dynamic_specification; };


/**
 * Stub for a future edge dynamic specification
 *
 * @tparam T a potential edge dynamic specification class
 */
template <class T>
concept edge = self_declared_as_edge_dynamic_specification<T>;
}  // namespace tiledb::flow_graph::dynamic_specification

namespace tiledb::flow_graph {

/**
 * An edge static specification.
 *
 * @tparam T a potential edge dynamic specification class
 */
template <class T>
concept edge_dynamic_specification = dynamic_specification::edge<T>;

}  // namespace tiledb::flow_graph
#endif  // TILEDB_FLOW_GRAPH_EDGE_DYNAMIC_SPECIFICATION_H
