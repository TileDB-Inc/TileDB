/**
 * @file flow_graph/system/graph_dynamic_specification.h
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

#ifndef TILEDB_FLOW_GRAPH_DYNAMIC_GRAPH_SPECIFICATION_H
#define TILEDB_FLOW_GRAPH_DYNAMIC_GRAPH_SPECIFICATION_H

#include <concepts>

#include "graph_type.h"

//-------------------------------------------------------
// Dynamic specification of a graph
//-------------------------------------------------------
/*
 * The dynamic specification is the most general specification. It's named as
 * "dynamic" in contradistinction to static graph specifications.
 *
 * A graph specification contains all the information to build a graph without
 * actually being the graph. A flow graph suitable for execution must have
 * memory allocated to it (however allocated, on the heap or otherwise) for all
 * the graph state, but a graph specification does not require this.
 *
 * The dynamic specification is responsible for type erasure. It contains within
 * it all the types needed to fully instantiate an execution graph, but presents
 * a type-erased interface to them. References to the graph elements (nodes,
 * ports, edges), are all based on integral indices. Given an index, a dynamic
 * specification must be able to construct an appropriate object.
 *
 * The concept of a dynamic specification is the graph as a whole; it is not a
 * concept for graph operations such as adding or removing nodes and edges. It
 * is expected that most dynamic specifications will be generated from static
 * information rather than constructed directly. Classes that satisfy the
 * dynamic specification concept might contain such graph operations. If they
 * do, though, they're entirely internal to those classes and are not used by
 * the system as a whole. This remains true even for so-called "wide" nodes and
 * other kinds of graph topologies that might at some future point be mutable
 * during execution.
 */

namespace tiledb::flow_graph::dynamic_specification {
/**
 * A class declares itself as a graph dynamic specification.
 *
 * @tparam T a potential specification graph class
 */
template <class T>
concept self_declared_as_graph_dynamic_specification =
    requires(T x) { x.invariants.i_am_graph_dynamic_specification; };

/**
 * Concept that a class is valid as graph specification. Graph specifications in
 * general must be dynamic to allow graphs specified with parameters.
 *
 * Maturity Note: At this time there's only one kind of dynamic specification.
 * It's `ToDynamic`, which adapts static specifications into the dynamic ones.
 * As such, there's not yet any pressing need to work out the details of a
 * concept for dynamic specifications.
 *
 * @tparam T a potential dynamic graph specification
 */
template <class T>
concept graph = self_declared_as_graph_dynamic_specification<T>;
}  // namespace tiledb::flow_graph::dynamic_specification

namespace tiledb::flow_graph {
template <class T>
concept graph_dynamic_specification = dynamic_specification::graph<T>;
}
#endif  // TILEDB_FLOW_GRAPH_DYNAMIC_GRAPH_SPECIFICATION_H
