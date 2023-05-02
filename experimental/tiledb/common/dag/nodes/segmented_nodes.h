/**
 * @file   segmented_nodes.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2022 TileDB, Inc.
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
 *
 * This file defines nodes that support segmented execution for the TileDB task
 * graph.
 * *
 * Segmented execution is implemented using a "Duff's device" style loop,
 * allowing the node to yield control back to the scheduler and return execution
 * where it left off.
 *
 * There are four types of segmented nodes:
 *   - Producer, which encapsulates a producer function that produces a single
 * result.
 *   - Consumer, which encapsulates a consumer function that consumes a single
 * result.
 *   - Function, which encapsulates a function that produces and consumes a
 * single result.
 *   - MIMO, which encapsulates a function that produces and consumes
 * results with arbitrary cardinality.
 *
 * The function encapsulated in the producer node may issue a stop request, in
 * which case the producer node will begin shutting down the task graph.
 *
 * Execution of a node is accessed through the `resume` function.
 *
 * To enable the different kinds of nodes to be stored in a singly typed
 * container, we use an abstract base class `node_base` from which all other
 * nodes are derived.
 *
 * Nodes maintain a link to a correspondent node, which links are used for
 * scheduling purposes (sending events).  The links are maintained on the nodes
 * rather than on tasks, because the nodes are the objects that are actually
 * created (by the user) and stored in the task graph when the task graph is
 * created.  This connectivity is redundant with the connectivity between ports.
 * @todo Consider removing the connectivity between nodes and instead using the
 * connectivity between ports.
 *
 * The following can be a useful debug string:
 *   `this->name() + " " + std::to_string(this->id())`
 */

#ifndef TILEDB_DAG_NODES_SEGMENTED_NODES_H
#define TILEDB_DAG_NODES_SEGMENTED_NODES_H

#include "detail/segmented/consumer.h"
#include "detail/segmented/function.h"
#include "detail/segmented/mimo.h"
#include "detail/segmented/producer.h"
#include "node_traits.h"

namespace tiledb::common {

// using node = std::shared_ptr<node_base>;
// using node_handle = node_handle_t<node_base>;

using node = node_handle_t<node_base>;

template <class T>
struct correspondent_traits {};

}  // namespace tiledb::common

#endif  // TILEDB_DAG_NODES_SEGMENTED_NODES_H
