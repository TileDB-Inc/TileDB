/**
* @file   experimental/tiledb/common/dag/nodes/detail/segmented/edge_node_ctad.h
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
*/


#ifndef TILEDB_DAG_NODES_EDGE_NODE_CTAD_H
#define TILEDB_DAG_NODES_EDGE_NODE_CTAD_H

#include "experimental/tiledb/common/dag/nodes/segmented_nodes.h"
#include "experimental/tiledb/common/dag/edge/edge.h"

namespace tiledb::common {

template <template <class> class Mover, class T>
Edge(producer_node<Mover, T>, consumer_node<Mover, T>) -> Edge<Mover, T>;

template <template <class> class Mover, class T>
Edge(producer_node<Mover, T>&, Sink<Mover, T>) -> Edge<Mover, T>;

template <template <class> class Mover, class T>
Edge(std::shared_ptr<producer_node_impl<Mover, T>>&, Sink<Mover, T>) -> Edge<Mover, T>;

template <template <class> class Mover, class T>
Edge(Source<Mover, T>, consumer_node<Mover, T>&) -> Edge<Mover, T>;

template <template <class> class Mover, class T>
Edge(Source<Mover, T>, std::shared_ptr<consumer_node_impl<Mover, T>>&) -> Edge<Mover, T>;

template <template <class> class Mover, class T>
Edge(std::shared_ptr<producer_node_impl<Mover, T>>&, std::shared_ptr<consumer_node_impl<Mover, T>>&) -> Edge<Mover, T>;

template <template <class> class Mover, class T>
Edge(std::shared_ptr<producer_node_impl<Mover, T>>&, std::shared_ptr<function_node_impl<Mover, T>>&) -> Edge<Mover, T>;

template <template <class> class Mover, class T>
Edge(std::shared_ptr<function_node_impl<Mover, T>>&, std::shared_ptr<consumer_node_impl<Mover, T>>&) -> Edge<Mover, T>;

template <template <class> class Mover, class T>
Edge(std::shared_ptr<function_node_impl<Mover, T>>&, std::shared_ptr<function_node_impl<Mover, T>>&) -> Edge<Mover, T>;

template <template <class> class SinkMover, class T, template <class> class SourceMover, class U>
Edge(std::shared_ptr<mimo_node_impl<SinkMover, T, SourceMover, T>>&,
     std::shared_ptr<mimo_node_impl<SourceMover, T, SinkMover, U>>&) -> Edge<SourceMover, T>;

template <template <class> class SinkMover, class T, template <class> class SourceMover, class U>
Edge(std::shared_ptr<producer_node_impl<SourceMover, T>>&,
     std::shared_ptr<mimo_node_impl<SourceMover, T, SinkMover, U>>&) -> Edge<SourceMover, T>;

}

#endif  // TILEDB_DAG_NODES_EDGE_NODE_CTAD_H
