/**
 * @file   edge_node_ctad.h
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

#ifndef TILEDB_DAG_NODES_EDGE_NODE_CTAD_H
#define TILEDB_DAG_NODES_EDGE_NODE_CTAD_H

#include "experimental/tiledb/common/dag/edge/edge.h"
#include "experimental/tiledb/common/dag/nodes/segmented_nodes.h"

namespace tiledb::common {

#if 0
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

template <template <class> class Mover, class T, template <class> class Mover2, class T2>
Edge(std::shared_ptr<producer_node_impl<Mover, T>>&, std::shared_ptr<function_node_impl<Mover, T, Mover2, T2>>&) -> Edge<Mover, T>;

template <template <class> class Mover, class T, template <class> class Mover2, class T2>
Edge(std::shared_ptr<function_node_impl<Mover, T, Mover2, T2>>&, std::shared_ptr<consumer_node_impl<Mover2, T2>>&) -> Edge<Mover2, T2>;

template <template <class> class Mover, class T, template <class> class Mover2, class T2, template <class> class Mover3, class T3>
Edge(std::shared_ptr<function_node_impl<Mover, T, Mover2, T2>>&, std::shared_ptr<function_node_impl<Mover2, T2, Mover3, T3>>&) -> Edge<Mover2, T2>;

template <template <class> class SinkMover, class T, template <class> class SourceMover, class U>
Edge(std::shared_ptr<mimo_node_impl<SinkMover, T, SourceMover, T>>&,
     std::shared_ptr<mimo_node_impl<SourceMover, T, SinkMover, U>>&) -> Edge<SourceMover, T>;

template <template <class> class SinkMover, class T, template <class> class SourceMover, class U>
Edge(std::shared_ptr<producer_node_impl<SourceMover, T>>&,
     std::shared_ptr<mimo_node_impl<SourceMover, T, SinkMover, U>>&) -> Edge<SourceMover, T>;
#endif

#if 1
// @todo there are probably better ways to do this,
//   but it seems to work without having combinatorial explosion of ctad
// @todo repeat for shared_ptr of each of these?  Doesn't seem to be necessary
// @todo tighten this up once we have concept support
template <
    template <template <class> class, class>
    class P,
    template <template <class> class, class>
    class C,
    template <class>
    class Mover,
    class T>
Edge(P<Mover, T>, C<Mover, T>) -> Edge<Mover, T>;

template <
    template <template <class> class, class>
    class P,
    template <template <class> class, class, template <class> class, class>
    class F,
    template <class>
    class SinkMover,
    class T,
    template <class>
    class SourceMover,
    class U>
Edge(P<SinkMover, T>, F<SinkMover, T, SourceMover, U>) -> Edge<SinkMover, T>;

template <
    template <template <class> class, class>
    class C,
    template <template <class> class, class, template <class> class, class>
    class F,
    template <class>
    class SinkMover,
    class T,
    template <class>
    class SourceMover,
    class U>
Edge(F<SinkMover, T, SourceMover, U>, C<SourceMover, U>)
    -> Edge<SourceMover, U>;

// @todo If we ever want to base this on value_types...
// Edge(F<SinkMover, T, SourceMover, U>, C<SourceMover, typename F<SinkMover, T,
// SourceMover, U>::out_value_type>) -> Edge<SourceMover, typename F<SinkMover,
// T, SourceMover, U>::out_value_type>;

template <
    template <template <class> class, class, template <class> class, class>
    class F,
    template <template <class> class, class, template <class> class, class>
    class G,
    template <class>
    class SinkMover,
    class T,
    template <class>
    class MidMover,
    class U,
    template <class>
    class SourceMover,
    class W>
Edge(F<SinkMover, T, MidMover, U>, G<MidMover, U, SourceMover, W>)
    -> Edge<MidMover, U>;
#else
// @todo The baleful effects of template template parameter for Mover -- can't
// use this much simpler version
template <
    class N,
    class M,
    std::enable_if_t<
        std::is_same_v<typename N::value_type, typename M::value_type>,
        bool> = true>
Edge(N, M) -> Edge<typename N::value_type>;

#endif
}  // namespace tiledb::common

#endif  // TILEDB_DAG_NODES_EDGE_NODE_CTAD_H
