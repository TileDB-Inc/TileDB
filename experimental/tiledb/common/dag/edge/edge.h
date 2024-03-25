/**
 * @file   edge.h
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
 * This file declares the edge class for dag.
 *
 * @todo Use CTAD to get template arguments automatically for `Edge`.
 */

#ifndef TILEDB_DAG_EDGE_H
#define TILEDB_DAG_EDGE_H

#include <array>
#include <cassert>
#include <iostream>
#include <type_traits>
#include "experimental/tiledb/common/dag/ports/ports.h"

namespace tiledb::common {

/**
 * Trivial base class to enable storage of `Edge` objects of different types in
 * a task graph.
 */
struct GraphEdge {};

/**
 * An edge in a task graph.
 *
 * Creating an edge sets up an item mover between the `Source` and the `Sink`.
 * The `Edge` may go out of scope when this is done.  The item mover will still
 * be pointed to by `Source` and the `Sink`.
 *
 * @todo Since the `Edge` doesn't really maintain any information related to
 * `Source` and `Sink` it probably doesn't need those as template parameters,
 * but rather we could make the constructor a function template.
 */

template <template <class> class Mover_T, class Block>
class Edge;

/**
 * Deduction guides for `Edge`.
 */
template <template <class> class Mover_T, class Block>
Edge(Source<Mover_T, Block>&, Sink<Mover_T, Block>&) -> Edge<Mover_T, Block>;

template <template <class> class Mover_T, class Block>
Edge(std::shared_ptr<Source<Mover_T, Block>>&, Sink<Mover_T, Block>&)
    -> Edge<Mover_T, Block>;

template <template <class> class Mover_T, class Block>
Edge(Source<Mover_T, Block>&, std::shared_ptr<Sink<Mover_T, Block>>&)
    -> Edge<Mover_T, Block>;

template <template <class> class Mover_T, class Block>
Edge(
    std::shared_ptr<Source<Mover_T, Block>>&,
    std::shared_ptr<Sink<Mover_T, Block>>&) -> Edge<Mover_T, Block>;

/**
 * An edge in a task graph.
 *
 * Creating an edge sets up an item mover between the `Source` and the `Sink`.
 * The `Edge` may go out of scope when this is done.  The item mover will still
 * be pointed to by `Source` and the `Sink`.
 *
 * @todo Since the `Edge` doesn't really maintain any information related to
 * `Source` and `Sink` it probably doesn't need those as template parameters,
 * but rather we could make the constructor a function template.
 */
template <template <class> class Mover_T, class Block>
class Edge : public GraphEdge {
  using source_type = Source<Mover_T, Block>;
  using sink_type = Sink<Mover_T, Block>;

  using mover_type = Mover_T<Block>;

  /**
   * Indicates whether there is a buffer item in the item mover, or if the
   * `Source` and `Sink` can be directly connected.
   */
  constexpr static bool edgeful = mover_type::edgeful;
  std::shared_ptr<mover_type> item_mover_;

 public:
  /**
   * Constructor.
   */
  Edge(source_type& from, sink_type& to) {
    item_mover_ = std::make_shared<mover_type>();
    attach(from, to, item_mover_);
  }
};  // namespace tiledb::common

}  // namespace tiledb::common

#endif  // TILEDB_DAG_EDGE_H
