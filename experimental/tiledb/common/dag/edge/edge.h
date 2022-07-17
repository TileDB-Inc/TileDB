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
 */

#ifndef TILEDB_DAG_EDGE_H
#define TILEDB_DAG_EDGE_H

#include "experimental/tiledb/common/dag/ports/ports.h"

namespace tiledb::common {

/**
 * An edge in a task graph.
 *
 * Contains a queue of blocks of size 3, that is, at any time it has between 0
 * and 3 blocks in it.  Three blocks in the queue allows one to be written to on
 * one side of the edge, read from on the other side of the edge, with one ready
 * to be read.
 *
 * Edges implement a demand-pull pattern for synchronization.
 */
template <class Block, class StateMachine>
class Edge : public Source<Block, StateMachine>,
             public Sink<Block, StateMachine> {
  using SourceBase = Source<Block, StateMachine>;
  using SinkBase = Source<Block, StateMachine>;

  std::optional<Block> item_{};

 public:
  Edge(Source<Block, StateMachine>& from, Sink<Block, StateMachine>& to) {
    attach(*this, to);
    attach(from, *this);
  }
};

}  // namespace tiledb::common

#endif  // TILEDB_DAG_EDGE_H
