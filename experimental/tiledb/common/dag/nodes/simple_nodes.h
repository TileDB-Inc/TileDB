/**
 * @file   experimental/tiledb/common/dag/nodes/simple_nodes.h
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
 * This file declares "simple" nodes for dag task graph library. Simple nodes
 * are nodes whose enclosed functions are assumed to have no state and whose
 * enclosed function takes one input and produces one output (though the input
 * and output could be tuples).  Furthermore, the enclosed function produces one
 * output for every input. Simple nodes have no capability of maintaining,
 * saving, nor restoring state for the enclosed functions.
 */

#ifndef TILEDB_DAG_SIMPLE_NODES_H
#define TILEDB_DAG_SIMPLE_NODES_H

#include "experimental/tiledb/common/dag/nodes/detail/simple/consumer.h"
#include "experimental/tiledb/common/dag/nodes/detail/simple/function.h"
#include "experimental/tiledb/common/dag/nodes/detail/simple/mimo.h"
#include "experimental/tiledb/common/dag/nodes/detail/simple/producer.h"

#endif  // TILEDB_DAG_SIMPLE_NODES_H
