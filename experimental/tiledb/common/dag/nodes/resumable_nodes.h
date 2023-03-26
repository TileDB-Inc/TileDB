/**
* @file   resumable_nodes.h
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
* Resumable nodes
*/

#ifndef TILEDB_DAG_NODES_RESUMABLE_NODES_H
#define TILEDB_DAG_NODES_RESUMABLE_NODES_H

#include "detail/resumable/proto_mimo.h"
// #include "detail/resumable/consumer.h"
// #include "detail/resumable/function.h"
// #include "detail/resumable/mimo.h"
// #include "detail/resumable/producer.h"
#include "node_traits.h"

namespace tiledb::common {

// using node = std::shared_ptr<node_base>;
// using node_handle = node_handle_t<node_base>;

using node = node_handle_t<node_base>;

template <class T>
struct correspondent_traits {};

}  // namespace tiledb::common

#endif  // TILEDB_DAG_NODES_RESUMABLE_NODES_H