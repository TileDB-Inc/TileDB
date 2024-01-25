/**
 * @file  experimental/tiledb/common/dag/nodes/node_traits.h
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

#ifndef TILEDB_DAG_NODES_NODE_TRAITS_H
#define TILEDB_DAG_NODES_NODE_TRAITS_H

#include <memory>

namespace tiledb::common {

template <class N>
struct node_traits;

template <class N>
struct node_traits {
  using node_type = typename N::node_type;
  using node_handle_type = typename N::node_handle_type;
};

template <class N>
struct node_traits<std::shared_ptr<N>> {
  using node_type = typename node_traits<N>::node_type;
  using node_handle_type = typename node_traits<N>::node_handle_type;
};

template <class N>
struct node_traits<N*> {
  using node_type = N;
  using node_handle_type = N*;
};

template <class N>
using node_t = typename node_traits<N>::node_type;

template <class N>
using node_handle_t = typename node_traits<N>::node_handle_type;

}  // namespace tiledb::common

#endif  // TILEDB_DAG_NODES_NODE_TRAITS_H
