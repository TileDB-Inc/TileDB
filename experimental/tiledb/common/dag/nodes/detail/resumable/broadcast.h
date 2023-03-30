/**
* @file   experimental/tiledb/common/dag/nodes/detail/resumable/mimo.h
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
*
* This file declares a multi-input multi-outpu function node class for the
* TileDB task graph library.  A mimo node is a function node that takes
* multiple inputs and multiple outputs.
*
* The general node can also be specialized to provide equivalent functionality
* to a producer or consumer node, respectively, by passing in std::tuple<> for
* the input block type or for the output block type. In either case, a dummy
* argument needs to be used for the associated mover.
*
* @todo Specialize for non-tuple block types.
*
* @todo Specialize for void input/output types (rather than tuple<>) as well as
* for void mover types.
 */

#ifndef TILEDB_DAG_NODES_DETAIL_BROADCAST_MIMO_H
#define TILEDB_DAG_NODES_DETAIL_BROADCAST_MIMO_H


#include <functional>
#include <tuple>
#include <type_traits>
#include <utility>

#include "experimental/tiledb/common/dag/execution/jthread/stop_token.hpp"
#include "experimental/tiledb/common/dag/nodes/detail/resumable/mimo_base.h"
#include "experimental/tiledb/common/dag/nodes/detail/resumable/utils.h"
#include "experimental/tiledb/common/dag/nodes/resumable_nodes.h"
#include "experimental/tiledb/common/dag/ports/ports.h"

#include "experimental/tiledb/common/dag/utility/print_types.h"

namespace tiledb::common {

template <size_t N, class In, class Out>
struct fanout;

template <size_t N, class... In, class... Out>
struct fanout<N, std::tuple<In...>, std::tuple<Out...>> {

  std::function<std::tuple<Out...>(const std::tuple<In...>&)> f_;

  template <class Function>
  // requires (sizeof...(Out) == 1 && sizeof...(In) == 1)
  fanout(Function&& f) : f_{std::forward<Function>(f)}{}

  auto operator()(const std::tuple<In...>& in) {
    auto tmp = f_(in);
    return fill_tuple<N>(std::get<0>(tmp));
  }
};

/**
 * Forward Declaration
 *
 * @tparam SinkMover
 * @tparam BlocksIn
 * @tparam SourceMover
 * @tparam BlocksOut
 */
template <
    size_t N,
    template <class>
    class SinkMover,
    class BlocksIn,
    template <class> class SourceMover = SinkMover,
    class BlocksOut = BlocksIn>
class broadcast_node_impl;


/**
 * @brief A broadcast node takes in a tuple of blocks and outputs a tuple with a single value.
 * It is a special case of a mimo node where the number of outputs is 1, which is required
 * by the constructor.
 *
 * @tparam SinkMover
 * @tparam BlocksIn
 * @tparam SourceMover
 * @tparam BlocksOut
 */
template <
    size_t N,
    template <class>
    class SinkMover,
    class... BlocksIn,
    template <class>
    class SourceMover,
    class... BlocksOut>
class broadcast_node_impl<
    N,
    SinkMover,
    std::tuple<BlocksIn...>,
    SourceMover,
    std::tuple<BlocksOut...>>
    : public mimo_node_impl<
          SinkMover,
          std::tuple<BlocksIn...>,
          SourceMover,
          std::tuple<BlocksOut...>> {
  using Base = mimo_node_impl<
      SinkMover,
      std::tuple<BlocksIn...>,
      SourceMover,
      std::tuple<BlocksOut...>>;

 public:
  explicit broadcast_node_impl(
      std::function<std::tuple<BlocksOut...>(std::tuple<BlocksIn...>)> f,
      std::enable_if_t<sizeof...(BlocksOut) == N, void**> = nullptr)
      : Base{std::move(f)} {
  }
};

template <
    size_t N,
    template <class>
    class SinkMover,
    class BlocksIn,
    template <class> class SourceMover = SinkMover,
    class BlocksOut = BlocksIn>
class broadcast_node
    : public std::shared_ptr<
          broadcast_node_impl<N, SinkMover, BlocksIn,
                              SourceMover, decltype(fill_tuple<N>(std::declval<std::tuple_element_t<0, BlocksOut>>()))>> {
  using fanned_out = decltype(fill_tuple<N>(std::declval<std::tuple_element_t<0, BlocksOut>>()));
  using PreBase =
      broadcast_node_impl<N, SinkMover, BlocksIn, SourceMover, fanned_out>;
  using Base = std::shared_ptr<PreBase>;
  using Base::Base;

 public:
  template <class Function>
  explicit broadcast_node(Function&& f)
      : Base{std::make_shared<PreBase>(fanout<N, BlocksIn, BlocksOut>(f))} {
  }
};

// https://godbolt.org/z/3sdso6rTo

}  // namespace tiledb::common
#endif  // TILEDB_DAG_NODES_DETAIL_BROADCAST_MIMO_H
