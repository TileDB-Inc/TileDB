/**
 * @file   experimental/tiledb/common/dag/nodes/detail/resumable/reduce.h
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
 */

#ifndef TILEDB_DAG_NODES_DETAIL_RESUMABLE_REDUCE_H
#define TILEDB_DAG_NODES_DETAIL_RESUMABLE_REDUCE_H

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

/**
 * Forward Declaration
 *
 * @tparam SinkMover
 * @tparam BlocksIn
 * @tparam SourceMover
 * @tparam BlocksOut
 */
template <
    template <class>
    class SinkMover,
    class BlocksIn,
    template <class> class SourceMover = SinkMover,
    class BlocksOut = BlocksIn>
class reducer_node_impl;


/**
 * @brief A reducer node takes in a tuple of blocks and outputs a tuple with a single value.
 * It is a special case of a mimo node where the number of outputs is 1, which is required
 * by the constructor.
 *
 * @tparam SinkMover
 * @tparam BlocksIn
 * @tparam SourceMover
 * @tparam BlocksOut
 */
template <
    template <class>
    class SinkMover,
    class... BlocksIn,
    template <class>
    class SourceMover,
    class... BlocksOut>
class reducer_node_impl<
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

  /**
   * @brief Constructor to enforce reducer having one or zero outputs.
   *
   * @param f
   */
  explicit reducer_node_impl(
    std::function<std::tuple<BlocksOut...>(std::tuple<BlocksIn...>)> f,
    std::enable_if_t<sizeof...(BlocksOut) == 1 || sizeof...(BlocksOut) == 0
        , void**> = nullptr)
    : Base{std::move(f)} {
  }
};

template <
    template <class>
    class SinkMover,
    class BlocksIn,
    template <class> class SourceMover = SinkMover,
    class BlocksOut = BlocksIn>
class reducer_node
    : public std::shared_ptr<
          mimo_node_impl<SinkMover, BlocksIn, SourceMover, BlocksOut>> {
  using PreBase =
      mimo_node_impl<SinkMover, BlocksIn, SourceMover, BlocksOut>;
  using Base = std::shared_ptr<PreBase>;
  using Base::Base;

 public:
  template <class Function>
  explicit reducer_node(Function&& f)
      : Base{std::make_shared<PreBase>(std::forward<Function>(f))} {
  }
};

}  // namespace tiledb::common
#endif  //  TILEDB_DAG_NODES_DETAIL_RESUMABLE_REDUCE_H
