/**
* @file   experimental/tiledb/common/dag/nodes/detail/resumable/mimo_base.h
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

#ifndef TILEDB_DAG_NODES_DETAIL_SEGMENTED_MIMO_BASE_H
#define TILEDB_DAG_NODES_DETAIL_SEGMENTED_MIMO_BASE_H

#include <functional>
#include <tuple>
#include <type_traits>
#include <utility>

#include "experimental/tiledb/common/dag/ports/ports.h"

#include "experimental/tiledb/common/dag/execution/jthread/stop_token.hpp"

//#include
//"experimental/tiledb/common/dag/nodes/detail/resumable/resumable_base.h"
//#include
//"experimental/tiledb/common/dag/nodes/detail/resumable/resumable_fwd.h"
#include "experimental/tiledb/common/dag/nodes/detail/resumable/utils.h"
#include "experimental/tiledb/common/dag/nodes/resumable_nodes.h"

#include "experimental/tiledb/common/dag/utility/print_types.h"

namespace tiledb::common {

template <
    template <class>
    class SinkMover,
    class BlocksIn,
    template <class> class SourceMover = SinkMover,
    class BlocksOut = BlocksIn>
class mimo_node_impl_base;

/**
* @todo Partial specialization for non-tuple types?
*
* @todo CTAD deduction guides to simplify construction (and eliminate
* redundancy, site of errors, due to template args of class and function in
* constructor).
*
 */

template <
    template <class>
    class SinkMover,
    class... BlocksIn,
    template <class>
    class SourceMover,
    class... BlocksOut>
class mimo_node_impl_base<
    SinkMover,
    std::tuple<BlocksIn...>,
    SourceMover,
    std::tuple<BlocksOut...>> : public resumable_node_base {
 protected:
  using node_base = resumable_node_base;
  // Alternatively... ?
  // std::function<std::tuple<BlocksOut...>(std::tuple<BlocksIn...>)> f_;
  //  std::function<void(const std::tuple<BlocksIn...>&,
  //  std::tuple<BlocksOut...>&)> f_;
  typename fn_type<std::tuple<BlocksIn...>, std::tuple<BlocksOut...>>::type f_;

  using node_base_type = node_base;

  //  static_assert(std::is_same_v<
  //                typename sink_mover_type::scheduler_event_type,
  //                typename source_mover_type::scheduler_event_type>);

 public:
  /*
  * Make these public for now for testing
  *
  * @todo Develop better interface for `Edge` connections.
  *
  *  @todo It would be nice to use std::array here, but it doesn't
  *  support different types for each element, which we need since
  *  we aren't doing type erasure.
   */
  std::tuple<Sink<SinkMover, BlocksIn>...> inputs_;
  std::tuple<Source<SourceMover, BlocksOut>...> outputs_;

  constexpr const auto& operator[](size_t index) {
    return std::get<index>(inputs_);
  }

  auto& get_input_ports() {
    return inputs_;
  }
  auto& get_output_ports() {
    return outputs_;
  }

  static constexpr size_t num_inputs() {
    return std::tuple_size_v<decltype(inputs_)>;
  }
  static constexpr size_t num_outputs() {
    return std::tuple_size_v<decltype(outputs_)>;
  }

 protected:
  /****************************************************************
  *                                                              *
  *                    Helper functions                          *
  *                                                              *
  ****************************************************************/

  std::tuple<BlocksIn...> input_items_;
  std::tuple<BlocksOut...> output_items_;

  /**
  * Helper function to deal with tuples.
  * Applies the same single input single
  * output function to elements of an input tuple to set values of an output
  * tuple.
  *
  * @note Elements are processed in order from 0 to sizeof(Ts)-1
  *
  * @pre sizeof(Ts) == sizeof(Us)
   */
  static constexpr const bool is_producer_{
      std::is_same_v<decltype(input_items_), std::tuple<>>};
  static constexpr const bool is_consumer_{
      std::is_same_v<decltype(output_items_), std::tuple<>>};

  template <scheduler_event_type event>
  auto static either(scheduler_event_type a, scheduler_event_type b) {
    if (a == event || b == event) {
      return event;
    }
    return scheduler_event_type::noop;
  }

  /**
  * Helper function to deal with tuples.  A tuple version of simple nodes
  * extract.  Copies items from inputs_ (Sinks) to a tuple of input_items.
  *
  * @note Elements are processed in order from 0 to sizeof(Ts)-1
  *
  * @pre sizeof(Ts) == sizeof(Us)
  *
  * @todo Use std::conjunction?
  *
   */
  template <size_t I = 0, class... Ts, class... Us>
  constexpr void extract_all(std::tuple<Ts...>& in, std::tuple<Us...>& out) {
    static_assert(sizeof...(Ts) == sizeof...(Us));
    if constexpr (is_producer_) {
      return;
    }
    if constexpr (I == sizeof...(Ts)) {
      return;
    } else {
      std::get<I>(out) = *(std::get<I>(in).extract());
      extract_all<I + 1>(in, out);
    }
  }

  /**
  * Helper function to deal with tuples.  A tuple version of simple node
  * inject. Copies items from tuple of output_items to outputs_ (Sources).
  *
  * @note Elements are processed in order from 0 to sizeof(Ts)-1
  *
  * @pre sizeof(Ts) == sizeof(Us)
   */
  template <size_t I = 0, class... Ts, class... Us>
  constexpr void inject_all(std::tuple<Ts...>& in, std::tuple<Us...>& out) {
    static_assert(sizeof...(Ts) == sizeof...(Us));
    if constexpr (is_consumer_) {
      return;
    }
    if constexpr (I == sizeof...(Ts)) {
      return;
    } else {
      std::get<I>(out).inject(std::get<I>(in));
      inject_all<I + 1>(in, out);
    }
  }

  /****************************************************************
  *                                                              *
  * Tuple versions of port actions                               *
  *                                                              *
  ****************************************************************/

  /**
  * Test that all sinks are in the done state.  Always false if producer.
   */
  bool sink_done_all() {
    if constexpr (is_producer_) {
      return false;
    }
    return std::apply(
        [](auto&... sink) { return (sink.get_mover()->is_done() && ...); },
        inputs_);
  }

  /**
  * Test that at least one sink is in the done state.  Always false if
  * producer.
   */
  bool sink_done_any() {
    if constexpr (is_producer_) {
      return false;
    }
    return std::apply(
        [](auto&... sink) { return (sink.get_mover()->is_done() || ...); },
        inputs_);
  }

  /**
  * Test that all sources are in the done state.  Always false if consumer.
   */
  bool source_done_all() {
    if constexpr (is_consumer_) {
      return false;
    }
    return std::apply(
        [](auto&... source) { return (source.get_mover()->is_done() && ...); },
        outputs_);
  }

  /**
  * Test that at least one source is in the done state.  Always false if
  * consumer.
   */
  bool source_done_any() {
    if constexpr (is_consumer_) {
      return false;
    }
    return std::apply(
        [](auto&... source) { return (source.get_mover()->is_done() || ...); },
        outputs_);
  }

  /**
  * Apply port_pull to every input port.  Make empty function if producer.
   */
  auto pull_all() {
    static_assert(!is_producer_);
    return tuple_fold<0 /*std::tuple_size_v<decltype(inputs_)>*/>(
        &mimo_node_impl_base::either<scheduler_event_type::sink_wait>,
        [](auto& sink) { return sink.get_mover()->port_pull(); },
        inputs_);
  }

  /**
  * Apply port_drain to every input port.  Make empty function if producer.
   */
  auto drain_all() {
    static_assert(!is_producer_);
    return tuple_fold<0>(
        &mimo_node_impl_base::either<scheduler_event_type::notify_source>,
        [](auto& sink) { return sink.get_mover()->port_drain(); },
        inputs_);
  }

  /**
  * Apply port_fill to every output port.  Make empty function if consumer.
   */
  auto fill_all() {
    static_assert(!is_consumer_);
    return tuple_fold<0>(
        &mimo_node_impl_base::either<scheduler_event_type::notify_sink>,
        [](auto& source) { return source.get_mover()->port_fill(); },
        outputs_);
  }

  /**
  * Apply port_push to every output port.  Make empty function if consumer.
   */
  auto push_all() {
    static_assert(!is_consumer_);
    return tuple_fold<0>(
        &mimo_node_impl_base::either<scheduler_event_type::source_wait>,
        [](auto& source) { return source.get_mover()->port_push(); },
        outputs_);
  }

  /**
  * Send stop to every output port.
   */
  auto stop_all() {
    return tuple_fold<0>(
        &mimo_node_impl_base::either<scheduler_event_type::source_wait>,
        [](auto& source) { return source.get_mover()->port_exhausted(); },
        inputs_);
  }

  /****************************************************************
  *                                                              *
  *                  End Helper functions                        *
  *                                                              *
  ****************************************************************/
 public:
  /**
  * Primary constructor.
  *
  * @param f A function that accepts a tuple of input items and sets values in
  * tuple of output items.
  *
  * @tparam The type of the function (or function object) that accepts items.
  *
  * @note The enclosed function is assumed to be stateless.  I.e., it can be
  * restarted with the same input multiple times and produce the same output
  * each time.  This requirement will be necessary for stopping and restarting
  * these nodes.
   */
  template <class Function>
  explicit mimo_node_impl_base(
      Function&& f,
      std::enable_if_t<
          std::is_invocable_r_v<
              std::tuple<BlocksOut...>,
              Function,
              const std::tuple<BlocksIn...>&>,
          void**> = nullptr)
      : f_{std::forward<Function>(f)}
      , inputs_{} {
  }

  /**
  * Secondary constructor: Consumer node.
  *
  * @param f A function that accepts a tuple of input items and returns void.
  *
  * @tparam The type of the function (or function object) that accepts items.
   */
  template <class Function>
  explicit mimo_node_impl_base(
      Function&& f,
      std::enable_if_t<
          are_same_v<std::tuple<>, BlocksOut...> &&
              std::is_invocable_r_v<
                  void,
                  Function,
                  const std::tuple<BlocksIn...>&> &&
              sizeof...(BlocksIn) != 1,
          void**> = nullptr)
      : f_{std::forward<Function>(f)}
      , inputs_{} {
  }

  template <class Function>
  explicit mimo_node_impl_base(
      Function&& f,
      std::enable_if_t<
          are_same_v<std::tuple<>, BlocksOut...> &&
              std::is_invocable_r_v<void, Function, BlocksIn&...> &&
              sizeof...(BlocksIn) == 1,
          void**> = nullptr)
      : f_{std::forward<Function>(f)}
      , inputs_{} {
  }

  /**
  * Secondary constructor: Producer node.
  *
  * @param f A function that accepts void and fills in output items.
  *
  * @tparam The type of the function (or function object) that accepts items.
   */
  template <class Function>
  explicit mimo_node_impl_base(
      Function&& f,
      std::enable_if_t<
          are_same_v<std::tuple<>, BlocksIn...> && std::is_invocable_r_v<
                                                       std::tuple<BlocksOut...>,
                                                       Function,
                                                       std::stop_source&>,
          void**> = nullptr)
      : f_{std::forward<Function>(f)}
      , inputs_{} {
  }

  template <class Function>
  explicit mimo_node_impl_base(
      Function&& f,
      std::enable_if_t<
          are_same_v<std::tuple<>, BlocksIn...> &&
              std::
                  is_invocable_r_v<BlocksOut..., Function, std::stop_source&> &&
              sizeof...(BlocksOut) == 1,
          void**> = nullptr)
      : f_{std::forward<Function>(f)}
      , inputs_{} {
  }
};
} // namespace tiledb::common
#endif // TILEDB_DAG_NODES_DETAIL_SEGMENTED_MIMO_BASE_H
