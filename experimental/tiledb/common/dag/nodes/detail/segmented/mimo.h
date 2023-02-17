/**
 * @file   experimental/tiledb/common/dag/nodes/detail/segmented/mimo.h
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

#ifndef TILEDB_DAG_NODES_DETAIL_SEGMENTED_MIMO_H
#define TILEDB_DAG_NODES_DETAIL_SEGMENTED_MIMO_H

#include <functional>
#include <tuple>
#include <type_traits>
#include <utility>

#include "experimental/tiledb/common/dag/execution/jthread/stop_token.hpp"
#include "experimental/tiledb/common/dag/nodes/segmented_nodes.h"
#include "experimental/tiledb/common/dag/ports/ports.h"
#include "experimental/tiledb/common/dag/utility/print_types.h"

namespace tiledb::common {

/**
 * @todo Partial specialization for non-tuple types?
 *
 * @todo CTAD deduction guides to simplify construction (and eliminate
 * redundancy, site of errors, due to template args of class and function in
 * constructor).
 *
 */

/**
 * Some type aliases for the function enclosed by the node, to allow empty
 * inputs or empty outputs (for creating producer or consumer nodes for
 * testing/validation from function node).
 */
template <
    class BlocksIn = std ::tuple<std::tuple<>>,
    class BlocksOut = std::tuple<std::tuple<>>>
struct fn_type;

template <class... BlocksIn, class... BlocksOut>
struct fn_type<std::tuple<BlocksIn...>, std::tuple<BlocksOut...>> {
  using type =
      std::function<std::tuple<BlocksOut...>(const std::tuple<BlocksIn...>&)>;
};

template <class... BlocksOut>
struct fn_type<std::tuple<>, std::tuple<BlocksOut...>> {
  using type = std::function<std::tuple<BlocksOut...>(std::stop_source&)>;
};

template <class... BlocksIn>
struct fn_type<std::tuple<BlocksIn...>, std::tuple<>> {
  using type = std::function<void(const std::tuple<BlocksIn...>&)>;
};

/**
 *
 */
template <
    template <class>
    class SinkMover,
    class BlocksIn,
    template <class> class SourceMover = SinkMover,
    class BlocksOut = BlocksIn>
class mimo_node_impl;

/*
 * By specializing through the use of `std::tuple`, we are able to have two
 * variadic template lists.  Thus, we can define a node with `size_t` and `int`
 * for its inputs and `size_t` and `double` for its outputs in the following
 * way:
 *
 * @code{.cpp}
 * mimo_node<AsyncMover2, std::tuple<size_t, int>,
 *                     AsyncMover3, std::tuple<size_t, double>> x{};
 * @endcode
 *
 * @todo Variadic list of movers (one per input/output)?
 */
template <
    template <class>
    class SinkMover,
    class... BlocksIn,
    template <class>
    class SourceMover,
    class... BlocksOut>
class mimo_node_impl<
    SinkMover,
    std::tuple<BlocksIn...>,
    SourceMover,
    std::tuple<BlocksOut...>>
    : public node_base {  // @todo Inherit from tuple<Source>, tuple<Sink>?

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
   *  @todo It would be nice to use std::array here, but it doesn't appear to
   *  support different types for each element.
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

 private:
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

  /**
   * Helper function to deal with tuples.  Applies the same single input single
   * output function to elements of an input tuple to set values of an output
   * tuple.
   *
   * @note Elements are processed in order from 0 to sizeof(Ts)-1
   *
   * @pre sizeof(Ts) == sizeof(Us)
   */
  template <size_t I = 0, class Fn, class... Ts, class... Us>
  constexpr void tuple_map(
      Fn&& f, const std::tuple<Ts...>& in, std::tuple<Us...>& out) {
    static_assert(sizeof(in) == sizeof(out));
    if constexpr (I == sizeof...(Ts)) {
      return;
    } else {
      std::get<I>(out) = std::forward<Fn>(f)(std::get<I>(in));
      tuple_map(I + 1, std::forward<Fn>(f), in, out);
    }
  }

  template <size_t I = 0, class Op, class Fn, class... Ts>
  constexpr auto tuple_fold(Op&& op, Fn&& f, const std::tuple<Ts...>& in) {
    // static_assert(I == 0);
    static_assert(I >= 0);
    // static_assert(sizeof...(Ts) > 0 && sizeof...(Ts) < 10);
    if constexpr (I == sizeof...(Ts) - 1) {
      return f(std::get<I>(in));
    } else {
      return op(f(std::get<I>(in)), tuple_fold<I + 1>(op, f, in));
    }
  }

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
        &mimo_node_impl::either<scheduler_event_type::sink_wait>,
        [](auto& sink) { return sink.get_mover()->port_pull(); },
        inputs_);
  }

  /**
   * Apply port_drain to every input port.  Make empty function if producer.
   */
  auto drain_all() {
    static_assert(!is_producer_);
    return tuple_fold<0>(
        &mimo_node_impl::either<scheduler_event_type::notify_source>,
        [](auto& sink) { return sink.get_mover()->port_drain(); },
        inputs_);
  }

  /**
   * Apply port_fill to every output port.  Make empty function if consumer.
   */
  auto fill_all() {
    static_assert(!is_consumer_);
    return tuple_fold<0>(
        &mimo_node_impl::either<scheduler_event_type::notify_sink>,
        [](auto& source) { return source.get_mover()->port_fill(); },
        outputs_);
  }

  /**
   * Apply port_push to every output port.  Make empty function if consumer.
   */
  auto push_all() {
    static_assert(!is_consumer_);
    return tuple_fold<0>(
        &mimo_node_impl::either<scheduler_event_type::source_wait>,
        [](auto& source) { return source.get_mover()->port_push(); },
        outputs_);
  }

  /**
   * Send stop to every output port.
   */
  auto stop_all() {
    return tuple_fold<0>(
        &mimo_node_impl::either<scheduler_event_type::source_wait>,
        [](auto& source) { return source.get_mover()->port_exhausted(); },
        inputs_);
  }

  template <class T, class... Ts>
  struct are_same : std::conjunction<std::is_same<T, Ts>...> {};

  template <class T, class... Ts>
  inline constexpr static bool are_same_v = are_same<T, Ts...>::value;

  /****************************************************************
   *                                                              *
   *                  End Helper functions                        *
   *                                                              *
   ****************************************************************/

 public:
  /**
   * Default constructor, for testing only.
   */
  mimo_node_impl() = default;

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
  explicit mimo_node_impl(
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
  explicit mimo_node_impl(
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
  explicit mimo_node_impl(
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
  explicit mimo_node_impl(
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
  explicit mimo_node_impl(
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

  /**
   * Function for applying all actions of the node: Pull the sinks, fill the
   * input tuple from sinks, apply stored function, fill the output tuple, and
   * push the sources.
   *
   * @todo Develop better model and API for use of `instruction_counter_` as
   * part of `GeneralNode` object and as interface to scheduler.
   *
   * @note Right now, yielding (waiting) may be done in `Mover` calls (via a
   * `Policy`), i.e., `port_push` and `port_pull`.  Those should interface to
   * the scheduler, e.g., and cause a return from `resume` or `resume` that
   * will then be started from when the scheduler is notified (again, down in
   * the `Mover` policy).
   *
   * @note It would be nice if this could be agnostic here as to what kind of
   * scheduler is being used, but it isn't clear how to do a return in the case
   * of a coroutine-like scheduler, or a condition-variable wait in the case of
   * an `std::async` (or similar) scheduler. Perhaps we could use a return value
   * of `port_pull` or `port_push` to indicate what to do next (yield or
   * continue). Or we could pass in a continuation?  Either approach seems to
   * couple `Mover`, `Scheduler`, and `Node` too much.
   *
   * @todo Maybe we need a many-to-one and a one-to-many `ItemMover`.  We could
   * then put items in the mover rather than in the nodes -- though that would
   * make the nodes almost superfluous.
   *
   */
  scheduler_event_type resume() override {
    std::stop_source stop_source_;

    switch (this->program_counter_) {
      case 0: {
        ++this->program_counter_;

        if constexpr (!is_producer_) {
          auto tmp_state = pull_all();

          if (sink_done_all()) {
            return stop_all();
            break;
          } else {
            return tmp_state;
          }
        }
      }
        [[fallthrough]];

      case 1: {
        ++this->program_counter_;
        if constexpr (!is_producer_) {
          extract_all(inputs_, input_items_);
        }
      }
        [[fallthrough]];

      case 2: {
        ++this->program_counter_;
        if constexpr (!is_producer_) {
          return drain_all();
        }
      }
        [[fallthrough]];

      case 3: {
        ++this->program_counter_;
      }
        [[fallthrough]];

      case 4: {
        ++this->program_counter_;

        /**
         * @todo Should this instead be output_items = f_(input_items_)
         * @todo Maybe use variant?
         * @todo How to deal with `stop_source`?
         */
        if constexpr (is_producer_) {
          output_items_ = f_(stop_source_);
        } else if constexpr (is_consumer_) {
          f_(input_items_);
        } else {
          output_items_ = f_(input_items_);
        }
      }
        [[fallthrough]];

      case 5: {
        ++this->program_counter_;

        if constexpr (!is_consumer_) {
          inject_all(output_items_, outputs_);
        }
      }
        [[fallthrough]];

      case 6: {
        ++this->program_counter_;

        if constexpr (!is_consumer_) {
          return fill_all();
        }
      }
        [[fallthrough]];

      case 7: {
        ++this->program_counter_;
      }
        [[fallthrough]];

      case 8: {
        ++this->program_counter_;
        if constexpr (!is_consumer_) {
          return push_all();
        }
      }
        [[fallthrough]];
      case 9: {
        this->program_counter_ = 0;
        return scheduler_event_type::yield;
      }
        // [[fallthrough]];

      default: {
        break;
      }
    }

    return scheduler_event_type::yield;
  }

  /** Run the node until it is done. */
  void run() override {
    while (!sink_done_all() && !source_done_all()) {
      resume();
    }
    if constexpr (!is_producer_) {
      if (!sink_done_all()) {
        pull_all();
      }
    }
  }
  void dump_node_state() override {
  }
};

#if 0
// @todo: Define function_node et al as degenerate mimo nodes?

/** A mimo node is a shared pointer to the implementation class */
template <
    template <class>
    class SinkMover,
    class BlockIn,
    template <class> class SourceMover,
    class BlockOut>
struct function_node
    : public std::shared_ptr<
          function_node_impl<SinkMover, BlockIn, SourceMover, BlockOut>> {
  using Base = std::shared_ptr<
      function_node_impl<SinkMover, BlockIn, SourceMover, BlockOut>>;
  using Base::Base;

  template <class Function>
  explicit function_node(Function&& f)
      : Base{std::make_shared<
            function_node_impl<SinkMover, BlockIn, SourceMover, BlockOut>>(
            std::forward<Function>(f))} {
  }

  explicit function_node(
      function_node_impl<SinkMover, BlockIn, SourceMover, BlockOut>& impl)
      : Base{std::make_shared<
            function_node_impl<SinkMover, BlockIn, SourceMover, BlockOut>>(
            std::move(impl))} {
  }
};
#endif
#if 0
template <
    template <class>
    class SinkMover,
    class... BlocksIn,
    template <class>
    class SourceMover,
    class... BlocksOut>
class mimo_node<
    SinkMover,
    std::tuple<BlocksIn...>,
    SourceMover,
    std::tuple<BlocksOut...>>
    : public std::shared_ptr<
          mimo_node_impl<SinkMover, BlocksIn..., SourceMover, BlocksOut...>> {};
#else
template <class T>
struct EmptyMover {
  void operator()() {
  }
};

template <
    template <class>
    class SinkMover,
    class BlocksIn,
    template <class> class SourceMover = SinkMover,
    class BlocksOut = BlocksIn>
class mimo_node
    : public std::shared_ptr<
          mimo_node_impl<SinkMover, BlocksIn, SourceMover, BlocksOut>> {
  using PreBase = mimo_node_impl<SinkMover, BlocksIn, SourceMover, BlocksOut>;
  using Base = std::shared_ptr<PreBase>;
  using Base::Base;

 public:
  template <class Function>
  explicit mimo_node(Function&& f)
      : Base{std::make_shared<PreBase>(std::forward<Function>(f))} {
  }
};

template <template <class> class SourceMover, class BlocksOut>
using producer_mimo =
    mimo_node<EmptyMover, std::tuple<>, SourceMover, BlocksOut>;

template <template <class> class SinkMover, class BlocksIn>
using consumer_mimo = mimo_node<SinkMover, BlocksIn, EmptyMover, std::tuple<>>;

#endif

template <class MimoNode, size_t portnum>
struct Proxy {
  constexpr static const size_t portnum_{portnum};
  MimoNode* node_ptr_;
  Proxy(MimoNode& node)
      : node_ptr_{&node} {
  }
};

template <size_t N, class T>
auto make_proxy(const T& u) {
  return Proxy<std::remove_reference_t<decltype(u)>, N>(u);
}
template <typename T>
struct is_proxy : std::false_type {};

template <typename T, size_t portnum>
struct is_proxy<Proxy<T, portnum>> : std::true_type {};

template <class T>
constexpr const bool is_proxy_v{is_proxy<T>::value};
#if 0
/**
 * @brief A proxy class for accessing the inputs and outputs of a mimo node
 * as if they were a single port.
 *
 * @todo Should be able to unify all of these into a single class, and use
 * ctad -- leave that for later
 *
 * @tparam MimoNode The mimo node type
 * @tparam portnum The port number to access
 */
template <class MimoNode, size_t input_portnum, size_t output_portnum>
struct Proxy : public std::tuple_element<input_portnum, decltype(MimoNode::inputs_)>::type,
               public std::tuple_element<output_portnum, decltype(MimoNode::outputs_)>::type {
  using input_ = typename std::tuple_element<input_portnum, decltype(MimoNode::inputs_)>::type;
  using output_ = typename std::tuple_element<output_portnum, decltype(MimoNode::outputs_)>::type;

  /**
   * @brief Construct a new Proxy object
   *
   * @param node The mimo node to proxy
   */
  Proxy(MimoNode& node) : input_{std::get<input_portnum>(node.inputs_)},
      output_{std::get<output_portnum>(node.outputs_)}{}
};

/**
 * @brief A proxy class for accessing the inputs of a mimo node
 * as if they were a single port.
 *
 * @tparam MimoNode The mimo node type
 * @tparam portnum The port number to access
 */
template <class MimoNode, size_t portnum>
struct InputProxy : public std::tuple_element<portnum, decltype(MimoNode::inputs_)>::type {
  using input_ = typename std::tuple_element<portnum, decltype(MimoNode::inputs_)>::type;

  /**
   * @brief Construct a new InputProxy object
   *
   * @param node The mimo node to proxy
   */
  InputProxy(MimoNode& node) : input_{std::get<portnum>(node.inputs_)}{}
};

/**
 * @brief A proxy class for accessing the outputs of a mimo node
 * @tparam MimoNode
 * @tparam portnum
 */
template <class MimoNode, size_t portnum>
struct OutputProxy : public std::tuple_element<portnum, decltype(MimoNode::outputs_)>::type {
  using output_ = typename std::tuple_element<portnum, decltype(MimoNode::outputs_)>::type;

  /**
   * @brief Construct a new OutputProxy object
   *
   * @param node The mimo node to proxy
   */
  OutputProxy(MimoNode& node) : output_{std::get<portnum>(node.outputs_)}{}
};
#endif

}  // namespace tiledb::common
#endif  // TILEDB_DAG_NODES_DETAIL_SEGMENTED_MIMO_H
