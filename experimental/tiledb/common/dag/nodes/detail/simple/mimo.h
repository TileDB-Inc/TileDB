/**
 * @file   mimo.h
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
 * This file declares the general node class for the dag task graph library.  A
 * general node is a function node that takes multiple inputs and multiple
 * outputs. The stages of the general node are demarcated by cases of a switch
 * statement, in anticipation of potential use with a Duff's device based
 * scheduler.
 *
 * The general node can also be specialized to be a producer or consumer
 * respectively by passing in std::tuple<> for the input block type or for the
 * output block type. In either case, a dummy argument needs to be used for the
 * associated mover.
 *
 * @todo Specialize for non-tuple block types.
 *
 * @todo Specialize for void input/output types (rather than tuple<>) as well as
 * for void mover types.
 *
 * @todo This should be deprecated.
 */

#ifndef TILEDB_DAG_NODES_GENERAL_NODES_H
#define TILEDB_DAG_NODES_GENERAL_NODES_H

#include <functional>
#include <type_traits>

#include "experimental/tiledb/common/dag/ports/ports.h"
#include "experimental/tiledb/common/dag/state_machine/test/helpers.h"

#include "experimental/tiledb/common/dag/nodes/detail/simple/simple_base.h"

namespace tiledb::common {

/**
 * In anticipation of the possible use of a Duff's device mechanism for
 * interaction between nodes and schedulers, we define some candidate callback
 * states.
 */
enum class NodeState {
  init,
  input,
  compute,
  output,
  waiting,
  runnable,
  running,
  done,
  exit,
  error,
  abort,
  last
};

/**
 * A function to convert a `NodeState` to an integral type suitable for
 * indexing.
 */
constexpr unsigned short to_index(NodeState x) {
  return static_cast<unsigned short>(x);
}

/**
 * A count of the number of node states.
 */
namespace {
constexpr unsigned short num_states = to_index(NodeState::last) + 1;

/**
 * A vector of strings corresponding to the states.  Useful for diagnostics,
 * testing, and debugging.
 */
std::vector<std::string> node_state_strings{
    "init",
    "input",
    "compute",
    "output",
    "waiting",
    "runnable",
    "running",
    "done",
    "exit",
    "error",
    "abort",
    "last"};
}  // namespace

/**
 * @todo Partial specialization for non-tuple types?
 *
 * @todo CTAD deduction guides to simplify construction (and eliminate
 * redundancy, site of errors, due to template args of class and function in
 * constructor).
 *
 * @todo Unify function behavior for simple and general function nodes.
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
  using type = std::function<void(
      const std::tuple<BlocksIn...>&, std::tuple<BlocksOut...>&)>;
};

template <class... BlocksOut>
struct fn_type<std::tuple<>, std::tuple<BlocksOut...>> {
  using type = std::function<void(std::tuple<BlocksOut...>&)>;
};

template <class... BlocksIn>
struct fn_type<std::tuple<BlocksIn...>, std::tuple<>> {
  using type = std::function<void(const std::tuple<BlocksIn...>&)>;
};

template <
    template <class>
    class SinkMover_T,
    class BlocksIn,
    template <class> class SourceMover_T = SinkMover_T,
    class BlocksOut = BlocksIn>
class GeneralFunctionNode;

/*
 * By specializing through the use of `std::tuple`, we are able to have two
 * variadic template lists.  Thus, we can define a node with `size_t` and `int`
 * for its inputs and `size_t` and `double` for its outputs in the following
 * way:
 *
 * @code{.cpp}
 * GeneralFunctionNode<AsyncMover2, std::tuple<size_t, int>,
 *                     AsyncMover3, std::tuple<size_t, double>> x{};
 * @endcode
 *
 */
template <
    template <class>
    class SinkMover_T,
    class... BlocksIn,
    template <class>
    class SourceMover_T,
    class... BlocksOut>
class GeneralFunctionNode<
    SinkMover_T,
    std::tuple<BlocksIn...>,
    SourceMover_T,
    std::tuple<BlocksOut...>> {
  // Alternatively... ?
  // std::function<std::tuple<BlocksOut...>(std::tuple<BlocksIn...>)> f_;
  //  std::function<void(const std::tuple<BlocksIn...>&,
  //  std::tuple<BlocksOut...>&)> f_;
  typename fn_type<std::tuple<BlocksIn...>, std::tuple<BlocksOut...>>::type f_;

  // exists: SinkMover_T::scheduler
  // exists: SourceMover_T::scheduler

  /*
   * Make these public for now for testing
   *
   * @todo Develop better interface for `Edge` connections.
   */
 public:
  std::tuple<Sink<SinkMover_T, BlocksIn>...> inputs_;
  std::tuple<Source<SourceMover_T, BlocksOut>...> outputs_;

 private:
  /*
   * Tuple of items, collected from `inputs_` and `outputs_`.
   *
   * @todo Avoid copying somehow?  E.g., via `get_item()`?
   */
  std::tuple<BlocksIn...> input_items_;
  std::tuple<BlocksOut...> output_items_;

  NodeState instruction_counter_{NodeState::init};

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

 public:
  /**
   * Default constructor, for testing only.
   */
  GeneralFunctionNode() = default;

  template <class T, class... Ts>
  struct are_same : std::conjunction<std::is_same<T, Ts>...> {};

  template <class T, class... Ts>
  inline constexpr static bool are_same_v = are_same<T, Ts...>::value;

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
  explicit GeneralFunctionNode(
      Function&& f,
      std::enable_if_t<
          std::is_invocable_r_v<
              void,
              Function,
              const std::tuple<BlocksIn...>&,
              std::tuple<BlocksOut...>&>,
          void**> = nullptr)
      : f_{std::forward<Function>(f)}
      , inputs_{} {
  }

  /**
   * Secondary constructor.
   *
   * @param f A function that accepts a tuple of input items and returns void.
   *
   * @tparam The type of the function (or function object) that accepts items.
   */
  template <class Function>
  explicit GeneralFunctionNode(
      Function&& f,
      std::enable_if_t<
          are_same_v<std::tuple<>, BlocksOut...> &&
              std::is_invocable_r_v<
                  void,
                  Function,
                  const std::tuple<BlocksIn...>&>,
          void**> = nullptr)
      : f_{std::forward<Function>(f)}
      , inputs_{} {
  }

  /**
   * Secondary constructor.
   *
   * @param f A function that accepts void and fills in output items.
   *
   * @tparam The type of the function (or function object) that accepts items.
   */
  template <class Function>
  explicit GeneralFunctionNode(
      Function&& f,
      std::enable_if_t<
          are_same_v<std::tuple<>, BlocksIn...> &&
              std::is_invocable_r_v<void, Function, std::tuple<BlocksOut...>&>,
          void**> = nullptr)
      : f_{std::forward<Function>(f)}
      , inputs_{} {
  }

  /**
   * Test that all sinks are in the done state.  Always false if producer.
   */
  bool term_sink_all() {
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
  bool term_sink_any() {
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
  bool term_source_all() {
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
  bool term_source_any() {
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
  void pull_all() {
    if constexpr (is_producer_) {
      return;
    }
    std::apply(
        [](auto&... sink) { (sink.get_mover()->port_pull(), ...); }, inputs_);
  }

  /**
   * Apply port_drain to every input port.  Make empty function if producer.
   */
  void drain_all() {
    if constexpr (is_producer_) {
      return;
    }
    std::apply(
        [](auto&... sink) { (sink.get_mover()->port_drain(), ...); }, inputs_);
  }

  /**
   * Apply port_fill to every output port.  Make empty function if consumer.
   */
  void fill_all() {
    if constexpr (is_consumer_) {
      return;
    }
    std::apply(
        [](auto&... source) { (source.get_mover()->port_fill(), ...); },
        outputs_);
  }

  /**
   * Apply port_push to every output port.  Make empty function if consumer.
   */
  void push_all() {
    if constexpr (is_consumer_) {
      return;
    }
    std::apply(
        [](auto&... source) { (source.get_mover()->port_push(), ...); },
        outputs_);
  }

  /**
   * Send stop to every output port.
   */
  void stop_all() {
    std::apply(
        [](auto&... source) { (source.get_mover()->port_exhausted(), ...); },
        outputs_);
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
  NodeState resume() {
    switch (instruction_counter_) {
      case NodeState::init:

        instruction_counter_ = NodeState::input;
      case NodeState::input:
        /*
         * Here begins pull-check-extract-drain (aka `input`)
         * This follows the same pattern as for simple function nodes, but
         * over the tuple inputs and outputs.
         *
         * @todo Add (indicate) state update
         *
         */

        // pull
        pull_all();

        /*
         * Check if all sources or all sinks are done.
         *
         * @note All sources or all sinks need to be done for the `input_` or
         * `output_` to be considered done.
         *
         * @todo Develop model and interface for partial completion (i.e.,
         * only some of the members of the tuple being done while others not).
         *
         */
        if (term_sink_all() || term_source_all()) {
          instruction_counter_ = NodeState::done;
          return instruction_counter_;
        }

        // extract
        extract_all(inputs_, input_items_);

        // drain
        drain_all();

        instruction_counter_ = NodeState::compute;
      case NodeState::compute:

        /*
         * Here begins function application (aka compute).  We have constexpr
         * special cases for empty inputs or empty outputs (producers and
         * consumers).
         *
         * @todo Indicate use of state and application of state update.
         *
         * @todo Atomicity?
         */
        if constexpr (is_producer_) {
          f_(output_items_);
        } else if constexpr (is_consumer_) {
          f_(input_items_);
        } else {
          f_(input_items_, output_items_);
        }

        instruction_counter_ = NodeState::output;
      case NodeState::output:
        if constexpr (!is_consumer_) {
          /*
           * Here begins inject-fill-push (aka `output`).
           *
           */

          // inject
          inject_all(output_items_, outputs_);

          // fill
          fill_all();

          // push
          push_all();
        }
        instruction_counter_ = NodeState::done;

      case NodeState::done:
        break;

      case NodeState::exit:
        break;

      default:
        break;
    }
    return instruction_counter_;
  }

  /**
   *
   * Function for invoking `resume` multiple times, as given by the input
   * parameter, or until the node is stopped, whichever happens first.
   *
   * @param rounds The maximum number of times to invoke the producer function.
   * @todo Develop model and API for yielding after each (or some number of)
   * `resume`.
   */
  void run_for(size_t rounds) {
    while (rounds--) {
      if (term_sink_all() || term_source_all()) {
        break;
      }

      resume();
      reset();
    }
    if (!term_sink_all()) {
      pull_all();
    }
    stop_all();
  }

  /**
   * Function for invoking `resume` multiple times, until the node is
   * stopped.
   *
   * @todo Develop model and API for yielding after each (or some number of)
   * `resume`.  This doesn't really do a "resume".
   *
   */
  NodeState run() {
    /*
     * Call resume repeatedly until done.
     *
     * @note Right now yielding is done inside of resume.  Perhaps part (or
     * all) of Duff's device should happen here instead?
     */
    while (!term_source_all() && !term_sink_all()) {
      //      resume(instruction_counter_);
      resume();
      reset();
    }

    if (!term_sink_all()) {
      pull_all();
    }
    stop_all();

    instruction_counter_ = NodeState::exit;

    return instruction_counter_;
  }

  /**
   * `resume` leaves the `instruction_counter` in the `done` state.  `reset`
   * sets the `instruction_counter_` to `input` so that `resume` can be
   * invoked again.
   */
  NodeState reset() {
    instruction_counter_ = NodeState::input;
    return instruction_counter_;
  }
};

}  // namespace tiledb::common
#endif  // TILEDB_DAG_NODES_GENERAL_NODES_H
