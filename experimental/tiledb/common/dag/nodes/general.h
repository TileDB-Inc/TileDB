/**
 * @file   general.h
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
 * This file declares the general nodes classes for the dag task graph library.
 */

#ifndef TILEDB_DAG_GENERAL_H
#define TILEDB_DAG_GENERAL_H

#include <functional>
#include <type_traits>
#include "experimental/tiledb/common/dag/ports/ports.h"
#include "experimental/tiledb/common/dag/state_machine/test/helpers.h"

#include "experimental/tiledb/common/dag/execution/stop_token.hpp"

#include "experimental/tiledb/common/dag/nodes/base.h"
#include "experimental/tiledb/common/dag/utils/print_types.h"

namespace tiledb::common {

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

constexpr unsigned short to_index(NodeState x) {
  return static_cast<unsigned short>(x);
}

namespace {
constexpr unsigned short num_states = to_index(NodeState::last) + 1;

std::vector<std::string> node_state_strings{"init",
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
 redundancy,
 * site of errors, due to template args of class and function in constructor).

 * @todo Unify function behavior for simple and general function nodes.
 */

/**
 * Some type aliases for the enclosed function, to allow empty inputs or empty
 * outputs (for creating producer or consumer nodes from function node).
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
    class CalculationState,
    template <class>
    class SinkMover_T,
    class BlocksIn,
    template <class> class SourceMover_T = SinkMover_T,
    class BlocksOut = BlocksIn>
class GeneralFunctionNode;

template <
    class CalculationState,
    template <class>
    class SinkMover_T,
    class... BlocksIn,
    template <class>
    class SourceMover_T,
    class... BlocksOut>
class GeneralFunctionNode<
    CalculationState,
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

  CalculationState current_state_;
  CalculationState new_state_;

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
  constexpr void tuple_extract(std::tuple<Ts...>& in, std::tuple<Us...>& out) {
    static_assert(sizeof...(Ts) == sizeof...(Us));
    if constexpr (I == sizeof...(Ts)) {
      return;
    } else {
      std::get<I>(out) = *(std::get<I>(in).extract());
      tuple_extract<I + 1>(in, out);
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
  constexpr void tuple_inject(std::tuple<Ts...>& in, std::tuple<Us...>& out) {
    static_assert(sizeof...(Ts) == sizeof...(Us));
    if constexpr (I == sizeof...(Ts)) {
      return;
    } else {
      std::get<I>(out).inject(std::get<I>(in));
      tuple_inject<I + 1>(in, out);
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
   * Function for applying all actions of the node: Pull the sinks, fill the
   * input tuple from sinks, apply stored function, fill the output tuple, and
   * push the sources.
   *
   * @todo Develop better model and API for use of `instruction_counter_` as
   * part of `GeneralNode` object and as interface to scheduler.
   *
   * @note Right now, yielding (waiting) may be done in `Mover` calls (via a
   * `Policy`), i.e., `do_push` and `do_pull`.  Those should interface to the
   * scheduler, e.g., and cause a return from `resume` or `run_once` that will
   * then be started from when the scheduler is notified (again, down in the
   * `Mover` policy).
   *
   * @note It would be nice if this could be agnostic here as to what kind of
   * scheduler is being used, but it isn't clear how to do a return in the case
   * of a coroutine-like scheduler, or a condition-variable wait in the case of
   * an `std::async` (or similar) scheduler. Perhaps we could use a return value
   * of `do_pull` or `do_push` to indicate what to do next (yield or continue).
   * Or we could pass in a continuation?  Either approach seems to couple
   * `Mover`, `Scheduler`, and `Node` too much.
   *
   * @todo Maybe we need a many-to-one and a one-to-many `ItemMover`.  We could
   * then put items in the mover rather than in the nodes -- though that would
   * make the nodes almost superfluous.
   */

  NodeState run_once() {
    switch (instruction_counter_) {
      case NodeState::init:

        instruction_counter_ = NodeState::input;
      case NodeState::input:

        /*
         * Here begins pull-check-extract-drain (aka `input`)
         *
         * @todo Add (indicate) state update
         *
         * @todo Atomicity with extract and drain?  (Note that we can't extract
         * if we get a done on pull()).
         */

        /*
         * A custom "f_"
         */

        // ----------------------------------------------------------------
        // pull all
        std::apply(
            [](auto&... sink) { (sink.get_mover()->do_pull(), ...); }, inputs_);
        // @todo Should we have a NodeState case here to resume (since pull may
        // block/return)

        /*
         * Check all for source or sink connections being done
         *
         * @note All sources or all sinks need to be done at the same time.
         *
         * @todo Develop model and interface for partial completion (i.e., only
         * some of the members of the tuple being done while others not).
         */
        {
          auto sink_done = std::apply(
              [](auto&... sink) {
                return (sink.get_mover()->is_done() && ...);
              },
              inputs_);

          auto source_done = std::apply(
              [](auto&... source) {
                return (source.get_mover()->is_done() && ...);
              },
              outputs_);

          if (sink_done || source_done) {
            instruction_counter_ = NodeState::done;
            return instruction_counter_;
          }
        }

        // extract all
        tuple_extract(inputs_, input_items_);

        // drain all
        std::apply(
            [](auto&... sink) { (sink.get_mover()->do_drain(), ...); },
            inputs_);
        // ----------------------------------------------------------------

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
        /*
         * Here begins inject-fill-push (aka `output`).
         *
         * @todo Atomicity?
         */

        // ----------------------------------------------------------------
        // inject all
        tuple_inject(output_items_, outputs_);

        // fill all
        std::apply(
            [](auto&... source) { (source.get_mover()->do_fill(), ...); },
            outputs_);

        // push all
        std::apply(
            [](auto&... source) { (source.get_mover()->do_push(), ...); },
            outputs_);
        // ----------------------------------------------------------------

        instruction_counter_ = NodeState::done;

      case NodeState::done:
        break;

      default:
        break;
    }
    return instruction_counter_;
  }

  /**
   *
   * Function for invoking `run_once` multiple times, as given by the input
   * parameter, or until the node is stopped, whichever happens first.
   *
   * @param rounds The maximum number of times to invoke the producer function.
   * @todo Develop model and API for yielding after each (or some number of)
   * `run_once`.
   */
  void run_for(size_t rounds) {
    while (rounds--) {
      auto sink_done = std::apply(
          [](auto&... sink) { return (sink.get_mover()->is_done() && ...); },
          inputs_);

      auto source_done = std::apply(
          [](auto&... source) {
            return (source.get_mover()->is_done() && ...);
          },
          outputs_);

      if (sink_done || source_done) {
        break;
      }

      run_once();
      reset();
    }
    auto sink_done = std::apply(
        [](auto&... sink) { return (sink.get_mover()->is_done() && ...); },
        inputs_);

    if (!sink_done) {
      std::apply(
          [](auto&... sink) { (sink.get_mover()->do_pull(), ...); }, inputs_);
    }
    std::apply(
        [](auto&... source) { (source.get_mover()->do_stop(), ...); },
        outputs_);
  }

  /**
   * Function for invoking `run_once` multiple times, until the node is
   * stopped.
   *
   * @todo Develop model and API for yielding after each (or some number of)
   * `run_once`.  This doesn't really do a "resume".
   *
   */
  NodeState resume() {
    auto sink_done = std::apply(
        [](auto&... sink) { (sink.get_mover()->is_done() && ...); }, inputs_);

    auto source_done = std::apply(
        [](auto&... source) { (source.get_mover()->is_done() && ...); },
        outputs_);

    /*
     * Call run_once repeatedly until done.  Right now yielding is done inside
     * of run_once.  Perhaps part (or all) of Duff device should happen here
     * instead?
     */
    while (!source_done && !sink_done) {
      run_once(instruction_counter_);
    }

    sink_done = std::apply(
        [](auto&... sink) { (sink.get_mover()->is_done() && ...); }, inputs_);

    if (!sink_done) {
      std::apply(
          [](auto&... sink) { (sink.get_mover()->do_pull(), ...); }, inputs_);
    }

    std::apply(
        [](auto&... source) { (source.get_mover()->do_stop(), ...); },
        outputs_);

    instruction_counter_ = NodeState::exit;

    return instruction_counter_;
  }

  /**
   * `run_once` leaves the `instruction_counter` in the `done` state.  `reset`
   * sets the `instruction_counter_` to `input` so that `run_once` can be
   * invoked again.
   */
  NodeState reset() {
    instruction_counter_ = NodeState::input;
    return instruction_counter_;
  }
};

}  // namespace tiledb::common
#endif  // TILEDB_DAG_GENERAL_H
