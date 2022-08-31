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
 * @todo Unify function behavior for simple and general function nodes.
 */
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
  /*
   * Make public for now for testing
   */
 public:
  // Alternatively... ?
  // std::function<std::tuple<BlocksOut...>(std::tuple<BlocksIn...>)> f_;
  std::function<void(const std::tuple<BlocksIn...>&, std::tuple<BlocksOut...>&)>
      f_;

  std::tuple<Sink<SinkMover_T, BlocksIn>...> inputs_;
  std::tuple<Source<SourceMover_T, BlocksOut>...> outputs_;

 private:
  std::tuple<BlocksIn...> input_items_;
  std::tuple<BlocksOut...> output_items_;

  CalculationState current_state_;
  CalculationState new_state_;

  NodeState instruction_counter_{NodeState::init};

  template <size_t I = 0, class Fn, class... Ts, class... Us>
  constexpr void tuple_map(
      Fn&& f, std::tuple<Ts...>& in, std::tuple<Us...>& out) {
    static_assert(sizeof(in) == sizeof(out));
    if constexpr (I == sizeof...(Ts)) {
      return;
    } else {
      std::get<I>(out) = std::forward<Fn>(f)(std::get<I>(in));
      tuple_map(I + 1, std::forward<Fn>(f), in, out);
    }
  }

  // From inputs to input_items
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

  // From output_items to outputs
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
  /*
   * For testing
   */
  GeneralFunctionNode() = default;

  /**
   * Constructor
   * @param f A function that accepts items.
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
   * Pull the sinks, fill the input tuple, apply stored function, fill the
   * output tuple, and push the sources.
   */
  NodeState run_once() {
    switch (instruction_counter_) {
      case NodeState::init:

        instruction_counter_ = NodeState::input;
      case NodeState::input:
        // pull
        std::apply(
            [](auto&... sink) { (sink.get_mover()->do_pull(), ...); }, inputs_);

        // check for source or sink connections being done
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

        // extract
        tuple_extract(inputs_, input_items_);

        // drain
        std::apply(
            [](auto&... sink) { (sink.get_mover()->do_drain(), ...); },
            inputs_);

        instruction_counter_ = NodeState::compute;
      case NodeState::compute:

        // apply
        // f(input_items_, output_items_);

        f_(input_items_, output_items_);

        instruction_counter_ = NodeState::output;
      case NodeState::output:

        // inject
        tuple_inject(output_items_, outputs_);

        // fill
        std::apply(
            [](auto&... source) { (source.get_mover()->do_fill(), ...); },
            outputs_);

        // push
        std::apply(
            [](auto&... source) { (source.get_mover()->do_push(), ...); },
            outputs_);

        instruction_counter_ = NodeState::done;

      case NodeState::done:
        break;

      default:
        break;
    }
    return instruction_counter_;
  }

  /*
   * Function for invoking `run_once` mutiple times, as given by the input
   * parameter, or until the node is stopped, whichever happens first.
   *
   * @param rounds The maximum number of times to invoke the producer function.
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

  /*
   * Function for invoking `run_once` mutiple times, until the node is stopped.
   *
   */
  NodeState resume() {
    auto sink_done = std::apply(
        [](auto&... sink) { (sink.get_mover()->is_done() && ...); }, inputs_);

    auto source_done = std::apply(
        [](auto&... source) { (source.get_mover()->is_done() && ...); },
        outputs_);

    while (!source_done && !sink_done) {
      return run_once(instruction_counter_);
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

  NodeState reset() {
    instruction_counter_ = NodeState::input;
    return instruction_counter_;
  }
};

}  // namespace tiledb::common
#endif  // TILEDB_DAG_GENERAL_H
