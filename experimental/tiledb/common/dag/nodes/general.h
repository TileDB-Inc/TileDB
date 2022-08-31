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

#if 0

    if (sink_state_machine->debug_enabled())
      std::cout << "function pulled "
                << " ( done: " << sink_state_machine->is_done() << " )"
                << std::endl;

    /*
     * The "other side" of the sink state machine is a `Source`, which can be
     * stopped.  Similarly, the "other side" of the `Source` could be stopped.
     */
    if (source_state_machine->is_done() || sink_state_machine->is_done()) {
      if (sink_state_machine->debug_enabled())
        std::cout << "function returning i " << std::endl;
      return;
    }

    if (sink_state_machine->debug_enabled())
      std::cout << "function checked done "
                << " ( done: " << sink_state_machine->is_done() << " )"
                << std::endl;

    // @todo as elsewhere, extract+drain should be atomic
    auto b = SinkBase::extract();

    if (sink_state_machine->debug_enabled())
      std::cout << "function extracted, about to drain " << std::endl;

    sink_state_machine->do_drain();

    if (sink_state_machine->debug_enabled())
      std::cout << "function drained " << std::endl;

    //    if (b.has_value()) {

    auto j = f_(*b);

    if (sink_state_machine->debug_enabled())
      std::cout << "function ran function " << std::endl;

    // @todo as elsewhere, inject+fill should be atomic
    SourceBase::inject(j);
    if (source_state_machine->debug_enabled())
      std::cout << "function injected " << std::endl;

    source_state_machine->do_fill();
    if (source_state_machine->debug_enabled())
      std::cout << "function filled " << std::endl;

    source_state_machine->do_push();
    if (source_state_machine->debug_enabled())
      std::cout << "function pushed " << std::endl;

    //    } else {
    //      if (source_state_machine->debug_enabled()) {
    //        std::cout << "No value in function node @ " << std::endl;
    //        std::cout << "State = " << str(sink_state_machine->state()) << " @
    //        "
    //                  << std::endl;
    //      }
    //      return;
    //  }

    if (source_state_machine->is_done() || sink_state_machine->is_done()) {
      if (sink_state_machine->debug_enabled())
        std::cout << "function break ii " << std::endl;
      return;
    }
  }

  /*
   * Function for invoking `run_once` mutiple times, as given by the input
   * parameter, or until the node is stopped, whichever happens first.
   *
   * @param rounds The maximum number of times to invoke the producer function.
   */
  void run_for(size_t rounds) {
    auto source_state_machine = SourceBase::get_mover();
    auto sink_state_machine = SinkBase::get_mover();

    while (rounds--) {
      if (sink_state_machine->is_done() || source_state_machine->is_done()) {
        break;
      }
      run_once();
    }
    if (!sink_state_machine->is_done()) {
      if (sink_state_machine->debug_enabled())
        std::cout << "function final pull " << rounds << std::endl;
      sink_state_machine->do_pull();
    }
    source_state_machine->do_stop();
  }

  /*
   * Function for invoking `run_once` mutiple times, until the node is stopped.
   *
   */
  void run() {
    auto source_state_machine = SourceBase::get_mover();
    auto sink_state_machine = SinkBase::get_mover();

    while (!sink_state_machine->is_done() && !source_state_machine->is_done()) {
      run_once();
    }
    if (!sink_state_machine->is_done()) {
      if (sink_state_machine->debug_enabled())
        std::cout << "function final pull in run()" << std::endl;
      sink_state_machine->do_pull();
    }
    source_state_machine->do_stop();
  }
#endif
};

#if 0

enum class NodeEvent : unsigned short {
  run,
  yield,
  have_input,
  done_computing,
  wait,
  notified,
  done,
  stop,
  error,
  abort,
  last
};

inline constexpr unsigned short to_index(NodeEvent x) {
  return static_cast<unsigned short>(x);
}

/**
 * Number of events in the NodeEvent state machine
 */
constexpr unsigned int n_events = to_index(NodeEvent::last) + 1;

/**
 * Strings for each enum member, useful for debugging.
 */
static std::vector<std::string> event_strings{"run",
                                              "yield",
                                              "have_input",
                                              "done_computing",
                                              "wait",
                                              "notified",
                                              "done",
                                              "stop",
                                              "error",
                                              "abort",
                                              "last"};

/**
 * Function to convert event to a string.
 *
 * @param event The event to stringify.
 * @return The string corresponding to the event.
 */
static inline auto str(NodeEvent ev) {
  return event_strings[static_cast<int>(ev)];
}

}  // namespace

static inline auto str(NodeState st) {
  return node_state_strings[static_cast<int>(st)];
}

namespace {

// clang-format off
constexpr const NodeState node_transition_table[num_states][n_events]{

    /* start */      /* run */            /* yield */          /* have_input */  /* done_computing */  /* wait */       /* notified */       /* done */        /* stop */        /* error */       /* abort */

    /* input */    { NodeState::error,    NodeState::runnable, NodeState::compute, NodeState::error,  NodeState::waiting, NodeState::error,    NodeState::error, NodeState::error, NodeState::error, NodeState::error, },
    /* compute */  { NodeState::error,    NodeState::runnable, NodeState::error,   NodeState::output, NodeState::waiting, NodeState::error,    NodeState::error, NodeState::error, NodeState::error, NodeState::error, },
    /* output */   { NodeState::error,    NodeState::runnable, NodeState::error,   NodeState::error,  NodeState::waiting, NodeState::error,    NodeState::error, NodeState::error, NodeState::error, NodeState::error, },

    /* waiting */  { NodeState::error,    NodeState::error,    NodeState::error,   NodeState::error,  NodeState::error,   NodeState::runnable, NodeState::error, NodeState::error, NodeState::error, NodeState::error, },
    /* runnable */ { NodeState::last,     NodeState::error,    NodeState::error,   NodeState::error,  NodeState::error,   NodeState::error,    NodeState::error, NodeState::error, NodeState::error, NodeState::error, },
    /* running  */ { NodeState::error,    NodeState::error,    NodeState::error,   NodeState::error,  NodeState::error,   NodeState::error,    NodeState::error, NodeState::error, NodeState::error, NodeState::error, },

    /* done */     { NodeState::error,    NodeState::error,    NodeState::error,   NodeState::error,  NodeState::error,   NodeState::error,    NodeState::error, NodeState::error, NodeState::error, NodeState::error, },
    /* error */    { NodeState::error,    NodeState::error,    NodeState::error,   NodeState::error,  NodeState::error,   NodeState::error,    NodeState::error, NodeState::error, NodeState::error, NodeState::error, },
    /* abort */    { NodeState::error,    NodeState::error,    NodeState::error,   NodeState::error,  NodeState::error,   NodeState::error,    NodeState::error, NodeState::error, NodeState::error, NodeState::error, },

    /* last */     { NodeState::error,    NodeState::error,    NodeState::error,   NodeState::error,  NodeState::error,   NodeState::error,    NodeState::error, NodeState::error, NodeState::error, NodeState::error, },
};

// clang-format on
}  // namespace

#if 0
template <
    class CalculationState,
    template <class>
    class SinkMover_T,
    std::tuple<BlockIn...>,  // Multiple types
    // Only one mover type:  Input Mover and Output are the same
    std::tuple<BlockOut...>>
class FunctionNode : protected GeneralGraphNode<CalculationState> {
  using SourceMover_T = SinkMover_T;  // just Mover_T

  std::tuple<Source<SourceMover_T, BlockOut>...>> inputs_;
  std::tuple<Sink<SinkMover_T, BlockIn>>... >> outputs_;

  FunctionNode() = default;

  // Bound function node looks like this
  template <class Function>
  FunctionNode(Function&& f) {
  }

  FunctionNode()
      : GeneralGraphNode<CalculationState>{} {
  }

  // Restart from suspension constructor
  FunctionNode(const CalculationState& saved_calculation_state)
      : GeneralGraphNode<CalculationState>{saved_calculation_state} {
  }

  // User implements this rather than passing in a function
  // Scheduler just calls this and passes no calculation state -- i.e., this is
  // really a resume.
  virtual void operator()() = 0;
  //
  //
  // Sink:
  //   pull() -- moves data forward along the channel x0 -> x1
  //   drain() -- state machine event x1 -> x0
  //
  // Source:
  //   fill()
  //   push()
  //
  // NodeInput<N>
  //   std::tuple_ref<N, BlockIn>& value()
  //
  //   template <size_t N>
  //   constexpr std::tuple_ref<N, BlockIn>& value() {}
  //
  //   auto foo = node_inputs_v<N>.value();
  //
  // Application developer gives user-readable names to tuple indices
  // using lhs = node_inputs_v<0>;   // Or something similar
  // using rhs = node_inputs_v<1>;
  //
  // decltype(auto) foo = lhs.value();
  //
  // ConsumerNode step():
  //
  // This is "g()":
  // switch (new_state.instruction_pointer) {
  //
  // step(top);
  // case top:
  //   auto input_block = lhs.value();   // value() calls try_pull(), return an
  //   optional if (!input_block.has_value()) {
  //     return;
  //   }
  //
  // step(middle);
  // case middle:
  //   auto && [output_block, new_state, really] = work on input_block
  //
  //   current_state <- new_state
  //   if (not done working) {
  //     if (yield requested)
  //       return;
  //   }
  //
  // step(bottom);
  // case bottom:
  //   lhs.consume();
  //   change_state() {
  //   --------------------------------
  //     consume item(lhs.get_consume())
  //     update_state
  //     drain(lhs.get_consume())
  //   --------------------------------
  //   }
  //
  // case abort:
  //   abort();
  //
  // case done:
  //   // Clean up work...
  //   finished();
  // }

  // NodeOutput

  //  operator()() {
  //  ProducerNode step():
  //  - auto new_, block = f(old_, block)
  //  - output() {
  //  -------------------------------
  //    - update_state (new_);
  //    - inject return_block;
  //    - fill
  //  --------------------------------
  //  }
  //  - push

  // user_function(
  //

  update_state() {
  }

  // update_state will do these
  update_state_with_output() {
  }
  update_state_with_input() {
  }
  update_state_with_input_and_output() {
  }

  yield() {
  }

  finish() {
  }
};
#endif
#endif

#if 0

/**
 * "Bound" Function node, based on `GeneralFunctionNode`.  Supports functions with no
 * state as well as stateful functions with no intermediate yields.  I.e., functions
 * that can be given to `BoundFunctionNode` in the form of initial state and final
 * state.  Allows specializations to emulate `BoundProducerNode` and
 * `BoundConsumerNode`, as well as the stateless `ProducerNode`, `ConsumerNode`, and
 * `FunctionNode` as given in simple.h.
 *
 * @tparam SinkMover_T The type of item mover to be used by the `Sink` part of
 * this class.  The class can invoke any of the functions in the mover.  The
 * `Source` part of the `FunctionNode` will invoke `do_fill`, `do_push`,
 * `inject`, and `do_stop`.
 *
 * @tparam SourceMover_T The type of item mover to be used with the `ProducerNode`
 * part of thi class. It is a template template to be composed with `Block`.
 *
 * @tparam BlockIn The type of item to be consumed by the `Sink` part of the
 * `ProducerNode`.
 *
 * @tparam BlockIn The type of item to be produced by the `Source` part of the
 * `ProducerNode`.
 *
 * @todo Do we want to be able to put things directly into the `Sink`?
 */
template <
    template <class>
    class SinkMover_T,
    class BlockIn,
    template <class> class SourceMover_T = SinkMover_T,
    class BlockOut = BlockIn>
class BoundFunctionNode : public GeneralFunctionNode<void>,
                          SinkMover_T,
                          BlockIn,
                          SourceMover_T,
                          BlockOut {
  // What follows goes into BoundFunctionNode

  std::function<BlockOut(const BlockIn&)> f_;
  using SourceBase = Source<SourceMover_T, BlockOut>;
  using SinkBase = Sink<SinkMover_T, BlockIn>;

 public:
  /**
   * Trivial default constructor, for testing.
   */
  FunctionNode() = default;
  FunctionNode(const FunctionNode&) {
  }
  FunctionNode(FunctionNode&&) = default;

  /**
   * Constructor
   * @param f A function that accepts items.
   * @tparam The type of the function (or function object) that accepts items.
   */
  template <class Function>
  explicit FunctionNode(
      Function&& f,
      std::enable_if_t<
          std::is_invocable_r_v<BlockOut, Function, BlockIn>,
          void**> = nullptr)
      : f_{std::forward<Function>(f)} {
  }

  /**
   * Constructor for anything non-invocable.  It is here to provide more
   * meaningful error messages when the constructor is called with wrong type of
   * arguments.
   */
  template <class Function>
  explicit FunctionNode(
      Function&&,
      std::enable_if_t<
          !std::is_invocable_r_v<BlockOut, Function, BlockIn>,
          void**> = nullptr) {
    static_assert(
        std::is_invocable_r_v<BlockOut, Function, BlockIn>,
        "Function constructor with non-invocable type");
  }

  void run() {
    auto source_state_machine = SourceBase::get_mover();
    auto sink_state_machine = SinkBase::get_mover();

    sink_state_machine->do_pull();

    if (sink_state_machine->debug_enabled())
      std::cout << "function pulled "
                << " ( done: " << sink_state_machine->is_done() << " )"
                << std::endl;

    /*
     * The "other side" of the sink state machine is a `Source`, which can be
     * stopped.  Similarly, the "other side" of the `Source` could be stopped.
     */
    if (source_state_machine->is_done() || sink_state_machine->is_done()) {
      if (sink_state_machine->debug_enabled())
        std::cout << "function returning i " << std::endl;
      return;
    }

    if (sink_state_machine->debug_enabled())
      std::cout << "function checked done "
                << " ( done: " << sink_state_machine->is_done() << " )"
                << std::endl;

    auto b = SinkBase::extract();

    if (sink_state_machine->debug_enabled())
      std::cout << "function extracted, about to drain " << std::endl;

    sink_state_machine->do_drain();

    if (sink_state_machine->debug_enabled())
      std::cout << "function drained " << std::endl;

    if (b.has_value()) {
      auto j = f_(*b);

      if (sink_state_machine->debug_enabled())
        std::cout << "function ran function " << std::endl;

      SourceBase::inject(j);
      if (source_state_machine->debug_enabled())
        std::cout << "function injected " << std::endl;

      source_state_machine->do_fill();
      if (source_state_machine->debug_enabled())
        std::cout << "function filled " << std::endl;

      source_state_machine->do_push();
      if (source_state_machine->debug_enabled())
        std::cout << "function pushed " << std::endl;

    } else {
      if (source_state_machine->debug_enabled()) {
        std::cout << "No value in function node @ " << std::endl;
        std::cout << "State = " << str(sink_state_machine->state()) << " @ "
                  << std::endl;
      }
      return;
    }

    if (source_state_machine->is_done() || sink_state_machine->is_done()) {
      if (sink_state_machine->debug_enabled())
        std::cout << "function break ii " << std::endl;
      return;
    }
  }

  /**
   * Function for extracting data from the item mover, invoking the function in
   * the `FunctionNode` and sending the result to  the item mover.  The
   * `run` function will invoke the stored function  multiple times, given
   * by the input parameter, or until the node is stopped,  whichever happens
   * first. It will also issue `stop` if either its `Sink` portion or its
   * `Source` portion is stopped.
   *
   * @param rounds The maximum number of times to invoke the producer function.
   */
  void run(size_t rounds) {
    auto source_state_machine = SourceBase::get_mover();
    auto sink_state_machine = SinkBase::get_mover();

    while (rounds--) {
      if (sink_state_machine->is_done() || source_state_machine->is_done()) {
        break;
      }
      run();
    }
    if (!sink_state_machine->is_done()) {
      if (sink_state_machine->debug_enabled())
        std::cout << "function final pull " << rounds << std::endl;
      sink_state_machine->do_pull();
    }
    source_state_machine->do_stop();
  }

  /**
   * Same as the previous function, but with random delays inserted between
   * operations to expose race conditions and deadlocks.
   */
  void run_with_delays(size_t rounds) {
    auto source_state_machine = SourceBase::get_mover();
    auto sink_state_machine = SinkBase::get_mover();

    while (rounds--) {
      sink_state_machine->do_pull();

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(555)));

      if (source_state_machine->is_done() || sink_state_machine->is_done()) {
        break;
      }

      CHECK(is_sink_full(sink_state_machine->state()) == "");
      auto b = SinkBase::extract();

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(555)));

      sink_state_machine->do_drain();

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(555)));
      if (b.has_value()) {
        auto j = f_(*b);

        SourceBase::inject(j);
        std::this_thread::sleep_for(std::chrono::microseconds(random_us(555)));

        source_state_machine->do_fill();
        std::this_thread::sleep_for(std::chrono::microseconds(random_us(555)));
        source_state_machine->do_push();

      } else {
        if (source_state_machine->debug_enabled())
          std::cout << "No value in function node" << std::endl;
        break;
      }
      if (rounds == 0) {
        sink_state_machine->do_pull();
      }
      std::this_thread::sleep_for(std::chrono::microseconds(random_us(555)));
    }
    source_state_machine->do_stop();
  }
};

template <class Mover_T, class Block>
using BoundProducerNode = BoundFunctionNode<NullMover, void, Mover_T, Block>;

template <class Mover_T, class Block>
using BoundConsumerNode = BoundFunctionNode<Mover_T, Block, NullMover, void>;

#endif

}  // namespace tiledb::common
#endif  // TILEDB_DAG_GENERAL_H
