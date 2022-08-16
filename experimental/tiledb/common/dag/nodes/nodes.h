/**
 * @file   node.h
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
 * This file declares the nodes classes for dag.
 */

#ifndef TILEDB_DAG_NODE_H
#define TILEDB_DAG_NODE_H

#include <functional>
#include <type_traits>
#include "experimental/tiledb/common/dag/ports/ports.h"
#include "experimental/tiledb/common/dag/state_machine/test/helpers.h"

#include "experimental/tiledb/common/dag/execution/stop_token.hpp"

namespace tiledb::common {

/**
 * A trivial base class so that we can store containers of different types of
 * nodes in a single container.
 */
template <class CalculationState>
class GraphNode {
 protected:
  CalculationState current_calculation_state_;
  CalculationState new_calculation_state_;

 public:
  // including swap
  update_state();

  virtual void run(size_t = 1) = 0;
  //  virtual void stop();
};

template <>
class GraphNode<void> : GraphNode<monostate> {};

/**
 * Flow control class.  To be used as optional argument to function call inside
 * a `ProducerNode` to indicate there will not be any more data produced by the
 * function.  The return value from the function when stop is called is
 * meaningless.
 */

/**
 * Producer node.  Constructed with a function that creates Blocks.  A
 * Producer is derived from Source.
 *
 * @tparam Mover_T The type of item mover to be used by this class.  The
 * class can invoke any of the functions in the mover.  The `ProducerNode`
 * will invoke `do_fill`, `do_push`, `inject`, and `do_stop`.
 *
 * @tparam Mover_T The type of item mover to be used with the
 * `ProducerNode`. It is a template template to be composed with `Block`.
 * @tparam Block The type of data to be produced by the `ProducerNode` and
 * sent through the item mover.
 */

template <template <class> class Mover_T, class Block>
class ProducerNode : public GraphNode, public Source<Mover_T, Block> {
  using Base = Source<Mover_T, Block>;
  using source_type = Source<Mover_T, Block>;
  /**
   * `std::stop_source` to be used with `ProducerNode`
   */
  std::stop_source stop_source_;

  /**
   * The function to be invoked by the `ProducerNode`
   */
  std::variant<std::function<Block()>, std::function<Block(std::stop_source&)>>
      f_;

 public:
  /**
   * Trivial default constructor, for testing.
   */
  ProducerNode() = default;

  /**
   * Trivial copy constructor, needed to satisfy `is_movable`
   */
  ProducerNode(const ProducerNode&) {
  }
  ProducerNode(ProducerNode&&) = default;

  /**
   * Constructor
   * @param f A function taking void that creates items.
   *
   * @tparam The type of the function (or function object) that generates items.
   */

  /**
   * Constructor: Function takes void, returns Block
   */
  template <class Function>
  ProducerNode(
      Function&& f,
      std::enable_if_t<
          !std::is_bind_expression_v<
              std::remove_cv_t<std::remove_reference_t<Function>>>,
          void**> = nullptr)
      : f_{std::forward<Function>(f)} {
  }

  /**
   * Constructor: Special constructor for std::bind, no std::stop_source
   */
  template <class Function>
  ProducerNode(
      Function&& f,
      std::enable_if_t<
          std::is_bind_expression_v<
              std::remove_cv_t<std::remove_reference_t<Function>>> &&
              std::is_invocable_r_v<Block, Function, void>,
          void**> = nullptr)
      : f_{[&f]() { return std::forward<Function>(f)(); }} {
  }

  /**
   * Constructor: Special constructor for std::bind, with std::stop_source
   */
  template <class Function>
  ProducerNode(
      Function&& f,
      std::enable_if_t<
          std::is_bind_expression_v<
              std::remove_cv_t<std::remove_reference_t<Function>>> &&
              std::is_invocable_r_v<Block, Function, std::stop_source&>,
          void**> = nullptr)
      : f_{[&f](std::stop_source& s) { return std::forward<Function>(f)(s); }} {
  }

#if 0

  template <class Function>
  explicit ProducerNode(Function&& f)
      : f_{f} {
  }



 
  template <class Function>
  explicit ProducerNode(
      Function&& f,
      std::enable_if_t<
          !std::is_bind_expression_v<U>
          //          ((std::is_invocable_r_v<Block, Function> ||
          //            std::is_invocable_r_v<
          //                Block,
          //                Function,
          //                std::
          //                    stop_source>)&&(std::is_bind_expression_v<Function>
          //                    == false)),
          ,
          void**> = nullptr)
      : f_{std::forward<Function>(f)} {
  }

  template <class Function>
  explicit ProducerNode(
      Function& f,
      std::enable_if_t<
          ((std::is_invocable_r_v<Block, Function> ||
            std::is_invocable_r_v<Block, Function, std::stop_source>)&&std::
               is_bind_expression_v<Function>),
          void**> = nullptr)
      : f_{[&f]() { return std::forward<Function>(f)(); }} {
  }

  template <class Function>
  explicit ProducerNode(
      Function&& f,
      std::enable_if_t<
          ((std::is_invocable_r_v<Block, Function> ||
            std::is_invocable_r_v<Block, Function, std::stop_source>)&&std::
               is_bind_expression_v<Function>),
          void**> = nullptr)
      : f_{[&f]() { return std::forward<Function>(f)(); }} {
  }

#if 0
  /**
   * Constructor
   * @param f A function that creates items.
   * @tparam The type of the function (or function object) that generates items.
   */
  template <class Function>
  explicit ProducerNode(
      Function&& f,
      std::enable_if_t<
          (std::is_invocable_r_v<Block, Function> ||
           std::is_invocable_r_v<Block, Function, std::stop_source>),

          void**> = nullptr)
      : f_{std::forward<Function>(f)} {
  }
#endif
#if 0
  /**
   * Constructor 
   * @param f A function that creates items.  The function should be
   * invocable with a flow_control object.  Still trying to figure out how to allow
   * overloading of the stored function, so this is not used for now.  
   * @tparam The type of the function (or function object) that generates items.
   */
  template <class Function>
  explicit ProducerNode(
      Function&& f,
      std::enable_if_t<std::is_invocable_r_v<Block, Function>, void**> =
          nullptr)
      : f_{[f](FlowControl&) { return std::forward<Function>(f)(); }} {
  }
#endif

  /**
   * Constructor for anything non-invocable.  It is here to provide more
   * meaningful error messages when the constructor is called with wrong type of
   * arguments.  Otherwise there is a huge list of unsuccessful substitutions,
   * which are not informative.
   */
  template <class Function>
  explicit ProducerNode(
      Function&&,
      std::enable_if_t<!std::is_invocable_r_v<Block, Function>, void**> =
          nullptr) {
    static_assert(
        std::is_invocable_r_v<Block, Function>,
        "Source constructor with non-invocable type");
  }
#endif

  /**
   * Function for invoking the function in the `ProducerNode` and sending it to
   * the item mover.  The `run_for` function will invoke the stored function
   * multiple times, given by the input parameter, or until the node is stopped,
   * whichever happens first.  time (unless the state machine in the item mover
   * is stopped).  It will also issue `stop` if the `flow_control` is stopped
   * (the latter to be implemented).
   *
   * @param rounds The maximum number of times to invoke the producer function.
   */
  void run() {
    auto state_machine = this->get_mover();
    /**
     * @todo Make inject and do_fill atomic (while properly separating
     * concerns). Atomic on the mover would be the right thing.  There seems
     * to still be the problem that the function will have created the item
     * but is handing it off to the mover.  Need to do inject with an atomic
     * swap so that the state of the function can know whether or not it has
     * handed the item to the mover.
     */
    //  { state == st_00 ∨ state == st_01 }

    switch (f_.index()) {
      case 0:
        Base::inject(std::get<0>(f_)());
        break;
      case 1:
        Base::inject(std::get<1>(f_)(stop_source_));
        break;
      default:
        throw std::logic_error("Impossible index for variant");
    }
    if (stop_source_.stop_requested()) {
      return;
    }

    state_machine->do_fill();
    //  { state == st_10 ∨ state == st_11 }

    if (state_machine->debug_enabled())
      std::cout << "producer filled "
                << "state: " << str(state_machine->state()) << std::endl;

    state_machine->do_push();
    //  { state == st_01 ∨ state == st_00 }

    if (state_machine->debug_enabled())
      std::cout << "producer pushed " << std::endl;
  }

  void run(size_t rounds) {
    auto state_machine = this->get_mover();
    if (state_machine->debug_enabled())
      std::cout << this->get_mover() << std::endl;

    if (state_machine->debug_enabled())
      std::cout << "producer starting " << rounds << std::endl;

    while (rounds--) {
      if (stop_source_.stop_requested()) {
        break;
      }

      run();
    }
    if (state_machine->debug_enabled())
      std::cout << "run stopping" << std::endl;
    state_machine->do_stop();
  }

  /**
   * Same as `run` but with random delays inserted.  Used for testing to
   * encourage race conditions and deadlocks.
   */
  void run_with_delays(size_t rounds) {
    auto state_machine = this->get_mover();
    if (state_machine->debug_enabled())
      std::cout << this->get_mover() << std::endl;

    while (rounds--) {
      if (state_machine->debug_enabled())
        std::cout << "producer starting " << rounds << std::endl;

      switch (f_.index()) {
        case 0:
          Base::inject(std::get<0>(f_)());
          break;
        case 1:
          Base::inject(std::get<1>(f_)(stop_source_));
          break;
        default:
          throw std::logic_error("Impossible index for variant");
      }

      if (state_machine->debug_enabled())
        std::cout << "producer injected " << rounds << std::endl;

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(555)));

      state_machine->do_fill();
      if (state_machine->debug_enabled())
        std::cout << "producer filled " << rounds << std::endl;

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(555)));

      state_machine->do_push();
      if (state_machine->debug_enabled())
        std::cout << "producer pushed " << rounds << std::endl;

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(555)));
    }
    if (state_machine->debug_enabled())
      std::cout << "run stopping" << std::endl;
    state_machine->do_stop();
  }
};

/**
 * Consumer node.  Constructed with a function that accepts Blocks and returns
 * void.
 *
 * @tparam Mover_T The type of item mover to be used by this class.  The class
 * can invoke any of the functions in the mover.  The `ConsumerNode` will invoke
 * `do_pull`, `do_drain`, and `extract`.
 *
 * @param Block The type of data to be obtained from the item mover and
 * consumed.
 */
template <template <class> class Mover_T, class Block>
class ConsumerNode : public GraphNode, public Sink<Mover_T, Block> {
  std::function<void(const Block&)> f_;

 public:
  using Base = Sink<Mover_T, Block>;

 public:
  /**
   * Trivial default constructor, for testing.
   */
  ConsumerNode() = default;
  ConsumerNode(const ConsumerNode&) {
  }
  ConsumerNode(ConsumerNode&&) = default;

  /**
   * Constructor
   *
   * @tparam The type of the function (or function object) that accepts items.
   * May be an actual function, a function object, or the result of `std::bind`.
   *
   * @param f A function that accepts items.  Must be invocable with an object
   * of type `Block`.
   */
  template <class Function>
  explicit ConsumerNode(
      Function&& f,
      std::enable_if_t<std::is_invocable_r_v<void, Function, Block>, void**> =
          nullptr)
      : f_{std::forward<Function>(f)} {
  }

  /**
   * Constructor for anything non-invocable.  It is here to provide more
   * meaningful error messages when the constructor is called with wrong type of
   * arguments.
   */
  template <class Function>
  explicit ConsumerNode(
      Function&&,
      std::enable_if_t<!std::is_invocable_r_v<void, Function, Block>, void**> =
          nullptr) {
    static_assert(
        std::is_invocable_v<Function, Block>,
        "Sink constructor with non-invocable type");
  }

  void run() {
    auto state_machine = this->get_mover();
    //  {{ state == st_00 ∨ state == st_10 } ∧ { item == empty }}
    //  ∨
    //  {{ state == st_01 } ∨ { state == st_11 } ∧ { item == full }}
    state_machine->do_pull();
    //  { state == st_01 ∨ state == st_11 } ∧ { item == full }

    if (state_machine->debug_enabled())
      std::cout << "consumer pulled "
                << " ( done: " << state_machine->is_done() << " )" << std::endl;

    if (state_machine->is_done()) {
      if (state_machine->debug_enabled())
        std::cout << "consumer done i " << std::endl;
      return;
    }

    if (state_machine->debug_enabled())
      std::cout << "consumer checked done "
                << " ( done: " << state_machine->is_done() << " )" << std::endl;

    //  { state == st_01 ∨ state == st_11 } ∧ { item == full }

    // Returns an optional, may not be necessary given stop state
    // @todo Pass in b as parameter so assignment is atomic
    auto b = Base::extract();
    //  { state == st_01 ∨ state == st_11 } ∧ { item == empty }  (This is
    //  bad.) Source cannot change st_11 until drain is called.

    if (state_machine->debug_enabled())
      std::cout << "consumer extracted, about to drain " << std::endl;

    state_machine->do_drain();
    //  {{ state == st_00 ∨ state == st_10 } ∧ { item == empty }}
    //  ∨
    //  {{ state == st_01 } ∨ { state == st_11 } ∧ { item == full }}
    //  Since Source could have filled and pushed (perhaps twice)

    //  We could move drain here and make atomic with extract() ?
    //  Then would have { state == st_00 ∨ state == st_10 } ∧ { item == empty
    //  } And Source would be notified, which could change state to
    //  {{ state == st_01 ∨ state == st_11 } ∧ { item == full }}
    //  ∨
    //  {{ state == st_10 } ∧ { item == empty }
    //  We have the previous item in b, so { item == full } is okay
    //  This would benefit concurrency

    if (state_machine->debug_enabled())
      std::cout << "consumer drained " << std::endl;

    //  Or maybe not have an extract and just send (full) item to f_() ?
    CHECK(b.has_value());

    //  Here we have that item has been extracted and
    // { item == empty } ∨ { item == full }
    // (but would be different item if full)
    f_(*b);
    //  Here we have that item has been processed and
    // { item == empty } ∨ { item == full }
    // Deallocate b

    if (state_machine->debug_enabled())
      std::cout << "consumer ran function " << std::endl;
  }

  /**
   * Function for obtaining items from the item mover and invoking the stored
   * function on each item. The `run` function will invoke the stored
   * function multiple times or until the state machine in the item mover is
   * stopped, whichever happens first.
   *
   * @param rounds The maximum number of times to invoke the producer function.
   */
  void run(size_t rounds) {
    auto state_machine = this->get_mover();

    while (rounds--) {
      if (state_machine->debug_enabled()) {
        std::cout << "consumer starting " << rounds << std::endl;
        std::cout << this->get_mover() << std::endl;
      }

      if (state_machine->is_done()) {
        if (state_machine->debug_enabled())
          std::cout << "consumer breaking ii " << rounds << std::endl;
        break;
      }
      run();
    }
    if (!state_machine->is_done()) {
      state_machine->do_pull();
    }
  }

  /**
   * Same as the above function but with random delays inserted to encourage
   * race conditions and deadlocks.
   */
  void run_with_delays(size_t rounds) {
    auto state_machine = this->get_mover();

    while (rounds--) {
      if (state_machine->debug_enabled())
        std::cout << "consumer starting " << rounds << std::endl;

      if (state_machine->debug_enabled())
        std::cout << this->get_mover() << std::endl;

      state_machine->do_pull();
      if (state_machine->debug_enabled())
        std::cout << "consumer pulled " << rounds << std::endl;

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(555)));

      if (state_machine->is_done()) {
        break;
      }

      if (state_machine->debug_enabled())
        std::cout << "consumer checked done " << rounds << std::endl;

      auto b = Base::extract();

      if (state_machine->debug_enabled())
        std::cout << "consumer extracted, about to drain " << rounds
                  << std::endl;

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(555)));

      state_machine->do_drain();

      if (state_machine->debug_enabled())
        std::cout << "consumer drained " << rounds << std::endl;

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(555)));

      f_(*b);

      if (state_machine->debug_enabled())
        std::cout << "consumer ran function " << rounds << std::endl;

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(555)));

      if (state_machine->is_done()) {
        break;
      }
    }
    if (!state_machine->is_done()) {
      state_machine->do_pull();
    }
  }
};

/**
 * Function node. Constructed with function that accepts a Block and returns
 * a Block.  Derived from both `Sink` and `Source`.  The `FunctionNode` accepts
 * an item on its `Sink` and submits it to its `Source`
 *
 * @tparam SinkMover_T The type of item mover to be used by the `Sink` part of
 * this class.  The class can invoke any of the functions in the mover.  The
 * `Source` part of the `FunctionNode` will invoke `do_fill`, `do_push`,
 * `inject`, and `do_stop`.
 *
 * @tparam SourceMover_T The type of item mover to be used with the
 * `ProducerNode` part of thi class. It is a template template to be composed
 * with `Block`.
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
    BoundFunctionNode : public FunctionNode < void,
    SinkMover_T, BlockIn, SourceMover_T,
    BlockOut{
        // Move stuff from current "FunctionNode"
    };

template <class Mover_T, class Block>
using BoundProducerNode = BoundFunctionNode<NullMover, void, Mover_T, Block>;

template <class Mover_T, class Block>
using BoundConsumerNode = BoundFunctionNode<Mover_T, Block, NullMover, void>;

template <
    class CalculationState,
    template <class>
    class SinkMover_T,
    std::tuple<BlockIn...>,  // Multiple types
    // Only one mover type:  Input Mover and Output are the same
    std::tuple<BlockOut...>>
class FunctionNode : protected GraphNode<CalculationState> {
  using SourceMover_T = SinkMover_T;  // just Mover_T

  std::tuple<Source<SourceMover_T, BlockOut>...>> inputs_;
  std::tuple<Sink<SinkMover_T, BlockIn>>... >> outputs_;

  FunctionNode() = default;

  // Bound function node looks like this
  template <class Function>
  FunctionNode(Function&& f) {
  }

  FunctionNode()
      : GraphNode<CalculationState>{} {
  }

  // Restart from suspension constructor
  FunctionNode(const CalculationState& saved_calculation_state)
      : GraphNode<CalculationState>{saved_calculation_state} {
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
  //   ________________________________
  //   consume item(lhs.get_consume())
  //   update_state
  //   drain(lhs.get_consume())
  //   ________________________________
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
   * the `FunctionNode` and sending the result to * the item mover.  The
   * `run` function will invoke the stored function * multiple times, given
   * by the input parameter, or until the node is stopped, * whichever happens
   * first.It will also issue `stop` if either its `Sink` portion or its
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

}  // namespace tiledb::common
#endif  // TILEDB_DAG_NODE_H
