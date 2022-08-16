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

namespace tiledb::common {

/**
 * A trivial base class so that we can store containers of different types of
 * nodes in a single container.
 */
struct GraphNode {};

/**
 * Flow control class.  To be used as optional argument to function call inside
 * a `ProducerNode` to indicate there will not be any more data produced by the
 * function.  The return value from the function when stop is called is
 * meaningless.
 */
class FlowControl {
  std::atomic<bool> stopped_{false};

 public:
  void stop() {
    stopped_ = true;
  }

  bool is_stopped() {
    return stopped_;
  }
};

/**
 * Producer node.  Constructed with a function that creates Blocks.  A Producer
 * is derived from Source.
 *
 * @tparam Mover_T The type of item mover to be used by this class.  The class
 * can invoke any of the functions in the mover.  The `ProducerNode` will invoke
 * `do_fill`, `do_push`, `inject`, and `do_stop`.
 *
 * @tparam Mover_T The type of item mover to be used with the `ProducerNode`. It
 * is a template template to be composed with `Block`.  @tparam Block The type
 * of data to be produced by the `ProducerNode` and sent through the item mover.
 */

template <template <class> class Mover_T, class Block>
class ProducerNode : public GraphNode, public Source<Mover_T, Block> {
  using Base = Source<Mover_T, Block>;
  using source_type = Source<Mover_T, Block>;
  /**
   * FlowControl object to be used by the function.
   */
  FlowControl flow_control_;

  /**
   * The function to be invoked by the `ProducerNode`
   */
  std::function<Block()> f_;

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
   * @param f A function that creates items.
   * @tparam The type of the function (or function object) that generates items.
   */
  template <class Function>
  explicit ProducerNode(
      Function&& f,
      std::enable_if_t<std::is_invocable_r_v<Block, Function>, void**> =
          nullptr)
      : f_{std::forward<Function>(f)} {
  }

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

  /**
   * Function for invoking the function in the `ProducerNode` and sending it to
   * the item mover.  The `run` function will invoke the stored function one
   * time (unless the state machine in the item mover is stopped).  It will also
   * issue `stop` if the `flow_control` is stopped (the latter to be
   * implemented).
   */
  void run() {
    auto state_machine = this->get_mover();

    /**
     * @todo Make inject and do_fill atomic (while properly separating
     * concerns). Atomic on the mover would be the right thing.  There seems to
     * still be the problem that the function will have created the item but is
     * handing it off to the mover.  Need to do inject with an atomic swap so
     * that the state of the function can know whether or not it has handed the
     * item to the mover.
     */
    //  { state == st_00 ∨ state == st_01 }

    if (state_machine->is_done()) {
      return;
    }
    Base::inject(f_());

    if (flow_control_.is_stopped()) {
      if (state_machine->debug_enabled())
        std::cout << "Producer stopping" << std::endl;
      state_machine->do_stop();
      return;
    }

    state_machine->do_fill();
    //  { state == st_10 ∨ state == st_11 }

    state_machine->do_push();
    //  { state == st_01 ∨ state == st_00 }
  }

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
  void run_for(size_t rounds) {
    auto state_machine = this->get_mover();
    if (state_machine->debug_enabled())
      std::cout << this->get_mover() << std::endl;

    while (rounds--) {
      if (state_machine->debug_enabled())
        std::cout << "producer starting " << rounds << std::endl;

      Base::inject(f_());
      state_machine->do_fill();

      if (state_machine->debug_enabled())
        std::cout << "producer filled " << rounds
                  << "state: " << str(state_machine->state()) << std::endl;

      state_machine->do_push();
      if (state_machine->debug_enabled())
        std::cout << "producer pushed " << rounds << std::endl;
    }
    if (state_machine->debug_enabled())
      std::cout << "run_for stopping" << std::endl;
    state_machine->do_stop();
  }

  /**
   * Same as `run_for` but with random delays inserted.  Used for testing to
   * encourage race conditions and deadlocks.
   */
  void run_for_with_delays(size_t rounds) {
    auto state_machine = this->get_mover();
    if (state_machine->debug_enabled())
      std::cout << this->get_mover() << std::endl;

    while (rounds--) {
      if (state_machine->debug_enabled())
        std::cout << "producer starting " << rounds << std::endl;

      Base::inject(f_());
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
      std::cout << "run_for stopping" << std::endl;
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

  /**
   * Function for fetching an item from item mover and invoking the function in
   * the `ConsumerNode`. The `run` function will invoke the stored function one
   * time (unless the state machine in the item mover is stopped).
   */
  void run() {
    auto state_machine = this->get_mover();

    /**
     * @todo Make properly atomic so there isn't any inconsistent state.  When
     * pull is called, it will set Sink state to 1.  But extract can remove the
     * item from the Sink while the state is still equal to 1.  In fact, it will
     * be inconsistent during all of the function call invocation.  Should
     * perhaps move drain to come directly after extract and make extract and
     * drain atomic so there is never the conditon that the state machine has
     * value of 1 for the Sink after the item has been cleared (and vice versa).
     *
     * Or maybe we don't need to worry about atomicity since scheduling is
     * cooperative?
     */
    //  {{ state == st_00 ∨ state == st_10 } ∧ { item == empty }}
    //  ∨
    //  {{ state == st_01 } ∨ { state == st_11 } ∧ { item == full }}
    state_machine->do_pull();
    //  { state == st_01 ∨ state == st_11 } ∧ { item == full }

    // Returns an optional, may not be necessary given stop state
    // @todo Pass in b as parameter so assignment is atomic
    auto b = Base::extract();
    //  { state == st_01 ∨ state == st_11 } ∧ { item == empty }  (This is bad.)
    //  Source cannot change st_11 until drain is called.

    state_machine->do_drain();
    //  {{ state == st_00 ∨ state == st_10 } ∧ { item == empty }}
    //  ∨
    //  {{ state == st_01 } ∨ { state == st_11 } ∧ { item == full }}
    //  Since Source could have filled and pushed (perhaps twice)

    //  We could move drain here and make atomic with extract() ?
    //  Then would have { state == st_00 ∨ state == st_10 } ∧ { item == empty }
    //  And Source would be notified, which could change state to
    //  {{ state == st_01 ∨ state == st_11 } ∧ { item == full }}
    //  ∨
    //  {{ state == st_10 } ∧ { item == empty }
    //  We have the previous item in b, so { item == full } is okay
    //  This would benefit concurrency

    //  Or maybe not have an extract and just send (full) item to f_() ?
    CHECK(b.has_value());

    //  Here we have that item has been extracted and
    // { item == empty } ∨ { item == full }
    // (but would be different item if full)
    f_(*b);
    //  Here we have that item has been processed and
    // { item == empty } ∨ { item == full }
    // Deallocate b
  }

  /**
   * Function for obtaining items from the item mover and invoking the stored
   * function on each item. The `run_for` function will invoke the stored
   * function multiple times or until the state machine in the item mover is
   * stopped, whichever happens first.
   *
   * @param rounds The maximum number of times to invoke the producer function.
   */
  void run_for(size_t rounds) {
    auto state_machine = this->get_mover();

    while (rounds--) {
      if (state_machine->debug_enabled())
        std::cout << "consumer starting " << rounds << std::endl;

      if (state_machine->debug_enabled())
        std::cout << this->get_mover() << std::endl;

      state_machine->do_pull();

      if (state_machine->debug_enabled())
        std::cout << "consumer pulled " << rounds
                  << " ( done: " << state_machine->is_done() << " )"
                  << std::endl;

      if (state_machine->is_done()) {
        if (state_machine->debug_enabled())
          std::cout << "consumer breaking i " << rounds << std::endl;
        break;
      }

      if (state_machine->debug_enabled())
        std::cout << "consumer checked done " << rounds
                  << " ( done: " << state_machine->is_done() << " )"
                  << std::endl;

      auto b = Base::extract();

      if (state_machine->debug_enabled())
        std::cout << "consumer extracted, about to drain " << rounds
                  << std::endl;

      state_machine->do_drain();

      if (state_machine->debug_enabled())
        std::cout << "consumer drained " << rounds << std::endl;

      f_(*b);

      if (state_machine->debug_enabled())
        std::cout << "consumer ran function " << rounds << std::endl;

      if (state_machine->is_done()) {
        if (state_machine->debug_enabled())
          std::cout << "consumer breaking ii " << rounds << std::endl;
        break;
      }
    }
    if (!state_machine->is_done()) {
      state_machine->do_pull();
    }
  }

  /**
   * Same as the above function but with random delays inserted to encourage
   * race conditions and deadlocks.
   */
  void run_for_with_delays(size_t rounds) {
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
 * a Block.  Derived from both `Sink` and `Source`.  The `FunctionNode` accepts an item on its `Sink` and submits it to its `Source`
 *
 * @tparam SinkMover_T The type of item mover to be used by the `Sink` part of this class.  The class
 * can invoke any of the functions in the mover.  The `Source` part of the `FunctionNode` will invoke
 * `do_fill`, `do_push`, `inject`, and `do_stop`.
 *
 * @tparam SourceMover_T The type of item mover to be used with the `ProducerNode` part of thi class. It
 * is a template template to be composed with `Block`.  
 *
 * @tparam BlockIn The type of item to be consumed by the `Sink` part of the `ProducerNode`.
 *
 * @tparam BlockIn The type of item to be produced by the `Source` part of the `ProducerNode`.
 */
template <
    template <class>
    class SinkMover_T,
    class BlockIn,
    template <class> class SourceMover_T = SinkMover_T,
    class BlockOut = BlockIn>
class FunctionNode : public GraphNode,
                     public Source<SourceMover_T, BlockOut>,
                     public Sink<SinkMover_T, BlockIn> {
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

  /**
   * Function for obtaining an item from the `Sink` part, applying the store
   * function, and sending the resulting item to the item mover. The `run`
   * function will invoke the stored function one time (unless the state machine
   * in the item mover is stopped).
   *
   * @todo Address atomicity, per ProducerNode and ConsumerNode above?
   */
  bool run() {
    auto source_state_machine = SourceBase::get_mover();
    auto sink_state_machine = SinkBase::get_mover();

    if (source_state_machine->is_done() || sink_state_machine->is_done()) {
      if (sink_state_machine->debug_enabled())
        std::cout << "Function node stopping" << std::endl;

      source_state_machine->do_stop();
      return true;
    }

    sink_state_machine->do_pull();
    auto b = SinkBase::extract();
    sink_state_machine->do_drain();

    CHECK(b.has_value());

    if (b.has_value()) {
      auto j = f_(*b);

      SourceBase::inject(j);
      source_state_machine->do_fill();
      source_state_machine->do_push();

      return true;
    } else {
      if (sink_state_machine->debug_enabled() ||
          source_state_machine->debug_enabled()) {
        std::cout << "No value in function node" << std::endl;
      }
      return false;
    }
  }

  /**
   * Function for extracting data from the item mover, invoking the function in
   * the `FunctionNode` and sending the result to * the item mover.  The
   * `run_for` function will invoke the stored function * multiple times, given
   * by the input parameter, or until the node is stopped, * whichever happens
   * first.It will also issue `stop` if either its `Sink` portion or its
   * `Source` portion is stopped.
   *
   * @param rounds The maximum number of times to invoke the producer function.
   */
  void run_for(size_t rounds) {
    auto source_state_machine = SourceBase::get_mover();
    auto sink_state_machine = SinkBase::get_mover();

    while (rounds--) {
      sink_state_machine->do_pull();

      if (sink_state_machine->debug_enabled())
        std::cout << "function pulled " << rounds
                  << " ( done: " << sink_state_machine->is_done() << " )"
                  << std::endl;

      /*
       * The "other side" of the sink state machine is a `Source`, which can be
       * stopped.  Similarly, the "other side" of the `Source` could be stopped.
       */
      if (source_state_machine->is_done() || sink_state_machine->is_done()) {
        if (sink_state_machine->debug_enabled())
          std::cout << "function breaking i " << rounds << std::endl;
        break;
      }

      if (sink_state_machine->debug_enabled())
        std::cout << "function checked done " << rounds
                  << " ( done: " << sink_state_machine->is_done() << " )"
                  << std::endl;

      auto b = SinkBase::extract();

      if (sink_state_machine->debug_enabled())
        std::cout << "function extracted, about to drain " << rounds
                  << std::endl;

      sink_state_machine->do_drain();

      if (sink_state_machine->debug_enabled())
        std::cout << "function drained " << rounds << std::endl;

      if (b.has_value()) {
        auto j = f_(*b);

        if (sink_state_machine->debug_enabled())
          std::cout << "function ran function " << rounds << std::endl;

        SourceBase::inject(j);
        if (source_state_machine->debug_enabled())
          std::cout << "function injected " << rounds << std::endl;

        source_state_machine->do_fill();
        if (source_state_machine->debug_enabled())
          std::cout << "function filled " << rounds << std::endl;

        source_state_machine->do_push();
        if (source_state_machine->debug_enabled())
          std::cout << "function pushed " << rounds << std::endl;

      } else {
        if (source_state_machine->debug_enabled()) {
          std::cout << "No value in function node @ " << rounds << std::endl;
          std::cout << "State = " << str(sink_state_machine->state()) << " @ "
                    << rounds << std::endl;
        }
        break;
      }

      if (source_state_machine->is_done() || sink_state_machine->is_done()) {
        if (sink_state_machine->debug_enabled())
          std::cout << "function break ii " << rounds << std::endl;
        break;
      }
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
  void run_for_with_delays(size_t rounds) {
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
