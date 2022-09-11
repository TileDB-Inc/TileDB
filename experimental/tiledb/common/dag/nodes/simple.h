/**
 * @file   simple.h
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
 * This file declares "simple" nodes for dag task graph library.  Simple nodes
 * are nodes whose enclosed functions are assumed to have no state.  (More
 * specifically, simple nodes have no capability of maintaining, saving, nor
 * restoring state for the enclosed functions.
 */

#ifndef TILEDB_DAG_SIMPLE_H
#define TILEDB_DAG_SIMPLE_H

#include <functional>
#include <type_traits>

#include "experimental/tiledb/common/dag/execution/stop_token.hpp"
#include "experimental/tiledb/common/dag/nodes/base.h"
#include "experimental/tiledb/common/dag/ports/ports.h"

#include "experimental/tiledb/common/dag/state_machine/test/helpers.h"

namespace tiledb::common {

/**
 * Producer node.  Constructed with a function that creates Blocks.  A
 * Producer is derived from Source port.
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
   * The function may either take `void` or take a `stop_source` as argument.
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

  /**
   * Function for invoking the function in the `ProducerNode` and sending it to
   * the item mover.  It will also issue `stop` if the `stop_source` is stopped
   * by the enclosed function.
   */
  void run_once() {
    auto state_machine = this->get_mover();
    if (state_machine->is_stopping()) {
      throw std::runtime_error("Trying to stop a stopping producer");
    }

    /**
     * @todo Make inject and do_fill atomic (while properly separating
     * concerns). Atomic on the mover would be the right thing.  There seems
     * to still be the problem that the function will have created the item
     * but is handing it off to the mover.  Need to do inject with an atomic
     * swap so that the state of the function can know whether or not it has
     * handed the item to the mover.
     */

    /*
     * { state = 00 ∧ items = 00 }                  ∨
     * { state = 01 ∧ ( items = 00 ∨ items = 01 ) }
     */
    switch (f_.index()) {
      case 0:
        Base::inject(std::get<0>(f_)());
        break;
      case 1:
        /*
         * f_(stop_source_) may set stop_requested()
         */
        Base::inject(std::get<1>(f_)(stop_source_));
        break;
      default:
        throw std::logic_error("Impossible index for variant");
    }
    if (stop_source_.stop_requested()) {
      if (state_machine->debug_enabled()) {
        std::cout << "run_once stopping" << std::endl;
      }
      state_machine->do_stop();
      return;
    }

    /*
     * { state = 00 ∧ items = 10 }                  ∨
     * { state = 01 ∧ ( items = 10 ∨ items = 11 ) }
     */
    state_machine->do_fill();
    /*
     * { state = 00 ∧ items = 00 }                  ∨
     * { state = 01 ∧ ( items = 00 ∨ items = 01 ) } ∨
     * { state = 10 ∧ items = 10 }                  ∨
     * { state = 11 ∧ ( items = 10 ∨ items = 11 ) }
     */
    state_machine->do_push();

    /*
     * { state = 00 ∧ items = 00 }                  ∨
     * { state = 01 ∧ ( items = 00 ∨ items = 01 ) }
     */

    if (state_machine->debug_enabled())
      std::cout << "producer pushed " << std::endl;
  }

  /**
   * Invoke `run_once` until stopped.
   */
  void run() {
    auto state_machine = this->get_mover();
    if (state_machine->debug_enabled()) {
      std::cout << "producer starting run on " << this->get_mover()
                << std::endl;
    }

    while (!state_machine->is_stopping()) {
      run_once();
    }
    // run_once() will have to have invoked do_stop() to break out of the loop.
  }

  /**
   * Invoke `run_once` a given number of times, or until stopped, whichever
   * comes first.
   *
   *  @param rounds The maximum number of times to invoke the producer function.
   */
  void run_for(size_t rounds) {
    auto state_machine = this->get_mover();

    if (state_machine->debug_enabled()) {
      std::cout << "producer starting "
                << "run_for with " << rounds << " rounds on mover "
                << this->get_mover() << std::endl;
    }

    while (rounds-- && !state_machine->is_stopping()) {  // or stop_requested()?
      run_once();
    }

    if (!state_machine->is_stopping()) {  // or stop_requested()?
      stop_source_.request_stop();
      state_machine->do_stop();
    }
  }

  /**
   * Same as `run_for` but with random delays inserted.  Used for testing and
   * debugging to encourage race conditions and deadlocks.
   */
  void run_for_with_delays(size_t rounds) {
    auto state_machine = this->get_mover();
    if (state_machine->debug_enabled())
      std::cout << this->get_mover() << std::endl;

    while (rounds--) {
      if (state_machine->debug_enabled())
        std::cout << "producer starting " << rounds << std::endl;

      // @todo Do inject() and fill() need to be atomic (use state machine
      // mutex?) Note that there are other places where these are used together,
      // there should probably be just one atomic function.
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
        if (state_machine->debug_enabled()) {
          std::cout << "run_once stopping" << std::endl;
        }
        break;
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

    // Could have fallen through or gotten stop_requested()
    // Either way, need to calls do_stop
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
 *
 * @note We include the proof outline for Sink inline here.
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

#if 0
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
#endif

  /**
   * Function for obtaining items from the item mover and invoking the stored
   * function on each item.
   */
  void run_once() {
    auto state_machine = this->get_mover();

    /*
     * { state = 00 ∧ ( items = 00 ∨ items = 10 ) } ∨
     * { state = 01 ∧ ( items = 01 ∨ items = 11 ) } ∨
     * { state = 10 ∧ items = 10 }                  ∨
     * { state = 11 ∧ items = 11 }
     */
    state_machine->do_pull();
    /*
     * { state = 01 ∧ ( items = 01 ∨ items = 11 ) } ∨
     * { state = 11 ∧ items = 11 ) }
     */

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

    // Returns an optional, may not be necessary given stop state
    // @todo Pass in b as parameter so assignment is atomic
    // @todo Do extract() and drain() need to be atomic (use state machine
    // mutex?)
    auto b = Base::extract();
    /*
     * { state = 01 ∧ ( items = 00 ∨ items = 10 ) } ∨
     * { state = 11 ∧ items = 01 ) }
     */

    if (state_machine->debug_enabled())
      std::cout << "consumer extracted, about to drain " << std::endl;

    state_machine->do_drain();
    /*
     * { state = 00 ∧ ( items = 00 ∨ items = 10 ) } ∨
     * { state = 01 ∧ ( items = 01 ∨ items = 11 ) } ∨
     * { state = 10 ∧ items = 10 }                  ∨
     * { state = 11 ∧ items = 11 ) }
     */

    if (state_machine->debug_enabled())
      std::cout << "consumer drained " << std::endl;

    //  CHECK(b.has_value());

    /**
     * @todo Invoke f_ directly on the `item_` of the `Sink`.  This would remove
     * one layer of buffering (but that could always be addressed by adding
     * another stage to the `Edge`.
     */
    f_(*b);
    // Deallocate b

    if (state_machine->debug_enabled())
      std::cout << "consumer ran function " << std::endl;
  }

  /**
   * Invoke `run_once()` until the node is stopped.
   */
  void run() {
    auto state_machine = this->get_mover();
    if (state_machine->debug_enabled()) {
      std::cout << "consumer starting run on " << this->get_mover()
                << std::endl;
    }

    while (!state_machine->is_done()) {
      run_once();
    }
  }

  /**
   * Invoke `run_once()` a given number of times, or until the node is stopped,
   * whichever comes first.
   *
   * @param rounds The maximum number of times to invoke the producer function.
   */
  void run_for(size_t rounds) {
    auto state_machine = this->get_mover();

    if (state_machine->debug_enabled()) {
      std::cout << "consumer starting "
                << "run_for with " << rounds << " rounds on mover "
                << this->get_mover() << std::endl;
    }

    while (rounds-- && !state_machine->is_done()) {
      run_once();
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

    if (state_machine->debug_enabled())
      std::cout << "consumer starting for " << rounds << " on "
                << this->get_mover() << std::endl;

    while (rounds--) {
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
 * `ProducerNode` part of this class. It is a template template to be composed
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
class FunctionNode : public GraphNode,
                     public Sink<SinkMover_T, BlockIn>,
                     public Source<SourceMover_T, BlockOut> {
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
   * Function for extracting data from the item mover, invoking the function in
   * the `FunctionNode` and sending the result to the item mover. It will also
   * issue `stop` if either its `Sink` portion or its `Source` portion is
   * stopped.
   */
  void run_once() {
    auto source_state_machine = SourceBase::get_mover();
    auto sink_state_machine = SinkBase::get_mover();

    sink_state_machine->do_pull();

    if (sink_state_machine->debug_enabled())
      std::cout << "function pulled "
                << " ( done: " << sink_state_machine->is_done() << " )"
                << std::endl;

    /*
     * The "other side" of the `Sink` state machine is a `Source`, which can be
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

    // @todo Should inject+fill be atomic? (No need.)
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

  /**
   * Same as `run_for`, but with random delays inserted between
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
};  // namespace tiledb::common

}  // namespace tiledb::common
#endif  // TILEDB_DAG_SIMPLE_H
