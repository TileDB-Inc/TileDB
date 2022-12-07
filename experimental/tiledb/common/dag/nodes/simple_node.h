/**
 * @file   simple_node.h
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
 * This file declares "simple" nodes for dag task graph library. Simple nodes
 * are nodes whose enclosed functions are assumed to have no state and whose
 * enclosed function takes one input and produces one output (though the input
 * and output could be tuples).  Furthermore, the enclosed function produces one
 * output for every input. Simple nodes have no capability of maintaining,
 * saving, nor restoring state for the enclosed functions.
 */

#ifndef TILEDB_DAG_SIMPLE_H
#define TILEDB_DAG_SIMPLE_H

#include <functional>
#include <type_traits>

#include "experimental/tiledb/common/dag/execution/jthread/stop_token.hpp"
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
 * will invoke `port_fill`, `port_push`, `inject`, and `port_exhausted`.
 *
 * @tparam Mover_T The type of item mover to be used with the
 * `ProducerNode`. It is a template template to be composed with `Block`.
 *
 * @tparam Block The type of data to be produced by the `ProducerNode` and
 * sent through the item mover.
 *
 * @note We include the two-stage proof outline for Source inline here.
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

  constexpr static inline bool is_source_port_ = true;
  constexpr static inline bool is_sink_port_ = false;

 public:
  using source_port_type = Base;
  using base_port_type = typename Base::base_port_type;

  bool is_source_port() {
    return is_source_port;
  }

  bool is_sink_port() {
    return is_sink_port;
  }

  /**
   * Trivial default constructor, for testing.
   */
  ProducerNode() = default;

  /**
   * Trivial copy constructor, needed to satisfy `is_movable`
   */
  ProducerNode(const ProducerNode&) {
  }

  ProducerNode(ProducerNode&&) noexcept = default;

  /**
   * Constructor
   * @param f A function taking void that creates items.
   *
   * @tparam The type of the function (or function object) that generates items.
   * The function may either take `void` or take a `stop_source` as argument.
   */
  template <class Function>
  explicit ProducerNode(
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
  explicit ProducerNode(
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
  explicit ProducerNode(
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
  void resume() override {
    auto mover = this->get_mover();
    if (mover->is_stopping()) {
      throw std::runtime_error("Trying to stop a stopping producer");
    }

    /**
     * @todo Make inject and port_fill atomic (while properly separating
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
      if (mover->debug_enabled()) {
        std::cout << "resume stopping" << std::endl;
      }
      mover->port_exhausted();
      return;
    }

    /*
     * { state = 00 ∧ items = 10 }                  ∨
     * { state = 01 ∧ ( items = 10 ∨ items = 11 ) }
     */
    mover->port_fill();
    /*
     * { state = 00 ∧ items = 00 }                  ∨
     * { state = 01 ∧ ( items = 00 ∨ items = 01 ) } ∨
     * { state = 10 ∧ items = 10 }                  ∨
     * { state = 11 ∧ ( items = 10 ∨ items = 11 ) }
     */
    mover->port_push();

    /*
     * { state = 00 ∧ items = 00 }                  ∨
     * { state = 01 ∧ ( items = 00 ∨ items = 01 ) }
     */

    if (mover->debug_enabled())
      std::cout << "producer pushed " << std::endl;
  }

  /**
   * Invoke `resume` until stopped.
   */
  void run() override {
    auto mover = this->get_mover();
    if (mover->debug_enabled()) {
      std::cout << "producer starting run on " << this->get_mover()
                << std::endl;
    }

    while (!mover->is_stopping()) {
      resume();
    }
    // resume() will have to have invoked port_exhausted() to break out of the
    // loop.
  }

  /**
   * Invoke `resume` a given number of times, or until stopped, whichever
   * comes first.
   *
   *  @param rounds The maximum number of times to invoke the producer function.
   */
  void run_for(size_t rounds) override {
    auto mover = this->get_mover();

    if (mover->debug_enabled()) {
      std::cout << "producer starting "
                << "run_for with " << rounds << " rounds on mover "
                << this->get_mover() << std::endl;
    }

    while (rounds-- && !mover->is_stopping()) {  // or stop_requested()?
      resume();
    }

    if (!mover->is_stopping()) {  // or stop_requested()?
      stop_source_.request_stop();
      mover->port_exhausted();
    }
  }

  /**
   * Same as `run_for` but with random delays inserted.  Used for testing and
   * debugging to encourage race conditions and deadlocks.
   */
  void run_for_with_delays(size_t rounds) {
    auto mover = this->get_mover();
    if (mover->debug_enabled())
      std::cout << this->get_mover() << std::endl;

    while (rounds--) {
      if (mover->debug_enabled())
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
        if (mover->debug_enabled()) {
          std::cout << "resume stopping" << std::endl;
        }
        break;
      }

      if (mover->debug_enabled())
        std::cout << "producer injected " << rounds << std::endl;

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(555)));

      mover->port_fill();

      if (mover->debug_enabled())
        std::cout << "producer filled " << rounds << std::endl;

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(555)));

      mover->port_push();

      if (mover->debug_enabled())
        std::cout << "producer pushed " << rounds << std::endl;

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(555)));
    }

    // Could have fallen through or gotten stop_requested()
    // Either way, need to call port_exhausted
    if (mover->debug_enabled())
      std::cout << "run stopping" << std::endl;
    mover->port_exhausted();
  }
};

/**
 * Consumer node.  Constructed with a function that accepts Blocks and returns
 * void.
 *
 * @tparam Mover_T The type of item mover to be used by this class.  The class
 * can invoke any of the functions in the mover.  The `ConsumerNode` will invoke
 * `port_pull`, `port_drain`, and `extract`.
 *
 * @param Block The type of data to be obtained from the item mover and
 * consumed.
 *
 * @note We include the two-stage proof outline for Sink inline here.
 */
template <template <class> class Mover_T, class Block>
class ConsumerNode : public GraphNode, public Sink<Mover_T, Block> {
  std::function<void(const Block&)> f_;

  constexpr static inline bool is_source_port_ = false;
  constexpr static inline bool is_sink_port_ = true;

 public:
  using Base = Sink<Mover_T, Block>;

  using sink_port_type = Base;
  using base_port_type = typename Base::base_port_type;

  bool is_sink_port() {
    return is_sink_port_;
  }

  bool is_source_port() {
    return is_source_port_;
  }

 public:
  /**
   * Trivial default constructor, for testing.
   */
  ConsumerNode() = default;

  ConsumerNode(const ConsumerNode&) {
  }

  ConsumerNode(ConsumerNode&&) noexcept = default;

  /**
   * Constructor
   *
   * @tparam Function The type of the function (or function object) that accepts
   * items. May be an actual function, a function object, or the result of
   * `std::bind`.
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
  void resume() override {
    auto mover = this->get_mover();

    /*
     * { state = 00 ∧ ( items = 00 ∨ items = 10 ) } ∨
     * { state = 01 ∧ ( items = 01 ∨ items = 11 ) } ∨
     * { state = 10 ∧ items = 10 }                  ∨
     * { state = 11 ∧ items = 11 }
     */
    mover->port_pull();
    /*
     * { state = 01 ∧ ( items = 01 ∨ items = 11 ) } ∨
     * { state = 11 ∧ items = 11 ) }
     */

    if (mover->debug_enabled())
      std::cout << "consumer pulled "
                << " ( done: " << mover->is_done() << " )" << std::endl;

    if (mover->is_done()) {
      if (mover->debug_enabled())
        std::cout << "consumer done i " << std::endl;
      return;
    }

    if (mover->debug_enabled())
      std::cout << "consumer checked done "
                << " ( done: " << mover->is_done() << " )" << std::endl;

    // Returns an optional, may not be necessary given stop state
    // @todo Pass in b as parameter so assignment is atomic
    // @todo Do extract() and drain() need to be atomic (use state machine
    // mutex?)
    auto b = Base::extract();
    /*
     * { state = 01 ∧ ( items = 00 ∨ items = 10 ) } ∨
     * { state = 11 ∧ items = 01 ) }
     */

    if (mover->debug_enabled())
      std::cout << "consumer extracted, about to drain " << std::endl;

    mover->port_drain();
    /*
     * { state = 00 ∧ ( items = 00 ∨ items = 10 ) } ∨
     * { state = 01 ∧ ( items = 01 ∨ items = 11 ) } ∨
     * { state = 10 ∧ items = 10 }                  ∨
     * { state = 11 ∧ items = 11 ) }
     */

    if (mover->debug_enabled())
      std::cout << "consumer drained " << std::endl;

    if (!b.has_value()) {
      throw(std::runtime_error("ConsumerNode::resume() - no value"));
    }

    /**
     * @todo Invoke f_ directly on the `item_` of the `Sink`.  This would remove
     * one layer of buffering (but that could always be addressed by adding
     * another stage to the `Edge`.
     */
    f_(*b);
    // Deallocate b?

    if (mover->debug_enabled())
      std::cout << "consumer ran function " << std::endl;
  }

  /**
   * Invoke `resume()` until the node is stopped.
   */
  void run() override {
    auto mover = this->get_mover();
    if (mover->debug_enabled()) {
      std::cout << "consumer starting run on " << this->get_mover()
                << std::endl;
    }

    while (!mover->is_done()) {
      resume();
    }
  }

  /**
   * Invoke `resume()` a given number of times, or until the node is stopped,
   * whichever comes first.
   *
   * @param rounds The maximum number of times to invoke the producer function.
   */
  void run_for(size_t rounds) override {
    auto mover = this->get_mover();

    if (mover->debug_enabled()) {
      std::cout << "consumer starting "
                << "run_for with " << rounds << " rounds on mover "
                << this->get_mover() << std::endl;
    }

    while (rounds-- && !mover->is_done()) {
      resume();
    }
    if (!mover->is_done()) {
      mover->port_pull();
    }
  }

  /**
   * Same as the above function but with random delays inserted to encourage
   * race conditions and deadlocks.
   */
  void run_for_with_delays(size_t rounds) {
    auto mover = this->get_mover();

    if (mover->debug_enabled())
      std::cout << "consumer starting for " << rounds << " on "
                << this->get_mover() << std::endl;

    while (rounds--) {
      mover->port_pull();
      if (mover->debug_enabled())
        std::cout << "consumer pulled " << rounds << std::endl;

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(555)));

      if (mover->is_done()) {
        break;
      }

      if (mover->debug_enabled())
        std::cout << "consumer checked done " << rounds << std::endl;

      auto b = Base::extract();

      if (mover->debug_enabled())
        std::cout << "consumer extracted, about to drain " << rounds
                  << std::endl;

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(555)));

      mover->port_drain();

      if (mover->debug_enabled())
        std::cout << "consumer drained " << rounds << std::endl;

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(555)));

      f_(*b);

      if (mover->debug_enabled())
        std::cout << "consumer ran function " << rounds << std::endl;

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(555)));

      if (mover->is_done()) {
        break;
      }
    }
    if (!mover->is_done()) {
      mover->port_pull();
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
 * `Source` part of the `FunctionNode` will invoke `port_fill`, `port_push`,
 * `inject`, and `port_exhausted`.
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

  constexpr static inline bool is_source_port_ = true;
  constexpr static inline bool is_sink_port_ = true;

 public:
  using source_port_type = SourceBase;
  using sink_port_type = SinkBase;
  using base_port_type = typename SinkBase::base_port_type;

  /**
   * Trivial default constructor, for testing.
   */
  FunctionNode() = default;

  FunctionNode(const FunctionNode&) {
  }

  FunctionNode(FunctionNode&&) noexcept = default;

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
  void resume() override {
    auto source_mover = SourceBase::get_mover();
    auto sink_mover = SinkBase::get_mover();

    sink_mover->port_pull();

    if (sink_mover->debug_enabled())
      std::cout << "function pulled "
                << " ( done: " << sink_mover->is_done() << " )" << std::endl;

    /*
     * The "other side" of the `Sink` state machine is a `Source`, which can be
     * stopped.  Similarly, the "other side" of the `Source` could be stopped.
     */
    if (source_mover->is_done() || sink_mover->is_done()) {
      if (sink_mover->debug_enabled())
        std::cout << "function returning i " << std::endl;
      return;
    }

    if (sink_mover->debug_enabled())
      std::cout << "function checked done "
                << " ( done: " << sink_mover->is_done() << " )" << std::endl;

    // @todo as elsewhere, extract+drain should be atomic
    auto b = SinkBase::extract();

    if (sink_mover->debug_enabled())
      std::cout << "function extracted, about to drain " << std::endl;

    sink_mover->port_drain();

    if (sink_mover->debug_enabled())
      std::cout << "function drained " << std::endl;

    if (!b.has_value()) {
      throw(std::runtime_error("FunctionNode::resume() called with no data"));
    }

    auto j = f_(*b);

    if (sink_mover->debug_enabled())
      std::cout << "function ran function " << std::endl;

    // @todo Should inject+fill be atomic? (No need.)
    SourceBase::inject(j);
    if (source_mover->debug_enabled())
      std::cout << "function injected " << std::endl;

    source_mover->port_fill();
    if (source_mover->debug_enabled())
      std::cout << "function filled " << std::endl;

    source_mover->port_push();
    if (source_mover->debug_enabled())
      std::cout << "function pushed " << std::endl;

    if (source_mover->is_done() || sink_mover->is_done()) {
      if (sink_mover->debug_enabled())
        std::cout << "function break ii " << std::endl;
      return;
    }
  }

  /*
   * Function for invoking `resume` multiple times, as given by the input
   * parameter, or until the node is stopped, whichever happens first.
   *
   * @param rounds The maximum number of times to invoke the producer function.
   */
  void run_for(size_t rounds) override {
    auto source_mover = SourceBase::get_mover();
    auto sink_mover = SinkBase::get_mover();

    while (rounds--) {
      if (sink_mover->is_done() || source_mover->is_done()) {
        break;
      }
      resume();
    }
    if (!sink_mover->is_done()) {
      if (sink_mover->debug_enabled())
        std::cout << "function final pull " << rounds << std::endl;
      sink_mover->port_pull();
    }
    source_mover->port_exhausted();
  }

  /**
   * Function for invoking `resume` multiple times, until the node is stopped.
   */
  void run() override {
    auto source_mover = SourceBase::get_mover();
    auto sink_mover = SinkBase::get_mover();

    while (!sink_mover->is_done() && !source_mover->is_done()) {
      resume();
    }
    if (!sink_mover->is_done()) {
      if (sink_mover->debug_enabled())
        std::cout << "function final pull in run()" << std::endl;
      sink_mover->port_pull();
    }
    source_mover->port_exhausted();
  }

  /**
   * Same as `run_for`, but with random delays inserted between
   * operations to expose race conditions and deadlocks.
   */
  void run_for_with_delays(size_t rounds) {
    auto source_mover = SourceBase::get_mover();
    auto sink_mover = SinkBase::get_mover();

    while (rounds--) {
      sink_mover->port_pull();

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(555)));

      if (source_mover->is_done() || sink_mover->is_done()) {
        break;
      }

      CHECK(is_sink_full(sink_mover->state()) == "");
      auto b = SinkBase::extract();

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(555)));

      sink_mover->port_drain();

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(555)));
      if (b.has_value()) {
        auto j = f_(*b);

        SourceBase::inject(j);
        std::this_thread::sleep_for(std::chrono::microseconds(random_us(555)));

        source_mover->port_fill();
        std::this_thread::sleep_for(std::chrono::microseconds(random_us(555)));
        source_mover->port_push();

      } else {
        if (source_mover->debug_enabled())
          std::cout << "No value in function node" << std::endl;
        break;
      }
      if (rounds == 0) {
        sink_mover->port_pull();
      }
      std::this_thread::sleep_for(std::chrono::microseconds(random_us(555)));
    }
    source_mover->port_exhausted();
  }
};  // namespace tiledb::common

}  // namespace tiledb::common
#endif  // TILEDB_DAG_SIMPLE_H
