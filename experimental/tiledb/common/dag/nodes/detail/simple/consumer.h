/**
 * @file   experimental/tiledb/common/dag/nodes/detail/simple/consumer.h
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
 */

#ifndef TILEDB_DAG_NODES_DETAIL_SIMPLE_CONSUMER_H
#define TILEDB_DAG_NODES_DETAIL_SIMPLE_CONSUMER_H

#include <functional>
#include <iostream>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <variant>

#include <thread>  //  @todo debugging

#include "experimental/tiledb/common/dag/nodes/detail/simple/simple_base.h"

namespace tiledb::common {

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

}  // namespace tiledb::common

#endif  // TILEDB_DAG_NODES_DETAIL_SIMPLE_CONSUMER_H
