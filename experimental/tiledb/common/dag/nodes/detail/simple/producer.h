/**
 * @file   experimental/tiledb/common/dag/nodes/detail/simple/producer.h
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

#ifndef TILEDB_DAG_NODES_DETAIL_SIMPLE_PRODUCER_H
#define TILEDB_DAG_NODES_DETAIL_SIMPLE_PRODUCER_H

#include <functional>
#include <iostream>
#include <stdexcept>
#include <stop_token>
#include <type_traits>
#include <utility>
#include <variant>

#include <thread>  //  @todo debugging

#include "experimental/tiledb/common/dag/nodes/detail/simple/simple_base.h"

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
}  // namespace tiledb::common

#endif  // TILEDB_DAG_NODES_DETAIL_SIMPLE_PRODUCER_H
