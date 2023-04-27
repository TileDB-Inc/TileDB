/**
 * @file   experimental/tiledb/common/dag/nodes/detail/simple/function.h
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

#ifndef TILEDB_DAG_NODES_DETAIL_SIMPLE_FUNCTION_H
#define TILEDB_DAG_NODES_DETAIL_SIMPLE_FUNCTION_H

#include <functional>
#include <iostream>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <variant>

#include <thread>  //  @todo debugging

#include "experimental/tiledb/common/dag/nodes/detail/simple/simple_base.h"
#include "experimental/tiledb/common/dag/utility/print_types.h"

namespace tiledb::common {

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
  explicit FunctionNode(Function&& f
#if 0 
      ,
      std::enable_if_t<
          std::is_invocable_r_v<BlockOut, Function, const BlockIn&>,
          void**> = nullptr
#endif
                        )
      : f_{std::forward<Function>(f)} {
    //    print_types(BlockIn{}, BlockOut{});
  }
#if 0
  /**
   * Constructor for anything non-invocable.  It is here to provide more
   * meaningful error messages when the constructor is called with wrong type of
   * arguments.
   */
  template <class Function>
  explicit FunctionNode(
      Function&&,
      std::enable_if_t<
          !std::is_invocable_r_v<BlockOut, Function, const BlockIn&>,
          void**> = nullptr) {
    print_types(BlockIn{});
    print_types(BlockOut{});
    static_assert(
        std::is_invocable_r_v<BlockOut, Function, const BlockIn&>,
        "Function constructor with non-invocable type");
  }
#endif

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
};

}  // namespace tiledb::common

#endif  // TILEDB_DAG_NODES_DETAIL_SIMPLE_FUNCTION_H
