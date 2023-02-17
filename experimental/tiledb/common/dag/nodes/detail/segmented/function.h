/**
 * @file   experimental/tiledb/common/dag/nodes/detail/function.h
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

#ifndef TILEDB_DAG_NODES_DETAIL_FUNCTION_H
#define TILEDB_DAG_NODES_DETAIL_FUNCTION_H

#include <atomic>
#include <functional>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>

#include "experimental/tiledb/common/dag/ports/ports.h"
#include "experimental/tiledb/common/dag/state_machine/fsm.h"

#include "experimental/tiledb/common/dag/nodes/node_traits.h"
#include "segmented_base.h"
#include "segmented_fwd.h"

namespace tiledb::common {

/**
 * @brief Implementation of function node, a node that transforms data.
 *
 * @tparam SinkMover The mover type for the sink (input) port.
 * @tparam BlockIn The input block type.
 * @tparam SourceMover The mover type for the source port.
 * @tparam BlockOut The type of data emitted by the function node.
 */
template <
    template <class>
    class SinkMover,
    class BlockIn,
    template <class> class SourceMover = SinkMover,
    class BlockOut = BlockIn>
class function_node_impl : public node_base,
                           public Sink<SinkMover, BlockIn>,
                           public Source<SourceMover, BlockOut> {
  using sink_mover_type = SinkMover<BlockIn>;
  using source_mover_type = SourceMover<BlockOut>;
  using node_base_type = node_base;
  using scheduler_event_type = typename sink_mover_type::scheduler_event_type;

  static_assert(std::is_same_v<
                typename sink_mover_type::scheduler_event_type,
                typename source_mover_type::scheduler_event_type>);

  using SinkBase = Sink<SinkMover, BlockIn>;
  using SourceBase = Source<SourceMover, BlockOut>;

  std::function<BlockOut(/*const*/ BlockIn&)> f_;

  std::atomic<size_t> processed_items_{0};

  size_t processed_items() {
    return processed_items_.load();
  }

 public:
  /** Main constructor.  Takes a transform function as argument. */
  template <class Function>
  explicit function_node_impl(
      Function&& f,
      std::enable_if_t<
          (std::is_invocable_r_v<BlockOut, Function, const BlockIn&> ||
           std::is_invocable_r_v<BlockOut, Function, BlockIn&>),
          void**> = nullptr)
      : node_base_type()
      , f_{std::forward<Function>(f)}
      , processed_items_{0} {
  }

  /** Utility functions for indicating what kind of node and state of the ports
   * being used.
   *
   * @todo Are these used anywhere?  This is an abstraction violation, so we
   * should try not to use them.
   * */
  bool is_function_node() const override {
    return true;
  }

  bool is_source_empty() const override {
    auto mover = this->get_source_mover();
    return empty_source(mover->state());
  }

  bool is_sink_full() const override {
    auto mover = this->get_sink_mover();
    return full_sink(mover->state());
  }

  bool is_source_terminating() const override {
    auto mover = this->get_source_mover();
    return terminating(mover->state());
  }

  bool is_sink_terminating() const override {
    auto mover = this->get_sink_mover();
    return terminating(mover->state());
  }
  bool is_source_terminated() const override {
    auto mover = this->get_source_mover();
    return terminated(mover->state());
  }

  bool is_sink_terminated() const override {
    auto mover = this->get_sink_mover();
    return terminated(mover->state());
  }

  bool is_source_done() const override {
    auto mover = this->get_source_mover();
    return done(mover->state());
  }

  bool is_sink_done() const override {
    auto mover = this->get_sink_mover();
    return done(mover->state());
  }

  bool is_sink_state_empty() const override {
    auto mover = this->get_sink_mover();
    return empty_state(mover->state());
  }

  bool is_sink_state_full() const override {
    auto mover = this->get_sink_mover();
    return full_state(mover->state());
  }

  bool is_source_state_empty() const override {
    auto mover = this->get_source_mover();
    return empty_state(mover->state());
  }

  bool is_source_state_full() const override {
    auto mover = this->get_source_mover();
    return full_state(mover->state());
  }
  /**
   * @brief Get the name of the node.
   *
   * @return The name of the node.
   */
  std::string name() const override {
    return {"function"};
  }

  /**
   * @brief Enable debug output for this node.
   */
  void enable_debug() override {
    node_base_type::enable_debug();
    if (SinkBase::item_mover_)
      SinkBase::item_mover_->enable_debug();
    if (SourceBase::item_mover_)
      SourceBase::item_mover_->enable_debug();
  }

 private:
  auto get_sink_mover() const {
    return SinkBase::get_mover();
  }

  auto get_source_mover() const {
    return SourceBase::get_mover();
  }

  void dump_node_state() override {
    auto source_mover = this->get_source_mover();
    auto sink_mover = this->get_sink_mover();
    std::cout << this->name() << " Node state: " << str(sink_mover->state())
              << " -> " << str(source_mover->state()) << std::endl;
  }

  BlockIn in_thing{};  // @todo We should use the port item_
  BlockOut out_thing{};

 public:
  // #pragma clang diagnostic push
  // #pragma ide diagnostic ignored "UnreachableCode"
  /**
   * Resume the node.  This will call the function that produces items.
   * Main entry point of the node.
   *
   * Resume makes one pass through the "function node cycle" and returns /
   * yields. That is, it calls `pull` to get a data item from the port, calls
   * `drain`, applies the enclosed function to create a data item, puts it into
   * the port, invokes `fill` and then invokes `push`.
   *
   * Implements a Duff's device emulation of a coroutine.  The current state of
   * function execution is stored in the program counter.  A switch statement is
   * used to jump to the current program counter location.
   */
  scheduler_event_type resume() override {
    auto source_mover = SourceBase::get_mover();
    auto sink_mover = SinkBase::get_mover();

    // #ifdef __clang__
    // #pragma clang diagnostic push
    // #pragma ide diagnostic ignored "UnreachableCode"
    // #endif

    switch (this->program_counter_) {
      // pull / extract drain
      case 0: {
        ++this->program_counter_;

        auto pull_state = sink_mover->port_pull();

        if (sink_mover->is_done()) {
          return source_mover->port_exhausted();
          break;
        } else {
          if (pull_state == scheduler_event_type::sink_wait) {
            this->decrement_program_counter();
          }
          return pull_state;
        }

// Is this needed?  It seems like it was just for debugging.
#if 0
auto pre_state = sink_mover->state();
auto post_state = sink_mover->state();

        if constexpr (std::is_same_v<decltype(post_state), two_stage>) {
          if (pre_state == two_stage::st_00 && post_state == two_stage::xt_00) {
            throw std::runtime_error("consumer got stuck in xt_00 state");
          }
        } else {
          if (pre_state == three_stage::st_000 &&
              post_state == three_stage::xt_000) {
            throw std::runtime_error("consumer got stuck in xt_000 state");
          }
        }
#endif
      }
        [[fallthrough]];

      case 1: {
        ++this->program_counter_;

        in_thing = *(SinkBase::extract());
      }
        [[fallthrough]];

      case 2: {
        ++this->program_counter_;

        return sink_mover->port_drain();
      }
        [[fallthrough]];

      case 3: {
        ++this->program_counter_;

        // assert(this->source_correspondent() != nullptr);
        // assert(this->sink_correspondent() != nullptr);
      }
        [[fallthrough]];

      case 4: {
        ++this->program_counter_;

        out_thing = f_(in_thing);
      }
        [[fallthrough]];

        // inject / fill / push

      case 5: {
        ++this->program_counter_;
        SourceBase::inject(out_thing);
      }
        [[fallthrough]];

      case 6: {
        ++this->program_counter_;
        return source_mover->port_fill();
      }
        [[fallthrough]];

      case 7: {
        ++this->program_counter_;
      }
        [[fallthrough]];

      case 8: {
        ++this->program_counter_;
        auto push_state = source_mover->port_push();
        if (push_state == scheduler_event_type::source_wait) {
          this->decrement_program_counter();
        }
        return push_state;
      }
        [[fallthrough]];

        // @todo Should skip yield if push waited;
      case 9: {
        this->program_counter_ = 0;
        // return this->task_yield(*this);
        return scheduler_event_type::yield;
      }
        [[fallthrough]];

      default: {
        break;
      }
    }

    // #ifdef __clang__
    // #pragma clang diagnostic pop
    // #endif

    return scheduler_event_type::error;
  }
  // #pragma clang diagnostic pop

  /** Run the node until it is done. */
  void run() override {
    auto source_mover = SourceBase::get_mover();
    auto sink_mover = SinkBase::get_mover();

    while (!sink_mover->is_done() && !source_mover->is_done()) {
      resume();
    }
    if (!sink_mover->is_done()) {
      sink_mover->port_pull();
    }
    // @todo ?? port_exhausted is called in resume -- should it be called here
    // instead? source_mover->port_exhausted();
  }
};

/** A function node is a shared pointer to the implementation class */
template <
    template <class>
    class SinkMover,
    class BlockIn,
    template <class> class SourceMover,
    class BlockOut>
struct function_node
    : public std::shared_ptr<
          function_node_impl<SinkMover, BlockIn, SourceMover, BlockOut>> {
  using Base = std::shared_ptr<
      function_node_impl<SinkMover, BlockIn, SourceMover, BlockOut>>;
  using Base::Base;

  template <class Function>
  explicit function_node(Function&& f)
      : Base{std::make_shared<
            function_node_impl<SinkMover, BlockIn, SourceMover, BlockOut>>(
            std::forward<Function>(f))} {
  }

  explicit function_node(
      function_node_impl<SinkMover, BlockIn, SourceMover, BlockOut>& impl)
      : Base{std::make_shared<
            function_node_impl<SinkMover, BlockIn, SourceMover, BlockOut>>(
            std::move(impl))} {
  }
};

}  // namespace tiledb::common
#endif  // TILEDB_DAG_NODES_DETAIL_FUNCTION_H
