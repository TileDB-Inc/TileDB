/**
 * @file   tuple_maker_node.h
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
 * This file implements a notional stateful node.
 */

#ifndef TILEDB_DAG_NODES_TUPLE_MAKER_NODE_H
#define TILEDB_DAG_NODES_TUPLE_MAKER_NODE_H

#include <atomic>
#include <functional>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>

#include "experimental/tiledb/common/dag/ports/ports.h"
#include "experimental/tiledb/common/dag/state_machine/fsm.h"

#include "experimental/tiledb/common/dag/nodes/node_traits.h"
#include "experimental/tiledb/common/dag/nodes/segmented_nodes.h"
namespace tiledb::common {

template <class T>
struct tuple_maker_state {
  size_t counter_ = 0;
  T t0;
  T t1;
  T t2;
};

template <
    template <class>
    class SinkMover,
    class BlockIn,
    template <class>
    class SourceMover,
    class BlockOut>
class tuple_maker_node_impl : public node_base,
                              public Sink<SinkMover, BlockIn>,
                              public Source<SourceMover, BlockOut> {
  using sink_mover_type = SinkMover<BlockIn>;
  using source_mover_type = SourceMover<BlockOut>;
  using node_base_type = node_base;
  using scheduler_event_type = typename sink_mover_type::scheduler_event_type;

  using SinkBase = Sink<SinkMover, BlockIn>;
  using SourceBase = Source<SourceMover, BlockOut>;

  using state_type = tuple_maker_state<BlockIn>;
  state_type state{0, 0, 0, 0};

 public:
  tuple_maker_node_impl() = default;

 private:
  auto get_sink_mover() const {
    return SinkBase::get_mover();
  }

  auto get_source_mover() const {
    return SourceBase::get_mover();
  }

  BlockIn in_thing{};

 public:
  //  #pragma ide diagnostic ignored "UnreachableCode"

  scheduler_event_type resume() override {
    auto source_mover = SourceBase::get_mover();
    auto sink_mover = SinkBase::get_mover();

    switch (state.counter_) {
      case 0: {
        ++state.counter_;

        auto pull_state = sink_mover->port_pull();

        if (sink_mover->is_done()) {
          return source_mover->port_exhausted();
          break;
        } else {
          if (pull_state == scheduler_event_type::sink_wait) {
            --state.counter_;
          }
          return pull_state;
        }
      }

      case 1: {
        ++state.counter_;
        state.t0 = *(SinkBase::extract());
        return sink_mover->port_drain();
      }

      case 2: {
        ++state.counter_;

        auto pull_state = sink_mover->port_pull();

        if (sink_mover->is_done()) {
          return source_mover->port_exhausted();
          break;
        } else {
          if (pull_state == scheduler_event_type::sink_wait) {
            --state.counter_;
          }
          return pull_state;
        }
      }

      case 3: {
        ++state.counter_;
        state.t1 = *(SinkBase::extract());
        return sink_mover->port_drain();
      }

      case 4: {
        ++state.counter_;

        auto pull_state = sink_mover->port_pull();

        if (sink_mover->is_done()) {
          return source_mover->port_exhausted();
          break;
        } else {
          if (pull_state == scheduler_event_type::sink_wait) {
            --state.counter_;
          }
          return pull_state;
        }
      }

      case 5: {
        ++state.counter_;
        state.t2 = *(SinkBase::extract());
        return sink_mover->port_drain();
      }

      case 6: {
        ++state.counter_;
        SourceBase::inject(std::make_tuple(state.t0, state.t1, state.t2));
        return source_mover->port_fill();
      }

      case 7: {
        ++state.counter_;
        auto push_state = source_mover->port_push();
        if (push_state == scheduler_event_type::source_wait) {
          --state.counter_;
        }
        return push_state;
      }

      case 8: {
        state.counter_ = 0;
        return scheduler_event_type::yield;
      }

      default: {
        break;
      }
    }
    return scheduler_event_type::error;
  }
  //#pragma clang diagnostic pop

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
  }
  void dump_node_state() override {
    std::cout << "tuple_maker_node_impl state: " << state.counter_ << std::endl;
  }
};

/** A function node is a shared pointer to the implementation class */
template <
    template <class>
    class SinkMover,
    class BlockIn,
    template <class>
    class SourceMover,
    class BlockOut>
struct tuple_maker_node
    : public std::shared_ptr<
          tuple_maker_node_impl<SinkMover, BlockIn, SourceMover, BlockOut>> {
  using Base = std::shared_ptr<
      tuple_maker_node_impl<SinkMover, BlockIn, SourceMover, BlockOut>>;
  using Base::Base;

  tuple_maker_node()
      : Base{std::make_shared<tuple_maker_node_impl<
            SinkMover,
            BlockIn,
            SourceMover,
            BlockOut>>()} {
  }
};

}  // namespace tiledb::common

#endif  // TILEDB_DAG_NODES_TUPLE_MAKER_NODE_H
