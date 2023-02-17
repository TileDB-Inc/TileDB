/**
 * @file   experimental/tiledb/common/dag/nodes/detail/consumer.h
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

#ifndef TILEDB_DAG_NODES_DETAIL_CONSUMER_H
#define TILEDB_DAG_NODES_DETAIL_CONSUMER_H

#include <atomic>
#include <functional>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>

#include "experimental/tiledb/common/dag/nodes/node_traits.h"
#include "segmented_base.h"
#include "segmented_fwd.h"

#include "experimental/tiledb/common/dag/ports/ports.h"
#include "experimental/tiledb/common/dag/state_machine/fsm.h"

namespace tiledb::common {

/**
 * @brief Implementation of a segmented consumer node.
 * @tparam Mover The item mover type.
 * @tparam T The item type.
 */
template <template <class> class Mover, class T>
class consumer_node_impl : public node_base, public Sink<Mover, T> {
  using SinkBase = Sink<Mover, T>;
  using mover_type = Mover<T>;
  using node_base_type = node_base;
  using scheduler_event_type = typename mover_type::scheduler_event_type;

  std::function<void(/*const*/ T&)> f_;

  std::atomic<size_t> consumed_items_{0};

  void set_item_mover(std::shared_ptr<mover_type> mover) {
    this->item_mover_ = mover;
  }

 public:
  size_t consumed_items() {
    return consumed_items_.load();
  }

  /** Main constructor. Takes a consumer function as argument. */
  template <class Function>
  explicit consumer_node_impl(
      Function&& f,
      std::enable_if_t<
          (std::is_invocable_r_v<void, Function, const T&> ||
           std::is_invocable_r_v<void, Function, T&>),
          void**> = nullptr)
      : node_base_type()
      , f_{std::forward<Function>(f)}
      , consumed_items_{0} {
  }

  auto get_input_port() {
    return *reinterpret_cast<SinkBase*>(this);
  }


  /** Utility functions for indicating what kind of node and state of the ports
   * being used.
   *
   * @todo Are these used anywhere?  This is an abstraction violation, so we
   * should try not to use them.
   * */
  bool is_consumer_node() const override {
    return true;
  }

  bool is_source_empty() const override {
    auto mover = this->get_mover();
    return empty_source(mover->state());
  }

  bool is_sink_full() const override {
    auto mover = this->get_mover();
    return full_sink(mover->state());
  }

  bool is_sink_state_empty() const override {
    auto mover = this->get_mover();
    return empty_state(mover->state());
  }

  bool is_sink_state_full() const override {
    auto mover = this->get_mover();
    return full_state(mover->state());
  }

  bool is_source_state_empty() const override {
    auto mover = this->get_mover();
    return empty_state(mover->state());
  }

  bool is_source_state_full() const override {
    auto mover = this->get_mover();
    return full_state(mover->state());
  }

  bool is_source_terminating() const override {
    auto mover = this->get_mover();
    return terminating(mover->state());
  }

  bool is_sink_terminating() const override {
    auto mover = this->get_mover();
    return terminating(mover->state());
  }

  bool is_source_terminated() const override {
    auto mover = this->get_mover();
    return terminated(mover->state());
  }

  bool is_sink_terminated() const override {
    auto mover = this->get_mover();
    return terminated(mover->state());
  }

  bool is_source_done() const override {
    auto mover = this->get_mover();
    return done(mover->state());
  }

  bool is_sink_done() const override {
    auto mover = this->get_mover();
    return done(mover->state());
  }

  std::string name() const override {
    return {"consumer"};
  }

  void enable_debug() override {
    node_base_type::enable_debug();
    if (this->item_mover_)
      this->item_mover_->enable_debug();
  }

  auto get_sink_mover() const {
    return this->get_mover();
  }

  void dump_node_state() override {
    auto mover = this->get_mover();
    std::cout << this->name() << " Node state: " << str(mover->state())
              << std::endl;
  }

  T thing{};  // @todo We should use the port item_

  /**
   * Resume the node.  This will call the function that consumes items.
   * Main entry point of the node.
   *
   * Resume makes one pass through the "consumer node cycle" and returns /
   * yields. That is, it pulls a data item, extracts it from the port, invokes
   * `drain` and then calls the enclosed function on the item.
   *
   * Implements a Duff's device emulation of a coroutine.  The current state of
   * function execution is stored in the program counter.  A switch statement is
   * used to jump to the current program counter location.
   */
  scheduler_event_type resume() override {
    auto mover = SinkBase::get_mover();

    // #ifdef __clang__
    // #pragma clang diagnostic push
    // #pragma ide diagnostic ignored "UnreachableCode"
    // #endif

    switch (this->program_counter_) {
      /*
       * @todo: don't do this -- case 0 shold be executed on all calls
       * case 0 is executed only on the very first call to resume.
       */
      case 0: {
        ++this->program_counter_;

        auto pull_state = mover->port_pull();

        if (mover->is_done()) {
          return mover->port_exhausted();
          break;
        } else {
          if (pull_state == scheduler_event_type::sink_wait) {
            this->decrement_program_counter();
          }
          return pull_state;
        }
      }

        [[fallthrough]];

        /*
         * To make the flow here similar to producer, we start with pull() the
         * first time we are called but thereafter the loop goes from 1 to 5;
         */
      case 1: {
        ++this->program_counter_;

        thing = *(SinkBase::extract());
      }
        [[fallthrough]];

      case 2: {
        ++this->program_counter_;

        return mover->port_drain();
      }
        [[fallthrough]];

      case 3: {
        ++this->program_counter_;

        assert(this->source_correspondent() != nullptr);
      }
        [[fallthrough]];

      case 4: {
        ++this->program_counter_;

        f_(thing);
        ++consumed_items_;
      }
        [[fallthrough]];

#if 0
        // @todo Should skip yield if pull waited;
      case 5: {
        ++this->program_counter_;

        auto pull_state = mover->port_pull();

        if (mover->is_done()) {
          return mover->port_exhausted();
          break;
        } else {
          if (pull_state == scheduler_event_type::sink_wait) {
            this->decrement_program_counter();
          }
          return pull_state;
        }
      }

        [[fallthrough]];
#endif

      // @todo Where is the best place to yield?
      case 5: {
        this->program_counter_ = 0;
        return scheduler_event_type::yield;
      }

      default: {
        break;
      }
    }

    // #ifdef __clang__
    // #pragma clang diagnostic pop
    // #endif

    return scheduler_event_type::error;
  }

  /** Execute `resume` in a loop until the node is done. */
  void run() override {
    auto mover = this->get_mover();

    while (!mover->is_done()) {
      resume();
    }
  }
};

/** A consumer node is a shared pointer to the implementation class */
template <template <class> class Mover, class T>
struct consumer_node : public std::shared_ptr<consumer_node_impl<Mover, T>> {
  using Base = std::shared_ptr<consumer_node_impl<Mover, T>>;
  using Base::Base;

  template <class Function>
  explicit consumer_node(Function&& f)
      : Base{std::make_shared<consumer_node_impl<Mover, T>>(
            std::forward<Function>(f))} {
  }

  explicit consumer_node(consumer_node_impl<Mover, T>& impl)
      : Base{std::make_shared<consumer_node_impl<Mover, T>>(std::move(impl))} {
  }
};

}  // namespace tiledb::common
#endif  // TILEDB_DAG_NODES_DETAIL_CONSUMER_H
