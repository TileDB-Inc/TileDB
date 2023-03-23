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

  std::function<void(/*const*/ T&)> f_;

 public:
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

  consumer_node_impl(consumer_node_impl&&) noexcept = default;

 private:
  void set_item_mover(std::shared_ptr<mover_type> mover) {
    this->item_mover_ = mover;
  }

  auto get_sink_mover() const {
    return this->get_mover();
  }

  auto get_input_port() {
    return *reinterpret_cast<SinkBase*>(this);
  }

 public:
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

  T thing{};  // @todo We should use the port item_

  scheduler_event_type resume() override {
    auto mover = this->get_mover();

    switch (this->program_counter_) {
      case 0: {
        ++this->program_counter_;

        auto pull_state = mover->port_pull();

        if (mover->is_done()) {
          this->program_counter_ = 999;
          return mover->port_exhausted();
        } else {
          if (pull_state == scheduler_event_type::sink_wait) {
            this->decrement_program_counter();
          }
          return pull_state;
        }
      }

      case 1: {
        ++this->program_counter_;
        thing = *(SinkBase::extract());
        return mover->port_drain();
      }

      case 2: {
        ++this->program_counter_;
        f_(thing);
        ++consumed_items_;
      }
        [[fallthrough]];

      // @todo Where is the best place to yield?
      case 3: {
        this->program_counter_ = 0;
        return scheduler_event_type::yield;
      }


      case 999: {
        return scheduler_event_type::done;
      }
      default: {
        break;
      }
    }

    return scheduler_event_type::error;
  }

  /** Execute `resume` in a loop until the node is done. */
  void run() override {
    auto mover = this->get_mover();

    while (!mover->is_done()) {
      resume();
    }
  }

  /****************************************************************
   *
   * Debugging and testing support
   *
   ****************************************************************/

  std::string name() const override {
    return {"consumer"};
  }

  std::atomic<size_t> consumed_items_{0};

  size_t consumed_items() {
    return consumed_items_.load();
  }

  void enable_debug() override {
    node_base_type::enable_debug();
    if (this->item_mover_)
      this->item_mover_->enable_debug();
  }

  void dump_node_state() override {
    auto mover = this->get_mover();
    std::cout << this->name() << " Node state: " << str(mover->state())
              << std::endl;
  }
};

/** A consumer node is a shared pointer to the implementation class */
template <template <class> class Mover, class T>
struct consumer_node : public std::shared_ptr<consumer_node_impl<Mover, T>> {
  using PreBase = consumer_node_impl<Mover, T>;
  using Base = std::shared_ptr<PreBase>;
  using Base::Base;

  template <class Function>
  explicit consumer_node(Function&& f)
      : Base{std::make_shared<PreBase>(std::forward<Function>(f))} {
  }

  explicit consumer_node(PreBase& impl)
      : Base{std::make_shared<PreBase>(std::move(impl))} {
  }
};

}  // namespace tiledb::common
#endif  // TILEDB_DAG_NODES_DETAIL_CONSUMER_H
