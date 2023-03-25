/**
 * @file   experimental/tiledb/common/dag/nodes/detail/producer.h
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

#ifndef TILEDB_DAG_NODES_DETAIL_PRODUCER_H
#define TILEDB_DAG_NODES_DETAIL_PRODUCER_H

#include <atomic>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include "experimental/tiledb/common/dag/execution/jthread/stop_token.hpp"

#include "experimental/tiledb/common/dag/ports/ports.h"
#include "experimental/tiledb/common/dag/state_machine/fsm.h"

#include "experimental/tiledb/common/dag/nodes/node_traits.h"
#include "segmented_base.h"
#include "segmented_fwd.h"

namespace tiledb::common {

/**
 * @brief Implementation of a segmented producer node.
 * @tparam Mover The type of the data item mover.
 * @tparam T The type of the data item.
 *
 * @todo Simplify API by removing the need for the user to specify the mover.
 */
template <template <class> class Mover, class T>
struct producer_node_impl : public node_base, public Source<Mover, T> {
  using SourceBase = Source<Mover, T>;
  using mover_type = Mover<T>;
  using node_base_type = node_base;

  std::function<T(std::stop_source&)> f_;

  /**
   * @brief Constructor, takes a function that produces items.
   * @tparam Function type
   * @param f The function that produces items.
   *
   */
  template <class Function>
  explicit producer_node_impl(
      Function&& f,
      std::enable_if_t<
          std::is_invocable_r_v<T, Function, std::stop_source&>,
          void**> = nullptr)
      : node_base_type()
      , f_{std::forward<Function>(f)}
      , produced_items_{0} {
  }

  producer_node_impl(producer_node_impl&&) noexcept = default;

 private:
  /**
   * @brief Set the item mover for this node.
   * @param mover The item mover.
   */
  void set_item_mover(std::shared_ptr<mover_type> mover) {
    this->item_mover_ = mover;
  }

  auto get_source_mover() const {
    return this->get_mover();
  }

  auto get_output_port() {
    return *reinterpret_cast<SourceBase*>(this);
  }

 public:
  /**
   * Resume the node.  This will call the function that produces items.
   * The function is passed a stop_source that can be used to terminate the
   * node. Main entry point of the node.
   *
   * Resume makes one pass through the "producer node cycle" and returns /
   * yields. That is, it creates a data item, puts it into the port, invokes
   * `fill` and then invokes `push`.
   *
   * Implements a Duff's device emulation of a coroutine.  The current state of
   * function execution is stored in the program counter.  A switch statement is
   * used to jump to the current program counter location.
   */
  scheduler_event_type resume() override {
    auto mover = this->get_mover();

    std::stop_source st;
    decltype(f_(st)) thing{};  // @todo We should use the port item_

    std::stop_source stop_source_;
    assert(!stop_source_.stop_requested());

    switch (this->program_counter_) {
      case 0: {
        ++this->program_counter_;

        thing = f_(stop_source_);

        if (stop_source_.stop_requested()) {
          this->program_counter_ = 999;
          return mover->port_exhausted();
        }
        ++produced_items_;
      }
        [[fallthrough]];

      case 1: {
        ++this->program_counter_;
        SourceBase::inject(thing);
        return mover->port_fill();
      }

      case 2: {
        ++this->program_counter_;
        auto push_state = mover->port_push();
        if (push_state == scheduler_event_type::source_wait) {
          this->decrement_program_counter();
        }
        return push_state;
      }

        // @todo Should skip yield if push waited, because the wait is already
        //   a yield.
      case 3: {
        this->program_counter_ = 0;
        return scheduler_event_type::yield;
      }

      case 999: {
        return scheduler_event_type::done;
      }

      default:
        break;
    }

    return scheduler_event_type::error;
  }

  /** Execute `resume` in a loop until the node is done. */
  void run() override {
    auto mover = this->get_mover();

    while (!mover->is_stopping()) {
      resume();
    }
  }

  /****************************************************************
   *
   * Debugging and testing support
   *
   ****************************************************************/

  /** Return name of node. */
  std::string name() const override {
    return {"producer"};
  }

  /**
   * Counter to keep track of how many times the producer has been resumed.
   */
  std::atomic<size_t> produced_items_{0};

  /**
   * @brief Return the number of items produced by this node.
   * @return The number of items produced by this node.
   */
  size_t produced_items() {
    return produced_items_.load();
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

/** A producer node is a shared pointer to the implementation class */
template <template <class> class Mover, class T>
struct producer_node : public std::shared_ptr<producer_node_impl<Mover, T>> {
  using PreBase = producer_node_impl<Mover, T>;
  using Base = std::shared_ptr<PreBase>;
  using Base::Base;

  using node_type = node_t<producer_node_impl<Mover, T>>;
  using node_handle_type = node_handle_t<producer_node_impl<Mover, T>>;

  template <class Function>
  explicit producer_node(Function&& f)
      : Base{std::make_shared<PreBase>(std::forward<Function>(f))} {
  }

  explicit producer_node(PreBase& impl)
      : Base{std::make_shared<PreBase>(std::move(impl))} {
  }
};

}  // namespace tiledb::common
#endif  // TILEDB_DAG_NODES_DETAIL_PRODUCER_H
