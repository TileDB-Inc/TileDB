/**
 * @file   segmented_nodes.h
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
 * This file defines segmented execution nodes for dag.
 */

#include "experimental/tiledb/common/dag/execution/jthread/stop_token.hpp"
#include "experimental/tiledb/common/dag/ports/ports.h"
#include <atomic>
#include <iostream>
#include <memory>

#include "experimental/tiledb/common/dag/utils/print_types.h"

namespace tiledb::common {

/*
 * Forward declarations
 */
template<template<class> class Mover, class T>
struct producer_node_impl;
template<template<class> class Mover, class T>
struct consumer_node_impl;
template<template<class> class SinkMover, class BlockIn,
        template<class> class SourceMover, class BlockOut>
struct function_node_impl;

/**
 * Base class for all segmented nodes.
 */
struct node_base {
  using node_type = std::shared_ptr<node_base>;

  bool debug_{false};

  size_t id_{0UL};
  size_t program_counter_{0};

  node_type sink_correspondent_{nullptr};
  node_type source_correspondent_{nullptr};

  virtual node_type &sink_correspondent() {
    return sink_correspondent_;
  }

  virtual node_type &source_correspondent() {
    return source_correspondent_;
  }

  node_base(node_base &&) = default;

  node_base(const node_base &) {
    std::cout << "Nonsense copy constructor" << std::endl;
  }

  virtual ~node_base() = default;

  [[nodiscard]] inline size_t id() const {
    return id_;
  }

  inline size_t &id() {
    return id_;
  }

  explicit node_base(size_t id) : id_{id} {
  }

  virtual void resume() = 0;

  virtual void run() = 0;

  void decrement_program_counter() {
    assert(program_counter_ > 0);
    --program_counter_;
  }

  virtual std::string name() {
    return {"abstract base"};
  }

  virtual void enable_debug() {
    debug_ = true;
  }

  void disable_debug() {
    debug_ = false;
  }

  [[nodiscard]] bool debug() const {
    return debug_;
  }
};

/**
 * A node is a shared pointer to a node_base.
 */
using node = std::shared_ptr<node_base>;


/**
 * Connect two nodes.
 * @tparam From Source node type.
 * @tparam To Sink node type.
 * @param from Source node.
 * @param to Sink node.
 */
template<class From, class To>
void connect(From &from, To &to) {
  (*from).sink_correspondent() = to;
  (*to).source_correspondent() = from;
}

/**
 * An atomic counter used to assign unique ids to nodes.
 */
std::atomic<size_t> id_counter{0};

/**
 * @brief Implementation of a segmented producer node.
 * @tparam Mover
 * @tparam T
 *
 * @todo Simplify API by removing the need for the user to specify the mover.
 */
template<template<class> class Mover, class T>
struct producer_node_impl : public node_base, public Source<Mover, T> {
  using SourceBase = Source<Mover, T>;

  using mover_type = Mover<T>;
  using node_base_type = node_base;

  std::function<T(std::stop_source &)> f_;

  /**
   * Counter to keep track of how many times the producer has been resumed.
   */
  std::atomic<size_t> produced_items_{0};

  /**
   * @brief Return the number of items produced by this node.
   * @return The number of items produced by this node.
   */
  size_t produced_items() {
    if (this->debug())
      std::cout << std::to_string(produced_items_.load()) << " produced items in produced_items()\n";
    return produced_items_.load();
  }

  /**
 * @brief Set the item mover for this node.
 * @param mover The item mover.
 */
  void set_item_mover(std::shared_ptr<mover_type> mover) {
    this->item_mover_ = mover;
  }

/**
 * @brief Constructor, takes a function that produces items.
 * @tparam Function type
 * @param f The function that produces items.
 *
 */
  template<class Function>
  explicit producer_node_impl(Function &&f,
                              std::enable_if_t<std::is_invocable_r_v<T, Function, std::stop_source &>, void **> = nullptr)
          : node_base_type(id_counter++), f_{std::forward<Function>(f)}, produced_items_{0} {
  }

  producer_node_impl(producer_node_impl &&rhs) noexcept = default;

  std::string name() override {
    return {"producer"};
  }

  void enable_debug() override {
    node_base_type::enable_debug();
    if (this->item_mover_)
      this->item_mover_->enable_debug();
  }

  void resume() override {
    auto mover = this->get_mover();

    if (this->debug()) {
      std::cout << "producer resuming\n";

      std::cout << this->name() + " node " + std::to_string(this->id()) + " resuming with " +
                   std::to_string(produced_items_) + " produced_items" + "\n";
    }

    [[maybe_unused]] std::thread::id this_id = std::this_thread::get_id();

    std::stop_source st;
    decltype(f_(st)) thing{};

    std::stop_source stop_source_;
    assert(!stop_source_.stop_requested());

    switch (this->program_counter_) {
      case 0: {
        ++this->program_counter_;

#if 0
        if (produced_items_ >= problem_size) {
          if (this->debug())
            std::cout << this->name() + " node " + std::to_string(this->id()) +
                             " has produced enough items -- calling "
                             "port_exhausted with " +
                             std::to_string(produced_items_) +
                             " produced items and " +
                             std::to_string(problem_size) + " problem size\n";
          mover->port_exhausted();
          break;
        }
#endif

        thing = f_(stop_source_);

        if (this->debug())
          std::cout << "producer thing is " + std::to_string(thing) + "\n";

        if (stop_source_.stop_requested()) {
          if (this->debug())
            std::cout << this->name() + " node " + std::to_string(this->id()) +
                         " has gotten stop -- calling port_exhausted with " + std::to_string(produced_items_) +
                         " produced items\n";

          mover->port_exhausted();
          break;
        }
        ++produced_items_;
      }

        [[fallthrough]];

      case 1: {
        ++this->program_counter_;

        SourceBase::inject(thing);
      }
        [[fallthrough]];

      case 2: {
        this->program_counter_ = 3;
        mover->port_fill();
      }
        [[fallthrough]];

      case 3: {
        this->program_counter_ = 4;
      }
        [[fallthrough]];

      case 4: {
        this->program_counter_ = 5;
        mover->port_push();
      }
        [[fallthrough]];

        // @todo Should skip yield if push waited;
      case 5: {
        this->program_counter_ = 0;
        // this->task_yield(*this);
        break;
      }

      default:
        break;
    }
  }

  void run() override {
    auto mover = this->get_mover();
    if (mover->debug_enabled()) {
      std::cout << "producer starting run on " << this->get_mover()
                << std::endl;
    }
    while (!mover->is_stopping()) {
      resume();
    }
  }
};


/**
 * @brief Implementation of a segmented consumer node.
 * @tparam Mover The item mover type.
 * @tparam T The item type.
 */
template<template<class> class Mover, class T>
struct consumer_node_impl : public node_base, public Sink<Mover, T> {
  using SinkBase = Sink<Mover, T>;
  using mover_type = Mover<T>;
  using node_base_type = node_base;

  std::function<void(const T &)> f_;

  std::atomic<size_t> consumed_items_{0};

  void set_item_mover(std::shared_ptr<mover_type> mover) {
    this->item_mover_ = mover;
  }

  size_t consumed_items() {
    return consumed_items_.load();
  }

  template<class Function>
  explicit consumer_node_impl(Function &&f,
                              std::enable_if_t<std::is_invocable_r_v<void, Function, const T &>, void **> = nullptr)
          : node_base_type(id_counter++), f_{std::forward<Function>(f)}, consumed_items_{0} {
  }

  std::string name() override {
    return {"consumer"};
  }

  void enable_debug() override {
    node_base_type::enable_debug();
    if (this->item_mover_)
      this->item_mover_->enable_debug();
  }

  T thing{};

  void resume() override {
    auto mover = SinkBase::get_mover();

    if (this->debug())
      std::cout << this->name() + " node " + std::to_string(this->id()) +
                       " resuming with " + std::to_string(consumed_items_) +
                       " consumed_items" + "\n";

    [[maybe_unused]] std::thread::id this_id = std::this_thread::get_id();

    if (mover->is_done()) {
      if (this->debug())
        std::cout << this->name() + " node " + std::to_string(this->id()) +
                         " got mover done in consumer at top of resume -- "
                         "returning\n";

      mover->port_exhausted();

      return;
    }

    switch (this->program_counter_) {
      /*
       * case 0 is executed only on the very first call to resume.
       */
      case 0: {
        ++this->program_counter_;
        mover->port_pull();

        if (is_done(mover->state()) == "") {
          if (this->debug()) {
            std::cout << "=== sink mover done\n";
          }
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

        mover->port_drain();
      }
        [[fallthrough]];

      case 3: {
        ++this->program_counter_;

        assert(this->source_correspondent_ != nullptr);
      }
        [[fallthrough]];

      case 4: {
        ++this->program_counter_;

#if 0
        if (consumed_items_++ >= problem_size) {
          //                    if (this->debug())
          std::cout << "THIS SHOULD NOT HAPPEN " + this->name() + " node " +
                           std::to_string(this->id()) +
                           " has produced enough items -- calling "
                           "port_exhausted with " +
                           std::to_string(consumed_items_) +
                           " consumed items and " +
                           std::to_string(problem_size) + " problem size\n";
          //          assert(false);

          mover->port_exhausted();
          break;
        }
#endif
        f_(thing);
        ++consumed_items_;
      }

        // @todo Should skip yield if pull waited;
      case 5: {
        ++this->program_counter_;

        mover->port_pull();
      }

      case 6: {
        this->program_counter_ = 1;
        // this->task_yield(*this); ??
        break;
      }
      default: {
        break;
      }
    }
  }

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
};

/**
 * @brief Implementation of function node, a node that transforms data.
 *
 * @tparam SinkMover The mover type for the sink (input) port.
 * @tparam BlockIn The input block type.
 * @tparam SourceMover The mover type for the source port.
 * @tparam BlockOut The type of data emitted by the function node.
 */
template<template<class> class SinkMover, class BlockIn,
        template<class> class SourceMover = SinkMover, class BlockOut = BlockIn>
struct function_node_impl : public node_base, public Sink<SinkMover, BlockIn>, public Source<SourceMover, BlockOut> {
  using sink_mover_type = SinkMover<BlockIn>;
  using source_mover_type = SourceMover<BlockOut>;
  using node_base_type = node_base;

  using SinkBase = Sink<SinkMover, BlockIn>;
  using SourceBase = Source<SourceMover, BlockOut>;

  std::function<BlockOut(const BlockIn &)> f_;

  std::atomic<size_t> processed_items_{0};

  size_t processed_items() {
    return processed_items_.load();
  }

  template<class Function>
  explicit function_node_impl(Function &&f,
                              std::enable_if_t<std::is_invocable_r_v<BlockOut, Function, const BlockIn &>, void **> = nullptr)
          : node_base_type(id_counter++), f_{std::forward<Function>(f)}, processed_items_{0} {
  }

  /**
   * @brief Get the name of the node.
   *
   * @return The name of the node.
   */
  std::string name() override {
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

  BlockIn in_thing{};
  BlockOut out_thing{};

  /**
   * @brief Resume executing the node.
   *
   * This is the main function of the node. It is called by the scheduler to execute the enclosed function one time.
   */
  void resume() override {
    auto source_mover = SourceBase::get_mover();
    auto sink_mover = SinkBase::get_mover();

    if (this->debug())
      std::cout << this->name() + " node " + std::to_string(this->id()) + " resuming at program counter = " +
                   std::to_string(this->program_counter_) + " and " + std::to_string(processed_items_) +
                   " consumed_items\n";

    [[maybe_unused]] std::thread::id this_id = std::this_thread::get_id();

    if (source_mover->is_done() || sink_mover->is_done()) {
      if (this->debug())
        std::cout << this->name() + " node " + std::to_string(this->id()) +
                     " got sink_mover done at top of resumes -- returning\n";

      source_mover->port_exhausted();

      return;
    }

    switch (this->program_counter_) {

      // pull / extract drain
      case 0: {
        ++this->program_counter_;
        sink_mover->port_pull();

        if (source_mover->is_done() || sink_mover->is_done()) {
          if (this->debug())
            std::cout << this->name() + " node " + std::to_string(this->id()) +
                         " got sink_mover done -- going to exhaust source\n";

          source_mover->port_exhausted();
          break;
        }
      }
        [[fallthrough]];

      case 1: {
        ++this->program_counter_;

        in_thing = *(SinkBase::extract());

        if (this->debug())
          std::cout << "function in_thing is " + std::to_string(in_thing) + "\n";
      }
        [[fallthrough]];

      case 2: {
        ++this->program_counter_;

        sink_mover->port_drain();
      }
        [[fallthrough]];

      case 3: {
        ++this->program_counter_;

        assert(this->source_correspondent() != nullptr);
        assert(this->sink_correspondent() != nullptr);
      }
        [[fallthrough]];

      case 4: {
        ++this->program_counter_;

#if 0
        if (processed_items_++ >= problem_size) {
          //                    if (this->debug())
          std::cout << "THIS SHOULD NOT HAPPEN " + this->name() + " node " +
                           std::to_string(this->id()) +
                           " has produced enough items -- calling "
                           "port_exhausted with " +
                           std::to_string(processed_items_) +
                           " consumed items and " +
                           std::to_string(problem_size) + " problem size\n";
          //          assert(false);

          sink_mover->port_exhausted();
          break;
        }
#endif

        out_thing = f_(in_thing);
      }

      // inject / fill / push

      case 5: {
        ++this->program_counter_;
        SourceBase::inject(out_thing);
      }
        [[fallthrough]];

      case 6: {
        ++this->program_counter_;
        source_mover->port_fill();
      }
        [[fallthrough]];

      case 7: {
        ++this->program_counter_;
      }
        [[fallthrough]];

      case 8: {
        ++this->program_counter_;
        source_mover->port_push();
      }
        [[fallthrough]];

        // @todo Should skip yield if push waited;
      case 9: {
        //
        // this->task_yield(*this);
      }
        [[fallthrough]];

      case 10: {
        this->program_counter_ = 0;

        if (source_mover->is_done() || sink_mover->is_done()) {
          if (this->debug())
            std::cout << this->name() + " node " + std::to_string(this->id()) +
                         " at bottom got sink_mover done -- going to exhaust source\n";

          sink_mover->port_exhausted();
          break;
        }
      }
        [[fallthrough]];

      default: {
        break;
      }
    }
  }

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
    // source_mover->port_exhausted();
  }
};

/*
 * Forward references.
 */
template<template<class> class Mover, class T>
struct consumer_node;

template<template<class> class Mover, class T>
struct producer_node;

template<template<class> class SinkMover, class BlockIn,
        template<class> class SourceMover, class BlockOut>
struct function_node;

template<class T>
struct correspondent_traits {
};

template<template<class> class Mover, class T>
struct producer_node : public std::shared_ptr<producer_node_impl<Mover, T>> {
  using Base = std::shared_ptr<producer_node_impl<Mover, T>>;
  using Base::Base;

  template<class Function>
  explicit producer_node(Function &&f)
          : Base{std::make_shared<producer_node_impl<Mover, T>>(std::forward<Function>(f))} {
  }

  explicit producer_node(producer_node_impl<Mover, T> &impl) : Base{
          std::make_shared<producer_node_impl<Mover, T>>(std::move(impl))} {
  }
};

template<template<class> class Mover, class T>
struct consumer_node : public std::shared_ptr<consumer_node_impl<Mover, T>> {
  using Base = std::shared_ptr<consumer_node_impl<Mover, T>>;
  using Base::Base;

  template<class Function>
  explicit consumer_node(Function &&f)
          : Base{std::make_shared<consumer_node_impl<Mover, T>>(std::forward<Function>(f))} {
  }

  explicit consumer_node(consumer_node_impl<Mover, T> &impl) : Base{
          std::make_shared<consumer_node_impl<Mover, T>>(std::move(impl))} {
  }
};

template<template<class> class SinkMover, class BlockIn,
        template<class> class SourceMover = SinkMover, class BlockOut = BlockIn>
struct function_node : public std::shared_ptr<function_node_impl<SinkMover, BlockIn, SourceMover, BlockOut>> {
  using Base = std::shared_ptr<function_node_impl<SinkMover, BlockIn, SourceMover, BlockOut>>;
  using Base::Base;

  template<class Function>
  explicit function_node(Function &&f)
          : Base{
          std::make_shared<function_node_impl<SinkMover, BlockIn, SourceMover, BlockOut>>(std::forward<Function>(f))} {
  }

  explicit function_node(function_node_impl<SinkMover, BlockIn, SourceMover, BlockOut> &impl) : Base{
          std::make_shared<function_node_impl<SinkMover, BlockIn, SourceMover, BlockOut>>(std::move(impl))} {
  }
};
}// namespace tiledb::common
