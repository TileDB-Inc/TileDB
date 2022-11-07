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
 * This file defines nodes that support segmented execution for the TileDB task
 * graph.
 * *
 * Segmented execution is implemented using a "Duff's device" style loop,
 * allowing the node to yield control back to the scheduler and return execution
 * where it left off.
 *
 * There are three types of segmented nodes:
 *   - Producer, which encapsulates a producer function that produces a single
 * result.
 *   - Consumer, which encapsulates a consumer function that consumes a single
 * result.
 *   - Function, which encapsulates a function that produces and consumes a
 * single result.
 *
 * The function encapsulated in the producer node may issue a stop request, in
 * which case the producer node will begin shutting down the task graph.
 *
 * Execution of a node is accessed through the `resume` function.
 *
 * To enable the different kinds of nodes to be stored in a singly typed
 * container, we use an abstract base class `node_base` from which all other
 * nodes are derived.
 *
 * Nodes maintain a link to a correspondent node, which links are used for
 * scheduling purposes (sending events).  The links are maintained on the nodes
 * rather than on tasks, because the nodes are the objects that are actually
 * created (by the user) and stored in the task graph when the task graph is
 * created.  This connectivity is redundant with the connectivity between ports.
 * @todo Consider removing the connectivity between nodes and instead using the
 * connectivity between ports.
 *
 * The following can be a useful debug string:
 *   `this->name() + " " + std::to_string(this->id())`
 */

#include <atomic>
#include <iostream>
#include <memory>
#include "experimental/tiledb/common/dag/execution/jthread/stop_token.hpp"
#include "experimental/tiledb/common/dag/ports/ports.h"
#include "experimental/tiledb/common/dag/state_machine/fsm_types.h"
#include "experimental/tiledb/common/dag/utils/print_types.h"
#include "experimental/tiledb/common/dag/execution/duffs_types.h"
#include "experimental/tiledb/common/dag/execution/task_state_machine.h"

namespace tiledb::common {

/*
 * Forward declarations
 */
template <template <class> class Mover, class T>
struct producer_node_impl;
template <template <class> class Mover, class T>
class consumer_node_impl;
template <
    template <class>
    class SinkMover,
    class BlockIn,
    template <class>
    class SourceMover,
    class BlockOut>
class function_node_impl;

/**
 * Base class for all segmented nodes.  Maintains a program counter (for the
 * Duff's device) and a link to other nodes with which it communicates.  For
 * testing and debugging purposes, the node also maintains a name and an id.
 */
class node_base {
  using node_handle_type = std::shared_ptr<node_base>;

 protected:
  using scheduler_event_type = SchedulerAction;

  bool debug_{false};

  size_t id_{0UL};
  size_t program_counter_{0};

 private:
  node_handle_type sink_correspondent_{nullptr};
  node_handle_type source_correspondent_{nullptr};

 public:
  [[nodiscard]] size_t get_program_counter() const {
    return program_counter_;
  }

  virtual node_handle_type& sink_correspondent() {
    return sink_correspondent_;
  }

  virtual node_handle_type& source_correspondent() {
    return source_correspondent_;
  }

  /** Default constructor */
  node_base(node_base&&) = default;

  /** Nonsensical constructor, provided so that node_base will meet movable
   * concept requirements */
  node_base(const node_base&) {
    std::cout << "Nonsense copy constructor" << std::endl;
  }

  /** Default destructor
   * @todo Is virtual necessary?
   */
  virtual ~node_base() = default;

  /** Return the id of the node (const) */
  [[nodiscard]] inline size_t id() const {
    return id_;
  }

  /** Return a reference to the id of the node (non const) */
  inline size_t& id() {
    return id_;
  }

  /** Constructor taking an id */
  explicit node_base(size_t id)
      : id_{id} {
  }

  /** Utility functions for indicating what kind of node and state of the ports
   * being used.
   *
   * @todo Are these used anywhere?  This is an abstraction violation, so we
   * should try not to use them.
   * */
  [[nodiscard]] virtual bool is_producer_node() const {
    return false;
  }

  [[nodiscard]] virtual bool is_consumer_node() const {
    return false;
  }

  [[nodiscard]] virtual bool is_function_node() const {
    return false;
  }

  [[nodiscard]] virtual bool is_source_empty() const {
    return false;
  }

  [[nodiscard]] virtual bool is_sink_full() const {
    return false;
  }

  [[nodiscard]] virtual bool is_sink_state_empty() const {
    return false;
  }

  [[nodiscard]] virtual bool is_sink_state_full() const {
    return false;
  }

  [[nodiscard]] virtual bool is_source_state_empty() const {
    return false;
  }

  [[nodiscard]] virtual bool is_source_state_full() const {
    return false;
  }

  [[nodiscard]] virtual bool is_source_terminating() const {
    return false;
  }

  [[nodiscard]] virtual bool is_sink_terminating() const {
    return false;
  }

  [[nodiscard]] virtual bool is_source_terminated() const {
    return false;
  }

  [[nodiscard]] virtual bool is_sink_terminated() const {
    return false;
  }

  [[nodiscard]] virtual bool is_source_done() const {
    return false;
  }

  [[nodiscard]] virtual bool is_sink_done() const {
    return false;
  }

  /**
   * The resume function.  Primary entry point for execution of the node.
   */
  virtual scheduler_event_type resume() = 0;

  /**
   * The run function.  Executes resume in loop until the node is done.
   */
  virtual void run() = 0;

  /** Decrement the program counter */
  void decrement_program_counter() {
    assert(program_counter_ > 0);
    --program_counter_;
  }

  /** Function for getting name of node.  As used in this library, the name
   * is just a string that specifies the type of node.
   *
   * @return Name of the node
   */
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

  /** Function useful for debugging.  */
  virtual void dump_node_state() = 0;
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
template <class From, class To>
void connect(From& from, To& to) {
  (*from).sink_correspondent() = to;
  (*to).source_correspondent() = from;
}

/**
 * An atomic counter used to assign unique ids to nodes.
 */
std::atomic<size_t> id_counter{0};

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
  template <class Function>
  explicit producer_node_impl(
      Function&& f,
      std::enable_if_t<
          std::is_invocable_r_v<T, Function, std::stop_source&>,
          void**> = nullptr)
      : node_base_type(id_counter++)
      , f_{std::forward<Function>(f)}
      , produced_items_{0} {
  }

  producer_node_impl(producer_node_impl&& rhs) noexcept = default;

  /** Utility functions for indicating what kind of node and state of the ports
   * being used.
   *
   * @todo Are these used anywhere?  This is an abstraction violation, so we
   * should try not to use them.
   * */
  bool is_producer_node() const override {
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

  /** Return name of node. */
  std::string name() override {
    return {"producer"};
  }

  void enable_debug() override {
    node_base_type::enable_debug();
    if (this->item_mover_)
      this->item_mover_->enable_debug();
  }

  auto get_source_mover() const {
    return this->get_mover();
  }

  void dump_node_state() override {
    auto mover = this->get_mover();
    std::cout << this->name() << " Node state: " << str(mover->state())
              << std::endl;
  }

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

    [[maybe_unused]] std::thread::id this_id = std::this_thread::get_id();

    std::stop_source st;
    decltype(f_(st)) thing{};

    std::stop_source stop_source_;
    assert(!stop_source_.stop_requested());

    switch (this->program_counter_) {
      case 0: {
        ++this->program_counter_;

        thing = f_(stop_source_);

        if (stop_source_.stop_requested()) {
          this->program_counter_ = 999;
          return mover->port_exhausted();
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
        return mover->port_fill();
      }
        [[fallthrough]];

      case 3: {
        this->program_counter_ = 4;
      }
        [[fallthrough]];

      case 4: {
        this->program_counter_ = 5;
        return mover->port_push();
      }
        [[fallthrough]];

        // @todo Should skip yield if push waited;
      case 5: {
        this->program_counter_ = 0;
        // this->task_yield(*this);
        return scheduler_event_type::yield;
      }
      case 999: {
        return scheduler_event_type::error;
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
};

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

  std::function<void(const T&)> f_;

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
          std::is_invocable_r_v<void, Function, const T&>,
          void**> = nullptr)
      : node_base_type(id_counter++)
      , f_{std::forward<Function>(f)}
      , consumed_items_{0} {
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

  std::string name() override {
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

  T thing{};

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

    switch (this->program_counter_) {
      /*
       * case 0 is executed only on the very first call to resume.
       */
      case 0: {
        ++this->program_counter_;

        auto pre_state = mover->state();

        auto tmp_state = mover->port_pull();

        auto post_state = mover->state();

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

        if (mover->is_done()) {
          if constexpr (std::is_same_v<decltype(post_state), two_stage>) {
            if (post_state == two_stage::xt_01) {
              throw std::runtime_error("consumer got stuck in xt_01 state");
            }
          } else {
            if (post_state == three_stage::xt_001) {
              throw std::runtime_error("consumer got stuck in xt_001 state");
            }
          }

          return mover->port_exhausted();
          break;
        } else {
          return tmp_state;
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

        // @todo Should skip yield if pull waited;
      case 5: {
        ++this->program_counter_;

        auto tmp_state = mover->port_pull();

        if (mover->is_done()) {
          return mover->port_exhausted();
          break;
        } else {
          return tmp_state;
        }
      }

        [[fallthrough]];

      // @todo Where is the best place to yield?
      case 6: {
        this->program_counter_ = 1;
        // this->task_yield(*this); ??
        return scheduler_event_type::yield;
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
};

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

  std::function<BlockOut(const BlockIn&)> f_;

  std::atomic<size_t> processed_items_{0};

  size_t processed_items() {
    return processed_items_.load();
  }

 public:
  /** Primary constructor. */
  template <class Function>
  explicit function_node_impl(
      Function&& f,
      std::enable_if_t<
          std::is_invocable_r_v<BlockOut, Function, const BlockIn&>,
          void**> = nullptr)
      : node_base_type(id_counter++)
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

  BlockIn in_thing{};
  BlockOut out_thing{};

 public:
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

    switch (this->program_counter_) {
      // pull / extract drain
      case 0: {
        ++this->program_counter_;

        auto tmp_state = sink_mover->port_pull();

        if (sink_mover->is_done()) {
          return source_mover->port_exhausted();
          break;
        } else {
          return tmp_state;
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

        assert(this->source_correspondent() != nullptr);
        assert(this->sink_correspondent() != nullptr);
      }
        [[fallthrough]];

      case 4: {
        ++this->program_counter_;

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
        return source_mover->port_fill();
      }
        [[fallthrough]];

      case 7: {
        ++this->program_counter_;
      }
        [[fallthrough]];

      case 8: {
        ++this->program_counter_;
        return source_mover->port_push();
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
    return scheduler_event_type::error;
  }

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

/*
 * Forward references.
 */
template <template <class> class Mover, class T>
struct consumer_node;

template <template <class> class Mover, class T>
struct producer_node;

template <
    template <class>
    class SinkMover,
    class BlockIn,
    template <class>
    class SourceMover,
    class BlockOut>
struct function_node;

template <class T>
struct correspondent_traits {};

/** A producer node is a shared pointer to the implementation class */
template <template <class> class Mover, class T>
struct producer_node : public std::shared_ptr<producer_node_impl<Mover, T>> {
  using Base = std::shared_ptr<producer_node_impl<Mover, T>>;
  using Base::Base;

  template <class Function>
  explicit producer_node(Function&& f)
      : Base{std::make_shared<producer_node_impl<Mover, T>>(
            std::forward<Function>(f))} {
  }

  explicit producer_node(producer_node_impl<Mover, T>& impl)
      : Base{std::make_shared<producer_node_impl<Mover, T>>(std::move(impl))} {
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

/** A function node is a shared pointer to the implementation class */
template <
    template <class>
    class SinkMover,
    class BlockIn,
    template <class> class SourceMover = SinkMover,
    class BlockOut = BlockIn>
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
