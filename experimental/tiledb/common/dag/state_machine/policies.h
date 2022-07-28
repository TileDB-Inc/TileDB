/**
 * @file policies3.h
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
 * Implementation of finite state machine policies3.
 *
 * The different policies3 currently include an extensive amount of debugging
 * code.
 *
 * @todo Remove the debugging code.
 */

#ifndef TILEDB_DAG_POLICIES3_H
#define TILEDB_DAG_POLICIES3_H

#include <array>
#include <optional>
#include <string>
#include <tuple>
#include "experimental/tiledb/common/dag/state_machine/test/helpers.h"
#include "experimental/tiledb/common/dag/utils/print_types.h"
#include "experimental/tiledb/common/dag/utils/traits.h"

namespace tiledb::common {

// clang-format off
/**
 *
 * State machine policies3 work with the `PortFiniteStateMachine` class template to
 * mix in functions associated with various entry and exit actions invoked when
 * processing state transition events.  The policy classes defined here use CRTP
 * to effect the mixins.
 *
 * Although we call these "policy" classes, when used with CRTP, we actually end
 * up using the "policy" class as the state machine. Consider `AsyncStateMachine:`
 * declared as:
 * ```c++
 * template <class T>
 * class AsyncStateMachine : public PortFiniteStateMachine<AsyncStateMachine<T>>;
 * ```
 * To use the state machine in fsm.h with the `AsyncStateMachine` policy to
 * transfer integers (say), we use
 * ```c++
 * AsyncStateMachine<int>
 * ```
 * as the state machine class.
 *
 * Currently, the actions defined for use by the PortFiniteStateMachine class, and
 * associated mixin functions are
 *   ac_return: on_ac_return()
 *   src_move: on_source_move()
 *   sink_move: on_sink_move()
 *   notify_source: on_notify_source()
 *   notify_sink: on_notify_sink()
 *
 * NB: With our current approach, we seem to really only need a single move function
 * and a single notify function, so we may be able to condense this in the future.
 * For potential future flexibility, we are leaving separate source and sink versions
 * for now.  The `UnifiedAsyncStateMachine` tests out this idea.
 *
 * With our definition of the state machine (see fsm.h), these actions have the
 * following functionality:
 *   on_ac_return(): empty function at the moment.
 *   on_source_move(): if state is full_empty, notify sink and move, otherwise, notify sink and wait.
 *   on_sink_move(): if state is full_empty, notify source and move, otherwise, notify source and wait.
 *   on_notify_source(): notify sink
 *   on_notify_sink(): notify source
 *
 * The implementation of `AsyncStateMachine` currently uses locks and condition
 * variable for effecting the wait (and notify) functions.
 *
 * A crucial piece of functionality in these classes is the move operation that may take place.
 * When operating with `Source` and `Sink` ports, we need to be able to move the item_ member
 * variables associated with the `Source` and a `Sink` that are bound to each other.  To
 * enable this, the state machine policies3 maintatin pointers to items.  When moving is
 * required, `std::move` is invoked on the pointed-to items.  (This has been tested for
 * integer items when testing just the state machines as weill as with `std::optional` items
 * when testing the ports.)  These internal pointers are initialized with a `register_items`
 * function, and are reset with a `deregister_items` function.
 */
// clang-format on

#if 0
template <class enumerator>
class AsyncPolicy;

template <class enumerator>
class UnifiedAsyncPolicy;
#endif

#if 1
/**
 * Base action policy. Includes items array and `register` and `deregister`
 * functions.
 */
template <class enumerator, class Block>
class BaseMover;

template <class Block>
class BaseMover<three_stage, Block> {
  using item_type = std::optional<Block>;

  std::array<item_type*, 3> items_;
  std::array<size_t, 3> moves_;

  using PortState = PortState<three_stage>;

 public:
  BaseMover() = default;
  BaseMover(
      item_type& source_init, item_type& edge_init, item_type& sink_init, bool)
      : items_{&source_init, &edge_init, &sink_init} {
  }

  /**
   * @pre Called under lock
   */
  void register_items(
      item_type& source_item, item_type& edge_item, item_type& sink_item) {
    items_[0] = &source_item;
    items_[1] = &edge_item;
    items_[2] = &sink_item;
  }

  void deregister_items(
      item_type& source_item, item_type& edge_item, item_type& sink_item) {
    if (items_[0] != &source_item || items_[1] != &edge_item ||
        items_[2] != &sink_item) {
      throw std::runtime_error(
          "Attempting to deregister source, edge, or sink items that were not "
          "registered.");
    }
    for (auto& j : items_) {
      j->reset();
    }
  }

  /**
   * @pre Called under lock
   */
  inline void on_move(
      //      const StateMachine& state_machine,
      std::atomic<int>& event) {
    auto state = this->state();
    bool debug = this->debug_enabled();
    CHECK(
        (state == PortState::st_010 || state == PortState::st_100 ||
         state == PortState::st_101 || state == PortState::st_110));

    if (this->debug_enabled()) {
      std::cout << event << "  "
                << " source swapping items with " + str(state) + " and " +
                       str(this->next_state())
                << std::endl;

      std::cout << event;
      std::cout << "    "
                << "Action on_move state = (";
      for (auto& j : items_) {
        // print_types(j);
        std::cout << " "
                  << (j == nullptr && j->has_value() ?
                          std::to_string(j->value()) :
                          "no_value")
                  << " ";
      }
      std::cout << ") -> ";
    }

    switch (state) {
      case PortState::st_101:
        CHECK(*(items_[0]) != EMPTY_SINK);
        std::swap(*items_[0], *items_[1]);
        break;

      case PortState::st_010:
        CHECK(*(items_[1]) != EMPTY_SINK);
        std::swap(*items_[1], *items_[2]);
        break;

      case PortState::st_100:
        std::swap(*items_[0], *items_[1]);
        std::swap(*items_[1], *items_[2]);
        break;

      case PortState::st_110:
        CHECK(*(items_[1]) != EMPTY_SINK);
        std::swap(*items_[1], *items_[2]);
        CHECK(*(items_[0]) != EMPTY_SINK);
        std::swap(*items_[0], *items_[1]);
        break;

      default:
        if (debug) {
          std::cout << "???" << std::endl;
        }
        break;
    }
    if (debug) {
      std::cout << "(";
      for (auto& j : items_) {
        std::cout << " "
                  << (j == nullptr && j->has_value() ?
                          std::to_string(j->value()) :
                          "no_value")
                  << " ";
      }
      std::cout << ")" << std::endl;
      std::cout << event << "  "
                << " source done swapping items with " + str(this->state()) +
                       " and " + str(this->next_state())
                << std::endl;
      ++event;
    }
  }
};

template <class Block>
class BaseMover<two_stage, Block> {
  using PortState = two_port_type;
  using item_type = std::optional<Block>;

  std::array<item_type*, 2> items_;
  std::array<size_t, 2> moves_{0, 0};

 public:
  BaseMover() = default;
  BaseMover(item_type& source_init, item_type& sink_init, bool)
      : items_{&source_init, &sink_init} {
  }

  /**
   * @pre Called under lock
   */
  void register_items(item_type& source_item, item_type& sink_item) {
    items_[0] = &source_item;
    items_[1] = &sink_item;
  }

  void deregister_items(item_type& source_item, item_type& sink_item) {
    if (items_[0] != &source_item || items_[1] != &sink_item) {
      throw std::runtime_error(
          "Attempting to deregister source or sink items that were not "
          "registered.");
    }
    for (auto& j : items_) {
      j->reset();
    }
  }

  /**
   * @pre Called under lock
   */
  template <class StateMachine>
  inline void on_move(
      const StateMachine& state_machine, std::atomic<int>& event) {
    auto state = state_machine.state();
    bool debug = state_machine.debug_enabled();
    CHECK(state == PortState::st_10);

    if (state_machine.debug_enabled()) {
      std::cout << event << "  "
                << " source swapping items with " + str(state) + " and " +
                       str(state_machine.next_state())
                << std::endl;

      std::cout << event;
      std::cout << "    "
                << "Action on_move state = (";
      for (auto& j : items_) {
        //        print_types(j);

        if constexpr (has_to_string_v<decltype(j->value())>) {
          std::cout << " "
                    << (j == nullptr && j->has_value() ?
                            std::to_string(j->value()) :
                            "no_value")
                    << " ";
        }
      }
      std::cout << ") -> ";
    }

    std::swap(*items_[0], *items_[1]);

    if (debug) {
      std::cout << "(";
      for (auto& j : items_) {
        if constexpr (has_to_string_v<decltype(j->value())>) {
          std::cout << " "
                    << (j == nullptr && j->has_value() ?
                            std::to_string(j->value()) :
                            "no_value")
                    << " ";
        }
      }
      std::cout << ")" << std::endl;
      std::cout << event << "  "
                << " source done swapping items with " +
                       str(state_machine.state()) + " and " +
                       str(state_machine.next_state())
                << std::endl;
      ++event;
    }
  }

  size_t source_swaps() const {
    return moves_[0];
  }
  size_t sink_swaps() const {
    return moves_[1];
  }

  auto& source_item() {
    return *items_[0];
  }
  auto& sink_item() {
    return *items_[1];
  }
};
#else

template <template <class> class Policy, class Block>
class BaseMover<Policy, two_stage, Block> {
 public:
  template <class M = enumerator>
  BaseMover(
      item_type& source_item,
      item_type& sink_item,
      bool debug = false,
      : mover_type{source_item, sink_item} {
    if (debug) {
      state_machine->enable_debug();
    }
  }


  void move() {
  }
};  // namespace tiledb::common

template <template <class> class Policy, class Block>
class BaseMover<Policy, three_stage, Block> {
 public:
  template <class M = enumerator>
  BaseMover(
      item_type& source_item,
      item_type& edge_item,
      item_type& sink_item,
      bool debug = false,
      typename std::enable_if_t<std::is_same_v<M, three_stage>, void**> =
          nullptr)
      : mover_type{source_item, edge_item, sink_item} {
    if (debug) {
      state_machine->enable_debug();
    }
  }

  template <class M = enumerator>
  BaseMover(
      item_type& source_item,
      item_type& sink_item,
      bool debug = false,
      typename std::enable_if_t<std::is_same_v<M, two_stage>, void**> = nullptr)
      : mover_type{source_item, sink_item} {
    if (debug) {
      state_machine->enable_debug();
    }
  }

  void move() {
  }
};
#endif

// template <template <class> class Policy, class enumerator, class Block>
// class ItemMover : public BaseMover<enumerator, Block> {
template <template <class, class> class Policy, class enumerator, class Block>
class ItemMover
    : public Policy<ItemMover<Policy, enumerator, Block>, enumerator>,
      public BaseMover<enumerator, Block> {
 public:
  //  using BaseMover<enumerator, Block>::BaseMover<enumerator, Block>;
  using BaseMover<enumerator, Block>::BaseMover;

  using state_machine_type =
      PortFiniteStateMachine<ItemMover<Policy, enumerator, Block>, enumerator>;
  using stages_type = enumerator;
  using enumerator_type = enumerator;

  /**
   * Invoke `source_fill` event
   */
  void do_fill(const std::string& msg = "") {
    //    this->event(PortEvent::source_fill, msg);
    this->a(msg);
  }

  /**
   * Invoke `source_push` event
   */
  void do_push(const std::string& msg = "") {
    this->event(PortEvent::source_push, msg);
  }

  /**
   * Invoke `sink_drain` event
   */
  void do_drain(const std::string& msg = "") {
    this->event(PortEvent::sink_drain, msg);
  }

  /**
   * Invoke `sink_pull` event
   */
  void do_pull(const std::string& msg = "") {
    this->event(PortEvent::sink_pull, msg);
  }

  /**
   * Invoke `shutdown` event
   */
  void do_shutdown(const std::string& msg = "") {
    this->event(PortEvent::shutdown, msg);
  }

  state_machine_type* get_state_machine() {
    return static_cast<state_machine_type*>(*this);
  }
  auto get_state() {
    return static_cast<state_machine_type*>(this)->state();
  }
};

/**
 * Null action policy.  Verifies compilation of CRTP.  All functions except
 * registration are empty.
 */
template <class ItemMover>
class NullPolicy : public ItemMover {
  using mover_type = ItemMover;
  using lock_type = typename mover_type::lock_type;

  inline void on_ac_return(lock_type&, std::atomic<int>&) {
  }
  inline void on_source_move(lock_type&, std::atomic<int>&) {
  }
  inline void on_sink_move(lock_type&, std::atomic<int>&) {
  }
  inline void notify_source(lock_type&, std::atomic<int>&) {
  }
  inline void notify_sink(lock_type&, std::atomic<int>&) {
  }
};

/**
 * A state machine for testing progress of messages using manual invocations of
 * port state machine functions.  The only non-trivial functions are
 * `on_source_move` and `on_sink_move`, both of which simply invoke
 * `std::move` on the cached items.
 */
template <class ItemMover>
class ManualPolicy : public ItemMover {
  using mover_type = ItemMover;
  using state_machine_type = typename mover_type::state_machine_type;
  using lock_type = typename mover_type::lock_type;

  state_machine_type state_machine = mover_type::get_state_machine();

  using enumerator = typename mover_type::enumerator_type;

 public:
  ManualPolicy() {
    if constexpr (std::is_same_v<enumerator, two_port_type>) {
      CHECK(str(mover_type::get_state()) == "st_00");
    } else if constexpr (std::is_same_v<enumerator, three_port_type>) {
      CHECK(str(mover_type::get_state()) == "st_000");
    }
  }
  inline void on_ac_return(lock_type&, std::atomic<int>&) {
    if (mover_type::debug_enabled())
      std::cout << "    "
                << "Action return" << std::endl;
  }
  inline void on_source_move(lock_type&, std::atomic<int>& event) {
    mover_type::move(*this, event);
  }

  inline void on_sink_move(lock_type&, std::atomic<int>& event) {
    mover_type::move(*this, event);
  }
  inline void notify_source(lock_type&, std::atomic<int>&) {
    if (state_machine->debug_enabled())
      std::cout << "    "
                << "Action notify source" << std::endl;
  }
  inline void notify_sink(lock_type&, std::atomic<int>&) {
    if (state_machine->debug_enabled())
      std::cout << "    "
                << "Action notify source" << std::endl;
  }
  inline void on_source_wait(lock_type&, std::atomic<int>&) {
    if (state_machine->debug_enabled())
      std::cout << "    "
                << "Action source wait" << std::endl;
  }

  inline void on_sink_wait(lock_type&, std::atomic<int>&) {
    if (state_machine->debug_enabled())
      std::cout << "    "
                << "Action sink wait" << std::endl;
  }
};

/**
 * An asynchronous state machine class.  Implements on_sink_move and
 * on_source_move using locks and condition variables.
 *
 * It is assumed that the source and sink are running as separate asynchronous
 * tasks (in this case, effected by `std::async`).
 *
 * @note This class includes a fair amount of debugging code, including
 * Catch2 macros.
 *
 * @todo Remove the debugging code, or at the very least, make sure we can
 * compile without needing the Catch2 macros defined (e.g., defining them
 * as empty when the Catch2 header has not been included..
 *
 * @todo Investigate coroutine-like approach for corresponding with implementing
 * actions so that the procession of steps is driven by the state machine rather
 * than the user of the state machine.
 */

template <class Mover, class enumerator>
class AsyncPolicy
    : public PortFiniteStateMachine<AsyncPolicy<Mover, enumerator>, enumerator>

{
  using mover_type = Mover;

  //  using state_machine_type = typename mover_type::state_machine_type;
  //  state_machine_type state_machine = mover_type::get_state_machine();

  //  using lock_type = typename mover_type::lock_type;
  //  using item_type = typename mover_type::item_type;

  std::condition_variable sink_cv_;
  std::condition_variable source_cv_;

 public:
  AsyncPolicy() = default;
  AsyncPolicy(const AsyncPolicy&) {
  }
  AsyncPolicy(AsyncPolicy&&) = default;

  /**
   * Function for handling `ac_return` action.
   */
  template <class lock_type>
  inline void on_ac_return(lock_type&, std::atomic<int>&) {
  }

  /**
   * Function for handling `notify_source` action.
   */
  template <class lock_type>
  inline void notify_source(lock_type&, std::atomic<int>& event) {
    if (this->debug_enabled())
      std::cout << event++ << "  "
                << " sink notifying source (on_signal_source) with " +
                       str(this->state()) + " and " + str(this->next_state())
                << std::endl;

    CHECK(is_sink_empty(this->state()) == "");
    source_cv_.notify_one();
  }

  /**
   * Function for handling `notify_sink` action.
   */
  template <class lock_type>
  inline void notify_sink(lock_type&, std::atomic<int>& event) {
    if (this->debug_enabled())
      std::cout << event++ << "  "
                << " source notifying sink(on_signal_sink) with " +
                       str(this->state()) + " and " + str(this->next_state())
                << std::endl;

    CHECK(is_source_full(this->state()) == "");
    sink_cv_.notify_one();
  }

  /**
   * Function for handling `sink_move` action.
   */
  template <class lock_type>
  inline void on_sink_move(lock_type&, std::atomic<int>& event) {
    static_cast<Mover*>(this)->on_move(*this, event);
  }

  /**
   * Function for handling `src_move` action.
   */
  template <class lock_type>
  inline void on_source_move(lock_type&, std::atomic<int>& event) {
    static_cast<Mover*>(this)->on_move(*this, event);
  }

  template <class lock_type>
  inline void on_sink_wait(lock_type& lock, std::atomic<int>& event) {
    if constexpr (std::is_same_v<enumerator, two_port_type>) {
      CHECK(str(this->state()) == "st_00");
    } else if (std::is_same_v<enumerator, three_port_type>) {
      CHECK(str(this->state()) == "st_000");
    }

    if (this->debug_enabled())
      std::cout << event++ << "  "
                << " sink going to sleep on_sink_move with " +
                       str(this->state())
                << std::endl;
    sink_cv_.wait(lock);

    CHECK(is_sink_post_move(this->state()) == "");

    if (this->debug_enabled())
      std::cout << event++ << "  "
                << " sink waking up on_sink_move with " + str(this->state()) +
                       " and " + str(this->next_state())
                << std::endl;

    if (this->debug_enabled())
      std::cout << event++ << "  "
                << " sink leaving on_sink_move with " + str(this->state()) +
                       " and " + str(this->next_state())
                << std::endl;
  }

  template <class lock_type>
  inline void on_source_wait(lock_type& lock, std::atomic<int>& event) {
    if constexpr (std::is_same_v<enumerator, two_port_type>) {
      CHECK(str(this->state()) == "st_11");
    } else if constexpr (std::is_same_v<enumerator, three_port_type>) {
      CHECK(str(this->state()) == "st_111");
    }

    if (this->debug_enabled())
      std::cout << event++ << "  "
                << " source going to sleep on_source_move with " +
                       str(this->state()) + " and " + str(this->next_state())
                << std::endl;

    source_cv_.wait(lock);

    CHECK(is_source_post_move(this->state()) == "");

    if (this->debug_enabled())
      std::cout << event++ << "  "
                << " source waking up to " + str(this->state()) + " and " +
                       str(this->next_state())
                << std::endl;

    if (this->debug_enabled())
      std::cout << event++ << "  "
                << " source leaving on_source_move with " + str(this->state()) +
                       " and " + str(this->next_state())
                << std::endl;
  }
};

/**
 * An asynchronous state machine class.  Implements on_sink_move and
 * on_source_move using locks and condition variables.  This class is similar
 * to `AsyncPolicy`, but takes advantage of the fact that the notify and
 * move functions are the same, and uses just a single implementation of them,
 * along with just a single condition variable
 *
 * @note This class includes a fair amount of debugging code.
 *
 * @todo Investigate coroutine-like approach for corresponding with implementing
 * actions so that the procession of steps is driven by the state machine rather
 * than the user of the state machine.
 */
template <class Mover, class enumerator>
class UnifiedAsyncPolicy : public PortFiniteStateMachine<
                               UnifiedAsyncPolicy<Mover, enumerator>,
                               enumerator> {
  std::condition_variable cv_;

 public:
#if 0
  using mover_type = Mover;
  using state_machine_type = typename mover_type::state_machine_type;
  using lock_type = typename mover_type::lock_type;
  using enumerator = typename mover_type::enumerator_type;
  state_machine_type state_machine = mover_type::get_state_machine();

  using item_type = typename mover_type::item_type;
#endif

  UnifiedAsyncPolicy() = default;
  UnifiedAsyncPolicy(const UnifiedAsyncPolicy&) {
  }
  UnifiedAsyncPolicy(UnifiedAsyncPolicy&&) = default;

  /**
   * Function for handling `ac_return` action.
   */
  template <class lock_type>
  inline void on_ac_return(lock_type&, std::atomic<int>&){};

  /**
   * Single  notify function for source and sink.
   */
  template <class lock_type>
  inline void do_notify(lock_type&, std::atomic<int>&) {
    cv_.notify_one();
  }

  /**
   * Function for handling `notify_source` action, invoking a `do_notify`
   * action.
   */
  template <class lock_type>
  inline void notify_source(lock_type& lock, std::atomic<int>& event) {
    if (this->debug_enabled())
      std::cout << event++ << "  "
                << " sink notifying source" << std::endl;
    do_notify(lock, event);
  }

  /**
   * Function for handling `notify_sink` action, invoking a `do_notify` action.
   */
  template <class lock_type>
  inline void notify_sink(lock_type& lock, std::atomic<int>& event) {
    if (this->debug_enabled())
      std::cout << event++ << "  "
                << " source notifying sink" << std::endl;
    do_notify(lock, event);
  }

  /**
   * Function for handling `source_move` action.
   */
  template <class lock_type>
  inline void on_source_move(lock_type&, std::atomic<int>& event) {
    static_cast<Mover*>(this)->on_move(*this, event);
  }

  /**
   * Function for handling `sink_move` action.  It simply calls the source
   * move action.
   */
  template <class lock_type>
  inline void on_sink_move(lock_type& lock, std::atomic<int>& event) {
    on_source_move(lock, event);
  }

  /**
   * Function for handling `source_move` action.
   */
  template <class lock_type>
  inline void on_source_wait(lock_type& lock, std::atomic<int>&) {
    cv_.wait(lock);
  }

  /**
   * Function for handling `sink_move` action.  It simply calls the source
   * move action.
   */
  template <class lock_type>
  inline void on_sink_wait(lock_type& lock, std::atomic<int>& event) {
    on_source_wait(lock, event);
  }
};

/**
 * A simple debugging action policy that simply prints that an action has been
 * called.
 */
template <class Mover, class enumerator>
class DebugPolicy : public PortFiniteStateMachine<
                        DebugPolicy<Mover, enumerator>,
                        enumerator> {
#if 0
  using mover_type = Mover;
  using state_machine_type = typename mover_type::state_machine_type;
  using lock_type = typename mover_type::lock_type;
  using enumerator = typename mover_type::enumerator_type;
  state_machine_type* state_machine = mover_type::get_state_machine();
#endif

 public:
  template <class lock_type>
  inline void on_ac_return(lock_type&, std::atomic<int>&) {
    if (this->debug_enabled())
      std::cout << "    "
                << "Action return" << std::endl;
  }
  template <class lock_type>
  inline void on_source_move(lock_type&, std::atomic<int>&) {
    if (this->debug_enabled())

      std::cout << "    "
                << "Action move source" << std::endl;
  }
  template <class lock_type>
  inline void on_sink_move(lock_type&, std::atomic<int>&) {
    if (this->debug_enabled())
      std::cout << "    "
                << "Action move sink" << std::endl;
  }
  template <class lock_type>
  inline void on_source_wait(lock_type&, std::atomic<int>&) {
    if (this->debug_enabled())

      std::cout << "    "
                << "Action source wait" << std::endl;
  }
  template <class lock_type>
  inline void on_sink_wait(lock_type&, std::atomic<int>&) {
    if (this->debug_enabled())
      std::cout << "    "
                << "Action sink wait" << std::endl;
  }
  template <class lock_type>
  inline void notify_source(lock_type&, std::atomic<int>&) {
    if (this->debug_enabled())
      std::cout << "    "
                << "Action notify source" << std::endl;
  }
  template <class lock_type>
  inline void notify_sink(lock_type&, std::atomic<int>&) {
    if (this->debug_enabled())
      std::cout << "    "
                << "Action notify sink" << std::endl;
  }
};

/**
 * Debug action policy with some non-copyable elements (to verify
 * compilation).
 */
template <class Mover>
class DebugPolicyWithLock : public Mover {
  using mover_type = Mover;
  using state_machine_type = typename mover_type::state_machine_type;
  using lock_type = typename mover_type::lock_type;
  using enumerator = typename mover_type::enumerator_type;

  state_machine_type state_machine = mover_type::get_state_machine();
  std::mutex mutex_;
  std::condition_variable sink_cv_;
  std::condition_variable source_cv_;

 public:
  inline void on_ac_return(lock_type&, std::atomic<int>&) {
    std::cout << "    "
              << "Action return" << std::endl;
  }
  inline void on_source_move(lock_type&, std::atomic<int>&) {
    std::cout << "    "
              << "Action move source" << std::endl;
  }
  inline void on_sink_move(lock_type&, std::atomic<int>&) {
    std::cout << "    "
              << "Action move sink" << std::endl;
  }
  inline void notify_source(lock_type&, std::atomic<int>&) {
    std::cout << "    "
              << "Action notify source" << std::endl;
  }
  inline void notify_sink(lock_type&, std::atomic<int>&) {
    std::cout << "    "
              << "Action notify sink" << std::endl;
  }
};

}  // namespace tiledb::common
#endif  // TILEDB_DAG_POLICIES3_H
