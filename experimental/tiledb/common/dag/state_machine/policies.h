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

template <class PortState, class T>
class AsyncStateMachine;

template <class PortState, class T>
class UnifiedAsyncStateMachine;

/**
 * Base action policy. Includes items array and `register` and `deregister`
 * functions.
 */
template <class PortState, class T>
class BaseStateMachine;

template <class T>
class BaseStateMachine<PortState<3>, T> {
  friend AsyncStateMachine<PortState<3>, T>;
  friend UnifiedAsyncStateMachine<PortState<3>, T>;

  using ItemType = std::optional<T>;

  std::array<ItemType*, 3> items_;
  std::array<size_t, 3> moves_;

  using PortState = PortState<3>;

 public:
  BaseStateMachine() = default;
  BaseStateMachine(
      ItemType& source_init, ItemType& edge_init, ItemType& sink_init)
      : items_{&source_init, &edge_init, &sink_init} {
  }

  /**
   * @pre Called under lock
   */
  void register_items(
      ItemType& source_item, ItemType& edge_item, ItemType& sink_item) {
    items_[0] = &source_item;
    items_[1] = &edge_item;
    items_[2] = &sink_item;
  }

  void deregister_items(
      ItemType& source_item, ItemType& edge_item, ItemType& sink_item) {
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
  template <class StateMachine>
  inline void on_move(
      const StateMachine& state_machine, std::atomic<int>& event) {
    auto state = state_machine.state();
    bool debug = state_machine.debug_enabled();
    CHECK(
        (state == PortState::st_010 || state == PortState::st_100 ||
         state == PortState::st_101 || state == PortState::st_110));

    if (state_machine.debug_enabled()) {
      std::cout << event << "  "
                << " source swapping items with " + str(state) + " and " +
                       str(state_machine.next_state())
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
                << " source done swapping items with " +
                       str(state_machine.state()) + " and " +
                       str(state_machine.next_state())
                << std::endl;
      ++event;
    }
  }
};

template <class T>
class BaseStateMachine<PortState<2>, T> {
  friend AsyncStateMachine<PortState<2>, T>;
  friend UnifiedAsyncStateMachine<PortState<2>, T>;

  using PortState = PortState<2>;
  using ItemType = std::optional<T>;

  std::array<ItemType*, 2> items_;
  std::array<size_t, 2> moves_{0, 0};

 public:
  BaseStateMachine() = default;
  BaseStateMachine(ItemType& source_init, ItemType& sink_init)
      : items_{&source_init, &sink_init} {
  }

  /**
   * @pre Called under lock
   */
  void register_items(ItemType& source_item, ItemType& sink_item) {
    items_[0] = &source_item;
    items_[1] = &sink_item;
  }

  void deregister_items(ItemType& source_item, ItemType& sink_item) {
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

        std::cout << " "
                  << (j == nullptr && j->has_value() ?
                          std::to_string(j->value()) :
                          "no_value")
                  << " ";
      }
      std::cout << ") -> ";
    }

    std::swap(*items_[0], *items_[1]);

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

/**
 * Null action policy.  Verifies compilation of CRTP.  All functions except
 * registration are empty.
 */
template <class PortState, class T>
class NullStateMachine
    : public BaseStateMachine<PortState, T>,
      public PortFiniteStateMachine<PortState, NullStateMachine<PortState, T>> {
  using BSM = BaseStateMachine<PortState, T>;
  using FSM = PortFiniteStateMachine<PortState, NullStateMachine<PortState, T>>;
  using lock_type = typename FSM::lock_type;

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
template <class PortState, class T>
class ManualStateMachine : public BaseStateMachine<PortState, T>,
                           public PortFiniteStateMachine<
                               PortState,
                               ManualStateMachine<PortState, T>> {
  using BSM = BaseStateMachine<PortState, T>;
  using FSM =
      PortFiniteStateMachine<PortState, ManualStateMachine<PortState, T>>;
  using lock_type = typename FSM::lock_type;

 public:
  ManualStateMachine() {
    if constexpr (PortState::N_ == 2) {
      CHECK(str(FSM::state()) == "st_00");
    } else if constexpr (PortState::N_ == 3) {
      CHECK(str(FSM::state()) == "st_000");
    }
  }
  inline void on_ac_return(lock_type&, std::atomic<int>&) {
    if (FSM::debug_enabled())
      std::cout << "    "
                << "Action return" << std::endl;
  }
  inline void on_source_move(lock_type&, std::atomic<int>& event) {
    BSM::on_move(*this, event);
  }

  inline void on_sink_move(lock_type&, std::atomic<int>& event) {
    BSM::on_move(*this, event);
  }
  inline void notify_source(lock_type&, std::atomic<int>&) {
    if (FSM::debug_enabled())
      std::cout << "    "
                << "Action notify source" << std::endl;
  }
  inline void notify_sink(lock_type&, std::atomic<int>&) {
    if (FSM::debug_enabled())
      std::cout << "    "
                << "Action notify source" << std::endl;
  }
  inline void on_source_wait(lock_type&, std::atomic<int>&) {
    if (FSM::debug_enabled())
      std::cout << "    "
                << "Action source wait" << std::endl;
  }

  inline void on_sink_wait(lock_type&, std::atomic<int>&) {
    if (FSM::debug_enabled())
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
template <class PortState, class T>
class AsyncStateMachine : public BaseStateMachine<PortState, T>,
                          public PortFiniteStateMachine<
                              PortState,
                              AsyncStateMachine<PortState, T>> {
  using BSM = BaseStateMachine<PortState, T>;
  using FSM =
      PortFiniteStateMachine<PortState, AsyncStateMachine<PortState, T>>;
  //  std::mutex mutex_;
  std::condition_variable sink_cv_;
  std::condition_variable source_cv_;

  using ItemType = typename BSM::ItemType;

 public:
  using lock_type = typename FSM::lock_type;

  //  template <your_stuff>
  //  your_return_type_if_present yourfunction(
  //      args,
  //      typename enable_if<your_condition, void**>::type = nullptr) {  // ...
  //  }

  template <size_t M = PortState::N_>
  AsyncStateMachine(
      ItemType& source_item,
      ItemType& edge_item,
      ItemType& sink_item,
      bool debug = false,
      typename std::enable_if<(M == 3), void**>::type = nullptr)
      : BSM{source_item, edge_item, sink_item} {
    if (debug) {
      FSM::enable_debug();
    }
  }

  template <size_t M = PortState::N_>
  AsyncStateMachine(
      ItemType& source_item,
      ItemType& sink_item,
      bool debug = false,
      typename std::enable_if<(M == 2), void**>::type = nullptr)
      : BSM{source_item, sink_item} {
    if (debug) {
      FSM::enable_debug();
    }
  }

  AsyncStateMachine() = default;
  AsyncStateMachine(const AsyncStateMachine&) = default;
  AsyncStateMachine(AsyncStateMachine&&) = default;

  /**
   * Function for handling `ac_return` action.
   */
  inline void on_ac_return(lock_type&, std::atomic<int>&) {
  }

  /**
   * Function for handling `notify_source` action.
   */
  inline void notify_source(lock_type&, std::atomic<int>& event) {
    if (FSM::debug_enabled())
      std::cout << event++ << "  "
                << " sink notifying source (on_signal_source) with " +
                       str(FSM::state()) + " and " + str(FSM::next_state())
                << std::endl;

    CHECK(is_sink_empty(FSM::state()) == "");
    source_cv_.notify_one();
  }

  /**
   * Function for handling `notify_sink` action.
   */
  inline void notify_sink(lock_type&, std::atomic<int>& event) {
    if (FSM::debug_enabled())
      std::cout << event++ << "  "
                << " source notifying sink(on_signal_sink) with " +
                       str(FSM::state()) + " and " + str(FSM::next_state())
                << std::endl;

    CHECK(is_source_full(FSM::state()) == "");
    sink_cv_.notify_one();
  }

  /**
   * Function for handling `sink_move` action.
   */
  inline void on_sink_move(lock_type&, std::atomic<int>& event) {
    BSM::on_move(*this, event);
    BSM::moves_[PortState::N_ - 1]++;
  }

  /**
   * Function for handling `src_move` action.
   */
  inline void on_source_move(lock_type&, std::atomic<int>& event) {
    BSM::on_move(*this, event);
    BSM::moves_[0]++;
  }

  inline void on_sink_wait(lock_type& lock, std::atomic<int>& event) {
    if constexpr (PortState::N_ == 2) {
      CHECK(str(FSM::state()) == "st_00");
    } else if constexpr (PortState::N_ == 3) {
      CHECK(str(FSM::state()) == "st_000");
    }

    if (FSM::debug_enabled())
      std::cout << event++ << "  "
                << " sink going to sleep on_sink_move with " + str(FSM::state())
                << std::endl;
    sink_cv_.wait(lock);

    CHECK(is_sink_post_move(FSM::state()) == "");

    if (FSM::debug_enabled())
      std::cout << event++ << "  "
                << " sink waking up on_sink_move with " + str(FSM::state()) +
                       " and " + str(FSM::next_state())
                << std::endl;

    if (FSM::debug_enabled())
      std::cout << event++ << "  "
                << " sink leaving on_sink_move with " + str(FSM::state()) +
                       " and " + str(FSM::next_state())
                << std::endl;
  }

  inline void on_source_wait(lock_type& lock, std::atomic<int>& event) {
    if constexpr (PortState::N_ == 2) {
      CHECK(str(FSM::state()) == "st_11");
    } else if constexpr (PortState::N_ == 3) {
      CHECK(str(FSM::state()) == "st_111");
    }

    if (FSM::debug_enabled())
      std::cout << event++ << "  "
                << " source going to sleep on_source_move with " +
                       str(FSM::state()) + " and " + str(FSM::next_state())
                << std::endl;

    source_cv_.wait(lock);

    CHECK(is_source_post_move(FSM::state()) == "");

    if (FSM::debug_enabled())
      std::cout << event++ << "  "
                << " source waking up to " + str(FSM::state()) + " and " +
                       str(FSM::next_state())
                << std::endl;

    if (FSM::debug_enabled())
      std::cout << event++ << "  "
                << " source leaving on_source_move with " + str(FSM::state()) +
                       " and " + str(FSM::next_state())
                << std::endl;
  }
};

/**
 * An asynchronous state machine class.  Implements on_sink_move and
 * on_source_move using locks and condition variables.  This class is similar
 * to `AsyncStateMachine`, but takes advantage of the fact that the notify and
 * move functions are the same, and uses just a single implementation of them,
 * along with just a single condition variable
 *
 * @note This class includes a fair amount of debugging code.
 *
 * @todo Investigate coroutine-like approach for corresponding with implementing
 * actions so that the procession of steps is driven by the state machine rather
 * than the user of the state machine.
 */
template <class PortState, class T>
class UnifiedAsyncStateMachine : public BaseStateMachine<PortState, T>,
                                 public PortFiniteStateMachine<
                                     PortState,
                                     UnifiedAsyncStateMachine<PortState, T>> {
  using BSM = BaseStateMachine<PortState, T>;
  using FSM =
      PortFiniteStateMachine<PortState, UnifiedAsyncStateMachine<PortState, T>>;
  using lock_type = typename FSM::lock_type;
  std::condition_variable cv_;

  using ItemType = typename BSM::ItemType;

 public:
  bool debug_{false};

  template <size_t M = PortState::N_>
  UnifiedAsyncStateMachine(
      ItemType& source_item,
      ItemType& edge_item,
      ItemType& sink_item,
      bool debug = false,
      typename std::enable_if<(M == 3), void**>::type = nullptr)
      : BSM{source_item, edge_item, sink_item} {
    if (debug) {
      FSM::enable_debug();
    }
  }

  template <size_t M = PortState::N_>
  UnifiedAsyncStateMachine(
      ItemType& source_item,
      ItemType& sink_item,
      bool debug = false,
      typename std::enable_if<(M == 2), void**>::type = nullptr)
      : BSM{source_item, sink_item} {
    if (debug) {
      FSM::enable_debug();
    }
  }

  UnifiedAsyncStateMachine(const UnifiedAsyncStateMachine&) = default;
  UnifiedAsyncStateMachine(UnifiedAsyncStateMachine&&) = default;

  /**
   * Function for handling `ac_return` action.
   */
  inline void on_ac_return(lock_type&, int){};

  /**
   * Single  notify function for source and sink.
   */
  inline void do_notify(lock_type&, std::atomic<int>&) {
    cv_.notify_one();
  }

  /**
   * Function for handling `notify_source` action, invoking a `do_notify`
   * action.
   */
  inline void notify_source(lock_type& lock, std::atomic<int>& event) {
    if (debug_)
      std::cout << event++ << "  "
                << " sink notifying source" << std::endl;
    do_notify(lock, event);
  }

  /**
   * Function for handling `notify_sink` action, invoking a `do_notify` action.
   */
  inline void notify_sink(lock_type& lock, std::atomic<int>& event) {
    if (debug_)
      std::cout << event++ << "  "
                << " source notifying sink" << std::endl;
    do_notify(lock, event);
  }

  /**
   * Function for handling `source_move` action.
   */
  inline void on_source_move(lock_type&, std::atomic<int>& event) {
    BSM::on_move(*this, event);
  }

  /**
   * Function for handling `sink_move` action.  It simply calls the source
   * move action.
   */
  inline void on_sink_move(lock_type& lock, std::atomic<int>& event) {
    on_source_move(lock, event);
  }

  /**
   * Function for handling `source_move` action.
   */
  inline void on_source_wait(lock_type& lock, std::atomic<int>&) {
    cv_.wait(lock);
  }

  /**
   * Function for handling `sink_move` action.  It simply calls the source
   * move action.
   */
  inline void on_sink_wait(lock_type& lock, std::atomic<int>& event) {
    on_source_wait(lock, event);
  }
};

/**
 * A simple debugging action policy that simply prints that an action has been
 * called.
 */
template <class PortState, class T>
class DebugStateMachine : public BaseStateMachine<PortState, T>,
                          public PortFiniteStateMachine<
                              PortState,
                              DebugStateMachine<PortState, T>> {
  using BSM = BaseStateMachine<PortState, T>;
  using FSM =
      PortFiniteStateMachine<PortState, DebugStateMachine<PortState, T>>;

  using lock_type = typename FSM::lock_type;

 public:
  inline void on_ac_return(lock_type&, std::atomic<int>&) {
    if (this->debug_enabled())
      std::cout << "    "
                << "Action return" << std::endl;
  }
  inline void on_source_move(lock_type&, std::atomic<int>&) {
    if (this->debug_enabled())

      std::cout << "    "
                << "Action move source" << std::endl;
  }
  inline void on_sink_move(lock_type&, std::atomic<int>&) {
    if (this->debug_enabled())
      std::cout << "    "
                << "Action move sink" << std::endl;
  }
  inline void on_source_wait(lock_type&, std::atomic<int>&) {
    if (this->debug_enabled())

      std::cout << "    "
                << "Action source wait" << std::endl;
  }
  inline void on_sink_wait(lock_type&, std::atomic<int>&) {
    if (this->debug_enabled())
      std::cout << "    "
                << "Action sink wait" << std::endl;
  }
  inline void notify_source(lock_type&, std::atomic<int>&) {
    if (this->debug_enabled())
      std::cout << "    "
                << "Action notify source" << std::endl;
  }
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
class DebugStateMachineWithLock
    : public BaseStateMachine<PortState<2>, size_t>,
      public PortFiniteStateMachine<PortState<2>, DebugStateMachineWithLock> {
  std::mutex mutex_;
  std::condition_variable sink_cv_;
  std::condition_variable source_cv_;
  using lock_type = std::unique_lock<std::mutex>;

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
