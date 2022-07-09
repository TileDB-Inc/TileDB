/**
 * @file policies.h
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
 * Implementation of finite state machine policies.
 *
 * The different policies currently include an extensive amount of debugging
 * code.
 *
 * @todo Remove the debugging code.
 */

#ifndef TILEDB_DAG_POLICIES_H
#define TILEDB_DAG_POLICIES_H

#include <optional>
#include "experimental/tiledb/common/dag/ports/test/helpers.h"
#include "experimental/tiledb/common/dag/utils/print_types.h"

namespace tiledb::common {

// clang-format off
/**
 *
 * State machine policies work with the `PortFiniteStateMachine` class template to
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
 *   src_swap: on_source_swap()
 *   sink_swap: on_sink_swap()
 *   notify_source: on_notify_source()
 *   notify_sink: on_notify_sink()
 *
 * NB: With our current approach, we seem to really only need a single swap function
 * and a single notify function, so we may be able to condense this in the future.
 * For potential future flexibility, we are leaving separate source and sink versions
 * for now.  The `UnifiedAsyncStateMachine` tests out this idea.
 *
 * With our definition of the state machine (see fsm.h), these actions have the
 * following functionality:
 *   on_ac_return(): empty function at the moment.
 *   on_source_swap(): if state is full_empty, notify sink and swap, otherwise, notify sink and wait.
 *   on_sink_swap(): if state is full_empty, notify source and swap, otherwise, notify source and wait.
 *   on_notify_source(): notify sink
 *   on_notify_sink(): notify source
 *
 * The implementation of `AsyncStateMachine` currently uses locks and condition
 * variable for effecting the wait (and notify) functions.
 *
 * A crucial piece of functionality in these classes is the swap operation that may take place.
 * When operating with `Source` and `Sink` ports, we need to be able to swap the item_ member
 * variables associated with the `Source` and a `Sink` that are bound to each other.  To
 * enable this, the state machine policies maintatin pointers to items.  When swapping is
 * required, `std::swap` is invoked on the pointed-to items.  (This has been tested for
 * integer items when testing just the state machines as weill as with `std::optional` items
 * when testing the ports.)  These internal pointers are initialized with a `register_items`
 * function, and are reset with a `deregister_items` function.
 */
// clang-format on

/**
 * Null action policy.  Verifies compilation of CRTP.  All functions except
 * registration are empty.
 */
template <class T = size_t>
class NullStateMachine : public PortFiniteStateMachine<NullStateMachine<T>> {
  using FSM = PortFiniteStateMachine<NullStateMachine<T>>;
  using lock_type = typename FSM::lock_type;

  T* source_item_;
  T* sink_item_;

 public:
  void register_items(T& source_item, T& sink_item) {
    std::scoped_lock lock(FSM::mutex_);
    source_item_ = &source_item;
    sink_item_ = &sink_item;
  }
  void deregister_items(T& source_item, T& sink_item) {
    std::scoped_lock lock(FSM::mutex_);
    if (source_item_ != &source_item || sink_item_ != &sink_item) {
      throw std::runtime_error(
          "Attempting to deregister source and sink items that were not "
          "registered.");
    }
    source_item_->reset();
    sink_item_->reset();
  }

  inline void on_ac_return(lock_type&, std::atomic<int>&) {
  }
  inline void on_source_swap(lock_type&, std::atomic<int>&) {
  }
  inline void on_sink_swap(lock_type&, std::atomic<int>&) {
  }
  inline void notify_source(lock_type&, std::atomic<int>&) {
  }
  inline void notify_sink(lock_type&, std::atomic<int>&) {
  }
};

/**
 * A state machine for testing progress of messages using manual invocations of
 * port state machine functions.  The only non-trivial functions are
 * `on_source_swap` and `on_sink_swap`, both of which simply invoke
 * `std::swap` on the cached items.
 */
template <class T = size_t>
class ManualStateMachine
    : public PortFiniteStateMachine<ManualStateMachine<T>> {
  using FSM = PortFiniteStateMachine<ManualStateMachine<T>>;
  using lock_type = typename FSM::lock_type;

  T* source_item_;
  T* sink_item_;

 public:
  ManualStateMachine() {
    CHECK(str(FSM::state()) == "empty_empty");
  }

  void register_items(T& source_item, T& sink_item) {
    std::scoped_lock lock(FSM::mutex_);
    source_item_ = &source_item;
    sink_item_ = &sink_item;
    //    print_types(source_item_, sink_item_, *source_item_, *sink_item_);
  }
  void deregister_items(T& source_item, T& sink_item) {
    std::scoped_lock lock(FSM::mutex_);
    if (source_item_ != &source_item || sink_item_ != &sink_item) {
      throw std::runtime_error(
          "Attempting to deregister source and sink items that were not "
          "registered.");
    }
    source_item_ = nullptr;
    sink_item_ = nullptr;
  }

  inline void on_ac_return(lock_type&, std::atomic<int>&) {
    if (FSM::debug_enabled())
      std::cout << "    "
                << "Action return" << std::endl;
  }
  inline void on_source_swap(lock_type&, std::atomic<int>&) {
    //    print_types(source_item_, sink_item_, *source_item_, *sink_item_);
    if (FSM::debug_enabled())
      std::cout << "    "
                << "Action source swap (" << source_item_->value() << ", "
                << (sink_item_->has_value() ?
                        std::to_string(sink_item_->value()) :
                        "no_value")
                << ") -> (";
    std::swap(*source_item_, *sink_item_);
    if (FSM::debug_enabled())
      std::cout << (source_item_->has_value() ?
                        std::to_string(source_item_->value()) :
                        "no_value")
                << ", " << sink_item_->value() << ")" << std::endl;
  }

  inline void on_sink_swap(lock_type&, std::atomic<int>&) {
    // print_types(source_item_, sink_item_, *source_item_, *sink_item_);
    if (FSM::debug_enabled())
      std::cout << "    "
                << "Action sink swap (" << source_item_->value() << ", "
                << (sink_item_->has_value() ?
                        std::to_string(sink_item_->value()) :
                        "no_value)")
                << ") -> ";
    std::swap(*source_item_, *sink_item_);
    if (FSM::debug_enabled())
      std::cout << (source_item_->has_value() ?
                        std::to_string(source_item_->value()) :
                        "no_value")
                << ", " << sink_item_->value() << ")" << std::endl;
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
};

/**
 * An asynchronous state machine class.  Implements on_sink_swap and
 * on_source_swap using locks and condition variables.
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
template <class T>
class AsyncStateMachine : public PortFiniteStateMachine<AsyncStateMachine<T>> {
  using FSM = PortFiniteStateMachine<AsyncStateMachine<T>>;
  //  std::mutex mutex_;
  std::condition_variable sink_cv_;
  std::condition_variable source_cv_;

 public:
  // Counters for testing
  size_t source_swaps{};
  size_t sink_swaps{};

  T* source_item_{};
  T* sink_item_{};

  using lock_type = typename FSM::lock_type;

  AsyncStateMachine(T& source_item, T& sink_item, bool debug = false)
      : source_item_(&source_item)
      , sink_item_(&sink_item) {
    if (debug) {
      FSM::enable_debug();
    }
  }
  AsyncStateMachine() = default;
  AsyncStateMachine(const AsyncStateMachine&) = default;
  AsyncStateMachine(AsyncStateMachine&&) = default;

  void register_items(T& source_item, T& sink_item) {
    std::scoped_lock lock(FSM::mutex_);
    source_item_ = &source_item;
    sink_item_ = &sink_item;
  }

  void deregister_items(T& source_item, T& sink_item) {
    std::scoped_lock lock(FSM::mutex_);
    if (source_item_ != &source_item || sink_item_ != &sink_item) {
      throw std::runtime_error(
          "Attempting to deregister source and sink items that were not "
          "registered.");
    }
    source_item_ = nullptr;
    sink_item_ = nullptr;
  }

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
   * Function for handling `sink_swap` action.
   */
  inline void on_sink_swap(lock_type&, std::atomic<int>& event) {
    // { state == full_empty }
    CHECK(FSM::state() == PortState::full_empty);
    CHECK(*source_item_ != EMPTY_SINK);

    std::swap(*source_item_, *sink_item_);

    // Will fail tests without this (needed for entry_action invocations)
    // Could instead set this in the action case, or update state with next
    // state after entry processing.
    // FSM::set_state(PortState::empty_full);

    // Not needed -- the next_state_ for full_empty is empty_full
    // FSM::set_next_state(PortState::empty_full);

    // { state == empty_full }
    // CHECK(FSM::state() == PortState::empty_full);

    if (FSM::debug_enabled())
      std::cout << event++ << "  "
                << " sink done swapping items with " + str(FSM::state()) +
                       " and " + str(FSM::next_state())
                << std::endl;

    sink_swaps++;
  }

  /**
   * Function for handling `src_swap` action.
   */
  inline void on_source_swap(lock_type&, std::atomic<int>& event) {
    // { state == full_empty }
    CHECK(str(FSM::state()) == "full_empty");

    if (FSM::debug_enabled())
      std::cout << event++ << "  "
                << " source swapping items with " + str(FSM::state()) +
                       " and " + str(FSM::next_state())
                << std::endl;

    CHECK(*source_item_ != EMPTY_SINK);

    std::swap(*source_item_, *sink_item_);

    // Needed
    // FSM::set_state(PortState::empty_full);

    // Not needed?
    // FSM::set_next_state(PortState::empty_full);

    // { state == empty_full }
    // CHECK(str(FSM::state()) == "empty_full");

    if (FSM::debug_enabled())
      std::cout << event++ << "  "
                << " source done swapping items with " + str(FSM::state()) +
                       " and " + str(FSM::next_state())
                << std::endl;

    source_swaps++;
  }

  inline void on_sink_wait(lock_type& lock, std::atomic<int>& event) {
    CHECK(str(FSM::state()) == "empty_empty");

    if (FSM::debug_enabled())
      std::cout << event++ << "  "
                << " sink going to sleep on_sink_swap with " + str(FSM::state())
                << std::endl;
    sink_cv_.wait(lock);

    // Will deadlock without this
    // FSM::set_next_state(FSM::state());

    CHECK(is_sink_post_swap(FSM::state()) == "");

    if (FSM::debug_enabled())
      std::cout << event++ << "  "
                << " sink waking up on_sink_swap with " + str(FSM::state()) +
                       " and " + str(FSM::next_state())
                << std::endl;

    if (FSM::debug_enabled())
      std::cout << event++ << "  "
                << " sink leaving on_sink_swap with " + str(FSM::state()) +
                       " and " + str(FSM::next_state())
                << std::endl;
  }

  inline void on_source_wait(lock_type& lock, std::atomic<int>& event) {
    CHECK(str(FSM::state()) == "full_full");

    if (FSM::debug_enabled())
      std::cout << event++ << "  "
                << " source going to sleep on_source_swap with " +
                       str(FSM::state()) + " and " + str(FSM::next_state())
                << std::endl;

    source_cv_.wait(lock);

    // { state == empty_empty ∨ state == empty_full }
    // FSM::set_next_state(FSM::state());

    CHECK(is_source_post_swap(FSM::state()) == "");

    if (FSM::debug_enabled())
      std::cout << event++ << "  "
                << " source waking up to " + str(FSM::state()) + " and " +
                       str(FSM::next_state())
                << std::endl;

    if (FSM::debug_enabled())
      std::cout << event++ << "  "
                << " source leaving on_source_swap with " + str(FSM::state()) +
                       " and " + str(FSM::next_state())
                << std::endl;
  }
};

/**
 * An asynchronous state machine class.  Implements on_sink_swap and
 * on_source_swap using locks and condition variables.  This class is similar to
 * `AsyncStateMachine`, but takes advantage of the fact that the notify and swap
 * functions are the same, and uses just a single implementation of them, along
 * with just a single condition variable
 *
 * @note This class includes a fair amount of debugging code.
 *
 * @todo Investigate coroutine-like approach for corresponding with implementing
 * actions so that the procession of steps is driven by the state machine rather
 * than the user of the state machine.
 */
template <class T>
class UnifiedAsyncStateMachine
    : public PortFiniteStateMachine<UnifiedAsyncStateMachine<T>> {
  using FSM = PortFiniteStateMachine<UnifiedAsyncStateMachine<T>>;
  using lock_type = typename FSM::lock_type;
  std::condition_variable cv_;

 public:
  size_t source_swaps{};
  size_t sink_swaps{};

  T* source_item_{nullptr};
  T* sink_item_{nullptr};

  bool debug_{false};

  UnifiedAsyncStateMachine(T& source_init, T& sink_init, bool debug = false)
      : source_item_(&source_init)
      , sink_item_(&sink_init)
      , debug_{debug} {
    if (debug)
      FSM::enable_debug();
    if (debug_)
      std::cout << "\nConstructing UnifiedAsyncStateMachine" << std::endl;
  }

  UnifiedAsyncStateMachine(const UnifiedAsyncStateMachine&) = default;
  UnifiedAsyncStateMachine(UnifiedAsyncStateMachine&&) = default;

  void register_items(T& source_item, T& sink_item) {
    source_item_ = &source_item;
    sink_item_ = &sink_item;
  }

  void deregister_items(T& source_item, T& sink_item) {
    if (source_item_ != &source_item || sink_item_ != &sink_item) {
      throw std::runtime_error(
          "Attempting to deregister source and sink items that were not "
          "registered.");
    }
    source_item_ = nullptr;
    sink_item_ = nullptr;
  }

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
   * Function for handling `source_swap` action.
   */
  inline void on_source_swap(lock_type&, std::atomic<int>& event) {
    if (debug_)
      std::cout << event++ << "  "
                << " source swapping items " << *source_item_ << " and "
                << *sink_item_ << std::endl;

    CHECK(*source_item_ != EMPTY_SINK);

    std::swap(*source_item_, *sink_item_);

    // FSM::set_state(PortState::empty_full);

    // FSM::set_next_state(PortState::empty_full);
    source_swaps++;
  }

  /**
   * Function for handling `sink_swap` action.  It simply calls the source swap
   * action.
   */
  inline void on_sink_swap(lock_type& lock, std::atomic<int>& event) {
    on_source_swap(lock, event);
  }

  /**
   * Function for handling `source_swap` action.
   */
  inline void on_source_wait(lock_type& lock, std::atomic<int>&) {
    cv_.wait(lock);
    // { state == empty_empty ∨ state == empty_full }

    // FSM::set_next_state(FSM::state());
  }

  /**
   * Function for handling `sink_swap` action.  It simply calls the source swap
   * action.
   */
  inline void on_sink_wait(lock_type& lock, std::atomic<int>& event) {
    on_source_wait(lock, event);
  }
};

/**
 * A simple debugging action policy that simply prints that an action has been
 * called.
 */
template <class T = size_t>
class DebugStateMachine : public PortFiniteStateMachine<DebugStateMachine<T>> {
  using FSM = PortFiniteStateMachine<DebugStateMachine<T>>;

  using lock_type = typename FSM::lock_type;

 public:
  inline void on_ac_return(lock_type&, std::atomic<int>&) {
    if (FSM::debug_enabled())
      std::cout << "    "
                << "Action return" << std::endl;
  }
  inline void on_source_swap(lock_type&, std::atomic<int>&) {
    if (FSM::debug_enabled())

      std::cout << "    "
                << "Action swap source" << std::endl;
  }
  inline void on_sink_swap(lock_type&, std::atomic<int>&) {
    if (FSM::debug_enabled())
      std::cout << "    "
                << "Action swap sink" << std::endl;
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
  inline void notify_source(lock_type&, std::atomic<int>&) {
    if (FSM::debug_enabled())
      std::cout << "    "
                << "Action notify source" << std::endl;
  }
  inline void notify_sink(lock_type&, std::atomic<int>&) {
    if (FSM::debug_enabled())
      std::cout << "    "
                << "Action notify sink" << std::endl;
  }
};

/**
 * Debug action policy with some non-copyable elements (to verify
 * compilation).
 */
class DebugStateMachineWithLock
    : public PortFiniteStateMachine<DebugStateMachineWithLock> {
  std::mutex mutex_;
  std::condition_variable sink_cv_;
  std::condition_variable source_cv_;
  using lock_type = std::unique_lock<std::mutex>;

 public:
  inline void on_ac_return(lock_type&, std::atomic<int>&) {
    std::cout << "    "
              << "Action return" << std::endl;
  }
  inline void on_source_swap(lock_type&, std::atomic<int>&) {
    std::cout << "    "
              << "Action swap source" << std::endl;
  }
  inline void on_sink_swap(lock_type&, std::atomic<int>&) {
    std::cout << "    "
              << "Action swap sink" << std::endl;
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
#endif  // TILEDB_DAG_POLICIES_H
