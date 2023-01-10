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

#include <array>
#include <cassert>
#include <optional>
#include <string>
#include <tuple>

#include "experimental/tiledb/common/dag/state_machine/fsm.h"

namespace tiledb::common {

// clang-format off
/**
 *
 * State machine policies work with the `PortFiniteStateMachine` class template to
 * mix in functions associated with various entry and exit actions invoked when
 * processing state transition events.  The policy classes defined here use CRTP
 * to effect the mixins.
 *
 * The policy classes themselves are in turn parameterized by a data mover class, which
 * is the class responsible for actually transferring data from a source to a sink (or
 * from a source to a sink along an edge).  The data mover class also inherits from
 * the policy class using CRTP.  Thus, we have the data mover inheriting from the
 * policy, which inherits from the state machine -- and the state machine being
 * parameterized by the policy, and the policy being parameterized by the data mover.
 *
 * With this chain of classes, the final class in the chain (the data mover) is what the ports use. 
 *
 * @example
 *
 * Consider the following declarations (of a data mover, a policy, and the state machine):
 * ```
 * template <template <class, class> class Policy, class PortState, class Block>
 * class ItemMover;
 * 
 * template <class Mover, class PortState>
 * class AsyncPolicy;
 *
 * template <class Policy, class PortState>
 * class PortFiniteStateMachine;
 * ```
 *
 * We can construct a concrete class's for data movement as follows:
 * ```
 * using Mover = ItemMover<AsyncPolicy, two_stage, size_t>;
 * ```
 *
 * If for some reason we also wanted explicit Policy or Machine classes (though 
 * we probably wouldn't except for debugging), we could do so:
 *```
 * using Policy = AsyncPolicy<Mover, two_stage>;
 * using Machine = PortFiniteStateMachine<Policy, two_stage>;
 *```
 * Note that because of CRTP, defining a concrete policy class requires a concrete 
 * data mover class.  Note also that even though `two_stage` is part of the `Mover` 
 * class, we still have to pass it explicitly along up (down?) the stack because of 
 * some of the instantiation details of CRTP.)
 *
 * Currently, the policy actions defined for use by the PortFiniteStateMachine class,
 * and associated mixin functions (provided by the policy class) are
 *   ac_return: on_ac_return()
 *   src_move: on_source_move()
 *   sink_move: on_sink_move()
 *   notify_source: on_notify_source()
 *   notify_sink: on_notify_sink()
 *   source_available: on_source_available()
 *   sink_available: on_sink_available()
 *   done: on_term_source(), on_term_sink()
 *
 * NB: With our current approach, we seem to really only need single functions for 
 * wait, notify, and move, so we may be able to condense this in the future.
 * For potential future flexibility, we are leaving separate source and sink versions
 * for now. The `UnifiedAsyncStateMachine` tests out this idea by using a single
 * implementation of move for both source_move and sink_move, a single implementation
 * of notify for both notify_source and notify_sink, and a single implementation of
 * wait for both source_wait and sink_wait.
 *
 * With our definition of the state machine (see fsm.h), these actions have the
 * following functionality:
 *   on_ac_return(): empty function at the moment.
 *   on_source_move(): invoke move function associated with data mover
 *   on_sink_move(): invoke move function associated with data mover
 *   on_notify_source(): notify sink
 *   on_notify_sink(): notify source
 *   on_source_available(): put source into waiting
 *   on_sink_available(): put sink into waiting state
 *   on_term_source(): put source into waiting state
 *   on_term_sink(): put sink into waiting state
 *
 * The implementations of `AsyncStateMachine` and `UnifiedAsyncStateMachine` 
 * currently use locks and condition variable for effecting the wait (and notify) functions.
 *
 * A crucial piece of functionality in these classes is the move operation that may take place.
 * When operating with `Source` and `Sink` ports, we need to be able to move the item_ member
 * variables associated with the `Source` and a `Sink` that are bound to each other.  To
 * enable this, the data mover maintains pointers to items (which items are assumed to be
 * `std::optional` of some underlying type.  When moving is required, `std::swap` is invoked 
 * between an empty item and a full item.  These internal pointers are initialized with a 
 * `register_items` function, and are reset with a `deregister_items` function.
 */
// clang-format on

/**
 * Null action policy.  Verifies compilation of CRTP.  All functions except
 * registration are empty.
 */
template <class Mover, class PortState>
class NullPolicy
    : public PortFiniteStateMachine<NullPolicy<Mover, PortState>, PortState> {
  using state_machine_type =
      PortFiniteStateMachine<NullPolicy<Mover, PortState>, PortState>;
  using lock_type = typename state_machine_type::lock_type;

 public:
  using scheduler_event_type = SchedulerAction;

  inline scheduler_event_type on_ac_return(lock_type&, std::atomic<int>&) {
    return {};
  }
  inline scheduler_event_type on_source_move(lock_type&, std::atomic<int>&) {
    return {};
  }
  inline scheduler_event_type on_sink_move(lock_type&, std::atomic<int>&) {
    return {};
  }
  inline scheduler_event_type on_notify_source(lock_type&, std::atomic<int>&) {
    return {};
  }
  inline scheduler_event_type on_notify_sink(lock_type&, std::atomic<int>&) {
    return {};
  }
  inline scheduler_event_type on_source_wait(lock_type&, std::atomic<int>&) {
    return {};
  }
  inline scheduler_event_type on_sink_wait(lock_type&, std::atomic<int>&) {
    return {};
  }
  inline scheduler_event_type on_term_source(lock_type&, std::atomic<int>&) {
    return {};
  }
  inline scheduler_event_type on_term_sink(lock_type&, std::atomic<int>&) {
    return {};
  }
};

/**
 * A state machine for testing progress of messages using manual invocations of
 * port state machine functions.  The only non-trivial functions are
 * `on_source_move` and `on_sink_move`, both of which simply invoke
 * `std::move` on the cached items.
 */
template <class Mover, class PortState = typename Mover::PortState>
class ManualPolicy
    : public PortFiniteStateMachine<ManualPolicy<Mover, PortState>, PortState> {
  using state_machine_type =
      PortFiniteStateMachine<ManualPolicy<Mover, PortState>, PortState>;
  using lock_type = typename state_machine_type::lock_type;

  using mover_type = Mover;

 public:
  using scheduler_event_type = SchedulerAction;
  constexpr static bool wait_returns_{true};

  ManualPolicy() {
    if constexpr (std::is_same_v<PortState, two_stage>) {
      CHECK(str(static_cast<Mover*>(this)->state()) == "st_00");
    } else if constexpr (std::is_same_v<PortState, three_stage>) {
      CHECK(str(static_cast<Mover*>(this)->state()) == "st_000");
    }
  }
  inline scheduler_event_type on_ac_return(lock_type&, std::atomic<int>&) {
    return scheduler_event_type::noop;
  }
  inline scheduler_event_type on_source_move(
      lock_type&, std::atomic<int>& event) {
    static_cast<Mover*>(this)->on_move(event);
    return scheduler_event_type::noop;
  }
  inline scheduler_event_type on_sink_move(
      lock_type&, std::atomic<int>& event) {
    static_cast<Mover*>(this)->on_move(event);
    return scheduler_event_type::noop;
  }
  inline scheduler_event_type on_notify_source(lock_type&, std::atomic<int>&) {
    debug_msg("Action notify source");
    return scheduler_event_type::noop;
  }
  inline scheduler_event_type on_notify_sink(lock_type&, std::atomic<int>&) {
    debug_msg("Action notify source");
    return scheduler_event_type::noop;
  }
  inline scheduler_event_type on_source_wait(lock_type&, std::atomic<int>&) {
    debug_msg("Action source wait");
    return scheduler_event_type::noop;
  }
  inline scheduler_event_type on_sink_wait(lock_type&, std::atomic<int>&) {
    debug_msg("Action sink wait");
    return scheduler_event_type::noop;
  }
  inline scheduler_event_type on_term_source(lock_type&, std::atomic<int>&) {
    debug_msg("Action source done");
    return scheduler_event_type::noop;
  }
  inline scheduler_event_type on_term_sink(lock_type&, std::atomic<int>&) {
    debug_msg("Action sink done");
    return scheduler_event_type::noop;
  }

 private:
  void debug_msg(const std::string& msg) {
    if (static_cast<Mover*>(this)->debug_enabled()) {
      std::cout << msg << std::endl;
    }
  }
};

/**
 * An asynchronous policy class.  Implements wait and notify functions
 * using locks and condition variables.
 *
 * It is assumed that the source and sink are running as separate asynchronous
 * tasks (in this case, effected by `std::async`).
 *
 * @todo Investigate coroutine-like approach for corresponding with implementing
 * actions so that the procession of steps is driven by the state machine rather
 * than the user of the state machine.
 */
template <class Mover, class PortState = typename Mover::PortState>
class AsyncPolicy
    : public PortFiniteStateMachine<AsyncPolicy<Mover, PortState>, PortState> {
  using mover_type = Mover;

  std::condition_variable sink_cv_;
  std::condition_variable source_cv_;

  std::array<size_t, 2> moves_{0, 0};

  using scheduler_event_type = SchedulerAction;

 public:
  constexpr static bool wait_returns_{true};

  using state_machine_type =
      PortFiniteStateMachine<AsyncPolicy<Mover, PortState>, PortState>;
  using lock_type = typename state_machine_type::lock_type;

  AsyncPolicy() = default;
  AsyncPolicy(const AsyncPolicy&) {
  }
  AsyncPolicy(AsyncPolicy&&) = default;

  /**
   * Function for handling `ac_return` action.
   */
  inline scheduler_event_type on_ac_return(lock_type&, std::atomic<int>&) {
    return scheduler_event_type::noop;
  }

  /**
   * Function for handling `source_move` action.
   */
  inline scheduler_event_type on_source_move(
      lock_type&, std::atomic<int>& event) {
    moves_[0]++;
    static_cast<Mover*>(this)->on_move(event);
    return scheduler_event_type::noop;
  }

  /**
   * Function for handling `sink_move` action.
   */
  inline scheduler_event_type on_sink_move(
      lock_type&, std::atomic<int>& event) {
    moves_[1]++;
    static_cast<Mover*>(this)->on_move(event);
    return scheduler_event_type::noop;
  }

  size_t source_swaps() const {
    return moves_[0];
  }

  size_t sink_swaps() const {
    return moves_[1];
  }

  /**
   * Function for handling `notify_source` action.
   */
  inline scheduler_event_type on_notify_source(lock_type&, std::atomic<int>&) {
    assert(is_sink_empty(this->state()) == "");

    source_cv_.notify_one();
    return scheduler_event_type::notify_source;
  }

  /**
   * Function for handling `notify_sink` action.
   */
  inline scheduler_event_type on_notify_sink(lock_type&, std::atomic<int>&) {
    // This CHECK will fail when state machine is stopping, so check
    if (!static_cast<Mover*>(this)->is_stopping()) {
      assert(is_source_full(this->state()) == "");
    }

    sink_cv_.notify_one();
    return scheduler_event_type::notify_source;
  }

  /**
   * Function for handling `source_available` action.
   */
  inline scheduler_event_type on_source_wait(
      lock_type& lock, std::atomic<int>&) {
    if constexpr (std::is_same_v<PortState, two_stage>) {
      assert(str(this->state()) == "st_11");
    } else if constexpr (std::is_same_v<PortState, three_stage>) {
      assert(str(this->state()) == "st_111");
    }
    source_cv_.wait(lock);

    assert(is_source_post_move(this->state()) == "");

    return scheduler_event_type::source_wait;
  }

  /**
   * Function for handling `sink_available` action.
   */
  inline scheduler_event_type on_sink_wait(lock_type& lock, std::atomic<int>&) {
    sink_cv_.wait(lock);

    assert(is_sink_post_move(this->state()) == "");

    return scheduler_event_type::sink_wait;
  }

  /**
   * Function for handling `term_source` action.
   */
  inline scheduler_event_type on_term_source(
      lock_type& lock, std::atomic<int>& event) {
    // @note This is not optimal.  Have to notify sink when source is ending b/c
    // can't do it with throw_catch in fsm.h
    on_notify_sink(lock, event);
    return scheduler_event_type::source_exit;
  }

  /**
   * Function for handling `term_sink` action.
   */
  inline scheduler_event_type on_term_sink(lock_type&, std::atomic<int>&) {
    return scheduler_event_type::noop;
  }

 private:
  void debug_msg(const std::string& msg) {
    if (static_cast<Mover*>(this)->debug_enabled()) {
      std::cout << msg << std::endl;
    }
  }
};  // namespace tiledb::common

/**
 * An asynchronous policy class.  Implements on_sink_move and
 * on_source_move using locks and condition variables.  This class is similar
 * to `AsyncPolicy`, but takes advantage of the fact that the wait, notify, and
 * move functions are the same, and uses just a single implementation of them,
 * along with just a single condition variable
 *
 * @note This class includes a fair amount of debugging code.
 *
 * @todo Investigate coroutine-like approach for corresponding with implementing
 * actions so that the procession of steps is driven by the state machine rather
 * than the user of the state machine.
 */
template <class Mover, class PortState = typename Mover::PortState>
class UnifiedAsyncPolicy : public PortFiniteStateMachine<
                               UnifiedAsyncPolicy<Mover, PortState>,
                               PortState> {
  using mover_type = Mover;
  using scheduler_event_type = SchedulerAction;

  std::condition_variable cv_;

 public:
  constexpr static bool wait_returns_{true};

  using state_machine_type =
      PortFiniteStateMachine<UnifiedAsyncPolicy<Mover, PortState>, PortState>;
  using lock_type = typename state_machine_type::lock_type;

  UnifiedAsyncPolicy() = default;
  UnifiedAsyncPolicy(const UnifiedAsyncPolicy&) {
  }
  UnifiedAsyncPolicy(UnifiedAsyncPolicy&&) noexcept = default;

  /**
   * Function for handling `ac_return` action.
   */
  inline scheduler_event_type on_ac_return(lock_type&, std::atomic<int>&) {
    return scheduler_event_type::noop;
  };

  /**
   * Function for handling `source_move` action.
   */
  inline scheduler_event_type on_source_move(
      lock_type&, std::atomic<int>& event) {
    static_cast<Mover*>(this)->on_move(event);
    return scheduler_event_type::noop;
  }

  /**
   * Function for handling `sink_move` action.  It simply calls the source
   * move action.
   */
  inline scheduler_event_type on_sink_move(
      lock_type& lock, std::atomic<int>& event) {
    return on_source_move(lock, event);
  }

  /**
   * Single  notify function for source and sink.
   */
  inline void task_notify(lock_type&, std::atomic<int>&) {
    cv_.notify_one();
  }

  /**
   * Function for handling `notify_source` action, invoking a `task_notify`
   * function.
   */
  inline scheduler_event_type on_notify_source(
      lock_type& lock, std::atomic<int>& event) {
    task_notify(lock, event);
    return scheduler_event_type::notify_source;
  }

  /**
   * Function for handling `notify_sink` action, invoking a `task_notify`
   * function.
   */
  inline scheduler_event_type on_notify_sink(
      lock_type& lock, std::atomic<int>& event) {
    task_notify(lock, event);
    return scheduler_event_type::notify_source;
  }

  /**
   * Function for handling `source_move` action.
   */
  inline scheduler_event_type on_source_wait(
      lock_type& lock, std::atomic<int>&) {
    cv_.wait(lock);
    return scheduler_event_type::source_wait;
  }

  /**
   * Function for handling `sink_available` action.  It simply calls the source
   * wait action.
   */
  inline scheduler_event_type on_sink_wait(lock_type& lock, std::atomic<int>&) {
    cv_.wait(lock);
    return scheduler_event_type::sink_wait;
  }

  /**
   * Function for handling `term_source` action.
   */
  inline scheduler_event_type on_term_source(
      lock_type& lock, std::atomic<int>& event) {
    assert(lock.owns_lock());

    // @note This is not optimal.  Have to notify sink when source is ending b/c
    // can't do it with throw_catch in fsm.h
    on_notify_sink(lock, event);
    return scheduler_event_type::source_exit;
  }

  /**
   * Function for handling `sink_available` action.  It simply calls the source
   * wait action.
   */
  inline scheduler_event_type on_term_sink(
      lock_type& lock, std::atomic<int>& event) {
    on_term_source(lock, event);
    return scheduler_event_type::sink_exit;
  }

 private:
  void debug_msg(const std::string& msg) {
    if (static_cast<Mover*>(this)->debug_enabled()) {
      std::cout << msg << std::endl;
    }
  }
};

/**
 * A simple action policy useful for debugging that simply prints that an
 * action has been called.
 */
template <class Mover, class PortState = typename Mover::PortState>
class DebugPolicy
    : public PortFiniteStateMachine<DebugPolicy<Mover, PortState>, PortState> {
  void debug_msg(const std::string& msg) {
    if (static_cast<Mover*>(this)->debug_enabled()) {
      std::cout << msg << std::endl;
    }
  }

  using scheduler_event_type = SchedulerAction;

 public:
  constexpr static bool wait_returns_{true};

  template <class lock_type>
  inline scheduler_event_type on_ac_return(lock_type&, std::atomic<int>&) {
    debug_msg("    Action return");
    return scheduler_event_type::noop;
  }
  template <class lock_type>
  inline scheduler_event_type on_source_move(lock_type&, std::atomic<int>&) {
    debug_msg("    Action move source");
    return scheduler_event_type::noop;
  }
  template <class lock_type>
  inline scheduler_event_type on_sink_move(lock_type&, std::atomic<int>&) {
    debug_msg("    Action move sink");
    return scheduler_event_type::noop;
  }
  template <class lock_type>
  inline scheduler_event_type on_notify_source(lock_type&, std::atomic<int>&) {
    debug_msg("    Action notify source");
    return scheduler_event_type::notify_source;
  }
  template <class lock_type>
  inline scheduler_event_type on_notify_sink(lock_type&, std::atomic<int>&) {
    debug_msg("    Action notify sink");
    return scheduler_event_type::notify_sink;
  }
  template <class lock_type>
  inline scheduler_event_type on_source_wait(lock_type&, std::atomic<int>&) {
    debug_msg("    Action source wait");
    return scheduler_event_type::source_wait;
  }
  template <class lock_type>
  inline scheduler_event_type on_sink_wait(lock_type&, std::atomic<int>&) {
    debug_msg("    Action sink wait");
    return scheduler_event_type::sink_wait;
  }
  template <class lock_type>
  inline scheduler_event_type on_term_source(lock_type&, std::atomic<int>&) {
    debug_msg("    Action source wait");
    return scheduler_event_type::source_exit;
  }
  template <class lock_type>
  inline scheduler_event_type on_term_sink(lock_type&, std::atomic<int>&) {
    debug_msg("    Action sink wait");
    return scheduler_event_type::sink_exit;
  }
};

/**
 *
 * Debug action policy with some non-copyable elements (to verify
 * compilation).
 */
template <class Mover, class PortState = typename Mover::PortState>
class DebugPolicyWithLock
    : public PortFiniteStateMachine<DebugPolicy<Mover, PortState>, PortState> {
  void debug_msg(const std::string& msg) {
    if (static_cast<Mover*>(this)->debug_enabled()) {
      std::cout << msg << std::endl;
    }
  }
  std::mutex mutex_;
  std::condition_variable sink_cv_;
  std::condition_variable source_cv_;

  using scheduler_event_type = SchedulerAction;

 public:
  constexpr static bool wait_returns_{true};

  template <class lock_type>
  inline scheduler_event_type on_ac_return(lock_type&, std::atomic<int>&) {
    debug_msg("    Action return");
    return scheduler_event_type::noop;
  }
  template <class lock_type>
  inline scheduler_event_type on_source_move(lock_type&, std::atomic<int>&) {
    debug_msg("    Action move source");
    return scheduler_event_type::noop;
  }
  template <class lock_type>
  inline scheduler_event_type on_sink_move(lock_type&, std::atomic<int>&) {
    debug_msg("    Action move sink");
    return scheduler_event_type::noop;
  }
  template <class lock_type>
  inline scheduler_event_type on_notify_source(lock_type&, std::atomic<int>&) {
    debug_msg("    Action notify source");
    return scheduler_event_type::notify_source;
  }
  template <class lock_type>
  inline scheduler_event_type on_notify_sink(lock_type&, std::atomic<int>&) {
    debug_msg("    Action notify sink");
    return scheduler_event_type::notify_sink;
  }
  template <class lock_type>
  inline scheduler_event_type on_source_wait(lock_type&, std::atomic<int>&) {
    debug_msg("    Action source wait");
    return scheduler_event_type::source_wait;
  }
  template <class lock_type>
  inline scheduler_event_type on_sink_wait(lock_type&, std::atomic<int>&) {
    debug_msg("    Action sink wait");
    return scheduler_event_type::sink_wait;
  }
  template <class lock_type>
  inline scheduler_event_type on_term_source(lock_type&, std::atomic<int>&) {
    debug_msg("    Action source wait");
    return scheduler_event_type::source_exit;
  }
  template <class lock_type>
  inline scheduler_event_type on_term_sink(lock_type&, std::atomic<int>&) {
    debug_msg("    Action sink wait");
    return scheduler_event_type::sink_exit;
  }
};

}  // namespace tiledb::common
#endif  // TILEDB_DAG_POLICIES_H
