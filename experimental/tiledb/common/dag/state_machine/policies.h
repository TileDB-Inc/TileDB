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
 * The policy classes themselves are in turn paramterized by a data mover class, which
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
 * We can construct a concrete classs for data movement as follows:
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
 *   source_wait: on_source_wait()
 *   sink_wait: on_sink_wait()
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
 *   on_source_wait(): put source into waiting state
 *   on_sink_wait(): put sink into waiting state
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

#ifdef NDEBUG
#define _unused(x)
#else
#define _unused(x) x
#endif

template <class Mover, class PortState>
class AsyncPolicy;

/**
 * Null action policy.  Verifies compilation of CRTP.  All functions except
 * registration are empty.
 */
template <class Mover, class PortState>
class NullPolicy
    : public PortFiniteStateMachine<NullPolicy<Mover, PortState>, PortState> {
  using state_machine_type =
      PortFiniteStateMachine<AsyncPolicy<Mover, PortState>, PortState>;
  using lock_type = typename state_machine_type::lock_type;

  inline void on_ac_return(lock_type&, std::atomic<int>&) {
  }
  inline void on_source_move(lock_type&, std::atomic<int>&) {
  }
  inline void on_sink_move(lock_type&, std::atomic<int>&) {
  }
  inline void on_notify_source(lock_type&, std::atomic<int>&) {
  }
  inline void on_notify_sink(lock_type&, std::atomic<int>&) {
  }
  inline void on_source_wait(lock_type&, std::atomic<int>&) {
  }
  inline void on_sink_wait(lock_type&, std::atomic<int>&) {
  }
};

/**
 * A state machine for testing progress of messages using manual invocations of
 * port state machine functions.  The only non-trivial functions are
 * `on_source_move` and `on_sink_move`, both of which simply invoke
 * `std::move` on the cached items.
 */
template <class Mover, class PortState>
class ManualPolicy
    : public PortFiniteStateMachine<ManualPolicy<Mover, PortState>, PortState> {
  using state_machine_type =
      PortFiniteStateMachine<AsyncPolicy<Mover, PortState>, PortState>;
  using lock_type = typename state_machine_type::lock_type;

  using mover_type = Mover;

 public:
  ManualPolicy() {
    if constexpr (std::is_same_v<PortState, two_stage>) {
      CHECK(str(static_cast<Mover*>(this)->state()) == "st_00");
    } else if constexpr (std::is_same_v<PortState, three_stage>) {
      CHECK(str(static_cast<Mover*>(this)->get_state()) == "st_000");
    }
  }
  inline void on_ac_return(lock_type&, std::atomic<int>&) {
    debug_msg("Action return");
  }
  inline void on_source_move(lock_type&, std::atomic<int>& event) {
    static_cast<Mover*>(this)->on_move(event);
  }

  inline void on_sink_move(lock_type&, std::atomic<int>& event) {
    static_cast<Mover*>(this)->on_move(event);
  }
  inline void on_notify_source(lock_type&, std::atomic<int>&) {
    debug_msg("Action notify source");
  }
  inline void on_notify_sink(lock_type&, std::atomic<int>&) {
    debug_msg("Action notify source");
  }
  inline void on_source_wait(lock_type&, std::atomic<int>&) {
    debug_msg("Action source wait");
  }

  inline void on_sink_wait(lock_type&, std::atomic<int>&) {
    debug_msg("Action sink wait");
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

template <class Mover, class PortState>
class AsyncPolicy
    : public PortFiniteStateMachine<AsyncPolicy<Mover, PortState>, PortState>

{
  using mover_type = Mover;
  using state_machine_type =
      PortFiniteStateMachine<AsyncPolicy<Mover, PortState>, PortState>;
  using lock_type = typename state_machine_type::lock_type;

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
  inline void on_ac_return(lock_type&, std::atomic<int>&) {
  }

  /**
   * Function for handling `notify_source` action.
   */
  inline void on_notify_source(
      lock_type& _unused(lock), std::atomic<int>& event) {
    assert(lock.owns_lock());
    debug_msg(
        std::to_string(event++) +
        "    sink notifying source (on_signal_source) with " +
        str(this->state()) + " and " + str(this->next_state()));

    CHECK(is_sink_empty(this->state()) == "");
    source_cv_.notify_one();
  }

  /**
   * Function for handling `notify_sink` action.
   */
  inline void on_notify_sink(
      lock_type& _unused(lock), std::atomic<int>& event) {
    assert(lock.owns_lock());

    debug_msg(
        std::to_string(event++) +
        "    source notifying sink(on_signal_sink) with " + str(this->state()) +
        " and " + str(this->next_state()));

    CHECK(is_source_full(this->state()) == "");
    sink_cv_.notify_one();
  }

  /**
   * Function for handling `sink_move` action.
   */
  inline void on_sink_move(lock_type& _unused(lock), std::atomic<int>& event) {
    assert(lock.owns_lock());

    debug_msg(
        std::to_string(event++) + "    sink moving (on_sink_move) with " +
        str(this->state()) + " and " + str(this->next_state()));

    static_cast<Mover*>(this)->on_move(event);
  }

  /**
   * Function for handling `source_move` action.
   */
  inline void on_source_move(
      lock_type& _unused(lock), std::atomic<int>& event) {
    assert(lock.owns_lock());

    debug_msg(
        std::to_string(event++) + "    source moving (on_source_move) with " +
        str(this->state()) + " and " + str(this->next_state()));

    static_cast<Mover*>(this)->on_move(event);
  }

  /**
   * Function for handling `sink_wait` action.
   */
  inline void on_sink_wait(lock_type& lock, std::atomic<int>& event) {
    assert(lock.owns_lock());

    if constexpr (std::is_same_v<PortState, two_stage>) {
      CHECK(str(this->state()) == "st_00");
    } else if (std::is_same_v<PortState, three_stage>) {
      CHECK(str(this->state()) == "st_000");
    }

    debug_msg(
        std::to_string(event++) + "    sink going to sleep on_sink_move with " +
        str(this->state()));
    sink_cv_.wait(lock);

    CHECK(is_sink_post_move(this->state()) == "");

    debug_msg(
        std::to_string(event++) + "    sink waking up on_sink_move with " +
        str(this->state()) + " and " + str(this->next_state()));

    debug_msg(
        std::to_string(event++) + "    sink leaving on_sink_move with " +
        str(this->state()) + " and " + str(this->next_state()));
  }

  /**
   * Function for handling `source_wait` action.
   */
  inline void on_source_wait(lock_type& lock, std::atomic<int>& event) {
    assert(lock.owns_lock());

    if constexpr (std::is_same_v<PortState, two_stage>) {
      CHECK(str(this->state()) == "st_11");
    } else if constexpr (std::is_same_v<PortState, three_stage>) {
      CHECK(str(this->state()) == "st_111");
    }

    debug_msg(
        std::to_string(event++) +
        "    source going to sleep on_source_move with " + str(this->state()) +
        " and " + str(this->next_state()));

    source_cv_.wait(lock);

    //    print_types(this->state());

    CHECK(is_source_post_move(this->state()) == "");

    debug_msg(
        std::to_string(event++) + "    source waking up to " +
        str(this->state()) + " and " + str(this->next_state()));

    debug_msg(
        std::to_string(event++) + "    source leaving on_source_move with " +
        str(this->state()) + " and " + str(this->next_state()));
  }

 private:
  void debug_msg(const std::string& msg) {
    if (static_cast<Mover*>(this)->debug_enabled()) {
      std::cout << msg << std::endl;
    }
  }
};

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
template <class Mover, class PortState>
class UnifiedAsyncPolicy : public PortFiniteStateMachine<
                               UnifiedAsyncPolicy<Mover, PortState>,
                               PortState> {
  using mover_type = Mover;
  using state_machine_type =
      PortFiniteStateMachine<AsyncPolicy<Mover, PortState>, PortState>;
  using lock_type = typename state_machine_type::lock_type;

  std::condition_variable cv_;

 public:
  UnifiedAsyncPolicy() = default;
  UnifiedAsyncPolicy(const UnifiedAsyncPolicy&) {
  }
  UnifiedAsyncPolicy(UnifiedAsyncPolicy&&) = default;

  /**
   * Function for handling `ac_return` action.
   */
  inline void on_ac_return(lock_type&, std::atomic<int>&){};

  /**
   * Single  notify function for source and sink.
   */
  inline void do_notify(lock_type&, std::atomic<int>&) {
    cv_.notify_one();
  }

  /**
   * Function for handling `notify_source` action, invoking a `do_notify`
   * function.
   */
  inline void on_notify_source(lock_type& lock, std::atomic<int>& event) {
    debug_msg(std::to_string(event++) + "    sink notifying source");
    do_notify(lock, event);
  }

  /**
   * Function for handling `notify_sink` action, invoking a `do_notify`
   * function.
   */
  inline void on_notify_sink(lock_type& lock, std::atomic<int>& event) {
    debug_msg(std::to_string(event++) + "    source notifying sink");
    do_notify(lock, event);
  }

  /**
   * Function for handling `source_move` action.
   */
  inline void on_source_move(lock_type&, std::atomic<int>& event) {
    static_cast<Mover*>(this)->on_move(event);
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
   * Function for handling `sink_wait` action.  It simply calls the source
   * wait action.
   */
  inline void on_sink_wait(lock_type& lock, std::atomic<int>& event) {
    on_source_wait(lock, event);
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
template <class Mover, class PortState>
class DebugPolicy
    : public PortFiniteStateMachine<DebugPolicy<Mover, PortState>, PortState> {
  void debug_msg(const std::string& msg) {
    if (static_cast<Mover*>(this)->debug_enabled()) {
      std::cout << msg << std::endl;
    }
  }

 public:
  template <class lock_type>
  inline void on_ac_return(lock_type&, std::atomic<int>&) {
    debug_msg("    Action return");
  }
  template <class lock_type>
  inline void on_source_move(lock_type&, std::atomic<int>&) {
    debug_msg("    Action move source");
  }
  template <class lock_type>
  inline void on_sink_move(lock_type&, std::atomic<int>&) {
    debug_msg("    Action move sink");
  }
  template <class lock_type>
  inline void on_source_wait(lock_type&, std::atomic<int>&) {
    debug_msg("    Action source wait");
  }
  template <class lock_type>
  inline void on_sink_wait(lock_type&, std::atomic<int>&) {
    debug_msg("    Action sink wait");
  }
  template <class lock_type>
  inline void on_notify_source(lock_type&, std::atomic<int>&) {
    debug_msg("    Action notify source");
  }
  template <class lock_type>
  inline void on_notify_sink(lock_type&, std::atomic<int>&) {
    debug_msg("    Action notify sink");
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
  using PortState = typename mover_type::PortState_type;

  std::mutex mutex_;
  std::condition_variable sink_cv_;
  std::condition_variable source_cv_;

 public:
  inline void on_ac_return(lock_type&, std::atomic<int>&) {
    debug_msg("    Action return");
  }
  inline void on_source_move(lock_type&, std::atomic<int>&) {
    debug_msg("    Action move source");
  }
  inline void on_sink_move(lock_type&, std::atomic<int>&) {
    debug_msg("    Action move sink");
  }
  inline void on_notify_source(lock_type&, std::atomic<int>&) {
    debug_msg("    Action notify source");
  }
  inline void on_notify_sink(lock_type&, std::atomic<int>&) {
    debug_msg("    Action notify sink");
  }

 private:
  void debug_msg(const std::string& msg) {
    if (static_cast<Mover*>(this)->debug_enabled()) {
      std::cout << msg << std::endl;
    }
  }
};

}  // namespace tiledb::common
#endif  // TILEDB_DAG_POLICIES_H
