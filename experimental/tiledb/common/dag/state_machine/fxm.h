/*
 * @file   fxm.h
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
 * This file defines the operation of a finite state machine, parameterized to
 * accommodate 2 stage or 3 stage data transfer, i.e., 4 or 8 states.  Each
 * state corresponds to a binary number in [0, 2^N) (for N = 2 or N = 3).
 *
 * @note This state machine was orginally intended for use with stateful nodes
 * and a Duff's device scheduler.  This initial implementation does not include
 * is_*_available as an explicit state machine action. As a result, it neither
 * blocks nor moves data. This is incorrect behavior. The is_*_available
 * functions should block as well as move items.  Which is exactly equivalent to
 * port_push and port_pull. Hence, a correct fxm state machine will be
 * equivalent to the fsm state machine, obviating the need for this state
 * machine.
 */

#ifndef TILEDB_DAG_FXM_H
#define TILEDB_DAG_FXM_H

/*
 * Preprocessor directive to admit small differences in behavior in other parts
 * of the state machine, policy, mover hierarchy.
 */
#define FXM
#include "experimental/tiledb/common/dag/state_machine/fsm_types.h"

namespace tiledb::common {

/**
 * Tables for state transitions, exit events, and entry events.  Indexed by
 * state and event.
 */
template <class PortState>
PortState transition_table[num_states<PortState>][n_events];

template <class PortState>
PortAction exit_table[num_states<PortState>][n_events];

template <class PortState>
PortAction entry_table[num_states<PortState>][n_events];

// clang-format off
/**
 * Specialization of `transition_table` for `two_stage`.
 */
template<>
constexpr const two_stage
transition_table<two_stage>[num_states<two_stage>][n_events] {
  /* state */   /*source_inject*/ /*sink_extract*/  /* stop */

  /* st_00 */ { two_stage::st_01, two_stage::error, two_stage::xt_00 },
  /* st_01 */ { two_stage::st_11, two_stage::st_00, two_stage::xt_01 },
  /* st_10 */ { two_stage::error, two_stage::error, two_stage::xt_10 },
  /* st_11 */ { two_stage::error, two_stage::st_01, two_stage::xt_11 },

  /* xt_00 */ { two_stage::error, two_stage::error, two_stage::error },
  /* xt_01 */ { two_stage::error, two_stage::xt_00, two_stage::error },
  /* xt_10 */ { two_stage::error, two_stage::error, two_stage::error },
  /* xt_11 */ { two_stage::error, two_stage::xt_01, two_stage::error },

  /* done  */ { two_stage::error, two_stage::error, two_stage::error },
  /* error */ { two_stage::error, two_stage::error, two_stage::error },

  /* last */  { two_stage::error, two_stage::error, two_stage::error },
};


/**
 * Specialization of `exit_table` for `two_stage`.
 */
template<>
constexpr const PortAction exit_table<two_stage>[num_states<two_stage>][n_events] {
  /* state */   /* source_inject */      /* sink_extract */     /* stop */

  /* st_00 */ { PortAction::source_move, PortAction::none,      PortAction::notify_sink },
  /* st_01 */ { PortAction::none,        PortAction::none,      PortAction::notify_sink },
  /* st_10 */ { PortAction::source_move, PortAction::sink_move, PortAction::notify_sink },
  /* st_11 */ { PortAction::none,        PortAction::sink_move, PortAction::notify_sink },

  /* xt_00 */ { PortAction::none,        PortAction::none,      PortAction::none },
  /* xt_01 */ { PortAction::none,        PortAction::none,      PortAction::none },
  /* xt_10 */ { PortAction::none,        PortAction::sink_move, PortAction::none },
  /* xt_11 */ { PortAction::none,        PortAction::sink_move, PortAction::none },

  /* done  */ { PortAction::none,        PortAction::none,      PortAction::none },
  /* error */ { PortAction::none,        PortAction::none,      PortAction::none },

  /* last */  { PortAction::none,        PortAction::none,      PortAction::none },
};

/**
 * Specialization of `entry_table` for `two_stage`.
 */
template<>
constexpr const PortAction entry_table<two_stage> [num_states<two_stage>][n_events] {
  /* state */   /* source_fill */        /* source_push */        /* sink_drain */           /* sink_pull */        /* stop */

  /* st_00 */ { PortAction::none,        PortAction::notify_source, PortAction::none },
  /* st_01 */ { PortAction::notify_sink, PortAction::notify_source, PortAction::none },
  /* st_10 */ { PortAction::none,        PortAction::none,          PortAction::none },
  /* st_11 */ { PortAction::notify_sink, PortAction::none,          PortAction::none },

  /* xt_00 */ { PortAction::none,        PortAction::none,          PortAction::term_source },
  /* xt_01 */ { PortAction::none,        PortAction::notify_source, PortAction::term_source },
  /* xt_10 */ { PortAction::none,        PortAction::none,          PortAction::term_source },
  /* xt_11 */ { PortAction::none,        PortAction::none,          PortAction::term_source },

  /* done  */ { PortAction::none,        PortAction::term_sink,     PortAction::none },
  /* error */ { PortAction::none,        PortAction::none,          PortAction::none },

  /* last */  { PortAction::none,        PortAction::none,          PortAction::none },
};

/**
 * Specialization of `transition_table` for `three_stage`.
 */
template<>
constexpr const three_stage
transition_table<three_stage>[num_states<three_stage>][n_events] {
  /* state  */   /* source_inject */  /* sink_extract */    /* sink_drain */     /* sink_pull */      /* stop */

  /* st_000 */ { three_stage::st_001, three_stage::error,  three_stage::xt_000,},
  /* st_001 */ { three_stage::st_011, three_stage::st_000, three_stage::xt_001,},
  /* st_010 */ { three_stage::st_011, three_stage::error,  three_stage::xt_010,},
  /* st_011 */ { three_stage::st_111, three_stage::st_001, three_stage::xt_011,},
  /* st_100 */ { three_stage::error,  three_stage::error,  three_stage::xt_100,},
  /* st_101 */ { three_stage::error,  three_stage::st_001, three_stage::xt_101,},
  /* st_110 */ { three_stage::error,  three_stage::error,  three_stage::xt_110,},
  /* st_111 */ { three_stage::error,  three_stage::st_011, three_stage::xt_111,},

  /* xt_000 */ { three_stage::error,  three_stage::error,  three_stage::error,},
  /* xt_001 */ { three_stage::error,  three_stage::xt_001, three_stage::error,},
  /* xt_010 */ { three_stage::error,  three_stage::error,  three_stage::error,},
  /* xt_011 */ { three_stage::error,  three_stage::xt_001, three_stage::error,},
  /* xt_100 */ { three_stage::error,  three_stage::error,  three_stage::error,},
  /* xt_101 */ { three_stage::error,  three_stage::xt_001, three_stage::error,},
  /* xt_110 */ { three_stage::error,  three_stage::error,  three_stage::error,},
  /* xt_111 */ { three_stage::error,  three_stage::xt_011, three_stage::error,},

  /* done   */ { three_stage::error,  three_stage::error,  three_stage::error },
  /* error  */ { three_stage::error,  three_stage::error,  three_stage::error },

  /* last  */  { three_stage::error,  three_stage::error,  three_stage::error },
};

/**
 * Specialization of `exit_table` for `three_stage`.
 */
template<>
constexpr const PortAction exit_table<three_stage>[num_states<three_stage>][n_events] {
  /* state  */  /* source_inject */       /* sink_extract */     /* stop */

  /* st_000 */ { PortAction::none,        PortAction::none,      PortAction::notify_sink },
  /* st_001 */ { PortAction::none,        PortAction::none,      PortAction::notify_sink },
  /* st_010 */ { PortAction::source_move, PortAction::sink_move, PortAction::notify_sink },
  /* st_011 */ { PortAction::none,        PortAction::none,      PortAction::notify_sink },
  /* st_100 */ { PortAction::source_move, PortAction::sink_move, PortAction::notify_sink },
  /* st_101 */ { PortAction::source_move, PortAction::sink_move, PortAction::notify_sink },
  /* st_110 */ { PortAction::source_move, PortAction::sink_move, PortAction::notify_sink },
  /* st_111 */ { PortAction::none,        PortAction::none,      PortAction::notify_sink },

  /* xt_000 */ { PortAction::none,        PortAction::none,      PortAction::none },
  /* xt_001 */ { PortAction::none,        PortAction::none,      PortAction::none },
  /* xt_010 */ { PortAction::none,        PortAction::sink_move, PortAction::none },
  /* xt_011 */ { PortAction::none,        PortAction::none,      PortAction::none },
  /* xt_100 */ { PortAction::none,        PortAction::sink_move, PortAction::none },
  /* xt_101 */ { PortAction::none,        PortAction::sink_move, PortAction::none },
  /* xt_110 */ { PortAction::none,        PortAction::sink_move, PortAction::none },
  /* xt_111 */ { PortAction::none,        PortAction::none,      PortAction::none },

  /* done   */ { PortAction::none,        PortAction::none,      PortAction::none },
  /* error  */ { PortAction::none,        PortAction::none,      PortAction::none },

  /* last   */ { PortAction::none,        PortAction::none,      PortAction::none },
};

/**
 * Specialization of `entry_table` for `three_stage`.
 */
template<>
constexpr const PortAction entry_table<three_stage>[num_states<three_stage>][n_events] {
  /* state  */  /* source_inject */       /* sink_extract */         /* stop */

  /* st_000 */ { PortAction::notify_sink, PortAction::none, PortAction::none },
  /* st_001 */ { PortAction::notify_sink, PortAction::notify_source,          PortAction::none },
  /* st_010 */ { PortAction::notify_sink, PortAction::none, PortAction::none },
  /* st_011 */ { PortAction::notify_sink, PortAction::notify_source,          PortAction::none },
  /* st_100 */ { PortAction::none,        PortAction::none, PortAction::none },
  /* st_101 */ { PortAction::none,        PortAction::notify_source,          PortAction::none },
  /* st_110 */ { PortAction::none,        PortAction::none, PortAction::none },
  /* st_111 */ { PortAction::none,        PortAction::notify_source,          PortAction::none },

  /* xt_000 */ { PortAction::none,        PortAction::none,          PortAction::term_source },
  /* xt_001 */ { PortAction::none,        PortAction::none,          PortAction::term_source },
  /* xt_010 */ { PortAction::none,        PortAction::notify_source, PortAction::term_source },
  /* xt_011 */ { PortAction::none,        PortAction::none,          PortAction::term_source },
  /* xt_100 */ { PortAction::none,        PortAction::notify_source, PortAction::term_source },
  /* xt_101 */ { PortAction::none,        PortAction::none,          PortAction::term_source },
  /* xt_110 */ { PortAction::none,        PortAction::notify_source, PortAction::term_source },
  /* xt_111 */ { PortAction::none,        PortAction::none,          PortAction::term_source },

  /* done   */ { PortAction::none,        PortAction::term_sink,     PortAction::none },
  /* error  */ { PortAction::none,        PortAction::none,          PortAction::none },

  /* last   */ { PortAction::none,        PortAction::none,          PortAction::none },

};
// clang-format on

/**
 * Class template representing states of a bound source and sink node (in the
 * two stage case) or the states of a bound source, sink node, and intermediary
 * (in the three stage case).  The class supplies an `event` function for
 * effecting state transitions (including application of exit and entry
 * functions).  The class is agnostic as to how the actions are actually
 * implemented by the users of the state machine.  A policy class is used by the
 * `PortFinitStateMachine` to realize the specific state transition actions.  It
 * is assumed the the Policy class inherits from `PortFiniteStateMachine` using
 * CRTP.
 *
 * Extended documentation for the operation of the state machine for the
 * two-stage case can be found in * fxm.md.
 *
 * The `Policy` class is expected to use the `PortFiniteStateMachine` class
 * using CRTP so that functions provided by the `Policy` class are available to
 * the `PortFiniteStateMachine`. Documentation about action policy classes can
 * be found in policies.h.
 *
 * @note There is a fair amount of debugging code inserted into the class at the
 * moment.
 *
 * @todo Clean up the debugging code as we gain more confidence/experiene with
 * the state machine (et al).
 *
 * @todo Use an aspect class (as another template argument) to effect callbacks
 * at each interesting point in the state machine (such as debugging
 * statements).
 */
template <class Policy, class PortState>
class PortFiniteStateMachine {
 private:
  PortState state_;
  PortState next_state_;

 public:
  using port_state = PortState;
  using lock_type = std::unique_lock<std::mutex>;

  /**
   * Default constructor
   */
  PortFiniteStateMachine()
      : state_(static_cast<port_state>(0)) {
  }

  /**
   * Copy constructor -- completely bogus, but needed for move requirement.
   * `PortFiniteStateMachine` objects cannot actually be copied because of its
   * `mutex` member.
   */
  PortFiniteStateMachine(const PortFiniteStateMachine&) {
  }

  /**
   * Return the current state
   */
  [[nodiscard]] inline auto state() const {
    return state_;
  }

  /**
   * Return the next state
   */
  [[nodiscard]] inline auto next_state() const {
    return next_state_;
  }

 private:
  // Used for debugging.
  inline static std::atomic<int> event_counter{};
  bool debug_{false};

  std::mutex mutex_;

 protected:
  /**
   * Function to handle state transitions based on external events.
   *
   * The function is protected by a mutex.  Exit and entry actions
   * may use the lock (for example, to wait on condition variables,
   * so the lock is passed to each action.
   *
   * Note that even processing proceeds as:
   *  - Process exit function for current state
   *  - Transition to new state
   *  - Process entry function for new state
   *
   * Some code that prints state information is currently included for
   * debugging purposes.
   *
   * @param event The event to be processed
   * @param msg A debugging string to preface printout information for
   * the state transition
   */
  void event(PortEvent event, const std::string msg = "") {
    std::unique_lock lock(mutex_);

    /*
     * Look up next state, along with exit and entry action.
     */
    next_state_ =
        transition_table<port_state>[to_index(state_)][to_index(event)];
    auto exit_action{exit_table<port_state>[to_index(state_)][to_index(event)]};
    auto entry_action{
        entry_table<port_state>[to_index(next_state_)][to_index(event)]};

    if (msg != "" || debug_) {
      std::cout << "\n"
                << event_counter++
                << " On event start: " + msg + " " + str(event) + ": " +
                       str(state_) + " (" + str(exit_action) + ") -> (" +
                       str(entry_action)
                << ") " + str(next_state_) << std::endl;
    }

    if ((msg != "" || debug_) && (next_state_ == port_state::error)) {
      std::cout << "\n"
                << event_counter++
                << " ERROR On event start: " + msg + " " + str(event) + ": " +
                       str(state_) + " (" + str(exit_action) + ") -> (" +
                       str(entry_action)
                << ") " + str(next_state_) << std::endl;
    }

    if (msg != "" || debug_) {
      std::cout << event_counter++
                << " Pre exit event: " + msg + " " + str(event) + ": " +
                       str(state_) + " (" + str(exit_action) + ") -> (" +
                       str(entry_action)
                << ") " + str(next_state_) << std::endl;
    }

    /**
     * Perform any exit actions.  Based on the exit action in the exit action
     * table, we dispatch to a function provided by the policy.  Since Policy is
     * assumed to be derived from this class, we use a `static_cast` to
     * `Policy*` on `this`.
     */
    switch (exit_action) {
      case PortAction::none:
        break;

      case PortAction::ac_return:
        if (msg != "")
          std::cout << event_counter++
                    << "      " + msg + " exit about to ac_return" << std::endl;
        static_cast<Policy*>(this)->on_ac_return(lock, event_counter);
        return;
        break;

      case PortAction::source_move:
        if (msg != "")
          std::cout << event_counter++
                    << "      " + msg + " exit about to source_move"
                    << std::endl;
        static_cast<Policy*>(this)->on_source_move(lock, event_counter);
        break;

      case PortAction::sink_move:
        if (msg != "")
          std::cout << event_counter++
                    << "      " + msg + " exit about to sink_move" << std::endl;
        static_cast<Policy*>(this)->on_sink_move(lock, event_counter);
        break;

      case PortAction::notify_source:
        if (msg != "")
          std::cout << event_counter++
                    << "      " + msg + " exit about to notify source"
                    << std::endl;
        static_cast<Policy*>(this)->on_notify_source(lock, event_counter);
        break;

      case PortAction::notify_sink:
        if (msg != "")
          std::cout << event_counter++
                    << "      " + msg + " exit about to notify sink"
                    << std::endl;
        static_cast<Policy*>(this)->on_notify_sink(lock, event_counter);
        break;

      case PortAction::term_source:
        if (msg != "")
          std::cout << event_counter++
                    << "      " + msg + " exit about to term_source"
                    << std::endl;
        static_cast<Policy*>(this)->on_term_source(lock, event_counter);
        break;

      case PortAction::term_sink:
        if (msg != "")
          std::cout << event_counter++
                    << "      " + msg + " exit about to term_sink" << std::endl;
        static_cast<Policy*>(this)->on_term_sink(lock, event_counter);
        break;

      default:
        throw std::logic_error(
            "Unexpected exit action: " + str(exit_action) + ": " + str(state_) +
            " -> " + str(next_state_));
    }

    if (msg != "" || debug_) {
      if (msg != "")
        std::cout << event_counter++
                  << " Post exit: " + msg + " " + str(event) + ": " +
                         str(state_) + " (" + str(exit_action) + ") -> (" +
                         str(entry_action)
                  << ") " + str(next_state_) << std::endl;
    }

    /*
     * Assign new state.
     */
    state_ = next_state_;

    /**
     * Perform any entry actions.
     */
    switch (entry_action) {
      case PortAction::none:
        break;

      case PortAction::ac_return:

        if (msg != "")
          std::cout << event_counter++
                    << "      " + msg + " entry about to ac_return"
                    << std::endl;

        static_cast<Policy*>(this)->on_ac_return(lock, event_counter);
        return;
        break;

      case PortAction::notify_source:
        if (msg != "")
          std::cout << event_counter++
                    << "      " + msg + " entry about to notify source"
                    << std::endl;
        static_cast<Policy*>(this)->on_notify_source(lock, event_counter);
        break;

      case PortAction::notify_sink:
        if (msg != "")
          std::cout << event_counter++
                    << "      " + msg + " entry about to notify sink"
                    << std::endl;
        static_cast<Policy*>(this)->on_notify_sink(lock, event_counter);
        break;

      case PortAction::term_source:
        if (msg != "")
          std::cout << event_counter++
                    << "      " + msg + " entry about to term_source"
                    << std::endl;
        static_cast<Policy*>(this)->on_term_source(lock, event_counter);
        break;

      case PortAction::term_sink:
        if (msg != "")
          std::cout << event_counter++
                    << "      " + msg + " entry about to term_sink"
                    << std::endl;
        static_cast<Policy*>(this)->on_term_sink(lock, event_counter);
        break;

      default:
        throw std::logic_error(
            "Unexpected entry action: " + str(entry_action) + ": " +
            str(state_) + " -> " + str(next_state_));
    }

    if (msg != "" || debug_) {
      std::cout << event_counter++
                << " Post entry event: " + msg + " " + str(event) + ": " +
                       str(state_) + " (" + str(exit_action) + ") -> (" +
                       str(entry_action)
                << ") " + str(next_state_) << std::endl;
    }
  }

 public:
  /**
   * Set state.
   */
  inline auto set_state(port_state next_state_) {
    state_ = next_state_;
    return state_;
  }

  /**
   * Set next state.
   */
  inline auto set_next_state(port_state next_state) {
    next_state_ = next_state;
    return next_state_;
  }

  /**
   * Functions for invoking events in the StateMachine.  In general, clients of
   * the StateMachine will be using it via a mover class.  These are here to
   * enable direct testing of the state machine.
   */

  /**
   * Invoke `do_inject` event.
   */
  void do_inject(const std::string& msg = "") {
    event(PortEvent::source_inject, msg);
  }

  /**
   * Invoke `do_extract` event.
   */
  void do_extract(const std::string& msg = "") {
    event(PortEvent::sink_extract, msg);
  }

  /**
   * Invoke `source_available` event.
   */
  bool do_source_available(const std::string&) {
    std::unique_lock lock(mutex_);
    return static_cast<Policy*>(this)->on_source_available(lock, event_counter);
  }

  /**
   * Invoke `sink_available` event.
   */
  bool do_sink_available(const std::string&) {
    std::unique_lock lock(mutex_);
    return static_cast<Policy*>(this)->on_sink_available(lock, event_counter);
  }

  /**
   * Invoke `stop` event
   */
  void port_exhausted(const std::string& msg = "") {
    this->event(PortEvent::stop, msg);
  }

  /**
   * Invoke `out_of_data` event.
   */
  void out_of_data(const std::string&) {
    // unimplemented
  }

  /**
   * Turn on debugging output.
   */
  void enable_debug() {
    debug_ = true;
  }

  /**
   * Turn off debugging output.
   */
  void disable_debug() {
    debug_ = false;
  }

  /**
   * Check if debugging has been enabled.
   */
  constexpr inline bool debug_enabled() const {
    return debug_;
  }
};

}  // namespace tiledb::common

#endif  // TILEDB_DAG_FXM_H
