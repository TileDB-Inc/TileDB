/**
 * @file   fsm3.h
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
 *
 * This file defines the operation of a finite state machine with 2^3 states,
 * one state for each binary number in [0, 2^3).
 *
 */

#ifndef TILEDB_DAG_FSM3_H
#define TILEDB_DAG_FSM3_H

#include <atomic>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string>
#include <vector>

namespace tiledb::common {

/**
 * An enum representing the different states of the bound ports.
 */
enum class PortState {
  st_000,
  st_001,
  st_010,
  st_011,
  st_100,
  st_101,
  st_110,
  st_111,
  error,
  done
};

namespace {
constexpr unsigned short to_index(PortState x) {
  return static_cast<unsigned short>(x);
}

/**
 * Number of states in the Port state machine
 */
constexpr unsigned short n_states = to_index(PortState::done) + 1;
}  // namespace

/**
 * Strings for each enum member, for debugging.
 */
static std::vector<std::string> port_state_strings{
    "st_000",
    "st_001",
    "st_010",
    "st_011",
    "st_100",
    "st_101",
    "st_110",
    "st_111",
    "error",
    "done",
};

/**
 * Function to convert a state to a string.
 *
 * @param event The event to stringify.
 * @return The string corresponding to the event.
 */
static inline auto str(PortState st) {
  return port_state_strings[static_cast<int>(st)];
}

/**
 * enum class for the state machine events.
 */
enum class PortEvent : unsigned short {
  source_fill,
  source_push,
  sink_drain,
  sink_pull,
  shutdown,
};

inline constexpr unsigned short to_index(PortEvent x) {
  return static_cast<unsigned short>(x);
}

/**
 * Number of events in the PortEvent state machine
 */
constexpr unsigned int n_events = to_index(PortEvent::shutdown) + 1;

/**
 * Strings for each enum member, useful for debugging.
 */
static std::vector<std::string> event_strings{
    "source_fill",
    "source_push",
    "sink_drain",
    "sink_pull",
    "shutdown",
};

/**
 * Function to convert event to a string.
 *
 * @param event The event to stringify.
 * @return The string corresponding to the event.
 */
static inline auto str(PortEvent ev) {
  return event_strings[static_cast<int>(ev)];
}

/**
 * enum class for port actions associated with transitions.
 */
enum class PortAction : unsigned short {
  none,
  ac_return,
  source_move,
  sink_move,
  notify_source,
  notify_sink,
  source_wait,
  sink_wait,
  error,
};

inline constexpr unsigned short to_index(PortAction x) {
  return static_cast<unsigned short>(x);
}

/**
 * Number of actions in the Port state machine
 */
constexpr unsigned int n_actions = to_index(PortAction::error) + 1;

/**
 * Strings for each enum member, useful for debugging.
 */
static std::vector<std::string> action_strings{
    "none",
    "ac_return",
    "source_move",
    "sink_move",
    "notify_source",
    "notify_sink",
    "source_wait",
    "sink_wait",
    "error",
};

/**
 * Function to convert an action to a string.
 *
 * @param ac The action to stringify.
 * @return The string corresponding to the action.
 */
static auto inline str(PortAction ac) {
  return action_strings[static_cast<int>(ac)];
}

/**
 * Tables for state transitions, exit events, and entry events.  Indexed by
 * state and event.
 */
namespace {

// clang-format off

constexpr const PortState transition_table[n_states][n_events] {
  /* state  */  /* source_fill */   /* source_push */  /* sink_drain */   /* sink_pull */    /* shutdown */

  /* st_000 */ { PortState::st_100, PortState::st_000, PortState::error,  PortState::st_000, PortState::error },
  /* st_001 */ { PortState::st_101, PortState::st_001, PortState::st_000, PortState::st_001, PortState::error },
  /* st_010 */ { PortState::st_110, PortState::st_001, PortState::error,  PortState::st_001, PortState::error },
  /* st_011 */ { PortState::st_111, PortState::st_011, PortState::st_010, PortState::st_011, PortState::error },
  /* st_100 */ { PortState::error,  PortState::st_001, PortState::error,  PortState::st_001, PortState::error },
  /* st_101 */ { PortState::error,  PortState::st_011, PortState::st_100, PortState::st_011, PortState::error },
  /* st_110 */ { PortState::error,  PortState::st_011, PortState::error,  PortState::st_011, PortState::error },
  /* st_111 */ { PortState::error,  PortState::st_111, PortState::st_110, PortState::st_111, PortState::error },

  /* error  */ { PortState::error,  PortState::error,  PortState::error,  PortState::error,  PortState::error },
  /* done   */ { PortState::error,  PortState::error,  PortState::error,  PortState::error,  PortState::error },
};

constexpr const PortAction exit_table[n_states][n_events] {
  /* state  */  /* source_fill */  /* source_push */        /* sink_drain */  /* sink_pull */        /* shutdown */

  /* st_000 */ { PortAction::none, PortAction::none,        PortAction::none, PortAction::sink_wait, PortAction::none },
  /* st_001 */ { PortAction::none, PortAction::none,        PortAction::none, PortAction::none,      PortAction::none },
  /* st_010 */ { PortAction::none, PortAction::source_move, PortAction::none, PortAction::sink_move, PortAction::none },
  /* st_011 */ { PortAction::none, PortAction::none,        PortAction::none, PortAction::none,      PortAction::none },
  /* st_100 */ { PortAction::none, PortAction::source_move, PortAction::none, PortAction::sink_move, PortAction::none },
  /* st_101 */ { PortAction::none, PortAction::source_move, PortAction::none, PortAction::sink_move, PortAction::none },
  /* st_110 */ { PortAction::none, PortAction::source_move, PortAction::none, PortAction::sink_move, PortAction::none },
  /* st_111 */ { PortAction::none, PortAction::source_wait, PortAction::none, PortAction::none,      PortAction::none },

  /* error  */ { PortAction::none, PortAction::none,        PortAction::none, PortAction::none,      PortAction::none },
  /* done   */ { PortAction::none, PortAction::none,        PortAction::none, PortAction::none,      PortAction::none },
};

constexpr const PortAction entry_table[n_states][n_events] {
  /* state  */  /* source_fill */         /* source_push */        /* sink_drain */           /* sink_pull */        /* shutdown */

  /* st_000 */ { PortAction::none,        PortAction::none,        PortAction::notify_source, PortAction::none,      PortAction::none },
  /* st_001 */ { PortAction::none,        PortAction::none,        PortAction::none,          PortAction::none,      PortAction::none },
  /* st_010 */ { PortAction::none,        PortAction::source_move, PortAction::notify_source, PortAction::sink_move, PortAction::none },
  /* st_011 */ { PortAction::none,        PortAction::none,        PortAction::none,          PortAction::none,      PortAction::none },
  /* st_100 */ { PortAction::notify_sink, PortAction::source_move, PortAction::notify_source, PortAction::sink_move, PortAction::none },
  /* st_101 */ { PortAction::notify_sink, PortAction::source_move, PortAction::none,          PortAction::sink_move, PortAction::none },
  /* st_110 */ { PortAction::notify_sink, PortAction::source_move, PortAction::notify_source, PortAction::sink_move, PortAction::none },
  /* st_111 */ { PortAction::notify_sink, PortAction::none,        PortAction::none,          PortAction::none,      PortAction::none },

  /* error  */ { PortAction::none,        PortAction::none,        PortAction::none,          PortAction::none,      PortAction::none },
  /* done   */ { PortAction::none,        PortAction::none,        PortAction::none,          PortAction::none,      PortAction::none },
};
// clang-format on

}  // namespace

/**
 * Class template representing states of a bound source and sink node.  The
 * class is agnostic as to how the actions are actually implemented by the users
 * of the the state machine.  A policy class is used by the
 * `PortFinitStateMachine` to realize the specific state transition actions.
 *
 * The `ActionPolicy` class is expected to use the `FiniteStateMachine` class
 * using CRTP. Documentation about action policy classes can be found in
 * policies.h.
 *
 * @note There is a fair amount of debugging code inserted into the class at the
 * moment.
 *
 * @todo Use an aspect class (as another template argument) to effect callbacks
 * at each interesting point in the state machine.
 */
template <class ActionPolicy>
class PortFiniteStateMachine {
 private:
  PortState state_;
  PortState next_state_;

 public:
  using lock_type = std::unique_lock<std::mutex>;

  /**
   * Default constructor
   */
  PortFiniteStateMachine()
      : state_(PortState::st_000) {
  }

/**
 * Return the current state
 */
[[nodiscard]] inline PortState
state() const {
  return state_;
}

  /**
   * Return the next state
   */
  [[nodiscard]] inline PortState next_state() const {
    return next_state_;
  }

 private:
  /**
   * Function to handle state transitions based on external events.
   *
   * The function is protected by a mutex.  Exit and entry actions
   * may use the lock (for example, to wait on condition variables,
   * so the lock is passed to each action.
   *
   * Todo: Make event() private and create do_fill(), do_push(),
   * do_drain(), and do_pull() as public members.
   *
   * Some code that prints state information is currently included for
   * debugging purposes.
   *
   * @param event The event to be processed
   * @param event msg A debugging string to preface printout information for
   * the state transition
   */

  // Used for debugging.
  std::atomic<int> event_counter{};
  bool debug_{false};

 protected:
  std::mutex mutex_;

 private:
  void event(PortEvent event, const std::string msg = "") {
    std::unique_lock lock(mutex_);

    next_state_ = transition_table[to_index(state_)][to_index(event)];
    auto exit_action{exit_table[to_index(state_)][to_index(event)]};
    auto entry_action{entry_table[to_index(next_state_)][to_index(event)]};

    auto old_state = state_;

    if (msg != "" || debug_) {
      std::cout << "\n"
                << event_counter++
                << " On event start: " + msg + " " + str(event) + ": " +
                       str(state_) + " (" + str(exit_action) + ") -> (" +
                       str(entry_action)
                << ") " + str(next_state_) << std::endl;
    }

    // For now, ignore shutdown events
    if (PortEvent::shutdown == event) {
      return;
    }

    if (next_state_ == PortState::error) {
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
     * Perform any exit actions.
     */
    switch (exit_action) {
      case PortAction::none:
        break;

      case PortAction::ac_return:
        if (msg != "")
          std::cout << event_counter++
                    << "      " + msg + " exit about to ac_return" << std::endl;
        static_cast<ActionPolicy&>(*this).on_ac_return(lock, event_counter);
        return;
        break;

      case PortAction::source_move:
        if (msg != "")
          std::cout << event_counter++
                    << "      " + msg + " exit about to source_move"
                    << std::endl;
        static_cast<ActionPolicy&>(*this).on_source_move(lock, event_counter);
        break;

      case PortAction::sink_move:
        if (msg != "")
          std::cout << event_counter++
                    << "      " + msg + " exit about to sink_move" << std::endl;
        static_cast<ActionPolicy&>(*this).on_sink_move(lock, event_counter);
        break;

      case PortAction::source_wait:
        if (msg != "")
          std::cout << event_counter++
                    << "      " + msg + " exit about to source_wait"
                    << std::endl;
        static_cast<ActionPolicy&>(*this).on_source_wait(lock, event_counter);
        break;

      case PortAction::sink_wait:
        if (msg != "")
          std::cout << event_counter++
                    << "      " + msg + " exit about to sink_wait" << std::endl;
        static_cast<ActionPolicy&>(*this).on_sink_wait(lock, event_counter);
        break;

      case PortAction::notify_source:
        if (msg != "")
          std::cout << event_counter++
                    << "      " + msg + " exit about to notify source"
                    << std::endl;
        static_cast<ActionPolicy&>(*this).notify_source(lock, event_counter);
        break;

      case PortAction::notify_sink:
        if (msg != "")
          std::cout << event_counter++
                    << "      " + msg + " exit about to notify sink"
                    << std::endl;
        static_cast<ActionPolicy&>(*this).notify_sink(lock, event_counter);
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
     * Assign new state.  Note that next_state_ may have been changed by one of
     * the actions above (in particular, wait or move).
     */
    state_ = next_state_;

    /*
     * Update the entry_action in case next_state_ was changed.
     */
    entry_action = entry_table[to_index(next_state_)][to_index(event)];

    if (msg != "" || debug_) {
      std::cout << event_counter++
                << " Pre entry event: " + msg + " " + str(event) + ": " +
                       str(old_state) + " (" + str(exit_action) + ") -> (" +
                       str(entry_action)
                << ") " + str(state_) << std::endl;
    }

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

        static_cast<ActionPolicy&>(*this).on_ac_return(lock, event_counter);
        return;
        break;

      case PortAction::source_move:

        if (msg != "")
          std::cout << event_counter++
                    << "      " + msg + " entry about to source_move"
                    << std::endl;

        static_cast<ActionPolicy&>(*this).on_source_move(lock, event_counter);
        switch (state_) {
          case PortState::st_010:
            // Fall through
          case PortState::st_100:
            state_ = PortState::st_001;
            break;
          case PortState::st_110:
            // Fall through
          case PortState::st_101:
            state_ = PortState::st_011;
            break;
          default:
            break;
        }
        break;

      case PortAction::sink_move:

        if (msg != "")
          std::cout << event_counter++
                    << "      " + msg + " entry about to sink_move"
                    << std::endl;

        static_cast<ActionPolicy&>(*this).on_sink_move(lock, event_counter);
        switch (state_) {
          case PortState::st_010:
            // Fall through
          case PortState::st_100:
            state_ = PortState::st_001;
            break;
          case PortState::st_110:
            // Fall through
          case PortState::st_101:
            state_ = PortState::st_011;
            break;
          default:
            break;
        }

        break;

      case PortAction::source_wait:
        if (msg != "")
          std::cout << event_counter++
                    << "      " + msg + " exit about to source_wait"
                    << std::endl;
        static_cast<ActionPolicy&>(*this).on_source_wait(lock, event_counter);
        break;

      case PortAction::sink_wait:
        if (msg != "")
          std::cout << event_counter++
                    << "      " + msg + " entry about to sink_wait"
                    << std::endl;
        static_cast<ActionPolicy&>(*this).on_sink_wait(lock, event_counter);
        break;

      case PortAction::notify_source:
        if (msg != "")
          std::cout << event_counter++
                    << "      " + msg + " entry about to notify source"
                    << std::endl;
        static_cast<ActionPolicy&>(*this).notify_source(lock, event_counter);
        break;

      case PortAction::notify_sink:
        if (msg != "")
          std::cout << event_counter++
                    << "      " + msg + " entry about to notify sink"
                    << std::endl;
        static_cast<ActionPolicy&>(*this).notify_sink(lock, event_counter);
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
   * Set state
   */
  inline PortState set_state(PortState next_state_) {
    state_ = next_state_;
    return state_;
  }

  /**
   * Set next state
   */
  inline PortState set_next_state(PortState next_state) {
    next_state_ = next_state;
    return next_state_;
  }

  /**
   * Invoke `source_fill` event
   */
  void do_fill(const std::string& msg = "") {
    event(PortEvent::source_fill, msg);
  }

  /**
   * Invoke `source_push` event
   */
  void do_push(const std::string& msg = "") {
    event(PortEvent::source_push, msg);
  }

  /**
   * Invoke `sink_drain` event
   */
  void do_drain(const std::string& msg = "") {
    event(PortEvent::sink_drain, msg);
  }

  /**
   * Invoke `sink_pull` event
   */
  void do_pull(const std::string& msg = "") {
    event(PortEvent::sink_pull, msg);
  }

  /**
   * Invoke `shutdown` event
   */
  void do_shutdown(const std::string& msg = "") {
    event(PortEvent::shutdown, msg);
  }

  /**
   * Invoke `out_of_data` event
   */
  void out_of_data(const std::string&) {
    // unimplemented
  }

  void enable_debug() {
    debug_ = true;
  }

  void disable_debug() {
    debug_ = false;
  }

  constexpr inline bool debug_enabled() const {
    return debug_;
  }
};

}  // namespace tiledb::common

#endif  // TILEDB_DAG_FSM3_H
