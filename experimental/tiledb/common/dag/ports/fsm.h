/**
 * @file   fsm.h
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
 * This file declares the finite state machine for communicating ports.
 *
 */

#ifndef TILEDB_DAG_FSM_H
#define TILEDB_DAG_FSM_H

#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

namespace tiledb::common {

// clang-format off
/**
 * This is the state transition table for a simple state machine for two
 * communicating ports.  Each port has two states, empty or full.  There are
 * three events: source_fill, sink_drain, and swap.  For simplicty there are
 * currently no defined events for startup, stop, forced shutdown, or abort
 * (though we have an event enum that anticipates shutdown).
 *
 *    +-----------------+---------------------------------------------------+
 *    |      States     |                    Events                         |
 *    +--------+--------+-------------+------------+-------------+----------+
 *    | Source |  Sink  | source_fill | swap       | sink_drain  | shutdown |
 *    |--------+--------+-------------+------------+-------------+----------+
 *    | empty  | empty  | full/empty  |            |             |          |
 *    |--------+--------+-------------+------------+-------------+----------+
 *    | empty  | full   | full/full   |            | empty/empty |          |
 *    |--------+--------+-------------+------------+-------------+----------+
 *    | full   | empty  |             | empty/full |             |          |
 *    |--------+--------+-------------+------------+-------------+----------+
 *    | full   | full   |             |            | full/empty  |          |
 *    +--------+--------+-------------+------------+-------------+----------+
 *
 *
 *
 * The basic behavior of a user of a source port is
 *   init: { state == empty_full }
 *   start: { state == empty_full /\ state == empty_empty }
 *   while (not done)
 *     loop_begin: { state == empty_empty /\ state == empty_full }
 *     pre_produce: { state == empty_empty /\ state == empty_full }
 *     invoke producer function
 *     during producer function, sink could have drained
 *     source_fill: { state == full_empty /\ state == full_full }
 *     sink.notify()
 *     wait
 *     post wait, the following could have happened
 *       sink could have swapped -> state = empty_full
 *       sink could have drained -> state = full_empty
 *     post_wait: { state == full_empty /\ state == empty_full }
 *     if (state == full_empty)
 *       pre_swap: { state ==  full_empty }
 *       swap elements
 *       post_swap: { state ==  empty_full }
 *       sink.notify()
 *     else
 *       else_swap: { state == empty_empty \/ state == empty_full
 *   post_loop: { state == empty_empty \/ state == empty_full }
 *
 *
 * The basic behavior of a user of a sink port is
 *   We perform some initializetion to get into a proper starting state
 *   init: { state == empty_full /\ state == full_full }
 *   drain: { state == empty_empty /\ state == full_empty }
 *   start: { state == empty_empty /\ state == full_empty }
 *   source.notify()
 *   while (not done)
 *     loop_begin: { state == empty_empty /\ state == full_empty }
 *     wait
 *     post wait, the following could have happened
 *       source could have swapped -> state = empty_full
 *       source could have filled  -> full_empty
 *     post_wait: { state == full_empty /\ state == empty_full }
 *     if (state == full_empty)
 *       pre_swap: { state == full_empty }
 *       swap
 *       post_swap: { state ==  empty_full }
 *       source.notify()
 *     else
 *       else_swap: { state == full_empty /\ state == empty_full }
 *     pre_consume: { state == empty_full /\ state == full_full }
 *     invoke consumer function
 *     during consumer function, source could have filled
 *     drain: { state == empty_empty /\ state == full_empty }
 *     source.notify()
 *  post_loop: { state = empty_empty }
 *
 *
 */
// clang-format on

/**
 * An enum representing the different states of the bound ports
 */
enum class PortState {
  empty_empty,
  empty_full,
  full_empty,
  full_full,
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
    "empty_empty",
    "empty_full",
    "full_empty",
    "full_full",
    "error",
    "done",
};

/**
 * Function to convert a state to a string.
 *
 * @param event The event to stringify.
 * @return The string corresponding to the event.
 */
static auto str(PortState st) {
  return port_state_strings[static_cast<int>(st)];
}

enum class PortEvent : unsigned short {
  source_fill,
  swap,
  sink_drain,
  shutdown
};

inline constexpr unsigned short to_index(PortEvent x) {
  return static_cast<unsigned short>(x);
}

/**
 * Number of events in the PortEvent state machine
 */
constexpr unsigned int n_events = to_index(PortEvent::shutdown) + 1;

/**
 * Strings for each enum member, for debugging.
 */
static std::vector<std::string> event_strings{
    "source_fill",
    "swap",
    "sink_drain",
    "shutdown",
};

/**
 * Function to convert event to a string.
 *
 * @param event The event to stringify.
 * @return The string corresponding to the event.
 */
static auto str(PortEvent ev) {
  return event_strings[static_cast<int>(ev)];
}

/**
 * State transition table.
 *
 * Originally included start events and separate sink_drained and source_filled
 * events. The start events are not necessary for a connected source and sink
 * if the initial state of the fsm is empty_full. The source_fill and
 * source_filled are really the same event, as are sink_drained and
 * sink_drain. Without start events, there are also no start states.
 *
 * The original fsm also included a "ready" state for source and sink, but this
 * was absorbed into "full" for source and "empty" for sink since the transition
 * from ready to those states wwas "always", i.e., it did not require an actual
 * event for that state transition to happen.  Moreover, the state of the source
 * and sink is really just the state of the item it contains.
 *
 */
namespace {

// clang-format off
  constexpr PortState transition_table[n_states][n_events] {
  /* source_sink */    /* source_fill */      /* swap */             /* sink_drain */        /* shutdown */

  /* empty_empty */  { PortState::full_empty, PortState::error,      PortState::error,       PortState::error },
  /* empty_full  */  { PortState::full_full,  PortState::error,      PortState::empty_empty, PortState::error },
  /* full_empty  */  { PortState::error,      PortState::empty_full, PortState::error,       PortState::error },
  /* full_full   */  { PortState::error,      PortState::error,      PortState::full_empty,  PortState::error },

  /* error       */  { PortState::error,      PortState::error,      PortState::error,       PortState::error },
  /* done        */  { PortState::error,      PortState::error,      PortState::error,       PortState::error },
  };
// clang-format on

  }  // namespace

/**
 * Class representing states of a bound source and sink node.
 * The class is agnostic as to how the events are actually
 * implemented by the users of the the state machine.
 *
 * Todo: Derived class to represent bound nodes.
 */
class PortStateMachine {
 private:
  PortState state_;

 public:
  /**
   * Default constructor
   */
  PortStateMachine()
      : state_(PortState::empty_full){};

  /**
   * Return the current state
   */
  [[nodiscard]] inline PortState state() const {
    return state_;
  }

 public:
  /**
   * Function to handle state transitions based on external events.
   *
   * Todo: Protect state transitions with mutex?
   *
   * Todo: Make event() private and create do_fill(), do_drain(),
   * and do_swap() public members.
   *
   * Todo: Investigate coroutine-like approach for corresponding with external
   * events so that the procession of steps is driven by the state machine
   * rather than the user of the state machine.
   *
   * Some code that prints state information is currently included for debugging
   * purposes.
   *
   * @param event The event to be processed
   * @param event msg A debugging string to preface printout information for the
   * state transition
   */
  void event(PortEvent event, const std::string msg = "") {
    auto new_state{transition_table[to_index(state_)][to_index(event)]};

    if (msg != "") {
      std::cout << msg + " " + str(event) + ": " + str(state_) + " -> " +
                       str(new_state)
                << std::endl;
    }

    /*
     * Assign new state
     */
    state_ = new_state;
  }
};
}  // namespace tiledb::common

#endif  // TILEDB_DAG_FSM_H
