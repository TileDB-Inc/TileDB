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

#include <atomic>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

namespace tiledb::common {

// clang-format off
/**
 *
 * This file implements a state machine for two communicating ports.  Each port has
 * two states, empty or full.  There are two events associated with the source:
 * source_fill and push.  There are two events associated with the sink: sink_drain,
 * and pull. For simplicty there are currently no defined events for startup, stop,
 * forced shutdown, or abort.
 *
 * Diagrams for the source and sink state machines can be found in
 * source_state_machine.svg and sink_state_machine.svg, respectively.  The diagrams also
 * show the entry and exit actions for each state machine.
 *
 * In words, the source state machine would be used as follows:
 *   client of the source inserts an item
 *   client invokes source_fill event to transition from empty to full.
 *     when making this transition, on entry to full, the source notifies the sink that it is 
 *     full, and returns
 *   client invokes push event to transition from full to empty, sending its item to the sink 
 *     when making the transition, on exit the source invokes its swap action
 *       the swap action first checks if its current state is full and the sinks state is empty.  
 *       If so, it swaps the sink and source items, which puts the item in the sink.  It then
 *       notifies the sink. On entry to the empty state, the sink returns
 *       If not, the soure waits until it is notified by the sink that it is empty. When the
 *       source is notified, it returns.
 *
 * The sink state machine is the dual of the source state machine:
 *   client invokes pull event to transition from empty to pull, getting its item from the source
 *     when making the transition, on exit the sik invokes its swap action
 *       the swap action first checks if its current state is full and the source state is empty.  
 *       If so, it swaps the sink and source items, which puts the item in the sink.  It then
 *       notifies the source. On entry to the empty state, the sink returns
 *       If not, the sink waits until notified by the source that it is full.  When the sink is 
 *       notified, it returns.
 *   client invokes drain event to transition from full to empty.
 *     when making this transition, on entry to empty, the sink notifies the source that it is 
 *     empty, and returns
 *
 *
 * The product state transition table is thus
 *
 *    +-----------------+------------------------------------------------------------------+
 *    |      States     |                     Events                                       |
 *    +--------+--------+-------------+-------------+-------------+-------------+----------+
 *    | Source |  Sink  | source_fill | push        | sink_drain  | pull        | shutdown |
 *    |--------+--------+-------------+-------------+-------------+-------------+----------+
 *    | empty  | empty  | full/empty  |             |             | empty/full  |          |
 *    |--------+--------+-------------+-------------+-------------+-------------+----------+
 *    | empty  | full   | full/full   |             | empty/empty |             |          |
 *    |--------+--------+-------------+-------------+-------------+-------------+----------+
 *    | full   | empty  |             | empty/empty |             | full/full   |          |
 *    |--------+--------+-------------+-------------+-------------+-------------+----------+
 *    | full   | full   |             | empty/full  | full/empty  |             |          |
 *    +--------+--------+-------------+-------------+-------------+-------------+----------+
 *
 *
 * The table for entry actions to be performend on state transitions is:
 *
 *    +-----------------+------------------------------------------------------------------+
 *    |      States     |                     Events                                       |
 *    +--------+--------+-------------+-------------+-------------+-------------+----------+
 *    | Source |  Sink  | source_fill | push        | sink_drain  | pull        | shutdown |
 *    |--------+--------+-------------+-------------+-------------+-------------+----------+
 *    | empty  | empty  |             | return      |notify_src   |             |          |
 *    |--------+--------+-------------+-------------+-------------+-------------+----------+
 *    | empty  | full   |             | return      |  	          | return      |          |
 *    |--------+--------+-------------+-------------+-------------+-------------+----------+
 *    | full   | empty  | notify_sink | src_swap    | notify_src  | snk_swap    |          |
 *    |--------+--------+-------------+-------------+-------------+-------------+----------+
 *    | full   | full   | notify_sink |             |  	       	  | return      |          |
 *    +--------+--------+-------------+-------------+-------------+-------------+----------+
 *
 *
 * The table for exit actions to be perfomed on state transitions is:
 *
 *    +-----------------+------------------------------------------------------------------+
 *    |      States     |                     Events                                       |
 *    +--------+--------+-------------+-------------+-------------+-------------+----------+
 *    | Source |  Sink  | source_fill | push        | sink_drain  | pull        | shutdown |
 *    |--------+--------+-------------+-------------+-------------+-------------+----------+
 *    | empty  | empty  |             |             |             | sink_swap   |          |
 *    |--------+--------+-------------+-------------+-------------+-------------+----------+
 *    | empty  | full   |             | return      |             | return      |          |
 *    |--------+--------+-------------+-------------+-------------+-------------+----------+
 *    | full   | empty  |             | src_swap    |             | sink_swap   |          |
 *    |--------+--------+-------------+-------------+-------------+-------------+----------+
 *    | full   | full   |             | src_swap    |             |             |          |
 *    +--------+--------+-------------+-------------+-------------+-------------+----------+
 *
 *
 * The sink_swap and src_swap functions are identical. They check to see if the state is
 * equal to full_empty, if so, swap the state to empty_full (and perform an action swap 
 * of the items assoiated with the source and sink), and notifies the other.  If the state
 * is not equal to full_empty, the swap function notifies the other and goes into a wait.
 *
 * The state transition tables operate in conjunction with entry and exit events
 * associated with each transition.  The entry and exit functions for a state transition 
 * from old to new are called in the following way:
 *
 * begin_transition: given old_state and event
 *
 *    execute exit(old_state, event)
 *    new_state = transition(old_state, event)
 *    execute entry(new_state, event)
 *
 * Note that the exit action is called before the state transition, and then the 
 * entry action is called.
 *
 * Desired usage of a source port / fsm from a source node:
 *
 * while (true) {
 *   produce an item
 *   cause event filled
 *   cause event push
 * }
 *
 *
 * Desired usage of a sink port / fsm from a source node:
 *
 * while (true) {
 *   cause event pull
 *   consume an item
 *   cause event drained
 * }
 *
 *
 *  More formally, with predicate annotations of the state at each part of the state
 *  transitions, the basic behavior of a client of a source port is
 *
 *   init: { state == empty_empty }
 *   start: { state == empty_empty }
 *   while (not done)
 *     loop_begin: { state == empty_empty ∨ state == empty_full }
 *       invoke exit action: none
 *     pre_produce: { state == empty_empty ∨ state == empty_full }
 *     client calls producer function and insert item into source
 *       (during producer function, sink could have drained) { state == empty_empty ∨ state == empty_full }
 *     post_produce: { state == empty_empty ∨ state == empty_full }
 *     client invokes fill event
 *     pre_fill: { state == empty_empty ∨ state == empty_full }
 *     invoke full_empty or full_full entry action: nil
 *     source_fill transition event - source state transitions to full
 *     source_fill: { state == full_empty ∨ state == full_full }
 *     invoke full_empty or full_full entry action: notify_sink
 *       sink.notify(): sink is notifed with { state == full_empty or state == full_full }
 *     return
 *     post_push: { state == full_empty or state == full_full } 
 *
 *     client invoke push event
 *     between source fill and push, the following could have happened
 *       sink could have drained: full_full -> full_empty
 *       sink could have pulled: full_empty -> empty_full
 *       sink could have pulled and drained: full_empty -> empty_empty
 *       sink could have done nothing: full_full -> full, full_empty -> full_empty
 *     pre_push: { state == full_empty ∨  state == empty_full ∨  state == empty_empty ∨ state == full_full }
 *     invoke exit action: empty_full -> return, full_empty -> src_swap, full_full -> src_swap, empty_empty: none
 *     if (state == full_empty)
 *       pre_swap: { state == full_empty }
 *       swap elements
 *       post_swap: { state == empty_full }
 *       sink.notify()
 *     else
 *       else_swap: { state == full_full }
 *       sink.notify()
 *       source.wait()
 *         the sink would have notified after draining, after swapping, or after pulling
 *         draining: sink state would be empty_full or empty_empty
 *         swapping: sink state would be empty_full
 *         else_swap: sink state would be empty_empty
 *       post_wait:  { state == empty_empty ∨ state == empty_full }
 *       set state to state of *sink* state and event at notification
 *       post_event_update: { state == empty_empty ∨ state == empty_full }
 *       invoke entry function for that state
 *         draining: sink state would be empty_full or empty_empty -> none
 *         swapping: sink state would be empty_full -> return
 *         else_swap: sink state would be empty_empty -> none
 *       post_push: { state == empty_full ∨ state == empty_empty }
 *       end_loop:  { state == empty_full ∨ state == empty_empty }
 *     post_loop:   { state == empty_full ∨ state == empty_empty }
 *
 *
 *  Similarly, with predicate annotations of the state at each part of the state
 *  transitions, the basic behavior of a client of a sink port is
 *
 *   init: { state == empty_empty }
 *   start: { state == empty_empty }
 *   while (not done)
 *     loop_begin: { state == empty_full ∨ state == empty_empty }
 *     client invoke pull event
 *     between sink drain and pull the following could have happened
 *       source could have filled: empty_empty -> full_empty
 *       source could have filled and pushed: empty_empty -> empty_full
 *       source could have done nothing: empty_empty -> empty_empty, empty_full -> empty_full
 *     pre_pull: { state == empty_full ∨ state == full_empty ∨  state == empty_empty }
 *     invoke exit action: empty_full -> return, full_empty -> snk_swap, empty_empty: snk_swap
 *     post_exit: { state == full_empty ∨  state == empty_empty }
 *     if (state == full_empty)
 *       pre_swap: { state == full_empty }
 *       swap elements
 *       post_swap: { state == empty_full }
 *       source.notify()
 *     else
 *       else_swap: { state == empty_empty }
 *       source.notify()
 *       sink.wait()
 *         the source would have notified after filling, after swapping, or after pushing
 *         filling: source state would be full_empty or full_full
 *         swapping: sink state would be empty_full
 *         else_swap: sink state would be full_full
 *       post_wait:  { state == full ∨ state == empty_full }
 *       set state to state of *source* state and event at notification
 *       post_event_update: { state == full_full ∨ state == empty_full }
 *       invoke entry function for that state
 *         filling: source state would be full_empty or full_full -> none
 *         swapping: sink state would be empty_full -> return
 *         else_swap: sink state would be empty_empty -> none
 *       post_push: { state == full_empty ∨ state == full_full }
 *
 *     pre_produce: { state == full_empty ∨ state == full_full }
 *     client calls consumer function and takes item from sink
 *       (during consumer function, source could have pushed) { state == full_full ∨ state == empty_full }
 *     post_consum: { state == full_full ∨ state == empty_full }
 *     client invokes drain event
 *     pre_drain: { state == full_full ∨ state == empty_full }
 *     invoke empty_full or full_full entry action: nil
 *     sink_drain transition event - sink state transitions to empty
 *     source_fill: { state == full_empty ∨ state == empty_empty }
 *     invoke empty_full or empty_empty entry action: notify_source
 *       source.notify(): source is notifed with { state == empty_full or state == empty_empty }
 *     return
 *     post_push: { state == empty_full or state == empty_empty } 
 *
 *     end_loop:  { state == empty_full ∨ state == empty_empty }
 *   post_loop:   { state == empty_full ∨ state == empty_empty }
 */
// clang-format on

/**
 * An enum representing the different states of the bound ports.
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
static inline auto str(PortState st) {
  return port_state_strings[static_cast<int>(st)];
}

enum class PortEvent : unsigned short {
  source_fill,
  push,
  sink_drain,
  pull,
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
    "push",
    "sink_drain",
    "pull",
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
 * Port Actions
 */
enum class PortAction : unsigned short {
  none,
  ac_return,
  src_swap,
  snk_swap,
  notify_source,
  notify_sink,
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
    "src_swap",
    "snk_swap",
    "notify_source",
    "notify_sink",
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
  /* source_sink */ /* source_fill */        /* push */              /* sink_drain */        /* pull */            /* shutdown */

  /* empty_empty */ { PortState::full_empty, PortState::empty_empty, PortState::error,       PortState::empty_full, PortState::error },
  /* empty_full  */ { PortState::full_full,  PortState::empty_full,  PortState::empty_empty, PortState::empty_full, PortState::error },
  /* full_empty  */ { PortState::empty_full, PortState::empty_full,  PortState::error,       PortState::empty_full, PortState::error },
  /* full_full   */ { PortState::error,      PortState::empty_full,  PortState::full_empty,  PortState::full_full,  PortState::error },

  /* error       */ { PortState::error,      PortState::error,       PortState::error,       PortState::error,      PortState::error },
  /* done        */ { PortState::error,      PortState::error,       PortState::error,       PortState::error,      PortState::error },
};

constexpr const PortAction exit_table[n_states][n_events] {
  /* source_sink */ /* source_fill */   /* push */             /* sink_drain */  /* pull */            /* shutdown */

  /* empty_empty */ { PortAction::none, PortAction::none,      PortAction::none, PortAction::snk_swap,  PortAction::none },
  /* empty_full  */ { PortAction::none, PortAction::ac_return, PortAction::none, PortAction::ac_return, PortAction::none },
  /* full_empty  */ { PortAction::none, PortAction::src_swap,  PortAction::none, PortAction::snk_swap,  PortAction::none },
  /* full_full   */ { PortAction::none, PortAction::src_swap,  PortAction::none, PortAction::none,      PortAction::none },

  /* error       */ { PortAction::none, PortAction::none,      PortAction::none, PortAction::none,      PortAction::none },
  /* done        */ { PortAction::none, PortAction::none,      PortAction::none, PortAction::none,      PortAction::none },
};

constexpr const PortAction entry_table[n_states][n_events] {
  /* source_sink */ /* source_fill */          /* push */             /* sink_drain */           /* pull */             /* shutdown */

  /* empty_empty */ { PortAction::none,        PortAction::ac_return, PortAction::notify_source, PortAction::none,      PortAction::none },
  /* empty_full  */ { PortAction::none,        PortAction::ac_return, PortAction::none,          PortAction::ac_return, PortAction::none },
  /* full_empty  */ { PortAction::notify_sink, PortAction::src_swap,  PortAction::notify_source, PortAction::src_swap,  PortAction::none },
  /* full_full   */ { PortAction::notify_sink, PortAction::none,      PortAction::none,          PortAction::ac_return, PortAction::none },

  /* error       */ { PortAction::none,        PortAction::none,      PortAction::none,          PortAction::none,      PortAction::none },
  /* done        */ { PortAction::none,        PortAction::none,      PortAction::none,          PortAction::none,      PortAction::none },
};
// clang-format on

}  // namespace

/**
 * Class template representing states of a bound source and sink node.  The
 * class is agnostic as to how the actions are actually implemented by the users
 * of the the state machine.  A policy class is used by the
 * `PortFinitStateMachine` to realize the specific state transition actions.
 *
 * The `ActionPolicy` class is expted to use the `FiniteStateMachine` class
 * using CRTP.
 *
 * @note There is a fair amount of debugging code inserted into the class at the
 * moment.
 *
 * @todo Remove the debugging code.
 */
template <class ActionPolicy>
class PortFiniteStateMachine {
 private:
  PortState state_;
  PortState next_state_;

 public:
  /**
   * Default constructor
   */
  PortFiniteStateMachine()
      : state_(PortState::empty_empty){};

  /**
   * Return the current state
   */
  [[nodiscard]] inline PortState state() const {
    return state_;
  }

  /**
   * Return the next state
   */
  [[nodiscard]] inline PortState next_state() const {
    return next_state_;
  }

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

 public:
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
  std::mutex mutex_;
  bool debug_{false};
  std::atomic<int> event_counter{};

  using lock_type = std::unique_lock<std::mutex>;

  void event(PortEvent event, const std::string msg = "") {
    std::unique_lock lock(mutex_);

    next_state_ = transition_table[to_index(state_)][to_index(event)];
    auto exit_action{exit_table[to_index(state_)][to_index(event)]};
    auto entry_action{entry_table[to_index(next_state_)][to_index(event)]};

    auto old_state = state_;

    if (msg != "") {
      std::cout << "\n"
                << event_counter++
                << " On event start: " + msg + " " + str(event) + ": " +
                       str(state_) + " (" + str(exit_action) + ") -> (" +
                       str(entry_action)
                << ") " + str(next_state_) << std::endl;
    }

    if (next_state_ == PortState::error) {
      std::cout << "\n"
                << event_counter++
                << " ERROR On event start: " + msg + " " + str(event) + ": " +
                       str(state_) + " (" + str(exit_action) + ") -> (" +
                       str(entry_action)
                << ") " + str(next_state_) << std::endl;
    }

    if (msg != "") {
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

      case PortAction::src_swap:
        if (msg != "")
          std::cout << event_counter++
                    << "      " + msg + " exit about to src_swap" << std::endl;
        static_cast<ActionPolicy&>(*this).on_source_swap(lock, event_counter);
        break;

      case PortAction::snk_swap:
        if (msg != "")
          std::cout << event_counter++
                    << "      " + msg + " exit about to snk_swap" << std::endl;
        static_cast<ActionPolicy&>(*this).on_sink_swap(lock, event_counter);
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
        throw std::logic_error("Unexpected entry action");
    }

    if (msg != "") {
      if (msg != "")
        std::cout << event_counter++
                  << " Post exit: " + msg + " " + str(event) + ": " +
                         str(state_) + " (" + str(exit_action) + ") -> (" +
                         str(entry_action)
                  << ") " + str(next_state_) << std::endl;
    }

    /*
     * Assign new state
     */
    state_ = next_state_;

    entry_action = entry_table[to_index(next_state_)][to_index(event)];

    if (msg != "") {
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

      case PortAction::src_swap:

        if (msg != "")
          std::cout << event_counter++
                    << "      " + msg + " entry about to src_swap" << std::endl;

        static_cast<ActionPolicy&>(*this).on_source_swap(lock, event_counter);
        break;

      case PortAction::snk_swap:

        if (msg != "")
          std::cout << event_counter++
                    << "      " + msg + " entry about to sink_swap"
                    << std::endl;

        static_cast<ActionPolicy&>(*this).on_sink_swap(lock, event_counter);
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
        throw std::logic_error("Unexpected entry action");
    }

    if (msg != "") {
      std::cout << event_counter++
                << " Post entry event: " + msg + " " + str(event) + ": " +
                       str(state_) + " (" + str(exit_action) + ") -> (" +
                       str(entry_action)
                << ") " + str(next_state_) << std::endl;
    }
  }
};

/**
 * Null action policy
 */
template <class T = size_t>
class NullStateMachine : public PortFiniteStateMachine<NullStateMachine<T>> {
  using FSM = PortFiniteStateMachine<NullStateMachine<T>>;
  using lock_type = typename FSM::lock_type;

 public:
};

/**
 * A simple debuggin action policy that simply prints that an action has been
 * called.
 */
template <class T = size_t>
class DebugStateMachine : public PortFiniteStateMachine<DebugStateMachine<T>> {
  using FSM = PortFiniteStateMachine<DebugStateMachine<T>>;

  using lock_type = typename FSM::lock_type;

 public:
  inline void on_ac_return(lock_type&, std::atomic<int>&) {
    if (FSM::debug_)
      std::cout << "    "
                << "Action return" << std::endl;
  }
  inline void on_source_swap(lock_type&, std::atomic<int>&) {
    if (FSM::debug_)

      std::cout << "    "
                << "Action swap source" << std::endl;
  }
  inline void on_sink_swap(lock_type&, std::atomic<int>&) {
    if (FSM::debug_)
      std::cout << "    "
                << "Action swap sink" << std::endl;
  }
  inline void notify_source(lock_type&, std::atomic<int>&) {
    if (FSM::debug_)
      std::cout << "    "
                << "Action notify source" << std::endl;
  }
  inline void notify_sink(lock_type&, std::atomic<int>&) {
    if (FSM::debug_)
      std::cout << "    "
                << "Action notify sink" << std::endl;
  }
};

/**
 * Debug action policy with some non-copyable elements (to verify compilation).
 */
class DebugStateMachineWithLock
    : public PortFiniteStateMachine<DebugStateMachineWithLock> {
  std::mutex(mutex_);
  std::condition_variable sink_cv_;
  std::condition_variable source_cv_;

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

#endif  // TILEDB_DAG_FSM_H
