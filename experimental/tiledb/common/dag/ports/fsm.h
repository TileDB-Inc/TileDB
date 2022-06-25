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
 * three events: source_fill, swap, and sink_drain.  For simplicty there are
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
 * The entry and exit functions for a state transition from old to new are
 * called in the following way:
 *
 * begin_transition: given old_state and event
 *
 *    execute exit(old_state, event)
 *    new_state = transition(old_state, event)
 *    execute entry(new_state, event)
 *
 * (It is perhaps somewhat non-intuitive that the state transition executes 
 * exit and then executes entry.)
 *
 * Desired usage of a source port / fsm from a source node:
 *
 * while (true) {
 *   produce an item
 *   cause event filled
 * }
 *
 *
 * Desired usage of a sink port / fsm from a source node:
 *
 * while (true) {
 *   cause event drained
 *   consume an item
 * }
 *
 *
 * We can look at the states of the FSM as the ports proceed through a data exchange.
 *
 * The basic behavior of a user of a source port is
 *   init: { state == empty_empty }
 *   start: { state == empty_empty }
 *   while (not done)
 *     loop_begin: { state == empty_empty ∨ state == empty_full }
 *     pre_produce: { state == empty_empty ∨ state == empty_full }
 *     invoke producer function
 *     during producer function, sink could have drained
 *     source_fill: { state == full_empty ∨ state == full_full }
 *     sink.notify()
 *     wait
 *     post wait, the following could have happened
 *       sink could have swapped -> state = empty_full
 *       sink could have drained -> state = full_empty
 *       sink could have swapped and drained -> state = empty_empty
 *     post_wait: { state == full_empty ∨ state == empty_full ∨ state == empty_empty }
 *     if (state == full_empty)
 *       pre_swap: { state ==  full_empty }
 *       swap elements
 *       post_swap: { state ==  empty_full }
 *       sink.notify()
 *     else
 *       else_swap: { state == empty_empty ∨ state == empty_full }
 *   post_loop: { state == empty_empty ∨ state == empty_full }
 *
 *
 * The basic behavior of a user of a sink port is
 *   We perform some initializetion to get into a proper starting state
 *   init: { state == empty_empty }
 *   start: { state == empty_empty ∨ state == full_empty }
 *   while (not done)
 *     loop_begin: { state == empty_empty ∨ state == full_empty }
 *     source.notify()
 *     wait
 *     post wait, the following could have happened
 *       source could have swapped -> state = empty_full
 *       source could have filled  -> full_empty
 *       source could have swapped and filled -> state = full_full
 *     post_wait: { state == full_empty ∨ state == empty_full ∨ state == full_full }
 *     if (state == full_empty)
 *       pre_swap: { state == full_empty }
 *       swap
 *       post_swap: { state ==  empty_full }
 *       source.notify()
 *     else
 *       else_swap: { state == full_empty ∨ state == empty_full }
 *     pre_consume: { state == empty_full ∨ state == full_full }
 *     invoke consumer function
 *     during consumer function, source could have filled
 *     drain: { state == empty_empty ∨ state == full_empty }
 *  post_loop: { state = empty_empty }
 *
 *
 * We can also look at the behavior of the states from the point of view of the FSM.
 *
 * source:
 *   while (true) {
 *
 *     ( event called by node )
 *
 *    source_entry:
 *     { state == empty_full ∨ state == empty_empty } ∧ { source_item == true }
 *
 *     do filled transition : event(drained)
 *
 *     { state == full_empty ∨ state == full_full } ∧ { source_item == true }
 *
 *     notify sink : entry(full_empty/full_full)
 *
 *     wait
 *     { state != full_full } ∧ { source_item == true ∨ source_item == false }
 *     if state is full_empty
 *       { state == full_empty } ∧ { source_item == true }
 *       do_swap
 *       { state == full_empty } ∧ { source_item == false }
 *       do swap transition
 *       { state == empty_full } ∧ { source_item == false }
 *       notify sink
 *       { state == empty_full ∨ state = empty_empty } ∧ { source_item == false }
 *
 *     else { state == empty_full ∨ state == empty_empty } ∧ { source_item == false }
 *
 *     return
 *       { state == empty_full ∨ state = empty_empty } ∧ { source_item == false }
 *   }
 *
 * sink:
 *   while (true)
 *
 *     ( event called by node )
 *
 *    sink_entry:
 *     { state == empty_full ∨ state == full_full } ∧ { sink_item == false }
 *
 *     do drained transition : event(drained)
 *
 *     { state == full_empty ∨ state == empty_empty } ∧ { sink_item == false }
 *
 *     notify source  : entry(full_empty/empty_empty, drained)
 *     wait
 *
 *     { state != empty_empty } ∧ { sink_item == false ∨ sink_item == true }
 *     if state is full_empty
 *       { state == full_empty } ∧ { sink_item == false }
 *
 *       do_swap 
 *
 *       { state == full_empty } ∧ { sink_item == false }
 *
 *       do swap transition : state <- empty_full
 *
 *       { state == empty_full } ∧ { sink_item == true }
 *
 *       notify source : entry(empty_full, swap)
 *
 *       { state == empty_full ∨ state == full_full} ∧ { sink_item == true } 
 *     else { state == empty_full ∨ state == full_full } ∧ { sink_item == true }
 *
 *     return { state == empty_full ∨ state == full_full } ∧ { sink_item == true }
 * 
 *
 * Note that the loop structures for the source and sink are essentially the same.  The difference
 * is in the states at each point.
 *
 * We can write a generic source / sink program -- the differences between the two are
 * separated by a /, with source on the left and sink on the right.
 *
 *   function run:
 *   while (true) {
 *
 *     source entry:
 *       { state == empty_full ∨ state == empty_empty } / { state == empty_full ∨ state == full_full }
 *
 *       call back do_fill     / call back do_drain
 *       do filled transition  / do drained transition
 *
 *       { state == full_empty ∨ state == full_full } / { state == full_empty ∨ state == empty_empty }
 *
 *     sink entry:
 *       call back notify_wait  
 *
 *       { state != full_full } / { state != empty_empty }
 *
 *       if state is full_empty
 *         do_swap
 *         do swap transition 
 *         call back notify_other
 *
 *         { state == empty_full } / { state == empty_full }
 *
 *       else { state == empty_full ∨ state == empty_empty } / { state == empty_full ∨ state == full_full }
 *   }
 *
 *
 * To factor out what functionality to incorporate into the entry and exit points of the
 * finite-state machine, recall there are only two external events -- source_fill and sink_drain.
 *
 * Base on the pseudocode above, and the predicate annotations therein, we can see we need
 * two entry functions -- one for when the source enters the full state, and one for when the
 * sink enters the empty state. (In fact, since we can express the contents of that function
 * in the same way for both cases, we really only need one function for both cases.  We show
 * the cases separately for clarity.)
 *
 * In the case of the source, on entry, we need to do the following:
 * 
 *   notify the sink
 *   wait for the sink to become empty
 *   on wakeup, if the current state is full_empty, perform a swap and set the state to empty_full
 *
 *
 * In the case of the sink, on entry, we need to do the following:
 *
 * Notify the source
 *   wait for the source to become full  
 *   on wakeup, if the current state is full_empty, perform a swap and set the state to empty_full
 *
 * When we are using locks and condition variables, a single cv can be shared between source and
 * sink because (presumably), they will never sleep at the same time.  In that case, only a single
 * function is needed for source entry and sink entry.
 *
 * The table for entry functions contains actions to be performend on state transitions (as
 * described above: 
 *
 *    +-----------------+---------------------------------------------------+
 *    |      States     |                    Events                         |
 *    +--------+--------+-------------+------------+-------------+----------+
 *    | Source |  Sink  | source_fill | swap       | sink_drain  | shutdown |
 *    |--------+--------+-------------+------------+-------------+----------+
 *    | empty  | empty  |             |            | snk_swap    |          |
 *    |--------+--------+-------------+------------+-------------+----------+
 *    | empty  | full   |             |            |             |          |
 *    |--------+--------+-------------+------------+-------------+----------+
 *    | full   | empty  | src_swap    |            | snk_swap    |          |
 *    |--------+--------+-------------+------------+-------------+----------+
 *    | full   | full   | src_swap    |            |             |          |
 *    +--------+--------+-------------+------------+-------------+----------+
 *
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
 * Strings for each enum member, for debugging.
 */
static std::vector<std::string> action_strings{
    "none",
    "ac_return",
    "src_swap",
    "snk_swap",
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
constexpr const PortState transition_table[n_states][n_events] {
  /* source_sink */    /* source_fill */      /* swap */             /* sink_drain */        /* shutdown */

  /* empty_empty */  { PortState::full_empty, PortState::error,      PortState::error,       PortState::error },
  /* empty_full  */  { PortState::full_full,  PortState::error,      PortState::empty_empty, PortState::error },
  /* full_empty  */  { PortState::error,      PortState::empty_full, PortState::error,       PortState::error },
  /* full_full   */  { PortState::error,      PortState::error,      PortState::full_empty,  PortState::error },

  /* error       */  { PortState::error,      PortState::error,      PortState::error,       PortState::error },
  /* done        */  { PortState::error,      PortState::error,      PortState::error,       PortState::error },
  };


constexpr const PortAction entry_table[n_states][n_events] {
  /* source_sink */    /* source_fill */      /* swap */             /* sink_drain */        /* shutdown */

  /* empty_empty */  { PortAction::none,       PortAction::ac_return,  PortAction::snk_swap,     PortAction::none },
  /* empty_full  */  { PortAction::none,       PortAction::ac_return,  PortAction::none,         PortAction::none },
  /* full_empty  */  { PortAction::src_swap,   PortAction::ac_return,  PortAction::snk_swap,     PortAction::none },
  /* full_full   */  { PortAction::src_swap,   PortAction::ac_return,  PortAction::none,         PortAction::none },

  /* error       */  { PortAction::none,       PortAction::none,        PortAction::none,        PortAction::none },
  /* done        */  { PortAction::none,       PortAction::none,        PortAction::none,        PortAction::none },
  };


constexpr const PortAction exit_table[n_states][n_events] {
  /* source_sink */    /* source_fill */      /* swap */             /* sink_drain */        /* shutdown */

  /* empty_empty */  { PortAction::none,       PortAction::none,       PortAction::none,         PortAction::none },
  /* empty_full  */  { PortAction::none,       PortAction::none,       PortAction::none,         PortAction::none },
  /* full_empty  */  { PortAction::none,       PortAction::none,       PortAction::none,         PortAction::none },
  /* full_full   */  { PortAction::none,       PortAction::none,       PortAction::none,         PortAction::none },

  /* error       */  { PortAction::none,       PortAction::none,       PortAction::none,         PortAction::none },
  /* done        */  { PortAction::none,       PortAction::none,       PortAction::none,         PortAction::none },
  };

// clang-format on

}  // namespace

/**
 * Class representing states of a bound source and sink node.
 * The class is agnostic as to how the events are actually
 * implemented by the users of the the state machine.
 *
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
      : state_(PortState::empty_full){};

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
   * Todo: Protect state transitions with mutex?
   *
   * Todo: Make event() private and create do_fill(), do_drain(),
   * and do_swap() public members.
   *
   * Todo: Investigate coroutine-like approach for corresponding with external
   * events so that the procession of steps is driven by the state machine
   * rather than the user of the state machine.
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

  using lock_type = std::unique_lock<std::mutex>;

  void event(PortEvent event, const std::string msg = "") {
    std::unique_lock lock(mutex_);

    next_state_ = transition_table[to_index(state_)][to_index(event)];
    auto exit_action{exit_table[to_index(state_)][to_index(event)]};
    auto entry_action{entry_table[to_index(next_state_)][to_index(event)]};

    if (msg != "") {
      std::cout << "On event start: " + msg + " " + str(event) + ": " +
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
        if (debug_)
          std::cout << "      exit about to ac_return" << std::endl;
        static_cast<ActionPolicy&>(*this).on_ac_return(lock);
        return;
        break;

      case PortAction::src_swap:
        if (debug_)
          std::cout << "      exit about to src_swap" << std::endl;
        static_cast<ActionPolicy&>(*this).on_source_swap(lock);
        break;

      case PortAction::snk_swap:
        if (debug_)
          std::cout << "      exit about to snk_swap" << std::endl;
        static_cast<ActionPolicy&>(*this).on_sink_swap(lock);
        break;

      default:
        throw std::logic_error("Unexpected entry action");
    }

    if (msg != "") {
      if (debug_)
        std::cout << "Post exit: " + msg + " " + str(event) + ": " +
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
      std::cout << "Pre entry event: " + msg + " " + str(event) + ": " +
                       str(state_) + " (" + str(exit_action) + ") -> (" +
                       str(entry_action)
                << ") " + str(next_state_) << std::endl;
    }

    /**
     * Perform any entry actions.
     */
    switch (entry_action) {
      case PortAction::none:
        break;

      case PortAction::ac_return:

        if (debug_)
          std::cout << "      entry about to ac_return" << std::endl;

        static_cast<ActionPolicy&>(*this).on_ac_return(lock);
        return;
        break;

      case PortAction::src_swap:

        if (debug_)
          std::cout << "      entry about to src_swap" << std::endl;

        static_cast<ActionPolicy&>(*this).on_source_swap(lock);
        break;

      case PortAction::snk_swap:

        if (debug_)
          std::cout << "      entry about to sink_swap" << std::endl;

        static_cast<ActionPolicy&>(*this).on_sink_swap(lock);
        break;

      default:
        throw std::logic_error("Unexpected entry action");
    }

    if (msg != "") {
      std::cout << "Post entry event: " + msg + " " + str(event) + ": " +
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
  inline void on_ac_return() {
  }
  inline void on_source_swap() {
  }
  inline void on_sink_swap() {
  }
};

/**
 * Debug action policy
 */
template <class T = size_t>
class DebugStateMachine : public PortFiniteStateMachine<DebugStateMachine<T>> {
  using FSM = PortFiniteStateMachine<DebugStateMachine<T>>;

  using lock_type = typename FSM::lock_type;

 public:
  inline void on_ac_return(lock_type&) {
    if (FSM::debug_)
      std::cout << "    "
                << "Action return" << std::endl;
  }
  inline void on_source_swap(lock_type&) {
    if (FSM::debug_)

      std::cout << "    "
                << "Action swap source" << std::endl;
  }
  inline void on_sink_swap(lock_type&) {
    if (FSM::debug_)

      std::cout << "    "
                << "Action swap sink" << std::endl;
  }
};

/**
 * Debug action policy with some non-copyable elements (to verify compilation)
 */
class DebugStateMachineWithLock
    : public PortFiniteStateMachine<DebugStateMachineWithLock> {
  std::mutex(mutex_);
  std::condition_variable sink_cv_;
  std::condition_variable source_cv_;

 public:
  inline void on_ac_return(lock_type&) {
    std::cout << "    "
              << "Action return" << std::endl;
  }
  inline void on_source_swap(lock_type&) {
    std::cout << "    "
              << "Action swap source" << std::endl;
  }
  inline void on_sink_swap(lock_type&) {
    std::cout << "    "
              << "Action swap sink" << std::endl;
  }
};

}  // namespace tiledb::common

#endif  // TILEDB_DAG_FSM_H
