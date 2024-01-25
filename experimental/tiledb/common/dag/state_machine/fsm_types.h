/*
 * @file   fsm_types.h
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
 */

#ifndef TILEDB_DAG_FSM_TYPES_H
#define TILEDB_DAG_FSM_TYPES_H

#include <string>
#include <vector>

namespace tiledb::common {

/**
 * An enum representing the different states of two bound ports, plus an
 * intermediary. Since it is useful to consider the states as binary numbers, we
 * start our numbering of states with state 0 as enum 0.
 */

enum class three_stage {
  st_000,
  st_001,
  st_010,
  st_011,
  st_100,
  st_101,
  st_110,
  st_111,
  xt_000,
  xt_001,
  xt_010,
  xt_011,
  xt_100,
  xt_101,
  xt_110,
  xt_111,
  done,
  na,
  error,
  unreach,
  last
};

/**
 * An enum representing the different states of two bound ports.
 * Since it is useful to consider the states as binary numbers, we
 * start our numbering of states with state 0 as enum 0.
 */
enum class two_stage {
  st_00,
  st_01,
  st_10,
  st_11,
  xt_00,
  xt_01,
  xt_10,
  xt_11,
  done,
  na,
  error,
  unreach,
  last
};

template <class PortState>
constexpr unsigned short to_index(PortState x) {
  return static_cast<unsigned short>(x);
}

/**
 * Number of states in the Port state machine.
 */
template <class PortState>
constexpr unsigned short num_states = to_index(PortState::last) + 1;

#if 0
/**
 * Number of stages represented by the state machine.
 */
template <class PortState>
unsigned short num_stages = 0;

template <>
constexpr const unsigned short num_stages<three_stage> = 3;

template <>
constexpr const unsigned short num_stages<two_stage> = 2;
#endif

namespace {
/**
 * Strings for each enum member, for debugging.
 */
template <class PortState>
static std::vector<std::string> port_state_strings;

template <>
std::vector<std::string> port_state_strings<two_stage>{
    "st_00",
    "st_01",
    "st_10",
    "st_11",
    "xt_00",
    "xt_01",
    "xt_10",
    "xt_11",
    "done",
    "na",
    "error",
    "unreach",
    "last"};

template <>
std::vector<std::string> port_state_strings<three_stage>{
    "st_000", "st_001", "st_010", "st_011", "st_100", "st_101",  "st_110",
    "st_111", "xt_000", "xt_001", "xt_010", "xt_011", "xt_100",  "xt_101",
    "xt_110", "xt_111", "done",   "na",     "error",  "unreach", "last",
};
}  // namespace

/**
 * Function to convert a state to a string.  Specializations exist for
 * `two_stage` and `three_stage`.
 *
 * @param event The PortState to stringify.
 * @return The string corresponding to the PortState.
 */
template <class PortState>
static inline auto str(PortState st);

template <>
inline auto str(three_stage st) {
  return port_state_strings<three_stage>[static_cast<int>(st)];
}

template <>
inline auto str(two_stage st) {
  return port_state_strings<two_stage>[static_cast<int>(st)];
}

#ifndef FXM

/**
 * enum class for the state machine events.  Does not depend on the number of
 * states.
 */
enum class PortEvent : unsigned short {
  source_fill,  // Source has placed data into its output item_
  source_push,  // Source wishes to send its data along the data channel, will
                // block if state is st_11 (i.e., if channel is full)
  try_push,     // Non-blocking version of source_push
  sink_drain,   // Sink has taken data from its input item_
  sink_pull,    // Sources wishes to receive data along the data channel, will
                // block if state is st_00 (i.e., if channel is empty)
  try_pull,     // Non-blocking version of sink_pull
  exhausted,    // Source will not send any further data
  last,         // Customary final enum
};

inline constexpr unsigned short to_index(PortEvent x) {
  return static_cast<unsigned short>(x);
}

/**
 * Number of events in the PortEvent state machine
 */
constexpr unsigned int n_events = to_index(PortEvent::last) + 1;

/**
 * Strings for each enum member, useful for debugging.
 */
static std::vector<std::string> event_strings{
    "source_fill",
    "source_push",
    "try_push",
    "sink_drain",
    "sink_pull",
    "try_pull",
    "exhausted",
    "last"};

/**
 * Function to convert event to a string.
 *
 * @param event The event to stringify.
 * @return The string corresponding to the event.
 */
static inline auto str(PortEvent ev) {
  return event_strings[static_cast<int>(ev)];
}

#else
/**
 * enum class for the state machine events.  Does not depend on the number of
 * states.
 */
enum class PortEvent : unsigned short {
  source_inject,
  sink_extract,
  stop,
};

inline constexpr unsigned short to_index(PortEvent x) {
  return static_cast<unsigned short>(x);
}

/**
 * Number of events in the PortEvent state machine
 */
constexpr unsigned int n_events = to_index(PortEvent::stop) + 1;

/**
 * Strings for each enum member, useful for debugging.
 */
static std::vector<std::string> event_strings{
    "source_inject",
    "sink_extract",
    "stop",
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

#endif

/**
 * enum class for port actions associated with transitions.
 */
enum class PortAction : unsigned short {
  none,
  ac_return,      // To caller of event (likely item mover)
  source_move,    // To item mover
  sink_move,      // To item mover
  notify_source,  // To scheduler
  notify_sink,    // To scheduler
  source_wait,    // To scheduler
  sink_wait,      // To scheduler
  term_source,    // To scheduler (terminate source)
  term_sink,      // To scheduler (terminate source)
  source_throw,   // Serious error condition
  sink_throw,     // Serious error condition
  error,          // General error condition in action table
  last,
};

inline constexpr unsigned short to_index(PortAction x) {
  return static_cast<unsigned short>(x);
}

/**
 * Number of actions in the Port state machine
 */
constexpr unsigned int n_actions = to_index(PortAction::last) + 1;

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
    "term_source",
    "term_sink",
    "source_throw",
    "sink_throw",
    "error",
    "last",
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
 * Utility functions
 */
[[maybe_unused]] static bool null_s(three_stage st) {
  return (st == three_stage::st_000);
}

[[maybe_unused]] static bool null_x(three_stage st) {
  return (st == three_stage::xt_000);
}

[[maybe_unused]] static bool null(three_stage st) {
  return null_s(st) || null_x(st);
}

[[maybe_unused]] static bool empty_s_source(three_stage st) {
  return (
      st == three_stage::st_000 || st == three_stage::st_001 ||
      st == three_stage::st_010 || st == three_stage::st_011);
}

[[maybe_unused]] static bool empty_x_source(three_stage st) {
  return (
      st == three_stage::xt_000 || st == three_stage::xt_001 ||
      st == three_stage::xt_010 || st == three_stage::xt_011);
}

[[maybe_unused]] static bool empty_source(three_stage st) {
  return (empty_s_source(st) || empty_x_source(st));
}
[[maybe_unused]] static bool full_s_source(three_stage st) {
  return (
      st == three_stage::st_100 || st == three_stage::st_101 ||
      st == three_stage::st_110 || st == three_stage::st_111);
}

[[maybe_unused]] static bool full_x_source(three_stage st) {
  return (
      st == three_stage::xt_100 || st == three_stage::xt_101 ||
      st == three_stage::xt_110 || st == three_stage::xt_111);
}

[[maybe_unused]] static bool full_source(three_stage st) {
  return (full_s_source(st) || full_x_source(st));
}

[[maybe_unused]] static bool empty_s_sink(three_stage st) {
  return (
      st == three_stage::st_000 || st == three_stage::st_010 ||
      st == three_stage::st_100 || st == three_stage::st_110);
}

[[maybe_unused]] static bool empty_x_sink(three_stage st) {
  return (
      st == three_stage::xt_000 || st == three_stage::xt_010 ||
      st == three_stage::xt_100 || st == three_stage::xt_110);
}

[[maybe_unused]] static bool empty_sink(three_stage st) {
  return (empty_s_sink(st) || empty_x_sink(st));
}
[[maybe_unused]] static bool full_s_sink(three_stage st) {
  return (
      st == three_stage::st_001 || st == three_stage::st_011 ||
      st == three_stage::st_101 || st == three_stage::st_111);
}

[[maybe_unused]] static bool full_x_sink(three_stage st) {
  return (
      st == three_stage::xt_001 || st == three_stage::xt_011 ||
      st == three_stage::xt_101 || st == three_stage::xt_111);
}

[[maybe_unused]] static bool full_sink(three_stage st) {
  return (full_s_sink(st) || full_x_sink(st));
}

[[maybe_unused]] static bool empty_s_state(three_stage st) {
  return (st == three_stage::st_000);
}

[[maybe_unused]] static bool empty_x_state(three_stage st) {
  return (st == three_stage::xt_000);
}

[[maybe_unused]] static bool empty_state(three_stage st) {
  return (empty_s_state(st) || empty_x_state(st));
}

[[maybe_unused]] static bool full_s_state(three_stage st) {
  return (st == three_stage::st_111);
}

[[maybe_unused]] static bool full_x_state(three_stage st) {
  return (st == three_stage::xt_111);
}

[[maybe_unused]] static bool full_state(three_stage st) {
  return (full_s_state(st) || full_x_state(st));
}

[[maybe_unused]] static bool terminating(three_stage st) {
  return st == three_stage::xt_000 || st == three_stage::xt_001 ||
         st == three_stage::xt_010 || st == three_stage::xt_011;
}

[[maybe_unused]] static bool terminated(three_stage st) {
  return st == three_stage::xt_000;
}

[[maybe_unused]] static bool done(three_stage st) {
  return st == three_stage::done;
}
/**
 *
 */

[[maybe_unused]] static bool null_s(two_stage st) {
  return (st == two_stage::st_00);
}

[[maybe_unused]] static bool null_x(two_stage st) {
  return (st == two_stage::xt_00);
}

[[maybe_unused]] static bool null(two_stage st) {
  return null_s(st) || null_x(st);
}

[[maybe_unused]] static bool empty_s_source(two_stage st) {
  return (st == two_stage::st_00 || st == two_stage::st_01);
}

[[maybe_unused]] static bool empty_x_source(two_stage st) {
  return (st == two_stage::xt_00 || st == two_stage::xt_01);
}

[[maybe_unused]] static bool empty_source(two_stage st) {
  return (empty_s_source(st) || empty_x_source(st));
}
[[maybe_unused]] static bool full_s_source(two_stage st) {
  return (st == two_stage::st_10 || st == two_stage::st_11);
}

[[maybe_unused]] static bool full_x_source(two_stage st) {
  return (st == two_stage::xt_10 || st == two_stage::xt_11);
}

[[maybe_unused]] static bool full_source(two_stage st) {
  return (full_s_source(st) || full_x_source(st));
}

[[maybe_unused]] static bool empty_s_sink(two_stage st) {
  return (st == two_stage::st_00 || st == two_stage::st_10);
}

[[maybe_unused]] static bool empty_x_sink(two_stage st) {
  return (st == two_stage::xt_00 || st == two_stage::xt_10);
}

[[maybe_unused]] static bool empty_sink(two_stage st) {
  return (empty_s_sink(st) || empty_x_sink(st));
}
[[maybe_unused]] static bool full_s_sink(two_stage st) {
  return (st == two_stage::st_01 || st == two_stage::st_11);
}

[[maybe_unused]] static bool full_x_sink(two_stage st) {
  return (st == two_stage::xt_01 || st == two_stage::xt_11);
}

[[maybe_unused]] static bool full_sink(two_stage st) {
  return (full_s_sink(st) || full_x_sink(st));
}

[[maybe_unused]] static bool empty_s_state(two_stage st) {
  return (st == two_stage::st_00);
}

[[maybe_unused]] static bool empty_x_state(two_stage st) {
  return (st == two_stage::xt_00);
}

[[maybe_unused]] static bool empty_state(two_stage st) {
  return (empty_s_state(st) || empty_x_state(st));
}

[[maybe_unused]] static bool full_s_state(two_stage st) {
  return (st == two_stage::st_11);
}

[[maybe_unused]] static bool full_x_state(two_stage st) {
  return (st == two_stage::xt_11);
}

[[maybe_unused]] static bool full_state(two_stage st) {
  return (full_s_state(st) || full_x_state(st));
}

[[maybe_unused]] static bool terminating(two_stage st) {
  return st == two_stage::xt_00 || st == two_stage::xt_01;
}

[[maybe_unused]] static bool terminated(two_stage st) {
  return st == two_stage::xt_00;
}

[[maybe_unused]] static bool done(two_stage st) {
  return st == two_stage::done;
}

}  // namespace tiledb::common
#endif  // TILEDB_DAG_FSM_TYPES_H
