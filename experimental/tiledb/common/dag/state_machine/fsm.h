/*
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
 *
 * This file defines the operation of a finite state machine, parameterized to
 * accommodate 2 stage or 3 stage data transfer, i.e., 4 or 8 states.  Each
 * state corresponds to a binary number in [0, 2^N) (for N = 2 or N = 3).
 *
 */

#ifndef TILEDB_DAG_FSM_H
#define TILEDB_DAG_FSM_H

#include <atomic>
#include <cassert>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string>
#include <vector>
#include "experimental/tiledb/common/dag/execution/task_state_machine.h"
#include "experimental/tiledb/common/dag/state_machine/fsm_types.h"

namespace tiledb::common {
namespace {
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
  /* state */   /* source_fill */ /* source_push */ /* try_push */    /* sink_drain */  /* sink_pull */   /* try_pull */    /* exhausted */
						    		    		                                          
  /* st_00 */ { two_stage::st_10, two_stage::st_00, two_stage::st_00, two_stage::error, two_stage::na,    two_stage::st_00, two_stage::xt_00 },
  /* st_01 */ { two_stage::st_11, two_stage::st_01, two_stage::st_01, two_stage::st_00, two_stage::st_01, two_stage::st_01, two_stage::xt_01 },
  /* st_10 */ { two_stage::error, two_stage::st_01, two_stage::st_01, two_stage::error, two_stage::st_01, two_stage::st_01, two_stage::error },
  /* st_11 */ { two_stage::error, two_stage::na,    two_stage::st_11, two_stage::st_10, two_stage::st_11, two_stage::st_11, two_stage::error },
						    		    		                                          
  /* xt_00 */ { two_stage::error, two_stage::error, two_stage::error, two_stage::error, two_stage::done,  two_stage::done,  two_stage::error },
  /* xt_01 */ { two_stage::error, two_stage::error, two_stage::error, two_stage::xt_00, two_stage::xt_00, two_stage::xt_01, two_stage::error },
  /* xt_10 */ { two_stage::error, two_stage::error, two_stage::error, two_stage::error, two_stage::xt_01, two_stage::xt_01, two_stage::unreach },
  /* xt_11 */ { two_stage::error, two_stage::error, two_stage::error, two_stage::xt_10, two_stage::xt_11, two_stage::xt_11, two_stage::unreach },
						    		    		                                          
  /* done  */ { two_stage::error, two_stage::error, two_stage::error, two_stage::error, two_stage::error, two_stage::error, two_stage::/*error*/done },
  /* error */ { two_stage::error, two_stage::error, two_stage::error, two_stage::error, two_stage::error, two_stage::error, two_stage::error },
								                                          
  /* last */  { two_stage::error, two_stage::error, two_stage::error, two_stage::error, two_stage::error, two_stage::error },
};

/**
 * Specialization of `exit_table` for `two_stage`.
 */
template<>
constexpr const PortAction exit_table<two_stage>[num_states<two_stage>][n_events] {
  /* state */   /* source_fill */        /* source_push */        /* try_push */           /* sink_drain */       /* sink_pull */        /* try_pull */        /* exhausted */
							                            					                            
  /* st_00 */ { PortAction::none,        PortAction::ac_return,   PortAction::ac_return,   PortAction::none,      PortAction::sink_wait, PortAction::none,      PortAction::none /*PortAction::notify_sink */},
  /* st_01 */ { PortAction::none,        PortAction::ac_return,   PortAction::ac_return,   PortAction::none,      PortAction::ac_return, PortAction::ac_return, PortAction::none /*PortAction::notify_sink */},
  /* st_10 */ { PortAction::none,        PortAction::source_move, PortAction::source_move, PortAction::none,      PortAction::sink_move, PortAction::sink_move, PortAction::none /*PortAction::notify_sink */},
  /* st_11 */ { PortAction::none,        PortAction::source_wait, PortAction::none,        PortAction::none,      PortAction::ac_return, PortAction::ac_return, PortAction::none /*PortAction::notify_sink */},

  /* xt_00 */ { PortAction::none,        PortAction::none,        PortAction::none,        PortAction::none,      PortAction::none,      PortAction::none,      PortAction::none },
  /* xt_01 */ { PortAction::none,        PortAction::none,        PortAction::none,        PortAction::none,      PortAction::ac_return, PortAction::ac_return, PortAction::none },
  /* xt_10 */ { PortAction::source_throw, PortAction::source_throw, PortAction::source_throw, PortAction::sink_throw, PortAction::sink_throw, PortAction::sink_throw, PortAction::source_throw },
  /* xt_11 */ { PortAction::source_throw, PortAction::source_throw, PortAction::source_throw, PortAction::sink_throw, PortAction::sink_throw, PortAction::sink_throw, PortAction::source_throw },

  /* done  */ { PortAction::none,        PortAction::none,        PortAction::none,        PortAction::none,      PortAction::none,      PortAction::none,      PortAction::none },
  /* error */ { PortAction::none,        PortAction::none,        PortAction::none,        PortAction::none,      PortAction::none,      PortAction::none,      PortAction::none },
  
  /* last */  { PortAction::none,        PortAction::none,        PortAction::none,        PortAction::none,      PortAction::none,      PortAction::none,      PortAction::none },
};

/**
 * Specialization of `entry_table` for `two_stage`.
 */
template<>
constexpr const PortAction entry_table<two_stage> [num_states<two_stage>][n_events] {
  /* state */   /* source_fill */        /* source_push */        /* try_push */           /* sink_drain */           /* sink_pull */        /* try_pull */        /* exhausted */
								                           						                            
  /* st_00 */ { PortAction::none,        PortAction::none,        PortAction::none,        PortAction::notify_source, PortAction::none,      PortAction::none,      PortAction::none },
  /* st_01 */ { PortAction::none,        PortAction::none,        PortAction::none,        PortAction::none,          PortAction::none,      PortAction::none,      PortAction::none },
  /* st_10 */ { PortAction::notify_sink, PortAction::none,        PortAction::none,        PortAction::notify_source, PortAction::none,      PortAction::none,      PortAction::none },
  /* st_11 */ { PortAction::notify_sink, PortAction::none,        PortAction::none,        PortAction::none,          PortAction::none,      PortAction::none,      PortAction::none },
								                           						                            

  /* xt_00 */ { PortAction::none,        PortAction::none,        PortAction::none,        PortAction::none,          PortAction::none,      PortAction::none,      PortAction::term_source  },
  /* xt_01 */ { PortAction::none,        PortAction::none,        PortAction::none,        PortAction::none,          PortAction::none,      PortAction::none,      PortAction::term_source  },
  /* xt_10 */ { PortAction::none,        PortAction::none,        PortAction::none,        PortAction::none,          PortAction::none,      PortAction::none,      PortAction::source_throw },
  /* xt_11 */ { PortAction::none,        PortAction::none,        PortAction::none,        PortAction::none,          PortAction::none,      PortAction::none,      PortAction::source_throw },
								                           						                            
  /* done  */ { PortAction::none,        PortAction::none,        PortAction::none,        PortAction::none,          PortAction::term_sink, PortAction::term_sink, PortAction::/*none*/term_source },
  /* error */ { PortAction::none,        PortAction::none,        PortAction::none,        PortAction::none,          PortAction::none,      PortAction::none,      PortAction::none },
								                           						                            
  /* last */  { PortAction::none,        PortAction::none,        PortAction::none,        PortAction::none,          PortAction::none,      PortAction::none,      PortAction::none },
};

/**
 * Specialization of `transition_table` for `three_stage`.
 */
template<>
constexpr const three_stage
transition_table<three_stage>[num_states<three_stage>][n_events] {
  /* state  */   /* source_fill */    /* source_push */    /* try_push */       /* sink_drain */     /* sink_pull */       /* try_pull */        /* exhausted */

  /* st_000 */ { three_stage::st_100, three_stage::st_000, three_stage::st_000, three_stage::error,  three_stage::na,      three_stage::st_000,  three_stage::xt_000,},
  /* st_001 */ { three_stage::st_101, three_stage::st_001, three_stage::st_001, three_stage::st_000, three_stage::st_001,  three_stage::st_001,  three_stage::xt_001,},
  /* st_010 */ { three_stage::st_110, three_stage::st_001, three_stage::st_001, three_stage::error,  three_stage::st_001,  three_stage::st_001,  three_stage::xt_010,},
  /* st_011 */ { three_stage::st_111, three_stage::st_011, three_stage::st_011, three_stage::st_010, three_stage::st_011,  three_stage::st_011,  three_stage::xt_011,},
  /* st_100 */ { three_stage::error,  three_stage::st_001, three_stage::st_001, three_stage::error,  three_stage::st_001,  three_stage::st_001,  three_stage::error, },
  /* st_101 */ { three_stage::error,  three_stage::st_011, three_stage::st_011, three_stage::st_100, three_stage::st_011,  three_stage::st_011,  three_stage::error, },
  /* st_110 */ { three_stage::error,  three_stage::st_011, three_stage::st_011, three_stage::error,  three_stage::st_011,  three_stage::st_011,  three_stage::error, },
  /* st_111 */ { three_stage::error,  three_stage::na,     three_stage::st_111, three_stage::st_110, three_stage::st_111,  three_stage::st_111,  three_stage::error, },
							                        					                         
  /* xt_000 */ { three_stage::error,  three_stage::error,  three_stage::error,  three_stage::error,  three_stage::done,    three_stage::done,    three_stage::error,},
  /* xt_001 */ { three_stage::error,  three_stage::error,  three_stage::error,  three_stage::xt_000, three_stage::xt_001,  three_stage::xt_001,  three_stage::error,},
  /* xt_010 */ { three_stage::error,  three_stage::error,  three_stage::error,  three_stage::error,  three_stage::xt_001,  three_stage::xt_001,  three_stage::error,},
  /* xt_011 */ { three_stage::error,  three_stage::error,  three_stage::error,  three_stage::xt_010, three_stage::xt_011,  three_stage::xt_011,  three_stage::error,},
  /* xt_100 */ { three_stage::unreach,  three_stage::unreach,  three_stage::unreach,  three_stage::unreach,  three_stage::xt_001,  three_stage::xt_001,  three_stage::unreach,},
  /* xt_101 */ { three_stage::unreach,  three_stage::unreach,  three_stage::unreach,  three_stage::xt_100, three_stage::xt_011,  three_stage::xt_011,  three_stage::unreach,},
  /* xt_110 */ { three_stage::unreach,  three_stage::unreach,  three_stage::unreach,  three_stage::unreach,  three_stage::xt_011,  three_stage::xt_011,  three_stage::unreach,},
  /* xt_111 */ { three_stage::unreach,  three_stage::unreach,  three_stage::unreach,  three_stage::xt_110, three_stage::xt_111,  three_stage::xt_111,  three_stage::unreach,},
							                        					                         
  /* done   */ { three_stage::error,  three_stage::error,  three_stage::error,  three_stage::error,  three_stage::error,   three_stage::error,   three_stage::/*error*/done },
  /* error  */ { three_stage::error,  three_stage::error,  three_stage::error,  three_stage::error,  three_stage::error,   three_stage::error,   three_stage::error },
							                        					                         
  /* last  */  { three_stage::error,  three_stage::error,  three_stage::error,  three_stage::error,  three_stage::error,   three_stage::error,   three_stage::error },
};

/**
 * Specialization of `exit_table` for `three_stage`.
 */
template<>
constexpr const PortAction exit_table<three_stage>[num_states<three_stage>][n_events] {
  /* state  */  /* source_fill */         /* source_push */        /* try_push */           /* sink_drain */       /* sink_pull */        /* try_pull */         /* exhausted */
										                                                 
  /* st_000 */ { PortAction::none,        PortAction::ac_return,   PortAction::ac_return,   PortAction::none,      PortAction::sink_wait, PortAction::none,      PortAction::none },
  /* st_001 */ { PortAction::none,        PortAction::ac_return,   PortAction::ac_return,   PortAction::none,      PortAction::ac_return, PortAction::ac_return, PortAction::none },
  /* st_010 */ { PortAction::none,        PortAction::source_move, PortAction::source_move, PortAction::none,      PortAction::sink_move, PortAction::sink_move, PortAction::none },
  /* st_011 */ { PortAction::none,        PortAction::ac_return,   PortAction::ac_return,   PortAction::none,      PortAction::ac_return, PortAction::ac_return, PortAction::none },
  /* st_100 */ { PortAction::none,        PortAction::source_move, PortAction::source_move, PortAction::none,      PortAction::sink_move, PortAction::sink_move, PortAction::none },
  /* st_101 */ { PortAction::none,        PortAction::source_move, PortAction::source_move, PortAction::none,      PortAction::sink_move, PortAction::sink_move, PortAction::none },
  /* st_110 */ { PortAction::none,        PortAction::source_move, PortAction::source_move, PortAction::none,      PortAction::sink_move, PortAction::sink_move, PortAction::none },
  /* st_111 */ { PortAction::none,        PortAction::source_wait, PortAction::none,        PortAction::none,      PortAction::ac_return, PortAction::ac_return, PortAction::none },
				          			    			    			                                                 
  /* xt_000 */ { PortAction::none,        PortAction::none,        PortAction::none,        PortAction::none,       PortAction::none,      PortAction::none,      PortAction::none },
  /* xt_001 */ { PortAction::none,        PortAction::none,        PortAction::none,        PortAction::none,       PortAction::ac_return, PortAction::ac_return, PortAction::none },
  /* xt_010 */ { PortAction::none,        PortAction::none,        PortAction::none,        PortAction::sink_throw, PortAction::sink_move, PortAction::sink_move, PortAction::none },
  /* xt_011 */ { PortAction::none,        PortAction::none,        PortAction::none,        PortAction::none,       PortAction::ac_return, PortAction::ac_return, PortAction::none },
  /* xt_100 */ { PortAction::source_throw, PortAction::source_throw, PortAction::source_throw, PortAction::none,      PortAction::sink_throw, PortAction::sink_throw, PortAction::none },
  /* xt_101 */ { PortAction::source_throw, PortAction::source_throw, PortAction::source_throw, PortAction::none,      PortAction::sink_throw, PortAction::sink_throw, PortAction::none },
  /* xt_110 */ { PortAction::source_throw, PortAction::source_throw, PortAction::source_throw, PortAction::none,      PortAction::sink_throw, PortAction::sink_throw, PortAction::none },
  /* xt_111 */ { PortAction::source_throw, PortAction::source_throw, PortAction::source_throw, PortAction::none,      PortAction::sink_throw, PortAction::sink_throw, PortAction::none },
				          			    			    			                                                 
  /* done   */ { PortAction::none,        PortAction::none,        PortAction::none,        PortAction::none,      PortAction::none,      PortAction::none,      PortAction::none },
  /* error  */ { PortAction::none,        PortAction::none,        PortAction::none,        PortAction::none,      PortAction::none,      PortAction::none,      PortAction::none },
							    			    			                                                 
  /* last   */ { PortAction::none,        PortAction::none,        PortAction::none,        PortAction::none,      PortAction::none,      PortAction::none,      PortAction::none },
};


/**
 * Specialization of `entry_table` for `three_stage`.
 */
template<>
constexpr const PortAction entry_table<three_stage>[num_states<three_stage>][n_events] {
  /* state  */  /* source_fill */         /* source_push */        /* try_push */           /* sink_drain */           /* sink_pull */       /* try_pull */         /* exhausted */
											                                                  
  /* st_000 */ { PortAction::none,        PortAction::none,        PortAction::none,        PortAction::notify_source, PortAction::none,     PortAction::none,      PortAction::none },
  /* st_001 */ { PortAction::none,        PortAction::none,        PortAction::none,        PortAction::none,          PortAction::none,     PortAction::none,      PortAction::none },
  /* st_010 */ { PortAction::none,        PortAction::none,        PortAction::none,        PortAction::notify_source, PortAction::none,     PortAction::none,      PortAction::none },
  /* st_011 */ { PortAction::none,        PortAction::none,        PortAction::none,        PortAction::none,          PortAction::none,     PortAction::none,      PortAction::none },
  /* st_100 */ { PortAction::notify_sink, PortAction::none,        PortAction::none,        PortAction::notify_source, PortAction::none,     PortAction::none,      PortAction::none },
  /* st_101 */ { PortAction::notify_sink, PortAction::none,        PortAction::none,        PortAction::none,          PortAction::none,     PortAction::none,      PortAction::none },
  /* st_110 */ { PortAction::notify_sink, PortAction::none,        PortAction::none,        PortAction::notify_source, PortAction::none,     PortAction::none,      PortAction::none },
  /* st_111 */ { PortAction::notify_sink, PortAction::none,        PortAction::none,        PortAction::none,          PortAction::none,     PortAction::none,      PortAction::none },

  // Gaah!  need term_source for throw_catch but notify_sink for bountiful  (unit_throw_catch vs unit_stop)

  /* xt_000 */ { PortAction::none,        PortAction::none,        PortAction::none,        PortAction::none,          PortAction::none,     PortAction::none,      PortAction::term_source },
  /* xt_001 */ { PortAction::none,        PortAction::none,        PortAction::none,        PortAction::none,          PortAction::none,     PortAction::none,      PortAction::term_source },
  /* xt_010 */ { PortAction::none,        PortAction::none,        PortAction::none,        PortAction::none,          PortAction::none,     PortAction::none,      PortAction::term_source },
  /* xt_011 */ { PortAction::none,        PortAction::none,        PortAction::none,        PortAction::none,          PortAction::none,     PortAction::none,      PortAction::term_source },
  /* xt_100 */ { PortAction::source_throw, PortAction::source_throw, PortAction::source_throw, PortAction::sink_throw, PortAction::sink_throw, PortAction::sink_throw, PortAction::source_throw },
  /* xt_101 */ { PortAction::source_throw, PortAction::source_throw, PortAction::source_throw, PortAction::sink_throw, PortAction::sink_throw, PortAction::sink_throw, PortAction::source_throw },
  /* xt_110 */ { PortAction::source_throw, PortAction::source_throw, PortAction::source_throw, PortAction::sink_throw, PortAction::sink_throw, PortAction::sink_throw, PortAction::source_throw },
  /* xt_111 */ { PortAction::source_throw, PortAction::source_throw, PortAction::source_throw, PortAction::sink_throw, PortAction::sink_throw, PortAction::sink_throw, PortAction::source_throw },
								   			   			                                                  
  /* done   */ { PortAction::none,        PortAction::none,        PortAction::none,        PortAction::none,          PortAction::term_sink, PortAction::term_sink, PortAction::/*none*/term_source },
  /* error  */ { PortAction::none,        PortAction::none,        PortAction::none,        PortAction::none,          PortAction::none,     PortAction::none,      PortAction::none },
								   			   			                                                  
  /* last   */ { PortAction::none,        PortAction::none,        PortAction::none,        PortAction::none,          PortAction::none,     PortAction::none,      PortAction::none },
};

// clang-format on
}  // namespace
/**
 * Class template representing states of a bound source and sink node (in the
 * two stage case) or the states of a bound source, sink node, and
 * intermediary (in the three stage case).  The class supplies an `event`
 * function for effecting state transitions (including application of exit and
 * entry functions).  The class is agnostic as to how the actions are actually
 * implemented by the users of the state machine.  A policy class is used by
 * the `PortFiniteStateMachine` to realize the specific state transition
 * actions.  It is assumed the the Policy class inherits from
 * `PortFiniteStateMachine` using CRTP.
 *
 * Extended documentation for the operation of the state machine for the
 * two-stage case can be found in * fsm.md.
 *
 * The `Policy` class is expected to use the `PortFiniteStateMachine` class
 * using CRTP so that functions provided by the `Policy` class are available
 * to the `PortFiniteStateMachine`. Documentation about action policy classes
 * can be found in policies.h.
 *
 * @todo Use an aspect class (as another template argument) to effect
 * callbacks at each interesting point in the state machine (such as debugging
 * statements).
 */

template <class Policy>
struct PortPolicyTraits {
  constexpr static bool wait_returns_{true};
};

template <class Policy, class PortState>
class PortFiniteStateMachine {
 private:
  PortState state_;
  PortState next_state_;

  constexpr static bool wait_returns_ = PortPolicyTraits<Policy>::wait_returns_;

 public:
  using scheduler_event_type = SchedulerAction;
  using port_state = PortState;
  using lock_type = std::unique_lock<std::mutex>;

  void foo() {
  }

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
   *
   * @todo Get rid of msg and event_counter -- they were only for debugging.
   */
  scheduler_event_type event(PortEvent event, const std::string&) {
    std::unique_lock lock(mutex_);

    if (state_ == PortState::error) {
      throw std::logic_error(
          "PortFiniteStateMachine::event: state_ == PortState::error");
    }

  retry:

    /*
     * Look up next state, along with exit and entry action.
     */
    next_state_ =
        transition_table<port_state>[to_index(state_)][to_index(event)];

    if (next_state_ == PortState::error) {
      throw std::logic_error(
          "PortFiniteStateMachine::event: next_state_ == PortState::error");
    }

    auto exit_action{exit_table<port_state>[to_index(state_)][to_index(event)]};
    auto entry_action{
        entry_table<port_state>[to_index(next_state_)][to_index(event)]};

    /**
     * Perform any exit actions.  Based on the exit action in the exit action
     * table, we dispatch to a function provided by the policy.  Since Policy
     * is assumed to be derived from this class, we use a `static_cast` to
     * `Policy*` on `this`.
     */
    switch (exit_action) {
      case PortAction::none:
        break;

      case PortAction::ac_return:
        return static_cast<Policy*>(this)->on_ac_return(lock, event_counter);

      case PortAction::source_move:
        // @todo Why does this not notify?
        static_cast<Policy*>(this)->on_source_move(lock, event_counter);
        break;

      case PortAction::sink_move:
        // @todo Why does this not notify?
        static_cast<Policy*>(this)->on_sink_move(lock, event_counter);
        break;

      case PortAction::source_wait:
        if (wait_returns_) {
          static_cast<Policy*>(this)->on_source_wait(lock, event_counter);
          goto retry;
        } else {
          return static_cast<Policy*>(this)->on_source_wait(
              lock, event_counter);
        }

      case PortAction::sink_wait:
        if (wait_returns_) {
          static_cast<Policy*>(this)->on_sink_wait(lock, event_counter);
          goto retry;
        } else {
          return static_cast<Policy*>(this)->on_sink_wait(lock, event_counter);
        }

      case PortAction::notify_source:
        return static_cast<Policy*>(this)->on_notify_source(
            lock, event_counter);

      case PortAction::notify_sink:
        return static_cast<Policy*>(this)->on_notify_sink(lock, event_counter);

      case PortAction::term_source:
        return static_cast<Policy*>(this)->on_term_source(lock, event_counter);

      case PortAction::term_sink:
        return static_cast<Policy*>(this)->on_term_sink(lock, event_counter);

      case PortAction::source_throw:
        throw std::logic_error(
            "PortFiniteStateMachine::event: "
            "exit_action == PortAction::source_throw");

      case PortAction::sink_throw:
        throw std::logic_error(
            "PortFiniteStateMachine::event: "
            "exit_action == PortAction::sink_throw");

      default:
        throw std::logic_error(
            "Unexpected exit action: " + str(exit_action) + ": " + str(state_) +
            " -> " + str(next_state_));
    }

    /*
     * Assign new state.
     */
    state_ = next_state_;

    /*
     * Update the entry_action in case we have come back from a wait.
     *
     * @todo Will the behavior of this change with different scheduling?
     */
    entry_action =
        entry_table<port_state>[to_index(next_state_)][to_index(event)];

    /**
     * Perform any entry actions.
     */
    switch (entry_action) {
      case PortAction::none:
        break;

      case PortAction::ac_return:
        return static_cast<Policy*>(this)->on_ac_return(lock, event_counter);

      case PortAction::source_move:
        static_cast<Policy*>(this)->on_source_move(lock, event_counter);
        return static_cast<Policy*>(this)->on_notify_sink(lock, event_counter);

        // @todo -- properly handle cases of notify returning and not returning
        /*
         * If we do a move on entry, we need to fix up the state, since we
         * have already passed the state transition step.
         *
         * @todo This seems like unreachable -- why is it here?
         */
        if constexpr (std::is_same_v<port_state, two_stage>) {
          switch (state_) {
            case port_state::st_10:
              state_ = port_state::st_01;
              break;
            case port_state::xt_10:
              state_ = port_state::xt_01;
              break;
            default:
              break;
          }
        } else if constexpr (std::is_same_v<port_state, three_stage>) {
          switch (state_) {
            case port_state::st_010:
              // Fall through
            case port_state::st_100:
              state_ = port_state::st_001;
              break;
            case port_state::st_110:
              // Fall through
            case port_state::st_101:
              state_ = port_state::st_011;
              break;
            case port_state::xt_010:
              // Fall through
            case port_state::xt_100:
              state_ = port_state::xt_001;
              break;
            case port_state::xt_110:
              // Fall through
            case port_state::xt_101:
              state_ = port_state::xt_011;
              break;

            default:
              break;
          }
        } else {
          std::cout << "should not be here" << std::endl;
          assert(false);
        }
        break;

      case PortAction::sink_move:
        static_cast<Policy*>(this)->on_sink_move(lock, event_counter);
        return static_cast<Policy*>(this)->on_notify_source(
            lock, event_counter);

        /*
         * If we do a move on entry, we need to fix up the state, since we
         * have already passed the state transition step.
         * @todo This seems like unreachable -- why is it here?
         */
        if constexpr (std::is_same_v<port_state, two_stage>) {
          switch (state_) {
            case port_state::st_10:
              state_ = port_state::st_01;
              break;
            case port_state::xt_10:
              state_ = port_state::xt_01;
              break;
            default:
              break;
          }
        } else if constexpr (std::is_same_v<port_state, three_stage>) {
          switch (state_) {
            case port_state::st_010:
              // Fall through
            case port_state::st_100:
              state_ = port_state::st_001;
              break;
            case port_state::st_110:
              // Fall through
            case port_state::st_101:
              state_ = port_state::st_011;
              break;
            case port_state::xt_010:
              // Fall through
            case port_state::xt_100:
              state_ = port_state::xt_001;
              break;
            case port_state::xt_110:
              // Fall through
            case port_state::xt_101:
              state_ = port_state::xt_011;
              break;

            default:
              break;
          }
        } else {
          std::cout << "should not be here" << std::endl;
          assert(false);
        }
        break;

      case PortAction::source_wait:
        return static_cast<Policy*>(this)->on_source_wait(lock, event_counter);

      case PortAction::sink_wait:
        return static_cast<Policy*>(this)->on_sink_wait(lock, event_counter);

      case PortAction::notify_source:
        return static_cast<Policy*>(this)->on_notify_source(
            lock, event_counter);

      case PortAction::notify_sink:
        return static_cast<Policy*>(this)->on_notify_sink(lock, event_counter);

      case PortAction::term_source:
        return static_cast<Policy*>(this)->on_term_source(lock, event_counter);

      case PortAction::term_sink:
        return static_cast<Policy*>(this)->on_term_sink(lock, event_counter);

      default:
        throw std::logic_error(
            "Unexpected entry action: " + str(entry_action) + ": " +
            str(state_) + " -> " + str(next_state_));
    }

    return scheduler_event_type::noop;
  }

 public:
  /**
   * Set state.
   */
  inline auto set_state(port_state next_state) {
    state_ = next_state;
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
   * Functions for invoking events in the StateMachine.  In general, clients
   * of the StateMachine will be using it via a mover class.  These are here
   * to enable direct testing of the state machine.
   */

  /**
   * Invoke `port_fill` event.
   */
  void port_fill(const std::string& msg = "") {
    event(PortEvent::source_fill, msg);
  }

  /**
   * Invoke `port_push` event.
   */
  void port_push(const std::string& msg = "") {
    event(PortEvent::source_push, msg);
  }

  /**
   * Invoke `try_push` event.
   */
  void port_try_push(const std::string& msg = "") {
    event(PortEvent::try_push, msg);
  }

  /**
   * Invoke `port_pull` event.
   */
  void port_pull(const std::string& msg = "") {
    event(PortEvent::sink_pull, msg);
  }

  /**
   * Invoke `try_pull` event.
   */
  void port_try_pull(const std::string& msg = "") {
    event(PortEvent::try_pull, msg);
  }

  /**
   * Invoke `port exhausted` event
   */
  void port_exhausted(const std::string& msg = "") {
    this->event(PortEvent::exhausted, msg);
  }

  /**
   * Invoke `port_drain` event.
   */
  void port_drain(const std::string& msg = "") {
    event(PortEvent::sink_drain, msg);
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
  [[nodiscard]] constexpr inline bool debug_enabled() const {
    return debug_;
  }
};

}  // namespace tiledb::common

#endif  // TILEDB_DAG_FSM_H
