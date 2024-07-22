/**
 * @file query_state.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 TileDB, Inc.
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
 * This file declares a state machine for processing local queries. This
 * includes enumerations for the states and the events as well as the state
 * machine class itself.
 *
 * @section Distinction between `LocalQueryState` and `QueryStatus`
 *
 * There are two basic distinctions
 *   - `QueryStatus` is externally-visible, used for reporting summary status,
 *     and `LocalQueryState` is internal-only, used for executing the query.
 *   - `QueryStatus` applies to both local and remote queries. `LocalQueryState`
 *     is only for local queries.
 *
 * As a point of development history, `QueryStatus` originally served both the
 * internal implementation as well as the external interface. This was never a
 * good idea, since externally-visible types are part of the API and should not
 * change frequently. On the other hand, ordinary development periodically needs
 * to change implementation details and should not be bound away from that
 * because of a sticky API.
 *
 * Roughly speaking, there's a many-to-one relationship between internal states
 * and external statuses. For example, there's only one "failed" status
 * externally, but several possible internal states that map to "failed".
 *
 * @section State Machine
 *
 * This file contains a fully-encapsulated state machine that encapsulated the
 * state. Direct assignment of the state is not possible; the state changes
 * only as a result of events, which cause state transitions.
 *
 * @section C.41 concerns with `class Query`
 *
 * In the ideal world, `class Query` would be C.41 compliant and there would
 * be a designated set of initial states depending on whether the query was new
 * (never processed) or was resuming from suspension (in-progress but halted).
 * This is not currently the case, so accommodations must be made. The
 * accommodations take the form of additional mechanisms that supplement the
 * ideal, rather than ones that require changing it. Thus we have the following
 * design policy:
 *   - All initial states for a C.41 compliant `class Query` are present in
 *     the transitional state machine.
 *   - `class QueryState` may only be constructed in an initial state.
 *
 * These are the accommodations:
 *   - There is an additional event `ready`. The event does nothing on ordinary
 *     states. Ordinary states are all those that will be present after C.41
 *     compliance is achieved.
 *   - There are extra states. Each initial state is doubled, representing a
 *     fully-initialized state and a state that's not yet fully initialized.
 *     - These not-fully-initialized states are not ordinary states, but they
 *       are initial states.
 *     - The `ready` event transitions a not-fully-initialized state to its
 *       corresponding fully-initialized one.
 *   - The assignment operator is defined. This is necessary as a transition
 *     mechanism because at present initialization is done after construction.
 *     There are no separate constructors at present for new vs. resumed
 *     queries; once such constructors exist there will be no need for
 *     assignment.
 *
 * By design, the `ready` event is redundant once C.41 compliance is achieved.
 * As part of writing a C.41-compliant constructor, the ordinary initial state
 * will be used directly; neither the extra, mirrored initial state will appear
 * nor will assignment. Once `class Query` is entirely C.41 compliant, the
 * `ready` event can be removed along with all the invocations of the event,
 * as well as the extra states. At that point the assignment operator can be
 * deleted.
 *
 * @section Maturity
 *
 * At present the state machine is incomplete. It's not yet complete enough
 * even to properly have a map to externally-visible `QueryStatus`.
 *
 * The existing code in `class Query` is not yet ready to rely upon a state
 * machine to manage its state; it will have to transition to it over time. The
 * most glaring deficiency is that all the code does explicit state assignment.
 * There is no notion of events or formal state transition.
 *
 * The current version of the state machine is written to support only a single
 * purpose: to track the cancellation state of the query. At present the
 * cancellation operation is inconsistent and unreliable. There are, indeed, two
 * different kinds of cancellation that do not yield the same result.
 *   - A function `Query::cancel`, which simply puts the query into the "failed"
 *     status.
 *   - A function `StorageManager::cancel_all_tasks`, which sets an internal
 *     state retrievable through `StorageManager::cancellation_in_progress`.
 * Cancellation processing happens through the macro `RETURN_CANCEL_OR_ERROR`,
 * which internally calls `cancellation_in_progress`, which solely determines
 * whether processing is interrupted. `RETURN_CANCEL_OR_ERROR` does not consult
 * any local state, and thus `Query::cancel` cannot interrupt the processing of
 * a query.
 *
 * Accordingly, with regard to cancellation, these are the immediate goals for
 * the initial version of `class QueryState`:
 *   - A sufficient number of states to distinguish between "cancelled" and
 *     other states, to reject new operations on cancelled queries, and to
 *     handle logic errors.
 *   - A "cancel" event.
 *   - A predicate function on the state that returns whether processing of a
 *     query may proceed.
 */

#include <mutex>
#include <type_traits>

#ifndef TILEDB_QUERY_STATE_H
#define TILEDB_QUERY_STATE_H

namespace tiledb::sm {

/**
 * The set of life cycle states of a locally-processed query.
 *
 * Note that these states do not represent the states of a remotely-processed
 * query.
 *
 * @section Initial States
 *
 * The only initial state at present is `uninitialized`. This name befits the
 * C.41-noncompliance of the current `class Query`.
 *
 * @section Final States
 *
 * The final states follow the Tolstoy principle: "All happy families are alike;
 * each unhappy family is unhappy in its own way." There is a single final state
 * for a successfully completed query, and multiple final states for
 * unsuccessful queries.
 *
 *   - Success. The query completed by returning all its results.
 *     - `success`
 *   - Failure. The query did not complete. If it returned partial results,
 *     these results may or may not be all the results.
 *     - External causes. These arise from causes external to the code.
 *       - `aborted`: An error occurred because some external obstacle prevented
 *         the query from completing successfully.
 *       - `cancelled`: The query was directed to halt either by explicit
 *         command, cancellation of all activity on a context, or shutdown of
 *         the library
 *     - Internal causes. These arise internally from a defect in the code.
 *       - `error`. An event occurred in a state where it should not have
 *         occurred.
 *
 * @section Maturity
 *
 * The current states do not make an attempt to model the full life cycle of
 * a query.
 */
enum class LocalQueryState {
  /**
   * The state on construction of a C.41-noncompliant query object
   *
   * This is an initial state
   */
  uninitialized = 0,
  /**
   * All states not otherwise specified
   */
  everything_else,
  /**
   * The query has successfully completed and returned all its results.
   *
   * This is a final state.
   */
  success,
  /**
   * The query aborted during processing.
   *
   * This is a final state.
   *
   * The query need not have returned any results to enter this state. The
   * "aborted" state represents external causes for failure to complete.
   */
  aborted,
  /**
   * The query was cancelled during processing, either directly or indirectly.
   *
   * This is a final state.
   */
  cancelled,
  /**
   * The query encountered a fault that caused the query to fail.
   *
   * This is a final state.
   */
  error
};

/**
 * The number of local query states. This is the same as the number of
 * enumeration constants defined in `LocalQueryState`.
 */
constexpr size_t n_local_query_states = 6;

enum class LocalQueryEvent { ready, finish, abort, cancel };

/**
 * The number of local query events. This is the same as the number of
 * enumeration constants defined in `LocalQueryEvent`.
 */
constexpr size_t n_local_query_events = 4;

/**
 * The state machine for local processing of queries.
 *
 * @section Design
 *
 * This state machine is a tracking machine that follows the execution of a
 * query. There's no _a priori_ relationship between external events and the
 * events of this state machine. Instead, the events are generated from within
 * the query code.
 *
 * There is no way of manually changing states through assignment. The only way
 * to get the state machine into a particular state is one of two ways:
 *   - Construction of a state machine in a permissible initial state.
 *   - Transitions in state caused by events.
 */
class LocalQueryStateMachine {
  /**
   * Mutex that protects atomicity of state transitions
   *
   * All accesses to `state`, even trivial ones, need to be serialized though
   * the mutex.
   */
  mutable std::mutex m_{};

  /**
   * The current state of the machine.
   */
  LocalQueryState state_;

  /**
   * Conversion function from a query state to its integral representation
   *
   * This function is private because manipulation of the integers behind the
   * state machine is solely the purview of the state machine. Outside code
   * does not need this function, and if it thinks it does, it's defective.
   */
  static std::underlying_type_t<LocalQueryState> index_of(LocalQueryState s) {
    return static_cast<std::underlying_type_t<LocalQueryState>>(s);
  }

  static std::underlying_type_t<LocalQueryEvent> index_of(LocalQueryEvent e) {
    return static_cast<std::underlying_type_t<LocalQueryEvent>>(e);
  }

  /**
   * The predicate function `is_initial` represented as an array.
   */
  static constexpr bool initials_[n_local_query_states]{
      true, false, false, false, false, false};

  /**
   * The predicate function `is_final` represented as an array.
   */
  static constexpr bool finals_[n_local_query_states]{
      false, false, true, true, true, true};

 public:
  /**
   * The default constructor is deleted.
   *
   * Deleting this constructor is a design choice. We do not yet, but will need
   * to, construct objects in different initial states depending on whether
   * they're new queries or queries resuming from suspension.
   *
   * @section Maturity
   *
   * There is only a single initial state at present. That's not a good argument
   * for defining a default constructor. There will be multiple initial states;
   * there's no need for self-inflicted harm by writing code known to need to
   * change.
   */
  LocalQueryStateMachine() = delete;

  /**
   * Ordinary constructor at a given initial state.
   */
  LocalQueryStateMachine(LocalQueryState s);

  /**
   * Process an event on the state machine.
   *
   * @section Design
   *
   * This function is _not_ declared `noexcept`. At present, the implementation
   * does not generate exceptions, but in the future it will. In particular,
   * entering the `error` state (or self-transitioning in it) will throw an
   * exception.
   *
   * @param e The event that will trigger a transition.
   */
  void event(LocalQueryEvent e);

  /**
   * Accessor for the internal state
   */
  LocalQueryState state() const;

  /**
   * Predicate that the machine is in an initial state
   */
  bool is_initial() const {
    return initials_[index_of(state())];
  }

  /**
   * Predicate that the machine is in a final state
   */
  bool is_final() const {
    return finals_[index_of(state())];
  }

  /**
   * Predicate that the machine is in a cancelled state
   */
  bool is_cancelled() const {
    return state() == LocalQueryState::cancelled;
  }
};

}  // namespace tiledb::sm

#endif  // TILEDB_QUERY_STATE_H
