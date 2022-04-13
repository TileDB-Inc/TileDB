/**
 * @file tiledb/api/proxy/proxy.h
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
 */

#ifndef TILEDB_API_PROXY_H
#define TILEDB_API_PROXY_H

#include <queue>
#include "tiledb/common/common.h"
#include "tiledb/common/heap_memory.h"  // for tdb_unique_ptr

namespace tiledb::api {

// Forward
template <class, class>
class ProxyAccessGuard;

enum class ProxyEvent : unsigned short {
  construct = 0,
  access_attach,

  /**
   * Release the external access to the underlying object.
   */
  access_release,
  destroy,

  /**
   * Shut down an underlying object but keep the object in existence. This
   * event only occurs if the underlying object has a `shutdown()` function.
   */
  shutdown,
};

namespace {
inline constexpr unsigned short to_index(ProxyEvent x) {
  return static_cast<unsigned short>(x);
}

/**
 * Number of events in the Proxy state machine
 */
constexpr unsigned int n_events = to_index(ProxyEvent::shutdown) + 1;

}  // namespace

/**
 * The states of the state machine.
 *
 * The state transitions as a cartoon in ASCII art:
 * ```
 * nascent --> present --> destroyed
 *  |           ^    |      |
 *  v           |    v      v
 * aborted      access --> last_access
 * ```
 */
enum class ProxyState {
  /**
   * The proxy is able to gather construction arguments. The constructor has
   * not yet been called. The underlying object variable is `nullopt`.
   *
   * This is the initial state.
   *
   * Events
   *   - construct. Transition to `present`. Construct the underlying object.
   *   - access_attach. Transition to `error`.
   *   - access_release. Transition to error.
   *   - destroy. Transition to `aborted`.
   *   - shutdown. Transition to `aborted`.
   */
  nascent = 0,

  /**
   * The constructor has been called; its result is stored in the underlying
   * object variable. Changes to construction arguments are ignored.
   *
   * Events
   *   - construct. Transition to `error`.
   *   - access_attach. Transition to `access`. Construct an access object.
   *   - access_release. Transition to `error`.
   *   - destroy. Transition to `destroyed`. Destroy the underlying object.
   *   - shutdown. Self-transition.
   */
  present,

  /**
   * Same as `present`, but there is a current `ProxyAccessGuard` in existence.
   *
   * Events
   *   - construct. Transition to `error`.
   *   - access_attach. Transition to `error`.
   *   - access_release. Transition to `present`. Destroy the access object.
   *   - destroy. Transition to `last_access`.
   *   - shutdown. Self-transition.
   */
  access,

  /**
   * Same as `access`, but a destroy event has been processed. When access is
   * released, the object will be destroyed.
   *
   * Events
   *   - construct. Transition to `error`.
   *   - access_attach. Transition to `error`.
   *   - access_release. Transition to `destroyed`. Destroy both the underlying
   *       and the access object.
   *   - destroy. Self-transition.
   *   - shutdown. Self-transition.
   */
  last_access,

  /**
   * An object has been destroyed and its storage deallocated. The underlying
   * object variable is 'nullopt'. Any side-effects of the constructor may
   * persist.
   *
   * This is a final state.
   *
   * Events
   *   - construct. Transition to `error`.
   *   - access_attach. Transition to `error`.
   *   - access_release. Transition to `error`.
   *   - destroy. Self-transition.
   *   - shutdown. Self-transition.
   */
  destroyed,

  /**
   * No object was ever constructed; there are no side-effects of a constructor.
   * The underlying object variable is 'nullopt'.
   *
   * This is a final state.
   *
   * Events
   *   - construct. Transition to `error`.
   *   - access_attach. Transition to `error`.
   *   - access_release. Transition to `error`.
   *   - destroy. Self-transition.
   *   - shutdown. Self-transition.
   */
  aborted,

  /**
   * The error state. Nothing is known about the proxy.
   *
   * This is a final state.
   *
   * All Events self-transition.
   */
  error
};

namespace {
constexpr unsigned short to_index(ProxyState x) {
  return static_cast<unsigned short>(x);
}
/**
 * Number of states in the Proxy state machine
 */
constexpr unsigned short n_states = to_index(ProxyState::error) + 1;
}  // namespace

enum class ProxyAction : unsigned short {
  none = 0,
  construct,
  access,
  release,
  r_and_d,  // release and destroy
  destroy,
  shutdown,
};

namespace {
// clang-format off
  /**
   * The default state transition table. Transitions here may be overridden by
   * transition actions.
   */
  constexpr ProxyState transition_table[n_states][n_events] {
                   /*construct*/        /*access_attach*/        /*access_release*/     /*destroy*/              /*shutdown*/
  /*nascent*/     {ProxyState::present,     ProxyState::error,       ProxyState::error,     ProxyState::aborted,     ProxyState::aborted},
  /*present*/     {ProxyState::present,     ProxyState::access,      ProxyState::error,     ProxyState::destroyed,   ProxyState::present},
  /*access*/      {ProxyState::access,      ProxyState::access,      ProxyState::present,   ProxyState::last_access, ProxyState::access},
  /*last_access*/ {ProxyState::last_access, ProxyState::last_access, ProxyState::destroyed, ProxyState::last_access, ProxyState::last_access},
  /*destroyed*/   {ProxyState::error,       ProxyState::error,       ProxyState::error,     ProxyState::destroyed,   ProxyState::destroyed},
  /*aborted*/     {ProxyState::error,       ProxyState::error,       ProxyState::error,     ProxyState::aborted,     ProxyState::aborted},
  /*error*/       {ProxyState::error,       ProxyState::error,       ProxyState::error,     ProxyState::error,       ProxyState::error},
  };

  /**
   * Transition actions table.
   */
  constexpr ProxyAction action_table[n_states][n_events] {
                    /*construct*/          /*access_attach*/    /*access_release*/    /*destroy*/           /*shutdown*/
  /*nascent*/     {ProxyAction::construct, ProxyAction::none,   ProxyAction::none,    ProxyAction::none,    ProxyAction::none},
  /*present*/     {ProxyAction::none,      ProxyAction::access, ProxyAction::none,    ProxyAction::destroy, ProxyAction::shutdown},
  /*access*/      {ProxyAction::none,      ProxyAction::access, ProxyAction::release, ProxyAction::none,    ProxyAction::shutdown},
  /*last_access*/ {ProxyAction::none,      ProxyAction::access, ProxyAction::r_and_d, ProxyAction::none,    ProxyAction::shutdown},
  /*destroyed*/   {ProxyAction::none,      ProxyAction::none,   ProxyAction::none,    ProxyAction::none,    ProxyAction::none},
  /*aborted*/     {ProxyAction::none,      ProxyAction::none,   ProxyAction::none,    ProxyAction::none,    ProxyAction::none},
  /*error*/       {ProxyAction::none,      ProxyAction::none,   ProxyAction::none,    ProxyAction::none,    ProxyAction::none},
  };

  /**
   * Blocking trait for events.
   *
   * This determines if an event will block waiting for the state mutex to
   * become available or not. If it's not available, the event will be processed
   * by the handler of some blocking event.
   */
  constexpr bool will_block[n_events] {
    /*construct*/ true,
    /*access_attach*/ true,
    /*access_release*/ true,
    /*destroy*/ true,
    /*shutdown*/ false,
  };

// clang-format on
}  // namespace

/**
 * Base class for `Proxy` that has responsibility for receiving events and
 * transitioning state.
 *
 * This class has minimal dependency on the type required by a proxy; only
 * whether the class has its own `shutdown()` member function. Thus there are
 * only two variations of the state machine, not one for every class that
 * passes through a proxy.
 *
 * The internal queue ensures that shutdown events are handled deterministically
 * at the earliest possible opportunity. Without a queue, an operation on an
 * underlying object can proceed even when a shutdown event is waiting, a
 * behavior contrary to the purpose of a shutdown.
 *
 * @tparam has_shutdown Predicate whether the underlying class has a member
 * function `shutdown()`
 */
template <bool has_shutdown>
class ProxyStateMachine {
 protected:
  using lock_type = std::unique_lock<std::mutex>;

 private:
  /**
   * The current state of the state machine.
   */
  ProxyState state_;

  /**
   * The number of currently outstanding accessors.
   *
   * @invariant n_accessors > 0 if and only if the state is `access` or
   * `last_access`
   */
  unsigned int n_accessors{0};

  /**
   * Mutex to synchronize queue operations.
   *
   * This is the first of two mutexes. Whenever both mutexes must be locked,
   * this one must be locked first.
   */
  std::mutex m_queue_{};

 protected:
  /**
   * Mutex to synchronize state transitions.
   *
   * The is the second of two mutexes. Whenever both mutexes must be locked,
   * this one must locked second.
   */
  std::mutex m_state_{};

 private:
  /**
   * The internal event queue.
   */
  std::queue<ProxyEvent> queue_{};

  /*
   * Event processing states
   *
   * In addition to the state machine proper, event processing has an implicit
   * state machine that depends upon the lock states of the two mutexes. There
   * are four states; each state has two possible outgoing transitions depending
   * on which mutex is changing. Thus there are eight possible transitions but
   * only five are used. This state machine is embodied in the control flow of
   * `event()`.
   *
   * ```
   *                            Queue
   *                    Unlocked     Locked
   *                +------------------------------
   *                |           <--
   *       Unlocked |    Quiet  --> Receive
   * State          |      ^           |
   *                |      |           v
   *         Locked |   Change <-- Hand-off
   *                |
   * ```
   *
   * Event processing does not have a separate thread. All events are processed
   * by the thread that calls `event()`. A call to `event` may result a few
   * kinds of behavior.
   *
   * 1. (Ordinary) The queue is empty and stays empty while the state machine
   *    processes the event. This results in one single cycle.
   * 2. The queue is not empty and the event is non-blocking. The message is
   *    enqueued. This event moves from `Receive` to `Quiet` and returns.
   * 3. The queue is not empty and the event is blocking. The message is
   *    enqueued and blocks waiting to move to 'Hand-off'. The loop continues
   *    until the queue is empty.
   *
   * Put more simply, the `event()` calls for blocking events are able to handle
   * queued non-blocking ones.
   *
   * This behavior is necessary to support prompt shutdown. The `shutdown` event
   * is non-blocking so that a top-level shutdown thread is not at risk of
   * blocking on any potentially long-lived state transitions. The `construct`
   * event is considered long lived because it may involve storage I/O
   * operations.
   */

  /**
   * Perform a state transition and return holding a lock that prevents other
   * state transitions.
   *
   * This function blocks if the event cannot be processed immediately. A block
   * may happen either in by the queue mutex or the state machine mutex.
   *
   * @pre `lock_state` does not hold a lock on the state mutex.
   *
   * @param lock A lock object to be used for the state machine
   */
  void event(lock_type& lock_state, ProxyEvent ev) {
    // Assert: event state is `Quiet`
    ProxyEvent event;
    /*
     * Create the queue lock always in the locked state. Queue processing has
     * nothing long-lived, so we always block for exclusive access to the queue.
     */
    std::unique_lock<std::mutex> lock_queue{m_queue_};
    // Assert: event state is `Receive`

    /*
     * Try the transition to `Hand-off`. If it doesn't immediately succeed and
     * we have a non-blocking event, queue the event and return.
     */
    if (lock_state.try_lock()) {
      // We have a lock on the state mutex
      // Assert: event state is `Hand-off`
    } else {
      // We do not have a lock on the state mutex
      if (will_block[to_index(ev)]) {
        /*
         * `lock()` blocks until it obtains a lock.
         */
        lock_state.lock();
        // Assert: event state is `Hand-off`
      } else {
        /*
         * We cannot immediately process a non-blocking event, so we
         * queue it up and leave.
         */
        queue_.push(ev);
        return;
        /*
         * Assert: event state is `Quiet` after the destructor for
         * `lock_queue` executes
         */
      }
      // Assert: event state is `Hand-off`
      // In we had a non-blocking event, this function already returned.
    }
    // Assert: event state is `Hand-off`
    // Whether `try_lock` locked or not, we now have a lock.

    if (queue_.empty()) {
      event = ev;
    } else {
      queue_.push(ev);
      event = queue_.front();
      queue_.pop();
    }
    // Assert: `event` is initialized

    while (true) {
      // Assert: event state is `Hand-off`.
      /*
       * Now that we're inside the state-change loop, we can release the queue
       * lock.
       */
      lock_queue.unlock();
      // Assert: event state is `Change`

      auto new_state{transition_table[to_index(state_)][to_index(ev)]};
      auto action{action_table[to_index(state_)][to_index(ev)]};
      /*
       * Actions each happen within a try-block to ensure state consistency in
       * case of an exception.
       */
      switch (action) {
        case ProxyAction::none:
          break;
        case ProxyAction::construct:
          try {
            if (do_construct()) {
              // Use the ordinary state transition
            } else {
              // Construction did not succeed. Stay in the `nascent` state.
              new_state = ProxyState::nascent;
            }
          } catch (...) {
            new_state = ProxyState::aborted;
            throw;
          }
          break;
        case ProxyAction::access:
          ++n_accessors;
          break;
        case ProxyAction::release:
          --n_accessors;
          if (n_accessors > 0) {
            // Stay in the `access` state instead of moving back to `present`.
            new_state = ProxyState::access;
          }
          break;
        case ProxyAction::r_and_d:
          --n_accessors;
          if (n_accessors > 0) {
            // Stay in the `last_access` state instead of moving back to
            // `present`.
            new_state = ProxyState::last_access;
            break;
          }
          // Assert: n_accessors == 0
          [[fallthrough]];
        case ProxyAction::destroy:
          try {
            do_destroy();
          } catch (...) {
            new_state = ProxyState::error;
            throw;
          }
          break;
        case ProxyAction::shutdown:
          try {
            do_shutdown();
          } catch (...) {
            new_state = ProxyState::error;
            throw;
          }
          break;
        default:
          throw std::logic_error("Not reachable");
      }
      /*
       * Assign new state only after action returns without throwing.
       */
      state_ = new_state;

      /*
       * At this point we can return if the message queue is empty. We cannot,
       * however, check the message queue reliably without a queue lock. And
       * we can't acquire a queue lock from the `Changing` state because of the
       * anti-deadlock ordering of the mutexes. Thus we have to unlock to check
       * the queue and then re-lock the state mutex.
       *
       * Thus we cycle through event states until we're back at `Hand-off`.
       * That's the required state at the top of the event loop. That state will
       * also transition to `Change` as required if we're done processing
       * events.
       */
      lock_state.unlock();
      // Assert: event state is `Quiet`
      lock_queue.lock();
      // Assert: event state is `Receive`
      lock_state.lock();
      // Assert: event state is `Hand-off`

      /*
       * We continue to process events as long as any remain in the queue. In
       * this phase we process all events. The `will_block` trait only applies
       * to events when first introduced, not once they've been in the queue.
       */
      if (queue_.empty()) {
        return;
        /*
         * Assert: event state is `Change` after the destructor for
         * `lock_queue` executes.
         */
      }
    }
  }

 protected:
  [[nodiscard]] inline ProxyState state() const {
    return state_;
  }

  [[nodiscard]] inline std::mutex& m_state() {
    return m_state_;
  }

  virtual bool do_construct() = 0;
  virtual void do_destroy() = 0;
  virtual void do_shutdown() = 0;

  /**
   * Default constructor because Proxy needs a default constructor.
   */
  ProxyStateMachine()
      : state_(ProxyState::nascent){};

  void event_access_attach(lock_type& lock) {
    event(lock, ProxyEvent::access_attach);
  }

  void event_access_release() {
    lock_type lock{m_state_, std::defer_lock};
    event(lock, ProxyEvent::access_release);
  }

  /**
   * Predicate whether the state machine is in the `access` state.
   *
   * This predicate is used to determine whether to grant access to an
   * underlying object.
   */
  [[nodiscard]] bool is_attached() const {
    return state_ == ProxyState::access || state_ == ProxyState::last_access;
  }

 public:
  /**
   * Order to construct implemented here for exposure through `Proxy`.
   *
   * If validation or construction throws, the proxy will enter its error state.
   * If validation does not succeed, construction will not happen and the proxy
   * will stay in the `nascent` state.
   */
  void construct() {
    lock_type lock{m_state_, std::defer_lock};
    event(lock, ProxyEvent::construct);
  }

  /**
   * Order to destroy implemented here for exposure through `Proxy`.
   */
  void destroy() {
    lock_type lock{m_state_, std::defer_lock};
    event(lock, ProxyEvent::destroy);
  }

  /**
   * Order to shut down implemented here for exposure through `Proxy`.
   */
  void shutdown() {
    if constexpr (has_shutdown) {
      lock_type lock{m_state_, std::defer_lock};
      event(lock, ProxyEvent::shutdown);
    } else {
      destroy();
    }
  }
};

/*
 * Life cycle concept
 *
 * The `LC` template argument of `Proxy` has a number of requirements, some of
 * which are expressible with C++20 `concept`, some not.
 *
 * * The class has a default constructor. This is necessary to allow `Proxy` to
 *   have a default constructor, since `Proxy` contains a member variable of
 *   this type.
 * * The class has a declaration `arguments_type` that holds arguments for the
 *   constructor during the nascent state of the proxy. The `Proxy` class is
 *   outside _how_ this class is used. Typically its member variables act like a
 *   C-style `struct` and are not proscribed by class invariants. There's no
 *   requirement that the arguments class be this simple. An arguments class can
 *   do more, but that's outside the purview of `Proxy`.
 * * The class `LC::arguments_type` has a default constructor.
 * * The class has a `B validate()` member function that says whether the
 *   gathered arguments are well-formed, where `B` is a bool-convertible return
 *   value. This can be anything from a constant return to a full
 *   pre-construction validation.
 * * The underlying class has construtor `T(LC::arguments_type&)`.
 * * The class has a `constexpr bool has_shutdown` member variable that
 *   indicates whether the class has a `shutdown()` method defined. A class can
 *   shutdown if it can refuse new operations that would change any internal
 *   state. This facility exists to allow classes to report error conditions
 *   gracefully instead of simply disappearing.
 */

/**
 * Proxy class for presenting a C.41 class through a non-C.41 API.
 *
 * The basic principle of this class is first to gather constructor arguments
 * and then to construct an object when it's first used. After construction,
 * new constructor arguments are ignored. A full elaboration of this principle
 * covers the entire life cycle of an object, but all the complexity starts with
 * a need to set constructor arguments piecemeal rather than all at once.
 *
 * This class supports external shutdown from an outside controller. Upon a
 * shutdown signal, this proxy moves into a state where no underlying object
 * exists. If an object never existed, it refuses to construct a new one. If an
 * object does exist, it is destroyed.
 *
 * This class is thread-safe. More precisely, this class as instantiated is
 * as thread-safe as its template arguments allow. All public member functions
 * may be called _safely_ in any order, even as not all sequences of function
 * calls are _sensible_.
 *
 * Public member functions come in two types: synchronous and asynchronous. A
 * synchronous member function completes is operation before returning. An
 * asynchronous member function may queue its operation if another one is in
 * progress, but it is not required to do so.
 *
 * The synchronous functions are the life cycle functions.
 * - args(). Pre-construction. No state transition.
 * - construct(). Construction. Transition to `present` state.
 * - access(). Existence. Transitions involving access states.
 * - destroy(). Destruction. Transition to a non-existent state.
 * The one asynchronous function is an overlay on top of life cycle.
 * - shutdown(). Shut down the object if possible; destroy it if not.
 *
 * All synchronous operations that cause state transitions are atomic with the
 * state transition. Argument gathering always happens in the `nascent` state
 * and never causes a state transition. If the arguments object of this `Proxy`
 * is accessed from multiple threads, it's the joint responsibility of the
 * arguments class and the underlying class to prevent a data race.
 *
 * While this class _does not_ synchronize the arguments instance with
 * construction, it _does_ synchronize access the underlying object during its
 * life span. In other words, the underlying object is central and the arguments
 * object is ancillary; they are not on equal footing. An alternate version of
 * this `Proxy` could put them on equal footing; the present design choice is
 * to avoid overhead.
 *
 * Races between functions that cause state transitions always resolve
 * coherently, but it's worth illustrating with a few cases:
 *
 * * A race between `construct()` and `destroy()` may resolve either to the
 *   `aborted` or to the `destroyed` state, depending on which executes first.
 * * A race between `shutdown()` and `destroy()` has the same outcome if the
 *   underlying class down not support its own shutdown; in both cases the
 *   object is destroyed by the first call and the second has no effect. If
 *   the class does have its own shutdown, the underlying object is always
 *   destroyed, but it might receive a shutdown order immediately before
 *   destruction.
 * * A race between argument gathering and `shutdown()` is always resolved to
 *   the `aborted` state. Any arguments gathered before shutdown are ignored, so
 *   it makes no difference whether they're collected beforehand. This
 *   situation, though, is a reason that `args()` returns `shared_ptr`, so that
 *   the proxy can properly abort and destroy its own reference to the arguments
 *   object while at the same time avoiding a segfault in a thread that's still
 *   gathering arguments.
 *
 * @tparam T The underlying C.41 type behind the proxy
 * @tparam LC Life cycle class
 */
template <class T, class LC>
class Proxy : public ProxyStateMachine<LC::has_shutdown> {
  using Base = ProxyStateMachine<LC::has_shutdown>;
  using lock_type = typename Base::lock_type;

  /**
   * The access guard is an accessory extension to this class.
   */
  friend class ProxyAccessGuard<T, LC>;

  /**
   * The arguments object
   */
  shared_ptr<typename LC::arguments_type> args_;

  /**
   * The underlying object that the proxy represents.
   */
  optional<T> underlying_{nullopt};

  /**
   * Construct the underlying object.
   */
  bool do_construct() override {
    if (!bool{args_->validate()}) {
      return false;
    }
    underlying_.emplace(*args_.get());
    /*
     * There may still be other references to the arguments object, but we get
     * rid of ours here.
     */
    args_.reset();
    return true;
  }

  /**
   * Destroy any underlying object
   *
   * The order to destroy a proxy may arrive in any state. This function to
   * actually destroy the underlying instance is only called when the state
   * machine transitions appropriately.
   */
  void do_destroy() override {
    underlying_.reset();
  }

  /**
   * Shut down the proxy object.
   */
  void do_shutdown() override {
    /*
     * If the underlying does not shut down itself, this function won't be
     * called. It's guarded with `if constexpr` so that the symbol `shutdown()`
     * will only be compiled when it's present.
     */
    if constexpr (LC::has_shutdown) {
      underlying_.value().shutdown();
    }
  }

  /**
   * The destructor of the accessor calls this function. This function processes
   * a state machine event that makes the state consistent with one fewer
   * accessors in existence.
   */
  inline void release_guard() {
    Base::event_access_release();
  }

 public:
  /**
   * A default constructor is required for this class because we assume nothing
   * about the initial construction of the object.
   */
  Proxy()
      : args_(make_shared<LC::arguments_type>(HERE())) {
  }

  /// Copy constructor is deleted
  Proxy(const Proxy&) = delete;
  /// Move constructor is deleted
  Proxy(Proxy&&) = delete;
  /// Copy assignment is deleted
  Proxy& operator=(const Proxy&) = delete;
  /// Move assignment is deleted
  Proxy& operator=(Proxy&&) = delete;

  /**
   * Accessor to the arguments object.
   *
   * @return Returns a pointer to the arguments object if this proxy is in
   * a nascent state. Returns a null pointer otherwise.
   */
  shared_ptr<typename LC::arguments_type> args() {
    return args_;
  }

  /**
   * Provide access to the underlying object through a guard that's expected to
   * have a short life span.
   *
   * In order to synchronize shutdown signals with access to the underlying
   * object, access is provided through a guard that will block transition out
   * of the set of access states (`access` and `last_access`).
   *
   * @return An access guard for this proxy.
   */
  ProxyAccessGuard<T, LC> access() {
    /*
     * This class processes access events because its template arguments are
     * necessarily part of the function signature. The base class processes
     * non-access events because their function signatures have no such
     * dependency.
     */
    lock_type lock{Base::m_state_, std::defer_lock};
    event_access_attach(lock);
    /*
     * At this point the state machine has processed the event. If the `proxy`
     * is able to provide an accessor, it will be in an attached state. In all
     * other cases the proxy will be in an error state.
     */
    if (!Base::is_attached()) {
      throw std::runtime_error(
          "Proxy::access() cannot provide an access guard in its error state");
    }
    return ProxyAccessGuard<T, LC>(*this);
  }
};

/**
 * Guard provides access to the underyling object of a `Proxy` instance and
 * coordinates with the state of the proxy.
 *
 * This class is a scoped extension of `Proxy`. It must have lifespan entirely
 * contained within that of the `Proxy` from which it's derived. Instances of
 * this class only exist in states `access` and `last_access`.
 *
 * RECOMMENDATION: Instance of this class should only be created as temporary
 * objects associated with a single use of the underlying object. Because the
 * lifespan is subordinate to that of `Proxy` and synchronized with its state, a
 * proxy cannot destroy its underlying object as long as there's an access guard
 * in existence. Using this class as a scoped guard ensures that the lifespan
 * overhead of this accessor is minimal.
 *
 * CAUTION: Multiple accessors to the same underlying instance may exist
 * contemporaneously. It is the responsibility of the class of that instance to
 * deal with simultaneous usage.
 *
 * @subsection Design Note
 *
 * An alternative design of this class might have had a default constructor and
 * allowed construction of an accessor in any state of a proxy. While this might
 * look apparently simpler to use, it would have accomplished is to push
 * synchronization with state transitions elsewhere, complicating user code to
 * require additional synchronization with every use of `value()`, rather than
 * being able to rely solely on existence of this guard for the necessary
 * conditions of access.
 *
 * @tparam T The underlying type template argument of a `Proxy` class
 * @tparam LC The life cycle template argument of a `Proxy` class
 */
template <class T, class LC>
class ProxyAccessGuard {
  /**
   * This class is an accessory to the proxy class and may only be constructed
   * by it.
   */
  friend class Proxy<T, LC>;

  /**
   * A pointer to the proxy object that created this object. If it's `nullptr`,
   * then this guard doesn't protect anything.
   */
  Proxy<T, LC>& source_;

  /**
   * This constructor is private and only accessible to `Proxy` class through a
   * `friend` declaration. It's always called under a state machine lock and
   * only in response to an access event.
   */
  explicit ProxyAccessGuard(Proxy<T, LC>& source) noexcept
      : source_(source){};

 public:
  /**
   * Copy constructor prohibited for guard class.
   *
   * If we were to allow copy construction, we'd need to interface with the
   * state machine to track reference counts. It's a guard class, though;
   * there's no need to copy a guard object.
   */
  ProxyAccessGuard(const ProxyAccessGuard&) = delete;

  /// Move constructor prohibited for guard class.
  ProxyAccessGuard(ProxyAccessGuard&&) = delete;

  /// Copy assignment prohibited for guard class.
  const ProxyAccessGuard& operator=(const ProxyAccessGuard&) = delete;

  /// Move assignment prohibited for guard class.
  const ProxyAccessGuard& operator=(ProxyAccessGuard&&) = delete;

  /**
   * Destructor triggers a state transition out of an access state of the proxy.
   */
  ~ProxyAccessGuard() {
    source_.release_guard();
  }

  /**
   * Accessor to the underlying object.
   *
   * The underlying object is guaranteed to exist. The constructor is only
   * called in an access state, and in such a state an underlying object always
   * exists.
   *
   * @return A reference to the underlying object of the `Proxy` that created
   * the present gaurd instance.
   */
  T& value() {
    return source_.underlying_.value();
  }
};

}  // namespace tiledb::api
#endif  // TILEDB_API_PROXY_H
