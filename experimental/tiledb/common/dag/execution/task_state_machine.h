/**
 * @file   task_state_machine.h
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
 * This file defines the scheduler state machine for the TileDB task graph
 * library.  The state machine manages state transitions of tasks executing in a
 * task graph.
 *
 * Task states follow the typical OS 101 states and state transitions.  Our
 * case is vastly simplified from the general OS case, however.  The tasks
 * being executed are tasks within the TileDB task graph library.  Tasks
 * execute specified functions and they send and receive data.  Tasks only
 * need to wait when the task is unable to send or receive data on a channel.
 * Tasks are handled at the user level and switching from one task to another
 * is completely cooperative.
 */

#ifndef TILEDB_DAG_SCHEDULER_H
#define TILEDB_DAG_SCHEDULER_H

#include <cassert>
#include "experimental/tiledb/common/dag/execution/threadpool.h"

namespace tiledb::common {
/*
 * Below we just use standard OS scheduling terminology.  For our purposes a
 * "Task" is a node (perhaps wrapped up in another class).
 */

/** Possible states of a task. */
enum class TaskState {
  created,
  runnable,
  running,
  waiting,
  terminated,
  error,
  last
};

/** Utility function to convert a TaskState to an index. */
constexpr unsigned short to_index(TaskState x) {
  return static_cast<unsigned short>(x);
}
/** Strings corresponding to each TaskState. Useful for testing and debugging.
 */
namespace {
std::vector<std::string> task_state_strings{
    "created", "runnable", "running", "waiting", "terminated", "error", "last"};
}

/** Utility function to convert a TaskState to a string. */
static inline auto str(TaskState st) {
  return task_state_strings[to_index(st)];
}

/** Function to test validity of a state variable. */
constexpr unsigned short num_task_states = to_index(TaskState::last) + 1;
static inline bool is_valid_state(TaskState st) {
  return to_index(st) >= to_index(TaskState::created) &&
         to_index(st) < to_index(TaskState::last);
}

/** Possible events that can cause a task state transition. */
enum class TaskEvent {
  create,
  admit,
  dispatch,
  wait,
  notify,
  exit,
  yield,
  noop,
  error,
  last
};

/** Utility function to convert a TaskEvent to an index. */
constexpr unsigned short to_index(TaskEvent x) {
  return static_cast<unsigned short>(x);
}

/** Strings corresponding to each TaskEvent. Useful for testing and debugging.
 */
namespace {
std::vector<std::string> task_event_strings{
    "create",
    "admit",
    "dispatch",
    "wait",
    "notify",
    "exit",
    "yield",
    "noop",
    "error",
    "last"};
}

/** Utility function to convert a TaskEvent to a string. */
static inline auto str(TaskEvent st) {
  return task_event_strings[to_index(st)];
}

/** Function to test validity of an event variable. */
static inline bool is_valid_event(TaskEvent st) {
  return to_index(st) >= to_index(TaskEvent::create) &&
         to_index(st) < to_index(TaskEvent::last);
}

/** Possible actions that can be taken when a task state transition occurs. */
enum class TaskAction {
  none,
  create,
  stop_create,
  make_runnable,
  stop_runnable,
  make_running,
  stop_running,
  make_waiting,
  stop_waiting,
  ac_return,
  terminate,
  last
};

/** Utility function to convert a TaskAction to an index. */
constexpr unsigned short to_index(TaskAction ac) {
  return static_cast<unsigned short>(ac);
}

/** Strings corresponding to each TaskAction. Useful for testing and debugging.
 */
namespace {
std::vector<std::string> task_action_strings{
    "none",
    "create",
    "stop_create",
    "make_runnable",
    "stop_runnable",
    "make_running",
    "stop_running",
    "make_waiting",
    "stop_waiting",
    "ac_return",
    "terminate",
    "last"};
}

/** Utility function to convert a TaskAction to a string. */
static inline auto str(TaskAction ac) {
  return task_action_strings[to_index(ac)];
}

/** Function to test validity of an action variable. */
static inline bool is_valid_event(TaskAction st) {
  return to_index(st) >= to_index(TaskAction::create) &&
         to_index(st) < to_index(TaskAction::last);
}

constexpr unsigned short num_task_events = to_index(TaskEvent::last) + 1;

/**
 * enum class for port actions associated with transitions.
 */
enum class SchedulerAction : unsigned short {
  noop,
  yield,
  notify_source,  // To scheduler
  notify_sink,    // To scheduler
  source_wait,    // To scheduler
  sink_wait,      // To scheduler
  source_exit,    // To scheduler
  sink_exit,      // To scheduler
  error,          // General error condition
  last,
};

inline constexpr unsigned short to_index(SchedulerAction x) {
  return static_cast<unsigned short>(x);
}

/**
 * Number of actions in the Port state machine
 */
constexpr unsigned int n_sch_actions = to_index(SchedulerAction::last) + 1;

/**
 * Strings for each enum member, useful for debugging.
 */
static std::vector<std::string> sch_action_strings{
    "noop",
    "yield",
    "notify_source",
    "notify_sink",
    "source_wait",
    "sink_wait",
    "source_exit",
    "sink_exit",
    "error",
    "last",
};

/**
 * Function to convert an action to a string.
 *
 * @param ac The action to stringify.
 * @return The string corresponding to the action.
 */
static auto inline str(SchedulerAction ac) {
  return sch_action_strings[static_cast<int>(ac)];
}

namespace detail {

/**
 * Tables for state transitions, exit events, and entry events.  Indexed by
 * state and event.
 * @todo noop and error (and last) do not have columns
 */

// clang-format off
constexpr const TaskState transition_table[num_task_states][num_task_events]{
  /* state      */      /* create */        /* admit */          /* dispatch */         /* wait */          /* notify */           /* exit */             /* yield */
  /* created    */    { TaskState::created, TaskState::runnable, TaskState::error,      TaskState::error,   TaskState::error,      TaskState::error,      TaskState::error      },
  /* runnable   */    { TaskState::error,   TaskState::error,    TaskState::running,    TaskState::waiting, TaskState::runnable,   TaskState::terminated, TaskState::error      },
  /* running    */    { TaskState::error,   TaskState::error,    TaskState::error,      TaskState::waiting, TaskState::running,    TaskState::terminated, TaskState::runnable   },
  /* waiting    */    { TaskState::error,   TaskState::error,    TaskState::error,      TaskState::error,   TaskState::runnable,   TaskState::error,      TaskState::waiting    },
  /* terminated */    { TaskState::error,   TaskState::error,    TaskState::terminated, TaskState::error,   TaskState::terminated, TaskState::error,      TaskState::terminated },
  /* error      */    { TaskState::error,   TaskState::error,    TaskState::error,      TaskState::error,   TaskState::error,      TaskState::error,      TaskState::error      },
  /* last       */    { TaskState::error,   TaskState::error,    TaskState::error,      TaskState::error,   TaskState::error,      TaskState::error,      TaskState::error      },
};

constexpr const TaskAction exit_table[num_task_states][num_task_events]{
  /* state      */      /* create */      /* admit */              /* dispatch */             /* wait */                 /* notify */                    /* exit */                /* yield */
  /* created    */    { TaskAction::none, TaskAction::stop_create, TaskAction::none,          TaskAction::none,          TaskAction::none,               TaskAction::none,         TaskAction::none         },
  /* runnable   */    { TaskAction::none, TaskAction::none,        TaskAction::stop_runnable, TaskAction::stop_runnable, TaskAction::ac_return,          TaskAction::none,         TaskAction::none         },
  /* running    */    { TaskAction::none, TaskAction::none,        TaskAction::none,          TaskAction::stop_running,  TaskAction::ac_return /*none*/, TaskAction::stop_running, TaskAction::stop_running },
  /* waiting    */    { TaskAction::none, TaskAction::none,        TaskAction::none,          TaskAction::none,          TaskAction::stop_waiting,       TaskAction::none,         TaskAction::none         },
  /* terminated */    { TaskAction::none, TaskAction::none,        TaskAction::stop_runnable, TaskAction::none,          TaskAction::none,               TaskAction::none,         TaskAction::none         },
  /* error      */    { TaskAction::none, TaskAction::none,        TaskAction::none,          TaskAction::none,          TaskAction::none,               TaskAction::none,         TaskAction::none         },
  /* last       */    { TaskAction::none, TaskAction::none,        TaskAction::none,          TaskAction::none,          TaskAction::none,               TaskAction::none,         TaskAction::none         },
};

constexpr const TaskAction entry_table[num_task_states][num_task_events]{
  /* state      */      /* create */        /* admit */                /* dispatch */            /* wait */                /* notify */               /* exit */             /* yield */
  /* created    */    { TaskAction::create, TaskAction::none,          TaskAction::none,         TaskAction::none,         TaskAction::none,          TaskAction::none,      TaskAction::none          },
  /* runnable   */    { TaskAction::none,   TaskAction::make_runnable, TaskAction::none,         TaskAction::none,         TaskAction::make_runnable, TaskAction::none,      TaskAction::make_runnable },
  /* running    */    { TaskAction::none,   TaskAction::none,          TaskAction::make_running, TaskAction::none,         TaskAction::none,          TaskAction::none,      TaskAction::none          },
  /* waiting    */    { TaskAction::none,   TaskAction::none,          TaskAction::make_waiting, TaskAction::make_waiting, TaskAction::none,          TaskAction::none,      TaskAction::none          },
  /* terminated */    { TaskAction::none,   TaskAction::none,          TaskAction::none,         TaskAction::none,         TaskAction::none,          TaskAction::terminate, TaskAction::none          },
  /* error      */    { TaskAction::none,   TaskAction::none,          TaskAction::none,         TaskAction::none,         TaskAction::none,          TaskAction::none,      TaskAction::none          },
  /* last       */    { TaskAction::none,   TaskAction::none,          TaskAction::none,         TaskAction::none,         TaskAction::none,          TaskAction::none,      TaskAction::none          },
};

// Had some merge trouble. Keeping this until completely tested, just in case
#if 0
  constexpr const TaskState transition_table[num_task_states][num_task_events]{
    // enum class TaskState { created, runnable, running, waiting, terminated,
    // last };
    // enum class TaskEvent { admit, dispatch, wait, notify, exit, yield, last
    // };
    // clang-format on
    /* state      */ /* create */ /* admit */ /* dispatch */ /* wait */
    /* notify */ /* exit */                                  /* yield */
    /* created    */ {
        TaskState::created,
        TaskState::runnable,
        TaskState::error,
        TaskState::error,
        TaskState::error,
        TaskState::error,
        TaskState::error,
    },
    /* runnable   */
    {
        TaskState::error,
        TaskState::error,
        TaskState::running,
        TaskState::waiting,
        TaskState::runnable,
        TaskState::terminated,
        TaskState::error,
    },
    /* running    */
    {
        TaskState::error,
        TaskState::error,
        TaskState::error,
        TaskState::waiting,
        TaskState::running,
        TaskState::terminated,
        TaskState::runnable,
    },
    /* waiting    */
    {
        TaskState::error,
        TaskState::error,
        TaskState::error,
        TaskState::error,
        TaskState::runnable,
        TaskState::error,
        TaskState::waiting,
    },
    /* terminated */
    {
        TaskState::error,
        TaskState::error,
        TaskState::terminated,
        TaskState::error,
        TaskState::terminated,
        TaskState::error,
        TaskState::terminated,
    },
    /* error      */
    {
        TaskState::error,
        TaskState::error,
        TaskState::error,
        TaskState::error,
        TaskState::error,
        TaskState::error,
        TaskState::error,
    },
    /* last       */
    {
        TaskState::error,
        TaskState::error,
        TaskState::error,
        TaskState::error,
        TaskState::error,
        TaskState::error,
        TaskState::error,
    },

};

constexpr const TaskAction exit_table[num_task_states][num_task_events]{
    /* state      */ /* create */ /* admit */ /* dispatch */ /* wait */
    /* notify */ /* exit */                                  /* yield */
    /* created    */ {
        TaskAction::none,
        TaskAction::stop_create,
        TaskAction::none,
        TaskAction::none,
        TaskAction::none,
        TaskAction::none,
        TaskAction::none,
    },
    /* runnable   */
    {
        TaskAction::none,
        TaskAction::none,
        TaskAction::stop_runnable,
        TaskAction::stop_runnable /*none*/,
        TaskAction::ac_return,
        TaskAction::none,
        TaskAction::none,
    },
    /* running    */
    {
        TaskAction::none,
        TaskAction::none,
        TaskAction::none,
        TaskAction::stop_running,
        TaskAction::ac_return /*none*/,
        TaskAction::stop_running,
        TaskAction::stop_running,
    },
    /* waiting    */
    {
        TaskAction::none,
        TaskAction::none,
        TaskAction::none,
        TaskAction::none,
        TaskAction::stop_waiting,
        TaskAction::none,
        TaskAction::none,
    },
    /* terminated */
    {
        TaskAction::none,
        TaskAction::none,
        TaskAction::stop_runnable,
        TaskAction::none,
        TaskAction::none,
        TaskAction::none,
        TaskAction::none,
    },
    /* error      */
    {
        TaskAction::none,
        TaskAction::none,
        TaskAction::none,
        TaskAction::none,
        TaskAction::none,
        TaskAction::none,
        TaskAction::none,
    },
    /* last       */
    {
        TaskAction::none,
        TaskAction::none,
        TaskAction::none,
        TaskAction::none,
        TaskAction::none,
        TaskAction::none,
        TaskAction::none,
    },
};

constexpr const TaskAction entry_table[num_task_states][num_task_events]{
    /* state      */ /* create */ /* admit */ /* dispatch */ /* wait */
    /* notify */ /* exit */                                  /* yield */
    /* created    */ {
        TaskAction::create,
        TaskAction::none,
        TaskAction::none,
        TaskAction::none,
        TaskAction::none,
        TaskAction::none,
        TaskAction::none,
    },
    /* runnable   */
    {
        TaskAction::none,
        TaskAction::make_runnable,
        TaskAction::none,
        TaskAction::none,
        TaskAction::make_runnable,
        TaskAction::none,
        TaskAction::make_runnable,
    },
    /* running    */
    {
        TaskAction::none,
        TaskAction::none,
        TaskAction::make_running,
        TaskAction::none,
        TaskAction::none,
        TaskAction::none,
        TaskAction::none,
    },
    /* waiting    */
    {
        TaskAction::none,
        TaskAction::none,
        TaskAction::make_waiting,
        TaskAction::make_waiting,
        TaskAction::none,
        TaskAction::none,
        TaskAction::none,
    },
    /* terminated */
    {
        TaskAction::none,
        TaskAction::none,
        TaskAction::none,
        TaskAction::none,
        TaskAction::none,
        TaskAction::terminate,
        TaskAction::none,
    },
    /* error      */
    {
        TaskAction::none,
        TaskAction::none,
        TaskAction::none,
        TaskAction::none,
        TaskAction::none,
        TaskAction::none,
        TaskAction::none,
    },
    /* last       */
    {
        TaskAction::none,
        TaskAction::none,
        TaskAction::none,
        TaskAction::none,
        TaskAction::none,
        TaskAction::none,
        TaskAction::none,
    },
};
#endif
// clang-format on
}  // namespace detail

// Declare a traits class.
template <typename T>
struct SchedulerTraits;
template <class Task>
class ThrowCatchSchedulerPolicy;
template <class Task>
class EmptySchedulerPolicy;
template <typename T>
struct SchedulerTraits;

/**
 * A state machine for managing task state transitions.
 * @tparam Policy Class providing actual actions associated with state
 * transitions.
 */
template <class Policy>
class SchedulerStateMachine {
  std::mutex mutex_;
  bool debug_{false};

 public:
  using task_handle_type = typename SchedulerTraits<Policy>::task_handle_type;
  using lock_type = std::unique_lock<std::mutex>;

  /**
   * Default constructor.
   */
  SchedulerStateMachine() = default;

  /**
   * Default move constructor.
   */
  SchedulerStateMachine(SchedulerStateMachine&&) noexcept = default;

  /**
   * Nonsense copy constructor, needed so that the `SchedulerStateMachine`
   * meets movable concept requirements.
   */
  SchedulerStateMachine(const SchedulerStateMachine&) {
    std::cout << "Nonsense copy constructor" << std::endl;
  }

 protected:
  /**
   * @brief Main function for transitioning a task to a new state.
   * @param event
   * @param task
   */
  void event(TaskEvent event, task_handle_type& task, const std::string&) {
    // std::unique_lock lock(mutex_);

    if (!is_valid_state(task->task_state())) {
      throw std::runtime_error("Invalid state: " + str(task->task_state()));
    }
    if (!is_valid_event(event)) {
      throw std::runtime_error("Invalid event: " + str(event));
    }

    auto next_state =
        detail::transition_table[to_index(task->task_state())][to_index(event)];

    auto exit_action{
        detail::exit_table[to_index(task->task_state())][to_index(event)]};

    auto entry_action{
        detail::entry_table[to_index(next_state)][to_index(event)]};

    if (next_state == TaskState::error) {
      auto msg = "Invalid state --  event: " + str(event) +
                 ", state: " + str(task->task_state()) +
                 ", next_state: " + str(next_state) +
                 ", exit_action: " + str(exit_action) +
                 ", entry_action: " + str(entry_action);
      throw std::runtime_error(msg);
    }
    if (task->task_state() == TaskState::error) {
      auto msg = "Invalid state --  event: " + str(event) +
                 ", state: " + str(task->task_state()) +
                 ", next_state: " + str(next_state) +
                 ", exit_action: " + str(exit_action) +
                 ", entry_action: " + str(entry_action);
      throw std::runtime_error(msg);
    }

    /* Process actions.  Should be self-explanatory. */
    switch (exit_action) {
      case TaskAction::none:
        break;

      case TaskAction::create:
        static_cast<Policy*>(this)->on_create(task);
        break;

      case TaskAction::stop_create:
        static_cast<Policy*>(this)->on_stop_create(task);
        break;

      case TaskAction::make_runnable:
        static_cast<Policy*>(this)->on_make_runnable(task);
        break;

      case TaskAction::stop_runnable:
        static_cast<Policy*>(this)->on_stop_runnable(task);
        break;

      case TaskAction::make_running:
        static_cast<Policy*>(this)->on_make_running(task);
        break;

      case TaskAction::stop_running:
        static_cast<Policy*>(this)->on_stop_running(task);
        break;

      case TaskAction::make_waiting:
        static_cast<Policy*>(this)->on_make_waiting(task);
        break;

      case TaskAction::stop_waiting:
        static_cast<Policy*>(this)->on_stop_waiting(task);
        break;

      case TaskAction::ac_return:
        return;
        // break;

      case TaskAction::terminate:
        static_cast<Policy*>(this)->on_terminate(task);
        break;

      case TaskAction::last:
        [[fallthrough]];
      default:
        throw(std::logic_error("Bad action"));
    }

    /* Make the actual state transition. */
    task->task_state() = next_state;

    /* Process actions.  Should be self-explanatory. */
    switch (entry_action) {
      case TaskAction::none:
        break;

      case TaskAction::create:
        static_cast<Policy*>(this)->on_create(task);
        break;

      case TaskAction::stop_create:
        static_cast<Policy*>(this)->on_stop_create(task);
        break;

      case TaskAction::make_runnable:
        static_cast<Policy*>(this)->on_make_runnable(task);
        break;

      case TaskAction::stop_runnable:
        static_cast<Policy*>(this)->on_stop_runnable(task);
        break;

      case TaskAction::make_running:
        static_cast<Policy*>(this)->on_make_running(task);
        break;

      case TaskAction::stop_running:
        static_cast<Policy*>(this)->on_stop_running(task);
        break;

      case TaskAction::make_waiting:
        static_cast<Policy*>(this)->on_make_waiting(task);
        break;

      case TaskAction::stop_waiting:
        static_cast<Policy*>(this)->on_stop_waiting(task);
        break;

      case TaskAction::ac_return:
        return;
        // break;

      case TaskAction::terminate:
        static_cast<Policy*>(this)->on_terminate(task);
        break;

      case TaskAction::last:
        [[fallthrough]];
      default:
        throw(std::logic_error("Bad action"));
    }
  }

 public:
  void enable_debug() {
    debug_ = true;
  }

  void disable_debug() {
    debug_ = false;
  }

  bool debug_enabled() {
    return debug_;
  }

  void task_create(task_handle_type& task) {
    event(TaskEvent::create, task, "");
  }

  void task_admit(task_handle_type& task) {
    event(TaskEvent::admit, task, "");
  }

  void task_dispatch(task_handle_type& task) {
    event(TaskEvent::dispatch, task, "");
  }

  void task_wait(task_handle_type& task) {
    // auto node = (*(task->node()));

    event(TaskEvent::wait, task, "");
  }

  void task_notify(task_handle_type& task) {
    event(TaskEvent::notify, task, "");
  }

  void task_exit(task_handle_type& task) {
    event(TaskEvent::exit, task, "");
  }

  void task_yield(task_handle_type& task) {
    event(TaskEvent::yield, task, "");
  }
};

template <typename T>
struct SchedulerTraits<EmptySchedulerPolicy<T>> {
  using task_type = T;
  using task_handle_type = T;
};

/**
 * Rump scheduler policy.  Useful for testing.
 */
template <class Task>
class EmptySchedulerPolicy
    : public SchedulerStateMachine<EmptySchedulerPolicy<Task>> {
 public:
  using task_type =
      typename SchedulerTraits<EmptySchedulerPolicy<Task>>::task_type;
  using task_handle_type =
      typename SchedulerTraits<EmptySchedulerPolicy<Task>>::task_handle_type;

  EmptySchedulerPolicy(EmptySchedulerPolicy&&) noexcept = default;
  EmptySchedulerPolicy(EmptySchedulerPolicy&) {
    std::cout << "Nonsense copy constructor" << std::endl;
  }

  void on_create(Task&) {
  }
  void on_stop_create(Task&) {
  }
  void on_make_runnable(Task&) {
  }
  void on_stop_runnable(Task&) {
  }
  void on_make_running(Task&) {
  }
  void on_stop_running(Task&) {
  }
  void on_make_waiting(Task&) {
  }
  void on_stop_waiting(Task&) {
  }
  void on_terminate(Task&) {
  }
};

template <class Task>
class DebugSchedulerPolicy;

template <typename T>
struct SchedulerTraits<DebugSchedulerPolicy<T>> {
  using task_handle_type = typename DebugSchedulerPolicy<T>::task_handle_type;
};

/**
 * Rump scheduler policy. Useful for testing.
 */
template <class Task>
class DebugSchedulerPolicy
    : public SchedulerStateMachine<DebugSchedulerPolicy<Task>> {
 public:
  using task_handle_type = Task;

  void on_create(Task&) {
    std::cout << "calling on_create" << std::endl;
  }
  void on_stop_create(Task&) {
    std::cout << "calling on_stop_create" << std::endl;
  }
  void on_make_runnable(Task&) {
    std::cout << "calling on_make_runnable" << std::endl;
  }
  void on_stop_runnable(Task&) {
    std::cout << "calling on_stop_runnable" << std::endl;
  }
  void on_make_running(Task&) {
    std::cout << "calling on_make_running" << std::endl;
  }
  void on_stop_running(Task&) {
    std::cout << "calling on_stop_running" << std::endl;
  }
  void on_make_waiting(Task&) {
    std::cout << "calling on_make_waiting" << std::endl;
  }
  void on_stop_waiting(Task&) {
    std::cout << "calling on_stop_waiting" << std::endl;
  }
  void on_terminate(Task&) {
    std::cout << "calling on_terminate" << std::endl;
  }
};

}  // namespace tiledb::common
#endif  // TILEDB_DAG_SCHEDULER_H
