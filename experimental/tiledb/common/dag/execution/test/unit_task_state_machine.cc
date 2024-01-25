/**
 * @file unit_scheduler.cc
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

#include <memory>

#include "../task_state_machine.h"
#include "unit_task_state_machine.h"

using namespace tiledb::common;

struct _unit_test_task {
  TaskState state_{TaskState::created};
  TaskState& task_state() {
    return state_;
  }
  TaskState& set_state(TaskState st) {
    return state_ = st;
  }
};

// using unit_test_task = std::shared_ptr<_unit_test_task>;

struct unit_test_task : public std::shared_ptr<_unit_test_task> {
  using Base = std::shared_ptr<_unit_test_task>;
  unit_test_task()
      : Base{std::make_shared<_unit_test_task>()} {
  }
};

TEST_CASE(
    "Scheduler FSM: Construct SchedulerStateMachine<EmptySchedulerPolicy>",
    "[scheduler]") {
  [[maybe_unused]] auto sched = SchedulerStateMachine<
      //      unit_test_task,
      EmptySchedulerPolicy<unit_test_task>>{};
  unit_test_task a;
  CHECK(str(a->task_state()) == "created");
}

TEST_CASE(
    "Scheduler FSM: Test transitions with "
    "SchedulerStateMachine<EmptySchedulerPolicy>",
    "[scheduler]") {
  [[maybe_unused]] auto sched = SchedulerStateMachine<
      //      unit_test_task,
      EmptySchedulerPolicy<unit_test_task>>{};
  unit_test_task a;
  CHECK(str(a->task_state()) == "created");

  SECTION("Admit") {
    sched.task_admit(a);
    CHECK(str(a->task_state()) == "runnable");
  }

  SECTION("Admit + dispatch") {
    sched.task_admit(a);
    CHECK(str(a->task_state()) == "runnable");
    sched.task_dispatch(a);
    CHECK(str(a->task_state()) == "running");
  }

  SECTION("Admit + dispatch + yield") {
    sched.task_admit(a);
    CHECK(str(a->task_state()) == "runnable");
    sched.task_dispatch(a);
    CHECK(str(a->task_state()) == "running");
    sched.task_yield(a);
    CHECK(str(a->task_state()) == "runnable");
  }

  SECTION("Admit + dispatch + yield + dispatch + wait") {
    sched.task_admit(a);
    CHECK(str(a->task_state()) == "runnable");
    sched.task_dispatch(a);
    CHECK(str(a->task_state()) == "running");
    sched.task_yield(a);
    CHECK(str(a->task_state()) == "runnable");
    sched.task_dispatch(a);
    CHECK(str(a->task_state()) == "running");
    sched.task_wait(a);
    CHECK(str(a->task_state()) == "waiting");
  }

  SECTION("Admit + dispatch + yield + dispatch + wait + notify") {
    sched.task_admit(a);
    CHECK(str(a->task_state()) == "runnable");
    sched.task_dispatch(a);
    CHECK(str(a->task_state()) == "running");
    sched.task_yield(a);
    CHECK(str(a->task_state()) == "runnable");
    sched.task_dispatch(a);
    CHECK(str(a->task_state()) == "running");
    sched.task_wait(a);
    CHECK(str(a->task_state()) == "waiting");
    sched.task_notify(a);
    CHECK(str(a->task_state()) == "runnable");
  }

  SECTION(
      "Admit + dispatch + yield + dispatch + wait + notify + schedule + exit") {
    sched.task_admit(a);
    CHECK(str(a->task_state()) == "runnable");
    sched.task_dispatch(a);
    CHECK(str(a->task_state()) == "running");
    sched.task_yield(a);
    CHECK(str(a->task_state()) == "runnable");
    sched.task_dispatch(a);
    CHECK(str(a->task_state()) == "running");
    sched.task_wait(a);
    CHECK(str(a->task_state()) == "waiting");
    sched.task_notify(a);
    CHECK(str(a->task_state()) == "runnable");
    sched.task_dispatch(a);
    CHECK(str(a->task_state()) == "running");
    sched.task_exit(a);
    CHECK(str(a->task_state()) == "terminated");
  }
}

std::atomic<bool> created;
std::atomic<bool> runnable;
std::atomic<bool> running;
std::atomic<bool> waiting;
std::atomic<bool> terminated;

template <class Task>
class UnitTestSchedulerPolicy;

template <typename T>
struct SchedulerTraits<UnitTestSchedulerPolicy<T>> {
  using task_type = T;
  using task_handle_type = T;
};

template <class Task>
class UnitTestSchedulerPolicy
    : public SchedulerStateMachine<
          //                                    unit_test_task,
          UnitTestSchedulerPolicy<Task>> {
 public:
  using task_type =
      typename SchedulerTraits<EmptySchedulerPolicy<Task>>::task_type;
  using task_handle_type =
      typename SchedulerTraits<EmptySchedulerPolicy<Task>>::task_handle_type;

  UnitTestSchedulerPolicy() {
    created.store(true);
    runnable.store(false);
    running.store(false);
    waiting.store(false);
    terminated.store(false);
  }

  void on_create(Task&) {
    created.store(true);
  }

  void on_stop_create(Task&) {
    created.store(false);
  }

  void on_make_runnable(Task&) {
    runnable.store(true);
  }

  void on_stop_runnable(Task&) {
    runnable.store(false);
  }

  void on_make_running(Task&) {
    running.store(true);
  }

  void on_stop_running(Task&) {
    running.store(false);
  }

  void on_make_waiting(Task&) {
    waiting.store(true);
  }

  void on_stop_waiting(Task&) {
    waiting.store(false);
  }

  void on_terminate(Task&) {
    terminated.store(true);
  }
};

TEST_CASE(
    "Scheduler FSM: Test transitions and actions with "
    "SchedulerStateMachine<UnitTestSchedulerPolicy>",
    "[scheduler]") {
  auto sched = UnitTestSchedulerPolicy<unit_test_task>{};
  unit_test_task a;

  CHECK(str(a->task_state()) == "created");

  SECTION("Create") {
    sched.task_create(a);
    CHECK(str(a->task_state()) == "created");
    CHECK(created);
    CHECK(!runnable);
    CHECK(!terminated);
    CHECK(!running);
    CHECK(!waiting);
  }

  SECTION("Admit") {
    sched.task_create(a);
    CHECK(str(a->task_state()) == "created");
    CHECK(created);
    sched.task_admit(a);
    CHECK(str(a->task_state()) == "runnable");
    CHECK(runnable);
    CHECK(runnable);
    CHECK(!terminated);
    CHECK(!running);
    CHECK(!waiting);
    CHECK(!created);
  }

  SECTION("Admit + dispatch") {
    sched.task_create(a);
    CHECK(str(a->task_state()) == "created");
    CHECK(created);
    sched.task_admit(a);
    CHECK(str(a->task_state()) == "runnable");
    CHECK(runnable);
    sched.task_dispatch(a);
    CHECK(str(a->task_state()) == "running");
    CHECK(running);
    CHECK(!runnable);
    CHECK(!terminated);
    CHECK(!waiting);
    CHECK(!created);
  }

  SECTION("Admit + dispatch + yield") {
    sched.task_create(a);
    CHECK(str(a->task_state()) == "created");
    CHECK(created);
    sched.task_admit(a);
    CHECK(str(a->task_state()) == "runnable");
    CHECK(runnable);
    sched.task_dispatch(a);
    CHECK(str(a->task_state()) == "running");
    CHECK(running);
    sched.task_yield(a);
    CHECK(str(a->task_state()) == "runnable");
    CHECK(runnable);
    CHECK(!terminated);
    CHECK(!running);
    CHECK(!waiting);
    CHECK(!created);
  }

  SECTION("Admit + dispatch + yield + dispatch + wait") {
    sched.task_create(a);
    CHECK(str(a->task_state()) == "created");
    CHECK(created);
    sched.task_admit(a);
    CHECK(str(a->task_state()) == "runnable");

    sched.task_dispatch(a);
    CHECK(str(a->task_state()) == "running");
    CHECK(running);
    sched.task_yield(a);
    CHECK(str(a->task_state()) == "runnable");
    CHECK(runnable);
    sched.task_dispatch(a);
    CHECK(str(a->task_state()) == "running");
    CHECK(running);
    sched.task_wait(a);
    CHECK(str(a->task_state()) == "waiting");
    CHECK(waiting);
    CHECK(!runnable);
    CHECK(!terminated);
    CHECK(!running);
    CHECK(waiting);
    CHECK(!created);
  }

  SECTION("Admit + dispatch + yield + dispatch + wait + notify") {
    sched.task_create(a);
    CHECK(str(a->task_state()) == "created");
    CHECK(created);
    sched.task_admit(a);
    CHECK(str(a->task_state()) == "runnable");
    CHECK(runnable);
    sched.task_dispatch(a);
    CHECK(str(a->task_state()) == "running");
    CHECK(running);
    sched.task_yield(a);
    CHECK(str(a->task_state()) == "runnable");
    CHECK(runnable);
    sched.task_dispatch(a);
    CHECK(str(a->task_state()) == "running");
    CHECK(running);
    sched.task_wait(a);
    CHECK(str(a->task_state()) == "waiting");
    CHECK(waiting);
    sched.task_notify(a);
    CHECK(str(a->task_state()) == "runnable");
    CHECK(runnable);
    CHECK(!terminated);
    CHECK(!running);
    CHECK(!waiting);
    CHECK(!created);
  }

  SECTION(
      "Admit + dispatch + yield + dispatch + wait + notify + schedule + exit") {
    sched.task_create(a);
    CHECK(str(a->task_state()) == "created");
    CHECK(created);
    sched.task_admit(a);
    CHECK(str(a->task_state()) == "runnable");
    CHECK(runnable);
    sched.task_dispatch(a);
    CHECK(str(a->task_state()) == "running");
    CHECK(running);
    sched.task_yield(a);
    CHECK(str(a->task_state()) == "runnable");
    CHECK(runnable);
    sched.task_dispatch(a);
    CHECK(str(a->task_state()) == "running");
    CHECK(running);
    sched.task_wait(a);
    CHECK(str(a->task_state()) == "waiting");
    CHECK(waiting);
    sched.task_notify(a);
    CHECK(str(a->task_state()) == "runnable");
    CHECK(runnable);
    sched.task_dispatch(a);
    CHECK(str(a->task_state()) == "running");
    CHECK(running);
    sched.task_exit(a);
    CHECK(str(a->task_state()) == "terminated");
    CHECK(terminated);
    CHECK(!runnable);
    CHECK(!running);
    CHECK(!waiting);
    CHECK(!created);
  }
}
