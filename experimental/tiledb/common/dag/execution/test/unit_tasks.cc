/**
 * @file   unit_throw_catch_tasks.cc
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
 * This file tests the throw-catch tasks for dag.
 */

#include "experimental/tiledb/common/dag/nodes/segmented_nodes.h"

#include <atomic>
#include <iostream>
#include <map>
#include <stop_token>
#include <type_traits>
#include "unit_tasks.h"

#include "../throw_catch.h"
#include "experimental/tiledb/common/dag/edge/edge.h"
#include "experimental/tiledb/common/dag/execution/task.h"
#include "experimental/tiledb/common/dag/execution/task_state_machine.h"
#include "experimental/tiledb/common/dag/nodes/terminals.h"
#include "experimental/tiledb/common/dag/ports/ports.h"
#include "experimental/tiledb/common/dag/state_machine/test/helpers.h"

using namespace tiledb::common;

/*
 * Define some helpers
 */
template <template <class> class T, class N>
struct hm {
  T<N> operator()(const N& n) {
    return T<N>{n};
  }
};

template <template <class> class T>
struct hm<T, node> {
  auto operator()(const node& n) const {
    return Task<node>{n};
  }
};

template <class N>
Task<node> task_from_node(N& n) {
  return Task<node>{n};
}

template <class N>
TaskState task_state(const Task<N>& t) {
  return t.task_state();
}

template <class N>
TaskState& task_state(Task<N>& t) {
  return t.task_state();
}

template <class T>
T& task_handle(T& task) {
  return task;
}

auto hm_ = hm<Task, node>{};

TEMPLATE_TEST_CASE(
    "Tasks: Extensive tests of tasks with nodes",
    "[tasks]",
    (std::tuple<
        consumer_node<ThrowCatchMover2, size_t>,
        function_node<ThrowCatchMover2, size_t>,
        producer_node<ThrowCatchMover2, size_t>,
        ThrowCatchScheduler<node>>),
    (std::tuple<
        consumer_node<ThrowCatchMover3, size_t>,
        function_node<ThrowCatchMover3, size_t>,
        producer_node<ThrowCatchMover3, size_t>,
        ThrowCatchScheduler<node>>)) {
  using C = typename std::tuple_element<0, TestType>::type;
  using F = typename std::tuple_element<1, TestType>::type;
  using P = typename std::tuple_element<2, TestType>::type;

  using CI = typename C::Base::element_type;
  using FI = typename F::Base::element_type;
  using PI = typename P::Base::element_type;

  auto pro_node_impl = PI([](std::stop_source&) { return 0; });
  auto fun_node_impl = FI([](const size_t& i) { return i; });
  auto con_node_impl = CI([](const size_t&) {});

  auto pro_node = P([](std::stop_source&) { return 0; });
  auto fun_node = F([](const size_t& i) { return i; });
  auto con_node = C([](const size_t&) {});

  SECTION("I think this works (godbolt)", "[tasks]") {
    auto task_pro = Task<node>{pro_node};
    auto task_fun = Task<node>{fun_node};
    auto task_con = Task<node>{con_node};

    auto task_from_pro = task_from_node(pro_node);
    auto task_from_fun = task_from_node(fun_node);
    auto task_from_con = task_from_node(con_node);

    auto task_hm_pro = hm_(pro_node);
    auto task_hm_fun = hm_(fun_node);
    auto task_hm_con = hm_(con_node);

    /**
     * @todo Unify producer and consumer cases via TEMPLATE_TEST_CASE
     */

    /*
     * Producer case
     */

    /*
     * Tasks constructed from same nodes are unique
     * Though maybe we should not allow this?
     */

    auto task_pro_1 = Task<node>{pro_node};
    auto task_pro_2 = Task<node>{pro_node};
    auto task_pro_3 = task_from_node(pro_node);
    auto task_pro_4 = hm_(pro_node);
    auto task_pro_5 = task_pro_1;
    auto task_pro_6 = task_pro_3;

    CHECK(task_pro_1 != task_pro_2);
    CHECK(task_pro_2 != task_pro_3);
    CHECK(task_pro_3 != task_pro_4);
    CHECK(task_pro_4 != task_pro_5);
    CHECK(task_pro_5 != task_pro_6);

    Task<node> task_pro_7{task_pro_2};
    Task task_pro_8{task_pro_2};

    CHECK(task_pro_6 != task_pro_7);
    CHECK(task_pro_7 == task_pro_2);
    CHECK(task_pro_7 == task_pro_8);
    CHECK(task_pro_8 != task_pro_1);

    auto task_pro_1_x = task_pro_1;
    CHECK(task_pro_1 == task_pro_1);
    CHECK(task_pro_1_x == task_pro_1);
    CHECK(task_pro_1 == task_pro_1_x);

    auto task_pro_5_x = task_pro_5;
    CHECK(task_pro_5_x == task_pro_5);

    // Warning danger -- don't use task_pro_5 after the move
    Task<node> task_pro_5_moved{std::move(task_pro_5)};
    CHECK(task_pro_5_moved == task_pro_5_x);

    /*
     * Function Case
     */

    auto task_fun_1 = Task<node>{fun_node};
    auto task_fun_2 = Task<node>{fun_node};
    auto task_fun_3 = task_from_node(fun_node);
    auto task_fun_4 = hm_(fun_node);
    auto task_fun_5 = task_fun_1;
    auto task_fun_6 = task_fun_3;

    CHECK(task_fun_1 != task_fun_2);
    CHECK(task_fun_2 != task_fun_3);
    CHECK(task_fun_3 != task_fun_4);
    CHECK(task_fun_4 != task_fun_5);
    CHECK(task_fun_5 != task_fun_6);

    Task<node> task_fun_7{task_fun_2};
    Task task_fun_8{task_fun_2};

    CHECK(task_fun_6 != task_fun_7);
    CHECK(task_fun_7 == task_fun_2);
    CHECK(task_fun_7 == task_fun_8);
    CHECK(task_fun_8 != task_fun_1);

    auto task_fun_1_x = task_fun_1;
    CHECK(task_fun_1 == task_fun_1);
    CHECK(task_fun_1_x == task_fun_1);
    CHECK(task_fun_1 == task_fun_1_x);

    auto task_fun_5_x = task_fun_5;
    CHECK(task_fun_5_x == task_fun_5);

    // Warning danger -- don't use task_fun_5 after the move
    Task<node> task_fun_5_moved{std::move(task_fun_5)};
    CHECK(task_fun_5_moved == task_fun_5_x);

    /*
     * Consumer case
     */
    auto task_con_1 = Task<node>{con_node};
    auto task_con_2 = Task<node>{con_node};
    auto task_con_3 = task_from_node(con_node);
    auto task_con_4 = hm_(con_node);
    auto task_con_5 = task_con_1;
    auto task_con_6 = task_con_3;

    CHECK(task_con_1 != task_con_2);
    CHECK(task_con_2 != task_con_3);
    CHECK(task_con_3 != task_con_4);
    CHECK(task_con_4 != task_con_5);
    CHECK(task_con_5 != task_con_6);

    Task<node> task_con_7{task_con_2};
    Task task_con_8{task_con_2};

    CHECK(task_con_6 != task_con_7);
    CHECK(task_con_7 == task_con_2);
    CHECK(task_con_7 == task_con_8);
    CHECK(task_con_8 != task_con_1);

    auto task_con_1_x = task_con_1;
    CHECK(task_con_1 == task_con_1);
    CHECK(task_con_1_x == task_con_1);
    CHECK(task_con_1 == task_con_1_x);

    auto task_con_5_x = task_con_5;
    CHECK(task_con_5_x == task_con_5);

    // Warning danger -- don't use task_con_5 after the move
    Task<node> task_con_5_moved{std::move(task_con_5)};
    CHECK(task_con_5_moved == task_con_5_x);
  }

  SECTION("Check states") {
    auto task_pro = Task<node>{pro_node};
    auto task_fun = Task<node>{fun_node};
    auto task_con = Task<node>{con_node};

    auto task_from_pro = task_from_node(pro_node);
    auto task_from_fun = task_from_node(fun_node);
    auto task_from_con = task_from_node(con_node);

    auto task_hm_pro = hm_(pro_node);
    auto task_hm_fun = hm_(fun_node);
    auto task_hm_con = hm_(con_node);

    CHECK(str(task_state(task_pro)) == "created");
    CHECK(str(task_state(task_from_pro)) == "created");
    CHECK(str(task_state(task_hm_pro)) == "created");

    CHECK(str(task_state(task_fun)) == "created");
    CHECK(str(task_state(task_from_fun)) == "created");
    CHECK(str(task_state(task_hm_fun)) == "created");

    CHECK(str(task_state(task_con)) == "created");
    CHECK(str(task_state(task_from_con)) == "created");
    CHECK(str(task_state(task_hm_con)) == "created");

    /*
     * No aliasing of tasks
     */
    task_state(task_pro) = TaskState::running;
    CHECK(str(task_state(task_pro)) == "running");

    CHECK(str(task_state(task_from_pro)) == "created");
    CHECK(str(task_state(task_hm_pro)) == "created");

    CHECK(str(task_state(task_fun)) == "created");
    CHECK(str(task_state(task_from_fun)) == "created");
    CHECK(str(task_state(task_hm_fun)) == "created");

    CHECK(str(task_state(task_con)) == "created");
    CHECK(str(task_state(task_from_con)) == "created");
    CHECK(str(task_state(task_hm_con)) == "created");

    task_state(task_pro) = TaskState::created;
    CHECK(str(task_state(task_pro)) == "created");

    CHECK(str(task_state(task_from_pro)) == "created");
    CHECK(str(task_state(task_hm_pro)) == "created");

    CHECK(str(task_state(task_fun)) == "created");
    CHECK(str(task_state(task_from_fun)) == "created");
    CHECK(str(task_state(task_hm_fun)) == "created");

    CHECK(str(task_state(task_con)) == "created");
    CHECK(str(task_state(task_from_con)) == "created");
    CHECK(str(task_state(task_hm_con)) == "created");

    task_state(task_con) = TaskState::running;
    CHECK(str(task_state(task_con)) == "running");

    CHECK(str(task_state(task_pro)) == "created");
    CHECK(str(task_state(task_from_pro)) == "created");
    CHECK(str(task_state(task_hm_pro)) == "created");

    CHECK(str(task_state(task_fun)) == "created");
    CHECK(str(task_state(task_from_fun)) == "created");
    CHECK(str(task_state(task_hm_fun)) == "created");

    CHECK(str(task_state(task_from_con)) == "created");
    CHECK(str(task_state(task_hm_con)) == "created");
  }
}

/*
 * Some deduction guides
 */
namespace tiledb::common {
Task(node&)->Task<node>;

Task(const node&)->Task<node>;

template <template <class> class M, class T>
Task(producer_node<M, T>) -> Task<node>;

template <template <class> class M, class T>
Task(consumer_node<M, T>) -> Task<node>;

template <
    template <class>
    class M1,
    class T1,
    template <class>
    class M2,
    class T2>
Task(function_node<M1, T1, M2, T2>) -> Task<node>;

template <template <class> class M1, class T1>
Task(function_node<M1, T1>) -> Task<node>;

}  // namespace tiledb::common

TEMPLATE_TEST_CASE(
    "Tasks: Test Task",
    "[tasks]",
    (std::tuple<
        consumer_node<ThrowCatchMover2, size_t>,
        function_node<ThrowCatchMover2, size_t>,
        producer_node<ThrowCatchMover2, size_t>,
        ThrowCatchScheduler<node>>),
    (std::tuple<
        consumer_node<ThrowCatchMover3, size_t>,
        function_node<ThrowCatchMover3, size_t>,
        producer_node<ThrowCatchMover3, size_t>,
        ThrowCatchScheduler<node>>)) {
  using C = typename std::tuple_element<0, TestType>::type;
  using F = typename std::tuple_element<1, TestType>::type;
  using P = typename std::tuple_element<2, TestType>::type;

  using CI = typename C::Base::element_type;
  using FI = typename F::Base::element_type;
  using PI = typename P::Base::element_type;

  auto pro_node_impl = PI([](std::stop_source&) { return 0; });
  auto fun_node_impl = FI([](const size_t& i) { return i; });
  auto con_node_impl = CI([](const size_t&) {});

  auto pro_node = P([](std::stop_source&) { return 0; });
  auto fun_node = F([](const size_t& i) { return i; });
  auto con_node = C([](const size_t&) {});

  auto pro_node_2 = P([](std::stop_source&) { return 0; });
  auto fun_node_2 = F([](const size_t&) { return 0; });
  auto con_node_2 = C([](const size_t&) {});

  auto pro_task = Task(pro_node);
  auto fun_task = Task(fun_node);
  auto con_task = Task(con_node);

  auto pro_task_assign = pro_task;
  auto fun_task_assign = fun_task;
  auto con_task_assign = con_task;

  auto pro_task_copy = Task(pro_task);
  auto fun_task_copy = Task(fun_task);
  auto con_task_copy = Task(con_task);

  auto pro_task_2 = Task(pro_node_2);
  auto con_task_2 = Task(con_node_2);

  SECTION("Names") {
    CHECK(con_node->name() == "consumer");
    CHECK(pro_node->name() == "producer");
    CHECK(con_task->name() == "consumer task");
    CHECK(pro_task->name() == "producer task");
    CHECK(con_task_2->name() == "consumer task");
    CHECK(pro_task_2->name() == "producer task");
  }

  SECTION("Node Equality") {
    // This is brilliant
    node node_pro = pro_node;
    node node_fun = fun_node;
    node node_con = con_node;

    CHECK(node_pro == pro_node);
    CHECK(node_fun == fun_node);
    CHECK(node_con == con_node);
    CHECK(node_pro != node_con);
    CHECK(node_pro != node_fun);
    CHECK(node_fun != node_con);

    CHECK(static_cast<void*>(&(*node_pro)) == static_cast<void*>(&(*node_pro)));
    CHECK(static_cast<void*>(&(*node_fun)) == static_cast<void*>(&(*node_fun)));
    CHECK(static_cast<void*>(&(*node_pro)) == static_cast<void*>(&(*pro_node)));
  }

  SECTION("Task Equality") {
  }

  SECTION("Node and Task Equality") {
  }

  SECTION("Queue") {
    auto pro_node_i = P([](std::stop_source&) { return 0; });
    auto pro_node_j = P([](std::stop_source&) { return 0; });
    auto pro_node_k = PI([](std::stop_source&) { return 0; });

    auto fun_node_i = F([](const size_t&) { return 0; });
    auto fun_node_j = F([](const size_t&) { return 0; });
    auto fun_node_k = FI([](const size_t&) { return 0; });

    auto con_node_i = C([](const size_t&) {});
    auto con_node_j = C([](const size_t&) {});
    auto con_node_k = CI([](const size_t&) {});

    auto pro_task_i = Task<node>{pro_node_i};
    auto pro_task_j = Task<node>{pro_node_j};
    auto pro_task_i_deduced = Task{pro_node_i};
    auto pro_task_j_deduced = Task{pro_node_j};
    auto pro_task_i_tfn = task_from_node(pro_node_i);
    auto pro_task_j_tfn = task_from_node(pro_node_j);

    auto fun_task_i = Task<node>{fun_node_i};
    auto fun_task_j = Task<node>{fun_node_j};
    auto fun_task_i_deduced = Task{fun_node_i};
    auto fun_task_j_deduced = Task{fun_node_j};
    auto fun_task_i_tfn = task_from_node(fun_node_i);
    auto fun_task_j_tfn = task_from_node(fun_node_j);

    auto con_task_i = Task<node>{con_node_i};
    auto con_task_j = Task<node>{con_node_j};
    auto con_task_i_deduced = Task{con_node_i};
    auto con_task_j_deduced = Task{con_node_j};
    auto con_task_i_tfn = task_from_node(con_node_i);
    auto con_task_j_tfn = task_from_node(con_node_j);

    CHECK(pro_task_i != pro_task_i_deduced);
    CHECK(fun_task_i != fun_task_i_deduced);
    CHECK(pro_task_j != pro_task_j_deduced);

    std::queue<node> node_queue;
    node_queue.push(pro_node);
    node_queue.push(fun_node);
    node_queue.push(con_node);

    std::queue<Task<node>> task_queue;

    task_queue.push(pro_task_i);
    task_queue.push(fun_task_i);
    task_queue.push(con_task_i);
    task_queue.push(pro_task_j);
    task_queue.push(fun_task_j);
    task_queue.push(con_task_j);

    task_queue.push(pro_task_i_tfn);
    task_queue.push(fun_task_i_tfn);
    task_queue.push(con_task_i_tfn);

    task_queue.push(pro_task_i_deduced);
    task_queue.push(con_task_i_deduced);
    task_queue.push(fun_task_i_deduced);
    task_queue.push(pro_task_j_deduced);
    task_queue.push(con_task_j_deduced);
    task_queue.push(fun_task_j_deduced);

    CHECK(task_queue.front() == pro_task_i);
    task_queue.pop();
    CHECK(task_queue.front() == fun_task_i);
    task_queue.pop();
    CHECK(task_queue.front() == con_task_i);
    task_queue.pop();
    CHECK(task_queue.front() == pro_task_j);
    task_queue.pop();
    CHECK(task_queue.front() == fun_task_j);
    task_queue.pop();
    CHECK(task_queue.front() == con_task_j);
    task_queue.pop();

    CHECK(task_queue.front() == pro_task_i_tfn);
    task_queue.pop();
    CHECK(task_queue.front() == fun_task_i_tfn);
    task_queue.pop();
    CHECK(task_queue.front() == con_task_i_tfn);
    task_queue.pop();

    CHECK(task_queue.front() == pro_task_i_deduced);
    task_queue.pop();
    CHECK(task_queue.front() == con_task_i_deduced);
    task_queue.pop();
    CHECK(task_queue.front() == fun_task_i_deduced);
    task_queue.pop();
    CHECK(task_queue.front() == pro_task_j_deduced);
    task_queue.pop();
    CHECK(task_queue.front() == con_task_j_deduced);
    task_queue.pop();
    CHECK(task_queue.front() == fun_task_j_deduced);
    task_queue.pop();
    CHECK(task_queue.empty());

    auto pro_task_copy = pro_task;
    CHECK(pro_task == pro_task);
    CHECK(pro_task_copy == pro_task_copy);
    CHECK(pro_task_copy == pro_task);
    CHECK(pro_task == pro_task_copy);

    auto empty_queue = std::queue<Task<node>>{};
    task_queue.swap(empty_queue);
    CHECK(task_queue.empty());

    // Check that we get same task back when we push and pop
    task_queue.push(pro_task_copy);
    CHECK(!task_queue.empty());

    auto pro_task_front = task_queue.front();

    CHECK(pro_task == pro_task_copy);
    CHECK(pro_task == pro_task_front);
    task_queue.pop();
    CHECK(pro_task == pro_task_copy);
    CHECK(pro_task == pro_task_front);

    CHECK(str(task_state(pro_task)) == "created");
    CHECK(str(task_state(pro_task_copy)) == "created");
    CHECK(str(task_state(pro_task_front)) == "created");

    /*
     * Check that copies are shallow
     */
    task_state(pro_task_copy) = TaskState::running;
    CHECK(str(task_state(pro_task_copy)) == "running");
    CHECK(str(task_state(pro_task_copy)) == "running");
    CHECK(str(task_state(pro_task_front)) == "running");

    task_queue.push(pro_task_copy);
    auto pro_task_front_running = task_queue.front();
    CHECK(str(task_state(pro_task_front_running)) == "running");

    task_state(pro_task_copy) = TaskState::runnable;
    task_queue.push(pro_task_copy);
    CHECK(task_queue.front() == pro_task_copy);
    CHECK(task_state(task_queue.front()) == TaskState::runnable);
    CHECK(str(task_state(task_queue.front())) == "runnable");

    task_queue.pop();
    task_queue.pop();
    CHECK(task_queue.empty());
  }
}

SCENARIO(
    "Tasks can be pushed and popped into a queue without invalidating "
    "references to them") {
  auto pro_node = producer_node<ThrowCatchMover3, size_t>(
      [](std::stop_source&) { return 0; });
  auto con_node = consumer_node<ThrowCatchMover3, size_t>([](const size_t&) {});

  auto pro_task = Task(pro_node);
  auto con_task = Task(con_node);

  GIVEN("Tasks pro_task and pro_task_copy (copy of pro_task)") {
    auto pro_task_assign = pro_task;
    auto con_task_assign = con_task;

    auto pro_task_copy = Task(pro_task);
    auto con_task_copy = Task(con_task);
    THEN("pro_task == pro_task_copy") {
      CHECK(pro_task_assign == pro_task);
      CHECK(con_task_assign == con_task);

      CHECK(pro_task_copy == pro_task);
      CHECK(con_task_copy == con_task);

      CHECK(pro_task != con_task);
    }
    WHEN("Task with copy is pushed onto a queue") {
      std::queue<Task<node>> task_queue;
      auto pro_task_to_push = Task(pro_task);
      CHECK(pro_task_to_push == pro_task);
      task_queue.push(pro_task_to_push);

      THEN("The front of the queue is still equal to original task") {
        CHECK(task_queue.front() == pro_task);
        AND_THEN("A task copied from the front is equal to original task") {
          auto front_pro_task = task_queue.front();
          CHECK(task_queue.front() == pro_task);
          CHECK(front_pro_task == pro_task);
        }
      }
      AND_WHEN("The task is popped") {
        auto popped_pro_task = task_queue.front();
        task_queue.pop();
        THEN("The popped task is still equal to the original task") {
          CHECK(popped_pro_task == pro_task);
        }
      }
      AND_WHEN("We push tasks onto the queue") {
        std::queue<Task<node>> created_queue;
        std::queue<Task<node>> submitted_queue;

        auto created_pro_task_i = Task(pro_node);
        auto created_pro_task_j = Task(pro_node);
        auto created_pro_task_k = Task(pro_node);

        auto copied_pro_task_i = created_pro_task_i;
        auto copied_pro_task_j = created_pro_task_j;
        auto copied_pro_task_k = created_pro_task_k;

        created_queue.push(created_pro_task_i);
        created_queue.push(created_pro_task_j);
        created_queue.push(created_pro_task_k);
        AND_WHEN("Task state is changed") {
          auto popped_pro_task_i = created_queue.front();
          created_queue.pop();
          CHECK(task_state(popped_pro_task_i) == TaskState::created);
          auto popped_pro_task_j = created_queue.front();
          created_queue.pop();
          CHECK(task_state(popped_pro_task_j) == TaskState::created);
          auto popped_pro_task_k = created_queue.front();
          created_queue.pop();
          CHECK(task_state(popped_pro_task_k) == TaskState::created);

          task_state(popped_pro_task_i) = TaskState::runnable;
          submitted_queue.push(popped_pro_task_i);
          task_state(popped_pro_task_j) = TaskState::running;
          submitted_queue.push(popped_pro_task_j);
          task_state(popped_pro_task_k) = TaskState::terminated;
          submitted_queue.push(popped_pro_task_k);

          THEN("The property of the original changes also") {
            CHECK(task_state(copied_pro_task_i) == TaskState::runnable);
            CHECK(task_state(copied_pro_task_j) == TaskState::running);
            CHECK(task_state(copied_pro_task_k) == TaskState::terminated);

            CHECK(str(task_state(copied_pro_task_i)) == "runnable");
            CHECK(str(task_state(copied_pro_task_j)) == "running");
            CHECK(str(task_state(copied_pro_task_k)) == "terminated");
          }
        }
      }
    }
  }
}

SCENARIO(
    "Tasks can be inserted into and extracted from a set without invalidating "
    "references to them") {
  auto pro_node = producer_node<ThrowCatchMover3, size_t>(
      [](std::stop_source&) { return 0; });
  auto con_node = consumer_node<ThrowCatchMover3, size_t>([](const size_t&) {});

  auto pro_task = Task(pro_node);
  auto con_task = Task(con_node);

  GIVEN("Tasks pro_task and pro_task_copy (copy of pro_task)") {
    auto pro_task_assign = pro_task;
    auto con_task_assign = con_task;

    auto pro_task_copy = Task(pro_task);
    auto con_task_copy = Task(con_task);

    THEN("pro_task == pro_task_copy") {
      CHECK(pro_task_assign == pro_task);
      CHECK(con_task_assign == con_task);

      CHECK(pro_task_copy == pro_task);
      CHECK(con_task_copy == con_task);

      CHECK(pro_task != con_task);
    }
    WHEN("Task with copy is inserted into a set") {
      std::set<Task<node>> task_set;

      auto pro_task_to_insert = Task(pro_task);
      CHECK(pro_task_to_insert == pro_task);
      task_set.insert(pro_task_to_insert);

      THEN("The inserted task can be found using original") {
        CHECK(task_set.find(pro_task_to_insert) != end(task_set));
        CHECK(task_set.find(pro_task) != end(task_set));

        AND_THEN("A task extracted from the set is equal to original task") {
          auto extracted_pro_task_handle = task_set.extract(pro_task_to_insert);
          CHECK(!extracted_pro_task_handle.empty());
          CHECK(extracted_pro_task_handle.value() == pro_task);
        }
      }

      AND_WHEN("We insert multiple tasks into a set") {
        std::set<Task<node>> created_set;
        std::set<Task<node>> submitted_set;

        auto created_pro_task_i = Task(pro_node);
        auto created_pro_task_j = Task(pro_node);
        auto created_pro_task_k = Task(pro_node);

        auto copied_pro_task_i = created_pro_task_i;
        auto copied_pro_task_j = created_pro_task_j;
        auto copied_pro_task_k = created_pro_task_k;

        created_set.insert(created_pro_task_i);
        created_set.insert(created_pro_task_j);
        created_set.insert(created_pro_task_k);

        AND_WHEN("Task state is changed") {
          auto extracted_pro_task_i =
              created_set.extract(created_pro_task_i).value();
          CHECK(task_state(extracted_pro_task_i) == TaskState::created);
          auto extracted_pro_task_j =
              created_set.extract(created_pro_task_j).value();
          CHECK(task_state(extracted_pro_task_j) == TaskState::created);
          auto extracted_pro_task_k =
              created_set.extract(created_pro_task_k).value();
          CHECK(task_state(extracted_pro_task_k) == TaskState::created);

          task_state(extracted_pro_task_i) = TaskState::runnable;
          task_state(extracted_pro_task_j) = TaskState::running;
          task_state(extracted_pro_task_k) = TaskState::terminated;

          submitted_set.insert(copied_pro_task_i);
          submitted_set.insert(created_pro_task_j);
          submitted_set.insert(extracted_pro_task_k);

          THEN("The property of the original changes also") {
            CHECK(task_state(copied_pro_task_i) == TaskState::runnable);
            CHECK(task_state(copied_pro_task_j) == TaskState::running);
            CHECK(task_state(copied_pro_task_k) == TaskState::terminated);

            CHECK(str(task_state(copied_pro_task_i)) == "runnable");
            CHECK(str(task_state(copied_pro_task_j)) == "running");
            CHECK(str(task_state(copied_pro_task_k)) == "terminated");

            CHECK(str(task_state(created_pro_task_i)) == "runnable");
            CHECK(str(task_state(created_pro_task_j)) == "running");
            CHECK(str(task_state(created_pro_task_k)) == "terminated");

            CHECK(
                submitted_set.extract(created_pro_task_i).value() ==
                created_pro_task_i);
            CHECK(
                submitted_set.extract(copied_pro_task_j).value() ==
                created_pro_task_j);
            CHECK(
                submitted_set.extract(extracted_pro_task_k).value() ==
                created_pro_task_k);

            CHECK(str(task_state(created_pro_task_i)) == "runnable");
            CHECK(str(task_state(created_pro_task_j)) == "running");
            CHECK(str(task_state(created_pro_task_k)) == "terminated");
          }
        }
      }
    }
  }
}

/**
 * @todo Completely repeat scenarios for std::queue and std::map
 */
SCENARIO(
    "Tasks can be inserted into and looked up from a map, using nodes as "
    "keys") {
  auto pro_node = producer_node<ThrowCatchMover3, size_t>(
      [](std::stop_source&) { return 0; });
  auto con_node = consumer_node<ThrowCatchMover3, size_t>([](const size_t&) {});

  auto pro_task = Task(pro_node);
  auto con_task = Task(con_node);

  GIVEN("A node to task map") {
    std::map<node, Task<node>> node_to_task_map;

    WHEN("Insert node-task pair into map") {
      auto pro_task_copy = Task(pro_task);

      node_to_task_map[pro_node] = pro_task;

      THEN("Retrieved task is equal to inserted task") {
        CHECK(node_to_task_map[pro_node] == pro_task_copy);
      }
      THEN("Changing retrieved task state changes inserted task state") {
        auto retrieved_pro_task = node_to_task_map[pro_node];
        CHECK(retrieved_pro_task == pro_task_copy);
        CHECK(retrieved_pro_task == pro_task);
        CHECK(task_state(retrieved_pro_task) == TaskState::created);
        task_state(retrieved_pro_task) = TaskState::running;
        CHECK(task_state(retrieved_pro_task) == TaskState::running);
        CHECK(task_state(pro_task) == TaskState::running);
      }
    }
  }
}
