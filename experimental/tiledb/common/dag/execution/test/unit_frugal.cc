/**
 * @file   unit_frugal.cc
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
 * This file declares a throw-catch scheduler for dag.
 */

#include "experimental/tiledb/common/dag/nodes/segmented_nodes.h"

#include <atomic>
#include <cassert>
#include <iostream>
#include <map>
#include <numeric>
#include <tiledb/stdx/stop_token>
#include <type_traits>

#include "../frugal.h"
#include "experimental/tiledb/common/dag/edge/edge.h"
#include "experimental/tiledb/common/dag/execution/task.h"
#include "experimental/tiledb/common/dag/execution/task_state_machine.h"
#include "unit_frugal.h"

#include "experimental/tiledb/common/dag/nodes/terminals.h"
#include "experimental/tiledb/common/dag/ports/ports.h"
#include "experimental/tiledb/common/dag/state_machine/test/helpers.h"
#include "experimental/tiledb/common/dag/state_machine/test/types.h"

using namespace tiledb::common;

size_t problem_size = 1337UL;
size_t debug_problem_size = 3;

TEMPLATE_TEST_CASE(
    "FrugalScheduler: Test soft terminate of sink",
    "[throw_catch]",
    (std::tuple<
        consumer_node<FrugalMover2, size_t>,
        function_node<FrugalMover2, size_t>,
        producer_node<FrugalMover2, size_t>>),
    (std::tuple<
        consumer_node<FrugalMover3, size_t>,
        function_node<FrugalMover3, size_t>,
        producer_node<FrugalMover3, size_t>>)) {
  bool debug{true};

  auto num_threads = 1;
  [[maybe_unused]] auto sched = FrugalScheduler<node>(num_threads);

  size_t rounds = 5;

  using C = typename std::tuple_element<0, TestType>::type;
  using F = typename std::tuple_element<1, TestType>::type;
  using P = typename std::tuple_element<2, TestType>::type;

  auto p = P([rounds](std::stop_source& stop_source) {
    static size_t i{0};
    if (i > rounds) {
      stop_source.request_stop();
    }
    return i++;
  });
  auto f = F([](const size_t& i) { return i; });
  auto g = F([](const size_t& i) { return i; });
  auto c = C([](const size_t&) {});

  connect(p, f);
  connect(f, g);
  connect(g, c);

  Edge(*p, *f);
  Edge(*f, *g);
  Edge(*g, *c);

  sched.submit(p);
  sched.submit(f);
  sched.submit(g);
  sched.submit(c);

  if (debug) {
    //    sched.debug();

    p->enable_debug();
    f->enable_debug();
    g->enable_debug();
    c->enable_debug();
  }

  sched.sync_wait_all();
}

TEMPLATE_TEST_CASE(
    "FrugalScheduler: Test creating nodes",
    "[throw_catch]",
    (std::tuple<
        consumer_node<FrugalMover2, size_t>,
        function_node<FrugalMover2, size_t>,
        producer_node<FrugalMover2, size_t>>),
    (std::tuple<
        consumer_node<FrugalMover3, size_t>,
        function_node<FrugalMover3, size_t>,
        producer_node<FrugalMover3, size_t>>)) {
  using C = typename std::tuple_element<0, TestType>::type;
  using F = typename std::tuple_element<1, TestType>::type;
  using P = typename std::tuple_element<2, TestType>::type;

  auto p = P([](std::stop_source&) { return 0; });
  auto f = F([](const size_t& i) { return i; });
  auto c = C([](const size_t&) {});
}

TEMPLATE_TEST_CASE(
    "FrugalScheduler: Test assigning nodes",
    "[throw_catch]",
    (std::tuple<
        consumer_node<FrugalMover2, size_t>,
        function_node<FrugalMover2, size_t>,
        producer_node<FrugalMover2, size_t>>),
    (std::tuple<
        consumer_node<FrugalMover3, size_t>,
        function_node<FrugalMover3, size_t>,
        producer_node<FrugalMover3, size_t>>)) {
  using C = typename std::tuple_element<0, TestType>::type;
  using F = typename std::tuple_element<1, TestType>::type;
  using P = typename std::tuple_element<2, TestType>::type;

  auto p = P([](std::stop_source&) { return 0; });
  auto f = F([](const size_t& i) { return i; });
  auto c = C([](const size_t&) {});

  node q = p;
  node d = c;
  node g = f;

  q->source_correspondent() = p;
  CHECK(q->source_correspondent() == p);

  q->sink_correspondent() = p;
  CHECK(q->sink_correspondent() == p);

  q->source_correspondent() = f;
  CHECK(q->source_correspondent() == f);

  q->sink_correspondent() = f;
  CHECK(q->sink_correspondent() == f);

  q->source_correspondent() = c;
  CHECK(q->source_correspondent() == c);

  q->sink_correspondent() = c;
  CHECK(q->sink_correspondent() == c);

  d->source_correspondent() = p;
  CHECK(d->source_correspondent() == p);

  d->sink_correspondent() = p;
  CHECK(d->sink_correspondent() == p);

  d->source_correspondent() = f;
  CHECK(d->source_correspondent() == f);

  d->sink_correspondent() = f;
  CHECK(d->sink_correspondent() == f);

  d->source_correspondent() = c;
  CHECK(d->source_correspondent() == c);

  d->sink_correspondent() = c;
  CHECK(d->sink_correspondent() == c);

  g->source_correspondent() = p;
  CHECK(g->source_correspondent() == p);

  g->sink_correspondent() = p;
  CHECK(g->sink_correspondent() == p);

  g->source_correspondent() = f;
  CHECK(g->source_correspondent() == f);

  g->sink_correspondent() = f;
  CHECK(g->sink_correspondent() == f);

  g->source_correspondent() = c;
  CHECK(g->source_correspondent() == c);

  g->sink_correspondent() = c;
  CHECK(g->sink_correspondent() == c);
}

TEMPLATE_TEST_CASE(
    "FrugalScheduler: Test connect nodes",
    "[throw_catch]",
    (std::tuple<
        consumer_node<FrugalMover2, size_t>,
        function_node<FrugalMover2, size_t>,
        producer_node<FrugalMover2, size_t>>),
    (std::tuple<
        consumer_node<FrugalMover3, size_t>,
        function_node<FrugalMover3, size_t>,
        producer_node<FrugalMover3, size_t>>)) {
  using C = typename std::tuple_element<0, TestType>::type;
  using F = typename std::tuple_element<1, TestType>::type;
  using P = typename std::tuple_element<2, TestType>::type;

  auto p = P([](std::stop_source&) { return 0; });
  auto f = F([](const size_t& i) { return i; });
  auto c = C([](const size_t&) {});

  node q = p;
  node d = c;
  node g = f;

  connect(p, c);
  CHECK(p->sink_correspondent() == c);
  CHECK(c->source_correspondent() == p);

  connect(p, f);
  CHECK(p->sink_correspondent() == f);
  CHECK(f->source_correspondent() == p);

  connect(f, c);
  CHECK(f->sink_correspondent() == c);
  CHECK(c->source_correspondent() == f);

  Edge(*p, *c);
  detach(*p, *c);

  Edge(*p, *f);
  Edge(*f, *c);
}

/*
 * Define some helpers
 */
template <template <class> class T, class N>
struct hm {
  T<N> operator()(const N& n) {
    return {n};
  }
};

template <template <class> class T>
struct hm<T, node> {
  auto operator()(const node& n) {
    return Task{n};
  }
};

template <class N>
Task<node> task_from_node(N& n) {
  return Task<node>(n);
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

bool two_nodes(node_base&, node_base&) {
  return true;
}

bool two_nodes(const node&, const node&) {
  return true;
}

TEMPLATE_TEST_CASE(
    "FrugalScheduler: Extensive tests of nodes",
    "[throw_catch]",
    (std::tuple<
        consumer_node<FrugalMover2, size_t>,
        function_node<FrugalMover2, size_t>,
        producer_node<FrugalMover2, size_t>>),
    (std::tuple<
        consumer_node<FrugalMover3, size_t>,
        function_node<FrugalMover3, size_t>,
        producer_node<FrugalMover3, size_t>>)) {
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

  //  auto pro_specified =
  //      producer_node<FrugalMover3, size_t>(pro_node_impl);  // bad
  //  auto pro_deduced = producer_node(pro_node_impl);
  //  pro_deduced->fill();

  // producer_node_impl<FrugalMover3, size_t>
  //   ::producer_node_impl<producer_node_impl<FrugalMover3, size_t>>

  //  auto con_specified = consumer_node<FrugalMover3,
  //  size_t>(con_node_impl);
  //  // bad
  //  auto con_deduced = consumer_node(con_node_impl);

  // The types will print?
  // print_types(pro_deduced, con_deduced);
  //   producer_node<FrugalMover3, unsigned long>
  //   consumer_node<FrugalMover3, unsigned long>

  // print_types(pro_specified, con_specified);
  //   producer_node<FrugalMover3, unsigned long>
  //   consumer_node<FrugalMover3, unsigned long>

  SECTION("Check specified and deduced are same types") {
    //    CHECK(std::is_same_v<decltype(pro_specified), decltype(pro_deduced)>);
    //    CHECK(std::is_same_v<decltype(con_specified), decltype(con_deduced)>);
  }

  SECTION("Check polymorphism to node&") {
    CHECK(two_nodes(pro_node_impl, con_node_impl));
    CHECK(two_nodes(pro_node_impl, fun_node_impl));
    CHECK(two_nodes(fun_node_impl, con_node_impl));

    // No conversion from producer_node to node
    CHECK(two_nodes(pro_node, con_node));
    CHECK(two_nodes(pro_node, fun_node));
    CHECK(two_nodes(fun_node, con_node));

    // No conversion from producer_node to node&
    //    CHECK(two_shared_nodes(pro_node, con_node));
  }

  SECTION("Checks with Task and node (godbolt)") {
    auto shared_pro = node{pro_node};
    auto shared_fun = node{fun_node};
    auto shared_con = node{con_node};

    auto shared_nil = node{};
    shared_nil = shared_pro;
    CHECK(shared_nil == shared_pro);
  }

  SECTION("I think this works (godbolt)", "[throw_catch]") {
    auto throw_catch_pro = Task<node>{pro_node};
    auto throw_catch_fun = Task<node>{fun_node};
    auto throw_catch_con = Task<node>{con_node};

    auto throw_catch_from_pro = task_from_node(pro_node);
    auto throw_catch_from_fun = task_from_node(fun_node);
    auto throw_catch_from_con = task_from_node(con_node);

    auto throw_catch_hm_pro = hm_(pro_node);
    auto throw_catch_hm_fun = hm_(fun_node);
    auto throw_catch_hm_con = hm_(con_node);

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

    auto throw_catch_pro_1 = Task<node>{pro_node};
    auto throw_catch_pro_2 = Task<node>{pro_node};
    auto throw_catch_pro_3 = task_from_node(pro_node);
    auto throw_catch_pro_4 = hm_(pro_node);
    auto throw_catch_pro_5 = throw_catch_pro_1;
    auto throw_catch_pro_6 = throw_catch_pro_3;

    CHECK(throw_catch_pro_1 != throw_catch_pro_2);
    CHECK(throw_catch_pro_2 != throw_catch_pro_3);
    CHECK(throw_catch_pro_3 != throw_catch_pro_4);
    CHECK(throw_catch_pro_4 != throw_catch_pro_5);
    CHECK(throw_catch_pro_5 != throw_catch_pro_6);

    Task<node> throw_catch_pro_7{throw_catch_pro_2};
    Task throw_catch_pro_8{throw_catch_pro_2};

    CHECK(throw_catch_pro_6 != throw_catch_pro_7);
    CHECK(throw_catch_pro_7 == throw_catch_pro_2);
    CHECK(throw_catch_pro_7 == throw_catch_pro_8);
    CHECK(throw_catch_pro_8 != throw_catch_pro_1);

    auto throw_catch_pro_1_x = throw_catch_pro_1;
    CHECK(throw_catch_pro_1 == throw_catch_pro_1);
    CHECK(throw_catch_pro_1_x == throw_catch_pro_1);
    CHECK(throw_catch_pro_1 == throw_catch_pro_1_x);

    auto throw_catch_pro_5_x = throw_catch_pro_5;
    CHECK(throw_catch_pro_5_x == throw_catch_pro_5);

    // Warning danger -- don't use throw_catch_pro_5 after the move
    Task<node> throw_catch_pro_5_moved{std::move(throw_catch_pro_5)};
    CHECK(throw_catch_pro_5_moved == throw_catch_pro_5_x);

    /*
     * Function Case
     */

    auto throw_catch_fun_1 = Task<node>{fun_node};
    auto throw_catch_fun_2 = Task<node>{fun_node};
    auto throw_catch_fun_3 = task_from_node(fun_node);
    auto throw_catch_fun_4 = hm_(fun_node);
    auto throw_catch_fun_5 = throw_catch_fun_1;
    auto throw_catch_fun_6 = throw_catch_fun_3;

    CHECK(throw_catch_fun_1 != throw_catch_fun_2);
    CHECK(throw_catch_fun_2 != throw_catch_fun_3);
    CHECK(throw_catch_fun_3 != throw_catch_fun_4);
    CHECK(throw_catch_fun_4 != throw_catch_fun_5);
    CHECK(throw_catch_fun_5 != throw_catch_fun_6);

    Task<node> throw_catch_fun_7{throw_catch_fun_2};
    Task throw_catch_fun_8{throw_catch_fun_2};

    CHECK(throw_catch_fun_6 != throw_catch_fun_7);
    CHECK(throw_catch_fun_7 == throw_catch_fun_2);
    CHECK(throw_catch_fun_7 == throw_catch_fun_8);
    CHECK(throw_catch_fun_8 != throw_catch_fun_1);

    auto throw_catch_fun_1_x = throw_catch_fun_1;
    CHECK(throw_catch_fun_1 == throw_catch_fun_1);
    CHECK(throw_catch_fun_1_x == throw_catch_fun_1);
    CHECK(throw_catch_fun_1 == throw_catch_fun_1_x);

    auto throw_catch_fun_5_x = throw_catch_fun_5;
    CHECK(throw_catch_fun_5_x == throw_catch_fun_5);

    // Warning danger -- don't use throw_catch_fun_5 after the move
    Task<node> throw_catch_fun_5_moved{std::move(throw_catch_fun_5)};
    CHECK(throw_catch_fun_5_moved == throw_catch_fun_5_x);

    /*
     * Consumer case
     */
    auto throw_catch_con_1 = Task<node>{con_node};
    auto throw_catch_con_2 = Task<node>{con_node};
    auto throw_catch_con_3 = task_from_node(con_node);
    auto throw_catch_con_4 = hm_(con_node);
    auto throw_catch_con_5 = throw_catch_con_1;
    auto throw_catch_con_6 = throw_catch_con_3;

    CHECK(throw_catch_con_1 != throw_catch_con_2);
    CHECK(throw_catch_con_2 != throw_catch_con_3);
    CHECK(throw_catch_con_3 != throw_catch_con_4);
    CHECK(throw_catch_con_4 != throw_catch_con_5);
    CHECK(throw_catch_con_5 != throw_catch_con_6);

    Task<node> throw_catch_con_7{throw_catch_con_2};
    Task throw_catch_con_8{throw_catch_con_2};

    CHECK(throw_catch_con_6 != throw_catch_con_7);
    CHECK(throw_catch_con_7 == throw_catch_con_2);
    CHECK(throw_catch_con_7 == throw_catch_con_8);
    CHECK(throw_catch_con_8 != throw_catch_con_1);

    auto throw_catch_con_1_x = throw_catch_con_1;
    CHECK(throw_catch_con_1 == throw_catch_con_1);
    CHECK(throw_catch_con_1_x == throw_catch_con_1);
    CHECK(throw_catch_con_1 == throw_catch_con_1_x);

    auto throw_catch_con_5_x = throw_catch_con_5;
    CHECK(throw_catch_con_5_x == throw_catch_con_5);

    // Warning danger -- don't use throw_catch_con_5 after the move
    Task<node> throw_catch_con_5_moved{std::move(throw_catch_con_5)};
    CHECK(throw_catch_con_5_moved == throw_catch_con_5_x);
  }

  SECTION("Check states") {
    auto throw_catch_pro = Task<node>{pro_node};
    auto throw_catch_fun = Task<node>{fun_node};
    auto throw_catch_con = Task<node>{con_node};

    auto throw_catch_from_pro = task_from_node(pro_node);
    auto throw_catch_from_fun = task_from_node(fun_node);
    auto throw_catch_from_con = task_from_node(con_node);

    auto throw_catch_hm_pro = hm_(pro_node);
    auto throw_catch_hm_fun = hm_(fun_node);
    auto throw_catch_hm_con = hm_(con_node);

    CHECK(str(task_state(throw_catch_pro)) == "created");
    CHECK(str(task_state(throw_catch_from_pro)) == "created");
    CHECK(str(task_state(throw_catch_hm_pro)) == "created");

    CHECK(str(task_state(throw_catch_fun)) == "created");
    CHECK(str(task_state(throw_catch_from_fun)) == "created");
    CHECK(str(task_state(throw_catch_hm_fun)) == "created");

    CHECK(str(task_state(throw_catch_con)) == "created");
    CHECK(str(task_state(throw_catch_from_con)) == "created");
    CHECK(str(task_state(throw_catch_hm_con)) == "created");

    /*
     * No aliasing of tasks
     */
    task_state(throw_catch_pro) = TaskState::running;
    CHECK(str(task_state(throw_catch_pro)) == "running");

    CHECK(str(task_state(throw_catch_from_pro)) == "created");
    CHECK(str(task_state(throw_catch_hm_pro)) == "created");

    CHECK(str(task_state(throw_catch_fun)) == "created");
    CHECK(str(task_state(throw_catch_from_fun)) == "created");
    CHECK(str(task_state(throw_catch_hm_fun)) == "created");

    CHECK(str(task_state(throw_catch_con)) == "created");
    CHECK(str(task_state(throw_catch_from_con)) == "created");
    CHECK(str(task_state(throw_catch_hm_con)) == "created");

    task_state(throw_catch_pro) = TaskState::created;
    CHECK(str(task_state(throw_catch_pro)) == "created");

    CHECK(str(task_state(throw_catch_from_pro)) == "created");
    CHECK(str(task_state(throw_catch_hm_pro)) == "created");

    CHECK(str(task_state(throw_catch_fun)) == "created");
    CHECK(str(task_state(throw_catch_from_fun)) == "created");
    CHECK(str(task_state(throw_catch_hm_fun)) == "created");

    CHECK(str(task_state(throw_catch_con)) == "created");
    CHECK(str(task_state(throw_catch_from_con)) == "created");
    CHECK(str(task_state(throw_catch_hm_con)) == "created");

    task_state(throw_catch_con) = TaskState::running;
    CHECK(str(task_state(throw_catch_con)) == "running");

    CHECK(str(task_state(throw_catch_pro)) == "created");
    CHECK(str(task_state(throw_catch_from_pro)) == "created");
    CHECK(str(task_state(throw_catch_hm_pro)) == "created");

    CHECK(str(task_state(throw_catch_fun)) == "created");
    CHECK(str(task_state(throw_catch_from_fun)) == "created");
    CHECK(str(task_state(throw_catch_hm_fun)) == "created");

    CHECK(str(task_state(throw_catch_from_con)) == "created");
    CHECK(str(task_state(throw_catch_hm_con)) == "created");
  }
}

/*
 * Some deduction guides
 */
namespace tiledb::common {
Task(node&) -> Task<node>;

Task(const node&) -> Task<node>;

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
    "FrugalScheduler: Test Task",
    "[throw_catch]",
    (std::tuple<
        consumer_node<FrugalMover2, size_t>,
        function_node<FrugalMover2, size_t>,
        producer_node<FrugalMover2, size_t>>),
    (std::tuple<
        consumer_node<FrugalMover3, size_t>,
        function_node<FrugalMover3, size_t>,
        producer_node<FrugalMover3, size_t>>)) {
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
  auto pro_node =
      producer_node<FrugalMover3, size_t>([](std::stop_source&) { return 0; });
  auto con_node = consumer_node<FrugalMover3, size_t>([](const size_t&) {});

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
  auto pro_node =
      producer_node<FrugalMover3, size_t>([](std::stop_source&) { return 0; });
  auto con_node = consumer_node<FrugalMover3, size_t>([](const size_t&) {});

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
  auto pro_node =
      producer_node<FrugalMover3, size_t>([](std::stop_source&) { return 0; });
  auto con_node = consumer_node<FrugalMover3, size_t>([](const size_t&) {});

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

TEST_CASE("FrugalScheduler: Test construct scheduler", "[throw_catch]") {
  [[maybe_unused]] auto sched = FrugalScheduler<node>(1);
  // sched goes out of scope and shuts down the scheduler
}

TEST_CASE("FrugalScheduler: Test Task state changes", "[throw_catch]") {
  [[maybe_unused]] auto sched =
      SchedulerStateMachine<EmptySchedulerPolicy<Task<node>>>{};

  auto p =
      producer_node<FrugalMover3, size_t>([](std::stop_source&) { return 0; });
  auto c = consumer_node<FrugalMover3, size_t>([](const size_t&) {});

  auto q = Task<node>{p};
  auto d = Task<node>{c};

  // Can't directly compare nodes of different types
  // CHECK(p != c);
  CHECK(d != q);

  auto e = d;
  CHECK(e == d);

  // Cannot do this
  // auto r = p;
  // CHECK(r == p);

  auto s = q;
  CHECK(s == q);

  // Cannot do this
  // p.dump_task_state();

  // Can to this
  // q.dump_task_state();

  SECTION("Admit") {
    // Cannot do this
    // sched.task_admit(p);
    // CHECK(str(p.state()) == "runnable");

    // Can do this
    sched.task_admit(q);
    CHECK(str(q->task_state()) == "runnable");
    CHECK(str(s->task_state()) == "runnable");

    // Can't compare different types
    // CHECK(p == q);
    CHECK(s == q);
  }
}

TEMPLATE_TEST_CASE(
    "FrugalScheduler: Test submit nodes",
    "[throw_catch]",
    (std::tuple<
        consumer_node<FrugalMover2, size_t>,
        producer_node<FrugalMover2, size_t>>),
    (std::tuple<
        consumer_node<FrugalMover3, size_t>,
        producer_node<FrugalMover3, size_t>>)) {
  using C = typename std::tuple_element<0, TestType>::type;
  using P = typename std::tuple_element<1, TestType>::type;

  [[maybe_unused]] auto sched = FrugalScheduler<node>(1);

  auto p = P([](std::stop_source&) { return 0; });
  auto c = C([](const size_t&) {});

  connect(p, c);
  Edge(*p, *c);
  sched.submit(p);
  sched.submit(c);
}

TEMPLATE_TEST_CASE(
    "FrugalScheduler: Test submit and wait nodes",
    "[throw_catch]",
    (std::tuple<
        consumer_node<FrugalMover2, size_t>,
        producer_node<FrugalMover2, size_t>>),
    (std::tuple<
        consumer_node<FrugalMover3, size_t>,
        producer_node<FrugalMover3, size_t>>)) {
  using C = typename std::tuple_element<0, TestType>::type;
  using P = typename std::tuple_element<1, TestType>::type;

  bool debug{false};

  auto num_threads = GENERATE(1, 2, 3, 4, 5, 8, 17);

  auto rounds = problem_size;

  if (debug) {
    rounds = debug_problem_size;
  }

  SECTION("With " + std::to_string(num_threads) + " threads") {
    [[maybe_unused]] auto sched = FrugalScheduler<node>(num_threads);

    if (debug) {
      sched.enable_debug();
    }

    size_t i{0};

    auto p = P([rounds, debug, &i](std::stop_source& stop_source) {
      CHECK(!stop_source.stop_requested());

      if (debug)
        std::cout << "Producing\n";
      if (i >= rounds) {
        stop_source.request_stop();
      }
      return i++;
      ;
    });
    auto c = C([debug](const size_t&) {
      if (debug)
        std::cout << "Consuming\n";
    });

    connect(p, c);
    Edge(*p, *c);

    if (debug) {
      p->enable_debug();
      c->enable_debug();
    }

    if (sched.debug_enabled())
      std::cout << "==========================================================="
                   "=====\n";

    sched.submit(p);
    sched.submit(c);
    if (sched.debug_enabled())
      std::cout << "-----------------------------------------------------------"
                   "-----\n";
    sched.sync_wait_all();

    if (sched.debug_enabled())
      std::cout << "==========================================================="
                   "=====\n";

    CHECK(p->produced_items() == rounds);
    CHECK(c->consumed_items() == rounds);

#if 0
    if (debug) {
      CHECK(problem_size == debug_problem_size);
    } else {
      CHECK(problem_size == 1337);
    }
#endif
  }
}

TEMPLATE_TEST_CASE(
    "FrugalScheduler: Test passing integers",
    "[throw_catch]",
    (std::tuple<
        consumer_node<FrugalMover2, size_t>,
        function_node<FrugalMover2, size_t>,
        producer_node<FrugalMover2, size_t>>),
    (std::tuple<
        consumer_node<FrugalMover3, size_t>,
        function_node<FrugalMover3, size_t>,
        producer_node<FrugalMover3, size_t>>)) {
  using C = typename std::tuple_element<0, TestType>::type;
  using F = typename std::tuple_element<1, TestType>::type;
  using P = typename std::tuple_element<2, TestType>::type;

  bool debug{false};

  auto num_threads = GENERATE(1, 2, 3, 4, 5, 17);

  if (debug) {
    problem_size = debug_problem_size;
  }

  auto rounds = problem_size;

  std::vector<size_t> input(rounds);
  std::vector<size_t> output(rounds);

  std::iota(input.begin(), input.end(), 19);
  std::fill(output.begin(), output.end(), 0);
  auto i = input.begin();
  auto j = output.begin();

  if (rounds != 0) {
    CHECK(std::equal(input.begin(), input.end(), output.begin()) == false);
  }

  SECTION(
      "With " + std::to_string(num_threads) + " threads and " +
      std::to_string(rounds) + " integers") {
    [[maybe_unused]] auto sched = FrugalScheduler<node>(num_threads);

    if (debug) {
      sched.enable_debug();
    }

    auto p = P([&sched, &i, &input](std::stop_source& stop_source) {
      if (sched.debug_enabled())
        std::cout << "Producing " + std::to_string(*i) + " with distance " +
                         std::to_string(std::distance(input.begin(), i)) + "\n";
      if (std::distance(input.begin(), i) >= static_cast<long>(problem_size)) {
        if (sched.debug_enabled()) {
          std::cout << "Requesting stop\n";
        }

        stop_source.request_stop();
        return *(input.begin()) + 1;
      }

      if (sched.debug_enabled())
        std::cout << "producer function returning " + std::to_string(*i) + "\n";

      return (*i++) + 1;
    });
    auto f = F([&sched](std::size_t k) {
      if (sched.debug_enabled())
        std::cout << "Transforming " + std::to_string(k) + " to "
                  << std::to_string(k - 1) + "\n";
      return k - 1;
    });

    auto c = C([&j, &output, &debug](std::size_t k) {
      if (debug)
        std::cout << "Consuming " + std::to_string(k) + " with distance " +
                         std::to_string(std::distance(output.begin(), j)) +
                         "\n";

      *j++ = k;
    });

    connect(p, f);
    connect(f, c);
    Edge(*p, *f);
    Edge(*f, *c);

    sched.submit(p);
    sched.submit(c);
    sched.submit(f);
    sched.sync_wait_all();
  }
  CHECK(rounds != 0);
  CHECK(rounds == problem_size);

  CHECK(input.begin() != i);
  CHECK(input.size() == rounds);
  CHECK(output.size() == rounds);

  if (debug)
    std::cout << std::distance(input.begin(), i) << std::endl;

  CHECK(std::equal(input.begin(), i, output.begin()));

  if (debug)
    std::cout << "First input = " + std::to_string(*(input.begin())) +
                     ", First output = " + std::to_string(*(output.begin())) +
                     "\n";

  CHECK(std::distance(input.begin(), i) == static_cast<long>(rounds));
}
