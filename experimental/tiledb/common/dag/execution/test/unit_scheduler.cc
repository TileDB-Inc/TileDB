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
 * This file tests schedulers that may be used with dag.  All schedulers undergo
 * identical unit tests to ensure functional and API compatibility.
 */

#include "experimental/tiledb/common/dag/nodes/segmented_nodes.h"

#include <atomic>
#include <iostream>
#include <map>
#include <numeric>
#include <type_traits>

#include "unit_scheduler.h"

#include "../bountiful.h"
#include "../frugal.h"
#include "../throw_catch.h"

#include "experimental/tiledb/common/dag/edge/edge.h"
#include "experimental/tiledb/common/dag/execution/task_state_machine.h"

#include "experimental/tiledb/common/dag/nodes/terminals.h"
#include "experimental/tiledb/common/dag/ports/ports.h"
#include "experimental/tiledb/common/dag/state_machine/test/helpers.h"

#include "experimental/tiledb/common/dag/utility/print_types.h"

using namespace tiledb::common;

size_t problem_size = 1337UL;
size_t debug_problem_size = 3;

TEMPLATE_TEST_CASE(
    "Scheduler: Test construct scheduler",
    "[scheduler]",
    (std::tuple<
        consumer_node<ThrowCatchMover2, size_t>,
        function_node<ThrowCatchMover2, size_t>,
        producer_node<ThrowCatchMover2, size_t>,
        ThrowCatchScheduler<node>>),
    (std::tuple<
        consumer_node<ThrowCatchMover3, size_t>,
        function_node<ThrowCatchMover3, size_t>,
        producer_node<ThrowCatchMover3, size_t>,
        ThrowCatchScheduler<node>>),
    (std::tuple<
        consumer_node<BountifulMover2, size_t>,
        function_node<BountifulMover2, size_t>,
        producer_node<BountifulMover2, size_t>,
        BountifulScheduler<node>>),
    (std::tuple<
        consumer_node<BountifulMover3, size_t>,
        function_node<BountifulMover3, size_t>,
        producer_node<BountifulMover3, size_t>,
        BountifulScheduler<node>>),
    (std::tuple<
        consumer_node<FrugalMover2, size_t>,
        function_node<FrugalMover2, size_t>,
        producer_node<FrugalMover2, size_t>,
        FrugalScheduler<node>>),
    (std::tuple<
        consumer_node<FrugalMover3, size_t>,
        function_node<FrugalMover3, size_t>,
        producer_node<FrugalMover3, size_t>,
        FrugalScheduler<node>>)) {
  using S = std::tuple_element_t<3, TestType>;
  auto sched = S(1);
}

TEMPLATE_TEST_CASE(
    "Scheduler: Test creating nodes",
    "[scheduler]",
    (std::tuple<
        consumer_node<ThrowCatchMover2, size_t>,
        function_node<ThrowCatchMover2, size_t>,
        producer_node<ThrowCatchMover2, size_t>,
        ThrowCatchScheduler<node>>),
    (std::tuple<
        consumer_node<ThrowCatchMover3, size_t>,
        function_node<ThrowCatchMover3, size_t>,
        producer_node<ThrowCatchMover3, size_t>,
        ThrowCatchScheduler<node>>),
    (std::tuple<
        consumer_node<BountifulMover2, size_t>,
        function_node<BountifulMover2, size_t>,
        producer_node<BountifulMover2, size_t>,
        BountifulScheduler<node>>),
    (std::tuple<
        consumer_node<BountifulMover3, size_t>,
        function_node<BountifulMover3, size_t>,
        producer_node<BountifulMover3, size_t>,
        BountifulScheduler<node>>),
    (std::tuple<
        consumer_node<FrugalMover2, size_t>,
        function_node<FrugalMover2, size_t>,
        producer_node<FrugalMover2, size_t>,
        FrugalScheduler<node>>),
    (std::tuple<
        consumer_node<FrugalMover3, size_t>,
        function_node<FrugalMover3, size_t>,
        producer_node<FrugalMover3, size_t>,
        FrugalScheduler<node>>)) {
  using C = typename std::tuple_element<0, TestType>::type;
  using F = typename std::tuple_element<1, TestType>::type;
  using P = typename std::tuple_element<2, TestType>::type;

  auto p = P([](std::stop_source&) { return 0; });
  auto f = F([](const size_t& i) { return i; });
  auto c = C([](const size_t&) {});
}

TEMPLATE_TEST_CASE(
    "Scheduler: Test assigning nodes",
    "[scheduler]",
    (std::tuple<
        consumer_node<ThrowCatchMover2, size_t>,
        function_node<ThrowCatchMover2, size_t>,
        producer_node<ThrowCatchMover2, size_t>,
        ThrowCatchScheduler<node>>),
    (std::tuple<
        consumer_node<ThrowCatchMover3, size_t>,
        function_node<ThrowCatchMover3, size_t>,
        producer_node<ThrowCatchMover3, size_t>,
        ThrowCatchScheduler<node>>),
    (std::tuple<
        consumer_node<BountifulMover2, size_t>,
        function_node<BountifulMover2, size_t>,
        producer_node<BountifulMover2, size_t>,
        BountifulScheduler<node>>),
    (std::tuple<
        consumer_node<BountifulMover3, size_t>,
        function_node<BountifulMover3, size_t>,
        producer_node<BountifulMover3, size_t>,
        BountifulScheduler<node>>),
    (std::tuple<
        consumer_node<FrugalMover2, size_t>,
        function_node<FrugalMover2, size_t>,
        producer_node<FrugalMover2, size_t>,
        FrugalScheduler<node>>),
    (std::tuple<
        consumer_node<FrugalMover3, size_t>,
        function_node<FrugalMover3, size_t>,
        producer_node<FrugalMover3, size_t>,
        FrugalScheduler<node>>)) {
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
    "Scheduler: Test connect nodes",
    "[scheduler]",
    (std::tuple<
        consumer_node<ThrowCatchMover2, size_t>,
        function_node<ThrowCatchMover2, size_t>,
        producer_node<ThrowCatchMover2, size_t>,
        ThrowCatchScheduler<node>>),
    (std::tuple<
        consumer_node<ThrowCatchMover3, size_t>,
        function_node<ThrowCatchMover3, size_t>,
        producer_node<ThrowCatchMover3, size_t>,
        ThrowCatchScheduler<node>>),
    (std::tuple<
        consumer_node<BountifulMover2, size_t>,
        function_node<BountifulMover2, size_t>,
        producer_node<BountifulMover2, size_t>,
        BountifulScheduler<node>>),
    (std::tuple<
        consumer_node<BountifulMover3, size_t>,
        function_node<BountifulMover3, size_t>,
        producer_node<BountifulMover3, size_t>,
        BountifulScheduler<node>>),
    (std::tuple<
        consumer_node<FrugalMover2, size_t>,
        function_node<FrugalMover2, size_t>,
        producer_node<FrugalMover2, size_t>,
        FrugalScheduler<node>>),
    (std::tuple<
        consumer_node<FrugalMover3, size_t>,
        function_node<FrugalMover3, size_t>,
        producer_node<FrugalMover3, size_t>,
        FrugalScheduler<node>>)) {
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

TEMPLATE_TEST_CASE(
    "Scheduler: ThrowCatch, Bountiful, and Frugal: Two nodes and three nodes",
    "[scheduler]",
    (std::tuple<
        consumer_node<ThrowCatchMover2, size_t>,
        function_node<ThrowCatchMover2, size_t>,
        producer_node<ThrowCatchMover2, size_t>,
        ThrowCatchScheduler<node>>),
    (std::tuple<
        consumer_node<ThrowCatchMover3, size_t>,
        function_node<ThrowCatchMover3, size_t>,
        producer_node<ThrowCatchMover3, size_t>,
        ThrowCatchScheduler<node>>),
    (std::tuple<
        consumer_node<BountifulMover2, size_t>,
        function_node<BountifulMover2, size_t>,
        producer_node<BountifulMover2, size_t>,
        BountifulScheduler<node>>),
    (std::tuple<
        consumer_node<BountifulMover3, size_t>,
        function_node<BountifulMover3, size_t>,
        producer_node<BountifulMover3, size_t>,
        BountifulScheduler<node>>),
    (std::tuple<
        consumer_node<FrugalMover2, size_t>,
        function_node<FrugalMover2, size_t>,
        producer_node<FrugalMover2, size_t>,
        FrugalScheduler<node>>),
    (std::tuple<
        consumer_node<FrugalMover3, size_t>,
        function_node<FrugalMover3, size_t>,
        producer_node<FrugalMover3, size_t>,
        FrugalScheduler<node>>)) {
  bool debug{false};

  auto num_threads = GENERATE(1, 2, 3, 4, 5, 8, 17);

  if ((!std::is_same_v<
           typename std::tuple_element<0, TestType>::type,
           consumer_node<FrugalMover2, size_t>> &&
       !std::is_same_v<
           typename std::tuple_element<0, TestType>::type,
           consumer_node<FrugalMover2, size_t>> &&
       !std::is_same_v<
           typename std::tuple_element<0, TestType>::type,
           consumer_node<FrugalMover3, size_t>> &&
       !std::is_same_v<
           typename std::tuple_element<0, TestType>::type,
           consumer_node<FrugalMover3, size_t>>) ||
      num_threads > 3) {
    SECTION("With " + std::to_string(num_threads) + " threads") {
      size_t rounds = 5;
      using C = typename std::tuple_element<0, TestType>::type;
      using F = typename std::tuple_element<1, TestType>::type;
      using P = typename std::tuple_element<2, TestType>::type;
      using S = typename std::tuple_element<3, TestType>::type;

      // print_types(C{});

      auto sched = S(num_threads);

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
#if 1
      connect(p, f);
      connect(f, c);

      Edge(*p, *f);
      Edge(*f, *c);

      sched.submit(p);
      sched.submit(f);
      sched.submit(c);
#else
      connect(p, c);

      Edge(*p, *c);

      sched.submit(p);
      sched.submit(c);
#endif
      if (debug) {
        sched.enable_debug();

        // p->enable_debug();
        f->enable_debug();
        c->enable_debug();
      }

      sched.sync_wait_all();
    }
  }
}

TEMPLATE_TEST_CASE(
    "Scheduler: Test soft terminate of sink",
    "[scheduler]",
    (std::tuple<
        consumer_node<ThrowCatchMover2, size_t>,
        function_node<ThrowCatchMover2, size_t>,
        producer_node<ThrowCatchMover2, size_t>,
        ThrowCatchScheduler<node>>),
    (std::tuple<
        consumer_node<ThrowCatchMover3, size_t>,
        function_node<ThrowCatchMover3, size_t>,
        producer_node<ThrowCatchMover3, size_t>,
        ThrowCatchScheduler<node>>),
    (std::tuple<
        consumer_node<BountifulMover2, size_t>,
        function_node<BountifulMover2, size_t>,
        producer_node<BountifulMover2, size_t>,
        BountifulScheduler<node>>),
    (std::tuple<
        consumer_node<BountifulMover3, size_t>,
        function_node<BountifulMover3, size_t>,
        producer_node<BountifulMover3, size_t>,
        BountifulScheduler<node>>),
    (std::tuple<
        consumer_node<FrugalMover2, size_t>,
        function_node<FrugalMover2, size_t>,
        producer_node<FrugalMover2, size_t>,
        FrugalScheduler<node>>),
    (std::tuple<
        consumer_node<FrugalMover3, size_t>,
        function_node<FrugalMover3, size_t>,
        producer_node<FrugalMover3, size_t>,
        FrugalScheduler<node>>)) {
  bool debug{false};

  auto num_threads = GENERATE(1, 2, 3, 4, 5, 8, 17);

  if ((!std::is_same_v<
           typename std::tuple_element<0, TestType>::type,
           consumer_node<FrugalMover2, size_t>> &&
       !std::is_same_v<
           typename std::tuple_element<0, TestType>::type,
           consumer_node<FrugalMover2, size_t>> &&
       !std::is_same_v<
           typename std::tuple_element<0, TestType>::type,
           consumer_node<FrugalMover3, size_t>> &&
       !std::is_same_v<
           typename std::tuple_element<0, TestType>::type,
           consumer_node<FrugalMover3, size_t>>) ||
      num_threads > 4) {
    size_t rounds = 5;

    using C = typename std::tuple_element<0, TestType>::type;
    using F = typename std::tuple_element<1, TestType>::type;
    using P = typename std::tuple_element<2, TestType>::type;
    using S = typename std::tuple_element<3, TestType>::type;

    auto sched = S(num_threads);

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
}

TEMPLATE_TEST_CASE(
    "Scheduler: Test submit nodes",
    "[scheduler]",
    (std::tuple<
        consumer_node<ThrowCatchMover2, size_t>,
        function_node<ThrowCatchMover2, size_t>,
        producer_node<ThrowCatchMover2, size_t>,
        ThrowCatchScheduler<node>>),
    (std::tuple<
        consumer_node<ThrowCatchMover3, size_t>,
        function_node<ThrowCatchMover3, size_t>,
        producer_node<ThrowCatchMover3, size_t>,
        ThrowCatchScheduler<node>>),
    (std::tuple<
        consumer_node<BountifulMover2, size_t>,
        function_node<BountifulMover2, size_t>,
        producer_node<BountifulMover2, size_t>,
        BountifulScheduler<node>>),
    (std::tuple<
        consumer_node<BountifulMover3, size_t>,
        function_node<BountifulMover3, size_t>,
        producer_node<BountifulMover3, size_t>,
        BountifulScheduler<node>>),
    (std::tuple<
        consumer_node<FrugalMover2, size_t>,
        function_node<FrugalMover2, size_t>,
        producer_node<FrugalMover2, size_t>,
        FrugalScheduler<node>>),
    (std::tuple<
        consumer_node<FrugalMover3, size_t>,
        function_node<FrugalMover3, size_t>,
        producer_node<FrugalMover3, size_t>,
        FrugalScheduler<node>>)) {
  auto num_threads = GENERATE(1, 2, 3, 4, 5, 8, 17);

  if ((!std::is_same_v<
           typename std::tuple_element<0, TestType>::type,
           consumer_node<FrugalMover2, size_t>> &&
       !std::is_same_v<
           typename std::tuple_element<0, TestType>::type,
           consumer_node<FrugalMover2, size_t>> &&
       !std::is_same_v<
           typename std::tuple_element<0, TestType>::type,
           consumer_node<FrugalMover3, size_t>> &&
       !std::is_same_v<
           typename std::tuple_element<0, TestType>::type,
           consumer_node<FrugalMover3, size_t>>) ||
      num_threads > 43) {
    using C = typename std::tuple_element<0, TestType>::type;
    using F = typename std::tuple_element<1, TestType>::type;
    using P = typename std::tuple_element<2, TestType>::type;
    using S = typename std::tuple_element<3, TestType>::type;

    auto p = P([](std::stop_source& stop_source) {
      stop_source.request_stop();
      return 0;
    });
    auto f = F([](const size_t& i) { return i; });
    auto c = C([](const size_t&) {});
    auto sched = S(num_threads);

    const bool debug{false};
    if (debug) {
      sched.enable_debug();

      p->enable_debug();
      c->enable_debug();
    }
    SECTION("Producer Consumer") {
      connect(p, c);
      Edge(*p, *c);
      sched.submit(p);
      sched.submit(c);
      sched.sync_wait_all();
    }
    SECTION("Producer Function Consumer") {
      connect(p, f);
      connect(f, c);
      Edge(*p, *f);
      Edge(*f, *c);
      sched.submit(p);
      sched.submit(f);
      sched.submit(c);
      sched.sync_wait_all();
    }
  }
}

TEMPLATE_TEST_CASE(
    "Scheduler: Test submit and wait nodes",
    "[scheduler]",
    (std::tuple<
        consumer_node<ThrowCatchMover2, size_t>,
        function_node<ThrowCatchMover2, size_t>,
        producer_node<ThrowCatchMover2, size_t>,
        ThrowCatchScheduler<node>>),
    (std::tuple<
        consumer_node<ThrowCatchMover3, size_t>,
        function_node<ThrowCatchMover3, size_t>,
        producer_node<ThrowCatchMover3, size_t>,
        ThrowCatchScheduler<node>>),
    (std::tuple<
        consumer_node<BountifulMover2, size_t>,
        function_node<BountifulMover2, size_t>,
        producer_node<BountifulMover2, size_t>,
        BountifulScheduler<node>>),
    (std::tuple<
        consumer_node<BountifulMover3, size_t>,
        function_node<BountifulMover3, size_t>,
        producer_node<BountifulMover3, size_t>,
        BountifulScheduler<node>>),
    (std::tuple<
        consumer_node<FrugalMover2, size_t>,
        function_node<FrugalMover2, size_t>,
        producer_node<FrugalMover2, size_t>,
        FrugalScheduler<node>>),
    (std::tuple<
        consumer_node<FrugalMover3, size_t>,
        function_node<FrugalMover3, size_t>,
        producer_node<FrugalMover3, size_t>,
        FrugalScheduler<node>>)) {
  using C = typename std::tuple_element<0, TestType>::type;
  using P = typename std::tuple_element<2, TestType>::type;
  using S = typename std::tuple_element<3, TestType>::type;

  bool debug{false};

  auto num_threads = GENERATE(1, 2, 3, 4, 5, 8, 17);

  if ((!std::is_same_v<
           typename std::tuple_element<0, TestType>::type,
           consumer_node<FrugalMover2, size_t>> &&
       !std::is_same_v<
           typename std::tuple_element<0, TestType>::type,
           consumer_node<FrugalMover2, size_t>> &&
       !std::is_same_v<
           typename std::tuple_element<0, TestType>::type,
           consumer_node<FrugalMover3, size_t>> &&
       !std::is_same_v<
           typename std::tuple_element<0, TestType>::type,
           consumer_node<FrugalMover3, size_t>>) ||
      num_threads > 4) {
    auto rounds = problem_size;

    if (debug) {
      rounds = debug_problem_size;
    }

    SECTION("With " + std::to_string(num_threads) + " threads") {
      auto sched = S(num_threads);

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
        std::cout
            << "==========================================================="
               "=====\n";

      sched.submit(p);
      sched.submit(c);
      if (sched.debug_enabled())
        std::cout
            << "-----------------------------------------------------------"
               "-----\n";
      sched.sync_wait_all();

      if (sched.debug_enabled())
        std::cout
            << "==========================================================="
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
}

TEMPLATE_TEST_CASE(
    "Scheduler: Test passing integers",
    "[scheduler]",
    (std::tuple<
        consumer_node<ThrowCatchMover2, size_t>,
        function_node<ThrowCatchMover2, size_t>,
        producer_node<ThrowCatchMover2, size_t>,
        ThrowCatchScheduler<node>>),
    (std::tuple<
        consumer_node<ThrowCatchMover3, size_t>,
        function_node<ThrowCatchMover3, size_t>,
        producer_node<ThrowCatchMover3, size_t>,
        ThrowCatchScheduler<node>>),
    (std::tuple<
        consumer_node<BountifulMover2, size_t>,
        function_node<BountifulMover2, size_t>,
        producer_node<BountifulMover2, size_t>,
        BountifulScheduler<node>>),
    (std::tuple<
        consumer_node<BountifulMover3, size_t>,
        function_node<BountifulMover3, size_t>,
        producer_node<BountifulMover3, size_t>,
        BountifulScheduler<node>>),
    (std::tuple<
        consumer_node<FrugalMover2, size_t>,
        function_node<FrugalMover2, size_t>,
        producer_node<FrugalMover2, size_t>,
        FrugalScheduler<node>>),
    (std::tuple<
        consumer_node<FrugalMover3, size_t>,
        function_node<FrugalMover3, size_t>,
        producer_node<FrugalMover3, size_t>,
        FrugalScheduler<node>>)) {
  using C = typename std::tuple_element<0, TestType>::type;
  using F = typename std::tuple_element<1, TestType>::type;
  using P = typename std::tuple_element<2, TestType>::type;
  using S = typename std::tuple_element<3, TestType>::type;

  bool debug{false};

  auto num_threads = GENERATE(1, 2, 3, 4, 5, 8, 17);

  if ((!std::is_same_v<
           typename std::tuple_element<0, TestType>::type,
           consumer_node<FrugalMover2, size_t>> &&
       !std::is_same_v<
           typename std::tuple_element<0, TestType>::type,
           consumer_node<FrugalMover2, size_t>> &&
       !std::is_same_v<
           typename std::tuple_element<0, TestType>::type,
           consumer_node<FrugalMover3, size_t>> &&
       !std::is_same_v<
           typename std::tuple_element<0, TestType>::type,
           consumer_node<FrugalMover3, size_t>>) ||
      num_threads > 3) {
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
      auto sched = S(num_threads);

      if (debug) {
        sched.enable_debug();
      }

      auto p = P([&sched, &i, &input](std::stop_source& stop_source) {
        if (sched.debug_enabled())
          std::cout << "Producing " + std::to_string(*i) + " with distance " +
                           std::to_string(std::distance(input.begin(), i)) +
                           "\n";
        if (std::distance(input.begin(), i) >=
            static_cast<long>(problem_size)) {
          if (sched.debug_enabled()) {
            std::cout << "Requesting stop\n";
          }

          stop_source.request_stop();
          return *(input.begin()) + 1;
        }

        if (sched.debug_enabled())
          std::cout << "producer function returning " + std::to_string(*i) +
                           "\n";

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

      if (debug) {
        f->enable_debug();
      }

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
}
