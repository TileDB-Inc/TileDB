/**
 * @file unit_duffs.cc
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

#include <numeric>

#include "unit_duffs.h"
#include "../duffs.h"
#include "experimental/tiledb/common/dag/edge/edge.h"
#include "experimental/tiledb/common/dag/nodes/segmented_nodes.h"

using namespace tiledb::common;

using S = tiledb::common::DuffsScheduler<node>;
using C2 = consumer_node<DuffsMover2, size_t>;
using F2 = function_node<DuffsMover2, size_t>;
using P2 = producer_node<DuffsMover2, size_t>;

using C3 = consumer_node<DuffsMover3, size_t>;
using F3 = function_node<DuffsMover3, size_t>;
using P3 = producer_node<DuffsMover3, size_t>;

TEST_CASE("Construct Scheduler", "[duffs]") {
  auto sched = S(1);
}

TEST_CASE("Construct functions", "[duffs]") {
  // Test 2 stage edges -- 3 stage will have different behavior,
  // so needs its own test case
  auto p = P2([](std::stop_source&) { return 0; });
  auto f = F2([](const size_t& i) { return i; });
  auto c = C2([](const size_t&) {});
}

// @todo Note this might change, depending on how we handle decrementing
// the program counter for wait.

TEST_CASE("Resume functions", "[duffs]") {
  auto p = P2([](std::stop_source&) { return 0; });
  auto f = F2([](const size_t& i) { return i; });
  auto c = C2([](const size_t&) {});

  connect(p, f);
  connect(f, c);
  Edge(*p, *f);
  Edge(*f, *c);

  SECTION("Test Producer in Isolation ") {
    // One pass through node operation
    auto x = p->resume();
    CHECK(p->get_program_counter() == 3);
    CHECK(str(x) == "notify_sink");
    x = p->resume();
    CHECK(p->get_program_counter() == 5);
    CHECK(str(x) == "noop");
    x = p->resume();
    CHECK(p->get_program_counter() == 0);
    CHECK(str(x) == "yield");

    // Second pass through node operation -- this should wait since port will be
    // full
    x = p->resume();
    CHECK(p->get_program_counter() == 3);
    CHECK(str(x) == "notify_sink");
    x = p->resume();

    // Recall that wait decrements the program counter
    CHECK(p->get_program_counter() == 4);
    CHECK(str(x) == "source_wait");
    // Don't resume further after wait
  }

  SECTION("Test Consumer in Isolation ") {
    // One pass through node operation
    auto x = c->resume();

    // Recall that wait decrements the program counter
    CHECK(c->get_program_counter() == 0);
    CHECK(str(x) == "sink_wait");
  }

  SECTION("Test Function in Isolation ") {
    // One pass through node operation
    auto x = f->resume();

    // Recall that wait decrements the program counter
    CHECK(f->get_program_counter() == 0);
    CHECK(str(x) == "sink_wait");
  }

  SECTION("Emulate Passing Datum") {
    CHECK(p->get_program_counter() == 0);
    CHECK(f->get_program_counter() == 0);
    CHECK(c->get_program_counter() == 0);

    // Inject datum
    auto x = p->resume();
    CHECK(p->get_program_counter() == 3);
    CHECK(str(x) == "notify_sink");
    x = p->resume();
    CHECK(p->get_program_counter() == 5);
    CHECK(str(x) == "noop");
    x = p->resume();
    CHECK(p->get_program_counter() == 0);
    CHECK(str(x) == "yield");

    // Move datum to next node
    auto y = f->resume();  // pull
    CHECK(f->get_program_counter() == 1);
    CHECK(str(y) == "noop");

    y = f->resume();  // drain
    CHECK(f->get_program_counter() == 3);
    CHECK(str(y) == "notify_source");

    y = f->resume();  // fill
    CHECK(f->get_program_counter() == 7);
    CHECK(str(y) == "notify_sink");

    y = f->resume();  // push
    CHECK(f->get_program_counter() == 9);
    CHECK(str(y) == "noop");

    y = f->resume();  // yield
    CHECK(f->get_program_counter() == 0);
    CHECK(str(y) == "yield");

    auto z = c->resume();  // pull
    CHECK(c->get_program_counter() == 1);
    CHECK(str(z) == "noop");

    // Move datum to last node
    z = c->resume();  // drain
    CHECK(c->get_program_counter() == 3);
    CHECK(str(z) == "notify_source");

    z = c->resume();
    CHECK(c->get_program_counter() == 0);
    CHECK(str(z) == "yield");
  }

  SECTION("Emulate Passing Data, with Some Blocking") {
    CHECK(p->get_program_counter() == 0);
    CHECK(f->get_program_counter() == 0);
    CHECK(c->get_program_counter() == 0);

    // Inject datum
    auto x = p->resume();
    CHECK(p->get_program_counter() == 3);
    CHECK(str(x) == "notify_sink");
    x = p->resume();
    CHECK(p->get_program_counter() == 5);
    CHECK(str(x) == "noop");
    x = p->resume();
    CHECK(p->get_program_counter() == 0);
    CHECK(str(x) == "yield");

    x = p->resume();
    CHECK(p->get_program_counter() == 3);
    CHECK(str(x) == "notify_sink");
    x = p->resume();

    // Recall that wait decrements the program counter
    CHECK(p->get_program_counter() == 4);
    CHECK(str(x) == "source_wait");

    // Move datum to next node
    auto y = f->resume();  // pull
    CHECK(f->get_program_counter() == 1);
    CHECK(str(y) == "noop");

    y = f->resume();  // drain
    CHECK(f->get_program_counter() == 3);
    CHECK(str(y) == "notify_source");

    y = f->resume();  // fill
    CHECK(f->get_program_counter() == 7);
    CHECK(str(y) == "notify_sink");

    y = f->resume();  // push
    CHECK(f->get_program_counter() == 9);
    CHECK(str(y) == "noop");

    y = f->resume();  // yield
    CHECK(f->get_program_counter() == 0);
    CHECK(str(y) == "yield");

    // Move datum to next node
    y = f->resume();  // pull
    CHECK(f->get_program_counter() == 1);
    CHECK(str(y) == "noop");

    y = f->resume();  // drain
    CHECK(f->get_program_counter() == 3);
    CHECK(str(y) == "notify_source");

    y = f->resume();  // fill
    CHECK(f->get_program_counter() == 7);
    CHECK(str(y) == "notify_sink");

    y = f->resume();  // push

    // Recall that wait decrements the program counter
    CHECK(f->get_program_counter() == 8);
    CHECK(str(y) == "source_wait");

    auto z = c->resume();  // pull
    CHECK(c->get_program_counter() == 1);
    CHECK(str(z) == "noop");

    // Move datum to last node
    z = c->resume();  // drain
    CHECK(c->get_program_counter() == 3);
    CHECK(str(z) == "notify_source");

    z = c->resume();
    CHECK(c->get_program_counter() == 0);
    CHECK(str(z) == "yield");

    z = c->resume();  // pull
    CHECK(c->get_program_counter() == 1);
    CHECK(str(z) == "noop");

    // Move datum to last node
    z = c->resume();  // drain
    CHECK(c->get_program_counter() == 3);
    CHECK(str(z) == "notify_source");

    z = c->resume();
    CHECK(c->get_program_counter() == 0);
    CHECK(str(z) == "yield");
  }

  SECTION("Emulate Pulling Data, with Some Blocking") {
    auto z = c->resume();  // pull
    CHECK(
        c->get_program_counter() == 0);  // wait decrements the program counter
    CHECK(str(z) == "sink_wait");

    auto y = f->resume();  // pull
    CHECK(
        c->get_program_counter() == 0);  // wait decrements the program counter
    CHECK(str(z) == "sink_wait");

    // Inject datum
    auto x = p->resume();
    CHECK(p->get_program_counter() == 3);
    CHECK(str(x) == "notify_sink");
    x = p->resume();
    CHECK(p->get_program_counter() == 5);
    CHECK(str(x) == "noop");
    x = p->resume();
    CHECK(p->get_program_counter() == 0);
    CHECK(str(x) == "yield");

    y = f->resume();  // pull (successful)
    CHECK(f->get_program_counter() == 1);
    CHECK(str(y) == "noop");

    y = f->resume();  // drain
    CHECK(f->get_program_counter() == 3);
    CHECK(str(y) == "notify_source");

    y = f->resume();  // fill
    CHECK(f->get_program_counter() == 7);
    CHECK(str(y) == "notify_sink");

    y = f->resume();  // push
    CHECK(f->get_program_counter() == 9);
    CHECK(str(y) == "noop");

    y = f->resume();  // yield
    CHECK(f->get_program_counter() == 0);
    CHECK(str(y) == "yield");

    // Move datum to last node
    z = c->resume();  // pull (successful)
    CHECK(c->get_program_counter() == 1);
    CHECK(str(z) == "noop");

    z = c->resume();  // drain
    CHECK(c->get_program_counter() == 3);
    CHECK(str(z) == "notify_source");

    z = c->resume();  // yield
    CHECK(c->get_program_counter() == 0);
    CHECK(str(z) == "yield");
  }
}

TEMPLATE_TEST_CASE(
    "Submit Nodes",
    "[duffs]",
    (std::tuple<C2, F2, P2>),
    (std::tuple<C3, F3, P3>)) {
  using C = typename std::tuple_element<0, TestType>::type;
  using F = typename std::tuple_element<1, TestType>::type;
  using P = typename std::tuple_element<2, TestType>::type;

  auto sched = S(1);
  auto p = P([](std::stop_source& stop_source) {
    stop_source.request_stop();
    return 0;
  });
  auto f = F([](const size_t& i) { return i; });
  auto g = F([](const size_t& i) { return i; });
  auto c = C([](const size_t&) {});

  SECTION("Producer and Consumer, submit") {
    connect(p, c);
    Edge(*p, *c);
    sched.submit(p);
    sched.submit(c);
  }

  SECTION("Producer, Function, and Consumer, submit") {
    connect(p, f);
    connect(f, c);
    Edge(*p, *f);
    Edge(*f, *c);
    sched.submit(p);
    sched.submit(f);
    sched.submit(c);
  }

  SECTION("Producer and Consumer, submit") {
    connect(p, c);
    Edge(*p, *c);
    sched.submit(p);
    sched.submit(c);
    sched.sync_wait_all();
  }

  SECTION("Producer, Function, and Consumer, submit") {
    connect(p, f);
    connect(f, c);
    Edge(*p, *f);
    Edge(*f, *c);
    sched.submit(p);
    sched.submit(f);
    sched.submit(c);
    sched.sync_wait_all();
  }

  SECTION("Producer, Function, Function, and Consumer, submit") {
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
    sched.sync_wait_all();
  }
}

TEMPLATE_TEST_CASE(
    "Run Simple Nodes",
    "[duffs]",
    (std::tuple<C2, F2, P2>),
    (std::tuple<C3, F3, P3>)) {
  using C = typename std::tuple_element<0, TestType>::type;
  using F = typename std::tuple_element<1, TestType>::type;
  using P = typename std::tuple_element<2, TestType>::type;

  auto num_threads = GENERATE(1, 2, 3, 4, 5, 8, 17);

  size_t rounds = 5;

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

  SECTION("Producer and Consumer, submit") {
    connect(p, c);
    Edge(*p, *c);
    sched.submit(p);
    sched.submit(c);
    sched.sync_wait_all();
  }

  SECTION("Producer, Function, and Consumer, submit") {
    connect(p, f);
    connect(f, c);
    Edge(*p, *f);
    Edge(*f, *c);
    sched.submit(p);
    sched.submit(f);
    sched.submit(c);
    sched.sync_wait_all();
  }

  SECTION("Producer, Function, Function, and Consumer, submit") {
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
    sched.sync_wait_all();
  }
}

TEMPLATE_TEST_CASE(
    "Run Passing Integers",
    "[duffs]",
    (std::tuple<C2, F2, P2>),
    (std::tuple<C3, F3, P3>)) {
  using C = typename std::tuple_element<0, TestType>::type;
  using F = typename std::tuple_element<1, TestType>::type;
  using P = typename std::tuple_element<2, TestType>::type;

  auto num_threads = GENERATE(1, 2, 3, 4, 5, 8, 17);

  bool debug{false};
  size_t problem_size = 1337;
  size_t rounds = problem_size;

  auto sched = S(num_threads);

  std::vector<size_t> input(rounds);
  std::vector<size_t> output(rounds);

  std::iota(input.begin(), input.end(), 19);
  std::fill(output.begin(), output.end(), 0);
  auto i = input.begin();
  auto j = output.begin();

  if (rounds != 0) {
    CHECK(std::equal(input.begin(), input.end(), output.begin()) == false);
  }

  auto p = P([problem_size, &sched, &i, &input](std::stop_source& stop_source) {
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
                       std::to_string(std::distance(output.begin(), j)) + "\n";

    *j++ = k;
  });

  SECTION("Producer, Function, and Consumer, submit") {
    connect(p, f);
    connect(f, c);
    Edge(*p, *f);
    Edge(*f, *c);
    sched.submit(p);
    sched.submit(f);
    sched.submit(c);
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
