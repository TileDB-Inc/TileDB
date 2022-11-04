/**
 * @file unit_bountiful.cc
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
 * Tests the bountiful scheduler class.
 *
 */

#include "unit_bountiful.h"
#include <atomic>
#include <memory>
#include <vector>
#include "experimental/tiledb/common/dag/edge/edge.h"
#include "experimental/tiledb/common/dag/execution/bountiful.h"
#include "experimental/tiledb/common/dag/nodes/segmented_nodes.h"

using namespace tiledb::common;

TEMPLATE_TEST_CASE(
    "BountifulScheduler: Test soft terminate of sink",
    "[throw_catch]",
    (std::tuple<
        consumer_node<BountifulMover2, size_t>,
        function_node<BountifulMover2, size_t>,
        producer_node<BountifulMover2, size_t>>),
    (std::tuple<
        consumer_node<BountifulMover3, size_t>,
        function_node<BountifulMover3, size_t>,
        producer_node<BountifulMover3, size_t>>)) {

  bool debug{true};

  auto num_threads = 1UL;
  size_t rounds = 5;

  [[maybe_unused]] auto sched = BountifulScheduler<node>();

  using C = typename std::tuple_element<0, TestType>::type;
  using F = typename std::tuple_element<1, TestType>::type;
  using P = typename std::tuple_element<2, TestType>::type;
  auto p = P([rounds](std::stop_source& stop_source) {
    static size_t i {0};
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
    sched.debug();

    p->enable_debug();
    f->enable_debug();
    g->enable_debug();
    c->enable_debug();
  }

  sched.sync_wait_all();
}


#if 0


TEMPLATE_TEST_CASE(
    "BountifulScheduler: Test soft terminate of sink",
    "[throw_catch]",
    (std::tuple<
        consumer_node<BountifulMover2, size_t>,
        function_node<BountifulMover2, size_t>,
        producer_node<BountifulMover2, size_t>>),
    (std::tuple<
        consumer_node<BountifulMover3, size_t>,
        function_node<BountifulMover3, size_t>,
        producer_node<BountifulMover3, size_t>>)) {

  bool debug{false};

  auto num_threads = 1;
  [[maybe_unused]] auto sched = BountifulScheduler<node>(num_threads);

  size_t rounds = 5;
  
  using C = typename std::tuple_element<0, TestType>::type;
  using F = typename std::tuple_element<1, TestType>::type;
  using P = typename std::tuple_element<2, TestType>::type;

  auto p = P([rounds](std::stop_source& stop_source) { 
    static size_t i {0};
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
#endif
