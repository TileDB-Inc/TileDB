/**
* @file unit_tuple_maker.cc
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
 */

#include "unit_tuple_maker.h"

#include "experimental/tiledb/common/dag/nodes/tuple_maker_node.h"
#include "experimental/tiledb/common/dag/execution/duffs.h"
#include "experimental/tiledb/common/dag/edge/edge.h"
#include "experimental/tiledb/common/dag/nodes/detail/segmented/edge_node_ctad.h"
// #include "experimental/tiledb/common/dag/execution/random.h"

using namespace tiledb::common;

using S = tiledb::common::DuffsScheduler<node>;
using C2 = consumer_node<DuffsMover2, std::tuple<size_t,size_t,size_t>>;
using F2 = function_node<DuffsMover2, size_t>;
using T2 = tuple_maker_node<DuffsMover2, size_t>;
using P2 = producer_node<DuffsMover2, size_t>;

using C3 = consumer_node<DuffsMover3, std::tuple<size_t,size_t,size_t>>;
using F3 = function_node<DuffsMover3, size_t>;
using T3 = tuple_maker_node<DuffsMover3, size_t>;
using P3 = producer_node<DuffsMover3, size_t>;

TEMPLATE_TEST_CASE("TupleMaker: Verify construction", "[tuple_maker_node]",
                   (std::tuple<C2, F2, T2, P2>),
                   (std::tuple<C3, F3, T3, P3>)) {
  using C = typename std::tuple_element<0, TestType>::type;
  using F = typename std::tuple_element<1, TestType>::type;
  using T = typename std::tuple_element<2, TestType>::type;
  using P = typename std::tuple_element<3, TestType>::type;

  auto c = C {[](const std::tuple<size_t, size_t, size_t>&){}};
  auto t = T {};
  auto p = P([](std::stop_source& stop_source) {
    static size_t i = 6;
    if (i == 0) {
      stop_source.request_stop();
      return 0UL;
    }
    return --i;
  });

  CHECK(true);
}

TEMPLATE_TEST_CASE("TupleMaker: Verify connected nodes", "[tuple_maker_node]",
                   (std::tuple<C2, F2, T2, P2>),
                   (std::tuple<C3, F3, T3, P3>)) {
  using C = typename std::tuple_element<0, TestType>::type;
  using F = typename std::tuple_element<1, TestType>::type;
  using T = typename std::tuple_element<2, TestType>::type;
  using P = typename std::tuple_element<3, TestType>::type;

  std::vector<std::tuple<size_t, size_t, size_t>> results;
  auto sched = S(1);
  auto c = C {[&results](const std::tuple<size_t, size_t, size_t>& t){
    results.push_back(t);
  }};
  auto t = T {};
  auto p = P([](std::stop_source& stop_source) {
    static size_t i = 6;
    if (i == 0) {
      stop_source.request_stop();
      return 0UL;
    }
    return --i;
  });

  connect(p, t);
  connect(t, c);
  Edge(p, *t);
  Edge(*t, c);

  SECTION("Just submit") {
    // @todo Have to figure out how to create deduction guides without combinatorial explosion
    sched.submit(p);
    sched.submit(t);
    sched.submit(c);
  }

  SECTION("Submit and wait") {
    sched.submit(p);
    sched.submit(t);
    sched.submit(c);
    sched.sync_wait_all();
    auto c0 = results[0];
    CHECK(std::get<0>(c0) == 5);
    CHECK(std::get<1>(c0) == 4);
    CHECK(std::get<2>(c0) == 3);
    auto c1 = results[1];
    CHECK(std::get<0>(c1) == 2);
    CHECK(std::get<1>(c1) == 1);
    CHECK(std::get<2>(c1) == 0);
  }
}
