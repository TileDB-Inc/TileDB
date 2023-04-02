/**
 * @file unit_resumable_nodes.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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

#include "unit_resumable_nodes.h"
#include <future>
#include <type_traits>
#include "experimental/tiledb/common/dag/edge/edge.h"
#include "experimental/tiledb/common/dag/execution/duffs.h"
#include "experimental/tiledb/common/dag/execution/random.h"
#include "experimental/tiledb/common/dag/nodes/generators.h"
#include "experimental/tiledb/common/dag/nodes/resumable_nodes.h"
#include "experimental/tiledb/common/dag/nodes/terminals.h"
#include "experimental/tiledb/common/dag/state_machine/test/types.h"
#include "experimental/tiledb/common/dag/utility/print_types.h"

#include "experimental/tiledb/common/dag/nodes/detail/resumable/broadcast.h"
#include "experimental/tiledb/common/dag/nodes/detail/resumable/mimo.h"
#include "experimental/tiledb/common/dag/nodes/detail/resumable/proxy.h"
#include "experimental/tiledb/common/dag/nodes/detail/resumable/reduce.h"

using namespace tiledb::common;

using R = tiledb::common::RandomScheduler<node>;
using S = tiledb::common::DuffsScheduler<node>;

using R2_1_1 =
    mimo_node<DuffsMover2, std::tuple<size_t>, DuffsMover2, std::tuple<size_t>>;
using R2_3_1 = mimo_node<
    DuffsMover2,
    std::tuple<size_t, int, double>,
    DuffsMover2,
    std::tuple<size_t>>;
using R2_1_3 = mimo_node<
    DuffsMover2,
    std::tuple<size_t>,
    DuffsMover2,
    std::tuple<size_t, double, int>>;
using R2_3_3 = mimo_node<
    DuffsMover2,
    std::tuple<size_t, int, double>,
    DuffsMover2,
    std::tuple<size_t, double, int>>;

using R3_1_1 =
    mimo_node<DuffsMover3, std::tuple<size_t>, DuffsMover3, std::tuple<size_t>>;
using R3_3_1 = mimo_node<
    DuffsMover3,
    std::tuple<size_t, int, double>,
    DuffsMover3,
    std::tuple<size_t>>;
using R3_1_3 = mimo_node<
    DuffsMover3,
    std::tuple<size_t>,
    DuffsMover3,
    std::tuple<size_t, double, int>>;
using R3_3_3 = mimo_node<
    DuffsMover3,
    std::tuple<size_t, int, double>,
    DuffsMover3,
    std::tuple<size_t, double, int>>;

using reduce_1_0 =
    reducer_node<DuffsMover3, std::tuple<size_t>, EmptyMover, std::tuple<>>;

using reduce_1_1 = reducer_node<
    DuffsMover3,
    std::tuple<size_t>,
    DuffsMover3,
    std::tuple<size_t>>;

using reduce_3_0 = reducer_node<
    DuffsMover3,
    std::tuple<size_t, size_t, size_t>,
    EmptyMover,
    std::tuple<>>;
using reduce_3_1 = reducer_node<
    DuffsMover3,
    std::tuple<size_t, size_t, size_t>,
    DuffsMover3,
    std::tuple<size_t>>;
using reduce_3_3 = reducer_node<
    DuffsMover3,
    std::tuple<size_t, size_t, size_t>,
    DuffsMover3,
    std::tuple<size_t, size_t, size_t>>;

using broadcast_0_1 = broadcast_node<
    1,
    EmptyMover,
    std::tuple<>,
    DuffsMover3,
    std::tuple<size_t>>;

using broadcast_1_1 = broadcast_node<
    1,
    DuffsMover3,
    std::tuple<size_t>,
    DuffsMover3,
    std::tuple<size_t>>;

using broadcast_0_3 = broadcast_node<
    3,
    EmptyMover,
    std::tuple<>,
    DuffsMover3,
    std::tuple<size_t>>;

using broadcast_1_3 = broadcast_node<
    3,
    DuffsMover3,
    std::tuple<size_t>,
    DuffsMover3,
    std::tuple<size_t>>;

using producer_1 = producer_mimo<DuffsMover3, std::tuple<size_t>>;

using producer_3 =
    producer_mimo<DuffsMover3, std::tuple<size_t, size_t, size_t>>;

using consumer_1 = consumer_mimo<DuffsMover3, std::tuple<size_t>>;

using consumer_3 =
    consumer_mimo<DuffsMover3, std::tuple<size_t, size_t, size_t>>;


using async_producer_1 = producer_mimo<AsyncMover3, std::tuple<size_t>>;

using async_producer_3 =
    producer_mimo<AsyncMover3, std::tuple<size_t, size_t, size_t>>;

using async_consumer_1 = consumer_mimo<AsyncMover3, std::tuple<size_t>>;

using async_consumer_3 =
    consumer_mimo<AsyncMover3, std::tuple<size_t, size_t, size_t>>;


namespace tiledb::common {
// Tentative deduction guide
template <class... R, class... Args>
mimo_node(std::function<std::tuple<R...>(const std::tuple<Args...>&)>&&)
    -> mimo_node<
        DuffsMover3,
        std::tuple<std::remove_cv_t<std::remove_reference_t<Args>>...>,
        DuffsMover3,
        std::tuple<R>...>;
}  // namespace tiledb::common

TEST_CASE("ResumableNode: Verify Construction", "[resumable_node]") {
  SECTION("Test Construction") {
    R2_1_1 b2_1_1{[](std::tuple<size_t>) { return std::make_tuple(0UL); }};
    R2_1_3 b2_1_3{
        [](std::tuple<size_t>) { return std::make_tuple(0UL, 0.0, 0); }};
    R2_3_1 b2_3_1{
        [](std::tuple<size_t, int, double>) { return std::make_tuple(0UL); }};
    R2_3_3 b2_3_3{[](std::tuple<size_t, int, double>) {
      return std::make_tuple(0UL, 0.0, 0);
    }};

    R3_1_1 b3_1_1{[](std::tuple<size_t>) { return std::make_tuple(0UL); }};
    R3_1_3 b3_1_3{
        [](std::tuple<size_t>) { return std::make_tuple(0UL, 0.0, 0); }};
    R3_3_1 b3_3_1{
        [](std::tuple<size_t, int, double>) { return std::make_tuple(0UL); }};
    R3_3_3 b3_3_3{[](std::tuple<size_t, int, double>) {
      return std::make_tuple(0UL, 0.0, 0);
    }};

    // Deduction guide not working
    // mimo_node c_1_1 { [](std::tuple<size_t>) { return std::make_tuple(0UL); }
    // };
  }
}

TEST_CASE("Resumable Node: Construct reduce node", "[reducer_node]") {
  SECTION("Test Construction") {
    reduce_3_1 b3_3_1{[](const std::tuple<size_t, size_t, size_t>& a) {
      return std::make_tuple(std::get<0>(a) + std::get<1>(a) + std::get<2>(a));
    }};
    CHECK(b3_3_1->num_inputs() == 3);
    CHECK(b3_3_1->num_outputs() == 1);

    // Error: reduction goes M -> 1
    // reduce_3_3 b3_3_3{[](const std::tuple<size_t, size_t, size_t>& a) {
    //   return std::make_tuple(std::get<0>(a), std::get<1>(a), std::get<2>(a));
    // }};
  }
  SECTION("Test Construction") {
    reduce_3_0 b3_3_0{[](const std::tuple<size_t, size_t, size_t>&) {}};
    CHECK(b3_3_0->num_inputs() == 3);
    CHECK(b3_3_0->num_outputs() == 0);

    // Error: reduction goes M -> 1
    // reduce_3_3 b3_3_3{[](const std::tuple<size_t, size_t, size_t>& a) {
    //   return std::make_tuple(std::get<0>(a), std::get<1>(a), std::get<2>(a));
    // }};
  }
}

TEST_CASE("Resumable Node: Construct broadcast node", "[broadcast_node]") {
  SECTION("Test Construction") {
    broadcast_1_3 b3_1_3{[](const std::tuple<size_t>& a) {
      return std::make_tuple(5 * std::get<0>(a));
    }};
    CHECK(b3_1_3->num_inputs() == 1);
    CHECK(b3_1_3->num_outputs() == 3);
  }
  SECTION("Test Construction") {
    broadcast_0_3 b3_0_3{
        [](std::stop_source) { return std::make_tuple(42UL); }};
    CHECK(b3_0_3->num_inputs() == 0);
    CHECK(b3_0_3->num_outputs() == 3);
  }
}

TEST_CASE(
    "Resumable Node: Connect broadcast node to reduce node",
    "[broadcast_node]") {
  broadcast_1_3 b3_1_3{[](const std::tuple<size_t>& a) {
    return std::make_tuple(5 * std::get<0>(a));
  }};

  reduce_3_1 b3_3_1{[](const std::tuple<size_t, size_t, size_t>& a) {
    return std::make_tuple(std::get<0>(a) + std::get<1>(a) + std::get<2>(a));
  }};

  broadcast_0_3 b3_0_3{[](std::stop_source) { return std::make_tuple(7); }};

  reduce_3_0 b3_3_0{[](const std::tuple<size_t, size_t, size_t>&) {}};

  SECTION("Construct broadcast and reduce nodes") {
    CHECK(b3_1_3->num_inputs() == 1);
    CHECK(b3_1_3->num_outputs() == 3);
    CHECK(b3_3_1->num_inputs() == 3);
    CHECK(b3_3_1->num_outputs() == 1);
    CHECK(b3_0_3->num_inputs() == 0);
    CHECK(b3_0_3->num_outputs() == 3);
    CHECK(b3_3_0->num_inputs() == 3);
    CHECK(b3_3_0->num_outputs() == 0);
  }

  SECTION("Test One Connection") {
    Edge e0{b3_1_3->out_port<0>(), b3_3_1->in_port<0>()};
    Edge e1{b3_3_1->out_port<0>(), b3_1_3->in_port<0>()};
  }

  SECTION("Test Three Connections") {
    Edge e0{b3_1_3->out_port<0>(), b3_3_1->in_port<0>()};
    Edge e1{b3_1_3->out_port<1>(), b3_3_1->in_port<1>()};
    Edge e2{b3_1_3->out_port<2>(), b3_3_1->in_port<2>()};
  }

  SECTION("Test Three Connections") {
    Edge e0{b3_1_3->out_port<0>(), b3_3_0->in_port<0>()};
    Edge e1{b3_1_3->out_port<1>(), b3_3_0->in_port<1>()};
    Edge e2{b3_1_3->out_port<2>(), b3_3_0->in_port<2>()};
  }

  SECTION("Test Three Connections") {
    Edge e0{b3_0_3->out_port<0>(), b3_3_1->in_port<0>()};
    Edge e1{b3_0_3->out_port<1>(), b3_3_1->in_port<1>()};
    Edge e2{b3_0_3->out_port<2>(), b3_3_1->in_port<2>()};
  }

  SECTION("Test Three Connections") {
    Edge e0{b3_0_3->out_port<0>(), b3_3_0->in_port<0>()};
    Edge e1{b3_0_3->out_port<1>(), b3_3_0->in_port<1>()};
    Edge e2{b3_0_3->out_port<2>(), b3_3_0->in_port<2>()};
  }

  SECTION("Test Three Connections, vary order") {
    Edge e0{b3_1_3->out_port<0>(), b3_3_1->in_port<1>()};
    Edge e1{b3_1_3->out_port<1>(), b3_3_1->in_port<2>()};
    Edge e2{b3_1_3->out_port<2>(), b3_3_1->in_port<0>()};
  }
}

auto dummy_source(std::stop_source) {
  return std::make_tuple(42UL);
}

auto dummy_function(const std::tuple<size_t>& i) {
  return i;
}

auto dummy_sink(const std::tuple<size_t>&) {
}

class dummy_source_class {
 public:
  auto operator()(std::stop_source&) {
    return std::tuple<size_t>{42UL};
  }
};

class dummy_function_class {
 public:
  size_t operator()(const size_t&) {
    return size_t{};
  }
  auto operator()(const std::tuple<size_t>& in) {
    return in;
  }
};

class dummy_sink_class {
 public:
  void operator()(size_t) {
  }
  void operator()(const std::tuple<size_t>&) {
  }
};

auto dummy_bind_source = std::bind(
    [](std::stop_source&, double) { return std::tuple<size_t>{42UL}; },
    std::placeholders::_1,
    1.0);
auto dummy_bind_sink = std::bind(
    [](size_t, const std::tuple<size_t>&, const int&) {},
    42UL,
    std::placeholders::_1,
    42);
auto dummy_bind_function = std::bind(
    [](double, float, const std::tuple<size_t>& in) { return in; },
    1.0,
    1.0f,
    std::placeholders::_1);

TEST_CASE(
    "resumable_node: Construct different flavors of resumable_node",
    "[resumable_node]") {
  broadcast_0_1{dummy_source};
  broadcast_1_1{dummy_function};
  R3_1_1{dummy_function};
  reduce_1_0{dummy_sink};
  reduce_1_1{dummy_function};

  broadcast_0_1{dummy_source_class{}};
  broadcast_1_1{dummy_function_class{}};
  R3_1_1{dummy_function_class{}};
  reduce_1_0{dummy_sink_class{}};
  reduce_1_1{dummy_function_class{}};

  broadcast_0_1{dummy_bind_source};
  broadcast_1_1{dummy_bind_function};
  R3_1_1{dummy_bind_function};
  reduce_1_0{dummy_bind_sink};
  reduce_1_1{dummy_bind_function};
}

TEST_CASE(
    "mimo_node: Pass values with void-created Producer and Consumer "
    "[segmented_mimo]") {
  [[maybe_unused]] double ext1{0.0};
  [[maybe_unused]] size_t ext2{0};

  broadcast_1_3 b3_1_3{[](const std::tuple<size_t>& a) {
    return std::make_tuple(5 * std::get<0>(a));
  }};

  reduce_3_1 b3_3_1{[](const std::tuple<size_t, size_t, size_t>& a) {
    return std::make_tuple(std::get<0>(a) + std::get<1>(a) + std::get<2>(a));
  }};

  broadcast_0_3 b3_0_3{[](std::stop_source) { return std::make_tuple(7); }};

  reduce_3_0 b3_3_0{[&ext2](const std::tuple<size_t, size_t, size_t>& a) {
    ext2 += std::get<0>(a) + std::get<1>(a) + std::get<2>(a);
  }};

  producer_1 p_1{[](std::stop_source) { return std::make_tuple(11); }};
  producer_1 q_1{[](std::stop_source) { return std::make_tuple(13); }};
  producer_1 r_1{[](std::stop_source) { return std::make_tuple(17); }};

  consumer_1 c_1{[&ext2](std::tuple<size_t> a) { ext2 += std::get<0>(a); }};
  consumer_1 d_1{[&ext2](std::tuple<size_t> a) { ext2 += std::get<0>(a); }};
  consumer_1 e_1{[&ext2](std::tuple<size_t> a) { ext2 += std::get<0>(a); }};

  SECTION("Connect to producer and consumer") {
    Edge e0{p_1->out_port<0>(), b3_1_3->in_port<0>()};
    Edge e1{b3_3_1->out_port<0>(), c_1->in_port<0>()};
  }

  SECTION("Connect to producer and consumer") {
    Edge e0{p_1->out_port<0>(), b3_3_1->in_port<0>()};
    Edge e1{q_1->out_port<0>(), b3_3_1->in_port<1>()};
    Edge e2{r_1->out_port<0>(), b3_3_1->in_port<2>()};
  }

  SECTION("Connect to producer and consumer") {
    Edge e0{b3_1_3->out_port<0>(), c_1->in_port<0>()};
    Edge e1{b3_1_3->out_port<1>(), d_1->in_port<0>()};
    Edge e2{b3_1_3->out_port<2>(), e_1->in_port<0>()};
  }

  SECTION("Flow some data broadcast to consumers") {
    Edge e0{b3_0_3->out_port<0>(), c_1->in_port<0>()};
    Edge e1{b3_0_3->out_port<1>(), d_1->in_port<0>()};
    Edge e2{b3_0_3->out_port<2>(), e_1->in_port<0>()};

    b3_0_3->resume();

    c_1->resume();
    CHECK(ext2 == 0);
    c_1->resume();
    CHECK(ext2 == 0);
    c_1->resume();
    CHECK(ext2 == 7);

    d_1->resume();
    CHECK(ext2 == 7);
    d_1->resume();
    CHECK(ext2 == 7);
    d_1->resume();
    CHECK(ext2 == 14);

    e_1->resume();
    CHECK(ext2 == 14);
    e_1->resume();
    CHECK(ext2 == 14);
    e_1->resume();
    CHECK(ext2 == 21);
  }

  SECTION("Flow some data producers to reducers") {
    Edge e0{p_1->out_port<0>(), b3_3_0->in_port<0>()};
    Edge e1{q_1->out_port<0>(), b3_3_0->in_port<1>()};
    Edge e2{r_1->out_port<0>(), b3_3_0->in_port<2>()};

    // b3_3_0->resume();
    // CHECK(ext2 == 0);
    // Hmm.  Should be able to do this without error (maybe w/random?)
    // b3_3_0->resume();
    // CHECK(ext2 == 0);
    p_1->resume();
    q_1->resume();
    r_1->resume();
    CHECK(ext2 == 0);

    b3_3_0->resume();
    CHECK(ext2 == 0);

    b3_3_0->resume();
    CHECK(ext2 == 0);

    b3_3_0->resume();
    CHECK(ext2 == 41);
  }

  SECTION("Flow some data from producers to reducer") {
    Edge e0{p_1->out_port<0>(), b3_3_0->in_port<0>()};
    Edge e1{q_1->out_port<0>(), b3_3_0->in_port<1>()};
    Edge e2{r_1->out_port<0>(), b3_3_0->in_port<2>()};

    [[maybe_unused]] auto aa = p_1->resume();
    q_1->resume();
    r_1->resume();
    CHECK(ext2 == 0);

    [[maybe_unused]] auto bb = p_1->resume();
    q_1->resume();
    r_1->resume();

    [[maybe_unused]] auto cc = p_1->resume();
    q_1->resume();
    r_1->resume();

    [[maybe_unused]] auto dd = p_1->resume();
    q_1->resume();
    r_1->resume();

    CHECK(ext2 == 0);

    [[maybe_unused]] auto mm = b3_3_0->resume();
    CHECK(ext2 == 0);

    [[maybe_unused]] auto nn = b3_3_0->resume();
    CHECK(ext2 == 0);

    [[maybe_unused]] auto oo = b3_3_0->resume();
    CHECK(ext2 == 41);

    [[maybe_unused]] auto pp = b3_3_0->resume();
    CHECK(ext2 == 41);

    [[maybe_unused]] auto qq = b3_3_0->resume();
    CHECK(ext2 == 41);

    [[maybe_unused]] auto rr = b3_3_0->resume();
    CHECK(ext2 == 82);
  }

  SECTION("Flow some data from producers to reducer") {
    Edge e0{p_1->out_port<0>(), b3_3_0->in_port<0>()};
    Edge e1{q_1->out_port<0>(), b3_3_0->in_port<1>()};
    Edge e2{r_1->out_port<0>(), b3_3_0->in_port<2>()};

    [[maybe_unused]] auto aa = p_1->resume();
    q_1->resume();
    r_1->resume();
    CHECK(ext2 == 0);

    [[maybe_unused]] auto bb = p_1->resume();
    q_1->resume();
    r_1->resume();

    [[maybe_unused]] auto cc = p_1->resume();
    q_1->resume();
    r_1->resume();

    CHECK(ext2 == 0);

    [[maybe_unused]] auto mm = b3_3_0->resume();
    CHECK(ext2 == 0);

    [[maybe_unused]] auto nn = b3_3_0->resume();
    CHECK(ext2 == 0);

    [[maybe_unused]] auto dd = p_1->resume();
    q_1->resume();
    r_1->resume();

    CHECK(ext2 == 0);

    [[maybe_unused]] auto oo = b3_3_0->resume();
    CHECK(ext2 == 41);

    [[maybe_unused]] auto pp = b3_3_0->resume();
    CHECK(ext2 == 41);

    [[maybe_unused]] auto qq = b3_3_0->resume();
    CHECK(ext2 == 41);

    [[maybe_unused]] auto rr = b3_3_0->resume();
    CHECK(ext2 == 82);
  }
}

template <class Node>
auto run_for(Node& node, size_t rounds) {
  return [&node, rounds]() {
    size_t N = rounds;
    while (N) {
      auto code = node->resume();
      // std::cout << "code: " << str(code) << std::endl;
      if (code == SchedulerAction::yield) {
        // std::cout << "decrementing " << N << std::endl;
        --N;
      }
    }
  };
}

TEST_CASE("Resumable nodes run some", "[resumable]") {
  size_t ext2{0};
  size_t ext1{0};
  async_producer_3 p_3{
    [&ext1](std::stop_source) -> std::tuple<size_t, size_t, size_t> {
        auto a = ext1++;
        auto b = ext1++;
        auto c = ext1++;
        // std::cout << "producer_3: " << ext1 << std::endl;
      return { a, b, c };
    },
  };
  async_consumer_3 c_3 {
    [&ext2](const std::tuple<size_t, size_t, size_t>& t) {
      ext2 = std::get<0>(t) + std::get<1>(t) + std::get<2>(t);
      // std::cout << "consume_3: " << std::get<0>(t) << " " << std::get<1>(t) << " " << std::get<2>(t) << " = " << ext2 << std::endl;
    },
  };
  Edge e0{p_3->out_port<0>(), c_3->in_port<0>()};
  Edge e1{p_3->out_port<1>(), c_3->in_port<1>()};
  Edge e2{p_3->out_port<2>(), c_3->in_port<2>()};

  size_t rounds = 10;
  auto fun_a1 = run_for(p_3, rounds);
  auto fun_a2 = run_for(c_3, rounds);

  auto fut_a1 = std::async(std::launch::async, fun_a1);
  auto fut_a2 = std::async(std::launch::async, fun_a2);
  fut_a2.wait();
  fut_a1.wait();
  CHECK(ext2 == 3*(rounds-1) + 3* (rounds-1) + 1 + 3*(rounds-1)+2);
}

TEST_CASE("Resumable nodes run more", "[resumable]") {

  std::atomic<size_t> ext2{0};

  using producer_1 = producer_mimo<AsyncMover3, std::tuple<size_t>>;
  using consumer_1 = consumer_mimo<AsyncMover3, std::tuple<size_t>>;

  producer_1 p_1{[](std::stop_source) { return std::make_tuple(11); }};
  producer_1 q_1{[](std::stop_source) { return std::make_tuple(13); }};
  producer_1 r_1{[](std::stop_source) { return std::make_tuple(17); }};

  consumer_1 c_1{[&ext2](std::tuple<size_t> a) { ext2 += std::get<0>(a); }};
  consumer_1 d_1{[&ext2](std::tuple<size_t> a) { ext2 += std::get<0>(a); }};
  consumer_1 e_1{[&ext2](std::tuple<size_t> a) { ext2 += std::get<0>(a); }};

  Edge e0{p_1->out_port<0>(), c_1->in_port<0>()};
  Edge e1{q_1->out_port<0>(), d_1->in_port<0>()};
  Edge e2{r_1->out_port<0>(), e_1->in_port<0>()};

  size_t rounds = 11;
  size_t num_tests = 17;

  SECTION("single producer, single consumer") {
    for (size_t j = 0; j < num_tests; ++j) {
      ext2 = 0;

      auto fun_a1 = run_for(p_1, rounds);
      auto fun_a2 = run_for(c_1, rounds);
      auto fun_b1 = run_for(q_1, rounds);
      auto fun_b2 = run_for(d_1, rounds);
      auto fun_c1 = run_for(r_1, rounds);
      auto fun_c2 = run_for(e_1, rounds);

      auto fut_a1 = std::async(std::launch::async, fun_a1);
      auto fut_a2 = std::async(std::launch::async, fun_a2);
      auto fut_b1 = std::async(std::launch::async, fun_b1);
      auto fut_b2 = std::async(std::launch::async, fun_b2);
      auto fut_c1 = std::async(std::launch::async, fun_c1);
      auto fut_c2 = std::async(std::launch::async, fun_c2);

      fut_c2.wait();
      fut_c1.wait();
      fut_b2.wait();
      fut_b1.wait();
      fut_a2.wait();
      fut_a1.wait();
      CHECK(ext2 == rounds * (11 + 13 + 17));
    }
  }

  using reduce_3_0 = reducer_node<
      AsyncMover3,
      std::tuple<size_t, size_t, size_t>,
      EmptyMover,
      std::tuple<>>;

  using broadcast_0_3 = broadcast_node<
      3,
      EmptyMover,
      std::tuple<>,
      AsyncMover3,
      std::tuple<size_t>>;

  broadcast_0_3 b3_0_3{[](std::stop_source) { return std::make_tuple(7); }};
  reduce_3_0 b3_3_0{[](const std::tuple<size_t, size_t, size_t>&) {}};
}

TEST_CASE("Resumable nodes run some more", "[resumable]") {

  std::atomic<size_t> ext2{0};
  size_t rounds = 11;
  size_t num_tests = 17;

  using producer_1 = producer_mimo<AsyncMover3, std::tuple<size_t>>;
  using consumer_1 = consumer_mimo<AsyncMover3, std::tuple<size_t>>;

  producer_1 p_1{[](std::stop_source) { return std::make_tuple(11); }};
  producer_1 q_1{[](std::stop_source) { return std::make_tuple(13); }};
  producer_1 r_1{[](std::stop_source) { return std::make_tuple(17); }};

  consumer_1 c_1{[&ext2](std::tuple<size_t> a) { ext2 += std::get<0>(a); }};
  consumer_1 d_1{[&ext2](std::tuple<size_t> a) { ext2 += std::get<0>(a); }};
  consumer_1 e_1{[&ext2](std::tuple<size_t> a) { ext2 += std::get<0>(a); }};

  using reduce_3_0 = reducer_node<
      AsyncMover3,
      std::tuple<size_t, size_t, size_t>,
      EmptyMover,
      std::tuple<>>;

  using broadcast_0_3 = broadcast_node<
      3,
      EmptyMover,
      std::tuple<>,
      AsyncMover3,
      std::tuple<size_t>>;

  broadcast_0_3 b3_0_3{[](std::stop_source) { return std::make_tuple(7); }};
  reduce_3_0 b3_3_0{[&ext2](const std::tuple<size_t, size_t, size_t>& a) {
    ext2 += std::get<0>(a) + std::get<1>(a) + std::get<2>(a);
  }};
  broadcast_0_3 c3_0_3{[](std::stop_source) { return std::make_tuple(9); }};
  reduce_3_0 c3_3_0{[&ext2](const std::tuple<size_t, size_t, size_t>& a) {
    ext2 += std::get<0>(a) + std::get<1>(a) + std::get<2>(a);
  }};

  Edge e0{b3_0_3->out_port<0>(), c_1->in_port<0>()};
  Edge e1{b3_0_3->out_port<1>(), d_1->in_port<0>()};
  Edge e2{b3_0_3->out_port<2>(), e_1->in_port<0>()};

  Edge f0{p_1->out_port<0>(), b3_3_0->in_port<0>()};
  Edge f1{q_1->out_port<0>(), b3_3_0->in_port<1>()};
  Edge f2{r_1->out_port<0>(), b3_3_0->in_port<2>()};

  Edge g0{c3_0_3->out_port<0>(), c3_3_0->in_port<0>()};
  Edge g1{c3_0_3->out_port<1>(), c3_3_0->in_port<1>()};
  Edge g2{c3_0_3->out_port<2>(), c3_3_0->in_port<2>()};


  SECTION("triple producer, three consumers") {
    for (size_t j = 0; j < num_tests; ++j) {
      ext2 = 0;

      auto fun_a1 = run_for(b3_0_3, rounds);
      auto fun_c1 = run_for(c_1, rounds);
      auto fun_d1 = run_for(d_1, rounds);
      auto fun_e1 = run_for(e_1, rounds);

      auto fut_a1 = std::async(std::launch::async, fun_a1);
      auto fut_c1 = std::async(std::launch::async, fun_c1);
      auto fut_d1 = std::async(std::launch::async, fun_d1);
      auto fut_e1 = std::async(std::launch::async, fun_e1);

      fut_a1.wait();
      fut_c1.wait();
      fut_d1.wait();
      fut_e1.wait();

      CHECK(ext2 == rounds * (7 + 7 + 7));
    }
  }

  SECTION("three producers, triple consumer") {
    for (size_t j = 0; j < num_tests; ++j) {
      ext2 = 0;

      auto fun_a1 = run_for(b3_3_0, rounds);
      auto fun_c1 = run_for(p_1, rounds);
      auto fun_d1 = run_for(q_1, rounds);
      auto fun_e1 = run_for(r_1, rounds);

      auto fut_a1 = std::async(std::launch::async, fun_a1);
      auto fut_c1 = std::async(std::launch::async, fun_c1);
      auto fut_d1 = std::async(std::launch::async, fun_d1);
      auto fut_e1 = std::async(std::launch::async, fun_e1);

      fut_a1.wait();
      fut_c1.wait();
      fut_d1.wait();
      fut_e1.wait();

      CHECK(ext2 == rounds * (11+ 13 + 17));
    }
  }
  SECTION("triple producer, triple consumer") {
    for (size_t j = 0; j < num_tests; ++j) {
      ext2 = 0;

      auto fun_a1 = run_for(c3_0_3, rounds);
      auto fun_a2 = run_for(c3_3_0, rounds);

      auto fut_a1 = std::async(std::launch::async, fun_a1);
      auto fut_a2 = std::async(std::launch::async, fun_a2);

      fut_a1.wait();
      fut_a2.wait();

      CHECK(ext2 == rounds * (9 + 9 + 9));
    }
  }
}


#if 0

using S = tiledb::common::DuffsScheduler<node>;
using C2 = consumer_node<DuffsMover2, std::tuple<size_t, size_t, size_t>>;
using F2 = function_node<DuffsMover2, size_t>;
using T2 = tuple_maker_node<
    DuffsMover2,
    size_t,
    DuffsMover2,
    std::tuple<size_t, size_t, size_t>>;
using P2 = producer_node<DuffsMover2, size_t>;

using C3 = consumer_node<DuffsMover3, std::tuple<size_t, size_t, size_t>>;
using F3 = function_node<DuffsMover3, size_t>;
using T3 = tuple_maker_node<
    DuffsMover3,
    size_t,
    DuffsMover3,
    std::tuple<size_t, size_t, size_t>>;
using P3 = producer_node<DuffsMover3, size_t>;

/**
 * Verify various API approaches
 */
TEMPLATE_TEST_CASE(
    "ResumableNodes: Verify various API approaches",
    "[resumable_nodes]",
    (std::tuple<
        consumer_node<AsyncMover2, size_t>,
        producer_node<AsyncMover2, size_t>>),
    (std::tuple<
        consumer_node<AsyncMover3, size_t>,
        producer_node<AsyncMover3, size_t>>)) {
  using C = typename std::tuple_element<0, TestType>::type;
  using P = typename std::tuple_element<1, TestType>::type;

  SECTION("Test Construction") {
    P a;
    P b([](std::stop_source&) { return 0; });
    C c([](size_t) {});
  }
  SECTION("Test Connection") {
    P b([](std::stop_source&) { return 0; });
    C c([](size_t) {});
    Edge g{*b, *c};
  }
  SECTION("Enable if fail") {
    // These will fail, with good diagnostics.  This should be commented out
    // from time to time and tested by hand that we get the right error
    // messages.
    //   producer_node<AsyncMover3, size_t> bb{0UL};
    //   consumer_node<AsyncMover3, size_t> cc{-1.1};
    //   Edge g{bb, cc};
  }
}

bool two_nodes(node_base&, node_base&) {
  return true;
}

bool two_nodes(const node&, const node&) {
  return true;
}

TEMPLATE_TEST_CASE(
    "Tasks: Extensive tests of tasks with nodes",
    "[tasks]",
    (std::tuple<
        consumer_node<AsyncMover2, size_t>,
        function_node<AsyncMover2, size_t>,
        producer_node<AsyncMover2, size_t>>),
    (std::tuple<
        consumer_node<AsyncMover3, size_t>,
        function_node<AsyncMover3, size_t>,
        producer_node<AsyncMover3, size_t>>)) {
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

  SECTION("Check specified and deduced are same types") {
  }

  SECTION("Check polymorphism to node&") {
    CHECK(two_nodes(pro_node_impl, con_node_impl));
    CHECK(two_nodes(pro_node_impl, fun_node_impl));
    CHECK(two_nodes(fun_node_impl, con_node_impl));

    // No conversion from producer_node to node
    CHECK(two_nodes(pro_node, con_node));
    CHECK(two_nodes(pro_node, fun_node));
    CHECK(two_nodes(fun_node, con_node));
  }

  SECTION("Check some copying node (godbolt)") {
    auto shared_pro = node{pro_node};
    auto shared_fun = node{fun_node};
    auto shared_con = node{con_node};

    auto shared_nil = node{};
    shared_nil = shared_pro;
    CHECK(shared_nil == shared_pro);
  }
}

/**
 * Some dummy functions and classes to test node constructors
 * with.
 */
size_t dummy_source(std::stop_source&) {
  return size_t{};
}

size_t dummy_function(size_t) {
  return size_t{};
}

void dummy_sink(size_t) {
}

size_t dummy_const_function(const size_t&) {
  return size_t{};
}

void dummy_const_sink(const size_t&) {
}

class dummy_source_class {
 public:
  size_t operator()(std::stop_source&) {
    return size_t{};
  }
};

class dummy_function_class {
 public:
  size_t operator()(const size_t&) {
    return size_t{};
  }
};

class dummy_sink_class {
 public:
  void operator()(size_t) {
  }
};

size_t dummy_bind_source(double) {
  return size_t{};
}

size_t dummy_bind_function(double, float, size_t) {
  return size_t{};
}

void dummy_bind_sink(size_t, float, const int&) {
}

/**
 * Some dummy function template and class templates to test node constructors
 * with.
 */
template <class Block = size_t>
size_t dummy_source_t(std::stop_source&) {
  return Block{};
}

template <class InBlock = size_t, class OutBlock = InBlock>
OutBlock dummy_function_t(InBlock) {
  return OutBlock{};
}

template <class Block = size_t>
void dummy_sink_t(const Block&) {
}

template <class Block = size_t>
class dummy_source_class_t {
 public:
  Block operator()() {
    return Block{};
  }
};

template <class InBlock = size_t, class OutBlock = InBlock>
class dummy_function_class_t {
 public:
  OutBlock operator()(const InBlock&) {
    return OutBlock{};
  }
};

template <class Block = size_t>
class dummy_sink_class_t {
 public:
  void operator()(Block) {
  }
};

template <class Block = size_t>
Block dummy_bind_source_t(double) {
  return Block{};
}

template <class InBlock = size_t, class OutBlock = InBlock>
OutBlock dummy_bind_function_t(double, float, InBlock) {
  return OutBlock{};
}

template <class Block = size_t>
void dummy_bind_sink_t(Block, float, const int&) {
}

/**
 * Verify initializing producer_node and consumer_node with function, lambda,
 * in-line lambda, function object, bind, and rvalue bind.
 */
TEMPLATE_TEST_CASE(
    "ResumableNodes: Verify numerous API approaches, with edges",
    "[resumable_nodes]",
    (std::tuple<
        consumer_node<AsyncMover2, size_t>,
        producer_node<AsyncMover2, size_t>>),
    (std::tuple<
        consumer_node<AsyncMover3, size_t>,
        producer_node<AsyncMover3, size_t>>)) {
  using C = typename std::tuple_element<0, TestType>::type;
  using P = typename std::tuple_element<1, TestType>::type;

  SECTION("function") {
    P b{dummy_source};
    C c{dummy_sink};
    Edge g{*b, *c};
  }

  SECTION("function with const reference") {
    P b{dummy_source};
    C c{dummy_const_sink};
    Edge g{*b, *c};
  }

  SECTION("function, no star") {
    P b{dummy_source};
    C c{dummy_sink};
    Edge g{b, c};
  }

  SECTION("function with const reference, no star") {
    P b{dummy_source};
    C c{dummy_const_sink};
    Edge g{b, c};
  }

  SECTION("lambda") {
    auto dummy_source_lambda = [](std::stop_source&) { return 0UL; };
    auto dummy_sink_lambda = [](size_t) {};

    P b{dummy_source_lambda};
    C c{dummy_sink_lambda};

    Edge g{*b, *c};
  }

  SECTION("inline lambda") {
    P b([](std::stop_source&) { return 0; });
    C c([](size_t) {});

    Edge g{*b, *c};
  }

  SECTION("function object") {
    auto a = dummy_source_class{};
    dummy_sink_class d{};

    P b{a};
    C c{d};

    Edge g{*b, *c};
  }

  SECTION("inline function object") {
    P b{dummy_source_class{}};
    C c{dummy_sink_class{}};

    Edge g{*b, *c};
  }

  SECTION("bind") {
    double x = 0.01;
    float y = -0.001;
    int z = 8675309;

    auto a = std::bind(dummy_bind_source, x);
    auto d = std::bind(dummy_bind_sink, y, std::placeholders::_1, z);

    P b{a};
    C c{d};

    Edge g{*b, *c};
  }

  SECTION("inline bind") {
    double x = 0.01;
    float y = -0.001;
    int z = 8675309;

    P b{std::bind(dummy_bind_source, x)};
    C c{std::bind(dummy_bind_sink, y, std::placeholders::_1, z)};

    Edge g{*b, *c};
  }

  SECTION("bind with move") {
    double x = 0.01;
    float y = -0.001;
    int z = 8675309;

    auto a = std::bind(dummy_bind_source, std::move(x));
    auto d = std::bind(dummy_bind_sink, y, std::placeholders::_1, std::move(z));

    P b{std::move(a)};
    C c{std::move(d)};

    Edge g{*b, *c};
  }
}

/**
 * Verify initializing producer_node, function_node, and consumer_node with
 * function, lambda, in-line lambda, function object, bind, and rvalue bind.
 * (This is a repeat of the previous test, but modified to include a
 * function_node.)
 */
TEMPLATE_TEST_CASE(
    "ResumableNodes: Verify various API approaches, including function_node",
    "[resumable_nodes]",
    (std::tuple<
        consumer_node<AsyncMover2, size_t>,
        function_node<AsyncMover2, size_t>,
        producer_node<AsyncMover2, size_t>>),
    (std::tuple<
        consumer_node<AsyncMover3, size_t>,
        function_node<AsyncMover3, size_t>,
        producer_node<AsyncMover3, size_t>>)) {
  using C = typename std::tuple_element<0, TestType>::type;
  using F = typename std::tuple_element<1, TestType>::type;
  using P = typename std::tuple_element<2, TestType>::type;

  SECTION("function") {
    P a{dummy_source};
    F b{dummy_function};
    C c{dummy_sink};

    Edge g{*a, *b};
    Edge h{*b, *c};
  }

  SECTION("lambda") {
    auto dummy_source_lambda = [](std::stop_source&) { return 0UL; };
    auto dummy_function_lambda = [](size_t) { return 0UL; };
    auto dummy_sink_lambda = [](size_t) {};

    P a{dummy_source_lambda};
    F b{dummy_function_lambda};
    C c{dummy_sink_lambda};

    Edge g{*a, *b};
    Edge h{*b, *c};
  }

  SECTION("inline lambda") {
    P a([](std::stop_source&) { return 0UL; });
    F b([](size_t) { return 0UL; });
    C c([](size_t) {});

    Edge g{*a, *b};
    Edge h{*b, *c};
  }

  SECTION("function object") {
    auto ac = dummy_source_class{};
    dummy_function_class fc{};
    dummy_sink_class dc{};

    P a{ac};
    F b{fc};
    C c{dc};

    Edge g{*a, *b};
    Edge h{*b, *c};
  }

  SECTION("inline function object") {
    P a{dummy_source_class{}};
    F b{dummy_function_class{}};
    C c{dummy_sink_class{}};

    Edge g{*a, *b};
    Edge h{*b, *c};
  }
  SECTION("bind") {
    double x = 0.01;
    float y = -0.001;
    int z = 8675309;

    auto ac = std::bind(dummy_bind_source, x);
    auto dc = std::bind(dummy_bind_sink, y, std::placeholders::_1, z);
    auto fc = std::bind(dummy_bind_function, x, y, std::placeholders::_1);

    P a{ac};
    F b{fc};
    C c{dc};

    Edge g{*a, *b};
    Edge h{*b, *c};
  }

  SECTION("inline bind") {
    double x = 0.01;
    float y = -0.001;
    int z = 8675309;

    P a{std::bind(dummy_bind_source, x)};
    F b{std::bind(dummy_bind_function, x, y, std::placeholders::_1)};
    C c{std::bind(dummy_bind_sink, y, std::placeholders::_1, z)};

    Edge i{*a, *b};
    Edge j{*b, *c};
  }

  SECTION("bind with move") {
    double x = 0.01;
    float y = -0.001;
    int z = 8675309;

    auto ac = std::bind(dummy_bind_source, std::move(x));
    auto dc = std::bind(
        dummy_bind_sink, std::move(y), std::placeholders::_1, std::move(z));
    auto fc = std::bind(
        dummy_bind_function, std::move(x), std::move(y), std::placeholders::_1);

    P a{std::move(ac)};
    F b{std::move(fc)};
    C c{std::move(dc)};

    Edge i{*a, *b};
    Edge j{*b, *c};
  }
}
#endif