/**
 * @file unit_edge_pseudo_nodes.cc
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
 * @todo Unify this with unit_pseudo_nodes.cc
 */

#include "unit_edge_pseudo_nodes.h"
#include <future>
#include <vector>
#include "experimental/tiledb/common/dag/ports/ports.h"
#include "experimental/tiledb/common/dag/ports/test/pseudo_nodes.h"
#include "experimental/tiledb/common/dag/state_machine/fsm.h"
#include "experimental/tiledb/common/dag/state_machine/policies.h"
#include "experimental/tiledb/common/dag/state_machine/test/helpers.h"
#include "experimental/tiledb/common/dag/state_machine/test/types.h"

#include "experimental/tiledb/common/dag/edge/edge.h"

using namespace tiledb::common;

/**
 * Test of producer and consumer functions.  The producer generates a an
 * increasing sequence of numbers starting from 0 and incrementing by 1 on each
 * invocation.  The consumer appends its input to a specified output iterator --
 * in this case, a back inserter to an `std::vector`.
 *
 */
TEST_CASE(
    "Pseudo Nodes: Producer and consumer functions and nodes",
    "[pseudo_nodes]") {
  size_t N = 37;

  generators g(N);

  std::vector<size_t> v;
  std::back_insert_iterator<std::vector<size_t>> w(v);
  consumer<std::back_insert_iterator<std::vector<size_t>>> c(w);

  SECTION("Test generator function") {
    for (size_t i = 0; i < N; ++i) {
      CHECK(g() == i);
    }
  }

  SECTION("Test consumer function") {
    for (size_t i = 0; i < N; ++i) {
      c(i);
    }
    REQUIRE(v.size() == N);
    for (size_t i = 0; i < N; ++i) {
      CHECK(v[i] == i);
    }
  }

  SECTION("Construct Producer and Consumer pseudo nodes") {
    ConsumerNode<AsyncMover3, size_t> r(c);

    ProducerNode<AsyncMover3, size_t> p(g);
    ProducerNode<AsyncMover3, size_t> q([]() { return 0UL; });
  }
}

/**
 * Test that we can attach a producer and consumer node to each other.
 */
TEST_CASE(
    "Pseudo Nodes: Attach producer and consumer nodes", "[pseudo_nodes]") {
  size_t N = 41;

  using Producer = ProducerNode<AsyncMover3, size_t>;

  using Consumer = ConsumerNode<AsyncMover3, size_t>;

  SECTION("Attach trivial lambdas") {
    Producer left([]() -> size_t { return 0UL; });
    Consumer right([](size_t) -> void { return; });

    SECTION("left to right") {
      Edge(left, right);
    }

    /* Don't allow reverse connections
     *
    SECTION("right to left") {
      Edge(right, left);
    }
    */

    SECTION("Edge 2") {
      Producer foo{[]() { return 0UL; }};
      Consumer bar{[](size_t) {}};

      Edge(foo, bar);
    }
  }

  SECTION("Edge generator and consumer") {
    generators g(N);

    std::vector<size_t> v;
    std::back_insert_iterator<std::vector<size_t>> w(v);
    consumer<std::back_insert_iterator<std::vector<size_t>>> c(w);

    Consumer r(c);
    Producer p(g);

    SECTION("Edge generator to consumer") {
      Edge(p, r);
    }
    /*
    SECTION("Edge consumer to generator") {
      Edge(r, p);
    }
    */
  }
}

/**
 * Test that we can synchronously send data from a producer to an Edgeed
 * consumer.
 */
TEST_CASE(
    "Pseudo Nodes: Pass some data, two Edgement orders", "[pseudo_nodes]") {
  size_t rounds = 43;

  generators g(rounds);

  std::vector<size_t> v;
  std::back_insert_iterator<std::vector<size_t>> w(v);
  consumer<std::back_insert_iterator<std::vector<size_t>>> c(w);

  ConsumerNode<AsyncMover3, size_t> r(c);
  ProducerNode<AsyncMover3, size_t> p(g);

  SECTION("Edge p to r") {
    Edge(p, r);
  }
  SECTION("Edge r to p") {
    Edge(p, r);
  }

  p.get();
  r.put();

  CHECK(v.size() == 1);

  p.get();
  r.put();

  CHECK(v.size() == 2);

  p.get();
  r.put();

  CHECK(v.size() == 3);

  CHECK(v[0] == 0);
  CHECK(v[1] == 1);
  CHECK(v[2] == 2);
}

/**
 * Test that we can asynchronously send data from a producer to an Edgeed
 * consumer.
 */
TEST_CASE("Pseudo Nodes: Asynchronously pass some data", "[pseudo_nodes]") {
  size_t rounds = 423;

  generators g(rounds);

  std::vector<size_t> v;
  std::back_insert_iterator<std::vector<size_t>> w(v);
  consumer<std::back_insert_iterator<std::vector<size_t>>> c(w);

  ConsumerNode<AsyncMover3, size_t> r(c);
  ProducerNode<AsyncMover3, size_t> p(g);

  Edge(p, r);

  auto fun_a = [&]() {
    size_t N = rounds;
    while (N--) {
      p.get();
    }
  };

  auto fun_b = [&]() {
    size_t N = rounds;
    while (N--) {
      r.put();
    }
  };

  CHECK(v.size() == 0);
  SECTION("Async Nodes a, b, a, b") {
    auto fut_a = std::async(std::launch::async, fun_a);
    auto fut_b = std::async(std::launch::async, fun_b);
    fut_a.get();
    fut_b.get();
  }

  SECTION("Async Nodes a, b, b, a") {
    auto fut_a = std::async(std::launch::async, fun_a);
    auto fut_b = std::async(std::launch::async, fun_b);
    fut_b.get();
    fut_a.get();
  }

  SECTION("Async Nodes b, a, a, b") {
    auto fut_b = std::async(std::launch::async, fun_b);
    auto fut_a = std::async(std::launch::async, fun_a);
    fut_a.get();
    fut_b.get();
  }

  SECTION("Async Nodes b, a, b, a") {
    auto fut_b = std::async(std::launch::async, fun_b);
    auto fut_a = std::async(std::launch::async, fun_a);
    fut_b.get();
    fut_a.get();
  }

  CHECK(v.size() == rounds);
  for (size_t i = 0; i < rounds; ++i) {
    CHECK(v[i] == i);
  }
}

/**
 * Repeat previous test, adding a random delay to each function body to emulate
 * a computation being done by the node body.
 */
TEST_CASE(
    "Pseudo Nodes: Asynchronously pass some data, random delays",
    "[pseudo_nodes]") {
  size_t rounds = 433;

  std::vector<size_t> v;
  size_t i{0};

  ConsumerNode<AsyncMover3, size_t> r([&](size_t i) {
    v.push_back(i);
    std::this_thread::sleep_for(std::chrono::microseconds(random_us(1234)));
  });
  ProducerNode<AsyncMover3, size_t> p([&]() {
    std::this_thread::sleep_for(std::chrono::microseconds(random_us(1234)));
    return i++;
  });

  Edge(p, r);

  auto fun_a = [&]() {
    size_t N = rounds;
    while (N--) {
      p.get();
    }
  };

  auto fun_b = [&]() {
    size_t N = rounds;
    while (N--) {
      r.put();
    }
  };

  CHECK(v.size() == 0);

  SECTION("Async Nodes a, b, a, b") {
    auto fut_a = std::async(std::launch::async, fun_a);
    auto fut_b = std::async(std::launch::async, fun_b);
    fut_a.get();
    fut_b.get();
  }

  SECTION("Async Nodes a, b, b, a") {
    auto fut_a = std::async(std::launch::async, fun_a);
    auto fut_b = std::async(std::launch::async, fun_b);
    fut_b.get();
    fut_a.get();
  }

  SECTION("Async Nodes b, a, a, b") {
    auto fut_b = std::async(std::launch::async, fun_b);
    auto fut_a = std::async(std::launch::async, fun_a);
    fut_a.get();
    fut_b.get();
  }

  SECTION("Async Nodes b, a, b, a") {
    auto fut_b = std::async(std::launch::async, fun_b);
    auto fut_a = std::async(std::launch::async, fun_a);
    fut_b.get();
    fut_a.get();
  }

  CHECK(v.size() == rounds);
  for (size_t i = 0; i < rounds; ++i) {
    CHECK(v[i] == i);
  }
}

/**
 * Test that we can connect source node and a sink node to a
 * function node.
 */
TEST_CASE("Pseudo Nodes: Edge to function node", "[pseudo_nodes]") {
  ProducerNode<AsyncMover3, size_t> q([]() { return 0UL; });

  FunctionNode<AsyncMover3, size_t> r([](size_t) { return 0UL; });

  ConsumerNode<AsyncMover3, size_t> s([](size_t) {});

  Edge(q, r);
  Edge(r, s);
}

/**
 * Test that we can synchronously send data from a producer to an Edgeed
 * function node and then to consumer.
 */
TEST_CASE(
    "Pseudo Nodes: Manuallay pass some data in a chain with function node",
    "[pseudo_nodes]") {
  size_t i{0UL};
  ProducerNode<AsyncMover3, size_t> q([&]() { return i++; });

  FunctionNode<AsyncMover3, size_t> r([&](size_t i) { return 2 * i; });

  std::vector<size_t> v;
  ConsumerNode<AsyncMover3, size_t> s([&](size_t i) { v.push_back(i); });

  Edge(q, r);
  Edge(r, s);

  q.get();
  r.run();
  s.put();

  CHECK(v.size() == 1);

  q.get();
  r.run();
  s.put();

  CHECK(v.size() == 2);

  q.get();
  r.run();
  s.put();

  CHECK(v.size() == 3);

  CHECK(v[0] == 0);
  CHECK(v[1] == 2);
  CHECK(v[2] == 4);
}

/**
 * Test that we can asynchronously send data from a producer to an Edgeed
 * function node and then to consumer.  Each of the nodes is launched as an
 * asynchronous task.
 *
 * @param qwt Weighting for source node delay.
 * @param rwt Weighting for function node delay.
 * @param swt Weighting for sink node delay.
 *
 * @tparam delay Flag indicating whether or not to include a delay at all in
 * node function bodies to simulate processing time.
 */
template <bool delay>
void asynchronous_with_function_node(
    double qwt = 1, double rwt = 1, double swt = 1) {
  size_t rounds = 437;

  std::vector<size_t> v;
  size_t i{0};

  ProducerNode<AsyncMover3, size_t> q([&]() {
    if constexpr (delay) {
      std::this_thread::sleep_for(std::chrono::microseconds(
          static_cast<size_t>(qwt * random_us(1234))));
    }
    return i++;
  });

  FunctionNode<AsyncMover3, size_t> r([&](size_t i) {
    if constexpr (delay) {
      std::this_thread::sleep_for(std::chrono::microseconds(
          static_cast<size_t>(rwt * random_us(1234))));
    }
    return 3 * i;
  });

  ConsumerNode<AsyncMover3, size_t> s([&](size_t i) {
    v.push_back(i);
    if constexpr (delay) {
      std::this_thread::sleep_for(std::chrono::microseconds(
          static_cast<size_t>(swt * random_us(1234))));
    }
  });

  Edge(q, r);
  Edge(r, s);

  auto fun_a = [&]() {
    size_t N = rounds;
    while (N--) {
      q.get();
    }
  };

  auto fun_b = [&]() {
    size_t N = rounds;
    while (N--) {
      r.run();
    }
  };

  auto fun_c = [&]() {
    size_t N = rounds;
    while (N--) {
      s.put();
    }
  };

  CHECK(v.size() == 0);

  SECTION("Async Nodes a, b, c, a, b, c") {
    auto fut_a = std::async(std::launch::async, fun_a);
    auto fut_b = std::async(std::launch::async, fun_b);
    auto fut_c = std::async(std::launch::async, fun_c);
    fut_a.get();
    fut_b.get();
    fut_c.get();
  }

  SECTION("Async Nodes a, b, c, c, b, a") {
    auto fut_a = std::async(std::launch::async, fun_a);
    auto fut_b = std::async(std::launch::async, fun_b);
    auto fut_c = std::async(std::launch::async, fun_c);
    fut_c.get();
    fut_b.get();
    fut_a.get();
  }

  SECTION("Async Nodes c, b, a, a, b, c") {
    auto fut_c = std::async(std::launch::async, fun_c);
    auto fut_b = std::async(std::launch::async, fun_b);
    auto fut_a = std::async(std::launch::async, fun_a);
    fut_a.get();
    fut_b.get();
    fut_c.get();
  }

  SECTION("Async Nodes c, b, a, c, b, a") {
    auto fut_c = std::async(std::launch::async, fun_c);
    auto fut_b = std::async(std::launch::async, fun_b);
    auto fut_a = std::async(std::launch::async, fun_a);
    fut_c.get();
    fut_b.get();
    fut_a.get();
  }

  CHECK(v.size() == rounds);
  for (size_t i = 0; i < rounds; ++i) {
    CHECK(v[i] == 3 * i);
  }
}

/**
 * Exercise `asynchronous_with_function_node()` with and without
 * computation-simulating delays and with weighted delays.
 */
TEST_CASE(
    "Pseudo Nodes: Asynchronous with function node and delay",
    "[pseudo_nodes]") {
  SECTION("Without delay") {
    asynchronous_with_function_node<false>();
  }
  SECTION("With delay") {
    asynchronous_with_function_node<true>(1, 1, 1);
  }
  SECTION("With delay, fast source") {
    asynchronous_with_function_node<true>(0.2, 1, 1);
  }
  SECTION("With delay, fast sink") {
    asynchronous_with_function_node<true>(1, 1, 0.2);
  }
  SECTION("With delay, fast source and fast sink") {
    asynchronous_with_function_node<true>(0.2, 1, 0.2);
  }
  SECTION("With delay, fast function") {
    asynchronous_with_function_node<true>(1, 0.2, 1);
  }
}

/**
 * Test that we can asynchronously send data from a producer to an Edgeed
 * function node and then to consumer.  Each of the nodes is launched as an
 * asynchronous task.
 *
 * @param qwt Weighting for source node delay.
 * @param rwt Weighting for function node delay.
 * @param swt Weighting for function node delay.
 * @param twt Weighting for sink node delay.
 *
 * @tparam delay Flag indicating whether or not to include a delay at all in
 * node function bodies to simulate processing time.
 */
template <bool delay>
void asynchronous_with_function_node_4(
    double qwt = 1, double rwt = 1, double swt = 1, double twt = 1) {
  size_t rounds = 331;

  std::vector<size_t> v;
  size_t i{0};

  ProducerNode<AsyncMover3, size_t> q([&]() {
    if constexpr (delay) {
      std::this_thread::sleep_for(std::chrono::microseconds(
          static_cast<size_t>(qwt * random_us(1234))));
    }
    return i++;
  });

  FunctionNode<AsyncMover3, size_t> r([&](size_t i) {
    if constexpr (delay) {
      std::this_thread::sleep_for(std::chrono::microseconds(
          static_cast<size_t>(rwt * random_us(1234))));
    }
    return 3 * i;
  });

  FunctionNode<AsyncMover3, size_t> s([&](size_t i) {
    if constexpr (delay) {
      std::this_thread::sleep_for(std::chrono::microseconds(
          static_cast<size_t>(swt * random_us(1234))));
    }
    return i + 17;
  });

  ConsumerNode<AsyncMover3, size_t> t([&](size_t i) {
    v.push_back(i);
    if constexpr (delay) {
      std::this_thread::sleep_for(std::chrono::microseconds(
          static_cast<size_t>(twt * random_us(1234))));
    }
  });

  Edge(q, r);
  Edge(r, s);
  Edge(s, t);

  auto fun_a = [&]() {
    size_t N = rounds;
    while (N--) {
      q.get();
    }
  };

  auto fun_b = [&]() {
    size_t N = rounds;
    while (N--) {
      r.run();
    }
  };

  auto fun_c = [&]() {
    size_t N = rounds;
    while (N--) {
      s.run();
    }
  };

  auto fun_d = [&]() {
    size_t N = rounds;
    while (N--) {
      t.put();
    }
  };

  CHECK(v.size() == 0);

  SECTION("Async Nodes a, b, c, d, a, b, c, d") {
    auto fut_a = std::async(std::launch::async, fun_a);
    auto fut_b = std::async(std::launch::async, fun_b);
    auto fut_c = std::async(std::launch::async, fun_c);
    auto fut_d = std::async(std::launch::async, fun_d);
    fut_a.get();
    fut_b.get();
    fut_c.get();
    fut_d.get();
  }

  SECTION("Async Nodes a, b, c, d, d, c, b, a") {
    auto fut_a = std::async(std::launch::async, fun_a);
    auto fut_b = std::async(std::launch::async, fun_b);
    auto fut_c = std::async(std::launch::async, fun_c);
    auto fut_d = std::async(std::launch::async, fun_d);
    fut_d.get();
    fut_c.get();
    fut_b.get();
    fut_a.get();
  }

  SECTION("Async Nodes d, c, b, a, a, b, c, d") {
    auto fut_d = std::async(std::launch::async, fun_d);
    auto fut_c = std::async(std::launch::async, fun_c);
    auto fut_b = std::async(std::launch::async, fun_b);
    auto fut_a = std::async(std::launch::async, fun_a);
    fut_a.get();
    fut_b.get();
    fut_c.get();
    fut_d.get();
  }

  SECTION("Async Nodes d, c, b, a, d, c, b, a") {
    auto fut_d = std::async(std::launch::async, fun_d);
    auto fut_c = std::async(std::launch::async, fun_c);
    auto fut_b = std::async(std::launch::async, fun_b);
    auto fut_a = std::async(std::launch::async, fun_a);
    fut_d.get();
    fut_c.get();
    fut_b.get();
    fut_a.get();
  }

  CHECK(v.size() == rounds);
  for (size_t i = 0; i < rounds; ++i) {
    CHECK(v[i] == 3 * i + 17);
  }
}

/**
 * Exercise `asynchronous_with_function_node_4()` with and without
 * computation-simulating delays and with weighted delays.
 */
TEST_CASE(
    "Pseudo Nodes: Asynchronous with two function nodes and delay",
    "[pseudo_nodes]") {
  SECTION("Without delay") {
    asynchronous_with_function_node_4<false>();
  }
  SECTION("With delay") {
    asynchronous_with_function_node_4<true>(1, 1, 1, 1);
  }
  SECTION("With delay, fast source") {
    asynchronous_with_function_node_4<true>(0.2, 1, 1, 1);
  }
  SECTION("With delay, fast sink") {
    asynchronous_with_function_node_4<true>(1, 1, 1, 0.2);
  }
  SECTION("With delay, fast source and fast sink") {
    asynchronous_with_function_node_4<true>(0.2, 1, 1, 0.2);
  }
  SECTION("With delay, fast function") {
    asynchronous_with_function_node_4<true>(1, 0.2, 1, 1);
  }
  SECTION("With delay, fast function and slow function") {
    asynchronous_with_function_node_4<true>(1, 0.2, 2, 1);
  }
}
