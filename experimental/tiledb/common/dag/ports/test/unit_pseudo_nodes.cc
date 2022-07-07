/**
 * @file unit_pseudo_nodes.cc
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

#include "unit_pseudo_nodes.h"
#include <future>
#include <vector>
#include "../fsm.h"
#include "../ports.h"
#include "helpers.h"
#include "pseudo_nodes.h"

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

  generator g(N);

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
    ConsumerNode<size_t, AsyncStateMachine<size_t>> r(c);

    ProducerNode<size_t, AsyncStateMachine<size_t>> p(g);
    ProducerNode<size_t, AsyncStateMachine<size_t>> q([]() { return 0UL; });
  }
}

/**
 * Test that we can attach a producer and consumer node to each other.
 */
TEST_CASE(
    "Pseudo Nodes: Attach producer and consumer nodes", "[pseudo_nodes]") {
  size_t N = 41;

  SECTION("Attach trivial lambdas") {
    ProducerNode<int, DebugStateMachine<std::optional<int>>> left(
        []() -> int { return 0; });
    ConsumerNode<int, DebugStateMachine<std::optional<int>>> right(
        [](int) -> void { return; });

    SECTION("left to right") {
      attach(left, right);
    }
    SECTION("right to left") {
      attach(right, left);
    }
  }

  SECTION("Attach generator and consumer") {
    generator g(N);

    std::vector<size_t> v;
    std::back_insert_iterator<std::vector<size_t>> w(v);
    consumer<std::back_insert_iterator<std::vector<size_t>>> c(w);

    ConsumerNode<size_t, AsyncStateMachine<std::optional<size_t>>> r(c);
    ProducerNode<size_t, AsyncStateMachine<std::optional<size_t>>> p(g);

    SECTION("Attach generator to consumer") {
      attach(p, r);
    }
    SECTION("Attach consumer to generator") {
      attach(r, p);
    }
  }
}

/**
 * Test that we can synchronously send data from a producer to an attached
 * consumer.
 */
TEST_CASE(
    "Pseudo Nodes: Pass some data, two attachment orders", "[pseudo_nodes]") {
  size_t rounds = 43;

  generator g(rounds);

  std::vector<size_t> v;
  std::back_insert_iterator<std::vector<size_t>> w(v);
  consumer<std::back_insert_iterator<std::vector<size_t>>> c(w);

  ConsumerNode<size_t, AsyncStateMachine<std::optional<size_t>>> r(c);
  ProducerNode<size_t, AsyncStateMachine<std::optional<size_t>>> p(g);

  SECTION("Attach p to r") {
    attach(p, r);
  }
  SECTION("Attach r to p") {
    attach(p, r);
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
 * Test that we can asynchronously send data from a producer to an attached
 * consumer.
 */
TEST_CASE("Pseudo Nodes: Asynchronously pass some data", "[pseudo_nodes]") {
  size_t rounds = 423;

  generator g(rounds);

  std::vector<size_t> v;
  std::back_insert_iterator<std::vector<size_t>> w(v);
  consumer<std::back_insert_iterator<std::vector<size_t>>> c(w);

  ConsumerNode<size_t, AsyncStateMachine<std::optional<size_t>>> r(c);
  ProducerNode<size_t, AsyncStateMachine<std::optional<size_t>>> p(g);

  attach(p, r);

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

  ConsumerNode<size_t, AsyncStateMachine<std::optional<size_t>>> r(
      [&](size_t i) {
        v.push_back(i);
        std::this_thread::sleep_for(std::chrono::microseconds(random_us(1234)));
      });
  ProducerNode<size_t, AsyncStateMachine<std::optional<size_t>>> p([&]() {
    std::this_thread::sleep_for(std::chrono::microseconds(random_us(1234)));
    return i++;
  });

  attach(p, r);

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
TEST_CASE("Pseudo Nodes: Attach to function node", "[pseudo_nodes]") {
  ProducerNode<size_t, AsyncStateMachine<std::optional<size_t>>> q(
      []() { return 0UL; });

  FunctionNode<size_t, size_t, AsyncStateMachine<std::optional<size_t>>> r(
      [](size_t) { return 0UL; });

  ConsumerNode<size_t, AsyncStateMachine<std::optional<size_t>>> s(
      [](size_t) {});

  attach(q, r);
  attach(r, s);
}

/**
 * Test that we can synchronously send data from a producer to an attached
 * function node and then to consumer.
 */
TEST_CASE(
    "Pseudo Nodes: Manuallay pass some data in a chain with function node",
    "[pseudo_nodes]") {
  size_t i{0UL};
  ProducerNode<size_t, AsyncStateMachine<std::optional<size_t>>> q(
      [&]() { return i++; });

  FunctionNode<size_t, size_t, AsyncStateMachine<std::optional<size_t>>> r(
      [&](size_t i) { return 2 * i; });

  std::vector<size_t> v;
  ConsumerNode<size_t, AsyncStateMachine<std::optional<size_t>>> s(
      [&](size_t i) { v.push_back(i); });

  attach(q, r);
  attach(r, s);

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

template <bool delay>
void asynchronous_with_function_node() {
  size_t rounds = 437;

  std::vector<size_t> v;
  size_t i{0};

  ProducerNode<size_t, AsyncStateMachine<std::optional<size_t>>> q([&]() {
    if constexpr (delay) {
      std::this_thread::sleep_for(std::chrono::microseconds(random_us(1234)));
    }
    return i++;
  });

  FunctionNode<size_t, size_t, AsyncStateMachine<std::optional<size_t>>> r(
      [&](size_t i) {
        if constexpr (delay) {
          std::this_thread::sleep_for(
              std::chrono::microseconds(random_us(1234)));
        }
        return 3 * i;
      });

  ConsumerNode<size_t, AsyncStateMachine<std::optional<size_t>>> s(
      [&](size_t i) {
        v.push_back(i);
        if constexpr (delay) {
          std::this_thread::sleep_for(
              std::chrono::microseconds(random_us(1234)));
        }
      });

  attach(q, r);
  attach(r, s);

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

TEST_CASE(
    "Pseudo Nodes: Asynchronous with function node and delay",
    "[pseudo_nodes]") {
  SECTION("Without delay") {
    asynchronous_with_function_node<false>();
  }
  SECTION("With delay") {
    asynchronous_with_function_node<true>();
  }
}
