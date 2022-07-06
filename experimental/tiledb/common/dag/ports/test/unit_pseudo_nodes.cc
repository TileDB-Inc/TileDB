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
#include "pseudo_nodes.h"

using namespace tiledb::common;

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

TEST_CASE(
    "Pseudo Nodes: Attach producer and consumer nodes", "[pseudo_nodes]") {
  size_t N = 41;

  SECTION("Attach trivial lambdas") {
    ProducerNode<int, DebugStateMachine<std::optional<int>>> left(
        []() -> int { return 0; });
    ConsumerNode<int, DebugStateMachine<std::optional<int>>> right(
        [](int) -> void { return; });
    attach(left, right);
  }

  SECTION("Attach generator and consumer") {
    generator g(N);

    std::vector<size_t> v;
    std::back_insert_iterator<std::vector<size_t>> w(v);
    consumer<std::back_insert_iterator<std::vector<size_t>>> c(w);

    ConsumerNode<size_t, AsyncStateMachine<std::optional<size_t>>> r(c);
    ProducerNode<size_t, AsyncStateMachine<std::optional<size_t>>> p(g);

    attach(p, r);
  }
}

TEST_CASE("Pseudo Nodes: Pass some data", "[pseudo_nodes]") {
  size_t N = 43;

  generator g(N);

  std::vector<size_t> v;
  std::back_insert_iterator<std::vector<size_t>> w(v);
  consumer<std::back_insert_iterator<std::vector<size_t>>> c(w);

  ConsumerNode<size_t, AsyncStateMachine<std::optional<size_t>>> r(c);
  ProducerNode<size_t, AsyncStateMachine<std::optional<size_t>>> p(g);

  attach(p, r);

  size_t rounds = 29;

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

  SECTION("Get then put") {
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

  SECTION("Async Nodes a, b, a, b") {
    CHECK(v.size() == 0);

    auto fut_a = std::async(std::launch::async, fun_a);
    auto fut_b = std::async(std::launch::async, fun_b);
    fut_a.get();
    fut_b.get();
    CHECK(v.size() == rounds);
    for (size_t i = 0; i < rounds; ++i) {
      CHECK(v[i] == i);
    }
  }

  SECTION("Async Nodes a, b, b, a") {
    CHECK(v.size() == 0);
    auto fut_a = std::async(std::launch::async, fun_a);
    auto fut_b = std::async(std::launch::async, fun_b);
    fut_b.get();
    fut_a.get();
    CHECK(v.size() == rounds);
    for (size_t i = 0; i < rounds; ++i) {
      CHECK(v[i] == i);
    }
  }

  SECTION("Async Nodes b, a, a, b") {
    CHECK(v.size() == 0);
    auto fut_b = std::async(std::launch::async, fun_b);
    auto fut_a = std::async(std::launch::async, fun_a);
    fut_a.get();
    fut_b.get();
    CHECK(v.size() == rounds);
    for (size_t i = 0; i < rounds; ++i) {
      CHECK(v[i] == i);
    }
  }

  SECTION("Async Nodes b, a, b, a") {
    CHECK(v.size() == 0);
    auto fut_b = std::async(std::launch::async, fun_b);
    auto fut_a = std::async(std::launch::async, fun_a);
    fut_b.get();
    fut_a.get();
    CHECK(v.size() == rounds);
    for (size_t i = 0; i < rounds; ++i) {
      CHECK(v[i] == i);
    }
  }
}
