/**
 * @file unit_util_functions.cc
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
 * Tests the util_functions classes, `SourceNode`, `SinkNode`, and
 * `FunctionNode`.
 */

#include "unit_util_functions.h"
#include <future>
#include <numeric>
#include "experimental/tiledb/common/dag/edge/edge.h"
#include "experimental/tiledb/common/dag/nodes/generators.h"
#include "experimental/tiledb/common/dag/nodes/nodes.h"
#include "experimental/tiledb/common/dag/nodes/terminals.h"
#include "experimental/tiledb/common/dag/state_machine/test/types.h"

using namespace tiledb::common;

/**
 * Test various uses of `consumer` class.
 */
TEST_CASE(
    "Utility Functions: Test various uses of `consumer` class.",
    "[util_functions]") {
  std::vector<size_t> w(10);
  std::iota(begin(w), end(w), 19);

  /**
   * Test that the terminal can fill an existing container.
   */
  SECTION("Output iterator, starting a begin()") {
    std::vector<size_t> v(10);
    std::iota(begin(v), end(v), 0);
    CHECK(!std::equal(begin(v), end(v), begin(w)));

    auto c = terminal{v.begin()};
    for (size_t i = 0; i < size(v); ++i) {
      c(i + 19);
    }
    CHECK(std::size(v) == 10);
    CHECK(std::size(w) == 10);
    CHECK(std::equal(begin(v), end(v), begin(w)));
  }

  /**
   * Check that the consumer can fill an existing container, pushing back at the
   * end.
   */
  SECTION("Back insert iterator, starting at begin()") {
    std::vector<size_t> v;

    auto x = std::back_inserter(v);
    terminal c{x};
    for (size_t i = 0; i < size(w); ++i) {
      c(i + 19);
    }
    CHECK(size(v) == 10);
    CHECK(std::equal(begin(v), end(v), begin(w)));
  }
}

/**
 * Test various uses of `ConsumerNode` class.
 */
TEST_CASE(
    "Utility Functions: Test various uses of ConsumerNode instantiated with "
    "`consumer` function object "
    "class.",
    "[util_functions]") {
  std::vector<size_t> w(10);
  std::iota(begin(w), end(w), 19);

  /**
   * Test that the consumer can fill an existing container.
   */
  SECTION("Output iterator, starting a begin()") {
    std::vector<size_t> v(10);
    std::iota(begin(v), end(v), 0);
    CHECK(!std::equal(begin(v), end(v), begin(w)));

    auto c = terminal{v.begin()};
    size_t i = 0;
    ConsumerNode<ManualMover3, size_t> consumer_node{c};
    ProducerNode<ManualMover3, size_t> producer_node{
        [&i] { return (i++ + 19); }};
    Edge(producer_node, consumer_node);

    for (size_t i = 0; i < size(v); ++i) {
      producer_node.resume();
      consumer_node.resume();
    }
    CHECK(std::size(v) == 10);
    CHECK(std::size(w) == 10);
    CHECK(std::equal(begin(v), end(v), begin(w)));
  }

  /**
   * Check that the consumer can fill an existing container, pushing back at the
   * end.
   */
  SECTION(
      "Back insert iterator, starting at begin(), with default constructed "
      "vector") {
    std::vector<size_t> v;
    auto x = std::back_inserter(v);
    terminal c{x};

    size_t i{0};
    ConsumerNode<ManualMover3, size_t> consumer_node{c};
    ProducerNode<ManualMover3, size_t> producer_node{
        [&i] { return (i++ + 19); }};
    Edge(producer_node, consumer_node);

    for (size_t i = 0; i < size(w); ++i) {
      producer_node.resume();
      consumer_node.resume();
    }
    CHECK(size(v) == 10);
    CHECK(std::equal(begin(v), end(v), begin(w)));
  }

  /**
   * Check that the consumer can fill an existing container, pushing back at the
   * end.
   */
  SECTION("Back insert iterator, starting at begin(), with reserved vector") {
    std::vector<size_t> v;
    auto x = std::back_inserter(v);
    terminal c{x};

    size_t i{0};
    ConsumerNode<ManualMover3, size_t> consumer_node{c};
    ProducerNode<ManualMover3, size_t> producer_node{
        [&i] { return (i++ + 19); }};
    Edge(producer_node, consumer_node);

    for (size_t i = 0; i < size(w); ++i) {
      producer_node.resume();
      consumer_node.resume();
    }
    CHECK(size(v) == 10);
    CHECK(std::equal(begin(v), end(v), begin(w)));
  }
}

/**
 * Test various uses of `generator class.
 */
TEST_CASE(
    "Utility Functions: Test various uses of `generator` class.",
    "[util_functions]") {
  std::vector<size_t> w(10);
  std::iota(begin(w), end(w), 19);

  /**
   * Test that the generator can fill an existing container.
   */
  SECTION("Generator, starting at 19") {
    std::stop_source stop_source;

    std::vector<size_t> v(10);
    std::iota(begin(v), end(v), 0);
    CHECK(!std::equal(begin(v), end(v), begin(w)));

    auto c = generators{19};
    for (size_t i = 0; i < size(v); ++i) {
      v[i] = c(stop_source);
    }
    CHECK(std::size(v) == 10);
    CHECK(std::size(w) == 10);
    CHECK(std::equal(begin(v), end(v), begin(w)));
  }
}

/**
 * Test various uses of `prng` class.
 */
TEMPLATE_TEST_CASE(
    "Utility Functions: Test various uses of `prng` class.",
    "[util_functions]",
    int,
    long,
    float,
    double) {
  /**
   * Test that the prng generates two different sequences when started from
   * different seeds (default seed).
   */
  SECTION("PRNG, default seed") {
    std::vector<TestType> v(10);
    std::vector<TestType> w(10);
    auto c = prng<TestType>{-10, 10};
    for (size_t i = 0; i < size(v); ++i) {
      v[i] = c();
    }
    for (size_t i = 0; i < size(w); ++i) {
      w[i] = c();
    }
    CHECK(!std::equal(begin(v), end(v), begin(w)));
  }

  /**
   * Test that the prng generates same sequences when started from same seed.
   */
  SECTION("PRNG, fixed seed") {
    std::vector<TestType> v(10);
    std::vector<TestType> w(10);
    auto c = prng<TestType>{-10, 10};
    c.seed(314159);
    for (size_t i = 0; i < size(v); ++i) {
      v[i] = c();
    }
    c.seed(314159);
    for (size_t i = 0; i < size(w); ++i) {
      w[i] = c();
    }
    CHECK(std::equal(begin(v), end(v), begin(w)));
  }
}

/**
 * Test various uses of `ConsumerNode` class.
 */
TEST_CASE(
    "Utility Functions: Test various uses of ProducerNode instantiated with "
    "`generator` function object.",
    "[util_functions]") {
  std::vector<size_t> w(10);
  std::iota(begin(w), end(w), 19);

  /**
   * Test that `ProducerNode
   */
  SECTION("ProducerNode, using generator") {
    std::vector<size_t> v(10);
    std::iota(begin(v), end(v), 0);

    CHECK(!std::equal(begin(v), end(v), begin(w)));

    auto c = generators{19};
    size_t i = 0;
    ConsumerNode<ManualMover3, size_t> consumer_node{
        [&v, &i](size_t k) { v[i++] = k; }};
    ProducerNode<ManualMover3, size_t> producer_node{c};
    Edge(producer_node, consumer_node);

    for (size_t i = 0; i < size(v); ++i) {
      producer_node.resume();
      consumer_node.resume();
    }
    CHECK(std::size(v) == 10);
    CHECK(std::size(w) == 10);
    CHECK(std::equal(begin(v), end(v), begin(w)));
  }
}

/**
 * Test `ConsumerNode` with `ProducerNode`, using `consumer` and `generator`
 * function objects.
 */
TEST_CASE(
    "Utility Functions: Test ProducerNode an ConumerNode together instantiated "
    "with `generator` and `consumer` function object.",
    "[util_functions]") {
  std::vector<size_t> w(10);
  std::iota(begin(w), end(w), 19);

  /**
   * Test ProducerNode and ConsumerNoder, using generator and consumer, starting
   * at begin(v).
   */
  SECTION(
      "ProducerNode and ConsumerNoder, using generator and consumer, starting "
      "at begin(v)") {
    std::vector<size_t> v(10);
    std::iota(begin(v), end(v), 0);

    CHECK(!std::equal(begin(v), end(v), begin(w)));

    auto x = begin(v);
    terminal con{x};

    auto gen = generators{19};
    ConsumerNode<ManualMover3, size_t> consumer_node{con};
    ProducerNode<ManualMover3, size_t> producer_node{gen};
    Edge(producer_node, consumer_node);

    for (size_t i = 0; i < size(v); ++i) {
      producer_node.resume();
      consumer_node.resume();
    }
    CHECK(std::size(v) == 10);
    CHECK(std::size(w) == 10);
    CHECK(std::equal(begin(v), end(v), begin(w)));
  }

  /**
   * Test ProducerNode and ConsumerNoder, using generator and consumer, with
   * back_inserter.
   */
  SECTION(
      "ProducerNode and ConsumerNoder, using generator and consumer, starting "
      "at begin(v)") {
    std::vector<size_t> v;
    auto x = std::back_inserter(v);
    terminal con{x};

    auto gen = generators{19};
    ConsumerNode<ManualMover3, size_t> consumer_node{con};
    ProducerNode<ManualMover3, size_t> producer_node{gen};
    Edge(producer_node, consumer_node);

    for (size_t i = 0; i < size(w); ++i) {
      producer_node.resume();
      consumer_node.resume();
    }
    CHECK(std::size(v) == 10);
    CHECK(std::size(w) == 10);
    CHECK(std::equal(begin(v), end(v), begin(w)));
  }

  /**
   * Test ProducerNode and ConsumerNoder, using generator and consumer, with
   * back_inserter, rvalues for producer and consumer`.
   */
  SECTION(
      "ProducerNode and ConsumerNoder, using generator and consumer, starting "
      "at begin(v), rvalues for producer and consumer") {
    std::vector<size_t> v;
    auto x = std::back_inserter(v);

    ConsumerNode<ManualMover3, size_t> consumer_node{terminal{x}};
    ProducerNode<ManualMover3, size_t> producer_node{generators{19}};
    Edge(producer_node, consumer_node);

    for (size_t i = 0; i < size(w); ++i) {
      producer_node.resume();
      consumer_node.resume();
    }
    CHECK(std::size(v) == 10);
    CHECK(std::size(w) == 10);
    CHECK(std::equal(begin(v), end(v), begin(w)));
  }

  /**
   * Test ProducerNode and ConsumerNoder, using generator and consumer, starting
   * at begin(v), using run_for()
   */
  SECTION(
      "ProducerNode and ConsumerNoder, using generator and consumer, starting "
      "at begin(v)") {
    size_t offset = GENERATE(0, 1, 2, 5);

    std::vector<size_t> v(10 + offset);
    std::iota(begin(v), end(v), 0);

    CHECK(!std::equal(begin(v), end(v), begin(w)));

    auto x = begin(v);
    terminal con{x};

    auto gen = generators{19};
    ConsumerNode<AsyncMover3, size_t> consumer_node{con};
    ProducerNode<AsyncMover3, size_t> producer_node{gen};
    Edge(producer_node, consumer_node);

    auto a = std::async(
        std::launch::async, [&producer_node]() { producer_node.run_for(10); });
    auto b = std::async(std::launch::async, [&consumer_node, offset]() {
      consumer_node.run_for(10 + offset);
    });

    b.wait();
    a.wait();

    CHECK(std::size(v) == (10 + offset));
    CHECK(std::size(w) == 10);
    CHECK(std::equal(begin(w), end(w), begin(v)));
  }

  /**
   * Test ProducerNode and ConsumerNoder, using generator and consumer, with
   * back_inserter and AsyncMover3.
   */
  SECTION(
      "ProducerNode and ConsumerNoder, using generator and consumer, starting "
      "at begin(v), using AsyncMover3") {
    size_t offset = GENERATE(0, 1, 2, 5);
    std::vector<size_t> v;
    auto x = std::back_inserter(v);
    terminal con{x};

    auto gen = generators{19};
    ConsumerNode<AsyncMover3, size_t> consumer_node{con};
    ProducerNode<AsyncMover3, size_t> producer_node{gen};
    Edge(producer_node, consumer_node);

    auto a = std::async(
        std::launch::async, [&producer_node]() { producer_node.run_for(10); });
    auto b = std::async(std::launch::async, [&consumer_node, offset]() {
      consumer_node.run_for(10 + offset);
    });

    b.wait();
    a.wait();

    CHECK(std::size(v) == 10);
    CHECK(std::size(w) == 10);
    CHECK(std::equal(begin(w), end(w), begin(v)));
  }
}

/**
 * Test `ConsumerNode` with `ProducerNode`, using `consumer` and `generator`
 * function objects.
 */
TEMPLATE_TEST_CASE(
    "Utility Functions: Test InjectorNode an ConsumerNode together "
    "instantiated "
    "`consumer` function object.",
    "[util_functions]",
    (std::tuple<
        InjectorNode<AsyncMover2, size_t>,
        ConsumerNode<AsyncMover2, size_t>>),
    (std::tuple<
        InjectorNode<AsyncMover3, size_t>,
        ConsumerNode<AsyncMover3, size_t>>)) {
  bool delay = GENERATE(false, true);
  size_t offset = GENERATE(0, 1, 2, 5);

  size_t rounds = 1337;

  std::vector<size_t> w(rounds);
  std::iota(begin(w), end(w), 19);

  std::vector<size_t> v;
  auto x = std::back_inserter(v);
  terminal con{x};
  auto decon = [&con, delay](size_t j) {
    if (delay)
      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));
    con(j);
    if (delay)
      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));
  };

  //  InjectorNode<AsyncMover3, size_t> injector_node{};
  //  ConsumerNode<AsyncMover3, size_t> consumer_node{decon};

  using I = typename std::tuple_element<0, TestType>::type;
  using C = typename std::tuple_element<1, TestType>::type;

  I injector_node{};
  C consumer_node{decon};

  Edge(injector_node, consumer_node);

  /**
   * Test InjectorNode and ConsumerNoder, using put, back_inserter and
   * AsyncMover3.
   */
  SECTION(
      "InjectorNode and ConsumerNoder, using generator and consumer, starting "
      "at begin(v), using AsyncMover3, put " +
      std::to_string(offset) + " " + std::to_string(delay)) {
    CHECK(v.size() == 0);
    CHECK(w.size() == rounds);
    auto a = std::async(std::launch::async, [&injector_node, rounds]() {
      for (size_t i = 0; i < rounds; ++i) {
        injector_node.put(i + 19);
      }
      injector_node.stop();
    });
    if (delay)
      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));
    auto b = std::async(std::launch::async, [&consumer_node, offset, rounds]() {
      consumer_node.run_for(rounds + offset);
    });

    if (delay)
      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));
    a.wait();
    if (delay)
      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));
    b.wait();
  }

  /**
   * Test InjectorNode and ConsumerNoder, using try_put, back_inserter and
   * AsyncMover3.
   */
  SECTION(
      "InjectorNode and ConsumerNoder, using generator and consumer, starting "
      "at begin(v), using AsyncMover3, try_put " +
      std::to_string(offset) + " " + std::to_string(delay)) {
    CHECK(v.size() == 0);
    CHECK(w.size() == rounds);
    auto a = std::async(std::launch::async, [&injector_node, rounds]() {
      for (size_t i = 0; i < rounds; ++i) {
        injector_node.try_put(i + 19);
      }
      injector_node.stop();
    });
    if (delay)
      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));
    auto b = std::async(std::launch::async, [&consumer_node, offset, rounds]() {
      consumer_node.run_for(rounds + offset);
    });

    if (delay)
      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));
    a.wait();
    if (delay)
      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));
    b.wait();
  }

  CHECK(std::size(v) == rounds);
  CHECK(std::size(w) == rounds);
  CHECK(std::equal(begin(w), end(w), begin(v)));
}
