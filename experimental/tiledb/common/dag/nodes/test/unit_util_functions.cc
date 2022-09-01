/**
 * @file unit_nodes.cc
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
#include "experimental/tiledb/common/dag/edge/edge.h"
#include "experimental/tiledb/common/dag/nodes/consumer.h"
#include "experimental/tiledb/common/dag/nodes/generator.h"
#include "experimental/tiledb/common/dag/nodes/nodes.h"
#include "experimental/tiledb/common/dag/state_machine/test/types.h"

using namespace tiledb::common;

/**
 * Test various uses of `consumer class.
 */
TEST_CASE(
    "Utility Functions: Test various uses of `consumer class.",
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

    auto c = consumer{v.begin()};
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
    consumer c{x};
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

    auto c = consumer{v.begin()};
    size_t i = 0;
    ConsumerNode<ManualMover3, size_t> consumer_node{c};
    ProducerNode<ManualMover3, size_t> producer_node{
        [&i] { return (i++ + 19); }};
    Edge(producer_node, consumer_node);

    for (size_t i = 0; i < size(v); ++i) {
      producer_node.run_once();
      consumer_node.run_once();
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
    consumer c{x};

    size_t i{0};
    ConsumerNode<ManualMover3, size_t> consumer_node{c};
    ProducerNode<ManualMover3, size_t> producer_node{
        [&i] { return (i++ + 19); }};
    Edge(producer_node, consumer_node);

    for (size_t i = 0; i < size(w); ++i) {
      producer_node.run_once();
      consumer_node.run_once();
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
    consumer c{x};

    size_t i{0};
    ConsumerNode<ManualMover3, size_t> consumer_node{c};
    ProducerNode<ManualMover3, size_t> producer_node{
        [&i] { return (i++ + 19); }};
    Edge(producer_node, consumer_node);

    for (size_t i = 0; i < size(w); ++i) {
      producer_node.run_once();
      consumer_node.run_once();
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

    auto c = generator{19};
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

    auto c = generator{19};
    size_t i = 0;
    ConsumerNode<ManualMover3, size_t> consumer_node{
        [&v, &i](size_t k) { v[i++] = k; }};
    ProducerNode<ManualMover3, size_t> producer_node{c};
    Edge(producer_node, consumer_node);

    for (size_t i = 0; i < size(v); ++i) {
      producer_node.run_once();
      consumer_node.run_once();
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
    consumer con{x};

    auto gen = generator{19};
    ConsumerNode<ManualMover3, size_t> consumer_node{con};
    ProducerNode<ManualMover3, size_t> producer_node{gen};
    Edge(producer_node, consumer_node);

    for (size_t i = 0; i < size(v); ++i) {
      producer_node.run_once();
      consumer_node.run_once();
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
    consumer con{x};

    auto gen = generator{19};
    ConsumerNode<ManualMover3, size_t> consumer_node{con};
    ProducerNode<ManualMover3, size_t> producer_node{gen};
    Edge(producer_node, consumer_node);

    for (size_t i = 0; i < size(w); ++i) {
      producer_node.run_once();
      consumer_node.run_once();
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

    ConsumerNode<ManualMover3, size_t> consumer_node{consumer{x}};
    ProducerNode<ManualMover3, size_t> producer_node{generator{19}};
    Edge(producer_node, consumer_node);

    for (size_t i = 0; i < size(w); ++i) {
      producer_node.run_once();
      consumer_node.run_once();
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
    consumer con{x};

    auto gen = generator{19};
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
    consumer con{x};

    auto gen = generator{19};
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
