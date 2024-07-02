/**
 * @file unit_simple.cc
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
 * Tests the nodes classes, `SourceNode`, `SinkNode`, and `FunctionNode`.
 */

#include "unit_simple_nodes.h"
#include <future>
#include <numeric>
#include <type_traits>
#include "experimental/tiledb/common/dag/edge/edge.h"
#include "experimental/tiledb/common/dag/nodes/generators.h"
#include "experimental/tiledb/common/dag/nodes/simple_nodes.h"
#include "experimental/tiledb/common/dag/nodes/terminals.h"
#include "experimental/tiledb/common/dag/state_machine/test/types.h"
#include "experimental/tiledb/common/dag/utility/print_types.h"

using namespace tiledb::common;

/**
 * Verify various API approaches
 */
TEMPLATE_TEST_CASE(
    "SimpleNodes: Verify various API approaches",
    "[simple_nodes]",
    (std::tuple<
        ConsumerNode<AsyncMover2, size_t>,
        ProducerNode<AsyncMover2, size_t>>),
    (std::tuple<
        ConsumerNode<AsyncMover3, size_t>,
        ProducerNode<AsyncMover3, size_t>>)) {
  using C = typename std::tuple_element<0, TestType>::type;
  using P = typename std::tuple_element<1, TestType>::type;

  SECTION("Test Construction") {
    P a;
    P b([]() { return 0UL; });
    C c([](size_t) {});
  }
  SECTION("Test Connection") {
    P b([]() { return 0UL; });
    C c([](size_t) {});
    Edge g{b, c};
  }
  SECTION("Enable if fail") {
    // These will fail, with good diagnostics.  This should be commented out
    // from time to time and tested by hand that we get the right error
    // messages.
    //   ProducerNode<AsyncMover3, size_t> bb{0UL};
    //   ConsumerNode<AsyncMover3, size_t> cc{-1.1};
    //   Edge g{bb, cc};
  }
}

/**
 * Some dummy functions and classes to test node constructors
 * with.
 */
size_t dummy_source() {
  return size_t{};
}

size_t dummy_function(size_t) {
  return size_t{};
}

void dummy_sink(size_t) {
}

class dummy_source_class {
 public:
  size_t operator()() {
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
size_t dummy_source_t() {
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
 * Verify initializing ProducerNode and ConsumerNode with function, lambda,
 * in-line lambda, function object, bind, and rvalue bind.
 */
TEMPLATE_TEST_CASE(
    "SimpleNodes: Verify numerous API approaches, with edges",
    "[simple_nodes]",
    (std::tuple<
        ConsumerNode<AsyncMover2, size_t>,
        ProducerNode<AsyncMover2, size_t>>),
    (std::tuple<
        ConsumerNode<AsyncMover3, size_t>,
        ProducerNode<AsyncMover3, size_t>>)) {
  using C = typename std::tuple_element<0, TestType>::type;
  using P = typename std::tuple_element<1, TestType>::type;

  SECTION("function") {
    P b{dummy_source};
    C c{dummy_sink};
    Edge g{b, c};
  }

  SECTION("lambda") {
    auto dummy_source_lambda = []() { return 0UL; };
    auto dummy_sink_lambda = [](size_t) {};

    P b{dummy_source_lambda};
    C c{dummy_sink_lambda};

    Edge g{b, c};
  }

  SECTION("inline lambda") {
    P b([]() { return 0UL; });
    C c([](size_t) {});

    Edge g{b, c};
  }

  SECTION("function object") {
    auto a = dummy_source_class{};
    dummy_sink_class d{};

    P b{a};
    C c{d};

    Edge g{b, c};
  }

  SECTION("inline function object") {
    P b{dummy_source_class{}};
    C c{dummy_sink_class{}};

    Edge g{b, c};
  }

  SECTION("bind") {
    double x = 0.01;
    float y = -0.001;
    int z = 8675309;

    auto a = std::bind(dummy_bind_source, x);
    auto d = std::bind(dummy_bind_sink, y, std::placeholders::_1, z);

    P b{a};
    C c{d};

    Edge g{b, c};
  }

  SECTION("inline bind") {
    double x = 0.01;
    float y = -0.001;
    int z = 8675309;

    P b{std::bind(dummy_bind_source, x)};
    C c{std::bind(dummy_bind_sink, y, std::placeholders::_1, z)};

    Edge g{b, c};
  }

  SECTION("bind with move") {
    double x = 0.01;
    float y = -0.001;
    int z = 8675309;

    auto a = std::bind(dummy_bind_source, std::move(x));
    auto d = std::bind(dummy_bind_sink, y, std::placeholders::_1, std::move(z));

    P b{std::move(a)};
    C c{std::move(d)};

    Edge g{b, c};
  }
}

/**
 * Verify initializing ProducerNode, FunctionNode, and ConsumerNode with
 * function, lambda, in-line lambda, function object, bind, and rvalue bind.
 * (This is a repeat of the previous test, but modified to include a
 * FunctionNode.)
 */
TEMPLATE_TEST_CASE(
    "SimpleNodes: Verify various API approaches, including FunctionNode",
    "[simple_nodes]",
    (std::tuple<
        ConsumerNode<AsyncMover2, size_t>,
        FunctionNode<AsyncMover2, size_t>,
        ProducerNode<AsyncMover2, size_t>>),
    (std::tuple<
        ConsumerNode<AsyncMover3, size_t>,
        FunctionNode<AsyncMover3, size_t>,
        ProducerNode<AsyncMover3, size_t>>)) {
  using C = typename std::tuple_element<0, TestType>::type;
  using F = typename std::tuple_element<1, TestType>::type;
  using P = typename std::tuple_element<2, TestType>::type;

  SECTION("function") {
    P a{dummy_source};
    F b{dummy_function};
    C c{dummy_sink};

    Edge g{a, b};
    Edge h{b, c};
  }

  SECTION("lambda") {
    auto dummy_source_lambda = []() { return 0UL; };
    auto dummy_function_lambda = [](size_t) { return 0UL; };
    auto dummy_sink_lambda = [](size_t) {};

    P a{dummy_source_lambda};
    F b{dummy_function_lambda};
    C c{dummy_sink_lambda};

    Edge g{a, b};
    Edge h{b, c};
  }

  SECTION("inline lambda") {
    P a([]() { return 0UL; });
    F b([](size_t) { return 0UL; });
    C c([](size_t) {});

    Edge g{a, b};
    Edge h{b, c};
  }

  SECTION("function object") {
    auto ac = dummy_source_class{};
    dummy_function_class fc{};
    dummy_sink_class dc{};

    P a{ac};
    F b{fc};
    C c{dc};

    Edge g{a, b};
    Edge h{b, c};
  }

  SECTION("inline function object") {
    P a{dummy_source_class{}};
    F b{dummy_function_class{}};
    C c{dummy_sink_class{}};

    Edge g{a, b};
    Edge h{b, c};
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

    Edge g{a, b};
    Edge h{b, c};
  }

  SECTION("inline bind") {
    double x = 0.01;
    float y = -0.001;
    int z = 8675309;

    P a{std::bind(dummy_bind_source, x)};
    F b{std::bind(dummy_bind_function, x, y, std::placeholders::_1)};
    C c{std::bind(dummy_bind_sink, y, std::placeholders::_1, z)};

    Edge i{a, b};
    Edge j{b, c};
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

    Edge i{a, b};
    Edge j{b, c};
  }
}

/**
 * Test of producer and consumer functions.  The producer generates an
 * increasing sequence of numbers starting from 0 and incrementing by 1 on
 * each invocation.  The consumer appends its input to a specified output
 * iterator -- in this case, a back inserter to an `std::vector`.
 *
 */
TEMPLATE_TEST_CASE(
    "SimpleNodes: Producer and consumer functions and nodes",
    "[simple_nodes]",
    (std::tuple<
        ConsumerNode<AsyncMover2, size_t>,
        FunctionNode<AsyncMover2, size_t>,
        ProducerNode<AsyncMover2, size_t>>),
    (std::tuple<
        ConsumerNode<AsyncMover3, size_t>,
        FunctionNode<AsyncMover3, size_t>,
        ProducerNode<AsyncMover3, size_t>>)) {
  using C = typename std::tuple_element<0, TestType>::type;
  //  using F = typename std::tuple_element<1, TestType>::type;
  using P = typename std::tuple_element<2, TestType>::type;

  size_t N = 37;
  std::stop_source stop_source;

  generators g{0UL, N};

  std::vector<size_t> v;
  auto w = std::back_inserter(v);
  terminal c{w};

  SECTION("Test generator function") {
    for (size_t i = 0; i < N; ++i) {
      CHECK(g(stop_source) == i);
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

  SECTION("Construct Producer and Consumer nodes") {
    C r(c);
    P p(g);
    P q([]() { return 0UL; });
  }
}

/**
 * Test that we can attach a producer and consumer node to each other.
 */
TEMPLATE_TEST_CASE(
    "SimpleNodes: Attach producer and consumer nodes",
    "[simple_nodes]",
    (std::tuple<
        ConsumerNode<AsyncMover2, size_t>,
        FunctionNode<AsyncMover2, size_t>,
        ProducerNode<AsyncMover2, size_t>>),
    (std::tuple<
        ConsumerNode<AsyncMover3, size_t>,
        FunctionNode<AsyncMover3, size_t>,
        ProducerNode<AsyncMover3, size_t>>)) {
  using Consumer = typename std::tuple_element<0, TestType>::type;
  //  using Function = typename std::tuple_element<1, TestType>::type;
  using Producer = typename std::tuple_element<2, TestType>::type;

  size_t N = 41;

  SECTION("Attach trivial lambdas") {
    Producer left([]() -> size_t { return 0UL; });
    Consumer right([](size_t) -> void { return; });

    SECTION("left to right") {
      attach(left, right);
    }
    SECTION("right to left") {
      attach(right, left);
    }

    SECTION("Attach 2") {
      Producer foo{[]() { return 0UL; }};
      Consumer bar{[](size_t) {}};

      attach(foo, bar);
    }
  }

  SECTION("Attach generator and consumer") {
    generators g{N};

    std::vector<size_t> v;
    std::back_insert_iterator<std::vector<size_t>> w(v);
    terminal<std::back_insert_iterator<std::vector<size_t>>> c(w);

    Consumer r(c);
    Producer p(g);

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
TEMPLATE_TEST_CASE(
    "SimpleNodes: Pass some data, two attachment orders",
    "[simple_nodes]",
    (std::tuple<
        ConsumerNode<AsyncMover2, size_t>,
        FunctionNode<AsyncMover2, size_t>,
        ProducerNode<AsyncMover2, size_t>>),
    (std::tuple<
        ConsumerNode<AsyncMover3, size_t>,
        FunctionNode<AsyncMover3, size_t>,
        ProducerNode<AsyncMover3, size_t>>)) {
  using Consumer = typename std::tuple_element<0, TestType>::type;
  //  using Function = typename std::tuple_element<1, TestType>::type;
  using Producer = typename std::tuple_element<2, TestType>::type;

  generators g;

  std::vector<size_t> v;
  std::back_insert_iterator<std::vector<size_t>> w(v);
  terminal<std::back_insert_iterator<std::vector<size_t>>> c(w);

  Consumer r(c);
  Producer p(g);

  SECTION("Attach p to r") {
    attach(p, r);
  }
  SECTION("Attach r to p") {
    attach(p, r);
  }

  p.resume();
  r.resume();

  CHECK(v.size() == 1);

  p.resume();
  r.resume();

  CHECK(v.size() == 2);

  p.resume();
  r.resume();

  CHECK(v.size() == 3);

  CHECK(v[0] == 0);
  CHECK(v[1] == 1);
  CHECK(v[2] == 2);
}

/**
 * Test that we can asynchronously send data from a producer to an attached
 * consumer.
 */
TEMPLATE_TEST_CASE(
    "SimpleNodes: Asynchronously pass some data",
    "[simple_nodes]",
    (std::tuple<
        ConsumerNode<AsyncMover2, size_t>,
        FunctionNode<AsyncMover2, size_t>,
        ProducerNode<AsyncMover2, size_t>>),
    (std::tuple<
        ConsumerNode<AsyncMover3, size_t>,
        FunctionNode<AsyncMover3, size_t>,
        ProducerNode<AsyncMover3, size_t>>)) {
  using Consumer = typename std::tuple_element<0, TestType>::type;
  //  using Function = typename std::tuple_element<1, TestType>::type;
  using Producer = typename std::tuple_element<2, TestType>::type;

  size_t rounds = 423;

  generators g;

  std::vector<size_t> v;
  std::back_insert_iterator<std::vector<size_t>> w(v);
  terminal<std::back_insert_iterator<std::vector<size_t>>> c(w);

  Consumer r(c);
  Producer p(g);

  attach(p, r);

  auto fun_a = [&]() {
    size_t N = rounds;
    while (N--) {
      p.resume();
    }
  };

  auto fun_b = [&]() {
    size_t N = rounds;
    while (N--) {
      r.resume();
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
 * Repeat previous test, adding a random delay to each function body to
 * emulate a computation being done by the node body.
 */
TEMPLATE_TEST_CASE(
    "SimpleNodes: Asynchronously pass some data, random delays",
    "[simple_nodes]",
    (std::tuple<
        ConsumerNode<AsyncMover2, size_t>,
        FunctionNode<AsyncMover2, size_t>,
        ProducerNode<AsyncMover2, size_t>>),
    (std::tuple<
        ConsumerNode<AsyncMover3, size_t>,
        FunctionNode<AsyncMover3, size_t>,
        ProducerNode<AsyncMover3, size_t>>)) {
  using Consumer = typename std::tuple_element<0, TestType>::type;
  //  using Function = typename std::tuple_element<1, TestType>::type;
  using Producer = typename std::tuple_element<2, TestType>::type;

  size_t rounds = 433;

  std::vector<size_t> v;
  size_t i{0};

  Consumer r([&](size_t i) {
    v.push_back(i);
    std::this_thread::sleep_for(std::chrono::microseconds(random_us(1234)));
  });
  Producer p([&]() {
    std::this_thread::sleep_for(std::chrono::microseconds(random_us(1234)));
    return i++;
  });

  attach(p, r);

  auto fun_a = [&]() {
    size_t N = rounds;
    while (N--) {
      p.resume();
    }
  };

  auto fun_b = [&]() {
    size_t N = rounds;
    while (N--) {
      r.resume();
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
TEMPLATE_TEST_CASE(
    "SimpleNodes: Attach to function node",
    "[simple_nodes]",
    (std::tuple<
        ConsumerNode<AsyncMover2, size_t>,
        FunctionNode<AsyncMover2, size_t>,
        ProducerNode<AsyncMover2, size_t>>),
    (std::tuple<
        ConsumerNode<AsyncMover3, size_t>,
        FunctionNode<AsyncMover3, size_t>,
        ProducerNode<AsyncMover3, size_t>>)) {
  using Consumer = typename std::tuple_element<0, TestType>::type;
  using Function = typename std::tuple_element<1, TestType>::type;
  using Producer = typename std::tuple_element<2, TestType>::type;

  Producer q([]() { return 0UL; });
  Function r([](size_t) { return 0UL; });
  Consumer s([](size_t) {});

  attach(q, r);
  attach(r, s);
}

/**
 * Test that we can synchronously send data from a producer to an attached
 * function node and then to consumer.
 */
TEMPLATE_TEST_CASE(
    "SimpleNodes: Manualay pass some data in a chain with function node",
    "[simple_nodes]",
    (std::tuple<
        ConsumerNode<AsyncMover2, size_t>,
        FunctionNode<AsyncMover2, size_t>,
        ProducerNode<AsyncMover2, size_t>>),
    (std::tuple<
        ConsumerNode<AsyncMover3, size_t>,
        FunctionNode<AsyncMover3, size_t>,
        ProducerNode<AsyncMover3, size_t>>)) {
  using Consumer = typename std::tuple_element<0, TestType>::type;
  using Function = typename std::tuple_element<1, TestType>::type;
  using Producer = typename std::tuple_element<2, TestType>::type;

  size_t i{0UL};
  std::vector<size_t> v;

  Producer q([&]() { return i++; });
  Function r([&](size_t i) { return 2 * i; });
  Consumer s([&](size_t i) { v.push_back(i); });

  attach(q, r);
  attach(r, s);

  q.resume();
  r.resume();
  s.resume();

  CHECK(v.size() == 1);

  q.resume();
  r.resume();
  s.resume();

  CHECK(v.size() == 2);

  q.resume();
  r.resume();
  s.resume();

  CHECK(v.size() == 3);

  CHECK(v[0] == 0);
  CHECK(v[1] == 2);
  CHECK(v[2] == 4);
}

TEMPLATE_TEST_CASE(
    "SimpleNodes: Asynchronous with function node and delay",
    "[simple_nodes]",
    (std::tuple<
        ConsumerNode<AsyncMover2, size_t>,
        FunctionNode<AsyncMover2, size_t>,
        ProducerNode<AsyncMover2, size_t>,
        std::true_type>),
    (std::tuple<
        ConsumerNode<AsyncMover2, size_t>,
        FunctionNode<AsyncMover2, size_t>,
        ProducerNode<AsyncMover2, size_t>,
        std::false_type>),
    (std::tuple<
        ConsumerNode<AsyncMover3, size_t>,
        FunctionNode<AsyncMover3, size_t>,
        ProducerNode<AsyncMover3, size_t>,
        std::true_type>),
    (std::tuple<
        ConsumerNode<AsyncMover3, size_t>,
        FunctionNode<AsyncMover3, size_t>,
        ProducerNode<AsyncMover3, size_t>,
        std::false_type>)) {
  using Consumer = typename std::tuple_element<0, TestType>::type;
  using Function = typename std::tuple_element<1, TestType>::type;
  using Producer = typename std::tuple_element<2, TestType>::type;
  constexpr bool delay = std::tuple_element<3, TestType>::type::value;

  double qwt = GENERATE(1.0, 0.2);
  double rwt = GENERATE(1.0, 0.2);
  double swt = GENERATE(1.0, 0.2);

  size_t rounds = 437;

  std::vector<size_t> v;
  size_t i{0};

  Producer q([&]() {
    if constexpr (delay) {
      std::this_thread::sleep_for(std::chrono::microseconds(
          static_cast<size_t>(qwt * random_us(1234))));
    }
    return i++;
  });

  Function r([&](size_t i) {
    if constexpr (delay) {
      std::this_thread::sleep_for(std::chrono::microseconds(
          static_cast<size_t>(rwt * random_us(1234))));
    }
    return 3 * i;
  });

  Consumer s([&](size_t i) {
    v.push_back(i);
    if constexpr (delay) {
      std::this_thread::sleep_for(std::chrono::microseconds(
          static_cast<size_t>(swt * random_us(1234))));
    }
  });

  attach(q, r);
  attach(r, s);

  auto fun_a = [&]() {
    size_t N = rounds;
    while (N--) {
      q.resume();
    }
  };

  auto fun_b = [&]() {
    size_t N = rounds;
    while (N--) {
      r.resume();
    }
  };

  auto fun_c = [&]() {
    size_t N = rounds;
    while (N--) {
      s.resume();
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
 * Exercise `asynchronous_with_function_node_4()` with and without
 * computation-simulating delays and with weighted delays.
 */
TEMPLATE_TEST_CASE(
    "SimpleNodes: Asynchronous with two function nodes and delay",
    "[simple_nodes]",
    (std::tuple<
        ConsumerNode<AsyncMover2, size_t>,
        FunctionNode<AsyncMover2, size_t>,
        ProducerNode<AsyncMover2, size_t>,
        std::true_type>),
    (std::tuple<
        ConsumerNode<AsyncMover2, size_t>,
        FunctionNode<AsyncMover2, size_t>,
        ProducerNode<AsyncMover2, size_t>,
        std::false_type>),
    (std::tuple<
        ConsumerNode<AsyncMover3, size_t>,
        FunctionNode<AsyncMover3, size_t>,
        ProducerNode<AsyncMover3, size_t>,
        std::true_type>),
    (std::tuple<
        ConsumerNode<AsyncMover3, size_t>,
        FunctionNode<AsyncMover3, size_t>,
        ProducerNode<AsyncMover3, size_t>,
        std::false_type>)) {
  using Consumer = typename std::tuple_element<0, TestType>::type;
  using Function = typename std::tuple_element<1, TestType>::type;
  using Producer = typename std::tuple_element<2, TestType>::type;
  constexpr bool delay = std::tuple_element<3, TestType>::type::value;

  double qwt = GENERATE(1.0, 0.2);
  double rwt = GENERATE(1.0, 0.2);
  double swt = GENERATE(1.0, 0.2);
  double twt = GENERATE(1.0, 0.2);

  size_t rounds = 3317;

  std::vector<size_t> v;
  size_t i{0};

  Producer q([&]() {
    if constexpr (delay) {
      std::this_thread::sleep_for(std::chrono::microseconds(
          static_cast<size_t>(qwt * random_us(1234))));
    }
    return i++;
  });

  Function r([&](size_t i) {
    if constexpr (delay) {
      std::this_thread::sleep_for(std::chrono::microseconds(
          static_cast<size_t>(rwt * random_us(1234))));
    }
    return 3 * i;
  });

  Function s([&](size_t i) {
    if constexpr (delay) {
      std::this_thread::sleep_for(std::chrono::microseconds(
          static_cast<size_t>(swt * random_us(1234))));
    }
    return i + 17;
  });

  Consumer t([&](size_t i) {
    v.push_back(i);
    if constexpr (delay) {
      std::this_thread::sleep_for(std::chrono::microseconds(
          static_cast<size_t>(twt * random_us(1234))));
    }
  });

  attach(q, r);
  attach(r, s);
  attach(s, t);

  auto fun_a = [&]() {
    size_t N = rounds;
    while (N--) {
      q.resume();
    }
  };

  auto fun_b = [&]() {
    size_t N = rounds;
    while (N--) {
      r.resume();
    }
  };

  auto fun_c = [&]() {
    size_t N = rounds;
    while (N--) {
      s.resume();
    }
  };

  auto fun_d = [&]() {
    size_t N = rounds;
    while (N--) {
      t.resume();
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
 * Test that we can correctly pass a sequence of integers from producer node
 * to consumer node.  Uses `consumer` object to fill output vector.
 */
TEMPLATE_TEST_CASE(
    "SimpleNodes: Async pass n integers, two nodes",
    "[simple_nodes]",
    (std::tuple<
        ConsumerNode<AsyncMover2, size_t>,
        FunctionNode<AsyncMover2, size_t>,
        ProducerNode<AsyncMover2, size_t>>),
    (std::tuple<
        ConsumerNode<AsyncMover3, size_t>,
        FunctionNode<AsyncMover3, size_t>,
        ProducerNode<AsyncMover3, size_t>>)) {
  using Consumer = typename std::tuple_element<0, TestType>::type;
  //  using Function = typename std::tuple_element<1, TestType>::type;
  using Producer = typename std::tuple_element<2, TestType>::type;

  size_t rounds = GENERATE(0, 1, 2, 5, 3379);
  size_t offset = GENERATE(0, 1, 2, 5);

  [[maybe_unused]] constexpr bool debug = false;

  if (debug)
    rounds = 3;

  std::vector<size_t> input(rounds + offset);
  std::vector<size_t> output(rounds + offset);

  std::iota(input.begin(), input.end(), 19);
  std::fill(output.begin(), output.end(), 0);
  auto i = input.begin();
  auto j = output.begin();

  if (rounds + offset != 0) {
    CHECK(std::equal(input.begin(), input.end(), output.begin()) == false);
  }

  Producer source_node([&i]() { return (*i++); });
  Consumer sink_node(terminal{j});

  auto a = Edge(source_node, sink_node);
  auto source = [&]() { source_node.run_for(rounds); };
  auto sink = [&]() { sink_node.run_for(rounds + offset); };

  auto x = sink_node.get_mover();
  if (debug) {
    x->enable_debug();
  }

  SECTION(
      "a c a c " + std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_a = std::async(std::launch::async, source);
    auto fut_c = std::async(std::launch::async, sink);
    fut_a.get();
    fut_c.get();
  }
  SECTION(
      "a c c a " + std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_a = std::async(std::launch::async, source);
    auto fut_c = std::async(std::launch::async, sink);
    fut_c.get();
    fut_a.get();
  }
  SECTION(
      "c a a c " + std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_c = std::async(std::launch::async, sink);
    auto fut_a = std::async(std::launch::async, source);
    fut_a.get();
    fut_c.get();
  }
  SECTION(
      "c a c a " + std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_c = std::async(std::launch::async, sink);
    auto fut_a = std::async(std::launch::async, source);
    fut_c.get();
    fut_a.get();
  }

  if (!std::equal(input.begin(), i, output.begin())) {
    for (size_t j = 0; j < input.size(); ++j) {
      if (input[j] != output[j]) {
        std::cout << j << " (" << input[j] << ", " << output[j] << ")"
                  << std::endl;
      }
    }
  }

  if (!std::equal(input.begin(), i, output.begin())) {
    auto iter = std::find_first_of(
        input.begin(),
        input.end(),
        output.begin(),
        output.end(),
        std::not_equal_to<size_t>());
    if (iter != input.end()) {
      size_t k = iter - input.begin();
      std::cout << k << " (" << input[k] << ", " << output[k] << ")"
                << std::endl;
    } else {
      std::cout << "this should not happen" << std::endl;
    }
  }
  CHECK(std::distance(input.begin(), i) == static_cast<long>(rounds));

  //  Can't use this with consumer function nodes
  //  CHECK(std::distance(output.begin(), j) == static_cast<long>(rounds));
  CHECK(std::equal(input.begin(), i, output.begin()));
}

/**
 * Test that we can correctly pass a sequence of integers from producer node
 * to consumer node via function node.
 */
TEMPLATE_TEST_CASE(
    "SimpleNodes: Async pass n integers, three nodes",
    "[simple_nodes]",
    (std::tuple<
        ConsumerNode<AsyncMover2, size_t>,
        FunctionNode<AsyncMover2, size_t>,
        ProducerNode<AsyncMover2, size_t>>),
    (std::tuple<
        ConsumerNode<AsyncMover3, size_t>,
        FunctionNode<AsyncMover3, size_t>,
        ProducerNode<AsyncMover3, size_t>>)) {
  using Consumer = typename std::tuple_element<0, TestType>::type;
  using Function = typename std::tuple_element<1, TestType>::type;
  using Producer = typename std::tuple_element<2, TestType>::type;

  size_t rounds = GENERATE(0, 1, 2, 5, 3379);
  size_t offset = GENERATE(0, 1, 2, 5);

  [[maybe_unused]] constexpr bool debug = false;

  if (debug)
    rounds = 3;

  std::vector<size_t> input(rounds + offset);
  std::vector<size_t> output;

  std::iota(input.begin(), input.end(), 19);
  auto i = input.begin();
  auto w = std::back_inserter(output);
  terminal c{w};

  Producer source_node(generators{19});
  Function mid_node([](size_t k) { return k; });
  Consumer sink_node{c};

  auto a = Edge(source_node, mid_node);
  auto b = Edge(mid_node, sink_node);

  auto source = [&]() { source_node.run_for(rounds); };
  auto mid = [&]() { mid_node.run_for(rounds + offset); };
  auto sink = [&]() { sink_node.run_for(rounds); };

  SECTION(
      "test source launch, sink launch, source get, sink get " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_a = std::async(std::launch::async, source);
    auto fut_b = std::async(std::launch::async, mid);
    auto fut_c = std::async(std::launch::async, sink);
    fut_a.get();
    fut_b.get();
    fut_c.get();
  }
  SECTION(
      "test source launch, sink launch, source get, sink get " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_a = std::async(std::launch::async, source);
    auto fut_c = std::async(std::launch::async, sink);
    auto fut_b = std::async(std::launch::async, mid);
    fut_a.get();
    fut_b.get();
    fut_c.get();
  }
  SECTION(
      "test source launch, sink launch, source get, sink get " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_b = std::async(std::launch::async, mid);
    auto fut_a = std::async(std::launch::async, source);
    auto fut_c = std::async(std::launch::async, sink);
    fut_a.get();
    fut_b.get();
    fut_c.get();
  }
  SECTION(
      "test source launch, sink launch, source get, sink get " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_b = std::async(std::launch::async, mid);
    auto fut_c = std::async(std::launch::async, sink);
    auto fut_a = std::async(std::launch::async, source);
    fut_a.get();
    fut_b.get();
    fut_c.get();
  }
  SECTION(
      "test source launch, sink launch, source get, sink get " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_c = std::async(std::launch::async, sink);
    auto fut_b = std::async(std::launch::async, mid);
    auto fut_a = std::async(std::launch::async, source);

    fut_a.get();
    fut_b.get();
    fut_c.get();
  }
  SECTION(
      "test source launch, sink launch, source get, sink get " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_c = std::async(std::launch::async, sink);
    auto fut_a = std::async(std::launch::async, source);
    auto fut_b = std::async(std::launch::async, mid);

    fut_a.get();
    fut_b.get();
    fut_c.get();
  }
  SECTION(
      "test source launch, sink launch, source get, sink "
      "get " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_a = std::async(std::launch::async, source);
    auto fut_b = std::async(std::launch::async, mid);
    auto fut_c = std::async(std::launch::async, sink);
    fut_a.get();
    fut_b.get();
    fut_c.get();
  }
  SECTION(
      "test source launch, sink launch, source get, sink "
      "get " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_a = std::async(std::launch::async, source);
    auto fut_b = std::async(std::launch::async, mid);
    auto fut_c = std::async(std::launch::async, sink);
    fut_a.get();
    fut_c.get();
    fut_b.get();
  }
  SECTION(
      "test source launch, sink launch, source get, sink "
      "get " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_a = std::async(std::launch::async, source);
    auto fut_b = std::async(std::launch::async, mid);
    auto fut_c = std::async(std::launch::async, sink);
    fut_b.get();
    fut_a.get();
    fut_c.get();
  }
  SECTION(
      "test source launch, sink launch, source get, "
      "sink get " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_a = std::async(std::launch::async, source);
    auto fut_b = std::async(std::launch::async, mid);
    auto fut_c = std::async(std::launch::async, sink);
    fut_b.get();
    fut_c.get();
    fut_a.get();
  }
  SECTION(
      "test source launch, sink launch, source get, "
      "sink get " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_a = std::async(std::launch::async, source);
    auto fut_b = std::async(std::launch::async, mid);
    auto fut_c = std::async(std::launch::async, sink);
    fut_c.get();
    fut_a.get();
    fut_b.get();
  }
  SECTION(
      "test source launch, sink launch, source "
      "get, sink get " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_a = std::async(std::launch::async, source);
    auto fut_b = std::async(std::launch::async, mid);
    auto fut_c = std::async(std::launch::async, sink);
    fut_c.get();
    fut_b.get();
    fut_a.get();
  }

  if (!std::equal(input.begin(), i, output.begin())) {
    for (size_t j = 0; j < input.size(); ++j) {
      if (input[j] != output[j]) {
        std::cout << j << " (" << input[j] << ", " << output[j] << ")"
                  << std::endl;
      }
    }
  }

  if (!std::equal(input.begin(), i, output.begin())) {
    auto iter = std::find_first_of(
        input.begin(),
        input.end(),
        output.begin(),
        output.end(),
        std::not_equal_to<size_t>());
    if (iter != input.end()) {
      size_t k = iter - input.begin();
      std::cout << k << " (" << input[k] << ", " << output[k] << ")"
                << std::endl;
    } else {
      std::cout << "this should not happen" << std::endl;
    }
  }

  CHECK(size(output) == rounds);

  //  Can't use this with generator and consumer function nodes
  //  CHECK(std::distance(input.begin(), i) == static_cast<long>(rounds));
  CHECK(std::equal(input.begin(), i, output.begin()));
}

/**
 * Test that we can correctly pass a sequence of integers from producer node
 * to consumer node via function node.
 */
TEMPLATE_TEST_CASE(
    "SimpleNodes: Async pass n "
    "integers, four "
    "nodes",
    "[simple_nodes]",
    (std::tuple<
        ConsumerNode<AsyncMover2, size_t>,
        FunctionNode<AsyncMover2, size_t>,
        ProducerNode<AsyncMover2, size_t>>),
    (std::tuple<
        ConsumerNode<AsyncMover3, size_t>,
        FunctionNode<AsyncMover3, size_t>,
        ProducerNode<AsyncMover3, size_t>>)) {
  using Consumer = typename std::tuple_element<0, TestType>::type;
  using Function = typename std::tuple_element<1, TestType>::type;
  using Producer = typename std::tuple_element<2, TestType>::type;

  size_t rounds = GENERATE(0, 1, 2, 5, 3379);
  size_t offset = GENERATE(0, 1, 2, 5);

  [[maybe_unused]] constexpr bool debug = false;

  if (debug)
    rounds = 3;

  std::vector<size_t> input(rounds + offset);
  std::vector<size_t> output(rounds + offset);

  std::iota(input.begin(), input.end(), 19);
  std::fill(output.begin(), output.end(), 0);
  auto i = input.begin();
  auto j = output.begin();

  if (rounds + offset != 0) {
    CHECK(std::equal(input.begin(), input.end(), output.begin()) == false);
  }

  Producer source_node([&i]() { return (*i++); });
  Function mid_node1([](size_t k) { return k; });
  Function mid_node2([](size_t k) { return k; });
  Consumer sink_node([&j](size_t k) { *j++ = k; });

  auto a = Edge(source_node, mid_node1);
  auto b = Edge(mid_node1, mid_node2);
  auto c = Edge(mid_node2, sink_node);

  auto x = sink_node.get_mover();
  if (debug) {
    x->enable_debug();
  }

  auto source = [&]() { source_node.run_for(rounds); };
  auto mid1 = [&]() { mid_node1.run_for(rounds + offset); };
  auto mid2 = [&]() { mid_node2.run_for(rounds); };
  auto sink = [&]() { sink_node.run_for(rounds + offset); };

  SECTION(
      "dcba abcd " + std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_d = std::async(std::launch::async, mid2);
    auto fut_c = std::async(std::launch::async, sink);
    auto fut_b = std::async(std::launch::async, mid1);
    auto fut_a = std::async(std::launch::async, source);
    fut_a.get();
    fut_b.get();
    fut_c.get();
    fut_d.get();
  }
  SECTION(
      "abcd dcba" + std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_a = std::async(std::launch::async, source);
    auto fut_b = std::async(std::launch::async, mid1);
    auto fut_c = std::async(std::launch::async, sink);
    auto fut_d = std::async(std::launch::async, mid2);
    fut_d.get();
    fut_c.get();
    fut_b.get();
    fut_a.get();
  }
  SECTION(
      "dcba dcba " + std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_d = std::async(std::launch::async, mid2);
    auto fut_c = std::async(std::launch::async, sink);
    auto fut_b = std::async(std::launch::async, mid1);
    auto fut_a = std::async(std::launch::async, source);
    fut_d.get();
    fut_c.get();
    fut_b.get();
    fut_a.get();
  }
  SECTION(
      "abcd abcd " + std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_a = std::async(std::launch::async, source);
    auto fut_b = std::async(std::launch::async, mid1);
    auto fut_c = std::async(std::launch::async, sink);
    auto fut_d = std::async(std::launch::async, mid2);
    fut_a.get();
    fut_b.get();
    fut_c.get();
    fut_d.get();
  }

  if (!std::equal(input.begin(), i, output.begin())) {
    for (size_t j = 0; j < input.size(); ++j) {
      if (input[j] != output[j]) {
        std::cout << j
                  << " "
                     "("
                  << input[j]
                  << ","
                     " "
                  << output[j] << ")" << std::endl;
      }
    }
  }

  if (!std::equal(input.begin(), i, output.begin())) {
    auto iter = std::find_first_of(
        input.begin(),
        input.end(),
        output.begin(),
        output.end(),
        std::not_equal_to<size_t>());
    if (iter != input.end()) {
      size_t k = iter - input.begin();
      std::cout << k << " (" << input[k] << ", " << output[k] << ")"
                << std::endl;
    } else {
      std::cout << "This should not ever happen" << std::endl;
    }
  }
  CHECK(std::distance(input.begin(), i) == static_cast<long>(rounds));
  CHECK(std::distance(output.begin(), j) == static_cast<long>(rounds));
  CHECK(std::equal(input.begin(), i, output.begin()));
}

TEST_CASE("mimo_node: Verify various API approaches", "[segmented_mimo]") {
  [[maybe_unused]] GeneralFunctionNode<
      AsyncMover2,
      std::tuple<size_t, int>,
      AsyncMover3,
      std::tuple<size_t, double>>
      x{};
  [[maybe_unused]] GeneralFunctionNode<
      AsyncMover2,
      std::tuple<int>,
      AsyncMover3,
      std::tuple<size_t, double>>
      y{};
  [[maybe_unused]] GeneralFunctionNode<
      AsyncMover2,
      std::tuple<char*>,
      AsyncMover3,
      std::tuple<size_t, std::tuple<int, float>>>
      z{};
  [[maybe_unused]] GeneralFunctionNode<
      AsyncMover2,
      std::tuple<int, char, double, double, double>,
      AsyncMover3,
      std::tuple<int>>
      a{};
}

TEST_CASE(
    "GeneralFunctionNode: Verify construction with simple function",
    "[segmented_mimo]") {
  [[maybe_unused]] GeneralFunctionNode<
      AsyncMover2,
      std::tuple<size_t>,
      AsyncMover3,
      std::tuple<size_t>>
      x{[](std::tuple<size_t>, std::tuple<size_t>) {}};
}
