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
 * Tests the nodes classes, `SourceNode`, `SinkNode`, and `FunctionNode`.
 */

#include "unit_nodes.h"
#include <future>
#include "experimental/tiledb/common/dag/edge/edge.h"
#include "experimental/tiledb/common/dag/nodes/consumer.h"
#include "experimental/tiledb/common/dag/nodes/generator.h"
#include "experimental/tiledb/common/dag/nodes/nodes.h"
#include "experimental/tiledb/common/dag/state_machine/test/types.h"
#include "experimental/tiledb/common/dag/utils/print_types.h"

using namespace tiledb::common;

/**
 * Verify various API approaches
 */
TEST_CASE("Nodes: Verify various API approaches", "[nodes]") {
  ProducerNode<AsyncMover3, size_t> a;
  ProducerNode<AsyncMover3, size_t> b([]() { return 0UL; });
  ConsumerNode<AsyncMover3, size_t> c([](size_t) {});

  ProducerNode<AsyncMover2, size_t> d;
  ProducerNode<AsyncMover2, size_t> e([]() { return 0UL; });
  ConsumerNode<AsyncMover2, size_t> f([](size_t) {});
}

/**
 * Verify connecting nodes with edges
 */
TEST_CASE("Nodes: Verify connecting with edges", "[nodes]") {
  ProducerNode<AsyncMover3, size_t> a;
  ProducerNode<AsyncMover3, size_t> b([]() { return 0UL; });
  ConsumerNode<AsyncMover3, size_t> c([](size_t) {});
  Edge g{b, c};

  ProducerNode<AsyncMover2, size_t> d;
  ProducerNode<AsyncMover2, size_t> e([]() { return 0UL; });
  ConsumerNode<AsyncMover2, size_t> f([](size_t) {});
  Edge h{e, f};
}

/**
 * Verify enable_if for screening constructor calls.  Provides much better error
 * messages in the case of failure.
 */
TEST_CASE("Nodes: Verify connecting with edges, failing", "[nodes]") {
  // These will fail, with good diagnostics.  This should be commented out from
  // time to time and tested by hand that we get the right error messages.
  //   ProducerNode<AsyncMover3, size_t> b{0UL};
  //   ConsumerNode<AsyncMover3, size_t> c{-1.1};
  //   Edge g{b, c};

  ProducerNode<AsyncMover2, size_t> d;
  ProducerNode<AsyncMover2, size_t> e([]() { return 0UL; });
  ConsumerNode<AsyncMover2, size_t> f([](size_t) {});
  Edge h{e, f};
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
TEST_CASE("Nodes: Verify numerous API approaches, with edges", "[nodes]") {
  SECTION("function") {
    ProducerNode<AsyncMover3, size_t> b{dummy_source};
    ConsumerNode<AsyncMover3, size_t> c{dummy_sink};

    ProducerNode<AsyncMover2, size_t> e{dummy_source};
    ConsumerNode<AsyncMover2, size_t> f{dummy_sink};

    Edge g{b, c};
    Edge h{e, f};
  }

  SECTION("lambda") {
    auto dummy_source_lambda = []() { return 0UL; };
    auto dummy_sink_lambda = [](size_t) {};

    ProducerNode<AsyncMover3, size_t> b{dummy_source_lambda};
    ConsumerNode<AsyncMover3, size_t> c{dummy_sink_lambda};

    ProducerNode<AsyncMover2, size_t> e{dummy_source_lambda};
    ConsumerNode<AsyncMover2, size_t> f{dummy_sink_lambda};

    Edge g{b, c};
    Edge h{e, f};
  }

  SECTION("inline lambda") {
    ProducerNode<AsyncMover3, size_t> b([]() { return 0UL; });
    ConsumerNode<AsyncMover3, size_t> c([](size_t) {});

    ProducerNode<AsyncMover2, size_t> e([]() { return 0UL; });
    ConsumerNode<AsyncMover2, size_t> f([](size_t) {});

    Edge g{b, c};
    Edge h{e, f};
  }

  SECTION("function object") {
    auto a = dummy_source_class{};
    dummy_sink_class d{};

    ProducerNode<AsyncMover3, size_t> b{a};
    ConsumerNode<AsyncMover3, size_t> c{d};

    ProducerNode<AsyncMover2, size_t> e{a};
    ConsumerNode<AsyncMover2, size_t> f{d};

    Edge g{b, c};
    Edge h{e, f};
  }

  SECTION("inline function object") {
    ProducerNode<AsyncMover3, size_t> b{dummy_source_class{}};
    ConsumerNode<AsyncMover3, size_t> c{dummy_sink_class{}};

    ProducerNode<AsyncMover2, size_t> e{dummy_source_class{}};
    ConsumerNode<AsyncMover2, size_t> f{dummy_sink_class{}};

    Edge g{b, c};
    Edge h{e, f};
  }

  SECTION("bind") {
    double x = 0.01;
    float y = -0.001;
    int z = 8675309;

    auto a = std::bind(dummy_bind_source, x);
    auto d = std::bind(dummy_bind_sink, y, std::placeholders::_1, z);

    ProducerNode<AsyncMover3, size_t> b{a};
    ConsumerNode<AsyncMover3, size_t> c{d};

    ProducerNode<AsyncMover2, size_t> e{a};
    ConsumerNode<AsyncMover2, size_t> f{d};

    Edge g{b, c};
    Edge h{e, f};
  }

  SECTION("inline bind") {
    double x = 0.01;
    float y = -0.001;
    int z = 8675309;

    ProducerNode<AsyncMover3, size_t> b{std::bind(dummy_bind_source, x)};
    ConsumerNode<AsyncMover3, size_t> c{
        std::bind(dummy_bind_sink, y, std::placeholders::_1, z)};

    ProducerNode<AsyncMover2, size_t> e{std::bind(dummy_bind_source, x)};
    ConsumerNode<AsyncMover2, size_t> f{
        std::bind(dummy_bind_sink, y, std::placeholders::_1, z)};

    Edge g{b, c};
    Edge h{e, f};
  }

  SECTION("bind with move") {
    double x = 0.01;
    float y = -0.001;
    int z = 8675309;

    auto a = std::bind(dummy_bind_source, std::move(x));
    auto d = std::bind(dummy_bind_sink, y, std::placeholders::_1, std::move(z));

    ProducerNode<AsyncMover3, size_t> b{std::move(a)};
    ConsumerNode<AsyncMover3, size_t> c{std::move(d)};

    ProducerNode<AsyncMover2, size_t> e{std::move(a)};
    ConsumerNode<AsyncMover2, size_t> f{std::move(d)};

    Edge g{b, c};
    Edge h{e, f};
  }
}

/**
 * Verify initializing ProducerNode, FunctionNode, and ConsumerNode with
 * function, lambda, in-line lambda, function object, bind, and rvalue bind.
 * (This is a repeat of the previous test, but modified to include a
 * FunctionNode.)
 */
TEST_CASE(
    "Nodes: Verify various API approaches, including FunctionNode", "[nodes]") {
  SECTION("function") {
    ProducerNode<AsyncMover3, size_t> a{dummy_source};
    FunctionNode<AsyncMover3, size_t> b{dummy_function};
    ConsumerNode<AsyncMover3, size_t> c{dummy_sink};

    ProducerNode<AsyncMover2, size_t> d{dummy_source};
    FunctionNode<AsyncMover2, size_t> e{dummy_function};
    ConsumerNode<AsyncMover2, size_t> f{dummy_sink};

    Edge g{a, b};
    Edge h{b, c};

    Edge i{d, e};
    Edge j{e, f};
  }

  SECTION("lambda") {
    auto dummy_source_lambda = []() { return 0UL; };
    auto dummy_function_lambda = [](size_t) { return 0UL; };
    auto dummy_sink_lambda = [](size_t) {};

    ProducerNode<AsyncMover3, size_t> a{dummy_source_lambda};
    FunctionNode<AsyncMover3, size_t> b{dummy_function_lambda};
    ConsumerNode<AsyncMover3, size_t> c{dummy_sink_lambda};

    ProducerNode<AsyncMover2, size_t> d{dummy_source_lambda};
    FunctionNode<AsyncMover2, size_t> e{dummy_function_lambda};
    ConsumerNode<AsyncMover2, size_t> f{dummy_sink_lambda};

    Edge g{a, b};
    Edge h{b, c};

    Edge i{d, e};
    Edge j{e, f};
  }

  SECTION("inline lambda") {
    ProducerNode<AsyncMover3, size_t> a([]() { return 0UL; });
    FunctionNode<AsyncMover3, size_t> b([](size_t) { return 0UL; });
    ConsumerNode<AsyncMover3, size_t> c([](size_t) {});

    ProducerNode<AsyncMover2, size_t> d([]() { return 0UL; });
    FunctionNode<AsyncMover2, size_t> e([](size_t) { return 0UL; });
    ConsumerNode<AsyncMover2, size_t> f([](size_t) {});

    Edge g{a, b};
    Edge h{b, c};

    Edge i{d, e};
    Edge j{e, f};
  }

  SECTION("function object") {
    auto ac = dummy_source_class{};
    dummy_function_class fc{};
    dummy_sink_class dc{};

    ProducerNode<AsyncMover3, size_t> a{ac};
    FunctionNode<AsyncMover3, size_t> b{fc};
    ConsumerNode<AsyncMover3, size_t> c{dc};

    ProducerNode<AsyncMover2, size_t> d{ac};
    FunctionNode<AsyncMover2, size_t> e{fc};
    ConsumerNode<AsyncMover2, size_t> f{dc};

    Edge g{a, b};
    Edge h{b, c};

    Edge i{d, e};
    Edge j{e, f};
  }

  SECTION("inline function object") {
    ProducerNode<AsyncMover3, size_t> a{dummy_source_class{}};
    FunctionNode<AsyncMover3, size_t> b{dummy_function_class{}};
    ConsumerNode<AsyncMover3, size_t> c{dummy_sink_class{}};

    ProducerNode<AsyncMover2, size_t> d{dummy_source_class{}};
    FunctionNode<AsyncMover2, size_t> e{dummy_function_class{}};
    ConsumerNode<AsyncMover2, size_t> f{dummy_sink_class{}};

    Edge g{a, b};
    Edge h{b, c};

    Edge i{d, e};
    Edge j{e, f};
  }

  SECTION("bind") {
    double x = 0.01;
    float y = -0.001;
    int z = 8675309;

    auto ac = std::bind(dummy_bind_source, x);
    auto dc = std::bind(dummy_bind_sink, y, std::placeholders::_1, z);
    auto fc = std::bind(dummy_bind_function, x, y, std::placeholders::_1);

    ProducerNode<AsyncMover3, size_t> a{ac};
    FunctionNode<AsyncMover3, size_t> b{fc};
    ConsumerNode<AsyncMover3, size_t> c{dc};

    ProducerNode<AsyncMover2, size_t> d{ac};
    FunctionNode<AsyncMover2, size_t> e{fc};
    ConsumerNode<AsyncMover2, size_t> f{dc};

    Edge g{a, b};
    Edge h{b, c};

    Edge i{d, e};
    Edge j{e, f};
  }

  SECTION("inline bind") {
    double x = 0.01;
    float y = -0.001;
    int z = 8675309;

    ProducerNode<AsyncMover3, size_t> a{std::bind(dummy_bind_source, x)};
    FunctionNode<AsyncMover3, size_t> b{
        std::bind(dummy_bind_function, x, y, std::placeholders::_1)};
    ConsumerNode<AsyncMover3, size_t> c{
        std::bind(dummy_bind_sink, y, std::placeholders::_1, z)};

    ProducerNode<AsyncMover2, size_t> d{std::bind(dummy_bind_source, x)};
    FunctionNode<AsyncMover2, size_t> e{
        std::bind(dummy_bind_function, x, y, std::placeholders::_1)};
    ConsumerNode<AsyncMover2, size_t> f{
        std::bind(dummy_bind_sink, y, std::placeholders::_1, z)};

    Edge i{a, b};
    Edge j{b, c};
    Edge g{d, e};
    Edge h{e, f};
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

    ProducerNode<AsyncMover3, size_t> a{std::move(ac)};
    FunctionNode<AsyncMover3, size_t> b{std::move(fc)};
    ConsumerNode<AsyncMover3, size_t> c{std::move(dc)};

    ProducerNode<AsyncMover2, size_t> d{std::move(ac)};
    FunctionNode<AsyncMover2, size_t> e{std::move(fc)};
    ConsumerNode<AsyncMover2, size_t> f{std::move(dc)};

    Edge i{a, b};
    Edge j{b, c};
    Edge g{d, e};
    Edge h{e, f};
  }
}

/**
 * Verify initializing ProducerNode, FunctionNode, and ConsumerNode with
 * function templates, lambda templates, in-line lambda, function template
 * object, bind, and rvalue bind.  (This is a repeat of the previous test, but
 * modified to include a FunctionNode.)
 */
TEST_CASE(
    "Nodes: Verify various API approaches using templates, including "
    "FunctionNode",
    "[nodes]") {
  SECTION("function") {
    ProducerNode<AsyncMover3, size_t> a{dummy_source_t<size_t>};
    FunctionNode<AsyncMover3, size_t, AsyncMover3, double> b{
        dummy_function_t<size_t, double>};
    ConsumerNode<AsyncMover3, double> c{dummy_sink_t<double>};

    ProducerNode<AsyncMover2, double> d{dummy_source_t<double>};
    FunctionNode<AsyncMover2, double, DebugMover2, char*> e{
        dummy_function_t<double, char*>};
    ConsumerNode<DebugMover2, char*> f{dummy_sink_t<char*>};

    Edge g{a, b};
    Edge h{b, c};

    Edge i{d, e};
    Edge j{e, f};
  }

  SECTION("function object") {
    auto ac = dummy_source_class_t<size_t>{};
    dummy_function_class_t<size_t> fc{};
    dummy_sink_class_t<size_t> dc{};

    ProducerNode<AsyncMover3, size_t> a{ac};
    FunctionNode<AsyncMover3, size_t> b{fc};
    ConsumerNode<AsyncMover3, size_t> c{dc};

    ProducerNode<AsyncMover2, size_t> d{ac};
    FunctionNode<AsyncMover2, size_t> e{fc};
    ConsumerNode<AsyncMover2, size_t> f{dc};

    Edge g{a, b};
    Edge h{b, c};

    Edge i{d, e};
    Edge j{e, f};
  }

  SECTION("inline function object") {
    ProducerNode<AsyncMover3, size_t> a{dummy_source_class_t<size_t>{}};
    FunctionNode<AsyncMover3, size_t> b{dummy_function_class_t<size_t>{}};
    ConsumerNode<AsyncMover3, size_t> c{dummy_sink_class_t<size_t>{}};

    ProducerNode<AsyncMover2, size_t> d{dummy_source_class_t<size_t>{}};
    FunctionNode<AsyncMover2, size_t> e{dummy_function_class_t<size_t>{}};
    ConsumerNode<AsyncMover2, size_t> f{dummy_sink_class_t<size_t>{}};

    Edge g{a, b};
    Edge h{b, c};

    Edge i{d, e};
    Edge j{e, f};
  }

  SECTION("bind") {
    double x = 0.01;
    float y = -0.001;
    int z = 8675309;

    auto ac = std::bind(dummy_bind_source_t<size_t>, x);
    auto dc = std::bind(dummy_bind_sink_t<size_t>, y, std::placeholders::_1, z);
    auto fc =
        std::bind(dummy_bind_function_t<size_t>, x, y, std::placeholders::_1);

    ProducerNode<AsyncMover3, size_t> a{ac};
    FunctionNode<AsyncMover3, size_t> b{fc};
    ConsumerNode<AsyncMover3, size_t> c{dc};

    ProducerNode<AsyncMover2, size_t> d{ac};
    FunctionNode<AsyncMover2, size_t> e{fc};
    ConsumerNode<AsyncMover2, size_t> f{dc};

    Edge g{a, b};
    Edge h{b, c};

    Edge i{d, e};
    Edge j{e, f};
  }

  SECTION("inline bind") {
    double x = 0.01;
    float y = -0.001;
    int z = 8675309;

    ProducerNode<AsyncMover3, size_t> a{
        std::bind(dummy_bind_source_t<size_t>, x)};
    FunctionNode<AsyncMover3, size_t> b{
        std::bind(dummy_bind_function_t<size_t>, x, y, std::placeholders::_1)};
    ConsumerNode<AsyncMover3, size_t> c{
        std::bind(dummy_bind_sink_t<size_t>, y, std::placeholders::_1, z)};

    ProducerNode<AsyncMover2, size_t> d{
        std::bind(dummy_bind_source_t<size_t>, x)};
    FunctionNode<AsyncMover2, size_t> e{
        std::bind(dummy_bind_function_t<size_t>, x, y, std::placeholders::_1)};
    ConsumerNode<AsyncMover2, size_t> f{
        std::bind(dummy_bind_sink_t<size_t>, y, std::placeholders::_1, z)};

    Edge i{a, b};
    Edge j{b, c};
    Edge g{d, e};
    Edge h{e, f};
  }

  SECTION("bind with move") {
    double x = 0.01;
    float y = -0.001;
    int z = 8675309;

    auto ac = std::bind(dummy_bind_source_t<size_t>, std::move(x));
    auto dc = std::bind(
        dummy_bind_sink_t<size_t>,
        std::move(y),
        std::placeholders::_1,
        std::move(z));
    auto fc = std::bind(
        dummy_bind_function_t<size_t>,
        std::move(x),
        std::move(y),
        std::placeholders::_1);

    ProducerNode<AsyncMover3, size_t> a{std::move(ac)};
    FunctionNode<AsyncMover3, size_t> b{std::move(fc)};
    ConsumerNode<AsyncMover3, size_t> c{std::move(dc)};

    ProducerNode<AsyncMover2, size_t> d{std::move(ac)};
    FunctionNode<AsyncMover2, size_t> e{std::move(fc)};
    ConsumerNode<AsyncMover2, size_t> f{std::move(dc)};

    Edge i{a, b};
    Edge j{b, c};
    Edge g{d, e};
    Edge h{e, f};
  }
}

/**
 * Test of producer and consumer functions.  The producer generates a an
 * increasing sequence of numbers starting from 0 and incrementing by 1 on each
 * invocation.  The consumer appends its input to a specified output iterator --
 * in this case, a back inserter to an `std::vector`.
 *
 */
TEST_CASE("Nodes: Producer and consumer functions and nodes", "[nodes]") {
  size_t N = 37;

  generator g;

  std::vector<size_t> v;
  auto w = std::back_inserter(v);
  consumer c{w};

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
    ConsumerNode<AsyncMover2, size_t> r(c);

    ProducerNode<AsyncMover2, size_t> p(g);
    ProducerNode<AsyncMover2, size_t> q([]() { return 0UL; });
  }
}

/**
 * Test that we can attach a producer and consumer node to each other.
 */
TEST_CASE("Nodes: Attach producer and consumer nodes", "[nodes]") {
  size_t N = 41;

  using Producer = ProducerNode<AsyncMover2, size_t>;

  using Consumer = ConsumerNode<AsyncMover2, size_t>;

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
    generator g(N);

    std::vector<size_t> v;
    std::back_insert_iterator<std::vector<size_t>> w(v);
    consumer<std::back_insert_iterator<std::vector<size_t>>> c(w);

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
TEST_CASE("Nodes: Pass some data, two attachment orders", "[nodes]") {
  generator g;

  std::vector<size_t> v;
  std::back_insert_iterator<std::vector<size_t>> w(v);
  consumer<std::back_insert_iterator<std::vector<size_t>>> c(w);

  ConsumerNode<AsyncMover2, size_t> r(c);
  ProducerNode<AsyncMover2, size_t> p(g);

  SECTION("Attach p to r") {
    attach(p, r);
  }
  SECTION("Attach r to p") {
    attach(p, r);
  }

  p.run();
  r.run();

  CHECK(v.size() == 1);

  p.run();
  r.run();

  CHECK(v.size() == 2);

  p.run();
  r.run();

  CHECK(v.size() == 3);

  CHECK(v[0] == 0);
  CHECK(v[1] == 1);
  CHECK(v[2] == 2);
}

/**
 * Test that we can asynchronously send data from a producer to an attached
 * consumer.
 */
TEST_CASE("Nodes: Asynchronously pass some data", "[nodes]") {
  size_t rounds = 423;

  generator g{0};

  std::vector<size_t> v;
  std::back_insert_iterator<std::vector<size_t>> w(v);
  consumer<std::back_insert_iterator<std::vector<size_t>>> c(w);

  ConsumerNode<AsyncMover2, size_t> r(c);
  ProducerNode<AsyncMover2, size_t> p(g);

  attach(p, r);

  auto fun_a = [&]() {
    size_t N = rounds;
    while (N--) {
      p.run();
    }
  };

  auto fun_b = [&]() {
    size_t N = rounds;
    while (N--) {
      r.run();
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
TEST_CASE("Nodes: Asynchronously pass some data, random delays", "[nodes]") {
  size_t rounds = 433;

  std::vector<size_t> v;
  size_t i{0};

  ConsumerNode<AsyncMover2, size_t> r([&](size_t i) {
    v.push_back(i);
    std::this_thread::sleep_for(std::chrono::microseconds(random_us(1234)));
  });
  ProducerNode<AsyncMover2, size_t> p([&]() {
    std::this_thread::sleep_for(std::chrono::microseconds(random_us(1234)));
    return i++;
  });

  attach(p, r);

  auto fun_a = [&]() {
    size_t N = rounds;
    while (N--) {
      p.run();
    }
  };

  auto fun_b = [&]() {
    size_t N = rounds;
    while (N--) {
      r.run();
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
TEST_CASE("Nodes: Attach to function node", "[nodes]") {
  ProducerNode<AsyncMover2, size_t> q([]() { return 0UL; });

  FunctionNode<AsyncMover2, size_t> r([](size_t) { return 0UL; });

  ConsumerNode<AsyncMover2, size_t> s([](size_t) {});

  attach(q, r);
  attach(r, s);
}

/**
 * Test that we can synchronously send data from a producer to an attached
 * function node and then to consumer.
 */
TEST_CASE(
    "Nodes: Manuallay pass some data in a chain with function node",
    "[nodes]") {
  size_t i{0UL};
  ProducerNode<AsyncMover2, size_t> q([&]() { return i++; });

  FunctionNode<AsyncMover2, size_t> r([&](size_t i) { return 2 * i; });

  std::vector<size_t> v;
  ConsumerNode<AsyncMover2, size_t> s([&](size_t i) { v.push_back(i); });

  attach(q, r);
  attach(r, s);

  q.run();
  r.run();
  s.run();

  CHECK(v.size() == 1);

  q.run();
  r.run();
  s.run();

  CHECK(v.size() == 2);

  q.run();
  r.run();
  s.run();

  CHECK(v.size() == 3);

  CHECK(v[0] == 0);
  CHECK(v[1] == 2);
  CHECK(v[2] == 4);
}

/**
 * Test that we can asynchronously send data from a producer to an attached
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

  ProducerNode<AsyncMover2, size_t> q([&]() {
    if constexpr (delay) {
      std::this_thread::sleep_for(std::chrono::microseconds(
          static_cast<size_t>(qwt * random_us(1234))));
    }
    return i++;
  });

  FunctionNode<AsyncMover2, size_t> r([&](size_t i) {
    if constexpr (delay) {
      std::this_thread::sleep_for(std::chrono::microseconds(
          static_cast<size_t>(rwt * random_us(1234))));
    }
    return 3 * i;
  });

  ConsumerNode<AsyncMover2, size_t> s([&](size_t i) {
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
      q.run();
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
TEST_CASE("Nodes: Asynchronous with function node and delay", "[nodes]") {
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
 * Test that we can asynchronously send data from a producer to an attached
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
  size_t rounds = 3317;

  std::vector<size_t> v;
  size_t i{0};

  ProducerNode<AsyncMover2, size_t> q([&]() {
    if constexpr (delay) {
      std::this_thread::sleep_for(std::chrono::microseconds(
          static_cast<size_t>(qwt * random_us(1234))));
    }
    return i++;
  });

  FunctionNode<AsyncMover2, size_t> r([&](size_t i) {
    if constexpr (delay) {
      std::this_thread::sleep_for(std::chrono::microseconds(
          static_cast<size_t>(rwt * random_us(1234))));
    }
    return 3 * i;
  });

  FunctionNode<AsyncMover2, size_t> s([&](size_t i) {
    if constexpr (delay) {
      std::this_thread::sleep_for(std::chrono::microseconds(
          static_cast<size_t>(swt * random_us(1234))));
    }
    return i + 17;
  });

  ConsumerNode<AsyncMover2, size_t> t([&](size_t i) {
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
      q.run();
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
      t.run();
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
TEST_CASE("Nodes: Asynchronous with two function nodes and delay", "[nodes]") {
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
  SECTION("With delay, increasing") {
    asynchronous_with_function_node_4<true>(0.21, 0.33, 0.77, 1.3);
  }
  SECTION("With delay, decreasing") {
    asynchronous_with_function_node_4<true>(1.3, 0.77, 0.33, 0.21);
  }
}

/**
 * Test that we can correctly pass a sequence of integers from producer node to
 * consumer node.  Uses `consumer` object to fill output vector.
 */
TEST_CASE("Nodes: Async pass n integers, two nodes, two stage", "[nodes]") {
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

  ProducerNode<AsyncMover2, size_t> source_node([&i]() { return (*i++); });
  ConsumerNode<AsyncMover2, size_t> sink_node(consumer{j});

  auto a = Edge(source_node, sink_node);
  auto source = [&]() { source_node.run(rounds); };
  auto sink = [&]() { sink_node.run(rounds + offset); };

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
 * Test that we can correctly pass a sequence of integers from producer node to
 * consumer node.
 */
TEST_CASE("Nodes: Async pass n integers, two nodes, three stage", "[nodes]") {
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

  if (rounds + offset != 0) {
    CHECK(std::equal(input.begin(), input.end(), output.begin()) == false);
  }

  ProducerNode<AsyncMover3, size_t> source_node([&i]() { return (*i++); });
  ConsumerNode<AsyncMover3, size_t> sink_node(consumer{output.begin()});

  Edge(source_node, sink_node);

  auto source = [&]() { source_node.run(rounds); };
  auto sink = [&]() { sink_node.run(rounds + offset); };

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

  //  Can't use this with function nodes
  //  CHECK(std::distance(output.begin(), j) == static_cast<long>(rounds));
  CHECK(std::equal(input.begin(), i, output.begin()));
}

/**
 * Test that we can correctly pass a sequence of integers from producer node to
 * consumer node via function node.
 */
TEST_CASE("Nodes: Async pass n integers, three nodes, two stage", "[nodes]") {
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
  consumer c{w};

  ProducerNode<AsyncMover2, size_t> source_node(generator{19});
  FunctionNode<AsyncMover2, size_t> mid_node([](size_t k) { return k; });
  ConsumerNode<AsyncMover2, size_t> sink_node{c};

  auto a = Edge(source_node, mid_node);
  auto b = Edge(mid_node, sink_node);

  auto source = [&]() { source_node.run(rounds); };
  auto mid = [&]() { mid_node.run(rounds + offset); };
  auto sink = [&]() { sink_node.run(rounds); };

  SECTION(
      "test source launch, sink launch, source get, sink get" +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_a = std::async(std::launch::async, source);
    auto fut_b = std::async(std::launch::async, mid);
    auto fut_c = std::async(std::launch::async, sink);
    fut_a.get();
    fut_b.get();
    fut_c.get();
  }
  SECTION(
      "test source launch, sink launch, source get, sink get" +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_a = std::async(std::launch::async, source);
    auto fut_c = std::async(std::launch::async, sink);
    auto fut_b = std::async(std::launch::async, mid);
    fut_a.get();
    fut_b.get();
    fut_c.get();
  }
  SECTION(
      "test source launch, sink launch, source get, sink get" +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_b = std::async(std::launch::async, mid);
    auto fut_a = std::async(std::launch::async, source);
    auto fut_c = std::async(std::launch::async, sink);
    fut_a.get();
    fut_b.get();
    fut_c.get();
  }
  SECTION(
      "test source launch, sink launch, source get, sink get" +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_b = std::async(std::launch::async, mid);
    auto fut_c = std::async(std::launch::async, sink);
    auto fut_a = std::async(std::launch::async, source);
    fut_a.get();
    fut_b.get();
    fut_c.get();
  }
  SECTION(
      "test source launch, sink launch, source get, sink get" +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_c = std::async(std::launch::async, sink);
    auto fut_b = std::async(std::launch::async, mid);
    auto fut_a = std::async(std::launch::async, source);

    fut_a.get();
    fut_b.get();
    fut_c.get();
  }
  SECTION(
      "test source launch, sink launch, source get, sink get" +
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
      "get" +
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
      "get" +
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
      "get" +
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
      "sink get" +
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
      "sink get" +
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
      "get, sink get" +
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
 * Test that we can correctly pass a sequence of integers from producer node to
 * consumer node.
 */
TEST_CASE(
    "Nodes: Async pass n integers, three nodes, "
    "three stage",
    "[nodes]") {
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

  ProducerNode<AsyncMover3, size_t> source_node(generator{19});
  FunctionNode<AsyncMover3, size_t> mid_node([](size_t k) { return k; });
  ConsumerNode<AsyncMover3, size_t> sink_node(consumer<decltype(j), size_t>{j});

  Edge(source_node, mid_node);
  Edge(mid_node, sink_node);

  auto source = [&]() { source_node.run(rounds); };
  auto mid = [&]() { mid_node.run(rounds + offset); };
  auto sink = [&]() { sink_node.run(rounds); };

  SECTION(
      "test source launch, sink launch, source "
      "get, sink get" +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_a = std::async(std::launch::async, source);
    auto fut_b = std::async(std::launch::async, mid);
    auto fut_c = std::async(std::launch::async, sink);
    fut_a.get();
    fut_b.get();
    fut_c.get();
  }
  SECTION(
      "test source launch, sink launch, source "
      "get, sink get" +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_a = std::async(std::launch::async, source);
    auto fut_c = std::async(std::launch::async, sink);
    auto fut_b = std::async(std::launch::async, mid);
    fut_a.get();
    fut_b.get();
    fut_c.get();
  }
  SECTION(
      "test source launch, sink launch, "
      "source get, sink get" +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_b = std::async(std::launch::async, mid);
    auto fut_a = std::async(std::launch::async, source);
    auto fut_c = std::async(std::launch::async, sink);
    fut_a.get();
    fut_b.get();
    fut_c.get();
  }
  SECTION(
      "test source launch, sink launch, "
      "source get, sink get" +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_b = std::async(std::launch::async, mid);
    auto fut_c = std::async(std::launch::async, sink);
    auto fut_a = std::async(std::launch::async, source);
    fut_a.get();
    fut_b.get();
    fut_c.get();
  }
  SECTION(
      "test source launch, sink launch, "
      "source get, sink get" +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_c = std::async(std::launch::async, sink);
    auto fut_b = std::async(std::launch::async, mid);
    auto fut_a = std::async(std::launch::async, source);

    fut_a.get();
    fut_b.get();
    fut_c.get();
  }
  SECTION(
      "test source launch, sink "
      "launch, source get, sink get" +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_c = std::async(std::launch::async, sink);
    auto fut_a = std::async(std::launch::async, source);
    auto fut_b = std::async(std::launch::async, mid);

    fut_a.get();
    fut_b.get();
    fut_c.get();
  }
  SECTION(
      "test source launch, sink "
      "launch, source get, sink get" +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_a = std::async(std::launch::async, source);
    auto fut_b = std::async(std::launch::async, mid);
    auto fut_c = std::async(std::launch::async, sink);
    fut_a.get();
    fut_b.get();
    fut_c.get();
  }
  SECTION(
      "test source launch, sink "
      "launch, source get, sink "
      "get" +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_a = std::async(std::launch::async, source);
    auto fut_b = std::async(std::launch::async, mid);
    auto fut_c = std::async(std::launch::async, sink);
    fut_a.get();
    fut_c.get();
    fut_b.get();
  }
  SECTION(
      "test source launch, sink "
      "launch, source get, sink "
      "get" +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_a = std::async(std::launch::async, source);
    auto fut_b = std::async(std::launch::async, mid);
    auto fut_c = std::async(std::launch::async, sink);
    fut_b.get();
    fut_a.get();
    fut_c.get();
  }
  SECTION(
      "test source launch, "
      "sink launch, source "
      "get, sink get" +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_a = std::async(std::launch::async, source);
    auto fut_b = std::async(std::launch::async, mid);
    auto fut_c = std::async(std::launch::async, sink);
    fut_b.get();
    fut_c.get();
    fut_a.get();
  }
  SECTION(
      "test source launch, "
      "sink launch, source "
      "get, sink get" +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_a = std::async(std::launch::async, source);
    auto fut_b = std::async(std::launch::async, mid);
    auto fut_c = std::async(std::launch::async, sink);
    fut_c.get();
    fut_a.get();
    fut_b.get();
  }
  SECTION(
      "test source launch, "
      "sink launch, source "
      "get, sink get" +
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
      std::cout << "this "
                   "should not "
                   "happen"
                << std::endl;
    }
  }

  //  Can't use these with generator and consumer function nodes
  //  CHECK(std::distance(input.begin(), i) == static_cast<long>(rounds));
  //  CHECK(std::distance(output.begin(), j) == static_cast<long>(rounds));
  CHECK(std::equal(input.begin(), i, output.begin()));
}

/**
 * Test that we can correctly pass a sequence of integers from producer node to
 * consumer node via function node.
 */
TEST_CASE(
    "Nodes: Async pass n "
    "integers, four "
    "nodes, two stage",
    "[nodes]") {
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

  ProducerNode<AsyncMover2, size_t> source_node([&i]() { return (*i++); });
  FunctionNode<AsyncMover2, size_t> mid_node1([](size_t k) { return k; });
  FunctionNode<AsyncMover2, size_t> mid_node2([](size_t k) { return k; });
  ConsumerNode<AsyncMover2, size_t> sink_node([&j](size_t k) { *j++ = k; });

  auto a = Edge(source_node, mid_node1);
  auto b = Edge(mid_node1, mid_node2);
  auto c = Edge(mid_node2, sink_node);

  auto source = [&]() { source_node.run(rounds); };
  auto mid1 = [&]() { mid_node1.run(rounds + offset); };
  auto mid2 = [&]() { mid_node2.run(rounds); };
  auto sink = [&]() { sink_node.run(rounds + offset); };

  SECTION(
      "dcba abcd" + std::to_string(rounds) + " / " + std::to_string(offset)) {
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
      "dcba dcba" + std::to_string(rounds) + " / " + std::to_string(offset)) {
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
      "abcd abcd" + std::to_string(rounds) + " / " + std::to_string(offset)) {
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

/**
 * Test that we can correctly pass a sequence of integers from producer node to
 * consumer node via function node.
 */
TEST_CASE(
    "Nodes: "
    "Async pass "
    "n integers, "
    "four nodes, "
    "three stage",
    "[nodes]") {
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

  ProducerNode<AsyncMover3, size_t> source_node(generator{19});
  FunctionNode<AsyncMover3, size_t> mid_node1([](size_t k) { return k + 1; });
  FunctionNode<AsyncMover3, size_t> mid_node2([](size_t k) { return k - 1; });
  ConsumerNode<AsyncMover3, size_t> sink_node(consumer<decltype(j), size_t>{j});

  auto a = Edge(source_node, mid_node1);
  auto b = Edge(mid_node1, mid_node2);
  auto c = Edge(mid_node2, sink_node);

  auto source = [&]() { source_node.run(rounds); };
  auto mid1 = [&]() { mid_node1.run(rounds + offset); };
  auto mid2 = [&]() { mid_node2.run(rounds); };
  auto sink = [&]() { sink_node.run(rounds + offset); };

  SECTION(
      "abcd "
      "abcd" +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_a = std::async(std::launch::async, source);
    auto fut_b = std::async(std::launch::async, mid1);
    auto fut_c = std::async(std::launch::async, sink);
    auto fut_d = std::async(std::launch::async, mid2);
    fut_a.get();
    fut_b.get();
    fut_c.get();
    fut_d.get();
  }
  SECTION(
      "dcba "
      "abcd" +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
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
      "abcd "
      "dcba" +
      std::to_string(rounds) +
      " /"
      " " +
      std::to_string(offset)) {
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
      "dcba"
      " dcb"
      "a" +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_d = std::async(std::launch::async, mid2);
    auto fut_c = std::async(std::launch::async, sink);
    auto fut_b = std::async(std::launch::async, mid1);
    auto fut_a = std::async(std::launch::async, source);
    fut_d.get();
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
  //  Can't use these with generator and consumer function nodes
  //  CHECK(std::distance(input.begin(), i) == static_cast<long>(rounds));
  //  CHECK(std::distance(output.begin(), j) == static_cast<long>(rounds));
  CHECK(std::equal(input.begin(), i, output.begin()));
}

/**
 * Test that we can correctly pass a sequence of integers from producer node to
 * consumer node via function node.
 */
TEST_CASE(
    "Nodes: Async pass n integers, four nodes, "
    "three stage, with delays",
    "[nodes]") {
  size_t rounds = GENERATE(0, 1, 2, 5, 3379);
  size_t offset = GENERATE(0, 1, 2, 5);

  [[maybe_unused]] constexpr bool debug = false;

  if (debug)
    rounds = 3;

  std::vector<size_t> input(rounds + offset);
  std::vector<size_t> output(rounds + offset + 2);

  std::iota(input.begin(), input.end(), 19);
  std::fill(output.begin(), output.end(), 0);
  auto i = input.begin();
  auto j = output.begin();

  if (rounds + offset != 0) {
    CHECK(std::equal(input.begin(), input.end(), output.begin()) == false);
  }

  ProducerNode<AsyncMover3, size_t> source_node(generator{19});
  FunctionNode<AsyncMover3, size_t> mid_node1([](size_t k) { return k; });
  FunctionNode<AsyncMover3, size_t> mid_node2([](size_t k) { return k; });
  ConsumerNode<AsyncMover3, size_t> sink_node(consumer<decltype(j), size_t>{j});

  auto a = Edge(source_node, mid_node1);
  auto b = Edge(mid_node1, mid_node2);
  auto c = Edge(mid_node2, sink_node);

  auto source = [&]() { source_node.run_with_delays(rounds); };
  auto mid1 = [&]() { mid_node1.run_with_delays(rounds + offset); };
  auto mid2 = [&]() { mid_node2.run_with_delays(rounds + offset + 1); };
  auto sink = [&]() { sink_node.run_with_delays(rounds + offset + 2); };

  SECTION(
      "abcd abcd" + std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_a = std::async(std::launch::async, source);
    auto fut_b = std::async(std::launch::async, mid1);
    auto fut_c = std::async(std::launch::async, sink);
    auto fut_d = std::async(std::launch::async, mid2);
    fut_a.get();
    fut_b.get();
    fut_c.get();
    fut_d.get();
  }
  SECTION(
      "dcba abcd" + std::to_string(rounds) + " / " + std::to_string(offset)) {
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
      "dcba dcba" + std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_d = std::async(std::launch::async, mid2);
    auto fut_c = std::async(std::launch::async, sink);
    auto fut_b = std::async(std::launch::async, mid1);
    auto fut_a = std::async(std::launch::async, source);
    fut_d.get();
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

  //  Can't use these with generator and consumer function nodes
  //  CHECK(std::distance(input.begin(), i) == static_cast<long>(rounds));
  //  CHECK(std::distance(output.begin(), j) == static_cast<long>(rounds));
  CHECK(std::equal(input.begin(), i, output.begin()));
}
