/**
 * @file unit_general.cc
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

#include "unit_general_nodes.h"
#include <future>
#include <numeric>

#include "experimental/tiledb/common/dag/edge/edge.h"
#include "experimental/tiledb/common/dag/nodes/detail/simple/mimo.h"
#include "experimental/tiledb/common/dag/nodes/generators.h"
#include "experimental/tiledb/common/dag/nodes/simple_nodes.h"
#include "experimental/tiledb/common/dag/nodes/terminals.h"
#include "experimental/tiledb/common/dag/state_machine/test/types.h"

using namespace tiledb::common;

TEST_CASE("GeneralNode: Verify various API approaches", "[general]") {
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

TEST_CASE("GeneralNode: Verify simple resume", "[general]") {
  [[maybe_unused]] GeneralFunctionNode<
      AsyncMover2,
      std::tuple<size_t, int>,
      AsyncMover3,
      std::tuple<size_t, double>>
      x{};
}

TEST_CASE(
    "GeneralNode: Verify construction with simple function", "[general]") {
  [[maybe_unused]] GeneralFunctionNode<
      AsyncMover2,
      std::tuple<size_t>,
      AsyncMover3,
      std::tuple<size_t>>
      x{[](std::tuple<size_t>, std::tuple<size_t>) {}};
}

TEST_CASE(
    "GeneralNode: Verify construction with compound function", "[general]") {
  [[maybe_unused]] GeneralFunctionNode<
      AsyncMover2,
      std::tuple<size_t, int>,
      AsyncMover3,
      std::tuple<size_t, double>>
      x{[](std::tuple<size_t, int>, std::tuple<size_t, double>) {}};
}

template <class T>
struct foo {
  void operator()() {
  }
};

/*
 * @note Cannot use void for SinkMover_T nor SourceMover_T, because that must be
 * a template template.  Use dummy class `foo` instead.
 *
 * The GeneralFunctionNode includes some special casing to support these.  There
 * may be a more elegant way, given that the tuple being used (and hence the
 * corresponding variadic) is empty.
 *
 * @todo Partial specialization to allow void (?)
 */
template <template <class> class SourceMover_T, class... BlocksOut>
using GeneralProducerNode =
    GeneralFunctionNode<foo, std::tuple<>, SourceMover_T, BlocksOut...>;

template <template <class> class SinkMover_T, class... BlocksIn>
using GeneralConsumerNode =
    GeneralFunctionNode<SinkMover_T, BlocksIn..., foo, std::tuple<>>;

TEST_CASE(
    "GeneralNode: Verify use of (void) template arguments for "
    "producer/consumer [general]") {
  GeneralProducerNode<AsyncMover3, std::tuple<size_t, double>> x{
      [](std::tuple<size_t, double>) {}};
  GeneralConsumerNode<AsyncMover3, std::tuple<size_t, double>> y{
      [](std::tuple<size_t, double>) {}};
}

TEST_CASE("GeneralNode: Connect void-created Producer and Consumer [general]") {
  GeneralProducerNode<AsyncMover3, std::tuple<size_t, double>> x{
      [](std::tuple<size_t, double>) {}};
  GeneralConsumerNode<AsyncMover3, std::tuple<double, size_t>> y{
      [](std::tuple<double, size_t>) {}};

  Edge g{std::get<0>(x.outputs_), std::get<1>(y.inputs_)};
  Edge h{std::get<1>(x.outputs_), std::get<0>(y.inputs_)};
}

TEST_CASE(
    "GeneralNode: Pass values with void-created Producer and Consumer "
    "[general]") {
  double ext1{0.0};
  size_t ext2{0};

  GeneralProducerNode<AsyncMover3, std::tuple<size_t, double>> x{
      [](std::tuple<size_t, double>& a) { a = std::make_tuple(5UL, 3.14159); }};
  GeneralConsumerNode<AsyncMover3, std::tuple<double, size_t>> y{
      [&ext1, &ext2](const std::tuple<double, size_t>& b) {
        ext1 = std::get<0>(b);
        ext2 = std::get<1>(b);
      }};

  Edge g{std::get<0>(x.outputs_), std::get<1>(y.inputs_)};
  Edge h{std::get<1>(x.outputs_), std::get<0>(y.inputs_)};

  x.resume();
  y.resume();

  CHECK(ext1 == 3.14159);
  CHECK(ext2 == 5);
}

/**
 * Some dummy functions and classes to test node constructors
 * with.
 */
size_t dummy_source() {
  return size_t{};
}

void dummy_function(const std::tuple<size_t>& in, std::tuple<size_t>& out) {
  out = in;
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
  void operator()(const std::tuple<size_t>& in, std::tuple<size_t>& out) {
    out = in;
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

void dummy_bind_function(
    double, float, const std::tuple<size_t>& in, std::tuple<size_t>& out) {
  out = in;
}

void dummy_bind_sink(size_t, float, const int&) {
}

TEST_CASE("GeneralNode: Verify simple connections", "[general]") {
  SECTION("function") {
    ProducerNode<AsyncMover3, size_t> a{dummy_source};

    GeneralFunctionNode<
        AsyncMover3,
        std::tuple<size_t>,
        AsyncMover3,
        std::tuple<size_t>>
        b{dummy_function};
    ConsumerNode<AsyncMover3, size_t> c{dummy_sink};

    ProducerNode<AsyncMover2, size_t> d{dummy_source};
    GeneralFunctionNode<AsyncMover2, std::tuple<size_t>> e{dummy_function};
    ConsumerNode<AsyncMover2, size_t> f{dummy_sink};

    Edge g{a, std::get<0>(b.inputs_)};
    Edge h{std::get<0>(b.outputs_), c};

    Edge i{d, std::get<0>(e.inputs_)};
    Edge j{std::get<0>(e.outputs_), f};
  }

  SECTION("lambda") {
    auto dummy_source_lambda = []() { return 0UL; };
    auto dummy_function_lambda = [](const std::tuple<size_t>& in,
                                    std::tuple<size_t>& out) { out = in; };
    auto dummy_sink_lambda = [](size_t) {};

    ProducerNode<AsyncMover3, size_t> a{dummy_source_lambda};
    GeneralFunctionNode<AsyncMover3, std::tuple<size_t>> b{
        dummy_function_lambda};
    ConsumerNode<AsyncMover3, size_t> c{dummy_sink_lambda};

    ProducerNode<AsyncMover2, size_t> d{dummy_source_lambda};
    GeneralFunctionNode<AsyncMover2, std::tuple<size_t>> e{
        dummy_function_lambda};
    ConsumerNode<AsyncMover2, size_t> f{dummy_sink_lambda};

    Edge g{a, std::get<0>(b.inputs_)};
    Edge h{std::get<0>(b.outputs_), c};

    Edge i{d, std::get<0>(e.inputs_)};
    Edge j{std::get<0>(e.outputs_), f};
  }

  SECTION("inline lambda") {
    ProducerNode<AsyncMover3, size_t> a([]() { return 0UL; });
    GeneralFunctionNode<AsyncMover3, std::tuple<size_t>> b(
        [](const std::tuple<size_t>& in, std::tuple<size_t>& out) {
          out = in;
        });
    ConsumerNode<AsyncMover3, size_t> c([](size_t) {});

    ProducerNode<AsyncMover2, size_t> d([]() { return 0UL; });
    GeneralFunctionNode<AsyncMover2, std::tuple<size_t>> e(
        [](const std::tuple<size_t>& in, std::tuple<size_t>& out) {
          out = in;
        });
    ConsumerNode<AsyncMover2, size_t> f([](size_t) {});

    Edge g{a, std::get<0>(b.inputs_)};
    Edge h{std::get<0>(b.outputs_), c};

    Edge i{d, std::get<0>(e.inputs_)};
    Edge j{std::get<0>(e.outputs_), f};
  }

  SECTION("function object") {
    auto ac = dummy_source_class{};
    dummy_function_class fc{};
    dummy_sink_class dc{};

    ProducerNode<AsyncMover3, size_t> a{ac};
    GeneralFunctionNode<AsyncMover3, std::tuple<size_t>> b{fc};
    ConsumerNode<AsyncMover3, size_t> c{dc};

    ProducerNode<AsyncMover2, size_t> d{ac};
    GeneralFunctionNode<AsyncMover2, std::tuple<size_t>> e{fc};
    ConsumerNode<AsyncMover2, size_t> f{dc};

    Edge g{a, std::get<0>(b.inputs_)};
    Edge h{std::get<0>(b.outputs_), c};

    Edge i{d, std::get<0>(e.inputs_)};
    Edge j{std::get<0>(e.outputs_), f};
  }

  SECTION("inline function object") {
    ProducerNode<AsyncMover3, size_t> a{dummy_source_class{}};
    GeneralFunctionNode<AsyncMover3, std::tuple<size_t>> b{
        dummy_function_class{}};
    ConsumerNode<AsyncMover3, size_t> c{dummy_sink_class{}};

    ProducerNode<AsyncMover2, size_t> d{dummy_source_class{}};
    GeneralFunctionNode<AsyncMover2, std::tuple<size_t>> e{
        dummy_function_class{}};
    ConsumerNode<AsyncMover2, size_t> f{dummy_sink_class{}};

    Edge g{a, std::get<0>(b.inputs_)};
    Edge h{std::get<0>(b.outputs_), c};

    Edge i{d, std::get<0>(e.inputs_)};
    Edge j{std::get<0>(e.outputs_), f};
  }

  SECTION("bind") {
    double x = 0.01;
    float y = -0.001;
    int z = 8675309;

    auto ac = std::bind(dummy_bind_source, x);
    auto dc = std::bind(dummy_bind_sink, y, std::placeholders::_1, z);
    auto fc = std::bind(
        dummy_bind_function,
        x,
        y,
        std::placeholders::_1,
        std::placeholders::_2);

    ProducerNode<AsyncMover3, size_t> a{ac};
    GeneralFunctionNode<AsyncMover3, std::tuple<size_t>> b{fc};
    ConsumerNode<AsyncMover3, size_t> c{dc};

    ProducerNode<AsyncMover2, size_t> d{ac};
    GeneralFunctionNode<AsyncMover2, std::tuple<size_t>> e{fc};
    ConsumerNode<AsyncMover2, size_t> f{dc};

    Edge g{a, std::get<0>(b.inputs_)};
    Edge h{std::get<0>(b.outputs_), c};

    Edge i{d, std::get<0>(e.inputs_)};
    Edge j{std::get<0>(e.outputs_), f};
  }

  SECTION("inline bind") {
    double x = 0.01;
    float y = -0.001;
    int z = 8675309;

    ProducerNode<AsyncMover3, size_t> a{std::bind(dummy_bind_source, x)};
    GeneralFunctionNode<AsyncMover3, std::tuple<size_t>> b{std::bind(
        dummy_bind_function,
        x,
        y,
        std::placeholders::_1,
        std::placeholders::_2)};
    ConsumerNode<AsyncMover3, size_t> c{
        std::bind(dummy_bind_sink, y, std::placeholders::_1, z)};

    ProducerNode<AsyncMover2, size_t> d{std::bind(dummy_bind_source, x)};
    GeneralFunctionNode<AsyncMover2, std::tuple<size_t>> e{std::bind(
        dummy_bind_function,
        x,
        y,
        std::placeholders::_1,
        std::placeholders::_2)};
    ConsumerNode<AsyncMover2, size_t> f{
        std::bind(dummy_bind_sink, y, std::placeholders::_1, z)};

    Edge g{a, std::get<0>(b.inputs_)};
    Edge h{std::get<0>(b.outputs_), c};

    Edge i{d, std::get<0>(e.inputs_)};
    Edge j{std::get<0>(e.outputs_), f};
  }

  SECTION("bind with move") {
    double x = 0.01;
    float y = -0.001;
    int z = 8675309;

    auto ac = std::bind(dummy_bind_source, std::move(x));
    auto dc = std::bind(
        dummy_bind_sink, std::move(y), std::placeholders::_1, std::move(z));
    auto fc = std::bind(
        dummy_bind_function,
        std::move(x),
        std::move(y),
        std::placeholders::_1,
        std::placeholders::_2);

    ProducerNode<AsyncMover3, size_t> a{std::move(ac)};
    GeneralFunctionNode<AsyncMover3, std::tuple<size_t>> b{std::move(fc)};
    ConsumerNode<AsyncMover3, size_t> c{std::move(dc)};

    ProducerNode<AsyncMover2, size_t> d{std::move(ac)};
    GeneralFunctionNode<AsyncMover2, std::tuple<size_t>> e{std::move(fc)};
    ConsumerNode<AsyncMover2, size_t> f{std::move(dc)};

    Edge g{a, std::get<0>(b.inputs_)};
    Edge h{std::get<0>(b.outputs_), c};

    Edge i{d, std::get<0>(e.inputs_)};
    Edge j{std::get<0>(e.outputs_), f};
  }
}

TEST_CASE("GeneralNode: Verify compound connections", "[general]") {
  SECTION("inline lambda") {
    ProducerNode<AsyncMover3, size_t> a1([]() { return 0UL; });
    ProducerNode<AsyncMover3, double> a2([]() { return 0.0; });
    GeneralFunctionNode<AsyncMover3, std::tuple<size_t, double>> b(
        [](const std::tuple<size_t, double>& in,
           std::tuple<size_t, double>& out) { out = in; });
    ConsumerNode<AsyncMover3, size_t> c1([](size_t) {});
    ConsumerNode<AsyncMover3, double> c2([](double) {});

    ProducerNode<AsyncMover2, size_t> d1([]() { return 0UL; });
    ProducerNode<AsyncMover2, double> d2([]() { return 0.0; });
    GeneralFunctionNode<AsyncMover2, std::tuple<size_t, double>> e(
        [](const std::tuple<size_t, double>& in,
           std::tuple<size_t, double>& out) { out = in; });
    ConsumerNode<AsyncMover2, size_t> f1([](size_t) {});
    ConsumerNode<AsyncMover2, double> f2([](double) {});

    Edge g1{a1, std::get<0>(b.inputs_)};
    Edge g2{a2, std::get<1>(b.inputs_)};
    Edge h1{std::get<0>(b.outputs_), c1};
    Edge h2{std::get<1>(b.outputs_), c2};

    Edge i1{d1, std::get<0>(e.inputs_)};
    Edge i2{d2, std::get<1>(e.inputs_)};
    Edge j1{std::get<0>(e.outputs_), f1};
    Edge j2{std::get<1>(e.outputs_), f2};
  }
}

/**
 * Test that we can synchronously send data from a producer to an attached
 * function node and then to consumer.
 */
TEST_CASE(
    "Nodes: Manually pass some data in a chain with a one component general "
    "function node [general]") {
  size_t i{0UL};
  ProducerNode<AsyncMover2, size_t> q([&]() { return i++; });

  GeneralFunctionNode<AsyncMover2, std::tuple<size_t>> r(
      [&](const std::tuple<std::size_t>& in, std::tuple<std::size_t>& out) {
        std::get<0>(out) = 2 * std::get<0>(in);
      });

  std::vector<size_t> v;
  ConsumerNode<AsyncMover2, size_t> s([&](size_t i) { v.push_back(i); });

  Edge g{q, std::get<0>(r.inputs_)};
  Edge h{std::get<0>(r.outputs_), s};

  q.resume();
  r.resume();
  s.resume();

  CHECK(v.size() == 1);

  q.resume();
  r.reset();
  r.resume();
  s.resume();

  CHECK(v.size() == 2);

  q.resume();
  r.reset();
  r.resume();
  s.resume();

  CHECK(v.size() == 3);

  CHECK(v[0] == 0);
  CHECK(v[1] == 2);
  CHECK(v[2] == 4);
}

/**
 * Test that we can synchronously send data from a producer to an attached
 * compound general function node and then to consumer.
 */
TEST_CASE(
    "Nodes: Manually pass some data in a chain with a multi component general "
    "function node [general]") {
  size_t i{0UL};
  double j{0.0};
  ProducerNode<AsyncMover2, size_t> q1([&]() { return i++; });
  ProducerNode<AsyncMover2, double> q2([&]() { return j++; });

  GeneralFunctionNode<
      AsyncMover2,
      std::tuple<size_t, double>,
      AsyncMover2,
      std::tuple<double, size_t>>
      r([&](const std::tuple<size_t, double>& in,
            std::tuple<double, std::size_t>& out) {
        std::get<1>(out) = 2 * std::get<0>(in);
        std::get<0>(out) = 3.0 * std::get<1>(in);
      });

  std::vector<double> v;
  std::vector<size_t> w;
  ConsumerNode<AsyncMover2, double> s1([&](size_t i) { v.push_back(i); });
  ConsumerNode<AsyncMover2, size_t> s2([&](size_t i) { w.push_back(i); });

  Edge g1{q1, std::get<0>(r.inputs_)};
  Edge g2{q2, std::get<1>(r.inputs_)};
  Edge h1{std::get<0>(r.outputs_), s1};
  Edge h2{std::get<1>(r.outputs_), s2};

  q1.resume();
  q2.resume();
  r.resume();
  s1.resume();
  s2.resume();

  CHECK(v.size() == 1);
  CHECK(w.size() == 1);

  q1.resume();
  q2.resume();
  r.reset();
  r.resume();
  s1.resume();
  s2.resume();

  CHECK(v.size() == 2);
  CHECK(w.size() == 2);

  q1.resume();
  q2.resume();
  r.reset();
  r.resume();
  s1.resume();
  s2.resume();

  CHECK(v.size() == 3);
  CHECK(w.size() == 3);

  CHECK(w[0] == 0);
  CHECK(w[1] == 2);
  CHECK(w[2] == 4);
  CHECK(v[0] == 0.0);
  CHECK(v[1] == 3.0);
  CHECK(v[2] == 6.0);
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

  std::vector<double> v;
  double j{0.0};

  std::vector<size_t> w;
  size_t i{0};

  ProducerNode<AsyncMover2, size_t> q1([&]() {
    if constexpr (delay) {
      std::this_thread::sleep_for(std::chrono::microseconds(
          static_cast<size_t>(qwt * random_us(1234))));
    }
    return i++;
  });
  ProducerNode<AsyncMover2, double> q2([&]() {
    if constexpr (delay) {
      std::this_thread::sleep_for(std::chrono::microseconds(
          static_cast<size_t>(qwt * random_us(1234))));
    }
    return j++;
  });

  GeneralFunctionNode<
      AsyncMover2,
      std::tuple<size_t, double>,
      AsyncMover2,
      std::tuple<double, size_t>>
      r([&](const std::tuple<size_t, double>& in,
            std::tuple<double, size_t>& out) {
        if constexpr (delay) {
          std::this_thread::sleep_for(std::chrono::microseconds(
              static_cast<size_t>(rwt * random_us(1234))));
        }
        std::get<0>(out) = 3 * std::get<1>(in);
        std::get<1>(out) = 5.0 * std::get<0>(in);
      });

  ConsumerNode<AsyncMover2, size_t> s1([&](size_t i) {
    v.push_back(i);
    if constexpr (delay) {
      std::this_thread::sleep_for(std::chrono::microseconds(
          static_cast<size_t>(swt * random_us(1234))));
    }
  });
  ConsumerNode<AsyncMover2, double> s2([&](double i) {
    w.push_back(i);
    if constexpr (delay) {
      std::this_thread::sleep_for(std::chrono::microseconds(
          static_cast<size_t>(swt * random_us(1234))));
    }
  });

  Edge g1{q1, std::get<0>(r.inputs_)};
  Edge g2{q2, std::get<1>(r.inputs_)};
  Edge h1{std::get<1>(r.outputs_), s1};
  Edge h2{std::get<0>(r.outputs_), s2};

  auto fun_a1 = [&]() {
    size_t N = rounds;
    while (N--) {
      q1.resume();
    }
  };
  auto fun_a2 = [&]() {
    size_t N = rounds;
    while (N--) {
      q2.resume();
    }
  };

  auto fun_b = [&]() {
    size_t N = rounds;
    while (N--) {
      r.resume();
      r.reset();
    }
  };

  auto fun_c1 = [&]() {
    size_t N = rounds;
    while (N--) {
      s1.resume();
    }
  };

  auto fun_c2 = [&]() {
    size_t N = rounds;
    while (N--) {
      s2.resume();
    }
  };

  CHECK(v.size() == 0);
  CHECK(w.size() == 0);

  SECTION("Async Nodes a, b, c, a, b, c") {
    auto fut_a1 = std::async(std::launch::async, fun_a1);
    auto fut_a2 = std::async(std::launch::async, fun_a2);
    auto fut_b = std::async(std::launch::async, fun_b);
    auto fut_c1 = std::async(std::launch::async, fun_c1);
    auto fut_c2 = std::async(std::launch::async, fun_c2);
    fut_a1.get();
    fut_a2.get();
    fut_b.get();
    fut_c1.get();
    fut_c2.get();
  }

  SECTION("Async Nodes a, b, c, c, b, a") {
    auto fut_a1 = std::async(std::launch::async, fun_a1);
    auto fut_a2 = std::async(std::launch::async, fun_a2);
    auto fut_b = std::async(std::launch::async, fun_b);
    auto fut_c1 = std::async(std::launch::async, fun_c1);
    auto fut_c2 = std::async(std::launch::async, fun_c2);
    fut_c2.get();
    fut_c1.get();
    fut_b.get();
    fut_a2.get();
    fut_a1.get();
  }

  SECTION("Async Nodes c, b, a, a, b, c") {
    auto fut_c2 = std::async(std::launch::async, fun_c2);
    auto fut_c1 = std::async(std::launch::async, fun_c1);
    auto fut_b = std::async(std::launch::async, fun_b);
    auto fut_a2 = std::async(std::launch::async, fun_a2);
    auto fut_a1 = std::async(std::launch::async, fun_a1);
    fut_a1.get();
    fut_a2.get();
    fut_b.get();
    fut_c1.get();
    fut_c2.get();
  }

  SECTION("Async Nodes c, b, a, c, b, a") {
    auto fut_c2 = std::async(std::launch::async, fun_c2);
    auto fut_c1 = std::async(std::launch::async, fun_c1);
    auto fut_b = std::async(std::launch::async, fun_b);
    auto fut_a2 = std::async(std::launch::async, fun_a2);
    auto fut_a1 = std::async(std::launch::async, fun_a1);
    fut_c2.get();
    fut_c1.get();
    fut_b.get();
    fut_a2.get();
    fut_a1.get();
  }

  CHECK(v.size() == rounds);
  for (size_t i = 0; i < rounds; ++i) {
    CHECK(v[i] == 5.0 * i);
  }
  CHECK(w.size() == rounds);
  for (size_t i = 0; i < rounds; ++i) {
    CHECK(w[i] == 3 * i);
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

  std::vector<size_t> input1(rounds + offset);
  std::vector<double> input2(rounds + offset);
  std::vector<double> output1(rounds + offset);
  std::vector<size_t> output2(rounds + offset);

  std::iota(input1.begin(), input1.end(), 19);
  std::iota(input2.begin(), input2.end(), 337);
  std::fill(output1.begin(), output1.end(), 0.0);
  std::fill(output2.begin(), output2.end(), 0);
  auto i1 = input1.begin();
  auto i2 = input2.begin();
  auto j1 = output1.begin();
  auto j2 = output2.begin();

  if (rounds + offset != 0) {
    CHECK(std::equal(input1.begin(), input1.end(), output1.begin()) == false);
  }
  if (rounds + offset != 0) {
    CHECK(std::equal(input2.begin(), input2.end(), output2.begin()) == false);
  }

  ProducerNode<AsyncMover3, size_t> source_node1(generators{19});
  ProducerNode<AsyncMover3, double> source_node2(generators{337});

  GeneralFunctionNode<
      AsyncMover3,
      std::tuple<size_t, double>,
      AsyncMover3,
      std::tuple<double, size_t>>
      mid_node([](const std::tuple<size_t, double>& in,
                  std::tuple<double, size_t>& out) {
        std::get<0>(out) = std::get<1>(in);
        std::get<1>(out) = std::get<0>(in);
      });

  ConsumerNode<AsyncMover3, double> sink_node1(
      terminal<decltype(j1), double>{j1});
  ConsumerNode<AsyncMover3, size_t> sink_node2(
      terminal<decltype(j2), size_t>{j2});

  Edge(source_node1, std::get<0>(mid_node.inputs_));
  Edge(source_node2, std::get<1>(mid_node.inputs_));
  Edge(std::get<0>(mid_node.outputs_), sink_node1);
  Edge(std::get<1>(mid_node.outputs_), sink_node2);

  auto source1 = [&]() { source_node1.run_for(rounds); };
  auto source2 = [&]() { source_node2.run_for(rounds); };
  auto mid = [&]() { mid_node.run_for(rounds + offset); };
  auto sink1 = [&]() { sink_node1.run_for(rounds); };
  auto sink2 = [&]() { sink_node2.run_for(rounds); };

  SECTION(
      "test source launch, sink launch, source "
      "get, sink get " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_a1 = std::async(std::launch::async, source1);
    auto fut_a2 = std::async(std::launch::async, source2);
    auto fut_b = std::async(std::launch::async, mid);
    auto fut_c1 = std::async(std::launch::async, sink1);
    auto fut_c2 = std::async(std::launch::async, sink2);
    fut_a1.get();
    fut_a2.get();
    fut_b.get();
    fut_c1.get();
    fut_c2.get();
  }
  SECTION(
      "test source launch, sink launch, source "
      "get, sink get " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_a1 = std::async(std::launch::async, source1);
    auto fut_a2 = std::async(std::launch::async, source2);
    auto fut_c2 = std::async(std::launch::async, sink2);
    auto fut_c1 = std::async(std::launch::async, sink1);
    auto fut_b = std::async(std::launch::async, mid);
    fut_a1.get();
    fut_a2.get();
    fut_b.get();
    fut_c2.get();
    fut_c1.get();
  }
  SECTION(
      "test source launch, sink launch, "
      "source get, sink get " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_b = std::async(std::launch::async, mid);
    auto fut_c1 = std::async(std::launch::async, sink1);
    auto fut_c2 = std::async(std::launch::async, sink2);
    auto fut_a2 = std::async(std::launch::async, source2);
    auto fut_a1 = std::async(std::launch::async, source1);
    fut_a1.get();
    fut_a2.get();
    fut_b.get();
    fut_c2.get();
    fut_c1.get();
  }
  SECTION(
      "test source launch, sink "
      "launch, source get, sink get " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_c1 = std::async(std::launch::async, sink1);
    auto fut_a1 = std::async(std::launch::async, source1);
    auto fut_b = std::async(std::launch::async, mid);
    auto fut_a2 = std::async(std::launch::async, sink2);
    auto fut_c2 = std::async(std::launch::async, source2);
    fut_a2.get();
    fut_a1.get();
    fut_c1.get();
    fut_b.get();
    fut_c2.get();
  }
  SECTION(
      "test source launch, sink "
      "launch, source get, sink get " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_a2 = std::async(std::launch::async, source2);
    auto fut_a1 = std::async(std::launch::async, source1);
    auto fut_b = std::async(std::launch::async, mid);
    auto fut_c2 = std::async(std::launch::async, sink2);
    auto fut_c1 = std::async(std::launch::async, sink1);
    fut_a1.get();
    fut_a2.get();
    fut_b.get();
    fut_c1.get();
    fut_c2.get();
  }

  if (!std::equal(input1.begin(), i1, output2.begin())) {
    for (size_t j = 0; j < input1.size(); ++j) {
      if (input1[j] != output2[j]) {
        std::cout << j << " (" << input1[j] << ", " << output2[j] << ")"
                  << std::endl;
      }
    }
  }

  if (!std::equal(input2.begin(), i2, output1.begin())) {
    for (size_t j = 0; j < input2.size(); ++j) {
      if (input2[j] != output1[j]) {
        std::cout << j << " (" << input2[j] << ", " << output1[j] << ")"
                  << std::endl;
      }
    }
  }

  if (!std::equal(input1.begin(), i1, output2.begin())) {
    auto iter = std::find_first_of(
        input1.begin(),
        input1.end(),
        output2.begin(),
        output2.end(),
        std::not_equal_to<size_t>());
    if (iter != input1.end()) {
      size_t k = iter - input1.begin();
      std::cout << k << " (" << input1[k] << ", " << output2[k] << ")"
                << std::endl;
    } else {
      std::cout << "this should not ever happen" << std::endl;
    }
  }

  if (!std::equal(input2.begin(), i2, output1.begin())) {
    auto iter = std::find_first_of(
        input2.begin(),
        input2.end(),
        output1.begin(),
        output1.end(),
        std::not_equal_to<size_t>());
    if (iter != input2.end()) {
      size_t k = iter - input2.begin();
      std::cout << k << " (" << input2[k] << ", " << output1[k] << ")"
                << std::endl;
    } else {
      std::cout << "this should not ever happen" << std::endl;
    }
  }

  CHECK(std::equal(input1.begin(), i1, output2.begin()));
  CHECK(std::equal(input2.begin(), i2, output1.begin()));
}
