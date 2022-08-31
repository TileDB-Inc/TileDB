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

#include "unit_general.h"
#include <future>

#include "experimental/tiledb/common/dag/edge/edge.h"
#include "experimental/tiledb/common/dag/nodes/consumer.h"
#include "experimental/tiledb/common/dag/nodes/general.h"
#include "experimental/tiledb/common/dag/nodes/generator.h"
#include "experimental/tiledb/common/dag/nodes/simple.h"
#include "experimental/tiledb/common/dag/state_machine/test/types.h"

using namespace tiledb::common;

TEST_CASE("GeneralNode: Verify various API approaches", "[general]") {
  [[maybe_unused]] GeneralFunctionNode<
      size_t,
      AsyncMover2,
      std::tuple<size_t, int>,
      AsyncMover3,
      std::tuple<size_t, double>>
      x{};
  [[maybe_unused]] GeneralFunctionNode<
      size_t,
      AsyncMover2,
      std::tuple<int>,
      AsyncMover3,
      std::tuple<size_t, double>>
      y{};
  [[maybe_unused]] GeneralFunctionNode<
      size_t,
      AsyncMover2,
      std::tuple<char*>,
      AsyncMover3,
      std::tuple<size_t, std::tuple<int, float>>>
      z{};
  [[maybe_unused]] GeneralFunctionNode<
      size_t,
      AsyncMover2,
      std::tuple<int, char, double, double, double>,
      AsyncMover3,
      std::tuple<int>>
      a{};
}

TEST_CASE("GeneralNode: Verify simple run_once", "[general]") {
  [[maybe_unused]] GeneralFunctionNode<
      size_t,
      AsyncMover2,
      std::tuple<size_t, int>,
      AsyncMover3,
      std::tuple<size_t, double>>
      x{};
}

TEST_CASE(
    "GeneralNode: Verify construction with simple function", "[general]") {
  [[maybe_unused]] GeneralFunctionNode<
      size_t,
      AsyncMover2,
      std::tuple<size_t>,
      AsyncMover3,
      std::tuple<size_t>>
      x{[](std::tuple<size_t>, std::tuple<size_t>) {}};
}

TEST_CASE(
    "GeneralNode: Verify construction with compound function", "[general]") {
  [[maybe_unused]] GeneralFunctionNode<
      size_t,
      AsyncMover2,
      std::tuple<size_t, int>,
      AsyncMover3,
      std::tuple<size_t, double>>
      x{[](std::tuple<size_t, int>, std::tuple<size_t, double>) {}};
}

/**
 * Some dummy functions and classes to test node constructors
 * with.
 */
size_t dummy_source() {
  return size_t{};
}

//std::tuple<size_t> dummy_function(std::tuple<size_t>) {
//  return {0UL};
//}

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
    //    GeneralFunctionNode<size_t, AsyncMover3, std::tuple<size_t>> b{
    //        dummy_function};
    GeneralFunctionNode<
        size_t,
        AsyncMover3,
        std::tuple<size_t>,
        AsyncMover3,
        std::tuple<size_t>>
        b{dummy_function};
    ConsumerNode<AsyncMover3, size_t> c{dummy_sink};

    ProducerNode<AsyncMover2, size_t> d{dummy_source};
    GeneralFunctionNode<size_t, AsyncMover2, std::tuple<size_t>> e{
        dummy_function};
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
    GeneralFunctionNode<size_t, AsyncMover3, std::tuple<size_t>> b{
        dummy_function_lambda};
    ConsumerNode<AsyncMover3, size_t> c{dummy_sink_lambda};

    ProducerNode<AsyncMover2, size_t> d{dummy_source_lambda};
    GeneralFunctionNode<size_t, AsyncMover2, std::tuple<size_t>> e{
        dummy_function_lambda};
    ConsumerNode<AsyncMover2, size_t> f{dummy_sink_lambda};

    Edge g{a, std::get<0>(b.inputs_)};
    Edge h{std::get<0>(b.outputs_), c};

    Edge i{d, std::get<0>(e.inputs_)};
    Edge j{std::get<0>(e.outputs_), f};
  }

  SECTION("inline lambda") {
    ProducerNode<AsyncMover3, size_t> a([]() { return 0UL; });
    GeneralFunctionNode<size_t, AsyncMover3, std::tuple<size_t>> b(
        [](const std::tuple<size_t>& in, std::tuple<size_t>& out) {
          out = in;
        });
    ConsumerNode<AsyncMover3, size_t> c([](size_t) {});

    ProducerNode<AsyncMover2, size_t> d([]() { return 0UL; });
    GeneralFunctionNode<size_t, AsyncMover2, std::tuple<size_t>> e(
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
    GeneralFunctionNode<size_t, AsyncMover3, std::tuple<size_t>> b{fc};
    ConsumerNode<AsyncMover3, size_t> c{dc};

    ProducerNode<AsyncMover2, size_t> d{ac};
    GeneralFunctionNode<size_t, AsyncMover2, std::tuple<size_t>> e{fc};
    ConsumerNode<AsyncMover2, size_t> f{dc};

    Edge g{a, std::get<0>(b.inputs_)};
    Edge h{std::get<0>(b.outputs_), c};

    Edge i{d, std::get<0>(e.inputs_)};
    Edge j{std::get<0>(e.outputs_), f};
  }

  SECTION("inline function object") {
    ProducerNode<AsyncMover3, size_t> a{dummy_source_class{}};
    GeneralFunctionNode<size_t, AsyncMover3, std::tuple<size_t>> b{
        dummy_function_class{}};
    ConsumerNode<AsyncMover3, size_t> c{dummy_sink_class{}};

    ProducerNode<AsyncMover2, size_t> d{dummy_source_class{}};
    GeneralFunctionNode<size_t, AsyncMover2, std::tuple<size_t>> e{
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
    GeneralFunctionNode<size_t, AsyncMover3, std::tuple<size_t>> b{fc};
    ConsumerNode<AsyncMover3, size_t> c{dc};

    ProducerNode<AsyncMover2, size_t> d{ac};
    GeneralFunctionNode<size_t, AsyncMover2, std::tuple<size_t>> e{fc};
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
    GeneralFunctionNode<size_t, AsyncMover3, std::tuple<size_t>> b{std::bind(
        dummy_bind_function,
        x,
        y,
        std::placeholders::_1,
        std::placeholders::_2)};
    ConsumerNode<AsyncMover3, size_t> c{
        std::bind(dummy_bind_sink, y, std::placeholders::_1, z)};

    ProducerNode<AsyncMover2, size_t> d{std::bind(dummy_bind_source, x)};
    GeneralFunctionNode<size_t, AsyncMover2, std::tuple<size_t>> e{std::bind(
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
    GeneralFunctionNode<size_t, AsyncMover3, std::tuple<size_t>> b{
        std::move(fc)};
    ConsumerNode<AsyncMover3, size_t> c{std::move(dc)};

    ProducerNode<AsyncMover2, size_t> d{std::move(ac)};
    GeneralFunctionNode<size_t, AsyncMover2, std::tuple<size_t>> e{
        std::move(fc)};
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
    GeneralFunctionNode<size_t, AsyncMover3, std::tuple<size_t, double>> b(
        [](const std::tuple<size_t, double>& in,
           std::tuple<size_t, double>& out) { out = in; });
    ConsumerNode<AsyncMover3, size_t> c1([](size_t) {});
    ConsumerNode<AsyncMover3, double> c2([](double) {});

    ProducerNode<AsyncMover2, size_t> d1([]() { return 0UL; });
    ProducerNode<AsyncMover2, double> d2([]() { return 0.0; });
    GeneralFunctionNode<size_t, AsyncMover2, std::tuple<size_t, double>> e(
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
    "Nodes: Manuallay pass some data in a chain with function node",
    "[nodes]") {
  size_t i{0UL};
  ProducerNode<AsyncMover2, size_t> q([&]() { return i++; });

  GeneralFunctionNode<size_t, AsyncMover2, std::tuple<size_t>> r(
      [&](const std::tuple<std::size_t>& in, std::tuple<std::size_t>& out) {
        std::get<0>(out) = 2 * std::get<0>(in);
      });

  std::vector<size_t> v;
  ConsumerNode<AsyncMover2, size_t> s([&](size_t i) { v.push_back(i); });

  Edge g{q, std::get<0>(r.inputs_)};
  Edge h{std::get<0>(r.outputs_), s};

  q.run_once();
  r.run_once();
  s.run_once();

  CHECK(v.size() == 1);

  q.run_once();
  r.reset();
  r.run_once();
  s.run_once();

  CHECK(v.size() == 2);

  q.run_once();
  r.reset();
  r.run_once();
  s.run_once();

  CHECK(v.size() == 3);

  CHECK(v[0] == 0);
  CHECK(v[1] == 2);
  CHECK(v[2] == 4);
}
