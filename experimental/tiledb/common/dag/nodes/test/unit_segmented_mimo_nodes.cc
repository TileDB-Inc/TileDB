/**
 * @file unit_segmented_mimo_nodes.cc
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
 * Tests the segmented mimo node class
 *
 * @todo: Need to get better syntax for Edge with shared_ptr
 */

#include <numeric>

#include "unit_segmented_mimo_nodes.h"

#include "experimental/tiledb/common/dag/edge/edge.h"
#include "experimental/tiledb/common/dag/nodes/generators.h"
#include "experimental/tiledb/common/dag/nodes/terminals.h"

#include "experimental/tiledb/common/dag/state_machine/policies.h"
#include "experimental/tiledb/common/dag/state_machine/test/types.h"

#include "experimental/tiledb/common/dag/nodes/detail/segmented/mimo.h"
#include "experimental/tiledb/common/dag/nodes/segmented_nodes.h"

#include "experimental/tiledb/common/dag/nodes/detail/segmented/edge_node_ctad.h"
#include "experimental/tiledb/common/dag/utility/print_types.h"

using namespace tiledb::common;

TEST_CASE("mimo_node: Verify various API approaches", "[segmented_mimo]") {
  [[maybe_unused]] mimo_node<
      AsyncMover2,
      std::tuple<size_t, int>,
      AsyncMover3,
      std::tuple<size_t, double>>
      x{};
  CHECK(x->num_inputs() == 2);
  CHECK(x->num_outputs() == 2);
  [[maybe_unused]] mimo_node<
      AsyncMover2,
      std::tuple<int>,
      AsyncMover3,
      std::tuple<size_t, double>>
      y{};
  CHECK(y->num_inputs() == 1);
  CHECK(y->num_outputs() == 2);
  [[maybe_unused]] mimo_node<
      AsyncMover2,
      std::tuple<char*>,
      AsyncMover3,
      std::tuple<size_t, std::tuple<int, float>>>
      z{};
  CHECK(z->num_inputs() == 1);
  CHECK(z->num_outputs() == 2);
  [[maybe_unused]] mimo_node<
      AsyncMover2,
      std::tuple<int, char, double, double, double>,
      AsyncMover3,
      std::tuple<int>>
      a{};
  CHECK(a->num_inputs() == 5);
  CHECK(a->num_outputs() == 1);
}

TEST_CASE(
    "mimo_node: Verify construction with simple function", "[segmented_mimo]") {
  [[maybe_unused]] mimo_node<
      AsyncMover2,
      std::tuple<size_t, size_t>,
      AsyncMover3,
      std::tuple<size_t, char*>>
      x{[](const std::tuple<size_t, size_t>&) {
        return std::tuple<size_t, char*>{};
      }};
}

TEST_CASE(
    "mimo_node: Verify construction with compound function",
    "[segmented_mimo]") {
  [[maybe_unused]] mimo_node<
      AsyncMover2,
      std::tuple<size_t, int>,
      AsyncMover3,
      std::tuple<size_t, double, float>>
      x{[](std::tuple<size_t, int>) {
        return std::tuple<size_t, double, float>{};
      }};
  CHECK(x->num_inputs() == 2);
  CHECK(x->num_outputs() == 3);
}

template <class... R, class... T>
auto mimo(std::function<void(std::tuple<R...>, std::tuple<T...>)>&& f) {
  auto tmp = mimo_node<
      AsyncMover3,
      std::remove_cv_t<std::remove_reference_t<T>>...,
      AsyncMover3,
      R...>{f};
  return tmp;
}

template <class T>
struct foo {
  void operator()() {
  }
};

TEST_CASE(
    "mimo_node: Verify use of (void) template arguments for producer",
    "[segmented_mimo]") {
  mimo_node<foo, std::tuple<>, AsyncMover3, std::tuple<size_t, double>> x{
      [](std::stop_source) { return std::tuple<size_t, double>{}; }};
  mimo_node<AsyncMover3, std::tuple<size_t, double>, foo, std::tuple<>> y{
      [](std::tuple<size_t, double>) {}};
  mimo_node<AsyncMover3, std::tuple<char*>, foo, std::tuple<>> z{
      [](std::tuple<char*>) {}};
}

/*
 * @note Cannot use void for SinkMover_T nor SourceMover_T, because that must be
 * a template template.  Use dummy class `foo` instead.
 *
 * The mimo_node includes some special casing to support these.  There
 * may be a more elegant way, given that the tuple being used (and hence the
 * corresponding variadic) is empty.
 *
 * @todo Partial specialization to allow void (?)
 */
template <template <class> class SourceMover_T, class... BlocksOut>
using GeneralProducerNode =
    mimo_node<foo, std::tuple<>, SourceMover_T, BlocksOut...>;

template <template <class> class SinkMover_T, class... BlocksIn>
using GeneralConsumerNode =
    mimo_node<SinkMover_T, BlocksIn..., foo, std::tuple<>>;

TEST_CASE(
    "mimo_node: Verify use of (void) template arguments for "
    "producer/consumer [general]") {
  GeneralProducerNode<AsyncMover3, std::tuple<size_t, double>> x{
      [](std::stop_source) { return std::tuple<size_t, double>{}; }};
  GeneralConsumerNode<AsyncMover3, std::tuple<size_t, double>> y{
      [](std::tuple<size_t, double>) {}};
}

TEST_CASE(
    "mimo_node: Connect void-created Producer and Consumer [segmented_mimo]") {
  GeneralProducerNode<AsyncMover3, std::tuple<size_t, double>> x{
      [](std::stop_source) { return std::tuple<size_t, double>{}; }};
  GeneralConsumerNode<AsyncMover3, std::tuple<double, size_t>> y{
      [](std::tuple<double, size_t>) {}};

  Edge g{std::get<0>(x->outputs_), std::get<1>(y->inputs_)};
  Edge h{std::get<1>(x->outputs_), std::get<0>(y->inputs_)};
}

TEST_CASE(
    "mimo_node: Pass values with void-created Producer and Consumer "
    "[segmented_mimo]") {
  double ext1{0.0};
  size_t ext2{0};

  GeneralProducerNode<AsyncMover3, std::tuple<size_t, double>> x{
      [](std::stop_source) {
        auto a = std::make_tuple(5UL, 3.14159);
        return a;
      }};
  GeneralConsumerNode<AsyncMover3, std::tuple<double, size_t>> y{
      [&ext1, &ext2](const std::tuple<double, size_t>& b) {
        ext1 = std::get<0>(b);
        ext2 = std::get<1>(b);
      }};

  Edge g{std::get<0>(x->outputs_), std::get<1>(y->inputs_)};
  Edge h{std::get<1>(x->outputs_), std::get<0>(y->inputs_)};

  [[maybe_unused]] auto foo = x->resume();
  [[maybe_unused]] auto bar = y->resume();
  foo = x->resume();
  bar = y->resume();
  foo = x->resume();
  bar = y->resume();
  foo = x->resume();
  bar = y->resume();

  CHECK(ext1 == 3.14159);
  CHECK(ext2 == 5);
}

/**
 * Some dummy functions and classes to test node constructors
 * with.
 */
size_t dummy_source(std::stop_source&) {
  return size_t{};
}

auto dummy_general_source(std::stop_source) {
  return std::tuple<size_t>{};
}

auto dummy_function(const std::tuple<size_t>& in) {
  return in;
}

void dummy_sink(size_t) {
}

void dummy_general_sink(const std::tuple<size_t>&) {
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
  auto operator()(const std::tuple<size_t>& in) {
    return in;
  }
};

class dummy_sink_class {
 public:
  void operator()(size_t) {
  }
};

size_t dummy_bind_source(std::stop_source&, double) {
  return size_t{};
}

auto dummy_bind_function(double, float, const std::tuple<size_t>& in) {
  return in;
}

void dummy_bind_sink(size_t, float, const int&) {
}

TEST_CASE("mimo_node: Verify simple connections", "[segmented_mimo]") {
  SECTION("function") {
    GeneralProducerNode<AsyncMover3, std::tuple<size_t>> a{
        dummy_general_source};

    mimo_node<AsyncMover3, std::tuple<size_t>, AsyncMover3, std::tuple<size_t>>
        b{dummy_function};
    GeneralConsumerNode<AsyncMover3, std::tuple<size_t>> c{dummy_general_sink};

    producer_node<AsyncMover2, size_t> d{dummy_source};
    mimo_node<AsyncMover2, std::tuple<size_t>> e{dummy_function};
    consumer_node<AsyncMover2, size_t> f{dummy_sink};

    Edge g{std::get<0>(a->outputs_), std::get<0>(b->inputs_)};
    Edge h{std::get<0>(b->outputs_), std::get<0>(c->inputs_)};

    // @todo: Why were we trying this?
    Edge<AsyncMover2, size_t> i{*d, std::get<0>(e->inputs_)};
    Edge j{std::get<0>(e->outputs_), f};

    {
      producer_node_impl<AsyncMover2, size_t> x{dummy_source};
      consumer_node_impl<AsyncMover2, size_t> y{dummy_sink};
      // Edge<AsyncMover2, size_t> bb{x, y};
      Edge cc{x, y};

      auto foo = std::make_shared<producer_node_impl<AsyncMover2, size_t>>(
          dummy_source);
      auto bar =
          std::make_shared<consumer_node_impl<AsyncMover2, size_t>>(dummy_sink);

      // Edge<AsyncMover2, size_t> dd{foo, bar};
      Edge dd{foo, bar};
    }
  }

  SECTION("lambda") {
    auto dummy_source_lambda = [](std::stop_source&) { return 0UL; };
    auto dummy_function_lambda = [](const std::tuple<size_t>& in) {
      return in;
    };
    auto dummy_sink_lambda = [](size_t) {};

    producer_node<AsyncMover3, size_t> a{dummy_source_lambda};
    mimo_node<AsyncMover3, std::tuple<size_t>> b{dummy_function_lambda};
    consumer_node<AsyncMover3, size_t> c{dummy_sink_lambda};

    producer_node<AsyncMover2, size_t> d{dummy_source_lambda};
    mimo_node<AsyncMover2, std::tuple<size_t>> e{dummy_function_lambda};
    consumer_node<AsyncMover2, size_t> f{dummy_sink_lambda};

    Edge g{a, std::get<0>(b->inputs_)};
    Edge h{std::get<0>(b->outputs_), c};
  }

  SECTION("inline lambda") {
    producer_node<AsyncMover3, size_t> a([](std::stop_source&) { return 0UL; });
    mimo_node<AsyncMover3, std::tuple<size_t>> b(
        [](const std::tuple<size_t>& in) { return in; });
    consumer_node<AsyncMover3, size_t> c([](size_t) {});

    producer_node<AsyncMover2, size_t> d([](std::stop_source&) { return 0UL; });
    mimo_node<AsyncMover2, std::tuple<size_t>> e(
        [](const std::tuple<size_t>& in) { return in; });
    consumer_node<AsyncMover2, size_t> f([](size_t) {});

    Edge g{a, std::get<0>(b->inputs_)};
    Edge h{std::get<0>(b->outputs_), c};

    Edge i{d, std::get<0>(e->inputs_)};
    Edge j{std::get<0>(e->outputs_), f};
  }

  SECTION("function object") {
    auto ac = dummy_source_class{};
    dummy_function_class fc{};
    dummy_sink_class dc{};

    producer_node<AsyncMover3, size_t> a{ac};
    mimo_node<AsyncMover3, std::tuple<size_t>> b{fc};
    consumer_node<AsyncMover3, size_t> c{dc};

    producer_node<AsyncMover2, size_t> d{ac};
    mimo_node<AsyncMover2, std::tuple<size_t>> e{fc};
    consumer_node<AsyncMover2, size_t> f{dc};

    Edge g{a, std::get<0>(b->inputs_)};
    Edge h{std::get<0>(b->outputs_), c};

    Edge i{d, std::get<0>(e->inputs_)};
    Edge j{std::get<0>(e->outputs_), f};
  }

  SECTION("inline function object") {
    producer_node<AsyncMover3, size_t> a{dummy_source_class{}};
    mimo_node<AsyncMover3, std::tuple<size_t>> b{dummy_function_class{}};
    consumer_node<AsyncMover3, size_t> c{dummy_sink_class{}};

    producer_node<AsyncMover2, size_t> d{dummy_source_class{}};
    mimo_node<AsyncMover2, std::tuple<size_t>> e{dummy_function_class{}};
    consumer_node<AsyncMover2, size_t> f{dummy_sink_class{}};

    Edge g{a, std::get<0>(b->inputs_)};
    Edge h{std::get<0>(b->outputs_), c};

    Edge i{d, std::get<0>(e->inputs_)};
    Edge j{std::get<0>(e->outputs_), f};
  }
#if 1
  SECTION("bind") {
    double x = 0.01;
    float y = -0.001;
    int z = 8675309;

    auto ac = std::bind(dummy_bind_source, std::placeholders::_1, x);
    auto dc = std::bind(dummy_bind_sink, y, std::placeholders::_1, z);
    auto fc = std::bind(dummy_bind_function, x, y, std::placeholders::_1);

    producer_node<AsyncMover3, size_t> a{ac};
    mimo_node<AsyncMover3, std::tuple<size_t>> b{fc};
    consumer_node<AsyncMover3, size_t> c{dc};

    producer_node<AsyncMover2, size_t> d{ac};
    mimo_node<AsyncMover2, std::tuple<size_t>> e{fc};
    consumer_node<AsyncMover2, size_t> f{dc};

    Edge g{a, std::get<0>(b->inputs_)};
    Edge h{std::get<0>(b->outputs_), c};

    Edge i{d, std::get<0>(e->inputs_)};
    Edge j{std::get<0>(e->outputs_), f};
  }

  SECTION("inline bind") {
    double x = 0.01;
    float y = -0.001;
    int z = 8675309;

    producer_node<AsyncMover3, size_t> a{
        std::bind(dummy_bind_source, std::placeholders::_1, x)};
    mimo_node<AsyncMover3, std::tuple<size_t>> b{
        std::bind(dummy_bind_function, x, y, std::placeholders::_1)};
    consumer_node<AsyncMover3, size_t> c{
        std::bind(dummy_bind_sink, y, std::placeholders::_1, z)};

    producer_node<AsyncMover2, size_t> d{
        std::bind(dummy_bind_source, std::placeholders::_1, x)};
    mimo_node<AsyncMover2, std::tuple<size_t>> e{
        std::bind(dummy_bind_function, x, y, std::placeholders::_1)};
    consumer_node<AsyncMover2, size_t> f{
        std::bind(dummy_bind_sink, y, std::placeholders::_1, z)};

    Edge g{a, std::get<0>(b->inputs_)};
    Edge h{std::get<0>(b->outputs_), c};

    Edge i{d, std::get<0>(e->inputs_)};
    Edge j{std::get<0>(e->outputs_), f};
  }

  SECTION("bind with move") {
    double x = 0.01;
    float y = -0.001;
    int z = 8675309;

    auto ac = std::bind(dummy_bind_source, std::placeholders::_1, std::move(x));
    auto dc = std::bind(
        dummy_bind_sink, std::move(y), std::placeholders::_1, std::move(z));
    auto fc = std::bind(
        dummy_bind_function, std::move(x), std::move(y), std::placeholders::_1);

    producer_node<AsyncMover3, size_t> a{std::move(ac)};
    mimo_node<AsyncMover3, std::tuple<size_t>> b{std::move(fc)};
    consumer_node<AsyncMover3, size_t> c{std::move(dc)};

    producer_node<AsyncMover2, size_t> d{std::move(ac)};
    mimo_node<AsyncMover2, std::tuple<size_t>> e{std::move(fc)};
    consumer_node<AsyncMover2, size_t> f{std::move(dc)};

    Edge g{a, std::get<0>(b->inputs_)};
    Edge h{std::get<0>(b->outputs_), c};

    Edge i{d, std::get<0>(e->inputs_)};
    Edge j{std::get<0>(e->outputs_), f};
  }
#endif
}

TEST_CASE("mimo_node: Verify compound connections", "[segmented_mimo]") {
  SECTION("inline lambda") {
    producer_node<AsyncMover3, size_t> a1(
        [](std::stop_source&) { return 0UL; });
    producer_node<AsyncMover3, double> a2(
        [](std::stop_source&) { return 0.0; });
    mimo_node<AsyncMover3, std::tuple<size_t, double>> b(
        [](const std::tuple<size_t, double>& in) { return in; });
    consumer_node<AsyncMover3, size_t> c1([](size_t) {});
    consumer_node<AsyncMover3, double> c2([](double) {});

    producer_node<AsyncMover2, size_t> d1(
        [](std::stop_source&) { return 0UL; });
    producer_node<AsyncMover2, double> d2(
        [](std::stop_source&) { return 0.0; });
    mimo_node<AsyncMover2, std::tuple<size_t, double>> e(
        [](const std::tuple<size_t, double>& in) { return in; });
    consumer_node<AsyncMover2, size_t> f1([](size_t) {});
    consumer_node<AsyncMover2, double> f2([](double) {});

    Edge g1{a1, std::get<0>(b->inputs_)};
    Edge g2{a2, std::get<1>(b->inputs_)};
    Edge h1{std::get<0>(b->outputs_), c1};
    Edge h2{std::get<1>(b->outputs_), c2};

    Edge i1{d1, std::get<0>(e->inputs_)};
    Edge i2{d2, std::get<1>(e->inputs_)};
    Edge j1{std::get<0>(e->outputs_), f1};
    Edge j2{std::get<1>(e->outputs_), f2};
  }
}

/**
 * Test that we can synchronously send data from a producer to an attached
 * function node and then to consumer.
 */
TEST_CASE(
    "Nodes: Manually pass some data in a chain with a one component general "
    "function node [segmented_mimo]") {
  size_t i{0UL};
  producer_node<AsyncMover2, size_t> q([&](std::stop_source) { return i++; });

  mimo_node<AsyncMover2, std::tuple<size_t>> r(
      [&](const std::tuple<std::size_t>& in) {
        return std::make_tuple(2 * std::get<0>(in));
      });

  std::vector<size_t> v;
  consumer_node<AsyncMover2, size_t> s([&](size_t i) { v.push_back(i); });

  Edge g{q, std::get<0>(r->inputs_)};
  Edge h{std::get<0>(r->outputs_), s};
  connect(q, r);
  connect(r, s);

  q->resume();  // fill  10 / 00
  q->resume();  // push  01 / 00
  q->resume();  // yield 01 / 00
  q->resume();  // fill  11

  r->resume();  // pull   11 / 00
  r->resume();  // drain  10 / 00
  r->resume();  // fill   10 / 10
  r->resume();  // push   10 / 01
  r->resume();  // yield  10 / 01
  r->resume();  // pull   01 / 01
  r->resume();  // drain  00 / 01
  r->resume();  // fill   00 / 11

  s->resume();  // pull    00 / 11
  s->resume();  // drain   00 / 10
  s->resume();  // yield

  CHECK(v.size() == 1);

  q->resume();  // push  00 / 01
  r->resume();  // push  00 / 01
  s->resume();  // pull  00 / 01
  s->resume();  // drain 00 / 00
  s->resume();  // yield

  CHECK(v.size() == 2);

  q->resume();  // yield 00 / 00
  r->resume();  // yield 00 / 00

  q->resume();  // fill  10 / 00
  q->resume();  // push  01 / 00
  q->resume();  // yield 01 / 00

  r->resume();  // pull  01 / 00
  r->resume();  // drain 00 / 00
  r->resume();  // fill  00 / 10

  s->resume();  // pull  00 / 01
  s->resume();  // drain 00 / 00

  CHECK(v.size() == 2);

  s->resume();  // yield

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
    "Nodes: Manually pass some data in a chain with a multi component "
    "segmented_mimo "
    "function node [segmented_mimo]") {
  size_t i{0UL};
  double j{0.0};
  producer_node<AsyncMover2, size_t> q1([&](std::stop_source&) { return i++; });
  producer_node<AsyncMover2, double> q2([&](std::stop_source&) { return j++; });

  mimo_node<
      AsyncMover2,
      std::tuple<size_t, double>,
      AsyncMover2,
      std::tuple<double, size_t>>
      r([&](const std::tuple<size_t, double>& in) {
        return std::make_tuple(2 * std::get<0>(in), 3.0 * std::get<1>(in));
      });

  std::vector<double> v;
  std::vector<size_t> w;
  consumer_node<AsyncMover2, double> s1([&](double i) { v.push_back(i); });
  consumer_node<AsyncMover2, size_t> s2([&](size_t i) { w.push_back(i); });

  Edge g1{q1, std::get<0>(r->inputs_)};
  Edge g2{q2, std::get<1>(r->inputs_)};

  Edge h1{std::get<0>(r->outputs_), s1};
  Edge h2{std::get<1>(r->outputs_), s2};
  connect(q1, r);
  connect(q2, r);
  connect(r, s1);
  connect(r, s2);

  q1->resume();  // fill  10 / 00
  q2->resume();  // fill  10 : 10 / 00 : 00
  r->resume();   // pull  01 : 01 / 00 : 00
  r->resume();   // drain 00 : 00 / 00 : 00
  r->resume();   // fill  00 : 00 / 10 : 10

  s1->resume();  // pull  00 : 00 / 01 : 10
  s2->resume();  // pull  00 : 00 / 01 : 01
  s1->resume();  // drain 00 : 00 / 00 : 01
  s1->resume();  // yield 00 : 00 / 00 : 01

  CHECK(v.size() == 1);
  CHECK(w.size() == 0);

  s2->resume();  // drain 00 : 00 / 00 : 00
  s2->resume();  // yield 00 : 00 / 00 : 00

  CHECK(v.size() == 1);
  CHECK(w.size() == 1);

  q1->resume();  // push  00 : 00 / 00 : 00
  q1->resume();  // yield 00 : 00 / 00 : 00
  q1->resume();  // fill  10 : 00 / 00 : 00
  q1->resume();  // push  01 : 00 / 00 : 00
  q1->resume();  // yield 01 : 00 / 00 : 00

  q2->resume();  // push  01 : 00 / 00 : 00
  q2->resume();  // yield 01 : 00 / 00 : 00
  q2->resume();  // fill  01 : 10 / 00 : 00

  r->resume();   // push  01 : 10 / 00 : 00
  r->resume();   // yield 01 : 10 / 00 : 00
  r->resume();   // pull  01 : 01 / 00 : 00
  r->resume();   // drain 00 : 00 / 00 : 00
  r->resume();   // fill  00 : 00 / 10 : 10
  s1->resume();  // pull  00 : 00 / 01 : 10
  r->resume();   // push  00 : 00 / 01 : 01
  r->resume();   // yield 00 : 00 / 01 : 01

  CHECK(v.size() == 1);
  CHECK(w.size() == 1);

  s2->resume();  // pull  00 : 00 / 01 : 01
  s2->resume();  // drain 00 : 00 / 01 : 00
  s2->resume();  // yield 00 : 00 / 01 : 00
  s1->resume();  // drain 00 : 00 / 00 : 00
  s1->resume();  // yield 00 : 00 / 00 : 00

  CHECK(v.size() == 2);
  CHECK(w.size() == 2);

  q1->resume();  // fill  10 : 00 / 00 : 00
  q2->resume();  // push  10 : 00 / 00 : 00
  q2->resume();  // yield 10 : 00 / 00 : 00
  q2->resume();  // fill  10 : 10 / 00 : 00
  r->resume();   // pull  01 : 01 / 00 : 00
  r->resume();   // drain 00 : 00 / 00 : 00
  r->resume();   // fill  00 : 00 / 10 : 10

  s1->resume();  // pull  00 : 00 / 01 : 10
  s2->resume();  // pull  00 : 00 / 01 : 01
  s1->resume();  // drain 00 : 00 / 00 : 01
  s1->resume();  // yield 00 : 00 / 00 : 01

  CHECK(v.size() == 3);
  CHECK(w.size() == 2);

  s2->resume();  // drain 00 : 00 / 00 : 00
  s2->resume();  // yield 00 : 00 / 00 : 00

  CHECK(v.size() == 3);
  CHECK(w.size() == 3);

  CHECK(w[0] == 0);
  CHECK(w[1] == 3);
  CHECK(w[2] == 6);
  CHECK(v[0] == 0.0);
  CHECK(v[1] == 2.0);
  CHECK(v[2] == 4.0);
}

template <class Node>
auto run_for(Node& node, size_t rounds) {
  return [&node, rounds]() {
    size_t N = rounds;
    while (N) {
      auto code = node->resume();
      if (code == SchedulerAction::yield)
        --N;
    }
  };
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

  producer_node<AsyncMover2, size_t> q1([&](std::stop_source&) {
    if constexpr (delay) {
      std::this_thread::sleep_for(std::chrono::microseconds(
          static_cast<size_t>(qwt * random_us(1234))));
    }
    return i++;
  });
  producer_node<AsyncMover2, double> q2([&](std::stop_source&) {
    if constexpr (delay) {
      std::this_thread::sleep_for(std::chrono::microseconds(
          static_cast<size_t>(qwt * random_us(1234))));
    }
    return j++;
  });

  mimo_node<
      AsyncMover2,
      std::tuple<size_t, double>,
      AsyncMover2,
      std::tuple<double, size_t>>
      r([&](const std::tuple<size_t, double>& in) {
        if constexpr (delay) {
          std::this_thread::sleep_for(std::chrono::microseconds(
              static_cast<size_t>(rwt * random_us(1234))));
        }
        return std::make_tuple(3 * std::get<1>(in), 5.0 * std::get<0>(in));
      });

  consumer_node<AsyncMover2, size_t> s1([&](size_t i) {
    v.push_back(i);
    if constexpr (delay) {
      std::this_thread::sleep_for(std::chrono::microseconds(
          static_cast<size_t>(swt * random_us(1234))));
    }
  });
  consumer_node<AsyncMover2, double> s2([&](double i) {
    w.push_back(i);
    if constexpr (delay) {
      std::this_thread::sleep_for(std::chrono::microseconds(
          static_cast<size_t>(swt * random_us(1234))));
    }
  });

  Edge g1{q1, std::get<0>(r->inputs_)};
  Edge g2{q2, std::get<1>(r->inputs_)};
  Edge h1{std::get<1>(r->outputs_), s1};
  Edge h2{std::get<0>(r->outputs_), s2};
  connect(q1, r);
  connect(q2, r);
  connect(r, s1);
  connect(r, s2);

  auto fun_a1 = run_for(q1, rounds);
  auto fun_a2 = run_for(q2, rounds);
  auto fun_b = run_for(r, rounds);
  auto fun_c1 = run_for(s1, rounds);
  auto fun_c2 = run_for(s2, rounds);

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
class zero {};
class one {};
class two {};
class three {};

TEMPLATE_TEST_CASE(
    "Nodes: Async pass n integers, three nodes, "
    "three stage",
    "[nodes]",
    (std::tuple<
        producer_node<AsyncMover3, size_t>,
        producer_node<AsyncMover3, double>,
        consumer_node<AsyncMover3, double>,
        consumer_node<AsyncMover3, size_t>,
        zero>),
    (std::tuple<
        producer_node<AsyncMover3, size_t>,
        producer_node<AsyncMover3, double>,
        consumer_node<AsyncMover3, double>,
        consumer_node<AsyncMover3, size_t>,
        one>)) {
  size_t rounds = GENERATE(0, 1, 2, 5, 3379);
  size_t offset = GENERATE(0, 1, 2, 5);

  using P1 = std::tuple_element_t<0, TestType>;
  using P2 = std::tuple_element_t<1, TestType>;
  using C1 = std::tuple_element_t<2, TestType>;
  using C2 = std::tuple_element_t<3, TestType>;
  using NO = std::tuple_element_t<4, TestType>;

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

  // Can't do this because generator returns its result,
  // whereas the mimo node takes the result by reference.
  P1 source_node1(generators{19});
  P2 source_node2(generators{337});

  mimo_node<
      AsyncMover3,
      std::tuple<size_t, double>,
      AsyncMover3,
      std::tuple<double, size_t>>
      mid_node([](const std::tuple<size_t, double>& in) {
        return std::make_tuple(std::get<1>(in), std::get<0>(in));
      });

  C1 sink_node1(terminal<decltype(j1), double>{j1});
  C2 sink_node2(terminal<decltype(j2), size_t>{j2});

  auto source1 = run_for(source_node1, rounds);
  auto source2 = run_for(source_node2, rounds);
  auto mid = run_for(mid_node, rounds);
  auto sink1 = run_for(sink_node1, rounds);
  auto sink2 = run_for(sink_node2, rounds);

  if constexpr (std::is_same_v<NO, zero> || std::is_same_v<NO, one>) {
    Edge(source_node1, std::get<0>(mid_node->inputs_));
    Edge(source_node2, std::get<1>(mid_node->inputs_));
    Edge(std::get<0>(mid_node->outputs_), sink_node1);
    Edge(std::get<1>(mid_node->outputs_), sink_node2);
  } else {
    Edge(std::get<0>(source_node1->outputs_), std::get<0>(mid_node->inputs_));
    Edge(std::get<0>(source_node2->outputs_), std::get<1>(mid_node->inputs_));
    Edge(std::get<0>(mid_node->outputs_), std::get<0>(sink_node1->inputs_));
    Edge(std::get<1>(mid_node->outputs_), std::get<0>(sink_node2->inputs_));
  }

  connect(source_node1, mid_node);
  connect(source_node2, mid_node);
  connect(mid_node, sink_node1);
  connect(mid_node, sink_node2);

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

#if 0
// Repeat one of the tests above but with one-sided mimo nodes
// Annoying -- required different interface than special-purpose nodes
// @todo: fix this
// Best solution is probably to give mimo nodes the same
// interface as special-purpose nodes

// Repeat one of the tests above but with mimo connected to mimo and
// with differen cardinalities on input and output

// Repeat one of the tests above but with stop token

#endif
