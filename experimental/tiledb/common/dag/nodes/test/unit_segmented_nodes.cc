/**
 * @file unit_segmented_node.cc
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

#include "unit_segmented_nodes.h"
#include <future>
#include <type_traits>
#include <variant>
#include "experimental/tiledb/common/dag/edge/edge.h"
#include "experimental/tiledb/common/dag/nodes/generators.h"
#include "experimental/tiledb/common/dag/nodes/segmented_nodes.h"
#include "experimental/tiledb/common/dag/nodes/terminals.h"
#include "experimental/tiledb/common/dag/state_machine/test/types.h"
#include "experimental/tiledb/common/dag/utility/print_types.h"

using namespace tiledb::common;

/**
 * Verify various API approaches
 */
TEMPLATE_TEST_CASE(
    "SegementedNodes: Verify various API approaches",
    "[segmented_nodes]",
    (std::tuple<
        consumer_node<AsyncMover2, size_t>,
        producer_node<AsyncMover2, size_t>>),
    (std::tuple<
        consumer_node<AsyncMover3, size_t>,
        producer_node<AsyncMover3, size_t>>)) {
  using C = typename std::tuple_element<0, TestType>::type;
  using P = typename std::tuple_element<1, TestType>::type;

  SECTION("Test Construction") {
    P a;
    P b([](std::stop_source&) { return 0; });
    C c([](size_t) {});
  }
  SECTION("Test Connection") {
    P b([](std::stop_source&) { return 0; });
    C c([](size_t) {});
    Edge g{*b, *c};
  }
  SECTION("Enable if fail") {
    // These will fail, with good diagnostics.  This should be commented out
    // from time to time and tested by hand that we get the right error
    // messages.
    //   producer_node<AsyncMover3, size_t> bb{0UL};
    //   consumer_node<AsyncMover3, size_t> cc{-1.1};
    //   Edge g{bb, cc};
  }
}

bool two_nodes(node_base&, node_base&) {
  return true;
}

bool two_nodes(const node&, const node&) {
  return true;
}

TEMPLATE_TEST_CASE(
    "Tasks: Extensive tests of tasks with nodes",
    "[tasks]",
    (std::tuple<
        consumer_node<AsyncMover2, size_t>,
        function_node<AsyncMover2, size_t>,
        producer_node<AsyncMover2, size_t>>),
    (std::tuple<
        consumer_node<AsyncMover3, size_t>,
        function_node<AsyncMover3, size_t>,
        producer_node<AsyncMover3, size_t>>)) {
  using C = typename std::tuple_element<0, TestType>::type;
  using F = typename std::tuple_element<1, TestType>::type;
  using P = typename std::tuple_element<2, TestType>::type;

  using CI = typename C::Base::element_type;
  using FI = typename F::Base::element_type;
  using PI = typename P::Base::element_type;

  auto pro_node_impl = PI([](std::stop_source&) { return 0; });
  auto fun_node_impl = FI([](const size_t& i) { return i; });
  auto con_node_impl = CI([](const size_t&) {});

  auto pro_node = P([](std::stop_source&) { return 0; });
  auto fun_node = F([](const size_t& i) { return i; });
  auto con_node = C([](const size_t&) {});

  SECTION("Check specified and deduced are same types") {
  }

  SECTION("Check polymorphism to node&") {
    CHECK(two_nodes(pro_node_impl, con_node_impl));
    CHECK(two_nodes(pro_node_impl, fun_node_impl));
    CHECK(two_nodes(fun_node_impl, con_node_impl));

    // No conversion from producer_node to node
    CHECK(two_nodes(pro_node, con_node));
    CHECK(two_nodes(pro_node, fun_node));
    CHECK(two_nodes(fun_node, con_node));
  }

  SECTION("Check some copying node (godbolt)") {
    auto shared_pro = node{pro_node};
    auto shared_fun = node{fun_node};
    auto shared_con = node{con_node};

    auto shared_nil = node{};
    shared_nil = shared_pro;
    CHECK(shared_nil == shared_pro);
  }
}

/**
 * Some dummy functions and classes to test node constructors
 * with.
 */
size_t dummy_source(std::stop_source&) {
  return size_t{};
}

size_t dummy_function(size_t) {
  return size_t{};
}

void dummy_sink(size_t) {
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
size_t dummy_source_t(std::stop_source&) {
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
 * Verify initializing producer_node and consumer_node with function, lambda,
 * in-line lambda, function object, bind, and rvalue bind.
 */
TEMPLATE_TEST_CASE(
    "SegementedNodes: Verify numerous API approaches, with edges",
    "[segmented_nodes]",
    (std::tuple<
        consumer_node<AsyncMover2, size_t>,
        producer_node<AsyncMover2, size_t>>),
    (std::tuple<
        consumer_node<AsyncMover3, size_t>,
        producer_node<AsyncMover3, size_t>>)) {
  using C = typename std::tuple_element<0, TestType>::type;
  using P = typename std::tuple_element<1, TestType>::type;

  SECTION("function") {
    P b{dummy_source};
    C c{dummy_sink};
    Edge g{*b, *c};
  }

  SECTION("lambda") {
    auto dummy_source_lambda = [](std::stop_source&) { return 0UL; };
    auto dummy_sink_lambda = [](size_t) {};

    P b{dummy_source_lambda};
    C c{dummy_sink_lambda};

    Edge g{*b, *c};
  }

  SECTION("inline lambda") {
    P b([](std::stop_source&) { return 0; });
    C c([](size_t) {});

    Edge g{*b, *c};
  }

  SECTION("function object") {
    auto a = dummy_source_class{};
    dummy_sink_class d{};

    P b{a};
    C c{d};

    Edge g{*b, *c};
  }

  SECTION("inline function object") {
    P b{dummy_source_class{}};
    C c{dummy_sink_class{}};

    Edge g{*b, *c};
  }

  SECTION("bind") {
    double x = 0.01;
    float y = -0.001;
    int z = 8675309;

    auto a = std::bind(dummy_bind_source, x);
    auto d = std::bind(dummy_bind_sink, y, std::placeholders::_1, z);

    P b{a};
    C c{d};

    Edge g{*b, *c};
  }

  SECTION("inline bind") {
    double x = 0.01;
    float y = -0.001;
    int z = 8675309;

    P b{std::bind(dummy_bind_source, x)};
    C c{std::bind(dummy_bind_sink, y, std::placeholders::_1, z)};

    Edge g{*b, *c};
  }

  SECTION("bind with move") {
    double x = 0.01;
    float y = -0.001;
    int z = 8675309;

    auto a = std::bind(dummy_bind_source, std::move(x));
    auto d = std::bind(dummy_bind_sink, y, std::placeholders::_1, std::move(z));

    P b{std::move(a)};
    C c{std::move(d)};

    Edge g{*b, *c};
  }
}

/**
 * Verify initializing producer_node, function_node, and consumer_node with
 * function, lambda, in-line lambda, function object, bind, and rvalue bind.
 * (This is a repeat of the previous test, but modified to include a
 * function_node.)
 */
TEMPLATE_TEST_CASE(
    "SegementedNodes: Verify various API approaches, including function_node",
    "[segmented_nodes]",
    (std::tuple<
        consumer_node<AsyncMover2, size_t>,
        function_node<AsyncMover2, size_t>,
        producer_node<AsyncMover2, size_t>>),
    (std::tuple<
        consumer_node<AsyncMover3, size_t>,
        function_node<AsyncMover3, size_t>,
        producer_node<AsyncMover3, size_t>>)) {
  using C = typename std::tuple_element<0, TestType>::type;
  using F = typename std::tuple_element<1, TestType>::type;
  using P = typename std::tuple_element<2, TestType>::type;

  SECTION("function") {
    P a{dummy_source};
    F b{dummy_function};
    C c{dummy_sink};

    Edge g{*a, *b};
    Edge h{*b, *c};
  }

  SECTION("lambda") {
    auto dummy_source_lambda = [](std::stop_source&) { return 0UL; };
    auto dummy_function_lambda = [](size_t) { return 0UL; };
    auto dummy_sink_lambda = [](size_t) {};

    P a{dummy_source_lambda};
    F b{dummy_function_lambda};
    C c{dummy_sink_lambda};

    Edge g{*a, *b};
    Edge h{*b, *c};
  }

  SECTION("inline lambda") {
    P a([](std::stop_source&) { return 0UL; });
    F b([](size_t) { return 0UL; });
    C c([](size_t) {});

    Edge g{*a, *b};
    Edge h{*b, *c};
  }

  SECTION("function object") {
    auto ac = dummy_source_class{};
    dummy_function_class fc{};
    dummy_sink_class dc{};

    P a{ac};
    F b{fc};
    C c{dc};

    Edge g{*a, *b};
    Edge h{*b, *c};
  }

  SECTION("inline function object") {
    P a{dummy_source_class{}};
    F b{dummy_function_class{}};
    C c{dummy_sink_class{}};

    Edge g{*a, *b};
    Edge h{*b, *c};
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

    Edge g{*a, *b};
    Edge h{*b, *c};
  }

  SECTION("inline bind") {
    double x = 0.01;
    float y = -0.001;
    int z = 8675309;

    P a{std::bind(dummy_bind_source, x)};
    F b{std::bind(dummy_bind_function, x, y, std::placeholders::_1)};
    C c{std::bind(dummy_bind_sink, y, std::placeholders::_1, z)};

    Edge i{*a, *b};
    Edge j{*b, *c};
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

    Edge i{*a, *b};
    Edge j{*b, *c};
  }
}




/**
 * Some dummy_monostate functions and classes to test node constructors
 * with.
 */
std::monostate dummy_monostate_source(std::stop_source&) {
  return std::monostate{};
}

std::monostate dummy_monostate_function(std::monostate) {
  return std::monostate{};
}

void dummy_monostate_sink(std::monostate) {
}

class dummy_monostate_source_class {
 public:
  std::monostate operator()(std::stop_source&) {
    return std::monostate{};
  }
};

class dummy_monostate_function_class {
 public:
  std::monostate operator()(const std::monostate&) {
    return std::monostate{};
  }
};

class dummy_monostate_sink_class {
 public:
  void operator()(std::monostate) {
  }
};

std::monostate dummy_monostate_bind_source(double) {
  return std::monostate{};
}

std::monostate dummy_monostate_bind_function(double, float, std::monostate) {
  return std::monostate{};
}

void dummy_monostate_bind_sink(std::monostate, float, const int&) {
}

/**
 * Some dummy_monostate function template and class templates to test node constructors
 * with.
 */
template <class Block = std::monostate>
std::monostate dummy_monostate_source_t(std::stop_source&) {
  return Block{};
}

template <class InBlock = std::monostate, class OutBlock = InBlock>
OutBlock dummy_monostate_function_t(InBlock) {
  return OutBlock{};
}

template <class Block = std::monostate>
void dummy_monostate_sink_t(const Block&) {
}

template <class Block = std::monostate>
class dummy_monostate_source_class_t {
 public:
  Block operator()() {
    return Block{};
  }
};

template <class InBlock = std::monostate, class OutBlock = InBlock>
class dummy_monostate_function_class_t {
 public:
  OutBlock operator()(const InBlock&) {
    return OutBlock{};
  }
};

template <class Block = std::monostate>
class dummy_monostate_sink_class_t {
 public:
  void operator()(Block) {
  }
};

template <class Block = std::monostate>
Block dummy_monostate_bind_source_t(double) {
  return Block{};
}

template <class InBlock = std::monostate, class OutBlock = InBlock>
OutBlock dummy_monostate_bind_function_t(double, float, InBlock) {
  return OutBlock{};
}

template <class Block = std::monostate>
void dummy_monostate_bind_sink_t(Block, float, const int&) {
}

TEMPLATE_TEST_CASE(
    "SegementedNodes: Verify various API approaches with monostate, including function_node",
    "[segmented_nodes]",
    (std::tuple<
        consumer_node<AsyncMover2, std::monostate>,
        function_node<AsyncMover2, std::monostate>,
        producer_node<AsyncMover2, std::monostate>>),
    (std::tuple<
        consumer_node<AsyncMover3, std::monostate>,
        function_node<AsyncMover3, std::monostate>,
        producer_node<AsyncMover3, std::monostate>>)) {
  using C = typename std::tuple_element<0, TestType>::type;
  using F = typename std::tuple_element<1, TestType>::type;
  using P = typename std::tuple_element<2, TestType>::type;

  SECTION("function") {
    P a{dummy_monostate_source};
    F b{dummy_monostate_function};
    C c{dummy_monostate_sink};

    Edge g{*a, *b};
    Edge h{*b, *c};
  }

    SECTION("lambda") {
      auto dummy_source_lambda = [](std::stop_source&) { return std::monostate{}; };
      auto dummy_function_lambda = [](std::monostate) { return std::monostate{}; };
      auto dummy_sink_lambda = [](std::monostate) {};

      P a{dummy_source_lambda};
      F b{dummy_function_lambda};
      C c{dummy_sink_lambda};

      Edge g{*a, *b};
      Edge h{*b, *c};
    }

    SECTION("inline lambda") {
      P a([](std::stop_source&) { return std::monostate{}; });
      F b([](std::monostate) { return std::monostate{}; });
      C c([](std::monostate) {});

      Edge g{*a, *b};
      Edge h{*b, *c};
    }

    SECTION("function object") {
      auto ac = dummy_monostate_source_class{};
      dummy_monostate_function_class fc{};
      dummy_monostate_sink_class dc{};

      P a{ac};
      F b{fc};
      C c{dc};

      Edge g{*a, *b};
      Edge h{*b, *c};
    }

    SECTION("inline function object") {
      P a{dummy_monostate_source_class{}};
      F b{dummy_monostate_function_class{}};
      C c{dummy_monostate_sink_class{}};

      Edge g{*a, *b};
      Edge h{*b, *c};
    }
    SECTION("bind") {
      double x = 0.01;
      float y = -0.001;
      int z = 8675309;

      auto ac = std::bind(dummy_monostate_bind_source, x);
      auto dc = std::bind(dummy_monostate_bind_sink, std::placeholders::_1, y, z);
      auto fc = std::bind(dummy_monostate_bind_function, x, y, std::placeholders::_1);

      P a{ac};
      F b{fc};
      C c{dc};

      Edge g{*a, *b};
      Edge h{*b, *c};
    }

    SECTION("inline bind") {
      double x = 0.01;
      float y = -0.001;
      int z = 8675309;

      P a{std::bind(dummy_monostate_bind_source, x)};
      F b{std::bind(dummy_monostate_bind_function, x, y, std::placeholders::_1)};
      C c{std::bind(dummy_monostate_bind_sink, std::placeholders::_1, y, z)};

      Edge i{*a, *b};
      Edge j{*b, *c};
    }

    SECTION("bind with move") {
      double x = 0.01;
      float y = -0.001;
      int z = 8675309;

      auto ac = std::bind(dummy_monostate_bind_source, std::move(x));
      auto dc = std::bind(
          dummy_monostate_bind_sink, std::placeholders::_1, std::move(y), std::move(z));
      auto fc = std::bind(
          dummy_monostate_bind_function, std::move(x), std::move(y), std::placeholders::_1);

      P a{std::move(ac)};
      F b{std::move(fc)};
      C c{std::move(dc)};

      Edge i{*a, *b};
      Edge j{*b, *c};
    }}