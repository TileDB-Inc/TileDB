/**
 * @file experimental/tiledb/common/dag/graph/test/unit_taskgraph.cc
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
 */

#include <numeric>

#include "unit_taskgraph.h"
#include "experimental/tiledb/common/dag/execution/duffs.h"
#include "experimental/tiledb/common/dag/graph/taskgraph.h"
#include "experimental/tiledb/common/dag/nodes/segmented_nodes.h"

using namespace tiledb::common;

TEST_CASE("TaskGraph: Trivial test", "[taskgraph]") {
  CHECK(true);
}

TEST_CASE("TaskGraph: Default construction", "[taskgraph]") {
  auto graph = TaskGraph<DuffsScheduler<node>>();
}

TEST_CASE("TaskGraph: Default construction + initial node", "[taskgraph]") {
  auto graph = TaskGraph<DuffsScheduler<node>>();

  initial_node(
      graph, []([[maybe_unused]] std::stop_source stop) { return 0UL; });
}

void bar(const size_t) {
}

TEST_CASE("TaskGraph: Default construction + terminal node", "[taskgraph]") {
  auto graph = TaskGraph<DuffsScheduler<node>>();

  auto w = terminal_node(graph, bar);
  // auto foo = [](size_t){};
  // auto u = terminal_node(graph, foo);
  // auto v = terminal_node(graph, [](size_t) {});
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
  size_t operator()(size_t) {
    return size_t{};
  }
};

class dummy_sink_class {
 public:
  void operator()(size_t) {
  }
};

size_t dummy_bind_source(std::stop_source, double) {
  return size_t{};
}

size_t dummy_bind_function(double, float, size_t) {
  return size_t{};
}

void dummy_bind_sink(size_t, float, int) {
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
  OutBlock operator()(InBlock) {
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
Block dummy_bind_source_t(std::stop_source, double) {
  return Block{};
}

template <class InBlock = size_t, class OutBlock = InBlock>
OutBlock dummy_bind_function_t(double, float, InBlock) {
  return OutBlock{};
}

template <class Block = size_t>
void dummy_bind_sink_t(Block, float, const int) {
}

TEST_CASE(
    "TaskGraph: Initial and terminal node construction with various function "
    "types",
    "[taskgraph]") {
  auto graph = TaskGraph<DuffsScheduler<node>>();

  SECTION("function") {
    auto u = initial_node(graph, dummy_source);
    auto x = graph.terminal_node(dummy_sink);

    auto w = terminal_node(graph, dummy_sink);
  }

  SECTION("lambda") {
    auto dummy_source_lambda = [](std::stop_source&) { return 0UL; };
    auto dummy_sink_lambda = [](size_t) {};
    auto u = initial_node(graph, dummy_source_lambda);
    auto x = graph.terminal_node(dummy_sink_lambda);
    auto w = terminal_node(graph, dummy_sink_lambda);
  }

  SECTION("inline lambda") {
    auto u = initial_node(graph, [](std::stop_source&) { return 0UL; });
    auto w = terminal_node(graph, [](const size_t&) {});

    auto x = initial_node(graph, [](std::stop_source) { return 0UL; });
    auto z = terminal_node(graph, [](const size_t) {});

    auto a = initial_node(graph, [](std::stop_source&) { return 0UL; });
    auto b = graph.terminal_node([](size_t&) {});

    auto c = initial_node(graph, [](std::stop_source) { return 0UL; });
    // auto d = terminal_node(graph, [](size_t) {});
  }

  SECTION("function object") {
    auto a = dummy_source_class();
    auto b = dummy_sink_class();
    auto u = initial_node(graph, a);
    auto w = terminal_node(graph, b);
  }

  SECTION("inline function object") {
    auto u = initial_node(graph, dummy_source_class());
    auto w = terminal_node(graph, dummy_sink_class());
  }

  // Bind just ain't gonna work
#if 0
  SECTION("bind") {
    double x = 0.01;
    float y = -0.001;
    int z = 8675309;

    auto a = std::bind(dummy_bind_source, std::placeholders::_1, x);
    auto d = std::bind(dummy_bind_sink, std::placeholders::_1, y, z);

    auto u = initial_node(graph, a);
    auto w = terminal_node(graph, [](size_t x) { dummy_bind_sink(x, 0.0, 0); });
    auto ww = graph.terminal_node(d);
  }

  SECTION("inline bind") {
    double x = 0.01;
    float y = -0.001;
    int z = 8675309;

    auto u = initial_node ( graph, std::bind(dummy_bind_source, std::placeholders::_1, x) );
    auto w = terminal_node (graph, std::bind(dummy_bind_sink, y, std::placeholders::_1, z));
  }
#endif
}

TEST_CASE(
    "TaskGraph: Initial, terminal, and transform node construction with "
    "various function types",
    "[taskgraph]") {
  auto graph = TaskGraph<DuffsScheduler<node>>();

  SECTION("function") {
    auto u = initial_node(graph, dummy_source);
    auto v = transform_node(graph, dummy_function);
    auto w = terminal_node(graph, dummy_sink);
  }

  SECTION("lambda") {
    auto dummy_source_lambda = [](std::stop_source&) { return 0UL; };
    [[maybe_unused]] auto dummy_function_lambda = [](size_t) { return 0UL; };
    [[maybe_unused]] auto dummy_sink_lambda = [](size_t) {};
    [[maybe_unused]] auto u = initial_node(graph, dummy_source_lambda);
    [[maybe_unused]] auto v = transform_node(graph, [](size_t) { return 0UL; });
    [[maybe_unused]] auto x = graph.terminal_node(dummy_sink_lambda);
    [[maybe_unused]] auto w = terminal_node(graph, dummy_sink_lambda);
  }

  SECTION("inline lambda") {
    auto u = initial_node(graph, [](std::stop_source&) { return 0UL; });
    auto v = transform_node(graph, [](size_t) { return 0UL; });
    auto w = terminal_node(graph, [](size_t) {});

    auto x = initial_node(graph, [](std::stop_source) { return 0UL; });
    auto y = transform_node(graph, [](size_t) { return 0UL; });
    auto z = terminal_node(graph, [](const size_t) {});

    auto a = initial_node(graph, [](std::stop_source&) { return 0UL; });
    auto b = transform_node(graph, [](size_t&) { return 0UL; });
    auto c = terminal_node(graph, [](size_t&) {});

    auto d = initial_node(graph, [](std::stop_source) { return 0UL; });
    auto e = transform_node(graph, [](size_t&) { return 0UL; });
    auto f = terminal_node(graph, [](const size_t&) {});
  }

  SECTION("function object") {
    auto a = dummy_source_class();
    auto b = dummy_function_class();
    auto c = dummy_sink_class();
    auto u = initial_node(graph, a);
    auto v = transform_node(graph, b);
    auto w = terminal_node(graph, c);
  }

  SECTION("inline function object") {
    auto u = initial_node(graph, dummy_source_class());
    auto v = transform_node(graph, dummy_function_class());
    auto w = terminal_node(graph, dummy_sink_class());
  }
}

TEST_CASE("TaskGraph: Task graph construction + edges", "[taskgraph]") {
  auto graph = TaskGraph<DuffsScheduler<node>>();

  SECTION("function") {
    auto u = initial_node(graph, dummy_source);
    auto v = transform_node(graph, dummy_function);
    auto w = terminal_node(graph, dummy_sink);

    make_edge(graph, u, v);
    make_edge(graph, v, w);
  }

  SECTION("lambda") {
    auto dummy_source_lambda = [](std::stop_source&) { return 0UL; };
    [[maybe_unused]] auto dummy_function_lambda = [](size_t) { return 0UL; };
    [[maybe_unused]] auto dummy_sink_lambda = [](size_t) {};
    auto u = initial_node(graph, dummy_source_lambda);
    auto v = transform_node(graph, [](size_t) { return 0UL; });
    auto w = terminal_node(graph, dummy_sink_lambda);
    make_edge(graph, u, v);
    make_edge(graph, v, w);
  }

  SECTION("inline lambda") {
    auto u = initial_node(graph, [](std::stop_source&) { return 0UL; });
    auto v = transform_node(graph, [](size_t) { return 0UL; });
    auto w = terminal_node(graph, [](size_t) {});

    auto x = initial_node(graph, [](std::stop_source) { return 0UL; });
    auto y = transform_node(graph, [](size_t) { return 0UL; });
    auto z = terminal_node(graph, [](const size_t) {});

    make_edge(graph, u, v);
    make_edge(graph, v, w);
    make_edge(graph, x, y);
    make_edge(graph, y, z);
  }

  SECTION("function object") {
    auto a = dummy_source_class();
    auto b = dummy_function_class();
    auto c = dummy_sink_class();
    auto u = initial_node(graph, a);
    auto v = transform_node(graph, b);
    auto w = terminal_node(graph, c);

    make_edge(graph, u, v);
    make_edge(graph, v, w);
  }

  SECTION("inline function object") {
    auto u = initial_node(graph, dummy_source_class());
    auto v = transform_node(graph, dummy_function_class());
    auto w = terminal_node(graph, dummy_sink_class());

    make_edge(graph, u, v);
    make_edge(graph, v, w);
  }
}

TEST_CASE("TaskGraph: Schedule", "[taskgraph]") {
  auto graph = TaskGraph<DuffsScheduler<node>>();

  auto num_threads = GENERATE(1, 2, 3, 4, 5, 8, 17);
  auto sched = DuffsScheduler<node>(num_threads);

  auto u = initial_node(graph, [](std::stop_source stop) {
    stop.request_stop();
    return 0UL;
  });
  auto v = transform_node(graph, [](size_t) { return 0UL; });
  auto w = terminal_node(graph, [](size_t) {});

  make_edge(graph, u, v);
  make_edge(graph, v, w);

  schedule(graph);
  sync_wait(graph);
}

TEST_CASE("TaskGraph: Different types along graph", "[taskgraph]") {
  auto graph = TaskGraph<DuffsScheduler<node>>();
  auto aa = initial_node(graph, [](std::stop_source) { return 0UL; });
  auto bb = transform_node(graph, [](size_t) { return double{}; });
  // auto cc = terminal_node(graph, [](const size_t&){ });
  make_edge(graph, aa, bb);
}

TEST_CASE("TaskGraph: Run Passing Integers", "[taskgraph]") {
  auto graph = TaskGraph<DuffsScheduler<node>>();

  auto num_threads = GENERATE(1, 2, 3, 4, 5, 8, 17);
  auto sched = DuffsScheduler<node>(num_threads);

  size_t problem_size = 1337;
  size_t rounds = problem_size;

  std::vector<size_t> input(rounds);
  std::vector<size_t> output(rounds);

  std::iota(input.begin(), input.end(), 19);
  std::fill(output.begin(), output.end(), 0);
  auto i = input.begin();
  auto j = output.begin();

  if (rounds != 0) {
    CHECK(std::equal(input.begin(), input.end(), output.begin()) == false);
  }

  auto p = graph.initial_node([problem_size, &i, &input](
                                  std::stop_source& stop_source) {
    if (std::distance(input.begin(), i) >= static_cast<long>(problem_size)) {
      stop_source.request_stop();
      return *(input.begin()) + 1;
    }
    return (*i++) + 1;
  });

  auto f = transform_node(graph, [](std::size_t k) { return k - 1; });

  auto c = terminal_node(graph, [&j](std::size_t k) { *j++ = k; });

  SECTION("Producer, Function, and Consumer, submit") {
    make_edge(graph, p, f);
    make_edge(graph, f, c);
    schedule(graph);
    sync_wait(graph);
  }

  CHECK(rounds != 0);
  CHECK(rounds == problem_size);

  CHECK(input.begin() != i);
  CHECK(input.size() == rounds);
  CHECK(output.size() == rounds);

  CHECK(std::equal(input.begin(), i, output.begin()));

  CHECK(std::distance(input.begin(), i) == static_cast<long>(rounds));
}
