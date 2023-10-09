/**
 * @file experimental/tiledb/common/dag/graph/test/unit_mimo_taskgraph.cc
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2023 TileDB, Inc.
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

#include <cstdint>
#include <initializer_list>
#include <iostream>
#include <memory>
#include <tuple>
#include <type_traits>
#include "experimental/tiledb/common/dag/execution/duffs.h"
#include "experimental/tiledb/common/dag/graph/taskgraph.h"
#include "experimental/tiledb/common/dag/nodes/detail/segmented/edge_node_ctad.h"
#include "experimental/tiledb/common/dag/nodes/detail/segmented/mimo.h"
#include "experimental/tiledb/common/dag/nodes/segmented_nodes.h"
#include "unit_taskgraph.h"

using namespace tiledb::common;

TEST_CASE("mimo_node: Verify various API approaches", "[segmented_mimo]") {
  [[maybe_unused]] mimo_node<
      DuffsMover3,
      std::tuple<size_t, int>,
      DuffsMover3,
      std::tuple<size_t, double>>
      x{};
}
#if 0
template <class MimoNode, size_t portnum>
struct Proxy {
  constexpr static const size_t portnum_ {portnum};
  MimoNode* node_ptr_;
  Proxy(MimoNode& node) : node_ptr_{&node} {}
};

template <size_t N, class T>
auto make_proxy(const T& u) {
  return Proxy<std::remove_reference_t<decltype(u)>, N>(u);
}
template<typename T>
struct is_proxy: std::false_type {};

template<typename T, size_t portnum>
struct is_proxy<Proxy<T, portnum>> : std::true_type {};

template <class T>
constexpr const bool is_proxy_v {is_proxy<T>::value};

template <class From, class To>
std::enable_if_t<(!is_proxy_v<From> && !is_proxy_v<To>), void>
make_edge(From& from, To& to){
  connect(from, to);
  Edge(from, to);
}

template <class From, class To>
std::enable_if_t<!is_proxy_v<From> && is_proxy_v<To>, void>
make_edge(From&& from, To&& to){
  connect(from, *(to.node_ptr_));
  Edge(from, std::get<To::portnum_>((*(to.node_ptr_))->get_input_ports()));
}

template <class From, class To>
std::enable_if_t<(is_proxy_v<From> && !is_proxy_v<To>), void>
make_edge(From&& from, To&& to){
  connect(*(from.node_ptr_), to);
  Edge(std::get<From::portnum_>((*(from.node_ptr_))->get_output_ports()), to);
}

template <class From, class To>
std::enable_if_t<is_proxy_v<From> && is_proxy_v<To>, void>
make_edge(From&& from, To&& to){
  connect(*(from.node_ptr_), *(to.node_ptr_));
  Edge(std::get<From::portnum_>((*(from.node_ptr_))->get_output_ports()),
                  std::get<To::portnum_>((*(to.node_ptr_))->get_input_ports()));
}
#endif
TEST_CASE("mimo_node: Verify various Proxy API approaches", "[proxy]") {
  auto graph = TaskGraph<DuffsScheduler<node>>();
  [[maybe_unused]] mimo_node<
      DuffsMover3,
      std::tuple<size_t, int>,
      DuffsMover3,
      std::tuple<size_t, double>>
      x{};
  auto u = initial_node(
      graph, [](std::stop_source&) { return std::tuple<char*, double>{}; });
  auto v = mimo(graph, [](const std::tuple<char*, double>&) {
    return std::tuple<size_t, size_t, char>{};
  });
  auto w = terminal_node(graph, [](const std::tuple<size_t, size_t, char>&) {});

  auto y = initial_node(
      graph, [](std::stop_source&) { return std::tuple<char*, double>{}; });
  auto z = terminal_node(graph, [](const std::tuple<char*, double>&) {});

  auto s = initial_mimo(
      graph, [](std::stop_source&) { return std::tuple<char*, double>{}; });
  auto t = terminal_mimo(graph, [](const std::tuple<char*, double>&) {});

  SECTION("Very Simple connect") {
    connect(u, v);
    connect(v, w);
  }
  SECTION("Simple make_proxy") {
    make_proxy<0>(v);
    make_proxy<1>(v);
  }
}

TEST_CASE(
    "mimo_node: Verify construction with simple function", "[segmented_mimo]") {
  [[maybe_unused]] mimo_node<
      DuffsMover2,
      std::tuple<size_t, size_t>,
      DuffsMover3,
      std::tuple<size_t, char*>>
      x{[](std::tuple<size_t, size_t>) { return std::tuple<size_t, char*>{}; }};
}

TEST_CASE("TaskGraph: Very simple mimo compilation", "[taskgraph]") {
  auto graph = TaskGraph<DuffsScheduler<node>>();

  SECTION("One in one out mimo") {
    auto aa = [](const std::tuple<size_t>&) { return std::tuple<size_t>{}; };
    auto bb = std::function<std::tuple<size_t>(const std::tuple<size_t>&)>{};

    auto fun0 = graph.mimo(bb);
    auto fun1 = graph.mimo(
        [](const std::tuple<size_t>&) { return std::tuple<size_t>{}; });
    auto fun3 = graph.mimo(aa);
    auto fun2 = mimo(
        graph, [](const std::tuple<size_t>&) { return std::tuple<size_t>{}; });
    auto fun4 = mimo(graph, aa);
  }

  SECTION("Two in three out mimo") {
    auto aa = [](const std::tuple<size_t, double>&) {
      return std::tuple<float, char*, int>{};
    };

    auto fun3 = graph.mimo(aa);
    auto fun4 = mimo(graph, aa);
  }
}

/**
 * Some dummy functions and classes to test node constructors
 * with.
 */
size_t dummy_source(std::stop_source&) {
  return size_t{};
}

auto dummy_mimo_source(std::stop_source) {
  return std::tuple<size_t>{};
}

auto dummy_mimo_function(const std::tuple<size_t>& in) {
  return in;
}

auto dummy_mimo_function_2_3(
    [[maybe_unused]] const std::tuple<size_t, uint32_t>& in) {
  return std::tuple<uint16_t, uint32_t, size_t>{};
}

auto dummy_mimo_function_3_2(
    [[maybe_unused]] const std::tuple<size_t, uint32_t, uint16_t>& in) {
  return std::tuple<uint32_t, size_t>{};
}

size_t dummy_function(const size_t& in) {
  return in;
}

void dummy_sink(size_t) {
}

void dummy_mimo_sink(const std::tuple<size_t>&) {
}

class dummy_source_class {
 public:
  size_t operator()(std::stop_source&) {
    return size_t{};
  }
};

class dummy_mimo_function_class {
 public:
  auto operator()(const std::tuple<size_t>& in) const {
    return std::tuple<size_t>(in);
  }
};

class dummy_mimo_function_class_3_2 {
 public:
  auto operator()(
      [[maybe_unused]] const std::tuple<size_t, uint32_t, uint16_t>& in) {
    return std::tuple<uint32_t, size_t>{};
  }
};

class dummy_mimo_function_class_2_3 {
 public:
  auto operator()([[maybe_unused]] const std::tuple<size_t, uint32_t>& in) {
    return std::tuple<uint16_t, uint32_t, size_t>{};
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

class dummy_mimo_sink_class {
 public:
  void operator()(std::tuple<size_t>&) {
  }
};

TEST_CASE("mimo_node: Verify making simple mimo nodes", "[mimo_taskgraph]") {
  auto graph = TaskGraph<DuffsScheduler<node>>();

  SECTION("plain function") {
    auto u = initial_node(graph, dummy_source);
    auto v = transform_node(graph, dummy_function);
    auto w = terminal_node(graph, dummy_sink);
  }

  SECTION("function") {
    auto u = initial_node(graph, dummy_source);
    auto v = mimo(graph, dummy_mimo_function);
    auto w = terminal_node(graph, dummy_sink);
  }

  SECTION("lambda") {
    auto l = [](const std::tuple<size_t>&) { return std::tuple<size_t>{}; };
    auto u = initial_node(graph, dummy_source);
    auto v = mimo(graph, l);
    auto w = terminal_node(graph, dummy_sink);
  }

  SECTION("inline lambda") {
    auto u = initial_node(graph, dummy_source);
    auto v = mimo(
        graph, [](const std::tuple<size_t>&) { return std::tuple<size_t>{}; });
    auto w = terminal_node(graph, dummy_sink);
  }

  SECTION("function object") {
    auto x = dummy_mimo_function_class{};
    auto u = initial_node(graph, dummy_source);
    auto v = mimo(graph, x);
    auto w = terminal_node(graph, dummy_sink);
  }

  SECTION("inline function object") {
    auto u = initial_node(graph, dummy_source);
    auto v = mimo(graph, dummy_mimo_function_class{});
    auto w = terminal_node(graph, dummy_sink);
  }

  SECTION("M by N") {
    auto u = mimo(graph, dummy_mimo_function);          // 1 by 1
    auto u_3_2 = mimo(graph, dummy_mimo_function_3_2);  // 3 by 2
    auto u_2_3 = mimo(graph, dummy_mimo_function_2_3);  // 2 by 3

    auto v = mimo(graph, dummy_mimo_function_class{});          // 1 by 1
    auto v_3_2 = mimo(graph, dummy_mimo_function_class_3_2{});  // 3 by 2
    auto v_2_3 = mimo(graph, dummy_mimo_function_class_2_3{});  // 2 by 3
  }
}

TEST_CASE(
    "mimo_node: Verify making simple mimo nodes with edges",
    "[mimo_taskgraph]") {
  auto graph = TaskGraph<DuffsScheduler<node>>();

  SECTION("plain function") {
    auto u = initial_node(graph, dummy_source);
    auto v = transform_node(graph, dummy_function);
    auto w = terminal_node(graph, dummy_sink);
    make_edge(graph, u, v);
    make_edge(graph, v, w);
  }

  SECTION("mimo function, graph.make_edge member function") {
    auto u = initial_node(graph, dummy_source);
    auto v = mimo(graph, dummy_mimo_function);
    auto w = terminal_node(graph, dummy_sink);

    graph.make_edge(u, make_proxy<0>(v));
    graph.make_edge(make_proxy<0>(v), w);
  }

  SECTION("mimo function, make_edge free function") {
    auto u = initial_node(graph, dummy_source);
    auto v = mimo(graph, dummy_mimo_function);
    auto w = terminal_node(graph, dummy_sink);

    make_edge(graph, u, make_proxy<0>(v));
    make_edge(graph, make_proxy<0>(v), w);
  }

  SECTION("lambda") {
    auto l = [](const std::tuple<size_t>&) { return std::tuple<size_t>{}; };
    auto u = initial_node(graph, dummy_source);
    auto v = mimo(graph, l);
    auto w = terminal_node(graph, dummy_sink);
    make_edge(graph, u, make_proxy<0>(v));
    make_edge(graph, make_proxy<0>(v), w);
  }

  SECTION("inline lambda") {
    auto u = initial_node(graph, dummy_source);
    auto v = mimo(
        graph, [](const std::tuple<size_t>&) { return std::tuple<size_t>{}; });
    auto w = terminal_node(graph, dummy_sink);
    make_edge(graph, u, make_proxy<0>(v));
    make_edge(graph, make_proxy<0>(v), w);
  }

  SECTION("function object") {
    auto x = dummy_mimo_function_class{};
    auto u = initial_node(graph, dummy_source);
    auto v = mimo(graph, x);
    auto w = terminal_node(graph, dummy_sink);
    make_edge(graph, u, make_proxy<0>(v));
    make_edge(graph, make_proxy<0>(v), w);
  }

  SECTION("inline function object") {
    auto u = initial_node(graph, dummy_source);
    auto v = mimo(graph, dummy_mimo_function_class{});
    auto w = terminal_node(graph, dummy_sink);
    make_edge(graph, u, make_proxy<0>(v));
    make_edge(graph, make_proxy<0>(v), w);
  }

  SECTION("M by N") {
    auto u = mimo(graph, dummy_mimo_function);  // 1 by 1, size_t
    auto i32 = initial_mimo(graph, [](std::stop_source) {
      return std::tuple<uint32_t>{};
    });  // 0 by 1, uint32_t
    auto i16 = initial_mimo(graph, [](std::stop_source) {
      return std::tuple<uint16_t>{};
    });  // 0 by 1, uint16_t
    auto o32 = terminal_mimo(
        graph, [](const std::tuple<uint32_t>&) {});  // 1 by 0, uint32_t
    auto o16 = terminal_mimo(
        graph, [](const std::tuple<uint16_t>&) {});  // 1 by 0, uint16_t
    auto u32 = mimo(graph, [](const std::tuple<uint32_t>&) {
      return std::tuple<uint32_t>{};
    });  // 0 by 1, uint32_t
    auto u_3_2 =
        mimo(graph, dummy_mimo_function_3_2);  // 3 by 2  size_t, uint32_t,
                                               // uint16_t -> uint32_t, size_t
    auto u_2_3 =
        mimo(graph, dummy_mimo_function_2_3);  // 2 by 3  size_t, uint32_t ->
                                               // uint16_t, uint32_t, size_t

    SECTION("u_3_2 out") {
      make_edge(graph, make_proxy<0>(u), make_proxy<0>(u_3_2));
      make_edge(graph, make_proxy<0>(u32), make_proxy<1>(u_3_2));
      make_edge(graph, make_proxy<0>(i16), make_proxy<2>(u_3_2));
    }

    SECTION("u_2_3 out") {
      make_edge(graph, make_proxy<0>(u), make_proxy<0>(u_2_3));
      make_edge(graph, make_proxy<0>(u32), make_proxy<1>(u_2_3));
    }

    SECTION("u_2_3 in") {
      make_edge(graph, make_proxy<0>(u_2_3), make_proxy<0>(o16));
      make_edge(graph, make_proxy<1>(u_2_3), make_proxy<0>(o32));
      make_edge(graph, make_proxy<2>(u_2_3), make_proxy<0>(u));
    }

    SECTION("u_3_2 in") {
      make_edge(graph, make_proxy<0>(u_3_2), make_proxy<0>(u32));
      make_edge(graph, make_proxy<1>(u_3_2), make_proxy<0>(u));
    }

    SECTION("u_2_3  x u_3_2") {
      make_edge(graph, make_proxy<0>(u_2_3), make_proxy<2>(u_3_2));
      make_edge(graph, make_proxy<1>(u_2_3), make_proxy<1>(u_3_2));
      make_edge(graph, make_proxy<2>(u_2_3), make_proxy<0>(u_3_2));
    }

    SECTION("u_3_2  x u_2_3") {
      make_edge(graph, make_proxy<0>(u_3_2), make_proxy<1>(u_2_3));
      make_edge(graph, make_proxy<1>(u_3_2), make_proxy<0>(u_2_3));
    }

    SECTION("function to function_class") {
      auto u = mimo(graph, dummy_mimo_function);
      auto v = mimo(graph, dummy_mimo_function_class{});
      make_edge(graph, make_proxy<0>(u), make_proxy<0>(v));
    }

    auto v = mimo(graph, dummy_mimo_function_class{});          // 1 by 1
    auto v_3_2 = mimo(graph, dummy_mimo_function_class_3_2{});  // 3 by 2
    auto v_2_3 = mimo(graph, dummy_mimo_function_class_2_3{});  // 2 by 3
  }
}

TEST_CASE("mimo_node: Run a simple graph", "[mimo_taskgraph]") {
  auto graph = TaskGraph<DuffsScheduler<node>>();

  SECTION("execute simple pipeline") {
    std::vector<size_t> vec;
    auto u = initial_node(graph, [](std::stop_source stop) {
      static size_t i = 0;
      if (i < 10) {
        return i++;
      }
      stop.request_stop();
      return 0UL;
    });

    auto v = mimo(graph, [](const std::tuple<size_t>& t) {
      return std::tuple<size_t>{std::get<0>(t) + 1};
    });
    auto w =
        terminal_node(graph, [&vec](const size_t& t) { vec.push_back(t); });
    make_edge(graph, u, make_proxy<0>(v));
    make_edge(graph, make_proxy<0>(v), w);

    graph.sync_wait();
    CHECK(std::equal(
        vec.begin(),
        vec.end(),
        std::vector<size_t>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}.begin()));
  }

  SECTION("execute simple pipeline all mimo") {
    std::vector<size_t> vec;
    auto u = initial_mimo(graph, [](std::stop_source stop) {
      static size_t i = 0;
      if (i < 10) {
        return std::make_tuple(i++);
      }
      stop.request_stop();
      return std::make_tuple(0UL);
    });

    auto v = mimo(graph, [](const std::tuple<size_t>& t) {
      return std::tuple<size_t>{std::get<0>(t) + 1};
    });
    auto w = terminal_mimo(graph, [&vec](const std::tuple<size_t>& t) {
      vec.push_back(std::get<0>(t));
    });
    make_edge(graph, make_proxy<0>(u), make_proxy<0>(v));
    make_edge(graph, make_proxy<0>(v), make_proxy<0>(w));

    graph.sync_wait();
    CHECK(std::equal(
        vec.begin(),
        vec.end(),
        std::vector<size_t>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}.begin()));
  }
}
