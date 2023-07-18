/**
 * @file flow_graph/test/test_graph_specification.cc
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

#include "tdb_catch.h"

#include "../library/dynamic/to_dynamic_reference.h"
#include "../library/platform/basic_execution_platform.h"
#include "test_graphs.h"

using namespace tiledb::flow_graph;
using namespace tiledb::flow_graph::test;

template <class>
class TrivialClassTemplate {};

TEST_CASE("node_body concept is not trivial") {
  /*
   * The trivial class template should not satisfy the `node_body` concept.
   */
  STATIC_CHECK(!node_body<TrivialClassTemplate>);
}

//----------------------------------
// Graph
//----------------------------------

using Gr = DummyTestGraph<void>;
using Gr2 = DummyTestGraphActualTupleOfNodes<void>;

TEST_CASE("dummy graph instance, void", "[fgl][dummy]") {
  static_assert(
      graph_static_specification<DummyTestGraph<void>>,
      "It's supposed to be a specification graph");
  static_assert(
      graph_static_specification<DummyTestGraphActualTupleOfNodes<void>>,
      "It's supposed to be a specification graph");
  constexpr DummyTestGraph<void> g{};

  static_assert(is_reference_element_of(Gr::a, Gr::nodes));
  static_assert(is_reference_element_of(Gr2::a, Gr2::nodes));
  static_assert(is_reference_element_of(Gr::b, Gr::nodes));
  static_assert(is_reference_element_of(Gr2::b, Gr2::nodes));
}

TEMPLATE_LIST_TEST_CASE(
    "dummy graph instance", "[fgl][dummy]", AllTheDummyTestGraphs) {
  DYNAMIC_SECTION(TestGraphTraits<TestType>::name) {
    STATIC_CHECK(graph_static_specification<TestType>);
    constexpr TestType g{};

    STATIC_CHECK(std::tuple_size_v<decltype(TestType::nodes)> == 2);
    STATIC_CHECK(std::tuple_size_v<decltype(TestType::edges)> == 1);

    STATIC_CHECK(is_reference_element_of(TestType::a, TestType::nodes));
    STATIC_CHECK(is_reference_element_of(TestType::b, TestType::nodes));
  }
}

TEMPLATE_LIST_TEST_CASE(
    "dummy graph, static specification to dynamic",
    "[fgl][dummy]",
    AllTheDummyTestGraphs) {
  DYNAMIC_SECTION(TestGraphTraits<TestType>::name) {
    // graph in bulk
    using dynamic_specification_type =
        ToDynamicReference<TestType, BasicExecutionPlatform>;
    STATIC_CHECK(graph_dynamic_specification<dynamic_specification_type>);
    dynamic_specification_type g{};
    CHECK(g.nodes_size() == 2);
    REQUIRE(g.nodes_size() >= 2);
    auto edges{g.edges()};
    CHECK(g.edges_size() == 1);
    REQUIRE(g.edges_size() >= 1);

    // graph elements
    auto nodes{g.nodes()};
    auto initial{nodes[0]};
    STATIC_CHECK(dynamic_specification::node<decltype(initial)>);
    CHECK(initial.inputs_size() == 0);
    CHECK(initial.outputs_size() == 1);
    REQUIRE(initial.outputs_size() >= 1);
    auto initial_port{initial.outputs()[0]};

    auto final{nodes[1]};
    STATIC_CHECK(dynamic_specification::node<decltype(final)>);
    CHECK(final.inputs_size() == 1);
    CHECK(final.outputs_size() == 0);
    REQUIRE(final.inputs_size() >= 1);
    auto final_port{final.inputs()[0]};

    auto edge{edges[0]};
    /* No checks for edges at this time */
  }
}

//---------------------
//----------------------------------
//-------------------------------------------------------
