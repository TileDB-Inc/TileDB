/**
 * @file flow_graph/test/test_execution_graph.cc
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

/*
 * This file performs whole-system instantiation tests. As such it requires all
 * the stages:
 *   1. Static graph specifications. These are in `test_graphs.h`.
 *   2. Converter from static to dynamic specification. This is in
 *      "graph_specification.h"
 *   3. An execution platform. The basic one is used here.
 *   4. An execution graph class. This is in "flow_graph.h".
 */

#include "../library/static/static_zero_graph.h"
#include "test_graphs.h"

// the system definition
#include "../system.h"
// for the test
#include "../system/execution_graph.h"    // for `concept execution_graph`
#include "../system/flow_graph_system.h"  // for `concept flow_graph_system`

using namespace tiledb::flow_graph;
using namespace tiledb::flow_graph::test;

using S = tiledb::flow_graph::ReferenceSystem;

template <>
struct TestGraphTraits<static_specification::ZeroGraph> {
  static constexpr std::string_view name{"ZeroGraph"};
};

using AllTheTestGraphs = std::tuple<
    static_specification::ZeroGraph,
    DummyTestGraph<void>,
    DummyTestGraphActualTupleOfNodes<void>>;

TEST_CASE("ReferenceSystem soundness", "[flow_graph][ReferenceSystem]") {
  STATIC_CHECK(flow_graph_system<S>);
}

TEMPLATE_LIST_TEST_CASE(
    "dummy graph, execution ", "[flow_graph]", AllTheTestGraphs) {
  DYNAMIC_SECTION(TestGraphTraits<TestType>::name) {
    using GDS =
        S::static_to_dynamic_transformer<TestType, S::execution_platform>;
    STATIC_CHECK(graph_dynamic_specification<GDS>);
    S::dynamic_to_execution_transformer<GDS> x{GDS{}};
    STATIC_CHECK(execution_graph<decltype(x)>);
  }
}
