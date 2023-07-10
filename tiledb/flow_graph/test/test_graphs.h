/**
 * @file flow_graph/test/test_graphs.h
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

#include <string_view>

#include "../graph.h"
#include "../library/static/dummy.h"

namespace tiledb::flow_graph::test {

/**
 * Trait class for annotating DYNAMIC_SECTION
 *
 * @tparam T a class that appears in a list for TEMPLATE_LIST_TEST_CASE
 */
template <class T>
struct TestGraphTraits;

/*
 * The dummy graph is here for construction tests. It's the simplest non-
 * trivial graph, with two nodes with one port each and a single edge.
 *
 * There are multiple versions of the dummy graph with the same topology. They
 * are present in order to ensure that implementation-dependent details don't
 * leak through the concept. Node and edge lists must be tuple-like, but they
 * don't need to be `std::tuple` itself.
 *
 * The class `reference_tuple` is available for compact specification of node
 * lists. Use of this class avoids repeating the node definition type in a node
 * list.
 *
 * At present an analogous class for specifying edges is not available, such as
 * `edge_tuple`. Its benefit would be to avoid an edge type altogether for the
 * ordinary case when the graph specification only uses default edge parameters
 * and does not override any edge type.
 */

/**
 * The test graph has a dummy output node, a dummy input node, and a dummy edge.
 * It will instantiate, but it all starts out in a terminal state, so it won't
 * do anything.
 */
template <class T>
class DummyTestGraph {
 public:
  constexpr static struct invariant_type {
    constexpr static bool i_am_graph_static_specification{true};
  } invariants;
  constexpr static DummyOutputNodeSpecification<T> a{};
  constexpr static DummyInputNodeSpecification<T> b{};
  constexpr static reference_tuple nodes{a, b};
  constexpr static std::tuple edges{
      DummyEdgeSpecification{a, a.output, b, b.input}};
};

template <>
struct TestGraphTraits<DummyTestGraph<void>> {
  static constexpr std::string_view name{"DummyTestGraph<void>"};
};

/**
 * An alternate and more verbose way to specify a node list. This uses
 * `std::tuple` for its node list rather than `reference_tuple`.
 *
 * @tparam T
 */
template <class T>
class DummyTestGraphActualTupleOfNodes {
 public:
  constexpr static struct invariant_type {
    constexpr static bool i_am_graph_static_specification{true};
  } invariants;
  constexpr static DummyOutputNodeSpecification<T> a{};
  constexpr static DummyInputNodeSpecification<T> b{};
  constexpr static std::tuple<
      const DummyOutputNodeSpecification<T>&,
      const DummyInputNodeSpecification<T>&>
      nodes{a, b};
  constexpr static std::tuple edges{
      DummyEdgeSpecification{a, a.output, b, b.input}};
};

template <>
struct TestGraphTraits<DummyTestGraphActualTupleOfNodes<void>> {
  static constexpr std::string_view name{
      "DummyTestGraphActualTupleOfNodes<void>"};
};

/**
 * Tuple type containing all the variations of the dummy test graph for use with
 * `TEMPLATE_LIST_TEST_CASE`
 */
using AllTheDummyTestGraphs =
    std::tuple<DummyTestGraph<void>, DummyTestGraphActualTupleOfNodes<void>>;

}  // namespace tiledb::flow_graph::test