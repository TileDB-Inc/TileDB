/**
 * @file flow_graph/test/zero_graph.h
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

#include <exception>
#include "../../system/graph_dynamic_specification.h"

namespace tiledb::flow_graph::dynamic_specification {

/**
 * The zero graph has no nodes and no edges. It's a valid graph, and we want to
 * ensure that the flow graph system supports it.
 */
class ZeroGraph {
 public:
  constexpr static struct invariant_type {
    constexpr static bool i_am_graph_dynamic_specification{true};
  } invariants;

  using nodes_size_type = uint8_t;
  using edges_size_type = uint8_t;
  using ports_size_type = uint8_t;

  ZeroGraph() = default;

  [[nodiscard]] constexpr uint8_t nodes_size() const {
    return 0;
  }
  [[nodiscard]] constexpr uint8_t edges_size() const {
    return 0;
  }
  [[nodiscard]] constexpr const void* nodes() const {
    return nullptr;
  }
  [[nodiscard]] constexpr const void* edges() const {
    return nullptr;
  }
  void make_node(uint8_t) const {
    throw std::runtime_error("There is no node");
  }
  void make_edge(uint8_t) const {
    throw std::runtime_error("There is no edge");
  }
};
static_assert(graph_dynamic_specification<ZeroGraph>);

// template <>
// struct TestGraphTraits<DummyTestGraph<void>> {
//   static constexpr std::string_view name{"ZeroGraph"};
// };

}  // namespace tiledb::flow_graph::test