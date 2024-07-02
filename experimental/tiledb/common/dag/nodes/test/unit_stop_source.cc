/**
 * @file unit_stop_source.cc
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
 * Tests the use of stop tokens with ProducerNode
 */

#include <atomic>
#include <future>
#include <numeric>

#include "unit_stop_source.h"

#include "experimental/tiledb/common/dag/edge/edge.h"
#include "experimental/tiledb/common/dag/nodes/generators.h"
#include "experimental/tiledb/common/dag/nodes/nodes.h"
#include "experimental/tiledb/common/dag/nodes/terminals.h"
#include "experimental/tiledb/common/dag/state_machine/test/types.h"

using namespace tiledb::common;

std::atomic<size_t> atomic_i{0};

size_t stop_source_free_function(std::stop_source& stop_source) {
  if (atomic_i == 15) {
    stop_source.request_stop();
    return atomic_i;
  }
  return ++atomic_i;
}

size_t stop_source_bind_function(std::stop_source& stop_source, double) {
  if (atomic_i == 15) {
    stop_source.request_stop();
    return atomic_i;
  }
  return ++atomic_i;
}

/**
 * Verify free function with stop source
 */
TEST_CASE("StopSource: Verify free function", "[stop_source]") {
  std::stop_source stop_source;

  while (stop_source.stop_requested() == false) {
    stop_source_free_function(stop_source);
    ;
  }

  CHECK(atomic_i == 15);
}

/**
 * Verify construction of producer node
 */
TEST_CASE("StopSource: Verify construction of ProducerNode", "[stop_source]") {
  std::stop_source stop_source;

  atomic_i = 0;

  ProducerNode<ManualMover2, size_t> producer_0{};
  ProducerNode<ManualMover2, size_t> producer_1{stop_source_free_function};
  ProducerNode<ManualMover3, size_t> producer_2{};
  ProducerNode<ManualMover3, size_t> producer_3{stop_source_free_function};
  ProducerNode<AsyncMover3, size_t> producer_4{};
  ProducerNode<AsyncMover3, size_t> producer_5{stop_source_free_function};

  ProducerNode<AsyncMover3, size_t> producer_6{};
  ProducerNode<AsyncMover3, size_t> producer_7{
      std::bind(stop_source_bind_function, std::placeholders::_1, 1.0)};
}

/**
 * Verify one step of producer node
 */
TEST_CASE("StopSource: Verify one step of ProducerNode", "[stop_source]") {
  std::stop_source stop_source;

  atomic_i = 0;

  ProducerNode<ManualMover2, size_t> producer_0{};
  ProducerNode<ManualMover2, size_t> producer_1{stop_source_free_function};
  ProducerNode<ManualMover3, size_t> producer_2{};
  ProducerNode<ManualMover3, size_t> producer_3{stop_source_free_function};
  ProducerNode<AsyncMover3, size_t> producer_4{};
  ProducerNode<AsyncMover3, size_t> producer_5{stop_source_free_function};

  ProducerNode<AsyncMover3, size_t> producer_6{};
  ProducerNode<AsyncMover3, size_t> producer_7{
      std::bind(stop_source_bind_function, std::placeholders::_1, 1.0)};
}

/**
 * Test that we can correctly pass a sequence of integers from producer node to
 * consumer node via function node.
 */
TEST_CASE(
    "StopSource: "
    "Async pass "
    "n integers, "
    "four nodes, "
    "three stage",
    "[stop_source]") {
  size_t rounds = GENERATE(0, 1, 2, 5, 3379);
  size_t offset = GENERATE(0, 1, 2, 5);

  [[maybe_unused]] constexpr bool debug = false;

  if (debug)
    rounds = 3;

  std::vector<size_t> input(rounds + offset);
  std::vector<size_t> output(rounds + offset);

  std::iota(input.begin(), input.end(), 19);
  std::fill(output.begin(), output.end(), 0);
  auto i = input.begin();
  auto j = output.begin();

  generators g{19UL, 19 + rounds};

  if (rounds + offset != 0) {
    CHECK(std::equal(input.begin(), input.end(), output.begin()) == false);
  }

  ProducerNode<AsyncMover3, size_t> source_node(g);
  FunctionNode<AsyncMover3, size_t> mid_node1([](size_t k) { return k + 1; });
  FunctionNode<AsyncMover3, size_t> mid_node2([](size_t k) { return k - 1; });
  ConsumerNode<AsyncMover3, size_t> sink_node(terminal<decltype(j), size_t>{j});

  auto a = Edge(source_node, mid_node1);
  auto b = Edge(mid_node1, mid_node2);
  auto c = Edge(mid_node2, sink_node);

  auto source = [&]() { source_node.run(); };
  auto mid1 = [&]() { mid_node1.run(); };
  auto mid2 = [&]() { mid_node2.run(); };
  auto sink = [&]() { sink_node.run(); };

  SECTION(
      "abcd "
      "abcd " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_a = std::async(std::launch::async, source);
    auto fut_b = std::async(std::launch::async, mid1);
    auto fut_c = std::async(std::launch::async, sink);
    auto fut_d = std::async(std::launch::async, mid2);
    fut_a.get();
    fut_b.get();
    fut_c.get();
    fut_d.get();
  }
  SECTION(
      "dcba "
      "abcd " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_d = std::async(std::launch::async, mid2);
    auto fut_c = std::async(std::launch::async, sink);
    auto fut_b = std::async(std::launch::async, mid1);
    auto fut_a = std::async(std::launch::async, source);
    fut_a.get();
    fut_b.get();
    fut_c.get();
    fut_d.get();
  }
  SECTION(
      "abcd "
      "dcba " +
      std::to_string(rounds) +
      " /"
      " " +
      std::to_string(offset)) {
    auto fut_a = std::async(std::launch::async, source);
    auto fut_b = std::async(std::launch::async, mid1);
    auto fut_c = std::async(std::launch::async, sink);
    auto fut_d = std::async(std::launch::async, mid2);
    fut_d.get();
    fut_c.get();
    fut_b.get();
    fut_a.get();
  }
  SECTION(
      "dcba "
      "dcba " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_d = std::async(std::launch::async, mid2);
    auto fut_c = std::async(std::launch::async, sink);
    auto fut_b = std::async(std::launch::async, mid1);
    auto fut_a = std::async(std::launch::async, source);
    fut_d.get();
    fut_c.get();
    fut_b.get();
    fut_a.get();
  }

  if (!std::equal(input.begin(), i, output.begin())) {
    for (size_t j = 0; j < input.size(); ++j) {
      if (input[j] != output[j]) {
        std::cout << j << " (" << input[j] << ", " << output[j] << ")"
                  << std::endl;
      }
    }
  }

  if (!std::equal(input.begin(), i, output.begin())) {
    auto iter = std::find_first_of(
        input.begin(),
        input.end(),
        output.begin(),
        output.end(),
        std::not_equal_to<size_t>());
    if (iter != input.end()) {
      size_t k = iter - input.begin();
      std::cout << k << " (" << input[k] << ", " << output[k] << ")"
                << std::endl;
    } else {
      std::cout << "this should not happen" << std::endl;
    }
  }
  //  Can't use these with generator and consumer function nodes
  //  CHECK(std::distance(input.begin(), i) == static_cast<long>(rounds));
  //  CHECK(std::distance(output.begin(), j) == static_cast<long>(rounds));
  CHECK(std::equal(input.begin(), i, output.begin()));
}
