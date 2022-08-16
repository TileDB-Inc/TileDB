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

#include "unit_stop_source.h"
#include <atomic>
#include "experimental/tiledb/common/dag/edge/edge.h"
#include "experimental/tiledb/common/dag/execution/stop_token.hpp"
#include "experimental/tiledb/common/dag/nodes/consumer.h"
#include "experimental/tiledb/common/dag/nodes/generator.h"
#include "experimental/tiledb/common/dag/nodes/nodes.h"
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
  ConsumerNode<ManualMover2, size_t> producer_1{stop_source_free_function};
  ProducerNode<ManualMover3, size_t> producer_2{};
  ProducerNode<ManualMover3, size_t> producer_3{stop_source_free_function};
  ProducerNode<AsyncMover3, size_t> producer_4{};
  ProducerNode<AsyncMover3, size_t> producer_5{stop_source_free_function};

  ProducerNode<AsyncMover3, size_t> producer_6{};
  ProducerNode<AsyncMover3, size_t> producer_7{
      std::bind(stop_source_bind_function, std::placeholders::_1, 1.0)};
}
