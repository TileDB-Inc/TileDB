/**
 * @file unit_blocks_and_ports.cc
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
 * Unit tests for transferring `DataBlock`s over `Source` and `Sink` ports.
 *
 */

#include <future>
#include <numeric>

#include "unit_data_block.h"

#include "experimental/tiledb/common/dag/data_block/data_block.h"
#include "experimental/tiledb/common/dag/ports/ports.h"
#include "experimental/tiledb/common/dag/state_machine/fsm.h"
#include "experimental/tiledb/common/dag/state_machine/item_mover.h"
#include "experimental/tiledb/common/dag/state_machine/policies.h"

using namespace tiledb::common;

template <class Block>
using AsyncMover2 = ItemMover<AsyncPolicy, two_stage, Block>;
template <class Block>
using ManualMover2 = ItemMover<ManualPolicy, two_stage, Block>;
template <class Block>
using NullMover2 = ItemMover<NullPolicy, two_stage, Block>;

[[maybe_unused]] constexpr const size_t test_chunk_size = 1024 * 1024 * 4;

/**
 * Test creation of port with `DataBlock`
 */
TEST_CASE(
    "BlocksAndPorts: Create Source and Sink with DataBlock",
    "[blocks_and_ports]") {
  auto pn = Source<NullMover2, DataBlock<>>{};
  auto cn = Sink<NullMover2, DataBlock<>>{};

  attach(pn, cn);
}

/**
 * Test operation of inject and extract.
 */
TEST_CASE(
    "BlocksAndPorts: Manual set source port values", "[blocks_and_ports]") {
  auto source = Source<NullMover2, DataBlock<>>{};
  auto sink = Sink<NullMover2, DataBlock<>>{};

  DataBlock<> x{DataBlock<>::max_size()};
  DataBlock<> y{DataBlock<>::max_size()};

  SECTION("set source in bound pair") {
    attach(source, sink);
    CHECK(source.inject(x) == true);
    CHECK(source.extract().has_value() == true);
  }
  SECTION("set source in unbound source") {
    CHECK_THROWS(source.inject(x) == false);
  }
  SECTION("set source that has value") {
    attach(source, sink);
    CHECK(source.inject(x) == true);
    CHECK(source.inject(x) == false);
  }
}

bool zeroize(DataBlock<>& x) {
  for (auto& j : x) {
    j = std::byte{0};
  }
  auto e = std::find_if_not(
      x.begin(), x.end(), [](auto a) { return (std::byte{0} == a); });
  CHECK(e == x.end());
  return e == x.end();
}

void iotize(DataBlock<>& x) {
  std::iota(
      reinterpret_cast<uint8_t*>(x.begin()),
      reinterpret_cast<uint8_t*>(x.end()),
      uint8_t{0});
}

bool check_iotized(DataBlock<>& x) {
  bool ret = [&](DataBlock<>& z) {
    uint8_t b{0};
    for (auto&& j : z) {
      if (static_cast<uint8_t>(j) != b) {
        std::cout << static_cast<uint8_t>(j) << " " << b << std::endl;
        return false;
      }
      ++b;
    }
    return true;
  }(x);

  CHECK(ret == true);
  return ret;
}

bool check_zeroized(DataBlock<>& x) {
  bool ret = [&](DataBlock<>& z) {
    uint8_t b{0};
    for (auto&& j : z) {
      if (static_cast<uint8_t>(j) != b) {
        std::cout << static_cast<uint8_t>(j) << " " << b << std::endl;
        return false;
      }
    }
    return true;
  }(x);

  CHECK(ret == true);
  return ret;
}

/**
 * Test operation of inject and extract.
 */
TEST_CASE("BlocksAndPorts: Manual extract sink values", "[blocks_and_ports]") {
  auto source = Source<NullMover2, DataBlock<>>{};
  auto sink = Sink<NullMover2, DataBlock<>>{};

  DataBlock<> x{DataBlock<>::max_size()};
  DataBlock<> y{DataBlock<>::max_size()};

  SECTION("set source in unbound pair") {
    CHECK_THROWS(sink.extract().has_value() == false);
  }
  SECTION("set source in bound pair") {
    CHECK(x.size() == DataBlock<>::max_size());
    CHECK(y.size() == DataBlock<>::max_size());
    iotize(x);
    zeroize(y);
    check_iotized(x);
    check_zeroized(y);
    auto dx = x.data();

    attach(source, sink);
    CHECK(sink.extract().has_value() == false);
    CHECK(sink.inject(x) == true);
    auto v = sink.extract();
    CHECK(v.has_value() == true);
    check_iotized(*v);
    CHECK(dx == v->data());
  }
}

/**
 * Test that we can inject, transfer, and extract data items from Source and
 * Sink with ManualMover2.
 *
 */
TEST_CASE(
    "BlocksAndPorts: Manual transfer from Source to Sink",
    "[blocks_and_ports]") {
  Source<ManualMover2, DataBlock<>> source;
  Sink<ManualMover2, DataBlock<>> sink;

  DataBlock<> x{DataBlock<>::max_size()};
  DataBlock<> y{DataBlock<>::max_size()};

  [[maybe_unused]] auto dx = x.data();
  [[maybe_unused]] auto dy = y.data();

  CHECK(dx != dy);

  iotize(x);
  zeroize(y);
  check_iotized(x);
  check_zeroized(y);

  attach(source, sink);

  auto state_machine = sink.get_mover();
  // state_machine->enable_debug();

  CHECK(str(state_machine->state()) == "st_00");

  SECTION("test injection") {
    CHECK(source.inject(x) == true);
    CHECK(source.inject(y) == false);
    CHECK(sink.extract().has_value() == false);
  }
  SECTION("test extraction") {
    CHECK(sink.inject(x) == true);
    CHECK(sink.extract().has_value() == true);
    CHECK(sink.extract().has_value() == false);
  }

  SECTION("test one item transfer") {
    check_iotized(x);

    CHECK(source.inject(x) == true);
    state_machine->port_fill();
    state_machine->port_push();
    auto b = sink.extract();
    CHECK(b.has_value() == true);
    CHECK(b->data() == dx);
    check_iotized(*b);
    CHECK(str(state_machine->state()) == "st_01");
    state_machine->port_drain();
    CHECK(str(state_machine->state()) == "st_00");
  }

  SECTION("test two item transfer") {
    check_iotized(x);
    check_zeroized(y);

    CHECK(source.inject(x) == true);
    state_machine->port_fill();
    state_machine->port_push();
    auto b = sink.extract();
    CHECK(b.has_value() == true);
    CHECK(b->data() == x.data());
    check_iotized(*b);
    CHECK(str(state_machine->state()) == "st_01");
    state_machine->port_drain();
    CHECK(str(state_machine->state()) == "st_00");
    CHECK(sink.extract().has_value() == false);

    CHECK(source.inject(y) == true);
    state_machine->port_fill();
    state_machine->port_push();

    auto c = sink.extract();
    CHECK(c.has_value() == true);
    CHECK(c->data() == dy);
    check_zeroized(*c);
    CHECK(str(state_machine->state()) == "st_01");
    state_machine->port_drain();
    CHECK(str(state_machine->state()) == "st_00");
    CHECK(sink.extract().has_value() == false);
  }
}

/**
 * Test that we can inject and extract data items from Source and Sink with
 * AsyncMover.
 *
 */
TEST_CASE(
    "BlocksAndPorts: Manual transfer from Source to Sink, async policy",
    "[blocks_and_ports]") {
  Source<AsyncMover2, DataBlock<>> source;
  Sink<AsyncMover2, DataBlock<>> sink;

  DataBlock<> x{DataBlock<>::max_size()};
  DataBlock<> y{DataBlock<>::max_size()};

  [[maybe_unused]] auto dx = x.data();
  [[maybe_unused]] auto dy = y.data();

  CHECK(dx != dy);

  iotize(x);
  zeroize(y);
  check_iotized(x);
  check_zeroized(y);

  attach(source, sink);

  auto state_machine = sink.get_mover();
  CHECK(str(state_machine->state()) == "st_00");

  SECTION("test injection") {
    CHECK(source.inject(x) == true);
    CHECK(source.inject(y) == false);
    CHECK(sink.extract().has_value() == false);
  }
  SECTION("test extraction") {
    CHECK(sink.inject(x) == true);
    CHECK(sink.extract().has_value() == true);
    CHECK(sink.extract().has_value() == false);
  }
}

/**
 * Test that we can asynchronously transfer a value from Source to Sik.
 *
 * The test creates an asynchronous task for a source node client and for a sync
 * node client, and launches them separately using `std::async`.  To create
 * different interleavings of the tasks, we use all combinations of ordering for
 * launching the tasks and waiting on their futures.
 */
TEST_CASE(
    "BlocksAndPorts: Async transfer from Source to Sink",
    "[blocks_and_ports]") {
  Source<AsyncMover2, DataBlock<>> source;
  Sink<AsyncMover2, DataBlock<>> sink;

  DataBlock<> x{DataBlock<>::max_size()};
  DataBlock<> y{DataBlock<>::max_size()};

  [[maybe_unused]] auto dx = x.data();
  [[maybe_unused]] auto dy = y.data();

  CHECK(dx != dy);

  iotize(x);
  zeroize(y);
  check_iotized(x);
  check_zeroized(y);

  attach(source, sink);

  auto state_machine = sink.get_mover();
  CHECK(str(state_machine->state()) == "st_00");

  std::optional<DataBlock<>> b;

  auto source_node = [&]() {
    CHECK(source.inject(x) == true);
    state_machine->port_fill();
    state_machine->port_push();
  };
  auto sink_node = [&]() {
    state_machine->port_pull();
    b = sink.extract();
    state_machine->port_drain();
  };

  SECTION("test source launch, sink launch, source get, sink get") {
    auto fut_a = std::async(std::launch::async, source_node);
    auto fut_b = std::async(std::launch::async, sink_node);
    fut_a.get();
    fut_b.get();
  }
  SECTION("test source launch, sink launch, sink get, source get") {
    auto fut_a = std::async(std::launch::async, source_node);
    auto fut_b = std::async(std::launch::async, sink_node);
    fut_b.get();
    fut_a.get();
  }
  SECTION("test sink launch, source launch, source get, sink get") {
    auto fut_b = std::async(std::launch::async, sink_node);
    auto fut_a = std::async(std::launch::async, source_node);
    fut_a.get();
    fut_b.get();
  }
  SECTION("test sink launch, source launch, sink get, source get") {
    auto fut_b = std::async(std::launch::async, sink_node);
    auto fut_a = std::async(std::launch::async, source_node);
    fut_b.get();
    fut_a.get();
  }

  CHECK(b.has_value() == true);
  CHECK(b->data() == dx);
  check_iotized(*b);
}

/**
 * Test that we can correctly pass a sequence of blocks from source to sink.
 * Random delays are inserted between each step of each function in order to
 * increase the likelihood of exposing race conditions / deadlocks.
 *
 * The test creates an asynchronous task for a source node client and for a sync
 * node client, and launches them separately using `std::async`.  To create
 * different interleavings of the tasks, we use all combinations of ordering for
 * launching the tasks and waiting on their futures.
 */
TEST_CASE("BlocksAndPorts: Async pass n blocks", "[blocks_and_ports]") {
  [[maybe_unused]] constexpr bool debug = false;

  Source<AsyncMover2, DataBlock<>> source;
  Sink<AsyncMover2, DataBlock<>> sink;

  attach(source, sink);

  auto state_machine = sink.get_mover();
  CHECK(str(state_machine->state()) == "st_00");

  size_t rounds = 337;
  if (debug)
    rounds = 3;

  std::vector<size_t> input(rounds);
  std::vector<size_t> output(rounds);

  std::iota(input.begin(), input.end(), 19);
  std::fill(output.begin(), output.end(), 0);
  auto i = input.begin();
  auto j = output.begin();

  CHECK(std::equal(input.begin(), input.end(), output.begin()) == false);

  auto p = PoolAllocator<DataBlock<>::max_size()>{};
  size_t init_allocations = p.num_allocations();

  size_t max_allocated = 0;

  auto source_node = [&]() {
    size_t n = rounds;

    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }

      CHECK(is_source_empty(state_machine->state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      CHECK(is_source_empty(state_machine->state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      source.inject(DataBlock<>{*i++});
      max_allocated = std::max(max_allocated, p.num_allocated());

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      CHECK(is_source_empty(state_machine->state()) == "");

      state_machine->port_fill(debug ? "async source node" : "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      state_machine->port_push(debug ? "async source node" : "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));
    }
  };

  auto sink_node = [&]() {
    size_t n = rounds;
    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      state_machine->port_pull(debug ? "async sink node" : "");

      CHECK(is_sink_full(state_machine->state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      CHECK(is_sink_full(state_machine->state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      auto k = *(sink.extract());

      *j++ = k.size();

      CHECK(is_sink_full(state_machine->state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      state_machine->port_drain(debug ? "async sink node" : "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));
    }
  };

  SECTION("test source launch, sink launch, source get, sink get") {
    auto fut_a = std::async(std::launch::async, source_node);
    auto fut_b = std::async(std::launch::async, sink_node);
    fut_a.get();
    fut_b.get();
  }

  SECTION("test source launch, sink launch, sink get, source get") {
    auto fut_a = std::async(std::launch::async, source_node);
    auto fut_b = std::async(std::launch::async, sink_node);
    fut_b.get();
    fut_a.get();
  }

  SECTION("test sink launch, source launch, source get, sink get") {
    auto fut_b = std::async(std::launch::async, sink_node);
    auto fut_a = std::async(std::launch::async, source_node);
    fut_a.get();
    fut_b.get();
  }

  SECTION("test sink launch, source launch, sink get, source get") {
    auto fut_b = std::async(std::launch::async, sink_node);
    auto fut_a = std::async(std::launch::async, source_node);
    fut_b.get();
    fut_a.get();
  }

  if (!std::equal(input.begin(), input.end(), output.begin())) {
    for (size_t j = 0; j < input.size(); ++j) {
      if (input[j] != output[j]) {
        std::cout << j << " (" << input[j] << ", " << output[j] << ")"
                  << std::endl;
      }
    }
  }

  if (!std::equal(input.begin(), input.end(), output.begin())) {
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

  // Check that block sizes sent equal block sizes received
  CHECK(std::equal(input.begin(), input.end(), output.begin()));

  // We should have used one block per round
  CHECK(p.num_allocations() == init_allocations + rounds);

  // There should not have been any more than 3 blocks in flight at any one time
  CHECK(max_allocated <= 3);

  // All blocks should have been freed
  CHECK(p.num_allocated() == 0);
}
