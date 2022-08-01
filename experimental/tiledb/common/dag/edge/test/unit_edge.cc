/**
 * @file unit_edge.cc
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
 * Tests the operation of edges, including testing of data transfer with the
 * finite-state machine.
 *
 * The different tests currently include an extensive amount of debugging code.
 * There is also a significant amount of cut-and-paste repeated code.
 *
 * @todo Remove the debugging code.
 * @todo Refactor tests to remove duplicate code.
 *
 */

#include "unit_edge.h"

#include <future>

#include "experimental/tiledb/common/dag/edge/edge.h"
#include "experimental/tiledb/common/dag/ports/ports.h"
#include "experimental/tiledb/common/dag/state_machine/policies.h"
#include "experimental/tiledb/common/dag/state_machine/test/types.h"

using namespace tiledb::common;

/**
 * Attach an `Edge` to a `Source` and a `Sink`
 */
TEST_CASE("Edge: Attach a Source and Sink with an Edge", "[edge") {
  Source<NullMover3, size_t> left;
  Sink<NullMover3, size_t> right;
  Edge<NullMover3, size_t> mid(left, right);
}

/**
 * Test that we can inject, transfer, and extract data items from Source and
 * Sink with ManualMover2.
 *
 */
TEST_CASE("Ports: Manual transfer from Source to Sink", "[ports]") {
  Source<ManualMover3, size_t> source;
  Sink<ManualMover3, size_t> sink;
  Edge<ManualMover3, size_t> edge(source, sink);

  auto state_machine = sink.get_mover();
  // state_machine->enable_debug();

  CHECK(str(state_machine->state()) == "st_000");

  SECTION("test injection") {
    CHECK(source.inject(123UL) == true);
    CHECK(source.inject(321UL) == false);
    CHECK(sink.extract().has_value() == false);
  }
  SECTION("test extraction") {
    CHECK(sink.inject(123UL) == true);
    CHECK(sink.extract().has_value() == true);
    CHECK(sink.extract().has_value() == false);
  }

  SECTION("test one item transfer") {
    CHECK(source.inject(123UL) == true);
    state_machine->do_fill();
    state_machine->do_push();
    auto b = sink.extract();
    CHECK(b.has_value() == true);
    CHECK(*b == 123UL);
    CHECK(str(state_machine->state()) == "st_001");
    state_machine->do_drain();
    CHECK(str(state_machine->state()) == "st_000");
  }

  SECTION("test two item transfer") {
    CHECK(source.inject(456UL) == true);
    state_machine->do_fill();
    state_machine->do_push();
    auto b = sink.extract();
    CHECK(b.has_value() == true);
    CHECK(*b == 456UL);
    CHECK(str(state_machine->state()) == "st_001");
    state_machine->do_drain();
    CHECK(str(state_machine->state()) == "st_000");
    CHECK(sink.extract().has_value() == false);

    CHECK(source.inject(789UL) == true);
    state_machine->do_fill();
    state_machine->do_push();

    auto c = sink.extract();
    CHECK(c.has_value() == true);
    CHECK(*c == 789UL);
    CHECK(str(state_machine->state()) == "st_001");
    state_machine->do_drain();
    CHECK(str(state_machine->state()) == "st_000");
    CHECK(sink.extract().has_value() == false);
  }

  SECTION("test buffered two item transfer") {
    CHECK(source.inject(456UL) == true);
    state_machine->do_fill();
    state_machine->do_push();

    CHECK(str(state_machine->state()) == "st_001");
    CHECK(source.inject(789UL) == true);
    state_machine->do_fill();
    state_machine->do_push();

    CHECK(str(state_machine->state()) == "st_011");

    auto b = sink.extract();
    CHECK(b.has_value() == true);
    CHECK(*b == 456UL);
    state_machine->do_drain();

    state_machine->do_pull();

    CHECK(str(state_machine->state()) == "st_001");

    auto c = sink.extract();
    CHECK(c.has_value() == true);
    CHECK(*c == 789UL);
    CHECK(str(state_machine->state()) == "st_001");
    state_machine->do_drain();
    CHECK(str(state_machine->state()) == "st_000");
    CHECK(sink.extract().has_value() == false);
  }

  SECTION("test buffered three item transfer") {
    CHECK(source.inject(456UL) == true);
    state_machine->do_fill();
    state_machine->do_push();
    CHECK(str(state_machine->state()) == "st_001");

    CHECK(source.inject(789UL) == true);
    state_machine->do_fill();
    state_machine->do_push();
    CHECK(str(state_machine->state()) == "st_011");

    CHECK(source.inject(123UL) == true);
    state_machine->do_fill();
    state_machine->do_push();
    CHECK(str(state_machine->state()) == "st_111");

    state_machine->do_drain();
    CHECK(str(state_machine->state()) == "st_110");

    auto b = sink.extract();
    CHECK(b.has_value() == true);
    CHECK(*b == 456UL);
    state_machine->do_pull();
    CHECK(str(state_machine->state()) == "st_011");

    state_machine->do_drain();
    CHECK(str(state_machine->state()) == "st_010");
    auto c = sink.extract();
    CHECK(c.has_value() == true);
    CHECK(*c == 789UL);
    state_machine->do_pull();
    CHECK(str(state_machine->state()) == "st_001");

    state_machine->do_drain();
    CHECK(str(state_machine->state()) == "st_000");
    auto d = sink.extract();
    CHECK(c.has_value() == true);
    CHECK(*d == 123UL);

    CHECK(str(state_machine->state()) == "st_000");
    CHECK(sink.extract().has_value() == false);
  }
}

/**
 * Test that we can inject and extract data items from Source and Sink with
 * AsyncMover.
 *
 */
TEST_CASE("Edge: inject and extract", "[edge]") {
  Source<AsyncMover3, size_t> source;
  Sink<AsyncMover3, size_t> sink;
  Edge<AsyncMover3, size_t> edge(source, sink);

  auto state_machine = sink.get_mover();
  CHECK(str(state_machine->state()) == "st_000");

  SECTION("test injection") {
    CHECK(source.inject(123UL) == true);
    CHECK(source.inject(321UL) == false);
    CHECK(sink.extract().has_value() == false);
  }
  SECTION("test extraction") {
    CHECK(sink.inject(123UL) == true);
    CHECK(sink.extract().has_value() == true);
    CHECK(sink.extract().has_value() == false);
  }
}

/**
 * Test that we can asynchronously transfer a value from Source to Sink.
 *
 * The test creates an asynchronous task for a source node client and for a sync
 * node client, and launches them separately using `std::async`.  To create
 * different interleavings of the tasks, we use all combinations of ordering for
 * launching the tasks and waiting on their futures.
 */
TEST_CASE("Ports: Async transfer from Source to Sink", "[ports]") {
  Source<AsyncMover3, size_t> source;
  Sink<AsyncMover3, size_t> sink;
  Edge<AsyncMover3, size_t> edge(source, sink);

  auto state_machine = sink.get_mover();
  CHECK(str(state_machine->state()) == "st_000");

  std::optional<size_t> b;

  auto source_node = [&]() {
    CHECK(source.inject(8675309UL) == true);
    state_machine->do_fill();
    state_machine->do_push();
  };
  auto sink_node = [&]() {
    state_machine->do_pull();
    b = sink.extract();
    state_machine->do_drain();
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
  CHECK(*b == 8675309);
}

/**
 * Test that we can correctly pass a sequence of integers from source to sink.
 * Random delays are inserted between each step of each function in order to
 * increase the likelihood of exposing race conditions / deadlocks.
 *
 * The test creates an asynchronous task for a source node client and for a sync
 * node client, and launches them separately using `std::async`.  To create
 * different interleavings of the tasks, we use all combinations of ordering for
 * launching the tasks and waiting on their futures.
 */
TEST_CASE("Edge: Async pass n integers, random delays", "[edge]") {
  [[maybe_unused]] constexpr bool debug = false;

  Source<AsyncMover3, size_t> source;
  Sink<AsyncMover3, size_t> sink;
  Edge<AsyncMover3, size_t> edge(source, sink);

  auto state_machine = sink.get_mover();
  CHECK(str(state_machine->state()) == "st_000");

  size_t rounds = 3379;
  if (debug)
    rounds = 3;

  std::vector<size_t> input(rounds);
  std::vector<size_t> output(rounds);

  std::iota(input.begin(), input.end(), 19);
  std::fill(output.begin(), output.end(), 0);
  auto i = input.begin();
  auto j = output.begin();

  CHECK(std::equal(input.begin(), input.end(), output.begin()) == false);

  [[maybe_unused]] size_t b;

  auto source_node = [&]() {
    size_t n = rounds;

    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }

      // It doesn't seem we actually need these guards here?
      // while (state_machine->state() == PortState::st_10 ||
      //        state_machine->state() == PortState::st_11)// ;

      CHECK(is_source_empty(state_machine->state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      CHECK(is_source_empty(state_machine->state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      source.inject(*i++);

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      CHECK(is_source_empty(state_machine->state()) == "");

      state_machine->do_fill(debug ? "async source node" : "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      state_machine->do_push(debug ? "async source node" : "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      //      CHECK(source.extract().has_value() == false);

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));
    }
  };

  auto sink_node = [&]() {
    size_t n = rounds;
    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }

      // It doesn't seem we actually need these guards here?
      // while (state_machine->state() == PortState::st_11 ||
      //             state_machine->state() == PortState::st_01)
      //        ;

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      state_machine->do_pull(debug ? "async sink node" : "");

      CHECK(is_sink_full(state_machine->state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      CHECK(is_sink_full(state_machine->state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      *j++ = *(sink.extract());

      CHECK(is_sink_full(state_machine->state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      state_machine->do_drain(debug ? "async sink node" : "");

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

  CHECK(std::equal(input.begin(), input.end(), output.begin()));
}

/**
 * Repeat above test but without delays.
 *
 * The test creates an asynchronous task for a source node client and for a sync
 * node client, and launches them separately using `std::async`.  To create
 * different interleavings of the tasks, we use all combinations of ordering for
 * launching the tasks and waiting on their futures.
 */
TEST_CASE("Edge: Async pass n integers", "[edge]") {
  [[maybe_unused]] constexpr bool debug = false;
  Source<AsyncMover3, size_t> source;
  Sink<AsyncMover3, size_t> sink;
  Edge<AsyncMover3, size_t> edge(source, sink);

  auto state_machine = sink.get_mover();
  CHECK(str(state_machine->state()) == "st_000");

  size_t rounds = 3379;
  if (debug)
    rounds = 3;

  std::vector<size_t> input(rounds);
  std::vector<size_t> output(rounds);

  std::iota(input.begin(), input.end(), 19);
  std::fill(output.begin(), output.end(), 0);
  auto i = input.begin();
  auto j = output.begin();

  CHECK(std::equal(input.begin(), input.end(), output.begin()) == false);

  [[maybe_unused]] size_t b;

  auto source_node = [&]() {
    size_t n = rounds;

    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }

      CHECK(is_source_empty(state_machine->state()) == "");
      source.inject(*i++);
      CHECK(is_source_empty(state_machine->state()) == "");
      state_machine->do_fill(debug ? "async source node" : "");
      state_machine->do_push(debug ? "async source node" : "");
    }
  };

  auto sink_node = [&]() {
    size_t n = rounds;
    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }
      state_machine->do_pull(debug ? "async sink node" : "");
      CHECK(is_sink_full(state_machine->state()) == "");
      *j++ = *(sink.extract());

      CHECK(is_sink_full(state_machine->state()) == "");
      state_machine->do_drain(debug ? "async sink node" : "");
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

  CHECK(std::equal(input.begin(), input.end(), output.begin()));
}

#if 0

/**
 * Attach a Source and Sink with an Edge.
 */
TEST_CASE("Attach a Source and Sink with an Edge", "[edge") {

}


/**
 * Test that we can correctly pass a sequence of integers from source to sink.
 * Random delays are inserted between each step of each function in order to
 * increase the likelihood of exposing race conditions / deadlocks.
 *
 * The test creates an asynchronous task for a source node client and for a sync
 * node client, and launches them separately using `std::async`.  To create
 * different interleavings of the tasks, we use all combinations of ordering for
 * launching the tasks and waiting on their futures.
 */
TEST_CASE("Pass a sequence of n integers, async", "[edge]") {
  [[maybe_unused]] constexpr bool debug = false;

  size_t source_item{0};
  size_t sink_item{0};
  [[maybe_unused]] auto a = AsyncStateMachine{source_item, sink_item, debug};

  a.set_state(PortState::empty_empty);

  size_t rounds = 3379;
  if (debug)
    rounds = 3;

  std::vector<size_t> input(rounds);
  std::vector<size_t> output(rounds);

  std::iota(input.begin(), input.end(), 19);
  std::fill(output.begin(), output.end(), 0);
  auto i = input.begin();
  auto j = output.begin();

  CHECK(std::equal(input.begin(), input.end(), output.begin()) == false);

  auto source_node = [&]() {
    size_t n = rounds;

    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }

      // It doesn't seem we actually need these guards here?
      // while (a.state() == PortState::full_empty ||
      //        a.state() == PortState::full_full)// ;

      CHECK(is_source_empty(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      CHECK(is_source_empty(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      *(a.source_item_) = *i++;

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      CHECK(is_source_empty(a.state()) == "");

      a.do_fill(debug ? "async source node" : "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      a.do_push(debug ? "async source node" : "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      *(a.source_item_) = EMPTY_SOURCE;

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));
    }
  };

  auto sink_node = [&]() {
    size_t n = rounds;
    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }

      // It doesn't seem we actually need these guards here?
      // while (a.state() == PortState::full_full ||
      //             a.state() == PortState::empty_full)
      //        ;

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      a.do_pull(debug ? "async sink node" : "");

      CHECK(is_sink_full(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      CHECK(is_sink_full(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      *j++ = *(a.sink_item_);

      CHECK(is_sink_full(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      *(a.sink_item_) = EMPTY_SINK;

      a.do_drain(debug ? "async sink node" : "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));
    }
  };

  SECTION("launch source before sink, get source before sink") {
    auto fut_a = std::async(std::launch::async, source_node);
    auto fut_b = std::async(std::launch::async, sink_node);

    fut_a.get();
    fut_b.get();
  }

  SECTION("launch sink before source, get source before sink") {
    auto fut_b = std::async(std::launch::async, sink_node);
    auto fut_a = std::async(std::launch::async, source_node);

    fut_a.get();
    fut_b.get();
  }

  SECTION("launch source before sink, get sink before source") {
    auto fut_a = std::async(std::launch::async, source_node);
    auto fut_b = std::async(std::launch::async, sink_node);

    fut_b.get();
    fut_a.get();
  }

  SECTION("launch sink before source, get sink before source") {
    auto fut_b = std::async(std::launch::async, sink_node);
    auto fut_a = std::async(std::launch::async, source_node);

    fut_b.get();
    fut_a.get();
  }

  if (debug)
    for (size_t i = 0; i < rounds; ++i) {
      std::cout << i << " (" << input[i] << ", " << output[i] << ")"
                << std::endl;
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

  CHECK(std::equal(input.begin(), input.end(), output.begin()));
  CHECK(str(a.state()) == "empty_empty");
  CHECK((a.source_swaps + a.sink_swaps) == rounds);
}

/**
 * Repeat the previous test, but with the unified async state machine.  To test
 * rapid execution and interleaving of events, we do not include the delays
 * between steps.
 */
TEST_CASE("Pass a sequence of n integers, unified", "[edge]") {
  [[maybe_unused]] constexpr bool debug = false;

  size_t source_item{0};
  size_t sink_item{0};
  [[maybe_unused]] auto a =
      UnifiedAsyncStateMachine{source_item, sink_item, debug};

  a.set_state(PortState::empty_empty);

  size_t rounds = 3379;
  if (debug)
    rounds = 3;

  std::vector<size_t> input(rounds);
  std::vector<size_t> output(rounds);

  std::iota(input.begin(), input.end(), 19);
  std::fill(output.begin(), output.end(), 0);
  auto i = input.begin();
  auto j = output.begin();

  CHECK(std::equal(input.begin(), input.end(), output.begin()) == false);

  auto source_node = [&]() {
    size_t n = rounds;

    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }
      // while (a.state() == PortState::full_empty ||
      // a.state() == PortState::full_full)
      // ;

      CHECK(is_source_empty(a.state()) == "");

      *(a.source_item_) = *i++;
      a.do_fill(debug ? "async source node" : "");
      a.do_push(debug ? "async source node" : "");

      CHECK(is_source_empty(a.state()) == "");

      *(a.source_item_) = EMPTY_SOURCE;
    }
  };

  auto sink_node = [&]() {
    size_t n = rounds;
    while (n--) {
      if (debug) {
        std::cout << "sink node iteration " << n << std::endl;
      }

      // while (a.state() == PortState::full_full ||
      //             a.state() == PortState::empty_full)
      //        ;

      a.do_pull(debug ? "async sink node" : "");

      CHECK(is_sink_full(a.state()) == "");

      *j++ = *(a.sink_item_);
      *(a.sink_item_) = EMPTY_SINK;

      a.do_drain(debug ? "async sink node" : "");
    }
  };

  SECTION("launch source before sink, get source before sink") {
    auto fut_a = std::async(std::launch::async, source_node);
    auto fut_b = std::async(std::launch::async, sink_node);

    fut_a.get();
    fut_b.get();
  }

  SECTION("launch sink before source, get source before sink") {
    auto fut_b = std::async(std::launch::async, sink_node);
    auto fut_a = std::async(std::launch::async, source_node);

    fut_a.get();
    fut_b.get();
  }

  SECTION("launch source before sink, get sink before source") {
    auto fut_a = std::async(std::launch::async, source_node);
    auto fut_b = std::async(std::launch::async, sink_node);

    fut_b.get();
    fut_a.get();
  }

  SECTION("launch sink before source, get sink before source") {
    auto fut_b = std::async(std::launch::async, sink_node);
    auto fut_a = std::async(std::launch::async, source_node);

    fut_b.get();
    fut_a.get();
  }

  if (debug)
    for (size_t i = 0; i < rounds; ++i) {
      std::cout << i << " (" << input[i] << ", " << output[i] << ")"
                << std::endl;
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
  CHECK(std::equal(input.begin(), input.end(), output.begin()));
  CHECK(str(a.state()) == "empty_empty");
}
#endif
