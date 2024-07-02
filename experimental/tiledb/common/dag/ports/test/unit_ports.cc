/**
 * @file unit_ports.cc
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
 * Tests the ports classes, `Source` and `Sink`.
 */

#include "unit_ports.h"
#include <future>
#include <numeric>
#include "experimental/tiledb/common/dag/ports/ports.h"
#include "experimental/tiledb/common/dag/state_machine/policies.h"
#include "experimental/tiledb/common/dag/state_machine/test/types.h"

using namespace tiledb::common;

/**
 * Verify various API approaches
 */
TEST_CASE("Ports: Verify various API approaches", "[ports]") {
  Source<NullMover2, size_t> left_0;
  Sink<NullMover2, size_t> right_0;
  Source<AsyncMover2, size_t> left_1;
  Sink<AsyncMover2, size_t> right_1;
}

/**
 * Test attaching of Source and Sink ports.
 */
TEST_CASE("Ports: Test attach ports", "[ports]") {
  Source<NullMover2, size_t> left;
  Sink<NullMover2, size_t> right;
  attach(left, right);
}

/**
 * Test various types of attaching and unattaching of Source and Sink ports.
 */
template <class source_type, class sink_type>
void test_connections(source_type& pn, sink_type& cn) {
  attach(pn, cn);

  SECTION("unattach both") {
    unattach(pn, cn);
  }

  SECTION("unattach, reattach both") {
    unattach(pn, cn);
    attach(pn, cn);
  }

  SECTION("attach other way") {
    unattach(cn, pn);
    attach(cn, pn);
  }

  SECTION("unattach other way") {
    unattach(pn, cn);
    attach(cn, pn);
  }
}

/**
 * Test exception when trying to attach already bound ports.
 */
TEST_CASE("Ports: Test exceptions", "[ports]") {
  auto pn = Source<NullMover2, size_t>{};
  auto cn = Sink<NullMover2, size_t>{};

  attach(pn, cn);

  SECTION("Invalid attach") {
    CHECK_THROWS(attach(pn, cn));
  }
}

/**
 * Test operation of inject and extract.
 */
TEST_CASE("Ports: Manual set source port values", "[ports]") {
  Source<NullMover2, size_t> source;
  Sink<NullMover2, size_t> sink;

  SECTION("set source in bound pair") {
    attach(source, sink);
    CHECK(source.inject(55UL) == true);
    CHECK(source.extract().has_value() == true);
  }
  SECTION("set source in unbound source") {
    CHECK_THROWS(source.inject(9UL) == false);
  }
  SECTION("set source that has value") {
    attach(source, sink);
    CHECK(source.inject(11UL) == true);
    CHECK(source.inject(11UL) == false);
  }
}

/**
 * Test operation of inject and extract.
 */
TEST_CASE("Ports: Manual extract sink values", "[ports]") {
  Source<NullMover2, size_t> source;
  Sink<NullMover2, size_t> sink;

  SECTION("set source in unbound pair") {
    CHECK_THROWS(sink.extract().has_value() == false);
  }
  SECTION("set source in bound pair") {
    attach(source, sink);
    CHECK(sink.extract().has_value() == false);
    CHECK(sink.inject(13UL) == true);
    auto v = sink.extract();
    CHECK(v.has_value() == true);
    CHECK(*v == 13UL);
  }
}

/**
 * Test that we can inject, transfer, and extract data items from Source and
 * Sink with ManualMover2.
 *
 */
TEST_CASE("Ports: Manual transfer from Source to Sink", "[ports]") {
  Source<ManualMover2, size_t> source;
  Sink<ManualMover2, size_t> sink;

  attach(source, sink);

  auto state_machine = sink.get_mover();
  // state_machine->enable_debug();

  CHECK(str(state_machine->state()) == "st_00");

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
    state_machine->port_fill();
    state_machine->port_push();
    auto b = sink.extract();
    CHECK(b.has_value() == true);
    CHECK(*b == 123UL);
    CHECK(str(state_machine->state()) == "st_01");
    state_machine->port_drain();
    CHECK(str(state_machine->state()) == "st_00");
  }

  SECTION("test two item transfer") {
    CHECK(source.inject(456UL) == true);
    state_machine->port_fill();
    state_machine->port_push();
    auto b = sink.extract();
    CHECK(b.has_value() == true);
    CHECK(*b == 456UL);
    CHECK(str(state_machine->state()) == "st_01");
    state_machine->port_drain();
    CHECK(str(state_machine->state()) == "st_00");
    CHECK(sink.extract().has_value() == false);

    CHECK(source.inject(789UL) == true);
    state_machine->port_fill();
    state_machine->port_push();

    auto c = sink.extract();
    CHECK(c.has_value() == true);
    CHECK(*c == 789UL);
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
    "Ports: Manual transfer from Source to Sink, async policy", "[ports]") {
  Source<AsyncMover2, size_t> source;
  Sink<AsyncMover2, size_t> sink;

  attach(source, sink);

  auto state_machine = sink.get_mover();
  CHECK(str(state_machine->state()) == "st_00");

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
 * Test that we can asynchronously transfer a value from Source to Sinnk.
 *
 * The test creates an asynchronous task for a source node client and for a sync
 * node client, and launches them separately using `std::async`.  To create
 * different interleavings of the tasks, we use all combinations of ordering for
 * launching the tasks and waiting on their futures.
 */
TEST_CASE("Ports: Async transfer from Source to Sink", "[ports]") {
  Source<AsyncMover2, size_t> source;
  Sink<AsyncMover2, size_t> sink;

  attach(source, sink);

  auto state_machine = sink.get_mover();
  CHECK(str(state_machine->state()) == "st_00");

  std::optional<size_t> b;

  auto source_node = [&]() {
    CHECK(source.inject(8675309UL) == true);
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
TEST_CASE("Ports: Async pass n integers, random delays", "[ports]") {
  [[maybe_unused]] constexpr bool debug = false;

  Source<AsyncMover2, size_t> source;
  Sink<AsyncMover2, size_t> sink;

  attach(source, sink);

  auto state_machine = sink.get_mover();
  CHECK(str(state_machine->state()) == "st_00");

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

      state_machine->port_fill(debug ? "async source node" : "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      state_machine->port_push(debug ? "async source node" : "");

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

      state_machine->port_pull(debug ? "async sink node" : "");

      CHECK(is_sink_full(state_machine->state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      CHECK(is_sink_full(state_machine->state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      *j++ = *(sink.extract());

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
TEST_CASE("Ports: Async pass n integers", "[ports]") {
  [[maybe_unused]] constexpr bool debug = false;

  Source<AsyncMover2, size_t> source;
  Sink<AsyncMover2, size_t> sink;

  attach(source, sink);

  auto state_machine = sink.get_mover();
  CHECK(str(state_machine->state()) == "st_00");

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
      state_machine->port_fill(debug ? "async source node" : "");
      state_machine->port_push(debug ? "async source node" : "");
    }
  };

  auto sink_node = [&]() {
    size_t n = rounds;
    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }
      state_machine->port_pull(debug ? "async sink node" : "");
      CHECK(is_sink_full(state_machine->state()) == "");
      *j++ = *(sink.extract());

      CHECK(is_sink_full(state_machine->state()) == "");
      state_machine->port_drain(debug ? "async sink node" : "");
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
