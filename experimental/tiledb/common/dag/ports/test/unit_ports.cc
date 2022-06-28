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
 * Tests the ports classes, `Source` and `Sink`.  We use some pseudo-nodes
 * for the testing.
 */

#include "unit_ports.h"
#include <future>
#include "experimental/tiledb/common/dag/ports/policies.h"
#include "experimental/tiledb/common/dag/ports/ports.h"
#include "pseudo_nodes.h"

using namespace tiledb::common;

TEST_CASE("Ports: Test bind ports", "[ports]") {
  Source<int, NullStateMachine<int>> left;
  Sink<int, NullStateMachine<int>> right;
  bind(left, right);
}

TEST_CASE("Ports: Test bind pseudonodes", "[ports]") {
  ProducerNode<int, NullStateMachine<int>> left([]() { return 0; });
  ConsumerNode<int, NullStateMachine<int>> right([](int) { return 0; });
  bind(left, right);
}

template <class source_type, class sink_type>
void test_connections(source_type& pn, sink_type& cn) {
  bind(pn, cn);

  SECTION("unbind both") {
    unbind(pn, cn);
  }

  SECTION("unbind, rebind both") {
    unbind(pn, cn);
    bind(pn, cn);
  }

  SECTION("bind other way") {
    unbind(cn, pn);
    bind(cn, pn);
  }

  SECTION("unbind other way") {
    unbind(pn, cn);
    bind(cn, pn);
  }
}

TEST_CASE(
    "Ports: Test connect Sink and Source ports"
    "[ports]") {
  SECTION("Ports") {
    auto pn = Source<int, NullStateMachine<int>>{};
    auto cn = Sink<int, NullStateMachine<int>>{};
    test_connections(pn, cn);
  }
  SECTION("Pseudo Nodes") {
    auto pn = ProducerNode<int, NullStateMachine<int>>{[]() { return 0; }};
    auto cn = ConsumerNode<int, NullStateMachine<int>>{[](int) {}};
    test_connections(pn, cn);
  }
}

TEST_CASE("Ports: Test exceptions", "[ports]") {
  auto pn = Source<size_t, NullStateMachine<size_t>>{};
  auto cn = Sink<size_t, NullStateMachine<size_t>>{};

  bind(pn, cn);

  SECTION("Invalid bind") {
    CHECK_THROWS(bind(pn, cn));
  }
}

TEST_CASE("Ports: Manual set source port values", "[ports]") {
  Source<size_t, NullStateMachine<size_t>> src;
  Sink<size_t, NullStateMachine<size_t>> snk;

  SECTION("set source in bound pair") {
    bind(src, snk);
    CHECK(src.inject(55UL) == true);
    CHECK(src.extract().has_value() == true);
  }
  SECTION("set source in unbound src") {
    CHECK(src.inject(9UL) == false);
  }
  SECTION("set source that has value") {
    bind(src, snk);
    CHECK(src.inject(11UL) == true);
    CHECK(src.inject(11UL) == false);
  }
}

TEST_CASE("Ports: Manual extract sink values", "[ports]") {
  Source<size_t, NullStateMachine<size_t>> src;
  Sink<size_t, NullStateMachine<size_t>> snk;

  SECTION("set source in unbound pair") {
    CHECK(snk.extract().has_value() == false);
  }
  SECTION("set source in bound pair") {
    bind(src, snk);
    CHECK(snk.extract().has_value() == false);
    CHECK(snk.inject(13UL) == true);
    auto v = snk.extract();
    CHECK(v.has_value() == true);
    CHECK(*v == 13UL);
  }
}

TEST_CASE("Ports: Manual transfer from Source to Sink", "[ports]") {
  Source<size_t, ManualStateMachine<size_t>> src;
  Sink<size_t, ManualStateMachine<size_t>> snk;

  bind(src, snk);

  auto state_machine = snk.get_state_machine();
  // state_machine->enable_debug();

  CHECK(str(state_machine->state()) == "empty_empty");

  SECTION("test injection") {
    CHECK(src.inject(123UL) == true);
    CHECK(src.inject(321UL) == false);
    CHECK(snk.extract().has_value() == false);
  }
  SECTION("test extraction") {
    CHECK(snk.inject(123UL) == true);
    CHECK(snk.extract().has_value() == true);
    CHECK(snk.extract().has_value() == false);
  }

  SECTION("test one item transfer") {
    CHECK(src.inject(123UL) == true);
    state_machine->do_fill();
    state_machine->do_push();
    auto b = snk.extract();
    CHECK(b.has_value() == true);
    CHECK(*b == 123UL);
    CHECK(str(state_machine->state()) == "empty_full");
    state_machine->do_drain();
    CHECK(str(state_machine->state()) == "empty_empty");
  }

  SECTION("test two item transfer") {
    CHECK(src.inject(456UL) == true);
    state_machine->do_fill();
    state_machine->do_push();
    auto b = snk.extract();
    CHECK(b.has_value() == true);
    CHECK(*b == 456UL);
    CHECK(str(state_machine->state()) == "empty_full");
    state_machine->do_drain();
    CHECK(str(state_machine->state()) == "empty_empty");
    CHECK(snk.extract().has_value() == false);

    CHECK(src.inject(789UL) == true);
    state_machine->do_fill();
    state_machine->do_push();

    auto c = snk.extract();
    CHECK(c.has_value() == true);
    CHECK(*c == 789UL);
    CHECK(str(state_machine->state()) == "empty_full");
    state_machine->do_drain();
    CHECK(str(state_machine->state()) == "empty_empty");
    CHECK(snk.extract().has_value() == false);
  }
}

TEST_CASE(
    "Ports: Manual transfer from Source to Sink, async policy", "[ports]") {
  Source<size_t, AsyncStateMachine<size_t>> src;
  Sink<size_t, AsyncStateMachine<size_t>> snk;

  bind(src, snk);

  auto state_machine = snk.get_state_machine();
  CHECK(str(state_machine->state()) == "empty_empty");

  SECTION("test injection") {
    CHECK(src.inject(123UL) == true);
    CHECK(src.inject(321UL) == false);
    CHECK(snk.extract().has_value() == false);
  }
  SECTION("test extraction") {
    CHECK(snk.inject(123UL) == true);
    CHECK(snk.extract().has_value() == true);
    CHECK(snk.extract().has_value() == false);
  }
}

TEST_CASE("Ports: Async transfer from Source to Sink", "[ports]") {
  Source<size_t, AsyncStateMachine<size_t>> src;
  Sink<size_t, AsyncStateMachine<size_t>> snk;

  bind(src, snk);

  auto state_machine = snk.get_state_machine();
  CHECK(str(state_machine->state()) == "empty_empty");

  std::optional<size_t> b;

  auto source_node = [&]() {
    CHECK(src.inject(8675309UL) == true);
    state_machine->do_fill();
    state_machine->do_push();
  };
  auto sink_node = [&]() {
    state_machine->do_pull();
    b = snk.extract();
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

TEST_CASE("Ports: Async pass n integers", "[ports]") {
  [[maybe_unused]] constexpr bool debug = false;

  Source<size_t, AsyncStateMachine<size_t>> src;
  Sink<size_t, AsyncStateMachine<size_t>> snk;

  bind(src, snk);

  auto state_machine = snk.get_state_machine();
  CHECK(str(state_machine->state()) == "empty_empty");

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

  std::optional<size_t> b;

  auto source_node = [&]() {
    size_t n = rounds;

    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }

      // It doesn't seem we actually need these guards here?
      // while (state_machine->state() == PortState::full_empty ||
      //        state_machine->state() == PortState::full_full)// ;

      CHECK(is_src_empty(state_machine->state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      CHECK(is_src_empty(state_machine->state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      src.inject(*i++);

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      CHECK(is_src_empty(state_machine->state()) == "");

      state_machine->do_fill(debug ? "async source node" : "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      state_machine->do_push(debug ? "async source node" : "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      //      CHECK(src.extract().has_value() == false);

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
      // while (state_machine->state() == PortState::full_full ||
      //             state_machine->state() == PortState::empty_full)
      //        ;

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      state_machine->do_pull(debug ? "async sink node" : "");

      CHECK(is_snk_full(state_machine->state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      CHECK(is_snk_full(state_machine->state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      *j++ = *(snk.extract());

      CHECK(is_snk_full(state_machine->state()) == "");

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

#if 0

TEST_CASE("Ports: Test construct proto producer_node", "[ports]") {
  auto gen = generator<size_t>(10UL);
  auto pn = producer_node<size_t>(std::move(gen));
}

TEST_CASE("Ports: Test construct proto consumer_node", "[ports]") {
  std::vector<size_t> v;
  auto con = consumer<std::back_insert_iterator<std::vector<size_t>>>(
      std::back_insert_iterator<std::vector<size_t>>(v));
  auto cn = consumer_node<size_t>(std::move(con));
}
#endif
