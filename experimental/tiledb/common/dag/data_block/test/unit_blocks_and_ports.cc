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

#include "experimental/tiledb/common/dag/data_block/data_block.h"
#include "experimental/tiledb/common/dag/ports/ports.h"
#include "experimental/tiledb/common/dag/state_machine/fsm.h"
#include "experimental/tiledb/common/dag/state_machine/policies.h"
#include "unit_data_block.h"

template <class T>
using AsyncStateMachine2 = AsyncStateMachine<PortState<2>, T>;

template <class T>
using DebugStateMachine2 = DebugStateMachine<PortState<2>, T>;

template <class T>
using NullStateMachine2 = NullStateMachine<PortState<2>, T>;

template <class T>
using ManualStateMachine2 = ManualStateMachine<PortState<2>, T>;

using namespace tiledb::common;

[[maybe_unused]] constexpr const size_t test_chunk_size = 1024 * 1024 * 4;

/**
 * Test creation of port with `DataBlock`
 */
TEST_CASE("DataBlock: Create Source and Sink with DataBlock", "[data_block]") {
  auto pn = Source<DataBlock, NullStateMachine2<DataBlock>>{};
  auto cn = Sink<DataBlock, NullStateMachine2<DataBlock>>{};

  attach(pn, cn);
}

/**
 * Test operation of inject and extract.
 */
TEST_CASE("Ports: Manual set source port values", "[ports]") {
  auto source = Source<DataBlock, NullStateMachine2<DataBlock>>{};
  auto sink = Sink<DataBlock, NullStateMachine2<DataBlock>>{};

  DataBlock x{DataBlock::max_size()};
  DataBlock y{DataBlock::max_size()};

  SECTION("zero out blocks") {
    for (auto& j : source) {
      j = std::byte{0};
    }
    auto e = std::find_if_not(source.begin(), source.end(), [](auto a) {
      return (std::byte{0} == a);
    });
    CHECK(e == source.end());
    for (auto& j : sink) {
      j = std::byte{0};
    }
    auto e = std::find_if_not(
        sink.begin(), sink.end(), [](auto a) { return (std::byte{0} == a); });
    CHECK(e == sink.end());
  }
}

#if 0
  SECTION("set source in bound pair") {
    attach(source, sink);
    CHECK(source.inject(55UL) == true);
    CHECK(source.extract().has_value() == true);
  }
  SECTION("set source in unbound source") {
    CHECK(source.inject(9UL) == false);
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
  auto source = Source<DataBlock, NullStateMachine2<DataBlock>>{};
  auto sink = Sink<DataBlock, NullStateMachine2<DataBlock>>{};

  SECTION("set source in unbound pair") {
    CHECK(sink.extract().has_value() == false);
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
 * Sink with ManualStateMachine2.
 *
 */
TEST_CASE("Ports: Manual transfer from Source to Sink", "[ports]") {
  Source<size_t, ManualStateMachine2<size_t>> source;
  Sink<size_t, ManualStateMachine2<size_t>> sink;

  attach(source, sink);

  auto state_machine = sink.get_state_machine();
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
    state_machine->do_fill();
    state_machine->do_push();
    auto b = sink.extract();
    CHECK(b.has_value() == true);
    CHECK(*b == 123UL);
    CHECK(str(state_machine->state()) == "st_01");
    state_machine->do_drain();
    CHECK(str(state_machine->state()) == "st_00");
  }

  SECTION("test two item transfer") {
    CHECK(source.inject(456UL) == true);
    state_machine->do_fill();
    state_machine->do_push();
    auto b = sink.extract();
    CHECK(b.has_value() == true);
    CHECK(*b == 456UL);
    CHECK(str(state_machine->state()) == "st_01");
    state_machine->do_drain();
    CHECK(str(state_machine->state()) == "st_00");
    CHECK(sink.extract().has_value() == false);

    CHECK(source.inject(789UL) == true);
    state_machine->do_fill();
    state_machine->do_push();

    auto c = sink.extract();
    CHECK(c.has_value() == true);
    CHECK(*c == 789UL);
    CHECK(str(state_machine->state()) == "st_01");
    state_machine->do_drain();
    CHECK(str(state_machine->state()) == "st_00");
    CHECK(sink.extract().has_value() == false);
  }
}
#endif
