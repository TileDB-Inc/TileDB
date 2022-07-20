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

#include "unit_data_block.h"

#include "experimental/tiledb/common/dag/data_block/data_block.h"
#include "experimental/tiledb/common/dag/ports/ports.h"
#include "experimental/tiledb/common/dag/state_machine/fsm.h"
#include "experimental/tiledb/common/dag/state_machine/policies.h"

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

  SECTION("set source in bound pair") {
    attach(source, sink);
    CHECK(source.inject(x) == true);
    CHECK(source.extract().has_value() == true);
  }
  SECTION("set source in unbound source") {
    CHECK(source.inject(x) == false);
  }
  SECTION("set source that has value") {
    attach(source, sink);
    CHECK(source.inject(x) == true);
    CHECK(source.inject(x) == false);
  }
}

bool zeroize(DataBlock& x) {
  for (auto& j : x) {
    j = std::byte{0};
  }
  auto e = std::find_if_not(
      x.begin(), x.end(), [](auto a) { return (std::byte{0} == a); });
  CHECK(e == x.end());
  return e == x.end();
}

void iotize(DataBlock& x) {
  std::iota(
      reinterpret_cast<uint8_t*>(x.begin()),
      reinterpret_cast<uint8_t*>(x.end()),
      uint8_t{0});
}

bool check_iotized(DataBlock& x) {
  bool ret = [&](DataBlock& z) {
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

bool check_zeroized(DataBlock& x) {
  bool ret = [&](DataBlock& z) {
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
TEST_CASE("Ports: Manual extract sink values", "[ports]") {
  auto source = Source<DataBlock, NullStateMachine2<DataBlock>>{};
  auto sink = Sink<DataBlock, NullStateMachine2<DataBlock>>{};

  DataBlock x{DataBlock::max_size()};
  DataBlock y{DataBlock::max_size()};

  SECTION("set source in unbound pair") {
    CHECK(sink.extract().has_value() == false);
  }
  SECTION("set source in bound pair") {
    CHECK(x.size() == DataBlock::max_size());
    CHECK(y.size() == DataBlock::max_size());
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
 * Sink with ManualStateMachine2.
 *
 */
TEST_CASE("Ports: Manual transfer from Source to Sink", "[ports]") {
  Source<DataBlock, ManualStateMachine2<DataBlock>> source;
  Sink<DataBlock, ManualStateMachine2<DataBlock>> sink;

  DataBlock x{DataBlock::max_size()};
  DataBlock y{DataBlock::max_size()};

  [[maybe_unused]] auto dx = x.data();
  [[maybe_unused]] auto dy = y.data();

  CHECK(dx != dy);

  iotize(x);
  zeroize(y);
  check_iotized(x);
  check_zeroized(y);

  attach(source, sink);

  auto state_machine = sink.get_state_machine();
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
    state_machine->do_fill();
    state_machine->do_push();
    auto b = sink.extract();
    CHECK(b.has_value() == true);
    CHECK(b->data() == dx);
    check_iotized(*b);
    CHECK(str(state_machine->state()) == "st_01");
    state_machine->do_drain();
    CHECK(str(state_machine->state()) == "st_00");
  }

  SECTION("test two item transfer") {
    check_iotized(x);
    check_zeroized(y);

    CHECK(source.inject(x) == true);
    state_machine->do_fill();
    state_machine->do_push();
    auto b = sink.extract();
    CHECK(b.has_value() == true);
    CHECK(b->data() == x.data());
    check_iotized(*b);
    CHECK(str(state_machine->state()) == "st_01");
    state_machine->do_drain();
    CHECK(str(state_machine->state()) == "st_00");
    CHECK(sink.extract().has_value() == false);

    CHECK(source.inject(y) == true);
    state_machine->do_fill();
    state_machine->do_push();

    auto c = sink.extract();
    CHECK(c.has_value() == true);
    CHECK(c->data() == dy);
    check_zeroized(*c);
    CHECK(str(state_machine->state()) == "st_01");
    state_machine->do_drain();
    CHECK(str(state_machine->state()) == "st_00");
    CHECK(sink.extract().has_value() == false);
  }
}
