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

#if 0
TEST_CASE("Ports: Manual set source port values", "[ports]") {
  Source<size_t> src;
  Sink<size_t> snk;

  SECTION("set source in bound pair") {
    bind(src, snk);
    CHECK(src.try_set(5) == true);
  }
  SECTION("set source in unbound src") {
    CHECK(src.try_set(5) == false);
  }
  SECTION("set source that has value") {
    bind(src, snk);
    CHECK(src.try_set(5) == true);
    CHECK(src.try_set(5) == false);
  }
}

TEST_CASE("Ports: Manual retrieve sink values", "[ports]") {
  Source<size_t> src;
  Sink<size_t> snk;

  SECTION("set source in bound pair") {
    bind(src, snk);
    CHECK(snk.retrieve().has_value() == false);
  }
}

TEST_CASE("Ports: Manual send and receive", "[ports]") {
  Source<size_t> src;
  Sink<size_t> snk;

  bind(src, snk);
  CHECK(src.try_set(5) == true);
  CHECK(snk.retrieve().has_value() == false);

  SECTION("transfer value to snk") {
    CHECK(src.try_get() == true);
  }
  SECTION("transfer value to snk, check snk has value") {
    CHECK(src.try_get() == true);
    CHECK(snk.retrieve().has_value() == true);
  }
  SECTION("transfer value to snk, check snk value") {
    CHECK(src.try_get() == true);
    CHECK(snk.retrieve() == 5);
  }
  SECTION("transfer value from src") {
    CHECK(snk.try_put() == true);
  }
  SECTION("transfer value to snk, check snk has value") {
    CHECK(snk.try_put() == true);
    CHECK(snk.retrieve().has_value() == true);
  }
  SECTION("transfer value to snk, check snk value") {
    CHECK(snk.try_put() == true);
    CHECK(snk.retrieve() == 5);
  }
}

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
