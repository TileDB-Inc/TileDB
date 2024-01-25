/**
 * @file unit_proof.cc
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
 * Tests the ports finite state machine.
 *
 * The different tests currently include an extensive amount of debugging code.
 * There is also a significant amount of cut-and-paste repeated code.
 *
 * @todo Remove the debugging code.
 * @todo Refactor tests to remove duplicate code.
 *
 */

#include "unit_proof.h"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <iostream>
#include <mutex>
#include <numeric>
#include <thread>
#include <tuple>
#include <vector>
#include "experimental/tiledb/common/dag/state_machine/fsm.h"
#include "experimental/tiledb/common/dag/state_machine/item_mover.h"
#include "experimental/tiledb/common/dag/state_machine/policies.h"
#include "experimental/tiledb/common/dag/state_machine/test/helpers.h"
#include "experimental/tiledb/common/dag/state_machine/test/types.h"

using namespace tiledb::common;

/**
 * Verify states as specified by proof outlines for two-stage state machine.
 * Launch an emulated source client and an emulated sink client as asynchronous
 * tasks.  The test runs n iterations of each emulated client.  The test also
 * invokes the tasks in all combinations of orderings of task launch and waiting
 * on futures. We also test with and without inserted delays.
 */
TEST_CASE(
    "AsynchronousPolicy: Asynchronous source and asynchronous sink, n "
    "iterations with and without delay, checking items",
    "[fsm]") {
  [[maybe_unused]] constexpr bool debug = false;

  std::optional<size_t> source_item{};
  std::optional<size_t> sink_item{};
  [[maybe_unused]] auto a = AsyncMover2<size_t>{source_item, sink_item};

  if (debug)
    a.enable_debug();

  bool delay = GENERATE(true, false);

  a.set_state(two_stage::st_00);

  size_t rounds = 1337;
  if (debug)
    rounds = 3;

  auto source_node = [&]() {
    size_t n = rounds;

    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }

      /* { state = 00 } ∧ { items = 00 } */
      /* { state = 01 } ∧ { items = 00 ∨ items = 01 } */
      for (size_t i = 0; i < 3; ++i) {
        auto a0 = a.state();
        CHECK(((a0 == two_stage::st_00) || (a0 == two_stage::st_01)));

        // Check items.  Note that there is a race as states and items
        // may be asynchronously updated by the source.  We only check
        // the items that can't change.
        if (a0 == two_stage::st_00) {
          CHECK(source_item.has_value() == false);
          CHECK(sink_item.has_value() == false);
        }
        if (a0 == two_stage::st_01) {
          CHECK(source_item.has_value() == false);
        }
        if (delay)
          std::this_thread::sleep_for(
              std::chrono::microseconds(random_us(500)));
      }

      /*
       * Inject
       */
      auto val = std::optional<size_t>{9999};
      std::swap(source_item, val);

      /* { state = 00 } ∧ { items = 00 } */
      /* { state = 01 } ∧ { items = 00 ∨ items = 01 } */
      for (size_t i = 0; i < 3; ++i) {
        auto a1 = a.state();
        CHECK(((a1 == two_stage::st_00) || (a1 == two_stage::st_01)));

        // Check items.  Note that there is a race as states and items
        // may be asynchronously updated by the source.  We only check
        // the items that can't change.
        if (a1 == two_stage::st_00) {
          CHECK(source_item.has_value() == true);
          CHECK(sink_item.has_value() == false);
        }
        if (a1 == two_stage::st_01) {
          CHECK(source_item.has_value() == true);
        }
        if (delay)
          std::this_thread::sleep_for(
              std::chrono::microseconds(random_us(500)));
      }

      /*
       * Fill
       */
      a.port_fill(debug ? "async source node" : "");
      if (delay)
        std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      /*
       * Once the source state is filled, the sink may pull before the source
       * can push.  Nothing reasonable to check for here.
       */

      /*
       * Push
       */
      a.port_push(debug ? "async source node" : "");
      if (delay)
        std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));
    }
  };

  auto sink_node = [&]() {
    size_t n = rounds;
    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }

      a.port_pull(debug ? "async sink node" : "");

      /* { state = 11 } ∧ { items = 11 } */
      /* { state = 01 } ∧ { items = 01 ∨ items = 11 } */
      for (size_t i = 0; i < 3; ++i) {
        auto a3 = a.state();
        CHECK(((a3 == two_stage::st_01) || (a3 == two_stage::st_11)));

        // Check items.  Note that there is a race as states and items
        // may be asynchronously updated by the source.  We only check
        // the items that can't change.
        if (a3 == two_stage::st_11) {
          CHECK(source_item.has_value() == true);
          CHECK(sink_item.has_value() == true);
        }
        if (a3 == two_stage::st_01) {
          CHECK(sink_item.has_value() == true);
        }
        if (delay)
          std::this_thread::sleep_for(
              std::chrono::microseconds(random_us(500)));
      }

      /*
       * Extract
       */
      CHECK(sink_item.has_value() == true);
      auto val = std::optional<size_t>{};
      std::swap(sink_item, val);

      /* { state = 11 } ∧ { items = 10 } */
      /* { state = 01 } ∧ { items = 00 ∨ items = 10 } */
      for (size_t i = 0; i < 3; ++i) {
        auto a4 = a.state();
        CHECK(((a4 == two_stage::st_01) || (a4 == two_stage::st_11)));

        // Check items.  Note that there is a race as states and items
        // may be asynchronously updated by the source.  We only check
        // the items that can't change.
        if (a4 == two_stage::st_11) {
          CHECK(source_item.has_value() == true);
          CHECK(sink_item.has_value() == false);
        }
        if (a4 == two_stage::st_01) {
          CHECK(sink_item.has_value() == false);
        }
        if (delay)
          std::this_thread::sleep_for(
              std::chrono::microseconds(random_us(500)));
      }

      /*
       * Drain
       */
      a.port_drain(debug ? "async sink node" : "");
      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      /*
       * Once the sink state is drained, the source may push before the sink
       * can pull.  Nothing reasonable to check for here.
       */
    }
  };

  SECTION(
      "launch source before sink, get source before sink, " +
      std::to_string(delay)) {
    CHECK(str(a.state()) == "st_00");
    auto fut_a = std::async(std::launch::async, source_node);
    if (delay)
      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));
    auto fut_b = std::async(std::launch::async, sink_node);

    fut_a.get();
    fut_b.get();
  }

  SECTION(
      "launch sink before source, get source before sink, " +
      std::to_string(delay)) {
    CHECK(str(a.state()) == "st_00");
    auto fut_b = std::async(std::launch::async, sink_node);
    if (delay)
      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));
    auto fut_a = std::async(std::launch::async, source_node);

    fut_a.get();
    fut_b.get();
  }

  SECTION(
      "launch source before sink, get sink before source, " +
      std::to_string(delay)) {
    CHECK(str(a.state()) == "st_00");
    auto fut_a = std::async(std::launch::async, source_node);
    if (delay)
      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));
    auto fut_b = std::async(std::launch::async, sink_node);

    fut_b.get();
    fut_a.get();
  }

  SECTION(
      "launch sink before source, get sink before source, " +
      std::to_string(delay)) {
    CHECK(str(a.state()) == "st_00");
    auto fut_b = std::async(std::launch::async, sink_node);
    if (delay)
      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));
    auto fut_a = std::async(std::launch::async, source_node);

    fut_b.get();
    fut_a.get();
  }

  CHECK(str(a.state()) == "st_00");
  CHECK((a.source_swaps() + a.sink_swaps()) == rounds);
};

/**
 * Verify states as specified by proof outlines for three-stage state machine.
 * Launch an emulated source client and an emulated sink client as asynchronous
 * tasks.  The test runs n iterations of each emulated client.  The test also
 * invokes the tasks in all combinations of orderings of task launch and waiting
 * on futures. We also test with and without inserted delays.
 */
TEST_CASE(
    "AsynchronousPolicy: Two-stage, synchronous source, asynchronous sink, n "
    "iterations, checking items, with and without delay",
    "[fsm]") {
  [[maybe_unused]] constexpr bool debug = false;

  std::optional<size_t> source_item{};
  std::optional<size_t> mid_item{};
  std::optional<size_t> sink_item{};
  [[maybe_unused]] auto a =
      AsyncMover3<size_t>{source_item, mid_item, sink_item};

  bool delay = GENERATE(true, false);

  if (debug)
    a.enable_debug();

  a.set_state(three_stage::st_000);

  size_t rounds = 1337;
  if (debug)
    rounds = 3;

  auto source_node = [&]() {
    size_t n = rounds;

    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }

      /* { state = 000 } ∧ { items = 000 } */
      /* { state = 0x1 } ∧ { items = 0x0 ∨ items = 0x1 } */
      /* { state = 0x0 } ∧ { items = 0x0 ∨ items = 0x1 } */
      for (size_t i = 0; i < 3; ++i) {
        auto a0 = a.state();
        CHECK(
            ((a0 == three_stage::st_000) || (a0 == three_stage::st_001) ||
             (a0 == three_stage::st_011) || (a0 == three_stage::st_010)));

        // Check items.  Note that there is a race as states and items
        // may be asynchronously updated by the sink.  We only check
        // the items that can't change.
        if (a0 == three_stage::st_000) {
          CHECK(source_item.has_value() == false);
          CHECK(mid_item.has_value() == false);
          CHECK(sink_item.has_value() == false);
        }
        if (a0 == three_stage::st_001) {
          CHECK(source_item.has_value() == false);
          CHECK(mid_item.has_value() == false);
        }
        if (a0 == three_stage::st_011) {
          CHECK(source_item.has_value() == false);
        }
        if (a0 == three_stage::st_010) {
          CHECK(source_item.has_value() == false);
        }
        if (delay)
          std::this_thread::sleep_for(
              std::chrono::microseconds(random_us(500)));
      }

      /*
       * Inject
       */
      auto val = std::optional<size_t>{9999};
      std::swap(source_item, val);

      /* { state = 000 } ∧ { items = 100 } */
      /* { state = 0x1 } ∧ { items = 1x0 ∨ items = 1x1 } */
      /* { state = 0x0 } ∧ { items = 1x0 ∨ items = 1x1 } */
      for (size_t i = 0; i < 3; ++i) {
        auto a2 = a.state();
        CHECK(
            ((a2 == three_stage::st_000) || (a2 == three_stage::st_001) ||
             (a2 == three_stage::st_011) || (a2 == three_stage::st_010)));

        if (a2 == three_stage::st_000) {
          CHECK(source_item.has_value() == true);
          CHECK(mid_item.has_value() == false);
          CHECK(sink_item.has_value() == false);
        }
        if (a2 == three_stage::st_001) {
          CHECK(source_item.has_value() == true);
          CHECK(mid_item.has_value() == false);
        }
        if (a2 == three_stage::st_011) {
          CHECK(source_item.has_value() == true);
        }
        if (a2 == three_stage::st_010) {
          CHECK(source_item.has_value() == true);
        }
        if (delay)
          std::this_thread::sleep_for(
              std::chrono::microseconds(random_us(500)));
      }

      /*
       * Fill
       */
      a.port_fill(debug ? "async source node" : "");
      if (delay)
        std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      /*
       * Once the source state is filled, the sink may pull before the source
       * can push.  Nothing reasonable to check for here.
       */

      /*
       * Push
       */
      a.port_push(debug ? "async source node" : "");
      if (delay)
        std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));
    }
  };

  auto sink_node = [&]() {
    size_t n = rounds;
    while (n--) {
      if (debug) {
        std::cout << "sink node iteration " << n << std::endl;
      }

      /*
       * Pull
       */
      a.port_pull(debug ? "async sink node pull" : "");
      if (delay)
        std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      /* { state = 111 } ∧ { items = 111 } */
      /* { state = 0x1 } ∧ { items = 0x1 ∨ items = 1x1 } */
      /* { state = 1x1 } ∧ { items = 0x1 ∨ items = 1x1 } */
      for (size_t i = 0; i < 3; ++i) {
        auto a4 = a.state();
        CHECK(
            ((a4 == three_stage::st_111) || (a4 == three_stage::st_001) ||
             (a4 == three_stage::st_011) || (a4 == three_stage::st_101)));

        // Check items.  Note that there is a race as states and items
        // may be asynchronously updated by the source.  We only check
        // the items that can't change.
        if (a4 == three_stage::st_001) {
          CHECK(sink_item.has_value() == true);
        }
        if (a4 == three_stage::st_011) {
          CHECK(mid_item.has_value() == true);
          CHECK(sink_item.has_value() == true);
        }
        if (a4 == three_stage::st_111) {
          CHECK(source_item.has_value() == true);
          CHECK(mid_item.has_value() == true);
          CHECK(sink_item.has_value() == true);
        }
        if (a4 == three_stage::st_101) {
          CHECK(sink_item.has_value() == true);
        }
        if (delay)
          std::this_thread::sleep_for(
              std::chrono::microseconds(random_us(500)));
      }

      /*
       * Extract
       */
      CHECK(sink_item.has_value() == true);
      auto val = std::optional<size_t>{};
      std::swap(sink_item, val);

      /* { state = 111 } ∧ { items = 110 } */
      /* { state = 0x1 } ∧ { items = 0x0 ∨ items = 1x0 } */
      /* { state = 1x1 } ∧ { items = 0x0 ∨ items = 1x0 } */

      for (size_t i = 0; i < 3; ++i) {
        auto a4 = a.state();
        CHECK(
            ((a4 == three_stage::st_111) || (a4 == three_stage::st_001) ||
             (a4 == three_stage::st_011) || (a4 == three_stage::st_101)));

        // Check items.  Note that there is a race as states and items
        // may be asynchronously updated by the source.  We only check
        // the items that can't change.
        if (a4 == three_stage::st_001) {
          CHECK(sink_item.has_value() == false);
        }
        if (a4 == three_stage::st_011) {
          CHECK(mid_item.has_value() == true);
          CHECK(sink_item.has_value() == false);
        }
        if (a4 == three_stage::st_111) {
          CHECK(source_item.has_value() == true);
          CHECK(mid_item.has_value() == true);
          CHECK(sink_item.has_value() == false);
        }
        if (a4 == three_stage::st_101) {
          CHECK(sink_item.has_value() == false);
        }
        if (delay)
          std::this_thread::sleep_for(
              std::chrono::microseconds(random_us(500)));
      }

      /*
       * Drain
       */
      a.port_drain(debug ? "async sink node" : "");
      if (delay)
        std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      /*
       * Once the sink state is drained, the source may push before the sink
       * can pull.  Nothing reasonable to check for here.
       */
    }
  };

  SECTION(
      "launch source before sink, get source before sink, " +
      std::to_string(delay)) {
    CHECK(str(a.state()) == "st_000");
    auto fut_a = std::async(std::launch::async, source_node);
    if (delay)
      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));
    auto fut_b = std::async(std::launch::async, sink_node);

    fut_a.get();
    fut_b.get();
  }

  SECTION(
      "launch sink before source, get source before sink, " +
      std::to_string(delay)) {
    CHECK(str(a.state()) == "st_000");
    auto fut_b = std::async(std::launch::async, sink_node);
    if (delay)
      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));
    auto fut_a = std::async(std::launch::async, source_node);

    fut_a.get();
    fut_b.get();
  }

  SECTION(
      "launch source before sink, get sink before source, " +
      std::to_string(delay)) {
    CHECK(str(a.state()) == "st_000");
    auto fut_a = std::async(std::launch::async, source_node);
    if (delay)
      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));
    auto fut_b = std::async(std::launch::async, sink_node);

    fut_b.get();
    fut_a.get();
  }

  SECTION(
      "launch sink before source, get sink before source " +
      std::to_string(delay)) {
    CHECK(str(a.state()) == "st_000");
    auto fut_b = std::async(std::launch::async, sink_node);
    if (delay)
      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));
    auto fut_a = std::async(std::launch::async, source_node);

    fut_b.get();
    fut_a.get();
  }

  CHECK(str(a.state()) == "st_000");
  //  CHECK((a.source_swaps() + a.sink_swaps()) == rounds);
};
