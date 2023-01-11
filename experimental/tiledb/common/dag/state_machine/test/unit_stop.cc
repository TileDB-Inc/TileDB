/**
 * @file unit_stop.cc
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

#include "unit_stop.h"

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
#include "experimental/tiledb/common/dag/state_machine/policies.h"
#include "experimental/tiledb/common/dag/state_machine/test/helpers.h"
#include "experimental/tiledb/common/dag/state_machine/test/types.h"

using namespace tiledb::common;

TEST_CASE("Port FSM: Just stop, two stage", "[stop]") {
  [[maybe_unused]] constexpr bool debug = false;

  [[maybe_unused]] auto a = DebugStateMachine2<size_t>{};

  /*
   * Source will only be in this state before it will invoke stop
   * (It is the state it will be in following push.)
   *
   * { state = 00 ∨ state = 01 } ∧ { stop = 0 }
   */
  a.set_state(two_stage::st_00);
  CHECK(str(a.state()) == "st_00");
  a.port_exhausted();
  CHECK(str(a.state()) == "xt_00");

  a.set_state(two_stage::st_01);
  CHECK(str(a.state()) == "st_01");
  a.port_exhausted();
  CHECK(str(a.state()) == "xt_01");

  /*
   * After the stop is signalled, the source will be descheduled and not run any
   * more.  Any further actions should result in error.
   *
   *  We can't directly CHECK error state because the state machine will throw.
   */
  a.set_state(two_stage::st_00);
  CHECK(str(a.state()) == "st_00");
  a.port_exhausted();
  CHECK(str(a.state()) == "xt_00");

  CHECK_THROWS(a.port_fill());
  // CHECK(str(a.state()) == "error");

  a.set_state(two_stage::st_00);
  CHECK(str(a.state()) == "st_00");
  a.port_exhausted();
  CHECK(str(a.state()) == "xt_00");
  CHECK_THROWS(a.port_push());
  // CHECK(str(a.state()) == "error");

  a.set_state(two_stage::st_01);
  CHECK(str(a.state()) == "st_01");
  a.port_exhausted();
  CHECK(str(a.state()) == "xt_01");
  CHECK_THROWS(a.port_fill());
  // CHECK(str(a.state()) == "error");

  a.set_state(two_stage::st_01);
  CHECK(str(a.state()) == "st_01");
  a.port_exhausted();
  CHECK(str(a.state()) == "xt_01");
  CHECK_THROWS(a.port_push());
  // CHECK(str(a.state()) == "error");

  /*
   * The stop can be signalled at any time with respect to the sink.
   * Test pull and drain.
   *
   * Prior to sink_drain, state will be
   *
   * { state = 01 ∨ state = 11 } ∧ { stop = 0 ∨ stop = 1}
   */
  a.set_state(two_stage::st_00);
  CHECK(str(a.state()) == "st_00");
  a.port_exhausted();
  CHECK(str(a.state()) == "xt_00");
  a.port_pull();
  CHECK(str(a.state()) == "done");

  a.set_state(two_stage::st_01);
  CHECK(str(a.state()) == "st_01");
  a.port_exhausted();
  CHECK(str(a.state()) == "xt_01");
  a.port_pull();
  CHECK(str(a.state()) == "xt_01");
  a.port_drain();
  CHECK(str(a.state()) == "xt_00");
  a.port_pull();
  CHECK(str(a.state()) == "done");

  a.set_state(two_stage::st_01);
  CHECK(str(a.state()) == "st_01");
  a.port_exhausted();
  CHECK(str(a.state()) == "xt_01");
  a.port_drain();
  CHECK(str(a.state()) == "xt_00");
  a.port_pull();
  CHECK(str(a.state()) == "done");
}

TEST_CASE("Port FSM: Just stop, three stage", "[stop]") {
  [[maybe_unused]] constexpr bool debug = false;

  [[maybe_unused]] auto a = DebugStateMachine3<size_t>{};

  /*
   * Source will only be in this state before it will invoke stop
   * (It is the state it will be in following push.)
   *
   * { state = 000 ∨ state = 001 ∨ state = 010 ∨ state = 011 } ∧ { stop = 0 }
   */
  a.set_state(three_stage::st_000);
  CHECK(str(a.state()) == "st_000");
  a.port_exhausted();
  CHECK(str(a.state()) == "xt_000");

  a.set_state(three_stage::st_001);
  CHECK(str(a.state()) == "st_001");
  a.port_exhausted();
  CHECK(str(a.state()) == "xt_001");

  a.set_state(three_stage::st_010);
  CHECK(str(a.state()) == "st_010");
  a.port_exhausted();
  CHECK(str(a.state()) == "xt_010");

  a.set_state(three_stage::st_011);
  CHECK(str(a.state()) == "st_011");
  a.port_exhausted();
  CHECK(str(a.state()) == "xt_011");

  /*
   * After the stop is signalled, the source will be descheduled and not run any
   * more.  Any further actions should result in error.
   */
  a.set_state(three_stage::st_000);
  CHECK(str(a.state()) == "st_000");
  a.port_exhausted();
  CHECK(str(a.state()) == "xt_000");
  CHECK_THROWS(a.port_fill());
  // CHECK(str(a.state()) == "error");

  a.set_state(three_stage::st_001);
  CHECK(str(a.state()) == "st_001");
  a.port_exhausted();
  CHECK(str(a.state()) == "xt_001");
  CHECK_THROWS(a.port_fill());
  // CHECK(str(a.state()) == "error");

  a.set_state(three_stage::st_010);
  CHECK(str(a.state()) == "st_010");
  a.port_exhausted();
  CHECK(str(a.state()) == "xt_010");
  CHECK_THROWS(a.port_fill());
  // CHECK(str(a.state()) == "error");

  a.set_state(three_stage::st_011);
  CHECK(str(a.state()) == "st_011");
  a.port_exhausted();
  CHECK(str(a.state()) == "xt_011");
  CHECK_THROWS(a.port_fill());
  // CHECK(str(a.state()) == "error");

  a.set_state(three_stage::st_000);
  CHECK(str(a.state()) == "st_000");
  a.port_exhausted();
  CHECK(str(a.state()) == "xt_000");
  CHECK_THROWS(a.port_push());
  // CHECK(str(a.state()) == "error");

  a.set_state(three_stage::st_001);
  CHECK(str(a.state()) == "st_001");
  a.port_exhausted();
  CHECK(str(a.state()) == "xt_001");
  CHECK_THROWS(a.port_push());
  // CHECK(str(a.state()) == "error");

  a.set_state(three_stage::st_010);
  CHECK(str(a.state()) == "st_010");
  a.port_exhausted();
  CHECK(str(a.state()) == "xt_010");
  CHECK_THROWS(a.port_push());
  // CHECK(str(a.state()) == "error");

  a.set_state(three_stage::st_011);
  CHECK(str(a.state()) == "st_011");
  a.port_exhausted();
  CHECK(str(a.state()) == "xt_011");
  CHECK_THROWS(a.port_push());
  // CHECK(str(a.state()) == "error");

  /*
   * The stop can be signalled at any time with respect to the sink.
   * Test pull and drain.
   *
   * Prior to sink_drain, state will be
   *
   * { state = 01 ∨ state = 11 } ∧ { stop = 0 ∨ stop = 1}
   */
  a.set_state(three_stage::st_000);
  CHECK(str(a.state()) == "st_000");
  a.port_exhausted();
  CHECK(str(a.state()) == "xt_000");
  a.port_pull();
  CHECK(str(a.state()) == "done");

  a.set_state(three_stage::st_001);
  CHECK(str(a.state()) == "st_001");
  a.port_exhausted();
  CHECK(str(a.state()) == "xt_001");
  a.port_pull();
  CHECK(str(a.state()) == "xt_001");
  a.port_drain();
  CHECK(str(a.state()) == "xt_000");
  a.port_pull();
  CHECK(str(a.state()) == "done");

  a.set_state(three_stage::st_010);
  CHECK(str(a.state()) == "st_010");
  a.port_exhausted();
  CHECK(str(a.state()) == "xt_010");
  a.port_pull();
  CHECK(str(a.state()) == "xt_001");
  a.port_drain();
  CHECK(str(a.state()) == "xt_000");
  a.port_pull();
  CHECK(str(a.state()) == "done");

  a.set_state(three_stage::st_011);
  CHECK(str(a.state()) == "st_011");
  a.port_exhausted();
  CHECK(str(a.state()) == "xt_011");
  a.port_pull();
  CHECK(str(a.state()) == "xt_011");
  a.port_drain();
  CHECK(str(a.state()) == "xt_010");
  a.port_pull();
  CHECK(str(a.state()) == "xt_001");
  a.port_drain();
  CHECK(str(a.state()) == "xt_000");
  a.port_pull();
  CHECK(str(a.state()) == "done");

  a.set_state(three_stage::st_000);
  CHECK(str(a.state()) == "st_000");
  a.port_exhausted();
  CHECK(str(a.state()) == "xt_000");
  CHECK_THROWS(a.port_drain());
  // CHECK(str(a.state()) == "error");

  a.set_state(three_stage::st_001);
  CHECK(str(a.state()) == "st_001");
  a.port_exhausted();
  CHECK(str(a.state()) == "xt_001");
  a.port_drain();
  CHECK(str(a.state()) == "xt_000");
  a.port_pull();
  CHECK(str(a.state()) == "done");

  a.set_state(three_stage::st_010);
  CHECK(str(a.state()) == "st_010");
  a.port_exhausted();
  CHECK(str(a.state()) == "xt_010");
  CHECK_THROWS(a.port_drain());
  // CHECK(str(a.state()) == "error");

  a.set_state(three_stage::st_011);
  CHECK(str(a.state()) == "st_011");
  a.port_exhausted();
  CHECK(str(a.state()) == "xt_011");
  a.port_drain();
  CHECK(str(a.state()) == "xt_010");
  a.port_pull();
  CHECK(str(a.state()) == "xt_001");
  a.port_drain();
  CHECK(str(a.state()) == "xt_000");
  a.port_pull();
  CHECK(str(a.state()) == "done");
}

/**
 * Simple test of asynchrous state machine policy, launching an emulated source
 * client as an asynchronous task and running an emulated sink client in the
 * main thread. The test just runs one pass of each emulated client.
 */
TEST_CASE("Port FSM: Asynchronous source and manual sink", "[stop]") {
  [[maybe_unused]] constexpr bool debug = false;

  std::optional<size_t> source_item{0};
  std::optional<size_t> sink_item{0};
  [[maybe_unused]] auto a = AsyncMover2<size_t>{source_item, sink_item};

  a.set_state(two_stage::st_00);

  /*
   * Source
   */
  auto fut_a = std::async(std::launch::async, [&]() {
    CHECK(is_source_empty(a.state()) == "");

    a.port_fill(debug ? "async source (fill)" : "");
    // Cannot check for full here because sink may pull before push

    a.port_push(debug ? "async source (push)" : "");
    CHECK(is_source_empty(a.state()) == "");

    a.port_exhausted(debug ? "async source (stop)" : "");
    CHECK(is_stopping(a.state()) == "");
  });

  SECTION("Sink pulls first") {
    /*
     * Sink
     */
    a.port_pull(debug ? "manual sink (pull)" : "");
    CHECK(is_sink_full(a.state()) == "");

    a.port_drain(debug ? "manual sink (drain)" : "");
    if (!(is_done(a.state()) == "")) {
      a.port_pull(debug ? "async sink (pull)" : "");
      CHECK(is_done(a.state()) == "");
    } else {
      CHECK(is_done(a.state()) == "");
    }
  }

  fut_a.get();

  CHECK(str(a.state()) == "done");
};

/**
 * Simple test of asynchrous state machine policy, launching an emulated sink
 * client as an asynchronous task and running an emulated source client in the
 * main thread. The test just runs one pass of each emulated client.
 */
TEST_CASE("Port FSM: Asynchronous sink and manual source", "[stop]") {
  [[maybe_unused]] constexpr bool debug = false;

  std::optional<size_t> source_item{0};
  std::optional<size_t> sink_item{0};
  [[maybe_unused]] auto a = AsyncMover2<size_t>{source_item, sink_item};

  a.set_state(two_stage::st_00);

  /*
   * Sink
   */
  auto fut_b = std::async(std::launch::async, [&]() {
    a.port_pull(debug ? "async sink (pull)" : "");
    CHECK(is_sink_full(a.state()) == "");

    a.port_drain(debug ? "async sink (drain)" : "");
    if (!(is_done(a.state()) == "")) {
      a.port_pull(debug ? "async sink (pull)" : "");
      CHECK(is_done(a.state()) == "");
    } else {
      CHECK(is_done(a.state()) == "");
    }
  });

  SECTION("Sink pulls first") {
    /*
     * Source
     */
    CHECK(is_source_empty(a.state()) == "");

    a.port_fill(debug ? "manual source (fill)" : "");
    // Cannot check for full here because sink may pull before push

    a.port_push(debug ? "manual source (push)" : "");
    CHECK(is_source_empty(a.state()) == "");

    a.port_exhausted(debug ? "manual source (stop)" : "");
    CHECK(is_stopping(a.state()) == "");
  }

  fut_b.get();

  CHECK(str(a.state()) == "done");
};

/**
 * Simple test of unified asynchrous state machine policy, launching an emulated
 * source client as an asynchronous task and running an emulated sink client in
 * the main thread. The test just runs one pass of each emulated client.
 */
TEST_CASE("Port FSM: Unified Asynchronous source and manual sink", "[stop]") {
  [[maybe_unused]] constexpr bool debug = false;

  std::optional<size_t> source_item{0};
  std::optional<size_t> sink_item{0};
  [[maybe_unused]] auto a = UnifiedAsyncMover2<size_t>{source_item, sink_item};

  a.set_state(two_stage::st_00);

  /*
   * Source
   */
  auto fut_a = std::async(std::launch::async, [&]() {
    CHECK(is_source_empty(a.state()) == "");

    a.port_fill(debug ? "async source (fill)" : "");
    // Cannot check for full here because sink may pull before push

    a.port_push(debug ? "async source (push)" : "");
    CHECK(is_source_empty(a.state()) == "");

    a.port_exhausted(debug ? "async source (stop)" : "");
    CHECK(is_stopping(a.state()) == "");
  });

  SECTION("Sink pulls first") {
    /*
     * Sink
     */
    a.port_pull(debug ? "manual sink (pull)" : "");
    CHECK(is_sink_full(a.state()) == "");

    a.port_drain(debug ? "manual sink (drain)" : "");
    if (!(is_done(a.state()) == "")) {
      a.port_pull(debug ? "async sink (pull)" : "");
      CHECK(is_done(a.state()) == "");
    } else {
      CHECK(is_done(a.state()) == "");
    }
  }

  fut_a.get();

  CHECK(str(a.state()) == "done");
};

/**
 * Simple test of unified asynchrous state machine policy, launching an emulated
 * sink client as an asynchronous task and running an emulated source client in
 * the main thread. The test just runs one pass of each emulated client.
 */
TEST_CASE("Port FSM: Unified Asynchronous sink and manual source", "[stop]") {
  [[maybe_unused]] constexpr bool debug = false;

  std::optional<size_t> source_item{0};
  std::optional<size_t> sink_item{0};
  [[maybe_unused]] auto a = UnifiedAsyncMover2<size_t>{source_item, sink_item};

  a.set_state(two_stage::st_00);

  /*
   * Sink
   */
  auto fut_b = std::async(std::launch::async, [&]() {
    a.port_pull(debug ? "async sink (pull)" : "");
    CHECK(is_sink_full(a.state()) == "");

    a.port_drain(debug ? "async sink (drain)" : "");
    if (!(is_done(a.state()) == "")) {
      a.port_pull(debug ? "async sink (pull)" : "");
      CHECK(is_done(a.state()) == "");
    } else {
      CHECK(is_done(a.state()) == "");
    }
  });

  SECTION("Sink pulls first") {
    /*
     * Source
     */
    CHECK(is_source_empty(a.state()) == "");

    a.port_fill(debug ? "manual source (fill)" : "");
    // Cannot check for full here because sink may pull before push

    a.port_push(debug ? "manual source (push)" : "");
    CHECK(is_source_empty(a.state()) == "");

    a.port_exhausted(debug ? "manual source (stop)" : "");
    CHECK(is_stopping(a.state()) == "");
  }

  fut_b.get();

  CHECK(str(a.state()) == "done");
};

/**
 * Simple test of the asynchrous state machine policy, launching both an
 * emulated source client and an emulated sink client as asynchronous tasks. The
 * test just runs one pass of each emulated client.  The test also invokes the
 * tasks in all combinations of orderings of task launch and waiting on futures.
 */
TEST_CASE("Port FSM: Asynchronous source and asynchronous sink", "[stop]") {
  [[maybe_unused]] constexpr bool debug = false;

  std::optional<size_t> source_item{0};
  std::optional<size_t> sink_item{0};
  [[maybe_unused]] auto a = AsyncMover2<size_t>{source_item, sink_item};

  a.set_state(two_stage::st_00);

  auto la = [&]() {
    CHECK(is_source_empty(a.state()) == "");

    a.port_fill(debug ? "async source (fill)" : "");
    // Cannot check for full here because sink may pull before push

    a.port_push(debug ? "async source (push)" : "");
    CHECK(is_source_empty(a.state()) == "");

    a.port_exhausted(debug ? "async source (stop)" : "");
    CHECK(is_stopping(a.state()) == "");
  };

  auto lb = [&]() {
    a.port_pull(debug ? "async sink (pull)" : "");
    CHECK(is_sink_full(a.state()) == "");

    a.port_drain(debug ? "async sink (drain)" : "");
    if (!(is_done(a.state()) == "")) {
      a.port_pull(debug ? "async sink (pull)" : "");
      CHECK(is_done(a.state()) == "");
    } else {
      CHECK(is_done(a.state()) == "");
    }
  };

  SECTION("launch source then sink, get source then sink") {
    auto fut_a = std::async(std::launch::async, la);
    auto fut_b = std::async(std::launch::async, lb);
    fut_a.get();
    fut_b.get();
  }

  SECTION("launch source then sink, get sink then source") {
    auto fut_a = std::async(std::launch::async, la);
    auto fut_b = std::async(std::launch::async, lb);
    fut_b.get();
    fut_a.get();
  }

  SECTION("launch sink then source, get source then sink") {
    auto fut_b = std::async(std::launch::async, lb);
    auto fut_a = std::async(std::launch::async, la);
    fut_a.get();
    fut_b.get();
  }
  SECTION("launch source then sink, get sink then source") {
    auto fut_b = std::async(std::launch::async, lb);
    auto fut_a = std::async(std::launch::async, la);
    fut_b.get();
    fut_a.get();
  }

  CHECK(str(a.state()) == "done");
};

/**
 * Test of the asynchrous state machine policy, launching both an
 * emulated source client and an emulated sink client as asynchronous tasks. The
 * test runs n iterations of each emulated client.  The test also invokes the
 * tasks in all combinations of orderings of task launch and waiting on futures.
 */
TEST_CASE(
    "AsynchronousPolicy: Asynchronous source and asynchronous sink, n "
    "iterations, no delays",
    "[stop]") {
  size_t rounds = GENERATE(0, 1, 2, 17);
  size_t offset = GENERATE(0, 1, 2, 5);

  [[maybe_unused]] constexpr bool debug = false;

  std::optional<size_t> source_item{0};
  std::optional<size_t> sink_item{0};
  [[maybe_unused]] auto a = AsyncMover2<size_t>{source_item, sink_item};

  if (debug)
    a.enable_debug();

  a.set_state(two_stage::st_00);

  if (debug)
    rounds = 3;

  auto source_node = [&]() {
    size_t n = rounds;

    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }
      a.port_fill(debug ? "async source (fill)" : "");
      // Cannot check for full here because sink may pull before push

      a.port_push(debug ? "async source (push)" : "");
      CHECK(is_source_empty(a.state()) == "");
    }
    a.port_exhausted(debug ? "async source (stop)" : "");
    CHECK(is_stopping(a.state()) == "");
  };

  auto sink_node = [&]() {
    size_t n = rounds + offset;
    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }
      a.port_pull(debug ? "async sink (pull)" : "");

      if (a.is_done()) {
        CHECK(is_done(a.state()) == "");
        CHECK(n == offset - 1);
        break;
      }

      CHECK(is_sink_full(a.state()) == "");

      a.port_drain(debug ? "async sink (drain)" : "");

      if (a.is_done()) {
        CHECK(is_done(a.state()) == "");
        CHECK(n == offset);
        break;
      }
    }

    //    CHECK(is_done(a.state()) == "");

    if (!a.is_done()) {
      a.port_pull(debug ? "async sink (pull)" : "");
      CHECK(is_done(a.state()) == "");
    }
    CHECK(n == offset - 1);
    CHECK(is_done(a.state()) == "");
  };

  SECTION(
      "launch source before sink, get source before sink " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_a = std::async(std::launch::async, source_node);
    auto fut_b = std::async(std::launch::async, sink_node);

    fut_a.get();
    fut_b.get();
  }

  SECTION(
      "launch sink before source, get source before sink " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_b = std::async(std::launch::async, sink_node);
    auto fut_a = std::async(std::launch::async, source_node);

    fut_a.get();
    fut_b.get();
  }

  SECTION(
      "launch source before sink, get sink before source " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_a = std::async(std::launch::async, source_node);
    auto fut_b = std::async(std::launch::async, sink_node);

    fut_b.get();
    fut_a.get();
  }

  SECTION(
      "launch sink before source, get sink before source " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_b = std::async(std::launch::async, sink_node);
    auto fut_a = std::async(std::launch::async, source_node);

    fut_b.get();
    fut_a.get();
  }

  CHECK(str(a.state()) == "done");
  CHECK((a.source_swaps() + a.sink_swaps()) == rounds);
};

/**
 * Repeat of above but with unified asynchronous policy
 */
TEST_CASE(
    "UnifiedAsynchronousPolicy: UnifiedAsynchronous source and asynchronous "
    "sink, no delays, n "
    "iterations",
    "[stop]") {
  size_t rounds = GENERATE(0, 1, 2, 17);
  size_t offset = GENERATE(0, 1, 2, 5);

  [[maybe_unused]] constexpr bool debug = false;

  std::optional<size_t> source_item{0};
  std::optional<size_t> sink_item{0};
  [[maybe_unused]] auto a = UnifiedAsyncMover2<size_t>{source_item, sink_item};

  if (debug)
    a.enable_debug();

  a.set_state(two_stage::st_00);

  if (debug)
    rounds = 3;

  auto source_node = [&]() {
    size_t n = rounds;

    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }
      a.port_fill(debug ? "async source (fill)" : "");
      // Cannot check for full here because sink may pull before push

      a.port_push(debug ? "async source (push)" : "");
      CHECK(is_source_empty(a.state()) == "");
    }
    a.port_exhausted(debug ? "async source (stop)" : "");
    CHECK(is_stopping(a.state()) == "");
  };

  auto sink_node = [&]() {
    size_t n = rounds + offset;
    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }
      a.port_pull(debug ? "async sink (pull)" : "");
      if (a.is_done()) {
        CHECK(is_done(a.state()) == "");
        CHECK(n == offset - 1);
        break;
      }

      CHECK(is_sink_full(a.state()) == "");

      a.port_drain(debug ? "async sink (drain)" : "");
      if (a.is_done()) {
        CHECK(is_done(a.state()) == "");
        CHECK(n == offset);
        break;
      }
    }
    if (!a.is_done()) {
      a.port_pull(debug ? "async sink (pull)" : "");
      CHECK(is_done(a.state()) == "");
    }
    CHECK(n == offset - 1);
    CHECK(is_done(a.state()) == "");
  };

  SECTION(
      "launch source before sink, get source before sink " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_a = std::async(std::launch::async, source_node);
    auto fut_b = std::async(std::launch::async, sink_node);

    fut_a.get();
    fut_b.get();
  }

  SECTION(
      "launch sink before source, get source before sink " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_b = std::async(std::launch::async, sink_node);
    auto fut_a = std::async(std::launch::async, source_node);

    fut_a.get();
    fut_b.get();
  }

  SECTION(
      "launch source before sink, get sink before source " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_a = std::async(std::launch::async, source_node);
    auto fut_b = std::async(std::launch::async, sink_node);

    fut_b.get();
    fut_a.get();
  }

  SECTION(
      "launch sink before source, get sink before source " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_b = std::async(std::launch::async, sink_node);
    auto fut_a = std::async(std::launch::async, source_node);

    fut_b.get();
    fut_a.get();
  }

  CHECK(str(a.state()) == "done");
  //  CHECK((a.source_swaps() + a.sink_swaps()) == rounds);
};

/**
 * Test of the asynchrous state machine policy, launching both an
 * emulated source client and an emulated sink client as asynchronous tasks.
 * The test runs n iterations of each emulated client.  The test also invokes
 * the tasks in all combinations of orderings of task launch and waiting on
 * futures.
 */
TEST_CASE(
    "AsynchronousPolicy: Asynchronous source and asynchronous sink, n "
    "iterations, delays",
    "[stop]") {
  size_t rounds = GENERATE(0, 1, 2, 17);
  size_t offset = GENERATE(0, 1, 2, 5);

  [[maybe_unused]] constexpr bool debug = false;

  std::optional<size_t> source_item{0};
  std::optional<size_t> sink_item{0};
  [[maybe_unused]] auto a = AsyncMover2<size_t>{source_item, sink_item};

  if (debug)
    a.enable_debug();

  a.set_state(two_stage::st_00);

  if (debug)
    rounds = 3;

  auto source_node = [&]() {
    size_t n = rounds;

    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }
      CHECK(is_source_empty(a.state()) == "");

      /* Emulate running a producer task. */
      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));
      CHECK(is_source_empty(a.state()) == "");

      a.port_fill(debug ? "async source (fill)" : "");

      /* Insert delay to encourage context switch */
      std::this_thread::sleep_for(std::chrono::microseconds(random_us(250)));

      a.port_push(debug ? "async source (push)" : "");
      CHECK(is_source_empty(a.state()) == "");
    }
    a.port_exhausted(debug ? "async source (stop)" : "");
    CHECK(is_stopping(a.state()) == "");
  };

  auto sink_node = [&]() {
    size_t n = rounds + offset;
    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }
      a.port_pull(debug ? "async sink (pull)" : "");
      if (a.is_done()) {
        CHECK(is_done(a.state()) == "");
        CHECK(n == offset - 1);
        break;
      }
      CHECK(is_sink_full(a.state()) == "");

      /* Emulate running a consumer task. */
      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));
      CHECK(is_sink_full(a.state()) == "");

      a.port_drain(debug ? "async sink (drain)" : "");

      /* Insert delay to encourage context switch */
      std::this_thread::sleep_for(std::chrono::microseconds(random_us(250)));

      if (a.is_done()) {
        CHECK(is_done(a.state()) == "");
        CHECK(n == offset);
        break;
      }
    }
    if (!a.is_done()) {
      a.port_pull(debug ? "async sink node" : "");
      CHECK(is_done(a.state()) == "");
    }
    CHECK(n == offset - 1);
    CHECK(is_done(a.state()) == "");
  };

  SECTION(
      "launch source before sink, get source before sink " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_a = std::async(std::launch::async, source_node);
    auto fut_b = std::async(std::launch::async, sink_node);

    fut_a.get();
    fut_b.get();
  }

  SECTION(
      "launch sink before source, get source before sink " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_b = std::async(std::launch::async, sink_node);
    auto fut_a = std::async(std::launch::async, source_node);

    fut_a.get();
    fut_b.get();
  }

  SECTION(
      "launch source before sink, get sink before source " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_a = std::async(std::launch::async, source_node);
    auto fut_b = std::async(std::launch::async, sink_node);

    fut_b.get();
    fut_a.get();
  }

  SECTION(
      "launch sink before source, get sink before source " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_b = std::async(std::launch::async, sink_node);
    auto fut_a = std::async(std::launch::async, source_node);

    fut_b.get();
    fut_a.get();
  }

  CHECK(str(a.state()) == "done");
  CHECK((a.source_swaps() + a.sink_swaps()) == rounds);
};

/**
 * Repeat of above but with unified asynchronous scheduler
 */
TEST_CASE(
    "UnifiedAsynchronousPolicy: UnifiedAsynchronous source and asynchronous "
    "sink, n "
    "iterations, delays",
    "[stop]") {
  size_t rounds = GENERATE(0, 1, 2, 17);
  size_t offset = GENERATE(0, 1, 2, 5);

  [[maybe_unused]] constexpr bool debug = false;

  std::optional<size_t> source_item{0};
  std::optional<size_t> sink_item{0};
  [[maybe_unused]] auto a = UnifiedAsyncMover2<size_t>{source_item, sink_item};

  if (debug)
    a.enable_debug();

  a.set_state(two_stage::st_00);

  if (debug)
    rounds = 3;

  auto source_node = [&]() {
    size_t n = rounds;

    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }
      CHECK(is_source_empty(a.state()) == "");

      /* Emulate running a producer task. */
      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));
      CHECK(is_source_empty(a.state()) == "");

      a.port_fill(debug ? "async source (fill)" : "");

      /* Insert delay to encourage context switch */
      std::this_thread::sleep_for(std::chrono::microseconds(random_us(250)));

      a.port_push(debug ? "async source (push)" : "");
      CHECK(is_source_empty(a.state()) == "");
    }
    a.port_exhausted(debug ? "async source (stop)" : "");
    CHECK(is_stopping(a.state()) == "");
  };

  auto sink_node = [&]() {
    size_t n = rounds + offset;
    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }
      a.port_pull(debug ? "async sink (pull)" : "");
      if (a.is_done()) {
        CHECK(is_done(a.state()) == "");
        CHECK(n == offset - 1);
        break;
      }
      CHECK(is_sink_full(a.state()) == "");

      /* Emulate running a consumer task. */
      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));
      CHECK(is_sink_full(a.state()) == "");

      a.port_drain(debug ? "async sink (drain)" : "");

      /* Insert delay to encourage context switch */
      std::this_thread::sleep_for(std::chrono::microseconds(random_us(250)));

      if (a.is_done()) {
        CHECK(is_done(a.state()) == "");
        CHECK(n == offset);
        break;
      }
    }
    if (!a.is_done()) {
      a.port_pull(debug ? "async sink node" : "");
      CHECK(is_done(a.state()) == "");
    }
    CHECK(n == offset - 1);
    CHECK(is_done(a.state()) == "");
  };

  SECTION(
      "launch source before sink, get source before sink " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_a = std::async(std::launch::async, source_node);
    auto fut_b = std::async(std::launch::async, sink_node);

    fut_a.get();
    fut_b.get();
  }

  SECTION(
      "launch sink before source, get source before sink " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_b = std::async(std::launch::async, sink_node);
    auto fut_a = std::async(std::launch::async, source_node);

    fut_a.get();
    fut_b.get();
  }

  SECTION(
      "launch source before sink, get sink before source " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_a = std::async(std::launch::async, source_node);
    auto fut_b = std::async(std::launch::async, sink_node);

    fut_b.get();
    fut_a.get();
  }

  SECTION(
      "launch sink before source, get sink before source " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_b = std::async(std::launch::async, sink_node);
    auto fut_a = std::async(std::launch::async, source_node);

    fut_b.get();
    fut_a.get();
  }

  CHECK(str(a.state()) == "done");
  //  CHECK((a.source_swaps() + a.sink_swaps()) == rounds);
};

/**
 * Test that we can correctly pass a sequence of integers from source to sink.
 * Random delays are inserted between each step of each function in order to
 * increase the likelihood of exposing race conditions / deadlocks.
 *
 * The test creates an asynchronous task for a source node client and for a
 * sync node client, and launches them separately using `std::async`.  To
 * create different interleavings of the tasks, we use all combinations of
 * ordering for launching the tasks and waiting on their futures.
 */
TEST_CASE("Pass a sequence of n integers, async", "[stop]") {
  size_t rounds = GENERATE(0, 1, 2, 17);
  size_t offset = GENERATE(0, 1, 2, 5);

  [[maybe_unused]] constexpr bool debug = false;

  std::optional<size_t> source_item{0};
  std::optional<size_t> sink_item{0};
  [[maybe_unused]] auto a = AsyncMover2<size_t>{source_item, sink_item};
  if (debug) {
    a.enable_debug();
  }

  a.set_state(two_stage::st_00);

  if (debug)
    rounds = 3;

  std::vector<size_t> input(rounds);
  std::vector<size_t> output(rounds);

  std::iota(input.begin(), input.end(), 19);
  std::fill(output.begin(), output.end(), 0);
  auto i = input.begin();
  auto j = output.begin();

  if (rounds != 0) {
    CHECK(std::equal(input.begin(), input.end(), output.begin()) == false);
  }

  auto source_node = [&]() {
    size_t n = rounds;

    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }

      CHECK(is_source_empty(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      CHECK(is_source_empty(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(127)));

      *(a.source_item()) = *i++;

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(333)));

      CHECK(is_source_empty(a.state()) == "");

      a.port_fill(debug ? "async source node" : "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      a.port_push(debug ? "async source node" : "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(333)));

      *(a.source_item()) = EMPTY_SOURCE;

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(127)));
    }
    a.port_exhausted(debug ? "async source (stop)" : "");
    CHECK(is_stopping(a.state()) == "");
  };

  auto sink_node = [&]() {
    size_t n = rounds + offset;
    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }
      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      a.port_pull(debug ? "async sink node" : "");

      if (a.is_done()) {
        CHECK(is_done(a.state()) == "");
        CHECK(n == offset - 1);
        break;
      }

      CHECK(is_sink_full(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(127)));

      CHECK(is_sink_full(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(333)));

      *j++ = *(a.sink_item());

      CHECK(is_sink_full(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      *(a.sink_item()) = EMPTY_SINK;

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(333)));

      a.port_drain(debug ? "async sink node" : "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(127)));

      if (a.is_done()) {
        CHECK(is_done(a.state()) == "");
        CHECK(n == offset);
        break;
      }
    }
    if (!a.is_done()) {
      a.port_pull(debug ? "async sink node" : "");
      CHECK(is_done(a.state()) == "");
    }
    CHECK(n == offset - 1);
    CHECK(is_done(a.state()) == "");
  };

  SECTION(
      "launch source before sink, get source before sink " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_a = std::async(std::launch::async, source_node);
    auto fut_b = std::async(std::launch::async, sink_node);

    fut_a.get();
    fut_b.get();
  }

  SECTION(
      "launch sink before source, get source before sink " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_b = std::async(std::launch::async, sink_node);
    auto fut_a = std::async(std::launch::async, source_node);

    fut_a.get();
    fut_b.get();
  }

  SECTION(
      "launch source before sink, get sink before source " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_a = std::async(std::launch::async, source_node);
    auto fut_b = std::async(std::launch::async, sink_node);

    fut_b.get();
    fut_a.get();
  }

  SECTION(
      "launch sink before source, get sink before source " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
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
  CHECK(str(a.state()) == "done");
  CHECK((a.source_swaps() + a.sink_swaps()) == rounds);
}

/**
 * Repeat the above but with unified asynchronous scheduler
 */
TEST_CASE("Pass a sequence of n integers, unified async", "[stop]") {
  size_t rounds = GENERATE(0, 1, 2, 17);
  size_t offset = GENERATE(0, 1, 2, 5);

  [[maybe_unused]] constexpr bool debug = false;

  std::optional<size_t> source_item{0};
  std::optional<size_t> sink_item{0};
  [[maybe_unused]] auto a = UnifiedAsyncMover2<size_t>{source_item, sink_item};
  if (debug) {
    a.enable_debug();
  }

  a.set_state(two_stage::st_00);

  if (debug)
    rounds = 3;

  std::vector<size_t> input(rounds);
  std::vector<size_t> output(rounds);

  std::iota(input.begin(), input.end(), 19);
  std::fill(output.begin(), output.end(), 0);
  auto i = input.begin();
  auto j = output.begin();

  if (rounds != 0) {
    CHECK(std::equal(input.begin(), input.end(), output.begin()) == false);
  }

  auto source_node = [&]() {
    size_t n = rounds;

    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }

      CHECK(is_source_empty(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      CHECK(is_source_empty(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(127)));

      *(a.source_item()) = *i++;

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(333)));

      CHECK(is_source_empty(a.state()) == "");

      a.port_fill(debug ? "async source node" : "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      a.port_push(debug ? "async source node" : "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(333)));

      *(a.source_item()) = EMPTY_SOURCE;

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(127)));
    }
    a.port_exhausted(debug ? "async source (stop)" : "");
    CHECK(is_stopping(a.state()) == "");
  };

  auto sink_node = [&]() {
    size_t n = rounds + offset;
    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }

      // It doesn't seem we actually need these guards here?
      // while (a.state() == two_stage::st_11 ||
      //             a.state() == two_stage::st_01)
      //        ;

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      a.port_pull(debug ? "async sink node" : "");

      if (a.is_done()) {
        CHECK(is_done(a.state()) == "");
        CHECK(n == offset - 1);
        break;
      }

      CHECK(is_sink_full(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(127)));

      CHECK(is_sink_full(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(333)));

      *j++ = *(a.sink_item());

      CHECK(is_sink_full(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      *(a.sink_item()) = EMPTY_SINK;

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(333)));

      a.port_drain(debug ? "async sink node" : "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(127)));

      if (a.is_done()) {
        CHECK(is_done(a.state()) == "");
        CHECK(n == offset);
        break;
      }
    }
    if (!a.is_done()) {
      a.port_pull(debug ? "async sink (pull)" : "");
      CHECK(is_done(a.state()) == "");
    }
    CHECK(n == offset - 1);
    CHECK(is_done(a.state()) == "");
  };

  SECTION(
      "launch source before sink, get source before sink " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_a = std::async(std::launch::async, source_node);
    auto fut_b = std::async(std::launch::async, sink_node);

    fut_a.get();
    fut_b.get();
  }

  SECTION(
      "launch sink before source, get source before sink " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_b = std::async(std::launch::async, sink_node);
    auto fut_a = std::async(std::launch::async, source_node);

    fut_a.get();
    fut_b.get();
  }

  SECTION(
      "launch source before sink, get sink before source " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_a = std::async(std::launch::async, source_node);
    auto fut_b = std::async(std::launch::async, sink_node);

    fut_b.get();
    fut_a.get();
  }

  SECTION(
      "launch sink before source, get sink before source " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
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
  CHECK(str(a.state()) == "done");
  //  CHECK((a.source_swaps() + a.sink_swaps()) == rounds);
}

/**
 * Repeat of above but with three-stage mover
 */
TEST_CASE("Pass a sequence of n integers, three stage, async", "[stop]") {
  [[maybe_unused]] constexpr bool debug = false;
  size_t rounds = GENERATE(0, 1, 2, 17);
  size_t offset = GENERATE(0, 1, 2, 5);

  std::optional<size_t> source_item{0};
  std::optional<size_t> mid_item{0};
  std::optional<size_t> sink_item{0};
  [[maybe_unused]] auto a =
      AsyncMover3<size_t>{source_item, mid_item, sink_item};
  if (debug) {
    a.enable_debug();
  }

  a.set_state(three_stage::st_000);

  if (debug)
    rounds = 3;

  std::vector<size_t> input(rounds);
  std::vector<size_t> output(rounds);

  std::iota(input.begin(), input.end(), 19);
  std::fill(output.begin(), output.end(), 0);
  auto i = input.begin();
  auto j = output.begin();

  if (rounds != 0) {
    CHECK(std::equal(input.begin(), input.end(), output.begin()) == false);
  }

  auto source_node = [&]() {
    size_t n = rounds;

    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }

      CHECK(is_source_empty(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      CHECK(is_source_empty(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(127)));

      *(a.source_item()) = *i++;

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(333)));

      CHECK(is_source_empty(a.state()) == "");

      a.port_fill(debug ? "async source node" : "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      a.port_push(debug ? "async source node" : "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(333)));

      *(a.source_item()) = EMPTY_SOURCE;

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(127)));
    }
    a.port_exhausted(debug ? "async source (stop)" : "");
    CHECK(is_stopping(a.state()) == "");
  };

  auto sink_node = [&]() {
    size_t n = rounds + offset;
    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }
      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      a.port_pull(debug ? "async sink node" : "");

      if (a.is_done()) {
        CHECK(is_done(a.state()) == "");
        CHECK(n == offset - 1);
        break;
      }

      CHECK(is_sink_full(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(127)));

      CHECK(is_sink_full(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(333)));

      *j++ = *(a.sink_item());

      CHECK(is_sink_full(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      *(a.sink_item()) = EMPTY_SINK;

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(333)));

      a.port_drain(debug ? "async sink node" : "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(127)));

      if (a.is_done()) {
        CHECK(is_done(a.state()) == "");
        CHECK(n == offset);
        break;
      }
      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));
    }
    if (!a.is_done()) {
      a.port_pull(debug ? "async sink node" : "");
      CHECK(is_done(a.state()) == "");
    }
    CHECK(n == offset - 1);
    CHECK(is_done(a.state()) == "");
  };

  SECTION(
      "launch source before sink, get source before sink " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_a = std::async(std::launch::async, source_node);
    auto fut_b = std::async(std::launch::async, sink_node);

    fut_a.get();
    fut_b.get();
  }

  SECTION(
      "launch sink before source, get source before sink " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_b = std::async(std::launch::async, sink_node);
    auto fut_a = std::async(std::launch::async, source_node);

    fut_a.get();
    fut_b.get();
  }

  SECTION(
      "launch source before sink, get sink before source " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_a = std::async(std::launch::async, source_node);
    auto fut_b = std::async(std::launch::async, sink_node);

    fut_b.get();
    fut_a.get();
  }

  SECTION(
      "launch sink before source, get sink before source " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
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
        std::cout << "=== " << j << " (" << input[j] << ", " << output[j] << ")"
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
      std::cout << "--- " << k << " (" << input[k] << ", " << output[k] << ")"
                << std::endl;
    } else {
      std::cout << "this should not happen" << std::endl;
    }
  }

  CHECK(std::equal(input.begin(), input.end(), output.begin()));
  CHECK(str(a.state()) == "done");
}

/**
 * Repeat the above but with unified asynchronous scheduler
 */
TEST_CASE(
    "Pass a sequence of n integers, three stage, unified async", "[stop]") {
  [[maybe_unused]] constexpr bool debug = false;
  size_t rounds = GENERATE(0, 1, 2, 17);
  size_t offset = GENERATE(0, 1, 2, 5);

  std::optional<size_t> source_item{0};
  std::optional<size_t> mid_item{0};
  std::optional<size_t> sink_item{0};
  [[maybe_unused]] auto a =
      UnifiedAsyncMover3<size_t>{source_item, mid_item, sink_item};
  if (debug) {
    a.enable_debug();
  }

  a.set_state(three_stage::st_000);

  if (debug)
    rounds = 3;

  std::vector<size_t> input(rounds);
  std::vector<size_t> output(rounds);

  std::iota(input.begin(), input.end(), 19);
  std::fill(output.begin(), output.end(), 0);
  auto i = input.begin();
  auto j = output.begin();

  if (rounds != 0) {
    CHECK(std::equal(input.begin(), input.end(), output.begin()) == false);
  }

  auto source_node = [&]() {
    size_t n = rounds;

    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }

      CHECK(is_source_empty(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      CHECK(is_source_empty(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(127)));

      *(a.source_item()) = *i++;

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(333)));

      CHECK(is_source_empty(a.state()) == "");

      a.port_fill(debug ? "async source node" : "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      a.port_push(debug ? "async source node" : "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(333)));

      *(a.source_item()) = EMPTY_SOURCE;

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(127)));
    }
    a.port_exhausted(debug ? "async source (stop)" : "");
    CHECK(is_stopping(a.state()) == "");
  };

  auto sink_node = [&]() {
    size_t n = rounds + offset;
    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      a.port_pull(debug ? "async sink node" : "");

      if (a.is_done()) {
        CHECK(is_done(a.state()) == "");
        CHECK(n == offset - 1);
        break;
      }

      CHECK(is_sink_full(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(127)));

      CHECK(is_sink_full(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(333)));

      *j++ = *(a.sink_item());

      CHECK(is_sink_full(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      *(a.sink_item()) = EMPTY_SINK;

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(333)));

      a.port_drain(debug ? "async sink node" : "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(127)));

      if (a.is_done()) {
        CHECK(is_done(a.state()) == "");
        CHECK(n == offset);
        break;
      }
    }
    if (!a.is_done()) {
      a.port_pull(debug ? "async sink node" : "");
      CHECK(is_done(a.state()) == "");
    }
    CHECK(n == offset - 1);
    CHECK(is_done(a.state()) == "");
  };

  SECTION(
      "launch source before sink, get source before sink " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_a = std::async(std::launch::async, source_node);
    auto fut_b = std::async(std::launch::async, sink_node);

    fut_a.get();
    fut_b.get();
  }

  SECTION(
      "launch sink before source, get source before sink " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_b = std::async(std::launch::async, sink_node);
    auto fut_a = std::async(std::launch::async, source_node);

    fut_a.get();
    fut_b.get();
  }

  SECTION(
      "launch source before sink, get sink before source " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
    auto fut_a = std::async(std::launch::async, source_node);
    auto fut_b = std::async(std::launch::async, sink_node);

    fut_b.get();
    fut_a.get();
  }

  SECTION(
      "launch sink before source, get sink before source " +
      std::to_string(rounds) + " / " + std::to_string(offset)) {
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
        std::cout << "=== " << j << " (" << input[j] << ", " << output[j] << ")"
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
      std::cout << "--- " << k << " (" << input[k] << ", " << output[k] << ")"
                << std::endl;
    } else {
      std::cout << "this should not happen" << std::endl;
    }
  }

  CHECK(std::equal(input.begin(), input.end(), output.begin()));
  CHECK(str(a.state()) == "done");
}
