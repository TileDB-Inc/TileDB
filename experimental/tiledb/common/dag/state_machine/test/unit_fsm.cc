/**
 * @file unit_fsm.cc
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

#include "unit_fsm.h"
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

TEST_CASE("Port FSM: Construct DebugStateMachine2", "[fsm]") {
  [[maybe_unused]] auto a = DebugStateMachine2<size_t>{};

  CHECK(a.state() == two_stage::st_00);
}

TEST_CASE("Port FSM: Copy, Move, etc", "[fsm]") {
  [[maybe_unused]] auto a = DebugStateMachine3<size_t>{};
  std::vector<DebugStateMachine2<size_t>> v;
  v.reserve(55);

  [[maybe_unused]] auto b = DebugStateMachine2<size_t>{};

  std::vector<DebugStateMachine3<size_t>> w;
  w.reserve(55);

  auto t = std::make_tuple(a, b);
  std::vector<decltype(t)> u;
  u.reserve(55);

  auto foo = [](DebugStateMachine3<size_t>&&) {};
  auto bar = [](DebugStateMachine2<size_t>&&) {};

  foo(std::move(a));
  bar(std::move(b));
}

TEST_CASE("Port FSM: Start up", "[fsm]") {
  constexpr bool debug = false;
  [[maybe_unused]] auto a = DebugStateMachine2<size_t>{};

  if (debug)
    a.enable_debug();
  CHECK(a.state() == two_stage::st_00);

  SECTION("start source") {
    a.port_fill(debug ? "start source" : "");
    CHECK(str(a.state()) == "st_10");
  }

  SECTION("start sink") {
    a.port_fill(debug ? "start sink (fill)" : "");
    CHECK(str(a.state()) == "st_10");
    a.port_push(debug ? "start sink (push)" : "");
    CHECK(is_source_empty(a.state()) == "");
    a.port_drain(debug ? "start sink (drain)" : "");
    CHECK(is_sink_empty(a.state()) == "");
  }
}

/*
 * Use the `DebugPolicy` to verify startup state and some more involved
 * transition sequences.
 */
TEST_CASE("Port FSM: Basic manual sequence", "[fsm]") {
  [[maybe_unused]] auto a = DebugStateMachine2<size_t>{};
  CHECK(a.state() == two_stage::st_00);

  a.port_fill();
  CHECK(str(a.state()) == "st_10");
  a.port_push();
  CHECK(str(a.state()) == "st_01");
  a.port_fill();
  CHECK(str(a.state()) == "st_11");
  a.port_drain();
  CHECK(str(a.state()) == "st_10");
  a.port_push();
  CHECK(str(a.state()) == "st_01");

  a.port_drain();
  CHECK(str(a.state()) == "st_00");

  a.port_fill();
  CHECK(str(a.state()) == "st_10");
  a.port_pull();
  CHECK(str(a.state()) == "st_01");
  a.port_fill();
  CHECK(str(a.state()) == "st_11");
  a.port_drain();
  CHECK(str(a.state()) == "st_10");
  a.port_pull();
  CHECK(str(a.state()) == "st_01");

  a.port_drain();
  CHECK(a.state() == two_stage::st_00);

  a.port_fill();
  CHECK(str(a.state()) == "st_10");
  a.port_push();
  CHECK(str(a.state()) == "st_01");
  a.port_fill();
  CHECK(str(a.state()) == "st_11");
  a.port_drain();
  CHECK(str(a.state()) == "st_10");
  a.port_pull();
  CHECK(str(a.state()) == "st_01");

  a.port_drain();
  CHECK(a.state() == two_stage::st_00);

  a.port_fill();
  CHECK(str(a.state()) == "st_10");
  a.port_pull();
  CHECK(str(a.state()) == "st_01");
  a.port_fill();
  CHECK(str(a.state()) == "st_11");
  a.port_drain();
  CHECK(str(a.state()) == "st_10");
  a.port_push();
  CHECK(str(a.state()) == "st_01");

  a.port_drain();
  CHECK(a.state() == two_stage::st_00);
}

/*
 * Use the `DebugStateMachine` to verify startup state and some more involved
 * transition sequences.
 */
TEST_CASE("Port FSM3: Basic manual sequence", "[fsm]") {
  [[maybe_unused]] auto a = DebugStateMachine3<size_t>{};
  CHECK(a.state() == three_stage::st_000);

  SECTION("Two element tests") {
    a.port_fill();
    CHECK(str(a.state()) == "st_100");
    a.port_push();
    CHECK(str(a.state()) == "st_001");
    a.port_fill();
    CHECK(str(a.state()) == "st_101");
    a.port_drain();
    CHECK(str(a.state()) == "st_100");
    a.port_push();
    CHECK(str(a.state()) == "st_001");

    a.port_drain();
    CHECK(str(a.state()) == "st_000");

    a.port_fill();
    CHECK(str(a.state()) == "st_100");
    a.port_pull();
    CHECK(str(a.state()) == "st_001");
    a.port_fill();
    CHECK(str(a.state()) == "st_101");
    a.port_drain();
    CHECK(str(a.state()) == "st_100");
    a.port_pull();
    CHECK(str(a.state()) == "st_001");

    a.port_drain();
    CHECK(str(a.state()) == "st_000");

    a.port_fill();
    CHECK(str(a.state()) == "st_100");
    a.port_push();
    CHECK(str(a.state()) == "st_001");
    a.port_fill();
    CHECK(str(a.state()) == "st_101");
    a.port_push();
    CHECK(str(a.state()) == "st_011");
    a.port_drain();
    CHECK(str(a.state()) == "st_010");
    a.port_push();
    CHECK(str(a.state()) == "st_001");

    a.port_drain();
    CHECK(str(a.state()) == "st_000");

    a.port_fill();
    CHECK(str(a.state()) == "st_100");
    a.port_pull();
    CHECK(str(a.state()) == "st_001");
    a.port_fill();
    CHECK(str(a.state()) == "st_101");
    a.port_pull();
    CHECK(str(a.state()) == "st_011");
    a.port_drain();
    CHECK(str(a.state()) == "st_010");
    a.port_pull();
    CHECK(str(a.state()) == "st_001");

    a.port_drain();
    CHECK(str(a.state()) == "st_000");
  }

  SECTION("three element tests") {
    a.port_fill();
    CHECK(str(a.state()) == "st_100");
    a.port_push();
    CHECK(str(a.state()) == "st_001");
    a.port_fill();
    CHECK(str(a.state()) == "st_101");
    a.port_push();
    CHECK(str(a.state()) == "st_011");
    a.port_fill();
    CHECK(str(a.state()) == "st_111");
    a.port_drain();
    CHECK(str(a.state()) == "st_110");
    a.port_push();
    CHECK(str(a.state()) == "st_011");
    a.port_drain();
    CHECK(str(a.state()) == "st_010");
    a.port_push();
    CHECK(str(a.state()) == "st_001");

    a.port_drain();
    CHECK(str(a.state()) == "st_000");

    a.port_fill();
    CHECK(str(a.state()) == "st_100");
    a.port_pull();
    CHECK(str(a.state()) == "st_001");
    a.port_fill();
    CHECK(str(a.state()) == "st_101");
    a.port_pull();
    CHECK(str(a.state()) == "st_011");
    a.port_fill();
    CHECK(str(a.state()) == "st_111");
    a.port_drain();
    CHECK(str(a.state()) == "st_110");
    a.port_pull();
    CHECK(str(a.state()) == "st_011");
    a.port_drain();
    CHECK(str(a.state()) == "st_010");
    a.port_pull();
    CHECK(str(a.state()) == "st_001");

    a.port_drain();
    CHECK(str(a.state()) == "st_000");
  }
}

/**
 * Simple test of asynchrous state machine policy, launching an emulated source
 * client as an asynchronous task and running an emulated sink client in the
 * main thread. The test just runs one pass of each emulated client.
 */
TEST_CASE("AsynchronousPolicy: Asynchronous source and manual sink", "[fsm]") {
  [[maybe_unused]] constexpr bool debug = false;

  std::optional<size_t> source_item{0};
  std::optional<size_t> sink_item{0};
  [[maybe_unused]] auto a = AsyncMover2<size_t>{source_item, sink_item};

  a.set_state(two_stage::st_00);

  auto fut_a = std::async(std::launch::async, [&]() {
    a.port_fill(debug ? "async source (fill)" : "");
    CHECK(is_source_full(a.state()) == "");
    a.port_push(debug ? "async source (push)" : "");
    CHECK(is_source_empty(a.state()) == "");
  });

  //  std::this_thread::sleep_for(std::chrono::microseconds(random_us(5000)));

  if (debug)
    std::cout << "About to call drained" << std::endl;

  //  std::this_thread::sleep_for(std::chrono::microseconds(random_us(5000)));

  a.port_pull(debug ? "manual sink (pull)" : "");
  CHECK(str(a.state()) == "st_01");

  a.port_drain(debug ? "manual sink (drain)" : "");

  fut_a.get();

  CHECK(str(a.state()) == "st_00");
};

/**
 * Simple test of asynchrous state machine policy, launching an emulated sink
 * client as an asynchronous task and running an emulated source client in the
 * main thread. The test just runs one pass of each emulated client.
 */
TEST_CASE("AsynchronousPolicy: Manual source and asynchronous sink", "[fsm]") {
  [[maybe_unused]] constexpr bool debug = false;

  std::optional<size_t> source_item{0};
  std::optional<size_t> sink_item{0};
  [[maybe_unused]] auto a = AsyncMover2<size_t>{source_item, sink_item};

  a.set_state(two_stage::st_00);

  auto fut_b = std::async(std::launch::async, [&]() {
    a.port_pull(debug ? "async sink (pull)" : "");
    CHECK(is_sink_full(a.state()) == "");

    a.port_drain(debug ? "async sink (drain)" : "");
  });

  //  std::this_thread::sleep_for(std::chrono::microseconds(random_us(5000)));

  if (debug)
    std::cout << "About to call fill_source" << std::endl;

  //  std::this_thread::sleep_for(std::chrono::microseconds(random_us(5000)));

  a.port_fill(debug ? "manual source (fill)" : "");
  a.port_push(debug ? "manual source (push)" : "");

  fut_b.get();

  CHECK(str(a.state()) == "st_00");
};

/**
 * Simple test of the unified asynchrous state machine policy, launching an
 * emulated source client as an asynchronous task and running an emulated sink
 * client in the main thread. The test just runs one pass of each emulated
 * client.
 */
TEST_CASE(
    "UnifiedAsynchronousPolicy: Asynchronous source and manual sink", "[fsm]") {
  [[maybe_unused]] constexpr bool debug = false;

  std::optional<size_t> source_item{0};
  std::optional<size_t> sink_item{0};
  [[maybe_unused]] auto a = UnifiedAsyncMover2<size_t>{source_item, sink_item};

  a.set_state(two_stage::st_00);

  auto fut_a = std::async(std::launch::async, [&]() {
    a.port_fill(debug ? "manual async source (fill)" : "");
    a.port_push(debug ? "manual async source (push)" : "");
  });

  if (debug)
    std::cout << "About to call drained" << std::endl;

  a.port_pull(debug ? "manual async sink (pull)" : "");
  a.port_drain(debug ? "manual async sink (drained)" : "");

  fut_a.get();

  CHECK(str(a.state()) == "st_00");
};

/**
 * Simple test of the unified asynchrous state machine policy, launching an
 * emulated source client as an asynchronous task and running an emulated sink
 * client in the main thread. The test just runs one pass of each emulated
 * client.
 */
TEST_CASE(
    "UnifiedAsynchronousPolicy: Manual source and asynchronous sink", "[fsm]") {
  [[maybe_unused]] constexpr bool debug = false;

  std::optional<size_t> source_item{0};
  std::optional<size_t> sink_item{0};
  [[maybe_unused]] auto a = UnifiedAsyncMover2<size_t>{source_item, sink_item};

  a.set_state(two_stage::st_00);

  auto fut_b = std::async(std::launch::async, [&]() {
    a.port_pull(debug ? "manual async sink (pull)" : "");
    a.port_drain(debug ? "manual async sink (drain)" : "");
  });

  //  std::this_thread::sleep_for(std::chrono::microseconds(random_us(5000)));

  if (debug)
    std::cout << "About to call fill_source" << std::endl;

  //  std::this_thread::sleep_for(std::chrono::microseconds(random_us(5000)));

  a.port_fill(debug ? "manual async source (fill)" : "");
  a.port_push(debug ? "manual async source (push)" : "");

  fut_b.get();

  CHECK(str(a.state()) == "st_00");
};

/**
 * Simple test of the asynchrous state machine policy, launching both an
 * emulated source client and an emulated sink client as asynchronous tasks. The
 * test just runs one pass of each emulated client.  The test also invokes the
 * tasks in all combinations of orderings of task launch and waiting on futures.
 */
TEST_CASE(
    "AsynchronousPolicy: Asynchronous source and asynchronous sink", "[fsm]") {
  [[maybe_unused]] constexpr bool debug = false;

  std::optional<size_t> source_item{0};
  std::optional<size_t> sink_item{0};
  [[maybe_unused]] auto a = AsyncMover2<size_t>{source_item, sink_item};

  a.set_state(two_stage::st_00);

  SECTION("launch source then sink, get source then sink") {
    auto fut_a = std::async(std::launch::async, [&]() {
      a.port_fill(debug ? "async source (fill)" : "");
      a.port_push(debug ? "async source (push)" : "");
    });

    auto fut_b = std::async(std::launch::async, [&]() {
      a.port_pull(debug ? "async sink (pull)" : "");
      a.port_drain(debug ? "async dsink (drain)" : "");
    });
    fut_a.get();
    fut_b.get();
  }

  SECTION("launch source then sink, get sink then source") {
    auto fut_a = std::async(std::launch::async, [&]() {
      a.port_fill(debug ? "async source (fill)" : "");
      a.port_push(debug ? "async source (push)" : "");
    });

    auto fut_b = std::async(std::launch::async, [&]() {
      a.port_pull(debug ? "async sink (pull)" : "");
      a.port_drain(debug ? "async sink (drain)" : "");
    });
    fut_b.get();
    fut_a.get();
  }

  SECTION("launch sink then source, get source then sink") {
    auto fut_b = std::async(std::launch::async, [&]() {
      a.port_pull(debug ? "async sink (pull)" : "");
      a.port_drain(debug ? "async dsink (drain)" : "");
    });
    auto fut_a = std::async(std::launch::async, [&]() {
      a.port_fill(debug ? "async source (fill)" : "");
      a.port_push(debug ? "async source (push)" : "");
    });

    fut_a.get();
    fut_b.get();
  }
  SECTION("launch sink then source, get source then sink") {
    auto fut_b = std::async(std::launch::async, [&]() {
      a.port_pull(debug ? "async sink (pull)" : "");
      a.port_drain(debug ? "async dsink (drain)" : "");
    });
    auto fut_a = std::async(std::launch::async, [&]() {
      a.port_fill(debug ? "async source (fill)" : "");
      a.port_push(debug ? "async source (push)" : "");
    });

    fut_a.get();
    fut_b.get();
  }

  SECTION("launch source then sink, get sink then source") {
    auto fut_a = std::async(std::launch::async, [&]() {
      a.port_fill(debug ? "async source (fill)" : "");
      a.port_push(debug ? "async source (push)" : "");
    });

    auto fut_b = std::async(std::launch::async, [&]() {
      a.port_pull(debug ? "async sink (pull)" : "");
      a.port_drain(debug ? "async sink (drain)" : "");
    });
    fut_b.get();
    fut_a.get();
  }

  CHECK(str(a.state()) == "st_00");
};

/**
 * Simple test of the unified asynchrous state machine policy, launching both an
 * emulated source client and an emulated sink client as asynchronous tasks. The
 * test just runs one pass of each emulated client.  The test also invokes the
 * tasks in all combinations of orderings of task launch and waiting on futures.
 */
TEST_CASE(
    "UnifiedAsynchronousPolicy: Asynchronous source and asynchronous "
    "sink",
    "[fsm]") {
  [[maybe_unused]] constexpr bool debug = false;

  std::optional<size_t> source_item{0};
  std::optional<size_t> sink_item{0};
  [[maybe_unused]] auto a = UnifiedAsyncMover2<size_t>{source_item, sink_item};

  a.set_state(two_stage::st_00);

  SECTION("launch source then sink, get source then sink") {
    auto fut_a = std::async(std::launch::async, [&]() {
      CHECK(str(a.state()) == "st_00");
      a.port_fill(debug ? "async source (fill)" : "");
      a.port_push(debug ? "async source (push)" : "");
    });

    auto fut_b = std::async(std::launch::async, [&]() {
      a.port_pull(debug ? "async sink (pull)" : "");
      a.port_drain(debug ? "async sink (drain)" : "");
    });
    fut_a.get();
    fut_b.get();
  }

  SECTION("launch source then sink, get sink then source") {
    auto fut_b = std::async(std::launch::async, [&]() {
      a.port_pull(debug ? "async sink (pull)" : "");
      a.port_drain(debug ? "async sink (drain)" : "");
    });

    auto fut_a = std::async(std::launch::async, [&]() {
      a.port_fill(debug ? "async source (fill)" : "");
      a.port_push(debug ? "async source (push)" : "");
    });

    fut_a.get();
    fut_b.get();
  }

  SECTION("launch sink then source, get source then sink") {
    auto fut_a = std::async(std::launch::async, [&]() {
      a.port_fill(debug ? "async source (fill)" : "");
      a.port_push(debug ? "async source (push)" : "");
    });

    auto fut_b = std::async(std::launch::async, [&]() {
      a.port_pull(debug ? "async sink (pull)" : "");
      a.port_drain(debug ? "async sink (drain)" : "");
    });

    fut_b.get();
    fut_a.get();
  }

  SECTION("launch sink then source, get sink then source") {
    auto fut_b = std::async(std::launch::async, [&]() {
      a.port_pull(debug ? "async sink (pull)" : "");
      a.port_drain(debug ? "async sink (drain)" : "");
    });

    auto fut_a = std::async(std::launch::async, [&]() {
      a.port_fill(debug ? "async source (fill)" : "");
      a.port_push(debug ? "async source (push)" : "");
    });

    fut_b.get();
    fut_a.get();
  }

  CHECK(str(a.state()) == "st_00");
};

/**
 * Test of the asynchrous state machine policy, launching both an
 * emulated source client and an emulated sink client as asynchronous tasks. The
 * test runs n iterations of each emulated client.  The test also invokes the
 * tasks in all combinations of orderings of task launch and waiting on futures.
 */
TEST_CASE(
    "AsynchronousPolicy: Asynchronous source and asynchronous sink, n "
    "iterations",
    "[fsm]") {
  [[maybe_unused]] constexpr bool debug = false;

  std::optional<size_t> source_item{0};
  std::optional<size_t> sink_item{0};
  [[maybe_unused]] auto a = AsyncMover2<size_t>{source_item, sink_item};

  if (debug)
    a.enable_debug();

  a.set_state(two_stage::st_00);

  size_t rounds = 37;
  if (debug)
    rounds = 3;

  auto source_node = [&]() {
    size_t n = rounds;

    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }
      a.port_fill(debug ? "async source node" : "");
      a.port_push(debug ? "async source node" : "");
    }
  };

  auto sink_node = [&]() {
    size_t n = rounds;
    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }
      a.port_pull(debug ? "async sink node" : "");
      a.port_drain(debug ? "async sink node" : "");
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

  CHECK(str(a.state()) == "st_00");
  CHECK((a.source_swaps() + a.sink_swaps()) == rounds);
};

/**
 * Test of the unified asynchrous state machine policy, launching both an
 * emulated source client and an emulated sink client as asynchronous tasks. The
 * test runs n iterations of each emulated client.  The test also invokes the
 * tasks in all combinations of orderings of task launch and waiting on futures.
 */
TEST_CASE(
    "UnifiedAsynchronousPolicy: Asynchronous source and asynchronous "
    "sink, n iterations",
    "[fsm]") {
  [[maybe_unused]] constexpr bool debug = false;

  std::optional<size_t> source_item{0};
  std::optional<size_t> sink_item{0};
  [[maybe_unused]] auto a = UnifiedAsyncMover2<size_t>{source_item, sink_item};

  a.set_state(two_stage::st_00);

  size_t rounds = 37;
  if (debug)
    rounds = 3;

  auto source_node = [&]() {
    size_t n = rounds;

    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }
      /* Emulate running a producer task. */
      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));
      a.port_fill(debug ? "async source node" : "");
      a.port_push(debug ? "async source node" : "");
    }
  };

  auto sink_node = [&]() {
    size_t n = rounds;
    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }
      a.port_pull(debug ? "async sink node" : "");
      /* Emulate running a consumer task. */
      a.port_drain(debug ? "async sink node" : "");
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
  CHECK(str(a.state()) == "st_00");
};

/**
 * Repeat of above test, but without sleeping for emulated tasks.
 */
TEST_CASE(
    "UnifiedAsynchronousPolicy: Asynchronous source and asynchronous "
    "sink, n iterations, no sleeping",
    "[fsm]") {
  [[maybe_unused]] constexpr bool debug = false;

  std::optional<size_t> source_item{0};
  std::optional<size_t> sink_item{0};
  [[maybe_unused]] auto a = AsyncMover2<size_t>{source_item, sink_item};

  a.set_state(two_stage::st_00);

  size_t rounds = 37;
  if (debug)
    rounds = 3;

  auto source_node = [&]() {
    size_t n = rounds;

    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }
      a.port_fill(debug ? "async source node" : "");
      a.port_push(debug ? "async source node" : "");
    }
  };

  auto sink_node = [&]() {
    size_t n = rounds;
    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }
      a.port_pull(debug ? "async sink node" : "");
      a.port_drain(debug ? "async sink node" : "");
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
  CHECK(str(a.state()) == "st_00");
  CHECK(a.source_swaps() + a.sink_swaps() == rounds);
};

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
TEMPLATE_TEST_CASE(
    "Pass a sequence of n integers, async", "[fsm]", AsyncMover2<size_t>) {
  [[maybe_unused]] constexpr bool debug = false;

  std::optional<size_t> source_item{0};
  std::optional<size_t> sink_item{0};
  [[maybe_unused]] auto a = TestType{source_item, sink_item};
  if (debug) {
    a.enable_debug();
  }

  a.set_state(two_stage::st_00);

  size_t rounds = 3379;
  if (debug)
    rounds = 33;

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

      CHECK(is_source_empty(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      CHECK(is_source_empty(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      *(a.source_item()) = *i++;

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      CHECK(is_source_empty(a.state()) == "");

      a.port_fill(debug ? "async source node" : "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      a.port_push(debug ? "async source node" : "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      *(a.source_item()) = EMPTY_SOURCE;

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
      // while (a.state() == two_stage::st_11 ||
      //             a.state() == two_stage::st_01)
      //        ;

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      a.port_pull(debug ? "async sink node" : "");

      CHECK(is_sink_full(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      CHECK(is_sink_full(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      *j++ = *(a.sink_item());

      CHECK(is_sink_full(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      *(a.sink_item()) = EMPTY_SINK;

      a.port_drain(debug ? "async sink node" : "");

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
  CHECK(str(a.state()) == "st_00");
  CHECK((a.source_swaps() + a.sink_swaps()) == rounds);

  //  std::cout << "source " << a.source_swaps() << " sink " << a.sink_swaps()
  //            << std::endl;
}

/**
 * Repeat the previous test, but with the unified async state machine.  To test
 * rapid execution and interleaving of events, we do not include the delays
 * between steps.
 */
TEST_CASE("Pass a sequence of n integers, unified", "[fsm]") {
  [[maybe_unused]] constexpr bool debug = false;

  std::optional<size_t> source_item{0};
  std::optional<size_t> sink_item{0};
  [[maybe_unused]] auto a = UnifiedAsyncMover2<size_t>{source_item, sink_item};

  a.set_state(two_stage::st_00);

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
      // while (a.state() == two_stage::st_10 ||
      // a.state() == two_stage::st_11)
      // ;

      CHECK(is_source_empty(a.state()) == "");

      *(a.source_item()) = *i++;
      a.port_fill(debug ? "async source node" : "");
      a.port_push(debug ? "async source node" : "");

      CHECK(is_source_empty(a.state()) == "");

      *(a.source_item()) = EMPTY_SOURCE;
    }
  };

  auto sink_node = [&]() {
    size_t n = rounds;
    while (n--) {
      if (debug) {
        std::cout << "sink node iteration " << n << std::endl;
      }

      // while (a.state() == two_stage::st_11 ||
      //             a.state() == two_stage::st_01)
      //        ;

      a.port_pull(debug ? "async sink node" : "");

      CHECK(is_sink_full(a.state()) == "");

      *j++ = *(a.sink_item());
      *(a.sink_item()) = EMPTY_SINK;

      a.port_drain(debug ? "async sink node" : "");
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
  CHECK(str(a.state()) == "st_00");
}

TEST_CASE("Port FSM3: Construct", "[fsm]") {
  [[maybe_unused]] auto a = DebugStateMachine3<size_t>{};

  CHECK(a.state() == three_stage::st_000);
}

TEST_CASE("Port FSM3: Start up", "[fsm]") {
  constexpr bool debug = false;
  [[maybe_unused]] auto a = DebugStateMachine3<size_t>{};

  if (debug)
    a.enable_debug();
  CHECK(a.state() == three_stage::st_000);

  SECTION("start source") {
    CHECK(a.state() == three_stage::st_000);

    a.port_fill(debug ? "start source" : "");
    CHECK(a.state() == three_stage::st_100);
  }

  SECTION("start sink") {
    CHECK(a.state() == three_stage::st_000);

    a.port_fill(debug ? "start sink (fill)" : "");

    CHECK(str(a.state()) == "st_100");

    a.port_push(debug ? "start sink (push)" : "");
    CHECK(str(a.state()) == "st_001");
    CHECK(is_source_empty(a.state()) == "");

    a.port_drain(debug ? "start sink (drain)" : "");

    CHECK(str(a.state()) == "st_000");
    CHECK(is_sink_empty(a.state()) == "");
  }
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
TEST_CASE("Pass a sequence of n integers, async 3", "[fsm]") {
  [[maybe_unused]] constexpr bool debug = false;

  std::optional<size_t> source_item{0};
  std::optional<size_t> sink_item{0};
  std::optional<size_t> edge_item{0};
  [[maybe_unused]] auto a =
      AsyncMover3<size_t>{source_item, edge_item, sink_item};
  if (debug) {
    a.enable_debug();
  }

  a.set_state(three_stage::st_000);

  size_t rounds = 3379;
  if (debug)
    rounds = 333;

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

      CHECK(is_source_empty(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      CHECK(is_source_empty(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      *(a.source_item()) = *i++;

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      CHECK(is_source_empty(a.state()) == "");

      a.port_fill(debug ? "async source node" : "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      a.port_push(debug ? "async source node" : "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      *(a.source_item()) = EMPTY_SOURCE;

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

      a.port_pull(debug ? "async sink node" : "");

      CHECK(is_sink_full(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      CHECK(is_sink_full(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      *j++ = *(a.sink_item());

      CHECK(is_sink_full(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      *(a.sink_item()) = EMPTY_SINK;

      a.port_drain(debug ? "async sink node" : "");

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
  CHECK(str(a.state()) == "st_000");
}

/**
 * Repeat the previous test, but with the unified async state machine.  To test
 * rapid execution and interleaving of events, we do not include the delays
 * between steps.
 */
TEST_CASE("Pass a sequence of n integers, unified3", "[fsm]") {
  [[maybe_unused]] constexpr bool debug = false;

  std::optional<size_t> source_item{0};
  std::optional<size_t> sink_item{0};
  std::optional<size_t> edge_item{0};
  [[maybe_unused]] auto a =
      UnifiedAsyncMover3<size_t>{source_item, edge_item, sink_item};

  a.set_state(three_stage::st_000);

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

      CHECK(is_source_empty(a.state()) == "");

      *(a.source_item()) = *i++;
      a.port_fill(debug ? "async source node" : "");
      a.port_push(debug ? "async source node" : "");

      CHECK(is_source_empty(a.state()) == "");

      *(a.source_item()) = EMPTY_SOURCE;
    }
  };

  auto sink_node = [&]() {
    size_t n = rounds;
    while (n--) {
      if (debug) {
        std::cout << "sink node iteration " << n << std::endl;
      }

      a.port_pull(debug ? "async sink node" : "");

      CHECK(is_sink_full(a.state()) == "");

      *j++ = *(a.sink_item());
      *(a.sink_item()) = EMPTY_SINK;

      a.port_drain(debug ? "async sink node" : "");
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
  CHECK(str(a.state()) == "st_000");
}

TEST_CASE("Create a tuple, async", "[fsm]") {
  [[maybe_unused]] constexpr bool debug = false;

  std::optional<size_t> source_item{0};
  std::optional<size_t> sink_item{0};

  [[maybe_unused]] auto a = AsyncMover2<size_t>{source_item, sink_item};

  if (debug) {
    a.enable_debug();
  }

  a.set_state(two_stage::st_00);

  size_t rounds = 3379;
  if (debug)
    rounds = 33;

  std::vector<size_t> input(rounds * 3);
  std::vector<size_t> output(rounds * 3);

  std::iota(input.begin(), input.end(), 19);
  std::fill(output.begin(), output.end(), 0);
  auto i = input.begin();
  auto j = output.begin();

  CHECK(std::equal(input.begin(), input.end(), output.begin()) == false);

  auto source_node = [&]() {
    size_t n = rounds * 3;

    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }

      CHECK(is_source_empty(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      CHECK(is_source_empty(a.state()) == "");

      *(a.source_item()) = *i++;

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      CHECK(is_source_empty(a.state()) == "");

      a.port_fill();

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      a.port_push();

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      *(a.source_item()) = EMPTY_SOURCE;

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));
    }
  };

  auto sink_node = [&]() {
    size_t n = rounds;
    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }

      a.port_pull();

      CHECK(is_sink_full(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      CHECK(is_sink_full(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      *j++ = *(a.sink_item());  // This needs to be atomic with extract

      a.port_drain();

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      a.port_pull();

      CHECK(is_sink_full(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      CHECK(is_sink_full(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      *j++ = *(a.sink_item());  // This needs to be atomic with extract

      a.port_drain();

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));
      a.port_pull();

      CHECK(is_sink_full(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      CHECK(is_sink_full(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      *j++ = *(a.sink_item());  // This needs to be atomic with extract

      a.port_drain();

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
  CHECK(str(a.state()) == "st_00");
  CHECK((a.source_swaps() + a.sink_swaps()) == 3 * rounds);
}

TEST_CASE("Create a tuple2, async", "[fsm]") {
  [[maybe_unused]] constexpr bool debug = false;

  std::optional<size_t> source_item{0};
  std::optional<size_t> sink_item{0};

  [[maybe_unused]] auto a = AsyncMover2<size_t>{source_item, sink_item};

  if (debug) {
    a.enable_debug();
  }

  a.set_state(two_stage::st_00);

  size_t rounds = 3379;
  if (debug)
    rounds = 33;

  std::vector<size_t> input(rounds * 3);
  std::vector<size_t> output(rounds * 3);

  std::iota(input.begin(), input.end(), 19);
  std::fill(output.begin(), output.end(), 0);
  auto i = input.begin();
  auto j = output.begin();

  CHECK(std::equal(input.begin(), input.end(), output.begin()) == false);

  auto source_node = [&]() {
    size_t n = rounds * 3;

    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }

      CHECK(is_source_empty(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      CHECK(is_source_empty(a.state()) == "");

      *(a.source_item()) = *i++;

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      CHECK(is_source_empty(a.state()) == "");

      a.port_fill();

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      a.port_push();

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      *(a.source_item()) = EMPTY_SOURCE;

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));
    }
  };

  auto sink_node = [&]() {
    size_t n = rounds;
    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }

      a.port_pull();

      CHECK(is_sink_full(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      CHECK(is_sink_full(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      *j++ = *(a.sink_item());  // This needs to be atomic with extract

      a.port_drain();

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      a.port_pull();

      CHECK(is_sink_full(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      CHECK(is_sink_full(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      *j++ = *(a.sink_item());  // This needs to be atomic with extract

      a.port_drain();

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));
      a.port_pull();

      CHECK(is_sink_full(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      CHECK(is_sink_full(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      *j++ = *(a.sink_item());  // This needs to be atomic with extract

      a.port_drain();

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
  CHECK(str(a.state()) == "st_00");
  CHECK((a.source_swaps() + a.sink_swaps()) == 3 * rounds);
}
