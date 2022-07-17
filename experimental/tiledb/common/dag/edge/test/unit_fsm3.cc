/**
 * @file unit_fsm3.cc
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

#include "unit_fsm3.h"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <iostream>
#include <mutex>
#include <numeric>
#include <thread>
#include <vector>
#include "experimental/tiledb/common/dag/edge/fsm3.h"
#include "experimental/tiledb/common/dag/edge/policies3.h"
#include "helpers3.h"

using namespace tiledb::common;

// using PortStateMachine = NullStateMachine;

/*
 * Use the `DebugStateMachine` to verify startup state and some simple
 * transitions.
 */
using PortStateMachine = DebugStateMachine<size_t>;

TEST_CASE("Port FSM3: Construct", "[fsm3]") {
  [[maybe_unused]] auto a = PortStateMachine{};

  CHECK(a.state() == PortState::st_000);
}

TEST_CASE("Port FSM3: Start up", "[fsm3]") {
  constexpr bool debug = false;
  [[maybe_unused]] auto a = PortStateMachine{};

  if (debug)
    a.enable_debug();
  CHECK(a.state() == PortState::st_000);

  SECTION("start source") {
    CHECK(a.state() == PortState::st_000);

    a.do_fill(debug ? "start source" : "");
    CHECK(a.state() == PortState::st_100);
  }

  SECTION("start sink") {
    CHECK(a.state() == PortState::st_000);

    a.do_fill(debug ? "start sink (fill)" : "");

    CHECK(str(a.state()) == "st_100");

    a.do_push(debug ? "start sink (push)" : "");
    CHECK(str(a.state()) == "st_001");
    CHECK(is_source_empty(a.state()) == "");

    a.do_drain(debug ? "start sink (drain)" : "");

    CHECK(str(a.state()) == "st_000");
    CHECK(is_sink_empty(a.state()) == "");
  }
}

/*
 * Use the `DebugStateMachine` to verify startup state and some more involved
 * transition sequences.
 */
TEST_CASE("Port FSM3: Basic manual sequence", "[fsm3]") {
  [[maybe_unused]] auto a = PortStateMachine{};
  CHECK(a.state() == PortState::st_000);

  SECTION("Two element tests") {
    a.do_fill();
    CHECK(str(a.state()) == "st_100");
    a.do_push();
    CHECK(str(a.state()) == "st_001");
    a.do_fill();
    CHECK(str(a.state()) == "st_101");
    a.do_drain();
    CHECK(str(a.state()) == "st_100");
    a.do_push();
    CHECK(str(a.state()) == "st_001");

    a.do_drain();
    CHECK(str(a.state()) == "st_000");

    a.do_fill();
    CHECK(str(a.state()) == "st_100");
    a.do_pull();
    CHECK(str(a.state()) == "st_001");
    a.do_fill();
    CHECK(str(a.state()) == "st_101");
    a.do_drain();
    CHECK(str(a.state()) == "st_100");
    a.do_pull();
    CHECK(str(a.state()) == "st_001");

    a.do_drain();
    CHECK(str(a.state()) == "st_000");

    a.do_fill();
    CHECK(str(a.state()) == "st_100");
    a.do_push();
    CHECK(str(a.state()) == "st_001");
    a.do_fill();
    CHECK(str(a.state()) == "st_101");
    a.do_push();
    CHECK(str(a.state()) == "st_011");
    a.do_drain();
    CHECK(str(a.state()) == "st_010");
    a.do_push();
    CHECK(str(a.state()) == "st_001");

    a.do_drain();
    CHECK(str(a.state()) == "st_000");

    a.do_fill();
    CHECK(str(a.state()) == "st_100");
    a.do_pull();
    CHECK(str(a.state()) == "st_001");
    a.do_fill();
    CHECK(str(a.state()) == "st_101");
    a.do_pull();
    CHECK(str(a.state()) == "st_011");
    a.do_drain();
    CHECK(str(a.state()) == "st_010");
    a.do_pull();
    CHECK(str(a.state()) == "st_001");

    a.do_drain();
    CHECK(str(a.state()) == "st_000");
  }

  SECTION("three element tests") {
    a.do_fill();
    CHECK(str(a.state()) == "st_100");
    a.do_push();
    CHECK(str(a.state()) == "st_001");
    a.do_fill();
    CHECK(str(a.state()) == "st_101");
    a.do_push();
    CHECK(str(a.state()) == "st_011");
    a.do_fill();
    CHECK(str(a.state()) == "st_111");
    a.do_drain();
    CHECK(str(a.state()) == "st_110");
    a.do_push();
    CHECK(str(a.state()) == "st_011");
    a.do_drain();
    CHECK(str(a.state()) == "st_010");
    a.do_push();
    CHECK(str(a.state()) == "st_001");

    a.do_drain();
    CHECK(str(a.state()) == "st_000");

    a.do_fill();
    CHECK(str(a.state()) == "st_100");
    a.do_pull();
    CHECK(str(a.state()) == "st_001");
    a.do_fill();
    CHECK(str(a.state()) == "st_101");
    a.do_pull();
    CHECK(str(a.state()) == "st_011");
    a.do_fill();
    CHECK(str(a.state()) == "st_111");
    a.do_drain();
    CHECK(str(a.state()) == "st_110");
    a.do_pull();
    CHECK(str(a.state()) == "st_011");
    a.do_drain();
    CHECK(str(a.state()) == "st_010");
    a.do_pull();
    CHECK(str(a.state()) == "st_001");

    a.do_drain();
    CHECK(str(a.state()) == "st_000");
  }
}

/**
 * Simple test of asynchrous state machine policy, launching an emulated source
 * client as an asynchronous task and running an emulated sink client in the
 * main thread. The test just runs one pass of each emulated client.
 */
TEST_CASE(
    "AsynchronousStateMachine: Asynchronous source and manual sink", "[fsm3]") {
  [[maybe_unused]] constexpr bool debug = false;

  size_t source_item{0};
  size_t edge_item{0};
  size_t sink_item{0};
  [[maybe_unused]] auto a =
      AsyncStateMachine{source_item, edge_item, sink_item, debug};

  a.set_state(PortState::st_000);

  auto fut_a = std::async(std::launch::async, [&]() {
    a.do_fill(debug ? "async source (fill)" : "");
    CHECK(is_source_full(a.state()) == "");
    a.do_push(debug ? "async source (push)" : "");
    CHECK(is_source_empty(a.state()) == "");
  });

  //  std::this_thread::sleep_for(std::chrono::microseconds(random_us(5000)));

  if (debug)
    std::cout << "About to call drained" << std::endl;

  //  std::this_thread::sleep_for(std::chrono::microseconds(random_us(5000)));

  a.do_pull(debug ? "manual sink (pull)" : "");
  CHECK(str(a.state()) == "st_001");

  a.do_drain(debug ? "manual sink (drain)" : "");

  fut_a.get();

  CHECK(str(a.state()) == "st_000");
};

/**
 * Simple test of asynchrous state machine policy, launching an emulated sink
 * client as an asynchronous task and running an emulated source client in the
 * main thread. The test just runs one pass of each emulated client.
 */
TEST_CASE(
    "AsynchronousStateMachine: Manual source and asynchronous sink", "[fsm3]") {
  [[maybe_unused]] constexpr bool debug = false;

  size_t source_item{0};
  size_t edge_item{0};
  size_t sink_item{0};
  [[maybe_unused]] auto a =
      AsyncStateMachine{source_item, edge_item, sink_item, debug};

  a.set_state(PortState::st_000);

  auto fut_b = std::async(std::launch::async, [&]() {
    a.do_pull(debug ? "async sink (pull)" : "");
    CHECK(is_sink_full(a.state()) == "");

    a.do_drain(debug ? "async sink (drain)" : "");
  });

  //  std::this_thread::sleep_for(std::chrono::microseconds(random_us(5000)));

  if (debug)
    std::cout << "About to call fill_source" << std::endl;

  //  std::this_thread::sleep_for(std::chrono::microseconds(random_us(5000)));

  a.do_fill(debug ? "manual source (fill)" : "");
  a.do_push(debug ? "manual source (push)" : "");

  fut_b.get();

  CHECK(str(a.state()) == "st_000");
};

/**
 * Simple test of the unified asynchrous state machine policy, launching an
 * emulated source client as an asynchronous task and running an emulated sink
 * client in the main thread. The test just runs one pass of each emulated
 * client.
 */
TEST_CASE(
    "UnifiedAsynchronousStateMachine: Asynchronous source and manual sink",
    "[fsm3]") {
  [[maybe_unused]] constexpr bool debug = false;

  size_t source_item{0};
  size_t edge_item{0};
  size_t sink_item{0};
  [[maybe_unused]] auto a =
      UnifiedAsyncStateMachine{source_item, edge_item, sink_item, debug};

  a.set_state(PortState::st_000);

  auto fut_a = std::async(std::launch::async, [&]() {
    a.do_fill(debug ? "manual async source (fill)" : "");
    a.do_push(debug ? "manual async source (push)" : "");
  });

  if (debug)
    std::cout << "About to call drained" << std::endl;

  a.do_pull(debug ? "manual async sink (pull)" : "");
  a.do_drain(debug ? "manual async sink (drained)" : "");

  fut_a.get();

  CHECK(str(a.state()) == "st_000");
};

/**
 * Simple test of the unified asynchrous state machine policy, launching an
 * emulated source client as an asynchronous task and running an emulated sink
 * client in the main thread. The test just runs one pass of each emulated
 * client.
 */
TEST_CASE(
    "UnifiedAsynchronousStateMachine: Manual source and asynchronous sink",
    "[fsm3]") {
  [[maybe_unused]] constexpr bool debug = false;

  size_t source_item{0};
  size_t edge_item{0};
  size_t sink_item{0};
  [[maybe_unused]] auto a =
      UnifiedAsyncStateMachine{source_item, edge_item, sink_item, debug};

  a.set_state(PortState::st_000);

  auto fut_b = std::async(std::launch::async, [&]() {
    a.do_pull(debug ? "manual async sink (pull)" : "");
    a.do_drain(debug ? "manual async sink (drain)" : "");
  });

  //  std::this_thread::sleep_for(std::chrono::microseconds(random_us(5000)));

  if (debug)
    std::cout << "About to call fill_source" << std::endl;

  //  std::this_thread::sleep_for(std::chrono::microseconds(random_us(5000)));

  a.do_fill(debug ? "manual async source (fill)" : "");
  a.do_push(debug ? "manual async source (push)" : "");

  fut_b.get();

  CHECK(str(a.state()) == "st_000");
};

/**
 * Simple test of the asynchrous state machine policy, launching both an
 * emulated source client and an emulated sink client as asynchronous tasks. The
 * test just runs one pass of each emulated client.  The test also invokes the
 * tasks in all combinations of orderings of task launch and waiting on futures.
 */
TEST_CASE(
    "AsynchronousStateMachine: Asynchronous source and asynchronous sink",
    "[fsm3]") {
  [[maybe_unused]] constexpr bool debug = false;

  size_t source_item{0};
  size_t edge_item{0};
  size_t sink_item{0};
  [[maybe_unused]] auto a =
      AsyncStateMachine{source_item, edge_item, sink_item, debug};

  a.set_state(PortState::st_000);

  SECTION("launch source then sink, get source then sink") {
    auto fut_a = std::async(std::launch::async, [&]() {
      a.do_fill(debug ? "async source (fill)" : "");
      a.do_push(debug ? "async source (push)" : "");
    });

    auto fut_b = std::async(std::launch::async, [&]() {
      a.do_pull(debug ? "async sink (pull)" : "");
      a.do_drain(debug ? "async dsink (drain)" : "");
    });
    fut_a.get();
    fut_b.get();
  }

  SECTION("launch source then sink, get sink then source") {
    auto fut_a = std::async(std::launch::async, [&]() {
      a.do_fill(debug ? "async source (fill)" : "");
      a.do_push(debug ? "async source (push)" : "");
    });

    auto fut_b = std::async(std::launch::async, [&]() {
      a.do_pull(debug ? "async sink (pull)" : "");
      a.do_drain(debug ? "async sink (drain)" : "");
    });
    fut_b.get();
    fut_a.get();
  }

  SECTION("launch sink then source, get source then sink") {
    auto fut_b = std::async(std::launch::async, [&]() {
      a.do_pull(debug ? "async sink (pull)" : "");
      a.do_drain(debug ? "async dsink (drain)" : "");
    });
    auto fut_a = std::async(std::launch::async, [&]() {
      a.do_fill(debug ? "async source (fill)" : "");
      a.do_push(debug ? "async source (push)" : "");
    });

    fut_a.get();
    fut_b.get();
  }
  SECTION("launch sink then source, get source then sink") {
    auto fut_b = std::async(std::launch::async, [&]() {
      a.do_pull(debug ? "async sink (pull)" : "");
      a.do_drain(debug ? "async dsink (drain)" : "");
    });
    auto fut_a = std::async(std::launch::async, [&]() {
      a.do_fill(debug ? "async source (fill)" : "");
      a.do_push(debug ? "async source (push)" : "");
    });

    fut_a.get();
    fut_b.get();
  }

  SECTION("launch source then sink, get sink then source") {
    auto fut_a = std::async(std::launch::async, [&]() {
      a.do_fill(debug ? "async source (fill)" : "");
      a.do_push(debug ? "async source (push)" : "");
    });

    auto fut_b = std::async(std::launch::async, [&]() {
      a.do_pull(debug ? "async sink (pull)" : "");
      a.do_drain(debug ? "async sink (drain)" : "");
    });
    fut_b.get();
    fut_a.get();
  }

  CHECK(str(a.state()) == "st_000");
};

/**
 * Simple test of the unified asynchrous state machine policy, launching both an
 * emulated source client and an emulated sink client as asynchronous tasks. The
 * test just runs one pass of each emulated client.  The test also invokes the
 * tasks in all combinations of orderings of task launch and waiting on futures.
 */
TEST_CASE(
    "UnifiedAsynchronousStateMachine: Asynchronous source and asynchronous "
    "sink",
    "[fsm3]") {
  [[maybe_unused]] constexpr bool debug = false;

  size_t source_item{0};
  size_t edge_item{0};
  size_t sink_item{0};
  [[maybe_unused]] auto a =
      UnifiedAsyncStateMachine{source_item, edge_item, sink_item, debug};

  a.set_state(PortState::st_000);

  SECTION("launch source then sink, get source then sink") {
    auto fut_a = std::async(std::launch::async, [&]() {
      CHECK(str(a.state()) == "st_000");
      a.do_fill(debug ? "async source (fill)" : "");
      a.do_push(debug ? "async source (push)" : "");
    });

    auto fut_b = std::async(std::launch::async, [&]() {
      a.do_pull(debug ? "async sink (pull)" : "");
      a.do_drain(debug ? "async sink (drain)" : "");
    });
    fut_a.get();
    fut_b.get();
  }

  SECTION("launch source then sink, get sink then source") {
    auto fut_b = std::async(std::launch::async, [&]() {
      a.do_pull(debug ? "async sink (pull)" : "");
      a.do_drain(debug ? "async sink (drain)" : "");
    });

    auto fut_a = std::async(std::launch::async, [&]() {
      a.do_fill(debug ? "async source (fill)" : "");
      a.do_push(debug ? "async source (push)" : "");
    });

    fut_a.get();
    fut_b.get();
  }

  SECTION("launch sink then source, get source then sink") {
    auto fut_a = std::async(std::launch::async, [&]() {
      a.do_fill(debug ? "async source (fill)" : "");
      a.do_push(debug ? "async source (push)" : "");
    });

    auto fut_b = std::async(std::launch::async, [&]() {
      a.do_pull(debug ? "async sink (pull)" : "");
      a.do_drain(debug ? "async sink (drain)" : "");
    });

    fut_b.get();
    fut_a.get();
  }

  SECTION("launch sink then source, get sink then source") {
    auto fut_b = std::async(std::launch::async, [&]() {
      a.do_pull(debug ? "async sink (pull)" : "");
      a.do_drain(debug ? "async sink (drain)" : "");
    });

    auto fut_a = std::async(std::launch::async, [&]() {
      a.do_fill(debug ? "async source (fill)" : "");
      a.do_push(debug ? "async source (push)" : "");
    });

    fut_b.get();
    fut_a.get();
  }

  CHECK(str(a.state()) == "st_000");
};

/**
 * Test of the asynchrous state machine policy, launching both an
 * emulated source client and an emulated sink client as asynchronous tasks. The
 * test runs n iterations of each emulated client.  The test also invokes the
 * tasks in all combinations of orderings of task launch and waiting on futures.
 */
TEST_CASE(
    "AsynchronousStateMachine: Asynchronous source and asynchronous sink, n "
    "iterations",
    "[fsm3]") {
  [[maybe_unused]] constexpr bool debug = false;

  size_t source_item{0};
  size_t edge_item{0};
  size_t sink_item{0};
  [[maybe_unused]] auto a =
      AsyncStateMachine{source_item, edge_item, sink_item, debug};

  if (debug)
    a.enable_debug();

  a.set_state(PortState::st_000);

  size_t rounds = 377;
  if (debug)
    rounds = 3;

  auto source_node = [&]() {
    size_t n = rounds;

    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }
      a.do_fill(debug ? "async source node" : "");
      a.do_push(debug ? "async source node" : "");
    }
  };

  auto sink_node = [&]() {
    size_t n = rounds;
    while (n--) {
      if (debug) {
        std::cout << "sink node iteration " << n << std::endl;
      }
      a.do_pull(debug ? "async sink node" : "");
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

  CHECK(str(a.state()) == "st_000");

  // What can we say about moves now?
  // CHECK(a.source_moves <= rounds);
  // CHECK(a.sink_moves <= rounds);
  // CHECK((a.source_moves + a.sink_moves) == rounds);
};

/**
 * Test of the unified asynchrous state machine policy, launching both an
 * emulated source client and an emulated sink client as asynchronous tasks. The
 * test runs n iterations of each emulated client.  The test also invokes the
 * tasks in all combinations of orderings of task launch and waiting on futures.
 */
TEST_CASE(
    "UnifiedAsynchronousStateMachine: Asynchronous source and asynchronous "
    "sink, n iterations",
    "[fsm3]") {
  [[maybe_unused]] constexpr bool debug = false;

  size_t source_item{0};
  size_t edge_item{0};
  size_t sink_item{0};
  [[maybe_unused]] auto a =
      UnifiedAsyncStateMachine{source_item, edge_item, sink_item, debug};

  a.set_state(PortState::st_000);

  size_t rounds = 377;
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
      a.do_fill(debug ? "async source node" : "");
      a.do_push(debug ? "async source node" : "");
    }
  };

  auto sink_node = [&]() {
    size_t n = rounds;
    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }
      a.do_pull(debug ? "async sink node" : "");
      /* Emulate running a consumer task. */
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
  CHECK(str(a.state()) == "st_000");

  // What can we say about moves now?
  // CHECK(a.source_moves <= rounds);
  // CHECK(a.sink_moves <= rounds);
  // CHECK((a.source_moves + a.sink_moves) == rounds);
};

/**
 * Repeat of above test, but without sleeping for emulated tasks.
 */
TEST_CASE(
    "UnifiedAsynchronousStateMachine: Asynchronous source and asynchronous "
    "sink, n iterations, no sleeping",
    "[fsm3]") {
  [[maybe_unused]] constexpr bool debug = false;

  size_t source_item{0};
  size_t edge_item{0};
  size_t sink_item{0};
  [[maybe_unused]] auto a =
      AsyncStateMachine{source_item, edge_item, sink_item, debug};

  a.set_state(PortState::st_000);

  size_t rounds = 377;
  if (debug)
    rounds = 3;

  auto source_node = [&]() {
    size_t n = rounds;

    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }
      a.do_fill(debug ? "async source node" : "");
      a.do_push(debug ? "async source node" : "");
    }
  };

  auto sink_node = [&]() {
    size_t n = rounds;
    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }
      a.do_pull(debug ? "async sink node" : "");
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
  CHECK(str(a.state()) == "st_000");
};
