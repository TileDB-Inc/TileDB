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
 */

#include "unit_fsm.h"
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <iostream>
#include <mutex>
#include <thread>
#include "experimental/tiledb/common/dag/ports/fsm.h"

using namespace tiledb::common;

TEST_CASE("Port FSM: Construct", "[fsm]") {
  [[maybe_unused]] auto a = PortStateMachine{};

  CHECK(a.state() == PortState::empty_full);
}

TEST_CASE("Port FSM: Start up", "[fsm]") {
  [[maybe_unused]] auto a = PortStateMachine{};

  CHECK(a.state() == PortState::empty_full);

  SECTION("start source") {
    a.event(PortEvent::source_fill);
    CHECK(a.state() == PortState::full_full);
  }

  SECTION("start sink") {
    a.event(PortEvent::sink_drain);
    CHECK(a.state() == PortState::empty_empty);
  }
}

TEST_CASE("Port FSM: Basic manual sequence", "[fsm]") {
  [[maybe_unused]] auto a = PortStateMachine{};

  CHECK(a.state() == PortState::empty_full);

  a.event(PortEvent::source_fill);
  CHECK(str(a.state()) == "full_full");
  a.event(PortEvent::sink_drain);
  CHECK(str(a.state()) == "full_empty");
  a.event(PortEvent::swap);
  CHECK(str(a.state()) == "empty_full");

  a.event(PortEvent::sink_drain);
  CHECK(str(a.state()) == "empty_empty");

  CHECK(str(a.state()) == "empty_empty");
  a.event(PortEvent::source_fill);
  CHECK(str(a.state()) == "full_empty");
  a.event(PortEvent::swap);
  CHECK(a.state() == PortState::empty_full);

  a.event(PortEvent::sink_drain);
  CHECK(a.state() == PortState::empty_empty);

  a.event(PortEvent::source_fill);
  CHECK(str(a.state()) == "full_empty");
  a.event(PortEvent::swap);
  CHECK(str(a.state()) == "empty_full");
  a.event(PortEvent::source_fill);
  CHECK(str(a.state()) == "full_full");
  a.event(PortEvent::sink_drain);
  CHECK(str(a.state()) == "full_empty");
  a.event(PortEvent::swap);
  CHECK(str(a.state()) == "empty_full");

  a.event(PortEvent::sink_drain);
  CHECK(a.state() == PortState::empty_empty);

  a.event(PortEvent::source_fill);
  CHECK(str(a.state()) == "full_empty");
  a.event(PortEvent::swap);
  CHECK(str(a.state()) == "empty_full");
  a.event(PortEvent::source_fill);
  CHECK(str(a.state()) == "full_full");
  a.event(PortEvent::sink_drain);
  CHECK(str(a.state()) == "full_empty");
  a.event(PortEvent::swap);
  CHECK(str(a.state()) == "empty_full");

  a.event(PortEvent::sink_drain);
  CHECK(a.state() == PortState::empty_empty);

  a.event(PortEvent::source_fill);
  CHECK(str(a.state()) == "full_empty");
  a.event(PortEvent::swap);
  CHECK(str(a.state()) == "empty_full");
  a.event(PortEvent::source_fill);
  CHECK(str(a.state()) == "full_full");
  a.event(PortEvent::sink_drain);
  CHECK(str(a.state()) == "full_empty");
  a.event(PortEvent::swap);
  CHECK(str(a.state()) == "empty_full");

  a.event(PortEvent::sink_drain);
  CHECK(a.state() == PortState::empty_empty);

  a.event(PortEvent::source_fill);
  CHECK(str(a.state()) == "full_empty");
  a.event(PortEvent::swap);
  CHECK(str(a.state()) == "empty_full");
  a.event(PortEvent::source_fill);
  CHECK(str(a.state()) == "full_full");
  a.event(PortEvent::sink_drain);
  CHECK(str(a.state()) == "full_empty");
  a.event(PortEvent::swap);
  CHECK(str(a.state()) == "empty_full");

  a.event(PortEvent::sink_drain);
  CHECK(a.state() == PortState::empty_empty);
}

std::string is_src_empty(PortState st) {
  if (str(st) == "empty_full" || str(st) == "empty_empty") {
    return {};
  }
  return str(st);
}

std::string is_src_full(PortState st) {
  if (str(st) == "full_full" || str(st) == "full_empty") {
    return {};
  }
  return str(st);
}

std::string is_src_post_swap(PortState st) {
  if (str(st) == "full_empty" || str(st) == "empty_full" ||
      str(st) == "empty_empty") {
    return {};
  }
  return str(st);
}

std::string is_snk_empty(PortState st) {
  if (str(st) == "full_empty" || str(st) == "empty_empty") {
    return {};
  }
  return str(st);
}

std::string is_snk_full(PortState st) {
  if (str(st) == "full_full" || str(st) == "empty_full") {
    return {};
  }
  return str(st);
}

std::string is_snk_post_swap(PortState st) {
  if (str(st) == "full_empty" || str(st) == "empty_full" ||
      str(st) == "full_full") {
    return {};
  }
  return str(st);
}

size_t random_us(size_t max = 7500) {
  thread_local static uint64_t generator_seed =
      std::hash<std::thread::id>()(std::this_thread::get_id());
  thread_local static std::mt19937_64 generator(generator_seed);
  std::uniform_int_distribution<size_t> distribution(0, max);
  return distribution(generator);
}

TEST_CASE("Port FSM: Asynchronous source and sink", "[fsm]") {
  constexpr bool debug = false;

  [[maybe_unused]] auto a = PortStateMachine{};

  CHECK(str(a.state()) == "empty_full");

  std::mutex mutex_;
  std::condition_variable source_cv, sink_cv;

  size_t rounds = 37;
  if (debug)
    rounds = 3;

  int source_item{0};
  int sink_item{0};

  int source_swaps{};
  int sink_swaps{};

  /**
   * The basic behavior of a user of a source port is described in fsm.h.
   *
   * This test function loops for a given number of rounds, executing the
   * workflow shown in fsm.h. CHECKs are made for each of the predicates
   * therein.
   */
  auto source_node = [&]() {
    size_t n = rounds;

    // Take a really big lock to keep events atomic with checks.
    std::unique_lock lock(mutex_);

    // start: { state == empty_full /\ state == empty_empty }
    CHECK(is_src_empty(a.state()) == "");

    // Event loop for source
    while (n--) {
      // Begin production of new item
      // loop_begin: { state == empty_empty /\ state == empty_full }
      CHECK(is_src_empty(a.state()) == "");
      {
        if (debug)
          std::cout << "source filling " << str(a.state()) << std::endl;

        // pre_produce: { state == empty_empty /\ state == empty_full }
        CHECK(is_src_empty(a.state()) == "");
        lock.unlock();
        std::this_thread::sleep_for(std::chrono::microseconds(random_us(7500)));
        lock.lock();
      }

      CHECK(source_item == 0);
      source_item = 1;

      a.event(PortEvent::source_fill, debug ? "source" : "");

      // source_fill: { state == full_empty /\ state == full_full }
      CHECK(is_src_full(a.state()) == "");
      sink_cv.notify_one();
      source_cv.wait(lock);

      // post_wait: { state == full_empty \/ state == empty_full \/
      //             state == empty_empty }
      CHECK(is_src_post_swap(a.state()) == "");

      if (a.state() == PortState::full_empty) {
        // pre_swap: { state == full_empty }
        CHECK(str(a.state()) == "full_empty");
        if (debug)
          std::cout << "source swapping " << str(a.state()) << std::endl;

        a.event(PortEvent::swap, debug ? "source" : "");
        source_swaps++;

        // post_swap: { state == empty_full }
        CHECK(is_src_empty(a.state()) == "");
        CHECK(is_snk_full(a.state()) == "");

        // { source_item == 1 /\ sink_item == 0 }
        CHECK(source_item == 1);
        CHECK(sink_item == 0);
        std::swap(source_item, sink_item);

        // { source_item == 0 /\ sink_item == 1 }
        CHECK(source_item == 0);
        CHECK(sink_item == 1);

        sink_cv.notify_one();
      } else {
        // else_swap: { state == empty_empty \/ state == empty_full }
        CHECK(is_src_empty(a.state()) == "");

        if (debug)

          std::cout << "source try_swap: "
                    << port_state_strings[static_cast<int>(a.state())]
                    << std::endl;

        // { source_item == 0 }
        CHECK(source_item == 0);
      }
    }

    // post_loop: { state == empty_empty }
    CHECK(is_src_empty(a.state()) == "");

    // { source_item == 0 }
    CHECK(source_item == 0);
  };

  /**
   * The basic behavior of a user of a sink port is described in fsm.h.
   *
   * This test function loops for a given number of rounds, executing the
   * workflow shown in fsm.h. CHECKs are made for each of the predicates
   * therein.
   */
  auto sink_node = [&]() {
    size_t n = rounds;

    // Take a really big lock to keep events atomic with checks.
    std::unique_lock lock(mutex_);

    /**
     * Initialization for sink_node, to emulate it having been filled (necessary
     * to avoid deadlocking).
     */
    // init: { state == empty_full /\ state == full_full }
    CHECK(is_snk_full(a.state()) == "");

    a.event(PortEvent::sink_drain, debug ? "sink" : "");
    CHECK(is_snk_empty(a.state()) == "");
    source_cv.notify_one();

    // Event loop for sink
    while (n--) {
      // loop_begin: { state == empty_empty /\ state == full_empty }
      CHECK(is_snk_empty(a.state()) == "");

      sink_cv.wait(lock);

      // post_wait: { state == full_empty \/ state == empty_full \/
      //              state == full_full }
      CHECK(is_snk_post_swap(a.state()) == "");

      if (debug)
        std::cout << "sink coming out of wait  " << str(a.state()) << std::endl;

      if (a.state() == PortState::full_empty) {
        // pre_swap: { state == full_empty }
        CHECK(str(a.state()) == "full_empty");
        a.event(PortEvent::swap, debug ? "sink" : "");

        // post_swap: { state == full_empty }
        CHECK(is_src_empty(a.state()) == "");
        CHECK(is_snk_full(a.state()) == "");

        // { source_item == 1 /\ sink_item == 0 }
        CHECK(source_item == 1);
        CHECK(sink_item == 0);
        std::swap(source_item, sink_item);
        sink_swaps++;

        // { source_item == 0 /\ sink_item == 1 }
        CHECK(source_item == 0);
        CHECK(sink_item == 1);

        source_cv.notify_one();
      } else {
        // else_swap: { state == empty_full \/ state == full_full }
        CHECK(is_snk_full(a.state()) == "");

        if (debug)
          std::cout << "sink try swap " << str(a.state()) << std::endl;

        // { sink_item == 1 }
        CHECK(sink_item == 1);
      }

      // pre_consume: { state == empty_full /\ state == full_full }
      CHECK(is_snk_full(a.state()) == "");
      {
        if (debug)
          std::cout << "sink retrieving " << str(a.state()) << std::endl;

        lock.unlock();
        std::this_thread::sleep_for(std::chrono::microseconds(random_us(7500)));
        lock.lock();
      }
      a.event(PortEvent::sink_drain, debug ? "sink" : "");

      // drain: { state == empty_empty /\ state == full_empty }
      CHECK(is_snk_empty(a.state()) == "");

      CHECK(sink_item == 1);
      sink_item = 0;
      source_cv.notify_one();
    }
    // post_loop: { state == empty_empty }
    CHECK(str(a.state()) == "empty_empty");

    // { source_item == 0 /\ sink_item == 0 }
    CHECK(source_item == 0);
    CHECK(sink_item == 0);
  };

  /**
   * Test asynchronous execution of source and sink functions.  We use different
   * combinations of orderings for launching the asynchronous tasks, as well as
   * for waiting on their completion.  For now we use std::async.
   */
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

  /** Final check after each section
   *
   * The final state should be empty for both source and sink and both items
   * should be empty.
   *
   */
  // { state == empty_empty }
  CHECK(str(a.state()) == "empty_empty");
  CHECK(is_src_empty(a.state()) == "");
  CHECK(is_snk_empty(a.state()) == "");

  // { source_item == 0 /\ sink_item == 0 }
  CHECK(source_item == 0);
  CHECK(sink_item == 0);

  /**
   * Check that there were an expected number of swaps between the source and
   * the sink.  In debug mode, print out the number of each.  Over multiple
   * runs, we expect the number of each to change.
   */
  if (debug) {
    std::cout << source_swaps << " source_swaps and " << sink_swaps
              << " sink_swaps" << std::endl;
    CHECK((source_swaps + sink_swaps) == 3);
  } else {
    CHECK((source_swaps + sink_swaps) == 37);
  }
}
