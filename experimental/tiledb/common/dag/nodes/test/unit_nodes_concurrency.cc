/**
 * @file unit_nodes_concurrency.cc
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
 *  Test that nodes will actually compute concurrently.  We run two classes of
 * test cases -- one with two nodes (a source and a sink) and one with three
 * nodes (a source, a function node, and a sink).  In each test, one of the
 * nodes is synchronous, while the others are asynchronous (running as a task
 * under `std::async`).  Each node simply executes a delay and records the start
 * time and stop time of its execution.  For each such configuration, we vary
 * the execution time of each node.
 *
 * For each test, we verify that the total runtime is less than 1.2 times the
 * max delay given to any of the nodes.
 *
 * There is also a global debug flag. It it is set to `true`, the program will
 * print a table of diagnostic information showing when each node started and
 * stopped.
 */

#include "unit_nodes_concurrency.h"
#include <atomic>
#include <chrono>
#include <future>
#include <string>
#include <vector>
#include "experimental/tiledb/common/dag/nodes/nodes.h"
#include "experimental/tiledb/common/dag/state_machine/fsm.h"
#include "experimental/tiledb/common/dag/state_machine/item_mover.h"
#include "experimental/tiledb/common/dag/state_machine/policies.h"
#include "experimental/tiledb/common/dag/state_machine/test/helpers.h"
#include "experimental/tiledb/common/dag/utility/print_types.h"

#include "experimental/tiledb/common/dag/edge/edge.h"

using namespace tiledb::common;

template <class T>
using AsyncMover3 = ItemMover<AsyncPolicy, three_stage, T>;

template <class T>
using AsyncMover2 = ItemMover<AsyncPolicy, two_stage, T>;

template <class PortState>
void simple_graph() {
  // Define graph node types.  This is not scalable, but it seems difficult to
  // replicate a partial type alias inside of a function (which we need to do
  // here since the Mover depends on the functions PortState template parameter.
  // Will have to see if this makes trouble in practice.  Otherwise we can
  // refactor the Source/Sink/Node interface to just take normal parameters,
  // e.g.,
  //
  // template <class Mover, class Block = Mover::block_type>
  // class Source;
  //
  // In that case, we would write things as Source<Mover<Block>> rather than
  // Source<Mover, Block> (or maybe Source<Mover<Block>, Block> if the compiler
  // is not happy with Source<Mover<Block>>).

  using Producer = std::conditional_t<
      std::is_same_v<PortState, two_stage>,
      ProducerNode<AsyncMover2, size_t>,
      ProducerNode<AsyncMover3, size_t>>;

  using Consumer = std::conditional_t<
      std::is_same_v<PortState, two_stage>,
      ConsumerNode<AsyncMover2, size_t>,
      ConsumerNode<AsyncMover3, size_t>>;

  using Function = std::conditional_t<
      std::is_same_v<PortState, two_stage>,
      FunctionNode<AsyncMover2, size_t>,
      FunctionNode<AsyncMover3, size_t>>;

  bool debug = true;

  auto async_sync = [&](size_t source_delay,
                        size_t sink_delay,
                        size_t fun_delay) {
    using time_t = std::chrono::time_point<std::chrono::high_resolution_clock>;
    time_t start_time = std::chrono::high_resolution_clock::now();

    std::vector<std::tuple<size_t, std::string, size_t, double>> timestamps(60);
    std::atomic<size_t> time_index{0};

    std::atomic<size_t> i{0};
    size_t num_nodes{0};

    Producer /*Node<Mover, size_t> */
        q([&]() {
          timestamps[time_index++] = {
              time_index,
              "start",
              0,
              std::chrono::duration_cast<std::chrono::milliseconds>(
                  (time_t)std::chrono::high_resolution_clock::now() -
                  start_time)
                  .count()};

          std::this_thread::sleep_for(
              std::chrono::milliseconds(static_cast<size_t>(source_delay)));

          timestamps[time_index++] = {
              time_index,
              "stop",
              0,
              std::chrono::duration_cast<std::chrono::milliseconds>(
                  (time_t)std::chrono::high_resolution_clock::now() -
                  start_time)
                  .count()};

          return i++;
        });

    Consumer /*Node<Mover, size_t>*/ r([&](size_t) {
      timestamps[time_index++] = {
          time_index,
          "start",
          1,
          std::chrono::duration_cast<std::chrono::milliseconds>(
              (time_t)std::chrono::high_resolution_clock::now() - start_time)
              .count()};

      std::this_thread::sleep_for(
          std::chrono::milliseconds(static_cast<size_t>(sink_delay)));

      timestamps[time_index++] = {
          time_index,
          "stop",
          1,
          std::chrono::duration_cast<std::chrono::milliseconds>(
              (time_t)std::chrono::high_resolution_clock::now() - start_time)
              .count()};
    });

    Function /*Node<Mover, size_t>*/ t([&](size_t) {
      timestamps[time_index++] = {
          time_index,
          "start",
          2,
          std::chrono::duration_cast<std::chrono::milliseconds>(
              (time_t)std::chrono::high_resolution_clock::now() - start_time)
              .count()};

      std::this_thread::sleep_for(
          std::chrono::milliseconds(static_cast<size_t>(fun_delay)));

      timestamps[time_index++] = {
          time_index,
          "stop",
          2,
          std::chrono::duration_cast<std::chrono::milliseconds>(
              (time_t)std::chrono::high_resolution_clock::now() - start_time)
              .count()};
      return 0UL;
    });

    size_t rounds = 5;

    auto fun_q = [&]() {
      size_t N = rounds;
      while (N--) {
        q.resume();
      }
    };

    auto fun_r = [&]() {
      size_t N = rounds;
      while (N--) {
        r.resume();
      }
    };

    auto fun_t = [&]() {
      size_t N = rounds;
      while (N--) {
        t.resume();
      }
    };

    SECTION("Async node q, sync node r") {
      if (debug)
        std::cout << "Async node q, sync node r" << std::endl;

      num_nodes = 2;

      Edge(q, r);

      auto fut_q = std::async(std::launch::async, fun_q);

      r.resume();
      r.resume();
      r.resume();
      r.resume();
      r.resume();

      fut_q.get();
    }

    SECTION("Sync node q, async node r") {
      if (debug)
        std::cout << "Sync node q, async node r" << std::endl;

      num_nodes = 2;
      Edge(q, r);

      auto fut_r = std::async(std::launch::async, fun_r);

      q.resume();
      q.resume();
      q.resume();
      q.resume();
      q.resume();

      fut_r.get();
    }

    SECTION("Async node q, async node t, sync node r") {
      if (debug)
        std::cout << "Async node q, async node t, sync node r" << std::endl;

      num_nodes = 3;

      Edge(q, t);
      Edge(t, r);

      auto fut_q = std::async(std::launch::async, fun_q);
      auto fut_t = std::async(std::launch::async, fun_t);

      r.resume();
      r.resume();
      r.resume();
      r.resume();
      r.resume();

      fut_q.get();
      fut_t.get();
    }

    SECTION("Sync node q, async node, async node r") {
      if (debug)
        std::cout << "Sync node q, async node t,async node r" << std::endl;

      num_nodes = 3;

      Edge(q, t);
      Edge(t, r);

      auto fut_r = std::async(std::launch::async, fun_r);
      auto fut_t = std::async(std::launch::async, fun_t);

      q.resume();
      q.resume();
      q.resume();
      q.resume();
      q.resume();

      fut_t.get();
      fut_r.get();
    }

    auto&& [a, b, c, d] = timestamps.back();

    if (num_nodes == 2) {
      CHECK(time_index == 4 * rounds);
      CHECK(d < 1.2 * rounds * std::max(source_delay, sink_delay));
    } else if (num_nodes == 3) {
      CHECK(time_index == 6 * rounds);
      CHECK(
          d < 1.2 * rounds *
                  std::max(source_delay, std::max(sink_delay, fun_delay)));
    } else {
      CHECK(false);
    }

    timestamps.resize(time_index);
    if (debug) {
      if (num_nodes == 2) {
        for (auto&& [i, j, k, l] : timestamps) {
          if (k == 0) {
            std::cout << i << "\t" << k << "\t" << l << "\t" << j << std::endl;
          } else if (k == 1) {
            std::cout << i << "\t" << k << "\t" << l << "\t\t" << j
                      << std::endl;
          }
        }
      } else if (num_nodes == 3) {
        for (auto&& [i, j, k, l] : timestamps) {
          if (k == 0) {
            std::cout << i << "\t" << k << "\t" << l << "\t" << j << std::endl;
          } else if (k == 1) {
            std::cout << i << "\t" << k << "\t" << l << "\t\t\t" << j
                      << std::endl;
          } else if (k == 2) {
            std::cout << i << "\t" << k << "\t" << l << "\t\t" << j
                      << std::endl;
          }
        }

      } else {
        CHECK(false);
      }
    }
  };

  SECTION("250 500 750") {
    if (debug)
      std::cout << "250 500 750" << std::endl;
    async_sync(250, 500, 750);
  }

  if (debug)
    std::cout << std::endl;

  SECTION("500 250 750") {
    if (debug)
      std::cout << "500 250 750" << std::endl;
    async_sync(500, 250, 750);
  }

  if (debug)
    std::cout << std::endl;

  SECTION("250 500 100") {
    if (debug)
      std::cout << "250 500 100" << std::endl;
    async_sync(250, 500, 100);
  }

  if (debug)
    std::cout << std::endl;

  SECTION("500 250 100") {
    if (debug)
      std::cout << "500 250 100" << std::endl;
    async_sync(500, 250, 100);
  }
  SECTION("100 100 100") {
    if (debug)
      std::cout << "100 100 100" << std::endl;
    async_sync(100, 100, 100);
  }
}

TEST_CASE(
    "Concurrency: Test level of concurrency for simple two-stage graph",
    "[ports]") {
  simple_graph<two_stage>();
}

TEST_CASE(
    "Concurrency: Test level of concurrency for simple three-stage graph",
    "[ports]") {
  simple_graph<three_stage>();
}
