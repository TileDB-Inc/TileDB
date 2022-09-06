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
 * Tests emulated generalized function nodes with different emulated schedulers.
 *
 */

#include "experimental/tiledb/common/dag/state_machine/test/unit_sched.h"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <future>
#include <iostream>
#include <mutex>
#include <numeric>
#include <queue>
#include <set>
#include <string>
#include <thread>
#include <tuple>
#include <vector>
#include "experimental/tiledb/common/dag/execution/threadpool.h"
#include "experimental/tiledb/common/dag/state_machine/fsm.h"
#include "experimental/tiledb/common/dag/state_machine/policies.h"
#include "experimental/tiledb/common/dag/state_machine/test/helpers.h"
#include "experimental/tiledb/common/dag/state_machine/test/types.h"
#include "experimental/tiledb/common/dag/utils/print_types.h"
#include "unit_fsm.h"

using namespace tiledb::common;

template <class T, size_t N = 3UL>
struct triple_maker_state {
  size_t counter = 0;
  T t0;
  T t1;
};

TEST_CASE("Tuple maker: Test abundant pseudo-scheduler with fsm", "[sched]") {
  [[maybe_unused]] constexpr bool debug = false;

  using state_type = triple_maker_state<size_t, 3UL>;

  std::optional<size_t> source_item{0};
  std::optional<size_t> mid_item_in{0};
  std::optional<std::tuple<size_t, size_t, size_t>> mid_item_out;
  std::optional<std::tuple<size_t, size_t, size_t>> sink_item;

  [[maybe_unused]] auto a = AsyncMover2<size_t>{source_item, mid_item_in};
  [[maybe_unused]] auto b =
      AsyncMover2<std::tuple<size_t, size_t, size_t>>{mid_item_out, sink_item};

  if (debug) {
    a.enable_debug();
    b.enable_debug();
  }

  a.set_state(two_stage::st_00);
  b.set_state(two_stage::st_00);

  size_t rounds = 337;
  if (debug)
    rounds = 33;

  std::vector<size_t> input(rounds * 3);
  std::vector<size_t> midput_in(rounds * 3);
  std::vector<std::tuple<size_t, size_t, size_t>> midput_out;
  std::vector<std::tuple<size_t, size_t, size_t>> output;

  std::iota(input.begin(), input.end(), 19);
  std::fill(midput_in.begin(), midput_in.end(), 0);
  auto i = input.begin();
  auto j_in = midput_in.begin();
  //  auto j_out = midput_out.begin();
  //  auto k = output.begin();

  CHECK(std::equal(input.begin(), input.end(), midput_in.begin()) == false);

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

      a.do_fill();

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      a.do_push();

      CHECK(is_source_empty(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      *(a.source_item()) = EMPTY_SOURCE;

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));
    }
  };

  auto mid_node = [&]() {
    state_type state{};

    size_t n = rounds;
    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }

      a.do_pull();

      CHECK(is_sink_full(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      CHECK(is_sink_full(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      state.t0 = *(a.sink_item());
      *j_in++ = state.t0;

      a.do_drain();

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      a.do_pull();

      CHECK(is_sink_full(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      CHECK(is_sink_full(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      state.t1 = *(a.sink_item());
      *j_in++ = state.t1;

      a.do_drain();

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));
      a.do_pull();

      CHECK(is_sink_full(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      CHECK(is_sink_full(a.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      auto item = *(a.sink_item());
      *j_in++ = item;

      a.do_drain();

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      auto t = std::make_tuple(state.t0, state.t1, item);

      *(b.sink_item()) = t;
      midput_out.emplace_back(t);

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));
      CHECK(is_source_empty(b.state()) == "");
      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      b.do_fill();

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      b.do_push();

      CHECK(is_source_empty(b.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));
    }
  };

  auto sink_node = [&]() {
    size_t n = rounds;
    while (n--) {
      if (debug) {
        std::cout << "source node iteration " << n << std::endl;
      }

      b.do_pull();

      CHECK(is_sink_full(b.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      CHECK(is_sink_full(b.state()) == "");

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

      output.emplace_back(*(b.sink_item()));

      b.do_drain();

      std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));
    }
  };

  SECTION("launch source before sink, get source before sink") {
    auto fut_a = std::async(std::launch::async, source_node);
    auto fut_b = std::async(std::launch::async, mid_node);
    auto fut_c = std::async(std::launch::async, sink_node);

    fut_a.get();
    fut_b.get();
    fut_c.get();
  }

  SECTION("launch sink before source, get source before sink") {
    auto fut_c = std::async(std::launch::async, sink_node);
    auto fut_b = std::async(std::launch::async, mid_node);
    auto fut_a = std::async(std::launch::async, source_node);

    fut_a.get();
    fut_b.get();
    fut_c.get();
  }

  SECTION("launch source before sink, get sink before source") {
    auto fut_a = std::async(std::launch::async, source_node);
    auto fut_b = std::async(std::launch::async, mid_node);
    auto fut_c = std::async(std::launch::async, sink_node);
    fut_c.get();
    fut_b.get();
    fut_a.get();
  }

  SECTION("launch sink before source, get sink before source") {
    auto fut_c = std::async(std::launch::async, sink_node);
    auto fut_b = std::async(std::launch::async, mid_node);
    auto fut_a = std::async(std::launch::async, source_node);

    fut_c.get();
    fut_b.get();
    fut_a.get();
  }

  if (!std::equal(input.begin(), input.end(), midput_in.begin())) {
    for (size_t j = 0; j < input.size(); ++j) {
      if (input[j] != midput_in[j]) {
        std::cout << j << " (" << input[j] << ", " << midput_in[j] << ")"
                  << std::endl;
      }
    }
  }
  if (!std::equal(input.begin(), input.end(), midput_in.begin())) {
    auto iter = std::find_first_of(
        input.begin(),
        input.end(),
        midput_in.begin(),
        midput_in.end(),
        std::not_equal_to<size_t>());
    if (iter != input.end()) {
      size_t k = iter - input.begin();
      std::cout << k << " (" << input[k] << ", " << midput_in[k] << ")"
                << std::endl;
    } else {
      std::cout << "this should not happen" << std::endl;
    }
  }

  CHECK(midput_out.size() == rounds);
  CHECK(output.size() == rounds);
  CHECK(std::equal(input.begin(), input.end(), midput_in.begin()));
  CHECK(std::equal(midput_out.begin(), midput_out.end(), output.begin()));
  CHECK(str(a.state()) == "st_00");
  CHECK(str(b.state()) == "st_00");
  CHECK((a.source_swaps() + a.sink_swaps()) == 3 * rounds);
  CHECK((b.source_swaps() + b.sink_swaps()) == rounds);
}

enum class state {
  init,
  top,
  middle,
  bottom,
  alt_top,
  alt_middle,
  alt_bottom,
  exit
};

std::string strings[] = {"init",
                         "top",
                         "middle",
                         "bottom",
                         "alt_top",
                         "alt_middle",
                         "alt_bottom",
                         "exit"};

std::string str(state st) {
  return strings[static_cast<size_t>(st)];
}

std::string str(size_t st) {
  return strings[st];
}

template <class T>
struct alt_triple_maker_state {
  state counter = state::init;
  T t0{0};
  T t1{0};
  size_t n{0};
};

template <class T>
struct alt_single_maker_state {
  state counter = state::init;
  size_t n{0};
};

class node_hook {
 public:
  virtual node_hook* resume() = 0;
  virtual state get_state() const = 0;
  virtual std::string get_id() const = 0;
};

template <class State>
class node : public node_hook {
  State alt_state_;
  std::string id_;

 public:
  std::function<enum state(State&)> f_;

 public:
  template <class Function>
  node(Function&& f, const std::string& id = "")
      : id_{id}
      , f_{std::forward<Function>(f)} {
  }

  node_hook* resume() {
    f_(this->alt_state_);
    return this;
  }

  state get_state() const {
    return alt_state_.counter;
  }

  std::string get_id() const {
    return id_;
  }
};

template <class T>
class triple_maker_node : public node<alt_triple_maker_state<T>> {
  using Base = node<alt_triple_maker_state<T>>;
  using Base::Base;
};

template <class T>
class single_maker_node : public node<alt_single_maker_state<T>> {
  using Base = node<alt_single_maker_state<T>>;
  using Base::Base;
};

std::queue<node_hook*> runnable_queue;
std::set<node_hook*> running_queue;

void do_run() {
  static std::mutex mutex_;

  std::cout << "do_run starting " << std::endl;

  std::unique_lock lock(mutex_);

  auto id = std::this_thread::get_id();

  // Somehow a node is in the running queue without actually running

  while (!running_queue.empty() || !runnable_queue.empty()) {
    std::cout << id << " do run running "
              << " runnable queue.size() " << runnable_queue.size()
              << " running queue size " << running_queue.size() << std::endl;

    node_hook* n{nullptr};

    if (!runnable_queue.empty()) {
      n = runnable_queue.front();
      runnable_queue.pop();
      running_queue.emplace(n);

      std::cout << id << " about to run " << n->get_id() << std::endl;

      lock.unlock();
      n = n->resume();
      lock.lock();

      std::cout << id << " do run return " << n->get_id() << " from resume "
                << str(n->get_state()) << " runnable queue size "
                << runnable_queue.size() << " running queue size "
                << running_queue.size() << std::endl;

      if (running_queue.erase(n) == 0) {
        throw("element not found in running queue");
      }

      std::cout << id << " running dequeued " << n->get_id() << " with "
                << str(n->get_state()) << " running queue size "
                << running_queue.size() << " runnable queue size "
                << runnable_queue.size() << std::endl;

      if (n->get_state() == state::exit) {
        std::cout << id << " runnable " << n->get_id() << " exiting "
                  << std::endl;

        continue;
      }

      runnable_queue.push(n);
      std::cout << id << " runnable enqueued " << n->get_id() << " with "
                << str(n->get_state()) << " running queue size "
                << running_queue.size() << " runnable queue size "
                << runnable_queue.size() << std::endl;

    } else {
      if (running_queue.erase(n) == 0) {
        throw("element not found in running queue");
      }
      runnable_queue.push(n);
    }
  }
  std::cout << id << " bottom of while: running queue size "
            << running_queue.size() << " runnable queue size "
            << runnable_queue.size() << std::endl;

  std::cout << id << " post while: running queue size " << running_queue.size()
            << " runnable queue size " << runnable_queue.size() << std::endl;
}

TEST_CASE("Tuple maker: Test stingy pseudo-scheduler with fsm", "[sched]") {
  [[maybe_unused]] constexpr bool debug = true;

  std::optional<size_t> source_item{0};
  std::optional<size_t> mid_item_in{0};
  std::optional<std::tuple<size_t, size_t, size_t>> mid_item_out;
  std::optional<std::tuple<size_t, size_t, size_t>> sink_item;

  [[maybe_unused]] auto a = AsyncMover2<size_t>{source_item, mid_item_in};
  [[maybe_unused]] auto b =
      AsyncMover2<std::tuple<size_t, size_t, size_t>>{mid_item_out, sink_item};

  if (debug) {
    a.enable_debug();
    b.enable_debug();
  }

  a.set_state(two_stage::st_00);
  b.set_state(two_stage::st_00);

  size_t rounds = 33;
  if (debug)
    rounds = 3;

  std::vector<size_t> input(rounds * 3);
  std::vector<size_t> midput_in(rounds * 3);
  std::vector<std::tuple<size_t, size_t, size_t>> midput_out;
  std::vector<std::tuple<size_t, size_t, size_t>> output;

  std::iota(input.begin(), input.end(), 19);
  std::fill(midput_in.begin(), midput_in.end(), 0);
  auto i = input.begin();
  auto j_in = midput_in.begin();
  //  auto j_out = midput_out.begin();
  //  auto k = output.begin();

  CHECK(std::equal(input.begin(), input.end(), midput_in.begin()) == false);

  auto source_node_fn = [&](alt_single_maker_state<size_t>& alt_state) {
    if (debug)
      std::cout << "source node iteration " << alt_state.n << std::endl;

    switch (alt_state.counter) {
      case state::init:

        alt_state.n = rounds * 3;

        alt_state.counter = state::top;
        [[fallthrough]];

      case state::top:
        if (debug)
          std::cout << "source node top " << alt_state.n << std::endl;

        CHECK(is_source_empty(a.state()) == "");

        std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

        CHECK(is_source_empty(a.state()) == "");

        *(a.source_item()) = *i++;

        alt_state.counter = state::middle;

        [[fallthrough]];

      case state::middle:
        if (debug)
          std::cout << "source node middle " << alt_state.n << std::endl;

        std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

        CHECK(is_source_empty(a.state()) == "");

        a.do_fill();

        std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

        alt_state.counter = state::bottom;
        [[fallthrough]];

      case state::bottom:
        if (debug)
          std::cout << "source node bottom " << alt_state.n << std::endl;

        a.do_try_push();
        if (!empty_source(a.state())) {
          return alt_state.counter;
        }

        CHECK(is_source_empty(a.state()) == "");

        std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

        *(a.source_item()) = EMPTY_SOURCE;

        std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

        break;

      default:
        if (debug)
          std::cout << "Source node this should not happen!! " +
                           str(alt_state.counter)
                    << std::endl;
        break;
    }
    if (--alt_state.n == 0) {
      alt_state.counter = state::exit;
    } else {
      alt_state.counter = state::top;
    }
    if (debug)
      std::cout << "source node return " << alt_state.n << " "
                << str(alt_state.counter) << std::endl;
    return alt_state.counter;
  };

  auto mid_node_fn = [&](alt_triple_maker_state<size_t>& alt_state) {
    switch (alt_state.counter) {
      if (debug) {
        std::cout << "mid node iteration " << alt_state.n << std::endl;
      }

      case state::init: {
        alt_state.n = rounds;
      }
        alt_state.counter = state::top;
        [[fallthrough]];

      case state::top: {
        if (debug)
          std::cout << "mid node top " << alt_state.n << std::endl;

        a.do_try_pull();
        if (!full_sink(a.state())) {
          return alt_state.counter;
        }

        CHECK(is_sink_full(a.state()) == "");

        std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

        CHECK(is_sink_full(a.state()) == "");

        std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

        alt_state.t0 = *(a.sink_item());
        *j_in++ = alt_state.t0;

        a.do_drain();
      }

        alt_state.counter = state::middle;
        [[fallthrough]];

      case state::middle: {
        if (debug)
          std::cout << "mid node middle " << alt_state.n << std::endl;

        std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

        a.do_try_pull();
        if (!full_sink(a.state())) {
          return alt_state.counter;
        }

        CHECK(is_sink_full(a.state()) == "");

        std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

        CHECK(is_sink_full(a.state()) == "");

        std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

        alt_state.t1 = *(a.sink_item());
        *j_in++ = alt_state.t1;

        a.do_drain();
      }
        alt_state.counter = state::bottom;
        [[fallthrough]];

      case state::bottom: {
        if (debug)
          std::cout << "mid node bottom " << alt_state.n << std::endl;

        std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));
        a.do_try_pull();
        if (!full_sink(a.state())) {
          return alt_state.counter;
        }

        CHECK(is_sink_full(a.state()) == "");

        std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

        CHECK(is_sink_full(a.state()) == "");

        std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));
      }
        alt_state.counter = state::alt_top;
        [[fallthrough]];

      case state::alt_top: {
        if (debug)
          std::cout << "mid node alt top " << alt_state.n << std::endl;
        auto item = *(a.sink_item());
        *j_in++ = item;

        a.do_drain();

        std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

        auto t = std::make_tuple(alt_state.t0, alt_state.t1, item);

        *(b.sink_item()) = t;
        midput_out.emplace_back(t);

        std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));
        CHECK(is_source_empty(b.state()) == "");
        std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));
      }

        alt_state.counter = state::alt_middle;
        [[fallthrough]];

      case state::alt_middle: {
        if (debug)
          std::cout << "mid node alt middle " << alt_state.n << std::endl;

        b.do_fill();

        std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

        alt_state.counter = state::alt_bottom;
        [[fallthrough]];
      }
      case state::alt_bottom: {
        if (debug)
          std::cout << "mid node alt bottom " << alt_state.n << std::endl;

        b.do_try_push();
        if (!empty_source(b.state())) {
          return alt_state.counter;
        }

        CHECK(is_source_empty(b.state()) == "");

        std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));
      } break;

      default:
        if (debug)
          std::cout << "mid node this should not happen!! " +
                           str(alt_state.counter)
                    << std::endl;
        break;
    }
    if (--alt_state.n == 0) {
      alt_state.counter = state::exit;
    } else {
      alt_state.counter = state::top;
    }
    if (debug)
      std::cout << "mid node return " << alt_state.n << " "
                << str(alt_state.counter) << std::endl;
    return alt_state.counter;
  };

  auto sink_node_fn = [&](alt_single_maker_state<size_t>& alt_state) {
    if (debug)
      std::cout << "sink node iteration " << alt_state.n << std::endl;

    switch (alt_state.counter) {
      case state::init:
        alt_state.n = rounds;

        alt_state.counter = state::top;
        [[fallthrough]];

      case state::top:
        if (debug)
          std::cout << "sink node top " << alt_state.n << std::endl;

        b.do_try_pull();
        if (!full_sink(b.state())) {
          if (debug)
            std::cout << "sink node not full " << alt_state.n << std::endl;
          return alt_state.counter;
        }

        CHECK(is_sink_full(b.state()) == "");

        std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

        CHECK(is_sink_full(b.state()) == "");

        std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

        alt_state.counter = state::middle;
        [[fallthrough]];

      case state::middle:
        if (debug)
          std::cout << "sink node middle " << alt_state.n << std::endl;

        output.emplace_back(*(b.sink_item()));

        alt_state.counter = state::bottom;
        [[fallthrough]];

      case state::bottom:
        if (debug)
          std::cout << "sink node bottom " << alt_state.n << std::endl;

        b.do_drain();

        std::this_thread::sleep_for(std::chrono::microseconds(random_us(500)));

        break;

      default:;
        if (debug)
          std::cout << "Sink node this should not happen!! " +
                           str(alt_state.counter)
                    << std::endl;
    }
    if (--alt_state.n == 0) {
      alt_state.counter = state::exit;
    } else {
      alt_state.counter = state::top;
    }

    if (debug)
      std::cout << "sink node return " << alt_state.n << " "
                << str(alt_state.counter) << std::endl;
    return alt_state.counter;
  };

  size_t threads = GENERATE(1, 2, 3, 4, 8, 16);

  SECTION("Threadpool 2 with " + std::to_string(threads)) {
    //    ThreadPool tp(threads);

    auto c = single_maker_node<size_t>{source_node_fn, "source"};
    auto p = triple_maker_node<size_t>{mid_node_fn, "mid"};
    auto q = single_maker_node<size_t>{sink_node_fn, "sink"};

    runnable_queue.push(&c);
    runnable_queue.push(&p);
    runnable_queue.push(&q);

    std::vector<std::future<void>> futs;

    for (size_t t = 0; t < threads; ++t) {
      futs.emplace_back(std::async(std::launch::async, [&]() { do_run(); }));
    }
    CHECK(futs.size() == threads);

    //    auto fut_c = tp.async([&]() { do_run(); });
    //    auto fut_p = tp.async([&]() { do_run(); });
    //    auto fut_q = tp.async([&]() { do_run(); });

    for (size_t t = 0; t < threads; ++t) {
      futs[t].wait();
    }
    CHECK(running_queue.size() == 0);
    CHECK(runnable_queue.empty());
  }

  if (!std::equal(input.begin(), input.end(), midput_in.begin())) {
    for (size_t j = 0; j < input.size(); ++j) {
      if (input[j] != midput_in[j]) {
        std::cout << j << " (" << input[j] << ", " << midput_in[j] << ")"
                  << std::endl;
      }
    }
  }
  if (!std::equal(input.begin(), input.end(), midput_in.begin())) {
    auto iter = std::find_first_of(
        input.begin(),
        input.end(),
        midput_in.begin(),
        midput_in.end(),
        std::not_equal_to<size_t>());
    if (iter != input.end()) {
      size_t k = iter - input.begin();
      std::cout << k << " (" << input[k] << ", " << midput_in[k] << ")"
                << std::endl;
    } else {
      std::cout << "this should not happen" << std::endl;
    }
  }

  CHECK(midput_out.size() == rounds);
  CHECK(output.size() == rounds);
  CHECK(std::equal(input.begin(), input.end(), midput_in.begin()));
  CHECK(std::equal(midput_out.begin(), midput_out.end(), output.begin()));
  CHECK(str(a.state()) == "st_00");
  CHECK(str(b.state()) == "st_00");
  CHECK((a.source_swaps() + a.sink_swaps()) == 3 * rounds);
  CHECK((b.source_swaps() + b.sink_swaps()) == rounds);
}
