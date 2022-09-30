/**
 * @file   unit_frugal.cc
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
 * This file declares a throw-catch scheduler for dag.
 */

#include "unit_frugal.h"
#include <atomic>
#include <cassert>
#include <iostream>
#include <map>
#include "../frugal.h"
#include "experimental/tiledb/common/dag/edge/edge.h"
#include "experimental/tiledb/common/dag/execution/jthread/stop_token.hpp"
#include "experimental/tiledb/common/dag/execution/scheduler.h"
#include "experimental/tiledb/common/dag/nodes/consumer.h"
#include "experimental/tiledb/common/dag/ports/ports.h"
#include "experimental/tiledb/common/dag/state_machine/test/helpers.h"
#include "experimental/tiledb/common/dag/state_machine/test/types.h"

using namespace tiledb::common;

struct node;

template <class T>
struct port;

template <class T>
struct sender;

template <class T>
struct receiver;

template <class T>
struct port {};

template <class T>
struct sender : public port<T> {};

template <class T>
struct receiver : public port<T> {};

struct node {
  bool debug_{false};

  size_t id_;
  size_t program_counter_{0};

  std::shared_ptr<node> correspondent_{nullptr};

  node(node&&) = default;
  node(const node&) {
    std::cout << "Nonsense copy constructor" << std::endl;
  }

  virtual ~node() = default;

  //  inline TaskState set_node_state(TaskState st) {
  //    node_state_ = st;
  //    return st;
  //  }

  //  inline TaskEvent task_event() const {
  //    return task_event_;
  //  }

  //  inline TaskEvent set_task_event(TaskEvent ev) {
  //    task_event_ = ev;
  //    return task_event_;
  //  }

  inline size_t id() const {
    return id_;
  }

  node(size_t id)
      : id_{id} {
  }

  virtual node* resume() = 0;

  virtual std::string name() {
    return {"abstract base"};
  }

  void enable_debug() {
    debug_ = true;
  }

  void disable_debug() {
    debug_ = false;
  }

  bool debug() {
    return debug_;
  }
};

template <class From, class To>
void connect(From& from, To& to) {
  from->correspondent_ = to;
  to->correspondent_ = from;
}

static size_t problem_size = 7;  // 1337;

std::atomic<size_t> id_counter{0};

std::atomic<bool> item_{false};

template <template <class> class Mover, class T>
struct producer_node_impl : public node,
                            public sender<T>,
                            public Source<Mover, T> {
  using mover_type = Mover<T>;
  void set_item_mover(std::shared_ptr<mover_type> mover) {
    this->item_mover_ = mover;
  }

  std::function<T(std::stop_source&)> f_;

  std::atomic<size_t> produced_items_{0};

  size_t produced_items() {
    return produced_items_.load();
  }

  template <class Function>
  producer_node_impl(Function&& f)
      : node(id_counter++)
      , f_{std::forward<Function>(f)}
      , produced_items_{0} {
  }

  producer_node_impl(producer_node_impl&& rhs) = default;

  void inject(T) {
    if (this->debug())
      std::cout << "producer_node " + std::to_string(id_) + " injecting" + "\n";
  }

  void fill() {
    if (this->debug())
      std::cout << "producer_node " + std::to_string(id_) + " filling" + "\n";
  }

  std::string name() {
    return {"producer"};
  }

  node* resume() {
    auto mover = this->get_mover();

    std::cout << "producer resuming\n";

    if (this->debug())
      std::cout << this->name() + " node " + std::to_string(this->id()) +
                       " resuming with " + std::to_string(produced_items_) +
                       " produced_items" + "\n";

    [[maybe_unused]] std::thread::id this_id = std::this_thread::get_id();

    std::stop_source st;
    decltype(f_(st)) thing;

    std::stop_source stop_source_;

    switch (program_counter_) {
      case 0: {
        program_counter_ = 1;

        if (produced_items_++ >= problem_size) {
          if (this->debug())
            std::cout << this->name() + " node " + std::to_string(this->id()) +
                             " is done -- setting event to exit" + "\n";
          mover->do_stop();
          break;
        }

        thing = f_(stop_source_);
        if (stop_source_.stop_requested()) {
          mover->do_stop();
          break;
        }
      }
        [[fallthrough]];

      case 1: {
        program_counter_ = 2;
        inject(T{});
      }
        [[fallthrough]];

      case 2: {
        program_counter_ = 3;
        mover->do_fill();
      }
        [[fallthrough]];

      case 3: {
        program_counter_ = 4;
      }
        [[fallthrough]];

      case 4: {
        program_counter_ = 5;
        mover->do_push();
      }
        [[fallthrough]];

        // @todo Should skip yield if push waited;
      case 5: {
        program_counter_ = 0;
        //        this->do_yield(*this);
        // yield();
        break;
      }

      default:
        break;
    }
    return this;
  }
};

template <template <class> class Mover, class T>
struct consumer_node_impl : public node,
                            public receiver<T>,
                            public Sink<Mover, T> {
  using mover_type = Mover<T>;
  void set_item_mover(std::shared_ptr<mover_type> mover) {
    this->item_mover_ = mover;
  }

  std::function<void(const T&)> f_;

  std::atomic<size_t> consumed_items_{0};

  size_t consumed_items() {
    return consumed_items_.load();
  }

  template <class Function>
  consumer_node_impl(Function&& f)
      : node(id_counter++)
      , f_{std::forward<Function>(f)}
      , consumed_items_{0} {
  }

  T extract() {
    if (this->debug())
      std::cout << "consumer_node " + std::to_string(id_) + " extracting" +
                       "\n";
    return {};
  }

  void drain() {
    if (this->debug())
      std::cout << "consumer_node " + std::to_string(id_) + " draining" + "\n";
  }

  std::string name() {
    return {"consumer"};
  }

  node* resume() {
    auto mover = this->get_mover();

    if (this->debug())

      std::cout << this->name() + " node " + std::to_string(this->id()) +
                       " resuming with " + std::to_string(consumed_items_) +
                       " consumed_items" + "\n";

    [[maybe_unused]] std::thread::id this_id = std::this_thread::get_id();

    T thing;

    switch (program_counter_) {
      case 0: {
        ++program_counter_;
        mover->do_pull();
      }
        [[fallthrough]];

        /*
         * To make the flow here similar to producer, we start with pull() the
         * first time we are called but thereafter the loop goes from 1 to 5;
         */
      case 1: {
        ++program_counter_;

        thing = extract();
      }
        [[fallthrough]];

      case 2: {
        ++program_counter_;

        mover->do_drain();
      }
        [[fallthrough]];

      case 3: {
        ++program_counter_;

        assert(this->correspondent_ != nullptr);
      }
        [[fallthrough]];

      case 4: {
        ++program_counter_;

        if (consumed_items_++ >= problem_size) {
          if (this->debug())
            std::cout << this->name() + " node " + std::to_string(this->id()) +
                             " is done -- setting event to exit" + "\n";
          return this;
        }

        f_(thing);
      }

        // @todo Should skip yield if pull waited;
      case 5: {
        ++program_counter_;

        mover->do_pull();
      }

      case 6: {
        program_counter_ = 1;
        //        this->do_yield(*this);
        // yield();
        break;
      }
      default: {
        break;
      }
    }
    return this;
  }
};

template <template <class> class Mover, class T>
struct producer_node : std::shared_ptr<producer_node_impl<Mover, T>> {
  using Base = std::shared_ptr<producer_node_impl<Mover, T>>;
  using Base::Base;

  template <class Function>
  producer_node(Function&& f)
      : Base{std::make_shared<producer_node_impl<Mover, T>>(
            std::forward<Function>(f))} {
  }
};

template <template <class> class Mover, class T>
struct consumer_node : std::shared_ptr<consumer_node_impl<Mover, T>> {
  using Base = std::shared_ptr<consumer_node_impl<Mover, T>>;
  using Base::Base;

  template <class Function>
  consumer_node(Function&& f)
      : Base{std::make_shared<consumer_node_impl<Mover, T>>(
            std::forward<Function>(f))} {
  }
};

TEST_CASE("FrugalScheduler: Test create nodes", "[frugal]") {
  auto p =
      producer_node<FrugalMover3, size_t>([](std::stop_source&) { return 0; });
  auto c = consumer_node<FrugalMover3, size_t>([](const size_t&) {});
}

TEST_CASE("FrugalScheduler: Test assigning nodes", "[frugal]") {
  auto p =
      producer_node<FrugalMover3, size_t>([](std::stop_source&) { return 0; });
  auto c = consumer_node<FrugalMover3, size_t>([](const size_t&) {});

  std::shared_ptr<node> q = p;
  std::shared_ptr<node> d = c;
  q->correspondent_ = p;
  q->correspondent_ = c;
  d->correspondent_ = p;
  d->correspondent_ = c;
}

TEST_CASE("FrugalScheduler: Test connect nodes", "[frugal]") {
  auto p =
      producer_node<FrugalMover3, size_t>([](std::stop_source&) { return 0; });
  auto c = consumer_node<FrugalMover3, size_t>([](const size_t&) {});

  //  print_types(p, c, p->correspondent_, c->correspondent_);

  p->correspondent_ = c;
  c->correspondent_ = p;

  connect(p, c);
  Edge(*p, *c);
}

TEST_CASE("FrugalScheduler: Test FrugalTask", "[frugal]") {
  auto p =
      producer_node<FrugalMover3, size_t>([](std::stop_source&) { return 0; });
  auto c = consumer_node<FrugalMover3, size_t>([](const size_t&) {});

  // This doesn't work any longer -- took out converting constructor
  // auto q = FrugalTask(p);
  // auto d = FrugalTask(c);

  auto q = FrugalTask(p);
  auto d = FrugalTask(c);

  auto r = FrugalTask(p);
  auto e = FrugalTask(c);

  CHECK(c->name() == "consumer");
  CHECK(p->name() == "producer");
  CHECK(d->name() == "consumer");
  CHECK(q->name() == "producer");
  CHECK(e->name() == "consumer");
  CHECK(r->name() == "producer");

  auto s = r;
  auto t = s;

  SECTION("Node Equality") {
    // Not comparable
    // CHECK(p != c);

    // Not constructible
    // consumer_node<FrugalMover3, size_t> i{c};

    // Not constructible
    // auto j = p;

    // This is brilliant
    std::shared_ptr<node> a = p;
    std::shared_ptr<node> b = c;

    CHECK(a == p);
    CHECK(b == c);
    CHECK(a != b);

    // Not comparable
    // CHECK(*a == *p);
  }

  SECTION("FrugalTask Equality") {
    CHECK(reinterpret_cast<void*>(&c) != reinterpret_cast<void*>(&d));
    CHECK(reinterpret_cast<void*>(&c) != reinterpret_cast<void*>(&e));
    CHECK(reinterpret_cast<void*>(&e) != reinterpret_cast<void*>(&d));

    CHECK(&s != &r);
    CHECK(&s != &t);
    CHECK(&r != &t);

    CHECK(c == d);
    CHECK(e == e);
    CHECK(e == c);

    CHECK(q == p);
    CHECK(q == r);
    CHECK(s == r);
    CHECK(s == t);
    CHECK(r == t);
  }

  SECTION("Node and FrugalTask Equality") {
    std::shared_ptr<node> a = p;  // q
    std::shared_ptr<node> b = c;  // d

    CHECK(a == q);
    CHECK(b == d);
    CHECK(b != a);

    auto u = FrugalTask(c);
    auto v = FrugalTask(p);

    CHECK(u == c);
    u = c;
    v = p;

    std::shared_ptr<node> x{c};
    std::shared_ptr<node> y{p};

    // u = x;
    y = v;

    // CHECK(u == c);
    CHECK(y == p);
    // CHECK(c == u);
    CHECK(p == y);
  }

  SECTION("Queue") {
    std::queue<decltype(q)> q_;
    q_.push(q);
    auto u = q_.front();
    q_.pop();
    CHECK(q == u);
  }

  SECTION("Set") {
    std::set<FrugalTask<std::shared_ptr<node>>> q_;
    // s == r == t
    // q != d

    q_.insert(q);
    q_.insert(r);
    q_.insert(d);
    q_.insert(c);
    q_.insert(s);
    q_.insert(t);
    q_.insert(e);

    auto n = q_.extract(s);
    CHECK(!n.empty());
    CHECK(q == n.value());

    auto m = q_.extract(r);
    CHECK(m.empty());
    auto l = q_.extract(q);
    CHECK(l.empty());
    auto k = q_.extract(t);
    CHECK(k.empty());

    auto u = q_.extract(c);
    CHECK(!u.empty());
    auto v = q_.extract(c);
    CHECK(v.empty());
    auto w = q_.extract(s);
    CHECK(w.empty());
    auto x = q_.extract(e);
    CHECK(x.empty());
    auto y = q_.extract(d);
    CHECK(y.empty());
  }

  SECTION("Map") {
    std::map<FrugalTask<node>, std::shared_ptr<node>> m_;

    m_[r] = c;
    m_[e] = q;

    CHECK(m_[t] == e);
    CHECK(m_[c] == s);

    std::map<std::shared_ptr<node>, FrugalTask<node>> n_;

    n_[r] = c;
    n_[e] = q;

    CHECK(n_[t] == e);
    CHECK(n_[c] == s);

    std::map<std::shared_ptr<node>, std::shared_ptr<node>> o_;

    auto x = std::shared_ptr<node>{c};

    o_[r] = c;
    o_[e] = q;

    CHECK(o_[t] == e);
    CHECK(o_[c] == s);
    CHECK(o_[c] != d);
    CHECK(o_[c] != e);
    CHECK(o_[t] != r);
    CHECK(o_[t] != q);

    CHECK(m_[c] == n_[d]);
    CHECK(o_[d] == n_[e]);
    CHECK(o_[q] == m_[p]);
    CHECK(o_[t] == m_[r]);
  }
}

TEST_CASE("FrugalScheduler: Test construct scheduler", "[frugal]") {
  [[maybe_unused]] auto sched = FrugalScheduler<node>(1);
  // sched goes out of scope and shuts down the scheduler
}

TEST_CASE("FrugalScheduler: Test FrugalTask state changes", "[frugal]") {
  [[maybe_unused]] auto sched =
      SchedulerStateMachine<EmptySchedulerPolicy<FrugalTask<node>>>{};

  auto p =
      producer_node<FrugalMover3, size_t>([](std::stop_source&) { return 0; });
  auto c = consumer_node<FrugalMover3, size_t>([](const size_t&) {});

  auto q = FrugalTask<node>{p};
  auto d = FrugalTask<node>{c};

  CHECK(p == q);
  CHECK(d != q);

  auto e = d;
  CHECK(e == d);

  // Cannot do this
  // auto r = p;
  // CHECK(r == p);

  auto s = q;
  CHECK(s == q);

  // Cannot do this
  // p.dump_task_state();

  // Can to this
  // q.dump_task_state();

  SECTION("Admit") {
    // Cannot do this
    // sched.do_admit(p);
    // CHECK(str(p.state()) == "runnable");

    // Can do this
    sched.do_admit(q);
    CHECK(str(q.state()) == "runnable");
    CHECK(str(s.state()) == "runnable");

    CHECK(p == q);
    CHECK(s == q);
  }
}

TEST_CASE("FrugalScheduler: Test submit nodes", "[frugal]") {
  [[maybe_unused]] auto sched = FrugalScheduler<node>(1);

  auto p =
      producer_node<FrugalMover3, size_t>([](std::stop_source&) { return 0; });
  auto c = consumer_node<FrugalMover3, size_t>([](const size_t&) {});

  connect(p, c);
  Edge(*p, *c);
  sched.submit(p);
  sched.submit(c);
}

TEST_CASE("FrugalScheduler: Test submit and wait nodes", "[frugal]") {
  bool debug{true};

  // auto num_threads = GENERATE(1, 2, 3, 4);

  //  auto num_threads = 2UL;
  auto num_threads = 1UL;

  SECTION("With " + std::to_string(num_threads) + " threads") {
    [[maybe_unused]] auto sched = FrugalScheduler<node>(num_threads);

    if (debug) {
      sched.enable_debug();
    }

    auto p = producer_node<FrugalMover3, size_t>([&sched](std::stop_source&) {
      if (sched.debug())
        std::cout << "Producing"
                     "\n";
      return 0;
    });
    auto c = consumer_node<FrugalMover3, size_t>([&sched](const size_t&) {
      if (sched.debug())
        std::cout << "Consuming"
                     "\n";
    });

    if (debug) {
      p->enable_debug();
      c->enable_debug();
    }

    connect(p, c);
    Edge(*p, *c);
    sched.submit(p);
    sched.submit(c);
    sched.sync_wait_all();

    CHECK(p->produced_items() == problem_size + num_threads);
    CHECK(c->consumed_items() == problem_size + num_threads);
  }
}

#if 0

TEST_CASE("FrugalScheduler: Test passing integers", "[frugal]") {
  bool debug = true;

  //  auto num_threads = GENERATE(1, 2, 3, 4);
  auto num_threads = 1;
  auto rounds = problem_size;

  std::vector<size_t> input(rounds);
  std::vector<size_t> output(rounds);

  std::iota(input.begin(), input.end(), 19);
  std::fill(output.begin(), output.end(), 0);
  auto i = input.begin();
  auto j = output.begin();

  if (rounds != 0) {
    CHECK(std::equal(input.begin(), input.end(), output.begin()) == false);
  }

  SECTION(
      "With " + std::to_string(num_threads) + " threads and " +
      std::to_string(rounds) + " integers") {
    [[maybe_unused]] auto sched = FrugalScheduler<node>(num_threads);

    if (debug) {
      sched.enable_debug();
    }

    auto p =
        producer_node<FrugalMover3, size_t>([&sched, &i](std::stop_source&) {
          if (sched.debug())
            std::cout << "Producing"
                         "\n";
          return *i++;
        });
    auto c = consumer_node<FrugalMover3, size_t>(consumer{j});

    connect(p, c);
    sched.submit(p);
    sched.submit(c);
    sched.sync_wait_all();
  }
  CHECK(rounds != 0);
  CHECK(rounds == problem_size);

  CHECK(input.begin() != i);
  CHECK(input.size() == rounds);
  CHECK(output.size() == rounds);
  CHECK(std::equal(input.begin(), i, output.begin()));
  CHECK(std::distance(input.begin(), i) == static_cast<long>(rounds));
}
#endif
