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
#include <type_traits>
#include "../frugal.h"
#include "experimental/tiledb/common/dag/edge/edge.h"
#include "experimental/tiledb/common/dag/execution/jthread/stop_token.hpp"
#include "experimental/tiledb/common/dag/execution/scheduler.h"
#include "experimental/tiledb/common/dag/nodes/consumer.h"
#include "experimental/tiledb/common/dag/ports/ports.h"
#include "experimental/tiledb/common/dag/state_machine/test/helpers.h"
#include "experimental/tiledb/common/dag/state_machine/test/types.h"

using namespace tiledb::common;

struct node_base {
  using node_type = std::shared_ptr<node_base>;

  bool debug_{false};

  size_t id_;
  size_t program_counter_{0};

  std::shared_ptr<node_base> correspondent_{nullptr};

  node_base(node_base&&) = default;
  node_base(const node_base&) {
    std::cout << "Nonsense copy constructor" << std::endl;
  }

  virtual ~node_base() = default;

  inline size_t id() const {
    return id_;
  }

  node_base(size_t id)
      : id_{id} {
  }

  virtual void resume() = 0;

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

using node = std::shared_ptr<node_base>;

template <class From, class To>
void connect(From& from, To& to) {
  from->correspondent_ = to;
  to->correspondent_ = from;
}

static size_t problem_size = 7;  // 1337;

std::atomic<size_t> id_counter{0};

std::atomic<bool> item_{false};

template <template <class> class Mover, class T>
struct producer_node_impl : public node_base, public Source<Mover, T> {
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
  producer_node_impl(
      Function&& f,
      std::enable_if_t<
          std::is_invocable_r_v<T, Function, std::stop_source&>,
          void**> = nullptr)
      : node_base(id_counter++)
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

  void resume() {
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
  }
};

template <template <class> class Mover, class T>
struct consumer_node_impl : public node_base, public Sink<Mover, T> {
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
  consumer_node_impl(
      Function&& f,
      std::enable_if_t<
          std::is_invocable_r_v<void, Function, const T&>,
          void**> = nullptr)
      : node_base(id_counter++)
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

  void resume() {
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
          return;
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
  }
};

template <template <class> class Mover, class T>
struct producer_node : public std::shared_ptr<producer_node_impl<Mover, T>> {
  using Base = std::shared_ptr<producer_node_impl<Mover, T>>;
  using Base::Base;

  template <class Function>
  producer_node(Function&& f)
      : Base{std::make_shared<producer_node_impl<Mover, T>>(
            std::forward<Function>(f))} {
  }

  producer_node(producer_node_impl<Mover, T>& impl)
      : Base{std::make_shared<producer_node_impl<Mover, T>>(std::move(impl))} {
  }
};

template <template <class> class Mover, class T>
struct consumer_node : public std::shared_ptr<consumer_node_impl<Mover, T>> {
  using Base = std::shared_ptr<consumer_node_impl<Mover, T>>;
  using Base::Base;

  template <class Function>
  consumer_node(Function&& f)
      : Base{std::make_shared<consumer_node_impl<Mover, T>>(
            std::forward<Function>(f))} {
  }

  consumer_node(consumer_node_impl<Mover, T>& impl)
      : Base{std::make_shared<consumer_node_impl<Mover, T>>(std::move(impl))} {
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

  node q = p;
  node d = c;
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

/*
 * Define some helpers
 */
template <template <class> class T, class N>
struct hm {
  T<N> operator()(const N& n) {
    return {n};
  }
};

template <template <class> class T>
struct hm<T, node> {
  auto operator()(const node& n) {
    return FrugalTask{n};
  }
};

template <class N>
FrugalTask<node> task_from_node(N& n) {
  return {n};
}

template <class N>
TaskState task_state(const FrugalTask<N>& t) {
  return t.task_state();
}

template <class N>
TaskState& task_state(FrugalTask<N>& t) {
  return t.task_state();
}

template <class T>
T& task_handle(T& task) {
  return task;
}

auto hm_ = hm<FrugalTask, node>{};

bool two_nodes(node_base&, node_base&) {
  return true;
}

bool two_nodes(const node&, const node&) {
  return true;
}

TEST_CASE("FrugalScheduler: Construct nodes and impls", "[frugal]") {
  auto pro_node_impl = producer_node_impl<FrugalMover3, size_t>(
      [](std::stop_source&) { return 0; });
  auto con_node_impl =
      consumer_node_impl<FrugalMover3, size_t>([](const size_t&) {});

  auto pro_node =
      producer_node<FrugalMover3, size_t>([](std::stop_source&) { return 0; });
  auto con_node = consumer_node<FrugalMover3, size_t>([](const size_t&) {});

  //  auto pro_specified =
  //      producer_node<FrugalMover3, size_t>(pro_node_impl);  // bad
  //  auto pro_deduced = producer_node(pro_node_impl);
  //  pro_deduced->fill();

  // producer_node_impl<FrugalMover3, size_t>
  //   ::producer_node_impl<producer_node_impl<FrugalMover3, size_t>>

  //  auto con_specified = consumer_node<FrugalMover3, size_t>(con_node_impl);
  //  // bad
  //  auto con_deduced = consumer_node(con_node_impl);

  // The types will print?
  // print_types(pro_deduced, con_deduced);
  //   producer_node<FrugalMover3, unsigned long>
  //   consumer_node<FrugalMover3, unsigned long>

  // print_types(pro_specified, con_specified);
  //   producer_node<FrugalMover3, unsigned long>
  //   consumer_node<FrugalMover3, unsigned long>

  SECTION("Check specified and deduced are same types") {
    //    CHECK(std::is_same_v<decltype(pro_specified), decltype(pro_deduced)>);
    //    CHECK(std::is_same_v<decltype(con_specified), decltype(con_deduced)>);
  }

  SECTION("Check polymorphism to node&") {
    CHECK(two_nodes(pro_node_impl, con_node_impl));

    // No conversion from producer_node to node
    CHECK(two_nodes(pro_node, con_node));

    // No conversion from producer_node to node&
    //    CHECK(two_shared_nodes(pro_node, con_node));
  }

  SECTION("Checks with FrugalTask and node (godbolt)") {
    auto shared_pro = node{pro_node};
    auto shared_con = node{con_node};

    auto shared_nil = node{};
    shared_nil = shared_pro;
    CHECK(shared_nil == shared_pro);
  }

  SECTION("I think this works (godbolt)", "[frugal]") {
    auto frugal_pro = FrugalTask<node>{pro_node};
    auto frugal_con = FrugalTask<node>{con_node};

    auto frugal_from_pro = task_from_node(pro_node);
    auto frugal_from_con = task_from_node(con_node);

    auto frugal_hm_pro = hm_(pro_node);
    auto frugal_hm_con = hm_(con_node);

    /**
     * @todo Unify producer and consumer cases via TEMPLATE_TEST_CASE
     */

    /*
     * Producer case
     */

    /*
     * Tasks constructed from same nodes are unique
     * Though maybe we should not allow this?
     */

    auto frugal_pro_1 = FrugalTask<node>{pro_node};
    auto frugal_pro_2 = FrugalTask<node>{pro_node};
    auto frugal_pro_3 = task_from_node(pro_node);
    auto frugal_pro_4 = hm_(pro_node);
    auto frugal_pro_5 = frugal_pro_1;
    auto frugal_pro_6 = frugal_pro_3;

    CHECK(frugal_pro_1 != frugal_pro_2);
    CHECK(frugal_pro_2 != frugal_pro_3);
    CHECK(frugal_pro_3 != frugal_pro_4);
    CHECK(frugal_pro_4 != frugal_pro_5);
    CHECK(frugal_pro_5 != frugal_pro_6);

    FrugalTask<node> frugal_pro_7{frugal_pro_2};
    FrugalTask frugal_pro_8{frugal_pro_2};

    CHECK(frugal_pro_6 != frugal_pro_7);
    CHECK(frugal_pro_7 == frugal_pro_2);
    CHECK(frugal_pro_7 == frugal_pro_8);
    CHECK(frugal_pro_8 != frugal_pro_1);

    auto frugal_pro_1_x = frugal_pro_1;
    CHECK(frugal_pro_1 == frugal_pro_1);
    CHECK(frugal_pro_1_x == frugal_pro_1);
    CHECK(frugal_pro_1 == frugal_pro_1_x);

    auto frugal_pro_5_x = frugal_pro_5;
    CHECK(frugal_pro_5_x == frugal_pro_5);

    // Warning danger -- don't use frugal_pro_5 after the move
    FrugalTask<node> frugal_pro_5_moved{std::move(frugal_pro_5)};
    CHECK(frugal_pro_5_moved == frugal_pro_5_x);

    /*
     * Consumer case
     */
    auto frugal_con_1 = FrugalTask<node>{con_node};
    auto frugal_con_2 = FrugalTask<node>{con_node};
    auto frugal_con_3 = task_from_node(con_node);
    auto frugal_con_4 = hm_(con_node);
    auto frugal_con_5 = frugal_con_1;
    auto frugal_con_6 = frugal_con_3;

    CHECK(frugal_con_1 != frugal_con_2);
    CHECK(frugal_con_2 != frugal_con_3);
    CHECK(frugal_con_3 != frugal_con_4);
    CHECK(frugal_con_4 != frugal_con_5);
    CHECK(frugal_con_5 != frugal_con_6);

    FrugalTask<node> frugal_con_7{frugal_con_2};
    FrugalTask frugal_con_8{frugal_con_2};

    CHECK(frugal_con_6 != frugal_con_7);
    CHECK(frugal_con_7 == frugal_con_2);
    CHECK(frugal_con_7 == frugal_con_8);
    CHECK(frugal_con_8 != frugal_con_1);

    auto frugal_con_1_x = frugal_con_1;
    CHECK(frugal_con_1 == frugal_con_1);
    CHECK(frugal_con_1_x == frugal_con_1);
    CHECK(frugal_con_1 == frugal_con_1_x);

    auto frugal_con_5_x = frugal_con_5;
    CHECK(frugal_con_5_x == frugal_con_5);

    // Warning danger -- don't use frugal_con_5 after the move
    FrugalTask<node> frugal_con_5_moved{std::move(frugal_con_5)};
    CHECK(frugal_con_5_moved == frugal_con_5_x);
  }

  SECTION("Check states") {
    auto frugal_pro = FrugalTask<node>{pro_node};
    auto frugal_con = FrugalTask<node>{con_node};

    auto frugal_from_pro = task_from_node(pro_node);
    auto frugal_from_con = task_from_node(con_node);

    auto frugal_hm_pro = hm_(pro_node);
    auto frugal_hm_con = hm_(con_node);

    CHECK(str(task_state(frugal_pro)) == "created");
    CHECK(str(task_state(frugal_from_pro)) == "created");
    CHECK(str(task_state(frugal_hm_pro)) == "created");

    CHECK(str(task_state(frugal_con)) == "created");
    CHECK(str(task_state(frugal_from_con)) == "created");
    CHECK(str(task_state(frugal_hm_con)) == "created");

    /*
     * No aliasing of tasks
     */
    task_state(frugal_pro) = TaskState::running;
    CHECK(str(task_state(frugal_pro)) == "running");

    CHECK(str(task_state(frugal_from_pro)) == "created");
    CHECK(str(task_state(frugal_hm_pro)) == "created");
    CHECK(str(task_state(frugal_con)) == "created");
    CHECK(str(task_state(frugal_from_con)) == "created");
    CHECK(str(task_state(frugal_hm_con)) == "created");

    task_state(frugal_pro) = TaskState::created;
    CHECK(str(task_state(frugal_pro)) == "created");

    CHECK(str(task_state(frugal_from_pro)) == "created");
    CHECK(str(task_state(frugal_hm_pro)) == "created");
    CHECK(str(task_state(frugal_con)) == "created");
    CHECK(str(task_state(frugal_from_con)) == "created");
    CHECK(str(task_state(frugal_hm_con)) == "created");

    task_state(frugal_con) = TaskState::running;
    CHECK(str(task_state(frugal_con)) == "running");

    CHECK(str(task_state(frugal_pro)) == "created");
    CHECK(str(task_state(frugal_from_pro)) == "created");
    CHECK(str(task_state(frugal_hm_pro)) == "created");
    CHECK(str(task_state(frugal_from_con)) == "created");
    CHECK(str(task_state(frugal_hm_con)) == "created");
  }
}

TEST_CASE("FrugalScheduler: I think this works (godbolt)", "[frugal]") {
#if 0
  auto b = consumer_node<FrugalMover3, size_t>([](const size_t&) {});
  auto d =
      producer_node<FrugalMover3, size_t>([](std::stop_source&) { return 0; });

  auto i = node{b};
  auto z = node{};
  z = b;
  z = i;
  auto y = FrugalTask<node>{d};

  auto n = task_from_node(d);
  auto m = hm_(d);
  m = y;
  n = y;
  m = n;

  // Can only make tasks from node, not node_impl
  // auto c = consumer_node_impl<FrugalMover3, size_t>([](const size_t&) {});
  // auto x = FrugalTask<node>{c};
  auto w = FrugalTask<node>{b};

  //  std::cout << std::boolalpha;
  // Compare producer_node with FrugalTask
  CHECK(d == y);
  CHECK(d != w);

  auto x = y;
  CHECK(x == y);  // Shallow copy

  CHECK(str(task_state(x)) == "created");
  CHECK(str(task_state(y)) == "created");
  task_state(y) = TaskState::running;
  CHECK(str(task_state(x)) == "running");
  CHECK(str(task_state(y)) == "running");
  CHECK(x == y);  // Shallow copy
#endif
}

namespace tiledb::common {
FrugalTask(node)->FrugalTask<node>;
FrugalTask(node&)->FrugalTask<node>;
FrugalTask(const node&)->FrugalTask<node>;

template <template <class> class M, class T>
FrugalTask(producer_node<M, T>)->FrugalTask<node>;
template <template <class> class M, class T>
FrugalTask(consumer_node<M, T>)->FrugalTask<node>;
}  // namespace tiledb::common

TEST_CASE("FrugalScheduler: Test FrugalTask", "[frugal]") {
  auto pro_node =
      producer_node<FrugalMover3, size_t>([](std::stop_source&) { return 0; });
  auto con_node = consumer_node<FrugalMover3, size_t>([](const size_t&) {});

  auto pro_node_2 =
      producer_node<FrugalMover3, size_t>([](std::stop_source&) { return 0; });
  auto con_node_2 = consumer_node<FrugalMover3, size_t>([](const size_t&) {});

  auto pro_task = FrugalTask(pro_node);
  auto con_task = FrugalTask(con_node);

  auto pro_task_assign = pro_task;
  auto con_task_assign = con_task;

  auto pro_task_copy = FrugalTask(pro_task);
  auto con_task_copy = FrugalTask(con_task);

  auto pro_task_2 = FrugalTask(pro_node_2);
  auto con_task_2 = FrugalTask(con_node_2);

  SECTION("Names") {
    CHECK(con_node->name() == "consumer");
    CHECK(pro_node->name() == "producer");
    CHECK(con_task->name() == "consumer task");
    CHECK(pro_task->name() == "producer task");
    CHECK(con_task_2->name() == "consumer task");
    CHECK(pro_task_2->name() == "producer task");
  }

  SECTION("Node Equality") {
    // This is brilliant
    node node_pro = pro_node;
    node node_con = con_node;

    CHECK(node_pro == pro_node);
    CHECK(node_con == con_node);
    CHECK(node_pro != node_con);

    CHECK(static_cast<void*>(&(*node_pro)) == static_cast<void*>(&(*node_pro)));
    CHECK(static_cast<void*>(&(*node_pro)) == static_cast<void*>(&(*pro_node)));
  }

  SECTION("FrugalTask Equality") {
  }

  SECTION("Node and FrugalTask Equality") {
  }

  SECTION("Queue") {
    auto pro_task = FrugalTask(pro_node);
    auto con_task = FrugalTask(con_node);

    auto pro_node_i = producer_node<FrugalMover3, size_t>(
        [](std::stop_source&) { return 0; });
    auto pro_node_j = producer_node<FrugalMover3, size_t>(
        [](std::stop_source&) { return 0; });
    auto pro_node_k = producer_node_impl<FrugalMover3, size_t>(
        [](std::stop_source&) { return 0; });

    auto con_node_i = consumer_node<FrugalMover3, size_t>([](const size_t&) {});
    auto con_node_j = consumer_node<FrugalMover3, size_t>([](const size_t&) {});
    auto con_node_k = consumer_node<FrugalMover3, size_t>([](const size_t&) {});

    auto pro_task_i = FrugalTask<node>{pro_node_i};
    auto pro_task_j = FrugalTask<node>{pro_node_j};
    auto pro_task_i_deduced = FrugalTask{pro_node_i};
    auto pro_task_j_deduced = FrugalTask{pro_node_j};
    auto pro_task_i_tfn = task_from_node(pro_node_i);
    auto pro_task_j_tfn = task_from_node(pro_node_j);

    auto con_task_i = FrugalTask<node>{con_node_i};
    auto con_task_j = FrugalTask<node>{con_node_j};
    auto con_task_i_deduced = FrugalTask{con_node_i};
    auto con_task_j_deduced = FrugalTask{con_node_j};
    auto con_task_i_tfn = task_from_node(con_node_i);
    auto con_task_j_tfn = task_from_node(con_node_j);

    CHECK(pro_task_i != pro_task_i_deduced);
    CHECK(pro_task_j != pro_task_j_deduced);

    std::queue<node> node_queue;
    node_queue.push(pro_node);
    node_queue.push(con_node);

    std::queue<FrugalTask<node>> task_queue;

    task_queue.push(pro_task_i);
    task_queue.push(con_task_i);
    task_queue.push(pro_task_j);
    task_queue.push(con_task_j);

    task_queue.push(pro_task_i_tfn);
    task_queue.push(con_task_i_tfn);

    task_queue.push(pro_task_i_deduced);
    task_queue.push(con_task_i_deduced);
    task_queue.push(pro_task_j_deduced);
    task_queue.push(con_task_j_deduced);

    CHECK(task_queue.front() == pro_task_i);
    task_queue.pop();
    CHECK(task_queue.front() == con_task_i);
    task_queue.pop();
    CHECK(task_queue.front() == pro_task_j);
    task_queue.pop();
    CHECK(task_queue.front() == con_task_j);
    task_queue.pop();

    CHECK(task_queue.front() == pro_task_i_tfn);
    task_queue.pop();
    CHECK(task_queue.front() == con_task_i_tfn);
    task_queue.pop();

    CHECK(task_queue.front() == pro_task_i_deduced);
    task_queue.pop();
    CHECK(task_queue.front() == con_task_i_deduced);
    task_queue.pop();
    CHECK(task_queue.front() == pro_task_j_deduced);
    task_queue.pop();
    CHECK(task_queue.front() == con_task_j_deduced);
    task_queue.pop();
    CHECK(task_queue.empty());

    auto pro_task_copy = pro_task;
    CHECK(pro_task == pro_task);
    CHECK(pro_task_copy == pro_task_copy);
    CHECK(pro_task_copy == pro_task);
    CHECK(pro_task == pro_task_copy);

    auto empty_queue = std::queue<FrugalTask<node>>{};
    task_queue.swap(empty_queue);
    CHECK(task_queue.empty());

    // Check that we get same task back when we push and pop
    task_queue.push(pro_task_copy);
    CHECK(!task_queue.empty());

    auto pro_task_front = task_queue.front();

    CHECK(pro_task == pro_task_copy);
    CHECK(pro_task == pro_task_front);
    task_queue.pop();
    CHECK(pro_task == pro_task_copy);
    CHECK(pro_task == pro_task_front);

    CHECK(str(task_state(pro_task)) == "created");
    CHECK(str(task_state(pro_task_copy)) == "created");
    CHECK(str(task_state(pro_task_front)) == "created");

    /*
     * Check that copies are shallow
     */
    task_state(pro_task_copy) = TaskState::running;
    CHECK(str(task_state(pro_task_copy)) == "running");
    CHECK(str(task_state(pro_task_copy)) == "running");
    CHECK(str(task_state(pro_task_front)) == "running");

    task_queue.push(pro_task_copy);
    auto pro_task_front_running = task_queue.front();
    CHECK(str(task_state(pro_task_front_running)) == "running");

    task_state(pro_task_copy) = TaskState::runnable;
    task_queue.push(pro_task_copy);
    CHECK(task_queue.front() == pro_task_copy);
    CHECK(task_state(task_queue.front()) == TaskState::runnable);
    CHECK(str(task_state(task_queue.front())) == "runnable");

    task_queue.pop();
    task_queue.pop();
    CHECK(task_queue.empty());
  }

#if 0
  SECTION("Set") {
    std::set<FrugalTask<node>> task_set;



    lp = q;
    auto lq = q;
    lq = lp;

    // FrugalTask<producer_node_impl>, FrugalTask<node>
    print_types(q, lp, (typename decltype(q_)::value_type){}, FrugalTask<node>);

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
    std::map<FrugalTask<node>, node> m_;

    m_[r] = c;
    m_[e] = q;

    CHECK(m_[t] == e);
    CHECK(m_[c] == s);

    std::map<node, FrugalTask<node>> n_;

    n_[r] = c;
    n_[e] = q;

    CHECK(n_[t] == e);
    CHECK(n_[c] == s);

    std::map<node, node> o_;

    auto x = node{c};

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
#endif
}

#if 0
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
#endif
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
