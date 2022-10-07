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

  node_type correspondent_{nullptr};

  node_base(node_base&&) = default;
  node_base(const node_base&) {
    std::cout << "Nonsense copy constructor" << std::endl;
  }

  virtual ~node_base() = default;

  inline size_t id() const {
    return id_;
  }

  inline size_t& id() {
    return id_;
  }

  node_base(size_t id)
      : id_{id} {
  }

  virtual void resume() = 0;

  void decrement_program_counter() {
    assert(program_counter_ > 0);
    --program_counter_;
  }

  virtual std::string name() {
    return {"abstract base"};
  }

  virtual void enable_debug() {
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

static size_t problem_size = 1337;
static size_t debug_problem_size = 3;

std::atomic<size_t> id_counter{0};

std::atomic<bool> item_{false};

template <template <class> class Mover, class T>
struct producer_node_impl : public node_base, public Source<Mover, T> {
  using mover_type = Mover<T>;
  void set_item_mover(std::shared_ptr<mover_type> mover) {
    this->item_mover_ = mover;
  }
  using SourceBase = Source<Mover, T>;

  std::function<T(std::stop_source&)> f_;

  std::atomic<size_t> produced_items_{0};

  size_t produced_items() {
    if (this->debug())

      std::cout << std::to_string(produced_items_.load())
                << " produced items in produced_items()\n";
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

  //  void inject(T) {
  //    if (this->debug())
  //      std::cout << "producer_node " + std::to_string(id_) + " injecting" + "\n";
  //  }

  //  void fill() {
  //    if (this->debug())
  //      std::cout << "producer_node " + std::to_string(id_) + " filling" + "\n";
  //  }

  std::string name() {
    return {"producer"};
  }

  void enable_debug() {
    node_base::enable_debug();
    if (this->item_mover_)
      this->item_mover_->enable_debug();
  }

  void resume() {
    auto mover = this->get_mover();

    if (this->debug()) {
      std::cout << "producer resuming\n";

      std::cout << this->name() + " node " + std::to_string(this->id()) +
                       " resuming with " + std::to_string(produced_items_) +
                       " produced_items" + "\n";
    }

    [[maybe_unused]] std::thread::id this_id = std::this_thread::get_id();

    std::stop_source st;
    decltype(f_(st)) thing{};

    std::stop_source stop_source_;

    switch (program_counter_) {
      case 0: {
        program_counter_ = 1;

        if (produced_items_ >= problem_size) {
          if (this->debug())
            std::cout << this->name() + " node " + std::to_string(this->id()) +
                             " has produced enough items -- calling "
                             "port_exhausted with " +
                             std::to_string(produced_items_) +
                             " produced items and " +
                             std::to_string(problem_size) + " problem size\n";
          mover->port_exhausted();
          break;
        }

        thing = f_(stop_source_);

        if (this->debug())

          std::cout << "producer thing is " + std::to_string(thing) + "\n";

        if (stop_source_.stop_requested()) {
          if (this->debug())
            std::cout
                << this->name() + " node " + std::to_string(this->id()) +
                       " has gotten stop -- calling port_exhausted with " +
                       std::to_string(produced_items_) +
                       " produced items and " + std::to_string(problem_size) +
                       " problem size\n";

          mover->port_exhausted();
          break;
        }
        ++produced_items_;
      }

        [[fallthrough]];

      case 1: {
        program_counter_ = 2;

        SourceBase::inject(thing);
      }
        [[fallthrough]];

      case 2: {
        program_counter_ = 3;
        mover->port_fill();
      }
        [[fallthrough]];

      case 3: {
        program_counter_ = 4;
      }
        [[fallthrough]];

      case 4: {
        program_counter_ = 5;
        mover->port_push();
      }
        [[fallthrough]];

        // @todo Should skip yield if push waited;
      case 5: {
        program_counter_ = 0;
        //        this->task_yield(*this);
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
  using SinkBase = Sink<Mover, T>;

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

  // T extract() {
  //   if (this->debug())
  //     std::cout << "consumer_node " + std::to_string(id_) + " extracting" +
  //                      "\n";
  //   return {};
  // }

  // void drain() {
  //   if (this->debug())
  //     std::cout << "consumer_node " + std::to_string(id_) + " draining" +
  //     "\n";
  // }

  std::string name() {
    return {"consumer"};
  }

  void enable_debug() {
    node_base::enable_debug();
    if (this->item_mover_)
      this->item_mover_->enable_debug();
  }

  T thing{};

  void resume() {
    auto mover = this->get_mover();

    if (this->debug())

      std::cout << this->name() + " node " + std::to_string(this->id()) +
                       " resuming with " + std::to_string(consumed_items_) +
                       " consumed_items" + "\n";

    [[maybe_unused]] std::thread::id this_id = std::this_thread::get_id();

    switch (program_counter_) {
      case 0: {
        ++program_counter_;
        mover->port_pull();
      }
        [[fallthrough]];

        /*
         * To make the flow here similar to producer, we start with pull() the
         * first time we are called but thereafter the loop goes from 1 to 5;
         */
      case 1: {
        ++program_counter_;

        thing = *(SinkBase::extract());

        if (this->debug())

          std::cout << "consumer thing is " + std::to_string(thing) + "\n";
      }
        [[fallthrough]];

      case 2: {
        ++program_counter_;

        mover->port_drain();
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
          //                    if (this->debug())
          std::cout << "THIS SHOULD NOT HAPPEN " + this->name() + " node " +
                           std::to_string(this->id()) +
                           " has produced enough items -- calling "
                           "port_exhausted with " +
                           std::to_string(consumed_items_) +
                           " consumed items and " +
                           std::to_string(problem_size) + " problem size\n";
          //          assert(false);

          mover->port_exhausted();
          break;
        }

        if (this->debug())
          std::cout << "next consumer thing is " + std::to_string(thing) + "\n";
        f_(thing);
      }

        // @todo Should skip yield if pull waited;
      case 5: {
        ++program_counter_;

        mover->port_pull();
      }

      case 6: {
        program_counter_ = 1;
        //        this->task_yield(*this);
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
  SECTION("FrugalMover2") {
    auto p = producer_node<FrugalMover2, size_t>(
        [](std::stop_source&) { return 0; });
    auto c = consumer_node<FrugalMover2, size_t>([](const size_t&) {});
  }
  SECTION("FrugalMover3") {
    auto p = producer_node<FrugalMover3, size_t>(
        [](std::stop_source&) { return 0; });
    auto c = consumer_node<FrugalMover3, size_t>([](const size_t&) {});
  }
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

namespace tiledb::common {
// FrugalTask(node)->FrugalTask<node>;
FrugalTask(node&)->FrugalTask<node>;
FrugalTask(const node&)->FrugalTask<node>;

template <template <class> class M, class T>
FrugalTask(producer_node<M, T>) -> FrugalTask<node>;
template <template <class> class M, class T>
FrugalTask(consumer_node<M, T>) -> FrugalTask<node>;
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
}

SCENARIO(
    "Tasks can be pushed and popped into a queue without invalidating "
    "references to them") {
  auto pro_node =
      producer_node<FrugalMover3, size_t>([](std::stop_source&) { return 0; });
  auto con_node = consumer_node<FrugalMover3, size_t>([](const size_t&) {});

  auto pro_task = FrugalTask(pro_node);
  auto con_task = FrugalTask(con_node);

  GIVEN("Tasks pro_task and pro_task_copy (copy of pro_task)") {
    auto pro_task_assign = pro_task;
    auto con_task_assign = con_task;

    auto pro_task_copy = FrugalTask(pro_task);
    auto con_task_copy = FrugalTask(con_task);
    THEN("pro_task == pro_task_copy") {
      CHECK(pro_task_assign == pro_task);
      CHECK(con_task_assign == con_task);

      CHECK(pro_task_copy == pro_task);
      CHECK(con_task_copy == con_task);

      CHECK(pro_task != con_task);
    }
    WHEN("Task with copy is pushed onto a queue") {
      std::queue<FrugalTask<node>> task_queue;
      auto pro_task_to_push = FrugalTask(pro_task);
      CHECK(pro_task_to_push == pro_task);
      task_queue.push(pro_task_to_push);

      THEN("The front of the queue is still equal to original task") {
        CHECK(task_queue.front() == pro_task);
        AND_THEN("A task copied from the front is equal to original task") {
          auto front_pro_task = task_queue.front();
          CHECK(task_queue.front() == pro_task);
          CHECK(front_pro_task == pro_task);
        }
      }
      AND_WHEN("The task is popped") {
        auto popped_pro_task = task_queue.front();
        task_queue.pop();
        THEN("The popped task is still equal to the original task") {
          CHECK(popped_pro_task == pro_task);
        }
      }
      AND_WHEN("We push tasks onto the queue") {
        std::queue<FrugalTask<node>> created_queue;
        std::queue<FrugalTask<node>> submitted_queue;

        auto created_pro_task_i = FrugalTask(pro_node);
        auto created_pro_task_j = FrugalTask(pro_node);
        auto created_pro_task_k = FrugalTask(pro_node);

        auto copied_pro_task_i = created_pro_task_i;
        auto copied_pro_task_j = created_pro_task_j;
        auto copied_pro_task_k = created_pro_task_k;

        created_queue.push(created_pro_task_i);
        created_queue.push(created_pro_task_j);
        created_queue.push(created_pro_task_k);
        AND_WHEN("Task state is changed") {
          auto popped_pro_task_i = created_queue.front();
          created_queue.pop();
          CHECK(task_state(popped_pro_task_i) == TaskState::created);
          auto popped_pro_task_j = created_queue.front();
          created_queue.pop();
          CHECK(task_state(popped_pro_task_j) == TaskState::created);
          auto popped_pro_task_k = created_queue.front();
          created_queue.pop();
          CHECK(task_state(popped_pro_task_k) == TaskState::created);

          task_state(popped_pro_task_i) = TaskState::runnable;
          submitted_queue.push(popped_pro_task_i);
          task_state(popped_pro_task_j) = TaskState::running;
          submitted_queue.push(popped_pro_task_j);
          task_state(popped_pro_task_k) = TaskState::terminated;
          submitted_queue.push(popped_pro_task_k);

          THEN("The property of the original changes also") {
            CHECK(task_state(copied_pro_task_i) == TaskState::runnable);
            CHECK(task_state(copied_pro_task_j) == TaskState::running);
            CHECK(task_state(copied_pro_task_k) == TaskState::terminated);

            CHECK(str(task_state(copied_pro_task_i)) == "runnable");
            CHECK(str(task_state(copied_pro_task_j)) == "running");
            CHECK(str(task_state(copied_pro_task_k)) == "terminated");
          }
        }
      }
    }
  }
}

SCENARIO(
    "Tasks can be inserted into and extracted from a set without invalidating "
    "references to them") {
  auto pro_node =
      producer_node<FrugalMover3, size_t>([](std::stop_source&) { return 0; });
  auto con_node = consumer_node<FrugalMover3, size_t>([](const size_t&) {});

  auto pro_task = FrugalTask(pro_node);
  auto con_task = FrugalTask(con_node);

  GIVEN("Tasks pro_task and pro_task_copy (copy of pro_task)") {
    auto pro_task_assign = pro_task;
    auto con_task_assign = con_task;

    auto pro_task_copy = FrugalTask(pro_task);
    auto con_task_copy = FrugalTask(con_task);

    THEN("pro_task == pro_task_copy") {
      CHECK(pro_task_assign == pro_task);
      CHECK(con_task_assign == con_task);

      CHECK(pro_task_copy == pro_task);
      CHECK(con_task_copy == con_task);

      CHECK(pro_task != con_task);
    }
    WHEN("Task with copy is inserted into a set") {
      std::set<FrugalTask<node>> task_set;

      auto pro_task_to_insert = FrugalTask(pro_task);
      CHECK(pro_task_to_insert == pro_task);
      task_set.insert(pro_task_to_insert);

      THEN("The inserted task can be found using original") {
        CHECK(task_set.find(pro_task_to_insert) != end(task_set));
        CHECK(task_set.find(pro_task) != end(task_set));

        AND_THEN("A task extracted from the set is equal to original task") {
          auto extracted_pro_task_handle = task_set.extract(pro_task_to_insert);
          CHECK(!extracted_pro_task_handle.empty());
          CHECK(extracted_pro_task_handle.value() == pro_task);
        }
      }

      AND_WHEN("We insert multiple tasks into a set") {
        std::set<FrugalTask<node>> created_set;
        std::set<FrugalTask<node>> submitted_set;

        auto created_pro_task_i = FrugalTask(pro_node);
        auto created_pro_task_j = FrugalTask(pro_node);
        auto created_pro_task_k = FrugalTask(pro_node);

        auto copied_pro_task_i = created_pro_task_i;
        auto copied_pro_task_j = created_pro_task_j;
        auto copied_pro_task_k = created_pro_task_k;

        created_set.insert(created_pro_task_i);
        created_set.insert(created_pro_task_j);
        created_set.insert(created_pro_task_k);

        AND_WHEN("Task state is changed") {
          auto extracted_pro_task_i =
              created_set.extract(created_pro_task_i).value();
          CHECK(task_state(extracted_pro_task_i) == TaskState::created);
          auto extracted_pro_task_j =
              created_set.extract(created_pro_task_j).value();
          CHECK(task_state(extracted_pro_task_j) == TaskState::created);
          auto extracted_pro_task_k =
              created_set.extract(created_pro_task_k).value();
          CHECK(task_state(extracted_pro_task_k) == TaskState::created);

          task_state(extracted_pro_task_i) = TaskState::runnable;
          task_state(extracted_pro_task_j) = TaskState::running;
          task_state(extracted_pro_task_k) = TaskState::terminated;

          submitted_set.insert(copied_pro_task_i);
          submitted_set.insert(created_pro_task_j);
          submitted_set.insert(extracted_pro_task_k);

          THEN("The property of the original changes also") {
            CHECK(task_state(copied_pro_task_i) == TaskState::runnable);
            CHECK(task_state(copied_pro_task_j) == TaskState::running);
            CHECK(task_state(copied_pro_task_k) == TaskState::terminated);

            CHECK(str(task_state(copied_pro_task_i)) == "runnable");
            CHECK(str(task_state(copied_pro_task_j)) == "running");
            CHECK(str(task_state(copied_pro_task_k)) == "terminated");

            CHECK(str(task_state(created_pro_task_i)) == "runnable");
            CHECK(str(task_state(created_pro_task_j)) == "running");
            CHECK(str(task_state(created_pro_task_k)) == "terminated");

            CHECK(
                submitted_set.extract(created_pro_task_i).value() ==
                created_pro_task_i);
            CHECK(
                submitted_set.extract(copied_pro_task_j).value() ==
                created_pro_task_j);
            CHECK(
                submitted_set.extract(extracted_pro_task_k).value() ==
                created_pro_task_k);

            CHECK(str(task_state(created_pro_task_i)) == "runnable");
            CHECK(str(task_state(created_pro_task_j)) == "running");
            CHECK(str(task_state(created_pro_task_k)) == "terminated");
          }
        }
      }
    }
  }
}

/**
 * @todo Completely repeat scenarios for std::queue and std::map
 */
SCENARIO(
    "Tasks can be inserted into and looked up from a map, using nodes as "
    "keys") {
  auto pro_node =
      producer_node<FrugalMover3, size_t>([](std::stop_source&) { return 0; });
  auto con_node = consumer_node<FrugalMover3, size_t>([](const size_t&) {});

  auto pro_task = FrugalTask(pro_node);
  auto con_task = FrugalTask(con_node);

  GIVEN("A node to task map") {
    std::map<node, FrugalTask<node>> node_to_task_map;

    WHEN("Insert node-task pair into map") {
      auto pro_task_copy = FrugalTask(pro_task);

      node_to_task_map[pro_node] = pro_task;

      THEN("Retrieved task is equal to inserted task") {
        CHECK(node_to_task_map[pro_node] == pro_task_copy);
      }
      THEN("Changing retrieved task state changes inserted task state") {
        auto retrieved_pro_task = node_to_task_map[pro_node];
        CHECK(retrieved_pro_task == pro_task_copy);
        CHECK(retrieved_pro_task == pro_task);
        CHECK(task_state(retrieved_pro_task) == TaskState::created);
        task_state(retrieved_pro_task) = TaskState::running;
        CHECK(task_state(retrieved_pro_task) == TaskState::running);
        CHECK(task_state(pro_task) == TaskState::running);
      }
    }
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

  // Can't directly compare nodes of different types
  // CHECK(p != c);
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
    // sched.task_admit(p);
    // CHECK(str(p.state()) == "runnable");

    // Can do this
    sched.task_admit(q);
    CHECK(str(q->task_state()) == "runnable");
    CHECK(str(s->task_state()) == "runnable");

    // Can't compare different types
    // CHECK(p == q);
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


#if 0
    (std::tuple<
        InjectorNode<AsyncMover2, size_t>,
        ConsumerNode<AsyncMover2, size_t>>),
    (std::tuple<
        InjectorNode<AsyncMover3, size_t>,
        ConsumerNode<AsyncMover3, size_t>>)) {

  using I = typename std::tuple_element<0, TestType>::type;
  using C = typename std::tuple_element<1, TestType>::type;
#endif

TEMPLATE_TEST_CASE(
    "FrugalScheduler: Test submit and wait nodes",
    "[frugal]",
    (std::tuple<
        consumer_node<FrugalMover2, size_t>,
        producer_node<FrugalMover2, size_t>>),
    (std::tuple <
     consumer_node<FrugalMover3, size_t>,producer_node<FrugalMover3, size_t>>)) {
  using C = typename std::tuple_element<0, TestType>::type;
  using P = typename std::tuple_element<1, TestType>::type;


  bool debug{false};

  auto num_threads = GENERATE(1, 2, 3, 4, 5, 8, 17);

  // auto num_threads = 17UL;

  if (debug) {
    problem_size = debug_problem_size;
  }

  SECTION("With " + std::to_string(num_threads) + " threads") {
    [[maybe_unused]] auto sched = FrugalScheduler<node>(num_threads);

    if (debug) {
      sched.enable_debug();
    }

    auto p = P([&sched](std::stop_source&) {
      if (sched.debug())
        std::cout << "Producing"
                     "\n";
      return 0;
    });
    auto c = C([&sched](const size_t&) {
      if (sched.debug())
        std::cout << "Consuming"
                     "\n";
    });

    connect(p, c);
    Edge(*p, *c);

    if (debug) {
      p->enable_debug();
      c->enable_debug();
    }

    if (sched.debug())

      std::cout << "==========================================================="
                   "=====\n";

    sched.submit(p);
    sched.submit(c);
    if (sched.debug())

      std::cout << "-----------------------------------------------------------"
                   "-----\n";
    sched.sync_wait_all();

    if (sched.debug())

      std::cout << "==========================================================="
                   "=====\n";

    CHECK(p->produced_items() == problem_size);
    CHECK(c->consumed_items() == problem_size);

    if (debug) {
      CHECK(problem_size == debug_problem_size);
    } else {
      CHECK(problem_size == 1337);
    }
  }
}

#if 1

TEST_CASE("FrugalScheduler: Test passing integers", "[frugal]") {
  bool debug{false};

  //  auto num_threads = GENERATE(1, 2, 3, 4);
  auto num_threads = 1;

  if (debug) {
    problem_size = debug_problem_size;
  }

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

    auto p = producer_node<FrugalMover2, size_t>(
        [&sched, &i, &input](std::stop_source& stop_source) {
          if (sched.debug())
            std::cout << "Producing " + std::to_string(*i) + " with distance " +
                             std::to_string(std::distance(input.begin(), i)) +
                             "\n";
          if (std::distance(input.begin(), i) >=
              static_cast<long>(problem_size)) {
            if (sched.debug()) {
              std::cout << "Requesting stop\n";
            }

            stop_source.request_stop();
            return *(input.begin());
          }

          if (sched.debug())
            std::cout << "producer function returning " + std::to_string(*i) +
                             "\n";

          return *i++;
        });
    auto c = consumer_node<FrugalMover2, size_t>(
        [&j, &output, &debug](std::size_t k) {
          if (debug)
            std::cout << "Consuming " + std::to_string(k) + " with distance " +
                             std::to_string(std::distance(output.begin(), j)) +
                             "\n";

          *j++ = k;
        });

    connect(p, c);
    Edge(*p, *c);

    sched.submit(p);
    sched.submit(c);
    sched.sync_wait_all();
  }
  CHECK(rounds != 0);
  CHECK(rounds == problem_size);

  CHECK(input.begin() != i);
  CHECK(input.size() == rounds);
  CHECK(output.size() == rounds);

  if (debug)
    std::cout << std::distance(input.begin(), i) << std::endl;

  CHECK(std::equal(input.begin(), i, output.begin()));

  if (debug)
    std::cout << *(input.begin()) << " " << *(output.begin()) << std::endl;

  CHECK(std::distance(input.begin(), i) == static_cast<long>(rounds));
}
#endif
