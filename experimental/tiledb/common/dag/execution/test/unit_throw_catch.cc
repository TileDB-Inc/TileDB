/**
 * @file   unit_throw_catch.cc
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

#include "unit_throw_catch.h"
#include <atomic>
#include <cassert>
#include <iostream>
#include <map>
#include "../throw_catch.h"
#include "experimental/tiledb/common/dag/state_machine/test/helpers.h"

using namespace tiledb::common;

struct node;

template <class T>
struct port;

template <class T>
struct sender;

template <class T>
struct receiver;

void wait(node* n);

void notify(node* n);

void push(node* n);

void pull(node* n);

template <class T>
struct port {};

template <class T>
struct sender : public port<T> {};

template <class T>
struct receiver : public port<T> {};

#if 0
template <class Task>
struct cond_var {
  std::queue<node*> waiters;
  ThrowCatchScheduler* scheduler;

  template <class Lock>
  void wait(node* n, Lock* lock) {
  }

  void signal_one() {
  }
};
#endif

struct node {
  size_t id_;
  TaskState node_state_{TaskState::created};
  TaskEvent task_event_{TaskEvent::admitted};
  size_t program_counter_{0};

  node* correspondent_{nullptr};

  inline TaskState node_state() const {
    return node_state_;
  }

  inline TaskState set_node_state(TaskState st) {
    node_state_ = st;
    return st;
  }

  inline TaskEvent task_event() const {
    return task_event_;
  }

  inline TaskEvent set_task_event(TaskEvent ev) {
    task_event_ = ev;
    return task_event_;
  }

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
};

void connect(node& from, node& to) {
  from.correspondent_ = &to;
  to.correspondent_ = &from;
}

std::atomic<size_t> id_counter{0};

static std::atomic<size_t> produced_items{0};
template <class T>
struct producer_node : public node, public sender<T> {
  std::function<T()> f_;

  template <class Function>
  producer_node(Function&& f)
      : node(id_counter++)
      , f_{std::forward<Function>(f)} {
  }

  void inject(T) {
    std::cout << "producer_node " << id_ << " injecting" << std::endl;
  }

  void fill() {
    std::cout << "producer_node " << id_ << " filling" << std::endl;
  }

  std::string name() {
    return {"producer"};
  }

  node* resume() {
    std::cout << this->name() << " node " << this->id() << " resuming with "
              << produced_items << " produced_items" << std::endl;

    if (produced_items++ >= 3) {
      std::cout << this->name() << " node " << this->id()
                << " is done -- setting event to exit" << std::endl;
      this->task_event_ = TaskEvent::exit;
      notify(this->correspondent_);
      return this;
    }

    [[maybe_unused]] std::thread::id this_id = std::this_thread::get_id();

    decltype(f_()) thing;

    switch (program_counter_) {
      case 0: {
        program_counter_ = 1;
        thing = f_();
      }
        [[fallthrough]];
      case 1: {
        program_counter_ = 2;
        inject(T{});
      }
        [[fallthrough]];

      case 2: {
        program_counter_ = 3;
        fill();
      }
        [[fallthrough]];

      case 3: {
        program_counter_ = 4;
        assert(this->correspondent_ != nullptr);
        notify(this->correspondent_);
      }
        [[fallthrough]];

      case 4: {
        program_counter_ = 5;
        push(this);
      }
        [[fallthrough]];

        // @todo Should skip yield if push waited;
      case 5: {
        program_counter_ = 0;
        task_event_ = TaskEvent::yield;
        // yield();
        break;
      }
        [[fallthrough]];
      default:
        std::cout << "default " << str(this->node_state_) << std::endl;
    }
    return this;
  }
};

static std::atomic<size_t> consumed_items{0};

template <class T>
struct consumer_node : public node, public receiver<T> {
  std::function<void(const T&)> f_;

  template <class Function>
  consumer_node(Function&& f)
      : node(id_counter++)
      , f_{std::forward<Function>(f)} {
  }

  T extract() {
    std::cout << "consumer_node " << id_ << " extracting" << std::endl;
    return {};
  }

  void drain() {
    std::cout << "consumer_node " << id_ << " draining" << std::endl;
  }

  std::string name() {
    return {"consumer"};
  }

  node* resume() {
    std::cout << this->name() << " node " << this->id() << " resuming with "
              << consumed_items << " consumed_items" << std::endl;

    if (consumed_items++ >= 3) {
      std::cout << this->name() << " node " << this->id()
                << " is done -- setting event to exit" << std::endl;
      this->task_event_ = TaskEvent::exit;
      return this;
    }

    [[maybe_unused]] std::thread::id this_id = std::this_thread::get_id();

    T thing;

    switch (program_counter_) {
      case 0: {
        ++program_counter_;
        pull(this);
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

        drain();
      }
        [[fallthrough]];

      case 3: {
        ++program_counter_;

        assert(this->correspondent_ != nullptr);
        notify(this->correspondent_);
      }
        [[fallthrough]];

      case 4: {
        ++program_counter_;

        f_(thing);
      }

        // @todo Should skip yield if pull waited;
      case 5: {
        ++program_counter_;

        pull(this);
      }

      case 6: {
        program_counter_ = 1;
        task_event_ = TaskEvent::yield;
        // yield();
        break;
      }
      default: {
        std::cout << "default " << str(this->node_state_) << std::endl;
      }
    }
    return this;
  }
};

void pull(node* n) {
  std::cout << n->name() << " node " << n->id() << " pulling with "
            << str(n->node_state()) << " and " << str(n->task_event())
            << std::endl;

  if (n->task_event() == TaskEvent::notify) {
    std::cout << n->name() << " node " << n->id()
              << " has been notifed -- setting state to dispatch" << std::endl;

    n->set_task_event(TaskEvent::dispatch);
    return;
  }

  std::cout << n->name() << " node " << n->id()
            << " setting to wait and throwing" << std::endl;

  n->set_task_event(TaskEvent::wait);
  throw(n);
}

void push(node* n) {
  std::cout << n->name() << " node " << n->id() << " pushing with "
            << str(n->node_state()) << " and " << str(n->task_event())
            << std::endl;

  if (n->task_event() == TaskEvent::notify) {
    std::cout << n->name() << " node " << n->id() << " has been notifed "
              << std::endl;
    n->set_task_event(TaskEvent::dispatch);
    return;
  }

  std::cout << n->name() << " node " << n->id()
            << " setting to wait and throwing" << std::endl;

  n->set_task_event(TaskEvent::wait);
  throw(n);
}

void notify(node* n) {
  std::cout << n->name() << " node " << n->id() << " being notified with "
            << str(n->node_state()) << " and " << str(n->task_event())
            << std::endl;
  n->set_task_event(TaskEvent::notify);
}

#if 0
TEST_CASE("ThrowCatchScheduler: Test construct", "[throw_catch]") {
  [[maybe_unused]] auto sched = ThrowCatchScheduler<node>(1);
  // sched goes out of scope and shuts down the scheduler
}

TEST_CASE("ThrowCatchScheduler: Test create nodes", "[throw_catch]") {
  [[maybe_unused]] auto sched = ThrowCatchScheduler<node>(1);

  auto p = producer_node<size_t>([]() { return 0; });
  auto c = consumer_node<size_t>([](const size_t&) {});
}

TEST_CASE("ThrowCatchScheduler: Test connect nodes", "[throw_catch]") {
  [[maybe_unused]] auto sched = ThrowCatchScheduler<node>(1);

  auto p = producer_node<size_t>([]() { return 0; });
  auto c = consumer_node<size_t>([](const size_t&) {});

  connect(p, c);
}

TEST_CASE("ThrowCatchScheduler: Test submit nodes", "[throw_catch]") {
  [[maybe_unused]] auto sched = ThrowCatchScheduler<node>(1);

  auto p = producer_node<size_t>([]() { return 0; });
  auto c = consumer_node<size_t>([](const size_t&) {});

  connect(p, c);
  sched.submit(&p);
  sched.submit(&c);
}
#endif

TEST_CASE("ThrowCatchScheduler: Test submit and wait nodes", "[throw_catch]") {
  [[maybe_unused]] auto sched = ThrowCatchScheduler<node>(1);

  auto p = producer_node<size_t>([]() {
    std::cout << "Producing" << std::endl;
    return 0;
  });
  auto c = consumer_node<size_t>(
      [](const size_t&) { std::cout << "Consuming" << std::endl; });

  connect(p, c);
  sched.submit(&p);
  sched.submit(&c);
  sched.sync_wait_all();
}

TEST_CASE("ThrowCatchScheduler: Test run nodes", "[throw_catch]") {
}
