/**
 * @file   frugal_nodes.h
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
 * This file defines nodes for a throw-catch scheduler for dag.
 */

#include <atomic>
#include <iostream>
#include <memory>
#include "experimental/tiledb/common/dag/execution/jthread/stop_token.hpp"
#include "experimental/tiledb/common/dag/ports/ports.h"

#include "experimental/tiledb/common/dag/utils/print_types.h"

namespace tiledb::common {

template <template <class> class Mover, class T>
struct producer_node_impl;
template <template <class> class Mover, class T>
struct consumer_node_impl;
template <
    template <class>
    class SinkMover,
    class BlockIn,
    template <class>
    class SourceMover,
    class BlockOut>
struct function_node_impl;

struct node_base {
  using node_type = std::shared_ptr<node_base>;

  bool debug_{false};

  size_t id_;
  size_t program_counter_{0};

  node_type sink_correspondent_{nullptr};
  node_type source_correspondent_{nullptr};

  virtual node_type& sink_correspondent() {
    return sink_correspondent_;
  }

  virtual node_type& source_correspondent() {
    return source_correspondent_;
  }

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
  //  print_types(from, to, from.sink_correspondent(),
  //  to.source_correspondent());

  (*from).sink_correspondent() = to;
  (*to).source_correspondent() = from;
}

static size_t problem_size = 1337;
static size_t debug_problem_size = 3;

std::atomic<size_t> id_counter{0};

// std::atomic<bool> item_{false};

template <template <class> class Mover, class T>
struct producer_node_impl : public node_base, public Source<Mover, T> {
  using mover_type = Mover<T>;
  using node_base_type = node_base;

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
      : node_base_type(id_counter++)
      , f_{std::forward<Function>(f)}
      , produced_items_{0} {
  }

  producer_node_impl(producer_node_impl&& rhs) = default;

  std::string name() {
    return {"producer"};
  }

  void enable_debug() {
    node_base_type::enable_debug();
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
    assert (!stop_source_.stop_requested());

    switch (this->program_counter_) {
      case 0: {
        ++this->program_counter_;

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
        ++this->program_counter_;

        SourceBase::inject(thing);
      }
        [[fallthrough]];

      case 2: {
        this->program_counter_ = 3;
        mover->port_fill();
      }
        [[fallthrough]];

      case 3: {
        this->program_counter_ = 4;
      }
        [[fallthrough]];

      case 4: {
        this->program_counter_ = 5;
        mover->port_push();
      }
        [[fallthrough]];

        // @todo Should skip yield if push waited;
      case 5: {
        this->program_counter_ = 0;
        //        this->task_yield(*this);
        // yield();
        break;
      }

      default:
        break;
    }
  }

  //  using sink_correspondent_type = consumer_node_impl<Mover, T>;

  // sink_correspondent_type sink_correspondent_{nullptr};

  // sink_correspondent_type& sink_correspondent() {
  // return sink_correspondent_;
  // }
};

template <template <class> class Mover, class T>
struct consumer_node_impl : public node_base, public Sink<Mover, T> {
  using mover_type = Mover<T>;
  using node_base_type = node_base;

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
      : node_base_type(id_counter++)
      , f_{std::forward<Function>(f)}
      , consumed_items_{0} {
  }

  std::string name() {
    return {"consumer"};
  }

  void enable_debug() {
    node_base_type::enable_debug();
    if (this->item_mover_)
      this->item_mover_->enable_debug();
  }

  T thing{};

  void resume() {
    auto mover = SinkBase::get_mover();

    if (this->debug())
      std::cout << this->name() + " node " + std::to_string(this->id()) +
                       " resuming with " + std::to_string(consumed_items_) +
                       " consumed_items" + "\n";

    [[maybe_unused]] std::thread::id this_id = std::this_thread::get_id();


    if (mover->is_done()) {

      if (this->debug())
	std::cout
	  << this->name() + " node " + std::to_string(this->id()) +
	  " got mover done in consumer at top of resume -- returning\n";
      
      mover->port_exhausted();

      return;
    }



    switch (this->program_counter_) {
      case 0: {
        ++this->program_counter_;
        mover->port_pull();

	if (is_done(mover->state()) == "") {
	  std::cout << "=== sink mover done\n";
	  // mover->on_term_sink();
	  //	  throw(detail::frugal_exit{detail::frugal_target::sink});//	  
	  //	  mover->port_done();
	}

      }
        [[fallthrough]];

        /*
         * To make the flow here similar to producer, we start with pull() the
         * first time we are called but thereafter the loop goes from 1 to 5;
         */
      case 1: {
        ++this->program_counter_;

        thing = *(SinkBase::extract());

        //        if (this->debug())
        //          std::cout << "function thing is " + std::to_string(thing) +
        //          "\n";
      }
        [[fallthrough]];

      case 2: {
        ++this->program_counter_;

        mover->port_drain();
      }
        [[fallthrough]];

      case 3: {
        ++this->program_counter_;

        assert(this->source_correspondent_ != nullptr);
      }
        [[fallthrough]];

      case 4: {
        ++this->program_counter_;

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

        //        if (this->debug())
        //          std::cout << "next function thing is " +
        //          std::to_string(thing) + "\n";

        f_(thing);
      }

        // @todo Should skip yield if pull waited;
      case 5: {
        ++this->program_counter_;

        mover->port_pull();
      }

      case 6: {
        this->program_counter_ = 1;
        //        this->task_yield(*this);
        // yield();
        break;
      }
      default: {
        break;
      }
    }
  }

  //  using source_correspondent_type = producer_node_impl<Mover, T>;

  //  source_correspondent_type source_correspondent_{nullptr};

  //  source_correspondent_type& source_correspondent() {
  //    return source_correspondent_;
  //  }
};

template <
    template <class>
    class SinkMover,
    class BlockIn,
    template <class> class SourceMover = SinkMover,
    class BlockOut = BlockIn>
struct function_node_impl : public node_base,
                            public Sink<SinkMover, BlockIn>,
                            public Source<SourceMover, BlockOut> {
  using sink_mover_type = SinkMover<BlockIn>;
  using source_mover_type = SourceMover<BlockOut>;
  using node_base_type = node_base;

  using SinkBase = Sink<SinkMover, BlockIn>;
  using SourceBase = Source<SourceMover, BlockOut>;

  std::function<BlockOut(const BlockIn&)> f_;

  std::atomic<size_t> processed_items_{0};

  size_t processed_items() {
    return processed_items_.load();
  }

  template <class Function>
  function_node_impl(
      Function&& f,
      std::enable_if_t<
          std::is_invocable_r_v<BlockOut, Function, const BlockIn&>,
          void**> = nullptr)
      : node_base_type(id_counter++)
      , f_{std::forward<Function>(f)}
      , processed_items_{0} {
  }

  std::string name() {
    return {"function"};
  }

  void enable_debug() {
    node_base_type::enable_debug();
    if (SinkBase::item_mover_)
      SinkBase::item_mover_->enable_debug();
    if (SourceBase::item_mover_)
      SourceBase::item_mover_->enable_debug();
  }

  BlockIn in_thing{};
  BlockOut out_thing{};

  void resume() {
    auto source_mover = SourceBase::get_mover();
    auto sink_mover = SinkBase::get_mover();

    if (this->debug())
      std::cout << this->name() + " node " + std::to_string(this->id()) +
                       " resuming at program counter = " +
                       std::to_string(this->program_counter_) + " and " +
                       std::to_string(processed_items_) + " consumed_items\n";

    [[maybe_unused]] std::thread::id this_id = std::this_thread::get_id();

    if (source_mover->is_done() || sink_mover->is_done()) {

      if (this->debug())
	std::cout
	  << this->name() + " node " + std::to_string(this->id()) +
	  " got sink_mover done at top of resumes -- returning\n";
      
      source_mover->port_exhausted();

      return;
    }



    switch (this->program_counter_) {
      case 0: {
        ++this->program_counter_;
        sink_mover->port_pull();

	//        if (sink_mover->is_done()) {
        if (source_mover->is_done() || sink_mover->is_done()) {
          if (this->debug())
            std::cout
                << this->name() + " node " + std::to_string(this->id()) +
                       " got sink_mover done -- going to exhaust source\n";

          source_mover->port_exhausted();
          break;
        }
      }
        [[fallthrough]];

      case 1: {
        ++this->program_counter_;

        in_thing = *(SinkBase::extract());

        //        if (this->debug())
        //          std::cout << "function in_thing is " +
        //          std::to_string(in_thing) +
        //                           "\n";
      }
        [[fallthrough]];

      case 2: {
        ++this->program_counter_;

        sink_mover->port_drain();
      }
        [[fallthrough]];

      case 3: {
        ++this->program_counter_;

        assert(this->source_correspondent() != nullptr);
        assert(this->sink_correspondent() != nullptr);
      }
        [[fallthrough]];

      case 4: {
        ++this->program_counter_;

        if (processed_items_++ >= problem_size) {
          //                    if (this->debug())
          std::cout << "THIS SHOULD NOT HAPPEN " + this->name() + " node " +
                           std::to_string(this->id()) +
                           " has produced enough items -- calling "
                           "port_exhausted with " +
                           std::to_string(processed_items_) +
                           " consumed items and " +
                           std::to_string(problem_size) + " problem size\n";
          //          assert(false);

          sink_mover->port_exhausted();
          break;
        }

        //        if (this->debug())
        //          std::cout << "next processed thing is " +
        //          std::to_string(in_thing) +
        //"\n";
        out_thing = f_(in_thing);
      }

        // inject / fill / push

      case 5: {
        ++this->program_counter_;
        SourceBase::inject(out_thing);
      }
        [[fallthrough]];

      case 6: {
        ++this->program_counter_;
        source_mover->port_fill();
      }
        [[fallthrough]];

      case 7: {
        ++this->program_counter_;
      }
        [[fallthrough]];

      case 8: {
        ++this->program_counter_;
        source_mover->port_push();
      }
        [[fallthrough]];

        // @todo Should skip yield if push waited;
      case 9: {
        // this->program_counter_ = 0;
        //        this->task_yield(*this);
        // yield();
	//        break;
      }
        [[fallthrough]];

      case 10: {
	this->program_counter_ = 0;

	if (source_mover->is_done() || sink_mover->is_done()) {
	  break;
	}
      }
        [[fallthrough]];

      default: {
        break;
      }
    }
  }
  // using sink_correspondent_type = consumer_node_impl<SinkMover, BlockIn>*;
  // using source_correspondent_type = producer_node_impl<SourceMover,
  // BlockOut>*;

  // sink_correspondent_type sink_correspondent_{nullptr};
  // source_correspondent_type source_correspondent_{nullptr};

  // sink_correspondent_type& sink_correspondent() {
  //   return sink_correspondent_;
  // }

  // source_correspondent_type& source_correspondent() {
  //   return source_correspondent_;
  // }
};

template <template <class> class Mover, class T>
struct consumer_node;

template <template <class> class Mover, class T>
struct producer_node;

template <
    template <class>
    class SinkMover,
    class BlockIn,
    template <class>
    class SourceMover,
    class BlockOut>
struct function_node;

template <class T>
struct correspondent_traits {};

#if 0

template <template <class> class Mover, class T>
struct correspondent_traits<consumer_node_impl<Mover, T>> {
  using type = std::shared_ptr<producer_node_impl<Mover, T>>;
};

template <template <class> class Mover, class T>
struct correspondent_traits<producer_node_impl<Mover, T>> {
  using type = std::shared_ptr<consumer_node_impl<Mover, T>>;
};

template <template <class> class Mover, class T>
struct correspondent_traits<consumer_node<Mover, T>> {
  using type = producer_node<Mover, T>;
};

template <template <class> class Mover, class T>
struct correspondent_traits<producer_node<Mover, T>> {
  using type = consumer_node<Mover, T>;
};
#endif

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

template <
    template <class>
    class SinkMover,
    class BlockIn,
    template <class> class SourceMover = SinkMover,
    class BlockOut = BlockIn>
struct function_node
    : public std::shared_ptr<
          function_node_impl<SinkMover, BlockIn, SourceMover, BlockOut>> {
  using Base = std::shared_ptr<
      function_node_impl<SinkMover, BlockIn, SourceMover, BlockOut>>;
  using Base::Base;

  template <class Function>
  function_node(Function&& f)
      : Base{std::make_shared<
            function_node_impl<SinkMover, BlockIn, SourceMover, BlockOut>>(
            std::forward<Function>(f))} {
  }

  function_node(
      function_node_impl<SinkMover, BlockIn, SourceMover, BlockOut>& impl)
      : Base{std::make_shared<
            function_node_impl<SinkMover, BlockIn, SourceMover, BlockOut>>(
            std::move(impl))} {
  }
};
}  // namespace tiledb::common
