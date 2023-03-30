/**
* @file   experimental/tiledb/common/dag/nodes/detail/resumable/mimo.h
*
* @section LICENSE
*
* The MIT License
*
* @copyright Copyright (c) 2023 TileDB, Inc.
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
* This file declares a multi-input multi-outpu function node class for the
* TileDB task graph library.  A mimo node is a function node that takes
* multiple inputs and multiple outputs.
*
* The general node can also be specialized to provide equivalent functionality
* to a producer or consumer node, respectively, by passing in std::tuple<> for
* the input block type or for the output block type. In either case, a dummy
* argument needs to be used for the associated mover.
*
* @todo Specialize for non-tuple block types.
*
* @todo Specialize for void input/output types (rather than tuple<>) as well as
* for void mover types.
 */

#ifndef TILEDB_DAG_NODES_DETAIL_SEGMENTED_MIMO_H
#define TILEDB_DAG_NODES_DETAIL_SEGMENTED_MIMO_H

#include <functional>
#include <tuple>
#include <type_traits>
#include <utility>

#include "experimental/tiledb/common/dag/ports/ports.h"
#include "experimental/tiledb/common/dag/execution/jthread/stop_token.hpp"
#include "experimental/tiledb/common/dag/nodes/resumable_nodes.h"
#include "experimental/tiledb/common/dag/nodes/detail/resumable/mimo_base.h"
#include "experimental/tiledb/common/dag/nodes/detail/resumable/utils.h"

#include "experimental/tiledb/common/dag/utility/print_types.h"

namespace tiledb::common {

/**
 * Forward declaration of mimo_node_impl.
 */
template <
    template <class>
    class SinkMover,
    class BlocksIn,
    template <class> class SourceMover = SinkMover,
    class BlocksOut = BlocksIn>
class mimo_node_impl;

/*
 * By specializing through the use of `std::tuple`, we are able to have two
 * variadic template lists.  Thus, we can define a node with `size_t` and `int`
 * for its inputs and `size_t` and `double` for its outputs in the following
 * way:
 *
 * @code{.cpp}
 * mimo_node<AsyncMover2, std::tuple<size_t, int>,
 *                     AsyncMover3, std::tuple<size_t, double>> x{};
 * @endcode
 *
 * @todo Variadic list of movers (one per input/output)?
 */
template <
    template <class>
    class SinkMover,
    class... BlocksIn,
    template <class>
    class SourceMover,
    class... BlocksOut>
class mimo_node_impl<
    SinkMover,
    std::tuple<BlocksIn...>,
    SourceMover,
    std::tuple<BlocksOut...>>
    : public mimo_node_impl_base<SinkMover, std::tuple<BlocksIn...>, SourceMover, std::tuple<BlocksOut...>>
{  // @todo Inherit from tuple<Source>, tuple<Sink>?
  using Base = mimo_node_impl_base<SinkMover, std::tuple<BlocksIn...>, SourceMover, std::tuple<BlocksOut...>> ;
  using Base::Base;

  // Some aliases for convenience.  @todo Possible to do this without all the verbiage?
  using scheduler_event_type = typename Base::scheduler_event_type;
  using Base::f_;
  using Base::inputs_;
  using Base::outputs_;
  using Base::input_items_;
  using Base::output_items_;
  using Base::pull_all;
  using Base::drain_all;
  using Base::extract_all;
  using Base::inject_all;
  using Base::fill_all;
  using Base::push_all;
  using Base::stop_all;
  using Base::sink_done_all;
  using Base::source_done_all;
  using Base::is_producer_;
  using Base::is_consumer_;

 public:
  /**
   * Default constructor, for testing only.
   */
  mimo_node_impl() = default;

  /**
   * Function for applying all actions of the node: Pull the sinks, fill the
   * input tuple from sinks, apply stored function, fill the output tuple, and
   * push the sources.
   *
   * @todo Develop better model and API for use of `instruction_counter_` as
   * part of `GeneralNode` object and as interface to scheduler.
   *
   * @note Right now, yielding (waiting) may be done in `Mover` calls (via a
   * `Policy`), i.e., `port_push` and `port_pull`.  Those should interface to
   * the scheduler, e.g., and cause a return from `resume` or `resume` that
   * will then be started from when the scheduler is notified (again, down in
   * the `Mover` policy).
   *
   * @note It would be nice if this could be agnostic here as to what kind of
   * scheduler is being used, but it isn't clear how to do a return in the case
   * of a coroutine-like scheduler, or a condition-variable wait in the case of
   * an `std::async` (or similar) scheduler. Perhaps we could use a return value
   * of `port_pull` or `port_push` to indicate what to do next (yield or
   * continue). Or we could pass in a continuation?  Either approach seems to
   * couple `Mover`, `Scheduler`, and `Node` too much.
   *
   * @todo Maybe we need a many-to-one and a one-to-many `ItemMover`.  We could
   * then put items in the mover rather than in the nodes -- though that would
   * make the nodes almost superfluous.
   *
   */
  scheduler_event_type resume() override {
    std::stop_source stop_source_;

    switch (this->program_counter_) {
      case 0: {
        ++this->program_counter_;

        if constexpr (!is_producer_) {
          auto tmp_state = this->pull_all();

          if (sink_done_all()) {
            return stop_all();
          } else {
            return tmp_state;
          }
        }
      }
        [[fallthrough]];

      case 1: {
        ++this->program_counter_;
        if constexpr (!is_producer_) {
          extract_all(inputs_, input_items_);
          return drain_all();
        }
      }
        [[fallthrough]];

      case 2: {
        ++this->program_counter_;

        /**
         * @todo Should this instead be output_items = f_(input_items_)
         * @todo Maybe use variant?
         * @todo How to deal with `stop_source`?
         */
        if constexpr (is_producer_) {
          output_items_ = f_(stop_source_);
        } else if constexpr (is_consumer_) {
          f_(input_items_);
        } else {
          output_items_ = f_(input_items_);
        }
      }
        [[fallthrough]];

      case 3: {
        ++this->program_counter_;
        if constexpr (!is_consumer_) {
          inject_all(output_items_, outputs_);
          return fill_all();
        }
      }
        [[fallthrough]];

      case 4: {
        ++this->program_counter_;
        if constexpr (!is_consumer_) {
          return push_all();
        }
      }
        [[fallthrough]];

      case 5: {
        this->program_counter_ = 0;
        return scheduler_event_type::yield;
      }

      default: {
        break;
      }
    }

    return scheduler_event_type::yield;
  }

  /** Run the node until it is done. */
  void run() override {
    while (!sink_done_all() && !source_done_all()) {
      resume();
    }
    if constexpr (!is_producer_) {
      if (!sink_done_all()) {
        pull_all();
      }
    }
  }
};

#if 0
// @todo: Define function_node et al as degenerate mimo nodes?

/** A mimo node is a shared pointer to the implementation class */
template <
    template <class>
    class SinkMover,
    class BlockIn,
    template <class> class SourceMover,
    class BlockOut>
struct function_node
    : public std::shared_ptr<
          function_node_impl<SinkMover, BlockIn, SourceMover, BlockOut>> {
  using Base = std::shared_ptr<
      function_node_impl<SinkMover, BlockIn, SourceMover, BlockOut>>;
  using Base::Base;

  template <class Function>
  explicit function_node(Function&& f)
      : Base{std::make_shared<
            function_node_impl<SinkMover, BlockIn, SourceMover, BlockOut>>(
            std::forward<Function>(f))} {
  }

  explicit function_node(
      function_node_impl<SinkMover, BlockIn, SourceMover, BlockOut>& impl)
      : Base{std::make_shared<
            function_node_impl<SinkMover, BlockIn, SourceMover, BlockOut>>(
            std::move(impl))} {
  }
};
#endif
#if 0
template <
    template <class>
    class SinkMover,
    class... BlocksIn,
    template <class>
    class SourceMover,
    class... BlocksOut>
class mimo_node<
    SinkMover,
    std::tuple<BlocksIn...>,
    SourceMover,
    std::tuple<BlocksOut...>>
    : public std::shared_ptr<
          mimo_node_impl<SinkMover, BlocksIn..., SourceMover, BlocksOut...>> {};
#else


template <
    template <class>
    class SinkMover,
    class BlocksIn,
    template <class> class SourceMover = SinkMover,
    class BlocksOut = BlocksIn>
class mimo_node
    : public std::shared_ptr<
          mimo_node_impl<SinkMover, BlocksIn, SourceMover, BlocksOut>> {
  using PreBase = mimo_node_impl<SinkMover, BlocksIn, SourceMover, BlocksOut>;
  using Base = std::shared_ptr<PreBase>;
  using Base::Base;

 public:
  template <class Function>
  explicit mimo_node(Function&& f)
      : Base{std::make_shared<PreBase>(std::forward<Function>(f))} {
  }
};

template <template <class> class SourceMover, class BlocksOut>
using producer_mimo =
    mimo_node<EmptyMover, std::tuple<>, SourceMover, BlocksOut>;

template <template <class> class SinkMover, class BlocksIn>
using consumer_mimo = mimo_node<SinkMover, BlocksIn, EmptyMover, std::tuple<>>;

#endif

}  // namespace tiledb::common
#endif  // TILEDB_DAG_NODES_DETAIL_SEGMENTED_MIMO_H
