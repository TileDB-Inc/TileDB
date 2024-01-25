/**
 * @file pseudo_nodes.h
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
 * This file defines some elementary pseudo node types for testing source and
 * sink ports.
 */

#ifndef TILEDB_DAG_PSEUDO_NODES_H
#define TILEDB_DAG_PSEUDO_NODES_H

#include <atomic>
#include <functional>
#include <type_traits>
#include "../../utility/print_types.h"
#include "../ports.h"

namespace tiledb::common {

class GraphNode {};

/**
 * Prototype producer function object class.  This class generates a sequence of
 * integers from 0 to N (half-open interval).  It will invoke an out-of-data
 * event when the counter hits N.
 */
template <class Block = size_t>
class generators {
  std::atomic<Block> N_{0};
  std::atomic<Block> i_{0};

 public:
  generators(Block N)
      : N_{N} {
  }
  generators(const generators& rhs)
      : N_(rhs.N_.load())
      , i_(rhs.i_.load()) {
  }

  Block operator()() {
    return i_++;
  }
};

/**
 * Prototype source node class.  Constructed with a function that creates
 * Blocks.
 */
template <template <class> class Mover_T, class Block>
class ProducerNode : public Source<Mover_T, Block> {
  using Base = Source<Mover_T, Block>;
  using source_type = Source<Mover_T, Block>;
  // This causes initialization problems
  //  std::atomic<size_t> i_{0};
  size_t N_{0};
  std::function<Block()> f_;

 public:
  /**
   * Constructor
   * @param f A function that accepts items.
   * @tparam The type of the function (or function object) that generates items.
   */
  template <class Function>
  explicit ProducerNode(Function&& f)

      /*
       * It would be nice to guard this constructor so that only functions or
       * function objects are accepted.  Unfortunately, is_function does not
       * work for bind or for function objects.
       *
       * typename std::enable_if<
       * std::is_function_v<std::remove_reference_t<decltype(f)>> ||
       * std::is_bind_expression_v<std::remove_reference_t<decltype(f)>> ||
       * std::is_member_function_pointer_v<
       * std::remove_reference_t<decltype(f)>::operator()>, void**>::type =
       * nullptr
       */

      /*
       * It would be nice to guard this constructor so that only functions or
       * function objects are accepted.  Unfortunately, is_function does not
       * work for bind or for function objects.
       *
       * typename std::enable_if<
       * std::is_function_v<std::remove_reference_t<decltype(f)>> ||
       * std::is_bind_expression_v<std::remove_reference_t<decltype(f)>> ||
       * std::is_member_function_pointer_v<
       * std::remove_reference_t<decltype(f)>::operator()>, void**>::type =
       * nullptr
       */

      : f_{std::forward<Function>(f)} {
    //    print_types(f, f_);
    //    print_types(f, f_);
  }

  /**
   * Trivial default constructor, for testing.
   */
  ProducerNode() = default;
  //  ProducerNode(const ProducerNode&) = default;
  ProducerNode(const ProducerNode&) {
  }

  /**
   * Submit an item to be transferred to correspondent_ Sink.  Blocking.  The
   * behavior of `submit` and `try_submit` will depend on the policy associated
   * with the state machine.  Used by dag nodes and edges for transferring data.
   *
   */
  void get() {
    //  Don't need the guard since { state == st_01 ∨ state == st_00}
    //  at end of function and sink cannot change that.
    //  while (!source_is_empty())
    //    ;
    //
    //  { state == st_00 ∨ state == st_01 }
    //  produce source item
    //  inject source item
    auto state_machine = this->get_mover();

    Base::inject(f_());
    state_machine->port_fill();
    //  { state == st_10 ∨ state == st_11 }

    state_machine->port_push();
    //  { state == st_01 ∨ state == st_00 }
  }

  /**
   * Submit an item to be transferred to correspondent_ Sink.  Non-blocking. The
   * behavior of `submit` and `try_submit` will depend on the policy associated
   * with the state machine.  Used by dag nodes and edges for transferring data.
   *
   * @todo Investigate policy with non-blocking variant of `port_push`.  Will
   * require addtional `try_push` events, `try_swap` methods, updated tables,
   * and `event()` will need to return a bool.
   *
   */
  bool try_get() {
    //  Don't need the guard since { state == st_01 ∨ state == st_00}
    //  at end of function and sink cannot change that.
    //  while (!source_is_empty())
    //    ;
    //
    //  { state == st_00 ∨ state == st_01 }
    //  produce source item
    //  inject source item
    //  port_fill();
    //  { state == st_10 ∨ state == st_11 ∨ state == st_00
    //    ∨  state == st_01 }
    //  port_push(); // could have non-blocking try_push() event, but that would
    //             // leave the item  the item injected -- on failure, could
    //             // reject item -- would also need try_swap action.
    //  { state == st_01 ∨ state == st_00}
    return false;
  }
};

/**
 * Consumer function object class.  Takes items and puts them on an Output
 * Iterator.
 */
template <class OutputIterator, class Block = size_t>
class consumer {
  OutputIterator iter_;

 public:
  explicit consumer(OutputIterator iter)
      : iter_(iter) {
  }
  void operator()(Block& item) {
    *iter_++ = item;
  }
};

/**
 * A proto consumer node.  Constructed with a function that accepts Blocks.
 */
template <template <class> class Mover_T, class Block>
class ConsumerNode : public Sink<Mover_T, Block> {
  std::function<void(Block&)> f_;

 public:
  using Base = Sink<Mover_T, Block>;

 public:
  /**
   * Constructor
   * @param f A function that accepts items.
   * @tparam The type of the function (or function object) that accepts items.
   */
  template <class Function>
  explicit ConsumerNode(

      Function&& f
      /*
,
      typename std::enable_if<
          std::is_function_v<std::remove_reference_t<decltype(f)>> ||
              std::is_bind_expression_v<std::remove_reference_t<decltype(f)>>,
          void**>::type = nullptr)
      */

      /*
,
      typename std::enable_if<
          std::is_function_v<std::remove_reference_t<decltype(f)>> ||
              std::is_bind_expression_v<std::remove_reference_t<decltype(f)>>,
          void**>::type = nullptr)
      */
      )
      : f_{std::forward<Function>(f)} {
  }

  /**
   * Trivial default constructor, for testing.
   */
  ConsumerNode() = default;
  //  ConsumerNode(const ConsumerNode&) = default;
  ConsumerNode(const ConsumerNode&) {
  }
  ConsumerNode(ConsumerNode&&) = default;

  /**
   * Retrieve `item_` from the Sink.  Blocking.  The behavior of `retrieve` and
   * `try_retrieve` will depend on the policy associated with the state machine.
   * Used by dag nodes and edges for transferring data.
   *
   * @return The retrieved `item_`.  The return value will be empty if
   * the `item_` is empty.
   */
  void put() {
    //  Guard does not seem necessary
    //  while (!sink_is_empty())
    //    ;
    //  { state == st_00 ∨ state == st_10 }
    auto state_machine = this->get_mover();

    state_machine->port_pull();
    //  { state == st_01 ∨ state == st_11 }
    //  extract sink item
    //  invoke consumer function
    auto b = Base::extract();
    CHECK(b.has_value());
    f_(*b);

    state_machine->port_drain();
    //  { state == st_00 ∨ state == st_10 ∨ state == st_01 ∨
    //  state == st_11 } return item;
  }

  /**
   * Retrieve `item_` from the Sink.  Non-blocking.  The behavior of `retrieve`
   * and `try_retrieve` will depend on the policy associated with the state
   * machine.  Used by dag nodes and edges for transferring data.
   *
   * @todo Investigate policy with non-blocking variant of `port_pull`.  Will
   * require addtional `try_pull` events, `try_swap` methods, updated tables,
   * and `event()` will need to return a bool.
   *
   * @return The retrieved `item_`.  The return value will be empty if
   * the `item_` was not able to be retrieved.
   */
  void try_put() {
    //  if (sink_is_empty()) {
    //    { state == st_00 ∨ state == st_10 }
    //    port_pull();
    //    extract sink item
    //    invoke consumer function
    //    port_drain();
    //    return item;
    //  }
  }
};

/**
 * Purely notional proto `function_node`.  Constructed with function that
 * accepts an item and returns an item.
 */
template <
    template <class>
    class SinkMover_T,
    class BlockIn,
    template <class> class SourceMover_T = SinkMover_T,
    class BlockOut = BlockIn>
class FunctionNode : public Source<SourceMover_T, BlockOut>,
                     public Sink<SinkMover_T, BlockIn> {
  std::function<BlockOut(BlockIn)> f_;
  using SourceBase = Source<SourceMover_T, BlockOut>;
  using SinkBase = Sink<SinkMover_T, BlockIn>;

 public:
  template <class Function>
  FunctionNode(Function&& f)

      /*
       * It would be nice to guard this constructor so that only functions or
       * function objects are accepted.  Unfortunately, is_function does not
       * work for bind or for function objects.
       *
       * typename std::enable_if<
       * std::is_function_v<std::remove_reference_t<decltype(f)>> ||
       * std::is_bind_expression_v<
       * std::remove_reference_t<decltype(f)>::operator()>,
       * void**>::type = nullptr
       */

      : f_{std::forward<Function>(f)} {
  }

  /**
   * Trivial default constructor, for testing.
   */
  FunctionNode() = default;
  FunctionNode(const FunctionNode&) {
  }
  FunctionNode(FunctionNode&&) = default;

  void run() {
    auto source_state_machine = SourceBase::get_mover();
    auto sink_state_machine = SinkBase::get_mover();

    sink_state_machine->port_pull();

    auto b = SinkBase::extract();
    CHECK(b.has_value());

    auto j = f_(*b);

    sink_state_machine->port_drain();

    SourceBase::inject(j);

    source_state_machine->port_fill();
    source_state_machine->port_push();
  }
};

}  // namespace tiledb::common
#endif  // TILEDB_DAG_PSEUDO_NODES_H
