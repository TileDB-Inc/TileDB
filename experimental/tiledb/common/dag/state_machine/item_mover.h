/**
 * @file item_mover.h
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
 * Implementation of finite state machine policies.
 *
 * The different policies currently include an extensive amount of debugging
 * code.
 *
 * @todo Remove the debugging code.
 */

#ifndef TILEDB_DAG_ITEM_MOVER_H
#define TILEDB_DAG_ITEM_MOVER_H

#include <array>
#include <cassert>
#include <optional>
#include <string>
#include <tuple>
#include "experimental/tiledb/common/dag/state_machine/test/helpers.h"
#include "experimental/tiledb/common/dag/utils/print_types.h"
#include "experimental/tiledb/common/dag/utils/traits.h"

namespace tiledb::common {

template <class Mover, class PortState>
class AsyncPolicy;

template <class Mover, class PortState>
class UnifiedAsyncPolicy;

/**
 * Base class for data movers.  The base class contains the actual data items as
 * well as an `on_move` member functions for moving data along the pipeline.
 */
template <class Mover, class PortState, class Block>
class BaseMover;

/**
 * Specialization of the `BaseMover` class for three stages.
 */
template <class Mover, class Block>
class BaseMover<Mover, three_stage, Block> {
  using item_type = std::optional<Block>;
  using PortState = three_stage;

  std::array<item_type*, 3> items_;

 protected:
  /**
   * Record keeping of how many moves are made.  Used for diagnostics and
   * debugging.
   */
  std::array<size_t, 3> moves_;

 public:
  BaseMover() = default;
  BaseMover(item_type& source_init, item_type& edge_init, item_type& sink_init)
      : items_{&source_init, &edge_init, &sink_init} {
  }

  /**
   * Register items to be moved with the data mover.  In the context of TileDB
   * task graph, this will generally be a source, an edge intermediary, and a
   * sink.
   *
   * @pre Called under lock.
   */
  void register_items(
      item_type& source_item, item_type& edge_item, item_type& sink_item) {
    items_[0] = &source_item;
    items_[1] = &edge_item;
    items_[2] = &sink_item;
  }

  /**
   * Deegister items.
   *
   * @pre Called under lock.
   */
  void deregister_items(
      item_type& source_item, item_type& edge_item, item_type& sink_item) {
    if (items_[0] != &source_item || items_[1] != &edge_item ||
        items_[2] != &sink_item) {
      throw std::runtime_error(
          "Attempting to deregister source, edge, or sink items that were not "
          "registered.");
    }
    for (auto& j : items_) {
      j->reset();
    }
  }

  /**
   * Do the actual data movement.  We move all items in the pipeline towards
   * the end so that there are no "holes".
   *
   * @pre Called under lock
   */
  inline void on_move(std::atomic<int>& event) {
    //    auto state = this->state();
    auto state = static_cast<Mover*>(this)->state();
    bool debug = static_cast<Mover*>(this)->debug_enabled();
    CHECK(
        (state == PortState::st_010 || state == PortState::st_100 ||
         state == PortState::st_101 || state == PortState::st_110));

    if (static_cast<Mover*>(this)->debug_enabled()) {
      std::cout << event << "  "
                << " source swapping items with " + str(state) + " and " +
                       str(static_cast<Mover*>(this)->next_state())
                << std::endl;

      std::cout << event;
      std::cout << "    "
                << "Action on_move state = (";
      for (auto& j : items_) {
        // print_types(j);
        std::cout << " "
                  << (j == nullptr && j->has_value() ?
                          std::to_string(j->value()) :
                          "no_value")
                  << " ";
      }
      std::cout << ") -> ";
    }

    /*
     * Do the moves, always pushing all elements as far forward as possible.
     */
    switch (state) {
      case PortState::st_101:
        CHECK(*(items_[0]) != EMPTY_SINK);
        std::swap(*items_[0], *items_[1]);
        break;

      case PortState::st_010:
        CHECK(*(items_[1]) != EMPTY_SINK);
        std::swap(*items_[1], *items_[2]);
        break;

      case PortState::st_100:
        std::swap(*items_[0], *items_[1]);
        std::swap(*items_[1], *items_[2]);
        break;

      case PortState::st_110:
        CHECK(*(items_[1]) != EMPTY_SINK);
        std::swap(*items_[1], *items_[2]);
        CHECK(*(items_[0]) != EMPTY_SINK);
        std::swap(*items_[0], *items_[1]);
        break;

      default:
        if (debug) {
          std::cout << "???" << std::endl;
        }
        break;
    }
    if (debug) {
      std::cout << "(";
      for (auto& j : items_) {
        std::cout << " "
                  << (j == nullptr && j->has_value() ?
                          std::to_string(j->value()) :
                          "no_value")
                  << " ";
      }
      std::cout << ")" << std::endl;
      std::cout << event << "  "
                << " source done swapping items with " +
                       str(static_cast<Mover*>(this)->state()) + " and " +
                       str(static_cast<Mover*>(this)->next_state())
                << std::endl;
      ++event;
    }
  }

 private:
  void debug_msg(const std::string& msg) {
    if (static_cast<Mover*>(this)->debug_enabled()) {
      std::cout << msg << std::endl;
    }
  }
};

/**
 * Specialization of the `BaseMover` class for two stages.
 */
template <class Mover, class Block>
class BaseMover<Mover, two_stage, Block> {
  using PortState = two_stage;
  using item_type = std::optional<Block>;

  /*
   * Array to hold pointers to items we will be responsible for moving.
   */
  std::array<item_type*, 2> items_;

 protected:
  /*
   * Array to hold move counts.  Used for diagnostics and profiling.
   */
  std::array<size_t, 2> moves_{0, 0};

 public:
  BaseMover() = default;
  BaseMover(item_type& source_init, item_type& sink_init)
      : items_{&source_init, &sink_init} {
  }

  /**
   * Register items with data mover.  In the context of TileDB
   * task graph, this will generally be a source and a sink.
   *
   * @pre Called under lock
   */
  void register_items(item_type& source_item, item_type& sink_item) {
    items_[0] = &source_item;
    items_[1] = &sink_item;
  }

  /**
   * Deregister the items.
   */
  void deregister_items(item_type& source_item, item_type& sink_item) {
    if (items_[0] != &source_item || items_[1] != &sink_item) {
      throw std::runtime_error(
          "Attempting to deregister source or sink items that were not "
          "registered.");
    }
    for (auto& j : items_) {
      j->reset();
    }
  }

  /**
   * Do the actual data movement.  We move all items in the pipeline towards
   * the end so that there are no "holes".  In the case of two stages this
   * basically means that we are in state 10 and we swap to state 01.
   *
   * @pre Called under lock
   */
  inline void on_move(std::atomic<int>& event) {
    auto state = static_cast<Mover*>(this)->state();
    bool debug = static_cast<Mover*>(this)->debug_enabled();
    CHECK(state == two_stage::st_10);

    /**
     * Increment the move count.
     *
     * @todo Distinguish between source and sink
     */
    moves_[0]++;

    if (static_cast<Mover*>(this)->debug_enabled()) {
      std::cout << event << "  "
                << " source swapping items with " + str(state) + " and " +
                       str(static_cast<Mover*>(this)->next_state())
                << std::endl;

      std::cout << event;
      std::cout << "    "
                << "Action on_move state = (";
      for (auto& j : items_) {
        //        print_types(j);

        if constexpr (has_to_string_v<decltype(j->value())>) {
          std::cout << " "
                    << (j == nullptr && j->has_value() ?
                            std::to_string(j->value()) :
                            "no_value")
                    << " ";
        }
      }
      std::cout << ") -> ";
    }

    std::swap(*items_[0], *items_[1]);

    if (debug) {
      std::cout << "(";
      for (auto& j : items_) {
        if constexpr (has_to_string_v<decltype(j->value())>) {
          std::cout << " "
                    << (j == nullptr && j->has_value() ?
                            std::to_string(j->value()) :
                            "no_value")
                    << " ";
        }
      }
      std::cout << ")" << std::endl;
      std::cout << event << "  "
                << " source done swapping items with " +
                       str(static_cast<Mover*>(this)->state()) + " and " +
                       str(static_cast<Mover*>(this)->next_state())
                << std::endl;
      ++event;
    }
  }

  /**
   * Diagnostic functions for counting numbers of swaps (moves).
   */
  size_t source_swaps() const {
    return moves_[0];
  }

  size_t sink_swaps() const {
    return moves_[1];
  }

  auto& source_item() {
    return *items_[0];
  }

  auto& sink_item() {
    return *items_[1];
  }
};

/**
 * Class template for moving data between end ports (perhaps via an edge)
 * in TileDB task graph.
 *
 * @tparam Policy The policy that uses the `ItemMover` for its implementation
 * of event actions provided to the `PortFiniteStateMachine`.  Note that
 * `Policy` is a template template, taking `ItemMover` and `PortState` as
 * template parameters.
 * @tparam PortState The type of the states that will be used by the `ItemMover`
 * (and the Policy, and the StateMachine)
 * @tparam Block The type of data to be moved.  Note that the mover assumes that
 * underlying items are `std::optional<Block>`.  The mover maintains pointers to
 * each of the underlying items.  Data movement is accomplished by using
 * `std::swap` on the things being pointed to (i.e., on the underlying items of
 * `std::optional<Block>`).  This should be fairly lightweight.
 *
 * `ItemMover` inherits from `Policy`, which takes `ItemMover` as a template
 * parameter (CRTP). `ItemMover` also inherits from `BaseMover`, which
 * implements all of the actual data movement. Clients of the `ItemMover`
 * activate its actions by calling the member functions `do_fill`, `do_push`,
 * `do_drain`, and `do_pull`, which correspond to actions in the
 * `PortFiniteStateMachine`.
 */
template <template <class, class> class Policy, class PortState, class Block>
class ItemMover
    : public Policy<ItemMover<Policy, PortState, Block>, PortState>,
      public BaseMover<ItemMover<Policy, PortState, Block>, PortState, Block> {
 public:
  using Mover = ItemMover<Policy, PortState, Block>;
  using BaseMover<Mover, PortState, Block>::BaseMover;

  using port_state_type = PortState;

  using block_type = Block;

  /**
   * Invoke `source_fill` event
   */
  void do_fill(const std::string& msg = "") {
    debug_msg("    -- filling");

    this->event(PortEvent::source_fill, msg);
  }

  /**
   * Invoke `source_push` event
   */
  void do_push(const std::string& msg = "") {
    debug_msg("  -- pushing");

    this->event(PortEvent::source_push, msg);
  }

  /**
   * Invoke `sink_drain` event
   */
  void do_drain(const std::string& msg = "") {
    debug_msg("  -- draining");

    this->event(PortEvent::sink_drain, msg);
  }

  /**
   * Invoke `sink_pull` event
   */
  void do_pull(const std::string& msg = "") {
    debug_msg("  -- pulling");

    this->event(PortEvent::sink_pull, msg);
  }

  /**
   * Invoke `shutdown` event
   */
  void do_shutdown(const std::string& msg = "") {
    this->event(PortEvent::shutdown, msg);
  }

  //  using state_machine_type = PortFiniteStateMachine<
  //      Policy<ItemMover<Policy, PortState, Block>, PortState>,
  //      PortState>;
  //
  //  state_machine_type* get_mover() {
  //    return static_cast<state_machine_type*>(*this);
  //  }
  //  auto get_state() {
  //    return static_cast<state_machine_type*>(this)->state();
  //  }
 private:
  void debug_msg(const std::string& msg) {
    if (static_cast<Mover*>(this)->debug_enabled()) {
      std::cout << msg << std::endl;
    }
  }
};
}  // namespace tiledb::common
#endif  // TILEDB_DAG_ITEM_MOVER_H
