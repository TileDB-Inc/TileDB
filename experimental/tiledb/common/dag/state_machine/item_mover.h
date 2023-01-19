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
 * @todo Specialize for `void` Block type.
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
#include "experimental/tiledb/common/dag/utility/traits.h"

namespace tiledb::common {

/**
 * Base class for data movers.  The base class contains the actual data items as
 * well as an `on_move` member functions for moving data along the pipeline.
 */
template <class Mover, class PortState, class Block>
class BaseMover;

/**
 * Specialization of the `BaseMover` class for three stages.
 *
 * @todo Specialize for `void` Block type.  (That we have to do the same thing
 * for the ports is another reason to just keep the items actually in the
 * `Mover`.)
 */
template <class Mover, class Block>
class BaseMover<Mover, three_stage, Block> {
  using item_type = std::optional<Block>;

  std::array<item_type*, 3> items_;
  item_type edge_item_;

 protected:
  constexpr inline static bool edgeful = true;

  /**
   * Record keeping of how many moves are made.  Used for diagnostics and
   * debugging.
   */
  std::array<size_t, 3> moves_{0, 0, 0};

 public:
  using PortState = three_stage;

  BaseMover() = default;

  BaseMover(item_type& source_init, item_type& edge_init, item_type& sink_init)
      : items_{&source_init, &edge_init, &sink_init} {
  }

  /**
   * Register items to be moved with the data mover.  In the context of TileDB
   * task graph, this will generally be a source, an edge intermediary, and a
   * sink.  IMPORTANT:  The "edge item" is actually stored in the mover, as the
   * Edge itself may go out of scope.
   *
   * @pre Called under lock.
   */
  void register_port_items(item_type& source_item, item_type& sink_item) {
    items_[0] = &source_item;
    items_[1] = &edge_item_;
    items_[2] = &sink_item;
  }

  /**
   * Deregister items.
   *
   * @pre Called under lock.
   */
  void deregister_items() {
    for (auto&& j : items_) {
      if (j == nullptr) {
        throw std::runtime_error(
            "Attempting to deregister source, edge, or sink items that were "
            "not registered.");
      }
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
    // CHECK(is_ready_to_move(state) == "");

    moves_[0]++;

    if (static_cast<Mover*>(this)->debug_enabled()) {
      std::cout << event << "  "
                << "  base mover swapping items with " + str(state) + " and " +
                       str(static_cast<Mover*>(this)->next_state())
                << std::endl;

      std::cout << event;
      std::cout << "    "
                << "Action on_move state = (";
      if constexpr (std::is_fundamental_v<decltype(items_[0])>) {
        for (auto& j : items_) {
          std::cout << " "
                    << (j != nullptr && j->has_value() ?
                            std::to_string(j->value()) :
                            "no_value")
                    << " ";
        }
      } else {
        for (auto& j : items_) {
          std::cout << " "
                    << (j != nullptr && j->has_value() ? "has_value" :
                                                         "no_value")
                    << " ";
        }
      }
      std::cout << ") -> ";
    }

    /*
     * Do the moves, always pushing all elements as far forward as possible. The
     * moves call swap between pairs of items.  Note that the values being
     * pointed to by the items array are swapped, not the pointer themselves
     * (though, presumably, the pointers could be swapped.  Thus, the swaps and
     * the moves are **deep copies** since the item_type is an optional.
     */
    switch (state) {
      case PortState::st_101:
      case PortState::xt_101:
        //        CHECK(*(items_[0]) != EMPTY_SINK);
        std::swap(*items_[0], *items_[1]);
        break;

      case PortState::st_010:
      case PortState::xt_010:
        // CHECK(*(items_[1]) != EMPTY_SINK);
        std::swap(*items_[1], *items_[2]);
        break;

      case PortState::st_100:
      case PortState::xt_100:
        std::swap(*items_[0], *items_[1]);
        std::swap(*items_[1], *items_[2]);
        break;

      case PortState::st_110:
      case PortState::xt_110:
        //        CHECK(*(items_[1]) != EMPTY_SINK);
        std::swap(*items_[1], *items_[2]);
        //        CHECK(*(items_[0]) != EMPTY_SINK);
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
      if constexpr (std::is_fundamental_v<decltype(items_[0])>) {
        for (auto& j : items_) {
          std::cout << " "
                    << (j != nullptr && j->has_value() ?
                            std::to_string(j->value()) :
                            "no_value")
                    << " ";
        }
      } else {
        for (auto& j : items_) {
          std::cout << " "
                    << (j != nullptr && j->has_value() ? "has_value" :
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

  constexpr inline static bool is_direct_connection() {
    return false;
  }

#if 0
  size_t source_swaps() const {
    return moves_[0];
  }

  size_t sink_swaps() const {
    return moves_[2];
  }
#endif

  auto& source_item() {
    return *items_[0];
  }

  auto& sink_item() {
    return *items_[2];
  }

  bool is_stopping() {
    auto st = static_cast<Mover*>(this)->state();
    auto nst = static_cast<Mover*>(this)->next_state();
    return (
        (st >= three_stage::xt_000 && st <= three_stage::xt_111) ||
        (nst >= three_stage::xt_000 && nst <= three_stage::xt_111) ||
        st == three_stage::done);
  }

  bool is_done() {
    return static_cast<Mover*>(this)->state() == three_stage::done;
  }

#if 0
 private:
  void debug_msg(const std::string& msg) {
    if (static_cast<Mover*>(this)->debug_enabled()) {
      std::cout << msg << std::endl;
    }
  }
#endif
};

/**
 * Specialization of the `BaseMover` class for two stages.
 */
template <class Mover, class Block>
class BaseMover<Mover, two_stage, Block> {
  using item_type = std::optional<Block>;

  /*
   * Array to hold pointers to items we will be responsible for moving.
   */
  std::array<item_type*, 2> items_;

 protected:
  constexpr inline static bool edgeful = false;

  /*
   * Array to hold move counts.  Used for diagnostics and profiling.
   */
  std::array<size_t, 2> moves_{0, 0};

 public:
  using PortState = two_stage;

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
  void register_port_items(item_type& source_item, item_type& sink_item) {
    items_[0] = &source_item;
    items_[1] = &sink_item;
  }

  /**
   * Deregister the items.
   */
  void deregister_items() {
    for (auto& j : items_) {
      if (j == nullptr) {
        throw std::runtime_error(
            "Attempting to deregister source, edge, or sink items that "
            "were not registered.");
      }
      j->reset();
    }
  }

  /*
   * Do the actual data movement.  We move all items in the pipeline towards
   * the end so that there are no "holes".  In the case of two stages this
   * basically means that we are in state 10 and we swap to state 01.
   *
   * Note that the values being
   * pointed to by the items array are swapped, not the pointer themselves
   * (though, presumably, the pointers could be swapped.  Thus, the swaps and
   * the moves are **deep copies** since the item_type is an optional.
   *
   * @pre Called under lock
   */
  inline void on_move(std::atomic<int>& event) {
    auto state = static_cast<Mover*>(this)->state();
    bool debug = static_cast<Mover*>(this)->debug_enabled();

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
        if constexpr (has_to_string_v<decltype(j->value())>) {
          std::cout << " "
                    << (j != nullptr && j->has_value() ?
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
                    << (j != nullptr && j->has_value() ?
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

  constexpr inline static bool is_direct_connection() {
    return true;
  }

#if 0

  /**
   * Diagnostic functions for counting numbers of swaps (moves).
   */
  size_t source_swaps() const {
    return moves_[0];
  }

  size_t sink_swaps() const {
    return moves_[1];
  }

#endif

  auto& source_item() {
    return *items_[0];
  }

  auto& sink_item() {
    return *items_[1];
  }

  bool is_stopping() {
    auto st = static_cast<Mover*>(this)->state();
    auto nst = static_cast<Mover*>(this)->next_state();
    return (
        (st >= two_stage::xt_00 && st <= two_stage::xt_11) ||
        (nst >= two_stage::xt_00 && nst <= two_stage::xt_11) ||
        st == two_stage::done);
  }

  bool is_done() {
    return static_cast<Mover*>(this)->state() == two_stage::done;
  }
};

/**
 * Class template for moving data between end ports (perhaps via an edge)
 * in TileDB task graph.
 *
 * @tparam Policy The policy that uses the `ItemMover` for its
 * implementation of event actions provided to the `PortFiniteStateMachine`.
 * Note that `Policy` is a template template, taking `ItemMover` and
 * `PortState` as template parameters.
 * @tparam PortState The type of the states that will be used by the
 * `ItemMover` (and the Policy, and the StateMachine)
 * @tparam Block The type of data to be moved.  Note that the mover assumes
 * that underlying items are `std::optional<Block>`.  The mover maintains
 * pointers to each of the underlying items.  Data movement is accomplished
 * by using `std::swap` on the things being pointed to (i.e., on the
 * underlying items of `std::optional<Block>`).  This should be fairly
 * lightweight.
 *
 * `ItemMover` inherits from `Policy`, which takes `ItemMover` as a template
 * parameter (CRTP). `ItemMover` also inherits from `BaseMover`, which
 * implements all of the actual data movement. Clients of the `ItemMover`
 * activate its actions by calling the member functions `port_fill`,
 * `port_push`, `port_drain`, and `port_pull`, which correspond to actions in
 * the `PortFiniteStateMachine`.  `on_move` is handled by the base class.
 */
template <template <class, class> class Policy, class PortState, class Block>
class ItemMover
    : public Policy<ItemMover<Policy, PortState, Block>, PortState>,
      public BaseMover<ItemMover<Policy, PortState, Block>, PortState, Block> {
 public:
  using Mover = ItemMover<Policy, PortState, Block>;
  using BaseMover<Mover, PortState, Block>::BaseMover;
  using Base = BaseMover<Mover, PortState, Block>;

  using policy_type = Policy<Mover, PortState>;
  using scheduler_event_type = SchedulerAction;
  using port_state_type = PortState;
  using block_type = Block;
  constexpr inline static bool edgeful = Base::edgeful;

#ifndef FXM
  /**
   * Invoke `source_fill` event
   */
  scheduler_event_type port_fill(const std::string& msg = "") {
    debug_msg("    -- filling");

    return this->event(PortEvent::source_fill, msg);
  }

  /**
   * Invoke `source_push` event
   */
  scheduler_event_type port_push(const std::string& msg = "") {
    debug_msg("  -- pushing");

    return this->event(PortEvent::source_push, msg);
  }

  /**
   * Invoke `sink_drain` event
   */
  scheduler_event_type port_drain(const std::string& msg = "") {
    debug_msg("  -- draining");

    return this->event(PortEvent::sink_drain, msg);
  }

  /**
   * Invoke `sink_pull` event
   */
  scheduler_event_type port_pull(const std::string& msg = "") {
    debug_msg("  -- pulling");

    return this->event(PortEvent::sink_pull, msg);
  }
#else

  void do_inject(const std::string& msg = "") {
    debug_msg("  -- injecting");

    this->event(PortEvent::source_inject, msg);
  }

  void do_extract(const std::string& msg = "") {
    debug_msg("  -- extracting");

    this->event(PortEvent::sink_extract, msg);
  }

#endif
  /**
   * Invoke `port_exhausted` event
   */
  scheduler_event_type port_exhausted(const std::string& msg = "") {
    return this->event(PortEvent::exhausted, msg);
  }

  bool is_terminated() {
    return terminated(this->state());
  }

  bool is_terminating() {
    return terminating(this->state());
  }

 private:
  void debug_msg(const std::string& msg) {
    if (static_cast<Mover*>(this)->debug_enabled()) {
      std::cout << msg + ": state = " + str(this->state()) + "\n";
    }
  }
};
}  // namespace tiledb::common
#endif  // TILEDB_DAG_ITEM_MOVER_H
