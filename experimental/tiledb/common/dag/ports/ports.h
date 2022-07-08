/**
 * @file   ports.h
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
 * This file declares the Source and Sink ports for dag.
 *
 *
 * States for objects containing Source or Sink member variables.
 *
 * The design goal of these states is to limit the total number of std::thread
 * objects that simultaneously exist. Instead of a worker thread blocking
 * because correspondent source is empty or because a correspondent sink is
 * full, the worker can simply return. Tasks may become dormant without any
 * thread that runs them needing to block.
 *
 * States:
 *   Quiescent: initial and final state. No correspondent sources or sinks
 *   Dormant: Some correspondent exists, but no thread is currently active
 *   Active: Some correspondent exists, and some thread is currently active
 *
 * An element is alive if it is either dormant or active, that is, some
 * correspondent exists, regardless of thread state.
 *
 * Invariant: an element is registered with the scheduler as alive if and only
 * if the element is alive. Invariant: each element is registered with the
 * scheduler as either alive or quiescent.
 *
 * Todo: Refactor to use a Port base class.
 */

#ifndef TILEDB_DAG_PORTS_H
#define TILEDB_DAG_PORTS_H

#include <condition_variable>
#include <mutex>
#include <optional>

#include "fsm.h"
#include "policies.h"

#include "../utils/print_types.h"

namespace tiledb::common {

/* Forward declarations */
template <class Block, class StateMachine>
class Source;

template <class Block, class StateMachine>
class Sink;

/**
 * A data flow source, used by both edges and nodes.
 *
 * Source objects have two states: empty and full.
 */
template <class Block, class StateMachine>
class Source {
  friend class Sink<Block, StateMachine>;

 public:
  std::optional<Block> item_{};

  /**
   * The correspondent Sink, if any
   */
  Sink<Block, StateMachine>* correspondent_{nullptr};

  /**
   * Check if Source is bound.
   *
   * @pre Called under lock
   */
  bool is_bound() const {
    return correspondent_ != nullptr;
  }

  /**
   * Check if Source is bound to a Sink
   *
   * @pre Called under lock
   */
  bool is_bound_to(Sink<Block, StateMachine>* sink) const {
    return correspondent_ != nullptr && correspondent_ == sink;
  }

  /**
   * Remove the current attachment, if any.
   *
   * @pre Called under lock
   */
  void remove_attachment() {
    if (correspondent_ == nullptr || !is_bound_to(correspondent_) ||
        !correspondent_->is_bound_to(this)) {
      throw std::runtime_error(
          "Source attempting to unattach unbound correspondent");
    } else {
      correspondent_ = nullptr;
    }
  }

 public:
  StateMachine* get_state_machine() {
    return (correspondent_->get_state_machine());
  }

  bool inject(const Block& value) {
    if (!is_bound() || item_.has_value()) {
      return false;
    }
    std::scoped_lock lock(correspondent_->mutex_);
    item_ = value;
    // correspondent_->state_machine_->do_fill();

    return true;
  }

  std::optional<Block> extract() {
    if (!is_bound()) {
      return {};
    }
    std::optional<Block> ret{};
    std::swap(ret, item_);

    return ret;
  }
};  // namespace tiledb::common

/**
 * A data flow sink, used by both edges and nodes.
 *
 * Sink objects have two states: empty and full.  Their functionality is
 * determined by the states (and policies) of the `StateMachine`.
 */
template <class Block, class StateMachine>
class Sink {
  friend class Source<Block, StateMachine>;

  /**
   * @inv If an item is present, `try_receive` will succeed.
   */
  std::optional<Block> item_{};

  /**
   * The correspondent Source, if any
   */
  Source<Block, StateMachine>* correspondent_{nullptr};

  /**
   * The finite state machine controlling data transfer between Source and Sink.
   * We arbitrarily associate it with the Sink.
   */
  std::unique_ptr<StateMachine> state_machine_;

 public:
  Sink()
      : state_machine_{std::make_unique<StateMachine>()} {
  }

  StateMachine* get_state_machine() {
    return state_machine_.get();
  }

 private:
  /**
   * Mutex shared by a correspondent pair. It's arbitratily defined in only the
   * Sink. Protects attachment of Source and Sink. Protection of data transfer
   * is accomplished in the finite state machine.
   *
   * @todo Are there other Source / Sink functions that need to be protected?
   */
  mutable std::mutex mutex_;

 public:
  /**
   * Check if Sink is bound
   *
   * @pre Called under lock
   */
  bool is_bound() const {
    return correspondent_ != nullptr;
  }

  /**
   * Check if Sink is bound to a Source
   *
   * @pre Called under lock
   */
  bool is_bound_to(Source<Block, StateMachine>* source) const {
    return correspondent_ != nullptr && correspondent_ == source;
  }

  /**
   * Assign a correspondent for this Sink.
   *
   * @pre Called under lock
   */
  void attach(Source<Block, StateMachine>& predecessor) {
    if (is_bound() || predecessor.is_bound()) {
      throw std::runtime_error(
          "Sink attempting to attach to already bound correspondent");
    } else {
      correspondent_ = &predecessor;
      predecessor.correspondent_ = this;
      state_machine_->register_items(correspondent_->item_, item_);
    }
  }

  /**
   * Remove the current attachment, if any.
   *
   * @pre Called under lock
   */
  void unattach() {
    if (!is_bound_to(correspondent_) || !correspondent_->is_bound_to(this)) {
      throw std::runtime_error("Attempting to unattach unbound correspondent");
    } else {
      state_machine_->deregister_items(correspondent_->item_, item_);
      correspondent_->remove_attachment();
      correspondent_ = nullptr;
    }
  }

 public:
  /**
   * Free functions for attachment / unattachment Source and Sink.
   */

  /**
   * Assign sink as correspondent to source and vice versa.  Acquires lock
   * before calling any member functions.
   *
   * @pre Both source and sink are unbound
   */
  template <class Bl, class St>
  friend inline void attach(Source<Bl, St>& source, Sink<Bl, St>& sink);

  friend inline void attach(
      Source<Block, StateMachine>& source, Sink<Block, StateMachine>& sink) {
    std::scoped_lock lock(sink.mutex_);
    if (source.is_bound() || sink.is_bound()) {
      throw std::logic_error("Improperly bound in attach");
    }
    sink.attach(source);
    if (!source.is_bound_to(&sink) || !sink.is_bound_to(&source)) {
      throw std::logic_error("Improperly bound in attach");
    }
  }

  /**
   * Remove the correspondent relationship between a source and sink.  Acquires
   * lock before calling any member functions.
   *
   * @param source A Souce port
   * @param sink A Sink port
   *
   * @pre `source` and `sink` are in a correspondent relationship.
   */
  template <class Bl, class St>
  friend inline void unattach(Source<Bl, St>& source, Sink<Bl, St>& sink);

  friend inline void unattach(
      Source<Block, StateMachine>& source, Sink<Block, StateMachine>& sink) {
    std::scoped_lock lock(sink.mutex_);
    if (source.is_bound() && sink.is_bound()) {
      sink.unattach();
    } else {
      throw std::logic_error("Improperly bound in unattach");
    }
  }

 public:
  bool inject(const Block& value) {
    if (!is_bound() || item_.has_value()) {
      return false;
    }
    item_ = value;
    return true;
  }
  std::optional<Block> extract() {
    if (!is_bound()) {
      return {};
    }
    std::optional<Block> ret{};
    std::swap(ret, item_);

    return ret;
  }
};

/**
 * Assign sink as correspondent to source and vice versa.
 *
 * @pre Both source and sink are unbound
 */
template <class Block, class StateMachine>
inline void attach(
    Sink<Block, StateMachine>& sink, Source<Block, StateMachine>& source) {
  attach(source, sink);
}

#if 0
template <class Block, class StateMachine>
inline void attach(
    Source<Block, StateMachine>& source, Sink<Block, StateMachine>& sink) {
  sink.attach(source);
}
#endif

/**
 * Remove the correspondent relationship between a source and sink
 *
 * @param sink A Sink port
 * @param sink A Source port
 *
 * @pre `source` and `sink` are in a correspondent relationship.
 */
template <class Block, class StateMachine>
inline void unattach(
    Sink<Block, StateMachine>& sink, Source<Block, StateMachine>& source) {
  unattach(source, sink);
}

}  // namespace tiledb::common

#endif  // TILEDB_DAG_PORTS_H
