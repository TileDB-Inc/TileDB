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
#include <memory>
#include <mutex>
#include <optional>

// #include "fsm.h"
// #include "policies.h"
// #include "../utils/print_types.h"

namespace tiledb::common {

/* Forward declarations */
template <template <class> class Mover_T, class Block>
class Source;

template <template <class> class Mover_T, class Block>
class Sink;

/**
 * A data flow source, used by both edges and nodes.
 *
 * Source objects have two states: empty and full.
 */
template <template <class> class Mover_T, class Block>
class Source {
  friend class Sink<Mover_T, Block>;

  using mover_type = Mover_T<Block>;
  using source_type = Source<Mover_T, Block>;
  using sink_type = Sink<Mover_T, Block>;

 public:
  std::optional<Block> item_{};

  /**
   * The correspondent Sink, if any
   */
  sink_type* correspondent_{nullptr};

  /**
   * The finite state machine controlling data transfer between Source and Sink.
   * Shared between Source and Sink.
   */
  std::shared_ptr<mover_type> item_mover_;

  Source() = default;
  Source(const Source& rhs) = delete;
  Source(Source&& rhs) = delete;
  Source& operator=(const Source& rhs) = delete;
  Source& operator=(Source&& rhs) = delete;

  /**
   * Check if Source is attached.
   *
   * @pre Called under lock
   */
  bool is_attached() const {
    return correspondent_ != nullptr;
  }

  /**
   * Check if Source is attached to a Sink
   *
   * @pre Called under lock
   */
  bool is_attached_to(sink_type* sink) const {
    return correspondent_ != nullptr && correspondent_ == sink;
  }

  /**
   * Remove the current attachment, if any.
   *
   * @pre Called under lock
   */
  void remove_attachment() {
    if (correspondent_ == nullptr || !is_attached_to(correspondent_) ||
        !correspondent_->is_attached_to(this)) {
      throw std::runtime_error(
          "Source attempting to unattach unattached correspondent");
    } else {
      correspondent_ = nullptr;
    }
  }

 public:
  /**
   * Get the data mover associated with this Source.  Used only for testing.
   */
  mover_type* get_mover() {
    return item_mover_.get();
  }

  bool inject(const Block& value) {
    if (!is_attached() || item_.has_value()) {
      return false;
    }
    std::scoped_lock lock(correspondent_->mutex_);
    item_ = value;
    // item_mover_->do_fill();

    return true;
  }

  std::optional<Block> extract() {
    if (!is_attached()) {
      return {};
    }
    std::optional<Block> ret{};
    std::swap(ret, item_);

    return ret;
  }
};

/**
 * A data flow sink, used by both edges and nodes.
 *
 * Sink objects have two states: empty and full.  Their functionality is
 * determined by the states (and policies) of the `Mover`.
 */
template <template <class> class Mover_T, class Block>
class Sink {
  friend class Source<Mover_T, Block>;

  using mover_type = Mover_T<Block>;
  using source_type = Source<Mover_T, Block>;
  using sink_type = Sink<Mover_T, Block>;

  /**
   * @inv If an item is present, `try_receive` will succeed.
   */
  std::optional<Block> item_{};

  /**
   * The correspondent Source, if any
   */
  source_type* correspondent_{nullptr};

  /**
   * The finite state machine controlling data transfer between Source and Sink.
   * Shared between Source and Sink.
   */
  std::shared_ptr<mover_type> item_mover_;

 public:
  Sink() = default;

  Sink(const Sink& rhs) = delete;
  Sink(Sink&& rhs) = delete;
  Sink& operator=(const Sink& rhs) = delete;
  Sink& operator=(Sink&& rhs) = delete;

  /**
   * Get the data mover associated with this Sink.  Used only for testing.
   */
  mover_type* get_mover() {
    return item_mover_.get();
  }

 private:
  /**
   * Mutex shared by a correspondent pair. It's arbitratily defined in only the
   * Sink and only protects attachment of Source and Sink. Protection of data
   * transfer is accomplished in the finite state machine.
   *
   * @todo Are there other Source / Sink functions that need to be protected?
   */
  mutable std::mutex mutex_;

 public:
  /**
   * Check if Sink is attached
   *
   * @pre Called under lock
   */
  bool is_attached() const {
    return correspondent_ != nullptr;
  }

  /**
   * Check if Sink is attached to a Source
   *
   * @pre Called under lock
   */
  bool is_attached_to(source_type* source) const {
    return correspondent_ != nullptr && correspondent_ == source;
  }

  /**
   * Assign a correspondent for this Sink.
   *
   * @pre Called under lock
   */
  void attach(source_type& predecessor) {
    if (is_attached() || predecessor.is_attached()) {
      throw std::runtime_error(
          "Sink attempting to attach to already attached correspondent");
    } else {
      correspondent_ = &predecessor;
      predecessor.correspondent_ = this;

      if constexpr (mover_type::is_direct_connection()) {
        item_mover_ = std::make_shared<mover_type>();
        correspondent_->item_mover_ = item_mover_;
        item_mover_->register_items(correspondent_->item_, item_);
      }
    }
  }

  /**
   * Remove the current attachment, if any.
   *
   * @pre Called under lock
   */
  void unattach() {
    if (!is_attached_to(correspondent_) ||
        !correspondent_->is_attached_to(this)) {
      throw std::runtime_error(
          "Attempting to unattach unattached correspondent");
    } else {
      if constexpr (mover_type::is_direct_connection()) {
        item_mover_->deregister_items(correspondent_->item_, item_);
      }
      item_mover_.reset();
      correspondent_->item_mover_.reset();
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
   * @pre Both source and sink are unattached
   */
  template <template <class> class Mver_T, class Blck>
  friend inline void attach(
      Source<Mver_T, Blck>& source, Sink<Mver_T, Blck>& sink);

  friend inline void attach(source_type& source, sink_type& sink) {
    std::scoped_lock lock(sink.mutex_);
    if (source.is_attached() || sink.is_attached()) {
      throw std::logic_error("Improperly attached in attach");
    }
    sink.attach(source);
    if (!source.is_attached_to(&sink) || !sink.is_attached_to(&source)) {
      throw std::logic_error("Improperly attached in attach");
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
  template <template <class> class Mver_T, class Blck>
  friend inline void unattach(
      Source<Mver_T, Blck>& source, Sink<Mver_T, Blck>& sink);

  friend inline void unattach(
      Source<Mover_T, Block>& source, Sink<Mover_T, Block>& sink) {
    std::scoped_lock lock(sink.mutex_);
    if (source.is_attached() && sink.is_attached()) {
      sink.unattach();
    } else {
      throw std::logic_error("Improperly attached in unattach");
    }
  }

 public:
  bool inject(const Block& value) {
    if (!is_attached() || item_.has_value()) {
      return false;
    }
    item_ = value;
    return true;
  }

  std::optional<Block> extract() {
    if (!is_attached()) {
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
 * @pre Both source and sink are unattached
 */
template <template <class> class Mover_T, class Block>
inline void attach(Sink<Mover_T, Block>& sink, Source<Mover_T, Block>& source) {
  attach(source, sink);
}

#if 0
template <class Mover, class Block>
inline void attach(
    Source<Mover, Block>& source, Sink<Mover, Block>& sink) {
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
template <template <class> class Mover_T, class Block>
inline void unattach(
    Sink<Mover_T, Block>& sink, Source<Mover_T, Block>& source) {
  unattach(source, sink);
}

}  // namespace tiledb::common

#endif  // TILEDB_DAG_PORTS_H
