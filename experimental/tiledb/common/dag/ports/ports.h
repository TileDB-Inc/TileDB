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

#if 0

#include <condition_variable>
#include <mutex>
#include <optional>

#include "fsm.h"

namespace tiledb::common {




/* Forward declarations */
template <class Block>
class Source;

template <class Block>
class Sink;

/**
 * A data flow source, used by both edges and nodes.
 *
 * Source objects have three states: empty, full, and ready.
 */
template <class Block>
class Source {
  friend class Sink<Block>;

  template <class Bl>
  friend void unbind(Source<Bl>& src);

  /**
   * @inv If an item is present, `try_send` will succeed.
   */
  std::optional<Block> item_{};

  /**
   * The correspondent Sink, if any
   */
  Sink<Block>* correspondent_{nullptr};

 public:
  /**
   *
   */
  void submit(Block& item) {
    std::optional<Block> tmp{};
    std::swap(item_, tmp);
    (corresponent_->fsm).event(src_data_fill);
    (corresponent_->fsm).event(source_filled);
    src_cv.notify_one();
  }

  void try_swap() {
    if (ready) {
      std::swap(item_, correspondent_->item_);
      (corresponent_->fsm).event(source_swap);
      signal();
    }
  }

  /**
   * Assign a correspondent for this Source.
   */
  void bind(Sink<Block>& predecessor) {
    if (correspondent_ == nullptr) {
      correspondent_ = &predecessor;
    } else {
      throw std::runtime_error(
          "Attempting to bind to already bound correspondent");
    }
  }

  /**
   * Check if Sink is bound to a source
   */
  bool is_bound() const {
    return correspondent_ != nullptr;
  }

  /**
   * Remove the current correspondent, if any.
   */
  void unbind() {
    correspondent_ = nullptr;
  }
};

/**
 * A data flow sink, used by both edges and nodes.
 *
 * Sink objects have two states: empy, full, and ready.
 */
template <class Block>
class Sink {
  friend class Source<Block>;

  template <class Bl>
  friend void bind(Source<Bl>& src, Sink<Bl>& snk);

  template <class Bl>
  friend void unbind(Source<Bl>& src, Sink<Bl>& snk);

  template <class Bl>
  friend void unbind(Sink<Bl>& snk);

  /**
   * @inv If an item is present, `try_receive` will succeed.
   */
  std::optional<Block> item_;

  /**
   * The correspondent Source, if any
   */
  Source<Block>* correspondent_{nullptr};

  /**
   * Mutex shared by a correspondent pair. It's defined in only the Sink
   * arbitrarily.  Protects transfer of data item from Source to the Sink.
   */
  std::mutex mutex_;

 public:
  /**
   * Notification function to be called by a correspondent Source to signal that
   * it is ready to send data. If `try_put()` is called immediately, it should
   * ordinarily succeed.
   *
   * At the point of construction it should be as if
   * ready_to_send(false) was called in the constructor body.
   *
   * @pre This Sink object is registered as alive with the Scheduler.
   */
  void ready_to_send();

  /**
   * Retrieve `item_` from the Sink.
   *
   * @return The retrieved `item_`.  The return value will be empty if
   * the `item_` is empty.
   */
  std::optional<Block> retrieve() {
    std::scoped_lock lock(mutex_);
    std::optional<Block> tmp{};
    swap(tmp, item_);
    return tmp;
  }

  /**
   * Receive a block from a correspondent Source. Called by the Source.
   *
   * If `item_` is empty when `try_put` is called, it will be swapped with
   * the item being sent and true will be returned.  Otherwise, false will
   * be returned.
   *
   * @return true if items were successfully swapped
   * @post If return value is true, item_ will be full
   */
  bool try_put() {
    std::scoped_lock lock(mutex_);
    if (!item_.has_value() && (correspondent_->item_).has_value()) {
      std::swap(item_, correspondent_->item_);
      return true;
    }
    return false;
  }

  /**
   * Assign a correspondent for this Sink.
   */
  void bind(Source<Block>& successor) {
    if (correspondent_ == nullptr) {
      correspondent_ = &successor;
    } else {
      throw std::runtime_error(
          "Attempting to bind to already bound correspondent");
    }
  }

  /**
   * Check if Sink is bound to a source
   */
  bool is_bound() const {
    return correspondent_ != nullptr;
  }

  /**
   * Remove the current correspondent, if any.
   */
  void unbind() {
    if (correspondent_ == nullptr) {
      throw std::runtime_error("Attempting to unbind unbound correspondent");
    } else {
      correspondent_ = nullptr;
    }
  }
};

/**
 * Assign sink as correspondent to source and vice versa.
 *
 * @pre Both src and snk are unbound
 */
template <class Block>
inline void bind(Source<Block>& src, Sink<Block>& snk) {
  std::scoped_lock(snk.mutex_);
  src.bind(snk);
  snk.bind(src);
  assert(src == snk.correspondent_ && snk == src.correspondent_);
}

/**
 * Assign sink as correspondent to source and vice versa.
 *
 * @pre Both src and snk are unbound
 */
template <class Block>
inline void bind(Sink<Block>& snk, Source<Block>& src) {
  bind(src, snk);
}

/**
 * Remove the correspondent relationship between a source and sink
 *
 * @param src A Souce port
 * @param snk A Sink port
 *
 * @pre `src` and `snk` are in a correspondent relationship.
 */
template <class Block>
inline void unbind(Source<Block>& src, Sink<Block>& snk) {
  std::scoped_lock(snk.mutex_);
  assert(src == snk.correspondent_ && snk == src.correspondent_);

  src.unbind();
  snk.unbind();
};

/**
 * Remove the correspondent relationship between a source and sink
 *
 * @param snk A Sink port
 * @param snk A Source port
 *
 * @pre `src` and `snk` are in a correspondent relationship.
 */
template <class Block>
inline void unbind(Sink<Block>& snk, Source<Block>& src) {
  unbind(src, snk);
};

/**
 * Remove the correspondent relationship between a Source  and
 * its correspondent Sink
 *
 * @param src A Source port.
 */
template <class Block>
inline void unbind(Source<Block>& src) {
  unbind(src, *(src.correspondent_));
};

/**
 * Remove the correspondent relationship between a Sink and
 * its correspondent Source
 *
 * @param src A Sink port.
 */
template <class Block>
inline void unbind(Sink<Block>& snk) {
  unbind(*(snk.correspondent_), snk);
};

}  // namespace tiledb::common

#endif
#endif  // TILEDB_DAG_PORTS_H
