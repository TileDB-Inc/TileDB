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

namespace tiledb::common {

/* Forward declarations */
template <class Block>
class Source;

template <class Block>
class Sink;

/**
 * A data flow source, used by both edges and nodes.
 *
 * Source objects have two states: empty and full.
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
   * Submit an item to be transferred to correspondent_ Sink.  Blocking.  The
   * behavior of `submit` and `try_submit` will depend on the policy associated
   * with the state machine.  Used by dag nodes and edges for transferring data.
   *
   * @todo Investigate policy with blocking and non-blocking variants of
   * `do_push`.
   *
   * The time in the spinlock should be minimal.
   */
  void submit(Block&) {
    //  Don't need the guard since { state == empty_full ∨ state == empty_empty}
    //  at end of function and sink cannot change that.
    //  while (!source_is_empty())
    //    ;
    //
    //  { state == empty_empty ∨ state == empty_full }
    //  inject source item
    //  do_fill();
    //  { state == full_empty ∨ state == full_full }
    //  do_push();  // may block
    //  { state == empty_full ∨ state == empty_empty }
  }

  /**
   * Submit an item to be transferred to correspondent_ Sink.  Non-blocking. The
   * behavior of `submit` and `try_submit` will depend on the policy associated
   * with the state machine.  Used by dag nodes and edges for transferring data.
   *
   * @todo Investigate policy with blocking and non-blocking variants of
   * `do_push`.
   *
   * The time in the spinlock should be minimal.
   */
  bool try_submit() {
    //  Don't need the guard since { state == empty_full ∨ state == empty_empty}
    //  at end of function and sink cannot change that.
    //  while (!source_is_empty())
    //    ;
    //
    //  { state == empty_empty ∨ state == empty_full }
    //  inject source item
    //  do_fill();
    //  { state == full_empty ∨ state == full_full ∨ state == empty_empty
    //    ∨  state == empty_full }
    //  do_push(); // could have non-blocking try_push() event, but that would
    //             // leave the item  the item injected -- on failure, could
    //             // reject item -- would also need try_swap action.
    //  { state == empty_full ∨ state == empty_empty}
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
 * Sink objects have two states: empy and full.
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
   * Mutex shared by a correspondent pair. It's arbitratily defined in only the
   * Sink. Protects binding of Source and Sink. Protection of data transfer is
   * accomplished in the finite state machine.
   *
   * @todo Are there other Source / Sink functions that need to be protected?
   */
  std::mutex mutex_;

 public:
  /**
   * Retrieve `item_` from the Sink.  Blocking.  The behavior of `retrieve` and
   * `try_retrieve` will depend on the policy associated with the state machine.
   * Used by dag nodes and edges for transferring data.
   *
   * @todo Investigate policy with blocking and non-blocking variants of
   * `do_pull`.
   *
   * @return The retrieved `item_`.  The return value will be empty if
   * the `item_` is empty.
   */
  std::optional<Block> retrieve() {
    //  while (!sink_is_empty())
    //    ;
    //  { state == empty_empty ∨ state == full_empty }
    //  do_pull();
    //  { state == empty_full ∨ state == full_full }
    //  extract sink item;
    //  do_drain();
    //  { state == empty_empty ∨ state == full_empty ∨ state == empty_full ∨
    //  state == full_full } return item;

    //  Could also do?
  }

  /**
   * Retrieve `item_` from the Sink.  Non-blocking.  The behavior of `retrieve`
   * and `try_retrieve` will depend on the policy associated with the state
   * machine.  Used by dag nodes and edges for transferring data.
   *
   * @todo Investigate policy with blocking and non-blocking variants of
   * `do_pull`.
   *
   * @return The retrieved `item_`.  The return value will be empty if
   * the `item_` was not able to be retrieved.
   */
  std::optional<Block> try_retrieve() {
    //  if (sink_is_empty()) {
    //    { state == empty_empty ∨ state == full_empty }
    //    do_pull();
    //    extract sink item
    //    do_drain();
    //    return item;
    //  }
    //  return {};;
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
 * Free functions for binding / unbinding Source and Sink.
 */

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

#endif  // TILEDB_DAG_PORTS_H
