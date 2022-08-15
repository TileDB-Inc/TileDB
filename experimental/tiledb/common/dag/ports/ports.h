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
 * @todo: Refactor `Source` and `Sink` to use a Port base class.
 */

#ifndef TILEDB_DAG_PORTS_H
#define TILEDB_DAG_PORTS_H

#include <condition_variable>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>

#include "../utils/print_types.h"

namespace tiledb::common {

/* Forward declarations */
template <template <class> class Mover_T, class Block>
class Source;

template <template <class> class Mover_T, class Block>
class Sink;

/**
 * Base port class for both `Source` and `Sink`.  Maintains common data and
 * functions used by both.
 */
template <template <class> class Mover_T, class Block>
class Port {
 protected:
  using mover_type = Mover_T<Block>;
  using source_type = Source<Mover_T, Block>;
  using sink_type = Sink<Mover_T, Block>;

  /**
   * Pointer to the item_mover_ to be used by the `Port`
   */
  std::shared_ptr<mover_type> item_mover_;

  /**
   * Storage to cache an item to be sent or received via the `Port`
   */
  std::optional<Block> item_{};

  /**
   * Flag indicating whether the `Port` has been connected to another `Port`.
   */
  bool attached_{false};

  /**
   * Function return whether the `Port` has been connected to another `Port`.
   */
  bool is_attached() {
    return attached_;
  }

  /**
   * Set attached flag to true.
   */
  bool set_attached() {
    attached_ = true;
    return attached_;
  }

  /**
   * Set attached flag to false.
   */
  bool clear_attached() {
    attached_ = false;
    return attached_;
  }

  /**
   * Remove the current attachment, if any.
   *
   * @pre Called under lock
   */
  void unattach() {
    if (!is_attached()) {
      throw std::runtime_error(
          "Attempting to unattach unattached correspondent");
    }
    clear_attached();
    item_mover_.reset();
  }

 public:
  auto get_mover() {
    return item_mover_;
  }
};

/**
 * A data flow source, used by both edges and nodes.
 *
 * Source objects have two states: empty and full.
 */
template <template <class> class Mover_T, class Block>
class Source : public Port<Mover_T, Block> {
  using Port<Mover_T, Block>::Port;
  friend Port<Mover_T, Block>;
  using port_type = Port<Mover_T, Block>;

  friend class Sink<Mover_T, Block>;
  using mover_type = typename port_type::mover_type;
  using sink_type = typename port_type::sink_type;

 protected:
 public:
  Source() = default;
  Source(const Source& rhs) = delete;
  Source(Source&& rhs) = delete;
  Source& operator=(const Source& rhs) = delete;
  Source& operator=(Source&& rhs) = delete;

 public:
  /**
   * Inject an item into the `Source`.
   */
  bool inject(const Block& value) {
    if (!this->is_attached() || this->item_.has_value()) {
      return false;
    }
    this->item_ = value;

    using mover_type = Mover_T<Block>;
    using source_type = Source<Mover_T, Block>;
    using sink_type = Sink<Mover_T, Block>;

    /**
     * Extract an item into the `Source`.  Used only for testing.
     */
    std::optional<Block> extract() {
      if (!this->is_attached()) {
        throw std::logic_error("Sink not attached in extract");
        return {};
      }
      std::optional<Block> ret{};
      std::swap(ret, this->item_);

      return ret;
    }
  };

  /**
   * A data flow sink, used by both edges and nodes.
   *
   * Sink objects have two states: emptty and full.  Their functionality is
   * determined by the states (and policies) of the `Mover`.  Their
   * functionality is determined by the states (and policies) of the `Mover`.
   */
  template <template <class> class Mover_T, class Block>
  class Sink : public Port<Mover_T, Block> {
    using Port<Mover_T, Block>::Port;
    friend Port<Mover_T, Block>;
    using port_type = Port<Mover_T, Block>;

    friend class Source<Mover_T, Block>;
    using mover_type = typename port_type::mover_type;
    using source_type = typename port_type::source_type;
    using sink_type = typename port_type::sink_type;

   public:
    Sink() = default;

    Sink(const Sink& rhs) = delete;
    Sink(Sink&& rhs) = delete;
    Sink& operator=(const Sink& rhs) = delete;
    Sink& operator=(Sink&& rhs) = delete;

   public:
    /**
     * Create functions (and friends) to manage attaching and detaching of
     * `Sink` and `Source` ports.
     */
    void attach(source_type& predecessor) {
      if (this->is_attached() || predecessor.is_attached()) {
        throw std::runtime_error(
            "Sink attempting to attach to already attached ports");
      } else {
        this->item_mover_ = std::make_shared<mover_type>();
        predecessor.item_mover_ = this->item_mover_;
        this->item_mover_->register_port_items(predecessor.item_, this->item_);
        this->set_attached();
        predecessor.set_attached();
      }
    }
  };

  void attach(source_type& predecessor, std::shared_ptr<mover_type> mover) {
    if (this->is_attached() || predecessor.is_attached()) {
      throw std::runtime_error(
          "Sink attempting to attach to already attached ports");
    } else {
      predecessor.item_mover_ = this->item_mover_ = mover;
      this->item_mover_->register_port_items(predecessor.item_, this->item_);
      this->set_attached();
      predecessor.set_attached();
    }
  }

  void detach(source_type& predecessor) {
    if (!this->is_attached() || !predecessor.is_attached()) {
      throw std::runtime_error("Sink attempting to detach unattached ports");
    } else {
      this->item_mover_->deregister_items();
      detach();
      predecessor.detach();
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
    sink.attach(source);
  }

  template <template <class> class Mver_T, class Blck>
  friend inline void attach(
      Source<Mver_T, Blck>& source,
      Sink<Mver_T, Blck>& sink,
      std::shared_ptr<Mver_T<Blck>>& mover);

  friend inline void attach(
      source_type& source,
      sink_type& sink,
      std::shared_ptr<mover_type>& mover) {
    sink.attach(source, mover);
  }

  template <template <class> class Mver_T, class Blck>
  friend inline void detach(
      Source<Mver_T, Blck>& source, Sink<Mver_T, Blck>& sink);

  friend inline void detach(source_type& source, sink_type& sink) {
    sink.detach(source);
  }

  template <template <class> class Mver_T, class Blck>
  friend inline void detach(
      Source<Mver_T, Blck>& source,
      Sink<Mver_T, Blck>& sink,
      Mver_T<Blck>& mover);

  friend inline void detach(
      source_type& source, sink_type& sink, mover_type& mover) {
    sink.detach(source, mover);
  }

 public:
  /**
   * Inject an item into the `Sink`.  Used only for testing.
   */
  bool inject(const Block& value) {
    if (!this->is_attached() || this->item_.has_value()) {
      return false;
    }
    this->item_ = value;
    return true;
  }

  /**
   * Remove an item from the `Sink`
   */
  std::optional<Block> extract() {
    if (!this->is_attached()) {
      throw std::logic_error("Sink not attached in extract");
      return {};
    }
    std::optional<Block> ret{};

    if (!this->item_.has_value()) {
      if (this->get_mover()->debug_enabled()) {
        std::cout << "extract no value with state = "
                  << str(this->item_mover_->state()) << std::endl;
      }
    }

    std::swap(ret, this->item_);

    return ret;
  }
};

/**
 * Attach a `Source` and a `Sink`.
 */
template <template <class> class Mover_T, class Block>
inline void attach(Sink<Mover_T, Block>& sink, Source<Mover_T, Block>& source) {
  attach(source, sink);
}

/**
 * Attach a `Source` and a `Sink` that will share an item mover (conceptually,
 * an edge).
 */
template <template <class> class Mover_T, class Block>
inline void attach(
    Sink<Mover_T, Block>& sink,
    Source<Mover_T, Block>& source,
    std::shared_ptr<Mover_T<Block>>& mover) {
  attach(source, sink, mover);
}

/**
 * Remove attachment.
 */
template <template <class> class Mover_T, class Block>
inline void detach(Sink<Mover_T, Block>& sink, Source<Mover_T, Block>& source) {
  detach(source, sink);
}

}  // namespace tiledb::common

#endif  // TILEDB_DAG_PORTS_H
