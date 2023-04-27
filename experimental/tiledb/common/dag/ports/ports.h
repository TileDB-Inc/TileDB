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
 * This file declares the `Port`, `Source`, and `Sink` classes for the dag task
 * graph library.
 *
 * A port holds a data item that can be read or written by a client.  The
 * `Source` and `Sink` classes are derived from `Port` and share a significant
 * amount of functionality.  They are distinguished from each other in order to
 * establish "directionality": data items are sent from `Source` to `Sink`.  The
 * `Source` and `Sink` classes will be used to establish data transfer from dag
 * task graph nodes via `Edge` classes (or via direct connections, if, for some
 * reason, buffered data transfer is not desired).
 *
 */

#ifndef TILEDB_DAG_PORTS_H
#define TILEDB_DAG_PORTS_H

#include <atomic>
#include <cassert>
#include <condition_variable>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>

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

  std::mutex mutex_;

  /**
   * Pointer to the item_mover_ to be used by the `Port`
   */
  std::shared_ptr<mover_type> item_mover_;

  /**
   * Storage to cache an item to be sent or received via the `Port`
   */
  std::optional<Block> item_{};

  constexpr inline std::optional<Block>& get_item() {
    return item_;
  }

  /**
   * Flag indicating whether the `Port` has been connected to another `Port`.
   */
  std::atomic<bool> attached_{false};

  /**
   * Function return whether the `Port` has been connected to another `Port`.
   */
  bool is_attached() {
    return attached_;
  }

  /**
   * Set attached flag to true.
   *
   * @pre Called under lock
   */
  bool set_attached() {
    attached_ = true;
    return attached_;
  }

  /**
   * Set attached flag to false.
   *
   * @pre Called under lock
   */
  bool clear_attached() {
    attached_ = false;
    return attached_;
  }

  /**
   * Remove the current attachment, if any.
   *
   * @pre Called under lock
   *
   */
  void detach() {
    std::lock_guard lock(this->mutex_);
    if (!is_attached()) {
      throw std::runtime_error(
          "Attempting to unattached unattached correspondent");
    }
    clear_attached();
    item_mover_.reset();
  }

 public:
  auto get_mover() const {
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
  using base_port_type = port_type;

  /**
   * Inject an item into the `Source`.  The `item_` will not be set if it
   * already contains a value.
   *
   * @param value The `Block` with which to set `item`
   * @return `true` if `item_` was successfully set, otherwise `false`.
   *
   * @pre The `Source` port is attached to a `Sink` port.
   */
  bool inject(const Block& value) {
    std::lock_guard lock(this->mutex_);
    if (!this->is_attached()) {
      throw std::logic_error("Sink not attached in inject");
      return {};
    }

    if (this->item_.has_value()) {
      return false;
    }

    this->item_ = value;
    return true;
  }

  /**
   * Extract an item from the `Source` by swapping `item_` with an empty
   * `std::optional<Block>`.  Used only for testing/debugging.
   *
   * @post `item_` will be empty.
   */
  std::optional<Block> extract() {
    std::lock_guard lock(this->mutex_);
    if (!this->is_attached()) {
      throw std::logic_error("Source not attached in extract");
      return {};
    }
    if (!this->item_.has_value() && this->get_mover()->debug_enabled()) {
      std::cout << "Source extract no value with state = "
                << str(this->item_mover_->state()) << std::endl;
    }

    std::optional<Block> ret{};
    std::swap(ret, this->item_);

    return ret;
  }
};

/**
 * A data flow sink, used by both edges and nodes.
 *
 * Sink objects have two states: empty and full.  Their functionality is
 * determined by the states (and policies) of the `Mover`.  Their
 * functionality is determined by the states (and policies) of the `Mover`.
 */
template <template <class> class Mover_T, class Block>
class Sink : public Port<Mover_T, Block> {
  using Port<Mover_T, Block>::Port;
  friend Port<Mover_T, Block>;
  using port_type = Port<Mover_T, Block>;

  friend class Source<Mover_T, Block>;
  //    using mover_type = typename port_type::mover_type;
  //    using source_type = typename port_type::source_type;
  //    using sink_type = typename port_type::sink_type;

  using mover_type = Mover_T<Block>;
  using source_type = Source<Mover_T, Block>;
  using sink_type = Sink<Mover_T, Block>;

 public:
  using base_port_type = port_type;

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
    std::lock_guard lock(this->mutex_);
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

  void attach(source_type& predecessor, std::shared_ptr<mover_type> mover) {
    std::lock_guard lock(this->mutex_);
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
    std::lock_guard lock(this->mutex_);
  }
  else {
    this->clear_attached();
    predecessor.detach();
  }
}

public :
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
    friend inline void
    attach(Source<Mver_T, Blck>& source, Sink<Mver_T, Blck>& sink);

friend inline void attach(source_type& source, sink_type& sink) {
  sink.attach(source);
}

template <template <class> class Mver_T, class Blck>
friend inline void attach(
    Source<Mver_T, Blck>& source,
    Sink<Mver_T, Blck>& sink,
    std::shared_ptr<Mver_T<Blck>>& mover);

friend inline void attach(
    source_type& source, sink_type& sink, std::shared_ptr<mover_type>& mover) {
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
  std::lock_guard lock(this->mutex_);
  if (!this->is_attached()) {
    throw std::logic_error("Sink not attached in inject");
    return {};
  }

  if (this->item_.has_value()) {
    return false;
  }

  this->item_ = value;
  return true;
}

/**
 * Extract an item from the `Sink` by swapping `item_` with an empty
 * `std::optional<Block>`.
 *
 * @post `item_` will be empty.
 */
std::optional<Block> extract() {
  std::lock_guard lock(this->mutex_);
  if (!this->is_attached()) {
    throw std::logic_error("Sink not attached in extract");
    return {};
  }
  if (!this->item_.has_value() && this->get_mover()->debug_enabled()) {
    std::cout << "Sink extract no value with state = "
              << str(this->item_mover_->state()) << std::endl;
  }

  std::optional<Block> ret{};
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
