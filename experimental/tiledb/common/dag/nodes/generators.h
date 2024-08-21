/**
 * @file   generator.h
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
 * This file declares the generators classes for TileDB task graph ("dag").
 */

#ifndef TILEDB_DAG_GENERATOR_H
#define TILEDB_DAG_GENERATOR_H

#include <random>
#include <tiledb/stdx/stop_token>

#include "experimental/tiledb/common/dag/ports/ports.h"

namespace tiledb::common {

/**
 * Prototype producer function object class.  This class generates a sequence of
 * integers starting at `N`, returning a new integer with every invocation of
 * `operator()`.
 */
template <class Integral = size_t>
class generators {
  std::atomic<Integral> min_{0}, max_{0};
  std::atomic<Integral> i_{0};

 public:
  generators(Integral min, Integral max)
      : min_{min}
      , max_{max}
      , i_{min_.load()} {
  }

  explicit generators(Integral min = 0)
      : min_{min}
      , max_{std::numeric_limits<Integral>::max()}
      , i_{min_.load()} {
  }

  generators(const generators& rhs)
      : min_(rhs.min_.load())
      , max_(rhs.max_.load())
      , i_(rhs.i_.load()) {
  }

  /**
   * Function returning sequence of numbers, from `min_` to `max_`.  Stop is
   * requested once `max_` is reached.
   *
   * @param stop_source Stop is requested once `i_` hits `max_`
   *
   * @return Next number in sequence, up to `max_`,
   */
  Integral operator()(std::stop_source& stop_source) {
    if (i_ >= max_) {
      stop_source.request_stop();
      return max_;
    }
    return i_++;
  }
};

template <class T>
struct distrib_type {
  using type = std::uniform_int_distribution<T>;
};

template <>
struct distrib_type<float> {
  using type = std::uniform_real_distribution<>;
};

template <>
struct distrib_type<double> {
  using type = std::uniform_real_distribution<>;
};

/**
 * Prototype PRNG function object class.  This class generates a sequence of
 * pseudorandom numbers from a given seed, returning a new pseudorandom number
 * with each invocation of `operator()`.
 */
template <class Integral = size_t>
class prng {
  std::atomic<Integral> min_{0}, max_{0};

  std::random_device rd_;

  std::mt19937 gen_;

  typename distrib_type<Integral>::type distrib_;

 public:
  prng(Integral min, Integral max)
      : min_{min}
      , max_{max}
      , gen_{rd_()}
      , distrib_{min, max} {
  }

  void seed(Integral n) {
    gen_.seed(n);
  }

  /**
   * Function returning sequence of numbers, from `min_` to `max_`.  Stop is
   * requested once `max_` is reached.
   *
   * @param stop_source Stop is requested once `i_` hits `max_`
   *
   * @return Next number in sequence, up to `max_`,
   */
  Integral operator()() {
    return distrib_(gen_);
  }
};

/**
 * Injector node class.  This is a producer-like node that allows data items to
 * be injected into the root of a dag task graph and sent into the dag, as if
 * they had been generated by a `Producer` node. It exposes three member
 * functions: `put`, `try_put`, and `stop()`.
 *
 * @todo Properly implement `try_put` to be non-blocking.
 *
 * @todo Make safe for multithreaded operation.
 */
template <template <class> class Mover_T, class Block>
class InjectorNode : public Source<Mover_T, Block> {
  using Base = Source<Mover_T, Block>;
  using source_type = Source<Mover_T, Block>;

 public:
  /**
   * Non-blocking function to attempt injection of input item into task graph.
   *
   * Will return if an item is present in the `Source`.
   *
   * @note This is currently broken, as `port_push()` may still block.  Need to
   * implement something like 'is_pushable' because we don't want to inject and
   * then find the `port_push` will block.
   */
  bool try_put(const Block& input_item) {
    auto state_machine = this->get_mover();
    if (state_machine->is_stopping()) {
      return false;
    }
    if (!Base::inject(input_item)) {
      return false;
    }

    state_machine->port_fill();
    state_machine->port_push();

    return true;
  }

  /**
   * Blocking function to inject input item into task graph.
   */
  bool put(const Block& input_item) {
    auto state_machine = this->get_mover();
    if (state_machine->is_stopping()) {
      return false;
    }
    Base::inject(input_item);
    state_machine->port_fill();
    state_machine->port_push();

    return true;
  }

  /**
   * Issue stop event.
   */
  void stop() {
    auto state_machine = this->get_mover();
    state_machine->port_exhausted();
  }
};

}  // namespace tiledb::common
#endif  // TILEDB_DAG_GENERATOR_H
