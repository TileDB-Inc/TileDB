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
 * This file declares the generators classes for dag.
 */

#ifndef TILEDB_DAG_GENERATOR_H
#define TILEDB_DAG_GENERATOR_H

#include "experimental/tiledb/common/dag/execution/stop_token.hpp"

namespace tiledb::common {

/**
 * Prototype producer function object class.  This class generates a sequence of
 * integers starting at `N`, returning a new integer with every invocation of
 * `operator()`.  
 */
template <class Integral = size_t>
class generator {
  std::atomic<Integral> min_{0}, max_{0};
  std::atomic<Integral> i_{0};

 public:
  generator(Integral min, Integral max)
      : min_{min}
      , max_{max}
      , i_{min_.load()} {
  }

  explicit generator(Integral min = 0)
      : min_{min}
      , max_{std::numeric_limits<Integral>::max()}
      , i_{min_.load()} {
  }

  generator(const generator& rhs)
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

}  // namespace tiledb::common
#endif  // TILEDB_DAG_GENERATOR_H
